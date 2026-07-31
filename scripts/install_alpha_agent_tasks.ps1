<#
.SYNOPSIS
    Register the four Alpha Agent Stage 4 Windows Scheduled Tasks.

.DESCRIPTION
    Validates the repository, the venv Python, the Stage 4 config, the Stage
    1-3.5 packages and the Gmail credential presence; performs a report-only
    dry run; then registers exactly these four tasks for the CURRENT user:

        AlphaAgent-Collect          every 30 minutes (collect; no LLM/email)
        AlphaAgent-Morning-Report   daily 08:00 (research report + email)
        AlphaAgent-PostClose-Report Mon-Fri 18:30 (research report + email)
        AlphaAgent-Watchdog         hourly (deterministic health + recovery)
        AlphaAgent-Telegram         at logon (Stage 8 secure control plane;
                                    long polling; read-only + bounded research
                                    only; ALWAYS registered DISABLED here)

    All tasks run as the current user with LogonType Interactive (only while
    logged in), RunLevel Limited, MultipleInstances IgnoreNew,
    StartWhenAvailable, a bounded execution-time limit and at most two restarts
    on failure. NO Windows login password is ever requested or stored.

    RESEARCH AUTOMATION ONLY. These tasks never create orders, fills, signals,
    trade decisions, model promotions or Daily Close; trading automation stays
    OFF. If the Gmail credential is absent, the report/collect tasks are
    registered but left DISABLED and the one-time setup command is printed.

.PARAMETER ValidateOnly
    Validate + print the task definitions WITHOUT registering anything.

.PARAMETER SendTestReport
    After registering, run one immediate research report (test subject).
#>
[CmdletBinding()]
param(
    [string]$Config,
    [string]$PythonExe,
    [switch]$ValidateOnly,
    [switch]$SendTestReport
)

$ErrorActionPreference = "Stop"
$Repo = Split-Path -Parent $PSScriptRoot
if (-not $Config) { $Config = Join-Path $Repo "configs\alpha_agent\stage4_runtime.json" }
if (-not $PythonExe) { $PythonExe = Join-Path $Repo ".venv-win\Scripts\python.exe" }
$Runner = Join-Path $Repo "scripts\run_alpha_agent.py"
$CredDir = Join-Path $env:USERPROFILE ".paper_trader\alpha_agent_email"
$CredFile = Join-Path $CredDir "gmail_credential.dpapi"

function Fail($m) { Write-Host "ALPHA_AGENT_STAGE4_BLOCKED - $m"; exit 1 }
function Info($m) { Write-Host "[install] $m" }

# ---- validation ----------------------------------------------------------- #
if (-not (Test-Path $PythonExe)) { Fail "python not found: $PythonExe" }
if (-not (Test-Path $Runner))    { Fail "runner not found: $Runner" }
if (-not (Test-Path $Config))    { Fail "config not found: $Config" }
$cfg = Get-Content $Config -Raw | ConvertFrom-Json
foreach ($rootKey in @("stage1_registry_root", "stage2_ingestion_root",
                       "stage3_director_root", "stage3_5_news_rss_root")) {
    $root = $cfg.$rootKey
    if (-not (Test-Path (Join-Path $root "latest.json"))) {
        Fail "$rootKey has no verified latest.json ($root)"
    }
}
$credPresent = (Test-Path $CredFile)
Info "Repository:  $Repo"
Info "Python:      $PythonExe"
Info "Config:      $Config"
Info "Credential:  $(if ($credPresent) { 'present' } else { 'ABSENT' })"

# ---- report-only dry run -------------------------------------------------- #
Info "Report-only dry run (no email) ..."
& $PythonExe $Runner --config $Config --mode report-only --no-send-email
if (-not $?) { Fail "report-only dry run failed" }

# ---- task definitions ----------------------------------------------------- #
$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" `
    -LogonType Interactive -RunLevel Limited

function New-Settings([int]$limitMin) {
    New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew `
        -StartWhenAvailable `
        -ExecutionTimeLimit (New-TimeSpan -Minutes $limitMin) `
        -RestartCount 2 -RestartInterval (New-TimeSpan -Minutes 5)
}
function New-Action([string]$modeArgs) {
    New-ScheduledTaskAction -Execute $PythonExe `
        -Argument "`"$Runner`" --config `"$Config`" $modeArgs" `
        -WorkingDirectory $Repo
}

$today = (Get-Date).Date
$defs = @(
    @{ Name = "AlphaAgent-Collect";
       Action = (New-Action "--mode collect");
       Trigger = (New-ScheduledTaskTrigger -Once -At $today `
                    -RepetitionInterval (New-TimeSpan -Minutes 30));
       Settings = (New-Settings 20);
       Desc = "Alpha Agent Stage 4 collect (deterministic; no LLM, no email)." },
    @{ Name = "AlphaAgent-Morning-Report";
       Action = (New-Action "--mode research --label morning");
       Trigger = (New-ScheduledTaskTrigger -Daily -At "08:00");
       Settings = (New-Settings 25);
       Desc = "Alpha Agent Stage 4 morning research report + email." },
    @{ Name = "AlphaAgent-PostClose-Report";
       Action = (New-Action "--mode research --label post_close");
       Trigger = (New-ScheduledTaskTrigger -Weekly `
                    -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday `
                    -At "18:30");
       Settings = (New-Settings 25);
       Desc = "Alpha Agent Stage 4 post-close research report + email." },
    @{ Name = "AlphaAgent-Watchdog";
       Action = (New-Action "--mode watchdog");
       Trigger = (New-ScheduledTaskTrigger -Once -At $today `
                    -RepetitionInterval (New-TimeSpan -Hours 1));
       Settings = (New-Settings 10);
       Desc = "Alpha Agent Stage 4 hourly watchdog (deterministic recovery)." }
)

if ($ValidateOnly) {
    Info "ValidateOnly - task definitions (NOT registered):"
    foreach ($d in $defs) {
        Write-Host ("  {0} :: {1} {2}" -f $d.Name, $PythonExe,
                    $d.Action.Arguments)
    }
    Write-Host ("  {0} :: powershell.exe -File scripts\run_alpha_agent_telegram.ps1  (Stage 8; DISABLED)" -f "AlphaAgent-Telegram")
    Write-Host "ALPHA_AGENT_STAGE4_VALIDATED"
    exit 0
}

# ---- register ------------------------------------------------------------- #
foreach ($d in $defs) {
    Register-ScheduledTask -TaskName $d.Name -Action $d.Action `
        -Trigger $d.Trigger -Settings $d.Settings -Principal $principal `
        -Description $d.Desc -Force | Out-Null
    Info "Registered $($d.Name)"
    if (-not $credPresent) {
        Disable-ScheduledTask -TaskName $d.Name | Out-Null
        Info "Disabled  $($d.Name) (Gmail credential absent)"
    }
}

# ---- Stage 8: AlphaAgent-Telegram control task (idempotent; DISABLED) ------ #
# The secure Telegram long-polling control plane. It is READ-ONLY plus bounded
# research-request enqueue; it can never create an order/fill/trade decision,
# promote a model or mutate holdings/cash. It is ALWAYS registered DISABLED here
# and must be enabled by the user only after configuring the DPAPI bot token and
# completing final validation.
$TgRunner = Join-Path $Repo "scripts\run_alpha_agent_telegram.ps1"
$TgTokenDir = Join-Path $env:USERPROFILE ".paper_trader\alpha_agent_telegram"
$TgTokenFile = Join-Path $TgTokenDir "telegram_bot_token.dpapi"
$TgAction = New-ScheduledTaskAction -Execute "powershell.exe" `
    -Argument ("-NoProfile -ExecutionPolicy Bypass -File `"$TgRunner`"") `
    -WorkingDirectory $Repo
$TgTrigger = New-ScheduledTaskTrigger -AtLogOn
# No execution-time limit (0 = unlimited): the control plane long-polls.
$TgSettings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew `
    -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Seconds 0) `
    -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 2)
Register-ScheduledTask -TaskName "AlphaAgent-Telegram" -Action $TgAction `
    -Trigger $TgTrigger -Settings $TgSettings -Principal $principal `
    -Description ("Alpha Agent Stage 8 secure Telegram control (long polling; " +
                  "READ-ONLY + bounded research only; no orders/automation).") -Force | Out-Null
Disable-ScheduledTask -TaskName "AlphaAgent-Telegram" | Out-Null
Info "Registered AlphaAgent-Telegram (DISABLED)."
if (-not (Test-Path $TgTokenFile)) {
    Info "Telegram token absent - run scripts\configure_alpha_agent_telegram.ps1 before enabling."
}

# ---- immediate collect + optional test report ----------------------------- #
if ($credPresent) {
    Info "Running one immediate collect task ..."
    Start-ScheduledTask -TaskName "AlphaAgent-Collect"
}
if ($SendTestReport) {
    if (-not $credPresent) {
        Info "Skipping test report: Gmail credential absent."
    } else {
        Info "Running one immediate test research report ..."
        & $PythonExe $Runner --config $Config --mode research --label manual `
            --test-report
    }
}

# ---- state ---------------------------------------------------------------- #
Write-Host "== Alpha Agent scheduled task state =="
Get-ScheduledTask -TaskName "AlphaAgent-*" |
    Select-Object TaskName, State |
    Format-Table -AutoSize | Out-String | Write-Host

if (-not $credPresent) {
    Write-Host "ALPHA_AGENT_STAGE4_EMAIL_CREDENTIAL_REQUIRED"
    Write-Host "Run: powershell -ExecutionPolicy Bypass -File `"$Repo\scripts\configure_alpha_agent_email.ps1`""
    exit 0
}
Write-Host "ALPHA_AGENT_STAGE4_READY"
exit 0
