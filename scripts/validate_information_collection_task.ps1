# =============================================================================
# scripts\validate_information_collection_task.ps1  (Release 53.1)
#
# Validates the PaperTrader-InformationCollection scheduled task AND the
# live collector against the durable contract. Read-only: touches no task,
# no process, no store.
#
# A VALID production collection task must:
#   - run while the operator is LOGGED OFF (S4U / Password / ServiceAccount;
#     Interactive NEVER validates - an Interactive collector dies with the
#     logon session, which is the exact 2026-08-28 defect);
#   - carry the PERIODIC RECOVERY trigger (a repetition at the recovery
#     cadence) so a dead worker is relaunched within one cadence;
#   - carry a boot trigger;
#   - keep MultipleInstances=IgnoreNew (task half of the singleton);
#   - have NO execution time limit (the worker is long-lived by design);
#   - execute the canonical venv python + canonical worker script.
#
# Beyond the definition, the validator reports (informational, from the
# canonical control helper): worker topology (exactly one logical worker?),
# service state, heartbeat/progress freshness.
#
# -PrincipalProbe <LogonType> : hermetic test mode. Evaluates ONLY the
#   principal rule and returns without touching the scheduler.
#
# Reports (no exit statements):
#   printed token : R53_1_COLLECTION_TASK_VALID /
#                   R53_1_COLLECTION_TASK_INVALID - <reason> /
#                   R53_1_COLLECTION_SCHEDULER_INCOMPLETE - task not installed
#   $global:R531CollectionTaskValidateResult
# =============================================================================
[CmdletBinding()]
param(
    [string]$TaskName = 'PaperTrader-InformationCollection',
    [string]$PythonExe = 'C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe',
    [string]$WorkerScript = 'C:\Users\binis\paper_trader\scripts\run_information_collection_service.py',
    [int]$RecoveryRepetitionMinutes = 30,
    [string]$ReportFile = '',
    [string]$PrincipalProbe = ''
)

$global:R531CollectionTaskValidateResult = $null
$problems = @()

$LoggedOutCapable = @('S4U', 'Password', 'ServiceAccount')

if ($PrincipalProbe) {
    if ($LoggedOutCapable -contains $PrincipalProbe) {
        $global:R531CollectionTaskValidateResult = "PROBE_ACCEPT - $PrincipalProbe"
        Write-Output "R53_1_PRINCIPAL_ACCEPTED - LogonType=$PrincipalProbe is logged-out-capable"
    } else {
        $global:R531CollectionTaskValidateResult = "PROBE_REJECT - $PrincipalProbe"
        Write-Output "R53_1_PRINCIPAL_REJECTED - LogonType=$PrincipalProbe dies with the logon session; S4U required for a durable collector"
    }
    return
}

try { $t = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop } catch { $t = $null }
if ($null -eq $t) {
    $global:R531CollectionTaskValidateResult = 'SCHEDULER_INCOMPLETE'
    Write-Output "R53_1_COLLECTION_SCHEDULER_INCOMPLETE - task '$TaskName' is not installed; run scripts\install_information_collection_task.ps1"
    if ($ReportFile) {
        [PSCustomObject]@{ task = $TaskName; present = $false; verdict = 'SCHEDULER_INCOMPLETE' } |
            ConvertTo-Json -Depth 4 | Out-File -Encoding utf8 $ReportFile
    }
    return
}
$i = Get-ScheduledTaskInfo -TaskName $TaskName

if (-not $t.Settings.Enabled) { $problems += 'task is disabled' }

$act = $t.Actions[0]
if ($act.Execute -ne $PythonExe) { $problems += "action executes '$($act.Execute)' (expected the canonical venv python)" }
if ($act.Arguments -notmatch [regex]::Escape($WorkerScript)) { $problems += 'action does not run the canonical collection worker' }
if ($t.Actions.Count -ne 1) { $problems += "task has $($t.Actions.Count) actions (expected exactly 1)" }

$bootCount = 0
$recoveryCount = 0
$wantInterval = 'PT{0}M' -f $RecoveryRepetitionMinutes
foreach ($tr in $t.Triggers) {
    if (-not [bool]$tr.Enabled) { $problems += "a trigger of type $($tr.CimClass.CimClassName) is disabled" }
    if ($tr.CimClass.CimClassName -match 'Boot') { $bootCount++ }
    elseif ($tr.Repetition -and [string]$tr.Repetition.Interval -eq $wantInterval) { $recoveryCount++ }
}
if ($bootCount -lt 1) { $problems += 'no boot trigger (a reboot would leave collection down until something else fires)' }
if ($recoveryCount -lt 1) {
    $problems += ("no periodic recovery trigger repeating every $RecoveryRepetitionMinutes minutes " +
                  '(a dead worker would stay dead until the next logon - the 2026-08-28 defect)')
}

$logonType = [string]$t.Principal.LogonType
if ($LoggedOutCapable -notcontains $logonType) {
    $problems += ("Principal.LogonType=$logonType is not logged-out-capable; the collector dies with " +
                  "the logon session. Re-register from an ELEVATED PowerShell: " +
                  "install_information_collection_task.ps1 -PreferredLogonType S4U -Force")
}

if (-not $t.Settings.StartWhenAvailable) { $problems += 'StartWhenAvailable is off (a missed trigger would never recover)' }
if ([string]$t.Settings.MultipleInstances -ne 'IgnoreNew') { $problems += "MultipleInstances is $($t.Settings.MultipleInstances) (expected IgnoreNew - the task half of the singleton)" }
$etl = [string]$t.Settings.ExecutionTimeLimit
if ($etl -ne 'PT0S' -and $etl -ne '') {
    $problems += "ExecutionTimeLimit is $etl (expected none: the worker is long-lived; a limit would kill it mid-collection)"
}

# ---- live worker / service freshness (informational; read-only) ------------ #
$service = $null
$StatePy = Join-Path (Split-Path $PSScriptRoot -Parent) 'scripts\collection_service_control.py'
if (Test-Path $StatePy) {
    try {
        $out = & $PythonExe $StatePy --action status 2>&1
        if ($LASTEXITCODE -eq 0) { $service = ($out -join "`n") | ConvertFrom-Json }
    } catch { }
}

$report = [PSCustomObject]@{
    task                 = $TaskName
    present              = $true
    state                = [string]$t.State
    enabled              = $t.Settings.Enabled
    action               = [PSCustomObject]@{ Execute = $act.Execute; Arguments = $act.Arguments }
    boot_triggers        = $bootCount
    recovery_triggers    = $recoveryCount
    recovery_interval    = $wantInterval
    principal            = [PSCustomObject]@{ UserId = $t.Principal.UserId; LogonType = $logonType }
    logged_out_capable   = ($LoggedOutCapable -contains $logonType)
    start_when_available = $t.Settings.StartWhenAvailable
    multiple_instances   = [string]$t.Settings.MultipleInstances
    execution_time_limit = $etl
    restart_count        = $t.Settings.RestartCount
    restart_interval     = $t.Settings.RestartInterval
    last_run             = if ($i.LastRunTime) { $i.LastRunTime.ToString('s') } else { $null }
    last_result          = $i.LastTaskResult
    next_run             = if ($i.NextRunTime) { $i.NextRunTime.ToString('s') } else { $null }
    service              = if ($service) { [PSCustomObject]@{
                               service_state       = $service.service_state
                               worker_pid          = $service.worker_pid
                               heartbeat_age_s     = $service.heartbeat_age_seconds
                               progress_age_s      = $service.progress_age_seconds
                               loop_count          = $service.loop_count
                           } } else { $null }
    problems             = $problems
    verdict              = if ($problems.Count -eq 0) { 'VALID' } else { 'INVALID' }
}
if ($ReportFile) { $report | ConvertTo-Json -Depth 5 | Out-File -Encoding utf8 $ReportFile }

if ($problems.Count -eq 0) {
    $global:R531CollectionTaskValidateResult = 'VALID'
    Write-Output "R53_1_COLLECTION_TASK_VALID - $TaskName (boot=$bootCount recovery=$recoveryCount LogonType=$logonType; next run $($report.next_run))"
} else {
    $global:R531CollectionTaskValidateResult = 'INVALID - ' + ($problems -join '; ')
    Write-Output ("R53_1_COLLECTION_TASK_INVALID - " + ($problems -join '; '))
}
