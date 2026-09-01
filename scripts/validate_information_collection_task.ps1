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
#   - carry the PERIODIC RECOVERY trigger: a repetition at the recovery
#     cadence with CONTINUOUS coverage - either an indefinite repetition or a
#     daily recurrence whose one-day repetition windows abut (what the R53.1
#     installer registers; Task Scheduler rejects a serialized
#     TimeSpan.MaxValue duration). A finite window that leaves daily gaps, or
#     StopAtDurationEnd (which would kill the running worker), does NOT
#     validate - the SHAPE of the trigger is free, its coverage is not;
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
# -TriggerProbe <path> : hermetic test mode. Reads a JSON trigger set
#   ({"Triggers":[...]} or a bare array) and evaluates ONLY the trigger
#   contract (boot recovery + continuous periodic recovery) without touching
#   the scheduler. Prints R53_1_TRIGGER_CONTRACT_OK / _PROBLEMS.
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
    [string]$PrincipalProbe = '',
    [string]$TriggerProbe = ''
)

$global:R531CollectionTaskValidateResult = $null
$problems = @()

$LoggedOutCapable = @('S4U', 'Password', 'ServiceAccount')

# Same coverage rule as the installer: CONTINUOUS = indefinite repetition, or
# a daily (every-1-day) recurrence whose repetition window lasts the full day
# so consecutive windows abut. StopAtDurationEnd would kill the long-lived
# worker at each window end and never validates.
function Get-R531RepetitionCoverage([string]$Type, [string]$Interval,
                                    [string]$Duration, $DaysInterval,
                                    [bool]$StopAtDurationEnd = $false) {
    if (-not $Interval) { return 'NONE' }
    if ($StopAtDurationEnd) {
        return 'GAP:StopAtDurationEnd would kill the running worker at each duration end'
    }
    if (-not $Duration) { return 'CONTINUOUS' }
    $span = $null
    try { $span = [System.Xml.XmlConvert]::ToTimeSpan($Duration) } catch { }
    if ($null -eq $span) { return "GAP:unparseable repetition duration '$Duration'" }
    $di = 1
    if ($null -ne $DaysInterval -and [int]$DaysInterval -gt 0) { $di = [int]$DaysInterval }
    if ($Type -match 'Daily' -and $di -eq 1 -and $span -ge (New-TimeSpan -Days 1)) {
        return 'CONTINUOUS'
    }
    return "GAP:repetition stops after $Duration and the trigger does not recur every day"
}

# Evaluate a normalized trigger-record set against the durable contract.
# Records carry: Type, Enabled, RepetitionInterval, RepetitionDuration,
# DaysInterval, StopAtDurationEnd (missing fields are tolerated).
function Get-R531TriggerFindings($records, [string]$WantInterval) {
    $boot = 0; $recovery = 0; $probs = @()
    foreach ($r in @($records)) {
        $type = [string]$r.Type
        $enabled = $true
        if ($r.PSObject.Properties['Enabled']) { $enabled = [bool]$r.Enabled }
        if (-not $enabled) { $probs += "a trigger of type $type is disabled" }
        if ($type -match 'Boot') { $boot++; continue }
        $rep = ''
        if ($r.PSObject.Properties['RepetitionInterval']) { $rep = [string]$r.RepetitionInterval }
        if ($rep -ne $WantInterval) { continue }
        $dur = ''; $di = $null; $stopEnd = $false
        if ($r.PSObject.Properties['RepetitionDuration']) { $dur = [string]$r.RepetitionDuration }
        if ($r.PSObject.Properties['DaysInterval']) { $di = $r.DaysInterval }
        if ($r.PSObject.Properties['StopAtDurationEnd']) { $stopEnd = [bool]$r.StopAtDurationEnd }
        $coverage = Get-R531RepetitionCoverage $type $rep $dur $di $stopEnd
        if ($coverage -eq 'CONTINUOUS') { $recovery++ }
        else {
            $probs += ("a trigger repeats every $WantInterval but its coverage is not " +
                       "continuous ($coverage) - a dead worker could stay dead past one cadence")
        }
    }
    return [PSCustomObject]@{ Boot = $boot; Recovery = $recovery; Problems = @($probs) }
}

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

if ($TriggerProbe) {
    $raw = Get-Content -Path $TriggerProbe -Raw | ConvertFrom-Json
    $records = if ($raw.PSObject.Properties['Triggers']) { @($raw.Triggers) } else { @($raw) }
    $f = Get-R531TriggerFindings $records ('PT{0}M' -f $RecoveryRepetitionMinutes)
    $ok = ($f.Boot -ge 1 -and $f.Recovery -ge 1 -and @($f.Problems).Count -eq 0)
    $global:R531CollectionTaskValidateResult = if ($ok) { 'PROBE_TRIGGERS_OK' }
        else { 'PROBE_TRIGGERS_PROBLEMS' }
    [PSCustomObject]@{ boot_triggers = $f.Boot; recovery_triggers = $f.Recovery
                       problems = @($f.Problems) } | ConvertTo-Json -Depth 4 | Write-Output
    if ($ok) {
        Write-Output "R53_1_TRIGGER_CONTRACT_OK - boot=$($f.Boot) recovery=$($f.Recovery)"
    } else {
        Write-Output ("R53_1_TRIGGER_CONTRACT_PROBLEMS - boot=$($f.Boot) " +
                      "recovery=$($f.Recovery); " + (@($f.Problems) -join '; '))
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

$wantInterval = 'PT{0}M' -f $RecoveryRepetitionMinutes
$trRecords = @()
foreach ($tr in $t.Triggers) {
    $rep = $null; $dur = $null; $stopEnd = $false; $di = $null
    if ($tr.Repetition -and $tr.Repetition.Interval) { $rep = [string]$tr.Repetition.Interval }
    if ($tr.Repetition -and $tr.Repetition.Duration) { $dur = [string]$tr.Repetition.Duration }
    if ($tr.Repetition -and $tr.Repetition.StopAtDurationEnd) { $stopEnd = $true }
    if ($tr.PSObject.Properties['DaysInterval'] -and $tr.DaysInterval) { $di = [int]$tr.DaysInterval }
    $trRecords += [PSCustomObject]@{
        Type = [string]$tr.CimClass.CimClassName; Enabled = [bool]$tr.Enabled
        RepetitionInterval = $rep; RepetitionDuration = $dur
        DaysInterval = $di; StopAtDurationEnd = $stopEnd
    }
}
$findings = Get-R531TriggerFindings $trRecords $wantInterval
$bootCount = $findings.Boot
$recoveryCount = $findings.Recovery
$problems += @($findings.Problems)
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
