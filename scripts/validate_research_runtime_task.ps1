# =============================================================================
# scripts\validate_research_runtime_task.ps1  (Release 52)
#
# Validates the PaperTrader-ResearchRuntime scheduled task against the
# derived contract. Read-only: touches no task, no process, no store.
#
# A VALID production R52 task must run while the operator is LOGGED OFF:
# only a logged-out-capable Principal.LogonType (S4U / Password /
# ServiceAccount) can produce R52_TASK_VALID. Interactive NEVER can - an
# Interactive task silently reintroduces the exact defect R52 exists to
# close (evidence capture that depends on a person being at the keyboard).
#
# -PrincipalProbe <LogonType> : hermetic test mode. Evaluates ONLY the
#   principal rule for the given logon type and returns without touching
#   the scheduler.
#
# Reports (no exit statements):
#   printed token : R52_TASK_VALID / R52_TASK_INVALID - <reason> /
#                   R52_SCHEDULER_INCOMPLETE - task not installed
#   $global:R52TaskValidateResult
# =============================================================================
[CmdletBinding()]
param(
    [string]$TaskName = 'PaperTrader-ResearchRuntime',
    [string]$PythonExe = 'C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe',
    [string]$RuntimeScript = 'C:\Users\binis\paper_trader\scripts\run_research_runtime.py',
    [string[]]$TriggerTimes = @('08:15', '17:45', '19:45', '21:45'),
    [ValidateSet('Cycle', 'Persistent')][string]$Mode = 'Cycle',
    [string]$ReportFile = '',
    [string]$PrincipalProbe = ''
)

# Release 59. A PERSISTENT task is valid under a different contract from a
# CYCLE task, and validating one against the other's rules would reject a
# correct installation. The differences are exactly three: the entrypoint is
# invoked with --mode persistent, a logon trigger must exist so the worker
# returns after a reboot, and the execution time limit must be ABSENT
# (PT0S) - a two-hour cap on a persistent researcher is a guaranteed daily
# kill. The hung-run protection a time limit used to give is provided by the
# worker lease heartbeat instead, which is why removing it here is safe.
$persistent = ($Mode -eq 'Persistent')

$global:R52TaskValidateResult = $null
$problems = @()

# The logged-out-capable logon types. Everything else (Interactive,
# InteractiveOrPassword-degraded, Group, None) fails production validation.
$LoggedOutCapable = @('S4U', 'Password', 'ServiceAccount')

if ($PrincipalProbe) {
    if ($LoggedOutCapable -contains $PrincipalProbe) {
        $global:R52TaskValidateResult = "PROBE_ACCEPT - $PrincipalProbe"
        Write-Output "R52_PRINCIPAL_ACCEPTED - LogonType=$PrincipalProbe is logged-out-capable"
    } else {
        $global:R52TaskValidateResult = "PROBE_REJECT - $PrincipalProbe"
        Write-Output "R52_PRINCIPAL_REJECTED - LogonType=$PrincipalProbe cannot run while logged off; S4U required for production validation"
    }
    return
}

try { $t = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop } catch { $t = $null }
if ($null -eq $t) {
    $global:R52TaskValidateResult = 'SCHEDULER_INCOMPLETE'
    Write-Output "R52_SCHEDULER_INCOMPLETE - task '$TaskName' is not installed; run scripts\install_research_runtime_task.ps1"
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
if ($act.Arguments -notmatch [regex]::Escape($RuntimeScript)) { $problems += 'action does not run the canonical runtime entrypoint' }
if ($t.Actions.Count -ne 1) { $problems += "task has $($t.Actions.Count) actions (expected exactly 1)" }
if ($persistent -and ($act.Arguments -notmatch '--mode\s+persistent')) {
    $problems += 'action does not pass --mode persistent (the task would run one bounded cycle, not the autonomous researcher)'
}
if (-not $persistent -and ($act.Arguments -match '--mode\s+persistent')) {
    $problems += 'action passes --mode persistent but the requested contract is Cycle'
}

$allowedTriggerTypes = @('MSFT_TaskDailyTrigger')
if ($persistent) { $allowedTriggerTypes += 'MSFT_TaskLogonTrigger' }
$haveTimes = @()
$haveLogonTrigger = $false
foreach ($tr in $t.Triggers) {
    $ttype = [string]$tr.CimClass.CimClassName
    if ($allowedTriggerTypes -notcontains $ttype) {
        $problems += "trigger of type $ttype (expected " + ($allowedTriggerTypes -join ' or ') + ')'
    }
    if ($ttype -eq 'MSFT_TaskLogonTrigger') { $haveLogonTrigger = $true; continue }
    if ($tr.StartBoundary) { $haveTimes += ([DateTime]$tr.StartBoundary).ToString('HH:mm') }
}
$missing = @($TriggerTimes | Where-Object { $haveTimes -notcontains $_ })
if ($missing.Count -gt 0) { $problems += ('missing trigger times: ' + ($missing -join ', ')) }
if ($persistent -and -not $haveLogonTrigger) {
    $problems += 'no logon trigger (after a reboot nothing would restart the worker until the next daily time)'
}

$logonType = [string]$t.Principal.LogonType
if ($LoggedOutCapable -notcontains $logonType) {
    $problems += ("Principal.LogonType=$logonType is not logged-out-capable; the runtime must fire " +
                  "while the operator is logged off. Re-register from an ELEVATED PowerShell: " +
                  "install_research_runtime_task.ps1 -PreferredLogonType S4U -Force")
}

if (-not $t.Settings.StartWhenAvailable) { $problems += 'StartWhenAvailable is off (a missed trigger would never recover)' }
if ([string]$t.Settings.MultipleInstances -ne 'IgnoreNew') { $problems += "MultipleInstances is $($t.Settings.MultipleInstances) (expected IgnoreNew)" }
if ($persistent) {
    if ($t.Settings.ExecutionTimeLimit -notin @('PT0S', $null, '')) {
        $problems += "ExecutionTimeLimit is $($t.Settings.ExecutionTimeLimit); a persistent worker must have none (PT0S), or Windows kills the researcher on a timer"
    }
} else {
    if ($t.Settings.ExecutionTimeLimit -in @('PT0S', $null, '')) { $problems += 'no execution time limit (a hung run would block the queue forever)' }
}

$report = [PSCustomObject]@{
    task               = $TaskName
    mode               = $Mode
    present            = $true
    state              = [string]$t.State
    enabled            = $t.Settings.Enabled
    action             = [PSCustomObject]@{ Execute = $act.Execute; Arguments = $act.Arguments }
    trigger_times      = @($haveTimes | Sort-Object)
    expected_times     = @($TriggerTimes | Sort-Object)
    logon_trigger      = $haveLogonTrigger
    principal          = [PSCustomObject]@{ UserId = $t.Principal.UserId; LogonType = $logonType }
    logged_out_capable = ($LoggedOutCapable -contains $logonType)
    start_when_available = $t.Settings.StartWhenAvailable
    multiple_instances = [string]$t.Settings.MultipleInstances
    execution_time_limit = $t.Settings.ExecutionTimeLimit
    restart_count      = $t.Settings.RestartCount
    restart_interval   = $t.Settings.RestartInterval
    wake_to_run        = $t.Settings.WakeToRun
    last_run           = if ($i.LastRunTime) { $i.LastRunTime.ToString('s') } else { $null }
    last_result        = $i.LastTaskResult
    next_run           = if ($i.NextRunTime) { $i.NextRunTime.ToString('s') } else { $null }
    problems           = $problems
    verdict            = if ($problems.Count -eq 0) { 'VALID' } else { 'INVALID' }
}
if ($ReportFile) { $report | ConvertTo-Json -Depth 5 | Out-File -Encoding utf8 $ReportFile }

if ($problems.Count -eq 0) {
    $global:R52TaskValidateResult = 'VALID'
    Write-Output "R52_TASK_VALID - $TaskName in $Mode mode ($($haveTimes.Count) daily triggers$(if ($haveLogonTrigger) { ' + logon trigger' }); LogonType=$logonType; next run $($report.next_run))"
} else {
    $global:R52TaskValidateResult = 'INVALID - ' + ($problems -join '; ')
    Write-Output ("R52_TASK_INVALID - " + ($problems -join '; '))
}
