<#
.SYNOPSIS
    Release 59 canonical AUTONOMOUS RESEARCH WORKER manager
    (status / validate / install / start / stop / restart / uninstall).

.DESCRIPTION
    The ONE operator entry point for the lifecycle of the persistent
    autonomous researcher. It owns no research rule, no timing rule and no
    task definition: the task definition belongs to
    scripts\install_research_runtime_task.ps1 (the ONLY script in this
    repository permitted to register that task with the Windows scheduler,
    which the architecture audit enforces), validation belongs to
    scripts\validate_research_runtime_task.ps1, and disabling belongs to
    scripts\disable_research_runtime_task.ps1. This script DELEGATES to those
    owners and adds only what none of them covers: the state of the running
    WORKER, as distinct from the state of the TASK.

    Why the distinction matters. A scheduled task can be Ready, enabled and
    correct while no research has happened for a week, because the task is
    the thing that starts the worker and the worker is the thing that does
    the work. Release 46.6.2 lost six hours of collection to exactly that
    gap: the singleton gate was right, the task was fine, and nothing was
    running. Status therefore reports both, from the worker's own heartbeat.

    SAFETY. Starting this worker turns RESEARCH automation on. It does NOT
    turn execution automation on. Nothing reachable from the worker can
    create, confirm, fill or cancel an order, approve a proposal, confirm a
    target, run Daily Close, execute a rebalance, promote a model or
    activate a sleeve. A forward-confirmed challenger raises
    CHALLENGER_WARRANTS_GOVERNED_REVIEW for a human and stops there.

    SOURCE SAFETY. A persistent service may only run from the deployed,
    committed checkout. Every mutating action against a different RepoRoot
    is refused with RESEARCH_WORKER_BLOCKED, so an uncommitted development
    worktree cannot be promoted into a service by accident.

.PARAMETER RepoRoot
    Absolute path to the paper_trader repository the worker should run from.

.PARAMETER Action
    Status | Validate | Install | Start | Stop | Restart | Uninstall
    Status and Validate are READ-ONLY and need no -Execute.

.PARAMETER Execute
    Required for every MUTATING action (Install, Start, Stop, Restart,
    Uninstall). Without it the action is described and refused.

.EXAMPLE
    .\scripts\manage_research_runtime.ps1 -Action Status

.EXAMPLE
    .\scripts\manage_research_runtime.ps1 -RepoRoot C:\Users\binis\paper_trader -Action Install -Execute
#>
[CmdletBinding()]
param(
    [string]$RepoRoot = 'C:\Users\binis\paper_trader',
    [ValidateSet('Status', 'Validate', 'Install', 'Start', 'Stop', 'Restart',
                 'Uninstall')]
    [string]$Action = 'Status',
    [switch]$Execute,
    [string]$TaskName = 'PaperTrader-ResearchRuntime',
    [string]$PythonExe = 'C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe',
    [int]$StopTimeoutSec = 120
)

$DEPLOYED_ROOT = 'C:\Users\binis\paper_trader'

$STATUS_TOKEN    = 'RESEARCH_WORKER_STATUS_OK'
$VALID_TOKEN     = 'RESEARCH_WORKER_TASK_VALID'
$STARTED_TOKEN   = 'RESEARCH_WORKER_STARTED_OK'
$STOPPED_TOKEN   = 'RESEARCH_WORKER_STOPPED_OK'
$INSTALLED_TOKEN = 'RESEARCH_WORKER_INSTALLED_OK'
$REMOVED_TOKEN   = 'RESEARCH_WORKER_UNINSTALLED_OK'
$BLOCKED_TOKEN   = 'RESEARCH_WORKER_BLOCKED'

$global:R59ResearchWorkerResult = $null

function Info([string]$m) { Write-Host "[research-worker] $m" }
function Fmt($value, [string]$Absent = 'none') {
    if ($null -eq $value) { return $Absent }
    if ($value -is [string] -and $value.Trim() -eq '') { return $Absent }
    return $value
}
function Write-Blocked([string]$Reason) {
    $global:R59ResearchWorkerResult = "BLOCKED - $Reason"
    Write-Output "$BLOCKED_TOKEN - $Reason"
}

# --- guards ---------------------------------------------------------------- #
$mutating = @('Install', 'Start', 'Stop', 'Restart', 'Uninstall')
if ($mutating -contains $Action -and -not $Execute) {
    Write-Blocked "$Action is a mutating action and requires -Execute"
    return
}
# Stop is the one mutating action a development worktree may perform: it can
# only ever REDUCE what is running, and refusing it would leave an operator
# unable to stop a worker they can see.
$sourceSensitive = @('Install', 'Start', 'Restart', 'Uninstall')
if ($sourceSensitive -contains $Action -and
    $RepoRoot.TrimEnd('\') -ne $DEPLOYED_ROOT.TrimEnd('\')) {
    Write-Blocked ("$Action refused: RepoRoot '$RepoRoot' is " +
                   "not the deployed checkout ('$DEPLOYED_ROOT'). " +
                   "A persistent research service may not run from a " +
                   "development worktree")
    return
}

function Get-TaskRow {
    try { $t = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop }
    catch { return $null }
    $i = Get-ScheduledTaskInfo -TaskName $TaskName
    return [PSCustomObject]@{
        Present     = $true
        State       = [string]$t.State
        Enabled     = $t.Settings.Enabled
        Execute     = $t.Actions[0].Execute
        Arguments   = $t.Actions[0].Arguments
        LogonType   = [string]$t.Principal.LogonType
        TimeLimit   = $t.Settings.ExecutionTimeLimit
        Instances   = [string]$t.Settings.MultipleInstances
        LastRun     = $(if ($i.LastRunTime) { $i.LastRunTime.ToString('s') } else { $null })
        LastResult  = $i.LastTaskResult
        NextRun     = $(if ($i.NextRunTime) { $i.NextRunTime.ToString('s') } else { $null })
    }
}

function Get-WorkerRow {
    # The worker reports itself. This never starts a process that researches:
    # --mode status only reads the persisted status document and the lease.
    if (-not (Test-Path $PythonExe)) { return $null }
    $entry = Join-Path $RepoRoot 'scripts\run_research_runtime.py'
    if (-not (Test-Path $entry)) { return $null }
    try {
        $raw = & $PythonExe $entry --mode status
        if ($LASTEXITCODE -ne 0) { return $null }
        return ($raw -join "`n") | ConvertFrom-Json
    } catch { return $null }
}

# --- actions --------------------------------------------------------------- #
switch ($Action) {

    'Status' {
        $task = Get-TaskRow
        $w = Get-WorkerRow
        Write-Host ''
        Write-Host ('=' * 78)
        Write-Host 'AUTONOMOUS RESEARCH WORKER'
        Write-Host ('=' * 78)
        if ($null -eq $task) {
            Info "task           : NOT INSTALLED ($TaskName)"
        } else {
            Info "task           : $($task.State) (enabled=$($task.Enabled), logon=$($task.LogonType))"
            Info "task action    : $(Fmt $task.Arguments)"
            Info "task last/next : $(Fmt $task.LastRun) / $(Fmt $task.NextRun) (result $(Fmt $task.LastResult))"
        }
        if ($null -eq $w) {
            Info 'worker         : UNREADABLE (no python, no entrypoint, or no status yet)'
        } else {
            Info "worker state   : $(Fmt $w.worker_state)"
            Info "worker source  : $(Fmt $w.source_identity.repo_root) @ $(Fmt $w.source_identity.commit_short) dirty=$(Fmt $w.source_identity.dirty)"
            Info "started / beat : $(Fmt $w.started_at) / $(Fmt $w.last_heartbeat)"
            Info "lease live     : $(Fmt $w.lease_is_live)  holder=$(Fmt $w.lease.holder) pid=$(Fmt $w.lease.pid)"
            Info "current lane   : $(Fmt $w.current_lane)"
            Info "queue          : ready=$(Fmt $w.queue_ready) runnable=$(Fmt $w.queue_runnable_now) running=$(Fmt $w.queue_running) blocked=$(Fmt $w.queue_blocked)"
            Info "cumulative     : hypotheses=$(Fmt $w.cumulative_hypotheses) burden=$(Fmt $w.cumulative_search_burden) families=$(Fmt $w.distinct_families)"
            Info "sleep/stop     : $(Fmt $w.stop_or_sleep_reason)  next wake $(Fmt $w.next_planned_wake)"
            Info "maturation     : $(Fmt $w.maturation.reason)"
            Info "latest error   : $(Fmt $w.latest_error)"
        }
        $global:R59ResearchWorkerResult = 'STATUS'
        Write-Output "$STATUS_TOKEN - task=$(if ($task) { $task.State } else { 'NOT_INSTALLED' }) worker=$(if ($w) { $w.worker_state } else { 'UNREADABLE' })"
        return
    }

    'Validate' {
        # Delegate to the canonical validator; add nothing of our own.
        $validator = Join-Path $RepoRoot 'scripts\validate_research_runtime_task.ps1'
        if (-not (Test-Path $validator)) {
            Write-Blocked "canonical validator not found at $validator"
            return
        }
        & $validator -TaskName $TaskName -Mode Persistent
        $global:R59ResearchWorkerResult = $global:R52TaskValidateResult
        if ($global:R52TaskValidateResult -eq 'VALID') {
            Write-Output "$VALID_TOKEN - $TaskName satisfies the persistent-runtime contract"
        }
        return
    }

    'Install' {
        $installer = Join-Path $RepoRoot 'scripts\install_research_runtime_task.ps1'
        if (-not (Test-Path $installer)) {
            Write-Blocked "canonical installer not found at $installer"
            return
        }
        Info 'delegating to the canonical task installer (Persistent mode)'
        & $installer -TaskName $TaskName -Mode Persistent -Force
        $global:R59ResearchWorkerResult = $global:R52TaskInstallResult
        if ("$($global:R52TaskInstallResult)" -like 'BLOCKED*') {
            Write-Blocked "the canonical installer refused: $($global:R52TaskInstallResult)"
            return
        }
        Write-Output "$INSTALLED_TOKEN - $TaskName registered for persistent operation ($($global:R52TaskInstallResult))"
        return
    }

    'Start' {
        $task = Get-TaskRow
        if ($null -eq $task) {
            Write-Blocked "$TaskName is not installed; run -Action Install -Execute first"
            return
        }
        $before = Get-WorkerRow
        if ($null -ne $before -and $before.lease_is_live) {
            $global:R59ResearchWorkerResult = 'ALREADY_RUNNING'
            Write-Output "$STARTED_TOKEN - a worker already holds the lease (pid $($before.lease.pid)); nothing was started"
            return
        }
        Start-ScheduledTask -TaskName $TaskName
        $deadline = (Get-Date).AddSeconds(60)
        while ((Get-Date) -lt $deadline) {
            Start-Sleep -Seconds 3
            $w = Get-WorkerRow
            if ($null -ne $w -and $w.lease_is_live) {
                $global:R59ResearchWorkerResult = 'STARTED'
                Write-Output "$STARTED_TOKEN - worker $(Fmt $w.worker_identity.instance_id) is live (pid $(Fmt $w.lease.pid))"
                return
            }
        }
        Write-Blocked 'the task was started but no worker took the lease within 60s; check the runtime log'
        return
    }

    'Stop' {
        try { Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue } catch { }
        $w = Get-WorkerRow
        if ($null -ne $w -and $w.lease.pid) {
            $deadline = (Get-Date).AddSeconds($StopTimeoutSec)
            while ((Get-Date) -lt $deadline) {
                $again = Get-WorkerRow
                if ($null -eq $again -or -not $again.lease_is_live) { break }
                Start-Sleep -Seconds 3
            }
            $again = Get-WorkerRow
            if ($null -ne $again -and $again.lease_is_live) {
                try { Stop-Process -Id $again.lease.pid -ErrorAction Stop } catch { }
            }
        }
        $global:R59ResearchWorkerResult = 'STOPPED'
        Write-Output "$STOPPED_TOKEN - no live worker lease remains; the queue, memory and graveyard are untouched"
        return
    }

    'Restart' {
        & $PSCommandPath -RepoRoot $RepoRoot -Action Stop -Execute -TaskName $TaskName -PythonExe $PythonExe
        & $PSCommandPath -RepoRoot $RepoRoot -Action Start -Execute -TaskName $TaskName -PythonExe $PythonExe
        return
    }

    'Uninstall' {
        # Disabling is delegated; this script deletes no evidence and no task
        # definition, exactly like the R52 disable owner it calls.
        $disabler = Join-Path $RepoRoot 'scripts\disable_research_runtime_task.ps1'
        if (-not (Test-Path $disabler)) {
            Write-Blocked "canonical disable owner not found at $disabler"
            return
        }
        & $PSCommandPath -RepoRoot $RepoRoot -Action Stop -Execute -TaskName $TaskName -PythonExe $PythonExe
        & $disabler -TaskName $TaskName
        $global:R59ResearchWorkerResult = 'DISABLED'
        Write-Output "$REMOVED_TOKEN - $TaskName disabled; no task definition, ledger, graveyard or challenger was deleted"
        return
    }
}
