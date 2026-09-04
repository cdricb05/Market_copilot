<#
.SYNOPSIS
    Release 29 canonical COLLECTION SERVICE manager (install / start / stop /
    restart / status / uninstall).

.DESCRIPTION
    The ONE owner of the continuous information-collection service lifecycle.

    It registers a USER-LEVEL Windows Scheduled Task
    (PaperTrader-InformationCollection) that launches exactly one long-lived
    worker (scripts\run_information_collection_service.py) at logon. No
    administrator rights, no Windows service, no stored login password.

    This script DOES NOT restart or smoke-test the backend. That remains owned
    exclusively by scripts\restart_paper_trader_backend.ps1.

    SAFETY. Installing and starting this service turns INFORMATION COLLECTION
    automation ON. It does NOT turn execution automation on. The worker can
    never create, confirm, fill or cancel an order, approve a proposal, confirm
    a target, run Daily Close, run the full Daily Research Cycle, execute a
    rebalance or promote a model.

.PARAMETER RepoRoot
    Absolute path to the paper_trader repository.

.PARAMETER Action
    Install | Start | Stop | Restart | Status | Recover | Uninstall

    Recover (Release 46.6.2) is THE operator answer to a worker that died
    without being stopped. It is idempotent and singleton-safe: if one logical
    worker is already running it changes nothing and reports so; otherwise it
    clears the singleton lock ONLY when the process table proves no collection
    worker exists anywhere on this machine, then starts exactly one.

    Why it has to exist. On 2026-08-28 the worker was terminated at 13:51:44 ET
    when its interactive logon session ended, so its `finally` never ran and
    its lock stayed on disk with a 30-second-old heartbeat. The relaunch 52
    seconds later (the task's only trigger is a logon) correctly refused to
    become a second worker and exited 3, and nothing tried again: the task has
    no repetition and its NextRunTime was empty. Collection was dead for six
    hours and would have stayed dead. The singleton gate was right; what was
    missing was an authorised way back.

.PARAMETER Execute
    Required for every MUTATING action. Status is read-only and runs without it.

.EXAMPLE
    .\scripts\manage_information_collection.ps1 -RepoRoot C:\Users\binis\paper_trader -Action Status

.EXAMPLE
    .\scripts\manage_information_collection.ps1 -RepoRoot C:\Users\binis\paper_trader -Action Install -Execute
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$RepoRoot,
    [Parameter(Mandatory = $true)]
    [ValidateSet("Install", "Start", "Stop", "Restart", "Status", "Recover",
                 "Uninstall")]
    [string]$Action,
    [switch]$Execute,
    [int]$StartTimeoutSec = 90
)

$ErrorActionPreference = "Stop"

$TASK_NAME       = "PaperTrader-InformationCollection"
$OK_TOKEN        = "COLLECTION_SERVICE_LIVE_OK"
$STOPPED_TOKEN   = "COLLECTION_SERVICE_STOPPED_OK"
$INSTALLED_TOKEN = "COLLECTION_SERVICE_INSTALLED_OK"
$REMOVED_TOKEN   = "COLLECTION_SERVICE_UNINSTALLED_OK"
$BLOCKED_TOKEN   = "COLLECTION_SERVICE_BLOCKED"
$RECOVERED_TOKEN = "COLLECTION_SERVICE_RECOVERED_OK"
$NOOP_TOKEN      = "COLLECTION_SERVICE_RECOVERY_NOT_REQUIRED"

function Write-Section([string]$Title) {
    Write-Host ""
    Write-Host ("=" * 78)
    Write-Host $Title
    Write-Host ("=" * 78)
}
function Info([string]$m) { Write-Host "[collection] $m" }
# A never-started service has no pid, no heartbeat and no next wake. Printing an
# empty string there reads as a broken renderer; the state is named instead.
function Fmt($value, [string]$Absent = "none") {
    if ($null -eq $value) { return $Absent }
    if ($value -is [string] -and $value.Trim() -eq "") { return $Absent }
    return $value
}
function Fail([string]$m) {
    Write-Host ""
    Write-Host "$BLOCKED_TOKEN - $m"
    exit 1
}
function Require-Execute([string]$verb) {
    if (-not $Execute) {
        Write-Host ""
        Write-Host "PREVIEW ONLY - '$verb' is a mutating action."
        Write-Host "Re-run with -Execute to apply it."
        exit 0
    }
}

# ---- absolute paths, validated ------------------------------------------- #
if (-not (Test-Path -LiteralPath $RepoRoot)) { Fail "RepoRoot not found: $RepoRoot" }
$Repo      = (Resolve-Path -LiteralPath $RepoRoot).Path
$PythonExe = Join-Path $Repo ".venv-win\Scripts\python.exe"
$Worker    = Join-Path $Repo "scripts\run_information_collection_service.py"
$StatePy   = Join-Path $Repo "scripts\collection_service_control.py"

if (-not (Test-Path -LiteralPath $PythonExe)) { Fail "python not found: $PythonExe" }
if (-not (Test-Path -LiteralPath $Worker))    { Fail "worker not found: $Worker" }
if (-not (Test-Path -LiteralPath $StatePy))   { Fail "control helper not found: $StatePy" }

# ---- one control helper for every state read/write ------------------------ #
# No fragile inline python -c, no here-string python: a real file with a real
# argument contract.
function Invoke-Control([string[]]$ControlArgs) {
    $out = & $PythonExe $StatePy @ControlArgs 2>&1
    $code = $LASTEXITCODE
    return [pscustomobject]@{ Output = ($out -join "`n"); ExitCode = $code }
}

function Get-ServiceStatus() {
    $r = Invoke-Control @("--action", "status")
    if ($r.ExitCode -ne 0) { return $null }
    try { return $r.Output | ConvertFrom-Json } catch { return $null }
}

function Get-CollectionTask() {
    try { return Get-ScheduledTask -TaskName $TASK_NAME -ErrorAction Stop }
    catch { return $null }
}

function Show-Status([string]$Title) {
    Write-Section $Title
    $task = Get-CollectionTask
    if ($null -eq $task) {
        Info "Scheduled task : NOT INSTALLED ($TASK_NAME)"
    } else {
        $ti = $task | Get-ScheduledTaskInfo
        Info "Scheduled task : $TASK_NAME"
        Info "  state        : $($task.State)"
        Info "  last run     : $($ti.LastRunTime)"
        Info "  last result  : $($ti.LastTaskResult)"
        Info "  next run     : $($ti.NextRunTime)"
    }
    $st = Get-ServiceStatus
    if ($null -eq $st) {
        Info "Service state  : UNREADABLE (control helper returned no JSON)"
        return $null
    }
    Info "Service state  : $($st.service_state)  ($($st.reason))"
    Info "  collection automation : $($st.collection_automation_enabled)"
    Info "  execution automation  : $($st.execution_automation_enabled)  (permanently off)"
    Info "  instance      : $(Fmt $st.instance_id)"
    Info "  worker pid    : $(Fmt $st.worker_pid)   alive: $(Fmt $st.worker_alive 'unknown')"
    Info "  singleton lock: held=$($st.lock_held)  pid=$(Fmt $st.lock_pid)"
    $hb = if ($null -eq $st.heartbeat_age_seconds) { "never" }
          else { "$($st.heartbeat_age_seconds) s" }
    Info "  heartbeat age : $hb"
    # RUNNING answers "is the service up". ACTIVITY answers "is it working" - a
    # long collection pass that keeps advancing is BUSY, not degraded.
    $pg = if ($null -eq $st.progress_age_seconds) { "never" }
          else { "$($st.progress_age_seconds) s" }
    Info "  activity      : $(Fmt $st.worker_activity 'unknown')   ($(Fmt $st.worker_activity_reason ''))"
    Info "  progress age  : $pg   seq $(Fmt $st.progress_seq '0')   step $(Fmt $st.progress_step 'none')"
    Info "  iteration open: $(Fmt $st.iteration_in_flight 'false')   id $(Fmt $st.current_iteration_id 'none')"
    # R55.2 - WHICH RELEASE IS THIS LIVE WORKER RUNNING? A heartbeat proves the
    # process is alive; it says nothing about the code it loaded. The verdict,
    # the reason and the remediation sentence all come from the ONE identity
    # owner (api.runtime_identity) - this script prints them and decides nothing.
    Info "  loaded release: $(Fmt $st.loaded_commit 'UNKNOWN')   captured $(Fmt $st.loaded_captured_at 'never')"
    Info "  source release: $(Fmt $st.source_commit 'UNKNOWN')   branch $(Fmt $st.source_branch 'unknown')   dirty: $(Fmt $st.source_dirty 'unknown')"
    Info "  alignment     : $(Fmt $st.runtime_alignment 'UNKNOWN')   ($(Fmt $st.runtime_alignment_reason ''))"
    if ($st.runtime_alignment_statement) { Info "    $($st.runtime_alignment_statement)" }
    if ($st.runtime_alignment_remediation) { Info "    remediation : $($st.runtime_alignment_remediation)" }
    Info "  iterations    : $($st.loop_count)   restarts: $($st.restart_count)"
    Info "  last iteration: $(Fmt $st.last_iteration_finished_at 'never')"
    Info "  next wake     : $(Fmt $st.next_wake_at 'not scheduled')"
    Info "  sources due   : $($st.healthy_due) / $($st.due_now) healthy; not due $($st.not_due); backoff $($st.backoff); blocked $($st.blocked)"
    Info "  store root    : $($st.store_root)"
    if ($st.credential_summary) { Info "  credentials   : $($st.credential_summary)" }
    return $st
}

function Show-StartupDiagnostics([string]$Reason) {
    Write-Section "COLLECTION SERVICE START DIAGNOSTICS"
    Info "Reason: $Reason"
    $task = Get-CollectionTask
    if ($null -ne $task) {
        $ti = $task | Get-ScheduledTaskInfo
        Info "task state        : $($task.State)"
        Info "task last result  : $($ti.LastTaskResult)"
        Info "task last run     : $($ti.LastRunTime)"
    } else {
        Info "task              : NOT INSTALLED"
    }
    $scan = Get-WorkerScan
    Info "process scan      : $($scan.scanned) python.exe row(s); $($scan.unreadable) with an unreadable command line; query failed: $($scan.failed)"
    if ($scan.unreadable -gt 0) {
        Info "                    (an unreadable command line means THIS SHELL may not inspect that process, not that it is absent)"
    }
    foreach ($p in @($scan.matched)) {
        Info "process           : pid=$($p.ProcessId) parent=$($p.ParentProcessId) image=$($p.ExecutablePath)"
    }
    $st = Get-ServiceStatus
    if ($null -ne $st) {
        Info "service state     : $($st.service_state)  activity $($st.worker_activity)"
        Info "heartbeat         : $($st.heartbeat_at)  (age $($st.heartbeat_age_seconds) s)"
        Info "progress          : $($st.progress_at)  (age $($st.progress_age_seconds) s, seq $($st.progress_seq), step $($st.progress_step))"
        Info "lock owner        : pid=$($st.lock_pid) instance=$($st.lock_instance_id)"
        Info "store root        : $($st.store_root)"
        Info "config path       : $($st.cadence_policy_id)"
        Info "credentials       : $($st.credential_summary)"
        Info "source states     : $($st.source_state_summary)"
        if ($st.stdout_path) { Info "stdout            : $($st.stdout_path)" }
        if ($st.stderr_path) { Info "stderr            : $($st.stderr_path)" }
        foreach ($line in @($st.log_tail))   { Info "  log    | $line" }
        foreach ($line in @($st.stdout_tail)){ Info "  stdout | $line" }
        foreach ($line in @($st.stderr_tail)){ Info "  stderr | $line" }
    } else {
        Info "service state     : UNREADABLE"
    }
}

# PHYSICAL processes whose command line names the canonical worker. This is a
# SNAPSHOT, never a count of workers: `.venv-win\Scripts\python.exe` is the venv
# REDIRECTOR (its version resource reads OriginalFilename "py.exe"), and it
# CreateProcess-es the base interpreter from pyvenv.cfg with a byte-identical
# command line, then waits on it. Every clean Start therefore yields two rows
# here for ONE worker. How many LOGICAL workers those rows are is decided by
# ic.resolve_worker_topology, not by this function.
function Get-WorkerProcesses() {
    return (Get-WorkerScan).matched
}

# RELEASE 55.2.1 - REPORT WHAT THIS SHELL COULD SEE, NOT ONLY WHAT IT MATCHED.
#
# Measured here on 2026-09-03: Win32_Process returned 6 python.exe rows and FOUR
# of them (the backend and collection lineages, both owned by Task Scheduler)
# exposed a NULL CommandLine to this unelevated shell. The old collector's filter
# required a readable command line, so those rows were dropped silently and the
# snapshot arrived EMPTY while the worker was alive, heartbeating and holding the
# singleton lock. Python then said NO_LOGICAL_WORKER and Restart said BLOCKED.
#
# The counters below are the difference between "I looked and saw nothing" and
# "I was not allowed to look". PowerShell reports; Python decides what it means.
function Get-WorkerScan() {
    $matched   = @()
    $scanned   = 0
    $unreadable = 0
    $failed    = $false
    try {
        $all = @(Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction Stop)
        $scanned = $all.Count
        foreach ($p in $all) {
            if (-not $p.CommandLine) {
                # Visible process, unreadable metadata. It MIGHT be the worker;
                # this shell cannot tell, and must not pretend otherwise.
                $unreadable++
                continue
            }
            if ($p.CommandLine -like "*run_information_collection_service.py*") {
                $matched += $p
            }
        }
    } catch {
        $failed = $true
    }
    return [pscustomobject]@{
        matched    = $matched
        scanned    = $scanned
        unreadable = $unreadable
        failed     = $failed
    }
}

# The process snapshot ENVELOPE, in the shape the control helper's stdin
# contract wants: the rows this shell could read, plus what it could not.
function Get-WorkerSnapshotJson() {
    $scan = Get-WorkerScan
    $rows = @()
    foreach ($p in @($scan.matched)) {
        $created = $null
        if ($p.CreationDate) {
            try { $created = $p.CreationDate.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ") } catch { }
        }
        $rows += [pscustomobject]@{
            pid             = $p.ProcessId
            parent_pid      = $p.ParentProcessId
            command_line    = $p.CommandLine
            executable_path = $p.ExecutablePath
            created_at      = $created
        }
    }
    $envelope = [pscustomobject]@{
        rows          = @($rows)
        introspection = [pscustomobject]@{
            scanned_count                  = $scan.scanned
            matched_count                  = @($rows).Count
            unreadable_command_line_count  = $scan.unreadable
            query_failed                   = $scan.failed
        }
    }
    return (ConvertTo-Json -InputObject $envelope -Depth 5 -Compress)
}

# The ONE logical-worker verdict AND the ONE presence verdict. PowerShell
# enumerates; Python decides. This script never re-derives either.
function Get-WorkerTopology() {
    $json = Get-WorkerSnapshotJson
    $out = $json | & $PythonExe $StatePy --action worker-topology 2>&1
    if ($LASTEXITCODE -ne 0) { return $null }
    try { return ($out -join "`n") | ConvertFrom-Json } catch { return $null }
}

# Is the singleton PROVEN? Never re-implemented here - it is the owner's word.
function Test-SingletonProven($Topology) {
    if ($null -eq $Topology) { return $false }
    return [bool]$Topology.singleton_proven
}

function Show-Topology($Topology) {
    if ($null -eq $Topology) {
        Info "worker topology: UNREADABLE (control helper returned no JSON)"
        return
    }
    # R55.2.1 - PRESENCE first, because it is the authoritative answer to "is a
    # worker running". The process snapshot below it is OPTIONAL corroboration.
    Info "worker presence : $($Topology.presence_verdict)"
    Info "  $($Topology.presence_reason)"
    Info "  decided on    : $($Topology.presence_decided_on)   singleton proven: $($Topology.singleton_proven)"
    if ($Topology.presence_advisory) { Info "  advisory      : $($Topology.presence_advisory)" }
    $pr = $Topology.presence
    if ($null -ne $pr) {
        Info "  evidence      : pid $(Fmt $pr.worker_pid) alive=$(Fmt $pr.pid_alive 'unknown')  lock pid $(Fmt $pr.lock_pid)  instance $(Fmt $pr.state_instance_id)"
        Info "                  heartbeat $(Fmt $pr.heartbeat_age_seconds 'never') s (fresh: $($pr.heartbeat_fresh))  iteration open: $($pr.iteration_in_flight) advancing: $($pr.iteration_advancing)"
    }
    Info "  OS metadata   : available=$($Topology.os_metadata_available)   $(Fmt $Topology.snapshot_authority_detail '')"
    Info "worker topology : $($Topology.verdict)   (optional OS process correlation)"
    Info "  $($Topology.reason)"
    Info "  logical workers : $($Topology.logical_worker_count)   physical processes: $($Topology.physical_process_count)"
    Info "  executing pid   : $(Fmt $Topology.executing_pid)"
    foreach ($lin in @($Topology.lineages)) {
        Info ("  lineage root {0}: pids {1} -> executing {2} (owns lock: {3})" -f `
              $lin.root_pid, ((@($lin.pids)) -join ", "), (Fmt $lin.executing_pid), $lin.owns_lock)
        foreach ($exe in @($lin.executable_paths)) { Info "      image: $(Fmt $exe)" }
    }
    Info "  lock owner      : $(Fmt $Topology.lock_pid)  correlated: $($Topology.lock_correlated)"
    Info "  $($Topology.lock_correlation_reason)"
}

# A healthy start is ONE PROVEN worker. R55.2.1: proof comes from the presence
# owner's evidence ladder, not from `healthy`, which was true only when the
# optional command-line correlation happened to be readable. The snapshot is
# re-read a bounded number of times because a redirector that has not yet spawned
# its child is a transient, not a verdict.
function Wait-ForOneLogicalWorker([int]$Attempts = 5) {
    $last = $null
    for ($i = 0; $i -lt $Attempts; $i++) {
        $last = Get-WorkerTopology
        if (Test-SingletonProven $last) { return $last }
        Start-Sleep -Seconds 2
    }
    return $last
}

function Wait-ForHeartbeat([int]$TimeoutSec) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        $st = Get-ServiceStatus
        if ($null -ne $st -and $st.service_state -eq "RUNNING") { return $st }
        Start-Sleep -Seconds 3
    }
    return $null
}

function Install-CollectionTask() {
    $stdout = Join-Path $Repo "paper_trader_collection.stdout.log"
    $stderr = Join-Path $Repo "paper_trader_collection.stderr.log"
    # Release 53.1: the task DEFINITION is owned by exactly ONE installer
    # script (same single-owner rule as the backend restart owner). The old
    # inline registration here produced the Interactive, logon-only task that
    # died with the logon session on 2026-08-28 and never recovered. This
    # manager now delegates; -Force is legal because Install is already an
    # explicit, -Execute-gated operator action.
    $installer = Join-Path $Repo "scripts\install_information_collection_task.ps1"
    if (-not (Test-Path -LiteralPath $installer)) {
        Fail "definition owner not found: $installer"
    }
    & $installer -Force
    if ($global:R531CollectionTaskInstallResult -like 'BLOCKED*') {
        Fail ("task registration blocked: $($global:R531CollectionTaskInstallResult). " +
              "The durable S4U definition needs an ELEVATED PowerShell.")
    }
    Info "stdout log: $stdout"
    Info "stderr log: $stderr"
}

function Stop-Worker() {
    $procs = Get-WorkerProcesses
    if ($procs.Count -eq 0) {
        # R55.2.1 - AN EMPTY SNAPSHOT IS NOT PROOF OF A STOP. When this shell
        # cannot read command lines, the matched set is empty while the worker is
        # very much alive, and returning $true here reported a stop that never
        # happened. Fall back to the AUTHORITATIVE evidence - the pid the worker
        # itself recorded and the pid holding the singleton lock - and stop those.
        $verdict = Get-WorkerTopology
        $known = @()
        if ($null -ne $verdict -and $null -ne $verdict.presence) {
            foreach ($candidate in @($verdict.presence.worker_pid, $verdict.presence.lock_pid)) {
                if ($candidate -and ($known -notcontains $candidate)) { $known += $candidate }
            }
        }
        $live = @($known | Where-Object { $null -ne (Get-Process -Id $_ -ErrorAction SilentlyContinue) })
        if ($live.Count -eq 0) {
            Info "No collection worker process is running."
            return $true
        }
        Info "The process snapshot matched nothing, but the recorded worker pid(s) $($live -join ', ') are alive; stopping those."
        foreach ($stopPid in $live) {
            Info "Stopping worker pid $stopPid ..."
            try { Stop-Process -Id $stopPid -ErrorAction Stop } catch { }
        }
        $deadline = (Get-Date).AddSeconds(45)
        while ((Get-Date) -lt $deadline) {
            $stillAlive = @($live | Where-Object { $null -ne (Get-Process -Id $_ -ErrorAction SilentlyContinue) })
            if ($stillAlive.Count -eq 0) { return $true }
            Start-Sleep -Seconds 2
        }
        return $false
    }
    # Kill the LEAVES first. The venv redirector is a parent that WAITS on the
    # base interpreter, so killing the parent first orphans the child that owns
    # the singleton lock - the whole lineage has to go, deepest member first.
    $pids = @($procs | ForEach-Object { $_.ProcessId })
    $parentPids = @($procs | ForEach-Object { $_.ParentProcessId } |
                    Where-Object { $pids -contains $_ })
    $ordered = @($procs | Where-Object { $parentPids -notcontains $_.ProcessId }) + `
               @($procs | Where-Object { $parentPids -contains $_.ProcessId })
    foreach ($p in $ordered) {
        Info "Stopping worker pid $($p.ProcessId) ($(Fmt $p.ExecutablePath)) ..."
        try { Stop-Process -Id $p.ProcessId -ErrorAction Stop } catch { }
    }
    $deadline = (Get-Date).AddSeconds(45)
    while ((Get-Date) -lt $deadline) {
        if ((Get-WorkerProcesses).Count -eq 0) { return $true }
        Start-Sleep -Seconds 2
    }
    return ((Get-WorkerProcesses).Count -eq 0)
}

# =========================================================================== #
# Actions
# =========================================================================== #
switch ($Action) {

    "Status" {
        $st = Show-Status "COLLECTION SERVICE STATUS (read-only)"
        Write-Section "PROCESS TOPOLOGY (read-only)"
        Show-Topology (Get-WorkerTopology)
        Write-Section "SAFETY"
        Info "Information collection automation : $(if ($st) { $st.collection_automation_enabled } else { 'unknown' })"
        Info "Execution automation              : OFF (permanently, architecture-tested)"
        Info "Broker execution                  : NONE"
        Info "Manual review                     : REQUIRED"
        exit 0
    }

    "Install" {
        Require-Execute "Install"
        Write-Section "INSTALL COLLECTION SERVICE"
        Info "Repository : $Repo"
        Info "Python     : $PythonExe"
        Info "Worker     : $Worker"
        Info "Task       : $TASK_NAME (user-level, at logon, non-admin)"

        Info "Validating the worker can import its owners (no provider call) ..."
        $probe = Invoke-Control @("--action", "preflight")
        if ($probe.ExitCode -ne 0) {
            Write-Host $probe.Output
            Fail "worker preflight failed - the service was NOT installed"
        }
        Write-Host $probe.Output

        Info "Arming INFORMATION COLLECTION automation (execution automation stays OFF) ..."
        $arm = Invoke-Control @("--action", "enable")
        if ($arm.ExitCode -ne 0) { Write-Host $arm.Output; Fail "could not enable collection automation" }

        Install-CollectionTask
        Show-Status "POST-INSTALL STATE" | Out-Null
        Write-Section "RESULT"
        Info "Collection automation : ON"
        Info "Execution automation  : OFF"
        Write-Host ""
        Write-Host $INSTALLED_TOKEN
        Write-Host "Next: -Action Start -Execute"
        exit 0
    }

    "Start" {
        Require-Execute "Start"
        Write-Section "START COLLECTION SERVICE"
        if ($null -eq (Get-CollectionTask)) {
            Fail "the scheduled task is not installed. Run -Action Install -Execute first."
        }
        $existing = Get-WorkerTopology
        if ($null -ne $existing -and $existing.presence_verdict -ne "NO_WORKER") {
            if (-not (Test-SingletonProven $existing)) {
                Show-Topology $existing
                Show-StartupDiagnostics $existing.presence_reason
                Fail "$($existing.presence_verdict): $($existing.presence_reason)"
            }
            Info "A worker is already running (pid $(Fmt $existing.presence.lock_pid)). Singleton respected."
            Show-Topology $existing
            $st = Show-Status "COLLECTION SERVICE STATUS"
            if ($null -ne $st -and $st.service_state -eq "RUNNING") {
                Write-Host ""; Write-Host $OK_TOKEN; exit 0
            }
            Show-StartupDiagnostics "a worker process exists but the service is not reporting RUNNING"
            Fail "worker present but not healthy"
        }
        Start-ScheduledTask -TaskName $TASK_NAME
        Info "Task started; waiting up to $StartTimeoutSec s for the worker heartbeat ..."
        $st = Wait-ForHeartbeat $StartTimeoutSec
        if ($null -eq $st) {
            Show-StartupDiagnostics "no RUNNING heartbeat within $StartTimeoutSec seconds"
            Fail "collection service did not become live"
        }
        $topology = Wait-ForOneLogicalWorker
        if ($null -eq $topology) {
            Show-StartupDiagnostics "the worker topology could not be read"
            Fail "worker topology unreadable"
        }
        if (-not (Test-SingletonProven $topology)) {
            Show-Topology $topology
            Show-StartupDiagnostics $topology.presence_reason
            Fail "$($topology.presence_verdict): $($topology.presence_reason)"
        }
        Show-Status "COLLECTION SERVICE STATUS" | Out-Null
        Show-Topology $topology
        Write-Section "VERIFIED"
        Info "worker pid       : $($st.worker_pid)"
        Info "worker presence  : $($topology.presence_verdict) (decided on $($topology.presence_decided_on))"
        if ($topology.presence_advisory) { Info "advisory         : $($topology.presence_advisory)" }
        Info "singleton lock   : held=$($st.lock_held)"
        Info "heartbeat age    : $($st.heartbeat_age_seconds) s"
        Info "execution autom. : OFF"
        Write-Host ""
        Write-Host $OK_TOKEN
        exit 0
    }

    "Stop" {
        Require-Execute "Stop"
        Write-Section "STOP COLLECTION SERVICE"
        Info "This stops COLLECTION only. The backend on port 8001 is untouched."
        try { Stop-ScheduledTask -TaskName $TASK_NAME -ErrorAction SilentlyContinue } catch { }
        $clean = Stop-Worker
        if (-not $clean) { Fail "a collection worker process did not exit" }
        Invoke-Control @("--action", "mark-stopped") | Out-Null
        # The whole LINEAGE must be gone, not just the process that owned the lock.
        # R55.2.1: judged on PRESENCE, so an unreadable snapshot can neither
        # certify a stop that did not happen nor fail one that did.
        $after = Get-WorkerTopology
        if ($null -ne $after -and $after.presence_verdict -ne "NO_WORKER") {
            Show-Topology $after
            Fail "a collection worker survived Stop: $($after.presence_reason)"
        }
        Show-Status "POST-STOP STATE" | Out-Null
        Show-Topology $after
        Write-Host ""
        Write-Host $STOPPED_TOKEN
        exit 0
    }

    "Restart" {
        Require-Execute "Restart"
        Write-Section "RESTART COLLECTION SERVICE"
        $before = Get-ServiceStatus
        if ($null -ne $before) {
            Info "before: instance=$($before.instance_id) pid=$($before.worker_pid) iterations=$($before.loop_count)"
        }
        try { Stop-ScheduledTask -TaskName $TASK_NAME -ErrorAction SilentlyContinue } catch { }
        if (-not (Stop-Worker)) { Fail "a collection worker process did not exit" }
        Invoke-Control @("--action", "mark-stopped") | Out-Null
        Start-Sleep -Seconds 2
        Start-ScheduledTask -TaskName $TASK_NAME
        Info "Waiting up to $StartTimeoutSec s for the new worker heartbeat ..."
        $after = Wait-ForHeartbeat $StartTimeoutSec
        if ($null -eq $after) {
            Show-StartupDiagnostics "no RUNNING heartbeat after restart"
            Fail "collection service did not come back"
        }
        # R55.2.1 - RESTART SUCCEEDS WHEN THE SINGLETON IS PROVEN, and BLOCKS only
        # when it genuinely cannot be. Before this repair the gate required the
        # topology's `healthy` flag, which in turn required the OPTIONAL
        # command-line correlation to be readable; on this machine it is not, so a
        # restart that produced a new instance, a live pid, a held lock, a fresh
        # heartbeat and an ALIGNED loaded release still ended
        # COLLECTION_SERVICE_BLOCKED.
        $topology = Wait-ForOneLogicalWorker
        if (-not (Test-SingletonProven $topology)) {
            Show-Topology $topology
            Show-StartupDiagnostics "the restarted service is not exactly one proven worker"
            Fail "singleton violated after restart: $(if ($topology) { $topology.presence_reason } else { 'worker presence unreadable' })"
        }
        Show-Topology $topology
        Write-Section "RESTART VERIFIED"
        if ($null -ne $before) { Info "prior instance : $($before.instance_id)" }
        Info "new instance   : $($after.instance_id)"
        Info "worker presence: $($topology.presence_verdict) (decided on $($topology.presence_decided_on))"
        if ($topology.presence_advisory) { Info "advisory       : $($topology.presence_advisory)" }
        Info "restart count  : $($after.restart_count)"
        Info "watermarks     : preserved (source runtime state is durable)"
        Write-Host ""
        Write-Host $OK_TOKEN
        exit 0
    }

    "Recover" {
        # Release 46.6.2. Idempotent, singleton-safe, bounded and auditable.
        # It NEVER kills a worker, NEVER changes the scheduled task and NEVER
        # touches execution automation - it only puts back the ONE worker that
        # died without being stopped.
        Require-Execute "Recover"
        Write-Section "RECOVER COLLECTION SERVICE"

        $rec = Invoke-Control @("--action", "recovery-state")
        if ($rec.ExitCode -ne 0) { Write-Host $rec.Output; Fail "could not read the recovery state" }
        $state = $null
        try { $state = $rec.Output | ConvertFrom-Json } catch { }
        if ($null -eq $state) { Write-Host $rec.Output; Fail "recovery state was not JSON" }
        Info "recovery state  : $($state.recovery_state)"
        Info "  $($state.why)"
        Info "recovery required: $($state.recovery_required)"

        if ($state.recovery_state -eq "AUTOMATION_DISABLED") {
            Fail "information-collection automation is not enabled. Run -Action Install -Execute first."
        }
        if ($null -eq (Get-CollectionTask)) {
            Fail "the scheduled task is not installed. Run -Action Install -Execute first."
        }

        # 1. Is a worker already running? Then recovery is a NO-OP, by design:
        #    calling Recover twice must never produce two workers.
        $before = Get-WorkerTopology
        Show-Topology $before
        if (Test-SingletonProven $before) {
            $st = Show-Status "COLLECTION SERVICE STATUS"
            if ($null -ne $st -and $st.service_state -eq "RUNNING") {
                Write-Host ""
                Write-Host $NOOP_TOKEN
                exit 0
            }
            Info "One worker exists but the service is not RUNNING; use -Action Restart -Execute."
            Show-StartupDiagnostics "a worker process exists but the service is not reporting RUNNING"
            Fail "worker present but not healthy - Recover will not kill a running worker"
        }
        if ($null -ne $before -and $before.presence_verdict -eq "MULTIPLE_WORKERS") {
            Fail "MULTIPLE_WORKERS: $($before.presence_reason) - Recover refuses to add to a violation"
        }
        # R55.2.1 - anything that is not a PROVEN absence fails closed. Recovery
        # starts a worker, so it may act only when it is certain none is running.
        if ($null -eq $before -or $before.presence_verdict -ne "NO_WORKER") {
            Fail "worker presence could not be resolved; recovery fails closed rather than guessing"
        }

        # 2. NO worker exists. Clear a lock the dead worker never released -
        #    the control helper re-proves that from the same snapshot and
        #    refuses if anything is running.
        Info "No collection worker is running. Checking the singleton lock ..."
        $snapshot = Get-WorkerSnapshotJson
        $clear = $snapshot | & $PythonExe $StatePy --action clear-abandoned-lock 2>&1
        $clearExit = $LASTEXITCODE
        Write-Host ($clear -join "`n")
        if ($clearExit -ne 0) { Fail "the abandoned singleton lock could not be cleared" }

        # 3. Start exactly one worker through the SAME path Start uses.
        Info "Starting one worker ..."
        Start-ScheduledTask -TaskName $TASK_NAME
        $st = Wait-ForHeartbeat $StartTimeoutSec
        if ($null -eq $st) {
            Show-StartupDiagnostics "no RUNNING heartbeat within $StartTimeoutSec seconds after recovery"
            Fail "collection service did not come back"
        }
        $after = Wait-ForOneLogicalWorker
        if ($null -eq $after -or -not $after.healthy) {
            Show-Topology $after
            Show-StartupDiagnostics "the recovered service is not exactly one logical worker"
            Fail "singleton violated after recovery: $(if ($after) { $after.reason } else { 'topology unreadable' })"
        }
        Show-Topology $after
        Show-Status "POST-RECOVERY STATE" | Out-Null
        Write-Section "RECOVERY VERIFIED"
        Info "worker pid       : $($st.worker_pid)"
        Info "logical workers  : $($after.logical_worker_count) (physical processes: $($after.physical_process_count))"
        Info "watermarks       : preserved (source runtime state is durable)"
        Info "execution autom. : OFF"
        Write-Host ""
        Write-Host $RECOVERED_TOKEN
        exit 0
    }

    "Uninstall" {
        Require-Execute "Uninstall"
        Write-Section "UNINSTALL COLLECTION SERVICE"
        Info "Removing the collection task ONLY. Event evidence is never deleted."
        try { Stop-ScheduledTask -TaskName $TASK_NAME -ErrorAction SilentlyContinue } catch { }
        Stop-Worker | Out-Null
        # Stop-Worker TERMINATES the process, so the worker's own graceful
        # release never runs and its singleton lock is left on disk naming a pid
        # that no longer exists. -Action Stop records that; Uninstall did not, so
        # an Uninstall -> Install -> Start inside the 15-minute takeover window
        # was refused by the single-flight gate against a dead holder. Whoever
        # stopped the worker owes the state the same clean-shutdown marker.
        Invoke-Control @("--action", "mark-stopped") | Out-Null
        if ($null -ne (Get-CollectionTask)) {
            Unregister-ScheduledTask -TaskName $TASK_NAME -Confirm:$false
            Info "Task removed: $TASK_NAME"
        } else {
            Info "Task was not installed."
        }
        Invoke-Control @("--action", "disable") | Out-Null
        Show-Status "POST-UNINSTALL STATE" | Out-Null
        Write-Section "PRESERVED"
        Info "Event fabric, immutable events, watermarks and iteration history are intact."
        Write-Host ""
        Write-Host $REMOVED_TOKEN
        exit 0
    }
}
