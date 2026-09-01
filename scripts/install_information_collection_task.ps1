# =============================================================================
# scripts\install_information_collection_task.ps1  (Release 53.1)
#
# The ONE owner of the DURABLE PaperTrader-InformationCollection task
# definition. The R29 manager (manage_information_collection.ps1) keeps the
# lifecycle verbs (Start/Stop/Recover/Status); its Install now DELEGATES the
# task registration to this script so there is exactly one definition owner.
#
# WHY THIS DEFINITION. On 2026-08-28 the collector died with its interactive
# logon session; the relaunch (logon-only trigger) hit the still-warm
# single-flight lock, exited 3, and NOTHING ever tried again - the task had no
# periodic trigger, no logged-off capability and NextRunTime was empty.
# Collection stayed dead for days. Release 53 fixed the code half
# (acquire_service_lock_with_wait waits out a PROVABLY DEAD holder); this
# script fixes the operating half:
#
#     TaskName : PaperTrader-InformationCollection
#     Action   : <venv python> scripts\run_information_collection_service.py
#                --interval-seconds 60
#     Triggers : AtStartup (2-minute delay)
#                + ONE daily-anchored repetition trigger every 30 minutes,
#                  indefinitely (the PERIODIC RECOVERY: while the long-lived
#                  worker runs, MultipleInstances=IgnoreNew makes every firing
#                  a no-op; the moment the worker dies, the next firing
#                  relaunches it within 30 minutes, logged on or not)
#     Principal: S4U (runs while logged off; no stored password)
#     Settings : IgnoreNew, StartWhenAvailable, no execution time limit
#                (long-lived worker), RestartCount 3 / 5 min.
#
# SINGLETON SAFETY. The task-level half (IgnoreNew) plus the worker's own
# single-flight lock remain the singleton guarantee; this script NEVER starts
# a worker process itself and can never create a duplicate collector.
#
# Idempotent and explicit, exactly like the R52 installer:
#   - full-definition comparison (Action, WorkingDirectory, trigger set,
#     Settings, Principal); UNCHANGED / BLOCKED_PRINCIPAL /
#     BLOCKED_DEFINITION / MIGRATE / INSTALL decisions;
#   - migrating an EXISTING task registers with the requested logon type ONLY
#     (S4U needs an ELEVATED shell); the Interactive fallback exists only for
#     a FRESH install, and it says so;
#   - -DecisionProbe <path|ABSENT> : hermetic test mode - reads an
#     existing-task snapshot from JSON (literal ABSENT = no task), prints the
#     decision as JSON, touches NOTHING.
#
# SAFETY. Information-collection automation only. The worker can never create
# an order, approve a proposal, run Daily Close, or promote a model.
#
# Reports (no exit statements):
#   printed token   : R53_1_COLLECTION_TASK_INSTALLED / _MIGRATED /
#                     _UNCHANGED / R53_1_COLLECTION_TASK_INSTALL_BLOCKED - <r>
#   $global:R531CollectionTaskInstallResult
# =============================================================================
[CmdletBinding()]
param(
    [string]$TaskName = 'PaperTrader-InformationCollection',
    [string]$PythonExe = 'C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe',
    [string]$WorkerScript = 'C:\Users\binis\paper_trader\scripts\run_information_collection_service.py',
    [string]$WorkingDirectory = 'C:\Users\binis\paper_trader',
    [int]$IntervalSeconds = 60,
    [int]$RecoveryRepetitionMinutes = 30,
    [ValidateSet('S4U', 'Interactive')][string]$PreferredLogonType = 'S4U',
    [string]$EvidenceFile = '',
    [string]$DecisionProbe = '',
    [switch]$Force
)

$global:R531CollectionTaskInstallResult = $null

function Write-Blocked([string]$Reason) {
    $global:R531CollectionTaskInstallResult = "BLOCKED - $Reason"
    Write-Output "R53_1_COLLECTION_TASK_INSTALL_BLOCKED - $Reason"
}

$arguments = "`"$WorkerScript`" --interval-seconds $IntervalSeconds"

# ---- the ONE desired definition (every compared field, explicit) ----------- #
function Get-R531DesiredDefinition {
    return [PSCustomObject]@{
        Execute            = $PythonExe
        Arguments          = $arguments
        WorkingDirectory   = $WorkingDirectory
        # Trigger CONTRACT: exactly one boot trigger and exactly one
        # time-anchored trigger carrying an indefinite repetition at the
        # recovery cadence. Wall-clock anchor time is NOT compared (any
        # anchor works; the repetition is what matters).
        BootTriggers       = 1
        RepetitionTriggers = 1
        RepetitionInterval = ('PT{0}M' -f $RecoveryRepetitionMinutes)
        Enabled            = $true
        StartWhenAvailable = $true
        MultipleInstances  = 'IgnoreNew'
        ExecutionTimeLimit = 'PT0S'          # long-lived worker: no limit
        RestartCount       = 3
        RestartInterval    = 'PT5M'
        UserId             = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
        LogonType          = $PreferredLogonType
    }
}

function Get-TriggerKind($t) {
    $type = [string]$t.Type
    if ($type -match 'Boot') { return 'BOOT' }
    $rep = ''
    if ($t.PSObject.Properties['RepetitionInterval']) {
        $rep = [string]$t.RepetitionInterval
    } elseif ($t.PSObject.Properties['Repetition'] -and $t.Repetition) {
        $rep = [string]$t.Repetition.Interval
    }
    if ($rep) { return "REPETITION:$rep" }
    return "OTHER:$type"
}

# ---- full-definition comparison. A missing field is a mismatch. ------------ #
function Get-R531DefinitionMismatches($existing, $desired) {
    $mm = @()
    if ([string]$existing.Action.Execute -ne [string]$desired.Execute) {
        $mm += "Action.Execute '$($existing.Action.Execute)' != '$($desired.Execute)'"
    }
    if ([string]$existing.Action.Arguments -ne [string]$desired.Arguments) {
        $mm += "Action.Arguments '$($existing.Action.Arguments)' != '$($desired.Arguments)'"
    }
    if ([string]$existing.Action.WorkingDirectory -ne [string]$desired.WorkingDirectory) {
        $mm += "WorkingDirectory '$($existing.Action.WorkingDirectory)' != '$($desired.WorkingDirectory)'"
    }
    $kinds = @($existing.Triggers | ForEach-Object { Get-TriggerKind $_ })
    $boot = @($kinds | Where-Object { $_ -eq 'BOOT' })
    $rep  = @($kinds | Where-Object { $_ -eq "REPETITION:$($desired.RepetitionInterval)" })
    $other = @($kinds | Where-Object { $_ -ne 'BOOT' -and
                                       $_ -ne "REPETITION:$($desired.RepetitionInterval)" })
    if ($boot.Count -ne [int]$desired.BootTriggers) {
        $mm += "boot triggers $($boot.Count) != $($desired.BootTriggers)"
    }
    if ($rep.Count -ne [int]$desired.RepetitionTriggers) {
        $mm += ("repetition triggers at $($desired.RepetitionInterval): " +
                "$($rep.Count) != $($desired.RepetitionTriggers)")
    }
    if ($other.Count -gt 0) {
        $mm += ("unexpected trigger(s): " + ($other -join ', '))
    }
    $disabledTriggers = @($existing.Triggers | Where-Object { -not [bool]$_.Enabled })
    if ($disabledTriggers.Count -gt 0) { $mm += "$($disabledTriggers.Count) trigger(s) disabled" }
    if ([bool]$existing.Enabled -ne [bool]$desired.Enabled) {
        $mm += "Enabled $($existing.Enabled) != $($desired.Enabled)"
    }
    if ([bool]$existing.Settings.StartWhenAvailable -ne [bool]$desired.StartWhenAvailable) {
        $mm += "StartWhenAvailable $($existing.Settings.StartWhenAvailable) != $($desired.StartWhenAvailable)"
    }
    if ([string]$existing.Settings.MultipleInstances -ne [string]$desired.MultipleInstances) {
        $mm += "MultipleInstances '$($existing.Settings.MultipleInstances)' != '$($desired.MultipleInstances)'"
    }
    $etl = [string]$existing.Settings.ExecutionTimeLimit
    if ($etl -ne $desired.ExecutionTimeLimit -and $etl -ne '') {
        $mm += "ExecutionTimeLimit '$etl' != '$($desired.ExecutionTimeLimit)'"
    }
    if ([int]$existing.Settings.RestartCount -ne [int]$desired.RestartCount) {
        $mm += "RestartCount $($existing.Settings.RestartCount) != $($desired.RestartCount)"
    }
    if ([string]$existing.Settings.RestartInterval -ne [string]$desired.RestartInterval) {
        $mm += "RestartInterval '$($existing.Settings.RestartInterval)' != '$($desired.RestartInterval)'"
    }
    $haveUser = ([string]$existing.Principal.UserId -split '\\')[-1].ToLowerInvariant()
    $wantUser = ([string]$desired.UserId -split '\\')[-1].ToLowerInvariant()
    if ($haveUser -ne $wantUser) {
        $mm += "Principal.UserId '$($existing.Principal.UserId)' != '$($desired.UserId)'"
    }
    if ([string]$existing.Principal.LogonType -ne [string]$desired.LogonType) {
        $mm += "Principal.LogonType=$($existing.Principal.LogonType), requested=$($desired.LogonType)"
    }
    # Plain return on purpose (PS 5.1 pipeline unrolling; see R52 installer).
    return $mm
}

function Get-R531InstallDecision($existing, $desired, [bool]$ForceRequested) {
    if ($null -eq $existing) {
        return [PSCustomObject]@{ decision = 'INSTALL'; mismatches = @() }
    }
    $mm = @(Get-R531DefinitionMismatches $existing $desired)
    if ($mm.Count -eq 0) {
        return [PSCustomObject]@{ decision = 'UNCHANGED'; mismatches = @() }
    }
    if ($ForceRequested) {
        return [PSCustomObject]@{ decision = 'MIGRATE'; mismatches = $mm }
    }
    $principalMm = @($mm | Where-Object { $_ -like '*Principal.LogonType*' })
    if ($principalMm.Count -gt 0) {
        return [PSCustomObject]@{ decision = 'BLOCKED_PRINCIPAL'; mismatches = $mm }
    }
    return [PSCustomObject]@{ decision = 'BLOCKED_DEFINITION'; mismatches = $mm }
}

# ---- hermetic decision probe (no scheduler, no process, no file write) ----- #
if ($DecisionProbe) {
    $probeExisting = $null
    if ($DecisionProbe -ne 'ABSENT') {
        $probeExisting = Get-Content -Path $DecisionProbe -Raw | ConvertFrom-Json
    }
    $probeDecision = Get-R531InstallDecision $probeExisting (Get-R531DesiredDefinition) $Force.IsPresent
    $global:R531CollectionTaskInstallResult = "PROBE - $($probeDecision.decision)"
    $probeDecision | Add-Member -NotePropertyName requested_logon_type -NotePropertyValue $PreferredLogonType
    $probeDecision | ConvertTo-Json -Depth 4 | Write-Output
    return
}

if (-not (Test-Path $PythonExe)) { Write-Blocked "python not found at $PythonExe"; return }
if (-not (Test-Path $WorkerScript)) { Write-Blocked "worker script not found at $WorkerScript"; return }

function Get-TaskSnapshot([string]$Name) {
    try { $t = Get-ScheduledTask -TaskName $Name -ErrorAction Stop } catch { return $null }
    $i = Get-ScheduledTaskInfo -TaskName $Name
    $triggers = @()
    foreach ($tr in $t.Triggers) {
        $rep = $null
        if ($tr.Repetition -and $tr.Repetition.Interval) { $rep = [string]$tr.Repetition.Interval }
        $triggers += [PSCustomObject]@{
            Type               = $tr.CimClass.CimClassName
            StartBoundary      = $tr.StartBoundary
            Enabled            = $tr.Enabled
            RepetitionInterval = $rep
        }
    }
    return [PSCustomObject]@{
        TaskName  = $Name
        State     = [string]$t.State
        Enabled   = $t.Settings.Enabled
        Action    = [PSCustomObject]@{
            Execute          = $t.Actions[0].Execute
            Arguments        = $t.Actions[0].Arguments
            WorkingDirectory = $t.Actions[0].WorkingDirectory
        }
        Triggers  = $triggers
        Principal = [PSCustomObject]@{
            UserId    = $t.Principal.UserId
            LogonType = [string]$t.Principal.LogonType
            RunLevel  = [string]$t.Principal.RunLevel
        }
        Settings  = [PSCustomObject]@{
            StartWhenAvailable = $t.Settings.StartWhenAvailable
            MultipleInstances  = [string]$t.Settings.MultipleInstances
            ExecutionTimeLimit = $t.Settings.ExecutionTimeLimit
            RestartCount       = $t.Settings.RestartCount
            RestartInterval    = $t.Settings.RestartInterval
        }
        Info      = [PSCustomObject]@{
            LastRunTime    = if ($i.LastRunTime) { $i.LastRunTime.ToString('s') } else { $null }
            LastTaskResult = $i.LastTaskResult
            NextRunTime    = if ($i.NextRunTime) { $i.NextRunTime.ToString('s') } else { $null }
        }
    }
}

$before = Get-TaskSnapshot $TaskName
$desired = Get-R531DesiredDefinition
$verdict = Get-R531InstallDecision $before $desired $Force.IsPresent

switch ($verdict.decision) {
    'UNCHANGED' {
        $global:R531CollectionTaskInstallResult = 'UNCHANGED'
        Write-Output "R53_1_COLLECTION_TASK_UNCHANGED - $TaskName already matches the durable definition (LogonType=$PreferredLogonType)"
        if ($EvidenceFile) {
            [PSCustomObject]@{ before = $before; after = $before; changed = $false } |
                ConvertTo-Json -Depth 8 | Out-File -Encoding utf8 $EvidenceFile
        }
        return
    }
    'BLOCKED_PRINCIPAL' {
        Write-Blocked ("existing task LogonType=$($before.Principal.LogonType), " +
                       "requested=$PreferredLogonType; explicit -Force migration required " +
                       "(run from an ELEVATED PowerShell)")
        return
    }
    'BLOCKED_DEFINITION' {
        Write-Blocked ("an existing $TaskName has a DIFFERENT definition (" +
                       ($verdict.mismatches -join '; ') +
                       "); re-run with -Force for an explicit migration")
        return
    }
}
# INSTALL or MIGRATE falls through to registration.

$action = New-ScheduledTaskAction -Execute $PythonExe -Argument $arguments `
    -WorkingDirectory $WorkingDirectory

$bootTrigger = New-ScheduledTaskTrigger -AtStartup
$bootTrigger.Delay = 'PT2M'
$anchor = (Get-Date).Date.AddMinutes(5)   # 00:05 today; anchor time is not load-bearing
$recoveryTrigger = New-ScheduledTaskTrigger -Once -At $anchor `
    -RepetitionInterval (New-TimeSpan -Minutes $RecoveryRepetitionMinutes) `
    -RepetitionDuration ([TimeSpan]::MaxValue)
$triggers = @($bootTrigger, $recoveryTrigger)

$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 5) `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries

$user = $desired.UserId
$logonCandidates = if ($verdict.decision -eq 'MIGRATE') {
    @($PreferredLogonType)
} else {
    @($PreferredLogonType, 'Interactive') | Select-Object -Unique
}

$registered = $null
$logonUsed = $null
$lastRegError = ''
foreach ($logon in $logonCandidates) {
    try {
        $principal = New-ScheduledTaskPrincipal -UserId $user -LogonType $logon -RunLevel Limited
        $registered = Register-ScheduledTask -TaskName $TaskName -Action $action `
            -Trigger $triggers -Settings $settings -Principal $principal -Force -ErrorAction Stop
        $logonUsed = $logon
        break
    } catch {
        $lastRegError = $_.Exception.Message.Trim()
        Write-Output ("note: registration with LogonType=$logon failed: " + $lastRegError)
    }
}
if ($null -eq $registered) {
    if ($verdict.decision -eq 'MIGRATE') {
        Write-Blocked ("migration to LogonType=$PreferredLogonType failed ($lastRegError); " +
                       "S4U registration requires an ELEVATED PowerShell - " +
                       "re-run this script from an elevated shell")
    } else {
        Write-Blocked 'Register-ScheduledTask failed for every logon type'
    }
    return
}

if ($logonUsed -ne $PreferredLogonType) {
    Write-Output ("note: requested LogonType=$PreferredLogonType was NOT achieved; " +
                  "the task is registered with LogonType=$logonUsed and runs only while " +
                  "the user is logged on. Re-run from an ELEVATED shell with " +
                  "-PreferredLogonType S4U -Force to migrate.")
}

$after = Get-TaskSnapshot $TaskName
if ($EvidenceFile) {
    [PSCustomObject]@{
        before     = $before
        after      = $after
        changed    = $true
        decision   = $verdict.decision
        mismatches = $verdict.mismatches
        logon_type = $logonUsed
        note       = if ($logonUsed -eq 'Interactive') {
            'Interactive logon: the collector still dies with the logon session. ' +
            'Re-register from an elevated shell with -PreferredLogonType S4U -Force.'
        } else {
            'S4U logon: the collector runs and recovers whether or not the user is ' +
            'logged on (no stored password). The 30-minute repetition is the recovery ' +
            'clock; IgnoreNew makes it a no-op while the worker lives.'
        }
    } | ConvertTo-Json -Depth 8 | Out-File -Encoding utf8 $EvidenceFile
}

if ($verdict.decision -eq 'MIGRATE') {
    $global:R531CollectionTaskInstallResult = "MIGRATED ($logonUsed)"
    Write-Output "R53_1_COLLECTION_TASK_MIGRATED - $TaskName re-registered ($(@($verdict.mismatches).Count) definition difference(s) resolved), LogonType=$logonUsed"
} else {
    $global:R531CollectionTaskInstallResult = "INSTALLED ($logonUsed)"
    Write-Output "R53_1_COLLECTION_TASK_INSTALLED - $TaskName (boot + $RecoveryRepetitionMinutes-minute recovery repetition), LogonType=$logonUsed"
}
