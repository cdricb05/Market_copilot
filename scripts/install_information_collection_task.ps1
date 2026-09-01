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
#                + ONE DAILY trigger whose repetition is every 30 minutes for
#                  a duration of one day (the PERIODIC RECOVERY: consecutive
#                  one-day repetition windows abut, so coverage is continuous;
#                  while the long-lived worker runs, MultipleInstances=
#                  IgnoreNew makes every firing a no-op; the moment the worker
#                  dies, the next firing relaunches it within 30 minutes,
#                  logged on or not).
#                  WHY daily/P1D and not an "indefinite" duration: PowerShell
#                  serializes TimeSpan.MaxValue as P99999999DT23H59M59S, which
#                  Task Scheduler REJECTS as "incorrectly formatted or out of
#                  range" before registration (the 2026-09-01 operator
#                  failure). Daily + repeat-for-one-day is the scheduler's own
#                  UI preset and always-valid XML with the same recovery
#                  semantics.
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
#     decision as JSON, touches NOTHING;
#   - -TriggerProbe : hermetic test mode - builds the EXACT trigger objects
#     registration would use (client-side only) and prints their serialized
#     repetition interval/duration, proving the definition is in a
#     Task-Scheduler-supported form;
#   - -ClassifyProbe <message> [-ClassifyShell Elevated|NotElevated] :
#     hermetic test mode for the registration-failure classifier. A failure
#     is reported for WHAT IT IS: a definition/XML rejection is never blamed
#     on elevation.
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
    [switch]$TriggerProbe,
    [string]$ClassifyProbe = '',
    [ValidateSet('', 'Elevated', 'NotElevated')][string]$ClassifyShell = '',
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
        # time-anchored trigger carrying a CONTINUOUS repetition at the
        # recovery cadence - either an indefinite repetition (no duration) or
        # a daily recurrence whose one-day repetition windows abut (the shape
        # this script registers, because a serialized TimeSpan.MaxValue
        # duration is rejected by Task Scheduler). Wall-clock anchor time is
        # NOT compared (any anchor works; the repetition is what matters).
        BootTriggers       = 1
        RepetitionTriggers = 1
        RepetitionInterval = ('PT{0}M' -f $RecoveryRepetitionMinutes)
        RepetitionDuration = 'P1D'           # per DAILY recurrence; documentation
        RepetitionShape    = 'DAILY_P1D'     # what registration produces
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

# Coverage of a repetition trigger. 'CONTINUOUS' means a dead worker is
# always within one repetition interval of a relaunch: either the repetition
# is indefinite (no Duration), or the trigger recurs DAILY (every 1 day) and
# each day's repetition window lasts the full day, so consecutive windows
# abut. Anything else leaves a recurring gap - or, with StopAtDurationEnd,
# would KILL the long-lived worker at each window end.
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

function Get-TriggerKind($t) {
    $type = [string]$t.Type
    if ($type -match 'Boot') { return 'BOOT' }
    $rep = ''; $dur = ''; $di = $null; $stopEnd = $false
    if ($t.PSObject.Properties['RepetitionInterval']) {
        $rep = [string]$t.RepetitionInterval
    } elseif ($t.PSObject.Properties['Repetition'] -and $t.Repetition) {
        $rep = [string]$t.Repetition.Interval
        $dur = [string]$t.Repetition.Duration
    }
    if ($t.PSObject.Properties['RepetitionDuration']) { $dur = [string]$t.RepetitionDuration }
    if ($t.PSObject.Properties['DaysInterval']) { $di = $t.DaysInterval }
    if ($t.PSObject.Properties['StopAtDurationEnd']) { $stopEnd = [bool]$t.StopAtDurationEnd }
    if (-not $rep) { return "OTHER:$type" }
    $coverage = Get-R531RepetitionCoverage $type $rep $dur $di $stopEnd
    if ($coverage -eq 'CONTINUOUS') { return "REPETITION:$rep" }
    return "BROKEN_REPETITION:$rep ($coverage)"
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

# ---- the ONE trigger construction (used by registration AND -TriggerProbe) - #
function New-R531Triggers {
    $boot = New-ScheduledTaskTrigger -AtStartup
    $boot.Delay = 'PT2M'
    # PERIODIC RECOVERY: Task Scheduler's own UI preset "Daily; repeat every
    # N minutes for a duration of 1 day". Consecutive one-day windows abut,
    # so coverage is continuous. NEVER pass TimeSpan.MaxValue as the
    # duration: it serializes as P99999999DT23H59M59S and Task Scheduler
    # rejects the XML as "incorrectly formatted or out of range".
    $anchor = (Get-Date).Date.AddMinutes(5)   # 00:05; anchor time is not load-bearing
    $repetition = (New-ScheduledTaskTrigger -Once -At $anchor `
        -RepetitionInterval (New-TimeSpan -Minutes $RecoveryRepetitionMinutes) `
        -RepetitionDuration (New-TimeSpan -Days 1)).Repetition
    # PowerShell silently sets StopAtDurationEnd=true when a finite
    # RepetitionDuration is given - that would KILL the long-lived worker at
    # every window end. Clear it explicitly; the coverage rule refuses it.
    $repetition.StopAtDurationEnd = $false
    $recovery = New-ScheduledTaskTrigger -Daily -At $anchor
    $recovery.Repetition = $repetition
    return @($boot, $recovery)
}

function Test-R531ShellElevated {
    try {
        $id = [Security.Principal.WindowsIdentity]::GetCurrent()
        return ([Security.Principal.WindowsPrincipal]$id).IsInRole(
            [Security.Principal.WindowsBuiltInRole]::Administrator)
    } catch { return $false }
}

# Honest registration-failure classification. The 2026-09-01 operator run
# failed with a Task Scheduler XML rejection from an ELEVATED shell, and the
# installer blamed elevation. Never again: the DEFINITION class is checked
# FIRST, and elevation is only ever named when the shell is actually
# unelevated.
function Get-R531RegistrationFailureKind([string]$ErrorMessage,
                                         [bool]$ShellIsElevated) {
    $m = [string]$ErrorMessage
    if ($m -match 'incorrectly formatted|out of range|missing a required element|task XML|\(\d+,\d+\):') {
        return [PSCustomObject]@{
            kind     = 'DEFINITION_REJECTED_BY_SCHEDULER'
            guidance = ('Task Scheduler rejected the task DEFINITION itself: "' +
                        $m + '". This is a defect in the generated definition, ' +
                        'not an elevation problem - re-running elevated will not help.')
        }
    }
    $accessDenied = ($m -match 'Access is denied|0x80070005|E_ACCESSDENIED')
    if ($accessDenied -and -not $ShellIsElevated) {
        return [PSCustomObject]@{
            kind     = 'ELEVATION_REQUIRED'
            guidance = ('registration was denied and this shell is NOT elevated; ' +
                        'S4U registration needs an ELEVATED PowerShell - re-run ' +
                        'this script from an elevated shell ("' + $m + '")')
        }
    }
    if ($accessDenied) {
        return [PSCustomObject]@{
            kind     = 'ACCESS_DENIED_WHILE_ELEVATED'
            guidance = ('access was denied even though this shell IS elevated - ' +
                        'inspect the task''s existing permissions/policy; raw error: "' +
                        $m + '"')
        }
    }
    return [PSCustomObject]@{
        kind     = 'REGISTRATION_ERROR'
        guidance = ('Register-ScheduledTask failed: "' + $m + '"')
    }
}

# ---- hermetic classification probe (no scheduler, no process) -------------- #
if ($ClassifyProbe) {
    $probeElevated = switch ($ClassifyShell) {
        'Elevated'    { $true }
        'NotElevated' { $false }
        default       { Test-R531ShellElevated }
    }
    $k = Get-R531RegistrationFailureKind $ClassifyProbe $probeElevated
    $global:R531CollectionTaskInstallResult = "PROBE_CLASSIFY - $($k.kind)"
    [PSCustomObject]@{ kind = $k.kind; shell_elevated = $probeElevated
                       guidance = $k.guidance } | ConvertTo-Json | Write-Output
    return
}

# ---- hermetic trigger probe: build the EXACT trigger objects registration -- #
# ---- would use and print their serialized form (client-side objects only) -- #
if ($TriggerProbe) {
    $tt = New-R531Triggers
    $recRep = $tt[1].Repetition
    $probeOut = [PSCustomObject]@{
        boot_class           = [string]$tt[0].CimClass.CimClassName
        boot_delay           = [string]$tt[0].Delay
        recovery_class       = [string]$tt[1].CimClass.CimClassName
        days_interval        = [int]$tt[1].DaysInterval
        repetition_interval  = [string]$recRep.Interval
        repetition_duration  = [string]$recRep.Duration
        stop_at_duration_end = [bool]$recRep.StopAtDurationEnd
        coverage             = (Get-R531RepetitionCoverage `
            ([string]$tt[1].CimClass.CimClassName) ([string]$recRep.Interval) `
            ([string]$recRep.Duration) $tt[1].DaysInterval `
            ([bool]$recRep.StopAtDurationEnd))
    }
    $global:R531CollectionTaskInstallResult = "PROBE_TRIGGERS - $($probeOut.coverage)"
    $probeOut | ConvertTo-Json | Write-Output
    return
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
        $rep = $null; $dur = $null; $stopEnd = $false; $di = $null
        if ($tr.Repetition -and $tr.Repetition.Interval) { $rep = [string]$tr.Repetition.Interval }
        if ($tr.Repetition -and $tr.Repetition.Duration) { $dur = [string]$tr.Repetition.Duration }
        if ($tr.Repetition -and $tr.Repetition.StopAtDurationEnd) { $stopEnd = $true }
        if ($tr.PSObject.Properties['DaysInterval'] -and $tr.DaysInterval) { $di = [int]$tr.DaysInterval }
        $triggers += [PSCustomObject]@{
            Type               = $tr.CimClass.CimClassName
            StartBoundary      = $tr.StartBoundary
            Enabled            = $tr.Enabled
            RepetitionInterval = $rep
            RepetitionDuration = $dur
            StopAtDurationEnd  = $stopEnd
            DaysInterval       = $di
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

$triggers = New-R531Triggers

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
$lastRegKind = $null
$shellElevated = Test-R531ShellElevated
foreach ($logon in $logonCandidates) {
    try {
        $principal = New-ScheduledTaskPrincipal -UserId $user -LogonType $logon -RunLevel Limited
        $registered = Register-ScheduledTask -TaskName $TaskName -Action $action `
            -Trigger $triggers -Settings $settings -Principal $principal -Force -ErrorAction Stop
        $logonUsed = $logon
        break
    } catch {
        $lastRegError = $_.Exception.Message.Trim()
        $lastRegKind = Get-R531RegistrationFailureKind $lastRegError $shellElevated
        Write-Output ("note: registration with LogonType=$logon failed " +
                      "[$($lastRegKind.kind)]: $lastRegError")
    }
}
if ($null -eq $registered) {
    $why = if ($null -ne $lastRegKind) {
        "$($lastRegKind.kind) - $($lastRegKind.guidance)"
    } else {
        'Register-ScheduledTask failed for every logon type'
    }
    if ($verdict.decision -eq 'MIGRATE') {
        Write-Blocked ("migration to LogonType=$PreferredLogonType failed: $why")
    } else {
        Write-Blocked ("fresh installation failed: $why")
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
