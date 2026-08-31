# =============================================================================
# scripts\install_research_runtime_task.ps1  (Release 52)
#
# Installs the ONE durable Windows scheduled task for the persistent
# prospective research runtime:
#
#     TaskName : PaperTrader-ResearchRuntime
#     Action   : <venv python> scripts\run_research_runtime.py --trigger SCHEDULED
#     Triggers : daily 08:15, 17:45, 19:45, 21:45 local (machine local = ET)
#
# The trigger times are CONSUMED from the derived timing contract
# (alpha_agent.r52.timing_contract.INVOCATION_PLAN); this script adds no
# timing rule of its own. The runtime itself decides what is due on each
# invocation using the canonical timing owners, so firing more often than
# necessary is harmless by construction (ledger identities + runtime lock).
#
# Idempotent and explicit:
#   - task equivalence compares the FULL definition: Action.Execute,
#     Action.Arguments, WorkingDirectory, trigger types, trigger times,
#     Enabled, StartWhenAvailable, MultipleInstances, ExecutionTimeLimit,
#     RestartCount, RestartInterval, WakeToRun, Principal.UserId and
#     Principal.LogonType. An existing Interactive task is NOT identical
#     to a requested S4U task.
#   - installing over an IDENTICAL definition reports UNCHANGED;
#   - installing over a DIFFERENT existing definition requires -Force
#     (explicit migration, never a silent overwrite); a principal mismatch
#     without -Force reports its own specific blocker;
#   - migrating an EXISTING task registers with the requested logon type
#     ONLY - it never silently falls back to Interactive (S4U registration
#     needs an elevated shell); the Interactive fallback exists only for a
#     FRESH install, and it says so;
#   - no other task is read for write, modified or deleted.
#
# -DecisionProbe <path|ABSENT> : hermetic test mode. Reads an existing-task
#   snapshot from the JSON file at <path> (shape of Get-TaskSnapshot; the
#   literal ABSENT means no task), prints the install decision as JSON and
#   returns WITHOUT touching the scheduler, a process or a file.
#
# Safety: the scheduled action is research-only. It cannot call the portfolio
# cycle, the daily close, a broker, or any operational store; after the legal
# cutoff the runtime refuses emission and records a forfeiture - it can never
# backfill.
#
# Reports (canonical restart-owner convention - no exit statements):
#   printed token   : R52_TASK_INSTALLED / R52_TASK_MIGRATED /
#                     R52_TASK_UNCHANGED /
#                     R52_TASK_INSTALL_BLOCKED - <reason>
#   $global:R52TaskInstallResult
# =============================================================================
[CmdletBinding()]
param(
    [string]$TaskName = 'PaperTrader-ResearchRuntime',
    [string]$PythonExe = 'C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe',
    [string]$RuntimeScript = 'C:\Users\binis\paper_trader\scripts\run_research_runtime.py',
    [string]$WorkingDirectory = 'C:\Users\binis\paper_trader',
    [string[]]$TriggerTimes = @('08:15', '17:45', '19:45', '21:45'),
    [ValidateSet('S4U', 'Interactive')][string]$PreferredLogonType = 'S4U',
    [string]$EvidenceFile = '',
    [string]$DecisionProbe = '',
    [switch]$Force
)

$global:R52TaskInstallResult = $null

function Write-Blocked([string]$Reason) {
    $global:R52TaskInstallResult = "BLOCKED - $Reason"
    Write-Output "R52_TASK_INSTALL_BLOCKED - $Reason"
}

$arguments = "`"$RuntimeScript`" --trigger SCHEDULED"

# ---- the ONE desired definition (every compared field, explicit) ----------- #
function Get-R52DesiredDefinition {
    return [PSCustomObject]@{
        Execute            = $PythonExe
        Arguments          = $arguments
        WorkingDirectory   = $WorkingDirectory
        TriggerType        = 'MSFT_TaskDailyTrigger'
        TriggerTimes       = @(@($TriggerTimes) | Sort-Object)
        Enabled            = $true
        StartWhenAvailable = $true
        MultipleInstances  = 'IgnoreNew'
        ExecutionTimeLimit = 'PT2H'
        RestartCount       = 2
        RestartInterval    = 'PT10M'
        WakeToRun          = $true
        UserId             = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
        LogonType          = $PreferredLogonType
    }
}

# ---- full-definition comparison. A missing field is a mismatch. ------------ #
function Get-R52DefinitionMismatches($existing, $desired) {
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
    $badTypes = @($existing.Triggers | Where-Object { [string]$_.Type -ne $desired.TriggerType })
    if ($badTypes.Count -gt 0) {
        $mm += ("trigger types " + (@($badTypes | ForEach-Object { $_.Type }) -join ', ') +
                " (expected only $($desired.TriggerType))")
    }
    $disabledTriggers = @($existing.Triggers | Where-Object { -not [bool]$_.Enabled })
    if ($disabledTriggers.Count -gt 0) { $mm += "$($disabledTriggers.Count) trigger(s) disabled" }
    $haveTimes = @($existing.Triggers | ForEach-Object {
        if ($_.StartBoundary) { ([DateTime]$_.StartBoundary).ToString('HH:mm') } }) | Sort-Object
    if ((@($haveTimes) -join ',') -ne (@($desired.TriggerTimes) -join ',')) {
        $mm += ("trigger times [" + (@($haveTimes) -join ', ') + "] != [" +
                (@($desired.TriggerTimes) -join ', ') + "]")
    }
    if ([bool]$existing.Enabled -ne [bool]$desired.Enabled) {
        $mm += "Enabled $($existing.Enabled) != $($desired.Enabled)"
    }
    if ([bool]$existing.Settings.StartWhenAvailable -ne [bool]$desired.StartWhenAvailable) {
        $mm += "StartWhenAvailable $($existing.Settings.StartWhenAvailable) != $($desired.StartWhenAvailable)"
    }
    if ([string]$existing.Settings.MultipleInstances -ne [string]$desired.MultipleInstances) {
        $mm += "MultipleInstances '$($existing.Settings.MultipleInstances)' != '$($desired.MultipleInstances)'"
    }
    if ([string]$existing.Settings.ExecutionTimeLimit -ne [string]$desired.ExecutionTimeLimit) {
        $mm += "ExecutionTimeLimit '$($existing.Settings.ExecutionTimeLimit)' != '$($desired.ExecutionTimeLimit)'"
    }
    if ([int]$existing.Settings.RestartCount -ne [int]$desired.RestartCount) {
        $mm += "RestartCount $($existing.Settings.RestartCount) != $($desired.RestartCount)"
    }
    if ([string]$existing.Settings.RestartInterval -ne [string]$desired.RestartInterval) {
        $mm += "RestartInterval '$($existing.Settings.RestartInterval)' != '$($desired.RestartInterval)'"
    }
    if ([bool]$existing.Settings.WakeToRun -ne [bool]$desired.WakeToRun) {
        $mm += "WakeToRun $($existing.Settings.WakeToRun) != $($desired.WakeToRun)"
    }
    # The scheduler stores the leaf name ('binis'); WindowsIdentity returns
    # 'MACHINE\binis'. Compare the leaf, case-insensitively.
    $haveUser = ([string]$existing.Principal.UserId -split '\\')[-1].ToLowerInvariant()
    $wantUser = ([string]$desired.UserId -split '\\')[-1].ToLowerInvariant()
    if ($haveUser -ne $wantUser) {
        $mm += "Principal.UserId '$($existing.Principal.UserId)' != '$($desired.UserId)'"
    }
    if ([string]$existing.Principal.LogonType -ne [string]$desired.LogonType) {
        $mm += "Principal.LogonType=$($existing.Principal.LogonType), requested=$($desired.LogonType)"
    }
    # Plain return on purpose: the caller wraps with @(). A `return ,$mm`
    # here would emit an EMPTY result as one nested empty array, making
    # "no mismatches" count as a mismatch (PS 5.1 pipeline unrolling).
    return $mm
}

# ---- decision: UNCHANGED / BLOCKED_PRINCIPAL / BLOCKED_DEFINITION /
#      MIGRATE / INSTALL ----------------------------------------------------- #
function Get-R52InstallDecision($existing, $desired, [bool]$ForceRequested) {
    if ($null -eq $existing) {
        return [PSCustomObject]@{ decision = 'INSTALL'; mismatches = @() }
    }
    $mm = @(Get-R52DefinitionMismatches $existing $desired)
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
    $probeDecision = Get-R52InstallDecision $probeExisting (Get-R52DesiredDefinition) $Force.IsPresent
    $global:R52TaskInstallResult = "PROBE - $($probeDecision.decision)"
    $probeDecision | Add-Member -NotePropertyName requested_logon_type -NotePropertyValue $PreferredLogonType
    $probeDecision | ConvertTo-Json -Depth 4 | Write-Output
    return
}

if (-not (Test-Path $PythonExe)) { Write-Blocked "python not found at $PythonExe"; return }
if (-not (Test-Path $RuntimeScript)) { Write-Blocked "runtime script not found at $RuntimeScript"; return }

function Get-TaskSnapshot([string]$Name) {
    try { $t = Get-ScheduledTask -TaskName $Name -ErrorAction Stop } catch { return $null }
    $i = Get-ScheduledTaskInfo -TaskName $Name
    $triggers = @()
    foreach ($tr in $t.Triggers) {
        $triggers += [PSCustomObject]@{
            Type          = $tr.CimClass.CimClassName
            StartBoundary = $tr.StartBoundary
            Enabled       = $tr.Enabled
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
            WakeToRun          = $t.Settings.WakeToRun
            DisallowStartIfOnBatteries = $t.Settings.DisallowStartIfOnBatteries
        }
        Info      = [PSCustomObject]@{
            LastRunTime    = if ($i.LastRunTime) { $i.LastRunTime.ToString('s') } else { $null }
            LastTaskResult = $i.LastTaskResult
            NextRunTime    = if ($i.NextRunTime) { $i.NextRunTime.ToString('s') } else { $null }
        }
    }
}

$before = Get-TaskSnapshot $TaskName
$desired = Get-R52DesiredDefinition
$verdict = Get-R52InstallDecision $before $desired $Force.IsPresent

switch ($verdict.decision) {
    'UNCHANGED' {
        $global:R52TaskInstallResult = 'UNCHANGED'
        Write-Output "R52_TASK_UNCHANGED - $TaskName already matches the derived definition (including Principal.LogonType=$PreferredLogonType)"
        if ($EvidenceFile) {
            [PSCustomObject]@{ before = $before; after = $before; changed = $false } |
                ConvertTo-Json -Depth 8 | Out-File -Encoding utf8 $EvidenceFile
        }
        return
    }
    'BLOCKED_PRINCIPAL' {
        Write-Blocked ("existing task LogonType=$($before.Principal.LogonType), " +
                       "requested=$PreferredLogonType; explicit -Force migration required")
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

# ---- build the definition -------------------------------------------------- #
$action = New-ScheduledTaskAction -Execute $PythonExe -Argument $arguments `
    -WorkingDirectory $WorkingDirectory

$triggers = @()
foreach ($t in $TriggerTimes) {
    $triggers += New-ScheduledTaskTrigger -Daily -At ([DateTime]::ParseExact($t, 'HH:mm', $null))
}

$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2) `
    -RestartCount 2 `
    -RestartInterval (New-TimeSpan -Minutes 10) `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -WakeToRun

$user = $desired.UserId

# A MIGRATION registers with the requested logon type ONLY: the principal is
# usually the very thing being migrated, so a silent Interactive fallback
# would re-register the old principal and report success. The fallback is
# legal only for a FRESH install, where any working principal beats no task.
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
            'Interactive logon: the task runs only while the user is logged on. ' +
            'StartWhenAvailable fires a missed trigger at the next opportunity; ' +
            'a window that closes while logged off ends in a RECORDED FORFEITURE, ' +
            'never a backfill. Re-register from an elevated shell with ' +
            '-PreferredLogonType S4U -Force for logged-off operation.'
        } else { 'S4U logon: runs whether or not the user is logged on (no stored password).' }
    } | ConvertTo-Json -Depth 8 | Out-File -Encoding utf8 $EvidenceFile
}

if ($verdict.decision -eq 'MIGRATE') {
    $global:R52TaskInstallResult = "MIGRATED ($logonUsed)"
    Write-Output "R52_TASK_MIGRATED - $TaskName re-registered ($(@($verdict.mismatches).Count) definition difference(s) resolved), LogonType=$logonUsed"
} else {
    $global:R52TaskInstallResult = "INSTALLED ($logonUsed)"
    Write-Output "R52_TASK_INSTALLED - $TaskName with $($TriggerTimes.Count) daily triggers ($($TriggerTimes -join ', ')), LogonType=$logonUsed"
}
