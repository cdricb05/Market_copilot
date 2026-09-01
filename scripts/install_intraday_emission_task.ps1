# =============================================================================
# scripts\install_intraday_emission_task.ps1  (Release 53.1)
#
# Installs the ONE Windows scheduled task for the prospective INTRADAY
# emission runner:
#
#     TaskName : PaperTrader-IntradayEmission
#     Action   : <venv python> scripts\run_intraday_emission.py
#     Triggers : daily 10:00, 12:00, 14:00 (the frozen emission slots,
#                machine local = ET) and 16:20 (post-close scoring pass -
#                emission is structurally refused outside a slot; the run
#                only scores matured session-close outcomes).
#
# The slot times are CONSUMED from the frozen factory contract
# (alpha_agent.r53.intraday_factory.EMISSION_SLOTS_ET); this script adds no
# timing rule of its own. Firing on a weekend or holiday is harmless by
# construction: the factory's slot clock refuses (NOT_AN_EMISSION_SLOT) and
# the ledgers' identity keys make every re-fire idempotent.
#
# Same decision discipline as the R52 installer: full-definition comparison,
# UNCHANGED / BLOCKED / MIGRATE / INSTALL, -Force for explicit migration,
# -DecisionProbe hermetic test mode, Interactive fallback for a FRESH install
# only.
#
# SAFETY. The scheduled action is SHADOW research emission and scoring only:
# no order, no fill, no promotion, no portfolio write, no production store.
#
# Reports (no exit statements):
#   printed token   : R53_1_EMISSION_TASK_INSTALLED / _MIGRATED / _UNCHANGED /
#                     R53_1_EMISSION_TASK_INSTALL_BLOCKED - <reason>
#   $global:R531EmissionTaskInstallResult
# =============================================================================
[CmdletBinding()]
param(
    [string]$TaskName = 'PaperTrader-IntradayEmission',
    [string]$PythonExe = 'C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe',
    [string]$RunnerScript = 'C:\Users\binis\paper_trader\scripts\run_intraday_emission.py',
    [string]$WorkingDirectory = 'C:\Users\binis\paper_trader',
    [string[]]$TriggerTimes = @('10:00', '12:00', '14:00', '16:20'),
    [ValidateSet('S4U', 'Interactive')][string]$PreferredLogonType = 'S4U',
    [string]$EvidenceFile = '',
    [string]$DecisionProbe = '',
    [switch]$Force
)

$global:R531EmissionTaskInstallResult = $null

function Write-Blocked([string]$Reason) {
    $global:R531EmissionTaskInstallResult = "BLOCKED - $Reason"
    Write-Output "R53_1_EMISSION_TASK_INSTALL_BLOCKED - $Reason"
}

$arguments = "`"$RunnerScript`""

function Get-R531EmDesiredDefinition {
    return [PSCustomObject]@{
        Execute            = $PythonExe
        Arguments          = $arguments
        WorkingDirectory   = $WorkingDirectory
        TriggerType        = 'MSFT_TaskDailyTrigger'
        TriggerTimes       = @(@($TriggerTimes) | Sort-Object)
        Enabled            = $true
        StartWhenAvailable = $false   # a slot that passed is a recorded
                                      # forfeiture, NEVER a late catch-up run
        MultipleInstances  = 'IgnoreNew'
        ExecutionTimeLimit = 'PT30M'
        RestartCount       = 2
        RestartInterval    = 'PT5M'
        UserId             = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
        LogonType          = $PreferredLogonType
    }
}

function Get-R531EmDefinitionMismatches($existing, $desired) {
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
    $haveTimes = @($existing.Triggers | ForEach-Object {
        if ($_.StartBoundary) { ([DateTime]$_.StartBoundary).ToString('HH:mm') } }) | Sort-Object
    if ((@($haveTimes) -join ',') -ne (@($desired.TriggerTimes) -join ',')) {
        $mm += ("trigger times [" + (@($haveTimes) -join ', ') + "] != [" +
                (@($desired.TriggerTimes) -join ', ') + "]")
    }
    if ([bool]$existing.Settings.StartWhenAvailable -ne [bool]$desired.StartWhenAvailable) {
        $mm += "StartWhenAvailable $($existing.Settings.StartWhenAvailable) != $($desired.StartWhenAvailable) (a missed slot is a forfeiture, never a late run)"
    }
    if ([string]$existing.Settings.MultipleInstances -ne [string]$desired.MultipleInstances) {
        $mm += "MultipleInstances '$($existing.Settings.MultipleInstances)' != '$($desired.MultipleInstances)'"
    }
    if ([string]$existing.Settings.ExecutionTimeLimit -ne [string]$desired.ExecutionTimeLimit) {
        $mm += "ExecutionTimeLimit '$($existing.Settings.ExecutionTimeLimit)' != '$($desired.ExecutionTimeLimit)'"
    }
    $haveUser = ([string]$existing.Principal.UserId -split '\\')[-1].ToLowerInvariant()
    $wantUser = ([string]$desired.UserId -split '\\')[-1].ToLowerInvariant()
    if ($haveUser -ne $wantUser) {
        $mm += "Principal.UserId '$($existing.Principal.UserId)' != '$($desired.UserId)'"
    }
    if ([string]$existing.Principal.LogonType -ne [string]$desired.LogonType) {
        $mm += "Principal.LogonType=$($existing.Principal.LogonType), requested=$($desired.LogonType)"
    }
    return $mm
}

function Get-R531EmInstallDecision($existing, $desired, [bool]$ForceRequested) {
    if ($null -eq $existing) {
        return [PSCustomObject]@{ decision = 'INSTALL'; mismatches = @() }
    }
    $mm = @(Get-R531EmDefinitionMismatches $existing $desired)
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

if ($DecisionProbe) {
    $probeExisting = $null
    if ($DecisionProbe -ne 'ABSENT') {
        $probeExisting = Get-Content -Path $DecisionProbe -Raw | ConvertFrom-Json
    }
    $probeDecision = Get-R531EmInstallDecision $probeExisting (Get-R531EmDesiredDefinition) $Force.IsPresent
    $global:R531EmissionTaskInstallResult = "PROBE - $($probeDecision.decision)"
    $probeDecision | Add-Member -NotePropertyName requested_logon_type -NotePropertyValue $PreferredLogonType
    $probeDecision | ConvertTo-Json -Depth 4 | Write-Output
    return
}

if (-not (Test-Path $PythonExe)) { Write-Blocked "python not found at $PythonExe"; return }
if (-not (Test-Path $RunnerScript)) { Write-Blocked "runner script not found at $RunnerScript"; return }

function Get-TaskSnapshot([string]$Name) {
    try { $t = Get-ScheduledTask -TaskName $Name -ErrorAction Stop } catch { return $null }
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
        Action    = [PSCustomObject]@{
            Execute          = $t.Actions[0].Execute
            Arguments        = $t.Actions[0].Arguments
            WorkingDirectory = $t.Actions[0].WorkingDirectory
        }
        Triggers  = $triggers
        Principal = [PSCustomObject]@{
            UserId    = $t.Principal.UserId
            LogonType = [string]$t.Principal.LogonType
        }
        Settings  = [PSCustomObject]@{
            StartWhenAvailable = $t.Settings.StartWhenAvailable
            MultipleInstances  = [string]$t.Settings.MultipleInstances
            ExecutionTimeLimit = $t.Settings.ExecutionTimeLimit
        }
    }
}

$before = Get-TaskSnapshot $TaskName
$desired = Get-R531EmDesiredDefinition
$verdict = Get-R531EmInstallDecision $before $desired $Force.IsPresent

switch ($verdict.decision) {
    'UNCHANGED' {
        $global:R531EmissionTaskInstallResult = 'UNCHANGED'
        Write-Output "R53_1_EMISSION_TASK_UNCHANGED - $TaskName already matches the derived definition"
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

$action = New-ScheduledTaskAction -Execute $PythonExe -Argument $arguments `
    -WorkingDirectory $WorkingDirectory

$triggers = @()
foreach ($tm in $TriggerTimes) {
    $triggers += New-ScheduledTaskTrigger -Daily -At ([DateTime]::ParseExact($tm, 'HH:mm', $null))
}

$settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 30) `
    -RestartCount 2 `
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
                       "re-run from an ELEVATED PowerShell")
    } else {
        Write-Blocked 'Register-ScheduledTask failed for every logon type'
    }
    return
}

if ($logonUsed -ne $PreferredLogonType) {
    Write-Output ("note: requested LogonType=$PreferredLogonType was NOT achieved; " +
                  "the task runs only while the user is logged on. A slot missed while " +
                  "logged off ends in a RECORDED FORFEITURE, never a late run. Re-run " +
                  "from an ELEVATED shell with -PreferredLogonType S4U -Force to migrate.")
}

if ($EvidenceFile) {
    [PSCustomObject]@{
        before = $before; after = Get-TaskSnapshot $TaskName; changed = $true
        decision = $verdict.decision; mismatches = $verdict.mismatches
        logon_type = $logonUsed
    } | ConvertTo-Json -Depth 8 | Out-File -Encoding utf8 $EvidenceFile
}

if ($verdict.decision -eq 'MIGRATE') {
    $global:R531EmissionTaskInstallResult = "MIGRATED ($logonUsed)"
    Write-Output "R53_1_EMISSION_TASK_MIGRATED - $TaskName re-registered, LogonType=$logonUsed"
} else {
    $global:R531EmissionTaskInstallResult = "INSTALLED ($logonUsed)"
    Write-Output "R53_1_EMISSION_TASK_INSTALLED - $TaskName with daily triggers ($($TriggerTimes -join ', ')), LogonType=$logonUsed"
}
