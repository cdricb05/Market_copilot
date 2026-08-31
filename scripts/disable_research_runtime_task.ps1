# =============================================================================
# scripts\disable_research_runtime_task.ps1  (Release 52)
#
# Disables ONLY the PaperTrader-ResearchRuntime task. It deletes nothing,
# touches no other task, and removes no evidence: the forfeiture ledger, the
# run journal, the health read model and every research artifact stay exactly
# where they are. A disabled runtime means future legal windows will close
# unattended - which the next enabled run will record as forfeitures, never
# repair.
#
# Reports (no exit statements):
#   printed token : R52_TASK_DISABLED / R52_TASK_ALREADY_DISABLED /
#                   R52_TASK_NOT_INSTALLED
#   $global:R52TaskDisableResult
# =============================================================================
[CmdletBinding()]
param(
    [string]$TaskName = 'PaperTrader-ResearchRuntime'
)

$global:R52TaskDisableResult = $null

try { $t = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop } catch { $t = $null }
if ($null -eq $t) {
    $global:R52TaskDisableResult = 'NOT_INSTALLED'
    Write-Output "R52_TASK_NOT_INSTALLED - $TaskName does not exist; nothing was changed"
    return
}
if (-not $t.Settings.Enabled) {
    $global:R52TaskDisableResult = 'ALREADY_DISABLED'
    Write-Output "R52_TASK_ALREADY_DISABLED - $TaskName was already disabled; nothing was changed"
    return
}

Disable-ScheduledTask -TaskName $TaskName | Out-Null
$global:R52TaskDisableResult = 'DISABLED'
Write-Output "R52_TASK_DISABLED - $TaskName (evidence, ledgers and artifacts untouched; no other task was modified)"
