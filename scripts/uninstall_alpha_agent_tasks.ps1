<#
.SYNOPSIS
    Remove ONLY the four Alpha Agent Stage 4 Windows Scheduled Tasks.

.DESCRIPTION
    Unregisters exactly AlphaAgent-Collect, AlphaAgent-Morning-Report,
    AlphaAgent-PostClose-Report and AlphaAgent-Watchdog. Touches no other
    scheduled task, no runtime data, no report, no credential and no
    operational ledger.
#>
[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$names = @("AlphaAgent-Collect", "AlphaAgent-Morning-Report",
           "AlphaAgent-PostClose-Report", "AlphaAgent-Watchdog")

$removed = 0
foreach ($name in $names) {
    $existing = Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue
    if ($null -ne $existing) {
        Unregister-ScheduledTask -TaskName $name -Confirm:$false
        Write-Host "[uninstall] Removed $name"
        $removed++
    } else {
        Write-Host "[uninstall] Not present: $name"
    }
}
Write-Host "ALPHA_AGENT_STAGE4_TASKS_REMOVED - $removed of $($names.Count)"
exit 0
