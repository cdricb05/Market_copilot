# =============================================================================
# scripts\run_research_runtime_once.ps1  (Release 52)
#
# Manual one-shot of the persistent research runtime - the SAME entrypoint the
# scheduled task executes, called directly. Idempotent by construction.
#
#   -NoEmit : sweep-only invocation (score matured outcomes, record
#             forfeitures, rebuild read models; never emit a batch).
#
# Reports (no exit statements):
#   pass-through of the runtime's ONE terminal token
#   $global:R52RuntimeOnceResult = OK / REFUSED_CONCURRENT / INTEGRITY_FAILED /
#                                  FAILED
# =============================================================================
[CmdletBinding()]
param(
    [string]$PythonExe = 'C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe',
    [string]$RuntimeScript = 'C:\Users\binis\paper_trader\scripts\run_research_runtime.py',
    [string]$Trigger = 'MANUAL_ONE_SHOT',
    [switch]$NoEmit
)

$global:R52RuntimeOnceResult = $null

if (-not (Test-Path $PythonExe)) {
    $global:R52RuntimeOnceResult = 'FAILED'
    Write-Output "RESEARCH_RUNTIME_FAILED - python not found at $PythonExe"
    return
}

$argList = @("`"$RuntimeScript`"", '--trigger', $Trigger)
if ($NoEmit) { $argList += '--no-emit' }

& $PythonExe @argList
$code = $LASTEXITCODE

$global:R52RuntimeOnceResult = switch ($code) {
    0 { 'OK' }
    3 { 'REFUSED_CONCURRENT' }
    4 { 'INTEGRITY_FAILED' }
    default { 'FAILED' }
}
