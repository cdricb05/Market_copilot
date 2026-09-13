<#
.SYNOPSIS
    ONE command that advances REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1 by a day.

.DESCRIPTION
    The canonical research runtime (PaperTrader-ResearchRuntime ->
    run_research_runtime.py --mode persistent -> alpha_agent.r52.runtime) already
    calls this same owner on every cycle, so in normal operation NOBODY needs to
    run this. It exists so that a runtime that is down, wedged or unattended can
    never turn into a missed entry session, and so the operator has exactly one
    thing to remember instead of three.

    It performs, in order and all idempotently:

        1. probe whether Databento HISTORICAL OPRA has published the session
        2. append that session's 15:45 ET snapshot to the owned surface
           (APPEND ONLY - an existing row always wins)
        3. freeze ONE immutable prospective decision, if the window is open

    Safe to run repeatedly. Running it twice produces one decision, because the
    freeze is first-write-wins.

    RESEARCH ONLY. No order, no fill, no proposal, no promotion, no capital.
    PAID DOLLARS = 0: the probe is free metadata and the append is refused
    unless it fits the Databento free credit already held.

.PARAMETER Session
    The INFORMATION session (the one that just traded). Omit it and the session
    is derived from the clock and the exchange calendar, which is what the
    runtime does.

.PARAMETER NoAppend
    Price the append but never download.

.PARAMETER Json
    Emit the owner's full result as JSON instead of the readable summary.

.EXAMPLE
    .\scripts\run_next_open_forward_cycle.ps1

.EXAMPLE
    .\scripts\run_next_open_forward_cycle.ps1 -Session 2026-09-14
#>
[CmdletBinding()]
param(
    [string] $Session,
    [switch] $NoAppend,
    [switch] $Json
)

$ErrorActionPreference = 'Continue'

$repo = Split-Path -Parent $PSScriptRoot
$python = Join-Path $repo '.venv-win\Scripts\python.exe'
$entry = Join-Path $repo 'scripts\run_next_open_prospective_decision.py'

if (-not (Test-Path $python)) {
    Write-Output "NEXT_OPEN_CYCLE_BLOCKED - no interpreter at $python"
    return 2
}
if (-not (Test-Path $entry)) {
    Write-Output "NEXT_OPEN_CYCLE_BLOCKED - no entrypoint at $entry"
    return 2
}

$cmdArgs = @($entry, '--advance')
if ($Session)  { $cmdArgs += @('--session', $Session) }
if ($NoAppend) { $cmdArgs += '--no-append' }
if ($Json)     { $cmdArgs += '--json' }

Write-Output "advancing REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1 ..."
& $python @cmdArgs
$rc = $LASTEXITCODE

# The owner prints its own terminal token on the last line. This wrapper adds
# no verdict of its own: a second opinion about whether a decision was frozen
# is exactly the kind of thing this estate refuses to have two of.
return $rc
