# tests/support/restart_contract_probe.ps1
#
# Release 29 UX2 contract harness for the CANONICAL restart owner.
#
# It exists for exactly one reason: to prove, with the REAL PowerShell parameter binder,
# that a DIRECT in-process invocation of the canonical restart owner binds
#
#     -Force           as [switch]
#     -Port            as [Int32]
#     -SmokePath       as ONE [String[]] holding every element the caller passed
#     -ReadyTimeoutSec as [Int32]
#
# The historical production defect flattened a String[] across a child shell and bound a
# URL positionally to -ReadyTimeoutSec:Int32. A regex over the source cannot prove that
# cannot happen; the binder can. This harness therefore calls the owner the way an
# operator does - directly, with `&`, in this same process - and asks it for its bound
# contract via -ContractProbe.
#
# SAFETY: -ContractProbe stops the owner BEFORE any store-root evaluation, process stop,
# process start, port probe or HTTP read. Nothing is started, stopped, restarted or read.
param(
    [int]$Port = 8098
)

$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$Owner = Join-Path $RepoRoot "scripts\restart_paper_trader_backend.ps1"

# The five caller-supplied paths from the canonical production invocation. They stay ONE
# String[]: that is the whole assertion.
$SmokePaths = @(
    '/v1/operations/workflow-state',
    '/v1/operations/information-collection',
    '/v1/operations/daily-close',
    '/v1/operational-book',
    '/v1/operations/portfolio-reassessment'
)

& $Owner -ContractProbe -Force -Port $Port -SmokePath $SmokePaths

Write-Host ("PROBE_LASTEXITCODE=" + $LASTEXITCODE)
Write-Host "PROBE_CALLER_SURVIVED"
