# scripts/restart_paper_trader_backend.ps1
#
# CANONICAL_RESTART_SMOKE_OWNER = scripts/restart_paper_trader_backend.ps1
#
# THE ONE OWNER of stopping, starting and live-smoking the Paper Trader backend.
#
# WHY THIS FILE EXISTS
# --------------------
# Stage after stage regenerated its own restart_smoke.ps1 inside a throwaway handoff
# directory, and stage after stage reintroduced the SAME defect: polling
# http://127.0.0.1:8001/health when the canonical readiness routes are /v1/health and
# /v1/ready. The operator's own paper_trader_8001.stdout.log records 39 consecutive
#     GET /health HTTP/1.1  404 Not Found
# lines before a human finally probed the right path by hand. Stage 12 shipped it. Stage
# 21 shipped it again. Duplicated operator workflow is precisely how a fixed defect comes
# back, so the workflow now has exactly one owner and the duplication is forbidden by a
# static guard rather than by memory.
#
# THIS SCRIPT OWNS, AND NOTHING ELSE MAY REIMPLEMENT:
#   * process stop / start
#   * port handling and the exactly-one-listener assertion
#   * health and readiness polling on the CANONICAL routes
#   * authentication setup for the live read
#   * stdout / stderr diagnostics on failure
#   * production store-root validation
#
# A stage handoff DELEGATES to this script and may add stage-specific GET assertions with
# -SmokePath. scripts/audit_architecture.py fails the build if any other PowerShell script
# probes a health/readiness route or launches the application, and
# tests/test_canonical_backend_restart.py proves this script's contract against the real
# route table and against a live backend.
#
# SAFETY: GET only. This workflow never issues a POST, PUT, PATCH or DELETE. It never runs
# a Daily Close, never creates a proposal, an order or a fill, never calls a broker and
# never enables automation. It restarts a process and reads.
#
# Usage (Windows PowerShell only):
#   .\scripts\restart_paper_trader_backend.ps1              # preflight only, no restart
#   .\scripts\restart_paper_trader_backend.ps1 -Force       # restart 8001 + live smoke
#   .\scripts\restart_paper_trader_backend.ps1 -Force -SmokePath "/v1/operations/daily-close/progress"
param(
    [switch]$Force,
    [int]$Port = 8001,
    [string]$AppModule = "paper_trader.api.app:app",
    [string]$AppDir = "",
    [string]$PythonExe = "",
    [string[]]$SmokePath = @(),
    [int]$ReadyTimeoutSec = 90,
    [int]$DiagnosticLines = 20,
    [string]$LogDir = ""
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version 2.0

# --------------------------------------------------------------------------- #
# The canonical readiness contract. These two literals are the ONLY health and
# readiness paths any Paper Trader restart/smoke workflow may probe. /health,
# /healthz, /ready and /readyz are NOT served by this application and never were.
# --------------------------------------------------------------------------- #
$CANONICAL_HEALTH_PATH = "/v1/health"
$CANONICAL_READY_PATH = "/v1/ready"

# The canonical authenticated read. It is the one that proves the backend came up on the
# REAL book: an empty portfolio here is the Stage-20.1 store-root contamination signature.
$CANONICAL_SMOKE_PATHS = @("/v1/operations/portfolio-state")

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$RepoParent = (Resolve-Path (Join-Path $RepoRoot "..")).Path
if (-not $PythonExe) { $PythonExe = Join-Path $RepoRoot ".venv-win\Scripts\python.exe" }
if (-not $LogDir) { $LogDir = $RepoRoot }
$StdoutLog = Join-Path $LogDir ("paper_trader_" + $Port + ".stdout.log")
$StderrLog = Join-Path $LogDir ("paper_trader_" + $Port + ".stderr.log")

$script:LaunchedPid = $null
$script:LaunchedProc = $null
$script:ApiKey = $null


# --------------------------------------------------------------------------- #
# Output helpers
# --------------------------------------------------------------------------- #
function Write-Section([string]$Title) {
    Write-Host ""
    Write-Host ("=" * 72)
    Write-Host $Title
    Write-Host ("=" * 72)
}

function Read-LogTail([string]$Path, [int]$Lines) {
    # The child process holds these files open for writing, so they must be opened with
    # FileShare::ReadWrite. Get-Content alone can fail with a sharing violation exactly
    # when the diagnostics matter most.
    if (-not (Test-Path $Path)) { return @("<no log file at $Path>") }
    try {
        $fs = New-Object System.IO.FileStream(
            $Path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read,
            [System.IO.FileShare]::ReadWrite)
        $sr = New-Object System.IO.StreamReader($fs)
        $all = $sr.ReadToEnd()
        $sr.Close()
        $fs.Close()
    } catch {
        return @("<log unreadable: " + $_.Exception.Message + ">")
    }
    $rows = @($all -split "`r?`n" | Where-Object { $_ -ne "" })
    if (@($rows).Count -eq 0) { return @("<empty>") }
    if (@($rows).Count -le $Lines) { return $rows }
    return @($rows[(@($rows).Count - $Lines)..(@($rows).Count - 1)])
}

function Get-BackendListeners {
    $conns = @(Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue)
    return @($conns | ForEach-Object { $_.OwningProcess } | Sort-Object -Unique)
}

# uvicorn does not serve from the process this script starts: on Windows it supervises a
# CHILD that owns the socket ("Started server process [N]" is a different pid). The
# listener therefore has to be the launched process OR a descendant of it - comparing pids
# directly would fail every real restart.
function Test-OwnedByLaunched([int]$ProcId) {
    if (-not $script:LaunchedPid) { return $true }
    $current = $ProcId
    for ($hop = 0; $hop -lt 8; $hop++) {
        if ($current -eq [int]$script:LaunchedPid) { return $true }
        $rows = @(Get-CimInstance Win32_Process -Filter ("ProcessId = " + $current) `
                                  -ErrorAction SilentlyContinue)
        if (@($rows).Count -eq 0) { return $false }
        $parent = 0
        try { $parent = [int]$rows[0].ParentProcessId } catch { return $false }
        if ($parent -le 0 -or $parent -eq $current) { return $false }
        $current = $parent
    }
    return $false
}

function Test-ChildExited {
    if (-not $script:LaunchedProc) { return $false }
    try {
        $script:LaunchedProc.Refresh()
        return [bool]$script:LaunchedProc.HasExited
    } catch {
        return $true
    }
}

# REQUIREMENT: on any startup failure this prints the launched PID, whether it is still
# alive, its exit code when available, the port-listener state, and the tail of both logs
# BEFORE the script returns nonzero. A failed restart must never be a bare error line.
function Show-StartupDiagnostics([string]$Reason) {
    Write-Host ""
    Write-Host ("-" * 72)
    Write-Host "BACKEND STARTUP DIAGNOSTICS"
    Write-Host ("-" * 72)
    Write-Host ("  reason             : " + $Reason)
    if ($script:LaunchedPid) {
        Write-Host ("  launched pid       : " + $script:LaunchedPid)
    } else {
        Write-Host "  launched pid       : <no process was launched>"
    }
    $alive = "unknown"
    $code = "unavailable (process still running or never launched)"
    if ($script:LaunchedProc) {
        try {
            $script:LaunchedProc.Refresh()
            $alive = (-not $script:LaunchedProc.HasExited)
            if ($script:LaunchedProc.HasExited) { $code = $script:LaunchedProc.ExitCode }
        } catch {
            $alive = "unknown (" + $_.Exception.Message + ")"
        }
    }
    Write-Host ("  pid still alive    : " + $alive)
    Write-Host ("  process exit code  : " + $code)
    $listeners = @(Get-BackendListeners)
    if (@($listeners).Count -eq 0) {
        Write-Host ("  port " + $Port + " listener : NONE - nothing is listening")
    } else {
        Write-Host ("  port " + $Port + " listener : pid(s) " + (@($listeners) -join ", "))
    }
    Write-Host ""
    Write-Host ("  last " + $DiagnosticLines + " STDERR line(s)  " + $StderrLog)
    foreach ($ln in (Read-LogTail $StderrLog $DiagnosticLines)) { Write-Host ("    " + $ln) }
    Write-Host ""
    Write-Host ("  last " + $DiagnosticLines + " STDOUT line(s)  " + $StdoutLog)
    foreach ($ln in (Read-LogTail $StdoutLog $DiagnosticLines)) { Write-Host ("    " + $ln) }
    Write-Host ("-" * 72)
}

function Fail([string]$Message) {
    Write-Host ""
    Write-Host ("RESTART_SMOKE_FAILED - " + $Message)
    exit 1
}


# --------------------------------------------------------------------------- #
# HTTP. GET is the only verb this workflow is permitted to use.
# --------------------------------------------------------------------------- #
function Invoke-CanonicalGet([string]$Path, [int]$TimeoutSec = 15) {
    $uri = "http://127.0.0.1:" + $Port + $Path
    $headers = @{}
    if ($script:ApiKey) { $headers["X-API-Key"] = $script:ApiKey }
    try {
        $r = Invoke-WebRequest -Uri $uri -Method Get -Headers $headers `
                               -UseBasicParsing -TimeoutSec $TimeoutSec
        return @{ ok = $true; status = [int]$r.StatusCode; body = [string]$r.Content }
    } catch {
        $code = 0
        $msg = $_.Exception.Message
        try {
            if ($_.Exception.Response) { $code = [int]$_.Exception.Response.StatusCode }
        } catch { }
        return @{ ok = $false; status = $code; body = $msg }
    }
}

# A 404 on a canonical readiness route is NOT a slow start. It is the wrong path, and it
# is the exact defect this owner exists to prevent - so it fails immediately and by name
# instead of silently retrying forty times.
function Wait-ForCanonicalRoute([string]$Path, [int]$TimeoutSec, [string]$Label) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    $last = $null
    while ((Get-Date) -lt $deadline) {
        if (Test-ChildExited) {
            Show-StartupDiagnostics ("the backend process exited before " + $Path +
                                     " answered - it never finished starting")
            Fail ("backend startup failed before " + $Label + " (" + $Path + ")")
        }
        $r = Invoke-CanonicalGet $Path 8
        $last = $r
        if ($r.ok -and $r.status -eq 200) { return $r }
        if ($r.status -eq 404) {
            Show-StartupDiagnostics ("GET " + $Path + " returned 404 - the application " +
                                     "does not serve this path")
            Fail ("NONCANONICAL HEALTH ROUTE: " + $Path + " is not served by " + $AppModule +
                  " - the canonical routes are " + $CANONICAL_HEALTH_PATH + " and " +
                  $CANONICAL_READY_PATH)
        }
        Start-Sleep -Seconds 2
    }
    $status = 0
    if ($last) { $status = $last.status }
    Show-StartupDiagnostics ("no HTTP 200 from " + $Path + " within " + $TimeoutSec +
                             "s (last status " + $status + ")")
    Fail ($Label + " never became available on " + $Path)
}


# =========================================================================== #
# 1. PRODUCTION STORE ROOTS - delegated to the ONE owner of that rule.
# =========================================================================== #
Write-Section "PRODUCTION STORE ROOTS"
if (-not (Test-Path $PythonExe)) { Fail ("python not found: " + $PythonExe) }
Write-Host "  rule owner: api/environment_isolation.py (not reimplemented here)"

$auditJson = & $PythonExe -c @"
import json, os, sys
sys.path.insert(0, r'$RepoParent')
from paper_trader.api import environment_isolation as EI
r = EI.audit_store_roots(dict(os.environ))
print(json.dumps({'status': r['status'], 'ok': bool(r['ok']),
                  'violations': ['%s=%s' % (v['variable'], v['value'])
                                 for v in r['violations']],
                  'message': r['message']}))
"@
if ($LASTEXITCODE -ne 0) {
    Fail "could not evaluate production store roots via api/environment_isolation.py"
}
$verdict = (@($auditJson) -join "`n") | ConvertFrom-Json
Write-Host ("  " + $verdict.status)
if (-not $verdict.ok) {
    foreach ($v in @($verdict.violations)) { Write-Host ("    " + $v) }
    Write-Host ("  " + $verdict.message)
    Fail "production store roots are contaminated - clear them in THIS shell first"
}
if ($Port -eq 8001 -and $verdict.status -ne "PRODUCTION_STORE_ROOTS_OK") {
    Fail ("the live backend port may only be started with production store roots " +
          "(got " + $verdict.status + ")")
}

# =========================================================================== #
# 2. Preflight-only unless -Force. A restart is an explicit operator decision.
# =========================================================================== #
if (-not $Force) {
    Write-Section "NOT RESTARTING"
    Write-Host ("  target        : http://127.0.0.1:" + $Port)
    Write-Host ("  app module    : " + $AppModule)
    Write-Host ("  health / ready: " + $CANONICAL_HEALTH_PATH + " , " + $CANONICAL_READY_PATH)
    Write-Host "  Re-run with -Force to actually stop and restart the backend."
    Write-Host ""
    Write-Host "RESTART_PREFLIGHT_OK"
    exit 0
}

# =========================================================================== #
# 3. STOP whatever owns the port, then verify it is free.
# =========================================================================== #
Write-Section ("STOP EXISTING LISTENER ON PORT " + $Port)
$existing = @(Get-BackendListeners)
if (@($existing).Count -eq 0) {
    Write-Host ("  nothing is listening on " + $Port)
} else {
    foreach ($ownerPid in $existing) {
        Write-Host ("  stopping pid " + $ownerPid)
        Stop-Process -Id $ownerPid -Force -ErrorAction SilentlyContinue
    }
}
$freed = $false
for ($i = 0; $i -lt 20; $i++) {
    if (@(Get-BackendListeners).Count -eq 0) { $freed = $true; break }
    Start-Sleep -Milliseconds 500
}
if (-not $freed) { Fail ("port " + $Port + " is still listening after stop") }
Write-Host ("  port " + $Port + " is free")

# =========================================================================== #
# 4. START the backend and record everything needed for diagnostics.
# =========================================================================== #
Write-Section "START BACKEND"
$uvicornArgs = @("-m", "uvicorn", $AppModule, "--host", "127.0.0.1", "--port", "$Port")
if ($AppDir) { $uvicornArgs += @("--app-dir", $AppDir) }
Write-Host ("  " + $PythonExe + " " + ($uvicornArgs -join " "))
Write-Host ("  stdout -> " + $StdoutLog)
Write-Host ("  stderr -> " + $StderrLog)
try {
    $script:LaunchedProc = Start-Process -FilePath $PythonExe -ArgumentList $uvicornArgs `
        -WorkingDirectory $RepoRoot `
        -RedirectStandardOutput $StdoutLog -RedirectStandardError $StderrLog `
        -PassThru -WindowStyle Hidden
} catch {
    Show-StartupDiagnostics ("the backend process could not be launched: " + $_.Exception.Message)
    Fail "backend process launch failed"
}
$script:LaunchedPid = $script:LaunchedProc.Id
Write-Host ("  launched pid " + $script:LaunchedPid)

# =========================================================================== #
# 5. READINESS - canonical routes only.
# =========================================================================== #
Write-Section "READINESS"
Write-Host ("  health : http://127.0.0.1:" + $Port + $CANONICAL_HEALTH_PATH)
Write-Host ("  ready  : http://127.0.0.1:" + $Port + $CANONICAL_READY_PATH)
$null = Wait-ForCanonicalRoute $CANONICAL_HEALTH_PATH $ReadyTimeoutSec "the health probe"
Write-Host ("  " + $CANONICAL_HEALTH_PATH + " -> 200")
$null = Wait-ForCanonicalRoute $CANONICAL_READY_PATH $ReadyTimeoutSec "the readiness probe"
Write-Host ("  " + $CANONICAL_READY_PATH + " -> 200")

# =========================================================================== #
# 6. EXACTLY ONE LISTENER, and it must be the process this script launched.
# =========================================================================== #
Write-Section "PORT LISTENER"
$listeners = @(Get-BackendListeners)
Write-Host ("  listening pid(s) on " + $Port + " : " + (@($listeners) -join ", "))
if (@($listeners).Count -ne 1) {
    Show-StartupDiagnostics ("expected exactly ONE listener on port " + $Port +
                             ", found " + @($listeners).Count)
    Fail ("port " + $Port + " has " + @($listeners).Count +
          " listening process(es) - exactly one backend must own it")
}
if (-not (Test-OwnedByLaunched ([int]$listeners[0]))) {
    Show-StartupDiagnostics ("port " + $Port + " is owned by pid " + $listeners[0] +
                             ", which is not the process this script launched (" +
                             $script:LaunchedPid + ") nor a descendant of it")
    Fail "the port is owned by a process this script did not launch"
}
Write-Host ("  exactly one backend listener (pid " + $listeners[0] +
            "), owned by the launched process tree")

# =========================================================================== #
# 7. AUTHENTICATED LIVE READ. The key comes from the shell, or from the ONE
#    owner of service configuration. This script never parses .env itself and
#    never prints the key.
# =========================================================================== #
Write-Section "AUTHENTICATED LIVE READ"
$script:ApiKey = $env:PAPER_TRADER_SERVICE_API_KEY
if ($script:ApiKey) {
    Write-Host "  api key source: PAPER_TRADER_SERVICE_API_KEY (this shell)"
} else {
    $keyOut = & $PythonExe -c @"
import sys
sys.path.insert(0, r'$RepoParent')
try:
    from paper_trader.config import get_settings
    print(get_settings().service_api_key or '')
except Exception:
    print('')
"@
    $script:ApiKey = (@($keyOut) -join "").Trim()
    if ($script:ApiKey) { Write-Host "  api key source: paper_trader.config (settings owner)" }
}
if (-not $script:ApiKey) {
    Show-StartupDiagnostics "no service API key is available, so no authenticated read is possible"
    Fail ("PAPER_TRADER_SERVICE_API_KEY is not set in this shell and paper_trader.config " +
          "could not supply it - the authenticated live read cannot be performed")
}

$portfolioBody = $null
foreach ($p in (@($CANONICAL_SMOKE_PATHS) + @($SmokePath))) {
    if (-not $p) { continue }
    $r = Invoke-CanonicalGet $p 45
    if (-not $r.ok -or $r.status -ne 200) {
        Show-StartupDiagnostics ("authenticated GET " + $p + " -> " + $r.status)
        Fail ("live read failed: GET " + $p + " (" + $r.status + ")")
    }
    Write-Host ("  OK  GET " + $p + " -> 200")
    if ($p -eq $CANONICAL_SMOKE_PATHS[0]) { $portfolioBody = $r.body }
}

# The read must be authenticated AND real. An empty portfolio served by a backend that
# started fine is the store-root contamination signature: a fabricated empty book that is
# indistinguishable from a real one.
$positionCount = -1
try {
    $state = $portfolioBody | ConvertFrom-Json
    $positionCount = @($state.positions).Count
} catch {
    $positionCount = -1
}
Write-Host ("  positions served: " + $positionCount)
if ($positionCount -lt 1) {
    Show-StartupDiagnostics ("the authenticated read returned " + $positionCount +
                             " position(s)")
    Fail ("the backend served an EMPTY portfolio - a fabricated book, not the real one")
}

Write-Section "RESULT"
Write-Host ("  backend pid   : " + $script:LaunchedPid)
Write-Host ("  url           : http://127.0.0.1:" + $Port)
Write-Host ("  health / ready: 200 / 200 on the canonical v1 routes")
Write-Host ""
Write-Host "LIVE_SMOKE_OK"
exit 0
