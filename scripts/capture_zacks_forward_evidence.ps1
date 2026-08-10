# capture_zacks_forward_evidence.ps1
# Stage 13B MANUAL operator command: one bounded, idempotent capture of every
# entitled current Zacks feed, followed by the prospective forward-evidence
# ledger update (ingest -> form -> mature -> status).
#
# SAFETY: research-only. No scheduled task, no cadence, no orders, no fills,
# no portfolio/target/NAV mutation, no Daily Close, no Daily Research Cycle,
# no champion promotion. Credentials are resolved inside the released collector
# (env/DPAPI) and are never printed. Re-running on the same day is idempotent:
# already-captured feeds report ALREADY_CAPTURED and the ledger reports
# idempotent repeats instead of new rows.
#
# Usage (manual, from the repo root):
#   powershell -ExecutionPolicy Bypass -File scripts\capture_zacks_forward_evidence.ps1
#   ... optional: -Rate 1.0   (seconds between provider requests; raise if 429)

param(
    [double]$Rate = 0.5
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
$py = Join-Path $repo ".venv-win\Scripts\python.exe"
$acquire = Join-Path $repo "scripts\intrinio_trial_acquire.py"
$stage13b = Join-Path $repo "scripts\stage13b_analyst_expectations.py"

if (-not (Test-Path $py)) { Write-Host "FAIL: canonical python not found: $py"; exit 2 }

Write-Host "== Stage 13B forward-evidence capture (manual, idempotent) =="
Write-Host "   research-only / NO ORDERS / NO AUTOMATION / MANUAL REVIEW"

# 1) surprises snapshot (global cross-section; idempotent per business date).
#    Surprise feeds ONLY: the global estimate walks are 401-depth-walled and
#    vintage-inflated -- per-member estimates come from step 2.
Write-Host "-- [1/5] Zacks surprises snapshot"
& $py $acquire --zacks-snapshot --snapshot-feeds "eps_surprises,sales_surprises" --rate $Rate
if ($LASTEXITCODE -ne 0) { Write-Host "WARN: surprises snapshot exit $LASTEXITCODE (continuing)" }

# 2) per-member estimates snapshot, BOTH feeds (idempotent per business date;
#    a feed that already captured today reports ALREADY_CAPTURED and is skipped)
Write-Host "-- [2/5] Zacks per-member estimates snapshot (eps+sales)"
& $py $acquire --zacks-estimates-universe --feeds "eps,sales" --rate $Rate
if ($LASTEXITCODE -ne 0) { Write-Host "WARN: estimates snapshot exit $LASTEXITCODE (continuing)" }

# 3) ledger ingest (append-only; identical provider state = idempotent repeats)
Write-Host "-- [3/5] prospective ledger ingest"
& $py $stage13b --ingest
if ($LASTEXITCODE -ne 0) { Write-Host "FAIL: ledger ingest exit $LASTEXITCODE"; exit 3 }

# 4) frozen signal formation for today's snapshot
Write-Host "-- [4/5] frozen signal formation"
& $py $stage13b --form
if ($LASTEXITCODE -ne 0) { Write-Host "FAIL: signal formation exit $LASTEXITCODE"; exit 4 }

# 5) maturation sweep + derived status (horizons mature ONLY when the owned
#    session calendar has genuinely elapsed; PENDING is derived, never stored)
Write-Host "-- [5/5] maturation sweep + forward-evidence status"
& $py $stage13b --mature --status
if ($LASTEXITCODE -ne 0) { Write-Host "FAIL: maturation/status exit $LASTEXITCODE"; exit 5 }

Write-Host "== DONE. Status JSON: D:\Stock_Prediction_app_data\provider_trials\intrinio\stage13b_prospective_ledger\forward_evidence_status.json =="
exit 0
