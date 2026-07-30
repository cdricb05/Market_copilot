# PROJECT_STATE

- **Last updated:** 2026-07-30
- **Updated by phase:** Alpha Agent Stage 7 / Stage 7.1 — final checkpoint sync (Gmail acceptance confirmed + full repository regression green; PROJECT_STATE only)
- **Source Git HEAD:** `3726ac7` (`Backfill historical alpha data and activate real experiments`)
- **Working tree status:** DIRTY — Stage 7, Stage 7.1, and the durable as-of-close reconciliation correction remain **combined in one uncommitted working tree** (nothing committed by either phase yet), ready to commit as the 19-file allowlist.
- **Authoritative through:** operational paper book valued through the **2026-07-29** daily close (latest completed close).
- **Next required action:** commit the approved 19-file checkpoint (below). No blockers remain. Do not push or enable scheduled tasks unless explicitly requested.

## Current objective

Make the Gmail executive email the primary, plain-English Alpha Agent product (Stage 7.1) on top of the Stage 7 read-only alpha-recovery evidence layer, WITHOUT changing any operational paper-trading behaviour. Both release blockers are now cleared: the Phase 31B live accounting-reconciliation break was resolved by teaching the reconciliation to reconstruct historical rows from the immutable, first-write-wins close-price store instead of the revisable current-marks cache, and the Gmail executive-email acceptance delivery has succeeded. The full repository regression is green. The checkpoint is ready to commit.

## Current decision

**COMMIT_OK** at the close of this pass — no release blockers remain.

- **Gmail executive-email acceptance: MET.** Configuration `GMAIL_OAUTH_CONFIGURED`; delivery `EMAIL_SENT`; Gmail message id `19fb43023d3c2f18`; diagnostic "Report delivered through Gmail API"; subject exactly `TEST — Alpha Agent Executive Research Brief — 2026-07-30`; runtime terminal `ALPHA_AGENT_STAGE4_READY`. Exactly one successful delivery; no duplicate send.
- **Phase 31B live accounting-reconciliation break: RESOLVED** (frozen-close-mark precedence; the previously failing live test passes; strict $0.01 NAV tolerance unchanged; no operational history rewritten).
- **Full repository regression: GREEN** — 4,126 passed / 4 skipped / 0 failed (4,130 collected), 246.56 s.

Research disposition is unchanged: **NEED_MORE_EVIDENCE** — the current champion can be neither confirmed nor rejected; nothing is promoted. Scheduled research and trading automation remain **OFF** (all four tasks Disabled); COMMIT_OK authorizes committing the checkpoint, not enabling any automation.

## Git checkpoint

- HEAD: `3726ac7` (2026-07-29). Not moved by this pass.
- Uncommitted work-product (Stage 7 + Stage 7.1 + this correction): 12 modified tracked files + 7 new untracked committable files = the 19-file commit allowlist (below).
- Excluded from any commit: `.claude/settings.json`, `.playwright-mcp/`, generated runtime reports (`D:\Stock_Prediction_app_data\alpha_agent\runtime\...`), generated recovery evidence on `D:`, credentials / OAuth files, logs and screenshots.

## Architecture

- Paper Trader backend + UI: `http://127.0.0.1:8001` (UI at `/ui/`). Windows PowerShell only; no Bash/WSL.
- Prediction service is remote (GCP) via local tunnel `http://127.0.0.1:9000`; never run locally.
- Operational paper-trading desk store (append-only, chain-hashed ledgers): `C:\Users\binis\.paper_trader\paper_trading_desk`.
- Alpha Agent research artifacts (immutable, deterministic) live off-repo on `D:\Stock_Prediction_app_data\alpha_agent\...`.
- Preview-first only: no Create Orders, no order execution, no automation. Prediction/GCP is read-only through the tunnel.

## Operational paper portfolio

- **Alpha Paper Book #1**, single active book. Inception funding $100,000.
- 25 held names, long-only paper book; valued through the **2026-07-29** completed close.
- Latest scorecard (canonical `report_renderer.scorecard`, as rendered 2026-07-30 from the 2026-07-29 close): NAV **$98,125.23**, P/L today **-$443.45**, P/L since inception **-$1,874.77**, vs SPY **+0.53 pp**.
- This pass performed **no** operational mutation: no order/fill/signal/decision/model-promotion/holding/target/cash change; no PostgreSQL write; no Daily Close; no prediction-service call. Operational ledgers are byte-identical before and after (17 files; aggregate SHA-256 `97DD9F750A3E4B07AFB26E9EB6E2298FF11271799BE3DD4DEC838BD3EF8B0B66`, unchanged across the reconciliation edit, the test run and the Gmail attempt).

## Alpha Agent stages

- Stage 1–3.5 (ingestion, research director, news/RSS): committed through `7e550f8`.
- Stage 4 persistent runtime + Gmail OAuth reports: committed `7286aaa`.
- Stage 5 experiment engine: committed `4bb41cf`.
- Stage 6 historical backfill + real experiments: committed `3726ac7` (HEAD).
- **Stage 7 (alpha recovery) + Stage 7.1 (executive email / forward shadows / recurring cadence) + this durable-reconciliation correction: UNCOMMITTED**, combined in the working tree on top of HEAD.
- Latest verified Stage 7 recovery run: `stage7_f2d4dfc3895a3667`, as-of 2026-07-29, disposition **NEED_MORE_EVIDENCE**, terminal `ALPHA_AGENT_STAGE7_READY`, upstream fingerprint `4c309f55a02d2281`.

## Research evidence

- Champion autopsy: fundamental leg **UNVERIFIABLE** (no PIT fundamentals); price leg rank-IC ≈ t 0.23; forward sample below the 20-observation adequacy floor → **NEED_MORE_EVIDENCE**, nothing promoted.
- Risk-overlay tournament: deterministic, read-only; three overlays selected for forward shadow tracking — CURRENT_CONTROL, MARKET_REGIME_CASH_OVERLAY, PORTFOLIO_VOL_TARGET_20 (forward scales 1.0 / 1.0 / 0.4166; cash 0.047 / 0.047 / 0.603).
- Historical data readiness: Price history READY (2015→2026-07-29, 572 tickers, survivorship-free); Historical membership READY (survivorship-aware); PIT fundamentals NOT READY; Earnings history NOT READY; Historical sector classifications NOT READY.

## Safety state

- Scheduled tasks `AlphaAgent-Collect`, `AlphaAgent-Morning-Report`, `AlphaAgent-PostClose-Report`, `AlphaAgent-Watchdog`: **all Disabled** (re-confirmed this pass). No fifth task exists.
- Status flags carried in every executive report and endpoint: RESEARCH SCHEDULE: ON / TRADING AUTOMATION: OFF / BROKER EXECUTION: OFF / PAPER ONLY.
- No `alert()` / `confirm()` / `prompt()` in the UI (scan: 0 occurrences); no Create Orders control; no automation control.
- Forward risk shadows are SHADOW-ONLY: they never change the active paper portfolio.

## Validation state

- **Full repository regression: 4,126 passed / 4 skipped / 0 failed** (4,130 collected), runtime 246.56 s — the one final full-suite run is green.
- **Phase 31B file: 44 passed** (was 35) — includes 9 new durable-reconciliation tests and the previously failing live test `TestAttributionAndSignal::test_accounting_and_attribution_reconcile_live`, which now **PASSES**.
- Focused battery (Stage 4 runtime + Stage 5 + Stage 6 + Stage 7 recovery + api + Phase 31B): **1052 passed / 817 skipped**.
- Python compile: clean (`PY_COMPILE_OK`) on all 15 modified Python files.
- JSON validation: both modified config JSONs parse (`ALL_JSON_VALID`).
- `git diff --check`: clean (LF→CRLF advisories only).
- Secret scan of the committable footprint: clean (the only match is a self-labelled dummy `"test-secret-not-real"` in a Stage 4 test fixture).
- Operational ledger hash: byte-identical throughout the resolution work (research/read-only only).

## Release blockers — ALL RESOLVED

### 1. Gmail executive-email delivery — RESOLVED

- Gmail OAuth is configured (`GMAIL_OAUTH_CONFIGURED`) and the Stage 7.1 executive report was delivered: `EMAIL_SENT`, Gmail message id `19fb43023d3c2f18`, diagnostic "Report delivered through Gmail API", runtime terminal `ALPHA_AGENT_STAGE4_READY`.
- The delivered report rendered deterministically with the exact subject `TEST — Alpha Agent Executive Research Brief — 2026-07-30`. Exactly one successful delivery; no duplicate send.
- The refresh token is DPAPI-encrypted under `C:\Users\<you>\.paper_trader\alpha_agent_email\` and is never printed or committed; no credential value appears in the repo or this file.

### 2. Phase 31B live accounting reconciliation — RESOLVED

- **Root cause (was):** the accounting reconciliation reconstructed historical invested value from the mutable `desk_marks.json` current-marks cache. The 2026-07-29 close revised a prior-date vendor adjusted close (FANG 2026-07-28: 190.305 → 190.31), so the frozen 2026-07-28 NAV ($93,938.37 invested) was reconciled against a later-revised mark ($93,938.47), a benign +$0.10 (0.0001% of NAV) that tripped the strict $0.01 tolerance. Exactly one ticker, one date: 20 FANG shares × $0.005/sh = $0.10.
- **Evidence found:** the immutable, first-write-wins price store `forward_prediction_prices.json` (`kind = prediction_price_store_first_write_wins`, exposed to the engine as `S["price_series"]`) preserves the original as-of-close mark for every (ticker, date). It reconstructs **all six** operational rows to ±$0.0000, including 2026-07-28 to exactly $93,938.37.
- **Correction (in-footprint):** `build_accounting_reconciliation` now reconstructs each row under an explicit source precedence — (1) `FROZEN_CLOSE_MARKS` (exact immutable as-of-close mark) then (2) `HISTORICAL_MARK_FALLBACK` (revisable `desk_marks`, used only when no frozen mark exists and flagged `not fully reproducible`). Each row and the summary carry the provenance; the $0.01 NAV / 1.0 bps return tolerances are **unchanged**. Live result: all 6 rows `FROZEN_CLOSE_MARKS`, max NAV residual $0.00.

### 3. Immutable-evidence incident (Stage 7.1) — RESOLVED going forward, documented here

- During Stage 7.1's live proof, the Stage 7 recovery package `stage7_f2d4dfc3895a3667` was rewritten in place under the identical deterministic run id, because the run id was derived from (as-of, config, upstream evidence) but not the package schema/engine version. No byte-identical backup exists; no restoration was fabricated.
- Permanent protection added (in-footprint): `stage7_run_id` now binds package schema + engine version; `assemble_and_write` refuses to overwrite a non-empty existing run directory unless its recorded `identity` matches exactly (idempotent no-op), otherwise raises `ImmutableRunError`; the manifest records an `identity` block; four deterministic tests prove the guarantees. Stage 7 recovery suite green.

## Exact next action

**Commit the approved 19-file checkpoint (allowlist below).** Both blockers are cleared (Gmail delivered, message id `19fb43023d3c2f18`; Phase 31B reconciliation resolved) and the full repository regression is green (4,126 passed / 4 skipped / 0 failed). Do not push, and do not enable any scheduled task, unless the user explicitly requests it.

## Commit allowlist (19 files)

Tracked (modified):
`alpha_agent/__init__.py`, `alpha_agent/experiment_runner.py`, `alpha_agent/report_renderer.py`, `alpha_agent/runtime.py`, `api/app.py`, `api/ui/index.html`, `configs/alpha_agent/stage4_runtime.json`, `engine/absolute_return_research.py`, `scripts/run_alpha_agent.py`, `tests/test_alpha_agent_stage4_runtime.py`, `tests/test_api.py`, `tests/test_phase31b_absolute_return_research.py`

Untracked (new):
`alpha_agent/champion_forensics.py`, `alpha_agent/evidence_observatory.py`, `alpha_agent/risk_overlay_research.py`, `configs/alpha_agent/stage7_alpha_recovery.json`, `scripts/run_alpha_recovery.py`, `tests/test_alpha_agent_stage7_alpha_recovery.py`, `PROJECT_STATE.md`

The two files added to the footprint by this pass — `engine/absolute_return_research.py` and `tests/test_phase31b_absolute_return_research.py` — are the durable-reconciliation correction and its tests (the brief's pre-authorized potential footprint).

## Checkpoint history

- `7e550f8` — Stage 1–3.5 ingestion + research director + news/RSS.
- `7286aaa` — Stage 4 persistent runtime + Gmail OAuth reports.
- `4bb41cf` — Stage 5 experiment + evidence engine.
- `ed8a751` — Phase 31B joint portfolio caps fix.
- `3726ac7` — Stage 6 historical backfill + real experiments (current HEAD).
- (uncommitted, COMMIT_OK) — Stage 7 alpha recovery + Stage 7.1 executive email / forward shadows / recurring cadence + immutable-evidence protection + PROJECT_STATE.md + **durable as-of-close reconciliation** (frozen-close-mark precedence in `engine/absolute_return_research.py`; Phase 31B live reconciliation now passes). Gmail executive-email acceptance confirmed (message id `19fb43023d3c2f18`) and full repository regression green (4,126 passed / 4 skipped / 0 failed) — ready to commit as the 19-file checkpoint.
