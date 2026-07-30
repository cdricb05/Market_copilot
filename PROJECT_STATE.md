# PROJECT_STATE

- **Last updated:** 2026-07-30
- **Updated by phase:** Alpha Agent Stage 7.2 — executive-brief quality + reporting-contract repair, final live validation, then **Gmail SMTP transport cutover (retiring OAuth as the active transport)**
- **Source Git HEAD:** `4c158e9` (`Add Alpha Agent Stage 7 recovery and executive reporting`)
- **Working tree status:** DIRTY — Stage 7.2 reporting-quality changes + live-validation wiring fixes + the Gmail SMTP cutover are uncommitted. Nothing committed by this phase. The SMTP cutover changes **9 files** (see Commit allowlist): `alpha_agent/runtime.py`, `configs/alpha_agent/stage4_runtime.json`, five new `scripts/*_alpha_agent_smtp.*`, `tests/test_alpha_agent_stage4_runtime.py`, `PROJECT_STATE.md`. The earlier Stage 7.2 files remain modified.
- **Authoritative through:** operational paper book valued through the **2026-07-29** daily close (latest completed close).
- **Current decision:** **COMMIT_OK.** Gmail OAuth is retired as the active transport (its refresh token was rejected server-side with `invalid_grant`/HTTP 400; the OAuth code is retained but disabled by config). Paper Trader's primary and only active email transport is now **Gmail SMTP** (`smtp.gmail.com:587`, STARTTLS) with a dedicated Google **App Password** stored ONLY as a Windows DPAPI blob. The user configured the App Password; SMTP acceptance PASSED: **three** `SMTP_AUTHENTICATION_OK` diagnostics (pre-send, pre-send delayed ≥60 s, post-send ≥60 s) and **exactly one** `EMAIL_SENT` v2 email via `gmail_smtp` with non-empty RFC Message-ID `<178545049155.18856.4662823154049402559@gmail.com>`. No OAuth token exchange, no OAuth authorization, no watchdog. All code/tests/scans are green; the operational ledger is byte-identical; all four scheduled tasks remain Disabled.
- **Next required action:** commit the allowlist below and push (an explicit-path commit is given in "Commit + deploy commands"). Then restart the backend onto the committed tree and run the UI smoke test. Do not enable scheduled tasks.

## Current objective

Correct the delivered executive email from an improved-but-not-executive-grade report into a compact, plain-English, one-minute brief, and fix the canonical report/API/UI reporting contract so every surface agrees. Reporting-quality and reporting-contract correction only — no operational trading behaviour changed.

## What Stage 7.2 changed

Executive email (`alpha_agent/report_renderer.py`) rewritten to a **compact six-section brief** followed by a clear separator and a **five-line audit appendix**:

1. Bottom line 2. Your action today 3. Portfolio today 4. Research progress 5. Risk experiments 6. Data and system issues.

All nine email defects are corrected:

- **Mixed-period opening** fixed — each dollar figure is paired with the percent from the SAME period (today with today, since-launch with since-launch); periods are never mixed in one metric statement.
- **False schedule status** fixed — the automatic-research-schedule state is derived canonically from the real Windows scheduled-task states (`resolve_schedule_status` → `schedule_status`), never hardcoded to ON. All four tasks Disabled → the report says **Automatic research schedule: Off**; uncollectable state → "Not verified". Manual report generation, automatic scheduled delivery, trading automation, broker execution and paper-only are kept as separate, clearly-stated facts.
- **Conflicting experiment summaries** fixed — one canonical, exactly-reconciling research-decision summary (`research_decision_reconciliation`) with plain-English categories (rejected as noise / rejected as unstable / could-not-run / kept for research); Stage 5 data-held studies are reported SEPARATELY and never merged into the Stage 7 evaluated count.
- **Repeated verdict** fixed — the research verdict is stated exactly once in the body.
- **Excess technical language** removed from the body — a forbidden-jargon scan (point-in-time, rank-IC, t-statistic, reconstruction, overlay, forward scale, provider classification, machine/data-hold tokens, …) passes on the executive body; technical terms remain only in the compact appendix / the API-UI observatory.
- **Meaningless zero changes** suppressed — when nothing material moved the change note collapses to a single sentence; no `$0.00 / 0.00% / 0.00 pp` lines.
- **Shadow interpretation** fixed — the lower-risk shadow is explained by its ~60% cash; with only six observations realized volatility is explained as not-yet-measurable (never a bare "Not available"); benchmark differences use **percentage points**; cash levels carry no `+` sign.
- **Excessive appendix** trimmed — no local file paths, no per-stage run-id list, no provider internals in the email; one latest-research-run reference only.
- **Visual hierarchy** — one headline, one action card, one five-row portfolio table, one three-row shadow table, one exception-only data/system block; mobile-safe, dark-mode-tolerant, readable without colour.

Reporting-contract corrections (`evidence_observatory.py`, `api/app.py`, `api/ui/index.html`):

- **UTF-8 / mojibake** — stage labels are pure ASCII (no em-dash), eliminating the "Stage 1 [garbled-dash] Research registry" corruption on every surface; a regression test rejects the corrupted-UTF-8 markers (the mis-decoded em-dash and the Unicode replacement character).
- **Non-null canonical fields** — report date (`today`), market-data-through date, champion model, book name, holdings count, invested percentage and the Stage 6 date window are populated when the source has them.
- **Separated counters** — Stage 2 normalized ingestion, Stage 6 historical backfill, Stage 5 experiments completed and Stage 7 recovery ideas evaluated are distinct, scope-labelled counters (`counter_breakdown`), never merged.
- **Single-source contracts** — feed-health counts (`news_rss_health`), the schedule state (`schedule_status`) and the scorecard strings come from one canonical source shared by the email, API and UI; the UI shadow table uses percentage points and unsigned cash to match the email.

## Final live validation (backend restarted onto the current tree)

The local backend was stopped and relaunched (same `uvicorn api.app:app` command, port 8001) so the running process serves the current working tree, then `/v1/health`, `/v1/ready` and `/v1/research/alpha-agent-observatory` were queried live. The first pass exposed **real source-to-payload wiring gaps** (not a stale process): several critical fields returned null. These were fixed (no arbitrary defaults — every value comes from real evidence):

- **Nested operational-book read** (`api/app.py::_alpha_agent_book_context`): `load_operational_book()` nests the book fields under `operational_book` / `canonical_state`, not at the top level. The context was reading the (absent) top-level keys, so `champion_model` / `book_name` / `holdings_count` / `invested_pct` / `market_data_through` came back null while `nav` survived only via the `PortfolioReader` fallback. Now reads the nested block → all populated (`fundamental_momentum_50_50_v1`, `Alpha Paper Book #1`, 25, 95.28, `2026-07-29`).
- **Recovery-experiment count** (`evidence_observatory.py::_stage7`): counts the immutable per-experiment result rows (`alpha_experiment_results.jsonl` = 7) so `highlights.recovery_experiments_evaluated` and `counter_breakdown.stage7_recovery_experiments_evaluated` are non-null (7), kept distinct from Stage 5 experiments.
- **Top-level `source_health`** (`evidence_observatory.py::observatory_payload`): added `{feeds_healthy, feeds_total}` from the ONE canonical feed-health source shared with the email/UI.
- **Feed-health parse fix** (`evidence_observatory.py::_stage3_5`): the package CSV column is `health` (values `HEALTHY` / `HEALTHY_NOT_MODIFIED` / `CIRCUIT_OPEN`); the reader was counting a non-existent `status` column and ignoring the 304-not-modified healthy state, yielding a false 0. Now 7/11 — agreeing with `source_health` and `news_rss_health` across every surface.

After a second restart, the live payload returns **all 13 required critical fields non-null** (`today`, `market_data_through`, `operational_paper_book.{champion_model,book_name,holdings_count,invested_pct}`, `stages.stage6.{date_start,date_end}`, `highlights.recovery_experiments_evaluated`, `counter_breakdown.stage7_recovery_experiments_evaluated`, `schedule_status`, `source_health.{feeds_healthy,feeds_total}`) and is **mojibake-free** (no mis-decoded em-dash or replacement-character markers; all stage labels pure ASCII). Four focused regression tests lock these fixes in `tests/test_api.py`.

## Gmail root cause — verified project/account; publishing status pending (no local mismatch)

The rejection is Google returning `invalid_grant` (HTTP 400) on the refresh-token exchange (`send_alpha_agent_email.py::_refresh_access_token` → `OAUTH_REAUTHORIZATION_REQUIRED`). Non-secret metadata, gathered read-only:

- **Credential-path consistency:** the configure script default (`%USERPROFILE%\.paper_trader\alpha_agent_email`), the send wrapper, and the runtime `email.credential_dir` all resolve to the **identical** directory, token file (`gmail_oauth_refresh_token.dpapi`), account file and Windows user. No path/profile mismatch.
- **Token file:** minted **2026-07-30 14:01:02** (the time of the last successful send, `19fb43023d3c2f18`), 846 bytes, SHA-256 `5FAF6953…0BEE`; token/account write-times consistent (single configuration — **not** rewritten after 14:01). A stray, differently-named `gmail_credential.dpapi` (2026-07-29) exists but is **not** read by the current code.
- **Timeline:** the fresh token delivered at 14:01, then the v2 attempt at 15:50 failed — revoked server-side within ~2 hours with the file untouched and all tasks Disabled (nothing local rotated it).
- **OAuth project & client (verified this pass, no secrets printed):** project `stock-prediction-app-466420` (number `1074874095761`); OAuth client type **Desktop (`installed`)**; client-file SHA-256 `6423D404677AD1C53F9DEB2A7B27F11F1E19837DB8BCA67ACFF5F9DADD123A32` (417 bytes); standard Google endpoints; scope `gmail.send`. Configure default and runtime read this one client file — identical config.
- **Account identity:** the stored sender (`gmail_oauth_account.txt`) is `binisti@gmail.com`; the local `gcloud` CLI is authenticated as the **same** account and can mint `cloud-platform` access tokens for this project (so binisti@gmail.com administers it). Sender identity and authorizing account agree.
- **Publishing status — NOT programmatically verifiable:** no `gcloud` command or public REST API returns the consent-screen publishing status (Testing vs In production); a read-only IAP OAuth-brands probe returned HTTP 400 and would not expose it regardless. **Internal is ruled out** — a personal `@gmail.com` project cannot use an Internal consent screen — so the status is **Testing or In production**, and must be read by the user directly from **Google Auth Platform → Audience**. It is **not** inferred from `invalid_grant`.
- **Exact Google error (one read-only token-exchange probe, no email sent):** `invalid_grant` / "Bad Request" / HTTP 400 → classification `TOKEN_EXCHANGE_INVALID_GRANT`.

**Conclusion:** the proximate cause is confirmed — Google rejects the refresh token with `invalid_grant` (HTTP 400) — with **no local mismatch** (paths, profile, client file, and sender/authorizing account all consistent). The *underlying* cause is **not yet proven**: it depends on the OAuth app's publishing status, which is Console-only and unverified. If Testing, the same-day revocation is expected Testing-mode behaviour and the remedy is to publish to Production then re-authorize once; if already In production, the `invalid_grant` points to another cause (grant revoked, too many outstanding refresh tokens, client-secret rotation, or a mismatched Google account) to investigate before reauthorizing. It is **not** asserted as Testing without direct Console evidence. A new read-only diagnostic (`scripts/diagnose_alpha_agent_gmail.{py,ps1}`) makes future failures distinguishable (`TOKEN_FILE_NOT_FOUND`, `TOKEN_FILE_CHANGED_AFTER_CONFIGURATION`, `TOKEN_EXCHANGE_INVALID_GRANT`, `_CLIENT_MISMATCH`, `_ACCOUNT_MISMATCH`, `_POLICY_REJECTION`, `_UNREACHABLE`) without sending an email or exposing any secret.

## Gmail SMTP transport cutover (this pass)

OAuth's refresh-token lifecycle was abandoned per the cutover brief; Gmail SMTP with an App Password is now the primary transport.

- **Architecture:** `smtp.gmail.com:587`, STARTTLS, account `binisti@gmail.com`, authenticated with a **dedicated** Google App Password (separate from any other app, independently revocable). Delivery sequence is fixed: connect → `ehlo` → `starttls(secure ctx)` → `ehlo` → `login` → `send_message` → `quit`. SMTP debug output is never enabled, so the AUTH exchange is never printed.
- **Secure credential storage:** the App Password is entered via `Read-Host -AsSecureString`, normalised in memory (spaces stripped; exactly 16 alphanumerics enforced), DPAPI-encrypted for the current Windows user and written as `gmail_smtp_app_password.dpapi` (+ non-secret `gmail_smtp_account.txt`) under `C:\Users\binis\.paper_trader\alpha_agent_email\`. It is **never** placed in source, `.env`, a plaintext file, a command-line argument, an environment variable, a log, or test output. The send/diagnostic wrappers decrypt it in memory and pass it to Python over redirected **stdin only**.
- **RFC Message-ID:** SMTP returns no Gmail-API id, so the sender generates an RFC 5322 `Message-ID` (`<…@gmail.com>`) and returns it as the canonical, non-empty email identifier.
- **Runtime dispatch:** `email.transport = "gmail_smtp"` selects SMTP; `resolve_email_transport` defaults to SMTP and only the explicit legacy value selects OAuth. **No automatic fallback** and never both transports in one cycle. The OAuth sender is retained (`_make_oauth_email_sender`) for reference but is unreachable while SMTP is selected. New statuses: `EMAIL_SMTP_CREDENTIAL_MISSING` (→ credential-required terminal), `EMAIL_SMTP_AUTHENTICATION_REJECTED` / `EMAIL_SMTP_TLS_FAILED` (non-retryable), `EMAIL_SMTP_CONNECTION_FAILED` / `EMAIL_SEND_FAILED` (transient).
- **Idempotency identity `exec_test_v2`:** the SMTP acceptance runs under key `exec_test_v2`, whose derived cycle id for 2026-07-30 is `cyc_455fdbbeec9c25b1` — the same v2 cycle the prior OAuth attempt used. That prior failure (`OAUTH_REAUTHORIZATION_REQUIRED`, non-retryable) did **not** block the SMTP send: report-cycle idempotency guards only on a prior `EMAIL_SENT` (success), never on a failure, so the new send proceeded and produced **exactly one** `EMAIL_SENT` with no duplicate. The failed OAuth entry stays inert in `outbox/failed` and is never auto-retried.
- **BOM fix (real defect found + fixed during acceptance):** the first live diagnostic returned `SMTP_CONNECTION_FAILED` — root-caused to a Windows stdin pipe prepending a UTF-8 BOM (U+FEFF) that `str.strip()` does not remove, corrupting the App Password and raising an uncaught `UnicodeEncodeError` at AUTH. Fixed by stripping the BOM in `_read_app_password_from_stdin` (both the sender and the diagnostic) and defensively catching `UnicodeError` in the login phase; a regression test locks it. After the fix, all three diagnostics returned `SMTP_AUTHENTICATION_OK`.
- **New files:** `scripts/configure_alpha_agent_smtp.ps1`, `scripts/send_alpha_agent_smtp.py`, `scripts/send_alpha_agent_smtp.ps1`, `scripts/diagnose_alpha_agent_smtp.py`, `scripts/diagnose_alpha_agent_smtp.ps1`.
- **SMTP diagnostic results:** `SMTP_AUTHENTICATION_OK` × 3 — first (pre-send), second (≥60 s later), and final (post-send, ≥60 s after the send). App-Password DPAPI blob: 526 bytes, SHA-256 `D8E3DA258CAF9AD0071A0CC1CC65C3FDFE44214A552FFFE8C68A9272CC702875`, written 2026-07-30T18:19:28 (account `binisti@gmail.com`).
- **V2 delivery status:** **SENT.** `email_status=EMAIL_SENT`, `email_transport=gmail_smtp`, RFC Message-ID `<178545049155.18856.4662823154049402559@gmail.com>`, subject exactly `TEST — Alpha Agent Executive Brief v2 — 2026-07-30`, exactly **one** delivery (DB `email_deliveries` for the cycle: `EMAIL_SENT`×1; the old `OAUTH_REAUTHORIZATION_REQUIRED`×1 is the inert prior failure). Preflight passed (six sections, audit appendix, no forbidden jargon / machine tokens / local paths, plain-text + HTML both non-empty). No OAuth exchange, no watchdog.

## Current decision detail

**COMMIT_OK.** All SMTP code, scripts, runtime dispatch, config and tests are complete and green; scans are clean; the operational ledger is byte-identical; all four scheduled tasks remain Disabled. The user configured the App Password and the SMTP acceptance passed end-to-end: three `SMTP_AUTHENTICATION_OK` diagnostics and exactly one `EMAIL_SENT` v2 email over `gmail_smtp` with a non-empty RFC Message-ID.

- **Gmail v2 acceptance: MET** — exactly one v2 email delivered via Gmail SMTP; subject `TEST — Alpha Agent Executive Brief v2 — 2026-07-30`; Message-ID `<178545049155.18856.4662823154049402559@gmail.com>`; no duplicate; the prior failed OAuth attempt did not block it.
- Research disposition unchanged: **NEED_MORE_EVIDENCE** — nothing promoted.
- Scheduled research and trading automation remain **OFF** (all four tasks Disabled). No fifth task was created.

## Architecture

- Paper Trader backend + UI: `http://127.0.0.1:8001` (UI at `/ui/`). Windows PowerShell only; no Bash/WSL.
- Prediction service is remote (GCP) via local tunnel `http://127.0.0.1:9000`; never run locally.
- Operational paper-trading desk store (append-only, chain-hashed ledgers): `C:\Users\binis\.paper_trader\paper_trading_desk`.
- Alpha Agent research artifacts (immutable, deterministic) live off-repo on `D:\Stock_Prediction_app_data\alpha_agent\...`.
- Preview-first only: no Create Orders, no order execution, no automation.

## Operational paper portfolio

- **Alpha Paper Book #1**, single active book. Inception funding $100,000.
- 25 held names, long-only paper book; valued through the **2026-07-29** completed close.
- Canonical scorecard (as rendered 2026-07-30): NAV **$98,125.23**, P/L today **-$443.45 (-0.45%)**, P/L since inception **-$1,874.77 (-1.87%)**, SPY since inception **-2.40%**, ahead of SPY **+0.53 pp**.
- This phase performed **no** operational mutation: no order/fill/signal/decision/model-promotion/holding/target/cash change; no PostgreSQL write; no Daily Close; no prediction-service call; no Alpha Agent recovery run. Operational ledgers byte-identical before and after (17 files; aggregate SHA-256 `97DD9F750A3E4B07AFB26E9EB6E2298FF11271799BE3DD4DEC838BD3EF8B0B66`).

## Validation state

- **Full repository regression (user-run, previously recorded):** 4,147 passed / 4 skipped / 0 failed. NOT re-run this pass (per brief; the user runs the one final full suite). The SMTP cutover added ~24 tests to `test_alpha_agent_stage4_runtime.py`, so the final full-suite count will rise accordingly when the user re-runs it.
- **Focused tests this pass:** `test_alpha_agent_stage4_runtime.py` → **164 passed** (incl. ~25 new SMTP tests: port 587, STARTTLS-before-auth, multipart/alternative, UTF-8 subject/body, RFC Message-ID, auth/TLS/connection/send failure mapping, credential-missing, no-CLI/env/plaintext-leak, DPAPI-only + 16-char validation, read-only diagnostic, SMTP-only dispatch (no OAuth, one transport), non-retry of failed OAuth, `exec_test_v2` identity, no ledger mutation, no new task, and the **stdin-BOM regression** test). `test_api.py` Observatory/email selection → 15 passed / 4 skipped. The complete suite was NOT re-run this pass.
- **SMTP acceptance (live):** three read-only diagnostics returned `SMTP_AUTHENTICATION_OK` (pre-send, +65 s, and post-send +65 s); exactly one v2 email `EMAIL_SENT` over `gmail_smtp` with RFC Message-ID `<178545049155.18856.4662823154049402559@gmail.com>`; preflight clean; `email_deliveries` shows `EMAIL_SENT`×1 for the cycle; outbox job moved to `sent/`.
- **Live backend validation:** backend restarted onto the current tree; `/v1/health` + `/v1/ready` ok; `/v1/research/alpha-agent-observatory` returns all 13 critical fields non-null and is mojibake-free; feed health agrees 7/11 across `source_health`, `news_rss_health` and the raw `stage3_5` block.
- **OAuth verification (prior pass, retained for history):** the OAuth client file was read safely (no secrets printed) → project `stock-prediction-app-466420`, Desktop client; `gcloud` authenticated as `binisti@gmail.com`. Its refresh token was rejected `TOKEN_EXCHANGE_INVALID_GRANT`; OAuth is now retired as the active transport, so this no longer blocks delivery.
- Python compile: clean on all changed/new Python (SMTP sender + diagnostic + runtime + tests).
- PowerShell syntax (AST parse): clean on the three new SMTP scripts.
- JSON validation: `stage4_runtime.json` parses and is secret-free (`rc.scan_for_secrets` == []).
- `git diff --check`: clean (LF→CRLF advisories only).
- Mojibake scan over the SMTP footprint: clean (the three new `.ps1` are pure ASCII — em-dashes were removed to avoid a PowerShell-5.1 CP1252 misdecode).
- Secret-value scan over the SMTP footprint: clean; the only hits are pre-existing, explicitly-fake test fixtures (`_REFRESH_TOKEN`/`_ACCESS_TOKEN` `…FAKE…` and redaction-test `sk-ant-abcdef…` strings), none introduced by the SMTP work, and no App Password appears anywhere.
- Native-dialog scan: no `alert()`/`confirm()`/`prompt()` (no HTML changed by the SMTP work).
- Operational ledger hash: byte-identical throughout — `97DD9F750A3E4B07AFB26E9EB6E2298FF11271799BE3DD4DEC838BD3EF8B0B66` (17 files).
- Scheduled tasks: all four Disabled (`Get-ScheduledTask` probe → State Disabled). No fifth task.

## Release blocker

### Gmail v2 executive-brief delivery — RESOLVED (Gmail SMTP)

- The prior OAuth blocker is retired. Gmail SMTP with a DPAPI-stored App Password is live and the v2 acceptance email was delivered (`EMAIL_SENT`, `gmail_smtp`, Message-ID `<178545049155.18856.4662823154049402559@gmail.com>`, exactly one delivery). No open release blocker remains for email delivery.
- The App Password is DPAPI-encrypted under `C:\Users\<you>\.paper_trader\alpha_agent_email\` (`gmail_smtp_app_password.dpapi`) and is never printed or committed.

## Commit + deploy commands

Stage exactly the allowlist (never `git add -A`), commit, and push:

```powershell
Set-Location "C:\Users\binis\paper_trader"
git add `
  alpha_agent/runtime.py `
  configs/alpha_agent/stage4_runtime.json `
  scripts/configure_alpha_agent_smtp.ps1 `
  scripts/send_alpha_agent_smtp.py `
  scripts/send_alpha_agent_smtp.ps1 `
  scripts/diagnose_alpha_agent_smtp.py `
  scripts/diagnose_alpha_agent_smtp.ps1 `
  tests/test_alpha_agent_stage4_runtime.py `
  PROJECT_STATE.md `
  alpha_agent/report_renderer.py `
  alpha_agent/evidence_observatory.py `
  api/app.py `
  api/ui/index.html `
  tests/test_alpha_agent_stage5_experiment_factory.py `
  tests/test_alpha_agent_stage6_historical_backfill.py `
  tests/test_api.py `
  scripts/diagnose_alpha_agent_gmail.py `
  scripts/diagnose_alpha_agent_gmail.ps1
git commit -m "Cut Alpha Agent email over to Gmail SMTP (App Password via DPAPI)"
git push origin main
```

Restart the backend onto the committed tree and smoke-test the UI:

```powershell
# stop the running uvicorn (PID from: Get-CimInstance Win32_Process -Filter "name='python.exe'" | ? {$_.CommandLine -like '*uvicorn*8001*'})
C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe -m uvicorn api.app:app --host 127.0.0.1 --port 8001
# then browse http://127.0.0.1:8001/ui/ (1920x1080) and GET /v1/research/alpha-agent-observatory
```

## Commit allowlist (17 files)

**SMTP cutover this pass (9 files):**
Tracked (modified): `alpha_agent/runtime.py` (transport dispatch + SMTP statuses), `configs/alpha_agent/stage4_runtime.json` (SMTP selected, OAuth retired), `tests/test_alpha_agent_stage4_runtime.py` (SMTP tests + reconciled guard tests), `PROJECT_STATE.md`.
New (untracked): `scripts/configure_alpha_agent_smtp.ps1`, `scripts/send_alpha_agent_smtp.py`, `scripts/send_alpha_agent_smtp.ps1`, `scripts/diagnose_alpha_agent_smtp.py`, `scripts/diagnose_alpha_agent_smtp.ps1`.

**Carried from the earlier Stage 7.2 / validation passes (8 files):**
Tracked (modified): `alpha_agent/report_renderer.py`, `alpha_agent/evidence_observatory.py`, `api/app.py`, `api/ui/index.html`, `tests/test_alpha_agent_stage5_experiment_factory.py`, `tests/test_alpha_agent_stage6_historical_backfill.py`, `tests/test_api.py`.
New (untracked): `scripts/diagnose_alpha_agent_gmail.py`, `scripts/diagnose_alpha_agent_gmail.ps1` (read-only OAuth diagnostic, retained for reference).

Explicitly EXCLUDE: `.claude/settings.json`, `.playwright-mcp/`, `paper_trader_8001.stdout.log`, `paper_trader_8001.stderr.log`, generated runtime reports / recovery evidence on `D:`, credentials / OAuth / App-Password files, logs, screenshots.

## Safety state

- Scheduled tasks `AlphaAgent-Collect`, `AlphaAgent-Morning-Report`, `AlphaAgent-PostClose-Report`, `AlphaAgent-Watchdog`: **all Disabled**. No fifth task. No scheduled research or trading automation is enabled.
- No `alert()` / `confirm()` / `prompt()` introduced; no Create Orders control; no automation control.
- Forward risk shadows are SHADOW-ONLY: they never change the active paper portfolio.

## Checkpoint history

- `7e550f8` — Stage 1–3.5 ingestion + research director + news/RSS.
- `7286aaa` — Stage 4 persistent runtime + Gmail OAuth reports.
- `4bb41cf` — Stage 5 experiment + evidence engine.
- `3726ac7` — Stage 6 historical backfill + real experiments.
- `4c158e9` — Stage 7 alpha recovery + Stage 7.1 executive reporting (current HEAD).
- (uncommitted, COMMIT_OK) — **Stage 7.2** executive-brief quality + reporting-contract repair, **final live validation** (observatory wiring gaps fixed; every critical field non-null and mojibake-free live), then the **Gmail SMTP transport cutover**: OAuth retired as the active transport (refresh token rejected server-side `invalid_grant`), Gmail SMTP (`smtp.gmail.com:587` STARTTLS, App Password via Windows DPAPI) is now the primary/only active transport — secure configure/send/diagnostic scripts, runtime dispatch, RFC Message-ID identity, a stdin-BOM fix found during acceptance, and ~25 new deterministic tests. SMTP acceptance PASSED live: three `SMTP_AUTHENTICATION_OK` diagnostics and exactly one `EMAIL_SENT` v2 email over `gmail_smtp` (Message-ID `<178545049155.18856.4662823154049402559@gmail.com>`). Code + tests + scans green; operational ledger byte-identical; four tasks Disabled.
