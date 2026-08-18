# PROJECT_STATE

- **Last updated:** 2026-08-18
- **Updated by phase:** **Release 29.4 — normal-cycle session authority + close validity repair.**
- **Source Git HEAD:** `9ee3028`, branch `stage19-controlled-rebalance`.
- **Working tree status:** DIRTY — Release 29.4 changes uncommitted (footprint below). Nothing committed, pushed or enabled by this phase; the commit script is prepared for the user.
- **Current decision:** **DO_NOT_COMMIT (standing instruction) — RELEASE29_4_SESSION_AUTHORITY_FIXED.** On 2026-08-18 at 08:31 ET, with the market session still open, the operator screen offered `RUN_DAILY_CLOSE` for the 2026-08-17 session that had already been closed the previous evening. Root cause: `api.workflow_state` kept a private literal copy of the Daily Close owner's completed-close vocabulary; Release 29.3 renamed `REBALANCE_PROPOSAL_READY` to `DAILY_CLOSE_COMPLETE_MEMBERSHIP_DRIFT` and the copy kept the old spelling, so a real completed close read as invalid and the eligible session read as unclosed. Close validity now belongs to `api.daily_close` and takes no portfolio input at all. Production was READ ONLY throughout: no close, no DRC, no proposal, no order, no restart.
- **Next required action:** the user runs `D:\Temp\paper_trader_release29_4_session_authority_handoff\validate.ps1` and `ui_acceptance.ps1`, reviews the screenshots in `evidence\screenshots\`, then decides on `commit.ps1` / `push.ps1`. Claude has NOT committed or pushed.

## Release 29.4 — normal-cycle session authority + close validity (2026-08-18)

**The rule this phase enforces.** *A duplicated vocabulary is a vocabulary that will
drift.* Release 29.3 renamed a close status and migrated it on read — correctly, and
with tests. What it could not see was that two other modules each held a private
literal copy of the same vocabulary, kept "so the module stays importable without
`api.daily_close`". Neither copy was updated, and the very next day the product told
the operator to close a session it had already closed.

**The live contradiction (2026-08-18 08:31 ET).**

| Owner | Said |
|---|---|
| `engine.market_session` | `BEFORE_SESSION_CLOSE`, eligible completed session `2026-08-17` |
| `api.daily_close` | `AWAITING_MARKET_CLOSE`, `requires_close_run = false`, recorded close `2026-08-17` = `DAILY_CLOSE_COMPLETE_MEMBERSHIP_DRIFT`, forward evidence 6/6 |
| `api.workflow_state` | `READY_FOR_DAILY_CLOSE`, `daily_close_gate.execution_allowed = true`, `operational_close_valid = false`, "No completed operational close has been recorded yet." |

Both domain owners were right. The composition owner disagreed with both of them.

**Ownership after this release.**

* `engine.market_session` — WHICH completed session is eligible. The workflow owner runs
  no calendar arithmetic of its own (audited: no `walk_back_to_trading_day`,
  `previous_trading_day`, `resolve_expected_session`, `expected_from_reference_date`).
* `api.daily_close` — whether that session was operationally PROCESSED.
  `completed_close_statuses()` / `is_completed_close_status()` /
  `is_operational_close_complete(progress)` are the one definition.
  `CLOSE_VALIDITY_POLICY = "OPERATIONAL_COMPLETION_ONLY"`.
* `api.workflow_state` — composes those two answers and re-decides neither.
* Portfolio / research — membership drift, HOC, reassessment, proposal, decision. None
  of them can reopen, invalidate or re-run a recorded operational close.

**Close validity is operational completion.** `is_operational_close_complete` accepts
one argument — the close's own progress document. There is no parameter through which a
portfolio verdict could arrive, and the audit asserts that on the SIGNATURE rather than
on a comment. `DAILY_CLOSE_COMPLETE_HOLD` and `DAILY_CLOSE_COMPLETE_MEMBERSHIP_DRIFT`
are equally complete; the pre-29.3 byte still on disk normalises on read and is complete
too. History keeps its bytes.

**Three fail-closed session invariants** (`check_session_authority`, merged into
`consistency_status`): `DAILY_CLOSE_OFFERED_FOR_ALREADY_PROCESSED_SESSION`,
`COMPLETED_CLOSE_REPORTED_INVALID`, `COMPLETED_CLOSE_HIDDEN_FROM_EVIDENCE`. The first is
scoped by the market-session owner's EXPECTED date, not by a blanket rule: once the
post-close cutoff passes, a new session is expected and the Daily Close is precisely the
mechanism that advances owned marks (Stage 19.3), so offering it is correct then.

**Evidence presentation.** "No completed close has ever been recorded" and "the most
recent attempt did not complete" are different facts. The second is now
`NOT_COMPLETED` and names the date, so a recorded close can never be erased from the
operator's evidence.

**Today is the sole normal-path execution surface.** Release 29.3 collapsed the hero to
one compact line off Today but never collapsed the CTA column, so Portfolio still
rendered a full RUN DAILY CLOSE button. The execute control is now route-scoped to
Today, replaced elsewhere by an "Open Today to act" routing notice, and
`dispatchCanonicalPrimaryAction` refuses a mutation off-Today even if a control is
reached by other means.

**Model target snapshot lane.** `OPERATIONAL TARGET REVIEW / READY TO CONFIRM` sat
beside a withheld portfolio decision and read as an approval waiting on the operator. It
is an independent lane — the model's validated ranked 25-name snapshot — so it is
retitled `MODEL TARGET SNAPSHOT REVIEW`, states its scope explicitly, and its ready
state names the object (`READY TO CONFIRM SNAPSHOT`). It is not, and by audit cannot
become, an input to `canonical_portfolio_decision`.

## Release 29.3 — portfolio decision integrity + policy semantics (2026-08-17)

**The rule this phase enforces.** *One authoritative interpretation per business
concept — including its VOCABULARY.* Phase 29G.1 reclassified the legacy
rank-membership comparison's presentation but left its tokens saying `PROPOSAL_READY`.
Presentation is what a human reads; tokens are what every downstream consumer reads,
and Release 30 (Telegram) reads tokens.

**What changed, by owner.**

* `api.daily_action_gate` — `MEMBERSHIP_DRIFT_DETECTED` / `MEMBERSHIP_DRIFT`; the
  headline no longer claims changes are proposed; a membership difference is no longer
  `action_required`.
* `api.daily_close` — records `DAILY_CLOSE_COMPLETE_MEMBERSHIP_DRIFT`; the legacy token
  is migrated on READ (`normalize_close_status` / `normalize_close_decision`), so the
  immutable Aug-17 journal row keeps its bytes.
* `api.daily_research_cycle` — its assessment block is explicitly scoped
  `LEGACY_RANK_MEMBERSHIP_COMPARISON`, `is_portfolio_proposal: false`.
* `engine.portfolio_reassessment` — owns the ASK gate only. Turnover budget,
  concentration, sector concentration and post-change risk are DEFERRED to the
  complete-target owner and published as explicitly non-binding context. The mandatory
  eligibility-exit policy is declared once, versioned, and its documented intent is now
  what the code does.
* `engine.reallocation_proposal` — decides the four moved constraints on the COMPLETE
  target, exactly once, and yields the new fail-closed `WITHHELD` state.
* `api.portfolio_decision` — new `CHANGE_CANDIDATE_WITHHELD`; a withheld target can
  never be approved (`record_decision` refuses it).
* `api.workflow_state` — `canonical_portfolio_decision` (ONE decision object for
  Release 30) and `check_decision_semantics` (six semantic invariants).

**The evidence that forced the constraint move.** On 2026-08-17 the release set freed
~49.6% of the book (`retained_invested_weight = 0.504258`). Renormalising the retained
stub to 1.0 scaled every surviving weight by ~1.98x, so `max_name_weight` "rose"
0.044184 (DVN) → 0.081571 (FANG) without a dollar moving into FANG, and the sector cap
fired comparing `Unknown` (0.325195) against `Information Technology` (0.374216). All
four recorded blockers were renormalisation artifacts of a portfolio nobody will hold.

**UI.** Today keeps the full operator hero and gains one balanced full-width portfolio
status row (money lane left; HOC counts + the canonical verdict right — net improvement
vs hurdle, turnover vs budget, strongest signal, portfolio action). Portfolio shows a
compact one-line workflow notice; Markets, System · Audit and non-research Research show
none. Holding attention and the portfolio decision are rendered as different questions.

**Guards.** `tests/test_release29_3_decision_integrity.py` (59 tests) and
`scripts/audit_architecture.py` → `check_release29_3_decision_integrity`
(23 strict-blocking invariants, AST/symbol contracts). Proven to block: renaming one
moved constraint code on one side alone fails the audit with exit 1.

**Safety, unchanged.** Paper-only, preview-first, manual review mandatory, automation
off, no broker execution, no Create Orders, no fabricated expected return. Release 30
(Telegram / notifications) is deliberately NOT implemented.

## Release 29 UX2 — radical operator simplification + permanent restart/smoke invocation fix (2026-08-17)

**The product rule this phase enforces.** *If the operator cannot act on it, and does not
need it to make a portfolio decision, it does not belong on Today or Portfolio.* The
previous pass improved hierarchy and user acceptance still failed, because hierarchy does
not help when the screen carries information nobody can act on. This phase therefore
simplified **by removal** — and every removal is a **move**, never a deletion: each
relocated element keeps its id and stays its canonical loader's write target.

**Navigation** is now three operator questions: **OPERATE** (Today · Portfolio · Markets),
**RESEARCH** (Research), **SYSTEM** (System · Audit). Every legacy route still resolves.

**MARKETS (new, read-only).** MARKET NOW (S&P 500, Nasdaq, Dow, VIX / EUR-USD, Gold, WTI,
Brent / US 10Y, US 2Y, USD broad), MARKET TREND (30-day sparklines) and MARKET REGIME
(equity, volatility, rates, commodities, FX tone), labelled **REFERENCE CONTEXT — NOT A
PORTFOLIO SIGNAL**. It creates no calculation, owns no data and calls no provider: the
same single loaders (`GET /v1/market/indicators`, `GET /v1/market/context`) write the same
elements, relocated out of Today rather than rebuilt.

**TODAY** keeps only: the operator command bar; Active Manager (running/idle/busy, last
cycle, next check, source-health summary, latest material information, portfolio result,
and the current step **only while busy**); the portfolio snapshot (NAV, daily P&L,
cumulative, vs SPY, drawdown, holdings, cash, proposed changes, pending orders); the
opportunity-cost counts (HOLD/REDUCE/EXIT/REPLACE/ADD/DATA GAPS) with a link to Portfolio;
and ONE compact market strip (S&P 500 · VIX · US 10Y · WTI · View Markets) that MIRRORS
the authoritative tiles — no second fetch, no second owner, no market arithmetic. At
1920x1080 Today needs **no scrolling** (`main.scrollHeight == main.clientHeight`).

**Moved to SYSTEM · AUDIT** (one new `sysops-panel`, routed under Diagnostics): Data
Freshness & Market Session (12 input dates + gate), the source-by-source collection table,
the worker counters (PID / restarts / iterations / heartbeat / progress), the material-event
and affected-holding lists, the portfolio-decision line, and the Research Status strip.

**PORTFOLIO** keeps: current portfolio (NAV / today / cumulative / vs SPY / drawdown, then
cash, holdings, pending orders, current target, implementation, operational mark, next
review, book status); the current decision (one human statement, one concise blocked
reason, compact HOC counts, and the decision metrics only when they carry a value);
performance & risk (all six charts, unchanged); and an Action section rendered **only when
the canonical payload names an actual next action**. Removed from the primary route:
mature-evidence statistics, what-worked/what-did-not, churn and policy diagnostics, the 13
check badges and raw check names, rebalance lineage and superseded plans, raw order/fill
history, model-state strings, artifact ids, the duplicate Daily Close, the legacy
membership comparison and the developer paragraphs — all still reachable through System ·
Audit or an intentional drill-down.

**The persistent right diagnostic rail is removed** from Today, Portfolio, Markets and the
two full-screen reviews (route published on `<body data-route>`; rail markup and every id
in it retained, so no canonical writer or selector broke).

**Permanent restart/smoke invocation fix.** Two real production defects were INVOCATION
defects, not code defects: (1) a `String[]` of smoke paths forwarded across a child shell
started with the file switch flattened into bare tokens, and the next URL bound
positionally to the 32-bit timeout parameter; (2) the repair attempt built the lifecycle
command in a child shell and lost its continuation backticks to the outer parser.
`scripts/restart_paper_trader_backend.ps1` now contains **no process-terminating
statement**, is safe to call directly with `&`, prints its **bound parameter contract**
first, rejects a non-rooted smoke path **by name** with the flattening explanation, exposes
`-ContractProbe` (bind-and-report only), and reports through one printed token
(`LIVE_SMOKE_OK` / `RESTART_PREFLIGHT_OK` / `RESTART_SMOKE_FAILED - …`), `$LASTEXITCODE`
and `$global:PaperTraderRestartResult`. Every existing gate is preserved: production
store-root validation, PID tracking, stopping only the intended listener, canonical
`/v1/health` + `/v1/ready` polling, exactly-one-listener ownership, the authenticated live
read, the empty-portfolio contamination assertion, and stdout/stderr startup diagnostics.

**New guards.** `scripts/audit_architecture.py` gains
`check_release29_ux2_simplification` (the move happened, to one place, with every id
intact, no forked market owner, the rail removed but retained) and
`check_restart_invocation_hygiene` (owner is exit-free; nobody forwards the smoke paths
through a child shell's file switch; nobody builds a lifecycle command through a child
shell's command switch; nobody collapses the array; no second restart implementation).
Both are wired into `--strict`. Regressions: `tests/test_release29_ux2_simplification.py`
(34 tests) and `tests/test_release29_restart_contract.py` (31 tests, including a probe that
proves the binding with the REAL PowerShell parameter binder).

**Acceptance.** 530 targeted tests pass (UI/operator/market/workflow/analytics/architecture
contracts) plus 63 restart tests; `check_ui_js.py` = `checked_blocks=9 errors=0`; the strict
architecture audit exits 0; `git diff --check` clean. A real Windows lifecycle acceptance
ran on the **isolated port 8098** by direct canonical invocation with five caller-supplied
smoke paths: bound contract `[System.String[]] 5 element(s)`, production store roots OK,
backend started, health 200, ready 200, exactly one listener owned by the launched tree,
six authenticated GETs, **25 positions served**, `LIVE_SMOKE_OK`, caller survived, isolated
backend cleaned up, **production 8001 untouched** (same pid, still 200) and **production
collection untouched** (worker still running on its own cadence). Eight screenshots at
1920x1080 and 1440x900 were captured and inspected.

**Release 29 UX2 footprint (changed files).** New: `tests/test_release29_ux2_simplification.py`,
`tests/test_release29_restart_contract.py`, `tests/support/restart_contract_probe.ps1`.
Modified: `api/ui/index.html`, `scripts/restart_paper_trader_backend.ps1`,
`scripts/audit_architecture.py`, `tests/test_canonical_backend_restart.py`,
`tests/test_release29_ui_consolidation.py`, `CLAUDE.md`, `PROJECT_STATE.md`.
Handoff (outside the repository): `D:\Temp\paper_trader_release29_ux2_handoff`.

---

## Alpha Agent Stage 8 (earlier phase — retained below for history)

- **Prior Source Git HEAD:** `a0f3d9c` (`Repair Alpha Agent reporting and cut email to Gmail SMTP`); origin/main synchronized.
- **Prior working tree status:** DIRTY — Stage 8 additions uncommitted (footprint below). Nothing committed, pushed, or enabled by this phase.
- **Prior decision (Stage 8):** **DO_NOT_COMMIT (standing instruction) — Stage 8 REAL-PRODUCTION acceptance PROVEN.** Telegram credentials are now configured (DPAPI; bot **@PaperTrader05_bot**, allowed id `8284912423`); `getMe` returns `TELEGRAM_AUTH_OK`. The durable queue was proven to drive REAL production work (not placeholders): live-run evidence below. The operational ledger is byte-identical before and after all real acquisition + experiments; the four cadence tasks stay Disabled. The ONLY soft-pending item is the LIVE Telegram message capture: the bot currently has 0 pending updates, so the user must send the 3 messages TO **@PaperTrader05_bot** for a live poll to process them (the identical enqueue→real-experiment-handler path is already proven end-to-end).
- **Prior next action (Stage 8, still open):** (1) in Telegram, open **@PaperTrader05_bot** and send `/status`, `What data sources are currently available?`, and `Run a residual momentum experiment excluding financials`; (2) reply "sent" and a bounded live poll processes them (read-only replies + a bounded research enqueue); (3) the user runs the full regression and decides on committing the Stage 8 footprint. Claude has NOT committed, pushed or enabled any task.

## Stage 8 — autonomous data exhaustion + never-idle research + Telegram control

Permanent operating-model change (both principles now govern the agent):

- **Exhaustive data:** never declare a signal family blocked before exhausting every owned + every legally-accessible free source; classify every missing field with an exact source attempt and reason (eight classifications: `ACCESSIBLE_NOW`, `ACCESSIBLE_AFTER_REPAIR`, `PROSPECTIVE_ONLY`, `PAID_NOT_OWNED`, `LEGALLY_RESTRICTED`, `INVALID_CREDENTIAL`, `PROVIDER_OUTAGE`, `NOT_RELEVANT`). An unqualified `NO_ALPHA` conclusion is rejected in favour of five graded levels.
- **Never idle:** never voluntarily enter a "research complete / nothing to do / waiting for user" state while useful work remains; keep ≥1 next useful action in the durable queue; one blocked lane never stops the others; a sent report is never terminal. (This is a scheduling policy, not a busy-loop: cycles are bounded and may sleep between ticks.)

**Actual entitlement findings (live read-only `audit()` probe, 2026-07-31).** Owned + free sources are genuinely NOT blocked:

- **Norgate Data:** HEALTHY (NDU running, package v1.0.74) → adjusted/unadjusted prices, dividends/splits/capital events, delisted securities, index membership, security identity, classifications all `ACCESSIBLE_NOW`.
- **EODHD:** HEALTHY, **all seven probe families `ENTITLED`** (eod, dividends, splits, fundamentals, earnings, insider, news) → `ACCESSIBLE_NOW`.
- **SEC EDGAR:** HEALTHY (ticker/CIK map + daily filing index) → `ACCESSIBLE_NOW`; extended SEC lanes (companyfacts, companyconcept, submissions, Form 4, 8-K/EX-99, bulk) catalogued as `ACCESSIBLE_AFTER_REPAIR` (collector lane to be extended — free, no new money).
- **FRED/ALFRED:** HEALTHY → `ACCESSIBLE_NOW`. **FINRA:** HEALTHY → `ACCESSIBLE_NOW`. **GDELT:** deferred by config (`NOT_RUN`).
- **Registry tally:** 18 `ACCESSIBLE_NOW` · 10 `ACCESSIBLE_AFTER_REPAIR` (SEC-extended + BLS/BEA/Treasury/Nasdaq lanes) · 3 `PROSPECTIVE_ONLY` (analyst estimate revisions / price targets / estimate counts — no owned/free history) · **0 `PAID_NOT_OWNED` · 0 `INVALID_CREDENTIAL` · 0 outage.** Snapshot: `D:\Stock_Prediction_app_data\alpha_agent\stage8\source_registry.json`.

**Durable never-idle research queue** (`alpha_agent/autonomous_research.py`): a crash-safe, resumable, idempotent stdlib-`sqlite3` store (WAL + busy-timeout) under the existing research-state root `D:\...\alpha_agent\stage8\autonomy.sqlite` — **no PostgreSQL**, no operational-ledger write. 12 job categories (SOURCE_DISCOVERY, ENTITLEMENT_PROBE, DATA_ACQUISITION, COVERAGE_REPAIR, DATA_VALIDATION, HYPOTHESIS_GENERATION, EXPERIMENT, ROBUSTNESS_TEST, SIGNAL_COMBINATION, PROSPECTIVE_SNAPSHOT, REPORT, TELEGRAM_REQUEST) × 7 states (QUEUED, RUNNING, RETRYABLE, BLOCKED_SPECIFIC, COMPLETED, REJECTED, FAILED_PERMANENT). At-most-one live job per identity (idempotent enqueue); `BLOCKED_SPECIFIC` jobs are skipped but never block unrelated lanes; bounded retry → FAILED_PERMANENT; `ensure_never_idle`/`replenish` keep the queue non-empty from the source registry + feature families; a sent REPORT does not terminate research. Live smoke: durable persistence across reopen, `never_idle=True`.

**Watchdog** (`watchdog_scan`): detects stale RUNNING jobs / empty queue / stalled lanes, safely requeues stale work (bounded), replenishes, and classifies a genuine GLOBAL hard blocker (nothing outstanding AND nothing replenishable) — otherwise it keeps all unaffected lanes running. No operational mutation.

**Source exhaustion + PIT** (`alpha_agent/source_exhaustion.py`): the machine-readable source registry (per-endpoint metadata: entitlement/auth/legal/rate-limits/history/PIT-suitability/coverage/acquisition-status/next-action/failure-class/retry-policy), read-only probes reusing the Stage-2 collectors' `audit()` path, a coverage matrix (by field/symbol/date), coverage-repair job specs, point-in-time guards (after-close filings effective no earlier than the next valid session; prospective first-snapshot date is a hard PIT floor — never backfill before it), and the honest data-completeness contract.

**Telegram control plane** (`alpha_agent/telegram_control.py`, `scripts/*_alpha_agent_telegram.ps1`): long polling (no public webhook); DPAPI bot token (never in source/.env/argv/env/logs/tests/this file) passed to Python over redirected stdin only; exactly one allowed numeric user id + one allowed private chat id (stored non-secret OUTSIDE the repo), all others denied + audited; durable offset + per-update dedupe (idempotent); chunked, secret-redacted plain-text replies. The router exposes exactly two effect classes — a **read-only** evidence query or a **bounded research-job enqueue** — and has NO code path to a shell, Python, SQL, file delete, order/fill/trade decision, model promotion or holdings/cash mutation. Commands: `/help /status /data /coverage /queue /experiments /blocked /book /performance /report /sources /health /run <request>` + deterministic natural-language routing; injection-shaped input is routed to help. **Credential status: CONFIGURED** (DPAPI; bot @PaperTrader05_bot, allowed id 8284912423; `getMe` = TELEGRAM_AUTH_OK). The read-only providers (`/status /sources /coverage /data /experiments /health`) are wired to the live registry snapshot + durable queue and never mutate anything.

**REAL production handlers (WS2-WS9) — the durable queue drives genuine work.** `runtime.build_production_autonomy_handlers(cfg)` wires each durable-queue category to a REAL, bounded entrypoint and returns genuine evidence; `run_autonomy_cycle` selects it when `autonomy.handlers == "production"` (the shipped default), else the offline handlers (tests). NO category returns a placeholder/registry-rebuild completion. Live acceptance run over the ACTUAL durable queue (`stage8/autonomy.sqlite`), one job per category, drained through the real handlers — **8 COMPLETED + 1 correctly BLOCKED_SPECIFIC that did NOT stop the others (never-idle held); 0 handler errors; operational-ledger aggregate `81E9…A46D` byte-IDENTICAL before/after**:
  - **DATA_ACQUISITION → EODHD** (Stage 2 collect): 62 new normalized records, 15 raw objects, source HEALTHY, `ALPHA_AGENT_STAGE2_READY`.
  - **DATA_ACQUISITION → Norgate** (Stage 6 survivorship-free backfill, AAPL/MSFT/NVDA/AMZN/JPM/XOM): **17,472 bars written**, MARKET_BAR 400,000→417,460, unique dates 768→2,910, unlocked `price_momentum_rank`.
  - **DATA_ACQUISITION → FRED** (Stage 2 collect): 4 new records, HEALTHY.
  - **COVERAGE_REPAIR → Norgate** (Stage 6, KO/PG/JNJ/WMT/HD/CVX): 17,472 bars, real `coverage_delta` (before/after).
  - **PROSPECTIVE_SNAPSHOT** (entitled EODHD earnings calendar): 9 events snapshotted, 1 forward-dated, PIT floor `2026-07-31` stamped; analyst estimate revisions / price targets honestly reported NOT entitled (`PROSPECTIVE_ONLY`).
  - **EXPERIMENT** (Stage 5/7 price-factor engine, panel = 300 symbols): 6 real experiments; residual_momentum rank-IC t=0.245 → `REJECT_WEAK_EVIDENCE`, short_term_reversal t=1.76 (beats null control) → `REJECT_WEAK_EVIDENCE`, others `REJECT_INSTABILITY` — real rank-IC + deterministic gates, nothing promoted.
  - **ROBUSTNESS_TEST** (cost grid 5-100 bps): residual_momentum cost_erosion 0.06, cost_flips_sign false, subperiod_consistency 1.0, max_drawdown −0.219.
  - **SIGNAL_COMBINATION**: combo_mom_lowvol rank-IC t=0.53 (beats null control) → `REJECT_INSTABILITY`, ablated vs its components.
  - **TELEGRAM_REQUEST "run a residual momentum experiment excluding financials"** (enqueued via the exact `poll_once` path): ran through the REAL experiment handler → residual_momentum result + honest note that the financials exclusion needs a PIT GICS series (`DATA_HOLD_NO_POINT_IN_TIME`); the market-neutral residual factor is the closest real, leakage-safe answer.

**Telegram id contract (2026-07-31 fix).** Real Telegram user/chat ids exceed the signed 32-bit range (the live id `8284912423` > `2147483647`) and can be negative for groups, so the configuration script previously died with `Cannot convert value "8284912423" to type "System.Int32"`. Ids are now stored and compared everywhere as **normalized decimal strings** — never `[int]`/`[System.Int32]` (overflows), never a JSON number (loses precision above `2**53`): `configure_alpha_agent_telegram.ps1` trims + validates the ids as positive decimals and writes them as JSON strings (`@([string]$UserIdRaw)`); `telegram_control.normalize_telegram_id()` canonicalizes both the stored allowlist and the integer ids Telegram delivers before comparison (int-in-update matches string-in-store); `_as_id_list()` treats a lone scalar as ONE id (guarding the PowerShell single-element-array-unwrap quirk, so a scalar id is never split into digits); the `telegram_audit` `user_id`/`chat_id` columns are `TEXT`. `diagnose`/`run` scripts and `stage8_autonomy.json` were audited and need no change (display-only join / empty allowlist / production ids live in the external non-secret file). Covered by 9 new regression methods (large id accepted end-to-end, `> 2**31` and `> 2**53` no overflow, stored ids are strings, integer-update ↔ string-store match, unauthorized large id rejected, letters/decimals/blank/scalar rejected, no `[int]` cast or token leak in the script, duplicate large-id updates deduped, large-id user can only enqueue bounded research).

**Observability + count fix** (`evidence_observatory.py`, `report_renderer.py`, `api/app.py`, `api/ui/index.html`): a read-only `GET /v1/research/alpha-agent-autonomy` route + an `autonomy` block on the observatory payload + an "Autonomy Status" UI card (queue depth/state, source classification tally, Telegram status; NEVER-IDLE / READ-ONLY CONTROL / NO ORDERS badges). The prior report-count inconsistency (seven "evaluated" vs an outcome breakdown totalling ten) is fixed canonically: an *evaluated* recovery idea is any recorded decision, so `evaluated == accounted` always reconciles across email/API/UI/persisted evidence; a narrower `completed` count is surfaced separately.

**Validation (Claude-run, targeted — the user runs the one full regression):** 69 Stage 8 tests (incl. 9 large-Telegram-id regression methods + 6 production-handler / read-only-provider tests) + 166 Stage 4 runtime tests (incl. 2 Stage 8-compat tests) + targeted API tests (autonomy route skips only when `PAPER_TRADER_TEST_DATABASE_URL` is unset — a pre-existing env gate) all pass; `py_compile` clean on every changed module; PowerShell AST parse clean on all four scripts (pure ASCII — no CP1252 em-dash misdecode); both configs parse and `scan_for_secrets` == []; `git diff --check` clean (LF/CRLF advisories only); secret scan finds 0 real secrets (only intentional clearly-fake test fixtures); mojibake scan clean (the sole `U+FFFD` is a pre-existing intentional mojibake-*rejection* assertion in `test_api.py`); native-dialog scan finds no `alert()/confirm()/prompt()`; **operational-ledger aggregate SHA-256 `81E9094463AE3EF7CCFD1F30A4EB9E91FCB1134E26B7BD8C2F062EC23923A46D` (17 files) unchanged** before and after all Stage 8 work (no operational mutation).

**Windows tasks:** the four cadence tasks (`AlphaAgent-Collect`, `-Morning-Report`, `-PostClose-Report`, `-Watchdog`) are **Disabled**. The fifth control task `AlphaAgent-Telegram` is added by `scripts/install_alpha_agent_tasks.ps1` and **registered Disabled** (long-polling; read-only + bounded research only). (Registering a new task needs elevation, so Claude's non-admin shell could not create it directly; the idempotent installer — run by the user during final validation, possibly elevated — registers it Disabled. Claude did not enable any task.)

**Stage 8 footprint (changed files).** New: `alpha_agent/autonomous_research.py`, `alpha_agent/source_exhaustion.py`, `alpha_agent/telegram_control.py`, `configs/alpha_agent/stage8_autonomy.json`, `scripts/configure_alpha_agent_telegram.ps1`, `scripts/diagnose_alpha_agent_telegram.ps1`, `scripts/run_alpha_agent_telegram.ps1`, `tests/test_alpha_agent_stage8_autonomy.py`. Modified: `alpha_agent/runtime.py` (additive Stage 8 entry points **+ real Stage 2/5/6 production handlers** `build_production_autonomy_handlers` + production selection in `run_autonomy_cycle`), `alpha_agent/evidence_observatory.py` (autonomy snapshot + canonical recovery count), `alpha_agent/report_renderer.py` (count reconciliation), `api/app.py` (autonomy route + observatory block), `api/ui/index.html` (autonomy card), `configs/alpha_agent/stage4_runtime.json` (`stage8_enabled` + config pointer), `scripts/install_alpha_agent_tasks.ps1` (Telegram task), `tests/test_api.py`, `tests/test_alpha_agent_stage4_runtime.py`, `PROJECT_STATE.md`. The new-file `configs/alpha_agent/stage8_autonomy.json` now also carries the `production_handlers` block (stage2/5/6 config pointers + bounded universes) and `autonomy.handlers: "production"`; `alpha_agent/telegram_control.py` also carries the decimal-string Telegram-id contract, the enriched read-only providers and the per-cycle poll summary. **No new files were added by the id-fix or the production-handler work — the footprint is unchanged (18 files).**

**Commit allowlist (Stage 8; use explicit paths, never `git add -A`):** the eight new files + the ten modified files listed above (18 total). EXCLUDE `.claude/`, `.playwright-mcp/`, `paper_trader_8001.*.log`, any credential/DPAPI files, and all generated `D:\...\alpha_agent\stage8\` research state.

**Telegram credential setup (exact user commands).**

```powershell
Set-Location "C:\Users\binis\paper_trader"
# 1) In Telegram, message @BotFather -> /newbot -> copy the bot token.
#    Get your numeric user id from @userinfobot (user id == private chat id).
# 2) Store the token in DPAPI + the allowed ids outside the repo:
powershell -NoProfile -ExecutionPolicy Bypass -File `
  "C:\Users\binis\paper_trader\scripts\configure_alpha_agent_telegram.ps1"
# prints exactly TELEGRAM_CONFIGURED on success (the token is never displayed)
```

Then reply "Telegram configured. Continue." to run the acceptance test. Do NOT enable the `AlphaAgent-Telegram` task until acceptance passes and the allowlist is committed.

---

## Prior state (Stage 7.2 — Gmail SMTP cutover; superseded by Stage 8 above)

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

---

## Stage 8 — autonomous research runtime + exhaustive data-source closure (uncommitted)

Stage 8 adds a durable, never-idle research queue (`autonomous_research.py`), a live Telegram control plane (`telegram_control.py`), a machine-readable source-exhaustion registry (`source_exhaustion.py`), production autonomy handlers in `runtime.py`, and a read-only autonomy-status surface across `api/app.py` / `api/ui/index.html` / `evidence_observatory.py` / `report_renderer.py`. All research writes land under `D:\Stock_Prediction_app_data\alpha_agent`; nothing here can touch an operational ledger, order, fill, signal, trade decision, model promotion, the Alpha Paper Book, PostgreSQL, the prediction service or a Daily Close.

### Live Telegram acceptance (prior pass)
Real long-poll acceptance passed against **@PaperTrader05_bot** (bot id `8755427817`): real updates from the one allowed user/chat `8284912423`, `/status` + source-registry replies, exactly one durable research job created in the canonical queue `D:\Stock_Prediction_app_data\alpha_agent\stage8\autonomy.sqlite`, the real production handler completed it (residual-momentum experiment), the result was delivered back to the chat exactly once, duplicate polls created no duplicate job/reply, and an unauthorized synthetic update was rejected.

### FINAL closure workstreams (this pass)
- **WS1 — EODHD analyst-vintage collector** (`collectors/eodhd_analyst.py`, source `eodhd_analyst`). Persists ONE IMMUTABLE DAILY VINTAGE per security + snapshot timestamp under `…\ingestion\vintages\eodhd_analyst\<date>\<ticker>.json`; availability = capture date; never backfilled before the first snapshot. LIVE: 60 real records across 6 symbols, first-snapshot PIT floor = **2026-07-31**, a same-day second run is idempotent (0 new). Registry now classifies analyst estimate revisions / price targets / estimate counts **PROSPECTIVE_COLLECTION_ACTIVE** (was PROSPECTIVE_ONLY). Refreshed automatically by the daily `AlphaAgent-Collect` task after release.
- **WS2 — SEC Form 4 transaction extraction** (`collectors/sec_edgar.py`). Parses the official ownership XML (embedded in the full submission `.txt`) into transaction-level records (issuer/owner/relationship, code, shares, price, A/D, direct/indirect, post-txn holdings, derivative flag, amendment flag); PIT availability = SEC acceptance time; amendments are distinct append-only records. LIVE: 8 filings parsed → **24 transaction records**. Coverage bounded (1 issuer) → the insider event-study lane returns an honest **DATA_HOLD** with exact counts.
- **WS3 — SEC 8-K Item 2.02** (`collectors/sec_edgar.py`). Detects Item 2.02 in the official filing text (incl. EX-99), extracts inline EPS/revenue/guidance when present (never fabricated); PIT availability = acceptance. LIVE: **3 Item 2.02 records**. Wired to the earnings-surprise / PEAD lane with honest DATA_HOLD on low coverage.
- **WS4 — SEC bulk honesty**: three SEPARATE lanes. `SEC_FULL_INDEX` operational in-cycle; `SEC_COMPANYFACTS_BULK` and `SEC_SUBMISSIONS_BULK` HEAD-probed → real measured sizes **companyfacts.zip = 1,392,349,382 B**, **submissions.zip = 1,554,685,896 B**, both classified `OUT_OF_BAND_BULK_EXCEEDS_CAP` (a precise, evidence-based blocker; the per-CIK APIs already supply the same data PIT-correctly). A resumable ranged download + checkpoint + SHA-256 runs when an archive fits the bounded cap. The full index is never called "bulk facts."
- **WS5 — point-in-time SIC sector** (`pit_sector.py`). Versioned SIC→research-sector map (`sic-research-sector-1.0.0`) + a strict no-look-ahead PIT series built from contemporaneous SEC ASSIGNED-SIC filing headers (available_at = acceptance). The current-Norgate-GICS ex-financials result is relabelled **PROVISIONAL_CLASSIFICATION_LOOKAHEAD**; only the PIT-SIC variant is leakage-safe. LIVE: real ASSIGNED-SIC PIT records (e.g. AAPL 3571 → Technology) drive the three-way (full / current-GICS / PIT-SIC) comparison, with an honest low-coverage caveat.
- **WS6 — BEA secure config**: `scripts/configure_alpha_agent_bea.ps1` DPAPI-encrypts a FREE BEA UserID for the current Windows user OUTSIDE the repo; `collectors/bea.py` resolves + decrypts it at runtime via ctypes/crypt32; `scripts/diagnose_alpha_agent_bea.ps1` performs a read-only probe. UserID never printed / never in source / env / args / logs. **BEA remains the one honest hard blocker: the free UserID is not provisioned (`BEA_CREDENTIAL_SETUP_REQUIRED`).**

### Source registry
27 ACCESSIBLE_NOW (all wired collectors, live-proven), 3 PROSPECTIVE_COLLECTION_ACTIVE (analyst families — real forward-vintage collector), 2 ACCESSIBLE_AFTER_REPAIR (SEC bulk zips — proven out-of-band), 1 ACCESSIBLE_AFTER_REPAIR (BEA — free UserID absent). No unqualified "no data".

### Validation
Targeted tests green (Stage 8 workstreams 24 + new-sources 12 + autonomy suite). `py_compile` OK; PowerShell parse OK (new BEA scripts + Telegram scripts); JSON valid; secret scan finds no real key (only fake tokens inside redaction tests). **Operational ledger UNCHANGED across all live acquisition + experiments: 18 files, aggregate `6A9A7CCB47EBD1BCC356A1A0B635913F6365C7BCDD017B9441BD31EE6636691B` before == after.**

### Safety state (unchanged)
`AlphaAgent-Collect / -Morning-Report / -PostClose-Report / -Watchdog` = **Disabled**; `AlphaAgent-Telegram` = **not installed** (registered Disabled by `scripts/install_alpha_agent_tasks.ps1` only when the user runs it as admin). No `alert()`/`confirm()`/Create Orders/automation.

### Authoritative commit footprint (reconciled against `a0f3d9c` = current HEAD)
`a0f3d9c` IS the Stage 7.1/SMTP commit. EVERY residual working-tree delta is Stage 8 — the shared files `api/app.py`, `api/ui/index.html`, `alpha_agent/evidence_observatory.py`, `alpha_agent/report_renderer.py`, `configs/alpha_agent/stage4_runtime.json`, `scripts/install_alpha_agent_tasks.ps1`, `tests/test_alpha_agent_stage4_runtime.py`, `tests/test_api.py` carry the Stage 8 read-only autonomy-status surface + a count-consistency fix and are **INCLUDED** (correcting the earlier pass, which wrongly proposed excluding them).

INCLUDE (modified): `PROJECT_STATE.md`, `alpha_agent/collectors/__init__.py`, `alpha_agent/collectors/sec_edgar.py`, `alpha_agent/evidence_observatory.py`, `alpha_agent/ingestion.py`, `alpha_agent/report_renderer.py`, `alpha_agent/runtime.py`, `api/app.py`, `api/ui/index.html`, `configs/alpha_agent/stage2_ingestion.json`, `configs/alpha_agent/stage4_runtime.json`, `scripts/install_alpha_agent_tasks.ps1`, `tests/test_alpha_agent_stage4_runtime.py`, `tests/test_api.py`.
INCLUDE (new): `alpha_agent/autonomous_research.py`, `alpha_agent/source_exhaustion.py`, `alpha_agent/telegram_control.py`, `alpha_agent/pit_sector.py`, `alpha_agent/collectors/eodhd_analyst.py`, `alpha_agent/collectors/bea.py`, `alpha_agent/collectors/bls.py`, `alpha_agent/collectors/us_treasury.py`, `configs/alpha_agent/stage8_autonomy.json`, `scripts/configure_alpha_agent_telegram.ps1`, `scripts/diagnose_alpha_agent_telegram.ps1`, `scripts/run_alpha_agent_telegram.ps1`, `scripts/configure_alpha_agent_bea.ps1`, `scripts/diagnose_alpha_agent_bea.ps1`, `tests/test_alpha_agent_stage8_autonomy.py`, `tests/test_alpha_agent_stage8_new_sources.py`, `tests/test_alpha_agent_stage8_workstreams.py`.
EXCLUDE (non-code artifacts only): `.claude/settings.json`, `.playwright-mcp/`, `paper_trader_8001.stdout.log`, `paper_trader_8001.stderr.log`.

### Exact next user action
1. Register a FREE BEA UserID at `bea.gov/API/signup`.
2. Run `powershell -ExecutionPolicy Bypass -File scripts\configure_alpha_agent_bea.ps1` (enter the UserID; it is DPAPI-encrypted outside the repo).
3. Run `powershell -ExecutionPolicy Bypass -File scripts\diagnose_alpha_agent_bea.ps1` (expect `BEA_DIAGNOSTIC_OK`).
4. Reply here — a real bounded BEA acquisition will then be proven and the BEA lane flips to ACCESSIBLE_NOW.

### Release decision
**DO_NOT_COMMIT — BEA_CREDENTIAL_SETUP_REQUIRED.** All Stage 8 code + tests are complete and every other accessible owned/free lane is operational or has a proven genuine blocker; the sole outstanding item is the user's free BEA UserID (WS6). Full regression before committing: `C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe -m pytest -q`.

---

## Stage 8 — BEA acceptance + PRODUCTION-SCALE exhaustive acquisition (this pass; supersedes the DO_NOT_COMMIT decision above)

The user configured the free BEA UserID (`BEA_DIAGNOSTIC_OK`) and required that permanent production never remain a bounded six-symbol research subset — the agent must exhaust the COMPLETE eligible universe through durable, sharded, resumable work.

### WS1 — live BEA acquisition
BEA now resolves its FREE UserID from the DPAPI credential at runtime (no env var). LIVE bounded acquisition of the real NIPA T10101 table wrote **7,925 real macro records** under `D:\Stock_Prediction_app_data\alpha_agent\ingestion`; a second same-day run is idempotent (0 new). The live source probe upgrades **BEA ACCESSIBLE_AFTER_REPAIR → ACCESSIBLE_NOW** (a probe that upgrades a lane now also clears any stale `BLOCKED_*` acquisition status). BEA runs through the real queue handler (a `DATA_ACQUISITION` job whose display provider "BEA" resolves to collector `bea`). The UserID never appears in any path, log, argument or file.

### WS2 — acceptance vs production run modes + dynamic universe (`production_universe.py`)
Two explicit, non-overlapping modes replace the ambiguous bounded universe. **ACCEPTANCE** = the deterministic 6-symbol fixture (tests / smoke / manual only; never a scheduled task). **PRODUCTION** = dynamically resolves the COMPLETE eligible universe from the licensed Norgate survivorship-safe library. LIVE: `S&P 500 Current & Past` resolves to **1,895 symbols (503 current + 1,392 delisted/past, incl. e.g. AABA-201910, AAMRQ-201312)** — decisively not the fixture and with NO permanent 6/300/N cap. Forward-looking lanes (analyst vintages) correctly scope to **current constituents (503)**; price/filing lanes sweep the full survivorship set. Norgate-down degrades to the owned survivorship-free price panel (labelled degraded) and NEVER the fixture. The stage8 config sets `production.run_mode = "production"`, so scheduled tasks select production; tests/smoke pass `run_mode="acceptance"`.

### WS3/WS4/WS5/WS7 — durable sharded FULL-UNIVERSE campaigns (`acquisition_campaign.py` + runtime handlers)
`CampaignStore` (sqlite at `…\stage8\campaigns.sqlite`, off the operational ledger) holds one durable per-symbol cursor per campaign: `full_universe_target_count / completed / pending / repair_backlog / permanent_failed / remaining / acquisition_cursor`, reconciling exactly. A per-job batch size is permitted; a permanent total-universe cap is not. Universe growth APPENDS new PENDING symbols after the cursor and never re-does a COMPLETED symbol. A completed batch **enqueues the next batch** (autonomous continuation); the production planner also keeps one live batch job per incomplete campaign. Batches are PENDING-first then repair; failed symbols retry to a bounded max, then become identified **permanent** failures. Five campaigns: `norgate_prices`, `eodhd_analyst` (current scope), `sec_form4_8k` (CIK), and two SEC bulk archives. LIVE (bounded, real collectors): analyst current-scope batches **25/25 + 25/25 = 50 immutable vintages**, cursor 0→25→50 via auto-continuation, target 503, reconciles; Norgate price batch (50), SEC CIK batch (40 CIKs queried, real records); all reconcile; the durable cursor holds the remaining full-universe work.

### WS6 — production resumable SEC bulk-archive download (`sec_bulk_download.py`)
Not capped at the 32 MB ingestion raw-object limit. Free-space PREFLIGHT against a configurable disk budget (never C:), HTTP Range RESUME from a persistent byte checkpoint, bounded per-call SEGMENT (whole archive never in memory), SHA-256 + ATOMIC completion, archive version/date provenance (a changed version restarts), and a per-member extraction cursor. LIVE against the REAL `companyfacts.zip` = **1,392,349,382 B (1.39 GB)**: preflight OK on D: (803.6 GB free, 8.6 GB budget → NOT a disk blocker), segment #1 → 8 MB, a fresh downloader instance RESUMED segment #2 → 16 MB (restart-safe); the durable checkpoint holds the remainder for automatic continuation. If free space were genuinely insufficient the downloader returns a precise `SEC_BULK_DISK_CAPACITY_REQUIRED` with exact bytes — never a silent skip.

### Source registry (updated)
**BEA is now ACCESSIBLE_NOW.** 28 ACCESSIBLE_NOW, 3 PROSPECTIVE_COLLECTION_ACTIVE (analyst), 2 ACCESSIBLE_AFTER_REPAIR (SEC bulk zips — now actively mirrored on D: by the resumable downloader). No unqualified "no data".

### Validation (this pass)
**131 Stage 8 + production tests green** (autonomy 70 + new-sources 12 + workstreams 24 + **new production suite 25**), plus Stage 4 runtime 166 green. `py_compile` OK (8 modules); PowerShell parse OK (6 scripts); JSON valid (3 configs); `git diff --check` CLEAN; secret scan CLEAN (14 files, no real key; BEA UserID is DPAPI-only, absent from env). **Operational ledger UNCHANGED across all live acquisition (BEA 7,925 records, 50 analyst vintages, Norgate bars, SEC records, 16 MB bulk): 18 files, aggregate `274E2E3A8A4147ED09F1ADD388381F38B33D0D38BCFA90208F7A7422D5F44B7C` before == after.** (The prior pass's baseline differed because operational Daily-Close state legitimately advanced between sessions — not from Stage 8.)

### Safety state (unchanged)
4 cadence tasks **Disabled**; `AlphaAgent-Telegram` **NOT_REGISTERED**. No orders/fills/signals/decisions/model-promotion/Paper-Book/PostgreSQL/prediction/Daily-Close. All research writes under `D:`.

### Authoritative commit footprint (reconciled against `a0f3d9c` = current HEAD)
Adds 4 NEW files this pass — `alpha_agent/production_universe.py`, `alpha_agent/acquisition_campaign.py`, `alpha_agent/sec_bulk_download.py`, `tests/test_alpha_agent_stage8_production.py` — on top of the Stage 8 INCLUDE lists above (modified this pass: `alpha_agent/runtime.py`, `alpha_agent/source_exhaustion.py`, `configs/alpha_agent/stage8_autonomy.json`, `configs/alpha_agent/stage2_ingestion.json`, `tests/test_alpha_agent_stage8_new_sources.py`, `PROJECT_STATE.md`). Total: 14 modified + 21 new. EXCLUDE (non-code only): `.claude/settings.json`, `.playwright-mcp/`, `paper_trader_8001.stdout.log`, `paper_trader_8001.stderr.log`. Never `git add .`.

### Release decision (this pass)
**COMMIT_OK.** BEA works live (real acquisition, idempotent, ACCESSIBLE_NOW, through the queue). The permanent production agent is configured to exhaust the COMPLETE eligible universe (1,895 survivorship-safe symbols) through durable, sharded, resumable campaigns with cursors, repair queues and autonomous batch continuation; scheduled tasks select production mode; the multi-GB SEC bulk archives are actively mirrored on D:. Every accessible owned/free lane is operational; nothing is a fabricated completion. Full regression before committing: `C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe -m pytest -q`. Elevated task install (registers Disabled): `powershell -ExecutionPolicy Bypass -File scripts\install_alpha_agent_tasks.ps1`.
