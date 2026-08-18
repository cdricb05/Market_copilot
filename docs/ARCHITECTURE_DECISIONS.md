# Paper Trader — Architecture Decisions

> Confirmed, provisional, and unresolved decisions with evidence and
> consequences. Governs [TARGET_ARCHITECTURE.md](TARGET_ARCHITECTURE.md) and
> [CONSOLIDATION_ROADMAP.md](CONSOLIDATION_ROADMAP.md); intent in
> [PROJECT_CHARTER.md](PROJECT_CHARTER.md). Each decision has a **status** of
> CONFIRMED, PROVISIONAL, or UNRESOLVED.

## Operating-cycle decisions (explicit)

### D-1 — Signal refresh runs frequently — CONFIRMED
- **Decision:** prices, features, scores, rankings, forecasts and data-quality
  status refresh as often as the available data supports; the daily research
  cycle is the current cadence, intraday is deferred (Milestone 6).
- **Evidence:** `alpha_target.run_refresh`, `multi_horizon_engine.build_current`,
  `daily_operating_run` already refresh on demand; no scheduler is enabled
  (4 AlphaAgent tasks Disabled).
- **Consequence:** the research-cycle orchestration (Slice 3) must be idempotent
  and cheap enough to run frequently.

### D-2 — Portfolio reassessment runs frequently — CONFIRMED
- **Decision:** after every signal refresh, every holding is compared with the
  strongest eligible alternatives (opportunity cost) to decide whether capital
  is better deployed elsewhere.
- **Evidence:** `daily_action_gate` already recomputes target-vs-holdings each
  event; `portfolio_manager` composes the decision surface.
- **Consequence:** motivates the Milestone-2 opportunity-cost engine (Slice 6);
  reassessment must never mutate holdings (manual review).

### D-3 — Model recalibration is controlled and evidence-gated — CONFIRMED
- **Decision:** models recalibrate/replace only on sufficient new evidence,
  forward deterioration, drift, a formal checkpoint, or a proven challenger —
  never automatically.
- **Evidence:** `decide_tournament` yields eligibility only; factories emit
  `RESEARCH_ONLY`/`CHALLENGER_ELIGIBLE`; audit `research_execution_terms = 0`.
- **Consequence:** the unified `model_registry` (Slice 8) keeps promotion manual
  (Principle 7).

## Structural decisions

### D-4 — One authoritative owner per business concept — CONFIRMED
- **Evidence of the problem:** NAV has two authorities (`portfolio_valuation` vs
  `paper_trading_desk.book_nav`); eligible date has ≥8 resolvers; workflow state
  has 3 representations (CURRENT_ARCHITECTURE §10).
- **Consequence:** Slices 1–5 each end when the audit multi-writer set for the
  concept collapses to one.

### D-5 — Converge the two "daily operating" worlds — PROVISIONAL
- **Decision (proposed):** converge World A (`daily_operating_run`, Yahoo→Postgres)
  and World B (`daily_close`, owned-EODHD→JSON) onto one provider policy and one
  market-session service; deprecate the Yahoo ingest once parity is proven.
- **Evidence:** the two share no provider/store/market-date semantics
  (`daily_operating_run.py:4-14`); only `compute_market_date_alignment` reconciles.
- **Open question:** which store is authoritative for prices long-term
  (owned-EODHD is the operational truth; Postgres holds the legacy archive).
- **Consequence:** until resolved, both worlds coexist and can disagree about
  "today"; Slice 1 makes the date single-source first.

### D-6 — `db/session.py` is the model for a store service — CONFIRMED
- **Evidence:** zero ad-hoc DB sessions (audit `direct_db_sessions = 0`) vs ~13
  private ledger stores + ~11 hand-copied atomic writers.
- **Consequence:** the cross-cutting ledger/store service mirrors this boundary.

### D-7 — Keep Phase 28C separation of operational vs research/forward evidence — CONFIRMED
- **Evidence:** a valid operational close can be green while forward evidence is
  amber at a month boundary (`daily_close.py:720-735`); ACTIVE vs SHADOW never
  mixed (`forward_evidence.py:18-31`).
- **Consequence:** research failure never invalidates a valid close (Principle 3).

### D-8 — Point-in-time evidence is never fabricated — CONFIRMED
- **Evidence:** `ALREADY_PROCESSED` closes never backfill TRUE_FORWARD; recovery
  replays frozen artifacts verbatim or records a documented gap.
- **Consequence:** gaps remain visible; no backdating (Principle 4).

### D-9 — Legacy DB order-execution stack is quarantined, not extended — CONFIRMED
- **Decision:** `engine/reconciler`/`risk`/`portfolio`/`strategy` and the
  `/v1/review/create-orders` surface are legacy; no new execution is built on
  them; they move behind an explicit disabled boundary (Slice 11).
- **Evidence:** `engine/reconciler.run_fill_cycle` commits Order/Trade/Position —
  the only order/fill code, conflicting with the preview-only mandate.
- **Consequence:** execution stays deferred to Milestone 7 with explicit
  authorization.

### D-11 — Canonical market session & data freshness (Slice 1) — CONFIRMED (LANDED)
- **Decision:** one pure owner (`engine/market_session.py`) holds all current-session
  calendar/cutoff arithmetic; one read-only owner (`api/data_freshness.py`) holds the
  cross-source freshness classification; both are exposed at one endpoint
  (`GET /v1/operations/data-freshness`) and one UI loader (`loadDataFreshness()`).
- **Policy:** owned-provider-confirmed sessions are the holiday-safe authority; the
  weekday+cutoff calculation is an explicit *expectation* (`WEEKDAY_CUTOFF_NO_HOLIDAYS`)
  that never overrides confirmed owned data. No exchange-holiday calendar dependency
  is installed, so none is used. The two close policies (16:00 World A / 17:30
  World B) are preserved as an explicit `close_cutoff_et` parameter, not hard-coded.
- **Separation of concerns (D-7 upheld):** research/model or slower-cadence staleness
  may block a NEW signal refresh or TRUE_FORWARD capture but never invalidates an
  already-completed operational close; a month boundary is one freshness condition,
  not a workflow mode.
- **Evidence:** delegation parity (`daily_operating_run.latest_completed_market_date`,
  `daily_close._expected_session/_resolve_clock`) is byte-identical to the pre-slice
  implementation across a clock/DST/weekend matrix; audit `eligible_market_date`
  authoritative owner = `engine/market_session.py`, `unexpected_session_resolvers = 0`.
- **Consequence:** no runtime was deleted; `paper_trading_desk._required_mark_date`
  and other resolvers remain documented follow-ups (the desk owner is untouched this
  slice). Historical evidence (`forward_prediction_skill.eligible_calendar`) and the
  research forward-roll are kept as distinct concepts.

### D-11.1 — Active operational book owns every operational date (Phase 29B.1) — CONFIRMED
- **Defect:** the first D-11 build composed `api/data_freshness.py` from
  `current_operating_state.load_current_operating_state`, whose current-operating
  mark is fed by `portfolio_valuation.load_portfolio_valuation` — the **dormant
  legacy/current-alpha research book** (marked `2026-07-20`). That stale mark
  leaked in as the owned-data confirmation, so the freshness surface reported
  `WAITING_FOR_OWNED_DATA` / eligible `2026-07-20` while the ACTIVE operational
  book (*Alpha Paper Book #1*) was already valued, desk-marked and Daily-Close
  complete at `2026-08-04`.
- **Decision:** every OPERATIONAL date concept (eligible market date, owned-data
  confirmation, valuation, desk mark, benchmark, target) is owned by the **active
  operational book** (`operational_book.load_operational_book` — the authoritative
  book-selection policy) and its owned desk marks (`paper_trading_desk`). A
  dormant/legacy/current-alpha research book can never supply operational
  readiness. Distinct RESEARCH dates (champion evaluation mark, latest price/score
  refresh, frozen monthly momentum input, fundamental panel, TRUE_FORWARD
  snapshot) are resolved from their own owners and never collapsed.
- **No-proxy rule:** the frozen monthly momentum input is read DIRECTLY from its
  persisted source (`multi_horizon_engine.load_inputs` → `month_label`); it is
  never proxied from the target, valuation, champion mark or expected session, and
  degrades to `MISSING`/`UNKNOWN` when no persisted source exists.
- **Active-book identity + consistency:** the contract reports the active book
  identity (id/name/status/owner/operational mark) — multiple candidates degrade
  to `INCONSISTENT`, never silently selected — and a read-only cross-surface
  `consistency_status` (`CONSISTENT`/`INCONSISTENT`/`UNKNOWN`) that names every
  violation; no provider or prediction call.
- **Evidence:** the endpoint contract, endpoint safety, UI single-loader and
  no-date-arithmetic guards are unchanged; a regression fixture reproduces the
  observed live state and asserts `SESSION_READY`, eligible = active mark, the
  completed close remains valid, and `weakest_gate` = the exact stale research
  source (the due monthly momentum input) — never an owned-data lag.
- **Consequence (D-7 upheld):** research/monthly staleness still never invalidates
  a completed operational close. No runtime deleted; `current_operating_state` and
  `portfolio_valuation` remain (they own the legacy/research read models) but no
  longer supply the operational freshness contract.

### D-12 — Canonical workflow / operator state (Slice 2) — CONFIRMED (LANDED)
- **Decision:** one read-only owner, `api/workflow_state.py`, holds the **combined
  operator interpretation** (overall workflow state, current task, single primary
  next action + severity, queued follow-ups, portfolio-assessment currency,
  blockers, completed-state summary, cross-surface consistency). It is exposed at
  one endpoint (`GET /v1/operations/workflow-state`) and one UI loader
  (`loadWorkflowState()`), refining the roadmap's original "the gate is the state
  authority" into a dedicated composition owner so no surface re-derives the
  interpretation.
- **Composition, not recomputation:** it composes the Slice-1 `data_freshness`
  contract and the existing DOMAIN-fact owners — Daily Action Gate (assessment
  outcome/target_state + monthly review clock), probe-free `daily_close.load_close_progress`,
  active `operational_book`, `forward_prediction_skill`, `alpha_target.load_readiness`
  — and never re-implements their logic. Specialized modules keep their domain
  facts; the four legacy stage vocabularies (`app.py:_build_workflow_state` +
  `_canonical_daily_stage`, `command_center._derive_stage`,
  `daily_workflow_dashboard`) are documented and retire with the legacy
  Create-Orders surface (Slice 11), not this slice.
- **Deterministic priority policy (documented precedence):** P1 inconsistent state
  → P2 current session not closed (eligible session already processed) → P3 owned
  data not confirmed → P4 research inputs stale/missing → P5 research current but
  assessment missing/due/overdue/stale → P6 research+assessment current but close
  incomplete → then the terminal region, where a material-risk/manual-review gate
  (P7) preempts an evidence-gapped completion (P8) which preempts a plain
  completion (P9). First matching condition wins.
- **Decision currency (Workstream F):** a Daily Action Gate result is called
  current only when its date equals the latest eligible completed session under its
  review policy; a stale/overdue result is classified (`STALE`/`DUE`/`OVERDUE`) and
  its historical "no change on <date>" conclusion is preserved (dated) in
  `completed_summary` — never re-presented as a current "NO ACTION TODAY". No new
  assessment is run.
- **Separation of concerns (D-7 upheld):** research/model staleness or a documented
  forward-evidence gap is ATTENTION-level and never downgrades a valid completed
  operational close to a failure. Automatic model promotion remains `false`.
- **Evidence:** the endpoint is authenticated, GET-only, read-only, provider-free
  and prediction-free; the UI keeps exactly one `loadWorkflowState()` loader and
  performs no workflow-priority/assessment-currency arithmetic (audit
  `workflow_state_ownership`); a live regression fixture reproduces the observed
  state and asserts `overall_state = WAITING_FOR_SESSION_CLOSE`, the research cycle
  and overdue reassessment appear in `queued_actions`, the historical assessment is
  preserved but not labelled "today", the completed close stays valid, the evidence
  gap is ATTENTION, and `consistency_status = CONSISTENT`; audit inventory drift = 0.
- **Consequence:** Slice 2 performs no workflow action. The Persistent Daily
  Research Cycle and portfolio reassessment (Slice 3) are described/routed here but
  not executed; those actions are labelled not-yet-implemented.

### D-12.1 — Canonical DOM ownership: the UI hard cutover (Phase 29C.1) — CONFIRMED (LANDED)
- **Defect:** the Slice-2 backend (D-12) was correct, yet older visible surfaces
  still presented a valid **historical** (dated) assessment as a current conclusion —
  "DAILY ACTION GATE — TODAY / NO ACTION TODAY" on the gate cards and
  "DAILY GATE — NO ACTION TODAY" on the Action/Safety panel — contradicting the
  canonical workflow strip. Root cause: the canonical `renderWorkflowState` banners
  and the legacy `renderDailyActionGate` / `renderDailyClose` / `renderOperationalBook`
  writers targeted overlapping DOM nodes, so the last async loader to finish won.
- **Decision:** `api.workflow_state` owns **every visible primary operator
  interpretation**. Two additive, backend-generated presentation blocks stop the UI
  from turning raw dates/booleans into conclusions: `assessment_presentation` (a DATED
  historical result — "No portfolio change was recommended on <date>." — with its
  canonical currency label/badge/severity; a `today_wording_allowed` flag that is true
  ONLY when the assessment is current) and `evidence_presentation` (the still-open
  CURRENT session — "no result yet" — kept distinct from the LATEST COMPLETED close's
  documented, attention-level forward-evidence gap).
- **Exclusive DOM ownership (not render-last):** one UI owner, `renderWorkflowState`
  (via the single `loadWorkflowState()` loader), writes and STAMPS (`data-wf-owned`)
  the workflow banners, the right Action/Safety panel (current task / next action /
  primary button / factual close chip / assessment-currency chip) and the reframed
  Daily-Action-Gate card TITLE / currency BADGE / HEADLINE / EXPLANATION on every
  primary surface. The shared specialized setters (`_dcSet`/`_dagSet`/`_obSet`)
  hard-refuse canonical nodes, and legacy client-side mirrors
  (`updateTodayReview`/`applyCanonicalToActionPanel`/`updateCockpitReviewSummary`)
  are neutralised. The specialized loaders render only DETAIL (checks, turnover, dates,
  P&L, holdings). Because ownership is by static guard + stamp, the final visible state
  is identical under every async completion order (proven by an in-process DOM harness
  that runs the real renderers in each order).
- **Historic vocabulary retained (D-7 upheld):** the raw Daily Action Gate endpoint
  still returns its `NO_ACTION_TODAY` outcome code; only the *visible primary*
  presentation is canonical. A documented forward-evidence gap on a valid completed
  close remains ATTENTION, never an operational failure or a rerun-close suggestion.
- **Evidence:** the endpoint stays authenticated, GET-only, read-only, provider-free
  and prediction-free; audit `workflow_state_ownership` confirms one
  `loadWorkflowState`, the ownership declaration + guards, zero UI priority/currency
  derivation and zero unauthorized canonical-node writers; inventory drift = 0.
- **Consequence:** no runtime business action occurred; Slice 3 remains next and
  unimplemented.

### D-13 — Canonical Persistent Daily Research Cycle (Slice 3) — CONFIRMED (LANDED)
- **Decision:** one orchestration owner, `api/daily_research_cycle.py`, holds the
  ONE idempotent, resumable Daily Research Cycle — the daily research-and-reassessment
  pass (session → validate consistency → plan → refresh required inputs → validate
  date-alignment → score universe → prepare target → capture immutable evidence →
  portfolio-assessment bridge) with **no hidden operator prerequisite button or
  command**. Exposed read-only at `GET /v1/operations/daily-research-cycle/status`
  (planning only) and token-gated at `POST /v1/operations/daily-research-cycle/run`
  (`RUN_DAILY_RESEARCH_CYCLE`); one UI status loader + one execution function.
- **Orchestration, not consolidation (scope boundary):** it composes the existing
  authoritative owners through explicit adapters and reimplements no business logic
  — `data_freshness` (session/plan), `alpha_target.run_refresh` (daily price/score
  input), `multi_horizon_engine.build_current` (scoring — Slice 4 still owns
  consolidation), `alpha_target.load_readiness` (target), `forward_prediction_skill`
  (evidence), `daily_action_gate` (assessment bridge). It is **not** the Milestone-2
  Holding Opportunity-Cost engine, does not consolidate scoring (Slice 4) or
  portfolio state (Slice 5), and is an explicit compatibility layer.
- **Month boundary made explicit (Principle 4):** there is no proven safe automatic
  monthly-momentum emitter in-repo (`alpha_target.run_refresh` itself refuses at the
  month boundary — `RUN_RESEARCH_MONTHLY_INPUT_EMITTER`). The frozen `mom_6_1`
  monthly input is therefore **never approximated intramonth**; the cycle returns
  `BLOCKED` naming the exact source and the missing implementation. This surfaces the
  previously-hidden prerequisite behind the documented "August evidence gap" instead
  of silently producing a stale/approximated evidence bundle.
- **Idempotency / concurrency / resume (Principle 5):** idempotency key =
  `sha256(eligible date | active book | strategy version | universe | input-contract
  hash)`. A completed run is reused, a safe incomplete run resumes from the first
  incomplete step, a concurrent identical contract returns `RUN_IN_PROGRESS`, a
  conflicting concurrent contract returns `INCONSISTENT`, a stale lock is classified
  and recovered, and a different input contract for the same date is refused as an
  inconsistency (never overwrites the immutable bundle). Locks are bounded and never
  block unrelated reads.
- **Forward evidence (Principle 4, D-8 upheld):** the required snapshot count is
  **derived from the `forward_prediction_skill.SUPPORTED_BOOKS` registry** (never a
  hard-coded 6); one eligible date, one input-contract hash, one bundle id
  (`fca_<date>`), first-write-wins, never backdated, the mandatory active-book
  snapshot never silently omitted, a partial bundle never labelled complete
  (`COMPLETE_WITH_EVIDENCE_GAP`). Daily Close idempotently reuses the SAME immutable
  bundle through the same owner; the cycle **never** runs Daily Close.
- **Separation of concerns (Principle 3, D-7 upheld):** a research failure never
  invalidates a valid operational close; an assessment-bridge failure keeps the
  research outputs valid and requires reassessment; a prior valid close is never
  mutated by a later research run. Persistence is under a research root
  (`PAPER_TRADER_DRC_DIR`), never the operational ledger root except through the
  authoritative evidence owner. No model promotion/recalibration, no proposal, no
  order/signal/decision/fill.
- **Evidence:** both endpoints are authenticated (status GET-only/read-only, run
  POST-only/token-validated); the DRC UI region derives no dates/priority/freshness/
  plan; `api.workflow_state` consumes the cycle status (new `RESEARCH_CYCLE_RUNNING`
  / `RESEARCH_CYCLE_BLOCKED` states, the research action now executable). The static
  guard `check_daily_research_cycle_ownership` confirms the sole orchestration owner,
  full delegation, no forbidden execution calls, one UI loader + one execution
  function, and zero UI planning; audit inventory drift = 0. Deterministic tests
  inject every read model and every provider/write seam, so no provider/prediction/
  real cycle/Daily Close/ledger write occurs.
- **Consequence:** Milestone 1's reliable persistent daily research cycle now has one
  orchestration path. Slice 4 (canonical scoring), Slice 5 (portfolio state) and
  Milestone 2 (opportunity-cost engine) are described here but not begun; cadence
  remains disabled.

### D-13.1 — Slice 3 live-acceptance completion (Phase 29D.1) — CONFIRMED (LANDED)
- **Defect (first live acceptance).** At ~17:51 ET on 2026-08-05 — a weekday after the
  17:30 ET post-close cutoff, with owned market and benchmark data only through
  2026-08-04 — the released UI showed `RESEARCH_CYCLE_BLOCKED` with the Daily Research
  Cycle button disabled, `target_calculation — NO_REFRESH_OWNER` and
  `momentum_monthly — RUN_RESEARCH_MONTHLY_INPUT_EMITTER`. Three root causes:
  (1) **session mis-classification** — `engine/market_session.evaluate_session`
  inferred a holiday from a heuristic (`benchmark == confirmed` and
  `previous_trading_day(expected) == confirmed`), returning a READY
  `CALENDAR_POLICY_DEGRADED` with `eligible = D-1`. But the desk marks and the SPY
  benchmark come from the SAME owned EODHD provider and lag together on a normal
  post-cutoff publish delay, so the "two independent series" were not independent —
  a false-holiday inference. (2) **workflow priority** — `api/workflow_state`
  computed `owned_data_lag` only from the `WAITING_FOR_OWNED_DATA` / `NO_CONFIRMED_DATA`
  session statuses, so the false-ready session skipped P3 and the (false) research
  block won P3.6. (3) **target NO_REFRESH_OWNER** — `target_calculation` is a required
  reassessment input in `data_freshness` but was absent from the DRC refresh-owner
  registry, so `build_execution_plan` emitted `NO_REFRESH_OWNER`.
- **Decision — post-close session policy.** A weekday is classified `NON_SESSION`
  (new status) ONLY through an AUTHORITATIVE source: an installed exchange calendar
  (`authoritative_non_sessions`) or a persisted provider-confirmed non-session
  contract (`provider_confirmed_non_sessions`). The ABSENCE of same-day owned data is
  NEVER a holiday. With no authoritative calendar available, the expected weekday
  stays UNRESOLVED as `WAITING_FOR_OWNED_DATA` with `calendar_policy_degraded = True`;
  the latest valid PRIOR operational close is unchanged. The eligible session is
  confirmed (`SESSION_READY`) only when BOTH the owned market marks AND the benchmark
  reach the expected date. `latest_benchmark_date` is retained for compatibility but
  no longer drives any holiday inference. This supersedes the D-11 `likely_holiday`
  heuristic (removed).
- **Decision — precedence.** Unresolved current-session owned data ALWAYS outranks a
  research-cycle blocker: `WAITING_FOR_OWNED_DATA` (P3) precedes `RESEARCH_CYCLE_RUNNING`
  / `RESEARCH_CYCLE_BLOCKED` (P3.5/P3.6) in `workflow_state._decide_overall`, and the
  DRC's own pre-run gate returns `WAITING_FOR_OWNED_DATA` before planning any prior
  session's inputs.
- **Decision — canonical refresh owners.** Every required refreshable research input
  has ONE declared owner: `price_score_refresh → alpha_target.run_refresh`;
  `momentum_monthly → api/monthly_momentum_input` (a new pure-stdlib adapter that
  wraps an injectable emitter seam and owns the safe contract — due-ness, schema /
  period / provenance validation, idempotency, atomic persist, reuse-or-reject; it
  never approximates the frozen `mom_6_1` intramonth and never backdates);
  `target_calculation → alpha_target.load_readiness`, marked prepared-downstream
  (produced by `STEP_PREPARE_TARGET`, not a pre-scoring refresh step). No required
  input returns `NO_REFRESH_OWNER`. There is still no safe automatic monthly emitter
  bundled in the pure-stdlib repo, so a due month blocks HONESTLY through the adapter
  (owned by `api.monthly_momentum_input`, never a separate operator prerequisite) —
  the honest "August evidence gap" preserved (Principle 4, D-8 upheld).
- **Scope (unchanged).** Slice 3 remains an orchestration owner: it does not
  consolidate scoring (Slice 4) or portfolio state (Slice 5), is not the Milestone-2
  opportunity-cost engine, never runs the operational Daily Close, never auto-confirms
  a target (the operational target is never silently replaced), never promotes /
  recalibrates a model, and creates no order / signal / decision / fill. The DRC panel
  badge is corrected from the inaccurate `CREATES SIGNALS ONLY` (it creates no legacy
  Signal rows) to `CREATES RESEARCH EVIDENCE ONLY`.
- **Evidence.** Static guard `check_slice3_live_acceptance_ownership` confirms the
  non-session-requires-authoritative-source policy, the declared monthly-adapter and
  target owners, the monthly adapter's zero execution / provider / prediction calls,
  and `WAITING_FOR_OWNED_DATA` outranking research blockers; audit inventory drift = 0.
  Deterministic tests inject every read model and every provider / write seam (the
  monthly adapter writes only under a tmp inputs dir), so no provider / prediction /
  real cycle / Daily Close / operational-ledger / real research-artifact mutation
  occurs.
- **Consequence.** Slice 3 live acceptance is complete: the canonical Daily Research
  Cycle safely executes through one manual UI action once the expected market session
  is confirmed by owned data. Slice 5 (portfolio state) remains not started; cadence
  remains disabled.

### D-13.2 — Production monthly-momentum emitter bridge (Phase 29D.2) — CONFIRMED (LANDED)
- **Defect.** The first real 2026-08-05 Daily Research Cycle and Daily Close succeeded
  only after a MANUAL external monthly-input workflow (running the owned Phase-24 panel
  + Phase-25 emitter outside Paper Trader and restarting the backend): the released
  adapter (`api/monthly_momentum_input.py`) declared the owner but wired no production
  emitter, so a due month returned `momentum_monthly — RUN_RESEARCH_MONTHLY_INPUT_EMITTER`.
  That violated the Slice-3 requirement of ONE Daily Research Cycle action with no hidden
  prerequisite command.
- **Decision.** Wire ONE safe production monthly-momentum PRODUCER behind the adapter's
  existing seam — a NEW pure-stdlib SUBPROCESS bridge `api/monthly_momentum_emitter.py`,
  activated by the `api/app.py` import-time deployment wiring (guarded off under pytest).
  When momentum_monthly is due, the bridge resolves the configured external research repo
  + Python, inspects the owned Phase-24 daily panel's coverage, runs the AUTHORITATIVE
  Phase-25 mathematics in a UNIQUE temporary output dir through an EXPLICIT subprocess
  argument array (never a shell string), validates the produced artifacts (files exist /
  non-empty / schema / unique tickers / produced month == eligible month / produced date
  == eligible / no future data / provenance / source-panel fingerprint / no intramonth
  approximation), and hands them back for the adapter to promote ATOMICALLY (temp sibling
  + atomic replace; an old/new-hash promotion manifest; the canonical scoring cache
  cleared ONLY after a validated promotion; reuse-identical / reject-conflicting). The
  SAME run then continues through input alignment → scoring → target → TRUE_FORWARD
  evidence → assessment. It imports NEITHER numpy NOR pandas (the numeric work happens
  only in the external subprocess), so the Paper Trader process stays pure-stdlib.
- **Ownership (unchanged).** Phase 24 = owned survivorship-free source panel; Phase 25 =
  frozen `mom_6_1` mathematics; `api.monthly_momentum_emitter` = isolated subprocess
  emission + output validation; `api.monthly_momentum_input` = validation + idempotent
  atomic promotion; `api.daily_research_cycle` = combined orchestration;
  `api.universe_scoring` = canonical scoring interpretation. No SECOND monthly formula
  exists in Paper Trader (Principle 1, D-4 upheld).
- **Point-in-time (Principle 4, D-8 upheld).** No future-dated rows; no current-
  constituent substitution into historical dates; no survivorship reconstruction; no
  duplicate ticker/date rows; provenance preserved. Phase 24 supports no safe incremental
  extension, so a behind / future / unverifiable panel is an EXPLICIT DATA_HOLD blocker
  rather than an uncontrolled full rebuild (a recoverable hold maps to an honest BLOCKED,
  not a mixed input set).
- **Scope (unchanged).** No new monthly execution endpoint or UI button (the monthly
  step lives inside the DRC); no Create Orders / order execution / automation; no
  cadence; the operational Daily Close is never run; no model promotion / recalibration;
  no order / signal / decision / fill; no operational-ledger or database write. Slice 5
  (portfolio state) remains next; the Persistent Alpha Research Agent remains a future
  milestone (Slice 8 / Milestone 4).
- **Evidence.** Static guard `check_monthly_emitter_bridge_ownership` confirms the bridge
  is pure-stdlib, uses an argv array (no shell string), delegates Phase-25 math, has no
  second monthly formula, is wired by the adapter + app, exposes the monthly owner in the
  DRC status, and adds no separate monthly endpoint / UI button; audit inventory drift =
  0. The Workstream-K live regression fixture reproduces the 2026-08-05 eligible session
  with a 2026-07 monthly input and drives the whole cycle to `COMPLETE` through injected
  subprocess/provider seams under tmp fixture roots — no live provider / prediction /
  Daily Close / operational or research-artifact mutation occurs.

### D-14 — Canonical universe scoring (Slice 4) — CONFIRMED (LANDED)
- **Decision:** the pure model mathematics stay in ONE kernel
  (`api/multi_horizon_engine.py`), and ONE composition & read owner
  (`api/universe_scoring.py`) holds the authoritative *operational* interpretation of
  the current universe scoring & ranking. The owner is NOT a second scoring engine: it
  calls the kernel exactly once, deep-copies the mtime-cached result before reading it,
  and normalises it into one frozen read contract exposed at
  `GET /v1/research/universe-scoring` (authenticated, GET-only, read-only) and one UI
  loader `loadUniverseScoring()`.
- **Kernel vs owner (scope):** `multi_horizon_engine` owns `compute_scores` /
  `_percentiles` / `compute_combined` / `build_books` (unchanged — no weight is
  re-optimised, no formula changed). `universe_scoring` owns identity, the content-level
  `input_contract_hash`, count reconciliation, universe identity, exclusions and the
  consistency validator. No operational `api/*.py` module duplicates the kernel's
  combined-score mathematics; the Phase-13 `current_alpha_book` book construction is a
  separate frozen champion lineage (documented, to reconcile in a later slice), not a
  combined-score duplicate.
- **Cache safety (Workstream D):** the kernel `build_current` returns a mtime-cached
  dict by reference; the owner deep-copies before any read, so the cache is never
  mutated, repeated calls are equivalent, and a consumer mutation cannot contaminate a
  later canonical result (proven by tests).
- **Content-level input-contract hash (Workstream C):** derived from the owned input
  content fingerprints (sha1 of file bytes) + the frozen strategy / model / construction
  / weights / eligibility contract + the resolved point-in-time dates. NEVER over
  `evaluated_at` / `built_at` / object identity / absolute path alone / file mtime
  alone. Identical content ⇒ identical hash; changed input content or a changed
  model/construction contract ⇒ a different hash.
- **Ranking / exclusions / universe (Workstream E/F):** deterministic score-desc /
  ticker-asc ranking (inherited from the kernel), one rank per eligible name, no
  duplicate tickers, exact TOP25/TOP50 sector-capped equal-weight books (TOP25 ⊄ TOP50
  in general — the per-sector cap is looser at 50 — exposed as
  `top25_subset_of_top50`, never asserted as a false invariant), exclusions recorded
  with reasons and reconciled counts, and an explicit universe identity that is NOT
  labelled strict S&P 500 and keeps unknown membership explicit.
- **Consumers (Workstream G/I):** the Daily Research Cycle scoring adapter delegates to
  the owner (records the canonical input-contract hash; the run-level date-based
  idempotency hash is unchanged); `multi_horizon_platform.load_current_scores` is a
  compatibility wrapper over the owner behind the retained
  `GET /v1/research/current-alpha-scores` (legacy fields preserved); the primary
  model/book identity re-exports from the owner in `alpha_target` and
  `forward_prediction_skill`.
- **Consistency (Workstream J):** a deterministic validator returns
  `CONSISTENT` / `INCONSISTENT` / `UNKNOWN` and names every violation with the concept,
  both owners and both values; it never silently chooses a conflicting value.
- **No promotion (Workstream L, R-3 upheld):** `AUTOMATIC_PROMOTION_ALLOWED = False`;
  the champion (`composite_sn`) is never replaced; no recalibration / parameter search /
  experiment lives in the scoring read path.
- **Evidence:** the canonical endpoint is authenticated, GET-only, read-only,
  provider-free and prediction-free and never runs the Daily Research Cycle / Daily
  Close / reassessment; the static guard `check_universe_scoring_ownership` confirms the
  kernel + owner, delegation, no second scoring engine, no duplicate operational
  scoring, the DRC delegation, the compat wrapper, the GET-only route, ONE UI loader
  with no UI score/rank/exclusion/date computation and disabled promotion; audit
  inventory drift = 0.
- **Consequence:** Slice 5 (portfolio state) and Milestone 2 (opportunity-cost engine)
  are described but not begun; cadence remains disabled.

### D-15 — Canonical operational portfolio state (Slice 5) — CONFIRMED (LANDED)
- **Decision:** ONE read-only composition owner, `api/portfolio_state.py`, holds the
  authoritative complete operational portfolio-state of the ACTIVE Alpha Paper Book —
  the active-book identity + selection, the operational dates, the capital block
  (cash / invested / NAV / cost basis / P&L / cumulative return / benchmark value+date /
  excess vs benchmark / drawdown), the per-holding positions, the order / fill
  summaries, the current target reference, the latest portfolio-assessment reference,
  the forward-evidence reference, the operational safety mode, a deterministic
  consistency verdict and a stable state hash. Exposed at ONE endpoint
  (`GET /v1/operations/portfolio-state`, authenticated, GET-only, read-only) and ONE UI
  loader (`loadPortfolioState()`). This directly resolves **R-2** (the two divergent
  NAV authorities): the LIVE NAV authority is `paper_trading_desk.book_nav` (append-only
  ledger replay, surfaced through `operational_book`); `portfolio_valuation.py` is the
  explicitly-scoped legacy DB archive that is never the active book.
- **Composition, not recomputation (Principle 1 / D-4):** it composes
  `operational_book.load_operational_book` (active-book capital / positions / orders /
  target ref), `data_freshness.load_data_freshness` (dates + active-book selection +
  freshness consistency), `paper_trading_desk.load_performance` (cumulative return /
  benchmark / drawdown) and `.load_fills`, `daily_action_gate.load_daily_action_gate`
  (assessment), and `forward_prediction_skill.load_prediction_skill` (evidence). It
  re-implements NO business calculation; it computes no NAV of its own (it reads
  `book_nav`), and the audit `portfolio_nav_valuation` concept is resolved at the read
  layer (RESOLVED_SLICE5_READ_OWNER: read owner `portfolio_state`, live authority
  `paper_trading_desk.book_nav`, legacy archive `portfolio_valuation`; the remaining
  `engine/portfolio`/`current_alpha_book`/`absolute_return_research` writers are
  research/legacy lineages scoped for Slices 8/11).
- **Active-book selection (Workstream C):** the active Alpha Paper Book #1 is selected
  through the authoritative `operational_book` policy (mirrored by
  `data_freshness.active_book`). The dormant legacy DB book (`legacy_paper_portfolio`,
  dated `2026-07-20`) is NEVER selected merely because it exists or has a convenient
  loader; it is reported only as an explicitly ignored archive. The live acceptance
  fixture proves the canonical state selects Alpha Paper Book #1 / `2026-08-05`
  ($100,327.99 / 25 holdings) and the Portfolio-Manager status bar — which had been
  rendering the dormant legacy book ($9,999.52 / `2026-07-20` / 2 positions from
  `/v1/portfolio-manager/summary`) — is cut over to the canonical active book.
- **Consistency engine (Workstream D):** 12 read-only cross-source checks return
  `CONSISTENT` / `DEGRADED` / `INCONSISTENT` / `UNAVAILABLE` with exact reason codes
  (Decimal-safe ±$0.01 NAV reconciliation); it never silently repairs, and the read
  endpoint stays available in a degraded state.
- **Determinism (Workstream I):** repeated reads of unchanged source state produce the
  same `state_hash`; `generated_at` is explicitly excluded from the hash; per-source
  `source_hashes` are exposed; no read performs any write, provider, prediction, order
  or fill call.
- **UI hard cutover (Workstream F):** one shared `loadPortfolioState()` loader +
  `renderPortfolioState()` renderer own the operational valuation nodes (Command Center
  card, Portfolio header / performance KPIs / active holdings table, right panel,
  Daily-Plan summary, Portfolio-Manager active-book summary), STAMP them
  `data-ps-owned`, and the shared `_obSet` setter + `renderPmStatusbar` hard-refuse
  them so the visible value is independent of async completion order. The UI computes no
  NAV, aggregates no totals, selects no active book, derives no valuation date and
  counts no pending orders. The preliminary reassessment proposal remains visible but is
  labelled `PRELIMINARY PROPOSAL — OPPORTUNITY-COST ENGINE NOT YET IMPLEMENTED`, with no
  confirmation / order-creation path.
- **Separation of concerns (D-7 upheld):** research/model staleness or a documented
  forward-evidence gap never invalidates the current valuation or a completed close.
  The proposal is review-only and unapproved (the Milestone-2 Holding Opportunity-Cost
  engine and Milestone-3 Reallocation Proposal engine do not exist yet).
- **Evidence:** the endpoint is authenticated, GET-only, read-only, provider-free and
  prediction-free; the static guard `check_portfolio_state_ownership` confirms the sole
  owner, full delegation, no second owner, no writer, the dormant-legacy rejection, the
  GET-only route, ONE UI loader + renderer with no UI NAV/total/active-book/valuation
  computation; audit inventory drift = 0. Deterministic tests inject every read model,
  so no provider / prediction / real close / reassessment / ledger write occurs.
- **Consequence:** Milestone 1's single portfolio-state authority now exists. Slice 6
  (Holding Opportunity-Cost engine, Milestone 2), Slice 7 (Reallocation Proposal engine,
  Milestone 3) and Slice 8 (Persistent Alpha Research Agent, Milestone 4) are described
  but not begun; cadence remains disabled.

### D-16 — Canonical Holding Opportunity-Cost engine (Slice 6, Milestone 2) — CONFIRMED (LANDED)
- **Decision:** the Milestone-2 opportunity-cost engine has TWO canonical owners: a
  pure deterministic calculation kernel `engine/holding_opportunity_cost.py` (the SOLE
  holding comparison + decision engine, no I/O) and a composition / validation /
  immutable-artifact / read owner `api/holding_opportunity_cost.py` (the SOLE API
  owner). The kernel consumes ONE immutable point-in-time assessment-input contract and
  produces, per holding: current / previous rank + rank change, signal strength +
  deterioration, trailing returns (5/20/60), realized volatility (20/60), max drawdown
  (60), covariance risk contribution, concentration, liquidity, the strongest eligible
  NON-ALLOCATED replacement candidate, switching cost, gross / risk-adjusted / net
  improvement, and a recommendation from the frozen vocabulary HOLD / REDUCE / EXIT /
  REPLACE / ADD, plus non-held ADD candidates.
- **Source-of-truth reuse:** holdings / weights / NAV / cash / sectors →
  `api.portfolio_state`; rank / score / eligibility / adv_dollar → `api.universe_scoring`;
  owned trailing close + dollar volume → `api.price_panel` (extended with `dollar_vol`
  and `trailing_median_dollar_volume`); construction constants (entry rank / exit buffer
  / sector cap / name cap / liquidity floor) → `api.multi_horizon_engine`; transaction
  cost → `api.paper_trading_desk.COST_RATE_PER_SIDE`. None is forked; they are injected
  through one explicit versioned decision policy (`hoc_decision_policy.v1`).
- **Point-in-time honesty (D-8 upheld):** previous rank has NO pre-existing owner, so it
  is sourced from the previous eligible date's persisted artifact and reported
  UNAVAILABLE (with a reason) when none exists; owned volume absent → liquidity
  UNAVAILABLE (never invented); insufficient aligned returns → risk contribution
  UNAVAILABLE (never forced); no expected-return forecast is claimed
  (`expected_return_delta` is always null / UNAVAILABLE — improvement is a SCORE
  comparison net of a modeled switching-cost hurdle).
- **Decision policy:** reused constants + genuinely-new, declared, justified,
  hash-folded, boundary-tested thresholds (participation rate, liquidity day bands,
  relative single-name risk-contribution trip = `3 / n_covariance_names`, min gross /
  net improvement, cost-bps → score-hurdle factor). Precedence: EXIT (broken) > REDUCE
  (concentration / risk breach) > REPLACE (qualified alternative net of cost) > HOLD.
  Deterministic for identical inputs; `assessment_hash` excludes `generated_at`.
- **One orchestration path (D-13 / Principle 2):** the sole normal execution path is the
  Daily Research Cycle — a new `ASSESS_HOLDING_OPPORTUNITY_COST` step runs after
  canonical universe scoring and before the portfolio-assessment step, persists an
  immutable artifact under a research / decision-evidence root (`PAPER_TRADER_HOC_DIR`;
  atomic, indexed, idempotent identical rerun, conflicting artifact rejected,
  interrupted-write recoverable — never the operational ledger, PostgreSQL, order, fill,
  holding, cash or NAV), and feeds its summary into the Daily Action Gate. There is
  deliberately NO separate manual opportunity-cost execution endpoint; the read endpoint
  `GET /v1/operations/holding-opportunity-cost` is GET-only and never runs the engine.
- **Daily Action Gate compatibility (Workstream K):** the gate delegates to the
  opportunity-cost summary (new `opportunity_cost_*` fields) and the review-only banner
  now reads `HOLDING OPPORTUNITY-COST REVIEW — REALLOCATION ENGINE NOT YET IMPLEMENTED`
  (superseding the Slice-5 `PRELIMINARY PROPOSAL` banner), reflecting that the
  opportunity-cost review exists while the Reallocation Proposal engine (Slice 7) does
  not.
- **Evidence:** the static guard `check_holding_opportunity_cost_ownership` confirms the
  sole calculation + API owners, full delegation, the GET-only route, no separate manual
  execution endpoint, no second recommendation engine, no order / fill / target-weight /
  NAV / universe-score in either owner, kernel purity, ONE UI loader with no
  recommendation / cost computation, the gate delegation, and that Slice 7 / Slice 8
  remain future; audit inventory drift = 0. 84 deterministic tests inject every input,
  so no provider / prediction / real cycle / ledger write occurs.
- **Consequence:** Charter Milestone 2 is delivered, review-only. Slice 7 (Reallocation
  Proposal engine, Milestone 3) and Slice 8 (Persistent Alpha Research Agent, Milestone
  4) remain not begun; automatic model promotion remains prohibited; cadence remains
  disabled.

### D-10 — No big-bang rewrite of the monoliths — CONFIRMED
- **Decision:** `api/app.py` (20.5k) and `api/ui/index.html` (26.7k) are reduced
  incrementally (domain routers, extracted view logic) as owning contexts
  stabilize — never rewritten wholesale.
- **Evidence:** 178 routes + the legacy pipeline are entangled in one file; 648
  implementation-coupled test assertions raise rewrite risk.
- **Consequence:** every slice preserves behavior and migrates contracts
  (Principle 8).

### D-17 — Service vs workflow readiness + Slice 6 operator/UI hard cutover (Phase 29G.1) — CONFIRMED (LANDED)
- **Decision:** SERVICE readiness (can the backend serve authenticated operational
  reads?) and WORKFLOW readiness (can today's daily workflow action execute now?) are
  DISTINCT concepts, owned and displayed separately. `GET /v1/ready` is the SERVICE
  probe: a lightweight DB check that returns 200 (`ready=true`, `readiness_kind="service"`)
  or 503 with an EXACT `reason` — it never masks a real dependency failure and never keys
  off market-session timing. `api.workflow_state` owns WORKFLOW readiness; a valid
  `WAITING_FOR_SESSION_CLOSE` state never makes the service report unready. The header
  shows both indicators separately ("Service:" / "Workflow:"). The obsolete Slice-2
  "Run a portfolio reassessment (Slice 3 — not yet implemented)" placeholder is removed:
  the reassessment is produced by the Daily Research Cycle (its
  `ASSESS_HOLDING_OPPORTUNITY_COST` step) — the SOLE execution path — with no separate
  reassessment execution control. The legacy rank-membership comparison is reclassified
  as a read-only **LEGACY MEMBERSHIP-COMPARISON SUMMARY (compatibility-only)** and never
  labelled "Rebalance Proposal Ready"; the Holding Opportunity-Cost review is the PRIMARY
  portfolio decision card, rendered in every state (NOT_RUN / WAITING / BLOCKED / DEGRADED
  / completed) by the ONE single-flight loader with no fabricated recommendation counts
  before an artifact exists.
- **Evidence:** the header previously showed an ambiguous "Not Ready /v1/ready" while the
  Command Center said "System Ready" (two readiness concepts rendered ambiguously); the UI
  carried a stale "Slice 3 — not yet implemented" reassessment control and a
  "REBALANCE PROPOSAL READY" card for a comparison that is not an approved reallocation.
- **Guard:** `scripts/audit_architecture.py:check_slice6_live_acceptance_ownership`
  proves the obsolete control is gone, the legacy comparison is compatibility-only, the
  two readiness concepts are distinct, `/v1/ready` is service-scoped and never
  session-scoped, the HOC panel renders NOT_RUN and completed states, and no reassessment
  / rebalance / order / target-confirmation route was added.
- **Consequence:** the application is ready for the first post-close Slice 6 Daily
  Research Cycle. No target weight, no Reallocation Proposal engine, no orders, no
  automation; Slice 7 (Reallocation Proposal, Milestone 3) is next; the Persistent Alpha
  Research Agent (Slice 8, Milestone 4) remains planned; cadence remains disabled.

### D-17.1 — Slice 6 residual hard cutover: HOC is the sole primary decision (Phase 29G.2) — CONFIRMED (LANDED)
- **Defect:** Phase 29G.1 reclassified the FIRST compatibility card (the Daily Close
  card, `api.daily_close`) but a SECOND operator-facing renderer — the Daily Action Gate
  card (`cc/dw/pm-dag-card`) on the Command Center, Daily Workflow and Portfolio Manager —
  still presented the legacy rank-membership comparison as a PRIMARY decision: "LATEST
  PORTFOLIO ASSESSMENT", "PROPOSAL READY — MANUAL REVIEW REQUIRED", "PORTFOLIO CHANGES
  PROPOSED — MANUAL REVIEW REQUIRED", a "Review Proposed Changes" button and the 17-name
  Add/Remove membership comparison. Two conflicting interpretations of the same portfolio
  decision therefore remained visible.
- **Decision:** there is exactly ONE primary portfolio-decision concept — the **HOLDING
  OPPORTUNITY-COST REVIEW** (`api.holding_opportunity_cost`). The Daily Action Gate card
  on all three surfaces now presents the canonical HOC state (title / badge / headline /
  explanation owned by `renderWorkflowState` from `workflow_state.assessment_presentation`,
  which is the HOC presentation). Before the first production artifact the canonical
  operator state is `HOLDING_OPPORTUNITY_COST_NOT_RUN`, presented with "NONE YET" and no
  fabricated HOLD/REDUCE/EXIT/REPLACE/ADD counts. The legacy rank-membership comparison is
  demoted to a COLLAPSED, read-only **LEGACY MEMBERSHIP-COMPARISON SUMMARY — COMPATIBILITY
  ONLY** (`<details>` on each surface, "View Legacy Membership Comparison" affordance),
  explicitly not a portfolio proposal and creating no orders. The gate result carries an
  explicit classification (`compatibility_only=true`, `decision_authority=NONE`,
  `execution_available=false`, `canonical_decision_owner=api.holding_opportunity_cost`,
  `legacy_membership_comparison=true`); the raw gate outcome / target-state vocabulary is
  PRESERVED unchanged for historical consumers but never presented as a primary decision.
  A compatibility comparison can never set the canonical operator state to PROPOSAL_READY /
  REBALANCE_PROPOSAL_READY / PORTFOLIO_CHANGES_PROPOSED.
- **Workflow sequence:** wait for the session to close → run the Daily Research Cycle (the
  SOLE HOC execution path) → review the Holding Opportunity-Cost assessment (read-only) →
  run the Daily Close. There is no separate reassessment / proposal-review / rebalance /
  order execution control.
- **First-live operator gates:** two read-only PowerShell scripts (GET-only,
  `Invoke-RestMethod`, no `.NET` HTTP client, no write request) —
  `pre_drc_readiness.ps1` (service ready, session closed, no workflow lock, active book,
  25 holdings, 0 pending orders, HOC NOT_RUN, DRC executable → `READY_TO_RUN_DAILY_RESEARCH_
  CYCLE`) and `post_drc_acceptance.ps1` (DRC complete, opportunity-cost step done, one
  current artifact for the eligible date, artifact active-book / hash / holdings-evaluated
  reconciled, 0 pending orders, workflow READY_FOR_DAILY_CLOSE → `READY_TO_RUN_DAILY_CLOSE`).
- **Guard:** `scripts/audit_architecture.py:check_slice6_residual_cutover_ownership`
  proves no primary "Portfolio Changes Proposed" / "Proposal Ready" presentation and no
  "Review Proposed Changes" / "Review Rebalance Proposal" button remain, the legacy
  comparison is compatibility-only and collapsed on all three surfaces, HOC is the sole
  primary decision card that all three surfaces use, exactly one HOC loader, no JS
  recommendation/cost computation, the DRC is the sole execution path, and no
  reassessment/rebalance/order route exists.
- **Consequence:** no target weight, no order or target authority; Slice 7 (Reallocation
  Proposal, Milestone 3) is next; the Persistent Alpha Research Agent (Slice 8, Milestone
  4) remains planned; cadence remains disabled.

### D-17.2 — DRC terminal-manifest persistence/read-back + safe recovery + pre-close consistency (Phase 29G.3) — CONFIRMED (LANDED)
- **Defect (first real Slice 6 run, 2026-08-06):** the first live Daily Research Cycle
  persisted a COMPLETE run manifest and an immutable Holding Opportunity-Cost artifact, but
  `GET .../daily-research-cycle/status` returned `NOT_STARTED`. Root cause: the status
  reader loaded the run by eligible date but gated "reuse a completed run" on
  `input_contract_hash` equality, and that hash is derived from the current input as-of
  dates — including the FAST daily inputs the cycle ITSELF refreshes to the eligible session
  (`price_score_refresh`, `target_calc`). A status read after the run's refresh therefore
  recomputed a different hash than was persisted, the completed manifest was skipped, and
  the reader fell through to `NOT_STARTED` (a status/downstream split-brain). Separately,
  `portfolio_state` reported `INCONSISTENT` only because the valuation sat one eligible
  session ahead of the latest Daily Close before the August 6 close.
- **Decision (terminal persistence/read-back):** a terminal `COMPLETE` /
  `COMPLETE_WITH_EVIDENCE_GAP` requires manifest-contract validation, an atomic manifest +
  index write, and a READ-BACK that confirms the same durable record BEFORE the terminal
  response is returned. A validation/read-back failure never returns COMPLETE; it downgrades
  to `INCONSISTENT` (`MANIFEST_CONTRACT_INCOMPLETE` / `MANIFEST_PERSISTENCE_UNVERIFIED`) with
  the durable downstream references preserved for recovery.
- **Decision (status reader):** a persisted terminal manifest for the eligible session is
  REFLECTED verbatim (never NOT_STARTED under a benign recomputed-hash drift). When
  downstream terminal artifacts exist for the session without a run manifest, status returns
  `INCONSISTENT` with `TERMINAL_DOWNSTREAM_ARTIFACTS_WITHOUT_DRC_MANIFEST` and a safe
  idempotent recovery action; it never synthesises COMPLETE from downstream artifacts.
- **Decision (safe idempotent recovery):** a `session_contract_hash` — eligible date +
  active book + strategy + universe + the SLOW monthly/fundamental inputs, EXCLUDING the
  fast inputs the cycle refreshes — keys reuse/recovery, so a same-date rerun through the
  normal run endpoint REUSES the immutable outputs (no re-scoring, no duplicate evidence /
  HOC artifact, no order / fill / target confirmation / operational-ledger mutation) while a
  genuinely different slow-input contract for the same date is still refused
  (`DIFFERENT_CONTRACT_SAME_DATE`). Recovery is normal idempotent execution — there is NO
  "mark complete" endpoint and no separate recovery entry. The raw `input_contract_hash`
  (and the concurrency contract) is unchanged.
- **Decision (pre-close consistency):** a valuation exactly one eligible session ahead of
  the latest Daily Close, with the current session SESSION_READY and its close due-but-not-
  run, is classified `PENDING_DAILY_CLOSE` / `EXPECTED_PRE_CLOSE_GAP` (state
  `PORTFOLIO_STATE_READY_WITH_PENDING_CLOSE`), not corruption. Genuine gaps (>1 session,
  future-dated valuation, valuation behind the close, NAV / benchmark / active-book
  mismatch) remain `INCONSISTENT`. After the Daily Close, valuation and latest close align
  and the state returns `CONSISTENT` / `READY`.
- **Decision (workflow / HOC):** a completed cycle plus a current HOC assessment SATISFIES
  the portfolio reassessment (`READY_FOR_DAILY_CLOSE`, no separate reassessment control)
  even when the legacy gate date lags the pending close; a DRC `INCONSISTENT` surfaces an
  explicit recovery blocker (never "the assessment has not run"). The honest HOC `DEGRADED`
  state and its documented gaps remain visible and are never upgraded to pass a gate.
- **Guard:** `scripts/audit_architecture.py:check_drc_manifest_recovery`.
- **Consequence:** no evidence is fabricated and no order / target authority is added.
  Slice 7 (Reallocation Proposal, Milestone 3) remains next; the Persistent Alpha Research
  Agent (Slice 8, Milestone 4) remains planned; cadence remains disabled.

### D-18 — Canonical Reallocation Proposal engine (Slice 7, Milestone 3) — CONFIRMED (LANDED)
- **Decision:** the Milestone-3 reallocation engine has TWO canonical owners: a pure
  deterministic calculation kernel `engine/reallocation_proposal.py` (the SOLE
  allocation-math owner, no I/O) and a composition / validation / immutable-artifact /
  read owner `api/reallocation_proposal.py` (the SOLE API owner). From the canonical
  CURRENT portfolio state and the Slice 6 Holding Opportunity-Cost assessment the kernel
  builds ONE coherent paper-only proposed target portfolio and emits per-ticker actions
  (RETAIN / INCREASE / REDUCE / EXIT / ADD / REPLACE_OUT / REPLACE_IN), turnover,
  transaction + switching cost, before/after portfolio score, concentration and
  volatility, and hard-constraint validation, producing a state from the frozen vocabulary
  READY / DEGRADED / BLOCKED / NO_ACTIVE_BOOK (read layer adds NOT_RUN / UNAVAILABLE).
- **Deterministic allocation policy:** HOLD/REDUCE retained, EXIT zeroed, each REPLACE
  swapped to a specific traceable eligible non-held candidate that clears the net-of-cost
  hurdle (an unmatched REPLACE is retained, NEVER a silent exit-to-cash), ADD candidates
  filling the remaining slots by rank; equal-weight `min(1/N, name_cap)`, residual as cash,
  sector count-cap `int(sector_cap · N)`. It **reuses** (never forks) the
  `api.multi_horizon_engine` construction constants (book size N / name cap / sector cap /
  liquidity floor) and the `api.paper_trading_desk.COST_RATE_PER_SIDE` transaction-cost
  model via one versioned allocation policy (`reallocation_allocation_policy.v1`), and
  reuses the Slice-6 covariance primitive for before/after portfolio volatility.
- **Data-integrity (Principle 4, D-8 upheld):** expected return is NEVER fabricated — no
  validated forecast model exists, so `expected_return_before/after/improvement` are always
  null with an explicit `EXPECTED_RETURN_NOT_CALIBRATED` gap (by-design, not degrading);
  the improvement is a signal-SCORE comparison net of a modelled turnover cost. A DEGRADED
  HOC input does not BLOCK Slice 7; each carried source gap is classified by the analytic
  it affects (allocation / risk / expected-return / informational). Portfolio volatility is
  reported only when an authoritative covariance supports it, else null with an explicit
  gap — no fabricated covariance or diversification effect.
- **One orchestration path (D-13 / Principle 2):** the sole normal execution path is the
  Daily Research Cycle — a new `BUILD_REALLOCATION_PROPOSAL` step runs AFTER
  `ASSESS_HOLDING_OPPORTUNITY_COST` and before the portfolio-assessment step, persists an
  immutable artifact under `PAPER_TRADER_REALLOC_DIR` (atomic, indexed, idempotent identical
  rerun; a DIFFERENT source HOC assessment hash for the same date SUPERSEDES — never
  silently reuses — the stale proposal, keeping every artifact immutable). The read endpoint
  `GET /v1/operations/reallocation-proposal` is GET-only and never runs the engine (NOT_RUN
  before a proposal exists); there is deliberately NO create / apply / confirm-target /
  rebalance / order endpoint.
- **Separation of concerns (Principle 3, D-7 upheld):** `api.workflow_state` exposes the
  proposal state as an INFORMATIONAL review action (a separate operator-state vocabulary
  that never enters `OVERALL_STATES`); the Daily Close remains an independent operational
  action whose gating never depends on a proposal existing, and a proposal-review
  requirement never fabricates an order requirement. The Daily Action Gate delegates to
  `load_proposal_summary` and flips its banner from "REALLOCATION ENGINE NOT YET
  IMPLEMENTED" to a review-only "MANUAL REVIEW REQUIRED", `reallocation_engine_implemented =
  True`, while `decision_authority` stays NONE and `execution_available` stays false.
- **Evidence:** the static guard `check_reallocation_proposal_ownership` confirms the sole
  calculation + API owners, delegation, the GET-only route, no create/apply/confirm-target/
  rebalance/order route, the DRC as the sole execution path, no order / fill / target / NAV /
  holdings mutation in either owner, kernel purity, ONE UI loader with no allocation
  computation, immutable/idempotent artifacts, and that no second/unified model registry
  exists; audit inventory drift = 0. Deterministic tests inject every input, so no provider /
  prediction / real cycle / ledger write occurs.
- **Consequence:** Charter Milestone 3 is delivered, review-only. Slice 8 (Persistent Alpha
  Research Agent, Milestone 4) has since LANDED (D-19); automatic model promotion remains
  prohibited; cadence remains disabled.

### D-19 — Canonical Persistent Alpha Research Agent (Slice 8, Milestone 4) — CONFIRMED (LANDED)
- **Decision:** the Milestone-4 Persistent Alpha Research Agent has TWO canonical owners: a
  pure deterministic evaluation kernel `engine/research_agent.py` (the SOLE research-state
  calculation owner, no I/O) and a composition / persistence / read owner
  `api/research_agent.py` (the SOLE API owner). It continuously evaluates whether the current
  research/model stack remains trustworthy and whether bounded research experiments should be
  run, producing a research state from the frozen vocabulary HEALTHY / WATCH / INVESTIGATE /
  RECALIBRATION_DUE / CHALLENGER_PROMISING / INSUFFICIENT_EVIDENCE / BLOCKED (read layer adds
  NOT_RUN / NO_ACTIVE_BOOK / UNAVAILABLE). It is a monitoring & governance layer: it is NOT a
  second/unified model registry (the roadmap's earlier `model_registry` framing is a deferred,
  separate consolidation) and it never moves champion-promotion authority.
- **Evidence sufficiency (Principle 4, D-8 upheld):** the agent evaluates ONLY point-in-time
  evidence actually available at the evaluation timestamp; it never reconstructs old
  TRUE_FORWARD evidence from current snapshots, backfills missing predictions, or uses future
  outcomes. Forward-observation sufficiency is the DOMINANT gate: a short negative live P&L run
  yields INSUFFICIENT_EVIDENCE / WATCH (never a premature RECALIBRATION_DUE); realized-
  performance weakness on insufficient evidence is a documented symptom, capped, never proof of
  model degradation. Missing evidence remains an explicit gap. Thresholds are one versioned
  policy (`research_agent_policy.v1`); the authoritative decision-gate `MIN_FORWARD_OBS` is
  injected so no governance threshold is silently forked.
- **Reuse, never fork (Principle 1):** every metric is READ from its existing owner —
  champion / challenger identity (`api.universe_scoring` / `api.current_alpha_tournament`),
  rank IC / decile spread (`api.forward_prediction_skill`), realized performance
  (`api.paper_trading_desk` + `api.forward_evidence`), the Slice-6 HOC and Slice-7 reallocation
  immutable histories (enumerated from each `index.json`), and portfolio state
  (`api.portfolio_state`). The kernel defines none of those calculations. Champion health is a
  set of EXPLAINED components with reason codes (never one opaque score); degradation is
  categorized (PERFORMANCE_WEAKNESS / SIGNAL_DEGRADATION / RANKING_DEGRADATION / REGIME_DRIFT /
  SECTOR_INSTABILITY / TURNOVER_INEFFICIENCY / PORTFOLIO_STALENESS / DATA_QUALITY_DEGRADATION /
  INSUFFICIENT_EVIDENCE); HOC + reallocation history distinguish portfolio staleness /
  governance latency from model weakness.
- **No automatic promotion (Principle 7, R-3 / D-3 upheld):** the agent classifies a
  challenger (NOT_EVALUATED / INSUFFICIENT_EVIDENCE / UNDERPERFORMING / COMPETITIVE / PROMISING
  / SUPERIOR_CANDIDATE) and may recommend a CONTROLLED recalibration study when evidence gates
  pass, but SUPERIOR_CANDIDATE != PROMOTED. It promotes / recalibrates / retrains / replaces no
  model, writes no champion pointer, and executes no experiment — a generated bounded-experiment
  SPECIFICATION is the deliverable; every recommended action requires explicit manual approval.
- **One orchestration path (D-13 / Principle 2):** the sole scheduled execution path is the
  Daily Research Cycle — a new `RUN_RESEARCH_AGENT` step runs AFTER `BUILD_REALLOCATION_PROPOSAL`
  and before the portfolio-assessment step, persists an immutable artifact under
  `PAPER_TRADER_RESEARCH_AGENT_DIR` (atomic, indexed, idempotent identical rerun; a DIFFERENT
  evidence hash for the same date SUPERSEDES — never silently reuses — the stale assessment). The
  read endpoint `GET /v1/research/research-agent` is GET-only and never runs the engine (NOT_RUN
  before an assessment exists); there is deliberately NO promote / recalibrate / retrain / apply
  endpoint.
- **Separation of concerns (Principle 3, D-7 upheld):** research findings are informational
  research governance; the research-agent step is non-blocking and the Daily Close stays
  independent (research recommendation != operational action). A research weakness never
  becomes an operational-close failure.
- **Evidence:** the static guard `check_research_agent_ownership` confirms the sole calculation
  + API owners, delegation, the GET-only route, no promote/recalibrate/retrain/apply route, the
  DRC as the sole execution path, no champion-pointer / order / fill / target / NAV / holdings
  mutation in either owner, kernel purity, ONE UI loader with no research computation,
  immutable/idempotent artifacts, no second/unified model registry, and that no paid-data
  registry fork exists (Slice 9 landed as a purchase gate, not a registry); audit inventory
  drift = 0. Deterministic tests inject every input, so no provider / prediction / real cycle /
  ledger / real research-agent artifact write occurs.
- **Consequence:** Charter Milestone 4's monitoring/governance mandate is delivered, research
  governance only. Slice 9 (Data Expansion / Purchase-Gate, Milestone 5) has LANDED (Phase 29J,
  D-9 below); Slice 10 (Intraday) remains next; automatic model promotion / recalibration /
  retraining remains prohibited; cadence remains disabled.

## D-9 — Data Expansion / Purchase-Gate is a decision layer over existing owners, never a new provider or purchasing authority (Phase 29J, Slice 9, Milestone 5)

- **Status:** CONFIRMED (Phase 29J). Charter Milestone 5.
- **Decision:** the canonical framework for deciding whether a new external dataset is worth
  acquiring / integrating is ONE pure gate calculation (`engine/data_expansion_gate.py`,
  `evaluate_dataset`) over one immutable dataset-evaluation input contract, plus ONE
  composition / catalog / persistence / read owner (`api/data_expansion.py`). The gate scores
  sixteen dimensions (PIT integrity, historical depth, inactive/delisted coverage, universe
  breadth, effective sample, revision history, freshness, identifiers, survivorship risk,
  restatement/backfill, licensing, cost, incremental information, measured lift, implementation
  complexity, operational reliability), separates hard blockers from soft gaps, and returns ONE
  explicit recommendation drawn from a frozen vocabulary: `REJECT` / `INSUFFICIENT_EVIDENCE` /
  `RESEARCH_ONLY` / `CANDIDATE` / `PURCHASE_RECOMMENDED` / `INTEGRATION_RECOMMENDED`.
- **Why a gate, not a provider layer:** the existing owners already own the underlying concerns
  — `alpha_agent/source_contracts` (provider/provenance), `api/data_freshness` (freshness),
  `alpha_agent/experiment_contracts` (evidence gates), `alpha_agent/analyst_revisions` (Stage 13A
  analyst-revisions purchase framework) and `engine/research_agent` (Slice 8 DATA opportunities).
  Slice 9 REUSES them and adds only the cross-dataset acquisition decision; it does not fork a
  provider, a normalized-record contract, a freshness owner or a second analyst-revisions
  framework. No paid-data registry (`api/paid_data_registry.py`) is created.
- **No purchasing authority (Principle 3/7, safety boundary):** `PURCHASE_RECOMMENDED` /
  `INTEGRATION_RECOMMENDED` are always MANUAL APPROVAL REQUIRED. The gate purchases no dataset,
  subscribes to / activates no provider, calls no paid provider, uses no paid API quota, alters
  no credentials, integrates no dataset, mutates no portfolio, promotes no model, creates no
  order/fill, and enables no cadence. There is deliberately NO purchase / subscribe / activate /
  integrate / enable-paid-data endpoint. No purchase is ever recommended on in-sample-only
  evidence, and current / live P&L is never used as proof that a paid dataset is necessary.
- **Cadence DISABLED:** a full purchase-gate evaluation is never a daily job (`CADENCE_ENABLED =
  False`) — it runs only on candidate add / metadata change / sufficient new evidence / a review
  checkpoint / an explicit operator request; the Daily Research Cycle may only READ the latest
  status. GET endpoints read only persisted immutable evaluations (`NOT_RUN` before one exists);
  no GET recomputes a research study.

## UI / operator-experience decisions

### D-16 — Task-oriented operator information architecture — CONFIRMED (Phase 29J.1)
- **Decision:** the operator UI is organized around FOUR task areas — **Today / Portfolio /
  Research / System · Audit** — not around which backend modules exist. Today is the default
  landing and answers, in order: what is the market doing → what is my portfolio doing → is
  anything abnormal → what does the system recommend → what do I do next. Legacy/detail views
  (Daily Workflow, Model Target, Holdings detail) are DEMOTED out of primary navigation (kept
  reachable under an Advanced-views disclosure); every legacy route is preserved as an alias so
  no deep link breaks.
- **Evidence of the problem:** six architecture-centric views; NAV rendered in 7+ places, dates
  in 20+, workflow/next-action in 5–6 renderers, ~480 safety-badge spans across ~30+ strips; the
  reallocation review buried in a collapsed Advanced block.
- **Consequence:** one primary presentation per concept; the UI READS the canonical owners
  (`workflow_state`, `portfolio_state`, `holding_opportunity_cost`, `reallocation_proposal`,
  `research_agent`, `data_freshness`) and duplicates no NAV/workflow/HOC/reallocation/research
  computation in JS. Guarded by `check_operator_ux_consolidation_ownership`.

### D-17 — Market context is CONTEXT, from the single owned owner, never an alpha signal — CONFIRMED (Phase 29J.1)
- **Decision:** the restored Market Context strip READS the SINGLE authoritative backend owner
  `GET /v1/market/indicators` (`engine/market_data`). No second market-data owner is created, no
  new provider is added, and the browser never calls a provider directly. Market context is
  reference only — current-only with per-tile as-of labels — and is NEVER presented as an alpha
  signal or a BUY/SELL. Series with no owned/available source (DXY has no owned source; US rates
  are unavailable without a FRED key) render an explicit UNAVAILABLE tile; a number is never
  fabricated. There is no live server-side market-regime classifier (the only regime code is a
  research-only historical classifier with no endpoint), so no regime badge is displayed and none
  is created this phase.
- **Evidence:** `engine/market_data.fetch_market_indicator_latest` / `fetch_fred_latest_series`;
  the strip's UI markup had been deleted while the CSS + loader + endpoint survived (a pure UX
  regression); no DXY/BTC owner exists repo-wide; `PAPER_TRADER_FRED_API_KEY` is unset.
- **Consequence:** market context is a distinct, clearly-labelled reference surface; the market
  owner stays the single source; no provider/paid-data dependency is introduced by the UI.
- **Status boundary:** this is an operator-experience decision only; it changes no scoring,
  no portfolio math, and no safety contract.
- **Evidence:** the static guard `check_data_expansion_ownership` confirms the sole calculation +
  API owners, the reuse (never forking) of the existing owners, the two GET-only routes, no
  purchase/subscribe/activate/integrate route, no secret/credential ownership, ONE UI loader with
  no gate computation, immutable/idempotent artifacts, cadence disabled, that the gate is never a
  DRC daily job, and that Slice 10 (Intraday) remains future; audit inventory drift = 0.
  Deterministic tests inject every input; no paid provider is ever called (fixtures only).
- **Consequence:** Charter Milestone 5's data-expansion mandate is delivered as a formal,
  evidence-gated, manual-approval decision framework. Slice 10 (Intraday / near-real-time,
  Milestone 6) remains next; Slice 11 (Controlled Execution, Milestone 7) remains later; cadence
  remains disabled.

## Rejected alternatives

- **R-1 — Rewrite the backend from scratch.** Rejected: violates Principle 8;
  the entangled monolith + implementation-coupled tests make a big-bang rewrite
  high-risk with no incremental value.
- **R-2 — Bless the two NAV authorities as permanent.** Rejected: Principle 1;
  divergent NAV across UI surfaces is a correctness risk.
- **R-3 — Auto-promote challengers that pass gates.** Rejected: Principle 7;
  promotion requires manual approval and adequate forward evidence.
- **R-4 — Delete "unused" modules on static evidence alone.** Rejected: static
  analysis does not prove runtime behavior; REMOVE_CANDIDATE requires confirming
  no live consumer first.

## Unresolved

- **U-1 — Authoritative price store long-term** (owned-EODHD vs Postgres): see D-5.
- **U-2 — Fate of the frozen `current_alpha_*` (Phase 13/16/17/18) viewers:** most
  answer settled research questions; confirm no UI consumer before deprecating.
- **U-3 — Champion daily-refresh subprocess** (`current_alpha_daily_refresh`
  shelling into a separate research repo): whether to internalize or keep as an
  external boundary.
- **U-4 — `db/models` execution tables** (Signal/TradeDecision/Order/Trade):
  archive vs remove, pending Slice 11 quarantine outcome.

---

## D-19.3 — ONE operator command, ONE post-close orchestration path (CONFIRMED)

**Context (live evidence, 2026-08-13).** With 29 repaired NEXT_CLOSE paper orders
SUBMITTED under plan `rbop_2026-08-12_alpha_paper_book_1_1a198f560cca`, the live
operating path exposed two control-plane defects.

**Defect A — the operator had to hunt.** The Today / Portfolio Manager screens
rendered FOUR enabled controls for the SAME write action (`Run Daily Close`): the
Today hero CTA, the canonical workflow banner CTA, the daily-close panel button
(`cc-dc-btn` / `dw-dc-btn` / `pm-dc-btn` / `dc-perf-btn`) and the right-rail button —
beside a dozen unrelated controls (`Refresh`, `Refresh View`, `Refresh Alpha Target`,
`Preview Snapshot`, `Confirm Target Snapshot`, `Cancel Submitted Orders`, …), while
the authoritative workflow could simultaneously say *No action required right now*.

**Defect B — two post-close orchestration paths.** Two code paths made the standalone
Paper Desk refresh a PREREQUISITE of the canonical Daily Close:

1. `daily_close.resolve_daily_close_status` short-circuited on `pending_orders`
   BEFORE it considered whether a newly eligible completed session existed, so a new
   close could never surface as `DAILY_CLOSE_DUE` while any order was working;
2. `daily_close._run_daily_close_locked` returned a no-write `PAPER_ORDERS_SUBMITTED`
   for a book with pending orders, so the close refused to run at all.

Both forced `POST /v1/paper-desk/refresh` first — even though the Daily Close already
COMPOSES that exact owner (`desk.refresh_desk` → owned EOD marks →
`settle_due_orders` NEXT_CLOSE settlement → immutable fills → performance).

**Decision.**

1. **A newly eligible completed close OUTRANKS passive pending-order monitoring.**
   `resolve_daily_close_status` computes `new_close_pending` first; the pending-order
   state is returned only when there is nothing new to close. Every fail-closed path
   (`DATA_BLOCKED`, `WAITING_FOR_MARKET_DATA`, `AWAITING_MARKET_CLOSE`) is preserved —
   eligibility is never manufactured.
2. **The Daily Close settles pending NEXT_CLOSE orders internally**, through the
   EXISTING Paper Desk owner. No second settlement engine, fill simulator, mark
   writer, order ledger or NAV owner was created. Settlement provenance
   (`pending_orders_at_start`, `pending_orders_after_settlement`,
   `settled_through_paper_desk`) is recorded ONCE, in the same decision-journal row.
3. **`forward_tracking` is separated from `book_active`.** `book_active` was
   `(filled or lifecycle==FILLED) and not pending` — False by construction whenever
   orders were working, which is exactly the case the precedence rule must handle.
   `forward_tracking` drops the `not pending` clause and drives close eligibility, the
   provider probe and baseline currency; `book_active` is preserved verbatim for every
   existing consumer.
4. **The Paper Desk refresh is MAINTENANCE / RECOVERY only.** It keeps its endpoint,
   its confirmation token and its ownership of owned marks + NEXT_CLOSE settlement,
   but `workflow_state.MAINTENANCE_EXECUTION_KINDS` classifies it as non-normal-path
   and `assert_primary_action_contract` fails CLOSED if it is ever promoted to the
   canonical `primary_action`. `WAITING_FOR_OWNED_DATA` now promotes the Daily Close
   (a strict superset of the refresh).
5. **ONE backend-owned operator command.** `workflow_state.build_operator_command`
   projects the already-decided state + primary action into five operator fields
   (state / task / why / next / at most one action). `primary_action_available` is the
   single authority for whether ANY normal-path mutation control may render, on any
   page. Every surface mirrors it; no page reinterprets it.
6. **ONE execution surface.** The UI's `_wsCommandOwnsExecution()` is the single
   shared helper by which the Today hero, the workflow banners and the four
   daily-close panel buttons defer to the command bar. Navigation links are never
   suppressed (routing is not a write). The right action rail is the ONE sanctioned
   mirror and must carry the IDENTICAL label.
7. **Current-rebalance counts are LINEAGE-scoped.** `operational_book.
   current_rebalance_lineage` and `rebalance_execution.build_execution_summary` filter
   every current-state count by the current order-plan lineage. The book's historical
   initial-implementation fills and any superseded/cancelled plan are reported
   SEPARATELY and remain fully auditable. `PARTIALLY_FILLED` now means "the CURRENT
   rebalance is part-filled", not "this book has ever filled anything".

**Why not the alternatives.**

- *Keep the desk refresh as the post-close action and make the close depend on it.*
  Rejected: two orchestration paths for one transition (Principle 2), and the close
  must own the atomic cycle it already implements.
- *Introduce a new `SETTLEMENT_DUE` status.* Rejected: `DAILY_CLOSE_DUE` already means
  exactly this; a duplicate status for naming convenience violates Principle 1.
- *Let each page keep its own CTA and merely restyle the primary one.* Rejected: the
  brief's failure condition is the operator having to infer which control is
  canonical; recolouring does not remove the inference.

**Guarded by** `scripts/audit_architecture.py:check_operator_atomic_close_ownership`
(33 blocking invariants) and `tests/test_stage19_3_operator_workflow_atomic_close.py`.
No broker, no automation, no automatic rebalance, no model promotion, no cadence
change, no new recalibration.

---

## Stage 20 decisions

### D-S20-1 — The portfolio-level "should we act?" decision gets its own owner (CONFIRMED)

**Evidence.** `api/daily_research_cycle.py` ran `BUILD_REALLOCATION_PROPOSAL`
unconditionally after `ASSESS_HOLDING_OPPORTUNITY_COST`. `api/portfolio_decision.py`
derived materiality from the resulting proposal's own action counts — i.e. after the
allocation engine had already built a target. The system therefore produced a change
target on every signal refresh and relied on the operator to reject it.

**Decision.** Introduce exactly one new decision owner — `engine/portfolio_reassessment.py`
(pure) and `api/portfolio_reassessment.py` (composition/persistence/read) — between them.
It aggregates the Slice-6 per-holding analytics into ONE portfolio-level verdict and is
the only thing that authorises the Slice-7 target engine to run.

**Rejected alternative.** Extending Stage 18 materiality. That would have kept the target
being built first, and would have put an economic gate inside a module whose job is to
record a manual decision.

### D-S20-2 — Improvement is measured in signal-score points, never fabricated return (CONFIRMED)

No validated expected-return model exists (Phases 10-Q, 11-B/C, 13-C, 14-15, 17 all
terminated in `NO_DEFENSIBLE_ALPHA` on the tested families). Therefore every Stage-20
improvement is a combined-percentile comparison and `expected_return_*` is always
`EXPECTED_RETURN_NOT_CALIBRATED`. Switching cost is genuinely known from the canonical desk
cost model and IS reported in basis points and dollars. The illustrative "bps of return"
phrasing in the Stage-20 brief was deliberately NOT adopted for score deltas: presenting a
percentile delta as basis points of return would be fabricated evidence.

### D-S20-3 — The reassessment never assigns capital (CONFIRMED)

The kernel computes the concentration consequence of the recommended releases on the
RETAINED book by renormalising incumbents. That is the arithmetic consequence of an exit,
not an allocation. Assigning weight to a candidate remains `engine/reallocation_proposal.py`
alone, guarded by `second_target_engine_modules == []`.

### D-S20-4 — Risk vetoes test deterioration, not a pre-existing breach (CONFIRMED)

A book already above the sector cap or a concentration level is a standing condition the
operator owns. Blocking on the level would permanently freeze every future reallocation,
including the ones that would fix it. The gate therefore rejects a change only when it
makes concentration or sector weight WORSE.

### D-S20-5 — Cooldown is counted in observed sessions, never wall-clock days (CONFIRMED)

`evaluate_churn` counts distinct eligible sessions present in the immutable history, so a
weekend, a holiday or a missed cycle cannot silently expire a cooldown. The churn history
is derived from persisted reassessment artifacts, so a churn verdict is reproducible from
evidence rather than from a live desk read.

### D-S20-6 — Automatic GENERATION is allowed; automatic AUTHORISATION is not (CONFIRMED)

The cycle may compute and persist a reviewable proposal without a human. The Stage-18
approval token and the Stage-19 order-plan confirmation token remain independent and
mandatory, and only the second creates paper orders. `should_build_proposal` fails closed
on every non-`PROPOSAL_READY` state, including a missing or failed reassessment.

### D-S20-7 — An in-flight execution outranks a fresh reassessment (CONFIRMED)

A reassessment is evidence; a confirmed order plan is a commitment. `execution_precedence`
suppresses the reassessment's primary action in the workflow owner and the UI while paper
orders await NEXT_CLOSE settlement, so a new proposal can never overwrite, obscure or
compete with the execution lifecycle. Verified against the live shape (29 SUBMITTED,
plan `...1a198f560cca`).

### D-S20-8 — Reassessment history is forward-only (CONFIRMED)

Sessions before Stage 20 landed have no history row and are NOT reconstructed: a hindsight
backfill would be fabricated evidence. The gap is reported explicitly by
`GET /v1/operations/portfolio-reassessment/history`. Forward attribution measures an
outcome only where genuine owned closes exist after the recommendation date; a missing
outcome remains `PENDING`.

### D-S20-9 — Model recalibration stays a separate cycle (CONFIRMED)

The reassessment consumes model output and never promotes, retrains or recalibrates.
`api/research_agent.py` keeps recalibration governance, and may CONSUME reassessment
evidence for research without gaining operational authority. Guarded by
`recalibration_remains_separate`.

### D-S20-10 — Genuinely-new thresholds are versioned, justified and configurable (CONFIRMED)

Eight new thresholds are declared once in `default_policy()`, each with an inline economic
rationale, versioned by `portfolio_reassessment_policy.v1` /
`portfolio_reassessment_churn_policy.v1`, folded into the reassessment hash (so changing
one produces a NEW assessment rather than silently re-labelling an old one), exercised at
their boundaries by the test suite, and overridable through
`PAPER_TRADER_REASSESSMENT_POLICY` without a code change. No hidden magic numbers.

---

## Stage 21 decisions

### CONFIRMED - a currency check binds to an ECONOMIC fingerprint, never to a document hash

**Evidence.** On the live 2026-08-13 book, `state_hash` differed either side of the HOC
write (`02d9b7b8...` -> `636a16a6...`) while capital and positions were byte-identical. The
difference was `assessment.opportunity_cost_assessment_hash` - the assessment's own output,
composed into the state through `api.daily_action_gate`. Every fresh reassessment therefore
blocked itself.

**Decision.** `api.portfolio_state.economic_state_hash` is the ONE fingerprint any
"is this evidence still current?" question binds to. It is an explicit allowlist of economic
subtrees; research outputs and research cadence dates are structurally excluded. It is
stripped from `state_hash`, so adopting it invalidated nothing.

**Consequence.** A downstream consumer can never again invalidate its own input. A real
holdings / cash / NAV / corporate-action change still invalidates immediately.

### CONFIRMED - a missing fingerprint is UNVERIFIABLE, never STALE

**Evidence.** The HOC kernel recorded no corporate-action fingerprint, so every consumer
resolved `None`; `staleness_vs_registry` treats `None` as the EMPTY registry, which with the
MNST split registered made every reassessment permanently stale.

**Decision.** One canonical resolver (`hoc_corporate_actions_hash`), and an artifact that
recorded nothing is reported `UNVERIFIABLE` with an explicit reason. Claiming staleness from
missing evidence is fabrication: it blocked fresh assessments while telling the operator a
corporate action had been registered "since" an assessment that in fact post-dated it.

### CONFIRMED - execution identity is READ from the immutable ledger, never re-derived

**Evidence.** After settlement the rebalance read model reported `REBALANCE_NO_PROPOSAL`
with `filled_count = 0` while the ledger immutably recorded 29/29 filled on plan
`...1a198f560cca`. The read derived identity from the CURRENT proposal, which no longer
existed once the eligible session advanced.

**Decision.** `engine/execution_lineage.py` folds the immutable order + fill ledgers.
`latest_completed_rebalance` does not depend on a current proposal existing.

### CONFIRMED - order plans are ordered chronologically, never by id or hash

**Evidence.** `sorted(plan_ids)[-1]` ranks `...5bf9c6c20f8a` (22 cancelled, 0 filled) above
`...1a198f560cca` (29 filled) because "5" sorts after "1". A plan id ends in a hash, so id
ordering carries no temporal meaning at all.

**Decision.** Selection is by the lineage's recorded `created_at`, and a fully cancelled plan
is `SUPERSEDED_CANCELLED` and can never surface as current or executed.

### CONFIRMED - a client timeout is not an outcome

**Evidence.** The Aug-13 close succeeded while the POST outran a 300-second client timeout.

**Decision.** The close's progress document becomes a durable run record with an explicit
outcome, `writes_occurred`, and an explicit `safe_retry_allowed` + `retry_guidance`. The
status GET is the authority on reconnect. No second Daily Close and no new endpoint family.

### CONFIRMED - production fails closed on acceptance store roots

**Evidence.** Stage-20.1 fixture roots leaked into the operator's parent shell and a manual
restart served a fabricated empty portfolio indistinguishable from the real one.

**Decision.** A runtime guard at application import, not a script-only check, with an
explicit per-process hermetic opt-in (`PAPER_TRADER_ACCEPTANCE_MODE=1`). Protection must not
depend on remembering to use a particular launch script.

### CONFIRMED - OBSERVED and COUNTERFACTUAL_ESTIMATE are never mixed

**Decision.** A ticker's forward return between two owned closes is a market fact and is
always `OBSERVED`. A portfolio consequence is `OBSERVED` only when the recommendation
actually executed; otherwise it is an explicit `COUNTERFACTUAL_ESTIMATE`. The scorecard
reports the two totals in separate blocks and never sums them.

### CONFIRMED - Stage 21 recommends reviews, it never performs them

**Decision.** Policy intelligence may return `POLICY_REVIEW_CANDIDATE`. No threshold, model,
champion, cadence or portfolio can be changed by Stage 21, and `INSUFFICIENT_EVIDENCE`
changes nothing by construction. Enforced statically by
`check_stage21_outcome_intelligence`.

### UNRESOLVED - how much outcome evidence is enough to act on

Stage 21 reuses the existing forward-evidence gate boundaries (5 / 20 / 63 matured
observations) rather than inventing decision-quality-specific ones. Whether those boundaries
are the right ones for POLICY decisions - as opposed to model decisions - is genuinely
unresolved and needs its own evidence before anyone tunes it. Recorded here so the reuse is
a deliberate, revisitable choice rather than an implicit assumption.


### CONFIRMED — a hermetic harness owns its clock completely (Stage 21, Workstream 0F)

**Decision.** An acceptance harness that freezes a session must inject *every* read model
whose value derives from a date. A partially-injected harness is worse than no harness: it
reports a state inconsistency the product does not have, and it decays a little further with
every day the calendar advances, so the failure looks like drift rather than a defect.

**Evidence.** Stage 20.1 froze the eligible session at `2026-08-12` and injected the
operational book, desk marks, artifacts, freshness inputs and forward status — but not the
owned-model `current`, not alpha-target readiness, and not Daily Close progress. Those three
resolved from the live world. Six cross-panel tests were failing and had been carried as
known-bad on the (correct) reasoning that no operator-facing surface was wrong. That
reasoning was right about the product and wrong about the cost: the gate stayed unusable.

**Consequence.** `api.operational_book.load_operational_book` gains an additive
`target_readiness=` parameter, defaulting to `None`, matching the injection seam every other
read model in this codebase already offers. No market-session semantics changed; point-in-time
behaviour is unchanged; production behaviour with the parameter omitted is byte-identical.
Four blocking architecture invariants pin the three injections and the seam.

**Rejected alternative.** Overriding the dates after composition (`date_overrides`) would
have made the freshness rows agree while leaving the Operational Book panel itself carrying a
live target date — the two panels would have disagreed in the browser while the tests passed.
Binding the read seam, not the rendered value, is the Stage-20.1 principle and it still holds.

### CONFIRMED — the browser gate is not redundant with the contract gate (Stage 21)

**Decision.** UI changes are not releasable on HTTP contract verification alone. The mandatory
gate drives a real browser across every canonical scenario at 1920x1080 and 1440x900, and its
receipt is checked by both `validate.ps1` and `commit.ps1`.

**Evidence.** Both Stage-21 cockpit loaders called `apiGet`, which is not defined in that
scope. The Portfolio Manager fires its loaders fire-and-forget inside `try/catch`, so the
`ReferenceError` was swallowed: the console stayed empty, every route returned a correct
payload, every contract test passed — and the decision-evidence card sat on
*"Loading decision outcome evidence…"* indefinitely. Nothing short of rendering the page
could observe it. The loaders now call `_mhzGet` and the audit fails on any reintroduction
of `apiGet(`.

## Operator-workflow decisions

### CONFIRMED - ONE repository-owned backend restart / smoke workflow

**Decision.** Stopping, starting and live-smoking the Paper Trader backend is owned by
exactly one script in the repository:

    scripts/restart_paper_trader_backend.ps1

A stage handoff MUST delegate to it. A handoff `restart_smoke.ps1` may add stage-specific
authenticated **GET** assertions via `-SmokePath` and may fingerprint its own protected
stores; it may not reimplement process stop/start, port handling, health or readiness
polling, authentication setup, stdout/stderr diagnostics, or production store-root
validation.

**Evidence.** The project regenerated a stage-local `restart_smoke.ps1` for stage after
stage, and reintroduced the SAME defect each time: polling `http://127.0.0.1:8001/health`
when this application has only ever served `/v1/health` and `/v1/ready`. Stage 12 shipped
it. Stage 21 shipped it again. `paper_trader_8001.stdout.log` records the cost verbatim -
39 consecutive `GET /health HTTP/1.1 404 Not Found` lines, retried silently, before a human
probed the right path by hand. A defect that returns after being fixed is not a mistake
about health routes; it is a missing owner.

**Consequence.**

* The canonical readiness routes are permanent: `GET /v1/health` and `GET /v1/ready`.
* `scripts/audit_architecture.py::check_backend_restart_ownership` fails the build when any
  PowerShell workflow probes a noncanonical health/readiness path, launches the application
  outside the owner, uses a mutating HTTP verb, or probes a `/v1` path the application does
  not declare as GET. The release gate passes `--handoff-dir` so handoff scripts - which
  live outside the repository by design - are judged by the same guard.
* `LIVE_SMOKE_OK` is emitted by exactly one script, exactly once, and only after every live
  check has passed.
* On any startup failure the owner prints the launched PID, whether it is still alive, its
  exit code when available, the port-listener state and the tail of both logs BEFORE
  returning nonzero.
* `tests/test_canonical_backend_restart.py` proves the contract two ways: statically
  against the parsed route table, and by really executing the workflow against a hermetic
  stub backend on a throwaway port (contaminated roots, startup failure, success).

**Rejected alternative.** Keeping the workflow in the handoff kit and "remembering" the
right path. That is what was tried for nine stages. A handoff directory is a throwaway; the
operator workflow is not, so it belongs in the repository.

**Two findings that only a real execution could produce.** The first version of the owner
asserted that the listening PID equalled the PID it launched. That is false for uvicorn on
Windows, which supervises a CHILD that owns the socket - the assertion would have failed
every real restart. The check now walks the parent chain and accepts the launched process
or any descendant. The regression test itself hung until its output was redirected to files
rather than pipes: a successful restart deliberately leaves the backend running, and the
surviving grandchild holds the inherited pipe open long after the script has exited
cleanly. Both are recorded here because both are invisible to static analysis.

**Known second implementation, deliberately retained.** The handoff `_common.ps1` keeps its
own `Assert-ProductionStoreRoots`. It answers a DIFFERENT question - "is the operator's
parent shell clean?" - whereas `api/environment_isolation.py` answers "may THIS process
serve production?", including the hermetic-acceptance opt-in that the shell check must never
honour. The restart workflow no longer uses the PowerShell copy: it delegates to the Python
owner.

## Stage 22 decisions

### D-S22-1 — The normal daily cycle is a canonical, ordered contract with ONE owner (CONFIRMED)

**Decision.** The five stages (wait -> daily close -> daily research cycle -> portfolio
decision -> controlled rebalance) are a frozen, ordered contract owned by the PURE kernel
`engine/normal_cycle.py`. `api/workflow_state.py` resolves the overall state from the
authoritative domain owners and PROJECTS it onto that sequence; no other module may.

**Why.** Every operator surface was individually correct and collectively ambiguous. Making
the promoted action unique (Stage 19.3) did not answer "and what comes after that?", so each
surface still implied its own next step. A sequence with one owner is what makes the answer
identical everywhere.

**Evidence.** `stage_for_overall_state` is total over `ws.OVERALL_STATES` and fails CLOSED
into RECOVERY for anything else; `check_normal_cycle_ownership` blocks a second owner and a
reordered sequence.

### D-S22-2 — The Daily Close outranks the Daily Research Cycle for an unclosed session (CONFIRMED)

**Decision.** A confirmed eligible completed session whose close is not complete has exactly
one canonical next action: the Daily Close (priority P3.7). A research run already in flight
or blocked still outranks it.

**Why.** Two legal orderings existed for the same session. The close is what advances owned
marks, settles NEXT_CLOSE paper orders and records NAV — research produced ahead of it
describes a portfolio that is about to change, and the operator had no way to know which
order was intended.

**Cost accepted.** Phase 29G.3's `test_27a` encoded the other ordering for the
confirmed-but-unclosed case. It was updated, with the reassessment requirement still
recorded and still unsatisfied — it simply becomes the operator's action once the close
completes.

### D-S22-3 — A completed Daily Close makes the Daily Research Cycle DUE (CONFIRMED)

**Decision.** A completed close for the eligible session makes the research cycle the one
required action until a Holding Opportunity-Cost assessment bound to that same session
exists (priority P4.5).

**Why.** The close composes both the owned-mark refresh and the model-input refresh, so it
can leave every REQUIRED signal input current while the session it just closed has never
been reassessed. The operator was then shown "monitor / no action required" and the daily
reassessment silently never happened — precisely the failure the canonical objective exists
to prevent.

**Boundary.** The requirement applies ONLY when the HOC contract is observable on the gate.
An absent contract is UNVERIFIABLE; inferring "not run" from a missing key would fabricate
work for the operator, which is the same class of error as inferring staleness from a
missing fingerprint (D-S21).

### D-S22-4 — Stale evidence is DEMOTED, never reinterpreted (CONFIRMED)

**Decision.** A blocked/stale assessment is classified SYSTEM_BLOCKER (fix it now) or
EXPECTED_STALE_EVIDENCE (the next canonical cycle supersedes it). The expected case is
demoted to EVIDENCE, or to HISTORY while the workflow is passive. `blocks_portfolio_action`
stays True in BOTH cases.

**Why.** The operator could not distinguish "you must act" from "this is correctly
non-actionable", so a correct fail-closed state read as an outage. This is an information-
hierarchy repair, not a validity change: nothing is hidden, no history is rewritten and no
rule about what may drive a portfolio change was touched.

**Fail-closed default.** An unrecognised blocker code, no blocker codes at all, a data block,
or a workflow already in recovery all classify as SYSTEM_BLOCKER. "Expected" is never
assumed.

### D-S22-5 — Gap severity is a PROPERTY of the gap, never inferred from its code (CONFIRMED)

**Decision.** `engine/data_gap_taxonomy.py` maps every gap code to a machine-readable record
(ticker, metric, expected + available as-of date, source owner, reason, BLOCKING vs
NON_BLOCKING, effect on the recommendation, safe fallback). Consumers read `blocking`.

**Why.** "1 data gap(s) documented" told the operator nothing, and a downstream consumer had
to parse a string to guess severity — which is how "documented" quietly becomes "ignored".

**Two hard rules.** An unknown code is BLOCKING by construction. Missing data is NEVER
converted to zero or to current data; a gap with no genuine point-in-time substitute reports
`safe_fallback = None` and says so.

**Placement.** Classification runs at the READ layer over an already-persisted immutable
assessment. Emitting it from the kernel would have changed `assessment_hash` and made every
existing production artifact conflict with its own re-run.

### D-S22-6 — A broken binding fails closed EXACTLY ONCE (CONFIRMED)

**Decision.** `build_assessment_binding` produces one verdict over assessment currency
(session, book, corporate-action registry) and proposal binding (assessment hash, session).
Every surface renders that verdict; none re-derives it.

**Why.** Four surfaces independently discovering the same broken binding produced four red
cards for one fact. UNVERIFIABLE is a distinct state from BROKEN and never blocks: an
artifact that recorded no fingerprint cannot prove currency either way.

### D-S22-7 — The portfolio decision is a REVIEW, so it can never open a mutation gate (CONFIRMED)

**Decision.** Only DAILY_CLOSE and DAILY_RESEARCH_CYCLE may open a stage mutation gate. The
portfolio decision carries `review_required`; the controlled rebalance keeps its own two
manual gates.

**Why.** "At most one primary mutation" must be a statement about WRITES, not about buttons.
Counting a read-only review as a mutation would have made the invariant meaningless in the
one state where it matters most.

**Enforcement.** `assert_single_primary_mutation` raises inside the composition, so the
invariant fails before it can reach a browser rather than being asserted only in a test.

### D-S22-8 — A hermetic scenario must be able to bind EVERY seam (CONFIRMED)

**Decision.** `artifact=None` means "not supplied", not "none exists". Canonical artifact
readers take explicit `hoc_dir` / `reallocation_dir` / `actions_dir` seams, and
`load_workflow_state` accepts `reassessment_summary` and `decision_record`.

**Why.** Without them the acceptance harness composed a "synthetic" world whose
opportunity-cost, reallocation, reassessment, decision and corporate-action state were read
from the operator's REAL stores — so a scenario could pass while describing live evidence,
and an ABSENT artifact was unrepresentable. This is the same defect class Stage 20.1 fixed
for the freshness contract and Stage 21 fixed for the close journal, found in the remaining
readers.

### D-S22-9 — A payload that states a session twice must freeze it ONCE (CONFIRMED)

**Decision.** `api.alpha_book.load_desk_mark_readiness` takes an additive
`latest_completed` seam (omitted — the production path — it resolves the live clock through
`alpha_target`, unchanged). `api.operational_book.load_operational_book` gains **no** new
parameter: it forwards the session the caller already declared through `target_readiness`.
One injection therefore freezes both reads, and `scripts/stage20_ui_fixtures.py` fails any
scenario whose two published sessions differ (`REQUIRED_SESSION_NOT_FROZEN`).

**Why.** The operational payload states the latest completed market session twice —
`current_target.latest_completed_market_date` and `desk_mark_required_date`. In production
both are the same call, so they cannot disagree; making only the first injectable broke that
invariant inside hermetic worlds. Every frozen scenario published a real-calendar date and
degraded to `DESK_MARK_BEHIND` / `REFRESH_DESK_MARKS` with a
`DESK_MARK_DATE_BEHIND_REQUIRED` blocker the day the wall clock passed the frozen session —
the Stage-21 Workstream 0F decay signature, in the last unbound read. A second explicit
parameter was rejected: it would let a caller declare two different sessions and
reintroduce exactly the disagreement the seam exists to prevent.

### D-R28-1 — Decision authority is a property of the SOURCE FAMILY, decided once (CONFIRMED)

**Decision.** `engine/event_fabric.py` holds ONE table mapping every event family to a
decision authority (`OPERATIONAL_ALPHA` / `RESEARCH_ALPHA` / `OPERATIONAL_RISK` /
`EVENT_TRIGGER_ONLY` / `OBSERVABILITY_ONLY` / `BLOCKED`) together with the reason it has
that authority. Nothing downstream may grant an event more authority than the table
gives it, and classification fails closed: an unknown record type gets
`OBSERVABILITY_ONLY` and is counted by the terminal audit, which must report
`UNCLASSIFIED_SIGNAL_AUTHORITY = 0` to release.

**Why.** "News should make us look again" and "news should not move a score" are both
true, and every call site that has to re-derive that distinction is a place it can be
lost. Making it one table makes the unsafe case unreachable rather than merely
discouraged: `authority_may_change_alpha` returns True for exactly one authority, and
the reallocation, reassessment and scoring owners are only ever reached through the
dependency graph that authority feeds.

### D-R28-2 — The event lane refreshes calculations it does not own (CONFIRMED)

**Decision.** `api/event_signal_refresh.py` calls
`api.holding_opportunity_cost.run_and_persist`,
`api.portfolio_reassessment.run_and_persist` and
`api.reallocation_proposal.run_and_persist` — the exact entry points
`api.daily_research_cycle` uses — and defines no scoring, allocation, target or order
function of its own. The shared list is declared in `CANONICAL_CALCULATION_DELEGATES`.

**Why.** The alternative — an "event-mode" reassessment tuned for incremental input —
is how a system ends up with two portfolio brains that disagree at the moment it matters
most. Daily mode is the FULL refresh of the dependency graph; event mode is the
INCREMENTAL refresh of the same graph. That framing makes convergence a structural
property rather than a promise, and it is asserted by test.

### D-R28-3 — Supersession is keyed on the DATE, not the native id alone (CONFIRMED)

**Decision.** A re-issued record supersedes an earlier one only when
`(source_id, source_event_id, effective_at)` match. A record whose canonical document
URL was already ingested is `SYNDICATED`, checked *before* the supersession rule.

**Why.** Measured against the real corpus, the naive rule was badly wrong in two ways.
Several collectors reuse a native id across days — `nasdaqlisted|ABNB` appears every
session, a daily bar repeats its ticker — so 25 genuinely distinct daily observations
were classified as 24 "corrections" of the first. Separately, one wire article is
fetched once per symbol it mentions and one SEC accession is seen by two collector
lanes, so identical documents differing only in collection metadata looked like
material updates. Together those produced 4,065 spurious `MATERIAL_UPDATE` events in a
single 30-day window, every one of which would have carried new information into the
trigger path.

### D-R28-4 — Anti-churn is a fingerprint over CONCLUSIONS, not over events (CONFIRMED)

**Decision.** `engine/event_materiality.py` collapses triggers by
`(code, entity, family, event_date)` and computes the anti-churn fingerprint from those
collapsed keys — never from individual event ids.

**Why.** Keying on event ids meant that re-collecting the same filing produced a
different fingerprint and therefore a second full assessment, which is exactly the churn
the gate exists to prevent. Keying on the conclusion makes twenty re-collected copies
one reason (carrying `occurrences: 20`), while including the event DATE keeps
tomorrow's genuinely new 8-K on the same name a genuinely new trigger.

### D-R28-5 — BLS and BEA are REDUNDANT, not "not yet integrated" (CONFIRMED)

**Decision.** `bls` and `bea` carry the terminal state
`REDUNDANT_WITH_EXISTING_SOURCE`; `prediction_service` carries
`NOT_ECONOMICALLY_USEFUL` for the event fabric. There is no
`AVAILABLE_BUT_NOT_INTEGRATED` state in the vocabulary.

**Why.** CPI and unemployment are already collected from FRED **with ALFRED vintages**;
BLS v2 supplies the same numbers without vintages, so integrating it would add a second
copy with worse point-in-time quality. BEA's quarterly national accounts restate without
vintages and arrive far slower than any reassessment cadence. The prediction service
emits model output whose inputs are data the fabric already carries, so admitting it
would double-count. Each is a measured judgement recorded with its evidence rather than
a deferral, which is what makes `READY_UNINTEGRATED_USEFUL_SOURCES = 0` meaningful.

### D-R28-6 — The near-real-time lane is RISK-ONLY (CONFIRMED)

**Decision.** The delayed-quote adapter (`engine.market_data`, ~15 minutes behind the
tape — the fastest cadence available under current entitlements) emits `MARKET_QUOTE`
events with `OPERATIONAL_RISK` authority. GDELT emits `EVENT_TRIGGER_ONLY` news
metadata. Neither can move an expected-return score.

**Why.** No released signal contract is formed at intraday frequency. An intraday
feature is not alpha merely because it exists, and the honest thing a faster feed buys
today is a better answer to "is the amount of capital still appropriate?" — not a
faster opinion about value.

## Release 29 decisions

### D-R29-1 — Freshness has TWO questions, and the KPI answers only the first (CONFIRMED)

**Decision.** `engine/collection_cadence.py` separates `due_window_active` ("should this
source be current right now?") from `collect_now` ("should this iteration call it?").
The operator KPI denominator is the first question, restricted to OPERATIONAL lanes;
research and observability lanes are reported under `RESEARCH_ONLY`, and BLOCKED /
DISABLED rows never enter the denominator.

**Why.** Release 28 judged all 17 registry rows against a single anchor date, so a
monthly Treasury series, a quarterly BEA lane, a terminally blocked options feed and a
delayed quote on a Sunday all read STALE and were counted as "degraded". The number was
not wrong by accident — it was answering a question nobody asked. Restricting the
denominator to the sources whose own publication window is open makes "2 of 2 healthy"
on a Sunday a true statement instead of "1 of 17 fresh", which was a true statement
about nothing.

### D-R29-2 — `next_due_at` is ABSENT while the publication window is closed (CONFIRMED)

**Decision.** A source whose window is closed reports no `next_due_at`; the runtime
reason carries the explanation instead.

**Why.** The row previously read "Not a weekday (WEEKEND); this source does not publish.
Next due 00:13" — two contradictory statements, the second of which was false. There is
no authoritative exchange-holiday calendar in this repo, so a computed "window reopens
at" would be a fabricated fact. Saying nothing and naming the reason is the honest
option; it is the same discipline as refusing to invent a `HOLIDAY` session phase.

### D-R29-3 — A delayed quote identifies the DAY, not the minute (CONFIRMED)

**Decision.** `capture_market_quotes` keys `source_event_id` on `(ticker, market date)`.
An unchanged re-read is an exact duplicate; a changed price is one immutable
`MATERIAL_UPDATE` superseding the prior mark for that day.

**Why.** With a minute-keyed identity, every poll of a still market created a new event
for every holding — 25 fabricated events an hour, ~650 a session, none of which carried
a new fact. It also made the hermetic acceptance verdict depend on whether two
iterations straddled a real-world minute boundary. This is D-R28-3 (supersession is
date-keyed) applied to the fastest lane.

### D-R29-4 — A market OBSERVATION is never material on its own (CONFIRMED)

**Decision.** `market_bar` and `market_quote` are suppressed at the event-trigger stage
with `MARKET_OBSERVATION_NOT_MATERIAL_ON_ARRIVAL`. Materiality for those families is
decided by the risk lane from the MOVE it measures against a stated threshold. To keep
the quote lane useful, `ret_intraday` (delayed quote vs the owned close) was added and
raises `HOLDING_PRICE_SHOCK` at the SAME 7% level as a one-session move. Policy version
is now `event_materiality.v2`; no threshold number changed.

**Why.** A routine quote was being reported as "a material company event (market_quote /
DELAYED_QUOTE) named NVDA". Manually, that fired rarely. Continuously, it would have
re-run opportunity cost and reassessment every 15 minutes and listed every holding as
affected — the exact churn the materiality gate exists to prevent. This is the rule
already applied to macro releases: a new observation is never material on its own; a
measured transition is. Suppressing the arrival WITHOUT adding the measurement was
rejected: it would have made the Tier-0 quote lane pure cost, unable ever to influence
a decision, while the charter asks for a manager that reacts close to real time.

### D-R29-5 — ONE clock per event cycle, supplied by the caller (CONFIRMED)

**Decision.** `run_event_signal_refresh` accepts `now_iso` and threads it into every
live adapter; the collection iteration passes its own clock. An adapter takes no
ambient time reading of its own.

**Why.** The adapters called `datetime.now()` internally, so a replay driven by a
simulated clock stamped its events with the real one. The result was not a wrong answer
but an UNREPEATABLE one: the same code passed or failed depending on the real-world
minute. This is the Stage-21 Workstream 0F rule ("a hermetic harness owns its clock
completely") extended to the adapter layer, where it had leaked.

### D-R29-6 — The READ surface is bound to the GATE rule, never to a second definition (CONFIRMED)

**Decision.** `load_event_signal_refresh_status` filters its `material` list with the
materiality owner's own `MARKET_OBSERVATION_FAMILIES` constant.

**Why.** The gate and the surface that reports on the gate had drifted apart before
(Stage 20.1: bind the READ seam, not the stores). Left unbound, the operator would read
"42 material events" on a day when nothing happened except that the market was open,
while the decision path correctly did nothing — the worst combination, because the
number invites a manual override of a correct decision.

### D-R29-7 — Collection automation is a DIFFERENT switch from execution automation (CONFIRMED)

**Decision.** The operator arms collection once with
`CONFIRM_ENABLE_INFORMATION_COLLECTION`. There is no HTTP route that starts a worker,
runs an iteration or enables collection; the service is installed and started only
through `scripts/manage_information_collection.ps1`, whose `Status` is read-only and
whose every mutation requires `-Execute`. Execution automation stays off, unreachable
and architecture-tested.

**Why.** "Automation off" was a single badge covering two unrelated things. Making
information flow continuously is a governance decision about DATA; creating an order is
a governance decision about MONEY. Collapsing them would have forced a choice between a
stale decision surface and a weakened execution boundary. Keeping the start path out of
HTTP entirely means no browser, script or misrouted request can begin calling providers.


### D-R29.3-1 — A concept's vocabulary belongs to its owner (CONFIRMED)

**Decision.** A module may publish only the vocabulary of the concepts it owns. The
legacy rank-membership gate (`api.daily_action_gate`) no longer emits
`PROPOSAL_READY` / "PORTFOLIO CHANGES PROPOSED"; its tokens are
`MEMBERSHIP_DRIFT_DETECTED` / `MEMBERSHIP_DRIFT`, and `api.daily_close` records
`DAILY_CLOSE_COMPLETE_MEMBERSHIP_DRIFT`.

**Why.** Phase 29G.1 reclassified the legacy comparison's PRESENTATION but left its
TOKENS speaking the proposal owner's language. Presentation is what a human reads;
tokens are what every downstream consumer reads. On 2026-08-17 that gap produced one
payload asserting both `REBALANCE_PROPOSAL_READY` and `REALLOCATION_PROPOSAL_NOT_RUN`.
Release 30 (Telegram) would have forwarded the wrong half.

**Evidence.** Live `GET /v1/operations/workflow-state`, 2026-08-17, reassessment
`prs_2026-08-17_alpha_paper_book_1_7edb4353341f`.

### D-R29.3-2 — Immutable history is migrated on READ, never rewritten (CONFIRMED)

**Decision.** `api.daily_close.normalize_close_status` / `normalize_close_decision` map
the legacy token onto the canonical vocabulary at read time. Persisted journal rows are
never edited.

**Why.** The Aug-17 close row carries `REBALANCE_PROPOSAL_READY`. Rewriting it would
destroy evidence; leaving it unmapped would keep the contradiction alive on every
historical read. Read-time normalisation gives one vocabulary and zero rewritten bytes.

### D-R29.3-3 — A constraint is decided on the object that determines it (CONFIRMED)

**Decision.** Turnover budget, concentration, sector concentration and post-change risk
are properties of the COMPLETE TARGET and are decided once by
`engine.reallocation_proposal`. `engine.portfolio_reassessment` publishes the same
arithmetic as explicitly non-binding pre-proposal context and blocks on none of it.

**Why.** The reassessment can only see the retained stub, which must be renormalised to
1.0 to be comparable. On 2026-08-17 the release set freed ~49.6% of the book, so FANG's
reported weight rose 0.0442 → 0.0816 without a dollar moving, and the sector cap fired
comparing `Unknown` (0.325) against `Information Technology` (0.374). Those are
renormalisation artifacts, not economics. The proposal engine can also RETAIN an
incumbent when no feasible net-positive replacement exists, so the pre-proposal turnover
estimate is an upper bound — judging the budget on it rejects plans that are in fact
within it.

**Bounded by.** A breach WITHHOLDS the proposal (`WITHHELD`); it never trims the target
to fit and never relaxes a limit to force a proposal into existence. `WITHHELD` is
fail-closed at the kernel, the read API, the decision owner and the workflow composition.

### D-R29.3-4 — Mandatory eligibility exits override economics, never feasibility (CONFIRMED)

**Decision.** `ELIGIBILITY_EXIT_OVERRIDES_ECONOMIC_GATES_ONLY`. The override defeats
`BELOW_PORTFOLIO_NET_IMPROVEMENT_HURDLE` and `IMPROVEMENT_NOT_MEASURABLE`; it never
defeats liquidity or churn protection, and never the complete-target constraints. The
operator obligation is `REQUIRED_IF_REALLOCATION_PROCEEDS`.

**Why.** This was the policy the kernel already DOCUMENTED ("an unmeasurable or
sub-hurdle improvement must never trap an ineligible name in the book") but not the one
it implemented: the guard tested `not blockers` while the sub-hurdle code was itself in
`blockers`. On 2026-08-17 that trapped AIZ (rank 33/199) and SPG (rank 31/199) against
an exit buffer of 30, while the product simultaneously told the operator they "must
exit" and that no action was required. Option B (rename "mandatory" to a pure signal)
was rejected: the eligibility rule is a real portfolio constraint, and downgrading it to
advisory would let an ineligible name sit in the book indefinitely.

**Bounded by.** Clearing the ASK authorises nothing. It only lets the proposal owner
build a complete bounded target, which stays review-only behind two manual gates. No
naked sell-only order plan is ever produced.

### D-R29.3-5 — Consistency means SEMANTIC agreement between owners (CONFIRMED)

**Decision.** `api.workflow_state.check_decision_semantics` adds six semantic invariants
to the date checks, comparing authoritative owners and recomputing none of their
economics (architecture-tested).

**Why.** The pre-29.3 validator compared dates and one action-policy rule, so it
certified the 2026-08-17 payload CONSISTENT while it claimed both PROPOSAL_READY and
NOT_RUN. A validator that recomputed the economics to check them would itself become a
second calculation of the concept it exists to protect.
