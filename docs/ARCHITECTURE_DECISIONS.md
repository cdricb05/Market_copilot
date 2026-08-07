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
