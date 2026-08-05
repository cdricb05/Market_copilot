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

### D-10 — No big-bang rewrite of the monoliths — CONFIRMED
- **Decision:** `api/app.py` (20.5k) and `api/ui/index.html` (26.7k) are reduced
  incrementally (domain routers, extracted view logic) as owning contexts
  stabilize — never rewritten wholesale.
- **Evidence:** 178 routes + the legacy pipeline are entangled in one file; 648
  implementation-coupled test assertions raise rewrite risk.
- **Consequence:** every slice preserves behavior and migrates contracts
  (Principle 8).

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
