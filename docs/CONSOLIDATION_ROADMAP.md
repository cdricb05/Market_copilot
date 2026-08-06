# Paper Trader — Consolidation Roadmap

> Bounded, test-guarded slices that move the system from
> [CURRENT_ARCHITECTURE.md](CURRENT_ARCHITECTURE.md) toward
> [TARGET_ARCHITECTURE.md](TARGET_ARCHITECTURE.md), in service of the milestones
> in [PROJECT_CHARTER.md](PROJECT_CHARTER.md). Decisions behind these slices are
> recorded in [ARCHITECTURE_DECISIONS.md](ARCHITECTURE_DECISIONS.md).

## Guardrails (non-negotiable)

- **No big-bang rewrite.** There is no phase that rewrites `api/app.py` or
  `api/ui/index.html` from scratch, and none that replaces a subsystem wholesale
  in one step. Every slice is incremental, preserves working behavior, keeps
  regression coverage green, and deliberately deprecates obsolete paths
  (Principle 8).
- **No runtime behavior change without its own slice + tests.** Documentation and
  static tooling (Phase 29A) change no runtime code.
- **Paper-only, preview-first, manual-review, no-automation** boundaries hold in
  every slice. Execution stays deferred until Slice 11.
- **One concept, one owner** is the acceptance bar: a slice is done when the old
  duplicate is provably unused (or a thin wrapper) and the new owner is the only
  writer.

## Slice ordering rationale

Slices 1–2 remove the two conflicts that everything else reads (dates, workflow
state). Slices 3–5 make the daily research cycle and portfolio state canonical
(Milestone 1). Slices 6–8 build the active-manager product (Milestones 2–4).
Slices 9–11 are the charter's deferred tracks (Milestones 5–7).

---

## Slice 1 — Canonical market session & freshness

- **Objective:** one authoritative eligible-market-date / freshness service;
  eliminate the ≥8 resolvers and 6 `_today()` seams.
- **Existing modules:** `engine/market_hours` (primitive), `daily_operating_run`
  (16:00 logic), `daily_close` (17:30 + probe), `alpha_target`,
  `forward_prediction_skill`, `current_alpha_tournament_sync`, `market_screener`.
- **Target owner:** a `market_session` service built on `engine/market_hours`.
- **Files likely affected:** the resolver call sites (behind the new service),
  `api/app.py` date queries.
- **Dependencies:** none (foundational).
- **Migration method:** introduce the service; route each resolver through it one
  at a time; keep the 17:30-vs-16:00 policy explicit and configurable.
- **Compatibility bridge:** existing functions delegate to the service and keep
  their signatures until callers move.
- **Tests required:** parity tests proving each old resolver equals the service
  for a matrix of clocks/holidays; the existing date tests stay green.
- **Rollback:** revert the delegation; the primitives are unchanged.
- **Completion gate:** audit `eligible_market_date` multi-writer set = 1.
- **Principle:** 1. **Milestone:** 1.
- **Status — LANDED (Phase 29B).** Canonical owner `engine/market_session.py`
  (pure; expected vs owned-confirmed vs eligible date + frozen session-status
  vocabulary; 17:30-vs-16:00 close policy is an explicit `close_cutoff_et`
  parameter). Freshness owner `api/data_freshness.py` (read-only, cadence-aware,
  frozen freshness vocabulary) exposed at `GET /v1/operations/data-freshness` and
  rendered by the single UI `loadDataFreshness()` loader. `daily_operating_run`,
  `daily_close` and (transitively) `alpha_target` delegate; no runtime was
  deleted. **Remaining:** `paper_trading_desk._required_mark_date` (desk owner not
  touched this slice), `current_alpha_tournament_sync` and `engine/market_screener`
  are documented follow-ups. Historical-evidence and forward-roll calendars are
  kept as distinct concepts. Slice 2 (workflow state) is unblocked but not begun.
- **Corrective patch — active operational book alignment (Phase 29B.1).** The
  first build resolved the freshness dates from
  `current_operating_state`/`portfolio_valuation` — the **dormant
  legacy/current-alpha research book** (`2026-07-20`) — which leaked in as the
  owned-data confirmation and produced a false `WAITING_FOR_OWNED_DATA` /
  eligible `2026-07-20` while the ACTIVE operational book was complete at
  `2026-08-04`. The patch re-owns every OPERATIONAL date via
  `operational_book.load_operational_book` (active book) + `paper_trading_desk`
  (owned desk marks / SPY), resolves each RESEARCH date from its own owner without
  collapsing them, reads the frozen monthly momentum input directly from its
  persisted `month_label` (never proxied; `MISSING`/`UNKNOWN` when absent), and
  adds the active-book identity + a read-only cross-surface consistency validator.
  **Corrective-patch completion gate:** the endpoint stays authenticated, GET-only,
  read-only, provider-free and prediction-free; the UI keeps exactly one
  `loadDataFreshness()` loader and performs no market-date arithmetic; audit
  inventory drift = 0; a regression fixture reproducing the live state asserts
  `SESSION_READY`, eligible = active mark, completed close still valid,
  `momentum_monthly` STALE + in `slower_inputs_due`, `weakest_gate` = the exact
  stale research source (never an owned-data lag), and `consistency_status`
  `CONSISTENT`; no dormant research book supplies operational readiness.

## Slice 2 — Canonical workflow / read-state model

- **Objective:** one workflow/gate state vocabulary consumed everywhere.
- **Existing modules:** `daily_action_gate` (target_state), `daily_close`
  (11 statuses), `operational_book` (lifecycle), `daily_workflow_dashboard`
  (legacy stages), `command_center` (`_derive_stage`), `app.py:_build_workflow_state`.
- **Target owner:** `daily_action_gate` as the state authority; others render it.
- **Files likely affected:** the read-model composers + UI state rendering.
- **Dependencies:** Slice 1.
- **Migration method:** define the canonical state enum in the gate; have
  `daily_close`/`operational_book`/`command_center` map to it; retire the legacy
  dashboard vocabulary with the legacy Daily-Plan surface (Slice 11 quarantine).
- **Compatibility bridge:** a translation shim from old status strings.
- **Tests required:** contract tests that every surface reports the same state
  for a fixed backend fixture; migrate the UI-substring tests to state contracts.
- **Rollback:** shim reverts to per-surface strings.
- **Completion gate:** audit `workflow_state` multi-writer set ≤ 1 authority.
- **Principle:** 1, 2, 6. **Milestone:** 1.
- **Status — LANDED (Phase 29C).** Refined from "the gate is the state authority"
  to a dedicated read-only composition owner `api/workflow_state.py` — the ONE
  owner of the **combined operator interpretation** (overall state, current task,
  single primary next action + severity, queued follow-ups, assessment currency,
  blockers, completed summary, cross-surface consistency). It **composes** the
  Slice-1 `data_freshness` contract plus the existing domain-fact owners (Daily
  Action Gate, probe-free Daily Close progress, active Operational Book, Forward
  Prediction Skill, alpha-target readiness) and never re-derives their business
  logic. New surface: `GET /v1/operations/workflow-state` (authenticated, GET-only,
  read-only) rendered by ONE UI `loadWorkflowState()` loader on the Command Center,
  Daily Workflow, Portfolio, Portfolio Manager, Research & Audit and the
  Action/Safety panel; the UI derives no workflow priority or assessment currency.
  Frozen vocabularies: overall-state (`WAITING_FOR_SESSION_CLOSE` /
  `WAITING_FOR_OWNED_DATA` / `RESEARCH_CYCLE_REQUIRED` /
  `PORTFOLIO_REASSESSMENT_REQUIRED` / `READY_FOR_DAILY_CLOSE` /
  `DAILY_CYCLE_COMPLETE` / `DAILY_CYCLE_COMPLETE_EVIDENCE_GAP` /
  `MANUAL_REVIEW_REQUIRED` / `INCONSISTENT_STATE`), assessment-currency
  (`CURRENT`/`STALE`/`DUE`/`OVERDUE`/`MISSING`/`INCONSISTENT`), action-severity
  (`INFO`/`SUCCESS`/`ATTENTION`/`BLOCKED`/`ERROR`). The decision-currency defect is
  repaired: an older Daily Action Gate result is never re-presented as a stale
  "NO ACTION TODAY" today-conclusion (it is preserved, dated, in
  `completed_summary`); a valid completed close with a forward-evidence gap is
  ATTENTION, never an operational failure (D-7). **Deliberately kept:** the four
  legacy stage vocabularies (`app.py:_build_workflow_state` + `_canonical_daily_stage`,
  `command_center._derive_stage`, `daily_workflow_dashboard`) and every existing
  endpoint/panel remain (they retire with the legacy Create-Orders surface in
  Slice 11). Slice 2 performs no workflow action; the Persistent Daily Research
  Cycle and portfolio reassessment (Slice 3) are described/routed but not executed.
- **UI hard cutover — Phase 29C.1 (COMPLETE).** The Slice-2 backend was correct, but
  older visible surfaces still contradicted it: the Daily Action Gate cards read
  "DAILY ACTION GATE — TODAY / NO ACTION TODAY" and the Action/Safety panel read
  "DAILY GATE — NO ACTION TODAY", re-presenting a valid **historical** (dated) result
  as a current "today" conclusion. Root cause: the canonical `renderWorkflowState`
  banners and the legacy `renderDailyActionGate` / `renderDailyClose` /
  `renderOperationalBook` writers targeted overlapping DOM nodes, so the last async
  loader to finish won. The cutover makes `api.workflow_state` own **every visible
  primary interpretation** via two additive backend presentation blocks
  (`assessment_presentation` — a DATED historical result with its canonical currency,
  "today" wording permitted only when current; `evidence_presentation` — the still-open
  current session kept distinct from the completed close's documented, attention-level
  forward-evidence gap) and one canonical UI owner (`renderWorkflowState`, the single
  `loadWorkflowState()` loader) that exclusively writes the workflow banners, the right
  Action/Safety panel (task / next action / primary button / close + assessment-currency
  badges) and the reframed Daily-Action-Gate card TITLE / currency BADGE / HEADLINE /
  EXPLANATION across Command Center, Daily Workflow, Portfolio, Portfolio Manager and
  Research & Audit. Ownership is enforced by a STATIC guard in the shared specialized
  setters (`_dcSet`/`_dagSet`/`_obSet` refuse canonical nodes) plus a `data-wf-owned`
  stamp, so the final visible state is **independent of async completion order** — the
  canonical value wins by ownership, not timing (proven by an in-process DOM harness
  that runs the real renderers in every order). The specialized loaders keep their
  DETAIL only (checks, turnover, dates, P&L, holdings); the raw Daily Action Gate
  endpoint retains its historic `NO_ACTION_TODAY` outcome vocabulary. No runtime
  business action; Slice 3 remains next and unimplemented.

## Slice 3 — Persistent Daily Research Cycle orchestration

- **Objective:** one orchestration path for the daily research pass with no
  hidden operator prerequisites.
- **Existing modules:** `alpha_target.run_refresh`, `current_alpha_daily_refresh`
  (subprocess), `multi_horizon_engine.build_current`, `daily_close` (embeds refresh).
- **Target owner:** a `research_cycle` orchestration composing session → data →
  features → scoring → status → forward evidence.
- **Files likely affected:** the standalone refresh endpoints become thin wrappers.
- **Dependencies:** Slices 1–2.
- **Migration method:** compose the existing writers behind one entry point;
  standalone `/v1/alpha-target/refresh` and `/v1/paper-desk/refresh` delegate to
  it so they can no longer desynchronize (removes the pre-27H bypass).
- **Compatibility bridge:** endpoints keep their tokens and payloads.
- **Tests required:** idempotency + "no standalone desync" tests.
- **Rollback:** re-expose the standalone writers.
- **Completion gate:** desk-mark date and `market_as_of_date` provably advance
  together on every path.
- **Principle:** 2, 5. **Milestone:** 1.
- **Status — LANDED (Phase 29D).** Canonical owner `api/daily_research_cycle.py` —
  the ONE idempotent, resumable orchestration owner of the daily
  research-and-reassessment pass with **no hidden operator prerequisite**. It
  *orchestrates* the existing authoritative owners through explicit adapters and
  reimplements no business logic: session/plan/freshness → `api.data_freshness`
  (Slice 1); the daily price/score input refresh → `alpha_target.run_refresh`;
  universe scoring / TOP25 / TOP50 → `multi_horizon_engine.build_current`
  (unchanged — Slice 4 still owns consolidation); target preparation →
  `alpha_target.load_readiness` (**never auto-confirmed**); the immutable
  TRUE_FORWARD bundle → `forward_prediction_skill.capture_for_daily_close` (the
  required snapshot count is **derived from `SUPPORTED_BOOKS`**, never hard-coded,
  bundle `fca_<date>` first-write-wins, never backdated); the paper-only
  portfolio-assessment **bridge** → `daily_action_gate` (a compatibility bridge,
  **not** the Milestone-2 opportunity-cost engine). Frozen 16-state machine and
  step contract; idempotency key = `sha256(eligible date | active book | strategy
  version | universe | input-contract hash)` (completed runs are reused, safe
  incomplete runs resume, a conflicting concurrent contract is `INCONSISTENT`,
  a different contract for the same date never overwrites the immutable bundle).
  **The month boundary is now explicit:** there is no safe automatic
  monthly-momentum emitter in-repo, so the cycle returns `BLOCKED` /
  `RUN_RESEARCH_MONTHLY_INPUT_EMITTER` rather than approximating the frozen
  `mom_6_1` monthly input (the documented "August evidence gap" made visible).
  New surfaces: `GET /v1/operations/daily-research-cycle/status` (read-only,
  planning-only) and `POST /v1/operations/daily-research-cycle/run` (token
  `RUN_DAILY_RESEARCH_CYCLE`); one UI status loader `loadDailyResearchCycle()` +
  one execution function `runDailyResearchCycle()` render the canonical
  Daily-Research-Cycle card (the shell "Daily Alpha Refresh" is demoted to a
  champion-mark research detail). `api.workflow_state` **consumes** the cycle
  status (new `RESEARCH_CYCLE_RUNNING` / `RESEARCH_CYCLE_BLOCKED` overall states;
  the research action is now executable). Run manifests persist under
  `PAPER_TRADER_DRC_DIR` (a research root, atomic) — **never** the operational
  ledger root except through the already-authoritative evidence owner. It
  **never** runs the operational Daily Close (which idempotently reuses the SAME
  fps bundle), promotes/recalibrates a model, or creates an order/signal/decision/
  fill. Static guard `check_daily_research_cycle_ownership` enforces the sole
  owner, delegation, one UI loader/execution function, no UI planning, and the
  no-execution invariants; inventory drift = 0. **Not begun:** Slice 4 (canonical
  scoring), Slice 5 (portfolio state), Milestone 2 (opportunity-cost engine);
  cadence remains disabled.
- **Live-acceptance completion — Phase 29D.1 (Slice 3 remains LANDED).** The first
  real post-close live acceptance (2026-08-05, after the 17:30 ET cutoff) exposed
  three defects the offline fixtures had not: (1) `engine/market_session` inferred a
  *holiday* from the ABSENCE of same-day owned data — the desk marks and the SPY
  benchmark share ONE owned provider and lagged together on a normal publish delay —
  returning a ready `CALENDAR_POLICY_DEGRADED`; (2) `api/workflow_state` therefore
  surfaced the research blocker (`RESEARCH_CYCLE_BLOCKED`) instead of
  `WAITING_FOR_OWNED_DATA`; and (3) `api/daily_research_cycle` emitted
  `target_calculation — NO_REFRESH_OWNER`. Corrected: a weekday is a non-session ONLY
  through an authoritative exchange calendar or a persisted provider-confirmed
  contract (new `NON_SESSION` status); otherwise the expected weekday stays
  unresolved as `WAITING_FOR_OWNED_DATA` with a `calendar_policy_degraded` flag and
  the prior valid close is unchanged; the session is confirmed only when BOTH the
  owned market marks AND the benchmark reach the expected date. `target_calculation`
  now has a DECLARED prepared-downstream owner (`alpha_target.load_readiness`,
  produced by `STEP_PREPARE_TARGET`) — never `NO_REFRESH_OWNER`. The frozen monthly
  momentum input gains a DECLARED canonical in-repo adapter owner
  (`api/monthly_momentum_input.py`) that wraps an injectable emitter seam and owns the
  safe contract (due-ness / schema-period-provenance validation / idempotency /
  atomic persist / reuse-or-reject). There is still no safe automatic emitter bundled
  in the pure-stdlib repo, so a due month blocks HONESTLY through the adapter (never
  `NO_REFRESH_OWNER`, never a separate operator monthly button). `WAITING_FOR_OWNED_DATA`
  strictly outranks any research blocker. Static guard
  `check_slice3_live_acceptance_ownership`; inventory drift = 0. Slice 4 still owns
  consolidation; Slice 5 (portfolio state) remains not begun; cadence remains disabled.
- **Production monthly emitter bridge — Phase 29D.2 (Slice 3 remains LANDED).** The
  first live 2026-08-05 Daily Research Cycle and Daily Close succeeded (eligible session
  2026-08-05, full universe scored 234, target `READY_TO_CONFIRM`, assessment
  `PROPOSAL_READY`, TRUE_FORWARD 6/6, `FORWARD_EVIDENCE_COMPLETE`, Daily Close `VALID`),
  but ONLY after a MANUAL external monthly-input workflow + backend restart, because the
  released adapter's production emitter was unwired (`momentum_monthly —
  RUN_RESEARCH_MONTHLY_INPUT_EMITTER`). This bridge removes that final hidden
  prerequisite: `api/monthly_momentum_emitter.py` is the ONE pure-stdlib subprocess
  PRODUCER wired behind the adapter's seam (activated by the `api/app.py` import-time
  wiring). When momentum_monthly is due, ONE `RUN DAILY RESEARCH CYCLE` action resolves
  the external repo + Python, inspects the owned Phase-24 panel, runs the authoritative
  Phase-25 mathematics in an isolated temp dir through an explicit subprocess argument
  array, validates the artifacts (schema / unique tickers / produced month == eligible
  month / produced date == eligible / no future data / provenance / source-panel
  fingerprint / no intramonth approximation), promotes atomically through
  `api/monthly_momentum_input.py` (old/new-hash manifest; scoring cache cleared only after
  a validated promotion), and continues the SAME run through input alignment → scoring →
  target → TRUE_FORWARD evidence → assessment. Phase 24 supports no safe incremental
  extension, so a behind / future / unverifiable panel is an explicit DATA_HOLD blocker,
  never an uncontrolled full rebuild. No second monthly formula exists in Paper Trader;
  ownership is unchanged (Phase 24 = source panel, Phase 25 = mathematics,
  `monthly_momentum_input` = adapter, `daily_research_cycle` = orchestration,
  `universe_scoring` = scoring). Static guard `check_monthly_emitter_bridge_ownership`;
  inventory drift = 0. Slice 5 (portfolio state) remains next; the Persistent Alpha
  Research Agent remains a future milestone (Slice 8 / Milestone 4); cadence disabled.

## Slice 4 — Canonical universe scoring

- **Objective:** one scoring/ranking lineage; one shared `zscore`/`rank`
  primitive.
- **Existing modules:** `multi_horizon_engine` (canonical), `engine/scoring`
  (legacy), `market_screener`, the alpha factories (own battery), ≥8 z-score copies.
- **Target owner:** `universe_scoring` = `multi_horizon_engine` + one shared
  primitive.
- **Files likely affected:** the z-score/rank copies across `api/*` and
  `alpha_agent/*` (delete-and-delegate incrementally).
- **Dependencies:** Slice 1.
- **Migration method:** extract the primitive; replace copies one module per PR;
  keep `engine/scoring` only if a live Daily-Review consumer remains, else
  deprecate.
- **Tests required:** numeric-parity tests per replaced copy.
- **Rollback:** per-module revert (copies are independent).
- **Completion gate:** audit `universe_scoring_rankings` writers reduced to the
  engine + the shared primitive.
- **Principle:** 1, 8. **Milestone:** 1.
- **Status — LANDED (Phase 29E).** Canonical owner `api/universe_scoring.py` — the
  ONE operational scoring/ranking **composition & read owner** over the unchanged pure
  kernel `api/multi_horizon_engine.py` (`build_current`). It is NOT a second scoring
  engine: it calls the kernel exactly once, **deep-copies** the mtime-cached result
  before reading (cache never mutated; a consumer mutation cannot contaminate a later
  result), and normalises it into ONE frozen read contract — strategy / model /
  universe identity, a deterministic *content-level* `input_contract_hash` (over the
  owned input fingerprints + the frozen model / construction / weights / eligibility
  contract; NEVER over `evaluated_at`, object identity, absolute path or file mtime),
  reconciled counts (`scored = fundamental_eligible + excluded` and
  `scored = combined_eligible + combined_excluded`), the full deterministic combined
  ranking (score-desc / ticker-asc tie-break inherited from the kernel), TOP25 / TOP50
  (the deterministic sector-capped equal-weight books — TOP25 is **not** asserted as a
  strict subset of TOP50 because the per-sector cap is looser at 50; the actual
  relationship is exposed as `top25_subset_of_top50`), exclusions with retained
  reasons, an explicit universe identity (**not** labelled strict S&P 500; unknown
  membership explicit) and a deterministic cross-consumer consistency validator
  (`CONSISTENT` / `INCONSISTENT` / `UNKNOWN`, every violation naming both owners and
  both values). New surface: `GET /v1/research/universe-scoring` (authenticated,
  GET-only, read-only) rendered by ONE UI loader `loadUniverseScoring()` that computes
  no score / rank / exclusion / universe / date. **Consumers migrated:**
  `daily_research_cycle` scoring adapter delegates to the canonical owner (records the
  canonical input-contract hash; the run-level date-based idempotency hash is
  unchanged); `multi_horizon_platform.load_current_scores` is a **compatibility
  wrapper** over the owner (all legacy per-security fields preserved) behind the
  retained `GET /v1/research/current-alpha-scores`; the operational primary
  model/book identity re-exports from the owner in `alpha_target` and
  `forward_prediction_skill` (one source of truth). No operational `api/*.py` module
  defines the kernel's combined-score mathematics (`compute_scores` / `_percentiles` /
  `compute_combined`) — the Phase-13 `current_alpha_book` book **construction** is a
  separate frozen lineage, not combined-score duplication. Static guard
  `check_universe_scoring_ownership` enforces the kernel + owner, delegation, no second
  scoring engine, no duplicate operational scoring, the DRC delegation, the compat
  wrapper, the GET-only canonical route, ONE UI loader with no UI computation, and
  disabled automatic promotion; inventory drift = 0. No model promotion / recalibration;
  the champion (`composite_sn`) is never replaced. **Not begun:** Slice 5 (portfolio
  state), Milestone 2 (opportunity-cost engine); cadence remains disabled.

## Slice 5 — Canonical portfolio state (one NAV)

- **Objective:** one NAV authority; the UI renders NAV/holdings from one payload.
- **Existing modules:** `portfolio_valuation` (DB book), `paper_trading_desk.book_nav`
  (ledger book), `engine/portfolio` (`cached_total_value`),
  `portfolio_terminal._collect_positions`, three UI NAV render paths.
- **Target owner:** `portfolio_state` — `paper_trading_desk.book_nav` for the live
  book, `portfolio_valuation` explicitly scoped to the legacy DB archive.
- **Files likely affected:** UI NAV/holdings renderers; `portfolio_terminal`
  (drop the uncalled re-mark); `engine/portfolio` cache retirement with the
  legacy DB stack.
- **Dependencies:** Slices 1, 3.
- **Migration method:** add one reconciliation read model; point every UI surface
  at it; label the DB valuation as the legacy archive.
- **Compatibility bridge:** the DB valuation endpoint remains for the archive view.
- **Tests required:** NAV reconciliation (±$0.01) across all render surfaces.
- **Rollback:** UI reverts to per-payload rendering.
- **Completion gate:** audit `portfolio_nav_valuation` writers = 1 live authority
  + 1 explicitly-scoped legacy archive.
- **Principle:** 1, 6. **Milestone:** 1.
- **Status — LANDED (Phase 29F).** Canonical owner `api/portfolio_state.py` — the ONE
  authoritative, READ-ONLY composition owner of the complete operational
  portfolio-state of the active Alpha Paper Book. It **composes** the existing
  authoritative read models and recomputes NO business logic: the active book +
  capital + positions + orders + target reference → `operational_book.load_operational_book`
  (NAV/cash/holdings each produced exactly once by the desk-ledger replay
  `paper_trading_desk.book_nav`); the operational dates + active-book selection +
  freshness consistency → `data_freshness.load_data_freshness` (Slice 1); the
  cumulative performance (cumulative return / benchmark value+date / excess vs
  benchmark / drawdown) → `paper_trading_desk.load_performance`; the portfolio
  assessment (date / outcome / proposed-change count / target state) →
  `daily_action_gate.load_daily_action_gate`; the forward-evidence reference →
  `forward_prediction_skill.load_prediction_skill`. New surface:
  `GET /v1/operations/portfolio-state` (authenticated, GET-only, read-only) rendered
  by ONE UI loader `loadPortfolioState()` that is the SOLE writer of the operational
  VALUATION nodes across the Command Center card, the Portfolio header + performance
  KPIs + active holdings table, the right panel, the Daily-Plan summary and the
  Portfolio-Manager active-book summary (canonical nodes STAMPED `data-ps-owned`; the
  shared `_obSet` setter and `renderPmStatusbar` hard-refuse them, so the visible value
  is independent of async completion order). The UI computes NO NAV, aggregates NO
  totals, selects NO active book, derives NO valuation date and counts NO pending
  orders. **Active-book selection** picks the active Alpha Paper Book #1 through the
  authoritative policy and NEVER the dormant legacy DB book
  (`portfolio_valuation` — `legacy_paper_portfolio`, dated 2026-07-20), which is
  reported only as an explicitly ignored archive; this repaired the Portfolio-Manager
  status bar, which had been showing the dormant legacy book ($9,999.52 / 2026-07-20)
  instead of the active book ($100,327.99 / 2026-08-05 / 25 holdings). **Consistency
  engine:** 12 read-only cross-source checks (valuation-vs-desk-mark, valuation-vs-close,
  benchmark-vs-valuation, holdings-count-vs-rows, NAV reconciliation ±$0.01,
  invested-vs-positions, pending-order count, fill count, target-membership count,
  target-date-vs-owner, assessment-vs-eligible, active-book identity) returning
  `CONSISTENT` / `DEGRADED` / `INCONSISTENT` / `UNAVAILABLE` with exact reason codes;
  it never silently repairs and the endpoint stays available in a degraded state.
  Deterministic: a stable `state_hash` + per-source `source_hashes` (with
  `generated_at` explicitly excluded). It is a READ model — it is **never** a writer,
  runs no Daily Close / research refresh / reassessment, calls no provider or
  prediction service, promotes no model, and creates no order / signal / decision /
  fill. The preliminary reassessment proposal it references (the August 17 proposed
  changes) is **review-only and unapproved** and labelled
  `PRELIMINARY PROPOSAL — OPPORTUNITY-COST ENGINE NOT YET IMPLEMENTED` — the Holding
  Opportunity-Cost engine (Slice 6) and the Reallocation Proposal engine (Slice 7) are
  not implemented yet; confirmation / order creation is not permitted. Static guard
  `check_portfolio_state_ownership` enforces the sole owner, delegation, no second
  owner, no writer, the dormant-legacy rejection, the GET-only route, ONE UI loader +
  renderer with no UI NAV/total/active-book/valuation computation; inventory drift = 0.
  **Not begun:** Slice 6 (Holding Opportunity-Cost engine, Milestone 2), Slice 7
  (Reallocation Proposal engine), Slice 8 (Persistent Alpha Research Agent,
  Milestone 4); cadence remains disabled.

## Slice 6 — Holding opportunity-cost engine

- **Objective:** per-holding HOLD/REDUCE/EXIT/REPLACE/ADD with the full measure
  set.
- **Existing modules:** `portfolio_manager` (seed), `multi_horizon_engine`
  (ranks), `daily_action_gate` (checks), `forward_evidence` (deterioration).
- **Target owner:** `opportunity_cost_engine` (new).
- **Files likely affected:** new module + `portfolio_manager` wiring + one read
  endpoint + a UI panel.
- **Dependencies:** Slices 4, 5.
- **Migration method:** additive; read-only; no holdings mutation.
- **Tests required:** per-measure unit tests; recommendation determinism.
- **Rollback:** feature-flag the panel/endpoint off.
- **Completion gate:** every holding gets a recommendation with evidence.
- **Principle:** 1, 3. **Milestone:** 2.
- **Status — LANDED (Phase 29G).** Two canonical owners: the pure deterministic
  calculation kernel `engine/holding_opportunity_cost.py` (the SOLE holding comparison
  + decision engine) and the composition/validation/immutable-artifact/read owner
  `api/holding_opportunity_cost.py` (the SOLE API owner). For every holding the kernel
  measures — from ONE immutable point-in-time assessment-input contract — current /
  previous rank + rank change (previous rank honestly UNAVAILABLE when no prior
  artifact exists; no owner stored prior ranks before this slice), signal strength +
  deterioration, trailing returns (5/20/60 closes), realized volatility (20/60,
  annualized), max drawdown (60), covariance risk contribution
  (`w_i (Σw)_i / portfolio variance` from date-aligned owned daily returns, with an
  explicit lookback / min-observation / missing-data policy and a variance floor),
  concentration, liquidity (owned trailing median dollar volume → estimated days to
  liquidate; UNAVAILABLE when owned volume is absent — never invented), the strongest
  eligible NON-ALLOCATED replacement candidate, the reused desk switching cost, and
  gross / risk-adjusted / net improvement (a SCORE comparison — `expected_return_delta`
  is always null/UNAVAILABLE because no validated forecast model exists), producing a
  recommendation from the frozen vocabulary **HOLD / REDUCE / EXIT / REPLACE / ADD**
  plus non-held ADD candidates. It **reuses** (never forks) the
  `api.multi_horizon_engine` construction constants (entry rank / exit buffer / sector
  cap / name cap / liquidity floor) and the `api.paper_trading_desk.COST_RATE_PER_SIDE`
  transaction-cost model, injected through one explicit versioned decision policy
  (`hoc_decision_policy.v1`) folded into a deterministic `assessment_hash`
  (`generated_at` excluded). New owned input: `api/price_panel.py` now exposes owned
  point-in-time trailing dollar volume (`dollar_vol` + `trailing_median_dollar_volume`).
  New surface: `GET /v1/operations/holding-opportunity-cost` (authenticated, GET-only,
  read-only, readable in DEGRADED / BLOCKED / NOT_RUN) rendered by ONE UI loader
  `loadHoldingOpportunityCost()` (single-flight; no JS recommendation / rank / risk /
  cost / total computation). The **sole normal execution path is the Daily Research
  Cycle** — a new `ASSESS_HOLDING_OPPORTUNITY_COST` step runs after canonical universe
  scoring and before the portfolio-assessment step, persists an immutable artifact
  under a research / decision-evidence root (`PAPER_TRADER_HOC_DIR`; atomic, indexed,
  idempotent identical rerun, conflicting artifact rejected, interrupted-write
  recoverable — never the operational ledger), and feeds its summary into the Daily
  Action Gate; there is deliberately NO separate manual execution endpoint. The Daily
  Action Gate now delegates to the opportunity-cost summary (new `opportunity_cost_*`
  fields) and the review-only banner reads **HOLDING OPPORTUNITY-COST REVIEW —
  REALLOCATION ENGINE NOT YET IMPLEMENTED**. Static guard
  `check_holding_opportunity_cost_ownership` enforces the sole calculation + API
  owners, delegation, the GET-only route, no separate manual execution endpoint, no
  second recommendation engine, no order / fill / target-weight / NAV / universe-score
  in either owner, one UI loader with no computation, the gate delegation, and that
  Slice 7 / Slice 8 remain future; inventory drift = 0. Review-only, preview-first,
  paper-only: confirms no target, creates no order / fill, changes no holding / cash /
  NAV, promotes no model, enables no cadence. **Not begun:** Slice 7 (Reallocation
  Proposal engine, Milestone 3), Slice 8 (Persistent Alpha Research Agent,
  Milestone 4); cadence remains disabled.

## Slice 7 — Portfolio reallocation proposal engine

- **Objective:** a complete paper-only target with full before/after explanation.
- **Existing modules:** `alpha_target`, `operational_book`, `current_alpha_book`
  (two "book" concepts to unify), `engine/risk` + cap families.
- **Target owner:** `portfolio_proposal` + `risk_cost_service`.
- **Files likely affected:** proposal composer + risk/cost service + UI proposal
  view.
- **Dependencies:** Slice 6.
- **Migration method:** unify the operational-snapshot and frozen-champion book
  concepts behind one proposal payload; manual review remains mandatory.
- **Tests required:** turnover/cost/vol/drawdown/concentration before-after tests.
- **Rollback:** proposal view feature-flagged.
- **Completion gate:** one proposal payload; no order is ever created.
- **Principle:** 1, 3. **Milestone:** 3.

## Slice 8 — Persistent Alpha Research Agent (unify registries)

- **Objective:** one model registry + champion/challenger governance; monitor
  freshness/degradation/experiments; never auto-promote.
- **Existing modules:** `alpha_registry`, `multi_horizon_registry`, `alpha_factory`,
  `price_alpha_factory`, `current_alpha_tournament_sync`, dead phase18 wire,
  `alpha_agent/*`.
- **Target owner:** `model_registry` (unified).
- **Files likely affected:** merge the two factory registries; remove the dead
  `run_current_alpha_tournament_refresh` import; fold the factory cores.
- **Dependencies:** Slice 4.
- **Migration method:** shared factory core; one registry schema; retire the
  frozen Phase 13/16/17/18 `current_alpha_*` viewers as their questions close.
- **Tests required:** governance gate tests; "no auto-promotion" invariant.
- **Rollback:** keep the second registry until parity proven.
- **Completion gate:** one registry; `champion_challenger_registry` writers = 1.
- **Principle:** 7, 8. **Milestone:** 4.

## Slice 9 — Paid-data integration (Data Expansion)

- **Objective:** integrate economically distinct datasets only when PIT/coverage/
  cost gates pass; historical analyst revisions live here.
- **Existing modules:** `alpha_agent/analyst_revisions` (Stage 13A, `TRIAL_NOT_STARTED`),
  ingestion collectors.
- **Target owner:** `feature_service` + `model_registry` (evidence-gated).
- **Dependencies:** Slice 8.
- **Migration method:** the existing adequacy/power/purchase gate stays; data
  enters only through the PIT contract.
- **Tests required:** the Stage 13A fixture self-test + PIT invariants.
- **Rollback:** data source disabled by config.
- **Completion gate:** a dataset passes all seven acquisition criteria before use.
- **Principle:** 4. **Milestone:** 5 (supporting track, not the main objective).

## Slice 10 — Intraday / near-real-time evolution

- **Objective:** add intraday data, incremental features, event-driven rescoring,
  turnover/latency controls — only after the daily system is reliable.
- **Existing modules:** `market_data_service`, `research_cycle`, `market_session`.
- **Dependencies:** Slices 1–5 stable.
- **Migration method:** additive cadence layer; the daily path remains the
  fallback.
- **Tests required:** latency + data-quality safeguards; no-lookahead invariants.
- **Rollback:** disable the intraday cadence; revert to daily.
- **Completion gate:** intraday rescoring runs without violating PIT.
- **Principle:** 2, 4. **Milestone:** 6 (deferred).

## Slice 11 — Controlled execution (quarantine first)

- **Objective:** the deferred execution track; first, quarantine the legacy paper
  "Create Orders" surface and the DB order-execution stack out of the operational
  path.
- **Existing modules:** `engine/reconciler`/`risk`/`portfolio`/`strategy`, the
  `/v1/review/create-orders` + fill/cancel surface, `db/models` execution tables.
- **Target owner:** `execution` (empty until Milestone 7).
- **Dependencies:** all prior slices stable.
- **Migration method:** confirm no live route drives the DB fill cycle; move the
  legacy Daily-Plan/execution routes behind an explicit disabled/quarantined
  boundary; only then design approved paper execution.
- **Tests required:** "no live order / no automation" invariants; the quarantine
  is provably inert.
- **Rollback:** the quarantine is reversible; nothing is deleted until confirmed.
- **Completion gate:** the operational path contains no execution code;
  `execution` writers = 0 until explicitly authorized.
- **Principle:** 3, 7. **Milestone:** 7 (deferred, explicit authorization required).

---

## Cross-cutting (runs alongside, not a phase of its own)

- **Ledger/store service:** replace the ~13 private stores + ~11 hand-copied
  atomic writers with one store service modeled on `db/session.py`; reduce the
  172 `alias._private` cross-module accesses (worst `alpha_book`→`desk` ×80).
  Sequenced opportunistically inside Slices 3, 5, 8.
- **Monolith reduction:** split `api/app.py` into domain routers and extract UI
  view logic as the owning contexts stabilize — never as a standalone rewrite.
- **Test-contract migration:** convert the 648 implementation-coupled
  `.count(`/`.index(` assertions to behavioral contracts as each slice lands.
