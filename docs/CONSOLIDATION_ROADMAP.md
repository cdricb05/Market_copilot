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
- **Live-acceptance readiness / operator-workflow & UI hard cutover (Phase 29G.1):**
  before the first post-close production HOC assessment, the operator path was hardened
  without running a real cycle. SERVICE readiness (`/v1/ready`, DB probe, exact 503
  reason) and WORKFLOW readiness (`api.workflow_state`) are now DISTINCT and shown
  separately in the header — a `WAITING_FOR_SESSION_CLOSE` workflow never means the
  service is unhealthy. The obsolete "Slice 3 — not yet implemented" reassessment control
  is removed (the reassessment runs inside the Daily Research Cycle, the sole execution
  path); the legacy rank-membership comparison is reclassified **LEGACY
  MEMBERSHIP-COMPARISON SUMMARY (compatibility-only)** and the Holding Opportunity-Cost
  review is the PRIMARY decision card, rendered in every state (NOT_RUN … completed) with
  no fabricated counts before an artifact exists; the DRC UI surfaces the
  `ASSESS_HOLDING_OPPORTUNITY_COST` step lifecycle. Guarded by
  `check_slice6_live_acceptance_ownership`. First-live-cycle acceptance procedure: wait
  for the session close → run the Daily Research Cycle → review the Holding
  Opportunity-Cost assessment → run the Daily Close. No target / rebalance / order
  authority is added.
- **Residual hard cutover + first-live operator gates (Phase 29G.2):** Phase 29G.1 fixed
  the FIRST compatibility card (Daily Close) but a SECOND renderer — the Daily Action Gate
  card on the Command Center, Daily Workflow and Portfolio Manager — still presented the
  legacy comparison as a primary decision ("LATEST PORTFOLIO ASSESSMENT", "PROPOSAL READY",
  "PORTFOLIO CHANGES PROPOSED", "Review Proposed Changes", the 17-name Add/Remove list).
  The residual cutover makes the **Holding Opportunity-Cost Review** the ONE primary
  portfolio-decision card on all three surfaces (canonical operator state
  `HOLDING_OPPORTUNITY_COST_NOT_RUN` before the first artifact, with "NONE YET" and no
  fabricated counts), and demotes the legacy rank-membership comparison to a COLLAPSED,
  read-only **LEGACY MEMBERSHIP-COMPARISON SUMMARY — COMPATIBILITY ONLY** (explicitly not a
  proposal, creates no orders). The gate result carries an explicit classification
  (`compatibility_only` / `decision_authority=NONE` / `execution_available=false` /
  `canonical_decision_owner` / `legacy_membership_comparison`); the raw gate vocabulary is
  preserved for historical consumers only. Two read-only GET-only operator scripts
  (`pre_drc_readiness.ps1`, `post_drc_acceptance.ps1`) gate the first live cycle. Guarded
  by `check_slice6_residual_cutover_ownership`. The Daily Research Cycle remains the sole
  HOC execution path; no order or target authority is added; cadence remains disabled.

- **First-live DRC terminal-manifest persistence + pre-close consistency (Phase 29G.3):**
  the first real Daily Research Cycle (2026-08-06) persisted a COMPLETE manifest and an
  immutable Holding Opportunity-Cost artifact, but `GET .../daily-research-cycle/status`
  returned `NOT_STARTED` because the reader gated reuse on the raw `input_contract_hash`,
  which the cycle mutates by refreshing its own fast daily inputs (a status/downstream
  split-brain). Fix: (a) a terminal `COMPLETE` now requires a validated + read-back manifest
  (never COMPLETE on an unverified persist — `MANIFEST_PERSISTENCE_UNVERIFIED` /
  `MANIFEST_CONTRACT_INCOMPLETE`); (b) the status reader REFLECTS a persisted terminal
  manifest verbatim (never NOT_STARTED) and returns `INCONSISTENT` /
  `TERMINAL_DOWNSTREAM_ARTIFACTS_WITHOUT_DRC_MANIFEST` for downstream artifacts without a
  manifest; (c) a new `session_contract_hash` keys safe idempotent recovery so a same-date
  rerun REUSES the immutable outputs (no duplicate evidence / HOC, no order / fill / target
  confirmation / ledger mutation) while a genuinely different slow-input contract for the
  same date is still refused; (d) `portfolio_state` classifies the expected one-session
  pre-close valuation gap as `PENDING_DAILY_CLOSE` / `READY_WITH_PENDING_CLOSE` (genuine
  gaps stay `INCONSISTENT`); (e) `workflow_state` treats a completed cycle + current HOC as a
  satisfied reassessment (`READY_FOR_DAILY_CLOSE`, no separate reassessment control) and
  preserves the honest HOC `DEGRADED` gaps. Recovery is normal idempotent execution (no
  "mark complete" endpoint). Guarded by `check_drc_manifest_recovery`. No evidence is
  fabricated; no order/target authority is added; cadence remains disabled.

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
- **Status — LANDED (Phase 29H).** Two canonical owners: the pure deterministic
  calculation kernel `engine/reallocation_proposal.py` (the SOLE allocation-math owner)
  and the composition / validation / immutable-artifact / read owner
  `api/reallocation_proposal.py` (the SOLE API owner). From the canonical CURRENT
  portfolio state (`api.portfolio_state`) and the Slice 6 Holding Opportunity-Cost
  assessment (`api.holding_opportunity_cost`) the kernel builds ONE coherent paper-only
  proposed target portfolio: HOLD/REDUCE retained, EXIT zeroed, each REPLACE swapped to a
  traceable eligible non-held candidate that clears the net-of-cost hurdle (unmatched
  REPLACEs are retained, never a silent exit-to-cash), and ADD candidates filling the
  remaining slots by rank — equal-weight `min(1/N, name_cap)` with the residual as cash and
  a sector count-cap `int(sector_cap · N)`. It **reuses** (never forks) the
  `api.multi_horizon_engine` construction constants (book size N / name cap / sector cap /
  liquidity floor) and the `api.paper_trading_desk.COST_RATE_PER_SIDE` transaction-cost
  model via one versioned allocation policy (`reallocation_allocation_policy.v1`), and
  reuses the Slice-6 covariance primitive for before/after portfolio volatility. It emits
  per-ticker actions (RETAIN/INCREASE/REDUCE/EXIT/ADD/REPLACE_OUT/REPLACE_IN), turnover,
  transaction + switching cost, before/after portfolio SCORE (expected return is NEVER
  fabricated — no validated forecast model exists, so it is null / `NOT_CALIBRATED` with an
  explicit `EXPECTED_RETURN_NOT_CALIBRATED` gap), concentration and portfolio volatility
  before/after, and hard-constraint validation, producing a state from the frozen
  vocabulary **READY / DEGRADED / BLOCKED / NO_ACTIVE_BOOK** (read layer adds NOT_RUN /
  UNAVAILABLE). A DEGRADED HOC input does not BLOCK Slice 7; the exact source gaps are
  carried forward and classified by the analytic they affect. New surface:
  `GET /v1/operations/reallocation-proposal` (authenticated, GET-only, read-only, NOT_RUN
  before a proposal exists) rendered by ONE UI loader `loadReallocationProposal()` (a
  first-class "REALLOCATION PROPOSAL — MANUAL REVIEW REQUIRED" Portfolio-Manager card plus
  concise Command Center / Daily Workflow status; no JS allocation math). The **sole
  execution path is the Daily Research Cycle** — a new `BUILD_REALLOCATION_PROPOSAL` step
  runs after `ASSESS_HOLDING_OPPORTUNITY_COST` and before the portfolio-assessment step,
  persists an immutable artifact under a research root (`PAPER_TRADER_REALLOC_DIR`; atomic,
  indexed, idempotent identical rerun; a proposal from a DIFFERENT source HOC assessment
  hash for the same date SUPERSEDES — never silently reuses — the stale proposal, keeping
  every artifact immutable). The Daily Action Gate delegates to `load_proposal_summary`;
  `api.workflow_state` exposes the proposal state as an INFORMATIONAL review action (a
  separate operator-state vocabulary that never enters `OVERALL_STATES` and never gates the
  Daily Close, which stays independent). Static guard
  `check_reallocation_proposal_ownership` enforces the sole calculation + API owners,
  delegation, the GET-only route, no create/apply/confirm-target/rebalance/order route, the
  DRC as the sole execution path, no order / fill / target / NAV / holdings mutation, kernel
  purity, one UI loader with no allocation computation, immutable/idempotent artifacts, and
  that no second/unified model registry exists; inventory drift = 0. Review-only,
  preview-first, paper-only, manual review mandatory: confirms no operational or alpha
  target, creates no order / fill, changes no holding / cash / NAV, promotes no model,
  enables no cadence. Slice 8 (Persistent Alpha Research Agent, Milestone 4) has since
  LANDED (Phase 29I, below); cadence remains disabled.

## Slice 8 — Persistent Alpha Research Agent (monitoring & governance)

- **Objective:** continuously evaluate whether the research/model stack remains
  trustworthy; monitor freshness/degradation/challenger evidence; recommend bounded
  research; never auto-promote, auto-recalibrate or auto-retrain.
- **Existing modules:** `universe_scoring` (champion identity), `forward_prediction_skill`
  (rank IC / decile spread), `paper_trading_desk` + `forward_evidence` (realized
  performance), `current_alpha_tournament` (challenger), `current_alpha_decision_gate`
  (thresholds), Slice-6 `holding_opportunity_cost` + Slice-7 `reallocation_proposal`
  histories, `alpha_registry` / `alpha_factory` / `price_alpha_factory` (existing
  registries — READ, never forked), `alpha_agent/*`.
- **Target owner:** `engine/research_agent` (pure evaluation kernel) +
  `api/research_agent` (composition/persistence/read). The Research Agent READS the
  existing champion/challenger registries; it does NOT create a second/unified
  `model_registry` and never moves champion-promotion authority (registry unification is a
  deferred, separate consolidation, not part of Milestone 4).
- **Files likely affected:** two new owners; one `RUN_RESEARCH_AGENT` DRC step; one GET
  route; one UI panel + loader.
- **Dependencies:** Slices 4, 5, 6, 7.
- **Migration method:** monitoring/governance layer over the existing evidence owners
  behind tests; no metric is re-derived; the sole scheduled path is the Daily Research
  Cycle; a generated bounded-experiment specification is sufficient (no automatic
  experiment execution yet).
- **Tests required:** evidence-sufficiency gates; "insufficient evidence prevents premature
  recalibration"; "no auto-promotion / no auto-retraining" invariants; challenger
  governance; deterministic opportunity ranking; persistence idempotency/supersede; DRC
  ordering; strict ownership guard.
- **Rollback:** the DRC step and GET route are additive and non-blocking (research
  recommendation != operational action); the panel is section-gated.
- **Completion gate:** two canonical owners; one GET route; one UI loader; no
  promote/recalibrate/retrain/apply route; `check_research_agent_ownership` green;
  inventory drift = 0.
- **Principle:** 3, 4, 7, 8. **Milestone:** 4.
- **Status — LANDED (Phase 29I).** Two canonical owners: the pure deterministic evaluation
  kernel `engine/research_agent.py` (the SOLE research-state calculation owner) and the
  composition / persistence / read owner `api/research_agent.py` (the SOLE API owner). From
  an immutable point-in-time research-evidence contract — champion/challenger identity
  (`api.universe_scoring` / `api.current_alpha_tournament`), matured TRUE_FORWARD rank IC /
  decile spread / observation counts (`api.forward_prediction_skill`), realized
  benchmark-relative return / drawdown / turnover / cost (`api.paper_trading_desk` +
  `api.forward_evidence`), the Slice-6 HOC and Slice-7 reallocation immutable histories, and
  regime evidence — the kernel evaluates **evidence sufficiency** (a short negative live P&L
  run yields INSUFFICIENT_EVIDENCE / WATCH, never a premature RECALIBRATION_DUE), **explained
  champion-health components** with reason codes (never one opaque score), **model-degradation
  categories** (PERFORMANCE_WEAKNESS / SIGNAL_DEGRADATION / RANKING_DEGRADATION / REGIME_DRIFT
  / SECTOR_INSTABILITY / TURNOVER_INEFFICIENCY / PORTFOLIO_STALENESS / DATA_QUALITY_DEGRADATION
  / INSUFFICIENT_EVIDENCE), **HOC + reallocation diagnostic feedback** (distinguishing
  portfolio staleness / governance latency from model weakness), **challenger classification**
  (NOT_EVALUATED / INSUFFICIENT_EVIDENCE / UNDERPERFORMING / COMPETITIVE / PROMISING /
  SUPERIOR_CANDIDATE — never PROMOTED), a **controlled recalibration recommendation** gated on
  evidence, and a deterministic ranked **queue of bounded research opportunities** each with a
  hypothesis, supporting evidence, gaps, priority and a fully-specified SHADOW-only experiment.
  The evidence-sufficiency and health thresholds are one versioned policy
  (`research_agent_policy.v1`); the decision-gate `MIN_FORWARD_OBS` is injected so no
  governance threshold is silently forked. It **reuses** (never re-derives) every metric from
  its existing owner and does NOT create a second/unified model registry. New surface:
  `GET /v1/research/research-agent` (authenticated, GET-only, read-only, NOT_RUN before an
  assessment exists) rendered by ONE UI loader `loadResearchAgent()` (a first-class Research
  Agent panel in the Research & Audit workspace; no JS research math). The **sole execution
  path is the Daily Research Cycle** — a new `RUN_RESEARCH_AGENT` step runs after
  `BUILD_REALLOCATION_PROPOSAL` and before the portfolio-assessment step, persists an
  immutable artifact under a research root (`PAPER_TRADER_RESEARCH_AGENT_DIR`; atomic, indexed,
  idempotent identical rerun; a DIFFERENT evidence hash for the same date SUPERSEDES — never
  silently reuses — the stale assessment). The research-agent step is non-blocking (research
  recommendation != operational action; the Daily Close stays independent). Static guard
  `check_research_agent_ownership` enforces the sole calculation + API owners, delegation, the
  GET-only route, no promote/recalibrate/retrain/apply route, the DRC as the sole execution
  path, no champion-pointer / order / fill / target / NAV / holdings mutation, kernel purity,
  one UI loader with no research computation, immutable/idempotent artifacts, no second/unified
  model registry, and that no paid-data registry fork exists (Slice 9 landed as a purchase
  gate, not a registry); inventory drift = 0. Research governance only, manual approval
  mandatory: promotes / recalibrates / retrains / replaces no model, writes no champion
  pointer, confirms no target, creates no order / fill, changes no holding / cash / NAV,
  executes no experiment, enables no cadence. **Next:** Slice 10 (Intraday / near-real-time,
  Milestone 6); cadence remains disabled.

## Slice 9 — Data Expansion / Purchase-Gate

- **Status — LANDED (Phase 29J).** Two canonical owners: the pure deterministic evaluation
  kernel `engine/data_expansion_gate.py` (the SOLE dataset purchase/integration-gate
  calculation owner; `evaluate_dataset`) and `api/data_expansion.py` (the SOLE dataset-catalog
  / composition / persistence / read owner). Given an external-dataset candidate's metadata,
  the intended research requirements and the MEASURED research evidence produced by the
  existing experiment/evidence owners, it decides — across sixteen explicit dimensions
  (point-in-time integrity, historical depth, inactive/delisted coverage, universe breadth,
  effective sample, revision history, freshness, identifier quality, survivorship risk,
  restatement/backfill, licensing, cost, incremental information, measured lift, implementation
  complexity, operational reliability) — whether the dataset is worth acquiring / integrating,
  separating hard blockers from soft visible gaps and returning ONE explicit recommendation
  (`REJECT` / `INSUFFICIENT_EVIDENCE` / `RESEARCH_ONLY` / `CANDIDATE` / `PURCHASE_RECOMMENDED` /
  `INTEGRATION_RECOMMENDED`). It never fabricates a score when required data is absent, and
  never recommends a purchase on in-sample-only evidence or on current/live P&L. It REUSES —
  never forks — the existing provider/provenance owner (`alpha_agent/source_contracts`), the
  freshness owner (`api/data_freshness`), the evidence gates (`alpha_agent/experiment_contracts`),
  the Stage 13A analyst-revisions framework (`alpha_agent/analyst_revisions`) for the
  analyst-revisions candidate, and the Slice 8 DATA research opportunities
  (`engine/research_agent`). Read-only endpoints `GET /v1/research/data-expansion` and
  `GET /v1/research/data-expansion/{dataset_id}` return the catalog + latest immutable
  evaluations (`NOT_RUN` before an evaluation exists; no GET recomputes a research study);
  there is deliberately NO purchase / subscribe / activate-provider / integrate /
  enable-paid-data endpoint — the gate has no purchasing authority. Immutable idempotent
  artifacts under `PAPER_TRADER_DATA_EXPANSION_DIR` (different dataset metadata / evidence /
  policy SUPERSEDES — never silently reuses — the stale evaluation). **Cadence is DISABLED**
  (`CADENCE_ENABLED = False`): a full purchase-gate evaluation is never a daily job — it runs
  only when a candidate is added, candidate metadata changes, enough new research evidence
  exists, a formal review checkpoint is due, or an operator explicitly requests a
  re-evaluation; the Daily Research Cycle may only READ the latest status. Static guard
  `check_data_expansion_ownership` enforces the sole calculation + API owners, the reuse of
  the existing provider/data/evidence owners (never forked), the two GET-only routes, no
  purchase/subscribe/activate/integrate route, no secret/credential ownership, one UI loader
  with no gate computation, immutable/idempotent artifacts, cadence disabled, that the gate is
  never a DRC daily job, and that Slice 10 (Intraday) remains future; inventory drift = 0. No
  paid provider is ever called during implementation/tests (fixtures only). **`PURCHASE_RECOMMENDED`
  / `INTEGRATION_RECOMMENDED` are always MANUAL APPROVAL REQUIRED and never purchasing
  authority.**
- **Objective:** integrate economically distinct datasets only when PIT/coverage/
  cost gates pass; historical analyst revisions live here.
- **Existing modules reused (not forked):** `alpha_agent/analyst_revisions` (Stage 13A,
  `TRIAL_NOT_STARTED`), `alpha_agent/source_contracts`, `api/data_freshness`,
  `alpha_agent/experiment_contracts`, ingestion collectors.
- **Target owner:** `engine/data_expansion_gate.py` + `api/data_expansion.py` (evidence-gated).
- **Dependencies:** Slice 8.
- **Migration method:** the existing adequacy/power/purchase gate stays; data
  enters only through the PIT contract; the gate is a decision layer over existing owners.
- **Tests required:** `tests/test_slice9_data_expansion.py` (PIT/history, coverage/sample,
  incremental value, cost/licensing, the six-outcome gate, persistence/api, UI, architecture).
- **Rollback:** data source disabled by config; no dataset is ever purchased/activated.
- **Completion gate:** a dataset passes all hard acquisition gates and shows measured,
  robust, out-of-sample, cost-adjusted incremental lift before a purchase is recommended.
- **Principle:** 4. **Milestone:** 5 (supporting track, not the main objective).

## Phase 29J.1 — Operator UX Consolidation (LANDED)

- **Objective:** correct the frontend/backend imbalance that accumulated across Slices
  1–9. The backend became substantially stronger than the operator-facing information
  architecture: six architecture-centric views, NAV rendered in 7+ places, dates in 20+,
  workflow/next-action in 5–6 renderers, ~480 safety-badge spans across ~30+ strips, and
  the market-context strip dead. This is an information-architecture and interaction-design
  consolidation, not a rewrite, a cosmetic pass, or a diagnostics removal.
- **Status — LANDED (Phase 29J.1).** The primary navigation is FOUR operator-oriented
  areas — **Today / Portfolio / Research / System · Audit**. **Today** (default landing)
  is ordered MARKET CONTEXT → portfolio performance → concise system status → what changed
  → ONE dominant next action (from `workflow_state.primary_action`; the UI derives no
  priority), with diagnostics behind progressive disclosure. The **Market Context** strip is
  RESTORED against the SINGLE authoritative owner `GET /v1/market/indicators` (no new
  market-data owner, no new provider, no provider call from JS, no market math in JS); it is
  reference context only (never a signal / BUY-SELL) and shows explicit UNAVAILABLE tiles for
  series with no owned source (DXY; US rates without a FRED key) — never a fabricated number;
  there is no live regime classifier, so no regime badge is shown. **Portfolio** makes the
  Holding Opportunity-Cost review + Reallocation Proposal first-class (open by default; the
  review deep-link lands on a visible card); the legacy membership comparison and archived
  book stay compatibility-only behind disclosure. ONE persistent safety strip carries
  `PAPER ONLY · MANUAL REVIEW · AUTOMATION OFF · NO BROKER EXECUTION · NO LIVE BROKER ORDERS
  · NO MODEL PROMOTION`. Legacy/detail views (Daily Workflow, Model Target, Holdings detail)
  are demoted under an Advanced-views disclosure; every old route resolves as an alias (no
  dead links). No backend authority moved: the UI READS the canonical owners and duplicates
  no NAV/workflow/market/HOC/reallocation/research computation. Guarded by
  `check_operator_ux_consolidation_ownership` (four areas present, legacy demoted, aliases
  resolve, single authoritative market owner GET-only with no direct provider host / market
  math, one next-action renderer, safety strip present, no purchase/order/promotion route,
  cadence disabled); inventory drift = 0. No intraday, no execution, no cadence, no new
  provider, no data purchase, no model promotion, no holdings change.
- **Principle:** 6, 8. **Milestone:** supporting (operator experience across Milestones 1–5).

## Phase 29J.2 — Controlled Autonomous Research Execution Bridge (NEXT, NOT STARTED)

- **Objective (planned):** a bounded, evidence-gated bridge that lets the Slice 8 Research
  Agent's fully-specified SHADOW-only experiments be *queued for controlled execution* under
  explicit manual authorization — never auto-promotion, auto-retraining or auto-reallocation.
  It does NOT bridge Slice 8 automatically to `alpha_agent`, does NOT enable cadence, and adds
  no intraday or execution capability. Deferred until after Phase 29J.1; specified here only to
  fix the sequence (29J.1 → 29J.2 → Slice 10 → Slice 11).
- **Principle:** 3, 4, 7. **Milestone:** 4 (governance extension).

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

---

## Stage 19.3 — Operator workflow & atomic post-close consolidation (COMPLETE)

**Trigger.** The 2026-08-13 live operating path, with 29 repaired NEXT_CLOSE paper
orders SUBMITTED, exposed a control-plane problem rather than a cosmetic one: the
operator faced many simultaneously-visible controls while the authoritative workflow
said "No action required right now", and a standalone Paper Desk refresh competed with
the canonical Daily Close for the SAME post-close transition.

**Scope (consolidation of existing owners; nothing new was built).**

- `api/daily_close.py` — a newly eligible completed close now OUTRANKS passive
  pending-order monitoring; the close settles eligible NEXT_CLOSE orders through the
  EXISTING Paper Desk owner; `forward_tracking` separated from `book_active`; the
  offline operational seam honours `desk_dir`; settlement provenance recorded once.
- `api/workflow_state.py` — `MAINTENANCE_EXECUTION_KINDS` +
  `assert_primary_action_contract` (fails closed); `WAITING_FOR_OWNED_DATA` promotes
  the Daily Close; `build_operator_command` is the ONE operator-command contract.
- `api/operational_book.py` — `current_rebalance_lineage`; lineage-aware lifecycle
  classification; no standalone post-close refresh label.
- `api/rebalance_execution.py` — `build_execution_summary` (lineage-scoped four-stage
  execution summary).
- `api/daily_action_gate.py` — gate labels name the canonical close, not a competing
  standalone refresh.
- `api/ui/index.html` — the persistent Operator Command bar; ONE execution surface via
  `_wsCommandOwnsExecution()`; lineage-aware current-rebalance strip; the desk refresh
  demoted to a collapsed maintenance / recovery area.

**Explicitly NOT built:** a second order engine, mark owner, fill simulator, Daily
Close, workflow-state owner or NAV owner. No broker, no automation, no automatic
rebalance, no automatic promotion, no model recalibration, no cadence change.

**Tests.** `tests/test_stage19_3_operator_workflow_atomic_close.py` (61 tests:
precedence, settlement-through-close, failure atomicity / idempotency, rebalance
lineage, operator-command contract, rendered UX, safety) plus updates to the
superseded assertions in the 27B / 27C / 27E / 27F and operator-action-integrity
suites. Hermetic browser acceptance:
`scripts/stage19_3_ui_fixtures.py` + `scripts/stage19_3_ui_acceptance.js`
(5 scenarios x 1920x1080 and 1440x900, request-intercepted, non-GET blocked).

**Guard.** `check_operator_atomic_close_ownership` with 33 blocking invariants.

**Deferred (unchanged by this slice).**

- Slice 9 (Evidence & Attribution consolidation), Slice 10 (Model governance,
  Milestone 6) and Slice 11 (Controlled Execution, Milestone 7) remain sequenced as
  before; cadence remains disabled.
- The legacy `wf-*` per-page workflow banners are retained (deduplicated against the
  command bar) rather than removed, so their session / assessment / evidence chips and
  "Up next" context survive; folding them into the command bar is a later UX slice.
- The generic `Refresh` / `Refresh View` controls remain under SYSTEM / MAINTENANCE and
  in per-band reload buttons; they are read-only and never compete with the canonical
  action, so no further consolidation was attempted here.

---

## Stage 20 — Continuous Active Portfolio Reassessment & Proposal Cycle (LANDED)

**Slice goal.** Make the existing components behave as ONE active portfolio manager:
signal refresh → full-universe ranking → holding opportunity-cost → **portfolio
reassessment** → rebalance-necessity decision → no-change or relocation proposal → manual
review → existing Stage-19 controlled execution.

**What landed**

1. `engine/portfolio_reassessment.py` — the pure portfolio-level economic-change-gate
   kernel (identity, aggregation, cost, churn, concentration, PIT classification,
   deterministic explanations). No I/O, no clock.
2. `api/portfolio_reassessment.py` — the sole composition / persistence / history /
   attribution / read owner, plus `should_build_proposal`, `proposal_binding`,
   `proposal_is_current_for` and `execution_precedence`.
3. `api/daily_research_cycle.py` — new `REASSESS_PORTFOLIO` step between
   `ASSESS_HOLDING_OPPORTUNITY_COST` and `BUILD_REALLOCATION_PROPOSAL`; the target engine
   now runs ONLY when the gate returns `PROPOSAL_READY`, and fails closed otherwise.
4. `api/workflow_state.py` — the reassessment lane, its operator presentation, the
   Stage-19 precedence suppression, and one routing action. No second economic gate.
5. `api/app.py` — three GET-only routes.
6. `api/ui/index.html` — the ACTIVE PORTFOLIO ASSESSMENT card with exception-first
   disclosure; the browser derives nothing.
7. `scripts/audit_architecture.py` — `check_portfolio_reassessment_ownership` with 30
   blocking invariants.

**Consolidation achieved.** The "should we act?" judgement moved from an implicit
side-effect of building a target (Stage 18 reading Slice-7 action counts) to an explicit,
versioned, persisted portfolio-level decision that exists whether or not a change is made.
`CURRENT_NO_CHANGE` is now a first-class, durable outcome with evidence.

**Next slice (unchanged).** Slice 10 — intraday / near-real-time operation. Stage 20 is
deliberately session-keyed; moving to intraday requires the market-session owner, the
freshness contract and the desk mark owner to support intraday marks first.

---

## Stage 21 - outcome intelligence (LANDED)

Bounded slice, behind tests, no big-bang rewrite.

1. **0E fresh-reassessment false invalidation** - canonical economic fingerprint in
   `api/portfolio_state.py`; HOC records both fingerprints; one corporate-action resolver;
   versioned reassessment artifacts with current-state lookup. **LANDED.**
2. **0A execution lineage** - `engine/` + `api/execution_lineage.py`; chronological plan
   selection; `latest_completed_rebalance` composed into the rebalance read. **LANDED.**
3. **0B durable daily-close run status** - run identity, outcome vocabulary, retry contract
   inside the EXISTING close owner. **LANDED.**
4. **0C HOC vs portfolio-decision clarity** - `build_decision_scope` in the owner of the
   verdict; UI renders it verbatim. **LANDED.**
5. **0D environment isolation** - `api/environment_isolation.py`, fail-closed at app import,
   child-scoped acceptance opt-in. **LANDED.**
6. **Outcome evidence + policy intelligence** - `engine/` + `api/reassessment_outcomes.py`,
   GET-only routes, maturation inside the Daily Close. **LANDED.**

Deferred, deliberately:

* Nothing in Stage 21 tunes a Stage-20 threshold. When outcome evidence eventually reaches
  `POLICY_REVIEW_CANDIDATE` with a horizon-aligned sample, the review is a separate,
  human-gated slice.
* Outcome evidence for sessions before Stage 20 first ran is a permanent, documented gap.
  Reconstructing it would be fabricated evidence and is explicitly out of scope forever.

---

## Operator workflow - ONE backend restart / smoke owner (LANDED)

Bounded slice, behind tests. Three repository files and two docs.

1. **`scripts/restart_paper_trader_backend.ps1`** - the single owner of process stop/start,
   port handling, canonical `/v1/health` + `/v1/ready` polling, authentication for the live
   read, startup diagnostics, and production store-root validation (delegated to
   `api/environment_isolation.py`, not copied). **LANDED.**
2. **`scripts/audit_architecture.py::check_backend_restart_ownership`** - eleven blocking
   invariants; the load-bearing one checks every probed `/v1` path against the parsed route
   table. Runs over repository AND `--handoff-dir` PowerShell. **LANDED.**
3. **`tests/test_canonical_backend_restart.py`** - static contract plus three real
   executions against a hermetic stub backend on a throwaway port. **LANDED.**

Deferred, deliberately:

* The hermetic acceptance harness (`ui_acceptance.ps1`) keeps its own process/port handling.
  It launches the acceptance server, not the application, on a port that explicitly refuses
  to be 8001, and it is governed by its own `acceptance_scenario_ownership` invariants.
  Folding it into the restart owner would couple two workflows that fail for different
  reasons.
* The handoff `_common.ps1` keeps `Assert-ProductionStoreRoots`. It answers "is the
  operator's shell clean?", which is a different question from "may this process serve
  production?" - see the decision record.

## Stage 22 — Normal-cycle reliability & decision readiness (LANDED)

Bounded slice, behind tests. Two new pure kernels, four touched owners, one UI surface,
one guard, one regression suite.

1. **`engine/normal_cycle.py`** — the PURE canonical cycle: five ordered stages, the
   per-stage gate every surface obeys, the enforced single-mutation invariant, and the four
   operator answers (now / do / why / after). No IO, no clock, no `api.*` import. **LANDED.**
2. **`engine/data_gap_taxonomy.py`** — the PURE gap taxonomy: severity is a property of the
   gap; an unknown code is BLOCKING; no missing value is ever substituted. **LANDED.**
3. **`api/workflow_state.py`** — projects the decided state onto the cycle; adds the close-
   precedence rule (P3.7), the post-close research requirement (P4.5), the stale-evidence
   classification and the one fail-closed binding verdict. Still the ONE combined-
   interpretation owner; no second state engine. **LANDED.**
4. **`api/holding_opportunity_cost.py` / `api/reallocation_proposal.py` /
   `api/daily_action_gate.py`** — carry the taxonomy and the binding fields through the ONE
   shared gate path, and take explicit store seams so a hermetic caller can never reach a
   production root. **LANDED.**
5. **`api/ui/index.html`** — the existing operator command bar answers all four questions and
   renders the cycle strip verbatim; the reassessment card obeys the backend evidence
   hierarchy; the opportunity-cost surface renders every gap field; the right rail defers to
   the one execution surface. No new dashboard, no new route. **LANDED.**
6. **`scripts/stage20_ui_fixtures.py`** — three normal-cycle scenarios (7 pre-close, 8 close
   due, 9 post-close research due) from the SAME single scenario owner, plus the cross-panel
   verdict over cycle stage, open gates, evidence class and mutation count. **LANDED.**
7. **`scripts/audit_architecture.py::check_normal_cycle_ownership`** — 21 blocking
   invariants. **LANDED.**
8. **`tests/test_stage22_normal_cycle.py`** — the regression. **LANDED.**

Deferred, deliberately:

* **The legacy DB review workflow's order-ticket controls** (`/v1/review/create-orders` and
  its panel) are untouched. They predate the operational book and are not part of the normal
  cycle; folding them in would mean changing a workflow this stage never exercised.
* **Merging the Daily Close and the Daily Research Cycle** into one button. They remain two
  distinct operations with two distinct owners and two confirmation tokens — Stage 22 makes
  the TRANSITION between them explicit and deterministic, which is what was missing. Merging
  them would hide a write behind another write.
* **The `PM_INPUTS_UNAVAILABLE` panel in the hermetic harness.** The portfolio-manager
  endpoint is not one of the bound canonical panels, so it renders its own empty world in an
  acceptance run. It is pre-existing, visible only under the harness, and belongs to the
  acceptance-panel list rather than to the cycle contract.

## Release 30 - Zero-base adaptive alpha capital allocation (LANDED, NOT ACTIVATED)

**Scope.** Change the FORECAST and TARGET-SELECTION methodology feeding the existing
capital-management machinery. No downstream owner is replaced.

| Slice | State | Owner |
|---|---|---|
| Forward-return forecasting layer (5 / 20 / 60 sessions, uncertainty, downside, hashes) | LANDED | `engine/return_forecast` + `api/return_forecast` |
| Zero-base target + implementable target + transition economics | LANDED | `engine/zero_base_allocator` + `api/zero_base_target` |
| Walk-forward tournament, ensemble weighting, risk-price calibration | LANDED (research lane) | `alpha_agent/release30_*`, `scripts/run_release30_zero_base_research.py` |
| Capital-impact feed on Today | LANDED | `api/material_information` (read model) |
| Alpha model leaderboard in Research | LANDED | `api/alpha_leaderboard` (read model) |
| Covariance builder consolidation | LANDED | `engine/holding_opportunity_cost.build_covariance` |
| Aligned-return series consolidation | LANDED | `api/price_panel.aligned_returns` |
| Manual activation of the adaptive candidate | **NOT DONE - evidence says NO_GO** | a human, via an activation record |

**Deliberately deferred.**

* Activation of any forecasting model. The candidate is `R30_ADAPTIVE_MODEL_NO_GO` on both
  universes; nothing is promoted and the operational champion is unchanged.
* Wiring the zero-base target INTO `engine/reallocation_proposal`. The allocator is a
  review surface this release. Feeding it into the proposal owner is a separate, bounded
  slice that must be taken only after a forecasting model has been manually activated -
  otherwise the proposal would inherit an uncalibrated expected return, which is exactly
  the `EXPECTED_RETURN_NOT_CALIBRATED` gap it correctly refuses to fake today.
* Refreshing the owned Phase-24 daily feature panel. Its last session (2026-08-05) sits
  behind the eligible market date (2026-08-18); the gap is DECLARED on every forecast
  rather than extrapolated. Closing it is a data-refresh task, not an architecture one.
* A canonical risk-free / cash-return input. Cash is modelled at a declared zero until an
  admissible owned series exists.

---

## Release 31 Campaign v3 — the three seams the corrections consolidated

Campaign v3 did not add research surface; it removed duplication that the v2
judge had introduced by not existing.

| Seam | Before v3 | After v3 |
|---|---|---|
| **portfolio construction** | the judge built its own top-*N* book, so the research lane and the operational lane constructed portfolios two different ways | ONE seam (`alpha_agent/r31/allocation.py`) delegating to `engine.zero_base_allocator.optimise`; both research tracks and the operator face the same caps, floor and cost |
| **covariance** | absent from research entirely (a top-*N* book needs none), so the research lane had no risk model at all | the canonical `engine.holding_opportunity_cost.build_covariance`, cached once per decision date and reused by every candidate |
| **turnover** | two implementations — the judge's symbol-keyed one and the learner's positional one — that disagreed silently | ONE `traded_notional` over the union of security identities, used by the judge and by both Track-B learners |

The architecture audit's engine allowlist was widened to admit
`engine.holding_opportunity_cost` for exactly this reason, and simultaneously
tightened: every admitted engine owner is re-parsed and must import nothing
outside the standard library, so admission by name cannot smuggle a database
dependency into the research lane.

**What v3 deliberately did not consolidate.** The operational lane still builds
its own book through the released Release-30 path. Release 31 is read-only
research and merges nothing into production; a winning candidate ends at
`MODEL_READY_FOR_MANUAL_PAPER_REVIEW` and a human decides.

## Release 31 — what it consolidated, and what it deliberately did not

### Consolidated

**Model research now has one shape.** Before Release 31 every research stage
(23, 24, 25, 26, 27, 29A–C, 30) invented its own registry, its own evaluation
conventions, its own budget discipline and its own notion of "held out". They
mostly agreed, which is the dangerous case: the differences were invisible and
the results were compared as if they were commensurable.

`alpha_agent/r31/` is the consolidated shape — contract, snapshot, partition,
judge, registry, lockbox, inference, verdict — with one owner each and a strict
audit guard. A future campaign reuses it and changes only the contract.

**One judge, reading canonical economics.** Every prior stage restated a cost
assumption. Release 31's judge reads
`engine.zero_base_allocator.default_policy()` and the audit forbids a literal
cost or cap number inside it. This closes the "research number that cannot be
compared with an operational number" gap that made several earlier stages'
conclusions hard to act on.

**Learners are no longer per-stage.** Ridge, GBRT and extremely-randomised trees
are the released Release-30 implementations, re-exported rather than copied.
Release 31 adds nine more families in the same module, so the next campaign
inherits them.

### Explicitly NOT consolidated (and why)

* **The zero-base allocator is still not the operational allocation path.**
  Release 30.1's verdict stands: it consumes `mu`, and cutting it over without a
  defensible one would replace a book built from the approved model's top ranks
  with a book shaped by constraints and noise. Release 31 is the search for that
  `mu`; it does not pre-empt the result.
* **`engine.reallocation_proposal` remains the operational construction path**,
  unchanged and unendorsed. It is at least derived from the approved model's
  ranking in the approved direction.
* **No research candidate was given a path to the operational model.** The audit
  enforces that the research package cannot import the API, cannot reference the
  proposal/decision/execution owners, and contains no promotion or activation
  call.

### Remaining gaps, restated with Release 31's measurements

* **The owned point-in-time fundamental history is survivorship-limited by a
  measured 3.42×** (46.2 % coverage of still-trading names against 13.5 % of
  delisted names, 846 covered CIKs). This is now a number that changes what a
  sample is permitted to conclude, not an adjective. Closing it needs a
  survivorship-complete PIT fundamental source — a data acquisition, not an
  architecture change.
* **Historical sector remains unusable** for signal construction, and therefore
  historical sector EXPOSURE remains unmeasurable. The judge reports
  `UNMEASURABLE_PIT` rather than a number. Closing it needs a point-in-time
  historical sector/industry mapping.
* **The benchmark is the owned universe's equal-weight return**, because the
  owned panel carries no index security. Closing it needs an owned index series;
  until then the campaign benchmarks against the universe it actually selects
  from and says so.
* **Cash is still modelled at a declared zero return.** Unchanged by Release 31.
* **The owned Phase-24 daily panel still ends before the eligible market date.**
  Unchanged by Release 31; the campaign's snapshot declares its own last date
  (2026-04-23 for a completed 60-session label) rather than extrapolating.

### The next slice, if the campaign exhausts

Not another factor campaign over the same information — the contract explicitly
forbids extending the budget in response to a poor result, and says why: further
search over the same information set raises data-mining risk faster than
knowledge.

The next slice is **new orthogonal information** through the extension contract
in [TARGET_ARCHITECTURE.md](TARGET_ARCHITECTURE.md): a new data family, its own
PIT and publication semantics, its **measured** survivorship coverage, a new
campaign id, and the same judge and budgets unchanged.
## Release 32 — what it consolidated, and what it deliberately did not

**Consolidated.**

- ONE data source registry with a MEASURED point-in-time admissibility class per
  source, replacing the per-phase habit of deciding admissibility in prose.
- ONE canonical `InformationState` carrying four distinct timestamps
  (`observed_at` / `published_at` / `effective_at` /
  `eligible_for_decision_at`), so look-ahead is refused at construction instead
  of being reviewed for.
- ONE Strategy Sleeve contract, so a sleeve cannot become a second portfolio
  optimiser.
- ONE common economic judge for every sleeve, and ONE control — the
  volatility-matched benchmark/cash mix — so six strategies are comparable.
- ONE bounded funnel owning every budget as a number and the multiple-testing
  denominator, including hypotheses that failed.
- Multiple-testing statistics and the lockbox discipline REUSED from Release 31
  rather than reimplemented.

**Deliberately not consolidated.**

- No production allocator. Release 32 produces a research comparison and a
  governance contract; Release 33 builds the allocator that consumes them.
- No second event system: the governance contract declares reuse of
  `engine.event_fabric`.
- No multi-asset NAV implementation: the future owner is *declared*
  (`api.portfolio_valuation`) so a second one cannot appear, but nothing was
  extended.
- Release 31's equity-selection search was NOT rerun. It is inherited as the
  control with its frozen verdict, because more mining of the same information
  adds to the multiple-testing denominator and adds no knowledge.

## R54 Slice 1 — Active Manager Operating State (LANDED) + Slice 1 consolidation

Consolidated:
- ONE composed operating-state read model (`api.active_manager_state`,
  `GET /v1/operations/active-manager-state`) over the existing canonical
  owners; ONE Today loader; the explicit operational-vs-live time-state
  distinction; stale/missing components stated in each owner's own vocabulary.
- The Today operational-mark pill (`cc-status-mark`): the legacy guard-free
  command-center writer (fallback = the dormant legacy DB book's date) is
  removed; `renderPortfolioState`/`_psOwnSet` is the ONE unguarded writer;
  strict-audit-guarded against reintroduction.
- Phase-D classification register:
  `docs/architecture/system_inventory.json` → `r54_ownership_classification`
  (no TRUE_DUPLICATE_OWNER live in the operational decision path; the
  equity-era cockpit family is explicitly LEGACY_DEPRECATED).

Explicitly NOT consolidated (and why):
- The legacy cockpit surfaces (`command_center`, `daily_workflow_dashboard`,
  `current_operating_state`, `daily_operating_run`,
  `/v1/review/current-workflow-state`) stay as classified LEGACY_DEPRECATED:
  they serve demoted views, their removal is a contract change belonging to the
  quarantine slice (Slice 11), and they no longer write any canonical node.
- No reassessment cadence was enabled (`cadence_enabled` stays a declared
  False): R54.1 owns the activation, under the contract in
  `docs/RELEASE54_ACTIVE_MANAGER_OPERATING_MODEL.md`.

## R54.1 — the governed intraday decision cycle (LANDED)

The activation the R54 contract described, delivered as ONE gate rather than a
cadence change:

- **ONE intraday governance gate**, inside the canonical decision owner
  `api.portfolio_decision`. 38 checks in nine groups decide admissibility only;
  every hurdle, cost, risk and outcome is read verbatim from
  `engine.constrained_reallocation`. Verdicts:
  `GOVERNED_INTRADAY_DECISION_ELIGIBLE` / `INTRADAY_DECISION_WITHHELD` with a
  classified reason taxonomy that REUSES the canonical codes.
- **ONE governed decision lane** (`governed_decisions.json` +
  `governed_index.json`) in the same owner's root, separate from the manual
  operator lane. Append-only, idempotent on evidence identity, immutable;
  supersession names `supersedes_decision_id` under ONE total ordering shared
  by the gate and the read.
- **The live cycle DELEGATES** (`GOVERNANCE_DELEGATE = "api.portfolio_decision"`)
  and hosts no governance rule; a failing gate degrades to a warning.
- **Active Manager State projects both lanes separately** plus the gate verdict
  and the measured decision latency. No governance logic in JavaScript.
- **Boundaries held:** no cadence change (`cadence_enabled` stays a declared
  False — detection, not the ~7.3 s decision chain, is still the bottleneck, and
  raising it waits on the measured `observation_to_governed_seconds` series); no
  scheduler touched; no emission-slot change (the 16:20 ET trigger is the
  declared post-close scoring pass, not a defect); manual review, approval token
  and execution path unchanged.
- Guarded by `check_release54_1_governed_intraday_decision` (24 strict-blocking
  invariants) and `tests/test_release54_1_governed_intraday_decision.py` (95).

**Live finding (read-only dry run, nothing written):** the gate scores 37/38 on
the real 2026-09-01 state and withholds on ONE check —
`CYCLE_REASSESSMENT_IS_THE_CANDIDATE`. Proven cause: the event cycle computed a
different reassessment from newer signals, but the ECONOMIC fingerprint is
unchanged, so Stage-21 case (a) applies and `persist_reassessment` returns
`CONFLICT_REJECTED`; the cycle's conclusion has no immutable artifact behind it
and must not be governed. Under continuous collection "the prior artifact still
describes the portfolio" stays true about the PORTFOLIO and becomes false about
the ANSWER — which is the whole premise of an active manager.

## R54.2 — same-session reassessment versioning (LANDED)

The persistence rule that closed R54.1's 37/38, made correct rather than lenient:

- **A third identity, in the ONE reassessment owner.**
  `assessment_evidence_hash` (11 canonical bound-evidence components including a
  `declared_inputs_fingerprint` for freshness) answers "has the EVIDENCE
  changed?" beside Stage-21's `economic_state_hash` ("has the PORTFOLIO
  changed?"), with `decision_fingerprint` (the result minus `provenance` and
  `reassessment_hash`) answering "has the ANSWER changed?". The evidence
  identity excludes `portfolio_state_hash` (the Stage-21 trap), the economic
  axis, the conclusion, and all provenance (clock / run id / trigger
  fingerprint) — so a poll can never manufacture a version.
- **Four persistence outcomes, append-only.** `REUSED_EXISTING`,
  **`CREATED_ASSESSMENT_VERSION`** (the new case), `CREATED_NEW_VERSION`
  (Stage 21, unchanged) and `CONFLICT_REJECTED` (same evidence, DIFFERENT
  conclusion — the residue of the old rule), plus
  `REJECTED_INCONSISTENT_IDENTITY` when an artifact's own parts disagree about
  the session or the book. Nothing is ever rewritten; a collision guard enforces
  that at the write; a pre-R54.2 index entry is recomputed, not rewritten.
- **ONE history.** The DRC and the event cycle append to the same chain through
  `run_and_persist`; `load_artifact_versions` exposes it and
  `load_artifact_by_id` keeps an older id resolving to its exact artifact.
- **No double counting and no self-blocking churn.**
  `authoritative_history_rows` gives one authoritative row per session for the
  churn input, `build_attribution` and `api.reassessment_outcomes`, and
  `recent_change_rows` excludes the session being assessed.
- **The R54.1 gate was TIGHTENED.** Still 38 checks;
  `CYCLE_REASSESSMENT_IS_THE_CANDIDATE` now also requires that the cycle's
  conclusion became an immutable artifact, and the cycle publishes the
  persistence outcome so a refused write stays visible.
- Guarded by `check_release54_2_same_session_reassessment_versioning` (20
  strict-blocking invariants) and
  `tests/test_release54_2_same_session_reassessment_versioning.py` (55),
  including a hermetic end-to-end 38/38 proof driven by the real persistence
  owner.

**Live read-only verdict (nothing written):** on the real 2026-08-31 state the
economic fingerprint is unchanged and the ONE changed evidence component is
`hoc_assessment_hash` (`6de5ece4…` persisted vs `9efb688d…` live), so the
verdict is `CREATED_ASSESSMENT_VERSION` — the live cycle's reassessment would be
appended as version 2 and the withheld check would pass.

Next (R54.3): give `api.holding_opportunity_cost.persist_assessment` the same
two-axis append semantics. It still `CONFLICT_REJECTED`s a same-session
assessment with different evidence, so the live HOC assessment has no artifact
on disk while the persisted reallocation proposal already binds it —
PRE-EXISTING (the proposal owner has superseded on changed inputs since Slice 7)
and not created by R54.2, but a governed intraday decision would bind a HOC hash
that is not retrievable. It is a separate slice because it changes a second
canonical owner's immutability contract and its own required regression.

Then (R54.4): make `api.daily_research_cycle` persist its terminal decision
through the SAME governed writer (`provenance=GOVERNED_DAILY_CYCLE`), so the
governed lane becomes the single durable history of every authoritative
recommendation and the projection step disappears. One call site, behind the
invariants R54.1 added.

---

## R54.2.1 — missed eligible session recovery (LANDED)

The reliability gap the R54.2 head left open: a completed market session that
was never closed simply stopped existing when the wall clock rolled forward.

- **The obligation is a CALENDAR question against the CLOSE JOURNAL.**
  `_decide_overall` P2 asserted "the latest eligible completed session is already
  fully processed" and tested it with `eligible_session_closed`, which is
  computed against the OWNED-DATA-CONFIRMED session. Owned confirmation is the
  persisted desk-mark date and only advances when a close runs, so **an unclosed
  completed session can never confirm itself**: on 2026-09-02 P2 answered
  truthfully about 2026-08-31 while 2026-09-01 had never been closed, and the
  Daily Close owner said "SEPTEMBER 1 EOD DATA READY" in the same minute.
- **Two questions, two owners.** `engine.market_session` keeps the CONFIRMATION
  question and gains the pure enumeration `completed_sessions_after(last_closed,
  through=…, non_sessions=…)`; `api.workflow_state.build_session_recovery`
  composes the catch-up STATE from it plus `api.daily_close`'s close journal.
  `recovery_session` is always the OLDEST missed session; the still-forming
  current session is excluded by construction and no `today - 1` arithmetic
  exists anywhere.
- **Four states, no invented vocabulary.** `NO_CATCH_UP_REQUIRED` /
  `CATCH_UP_REQUIRED` / `CATCH_UP_WAITING_FOR_OWNED_DATA` / `CATCH_UP_BLOCKED`,
  with a probe-free owned-data axis (`CONFIRMED` /
  `UNVERIFIED_UNTIL_CLOSE_REVALIDATES` / `OWNED_DATA_LAGGING`). An un-ingested
  mark is a publish/ingest gap, never proof the provider lacks the session —
  the Phase-29D.1 holiday principle, generalised. `api.operator_presentation`
  places the close owner's real provider answer beside the obligation.
- **ONE orchestration path, session BOUND by the server.** Recovery runs through
  the existing `POST /v1/operations/portfolio-cycle/run`.
  `api.portfolio_cycle.recovery_binding()` reads `recovery_session` verbatim and
  passes `target_market_date` to `api.daily_close`, whose `_apply_session_binding`
  narrows `clock["expected_market_date"]` — already the ONE value the provider
  probe, the desk-mark `completed_through`, the model-input refresh and the
  idempotency key read. A forward binding is REFUSED, never clamped. No
  recover/backfill/force-close route, no second orchestrator, no operator date.
- **Priority, not a new state.** P2 is suppressed by `catch_up_required` and P3.7
  fires on it, so recovery resolves through the existing `READY_FOR_DAILY_CLOSE`.
  A missed session outranks every "nothing is outstanding" claim but never an
  inconsistency, an unconfirmed session, an in-flight cycle or a named blocker.
- Guarded by `check_release54_2_1_missed_session_recovery` (25 strict-blocking
  invariants) and `tests/test_release54_2_1_missed_session_recovery.py` (43).

**Live read-only verdict (nothing written, backend never restarted):** the
repaired owner against the REAL stores reports `CATCH_UP_REQUIRED`,
`recovery_session 2026-09-01`, `next_action RUN_PORTFOLIO_CYCLE`, payload
`CONSISTENT`; Sep-1 owned data IS published (`provider_latest_date 2026-09-01`).
The running 8001 backend still holds the pre-R54.2.1 runtime, so it must be
restarted with the canonical script before Today can offer the recovery.
