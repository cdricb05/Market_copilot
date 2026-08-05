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
