# Paper Trader — Target Architecture

> The intended boundaries that let the system deliver the seven milestones in
> [PROJECT_CHARTER.md](PROJECT_CHARTER.md) **without a big-bang rewrite**. This
> is a destination, reached by the bounded slices in
> [CONSOLIDATION_ROADMAP.md](CONSOLIDATION_ROADMAP.md). Names are derived from
> existing modules wherever a sensible owner already exists (Principle 8).
>
> The organizing rule is **Principle 1**: exactly one authoritative
> implementation per business concept, and **Principle 2**: one orchestration
> path per operator workflow. The current conflicts are catalogued in
> [CURRENT_ARCHITECTURE.md](CURRENT_ARCHITECTURE.md) §10–§11.

## 1. Bounded contexts

```mermaid
flowchart TD
  MS["Market Session and Data Freshness"] --> MD["Market Data"]
  MD --> FP["Feature Production"]
  FP --> US["Universe Scoring"]
  US --> RC["Research Cycle"]
  RC --> MR["Model Registry and Champion/Challenger Governance"]
  US --> HOC["Holding Opportunity-Cost Assessment"]
  PS["Portfolio State"] --> HOC
  HOC --> PP["Portfolio Proposal"]
  PP --> RCE["Risk and Cost Evaluation"]
  RCE --> ODC["Operational Daily Close"]
  ODC --> FEV["Forward Evidence"]
  ODC --> PS
  ODC --> EX["Execution (deferred)"]
  ALL["Read Models / UI"] -.-> ODC
  ORCH["Orchestration"] --> ODC
  ORCH --> RC
```

## 2. One authoritative owner per canonical concept

This table is the contract: exactly one owner per concept. A second writer is a
defect to be migrated, not a new owner to be blessed.

| Concept | Target owner | Consolidates today's |
|---|---|---|
| Market session / eligible date | `engine/market_session` (**LANDED, Slice 1**; built on `market_hours`) | ≥8 date resolvers + 6 `_today()` seams |
| Cross-source data freshness | `api/data_freshness` (**LANDED, Slice 1**; read-only) | ad-hoc per-surface date/freshness displays |
| Latest completed price date | `engine/market_session` | `func.max(PriceSnapshot.market_date)` sites, `daily_close._latest_eligible_market_date` |
| Market data (prices) | `market_data_service` (unify `engine/market_data` + owned-EOD transport) | World A Yahoo vs World B EODHD split |
| Feature production | `feature_service` (extract from `multi_horizon_engine` inputs) | scattered CSV input builders |
| Universe scoring / rankings | `api/universe_scoring` (**LANDED, Slice 4**; composition & read owner over the `multi_horizon_engine.compute_scores` kernel) | ≥8 z-score/rank reimplementations (legacy copies retire with the DB screener, Slice 11) |
| Research cycle orchestration | `api/daily_research_cycle` (**LANDED, Slice 3**; composes session/freshness + `alpha_target.run_refresh` + `multi_horizon_engine` scoring + `forward_prediction_skill` evidence + `daily_action_gate` bridge) | Daily Alpha Run vs alpha-target vs close-embedded refresh; hidden month-boundary prerequisite |
| Model registry / champion governance | `model_registry` (unify `alpha_registry` + tournament) | 2 challenger registries (phase20/21), dead phase18 wire |
| Portfolio state (NAV, cash, holdings) | `portfolio_state` (`portfolio_valuation` as the mark authority) | 2 NAV authorities, `book_nav`, `cached_total_value`, `_collect_positions` |
| Holding opportunity-cost | `opportunity_cost_engine` (new; Milestone 2) | ad-hoc rank/deterioration logic in `portfolio_manager` |
| Portfolio proposal / target | `portfolio_proposal` (`alpha_target` + `operational_book`) | operational snapshot vs frozen champion book |
| Risk and cost evaluation | `risk_cost_service` (unify `engine/risk` + cap families) | duplicated top-N-sector-cap + name-cap code |
| Forward evidence | `forward_evidence` (+ `forward_prediction_skill`) | already coherent — keep |
| Operational daily close | `daily_close.run_daily_close` | World A/B split; standalone refresh bypass |
| Workflow / gate state | `api/workflow_state` (**LANDED, Slice 2**; composes the gate/close/book/freshness domain facts) | `app.py:_build_workflow_state` (legacy), `command_center._derive_stage`, `daily_workflow_dashboard` stages, `derive_lifecycle_view` |
| Execution | `execution` (deferred; empty until Milestone 7) | the legacy paper "Create Orders" surface |
| Read models / UI | `read_models` + `api/ui` | NAV/holdings rendered from 3 payloads |
| Orchestration | `orchestration` (one path per workflow) | 5 standalone mark writers |

## 3. Context definitions

Each context lists responsibility, inputs, outputs, owned state, forbidden
responsibilities, candidate existing modules, and migration approach.

### Market Session and Data Freshness
- **Responsibility:** resolve the single eligible market session / latest
  completed date and classify freshness of every input family.
- **Inputs:** wall clock (ET), owned SPY/price calendar, provider readiness.
- **Outputs:** `eligible_market_date`, per-family freshness (`FRESH/STALE/BLOCKED`).
- **Source-of-truth boundary (Phase 29B.1):** OPERATIONAL date concepts
  (eligible/owned-data confirmation, valuation, desk mark, benchmark, target) are
  owned by the **active operational book** (`api/operational_book.py` — the
  authoritative book-selection policy) and its owned desk marks; a dormant
  legacy/current-alpha research book (`portfolio_valuation`/`current_operating_state`)
  MUST NOT supply operational readiness. RESEARCH dates (champion evaluation mark,
  latest price/score refresh, frozen monthly momentum input, fundamental panel,
  TRUE_FORWARD snapshot) are owned separately and never collapsed. The frozen
  monthly momentum input is read directly from its persisted `month_label` and is
  never proxied. The freshness owner additionally emits the active-book identity
  and a read-only cross-surface `consistency_status` (never a provider call).
- **Owned state:** none persistent (pure function over the calendar).
- **Forbidden:** fetching prices, writing ledgers, scoring.
- **Candidates:** `daily_operating_run.latest_completed_market_date`,
  `daily_close._latest_eligible_market_date`, `market_hours`.
- **Migration:** extract one `market_session` function; route all resolvers and
  `_today()` seams through it behind tests.
- **Status (Slice 1, LANDED):** `engine/market_session.py` (pure owner) +
  `api/data_freshness.py` (read-only freshness) + `GET /v1/operations/data-freshness`
  + one UI `loadDataFreshness()` loader. `daily_operating_run`, `daily_close` and
  `alpha_target` delegate the session arithmetic; the 17:30-vs-16:00 close policy
  is an explicit parameter. Owned-provider-confirmed sessions are the holiday-safe
  authority (no exchange-calendar dependency is installed); the weekday+cutoff
  calculation is a labelled *expectation* that never overrides confirmed owned
  data. The historical evidence calendar (`forward_prediction_skill.eligible_calendar`)
  and the research forward-roll (`alpha_agent/source_exhaustion.py`) are kept
  separate. Remaining resolvers (`paper_trading_desk._required_mark_date`,
  `current_alpha_tournament_sync`, `market_screener`) are documented follow-ups.

### Workflow / Operator State
- **Responsibility:** hold the single authoritative *combined operator
  interpretation* — the overall workflow state, the current task, the one primary
  next action (severity/destination/safe/execution flags), the queued follow-ups,
  the portfolio-assessment currency, the blockers, the completed-state summary and
  a cross-surface consistency verdict.
- **Inputs:** the Slice-1 `data_freshness` contract (session + dates + active book
  + consistency), the Daily Action Gate (assessment domain fact + review clock),
  the probe-free Daily Close progress, the active Operational Book, the Forward
  Prediction Skill, alpha-target readiness. **Composed, never recomputed.**
- **Outputs:** one workflow contract (`GET /v1/operations/workflow-state`).
- **Owned state:** none (pure composition over injected read models).
- **Forbidden:** re-deriving any domain fact; any provider/prediction call; any
  Daily Close, research refresh, portfolio reassessment, model promotion or write.
- **Candidates:** `api/workflow_state` (**LANDED, Slice 2**).
- **Status (Slice 2, LANDED):** `api/workflow_state.py` (read-only owner) +
  `GET /v1/operations/workflow-state` + one UI `loadWorkflowState()` loader that
  fans the ONE payload to every primary surface and the Action/Safety panel. The
  frozen overall-state vocabulary, the assessment-currency vocabulary, the
  deterministic priority policy and the decision-currency rule (a stale gate
  result is never re-presented as a "today" conclusion) live here. Specialized
  modules keep their domain facts; the four legacy stage vocabularies
  (`app.py`×2, `command_center`, `daily_workflow_dashboard`) are documented and
  retired with the legacy Create-Orders surface (Slice 11). The UI derives no
  workflow priority or assessment currency. Slice 2 performs no workflow action.
- **UI DOM-ownership boundary (Phase 29C.1, LANDED):** `renderWorkflowState` is the
  EXCLUSIVE owner of every visible primary operator-interpretation node (the workflow
  banners, the right Action/Safety panel, and the reframed Daily-Action-Gate card
  title/currency-badge/headline/explanation), backed by additive backend
  `assessment_presentation` / `evidence_presentation` blocks. The specialized detail
  renderers (`renderDailyActionGate`/`renderDailyClose`/`renderOperationalBook`) own
  DETAIL only and are hard-guarded out of the canonical nodes, so the final visible
  state is independent of async loader completion order (ownership, not timing). This
  is the DOM-side analogue of "one concept, one owner": one owner per *visible*
  interpretation, mirroring the backend one-owner-per-concept boundary.

### Market Data
- **Responsibility:** produce point-in-time EOD prices for the universe and
  benchmark, from one provider policy.
- **Inputs:** market session, vendor (owned EODHD; Yahoo only as fallback).
- **Outputs:** price rows keyed `(ticker, market_date)`.
- **Owned state:** `price_snapshots`, `benchmark_prices`, desk mark cache.
- **Forbidden:** scoring, portfolio math.
- **Candidates:** `engine/market_data`, owned-EOD transport in `alpha_target`/
  `daily_close`.
- **Migration:** converge World A/World B onto one provider policy; keep both
  stores until parity is proven, then deprecate the Yahoo path.

### Feature Production
- **Responsibility:** derive model inputs (momentum, risk stats) as PIT CSVs.
- **Inputs:** market data, fundamentals.
- **Outputs:** input CSVs consumed by scoring.
- **Owned state:** input CSVs + manifest.
- **Forbidden:** ranking/selection.
- **Candidates:** `alpha_target` input builders.
- **Migration:** name the boundary; leave frozen monthly-momentum semantics.

### Universe Scoring
- **Responsibility:** compute `composite_sn` and rankings for the full universe.
- **Inputs:** features.
- **Outputs:** per-ticker scores/ranks (sector-neutral).
- **Owned state:** none (recomputed).
- **Forbidden:** portfolio construction, caps.
- **Candidates:** `api/universe_scoring` (**LANDED, Slice 4**) over the
  `multi_horizon_engine.compute_scores/build_current` kernel.
- **Status (Slice 4, LANDED):** `api/universe_scoring.py` is the ONE operational
  scoring/ranking composition & read owner. The kernel keeps all model mathematics
  (`compute_scores`/`_percentiles`/`compute_combined`/`build_books`, unchanged); the
  owner adds identity, the content-level `input_contract_hash`, count reconciliation,
  universe identity, exclusions and a cross-consumer consistency validator, exposed at
  `GET /v1/research/universe-scoring` (compat: `current-alpha-scores`). It deep-copies
  the kernel cache (never mutated), performs no scoring math, no provider/prediction
  call and no write, and never promotes/recalibrates a model.
- **Migration:** the DRC scoring adapter and `multi_horizon_platform.load_current_scores`
  delegate to the owner; `alpha_target`/`forward_prediction_skill` re-export its primary
  identity. Remaining: the ≥8 legacy `zscore`/`rank` copies and `engine/scoring.py`
  retire with the DB screener (Slice 11).

### Research Cycle
- **Responsibility:** orchestrate one persistent daily research pass (session →
  plan → data refresh → date-alignment → scoring → target → forward evidence →
  portfolio-assessment bridge) with no hidden operator prerequisites (Milestone 1).
- **Inputs:** market session / freshness, data, scoring.
- **Outputs:** one canonical run manifest + status contract.
- **Owned state:** run-status manifests under a research root (`PAPER_TRADER_DRC_DIR`).
- **Forbidden:** mutating holdings; running Daily Close; auto-confirming a target;
  promoting/recalibrating a model; creating an order/signal/decision/fill;
  approximating the frozen monthly input.
- **Candidates:** `api/daily_research_cycle` (**LANDED, Slice 3**).
- **Status (Slice 3, LANDED):** `api/daily_research_cycle.py` is the ONE idempotent,
  resumable orchestration owner. It composes the existing authoritative owners
  through adapters (it does NOT consolidate scoring — Slice 4 — or portfolio state —
  Slice 5, and is NOT the Milestone-2 opportunity-cost engine). The evidence count is
  derived from `forward_prediction_skill.SUPPORTED_BOOKS`; the bundle is immutable and
  first-write-wins; Daily Close idempotently reuses it (the cycle never runs Daily
  Close). There is no safe automatic monthly-momentum emitter, so the cycle BLOCKS at
  the month boundary (`RUN_RESEARCH_MONTHLY_INPUT_EMITTER`) instead of approximating.
  `GET /v1/operations/daily-research-cycle/status` + `POST .../run`
  (`RUN_DAILY_RESEARCH_CYCLE`); one UI status loader + one execution function;
  `api/workflow_state` consumes the status (`RESEARCH_CYCLE_RUNNING` /
  `RESEARCH_CYCLE_BLOCKED`). The standalone champion refresh remains a research detail.

### Model Registry and Champion/Challenger Governance
- **Responsibility:** hold champion + challenger models, run gated evaluation,
  surface promotion eligibility (never auto-promote).
- **Inputs:** forward evidence, tournament sync.
- **Outputs:** leaderboard, eligibility, promotion proposals.
- **Owned state:** one unified registry.
- **Forbidden:** auto-promotion, changing operational holdings.
- **Candidates:** `alpha_registry`, `tournament`, `alpha_factory`,
  `price_alpha_factory`, `current_alpha_tournament_sync`.
- **Migration:** merge the two factory registries; remove the dead phase18 wire.

### Portfolio State
- **Responsibility:** the one authoritative NAV / cash / holdings mark.
- **Inputs:** market data, ledger fills.
- **Outputs:** NAV, positions, freshness, reconciliation.
- **Owned state:** valuation read model (never `cached_total_value`).
- **Forbidden:** proposing changes.
- **Candidates:** `portfolio_valuation` (mark authority), `paper_trading_desk.book_nav`.
- **Migration:** one NAV function; UI reads one payload; retire `book_nav`/
  `_collect_positions`/`cached_total_value` as marks.

### Holding Opportunity-Cost Assessment
- **Responsibility:** per-holding HOLD/REDUCE/EXIT/REPLACE/ADD with the full
  measure set (Milestone 2).
- **Inputs:** scoring, portfolio state, risk.
- **Outputs:** per-holding recommendation + evidence.
- **Owned state:** none.
- **Forbidden:** executing changes.
- **Candidates:** new; seed from `portfolio_manager` signals.

### Portfolio Proposal
- **Responsibility:** a complete paper-only target with full before/after
  explanation (Milestone 3).
- **Inputs:** opportunity-cost, risk/cost.
- **Outputs:** target portfolio + turnover/cost/risk deltas.
- **Owned state:** confirmed snapshot ledger.
- **Forbidden:** creating orders without manual approval.
- **Candidates:** `alpha_target`, `operational_book`, `current_alpha_book`.

### Risk and Cost Evaluation
- **Responsibility:** volatility, drawdown, concentration, caps, switching cost.
- **Inputs:** portfolio state, target.
- **Outputs:** risk/cost metrics + cap compliance.
- **Owned state:** none.
- **Forbidden:** selection (only evaluates).
- **Candidates:** `engine/risk`, cap families in `multi_horizon_engine`,
  `absolute_return_research`, `daily_action_gate`.

### Forward Evidence
- **Responsibility:** immutable TRUE_FORWARD capture, maturation, attribution;
  gaps documented never fabricated (Principle 4).
- **Candidates:** `forward_prediction_skill`, `forward_evidence`. **Keep as-is.**

### Operational Daily Close
- **Responsibility:** the one atomic operational close composing marks + model
  inputs + gate + decision + evidence (Principle 2).
- **Candidates:** `daily_close.run_daily_close`. **Keep; remove bypass paths.**

### Execution (deferred — Milestone 7)
- **Responsibility:** none until Milestone 7. Empty by design.
- **Forbidden:** everything (no broker, no live orders, no automation).
- **Migration:** quarantine and document the legacy paper "Create Orders" surface.

### Read Models / UI
- **Responsibility:** render authoritative backend state only (Principle 6).
- **Candidates:** `api/ui/index.html`, `command_center`, read endpoints.
- **Migration:** every concept renders from one payload; no client-side money/
  date math.

### Orchestration
- **Responsibility:** sequence workflows deterministically; one path each.
- **Candidates:** `daily_close`, `daily_operating_run`, `research_cycle`.
- **Migration:** standalone mark writers become wrappers over the composed path.

## 4. What the target deliberately keeps

- Paper-only, preview-first, manual-review, no-automation boundaries.
- Remote prediction at `:9000`; no local prediction.
- The clean `db/session.py` boundary — the model for the future store service.
- The Phase 28C separation of operational status from research/forward evidence.
- Incremental, test-guarded migration — no monolith is rewritten wholesale.
