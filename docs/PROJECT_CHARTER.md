# Paper Trader — Project Charter

> Canonical, durable statement of what this system is for. This document is the
> single source of intent. When any other document, module, endpoint, UI panel,
> test, or roadmap slice disagrees with the charter, the charter wins.
>
> Related canonical documents:
> [CURRENT_ARCHITECTURE.md](CURRENT_ARCHITECTURE.md) ·
> [TARGET_ARCHITECTURE.md](TARGET_ARCHITECTURE.md) ·
> [CONSOLIDATION_ROADMAP.md](CONSOLIDATION_ROADMAP.md) ·
> [ARCHITECTURE_DECISIONS.md](ARCHITECTURE_DECISIONS.md)

## Canonical Objective

Build an active, research-driven paper portfolio manager that continuously
determines whether the current holdings remain the best risk-adjusted use of
capital, identifies stronger alternatives, and produces explainable
portfolio-change proposals under strict safety, point-in-time evidence, and
manual-review controls.

The system must eventually operate close to real time.

### The objective is asset-agnostic (Release 32)

Equities were the initial proving ground. They are not the objective. The
permanent question is:

> **If every investable dollar were cash right now, given everything
> legitimately observable right now, where should capital be deployed to
> maximise expected after-cost, risk-adjusted paper portfolio PnL?**

Legitimate answers eventually include cash, individual equities, equity
indices, sectors and factors, rates and bonds, commodities, FX, volatility
exposures, event-driven opportunities, and other validated strategy sleeves.

Three consequences are binding:

- The system is **not required** to allocate to every asset class. Capital
  belongs only where the evidence supports it.
- A **NULL result is valid.** "Nothing qualified" is knowledge.
- **Cash is a real asset choice**, not a residual.

See [PNL_OPPORTUNITY_FRONTIER.md](PNL_OPPORTUNITY_FRONTIER.md) for the
comparison rules and [STRATEGY_SLEEVE_CONTRACT.md](STRATEGY_SLEEVE_CONTRACT.md)
for the sleeve boundary.

### What this system is NOT

- It is **not** a static buy-and-hold tracker.
- It is **not** a monthly-maintenance tool.
- It is **not** a collection of disconnected research dashboards.
- It is **not** a system that constantly retrains models without evidence.
- It is **not** an automated broker-execution system (yet).

## Three Operating Cycles

The system runs three distinct cycles at deliberately different cadences.

1. **Signal Refresh — FREQUENT.** Refresh prices, features, scores, rankings,
   forecasts, and data-quality status as frequently as the available data
   supports.
2. **Portfolio Reassessment — FREQUENT.** After every signal refresh, compare
   every current holding with the strongest eligible alternatives and determine
   whether capital would be better deployed elsewhere.
3. **Model Recalibration — CONTROLLED.** Recalibrate or replace models only when
   sufficient new evidence exists, forward performance deteriorates,
   relationships drift, a formal checkpoint is reached, or a challenger proves
   superior.

**Standing principle:** Refresh signals frequently. Reassess the portfolio
frequently. Recalibrate models under controlled evidence gates.

## Seven Canonical Milestones

### Milestone 1 — Reliable Persistent Daily Research Cycle

Automatically determine the latest eligible market session, refresh all
daily-compatible inputs, identify stale slower-moving inputs, score the full
universe, populate run status, and capture immutable forward evidence without
hidden operator prerequisites.

### Milestone 2 — Holding Opportunity-Cost Engine

For every holding, measure: current rank; rank change; signal strength;
deterioration; recent performance; drawdown; risk contribution; concentration;
liquidity; strongest replacement candidate; switching cost; expected
improvement. Recommend one of: **HOLD, REDUCE, EXIT, REPLACE, ADD.**

### Milestone 3 — Portfolio Reallocation Proposal Engine

Generate a complete paper-only target portfolio and explain: retained holdings;
reductions; exits; additions; expected turnover; transaction costs;
expected-return improvement; volatility before and after; drawdown before and
after; concentration before and after; risk before and after. Manual review
remains mandatory.

### Milestone 4 — Persistent Alpha Research Agent

Continuously monitor: data freshness; champion and challenger models; forward
evidence; model degradation; research opportunities; bounded experiments. Never
automatically promote a model or change operational holdings.

### Milestone 5 — Data Expansion

Evaluate and acquire economically distinct datasets only when these pass:
point-in-time integrity; historical depth; inactive/delisted coverage;
effective sample; incremental research value; licensing; cost. Historical
analyst revisions (Stage 13A) belong to this supporting milestone.

### Milestone 6 — Intraday and Near-Real-Time Operation

After the daily system is reliable, add: intraday data; incremental features;
event-driven rescoring; opportunity alerts; turnover controls; latency
monitoring; data-quality safeguards.

### Milestone 7 — Controlled Execution

Only after the prior milestones are stable: paper-order creation; paper
execution; reconciliation; controlled broker integration; explicitly approved
automation.

## Eight Architectural Principles

There are exactly eight architectural principles, and there always have been.
Later releases derive more specific *design rules* from them — see
[Release-32 Multi-Asset Design Rules](#release-32-multi-asset-design-rules) —
but a derived rule never becomes a ninth principle. The principle set is the
stable spine of the architecture; if it grew every time a release added a
constraint, it would stop being a spine.

### Principle 1 — One Canonical Calculation Per Business Concept

NAV, eligible market date, model mark, rankings, target portfolio, evidence
state, and workflow state must each have exactly one authoritative
implementation.

### Principle 2 — One Orchestration Path Per Operator Workflow

Daily Research, Daily Close, Portfolio Review, Evidence Capture, and later
Reallocation must not depend on hidden button ordering or disconnected
prerequisites.

### Principle 3 — Research, Operations, Portfolio Decisions, and Execution Remain Separate

A research failure must not invalidate a valid operational close. A research
recommendation must not create an order. Execution must require explicit
authorization.

### Principle 4 — Point-in-Time Evidence Is Never Fabricated

Missing TRUE_FORWARD evidence remains a documented gap when safe recovery is
impossible. No silent backdating; no hindsight reconstruction relabeled as
forward evidence; no current snapshots substituted backward.

### Principle 5 — Idempotency Everywhere

Repeated requests must not duplicate research runs, closes, snapshots,
proposals, decisions, orders, NAV records, or ledger events.

### Principle 6 — UI Panels Read Authoritative Backend State

No UI panel may maintain a conflicting local interpretation of workflow, dates,
marks, rankings, evidence, holdings, or status.

### Principle 7 — No Automatic Model Promotion

Champion changes require adequate forward evidence, defined gates, and manual
approval. Challenger research never changes operational holdings automatically.

### Principle 8 — Consolidate Incrementally Behind Tests

Do not perform a blind rewrite. Identify ownership, remove duplication in
bounded slices, preserve working behavior, maintain regression coverage, and
deliberately deprecate obsolete paths.

## Release-32 Multi-Asset Design Rules

These four rules are **derived design rules**, not architectural principles.
They are mandatory wherever multi-asset opportunity research, sleeve output, or
future allocation is concerned, and each one is traceable to a principle above.
They are stated separately so the eight principles stay the stable spine while
release-specific rules can be added, refined, or retired.

### Design Rule A — Strategy Sleeves Generate Opportunities; They Do Not Own Capital

A sleeve expresses an opinion in its own terms. A sleeve that sizes a book,
writes a proposal, or creates an order has become a second portfolio optimiser.
*Derived from Principle 1 (one canonical calculation per business concept) and
Principle 3 (research does not create orders).*

### Design Rule B — The Global Portfolio Allocator Owns Capital

One allocator, many opinions. Six sleeves must never become six competing
portfolio managers, each unaware of the others' exposures. *Derived from
Principle 1: target portfolio has exactly one authoritative implementation.*

### Design Rule C — Asset Labels Do Not Equal Diversification

Many tickers are not many independent risks. Exposure is tracked by risk factor
and correlation cluster, never by counting instruments. *Derived from
Principle 4: a diversification claim not supported by point-in-time evidence is
fabricated evidence.*

### Design Rule D — Daily Reassessment Does Not Imply Daily Trading

A difference between the current portfolio and the target is a reason to
evaluate a change, not to make one. The portfolio changes only when expected
after-cost utility improvement exceeds the governance hurdle and every gate
passes. Most days, the correct action is none. *Derived from Principle 3
(separation of reassessment from execution) and Principle 7 (no automatic
promotion).*

See [STRATEGY_SLEEVE_CONTRACT.md](STRATEGY_SLEEVE_CONTRACT.md) and
[DAILY_MULTI_ASSET_GOVERNANCE.md](DAILY_MULTI_ASSET_GOVERNANCE.md) for how each
rule is enforced.

## Safety Boundaries

These boundaries are absolute and are enforced in code, tests, and operating
procedure:

- **Paper-only.** No live orders, no broker execution, no automation are
  implemented or enabled.
- **Preview-first.** DB-writing and ledger-writing actions are explicit,
  operator-initiated, and token-gated where they mutate durable state.
- **Manual review is mandatory** for every portfolio-change proposal and every
  champion change.
- **Prediction runs remotely** (GCP via the `http://127.0.0.1:9000` tunnel) and
  is never run or installed locally.
- **Point-in-time integrity** is never violated to manufacture evidence.
- **Visible safety badges** (`PREVIEW ONLY`, `NO LIVE BROKER ORDERS`,
  `AUTOMATION OFF`, `MANUAL REVIEW`, `CREATES SIGNALS ONLY`,
  `CREATES TRADE DECISIONS ONLY`) remain present where relevant. Phase 27B.6
  settled the order wording deliberately: **paper orders are real** and exist in
  the operational book under a governed, manually reviewed workflow; only **live
  brokerage** orders are structurally disabled. A badge that says "no orders"
  without saying which kind understates what the system does, so
  `>NO LIVE ORDERS</span>` and `>ORDERS DISABLED<` are refused by test and audit.

## Current Scope

- **Release 32 — asset-agnostic opportunity research (active).** Zero-cost
  information expansion, six strategy sleeves under one common economic judge,
  the PnL Opportunity Frontier, the Information Purchase Gate, and the Daily
  Multi-Asset Governance contract. Research and read-only throughout: it builds
  the contracts Release 33 will consume, and moves no capital.
  See [RELEASE32_ZERO_COST_INFORMATION_EXPANSION.md](RELEASE32_ZERO_COST_INFORMATION_EXPANSION.md).
- Consolidation of duplicated calculations and orchestration paths behind tests,
  as sequenced in [CONSOLIDATION_ROADMAP.md](CONSOLIDATION_ROADMAP.md).
- Read-only architecture inventory, documentation, and static tooling.

### Closed

- **Release 31 — Mathematical Alpha Frontier.** Terminal verdict
  `R31_CURRENT_INFORMATION_MODEL_FRONTIER_EXHAUSTED`, dominant constraint
  `INFORMATION_NOT_METHOD`. Loaded as the Release-32 equity-selection baseline;
  **not rerun**.

## Explicitly Deferred Scope

- **Milestone 6 — Intraday / near-real-time** operation is deferred until the
  daily system is reliable.
- **Milestone 7 — Controlled execution** (paper-order creation, paper execution,
  reconciliation, broker integration, approved automation) is deferred until all
  prior milestones are stable.
- **Milestone 5 — Paid-data acquisition** (including historical analyst
  revisions) is a supporting track, gated on point-in-time and cost criteria;
  it is not the main application objective. Release 32 spends **nothing** and
  does not wait for a provider sample; it exists to identify which economic
  state variables actually matter *before* paying for them. See
  [INFORMATION_PURCHASE_GATE.md](INFORMATION_PURCHASE_GATE.md).
- **The production multi-asset allocator is deferred to Release 33.** Release 32
  produces a research-only comparative frontier and the governance contract; it
  does not allocate capital across asset classes.
- No Create Orders, order execution, or automation is in scope in any current
  phase.
