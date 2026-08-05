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
- **Visible safety badges** (`PREVIEW ONLY`, `NO LIVE ORDERS`, `AUTOMATION OFF`,
  `MANUAL REVIEW`, `CREATES SIGNALS ONLY`, `CREATES TRADE DECISIONS ONLY`) remain
  present where relevant.

## Current Scope

- Milestone 1 (reliable persistent daily research cycle) is the active focus:
  authoritative dates/freshness, one workflow/read-state model, and a persistent
  Daily Research Cycle orchestration.
- Consolidation of duplicated calculations and orchestration paths behind tests,
  as sequenced in [CONSOLIDATION_ROADMAP.md](CONSOLIDATION_ROADMAP.md).
- Read-only architecture inventory, documentation, and static tooling (this
  phase, Phase 29A).

## Explicitly Deferred Scope

- **Milestone 6 — Intraday / near-real-time** operation is deferred until the
  daily system is reliable.
- **Milestone 7 — Controlled execution** (paper-order creation, paper execution,
  reconciliation, broker integration, approved automation) is deferred until all
  prior milestones are stable.
- **Milestone 5 — Paid-data acquisition** (including historical analyst
  revisions) is a supporting track, gated on point-in-time and cost criteria;
  it is not the main application objective.
- No Create Orders, order execution, or automation is in scope in any current
  phase.
