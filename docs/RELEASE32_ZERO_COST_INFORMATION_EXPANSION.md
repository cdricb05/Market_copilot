# Release 32 — Zero-Cost Information Expansion

> The Release-32 programme: what it expands, the research funnel it is bounded
> by, the artifacts it must produce, and the invariants that gate it.
>
> Related canonical documents:
> [PNL_OPPORTUNITY_FRONTIER.md](PNL_OPPORTUNITY_FRONTIER.md) ·
> [STRATEGY_SLEEVE_CONTRACT.md](STRATEGY_SLEEVE_CONTRACT.md) ·
> [DAILY_MULTI_ASSET_GOVERNANCE.md](DAILY_MULTI_ASSET_GOVERNANCE.md) ·
> [INFORMATION_PURCHASE_GATE.md](INFORMATION_PURCHASE_GATE.md)

## Base

    branch   stage19-controlled-rebalance
    base     0afb8863  (Release 31)
             59eaa05   (Release 31 closeout: research-card rendering repair)

Release 31 is committed and closed. Release 32 does not rerun it and does not
mix with it.

## What expands

Three dimensions at once:

1. **Information state** — beyond price-derived equity features.
2. **Prediction target** — beyond single-stock cross-sectional ranking.
3. **Asset / strategy opportunity set** — beyond S&P 500 equities.

**Release 32 spends zero money.** Priority order for information: existing
owned data → official public data → free public data with defensible terms →
no paid data.

## Phase map

| Phase | Deliverable |
|---|---|
| 1 | Zero-cost / owned information inventory → `data_source_registry.json` |
| 2 | Canonical `InformationState` with full PIT metadata |
| 3 | Six strategy sleeves researched under one contract |
| 4 | Common economic judge |
| 5 | Bounded research funnel |
| 6 | PnL Opportunity Frontier |
| 7 | Information Purchase Gate |
| 8 | Daily Multi-Asset Governance contract |
| 9 | UI — Research page frontier panel; System/Audit integrity |
| 10 | Read-only API `GET /v1/research/pnl-opportunity-frontier` |
| 11 | Actual research execution (resumable, idempotent, persisted per candidate) |

## Phase 1 — information inventory

For every source, record:

    source_id            provider              economic_state
    raw fields           asset coverage        start date / end date
    frequency            publication timestamp market timestamp
    revisionable?        vintage support?      publication lag
    PIT semantics        inactive/delisted relevance
    licensing / terms    data quality          current freshness
    acquisition cost     research admissibility

Owned / existing candidates include Norgate market histories, already-owned
EODHD, the existing S&P 500 PIT history, existing fundamentals, existing
SEC/PIT data, the existing event fabric, existing volatility/price features,
and existing benchmark/index history.

Official public zero-cost candidates include Cboe volatility-index history,
FRED, ALFRED where vintages are required, US Treasury market/rate data, CFTC
Commitments of Traders, and SEC filing/event history. Other official sources
only after source and terms validation.

**Strategy logic is never built from arbitrary scraped websites.**

## Point-in-time rule for macro data

Latest revised macro history is **not** what the market knew historically.

Where macro data are revisionable, use legitimate vintage information (ALFRED,
or another proven publication history). Where vintage or release-time evidence
is unavailable, the correct outcome is:

    MACRO_PIT_BLOCKED

Market prices and rates are themselves traded observations and do not require
macroeconomic vintage reconstruction — but they still require correct
timestamps.

Note the known ceiling recorded from Stage 15B: ALFRED vintages effectively cap
around 2000, which bounds how far back a vintage-safe macro sleeve can reach.
That bound is reported, not worked around.

## Phase 2 — canonical information state

Every observation must answer:

    WHAT was observed?
    WHEN was it observed?
    WHEN did it become public?
    WHICH market did it describe?
    WHEN could a strategy legitimately act on it?
    WAS it later revised?
    IS the stored value PIT-safe?

At minimum, these are distinct fields and never collapsed:

    observed_at    published_at    effective_at
    eligible_for_decision_at       source_timestamp    ingestion_timestamp

`eligible_for_decision_at` is the one that prevents future leakage through
daily joins, and it is the one most easily lost by a convenient merge.

## Phase 5 — the bounded funnel

No unlimited grid search. Budgets are ceilings, not targets — they are not
consumed merely because they exist.

**Screening** — max **8** transparent hypotheses per new sleeve. No complex ML.
Purpose: does the mechanism show any economic evidence at all?

**Qualification** — max **3** model families per sleeve, **8** configurations
per family, **24** per sleeve, **120** total across all new sleeves.

**Novel / refinement** — only for sleeves that survive screening and
qualification. Max **12** specs per sleeve, **60** total, refinement depth
**2**.

**Lockbox** — max **2** finalists per sleeve, **12** total. One access per
frozen finalist. **No retuning after lockbox.**

## Evidence partition

    DISCOVERY → VALIDATION → LOCKBOX → TRUE_FORWARD

Chronological ordering only. Purge and embargo appropriate to the target
horizon. No random train/test split for market time series. Never normalise
using the lockbox; never retune from it.

## Multiple testing

Every executed hypothesis stays in the denominator — rejected and failed
candidates included. Controls: BH/FDR, SPA / Reality-Check-style inference,
paired and block bootstrap, exact matched-date comparison.

A significant **negative** result is not alpha. Direction is explicit.

## Package structure

    alpha_agent/r32/__init__.py
    alpha_agent/r32/contract.py
    alpha_agent/r32/data_registry.py
    alpha_agent/r32/information_state.py
    alpha_agent/r32/sleeve_contract.py
    alpha_agent/r32/judge.py
    alpha_agent/r32/opportunity_frontier.py
    alpha_agent/r32/purchase_gate.py
    alpha_agent/r32/daily_governance.py
    alpha_agent/r32/campaign.py

    alpha_agent/r32/sleeves/
        equity_selection_baseline.py
        equity_beta_timing.py
        sector_rotation.py
        cross_asset_trend.py
        volatility_regime.py
        event_driven.py

    scripts/run_release32_pnl_opportunity_frontier.py
    api/pnl_opportunity_frontier.py

Existing canonical owners are reused, never duplicated: eligible market/session
date, price panel, market data, data freshness, return forecast, zero-base
allocation, covariance/risk, event fabric, material information, holding
opportunity cost, portfolio reassessment, reallocation proposal, portfolio
decision, NAV, workflow state, research evidence, candidate registry, lockbox,
multiple testing, forward evidence.

New owners are created only for genuinely new concepts: multi-source
information state, strategy sleeve contract, PnL opportunity frontier,
information purchase gate, daily multi-asset governance contract.

## Phase 10 — read-only API

    GET /v1/research/pnl-opportunity-frontier

Authenticated, read-only, exposing authoritative backend artifacts only. No GET
writes. No research calculation in the browser.

Sections: `campaign`, `information_state`, `data_sources`, `sleeve_registry`,
`sleeve_results`, `opportunity_frontier`, `purchase_gate`,
`daily_governance_contract`, `terminal_verdict`, `safety`.

The safety block states explicitly:

    creates_orders = false                creates_proposals = false
    creates_portfolio_decisions = false   mutates_holdings = false
    mutates_cash = false                  promotes_models = false
    activates_models = false              production_allocator_changed = false

Every new GET route is registered in **both** registries of
`docs/architecture/system_inventory.json` — `modules` *and* `route_ownership`.
Release 31 registered only the first and broke the route-ownership contract.

## Phase 9 — UI

The Research page gains a **PnL Opportunity Frontier** panel: sleeve, state,
best evidence, net economics, risk, correlation/diversification, evidence
state, data blocker, purchase-gate state.

System/Audit shows data freshness, source integrity, PIT blockers, campaign
state.

**Today remains the operator portfolio page.** Because production multi-asset
allocation is not live, no fake multi-asset actions appear there. A compact
read-only future-governance status may appear only if clearly labelled:

    RESEARCH / ARCHITECTURE READY — NOT OPERATIONAL

No frontend research math. No activation buttons. No orders.

UI wording follows the canonical Phase-27B.6 contract: paper orders are real;
only live brokerage orders are structurally disabled. The canonical badge is
`NO LIVE BROKER ORDERS`. `>NO LIVE ORDERS</span>` and `>ORDERS DISABLED<` are
refused.

## Artifacts

Persisted under `D:\Stock_Prediction_app_data\pnl_opportunity_frontier\r32\`:

    release32_contract.json           data_source_registry.json
    information_state_manifest.json   strategy_sleeve_registry.json

    sleeve_equity_selection.json      sleeve_equity_beta_timing.json
    sleeve_sector_rotation.json       sleeve_cross_asset_trend.json
    sleeve_volatility_regime.json     sleeve_event_driven.json

    candidate_registry.json           multiple_testing_results.json
    lockbox_finalists.json            lockbox_results.json

    pnl_opportunity_frontier.json     risk_cluster_map.json
    information_purchase_frontier.json
    daily_multi_asset_governance_contract_v2.json
    final_verdict.json

All immutable evidence artifacts carry hashes.

`daily_multi_asset_governance_contract.json` (schema 1) remains on disk,
frozen and superseded: it declared invented turnover budget values. Schema 2
declares the same three budget CONCEPTS with null values, a `NOT_CALIBRATED`
state and a named future value owner. No verdict, frontier or sleeve number
reads the governance artifact, which is why the correction is an artifact
supersession rather than a new campaign.

## Architecture audit — blocking invariants

1. one Strategy Sleeve contract owner
2. one Information State owner
3. one Opportunity Frontier owner
4. one Information Purchase Gate owner
5. one Daily Multi-Asset Governance contract owner
6. no second portfolio optimizer
7. no second covariance owner
8. no second HOC owner
9. sleeves cannot write capital allocation
10. sleeves cannot create proposals
11. sleeves cannot create orders
12. the Release-31 equity-selection result is reused, not rerun
13. PIT timestamps are required for external state
14. revised macro cannot enter PIT history silently
15. current sector membership cannot be backfilled
16. ETF history cannot exist pre-inception
17. common-overlap comparison enforced
18. lockbox inaccessible to selection
19. all executed hypotheses remain in the denominator
20. a significant negative result cannot be labelled alpha
21. no automatic sleeve activation
22. no automatic model promotion
23. no production portfolio mutation
24. no external reference links as features
25. GDELT text cannot become alpha silently
26. current analyst snapshots cannot become historical revisions
27. the purchase gate cannot approve data without PIT, coverage and licensing
28. daily reassessment is not automatic trading
29. a closed-market target delta remains pending, never silently dropped
30. no unrelated-instrument hedge substitution without a validated hedge policy
31. multi-asset NAV has exactly one declared future authoritative owner
32. asset count cannot be presented as risk diversification
33. event-driven reassessment reuses the existing event fabric
34. scheduled and event-driven governance share one orchestration contract
35. daily / weekly / monthly turnover budgets are explicit future-governance
    concepts, declared rather than implied
36. cash is a valid opportunity and a valid winner
37. stale data fails closed
38. sleeve exhaustion stops same-information search
39. no N+1 hidden research campaign after exhaustion
40. Release 32 remains production read-only

Invariants 28–40 arrived after the first 27 were persisted; they are merged
here as the single authoritative list. Each is enforced by
`scripts/audit_architecture.py` and carries at least one **negative probe** — a
test that deliberately violates the invariant and proves the guard fires.
A guard never shown to fail is not proven; Release 31 shipped two checks that
reported OK for work they never did, which is why this rule exists.

## Production safety

Release 32 is production read-only. It does not run Daily Close or DRC, create
a portfolio decision, proposal, approval or order, rebalance, modify holdings
or cash, promote a model, activate a sleeve, enable automation, change broker
integration, restart production, stop information collection, or stop Telegram
control.

## Result — campaign `r32_pnl_opportunity_frontier_v4`

**Terminal verdict: `R32_ZERO_COST_OPPORTUNITY_FRONTIER_EXHAUSTED`.**
Secondary: `INFORMATION_SAMPLE_PRIORITY_IDENTIFIED`. Zero sleeves qualified.
Nothing was activated, allocated, proposed or purchased.

### Phase 1 — what the information inventory actually found

The subscription is not equities-only: Continuous Futures, Forex Spot, Cash
Commodities, US Indices, World Indices and Economic are all owned. That is a
genuinely large zero-cost cross-asset information set, and it made five of the
six sleeves testable at all.

The constraint is not breadth, it is **admissibility**. Classified by measured
change-day fingerprint rather than by assertion:

| database | symbols | PIT market observable | revised, not PIT | coverage limited |
|---|---:|---:|---:|---:|
| Economic | 144 | 27 | **106** | 11 |
| US Indices | 1609 | 1604 | 0 | 5 |
| Forex Spot | 57 | 57 | 0 | 0 |
| Cash Commodities | 15 | 15 | 0 | 0 |
| World Indices | 31 | 31 | 0 | 0 |
| Continuous Futures | 1 | 1 | 0 | 0 |

**117 of 144 owned macro series are inadmissible as history.** Every statistical
release in that database changes value on the *first business day of the period
it measures* — CPI for month M appears on day one of month M, GDP on day one of
its quarter — roughly six weeks before publication, and carrying today's revised
vintage rather than the number that was printed. Measured, not assumed: 135 of
138 observed value changes since 2015 land on day ≤ 3, and GDP flips on 1 Jan /
1 Apr / 1 Jul / 1 Oct.

Market observables — yields, index levels, volatility indices, FX, commodity
indices — change on nearly every session because they *are* the market's
real-time opinion, and are admissible.

Two further measured limitations: vendor metadata overstates availability
(`$USTSY` advertises 1990 and delivers 2022), and the owned event stores are
**synthetic test fixtures** (`provider_id: synthetic_test`, tickers `S000`) with
SEC filing timestamps covering 63 tickers.

### Sleeve results

Every sleeve was measured on its own lockbox, then re-scored on the shared
decision calendar for cross-sleeve comparison. Denominator: **104 executed
hypotheses**; 10 lockbox results; **0 survive BH/FDR at q = 0.10**.

| sleeve | net p.a. | Sharpe | max DD | cash | t vs cash | t vs vol-matched | state |
|---|---:|---:|---:|---:|---:|---:|---|
| VOLATILITY_RISK_REGIME | 11.75 % | 0.82 | −25.1 % | 10.6 % | +2.70 | **−1.03** | REJECTED |
| EQUITY_BETA_TIMING | 10.42 % | 0.84 | −26.0 % | 19.0 % | +2.64 | **−1.42** | REJECTED |
| SECTOR_ROTATION | 12.98 % | 0.82 | −23.1 % | 0.0 % | +2.34 | **−0.93** | REJECTED |
| EVENT_DRIVEN | 4.57 % | 1.20 | −3.8 % | 0.0 % | +1.63 | **−1.09** | REJECTED |
| CROSS_ASSET_TREND | 4.03 % | 0.41 | −22.8 % | 6.9 % | +0.34 | **−1.83** | REJECTED |
| EQUITY_SELECTION (control) | — | — | — | — | — | — | inherited from R31 |

Every sleeve beats cash. **Not one beats a volatility-matched mix of the
benchmark and cash**, which is the whole finding: these strategies deliver
equity exposure, not skill.

### The clustering result

On the shared calendar, three sleeves form a **single latent risk cluster**
(threshold 0.70):

    EQUITY_BETA_TIMING ↔ VOLATILITY_RISK_REGIME    +0.91
    EQUITY_BETA_TIMING ↔ SECTOR_ROTATION           +0.88
    SECTOR_ROTATION    ↔ VOLATILITY_RISK_REGIME    +0.78

Three differently-named strategies, built from different instruments and
different state variables, are one bet: long equity beta. Only EVENT_DRIVEN is
genuinely uncorrelated (−0.01 to +0.33). This is what "asset labels do not equal
diversification" means in numbers, and a portfolio holding all three would have
believed it was three times as diversified as it was.

### Superseded campaigns

`v1` is `SUPERSEDED_EXPERIMENTAL_DESIGN`; `v2` and `v3` are
`SUPERSEDED_INCOMPLETE_REPORTING`. All are preserved on disk. Their defects and
corrections are recorded in `alpha_agent/r32/contract.py`:

1. **v1** ranked and FDR-corrected on excess over **cash**, let the
   `always_invested_control` reach the lockbox, and read the inherited R31
   verdict from a key that does not exist.
2. **v2** stripped sleeve return paths before returning them, so the correlation
   map and latent clusters were empty — which reads as "nothing is related".
3. **v3** gave every panel its own decision calendar, so no two sleeves shared a
   single decision date and correlation stayed unmeasurable.

Because the judge behaviour hash is unchanged between v2, v3 and v4, every
per-candidate number reproduces exactly across them — which is why the reporting
defects were superseded rather than rewritten in place.

## Terminal verdicts

Exactly one primary verdict:

    R32_MULTIPLE_SLEEVES_QUALIFIED
    R32_SINGLE_SLEEVE_QUALIFIED
    R32_ZERO_COST_OPPORTUNITY_FRONTIER_EXHAUSTED
    R32_POINT_IN_TIME_EVIDENCE_BLOCKED
    R32_RESOURCE_BUDGET_EXHAUSTED
    R32_DATA_SOURCE_BLOCKED

Secondary, where applicable: `READY_FOR_R33_MULTI_ASSET_INTEGRATION` or
`INFORMATION_SAMPLE_PRIORITY_IDENTIFIED`.
