# Release 50 — Multi-Asset Operational Capital Manager

**One capital pool · one multi-asset NAV · one position contract · one investability
registry · one cross-asset risk state · one opportunity frontier · one zero-base
target · one best feasible target · one governed paper reallocation path · one
decision snapshot**

Paper-only. Preview-first. Manual review mandatory. No broker, no live order, no
automation, no model promotion. Release 50 changed no Alpha science, promoted no
research strategy, and touched no holding, NAV, order, fill, approval or historical
evidence during development.

---

## 1. The question, and the honest answer

The canonical question is *if all investable capital were cash now, what feasible
portfolio should we own across every validated operational asset / strategy sleeve?*
Before Release 50 the operational manager could only answer it with US cash
equities: a holding was `quantity x adjusted close`, a mark came from one equity
transport, a target was a list of tickers, and nothing in the operational path knew
what a point value, an initial margin or a currency was.

Release 50 makes the operational path **asset-agnostic** — every sleeve that can be
made safe competes for the same capital through one frontier, one constraint owner,
one proposal, one approval, one execution path and one NAV — and it does so
**without inventing Alpha**. The production result is stated plainly:

| Fact | Value |
|---|---|
| Sleeves inventoried | 10 (US equities, cash, equity-index / rates / commodity / volatility / FX / international-index / crypto futures, FX spot, event-macro lanes) |
| Capital-eligible before R50 | US equities, cash |
| Capital-eligible after R50 (production) | US equities, cash |
| Newly activated in production | none |
| Non-equity sleeves fully implemented for operation | all eight; every plumbing capability is TRUE |
| The ONE remaining blocker, every non-equity sleeve | `NO_APPROVED_OPERATIONAL_SIGNAL` — an evidence gate, not a code gap |

Every non-equity sleeve now carries mark, USD valuation, risk, cost, liquidity,
capacity, position accounting, paper execution and reconciliation. What none of them
carries is an operational signal approved through the evidence-gated model
recalibration cycle: the only candidates are Release-46 prospective challengers
(TRUE_FORWARD tournament, verdict `TOO_EARLY` / `FORWARD_PENDING`, no matured cohort
through the frozen gates), adopted prior-release research shadows, or historical
results that were never frozen prospectively (R36 / R43 FX carry). The one crypto edge
with historical support was shown by Release 42 to earn less than remunerated cash
collateral. Granting approval inside a development release would be an automatic
promotion, which the charter and the R46 contract forbid. So the registry says
exactly that, per sleeve, with the evidence, and the moment an approval is recorded
the sleeve competes for capital with no further code.

The full multi-asset path is **proven hermetically** with an injected approval
(the registry's `approvals=` seam, never a production input): rates and index
futures enter the frontier, receive target capital, are re-optimised under the
cross-asset caps, clear (or fail) the frozen switching hurdle, are approved and
confirmed through the unchanged Stage-18/19 gates, fill under futures semantics at
the instrument's own next settlement, reconcile in the ONE NAV with collateral, and
freeze both decision-evidence paths on USD marks.

## 2. Owners (one per business concept)

| Concept | Owner | Route |
|---|---|---|
| Position contract + valuation semantics | `engine/instrument_contract.py` (pure) | — |
| Owned non-equity reference data + marks | `api/market_reference_data.py` | — |
| Investability registry | `api/investability_registry.py` | `GET /v1/operations/investability-registry` |
| ONE NAV (extended) | `api/paper_trading_desk.book_nav` | — |
| Capital pool | `api/capital_pool.py` | `GET /v1/operations/capital-pool` |
| Cross-asset risk | `engine/cross_asset_risk.py` → `api/cross_asset_risk.py` | `GET /v1/operations/cross-asset-risk` |
| Canonical drawdown | `api/paper_trading_desk.current_drawdown` | (consumed everywhere) |
| Opportunity frontier | `engine/opportunity_frontier.py` → `api/opportunity_frontier.py` | `GET /v1/operations/opportunity-frontier` |
| Zero-base target | `engine/zero_base_allocator.py` (extended) | `GET /v1/operations/zero-base-target` |
| Feasible target + switching | `engine/constrained_reallocation.py` (extended) | `GET /v1/operations/constrained-reallocation` |
| Governed proposal | `engine/reallocation_proposal.py` (extended) | `GET /v1/operations/reallocation-proposal` |
| Paper execution | `api/rebalance_execution.py` + desk settlement (extended) | `GET /v1/operations/rebalance` |
| Decision evidence | `api/portfolio_decision_outcome.py` (USD marks) | `GET /v1/operations/portfolio-decision-outcomes` |
| Decision snapshot / fan-out | `api/decision_snapshot.py` | `GET /v1/operations/decision-snapshot` |

## 3. The position contract

A row without an `instrument` block IS a US cash equity — multiplier 1, USD, fully
cash settled — so every historical ledger row values exactly as before and no ledger
is rewritten. A future is **never** valued like a share:

```
notional_usd        = quantity x mark x point value x fx
market_value_usd    = quantity x (mark - entry mark) x point value x fx   (unrealised variation; the NAV contribution)
collateral_usd      = quantity x initial margin x fx                     (encumbered cash, NOT a cash outflow)
capital_usage_usd   = collateral_usd
exposure_weight     = notional_usd / NAV
```

NAV = cash + Σ market_value_usd. Free (available) capital = cash − collateral. FX
comes from the owned Forex Spot database (EURUSD multiplies, USDJPY divides); a
non-USD instrument without a rate is a named gap, never 1.0. Long-only is declared
(`SHORT_EXPOSURE_SUPPORTED = False`). Cash earns the declared zero on every path,
including collateral, so a collateralised sleeve can never "beat" a fully-paid one
by fiat.

## 4. Marks, settlement, execution convention

The desk's ONE mark owner routes an owned non-equity symbol (`&ZN`, `EURUSD`) to the
owned reference-data seam and an equity to the owned EODHD transport; the FX pair of a
non-USD instrument travels in the same store. The ONE settlement engine fills by
instrument semantics: an equity exactly as before; a future opens for its
transaction cost only and realises `units x (price − entry) x point value x fx` on
close, with the entry replayed from the immutable fills.

ONE governed, asset-aware execution convention — `NEXT_SESSION_SETTLEMENT`: the
instrument's own first completed daily settlement strictly after the marks known at
approval. For a US equity it is exactly `NEXT_CLOSE`, unchanged. Decision timestamp,
eligible session, mark/fill convention and settlement/collateral semantics are
declared once in `engine.instrument_contract.EXECUTION_CONVENTION_DOC`.

## 5. Score comparability

One normalised `opportunity_score` in [0, 1] with an explicit `score_basis`:
`OPERATIONAL_MODEL_COMBINED_PERCENTILE` (the approved equity model, unchanged),
`OPERATIONAL_SLEEVE_NORMALISED_RANK` (an APPROVED non-equity sleeve's own signal,
rank-normalised within its opportunity set), `CASH_DECLARED_ZERO`, or
`NONE_RESEARCH_ONLY` (listed, unscored, ineligible). `expected_return` is populated
only from a calibrated forecast and is otherwise `NOT_CALIBRATED`; a research
statistic never becomes an expected return. The declared limitation: two
rank-normalised scores assume comparable opportunity dispersion across sleeves, which
is why every non-equity name clears the same entry / exit and switching rules and a
zero-signal instrument is never a residual capital sink (cash is).

## 6. Cross-asset constraints (all RESHAPING, never blockers)

`ASSET_CLASS_WEIGHT_CAP`, `SLEEVE_WEIGHT_CAP`, `CURRENCY_EXPOSURE_CAP`,
`COLLATERAL_USAGE_CAP`, `UNIT_GRANULARITY_AT_NAV`, plus gross / net exposure ≤ 100 %.
Declared once in `engine.constrained_reallocation`; the proposal kernel and the
zero-base allocator reuse the same definitions. The approved equity sleeve and cash
are uncapped (1.0), so an equity-only book solves byte-for-byte as before; a
non-equity class or sleeve is capped at a declared quarter of NAV, non-USD notional at
20 %, futures collateral at 25 %. **Unit granularity is a real capacity limit, found
live:** at the book's NAV ($99,383) and a 10 % name cap every full-size rates
contract ($106k–$241k unit notional) is `UNIT_NOTIONAL_EXCEEDS_NAME_CAP_AT_NAV`;
micro contracts are the executable ones at this book size.

## 7. Drawdown ownership resolved

`api.paper_trading_desk.current_drawdown` is the ONE current operational drawdown
(the corporate-action-corrected forward-performance ledger; peak includes the initial
capital). The Daily Close forward monitor no longer runs its own peak-to-trough loop
(whose peak started at the first NAV row — a second definition), the Portfolio
analytics chart reads `current_rows`, the portfolio state names the owner. Research
variants (research paper books, the per-constituent 252-day diagnostic) stay
explicitly labelled and are never this number.

## 8. The decision snapshot

ONE identity fingerprint over every store that can change a decision (market date,
desk ledgers and marks, corporate actions, signal and research-cycle roots,
proposal / decision / order-plan / outcome ledgers, the store-root environment); ONE
composition per identity; every normal route (`operational-book`, `daily-close`,
`workflow-state`, `portfolio-state`, `constrained-reallocation`, `rebalance`,
`operator-presentation`, `capital-pool`) served from it. A changed identity
regenerates from the canonical owners — not a stale cache. The snapshot computes no
number of its own. Measured numbers are in `PROJECT_STATE.md` and the handoff
(`performance_before/after.json`, `read_fanout_before/after.json`).

## 9. UI (R49 preserved)

Today keeps its four sections; the snapshot gains one allocation line listing only
present asset classes. Portfolio Overview shows the allocation of each column;
Reallocation shows the target's sleeves; Audit & Details gains the registry card
(every sleeve, its capability verdicts and blocker). Nothing renders `FX 0%`.

## 10. Safety

Paper only · preview only · manual review · two manual gates unchanged · no broker ·
no automation · no model promotion · no research auto-promotion · no purchase.
`scripts/audit_architecture.py --strict` gains `check_release50_multi_asset` (55
blocking invariants). `tests/test_release50_multi_asset_operational_manager.py`
covers scenarios A–S.
