# GLOBAL MULTI-ASSET ALPHA FRONTIER

**GLOBAL_MULTI_ASSET_FRONTIER = COMPLETE** (state `COMPLETE`)

Rendered from `global_multi_asset_frontier.json` (alpha_agent.r59.global_frontier). The artifact is authoritative; this page is a view of it.

> If all research capacity and investable capital were uncommitted today, where across all asset classes is the strongest after-cost P&L opportunity?

Owner identities reconciled: 129 claimed of 129 held; unreconciled 0; owner conflicts 0; invalid declarations 0.

## GLOBAL TOP 10

| Rank | Candidate | Asset | State | After-cost evidence | Forward evidence | Remaining gate | Next action |
|---|---|---|---|---|---|---|---|
| 1 | `GC_EQIDX_SPY_REVERSED_PUT_CALL_SKEW` (0.6137) | EQUITY_INDEX | FORWARD_PENDING | net +26.00 %/yr, t 2.62 | 0 emitted / 0 matured / 0 of 40 effective | FORWARD_EVIDENCE_FLOOR | ACCRUE_FORWARD_EVIDENCE |
| 2 | `GC_FX_XS_CARRY_DATED_CONTRACT` (0.4561) | FX | TRUE_FORWARD | net +2.28 %/yr, t 2.49 | 22 emitted / 5 matured / 1 of 40 effective | UNTOUCHED_CONFIRMATION | ACCRUE_FORWARD_EVIDENCE |
| 3 | `GC_XA_R39_R40_MACHINE_SHADOWS` (0.3043) | CROSS_ASSET | TRUE_FORWARD | net +3.65 %/yr, t 2.58 | 2 emitted / 0 matured / 0 of 24 effective | MULTIPLICITY | ACCRUE_FORWARD_EVIDENCE |
| 4 | `GC_COMMODITY_CURVE_CARRY` (0.3017) | COMMODITIES | TRUE_FORWARD | net +2.80 %/yr, t 1.6 | 26 emitted / 7 matured / 1 of 40 effective | OVERTURN_FAILED_UNTOUCHED_EVIDENCE | ACCRUE_FORWARD_EVIDENCE |
| 5 | `GC_USEQ_INCUMBENT_FUNDAMENTAL_MOMENTUM` (0.2925) | US_EQUITY | LIVE_CANDIDATE | net +2.73 %/yr, t 1.13 | 205 emitted / 38 matured / 12 of 24 effective | UNTOUCHED_CONFIRMATION | CONTINUE_OPERATIONAL_OBSERVATION |
| 6 | `GC_USEQ_SINGLE_NAME_OPTIONS_INFORMED_TRADING` (0.2875) | US_EQUITY | HUMAN_GATE | net -, t None | 0 emitted / 0 matured / 0 of 24 effective | UNTOUCHED_CONFIRMATION | DATA_PURCHASE_DECISION |
| 7 | `GC_EQIDX_SCHEDULED_EVENT_PREMIUM` (0.2849) | EQUITY_INDEX | TRUE_FORWARD | net +2.46 %/yr, t 3.37 | 7 emitted / 3 matured / 3 of 60 effective | UNTOUCHED_CONFIRMATION | ACCRUE_FORWARD_EVIDENCE |
| 8 | `GC_XA_FUTURES_TREND_AND_MOMENTUM` (0.2589) | CROSS_ASSET | TRUE_FORWARD | net -0.70 %/yr, t 2.52 | 27 emitted / 0 matured / 0 of 24 effective | UNTOUCHED_CONFIRMATION | ACCRUE_FORWARD_EVIDENCE |
| 9 | `GC_XA_ORTHOGONAL_MULTI_ASSET_COMPOSITE` (0.2417) | CROSS_ASSET | BLOCKED | net -, t None | 0 emitted / 0 matured / 0 of 24 effective | UNTOUCHED_CONFIRMATION | RESOLVE_DATA_HOLD |
| 10 | `GC_USEQ_LIQUIDITY_AND_SEASONALITY_FORWARD` (0.2328) | US_EQUITY | TRUE_FORWARD | net -, t 1.65 | 26 emitted / 0 matured / 0 of 24 effective | UNTOUCHED_CONFIRMATION | ACCRUE_FORWARD_EVIDENCE |

## BEST BY ASSET CLASS

- **US_EQUITY** = `GC_USEQ_INCUMBENT_FUNDAMENTAL_MOMENTUM` (global #5) - state LIVE_CANDIDATE
- **EQUITY_INDEX** = `GC_EQIDX_SPY_REVERSED_PUT_CALL_SKEW` (global #1) - state TRUE_FORWARD
- **FX** = `GC_FX_XS_CARRY_DATED_CONTRACT` (global #2) - state TRUE_FORWARD
- **RATES** = `GC_RATES_COPPER_GOLD_LEAD` (global #14) - state TRUE_FORWARD
- **COMMODITIES** = `GC_COMMODITY_CURVE_CARRY` (global #4) - state TRUE_FORWARD
- **VOLATILITY** = `GC_VOL_VX_TERM_STRUCTURE_CARRY_FORWARD` (global #23) - state TRUE_FORWARD
- **CREDIT** = `GC_CREDIT_SPREAD_REGIME_FORWARD` (global #21) - state TRUE_FORWARD
- **CRYPTO** = none open - state BLOCKED; data gaps: no point-in-time listing record for a broad crypto universe (two majors only); the declared venue answers HTTP 451 and its public funding archive is monthly with a ~24-day lag
- **CROSS_ASSET** = `GC_XA_R39_R40_MACHINE_SHADOWS` (global #3) - state TRUE_FORWARD

## WHAT THE OLD FRONTIER MISSED

- Owner identities the mechanism frontier never read: 106
- Open candidates previously omitted: `GC_XA_R39_R40_MACHINE_SHADOWS` (CROSS_ASSET, #3), `GC_COMMODITY_CURVE_CARRY` (COMMODITIES, #4), `GC_USEQ_INCUMBENT_FUNDAMENTAL_MOMENTUM` (US_EQUITY, #5), `GC_XA_FUTURES_TREND_AND_MOMENTUM` (CROSS_ASSET, #8), `GC_USEQ_LIQUIDITY_AND_SEASONALITY_FORWARD` (US_EQUITY, #10), `GC_XA_FUTURES_VALUE_5Y` (CROSS_ASSET, #11), `GC_COMMODITY_XS_SKEWNESS` (COMMODITIES, #12), `GC_RATES_COPPER_GOLD_LEAD` (RATES, #14), `GC_RATES_MACRO_SURPRISE_REPRICING` (RATES, #15), `GC_RATES_CURVE_RV_AND_TERM_CARRY` (RATES, #17), `GC_USEQ_SHORT_VOLUME_PRESSURE` (US_EQUITY, #18), `GC_COMMODITY_XS_MOMENTUM` (COMMODITIES, #19), `GC_CREDIT_SPREAD_REGIME_FORWARD` (CREDIT, #21), `GC_FX_XS_MOMENTUM` (FX, #22), `GC_VOL_VX_TERM_STRUCTURE_CARRY_FORWARD` (VOLATILITY, #23), `GC_EQIDX_XS_RELATIVE_MOMENTUM` (EQUITY_INDEX, #24), `GC_EQIDX_INDEX_TREND_TIMING` (EQUITY_INDEX, #25), `GC_USEQ_DISCLOSURE_INTENSITY` (US_EQUITY, #26), `GC_USEQ_FREE_CASH_FLOW_QUALITY` (US_EQUITY, #27), `GC_USEQ_XS_PRICE_STATE_FORWARD` (US_EQUITY, #28), `GC_XA_COT_POSITIONING` (CROSS_ASSET, #29), `GC_USEQ_INSIDER_FLOW` (US_EQUITY, #30)
- Asset classes previously underrepresented: US_EQUITY (LISTED_BUT_NOT_RANKED), EQUITY_INDEX (LISTED_BUT_NOT_RANKED), FX (LISTED_BUT_NOT_RANKED), RATES (ABSENT), COMMODITIES (ABSENT), VOLATILITY (ABSENT), CREDIT (ABSENT), CRYPTO (ABSENT), CROSS_ASSET (LISTED_BUT_NOT_RANKED)
- Rank materially changed: YES
  - `REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1`: old rank unranked -> `GC_EQIDX_SPY_REVERSED_PUT_CALL_SKEW` global #1
  - `r51_fx_xs_carry_cip`: old rank unranked -> `GC_FX_XS_CARRY_DATED_CONTRACT` global #2

## RECORDED HUMAN DECISIONS

- `ALPHA_RECOVERY_FX_CARRY_CADENCE_H1_F9B1ACA7` on `GC_FX_XS_CARRY_DATED_CONTRACT` (2026-09-14): **AUTHORISED** -> AUTHORISED_AND_EXECUTED. Authorised for prospective TRUE_FORWARD evidence collection of this exact frozen identity only; not model promotion, capital eligibility, portfolio allocation, portfolio mutation, an order or an execution instruction.
- `TREASURY_MONTH_END_ZN_POSTHOC_CONTROL` on `GC_RATES_MONTH_END_DURATION_LONG_HUMAN_JUDGEMENT` (2026-09-14): **DECLINED** -> CANDIDATE_CLOSED. Post-hoc control observed only as the control of a killed cell, below the 1.5 %/yr materiality floor, and an unacceptable rescue path.
- `SINGLE_NAME_OPTIONS_PURCHASE` on `GC_USEQ_SINGLE_NAME_OPTIONS_INFORMED_TRADING` (2026-09-14): **DEFERRED** -> RESURFACED. Currently ranked below higher-value global opportunities; nothing is purchased and the purchase resurfaces only if it becomes globally justified.

## AGENT-EXECUTABLE QUEUE (admitted, best first)


## ANSWERS

- IS SINGLE-NAME OPTIONS STILL THE BEST NEXT RESEARCH SPEND? **YES** - YES: no research action or human decision in the global top 5 scores higher than GC_USEQ_SINGLE_NAME_OPTIONS_INFORMED_TRADING (US_EQUITY, rank #6, score 0.2875, next action DATA_PURCHASE_DECISION); passive accrual ahead of it costs nothing (GC_EQIDX_SPY_REVERSED_PUT_CALL_SKEW, GC_FX_XS_CARRY_DATED_CONTRACT, GC_XA_R39_R40_MACHINE_SHADOWS, GC_COMMODITY_CURVE_CARRY, GC_USEQ_INCUMBENT_FUNDAMENTAL_MOMENTUM)
- IS FX CURRENTLY A TOP GLOBAL PRIORITY? **YES** ({'candidate_id': 'GC_FX_XS_CARRY_DATED_CONTRACT', 'global_rank': 2, 'score': 0.4561})
- FX versus the single-name options purchase: **STRONGER**
- Highest-value next action globally: `GC_USEQ_SINGLE_NAME_OPTIONS_INFORMED_TRADING` (DATA_PURCHASE_DECISION) - HUMAN PURCHASE DECISION (research/data_gaps/SINGLE_NAME_OPTIONS_PURCHASE_CASE.md): approve or decline a one-time Databento OPRA purchase capped at $400, then about twelve working days to build implied volatility from quotes and run a preregistered test.

## INVARIANTS

- GLOBAL_MULTI_ASSET_FRONTIER = YES (computed: state COMPLETE)
- ALL_REQUIRED_ASSET_CLASSES_PRESENT = YES (computed over the nine required classes)
- OLD_FORWARD_CANDIDATES_RECONCILED = YES (computed: every forward-owner identity is claimed)
- R46_CANDIDATES_NOT_SILENTLY_DROPPED = YES (computed over the R46 leaderboard and adopted continuation)
- R63_R64_CANDIDATES_RECONCILED = YES (computed over the R63 and R64 challenger records)
- TRUE_FORWARD_CANDIDATES_RECONCILED = YES (computed over the canonical registry and accrual projection)
- FX_CANDIDATES_RECONCILED = YES (computed over every FX-labelled owner identity)
- CREDIT_CANDIDATES_RECONCILED = YES (computed over every credit-labelled owner identity)
- VOLATILITY_CANDIDATES_RECONCILED = YES (computed over every volatility-labelled owner identity)
- EQUITY_HAS_NO_PRIORITY_BONUS = YES (signature check; perturbation proven in tests/test_alpha_agent_global_multi_asset_frontier.py)
- RECENT_MECHANISM_HAS_NO_PRIORITY_BONUS = YES (signature check; perturbation proven in tests/test_alpha_agent_global_multi_asset_frontier.py)
- EXECUTOR_AVAILABILITY_HAS_NO_PRIORITY_BONUS = YES (signature check; perturbation proven in tests/test_alpha_agent_global_multi_asset_frontier.py)
- GLOBAL_OPPORTUNITY_COST_REQUIRED = YES (computed; the mechanism frontier's gate is proven in tests/test_alpha_agent_global_multi_asset_frontier.py)
- PURCHASE_REQUIRES_GLOBAL_COMPARISON = YES (computed over every purchase decision)
- ASSET_CLASS_CANNOT_DISAPPEAR = YES (computed: every required class carries a state)
- WHOLE_ASSET_CLASS_NOT_EXHAUSTED_FROM_PARTIAL_FAILURES = YES (computed: EXHAUSTED only with a declared exhaustion and no open candidate)
- NO_SECOND_RESEARCH_MEMORY = YES (structural: this module holds no database and calls no memory writer; proven in tests/test_alpha_agent_global_multi_asset_frontier.py)
- NO_SECOND_FORWARD_LEDGER = YES (computed: the declaration carries no forward state; the module writes no ledger)
- NO_PORTFOLIO_MUTATION = YES (safety flags; no api/engine import, proven in tests/test_alpha_agent_global_multi_asset_frontier.py)

Research only. No purchase, subscription, registration, promotion, order, fill, capital allocation or backfill was performed by this frontier.
