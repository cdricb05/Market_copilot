# PREREGISTRATION - ORTHOGONAL MULTI-ASSET ALPHA COMPOSITE OF FROZEN SLEEVES

**Mechanism id:** `ORTHOGONAL_MULTI_ASSET_ALPHA_COMPOSITE_V1`
**Global candidate:** `GC_XA_ORTHOGONAL_MULTI_ASSET_COMPOSITE` (declared with this mechanism)
**Run:** ALPHA_COMPOSITE_STRIKE_SEP15_V1, branch `alpha-composite-strike-sep15-v1`, cut from
`alpha-strike-sep15-v2` 0c9c931.
**Catalog:** `research/alpha_agent/MECHANISM_FRONTIER.json` (data_version 1)
**Status:** FROZEN. Written and committed BEFORE any composite return, weight, covariance, correlation
between sleeves or composite statistic was computed. The sleeve inventory (section 2) read only the
canonical global frontier's declared metadata, owner labels, artifact structure and DATES. No sleeve
return path was loaded. Not edited after results exist.
**Data cost:** $0. No purchase, subscription, trial or purchase case.

---

## 0. THE QUESTION

Can the strongest already-frozen, economically distinct Alpha sleeves be combined into a materially
stronger after-cost portfolio than any one sleeve alone?

The claimed P&L mechanism is diversification across independent return sources: sleeves whose
positive expected returns come from different economic actors (hedgers, carry-seeking currency
investors, ...) have low return correlation, so an inverse-volatility, volatility-targeted combination
raises the Sharpe ratio and lets a modest, capped leverage turn it into a material excess return.

This is PORTFOLIO-SYNTHESIS evidence. It cannot erase selection already present inside a component
(for example a sign chosen post hoc); every component keeps its own evidence label.

## 1. INVENTORY SOURCE (owner, not a second frontier)

The inventory is read from the ONE canonical global multi-asset frontier,
`alpha_agent.r59.global_frontier`, artifact
`D:\Stock_Prediction_app_data\r59_autonomous_alpha\agent\global_multi_asset_frontier.json`
(generated 2026-09-15T20:25:37Z, artifact_hash `b79c8c3f0d62b1e1...`, state COMPLETE, 29 open ranked
candidates, 18 closed or control candidates). Nothing is re-ranked here. The executor re-derives the
inventory from the same owner at run time (section 3.3).

## 2. SLEEVE INVENTORY (frozen)

### 2.1 Eligibility criteria and their mechanical reading

| code | criterion (run brief) | mechanical reading on the canonical frontier candidate |
|---|---|---|
| E1 | an existing frozen identity/spec | `members` non-empty |
| E2 | not CLOSED / NO_EDGE / DATA_HOLD | `disposition` OPEN, `current_state` not BLOCKED/CLOSED/CONTROL, and the owner-derived `evidence_quality` is `GOOD_STRATEGY_*` (BAD_STRATEGY_COMPLETE_EVIDENCE = closed on untouched evidence; UNPROVEN = no qualifying history or an executed kill) |
| E3 | no paid-data dependency | `data_requirement` does not begin `PAID` |
| E4 | no current human purchase gate | `current_state` is not HUMAN_GATE and the next action is not DATA_PURCHASE_DECISION |
| E5 | a genuine economic signal | `mechanism_class` is not in the catalog's closed class list (`PRICE_STATE_TRANSFORMATION`) |
| E6 | a PIT-safe historical evidence owner | historical `pit` in PIT_TRUE / PIT_MARKET_OBSERVABLE / PIT_BY_DECLARED_LAG and a measured historical t |
| E7 | exact per-session or per-period path recoverable from the canonical owner without re-specification | a declared return-path owner (3.2) whose state is AVAILABLE |
| E8 | return semantics and costs identifiable | declared in the same return-path owner row |
| E9 | not the incumbent portfolio | `current_state` is not LIVE_CANDIDATE (the operational book) |
| E10 | not a duplicate of a selected sleeve | not this composite itself; mechanism class not already selected |

### 2.2 The inventory (global rank order)

| # | SLEEVE | ASSET CLASS | MECHANISM | FROZEN IDENTITY | HISTORICAL OWNER | RETURN PATH AVAILABLE | RETURN TYPE | COST OWNER | PIT STATUS | ELIGIBLE |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | `GC_EQIDX_SPY_REVERSED_PUT_CALL_SKEW` | EQUITY_INDEX | HEDGER_DEMAND_PRESSURE | `REVERSED_SPY_PUT_CALL_SKEW_H5` (identity 4e2d1027) | `alpha_agent.alpha_recovery.options_surface` / `reversed_skew` | YES, PER-PERIOD: non-overlapping 5-session decisions on two disjoint OPRA surfaces, 2022-09-09..2024-08-15 (confirmation) and 2024-09-10..2026-08-20 (discovery) - about 3.9 years | sign x SPY 5-session return, gross 1.0, net of 1 bp per side | options_surface cost ladder (1/2/5 bp per side) | PIT_MARKET_OBSERVABLE; **SIGN DISCOVERED POST HOC on 2024-2026**; 2022-2024 confirmation independent | **YES** |
| 2 | `GC_FX_XS_CARRY_DATED_CONTRACT` | FX | RISK_TRANSFER_PREMIUM | `ALPHA_RECOVERY_FX_CARRY_CADENCE_H1_F9B1ACA7` (record b4016c35, registered identity 436a1301) | `alpha_agent.alpha_recovery.cadence` (R63 scorer, R64 risk-controlled construction) | YES, PER-SESSION NET: `cadence.sleeve_series("FX_FUTURES\|XS\|1\|CARRY\|k5\|b0.25")`, stability blocks 1998-2027 | net futures excess return of the risk-controlled cadence-5 cross-sectional book | R64 per-instrument cost_bps_per_side | PIT_MARKET_OBSERVABLE; lockbox POST_SELECTION | **YES** |
| 3 | `GC_XA_R39_R40_MACHINE_SHADOWS` | CROSS_ASSET | PRICE_STATE_TRANSFORMATION | three distinct shadows: `shadow_wide_xs`, `shadow_carry_rule_xs`, `shadow_slot5_c39_fad367467c79` | `alpha_agent.r39` continuation | NO per-session path: the owner stores month-end Zone-C rows only (`wide_zone_c_streams.csv`, 140 rows 2016-12..2026-06); Zone B not stored | monthly net after cost vs risk-matched cash | R38 frozen cost | METHODOLOGY_BACKFILL | NO - E5 (closed class), E6 (backfill), E7/E8 (monthly only), and killed by the deflated Sharpe (MACHINE_REPRESENTATION_SEARCH) |
| 4 | `GC_COMMODITY_CURVE_CARRY` | COMMODITIES | RISK_TRANSFER_PREMIUM | `r46_3_comdty_curve_carry`, `R63/R64_COMMODITY_FUTURES_CARRY_H63_F37C04AF` | R38 / R63 / R64 | not declared (E2 fails first) | futures excess | R64 | PIT_MARKET_OBSERVABLE | NO - E2 BAD_STRATEGY_COMPLETE_EVIDENCE (COMMODITY_CURVE_CARRY_SPREADS; untouched FAILED; R64 NOT_ECONOMIC_UNDER_CONTROLS) |
| 5 | `GC_USEQ_INCUMBENT_FUNDAMENTAL_MOMENTUM` | US_EQUITY | PUBLIC_SLOW_DISCLOSURE | `us_equity_fundamental_momentum_50_50_v1` | Alpha Recovery incumbent baseline | 21-session OOS path | top-25 book | desk 12.5 bp | PIT_TRUE | NO - E9 incumbent; E2 UNPROVEN |
| 6 | `GC_USEQ_SINGLE_NAME_OPTIONS_INFORMED_TRADING` | US_EQUITY | DERIVATIVES_LEAD | `SINGLE_NAME_OPTION_INFORMED_TRADING_V1` | none (never tested) | NO | - | - | - | NO - E3 PAID, E4 purchase gate, E6 |
| 7 | `GC_EQIDX_SCHEDULED_EVENT_PREMIUM` | EQUITY_INDEX | RISK_PREMIUM_TIMING | `r46_4_spx_pre_fomc_drift`, `r46_4_spx_announcement_day_premium` | `scheduled_event_equity_premium` | executor path exists | SPY TR vs bills | 1 bp | PIT_TRUE | NO - E2: NO_EDGE on 2026-09-15 (increment t 1.16); CLOSED, not revisited |
| 8 | `GC_XA_FUTURES_TREND_AND_MOMENTUM` | CROSS_ASSET | PRICE_STATE_TRANSFORMATION | `r46_fut_ts_mom_252`, `r46_3_fut_xs_mom_252`, `R63_CROSS_ASSET_TREND_H1_7C708CF5` | R63 / R64 / cadence | not declared (E2 fails first) | futures excess | R64 | PIT_MARKET_OBSERVABLE | NO - E2 (R64 NOT_ECONOMIC_UNDER_CONTROLS, net -0.70 %/yr, CROSS_ASSET_TREND_AND_XS_MOMENTUM), E5 |
| 9 | `GC_USEQ_LIQUIDITY_AND_SEASONALITY_FORWARD` | US_EQUITY | LIQUIDITY_PRESSURE | `r46_3_eq_xs_amihud_illiq`, `r46_3_eq_xs_seasonal_month` | R46 ran no historical screen | NO | - | - | PIT_TRUE | NO - E2 (SAME_MONTH_SEASONALITY rejected, liquidity NO_ALPHA_EVIDENCE), E6/E7 |
| 10 | `GC_XA_FUTURES_VALUE_5Y` | CROSS_ASSET | PRICE_STATE_TRANSFORMATION | `r53_fut_xs_value_5y` | none (never tested) | NO | - | - | PIT_MARKET_OBSERVABLE | NO - E5, E6, E7 |
| 11 | `GC_COMMODITY_XS_SKEWNESS` | COMMODITIES | PRICE_STATE_TRANSFORMATION | `r53_comdty_xs_skew_12m` | none (never tested) | NO | - | - | PIT_MARKET_OBSERVABLE | NO - E5, E6, E7 |
| 12 | `GC_USEQ_EARNINGS_DRIFT` | US_EQUITY | INFORMATION_DIFFUSION_SPEED | `r46_5_pead_announcement_return_20d`, `r46_6_pead_*` | Alpha Recovery scoreboard | NO frozen-identity path | - | - | PIT_TRUE | NO - E2 BAD (untouched FAILED) |
| 13 | `GC_RATES_COPPER_GOLD_LEAD` | RATES | INFORMATION_DIFFUSION_SPEED | `r52_rates_copper_gold_lead` | none (never tested) | NO | - | - | PIT_MARKET_OBSERVABLE | NO - E6, E7 |
| 14 | `GC_RATES_MACRO_SURPRISE_REPRICING` | RATES | INFORMATION_DIFFUSION_SPEED | `r46_4_macro_surprise_rates_5d` | none (never tested) | NO | - | - | PIT_TRUE | NO - E6, E7 |
| 15 | `GC_USEQ_PEER_EARNINGS_TRANSFER` | US_EQUITY | INFORMATION_DIFFUSION_SPEED | `PEER_EARNINGS_INFORMATION_TRANSFER_V1` | executor, DATA_HOLD | NO | - | - | UNESTABLISHED | NO - E2 DATA_HOLD (BLOCKED), E6 |
| 16 | `GC_RATES_CURVE_RV_AND_TERM_CARRY` | RATES | RELATIVE_VALUE_DISLOCATION | `r46_rates_curve_rv_5d`, `r46_3_rates_curve_carry`, `shadow_intl_rates_carry_rv` | R43 / R41 / R64 | not declared | futures excess | R38 | PIT_MARKET_OBSERVABLE | NO - E2 BAD (RATES_CARRY_CURVE_RV) |
| 17 | `GC_USEQ_SHORT_VOLUME_PRESSURE` | US_EQUITY | FUNDING_COLLATERAL_CONSTRAINT | `R58_SHORT_VOLUME_PRESSURE_V1` | R58, data owned only since 2026-07 | NO | - | - | PIT_TRUE | NO - E6, E7 |
| 18 | `GC_COMMODITY_XS_MOMENTUM` | COMMODITIES | PRICE_STATE_TRANSFORMATION | `r46_comdty_xs_mom_252` | R38 / R59 | NO | - | - | PIT_MARKET_OBSERVABLE | NO - E2 BAD, E5 |
| 19 | `GC_EQIDX_SPX_TURN_OF_MONTH` | EQUITY_INDEX | CALENDAR_SEASONALITY | `r46_3_spx_turn_of_month` | R46 ran no historical screen | NO | - | - | PIT_TRUE | NO - E2 (R32_EVENT_DRIVEN_CALENDAR closed), E6/E7 |
| 20 | `GC_CREDIT_SPREAD_REGIME_FORWARD` | CREDIT | RISK_PREMIUM_TIMING | `r46_4_credit_hy_ig_momentum`, `r46_4_credit_regime_spx_timing`, `r46_6_credit_shock_spx_5d` | R41 | NO | - | - | PIT_TRUE | NO - E2 (untouched FAILED, CREDIT_SPREAD_TIMING) |
| 21 | `GC_FX_XS_MOMENTUM` | FX | PRICE_STATE_TRANSFORMATION | `r46_fx_xs_mom_252` | R41 / R59 | NO | - | - | PIT_MARKET_OBSERVABLE | NO - E2 BAD, E5 |
| 22 | `GC_VOL_VX_TERM_STRUCTURE_CARRY_FORWARD` | VOLATILITY | RISK_TRANSFER_PREMIUM | `r46_vx_term_carry_5d`, `r46_3_vx_term_carry_1d`, `shadow_vx_carry_ts` | R38 | not declared | VX futures excess | R38 | PIT_MARKET_OBSERVABLE | NO - E2 BAD (VX_TERM_CARRY) |
| 23 | `GC_EQIDX_XS_RELATIVE_MOMENTUM` | EQUITY_INDEX | PRICE_STATE_TRANSFORMATION | `r52_eqidx_xs_rel_mom_12_1` | R59 | NO | - | - | PIT_MARKET_OBSERVABLE | NO - E2 BAD, E5 |
| 24 | `GC_EQIDX_INDEX_TREND_TIMING` | EQUITY_INDEX | PRICE_STATE_TRANSFORMATION | `r46_spx_trend_200d` | R57 | NO | - | - | PIT_MARKET_OBSERVABLE | NO - E2 BAD, E5 |
| 25 | `GC_USEQ_DISCLOSURE_INTENSITY` | US_EQUITY | PUBLIC_SLOW_DISCLOSURE | `R58_DISCLOSURE_INTENSITY_V1` | R63 | NO | - | - | PIT_TRUE | NO - E2 BAD |
| 26 | `GC_USEQ_FREE_CASH_FLOW_QUALITY` | US_EQUITY | PUBLIC_SLOW_DISCLOSURE | `R58_FCF_PURE_V1`, `R58_FUND_MOMENTUM_VETO_V1`, `R63_US_EQUITY_FREE_CASH_FLOW_H1_BE786041` | R58 / R63 | NO | - | - | PIT_TRUE | NO - E2 BAD |
| 27 | `GC_USEQ_XS_PRICE_STATE_FORWARD` | US_EQUITY | PRICE_STATE_TRANSFORMATION | 13 R46/R63 price-state books | R57 / R63 | NO | - | - | PIT_TRUE | NO - E2 BAD, E5 |
| 28 | `GC_XA_COT_POSITIONING` | CROSS_ASSET | HEDGER_DEMAND_PRESSURE | `r46_4_cot_*`, `r46_6_cot_commercial_xs_5d` | R35 / R38 | NO | - | - | PIT_BY_DECLARED_LAG | NO - E2 BAD |
| 29 | `GC_USEQ_INSIDER_FLOW` | US_EQUITY | PUBLIC_SLOW_DISCLOSURE | `r46_5_insider_*`, `r46_6_insider_cluster_buy_5d` | INSIDER_FORM4 executor | NO | - | - | PIT_TRUE | NO - E2 BAD |

The 18 CLOSED or CONTROL candidates (`GC_EQIDX_DATED_CARRY`, `GC_EQIDX_INDEX_IMPLIED_STATE_TIMING`,
`GC_EQIDX_OIL_SHOCK_DIFFUSION`, `GC_CREDIT_PROXY_MACRO_CONDITIONING_R63`,
`GC_FX_POSITIONING_AND_LIQUIDITY_R63`, `GC_RATES_VOLATILITY_STATE_R63`,
`GC_RATES_TREASURY_AUCTION_CONCESSION`, `GC_RATES_ISSUANCE_SIZED_DURATION_EXTENSION`,
`GC_RATES_MONTH_END_DURATION_LONG_HUMAN_JUDGEMENT`, `GC_COMMODITY_PRICE_TAIL_STATE_R63`,
`GC_COMMODITY_INDEX_ROLL_WINDOW`, `GC_VOL_POLICY_EXPECTATIONS_R63`, `GC_USEQ_R56_CONTROL_BOOKS`,
`GC_USEQ_MERGER_ARBITRAGE`, `GC_XA_DATED_CARRY_R63_R64`, `GC_XA_MONTH_END_REBALANCING_FLOW`,
`GC_XA_CALENDAR_TERM_STRUCTURE_R59`, `GC_CRYPTO_BTC_FUNDING_BASIS_CARRY`) fail E2. The three strike
candidates closed on 2026-09-15 (scheduled event, FX month-end hedge flow, dividend month) are closed
and are not revisited.

### 2.3 Frozen eligible set

`FROZEN_ELIGIBLE = (GC_EQIDX_SPY_REVERSED_PUT_CALL_SKEW, GC_FX_XS_CARRY_DATED_CONTRACT)`

## 3. SELECTION RULE (frozen before any composite return)

### 3.1 Rule

1. Walk the eligible set in ascending global rank.
2. Take the highest-ranked eligible sleeve; continue downward.
3. Skip a sleeve whose `mechanism_class` is already selected.
4. When two consecutive eligible sleeves have opportunity-cost scores within 0.02 of each other and
   the lower-ranked one adds a new asset class while the higher-ranked one does not, take the new
   asset class first.
5. Stop at 5.
6. Require at least 3 sleeves and at least 3 distinct mechanism classes; otherwise **DATA_HOLD**.

### 3.2 Declared return-path owners (E7/E8)

| candidate | frozen identity | owner callable | resolution | normalisation to DAILY NET EXCESS RETURN OVER CASH |
|---|---|---|---|---|
| `GC_FX_XS_CARRY_DATED_CONTRACT` | `ALPHA_RECOVERY_FX_CARRY_CADENCE_H1_F9B1ACA7` | `alpha_agent.alpha_recovery.cadence.sleeve_series("FX_FUTURES\|XS\|1\|CARRY\|k5\|b0.25")` | per session | already a net futures excess return (unfunded notional; collateral earns the bill); a return dated on a non-NYSE session is compounded into the next NYSE session |
| `GC_EQIDX_SPY_REVERSED_PUT_CALL_SKEW` | `REVERSED_SPY_PUT_CALL_SKEW_H5` | `alpha_agent.alpha_recovery.options_surface.path` (frozen sp of the confirmation artifact) | per 5-session period | the frozen sign held over its 5 sessions: `sign x (SPY total-return session return - bill)`, 1 bp per side charged on the entry and exit sessions; the decision rule, sign, entry and hold are unchanged |
| `GC_XA_R39_R40_MACHINE_SHADOWS` | three shadows | `r39` month-end streams | month end only | UNAVAILABLE - a month-end return cannot be split into sessions without a construction the owner never froze |

### 3.3 Applying the rule to the frozen inventory

Walking 2.3: rank 1 `GC_EQIDX_SPY_REVERSED_PUT_CALL_SKEW` (EQUITY_INDEX, HEDGER_DEMAND_PRESSURE), rank
2 `GC_FX_XS_CARRY_DATED_CONTRACT` (FX, RISK_TRANSFER_PREMIUM); no other candidate is eligible.
**Two sleeves, two mechanisms, two asset classes - below the frozen minimum of three.** Applied to the
committed inventory, the rule gives `DATA_HOLD` at gate 1 and no return path is loaded. Independently,
the SPY skew path begins 2022-09-09, so even a three-sleeve set containing it could not reach the
frozen common-history minimum (4.3).

The executor re-derives the inventory from the canonical frontier at run time. If the derived
eligible set differs from `FROZEN_ELIGIBLE`, the owners have changed since this preregistration and
the verdict is `DATA_HOLD` (`INVENTORY_DRIFT`); a new preregistration would be required. No selection
other than the frozen one is ever combined.

## 4. THE COMPOSITE (frozen, in full, whether or not gate 1 passes)

### 4.1 Calendar and accounting owners

* Portfolio sessions: the authoritative NYSE rule-based calendar `engine.exchange_calendar`
  (`NYSE_RULE_BASED_R60_1`), as named by `api.forward_challenger_registry.observation_calendar_for`
  (exchange-session classes on the exchange calendar; futures classes keep their own bar calendar and
  are mapped onto the next NYSE session). No second calendar or NAV owner is created; the executor
  adapts owner outputs only.
* Cash: FRED `DTB3` (the owned R36 acquisition used by `scheduled_event_equity_premium.load_dtb3`,
  sha256 `6028a3cb...`), the last observation dated strictly before each session, / 100 / 252.
* SPY: Norgate `SPY` TOTALRETURN closes.

### 4.2 Construction - ONE rule, no optimiser, no search

* Common sessions: NYSE sessions on which every selected sleeve has a daily net excess return.
* Rebalance: the first common session of each calendar month that has at least 126 prior common
  sessions. The first such session is the first PORTFOLIO session.
* At a rebalance, using only the prior 126 common sessions: `vol_i = sd(R_i, ddof=1) x sqrt(252)`;
  `raw_i = 1 / vol_i`; normalise to 100 %; cap every weight at 35 % and redistribute the excess across
  uncapped sleeves proportionally to their raw weights, iterating until no weight exceeds 35 %; no
  negative weight (internal sleeve positions may be long/short).
* Covariance over the same 126 sessions (`ddof=1`, x 252) is used only for
  `predicted_vol = sqrt(w' S w)`; `scale = min(1.50, 0.10 / predicted_vol)`.
* Exposures `e_i = scale x w_i`, held constant until the next rebalance.
* Daily composite NET EXCESS return:
  `r_p(t) = sum_i e_i R_i(t) - max(0, scale - 1) x 0.0050 / 252 - overlay_cost(t)`.
  Cash and borrowing both accrue at the last-known bill, so the residual cash (scale < 1) earns
  exactly the cash rate (zero excess) and the borrowed amount above 1x pays the bill plus 50 bp,
  whose excess part is the 50 bp.
* Overlay cost on a rebalance session: `turnover = 0.5 x sum_i |e_new_i - e_old_i|` (the first
  rebalance counts from zero exposure); primary 2 bp, stress 4 bp per one-way unit.
  Each sleeve path already carries its own frozen internal trading cost.

### 4.3 Windows (dates only, fixed before any composite return)

* Minimum common history 2,268 sessions (9 x 252).
* The portfolio sessions are split by count: the first 70 % (floor) are QUALIFICATION, the rest the
  untouched COMPOSITE CONFIRMATION. QUALIFICATION >= 1,500 and CONFIRMATION >= 600 sessions, else
  `DATA_HOLD`. The split session is written to the artifact before any composite return is computed.
* Qualification halves: first and second half by session count.

### 4.4 Measurement - existing owners only

* Annualisation 252; Sharpe `mean / sd x sqrt(252)`; NW t and one-sided p via
  `alpha_agent.r63.sensitivity.nw_tstat(x, 10)`; maximum drawdown via `S._max_dd` (compounded).
* SPY regression: `SPY_EXCESS = SPY TR session return - bill`; `beta = cov(r_p, SPY_EXCESS) / var`;
  `alpha_series = r_p - beta x SPY_EXCESS`; annualised alpha `252 x mean(alpha_series)`; HAC t via
  `nw_tstat(alpha_series, 10)`.
* Volatility-matched SPY: `SPY_EXCESS x sd(r_p) / sd(SPY_EXCESS)` on the same sessions (its Sharpe is
  the SPY excess Sharpe).
* Contribution: `c_i = sum_t e_i(t) R_i(t)`; share `|c_i| / sum_j |c_j|`.
* Benchmarks reported: cash (zero excess), SPY total return, volatility-matched SPY, and every
  selected sleeve on the same sessions.

## 5. GATES - in this order

1. **Data -> `DATA_HOLD`.** Inventory drift; fewer than 3 selected sleeves or fewer than 3 distinct
   mechanism classes; any selected path unavailable or unreadable; bill rate missing on more than 1 %
   of portfolio sessions; common history below 2,268 sessions, qualification below 1,500 or
   confirmation below 600.
2. **Qualification** (2 bp overlay), each failure -> `NO_EDGE` with the gate recorded, and the
   confirmation is NOT read:
   net annualised excess >= 5.0 %; Sharpe >= 1.00; NW t >= 2.50; maximum drawdown >= -15.0 %; positive
   net in both halves; SPY-regression alpha >= 3.0 %/yr; its HAC t >= 2.00; Sharpe >= volatility-matched
   SPY Sharpe + 0.25; |correlation to SPY excess| <= 0.60; no sleeve above 60 % of absolute
   contribution; at 4 bp overlay net >= 4.0 % and Sharpe >= 0.80.
3. **Untouched composite confirmation** (read only after 2): net >= 4.0 %; Sharpe >= 0.80;
   NW t >= 2.00; maximum drawdown >= -15.0 %; SPY alpha >= 2.5 %/yr; HAC t >= 1.50; net > 0 at 4 bp; no
   sleeve above 60 % -> else `NO_EDGE`.
4. **Full common history:** net >= 5.0 %; Sharpe >= 1.0; maximum drawdown >= -15 %; net > 0 at 4 bp ->
   else `NO_EDGE`.
5. Otherwise `QUALIFIED`: a governed portfolio-level TRUE_FORWARD challenger packet is prepared for a
   HUMAN gate. **CAPITAL ELIGIBLE = NO** in every case. Nothing is promoted, registered or allocated.

Multiplicity: one preregistered composite (m = 1); every component keeps its own multiplicity label
(FX carry family Holm FAIL; SPY skew sign post hoc) and the composite never upgrades a component.

## 6. ROBUSTNESS - diagnostics only, after the primary verdict, never a rescue

Calendar-year and macro-regime performance, leave-one-sleeve-out, sleeve P&L and risk contributions,
turnover, gross leverage distribution, SPY beta and correlation, 2x overlay cost. None of them can
change a failed verdict.

## 7. FORBIDDEN AFTER RESULTS

Any other weighting rule, volatility target, lookback, cap or leverage cap; dropping or adding a
sleeve; a sign flip; an alternate holdout or split; cost relaxation; reading the confirmation after a
qualification failure; re-deriving the inventory under a different reading of E1-E10.

## 8. SAFETY

RESEARCH ONLY. No purchase, subscription, credential, registration, promotion, capital allocation,
portfolio mutation, order, fill, backfill or live write. The live checkout
`C:\Users\binis\paper_trader` is not touched.
