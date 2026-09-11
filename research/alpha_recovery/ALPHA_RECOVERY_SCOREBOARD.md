# ALPHA RECOVERY SCOREBOARD

**STATUS: OWNED_FREE_INFORMATION_EXHAUSTED**

Stop-loss clock: 1 / 10 eligible sessions elapsed, 9 remaining, deadline 2026-09-24 (BEFORE_DEADLINE)
Deadline outcome: PENDING

| field | INCUMBENT (historical OOS, 21s, top-25) | INCUMBENT (TRUE_FORWARD) | BEST CHALLENGER (historical OOS) |
|---|---|---|---|
| identity | fundamental_momentum_50_50_v1 | alpha_paper_book_1 | FX_FUTURES|XS|1|CARRY|k5|b0.25 |
| verdict | INCUMBENT_WEAK_OR_UNPROVEN | separate gate | ECONOMIC_UNDER_CONTROLS_NOT_FDR |
| net return / excess | 0.0273 /yr net excess | -0.0243 cum (SPY 0.0201) | advantage 0.0292 /yr |
| Sharpe | 0.25 (excess) | -1.14 | delta 0.27 |
| drawdown | -0.222 | -0.047 | delta n/a |
| turnover (one-way / period) | 0.314 | 0.828 total | 0.087 |
| rank IC (t) | 0.0132 (1.13) | h1 0.0082 (0.25) | conditional t 4.26 |
| calibration | UNAVAILABLE | n/a | NOT_APPLICABLE (rank model) |
| multiplicity | benchmark (exempt) | n/a | BH True / Holm False |
| evidence sample | 181 periods (181 effective) | 35 sessions | 6238 effective |
| evidence maturity | HISTORICAL_OOS 2011-07-01..2026-07-16 | TRUE_FORWARD_ACCRUING (35 sessions; effective independent monthly observations 1) | HISTORICAL_OOS_ONLY |
| forward evidence | n/a | this IS the forward evidence | NONE (no ALPHA_RECOVERY challenger is registered) |

## Best challenger on the SAME capital as the incumbent

`US_EQUITY|TOP25|blend|k126` - NO_ADVANTAGE

| net advantage /yr | t | Sharpe delta | drawdown delta | turnover delta | lockbox advantage | failed gates |
|---|---|---|---|---|---|---|
| 0.0218 | 1.61 | 0.306 | 0.015 | -0.209 | 0.0137 | benjamini_hochberg,family_holm,lockbox_halves_ge_floor,paired_t_ge_2 |

US_EQUITY long-only top-25 book: the SAME universe, score and cost as the incumbent, rebalanced every 126 sessions

## First NON-INCUMBENT alpha - NOT_ACHIEVED

52 non-incumbent candidates measured, 0 qualified.

Best intraday sleeve `INTRADAY|SESSION_CARRY|CARRY_REVERSION_SPY_LARGE_ONLY` (NO_ADVANTAGE), markets US_EQUITY_BETA:

| standalone net /yr | t | Sharpe | net /yr at STRESS cost | gross /yr | gross t | corr. to incumbent | equal-risk increment | t |
|---|---|---|---|---|---|---|---|---|
| 0.0464 | 1.27 | 0.90 | 0.0026 | 0.0756 | 2.05 | -0.056 | 0.0935 | 0.98 |

| information axis | state | evidence |
|---|---|---|
| NATIVE_INTRADAY_CROSS_ASSET | CLOSED_NO_QUALIFIED_SIGNAL | Across the 30 PRIMARY specifications the largest Newey-West t at ZERO transaction cost is 1.72, and positive-gross arms are 53 % of the grid - what an information-free grid looks like. Cost is therefore NOT what killed the primary grid; there was no credible gross edge to kill. The engagement-condit |
| NATIVE_CME_FUTURES_INTRADAY | CLOSED_NO_QUALIFIED_SIGNAL | the widest and deepest intraday panel this project has ever held - 2.5x the sessions and 9x the minutes of the closed ETF axis, across 10 contracts and 5 buckets - produced no arm reaching even gross t 2.0 at ZERO cost. The failure is information, not execution or coverage. |
| NATIVE_CME_FUTURES_MICROSTRUCTURE | CLOSED_NO_QUALIFIED_SIGNAL |  |
| OPTIONS_IMPLIED_VOLATILITY_SURFACE | USABLE |  |
| ANALYST_EXPECTATIONS_REVISION_VINTAGES | NOT_OWNED | external_normalized/analyst_revision holds a 3-row mock fixture, an EMPTY normalized file and a 960-row / 40-ticker proxy; there is no revision vintage history to test |
| OWNERSHIP_INSTITUTIONAL_FLOW | NOT_OWNED | external_normalized/short_interest is EMPTY and the FINRA raw store is a 93-byte probe; 13F holdings are not on disk (only an EDGAR submissions cache), so the need remains blocked by an unbuilt CUSIP-to-ticker bridge AND by absent data |
| MACRO_EVENT_INTRADAY_REACTION | CLOSED_BY_R45 | Release 45's own data frontier records that the effect 'failed on its own holdout, in listed US rates and equities over two years, and in every other market the estate owns'. Re-running it would repeat closed work |

## Forecast products

- 2 of 4 product families carry a lockbox-tested economic forecast: EQUITY_BOOK_LEVEL, MULTI_ASSET_SLEEVE

## Campaign counters

- eligible sessions elapsed / 10: 1 / 10 (remaining 9)
- share of new research effort on non-price information: 0.842 (rule >= 0.75: True); inclusive of construction-only specifications 0.762 (True)
- economically distinct information families tested: 13 (CROSS_ASSET_TREND_CADENCE, EARNINGS_EVENT_REACTION, EQUITY_INCUMBENT_CADENCE, FRONTIER_MANDATES_RISK_CONTROLLED, FX_CARRY_CADENCE, INCUMBENT_DECOMPOSITION, INTRADAY_CROSS_MARKET_LEADLAG, INTRADAY_OPENING_RANGE, INTRADAY_RELATIVE_STRENGTH, INTRADAY_SESSION_CARRY, INTRADAY_VOLATILITY_STATE, MARKET_DIRECTION_SPY, NEWS_INTENSITY)
- candidate specifications alive: 5
- in TRUE_FORWARD competition: 0
- highest-value unresolved information gap: US_EQUITY|5|OWNERSHIP_INSTITUTIONAL_FLOW
- top missing information need (purchase case): EARNINGS_EXPECTATIONS_AS_WAS_CONSENSUS

## Every candidate

| cell | kind | verdict | net advantage /yr | t | Sharpe delta | lockbox advantage | BH | Holm | failed gates |
|---|---|---|---|---|---|---|---|---|---|
| INTRADAY|CROSS_MARKET_LEADLAG|CURVE_TO_SPY_RISKOFF | INTRADAY_SLEEVE | NO_ADVANTAGE | -0.2077 | -2.14 | -0.726 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|CROSS_MARKET_LEADLAG|GLD_TO_SPY_RISKOFF | INTRADAY_SLEEVE | WORSE_THAN_INCUMBENT | -0.2172 | -2.30 | -0.759 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|CROSS_MARKET_LEADLAG|RISKOFF_VOTE_TO_SPY | INTRADAY_SLEEVE | WORSE_THAN_INCUMBENT | -0.3418 | -3.57 | -1.194 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|CROSS_MARKET_LEADLAG|TLT_TO_SPY_RISKOFF | INTRADAY_SLEEVE | NO_ADVANTAGE | -0.1858 | -1.89 | -0.649 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|CROSS_MARKET_LEADLAG|TLT_TO_SPY_RISKON | INTRADAY_SLEEVE | WORSE_THAN_INCUMBENT | -0.3224 | -3.68 | -1.126 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|CROSS_MARKET_LEADLAG|UUP_TO_SPY_RISKOFF | INTRADAY_SLEEVE | WORSE_THAN_INCUMBENT | -0.3488 | -3.91 | -1.218 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|OPENING_RANGE|OPENING_RANGE_BREAKOUT_MULTI | INTRADAY_SLEEVE | NO_ADVANTAGE | -0.0823 | -0.84 | -0.287 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|OPENING_RANGE|OPENING_RANGE_BREAKOUT_QQQ | INTRADAY_SLEEVE | NO_ADVANTAGE | -0.1334 | -1.41 | -0.466 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|OPENING_RANGE|OPENING_RANGE_BREAKOUT_SPY | INTRADAY_SLEEVE | NO_ADVANTAGE | -0.0438 | -0.45 | -0.153 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|OPENING_RANGE|OPENING_RANGE_FADE_MULTI | INTRADAY_SLEEVE | WORSE_THAN_INCUMBENT | -0.2339 | -2.64 | -0.817 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|OPENING_RANGE|OPENING_RANGE_FADE_QQQ | INTRADAY_SLEEVE | NO_ADVANTAGE | -0.0652 | -0.71 | -0.228 | n/a | False | False | benjamini_hochberg,family_holm,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|OPENING_RANGE|OPENING_RANGE_FADE_SPY | INTRADAY_SLEEVE | NO_ADVANTAGE | -0.1694 | -1.92 | -0.592 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|RELATIVE_STRENGTH|RS_MOMENTUM_1000 | INTRADAY_SLEEVE | WORSE_THAN_INCUMBENT | -0.3364 | -3.56 | -1.175 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|RELATIVE_STRENGTH|RS_MOMENTUM_1030 | INTRADAY_SLEEVE | WORSE_THAN_INCUMBENT | -0.5232 | -5.52 | -1.827 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|RELATIVE_STRENGTH|RS_MOMENTUM_1100 | INTRADAY_SLEEVE | WORSE_THAN_INCUMBENT | -0.5663 | -5.91 | -1.978 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|RELATIVE_STRENGTH|RS_REVERSAL_1000 | INTRADAY_SLEEVE | WORSE_THAN_INCUMBENT | -0.3767 | -4.09 | -1.316 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|RELATIVE_STRENGTH|RS_REVERSAL_1030 | INTRADAY_SLEEVE | WORSE_THAN_INCUMBENT | -0.3285 | -3.57 | -1.147 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|RELATIVE_STRENGTH|RS_REVERSAL_1100 | INTRADAY_SLEEVE | WORSE_THAN_INCUMBENT | -0.4806 | -5.29 | -1.678 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|SESSION_CARRY|CARRY_CONTINUATION_SPY | INTRADAY_SLEEVE | WORSE_THAN_INCUMBENT | -0.3469 | -3.86 | -1.212 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|SESSION_CARRY|CARRY_REVERSION_MULTI | INTRADAY_SLEEVE | NO_ADVANTAGE | -0.2049 | -2.13 | -0.716 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|SESSION_CARRY|CARRY_REVERSION_MULTI_LARGE_ONLY | INTRADAY_SLEEVE | NO_ADVANTAGE | -0.1342 | -1.40 | -0.469 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|SESSION_CARRY|CARRY_REVERSION_QQQ | INTRADAY_SLEEVE | NO_ADVANTAGE | -0.0659 | -0.69 | -0.230 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|SESSION_CARRY|CARRY_REVERSION_SPY | INTRADAY_SLEEVE | NO_ADVANTAGE | -0.0278 | -0.29 | -0.097 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|SESSION_CARRY|CARRY_REVERSION_SPY_LARGE_ONLY | INTRADAY_SLEEVE | NO_ADVANTAGE | 0.0935 | 0.98 | 0.327 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|SESSION_CARRY|PREMARKET_CONTINUATION_SPY | INTRADAY_SLEEVE | NO_ADVANTAGE | -0.1894 | -2.04 | -0.661 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|SESSION_CARRY|PREMARKET_REVERSION_SPY | INTRADAY_SLEEVE | NO_ADVANTAGE | -0.1936 | -2.07 | -0.676 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|VOLATILITY_STATE|OPEN_MOVE_ANY_MULTI | INTRADAY_SLEEVE | WORSE_THAN_INCUMBENT | -0.3673 | -3.83 | -1.283 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|VOLATILITY_STATE|OPEN_MOVE_ANY_SPY | INTRADAY_SLEEVE | WORSE_THAN_INCUMBENT | -0.2172 | -2.27 | -0.759 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|VOLATILITY_STATE|OPEN_MOVE_COMPRESSED_MULTI | INTRADAY_SLEEVE | WORSE_THAN_INCUMBENT | -0.3129 | -3.30 | -1.093 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|VOLATILITY_STATE|OPEN_MOVE_COMPRESSED_SPY | INTRADAY_SLEEVE | NO_ADVANTAGE | -0.0893 | -0.95 | -0.312 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|VOLATILITY_STATE|OPEN_MOVE_EXPANDED_MULTI | INTRADAY_SLEEVE | WORSE_THAN_INCUMBENT | -0.2861 | -3.04 | -0.999 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| INTRADAY|VOLATILITY_STATE|OPEN_MOVE_EXPANDED_SPY | INTRADAY_SLEEVE | WORSE_THAN_INCUMBENT | -0.2380 | -2.51 | -0.831 | n/a | False | False | benjamini_hochberg,family_holm,holdout_halves_ge_floor,holdout_sign_agrees,materiality_ge_1p5pct,positive_equal_risk_utility,survives_stress_cost,t_ge_2 |
| US_EQUITY|TOP25|blend|k126 | SAME_DOMAIN_CONSTRUCTION | NO_ADVANTAGE | 0.0218 | 1.61 | 0.306 | 0.0137 | False | False | benjamini_hochberg,family_holm,lockbox_halves_ge_floor,paired_t_ge_2 |
| US_EQUITY|TOP25|blend|k21 | SAME_DOMAIN_CONSTRUCTION | REFERENCE_ARM | 0.0000 | 0.00 | n/a | 0.0000 | None | None | lockbox_sign_agrees,materiality_ge_1p5pct,paired_t_ge_2 |
| US_EQUITY|TOP25|blend|k42 | SAME_DOMAIN_CONSTRUCTION | NO_ADVANTAGE | 0.0169 | 1.62 | 0.412 | -0.0022 | False | False | benjamini_hochberg,family_holm,lockbox_halves_ge_floor,lockbox_sign_agrees,paired_t_ge_2 |
| US_EQUITY|TOP25|blend|k63 | SAME_DOMAIN_CONSTRUCTION | NO_ADVANTAGE | 0.0168 | 1.49 | 0.346 | 0.0257 | False | False | benjamini_hochberg,family_holm,lockbox_halves_ge_floor,paired_t_ge_2 |
| US_EQUITY|TOP25|fundamental_only|k21 | SAME_DOMAIN_CONSTRUCTION | NO_ADVANTAGE | -0.0104 | -0.36 | -0.092 | -0.0929 | False | False | benjamini_hochberg,family_holm,lockbox_halves_ge_floor,lockbox_sign_agrees,materiality_ge_1p5pct,paired_t_ge_2 |
| US_EQUITY|TOP25|fundamental_only|k63 | SAME_DOMAIN_CONSTRUCTION | NO_ADVANTAGE | -0.0120 | -0.46 | -0.110 | -0.0921 | False | False | benjamini_hochberg,family_holm,lockbox_halves_ge_floor,lockbox_sign_agrees,materiality_ge_1p5pct,paired_t_ge_2 |
| US_EQUITY|TOP25|momentum_only|k21 | SAME_DOMAIN_CONSTRUCTION | NO_ADVANTAGE | 0.0087 | 0.44 | 0.113 | 0.0716 | False | False | benjamini_hochberg,family_holm,lockbox_sign_agrees,materiality_ge_1p5pct,paired_t_ge_2,turnover_le_cap |
| US_EQUITY|TOP25|momentum_only|k63 | SAME_DOMAIN_CONSTRUCTION | NO_ADVANTAGE | 0.0086 | 0.40 | 0.099 | 0.0391 | False | False | benjamini_hochberg,family_holm,lockbox_sign_agrees,materiality_ge_1p5pct,paired_t_ge_2 |
| CREDIT_PROXY|TS|21|INFLATION_EXPECTATIONS | FRONTIER_NEED | NOT_ECONOMIC_UNDER_CONTROLS | 0.0121 | 1.15 | 0.098 | n/a | False | False |  |
| US_EQUITY|XS|1|FREE_CASH_FLOW | FRONTIER_NEED | NOT_ECONOMIC_UNDER_CONTROLS | 0.0362 | 3.49 | 0.343 | n/a | True | True |  |
| VOLATILITY|TS|1|VOLATILITY_EXPECTATIONS_IV | FRONTIER_NEED | REPRODUCTION_FAILED | 0.0004 | 0.11 | 0.003 | n/a | False | False |  |
| VOLATILITY|TS|5|VOLATILITY_EXPECTATIONS_IV | FRONTIER_NEED | REPRODUCTION_FAILED | 0.0029 | 0.87 | 0.026 | n/a | False | False |  |
| US_EQUITY|XS|1|EARNINGS_EVENT_REACTION|vs|INCUMBENT_SCORE | SAME_DOMAIN | NO_ADVANTAGE | 0.0197 | 1.57 | 0.581 | 0.0500 | False | False | benjamini_hochberg,family_holm,information_conditional_t_ge_2,paired_t_ge_2 |
| US_EQUITY|XS|1|NEWS_INTENSITY|vs|INCUMBENT_SCORE | SAME_DOMAIN | WORSE_THAN_INCUMBENT | -0.0346 | -2.46 | -1.157 | -0.0486 | False | False | benjamini_hochberg,family_holm,information_conditional_t_ge_2,lockbox_halves_ge_floor,lockbox_sign_agrees,materiality_ge_1p5pct,paired_t_ge_2 |
| US_EQUITY|XS|21|EARNINGS_EVENT_REACTION|vs|INCUMBENT_SCORE | SAME_DOMAIN | NO_ADVANTAGE | -0.0038 | -0.25 | -0.090 | -0.0189 | False | False | benjamini_hochberg,family_holm,information_conditional_t_ge_2,lockbox_halves_ge_floor,lockbox_sign_agrees,materiality_ge_1p5pct,paired_t_ge_2,turnover_le_cap |
| US_EQUITY|XS|21|EARNINGS_EVENT_REACTION|vs|PRICE_RETURN_STATE+TREND+MOMENTUM+REVERSAL+REALISED_VOLATILITY+TAIL_CRASH_STATE+LIQUIDITY+VOLUME_PARTICIPATION+FUNDAMENTAL_LEVELS+FREE_CASH_FLOW | SAME_DOMAIN | NO_ADVANTAGE | 0.0060 | 0.68 | 0.256 | -0.0016 | False | False | benjamini_hochberg,family_holm,information_conditional_t_ge_2,lockbox_sign_agrees,materiality_ge_1p5pct,paired_t_ge_2,turnover_le_cap |
| US_EQUITY|XS|21|EARNINGS_REACTION_ONLY|vs|INCUMBENT_SCORE | SAME_DOMAIN | NO_ADVANTAGE | -0.0130 | -1.15 | -0.368 | -0.0329 | False | False | benjamini_hochberg,family_holm,information_conditional_t_ge_2,lockbox_halves_ge_floor,lockbox_sign_agrees,materiality_ge_1p5pct,paired_t_ge_2 |
| US_EQUITY|XS|21|EARNINGS_SURPRISE_ONLY|vs|INCUMBENT_SCORE | SAME_DOMAIN | NO_ADVANTAGE | 0.0081 | 1.09 | 0.369 | 0.0030 | False | False | benjamini_hochberg,family_holm,information_conditional_t_ge_2,materiality_ge_1p5pct,paired_t_ge_2,turnover_le_cap |
| US_EQUITY|XS|5|EARNINGS_EVENT_REACTION|vs|INCUMBENT_SCORE | SAME_DOMAIN | NO_ADVANTAGE | -0.0052 | -0.48 | -0.176 | -0.0084 | False | False | benjamini_hochberg,family_holm,information_conditional_t_ge_2,lockbox_halves_ge_floor,lockbox_sign_agrees,materiality_ge_1p5pct,paired_t_ge_2 |
| US_EQUITY|XS|5|NEWS_COUNT_ONLY|vs|INCUMBENT_SCORE | SAME_DOMAIN | NO_ADVANTAGE | -0.0102 | -1.18 | -0.549 | -0.0124 | False | False | benjamini_hochberg,family_holm,information_conditional_t_ge_2,lockbox_halves_ge_floor,lockbox_sign_agrees,materiality_ge_1p5pct,paired_t_ge_2 |
| US_EQUITY|XS|5|NEWS_INTENSITY|vs|INCUMBENT_SCORE | SAME_DOMAIN | NO_ADVANTAGE | -0.0137 | -1.02 | -0.479 | -0.0099 | False | False | benjamini_hochberg,family_holm,information_conditional_t_ge_2,lockbox_halves_ge_floor,lockbox_sign_agrees,materiality_ge_1p5pct,paired_t_ge_2 |
| US_EQUITY|XS|5|NEWS_SENTIMENT_ONLY|vs|INCUMBENT_SCORE | SAME_DOMAIN | NO_ADVANTAGE | -0.0139 | -1.18 | -0.553 | -0.0052 | False | False | benjamini_hochberg,family_holm,information_conditional_t_ge_2,lockbox_halves_ge_floor,lockbox_sign_agrees,materiality_ge_1p5pct,paired_t_ge_2 |
| US_EQUITY|XS|63|EARNINGS_EVENT_REACTION|vs|INCUMBENT_SCORE | SAME_DOMAIN | NO_ADVANTAGE | -0.0098 | -1.40 | -0.273 | -0.0258 | False | False | benjamini_hochberg,effective_sample_ge_floor,family_holm,information_conditional_t_ge_2,lockbox_halves_ge_floor,lockbox_sign_agrees,materiality_ge_1p5pct,paired_t_ge_2,turnover_le_cap |
| CROSS_ASSET|XS|1|TREND|k10|b0.25 | CROSS_DOMAIN_SLEEVE | NOT_ECONOMIC_UNDER_CONTROLS | 0.0060 | 0.34 | 0.080 | 0.0274 | False | False |  |
| CROSS_ASSET|XS|1|TREND|k1|b0.25 | CROSS_DOMAIN_SLEEVE | NOT_ECONOMIC_UNDER_CONTROLS | 0.0995 | 7.96 | 0.968 | 0.1329 | True | True |  |
| CROSS_ASSET|XS|1|TREND|k21|b0.25 | CROSS_DOMAIN_SLEEVE | NOT_ECONOMIC_UNDER_CONTROLS | 0.0181 | 1.63 | 0.186 | 0.0595 | True | False |  |
| CROSS_ASSET|XS|1|TREND|k5|b0.25 | CROSS_DOMAIN_SLEEVE | NOT_ECONOMIC_UNDER_CONTROLS | 0.0118 | 0.68 | 0.182 | 0.0712 | False | False |  |
| FX_FUTURES|XS|1|CARRY|k10|b0.25 | CROSS_DOMAIN_SLEEVE | ECONOMIC_UNDER_CONTROLS_NOT_FDR | 0.0240 | 1.61 | 0.222 | 0.1005 | True | False |  |
| FX_FUTURES|XS|1|CARRY|k1|b0.25 | CROSS_DOMAIN_SLEEVE | NOT_ECONOMIC_UNDER_CONTROLS | 0.0356 | 2.49 | 0.335 | 0.1180 | True | True |  |
| FX_FUTURES|XS|1|CARRY|k21|b0.25 | CROSS_DOMAIN_SLEEVE | ECONOMIC_UNDER_CONTROLS_NOT_FDR | 0.0160 | 1.09 | 0.143 | 0.0715 | False | False |  |
| FX_FUTURES|XS|1|CARRY|k21|b0.50 | CROSS_DOMAIN_SLEEVE | ECONOMIC_UNDER_CONTROLS_NOT_FDR | 0.0154 | 1.04 | 0.132 | 0.0717 | False | False |  |
| FX_FUTURES|XS|1|CARRY|k5|b0.25 | CROSS_DOMAIN_SLEEVE | ECONOMIC_UNDER_CONTROLS_NOT_FDR | 0.0292 | 1.95 | 0.270 | 0.0799 | True | False |  |
| FX_FUTURES|XS|1|CARRY|k5|b0.50 | CROSS_DOMAIN_SLEEVE | ECONOMIC_UNDER_CONTROLS_NOT_FDR | 0.0244 | 1.56 | 0.221 | 0.0585 | True | False |  |
| SPY|1 | MARKET_DIRECTION | NO_DIRECTIONAL_SKILL | -0.0030 | -1.23 | -0.007 | n/a | None | None |  |
| SPY|21 | MARKET_DIRECTION | NO_DIRECTIONAL_SKILL | -0.0023 | -1.52 | -0.003 | n/a | None | None |  |
| SPY|5 | MARKET_DIRECTION | NO_DIRECTIONAL_SKILL | -0.0017 | -1.67 | -0.007 | n/a | None | None |  |
| SPY|63 | MARKET_DIRECTION | NO_DIRECTIONAL_SKILL | -0.0050 | -2.96 | -0.020 | n/a | None | None |  |
| RATES_FUTURES|XS|21|VOLATILITY_EXPECTATIONS_IV | CROSS_DOMAIN_SLEEVE | REPRODUCTION_FAILED | 0.0027 | 1.00 | 0.025 | n/a | False | False |  |
| FX_FUTURES|XS|21|POSITIONING_COMMITMENTS | CROSS_DOMAIN_SLEEVE | REPRODUCTION_FAILED | -0.0042 | -0.32 | -0.046 | n/a | False | False |  |

Historical OOS and TRUE_FORWARD are separate columns and are never pooled. Release numbers, lines changed, tests passed and architecture work are not on this page.
