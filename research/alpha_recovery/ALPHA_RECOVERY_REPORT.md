# ALPHA RECOVERY SCOREBOARD

**STATUS: OWNED_FREE_INFORMATION_EXHAUSTED**

| field | value |
|---|---|
| INCUMBENT | fundamental_momentum_50_50_v1 (INCUMBENT_WEAK_OR_UNPROVEN) |
| BEST CHALLENGER | FX_FUTURES|XS|1|CARRY|k5|b0.25 (ECONOMIC_UNDER_CONTROLS_NOT_FDR) |
| BEST CHALLENGER ON THE SAME CAPITAL | US_EQUITY|TOP25|blend|k126 (NO_ADVANTAGE): +2.18 % /yr advantage, paired t 1.61, Sharpe delta 0.306, drawdown delta 0.015, turnover delta -0.209 |
| HORIZON | 1 sessions |
| INFORMATION | FX_CARRY_CADENCE / CARRY |
| HISTORICAL OOS NET ADVANTAGE | +2.92 % /yr (incumbent +2.73 % /yr net excess) |
| SHARPE DELTA | 0.270 |
| DRAWDOWN DELTA | n/a |
| TURNOVER | challenger 0.087 one-way / period (incumbent 0.314) |
| BEST NON-INCUMBENT (INTRADAY) CHALLENGER | INTRADAY|SESSION_CARRY|CARRY_REVERSION_SPY_LARGE_ONLY (NO_ADVANTAGE): +4.64 % /yr standalone at t 1.27, +0.26 % /yr at the STRESS cost, gross t 2.05, correlation to the incumbent -0.056 |
| FIRST NON-INCUMBENT ALPHA | NOT_ACHIEVED - 52 non-incumbent candidates measured, 0 qualified |
| FORECAST PRODUCTS | 2 of 4 product families carry a lockbox-tested economic forecast: EQUITY_BOOK_LEVEL, MULTI_ASSET_SLEEVE |
| FORECAST CALIBRATION | NOT_APPLICABLE (rank model) |
| MULTIPLICITY STATUS | paired t 1.95, conditional t 4.26, BH True, Holm False, failed gates none |
| TRUE_FORWARD OBSERVATIONS | 0 for any campaign challenger; incumbent book 35 sessions |
| FORWARD STATUS | NONE (no ALPHA_RECOVERY challenger is registered) |
| CAPITAL APPLICABILITY | futures sleeve; incremental to the incumbent under equal risk |
| DECISION | OWNED_FREE_INFORMATION_EXHAUSTED |

## The answers

1. **Does the incumbent actually have demonstrated Alpha?** INCUMBENT_WEAK_OR_UNPROVEN. Historical OOS (2011-07 to 2026-07, top-25, 21 sessions): net excess +2.73 % /yr at t 0.99, rank-IC t 1.13, lockbox (2023+) +10.98 % /yr at t 1.59 versus selection +0.15 % /yr. TRUE_FORWARD (separate, never pooled): 35 sessions, -2.43 % cumulative versus SPY +2.01 %, excess -4.43 %. Checks: effective_periods_ge_floor=True, lockbox_sign_agrees=True, net_excess_ge_materiality=True, t_net_excess_ge_2=False, t_rank_ic_ge_2=False.

2. **What does the incumbent predict today, in economic units rather than scores?** the incumbent ranks 50 names; its rank score is NOT an expected return; no calibrated economic translation exists, so expected returns are UNAVAILABLE (snapshot 2026-09-09, fundamental data as of 2026-05-22, 199 names ranked). At BOOK level the answer is no longer empty: 3 of 4 cadence books carry a lockbox-tested expected excess return

3. **What is the strongest challenger?** Ranked best: FX_FUTURES|XS|1|CARRY|k5|b0.25 (verdict ECONOMIC_UNDER_CONTROLS_NOT_FDR, family FX_CARRY_CADENCE, horizon 1). On the SAME capital as the incumbent the best is US_EQUITY|TOP25|blend|k126 (verdict NO_ADVANTAGE): +2.18 % /yr advantage at paired t 1.61, Sharpe delta 0.306, drawdown delta 0.015, turnover delta -0.209. The ranked best is a futures sleeve and is not applicable to this portfolio; the same-capital candidate is.

4. **How much does it beat the incumbent AFTER COSTS?** Ranked best: +2.92 % /yr (paired t 1.95; lockbox +7.99 % /yr); Sharpe delta 0.270; drawdown delta n/a. Same capital (US_EQUITY|TOP25|blend|k126): +2.18 % /yr at paired t 1.61, lockbox +1.37 % /yr, Sharpe delta 0.306, drawdown delta 0.015, turnover delta -0.209 - it clears the 1.5 %/yr materiality bar and FAILS the frozen t >= 2 bar.

5. **Is the result statistically and economically credible?** Materiality True; paired t 1.95; conditional-information t 4.26; Benjamini-Hochberg True; Holm False; failed gates: none. POST_SELECTION: the R63/R64 lockbox of this family was viewed before this campaign

6. **Does it survive regimes?** FX_FUTURES|XS|1|CARRY|k5|b0.25: 0.90 of 3-year blocks positive, regimes {'iv_high': 0.0137, 'iv_low': 0.0117, 'trend_down': 0.0114, 'trend_up': 0.0136} (the conditional statistic's stability; the cadence book is POST_SELECTION); US_EQUITY|XS|1|EARNINGS_EVENT_REACTION|vs|INCUMBENT_SCORE: 0.00 of 3-year blocks positive, regimes {'iv_high': -0.0009, 'iv_low': -0.0006, 'trend_down': 0.0, 'trend_up': -0.0008}; US_EQUITY|XS|1|NEWS_INTENSITY|vs|INCUMBENT_SCORE: 0.00 of 3-year blocks positive, regimes {'iv_high': -0.0024, 'iv_low': -0.0238, 'trend_down': 0.0024, 'trend_up': -0.0175}

7. **Does it improve forecast calibration?** Cross-sectional rank models emit no probability; calibration is measured on the broad-market direction forecasts: SPY 1s: Brier skill -0.0179 (t -3.47), ECE 0.036, hit 0.545 vs always-up 0.553 -> NO_DIRECTIONAL_SKILL; SPY 21s: Brier skill -0.0068 (t -0.36), ECE 0.053, hit 0.730 vs always-up 0.730 -> NO_DIRECTIONAL_SKILL; SPY 5s: Brier skill -0.0106 (t -0.77), ECE 0.053, hit 0.636 vs always-up 0.636 -> NO_DIRECTIONAL_SKILL; SPY 63s: Brier skill -0.0283 (t -1.41), ECE 0.106, hit 0.779 vs always-up 0.779 -> NO_DIRECTIONAL_SKILL

8. **Is it ready for TRUE_FORWARD competition?** OWNED_FREE_INFORMATION_EXHAUSTED. READY records: 0; survivors not qualified: 5.

9. **What exact human-gated command would start that competition?** Only a READY record may be offered. None is READY. The command shape, for review, is: `C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe scripts\adopt_prospective_freeze.py --challenger-id ALPHA_RECOVERY_FX_CARRY_CADENCE_H1_F9B1ACA7 --confirm ADOPT_PROSPECTIVE_FORWARD_CLOCKS --execute` (dry run without --execute; a live registration requires the typed confirmation token).

10. **What broad-market direction forecasts can we now produce?** SPY 1s: probability_up 0.536 (climatology 0.547) -> UNAVAILABLE (uncalibrated OOS); SPY 21s: probability_up 0.760 (climatology 0.693) -> UNAVAILABLE (uncalibrated OOS); SPY 5s: probability_up 0.639 (climatology 0.609) -> UNAVAILABLE (uncalibrated OOS); SPY 63s: probability_up 0.639 (climatology 0.770) -> UNAVAILABLE (uncalibrated OOS)

11. **What equity expected-return forecasts can we now produce?** PER NAME: the incumbent ranks 50 names; its rank score is NOT an expected return; no calibrated economic translation exists, so expected returns are UNAVAILABLE The forecast contract carries the ranks as labelled scores with expected_excess_return UNAVAILABLE. PER BOOK: 3 of 4 cadence books carry a lockbox-tested expected excess return Licensed books: US_EQUITY|TOP25|blend|k126, US_EQUITY|TOP25|blend|k42, US_EQUITY|TOP25|blend|k63. The operational 21-session book is NOT licensed - its selection-period mean excess and its lockbox mean disagree in sign, which is the calibration test doing its job on the incumbent's own construction.

12. **What multi-asset forecasts can we now produce?** FX carry sleeve (FX_FUTURES|XS|1|CARRY|k21|b0.50): incumbent-only Sharpe 0.929 versus incumbent + sleeve at equal risk 0.831; incremental net -1.66 % /yr (t -0.95); OK. No calibrated sleeve expected return exists; the sleeve forecast fields are UNAVAILABLE. Sleeve product: the sleeve carries a lockbox-tested expected return

13. **What information actually added value?** QUALIFIED (every frozen gate): none. MEASURED BUT NOT QUALIFIED (conditional information value real, economics positive under controls, multiplicity or the equal-risk utility test failed): FX_FUTURES|XS|1|CARRY|k10|b0.25 (ECONOMIC_UNDER_CONTROLS_NOT_FDR, +2.40 % /yr); FX_FUTURES|XS|1|CARRY|k21|b0.25 (ECONOMIC_UNDER_CONTROLS_NOT_FDR, +1.60 % /yr); FX_FUTURES|XS|1|CARRY|k21|b0.50 (ECONOMIC_UNDER_CONTROLS_NOT_FDR, +1.54 % /yr); FX_FUTURES|XS|1|CARRY|k5|b0.25 (ECONOMIC_UNDER_CONTROLS_NOT_FDR, +2.92 % /yr); FX_FUTURES|XS|1|CARRY|k5|b0.50 (ECONOMIC_UNDER_CONTROLS_NOT_FDR, +2.44 % /yr). The FX carry sleeve adds no utility to the incumbent-only book at equal risk (-1.66 % /yr, t -0.95).

14. **What information was rejected?** INTRADAY|CROSS_MARKET_LEADLAG|CURVE_TO_SPY_RISKOFF (NO_ADVANTAGE, -20.77 % /yr); INTRADAY|CROSS_MARKET_LEADLAG|GLD_TO_SPY_RISKOFF (WORSE_THAN_INCUMBENT, -21.72 % /yr); INTRADAY|CROSS_MARKET_LEADLAG|RISKOFF_VOTE_TO_SPY (WORSE_THAN_INCUMBENT, -34.18 % /yr); INTRADAY|CROSS_MARKET_LEADLAG|TLT_TO_SPY_RISKOFF (NO_ADVANTAGE, -18.58 % /yr); INTRADAY|CROSS_MARKET_LEADLAG|TLT_TO_SPY_RISKON (WORSE_THAN_INCUMBENT, -32.24 % /yr); INTRADAY|CROSS_MARKET_LEADLAG|UUP_TO_SPY_RISKOFF (WORSE_THAN_INCUMBENT, -34.88 % /yr); INTRADAY|OPENING_RANGE|OPENING_RANGE_BREAKOUT_MULTI (NO_ADVANTAGE, -8.23 % /yr); INTRADAY|OPENING_RANGE|OPENING_RANGE_BREAKOUT_QQQ (NO_ADVANTAGE, -13.34 % /yr); INTRADAY|OPENING_RANGE|OPENING_RANGE_BREAKOUT_SPY (NO_ADVANTAGE, -4.38 % /yr); INTRADAY|OPENING_RANGE|OPENING_RANGE_FADE_MULTI (WORSE_THAN_INCUMBENT, -23.39 % /yr); INTRADAY|OPENING_RANGE|OPENING_RANGE_FADE_QQQ (NO_ADVANTAGE, -6.52 % /yr); INTRADAY|OPENING_RANGE|OPENING_RANGE_FADE_SPY (NO_ADVANTAGE, -16.94 % /yr); INTRADAY|RELATIVE_STRENGTH|RS_MOMENTUM_1000 (WORSE_THAN_INCUMBENT, -33.64 % /yr); INTRADAY|RELATIVE_STRENGTH|RS_MOMENTUM_1030 (WORSE_THAN_INCUMBENT, -52.32 % /yr); INTRADAY|RELATIVE_STRENGTH|RS_MOMENTUM_1100 (WORSE_THAN_INCUMBENT, -56.63 % /yr); INTRADAY|RELATIVE_STRENGTH|RS_REVERSAL_1000 (WORSE_THAN_INCUMBENT, -37.67 % /yr); INTRADAY|RELATIVE_STRENGTH|RS_REVERSAL_1030 (WORSE_THAN_INCUMBENT, -32.85 % /yr); INTRADAY|RELATIVE_STRENGTH|RS_REVERSAL_1100 (WORSE_THAN_INCUMBENT, -48.06 % /yr); INTRADAY|SESSION_CARRY|CARRY_CONTINUATION_SPY (WORSE_THAN_INCUMBENT, -34.69 % /yr); INTRADAY|SESSION_CARRY|CARRY_REVERSION_MULTI (NO_ADVANTAGE, -20.49 % /yr); INTRADAY|SESSION_CARRY|CARRY_REVERSION_MULTI_LARGE_ONLY (NO_ADVANTAGE, -13.42 % /yr); INTRADAY|SESSION_CARRY|CARRY_REVERSION_QQQ (NO_ADVANTAGE, -6.59 % /yr); INTRADAY|SESSION_CARRY|CARRY_REVERSION_SPY (NO_ADVANTAGE, -2.78 % /yr); INTRADAY|SESSION_CARRY|CARRY_REVERSION_SPY_LARGE_ONLY (NO_ADVANTAGE, +9.35 % /yr); INTRADAY|SESSION_CARRY|PREMARKET_CONTINUATION_SPY (NO_ADVANTAGE, -18.94 % /yr); INTRADAY|SESSION_CARRY|PREMARKET_REVERSION_SPY (NO_ADVANTAGE, -19.36 % /yr); INTRADAY|VOLATILITY_STATE|OPEN_MOVE_ANY_MULTI (WORSE_THAN_INCUMBENT, -36.73 % /yr); INTRADAY|VOLATILITY_STATE|OPEN_MOVE_ANY_SPY (WORSE_THAN_INCUMBENT, -21.72 % /yr); INTRADAY|VOLATILITY_STATE|OPEN_MOVE_COMPRESSED_MULTI (WORSE_THAN_INCUMBENT, -31.29 % /yr); INTRADAY|VOLATILITY_STATE|OPEN_MOVE_COMPRESSED_SPY (NO_ADVANTAGE, -8.93 % /yr); INTRADAY|VOLATILITY_STATE|OPEN_MOVE_EXPANDED_MULTI (WORSE_THAN_INCUMBENT, -28.61 % /yr); INTRADAY|VOLATILITY_STATE|OPEN_MOVE_EXPANDED_SPY (WORSE_THAN_INCUMBENT, -23.80 % /yr); US_EQUITY|TOP25|blend|k126 (NO_ADVANTAGE, +2.18 % /yr); US_EQUITY|TOP25|blend|k21 (REFERENCE_ARM, +0.00 % /yr); US_EQUITY|TOP25|blend|k42 (NO_ADVANTAGE, +1.69 % /yr); US_EQUITY|TOP25|blend|k63 (NO_ADVANTAGE, +1.68 % /yr); US_EQUITY|TOP25|fundamental_only|k21 (NO_ADVANTAGE, -1.04 % /yr); US_EQUITY|TOP25|fundamental_only|k63 (NO_ADVANTAGE, -1.20 % /yr); US_EQUITY|TOP25|momentum_only|k21 (NO_ADVANTAGE, +0.87 % /yr); US_EQUITY|TOP25|momentum_only|k63 (NO_ADVANTAGE, +0.86 % /yr); CREDIT_PROXY|TS|21|INFLATION_EXPECTATIONS (NOT_ECONOMIC_UNDER_CONTROLS, +1.21 % /yr); US_EQUITY|XS|1|FREE_CASH_FLOW (NOT_ECONOMIC_UNDER_CONTROLS, +3.62 % /yr); VOLATILITY|TS|1|VOLATILITY_EXPECTATIONS_IV (REPRODUCTION_FAILED, +0.04 % /yr); VOLATILITY|TS|5|VOLATILITY_EXPECTATIONS_IV (REPRODUCTION_FAILED, +0.29 % /yr); US_EQUITY|XS|1|EARNINGS_EVENT_REACTION|vs|INCUMBENT_SCORE (NO_ADVANTAGE, +1.97 % /yr); US_EQUITY|XS|1|NEWS_INTENSITY|vs|INCUMBENT_SCORE (WORSE_THAN_INCUMBENT, -3.46 % /yr); US_EQUITY|XS|21|EARNINGS_EVENT_REACTION|vs|INCUMBENT_SCORE (NO_ADVANTAGE, -0.38 % /yr); US_EQUITY|XS|21|EARNINGS_EVENT_REACTION|vs|PRICE_RETURN_STATE+TREND+MOMENTUM+REVERSAL+REALISED_VOLATILITY+TAIL_CRASH_STATE+LIQUIDITY+VOLUME_PARTICIPATION+FUNDAMENTAL_LEVELS+FREE_CASH_FLOW (NO_ADVANTAGE, +0.60 % /yr); US_EQUITY|XS|21|EARNINGS_REACTION_ONLY|vs|INCUMBENT_SCORE (NO_ADVANTAGE, -1.30 % /yr); US_EQUITY|XS|21|EARNINGS_SURPRISE_ONLY|vs|INCUMBENT_SCORE (NO_ADVANTAGE, +0.81 % /yr); US_EQUITY|XS|5|EARNINGS_EVENT_REACTION|vs|INCUMBENT_SCORE (NO_ADVANTAGE, -0.52 % /yr); US_EQUITY|XS|5|NEWS_COUNT_ONLY|vs|INCUMBENT_SCORE (NO_ADVANTAGE, -1.02 % /yr); US_EQUITY|XS|5|NEWS_INTENSITY|vs|INCUMBENT_SCORE (NO_ADVANTAGE, -1.37 % /yr); US_EQUITY|XS|5|NEWS_SENTIMENT_ONLY|vs|INCUMBENT_SCORE (NO_ADVANTAGE, -1.39 % /yr); US_EQUITY|XS|63|EARNINGS_EVENT_REACTION|vs|INCUMBENT_SCORE (NO_ADVANTAGE, -0.98 % /yr); CROSS_ASSET|XS|1|TREND|k10|b0.25 (NOT_ECONOMIC_UNDER_CONTROLS, +0.60 % /yr); CROSS_ASSET|XS|1|TREND|k1|b0.25 (NOT_ECONOMIC_UNDER_CONTROLS, +9.95 % /yr); CROSS_ASSET|XS|1|TREND|k21|b0.25 (NOT_ECONOMIC_UNDER_CONTROLS, +1.81 % /yr); CROSS_ASSET|XS|1|TREND|k5|b0.25 (NOT_ECONOMIC_UNDER_CONTROLS, +1.18 % /yr); FX_FUTURES|XS|1|CARRY|k1|b0.25 (NOT_ECONOMIC_UNDER_CONTROLS, +3.56 % /yr); SPY|1 (NO_DIRECTIONAL_SKILL, -0.30 % /yr); SPY|21 (NO_DIRECTIONAL_SKILL, -0.23 % /yr); SPY|5 (NO_DIRECTIONAL_SKILL, -0.17 % /yr); SPY|63 (NO_DIRECTIONAL_SKILL, -0.50 % /yr); RATES_FUTURES|XS|21|VOLATILITY_EXPECTATIONS_IV (REPRODUCTION_FAILED, +0.27 % /yr); FX_FUTURES|XS|21|POSITIONING_COMMITMENTS (REPRODUCTION_FAILED, -0.42 % /yr)

15. **Did the NEW intraday cross-asset information produce a non-incumbent signal?** No. 32 specifications across five bounded families on 500 sessions of owned one-minute history in GOLD, US_EQUITY_BETA, US_LONG_DURATION, US_TECH_BETA. Across the 30 PRIMARY specifications the largest Newey-West t at ZERO transaction cost is 1.72, and positive-gross arms are 53 % of the grid - what an information-free grid looks like. Cost is therefore NOT what killed the primary grid; there was no credible gross edge to kill. The engagement-conditioned RESCUE does reach 2.05 gross by lifting the per-trade edge above the round trip, but its NET t is still below the frozen 2.0, it is POST-SELECTION on the same 500 sessions, and over a denominator of 32 tests a single t near 2 is exactly what the null predicts. Benjamini-Hochberg rejects nothing. The best arm, INTRADAY|SESSION_CARRY|CARRY_REVERSION_SPY_LARGE_ONLY, is +4.64 % /yr standalone at t 1.27, but it collapses to +0.26 % /yr at the 5.0 bp stress cost, its second holdout half is negative, and its equal-risk incremental utility is t 0.98. Nothing qualified.

16. **Why was the options / implied-volatility axis not decided?** USABLE.  Exact missing requirement: a daily SPY option chain anchored on MONEYNESS rather than on fixed strikes - at minimum a +/-10 % moneyness band with two expiries beyond 18 days - over >= 2 years (~500 dates). The owned surface is 6 fixed expiries x 40 fixed strikes.

17. **How much research effort was non-price?** AUTONOMOUS research: 32 of 38 information-directed specifications (0.842) targeted non-PRICE_STATE information; rule >= 0.75 met: True. Counting the construction-only family too: 32 of 42 (0.762), met: True. Both denominators are reported and both must hold, so the rule cannot be met by reclassifying a family. ALL EXECUTED specifications including the OPERATOR-DIRECTED intraday axis: 32 of 128 (0.250), which is BELOW 0.75. Contract rule 14 scopes its threshold to autonomous research, so that third number is published for transparency rather than judged - it is reported as failing, not reclassified into compliance.

18. **What did AlphaAgent choose from the information frontier?** Isolated governor run (OK): 6 information needs offered, mandates: RATES_FUTURES|21|VOLATILITY_EXPECTATIONS_IV (eiv 0.819); VOLATILITY|21|VOLATILITY_EXPECTATIONS_IV (eiv 0.711); FX_FUTURES|5|CARRY (eiv 0.697); FX_FUTURES|21|POSITIONING_COMMITMENTS (eiv 0.690); CREDIT_PROXY|21|INFLATION_EXPECTATIONS (eiv 0.656); US_EQUITY|1|FREE_CASH_FLOW (eiv 0.746).

19. **What is the next highest-value information need?** Frontier: US_EQUITY|5|OWNERSHIP_INSTITUTIONAL_FLOW (remaining value 0.0251, None). Purchase case: EARNINGS_EXPECTATIONS_AS_WAS_CONSENSUS.

20. **How many stop-loss sessions remain?** 1 / 10 eligible sessions elapsed, 9 remaining, deadline 2026-09-24 (BEFORE_DEADLINE)

21. **If no survivor exists, are owned/free information sources exhausted?** True. Every one of the governor's mandated needs is now executed (0 left unexecuted). The frontier's top OWNED needs were measured rather than assumed: CREDIT_PROXY|TS|21|INFLATION_EXPECTATIONS (frontier rank 6): NOT_ECONOMIC_UNDER_CONTROLS, conditional t 2.86, economic increment +1.21 % /yr; US_EQUITY|XS|1|FREE_CASH_FLOW (frontier rank 2): NOT_ECONOMIC_UNDER_CONTROLS, conditional t 2.21, economic increment +3.62 % /yr; VOLATILITY|TS|1|VOLATILITY_EXPECTATIONS_IV (frontier rank 3): REPRODUCTION_FAILED, conditional t -1.21, economic increment +0.04 % /yr; VOLATILITY|TS|5|VOLATILITY_EXPECTATIONS_IV (frontier rank 3): REPRODUCTION_FAILED, conditional t -1.32, economic increment +0.29 % /yr. Two of them carry REAL conditional information and still fail the economics - the equity free-cash-flow cell improves a daily long-short book that loses 20.3 %/yr to cost and draws down 77.5 %, which the R64 degeneracy rule voids. Families closed: CROSS_ASSET_TREND_CADENCE, EARNINGS_EVENT_REACTION, EQUITY_INCUMBENT_CADENCE, FRONTIER_MANDATES_RISK_CONTROLLED, FX_CARRY_CADENCE, INCUMBENT_DECOMPOSITION, INTRADAY_CROSS_MARKET_LEADLAG, INTRADAY_OPENING_RANGE, INTRADAY_RELATIVE_STRENGTH, INTRADAY_SESSION_CARRY, INTRADAY_VOLATILITY_STATE, MARKET_DIRECTION_SPY, NEWS_INTENSITY. News sample: {'state': None, 'n_complete': None, 'items': 221409}.

22. **If yes, what EXACT data purchase experiment is economically justified?** EARNINGS_EXPECTATIONS_AS_WAS_CONSENSUS -> DO_NOT_BUY (the measured proxy evidence (0.0081/yr gross, halved) does not clear the break-even alpha even at the lowest fee on the ladder ($250/yr)); MONEYNESS_ANCHORED_OPTION_SURFACE -> DO_NOT_BUY (no owned / free proxy shows incremental value (R32 condition 3 fails first); no fee on the ladder could clear); SHORT_INTEREST_HISTORY -> DO_NOT_BUY (no owned / free proxy shows incremental value (R32 condition 3 fails first); no fee on the ladder could clear); SINGLE_NAME_OPTIONS_IV_SURFACE_HISTORY -> DO_NOT_BUY (no owned / free proxy shows incremental value (R32 condition 3 fails first); no fee on the ladder could clear); SUB_MINUTE_ORDER_FLOW -> DO_NOT_BUY (no owned / free proxy shows incremental value (R32 condition 3 fails first); no fee on the ladder could clear); UNIVERSE_WIDE_NEWS_HISTORY_PRE_2021 -> DO_NOT_BUY (no owned / free proxy shows incremental value (R32 condition 3 fails first); no fee on the ladder could clear)

## Full scoreboard

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
| ANALYST_EXPECTATIONS_REVISION_VINTAGES | NOT_OWNED | external_normalized/analyst_revision holds a 3-row mock fixture, an EMPTY normalized file and a 960-row / 40-ticker proxy; there is no revision vintage history to test |
| MACRO_EVENT_INTRADAY_REACTION | CLOSED_BY_R45 | Release 45's own data frontier records that the effect 'failed on its own holdout, in listed US rates and equities over two years, and in every other market the estate owns'. Re-running it would repeat closed work |
| NATIVE_CME_FUTURES_INTRADAY | CLOSED_NO_QUALIFIED_SIGNAL | the widest and deepest intraday panel this project has ever held - 2.5x the sessions and 9x the minutes of the closed ETF axis, across 10 contracts and 5 buckets - produced no arm reaching even gross t 2.0 at ZERO cost. The failure is information, not execution or coverage. |
| NATIVE_CME_FUTURES_MICROSTRUCTURE | CLOSED_NO_QUALIFIED_SIGNAL |  |
| NATIVE_INTRADAY_CROSS_ASSET | CLOSED_NO_QUALIFIED_SIGNAL | Across the 30 PRIMARY specifications the largest Newey-West t at ZERO transaction cost is 1.72, and positive-gross arms are 53 % of the grid - what an information-free grid looks like. Cost is therefore NOT what killed the primary grid; there was no credible gross edge to kill. The engagement-condit |
| OPTIONS_IMPLIED_VOLATILITY_SURFACE | USABLE |  |
| OWNERSHIP_INSTITUTIONAL_FLOW | NOT_OWNED | external_normalized/short_interest is EMPTY and the FINRA raw store is a 93-byte probe; 13F holdings are not on disk (only an EDGAR submissions cache), so the need remains blocked by an unbuilt CUSIP-to-ticker bridge AND by absent data |
| REVERSED_SPY_PUT_CALL_SKEW_H5 | HISTORICALLY_CONFIRMED_AWAITING_TRUE_FORWARD | a pre-registered sign was contradicted; the contradicted direction is frozen as a hypothesis and judged ONLY by evidence that did not choose it - an untouched window that precedes the discovery sample, and TRUE_FORWARD. Neither is a promotion and neither makes it capital-eligible. |

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


## Files, tests, audit, branch (reported last, as the contract requires)

- package: `alpha_agent/alpha_recovery/` (checkpoint, scoreboard, forecast_contract, incumbent, program, earnings_events, news, tournament, cadence, market_direction, equity_challengers, frontier_residual, forecast_products, intraday_data, intraday_alpha, options_surface, forward_package, purchase_case, report)
- runner: `scripts/run_alpha_recovery_offensive.py`; protocol: `research/alpha_recovery/ALPHA_RECOVERY_PROTOCOL.json`
- contract: `docs/ALPHA_RECOVERY_OPERATING_CONTRACT.md`; checkpoint: `research/alpha_recovery/alpha_recovery_checkpoint.json`
- tests: `tests/test_alpha_recovery_offensive.py`; audit: `check_alpha_recovery_operating_contract`
- research root: `D:\Stock_Prediction_app_data\alpha_recovery_offensive`
