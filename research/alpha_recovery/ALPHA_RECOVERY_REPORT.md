# ALPHA RECOVERY SCOREBOARD

**STATUS: OWNED_FREE_INFORMATION_EXHAUSTED**

| field | value |
|---|---|
| INCUMBENT | fundamental_momentum_50_50_v1 (INCUMBENT_WEAK_OR_UNPROVEN) |
| BEST CHALLENGER | FX_FUTURES|XS|1|CARRY|k5|b0.25 (ECONOMIC_UNDER_CONTROLS_NOT_FDR) |
| HORIZON | 1 sessions |
| INFORMATION | FX_CARRY_CADENCE / CARRY |
| HISTORICAL OOS NET ADVANTAGE | +2.92 % /yr (incumbent +2.73 % /yr net excess) |
| SHARPE DELTA | 0.270 |
| DRAWDOWN DELTA | n/a |
| TURNOVER | challenger 0.087 one-way / period (incumbent 0.314) |
| FORECAST CALIBRATION | NOT_APPLICABLE (rank model) |
| MULTIPLICITY STATUS | paired t 1.95, conditional t 4.26, BH True, Holm False, failed gates none |
| TRUE_FORWARD OBSERVATIONS | 0 for any campaign challenger; incumbent book 35 sessions |
| FORWARD STATUS | NONE (no ALPHA_RECOVERY challenger is registered) |
| CAPITAL APPLICABILITY | futures sleeve; incremental to the incumbent under equal risk |
| DECISION | OWNED_FREE_INFORMATION_EXHAUSTED |

## The twenty answers

1. **Does the incumbent actually have demonstrated Alpha?** INCUMBENT_WEAK_OR_UNPROVEN. Historical OOS (2011-07 to 2026-07, top-25, 21 sessions): net excess +2.73 % /yr at t 0.99, rank-IC t 1.13, lockbox (2023+) +10.98 % /yr at t 1.59 versus selection +0.15 % /yr. TRUE_FORWARD (separate, never pooled): 35 sessions, -2.43 % cumulative versus SPY +2.01 %, excess -4.43 %. Checks: effective_periods_ge_floor=True, lockbox_sign_agrees=True, net_excess_ge_materiality=True, t_net_excess_ge_2=False, t_rank_ic_ge_2=False.

2. **What does the incumbent predict today, in economic units rather than scores?** the incumbent ranks 50 names; its rank score is NOT an expected return; no calibrated economic translation exists, so expected returns are UNAVAILABLE (snapshot 2026-09-09, fundamental data as of 2026-05-22, 199 names ranked).

3. **What is the strongest challenger?** FX_FUTURES|XS|1|CARRY|k5|b0.25: verdict ECONOMIC_UNDER_CONTROLS_NOT_FDR, family FX_CARRY_CADENCE, horizon 1.

4. **How much does it beat the incumbent AFTER COSTS?** Historical OOS net advantage +2.92 % /yr (paired t 1.95; lockbox +7.99 % /yr); Sharpe delta 0.270; drawdown delta n/a.

5. **Is the result statistically and economically credible?** Materiality True; paired t 1.95; conditional-information t 4.26; Benjamini-Hochberg True; Holm False; failed gates: none. POST_SELECTION: the R63/R64 lockbox of this family was viewed before this campaign

6. **Does it survive regimes?** FX_FUTURES|XS|1|CARRY|k5|b0.25: 0.90 of 3-year blocks positive, regimes {'iv_high': 0.0137, 'iv_low': 0.0117, 'trend_down': 0.0114, 'trend_up': 0.0136} (the conditional statistic's stability; the cadence book is POST_SELECTION); US_EQUITY|XS|1|EARNINGS_EVENT_REACTION|vs|INCUMBENT_SCORE: 0.00 of 3-year blocks positive, regimes {'iv_high': -0.0009, 'iv_low': -0.0006, 'trend_down': 0.0, 'trend_up': -0.0008}; US_EQUITY|XS|1|NEWS_INTENSITY|vs|INCUMBENT_SCORE: 0.00 of 3-year blocks positive, regimes {'iv_high': -0.0024, 'iv_low': -0.0238, 'trend_down': 0.0024, 'trend_up': -0.0175}

7. **Does it improve forecast calibration?** Cross-sectional rank models emit no probability; calibration is measured on the broad-market direction forecasts: SPY 1s: Brier skill -0.0179 (t -3.47), ECE 0.036, hit 0.545 vs always-up 0.553 -> NO_DIRECTIONAL_SKILL; SPY 21s: Brier skill -0.0068 (t -0.36), ECE 0.053, hit 0.730 vs always-up 0.730 -> NO_DIRECTIONAL_SKILL; SPY 5s: Brier skill -0.0106 (t -0.77), ECE 0.053, hit 0.636 vs always-up 0.636 -> NO_DIRECTIONAL_SKILL; SPY 63s: Brier skill -0.0283 (t -1.41), ECE 0.106, hit 0.779 vs always-up 0.779 -> NO_DIRECTIONAL_SKILL

8. **Is it ready for TRUE_FORWARD competition?** OWNED_FREE_INFORMATION_EXHAUSTED. READY records: 0; survivors not qualified: 5.

9. **What exact human-gated command would start that competition?** Only a READY record may be offered. None is READY. The command shape, for review, is: `C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe scripts\adopt_prospective_freeze.py --challenger-id ALPHA_RECOVERY_FX_CARRY_CADENCE_H1_F9B1ACA7 --confirm ADOPT_PROSPECTIVE_FORWARD_CLOCKS --execute` (dry run without --execute; a live registration requires the typed confirmation token).

10. **What broad-market direction forecasts can we now produce?** SPY 1s: probability_up 0.536 (climatology 0.547) -> UNAVAILABLE (uncalibrated OOS); SPY 21s: probability_up 0.760 (climatology 0.693) -> UNAVAILABLE (uncalibrated OOS); SPY 5s: probability_up 0.639 (climatology 0.609) -> UNAVAILABLE (uncalibrated OOS); SPY 63s: probability_up 0.639 (climatology 0.770) -> UNAVAILABLE (uncalibrated OOS)

11. **What equity expected-return forecasts can we now produce?** the incumbent ranks 50 names; its rank score is NOT an expected return; no calibrated economic translation exists, so expected returns are UNAVAILABLE The forecast contract carries the ranks as labelled scores with expected_excess_return UNAVAILABLE.

12. **What multi-asset forecasts can we now produce?** FX carry sleeve (FX_FUTURES|XS|1|CARRY|k21|b0.50): incumbent-only Sharpe 0.929 versus incumbent + sleeve at equal risk 0.831; incremental net -1.66 % /yr (t -0.95); OK. No calibrated sleeve expected return exists; the sleeve forecast fields are UNAVAILABLE.

13. **What information actually added value?** QUALIFIED (every frozen gate): none. MEASURED BUT NOT QUALIFIED (conditional information value real, economics positive under controls, multiplicity or the equal-risk utility test failed): FX_FUTURES|XS|1|CARRY|k10|b0.25 (ECONOMIC_UNDER_CONTROLS_NOT_FDR, +2.40 % /yr); FX_FUTURES|XS|1|CARRY|k21|b0.25 (ECONOMIC_UNDER_CONTROLS_NOT_FDR, +1.60 % /yr); FX_FUTURES|XS|1|CARRY|k21|b0.50 (ECONOMIC_UNDER_CONTROLS_NOT_FDR, +1.54 % /yr); FX_FUTURES|XS|1|CARRY|k5|b0.25 (ECONOMIC_UNDER_CONTROLS_NOT_FDR, +2.92 % /yr); FX_FUTURES|XS|1|CARRY|k5|b0.50 (ECONOMIC_UNDER_CONTROLS_NOT_FDR, +2.44 % /yr). The FX carry sleeve adds no utility to the incumbent-only book at equal risk (-1.66 % /yr, t -0.95).

14. **What information was rejected?** US_EQUITY|XS|1|EARNINGS_EVENT_REACTION|vs|INCUMBENT_SCORE (NO_ADVANTAGE, +1.97 % /yr); US_EQUITY|XS|1|NEWS_INTENSITY|vs|INCUMBENT_SCORE (WORSE_THAN_INCUMBENT, -3.46 % /yr); US_EQUITY|XS|21|EARNINGS_EVENT_REACTION|vs|INCUMBENT_SCORE (NO_ADVANTAGE, -0.38 % /yr); US_EQUITY|XS|21|EARNINGS_EVENT_REACTION|vs|PRICE_RETURN_STATE+TREND+MOMENTUM+REVERSAL+REALISED_VOLATILITY+TAIL_CRASH_STATE+LIQUIDITY+VOLUME_PARTICIPATION+FUNDAMENTAL_LEVELS+FREE_CASH_FLOW (NO_ADVANTAGE, +0.60 % /yr); US_EQUITY|XS|21|EARNINGS_REACTION_ONLY|vs|INCUMBENT_SCORE (NO_ADVANTAGE, -1.30 % /yr); US_EQUITY|XS|21|EARNINGS_SURPRISE_ONLY|vs|INCUMBENT_SCORE (NO_ADVANTAGE, +0.81 % /yr); US_EQUITY|XS|5|EARNINGS_EVENT_REACTION|vs|INCUMBENT_SCORE (NO_ADVANTAGE, -0.52 % /yr); US_EQUITY|XS|5|NEWS_COUNT_ONLY|vs|INCUMBENT_SCORE (NO_ADVANTAGE, -1.02 % /yr); US_EQUITY|XS|5|NEWS_INTENSITY|vs|INCUMBENT_SCORE (NO_ADVANTAGE, -1.37 % /yr); US_EQUITY|XS|5|NEWS_SENTIMENT_ONLY|vs|INCUMBENT_SCORE (NO_ADVANTAGE, -1.39 % /yr); US_EQUITY|XS|63|EARNINGS_EVENT_REACTION|vs|INCUMBENT_SCORE (NO_ADVANTAGE, -0.98 % /yr); CROSS_ASSET|XS|1|TREND|k10|b0.25 (NOT_ECONOMIC_UNDER_CONTROLS, +0.60 % /yr); CROSS_ASSET|XS|1|TREND|k1|b0.25 (NOT_ECONOMIC_UNDER_CONTROLS, +9.95 % /yr); CROSS_ASSET|XS|1|TREND|k21|b0.25 (NOT_ECONOMIC_UNDER_CONTROLS, +1.81 % /yr); CROSS_ASSET|XS|1|TREND|k5|b0.25 (NOT_ECONOMIC_UNDER_CONTROLS, +1.18 % /yr); FX_FUTURES|XS|1|CARRY|k1|b0.25 (NOT_ECONOMIC_UNDER_CONTROLS, +3.56 % /yr); SPY|1 (NO_DIRECTIONAL_SKILL, -0.30 % /yr); SPY|21 (NO_DIRECTIONAL_SKILL, -0.23 % /yr); SPY|5 (NO_DIRECTIONAL_SKILL, -0.17 % /yr); SPY|63 (NO_DIRECTIONAL_SKILL, -0.50 % /yr); RATES_FUTURES|XS|21|VOLATILITY_EXPECTATIONS_IV (REPRODUCTION_FAILED, +0.27 % /yr); FX_FUTURES|XS|21|POSITIONING_COMMITMENTS (REPRODUCTION_FAILED, -0.42 % /yr)

15. **How much research effort was non-price?** 24 of 28 executed specifications (0.857) targeted non-PRICE_STATE information; rule >= 0.75 met: True.

16. **What did AlphaAgent choose from the information frontier?** Isolated governor run (OK): 6 information needs offered, mandates: RATES_FUTURES|21|VOLATILITY_EXPECTATIONS_IV (eiv 0.819); VOLATILITY|21|VOLATILITY_EXPECTATIONS_IV (eiv 0.711); FX_FUTURES|5|CARRY (eiv 0.697); FX_FUTURES|21|POSITIONING_COMMITMENTS (eiv 0.690); CREDIT_PROXY|21|INFLATION_EXPECTATIONS (eiv 0.656); US_EQUITY|1|FREE_CASH_FLOW (eiv 0.746).

17. **What is the next highest-value information need?** Frontier: VOLATILITY|21|VOLATILITY_EXPECTATIONS_IV (remaining value 0.0537, None). Purchase case: EARNINGS_EXPECTATIONS_AS_WAS_CONSENSUS.

18. **How many stop-loss sessions remain?** 0 / 10 eligible sessions elapsed, 10 remaining, deadline 2026-09-24 (BEFORE_DEADLINE)

19. **If no survivor exists, are owned/free information sources exhausted?** True. Families closed: CROSS_ASSET_TREND_CADENCE, EARNINGS_EVENT_REACTION, FX_CARRY_CADENCE, MARKET_DIRECTION_SPY, NEWS_INTENSITY. News sample: {'state': None, 'n_complete': None, 'items': 221409}.

20. **If yes, what EXACT data purchase experiment is economically justified?** EARNINGS_EXPECTATIONS_AS_WAS_CONSENSUS -> DO_NOT_BUY (the measured proxy evidence (0.0081/yr gross, halved) does not clear the break-even alpha even at the lowest fee on the ladder ($250/yr)); SHORT_INTEREST_HISTORY -> DO_NOT_BUY (no owned / free proxy shows incremental value (R32 condition 3 fails first); no fee on the ladder could clear); SINGLE_NAME_OPTIONS_IV_SURFACE_HISTORY -> DO_NOT_BUY (no owned / free proxy shows incremental value (R32 condition 3 fails first); no fee on the ladder could clear); UNIVERSE_WIDE_NEWS_HISTORY_PRE_2021 -> DO_NOT_BUY (no owned / free proxy shows incremental value (R32 condition 3 fails first); no fee on the ladder could clear)

## Full scoreboard

# ALPHA RECOVERY SCOREBOARD

**STATUS: OWNED_FREE_INFORMATION_EXHAUSTED**

Stop-loss clock: 0 / 10 eligible sessions elapsed, 10 remaining, deadline 2026-09-24 (BEFORE_DEADLINE)
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

## Campaign counters

- eligible sessions elapsed / 10: 0 / 10 (remaining 10)
- share of new research effort on non-price information: 0.857 (rule >= 0.75: True)
- economically distinct information families tested: 6 (CROSS_ASSET_TREND_CADENCE, EARNINGS_EVENT_REACTION, FRONTIER_MANDATES_RISK_CONTROLLED, FX_CARRY_CADENCE, MARKET_DIRECTION_SPY, NEWS_INTENSITY)
- candidate specifications alive: 5
- in TRUE_FORWARD competition: 0
- highest-value unresolved information gap: VOLATILITY|21|VOLATILITY_EXPECTATIONS_IV
- top missing information need (purchase case): EARNINGS_EXPECTATIONS_AS_WAS_CONSENSUS

## Every candidate

| cell | kind | verdict | net advantage /yr | t | Sharpe delta | lockbox advantage | BH | Holm | failed gates |
|---|---|---|---|---|---|---|---|---|---|
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

- package: `alpha_agent/alpha_recovery/` (checkpoint, scoreboard, forecast_contract, incumbent, program, earnings_events, news, tournament, cadence, market_direction, forward_package, purchase_case, report)
- runner: `scripts/run_alpha_recovery_offensive.py`; protocol: `research/alpha_recovery/ALPHA_RECOVERY_PROTOCOL.json`
- contract: `docs/ALPHA_RECOVERY_OPERATING_CONTRACT.md`; checkpoint: `research/alpha_recovery/alpha_recovery_checkpoint.json`
- tests: `tests/test_alpha_recovery_offensive.py`; audit: `check_alpha_recovery_operating_contract`
- research root: `D:\Stock_Prediction_app_data\alpha_recovery_offensive`
