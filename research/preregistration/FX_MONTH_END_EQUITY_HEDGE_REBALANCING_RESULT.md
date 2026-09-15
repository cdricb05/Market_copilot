# RESULT - MONTH-END CURRENCY HEDGE REBALANCING AT THE WM/R FIX (EUR, JPY, CHF, CAD FUTURES)

**Mechanism:** `FX_MONTH_END_EQUITY_HEDGE_REBALANCING_V1` (global candidate `GC_FX_MONTH_END_EQUITY_HEDGE_FIX_FLOW`)
**Run:** ALPHA_STRIKE_SEP15_V2, verdict 2. Executed 2026-09-15 10:23 UTC through the canonical handler body
`alpha_agent.r59.mechanisms.execute_job`.
**Preregistration:** `05a74f3` (sha256 `89dc1f7e...`). **Implementation:** `c9628c1` (`39b55160...`).
**Pin:** `fdd97f8`.
**Artifact:** `D:\Stock_Prediction_app_data\alpha_recovery_offensive\results\fx_month_end_equity_hedge_rebalancing.json`
(artifact_hash `36f16f51...ff666`).
**Input identity:** `NORGATE:&ES,&6E/&FESX,&6J/&NKD,&6S/&FSMI,&6C/&SXF(+_CCB):1979-03-06..2026-09-14;VALIDATION:FXE,FXY`.
**ResearchMemory:** `HM_FX_MONTH_END_EQUITY_HEDGE_REBALANCING_V1`, outcome `NO_ALPHA_EVIDENCE`, settled 2026-09-15T10:23:42Z.

## VERDICT: `NO_EDGE` (gate 2, `WRONG_SIGN`). CAPITAL ELIGIBLE = NO.

Gate 1 passed: 180 of 180 qualification months and 140 of 140 confirmation months with all four pairs
finite; the roll-free 6E leg correlates 0.960 with FXE total return (2,262 sessions) and 6J 0.949 with
FXY (1,973 sessions), floors 0.90.

| qualification 2000-01..2014-12, 180 fix sessions | 0 bp | 1 bp | **3 bp (primary)** | 6 bp |
|---|---|---|---|---|
| annualised net | -0.51 % | -1.62 % | **-3.83 %** | -7.15 % |
| NW t | -0.38 | -1.19 | **-2.81** | -5.22 |
| Sharpe | -0.10 | -0.31 | **-0.72** | -1.34 |
| hit rate | 51.7 % | 46.7 % | **40.6 %** | 31.7 % |
| maximum drawdown | -15.6 % | -26.4 % | **-46.7 %** | -67.4 % |

Diagnostics (never gated, never a rescue): mean gross notional 4.6 x NAV per window, 111 x NAV turnover a
year, one session held per month. Annualised net at 3 bp by currency: EUR -0.91 %, JPY -0.29 %, CHF -0.49 %,
CAD -2.13 % - every currency negative. Quarter-end months +1.40 %/yr against other months -6.45 %/yr.

The untouched 2015-01..2026-08 confirmation window was **not read**, as preregistered; the increment,
stability, drawdown, multiplicity and cost gates were not reached.

## What this means

On the session that contains the month-end fix, signing the four major currencies against their equity
markets' month-to-date performance relative to the S&P 500 earned nothing before costs (-0.5 %/yr, t
-0.38 on 180 months) - even on the 2000-2014 window that overlaps the published sample. Whatever hedge
adjustment reaches the fix is either too small against one day of currency noise, already priced by
the close before, or absorbed intraday before the futures settle. With one unit of risk per currency
the round trips alone cost about 3.3 %/yr, so the frozen book lost money at every cost rung above zero.

**Not done, and never to be done:** a quarter-end-only cell (the +1.40 %/yr diagnostic is exactly the
subset rescue the preregistration forbids), another signal session or window, GBP/AUD/DXY legs, smaller
sizing, ETF legs, or reading the confirmation window.

**For the frontier:** FORCED_TRADING_FLOW gains a third executed closed member (month-end equity-bond
rebalancing, commodity index roll, month-end currency hedge rebalancing). The global candidate has no
forward clock, so the frontier closes it by derivation from this settled verdict.
