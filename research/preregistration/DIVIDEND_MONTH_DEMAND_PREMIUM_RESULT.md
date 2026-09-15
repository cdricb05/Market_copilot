# RESULT - DIVIDEND-MONTH DEMAND PREMIUM IN POINT-IN-TIME S&P 500 PAYERS

**Mechanism:** `DIVIDEND_MONTH_DEMAND_PREMIUM_V1` (global candidate `GC_USEQ_DIVIDEND_MONTH_DEMAND`)
**Run:** ALPHA_STRIKE_SEP15_V2, verdict 3. Executed 2026-09-15 10:42-10:44 UTC through the canonical handler
body `alpha_agent.r59.mechanisms.execute_job` (116 s).
**Preregistration:** `b7ef8c4` (sha256 `185ff3bf...`). **Implementation:** `0f76ad6` (`e55ae9e5...`).
**Pin:** `2f44723`.
**Artifact:** `D:\Stock_Prediction_app_data\alpha_recovery_offensive\results\dividend_month_demand_premium.json`
(artifact_hash `5b6fd8e6...1db79d`).
**Input identity:** `NORGATE:S&P 500 Current & Past(TOTALRETURN,Dividend,S&P 500 flags):1897 securities (0 failed):1993-01-29..2026-09-14`.
**ResearchMemory:** `HM_DIVIDEND_MONTH_DEMAND_PREMIUM_V1`, outcome `NO_ALPHA_EVIDENCE`, settled 2026-09-15T10:44:26Z.

## VERDICT: `NO_EDGE` (gate 2, `WRONG_SIGN` at the frozen 12.5 bp cost). CAPITAL ELIGIBLE = NO.

Gate 1 passed: 1,897 securities with 0 load failures; 228 qualification and 164 confirmation months; 500.2
members a month; long leg 127.2 and short leg 269.6 names on average, no thin month; 0.20 % of members
unquoted at holding end.

| qualification 1994-01..2012-12, 228 months | 0 bp | **12.5 bp (primary)** | 25 bp | 50 bp |
|---|---|---|---|---|
| annualised net | +1.03 % | **-3.54 %** | -8.10 % | -17.22 % |
| NW t (lag 3) | 1.28 | **-4.42** | -10.10 | -21.43 |
| Sharpe | 0.29 | **-1.01** | -2.31 | -4.91 |
| hit rate | 57.5 % | **41.2 %** | 21.9 % | 4.4 % |
| maximum drawdown | -12.9 % | **-50.8 %** | -78.9 % | -96.3 % |

Diagnostics (never gated, never a rescue): mean turnover 3.04 x NAV a month; beta to SPY -0.04; the
same-calendar-month seasonality control book earned +3.46 %/yr gross over the same months. Mean monthly
gross by calendar month ranged from -0.53 % (April) to +0.44 % (June) with no quarterly pattern
matching the payment cycle.

The untouched 2013-01..2026-08 confirmation window was **not read**, as preregistered; the increment,
stability, drawdown, multiplicity and cost gates were not reached, and no confirmation month is written
in the artifact.

## What this means

Inside point-in-time S&P 500 payers, the names due to go ex-dividend this month did beat the payers not
due - by about 8.5 bp a month, t 1.28 on 19 years, the right sign but not statistically distinguishable
from zero and a small fraction of the published all-stock effect. The book cannot hold it: because the
long leg is a different third of the payers every month, it trades about three times NAV monthly, and
at the 12.5 bp desk rate that costs roughly 4.6 %/yr. Large-cap dividend-seeking demand, if it exists,
is too small against the trading it takes to follow the payment calendar.

**Not done, and never to be done:** t-3/t-6/t-9 predictions, quarterly-only or high-yield subsets, a
long-only or benchmark-relative leg, a lower cost rate, a smaller-cap universe, or reading the
confirmation window.

**For the frontier:** LIQUIDITY_PRESSURE gains an executed closed member; the global candidate has no
forward clock and closes by derivation. The census trap is kept for every later Norgate membership
test: a delisted security's last S&P 500 flag forward-fills past its final quote unless a recent close
is required.
