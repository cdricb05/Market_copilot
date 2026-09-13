# RESULT - IMPLIED CORRELATION PREMIUM AND INDEX TIMING

**Mechanism:** `IMPLIED_CORRELATION_PREMIUM_INDEX_TIMING_V1`
**Selected and executed by:** the Alpha Agent, session 11 of the continuous loop, 2026-09-13,
picked up automatically after the executor was pinned. No human prompt chose it.
**Preregistration:** `2add128`. **Implementation:** `c6bf23f`. **Pin:** `53a62b8`.
**Artifact:** `D:\Stock_Prediction_app_data\alpha_recovery_offensive\results\implied_correlation_premium.json`
(artifact_hash `762df35b...8f25a`). **Input identity:** `CBOE_COR3M:147593ee93b6f4a5:2006-01-03..2026-09-11 |
R57_SP500_PIT_PANEL:9c35d293...:2004-06-01..2026-09-03 | NORGATE_SPY_TOTALRETURN:1993-01-29..2026-09-11`.

## VERDICT: `NO_EDGE` (gate 3, STANDALONE_T). CAPITAL ELIGIBLE = NO.

| qualification, entries 2008-01..2018-12, 132 monthly decisions | gross | 1 bp (primary) | 2 bp | 5 bp |
|---|---|---|---|---|
| annualised net | +6.91 % | +6.67 % | +6.43 % | +5.71 % |
| NW t | 1.30 | **1.26** | 1.21 | 1.07 |
| one-sided p | 0.097 | 0.105 | 0.113 | 0.141 |
| Sharpe | 0.39 | 0.38 | 0.36 | 0.32 |
| hit rate | 52.3 % | 51.5 % | 50.8 % | 50.8 % |
| max drawdown | -32.3 % | -32.7 % | -33.0 % | -34.0 % |

Data gate passed: 100 % finite z and 60 / 72 decisions in the two partitions, with a median of 502
names in the realised correlation. The timing-increment gate (volatility- and regression-matched),
the partition-sign gate, BH at m=8 and the incumbent gate were never reached. The untouched
2019-2026 confirmation window was **not read**, as preregistered.

## What this means

The sign was right, but the evidence is not there. A +/-1 SPY sign book carries index-like
volatility, so +6.7 %/yr at a Sharpe of 0.38 over eleven years cannot be told apart from luck (t
1.26). The raw mean was never tested against simply holding the index, which is where every earlier
implied-state timing test failed. Even the one-sided p of 0.105 would need to be at most 0.0125 to
survive the seven inherited nulls. On COR3M's methodology-backfilled history, the correlation risk
premium's trailing z-score does not time the S&P 500.

By its own declared decision rule, implied correlation joins the VIX level, VIX term structure,
VVIX/SKEW conditioners, the SKEW tail-hedge index and relative index implied volatility: **a real
option premium with no tradable index-timing signal.** Index-level implied-state timing is closed
for the derivatives domain.

**Not done, and never to be done:** the premium level instead of its z; other z windows or
lookbacks; cap-weighted, top-50 or sector realised correlation; COR1M, COR6M, COR1Y or DSPX;
thresholds or magnitude sizing; long-only or short-only books; other horizons; futures legs; dropping
2008, 2020 or 2022; reading the confirmation window.

**For the frontier:** RISK_PREMIUM_TIMING and the DERIVATIVES_IMPLIED_INFORMATION domain lose their
last free index-level mechanism. The derivatives domain's remaining route is cross-sectional option
information (SINGLE_NAME_OPTION_INFORMED_TRADING_V1), which is at the human purchase gate.
