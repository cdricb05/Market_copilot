# RESULT - RELATIVE INDEX IMPLIED VOLATILITY (NDX AND RUT VERSUS SPX)

**Mechanism:** `RELATIVE_INDEX_IMPLIED_VOL_HEDGING_DEMAND_V1`
**Selected and executed by:** the Alpha Agent, session 1 of the continuous loop,
2026-09-13 22:23 UTC. No human prompt chose it.
**Preregistration:** `03bea7f` (sha256 `6d197d39...`). **Implementation:** `94b5523`
(`ed80fd70...`). **Pin:** `38d7f27`.
**Artifact:** `D:\Stock_Prediction_app_data\alpha_recovery_offensive\results\relative_index_implied_vol.json`
(artifact_hash `e3c11e24...3c59`). **Input identity:** FRED VIXCLS / VXNCLS / RVXCLS (sha256
manifest in `_data_implied_vol_indices`), Norgate SPY / QQQ / IWM total return.

## VERDICT: `KILLED_WRONG_SIGN` (gate 2, FROZEN_SIGN at the primary 2 bp rate). CAPITAL ELIGIBLE = NO.

| qualification 2004-01..2019-12, 191 monthly periods | 1 bp | 2 bp (primary) | 5 bp |
|---|---|---|---|
| annualised net | +0.39 % | -0.09 % | -1.53 % |
| NW t | 0.31 | -0.07 | -1.22 |
| Sharpe | 0.08 | -0.02 | -0.31 |
| max drawdown | -16.8 % | -19.2 % | -29.6 % |

Gross (0 bp): +0.87 %/yr, t 0.70. Both legs engaged on all 191 decisions; the data gate passed.

The gate fired on the frozen sign because the primary-rate mean is below zero; economically the
gross book is simply empty (t 0.70). The untouched 2020-01..2026-08 confirmation window was
**not read**, as preregistered.

## What this means

A rich implied-minus-realised premium in the Nasdaq-100 or Russell 2000 relative to the S&P 500
carried no after-cost information about their relative return over the next month. Index-specific
hedging demand, as far as these free Cboe indices can measure it, is not a market-neutral return
source. The mechanism is closed.

**Not done, and never to be done:** reversing the sign, one leg alone, another lookback, the
implied level or ratio, other horizons, futures legs, thresholds.

**For the live candidate:** nothing - the census had already shown the relative premium is
uncorrelated with its signal.

**For the frontier:** HEDGER_DEMAND_PRESSURE now holds five closed members (multiplier 0.33),
and DERIVATIVES_IMPLIED_INFORMATION has one eligible mechanism left in the owned/free estate
(implied correlation, RISK_PREMIUM_TIMING). The derivatives domain's remaining value sits with
the live candidate's forward evidence and the single-name option purchase gate.
