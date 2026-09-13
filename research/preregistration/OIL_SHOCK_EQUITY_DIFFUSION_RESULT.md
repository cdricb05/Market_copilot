# RESULT - GRADUAL DIFFUSION OF OIL SHOCKS INTO THE EQUITY INDEX

**Mechanism:** `OIL_SHOCK_EQUITY_SLOW_DIFFUSION_V1`
**Selected and executed by:** the Alpha Agent, session 4 of the continuous loop, 2026-09-13,
picked up automatically after the executor was pinned. No human prompt chose it.
**Preregistration:** `ee47d2a`. **Implementation:** `0e11c07`. **Pin:** `403f9c8`.
**Artifact:** `D:\Stock_Prediction_app_data\alpha_recovery_offensive\results\oil_shock_equity_diffusion.json`
(artifact_hash `c73ef19b...a652b`). **Input identity:**
`NORGATE:&BRN(+_CCB):1988-06-24..2026-09-11 | NORGATE_SPY_TOTALRETURN:1993-01-29..2026-09-11`.

## VERDICT: `KILLED_WRONG_SIGN` (gate 2, FROZEN_SIGN). CAPITAL ELIGIBLE = NO.

| qualification 1993-03..2012-12, 238 months | 1 bp | 2 bp | 5 bp |
|---|---|---|---|
| annualised net | -2.09 % | -2.33 % | -3.05 % |
| NW t | -0.57 | -0.63 | -0.83 |
| Sharpe | -0.13 | -0.14 | -0.19 |
| max drawdown | -60.7 % | -61.6 % | -64.2 % |

Gross (0 bp): -1.85 %/yr, t -0.50, hit rate 47.9 %. Data gate passed: 238 of 238 months finite,
154 months in 1993-2005 and 84 in 2006-2012.

The reversal-confound gate was never reached. The untouched 2013-2026 confirmation window was
**not read**, as preregistered.

## What this means

On Brent futures and SPY total return, the sign of last month's oil shock did not predict next
month's S&P 500 in the declared direction; if anything, a rising oil month was followed by
slightly better equity returns, and the difference from zero is noise. The gradual-diffusion
channel is closed for the monthly index horizon.

**Not done, and never to be done:** reversing the sign (the reversed book would be the same
noise); magnitude or threshold versions; WTI; other lags or horizons; energy-sector legs.

**For the frontier:** INFORMATION_DIFFUSION_SPEED gains a closed member (six in the ledger and
memory), which lowers the peer-earnings transfer mechanism further.
