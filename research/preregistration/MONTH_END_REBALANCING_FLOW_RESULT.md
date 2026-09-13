# RESULT - MONTH-END BALANCED-FUND REBALANCING FLOW (ES VERSUS ZN)

**Mechanism:** `MONTH_END_BALANCED_REBALANCING_FLOW_V1`
**Selected and executed by:** the Alpha Agent, session 1 of the continuous loop,
2026-09-13 22:23 UTC. No human prompt chose it.
**Preregistration:** `0a55ec1` (sha256 `8c393f61...`). **Implementation:** `bd88c9e`
(`583bf8ad...`). **Pin:** `38d7f27`.
**Artifact:** `D:\Stock_Prediction_app_data\alpha_recovery_offensive\results\month_end_rebalancing_flow.json`
(artifact_hash `bf7ddfd7...97c7a`). **Input identity:** `NORGATE:&ES,&ZN(+_CCB):1982-05-04..2026-09-11`.

## VERDICT: `NO_EDGE` (gate 3, STANDALONE_T). CAPITAL ELIGIBLE = NO.

| qualification 1998-01..2014-12, 204 monthly windows | 1 bp | 2 bp | 5 bp |
|---|---|---|---|
| annualised net | +0.99 % | +0.38 % | -1.44 % |
| NW t | 0.56 | 0.22 | -0.81 |
| Sharpe | 0.13 | 0.05 | -0.20 |
| max drawdown | -19.1 % | -21.8 % | -34.3 % |

Gross (0 bp): +1.60 %/yr, t 0.90, hit rate 50.0 %. The data gates passed: the roll-free ES leg
correlates 0.993 with SPY total return and the ZN leg 0.939 with IEF (floors 0.95 / 0.85);
204 of 204 windows finite; 68 quarter-end and 136 other months.

The untouched 2015-01..2026-08 confirmation window was **not read**, as preregistered.

## What this means

Signing an equal-volatility equity-versus-bond spread against the month-to-date relative return
points the right way, but the effect is about a tenth of what a 1.5 %/yr material, t >= 2 book
would need and it does not survive a 2 bp rate. Whatever mandated rebalancing flow exists at
month end is absorbed at a price that leaves nothing after costs on these instruments. The
mechanism is closed in both its unconditional (R32) and conditional forms.

**Not done, and never to be done:** another window length, quarter-end-only or large-move-only
cells, thresholds, dollar-neutral weights, ETF legs, or year exclusions.

**For the frontier:** FORCED_TRADING_FLOW gains a closed member; the measured multiplier lowers
the Treasury-auction and commodity-roll mechanisms (0.6895 -> 0.6853 and 0.6035 -> 0.6000).
