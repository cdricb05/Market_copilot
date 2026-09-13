# RESULT - SPX TAIL-HEDGING DEMAND (CBOE SKEW), 1993-2022

**Mechanism:** `SPX_TAIL_HEDGE_DEMAND_SKEW_INDEX_1993_2022_V1`
**Selected and executed by:** the Alpha Agent (mechanism frontier -> canonical queue -> pinned
executor), session 1 of the continuous loop, 2026-09-13 22:23 UTC. No human prompt chose it.
**Preregistration:** `ad14c56` (sha256 `be4808b7...`). **Implementation:** `c05bcce`
(`70358bb1...`). **Pin:** `38d7f27`.
**Artifact:** `D:\Stock_Prediction_app_data\alpha_recovery_offensive\results\spx_tail_hedge_skew_index.json`
(artifact_hash `b5960353...fe45f`). **Input identity:** `CBOE_SKEW:790547da55a75ee8 |
NORGATE_SPY_TOTALRETURN:1993-01-29..2026-09-11:8462`.

## VERDICT: `KILLED_WRONG_SIGN` (gate 2, FROZEN_SIGN). CAPITAL ELIGIBLE = NO.

| qualification 1993-01..2022-07, 1,477 weekly periods | 1 bp | 2 bp | 5 bp |
|---|---|---|---|
| annualised net | -1.32 % | -2.32 % | -5.35 % |
| NW t | -0.42 | -0.74 | -1.71 |
| hit rate | 47.9 % | 47.6 % | 46.6 % |
| max drawdown | -68.1 % | -74.1 % | -87.7 % |

Gross (0 bp): -0.31 %/yr, t -0.10. Partition annualised net at 1 bp: P1 1993-99 +0.20 %,
P2 2000-09 -1.92 %, P3 2010-19 +0.36 %, P4 2020-22 -9.45 %. Data gate passed (99.8 % finite).

The untouched 2022-08..2026-08 confirmation window was **not read**, as preregistered.

## What this means

An abnormally steep Cboe SKEW, read the session before entry, carried no information about the
next five SPY sessions over thirty years: the gross book is indistinguishable from zero and the
net book loses its costs. The mechanism is closed.

**Not done, and never to be done:** reversing the sign (the reversed gross book would earn
+0.31 %/yr at t 0.10, which is nothing); another lookback, horizon, threshold or level cell;
dropping 2020-2022.

**For the live candidate:** nothing. The preregistered census had already established that the
SKEW z-score is uncorrelated with `REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1`'s signal, so this
null neither weakens nor supports it.

**For the frontier:** the HEDGER_DEMAND_PRESSURE class gains a closed member, which lowers the
measured class multiplier for the remaining hedging-demand work.
