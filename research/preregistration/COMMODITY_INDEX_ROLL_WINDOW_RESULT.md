# RESULT - COMMODITY INDEX ROLL-WINDOW PRESSURE

**Mechanism:** `COMMODITY_INDEX_ROLL_WINDOW_PRESSURE_V1`
**Selected and executed by:** the Alpha Agent, session 8 of the continuous loop, 2026-09-13,
picked up automatically after the executor was pinned. No human prompt chose it.
**Preregistration:** `6e2c20d`. **Implementation:** `89f43d9`. **Pin:** `a14b3fb`.
**Artifact:** `D:\Stock_Prediction_app_data\alpha_recovery_offensive\results\commodity_index_roll_window.json`
(artifact_hash `e590594a...caf6c`). **Input identity:** `R38_NATIVE_LAYER` for HO, RB, NG, GC, SI,
HG, PL, PA, ZC, ZS, ZW, KE, ZL, ZM, KC, SB, CT, each through 2026-08-21.

## VERDICT: `KILLED_WRONG_SIGN` (gate 2, FROZEN_SIGN). CAPITAL ELIGIBLE = NO.

| qualification 1995-01..2014-12, 240 months | gross (0) | R38 cost (primary) | 2 x R38 cost |
|---|---|---|---|
| annualised net | -0.02 % | -3.48 % | -6.94 % |
| NW t | -0.11 | -17.56 | -35.06 |
| Sharpe | -0.02 | -3.92 | -7.82 |
| hit rate | 51.7 % | 9.6 % | 1.7 % |
| max drawdown | -7.0 % | -50.2 % | -75.0 % |

Mean monthly primary net -0.002899. Data gate passed: 240 of 240 qualification months had at
least 10 eligible markets (median 16, minimum 13). The seven markets whose owned contract rolls
inside or before the window (CL, BRN, HE, GAS, LE, CC, GF) were excluded before results, as
preregistered.

The passive-spread control, halves, leave-one-market-out, multiplicity and cost gates were never
reached. The untouched 2015-2026 confirmation window was **not read**, as preregistered.

## What this means

The label is "wrong sign", but the economics are sharper than that. Holding a long-deferred /
short-nearby spread across the published GSCI/BCOM roll days earned **exactly nothing gross**:
-0.02 %/yr, t -0.11, a 51.7 % hit rate over 240 months and 16 markets. The whole net loss is the
cost of four outright legs. The effect is not merely too small to trade: it is absent. This is not
a cost kill that a cheaper instrument could rescue, because a spread-instrument cost model cannot
create gross return that does not exist. On the owned 1995-2014 data the published index roll is
either pre-positioned before business day 3 or absorbed without a measurable concession.

**Not done, and never to be done:** reversing the sign; other entry or exit days; GSCI-only or
BCOM-only windows; re-admitting the excluded markets; market weights; a spread-instrument or halved
cost; reading the confirmation window.

**For the frontier:** FORCED_TRADING_FLOW now has two closed members from this campaign
(month-end balanced rebalancing and the commodity index roll), both with a gross effect
indistinguishable from zero. That lowers the class prior on the remaining forced-flow mechanism,
the Treasury auction concession, which the agent still ranks first on its own mechanism strength
and executes next.
