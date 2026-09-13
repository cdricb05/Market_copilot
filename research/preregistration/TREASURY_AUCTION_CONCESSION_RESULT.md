# RESULT - TREASURY AUCTION SUPPLY CONCESSION

**Mechanism:** `TREASURY_AUCTION_SUPPLY_CONCESSION_V1`
**Selected and executed by:** the Alpha Agent, session 9 of the continuous loop, 2026-09-13,
picked up automatically after the executor was pinned. No human prompt chose it.
**Preregistration:** `dadda9e`. **Implementation:** `d0c7b94`. **Pin:** `383c8a7`.
**Artifact:** `D:\Stock_Prediction_app_data\alpha_recovery_offensive\results\treasury_auction_concession.json`
(artifact_hash `b6cdb4fc...a3555`, input identity `dd25b8ab...86b97`). **Auction cache:** 1,635 TA_WS
rows, sha256 `019bea4f...d186`.

## VERDICT: `KILLED_WRONG_SIGN` (kill rule `SHORT_DOES_NOT_EARN`). CAPITAL ELIGIBLE = NO.

**Qualification: 2000-2016, 295 clusters, 436 events** (5y ZF 174, 10y ZN 154, 30y ZB 108), about
17 clusters a year. All figures are fractions of one unit of ZN-equivalent DV01 notional per auction,
summed per year.

| DV01-matched pre-auction short | gross | 1 bp | 2 bp (gate, R38) | 5 bp |
|---|---|---|---|---|
| annualised | -1.05 % | -1.60 % | -2.15 % | -3.81 % |
| NW t (clusters, lag 0) | -1.16 | | -2.38 | |

At the gate cost the hit rate is 39.3 % and the maximum drawdown -38.2 %.

| diagnostic | mean | t |
|---|---|---|
| 5y leg gross | -0.029 % / event | -0.74 |
| 10y leg gross | -0.091 % / event | -1.78 |
| 30y leg gross | +0.011 % / event | +0.20 |
| announcement-to-entry gross (priced before entry?) | -0.018 % / cluster | -0.50 |
| excess over the always-short book of the same tenors | +0.016 % / cluster | +0.30 |

Multiplicity failed both at m=1 and at m=3, with the two inherited nulls at p=1; the one-sided p was
0.991. The untouched 2017-2026 confirmation window was **not read**, as preregistered.

The artifact's `kill_rule_cost_bps_per_side: 1.0` is the floor constant. The gate charged
`max(1.0, R38) = 2.0` bp per side on every leg, as the frozen kill rule requires, and the verdict
fired on the gross sign before any cost.

## What this means

Dealers do not cheapen the auctioned tenor in the sessions between the point-in-time announcement
and the auction, at least not in a way a futures short can collect. The pre-auction short lost
before costs. The loss is the 2000-2016 bond bull market and nothing else: the always-short book of
the same futures lost at the same rate, and the conditional excess over it is +0.016 % per cluster
at t 0.30. Nor was the concession already done before entry: announcement-to-entry is flat. The
three tenor legs do not even agree with one another.

The artifact also reports a descriptive post-auction number (t -4.47 for the short). It sits within
the same secular drift, it was not a preregistered cell, and it has now been seen. It is **not** a
candidate, and no post-auction recovery mechanism may be declared on 2000-2016 data.

**Not done, and never to be done:** reversing the sign; re-admitting the 2y, 3y, 7y or 20y legs;
other entry or exit sessions; tenor-only books; cash-bond or when-issued instruments on this
evidence; reading the confirmation window.

**For the frontier:** FORCED_TRADING_FLOW now has three closed members from this campaign
(month-end balanced rebalancing, the commodity index roll, the Treasury auction concession). Each
was measured in liquid futures with point-in-time entry, and each had a gross effect
indistinguishable from zero. The remaining rates-flow mechanism,
TREASURY_INDEX_MONTH_END_DURATION_EXTENSION_V1, inherits this null at p=1 (kill rule tightened
before any return). By its own declared decision rule, its failure would close forced Treasury
supply-and-demand timing.
