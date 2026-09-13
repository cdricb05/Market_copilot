# RESULT - BOND INDEX MONTH-END DURATION EXTENSION

**Mechanism:** `TREASURY_INDEX_MONTH_END_DURATION_EXTENSION_V1`

**Selected and executed by:** the Alpha Agent, session 13 of the continuous loop, 2026-09-13. The
agent picked it up automatically after the executor was pinned; no human prompt chose it.

**Commits:** preregistration `b4a7580`, implementation `e0cace5`, pin `1b6322f`. The kill rule was
tightened before any return in `fc7833b`.

**Artifact:** `D:\Stock_Prediction_app_data\alpha_recovery_offensive\results\treasury_month_end_duration_extension.json`
(artifact_hash `e9ad93f2...751f4`, input identity `3eda14ca...0f2`). It was built from the TA_WS cache
(sha256 `019bea4f...d186`) and the R38 ZN layer.

## VERDICT: `KILLED_BELOW_MATERIALITY` (gate 4, `NET_BELOW_1.5PCT_PER_YEAR`). CAPITAL ELIGIBLE = NO.

**Qualification, ranked months 2001-01..2016-12:** 192 months, 103 high / 89 low (high share 53.6 %).
The unit is one unit of ZN notional per month, times 12 per year.

| extension-ranked month-end long | gross | 2 bp (gate, R38) | 5 bp (stress) |
|---|---|---|---|
| annualised | +1.32 % | **+0.93 %** | +0.35 % |
| NW t (lag 0) | 3.48 | 2.50 | |
| one-sided p | 0.0003 | 0.0063 | |

At the gate cost the book's hit rate in active months is 62.1 %, its maximum drawdown -1.76 %, and the
gross mean per active month +0.205 %.

Gates 1-3 passed:
- data;
- sample (103 / 89 months);
- the frozen sign;
- NW t 2.50 at 2 bp.

Gate 4 failed: 0.93 %/yr is below 1.5 %/yr. The later gates did not decide the verdict, but the
artifact computed their inputs, reported here for the record:

| later gate | value | would it pass? |
|---|---|---|
| 5: increment over the unconditional month-end long | +0.14 %/yr, NW t **0.62**, p 0.27 | no |
| 6: halves | +0.96 %/yr (2001-2008), +0.90 %/yr (2009-2016) | yes |
| 7: BH at m=3 | p 0.006 | yes |
| 8: stress cost | 0.35 %/yr | no |

The untouched 2017-2026 confirmation window was **not read**, as preregistered.

## What this means

**The declared mechanism is dead twice over.** Sizing the month-end long by the duration that
point-in-time settled issuance adds does not select better month ends: the increment over being long
at every month end is t 0.62. What remains of the book is a thinner copy of the unconditional
month-end long. It is active in only half the months, and it pays extra roll round trips because
refunding months are also roll months (rolls hit 50.5 % of high months against 13.5 % of low). That
leaves 0.93 %/yr per unit of ZN notional. The catalog's decision rule applies: both Treasury flow
mechanisms have failed, so **issuance-sized Treasury supply-and-demand timing is closed.**

**The one number the human should see is the control, and it is not a candidate.** The
unconditional month-end long in ZN (the frozen increment's baseline) earned +2.11 %/yr gross (t 4.59)
and +1.47 %/yr at 2 bp (t 3.20), with a maximum drawdown of -2.63 %. That is consistent with a
month-end index-extension demand that does not depend on how much issuance settled. It was observed as
a control of a failed cell, not preregistered, and section 12 of the preregistration forbids any
second cell. The estate's permanent rule is no rename and no rescue, so it **cannot be adopted from
this evidence.** The only admissible route would be a new, human-authorised preregistration read
solely on the still-untouched 2017-2026 window, with this search counted in its multiplicity. That is
recorded as an open human judgement, not done.

**Not done, and never to be done:**
- reversing the sign or shorting low months;
- 24- or 36-month lookbacks, thresholds or continuous sizing;
- conditioning on refunding months directly;
- other entry or exit sessions;
- an announcement-based measure;
- ZF, ZB, UB or a DV01 basket;
- removing the roll charge;
- reading the confirmation window on this cell's authority.

**For the frontier:**
- INDEX_FUND_DEALER_FLOW gains a closed member.
- STRUCTURAL_FLOWS_FORCED_TRADING has now closed four mechanisms this session: month-end rebalancing,
  the commodity index roll, the Treasury auction concession, and this one.
- The agent's only remaining buildable mechanism is PEER_EARNINGS_INFORMATION_TRANSFER_V1. The
  derivatives route waits at the priced purchase gate.
