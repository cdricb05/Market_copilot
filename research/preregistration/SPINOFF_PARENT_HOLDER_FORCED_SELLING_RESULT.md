# RESULT - SPIN-OFF PARENT-HOLDER FORCED SELLING

**Mechanism:** `SPINOFF_PARENT_HOLDER_FORCED_SELLING_V1` (global candidate `GC_USEQ_SPINOFF_FORCED_SELLING`)
**Run:** ALPHA_COMPOSITE_STRIKE_SEP15_V1, verdict 3. Executed 2026-09-15 22:23-22:27 UTC through the canonical handler
body `alpha_agent.r59.mechanisms.execute_job` (199 s).
**Preregistration:** `029230e` (sha256 `a3d74e1c...`). **Implementation:** `3b7fb5b` (`d66846fe...`). **Pin:** `0d73c96`.
**Artifact:** `D:\Stock_Prediction_app_data\alpha_recovery_offensive\results\spinoff_parent_holder_forced_selling.json`
(artifact_hash `57ec6204...13019`).
**Input identity:** `EDGAR:Form 10-12B(2540 rows, 2539 parsed, 495 submissions records);NORGATE:US Equities+Delisted TOTALRETURN open/close of 373 linked securities;$RUATR;$RUTTR,$SPXTR`.
**ResearchMemory:** `HM_SPINOFF_PARENT_HOLDER_FORCED_SELLING_V1`, outcome `NO_ALPHA_EVIDENCE`.

## VERDICT: `NO_EDGE` (gate 10, `CONFIRMATION`). CAPITAL ELIGIBLE = NO.

Gate 1 passed:

* index complete, parsed 100 %, submissions records 100 %;
* identity resolution 92.3 % (raw coverage 75.6 %);
* 168 qualification and 206 confirmation events, no price load failure, hedge finite on 100 % of sessions.

**Qualification, entries 1996-2012: every gate passed.**

| 168 events, 4,290 sessions | 0 bp | 12.5 bp | **25 bp (primary)** | 50 bp (stress) |
|---|---|---|---|---|
| annualised net (% of NAV) | +2.42 % | +2.37 % | **+2.31 %** | +2.21 % |
| Sharpe | 0.86 | 0.84 | **0.82** | 0.78 |
| NW t (lag 10) | 3.57 | 3.50 | **3.42** | 3.27 |
| maximum drawdown | -6.3 % | -6.4 % | **-6.6 %** | -6.9 % |
| mean six-month event excess over the Russell 3000, net | +12.3 % | +12.0 % | **+11.7 %** | +11.2 % |
| event-level t / hit rate | 3.73 / 55.4 % | 3.65 | **3.58 / 55.4 %** | 3.42 |

The other qualification gates:

* **Increment over `$RUTTR` and `$SPXTR`:** +2.65 %/yr alpha, HAC t 3.91 (betas +0.04 and -0.08).
* **Halves:** 1996-2004 +2.33 %/yr, 2005-2012 +2.19 %/yr.
* **Multiplicity:** BH at m = 4 with p = 0.0003, against a single-survivor threshold of 0.025.
* **Book usage:** 50 bp stress still +2.21 %/yr. Mean active slots 5.0, maximum 15, no event skipped. The book
  averaged about 10 % of NAV gross, so the annual figures are small against a large per-event effect.

**Untouched confirmation, entries 2013-2026: FAILED.**

| 206 events, 3,441 sessions, 25 bp | value |
|---|---|
| annualised net | **+0.50 %** |
| Sharpe | 0.16 |
| NW t | **0.59** (p 0.28) |
| maximum drawdown | -12.3 % |
| mean six-month event excess, net | +1.7 % (event t 0.63, hit rate 42.7 %) |

Mean event excess by entry year:

* **Qualification:** positive in 12 of 17 years, including 2000 +18 %, 2001 +42 %, 2003 +72 % (4 events),
  2005 +29 % and 2009 +37 %.
* **Confirmation:** negative in 8 of 14 years, 2014-2019 in particular (2014 -9 %, 2017 -11 %, 2018 -12 %).
  The average rests on a few years: 2020 +34 % (10 events) and 2025 +53 % (6 events).

## What this means

Spin-off parent-holder forced selling was a real, large and cheap-to-trade premium in 1996-2012: about +12 % over
the Russell 3000 in the six months after a spinco's first month, surviving index controls and every cost rung.
It did not survive into the untouched window. Across 2013-2026, the years after the spin-off anomaly was
widely published and packaged, the same frozen book earned +1.7 % per event with no statistical distinction from
zero, and lost money in most years. This is the estate's second "passes qualification, dies on the untouched
window" result after merger arbitrage (2026-09-14). A premium that was once paid for absorbing forced selling now
appears to be competed away. The mechanism closes; the verdict is not a data problem.

The identity layer the test needed was built at $0 and is reusable: EDGAR Form 10-12B index and filings, EDGAR
submissions names, and a Norgate link resolving 92 % of candidate spincos.

**Not done, and never to be done:**

* another entry session or hold;
* an unhedged book;
* size, index, link-state or sector subsets;
* dropping NAME links;
* excluding 2014-2019;
* moving the confirmation boundary;
* any further calibration.

**For the frontier:** FORCED_TRADING_FLOW gains an executed closed member. The global candidate has no forward clock
and closes by derivation.
