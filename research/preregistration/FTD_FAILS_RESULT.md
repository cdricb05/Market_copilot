# SEC FAILS-TO-DELIVER RELATIVE TO VOLUME — RESULT

**FAILS-TO-DELIVER ALPHA = NO**

Preregistration: `FTD_FAILS_PREREGISTRATION.md`, commit `bc7da89` (blob sha256
`7626c369…c0de`, pinned by test), frozen before any forward return existed. Implementation
committed at `45d58c6`, also before any result. Family
`SHORT_POSITIONING_FTD_FAILS_TO_VOLUME_V1`. Artifact: `results/ftd_fails.json`
(`artifact_hash c54cac95da3156518cb27c4be0f4b50448bab35bd2014d087ed42ec8040e07a2`).
Census: `results/ftd_fails_census.json` (`e4f5ecde…2f65`). Dollars spent: **0**. Nothing
acquired.

---

## 1. Data

```
SOURCE             SEC Fails-to-Deliver, 409 semi-monthly files 2009-07a -> 2026-08a,
                   4,257 settlement dates; owned
PUBLICATION        a file used only strictly after day 10 (first half) / day 25 (second
                   half) of the next month; the half-month a decision uses ended 25-55
                   days earlier (median 36)
IDENTITY           owned CUSIP-anchored bridge, no name matching: 932 of 1,897 panel rows;
                   eligible universe identified 99.13% mean, 98.37% worst,
                   0 of 181 sessions under 95%
DELISTED COVERAGE  273 of 1,236 later-delisted rows; 95.7% of later-delisted eligible
                   observations identified
CROSS-SECTION      ~495 ranked names per decision session, 181 (h=21) / 179 (h=63) sessions
```

## 2. Result, per preregistered cell and frozen horizon

The signal is multiplied by the frozen sign (−1), so a POSITIVE statistic is the
predicted direction (more fails → lower return).

| cell | h | signed rank IC | t | SELECTION IC (t) | LOCKBOX IC (t) | L/S gross | net 1bp | net 2bp | net 5bp | net 12.5bp | t @1bp | Sharpe @1bp | max DD @1bp |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| FAILS_TO_VOLUME | 21 | **−0.0018** | −0.52 | −0.0019 (−0.45) | −0.0015 (−0.25) | −0.68% | −1.02% | −1.35% | −2.35% | −4.85% | −1.12 | −0.29 | −0.187 |
| FAILS_TO_VOLUME | 63 | +0.0006 | 0.14 | −0.0002 (−0.05) | +0.0032 (0.40) | −0.48% | −0.59% | −0.70% | −1.03% | −1.86% | −0.95 | −0.15 | −0.342 |

The long-short book turns over **1.39 one-way per rebalance** at both horizons — the
census measured a consecutive-decision rank autocorrelation of 0.21 — so cost erodes an
already negative gross.

### Orthogonalised result, conditional value, incremental vs incumbent

| h | IC orthogonalised | orth. L/S net @1bp | scorer conditional increment | eff. periods | scorer net increment | redundancy | vs-incumbent scorer t | incremental vs `fundamental_momentum_50_50_v1` @1bp | @12.5bp |
|---|---|---|---|---|---|---|---|---|---|
| 21 | −0.0034 (t −0.88) | −0.61% (t −0.60) | −0.00023, t −0.33 | 115 | −1.28%/yr | DISTINCT, residual share 0.998 | −0.77 | +0.27%/yr (t 0.25) | −0.37%/yr (t −0.34) |
| 63 | +0.0030 (t 0.69) | −0.20% (t −0.32) | −0.00013, t −0.51 | 37 | −0.06%/yr | DISTINCT, 0.998 | −1.83 | +0.53%/yr (t 0.79) | +0.32%/yr (t 0.47) |

The canonical scorer returns NO_CONDITIONAL_VALUE in both cells (3-year block stability
0.5; the h=21 lockbox sign disagrees with selection).

`MULTIPLICITY = Benjamini-Hochberg, q=0.10. Family m=2: 0 rejected. Inherited burden
(family + 2 prior short-interest tests at p=1) m=4: 0 rejected. Neither reset.`

**Missingness:** unassessable names earn −0.59% / −0.88% per year relative (t −0.14 /
−0.21), under the 1.5% floor. The gate did not fire.

## 3. Why

For the first time in this campaign **both data gates passed**: identity covered 99% of
the universe on every session, and the unassessable names were not different. The verdict
is therefore a statement about information, with no data caveat in front of it.

In the PIT S&P 500 the fails balance is small — a median of 0.02–0.12% of a day's volume
among names with any fail — and 91–98% of names carry some fail in every half-month. The
cross-sectional ordering barely persists from one month to the next (rank autocorrelation
0.21). That is consistent with large-cap fails being dominated by operational and
market-making settlement timing rather than by a binding borrow constraint; either way,
**observed only after the SEC could have published it, the ordering says nothing about
the next one or three months.** The signal is fully distinct from the owned families
(residual share 0.998) and still empty — the fourth axis in this campaign that is
orthogonal and worthless (13D/G, 8-K, Form 4, fails-to-deliver).

## 4. Verdicts

```
SIGN                 = pre-specified NEGATIVE; observed opposite at h=21 (t -0.52),
                       in direction but t 0.14 at h=63 - never reversed
FINAL VERDICT        = NO_EDGE (formal and merits identical: data gates passed)
CAPITAL ELIGIBLE     = NO
```

| h | formal verdict | gate | reason |
|---|---|---|---|
| 21 | **NO_EDGE** | FROZEN_SIGN | signed rank IC −0.0018, opposite to the frozen direction |
| 63 | **NO_EDGE** | STANDALONE_T | signed rank IC t 0.14 < 2.0 |

No cell is rescued: the usable days, the volume denominator, the rank transform, the
horizons and the identity mode are unchanged, and no persistence, change or threshold
cell is created.

## 5. Declared limitations, restated

1. Historical SEC posting dates are unobservable; the usable days are a bound with slack,
   and they make the signal 25–55 days old.
2. No PIT shares outstanding: the Reg SHO threshold test could not be computed; the
   denominator is the security's own volume.
3. Large caps only — the universe capital would use.
4. The short-positioning prior tests (Phase 10-A, 11bc) are memory records, not estate
   artifacts in this repository.

## 6. Safety

No purchase, no subscription, no model promotion, no capital allocation, no portfolio
mutation, no proposal, no order, no fill, no backfill, no live deployment. The live
next-open skew challenger and the live checkout (`20598fa`) were not touched. Research only.
