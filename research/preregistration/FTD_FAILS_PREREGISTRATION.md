# PREREGISTRATION — SEC FAILS-TO-DELIVER RELATIVE TO VOLUME

**Family id:** `SHORT_POSITIONING_FTD_FAILS_TO_VOLUME_V1`
**Ontology dimension:** `SHORT_POSITIONING` (US_EQUITY), scored under the block key
`SEC_FTD_FAILS_TO_VOLUME`
**Status:** FROZEN. Written and committed BEFORE any forward return, rank IC, book or
t-statistic for this family was computed on real data.

This document is the contract. It is not edited after results exist. If a statement
here turns out to be inconvenient, the inconvenience is the result.

---

## 0. WHY THIS AXIS, AND WHAT IT IS NOT

Form 4 insider buying is CLOSED (`10d2c21`, NO). 13F breadth, 13D/G, the 8-K family
and analyst revisions are closed and not revisited.

Three event axes in a row (13D/G, 8-K, Form 4) failed the same way: the information was
priced at or before publication. This axis is deliberately a different kind of object —
a slowly-published **STATE** of the settlement system, not a corporate event.

**Economic hypothesis.** When a security's outstanding settlement fails are large
relative to its own trading volume, short-selling pressure exceeds what the securities-
lending market clears. Binding short-sale constraints keep pessimistic information out of
the price (Miller 1977), so the price stays above value and **subsequent return is
lower.** The information is the balance of fails published by the SEC from NSCC data —
not price, not fundamentals, not filings.

**Direction: NEGATIVE, frozen.** A contradicted sign closes the cell and is DISCOVERY only.

### 0.1 Prior tests on this information

| record | object | result |
|---|---|---|
| Phase 10-A (estate memory; legacy research repo) | Polygon short interest, 545 current tickers | best feature t 1.56, FAILED |
| Phase 11bc (estate memory) | short-interest change | weak, turnover cost-killed |

No estate artifact in this repository scores fails-to-deliver as a return signal; the
file has been used only as an identity bridge (`ownership_identity`). R63's
`SHORT_POSITIONING` dimension was never scored — the canonical equity dataset carries
no such block (measured). Both priors enter this family's inherited burden (§9).

---

## 1. DATA — FREE SEC, ALREADY OWNED

`https://www.sec.gov/data-research/sec-markets-data/fails-deliver-data`, acquired by
`ownership_data`: **409 semi-monthly files, 2009-07a → 2026-08a, 4,257 settlement
dates.** No purchase, no key. `paid_dollars: 0`. Nothing new was acquired.

Census artifact `results/ftd_fails_census.json`
(`artifact_hash e4f5ecde3cc80fd1bf8cee9a3f9738056a7dc709f1840a0f188fe160f5532f65`).

### 1.1 What a file row is — the SEC's own definition

`SETTLEMENT DATE | CUSIP | SYMBOL | QUANTITY (FAILS) | DESCRIPTION | PRICE`. The quantity
is "the aggregate net balance of shares that failed to be delivered as of a particular
settlement date" in NSCC's CNS system — new fails plus existing fails less settled fails.
**It is a BALANCE, not a daily flow**, and its age cannot be read from it. **From
2008-09-16 every non-zero balance is published**, so a security absent on a covered
settlement date is an OBSERVED ZERO.

### 1.2 Publication — measured, and why the rule is a bound

The SEC: first half of month M "available at the end of the month"; second half "at about
the 15th of the next month"; "we cannot guarantee that the data will be posted by a
particular date". Measured HTTP Last-Modified (2026-09-13):

| file | posted | | file | posted |
|---|---|---|---|---|
| 2021-04a | 2021-04-30 | | 2021-04b | 2021-05-17 |
| 2024-02a | **2024-03-01** | | 2024-02b | 2024-03-15 |
| 2024-05a | 2024-05-30 | | 2024-05b | 2024-06-17 |
| 2024-10a | 2024-10-30 | | 2024-10b | 2024-11-15 |
| 2026-01a | **2026-02-05** | | 2026-01b | 2026-02-17 |
| 2026-06a / 07a / 08a | 06-30 / 07-30 / 08-31 | | 2026-06b / 07b | 07-15 / 08-17 |

**Every file before 2021 carries the bulk re-upload stamp 2020-12-19**: historical posting
dates are UNOBSERVABLE. The availability rule is therefore a declared, conservative bound:

* **first half of month M: usable only at a session STRICTLY AFTER day 10 of month M+1**
  (latest measured posting: day 5);
* **second half of month M: usable only at a session STRICTLY AFTER day 25 of month M+1**
  (latest measured posting: day 17).

Measured on the h=21 grid, the file a decision uses covers a half-month that ended
**25 to 55 days earlier (median 36).** The signal is stale by construction; that is the
cost of not looking ahead.

### 1.3 Identity — the owned CUSIP-anchored bridge

`ownership_identity.panel_cusip_index`: FTD states CUSIP and SYMBOL in one record, and
the CUSIP → panel-row correspondence is bookkeeping settled by the whole archive; the
panel retains delisted securities. **No issuer-name matching.** 1,104 CUSIPs indexed;
**932 of 1,897 panel rows identified**; 273 of 1,236 later-delisted rows; 10 ambiguous
CUSIPs dropped.

| measured on the eligible cross-section | h = 21 grid | h = 63 grid |
|---|---|---|
| identified share, mean | **99.13%** | 99.12% |
| identified share, worst session | **98.37%** | 98.37% |
| sessions under 95% | **0 of 181** | 0 of 179 |
| later-delisted eligible observations identified | **95.7%** | 95.7% |

---

## 2. THE MEASURE — census, no return

| year | files | eligible identified names with any fail | fails/volume median (if > 0) | p90 | share > 0.5% of volume |
|---|---|---|---|---|---|
| 2009H2 | 12 | 91.6% | 0.054% | 0.40% | 7.2% |
| 2012 | 24 | 91.9% | 0.116% | 0.52% | 9.7% |
| 2015 | 24 | 93.8% | 0.099% | 0.67% | 13.1% |
| 2018 | 24 | 96.6% | 0.079% | 0.62% | 12.5% |
| 2021 | 24 | 98.1% | 0.050% | 0.50% | 9.8% |
| 2024 | 24 | 95.0% | 0.036% | 0.44% | 8.1% |
| 2025 | 24 | 94.4% | 0.023% | 0.29% | 5.0% |

The signal is DENSE (not an event): 91–98% of eligible identified names carry a fail in a
half-month. **Rank autocorrelation between consecutive h=21 decisions: median 0.21 (p10
0.11, p90 0.39)** — the cross-sectional ordering is not persistent, so a book on it trades
heavily. Declared now; the canonical 12.5 bp desk cost is applied by the scorer.

---

## 3. THE ONE FROZEN CELL — `FAILS_TO_VOLUME`

For security *i* and fails file *f*:

```
FAILS_TO_VOLUME[i, f] = mean over the file's settlement dates of the fails balance of i
                        (absent on a covered date = 0)
                      / mean daily share volume of i over the sessions of f's half-month
```

* At decision session *t*, the value comes from the file covering the **latest
  half-month whose usable day is strictly before t** (§1.2).
* The signal is the **cross-sectional percentile rank (ties averaged)** among eligible
  identified names with a finite ratio, **multiplied by the frozen sign −1**. NaN — never
  0 — for an unidentified name or one with no volume in the half-month.
* **Why volume, not shares outstanding.** Reg SHO's threshold test uses 0.5% of shares
  outstanding; the estate owns no PIT shares outstanding, so it cannot be computed. A
  security's own volume is PIT-safe and scale-free, and the owned LIQUIDITY and
  VOLUME_PARTICIPATION controls absorb the denominator's own information.
* **Why a rank.** The ratio is heavily skewed (p99 ≈ 20–60× the median); a rank needs no
  threshold and no winsor choice.
* **Why one cell.** A persistence cell (days with a non-zero balance) is near-saturated in
  large caps and a change cell restates the prior short-interest-change failure.
  **No second cell is created, before or after results.**

---

## 4. POINT-IN-TIME CONTRACT

* The fails file is visible only at sessions strictly after its declared usable day.
* The volume denominator covers the file's OWN half-month, which ended at least 25 days
  before any session that may use it.
* The forward return is the canonical NEXT_CLOSE window `t+2 … t+1+h`
  (`alpha_agent.r63.pit.forward_compound`), on the canonical R63 equity grid, cadence 21.
* Identity uses the CUSIP → row correspondence of the whole archive (bookkeeping, frozen
  in the 13F preregistration); the INFORMATION enters only through files already usable.

---

## 5. HORIZONS — 21 AND 63, m = 2

`FAMILY_PRIMARY_MAX = 6` would allow 5 sessions, and it is **not** used: the signal is
refreshed twice a month and already 25–55 days old when first usable, so a 5-session
decision grid would re-score the same stale file about four times per refresh, inflating
the period count without adding information. 21 and 63, as the brief prefers. Nothing is
added, removed or searched after results.

---

## 6. MEASUREMENT

* **Standalone:** per-period Spearman rank IC of the signed signal against the forward
  return (`control_block_alpha.raw_and_orthogonal`), Newey-West; SELECTION / LOCKBOX split.
* **Books:** the canonical XS long-short book of the raw and orthogonalised signal at 1 / 2
  / 5 bp and the 12.5 bp desk rate.
* **Conditional:** `alpha_agent.r63.sensitivity.run_cell` with the owned US_EQUITY baseline
  (10 dimensions: price state, trend, momentum, reversal, volatility, tail, **liquidity,
  volume participation**, fundamental levels, free cash flow). **No second scorer.**
* **Orthogonalised:** contemporaneous cross-sectional residual on the baseline + the
  incumbent `fundamental_momentum_50_50_v1`.
* **Incremental vs incumbent:** a second `run_cell` with the incumbent as baseline, and the
  incumbent top-50 book blended 50/50 with the orthogonalised signal (DESCRIPTIVE).
* **Short-positioning control:** not applicable — this family IS the estate's only
  short-positioning information.

---

## 7. COSTS

1 / 2 / 5 bp per side, plus `EQ_COST_RATE_PER_SIDE` = **12.5 bp per side** in the scorer.

---

## 8. IDENTITY GAPS AND BIAS

The unassessable set is small (~0.9% of eligible names, a handful per session). The
missingness gate (`control_block_alpha.missingness_bias`, |annual difference| > 1.5%) is
kept **unchanged**; **declared in advance: with so few names per session their mean
return is noisy, and the gate may fire on noise.** The merits reading (§10) is reported
beside it, exactly as for 13D/G, 8-K and Form 4.

---

## 9. MULTIPLE TESTING

Owner `alpha_agent.r31.multiple_testing` (BH, q = 0.10) over the two-sided p-values of the
signed rank IC. Missing p counts as 1.

* **Family:** one cell × two horizons, **m = 2**.
* **Inherited:** the family plus the **2 prior short-interest tests** of §0.1 at p = 1,
  **m = 4**. Both must pass. Neither denominator is ever reset.

---

## 10. QUALIFICATION — existing gates only, in this order

1. **Coverage.** More than 20% of decision sessions under 95% identified → **DATA_HOLD**.
   *Measured: 0%. Expected to pass.*
2. **Missingness bias.** |annual difference| > 1.5%/yr → **DATA_HOLD** (see §8).
3. **Effective periods.** Scorer DATA_HOLD, or < 36 effective periods →
   **NEED_MORE_EVIDENCE**. *Measured: 115 at h=21, 37 at h=63.*
4. **Frozen sign.** Signed rank IC mean ≤ 0 → **NO_EDGE**. Never reversed.
5. **Standalone.** Signed rank IC t < `STANDALONE_T_FLOOR` = 2.0 → **NO_EDGE**.
6. **Conditional.** Scorer conditional t < 2.0 (must HELP) → **NO_EDGE**.
7. **Family multiplicity.** BH q = 0.10, m = 2 → **NO_EDGE**.
8. **Inherited multiplicity.** BH q = 0.10, m = 4 → **NO_EDGE**.
9. **Incremental.** `REDUNDANT`, orthogonalised IC t < 2.0, or vs-incumbent scorer t < 2.0
   → **NO_INCREMENTAL_INFORMATION_EDGE**. Not rescued.
10. **Materiality.** Scorer net annual increment < 1.5%/yr → **NO_EDGE**.
11. Otherwise → **QUALIFIED**.

The same frozen function is also evaluated with gates 1–2 satisfied and reported beside the
formal verdict (merits reading). **CAPITAL ELIGIBLE remains NO** unless every qualification
gate AND every forward-evidence and governance gate is satisfied.

---

## 11. FALSIFICATION — and what will NOT be done

A null result means: fails-to-deliver large relative to a security's own volume, observed
only after the SEC could have published it, carries no information about subsequent return
that the owned price, liquidity and fundamental families and the incumbent do not carry.

**Forbidden after results:** reversing the sign; changing the usable days; a shorter lag
"because 2024 posted on time"; a different denominator (shares outstanding, dollar volume,
ADV over another window); a log, z-score, threshold or winsor instead of the rank; a 5-session
horizon; a persistence or change cell; excluding ETFs, sectors, sizes or years; the
ticker-as-of identity mode; any second cell.

## 12. SAFETY

RESEARCH ONLY. No purchase, no subscription, no licence, no model promotion, no capital
allocation, no portfolio mutation, no proposal, no order, no fill, no backfill, no live
deployment, no live write. The live checkout and the live next-open skew challenger are not
touched.
