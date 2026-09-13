# SEC SCHEDULE 13D/G CONTROL-BLOCK EVENT AXIS — RESULT

**13D/G CONTROL-BLOCK ALPHA = NO**

Preregistration: `CONTROL_BLOCK_13DG_PREREGISTRATION.md`, commit `1d3f491`, frozen
before a single forward return was computed. Implementation committed at `ed42772`,
reader repairs at `f59b2f0` — all three before this file existed.
Family `OWNERSHIP_CONTROL_BLOCK_13DG_EVENT_V1`. Artifact:
`results/control_block_13dg.json`. Dollars spent: **0**.

---

## 1. Data

```
DATA COVERAGE        97,613 schedules, 2009-01-02 -> 2026-09-09 (136,395 back to 1994)
                     13D 1,363 | 13D/A 6,649 | 13G 24,830 | 13G/A 64,771
                     documents 97,609 of 97,613 stored (4 fetch failures, 0.004%)
                     cover-page percent read 98.63%  (text 98.67%, structured 98.35%)
                     CUSIP read 96.80%   acceptance timestamp 100%   accession 100%
                     filer identified 85,378 / 97,613 = 87.5%
EVENT COUNT          NEW_CONTROL_BLOCK        9,706   2009-01-05 -> 2026-08-26
                     MATERIAL_BLOCK_INCREASE  5,913   2009-01-12 -> 2026-08-27
EARLIEST RELIABLE    2009-01-02 (primaryDocument is populated from 2001; the panel
                     and the R63 discovery window begin later, at 2011-07-01)
PIT MATCH RATE       1,130 of 1,897 panel rows carry a CIK = 59.6% of rows,
                     95.32% of the ELIGIBLE universe on an average decision session
                     (min 89.63%); 0 issuer-name matches used, 0 route disagreements
```

Identity on this axis needs no CUSIP and no licence: EDGAR indexes a Schedule under
the **subject issuer's own CIK**, so the join is one hop, CIK → panel row. 1,124 rows
came from the owned SEC bridge and 6 from the SEC ticker map, with zero disagreements
between the two routes.

## 2. Result, per preregistered cell and frozen horizon

`market_adj` is the preregistered primary statistic. Entry is the **first session
whose close falls strictly after the SEC acceptance instant** — no same-day lookahead.

| cell | h | events | gross (mkt-adj) | net 1bp | net 2bp | net 5bp | net 12.5bp | t | Sharpe | max DD |
|---|---|---|---|---|---|---|---|---|---|---|
| NEW_CONTROL_BLOCK | 5 | 9,706 | +2.01% | +1.00% | −0.00% | −3.03% | −10.59% | 0.37 | 0.03 | −0.944 |
| NEW_CONTROL_BLOCK | 21 | 9,584 | +0.06% | −0.18% | −0.42% | −1.14% | −2.94% | 0.02 | −0.01 | −1.000 |
| NEW_CONTROL_BLOCK | 63 | 9,530 | +0.29% | +0.21% | +0.13% | −0.11% | −0.71% | 0.10 | 0.01 | −1.000 |
| MATERIAL_BLOCK_INCREASE | 5 | 5,912 | +0.65% | −0.36% | −1.36% | −4.39% | −11.95% | 0.12 | −0.01 | −0.735 |
| MATERIAL_BLOCK_INCREASE | 21 | 5,840 | +3.88% | +3.64% | +3.40% | +2.68% | +0.88% | 1.09 | 0.15 | −0.955 |
| MATERIAL_BLOCK_INCREASE | 63 | 5,804 | +1.37% | +1.29% | +1.21% | +0.97% | +0.37% | 0.48 | 0.06 | −1.000 |

Raw (unadjusted) return is large and significant in every cell — +18.5%/yr at h=5,
t 2.45 — and essentially all of it is **market beta**: subtracting the market removes
90–99% of it and every t-statistic collapses below 1.1. Sector-adjusted and
equal-risk variants agree, several turning negative.

### Rank IC, orthogonalised result, incremental vs incumbent

| cell | h | rank IC raw | IC orthogonalised | incremental vs `fundamental_momentum_50_50_v1` @1bp | @12.5bp | redundancy |
|---|---|---|---|---|---|---|
| NEW_CONTROL_BLOCK | 5 | +0.0033 (t 1.68) | **−0.0019** (t −0.40) | −0.02%/yr (t −0.02) | t −2.36 | DISTINCT |
| NEW_CONTROL_BLOCK | 21 | −0.0050 (t −1.11) | +0.0025 (t 0.30) | −0.61%/yr (t −0.71) | t −1.21 | DISTINCT |
| NEW_CONTROL_BLOCK | 63 | −0.0078 (t −1.65) | +0.0021 (t 0.26) | **−1.55%/yr (t −2.47)** | t −2.69 | DISTINCT |
| MATERIAL_BLOCK_INCREASE | 5 | +0.0009 (t 0.43) | +0.0075 (t 1.37) | −0.11%/yr (t −0.18) | t −2.52 | DISTINCT |
| MATERIAL_BLOCK_INCREASE | 21 | +0.0009 (t 0.19) | +0.0052 (t 0.62) | −0.24%/yr (t −0.29) | t −0.33 | DISTINCT |
| MATERIAL_BLOCK_INCREASE | 63 | −0.0021 (t −0.37) | +0.0046 (t 0.50) | −1.06%/yr (t −1.77) | t −1.99 | DISTINCT |

The signal is **not redundant** — residual share 0.988–0.996 against the ten baseline
dimensions, max absolute rank correlation 0.078. It is genuinely new information.
It is also worthless: every rank IC is within noise, and **adding it to the incumbent
subtracts return at every horizon and every cost level**, significantly so at h=63.

`MULTIPLICITY = Benjamini-Hochberg, q=0.10, m=6 (one declared family, six cells).
0 of 6 rejected. The denominator was not reset.`

## 3. Why: the information is real, and it is already in the price

The announcement session itself is the largest, most significant effect measured
anywhere on this axis:

| cell | announcement-session return | t | prior 21-day run-up (post-hoc) | t |
|---|---|---|---|---|
| NEW_CONTROL_BLOCK | **+0.217%** | **5.48 – 5.96** | **+1.52%** | 2.57 – 3.79 |
| MATERIAL_BLOCK_INCREASE | **+0.199%** | **2.73 – 3.58** | **+1.27%** | 1.60 – 2.29 |

A 13D/G filing is genuinely informative — but the information is spent by the time it
can be traded. The price has already risen ~1.4% over the 21 sessions *before* the
filing (the accumulation that triggers the 5% obligation is itself visible in the
tape), it moves another ~0.2% on the disclosure session, and from the next close
onward — the first instant a point-in-time investor may act — the market-adjusted
return is indistinguishable from zero at all three horizons. Two basis points per side
is enough to erase what remains of the 5-day cell.

The `prior_21d_runup` figure is a **post-hoc diagnostic**, labelled as such in the
preregistration, excluded from every gate by test, and reported only because it
distinguishes "the announcement moved the price" from "the filing reported a move that
had already happened". Both are true here; the second is larger.

The 13D-only sub-sample — the activist half, where the effect should be strongest —
is **negative** in all six cells (h=5: −14.3%/yr, t −1.47 on 616 events).

## 4. Verdicts

```
SIGN                 = DISCOVERY_ONLY
                       No sign was pre-specified: the preregistration fixes the
                       direction as TWO-SIDED and states that a sign read off the
                       sample is DISCOVERY_ONLY. The observed sign is positive and
                       is not confirmation of anything.
FINAL VERDICT        = DATA_HOLD on the frozen gate order, NO_EDGE on the merits
CAPITAL ELIGIBLE     = NO
```

All six cells return **DATA_HOLD**, because the coverage gate fires first: 46–47% of
decision sessions resolve under the 95% floor, against a preregistered limit of 20%.
**The preregistration predicted this gate would fire, before any return was computed.**

That is a statement about the data, not the economics, so the same frozen verdict
function was re-run with both data gates satisfied. The answer does not change:

| cell | h | actual | if both data gates had passed |
|---|---|---|---|
| NEW_CONTROL_BLOCK | 5 | DATA_HOLD | NO_EDGE — conditional t 0.05 < 2.0 |
| NEW_CONTROL_BLOCK | 21 | DATA_HOLD | NO_EDGE — conditional t 1.20 < 2.0 |
| NEW_CONTROL_BLOCK | 63 | DATA_HOLD | NO_EDGE — fails BH q=0.10 over m=6 |
| MATERIAL_BLOCK_INCREASE | 5 | DATA_HOLD | NO_EDGE — conditional t −1.55 < 2.0 |
| MATERIAL_BLOCK_INCREASE | 21 | DATA_HOLD | NO_EDGE — conditional t 0.33 < 2.0 |
| MATERIAL_BLOCK_INCREASE | 63 | DATA_HOLD | NO_EDGE — fails BH q=0.10 over m=6 |

Two cells (h=63) clear the conditional t floor at 2.53 on 37 effective periods —
one period above the floor of 36 — and both die on the declared multiplicity burden
that was fixed in advance at m=6. Neither is rescued, and no seventh cell is created.

## 5. The declared limitations, restated after the fact

1. **Survivorship.** Unidentified names leave the investable universe at 17.9%/yr
   against 3.05% for identified ones: 4.7% of the cross-section but **22.3% of all
   exits**. A 13D often precedes an acquisition, so the experiment is structurally
   least able to observe the outcome the hypothesis predicts. This was measured and
   declared *before* freezing, not discovered afterwards. It cannot be closed without
   the fuzzy issuer-name matching the brief forbids.
2. **Missingness bias.** The names that cannot be assessed earn **−4.9 to −5.2%/yr**
   relative to those that can (t −1.77 to −2.43), against a 1.5% materiality floor.
   The preregistered bias gate fires.
3. **No PIT GICS.** The estate owns a current sector snapshot only, so sector
   adjustment is a diagnostic and never a qualification input (inherited from R58).
4. **Share class.** 5.6% of filings report blocks over 30%, some against a class other
   than the common stock. The text era does not state the class machine-readably; this
   is a declared limitation of Cell B, not a post-hoc filter.

## 6. Safety

No purchase, no subscription, no model promotion, no capital allocation, no portfolio
mutation, no proposal, no order, no fill, no backfill, no live deployment. The live
next-open skew challenger was not touched. Research only.
