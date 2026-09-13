# SEC FORM 4 CLUSTERED OPEN-MARKET INSIDER BUYING — RESULT

**FORM 4 INSIDER ALPHA = NO**

Preregistration: `INSIDER_FORM4_PREREGISTRATION.md`, commit `944deb6` (blob sha256
`9aa2886b…21cfc`, pinned by test), frozen before a single forward return was computed.
Implementation committed at `6e33e7b`, also before any result existed. Family
`INSIDER_FORM4_CLUSTERED_PURCHASE_EVENT_V1`. Artifact: `results/insider_form4.json`
(`artifact_hash fbbdd0f68daea930faf6497a4880505b28db382c386811128296671d93359534`).
Census: `results/insider_form4_census.json` (`79aae04b…e0a75`). Dollars spent: **0**.
Nothing acquired: the estate already owned the history; 2026Q2/Q3 answer HTTP 404.

---

## 1. Data

```
PIT DATA COVERAGE    48,442 purchase rows; 99.25% carry the exact SEC acceptance instant
                     (48,080), 0.75% the 17:30 ET filing-date upper bound (362)
                     eligible-universe identity coverage mean 95.3%, worst 89.6%;
                     46% of decision sessions under 95%
HISTORY              transactions 2008-01-02 -> 2026-03-31 (73 owned quarters);
                     events 2011-07-07 -> 2026-03-20
QUALIFYING PURCHASES 33,637 rows -> 19,002 filings, 6,062 officers/directors
CLUSTER EVENTS       1,091 eligible (878 selection, 213 lockbox), 879 decision sessions
UNIQUE ISSUERS       816 with a qualifying purchase; 344 with an eligible cluster event
DELISTED COVERAGE    469 of 1,236 later-delisted panel rows identified (37.9%);
                     64.3% of eligible delisted-row observations assessable vs 100% of
                     still-listed; 96 events on 46 delisted rows; unidentified names are
                     4.7% of the cross-section and 22.3% of all exits
```

## 2. Result, per preregistered cell and frozen horizon

`market-adj` (minus SPY total return) is the preregistered primary statistic. Entry is
the close AFTER the first session whose close follows the SEC acceptance instant.

| cell | h | events | gross (mkt-adj) | net 1bp | net 2bp | net 5bp | net 12.5bp | t | Sharpe @1bp | max DD @1bp |
|---|---|---|---|---|---|---|---|---|---|---|
| CLUSTERED_BUYING | 5 | 1,091 | +4.39% | +3.38% | +2.37% | −0.65% | −8.21% | 0.62 | 0.13 | −0.749 |
| CLUSTERED_BUYING | 21 | 1,091 | **−3.23%** | −3.47% | −3.71% | −4.43% | −6.23% | −0.93 | −0.14 | −0.994 |
| CLUSTERED_BUYING | 63 | 1,091 | **−3.51%** | −3.59% | −3.67% | −3.91% | −4.51% | −1.48 | −0.13 | −1.000 |

Raw (unadjusted) return is large — +19.5% / +13.9% / +14.7% per year, t 1.78 / 2.39 /
3.87 — and it is **market beta**: subtracting SPY over the identical window removes all
of it. Sector-adjusted (diagnostic, no PIT GICS) +1.1% / −4.0% / −2.5%; equal-risk t
0.26 / −1.34 / −1.89. SELECTION vs LOCKBOX: h=5 +1.9% (t 0.23) / +14.5% (t 0.93); h=21
−2.3% (t −0.55) / −7.2% (t −1.20); **h=63 −1.5% (t −0.60) / −11.5% (t −2.02)**.

### Rank IC, orthogonalised result, incremental vs incumbent

Controls: the owned US_EQUITY baseline (10 dimensions) + the owned `INSIDER_BEHAVIOUR`
block. No short-positioning block exists in the dataset.

| h | rank IC raw | IC orthogonalised | scorer conditional increment | vs-incumbent scorer t | incremental vs `fundamental_momentum_50_50_v1` @1bp | @12.5bp | redundancy |
|---|---|---|---|---|---|---|---|
| 5 | +0.0007 (t 0.33) | −0.0002 (t −0.04) | −0.00037, **t −2.31** | **−2.22** | −0.07%/yr (t −0.14) | −1.08%/yr (t −2.14) | DISTINCT, residual share 0.970 |
| 21 | −0.0032 (t −0.90) | +0.0016 (t 0.24) | +0.00084, t 0.67 | 1.05 | **−1.67%/yr (t −2.30)** | −1.94%/yr (t −2.66) | DISTINCT, 0.865 |
| 63 | −0.0011 (t −0.32) | −0.0071 (t −1.02) | +0.00008, t 0.19 | 0.21 | **−1.12%/yr (t −2.83)** | −1.21%/yr (t −3.05) | DISTINCT, 0.865 |

The canonical scorer returns NO_CONDITIONAL_VALUE in all three cells; effective periods
465 / 111 / 37, informative periods 297 / 111 / 37 (floor 36). The raw long-short book
earns −0.68% / −0.94% / −0.37% net at 1 bp (t −0.71 / −1.01 / −0.48; Sharpe −0.18 /
−0.26 / −0.10; max DD −0.24 / −0.20 / −0.43).

`MULTIPLICITY = Benjamini-Hochberg, q=0.10. Family m=3: 0 rejected. Inherited burden
(family + the 18 prior insider tests at p=1) m=21: 0 rejected. Neither denominator reset.`

## 3. Why: insiders buy weakness, the market reacts at once, and nothing is left

| window (market-adjusted, per event) | mean | t | status |
|---|---|---|---|
| sessions t−21 … t−1, BEFORE publication | **−5.64%** | **−11.37** | diagnostic |
| session t, CONTAINS publication | **+0.55%** | **7.68** | diagnostic |
| session t+1, legal but forgone by the canonical lag | +0.21% | 2.97 | diagnostic |
| from entry at close t+1, h=5 / 21 / 63 | +4.4% / −3.2% / −3.5% per year | 0.62 / −0.93 / −1.48 | **qualification input** |

Clustered officer and director buying in the PIT S&P 500 is a **contrarian act**: it
arrives after a 5.6% market-adjusted fall in the preceding month. The filing IS
informative — the publication session moves +0.55% at t 7.7, and the following session
another +0.21% — but that reaction is complete within two sessions. From the first
canonical entry the drift is zero at five sessions and negative at one and three months,
most negative in the lockbox. That is the same shape R27 recorded (a 182-day cluster
state, significantly wrong-signed) and R63 recorded (its insider block subtracting
conditional value at 21 and 63 sessions): in large caps, stocks insiders buy into
weakness have kept underperforming over the next quarter.

**Orthogonal again, and again worthless.** Residual share 0.86–0.97 against the owned
controls; the signal is distinct information. Adding it to the incumbent subtracts
1.1–1.7%/yr at 21 and 63 sessions with |t| 2.3–3.1 at every cost.

**The forgone session is not a rescue, and would not pay.** Claiming session t+1 was
forbidden in advance. For the record: +0.21% per event against a round trip of 25 bp at
the desk rate is −0.04% per event, before the negative drift that follows.

## 4. Verdicts

```
SIGN                 = pre-specified POSITIVE; observed POSITIVE at h=5 (t 0.62) and
                       NEGATIVE at h=21 and h=63 - DISCOVERY only, never reversed
FINAL VERDICT        = DATA_HOLD on the frozen gate order (predicted),
                       NO_EDGE on the merits
CAPITAL ELIGIBLE     = NO
```

| h | formal verdict | gate | if both data gates had passed |
|---|---|---|---|
| 5 | DATA_HOLD | COVERAGE — 46% of sessions under 95% (limit 20%) | **NO_EDGE** — post-entry market-adjusted t 0.62 < 2.0 |
| 21 | DATA_HOLD | COVERAGE | **NO_EDGE** — FROZEN_SIGN: post-entry return negative |
| 63 | DATA_HOLD | COVERAGE | **NO_EDGE** — FROZEN_SIGN: post-entry return negative |

The coverage gate fires first exactly as the preregistration predicted; the missingness
gate would fire next (unassessable names earn −4.9 to −5.2%/yr relative, t −1.77 to
−2.43); the merits reading, computed by the same frozen function, closes every cell
before multiplicity, incrementality or the declared short-positioning gate is reached.
No cell is rescued, no second cell is created, no threshold, window, horizon, code or role
is changed.

## 5. The declared limitations, restated after the fact

1. **Survivorship.** The identity layer sees 64% of later-delisted observations and all
   still-listed ones; unidentified names are 22% of exits. Declared before results.
2. **Missingness bias.** The unassessable names earn about 5%/yr less; the gate fires.
3. **Large caps only.** The substrate is the PIT S&P 500, where insider information is
   expected to be weakest. It is the universe capital would use, and it was not changed.
4. **Code P includes private purchases**, and 10b5-1 reliance is unobservable before 2023.
5. **Tail.** The owned archive ends 2026-03-31; later decision sessions are NaN, not 0.
6. **Short positioning** remains an unavailable control; it did not bind.

## 6. The axis, in the estate's cumulative record

Insider information has now been tested **19 ways in 6 releases** (R27 ×6, R35 ×4, R39
×3, R63 ×4, Phase 11 ×1, this family's one cell at three horizons). Not one succeeded in
its own direction. The clean event construction — acceptance instant, officers and
directors only, discretionary common-stock purchases, a 2-insider 30-day cluster — makes
the conclusion sharper, not different. **Do not revisit Form 4 buying on this substrate.**

## 7. Safety

No purchase, no subscription, no model promotion, no capital allocation, no portfolio
mutation, no proposal, no order, no fill, no backfill, no live deployment. The live
next-open skew challenger and the live checkout (`20598fa`) were not touched. Research only.
