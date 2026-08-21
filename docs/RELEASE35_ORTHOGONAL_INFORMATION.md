# Release 35 — Orthogonal Information Acquisition & Incremental Alpha

**Terminal verdict: `R35_NO_INCREMENTAL_INFORMATION_EDGE`.
SYSTEM_RESULT = PASS. RESEARCH_CANDIDATE_RESULT = FAIL. ALPHA_RESULT = FAIL.**

28 executed configurations (ceiling 80), 6 new information families acquired,
0 incremental survivors, **$0 spent**, production untouched. Research only: no
order, proposal, decision, allocation, model promotion, sleeve activation or
operational write.

Campaign id `r35_orthogonal_information_v1`.
Research root `D:\Stock_Prediction_app_data\orthogonal_information_r35\`.
Acquisition store `…\orthogonal_information_r35\acquired\` — 130 payloads,
901 MB, every one free and public.

---

## 1. The result in one paragraph

Release 34 measured that the binding constraint is information content: its
forecast captured 14.9 % of what the same machinery earns with perfect
foresight. Release 35 went and got more information. Six economically distinct
families were acquired from free public sources and normalised with true
publication timestamps — futures positioning, FX interest carry, a real
commodity futures curve, implied-volatility term structure, market-implied risk
premia, and SEC insider filings. **Fifteen of the nineteen new features are
measurably distinct from the 28-feature base set**, with residual shares up to
0.94 after regressing out everything R34 already knew. Four of the six families
predict returns on their own at t between 2.1 and 3.0. And **not one of them
adds anything conditional on the base information set**: across 21 paired
comparisons the largest increment in cross-sectional rank IC was **+0.017 at
t = 1.78**, below the pre-registered gate of t ≥ 2.0, and 0 of 19 configurations
with a p-value survived Benjamini–Hochberg. No family reached the economic
stage. The base arm reproduced R34's finalist exactly — after-cost excess
`+2.072422558810863e-05`, t = `0.0037564469686976847`, matching R34's published
figure to every digit — so the comparison rests on a verified anchor.

---

## 2. What was actually acquired (and it is real)

This release does not stop at a provider-planning document. Everything below is
on disk, checksummed in `acquisition_manifest.json`, and reproducible from the
runner.

| family | source | licence | history | coverage |
|---|---|---|---|---|
| **FUTURES_POSITIONING** | CFTC Commitments of Traders, legacy futures-only, 41 annual archives | public domain | **1986-01-15 → 2026-08-18**, 44,347 rows | 17 of 47 instruments |
| **FX_INTEREST_CARRY** | FRED / OECD 3-month interbank rates, 6 currencies + USD | free API, key already held | 1985 → 2026 | 7 instruments |
| **COMMODITY_TERM_STRUCTURE** | EIA petroleum bulk archive, NYMEX contracts 1–4 | public domain | **1983-03-30 → 2024-04-05** | 3 instruments |
| **IMPLIED_VOLATILITY_TERM_STRUCTURE** | Cboe VIX and VIX3M daily history | freely published | 2009-09-18 → 2026 | all 47 (market level) |
| **MARKET_IMPLIED_RISK_PREMIA** | FRED: DFII10, T10YIE, DGS2/10/30, BAA10Y | free API | 2003-01-02 → 2026 | all 47 (market level) |
| **INSIDER_TRANSACTION_INTENSITY** | SEC Form 3/4/5 structured data sets, 73 quarters, + the **already-owned** Financial Statement Data Sets for point-in-time SIC | public domain | 2008 → 2026-03 | 10 sector ETFs + market |

**Two families fill absences Release 33 declared rather than approximated.**
R33's feature registry records FX carry as ABSENT ("the owned estate has US
short rates but no foreign short-rate history") and commodity carry as ABSENT
("the owned Continuous Futures entitlement is one market, so no curve exists").
R35 supplies the missing legs: real foreign short rates, and a genuine dated
contract-1-to-4 settlement curve. Neither is manufactured from spot momentum,
which `contract.PROHIBITED_SUBSTITUTIONS` forbids and a test negative-probes.

**The commodity curve ends where its publisher ended it.** EIA stopped
republishing NYMEX settlements on **2024-04-05**. The coverage artifact records
the discontinuation; nothing extrapolates past it.

---

## 3. Point-in-time, enforced in one function

Every series passes through `information.as_of_align`, and no other module in
the package is allowed to align anything. Three rules matter because each has a
tempting shortcut that would have produced a nicer release:

- **Positioning is indexed at its RELEASE, not at its report date.** A COT row is
  stamped with a Tuesday and published the following Friday; the declared lag is
  6 calendar days plus the uniform one-session broadcast lag. It is the only
  acquired family whose publication date is *inferred* rather than read, so it
  is also the only one re-run under stress — see §7.
- **An insider filing is public at its FILING date and at no earlier moment.**
  The transaction date inside the document was private on the day it happened.
  `INSIDER_TRANSACTION_DATE_MAY_BE_OBSERVABLE = False`.
- **Monthly OECD rates are stamped two months forward.** A month-M interbank rate
  carrying a month-M index would be a publication-lag look-ahead repeated 300
  times.

**Sector classification is point-in-time.** Insider filings are mapped to sector
ETFs through the released `alpha_agent.pit_sector` no-look-ahead reader, keyed on
the SIC a filing carried at its own SEC acceptance timestamp. A company
reclassified in 2024 is not reclassified in 2011. Classification coverage ramps
with the owned Financial Statement Data Sets — 0 % in 2008, 24 % in 2010, **96 %
from 2012** — and the ramp is reported rather than hidden.

### The insider family is counted, never valued — and that was measured

The obvious construction is dollar value: shares × price per share, signed. It
was built first, and disqualified by measurement. `TRANS_SHARES` and
`TRANS_PRICEPERSHARE` are unvalidated filer-entered fields, and the acquired
archives contain a **single filing implying $2.1 × 10¹⁶** and a 2008 total of
$1.3 × 10¹⁶ — about a hundred thousand times the true figure. A value-weighted
series measures typography. A **count** cannot be mistyped, so each (issuer,
filing) is classified BUY / SELL / MIXED by transaction code alone — the one
field with a closed vocabulary — and every feature is a filing count. The
decision was taken on the acquired data *before* any predictive evaluation and
is recorded in the source, not in a commit message.

---

## 4. Orthogonality: measured before prediction, and a gate

The wrong test is raw correlation, and it is the easy one to pass. The measured
quantity is the **residual share** — the fraction of a feature's variance that
survives a regression on all 28 base features, on TRAINING rows only.

| family | median residual share | verdict |
|---|---|---|
| FUTURES_POSITIONING | **0.892** | 4 of 4 features DISTINCT |
| FX_INTEREST_CARRY | **0.848** | 2 of 2 DISTINCT |
| COMMODITY_TERM_STRUCTURE | **0.622** | 3 of 3 DISTINCT |
| INSIDER_TRANSACTION_INTENSITY | 0.535 | 2 DISTINCT, 1 partially redundant |
| IMPLIED_VOLATILITY_TERM_STRUCTURE | 0.243 | 1 DISTINCT, 2 partially redundant |
| MARKET_IMPLIED_RISK_PREMIA | 0.244 | 2 DISTINCT, **2 REDUNDANT** |

**The gate did fire, and on the family that most deserved it.** Two of the four
risk-premia features are labelled REDUNDANT against the base set:
`curve_curvature` retains 9.2 % of its variance and ranks 0.952 with the base's
`g_yield_slope`; `credit_premium_baa10y` retains 9.5 % and ranks 0.73 with
`g_credit_spread`. Those are not new information, and the label says so before
any return was looked at.

**Two measured facts worth keeping.** `iv_term_slope_chg_21` ranks **0.886** with
the base's one-month VIX change — the release's own warning that "a renamed VIX
level is not orthogonal information" turned out to apply to part of its own
implied-volatility family. And `insider_market_net_buy_63` ranks **0.785** with
the Baa−Aaa quality spread: aggregate insider buying is largely a reading of the
credit cycle, which is a genuinely interesting thing to have measured and a poor
reason to expect incremental alpha.

---

## 5. The predictive increment — the release's primary object

The statistic is a **paired per-date difference**, declared before the run:

```
delta(d) = rankIC( BASE + NEW , d ) − rankIC( BASE , d )
```

on the same date, the same 47 instruments, the same realised returns and the
**same model configuration** — selected on the base arm's inner-validation block
inside training and then forced on both arms, so a difference cannot be a
luckier architecture. The two arms are row-identical by construction: R35 appends
columns to R34's design matrix and never rebuilds it.

| information set | h=5 | h=20 | h=60 |
|---|---|---|---|
| FUTURES_POSITIONING | −0.0011 (t −0.47) | −0.0002 (t −0.05) | `NO_EFFECT` |
| FX_INTEREST_CARRY | −0.0006 (t −0.71) | −0.0031 (t −1.78) | +0.0008 (t 0.55) |
| COMMODITY_TERM_STRUCTURE | +0.0007 (t 0.45) | +0.0001 (t 0.03) | `NO_EFFECT` |
| IMPLIED_VOLATILITY_TERM_STRUCTURE | +0.0005 (t 0.34) | −0.0061 (t −0.63) | +0.0025 (t 0.61) |
| MARKET_IMPLIED_RISK_PREMIA | −0.0002 (t −0.24) | **+0.0168 (t 1.78)** | +0.0049 (t 1.38) |
| INSIDER_TRANSACTION_INTENSITY | −0.0004 (t −0.20) | +0.0003 (t 0.05) | +0.0034 (t 0.82) |
| ALL_NEW_COMBINED | +0.0014 (t 0.34) | **−0.0247 (t −1.61)** | +0.0091 (t 1.51) |

Nothing clears `t ≥ 2.0`. `ALL_NEW_COMBINED` is the most negative cell in the
table at the primary horizon: adding all 19 features to the 28 makes the forecast
**worse**, which is what over-parameterising a design that had no incremental
signal to find looks like.

**`NO_EFFECT` is not a tested null.** At h=60 the base arm's inner validation
selected an elastic net that shrank every added coefficient to exactly zero, so
the augmented arm reproduced the base arm bit for bit. That is a real
observation — the model declined the information — and counting it as "no
increment" would repeat the defect that superseded R34 v1, where three finalists
reported identical economics because the object they varied could not move.
`arm_responded` detects it, the gate fails on `arm_could_respond`, and the
summary excludes it from "best increment".

### Not significant, and here is what would have been

Every failed comparison reports the **minimum detectable increment** — the
smallest mean per-date increment the comparison could have called significant at
the frozen threshold. At h=20:

| information set | observed | MDI |
|---|---|---|
| MARKET_IMPLIED_RISK_PREMIA | +0.0168 | 0.0188 |
| IMPLIED_VOLATILITY_TERM_STRUCTURE | −0.0061 | 0.0193 |
| INSIDER_TRANSACTION_INTENSITY | +0.0003 | 0.0124 |
| FUTURES_POSITIONING | −0.0002 | 0.0077 |
| FX_INTEREST_CARRY | −0.0031 | 0.0034 |
| COMMODITY_TERM_STRUCTURE | +0.0001 | 0.0041 |

The risk-premia family missed by **11 %** of its own standard error. That is a
near miss and it is reported as one; it is not a finding, and §7 explains why it
would not have been one even if it had cleared.

---

## 6. The finding that matters: significant standalone is not incremental

Four of six families predict returns **on their own**, scored on the rows they
actually cover:

| family | standalone rank IC | t | best increment over BASE |
|---|---|---|---|
| MARKET_IMPLIED_RISK_PREMIA | **+0.0638** | **2.95** | +0.0168 (t 1.78) |
| FUTURES_POSITIONING | **+0.0492** | **2.29** | −0.0002 (t −0.05) |
| IMPLIED_VOLATILITY_TERM_STRUCTURE | **+0.0474** | **2.08** | +0.0025 (t 0.61) |
| INSIDER_TRANSACTION_INTENSITY | **+0.0408** | **2.72** | +0.0034 (t 0.82) |
| FX_INTEREST_CARRY | +0.0458 | 1.62 | +0.0008 (t 0.55) |
| COMMODITY_TERM_STRUCTURE | — | — | +0.0007 (t 0.45) |

Positioning ranks 17 instruments at rank IC 0.049, t = 2.29, over a
survivorship-safe 26-year sample, using information no price series contains —
and conditional on 28 price, trend and risk-state features it contributes
**−0.0002**. That is the release in one row. A dataset is not successful because
it has a significant standalone relationship with returns; the base information
set already contained everything in these sources that predicts.

Commodity term structure reports no standalone number rather than a fabricated
one: it covers three instruments, and a cross-sectional rank IC needs at least
five names on a date. The standalone arms are scored on **covered rows only**,
because a family covering 17 of 47 instruments leaves the other 30 with an
identical all-zero feature block, one shared mid-rank, and a rank IC that could
come out of the coverage pattern alone.

---

## 7. Robustness, and the near miss

**Subperiod stability kills the near miss on its own terms.** The risk-premia
increment at h=20, one walk-forward evaluation block at a time:

| block | increment | t |
|---|---|---|
| 2008–2010 | **+0.0349** | 2.07 |
| 2011–2013 | **+0.0503** | 1.54 |
| 2014–2016 | +0.0032 | 0.15 |
| 2017–2019 | +0.0066 | 0.71 |
| 2020–2022 | +0.0147 | 0.51 |
| 2023–2026 | −0.0059 | −0.38 |

The whole effect lives in 2008–2013 and is gone afterwards. Real yields and
breakeven inflation carried information about cross-asset returns during the
financial crisis and the euro crisis, and stopped. That is a regime observation,
not an information finding, and the per-block table exists to tell the two apart.

**Publication-lag sensitivity.** Positioning was re-measured at a 28-day lag —
long enough to cover the 2013 and 2018-19 shutdown catch-ups, when reports
appeared weeks late and a six-day rule would claim knowledge nobody had. The
increment moves from −0.0002 (t −0.05) to +0.0065 (t 1.73). The sign flips and
**neither lag clears the gate**; the declared lag is the contract's and the
stressed number qualifies it rather than replacing it.

---

## 8. Lane A — analyst expectation change: `SOURCE_ACQUISITION_BLOCKED`

The primary equity lane could not be executed, and the reason is measured rather
than remembered.

- **What the estate owns.** The Intrinio/Zacks trial extract is **one retrieval
  day** (2026-08-10), stamped `CURRENT consensus snapshot per current member`
  over the vendor's `norgate_current_members` universe — survivorship-biased by
  construction — plus one shallow 2023–2024 sales-surprise pull. A revision
  history needs a dated consensus observation per issuer per date across a
  survivorship-safe universe. Differencing today's numbers to manufacture one is
  the first item on the prohibited-substitutions list.
- **What the free entitlements return.** Six endpoints this estate already holds
  keys for were probed read-only. FMP, Finnhub and Nasdaq Data Link answer
  **HTTP 403** — no entitlement. EODHD and Alpha Vantage answer, and what they
  answer with is today's estimate plus "7 / 30 / 60 / 90 days ago" deltas:
  `CURRENT_SNAPSHOT_ONLY` in the Release-32 vocabulary, **inadmissible as
  history**. Zero of six are admissible.
- **The gates are the released ones, called.** Stage 13A's adequacy gate returns
  `TRIAL_DATA_INSUFFICIENT`; its purchase decision names the weakest gate.
  Release 32's ten-condition Information Purchase Gate returns
  **`EVALUATED_DO_NOT_BUY`**, consistent with the prior evaluations it already
  carries (Intrinio: `NO_DEFENSIBLE_ALPHA` on a survivorship-safe 16-year test;
  FMP grades: survivorship FAIL).

**No statistical evidence is claimed from the one-day sample**, and the artifact
says so in a named field. What the sample validated is schema and acquisition
mechanics — which is what a sample of that size can validate.

**What would unblock it:** a dated historical consensus panel with ≥ 15 years,
≥ 800 distinct issuers and ≥ 30 % inactive/delisted coverage. Both routes
require money or a sales conversation. Neither is authorised by this release,
and the campaign did not wait for one.

---

## 9. Evidence honesty

**`FRESH_UNSEEN_EVIDENCE_EXISTS = False`, declared in the contract before the
campaign ran.** Releases 31–34 all selected on market outcomes through August
2026 and R33's lockbox was opened eight times. **Acquiring a new FEATURE does
not make an already-consumed OUTCOME period unseen** — the returns this release
is scored against are the same returns four campaigns have already looked at. No
untouched historical block remains, so the release produces
`HISTORICAL_WALK_FORWARD_EVIDENCE` and never a lockbox result, and
`R35_ORTHOGONAL_INFORMATION_ADDS_INCREMENTAL_EDGE` could at most have
established a RESEARCH CANDIDATE.

**Three results, not two.** Release 33 introduced `SYSTEM_RESULT` and
`ALPHA_RESULT`. Release 35 adds `RESEARCH_CANDIDATE_RESULT` between them, because
a genuinely positive historical increment is a real finding AND is not Alpha, and
collapsing those into one word is how a release starts lying to itself.
`ALPHA_RESULT = PASS` requires a qualified verdict **and**
`genuinely_independent_evidence_exists()`, which returns False in one place — so
it is structurally unreachable here, and a test proves it by feeding the verdict
builder a fully qualified result and watching Alpha still come back FAIL.

---

## 10. Integrity

- **Nothing about the model or the conversion was searched.** The universe, the
  panel, the 28 base features, the model families, the walk-forward partition and
  the entire conversion configuration are imported frozen. The only degree of
  freedom is which information the model may see.
- **The base arm is a verified anchor.** It reproduces R34's finalist to every
  published digit. A test asserts it against R34's own artifact.
- **Denominator counts all 28 executed configurations**, ceiling 80, planned 35.
  19 carried a p-value; Benjamini–Hochberg at q = 0.10 rejected **none**, in
  either direction.
- **BH rejections are split by direction.** Only positive rejections can support a
  qualification — R34 learned that a two-sided rejection can be a significant
  *loss*.
- **Orthogonality is measured on training rows only.** Measuring redundancy on
  evaluation rows would put evaluation data into the decision that gates
  evaluation.
- **$0 spent, 0 trials started, 0 accounts created**, and a test asserts that no
  credential reaches an artifact.
- **No operational write**, proven statically by the architecture audit and at
  runtime by the canonical Release-33 attribution rule, extended with an R35
  profile: `ATTRIBUTED`, 11 sources scanned, 0 findings.

---

## 11. What Release 36 should not conclude

This is not "the new data was bad". It was acquired, checksummed, stamped with
real publication times, measured 0.24–0.94 residual against everything R34 knew,
and four of six families predict on their own at t > 2. It is not "there was not
enough of it": 130 payloads, 901 MB, 26 years of positioning, 40 years of a real
futures curve, 760,000 insider filings.

The finding is narrower and harder: **on a 47-instrument cross-asset ETF
cross-section at monthly cadence, six economically distinct free information
families add nothing to what price, trend and risk state already say.** The
strongest candidate — market-implied risk premia — missed its pre-registered
threshold by 11 % of a standard error, and its entire effect sits in 2008–2013.

Two directions remain honestly open, and they are different from each other.
The first is information that is *not free*: the one lane this release could not
execute is the one every prior release has pointed at, and it is blocked on
money rather than on method. The second is a different **decision problem** —
this cross-section is 47 liquid funds rebalanced monthly, and information about
individual companies has nowhere to express itself in it. Neither is a reason to
run another optimiser campaign.
