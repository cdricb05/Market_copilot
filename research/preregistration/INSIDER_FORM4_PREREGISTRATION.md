# PREREGISTRATION — SEC FORM 4 CLUSTERED OPEN-MARKET INSIDER BUYING

**Family id:** `INSIDER_FORM4_CLUSTERED_PURCHASE_EVENT_V1`
**Ontology dimension:** `INSIDER_BEHAVIOUR` (US_EQUITY), scored under the separate
block key `SEC_FORM4_CLUSTER_EVENT`
**Status:** FROZEN. Written and committed BEFORE any forward return, event return,
rank IC, book or t-statistic for this family was computed on real data.

This document is the contract. It is not edited after results exist. If a
statement here turns out to be inconvenient, the inconvenience is the result.

---

## 0. WHY THIS AXIS, AND WHAT IT IS NOT

13F breadth (`8cf9fa3`), 13D/G control blocks (`37c5e8e`), the 8-K Item family
(`36d7d47`) and analyst-revision vintages (`f4b4b08`, INADEQUATE) are CLOSED and
not revisited.

**Economic hypothesis.** Corporate officers and directors hold information about
their own firm that is not fully in price or fundamentals. When SEVERAL of them
independently commit their OWN capital to the common stock in discretionary
purchases within a short window, that act carries information about subsequent
return that survives after the filings become public.

### 0.1 This axis is NOT fresh — the estate has tested insider information 18 times

Read from the stored artifacts before this document was written. **None succeeded
in its own preregistered direction.**

| release | object | tests | result |
|---|---|---|---|
| R27 (`alpha_exhaustion_campaign/.../insider_transactions_results.json`) | per-name monthly 182-day STATE: net shares, buyer ratio, officer net buy, net dollars, sell intensity, **3+ buyers in 182 days** | 6 | all REJECTED; rank IC t −1.54 / −0.68 / −1.50 / −1.49 / −1.52 / **−3.22 (cluster, wrong sign)** |
| R35 (`predictive_increment.json`) | sector-ETF insider intensity | 4 | increment t −0.20 / 0.05 / 0.82; standalone t 2.72 with no increment |
| R39 (`continuation_candidate_registry.json`) | SPY insider-breadth timing; per-name insider added to ridge and LightGBM | 3 | after-cost t −1.18 / −0.47 / +0.34 |
| R63 (`information_sensitivity_matrix.json`) | the owned `INSIDER_BEHAVIOUR` block (net buy filings 63d, buys/sells 126d) at h = 1/5/21/63 | 4 | all NO_CONDITIONAL_VALUE; conditional t 0.33 / −0.06 / **−2.13 / −2.22** |
| Phase 11 (memory record; no artifact located) | Finnhub insider MSPR, 292 tickers | 1 | weak and wrong-signed |

**The prior points AGAINST this hypothesis, and it is declared here so that a null
result cannot be described as a surprise.** R27's cluster measure was significantly
negative, and R63's own insider block subtracts information at 21 and 63 sessions.

### 0.2 What makes this family a different object — and not a rescue

| | every prior test | this family |
|---|---|---|
| timing | a trailing STATE sampled at month-end or on a grid, up to 6 months stale | an EVENT at the SEC acceptance instant of the filing that completes the cluster |
| availability | FILING_DATE (day precision) | the acceptance INSTANT, 99.25% of rows |
| who | any P/S reporter, incl. ten-per-cent funds | officers and directors only |
| what | P and S counted, netted, or valued | P only, original forms, common equity, not swaps, not declared 10b5-1, not stale |
| measurement | rank IC or a book on the state | post-publication event study + the canonical scorer, conditional on the owned insider block |

The cluster parameters were chosen from the word "multiple" and from the
canonical grid unit (§3), not from R27. R27's result was known when this document
was written; that is why R27 enters this family's multiplicity burden (§9) instead
of being ignored.

**Insider SELLING is not a cell.** Sales have liquidity, diversification, tax and
plan motives that the filing does not separate; no economically clean pre-return
hypothesis exists for it here.

---

## 1. DATA — FREE SEC ONLY, ALREADY OWNED

No purchase, no subscription, no licence, no key. `paid_dollars: 0`.

### 1.1 Inventory of the existing estate

| store | what it is | usable as PIT history? |
|---|---|---|
| SEC Insider Transactions Data Sets, R35 acquisition (`r63.FORM345_DIR`) | 73 quarters 2008Q1–2026Q1: SUBMISSION, REPORTINGOWNER, NONDERIV_TRANS | **YES** — transactions |
| EDGAR submissions histories, R63 acquisition (1,080 issuers) | `acceptanceDateTime` for 837,920 Form 4 rows, 2003→2026-09-09 | **YES** — the availability instant |
| `alpha_agent/ingestion/normalized/INSIDER_FILING` | 32,673 filing HEADERS (32,573 sec_edgar, 100 eodhd), available 2025-11-19→2026-09-11, no transaction code | no — header only, 10 months |
| R46 `_data_form4` | 39 daily parsed files 2026-07-29→2026-09-11 | no — prospective only |
| R59 `r59_insider_transactions.json` | 5.7 KB | no |
| EODHD insider endpoint | entitled; 100 records captured | no historical cache |

**The estate already owns adequate PIT history. Nothing was acquired.** The one
missing-quarter request made (`insider_form4_data.acquire_missing`, 2026-09-13):
2026Q2 and 2026Q3 answer **HTTP 404** — not yet published by the SEC. 2006–2007
exist at the SEC but precede every scored window by more than three years.

### 1.2 Measured stream (panel-identified issuers; census, no return)

Census artifact: `results/insider_form4_census.json`
(`artifact_hash 79aae04bac040897f27351582f17f7eb381ce36cef112d4edfb65ec4d01e0a75`).

| measurement | value |
|---|---|
| filings covered | 2008-01-02 → **2026-03-31** (observable through the last owned quarter) |
| all non-derivative codes, panel issuers | S 531,519 · A 294,659 · M 268,298 · F 261,711 · G 56,534 · **P 48,442** · J 36,583 · D 23,190 · C 14,170 · I 3,340 · X 1,738 · L 1,018 · W 468 · U 262 · Z 230 · O 9 · E 8 |
| purchase (P) rows / filings / issuers | **48,442 / 23,751 / 831** |
| P form mix | 4: 46,348 · 4/A: 1,217 · 5: 867 · 5/A: 10 |
| P direct / indirect | 30,482 / 17,960 |
| P security class | common 47,234 · preferred 1,022 · other 127 · depositary 58 · debt 1 |
| P reporter role | officer or director 39,604 · ten-per-cent owner only 8,294 · other only 544 |
| acceptance instant joined | **48,080 (99.25%)**; 362 use the 17:30 ET filing-date bound |
| publication lag (acceptance − transaction date) | median 2 d, p90 6 d, p95 100 d, p99 897 d; **6.76% > 30 d** |
| `AFF10B5ONE` (10b5-1 plan declared), P rows | column ABSENT before the 2023 amendments; declared TRUE: 2023 474 · 2024 884 · 2025 780 · 2026Q1 89 |
| price field present | 99.65% — **not used** (value is BLOCKED_SOURCE in the estate) |

### 1.3 Traps measured, each of which silently corrupts the stream

1. **The data sets carry a DATE, not an instant.** `FILING_DATE` only. The instant
   is joined on (issuer CIK, accession) from the submissions histories; where
   absent, **17:30 Eastern on the filing date** — EDGAR assigns the next business
   day to anything accepted at or after 17:30, so this bound is never earlier than
   the truth.
2. **`acceptanceDateTime` is UTC** (measured on 13D/G); the one converter is reused.
3. **The role vocabulary drifts.** 2010 files glue tokens (`TenPercentOwnerOther`,
   `Director,OfficerOther`); 2024 files use commas. Roles are read by containment.
4. **`AFF10B5ONE` is absent before 2023 and spelled `0/1/false/true` after.** Absent
   or blank is UNOBSERVED, never "not under a plan".
5. **"Shares" is not "common".** `Depositary Shares "A" Preferred` must be classed
   before the word "shares" is read.
6. **Stale catch-up reports.** 1% of P rows are published more than 897 days late.
   A late filing stays late; it is never back-dated, and a purchase older than the
   cluster window at its own publication cannot join a cluster.

---

## 2. THE CLEANING — fixed in advance, in this order

A **qualifying purchase** is a non-derivative row that passes every rule. The
first failing rule is recorded, so the funnel adds up (census, no return):

| # | rule | rows removed |
|---|---|---|
| 1 | original Form 4 or Form 5 only (amendments restate; counting both double-counts) | 1,227 |
| 2 | marked Acquired | 130 |
| 3 | positive shares | 46 |
| 4 | not an equity swap | 8 |
| 5 | COMMON equity (preferred, depositary, debt, warrants/rights excluded) | 1,169 |
| 6 | the filing does NOT declare Rule 10b5-1 plan reliance | 2,179 |
| 7 | at least one reporter is an OFFICER or DIRECTOR | 8,035 |
| 8 | an availability instant exists | 0 |
| 9 | transaction date present and not after publication | 14 |
| 10 | transaction not more than 30 days before its own publication | 1,997 |
| | **qualifying rows** | **33,637** → 19,002 filings, 816 issuers, 6,062 insiders |

Code `P` is the only transaction code. Grants (A), exercises (M, X), tax
withholding (F), gifts (G), inheritance (W), conversions (C), dispositions to the
issuer (D), discretionary plan transactions (I), small acquisitions (L), voting
trusts (Z), tender dispositions (U), other (J, O, E) and every sale (S) are
excluded by construction. **No code is added after results.**

**Declared limitations of the cleaning, not repaired:**

* Code `P` covers open-market AND private purchases; the filing does not separate
  them machine-readably. Not inferred.
* 10b5-1 reliance is observable only from 2023. Earlier plan purchases cannot be
  seen and stay in. Discretionary intent is never inferred from footnote text.
* A filing is ONE insider act. Its insider is the lowest officer/director CIK on
  it, so a director and the director's fund filing jointly are one insider.

---

## 3. CELL SELECTION — WITHOUT RETURNS

### 3.1 CELL A — CLUSTERED OPEN-MARKET INSIDER BUYING: **FROZEN**

At the availability instant `a` of a qualifying purchase filing of issuer *i*, an
event fires when:

* the qualifying filings of *i* public in **(a − 30 days, a]**, whose latest
  qualifying purchase is dated after **date(a) − 30 days**, name **at least 2
  distinct insiders**; and
* *i* has had **no event of this cell in the 30 days before `a`**.

* **K = 2** is the literal meaning of "multiple distinct insiders". It is not R27's
  3, and it was not searched.
* **30 calendar days** is one canonical grid period (21 sessions). The same number
  bounds membership, staleness and the refractory period, so the cell has exactly
  ONE time parameter.
* Only filings already public at `a` are ever consulted.
* **Direction: POSITIVE, frozen.**

Composition (eligible, decision session from 2011-07-01): **1,091 events**,
2011-07-07 → 2026-03-20, **344 issuers**, 346 panel rows; **96 events on 46
later-delisted rows**; 878 selection / 213 lockbox; 879 distinct decision
sessions. Completions before 2011-07-01 (712) build state only; 835 completions
fell while the issuer was not eligible. The completing filing was accepted after
the close for 665, intraday 345, pre-open 79. 1.56% of event names leave the
eligible universe within 63 sessions.

Events per year: 2011H2 68 · 2012 82 · 2013 60 · 2014 52 · 2015 74 · 2016 88 ·
2017 73 · 2018 82 · 2019 90 · 2020 97 · 2021 43 · 2022 69 · 2023 67 · 2024 47 ·
2025 72 · 2026Q1 27.

**Sufficiency, in the scorer's own units** (`event_8k_data.oos_sufficiency`: only
out-of-sample test periods whose cadence window holds an event):

| h | OOS test periods | with an event | effective informative periods | floor 36 |
|---|---|---|---|---|
| 5 | 485 | 297 | **297** | met |
| 21 | 115 | 111 | **111** | met |
| 63 | 113 | 111 | **37** | met — one period above the floor |

### 3.2 CELL B — LARGE OPEN-MARKET INSIDER PURCHASE: **NOT DEFINED**

The brief's own rule: use CELL B only if its denominator is PIT-safe and it can be
defined cleanly; otherwise run one cell rather than invent a replacement.

* **Relative to insider holdings.** `SHRS_OWND_FOLWNG_TRANS` is populated (99.95%)
  and PIT-safe, but it is the holding of ONE ownership line — direct, or one
  indirect nature — not the insider's total beneficial ownership, and it omits
  derivatives. 29.3% of qualifying purchases are on indirect lines, and 7.3% start
  from a line holding ≤ 0, where the ratio is undefined. In large caps, a large
  purchase relative to a small holding is often a new director meeting ownership
  guidelines — a non-informational motive. **No threshold exists in law or
  regulation**; any cutoff would be arbitrary.
* **Relative to issuer market value.** The numerator needs `TRANS_SHARES ×
  TRANS_PRICEPERSHARE`, which the estate records as **BLOCKED_SOURCE
  FILER_ENTERED_FIELD_UNVALIDATED** (R35: one filing implied $2.1e16). Using it
  would contradict a standing estate rule.

**CELL B is not run. No replacement cell is created, before or after results.**

---

## 4. POINT-IN-TIME CONTRACT

**The boundary is the SEC ACCEPTANCE INSTANT** (UTC → Eastern, US DST rule),
else 17:30 ET on the filing date as an upper bound. **The transaction date is
never the availability boundary.** A late filing stays late.

**Decision session `t`** = the FIRST session whose 16:00 Eastern close is STRICTLY
after the instant. **Entry at the close of `t+1`** — canonical NEXT_CLOSE
(`alpha_agent.r63.pit.forward_compound`), window `t+2 … t+1+h`. No return that
contains a pre-publication session can qualify.

**Diagnostics — reported, excluded from every gate by test:**

* **pre-publication** — market-adjusted return over sessions `t−21 … t−1`, strictly
  before the session containing publication (the purchases were private then);
* **announcement session** — close `t−1` → close `t`;
* **forgone first post-publication session** — close `t` → close `t+1`. Legal,
  forgone by the canonical lag. **Not a rescue: no same-day or first-session
  variant will be scored.**

**The unobservable tail.** The owned archive ends 2026-03-31. On a decision session
whose window reaches past it, "no event" is UNOBSERVED: the panel signal is NaN
there, never 0 (h=5: 20 sessions, h=21: 4, h=63: 2).

---

## 5. IDENTITY AND SURVIVORSHIP

Identity is **issuer CIK → panel row**, one hop: `ISSUERCIK` is on every filing,
and the routes are the SAME authoritative ones as 13D/G and 8-K (owned R58 bridge
1,124 rows, SEC `company_tickers.json` 6 still-listed rows, 0 disagreements). **No
issuer-name matching anywhere.** 42 CIKs map to 86 rows; an event is attributed to
each eligible row and reported.

**Declared limitation, measured before results:** 1,130 of 1,897 panel rows carry a
CIK. On the eligible cross-section the assessable share is mean **95.32%**, worst
**89.63%**, and **46.4% of decision sessions (84/181) sit below 95%**.

**Delisted coverage:** 1,236 panel rows are later-delisted; 469 of them (37.9%) are
identified. On eligible decision sessions, **64.3% of delisted-row observations are
assessable against 100% of still-listed ones.** Unidentified names leave the
universe at **17.90%/yr against 3.05%** — 4.7% of the cross-section, **22.3% of all
exits**. A firm whose insiders buy and which is later acquired or fails is exactly
what this blindness hides.

---

## 6. HORIZONS — FROZEN, BY THE EXISTING BUDGET

The brief prefers 21 and 63, adding 5 only if the existing multiplicity budget
supports it without expanding the family beyond the frozen campaign rule. That
rule is `FAMILY_PRIMARY_MAX = 6`; one cell × three horizons = 3 ≤ 6. **Horizons are
5, 21 and 63 sessions**, cadence `min(h, 21)` — the same rule the one-cell 8-K
family applied. Freezing 5 now also forecloses a later "try the short horizon".
Nothing is added, removed or searched after results.

---

## 7. MEASUREMENT — two arms, both frozen

### Arm 1 — EVENT STUDY (the qualification input)

The frozen owners `control_block_alpha.event_study` / `event_8k_alpha.event_study`,
entry per §4: raw; **market-adjusted (minus SPY total return) — the primary
statistic**; sector-adjusted (DIAGNOSTIC — no PIT GICS); equal-risk. Clustered by
decision session, Newey-West lag `h−1`. SELECTION (< 2023) / LOCKBOX (≥ 2023)
split; the market-adjusted return in the frozen direction net of the cost ladder
with Sharpe and maximum drawdown.

### Arm 2 — CROSS-SECTIONAL PANEL (the incremental test)

`sig[i,t] = +1 × (events for name i in (t−cadence, t])`, 0 for an eligible name
without an event, NaN on the unobservable tail. Scored by the ONE scorer
`alpha_agent.r63.sensitivity.run_cell` (with the sparse-event repair `a33f241`,
which removes a blocker and relaxes nothing). **No second scorer.**

### Required controls

* the owned US_EQUITY baseline (`ontology.baseline_for`, 10 dimensions: price
  state, trend, momentum, reversal, volatility, tail, liquidity, participation,
  fundamental levels, free cash flow);
* **the owned `INSIDER_BEHAVIOUR` block** — a clustered-buying event is mechanically
  correlated with trailing insider buy counts, so what that block carries cannot be
  credited to the event;
* **short positioning** — REQUIRED. Measured: the canonical equity dataset carries
  **no** `SHORT_POSITIONING` block (its blocks are the 10 baseline dimensions,
  FUNDAMENTAL_CHANGE, CORPORATE_DISCLOSURES, INSIDER_BEHAVIOUR,
  DISCLOSURE_INTENSITY_LANGUAGE, EVENT_INFORMATION). The control is applied
  automatically if the block exists; while it does not, §10 gate 12 fires;
* the incumbent `fundamental_momentum_50_50_v1` — a second scorer run with the
  incumbent as baseline, and a contemporaneous cross-sectional residualisation on
  controls + incumbent for the orthogonalised rank IC and book.

The incumbent top-50 book blended 50/50 (z units) with the orthogonalised signal is
DESCRIPTIVE only.

---

## 8. COSTS

1 / 2 / 5 bp per side, plus the desk single-name rate `EQ_COST_RATE_PER_SIDE` =
**12.5 bp per side** reported alongside. The event study pays entry and exit.

---

## 9. MULTIPLE TESTING — the burden is inherited, never reset

Owner `alpha_agent.r31.multiple_testing` (Benjamini-Hochberg, q = 0.10), over the
event-study market-adjusted two-sided p-values. A missing p-value counts as p = 1.

* **Family burden:** ONE declared family, **m = 3**.
* **Inherited burden:** the same three p-values judged again with the **18 prior
  insider tests of §0.1 in the denominator at p = 1**, **m = 21**. A prior failure
  can never lower the bar, and none of the priors can occupy a rejection slot.

Both must pass. Rejected cells stay in both denominators.

---

## 10. QUALIFICATION — existing gates only, in this order

1. **Coverage.** More than `MAX_UNCOVERED_SHARE` = 0.20 of decision sessions under
   `MIN_DATE_COVERAGE` = 0.95 → **DATA_HOLD**. *Measured: 46.4%. EXPECTED TO FIRE.*
2. **Missingness bias.** Unassessable names' mean forward return differing by more
   than `MATERIALITY_ANN_NET` = 1.5%/yr → **DATA_HOLD**. *The same identity layer
   fired this gate on 13D/G and 8-K.*
3. **Effective periods.** Scorer DATA_HOLD, or `min(scorer effective periods,
   informative periods of §3.1)` < 36 → **NEED_MORE_EVIDENCE**.
4. **Event-study floor.** Event study not OK → **NEED_MORE_EVIDENCE**.
5. **Frozen sign.** Post-entry market-adjusted mean ≤ 0 (opposite to POSITIVE) →
   **NO_EDGE**. The sign is never reversed; the observed sign is DISCOVERY only.
6. **Post-publication effect.** Event-study market-adjusted |t| <
   `STANDALONE_T_FLOOR` = 2.0 → **NO_EDGE**.
7. **Conditional value.** Scorer conditional t < `CONDITIONAL_T_FLOOR` = 2.0 (must
   HELP) → **NO_EDGE**.
8. **Family multiplicity.** Failing BH q = 0.10 over m = 3 → **NO_EDGE**.
9. **Inherited multiplicity.** Failing BH q = 0.10 over m = 21 → **NO_EDGE**.
10. **Incremental.** Redundancy `REDUNDANT`, or orthogonalised rank-IC t < 2.0, or
    incremental-vs-incumbent scorer t < 2.0 → **NO_INCREMENTAL_INFORMATION_EDGE**.
    Not rescued.
11. **Materiality.** Scorer net annual increment < 1.5%/yr → **NO_EDGE**.
12. **Required control.** No short-positioning block in the dataset →
    **NEED_MORE_EVIDENCE**.
13. Otherwise → **QUALIFIED**.

**Declared merits reading.** Because gates 1–2 are statements about data, the SAME
frozen function is also evaluated with both data gates satisfied and reported beside
the formal verdict. It cannot turn a formal DATA_HOLD into a qualification.

**CAPITAL ELIGIBLE remains NO** unless every qualification gate AND every existing
forward-evidence and governance gate is satisfied. Nothing here can promote a model,
allocate capital, or create a proposal, order or fill.

---

## 11. WHAT WOULD FALSIFY THE HYPOTHESIS — and what will NOT be done

A null result means: two or more officers or directors buying their own company's
common stock within 30 days, observed at the true acceptance instant and entered at
the canonical next close, carries no information about subsequent market-adjusted
return that the estate's owned families and its own insider block do not already
carry — subject to the blindness declared in §5.

* buying has no edge → the cell closes;
* sign opposite → NO_EDGE, DISCOVERY only;
* effect only in the pre-publication or announcement windows → not tradable alpha;
* effect gone after controls → NO_INCREMENTAL_INFORMATION_EDGE;
* multiplicity kills it → the cell closes.

**Forbidden after results:** reversing the sign; changing K, the 30-day window or
the refractory period; changing horizons; adding transaction codes, ten-per-cent
owners, amendments or plan purchases; splitting by role (CEO/CFO), size, sector,
direct-only or dollar value; a small-cap universe; a same-day or first-session entry;
a second cell.

## 12. SAFETY

RESEARCH ONLY. No purchase, no subscription, no licence, no model promotion, no
capital allocation, no portfolio mutation, no proposal, no order, no fill, no
backfill, no live deployment, no live write. The live checkout and the live
next-open skew challenger are not touched.
