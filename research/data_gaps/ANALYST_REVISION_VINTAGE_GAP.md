# Analyst expectation revision vintages: owned / free inventory and minimum purchase gap

Measured 2026-09-13 on branch `alpha-recovery-offensive`. Research only.
No purchase, no trial, no subscription change, no vendor e-mail, no
preregistration, no forward return computed.

## Verdict

**ANALYST_REVISION_DATA = INADEQUATE. STOP BEFORE PURCHASE.**

No owned or free source provides point-in-time revision vintages: the
consensus as it stood on a past date, re-observable later without hindsight,
for names that were later delisted. The R32 gate state for every owned and
free source is **PIT_BLOCKED**. The one field that does have deep history —
EPS surprise against the vendor's report-time consensus — is also
**COVERAGE_BLOCKED** in exactly the years the canonical equity grid needs.

No analyst-revision family was preregistered, because the brief makes adequate
PIT vintages a precondition. Substituting current snapshots into history is a
prohibited substitution (docs/INFORMATION_PURCHASE_GATE.md).

## How it was measured

* **Estate scan.** Every cached analyst, estimate or earnings artifact under the
  data root, the live `research/data` and `data` folders, and this worktree,
  including the owned EODHD daily analyst captures and the Intrinio/Zacks trial
  cache. Synthetic fixtures (tickers S000-S039, MOCKA-MOCKC) were excluded.
* **Read-only entitlement probe.** 25 GETs: EODHD 14, FMP 4, Alpha Vantage 3,
  Finnhub 4, plus the local Norgate API listing. Keys were read from the
  environment and never stored.
* **$0 offline PIT test.** Across the owned EODHD captures (6 tickers × 18-19
  `retrieved_at`-stamped captures, 2026-07-31 to 2026-09-11), each past report's
  `epsEstimate` was compared between captures, and each report that fell between
  two captures was compared with the last pre-report consensus.
* **$0 offline coverage test.** EODHD calendar windows (fetched once) against the
  survivorship-safe Norgate PIT S&P 500 eligibility, by base ticker. This is an
  approximate availability measure; a reused ticker can match another company.

## Inventory by source

| Source | Cost | History start | PIT timestamp | Revision vintages | Inactive / delisted | Universe coverage | Usable without hindsight |
|---|---|---|---|---|---|---|---|
| EODHD `Earnings.Trend` + `AnalystRatings` (EPS/revenue avg/high/low, analyst counts, `epsTrend7/30/60/90daysAgo`, `epsRevisionsUp/Down` 7/30d, target price, rating counts) | existing subscription (paid, monthly, 100k calls/day) | none: a current snapshot; "N days ago" is relative to the request | request time only | **NO** | delisted names return the snapshot frozen at delisting (ATVI 28 periods, TWTR 24, CELG 13, SIVB 0) | current only | **NO** |
| EODHD `Earnings.History` + `calendar/earnings` (report date, EPS actual, **EPS consensus at report**, surprise) | existing subscription | calendar 2004-01 (4,862 rows in 2004Q1); per ticker 1993+ | report date only; no as-of stamp | **NO** | acquired names kept (ATVI 105/113 with estimate, SIVB 99/100, TWTR 35/36; CELG only 11/83) | PIT S&P 500 members with a prior estimate: 70.4% (2008Q1), 71.8% (2008Q3), 80.0% (2012Q1), 96.4% (2018Q1), 98.4% (2025Q1). **Later-delisted members: 23.9%, 24.7%, 34.1%, 82.4%, 100%** | **NO**: EPS only; no vintage; the stored estimate is set by the vendor at the report (see below); roughly 3× survivorship skew before 2013 |
| EODHD forward daily analyst vintages (estate collector) | $0 on the existing subscription | 2026-07-31 | `retrieved_at` per capture (**true PIT**) | YES, forward only | 0 delisted (10 tickers, 6 captured daily) | 10 tickers | YES, but 6 weeks of history |
| Intrinio / Zacks trial cache | trial licence (research use) | consensus: one day (2026-08-10, 646 tickers); EPS surprises 2025-05+; sales surprises 2023-05+ | capture day; surprises at report | **NO** (manifests: "NOT historical point-in-time revision data") | some (ATVI present) | trial universe | **NO**: history 1-3 years |
| FMP (free key) `grades` | $0 | 2012-02-08 (AAPL: 1,799 rating changes, previous → new grade, firm) | event date | rating-change events only | **delisted SIVB → HTTP 402** | active names only | **NO**: survivorship-unsafe |
| FMP `analyst-estimates` / `price-target-news` | $0 / paid tier | current forward periods only / HTTP 402 | none | **NO** | n/a | n/a | **NO** |
| Alpha Vantage (free) `EARNINGS` / `EARNINGS_ESTIMATES` | $0 | IBM 1996-04 (122 quarters with `estimatedEPS`) / current snapshot with 7-90-day-ago and trailing revision counts | report date / request time | **NO** | **delisted SIVB → empty** | active names only | **NO** |
| Finnhub (free) | $0 | `upgrade-downgrade` 403; `eps-estimate` 403; `recommendation` 4 monthly rows; `earnings` 4 quarters | n/a | **NO** | n/a | n/a | **NO** |
| Norgate (owned) | existing subscription | API exposes prices, index constituents, corporate actions, classification, shares outstanding / float, dividend yield — **no estimate or revision function** | n/a | **NO** | n/a | n/a | n/a |
| SEC EDGAR (free) | $0 | actuals only (XBRL company facts, 8-K Item 2.02 acceptance) — no analyst expectations. The owned seasonal-difference surprise is already R58 `TESTED_NO_ALPHA_EVIDENCE` | acceptance time | n/a | yes | high | not an analyst field |

### The report-time consensus is not a frozen pre-report value

Two measurements on the owned captures:

1. **No rewrite was seen in a short window.** Across 13,227 comparisons of
   already-reported quarters between captures, `epsEstimate` changed 0 times.
   Six weeks and six large caps cannot establish point-in-time behaviour.
2. **The report-time value is set at the report, not before it.**
   * NVDA (report 2026-08-26): the stored 2.09 matches the last pre-report
     consensus of 2.0916.
   * XOM (report 2026-07-31): the capture on the report date held 3.80, the
     stored value afterwards is 3.68, and the consensus captured that same day
     was 3.6289.

   The vendor sets the "prior consensus" at the report, with no timestamp
   showing what was knowable before it.

## Field-by-field availability (best owned or free source)

| Field requested | Best owned/free source | Historical PIT state |
|---|---|---|
| EPS estimate revisions | EODHD `Trend` (7/30/60/90-day-ago, up/down counts) | current window only: **PIT_BLOCKED** |
| Revenue estimate revisions | EODHD `Trend` / AV `EARNINGS_ESTIMATES` | current only: **PIT_BLOCKED** |
| Target-price revisions | EODHD `TargetPrice` (one current number); FMP 402 | **PIT_BLOCKED** |
| Number of analysts | EODHD `Trend` counts (current); Zacks trial counts 2023+ | **PIT_BLOCKED** |
| Dispersion | EODHD `Trend` high/low (current); Zacks trial std. dev. 2023+ | **PIT_BLOCKED** |
| Upgrade / downgrade direction | FMP `grades`, dated 2012+ | dated, but active names only: **COVERAGE_BLOCKED** |
| Earnings surprise vs prior consensus | EODHD `calendar/earnings` 2004+ | EPS only, no vintage stamp, 24-34% coverage of later-delisted members before 2013: **PIT_UNVERIFIED + COVERAGE_BLOCKED** |

## Minimum data-purchase gap

**Exact missing fields.** For each security, fiscal period (FY1, FY2, next
quarter) and **as-of date**:

* consensus mean EPS
* consensus mean revenue
* number of estimates
* standard deviation (or high/low)

The as-of date must be the day the consensus was knowable, at no worse than
monthly resolution (daily preferred). Also needed:

* analyst-level estimate records (estimate date, previous estimate, broker id),
  or up/down revision counts over trailing windows stamped at each as-of date
* recommendation-change events with timestamp, broker, and previous/new rating

Target-price history is optional. Actuals and report timestamps are already
owned (SEC acceptance, EODHD report dates).

**Semantics.** A later revision must be a new record, never an overwrite
(R59 `no_restatement_backfill`). The vendor must state in writing that a record
reflects what a subscriber could have seen on its as-of date.

**Minimum history.** 2010-07-01 to the present: the canonical equity grid starts
2011-07-01 (`EQUITY_DISCOVERY_START`), plus one year of lookback for revision
changes. The R59 gate asks for ≥ 15 years, which is the same date.

**Universe.** The owned Norgate S&P 500 Current & Past PIT membership (1,897
panel rows, about 500 members per date), including delisted and acquired names.
Coverage must meet the frozen gate the 13F / 13D/G / 8-K families used: ≥ 95% of
members per decision date and ≤ 20% of dates below that. Delisted coverage must
not be materially below alive coverage (R32 condition 6). Identifiers must
resolve to the estate identity at ≥ 90% (R59).

**Expected effective sample.** Measured by the canonical scorer on this exact
grid (13D/G cells, `control_block_13dg.json`). A revision signal is dense, so
the sparse-event scaler repair does not bind.

| Horizon | Rows | Effective periods (floor 36) | Minimum detectable rank-IC increment |
|---|---|---|---|
| h = 21 | 59,989 | 115 | 0.0009-0.0013 |
| h = 63 | 59,224 | 37 (marginal) | 0.0017-0.0022 |

A family of 2 signals × 2 horizons (m = 4) fits `FAMILY_PRIMARY_MAX = 6`.

**Provider candidates.** Each would need a real sample first (R32 condition 7):

* **True as-of revision history with inactive names:** LSEG I/B/E/S (Detail and
  Summary History), S&P Capital IQ Estimates, FactSet Estimates.
* **Zacks via Nasdaq Data Link or Intrinio.** `ZACKS/EEH` + `ZACKS/EREV` exist,
  but the free key returns only a curated premium sample (Phase 12-A). The R59
  Steele sample is `SAMPLE_NOT_RECEIVED`, and the R44 request letter was
  prepared and not sent.
* **Estimize.** Timestamped crowd consensus; delisted coverage unverified.
* **Not candidates.** FMP premium (delisted names 402, no estimate vintages) and
  EODHD (no vintage product).

**Incremental cost.** No vendor quote is recorded anywhere in the estate. The
only figure is R44's own estimate: about **$10,000 in the first year** for
Steele / Intrinio / Zacks historical vintages (`NEED_SAMPLE`), which is not a
quote. The owned `alpha_agent.r63.sourcing.break_even` rule against the
recorded paper NAV of $97,496.72 (R63 `sourcing_economics.json`, 2026-09-09)
gives:

| Annual fee | Share of NAV | Required net incremental alpha, h = 21 | h = 63 | State |
|---|---|---|---|---|
| $10,000 | 10.26% | **12.48%/yr** | 11.88%/yr | **EXTREME_HURDLE** |
| $1,949.93 (the 2% line) | 2.00% | 3.40%/yr | 2.80%/yr | at the hurdle |

Any enterprise I/B/E/S, Capital IQ or FactSet licence is far above the 2% line
at this NAV.

**Why a purchase could plausibly create orthogonal alpha, and why that is not
enough.**

* **Mechanism, stated before any test.** Analysts revise gradually and prices
  underreact to the flow of revisions (the earnings-momentum literature). A
  revision is forward-looking consensus change, which neither price history nor
  filed fundamentals contain.
* **Estate evidence cuts against the orthogonality claim.**
  * Stage 13B's revision signals were near-orthogonal to the old champion
    (Spearman +0.04) but correlated +0.36 with 6-1 momentum.
  * The incumbent here is `fundamental_momentum_50_50_v1`, the family most
    likely to absorb a revision signal.
  * Stage 13C's out-of-sample replication failed (t −0.29).
  * The S&P 500 is where the anomaly is most arbitraged.

  Plausibility is low to moderate. It does not justify clearing a 12%/yr
  hurdle, and the purchase gate forbids buying without a sample.

**Recommendation: DO NOT BUY.** Paid candidates stay `WAITING_FOR_SAMPLE`.

The $0 route to genuine vintages is the one the estate already owns: extend the
EODHD forward vintage capture from 10 tickers to the PIT S&P 500. That is about
500 fundamentals calls × 10 credits = 5,000 of the 100,000 daily credits. It
would create true as-of history from its first day, and reach 36 effective
monthly periods at h = 21 about three years later. That is an operational change
and is **not** made here: no live deployment and no automation under this brief.
