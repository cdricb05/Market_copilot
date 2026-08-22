# Release 38 — Native Futures Information Frontier

- **Date:** 2026-08-22
- **Branch:** `stage19-controlled-rebalance` (base commit `b1a36a3`, the
  Release 37 / 37.1 closeout)
- **Owner package:** `alpha_agent/r38/` (11 modules)
- **Runner:** `scripts/run_release38_native_futures_information_frontier.py`
- **Regression:** `tests/test_release38_native_futures_information_frontier.py`
- **Audit guard:** `check_release38_native_futures_information_frontier`
  (27 required assertions)
- **Research root:** `D:\Stock_Prediction_app_data\native_futures_r38\`
- **Authoritative campaign:** `r38_native_futures_information_frontier_v4`
  (v1–v3 superseded, defects named, artifacts retained)

Release 37 answered *"is the futures dataset worth paying to investigate?"*
(`RESEARCH_ACQUISITION_RECOMMENDED`, canonical Stage A). The operator manually
purchased the **Norgate World Futures (Silver Package)** — 6-month term,
vendor-shown expiry **2027-02-22** — before this release began. Release 38
inherits that purchase as a FACT, spends nothing, changes no subscription, and
answers the five questions the purchase created: what was actually delivered;
which Release-36 blocked cells are ACTUALLY open; whether native
contract-level data carries robust predictive or economic Alpha the ETF/proxy
research could not see; whether the data earns its renewal; and what clean
foundation the modern-ML challenger (Release 39) should consume.

## The six results, never collapsed

| axis | result |
|---|---|
| SYSTEM_RESULT | **PASS** |
| DATA_ENTITLEMENT_RESULT | **SYNCHRONIZED** — delivered locally 2026-08-22 16:59:57 ET |
| DATA_CAPABILITY_RESULT | **105 markets, 23,805 dated contracts, 59 R36 cells native-verified vs 53 expected** |
| RESEARCH_CANDIDATE_RESULT | **FAIL** — 13/13 executed, 0 Benjamini-Hochberg survivors |
| ALPHA_RESULT | **FAIL** |
| POST_ACQUISITION_VALUE_RESULT | **RESEARCH_ONLY** (canonical Stage-B gate) |

**Terminal verdict: `R38_FRONTIER_MEASURED_NO_QUALIFIED_ALPHA`.**

A working pipeline is not Alpha; a statistically interesting result is not
economic Alpha; historical Alpha is not TRUE_FORWARD evidence; and a renewal
recommendation is not a renewal. `ALPHA_RESULT` may be `PASS` only alongside
`R38_NATIVE_FUTURES_ALPHA_QUALIFIED`, enforced in `campaign.build_verdict`,
not in prose.

## Phase 1 — the local entitlement, proven not assumed

**A website confirmation is not a synchronized database.** At 17:03 ET the
local Norgate Data Updater still served the pre-purchase baseline this estate
has measured three times: ONE futures market (`&ES`), eight databases, no
dated contracts. The NDU log shows the server-side subscription list excluded
futures through the 15:59 ET hourly check-in. **At 16:59:57 ET the hourly
update distributed the new `future` database** (`NduLog.20260822.txt`:
`price_future.pricehistory` / `pricecurrent` distributions, 143,495 Cobra
records checkpointed), and the next probe measured:

- `databases()` → nine, now including **`Futures`**;
- `futures_market_symbols()` → **105 markets** (was 1);
- 124 session symbols; **27,357** session-contract rows (23,805 distinct
  primary-session dated contracts);
- dated-contract enumeration returning real symbol lists for every session.

Both states — NOT_SYNCHRONIZED at 17:03, SYNCHRONIZED at 17:13 — are
persisted verbatim under `phase1_entitlement/` beside the NDU log extract, so
the sync timeline is evidence, not narrative.

### The `'&ES'` question, answered permanently

Release 37 observed `futures_market_session_contracts('&ES')` raise
`ValueError` and refused to classify it. Release 38 read the installed
client's source: the function calls the
`futuresmarketsession/<symbol>/sessioncontracts` endpoint, whose identifier
domain is the SESSION-symbol namespace (`futures_market_session_symbols()`),
and `&ES` is a symbol from the Continuous-Futures DATABASE namespace. The
call was a **PARAMETER_ERROR** — a caller defect, not an entitlement
measurement — and post-synchronization the same call answers ok-but-empty,
which classifies identically. Every provider call in this release is
classified through a frozen six-state taxonomy —
`VALID_REQUEST_WITH_DATA / PARAMETER_ERROR / ENTITLEMENT_ERROR /
EMPTY_HISTORY / UNSUPPORTED_MARKET / OTHER_PROVIDER_ERROR` — with
`A_PROGRAMMER_ERROR_IS_NOT_AN_ENTITLEMENT_LIMITATION = True` and a permanent
regression (`TestProviderCallTaxonomy`, eight cases). A valid session symbol
failing while the dated database is absent is `ENTITLEMENT_ERROR`; the same
failure with the database present is `OTHER_PROVIDER_ERROR`; the distinction
is measured, never guessed.

## Phase 2 — what was actually bought (delivered bytes, not the brochure)

`futures_market_registry.json` and `dated_contract_registry.json`, every
number a local API answer:

- **105 markets · 23,805 primary-session dated contracts** (27,357 distinct
  across day-session variants) · **15 exchanges**: CME 28, CBOT 16,
  ICE Europe 13, Eurex 10, NYMEX 9, ICE US 7, ASX 7, SGX 4, HKFE 3,
  Montreal 3, CBOE 1, EURONEXT 1, KCBT 1, KRX 1, MIAX 1.
- **Asset classes** (declared classification, completed from delivered vendor
  names/exchanges — metadata, never outcomes): COMMODITY 41, RATES 24,
  INTERNATIONAL_EQUITY 17, FX 9, US_EQUITY 9, CRYPTO 4, VOLATILITY 1.
- **History:** 26 markets first-quoted in the 1970s, 21 in the 1980s, 25 in
  the 1990s — CL from 1983, GC/SI/HG/HO from the 1970s-80s, SNK (SGX Nikkei)
  from 1990, FGBL from 1999, **VX from 2004** (the full Cboe curve Release 36
  recorded as BLOCKED_LICENSING).
- **Metadata coverage 105/105** on point value, tick size, margin, currency,
  exchange and lowest-ever tick; OHLC + Volume + Open Interest delivered on
  every probed contract; first-notice dates present on physically delivered
  markets.
- **Activity:** 105/105 markets ACTIVE (judged on the freshest bar among the
  next three undelivered months — two earlier heuristics mislabelled Henry
  Hub and Brent and were superseded as campaigns v2/v3).
- **Survivorship, stated honestly:** each market's own contract series is
  complete including every expired contract back to inception, but the
  MARKET list is current-composition — terminated markets (pork bellies,
  propane) are absent from the delivered package. Cross-market universes
  carry that caveat in every artifact that builds one.

## Phase 3 — the delivered data validated, structurally

30 markets across every mandated slot (energy, metals, grains, softs,
livestock, US and international rates, FX, US and international equity index,
volatility), 4 contracts each — old expired, mid-history expired, recently
expired, current front — **120 contracts inspected: 30 PASS, 0 WATCH, 0
FAIL.** Checks: OHLC presence and internal consistency, NaN/non-positive
closes, duplicate dates, index monotonicity, calendar gaps, discontinuities
(|log return| > 0.5), volume/open-interest coverage, expiry identity (an
expired contract's final bar may not postdate its scheduled last trading
day), currency, first notice. Checksums persisted per series. No Alpha
conclusion is drawn from a validation sample
(`ALPHA_CONCLUSIONS_IN_THIS_MODULE = False`).

## Phase 4 — ONE dated-contract research representation

`alpha_agent/r38/research_layer.py` builds one daily series per market from
DATED contracts under the ONE frozen observable roll policy:

- exit the held contract at the EARLIER of (first notice − 2 business days)
  and (scheduled last quoted day − 5 business days) — both exchange-published
  schedule metadata, observable ex ante; cash-settled markets use the
  last-quoted leg;
- the switch happens after the exit day's settlement, and the first return on
  the new contract is computed against ITS OWN prior settlement — no roll
  jump is ever booked as PnL;
- no hindsight roll, no roll-rule search (`NO_ROLL_RULE_SEARCH = True`), and
  no vendor continuous series in any economic path
  (`vendor_continuous_series_used: false` in the layer manifest).

68 experiment-relevant markets built, checksummed CSVs carrying the daily
front-roll excess return, the held-contract identity per date (the explicit
roll record), the second-contract return, the annualised calendar slope
`log(F1/F2)`, volume and open interest. Sanity anchors: long WTI front-roll
+10.1 %/yr at 38 % vol since 1983 across 521 held contracts; the VX front
roll **−40.8 %/yr** since 2004 (the textbook volatility roll-down, measured
natively for the first time in this estate); Bund +2.1 %/yr at 5.8 % vol.

## Phase 5 — expected unlocks vs DELIVERED unlocks

Release 37's ~53 cells were an EXPECTATION
(`EXPECTED_UNLOCKS_ARE_NOT_MEASURED_UNLOCKS`), and Release 38 replaced them
with measurement, cell by cell, across all 95 R36 blocked cells
(`r37_expected_vs_r38_actual_unlocks.json`):

| measured status | cells |
|---|---|
| **NATIVE_DATA_VERIFIED_RESEARCHABLE** | **59** |
| PARTIALLY_UNLOCKED | 6 |
| STILL_BLOCKED_ENTITLEMENT | 27 |
| STILL_BLOCKED_HISTORY | 3 |

**R37 expected 53 full unlocks; R38 measured 59 — 48 confirmed, 5
downgraded, 11 opened beyond expectation.** Truth ran in both directions:

- **Downgrades (5).** `CMDTY_GRAINS::FUNDAMENTAL_SUPPLY_DEMAND` and
  `INTL_EQUITY_DEVELOPED::VALUE`: the futures package delivers PRICES, and
  those strategies also need an information leg (USDA physical supply/demand;
  issuer fundamentals) this estate does not own — the price leg alone is
  PARTIAL, however good it is. `CMDTY_INDUSTRIAL::CROSS_SECTIONAL` and
  `::RELATIVE_VALUE`: one industrial metal (COMEX copper) was delivered, and
  one market is not a cross-section. `RATES_TREASURY_FUTURES::POSITIONING`
  fell to the COT-mapping gap on non-US listings.
- **Beyond expectation (11).** `RATES_INTERNATIONAL` (expected PARTIAL)
  arrived ELEVEN markets deep — the Eurex Bund/Bobl/Schatz/Buxl complex,
  BTP, OAT, JGB, Canada, and the ASX 3y/10y — all with ≥ 10 years of
  history: five cells fully verified. `INTL_EQUITY_EMERGING` (expected
  PARTIAL) delivered MSCI Taiwan (HKFE, from 1997) and FTSE China A50 (SGX):
  six cells fully verified.
- **Still honest zeros.** The three `CRYPTO_BASIS_FUNDING` cells stay blocked
  on HISTORY: CME Bitcoin futures begin in 2017 and fail the 10-year floor.
  The 27 `STILL_BLOCKED_ENTITLEMENT` cells (credit, NDF, FX options,
  inflation swaps, analyst estimates, the IV surface, crypto cross-section)
  belong to families this package never claimed to touch.

`updated_global_multi_asset_coverage.json` records this as an **overlay** on
the frozen Release-36 matrix — `alpha_agent.r36.coverage` remains the ONE
coverage authority and is never recomputed here.

## Phases 6–14 — the frozen native campaign

**Frozen before any outcome was viewed:** 13 primary configurations
(`FROZEN_PRIMARY_CONFIGURATIONS`, ceiling 20), each with a declared universe
rule over delivered metadata, a binding signal definition, a cadence
(monthly 21 sessions; VX weekly 5) and a lane-correct control. No optimizer,
no grid, no result-driven expansion; every executed configuration enters the
Benjamini-Hochberg denominator (q = 0.10, `alpha_agent.r31.multiple_testing`)
and a configuration that cannot execute is recorded with its reason, never
removed. Signals: cross-sectional 12-1 momentum, curve carry
(annualised `log(F1/F2)` — for FX futures this IS the covered-interest-parity
rate differential Release 36 measured from OECD rates), time-series trend,
seasonality (prior-years same-calendar-month mean), CFTC commercial
hedging-pressure z (Release 35's deacot archive and publication lag, with a
declared root→code mapping), calendar-spread mean reversion, VX
term-structure carry and slope reversion, Treasury curve carry, and
international index momentum/trend in LOCAL currency.

Costs are charged on TRADED NOTIONAL (`alpha_agent.r34.economics`), per-side
bps by declared group — energy/precious 5, grains/industrial 8,
softs/livestock 10, Treasuries 2, international rates and intl index 3–5,
FX and US index 3, VX 15, crypto 25 — all labelled
`MODELLED_NOT_OBSERVED`, with a 2× stress arm. Controls: the
volatility-matched passive equal-weight front-roll basket of each
configuration's own universe (same dates, same roll, same costs), or
risk-matched cash for the long/short spread and conditional books. Micro
contracts, second listings of one underlying (ICE WTI vs NYMEX WTI, CME
Nikkei vs the 1990-history SGX listing) and the GSCI index future are
excluded from cross-sections by declared rule.

### Results — 13 of 13 executed, 0 multiple-testing survivors

**Verdict: `R38_FRONTIER_MEASURED_NO_QUALIFIED_ALPHA`.
RESEARCH_CANDIDATE_RESULT = FAIL. ALPHA_RESULT = FAIL.** Every configuration
executed (none fell to a universe floor or data gap); Benjamini-Hochberg at
q = 0.10 over all 13 two-sided p-values rejected **nothing** — in either
direction.

| configuration | periods (from) | net %/yr | Sharpe | after-cost excess vs control %/yr | t | class |
|---|---|---|---|---|---|---|
| CMDTY_TS_TREND_12M | 586 (1978) | +5.43 | 0.79 | **+3.85** | **+2.48** | near miss |
| VX_TERM_STRUCTURE_CARRY | 1114 wk (2004) | +5.69 | 0.53 | **+6.24** | **+2.28** | near miss |
| CMDTY_XS_CARRY | 594 (1978) | +4.56 | 0.60 | +2.80 | +1.60 | indistinguishable |
| FX_FUT_CARRY_IMPLEMENTATION | 563 (1979) | +2.95 | 0.52 | +2.44 | +1.79 | indistinguishable |
| CMDTY_XS_MOMENTUM_12_1 | 584 (1979) | +3.78 | 0.43 | +1.90 | +0.91 | indistinguishable |
| RATES_TS_TREND | 568 (1979) | +2.21 | 0.35 | +0.56 | +0.38 | indistinguishable |
| RATES_XS_CURVE_CARRY | 457 (1988) | +0.38 | 0.16 | +0.41 | +0.96 | indistinguishable |
| CMDTY_SEASONALITY | 536 (1983) | +2.19 | 0.27 | +0.24 | +0.11 | indistinguishable |
| CMDTY_CALENDAR_SPREAD_MR | 588 (1978) | −0.08 | −0.07 | −0.07 | −0.50 | indistinguishable |
| CMDTY_COT_HEDGING_PRESSURE | 461 (1988) | −1.37 | −0.14 | −3.16 | −1.70 | indistinguishable |
| VX_CALENDAR_SLOPE_MR | 1065 wk (2005) | −6.91 | −0.29 | −5.41 | −1.41 | indistinguishable |
| INTL_IDX_XS_MOMENTUM | 443 (1990) | −0.23 | +0.01 | −3.29 | −1.82 | indistinguishable |
| INTL_IDX_TS_TREND | 524 (1984) | −0.49 | +0.05 | −5.90 | −1.87 | indistinguishable |

**The strongest predictive discoveries are unambiguous.** Cross-sectional
commodity carry predicts at **rank IC +0.057, t = 6.01** over 594 monthly
dates; the FX-futures carry implementation predicts at **rank IC +0.148,
t = 6.29** — independently reproducing Release 36's forward-derived carry
finding (IC +0.155, t = 7.97) from a completely different instrument set via
covered interest parity, which is exactly what a real signal should do.
Commodity momentum (IC +0.053, t = 4.62) and seasonality (IC +0.049,
t = 4.02) also predict; VX term-structure carry calls the direction of the
front contract 61.2 % of 1,107 weeks.

**And prediction did not convert past the pre-registered bar — again.** The
two candidates that cleared t ≥ 2.0 individually died on the multiple-testing
step-up (p = 0.0133 and 0.0224 against BH cut-offs of 0.0077/0.0154 at
m = 13): with thirteen frozen tests, neither is distinguishable from the
luckiest of thirteen tries. Both are otherwise robust — same sign in halves
AND thirds, and the trend book survives dropping ANY of its 39 markets
(0 sign flips) with 32 %/yr turnover costing 31 bps. The frozen denominator
was not widened to save them (`NO_RESULT_DRIVEN_EXPANSION`), and no
neighbouring parameter was added. That is the discipline, not a shortfall:
the estate has now watched a t≈2.3 near-miss evaporate out-of-sample once
(Stage 13B → 13C) and priced the lesson.

Notable honest losers: COT hedging pressure is **negative** after costs
(−3.16 %/yr) — the textbook premium did not survive this design;
international index momentum and trend LOSE to their own passive baskets
(−3.3/−5.9 %/yr), repeating Release 36's finding that timing equity indices
underperforms holding them; and the VX calendar-spread book pays 4.2 %/yr in
modelled spread costs — the curve's mean reversion exists and execution eats
it. Every failure carries its minimum detectable effect (trend's is
3.11 %/yr, VX carry's 5.47 %/yr) so "not significant" comes with the size the
design could have found.

## Phase 15 — the canonical POST_ACQUISITION_VALUE gate

**POST_ACQUISITION_VALUE_RESULT: `RESEARCH_ONLY`** — *"usable & distinct but
no proven purchase-grade lift"*, reason code
`NO_MATERIAL_LIFT_BUT_DISTINCT_INFORMATION`.

The MEASURED evidence went into the ONE canonical gate
(`engine.data_expansion_gate` via `api.data_expansion`, Stage B): 105
delivered markets, ~56 years of history, daily official settlements with
1-day lag, STRONG identifiers, HIGH operational reliability (the entitlement
synchronized itself through the vendor's hourly cycle on purchase day), LOW
survivorship risk with the market-composition caveat declared, USD 540/yr
annualised cost — and, decisively, a lift field fed ONLY from
multiple-testing survivors, of which there are none. One dimension failed
(`measured_research_lift`), licensing sits at WATCH (single-workstation
redistribution limits), `incremental_information` is UNKNOWN pending a
measured correlation against the owned book. The result is recorded verbatim
in `post_acquisition_data_gate_result.json`, is NOT persisted to the Slice-9
store, and grants nothing: `purchase_authorised: False`,
`renewal_authorised: False`.

**What RESEARCH_ONLY means for the renewal decision (operator's, due by
2027-02-22):** the capability value is real and measured — 59 R36 cells
opened, two 45-year-plus asset-class histories this estate never had, a
decisive closure of several long-standing hypotheses, and the ML foundation
for Release 39 — while the ALPHA value remains unproven at the purchase-grade
bar. Renewal is not justified by Release 38's Alpha evidence alone; it may be
justified by what Release 39 does with the foundation before the term ends.
That decision belongs to the operator, with six months of runway to take it.

## Phase 16 — the ML-ready native foundation

`ml_ready_native_futures_panel.csv` — **31,175 decision-stamped rows across
68 markets** (9.7 MB, sha256 recorded), one row per (market, monthly
decision): forward target realised strictly after the decision, trailing
1/3/12-month returns, 63-session volatility, the calendar-slope carry,
volume, open interest, the raw COT commercial z where mapped, missingness
MASKS (never silent fills), per-row modelled cost, an equal-weight basket
control return per date, market activity and PIT states, and a chronological
TRAIN/VALIDATION/TEST partition (60/20/20) separated by explicit EMBARGO
rows. Full OHLC/settlement sequences stay in the checksummed per-market layer
CSVs, referenced by hash. `TRAINS_A_MODEL = False`: nothing was fitted, no
library was installed, and Release 39 inherits data engineering, not
conclusions.

## Phase 17 — market structure readiness (verified, not launched)

The Release-37 market-structure/Fibonacci backlog is preserved and NOT
launched. The delivered data supports it when its time comes: every probed
contract carries daily OHLC (pivots, ATR geometry, retracements, chart-image
rendering), volume and open interest, with the confirmation-dating and
placebo-level rules already designed in `alpha_agent/r37/market_structure.py`.

## Parallel track — Intrinio / Steele Barcomb

The historical-analyst-revisions lane continues in parallel and never blocked
this release. No frozen five-ticker set existed in project evidence, so
`intrinio_steele_sample_request.json` proposes a deliberately informative
five: **AAPL** (dense coverage, September FYE), **MON** (delisted 2018 —
archive completeness for inactive names), **META** (FB ticker/name change —
identifier continuity), **HTZ** (bankruptcy, OTC period, re-listing),
**CALM** (sparse small-cap coverage, May/June fiscal year end). The
operator-ready message (`intrinio_steele_sample_request_message.md`) asks
Steele for monthly 2004–2024 observations proving true as-of dates, EPS and
revenue consensus, mean/median/high/low, analyst count, dispersion,
revision-up/down counts, fiscal-period identity, subsequent actuals,
identifier continuity and delisted handling. The sample is
`SCHEMA_AND_PIT_VALIDATION_ONLY` — five tickers can validate a schema and can
never validate Alpha — and nothing is purchased, trialled or licensed.

## Ownership map

===========================  =================================================
concern                      owner
===========================  =================================================
campaign contract            `alpha_agent/r38/contract.py`
entitlement proof + taxonomy `alpha_agent/r38/entitlement.py`
market/contract enumeration  `alpha_agent/r38/enumeration.py`
data quality                 `alpha_agent/r38/quality.py`
dated-contract layer         `alpha_agent/r38/research_layer.py`
R36 unlock recomputation     `alpha_agent/r38/unlock_actual.py`
frozen experiments           `alpha_agent/r38/experiments.py`
ML-ready contract            `alpha_agent/r38/ml_contract.py`
Steele sample request        `alpha_agent/r38/steele.py`
orchestration + verdict      `alpha_agent/r38/campaign.py`
===========================  =================================================

**Release 38 defines NO acquisition gate and NO coverage authority.** The
renewal/value question is answered by `engine.data_expansion_gate` through
`api.data_expansion` in its `POST_ACQUISITION_VALUE` context; the frozen R36
matrix is read through `alpha_agent.r36.coverage`; the R37 expectation
through `alpha_agent.r37.unlock`. `alpha_agent/r38/purchase_gate.py` and
`alpha_agent/r38/coverage.py` are forbidden by the architecture audit.
Reused, never rebuilt: r31 hashing and multiple testing, r34 economics,
r35 COT parsing and publication lags, r36 qualification conditions and MDE.

## Superseded campaigns (defects named, artifacts retained)

- **v1 — classification map incomplete.** 34 of 105 DELIVERED markets
  enumerated UNCLASSIFIED because the pre-registered map covered only
  anticipated symbols. Completed from delivered vendor names/exchanges —
  metadata, not outcomes.
- **v2 — activity judged on the newest LISTED contract.** A strip-quoted
  market's furthest month can trade once and go quiet; Henry Hub was recorded
  INACTIVE on that defect.
- **v3 — the calendar-front contract can expire before its delivery month.**
  Brent ceases trading ~2 months before delivery and was recorded INACTIVE.
  v4 judges on the freshest bar among the next three undelivered months.

## Safety

MONEY_SPENT_DURING_R38 **$0.00** · NEW_SUBSCRIPTIONS 0 ·
SUBSCRIPTION_CHANGES 0 · TRIALS_STARTED 0 · NEW_ACCOUNTS 0 ·
CLOUD_COMPUTE_SPEND $0.00 · OPERATIONAL_WRITES 0 (proven by
`scripts/r33_operational_write_attribution.py --release R38` → `ATTRIBUTED`,
0 findings, 14 sources scanned) · PORTFOLIO_MUTATIONS 0 · MODEL_PROMOTIONS 0
· PRODUCTION_RESTARTS 0. The Norgate package stays pinned at 1.0.74
(`MAY_UPGRADE_NORGATE_PACKAGES = False`). Every gate state carries
`purchase_authorised: False` and `renewal_authorised: False`; renewal is the
operator's decision alone, due before **2027-02-22**.
