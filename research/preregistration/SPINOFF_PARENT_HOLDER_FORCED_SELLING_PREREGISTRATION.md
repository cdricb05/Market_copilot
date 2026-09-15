# PREREGISTRATION - SPIN-OFF PARENT-HOLDER FORCED SELLING

**Mechanism id:** `SPINOFF_PARENT_HOLDER_FORCED_SELLING_V1`
**Global candidate:** `GC_USEQ_SPINOFF_FORCED_SELLING` (declared with this mechanism)
**Selected because:** the run brief requires a distinct structural forced-flow mechanism after the Russell
reconstitution NO_EDGE (`92ddda8`). Every index family is closed or forbidden as a rescue of that verdict, and
the two Norgate-native forced-flow designs fail identification (0.2). A spin-off is a mechanically induced
corporate action: newly created shares are handed to holders who never chose them. No spin-off hypothesis exists
in ResearchMemory or the repository.
**Catalog:** `research/alpha_agent/MECHANISM_FRONTIER.json` (data_version 1)
**Run:** ALPHA_COMPOSITE_STRIKE_SEP15_V1, verdict 3 contract, branch `alpha-composite-strike-sep15-v1`.
**Status:** FROZEN. Written and committed BEFORE any spinco return, book, hedge return, control or t-statistic was
computed on real data. Both identity censuses (1.5, 1.6) read only EDGAR filing text, EDGAR names, Norgate
symbols, Norgate names and quote DATES. Not edited after results exist.

---

## 0. THE P&L MECHANISM

When a parent distributes a subsidiary pro rata, its holders receive shares they never chose. Three groups must
sell whatever the price, in the first weeks of regular-way trading:

* index funds whose benchmark excludes the spinco;
* income funds, when the spinco pays no dividend;
* managers whose size, sector or style mandate rules it out.

The spinco has no trading history, no analyst coverage and no natural holder base. The selling depresses it below
value, and it recovers once the forced sellers are done (Cusatis, Miles and Woolridge 1993; McConnell and
Ovtchinnikov 2004; Abarbanell, Bushee and Raedy 2003).

**The P&L:** a slot book. Each linked spin-off registrant is bought at the open of its 21st quoted session, after
the when-issued market and the first weeks of forced selling. It is held 126 sessions in a 2 % NAV slot, hedged
one-for-one with the Russell 3000 total-return index.

**Frozen direction:** LONG spinco, SHORT index. A contradicted sign closes the mechanism; it is never reversed.

### 0.1 Prior estate tests nearby

| record | object | why this is different |
|---|---|---|
| `SP500_INDEX_ADDITION_DELETION` (S14-B1, S14-B2), `INDEX_MEMBERSHIP_FORCED_FLOW_V1` (NO_EDGE, 2026-09-15) | membership changes of existing index constituents | this trades a NEWLY CREATED security, distributed by a corporate action and identified from its own Form 10 registration. All three are inherited at p = 1 |
| `MERGER_ARBITRAGE_CASH_DEAL_TARGET_SPREAD_V1` | the completion spread of announced cash acquisitions | no distribution of new shares |
| `EVENT_8K_ITEM_CODES` | 8-K items as return signals | no forced holder |

### 0.2 Why not a Norgate-native forced-flow design (census 2026-09-15, flags and dates only)

* **Spin-off events.** `capital_event_timeseries` is empty around IBM/Kyndryl (2021), GE/GE Vernova (2024),
  3M/Solventum (2024) and Danaher/Veralto (2023). A spinco's `first_quoted_date` is its when-issued start
  (KD 2021-10-22, GEV 2024-03-27, SOLV 2024-03-26, VLTO 2023-09-27).
* **De-SPAC arbitrage exit.** The `Blank Check Company` flag switches off only 168 times in 1991-2026 across 41,918
  securities, 43 of them in 2021, when hundreds of SPAC mergers closed. It is never set for DraftKings, SoFi or
  Lucid, whose SPAC quotes Norgate carries under the final symbol.
* **Exchange delisting.** `Major Exchange Listed` reads 0 for Nasdaq names such as AAPL and MSFT, so it does not
  separate an exchange listing from OTC. Its 1,971 off-switches with later quotes are dominated by bankruptcy
  (Q-suffix) securities.

---

## 1. DATA - FREE AND OWNED, $0

* **EDGAR quarterly full-index**, `master.zip` for 1996Q1-2026Q3: 123 quarters, one re-fetched after a transient
  failure. Only `10-12B` and `10-12B/A` rows are kept: 2,539 filings by 776 registrant CIKs.
* **Filing reads.** The first 256 KB of every filing, plus a 2 MB deep read of every filing of a distribution
  registrant whose reads state no listing symbol (292 deep reads).
* **EDGAR `data.sec.gov/submissions` record** of every registrant (776 of 776): current and former names, used for
  identity only.
* All EDGAR data comes through the canonical SEC client (`alpha_agent.r63.acquire`, declared user agent, 0.13 s rate
  gate), called by `alpha_agent.alpha_recovery.spinoff_data`.
* **Norgate US Equities and US Equities Delisted** (41,936 securities): symbols, security names, first quoted dates,
  and `TOTALRETURN` opens and closes of linked securities.
* **Norgate US Indices:** `$RUATR` (hedge), `$RUTTR` and `$SPXTR` (controls). The `$SPXTR` session dates are the
  market calendar.

### 1.1 Distribution registrant (from the filing's own words)

A registrant qualifies when both conditions below hold. Each filing is read over its first 120,000 flattened
characters.

**At least one of its filings states a distribution**, as one of:

* a pro rata distribution or basis;
* a distribution of all (or N %) of the outstanding shares;
* a record date for the distribution;
* a distribution date or ratio;
* the Form 10's own defined terms: `(the "Distribution")`, `"The Spin-Off"`, `"The Distribution"`,
  `"The Separation and Distribution"`.

**None of its filings states an exclusion:**

* emergence from Chapter 11 or bankruptcy;
* a plan of reorganisation under Chapter 11, or a confirmed plan of reorganisation;
* a business development company;
* a blank check company.

### 1.2 Listing symbols

A symbol counts when it is stated as `under the (ticker|trading) symbol "XXX"` and both of these precede it:

* an exchange venue (NYSE, Nasdaq, AMEX, NYSE American and their names), within 260 characters;
* a listing verb (list, listed, listing, trade, traded, trading, quoted), within 200 characters.

An exchange parenthesis (`(NYSE: XXX)`) also counts. Over-the-counter statements are excluded.

### 1.3 Link to Norgate - identity only (symbols, names and dates)

The registrant's names are every EDGAR name it has had: the index conformed names plus the current and former
names of its submissions record. The window is [first filing date, last filing date + 365 days].

* **`SYMBOL`**: exactly one security that meets all three conditions:
  * its base symbol (before any `-YYYYMM` delisting suffix) is a stated listing symbol;
  * its first quote is inside the window;
  * its Norgate name shares a distinctive token with ANY of the registrant's EDGAR names.

  A symbol stated for a parent or a sibling spin-off does not link on its own. Wyndham's filing, for example,
  states Realogy's "H".
* **`NAME`**: when no symbol link exists, exactly one security first quoted inside the window whose Norgate name
  carries the first distinctive token of the registrant's current EDGAR name or of its index name. Norgate names a
  security by its LAST name and ticker, so a spinco renamed before delisting links through its later EDGAR name
  (Imation -> GlassBridge).
* **Unresolved:** `SYMBOL_AMBIGUOUS`, `NAME_AMBIGUOUS`, `UNCONFIRMED_SYMBOL`, `NO_NAME_KEY`.
* **No new security in the window:** `NO_QUOTE_IN_WINDOW`.

### 1.4 Point in time

* The distribution classification must be stated in a filing dated on or before the entry session; otherwise the
  event is `NOT_PUBLIC_BY_ENTRY`.
* The link resolves identity, not a signal.
* The first quote is observable on its date.
* Entry is the total-return open of the 21st quoted session.
* The hedge accrues from the index close before the entry session.

### 1.5 Calibration before returns - two disclosed restatements, then frozen

**Round 1** (first identity census, no return) found three parser defects:

* **Exclusions too broad.** A bare `plan of reorganization` excluded real spin-offs: it is the Section 368 tax-free
  boilerplate (Dow, Embarq, Certegy, Washington Prime). A bare `blank check` did the same, because it matches blank
  check preferred stock (Hanesbrands, Viasys). Together they excluded 187 distribution registrants.
* **Distributions missed.** Filings that describe the spin-off only through the Form 10's defined terms were not
  classified (Norlight, Teton, SunGard Availability, Avalon).
* **Symbol-only linking failed.** It could not link renamed spincos, and it linked parents or siblings from the same
  filing (Wyndham -> Realogy, New Ceridian -> Arbitron, John Bean -> TechnipFMC). Symbol-link coverage was 52 %.

Rules 1.1 and 1.3 are the repair. Distribution registrants rose from 303 to 495.

**Round 2** (second identity census, no return) found raw coverage (linked / distribution registrants) of 75.6 %.
Of the 121 unlinked registrants, 90 have NO new security in their window. Their classification is mechanical, from
cached symbols and quote dates:

* **77 continuing older security.** A symbol or name candidate was first quoted BEFORE the window. These are
  mostly reverse spins that keep the old listing's history (new Ralcorp, W R Grace, New ManorCare, Flowers Foods,
  Aetna, Gaylord), plus abandoned plans and existing companies (Westinghouse Electric, Earthgrains 2001, Altair).
* **9 absent from Norgate.** Never listed, or tiny registrants: CITGO, ValueRx, PrimeCare, Kinbasha, FlexFridge,
  CEN Biotech, Probation Tracker, ReElement, Vylor.
* **4 only a later security.** New D&B/Moody's, Folgers, NYTEX, Archeo.

Coverage over all distribution registrants therefore measures which plans produced a new security, not whether the
linker resolves identity. Before any return, the 85 % data-gate floor was restated to measure **identity
resolution = linked / (linked + unresolved)**. Its value, 0.85, was set before the first census and has not moved;
the 150/100 event floors have not moved either. Raw coverage is reported beside it.

This is the second and last restatement. Nothing about classification, linking or gates changes after this
commit.

**Survivorship caveat.** The continuing-older-security class is assigned mechanically. A real spinco missing from
Norgate whose symbol or name also belongs to an older security is counted there, e.g. Cognizant Corp 1996 ->
Nielsen Media Research, or Providian Bancorp 1997. Such gaps sit before 2000, inside the qualification window. They
remove spincos that later delisted, so they bias the book toward survivors.

### 1.6 Census before returns (round 2, frozen inputs)

| quantity | value |
|---|---|
| Form 10-12B filings / registrant CIKs / parsed / deep reads | 2,539 / 776 / 100 % / 292 |
| submissions records (distribution registrants) | 495 of 495 (100 %) |
| distribution registrants | 495; a further 10 carry distribution AND exclusion language and are excluded |
| linked | 374: `SYMBOL` 258, `NAME` 116 |
| unresolved | 31: `NAME_AMBIGUOUS` 24, `UNCONFIRMED_SYMBOL` 4, `SYMBOL_AMBIGUOUS` 2, `NO_NAME_KEY` 1 |
| no new security in window | 90: continuing older 77, absent 9, only later 4 |
| **identity resolution** | **374 / 405 = 92.3 %** (floor 85 %) |
| raw coverage | 374 / 495 = 75.6 % (reported, not gated) |
| point-in-time events (public by entry) | 374; full 126-session holds 97.3 % |
| QUALIFICATION entries 1996-2012 | 168 (floor 150): 1996-2004 98, 2005-2012 70; `SYMBOL` 107, `NAME` 61 |
| CONFIRMATION entries 2013-2026 | 206 (floor 100): `SYMBOL` 151, `NAME` 55 |
| identity audit | 60 random events read by hand. 1 wrong link: U S Bioscience Inc -> PURE Bioscience, a `NAME` link on the shared token BIOSCIENCE (1.7 %). Kept, because the rule is frozen and never edited per event. |

Entries by year:

| 1996 | 1997 | 1998 | 1999 | 2000 | 2001 | 2002 | 2003 | 2004 | 2005 | 2006 | 2007 | 2008 | 2009 | 2010 | 2011 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10 | 21 | 12 | 12 | 13 | 11 | 8 | 4 | 7 | 4 | 7 | 10 | 18 | 8 | 4 | 9 |

| 2012 | 2013 | 2014 | 2015 | 2016 | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | 2023 | 2024 | 2025 | 2026 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 10 | 14 | 26 | 29 | 21 | 11 | 15 | 9 | 10 | 13 | 14 | 16 | 11 | 6 | 11 |

---

## 2. THE ONE FROZEN CELL (m_declared = 1)

```
event e          = a linked distribution registrant whose classification is public by entry (1.1-1.4)
entry(e)         = the 21st quoted session of its Norgate security;  exit(e) = its 146th quoted session (or its last quote)
G_e(t)           = spinco growth from the entry open: open-to-close on the entry session, close-to-close on later quoted
                   sessions, flat on unquoted sessions
H_e(t)           = $RUATR(t) / $RUATR(session before entry)
slot P&L(t)      = 0.02 x [ (G_e(t) - H_e(t)) - (G_e(t-1) - H_e(t-1)) ],  G = H = 1 before entry
costs            = 0.02 x (c + 1 bp) at entry;  0.02 x (c x G_e(exit) + 1 bp x H_e(exit)) at exit
NAV P&L(t)       = sum over active slots (NAV fixed at 1; unused capital earns the bill, zero excess)
slots            = at most 50 active at once; an event arriving with all 50 taken is skipped
```

No second cell exists or will be created, before or after results:

* no other entry session or hold;
* no unhedged or differently hedged book;
* no parent-side book;
* no subset by size, index membership, link state, dividend or sector;
* no value weights;
* no other cost rate.

## 3. POINT IN TIME

See 1.4. A delisted spinco stays in the book until its last quote; nothing is filled after it.

## 4. WINDOWS

* **QUALIFICATION:** events with entry sessions 1996-01-01..2012-12-31. Halves by entry: 1996-2004 and 2005-2012.
* **CONFIRMATION (untouched):** entries 2013-01-01..2026-12-31. A holding still open at the data end runs to the last
  available close. READ ONLY IF every qualification gate passes.

## 5. COSTS

* Spinco: 25 bp per side on the slot notional, at entry and at exit. This is the primary rate, the small and
  mid-cap rate.
* Hedge: 1 bp per side.
* Ladder: 0 / 12.5 / 25 / 50 bp. Stress rung: 50 bp.

## 6. MEASUREMENT - existing owners only

* **Series.** The daily NAV P&L runs from the first to the last active session of the window's events.
* **Scale and inference.** Annualisation uses 252 sessions. t and one-sided p come from
  `alpha_agent.r63.sensitivity.nw_tstat(x, 10)`; maximum drawdown from `S._max_dd`.
* **Increment.** OLS of the daily P&L on the `$RUTTR` and `$SPXTR` returns: `a = pnl - b1 x RUTTR - b2 x SPXTR`.
  t comes from `nw_tstat(a, 10)`; annualised alpha is `252 x mean(a)`.
* **Multiplicity.** `bh_fdr` with q = 0.10, one-sided p and m = 4. S14-B1, S14-B2 and INDEX_MEMBERSHIP_FORCED_FLOW_V1
  enter at p = 1.
* **Diagnostics** (never gated, never a rescue): mean event net excess, event-level t, hit rate, active slots,
  events skipped, and link states of the events.

## 7. GATES - in this order

1. **Data -> `DATA_HOLD`** if any of:
   * an index quarter is missing;
   * the parsed share is below 99 %;
   * submissions records cover fewer than 99 % of distribution registrants;
   * identity resolution, linked / (linked + `SYMBOL_AMBIGUOUS` + `NAME_AMBIGUOUS` + `UNCONFIRMED_SYMBOL` +
     `NO_NAME_KEY`), is below 85 %;
   * a price fails to load;
   * there are fewer than 150 qualification or 100 confirmation events;
   * the hedge is finite on fewer than 99 % of sessions.
2. **Frozen sign.** Annualised net at 25 bp <= 0 -> `WRONG_SIGN`.
3. **Standalone.** NW t < 2.0 -> `STANDALONE_T`.
4. **Materiality.** Annualised net < 1.5 %/yr -> `MATERIALITY`.
5. **Increment.** t < 2.0 or annualised alpha < 1.5 %/yr -> `INCREMENT`.
6. **Stability.** Annualised net <= 0 in either half -> `STABILITY`.
7. **Drawdown.** Maximum drawdown worse than -20 % -> `DRAWDOWN`.
8. **Multiplicity.** Fails BH q = 0.10 at m = 4 -> `MULTIPLICITY`.
9. **Cost survivability.** Annualised net at 50 bp < 1.5 %/yr -> `COST`.
10. **Untouched confirmation.** Annualised net <= 0, NW t < 2.0 or annualised net < 1.5 %/yr at 25 bp ->
    `CONFIRMATION`.
11. Otherwise `QUALIFIED`, which raises a HUMAN gate (prospective registration, contract rule 16).

Verdicts:

* gate 1 -> `DATA_HOLD`;
* gates 2-10 -> `NO_EDGE`, with the fired gate recorded;
* gate 11 -> `QUALIFIED`.

**CAPITAL ELIGIBLE = NO** in every case.

## 8. FALSIFICATION - and what will NOT be done

A null means: spincos bought after their first month of trading, identified from filings public at the time, earned
no after-cost return over the Russell 3000 that survives the Russell 2000 and S&P 500 controls, on 1996-2012 or on
the untouched 2013-2026 entries.

**Forbidden after results:**

* reversing the sign;
* another entry session (1st, 11th, 63rd) or hold (21, 63, 252 sessions);
* removing or changing the hedge;
* a parent-side book;
* subsets by size, index membership, dividend, sector or link state, including dropping `NAME` links or the one
  audited wrong link;
* value weights;
* a lower cost rate;
* excluding 2000-2002, 2008 or 2020;
* moving the halves or the confirmation boundary;
* relaxing any threshold;
* any further calibration of classification, linking or gates;
* reading the confirmation after a qualification failure.

## 9. SAFETY

RESEARCH ONLY. No purchase, subscription, credential, registration, promotion, capital allocation, portfolio
mutation, order, fill, backfill or live write. The EDGAR reads are free public filings and records, fetched through
the canonical collector. The live checkout `C:\Users\binis\paper_trader` is not touched.
