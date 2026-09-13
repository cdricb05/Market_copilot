# PREREGISTRATION — PEER EARNINGS INFORMATION TRANSFER

**Family id:** `PEER_EARNINGS_INFORMATION_TRANSFER_V1`
**Domain / class:** `CROSS_MARKET_LEAD_LAG` / `INFORMATION_DIFFUSION_SPEED`, asset class `US_EQUITY`, model family `XS_EVENT_BOOK`
**Executor:** `alpha_agent.alpha_recovery.peer_earnings_information_transfer.run_mechanism`
**Status:** FROZEN. Written BEFORE any return of a non-reporting (traded) firm after a peer signal was
computed on real data. No pre-entry return, holding return, book, P&L, IC, t-statistic or hit rate exists
for this family. The only real-data computations behind this document are the §0 census: announcement
dates and acceptance instants, PIT membership, the CIK bridge, FSDS SIC observations, and PEERS' own
announcement-window reactions (the signal). The census scripts mirror §§1–5. The executor was exercised
end to end on SYNTHETIC data only (`tests/test_peer_earnings_information_transfer.py`).

This document is the contract. It is not edited after results exist. If a statement here turns out to be
inconvenient, the inconvenience is the result.

> **HEADLINE AT FREEZE — the preregistered outcome on the owned data is `DATA_HOLD` at gate 1.**
> Point-in-time SIC covers **86.4 %** of qualification member-quarters, below the frozen **90 %** floor.
> The binding shortfall is identity, not FSDS: 8.5 % of member-quarters have no RESOLVED CIK. Of the
> bridged ones, FSDS covers 94.5 %. The executor stops at gate 1 before it reads any traded name's return.
> Current SIC is NOT substituted. §0.6 states what would lift the hold.

---

## 0. CENSUS — signal and calendar only

### 0.1 Why this axis, and what it is not

**Economic hypothesis.** When the first firms in an industry report, their announcement-window returns
reveal industry-wide demand and cost news. Firms in the same industry that have not yet reported are
repriced only partially until their own announcement. A long-short book of not-yet-reporting firms, ranked
by the already-reported peers' announcement reaction, earns the delayed repricing (Thomas and Zhang 2008).

**Direction: a HIGH peer reaction is LONG, frozen.** A contradicted sign closes the family; it is never
reversed.

**Not duplicative.** The following scored an issuer's OWN event against its OWN return and found the
information spent at publication:

* `EARNINGS_EVENT_REACTION_SUE`
* `EVENT_8K_ITEM_CODES`
* `INSIDER_FORM4_ALL`

This family trades the unexposed PEERS, a different information object. The kill rule contains the exact
priced-before-entry test those families failed.

### 0.2 The frozen kill rule (catalog `pnl_gate.KILL_RULE`, verbatim)

> Kill if the signed rank IC t < 2.0 or the incremental net return over the incumbent is below 1.5 %/yr at
> 12.5 bp; or the late reporters' move is already complete by the session after the peer signal (priced
> before entry); or it fails BH q=0.10 inheriting the own-issuer earnings and 8-K event cells at p=1; or the
> industry assignment would require classification data from after the decision date.

`KILL_RULE_FROZEN` in the executor is this text byte for byte, and a test pins it against the catalog.
`run_mechanism` raises on any other text.

**The last clause binds over the catalog's data budget.** The data budget sentence ("current industry
classification only, disclosed as a look-ahead caveat") is superseded: current classification would require
data from after the decision date. The industry is therefore point-in-time SIC from FSDS (§2).

### 0.3 Owned data (no network, no purchase, $0)

| input | owner / path | used for |
|---|---|---|
| R57 PIT S&P 500 panel | `D:\Stock_Prediction_app_data\r57_alpha_discovery\panels\sp500_pit_panel_v1.npz` (+ `.meta.json`, manifest `9c35d293…`), 1,897 symbols incl. delisted, 5,601 sessions 2004-06-01..2026-09-03 | `tr` total-return closes, `mem` PIT membership, `spy_tr` |
| Item-coded 8-K stream | `event_8k_data.stream_path()` = `…\alpha_recovery_offensive\_data_sec_8k\filings_8k.jsonl` (built from the R63 EDGAR submissions histories, `items` column) | Item 2.02 acceptance instants |
| FSDS `sub.txt` | `D:\Stock_Prediction_app_data\alpha_agent\identity\sec_bulk\financial_statement_data_sets\2009q2..2026q1` (Stage 26), 68 quarters, each sha256-verified against its `manifest.json` | PIT `sic` per submission with `accepted` timestamp: 423,766 observations, 16,575 CIKs |
| CIK bridge | `alpha_agent.r58.fundamentals.cik_bridge()` (historical_identity `cik_map`, RESOLVED + active only) through `control_block_events.identity_map(use_sec_ticker_map=False)` | CIK → panel rows. 1,124 of 1,897 panel rows, 1,080 CIKs; 42 CIKs map to more than one row |
| incumbent daily path | `intraday_alpha.incumbent_daily_path()` (starts at `EQUITY_DISCOVERY_START` 2011-07-01) | gate 9 only |

**No PIT market capitalisation is owned.** The stage-24 companyfacts index holds zero
`EntityCommonStockSharesOutstanding` / `CommonStockSharesOutstanding` facts, and the panel carries no share
counts. The catalog's "size-weighted" signal is therefore **equal-weighted, frozen and disclosed** (§3).

### 0.4 Census tables

**A. Item 2.02 announcements (panel CIKs, all years).**
* 65,267 original 8-K filings declaring Item 2.02, across all issuer histories.
* For bridged panel CIKs: **59,503 announcements kept**, **5,760 follow-ups collapsed** (a 2.02 within 45 days
  of the last kept one; 8.8 %), 4 outside the calendar.
* Collapse windows considered before results: 30 d drops 7.2 %, 45 d drops 8.2 %, 60 d drops 9.0 % (exploratory
  census). 45 d is frozen.

**Acceptance instant of kept announcements (Eastern), share by year:**

| year | n | pre-open (<09:30) | intraday | after close (≥16:00) | non-session day |
|---|---|---|---|---|---|
| 2010 | 2,729 | 48.6 % | 20.0 % | 31.3 % | 0.1 % |
| 2012 | 2,721 | 49.8 % | 17.1 % | 31.9 % | 1.2 % |
| 2014 | 2,788 | 49.1 % | 15.6 % | 35.3 % | 0.0 % |
| 2016 | 2,769 | 49.9 % | 11.7 % | 38.4 % | 0.0 % |
| 2018 | 2,761 | 50.0 % | 8.6 % | 41.4 % | 0.0 % |
| 2019 | 2,778 | 50.8 % | 7.7 % | 41.5 % | 0.0 % |
| 2020 | 2,775 | 50.3 % | 6.9 % | 42.7 % | 0.0 % |
| 2022 | 2,757 | 51.2 % | 4.9 % | 43.9 % | 0.0 % |
| 2024 | 2,756 | 50.8 % | 4.1 % | 45.1 % | 0.0 % |
| 2026 (to 08) | 1,981 | 50.8 % | 2.5 % | 46.6 % | 0.1 % |

The intraday share (the minority whose event session carries a partial-day reaction) falls from 20 % to
under 8 % by 2019.

**B. Member-quarter coverage** (PIT member on the quarter's first panel session; PIT SIC as of that session,
strictly before its date; an announcement event session inside the quarter):

| year | member-quarters | CIK bridged | **PIT SIC** | Item 2.02 in quarter | PIT SIC / bridged |
|---|---|---|---|---|---|
| 2010 | 2,000 | 89.6 % | **69.0 %** | 83.6 % | 77.0 % |
| 2011 | 2,000 | 89.1 % | 84.0 % | 82.8 % | 94.3 % |
| 2012 | 1,998 | 89.5 % | 85.0 % | 83.4 % | 95.0 % |
| 2013 | 2,000 | 90.0 % | 85.8 % | 84.4 % | 95.3 % |
| 2014 | 2,003 | 90.5 % | 86.9 % | 85.4 % | 96.0 % |
| 2015 | 2,011 | 91.0 % | 87.4 % | 86.0 % | 96.0 % |
| 2016 | 2,017 | 92.0 % | 89.2 % | 87.6 % | 97.0 % |
| 2017 | 2,020 | 92.7 % | 90.3 % | 88.8 % | 97.4 % |
| 2018 | 2,019 | 94.2 % | 92.3 % | 90.5 % | 98.0 % |
| 2019 | 2,020 | 95.9 % | 94.2 % | 93.1 % | 98.2 % |
| **QUALIFICATION 2010–2019** | **20,088** | **91.5 %** | **86.4 %** | **86.6 %** | **94.5 %** |
| 2020 | 2,020 | 96.8 % | 95.5 % | 94.0 % | 98.7 % |
| 2022 | 2,016 | 97.7 % | 96.7 % | 95.5 % | 99.0 % |
| 2024 | 2,013 | 98.3 % | 97.7 % | 97.2 % | 99.4 % |
| 2026 (Q1–Q3) | 1,509 | 98.8 % | 98.7 % | 95.7 % | 99.9 % |
| **CONFIRMATION 2020–2026-08** | **13,603** | **97.9 %** | **97.2 %** | **96.0 %** | **99.2 %** |

* 2010 is low because of the XBRL phase-in: only the largest filers are in FSDS before mid-2010.
* Even excluding 2010, which is forbidden, qualification coverage would be 88.4 %.

**C. SIC reclassification — the look-ahead the kill rule forbids is real.**
* Of 850 panel CIKs with FSDS filings, **59 changed 4-digit SIC** over time and **46 changed 2-digit SIC**.
* Share of member-decisions whose PIT 2-digit SIC differs from the CIK's LATEST FSDS 2-digit SIC (the proxy for
  "current" SIC): **2010 4.1 %, 2012 3.6 %, 2014 3.3 %, 2016 2.1 %, 2018 1.7 %, 2019 1.5 %, 2022 0.7 %** (exploratory
  census).
* Current SIC would therefore have put 1.5–4 % of qualification members in an industry they were not in on the decision date.

**D. The identity gap** (qualification member-quarters with no RESOLVED CIK): **1,715 of 20,088 (8.5 %)**.
* 1,556 are delisted-suffix symbols; 159 are still listed.
* `cik_map` status, weighted by member-quarters: AMBIGUOUS 956, UNRESOLVED 559, SUPERSEDED 105, CONFLICT 40.
* Largest: `XOM` (AMBIGUOUS, 40 quarters), `BF.B`, `BRK.B`, `AGN-202005`, `CELG-201911`, `RTN-202004`, `ETFC-202010`,
  `VIAB-201912`, `ESRX-201812`, `AET-201811`, `TWX-201806`, `MON-201806`.
* Survivorship direction: mostly names that later left the index through acquisition.
* No other owned route yields a CIK. The FTD bridge maps CUSIP ↔ symbol; no local SEC ticker map exists; name
  matching is forbidden.

**E. Industry size per member** (qualification member-decisions with PIT SIC, exploratory census):

| grouping | median group size | p25 | p10 | share of members in groups ≥ 3 | ≥ 4 |
|---|---|---|---|---|---|
| **2-digit SIC (frozen)** | **20** | 6 | 3 | 93.6 % | 89.1 % |
| 3-digit SIC | 6 | 2 | 1 | 73.6 % | 65.9 % |

**F. The calendar under the frozen design** (§§1–5; weekly decisions; traded cohort = ≥ 9 entrants):

| | QUALIFICATION 2010–2019 | CONFIRMATION 2020–2026-08 |
|---|---|---|
| weekly decisions | 521 | 348 |
| entrants per decision: mean / median / p90 / max | 11.0 / 5 / 32 / 90 | 14.4 / 6.5 / 38 / 80 |
| weekly decisions with no entrant | 5.4 % | 1.7 % |
| entrants (all / in traded cohorts) | 5,730 / **4,668** | 5,023 / **4,308** |
| **traded cohorts** (per year) | **188** (18.8) | **151** (22.7) |
| entrants per traded cohort, median (long = short names) | 18 (6) | 26 (8) |
| industries per traded cohort: median / share ≤ 2 | 3.5 / 35.6 % | 4 / 16.6 % |
| reported peers per entrant: median / p10 / p90 | 4 / 3 / 7 | 4 / 3 / 10 |
| held sessions: mean / median / at the 20 cap / 3–5 | 10.5 / 10 / 8.6 % / 24.0 % | 11.1 / 11 / 9.9 % / 18.1 % |
| **actual announcement at or before the planned exit** | **10.9 %** (496 inside the hold, 12 at or before the entry close) | **11.4 %** (479 + 12) |
| traded cohorts concurrently open: max / mean | 5 / 1.41 | 5 / 1.68 |

* Traded cohorts by year (Q): 2010 11 · 2011 16 · 2012 21 · 2013 22 · 2014 19 · 2015 21 · 2016 18 · 2017 19 ·
  2018 21 · 2019 20.
* Traded cohorts by year (C): 2020 25 · 2021 18 · 2022 20 · 2023 22 · 2024 24 · 2025 25 · 2026 17.
* Across 2010–2026, 5,721 scored (CIK, expected session) keys dropped because fewer than 3 sessions remained
  before the planned exit at first eligibility.

**Variants considered on this census and NOT frozen** (exploratory census, qualification, cohorts ≥ 9):

| grouping / min peers | traded cohorts | traded entrants | median peers |
|---|---|---|---|
| **2-digit / 3 (frozen)** | **190** | **4,726** | **4** |
| 2-digit / 2 | 234 | 6,570 | 3 |
| 3-digit / 2 | 140 | 2,754 | 2 |
| 3-digit / 3 | 79 | 1,479 | 4 |

The frozen row differs from table F only by the per-CIK de-duplication added in the final census.

**G. The signal** (qualification traded entrants): equal-weight mean peer reaction.
* Quantiles: p5 −3.75 %, p25 −1.31 %, median −0.16 %, p75 +1.05 %, p95 +4.14 %; sd 2.27 %.
* Confirmation: p5 −4.74 %, p95 +4.40 %, sd 2.91 %.
* Individual peer reactions used as inputs, 2010–2019 (n = 17,415, exploratory): p1 −14.7 %, p5 −7.7 %,
  median +0.02 %, p95 +7.4 %, p99 +13.5 %; sd 4.9 %.

**H. Cost hurdle** (12.5 bp per side, §11). Each cohort carries 0.20 NAV long and 0.20 NAV short, entered and
exited once:

| | cohorts / yr | round-trip drag | gross needed for 1.5 %/yr net |
|---|---|---|---|
| QUALIFICATION | 18.8 | **1.88 %/yr of NAV** | **3.38 %/yr** |
| CONFIRMATION | 22.7 | **2.27 %/yr of NAV** | **3.77 %/yr** |

* Leverage-free: each cohort's gross long-short spread (mean long return minus mean short return, $1 each side)
  must exceed **50 bp** to pay its own four sides.
* Mean gross exposure is 1.41 × 0.40 = 0.56 NAV. The book is in cash off-season, and that cash earns zero in the NAV.

### 0.5 What the census decided, before any return

* 2-digit SIC, because 3-digit leaves a p10 group of one member.
* Minimum peers 3, because the signal is the mean of reactions with sd 4.9 % and 2 peers halve the averaging.
* Minimum cohort 9, so each tercile has at least three names.
* Slot weight 0.20, because up to 5 traded cohorts are open at once and 0.20 keeps gross at or below 2 NAV.
* A pooled (not per-cohort) price-state residualisation, because 35.6 % of qualification cohorts span at most two
  industries, and a per-cohort regression on an industry return would be degenerate there (§7).

### 0.6 The binding fact, and what would lift the hold

**Gate 1 fails at freeze: PIT SIC 86.4 % < 90 %.**
* 2.02 coverage (86.6 % ≥ 80 %), traded cohorts (188 ≥ 36) and FSDS integrity (68 of 68 quarters verified) pass.
* The executor therefore returns `DATA_HOLD`, `kill_rule_fired = DATA`.
* It never calls `measure` and reads no traded name's return; the artifact records `max_session_read = -1`.

The hold lifts only if an **authoritative** identity route resolves enough of §0.4 D before any return exists:
* roughly 760 more bridged member-quarters at the observed 94.5 % FSDS hit rate;
* a lead decision and an identity-owner change, followed by a fresh census;
* the 90 % floor, the windows and every parameter here stay frozen.

A current-SIC fallback, a lowered floor, a shortened window and name matching are all forbidden (§13).

---

## 1. EVENT AND REACTION WINDOW

* **Announcement:** an ORIGINAL `8-K` (form matched exactly, `event_8k_data.ORIGINAL_FORM`) whose structured
  `items` include `2.02`. Per issuer, in acceptance order, a 2.02 filed within **45 calendar days** of the last KEPT
  one is a follow-up and is dropped.
* **Event session:** `event_8k_data.decision_session(acceptanceDateTime)`, the first session whose 16:00 ET close
  is strictly after acceptance. Accepted before 16:00 on a session means that session; at or after 16:00, or on a
  non-session day, means the next session. `acceptanceDateTime` is UTC, converted by `control_block_data.acceptance_et`.
* **Reaction of a peer:** `tr(e)/tr(e−1) − spy(e)/spy(e−1)`, the total return from the close before the event
  session to the event-session close, minus SPY total return over the same span. One frozen benchmark, the panel's
  Norgate `spy_tr`. A non-finite price drops that peer.
* **Floor:** Item 2.02 announcements must cover **≥ 80 %** of member-quarters (census 86.6 % Q, 96.0 % C).

## 2. INDUSTRY — point in time

* **Source:** FSDS `sub.txt`, parsed by `sec_financial_statement_sets.parse_sub_txt` / `sic_observations`
  (`accepted` is the EDGAR acceptance instant, Eastern). Every quarter is sha256-checked against its manifest; a
  missing or mismatched quarter is `DATA_HOLD`.
* **Rule:** a member's SIC on decision date `d` is the `sic` of its latest FSDS submission accepted on a calendar
  date **strictly before `d`**. A submission accepted on `d` itself is not used, at any time of day. A later
  reclassification never reaches an earlier decision.
* **Grouping:** **2-digit SIC** (`sic // 100`). A member with no PIT SIC is excluded from peers and non-reporters
  (unknown stays unknown).
* **Floor:** PIT SIC must cover **≥ 90 %** of QUALIFICATION member-quarters (census **86.4 % → DATA_HOLD**) and
  ≥ 90 % of confirmation member-quarters before the confirmation is read (census 97.2 %).
* **Identity:** R58 RESOLVED `cik_map` only, through `identity_map(use_sec_ticker_map=False)`; no network, no
  names. One panel row per CIK per decision: the first member row in panel order.

## 3. SEASON AND PEER SET

* **Decisions:** the last panel session of each ISO week.
* **Peers of industry g at decision t:** members at t with PIT SIC group g whose latest announcement event session
  lies in **[t − 20, t]**, so the reaction is complete by the decision close, and whose reaction is finite.
* **Signal:** the **equal-weight mean** of those reactions, requiring **≥ 3 peers**. It is equal-weighted, not
  size-weighted, because no PIT market capitalisation is owned (§0.3). This deviation from the catalog wording is
  frozen.
* **Season:** implicit. A peer has reported within 20 sessions; a non-reporter has not reported within 42 sessions
  and is expected within 30. Under quarterly reporting these select the same season.

## 4. NON-REPORTER ELIGIBILITY AND EXIT — no look-ahead

A member at t (one row per CIK, PIT SIC known, finite `tr` at t) is a non-reporter candidate if:

1. it has **no announcement event session in [t − 42, t]**;
2. its **expected session** E exists and **E ≤ t + 30**. E is the first session on or after `min{ date(a) + 364 days :
   a an announcement event session ≤ t, date(a) + 364 > date(t) }`, the same fiscal quarter one year earlier plus
   52 weeks, using only announcements known at t;
3. its industry has **≥ 3 peers** at t.

* **Scored once.** Each (CIK, E) is scored ONCE, at the first weekly decision at which its industry has ≥ 3 peers.
  It is consumed there, whether or not it is tradable.
* **Entry:** close of **t + 1**.
* **Exit:** close of **min(entry + 20, E − 2)**. At least **3 held sessions** are required; otherwise the key is
  dropped (5,721 keys, 2010–2026).
* **An actual announcement arriving before the planned exit is NOT used to exit early** (that would be look-ahead).
  The position is held to the planned exit and disclosed: 10.9 % of qualification entrants (§0.4 F).
* **Missing prices:**
  * a name whose price stops inside the window exits at its last finite close;
  * a name with no entry-close price is held in cash (no return, no cost);
  * no name is removed for a price missing after t.

## 5. BOOK AND THE SIGNED RANK IC

* **Traded cohort:** a weekly decision with **≥ 9 entrants**. Rank by signal (ties by panel row):
  * **long the top ⌊n/3⌋**;
  * **short the bottom ⌊n/3⌋**;
  * the middle tercile is not traded.
* **Weights:** equal within leg; **0.20 NAV long and 0.20 NAV short per cohort** (dollar neutral, fixed at entry,
  buy-and-hold to each name's exit). Uninvested capital earns 0.
* **Daily book:** the sum over open positions of weight × side × daily change in value relative to the entry close.
  Costs of **12.5 bp per side** are charged on entry notional at the entry close and on exit notional at the exit close.
* **Signed rank IC (the kill rule's statistic):** per traded cohort, `r63.sensitivity.spearman(+signal, y)`.
  `y` = the entrant's total return from entry close to exit close minus SPY over the same span (all entrants, middle
  tercile included). The time series of cohort ICs is tested with `nw_tstat` at **Newey-West lag 3**
  (`r63.pit.nw_lag(20, 5)`: holdings of up to 20 sessions overlap about four weekly cohorts).
* **Book statistics:** annualised mean daily net × 252; Newey-West lag 5 (descriptive); `r63.sensitivity._max_dd`.
* **Why not `run_cell`:** its walk-forward ridge scorer conditions a grid feature on a baseline block. This is an
  event cohort with per-name entry and exit windows.

## 6. PRICED-BEFORE-ENTRY — the kill clause made measurable

For each traded cohort:
* **pre-entry IC** = `spearman(+signal, y_pre)`, with `y_pre` = the entrant's return from the signal close t to the
  entry close t+1 minus SPY (NW lag 0);
* **spreads** = the mean gross long-short spread over the pre-entry session and over the holding window.

The pre-entry **share** = pre / (pre + hold) of the mean spreads: 0 if the pre-entry mean ≤ 0, 1 if the holding
mean ≤ 0.

The verdict is `KILLED_PRICED_BEFORE_ENTRY` if either:
* the holding window fails the sign or IC-t gate while the pre-entry IC is positive with t ≥ 2.0, so the move
  happened before a legitimate entry; or
* the holding IC passes but the **pre-entry share ≥ 0.50**.

## 7. PRICE-STATE CONTROL — a gate that can only close

Peer reactions co-move with industry momentum, and the estate closed residual sector momentum
(`PRICE_STATE_EQUITY_FACTORS`).

1. Within each traded cohort, z-score (population sd; zero sd → 0) three quantities:
   * the signal;
   * the entrant's **own total return over (t − 20, t]**;
   * its PIT industry's **equal-weight total return over (t − 20, t]**, across members at t with finite prices.
2. Residualise z(signal) on [z(own), z(industry)] by **one pooled OLS over all entrants in the window**. The
   regression is pooled because 35.6 % of qualification cohorts contain ≤ 2 industries.
3. The residual's per-cohort rank IC against `y` must keep **NW t ≥ 2.0 (lag 3)**, otherwise `KILLED_NONINCREMENTAL`.

These are pre-signal returns only.

## 8. WINDOWS

Cohorts are assigned by decision date.
* **Qualification** 2010-01-01..2019-12-31; halves **H1 2010–2014** and **H2 2015–2019**. Qualification cohorts
  whose exits spill into January 2020 belong to qualification.
* **Confirmation** 2020-01-01..2026-08-31, read **only if gates 1–9 all pass**. Otherwise it stays untouched:
  `lockbox_t` is null and the prices after the last qualification exit are never read. A test proves this by
  poisoning them.

## 9. MULTIPLICITY

Owner: `r63.sensitivity.bh_fdr`, q = 0.10. The p-value is the one-sided NW p (lag 3) of the QUALIFICATION signed
rank IC series.

Inherited at **p = 1** — the kill rule's "own-issuer earnings and 8-K event cells", counted exactly:

| inherited cell | source | recorded result |
|---|---|---|
| `EARNINGS_EVENT_REACTION` h1 vs INCUMBENT_SCORE | `research/alpha_recovery/ALPHA_RECOVERY_SCOREBOARD.md` | NO_ADVANTAGE, t 1.57 |
| `EARNINGS_EVENT_REACTION` h5 vs INCUMBENT_SCORE | same | NO_ADVANTAGE, t −0.48 |
| `EARNINGS_EVENT_REACTION` h21 vs INCUMBENT_SCORE | same | NO_ADVANTAGE, t −0.25 |
| `EARNINGS_EVENT_REACTION` h21 vs PRICE_RETURN_STATE+…+FREE_CASH_FLOW | same | NO_ADVANTAGE, t 0.68 |
| `EARNINGS_REACTION_ONLY` h21 vs INCUMBENT_SCORE | same | NO_ADVANTAGE, t −1.15 |
| `EARNINGS_SURPRISE_ONLY` h21 vs INCUMBENT_SCORE | same | NO_ADVANTAGE, t 1.09 |
| `EARNINGS_EVENT_REACTION` h63 vs INCUMBENT_SCORE | same | NO_ADVANTAGE, t −1.40 |
| `EVENT_8K_ITEM_CODES` RESTRUCTURING_OR_IMPAIRMENT h5 | `research/preregistration/EVENT_8K_ITEM_RESULT.md` (m = 3) | not rejected |
| `EVENT_8K_ITEM_CODES` RESTRUCTURING_OR_IMPAIRMENT h21 | same | not rejected |
| `EVENT_8K_ITEM_CODES` RESTRUCTURING_OR_IMPAIRMENT h63 | same | not rejected |

* **m = 1 + 10 = 11.** A sole survivor needs **p ≤ 0.10 / 11 = 0.00909** (one-sided t ≈ 2.36).
* The denominator is never reset. `INSIDER_FORM4_ALL` and the news cells are not in the kill rule's clause and are
  not added.
* Every p comparison tests `p is not None` explicitly (an underflowed p = 0.0 is the strongest evidence, not a
  missing one).

## 10. GATES — in this order

| # | gate | verdict | `kill_rule_fired` | measured at freeze |
|---|---|---|---|---|
| 1 | FSDS integrity fails, PIT SIC < 90 % of qualification member-quarters, Item 2.02 < 80 %, or < 36 traded qualification cohorts | `DATA_HOLD` | `DATA` | **integrity 68/68; PIT SIC 86.4 % — FAILS**; 2.02 86.6 %; 188 cohorts |
| 2 | mean signed rank IC ≤ 0 (→ `KILLED_PRICED_BEFORE_ENTRY` if the pre-entry IC is positive with t ≥ 2) | `KILLED_WRONG_SIGN` | `SIGNED_RANK_IC_NOT_POSITIVE` | — |
| 3 | signed rank IC NW t < 2.0 (same pre-entry refinement) | `NO_EDGE` | `SIGNED_RANK_IC_T_BELOW_2` | — |
| 4 | pre-entry share ≥ 0.50 (or not measurable) | `KILLED_PRICED_BEFORE_ENTRY` | `PRICED_BEFORE_ENTRY` | — |
| 5 | residual IC t < 2.0 after the price-state control | `KILLED_NONINCREMENTAL` | `NOT_INCREMENTAL_TO_PRICE_STATE` | — |
| 6 | net book < 1.5 %/yr of NAV at 12.5 bp | `KILLED_BELOW_MATERIALITY` | `NET_BOOK_BELOW_1.5PCT_PER_YEAR` | hurdle 1.88 %/yr drag |
| 7 | either half: net book ≤ 0 or mean IC ≤ 0 | `KILLED_UNSTABLE` | `HALVES_DISAGREE` | — |
| 8 | p is None or BH q = 0.10 fails at m = 11 | `KILLED_MULTIPLICITY` | `BH_Q010_FAILS_WITH_INHERITED_EARNINGS_AND_8K_CELLS` | — |
| 9 | incumbent path unavailable → `DATA_HOLD`; equal-risk increment < 1.5 %/yr or not positive with t ≥ 2 (`equal_risk_daily`) | `NO_INCREMENTAL_INFORMATION_EDGE` | `INCREMENT_OVER_INCUMBENT_BELOW_1.5PCT` | incumbent path from 2011-07-01 |
| 10a | confirmation: PIT SIC < 90 % or 2.02 < 80 % | `DATA_HOLD` | `DATA` | 97.2 % / 96.0 % |
| 10b | confirmation traded cohorts < 36 | `NEED_MORE_EVIDENCE` | — | 151 |
| 10c | confirmation mean IC ≤ 0 | `KILLED_UNSTABLE` | `CONFIRMATION_SIGN_REVERSED` | — |
| 10d | confirmation IC t < 2.0, or p is None or p > 0.10 | `NO_EDGE` | `CONFIRMATION_IC_T_BELOW_2` / `CONFIRMATION_P_ABOVE_Q` | — |
| 10e | confirmation pre-entry share ≥ 0.50 | `KILLED_PRICED_BEFORE_ENTRY` | `PRICED_BEFORE_ENTRY` | — |
| 10f | confirmation net book < 1.5 %/yr | `KILLED_BELOW_MATERIALITY` | `CONFIRMATION_NET_BELOW_1.5PCT_PER_YEAR` | — |
| 11 | otherwise | `QUALIFIED` — a HUMAN gate only | — | — |

* **`capital_eligible` is always False.** `QUALIFIED` only opens the human-gated prospective registration; capital
  needs forward evidence and governance.
* **Gate 9 overlap.** The incumbent's daily path begins 2011-07-01, so gate 9 compares the overlapping 2011-07..2019
  sessions, zero-P&L off-season days included.
* **Additions beyond the catalog rule** can only close, never rescue:
  * gate 1's coverage and sample floors;
  * the price-state control (gate 5);
  * the halves (gate 7);
  * the t ≥ 2 requirement on the incumbent increment;
  * the confirmation gates.

## 11. COSTS

* The equity desk rate `EQ_COST_RATE_PER_SIDE` = **12.5 bp per side** (`alpha_agent.alpha_recovery` ← `r63`). It is
  charged on every entry and exit notional, both legs.
* Ladder (descriptive): 0 / 12.5 / 25 bp.
* Hurdle from the census, no return involved: **1.88 %/yr of NAV (Q), 2.27 %/yr (C)**; gross needed for 1.5 %/yr
  net **3.38 / 3.77 %/yr**; per cohort **50 bp** of gross long-short spread (§0.4 H).

## 12. DESCRIPTIVE ONLY — never a gate

* The book at 0 and 25 bp.
* The book's NW t (lag 5), maximum drawdown and volatility.
* The per-half IC t.
* The pre-entry IC when gates 2–3 pass.
* The price-state regression coefficients.
* The calendar census block.
* Entrants whose actual announcement fell inside the hold.
* The confirmation coverage when not read.

## 13. FORBIDDEN AFTER RESULTS

* Current, entity-level or Norgate/GICS classification; any classification accepted on or after the decision date;
  back-filling a missing PIT SIC from a later filing; a sector map instead of 2-digit SIC; 3- or 4-digit SIC.
* Lowering the 90 % PIT SIC or the 80 % announcement floor; excluding 2010 or any year; changing the windows or
  halves.
* Adding an identity route (name matching, a current ticker map, a guessed AMBIGUOUS resolution) after any return
  exists. An identity repair BEFORE results is a lead decision that requires a fresh census (§0.6, §14).
* Size, value, volatility or signal-magnitude weighting; quintiles or deciles; long-only or short-only; reversing
  the sign; a minimum cohort other than 9; a slot weight other than 0.20.
* MIN_PEERS ≠ 3, peer look-back ≠ 20, no-report window ≠ 42, maximum lead ≠ 30, hold cap ≠ 20, pre-expected buffer
  ≠ 2, minimum hold ≠ 3, follow-up window ≠ 45 days, expected shift ≠ 364 days, a non-weekly decision grid.
* Exiting on the actual announcement; holding through or trading after the announcement; entering at the decision
  close; any second cell (the late reporter's own post-announcement drift, the early reporter, an industry-portfolio
  version, a SUE-based signal).
* A benchmark other than the panel's SPY total return for the reaction or `y`; raw instead of market-adjusted `y` for
  the IC.
* Newey-West lag ≠ 3 for the IC; a pre-entry share limit ≠ 0.50; per-cohort instead of pooled residualisation;
  dropping own or industry return from the control; a T floor below 2.0.
* Charging less than 12.5 bp; resetting m or dropping any inherited cell; using the book's t instead of the IC's p for BH.
* Reading the confirmation before every qualification gate passes, or re-reading it on another window.

## 14. DECISIONS FOR THE LEAD

1. **Accept the preregistered `DATA_HOLD`** (commit, pin, run; the executor stops at gate 1 with no traded-name
   return read), **or** commission, BEFORE committing, an authoritative identity-resolution slice for the §0.4 D
   rows (95 AMBIGUOUS and 671 UNRESOLVED securities in `cik_map`). If it lifts qualification PIT SIC coverage to
   ≥ 90 %, a fresh census precedes any run. Nothing else in this document changes.
2. Confirm **equal weighting** as the disclosed deviation from the catalog's "size-weighted" wording (no owned PIT
   market capitalisation).
3. Confirm that the kill rule's last clause supersedes the catalog data budget's "current industry classification"
   sentence.
4. Set the executor's catalog pin (`module_sha256`, `preregistration_sha256`) after this document is committed.

**Lead decisions, recorded 2026-09-13 before any return:**

* **(1) The preregistered `DATA_HOLD` is accepted.** No identity-resolution slice is commissioned in this
  session. The mechanism's class already has several closed members, its gross hurdle is 3.38 %/yr, and an
  authoritative resolution of AMBIGUOUS or UNRESOLVED CIKs would need new SEC identity work. That work would not
  change the odds of after-cost P&L enough to be worth doing ahead of the priced purchase gate.
  * The agent re-runs a settled hold only when the catalog `data_version` changes (`alpha_agent.r59.mechanisms`).
  * Any future identity repair therefore needs a lead catalog change, a fresh census and a re-review of this
    executor before a return can be read.
* **(2) Equal weighting is confirmed** as the disclosed deviation.
* **(3) The kill rule's last clause supersedes the data budget's "current classification" sentence.** This is
  confirmed.

## 15. SAFETY

RESEARCH ONLY. No purchase, no subscription, no registration, no promotion, no capital allocation, no portfolio
mutation, no proposal, no order, no fill, no live write, no network. Writes land only under the campaign research
root (`results/peer_earnings_information_transfer.json`). The live checkout is not touched.
