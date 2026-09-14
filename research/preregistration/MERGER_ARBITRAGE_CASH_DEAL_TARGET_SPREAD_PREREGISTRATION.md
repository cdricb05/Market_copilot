# PREREGISTRATION — MERGER ARBITRAGE, ALL-CASH DEAL TARGETS

**Family id:** `MERGER_ARBITRAGE_CASH_DEAL_TARGET_SPREAD_V1`
**Domain / class:** `STRUCTURAL_FLOWS_FORCED_TRADING` / `RISK_TRANSFER_PREMIUM`, asset class `US_EQUITY`, model family `EVENT_CALENDAR_BOOK`
**Executor:** `alpha_agent.alpha_recovery.merger_arbitrage_cash_deal.run_mechanism`
**Status:** FROZEN. Written and committed BEFORE any return of a target, a book, a P&L, a t-statistic or a
hit rate existed. The only real-data computations behind this document are the §0 census: EDGAR index rows,
the opening text of merger-form filings, Norgate security METADATA (base ticker, name, first and last quoted
date) and SPY's session DATES. The executor was exercised end to end on SYNTHETIC data only
(`tests/test_merger_arbitrage_cash_deal.py`, `tests/test_merger_arbitrage_calendar.py`).

This document is the contract. It is not edited after results exist. If a statement here turns out to be
inconvenient, the inconvenience is the result.

> **HEADLINE AT FREEZE —** 1,815 all-cash deals included from 8,248 EDGAR merger-form episodes (15,668 of 15,676
> filings read, 99.95 %): 1,252 qualification deals (2003–2016) at 88.2 % identity coverage and 474 confirmation deals
> (2017 – 2026-08) at 93.9 %; classification gate audit 58 / 60 (96.7 %); 50 slots of 2 % of NAV each, census mean
> exposure 37.4 % of NAV in qualification, so the 1.5 %/yr materiality floor needs about 4.0 %/yr net excess return on
> the capital deployed. No target price, spread, P&L, hit rate or statistic has been read.

---

## 0. CENSUS — calendar, identity and metadata only

### 0.1 The economic hypothesis, and what it is not

**Mechanism.** When an all-cash acquisition is announced, the target trades below the cash consideration until
the deal closes. Holders who do not want deal-break, time-to-close, financing and regulatory risk sell to
capital-constrained arbitrageurs, so the spread exceeds fair compensation for the probability-weighted break
loss (Mitchell and Pulvino 2001; Baker and Savasoglu 2002).

**Research question.** Does a strictly point-in-time, tradeable, cash-deal target book produce material
after-cost, risk-adjusted paper P&L — measured as a calendar-time portfolio, net of costs, of the Treasury bill
on the capital it ties up, of failed deals and of overlapping positions — rather than as a spread percentage?

**Direction: LONG the target, frozen.** A negative book closes the family; it is never reversed, hedged into a
different claim or re-cut.

**Not duplicative.** `EVENT_8K_ITEM_CODES` scored item codes as signals of the filer's own later return and
found them priced at publication; `SP500_INDEX_ADDITION_DELETION` traded index-membership flows;
`CONTROL_BLOCK_13DG` traded ownership-stake filings. This family harvests a completion-risk premium on a known
cash offer; it times nothing and claims no information edge.

### 0.2 The frozen kill rule (catalog `pnl_gate.KILL_RULE`, verbatim)

> Kill if the calendar-time all-cash target book has NW t < 2.0 or net below 1.5 %/yr of NAV at 12.5 bp; or its
> increment over a beta-matched SPY exposure has NW t < 2.0; or annualised net is non-positive in either
> qualification half; or it fails BH q=0.10 at m=1 with EVENT_8K_ITEM_CODES and SP500_INDEX_ADDITION_DELETION
> inherited at p=1; or deal identification or consideration type requires information dated after the entry
> session.

`KILL_RULE_FROZEN` in the executor is this text byte for byte; a test pins it against the catalog and
`run_mechanism` raises on any other text.

### 0.3 Data ($0, free public or owned; nothing purchased)

| input | owner / location | used for |
|---|---|---|
| EDGAR quarterly `master.zip`, 2002Q1–2026Q3, EVERY filer | `merger_arb_data.fetch_full_index` → `…\alpha_recovery_offensive\_data_sec_merger\full_index` | the universe: forms DEFM14A, PREM14A, DEFM14C, PREM14C, SC 14D9, SC TO-T |
| the opening 256 KB of each filing's complete submission text; 2 MB for cash-like episodes with no resolvable symbol | `merger_arb_data.fetch_heads` / `fetch_deep` (the estate's SEC user agent, one shared rate gate ≤ 7.7 request starts/s) | SEC header (form, filing date, subject/filer CIK), consideration terms, symbol statements |
| Norgate US Equities + US Equities Delisted metadata | `merger_arb_events.build_universe` → `norgate_us_equity_universe.csv` (41,917 securities: 14,683 active, 27,234 delisted) | base ticker and quoted range |
| Norgate total-return closes | `r63.panels.load_norgate_total_return` | target and SPY prices — read ONLY by the executor, after gate 1 |
| 3-month Treasury constant maturity (`CMT_3M`) | `r63.panels.load_fred_daily` (R41 FRED panel) | financing / the bill |

**Why not the owned SEC store.** The R63 submissions histories hold 1,080 CIKs selected on S&P 500 membership and
its canonical `keep_form` drops every merger form; a universe built on it would contain only index members.

**sec.gov ignores HTTP Range** on these files (measured: a DEFM14A returned 200 with all 5,983,713 bytes), so the
head is read from the stream and the connection closed.

### 0.4 Census tables

Final calendar (after repairs 1–10). EDGAR index: 18,031 rows, 15,676 accessions; 15,668 filings read (8 still
refused by sec.gov after retries: 99.95 %); 1,147 deep reads (of 1,156 requested) of the documents of cash-like
episodes with no resolvable symbol.

**Episodes by state, 2002–2026**

| state | episodes |
|---|---|
| `INCLUDED_ALL_CASH` | 1,815 |
| `EXCLUDED_NOT_AN_ALL_CASH_ALL_SHARES_DEAL` | 2,930 |
| `EXCLUDED_TERMS_NEVER_ESTABLISHED` | 3,194 |
| `EXCLUDED_TARGET_QUOTED_OVER_THE_COUNTER` | 69 |
| `UNRESOLVED_IDENTITY` | 226 (no symbol statement 120, stated ticker not quoted 58, refused by the name guard 48) |
| `AMBIGUOUS_IDENTITY` | 14 |
| total | 8,248 |

**Windows** (by establishing or deciding date; identity coverage = included / (included + unresolved + ambiguous))

| window | episodes | included | unresolved | ambiguous | OTC | identity coverage |
|---|---|---|---|---|---|---|
| Qualification 2003–2016 | 4,825 | 1,252 | 161 | 6 | 55 | **88.2 %** |
| H1 2003–2009 | 2,718 | 691 | 124 | 2 | 38 | 84.6 % |
| H2 2010–2016 | 2,107 | 561 | 37 | 4 | 17 | 93.2 % |
| Confirmation 2017 – 2026-08 | 2,911 | 474 | 24 | 7 | 5 | **93.9 %** |

The 85 % floor binds on the qualification and confirmation windows as frozen; the halves are a sign test only. H1's
84.6 % is disclosed: identity coverage is 73.9 % in 2003, 79.0 % in 2004 and 84.0 % in 2006 (early proxies often state
no trading symbol), and 89–95 % in every year from 2008.

**Deals entered per year** — qualification: 2003 82, 2004 83, 2005 94, 2006 132, 2007 142, 2008 97, 2009 61, 2010 105,
2011 87, 2012 88, 2013 82, 2014 63, 2015 69, 2016 67 (1,252). Confirmation: 2017 73, 2018 54, 2019 50, 2020 38,
2021 51, 2022 47, 2023 46, 2024 40, 2025 46, 2026 29 (474).

**Concurrency and holds** (entry by §3; exit at the target's Norgate last-quoted date or 126 sessions — metadata only)

| | qualification | confirmation |
|---|---|---|
| open deals median / p90 / p95 / max | 18 / 29 / 34 / 45 | 10 / 14 / 16 / 23 |
| hold, sessions: median / p90 | 43 / 126 | 37.5 / 126 |
| holds reaching the 126-session cap | 11.1 % | 11.6 % |

**Slot ladder** (capacity refusals, mean exposure as a share of NAV, round-trip cost drag at 12.5 bp per side)

| slots | Q refusals | Q mean exposure | Q cost drag / yr | C refusals | C mean exposure | C cost drag / yr |
|---|---|---|---|---|---|---|
| 40 | 9 (0.7 %) | 46.2 % | 0.54 % | 0 | 25.3 % | 0.31 % |
| **50** | **0** | **37.4 %** | **0.43 %** | **0** | **20.2 %** | **0.25 %** |
| 60 | 0 | 31.1 % | 0.36 % | 0 | 16.9 % | 0.21 % |
| 80 | 0 | 23.4 % | 0.27 % | 0 | 12.6 % | 0.15 % |
| 100 | 0 | 18.7 % | 0.22 % | 0 | 10.1 % | 0.12 % |

**Included deals, descriptive**

* First form of the episode: PREM14A 1,057; SC TO-T 517; SC 14D9 159; PREM14C 60; DEFM14A 20; DEFM14C 2.
* The establishing date is the episode's first filing date for 91.5 % of deals (median lag 0 days).
* Symbol source: company-tagged 1,269; untagged 546 (the name guard applies to both).
* Norgate status today: delisted 1,778; still quoted 37 (failed deals and deals still pending).
* Norgate exchange — the security's LAST venue, not its venue at the deal: Nasdaq 1,202, NYSE 436, NYSE American 82,
  OTC tiers 95. The 95 are **not** excluded: a last venue is later information (a broken deal can be delisted to the
  counter), and excluding on it would remove failed deals after the fact. The OTC exclusion of §0.5 item 5 uses only
  what the filing itself said.

### 0.5 Repairs made to the parser BEFORE freezing — each measured, none after a return

All measured on filing text only (the first 2,500–4,000 heads), before any price of a target was read.

1. **Stock consideration by shape, not phrase.** A bare-phrase match flagged 54–59 % of merger proxies as stock deals,
   including plain cash deals (Howell / Anadarko $20.75 cash; Vestcom $6.25 cash). "exchange offer" (202 hits) and
   "exchange ratio" (103) were overwhelmingly no-shop boilerplate and competing-bid background; "will receive …
   shares of … common stock" matched option cash-outs. Stock consideration is now recognised only as a number or
   fraction of shares received in the conversion clause, an offer to EXCHANGE, a cash-and-stock TRANSACTION noun
   phrase, or "cash payment and shares of". After repair: 4–21 % of priced documents.
2. **Partial offers.** A bare "up to N shares" matched option grants, share issuances and buyback authorisations in
   proxies (11–24 %). Now only "offer to purchase … up to N shares" or "partial tender offer": 0 % of proxies, 4 % of
   tender documents.
3. **Elections.** "elect to receive" also matched appraisal rights and noteholders' options; those contexts are
   excluded, and a cash/stock/shares object is required.
4. **Symbol statements in the whole stored text.** Of 252 cash-like episodes with no symbol in the first 90K
   flattened characters, 84 stated one further in, 83 of them only there (the market-price section). Terms stay on
   the opening 90K characters, where the consideration is.
5. **Over-the-counter targets.** 82 of those 252 said the target was quoted over the counter; an OTC symbol is not a
   listed Norgate US equity. An episode whose only stated symbols are OTC-quoted and unlisted is outside the
   investable universe (`EXCLUDED_TARGET_QUOTED_OVER_THE_COUNTER`) and is not counted as an identity failure.
6. **Refusal-only name guard.** An acquirer's symbol printed in a cash deal could otherwise be mapped as the target.

Repairs 7–9 followed the FIRST classification audit (§0.6), which found three errors in 60 deals, all general parser
gaps. Each repair was measured on filing text among the 1,875 deals the pre-repair calendar included, every hit read
by hand, before any price of a target was read. Broad variants were rejected on the same reading: "a number of shares"
(559 deals) is overwhelmingly option cash-out arithmetic, "(A) … shares … (B) $" (163) is filing-fee arithmetic, and a
bare "up to N %" (17) is standstills, lock-up options and abandoned background proposals.

7. **Mixed consideration by its shape.** A cash amount joined by *and / plus* to a share count ("0.6494 shares",
   "0.1553 of a share", "a number of shares", "a fraction of a share"), in either order, enumerated or not; never a
   comma-grouped integer. 46 included deals hit; 45 were mixed or securities deals (DIRECTV, Alterra, Time Warner,
   Bard, Pinnacle Foods, Kansas City Southern, Humana, CareFusion, …). **One was an all-cash deal and is excluded
   anyway:** PLX Technology, whose Avago tender recommendation recounts IDT's earlier cash-and-stock bid in its
   background section. It is a disclosed, frozen classification error.
8. **Percentage partial offers**, only in the offer's own framing ("offer to purchase for cash up to N % of", "the
   offer by Purchaser to purchase up to N % of") and below 100 %: one included deal (Supervalu, 30 %).
9. **Securities registered for the deal.** A filing that calls itself a "proxy statement/prospectus" AND cites a
   registration statement on Form S-4/F-4 or securities "to be issued". Of the 13 included deals whose filings used the
   phrase and no shape caught, the 5 with that evidence were securities deals (Knoll, Zynga, Kimball, Macquarie
   Infrastructure) or an acquirer's own merger proxy (URS for Washington Group); the 8 without it were all-cash deals
   whose text mentions a prospectus in passing (LSI, Blue Buffalo, CH Energy, EnergySolutions, Mity, M&F Worldwide,
   Dover Saddlery, Mondavi), and they stay included. "exchange ratio" alone would have excluded LSI and Mondavi.
10. **Contingent consideration under other names** (after the SECOND audit found Indevus: "$4.50 per Share, net to
    the seller in cash … plus contractual rights to receive up to an additional $3.00 per Share in contingent cash
    consideration payments"). A bare contingent-payment phrase matched 32 included deals; 8 gave target holders a
    contingent right (Venture Catalyst, Indevus, Gerber Scientific, American Medical Alert, Adolor, New Frontier
    Media, NuPathe, ZAGG), the other 24 were background proposals, earnouts of earlier acquisitions, licensing
    milestones and employee awards. The rule anchors on the consideration clause's own verb ("right to receive",
    "converted into", "at a price of") and on the right being granted with the cash ("and (2) one …", "plus
    contractual rights …"); measured on every included deal, it excludes exactly those 8.

### 0.6 Classification audit (filing text only, judged before freezing)

**Question judged, per deal:** is this episode an all-cash acquisition of ALL shares of THIS target, and is the Norgate
symbol the target's own common stock? Judged from the stored filing text only, never from a price.

**Round 1 (pre-repair calendar, 60 qualification deals, `random.seed(20260914)`): 57 / 60 = 95.0 %.** Errors: DIRECTV
(cash plus AT&T shares), Alterra (Markel shares plus cash), Supervalu (a partial offer for up to 30 %). Checked and
correct: USANA (an all-cash minority buyout that did not complete — a failed deal, correctly kept), Trans Energy,
GrafTech, Resonate (cash stated as a range), Ribapharm (minority buyout). The errors were general parser gaps, so
repairs 7–9 (§0.5) were made and a second audit was drawn.

**Round 2 — THE GATE AUDIT (repaired calendar, 60 qualification deals drawn from the 1,201 that round 1 did not
judge, `random.seed(20260915)`): 58 / 60 = 96.7 % ≥ 90 %.** Errors: Indevus (contingent cash consideration rights
alongside the cash — repair 10 now excludes it, and the precision is NOT recomputed without it) and Methode
Electronics (an all-cash offer for every Class B share, the traded security, but not an acquisition of the company —
counted as an error conservatively and not repaired: one case, no general rule measured). Checked and correct:
Federal-Mogul (minority buyout), Marsh Supermarkets (Class A and B both cashed out), Engelhard (unsolicited all-cash
tender), Topps (a competing all-cash tender in the same episode).

Both rounds, every sampled row and every judgment are frozen in `_data_sec_merger\classification_audit.json`
(sha256 in §14). **Known residual error classes, disclosed and frozen:** PLX Technology (an all-cash deal excluded by
repair 7's background match) and class-level offers such as Methode's.

---

## 1. UNIVERSE AND EPISODES

* **Documents.** Every EDGAR full-index row of the six forms, 2002Q1–2026Q3. DEFM14A / PREM14A / DEFM14C / PREM14C /
  SC 14D9 are indexed under the target. SC TO-T is indexed under the subject company AND the bidder; a row is a
  target document only when the filing's own SEC header names that CIK as SUBJECT COMPANY.
* **Filing date** is the header's FILED AS OF DATE (EDGAR assigns the next business day to a submission accepted
  after its cut-off, so it is never earlier than the day the text became public).
* **Episode.** One target CIK's documents in filing-date order; a document more than **365 days** after the previous
  one starts a new episode.

## 2. THE ESTABLISHING DATE — terms and identity from the filings, point in time

Walking the episode's documents in filing-date order, within **120 days** of its first document, the establishing
date is the first filing date by which the documents filed so far, together:

1. state a **cash price per share** for **all** outstanding shares (merger conversion clause or all-shares cash offer);
2. state **no** stock, election, exchange-offer or contingent-value-right consideration and **no** partial offer —
   any such statement filed on or before a date excludes the episode permanently; a later document never rescues it;
3. state the target's **trading symbol** (not tagged to Parent / Purchaser / Acquirer / Buyer / Offeror / Merger Sub)
   that, after the refusal-only name guard, resolves to **exactly one** Norgate US equity quoted on that date.

**Name guard (refusal only).** A stated symbol is kept only if its Norgate security name shares a distinctive
token with the target's SEC conformed name. It can remove a candidate and can never create one.

Outcomes: `INCLUDED_ALL_CASH`; `EXCLUDED_NOT_AN_ALL_CASH_ALL_SHARES_DEAL`; `EXCLUDED_TERMS_NEVER_ESTABLISHED`;
`EXCLUDED_TARGET_QUOTED_OVER_THE_COUNTER`; `UNRESOLVED_IDENTITY`; `AMBIGUOUS_IDENTITY`.

**No CUSIP guess, no current ticker map, no name match that creates an identity, no information dated after the
establishing date.**

## 3. ENTRY

* The **close of the first SPY session strictly after the establishing filing date** (a legitimate one-session
  delay: the text was public before that session opened or during the preceding session).
* A target with no total-return close on the entry session is **not traded** (counted).
* The deal list is FROZEN: its hash (§14) must match at run time or the executor holds.

## 4. EXIT, FAILED DEALS, HALTS

* **Exit** at the earlier of the target's **last quoted session** (completion, or any delisting) and **entry + 126
  sessions** (the catalog cap), and never after the last panel session.
* **Failed deals are not detected and not exited early**: a broken deal is held to the cap and its break loss is
  realised in the book. No termination announcement is read.
* A session without a target close carries the last value (zero P&L that day); the next close books the move.

## 5. SIZING AND DIVERSIFICATION

* **50 slots**; each deal carries a fixed **1/50 of NAV** at its entry close, buy-and-hold to exit.
* **How the slot count was fixed (metadata only, §0.4):** the smallest of 40 / 50 / 60 / 80 / 100 at which the census
  capacity rule refuses no deal in either window, with holds bounded by each target's Norgate last-quoted date and the
  126-session cap. It maximises capital use without refusing deals; no return, spread or P&L entered the choice.
* **Maximum exposure per deal:** one slot. **No leverage:** at entry notional the book never exceeds one NAV.
* A deal arriving while every slot is occupied is **not traded** (counted); ties by establishing date, then CIK.

## 6. P&L ACCOUNTING

* **Daily net excess P&L** (fraction of NAV) = Σ over open slots of `slot × (V_t − V_{t−1})` − `slot × V_{t−1} ×
  CMT_3M_t / 100 / 252` − costs, where `V` is the total-return close relative to the entry close.
* **Costs:** **12.5 bp per side** on entry notional at the entry close and on exit notional at the exit close.
* **Cash:** idle capital earns nothing in this book and no financing is charged on it; the bill is charged on the
  marked capital of every open slot, so the book is measured IN EXCESS OF CASH.
* **Statistics:** annualised mean daily net × 252; Newey-West t with **lag 10** (`r63.sensitivity.nw_tstat`);
  Sharpe; `r63.sensitivity._max_dd`; mean and maximum exposure; per-deal gross return distribution; effective
  independent observations reported as deals and months.

## 7. CONTROLS

* **Beta-matched SPY:** the daily book minus β × (SPY total return − the bill), β estimated over the same sessions;
  its NW t (lag 10) must be ≥ 2.0.
* **Halves:** `H1 2003–2009` and `H2 2010–2016` (by entry date); annualised net must be positive in both.

## 8. WINDOWS

* **Qualification:** deals ENTERED 2003-01-01..2016-12-31. Their exits may read prices up to 126 sessions past the
  window end; nothing later is read.
* **Confirmation:** deals ENTERED 2017-01-01..2026-08-31, read **only if every qualification gate passes**.
  Otherwise untouched: `lockbox_t` is null and the reader's latest date is recorded.

## 9. MULTIPLICITY

Owner `r63.sensitivity.bh_fdr`, q = 0.10; the p-value is the one-sided Newey-West p (lag 10) of the qualification
daily net book. Inherited at **p = 1**, one entry per recorded cell of the kill rule's two families:

| inherited cell | recorded result |
|---|---|
| `SP500_INDEX_ADDITION_DELETION` addition reversal h63 | t −0.03 |
| `SP500_INDEX_ADDITION_DELETION` addition echo h5 | t −2.11, about +8 bp after 50 bp cost |
| `SP500_INDEX_ADDITION_DELETION` deletion rebound | t 0.80 |
| `EVENT_8K_ITEM_CODES` RESTRUCTURING_OR_IMPAIRMENT h5 | not rejected |
| `EVENT_8K_ITEM_CODES` RESTRUCTURING_OR_IMPAIRMENT h21 | not rejected |
| `EVENT_8K_ITEM_CODES` RESTRUCTURING_OR_IMPAIRMENT h63 | not rejected |

**m = 1 + 6 = 7**; a sole survivor needs p ≤ 0.10 / 7 = 0.01429. The denominator is never reset; `p is None` fails.

## 10. GATES — in this order

| # | gate | verdict | `kill_rule_fired` |
|---|---|---|---|
| 1 | head coverage < 98 %; audit missing, hash-mismatched or precision < 90 %; qualification identity coverage < 85 %; < 150 included qualification deals; frozen calendar hash mismatch; SPY or bill unavailable | `DATA_HOLD` | `DATA` |
| 2 | an included deal would enter on or before its establishing date | `KILLED_PIT_UNESTABLISHED` | `IDENTIFICATION_AFTER_ENTRY` |
| 3 | qualification net ≤ 0 | `KILLED_WRONG_SIGN` | `BOOK_NET_NOT_POSITIVE` |
| 4 | qualification NW t < 2.0 | `NO_EDGE` | `BOOK_NW_T_BELOW_2` |
| 5 | qualification net < 1.5 %/yr of NAV at 12.5 bp | `KILLED_BELOW_MATERIALITY` | `NET_BELOW_1.5PCT_PER_YEAR` |
| 6 | beta-matched SPY increment NW t < 2.0 | `KILLED_NONINCREMENTAL` | `INCREMENT_OVER_BETA_MATCHED_SPY_T_BELOW_2` |
| 7 | either half net ≤ 0 | `KILLED_UNSTABLE` | `HALF_NOT_POSITIVE` |
| 8 | BH q = 0.10 fails at m = 7 | `KILLED_MULTIPLICITY` | `BH_Q010_FAILS_WITH_INHERITED_CELLS` |
| 9a | confirmation identity coverage < 85 % or < 60 included deals | `DATA_HOLD` | `DATA` |
| 9b | confirmation net ≤ 0 | `KILLED_UNSTABLE` | `CONFIRMATION_SIGN_REVERSED` |
| 9c | confirmation NW t < 2.0 | `NO_EDGE` | `CONFIRMATION_NW_T_BELOW_2` |
| 9d | confirmation net < 1.5 %/yr | `KILLED_BELOW_MATERIALITY` | `CONFIRMATION_NET_BELOW_1.5PCT_PER_YEAR` |
| 10 | otherwise | `QUALIFIED` — a HUMAN gate only | — |

`capital_eligible` is always False. Additions beyond the catalog rule (the data floors, the PIT check, the
confirmation gates) can only close, never rescue. The governed state reported to the frontier is QUALIFIED,
DATA_HOLD, or NO_EDGE (every `KILLED_*` verdict and `NO_EDGE`).

## 11. COSTS AND THE HURDLE

12.5 bp per side (`EQ_COST_RATE_PER_SIDE`) on entry and exit notional. Ladder, descriptive only: 0 / 12.5 / 25 /
50 bp.

**The hurdle, from the census before any return.** At 50 slots the qualification census exposure averages 37.4 % of
NAV and the round-trip cost drag is about 0.43 %/yr of NAV. The 1.5 %/yr materiality floor therefore needs about
1.5 / 0.374 ≈ **4.0 %/yr net excess return on the capital deployed** — after costs, the bill and every failed deal —
or about 5.2 %/yr before costs. With a median hold of 43 sessions that is roughly **0.9 % gross excess return per deal
on average, net of all break losses**. A spread percentage is never the result; the calendar-time book is.

## 12. DESCRIPTIVE ONLY — never a gate

The cost ladder; volatility, Sharpe and drawdown; exposure; the share of exits by last quoted session versus the
cap; the per-deal return distribution and loss shares; the capacity and no-entry-price counts; β; the per-half
statistics beyond their sign.

## 13. FORBIDDEN AFTER RESULTS

* Changing any parser pattern, the name guard, the OTC rule, the episode gap, the 120-day establishing window or
  the deep-read rule; adding an identity route (CUSIP, name matching that creates a match, a current ticker map).
* Early exit on a termination, a spread or a price; a stop-loss; a cap other than 126; entering at the establishing
  close; shorting the acquirer; including stock, election, CVR or partial deals; a sign reversal.
* Slot counts other than 50; leverage; size or spread weighting; excluding a year, a sector or a deal.
* A cost below 12.5 bp; dropping the bill; a benchmark other than beta-matched SPY excess; NW lag ≠ 10.
* Lowering the 98 % / 90 % / 85 % / 150 / 60 floors; resetting m or dropping an inherited cell.
* Reading the confirmation before every qualification gate passes, or on another window.

## 14. FROZEN ARTIFACTS

| artifact | identity |
|---|---|
| the included deal list — `merger_arb_events.calendar_hash` (sorted target CIK, establishing date, Norgate symbol); executor `CALENDAR_HASH`, checked at run time (gate 1) | `7ce9ec4e479dfb7fbcb8e3756f1d2966b8e09a14180c8ea4fed5273f5d309f92` |
| the classification audit, both rounds with every sampled row — `_data_sec_merger\classification_audit.json`; executor `AUDIT_SHA256`, checked at run time (gate 1) | `d58ce030c21fc0277517a60967bab499ab4611c92dc3dc1fc3b6478d0936744b` |
| parsed filings — `_data_sec_merger\parsed_filings.jsonl` (15,668 records) | `1667339695af269492269d2e1003cb2ebe2b8db009a1ab22f41d012be42a045c` |
| Norgate universe — `_data_sec_merger\norgate_us_equity_universe.csv` (41,917 securities) | `a9512699dc4e90ff541e5dd9b81f9293a871b3e71a3f757df2aa3179efc02601` |
| the 99 quarterly index extracts, 2002Q1–2026Q3 (file name and bytes, in quarter order) | `3f4da1a145179355c539ad20842f3d2b86cb84dd5a01214e49e03e447e286ef2` |
| executor constants | `SLOTS = 50`, `HOLD_CAP = 126`, 12.5 bp per side, NW lag 10, m = 7, q = 0.10 |

The calendar hash alone fixes the traded sample: no parser, identity or data change after this commit can alter it
without the executor holding. The executor module and this document are pinned by sha256 in the catalog entry's
`executor` block after both are committed; any later edit to either un-pins the experiment.

## 15. SAFETY

RESEARCH ONLY. No purchase, no subscription, no registration, no promotion, no capital allocation, no portfolio
mutation, no proposal, no order, no fill, no live write. Writes land only under the campaign research root
(`_data_sec_merger\…`, `results/merger_arbitrage_cash_deal_target_spread.json`). The live checkout is not touched.
