# PREREGISTRATION - BOND INDEX MONTH-END DURATION EXTENSION

**Mechanism id:** `TREASURY_INDEX_MONTH_END_DURATION_EXTENSION_V1`
**Catalog:** `research/alpha_agent/MECHANISM_FRONTIER.json` (data_version 1)
**Domain / class:** `STRUCTURAL_FLOWS_FORCED_TRADING` / `INDEX_FUND_DEALER_FLOW`, asset class `RATES_FUTURES`
**Executor:** `alpha_agent.alpha_recovery.treasury_month_end_duration_extension.run_mechanism`
**Instrument:** ZN, one unit of notional. **One cell, m_declared = 1.**
**Status:** FROZEN. Written BEFORE any futures return, P&L, book, t-statistic or hit rate was
computed on real data for this mechanism or for its unconditional month-end baseline. The only
real-data computations behind this document are the census of section 2: issuance fields, the
duration-added measure, PIT exclusions, ranking counts, session dates, held contracts, a
finiteness flag on the layer's return column, and owned per-market costs. The executor was
exercised end to end on SYNTHETIC data only (`tests/test_treasury_month_end_duration_extension.py`).
No other mechanism's result artifact was opened.

This document is the contract. It is not edited after results exist. If a statement here
turns out to be inconvenient, the inconvenience is the result.

---

## 0. THE P&L MECHANISM

Bond index providers add newly issued Treasuries to their indices only at the month-end
rebalancing. Passive and benchmarked funds are penalised for tracking error, so they buy the
added duration at the month-end close regardless of price. Months in which settled issuance adds
more duration should therefore see a larger lift in Treasury prices into that close.

**Direction: the LONG EARNS, frozen.** A long ZN position held over the last two sessions of
months ranked high on the duration that month's settled issuance adds has positive expected
return. It must also earn more than the same long held in every month. A contradicted sign closes
the mechanism; it is never reversed.

### 0.1 Adjacent estate tests - inherited at p = 1

| family | what it traded | why it is not this test |
|---|---|---|
| `R32_EVENT_DRIVEN_CALENDAR` | unconditional turn-of-month and quarter-end dummies on equity exposure | never sized a bond position by the duration issuance adds |
| `TREASURY_AUCTION_SUPPLY_CONCESSION_V1` | short the auctioned tenor into the auction | same issuance calendar, opposite side of the flow: dealer supply before the auction, not benchmark demand after settlement |

Both enter the multiple-testing burden (section 11).

### 0.2 The frozen kill rule (catalog `pnl_gate.KILL_RULE`, verbatim)

> Kill if the extension-ranked month-end long book has NW t < 2.0 or net below 1.5 %/yr at the
> R38 per-market cost (2 bp per side); or its increment over an unconditional month-end long of
> the same futures has NW t < 2.0; or it fails BH q=0.10 over m=3 with R32_EVENT_DRIVEN_CALENDAR
> and TREASURY_AUCTION_SUPPLY_CONCESSION_V1 inherited at p=1.

The executor refuses to run against any other kill-rule text, byte for byte, before it touches
any data.

---

## 1. DATA - FREE, CACHED, AND OWNED ($0)

### 1.1 Issuance - TreasuryDirect TA_WS (cache owned by the sibling executor)

* Cache: `D:\Stock_Prediction_app_data\alpha_recovery_offensive\_data_treasury_auctions\ta_ws_notes_bonds_2000_2026.json`,
  **1,635 rows, sha256 `019bea4fdc6b53b9a77c5b2cdfaa8433930d5e0415a8f497659185b0ac29d186`**, fetched
  2026-09-13T22:28:17Z by `treasury_auction_concession.fetch_auctions`. It is read through that
  owner's `load_auctions(allow_fetch=False)` with its manifest check. **This executor never fetches.**
  A missing or tampered cache is `DATA_HOLD`. No network request was made for this document.
* Fields used: `cusip`, `securityType`, `securityTerm`, `reopening`, `tips`, `floatingRate`,
  `auctionDate`, `issueDate`, `maturityDate`, `interestRate`, `offeringAmount`. All are present on
  every row; **0 index-supply issues lack a field in either window**.
* Not used: `totalAccepted`, `somaAccepted`, bid-to-cover, yields, dealer take-up, and any paid
  index-extension projection.

### 1.2 Futures - the owned R38 native contract layer

`D:\Stock_Prediction_app_data\native_futures_r38\r38_native_futures_information_frontier_v4\native_contract_layer\ZN.csv`,
read through `alpha_agent.r59.native` paths and `treasury_auction_concession.load_bars`.

* `ret` is the return of the held dated contract; returns are excess of collateral.
* Roll policy `OBSERVABLE_FIRST_NOTICE_LAST_TRADE`, with buffers of 2 / 5 sessions.
* ZN: 11,152 rows, 1982-05-03 to 2026-08-21, layer sha256 `cc37a3e4...`.
* ZF, ZN and ZB have identical session calendars from 2000.

Census artifact: `results/treasury_month_end_duration_census.json`, schema
`alpha_recovery_treasury_month_end_duration_census/1`, written by
`treasury_month_end_duration_extension.run_census` (`reads_returns: false`).

---

## 2. CENSUS - no return

### 2.1 Index supply and the PIT exclusion

An issue is "PIT-excluded" when its auction falls on or after its issue month's entry session
(section 3.4). DA is the duration-added measure of section 3, in $bn x years.

| window | months | index-supply issues | PIT-excluded issues (2y / 5y / 7y) | months with an exclusion | DA share PIT-excluded |
|---|---|---|---|---|---|
| QUALIFICATION 2000-2016 | 204 | 854 | **101** (25/203 · 37/175 · **39/96**) | 73 | **11.9 %** |
| H1 2000-2008 | 108 | 284 | 40 (25 · 15 · 0) | 34 | 10.7 % |
| H2 2009-2016 | 96 | 570 | 61 (0 · 22 · 39) | 39 | 12.2 % |
| CONFIRMATION 2017-2026-07 | 115 | 770 | **39** (0 · 0 · **39/116**) | 39 | **5.4 %** |

* The 3y, 10y, 20y and 30y settle mid-month, and no issue in those tenors is ever PIT-excluded.
* Issue month differs from auction month for 2y 93, 5y 73, 7y 63 and 20y 23 issues. These are
  month-end notes that settled on the first business day of the next month. They are assigned by
  `issueDate` (section 3.3).
* **Agreement between the PIT high/low flag and a full-information flag** (same rule, excluded
  issues added back): 88.0 % (Q), 92.7 % (H1), 83.3 % (H2), 92.2 % (C).

The mechanism IS measurable point in time. The PIT rule changes the flag in 12 % of qualification
months, and it does so conservatively, by omission only.

### 2.2 The measure's distribution and scale

| window | DA per month: p10 | median | p90 | max | amount-weighted duration of counted issues |
|---|---|---|---|---|---|
| QUALIFICATION | 47 | 336 | 1,223 | 1,467 | 5.47 yr |
| H1 | 0 | 127 | 319 | 617 | 4.13 yr |
| H2 | 506 | 711 | 1,399 | 1,467 | 5.88 yr |
| CONFIRMATION | 699 | 1,274 | 2,572 | 3,670 | 6.16 yr |

Annual DA rose from about 820 (2000) to about 11,600 (2012) and 26,900 (2021). **The level is not
stationary**, so any fixed threshold or long lookback is a regime detector, not a ranking
(section 4). At least 10 % of H1 months have no PIT-countable issuance.

### 2.3 Which futures bucket the added duration sits in

Level share, then variance contribution `cov(bucket, total) / var(total)`. The buckets are the
deliverable ranges: ZT = 2y/3y, ZF = 5y/6y, ZN = 7y/9y/10y, ZB = 20y/30y.

| window | ZT | ZF | ZN | ZB |
|---|---|---|---|---|
| QUALIFICATION | 20.2 / 15.8 % | 17.6 / 22.0 % | **35.4 / 37.8 %** | 26.8 / 24.5 % |
| H1 | 27.5 / 24.4 % | 27.4 / 23.9 % | **34.3 / 34.4 %** | 10.8 / 17.3 % |
| H2 | 18.6 / 12.8 % | 15.5 / 40.8 % | **35.6 / 42.2 %** | 30.2 / 4.2 % |
| CONFIRMATION | 15.5 / 12.1 % | 16.5 / 23.5 % | 30.9 / 31.4 % | **37.1 / 33.0 %** |

### 2.4 Ranking lookback alternatives (measured, NOT tested)

Rule: high iff DA_M > median of the previous L months (section 4). The measure starts 2000-01.

| L | first ranked | Q ranked | Q high share | longest Q high run | H1 / H2 high share | C high share | longest C run |
|---|---|---|---|---|---|---|---|
| **12 (frozen)** | 2001-01 | **192** | **53.6 %** (103) | **8** | 55.2 / 52.1 % | **52.2 %** (60/115) | 4 |
| 24 | 2002-01 | 180 | 57.2 % | 23 | 56.0 / 58.3 % | 54.8 % | 5 |
| 36 | 2003-01 | 168 | 61.3 % | 37 | 61.1 / 61.5 % | 61.7 % | 16 |

High months by month of year (L = 12, qualification): Jan 6/16, **Feb 15/16**, Mar 5/16, Apr 6/16,
**May 11/16**, Jun 7/16, Jul 6/16, **Aug 13/16**, Sep 7/16, Oct 7/16, **Nov 13/16**, Dec 7/16.
Confirmation: Feb 8/10, May 6/10, Aug 6/9, Nov 4/9, other months 3-6 of 9-10. The quarterly
refunding months (new 10y and 30y) carry the ranking.

### 2.5 Session calendar, rolls and missing sessions (ZF, ZN, ZB identical)

* **Held-contract change inside [L[-3], L[-1]]:** in **every** Feb, May, Aug and Nov (first-notice
  months), in all three markets, every year; never in any other month.
  * QUALIFICATION: **68 of 204 months (33.3 %)**. 60 changes fall between L[-3] and L[-2]; 8 fall
    between L[-2] and L[-1].
  * CONFIRMATION: **38 of 115 (33.0 %)**, split 34 / 4.
  * No change ever falls on the entry session itself (between L[-4] and L[-3]).
* **Rolls fall on high months:** a roll lands in 50.5 % of high months against 13.5 % of low months
  (Q, L = 12). The figures are 58.5 / 2.3 % in H1, 42.0 / 23.9 % in H2, and 40.0 / 25.5 % in C.
* **Missing returns in a window session:** 0 in every month, 2000-01 to 2026-07. The minimum
  number of sessions in any month is 18.
* **Last layer session is not the last weekday of the month:**
  * Good Friday months, where the layer's last session is Thursday: 2002-03, 2013-03, 2018-03,
    2024-03.
  * Memorial Day months, where the last weekday is a holiday: 2004-05, 2010-05, 2021-05.
  * 2026-08: the layer ends 2026-08-21, so the month is not complete and not measured.
    **The last usable month is 2026-07.**

### 2.6 Owned cost

R38 median `cost_bps_per_side` is 2.0 for ZT, ZF, ZN, ZB, UB and TN (`alpha_agent.r59.native.load_meta`).
The cost model is marked `MODELLED_NOT_OBSERVED`.

---

## 3. THE DURATION-ADDED MEASURE - frozen

### 3.1 Which securities count

Every nominal fixed-coupon Treasury note and bond auction in the cache counts, new issues and
reopenings alike, for every remaining term (2y, 3y, 5y, 7y, 10y, 20y, 30y and off-cycle terms).
All of them are index-eligible supply.

Excluded, using the sibling's `classify`:

* TIPS (a separate index);
* FRNs (no duration);
* any row that is not a Note or Bond, or whose term cannot be parsed;
* bills and CMBs (not fetched; below the index's one-year floor).

The sibling's 2y / 3y / 7y / 20y tenor exclusions are a tradability decision for a matched short.
They do **not** apply here, because index funds buy every tenor.

### 3.2 Amount and duration

```
DA_i = offeringAmount_i / 1e9  x  D_i                                      ($bn x years)
D_i  = (1 - (1 + y/2)^(-2T)) / y,   y = interestRate / 100,  T = (maturityDate - issueDate) / 365.25
       (par-bond modified duration, semi-annual coupons; D = T when y = 0)
```

* **`offeringAmount`, not `totalAccepted`.** The public offering excludes SOMA add-ons: the Fed,
  not benchmarked funds, holds those. The offering is also fixed at announcement.
  `totalAccepted / offeringAmount` has a median of 1.055 and a maximum of 1.70, driven by SOMA.
* **Duration from coupon, maturity and issue date only.** No price, yield or curve is read. A
  reopening's price away from par is ignored: this is a declared approximation.

### 3.3 Which month

An issue belongs to month M iff its `issueDate` (settlement) is in M. Index providers add
securities that have settled by the rebalancing date. A month-end note that settles on the 1st of
M+1 belongs to M+1.

### 3.4 Point in time

```
DA_M = sum of DA_i over index-supply issues with issueDate in M AND auctionDate < date(L_M[-3])
```

* An issue is counted only if its auction date is **strictly before the entry session**. Coupon and
  size are then both known at the entry close.
* An issue auctioned on or after the entry session is **dropped from DA_M**. It is not moved to
  M+1, since the index adds it at the end of M.
* Section 2.1 counts these exclusions: 101 issues in Q and 39 in C.
* The trailing median (section 4) uses the same PIT-truncated DA of prior months, so the comparison
  is like for like.
* No later issuance, auction result or index statistic is read.

---

## 4. RANKING RULE - frozen

```
high_M = DA_M > median(DA_{M-12}, ..., DA_{M-1})        (strictly exceeds; prior calendar months only)
```

A month is unranked if any of its 12 prior months is unmeasured. The first ranked month is
2001-01, since the cache starts 2000-01.

**Why 12 months, from census alone (section 2.4):**

1. **The level is non-stationary.** DA rose about 13x from 2000 to 2012 and doubled again by 2021.
   * L = 36 flags **37 consecutive months** high in Q (through the 2008-2010 issuance surge), and
     L = 24 flags 23. Across those runs the "ranked" book is the unconditional long it must beat.
   * L = 12 never flags more than 8 consecutive months (4 in C).
2. **Balance.** L = 12 keeps the high share closest to one half in every window (53.6 / 55.2 / 52.1 /
   52.2 %), so both the book and the increment have a large low-month control.
3. **Sample.** It keeps 192 of 204 qualification months, against 168 for L = 36. The cache is not
   extended backwards.
4. **Seasonality.** It is the shortest window that contains every month of the quarterly refunding
   cycle equally (three full cycles), so a refunding month is always compared with a balanced year.

---

## 5. INSTRUMENT - ZN, frozen

**One unit of ZN notional. No DV01 basket and no second instrument.**

* **ZN is the largest bucket of the added duration in the qualification window**, by level
  (35.4 %) and by variance contribution (37.8 %). It is also the largest in both halves.
* ZN is at least 30 % by both measures in every window. ZB leads only in confirmation (37.1 %
  level), which may not drive a qualification-time choice.
* The amount-weighted duration of counted issues is 5.47 yr (Q) and 6.16 yr (C). ZN (about 6.2,
  section 7 of the sibling preregistration) is the nearest single contract; ZF is about 4.3 and ZB
  about 13.
* ZN is named in the catalog's data (TY).
* A ZF/ZN/ZB DV01 basket would add weights with no qualification-window history to justify them.

ZB (UB from 2010) was not chosen for its larger per-notional move: materiality is judged per unit
of the instrument actually chosen.

---

## 6. POSITION, ENTRY, EXIT - frozen

For calendar month M with sessions L (ZN's own R38 bar dates; no `engine` import):

```
usable      M complete (a layer session exists after L[-1]), >= 10 sessions, ranked, ret finite on L[-2], L[-1]
entry       close of L[-3]      (two sessions before the month-end session)
exit        close of L[-1]      (the month-end close, the index rebalancing close)
gross_M     (1 + ret[L[-2]]) (1 + ret[L[-1]]) - 1
position    LONG one unit of ZN notional if high_M, flat otherwise
```

* The month-end close is the layer's last session of M. In the four Good Friday months it is
  Thursday.
* Windows never overlap (one per month), so every statistic uses **Newey-West lag 0**.

---

## 7. ROLLS AND COSTS - frozen

```
cost_M(c) = (2 + 2 x rolls_M) x c / 1e4          rolls_M = held-contract changes between consecutive sessions in [L[-3], L[-1]]
book_M(c) = gross_M - cost_M(c)   if high_M,   0 otherwise
u_M(c)    = gross_M - cost_M(c)                  (the unconditional month-end long, every usable month)
```

* **Roll handling: one extra round trip per held-contract change, as the sibling executor does.**
  The alternative, skipping roll months, would delete every February, May, August and November,
  and those are the refunding months that carry the ranking (section 2.4).
* **The rule is conservative.** 60 of the 68 qualification charges (34 of 38 in confirmation) are
  changes between L[-3] and L[-2], which a direct entry in the new contract would avoid. The charge
  is kept anyway, for identity with the sibling on the same layer.
* **Gate cost** `c = max(2.0, R38 ZN cost)` = **2.0 bp per side**, as stated in the kill rule.
* **Stress** is 5.0 bp per side. **Gross** (0 bp) is reported.

**Cost hurdle, from the census, per unit of ZN notional (no return involved):**

| window | book drag at 2 bp | gross needed for 1.5 %/yr net | book drag at 5 bp | gross needed at stress | unconditional long drag at 2 bp / 5 bp |
|---|---|---|---|---|---|
| QUALIFICATION | **0.39 %/yr** | **1.89 %/yr** | 0.97 %/yr | **2.47 %/yr** | 0.64 / 1.60 %/yr |
| H1 | 0.42 | 1.92 | 1.05 | 2.55 | 0.64 / 1.60 |
| H2 | 0.36 | 1.86 | 0.89 | 2.39 | 0.64 / 1.60 |
| CONFIRMATION | 0.35 | 1.85 | 0.88 | 2.38 | 0.64 / 1.60 |

In qualification the book is active 6.44 months a year. It must therefore earn about **29 bp gross
per active two-session window** at 2 bp per side, and **38 bp** to survive the stress rung.

---

## 8. MEASUREMENT - existing owners only

* **Book:** the monthly `book_M(2 bp)` series over the window's usable ranked months, zeros
  included.
  * `alpha_agent.r63.sensitivity.nw_tstat(x, 0)`.
  * Annualised = mean x 12.
  * Drawdown via `S._max_dd`.
* **Increment over the unconditional month-end long (the frozen definition):**

  ```
  z_M = (1[high_M] - pi_W) x u_M(2 bp),     pi_W = share of high months among the window's usable ranked months
  ```

  * `nw_tstat(z, 0)`. The mean is `pi_W (1 - pi_W) (mean u | high - mean u | low)`.
  * `pi_W` is a calendar count, not a return. Using the window share makes the timing book's
    unconditional exposure net to exactly zero over the window, so the unconditional month-end
    premium cannot leak into the increment. A running share would leave residual exposure.
  * `u` is net, so the extra roll cost that falls on high months (section 2.5) is charged against
    the timing claim. The gross increment is descriptive.
* **Equal-risk increment over the incumbent:** `alpha_agent.alpha_recovery.intraday_alpha.equal_risk_daily`.
  Each high month's net return is spread evenly (geometrically) over its two window sessions; all
  other days are 0.
* **Multiplicity:** `alpha_agent.r63.sensitivity.bh_fdr`, q = 0.10 (section 11).
* **No second scorer.** `run_cell` is not used: a one-instrument calendar book has no cross-section
  or baseline block.

## 9. DESCRIPTIVE ONLY - never a gate

The following are reported but never gate the verdict:

* the gross book;
* the 0 / 2 / 5 bp ladder;
* the gross increment;
* the unconditional long's own statistics;
* the hit rate of active months;
* the maximum drawdown;
* roll shares.

---

## 10. GATES - in this order

The QUALIFICATION window runs over months 2000-01 to 2016-12. Ranked months are effectively
2001-01 to 2016-12, 192 expected. The halves are 2000-2008 (effectively 2001-2008, 96 months) and
2009-2016 (96 months).

The CONFIRMATION window runs over months 2017-01 to 2026-08, 116 expected and 115 complete. **It is
read ONLY if gates 1-9 pass.** Otherwise `window_rows` is never called for it, the artifact carries
`"confirmation": "UNTOUCHED"`, and `lockbox_t` is null.

| # | gate | verdict | `kill_rule_fired` | at freeze (census) |
|---|---|---|---|---|
| 1 | cache or ZN layer missing/tampered; ZN history starts after 2000-01-01; usable ranked months < 95 % of expected; > 2 % of index-supply issues lack a field | `DATA_HOLD` | - | 0 missing fields, 0 missing window returns |
| 1b | high or low usable months < 36 | `KILLED_INSUFFICIENT_SAMPLE` | `INSUFFICIENT_SAMPLE` | 103 high / 89 low |
| 2 | mean gross return of high months <= 0 | `KILLED_WRONG_SIGN` | `LONG_DOES_NOT_EARN` | - |
| 3 | book NW t < 2.0 at 2 bp | `NO_EDGE` | `NW_T_BELOW_2` | - |
| 4 | book net < 1.5 %/yr at 2 bp | `KILLED_BELOW_MATERIALITY` | `NET_BELOW_1.5PCT_PER_YEAR` | - |
| 5 | increment `z` NW t < 2.0 | `KILLED_NONINCREMENTAL` | `NO_INCREMENT_OVER_UNCONDITIONAL_MONTH_END` | - |
| 6 | annualised book net <= 0 in either half | `KILLED_UNSTABLE` | `HALF_NOT_POSITIVE` | - |
| 7 | BH q = 0.10 fails at m = 3 | `KILLED_MULTIPLICITY` | `BH_Q010_FAILS_M3` | - |
| 8 | book net < 1.5 %/yr at 5 bp | `KILLED_BELOW_MATERIALITY` | `NET_BELOW_1.5PCT_PER_YEAR_AT_STRESS_COST` | - |
| 9 | equal-risk increment over the incumbent: state OK and not positive after costs | `NO_INCREMENTAL_INFORMATION_EDGE` | `NO_EQUAL_RISK_INCREMENT_OVER_INCUMBENT` | - |
| 10a | confirmation: usable < 95 % or > 2 % missing fields | `DATA_HOLD` | - | 115 / 116 usable |
| 10b | confirmation high months < 36 | `NEED_MORE_EVIDENCE` | - | 60 high |
| 10c | confirmation fails ANY of: book mean > 0, net >= 1.5 %/yr, NW t >= 2.0, one-sided p <= 0.10, increment mean > 0 | `KILLED_UNSTABLE` | `CONFIRMATION_DID_NOT_REPRODUCE` | - |
| 11 | otherwise | `QUALIFIED` (**HUMAN gate**) | - | - |

**`capital_eligible` is always False.** `QUALIFIED` opens only the human-gated prospective
registration; capital needs forward evidence and governance.

**Notes declared before results:**

* **Gate 2 is gross.** The mechanism's sign is a property of price, not of cost; a cost failure is
  gates 3, 4 and 8.
* **Gate 7 cannot bind once gate 3 passes.** With two inherited p = 1 nulls, BH passes iff
  p <= 0.0333, and t >= 2.0 already implies a one-sided p <= 0.0228. It is kept because the kill rule
  names it and the denominator must be recorded.
* **Gate 9, when the incumbent path is unavailable,** records state `DATA_HOLD` and does not block,
  as in the commodity and month-end siblings.
* **In confirmation (gate 10c), the increment is required only in sign.** The window has 60 high
  months, about half the qualification sample.

**Gates stricter than the catalog kill rule, declared as additions:** 1b, 6, 8, 9 and 10, plus the
roll round trip. None loosens a catalog condition.

## 11. MULTIPLE TESTING

The p-value is the one-sided NW p (lag 0) of the QUALIFICATION book at 2 bp. The family is one
cell (m_declared = 1). `R32_EVENT_DRIVEN_CALENDAR` and `TREASURY_AUCTION_SUPPLY_CONCESSION_V1` are
inherited at p = 1, giving **m = 3**, q = 0.10. The denominator is never reset.

## 12. FALSIFICATION - and what will NOT be done

A null result means one of two things. Either a long ZN position over the last two sessions of
months whose PIT-settled issuance adds more duration than the trailing year's median does not pay
after costs. Or it pays no more than being long at every month end.

**Forbidden after results:**

* reversing the sign, or shorting low months;
* retuning the lookback (24 and 36 are recorded above precisely so they cannot be adopted later);
* a different threshold, quantile, z-score, continuous sizing by DA, or conditioning on the
  refunding months directly;
* another entry session, exit session or holding length, or trading into the next month;
* changing the PIT rule, including an announcement-based measure, adding PIT-excluded issues back,
  or using the issue close;
* `totalAccepted` or SOMA-inclusive amounts, a market-value, yield- or curve-based duration, or
  netting out maturing or roll-off securities;
* dropping tenors, reopenings or years (e.g. 2008, 2020), or skipping roll months;
* a second instrument (ZF, ZB, UB, TN), a DV01 basket, or volatility scaling;
* removing the roll round trip or charging less than 2 bp at the gates;
* a running pi or a regression beta in place of the frozen increment;
* moving the windows (2000-2016 / 2017-2026) or the halves;
* reading the confirmation window after any qualification failure;
* **any second cell.**

## 13. DECISIONS LEFT TO THE LEAD (before any return)

1. **The refunding calendar carries the ranking.** In qualification, Feb/May/Aug/Nov are high 52
   of 64 times; H1 is 31 of 32. Those months are also the only roll months. The increment test is
   against the unconditional month end, not against a refunding-month dummy. A pass would therefore
   be a "refunding month-end" effect as much as a sized-duration effect. This document does not add
   a refunding-dummy control, because that dummy is the mechanism's main variation.
2. **The PIT rule omits 39 of 96 qualification 7y notes and all 39 confirmation exclusions.**
   Their size and maturity are public at announcement, before entry; only the coupon is not. An
   announcement-based measure was not adopted, because the brief froze "auction strictly before
   entry".
3. The executor's catalog pin (`module_sha256`, `preregistration_sha256`) is set by the lead after
   this document and the executor are committed.

**Lead decisions, recorded before any return:**

* **(1) No refunding-dummy control is added.** Refunding months are where the index gains its duration,
  so that variation is the mechanism itself. The verdict therefore cannot claim more than a
  "month-end long in high-supply months" effect. **If the verdict is QUALIFIED,** the result
  document must report the book and the increment split between refunding (Feb/May/Aug/Nov) and other
  high months, and between roll and non-roll months, before any registration is proposed. That split
  is descriptive, never gates, and cannot rescue any other verdict.
* **(2) The strict PIT rule stands.** An announcement-based measure is forbidden (section 12).
* **Gate 7 cannot bind (section 10).** This is accepted and kept on record, not replaced with a
  stricter invented threshold.

## 14. SAFETY

RESEARCH ONLY. No purchase, no subscription, no licence, no network request, no model promotion, no
registration, no capital allocation, no portfolio mutation, no proposal, no order, no fill, no live
deployment, no live write. Writes land only under the campaign research root (the census and result
artifacts). The live checkout is not touched.
