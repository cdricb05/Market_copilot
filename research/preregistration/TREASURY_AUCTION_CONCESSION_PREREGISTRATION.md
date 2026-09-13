# PREREGISTRATION — TREASURY AUCTION SUPPLY CONCESSION

**Family id:** `TREASURY_AUCTION_SUPPLY_CONCESSION_V1`
**Domain / class:** `STRUCTURAL_FLOWS_FORCED_TRADING` / `FORCED_TRADING_FLOW`, asset class `RATES_FUTURES`
**Executor:** `alpha_agent.alpha_recovery.treasury_auction_concession.run_mechanism`
**Legs:** 5y → ZF, 10y → ZN, 30y → ZB. **The 2y leg is EXCLUDED before any result exists** (§2.3, §3).
**Status:** FROZEN. Written BEFORE any forward return, book or t-statistic for this family
was computed on real data. The only real-data computations behind this document are the
calendar census of §2 (auction dates, announcement dates, futures session dates, held
contracts) and the owned cost values of §11. The executor was exercised end to end on
SYNTHETIC data only (`tests/test_treasury_auction_concession.py`).

This document is the contract. It is not edited after results exist. If a statement here
turns out to be inconvenient, the inconvenience is the result.

---

## 0. WHY THIS AXIS, AND WHAT IT IS NOT

Every public, slow disclosure the estate scored (13F, 13D/G, 8-K, Form 4, fails-to-deliver)
was priced at or before a legitimate entry. This family is deliberately not information: it
is a **scheduled, price-insensitive supply shock** intermediated by balance-sheet-limited
dealers.

**Economic hypothesis.** The Treasury sells a pre-announced size of one tenor on a fixed date
regardless of price. Primary dealers must bid and then warehouse the supply until end
investors absorb it. To be paid for that inventory risk, the auctioned tenor cheapens in the
sessions before the auction and recovers after it (Lou, Yan and Zhang 2013). The concession
is compensation for risk that capital-limited dealers cannot arbitrage away, so it need not
vanish once the calendar is known.

**Direction: the SHORT EARNS, frozen.** A DV01-matched short in the matched-tenor future,
held into the auction-day close, has positive expected return. A contradicted sign closes the
family; it is never reversed.

### 0.1 Adjacent estate tests — inherited at p = 1

| family | what it traded | result |
|---|---|---|
| `PRIMARY_DEALER_POSITIONS` (R40) | one market-wide weekly dealer-positions series | increment null: one series cannot rank markets |
| `RATES_CARRY_CURVE_RV` | rates carry and curve slope | closed (catalog ledger) |

Neither timed a scheduled supply event, and no estate test used the Treasury auction calendar
(catalog sweep 2026-09-13). Both enter the inherited burden (§12).

### 0.2 The frozen kill rule (catalog `pnl_gate.KILL_RULE`, verbatim)

> Kill if the per-auction DV01-matched short book has NW t < 2.0 or net below 1.5 %/yr at the
> R38 per-market cost (2 bp per side); or the concession is already complete between
> announcement and entry (announcement-to-entry return carries the whole effect); or the
> 5y/10y/30y tenor legs disagree in sign; or it fails BH q=0.10 at m=1 with
> PRIMARY_DEALER_POSITIONS and RATES_CARRY_CURVE_RV inherited at p=1.

The executor refuses to run against any other kill-rule text.

---

## 1. DATA — FREE, CACHED ONCE, AND OWNED

### 1.1 Auction calendar — TreasuryDirect TA_WS

`https://www.treasurydirect.gov/TA_WS/securities/search?format=json&type={Note|Bond}&dateFieldName=auctionDate`,
**one GET per type per calendar year 2000–2026 (54 requests).** A single full-history request
to the fiscaldata mirror (`auctions_query`) timed out twice on 2026-09-13; the mirror is not
used. No key, no account, `paid_dollars: 0`.

* Cache: `D:\Stock_Prediction_app_data\alpha_recovery_offensive\_data_treasury_auctions\ta_ws_notes_bonds_2000_2026.json`,
  **1,635 rows, sha256 `019bea4fdc6b53b9a77c5b2cdfaa8433930d5e0415a8f497659185b0ac29d186`**,
  fetched 2026-09-13T22:28:17Z. `fetch_manifest.json` records every URL, `fetched_at_utc`,
  row count and response sha256.
* A present cache is never re-downloaded; a cache that disagrees with its manifest is
  `DATA_HOLD`, never refetched or substituted.
* Fields used: `cusip`, `securityType`, `securityTerm`, `originalSecurityTerm`, `reopening`,
  `tips`, `floatingRate`, `announcementDate`, `auctionDate`. **`offeringAmount` is not used**
  (§3).
* `type=Note` / `type=Bond` returns nominal coupons only: TIPS and FRNs are not in the cache
  (measured: every returned row has `tips = No`, `floatingRate = No`). The classifier still
  excludes them defensively.

Census artifact `results/treasury_auction_census.json`, schema `alpha_recovery_treasury_auction_census/2`,
5y/10y/30y scope with the pre-decision four-tenor scope kept as a reference block
(`artifact_hash 18acd6d25f66f13425b34fd443eef8a72bff1c195f8e3ddb50eb18f81eb6b81b`). The census
calendars carry session dates and held contracts only; no return array is passed to them.

### 1.2 Futures — the owned R38 native contract layer

`D:\Stock_Prediction_app_data\native_futures_r38\r38_native_futures_information_frontier_v4\native_contract_layer`,
read through the R59 owner's paths (`alpha_agent.r59.native`). Column `ret` is the return of
the **held dated contract** (roll handled inside the layer, not a vendor continuous series);
`returns_are_excess_of_collateral: true`; roll policy `OBSERVABLE_FIRST_NOTICE_LAST_TRADE`
(buffers 2 / 5 sessions).

| market | tenor | first bar | last bar | rows | contracts | sessions 2000–2016 | sessions 2017→2026-08-21 | missing `ret` 2000–2026 | layer sha256 | used |
|---|---|---|---|---|---|---|---|---|---|---|
| ZT | 2y | 1990-06-22 | 2026-08-21 | 9,094 | 145 | 4,273 | 2,423 | 0 | `86ead3e3…` | **no — 2y excluded (§3)** |
| ZF | 5y | 1988-05-20 | 2026-08-21 | 9,622 | 154 | 4,273 | 2,423 | 0 | `71086494…` | yes |
| ZN | 10y | 1982-05-03 | 2026-08-21 | 11,152 | 178 | 4,273 | 2,423 | 0 | `cc37a3e4…` | yes |
| ZB | 30y | 1978-03-10 | 2026-08-21 | 12,196 | 187 | 4,273 | 2,423 | 0 | `928bbf65…` | yes |

Largest calendar gap between sessions 2000–2026: 4 days. `UB` begins 2010-01-11 and `TN`
2016-01-11: **neither covers the qualification window and neither is used** (§3).

**No owned market or history needed by the frozen legs is missing. No DATA_HOLD condition
exists at freeze.**

---

## 2. THE CALENDAR — census, no return

### 2.1 Scope and exclusions (auctions by `auctionDate`)

The 2y column is kept as a measured fact; the 2y leg is **excluded before results** (§2.3).

| window | 2y (EXCLUDED) | 5y→ZF | 10y→ZN | 30y→ZB | excl. 3y | excl. 7y | excl. 20y | off-curve remaining term |
|---|---|---|---|---|---|---|---|---|
| QUALIFICATION 2000–2016 | 204 | 176 | 155 | 108 | 115 | 97 | 0 | 2 (6y, 9y) |
| CONFIRMATION 2017–2026 | 116 | 116 | 118 | 117 | 117 | 116 | 78 | 0 |

Reopenings are included and classified by the REMAINING term actually auctioned: 10y leg
59% (Q) / 67% (C) reopenings, 30y leg 64% / 67%, 5y leg 4.5% / 5.2%. Remaining-term remaps of
off-tenor originals: `7-Year→5Y` 8 (in scope). `5-Year→2Y` 8 and `7-Year→2Y` 3 are 2y supply and
therefore excluded.

### 2.2 Placement status — frozen 5y/10y/30y scope

| window | mapped | placed OK | announcement not before auction | no holding session after PIT | auction not a futures session | after last futures bar |
|---|---|---|---|---|---|---|
| QUALIFICATION | 439 | **436** | 1 | 2 | 0 | 0 |
| CONFIRMATION | 351 | **346** | 1 | 1 | 0 | 3 |

PIT-missing share: **0.23% (Q), 0.29% (C)**. Unusable share: **0% / 0%**.
(Pre-decision four-tenor scope, for reference: 636 Q / 459 C placed OK.)

Placed-OK auctions per year, 5y / 10y / 30y: 2000 4/4/2 · 2001 4/4/2 · 2002 4/4/0 · 2003 8/6/0 ·
2004 12/8/0 · 2005 10/8/0 · 2006 12/8/2 · 2007 12/8/4 · 2008 12/8/4 · 2009 12/12/10 ·
2010–2019 12/12/12 · 2020 11/12/12 · 2021–2025 12/12/12 · 2026 (to 08-21) 7/8/8. The 30-year
bond was not auctioned from late 2001 to early 2006. (2y, excluded: 11–12 per year throughout.)

### 2.3 Announcement lead, the PIT truncation, and the 2y exclusion

| leg | window | median lead (calendar days) | median lead (sessions, announcement session → auction) | entry deferred by PIT | holding sessions actually available |
|---|---|---|---|---|---|
| 2y ZT **(EXCLUDED)** | Q | 5 (min 2, max 8) | 3 | **100%** | 1: 97 · 2: 72 · 3: 10 · 4: 21 |
| 2y ZT **(EXCLUDED)** | C | 5 (4–6) | 2 | **100%** | 1: 62 · 2: 51 |
| 5y ZF | Q | 6 (1–7) | 4 | **100%** | 1: 19 · 2: 59 · 3: 81 · 4: 15 |
| 5y ZF | C | 6 (4–7) | 3 | **100%** | 1: 26 · 2: 37 · 3: 51 |
| 10y ZN | Q | 6 (2–8) | 4 | 92.9% | 1: 4 · 2: 32 · 3: 54 · 4: 53 · 5: 11 |
| 10y ZN | C | 6 (4–7) | 4 | 100% | 1: 11 · 2: 22 · 3: 51 · 4: 32 |
| 30y ZB | Q | 7 (5–8) | 5 | 58.3% | 2: 2 · 3: 12 · 4: 49 · 5: 45 |
| 30y ZB | C | 7 (5–8) | 5 | 70.7% | 2: 11 · 3: 18 · 4: 53 · 5: 34 |

**Declared now:** the catalog's "from the close five sessions before the auction" is NOT a
legitimate entry for most auctions. The announcement usually comes fewer than five sessions
before the auction. The tested position is therefore the **PIT-truncated** window of §4, which
is never lengthened by entering before the news.

**Why the 2y leg is excluded — a tradability and cost fact, measured before any return:**

1. **Its window is almost never tradable at length.** Under the frozen PIT entry, the 2y window
   is one or two sessions in **169 of 200 qualification events (84.5%)** and in **all 113
   confirmation events (100%)**.
2. **DV01 matching makes it the costliest leg.** A 2y short needs **3.26×** the ZN notional
   (§7). In the four-tenor book at 2 bp per side, the 2y leg would carry **58.7%** of
   qualification round-trip cost (52.4% in confirmation). That total is **2.67 %/yr (Q)** and
   **2.93 %/yr (C)** of ZN-DV01 notional, against **1.10 %/yr** and **1.39 %/yr** without the 2y
   leg.
3. **It would put the whole family at risk for the wrong reason.** No return was computed, so
   nothing measured suggests the 2y concession per unit of DV01 is larger than the other legs'.
   Nothing justifies paying that cost on a one-to-two-session window. Left in, a cost-starved leg
   could kill the whole family through the tenor-sign gate (§10 gate 6), on the strength of its
   tradability, not the mechanism.

The decision was taken by the lead on 2026-09-13 from these census numbers alone. It is not a
result, and it can never be reversed after results (§13).

### 2.4 Overlap — frozen 5y/10y/30y scope

Events whose return sessions intersect are chained into clusters (§6).

| window | placed events | clusters | single | pairs | triples | clusters / year |
|---|---|---|---|---|---|---|
| QUALIFICATION (16.99 yr) | 436 | **295** | 158 | 133 | 4 | 17.4 |
| CONFIRMATION (9.63 yr, to 2026-08-21) | 346 | **230** | 114 | 116 | 0 | 23.9 |

(Pre-decision four-tenor scope, for reference: 368 Q / 230 C clusters.)

A held-contract change falls inside the window in 4.6% of 5y qualification events and in no 10y
or 30y event, in either window.

---

## 3. THE ONE FROZEN CELL — `PIT_TRUNCATED_PRE_AUCTION_SHORT`

For every nominal coupon auction whose **remaining term, rounded to the nearest whole year
(half up)**, is 5, 10 or 30:

```
position   SHORT one DV01-matched unit of the matched future (5y→ZF, 10y→ZN, 30y→ZB)
entry      close of session E = max(A − 5, first futures session strictly after announcementDate)
exit       close of the auction session A
```

* **Remaining term, not original term.** The supply lands at the maturity actually auctioned
  (`9-Year 11-Month` is 10y supply; `4-Year 9-Month` is 5y supply). A `2-Year` reopening of an old
  5y note is 2y supply and is therefore **excluded**.
* **Excluded before results.**
  * **2y:** PIT-truncated one-to-two-session window in 84.5% of qualification events and every
    confirmation event, plus 58.7% of round-trip cost under DV01 matching (census 2026-09-13, §2.3).
  * **3y:** no owned contract delivers it (ZT takes ≤ 2y remaining, ZF ≥ 4y2m).
  * **7y:** deliverable into ZN, but a second auctioned tenor on the 10y leg doubles that leg and
    overlaps the 5y week; the frozen legs are 5y/10y/30y.
  * **20y:** re-introduced 2020-05, so no qualification history.
  * **TIPS:** real yield, a different investor base and no matched future.
  * **FRNs:** no duration.
  * **Bills and CMBs:** not coupon supply and not fetched.
  * **Off-curve remaining terms (e.g. 6y, 9y):** no matched leg.
* **ZB for 30y throughout.** UB (2010+) would change instrument mid-sample.
* **Why one cell.** One position per auction, unscaled. No cell sized by `offeringAmount`, no
  2y/3y/7y/20y cell, no UB/TN substitution, no pre-announcement or post-auction trading cell.
  **No second cell is created, before or after results.**

---

## 4. POINT-IN-TIME CONTRACT

* `announcementDate` is TreasuryDirect's published announcement date for the auction (each row
  carries its announcement PDF). Announcements are released during the trading day, so even the
  **close of the announcement date is not used**: entry is at a session strictly after it.
* `E = max(A − 5, first session strictly after announcementDate)`. When the announcement falls on
  a non-session day, the next session is the first eligible one.
* An event with `E ≥ A` has no tradable session and is dropped (2 Q, 1 C). An auction with no
  announcement date, or with an announcement date on or after the auction date, is a PIT failure
  (1 Q, 1 C).
* **Exchange calendar:** each market's own R38 bar dates (no `engine` import). An auction date that
  is not a futures session is excluded (measured: 0).
* The auction is known in full before entry; no later auction information (size, results,
  bid-to-cover) is read.

---

## 5. HORIZON — at most five sessions, ending at the auction-day close

The catalog horizon (5 sessions) is an upper bound, reached only when the announcement precedes
the auction by six or more sessions (§2.3). The window is never extended past the auction close,
never started before the announcement, and never replaced by a fixed post-announcement length.
Nothing is added, removed or searched after results.

---

## 6. MEASUREMENT AND AGGREGATION

Per event *i* on market *k* ∈ {ZF, ZN, ZB} with DV01 scale `s_k` (§7) and front-contract returns `r_t`:

```
gross_i       = − s_k · ( Π_{t=E+1..A} (1 + r_t) − 1 )
pre_entry_i   = − s_k · ( Π_{t=S_ann+1..E} (1 + r_t) − 1 )      S_ann = last session ≤ announcementDate
net_i(c)      = gross_i − (2 + 2·rolls_i) · c/10⁴ · s_k          rolls_i = held-contract changes in (E, A]
```

* **Non-overlapping aggregation rule.** Events whose return sessions `(E, A]` intersect are chained
  into ONE cluster. The cluster's P&L is the **SUM** of its events. Each event carries one unit
  decided at its own entry, so no weight depends on a later announcement. Clusters share no
  session, so every cluster-book t-statistic uses **Newey-West lag 0**
  (`alpha_agent.r63.sensitivity.nw_tstat`).
* **Annualisation:** the sum of cluster P&L divided by the window's years (first to last session,
  calendar days + 1, / 365.25; Q 16.99, C 9.63). Units are the fraction of one ZN-equivalent DV01
  notional per auction, summed per year, matching the catalog's cost framing.
* **Tenor legs 5y / 10y / 30y:** each leg's per-event series. The executor checks each leg for
  internal overlap: lag 0 when none exists, lag 4 otherwise. It is not assumed.
* **Drawdown:** `alpha_agent.r63.sensitivity._max_dd` on the net cluster book.
* **No second scorer.** `run_cell` is not used: an event-calendar book on one instrument per event
  has no cross-section and no baseline block for the ridge scorer to condition on. The incremental
  test is §9.

---

## 7. DV01 MATCHING — declared approximation

The estate owns no CTD or DV01 history. Constant approximate futures modified durations
(CTD-based, typical of 2000–2026) are declared now:

| market | duration | scale `s_k` = 6.2 / duration | cost per round trip at 2 bp, in ZN-DV01 notional |
|---|---|---|---|
| ZF | 4.3 | 1.442 | 5.77 bp |
| ZN | 6.2 | 1.000 | 4.00 bp |
| ZB | 13.0 | 0.477 | 1.91 bp |
| *ZT (2y, excluded)* | *1.9* | *3.263* | *13.05 bp — the cost fact behind §2.3* |

The approximation sets only the weights and cost scaling **across** tenors. Every per-tenor sign
and t-statistic is invariant to it.

---

## 8. DESCRIPTIVE ONLY — never a gate

The following are reported but never gate the verdict:

* the post-auction window (auction close → A+5 close, recovery predicted);
* the event-level net t (lag 4);
* hit rate and maximum drawdown;
* the 1 / 2 / 5 bp ladder;
* per-tenor net statistics.

---

## 9. CONTROL — THE PASSIVE SHORT (an addition that can only close)

Permanent lesson 4: a real premium with a worthless timing signal has been seen three times. The
event short must beat the always-short book of the same tenor:

```
excess_i = gross_i − (A − E) · μ_k,   μ_k = mean per-session DV01-scaled short return of market k over the same window
```

Cluster-summed; NW t < 2.0 → `KILLED_NONINCREMENTAL`. This gate is NOT in the catalog kill rule. It
is declared here as a strictly additional requirement; it can close the family, never rescue it.

---

## 10. QUALIFICATION — in this order

QUALIFICATION window 2000–2016; the CONFIRMATION window 2017–2026 is evaluated **only if gates 1–10
pass**. Otherwise it stays untouched and `lockbox_t` is null.

| # | gate | verdict | `kill_rule_fired` | measured at freeze |
|---|---|---|---|---|
| 1 | a needed market or history is missing, or more than 2% of in-scope auctions cannot be placed or lack a return | `DATA_HOLD` | — | 0% unusable |
| 2 | PIT-missing share > 5% | `KILLED_PIT_UNESTABLISHED` | `PIT_UNESTABLISHED` | 0.23% |
| 3 | clusters < 36 (`MIN_EFFECTIVE_PERIODS`) or any leg < 12 events | `KILLED_INSUFFICIENT_SAMPLE` | `INSUFFICIENT_SAMPLE` | 295 clusters; legs 174 / 154 / 108 |
| 4 | pre-entry gross t ≥ 2.0 AND holding gross t < 2.0 | `KILLED_PRICED_BEFORE_ENTRY` | `CONCESSION_COMPLETE_BETWEEN_ANNOUNCEMENT_AND_ENTRY` | — |
| 5 | holding gross mean ≤ 0 | `KILLED_WRONG_SIGN` | `SHORT_DOES_NOT_EARN` | — |
| 6 | any of the **5y/10y/30y** legs has gross mean ≤ 0 | `KILLED_UNSTABLE` | `TENOR_LEGS_DISAGREE_IN_SIGN` | — |
| 7 | net cluster book NW t < 2.0 at the R38 cost | `NO_EDGE` | `NW_T_BELOW_2` | — |
| 8 | net < 1.5 %/yr at the R38 cost | `KILLED_BELOW_MATERIALITY` | `NET_BELOW_1.5PCT_PER_YEAR` | — |
| 9 | excess over the passive short NW t < 2.0 (§9) | `KILLED_NONINCREMENTAL` | `NO_INCREMENT_OVER_PASSIVE_SHORT` | — |
| 10 | BH q = 0.10 fails at m = 1, or at m = 3 with the inherited nulls | `KILLED_MULTIPLICITY` | `BH_Q010_FAILS_M1_OR_INHERITED_M3` | — |
| 11 | confirmation: >2% unusable / >5% PIT-missing | `DATA_HOLD` / `KILLED_PIT_UNESTABLISHED` | — / `PIT_UNESTABLISHED` | 0% / 0.29% |
| 12 | confirmation clusters < 36 | `NEED_MORE_EVIDENCE` | — | 230 clusters |
| 13 | confirmation net mean ≤ 0 | `KILLED_UNSTABLE` | `CONFIRMATION_SIGN_REVERSED` | — |
| 14 | confirmation net NW t (`lockbox_t`) < 2.0 | `NO_EDGE` | `CONFIRMATION_NW_T_BELOW_2` | — |
| 15 | confirmation net < 1.5 %/yr | `KILLED_BELOW_MATERIALITY` | `CONFIRMATION_NET_BELOW_1.5PCT_PER_YEAR` | — |
| 16 | otherwise | `QUALIFIED` | — | — |

**Why gate 4 sits before the sign gate.** A concession spent before a legitimate entry can leave
the tradable window flat or negative. The more informative verdict is kept.

**`capital_eligible` is always False.** Even `QUALIFIED` only opens the human-gated prospective
registration; capital needs forward evidence and governance.

**Gates stricter than the catalog kill rule, declared as additions:**

* gate 9 (the passive short);
* the extra round trip per roll;
* gates 2, 3 and 11–15.

None loosens a catalog condition. The 2 bp gate cost is now in the kill rule itself (§0.2):
`max(KILL_RULE_COST_BPS = 1 bp, R38 cost)` = 2 bp.

---

## 11. COSTS

* **R38 owned model:** `cost_group TREASURY_FUTURE = 2.0 bp per side` (`research_contract.json`).
  The ML panel's median `cost_bps_per_side` is **ZF 2.0, ZN 2.0, ZB 2.0** (min = max = 2.0 over
  469 / 544 / 594 panel rows; ZT also 2.0, excluded). `cost_model_state: MODELLED_NOT_OBSERVED`.
* **Gate cost:** `max(1 bp, R38 cost)` = **2.0 bp per side** for every leg, on DV01-scaled traded
  notional. Each held-contract change inside the window adds one extra round trip.
* **Ladder (descriptive):** 1 / 2 / 5 bp per side.
* **Cost arithmetic from the census, no return involved (5y/10y/30y, 2 bp per side):**

| window | events | round-trip drag | 5y share | 10y share | 30y share | gross needed to clear 1.5 %/yr net |
|---|---|---|---|---|---|---|
| QUALIFICATION | 436 | **1.10 %/yr** | 56.1% | 32.9% | 11.0% | **2.60 %/yr** |
| CONFIRMATION | 346 | **1.39 %/yr** | 49.0% | 34.6% | 16.5% | **2.89 %/yr** |

  Units are ZN-DV01 notional per auction, summed per year. For reference, the pre-decision
  four-tenor book: 2.67 %/yr (Q) / 2.93 %/yr (C) drag and a 4.17 / 4.43 %/yr gross hurdle.

---

## 12. MULTIPLE TESTING

Owner: `alpha_agent.r63.sensitivity.bh_fdr`, q = 0.10. The p-value is the one-sided NW p of the
QUALIFICATION net cluster book of the 5y/10y/30y legs at the gate cost.

* **Family:** one cell, **m = 1**. Passing requires p ≤ 0.10.
* **Inherited:** `PRIMARY_DEALER_POSITIONS` and `RATES_CARRY_CURVE_RV` at p = 1, **m = 3**. Passing
  requires p ≤ 0.0333.

Both must pass. The denominator is never reset. The 2y exclusion creates no second cell and no
extra test: it was decided before any p-value existed.

---

## 13. FALSIFICATION — and what will NOT be done

A null result means one of two things. Either the pre-auction concession in the 5y/10y/30y futures,
taken only after the auction is announced, does not pay after DV01-scaled costs. Or it pays no more
than being short Treasuries anyway.

**Forbidden after results:**

* **re-adding the 2y leg** (in any window, as a rescue or as a second cell);
* reversing the sign;
* entering on or before the announcement date, or at its close;
* a fixed-length window, a longer window, or holding past the auction close;
* trading the post-auction recovery;
* dropping, re-weighting or adding a tenor (2y, 3y, 7y, 20y, UB, TN), or a tenor-specific window;
* original-term instead of remaining-term classification;
* sizing by `offeringAmount`, bid-to-cover or dealer take-up;
* changing the durations of §7;
* volatility-matching instead of DV01-matching;
* charging less than the R38 2 bp at the gates;
* removing the passive-short control or the roll round trip;
* changing the window years (2000–2016 / 2017–2026);
* excluding years (e.g. 2008 or 2020), reopenings or off-cycle auctions;
* a mean-weighted instead of summed cluster book;
* any second cell.

## 14. SAFETY

RESEARCH ONLY. No purchase, no subscription, no licence, no model promotion, no registration, no
capital allocation, no portfolio mutation, no proposal, no order, no fill, no live deployment, no
live write. Writes land only under the campaign research root (auction cache, census and result
artifacts). The live checkout and the live next-open skew challenger are not touched. The executor's
catalog pin (`module_sha256`, `preregistration_sha256`) is set by the lead after this document is
committed; the executor refuses a catalog kill rule that differs from §0.2.
