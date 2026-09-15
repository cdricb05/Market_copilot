# PREREGISTRATION - INDEX MEMBERSHIP FORCED FLOW: RUSSELL 2000 RECONSTITUTION POST-EFFECTIVE REVERSAL

**Mechanism id:** `INDEX_MEMBERSHIP_FORCED_FLOW_V1`
**Global candidate:** `GC_USEQ_RUSSELL_RECONSTITUTION_FORCED_FLOW` (declared with this mechanism)
**Selected because:** the run brief names index-membership forced flow as the first structural forced-flow
candidate once the composite reached a verdict (DATA_HOLD, `dc0173a`). A census of the owned Norgate
point-in-time index estate found 64 watchlists with historical constituents, including Russell 1000 / 2000 /
3000 / Micro Cap, S&P 400 / 600 / 1500 and Nasdaq-100. The S&P 500 committee-change family is closed
(`SP500_INDEX_ADDITION_DELETION`, Stage 14). The Russell 2000 annual reconstitution is the largest
rules-based, mechanically forced index trade in US equities, and no Russell reconstitution hypothesis
exists in ResearchMemory or the repository.
**Catalog:** `research/alpha_agent/MECHANISM_FRONTIER.json` (data_version 1)
**Run:** ALPHA_COMPOSITE_STRIKE_SEP15_V1, verdict 2 contract, branch `alpha-composite-strike-sep15-v1`.
**Status:** FROZEN. Written and committed BEFORE any event-window return, leg, book, control or
t-statistic was computed on real data. The census (1.4) read membership flags and quote DATES only. Not edited
after results exist.

---

## 0. THE P&L MECHANISM

Once a year FTSE Russell reconstitutes the Russell 2000 from rank-day market capitalisations. At the
reconstitution close, index funds and benchmarked small-cap managers must buy every security that joins and
sell every security that leaves, whatever the price. The flow is one-sided, price-insensitive and concentrated
into one closing auction across hundreds of small, thinly traded names. Liquidity providers absorb it at a
concession and unwind their inventory over the following weeks, so after the change is effective the
securities that were sold under pressure should outperform the securities that were bought under pressure
(Madhavan 2003; Petajisto 2011).

**The P&L:** a dollar-neutral book formed only after the effective membership state is observable. It is long
the securities that LEFT the Russell 2000 and short the securities that JOINED it, entered at the open of the
next session and held 20 sessions.

**Frozen direction:** LONG deletions, SHORT additions (post-effective reversal). A contradicted sign closes the
mechanism; it is never reversed, and no continuation book is run.

### 0.1 Prior estate tests nearby

| record | object | why this is different |
|---|---|---|
| `SP500_INDEX_ADDITION_DELETION` (S14-B1 addition reversal 63d t -0.03; S14-B2 deletion rebound t 0.80; 5d echo t -2.11 netting about +8 bp after 50 bp) | discretionary S&P 500 committee changes spread through the year | a different index family, actor and schedule: rules-based, annual, hundreds of changes on one effective session. Both Stage 14 hypotheses are inherited at p = 1 |
| `MONTH_END_BALANCED_REBALANCING_FLOW_V1`, `COMMODITY_INDEX_ROLL_WINDOW_PRESSURE_V1` | calendar-scheduled flows | no membership change |
| `SHORT_HORIZON_REVERSAL_GAP_FADE` | a name's own past price move | this signs names by the membership change alone |

---

## 1. DATA - OWNED, $0

* Norgate US equities watchlist **Russell 2000 Current & Past**: 11,129 securities including delisted names.
  * `index_constituent_timeseries(symbol, "Russell 2000")` for membership.
  * `TOTALRETURN` opens and closes for returns.
  * `index_constituent_timeseries(symbol, "Russell 1000")` for migrant diagnostics only.
* Norgate US Indices `$SPXTR` session dates define the market calendar (it covers 1991-1992, before SPY
  existed).
* `$RUTTR` and `$SPXTR` total-return closes are the controls.

### 1.1 Point-in-time rule

No announcement date is used or inferred from the effective date. The membership flag of session `E` is
treated as observable only after the close of `E`, so the earliest trade is the OPEN of `E+1`.

### 1.2 The census traps, found before any return

* A delisted security's last flag persists. A change therefore counts only on a flag row that actually
  changes, for a security that still quotes.
* The first Norgate snapshot (1990-07-03) adds every member and is not an event; events start in 1991.
* In the early 1990s some small names skip sessions. Declared rule: a change is tradeable only if the security
  has a quote within 5 calendar days before `E` AND its first quote after `E` is on session `E+1` with a
  finite, positive open. During the hold a session without a quote is flat, and after a final quote the
  capital sits in cash.

### 1.3 Definitions

```
changes(d)     = number of watchlist securities whose Russell 2000 flag on session d differs from their previous flag row
E(y)           = argmax over sessions d in [y-06-15, y-07-15] of changes(d), if that maximum >= 100      (y = 1991..2026)
A(y), D(y)     = tradeable securities whose flag changed 0 -> 1 (additions) / 1 -> 0 (deletions) on E(y)   (1.2 rule)
hold(y)        = sessions E+1 .. E+20
G_i(k)         = cumulative growth of i: open(E+1) -> close(E+1) on day 1, close-to-close on each later quoted session,
                 flat on unquoted sessions and after the last quote
L(k) = mean_{i in D} G_i(k)      S(k) = mean_{i in A} G_i(k)      B(k) = 1 + L(k) - S(k)
N(k)           = B(k) - 2c for k = 1..20;  N(20) -= c * (L(20) + S(20));  N(0) = 1                     (c = cost per side)
net(k)         = N(k) / N(k-1) - 1             (daily net on the active sessions)
event_net(y)   = N(20) - 1                      (one event a year: the calendar-year NAV return of the book)
```

### 1.4 Census before returns (flags and quote dates only; 0 load failures)

| window | events | additions / event: mean (min, max) | deletions / event: mean (min, max) | changes dropped by 1.2 | additions quoted on all 20 sessions | deletions quoted on all 20 sessions | additions from R1000 | deletions to R1000 | largest runner-up session |
|---|---|---|---|---|---|---|---|---|---|
| QUALIFICATION 1991-2012 | 22 | 411.6 (211, 685) | 303.1 (139, 438) | 210 of 15,933 (1.32 %) | 94.2 % | 88.9 % | 16.6 % | 25.4 % | 48 changes |
| half 1991-2001 | 11 | 515.8 (414, 685) | 370.7 (300, 438) | 207 of 9,959 (2.08 %) | 91.2 % | 83.3 % | 17.3 % | 24.8 % | 48 |
| half 2002-2012 | 11 | 307.4 (211, 486) | 235.5 (139, 357) | 3 of 5,974 (0.05 %) | 99.3 % | 97.5 % | 15.6 % | 26.4 % | 1 |
| CONFIRMATION 2013-2026 | 14 | 239.4 (183, 309) | 184.9 (118, 321) | 7 of 5,947 (0.12 %) | 99.5 % | 99.0 % | 13.5 % | 16.7 % | 1 |

Effective sessions (month-day): 1991-2003 the first session of July (07-01 .. 07-03); 2004-2026 the Monday after
the last Friday of June (06-25 .. 07-01). 29 of 36 are Mondays. In every year the runner-up in-window session has
at most 48 changes, against 322 to 1,124 on `E`.

---

## 2. THE ONE FROZEN CELL (m_declared = 1)

The book of 1.3: equal weight within each leg, dollar-neutral (one dollar long, one dollar short per dollar
of NAV), entered at the open of `E+1`, closed at the close of `E+20`, once a year.

No second cell exists or will be created, before or after results:

* no other entry or hold (1, 5, 10 or 63 sessions);
* no additions-only or deletions-only book;
* no migrant subset (R1000 <-> R2000), no size, liquidity or price filter;
* no value weights;
* no Russell 1000, Micro Cap, Nasdaq-100 or S&P 400/600 version;
* no pre-effective entry;
* no other cost rate.

## 3. POINT IN TIME

Membership flags and quotes on or before `E` are used only after the close of `E`. The book is formed at the
open of `E+1` and its return is measured strictly after that open. Delisted securities stay in the universe
while they trade.

## 4. WINDOWS

* **QUALIFICATION:** the 22 events 1991-2012; halves 1991-2001 and 2002-2012.
* **CONFIRMATION (untouched):** the 14 events 2013-2026. READ ONLY IF every qualification gate passes.

## 5. COSTS

25 bp per side (primary: the small-cap rate, twice the S&P 500 desk convention) on the traded notional:
2 x NAV at entry and `L(20) + S(20)` at exit, about 4 x NAV per event. Ladder 0 / 12.5 / 25 / 50 bp; stress rung
50 bp. Short borrow fees are not modelled separately; the stress rung carries them.

## 6. MEASUREMENT - existing owners only

* Annual NAV return = mean `event_net` (one event per calendar year). t and one-sided p come from
  `alpha_agent.r63.sensitivity.nw_tstat(x, 5)` on the active-session daily nets of all events in the window
  (the calendar-time portfolio). Maximum drawdown is `S._max_dd` over the concatenated active-session nets.
* **Increment over the indices:** OLS of the active-session daily nets on the `$RUTTR` and `$SPXTR`
  close-to-close returns of the same sessions. `a = net - b1 x RUTTR - b2 x SPXTR`; t via `nw_tstat(a, 5)`;
  annualised alpha = `mean(a) x 20`.
* Multiplicity: `alpha_agent.r63.sensitivity.bh_fdr`, q = 0.10, one-sided p; m = 3 (this cell, plus S14-B1 and
  S14-B2 at p = 1).
* Diagnostics, reported but never gated and never a rescue: gross book; long-leg and short-leg event returns;
  event-level t and per-year Sharpe; hit rate; turnover per event; the migrant shares.

## 7. GATES - in this order

1. **Data -> `DATA_HOLD`.**
   * More than 1 % of the watchlist fails to load.
   * A year 1991-2026 has no event session with at least 100 changes.
   * An event has fewer than 50 tradeable additions or 50 tradeable deletions.
   * Fewer than 20 qualification or 12 confirmation events.
   * Controls are finite on fewer than 99 % of held sessions.
   * An event window is incomplete.
2. **Frozen sign.** Mean event net at 25 bp <= 0 -> `WRONG_SIGN`.
3. **Standalone.** Active-session NW t < 2.0 -> `STANDALONE_T`.
4. **Materiality.** Mean event net at 25 bp < 1.5 %/yr -> `MATERIALITY`.
5. **Increment over the indices.** t < 2.0 or annualised alpha < 1.5 %/yr -> `INCREMENT`.
6. **Stability.** Mean event net <= 0 in either half -> `STABILITY`.
7. **Drawdown.** Maximum drawdown worse than -20 % -> `DRAWDOWN`.
8. **Multiplicity.** Fails BH q = 0.10 at m = 3 -> `MULTIPLICITY`.
9. **Cost survivability.** Mean event net at 50 bp < 1.5 %/yr -> `COST`.
10. **Untouched confirmation.** Mean event net <= 0, NW t < 2.0 or mean event net < 1.5 %/yr at 25 bp ->
    `CONFIRMATION`.
11. Otherwise `QUALIFIED`, which raises a HUMAN gate (prospective registration, contract rule 16).

Verdicts: gate 1 -> `DATA_HOLD`; gates 2-10 -> `NO_EDGE` with the fired gate recorded; gate 11 -> `QUALIFIED`.
**CAPITAL ELIGIBLE = NO** in every case.

## 8. FALSIFICATION - and what will NOT be done

A null means: after the Russell 2000 reconstitution is effective and observable, holding the securities index
funds were forced to sell against the securities they were forced to buy earned no after-cost premium beyond
the index returns, on 1991-2012 or on the untouched 2013-2026 events.

**Forbidden after results:**

* reversing the sign or running a continuation book;
* another entry or hold;
* additions-only, deletions-only or migrant-only legs;
* size, liquidity, price or volume filters;
* value or volatility weights;
* a lower cost rate;
* another index family;
* entering before `E+1`;
* excluding 1998-2002, 2008 or 2020;
* moving the halves or the confirmation boundary;
* relaxing any threshold;
* reading the confirmation after a qualification failure.

## 9. SAFETY

RESEARCH ONLY. No purchase, subscription, credential, registration, promotion, capital allocation, portfolio
mutation, order, fill, backfill or live write. The live checkout `C:\Users\binis\paper_trader` is not touched.
