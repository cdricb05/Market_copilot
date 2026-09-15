# PREREGISTRATION - DIVIDEND-MONTH DEMAND PREMIUM IN POINT-IN-TIME S&P 500 PAYERS

**Mechanism id:** `DIVIDEND_MONTH_DEMAND_PREMIUM_V1`
**Global candidate:** `GC_USEQ_DIVIDEND_MONTH_DEMAND` (declared with this mechanism)
**Selected because:** after verdict 2 the rebuilt global frontier (COMPLETE, 125 of 125 identities)
again had no agent-executable action and the agent checkpoint read `NO_ELIGIBLE_MECHANISM`. This is the
next independent free mechanism: a third asset class (US_EQUITY) and a third mechanism class
(LIQUIDITY_PRESSURE) after EQUITY_INDEX / RISK_PREMIUM_TIMING and FX / FORCED_TRADING_FLOW, on owned
survivorship-safe data, with no dividend-month hypothesis anywhere in ResearchMemory or the repository.
**Catalog:** `research/alpha_agent/MECHANISM_FRONTIER.json` (data_version 1)
**Run:** ALPHA_STRIKE_SEP15_V2, branch `alpha-strike-sep15-v2`.
**Status:** FROZEN. Written and committed BEFORE any holding-month return, book, control or t-statistic
for this mechanism was computed on real data. The census (1.3) read membership flags, ex-dividend dates
and quote dates only. Not edited after results exist.

---

## 0. THE P&L MECHANISM

Many investors prefer to receive dividends - income mandates, retirees, dividend-focused funds, tax
clienteles - and buy stocks ahead of their ex-dividend dates. A company's dividend schedule repeats
every year, so the months in which a payer will go ex-dividend are predictable, and that buying
pressure concentrates in those months: Hartzmark and Solomon (2013) find predicted dividend months earn
abnormal returns that reverse afterwards. The demand is price-insensitive and the pattern is public, so
the premium is compensation for absorbing clientele flow, not information.

**The P&L:** a dollar-neutral monthly book INSIDE point-in-time S&P 500 dividend payers - long the payers
predicted to go ex-dividend this month because they did twelve months earlier, short the payers that
are not. Restricting both legs to payers removes the payer-versus-non-payer characteristics (value,
low volatility) so the only difference between the legs is WHEN the dividend is due.

**Frozen direction:** LONG predicted-month payers, SHORT other payers. A contradicted sign closes the
mechanism; it is never reversed.

### 0.1 Prior estate tests nearby

| record | object | why it is different |
|---|---|---|
| `SAME_MONTH_SEASONALITY` (REJECTED, spread t 1.65) and graveyard `RETURN_SEASONALITY` (2) | a name's own same-calendar-month past RETURN | this signs by a recurring ex-dividend DATE and must beat the seasonality book (gate 4); the three seasonality hypotheses are inherited at p = 1 |
| `EQUITY_INDEX_CROSS_ASSET_CARRY_R64` | index-futures dividend carry | not single-name ex-dividend timing |
| `R32_EVENT_DRIVEN_CALENDAR`, `r46_3_spx_turn_of_month` | index calendar dummies | no dividend information |
| `SP500_INDEX_ADDITION_DELETION`, `SHORT_HORIZON_REVERSAL_GAP_FADE`, `MICROSTRUCTURE_ORDER_FLOW` | membership changes, daily reversals, intraday flow | different objects |

---

## 1. DATA - OWNED, $0

Norgate US equities, watchlist **S&P 500 Current & Past** (1,897 securities including delisted names):
unpadded `TOTALRETURN` closes for returns, the unadjusted frame's `Dividend` column (> 0 on an ex-dividend
date) for the schedule, and `index_constituent_timeseries(symbol, "S&P 500")` for membership. SPY session
dates define month ends.

### 1.1 The membership trap, found before any return

A delisted security's last membership flag stays 1 if it was a member when it stopped trading;
forward-filling the flag to later month ends counted 936 "members" a month. **Declared rule:** a
security is a member at formation only if its latest flag on or before the formation session is 1 AND
it has a close within 5 calendar days on or before that session. That gives 500.2 members a month.

### 1.2 Definitions

For holding month t (calendar month), formation session `f(t)` = the last SPY session of month t-1 and
holding end `h(t)` = the last SPY session of month t.

```
member(i, t)   = S&P 500 flag on or before f(t) is 1  AND  a close of i within 5 calendar days on or before f(t)
payer(i, t)    = member(i, t) AND i has an ex-dividend date in calendar months t-12 .. t-1
D(i, t)        = 1 if i has an ex-dividend date in calendar month t-12, else 0        (payers only)
L(t) = {payer, D = 1}   S(t) = {payer, D = 0}
w(i, t)        = +1/|L(t)| on L, -1/|S(t)| on S, 0 otherwise
r(i, t)        = TR(last close on or before h(t)) / TR(last close on or before f(t)) - 1
                 (a security that stops trading inside the month runs to its last close; none after f(t) -> 0)
gross(t)       = sum_i w(i, t) * r(i, t)
turnover(t)    = sum_i |w(i, t) - w(i, t-1)|            (target weights; the first month counts 2)
net(t)         = gross(t) - turnover(t) * cost
```

### 1.3 Census before returns (dates, flags and ex-dates only; 0 load failures)

| window | months | members / month | payers | long leg mean (min, max) | short leg mean (min) | payers changing side / month | members unquoted at holding end |
|---|---|---|---|---|---|---|---|
| QUALIFICATION 1994-01..2012-12 | 228 | 500.2 [500, 501] | 396.8 (79.3 %) | 127.2 (53, 227) | 269.6 (196) | 63.3 % | 0.25 % |
| CONFIRMATION 2013-01..2026-08 | 164 | 503.6 [500, 506] | 413.1 (82.0 %) | 134.2 (57, 197) | 278.9 (209) | 64.1 % | 0.14 % |

Long leg by calendar month (qualification mean): Jan 73, Feb 166, Mar 135, Apr 78, May 172, Jun 132,
Jul 78, Aug 175, Sep 124, Oct 82, Nov 174, Dec 137 - the quarterly payment cycle.

---

## 2. THE ONE FROZEN CELL (m_declared = 1)

The book of 1.2, rebalanced at every month-end close, equal weight within each leg, dollar-neutral
(one dollar long and one dollar short per dollar of NAV). No second cell exists or will be created:
no t-3/t-6/t-9 prediction, no quarterly-only or monthly-payer subsets, no long-only leg, no
value-weighting, no dividend-size or yield filter, no Russell universe, before or after results.

## 3. POINT IN TIME

Ex-dividend dates twelve months old, membership flags and quotes on or before `f(t)` are all public at
the formation close; the book is formed at that close and its return is measured strictly after it.
Delisted securities remain in the universe while they are members and trade.

## 4. WINDOWS

* **QUALIFICATION:** holding months 1994-01..2012-12. Halves 1994-01..2003-12 and 2004-01..2012-12.
* **CONFIRMATION (untouched):** holding months 2013-01..2026-08, after the 2013 publication. READ ONLY IF
  every qualification gate passes.

## 5. COSTS

12.5 bp per side (`alpha_agent.r63.EQ_COST_RATE_PER_SIDE`, the desk convention) on `turnover(t)`,
primary. Ladder 0 / 12.5 / 25 / 50 bp. Stress rung: 25 bp.

## 6. MEASUREMENT - existing owners only

* Annualisation at 12 months a year; t and one-sided p via `alpha_agent.r63.sensitivity.nw_tstat(x, 3)`
  (lag 3: the quarterly payment cycle puts overlapping names in months t and t+3); Sharpe
  `mean / sd * sqrt(12)`; maximum drawdown via `S._max_dd`.
* **Increment over the same-calendar-month return seasonality book:** `Z(t)` = equal-weight top-tercile
  minus bottom-tercile of payers ranked by their total return over calendar month t-12 (month-end closes
  of t-13 and t-12; payers without that return are left out of Z only), gross. `beta_hat` = OLS slope
  of `net` on `Z`; `a = net - beta_hat * Z`; t via `nw_tstat(a, 3)`; annualised mean `12 * mean(a)`.
* Multiplicity: `alpha_agent.r63.sensitivity.bh_fdr`, q = 0.10, one-sided p; m = 4 (this cell plus
  SAME_MONTH_SEASONALITY and the two RETURN_SEASONALITY graveyard hypotheses at p = 1).
* Diagnostics reported, never gated and never a rescue: gross book, hit rate, mean turnover, leg sizes,
  gross by calendar month, and beta of `net` to SPY total return.

## 7. GATES - in this order

1. **Data -> `DATA_HOLD`.** More than 1 % of the 1,897 securities fail to load; mean members per
   qualification month outside [490, 510]; any month in either window with fewer than 30 long or 100
   short names; more than 2 % of members unquoted at holding end; fewer than 200 qualification or 150
   confirmation months.
2. **Frozen sign.** Mean net at 12.5 bp <= 0 -> `WRONG_SIGN`.
3. **Standalone.** NW t < 2.0 -> `STANDALONE_T`.
4. **Materiality.** Annualised net at 12.5 bp < 1.5 %/yr -> `MATERIALITY`.
5. **Increment over the seasonality book.** t < 2.0 or annualised mean < 1.5 %/yr -> `INCREMENT`.
6. **Stability.** Annualised net <= 0 in either half -> `STABILITY`.
7. **Drawdown.** Maximum drawdown of `net` worse than -20 % -> `DRAWDOWN`.
8. **Multiplicity.** Fails BH q = 0.10 at m = 4 -> `MULTIPLICITY`.
9. **Cost survivability.** Annualised net at 25 bp < 1.5 %/yr -> `COST`.
10. **Untouched confirmation.** Mean net <= 0, NW t < 2.0 or annualised net < 1.5 %/yr at 12.5 bp ->
    `CONFIRMATION`.
11. Otherwise `QUALIFIED`, which raises a HUMAN gate (prospective registration, contract rule 16).

Verdicts: gate 1 -> `DATA_HOLD`; gates 2-10 -> `NO_EDGE` with the fired gate recorded; gate 11 ->
`QUALIFIED`. **CAPITAL ELIGIBLE = NO** in every case.

## 8. FALSIFICATION - and what will NOT be done

A null means: inside point-in-time S&P 500 payers, holding the names due to go ex-dividend this month
against the names that are not earned no after-cost premium beyond same-calendar-month return
seasonality, on 1994-2012 or on the untouched post-publication window.

**Forbidden after results:** reversing the sign; adding t-3/t-6/t-9 predictions; payer subsets
(quarterly, monthly, high-yield, large-dividend); long-only or benchmark-relative legs; value or
volatility weighting; a smaller-cap universe; excluding 2000-2002, 2008 or 2020; a lower cost rate;
moving the halves or the confirmation boundary; relaxing any threshold; reading the confirmation after
a qualification failure.

## 9. SAFETY

RESEARCH ONLY. No purchase, subscription, credential, registration, promotion, capital allocation,
portfolio mutation, order, fill, backfill or live write. The live checkout
`C:\Users\binis\paper_trader` is not touched.
