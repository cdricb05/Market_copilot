# PREREGISTRATION - GRADUAL DIFFUSION OF OIL SHOCKS INTO THE EQUITY INDEX

**Mechanism id:** `OIL_SHOCK_EQUITY_SLOW_DIFFUSION_V1`
**Catalog:** `research/alpha_agent/MECHANISM_FRONTIER.json` (data_version 2)
**Selected by:** the Alpha Agent mechanism frontier (rank 2 after the first autonomous session).
**Status:** FROZEN. Written and committed BEFORE any next-month equity return, book or t-statistic
for this mechanism was computed on real data. Not edited after results exist.

---

## 0. THE P&L MECHANISM

Crude oil price changes carry information about input costs and consumer demand for the whole
corporate sector, but generalist equity investors process commodity-market information slowly, so
a large oil price increase over a month should be followed by LOWER equity index returns the next
month (Driesprong, Jacobsen and Maat 2008). **Frozen direction: NEGATIVE** - long SPY after an oil
decline, short after an oil rise. A contradicted sign closes the mechanism.

### 0.1 Census before any return (signal-only)

| window | months | oil monthly sd | share abs > 10 % | same-month corr(oil, SPY) | oil lag-1 autocorr |
|---|---|---|---|---|---|
| 1993-02 .. 2005-12 | 155 | 0.090 | 29.7 % | -0.032 | +0.067 |
| 2006-01 .. 2012-12 | 84 | 0.090 | 21.4 % | **+0.542** | +0.374 |
| 2013-01 .. 2026-08 | 164 | 0.109 | 23.2 % | +0.291 | +0.161 |

Two data traps were measured before this document and shape it:

1. WTI futures print a negative front-month price on 2020-04-20; the roll-free WTI return is
   -306 % that day. The shock is therefore measured on **ICE Brent futures (`&BRN`, never
   negative, minimum 9.75)**. Norgate `&BRN` is the unadjusted front continuation and
   `&BRN_CCB` difference-adjusted, so the roll-free daily return is
   `diff(&BRN_CCB close) / prior &BRN close` (extremes -32 % / +21 %).
2. From 2006 the oil shock moves WITH the same month's equity return (+0.54). A negative-sign oil
   rule is then partly a monthly index REVERSAL bet. Gate 6 therefore demands an increment over a
   prior-month SPY reversal book as well as over a passive long; diffusion that is only reversal
   is not this mechanism.

### 0.2 Prior tests

R33/R34 pooled 66 markets / 47 ETFs into machine predictions (0/96 beat a volatility-matched
control); R63's cross-asset transmission was a composite conditioner; EIA inventory surprise was
an intraday energy reaction; R64 equity-index carry ranked futures by their own curve. None
preregistered a monthly oil shock into the equity index. All four are inherited at p = 1.

---

## 1. DATA - OWNED, $0

Norgate `&BRN`, `&BRN_CCB` (1988-06-23..) and SPY total return (1993-01-29..). The SPY session
calendar is the decision calendar.

## 2. THE ONE FROZEN CELL

For calendar month M:

```
oil_M   = product over Brent sessions in M of (1 + r_brent) - 1          (roll-free, observed at M's last close)
spy_M   = TR_SPY(last session of M) / TR_SPY(last session of M-1) - 1     (the same month; for the control)
entry   = close of the FIRST SPY session of M+1       (never the close that defines the signal)
exit    = close of the FIRST SPY session of M+2
r       = TR_SPY(exit) / TR_SPY(entry) - 1
pos     = -sign(oil_M)                                 (gross 1.0)
net     = pos * r - |pos| * 2 * cost
```

One position a month, non-overlapping. **One cell, m_declared = 1.** No magnitude weighting,
threshold, lag, horizon, WTI series or sector ETF is created, before or after results.

## 3. POINT-IN-TIME CONTRACT

`oil_M` and `spy_M` use closes through the last session of M; entry is the next session's close;
the return is measured strictly after entry.

## 4. WINDOWS

* **QUALIFICATION:** signal months whose entry lies in 1993-03-01..2012-12-31
  (partitions 1993-2005 and 2006-2012 by entry year).
* **CONFIRMATION (untouched):** entries 2013-01-01 .. the last month whose exit is on or before
  2026-09-11, READ ONLY IF every qualification gate passes.

## 5. COSTS

1 bp per side (canonical SPY rate), 2 bp, 5 bp stress.

## 6. MEASUREMENT - existing owners only

* Annualisation 12 periods a year; t via `alpha_agent.r63.sensitivity.nw_tstat(x, 0)`;
  drawdown `S._max_dd`.
* **Timing increment (gates 5 and 6):** least squares of the net period return on
  `[r_spy(period)]` (gate 5) and on `[r_spy(period), rev(period)]` (gate 6), where
  `rev = -sign(spy_M) * r_spy(period)` is the prior-month reversal book on the same grid; the
  timing series is `a = net - X * beta_hat` and its t is `nw_tstat(a, 0)`.
* Equal-risk increment over the incumbent: `intraday_alpha.equal_risk_daily`.
* Multiplicity: `alpha_agent.r63.sensitivity.bh_fdr`, q = 0.10, one-sided p.

## 7. QUALIFICATION GATES - in this order

1. **Data.** Brent finite in < 95 % of qualification months, or fewer than 36 months in either
   partition -> `DATA_HOLD`.
2. **Frozen sign.** Mean net <= 0 -> `KILLED_WRONG_SIGN`.
3. **Standalone.** NW t < 2.0 -> `NO_EDGE`.
4. **Materiality.** Annualised net at 1 bp < 1.5 %/yr -> `KILLED_BELOW_MATERIALITY`.
5. **Increment over a passive long.** t < 2.0 or annualised < 1.5 %/yr -> `KILLED_NONINCREMENTAL`.
6. **Increment over passive long AND prior-month reversal.** t < 2.0 or annualised < 1.5 %/yr ->
   `KILLED_NONINCREMENTAL`.
7. **Stability.** Annualised net <= 0 in either partition -> `KILLED_UNSTABLE`.
8. **Not 2008.** NW t of the qualification net series excluding 2008 entries < 1.0 ->
   `KILLED_UNSTABLE`.
9. **Multiplicity.** BH q = 0.10 over m = 5 (this cell + 4 inherited nulls at p = 1) fails ->
   `KILLED_MULTIPLICITY`.
10. **Cost survivability.** Annualised net at 5 bp < 1.5 %/yr -> `KILLED_BELOW_MATERIALITY`.
11. **Incumbent.** Equal-risk increment state OK and not positive -> `NO_INCREMENTAL_INFORMATION_EDGE`.
12. **Untouched confirmation.** Mean <= 0, annualised < 1.5 %/yr, NW t < 2.0 or one-sided p > 0.10
    -> `KILLED_UNSTABLE`.
13. Otherwise `QUALIFIED` (HUMAN gate). **CAPITAL ELIGIBLE = NO.**

Inherited nulls: R33 cross-market panel, R34 ETF prediction-to-P&L, R63 cross-asset transmission,
EIA inventory surprise.

## 8. FALSIFICATION - and what will NOT be done

A null result means: the sign of last month's Brent shock, observed at the month-end close, carries
no after-cost information about next month's S&P 500 return beyond a passive long and monthly
index reversal.

**Forbidden after results:** reversing the sign; magnitude or threshold versions; WTI or spot
series; two-month or quarterly shocks; weekly horizons; sector or energy-stock legs; dropping
2008, 2020 or 2022; any second cell; reading the confirmation window after a qualification failure.

## 9. SAFETY

RESEARCH ONLY. No purchase, subscription, registration, promotion, capital allocation, portfolio
mutation, order, fill, backfill or live write.
