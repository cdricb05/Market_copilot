# PREREGISTRATION - RELATIVE INDEX IMPLIED VOLATILITY (HEDGING DEMAND ACROSS INDICES)

**Mechanism id:** `RELATIVE_INDEX_IMPLIED_VOL_HEDGING_DEMAND_V1`
**Catalog:** `research/alpha_agent/MECHANISM_FRONTIER.json` (data_version 2)
**Selected by:** the Alpha Agent mechanism frontier (`alpha_agent.r59.mechanisms`).
**Status:** FROZEN. Written and committed BEFORE any forward return, book or t-statistic for this
mechanism was computed on real data. Not edited after results exist.

---

## 0. THE P&L MECHANISM

Hedging demand is index-specific: technology-concentrated portfolios hedge the Nasdaq-100 and
small-cap books hedge the Russell 2000, and dealers delta-hedge each index's options in that
index. When one index's implied volatility rises relative to the S&P 500's by more than its
realised volatility justifies, relative hedging pressure should become relative price pressure
that cross-index arbitrage does not remove, because the option books are not fungible.

**Frozen direction:** a RICH relative implied premium precedes that index UNDERPERFORMING the
S&P 500 over the next month. A contradicted sign closes the mechanism; it is never reversed.

### 0.1 Census before any return (signal-only)

| fact | value |
|---|---|
| FRED VXNCLS / RVXCLS / VIXCLS fetched free (sha256 manifest in `_data_implied_vol_indices`) | from 2001-02-02 / 2004-01-02 / 1990-01-02 |
| monthly decisions, NDX leg / RUT leg | 306 / 271 (2004-2019: 191 each; 2020-2026: 81 / 80) |
| Spearman of the z-score with the TRAILING 21-session relative return (price-state clause, kill at 0.5) | NDX 0.124, RUT 0.070 - not fired |
| correlation with the live SPY OPRA skew z-score | NDX -0.154 / -0.096, RUT +0.184 / -0.024 |
| daily correlation between the two legs' z-scores | 0.091 |
| lag-1 monthly autocorrelation of the z-score | NDX -0.004, RUT -0.064 |

The live-candidate correlations are near zero, so this test decides only its own question and is
recorded as NOT informative for `REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1`.

### 0.2 Prior tests

VXN and RVX were loaded into the R32 volatility panel and never used by any sleeve. Every
index-level implied test (R32 regime/beta timing, R35 implied risk premia, R35/R36/R63 VIX term
structure, R63 VOLATILITY_EXPECTATIONS_IV) timed OUTRIGHT index exposure on one volatility level;
none traded a relative premium across indices in a market-neutral spread. The six SPY
option-surface cells are inherited at p = 1 (section 8).

---

## 1. DATA - $0

* Implied volatility: FRED `VIXCLS`, `VXNCLS`, `RVXCLS` daily closes (Cboe index values), read
  from the fetched cache; a missing cache is `DATA_HOLD`, never a download inside the executor.
* Integrity gate: FRED `VIXCLS` must match the owned Cboe `VIX_History.csv` close with
  correlation >= 0.999 on their common sessions, or `DATA_HOLD`.
* Prices: Norgate `SPY`, `QQQ`, `IWM`, `StockPriceAdjustmentType.TOTALRETURN`, unpadded. The SPY
  session calendar is the decision calendar; implied-volatility values are aligned to it and
  never forward-filled.

## 2. THE ONE FROZEN CELL (two legs, one book)

For leg `k` in {NDX: (VXNCLS, QQQ), RUT: (RVXCLS, IWM)}:

```
rv_X(t)   = std(log return of X over the 21 sessions ending t-1) * sqrt(252)      (strictly prior)
RIP_k(t)  = (IV_k(t)/100 - rv_ETF_k(t)) - (VIX(t)/100 - rv_SPY(t))
z_k(t)    = alpha_recovery.options_surface._z(RIP_k)      (60 observations, strictly prior)
s_k(t)    = -sign(z_k(t))                                  (0 when z is NaN)
leg_k     = s_k(t) * (TR_ETF_k(t+22)/TR_ETF_k(t+1) - TR_SPY(t+22)/TR_SPY(t+1))
book      = 0.5 * leg_NDX + 0.5 * leg_RUT
cost      = sum_k 0.5 * |s_k| * 2 instruments * 2 * c
```

* **Decision grid:** every 21st SPY session from the first session on which BOTH z-scores are
  finite; non-overlapping. Entry at the close of t+1; exit 21 sessions later.
* **One book, two declared legs.** No leg weighting, threshold, lookback, horizon or third index
  is created, before or after results.

## 3. POINT-IN-TIME CONTRACT

Implied-volatility closes and realised volatility are read through t; the position is entered at
the close of t+1; the return is measured strictly after entry.

## 4. WINDOWS

* **QUALIFICATION:** decisions whose entry lies in 2004-01-01..2019-12-31.
* **CONFIRMATION (untouched):** entries 2020-01-01 .. the last decision whose exit is on or before
  2026-08-31, READ ONLY IF every qualification gate passes.

## 5. COSTS

2 bp per side per instrument (primary, the catalog's declared rate), 1 bp, and a 5 bp stress rung.

## 6. MEASUREMENT - existing owners only

* Annualisation at 252/21 periods per year; t via `alpha_agent.r63.sensitivity.nw_tstat(x, 0)`;
  drawdown via `S._max_dd`.
* **Timing increment over the passive relative book:** `P = 0.5*(TR_QQQ ret - TR_SPY ret) +
  0.5*(TR_IWM ret - TR_SPY ret)` over each period (always long the smaller indices against SPY);
  `beta_hat` = OLS slope of book net on P; `a = net - beta_hat * P`; t = `nw_tstat(a, 0)`.
* Equal-risk increment over the incumbent: `intraday_alpha.equal_risk_daily`.
* Multiplicity: `alpha_agent.r63.sensitivity.bh_fdr`, q = 0.10, one-sided p per LEG.

## 7. QUALIFICATION GATES - in this order

1. **Data.** Integrity correlation < 0.999, fewer than 95 % of qualification decisions with a
   finite z in both legs, or fewer than 36 finite decisions in either leg -> `DATA_HOLD`.
2. **Frozen sign.** Book mean net <= 0 -> `KILLED_WRONG_SIGN`.
3. **Standalone.** Book NW t < 2.0 -> `NO_EDGE`.
4. **Materiality.** Book annualised net at 2 bp < 1.5 %/yr -> `KILLED_BELOW_MATERIALITY`.
5. **Leg agreement.** Either leg's annualised net <= 0 -> `KILLED_UNSTABLE`.
6. **Timing increment.** t < 2.0 or annualised < 1.5 %/yr -> `KILLED_NONINCREMENTAL`.
7. **Multiplicity.** Either leg fails BH q = 0.10 over m = 8 (2 legs + 6 inherited nulls at
   p = 1) -> `KILLED_MULTIPLICITY`.
8. **Cost survivability.** Book annualised net at 5 bp < 1.5 %/yr -> `KILLED_BELOW_MATERIALITY`.
9. **Incumbent.** Equal-risk increment state OK and not positive -> `NO_INCREMENTAL_INFORMATION_EDGE`.
10. **Untouched confirmation.** Book mean <= 0, annualised < 1.5 %/yr, NW t < 2.0 or one-sided
    p > 0.10 -> `KILLED_UNSTABLE`.
11. Otherwise `QUALIFIED` (HUMAN gate: prospective registration). **CAPITAL ELIGIBLE = NO.**

Inherited nulls: the six SPY option-surface cells (ATM IV level, pre-registered skew, VRP at h 1
and 5).

## 8. FALSIFICATION - and what will NOT be done

A null result means: a rich implied-minus-realised premium in the Nasdaq-100 or Russell 2000
relative to the S&P 500 carries no after-cost information about their relative return next month.

**Forbidden after results:** reversing the sign; one leg alone; another realised-volatility or
z lookback; implied level or ratio instead of the premium; weekly or quarterly horizons; futures
legs; excluding 2008, 2020 or 2022; thresholds; any second cell; reading the confirmation window
after a qualification failure.

## 9. SAFETY

RESEARCH ONLY. No purchase, subscription, registration, promotion, capital allocation, portfolio
mutation, order, fill, backfill or live write.
