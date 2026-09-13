# PREREGISTRATION - SPX TAIL-HEDGING DEMAND (CBOE SKEW) AND 5-SESSION INDEX RETURNS, 1993-2022

**Mechanism id:** `SPX_TAIL_HEDGE_DEMAND_SKEW_INDEX_1993_2022_V1`
**Catalog:** `research/alpha_agent/MECHANISM_FRONTIER.json` (data_version 2)
**Selected by:** the Alpha Agent mechanism frontier (`alpha_agent.r59.mechanisms`), not by a
human topic choice.
**Status:** FROZEN. Written and committed BEFORE any forward return, book or t-statistic for
this mechanism was computed on real data. Not edited after results exist.

---

## 0. THE P&L MECHANISM AND WHY THIS TEST EXISTS

Portfolio hedgers buy SPX out-of-the-money puts for crash protection without optimising
expected return. When tail-hedging demand is abnormally high, dealers who are short those puts
sell index delta and quote a steeper risk-neutral skew, which the Cboe SKEW index measures
from 30-day SPX options across the strike range. **Frozen direction: NEGATIVE** - an
abnormally steep SKEW precedes LOWER 5-session index returns. A contradicted sign closes the
mechanism; it is never reversed.

### 0.1 The census that changed what this test can decide (signal-only, no return)

The catalog first ranked this mechanism for its value to the live candidate
`REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1`. Before any return existed, the SKEW z-score was
compared with that candidate's own signal (the SPY OPRA near-expiry 2 %-OTM put-call IV
spread z-score, same strictly-trailing 60-observation construction):

| overlap | sessions | Pearson | Spearman | sign agreement |
|---|---|---|---|---|
| confirmation surface 2022-10 .. 2024-07 | 437 | +0.026 | +0.076 | 0.547 |
| discovery surface 2024-10 .. 2026-08 | 434 | -0.109 | -0.092 | 0.470 |

The preregistered clause (correlation below 0.30) **fired before any return**: whatever this
experiment shows is recorded as **not informative for the live candidate**. It tests one
question only - whether index-level TAIL-hedging demand is an independent SPX return source.

### 0.2 Prior tests on this information

| record | object | result |
|---|---|---|
| R32 EQUITY_BETA_TIMING / VOLATILITY_RISK_REGIME | SKEW inside a monthly VIX+SKEW(+VVIX) percentile composite | t vs vol-matched -1.42 / -1.03 |
| R41 vol lab | SKEW_Z as a conditioner of VX futures spreads | best rule t 1.31 |
| R63 VOLATILITY_EXPECTATIONS_IV | skew_z inside a block for VOLATILITY / RATES scopes, h 1/5/21 | REPRODUCTION_FAILED |
| alpha recovery SPY option surface | 6 cells on SPY OPRA 2024-2026 | 0 qualified |

SKEW was never scored ALONE against the index at 5 sessions. All twelve prior cells enter the
inherited multiplicity at p = 1 (section 8).

---

## 1. DATA - OWNED, $0, NOTHING ACQUIRED

* **Signal:** Cboe SKEW daily close, owned copy
  `D:\Stock_Prediction_app_data\multi_horizon_alpha_r41\_data_cboe\SKEW_History.csv`
  (1990-01-02 .. 2026-08-21, 9,211 rows).
* **Price:** Norgate SPY, `StockPriceAdjustmentType.TOTALRETURN`, unpadded
  (1993-01-29 .. 2026-09-11, 8,462 sessions). The SPY session calendar is the decision
  calendar; SKEW is aligned to it, a missing SKEW session is NaN (15 in the qualification
  window), never forward-filled.

### 1.1 Census (signal-only)

| partition (entry date) | weekly decisions | finite z | SKEW median | p10 | p90 |
|---|---|---|---|---|---|
| P1 1993-01-29 .. 1999-12-31 | 350 | 344 | 115.6 | 110.1 | 121.8 |
| P2 2000 .. 2009 | 503 | 502 | 116.0 | 110.6 | 123.5 |
| P3 2010 .. 2019 | 504 | 503 | 124.8 | 117.2 | 137.7 |
| P4 2020-01 .. 2022-07 | 130 | 130 | 136.3 | 121.7 | 151.6 |
| CONFIRMATION 2022-08 .. 2026-08 | 204 | 204 | 142.0 | 121.4 | 156.5 |

The SKEW level drifts upward across decades; the strictly trailing z-score is used precisely so
the rule reads abnormality relative to the recent regime, not the level.

---

## 2. THE ONE FROZEN CELL

```
z_t   = (SKEW_t - mean(SKEW_{t-60..t-1})) / std(SKEW_{t-60..t-1})     (alpha_recovery.options_surface._z)
pos_t = -1 * sign(z_t)                                                (gross 1.0; 0 when z is NaN or 0)
```

* **Decision grid:** every 5th SPY session, starting at the first session with a finite z;
  non-overlapping.
* **Entry:** the SPY total-return CLOSE of session t+1 (SKEW is an end-of-day value published
  after the close; the decision session's own close is never used).
* **Exit:** the close of session t+1+5. Period return `r = TR(t+6)/TR(t+1) - 1`.
* **Net:** `pos * r - |pos| * 2 * cost` - a full round trip is charged EVERY period, which is
  conservative whenever the sign does not change.
* **One horizon (5), one cell, m_declared = 1.** No second horizon, threshold, lookback,
  winsor, level cell or change cell is created, before or after results.

## 3. POINT-IN-TIME CONTRACT

The z-score reads SKEW through session t only; the position is taken at the close of t+1; the
return is measured strictly after entry. No revision risk exists (index closes are not
revised). Norgate total-return closes are dividend-adjusted series whose adjustment does not
change the sign or timing of any period return.

## 4. WINDOWS - declared now

* **QUALIFICATION:** decisions whose ENTRY session lies in 1993-01-29 .. 2022-07-29.
  Partitions P1..P4 as in 1.1.
* **CONFIRMATION (untouched):** entries 2022-08-01 .. the last decision whose exit session is
  on or before 2026-08-31. It is COMPUTED ONLY IF every qualification gate passes; otherwise it
  stays unread and is reported as such.

## 5. COSTS

1 bp per side (the canonical `SPY_PROXY_COST_BPS`), 2 bp and a 5 bp stress rung.

## 6. MEASUREMENT - existing owners only

* Period statistics: `alpha_agent.alpha_recovery.options_surface._stats` (annualised net at
  252/5 periods per year, Sharpe, max drawdown) and `alpha_agent.r63.sensitivity.nw_tstat`
  (lag 0: the periods do not overlap).
* Timing alpha over a passive book: `beta_hat` is the OLS slope of the per-period net return on
  the same-period SPY return over the qualification window; the timing-alpha series is
  `a_t = net_t - beta_hat * spy_r_t`, and its t is `nw_tstat(a, 0)` (the periods do not
  overlap). This is the permanent lesson that a premium with a worthless timing signal must not
  pass.
* Equal-risk increment over the incumbent: `alpha_agent.alpha_recovery.intraday_alpha.equal_risk_daily`
  on the overlap with the incumbent's own daily path, where that path exists.
* Multiplicity: `alpha_agent.r63.sensitivity.bh_fdr` at q = 0.10 over the one-sided p.

## 7. QUALIFICATION GATES - in this order, mapped to the executor verdicts

1. **Data.** SKEW finite on < 95 % of qualification decisions, or any partition with fewer than
   `MIN_EFFECTIVE_PERIODS` = 36 decisions -> `DATA_HOLD`.
2. **Frozen sign.** Qualification mean net return (1 bp) <= 0 -> `KILLED_WRONG_SIGN`.
3. **Standalone.** NW t of the qualification net series < 2.0 -> `NO_EDGE`.
4. **Materiality.** Annualised net at 1 bp < 1.5 %/yr -> `KILLED_BELOW_MATERIALITY`.
5. **Stability.** Annualised net <= 0 in 2 or more of P1..P4 -> `KILLED_UNSTABLE`.
6. **Timing alpha.** Intercept NW t < 2.0 or intercept annualised < 1.5 %/yr ->
   `KILLED_NONINCREMENTAL`.
7. **Multiplicity.** BH q = 0.10 over m = 13 (this cell plus 12 inherited nulls at p = 1)
   fails -> `KILLED_MULTIPLICITY`.
8. **Cost survivability.** Annualised net at the 5 bp stress rung < 1.5 %/yr ->
   `KILLED_BELOW_MATERIALITY`.
9. **Incumbent.** Equal-risk increment state OK and not positive after costs ->
   `NO_INCREMENTAL_INFORMATION_EDGE` (a DATA_HOLD state is reported, not a gate).
10. **Untouched confirmation.** Mean net <= 0, or annualised net < 1.5 %/yr, or NW t < 2.0,
    or one-sided p > 0.10 (BH at denominator 1) -> `KILLED_UNSTABLE`.
11. Otherwise -> `QUALIFIED`, which starts a HUMAN gate (prospective registration under
    contract rule 16). **CAPITAL ELIGIBLE = NO** in every case.

The 12 inherited nulls: SPY option surface 6 cells; R32 EQUITY_BETA_TIMING and
VOLATILITY_RISK_REGIME (2); R41 VX conditional short-vol with SKEW_Z (1); R63
VOLATILITY_EXPECTATIONS_IV h 1/5/21 (3).

## 8. FALSIFICATION - and what will NOT be done

A null result means: abnormally steep 30-day SPX tail skew, read the session before entry,
carries no after-cost information about the next 5 SPY sessions beyond a passive position.

**Forbidden after results:** reversing the sign; another z lookback; the SKEW level or its
change instead of the z-score; thresholds or deciles; another horizon or entry delay; excluding
1987-style crash years, 2008 or 2020; SPX price return instead of SPY total return; any second
cell; reading the confirmation window after a qualification failure.

## 9. SAFETY

RESEARCH ONLY. No purchase, no subscription, no registration, no promotion, no capital
allocation, no portfolio mutation, no order, no fill, no backfill, no live write. The live
checkout and the live next-open skew challenger are not touched.
