# PREREGISTRATION - IMPLIED CORRELATION PREMIUM (INDEX-VERSUS-CONSTITUENT CORRELATION RISK) AND INDEX TIMING

**Mechanism id:** `IMPLIED_CORRELATION_PREMIUM_INDEX_TIMING_V1`
**Catalog:** `research/alpha_agent/MECHANISM_FRONTIER.json` (data_version 2)
**Selected by:** the Alpha Agent mechanism frontier (`alpha_agent.r59.mechanisms`).
**Executor:** `alpha_agent/alpha_recovery/implied_correlation_premium.py` (`run_mechanism`).
**Status:** FROZEN. Written and committed BEFORE any forward return, book, P&L or t-statistic for
this mechanism was computed on real data. Not edited after results exist.

---

## 0. THE P&L MECHANISM

Index put buyers pay for protection against stocks falling TOGETHER, so index options embed a
correlation risk premium over single-name options. Correlation risk cannot be diversified by the
dealers who sell index options against single-name options, so its price is set by
capital-constrained intermediaries and moves with hedging demand. When implied correlation (Cboe
COR3M) stands far above the correlation constituents actually realise, hedgers are paying an
abnormal price for joint-crash insurance, which should compensate an S&P 500 long for bearing that
joint-crash risk over the following month.

**Frozen direction: POSITIVE.** A HIGH premium z-score is LONG SPY; a LOW one is SHORT SPY. A
contradicted sign closes the mechanism; it is never reversed.

### 0.1 Census before any return (signal-only)

Measured on real data with the executor's own `build_grid`; the SPY series was read for its session
CALENDAR only. No SPY price, forward return, book, P&L or t-statistic was computed.

Inputs and integrity:

| fact | value |
|---|---|
| COR3M cache sha256 vs `fetch_manifest.json` | match (`147593ee...5ae250`) |
| COR3M rows / duplicate dates / non-finite closes | 5190 / 0 / 0; 2006-01-03 .. 2026-09-11 |
| R57 PIT panel | 1897 symbols x 5601 sessions, 2004-06-01 .. 2026-09-03; panel calendar = SPY calendar over its span (0 missing sessions) |
| grid anchor / first decision (first finite z) / last grid slot with an exit | 2006-01-03 / 2008-01-04 / 2026-07-20 |
| grid slots / slots without a COR3M close on the SPY session | 247 / 0 |
| names in the realised-correlation average (min / median / max) | 495 / 502 / 505 |

Per window (windows by ENTRY date; confirmation rows require exit <= 2026-08-31):

| window | grid slots | decisions | finite premium (slots) | finite z (decisions) | premium median / sd | COR3M median | realised corr median |
|---|---|---|---|---|---|---|---|
| P1 2006-2012 | 84 | 60 | 1.000 | 1.000 | 0.160 / 0.085 | 54.89 | 0.365 |
| P2 2013-2018 | 72 | 72 | 1.000 | 1.000 | 0.115 / 0.083 | 40.98 | 0.300 |
| QUALIFICATION 2006-2018 | 156 | 132 | 1.000 | 1.000 | 0.131 / 0.087 | 45.87 | 0.333 |
| CONFIRMATION 2019-2026-08 | 91 | 91 | 1.000 | 1.000 | -0.005 / 0.085 | 30.59 | 0.266 |
| - backfill rows 2019-01..2021-09 | 33 | 33 | 1.000 | 1.000 | 0.058 / 0.082 | 35.52 | 0.291 |
| - published rows 2021-10..2026-08 | 58 | 58 | 1.000 | 1.000 | -0.018 / 0.078 | 19.98 | 0.230 |

Monthly z-score distribution, persistence and turnover (decisions only):

| window | z mean / sd | z p05 / p50 / p95 | z min / max | share abs(z) > 2 | share long (z > 0) | lag-1 autocorr of z | sign-change share (turnover) |
|---|---|---|---|---|---|---|---|
| P1 2006-2012 | +0.438 / 1.213 | -2.048 / +0.647 / +2.129 | -2.685 / +2.823 | 0.150 | 0.667 | 0.462 | 0.305 |
| P2 2013-2018 | -0.418 / 1.053 | -2.148 / -0.501 / +1.251 | -3.292 / +2.358 | 0.083 | 0.319 | 0.483 | 0.361 |
| QUALIFICATION 2006-2018 | -0.029 / 1.203 | -2.089 / -0.092 / +1.952 | -3.292 / +2.823 | 0.114 | 0.477 | 0.538 | 0.336 |
| CONFIRMATION 2019-2026-08 | -0.348 / 1.115 | -1.889 / -0.233 / +1.135 | -4.560 / +2.653 | 0.066 | 0.352 | 0.547 | 0.264 |
| - backfill rows | -0.188 / 1.015 | -1.890 / -0.057 / +1.130 | -2.175 / +2.161 | 0.091 | 0.455 | 0.492 | 0.424 |
| - published rows | -0.439 / 1.167 | -1.845 / -0.271 / +1.058 | -4.560 / +2.653 | 0.052 | 0.293 | 0.563 | 0.172 |

Lead's earlier census (weekly points, same premium definition): premium median +0.156 (2006-2012),
+0.118 (2013-2018), +0.006 (2019-2026), sd 0.078-0.087; Spearman of the premium with the VIX level
0.070 and with the trailing 63-session SPY return -0.002. The monthly grid reproduces the levels.

What the census changes, recorded before any return:

1. **The effective qualification sample starts 2008-01-04, not 2006.** The 24-slot minimum of the
   strictly prior z consumes 2006-2007. P1 is effectively 2008-01..2012-12 (60 decisions) and
   contains the 2008 crisis; this is disclosed, not repaired.
2. **The book's long share differs sharply by partition** (67 % long in P1, 32 % in P2, 35 % in the
   confirmation). A sign book that is mostly short through a rising market, or mostly long through a
   falling one, can earn or lose the index drift without any timing skill. Gate 5 (increment over
   the volatility-matched passive long) exists for exactly this and runs BEFORE the partition gate.
3. **The premium LEVEL collapsed after 2019** (median -0.005); only a strictly trailing z-score is
   admissible, as the catalog's regime note requires.
4. The z is persistent (lag-1 autocorrelation about 0.5) and flips sign on about one decision in
   three; the declared cost charges a full round trip every period regardless (section 5).

### 0.2 Point-in-time caveat - disclosed, not a kill

The COR1M/COR3M family replaced Cboe's published ICJ/JCJ/KCJ implied correlation indices (which end
2021-11-19). COR3M history before its publication is a methodology backfill computed by Cboe from
historical option prices. The inputs were observable at each close and the method is not fitted to
returns, so the backfill is admitted. The whole qualification window (2006-2018) is backfill. The
executor therefore also reports the confirmation statistics split, **descriptively**, into
entries before 2021-10-01 (backfill rows) and from 2021-10-01 (published rows). The boundary date is
DECLARED here from the catalog's disclosure; it is not measured by the executor, and the split never
gates.

### 0.3 Prior tests

Implied correlation was never loaded by any estate test (catalog sweep 2026-09-13: zero hits); R33
used only a realised correlation feature inside CROSS_MARKET_PREDICTION_R33_R34. Every index-level
implied-state timing test prosecuted the volatility level or term structure and found a real premium
with a worthless timing signal. The seven nulls in section 8 are inherited at p = 1.

---

## 1. DATA - $0, NO PURCHASE

* **COR3M:** `<research root>/_data_implied_correlation/COR3M_History.csv` (Cboe CSV, `DATE`
  mm/dd/yyyy, `CLOSE`), fetched free with `fetch_manifest.json`. The executor verifies the file's
  raw-byte sha256 against the manifest's `COR3M.sha256`. A missing cache is `DATA_HOLD`; a hash
  mismatch or missing manifest is `DATA_HOLD` at gate 1. Nothing is downloaded inside the executor.
  Duplicate dates keep the first row (census: none).
* **Realised correlation:** the owned R57 PIT S&P 500 panel
  `D:\Stock_Prediction_app_data\r57_alpha_discovery\panels\sp500_pit_panel_v1.npz` (`tr` total-return
  closes, `mem` PIT membership) with its dates from `sp500_pit_panel_v1.meta.json`. A missing panel
  is `DATA_HOLD`.
* **Traded instrument:** Norgate `SPY`, `StockPriceAdjustmentType.TOTALRETURN`, unpadded. The SPY
  session calendar is the decision calendar; COR3M is aligned to it and **never forward-filled**. A
  missing Norgate source is `DATA_HOLD`.

## 2. THE ONE FROZEN CELL

```
t                = a grid session (SPY calendar)
members(t)       = names with mem = 1 on session t-1
window(t)        = the 64 total-return closes t-64 .. t-1  ->  63 daily returns over t-63 .. t-1
usable(t)        = members(t) with all 63 returns finite and non-zero variance
                   (the 64 panel dates must be exactly the 64 SPY sessions, else NaN)
Zi               = name i's returns standardised by its own window mean and sd
rho(t)           = mean of the off-diagonal of  Z @ Z.T / 63   over usable(t)   (NaN if < 100 names)
premium(t)       = COR3M_close(t) / 100 - rho(t)
z(t)             = (premium(t) - mean(prior)) / sd(prior),  prior = the 36 grid slots BEFORE t,
                   >= 24 finite, sample sd, sd = 0 -> NaN     (the options_surface._z formula)
pos(t)           = +sign(z(t))           (0 if z = 0; NaN if z is NaN: no position, no cost)
entry            = SPY close of t+1      (never the signal close)
exit             = SPY close of t+22     (21 sessions held)
r(t)             = TR_SPY(exit) / TR_SPY(entry) - 1
net(t)           = pos(t) * r(t) - |pos(t)| * 2 * c
```

* **Grid:** anchored at the first SPY session with a finite COR3M close and a complete realised
  window inside the panel (2006-01-03), then every 21st SPY session while an exit exists on the
  calendar. Premium and z are computed on grid slots only. Decisions begin at the first slot with a
  finite z (2008-01-04); later slots with a NaN z count against the finite share.
* **Non-overlapping and contiguous:** the step equals the holding period, so each exit is the next
  entry.
* **Equal-weight realised correlation** over all usable PIT members. COR3M is Cboe's own
  construction from index and constituent option prices with its own constituent weighting; the
  level difference between the two legs is absorbed by the trailing z-score and is not modelled.
* **One cell, m_declared = 1.** No magnitude weighting, threshold, lookback, horizon, COR1M/COR6M,
  DSPX, VIX adjustment or sector leg is created, before or after results.

## 3. POINT-IN-TIME CONTRACT

COR3M is read at the close of t. Realised correlation uses closes through t-1 and membership on
t-1 (strictly prior). The z uses premiums on earlier grid slots only. The position is entered at
the close of t+1, so the return from the signal close to the entry close is never earned; the
return is measured strictly after entry.

## 4. WINDOWS

* **QUALIFICATION:** decisions whose entry lies in 2006-01-01..2018-12-31 (effective first decision
  2008-01-04; 132 decisions in the census).
* **Partitions (by entry date):** P1 2006-01-01..2012-12-31 (60 decisions), P2
  2013-01-01..2018-12-31 (72 decisions).
* **CONFIRMATION (untouched):** entries 2019-01-01 .. with exit on or before 2026-08-31 (91
  decisions), READ ONLY IF every qualification gate passes. Reported whole (the gate) and split
  descriptively at entry 2021-10-01 (33 backfill rows / 58 published rows).

## 5. COSTS

1 bp per side (canonical SPY rate, `SPY_PROXY_COST_BPS`, primary), 2 bp, and a 5 bp stress rung. A
full round trip (2 sides) is charged on every period with a non-zero position, whether or not the
sign changed.

**Disclosed, not modelled:** returns are SPY total returns rather than returns in excess of cash.
For a +/-1 sign book the difference is mean(pos) x the cash rate, and the census long share of 0.477
puts |mean(pos)| near 0.05. A short SPY position's general-collateral borrow fee is not charged
either.

## 6. MEASUREMENT - existing owners only

* Annualisation 252/21 = 12 periods a year; t via `alpha_agent.r63.sensitivity.nw_tstat(x, 0)`;
  drawdown `S._max_dd`; one-sided p from the same owner.
* **Timing increment over the passive long (gate 5), measured two ways, both required.** The
  passive long is the SPY total return `r` over each held span, on the same entry->exit spans:
  * volatility-matched (the catalog's control): `k = sd(net) / sd(r)` (sample), `a = net - k * r`;
  * regression-matched: `beta_hat = cov(net, r) / var(r)` (sample), `a = net - beta_hat * r`.

  Each gives t = `nw_tstat(a, 0)` and an annualised increment of mean(a) * 12. Gate 5 binds on the
  worse of the two t values and the worse of the two annualised increments.
  *Lead review before commit:* the draft measured only the regression-matched increment and
  labelled it volatility-matched. The catalog's literal control is restored, and the regression
  increment is kept as an additional requirement. Both changes only tighten the gate.
* Equal-risk increment over the incumbent: `intraday_alpha.equal_risk_daily` against
  `intraday_alpha.incumbent_daily_path`, with each period's net spread geometrically over 21
  sessions on a business-day index (forward-filled at most 20 days).
* Multiplicity: `alpha_agent.r63.sensitivity.bh_fdr`, q = 0.10, one-sided p of this cell.
* Constants `MATERIALITY_ANN_NET` (1.5 %/yr) and `MIN_EFFECTIVE_PERIODS` (36) are imported from
  `alpha_agent.alpha_recovery`, never re-declared.

## 7. QUALIFICATION GATES - in this order

1. **Data.** Cache hash mismatch or missing manifest, fewer than 95 % of qualification decisions
   with a finite z, or fewer than 36 finite decisions in either partition -> `DATA_HOLD`. (A missing
   COR3M cache, panel or Norgate source is `DATA_HOLD` before any computation.)
2. **Frozen sign.** Qualification mean net at 1 bp <= 0 -> `KILLED_WRONG_SIGN`.
3. **Standalone.** NW t < 2.0 -> `NO_EDGE`.
4. **Materiality.** Annualised net at 1 bp < 1.5 %/yr -> `KILLED_BELOW_MATERIALITY`.
5. **Timing increment over the passive long.** Against EITHER the volatility-matched or the
   regression-matched passive long (section 6): t < 2.0 or annualised increment < 1.5 %/yr ->
   `KILLED_NONINCREMENTAL`.
6. **Partition sign.** Annualised net at 1 bp <= 0 in P1 or in P2 -> `KILLED_UNSTABLE`.
7. **Multiplicity.** BH q = 0.10 over m = 8 (this cell + 7 inherited nulls at p = 1) fails ->
   `KILLED_MULTIPLICITY`.
8. **Cost survivability.** Annualised net at 5 bp < 1.5 %/yr -> `KILLED_BELOW_MATERIALITY`.
9. **Incumbent.** Equal-risk increment state OK and not positive after costs ->
   `NO_INCREMENTAL_INFORMATION_EDGE`. A `DATA_HOLD` state is reported, not gated.
10. **Untouched confirmation.** Mean net <= 0, annualised < 1.5 %/yr, NW t < 2.0, or one-sided
    p > 0.10 or missing -> `KILLED_UNSTABLE`. (Lead review: the draft's `(p or 1.0)` treated
    p = 0.0 as missing; it is now an explicit None check.)
11. Otherwise `QUALIFIED` (HUMAN gate: prospective registration). **CAPITAL ELIGIBLE = NO.**

The executor contract reports `statistic.lockbox_t` = the qualification NW t at 1 bp.

## 8. INHERITED NULLS (p = 1)

The index-level implied-state timing tests, exactly seven:

1. `R32|EQUITY_BETA_TIMING`
2. `R32|VOLATILITY_RISK_REGIME`
3. `R35|MARKET_IMPLIED_RISK_PREMIA`
4. `R36|VOL_TERM_EQUITY_TIMING`
5. `R63|VOLATILITY_EXPECTATIONS_IV|h1`
6. `R63|VOLATILITY_EXPECTATIONS_IV|h5`
7. `R63|VOLATILITY_EXPECTATIONS_IV|h21`

m = 8. Under BH at q = 0.10 this cell survives only with one-sided p <= 0.0125.

## 9. FALSIFICATION - and what will NOT be done

A null result means: the strictly trailing z-score of COR3M implied correlation over realised S&P 500
pairwise correlation, observed at a monthly close, carries no after-cost information about the next
month's S&P 500 total return beyond a volatility-matched passive long. Implied correlation then joins
the VIX level and term structure as a real premium with a worthless timing signal, and index-level
implied-state timing is closed for the derivatives domain.

**Forbidden after results:** reversing the sign; the premium LEVEL instead of its z-score; another z
window, minimum or realised-correlation lookback; cap-weighted, top-50 or sector-matched realised
correlation; COR1M, COR6M, COR1Y or DSPX substitutes; thresholds or magnitude sizing; long-only or
short-only books; weekly or quarterly horizons; futures legs; dropping 2008, 2020 or 2022; moving the
2021-10-01 descriptive boundary; gating on the published-period rows alone; any second cell; reading
the confirmation window after a qualification failure.

## 10. SAFETY

RESEARCH ONLY. No purchase, subscription, registration, promotion, capital allocation, portfolio
mutation, order, fill, backfill or live write. `capital_eligible` is always False.
