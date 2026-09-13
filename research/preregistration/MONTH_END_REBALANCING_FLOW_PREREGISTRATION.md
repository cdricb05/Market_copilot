# PREREGISTRATION - MONTH-END BALANCED-FUND REBALANCING FLOW (EQUITY VERSUS BOND FUTURES)

**Mechanism id:** `MONTH_END_BALANCED_REBALANCING_FLOW_V1`
**Catalog:** `research/alpha_agent/MECHANISM_FRONTIER.json` (data_version 2)
**Selected by:** the Alpha Agent mechanism frontier (`alpha_agent.r59.mechanisms`).
**Status:** FROZEN. Written and committed BEFORE any window, forward return, book or t-statistic
for this mechanism was computed on real data. Not edited after results exist.

---

## 0. THE P&L MECHANISM

Pension, target-date and balanced funds hold fixed policy weights in equities and bonds and
restore them near each month end. When equities have outperformed bonds month to date, their
mandated selling of equities and buying of bonds should push the equity-minus-bond return
negative over the last sessions of the month, and the reverse after equities underperform
(Harvey, Mazzoleni and Melone 2025). The rebalancers are rule-bound and price-insensitive; the
size of their trade scales with a relative return everyone can observe.

**Frozen direction:** SELL the month-to-date outperformer. A contradicted sign closes the
mechanism; it is never reversed.

### 0.1 Prior tests on month-end information

| record | object | result |
|---|---|---|
| R32 EVENT_DRIVEN | UNCONDITIONAL turn-of-month, quarter-end and triple-witching dummies on index exposure | 4.57 % net, t vs vol-matched -1.09 |
| Stage 14 SAME_MONTH_SEASONALITY | per-name month-of-year return | IC t 2.50, spread t 1.65, FDR fail |
| r46_3_spx_turn_of_month | unconditional SPX window, forward pending | 3 matured observations |

None signs a position by the month-to-date equity-versus-bond return. All five enter the
inherited multiplicity at p = 1 (section 8), and gate 5 requires an increment over the
UNCONDITIONAL month-end spread so the closed calendar effect cannot pass under this name.

---

## 1. DATA - OWNED, $0

Norgate continuous futures, unpadded: `&ES` (E-mini S&P 500, 1997-09-09..) and `&ZN` (10-year
Treasury note, 1982-05-03..), with their `_CCB` companions; SPY and IEF total return
(2002-07-26..) for validation only.

### 1.1 The data trap, measured before any return

`&ES` / `&ZN` are the UNADJUSTED front-contract continuation (1997 ES closes at 944; 1982 ZN at
63.6). `&ES_CCB` / `&ZN_CCB` are DIFFERENCE back-adjusted (`&ZN_CCB` is negative in 1982). A
percent return from the unadjusted series contains every roll gap, and ZN rolls in the last
sessions of February, May, August and November - inside this mechanism's window. Declared leg
return:

```
r_leg(t) = (CCB_close(t) - CCB_close(t-1)) / unadjusted_close(t-1)
```

The difference-adjusted change removes the roll gap; the unadjusted prior close is the price of
the contract actually held. **Data gate:** over 2002-08..2014-12 the daily correlation of the ES
leg with SPY total return must be >= 0.95 and of the ZN leg with IEF total return >= 0.85, or
the verdict is `DATA_HOLD`. (The unadjusted series already measured 0.98-0.99 and 0.92-0.95.)

### 1.2 Census (signal-only)

| window | months | median abs MTD relative return | share above 2 % | lag-1 sign persistence | quarter-end months |
|---|---|---|---|---|---|
| QUALIFICATION 1998-01..2014-12 | 204 | 3.31 % | 65.7 % | 0.522 | 68 |
| CONFIRMATION 2015-01..2026-08 | 140 | 2.87 % | 67.9 % | 0.590 | 46 |

---

## 2. THE ONE FROZEN CELL

Calendar: sessions on which BOTH `&ES` and `&ZN` have a bar. For calendar month M with session
list L (at least 8 sessions): signal session `s = L[-5]`, entry `e = L[-4]`, the window holds
`L[-3], L[-2], L[-1]` and exit `x` = the first session of month M+1.

```
idx_leg        = cumulative product of (1 + r_leg)
mtd_rel(s)     = (idx_ES(s)/idx_ES(L_prev[-1]) - 1) - (idx_ZN(s)/idx_ZN(L_prev[-1]) - 1)
sigma_leg(s)   = std of r_leg over the 63 sessions ending at s (strictly through s) * sqrt(252)
w_ES           = -sign(mtd_rel) * 0.10 / sigma_ES
w_ZN           = +sign(mtd_rel) * 0.10 / sigma_ZN
window return  = w_ES * (idx_ES(x)/idx_ES(e) - 1) + w_ZN * (idx_ZN(x)/idx_ZN(e) - 1)
net            = window return - (|w_ES| + |w_ZN|) * 2 * cost
```

Each leg is scaled to a 10 % annualised volatility using only information through the signal
close. One window per month; windows never overlap. **One cell, m_declared = 1.** No second
window length, threshold, weighting, quarter-end-only cell or instrument pair is created, before
or after results.

## 3. POINT-IN-TIME CONTRACT

The signal reads closes through s; the position is entered at the close of e (the next session);
the volatility estimate ends at s; the return is measured strictly after entry.

## 4. WINDOWS

* **QUALIFICATION:** months whose signal session lies in 1998-01-01..2014-12-31.
  Stability groups: quarter-end months (Mar/Jun/Sep/Dec) versus other months; halves
  1998-2006 and 2007-2014.
* **CONFIRMATION (untouched):** signal sessions 2015-01-01..2026-08-31, READ ONLY IF every
  qualification gate passes.

## 5. COSTS

1 bp per side per leg (primary), 2 bp, and a 5 bp stress rung, charged on the absolute
volatility-scaled weights for a full round trip every window.

## 6. MEASUREMENT - existing owners only

* Annualisation at 12 windows per year; t via `alpha_agent.r63.sensitivity.nw_tstat(x, 0)` (the
  windows do not overlap); drawdown via `S._max_dd`.
* **Increment over the unconditional month-end spread:** `U = 0.10/sigma_ES * r_ES(window) -
  0.10/sigma_ZN * r_ZN(window)` (always long equity, short bonds); `beta_hat` = OLS slope of the
  net window return on U; timing series `a = net - beta_hat * U`; its t is `nw_tstat(a, 0)`.
* Equal-risk increment over the incumbent: `alpha_agent.alpha_recovery.intraday_alpha.equal_risk_daily`,
  each window's net spread evenly (geometrically) across its four sessions.
* Multiplicity: `alpha_agent.r63.sensitivity.bh_fdr`, q = 0.10, one-sided p.

## 7. QUALIFICATION GATES - in this order

1. **Data.** Finite on < 95 % of qualification months, fewer than 36 months in either stability
   group, or a failed data-validation correlation (1.1) -> `DATA_HOLD`.
2. **Frozen sign.** Mean net <= 0 -> `KILLED_WRONG_SIGN`.
3. **Standalone.** NW t < 2.0 -> `NO_EDGE`.
4. **Materiality.** Annualised net at 1 bp < 1.5 %/yr -> `KILLED_BELOW_MATERIALITY`.
5. **Increment over the unconditional month-end spread.** Timing t < 2.0 or its annualised mean
   < 1.5 %/yr -> `KILLED_NONINCREMENTAL`.
6. **Stability.** Annualised net <= 0 in either quarter-end or other months, or in either half ->
   `KILLED_UNSTABLE`.
7. **Multiplicity.** BH q = 0.10 over m = 6 (this cell plus 5 inherited nulls at p = 1) fails ->
   `KILLED_MULTIPLICITY`.
8. **Cost survivability.** Annualised net at 5 bp < 1.5 %/yr -> `KILLED_BELOW_MATERIALITY`.
9. **Incumbent.** Equal-risk increment state OK and not positive after costs ->
   `NO_INCREMENTAL_INFORMATION_EDGE` (DATA_HOLD reported, not gated).
10. **Untouched confirmation.** Mean net <= 0, annualised < 1.5 %/yr, NW t < 2.0 or one-sided
    p > 0.10 -> `KILLED_UNSTABLE`.
11. Otherwise `QUALIFIED`, which raises a HUMAN gate (prospective registration, contract
    rule 16). **CAPITAL ELIGIBLE = NO** in every case.

Inherited nulls: R32 turn-of-month, quarter-end and witching dummies (3); SAME_MONTH_SEASONALITY
(1); r46_3_spx_turn_of_month (1).

## 8. FALSIFICATION - and what will NOT be done

A null result means: signing an equity-bond futures spread against the month-to-date relative
return over the last three sessions of the month carries no after-cost increment over the
unconditional month-end spread.

**Forbidden after results:** reversing the sign; another window (last 1, 2, 5 sessions, or
excluding the first session of the next month); quarter-end-only or large-move-only cells;
thresholds on the relative return; another volatility lookback or target; dollar-neutral
weights; SPY/IEF or TLT as the traded legs; excluding 2008, 2020 or 2022; any second cell;
reading the confirmation window after a qualification failure.

## 9. SAFETY

RESEARCH ONLY. No purchase, subscription, registration, promotion, capital allocation,
portfolio mutation, order, fill, backfill or live write.
