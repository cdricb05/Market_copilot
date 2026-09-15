# PREREGISTRATION - MONTH-END CURRENCY HEDGE REBALANCING AT THE WM/R FIX (EUR, JPY, CHF, CAD FUTURES)

**Mechanism id:** `FX_MONTH_END_EQUITY_HEDGE_REBALANCING_V1`
**Global candidate:** `GC_FX_MONTH_END_EQUITY_HEDGE_FIX_FLOW` (declared with this mechanism)
**Selected because:** after verdict 1 the rebuilt global frontier (COMPLETE, 124 of 124 identities) had
no agent-executable action and the agent checkpoint read `NO_ELIGIBLE_MECHANISM: declare new
mechanisms from the economic frontier`. Among free, owned-data mechanisms not closed in the ledger,
this is the one that adds an asset class (FX) and a mechanism class (FORCED_TRADING_FLOW) different
from verdict 1 (EQUITY_INDEX, RISK_PREMIUM_TIMING).
**Catalog:** `research/alpha_agent/MECHANISM_FRONTIER.json` (data_version 1)
**Run:** ALPHA_STRIKE_SEP15_V2, branch `alpha-strike-sep15-v2`.
**Status:** FROZEN. Written and committed BEFORE any currency return, window return, book or
t-statistic for this mechanism was computed on real data. The census (1.2) read session dates and
equity-index futures closes up to each signal session only. Not edited after results exist.

---

## 0. THE P&L MECHANISM

International equity investors hedge a fixed share of their foreign equity holdings with currency
forwards and reset those hedges at the month-end WM/Refinitiv 4 pm London fix. When a foreign equity
market outperforms the US in local terms over the month, US holders of that market must sell more of
its currency to keep their hedge ratio, and foreign holders of US equity need fewer dollar sales; the
dollar is bid against that currency into the fix. After the US outperforms, the reverse. Melvin and
Prins (2015) document the effect on 1997-2012 fix data. The hedgers are rule-bound and trade at one
benchmark regardless of price; the size of the adjustment scales with a relative equity return
everyone can observe.

**Frozen direction:** SELL the currency of the equity market that outperformed the S&P 500 month to
date; BUY the currency of the market that underperformed. A contradicted sign closes the mechanism; it
is never reversed.

### 0.1 Prior estate tests touching month end or currencies

| record | object | why it is different |
|---|---|---|
| `MONTH_END_BALANCED_REBALANCING_FLOW_V1` (NO_EDGE, t 0.56) | ES-versus-ZN spread signed against balanced funds' month-to-date equity-minus-bond return | a different flow (asset-allocation rebalancing), a different signed quantity and a different traded asset |
| `R32_EVENT_DRIVEN_CALENDAR`, `SAME_MONTH_SEASONALITY`, `r46_3_spx_turn_of_month` | unsigned month-end and month-of-year dummies | no sign from the hedge flow's size |
| `FX_CARRY`, `r51_fx_xs_carry_cip` | interest-differential currency ranking | no month-end or equity information |
| `INTL_INDEX_TREND_FX_REVERSAL_XA_CURVE`, `CROSS_MARKET_PREDICTION_R33_R34` | own-price FX timing, pooled price predictors | not a mandated flow |
| `MICROSTRUCTURE_ORDER_FLOW`, `FUTURES_INTRADAY_8_FAMILIES` | intraday order flow | not a benchmark-fix hedge adjustment |

Gate 5 demands an increment over the UNCONDITIONAL month-end dollar basket, so a generic month-end
dollar effect cannot pass under this name.

---

## 1. DATA - OWNED, $0

Norgate, unpadded, each with its `_CCB` companion: CME `&6E` (euro, 1999-01-04..), `&6J` (yen),
`&6S` (Swiss franc), `&6C` (Canadian dollar), `&ES` (E-mini S&P 500, 1997-09-09..); Eurex `&FESX`
(EURO STOXX 50, 1999-03-29..) and `&FSMI` (SMI, 1999-01-05..); CME `&NKD` (Nikkei 225 in dollars,
quanto, so its price tracks the yen-denominated index, 1990-09-26..); Montreal `&SXF` (S&P/TSX 60,
1999-09-08..). FXE and FXY total return for validation only. Cost convention: R38
`research_contract.json` `FX_FUTURE` = 3 bp per side.

### 1.1 Leg returns

Every futures leg uses the declared roll-free return of the estate
(`alpha_agent.alpha_recovery.month_end_rebalancing_flow.leg_returns`):

```
r_leg(t) = (CCB_close(t) - CCB_close(t-1)) / unadjusted_close(t-1)
idx_leg  = cumulative product of (1 + r_leg)
```

CME FX futures are quoted in dollars per unit of foreign currency, so a long future is long the
foreign currency.

### 1.2 Census before returns (signal only)

Calendar: sessions on which ES, 6E, 6J, 6S and 6C all trade - 6,956 sessions, 1999-01-04..2026-09-14.

| window | months | quarter-end | months with >= 3 finite pairs | median abs signal EUR / JPY / CHF / CAD | share abs > 1 % | pairwise signal correlation | all four same sign |
|---|---|---|---|---|---|---|---|
| QUALIFICATION 2000-2014 | 180 | 60 | 180 | 1.77 / 2.94 / 1.85 / 1.79 % | 0.70 / 0.83 / 0.68 / 0.72 | 0.12 .. 0.45 | 24.4 % |
| CONFIRMATION 2015-01..2026-08 | 140 | 46 | 140 | 1.69 / 2.22 / 2.05 / 1.54 % | 0.69 / 0.82 / 0.73 / 0.64 | 0.09 .. 0.55 | 31.4 % |

Lag-1 sign persistence of each signal: 0.45-0.55. Entry-to-exit calendar gaps (qualification): 1 day
148, 2 days 5, 3 days 25 (weekends), 4 days 2.

---

## 2. THE ONE FROZEN CELL (m_declared = 1)

For calendar month M with session list L (at least 8 sessions) and L_prev the previous month's list:
signal session `s = L[-3]`, entry at the close of `e = L[-2]`, exit at the close of `x = L[-1]` (the
session containing the 4 pm London fix). For each pair i in (EUR: 6E/FESX, JPY: 6J/NKD, CHF: 6S/FSMI,
CAD: 6C/SXF):

```
eq_i(t)   = idx of the foreign index future at its last close on or before t (stale if older than 5 calendar days)
d_i       = (eq_i(s) / eq_i(L_prev[-1]) - 1) - (idx_ES(s) / idx_ES(L_prev[-1]) - 1)
sigma_i   = std of r_6X over the 63 CME sessions ending at s (strictly through s) * sqrt(252)
w_i       = -sign(d_i) * 0.10 / sigma_i          (0 if d_i or sigma_i is not finite, or d_i = 0)
R_i       = idx_6X(x) / idx_6X(e) - 1
gross     = sum_i w_i * R_i
net       = gross - sum_i |w_i| * 2 * cost
U         = sum_i ( -0.10 / sigma_i ) * R_i      (the unconditional month-end dollar basket, same pairs)
```

One unit of 10 % annualised currency volatility per pair, summed over the finite pairs (no leverage
cap beyond that sizing). One window a month; windows never overlap. No second window, signal session,
currency, index, weighting, threshold, quarter-end-only cell or DXY leg is created, before or after
results.

## 3. POINT IN TIME

The signal reads closes through s; every foreign index future has closed before the entry close at e
(the next CME session); the volatility estimate ends at s; the return is measured strictly after
entry. A foreign holiday uses that index's last close within 5 calendar days; older is not finite.

## 4. WINDOWS

* **QUALIFICATION:** months whose signal session lies in 2000-01-01..2014-12-31. Halves 2000-2007 and
  2008-2014.
* **CONFIRMATION (untouched):** signal sessions 2015-01-01..2026-08-31 - after the February 2015 fix
  reform and the 2015 publication. READ ONLY IF every qualification gate passes.

## 5. COSTS

3 bp per side per leg (R38 `FX_FUTURE`, primary), charged on `|w_i|` for a full round trip every
window. Ladder 0 / 1 / 3 / 6 bp. Stress rung: 6 bp.

## 6. MEASUREMENT - existing owners only

* Annualisation at 12 windows a year; t and one-sided p via `alpha_agent.r63.sensitivity.nw_tstat(x, 0)`
  (windows do not overlap); Sharpe `mean / sd * sqrt(12)`; drawdown via `S._max_dd`.
* **Increment over the unconditional month-end dollar basket:** `beta_hat` = OLS slope of `net` on `U`;
  `a = net - beta_hat * U`; t via `nw_tstat(a, 0)`; annualised mean `12 * mean(a)`.
* Leave-one-currency-out: the same book with pair i's weight set to 0, for each i.
* Multiplicity: `alpha_agent.r63.sensitivity.bh_fdr`, q = 0.10, one-sided p; m = 5 (this cell plus
  MONTH_END_BALANCED_REBALANCING_FLOW_V1 and the three R32_EVENT_DRIVEN_CALENDAR dummies at p = 1).
* Diagnostics reported, never gated and never a rescue: gross book, hit rate, per-currency annualised
  net, quarter-end versus other months, turnover (`12 * sum |w|` per year) and capital usage.

## 7. GATES - in this order

1. **Data -> `DATA_HOLD`.** A futures series missing or without `_CCB`; fewer than 150 qualification or
   120 confirmation months with at least 3 finite pairs, or finite pairs in fewer than 95 % of
   qualification months; daily roll-free 6E correlation with FXE total return below 0.90 over
   2006-01..2014-12, or 6J with FXY below 0.90 over 2007-03..2014-12.
2. **Frozen sign.** Mean net at 3 bp <= 0 -> `WRONG_SIGN`.
3. **Standalone.** NW t < 2.0 -> `STANDALONE_T`.
4. **Materiality.** Annualised net at 3 bp < 1.5 %/yr -> `MATERIALITY`.
5. **Increment over the unconditional month-end dollar basket.** t < 2.0 or annualised mean < 1.5 %/yr
   -> `INCREMENT`.
6. **Stability.** Annualised net <= 0 in either half or in any leave-one-currency-out basket ->
   `STABILITY`.
7. **Drawdown.** Maximum drawdown of `net` worse than 1.5 x that of `U` -> `DRAWDOWN`.
8. **Multiplicity.** Fails BH q = 0.10 at m = 5 -> `MULTIPLICITY`.
9. **Cost survivability.** Annualised net at 6 bp < 1.5 %/yr -> `COST`.
10. **Untouched confirmation.** Mean net <= 0, NW t < 2.0 or annualised net < 1.5 %/yr at 3 bp ->
    `CONFIRMATION`.
11. Otherwise `QUALIFIED`, which raises a HUMAN gate (prospective registration, contract rule 16).

Verdicts: gate 1 -> `DATA_HOLD`; gates 2-10 -> `NO_EDGE` with the fired gate recorded; gate 11 ->
`QUALIFIED`. **CAPITAL ELIGIBLE = NO** in every case.

## 8. FALSIFICATION - and what will NOT be done

A null means: signing CME currency futures against the month-to-date foreign-minus-US equity return
over the session containing the month-end fix carries no after-cost increment over the unconditional
month-end dollar position.

**Forbidden after results:** reversing the sign; another signal session or holding window (L[-2]
signal, L[-4]..L[-1], the first session of the next month); quarter-end-only or large-signal-only
cells; thresholds on the signal; dropping or adding a currency (GBP, AUD, DXY); another volatility
lookback or target; dollar-neutral or equal-notional weights; ETF legs; excluding 2008, 2020 or 2022;
moving the halves or the confirmation boundary; relaxing any threshold; reading the confirmation after
a qualification failure.

## 9. SAFETY

RESEARCH ONLY. No purchase, subscription, credential, registration, promotion, capital allocation,
portfolio mutation, order, fill, backfill or live write. The live checkout
`C:\Users\binis\paper_trader` is not touched.
