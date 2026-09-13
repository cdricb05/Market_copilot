# PREREGISTRATION - COMMODITY INDEX ROLL-WINDOW PRESSURE

**Mechanism id:** `COMMODITY_INDEX_ROLL_WINDOW_PRESSURE_V1`
**Catalog:** `research/alpha_agent/MECHANISM_FRONTIER.json` (data_version 2)
**Selected by:** the Alpha Agent mechanism frontier (next buildable rank after four closed mechanisms).
**Status:** FROZEN. Written and committed BEFORE any spread return, book or t-statistic for this
mechanism was computed on real data. Not edited after results exist.

---

## 0. THE P&L MECHANISM

Commodity index funds tracking the S&P GSCI and the Bloomberg Commodity Index must sell their
nearby contracts and buy the next contracts on published business days each month (GSCI days
5-9, BCOM days 6-10). The flow is price-insensitive and scheduled, so liquidity providers charge
for absorbing it: the nearby leg should cheapen relative to the deferred leg during the window
(Mou 2011; Bessembinder, Carrion, Tuttle and Venkataraman 2016). **Frozen direction:** LONG the
deferred, SHORT the nearby through the window earns. A contradicted sign closes the mechanism.

### 0.1 Census before any return (contract identity and costs only)

**Roll timing of the owned layer.** The R38 native contract layer rolls its held contract after
business day 10 in >= 95 % of months for HO, RB, NG, GC, SI, HG, PL, PA, ZC, ZS, ZW, KE, ZL, ZM,
KC, SB and CT, so during the index window its held contract is the nearby being sold and its
`ret2` the deferred being bought. CL (60 %), BRN (67 %) and HE (84 %) roll INSIDE the window, GAS
and LE roll BEFORE it, and CC / GF are mixed: **all seven are excluded before results.**

**Eligibility** (entry at the close of business day 3, exit at the close of business day 10, on each
market's own session calendar; a market-month is skipped if the held contract changes in that span
or a front/deferred return is missing):

| market | months | eligible | skipped for roll | missing return | R38 cost bp/side |
|---|---|---|---|---|---|
| HO | 574 | 572 | 0.0 % | 0.3 % | 5 |
| RB | 251 | 251 | 0.0 % | 0.0 % | 5 |
| NG | 437 | 423 | 3.2 % | 0.0 % | 5 |
| GC | 562 | 562 | 0.0 % | 0.0 % | 5 |
| SI | 582 | 561 | 0.0 % | 3.6 % | 5 |
| HG | 581 | 558 | 0.0 % | 4.0 % | 8 |
| PL | 578 | 566 | 0.0 % | 2.1 % | 5 |
| PA | 539 | 531 | 0.0 % | 1.5 % | 5 |
| ZC | 584 | 581 | 0.0 % | 0.5 % | 8 |
| ZS | 585 | 581 | 0.0 % | 0.7 % | 8 |
| ZW | 584 | 581 | 0.0 % | 0.5 % | 8 |
| KE | 568 | 566 | 0.0 % | 0.4 % | 8 |
| ZL | 584 | 581 | 0.0 % | 0.5 % | 8 |
| ZM | 583 | 581 | 0.0 % | 0.3 % | 8 |
| KC | 573 | 572 | 0.0 % | 0.2 % | 10 |
| SB | 575 | 574 | 0.0 % | 0.2 % | 10 |
| CT | 575 | 571 | 0.2 % | 0.5 % | 10 |

Markets per month: QUALIFICATION 1995-2014, 240 months, min 13 / median 16 / max 17;
CONFIRMATION 2015-2026-08, 140 months, 17 in every month.

**The cost hurdle, declared now.** Two legs, two sides each, at the owned R38 outright cost: 20 bp
(5 bp markets) to 40 bp (10 bp markets) per market-month, about **2.4-4.8 %/yr per unit of leg
notional**. The owned model prices outright legs, not exchange-listed calendar spreads; no spread
cost model is owned, so the outright cost is used and the result is conservative in that respect.

### 0.2 Prior tests

`CALENDAR_TERM_STRUCTURE_ROLL_STATE_R59` scored each market's OWN roll flags and dated-contract
slope (carry); `COMMODITY_CURVE_CARRY_SPREADS` and `INTER_COMMODITY_RV_SEASONALITY` traded curve
shape and seasonal relative value; no estate test used the index roll schedule. The first two are
inherited at p = 1.

---

## 1. DATA - OWNED, $0

`D:\Stock_Prediction_app_data\native_futures_r38\r38_native_futures_information_frontier_v4\native_contract_layer\<MKT>.csv`
(`Date, ret, close, held, volume, open_interest, ret2, slope_ann`), read through the paths and the
cost table of `alpha_agent.r59.native` (`load_meta().cost_bps_per_side`). `ret` is the held
(nearby) contract's return and `ret2` the deferred contract's; returns are excess of collateral.

## 2. THE ONE FROZEN CELL

For market k and calendar month M with at least 10 sessions of k:

```
entry        close of business day 3 of M      (index 2 of the month's sessions)
window       returns of business days 4..10    (indices 3..9; seven sessions)
eligible     held contract identical on business days 3..10, ret and ret2 finite on 4..10
spread_kM    prod(1 + ret2[4..10]) - prod(1 + ret[4..10])        (long deferred, short nearby)
net_kM(c)    spread_kM - 4 * c_k / 10^4                           (two legs, two sides)
book_M(c)    equal-weight mean of net_kM(c) over the eligible markets of M
```

One observation per calendar month; months never overlap. **One cell, m_declared = 1.** No market
weighting, other window, entry day, GSCI-only or BCOM-only cell, spread-instrument cost or signal
conditioning is created, before or after results.

## 3. POINT-IN-TIME CONTRACT

The roll schedule is published years ahead; entry uses only the calendar and the business-day-3
close; returns are measured strictly after entry. The universe was fixed from contract-identity
facts alone (section 0.1).

## 4. WINDOWS

* **QUALIFICATION:** months 1995-01 .. 2014-12. Halves 1995-2009 and 2010-2014.
* **CONFIRMATION (untouched):** months 2015-01 .. 2026-08, READ ONLY IF every qualification gate passes.

## 5. COSTS

Primary: the R38 per-market cost per side (5 / 8 / 10 bp). Stress: twice the R38 cost. Gross (0) is
reported.

## 6. MEASUREMENT - existing owners only

* Annualisation 12 months a year; t via `alpha_agent.r63.sensitivity.nw_tstat(x, 0)`; drawdown
  `S._max_dd`.
* **Passive spread control.** For each market, `d_k` = mean daily `(ret2 - ret)` over the same
  evaluation window's sessions that are NOT in any roll window (business days 4..10) and not on a
  held-contract change. Excess `x_kM = net_kM - 7 * d_k`; the excess book is the equal-weight
  mean. This separates index-roll pressure from the ordinary drift of a long-deferred /
  short-nearby spread (carry), which the estate has already found to be a premium without timing.
* **Leave one market out.** The primary book recomputed without each market in turn.
* Equal-risk increment over the incumbent: `intraday_alpha.equal_risk_daily`, each month's net
  spread evenly over the seven business days ending at business day 10 of that month.
* Multiplicity: `alpha_agent.r63.sensitivity.bh_fdr`, q = 0.10, one-sided p.

## 7. QUALIFICATION GATES - in this order

1. **Data.** Fewer than 36 qualification months with >= 10 eligible markets, or fewer than 95 % of
   qualification months with >= 10 eligible markets -> `DATA_HOLD`.
2. **Frozen sign.** Mean primary net <= 0 -> `KILLED_WRONG_SIGN`.
3. **Standalone.** NW t < 2.0 -> `NO_EDGE`.
4. **Materiality.** Annualised net < 1.5 %/yr -> `KILLED_BELOW_MATERIALITY`.
5. **Passive spread control.** Excess book NW t < 2.0 or annualised < 1.5 %/yr ->
   `KILLED_NONINCREMENTAL`.
6. **Halves.** Annualised net <= 0 in 1995-2009 or in 2010-2014 -> `KILLED_UNSTABLE`.
7. **Not one market.** Leave-one-market-out NW t < 1.0 for any market -> `KILLED_UNSTABLE`.
8. **Multiplicity.** BH q = 0.10 over m = 3 (this cell + 2 inherited nulls at p = 1) fails ->
   `KILLED_MULTIPLICITY`.
9. **Cost survivability.** Annualised net at twice the R38 cost < 1.5 %/yr ->
   `KILLED_BELOW_MATERIALITY`.
10. **Incumbent.** Equal-risk increment state OK and not positive -> `NO_INCREMENTAL_INFORMATION_EDGE`.
11. **Untouched confirmation.** Mean <= 0, annualised < 1.5 %/yr, NW t < 2.0 or one-sided p > 0.10
    -> `KILLED_UNSTABLE`.
12. Otherwise `QUALIFIED` (HUMAN gate). **CAPITAL ELIGIBLE = NO.**

## 8. FALSIFICATION - and what will NOT be done

A null result means: holding a long-deferred / short-nearby spread through the published commodity
index roll window earns nothing beyond outright transaction costs and the spread's ordinary drift.

**Forbidden after results:** reversing the sign; another entry day, window length or exit day;
GSCI-only or BCOM-only windows; re-admitting CL, BRN, HE, GAS, LE, CC or GF; dropping markets;
market weights; a spread-instrument or halved cost; excluding years; any second cell; reading the
confirmation window after a qualification failure.

## 9. SAFETY

RESEARCH ONLY. No purchase, subscription, registration, promotion, capital allocation, portfolio
mutation, order, fill, backfill or live write.
