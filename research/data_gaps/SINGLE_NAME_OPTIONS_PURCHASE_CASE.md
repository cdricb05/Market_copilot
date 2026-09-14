# PURCHASE CASE - SINGLE-NAME OPTION CHAINS FOR THE PIT S&P 500 (HUMAN GATE)

**Mechanism:** `SINGLE_NAME_OPTION_INFORMED_TRADING_V1` (catalog state `HUMAN_GATE_PURCHASE`)
**Data gap:** `SINGLE_NAME_OPTIONS_IV_SURFACE_HISTORY`
**Prepared by:** the Alpha Agent frontier, 2026-09-13. **Nothing was bought.** No download, batch
job, subscription or trial was started. The only calls were Databento `metadata.get_dataset_range`,
`metadata.get_cost` and `metadata.get_billable_size`, which bill nothing (`billed_endpoints_called:
[]`). The probe script and output are in the session scratchpad (`price_single_name_opra.py/.json`).

## 1. Why this is the gap that matters now

This session closed every free index-level derivatives signal: VIX level and term structure, VVIX
and SKEW conditioners, the SKEW tail-hedge index, relative index implied volatility (VXN/RVX) and
implied correlation (COR3M, NO_EDGE t 1.26). The derivatives domain's remaining mechanism is
**cross-sectional**: informed traders buy out-of-the-money puts before bad news, so a steep
single-name smirk and a put-call implied-volatility spread predict the stock (Xing, Zhang and Zhao
2010; Cremers and Weinbaum 2010). No owned or free source carries single-name option prices. The
owned OPRA data is SPY only, and the live SPY skew candidate is an index time series, not this
cross-section.

## 2. Exactly what would be bought

| item | specification |
|---|---|
| vendor / dataset | Databento `OPRA.PILLAR` (usage-based historical, no subscription) |
| schema | `cbbo-1m` (consolidated best bid and offer, one-minute), with implied volatility taken from the quote midpoint |
| snapshot | one minute, 15:59-16:00 ET, on the last session of each week |
| universe | full option chains (parent symbology `ROOT.OPT`) of the S&P 500 members on that date, from the owned R57 PIT panel, including later-delisted names |
| span | 2013-04-01 .. 2026-09-12 (the whole dataset range), about 700 weekly snapshots |
| not bought | `statistics` (open interest): not needed for the smirk or IV-spread signal, and it costs $21-27 per day from 2024 (2.1-2.6 GB a day) |

## 3. Measured cost (metadata.get_cost, 2026-09-13)

| sample Friday | PIT members | parents priced | cbbo-1m snapshot | bytes |
|---|---|---|---|---|
| 2013-04-12 | 500 | 497 | $0.1651 | 88.6 MB |
| 2015-03-06 | 502 | 499 | $0.3122 | 167.6 MB |
| 2017-02-03 | 505 | 505 | $0.3464 | 185.9 MB |
| 2019-01-04 | 505 | 505 | $0.4217 | 226.4 MB |
| 2020-11-27 | 505 | 505 | $0.0000 | 0 (day after Thanksgiving: 13:00 close, no 15:59 minute) |
| 2022-10-28 | 502 | 502 | $0.5788 | 310.7 MB |
| 2024-09-27 | 503 | 503 | $0.7179 | 385.4 MB |
| 2026-08-28 | 503 | 503 | $0.7682 | 412.4 MB |

The full chain is an upper bound; a near-the-money band would cost less.

* **Weekly snapshots, 2013-2026: about $290** (sample mean including the holiday zero) **to $332**
  (excluding it). It is a one-time cost.
* **Daily snapshots:** about $1,450-1,660 one-time.
* The cost of any purchase must be re-quoted with `metadata.get_cost` for the identical request
  immediately before it is placed. **Proposed hard cap: $400.**

## 4. Economics against the live paper NAV

The canonical `alpha_agent.r63.sourcing.break_even` is used, with the whole one-time cost charged as
if it were a single year's fee. NAV is $97,858.94 (desk ledger 2026-09-11, read only).

| one-time cost | fee share of NAV | break-even alpha, 21-session convention | break-even alpha, 5-session horizon | EXTREME_HURDLE (> 2 % of NAV) |
|---|---|---|---|---|
| $300 | 0.31 % | 1.54 %/yr | 3.34 %/yr | no |
| $332 (weekly) | 0.34 % | 1.57 %/yr | 3.37 %/yr | no |
| $450 | 0.46 % | 1.71 %/yr | 3.51 %/yr | no |
| $1,600 (daily) | 1.63 % | 3.00 %/yr | 4.80 %/yr | no |

* **The catalog's pre-purchase kill rule does not fire.** It stops the case if the priced cost of three
  or more years implies a break-even above 3.5 %/yr. The weekly purchase implies 1.57 %/yr under the
  purchase-case convention and 3.37 %/yr even at the 5-session trading-cost term.
* **The data are not the binding cost; trading is.** The catalog estimates a weekly
  cross-sectional book at 12.5 bp per side and about 40 % weekly turnover costs about 5 %/yr. The gross
  spread must exceed roughly 7 %/yr before the data fee matters.

## 5. R32 conditions - honest status

| condition | status |
|---|---|
| economic mechanism | MET: informed option demand, published and replicated |
| cheap proxy | NOT AVAILABLE: no owned or free single-name option source; the purchase itself is the cheap step (0.34 % of NAV) |
| marginal evidence | **NOT MET before purchase.** `purchase_case.assess_candidate` returns DO_NOT_BUY when no owned proxy shows value. That is the canonical rule, and this document does not override it; the human decides |
| exact data gap | MET (section 2) |
| point-in-time requirement | MET: exchange quotes are as-of by construction; universe from the owned PIT membership |
| survivorship / inactive coverage | MET: OPRA carries every listed root; the PIT panel includes delisted members |
| sample evaluation | about 700 weekly periods: qualification 2013-2019 and untouched confirmation 2020-2026, each far above MIN_EFFECTIVE_PERIODS 36 |
| incremental backtest | to be preregistered against the incumbent at 12.5 bp before any download is read |
| economic value | break-even 1.6-3.4 %/yr (section 4); trading cost is the real hurdle |
| licensing and retention | **UNVERIFIED:** Databento historical licence terms for research storage, and whether the account has free credit, must be read by the human |

## 6. Risks the human should weigh

* **Post-publication decay.** The published spreads are from pre-2010 samples, and the window bought
  here starts in 2013.
* **The effect may sit in small or illiquid names** with wide option quotes; the S&P 500 universe is
  the harder test.
* **Building IV from quotes takes work:** forwards from put-call parity, dividends, and early exercise
  on American single-name options. About 12 working days are budgeted.
* **Implementation.** A weekly long-short book needs short selling; the paper book's ability to hold
  shorts is a separate governance question.

## 7. The decision requested

**Approve or decline a one-time Databento OPRA.PILLAR historical purchase, re-quoted before
placement and capped at $400, of weekly 15:59 ET `cbbo-1m` full-chain snapshots for PIT S&P 500
members, 2013-04 to 2026-09.**

* **If approved:** preregister first, then acquire through the existing `databento_acquisition`
  owner's priced-signature gate, then execute through the Alpha Agent.
* **If declined:** the derivatives domain is closed at the purchase gate, and the frontier continues
  with the remaining free mechanisms.

## 8. Global opportunity-cost comparison (added 2026-09-14)

No purchase recommendation may be made without comparing the purchase to the global top five
(`alpha_agent.r59.global_frontier`, artifact
`D:\Stock_Prediction_app_data\r59_autonomous_alpha\agent\global_multi_asset_frontier.json`). This
case was priced when the mechanism frontier could see only its own catalog. Every owner is now
reconciled (123 of 123 candidate identities) and the verdict is **`PURCHASE_NOT_GLOBALLY_JUSTIFIED`**:

| global rank | opportunity | asset class | opportunity-cost score | next action |
|---|---|---|---|---|
| 1 | reversed SPY near-expiry put-call skew (next open) | EQUITY_INDEX | 0.6137 | accrue TRUE_FORWARD evidence (passive, costs nothing) |
| 2 | dated-contract FX carry (R51 challenger, R63/R64 cells, Alpha Recovery cadence) | FX | 0.4524 | human-gated forward registration |
| 3 | merger arbitrage on all-cash targets | US_EQUITY | 0.3458 | preregister, build and execute (free) |
| 4 | unconditional month-end ZN long (open human judgement) | RATES | 0.3391 | human-authorised preregistration on untouched 2017-2026 |
| 5 | R39/R40 cross-asset shadows | CROSS_ASSET | 0.3043 | accrue forward evidence (passive) |
| **8** | **this purchase** | US_EQUITY | **0.2875** | $332 purchase plus about 12 research days |

The purchase loses to advancing FX carry (a free human decision) and to the free merger-arbitrage and
rates preregistration actions. Its evidence is also the thinnest of the eight: nothing measured, no
untouched window, no multiplicity charge, and a trading-cost hurdle of about 5 %/yr. **Recommendation
to the human: do not buy now.** The gate stays open. The comparison is recomputed on every agent
iteration and flips to justified only when no research action or human decision above it scores higher.

## 9. Human decision recorded (2026-09-14)

**`SINGLE_NAME_OPTIONS_PURCHASE = DEFERRED`** - currently ranked below higher-value global
opportunities. Nothing was purchased, subscribed or trialled.

The decision is recorded in the catalog's reconciliation (`global_reconciliation.human_decisions`,
`HD_20260914_SINGLE_NAME_OPTIONS_PURCHASE`) with `resurface_when = PURCHASE_GLOBALLY_JUSTIFIED`. The
agent no longer lists this purchase among the gates waiting on a person. It resurfaces on its own
only if the global opportunity-cost comparison turns to `PURCHASE_GLOBALLY_JUSTIFIED`.
