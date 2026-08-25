# Release 43 — Global Alpha Offensive

- **Date:** 2026-08-24
- **Branch:** `stage19-controlled-rebalance` (base commit `e4d23aa`, the
  Release-42 closeout — verified local == remote, the inherited search
  burden re-derived from the R41 ledger's own bytes as 230 + 59 = **289**,
  and the R42 premise re-read from `R42_FINAL_VERDICT.json` — **before any
  Release-43 code existed**)
- **Owner package:** `alpha_agent/r43/` (14 modules)
- **Regression:** `tests/test_release43_global_alpha_offensive.py` (50 tests)
- **Audit guard:** `check_release43_global_alpha_offensive` (41 blocking
  invariants)
- **Research root:** `D:\Stock_Prediction_app_data\global_alpha_offensive_r43\`
- **Campaign:** `r43_global_alpha_offensive_v1`
- **Frozen contract:** `a34bffd0058a37f6082a1773f57c8727c9f19963f31a64407fc3f0597d7dda29`,
  written **before the first Release-43 number existed**

Release 42 prosecuted one candidate to destruction. Release 43 searched
thirteen lanes across rates, commodities, FX, equities, volatility, credit,
events, cross-asset relations and market structure, and found the same
shape in three unrelated places.

## The one-sentence answer

**Every premium this estate can measure is real; every timing signal laid on
top of one is worth zero — and the single candidate that cleared the Zone-B
gate was refuted by the lockbox at t −0.16.**

## The correction Release 43 exists to make

The tempting reading of R42 — *capital cost kills carry* — is false, and
believing it would destroy real edges that do not exist. R42's kill is a
property of **unremunerated collateral**, not of carry:

| collateral | example | committed capital earns | correct correction |
|---|---|---|---|
| `UNREMUNERATED_FULLY_FUNDED` | crypto cash-and-carry | **nothing** | subtract the risk-free rate **and** rescale |
| `REMUNERATED_MARGIN` | any exchange-traded futures book | **the risk-free rate** (T-bills at an FCM) | **rescale only** |
| `FUNDED_LONG_SHORT_EQUITY` | cash-neutral equity L/S | a short rebate **below** rf | subtract the **shortfall** |

This is declared in the frozen contract, before any result, and it is
implemented as ONE equation with two contract parameters — committed capital
`K` and the fraction `rho` of the risk-free rate that capital earns:

```
pnl_on_capital(t) = (gross(t) - cost(t) - borrow(t)) / K
benchmark(t)      = on(t) * rf(t) * (1 - rho)
excess(t)         = pnl_on_capital(t) - benchmark(t)
```

Both prior conventions are **exact special cases**, and the regression
proves it rather than asserting it:

| convention | `K` | `rho` | reproduces | worst_abs_diff |
|---|---|---|---|---|
| R41 per-notional, zero control | 1.00 | 1.0 | `alpha_agent.r41.evidence` | **0.0** |
| R42 committed capital, cash control | 1.35 | 0.0 | `alpha_agent.r42.capital` | **0.0** |

And the arithmetic that follows is the answer to *"did any old carry survive
the R42 judge?"*: for a margin book the correction multiplies gross, cost
and return on capital by the **same** factor, so it moves the LEVEL and
leaves the t-statistic and the Sharpe ratio **exactly** unchanged
(`all_t_unchanged = True`, verified on every Track-A candidate).

> **Nothing R41 killed can be resurrected by re-quoting it, and nothing R42
> killed transfers to a futures book.** The estate now has one judge for
> both.

## Track A — the carry rejudgment R42 asked for

Six carry books, quoted on every declared denominator. Exchange margin
implies 7–32× leverage, 39–131 %/yr volatility and a −100 % drawdown, so a
**risk-sized** denominator (10 %/yr target vol, measured on the fitting zone
only) is reported alongside — strictly harsher, disclosed as a post-freeze
addition, and the only column an investor could survive.

| book | Zone-B on margin | t | risk-sized excess | vol | max DD | increment over vol-matched passive |
|---|---|---|---|---|---|---|
| RATES_CARRY continuous | +30.99 % | **2.07** | **+6.31 %** | 12.0 % | −25.4 % | **−18.75 %/yr (t −1.74)** |
| FX_CARRY continuous | +15.44 % | 0.76 | +2.10 % | 10.5 % | −35.9 % | +1.17 %/yr (t **0.06**) |
| ALL_ASSET continuous | +7.70 % | 0.76 | +2.17 % | 10.8 % | −39.5 % | −9.28 %/yr (t −0.50) |
| RATES_CARRY band | +3.45 % | 0.16 | +0.54 % | 13.4 % | −41.2 % | −68.73 %/yr (t −2.59) |
| FX_CARRY band | +4.18 % | 0.12 | +0.46 % | 14.4 % | −49.1 % | −19.99 %/yr (t −0.62) |

**Not one carry book beats a volatility-matched passive long of its own
markets.** The R36 FX carry survivor — rank IC 0.155 at t 7.97, the
strongest predictive result in the estate's history — pays an increment of
+1.17 %/yr at **t 0.06**. It predicts beautifully and pays nothing.

## Track E — the one candidate, and the lockbox

The binding constraint on an owned-data RV book is **turnover**, not
information. R41's best international-rates carry portfolio earned 0.115 %/yr
on notional at t 1.78 while paying 0.117 %/yr in cost — **50 % of gross**.
The economically motivated fix is not a better signal but a different
expression: enter on conviction, hold through the noise. Measured on Zone A
across both curve families, a hysteresis band cuts turnover to a **median
19 %** of the continuous book's and the cost share of gross by roughly four.

That produced the release's only Zone-B survivor:

| `c43_986b5eb34ec6` — cross-sectional rates carry, 1.5σ/0.5σ band | value |
|---|---|
| Zone-A t (screen) | 1.89 |
| Zone-B excess on committed capital | **+4.18 %/yr** |
| Zone-B t (HAC) | **2.69** |
| Sharpe / turnover / cost share of gross | 0.64 / 3.1×yr / 17 % |
| positive at 2× cost / 3× cost | +3.30 %/yr (t 2.12) / +2.42 %/yr (t 1.55) |
| kill battery | **survives all 14 tests** |
| placebo (200 block-shuffled draws, turnover-matched) | placebo \|t\| p95 **1.89** vs candidate 2.69; empirical p **0.005** — the signal is **not** a turnover artefact |
| block bootstrap | p 0.003 |
| **increment over vol-matched passive** | **+0.64 %/yr, t 0.40** |
| leave-one-tag-out (drop cross-country) | t 2.69 → **0.037** (1.4 % retained) |
| factor-residual t | 1.51 — above the research bar (1.5), **below** the qualified bar (2.0) |
| **ZONE_C (lockbox, one access)** | **−0.24 %/yr, t −0.16** |

The placebo and bootstrap results matter for what they rule *out*: this is
not a turnover artefact and not a chance draw. What kills it is the pair of
economic facts underneath — the signal adds nothing over the exposure, and
the exposure itself does not repeat.

It cleared its gate, survived every attempt to destroy it, and then the one
zone reserved for exactly this purpose refuted it. `HISTORICAL_ALPHA_RESULT
= FAIL`. No shadow was frozen: the contract's own standard for a shadow is
*sufficient historical credibility to justify future evidence collection*,
and a candidate refuted by the lockbox with an increment of t 0.40 has none.

## Every other lane

| lane | outcome | burden spent |
|---|---|---|
| **F commodity curves** | 94 structures (82 calendar + 12 inter-commodity crack/crush/feed/metals). **Every** Zone-A screen negative; the reversed book is flat because cost, not sign, is binding | **0** |
| **H event-driven** | The estate's **first PIT macro event calendar** — 2,916 scheduled release dates, 1996→2026, 8 release types, acquired at $0. The two mirror rules solve in closed form: the effect is **REVERSION**, and cost is a **median 5.6×** its gross size on daily bars | **0** |
| **I cross-asset** | 12 relations, each predeclared **with its direction** before measurement. No Zone-A screen reached the bar | **0** |
| **J technical structure** | Named Fibonacci levels earn t 1.66 / 2.04 on Zone A — and the **predeclared placebo levels earn more** (t 2.13 / 2.48). Both collapse on Zone B (t 0.20, −1.60) | 2 |
| **L equity market-neutral** | 1,198 survivorship-safe symbols, membership resolved **per date**, TOTALRETURN, beta-neutral on a causal rolling beta. Short-horizon residual reversal: Zone A +20.4 % at t 3.00 → **Zone B t −0.39**. It worked before 2012 and does not now | 2 |

## The data frontier — two walls opened, five confirmed

**Opened at $0, using entitlements the estate already owned:**

1. **Historical option prices.** Polygon serves per-contract daily
   aggregates *and* the **expired**-contract reference, so the option
   universe is survivorship-safe. The vendor's 403 on greeks is **not** the
   wall — implied volatility inverts locally from the option's close, its
   strike and expiry, an owned Norgate underlying and an owned FRED rate.
   The wall is **history**: a ~2-year rolling window at 5 requests/minute,
   below this estate's own minimum fitting zone plus a judged zone.
2. **Scheduled macro release dates**, 1996→2026, which opened Track H.

**Confirmed binding, each probed live rather than asserted:**

| wall | blocker | evidence |
|---|---|---|
| options history | `HISTORICAL_DATA_UNAVAILABLE` / `PAYMENT_REQUIRED` | 403 "plan doesn't include this data timeframe"; Alpha Vantage `HISTORICAL_OPTIONS` returns a premium notice |
| analyst vintages | `HISTORICAL_DATA_UNAVAILABLE` | every reachable endpoint serves **current** consensus; the estate's own prospective ledger is under one month old |
| native intraday futures | `PAYMENT_REQUIRED` | no $0 route without an account, licence or payment detail |
| native credit | `LICENCE_REQUIRED` **and newly narrowed** | the ICE BofA OAS family's own FRED metadata: *"Starting in April 2026, this series will only include 3 years of observations."* Requesting from 1990 returns 785 rows from 2023-08 |
| maker microstructure | `HISTORICAL_DATA_UNAVAILABLE` | inherited from R42 and re-verified; no fill fabricated |
| crypto non-carry | `ACCOUNT_REQUIRED` | re-probed; research access to public data is not permission to trade |

## Result axes (never collapsed)

| axis | result |
|---|---|
| SYSTEM_RESULT | **PASS** — 50/50 R43, 38/38 R42, 23/23 R41, 28/28 R40, audit `--strict` exit 0 |
| CAPITAL_TREATMENT_RESULT | **R43_COLLATERAL_REMUNERATION_IS_THE_DECIDING_TERM** |
| CARRY_REJUDGMENT_RESULT | **R43_NO_CARRY_BEATS_ITS_OWN_PASSIVE_CONTROL** |
| RATES_RV_RESULT | **R43_ZONE_B_CANDIDATE_REFUTED_ON_ZONE_C** |
| COMMODITY_CURVE / CROSS_ASSET | no candidate advanced, **zero burden** |
| EVENT_DRIVEN_RESULT | **R43_EVENT_EFFECT_REAL_BUT_COST_DOMINATED** |
| TECHNICAL_STRUCTURE_RESULT | **R43_NAMED_LEVELS_INDISTINGUISHABLE_FROM_PLACEBO** |
| EQUITY_RESIDUAL_RESULT | no candidate survived Zone B |
| OPTIONS_DATA_RESULT | **R43_OPTIONS_HISTORY_WINDOW_BINDING** |
| SEARCH_ADJUSTED_RESULT | no BH survivor within family at q = 0.10 |
| HISTORICAL_ALPHA_RESULT | **FAIL** |
| TRUE_FORWARD_RESULT | **NOT_YET_TESTABLE** — 0 rows, 0 shadows frozen |

**Terminal state: `R43_NO_QUALIFIED_ALPHA_AFTER_GLOBAL_OFFENSIVE`** — valid
only because every material $0 lane either produced evidence or reached a
specific, live-probed blocker. All thirteen declared lanes terminated.

## Search burden

**289 inherited (230 pre-R41 + 59 R41, re-derived from the ledger's bytes)
+ 13 new Release-43 Zone-B candidates = 302.** Never reset, never laundered
through a new campaign id, and the R41 ledger was opened read-only. Eight of
the thirteen declared lanes spent **zero** burden because their Zone-A
screens never reached the frozen advance bar — which is what a screening
zone is for.

## Four bugs this release found in its own work

Naming them is what makes the surviving numbers credible.

1. **A look-ahead in the equity decile selection.** The cross-section was
   ranked on *today's* signal while the position held *yesterday's* sign, so
   the book systematically bought whatever had just fallen. It produced
   −304 %/yr at **t −50** — an impossible number, which is how it was
   caught. Fixed; the regression pins it.
2. **A full-sample volatility target** in the RV risk scaling, which let
   each structure's portfolio weight depend on volatility it had not yet
   realised. Replaced with an expanding median.
3. **A de-meaned carry z-score**, which deleted exactly the level
   information carry consists of. Corrected on Zone A — the contract's free
   screening zone — before any burden was spent and before Zone B was
   touched.
4. **A non-reproducible placebo.** The placebo generator was seeded from
   Python's built-in `hash()`, which is randomised per process, so the
   headline candidate's kill verdict **flipped between two runs of identical
   code on identical data** — surviving once, killed by `PLACEBO_FEATURE`
   the next. The seed now comes from a SHA-256 digest and the draw count
   went from 40 to 200, because at 40 draws the 95th percentile was itself
   noise and this candidate sits close enough to the placebo distribution
   that the estimate decided the verdict. The stable answer is that it
   survives comfortably (p95 1.89 vs t 2.69, empirical p 0.005). A
   reproducibility defect in a kill test is worse than a failing kill test.

## Ownership map

| concern | owner |
|---|---|
| frozen contract / collateral / caps / kill battery | `alpha_agent/r43/contract.py` |
| inheritance verification + freeze | `alpha_agent/r43/closeout.py` |
| never-reset burden ledger | `alpha_agent/r43/burden.py` |
| **the universal economic judge** | `alpha_agent/r43/judge.py` |
| read-only owned panels | `alpha_agent/r43/panels.py` |
| Track A capital-adjusted carry | `alpha_agent/r43/carry.py` |
| Tracks E/F curve relative value | `alpha_agent/r43/rv.py` |
| Tracks H/I/J events, relations, structure | `alpha_agent/r43/crossasset.py` |
| Track L market-neutral equity | `alpha_agent/r43/equity.py` |
| Tracks B/C/D/G/M/N/T/U acquisition + purchase gate | `alpha_agent/r43/acquisition.py` |
| Track Q alpha killer | `alpha_agent/r43/killer.py` |
| Tracks P/R/S frontier, freeze, readiness | `alpha_agent/r43/frontier.py` |
| orchestration + twenty answers | `alpha_agent/r43/campaign.py` |

Reused, never rebuilt: `r41.evidence` (zones, HAC, scorecards, factor
residualisation), `r41.curve_state` (dated-contract curves), `r42.capital`
(the equivalence target), `r31.multiple_testing` (BH), `r39.burden`
(deflated Sharpe), `r31` hashing/immutability. `alpha_agent/r43/{evidence,
multiple_testing, economics, zones, ledger, research_shadow, scheduler,
forward_freeze, curve_state, crypto_lab, deflated_sharpe}.py` are forbidden
by the audit.

## Safety

MONEY_SPENT **$0.00** · accounts created **0** · trials started **0** ·
licences accepted **0** · payment details submitted **0** · subscriptions
**0** · cloud compute **$0.00** · orders **0** · paper orders **0** · broker
connections **0** · operational writes **0** · portfolio mutations **0** ·
model promotions **0** · sleeve activations **0** · scheduler changes **0** ·
production restarts **0** · prior-release artifacts mutated **0**.

**Shell policy: Windows PowerShell only, with zero Bash/WSL/sh invocations
this release.** R42's single disclosed read-only Bash event is preserved in
the contract, not erased.

## What Release 44 inherits

**Make the control the product, not the signal.**

Three separate times — R42's crypto book, every Track-A carry family, and
the Track-E rates candidate — a real, persistent premium was found and the
timing rule on top of it added an increment indistinguishable from zero. The
estate has now spent **302** effective trials looking for timing alpha over
owned information and has not found it. What it has repeatedly *measured* is
harvestable structural premia whose only open questions are what they cost
to hold and how much capital they immobilise — and that instrument now
exists and is proven equivalent to both prior conventions.

Release 44 should stop searching for a signal and start pricing an
**allocation**: given the premia we can actually measure, each with its
correct committed capital and control, what is the best risk-adjusted
portfolio of passive structural exposures — and does it beat cash after
everything?

The one data action worth its price: **a single month of Polygon Options
Starter (~$29)** to verify the vendor's per-tier history claim. It is the
only route under $100 that converts a blocked information family into a
testable one, and Release 43 has already built and proven the whole pipeline
at $0 — universe enumeration, survivorship-safe expired contracts, price
acquisition and local IV inversion. `RECOMMEND: NEED_SAMPLE`, not
`RECOMMEND_BUY`.
