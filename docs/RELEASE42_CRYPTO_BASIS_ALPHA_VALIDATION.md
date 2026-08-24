# Release 42 — Crypto Funding/Basis Alpha Validation & Execution-Reality Campaign

- **Date:** 2026-08-24
- **Branch:** `stage19-controlled-rebalance` (base commit `87096b8`, the
  Release-41 closeout — verified local == remote, 25/25 research artifacts
  and 30/30 released repo files re-hashed against the R41 handoff
  manifests, the frozen shadow's spec hash re-derived from the R41 owner's
  own code, forward ledgers intact at 0 rows, burden 289/230 read from the
  artifact — **before any work**)
- **Owner package:** `alpha_agent/r42/` (21 modules)
- **Regression:** `tests/test_release42_crypto_basis_alpha.py` (38 tests)
- **Audit guard:** `check_release42_crypto_basis_alpha` (29 blocking
  invariants)
- **Research root:** `D:\Stock_Prediction_app_data\crypto_basis_r42\`
- **Campaign:** `r42_crypto_basis_alpha_validation_v1`
- **Frozen contract:** `0b751c9aad66ebf8df54120701a90a26f6b7638ada3257b7939eb734463951e2`,
  written **2026-08-24T00:30:59Z — before the first R42 number existed**

Release 41 handed over the strongest research candidate in the estate's
history. Release 42 did not try to improve it. It tried to destroy it.

## The one-sentence answer

**The crypto perpetual funding premium is real, structural, and replicates
almost everywhere — and it does not currently pay for the capital it
immobilises.** Scored exactly as R41 scored it, the BTC book earns
+3.15 %/yr on Zone C. Scored with the complete economic equation and the
capital it actually ties up, the same book earns **−0.67 %/yr against cash
(t −2.41)**.

## What R41 got right, verified rather than assumed

| claim | R42 finding |
|---|---|
| the stream reproduces | **EXACT** — Zone-B t, Zone-C t, Sharpe and ×3-cost t all reproduce with `worst_abs_diff = 0.0` |
| funding is measured correctly | **EVENT-EXACT** — 7,212 realised funding events, every gap 8.0 h, exactly 3/day for 2,404 days, no schedule change, reconciles to the daily aggregate with **0.0** error |
| no look-ahead | **CLEAN, and conservative** — `held_t = f(funding ≤ t−2)`; the signal is lagged twice, one day more than necessary |
| the basis term was included | **YES** — R41 did *not* treat funding as total return. It included `(spot_ret − perp_ret)` and it reconciles exactly |
| BTC/ETH selection | PIT-defensible; R42 replaced it with a frozen 471-symbol metadata rule anyway |

**R41 was not sloppy. Its arithmetic is right.** What it omitted was not a
term it mis-measured; it was four terms it never wrote down.

## The four omitted terms — and which one mattered

`SPREAD_SLIPPAGE`, `FINANCING`, `BORROW`, `COLLATERAL_DRAG`. Every one is
signed negative, so R41's equation is an **upper bound**, not a return.

The decisive omission is **FINANCING**. R41 judged a `DELTA_NEUTRAL_BASIS`
stream against a **zero control** — the convention `r41.contract.CONTROLS`
reserves for `RV_SELF_FINANCED` books. A cash-and-carry is the *opposite*
of self-financing: buying spot immobilises 100 % of the notional in a
non-interest-bearing coin, and the perpetual leg immobilises margin in
non-interest-bearing stablecoin. Scoring that against zero silently credits
the strategy with the entire risk-free rate it forgoes.

| BTC, Zone C (2025-04-14 → 2026-07-31) | excess/yr | t |
|---|---|---|
| R41 as scored (zero control, one leg's notional, 5 bps) | **+3.15 %** | +6.91 |
| same positions, complete economics, conservative capital | **−1.16 %** | −3.29 |
| predeclared positive-only cash-and-carry, complete economics | **−0.67 %** | **−2.41** |

And the conclusion does not depend on the denominator being harsh:

| capital model | K | ROIC | risk-free | excess | t |
|---|---|---|---|---|---|
| TRADED_NOTIONAL (R41's implicit) | 1.00 | +2.83 % | 3.26 % | −0.43 % | −0.89 |
| FULLY_FUNDED_COMMITTED | 1.20 | +2.36 % | 3.26 % | −0.90 % | −2.25 |
| **CONSERVATIVE_COLLATERAL (primary)** | **1.35** | **+2.09 %** | **3.26 %** | **−1.16 %** | **−3.29** |
| GROSS_EXPOSURE | 2.00 | +1.41 % | 3.26 % | −1.85 % | −7.69 |

Even at K = 1.00 — a denominator no one can actually achieve, since spot
exposure cannot be obtained for free — Zone C is negative.

## Where the P&L actually comes from

Funding is **99.6 %** of gross on Zone B and **100.7 %** on Zone C. The
spot/perp basis contributes **under 1 %**, and a measurable part of even
that is a 60-second mark-timing artefact (R41 marked spot on the UTC daily
kline close and the perp on the last 1-minute bar). This candidate is not a
basis trade. It is a **pure carry** trade, which is precisely why its value
is decided by the cost of its capital and nothing else.

## The z-gate is worth less than nothing

R41's own shuffled-gate placebo retained t 4.45 of 10.18 and pointed at
this. Measured directly against the same book held unconditionally, under
**R41's own scoring convention**:

| Zone B | excess/yr | t |
|---|---|---|
| hold the carry every day, no signal at all | **+9.59 %** | **11.80** |
| the frozen R41 z-gate rule | +8.75 % | 10.18 |
| **increment of the gate** | **−0.83 %** | **−3.65** |

A book with no signal beats the candidate on both zones, and the gate's
increment is *significantly negative* on Zone B. Under complete economics
the increment is +0.03 %/yr (t 0.14) and +0.07 %/yr (t 0.22) — noise
against the estate's own t ≥ 2 bar. **`R42_STRUCTURAL_PREMIUM_CONFIRMED_NOT_TIMING_ALPHA`.**

## The reverse leg: blocked, and irrelevant

Historical spot borrow cannot be proven. The venue publishes a *current*
margin table (BTC 0.44 %/yr, USDT 5.28 %/yr at VIP0) and no vintage of it;
its authenticated `interestRateHistory` route needs an account and an API
key, both forbidden. Verdict: **`HISTORICALLY_NON_IMPLEMENTABLE`**.

It costs nothing to exclude. The negative-funding leg is 0 days of Zone B
and 23 of Zone C, contributing **−2.2 %** of net. Removing it *helps*.

Note what the same table says about the positive leg: at the venue's own
posted **5.28 %/yr USDT borrow rate**, a levered cash-and-carry cannot even
cover its financing from a 3.6 %/yr gross carry.

## Three independent replications, one answer

**69 new assets** (frozen 471-symbol metadata rule: USDT-quoted, spot +
perp + funding history, ≥ 3 years joint, ≥ $5 m median volume, no
stablecoins/leveraged tokens/redenominations, delisted symbols included;
71 eligible, ETH and BTC labelled prior evidence). The exact R41 rule,
nothing fit:

| window | scoring | same-sign | random effect | t |
|---|---|---|---|---|
| each asset's full history | R41 convention | **69/69** | +9.56 %/yr | 25.69 |
| each asset's full history | complete economics | **69/69** | +4.41 %/yr | 14.76 |
| **BTC's Zone-C dates** (62 assets) | R41 convention | 59/62 | +1.87 %/yr | 8.79 |
| **BTC's Zone-C dates** (62 assets) | **complete economics** | **15/62** | **−1.13 %/yr** | **−7.15** |

Leave-one-out is stable at [−1.18 %, −1.08 %]: no single asset drives it.
And the premium is *higher where capital cannot go* — the correlation
between log volume and excess is −0.21.

**6 eligible venues** (Binance, Deribit, BitMEX, Coinbase INTX,
Hyperliquid, Kraken Futures; Bybit blocked 403, OKX serves only a rolling
3-month funding window). Over the deep 3-year overlap 2023-06 → 2026-07,
**9/9 streams show positive gross carry** (median +7.19 %/yr) — and the
median excess over cash is **+0.68 %/yr**, with Binance BTC itself at
+0.75 %/yr, **t 1.92 — below the estate's own t ≥ 2 bar**.

**CME regulated futures** (owned Norgate: 113 dated BTC contracts from
2017-12, 76 dated ETH from 2021-02; spot marked from the Binance 1-minute
archive at the 15:00 CT settlement minute). The premium is unmistakably
there — mean annualised basis **+6.02 %/yr**, positive in 73 % of sessions
— and:

> a ~6.1 %/yr basis on a ~20-day contract is harvested through a round trip
> that costs ~3.1 %/yr to keep rolling, against a risk-free rate of
> ~4.0 %/yr.

Under the *most favourable defensible* regulated treatment (capital = spot
notional only, FCM cash margin earning interest), BTC's full 8.7-year
excess is **+0.34 %/yr (t 0.24)** and its recent window is **−0.90 %/yr
(t −1.53)**.

This is the most important replication in the release, and it cuts both
ways: it proves the premium is **not** an offshore-leverage artefact, and
it proves it has never reliably beaten cash anywhere.

## Venue implementability: 6 eligible, 0 investable

The venue whose data produced the entire R41 result answers **HTTP 451 —
"Service unavailable from a restricted location"** to its own trading API
from the operator's location. Bybit answers 403. Every other venue is
reachable for *data* and none has a demonstrated admissible account path.
Research may use a venue's public data; that is not permission to trade
there, and the two are never conflated in this release.

## The statistical architecture never had to arbitrate

The hierarchy — LEVEL 1 lineage / LEVEL 2 Westfall–Young max-stat bootstrap
(21-day stationary blocks, 5,000 resamples, mean pairwise correlation 0.62)
/ LEVEL 3 random-effects meta-analysis, under closed testing — was hashed
into the frozen contract **before the first R42 outcome existed**, and it
is deliberately *more* forgiving of a correlated family than R41's
family-level DSR.

It still fails, and not on multiple-testing grounds. **LEVEL 1 does not
reject with a positive sign**: the predeclared representative's Zone-C
effect is −0.67 %/yr, p 0.016 two-sided — significant in the *wrong
direction*. No correction can rescue a negative point estimate, and none
was applied to try. LEVEL 2 rejects 0 of 4 variants; LEVEL 3 is not
reached.

**R41's own result is reported unchanged**: DSR at family burden 0.003761,
DSR at global burden 289 → 9.5e-24, `HISTORICAL_ALPHA_RESULT = FAIL`.
Release 42 did not repair Release 41 and did not need to.

## What is *not* wrong with this trade

Naming the things that do **not** kill it is what makes the thing that does
credible:

- **Execution is not the binding term.** Even charged at exactly R41's
  10 bps round trip, Zone C fails once its capital is priced; at *zero*
  cost the carry still does not clear the risk-free rate.
- **Capacity is not binding.** At $10 m committed capital the book is
  0.5 % of BTC spot daily volume and 0.09 % of open interest; modelled
  impact is under 1 bp.
- **Liquidation is not binding.** Unleveraged at K = 1.35 the short perp
  survives a +34.3 % adverse move; no declared price or basis stress
  liquidates it.
- **The reverse-leg borrow blocker is not what killed it** — that leg was
  a rounding error.

What *is* there, and invisible in a 0.4 %/yr measured volatility: 100 % of
committed capital at a single counterparty, in assets paying nothing. A
5 % exchange haircut costs **1.9 years** of gross carry. A one-leg fill
slip on a single rebalance costs, in expectation, more than a month of it.
A five-day venue outage converts the hedged book into a directional one at
the worst possible moment.

## Result axes (never collapsed)

| axis | result |
|---|---|
| SYSTEM_RESULT | **PASS** — 38/38 R42 tests, 23/23 R41, 28/28 R40, strict audit exit 0 |
| ECONOMIC_RECONCILIATION_RESULT | **EXACT** — `worst_abs_diff = 0.0` |
| EXECUTION_RESULT | **NOT_THE_BINDING_TERM** |
| CAPITAL_EFFICIENCY_RESULT | **R42_CAPITAL_EFFICIENCY_KILLS_EDGE** |
| CROSS_ASSET_REPLICATION_RESULT | premium replicates gross (69/69); fails vs cash in the recent window (15/62) |
| CROSS_VENUE_REPLICATION_RESULT | replicates on every eligible venue (9/9 gross); **0 investable** |
| REGULATED_MARKET_REPLICATION_RESULT | **REGULATED_PREMIUM_PRESENT_BUT_DOES_NOT_BEAT_CASH** |
| SEARCH_ADJUSTED_RESULT | **SEARCH_ADJUSTED_FAILS_AT_LEVEL_1** |
| HISTORICAL_ALPHA_RESULT | **FAIL** (R41's, inherited unchanged) |
| TRUE_FORWARD_RESULT | **NOT_YET_TESTABLE** — 0 rows |

**Qualification states (plural, by contract):**
`R42_CAPITAL_EFFICIENCY_KILLS_EDGE`,
`R42_STRUCTURAL_PREMIUM_CONFIRMED_NOT_TIMING_ALPHA`,
`R42_BORROW_REALITY_KILLS_REVERSE_LEG`,
`R42_CROSS_ASSET_REPLICATION_FAILS`,
`R42_CROSS_VENUE_REPLICATION_FAILS`,
`R42_SINGLE_VENUE_PREMIUM_ONLY`, `R42_DATA_LIMIT_BINDING`.

**Belief standard: 4 of 13 met.** Cashflow reconciliation ✓, reverse leg
excluded ✓, an independent regulated analogue ✓, no liquidation
dependence ✓. Positive after full costs ✗, positive on conservative
capital ✗, BTC survives ✗, ETH positive ✗, broad replication ✗, positive
at severe stress ✗, hierarchical evidence ✗. Forward evidence: not yet
testable.

## Forward evidence, and an operational defect worth naming

Zero TRUE_FORWARD rows exist. The R41 shadow froze 2026-08-23T21:39:06Z;
its first eligible decision is 2026-08-24. Capture was delegated to the R41
owner and **R42 wrote no R41 forward row**.

But the feasibility audit found something the estate needed to know: the
R41 shadow reads the Binance **monthly** archive, whose funding data
currently lags 24 days, and the venue's REST API — which would close the
gap — answers HTTP 451. The archive publishes no daily funding file
(probed). **A daily shadow reading a monthly archive cannot produce a daily
row.** The rows will still be genuinely prospective, but the R41
cadence claim of ~365 marks/yr is not achievable through this data path
from this location.

**One new R42 shadow frozen** (cap 3):
`R42_POSITIVE_ONLY_CASH_AND_CARRY_BTC` — RESEARCH_SHADOW_ONLY,
`PROMOTION_ALLOWED = False`, its rule written verbatim in the frozen
contract before any R42 outcome existed, and frozen with an explicitly
**negative** historical qualification. It tests one thing prospectively:
does the carry recover above the cost of its capital.

Two candidates were **declined** with reasons: a broad cross-asset funding
portfolio (the eligibility rule was predeclared, the *portfolio
construction* was not) and a CME basis candidate (the expression was
predeclared, the entry rule and the fair capital treatment were chosen
during the track). Both are strong R43 candidates to predeclare.

## Track R — closed with a blocker, not a fabrication

R41's signed-order-flow signal (+22.1 %/yr gross at Zone B, 0/12 net
positive) remains `INFORMATION_REAL_COST_KILLED`. A maker model needs four
components; three cannot be sourced free (queue/fill probability, adverse
selection, latency — the free archive has best-bid/ask only, and Tardis'
free tier covers ~1.5 % of the sample). **`BLOCKED_EXECUTION_MICROSTRUCTURE_DATA`.
No fills were fabricated.** Moving on.

## Safety

MONEY_SPENT **$0.00** · exchange accounts created **0** · deposits **0** ·
withdrawals **0** · API trading keys **0** · crypto purchased **0** ·
orders **0** · paper orders **0** · operational writes **0** · portfolio
mutations **0** · model promotions **0** · scheduler changes **0** ·
licences accepted **0** · subscriptions **0** · production restarts **0**.
All heavy data on `D:`. The R41 shadow, its registry, its spec hash and its
ledgers are byte-identical to their R41 state.

**Shell policy:** PowerShell only, with **one declared exception** —
a single read-only `grep` through the Bash tool during initial R41
reconnaissance, before any R42 code existed. It read source files, wrote
nothing and touched no repository state. It is recorded in
`shell_policy_events.json` and the handoff validator **blocks the commit
on it** unless the operator explicitly waives it. See the handoff README.

## Ownership map

| concern | owner |
|---|---|
| frozen contract / capital / control / universes / hierarchy | `alpha_agent/r42/contract.py` |
| R41 closeout verification | `alpha_agent/r42/closeout_import.py` |
| $0 public acquisition + venue cadence audit | `alpha_agent/r42/acquisition.py` |
| exact R41 reconstruction + term inventory | `alpha_agent/r42/pnl_audit.py` |
| event-exact funding cashflow + PIT | `alpha_agent/r42/funding_ledger.py` |
| funding / basis / execution attribution | `alpha_agent/r42/basis.py` |
| leg separation + borrow evidence | `alpha_agent/r42/legs.py` |
| **the complete equation + capital + control** | `alpha_agent/r42/capital.py` |
| execution ladder + maker admissibility | `alpha_agent/r42/execution.py` |
| margin / liquidation / path risk | `alpha_agent/r42/margin.py` |
| venue matrix + cross-venue replication | `alpha_agent/r42/venues.py` |
| frozen asset universe + meta-analysis | `alpha_agent/r42/asset_universe.py` |
| regulated CME basis | `alpha_agent/r42/cme_basis.py` |
| hierarchical multiple testing | `alpha_agent/r42/hierarchy.py` |
| unconditional vs timing | `alpha_agent/r42/attribution.py` |
| capacity | `alpha_agent/r42/capacity.py` |
| collateral / counterparty | `alpha_agent/r42/collateral.py` |
| forward capture + R42 shadows | `alpha_agent/r42/forward.py` |
| bounded maker-execution check | `alpha_agent/r42/microstructure_check.py` |
| verdict + twenty answers | `alpha_agent/r42/campaign.py` |

Reused, never rebuilt: `r41.evidence` (scorecards, HAC, zones, deflated
Sharpe), `r31.multiple_testing`, `r31` hashing/immutability,
`r39.research_shadow._desk` ledger primitives, `r41.crypto_lab` as the
loader of record for the R41 stream, `r41.forward_freeze` as the sole
owner of R41 forward capture, the owned Norgate entitlement, and the FRED
daily panel R41 acquired. `alpha_agent/r42/{evidence, multiple_testing,
economics, zones, ledger, burden, research_shadow, scheduler,
purchase_gate, crypto_lab}.py` are forbidden by the audit.

## What Release 43 inherits

**PRICE THE CARRY, DO NOT SEARCH FOR IT.** R42's real product is not a
verdict on one candidate; it is
`alpha_agent.r42.capital.implementable_book` — an instrument that charges a
strategy for the capital it immobilises and scores it against the cash it
forgoes. Three independent replications agree the crypto carry is real and
currently below cash.

The concrete first step: re-score the **R36 FX carry survivor** (rank IC
0.155, t 7.97) and the **R38 futures curve carry** through that instrument
with their own capital models. Neither has ever been subjected to this
treatment. If either clears cash on conservative capital, that is the
estate's first genuinely investable carry.

Explicitly **not**: another search over crypto perpetual funding, and no
options purchase before the carry comparator exists. The R41 purchase
ranking (ORATS $599, Alpha Vantage $50 pilot, Databento credits,
Steele/Intrinio) is preserved unchanged as a queued R43 lane.
