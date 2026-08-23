# Release 41 — Multi-Horizon Alpha Breakthrough Campaign

- **Date:** 2026-08-23
- **Branch:** `stage19-controlled-rebalance` (base commit `5f27ba4`, the
  Release-40 closeout — verified local == remote, artifact and repo
  manifests re-hashed, before any work)
- **Owner package:** `alpha_agent/r41/` (23 modules)
- **Regression:** `tests/test_release41_multi_horizon_alpha.py` (23 tests)
- **Audit guard:** `check_release41_multi_horizon_alpha` (20 blocking
  invariants)
- **Research root:** `D:\Stock_Prediction_app_data\multi_horizon_alpha_r41\`
- **Campaign:** `r41_multi_horizon_alpha_breakthrough_v1`

Release 41 attacked the four binding blockers TOGETHER: information
quality (two genuinely new information families acquired at $0), decision
cadence (a daily candidate frozen; minute-grid research on genuine minute
history), economic expression (curves, spreads, butterflies, basis and RV
books across five asset families), and search burden (global ledger
inherited at 230, family-level ledgers added, Zone-A screening before any
Zone-B spend).

## The correction

Paper Trader is NOT a monthly system. The three clocks (signal refresh /
portfolio reassessment / model recalibration) are restated in the frozen
contract and the horizon-contract artifact; **decision cadence is a
property of the candidate**. The R39/R40 shadows are monthly because those
candidates are monthly. The R41 shadow is daily. Intraday candidates await
native intraday data — and the free minute history this release acquired
already lets intraday hypotheses be tested scientifically.

## The result axes (never collapsed)

| axis | result |
|---|---|
| SYSTEM_RESULT | **PASS** — 23/23 R41 tests, 28/28 R40 tests, strict audit exit 0, attribution `ATTRIBUTED` |
| DATA_FRONTIER_RESULT | **FOUR_FREE_INTRADAY_LANES_OPENED** — Dukascopy tick/minute (FX 2003→, metals ≤2010→, index/energy/bond CFDs 2014/2018→), Binance archive (1m klines with SIGNED taker flow 2017→, funding 2019→, OI metrics 2021→), Tiingo IEX 1m (2017→, existing key), Tardis first-of-month L2 days |
| RESEARCH_CANDIDATE_RESULT | **PASS** — one candidate survived the declared gate (the single Zone-B miss, the 60-effective-decision floor at ESS ~50, reads 72 on Zone C and ~120 combined — recorded, not smoothed), a 15-test killer battery, and its single pre-gated Zone-C access |
| HISTORICAL_ALPHA_RESULT | **FAIL** — the frozen QUALIFIED_ALPHA_GATE fails on exactly one check (family deflated Sharpe), and the release documents WHY without loosening the gate |
| PROSPECTIVE_ALPHA_RESULT | **NOT_YET_TESTABLE** — the R41 shadow froze 2026-08-23T21:39:06Z; first eligible daily TRUE_FORWARD row 2026-08-24 |
| INFORMATION_RESULT | two NEW families with measurable edge signal: perp funding flows (the survivor) and signed order flow (real, taker-cost-killed) |
| CADENCE_RESULT | daily candidate frozen (~365 marks/yr — the fastest forward stream the estate has); intraday structure researched on real minute bars |
| EXPRESSION_RESULT | the surviving edge is a BASIS expression; the two strongest gross signals are SPREAD expressions; outright direction produced nothing |
| MODEL_RESULT | **SCALING_DEGRADES** — the scaled TCN (32/64/128 ch) reaches Zone-B t −0.03 vs the small TCN's 2.07 (exact re-score); the pooled-LGBM RV "edge" was killed by placebo insensitivity |
| PURCHASE_RESULT | **ORATS options archive recommended** ($599 one-time, 2007→, 5000+ underlyings) |

**Terminal: `R41_NO_QUALIFIED_ALPHA_YET` + `R41_TIME_LIMIT_BINDING`** — and
that verdict undersells what changed: the estate now holds the strongest
after-cost stream it has ever measured, frozen for daily forward evidence.

## Track 0 — R40 verified, not trusted

`r40_closeout_import.json` = `R40_VERIFIED`, 0 mismatches: 30/30 research
artifacts + 2/2 model-weight files and 31/31 released repo files hash to
the R40 handoff manifests; burden 230 (194+36) from the ledger; five
shadows immutable (registry hash reproduces), TRUE_FORWARD ledgers intact
at 0 rows; HEAD `5f27ba4` == origin. The R41 contract hash
(`00e0be34…8601f`) — gates, zones, placebo levels, cost model, killer
battery, blocker vocabulary — was frozen into this artifact before any
evaluation.

## The free data frontier (Track 3, $0, no accounts)

Measured by probe, acquired with provenance hashes:

- **Dukascopy public datafeed** — tick (bi5) and per-day 1-minute candle
  files, BID and ASK (real spreads). Measured starts: EURUSD 2003,
  XAUUSD ≤2010, USA500/WTI CFDs 2014, Bund CFD 2018. FX spot is native;
  CFDs are LEVEL-2 proxies, labelled.
- **Binance public archive** — 1-minute klines with signed taker volume
  (spot 2017-08→, perp 2019-09→), the full funding-rate history, daily
  OI/long-short metrics (2021→), full symbol listing for survivorship.
- **Tiingo IEX** — 1-minute SPY/QQQ bars 2017-01→ on the EXISTING key.
- **Tardis.dev free days** — exchange-native trades/derivative-ticker for
  the first day of each month, 2020→2026.
- **Cboe indices** (VIX 1990→, VIX9D/3M/6M/1D, VVIX, SKEW), **government
  curves** (ECB 2004→, MoF JGB 1974→, BoC, RBA), **FRED** market series.
- Measured walls: ICE BofA OAS on FRED now serves only ~3 years
  (LICENCE); Alpha Vantage HISTORICAL_OPTIONS is premium-only; Polygon
  free tier serves only a recent minute window; Kibot requires login;
  EODHD/Finnhub/FMP intraday are plan-walled; **Zacks via the existing
  NDL key now returns data (R37 got 403) but only a megacap sample tier
  with current-snapshot estimates — no vintages**.

## The labs (evidence zones A/B/C = 50/30/20, embargoed; Zone-A screening
is free; every Zone-B evaluation is a burden trial; Zone C opens once per
lineage at t ≥ 2.5)

**Rates RV (Track 4).** 21 duration-neutral structures (US/DE/AU curve
spreads and butterflies, 10 cross-country pairs, rolling 120-session
hedge betas), five signal families × five horizons plus pooled models.
Findings: spreads TREND at 1–2 sessions (gross t 3.3–3.4 — cost-killed);
carry is sign-stable but weak on Zone B (t ≤ 1.8); the pooled LGBM at 21s
scored Zone-B t 2.27 and was **killed by the battery** — one year-block
sign flip, and complete insensitivity to feature ablation and placebo
carry: a static tilt, not conditional prediction. The alpha-killer doing
exactly its job.

**Commodity curves (Track 5).** 33 calendar-spread + 33 butterfly
structures, six signals, XS books, an EIA event window, wave-2 cost-aware
composites. The curve information is REAL — carry gross t 2.1,
seasonality 3.6, fly-reversion 4.4 on Zone A — and every net expression
is cost-dominated: a diversified spread book's risk shrinks with √N while
notional costs do not. Wave-2 (composite sharing costs, 42–63s, energy-
only flys) passed Zone A and died on Zone B. `NO_QUALIFIED_CANDIDATE`,
with the gross-information finding recorded for the purchase engine.

**Volatility (Track 6).** VX calendar-spread books conditioned on the
newly acquired index term structure (BASIS/TERM/RVGAP/VVIX/SKEW/DTE),
gated short-vol vs the always-short control, pooled models. Nothing
clears the advance bar (best rule t 1.31); the conditional gate
UNDERPERFORMS the unconditional premium. The options-surface horizon
family stays data-blocked — hence the purchase recommendation.

**FX (Track 8).** XS carry/momentum/reversal at 1–21 sessions: Zone-A t
3.4–3.5 at EVERY horizon, Zone-B ≈ 0 at EVERY horizon. The premium is
**era-limited, not cadence-limited** — a clean multi-horizon answer.

**Credit (Track 8).** Duration-hedged HYG/LQD/IEF RV: nothing advances
(best A t 1.64 → B 0.16). Deep history is licence-walled.

**Crypto (Track 8) — the discovery.** The delta-neutral BTC funding-carry
basis book (z-gate ±0.5 on 30/90 funding, short-perp/long-spot):
**Zone-B t 10.2 (+8.7 %/yr on 0.7 % vol)**, survives cost ×3 (t 8.5),
+1-day latency (t 10.1), all three year blocks (t 6.4–7.7), threshold
perturbation; the placebo (shuffled funding gate) retains t 4.45 —
the unconditional premium is the main driver and the timing overlay adds
on top. Its Zone-B t ≥ 2.5 earned the lineage's ONE Zone-C access:
**t 6.91, +3.15 %/yr, Sharpe 7.8, max drawdown −0.5 %, survives ×3 costs
(t 3.0), ESS 72 ≥ 60**. And the strongest cheap falsification attempt
available — an OUT-OF-ASSET REPLICATION on ETH with the BTC-frozen
parameters and nothing fit — **confirms the mechanism: Zone-B t 9.5
(+8.9 %/yr), Zone-C t 4.5 (+2.3 %/yr), ×3 costs t 6.0**, with the same
compression profile. Caveats recorded: single-venue counterparty and
execution risk beyond taker fees; premium compression 8.7→3.2 %/yr;
BTC/ETH selection PIT-defensible but noted. Freezing the ETH replication
into the second shadow slot is left as an explicit Release-42 decision.

**Why HISTORICAL_ALPHA is still FAIL:** the frozen gate's family-deflated-
Sharpe check computes trial variance over the family's Zone-B population,
and three of the nine trials are cadence variants of the same lineage —
their shared true effect inflates the null's expected-max bar, and the
check fails (DSR 0.009). The gate is NOT loosened after the fact; the
nulls-only diagnostic (DSR ≈ 1.0) is reported beside it, labelled
DIAGNOSTIC-NEVER-THE-GATE. The forward stream arbitrates.

**Microstructure (Track 9).** First SIGNED-FLOW family: order-flow
imbalance from taker volume at 5m/15m/60m. Gross information real
(BTC +21 %/yr gross at 5m holds, direction out-of-sample) — and NET
deeply negative at taker fees at every hold and threshold.
`INFORMATION_REAL_COST_KILLED`: an execution-model problem, not an
information problem.

**Intraday structure + Fibonacci (Tracks 6/9).** Causal pivots (confirmed
only after ≥1 ATR displacement, stamped at the confirmation bar), named
retracement bands vs the contract's placebo bands, observed-spread costs,
day-clustered inference, volatility-regime splits, on EURUSD / XAUUSD /
USA500 minute history. Result (`intraday_lab_results.json`, full-coverage
run: 2,301 / 1,798 / 1,880 joint days): the named-minus-placebo
day-clustered t — the only Fibonacci claim admitted — is **negative in all
9 symbol × hold cells** (−1.21 to −5.39; −0.1 to −1.6 bps/day). Placebo
levels *beat* named Fibonacci levels wherever the difference is
significant, and the level-touch bounce itself is spread-dominated (XAU /
USA500 net-of-spread t ≈ −41 / −102). Named Fibonacci levels carry no
information beyond generic-level mean reversion: **FIB_REJECTED**.

**Model scale (Track 11).** The R40 "scale axis" hypothesis was executed
locally: TCN at 2–8× parameters (32/64/128 channels, +1 dilation, 60
epochs) through the exact R40 director protocol. The baseline re-scored
EXACTLY (t 2.072); the best scaled config reached **Zone-B t −0.03**.
Scaling DEGRADES at this data size, and the ~$10 GPU escalation request
is now WEAKENED by measurement, not just unpriced.

## Search burden (Track 12)

GLOBAL inherited 230 → **~248–250** (distinct Zone-B candidates across
all families; the ledger is the artifact of record) with FAMILY-level
counts and full lineage records (information family, asset, horizon,
expression, representation, model, hyperparameter budget, parents,
touches). Zone-A screening kept Zone-B spending to a fraction of the
candidates generated. Never reset.

## Forward freeze (Track 17)

`r41_shadow_registry.json`: **shadow_btc_funding_carry_1d** frozen
2026-08-23T21:39:06Z (cap 3, RESEARCH_SHADOW_ONLY, PROMOTION_ALLOWED
False, asset-disjoint from all five R40 shadows), chain-hashed ledgers on
the canonical desk primitives, first eligible decision the first full UTC
day after the freeze, contiguous capture with lateness recorded, capture
refuses any row at or before the freeze. `forward_freeze.capture()` is a
callable, not a schedule (AUTOMATION OFF).

## Purchase decision (Track 16)

`R41_DATA_PURCHASE_DECISION.json` ranks by expected information gain per
dollar: **1) ORATS near-EOD archive $599 one-time** (2007→, 5000+
underlyings, IV/greeks/OI — opens the largest still-closed family);
2) Alpha Vantage premium $50/1mo as the cheap options pilot; 3) Databento
$125 signup credits ($0 cash, operator account decision) for native
intraday futures; 4) FirstRate bundle; 5) ThetaData (dominated);
6) Zacks full (deprioritised — the estate has paid for this lesson
twice). No purchase is authorised by this release; the canonical
acquisition gate remains the authority.

## Safety

MONEY_SPENT $0.00 · accounts created 0 · trials 0 · licences accepted 0 ·
payment details 0 · operational writes 0 (`ATTRIBUTED`, R41 profile) ·
portfolio mutations 0 · promotions 0 · scheduler changes 0 · orders 0 ·
production restarts 0 · vendor emails sent 0 (the Steele request remains
operator-ready and unsent). All heavy data on D:. Existing entitlement
keys used read-only within their tiers. Shell policy: PowerShell only;
the handoff validator counts tool invocations from the transcript.

## Ownership map

| concern | owner |
|---|---|
| frozen contract / gates / cost model / placebo levels | `alpha_agent/r41/contract.py` |
| R40 closeout verification | `alpha_agent/r41/closeout_import.py` |
| multi-horizon inference (HAC/ESS/zones/scorecard/gates) | `alpha_agent/r41/evidence.py` |
| global + family burden ledgers | `alpha_agent/r41/burden.py` |
| dated-contract curve state (105 markets) | `alpha_agent/r41/curve_state.py` |
| free-sample acquisition + provenance | `alpha_agent/r41/sample_acquisition.py` |
| owned-data frequency inventory | `alpha_agent/r41/data_inventory.py` |
| 2026 provider frontier | `alpha_agent/r41/provider_frontier.py` |
| purchase decision engine | `alpha_agent/r41/purchase_engine.py` |
| horizon contract / decisions-per-year | `alpha_agent/r41/horizon_engine.py` |
| material-update triggers | `alpha_agent/r41/triggers.py` |
| near-real-time readiness | `alpha_agent/r41/readiness.py` |
| the five labs | `rates_rv_lab / commodity_curve_lab / vol_lab / crypto_lab / fx_credit_lab / intraday_lab` |
| sequence-model scale test | `alpha_agent/r41/model_scale.py` |
| alpha-killer battery | `alpha_agent/r41/alpha_killer.py` |
| R41 forward freeze + capture | `alpha_agent/r41/forward_freeze.py` |
| verdict + 22 answers + branch matrix + report | `alpha_agent/r41/campaign.py` |

Reused, never rebuilt: R31 hashing/immutability/multiple testing, R39
deflated Sharpe, the R38 roll policy and contract enumeration, the R40
director/model-challenge protocol, the desk chain-hash ledger primitives,
`r39.register_campaign_root` (one owner, three releases).
`alpha_agent/r41/{economics, multiple_testing, judge, zones, lockbox,
ledger, trade_space, universal_state, scheduler, purchase_gate}.py` are
forbidden by the audit.

## What Release 42 inherits

Two forward families now accrue evidence — five monthly R40 shadows
(first eligible 2026-08-31) and one daily R41 shadow (first eligible
2026-08-24; discriminates in months, not years). The binding constraint
is INFORMATION, and it is now priced to the dollar: $599 opens options
surfaces, $0-plus-an-account opens native intraday futures, one operator
email starts the Steele revision-vintage validation. The GPU request is
weakened by measurement. Another search over owned daily data is the one
move this release proves again to be worthless.
