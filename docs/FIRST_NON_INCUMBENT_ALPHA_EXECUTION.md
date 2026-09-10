# First Non-Incumbent Alpha - Execution Roadmap

**Status:** LIVE EXECUTION FILE. Update it, do not rewrite it.
**Governing contract:** `docs/ALPHA_RECOVERY_OPERATING_CONTRACT.md` (read first).
**Campaign:** Alpha Recovery Offensive, worktree
`D:\paper_trader_alpha_recovery_offensive`, branch `alpha-recovery-offensive`.
**Machine-readable state:** `research/alpha_recovery/alpha_recovery_scoreboard.json`.

This file exists so a future session can continue WITHOUT another giant prompt.
It answers, in order: what are we trying to do, what already exists, what is in
the way, what is being worked on now, what died, what is winning, what happens
next on its own, and what only the human can do.

---

## CURRENT OBJECTIVE

Get the **FIRST NON-INCUMBENT, CAPITAL-ELIGIBLE alpha signal** into governed
TRUE_FORWARD competition, from **any market and any horizon**.

The incumbent US-equity model `fundamental_momentum_50_50_v1` is one competing
sleeve, not the reference truth. It is `INCUMBENT_WEAK_OR_UNPROVEN`: +2.73 %/yr
net excess at t 0.99 historically, and **behind SPY by 4.43 % over its first 35
TRUE_FORWARD sessions**. Beating it is necessary, not sufficient - a challenger
must also be capital-eligible in its own right.

## WHAT IS ALREADY BUILT - DO NOT REIMPLEMENT

Treat as CLOSED / REUSE ONLY. Re-building any of these is a contract violation
unless a specific measured defect blocks alpha and is documented as
`BLOCKER / ALPHA IMPACT / MINIMUM FIX`.

| capability | owner | reuse it for |
|---|---|---|
| PIT walk-forward, purge/embargo, NW lag | `alpha_agent.r63.pit` | every OOS split |
| the ONE scorer | `alpha_agent.r63.sensitivity.run_cell` | cell scoring |
| FDR / Holm | `r63.sensitivity.bh_fdr`, `r64.family.holm` | multiplicity |
| risk-controlled book, paired increment | `alpha_agent.r64.construction` | portfolio construction |
| cell measurement + verdicts | `alpha_agent.r64.experiments` | futures/credit scopes |
| information frontier + governor | `r63.gaps`, `r59.information_needs`, `r59.governor` | what to research next |
| head-to-head + **cross-domain equal-risk utility** | `alpha_recovery.tournament.cross_domain` | capital applicability |
| incumbent baseline + its daily path | `alpha_recovery.incumbent`, `alpha_recovery.equity_challengers` | the benchmark |
| TRUE_FORWARD registry, prospective freeze | `api/forward_challenger_registry.py`, `scripts/adopt_prospective_freeze.py` | activation (HUMAN-GATED) |
| intraday prospective ledger + slot clock | `alpha_agent.r53.intraday_factory` | intraday forward emission |
| forecast products, purchase case | `alpha_recovery.forecast_products`, `.purchase_case` | economic-unit output |

Also closed: R32 opportunity/capital framework, R39 representation/Fibonacci,
R46 challenger path, R50 multi-asset accounting/risk/allocator, R51 promotion
governance, R59 memory/governor/queue, R62 forward evidence, R63/R64
information-directed research.

## CURRENT BOTTLENECK

`NO_APPROVED_OPERATIONAL_SIGNAL` for non-equity sleeves. The estate can measure,
govern, size and account for a non-equity sleeve; it has never had one that
passed the frozen gates. Everything below exists to produce that one signal.

Second, narrower bottleneck, now MEASURED (see next section): the only genuine
intraday history the estate owns **stops at 12:59 ET**. There is no afternoon
and no closing auction, so no intraday strategy can be marked to the close.

## ACTIVE INFORMATION AXIS

**NATIVE INTRADAY CROSS-ASSET PRICE PATH** (1-minute bars). This is the first
axis in the campaign that is not daily, and it is a *coverage* improvement of
the kind contract rule 13 requires for reopening price-derived research - not
another daily transform.

### The measured data state (established before any strategy was run)

Owned, on disk, no purchase, no new infrastructure:
`D:\Stock_Prediction_app_data\macro_event_alpha_r45\_data_intraday\*.csv.gz`

| panel | instruments | sessions | window | verdict |
|---|---|---|---|---|
| ETF 1-minute | SPY, QQQ, TLT, GLD, IEF, SHY, UUP | **500** (2024-08-26 → 2026-08-24) | 11:00–16:59 UTC = **07:00–12:59 ET** | **USABLE** |
| futures 1-minute | ES, NQ, 6E, 6J, GC, CL, ZB, ZN, ZF, ZT | **49** (2026-06-15 → 2026-08-25) | same | **DATA_INSUFFICIENT** |
| SPY option surface (daily) | 6,216 rows w/ iv, moneyness, T | — | 2025-06-27 → | usable, axis A |

Regular-session coverage measured inside 13:30–16:59 UTC (09:30–12:59 ET),
median bars per session out of 210, and the median 1-minute high-low range used
as the cost sanity check:

| sym | sessions | med bars | % sessions ≥200 bars | med 1-min range (bp) | med session $ volume |
|---|---|---|---|---|---|
| SPY | 500 | 210 | 99.8 | 4.04 | $16.1 bn |
| QQQ | 500 | 210 | 100.0 | 5.90 | $12.5 bn |
| TLT | 500 | 210 | 99.8 | 3.05 | $1.23 bn |
| GLD | 500 | 210 | 87.2 | 4.59 | $1.51 bn |
| IEF | 500 | 208 | 66.4 | 1.06 | $0.29 bn |
| SHY | 500 | 197 | 40.8 | 0.60 | $0.09 bn |
| UUP | 500 | 117 | 1.2 | 0.04 | $0.015 bn |

**Tradable legs: SPY, QQQ, TLT, GLD** - US equity beta, US tech beta, long
duration, gold. Four economically distinct markets. IEF / SHY / UUP print too
few minutes to be traded at this resolution and are used as **information only**.

**Live intraday feed state** (measured by R53.1, not re-probed): Norgate is
daily-only; Polygon answers 403 on the owned plan; the R38/R45 minute panels are
frozen history; **Yahoo chart bars DO serve the current session** (~94 s delay,
`engine.market_data.fetch_current_session_bars`, ~30 days of minute history).
So historical research runs on the frozen panels and forward emission, if ever
authorised, runs on the Yahoo lane through the R53 ledger.

### Pre-registered cost ladder (fixed BEFORE any result was seen)

| level | per side | round trip | role |
|---|---|---|---|
| PRIMARY | 2.0 bp | 4.0 bp | headline; ≈ half of one median 1-minute bar's range, ~12× SPY's quoted half-spread |
| STRESS | 5.0 bp | 10.0 bp | **capital-eligibility requires surviving this** |
| CANONICAL | 12.5 bp | 25.0 bp | the desk's single-name equity rate, always reported |

No threshold below the canonical frozen gates is introduced: materiality
≥ 1.5 %/yr, paired t ≥ 2.0, BH q = 0.10, family Holm α = 0.05, ≥ 36 effective
periods, halves floor −0.005, drawdown multiple 1.5.

## RESULT OF THE INTRADAY AXIS (executed 2026-09-10)

**No qualified signal. Axis closed.** 32 specifications (30 primary within the
6-per-family budget, 2 rescues), 500 sessions, four markets. Verdicts: 15
`NO_ADVANTAGE`, 17 `WORSE_THAN_INCUMBENT`, 0 survivors, Benjamini-Hochberg
rejects nothing over a denominator of 32.

**The diagnosis that matters — cost or information?** Measured at ZERO cost, so
execution cannot be blamed:

| | value |
|---|---|
| largest gross Newey-West t across the 30 PRIMARY arms | **1.72** |
| arms with positive gross | 16 of 32 (53 %) — what an information-free grid looks like |
| arms reaching t ≥ 2.0 gross | 1, and it is a rescue |
| arms reaching t ≥ 2.0 NET | **0** |

Not one primary specification clears the frozen t ≥ 2.0 *even with every basis
point of cost removed*. The primary grid was not killed by transaction cost;
there was no credible gross edge to kill.

**The rescue, and its falsification.** The named measured binding failure was
UNCONDITIONAL ENGAGEMENT: `CARRY_REVERSION_SPY` earns +9.67 %/yr gross at t 1.72
— 3.85 bp per engaged session — while engaging on 100 % of sessions and paying a
4.0 bp round trip. Both permitted rescues applied one magnitude condition (top
30 % of |carry| against a strictly prior 60-session distribution, fixed once, not
swept):

- `CARRY_REVERSION_SPY_LARGE_ONLY` — worked as predicted on the per-trade
  economics: **10.35 bp per engaged session against a 4 bp round trip**, gross t
  **2.05**, engaged 29 % of sessions, net **+4.64 %/yr at t 1.27**.
- `CARRY_REVERSION_MULTI_LARGE_ONLY` — the generalisation test, and it
  **falsified the effect**: the same condition across four markets returns gross
  **+0.03 %/yr at t 0.01**. A real, general effect should have shown a *higher* t
  from diversifying per-session noise. It showed none.

The best arm fails on: `t_ge_2` (1.27), `survives_stress_cost` (**collapses to
+0.26 %/yr at 5 bp/side, −10.71 %/yr at the canonical 12.5 bp**),
`holdout_halves_ge_floor` (second half −4.29 %/yr),
`positive_equal_risk_utility` (t 0.98), BH and Holm. Its correlation to the
incumbent is −0.056, which is the one genuinely attractive property it has —
and it is not enough.

## RESULT OF THE OTHER AXES (executed 2026-09-10)

| axis | state | evidence |
|---|---|---|
| A. options / implied volatility | **DATA_INSUFFICIENT** | The owned surface is a FIXED strike band (654–720, 6 expiries) bought for one R45 event study. SPY rallied 615 → 751, so the band drifted out of the money: only **25 of 264 dates** carry a near-dated expiry whose strikes bracket the money, against the floor of 36. No cell scored — building an "ATM" series from whichever strike was least far from the money would manufacture a result. |
| B. analyst expectations / revisions | **NOT OWNED** | `analyst_revision_normalized.csv` has **0 rows**; only a 3-row mock fixture and a 960-row / 40-ticker proxy. |
| C. institutional ownership / flow | **NOT OWNED** | `short_interest_normalized.csv` has **0 rows**; the FINRA raw store is a 93-byte probe; no 13F holdings on disk, only an EDGAR submissions cache. Still blocked by an unbuilt CUSIP→ticker bridge *and* by absent data. |
| macro event × real timestamps | **CLOSED BY R45** | R45's own data frontier records the effect "failed on its own holdout, in listed US rates and equities over two years, and in every other market the estate owns." Re-running it would repeat closed work. |

## ACTIVE EXPERIMENTS

Owners: `alpha_agent/alpha_recovery/intraday_data.py` (the ONE owner of the
intraday panel state) and `alpha_agent/alpha_recovery/intraday_alpha.py` (the
ONE owner of intraday challenger families). Runner stages `intraday` and
`intraday_alpha` in `scripts/run_alpha_recovery_offensive.py`.

Bounded families, ≤ 6 primary specifications each, ≤ 2 rescues and only against
a NAMED measured binding failure:

1. `INTRADAY_SESSION_CARRY` - prior-session carry and pre-market drift;
   reversion vs continuation.
2. `INTRADAY_OPENING_RANGE` - opening-range breakout vs failure.
3. `INTRADAY_VOLATILITY_STATE` - compression / expansion.
4. `INTRADAY_CROSS_MARKET_LEADLAG` - rates / gold → equity transmission.
5. `INTRADAY_RELATIVE_STRENGTH` - cross-sectional among the tradable legs.

Every arm is flat overnight by construction, which is why it can be additive to
the incumbent at equal risk rather than a substitute for it.

## CLOSED / FAILED PATHS

Do not re-open without new information, materially better PIT history, or
materially better coverage (contract rule 13).

- Daily PRICE_STATE transforms - exhausted across 8,380 hypotheses / 311 families.
- Earnings-event reaction + XBRL surprise (7 cells) - `NO_ADVANTAGE`.
- SPY daily direction, 1/5/21/63 + 2 rescues - `NO_DIRECTIONAL_SKILL`, Brier skill ≤ 0.
- Cross-asset **daily** trend, all cadences - degenerate under controls.
- EODHD news sentiment, 41 of 120 names - `NO_ADVANTAGE` / `WORSE_THAN_INCUMBENT`.
- `US_EQUITY|1|FREE_CASH_FLOW` - real conditional information (t 2.21) inside a
  book that loses 20.3 %/yr to cost and draws down 77.5 %. Vehicle not investable.
- `CREDIT_PROXY|21|INFLATION_EXPECTATIONS` - t 2.86 but +1.2 %/yr, below materiality.
- `VOLATILITY|21|IV` - DATA_HOLD on 196 rows vs floor 200; answered by cadence,
  NOT by moving the floor; negative conditional t, reproduction failed.
- `US_EQUITY|TOP25|blend|k126` (incumbent's own cadence) - +2.2 %/yr paired
  advantage at t 1.61, below the frozen t ≥ 2. `NO_ADVANTAGE`, not rescued.
- FX carry `k5|b0.25` - +2.9 %/yr but Holm-failed, POST_SELECTION, and adds NO
  equal-risk utility to the incumbent (−1.66 %/yr, t −0.95).
- **Futures 1-minute panels** - 49 sessions. Cannot reach 36 effective periods
  under any honest block scheme. Requirement stated below.

## BEST CURRENT CANDIDATE

**None is capital-eligible.** `research/alpha_recovery/alpha_recovery_scoreboard.json`:

- ranked best: `FX_FUTURES|XS|1|CARRY|k5|b0.25` — Holm-failed, POST_SELECTION,
  and adds NO equal-risk utility to the incumbent (−1.66 %/yr, t −0.95).
- best on the same capital: `US_EQUITY|TOP25|blend|k126` — +2.18 %/yr at t 1.61,
  below the frozen t ≥ 2. Not rescued.
- best non-incumbent: `INTRADAY|SESSION_CARRY|CARRY_REVERSION_SPY_LARGE_ONLY` —
  see above. `NO_ADVANTAGE`.

Nothing is registered, adopted, promoted or purchased. 52 non-incumbent
candidates measured, **0 qualified**.

## WHAT HAPPENS NEXT AUTOMATICALLY

The owned/free estate is now exhausted across every axis the brief named, so
there is no further autonomous research that does not require either new data or
a human decision. A future session should NOT re-run the closed families. In
order:

1. If the operator authorises a purchase, the *only* two experiments with a
   stated economic case are (a) a moneyness-anchored SPY option chain over ≥ 2
   years, which would decide axis A properly, and (b) ~500 sessions of 1-minute
   futures history, which would widen the intraday axis to ES/NQ/ZN/GC/CL/6E.
   Both are `DO_NOT_BUY` today on the campaign's own break-even arithmetic.
2. If no purchase: the campaign's terminal state is
   `OWNED_FREE_INFORMATION_EXHAUSTED` and the remaining stop-loss sessions are
   spent letting the incumbent's TRUE_FORWARD evidence accrue (35 sessions so
   far, −4.43 % against SPY), which is the one measurement that is still
   maturing without new information.
3. Do not open a new PRICE_STATE family. Rule 13 requires new orthogonal
   information, materially better PIT history or materially better coverage;
   the intraday axis was the last available coverage improvement and it is spent.

## ONLY USER ACTION CURRENTLY REQUIRED

**None is required for the campaign to remain valid.** Nothing is blocked on the
user; the honest terminal state is reachable without any purchase or approval.

One decision is *available* but not requested: whether to fund either of the two
data experiments in the section above. Both are `DO_NOT_BUY` on the current
evidence, and this campaign does not recommend either.

Actions reserved to the human, when reached:

- TRUE_FORWARD registration / adoption of any survivor (`scripts/adopt_prospective_freeze.py`).
- Any paid-data purchase.
- Merge / deploy to the live checkout.
- To make intraday futures research possible: **≈ 500 sessions of 1-minute
  history for ES / NQ / ZN / GC / CL / 6E**, which needs a Databento or CME
  DataMine account (both currently `ACCOUNT_REQUIRED`). Not requested; the ETF
  panel is sufficient for the current axis.
