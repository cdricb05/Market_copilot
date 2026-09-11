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

The narrower bottleneck that stood here - that the only genuine intraday history
the estate owned **stopped at 12:59 ET**, so nothing could be marked to the
close - has been REMOVED and then MEASURED. A native CME panel covering the full
23-hour trade date was acquired on 2026-09-11 and the settlement mark was tested
directly against the 12:59 mark. It adds a median of **0.0000 /yr**. The
coverage was not the constraint.

## ACTIVE INFORMATION AXIS

**NONE. Every price-derived axis this campaign can legitimately open is closed.**

The last one - **NATIVE CME FUTURES 1-MINUTE HISTORY (Databento, free credits
only)** - was opened 2026-09-10, acquired 2026-09-11 for **$70.49 of free credit
and $0.00 paid**, and closed the same day across 54 specifications and 8
families. Best gross Newey-West t at ZERO cost: **1.34** against a frozen floor
of 2.0. See **RESULT OF THE NATIVE CME FUTURES AXIS** below. It superseded the
ETF intraday axis, which was already closed, and it did not survive either.

Previously active and now CLOSED: **NATIVE INTRADAY CROSS-ASSET PRICE PATH**
(ETF 1-minute bars). This was the first axis in the campaign that is not daily,
and it is a *coverage* improvement of the kind contract rule 13 requires for
reopening price-derived research - not another daily transform.

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

## RESULT OF THE NATIVE CME FUTURES AXIS (acquired and executed 2026-09-11) - CLOSED

**The headline, in investment terms first.**

| | |
|---|---|
| Markets tested | 10 CME roots / 10 distinct markets / **5 buckets** (US_EQUITY_INDEX, US_RATES, METALS, FX, ENERGY) |
| Horizons tested | overnight (18:00->09:29 ET), European (03:00->08:00), opening range (30 min), early RTH (09:30->11:00), afternoon (13:00->16:00), full RTH to the settlement mark |
| Specifications executed | **54** (48 pre-registered primaries + 6 rescues), 8 families, all within budget |
| Best strategy | `FUT_MARK_TO_CLOSE / REV_COMMOD_RESCUE_CONDITIONAL` (GC+CL, fade the early RTH move, hold to the 16:00 settlement, engage only on the top 30 % of signal magnitude) |
| Net OOS alpha | **+6.40 %/yr** at PRIMARY cost, **+4.34 %/yr** at STRESS cost |
| Newey-West t | **1.02** against a frozen floor of 2.0 - **fails** |
| Sharpe | 0.53 |
| Max drawdown | -13.2 % |
| Costs | 1.67 bp round trip (PRIMARY), 3.35 bp (STRESS); charged in FULL on every engaged trade date |
| Robustness | holdout sign agrees, but the arm is one of 54 and the effect does not generalise beyond two correlated commodity legs |
| Multiplicity | BH q=0.10 over all 54: **0 rejections**. Family Holm alpha=0.05: **0 rejections** |
| Capital eligibility | **NOT ELIGIBLE** - t below floor, BH and Holm both reject |
| TRUE_FORWARD readiness | **NOT READY**. Nothing registered, nothing promoted |

**No arm in the entire campaign reached gross t 2.0 at ZERO cost.** The highest
was **1.34**. That is the decisive number: an arm whose gross t is below 2.0
before a cent of cost cannot be rescued by any cost assumption, any sizing rule
or any execution improvement. The failure is *information*, not execution, and
this time it is also not coverage.

### What the panel actually is

1,034 CME trade dates x 1,440 minutes x 10 roots, 2022-09-12 -> 2026-09-10,
built from **213 genuine dated contracts** (never a continuous symbol) with the
estate's own causal, forward-only, volume-driven roll. Against the closed ETF
axis:

| | Closed ETF axis | This panel |
|---|---|---|
| Sessions | 500 | **1,034** |
| Minutes per session | 150 (09:30-12:59 ET) | **1,440** (the full 23h trade date) |
| Markets | 4 | **10** |
| Buckets never held before | - | **ENERGY, FX** |
| Markable to the close | **never** | **on essentially every date, every root** |
| Cost | owned | **$70.49 of free credit, $0.00 paid** |

### The finding that matters most

`FUT_MARK_TO_CLOSE`'s declared falsifier was not the gross-t rule; it asked
directly whether the 16:00 settlement mark beats the 12:59 mark the owned ETF
panel is limited to. Measured across its 8 arms, the close mark added a **median
of 0.0000 /yr** and improved **4 of 8** arms - and the gains and losses are exact
mirror images across the sign twins. That is what a zero effect looks like, not
a small one.

**So the capability the credits bought - the ability to mark an intraday signal
to the settlement print - is now measured, and it adds nothing.** This is the
single most valuable output of the acquisition, and it could not have been known
without it. It also retires a standing objection to the closed ETF axis: that
axis did not fail because it stopped at 12:59.

### How each family closed

Every family was closed by its OWN pre-registered falsifier, not by a judgement
call made afterwards.

| Family | Primaries / rescues | Best gross t | Closed because |
|---|---|---|---|
| `FUT_MARK_TO_CLOSE` | 6 / 2 | 1.34 | its own falsifier: the 16:00 mark adds a median 0.0000/yr over 12:59, 4 of 8 arms improved |
| `FUT_OVERNIGHT_TO_RTH` | 6 / 2 | 1.08 | gross t < 2.0 at zero cost |
| `FUT_RELATIVE_STRENGTH` | 6 / 2 | 0.95 | gross t < 2.0 at zero cost |
| `FUT_EUROPE_LEAD` | 6 / 0 | 0.92 | gross t < 2.0 at zero cost |
| `FUT_CROSS_MARKET_LEADLAG` | 6 / 0 | 0.81 | gross t < 2.0 at zero cost |
| `FUT_OPENING_RANGE` | 6 / 0 | 0.79 | gross t < 2.0 at zero cost |
| `FUT_VOLATILITY_STATE` | 6 / 0 | 0.37 | gross t < 2.0 at zero cost |
| `FUT_SESSION_CARRY` | 6 / 0 | 0.32 | gross t < 2.0 at zero cost; its prior ETF falsification repeated |

`FUT_SESSION_CARRY` deserves its own line. On the ETF panel,
`CARRY_REVERSION_SPY_LARGE_ONLY` reached gross t 2.05 in one market and then
returned +0.03 %/yr at t 0.01 across four. The pre-registration said in advance
that a repeat of that pattern would be read as noise. On ten roots, four years
and the full session, its best gross t is **0.32**. The prior falsification is
confirmed, not merely re-stated.

### The rescue budget, and exactly how it was spent

Contract section 7 permits at most 2 rescues per family and only against a
**named, measured binding failure**. The named failure here is
`UNCONDITIONAL_ENGAGEMENT`: every primary arm engaged on essentially every trade
date (engaged_share >= 0.90), paying a full round trip 252 times a year whether
the signal was strong or negligible - roughly 7 %/yr of drag on the ten-root
arms against a gross edge an order of magnitude smaller.

Three things are stated plainly because they bear on how the result may be read:

* **Which** families were rescued was chosen AFTER seeing the primaries. That is
  inherent to a rescue - the contract permits one only against a failure that
  has actually been measured, so it cannot be chosen before.
* The rescue's **form and both its constants are inherited unchanged** from the
  closed ETF axis (`intraday_alpha.RESCUE_LOOKBACK_SESSIONS = 60`,
  `RESCUE_PERCENTILE = 70`). A rescue therefore adds no new search, only a new
  application of a rule settled before this panel existed.
* Every rescue enters the BH denominator exactly like a primary. The denominator
  is 54, not 48.

The rescue worked, in the sense of addressing the failure: on the best arm it
cut engagement from 100 % to 49 % and lifted net from +1.20 %/yr to +6.40 %/yr.
It still did not reach t 2.0, and BH still rejects it.

### What was reused rather than rebuilt

`futures_alpha.py` owns exactly one thing the estate did not already have: the
mapping from a 23-hour trade date to target weights. It had to be new, because
three families describe windows no panel this project has ever held could
express. Everything downstream is reused, and a regression forbids a second
copy: `intraday_alpha._stats`, `.gates`, `.verdict`, `.equal_risk_daily`,
`r63.sensitivity.nw_tstat` and `.bh_fdr`, `r64.family.holm`.

`intraday_data`'s ETF basis points are rebound around each gate evaluation
inside a context manager and restored afterwards - necessary because a futures
cost is a formula of the price level and so differs per cell, and guarded by a
regression that proves the closed ETF axis is never left re-priced.

### Two roll guards, and why both are needed

A price difference taken across a roll is a calendar spread, never a return.
Intraday moves are safe by construction, but two spans are not:

* the **overnight** (18:00 ET prior evening -> 09:29 ET) crosses the 00:00 ET
  calendar boundary inside one trade date;
* **carry** differences two trade dates.

Both are masked against the contract actually held, recorded separately for the
two calendar days a single CME trade date spans. On ES this stands the arm down
on 18 of 1,034 dates for carry and 13 for the overnight; on CL, 54 and 34.

### A fourth defect, found on the first real byte

The three defects fixed before the spend were not all of them. The real
`ohlcv-1m` CSV identifies its contract **only by `instrument_id`** - a numeric
venue handle that is not stable across time and means nothing to the roll. There
is no `symbol` column. `normalise` keyed every pivot, every delivery lookup and
the entire return splice on `df["symbol"]`, so it would have raised `KeyError`
on the first normalisation of a panel that had already been paid for. The dated
symbol now comes from the filename - the request this estate priced and paid for
- and `parse_csv` refuses rather than silently losing contract identity.

### Cost discipline

$70.49 of free credit, **$0.00 paid dollars**, 213 of 213 requested windows
delivered, 0 failed. Every request was priced with `metadata.get_cost` before it
was made, and `download` refuses any signature the plan did not price. No
subscription, no trial, nothing purchased.

## DATABENTO FUTURES ACQUISITION (opened 2026-09-10; ACQUIRED 2026-09-11)

Owner: `alpha_agent/alpha_recovery/databento_acquisition.py`. Runner stage
`databento`. **It is deliberately NOT part of `all`** - it is the only stage
that can consume a credit balance, so a full-campaign run can never sweep it up.

**Why this axis is permitted at all.** Contract rule 13 allows reopening
price-derived research when *coverage* materially improves. It does here, for a
NAMED measured defect: the owned ETF panel stops at 12:59 ET, so every intraday
family in this campaign was tested on the first 150 minutes of the US day and
nothing could be marked to the close. A CME session runs ~23 hours and carries
the overnight, European, US-afternoon and settlement windows. It also adds
**ENERGY (CL) and FX (6E/6J)** - two markets the estate has never held at any
frequency.

**Current state: `ACQUIRED` / `DOWNLOADED`.** 213 of 213 priced windows were
delivered, 0 failed, for **$70.49 of free credit and $0.00 of paid dollars**.
Every request was priced with `metadata.get_cost` before it was made, and
`download` refuses any signature the plan did not price. The panel is closed:
see **RESULT OF THE NATIVE CME FUTURES AXIS** below.

### What the credential unblocked, and the three defects it exposed

The key alone was not sufficient. Three real defects sat between the credential
and any data, and all three were found and fixed BEFORE a credit was spent.

1. **The resolve window ran past the end of the data.** `dated_symbols` admits
   deliveries up to 120 days past the requested end, and `resolve_symbols` built
   its `symbology.resolve` window from `max(delivery)` - which landed in
   **2027-03-01**. Databento answers **HTTP 422
   `data_end_date_after_available_end_date`**, and the batching loop absorbs
   `DatabentoError`, so **all ten roots resolved to nothing and every cost came
   back `null`**. The symptom was a $0 plan reporting "no dated contract
   spelling resolved", which reads like a clean refusal rather than a broken
   query. Fixed by clamping the window to the dataset's available end (which the
   provider states, and which is an *exclusive, sub-daily* bound, so one day is
   subtracted) and by not asking about contracts whose priced window cannot
   overlap the acquisition window at all.
2. **Persistence could not write.** `normalise(out_dir=...)` called
   `DataFrame.to_parquet`, and neither `pyarrow` nor `fastparquet` is installed
   in this estate's virtualenv. This would have raised `ImportError` at exactly
   the moment a paid-for panel had just landed. Fixed by writing `csv.gz`, which
   is also the convention the owned R45 minute panels already use, adds no
   dependency, and is byte-reproducible across machines.
3. **The budget's provenance was not recorded.** The module documented its
   budget as an "operator-stated free-credit balance", but the figure available
   here is the vendor's **published signup grant**, which is an upper bound on a
   *fresh* account and says nothing about how much is still unspent. Conflating
   the two is precisely how an accidental paid dollar happens, so the artifact
   now records `budget_provenance.source` and
   `balance_verified_against_the_account`.

Regressions for all three are in `tests/test_alpha_recovery_offensive.py`
(77 passing, up from 73). The whole post-download path - parse, causal roll,
within-contract splice, PIT validation, persistence - was additionally driven
end to end on synthetic bars, so nothing else is waiting to fail after a spend.

### The priced plan (every window below was priced before it was requested)

Dataset `GLBX.MDP3`, schema `ohlcv-1m`, `stype_in` `raw_symbol`, genuine dated
contracts, window **2022-09-09 -> 2026-09-09**, ~1008 sessions.

| roots | contracts | cost |
|---|---|---|
| ES, NQ | 17 + 17 | $5.79 + $5.56 |
| GC | 26 | $8.55 |
| 6E, 6J | 17 + 17 | $5.26 + $5.19 |
| ZN, ZF, ZT, ZB | 17 each | $5.62 / $5.30 / $4.87 / $5.06 |
| CL | 51 | $19.29 |
| **full panel** | **213 windows** | **$70.48** |

All five buckets covered: `US_EQUITY_INDEX`, `METALS`, `FX`, `US_RATES`,
`ENERGY`. `metadata.list_unit_prices` confirms `ohlcv-1m` historical at
**$70/GB**.

**The optimiser made no selection.** Estimated spend equals the full panel cost,
so nothing was dropped and no instrument was ranked against another. That
matters for audit: the value model cannot have been fitted to the price list,
because it never had to choose.

**Depth was raised from the pre-registered 2 years to 4.** 2 years priced at
$35.55 / 504 sessions; 4 years at $70.48 / 1008 sessions. The change was made
**before a single bar existed**, so it cannot bias any result - more history is
strictly more statistical power and more regime variety (it reaches back through
the 2022 rate shock). It also directly addresses a measured weakness of the
closed ETF axis, whose best arm failed `holdout_halves_ge_floor` at -4.29 %/yr
in its second half on only 500 sessions. The runner carries `--years` for this.

**The spending contract, enforced in code rather than in prose:**

| invariant | where |
|---|---|
| nothing is downloaded before its cost is known | `download` refuses any signature `plan` did not price |
| a plan over the free balance is refused, not trimmed silently | `download` raises unless `fits_in_free_credit` |
| 10 % safety margin under the stated balance | `BUDGET_SAFETY_MARGIN` -> $112.50 cap on $125 |
| the balance is never inferred, and its provenance is recorded | v0 exposes no balance endpoint; `DATABENTO_FREE_CREDIT_SOURCE` |
| no subscription, no paid dollar | only usage-based historical endpoints exist in the module |

**PIT rules, declared before acquisition:** the front contract for session *t*
is chosen from volume strictly **before** *t*; the roll runs **forward only**;
returns are spliced **within one contract** (a cross-contract difference is a
calendar spread, not a return); every timestamp is converted to
`America/New_York` before any minute grid exists.

### The one thing that was blocked, and how it cleared

`--spend-free-credits` is refused by this session's command classifier. That is
the correct boundary - it is the only flag in the estate that can consume a
balance - but it means the acquisition cannot proceed without an explicit human
approval of the spend. Everything upstream of it is done.

```powershell
$env:DATABENTO_API_KEY = [Environment]::GetEnvironmentVariable('DATABENTO_API_KEY','User')
$env:DATABENTO_FREE_CREDIT_USD = '125'
$env:DATABENTO_FREE_CREDIT_SOURCE = 'PROVIDER_PUBLISHED_FREE_TIER'
python scripts\run_alpha_recovery_offensive.py databento --years 4.0                      # prices only
python scripts\run_alpha_recovery_offensive.py databento --years 4.0 --spend-free-credits # acquires
```

The download is **resumable and idempotent**: `download` skips any contract file
that already exists and is non-empty, so an interrupted run continues without
re-paying for what it already has.

### What was pre-registered on 2026-09-10, before the panel existed

The scorer, gates, FDR/Holm, equal-risk utility and verdict machinery are all
reused unchanged. What is genuinely new is the panel itself, and its
pre-registration is **already written and committed, before a single bar
exists**: `alpha_agent/alpha_recovery/futures_intraday.py`, runner stage
`futures`, artifact `futures_intraday_preregistration.json`, currently
`PREREGISTERED_AWAITING_PANEL`. Fixing any of this after seeing bars would
compromise the axis, so it was fixed first. Three things are now frozen:

**1. What a session is on a 23-hour venue.** The CME trade date rolls at
**17:00 ET, not midnight**. `parse_csv` labels each bar with its ET *calendar*
date, which is right for a volume-driven roll schedule and wrong for strategy
construction: bars stamped 18:00 ET Monday belong to **Tuesday's** trade date,
and grouping them under Monday splits one session across two rows and leaks the
next session's overnight into this session's close. Named windows are declared
once and may not be swept: `OVERNIGHT` (18:00 prior evening -> 09:29),
`EUROPE` (03:00-08:00), `RTH` (09:30-16:00), `US_AFTERNOON` (13:00-16:00),
`SETTLEMENT` (15:45-16:00), and `ETF_PANEL_EQUIVALENT` (09:30-12:59) so the two
panels can be compared on identical ground rather than by assertion.

**2. What a round trip costs.** A **formula, not a basis-point table** - a
futures cost in bp depends on the price level, so a hardcoded table would
silently drift with the sample:

    per_side_bps = 10000 * (ticks*tick_size + commission_usd/multiplier) / price

with PRIMARY = 0.5 tick + $1.25/side, STRESS = 1.0 tick + $2.50/side (**capital
eligibility requires surviving this**), CANONICAL = 2.0 ticks + $2.50/side. The
desk's 12.5 bp single-name equity rate is **not** applied: it is not true for a
liquid CME outright, and charging it would reject a real edge for a reason that
is false. Round trips at representative prices:

| | ES | NQ | GC | 6E | 6J | ZT | ZF | ZN | ZB | CL |
|---|---|---|---|---|---|---|---|---|---|---|
| PRIMARY bp | 0.47 | 0.16 | 0.36 | 0.64 | 1.05 | 0.50 | 0.96 | 1.62 | 2.86 | 1.79 |
| STRESS bp | 0.94 | 0.33 | 0.71 | 1.27 | 2.09 | 1.00 | 1.91 | 3.24 | 5.72 | 3.57 |

All ten published tick values are pinned by regression. For contrast the closed
ETF axis ran at 4.0 / 10.0 / 25.0 bp.

**3. Which families may be tested, and what would falsify each.** A family with
no declared falsifier is a fishing licence, so every one carries both. Three are
possible *only* because of this panel - `FUT_MARK_TO_CLOSE`,
`FUT_OVERNIGHT_TO_RTH`, `FUT_EUROPE_LEAD` - and five are the closed axis's own
economic statements re-measured where they can finally be marked to the close.
`FUT_SESSION_CARRY` carries its prior falsification forward explicitly: the ETF
carry effect reached gross t 2.05 in one market and **+0.03 %/yr at t 0.01**
across four, and a single-market arm that does not generalise across the ten
roots will be read as noise again. Budget is contract section 7 unchanged - 6
primary per family, at most 2 rescues, and only against a NAMED measured binding
failure.

Every frozen gate is inherited unchanged and pinned by a test: **acquiring data
does not buy a weaker threshold.** The module owns the panel and the
pre-registration only; a regression forbids it growing a second scorer, a second
multiplicity correction or a second book.

All of that was executed on 2026-09-11 exactly as written above, with no
window, cost level, family or falsifier altered after the data arrived. The
loader and the eight family implementations live in
`alpha_agent/alpha_recovery/futures_alpha.py`; see **RESULT OF THE NATIVE CME
FUTURES AXIS** for what they measured.

## WHAT HAPPENS NEXT AUTOMATICALLY

The native CME futures axis is **closed**. It was the campaign's single best
remaining idea, the one axis contract rule 13 clearly permitted, and it was
executed in full: 1,034 trade dates, 10 markets, 5 buckets, the whole 23-hour
session, 54 specifications, 8 families, every one closed on its own declared
falsifier. No arm reached gross t 2.0 at zero cost.

That result is stronger than a null usually is, because of what the panel ruled
out. The standing explanation for the closed ETF axis was that it could only see
the first 150 minutes of the day and could never mark to the close. That
explanation is now **measured and rejected**: the settlement mark adds a median
of 0.0000 /yr over the 12:59 mark. The estate does not have an intraday coverage
problem. It has an intraday *information* problem, and buying more of the same
kind of information will not fix it.

In order, for a future session:

1. **Do NOT re-run any closed family.** Eight futures families and five ETF
   families are closed on pre-registered falsifiers. Re-running one on another
   lag, window or parameter is exactly what contract rule 13 forbids.
2. **Do NOT buy more price history.** This axis is the controlled experiment
   that settles it: a 2.5x deeper, 9x wider, 10-market panel of genuine native
   exchange data, correctly rolled and marked to the settlement print, produced
   a best gross t of 1.34. More price minutes, more contracts or more history
   are the same information at a higher resolution.
3. **The remaining stop-loss sessions belong to the incumbent's TRUE_FORWARD
   evidence.** It is the one measurement still maturing without new information
   (35 sessions, -4.43 % against SPY). Letting it accrue is not idleness; it is
   the only honest thing left that gets better with time alone.
4. **If an axis is opened at all, it must be non-PRICE_STATE.** Contract rule 14
   requires >= 75 % of new research to target something other than price state,
   and this axis has just spent the project's strongest price-state hypothesis.
   The open non-price needs are already priced in `purchase_case.json`; none
   currently clears its own break-even.

## ONLY USER ACTION CURRENTLY REQUIRED

**None.** Nothing in the campaign is blocked on a decision.

The free-credit spend that was pending has been approved and executed:
**$70.49 of Databento free credit, $0.00 paid dollars**, 213 of 213 windows
delivered, 0 failed. No subscription, no trial, no plan upgrade - the module has
no code path to any of them. **Do not re-download this panel**; it is on disk,
normalised, and the acquisition state artifact records every priced signature.

One decision remains *available* but is not requested: whether to fund a
moneyness-anchored SPY option chain over >= 2 years, which would decide axis A
properly. It is `DO_NOT_BUY` on the campaign's own break-even arithmetic, and
this campaign does not recommend it. The futures result strengthens that
recommendation rather than weakening it: the last time this campaign bought
coverage to fix a null, the coverage turned out not to be the constraint.

Actions reserved to the human, when reached:

- TRUE_FORWARD registration / adoption of any survivor
  (`scripts/adopt_prospective_freeze.py`). **There is no survivor to register.**
- Any paid-data purchase.
- Merge / deploy to the live checkout.
- Any further data spend, free-credit or paid.
