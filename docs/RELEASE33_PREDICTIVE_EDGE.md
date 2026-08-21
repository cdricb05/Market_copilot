# Release 33 — Predictive Edge Acquisition

**Terminal verdict: `R33_NO_PREDICTIVE_EDGE`.
SYSTEM_RESULT = PASS. ALPHA_RESULT = FAIL.**

Research only, production read-only. No order, proposal, decision, allocation,
model promotion, sleeve activation or operational write. $0 spent.

---

## 1. Why this release existed

Release 32 tested 104 bounded hypotheses and qualified zero sleeves. That was a
legitimate negative research result and it was **not** the investment
improvement the system needs. Release 33 existed for one reason: find measurable
out-of-sample predictive edge.

The release succeeds as an investment research release only if at least one
candidate produces **both**

1. genuine out-of-sample predictive improvement over a pre-registered forecast
   baseline, **and**
2. positive after-cost economic improvement over an appropriate risk-matched
   investable control, on frozen evidence.

Otherwise `ALPHA_RESULT = FAIL`. No euphemisms: infrastructure, documentation,
architecture, a completed campaign and a clean audit are **SYSTEM** outcomes.

---

## 2. The commissioned lane did not exist

Release 33 was commissioned to run a broad **continuous-futures** campaign
across roughly 30–60 economically distinct markets. The instruction was to
inspect the actual database first and not assume coverage.

The owned Norgate **Continuous Futures** database contains **one market**:

```
Continuous Futures : 1 symbol  -> &ES  (E-mini S&P 500 Continuous Contract)
```

That is the headline data-inventory finding, and it is measured at run time by
`universe.vendor_database_summary()` rather than assumed.

### What the estate does hold

The broad cross-market universe was assembled from what is actually owned:

| asset class | markets | source |
|---|---|---|
| Equity indices | 26 | Norgate World Indices + `&ES` |
| Government bonds | 6 | ICE Treasury total-return indices (1–3y … 25y+) |
| Credit bonds | 2 | FTSE / S&P investment-grade corporate TR indices |
| Commodities | 6 | Bloomberg sub-indices + WTI spot |
| Precious metals | 3 | Bloomberg precious sub-index, gold, silver |
| FX | 23 | Norgate Forex Spot, expressed XXXUSD |
| **total** | **66** | **6 asset classes, 26 economic groups, 1995–2026** |

### The distinction that may not be blurred

Continuous futures are not automatically equivalent to tradable futures PnL, and
here the gap is wider still. Roll yield, contract selection, execution price and
futures transaction-cost semantics are unsupported by the owned data for every
market but one. Additional declared gaps:

- spot FX excludes the **interest differential**, a first-order component of an
  FX position's return;
- commodity sub-indices are index levels, not positions in a roll-managed
  futures stack;
- equity indices exclude dividends while bond indices include coupon.

The universe is therefore labelled **`SIGNAL_RESEARCH_VALID`** and the contract
sets `FUTURES_IMPLEMENTABILITY_CLAIMABLE = False`. No result in this release may
claim futures implementability.

---

## 3. Selection by measured rule

Every candidate market carries a recorded reason, including the ones kept.

| rule | test | effect |
|---|---|---|
| R1 | FX expressed XXXUSD, sourced from the better-resolution quote direction | `JPYUSD` prints 23.9 % identical closes, `USDJPY` 0.7 % — a rounding artifact of quoting 0.0068, not a property of the yen |
| R2 | zero-return fraction > 10 % ⇒ administered | excluded CNY, MYR |
| R3 | annualised volatility < 2 % ⇒ pegged — **FX only** | excluded HKD |
| R4 | correlation > 0.98 with an admitted market ⇒ duplicate | excluded DKK (named preference: EUR is primary, DKK is its band satellite), `$SPTSX` |
| R5 | ≥ 2000 sessions and still updating | — |
| R6 | composite of admitted markets, or a ratio | excluded `$STOXX50`, `$BCOM`, `$USDX`, `#CUGC` (a copper/gold **ratio**, not an investable market) |

Two rule defects were found by running the rules and reading the output, not by
inspection:

- **R3 applied to everything** excluded the 1–3 year Treasury index at 1.43 %
  volatility. A short-duration bond has low volatility for an honest reason;
  a currency in a band does not. R3 is now FX-only.
- **R4 broke the euro.** Length is not economic primacy: `EURUSD` starts in
  1999 and `DKKUSD` in 1991, so the longer-history rule dropped the euro in
  favour of a currency held in a narrow band against it. A named preference now
  wins over length and must state why.

### Vendor metadata was checked, not trusted

`$USTSY` advertises 1990 and delivers from 2022 — excluded. Currency labels are
taken from the vendor's authoritative field and **checked** against a measured
diagnostic, because that field labels `$NIF` and `$SEN` — both Indian indices —
USD and INR respectively. The diagnostic regresses each index return on its
currency's USD return.

The check was **not decisive for emerging markets** and is reported as such: a
local EM index genuinely moves with its own currency through risk appetite, so a
moderate loading proves nothing. The first threshold flagged nine markets on
exactly that confound. The diagnostic is recorded as evidence, the threshold now
flags only loadings near the mechanical +1 of translation, and it is skipped
where the currency itself failed R2/R3 (a regression on a policy band produces
an unstable beta).

---

## 4. Experimental design

**Timing.** Decide from information through the close of session *t*; enter at
the close of *t+1*; measure *t+1 → t+1+h*. The universe spans Tokyo, London and
New York closes stamped with the same calendar date, so a signal built from
"day *t* closes" could otherwise trade on moves that happened after some markets
had already shut. Nobody can buy the Nikkei at yesterday's Tokyo close.

**Overlap.** Forecast dates step by the full horizon, so no two observations of
an *h*-session return share a day. Overlapping windows inflate the effective
sample by roughly *h* and would make every t-statistic here a fiction.

**Partition.** Chronological, never random:

```
DISCOVERY 1996-01 .. 2012-11  |embargo|  VALIDATION 2013-01 .. 2020-11  |embargo|  LOCKBOX 2021-01 .. 2026-07
```

A decision whose holding window crosses a boundary is dropped from the earlier
segment rather than allowed to leak.

**Targets and their primary metrics**, declared before any validation number
existed — no metric shopping afterwards:

| target | primary metric | baseline (fitted on TRAINING only) |
|---|---|---|
| excess return | OOS R² | unconditional training mean |
| positive-return probability | log-loss skill | training base rate |
| realised volatility | QLIKE skill | trailing realised volatility |
| cross-sectional rank | rank IC | zero forecast |

**Budgets** (ceilings, enumerated not searched): baseline ≤ 40, pooled ≤ 60,
regime ≤ 30, combined ≤ 40, total ≤ 170. **105 executed.** Every executed
configuration enters the denominator, including failures.

---

## 5. What was actually found

### Prediction improved

- 46 of 105 configurations survived Benjamini–Hochberg (q = 0.10) on their
  validation forecast score.
- Best validation cross-sectional rank IC: **0.095** (ridge, fully pooled,
  60-session horizon, t = 1.99).
- Volatility was the easiest target, as expected: QLIKE skill **+0.19** for the
  HAR model over trailing realised volatility.
- Pooling barely mattered. Fully pooled, economic-group pooled and hierarchical
  shrinkage produced rank ICs within 0.0001 of each other, and the transparent
  12–1 momentum rule matched the learners.

### It did not convert

**Zero of the 96 configurations with an economic path beat a volatility-matched
benchmark/cash control on validation** — at any horizon:

| horizon | configs | min t | median t | max t |
|---|---|---|---|---|
| 5 | 32 | −4.15 | −3.11 | −0.39 |
| 20 | 32 | −4.91 | −2.82 | −0.55 |
| 60 | 32 | −4.85 | −4.15 | −0.51 |

This is the contract's own branch: a candidate that predicts better but cannot
produce superior after-cost economics is **not investable alpha**.

The control is the reason the answer is honest. Release 32 measured six sleeves
against **cash** and all six beat it; not one beat a volatility-matched mix.
Over a long window anything holding risk beats bills, so excess over cash
measures **exposure**, not skill. The control holds
`w = book_volatility / benchmark_volatility` of the benchmark (capped at 1 — this
paper book has no leverage) and the rest in cash: the book's risk with none of
its timing.

### The one apparent exception was one currency

Five lockbox finalists showed positive after-cost excess over the control
(+0.0032 to +0.0041 per 60-session period, Sharpe ≈ 1.6, net ≈ +10.4 % to
+11.2 % annualised, and positive at every cost multiplier up to 4×).

Leave-one-market-out attributed **all of it to one market**:

| candidate | base mean excess | removing `TRYUSD` | retention |
|---|---|---|---|
| `COMBINED_REFINEMENT:103` | +0.004141 | **−0.006910** | −1.67 |
| `POOLED_STATISTICAL:057` | +0.003793 | **−0.006622** | −1.75 |
| `REGIME:084` | +0.003221 | **−0.006379** | −1.98 |

Retention is *negative*: removing the Turkish lira does not merely shrink the
result, it reverses it. The apparent broad cross-market predictive edge is a
short-the-lira trade riding one currency's collapse over 2021–2026. A result
that depends entirely on one market is not broad predictive edge, which is why
leave-market-out is a **gate** in this release and not commentary.

### Lane B

Genuinely point-in-time information was acquired for free and it did not rescue
the result. Regime configurations using filtered HMM states over market and
ALFRED-vintage macro state produced rank IC 0.0872 against 0.0873 for the
identical model **without** the state — the regime added nothing measurable.
That comparison is the whole design: every regime candidate is run against the
same model on the same features minus the state probabilities.

---

## 6. Integrity

**Point-in-time probe — measured, not declared.** The design matrix is rebuilt
from a panel truncated at 2015-06-30 and every row before the cut must be
identical: **16,170 rows checked, 0 mismatched**. A single `rolling(...)` written
without a shift would make the contract's no-look-ahead claim false, and only an
executable probe catches that.

**Filtered states only.** HMM state beliefs are `P(S_t | data up to t)`, never
the smoothed `P(S_t | all data)`. A regime strategy driven by smoothed states can
look like flawless market timing while being impossible to trade — the single
most common way a regime study fools itself. The HMM is fitted on training
observations and later blocks are filtered forward through frozen parameters.

**Training-only scaling.** Feature centring and scaling are fitted on training
rows. Scaling a validation block by its own statistics tells the model something
about the period it is about to be tested on.

**Point-in-time sources.**

| source | state | why |
|---|---|---|
| ALFRED vintages (8 series) | admissible | each observation carries the `realtime_start` at which that value first existed; CPI alone has 409 distinct vintages back to 1994 |
| CFTC Commitments of Traders (14 markets, 1995–2026) | admissible | Tuesday positioning published the following Friday; a conservative four business days is applied |
| owned market observables | admissible | yields, spreads, implied volatility and breadth are PRICES — stamped by the market, never revised |
| 106 of 144 Norgate economic series | **excluded** | Release 32 measured them changing on the first business day of the period they measure |
| owned earnings / analyst-revision stores | **excluded** | `provider_id: synthetic_test` / `PROXY_LOCAL` — synthetic data cannot support a claim |

### The production gate attributes writes; it does not read the clock

The first validation gate asserted `mtime >= campaign day ⇒ Release 33 wrote
this file`, and blocked the commit on the collection service's lock, service
state and iteration history. That inference is invalid in this estate: the
Release-29 continuous information-collection service exists precisely to
advance those files on a 60-second cadence, independently of any research
campaign. Under the old rule the only remedies were to stop production or to
whitelist a directory — a gate that can only be satisfied by disabling the
system it protects is not a safety gate.

The invariant is now `NO R33-ATTRIBUTABLE OPERATIONAL STORE WRITE`, owned by
`scripts/r33_operational_write_attribution.py`:

- **static** — the R33 source carries no operational write path at all: no
  protected-root name, no owned-file name, no operational owner import, no
  mutating owner call. This runs whether or not any store changed, so a quiet
  directory can never be mistaken for a clean campaign.
- **declaration** — the protected set may not shrink, and an exception must
  name an owner (service id, owner module, exact owned files) rather than skip
  a directory. The owner constants are re-read from
  `api/information_collection.py` and must still match.
- **strict roots** — the seven stores with no independent writer keep the
  original rule; there is nobody else a change could be attributed to.
- **attribution** — under a service-owned root a recent write is acquitted only
  when the declared service owns that exact path, the file names that service
  as its writer, and it carries no Release-33 marker.

It fails closed: unrecognised file, foreign or missing `service_id`,
unparseable content, a log record naming an unknown writer, lock and state
disagreeing about who is running, or any raised exception all block the commit.
Live worker evidence is corroboration only — the operator may stop the
collection service, and a stopped service must not block a clean campaign.

The measured outcome for this release: **five** files under
`information_collection` changed on the campaign day and all five were
attributed to `PAPER_TRADER_INFORMATION_COLLECTION`, instance
`800455d6-…`, PID 14920 — a worker started at 11:14:51, two and a half hours
*after* the campaign's last artifact was written at 09:07:29. Zero writes were
attributable to Release 33, and no R33 marker appears anywhere under the
protected root.

---

## 7. Campaign v1 is superseded

`r33_predictive_edge_v1` → `SUPERSEDED_GATE_DID_NOT_ENFORCE_A_FROZEN_TERM`.

Two defects, both in the **gate**, neither in a measurement:

1. the contract states that a candidate with fewer than
   `MIN_SCORED_FORECAST_DATES` scored dates in a segment cannot carry a verdict
   for that segment. The gate never read it, so every lockbox result — all
   resting on **23** scored dates against a declared minimum of **24** —
   satisfied the two predictive conditions;
2. subperiod stability returned no flag when it had too few observations to
   measure, so "not dependent on a single subperiod" passed **vacuously** for
   exactly the candidates whose stability could not be checked.

v2 enforces the frozen minimum and makes the stability check fail closed. The
change is **strictly tightening**: it can remove a qualification, never create
one. The selection rule, seeds and judge are identical, so v2 selects the same
finalists and gives each the same single lockbox execution. **The lockbox was
not reopened.**

That the supersession is a gate fix rather than a quiet re-measure is asserted,
not assumed: **1,260 per-candidate fields compared across v1 and v2, 0
mismatches, identical judge behaviour hash** `959a42afdc33f4d1`.

---

## 8. Honest limitations

- **Implementability is not proven** and is not claimable. See §2.
- **Return definitions are heterogeneous.** Equity indices exclude dividends
  (measured at **2.09 %/yr** from the owned `$SPX`/`$SPXTR` pair rather than
  assumed), bond indices include coupon, FX spot excludes carry. The primary
  economic construction is cross-sectional and zero-mean *within* asset class,
  where a constant per-market drift offset largely cancels.
- **Carry is absent where the estate cannot support it.** Bond carry is
  observable from the owned curve; FX carry needs foreign short rates and
  commodity carry needs a futures term structure. Neither exists here, so those
  features are recorded ABSENT rather than approximated by something that would
  look like carry and measure something else.
- **The lockbox is short at the 60-session horizon** — 23 scored dates, below
  the contract's own minimum of 24. This is now enforced.
- **Finalist selection concentrated on one horizon.** Ranking eligible
  candidates by primary-metric *value* favours the 60-session horizon, where
  rank IC is mechanically larger and the sample is smallest; all eight lockbox
  slots went there. The rule was frozen before the results and was not changed
  after seeing them. A future campaign should rank on statistical strength
  rather than raw effect size across horizons of very different sample size.
- **Emerging-market currency labelling remains uncertain** for some indices.
  Recorded, not silently trusted.

---

## 9. Where the evidence lives

```
D:\Stock_Prediction_app_data\predictive_edge_r33\r33_predictive_edge_v2\
    research_contract.json          data_inventory.json
    futures_universe.json           pit_information_manifest.json
    feature_registry.json           candidate_registry.json
    predictive_results.json         economic_results.json
    multiple_testing.json           lockbox_manifest.json
    robustness_results.json         lane_c_readiness.json
    final_verdict.json
```

`r33_predictive_edge_v1/` is preserved on disk as history and may select
nothing.

Owner package `alpha_agent/r33/`; runner
`scripts/run_release33_predictive_edge.py`; regression
`tests/test_release33_predictive_edge.py`; audit guard
`check_release33_predictive_edge` (39 sub-checks, negative-probed).
