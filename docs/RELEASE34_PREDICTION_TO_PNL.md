# Release 34 — Prediction-to-PnL Conversion

**Terminal verdict: `R34_PREDICTION_DOES_NOT_CONVERT`.
SYSTEM_RESULT = PASS. ALPHA_RESULT = FAIL.**

55 executed configurations, 0 qualified candidates, $0 spent, production
untouched. Research only: no order, proposal, decision, allocation, model
promotion, sleeve activation or operational write.

Campaign id `r34_prediction_to_pnl_v2` (v1 superseded, see §8).
Research root `D:\Stock_Prediction_app_data\prediction_to_pnl_r34\`.

---

## 1. The result in one paragraph

Release 33 found measurable predictive structure and no economics, and its one
attractive result was a single currency. Release 34 asked whether the
**conversion layer** between a forecast and a portfolio was what destroyed the
value. It is not. On a genuinely implementable universe — 47 US-listed ETFs on
total-return prices, selected by measured rule from an enumeration that includes
2,476 delisted products — the frozen R33 model families reproduce real
out-of-sample ranking skill: **rank IC 0.0647, Newey-West t = 3.39 over 233
non-overlapping monthly decision dates**. The best of 55 pre-registered
conversion configurations turns that into a book earning **+5.25 %/yr net,
6.03 % volatility, Sharpe 0.65, max drawdown −19.0 %**. A passive mix of
**0.351 × SPY and cash** — carrying the book's risk and none of its timing —
earned **+5.25 %/yr**. The after-cost excess is **+0.002 %/yr, t = 0.004**.
Every basis point the book earned was beta. A plain 60/40 portfolio earned
+8.39 %/yr at Sharpe 0.68 and beat it outright.

---

## 2. What was actually implementable (Lane A)

R33's panel was labelled `SIGNAL_RESEARCH_VALID` and never
`FUTURES_IMPLEMENTABILITY_PROVEN`, because nobody can buy `TRYUSD` or
`$BCOMGR`. R34's universe carries the label
**`IMPLEMENTABLE_RESEARCH_UNIVERSE`**, and it is earned rather than claimed.

| | |
|---|---|
| candidate pool | 8,139 exchange-traded products — **5,663 live and 2,476 delisted** |
| after global filters | ETFs only (431 ETNs excluded for issuer credit risk), no leveraged or inverse (1,114 excluded), no currency-hedged duplicates (140 excluded) |
| admitted | **47 instruments across 11 asset classes**, 1999-01-04 → 2026-08-20 |
| prices | **total return**, dividends reinvested — measured gap 1.97 %/yr (SPY), 3.50 % (TLT), 6.34 % (HYG) |
| selection rule | longest usable history among candidates matching each declared exposure slot; ties to higher median dollar volume. **Neither criterion is a function of returns**, so the choice cannot leak |
| liquidity | floor $5m median dollar volume, and separately **point-in-time**: an instrument is tradable on a date only if its trailing-252 median cleared the floor on THAT date |
| costs | by **measured liquidity tier**: 1.5 bps a side for mega-cap, up to 30 bps for the two thinnest. Illiquidity is priced, not pretended away |

One slot went unfilled and is recorded rather than quietly dropped:
**base metals** — the only candidate, `DBB`, medians $2.1m a day and is below a
defensible liquidity floor. The estate cannot express that exposure tradeably.

**Survivorship.** Assembling a universe from products that exist today is a
hindsight portfolio, and this estate has measured that bias at 2.74× and 3.42×
in two earlier releases. The enumeration therefore reads the delisted database
too, dead products compete on equal terms, and an instrument that stops quoting
is held to its last session and forced to cash.

---

## 3. Prediction is real, and better than R33's

The frozen R33 feature families (28 features) and model families were refit on
the new instrument returns. Nothing was searched.

| horizon | model | rank IC | t | dates |
|---|---|---|---|---|
| 20 | hierarchical shrinkage | **+0.0647** | **+3.39** | 233 |
| 20 | pooled ridge (α=10) | +0.0425 | +1.96 | 233 |
| 5 | hierarchical shrinkage | +0.0341 | **+3.68** | 937 |
| 5 | pooled ridge (α=10) | +0.0364 | +3.62 | 937 |
| 60 | hierarchical shrinkage | +0.0134 | +0.34 | 111 |

The model is selected per fold on the **inner-validation** block by rank IC,
inside the training partition. Elastic net at α=0.1 shrinks every coefficient to
zero and produces no rank IC at all; that is recorded, not hidden.

---

## 4. The conversion layer works. It has nothing to convert.

Five lanes, each varied with the others held at a default declared before any
result existed.

| lane | best | Δ utility vs control |
|---|---|---|
| **C** calibration | Bayesian shrinkage to zero | −0.0115 |
| **D** sizing | expected return / predicted variance | −0.0115 |
| **E** horizon | 5+20, equal weight | −0.0114 |
| **F** turnover | turnover-penalised target | −0.0076 |
| **G** portfolio | shrunk mean-variance | −0.0006 |
| **finalist** | all five combined | **+0.00006** |

Every single-lane configuration loses to the risk-matched control. The combined
finalist reaches almost exactly zero, and the margin — **+2.07×10⁻⁵ annualised,
t = 0.004** — is not distinguishable from zero by any measure the campaign
carries.

**Forecast magnitude does add value beyond rank** (+0.0362 in utility): the
rank-only book is materially worse than the calibrated one. But the measured
calibration slope on evaluation rows is **−0.55** — the model's magnitudes are
not merely too large, they are inverted out of sample, which is why every
calibration that shrinks harder scores better and the one that shrinks toward
zero wins.

**Cost is not the killer.** The winning book turns over 0.39× a year and pays
**1.3 bps a year**. Even at the severe-stress multiplier the excess moves by
0.04 percentage points.

---

## 5. The prediction-to-PnL attrition waterfall

Required whether or not alpha qualifies. Annualised, on the winning book.

| stage | value | drop |
|---|---|---|
| RAW_FORECAST_SKILL | +3.74 % | — |
| **PERFECT_FORESIGHT_SIZED** | **+37.31 %** | — |
| CALIBRATED_EXPECTED_RETURN | +7.55 % | **+29.76 pt** |
| AFTER_SIZING | +2.45 % | +5.10 pt |
| AFTER_CONSTRAINTS | +5.70 % | −3.25 pt |
| AFTER_TURNOVER_CONTROL | +5.59 % | +0.11 pt |
| AFTER_COST | +5.57 % | +0.02 pt |
| **AFTER_RISK_MATCHED_CONTROL** | **−0.06 %** | **+5.63 pt** |
| AFTER_UTILITY_CHARGE | −0.06 % | −0.00 pt |

**Share of the perfect-foresight ceiling captured: 14.9 %.**

The ceiling matters more than any other number here. The same machinery — same
caps, same sizing, same transition rule, same costs — driven by the realised
return earns **37.3 %/yr**. The conversion layer is not lossy; it is fed a
forecast that is 15 % of the way to useful. And of the value that does survive
to `AFTER_COST`, **every point of it is what a 0.351 × SPY + cash mix earned
anyway**.

Interesting secondary reading: **constraints ADD 3.25 points**. The uncapped
mean-variance book concentrates into a few low-volatility names and does worse
than the capped one. The 20 %/40 % caps are not a tax here, they are a
correction to an over-confident optimiser.

### Failure-mode decomposition

| mode | measured | evidence |
|---|---|---|
| forecast too weak | **yes** | captures 14.9 % of perfect foresight despite t = 3.39 |
| magnitude poorly calibrated | **yes** | evaluation calibration slope −0.55 |
| sizing destroys rank skill | **yes** | −5.10 pt from neutral to chosen sizing |
| turnover consumes edge | no | 1.3 bps a year |
| diversification dilutes edge | no | constraints add 3.25 pt; 10.95 effective instruments |
| **risk-matched benchmark dominates** | **yes — decisive** | −5.63 pt, the whole realised return |
| exposure neutrality removes alpha | no | utility charge costs 0.00 pt |
| works only in one asset class | no | see §6 |
| works only in one horizon | no | all nine horizon sets within 0.003 of each other |
| works only under unrealistic cost | **yes, trivially** | positive at OPTIMISTIC, negative at STRESSED — but the whole range spans 0.05 pt |
| covariance / risk forecast error | partly | variance-based sizing beats rank-based by 0.023 |

---

## 6. This is NOT R33's single-market failure

R33's five lockbox finalists all showed positive after-cost excess and
leave-one-market-out attributed every bit of it to `TRYUSD`. That failure mode
does not occur here, and the direct measures say so clearly:

| measure | value | threshold |
|---|---|---|
| max single-instrument share of gross PnL | **1.2 %** | ≤ 40 % |
| max single-asset-class share | **7.4 %** | ≤ 60 % |
| max mean single-instrument weight | 18.6 % | ≤ 25 % |
| effective instruments (inverse Herfindahl) | **10.95** | ≥ 5 |

The book is genuinely diversified. Per-asset-class contribution is spread across
all eleven classes, led by US sectors (+1.62 %) and US equity (+1.14 %).

**The leave-one-out sign gate nonetheless fails, and the reason is recorded
rather than glossed.** The base excess is +2×10⁻⁵ with t = 0.004, so removing
*any* instrument reverses its sign — 36 of 47 do. That is a statement about the
base being zero, not about concentration. The gate was frozen before evaluation
and is not loosened after it; instead
`concentration_results.json` carries `sign_reversal_test_is_informative: false`
and a sentence saying which numbers to read instead. A reader must not mistake
this for a TRYUSD finding.

---

## 7. Temporal design — no fake fresh lockbox

Six nested chronological walk-forward folds, 2008 → 2026, expanding training
window, session-accurate embargo, all selection inside the training partition.

| fold | net | control | excess |
|---|---|---|---|
| 2008–2010 | +3.67 % | +1.37 % | **+2.35 %** |
| 2011–2013 | +5.92 % | +4.81 % | **+1.08 %** |
| 2014–2016 | +3.23 % | +3.93 % | −0.67 % |
| 2017–2019 | +5.38 % | +6.49 % | −1.07 % |
| 2020–2022 | +5.54 % | +4.23 % | **+1.19 %** |
| 2023–2026 | +7.44 % | +10.08 % | −2.49 % |

Three of six folds positive — chance. There is a readable pattern: the book beats
the control in turbulent periods and loses in strong bull markets, because it
holds cash and diversifiers. That is a description of its exposure, not evidence
of skill.

**`FRESH_UNSEEN_EVIDENCE_EXISTS = False`, declared in the contract before the
campaign ran.** R31, R32 and R33 all selected on evidence through 2026, and
R33's lockbox opened 2021 onward and was accessed eight times. No untouched
historical block remains. Calling the last fold a lockbox would be a fiction, so
`walkforward.evidence_state()` refuses it in one place, and
`R34_ALPHA_QUALIFIED` is therefore **structurally unreachable** in this release
— stated in advance, not discovered afterwards.

---

## 8. v1 superseded — a guard that could not fail

v1 produced the same verdict and was superseded before it was accepted, for a
defect found by reading its own finalist table: three finalists differing in
calibration and in sizing reported **identical economics to seven significant
figures**. The multi-horizon conviction override was built once from the winning
lane settings and reused for every neighbour, so two of the four lanes the
parameter-cliff gate probes could not move — vacuous in exactly the way R33's v1
stability check was.

v2 rebuilds the override per finalist, marks any neighbour whose weights do not
actually differ as `NO_EFFECT` and excludes it from the retention median, and
adds a frozen minimum-engagement condition so that a book holding almost nothing
— whose excess over a risk-matched control is approximately zero and would
therefore outrank every genuinely negative candidate — cannot be reported as the
campaign's best result. Both changes can only remove a qualification. The
universe, the frozen models, the partition and the seeds are identical.

Three further corrections were made before the verdict was accepted, each of
which made a number *less* flattering:

- the **perfect-foresight ceiling** was being routed through a calibration
  fitted on model scores, whose slope is negative in one fold. That inverted the
  ceiling and reported the realised book capturing **98 %** of perfect foresight.
  The true figure is **14.9 %**;
- the **planned configuration count** was hand-typed as 12 for a family the
  frozen grid enumerates 18 of. It is now derived from the grids, so plan and
  enumeration cannot disagree;
- **Benjamini-Hochberg** uses two-sided p-values, so its single rejection —
  `CALIBRATION::RANK_BUCKET_EMPIRICAL` — is a candidate that significantly
  **loses** to the control. Reporting "1 of 55 survived" would have been badly
  misleading. The rejections are now split by direction and only the positive
  list can support a qualification: **0 candidates beat the control**.

---

## 9. Integrity

- **Cost on traded notional**, both legs, at measured per-instrument tiers.
- **The control decides.** Excess over cash may not rank anything; the
  volatility-matched benchmark/cash mix is the primary control and cannot use
  leverage.
- **Primary statistic declared before evaluation**: after-cost excess utility,
  `U(x) = μ_ann − ½γσ²_ann`, γ = 2, both legs on the same dates.
- **HNES declared before evaluation.** R33's finalists were all h=60 because a
  60-session rank IC is mechanically larger on a twelfth as many observations.
  R34 scores horizons on annualised IR × observation shrinkage × stability. In
  this campaign every horizon's training HNES was negative, a range in which the
  score carries no ordering information; the combiner clamps at zero and
  degrades to equal weight, and `ordering_is_meaningful: false` says so.
- **Denominator counts all 55 executed configurations**, ceiling 80. Controls
  are reference objects and do not enter it.
- **No operational write.** Enforced statically by the audit and at runtime by
  the canonical attribution rule added in Release 33.

---

## 10. What Release 35 should not conclude

This is not "the conversion layer was wrong". It was tested five ways and the
best of them reached zero. It is not "costs were too high": the winning book
pays 1.3 basis points a year. It is not "the universe was not tradable": every
instrument is an exchange-listed fund on total-return prices with a measured
liquidity tier.

The binding constraint is the same one Releases 31, 32 and 33 measured, and R34
now puts a number on it: **the forecast captures about 15 % of what the same
machinery would earn with perfect foresight, and 100 % of what it does earn is
available from a passive mix of the benchmark and cash.** A rank IC of 0.065 at
t = 3.39 is real and it is not enough. The next release should either buy
information that raises that 15 %, or stop asking this question.
