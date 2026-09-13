# R63 canonical scorer: the sparse-event winsor repair

**SPARSE_EVENT_SCORER_BLOCKER = FIXED.** This removes a blocker. It is not a
rescue: no closed family was rerun, and no verdict, threshold, floor or gate
moved.

| | |
|---|---|
| Owner | `alpha_agent.r63.sensitivity` (`run_cell` → `_fit_scaler`) |
| Released scorer | `36d7d47` |
| Change | one helper `_zero_mass_bounds`, one call inside `_fit_scaler` |
| Second scorer | none. Event-specific scorer: none |
| Proof | `tests/test_r63_sparse_event_scaler.py` (27 tests) + the non-qualification audits below |

## 1. The defect

`_fit_scaler` winsorises every feature at its own 1st and 99th **training**
percentiles, then standardises. An event feature is 0 on every row without an
event. When events occupy fewer rows than the winsor fraction, both percentiles
land on the zero mass. The clip then maps every event onto "no event", and the
column becomes a constant. The ridge receives a zero feature, and `run_cell`
returns `NO_RESPONSE` by construction.

The 8-K Item-code family measured this at 0.15–0.52% incidence
(`research/preregistration/EVENT_8K_ITEM_RESULT.md`, "The scorer blind spot
this cell found"). That record stays exactly as released. It describes the
scorer at `36d7d47`, and it declined to patch the owner after its own results.
This repair is made separately, as blocker removal, and changes none of its
conclusions.

## 2. The repair: flatten-only

A clip that turns a non-constant column into a single value deletes the column.
That is not outlier handling. The data minimum is at or below `lo` and the
maximum at or above `hi`, so **the clipped column is constant exactly when
`lo == hi`**.

`_zero_mass_bounds(X, lo, hi)` therefore acts only on a column where:

* `lo == hi == 0`, and
* at least one observation is non-zero.

For such a column, each non-empty side's bound becomes **the same winsor
percentile** (`WINSOR = (0.01, 0.99)`) of that side's non-zero training
observations:

* `hi = P99(x | x > 0)`, when there are positive events
* `lo = P1(x | x < 0)`, when there are negative events

As a result:

* events stay distinct from the zero mass, and a signed event keeps three
  ordered levels
* zero stays inside `[lo, hi]`, so the zero mass is still exactly 0 after the
  clip
* an extreme event is still clipped, at the winsor percentile of the events
  themselves

The repair adds no new constant, fraction or threshold. It uses the training
rows only: the same matrix, in the same call, as the bounds it corrects.
`_apply_scaler` is unchanged.

**Returned untouched, and bit-identical to the release:** every column that
still varies after the conventional clip. That includes:

* a dense column whose 1st or 99th percentile happens to sit on an exact zero
* a count or rank feature
* a signed event whose rarer side alone lands on zero

In the untouched case the function returns the very same `lo`/`hi` objects.

**Limitations (documented, not hidden):**

* **Signed event with one rare side.** If one side is dense enough to move its
  percentile off zero but the other side is not, the rarer side keeps the
  conventional clip, and those events merge into zero. The column still
  responds, so the scorer is not blind. It is coarser on that side.
* **Non-zero point mass.** A column whose "no event" value is not 0 (e.g. a
  constant 3 with rare deviations) is out of scope. Encode "no event" as 0,
  as every estate event signal already does (`CBA.signal_matrix`,
  `A8.signal_matrix`).

### An earlier rule was rejected by its own audit

The first version re-bound any side whose percentile was "swallowed" by the
zero mass, even when the column kept variation. The all-scope block audit
showed that it re-bound a **dense** column in one early fold:
`FUNDING_LIQUIDITY_CONDITIONS` column 2, `effr − cmt3m` (41.6% negative,
55.2% positive, 3.2% exactly zero). That fold was EQUITY_INDEX XS/TS,
VOLATILITY TS and CREDIT_PROXY TS at h=1. The column was never flattened, so
the rule violated dense bit-identity. It was narrowed to the flatten-only rule
above. `test_a_column_that_keeps_variation_is_left_to_the_conventional_winsor`
pins exactly that shape.

## 3. Proofs: the brief's eight requirements

| # | Requirement | Test(s) |
|---|---|---|
| 1 | Dense features bit-identical | `test_dense_features_scale_bit_identically[63,64,65]` checks a 12-column battery: gaussian, t2, lognormal, 30% zeros, ≤0 drawdown, Poisson counts, 1.5%/5%/50% binaries, a 13F-style rank, a constant, rounded ties, and 2% NaN. It compares `lo/hi/mu/sd` bytes and out-of-sample bytes, and requires that no bound is even re-assigned. `test_run_cell_on_a_dense_dataset_is_byte_identical[None, 0.05]` requires the whole `run_cell` output to be byte-identical against the released scaler. `test_a_column_that_keeps_variation_is_left_to_the_conventional_winsor` covers the remaining case |
| 2 | Sparse binary below 1% not flattened | `test_the_released_scaler_flattened_a_sparse_event_and_the_repair_does_not` (0.4%; the defect is measured first), `test_a_single_training_event_is_enough_to_stay_distinguishable`, `test_a_signed_sparse_event_keeps_three_ordered_levels`, and `test_run_cell_scores_a_sparse_event_the_released_scaler_could_not`. In the last, the released verdict is `NO_RESPONSE`; the repaired verdict is not, the increment is > 0, and the verdict still comes from the unchanged ladder |
| 3 | Zero mass remains zero | `test_the_zero_mass_stays_zero_before_standardisation`: a signed lognormal event with NaN; the zero rows are exactly 0 after the clip and one value after scaling |
| 4 | Extreme non-zero values still bounded | `test_an_extreme_event_is_still_bounded_on_both_sides`: ±1e6 outliers give `hi = P99(x>0)`, `lo = P1(x<0)`, and an out-of-sample ±1e9 is clipped to the bound |
| 5 | No future information | `test_a_later_event_cannot_move_an_earlier_prediction`: extreme events injected only on or after 2020-07-01 leave every `pred_B/pred_BD/pred_D` before the cut byte-identical, and do change predictions after it |
| 6 | Existing R63 tests green | the R63, R64, 8-K, 13D/G, 13F, alpha-recovery-offensive and R27 suites (405 passed with the repair) |
| 7 | 13F / 13D/G / 8-K conclusions not changed | `test_closed_family_records_and_verdict_code_are_the_released_ones` (11 files: the preregistrations, the results and the verdict code, each equal to `36d7d47`). The four persisted closed-family artifacts also keep their SHA-256 (§5) |
| 8 | No threshold or gate relaxed | `test_no_threshold_floor_or_gate_moved` checks every constant, plus `alpha_agent/r63/__init__.py` equal to `36d7d47`. `test_only_the_scaler_changed_in_the_canonical_scorer` requires every other top-level node of `sensitivity.py` to be AST-identical to `36d7d47`, and `_fit_scaler` to have gained exactly one statement. `test_the_released_scaler_copy_is_the_committed_one` proves the pinned released copy against git |

## 4. Non-qualification audits on the estate's real matrices

These audits compute no return, rank IC, book or verdict, and write no research
artifact. They replay the scorer's own row selection and folds, and fit both
the released and the repaired scaler on every matrix the scorer would scale.

### 4a. Closed equity families (13D/G, 8-K)

This covers every walk-forward training block of the baseline arm and the event
arm, every blocked inner-CV fit subset of `_select_alpha`, and the
`_residual_share` baseline sample. Each cell is replayed with its own baseline,
and again against the incumbent block.

| Family / cell | h | Baseline arm (train, inner CV, residual share) | Event arm |
|---|---|---|---|
| 13D/G NEW_CONTROL_BLOCK | 5/21/63 | bit-identical, both runs | bit-identical 7/7 folds; released flattened 0 (incidence 1.6–8.0%) |
| 13D/G MATERIAL_BLOCK_INCREASE | 5/21/63 | bit-identical, both runs | bit-identical 7/7 folds; released flattened 0 (incidence 1.18–5.3%) |
| 8-K RESTRUCTURING_OR_IMPAIRMENT vs incumbent | 5/21/63 | bit-identical | released flattened **7/7** folds (incidence 0.16–0.80%); repaired, now changed in 7/7 |
| 8-K RESTRUCTURING_OR_IMPAIRMENT vs owned control | 5/21/63 | only column 16 changes: `DISCLOSURE_INTENSITY_LANGUAGE` column 1, `nt_filings`. It changes in 8 / 8 / 10 of 29 matrices (training fold 2017, plus 2018 at h=63, and their inner-CV subsets). Every changed matrix is one where the release flattened that column. The residual-share sample is bit-identical, and no column is left constant | as above |

13F is not replayed. Its persisted scorer cells stopped at "too few covered
rows or periods", a check that returns before the scaler is called.

### 4b. Every R63 scope, every block, every column, every fold

This covers US_EQUITY XS at h=5/21/63, every futures scope in its XS and TS
modes at h=1/5/21/63, and the credit proxy TS. It spans 51 datasets, 1,269
block instances, 3,048 column instances and 60,227 column-fold fits.

**0 errors. Exactly one column in the estate changes:**

| Dataset | Block | Column | Incidence (all rows) | Folds re-bound | Folds the release flattened |
|---|---|---|---|---|---|
| US_EQUITY XS h=5 | `DISCLOSURE_INTENSITY_LANGUAGE` | 1, `nt_filings` (NT filing in the trailing 365 days) | 1.17% | 1 / 7 | 1 / 7 |
| US_EQUITY XS h=21 | `DISCLOSURE_INTENSITY_LANGUAGE` | 1, `nt_filings` | 1.18% | 1 / 7 | 1 / 7 |
| US_EQUITY XS h=63 | `DISCLOSURE_INTENSITY_LANGUAGE` | 1, `nt_filings` | 1.19% | 2 / 7 | 2 / 7 |

The rule can fire only on a column whose released clip is constant. Re-bound
folds are therefore a subset of flattened folds, and the equal counts make
them the same folds. `nt_filings` is above 1% over the whole sample, but below
it in the early training blocks. There the released scaler deleted it from the
owned 8-K intensity control.

Everything else is bit-identical:

* `FUNDING_LIQUIDITY_CONDITIONS` column 2 (the column the rejected rule
  touched)
* every other US_EQUITY block
* COMMODITY, FX, RATES, EQUITY_INDEX and CROSS_ASSET futures in XS and TS
* VOLATILITY TS
* CREDIT_PROXY TS

That covers every horizon.

### 4c. The one direct caller outside `run_cell`

`alpha_agent.alpha_recovery.market_direction.run_horizon` scales its own
time-series macro matrix with `S._fit_scaler`. The audit replayed its row
selection and walk-forward folds:

* primary: h=1/5/21/63, 31 columns
* rescue: h=21/63, with the rescue feature cut, 26 columns

That is 58 training fits in total. Every fit is **bit-identical**, and the
released scaler flattened no column. The persisted `market_direction.json`
(2026-09-10) is therefore unaffected.

## 5. What this does not change

* **No closed family was rerun.**
  * The 13F, 13D/G and 8-K verdicts stand as released.
  * The persisted artifacts keep their SHA-256: `ownership_breadth_13f.json`,
    `control_block_13dg.json`, `event_8k_census.json`, `event_8k_item.json`.
* **13D/G.** Every matrix its scorer touched is bit-identical, so a rerun would
  reproduce it exactly.
* **8-K.**
  * The owner can now score its event arm, but the family is **not** re-scored.
  * Its final gate was DATA_HOLD on coverage (the unassessable names earn
    6.2–6.7%/yr less, t up to −3.3).
  * Its descriptive drift is wrong-signed and insignificant at every horizon,
    and nothing survives BH at m=3.
  * Scoring it now, after its results, would be exactly the retrospective
    rescue the brief forbids.
* **Persisted R63 sensitivity results** for US_EQUITY
  `DISCLOSURE_INTENSITY_LANGUAGE` were computed with its `nt_flag` column
  (1.17–1.19% incidence overall, below 1% in the early folds) flattened in the
  folds §4b lists. They are not recomputed here. Any future scoring goes through
  the repaired owner.
* **No live path.** The live checkout, the next-open challenger, promotion,
  capital, proposals, orders, fills and backfill are untouched.

## 6. For the next family

An event family below 1% incidence is now visible to the canonical scorer.
Every sufficiency rule still applies unchanged: `MIN_EFFECTIVE_PERIODS = 36`,
the `oos_sufficiency` count of test periods that actually hold an event, BH at
q = 0.10, the materiality floor and the coverage gate. A sparse family can now
be scored, but it still has to reach the effective-period floor on its own
events.
