# Release 61 — Research Apparatus Calibration and Repair

**RUN_ID** `R61_RESEARCH_APPARATUS_CALIBRATION_AND_REPAIR`

Research infrastructure only. No new alpha hypothesis was registered, no
lockbox was spent, no forward request was created, no order or fill exists,
nothing was promoted, nothing was backfilled, and no paid data was bought.

---

## Why this release exists

R60 finished with eight pre-registered experiments across four asset classes,
one lockbox opened and zero survivors. The director's closing ruling was not
about any of the eight:

> The estate has settled ~8,453 hypotheses with zero qualified survivors, but
> the estate has never measured its own detection floor.

An apparatus that has never produced a survivor is observationally
indistinguishable from one with no power. Until that is measured, every
`NO_ALPHA_EVIDENCE` verdict is ambiguous between *there is nothing there* and
*we could not have seen it*, and every further hypothesis is spent at an
unknown detection probability.

So this release measured the apparatus, and repaired the four defects the
measurement exposed.

---

## The headline answer

**The gate invents nothing, and it sees very little.**

The false-positive rate at a zero injected effect is **0 out of 120 on every
one of the five panels**. The qualification path does not manufacture
survivors. Whatever explains 8,453 nulls, it is not Type-I error.

But the detection floor is high. Minimum detectable effect at 80% power, at
the **most favourable** burden denominator the gate admits (1):

| Panel | Instruments | Decisions | Eff. obs (L) | MDE_50 | MDE_80 | MDE_90 |
|---|---:|---:|---:|---|---|---|
| US_LARGE_CAP | 1,897 | 181 | 43 | IC 0.0353 / **3.24%**/yr | IC 0.0453 / **4.85%**/yr | IC 0.0486 / 5.38%/yr |
| EXTENSION_EQUITY | 2,947 | 182 | 44 | IC 0.0400 / **3.76%**/yr | IC 0.0469 / **5.31%**/yr | IC 0.0492 / 5.83%/yr |
| COMMODITY_FUTURES | 39 | 187 | 44 | IC 0.0605 / **4.64%**/yr | IC 0.0788 / **6.56%**/yr | IC 0.0904 / 7.67%/yr |
| EQUITY_INDEX_FUTURES | 14 | 187 | 44 | IC 0.1096 / **3.04%**/yr | IC 0.1412 / **4.32%**/yr | IC 0.1591 / 5.01%/yr |
| CROSS_ASSET_FUTURES | 28 | 187 | 44 | IC 0.0801 / **1.95%**/yr | IC 0.1096 / **2.98%**/yr | IC 0.1327 / 3.85%/yr |

Percentages are the **median lockbox net annual excess** that the injected
information coefficient actually delivered on that panel, measured, not
assumed.

**One caveat on the return column, stated rather than left to be found.** A
run that halts at D or V never computes a lockbox, so the median net excess at
a given `rho` is conditioned on the runs that *reached* the lockbox — the
luckier draws. Where coverage is partial that number is mildly optimistic.
Coverage at the interpolation brackets: near the MDE_80 points it is
86–120/120 or better on four panels and 118–120/120 on commodity futures, so
the **MDE_80 return figures are essentially unconditioned**. The MDE_50
figures sit lower on the curve — the extension universe's 3.76% is bracketed
by 34/120 coverage at `rho = 0.03` — and should be read as a mild
*under*statement of the floor. **The IC column is unaffected**, because
detection rate is counted over all 120 seeds whether they halted or not.

**No single reference number is load-bearing.** The brief names ~3%/yr net as
a yardstick, and the director never formally justified it before results were
seen, so no panel is *graded* by it. The same MDE_80 column, classified at
several thresholds, so a reader can re-grade without re-running anything:

| Panels with MDE_80 at or below… | |
|---|---|
| 2%/yr net | *(none)* |
| 3%/yr net | CROSS_ASSET_FUTURES |
| 4%/yr net | CROSS_ASSET_FUTURES |
| 5%/yr net | CROSS_ASSET_FUTURES, EQUITY_INDEX_FUTURES, US_LARGE_CAP |
| 6%/yr net | + EXTENSION_EQUITY |

At a realistic campaign burden the floor rises sharply, because burden enters
the gate as a multiplier on the lockbox p-value:

| Panel | MDE_80 @ burden 1 | @ 10 | @ 100 | @ 1000 |
|---|---|---|---|---|
| US_LARGE_CAP | 4.85%/yr | 7.60%/yr | 8.71%/yr | 9.50%/yr |
| EXTENSION_EQUITY | 5.31%/yr | 7.18%/yr | 9.34%/yr | 9.98%/yr |
| COMMODITY_FUTURES | 6.56%/yr | 10.63%/yr | 12.54%/yr | 13.75%/yr |
| EQUITY_INDEX_FUTURES | 4.32%/yr | 6.35%/yr | not reached on grid | not reached on grid |
| CROSS_ASSET_FUTURES | 2.98%/yr | 4.19%/yr | 5.32%/yr | 5.96%/yr |

---

## What the calibration actually did

One canonical injection mechanism, pre-registered and content-hashed before
any result was seen. At every decision date, over that date's own eligible
cross-section:

```
z_i     = normal scores of the REALISED forward return  (rank -> N(0,1))
eps_i   = N(0,1), keyed by (seed, panel, DECISION DATE)
score_i = rho * z_i + sqrt(1 - rho^2) * eps_i
```

Both components are standardised, so the ex-ante cross-sectional correlation
between the score and the normal-scored forward rank is exactly `rho`. The
realised Spearman IC is **measured per cell and reported**, not asserted: at
`rho = 0.10` on commodity futures the injection achieved a mean rank IC of
0.144 at `rho = 0.15`, 0.049 at `rho = 0.05` and 0.004 at `rho = 0`.

Everything else is the estate's own: the real panels, the real universe rules
and missingness, the real cost models, the real calendars, the real roll
schedules and delisting paths, the real D→V→L partition, the real
`stage_advance` rule and the real `engines.gate`.

- **Effect grid**, frozen ex ante on published-IC grounds and never moved:
  `0, 0.005, 0.01, 0.02, 0.03, 0.05, 0.075, 0.10, 0.15, 0.20`. Zero is on the
  grid because the gate's own false-positive rate is half the answer.
- **120 seeds per point**, 6,000 governed runs, Wilson intervals on every rate.
- **Detection = the full governed path**: D advances → V advances → L measured
  → `gate` qualified. A halt at D or V is a miss. Discovery-only success is
  not a detection.
- **Turnover realism was checked, not assumed.** The injected signal trades at
  0.90 one-way on equity and 0.66 on futures, against R60's real cells at
  0.870, 0.598 and 0.612 — the same band, so no persistence parameter was
  introduced.

### What binds

Just below each panel's floor, the dominant killers are
`burden_corrected_significant` and `lockbox_material`, with a large minority of
runs halting at **discovery** before a lockbox is ever computed. On the
extension universe at `rho = 0.03`, 77 of 120 runs with a genuinely positive
effect died at D. The lockbox layer carries ~44 effective observations on every
panel, which is what sets the statistical floor.

---

## The four repairs

### A. The turnover gate was wrong, and is replaced

`GATE_MAX_TURNOVER = 0.40` was a raw one-way turnover scalar applied to every
asset class at every horizon. It is horizon-blind and cost-blind. R60's three
halts, recomputed as economics:

| Cell | One-way | Cost/side | Rebalances/yr | Annual drag | Old gate | New gate |
|---|---:|---:|---:|---:|---|---|
| r60_08 EQ H5 reversal | 0.870 | 25 bp | 50.4 | **21.92%** | HALT | HALT |
| r60_05 commodity OI | 0.598 | 5 bp | 12 | **0.72%** | HALT | PASS |
| r60_07 index OI | 0.612 | 5 bp | 12 | **0.73%** | HALT | PASS |

Owner `alpha_agent.r61.cost_budget`, version
`R61_ANNUALISED_COST_BUDGET_V1`:

```
annualized_cost_drag = one_way_turnover * 2 * cost_per_side
                       * rebalances_per_year + additional_ann_cost_drag
rebalances_per_year  = 252 / rebalance_interval_sessions
```

Ceiling **0.03/yr = 2 × the frozen materiality floor (0.015)**. The economic
statement: *a cell may spend on trading at most twice what it must ultimately
deliver net.* At the ceiling the required gross premium is 4.5%/yr merely to
reach the floor the candidate has to clear anyway. The multiple is fixed
against a constant frozen long before this release for an unrelated reason; no
measured return enters the derivation.

It reproduces the old gate where the old gate was calibrated: monthly equity at
25 bp/side sitting exactly on the retired 0.40 scalar costs 2.4%/yr, just
inside the new ceiling. The two agree in the world the scalar came from and
differ exactly where it was blind.

A budget that **cannot be computed** — a frozen cost model stating a
per-instrument vector with no measured effective rate supplied — is
`COST_BUDGET_NOT_EVALUABLE`, and that **fails**. A cost that was not checked
was not met.

Raw turnover survives as a labelled diagnostic and gates nothing. The three
R60 halts stand as recorded and were not re-run.

### B. Drawdown is canonical

Two independent defects produced the `0.0000` the R60 skeptic found.

1. **The accumulator never included the starting capital.** Both layer-stats
   owners computed `peak = maximum.accumulate(cumprod(1+r))`, so the first NAV
   point was its own running peak and a loss in period one was invisible. The
   stream `[-0.30, +0.05, +0.05, +0.05]` reported a maximum drawdown of
   **exactly 0.0000**.
2. **One key name meant two concepts, and one concept did not exist.** The
   futures layer's `max_dd` was the *excess* series; the equity layer's
   `strat_max_dd` was the *strategy* series; the equity layer had no excess
   drawdown at all. A consumer asking for `max_dd` on an equity layer got
   nothing — and nothing, rendered, is 0.0000.

Both are fixed by one owner, `alpha_agent.r61.drawdown`, with two named
concepts kept deliberately separate:

- `strategy_max_drawdown` — what the capital experienced. **The risk agent's
  ruling concept.**
- `excess_max_drawdown` — the drawdown of the series the gate actually rules
  on (`ann_net_excess`).
- `benchmark_max_drawdown` for context.

`read()` returns `MEASURED` or `NOT_MEASURED`; it never substitutes 0.0 for an
absent field. The risk brief now carries `DD.risk_view(...)` with a `rulable`
flag, so a layer where the concept was never measured blocks a drawdown ruling
instead of supplying a zero. Legacy keys remain as aliases; no persisted
artifact was rewritten, and no settled verdict moves, because no gate in the
governed path reads a drawdown field.

### C. The extension identity bridge — **FAIL**

Verdict: **`IDENTITY_BRIDGE_FAIL`**. `EXTENSION_UNIVERSE_REUSE_ALLOWED = NO`.
The substrate stays frozen.

Thresholds were written to disk and content-hashed before any measurement.

| Check | Measured | Threshold | |
|---|---:|---:|---|
| name-channel false-match rate (live) | **14.15%** | 2% | **FAIL** |
| ambiguous-key share | **23.39%** | 10% | **FAIL** |
| attrition forward-return gap | **0.756** | 0.05 | **FAIL** |
| temporal implausibility rate | 1.03% | 5% | PASS |
| extreme-decile concentration multiple | 0.99× | 2.0× | PASS |
| delisted terminal-return coverage | 100% | 95% | PASS |
| share-count sign-error rate | 0.000% | 0.5% | PASS |
| share-count reverting-spike rate | 0.002% | 1% | PASS |

Coverage reproduces R60 exactly — 95.3% overall, **90.8% of delisted**, 99.9%
of live — which is the evidence that the instrumented bridge is the same
bridge.

**The mechanism.** `norm_issuer` strips INC, CORP, CO, HOLDINGS, GROUP, TRUST,
THE and more, then the bridge does `names.setdefault(key, cik)` — *first CIK
wins, silently*. Over the SEC's 1,049,941 name rows that normalisation yields
1,012,400 keys of which **23,631 map to two or more distinct CIKs**, including
the empty string (25 CIKs), `CAPITAL` (14), `ENERGY` (14) and `VENTURE` (9).
Measured on the panel, 657 of 2,809 resolved symbols went through an ambiguous
key, and **92% of the observed live mismatches sit on one**.

The mismatches are not cosmetic. `AA` resolves by ticker to Alcoa Corp
(0001675149) and by name to Alcoa Inc (0000004281) — the predecessor. `AAL`
resolves by ticker to American Airlines Group (0000006201) and by name to
American Airlines Inc (0000004515) — the *operating subsidiary*, which files
separately. Splicing either one's share count onto the listed entity is
precisely the failure the skeptic named.

**Two honest caveats, disclosed rather than buried.**

- The attrition check FAILED its pre-registered threshold, but its own
  supporting statistic says the gap is not distinguishable from zero (Welch
  t = −0.94, p = 0.35, n = 1,297 vs 123). The threshold was specified on the
  *mean* of a heavy-tailed quantity and is a weak instrument. **It was not
  relaxed after the fact** — that is the tuning this release exists to
  forbid — and the verdict does not turn on it: three other checks fail on
  unambiguous measurements.
- The skeptic's sharpest hazard — that the missing tail is disproportionately
  stock-financed M&A at firms that had been repurchasing — is **not
  answerable**, because measuring issuance for an unresolved name requires the
  bridge that failed on it. It is reported as an untestable hazard, never as a
  passed check.

One genuine negative worth recording: the rank-contamination hazard did **not**
materialise. Low-confidence names occupy 0.99× their base rate in the extreme
deciles of the share-count ratio feature — no concentration.

### D. Pre-measurement halts settle

`reveal_stage` judges a *measured* layer, so a cell that stopped before any
return existed had nowhere to go. Four R60 cells were still **open** in the
research memory, and `ResearchMemory.burden` counts `WHERE outcome IS NOT
NULL` — so four cells the director recorded as charged **were not charged at
all**.

`AgentPipeline.record_pre_measurement_halt` is the one canonical path. It
writes through the existing memory and event stream (no second registry), is
idempotent, refuses once any layer has been revealed, and **may never write
`NO_ALPHA_EVIDENCE`**: a cell that computed no return has said nothing about
alpha, and filing silence as evidence of absence is the mislabelling the R60
director refused.

| Cell | Halt reason | Measured | Frozen threshold | Outcome |
|---|---|---:|---:|---|
| r60_04 dividend drift | DATA_COVERAGE_FLOOR | 0.717 | 0.80 | `DATA_HOLD` |
| r60_05 commodity OI | TURNOVER_CEILING_EXCEEDED | 0.5982 | 0.40 | `REJECTED` |
| r60_07 index OI | TURNOVER_CEILING_EXCEEDED | 0.6122 | 0.40 | `REJECTED` |
| r60_08 EQ H5 reversal | TURNOVER_CEILING_EXCEEDED | 0.8700 | 0.40 | `REJECTED` |

Each is recorded **as it fired, against the threshold frozen at the time** —
the three turnover halts stand under 0.40 and carry a `superseded_by` note, not
a new verdict. Burden 8,409 → **8,413**; open pre-measurement halts 4 → **0**.

### E. Agent task sizing (routing unchanged)

Model routing is untouched: director Opus, skeptic Opus, data foundation Haiku
at 15 turns, structured roles cheap. What changed is task **size**.

R60 handed `ASSIGNMENT_DF4_SEC_STORES` two sources and
`ASSIGNMENT_DF5_LIGHT_PROBES` three, each to a 15-turn role, and each then had
to produce one certification per source (DF4A/B/C, DF5A/B/C). That is why the
budget was exhausted and resumption was required.

`alpha_agent.r61.assignments` splits a plan mechanically: **one source per
assignment**, at most four bounded questions, a named artifact that must exist
by turn 7, and a turn budget **read from the routing table** rather than
invented. Run against the real R60 files it flags both and produces bounded
replacements with zero remaining problems.

### F. Worker registry

`alpha_agent.r61.workers` — one file per worker, so two workers never contend
and the registry can never hold two disagreeing rows for one worker.

**PID reuse is defeated.** Every row carries the registering process's own OS
creation time, and liveness is judged on the `(pid, creation time)` pair. A row
whose pid is alive but whose creation time differs reads `INTERRUPTED`, never
`RUNNING`.

**There is no kill verb**, in the registry or in the operator surface, and a
test asserts that neither file mentions `taskkill`, `TerminateProcess`,
`os.kill`, `Stop-Process`, `SIGKILL` or `proc.kill`. The operator's reflex —
hunting `python.exe` by name — is how an unrelated process, a scheduled task,
or Claude Code itself gets terminated by mistake.

Read-only operator command:

```powershell
& C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe `
    C:\Users\binis\paper_trader\scripts\r61_research_workers.py
```

---

## Basis momentum

**`BASIS_MOMENTUM_EVIDENCE_STATE = NOT_TESTABLE_WITH_CURRENT_PANEL`**

Not re-tested, as the director required. The calibration answers it as a
by-product.

R60's three reads produced `|t| ≤ 0.68` — D +1.09%/yr, V −0.82%/yr, and a
disjoint replication at −0.89%/yr. The commodity panel needs a cross-sectional
rank IC of **0.079** (median net 6.56%/yr) for 80% detection, and **0.061**
(4.64%/yr) for even 50%. Published cross-sectional basis-momentum ICs sit
materially below that.

The two axes do not agree, and the disagreement is stated rather than resolved
by assertion. On the **return** axis the comparison is ambiguous: the
literature's figures are decile spreads, while this estate's book is
rank-weighted and group-demeaned across 39 markets — far more diversified, and
translating between them requires an assumption the calibration does not
license. On the **IC** axis, which is the axis the injection is defined in and
the one on which published estimates are directly comparable, the panel is
below the floor.

Where the axes disagree, the governed answer is the conservative one: the panel
could not have resolved the effect, so the R60 null is **silence, not evidence
of absence**. Both basis-momentum experiments were annotated in place —
`evidence_state`, the panel's MDE, and `do_not_read_as: EVIDENCE_OF_ABSENCE` —
with every measured column, statistic and outcome proven unchanged.

---

## What this implies for the ~8,453 historical nulls

Three statements, in decreasing confidence:

1. **They are not false negatives caused by a broken gate.** Zero false
   positives at a zero effect, and clean monotone detection curves, say the
   machinery works as specified.
2. **They are informative about large effects and silent about small ones.**
   Any mechanism that would have delivered more than ~5%/yr net on US large cap
   at a burden of 1 would have been caught 80% of the time. Nulls above each
   panel's floor are real evidence.
3. **A large share of the equity back catalogue is silence.** Most published
   cross-sectional equity premia, net of 12.5 bp/side at ~0.90 one-way monthly
   turnover, land in the 1–4%/yr band — *below* the US large-cap floor of
   4.85%/yr, and far below the 7.60%/yr the floor becomes at a realistic
   campaign burden of 10. Those nulls do not discriminate.

The estate's constraint is not its hypothesis space. It is that a 44-observation
lockbox, a 1.5%/yr materiality floor and a multiplicative burden cannot resolve
the size of effect the reachable universe plausibly contains.

---

## Artifacts

| Artifact | Path |
|---|---|
| Frozen power pre-registration | `D:\Stock_Prediction_app_data\r61_apparatus_calibration\power\PRE_REGISTRATION.json` |
| Per-panel curves and MDEs | `...\power\PANEL_<PANEL>.json` |
| Every measured cell | `...\power\CELLS_<PANEL>.json` |
| Power result + final table | `...\power\POWER_CALIBRATION_RESULT.json` |
| Identity audit pre-registration | `...\identity_audit\PRE_REGISTRATION.json` |
| Identity audit + content-hashed map | `...\identity_audit\IDENTITY_BRIDGE_AUDIT.json` |
| Pre-measurement halts | `research/r61/PRE_MEASUREMENT_HALTS.json` |
| Evidence-state annotations | `research/r61/EVIDENCE_STATE.json` |
| Release report | `research/r61/R61_RELEASE_REPORT.json` |

Reproduce:

```powershell
& .\.venv-win\Scripts\python.exe scripts\r61_power_calibration.py
& .\.venv-win\Scripts\python.exe scripts\r61_identity_audit.py
& .\.venv-win\Scripts\python.exe scripts\r61_settle_premeasurement_halts.py
& .\.venv-win\Scripts\python.exe scripts\r61_annotate_evidence_state.py
& .\.venv-win\Scripts\python.exe scripts\r61_release_report.py
& .\.venv-win\Scripts\python.exe scripts\r61_research_workers.py
```

---

## Guardrails

`scripts/audit_architecture.py::check_release61_apparatus_calibration` — 30
blocking invariants covering: one cost-budget owner with the retired scalar
marked diagnostic and absent from the machine checks; no second module
computing the canonical drag; both layer-stats owners delegating drawdown with
no local accumulator remaining; the halt verb owned by the pipeline, granted to
the four signal agents only, writing no second registry and unable to write
`NO_ALPHA_EVIDENCE`; the calibration unable to pre-register, submit a
candidate, record a result or freeze a forward; the worker registry free of any
kill verb and defeating pid reuse; and routing tiers unchanged.

`tests/test_release61_apparatus_calibration.py` — 69 tests.
