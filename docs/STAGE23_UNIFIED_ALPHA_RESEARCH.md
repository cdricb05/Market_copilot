# Stage 23 — Unified Autonomous Alpha Research & Model Evolution

**Status:** research complete, no promotion proposed, zero operational mutations.
**Branch:** `stage19-controlled-rebalance` · **Base HEAD:** `47eeeaf`
**Owner module:** [alpha_agent/stage23_unified.py](../alpha_agent/stage23_unified.py)
**CLI:** [scripts/run_stage23_unified_research.py](../scripts/run_stage23_unified_research.py)
**Research root:** `D:\Stock_Prediction_app_data\stage23_unified_alpha_research`

---

## 1. Stage objective

Tie the alpha models we already have, the operational ensemble, the
champion/challenger machinery, the autonomous research agent, the owned
point-in-time datasets and the future Steele/Intrinio analyst history into ONE
research system, and — for the first time — measure **where the current
operational edge actually comes from**.

Stage 23 adds no new statistic, no new gate, no second registry and no promotion
path. It contributes one genuinely new component: a leakage-safe **adapter** that
turns the owned panels into the `periods` cross-sections the released evaluator
already consumes, plus the attribution and prioritisation reporting built on it.

---

## 2. The current alpha system (verified from source, not assumed)

| Item | Value | Owner |
|---|---|---|
| Operational strategy | `fundamental_momentum_50_50_v1` | `api/universe_scoring.py` |
| Operational book | `fundamental_momentum_50_50_top25` | `api/alpha_book.py` |
| Compute kernel | `build_current` | `api/multi_horizon_engine.py` |
| Model contracts | five cadences per model | `api/multi_horizon_registry.py` |
| Component (slow) | `composite_sn`, weight **0.50** | frozen Phase 10-L panel |
| Component (medium) | `mom_6_1`, weight **0.50** | `api/monthly_momentum_emitter.py` |
| Research challenger | `composite_sn_repaired` | `api/alpha_factory.py` |
| Candidate lifecycle | `CandidateRegistry` | `alpha_agent/tournament.py` |
| Research memory | Unified Alpha Research Registry | `alpha_agent/research_registry.py` |
| Automatic promotion | **impossible** (`AUTOMATIC_PROMOTION_ALLOWED = False`) | `api/universe_scoring.py` |

**Corrections to prior assumptions.** `fundamental_momentum_50_50_v1` is not a
separate model — it *is* the fixed 50/50 blend of `composite_sn` and `mom_6_1`,
combined as cross-sectional z-scores. `composite_sn` is labelled CHAMPION and
`mom_6_1` CHALLENGER in `universe_scoring`, but **both drive live rankings** via
the blend; the champion/challenger labels describe research provenance, not
whether a leg influences the book.

### The two owned research panels

| | Momentum monthly | Frozen fundamental (Phase 10-L) |
|---|---|---|
| Rows | 307,972 | 38,725 raw → 21,085 deduped |
| Periods | 313 months (2000-08 → 2026-08) | 120 months (2016-06 → 2026-05) |
| Names / period (median) | 984 | 219 |
| Distinct tickers | 2,728 (1,347 delisting-tagged) | 545 |
| Survivorship | **SAFE** | **SURVIVOR-BIASED** |
| Target column | `fwd_1m_return` (21d) | `forward_63d_return` (63d) |
| Sector coverage | **0.0 %** | 33 % (current-as-of) |

The fundamental panel uses per-ticker **staggered** rebalance dates and its first
month (2016-06) is a bulk history seed carrying up to 99 rows for one ticker.
Stage 23 deduplicates to one row per (ticker, month) — dropping 17,640 duplicate
rows — and excludes the seed month, so no artificial cross-section is scored.

**Point-in-time contract.** `mom_6_1(m) = close[m-1]/close[m-7]-1` skips the most
recent month. `realized_vol_63d` and `adv_dollar` are trailing as of month-end.
`fwd_1m_return(m)` is the **target**; a feature at month *m* may read it only at
months strictly before *m*. `MomentumPanel.trailing_returns` is the single place
that reconstruction happens and it enforces the boundary. A regression blanks the
target column and asserts every feature is unchanged.

---

## 3. Current-model edge attribution (the main deliverable)

All comparisons on the **shared joint cross-section** — same dates, same names,
same 63-day horizon, 117 quarterly periods, ~170 names. This is the only
apples-to-apples basis for an incremental-contribution claim.

| Signal | rank IC | IC t | spread t | gross ann. | net25 | max DD |
|---|---|---|---|---|---|---|
| `composite_sn` | 0.0397 | **3.12** | 1.34 | 5.67 % | 3.67 % | −81.8 |
| `mom_6_1` | 0.0232 | 1.20 | 1.74 | 9.18 % | 7.18 % | −107.9 |
| **ensemble 50/50** | **0.0430** | 2.83 | **2.23** | **9.41 %** | **7.41 %** | **−44.2** |

**Partial rank IC** (each leg controlling for the other):

| | mean partial IC | t |
|---|---|---|
| `composite_sn` \| `mom_6_1` | 0.0389 | **3.09** |
| `mom_6_1` \| `composite_sn` | 0.0245 | 1.28 |

**Subperiod IC sign stability** — the decisive result:

| Block | `composite_sn` | `mom_6_1` | ensemble |
|---|---|---|---|
| 2016-07 → 2018-11 | +0.064 | +0.084 | +0.094 |
| 2018-12 → 2021-04 | +0.030 | −0.020 | +0.015 |
| 2021-05 → 2023-10 | +0.069 | −0.027 | +0.018 |
| 2023-11 → 2026-03 | **−0.005** | +0.057 | +0.046 |
| **sign-stable** | no (3/4) | no (2/4) | **yes (4/4)** |

### Answers

**A. Is the edge stock-selection alpha?** *Partially, and only via the fundamental
leg.* `composite_sn` retains essentially all its rank information after `mom_6_1`
is partialled out (t≈3.09). `mom_6_1`'s incremental rank information is not
distinguishable from zero (t≈1.28).

**B. How much is factor/beta exposure?** Not size or liquidity — neutralising log
dollar volume leaves `mom_6_1`'s IC unchanged (99.4 % retained). Neutralising
market beta or realised volatility **raises** the (still insignificant) IC from
0.0032 to ~0.0070, so adverse beta/vol exposure is *diluting* the momentum leg
rather than creating it. **Sector: unmeasurable** — see §4.

**C. Which component contributes incrementally?** `composite_sn` contributes the
**ranking** information; `mom_6_1` contributes **diversification**. The legs are
nearly uncorrelated (spread correlation **−0.066**, cross-sectional rank
correlation 0.053, top-25 overlap 42 %), so blending lifts spread t above either
leg alone and roughly **halves the drawdown**. The 50/50 is buying regime
insurance, not rank quality: each leg fails in the block where the other works.

**D. What is redundant?** *Neither.* They are complements. This is the strongest
available justification for keeping the frozen 50/50 exactly as it is.

**E. Where does the model fail?**
1. On the deep survivorship-safe panel, `mom_6_1` over **312 monthly
   cross-sections** is weak: rank IC 0.0128, **t = 1.27**, and flat through
   2005-2016. Its 2016-2026 contribution is recent, not structural.
2. `composite_sn` **cannot clear the project's own evidence-completeness gate**,
   because its only panel is survivor-biased (see §7).
3. Ensemble turnover is ~1.0 per quarterly rebalance, so net edge is materially
   below gross.

**F. What would most improve it?** Not another price factor — the owned price
panel is exhausted and Stage 23's own additions found nothing. The gap is that
the leg carrying the ranking information rests on an uncertifiable universe. The
highest-value additions are a **survivorship-safe PIT fundamental basis** and
**analyst expectations history**.

**Weight sensitivity** (the frozen weights are never fitted): fund70/mom30 gives
the best rank IC t (3.72); fund30/mom70 the best spread t (2.28) and net25
(8.14 %). 50/50 sits between. Nothing here justifies changing it, and Stage 23
does not propose to.

---

## 4. Sector neutralisation is BLOCKED, not skipped

The momentum panel is **0.0 %** sector-known. The owned repaired map covers 418
of 2,728 panel tickers (37.1 % of rows) and is **current-as-of**; applying it to a
historical cross-section injects a classification look-ahead. The tournament
records the identical wall independently (`point_in_time_gics`: 5 of 50 required
symbols PIT-classified; "current Norgate GICS is undated").

Stage 23 therefore reports sector neutralisation as
`BLOCKED_NO_POINT_IN_TIME_SECTOR` with the measured coverage and **never
substitutes the look-ahead map**. A regression enforces this.

---

## 5. Owned-data challenger campaign

Seven pre-registered, economically-motivated hypotheses on the survivorship-safe
monthly panel. Deliberately **excludes** every construct the tournament already
rejected (low vol, idiosyncratic vol, vol-scaled momentum, momentum acceleration,
short-term reversal, channel breakout, trend-slope t, path efficiency, realised
skewness, max daily return, 52-week high, vol-of-vol, short/long vol ratio,
overnight family, seasonality) — re-running those is correlated-variant tuning,
not research.

| Hypothesis | periods | rank IC | IC t | spread t | net25 | turnover | BH q | corr vs mom | verdict |
|---|---|---|---|---|---|---|---|---|---|
| `s23_size_liquidity` | 312 | 0.0094 | 1.80 | **2.19** | **+5.4 %** | **0.19** | 0.394 | **−0.28** | FAILED_ROBUSTNESS |
| `s23_momentum_consistency` | 303 | 0.0133 | 1.59 | 0.78 | +1.3 % | 0.29 | 0.394 | 0.84 | FAILED_ROBUSTNESS |
| `s23_path_drawdown` | 303 | 0.0148 | 1.20 | −0.11 | −1.8 % | 0.18 | 0.541 | 0.82 | FAILED_COSTS |
| `s23_amihud_illiquidity` | 311 | −0.0045 | −0.82 | 1.05 | −0.1 % | 0.60 | 0.719 | −0.71 | FAILED_COSTS |
| `s23_residual_momentum` | 287 | 0.0009 | 0.11 | 1.70 | +2.9 % | 0.38 | 0.949 | 0.69 | FAILED_ROBUSTNESS |
| `s23_dollar_volume_shock` | 300 | 0.0017 | 0.28 | −0.29 | −5.3 % | 0.71 | 0.949 | 0.82 | FAILED_COSTS |
| `s23_liquidity_trend` | 300 | −0.0004 | −0.06 | −0.55 | −5.9 % | 0.59 | 0.949 | 0.86 | FAILED_COSTS |

**0 of 7 cleared the released gates. 0 of 7 survived Benjamini-Hochberg FDR.**
All seven are registered through the released lifecycle (tournament 55 → 62
candidates, all REJECTED) so the nulls are preserved, not hidden.

Two results are worth carrying forward:

* **`s23_size_liquidity`** is the best near-miss: it survives costs (net25
  +5.4 %), has the second-lowest turnover, is the only candidate with
  `regime_consistency = 1.0`, and is genuinely orthogonal to the momentum leg
  (−0.28). It fails rank IC by a hair (0.0094 vs 0.0100) and IC t (1.80 vs 2.00).
  It is **not** a challenger — it did not clear the gate — but it is the only
  owned-data construct that looks economically different from what we run.
* **`s23_residual_momentum`** is a well-powered **null**: rank IC 0.0009, t 0.11
  over 287 survivorship-safe monthly cross-sections. The tournament previously
  rejected market-residual momentum on a short daily-bar sample at t = 1.96; the
  25-year replication settles it. Do not re-open this family.

**Two existing DATA_HOLDs were resolved by data recognition, not by purchase.**
`dollar_volume_shock` and `amihud_illiquidity_change` were held as
`DATA_HOLD_REQUIRES_VOLUME_TURNOVER_DATA`, yet the owned monthly panel carries
`adv_dollar` on 100 % of rows. Both were evaluated at monthly resolution and
rejected. The **daily** specifications remain untested and honestly still blocked
— the queue says exactly that rather than pretending the family is closed.

---

## 6. Autonomous agent map

**Already existed and is used unchanged:** `research_registry` (Stage 1 memory,
2,163 artifacts / 478 experiments / 121 hypotheses), `experiment_contracts` +
`experiment_factory` + `experiment_runner` (Stage 5), `tournament` (Stage 9
lifecycle, gates, FDR, shadow books), `signal_evaluation`, `orthogonality`,
`selection_controls`, `analyst_revisions` (Stage 13A), `autonomous_research`,
`runtime`, `telegram_control`.

**What was disconnected — and what Stage 23 connected:**

| Gap | Fix |
|---|---|
| The agent evaluated candidates but never the **operational ensemble**, so it had no baseline to be incremental *to*. | `run_edge_attribution` measures the live model through the same contract; `build_analyst_preregistration` anchors future work to that measured baseline. |
| It could not consume the two owned panels — no adapter produced `periods` from them. | `build_momentum_periods` / `build_fundamental_periods` / `build_joint_periods`. |
| It could not tell that **owned data had changed** and unblocked held candidates. | `resolves_existing_data_hold` + queue supersession. |
| The priority queue was implicit in 19 scattered blockers. | `build_priority_queue`, grounded in measured state. |
| Analyst hypotheses were pre-registered **in isolation**, answering the wrong question. | Stage-23 incremental-value requirement attached to all six. |
| No canonical seam from research to portfolio decisions. | `build_decision_link`. |

**Intentionally left separate:** the tournament remains the only candidate
authority; `research_registry` remains the only research memory;
`api/multi_horizon_registry` remains the only model contract; Stage 23 writes a
*report*, never a second registry.

### Stage 23 is NOT a second Persistent Research Agent

`engine/research_agent.py` is the sole Persistent Research Agent **calculation**
owner and `api/research_agent.py` the sole composition/persistence owner;
`scripts/audit_architecture.py::check_research_agent_ownership` enforces this by
requiring the token `def evaluate(` to appear in no other module.

Stage 23 initially named its signal-scoring helper `evaluate`, which tripped that
guard as a pure **naming collision** — the two functions share nothing but a verb:

| | `engine/research_agent.evaluate` | `stage23_unified.evaluate_cross_sectional_signal` |
|---|---|---|
| Input | the operational book's PIT research-evidence contract | a list of cross-sections for ONE candidate signal |
| Asks | is the live champion still supported by accumulated forward evidence? | what are this signal's rank IC / spread / cost / turnover, and which released gate does it hit? |
| Output | research-assessment + lifecycle verdict (`WATCH` / `BLOCKED` / `INSUFFICIENT_EVIDENCE`) | `{row, series, metrics, gate}` |
| Owns math? | yes — it is the kernel | **no** — delegates to `signal_evaluation.evaluate_periods`, `tournament.row_to_contract_metrics`, `tournament.classify_evidence` |
| Scope | persistent, operational | bounded, offline, research-only |

The Stage-23 function was therefore renamed to say what it actually evaluates. No
calculation moved and no number changed: all ten artifacts re-generate
byte-identical after the rename. Two regressions in
`tests/test_stage23_unified_alpha_research.py` now hold the boundary — one
forbids a bare `evaluate` in the module, the other asserts the function still
defines no statistic and no threshold of its own, so the collision cannot be
"fixed" in future by re-forking the released math under a new name.

---

## 7. Two gate observations (reported, deliberately NOT changed)

Changing a gate threshold is a governance decision, not a research one. Both are
recorded with evidence as `REPORTED_ONLY_GATE_UNCHANGED`.

1. **`DOMINANT_LEG_CANNOT_CLEAR_EVIDENCE_COMPLETENESS`.** `composite_sn` returns
   `DATA_HOLD / DATA_HOLD_POINT_IN_TIME_UNAVAILABLE` because
   `require_survivorship_safe = true` and its only panel is the survivor-biased
   545-name EODHD universe. The gate is behaving **correctly**. The consequence is
   severe: the leg carrying most of the operational ranking information is the one
   the project's own evidence contract cannot validate. This is the single most
   important structural finding of Stage 23.

2. **`DRAWDOWN_GATE_IS_LENGTH_SENSITIVE`.** `keep_max_drawdown_pct = -35.0` is
   applied to the drawdown of an **unnormalised cumulative sum** of per-period
   spreads. Over 312 periods `mom_6_1` measures −142.1; all seven Stage-23
   candidates breach it too. As configured, no candidate evaluated over a
   multi-decade panel can pass — depth of evidence is implicitly punished. The
   gate owner should consider a length-normalised drawdown.

---

## 8. Steele / Intrinio readiness

**No historical analyst data is present today.** The tournament records
`distinct_vintage_dates_on_disk: 1` against `required_distinct_vintage_dates: 20`,
with a hard PIT floor of **2026-07-31** and ~19 sessions of calendar accrual
remaining. No provider schema was invented and no paid quota was spent.

The contract, adapters, PIT invariants and adequacy gate already existed in
`alpha_agent/analyst_revisions.py` (Stage 13A). Stage 23 verified them against the
full required field list and found the contract complete **except for two gaps,
now filled inside that existing owner**:

| Gap | Invariant added | Why it matters |
|---|---|---|
| Duplicate revision delivery | `DUPLICATE_REVISION_EVENT_FOR_SAME_KEY_AND_TIMESTAMP` | A bulk extract that paginates or re-delivers overlapping windows inflates revision **breadth** — the primary analyst signal — without adding information. |
| Corporate-action basis | `PER_SHARE_VALUE_WITHOUT_CORPORATE_ACTION_BASIS` | EPS estimates are quoted in the share count of their vintage; a split between estimate and actual fabricates a surprise unless the basis is declared. Relates to `api/corporate_actions.py` (Stage 19). |

Optional schema fields added (so existing records still validate):
`corporate_action_basis`, `fiscal_year`, `fiscal_quarter`.
PIT invariants: 11 → **13**.

### What runs automatically when real history arrives

1. `LocalTrialImporter` ingests into the frozen normalized contract.
2. `pit_scan` classifies all 13 invariants — nothing is silently repaired.
3. The adequacy gate measures depth, breadth, inactive/delisted coverage and
   effective independent cohort count.
4. The six frozen hypotheses evaluate under BH-FDR (family_size = 6).
5. **Stage 23's incremental test** runs: existing model **vs** existing model +
   analyst feature on the shared cross-section, after costs.
6. A survivor enters the same tournament lifecycle as any other candidate —
   still with no automatic promotion.

---

## 9. Prospective analyst programme (Stage 13B)

Immutable prospective formations accrue live with a **hard PIT floor** — vintages
before the first snapshot can never be reconstructed and are never backfilled.
One distinct vintage exists; twenty are required; `min_scored_cohorts >= 12`
binds. Evidence is **immature and must not be adjudicated early**.

Its value is cross-validation: the *same* economic hypothesis will eventually be
tested on two independent evidence classes — historical PIT vintages (vendor) and
prospective TRUE_FORWARD formations (live). Agreement across both is far stronger
than either alone. The two classes must never be mixed in one statistic.

---

## 10. Research priority queue

`HIGH: 0 · MEDIUM: 0 · WAITING_FOR_DATA: 17 · LOW: 9`

**There is no high-priority owned-data research left.** That is the honest state,
consistent with Stage 12 (`OWNED_EXHAUSTED`), Stage 14/15/17 and Stage 23's own
0-of-7. The ordered next work is:

1. **Historical analyst revision vintages (Steele/Intrinio)** — 4 candidates
   blocked; economically disjoint from both realised fundamentals and price; the
   only untested orthogonal family; a real vendor feed satisfies the vintage
   requirement immediately instead of accruing one day at a time.
2. **A survivorship-safe point-in-time fundamental basis** — 5 candidates blocked
   on `SNAPSHOT_ONLY_NOT_PIT`; this is also what would let the *dominant existing
   leg* clear its own evidence gate (§7.1). Highest structural value.
3. **Point-in-time GICS sector** — 4 candidates blocked; also the missing
   neutralisation in §4. Accrues from SEC assigned-SIC filing headers.
4. **Bounded SEC Form 4 / 8-K acquisition** — 3 candidates blocked at 3 of 30
   required issuers; growable from owned sources without any purchase.
5. **Point-in-time event calendar** — 2 candidates blocked.
6. *(low)* Daily-resolution volume constructs — the monthly analogues failed;
   only attempt if daily volume is acquired for another reason.

---

## 11. Portfolio-decision research link

`build_decision_link` defines the canonical seam with seven measures, each
labelled `COUNTERFACTUAL_NOT_PROOF` (re-ranking past holdings) or `TRUE_FORWARD`
(realised outcomes): candidate ranking of current holdings, ranking of
replacement alternatives, deterioration lead time, false-exit rate, replacement
success rate, regret vs the operational decision, turnover-adjusted benefit.

Current status: **`INSUFFICIENT_FORWARD_EVIDENCE`** (minimum 12 matured
observations). The seam exists so measurement starts accruing. No historical
decision is rewritten and no counterfactual certainty is claimed.

---

## 12. Machine-readable outputs

Under `D:\Stock_Prediction_app_data\stage23_unified_alpha_research\runs\<run_id>\`,
with `latest.json` pointing at the newest run:

`model_system_map` · `data_capability_matrix` · `current_model_edge_attribution` ·
`experiment_manifest` · `challenger_results` · `challenger_registration` ·
`research_priority_queue` · `intrinio_readiness` ·
`analyst_revision_preregistration` · `portfolio_decision_research_link`

Run directories are immutable and content-addressed: identical inputs reproduce
an identical `run_id` and identical bytes.

---

## 13. Safety

Research-only and read-only w.r.t. every operational store. No orders, fills,
signals, trade decisions, proposals, rebalance plans, Daily Close, model
promotion or champion replacement. No network, PostgreSQL or prediction service.
Writes are confined to the Stage-23 research root and the research tournament
registry. `AUTOMATIC_PROMOTION_ALLOWED` remains `False`.

---

## 14. Exact continuation point

Stage 23 is complete and uncommitted. **Nothing is staged, committed or pushed;
the backend was never restarted.**

The next actionable work is **not** more owned-data factor search — that is
exhausted and Stage 23 confirmed it with a well-powered null. It is **data
acquisition**, in the order of §10.

Concretely, for the next session:

1. If Steele/Intrinio historical data has arrived: place the extract locally,
   run `LocalTrialImporter`, then `pit_scan` (now 13 invariants), then the
   adequacy gate, then the six frozen hypotheses **with** the Stage-23
   incremental requirement. Nothing else needs building.
2. Otherwise: the highest-value engineering task is a **survivorship-safe PIT
   fundamental panel**, because it simultaneously unblocks 5 held candidates and
   lets the dominant operational leg clear its own evidence gate.
3. Do **not** re-open residual momentum, low-vol, vol-scaled momentum or the
   monthly liquidity family; all are measured and rejected with evidence.
4. Stage 22.1's trailing-history persistence still awaits production acceptance
   on the next real market session; that is unrelated to Stage 23 and untouched.

**Is there enough evidence to propose any model promotion? NO.** No candidate
cleared the existing research gate, no manual promotion gate was reached, and the
operational model is unchanged.
