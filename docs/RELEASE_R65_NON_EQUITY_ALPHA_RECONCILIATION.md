# Release 65 — non-equity alpha reconciliation

**Workstream B of R65_MULTI_ASSET_ALPHA_ACTIVATION. Research only.** No
promotion, no adoption, no allocation, no purchase, no order. The CPU
correction is reported separately in
[RELEASE_R65_MATURATION_ELIGIBILITY.md](RELEASE_R65_MATURATION_ELIGIBILITY.md).

Every number below was measured directly from the canonical stores on
2026-09-22 (R46 forward ledgers, R59 `research_memory.sqlite`, the canonical
accrual store, the R61 cost-budget owner). Nothing is quoted from a summary.

---

## The headline, in plain English

**We have no non-equity alpha that could receive capital, and the closest
thing to a candidate fails the estate's own economics before statistics even
get a say.**

Fifty-six ideas are frozen and collecting forward evidence. Twenty-five of them
are non-equity. Of those twenty-five, fourteen have produced any scored result
at all, and exactly **one** has a positive average — commodity curve carry, at
+0.94% per five-day holding period.

That one candidate dies twice over:

1. **Statistically it is one market episode, not twelve observations.** It
   emits a new position every day and holds for five days, so consecutive
   "observations" share four days out of five. Twelve overlapping observations
   are about **2.4 independent ones**. The t-statistic falls from 2.81 (invalid)
   to **1.26** (honest) — indistinguishable from zero. Four of its twelve
   results (+2.55%, +3.02%, +2.17%, +0.95%) are overlapping windows covering
   one commodity move in the week of 8–11 September.
2. **Economically it is already disqualified.** It replaces 200% of its book
   every five days. Run through the estate's own canonical cost gate
   (`alpha_agent.r61.cost_budget`), that construction costs **3.53%/yr in
   trading alone against a pre-registered ceiling of 3.0%/yr** — and that is at
   its *own optimistic* assumption of 1.75 bp per side. It would need a gross
   premium of 5.03%/yr merely to clear the 1.5%/yr materiality floor. At a
   realistic thin-market cost of 10 bp/side the drag is 20%/yr and it would
   need 21.7%/yr gross.

So the honest answer to "how much could it contribute after costs?" is: **at
this construction, nothing, and the estate's own gate says so.**

---

## 1. The 56 forward-frozen candidates, reconciled

Population: `hypotheses WHERE outcome = 'FORWARD_FROZEN'` in
`D:\Stock_Prediction_app_data\r59_autonomous_alpha\research_memory.sqlite` →
exactly 56. Asset-class split: US_EQUITY 31, CROSS_ASSET 7,
EQUITY_INDEX_FUTURES 6, RATES_FUTURES 4, COMMODITY_FUTURES 3, FX_FUTURES 2,
VOLATILITY 2, CREDIT_PROXY 1 → **25 non-equity**.

Forward ledger: **699 predictions emitted, 190 outcomes scored.**

### 1a. The 14 non-equity candidates that have produced scored evidence

`t_raw` is on overlapping observations and is **not** a valid statistic;
`t_eff` divides the observation count by the horizon to approximate independent
blocks. Alpha is net of realised cost, versus each challenger's own declared
control, **per observation** (one holding period), not annualised.

| challenger | asset class | hz | scored | n_eff | mean alpha | t_raw | **t_eff** | at 2x cost | hit |
|---|---|---|---|---|---|---|---|---|---|
| r46_3_comdty_curve_carry | COMMODITY_FUTURES | 5 | 12 | 2.4 | **+0.0094** | 2.81 | **+1.26** | +0.0087 | 0.75 |
| r46_3_spx_turn_of_month | EQUITY_INDEX_FUTURES | 1 | 4 | 4.0 | +0.0003 | 0.07 | +0.07 | **−0.0003** | 0.50 |
| r46_4_macro_surprise_rates_5d | RATES_FUTURES | 5 | 1 | 1.0 | +0.0000 | — | — | −0.0003 | 1.00 |
| r46_rates_curve_rv_5d | RATES_FUTURES | 5 | 11 | 2.2 | −0.0005 | −3.66 | −1.64 | −0.0009 | 0.18 |
| r46_4_credit_regime_spx_timing | EQUITY_INDEX_FUTURES | 5 | 12 | 2.4 | −0.0006 | *degenerate* | *degenerate* | −0.0012 | 0.00 |
| r46_4_credit_hy_ig_momentum | CREDIT_PROXY | 5 | 12 | 2.4 | −0.0014 | −2.48 | −1.11 | −0.0020 | 0.33 |
| r46_6_cot_commercial_xs_5d | CROSS_ASSET | 5 | 10 | 2.0 | −0.0026 | −2.62 | −1.17 | −0.0032 | 0.30 |
| r46_4_spx_announcement_day_premium | EQUITY_INDEX_FUTURES | 1 | 5 | 5.0 | −0.0028 | −1.06 | −1.06 | −0.0034 | 0.20 |
| r51_fx_xs_carry_cip | FX_FUTURES | 5 | 10 | 2.0 | −0.0049 | −5.22 | **−2.34** | −0.0053 | 0.10 |
| r46_4_spx_pre_fomc_drift | EQUITY_INDEX_FUTURES | 1 | 2 | 2.0 | −0.0053 | −59.5 | −59.5 | −0.0059 | 0.00 |
| r46_4_cot_xs_positioning_flow | CROSS_ASSET | 5 | 12 | 2.4 | −0.0069 | −6.87 | **−3.07** | −0.0076 | 0.00 |
| r46_3_rates_curve_carry | RATES_FUTURES | 5 | 12 | 2.4 | −0.0084 | −5.97 | **−2.67** | −0.0087 | 0.00 |
| r46_3_vx_term_carry_1d | VOLATILITY | 1 | 16 | 16.0 | −0.0134 | −1.28 | −1.28 | −0.0160 | 0.31 |
| r46_vx_term_carry_5d | VOLATILITY | 5 | 13 | 2.6 | −0.0278 | −2.03 | −0.91 | −0.0304 | 0.38 |

Read that column of signs: **13 of 14 are negative**, three of them
significantly so on effective observations. This is not an inconclusive
sample; for FX carry (CIP), cross-asset positioning flow and rates curve carry
it is early evidence that the *forward* implementation loses money.

### 1b. Ten non-equity candidates have emitted but never scored

`r46_comdty_xs_mom_252`, `r53_comdty_xs_skew_12m`, `r46_3_fut_xs_mom_252`,
`r46_4_cot_xs_positioning_reversal`, `r46_fut_ts_mom_252`,
`r53_fut_xs_value_5y`, `r46_spx_trend_200d`, `r52_eqidx_xs_rel_mom_12_1`,
`r46_fx_xs_mom_252`, `r52_rates_copper_gold_lead` — all 21-session horizons
emitted from 2026-08-25/26 with 15–20 predictions each. Twenty-one challengers
in total (including equities) are in this state.

**This is the single most important dated fact in the estate right now.**
Measured, not estimated: **429 predictions sit at horizon 20, with
`effective_as_of` running 2026-08-26 → 2026-09-23 across 20 distinct dates, and
exactly ZERO of them have been scored.** The newest scored maturity anywhere in
the ledger is 2026-09-21. Counting 20 trading sessions forward from the first
emission (2026-08-26, with 2026-09-07 a holiday) puts the **first maturity of
that cohort at about 2026-09-24 — two trading sessions from now.**

The first genuine forward outcomes for time-series trend, cross-sectional
commodity momentum, FX momentum, rates lead-lag and cross-asset value are
therefore imminent, and they will more than double the estate's scored forward
evidence. They require no new work — only that the research runtime keeps
running, which is exactly what Workstream A protects.

### 1c. One non-equity candidate has zero forward evidence and never will

`R59_CALENDAR_TERM_STRUCTURE_F9BE2426` (CROSS_ASSET, frozen 2026-09-04) is
registered with **neither** forward-evidence owner. It has accrued nothing in
18 days. This is the known R60 gap
(`NOT_REGISTERED_WITH_FORWARD_EVIDENCE_OWNER`), still open.

### 1d. A measurement defect found during reconciliation

`r46_4_credit_regime_spx_timing` reports `realised_gross_return` **exactly
equal** to `realised_benchmark_return` on all 12 observations. Its residual is
identically 0.0 and its alpha is identically `−cost` every time — which is why
its t-statistic computes as ≈ −5×10¹⁶ (a zero-variance series with a non-zero
mean).

The challenger has never taken a position different from its benchmark. It is
not producing weak evidence; it is producing **no** evidence, and is
structurally guaranteed to lose exactly its cost forever. This belongs beside
the R46.6.2 finding that the VX lane was structurally unable to emit. It should
be either repaired or retired, not left accruing a tautology.

---

## 2. The newer non-equity pipelines have produced nothing yet

The canonical accrual store
(`D:\Stock_Prediction_app_data\canonical_forward_accrual\accrual_projection.json`)
holds **8 registrations, 5 predictions emitted, 0 matured observations, 0
forfeitures**. `matured_observations_total = 0` across the entire store.

That includes all three of the newest non-equity forward pipelines:
`ALPHA_RECOVERY_FX_CARRY_CADENCE_H1_F9B1ACA7` (FX, inception 2026-09-10, 1
emission pending), `ALPHA_RECOVERY_FUTURES_TS_TREND_H21_V1` (85 markets,
inception 2026-09-18, NOT_DUE) and the next-open options-skew sibling
(NOT_DUE). **None of them has a single matured forward observation.**

---

## 3. The candidate, quantified

`r46_3_comdty_curve_carry` — cross-sectional commodity futures curve carry,
5-session horizon, control = risk-free accrual on tied-up capital (DGS3MO).
38 predictions emitted, 12 scored 2026-09-03 → 2026-09-21, 26 pending.

```
mean net alpha vs control   +0.0094  per 5-session observation
stdev                        0.0116
mean realised GROSS         +0.0109
mean realised cost           0.0007  (7.0 bp per observation)
mean one-way turnover        2.0000  (200% of the book per rebalance)
hit rate                     0.75
```

### Overlap: twelve observations are 2.4

A new position is emitted daily and held five sessions, so consecutive
observations share four of five days.

| | value |
|---|---|
| n scored | 12 |
| effective independent blocks (n / horizon) | **2.40** |
| t on 12 overlapping observations | +2.81 — **not a valid t** |
| t on 2.40 effective observations | **+1.26** |

The per-observation series makes this visible: 2026-09-08 +2.55%, 09-09
+3.02%, 09-10 +2.17%, 09-11 +0.95% are four overlapping windows over **one**
commodity move. Naive compounding of the mean gives +60.6%/yr; that number is
the point estimate of a mean whose own t is 1.26, and stating it as a return is
precisely the false-survivor trap R59 recorded.

### Economics: it fails the estate's own gate at every cost assumption

Run through `alpha_agent.r61.cost_budget.evaluate_cost_budget` with its own
measured turnover, 5-session cadence, ceiling 3.0%/yr:

| assumed cost per side | annual cost drag | budget | gross needed for materiality | alpha/obs after cost |
|---|---|---|---|---|
| 1.75 bp *(what it is scored at today)* | **3.53%/yr** | **HALT** | 5.03%/yr | +0.94% |
| 5 bp | 10.08%/yr | HALT | 11.58%/yr | +0.81% |
| 10 bp | 20.16%/yr | HALT | 21.66%/yr | +0.61% |
| 15 bp | 30.24%/yr | HALT | 31.74%/yr | +0.41% |
| 25 bp | 50.40%/yr | HALT | 51.90%/yr | +0.01% |

**It fails at its own optimistic cost rate.** The binding constraint is not the
signal — it is a construction that turns over 200% of its book every five
sessions. R61's ceiling exists precisely to stop the estate spending burden on
a cell whose required gross premium is larger than anything its evaluation
conventions were built to find.

### And the cost rate itself is not credible

R59 memory holds an OPEN, FREE, zero-cost data opportunity,
`FUTURES_EXECUTION_COST_REALITY` (`gate_verdict: NO_PURCHASE_REQUIRED`,
`expected_value: PROTECTS_AGAINST_FALSE_POSITIVES`):

> *"a uniform cost rate across 103 markets makes a thin market look as cheap as
> the ES contract; any future futures survivor must be re-tested under
> per-market costs before it can be believed."*

The 1.75 bp/side implied by this candidate's own forward rows is an ES-like
rate applied to a cross-sectional commodity book. The free proxy — owned
Norgate volume and open interest — has never been used to replace it.

---

## 4. Why nothing can receive capital today

For every non-equity candidate, the path to the existing canonical multi-asset
frontier is the same and is **not** blocked by missing machinery. The machinery
exists: `api/canonical_forward_accrual.py` accrues, `alpha_agent/r52/runtime.py`
matures, `alpha_agent/r51` + `alpha_agent/r52/frontier_refresh.py` rank, and
`capital_eligibility_gate` decides. What is missing is evidence.

| gate | status |
|---|---|
| forward evidence exists | 14 of 25 non-equity have any; 10 have emitted nothing scored yet; 1 has nothing at all |
| **matured** forward observations | **0** in the canonical accrual store |
| effective (overlap-adjusted) observations | 1.0 – 16.0; the best candidate has 2.4 |
| statistical significance on effective observations | **none positive**; three significantly negative |
| clears R61 cost budget (3.0%/yr) | **no** — the only positive candidate is at 3.53%/yr |
| above the measured detection floor (MDE_80 ≈ 3–6.5%/yr net) | not established for any |
| independent validation (skeptic) | not reached — no candidate has been submitted |
| risk/portfolio ruling | not reached |
| human governed review + approval | not reached |
| `promotion_ready_count` | **0** |

**There is no manufactured survivor here. The correct report is a gap.**

---

## 5. What is actually blocking the research queue

The R59 queue has 17 blocked jobs and **zero runnable**. Classified by the
canonical owner (`alpha_agent.r59.blockers`):

```
FAMILY_EXHAUSTED     14   "generator produced no non-degenerate candidate"
DEPENDENCY_BLOCKED    3   "engine returned NO_MEMBERS"  (all r59.cross_asset)
every blocker classified: True     unclassified: 0
needs_new_information: 17          time_will_clear: 0
```

The precise missing input, stated once: **not CPU, not time, and not a new
dataset purchase.** Fourteen jobs need genuinely new information because their
families are prosecuted to a verdict. The three cross-asset jobs fail for a
different and more specific reason — the R59 cross-asset engine **cannot
construct a member set**, which the campaign census attributes to the governor
having no futures feature declared. CROSS_ASSET is simultaneously the only
frontier state that is `RESEARCH_READY` and the only lane whose engine cannot
run. That is one bounded engineering defect, not an information wall.

Frontier states: CROSS_ASSET **RESEARCH_READY**; COMMODITY_FUTURES,
EQUITY_INDEX_FUTURES, FX_FUTURES, RATES_FUTURES, US_EQUITY, VOLATILITY
**EXHAUSTED**; CREDIT_PROXY **BLOCKED**.

---

## 6. Where the next genuine non-equity experiment is

The last alpha campaign was **R60_INFORMATION_FRONTIER (2026-09-20):
NO_ROBUST_ALPHA_FOUND**, 8 cells, 0 survivors. No campaign has run since; R61
was infrastructure and R62–R64 were operational.

R60's own director recorded two things that matter here:

- **Basis momentum is UNDERPOWERED, not dead**, and must **not** be retested on
  the same panel under any re-parameterisation. It is blocked behind power.
- The 0.40 one-way turnover ceiling was **MISSPECIFIED_AS_A_SCALAR**: it priced
  futures at equity costs and killed two non-equity cells *before any return
  was scored* — `FUT_OPEN_INTEREST_GROWTH_COMMODITY` (0.598 one-way) and
  `FUT_INTL_INDEX_OPEN_INTEREST_GROWTH` (0.612). The director's condition for
  re-registration was explicit: *"the constraint schema is corrected
  campaign-wide and pinned by a test FIRST; only then may a new experiment id
  be minted and charged."*

**R61 satisfied that condition.** `alpha_agent/r61/cost_budget.py` replaced the
scalar with an annualised cost-budget gate, `alpha_agent/agents_v2/pipeline.py`
gates on `cost_budget_within_ceiling`, and R61's own docstring prices those two
cells at **0.72%/yr and 0.73%/yr** — comfortably inside the 3.0%/yr ceiling
that just disqualified the commodity carry candidate.

Those two cells are confirmed re-registerable in memory: `H_b5e5e7c0_f3e92699f95c`
(r60_05) and `H_47e2cf84_49076c9d82f0` (r60_07) both carry
`reopen_condition = NEW_PRE_REGISTRATION_UNDER_THE_CORRECTED_CONSTRAINT`, and
neither ever had a return computed, so there is nothing to fit to. Exchange
open-interest growth is a positioning/flow mechanism, not a price formula.

### Two things this reconciliation got wrong, corrected by the director

The campaign was pre-registered by `quant-research-director` as
`R65_NON_EQUITY_COST_HONEST_FRONTIER`
(`research/agents/campaign_r65_non_equity/`). It refused two items this
reconciliation had proposed, and it was right on both. Both have been verified
directly against the stores:

1. **Terms-of-trade data is NOT certified.**
   `CERTIFICATION_DF5B_TERMS_OF_TRADE.json` reads
   `"CERTIFICATION": "DATA_HOLD"`, with `hypotheses_unblocked: []` and the
   exact construct (PH11, "cross-currency commodity terms-of-trade
   relative-value") sitting in `hypotheses_blocked` pending frequency and lag
   verification. The existence of a file named `CERTIFICATION_*` is not a
   certification. A relative-value cell whose signal leg has an unmeasured
   publication lag cannot state its observability instant and is not
   pre-registrable. This goes to `data-foundation-agent` for a future campaign.
2. **`FUT_OPEN_INTEREST_GROWTH` as the census rank-4 phrases it is already
   settled.** `H_b5e5e7c0_483c5c1043eb` (aggregate whole-curve OI growth,
   commodity, h21, net of per-market and roll cost) is
   `NO_ALPHA_EVIDENCE` with `reopen_condition = NEW_ORTHOGONAL_INFORMATION`,
   which R65 does not supply; a second null, `H_b5e5e7c0_7f7cdc392f88`, sits in
   the same family. Only the **held-dated-contract** variant that the R60
   director's own reopen condition names is admissible, and its prior is
   accordingly LOW, recorded before any result.

Two process defects fall out of that and should be fixed at source:

- `research/agents/NEXT_CAMPAIGN_CENSUS.json` rank 4 proposes, in its own
  words, a hypothesis the memory has already settled null. **The census queue
  is not passing its own duplicate check**, and the spec-hash novelty check
  would not have caught it either.
- The role-brief construction path presented a `DATA_HOLD` artifact as an
  available dataset.

### The prerequisite, and how the director ruled on it

`FUTURES_EXECUTION_COST_REALITY` was put to the director as a straight choice:
prerequisite build, or follow-on re-test? It rejected both and recorded
`PER_MARKET_COST_MANDATORY_AT_SCORING, OBSERVED_COST_TABLE_MANDATORY_AT_CLEARING`,
on a premise worth stating here because it sharpens this document's own finding:

> There is no "flat futures rate" in the governed path. The R38 native
> dated-contract layer already carries a per-market `cost_bps_per_side` vector
> (2–15 bp), and `cost_budget.py` structurally refuses to coerce a prose rate to
> a number — a caller supplying neither a vector nor a measured effective rate
> gets `COST_BUDGET_NOT_EVALUABLE`, which fails. **The flat-rate exposure the
> R59 memory entry names belongs to `futures_panel_v1`** — the back-adjusted
> panel — not to the native layer.

That is precisely where `r46_3_comdty_curve_carry` lives, which is why its
forward rows imply a uniform 1.75 bp/side. All three R65 cells were therefore
placed on the R38 native layer, where the problem does not arise, and the
observed-cost table was made a **blocking pre-condition on `director_clear`**:
no R65 cell is cleared and no forward registration is raised until the observed
table exists, is certified, and the survivor is re-scored under it and still
passes. The `doubled_cost` adversarial check was made blocking rather than
advisory.

### The campaign, and its honest prior

Three cells, not four — the fourth slot was left deliberately unspent because
no admissible candidate exists, and the cap is a ceiling, not a target.

| executor | label | owner | class | H | panel MDE_80 | cost budget |
|---|---|---|---|---|---|---|
| r65_01 | FUT_INTL_INDEX_OPEN_INTEREST_GROWTH_COST_HONEST | volatility-liquidity-agent | EQUITY_INDEX_FUTURES | 21 | 4.32%/yr net | 1.03%/yr @5bp, 2.50%/yr @15bp |
| r65_02 | XA_METALS_TO_COMMODITY_CURRENCY_LEAD_LAG_H3 | trend-breadth-signal-agent | CROSS_ASSET | 3 | **unmeasured at this horizon** | turnover-capped 0.333 @5bp |
| r65_03 | FUT_COMMODITY_OPEN_INTEREST_GROWTH_COST_HONEST | volatility-liquidity-agent | COMMODITY_FUTURES | 21 | 6.56%/yr net | 1.22%/yr @5bp, 2.65%/yr @15bp |

**All three sit at or below their panel's measured detection floor.** The
director declared this ex ante: r65_01 LIKELY UNDERPOWERED, r65_03
STRUCTURALLY UNDERPOWERED, r65_02 POWER UNMEASURED at a 3-session horizon. A
binding interpretation rule is frozen with the spec: any lockbox null below the
panel MDE_80 at the denominator actually used is filed `UNDERPOWERED` with
`do_not_read_as: EVIDENCE_OF_ABSENCE`.

Experiment ids are **not yet minted** — the governed path refused
(`PIPELINE_REFUSED FEATURES_NOT_PUBLISHED`), so the spec carries
`PENDING_PREREGISTRATION_*` placeholders until the data-foundation chain
completes. Nothing durable has been written to research memory.

**The director's own escalation, recorded verbatim in substance:** R65 does not
fix the estate's real constraint — a 44-observation lockbox against a 1.5%/yr
materiality floor — and is not presented as fixing it. Raising detection power
is a human decision.

---

## 7. Registered next evidence steps

| # | step | owner | requires |
|---|---|---|---|
| 1 | Let the 21-session cohort mature. 21 challengers, ~15–20 predictions each, first maturities 2026-09-22/23. | `alpha_agent/r52/runtime.py` (protected by Workstream A) | nothing but uptime |
| 2 | Certify per-market futures execution costs from owned Norgate volume/OI. | `data-foundation-agent` | $0, owned data |
| 3 | Re-register exchange open-interest growth (commodity + international index) under the R61 cost budget with NEW experiment ids. | `quant-research-director` → `volatility-liquidity-agent` | step 2 |
| 4 | Register `XA_TERMS_OF_TRADE_RELATIVE_VALUE` on the only RESEARCH_READY frontier. | `quant-research-director` → `reversal-signal-agent` | R60 certification already in hand |
| 5 | Rule on `r46_4_credit_regime_spx_timing`: repair or retire. It is accruing a tautology. | `quant-research-director` | this document |
| 6 | Close the `R59_CALENDAR_TERM_STRUCTURE` registration gap, or retire the freeze. | forward-evidence owner | — |
| 7 | Fix the R59 cross-asset engine's `NO_MEMBERS` (no futures feature declared to the governor). Bounded engineering, unblocks the only RESEARCH_READY lane. | R59 governor owner | — |

Nothing above promotes, adopts, allocates, orders or purchases anything.
