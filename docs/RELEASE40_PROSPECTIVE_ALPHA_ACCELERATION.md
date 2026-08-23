# Release 40 — Prospective Alpha Acceleration & Open Intelligence Frontier

- **Date:** 2026-08-23
- **Branch:** `stage19-controlled-rebalance` (base commit `8d6d8d7`, the
  Release 39 closeout — verified local == remote before any work)
- **Owner package:** `alpha_agent/r40/` (19 modules)
- **Runners:** `scripts/run_release40_prospective_alpha.py` (phases
  `IEFGHRSQVPLMZ`), `scripts/run_r40_research_cycle.py` (the ONE research
  cycle)
- **Regression:** `tests/test_release40_prospective_alpha.py` (28 tests)
- **Audit guard:** `check_release40_prospective_alpha_acceleration`
  (27 blocking invariants)
- **Research root:** `D:\Stock_Prediction_app_data\prospective_alpha_r40\`
- **Campaign:** `r40_prospective_alpha_acceleration_v1`

Release 39 changed the problem: the estate now holds one historically
interesting machine-generated near miss (WIDE), a real
representation-availability defect behind its confirmation, several distinct
near misses, three immutable non-promotable research shadows, a universal
search engine, a cumulative search burden of 194 trials, and a first
eligible TRUE_FORWARD decision approaching. Release 40's objective was to
maximise **independent information gain per unit of calendar time** while
finishing the materially distinct information and model branches that
remained open — without manufacturing sample size and without waiting for
work that could be done today.

## The seven results, never collapsed

| axis | result |
|---|---|
| SYSTEM_RESULT | **PASS** — every phase executed end to end; 28/28 R40 tests, 71/71 R39 tests, strict audit exit 0, write attribution `ATTRIBUTED` (0 findings, 21 sources) |
| FORWARD_ENGINE_RESULT | **CANONICAL_IDEMPOTENT_CYCLE_READY** — one callable, contiguous catch-up, freshness-aware, chain-hashed, no backdating; `FORWARD_CAPTURE_STATE = READY_WAITING_FOR_ELIGIBLE_DATE` |
| FORWARD_EVIDENCE_RESULT | **NO_TRUE_FORWARD_OBSERVATIONS_YET** — 0 snapshots, 0 outcomes; the first eligible decisions are each market's last session of 2026-08 (2026-08-31) |
| INFORMATION_RESULT | **NO_INCREMENTAL_INFORMATION_EDGE** — NY Fed legacy history decoded and PIT-tested for the first time (best paired increment t 1.26); cross-asset screen kept 0/103 edges after FDR |
| MODEL_RESULT | **NO_MATERIAL_IMPROVEMENT_OVER_R39_TCN** — SSM-lite 1.80, PatchTST-lite 1.77, TabPFN-v2 0.15, Chronos ≤ 1.07 vs TCN 2.07 / ridge-WIDE 1.62 |
| HISTORICAL_ALPHA_RESULT | **FAIL** — nothing clears the R39 Track-K qualification at the cumulative burden (now 230) |
| PROSPECTIVE_ALPHA_RESULT | **NOT_YET_TESTABLE** — no matured TRUE_FORWARD outcome exists |

**Terminal states: `R40_PROSPECTIVE_ENGINE_READY_WAITING_FOR_TIME` +
`R40_NO_INCREMENTAL_EDGE_FOUND` + `R40_COMPUTE_LIMIT_BINDING`** (one
weakly-justified GPU request exists). Binding limitation: **TIME**.

## Track 0 — R39 closeout verified, not trusted

`r39_closeout_import.json` = `R39_VERIFIED`, 0 mismatches: 63/63 research
artifacts and 37/37 repository files hash to the R39 handoff manifest;
608 / 594 / 107 / 12; continuation +87 = **194** from both the ledger
artifact and the reuse ledger itself; 0 Zone-C accesses; WIDE
`c39_c9233eccaa74` spec hash `cfd2ec36…a361c`, +3.4699 %/yr, t 2.4311,
residual-alpha t 2.58, 0/12 kills; three shadows immutable (registry hash
`ae7f76da…f4f4e` reproduces from bytes, coefficient hash reproduces). One
measured discrepancy: the frozen WIDE ridge has **30** live features; the
"86-column bundle" in R39 narrative text counts generated columns — the
artifact wins. The R40 contract hash (Slot-5 rule included) was frozen
into this artifact before any evaluation:
`3c21aff4bcaec2887a69775a49c17bc76ab01e92511867cec35a71fefccf71fb`.

## Track A — the ONE research cycle

`alpha_agent.r40.research_cycle.run_cycle` (wrapped by
`scripts/run_r40_research_cycle.py --mode capture`): eligible dates are
strictly after each shadow's immutable freeze, present in the CURRENT
panel, never in the future, never captured twice; input freshness
(latest session, macro overlay, COT, VX) is measured and stamped on each
snapshot; R39 members are scored through the R39 capture owner
(`research_shadow.capture/mature`), R40 members through the registry-v2
scorer with frozen bytes; catch-up is **contiguous** (every missed date, in
order, lateness recorded, `LATE_CAPTURE_CONTIGUOUS` grade); outcomes mature
with realised per-market forwards, sign accuracy and rank IC as supporting
marks; the always-valid evidence is re-evaluated after every call. Rerun →
`NOTHING_NEW_IDEMPOTENT`. Ledgers are the canonical chain-hashed desk
ledgers. No scheduler was created or changed; attaching the callable to
the Persistent Daily Research Cycle is a separate operator decision.

**Defect found and repaired on the way:** the R39 manual capture command
had never run end to end on this machine — `universal_state.build_futures_panel`
refused a `datetime64[us]` (live Norgate) vs `[ns]` (frozen CSV / COT)
`merge_asof` under pandas 3. Fixed in the canonical builder (output-
preserving for the frozen layer; reconstruction unaffected) and in
`research_shadow.build_fresh_state`.

## Track D — the shadow family (five, frozen before any outcome)

| slot | shadow | candidate | origin | Zone-B t | spec / coefficient hash |
|---|---|---|---|---|---|
| 1 | shadow_wide_xs | c39_c9233eccaa74 | R39 (immutable) | 1.62 (C: 2.43) | cfd2ec36… / e39b7749… |
| 2 | shadow_carry_rule_xs | c39_8278ddd2d3b9 | R39 (immutable) | — | rule |
| 3 | shadow_vx_carry_ts | c39_0574796699fa | R39 (immutable) | 1.60 | rule |
| 4 | shadow_intl_rates_carry_rv | c39_1a0105dd2f0c | **R40** | 2.47 | 935e3de5… / rule |
| 5 | shadow_slot5_c39_fad367467c79 | c39_fad367467c79 (TCN) | **R40** | 2.07 | 95c6bfc5… / b77b2d1e… (torch state_dict sha 93d576ad…) |

Slot 4 is the international-rates carry / relative value rule re-scored
under its R39 id (t 2.47, 129 periods, 11 markets at 3 bps/side, vol-scaled
self-financed GROUP_RV). Slot 5 was resolved by the **pre-declared rule**
(highest Zone-B t among eligible options A/B/C; eligibility t ≥ 1.5,
same-sign halves, positive at 2× cost, |corr| < 0.90 with every existing
shadow, no identical family+expression): A = R39 TCN t 2.07 **eligible**
(correlations with WIDE/carry/intl-RV 0.44/0.33/0.06 — not even partially
redundant); B = corrected WIDE successor t 1.37 **ineligible**
(`ZONE_B_T_BELOW_1.5`); C = best new R40 branch (SSM-lite) t 1.80 eligible
but lower. The TCN was fit ONCE on Zone A+B and frozen as bytes; capture
never refits. New shadows froze at `2026-08-23T17:42:31Z`, R39's at
`2026-08-23T04:02:47Z`. All five: RESEARCH_SHADOW_ONLY /
HISTORICAL_QUALIFICATION = FAIL / PROMOTION_ALLOWED = False; cap 5 enforced
in code.

## Track B — evidence velocity (no fake N)

`evidence_velocity_registry.json` / `effective_sample_analysis.json`:

| shadow | obs/yr | ESS ratio (HAC) | eff. obs/yr | years to e ≥ 20 at point / 50 % / 25 % | IC info ratio |
|---|---|---|---|---|---|
| WIDE | 15.8 | 0.94 | 14.9 | **18.3** / 71.7 / 285 | 1.2× |
| carry rule | 15.8 | 0.98 | 15.4 | 21.9 / 87.3 / 399 | 3.1× |
| VX carry | 50.5 | 0.83 | 41.7 | 26.5 / 105 / — | — |
| intl-rates RV | 13.6 | 0.81 | 11.0 | **9.6** / 37.9 / 151 | 2.3× |
| TCN | 15.8 | 0.78 | 12.3 | 14.3 / 56.1 / 224 | 2.6× |

Cross-sectional dependence: 68 futures markets have an effective number of
≈ 9–10 (participation ratio), so markets × dates is never a sample count.
Daily marks of a fixed monthly position carry **zero** mean information
(the sum is sufficient) — they sharpen variance and drawdown detection
only. The rank-IC channel carries 1.2–3.1× the information per observation
about **predictive skill**; it cannot answer the after-cost question.
Honest arithmetic: under the pre-registered e-process the family discriminates
in years, not months; legitimate accelerators are broader cross-sections and
higher-cadence **new** candidates, the IC channel for the predictive
question, family-level pooling through the averaged e-process, and contiguous
catch-up. A velocity finding: the R39 futility boundary (e ≤ 0.05) is nearly
inert under the null (150–220 years) — the 60-observation horizon is the
real stop, and the anytime-valid confidence sequence is the continuous
"weakened" signal.

## Track C — always-valid designs

`prospective_validation_designs.json` (immutable): the R39 e-process is
reused, an e-process-inverted **confidence sequence** is added, the two new
members receive frozen designs (σ₀ from their Zone-B streams, registered
effect at 50 % shrinkage, e ≥ 20 success, R39 futility/horizon rules), and
the **candidate-family error budget** is declared: per-candidate α 0.05,
union bound 0.25 at five members REPORTED, family-level claims through the
averaged e-process at 1/20. Thresholds never reset. Promotion on boundary
crossing: impossible by construction.

## Track E — the availability defect, measured and removed

`wide_availability_defect_report.json`: with the declared rule (finite in
≥ 50 % of Zone-A AND Zone-B rows), **10 of the 30 frozen WIDE features are
inadmissible**: the five v1 LATENT/GRAPH columns (Zone-B coverage 0.00,
Zone-C 0.74–0.88), `btc_ret_21` (A 0.00 / B 0.18 / C 1.00), `vix_term`
(A 0.00 / B 0.70), `T10YIE`/`DFII10` (A 0.20), `cot_commercial_z`
(A 0.33 / B 0.41). Eight of them are live in Zone C but absent through
selection. The corrected successor (admissible columns + causal availability
masks + repaired calendar-grid latent/graph) reaches Zone-B t **1.37** vs
the original's 1.62 — removing the defect **lowers** selection-stage
economics (paired increment −0.70 %/yr, t −0.72; correlation with WIDE
0.85). The honest reading: part of WIDE's selection evidence rested on
features its training window could not see; the successor is a new
object (`c39_038f3feee792`, new spec hash) and did not earn a slot. Zone C
was never inspected.

## Track F — NY Fed legacy dealer positioning: decoded, PIT-tested, null

`nyfed_legacy_mapping.json` = `BRIDGE_VALID`. The NY Fed's own reference
menu (fetched read-only, hashed) labels every positions code per official
series break (SBP2001 1998-01→2001-06, SBP2013 2001-07→2013-03, SBN2013/
2015/2022/2024). Seven concepts bridge 1998-01-28 → 2026-08-12 (bills,
coupons total, TIPS, **total incl. TIPS**, and three duration buckets from
2001-07 whose 6- and 11-year cuts are identical across formats). Proofs:
`PDPUSGTNOP = bills + coupons + TIIS` holds with **zero** residual (613
weeks); `PDPOSGST-TOT = bills + FRN + coupons + TIPS` holds with **zero**
residual (606 weeks) — the API label says "excluding TIPS" and the
arithmetic wins; seam jumps ≤ 1.9 pooled-change SDs at every break.
Blocked, not invented: the 1998–2001 5-year coupon split
(`BLOCKED_IDENTITY_SEMANTICS`), dealer financing/repo (four taxonomy breaks),
FRN before 2015. A per-market *own-maturity-bucket* inventory feature was
built because a market-invariant series cannot move a cross-sectional rank.
Result (`nyfed_incremental_result.json`, **ZONE_B protocol for the first
time**, 388 Zone-A rows): best paired increment t 1.26 (+0.93 %/yr,
TS ridge + own bucket), best variant t 1.01 — `NYFED_NO_ROBUST_INCREMENT`.

## Track G — open weights under ten conditions

`open_model_technology_registry.json` inventories nine families with HF
API facts. Two were acquired ($0, no account, no click-through, D: only):
**TabPFN-v2 regressor** (ungated HF checkpoint, Prior Labs License 1.1 =
Apache-2.0 + attribution on distribution; sha `2ab5a07d…`) and
**chronos-bolt-small** (Apache-2.0; sha `06a6a19b…`). The `tabpfn`
package's default path demands an interactive Prior Labs login and a
click-through for v2.5 weights — **refused** (fails conditions 4 and 7);
the v2 checkpoint loads through a local `model_path`. Contamination
(`pretraining_contamination_registry.json`): TabPFN-v2 =
`PRETRAINING_DATA_KNOWN_CLEAN` (synthetic prior → clean historical-OOS label
admissible); Chronos = `PRETRAINING_OVERLAP_LIKELY` (Exchange-Rate, FRED-MD
in its public corpus → REPRESENTATION_RESEARCH only); TimesFM/Moirai/TTM/
MOMENT overlap-possible/likely and redundant with the Chronos lane; TabICL
clean but redundant; CLIP `PRETRAINING_UNKNOWN`. Built with PriorLabs-TabPFN.

## Track H — model challenge (same protocol, bounded search)

`r40_model_results.json`, fit Zone A / judge Zone B, ≤ 3 configs per
family screened on Zone A only, ONE Zone-B run each:

| model | Zone-B t | excess/yr | role |
|---|---|---|---|
| R39 TCN (re-scored, exact) | **2.07** | 3.71 % | capability |
| SSM-lite (diagonal state space, from scratch) | 1.80 | 2.90 % | capability |
| PatchTST-lite (from scratch) | 1.77 | 3.28 % | capability |
| ridge WIDE (re-scored, exact) | 1.62 | 2.96 % | baseline |
| GRU (re-scored) | 1.34 | 2.04 % | capability |
| ridge over own + one-hop graph features | 1.30 | 2.29 % | capability |
| Chronos-bolt forecast z, XS rule | 1.07 | 1.89 % | representation |
| graph-MLP (one-hop message passing) | −0.01 | −0.02 % | capability |
| **TabPFN-v2** (5k-row context, clean prior) | **0.15** | 0.31 % | clean OOS |

Nothing materially beats the TCN (margin 0.5 t). Three small sequence
architectures cluster at t 1.8–2.1 above every linear/boosted baseline —
model capability is open on the *scale* axis only, which is the one
(weakly justified) compute request.

## Track I — cross-asset relational research

29 monthly nodes (futures economic groups, macro, VX, ETF sleeves, equity
sectors) under a declared edge-class whitelist; 103 pairs screened on Zone A
with HAC inference; BH at q 0.10 rejected **0** (best: Treasury futures ←
precious metals t −2.83, p 0.005). No edge survived, so no economic book
was built — `CROSS_ASSET_NO_ROBUST_EDGE`. Nodes without 120 Zone-A months
(ETF sleeves, VX, equity sectors) could not be screened under the
training-only rule and are recorded, not silently dropped.

## Tracks J/K/L/M

- **Joint evidence** (`joint_evidence_table.json`): per shadow, selection
  evidence (economic t, IC) next to TRUE_FORWARD evidence; all five are
  `INSUFFICIENT_FORWARD_EVIDENCE`; promotion impossible.
- **Research portfolio** (`prospective_research_portfolio.json`): no pair of
  Zone-B streams above |ρ| 0.70 (max: TCN–WIDE 0.44); attention priority
  favours intl-rates RV and the TCN (highest information rate per unit of
  redundancy).
- **Intrinio** (`intrinio_sample_readiness.json`): `WAITING_FOR_SAMPLE`; an
  inbox + schema/PIT validator exists; purpose
  `SCHEMA_AND_PIT_VALIDATION_ONLY`; nothing sent, purchased or trialled.
- **Compute** (`R40_COMPUTE_ESCALATION_REQUEST.json`): three priced
  requests; only `R40_CE_02_DEEP_SEQUENCE_SCALE` (~6 GPU-hours, $5–12) is
  weakly justified by the cheap evidence; the TabPFN full-context and
  chart-vision requests are not.

## Search burden

`r40_cumulative_search_ledger.json`: **R39_INHERITED 194 + R40_NEW 36 =
CUMULATIVE 230** (273 Zone-B evaluations), inherited through the R39
ledger owner under the R40 root (`r39.register_campaign_root`), never
reset.

## Safety

MONEY_SPENT $0.00 · CLOUD_COMPUTE_SPEND $0.00 · NEW_SUBSCRIPTIONS 0 ·
TRIALS_STARTED 0 · OPERATIONAL_WRITES 0 (`ATTRIBUTED`) · PORTFOLIO_MUTATIONS
0 · MODEL_PROMOTIONS 0 · PRODUCTION_RESTARTS 0 · SCHEDULER_CHANGES 0 ·
ORDERS_CREATED 0 · MODEL_WEIGHTS_DOWNLOADED 2 (D: only, hashed).

**Shell policy:** 0 Bash tool invocations in the session transcript; ONE
`Monitor` tool invocation ran a bash-syntax log watcher for about a minute
(the tool labels it `local_bash`; read-only, nothing produced through it).
Recorded verbatim in `contract.SHELL_POLICY_EVENTS`; the handoff validator
counts both from the transcript and blocks on a mismatch. Under the literal
"NO sh" rule this is reported as `SHELL_POLICY_VIOLATION_REPORTED = True`;
the operator decides.

## Ownership map

| concern | owner |
|---|---|
| release contract / Slot-5 rule / policies | `alpha_agent/r40/contract.py` |
| R39 closeout verification | `alpha_agent/r40/closeout_import.py` |
| burden inheritance (records via `r39.zones`) | `alpha_agent/r40/burden_ledger.py` |
| feature availability integrity | `alpha_agent/r40/availability.py` |
| prepared director session (Director2) | `alpha_agent/r40/director.py` |
| corrected WIDE successor | `alpha_agent/r40/wide_successor.py` |
| NY Fed legacy bridge | `alpha_agent/r40/nyfed_bridge.py` |
| open-weight registry / provenance / contamination / adapters | `alpha_agent/r40/open_models.py` |
| temporal / relational model challenge | `alpha_agent/r40/model_challenge.py` |
| cross-asset relational research | `alpha_agent/r40/cross_asset.py` |
| shadow registry v2 + scorer | `alpha_agent/r40/shadow_registry.py` |
| the research cycle (captures via `r39.research_shadow`) | `alpha_agent/r40/research_cycle.py` |
| always-valid designs (e-process from `r39.prospective_design`) | `alpha_agent/r40/sequential.py` |
| evidence velocity | `alpha_agent/r40/evidence_velocity.py` |
| research portfolio | `alpha_agent/r40/research_portfolio.py` |
| Intrinio readiness | `alpha_agent/r40/intrinio_readiness.py` |
| compute escalation | `alpha_agent/r40/compute_escalation.py` |
| joint evidence + verdict | `alpha_agent/r40/campaign.py` |

Reused, never rebuilt: R39 zones/lockbox/reuse ledger, R39 economic judge
(→ R34 economics), R31 hashing/immutability/multiple testing, R39
e-process, R39 capture/maturation owner, the Phase-27/28 desk ledger
primitives, the R38 layer builder. `alpha_agent/r40/{economics,
multiple_testing, judge, zones, lockbox, ledger, forward_evidence,
purchase_gate, coverage, scheduler, trade_space, universal_state}.py` are
forbidden by the audit.

## What Release 41 inherits

The binding limitation is **time**. The single highest-value next release
is the governed attachment of `run_cycle` to the Persistent Daily Research
Cycle (operator decision), so the five shadows accrue contiguous TRUE_FORWARD
evidence from 2026-08-31 without a human remembering month-ends — plus the
one weakly-justified GPU experiment (sequence-model scale) if the operator
chooses to spend ~$10. No $0 local branch remains unexecuted; the corrected
successor, the NY Fed bridge, the clean-prior foundation model, three
sequence architectures, the graph lane and the cross-asset screen have all
been run and reported on their merits.
