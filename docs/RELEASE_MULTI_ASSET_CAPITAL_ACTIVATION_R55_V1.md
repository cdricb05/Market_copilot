# MULTI_ASSET_CAPITAL_ACTIVATION_R55_V1 - restore proposal integrity and make non-equity capital competition real

- **Date:** 2026-09-18 (single agent, Windows PowerShell only; built in the linked
  worktree `D:\paper_trader_multi_asset_capital_activation_r55_v1` on branch
  `multi-asset-capital-activation-r55-v1` over live `47b9129`).
- **Primary objective.** The Paper Trader must stop being operationally
  equity-only. A research-only FX / futures lane does not satisfy the canonical
  daily question; a non-equity sleeve counts only when it can legitimately enter
  the authoritative opportunity frontier, compete for capital, participate in
  cross-asset risk and enter the zero-base target after passing declared evidence
  and governance gates.
- **Safety boundaries held.** No proposal approved, no order, no fill, no
  rebalance, no evidence gate bypassed, no model promoted, no unqualified sleeve
  marked eligible, no historical forward evidence fabricated, no FX evidence
  backfilled, no paid data. Paper-only, preview-first, manual-review, automation
  off.

## Live evidence at the start of the run

| Fact | Value |
|---|---|
| Sep-17 proposal | `reap_2026-09-17_alpha_paper_book_1_3f0b29844493`, READY / PROPOSAL_READY, approvable - NOT approved |
| `frontier_eligible_non_equity_count` | 0 ; `frontier_rows_admitted = []` |
| target asset classes | `US_EQUITY` only; `non_equity_position_count_in_target = 0` |
| FX carry cadence | registered 2026-09-14 (identity `436a13011b67...`), 0 emissions, first legal entry 2026-09-22 |
| FX horizon | registration `horizon_sessions = 1`, policy `evaluation_horizon_sessions = 5` |
| S25 | first legitimate TRUE_FORWARD mark 2026-09-17 (raw 1 / valid 1) |
| runtime identity | backend loaded `20598fa`, collection worker `5226658`, source `47b9129` -> STALE_RUNTIME x2; research runtime reported NOT_APPLICABLE although it is a persistent worker |
| outcome store | 8,550 rows for 950 distinct economic observations (max multiplicity 19) |

## Workstream A - proposal assurance repaired

### A1 + A2 - ONE risk-contribution contract
`engine.holding_opportunity_cost` is the policy owner. It now declares
`RISK_CONTRIBUTION_FIELD = "risk_contribution_pct"`, `review_risk_contribution()`
(the only sanctioned reader of the field), `risk_contribution_limit(n)` (the ONE
threshold rule: `risk_contribution_excess_multiple / n_covariance_names`) and
`risk_contribution_breaches()`. The Release-47 repair kernel's
`max_name_risk_contribution` is a reused MIRROR of that rule at N = 25 (0.12), and
the proposal kernel overrides it with the owner's limit for the exact target being
repaired. `engine.reallocation_proposal` no longer reads a bare `risk_contribution`
key that no publisher wrote. `verify_feasibility` no longer lists
`RISK_CONTRIBUTION_CAP` as checked; it publishes
`re_measured_by_covariance_owner = [RISK_CONTRIBUTION_CAP]`.

### A3 - the AFTER-target gate
The risk block publishes `risk_contributions_after` / `_before`, the owner's limit
for each book's own covariance universe, and the breaches. A new complete-target
code `RISK_CONTRIBUTION_CAP_BREACH_BLOCKS_CHANGE` (agreed on both owners' literal
tuples) routes an over-cap target back through the repair kernel with the
RE-MEASURED shares, for a bounded number of rounds
(`max_risk_contribution_repair_rounds = 3`); every name a round reduced keeps its
reduced weight as a CEILING in later rounds (`weight_ceilings`), so released
capital cannot flow back to it. A target that still breaches is WITHHELD.

Two latent kernel defects surfaced by the gate and fixed: `_dilute_for_concentration`
could open a NEW name beyond `target_position_count` (the solver then verified its
own target infeasible); and successive repair rounds refilled the names an earlier
round had cut.

**Reproduced live (read-only, nothing persisted):** the repaired kernel finds the
ideal Sep-17 target breaching `RISK_CONTRIBUTION_CAP` for ALAB and SNDK (the two
unreported breaches), caps them to 3.17 % / 2.80 %, re-measures the target
(largest share 0.1156 < 0.125 limit), and produces READY / PROPOSAL_READY with a
NEW hash `30dd415ea6f3` (net improvement +0.0552, turnover 0.35, cost $86.15). The
live artifact `3f0b29844493` stays immutable and is superseded by the next governed
cycle; it was not approved.

### A4 - outcome-evidence idempotency
`engine.reassessment_outcomes` declares the ECONOMIC identity
(`active_book_id, reassessment_id, reassessment_hash, eligible_market_date, ticker,
recommendation, horizon_eligible_closes`) and `deduplicate_observations()` (first
recorded wins; conflicts reported; history preserved). `api.reassessment_outcomes`
hashes the evidence SOURCE (`price_source`, owner, horizons) instead of the store's
mutable `updated_at`, guards capture on BOTH `observation_id` and the economic key,
reads the deduplicated view for governance and annotates every historical duplicate
in the audit history. Nothing on disk was deleted or rewritten.

### A5 - operator workflow economics
`api.workflow_state.governed_proposal_economics()` is the ONE owner of the numbers a
surface renders: the proposal's complete-target switching economics when a proposal
exists (with the reassessment's pre-proposal estimate carried beside them under its
own name), else the reassessment's. The canonical decision, the reassessment lane,
the primary-action sentence and the presentation card all carry `economics_owner`,
`economics_basis` and `economics_binding`.

### A6 - runtime identity
`api.runtime_identity.RUNTIME_RESEARCH` is LONG_LIVED and identity-required (it is
the persistent R59 worker); `loaded_identity_from_worker_status()` shapes the
worker's start-time capture; `api.research_runtime.load_research_worker_identity()`
reads it from the worker's own status artifact and lease; the active manager
composes it. `/v1/ready` serves `loaded_commit` / `loaded_commit_short` /
`loaded_branch` / `loaded_captured_at` / `loaded_pid`, and the canonical restart
owner PROVES (section 5b) that the restarted backend serves the checkout's commit
- it fails the smoke on a mismatch instead of inferring alignment from a start
time.

## Workstream B - the multi-asset capital pipeline audit
`scripts/audit_multi_asset_capital_pipeline.py` maps the ten-step path and prints,
for every sleeve / lane, the thirteen required columns; the live run is recorded in
the final report. `frontier_eligible_non_equity_count = 0` because every research
sleeve is blocked at the CAPITAL-ELIGIBILITY step (the gate), not at the frontier,
the risk model or the execution model, which exist for every sleeve. The frontier
payload now carries `non_equity_admission_ledger` and
`eligible_non_equity_count_explanation`, and the proposal input contract carries
`frontier_non_equity_admission`, so the zero can never again hide behind a nominal
cross-asset feature.

## Workstream C - the FX horizon contract
`alpha_agent.alpha_recovery.fx_carry_cadence_challenger.HORIZON_CONTRACT` names every
consumer's number and its ROLE: the frozen record's `horizon_sessions = 1` is the
INFORMATION-LABEL horizon (the cell was scored on one-session returns), its
`trade_every_sessions = 5` is the HOLDING horizon; the registration copied the label
(1); the policy declares the hold (5); the accrual matures on the hold (5). A forward
observation is ONE frozen decision held five sessions, so maturity is 5 and the
evidence gate is the 5-session gate. `horizon_coherence()` judges the record, the
registration, the policy and the accrual against the contract; the producer refuses
to freeze under an INCOHERENT set and the accrual is INTEGRITY_BLOCKED when a
release's declared holding period disagrees with its evaluation horizon. Live:
CONSISTENT across five observed consumers. No immutable artifact was rewritten and
no new challenger identity was created.

## Workstream D - the first real non-equity capital candidate
Candidates evaluated (data owned; no purchase):

| Candidate | Data | PIT | Historical | Forward | Execution | Cross-asset risk | Gate | Blocker |
|---|---|---|---|---|---|---|---|---|
| FX futures carry (cadence) | READY | READY | ECONOMIC_UNDER_CONTROLS_NOT_FDR (t 4.26) | producer live, 0 obs, first entry 2026-09-22 | READY (long legs) | READY | NOT_PASSED | evidence 0/40 effective, 0/60 raw, 0/180 days; no conditional approval; unit granularity at $98k NAV |
| Futures time-series trend | READY (87 markets) | READY | HISTORICAL_CANDIDATE_VS_CASH (3.64 %/yr, t 2.64, 391 decisions; lockbox 2023+ negative) | registered by this release | READY (long legs) | READY | NOT_PASSED | 0 forward observations; no conditional approval; unit granularity at $98k NAV |
| Commodity futures trend / carry | READY | READY | R38 CMDTY_TS_TREND t 2.48 died on BH; R46 cohorts FORWARD_PENDING | R46 cohort, no gate binding | READY | READY | NOT_DECLARED | folded into the multi-asset trend sleeve (40 of 87 markets) |
| Rates futures | READY | READY | R46 cohorts negative early forward t | R46 cohort | READY | READY | NOT_DECLARED | no owned contract fits a 10 % name cap below ~$1.06M NAV |
| Non-equity ETF | READY | READY | none frozen | none | READY (cash equity path) | READY | NOT_DECLARED | would need a new research + forward pipeline from zero |

**Picked: FX futures carry** - it needs the least unsafe work (producer, policy and
registration already live; first legal decision 2026-09-22). The plumbing is now
generic: `api.capital_eligibility_gate` declares each sleeve's operational-signal
candidate, the frozen forward thresholds (mirroring `alpha_agent.r46.contract.
FORWARD_EVIDENCE_GATES` by horizon) and the required pre-declared CONDITIONAL
OPERATIONAL APPROVAL; it evaluates the gate from the accrual projection (now
carrying `forward_economics`), the registration and the approval record.
`api.investability_registry` DERIVES `MODEL_APPROVED_FOR_OPERATION` from a PASSED
gate (with the candidate's latest frozen decision, long legs only, rank-normalised
as the operational signal), so the sleeve enters the frontier, cross-asset risk and
the allocator on the next read with no separate operator action; a NOT_PASSED gate
publishes the exact remaining requirements (`CAPITAL_ELIGIBILITY_GATE_NOT_PASSED`).

`CAPITAL_ELIGIBLE = FALSE` for FX today. The exact remaining gate:
`MIN_EFFECTIVE_INDEPENDENT_OBSERVATIONS 0/40`, `MIN_RAW_MATURED_OBSERVATIONS 0/60`,
`MIN_CALENDAR_DAYS_OF_FORWARD_EVIDENCE 0/180`, `MIN_NET_EDGE_PER_DECISION`,
`MIN_T_STAT_NET_VS_CASH_CONTROL 2.5`, `CONDITIONAL_OPERATIONAL_APPROVAL_DECLARED`
(operator act: `declare_conditional_operational_approval` with the confirm token).
Structural caveat at the live NAV ($98,454): every owned FX contract exceeds the
10 % name cap (6A ~ $71k), so even a PASSED gate admits no FX row until NAV or the
cap policy changes; the frontier says so per row (`UNIT_NOTIONAL_EXCEEDS_NAME_CAP_AT_NAV`).

## Workstream E - the second non-equity forward pipeline
`alpha_agent.alpha_recovery.futures_trend_challenger` (research definition,
point-in-time validation on the frozen R41 store, frozen record, adoption row) and
`futures_trend_runtime` (per-boundary prospective decision, the FX producer's shape:
child-process store refresh, in-process stdlib/numpy scoring, ONE R64 construction,
00:00-09:30 ET window on the entry session, 21-session cadence, never backfills).
Entrypoints: `scripts/run_futures_trend_forward_cycle.py` and the registration door
`scripts/register_futures_trend_challenger.py` (composes the canonical adoption
owner; clock = `current_prospective_boundary()`). Stage
`futures_trend_prospective_decision` sits in `alpha_agent.r52.runtime` after the FX
stage and before the accrual. Historical result (never forward evidence): 87
markets across 4 classes, 391 monthly decisions 1995-2026, 3.64 %/yr net vs cash,
t 2.64, Sharpe 0.46, max DD -27 %; selection 4.36 %/yr t 2.96; lockbox 2023+
-2.0 %/yr (44 decisions). Classification `HISTORICAL_CANDIDATE_VS_CASH`, lockbox
sign DISAGREES - disclosed. No capital eligibility, no promotion.

## Workstream F - the multi-asset acceptance
`tests/test_multi_asset_capital_activation_r55_v1.py::TestMultiAssetAcceptance`
proves, hermetically (the R50 reference-data fixture, a PASSED gate injected
through the registry's seam): the frontier holds US_EQUITY + CASH + FX_FUTURES,
`eligible_non_equity_count >= 1`, `frontier_rows_admitted == ["&6E"]`, the proposal
funds the row (`asset_classes_in_target` has two risky classes, collateral > 0,
sleeve / class / currency on the row), cross-asset covariance is evaluated by class,
currency and collateral caps reshape, incumbency has no privilege beyond transition
cost, the zero-base allocator may fund a future within the cap, manual review only,
no order and no fill. An UNQUALIFIED sleeve is excluded fail-closed with the exact
gate named in the ledger.

## Tests, audit, gate
- 48 new tests; the adjacent regression set (release 47 / 50 / 51 / 52 / 54 / 55 /
  61 / 62 / S25 / FX / Stage 21 / Slice 6-7 / restart contract; 38 files,
  2,181 tests) passes after three test repairs (below).
- `scripts/audit_architecture.py --strict` exits 0 (the one pre-existing hit,
  `rearm_s25_prospective_collection.py` flagged by NAME as a collection worker, is
  excluded by an exclusion proven by content).
- `scripts/validate_multi_asset_capital_activation.py` prints `COMMIT_OK` /
  `DO_NOT_COMMIT <blocker>`.
- Sanctioned test edits: `test_slice7_reallocation_proposal._pol` relaxes the
  per-name risk gate for its perfectly (anti-)correlated toy returns;
  `test_release55_2` moves the research runtime from NOT_APPLICABLE to REQUIRED;
  `test_release62_1_1::test_15c` admits the trend registration door as an injector;
  `test_release50::test_30` accepts `CAPITAL_ELIGIBILITY_GATE_NOT_PASSED` beside
  `NO_APPROVED_OPERATIONAL_SIGNAL` for a sleeve WITH a declared candidate (the
  ownership change of Workstream D).
- Two PRE-EXISTING wall-clock failures at `47b9129` repaired in
  `test_release62_2_automatic_forward_accrual` (`test_03b`, `test_26b`): both read
  the real clock although the suite's own docstring pins `today`; they now pin the
  emission clock (`now=`) and the registrar's `today=` to the registration day.
  Reproduced failing on the untouched live tree before the repair.

## Deployment (after landing on live)
1. `git merge --ff-only` onto `stage19-controlled-rebalance`.
2. `scripts\restart_paper_trader_backend.ps1 -Force -Port 8001 -SmokePath ...`
   (now proves the loaded commit).
3. `scripts\manage_information_collection.ps1 -Action Restart -Execute` and
   `scripts\manage_research_runtime.ps1 -Action Restart -Execute`.
4. `scripts/run_futures_trend_forward_cycle.py --freeze-record --execute`,
   `scripts/register_futures_trend_challenger.py --execute`,
   `--declare-policy --execute`, `--seed-store`, `--refresh`.

## Next operational milestone
FIRST GOVERNED PORTFOLIO CYCLE WITH AT LEAST ONE LEGITIMATELY CAPITAL-ELIGIBLE
NON-EQUITY ALTERNATIVE - which requires, for FX carry: 40 effective independent
5-session decisions (~40 weeks from 2026-09-22), t >= 2.5 vs cash, the operator's
conditional approval, and a NAV (or cap policy) under which one FX contract is a
legal position.
