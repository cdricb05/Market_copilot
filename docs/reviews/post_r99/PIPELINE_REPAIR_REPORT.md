# Post-R99 pipeline repair report

Scope: the research-pipeline defects that R99 exposed. The fix lives in the canonical owner (`alpha_agent.agents_v2.runner`), plus one new owner module, `alpha_agent.agents_v2.provenance`. Historical R99 evidence was **not** rewritten.

**State: implemented and tested in the working tree. NOT committed** (no commit authorisation). Note that `alpha_agent/agents_v2/runner.py` was already dirty before this work: R91/R92 hunks, including `effective_cost_inputs_of` itself, are uncommitted. See "Remaining risks".

---

## PF1 — futures cost budget was `COST_BUDGET_NOT_EVALUABLE` for every dated-contract candidate

**Root cause, confirmed.**

- `effective_cost_inputs_of` derives the per-side rate as `ann_rebalance_cost_drag / (2 × ann_oneway_turnover)`.
- The dated-contract book `run_futures_book` (`research/agents/campaign_r56_v2/evaluator.py:103`) reports `mean_oneway_turnover_per_period` but never `ann_oneway_turnover`, so the rate was `None`.

**A correction to R99's own proposed patch.** `PF1_PROPOSED_PATCH.md` annualised turnover by `252 / cadence`. The book annualises `ann_rebalance_cost_drag` by `ppy = 252 / horizon` (`evaluator.py:196`, the same convention as `alpha_agent/r59/native.py:396`). The two bases agree only when cadence = horizon, which is true of every R99 cell. On an overlapping book (cadence < horizon), the proposed patch would understate the per-side rate by a factor of `cadence / horizon`. A regression test pins this.

**The fix**, in `runner.effective_cost_inputs_of(stats, *, horizon_sessions=None)`:

- When `ann_oneway_turnover` is absent, annualise `mean_oneway_turnover_per_period` by `252 / horizon_sessions`. That is the same basis the book used for the cost drag, so the ratio is exactly the notional-weighted per-side rate the book paid.
- The horizon is not in the layer stats, so the caller must supply it. `run_experiment` passes the **executed** horizon. That value is now provably equal to the frozen one (see PF2 below).
- **Fails closed** when the horizon, the turnover or the rebalance drag is missing, or when turnover is zero.
- **Event book unchanged.** It reports its own `ann_oneway_turnover`, which always wins.
- No R99 experiment ID is special-cased anywhere.

**Rerun of R02 and C02** (`pf1_rerun_r02_c02.py` → `PF1_RERUN_R02_C02.json`). This used the stored lockbox stats only, so no book was re-run and no return was read anew.

| Cell | Rate before | Rate after | Budget before | Budget after | Projected drag = measured lockbox drag | Verdict |
|---|---|---|---|---|---|---|
| R02 (ZF/ZB) | None | **2.0 bp/side** | NOT_EVALUABLE | WITHIN_COST_BUDGET | 0.72028%/yr, exact to 1e-9 | **KILLED** (unchanged) |
| C02 (NG spread) | None | **5.0 bp/side** | NOT_EVALUABLE | WITHIN_COST_BUDGET | 0.75974%/yr, exact | **KILLED** (unchanged) |

Returns are unchanged, because they were not recomputed. Both verdicts stand on lockbox materiality, burden-corrected significance and stability. C02 additionally fails sign consistency.

## PF2 — horizon ambiguity (cell table h=21, executed h=1)

**What changed.** `provenance.check_horizon_cadence` is now enforced by `run_experiment` for `FUTURES_DATED_CONTRACT` and `EQUITY_TOPN` books, before the first layer is read. The executed pair is taken from `plan["book_kwargs"]`, or from the book's own defaults (`r59.HORIZON`/`CADENCE` = 21) when the plan omits it.

`run_experiment` refuses with `HORIZON_NOT_PREREGISTERED` when:

- the executed horizon or cadence differs from the frozen `horizon_sessions` / `cadence_sessions`;
- the frozen spec states no horizon;
- the frozen spec states the same key twice with different values (top level vs `parameters`).

The result summary now carries `executed_horizon_cadence`: one pair, equal to the frozen one by construction. That same pair feeds PF1, so the preregistration, the executed plan, the result artifact and the cost budget all read the same values. The skeptic brief reads the frozen spec, which is now provably the executed one.

**Historical R99 returns were not changed.** R99's executed h=1 equals its preregistered `horizon_sessions=1`. The h=21 literal is a display defect, recorded in the errata.

## PF4 — frozen-source hash drift

**Finding, corrected.**

- 19 preregistrations recorded **3** distinct `r99_cells.py` hashes: 17 × `6786052f…`, 1 × `ac32c82d…` (E33), 1 × `b8244a9f…` (R24).
- None equals the final file `f01ece66…`.
- R99's PF4 counted a fourth hash, `3ec25710…`. That is R15's `frozen_rules_sha256` for `R99_EVENT_RULES.json`, which **also** drifted: it was rewritten at 12:30 after R15's 11:44 preregistration and now hashes to `f5aaff49…`. R15's event CSV (`a104eb75…`) is unchanged.
- Whether R15's own rule entry changed is **unverifiable**: the file is untracked and its prior bytes are gone.

**The mechanism** (`alpha_agent/agents_v2/provenance.py`):

1. **Bind.** `snapshot(paths, repo_root, store_dir)` copies each file into a write-once, **content-addressed** store (`<store>/<sha256>`). It returns a manifest `{repo-relative path: sha256, manifest_sha256}` to embed as `parameters.frozen_source` in the preregistration. Because the manifest sits inside the frozen spec, it is covered by the spec hash and is immutable in research memory.
2. **Immutable.** Blobs are never overwritten. A later wave that edits a file in place changes the live bytes, but cannot change the blob the earlier preregistration names.
3. **Reconstructable.** `materialize(manifest, store_dir, dest)` rebuilds the exact preregistered source tree.
4. **Verified before measurement.** `run_experiment` calls `require_frozen_source` before any layer. A live-file mismatch, a missing or corrupt blob, or a tampered manifest refuses with `FROZEN_SOURCE_MISMATCH`.
5. **Mandatory per campaign.** A spec with `require_frozen_source: true` refuses any preregistration without a manifest (`FROZEN_SOURCE_NOT_PREREGISTERED`).
   - Campaigns that declare nothing (R99 and earlier) behave exactly as before.
   - The intended convention is that later waves add new files rather than edit frozen ones; the verification step enforces it.

## C04 — measured before its admission was withdrawn

**Why it happened.** Times are UTC, from research-memory events and file mtimes.

| Time | Event |
|---|---|
| 15:40:57 | W1 admits C04 (G7 PASS) |
| 15:45:04 | C04 preregistered (event 193160) |
| 15:45:05 | Power assessed (193161) |
| 15:46:12 | D layer revealed (193186) |
| 15:46:34 | W1A withdraws C04 (G1 FAIL / G7 FAIL), applying the one-expression-per-family principle consistently once C01/C02 were reopened |

The wave-1 batch was dispatched on W1 while the W1A amendment was being written. The runner had no admission door, and per-cell admissions live only in campaign ruling files: the canonical `director_rulings` table holds mechanism-level closures only, and none for R99.

**The guard.**

- **Declaration.** A campaign spec may declare `admission_rulings`, an **ordered** list of ruling files. Declaration order is the authority on precedence, never file mtimes.
- **Door.** Immediately before the first layer, `run_experiment` calls `require_admission`. The latest ruling naming the cell must be `G7 = PASS`. If no ruling names the cell, or a declared ledger is empty, it refuses with `ADMISSION_NOT_G7_PASS`.
- **Afterwards.** `run_campaign` reports `governance_exceptions`: any measured cell whose current latest ruling is not PASS is flagged `GOVERNANCE_EXCEPTION_POST_MEASUREMENT_WITHDRAWAL`, so it can never be silently counted as clean.
- **Applied to R99** (`post_r99_errata.py`, ruling order W1 < W1A < W2 … W16): exactly **one** exception among the 20 measured cells, **C04**, with history W1 PASS → W1A FAIL.

**Residual race.** A ruling written *after* the door is read, but before measurement completes, cannot be prevented by any check-then-act door. That case now surfaces as a governance exception instead of passing silently. The operational rule is: do not dispatch a wave while a director amendment for it is pending.

## X04 stale carry-forward fact

- `R99_CARRY_FORWARD.json` (13:52, authoritative) records X04's time-to-floor as **4.5 years** at the observed non-zero share (0.531 × 8 meetings/yr; (36 − 17) / 4.25 = 4.47). It marks 3.4 as `_SUPERSEDED`.
- `R99_NEXT_CAMPAIGN_DRAFT.json` (13:34, hand-written) still says "~3.4 years".

**Repair.** `scripts/build_next_campaign_draft.py` builds the next-campaign draft as a **pure projection** of one carry-forward record:

- it copies every agenda item verbatim;
- it re-derives event-accrual timelines from their inputs and fails (exit 2) if they disagree with the recorded value;
- it records the source sha256;
- `--check` scans a hand-written draft for superseded values.

Output: `POST_R99_NEXT_CAMPAIGN_DRAFT.json`. It is consistent (4.47 ≈ 4.5) and flags the stale "~3.4 years" in the R99 draft. The R99 draft itself was not edited.

## F32 documentation

The docstring of `sig_F32` says "equal-vol basket"; the frozen legs are equal-notional, `{s: 1/6}`. The frozen code is authoritative. The erratum is recorded in `R99_PROVENANCE_ERRATA.json`.

`r99_cells.py` was **deliberately not edited**: changing a file that 19 preregistrations name would add yet another hash drift. F32 was refused before measurement, so no result changes.

---

## Tests

| Suite | Result |
|---|---|
| `tests/test_post_r99_pipeline_repair.py` (new, hermetic: synthetic panels, `tmp_path`, a fake pipeline that fails on any write) | **29 passed** |
| Impacted: `test_paper_trader_alpha_agents_v2`, `test_r91_event_book`, `test_r92_event_power_and_cost_budget`, `test_r89_power_expanded_alpha`, `test_campaign_r56_v2_evaluator`, `test_release61_apparatus_calibration` | **193 passed** |
| `research/agents/campaign_r99_.../test_r99_contract_guard.py` | **19 passed** |
| `scripts/audit_architecture.py --strict` | **exit 0** |
| R84 production write guard during the test runs | 0 refused writes |

New tests cover:

- dated-contract annualisation;
- R02 at 2.0 bp to 1e-9;
- missing horizon, zero or missing turnover, and missing drag all fail closed;
- event book unchanged;
- idempotence and no mutation of the input;
- cadence ≠ 1, using the canonical book on a per-market cost vector for (1,1), (5,5), (5,1) and (21,5) — the rate equals the paid notional-weighted rate to 1e-12, and the `252/cadence` patch is wrong by exactly `cadence/horizon`;
- the cost budget turning evaluable and reproducing the lockbox drag;
- horizon refusals;
- the frozen-source round-trip, edit, missing blob and tampered manifest;
- admission precedence and withdrawal exceptions;
- runner refusals before any layer;
- the draft generator.

## Remaining risks and open items

1. **Commit hygiene.** `runner.py` mixes pre-existing uncommitted R91/R92 hunks with this repair. A clean commit needs partial staging, or landing R91/R92 first. → `DO_NOT_COMMIT` until the operator authorises it and resolves this.
2. **The frozen-source store is not yet wired into the preregistration tooling.** `scripts/alpha_agents_v2.py preregister` does not call `snapshot()`. A campaign has to embed the manifest itself until roadmap slice P0-3 lands. The store location also needs a decision; the proposal is `D:\Stock_Prediction_app_data\r59_autonomous_alpha\frozen_source`.
3. **The admission ledger is campaign-local JSON.** A canonical per-experiment admission event in R59 memory would be stronger. That requires a contract change (`agent_contracts.json`, `contracts.validate()`), so it is deferred to P0-4.
4. **Overlapping-book cost convention.** The book annualises cost drag by `252/horizon` while the R61 budget projects by `252/cadence`. Per-side rate derivation is invariant to this, but the two drag figures differ for cadence < horizon. This is not a defect claim; it needs a deliberate ruling before an overlapping futures book is measured.
5. **R99's guard keeps accruing wall-clock time** if it is re-run. The closeout evaluation at 18:48:13Z is the authoritative one.
