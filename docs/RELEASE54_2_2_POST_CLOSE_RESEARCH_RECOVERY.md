# Release 54.2.2 — Post-close research recovery + close-attribution integrity

**Base:** `36153c1` (R54.2.1, committed and deployed).
**Branch:** `stage19-controlled-rebalance`. **Not committed** — operator gate.

R54.2.1 taught the workflow owner to remember a completed session that was never
**closed**. The 2026-09-01 catch-up then ran successfully through the one normal
portfolio cycle. Within the same morning it exposed the identical defect one stage
further down the daily cycle, and a second, unrelated integrity defect in the
attribution the recovered close produced.

---

## 1. What the operator saw

After the successful Sep-1 close the payload read:

| Field | Value |
|---|---|
| `latest_completed_close_date` | `2026-09-01` |
| `operational_close_valid` | `true` |
| `session_recovery_state` | `NO_CATCH_UP_REQUIRED` |
| **`overall_state`** | **`WAITING_FOR_SESSION_CLOSE`** |
| **`current_task`** | **"Wait for the current market session to close."** |

…while the *same* payload carried:

| Field | Value |
|---|---|
| `research_cycle_required` | `true` |
| `research_inputs_current` | `false` |
| `stale_source_ids` | `momentum_monthly`, `price_score_refresh` |
| `governed_research_evidence_current` | `false` |
| `governed_manifest_run_id` | `null` |
| `opportunity_cost_artifact_class` | `LIVE_PRE_DRC_SIGNAL` |
| `latest_daily_research_run_date` | `2026-08-05` |

Today additionally rendered a red service-wide **BLOCKED** banner whose reason was a
Python dict repr: `{'code': 'RESEARCH_INPUT_STALE', 'source_id': 'momentum_monthly'}`.

---

## 2. The cause — the same gate, one stage later

`api.workflow_state._decide_overall` gate **P2** asserts *"the latest eligible
completed session is already fully processed"* and tested that with
`eligible_session_closed` — **the close alone**. A completed close is one stage of the
daily cycle, not the whole of it, so the governed research still owed for that same
completed session disappeared behind the next session's open bell. `research_current`
and `research_cycle_due_after_close` were both computed and passed into the policy;
P2 simply returned before P4/P4.5 could see them.

This is R54.2.1's defect on a different axis: P2 makes a claim about a session, and
the claim was tested against an incomplete definition of "processed".

### The second owner with the same bug

`api.daily_research_cycle._pre_run_state` returned `WAITING_FOR_SESSION_CLOSE`
whenever the market was open — for **both** the status read and the RUN path. So even
if the workflow had named the right action, `run_daily_research_cycle` would have
refused. R46.2 had already written the correct rule into that module —

> "The state describes THE ELIGIBLE SESSION's cycle, not the wall clock"

— but scoped its repair to the case where a **completed** prior run existed. The case
with *no* prior run is precisely the missed one.

**Lifting it weakens no ordering.** `facts["eligible"]` is the OWNED-DATA-CONFIRMED
completed session, and owned confirmation only advances when a close runs, so a
session that has not been closed can never be eligible, and today's still-forming
session never is either. Close precedence is structural, not clock-based.

---

## 3. The root cause of the Sep-1 research gap (one thing)

> **Superseded by Release 54.2.3 (2026-09-02).** Point 1 below was true at this release:
> nothing could advance the owned source panel, so a behind-panel was a true blocker.
> R54.2.3 gives the panel owner a bounded, point-in-time as-of refresh and makes the
> governed cycle perform it for itself, so `momentum_monthly` became
> `SAFE_RECOVERABLE_POINT_IN_TIME` and Sep-1 governed research became recoverable. The
> rest of this section — the dependency chain and the permanent TRUE_FORWARD gap — still
> stands. See `docs/RELEASE54_2_3_CONTROLLED_MONTHLY_RESEARCH_INPUT_RECOVERY.md`.

Proven from the owners themselves, read-only:

1. The owned Phase-24 survivorship-free daily panel's `last_date` is **2026-08-05**.
   `api.monthly_momentum_emitter.inspect_source_panel` therefore returns
   `MONTHLY_PANEL_BEHIND_ELIGIBLE`: Phase 24 supports no safe incremental extension,
   so the September frozen monthly momentum input cannot be emitted.
2. `api.alpha_target.run_refresh` returns `R_MONTH_BOUNDARY` when the latest completed
   date is in a new month versus the frozen input month, with
   `required_next_action = RUN_RESEARCH_MONTHLY_INPUT_EMITTER`. So
   `price_score_refresh` cannot advance past `2026-08-31`.
   The Sep-1 close confirms it: `model_recalculation.recalculation_complete = false`,
   `model_calc_date 2026-08-31`.
3. The TRUE_FORWARD capture is blocked for the same reason:
   `gap_kind = RESEARCH_MONTHLY_INPUT_REQUIRED`, `weakest_gate =
   RESEARCH_MONTHLY_INPUT`, `recovery_classification = EVIDENCE_GAP_MUST_REMAIN`.

The Daily Close owner said all of this in plain English in its own
`operator_message`. The canonical workflow owner said *"wait"*.

### Point-in-time safety is already structural

The monthly emitter **refuses a panel dated AHEAD of the eligible session**
(`MONTHLY_PANEL_FUTURE_DATED`). A run started on a later calendar day therefore
cannot see data the session did not have. The daily refresh is likewise bound by the
cycle to `completed_through = the eligible session`. Nothing in this release relaxes
either rule, and nothing manufactures a value for a session that has none.

---

## 4. What this release adds

### 4.1 One post-close obligation owner — `api.workflow_state`

`build_research_obligation(...)` is a pure projection over already-published owner
answers. Vocabulary (frozen):

| State | Meaning |
|---|---|
| `NO_RESEARCH_OBLIGATION` | governed research is current for the completed close |
| `RESEARCH_OBLIGATION_OUTSTANDING` | the close is complete; the governed cycle has not run |
| `RESEARCH_OBLIGATION_BLOCKED` | outstanding, and a named condition must be repaired first |
| `RESEARCH_OBLIGATION_EVIDENCE_GAP` | no longer reconstructable point-in-time; documented |

**Three independent clocks**, stated as three clocks:
`latest_closed_session`, `latest_governed_research_session`,
`latest_governed_decision_session` — plus `decision_rests_on_governed_research`,
because a decision that *exists* is not the same as a decision built on governed
evidence.

**What suppresses P2:** `OUTSTANDING` and `BLOCKED` (both name real work or a real
fix). `EVIDENCE_GAP` deliberately does **not** — a permanent banner about something
the operator cannot act on is not information. A missed **close** still outranks a
missed research cycle by construction.

### 4.2 One stale-input classification owner — `api.daily_research_cycle`

`classify_stale_inputs(...)` classifies every stale/due input from the refresh-owner
registry and the monthly source-panel coverage this module already reports:

| Classification | Meaning |
|---|---|
| `SAFE_RECOVERABLE_POINT_IN_TIME` | its owner rebuilds it bound to the eligible session |
| `CURRENT_REFRESH_REQUIRED` | reproducible in principle; its owner refuses until a named dependency clears |
| `SLOW_MOVING_VALID_BUT_OLDER` | legitimately older than the session; not blocking |
| `UNRECOVERABLE_HISTORICAL_GAP` | cannot be rebuilt honestly; documented, never fabricated |
| `TRUE_BLOCKER` | a named condition the operator must repair |

**Live verdict for 2026-09-01:**

| Source | Classification | Code | Why |
|---|---|---|---|
| `momentum_monthly` | `TRUE_BLOCKER` | `MONTHLY_SOURCE_PANEL_BEHIND_ELIGIBLE_SESSION` | owned panel `2026-08-05`; Phase 24 has no safe incremental extension, so a **controlled owned-panel refresh** (deliberately out of the daily cycle) is required |
| `price_score_refresh` | `CURRENT_REFRESH_REQUIRED` | `BLOCKED_BY_MONTH_BOUNDARY_DEPENDENCY` | PIT-reproducible for Sep-1, but `alpha_target` refuses the month boundary until the September frozen input exists (`depends_on: momentum_monthly`) |

"Stale" is never treated as "bad", and the frozen monthly contract is not weakened.

### 4.3 One orchestration path — unchanged

Recovery resumes through the same `POST /v1/operations/portfolio-cycle/run`.
`plan_next_step` maps the workflow owner's own primary action to a step, so a
research action can never produce a Daily Close step: **the completed close is not
repeated.** There is no research-backfill route, no force-cycle control, no operator
date field, and `research_specific_route` is `null`.

Stage-aware idempotency is the composed owners' existing contract (reuse a completed
run, refuse a different contract for the same date, resume an incomplete one from its
OK/REUSED steps) — this release only removes the gate that prevented reaching it.

### 4.4 Severity is decided by the backend

Every `blockers` row now carries `severity`, `scope`
(`OPERATIONAL` / `GOVERNED_RESEARCH` / `PORTFOLIO_DECISION`),
`blocks_portfolio_decision` and `invalidates_operational_close`, plus the cycle
owner's classification and a human `detail` sentence.
`api.operator_presentation._system_readiness` **reads** that instead of escalating
every row to a red service-wide `BLOCKED`, and `_blocker_text` renders
`CODE (source) — sentence` rather than a dict repr. A research-only condition is
`DEGRADED` with *"The operational book remains valid."*

### 4.5 Today (Phase J)

A new `#today-governed-research` region (declared owner
`data-research-owner="api.workflow_state"`, hidden unless the backend says governed
research is owed) renders three backend sentences:

```
OPERATIONAL BOOK    Sep 1, 2026 close complete — NAV $97,906.63 — VALID
GOVERNED RESEARCH   Sep 1, 2026 incomplete — 2 research inputs require resolution
TRUE_FORWARD        Sep 1, 2026 historical snapshot gap documented — not recoverable
                    — does not invalidate the close
NEXT                Resolve momentum_monthly, then resume the portfolio cycle
```

It renders **no execution control** — the one-CTA invariant still binds at exactly 1;
its link only moves focus to the single primary action.

---

## 5. Close-attribution integrity (Phase K/L/M)

### 5.1 What was wrong

The Sep-1 attribution reported, for all 25 holdings:
`prior_mark_date 2026-08-31`, `current_mark_date 2026-08-31`,
`price_return_pct 0.0`, `pnl_contribution 0.0` — with
`position_contribution_sum 0.0`, `reconciliation_residual -1206.59`,
`reconciles false`, and yet `available: true` with 25/25 coverage.

### 5.2 Root cause, proven source by source

| Question | Answer |
|---|---|
| Were Sep-1 per-name marks persisted? | **Yes** into `desk_marks.json` (all 34 tickers, written by the Sep-1 close). **No** into `forward_prediction_prices.json` — the immutable TRUE_FORWARD price ledger, max date `2026-08-31`, **zero** Sep-1 rows, because its capture was blocked. |
| Why did attribution read Aug-31 twice? | `mark_at` asks the ledger first with "greatest date ≤ as_of". The greatest ledger date ≤ Sep-1 is Aug-31, so it returned a **real price from the authoritative source for the wrong date** — and never reached the exact-date cache row its own docstring describes. A stale hit still counts as "priced", so `missing` stayed empty and the diagnostic ranking fell through to its last resort. |
| How did the close compute the Sep-1 NAV? | From the Sep-1 desk-mark cache → `forward_performance.json` → the close journal (`nav 97906.63`). |
| Are NAV and per-position marks from different sources? | **Yes** — definitively. |
| Is the NAV itself reproducible? | **Yes.** Σ qty × (cache Sep-1 − ledger Aug-31) = **−1206.59**, residual **0.00**, and the cache's Aug-31 prices are **identical** to the immutable ledger's for all 25 names (no vintage mixing materialised). |

### 5.3 The repair — a reader fix, no history rewritten

* **Date exactness decides the source.** An EXACT ledger hit always wins, so Phase
  8.1B is untouched (a re-adjusted cache price can never displace a recorded prior
  close for the same date). Only a NON-exact ledger hit yields to an exact cache row.
* **A stale leg is recorded**, not hidden: `prior_mark_is_stale` /
  `current_mark_is_stale`, `mark_source.stale_mark_tickers`, and a new
  `MARK_DATE_NOT_AVAILABLE_FOR_REQUESTED_SESSION` diagnostic ranked **first**, because
  it explains every other symptom.
* **One availability owner.** `api.forward_evidence.attribution_availability` is used
  by both attribution surfaces. A non-reconciling decomposition is `available: false`,
  `decomposition_trustworthy: false`, `status: ATTRIBUTION_UNRECONCILED`, with
  winners / losers / sectors withheld. The more specific `COVERAGE_INCOMPLETE` cause
  keeps its own status.
* **Total P&L is not withheld.** NAV, the P&L block and the recorded decision keep
  their own owner and stay visible; only the decomposition is withheld. The UI says
  `ATTRIBUTION UNAVAILABLE — NAV RECONCILIATION FAILED`.

**Replayed READ-ONLY against the real Sep-1 stores:** `ATTRIBUTION_READY`,
`available true`, residual **0.00**, `market_movement −1206.59`, 25/25 priced,
`prior_mark_date 2026-08-31` / `current_mark_date 2026-09-01`, prior leg still from
the immutable ledger. No ledger row, NAV row, close row or TRUE_FORWARD snapshot was
written.

---

## 6. What did NOT change

* No TRUE_FORWARD backfill. The Sep-1 evidence gap remains documented and may remain
  permanently — that is correct point-in-time behaviour.
* No historical rewrite of any kind: no close row, no NAV, no mark, no decision.
* No new route, no second orchestrator, no second workflow owner, no automation,
  no order/fill/broker path, no model promotion, no sleeve activation.
* The frozen monthly momentum contract is unchanged and is never approximated
  intramonth.
* `plan_blocked` in the DRC execution plan is deliberately **not** changed: the
  hermetic run path cannot read the production source panel, and making the plan
  depend on a fact only the status path can see would let the two disagree. The
  classification carries the honest fact instead, and the emitter still fails closed
  at run time with the same code.

---

## 7. Operator consequence

The 8001 backend still holds the pre-R54.2.2 runtime. After validation and deploy,
restart with the canonical script and open Today: it will report **governed research
outstanding for 2026-09-01, blocked by `momentum_monthly`**, with the owned-panel
refresh named as the action that clears it.

**One honest caveat.** The monthly emitter requires `panel_last_date == eligible
session`. If the owned-panel refresh lands on a date later than 2026-09-01, the
emitter will correctly refuse it as `MONTHLY_PANEL_FUTURE_DATED` for Sep-1, and
**Sep-1 governed research becomes an `UNRECOVERABLE_HISTORICAL_GAP`** — documented,
never fabricated — while the cycle resumes normally from the next eligible session.
Bounding the panel refresh to the session being recovered is what preserves the
option to recover it.
---

## 8. Full-regression recovery — what the whole suite proved

The one full repository regression (10,111 collected) left **six** failures, all in
suites older than this release. An A/B against the committed R54.2.1 base proved every
one of them PASSED there and failed only here, so none was a stale expectation that had
already rotted: all six were behavioural changes this release made deliberately. Each
was then judged on its merits, not on its test.

**The A/B itself had to be repaired first.** A worktree at the base commit is NOT an
isolated tree: `__editable__.paper_trader-0.1.0.pth` maps the top-level name
`paper_trader` to `C:\Users\binis\paper_trader`, and every test in this repository
imports `paper_trader.api.*`. The first base run therefore executed the WORKING tree
and reproduced the working tree's answers exactly — a false "it already failed at base"
verdict. The comparison is only valid with the worktree checked out INTO a directory
named `paper_trader` and its parent on `PYTHONPATH` (the standard `PathFinder` precedes
the appended editable finder in `sys.meta_path`), with the imported `__file__` printed
and checked before any conclusion is drawn.

### The pre-close contract, restated

Two suites (R46.2, Slice 3.1) asserted that before the cutoff, with no cycle ever
completed, the Daily Research Cycle reports `WAITING_FOR_SESSION_CLOSE`. That is the
sentence this release exists to correct: the cycle is bound to the ELIGIBLE COMPLETED
session, so "no run exists for a session that closed and whose owned data is confirmed"
is outstanding governed research, not a reason to wait. For a status read,
`WAITING_FOR_SESSION_CLOSE` now means **there is no eligible completed session to work
on** — and that case is still asserted, in both suites, so lifting the clock is not a
blanket permission. The ordinary healthy rhythm is unaffected: yesterday's completed
cycle is reflected as `COMPLETE`, and the RUN path still refuses to start a new one.

### What the queue may offer

The Slice-2 fixture used to queue `RUN_DAILY_RESEARCH_CYCLE` with
`execution_available=True` **and `safe_to_execute=False`**, while the overall state said
"wait for the session to close" and the cycle owner itself refused to run — an action
offered that could not execute. With a TRUE blocker present the unsafe action is no
longer offered at all; the research work becomes the PRIMARY action
(`RESOLVE_RESEARCH_CYCLE_BLOCKER`), and the obligation is preserved in
`research_obligation` (`RESEARCH_OBLIGATION_BLOCKED`, `outstanding_research_session`,
the named `true_blockers`). Nothing is lost by not offering it.

### The blocker is named from the payload, never from an example

The blocked-cycle explanation used to name "the frozen monthly momentum input" as an
ILLUSTRATION (`e.g. ...`), in a code path that had no blocker in hand — prose that would
have said "monthly momentum" for a blocker that was something else. `true_blockers` now
carries `display_name` alongside `source_id`, so the workflow owner names the REAL cause
the way the operator knows it ("Frozen monthly momentum input", not
`momentum_monthly`), quotes the cycle owner's own reason, and states the required
action — while generic workflow logic still hard-codes no source. With no blocker
payload the sentence names nothing it cannot prove and points at the owner that can.
