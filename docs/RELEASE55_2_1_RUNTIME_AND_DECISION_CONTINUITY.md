# Release 55.2.1 — Runtime alignment reconciliation, legacy governed-decision continuity and the Windows topology truth model

**Date:** 2026-09-03 · **Branch:** `stage19-controlled-rebalance` · **Built over:** `c26cffebe074` (R55.2)
**Kind:** correctness / reliability repair. No new engine, workflow, decision owner, ledger or scheduler.
No trading economics, allocation, cadence or safety boundary changed. Nothing was committed, pushed or
restarted while it was written, and no Portfolio Cycle, Daily Close or Daily Research Cycle was run.

---

## 0. One sentence

Three surfaces reported an absence they had not established — an unknown runtime, a missing governed
decision and a missing worker — and each is repaired at the **owner of the fact**, never at the surface
that displayed it.

| # | Symptom | Actual cause | Repaired in |
|---|---|---|---|
| 1 | Active Manager said `UNKNOWN` while the collection owner said `ALIGNED` | the canonical lifecycle view did not carry the worker's captured identity | `api/information_collection.py` |
| 2 | the Sep-2 governed decision became `ABSENT` | R46.2 repaired one of the two ways the clock erases a completed run | `api/daily_research_cycle.py` |
| 3 | `NO_LOGICAL_WORKER` for a healthy worker; `COLLECTION_SERVICE_BLOCKED` | an unreadable process snapshot was treated as an empty machine | `api/information_collection.py`, `scripts/*` |

---

## 1. Defect 1 — runtime identity authority

### What happened

R55.2 published the worker's captured release on the **full** collection payload
(`load_information_collection`). The Active Manager does not read that payload. It reads the
**canonical lifecycle view**:

```
api.active_manager_state
  -> api.decision_snapshot.section("information_collection")
     -> api.operator_presentation.owner_loaders()["information_collection"]
        -> {"service": ic.resolve_service_lifecycle(state, lock, now)}
```

`resolve_service_lifecycle` returned the lifecycle verdict and nothing else, so
`svc.get("loaded_release")` was `None`, and the composition fell — correctly, given its inputs — to
`UNKNOWN` / `LOADED_COMMIT_NOT_RECORDED_BY_THIS_RUNTIME`. The worker had recorded its identity; the
seam in between dropped it.

### The repair

The lifecycle verdict now carries the worker's **own** identity facts, verbatim, out of the state it was
already handed: `instance_id`, `started_at`, `loaded_release`, `loaded_release_owner`. These are
properties of the worker **process**, exactly like `worker_pid`, which that verdict has always carried.

### Ownership after the repair

| Business concept | Authoritative owner | Source | Read consumers |
|---|---|---|---|
| loaded release identity | `api.runtime_identity.capture_loaded_identity` (memoised per process) | captured at process start | `api.app` (backend), `scripts/run_information_collection_service.py` (worker) |
| worker's recorded loaded identity | `api.information_collection` service state | `register_worker_start` | lifecycle verdict, full collection payload, `collection_service_control` |
| source release identity | `api.runtime_identity.read_source_identity` | `.git` files, re-read per call | alignment composition only |
| runtime alignment | `api.runtime_identity.classify_alignment` / `build_runtime_alignment` | pure comparison | `api.active_manager_state`, `scripts/collection_service_control.py` |
| collection process identity | `api.information_collection.resolve_service_lifecycle` | persisted state + lock | operator presentation, decision snapshot, Active Manager |
| backend process identity | `api.app` module-level capture | import time | `api.active_manager_state` |

**There is exactly one alignment calculation.** The Active Manager delegates to it and cannot disagree
with the collection owner for identical evidence — proven by test 6, which drives both callers from the
same lifecycle output and asserts the verdicts and reasons are equal. `UNKNOWN` remains fail-closed:
a worker that recorded no identity is still never reported as current (test 7).

---

## 2. Defect 2 — legacy governed-decision continuity

### This was not an R55.2 regression

`git show --name-only c26cffebe074` touches neither `api/daily_research_cycle.py`, `api/workflow_state.py`,
`api/data_freshness.py`, `engine/market_session.py`, `api/portfolio_reassessment.py` nor
`api/reallocation_proposal.py`. Its only change to `api/portfolio_decision.py` is threading
`observation_provenance` into the latency measurement. None of that can reach the projection.

### The actual cause

`project_governed_daily_cycle_decision` is gated on the Release-29.5 fact
`research_cycle_state.governed_research_evidence_current`, which is true only when the DRC status
reports a terminal-COMPLETE manifest for the eligible session. R46.2 had already found that the clock
could erase such a run and repaired **one** branch of it:

```python
if pre == WAITING_FOR_SESSION_CLOSE and prior and prior.get("state") in _COMPLETED:
    return _reflect_completed_run(prior, facts, warnings)
```

`WAITING_FOR_OWNED_DATA` was deliberately excluded on the reasoning that it means "the inputs cannot be
trusted". That verdict word actually covers two different situations:

* **(a)** the **eligible** session's own owned data is unconfirmed. Then there is no eligible session,
  `prior` is `None` (the index is keyed on it) and nothing can be reflected. Unchanged by this release.
* **(b)** the eligible session **is** owned-data-confirmed and a **later** session has completed whose
  data has not published. That is a statement about the next cycle's runnability and says nothing about
  the governed run that finished for the eligible session.

Case (b) is exactly what happened. Proven read-only against the production stores:

```
session_status                    WAITING_FOR_OWNED_DATA
eligible                          2026-09-02      owned_data_confirmed  True
expected                          2026-09-03      consistency           CONSISTENT
index[2026-09-02]                 drc_2026-09-02_15abfb01856f  state COMPLETE
_pre_run_state                    WAITING_FOR_OWNED_DATA
reflection branch requires        WAITING_FOR_SESSION_CLOSE   -> not taken
governed_research_evidence_current  False
```

At 16:00 ET the Sep-3 session closed without publishing owned data, `_pre_run_state` moved from
`WAITING_FOR_SESSION_CLOSE` to `WAITING_FOR_OWNED_DATA`, the completed Sep-2 manifest stopped being
reflected, and the Sep-2 governed portfolio decision went dark with it. **The decision did not change
and no evidence moved; only the clock did.**

### The repair

Reflect the completed run for the eligible session under case (b) as well, and keep the next session's
market-session gate beside it in a new `pending_session_gate` block so the operator loses nothing:

```python
later_session_awaited = bool(pre == WAITING_FOR_OWNED_DATA
                             and facts["owned_data_confirmed"] and facts["eligible"])
if (prior and prior.get("state") in _COMPLETED
        and (pre == WAITING_FOR_SESSION_CLOSE or later_session_awaited)):
    return _reflect_completed_run(prior, facts, warnings,
                                  pending_session_gate=...)
```

`INCONSISTENT` keeps its precedence. The reflected run still carries `executable = False`, the RUN path
still consults `_pre_run_state` and still refuses, and the reflection writes nothing (test 52).

### No ledger backfill

The repair restores a **read** projection of evidence that already existed. Sep-2 ran before R54.4 made
the daily cycle delegate its governed write, so it legitimately has no ledger row and must not be given
one. Verified live after the repair:

```
decision                   CURRENT_NO_CHANGE
provenance                 GOVERNED_DAILY_CYCLE
eligible_market_session    2026-09-02
persistence_status         LEGACY_COMPATIBILITY_PROJECTION
is_ledger_row              False
retrievable_through_owner  True
backfilled                 False
```

A later genuine ledger row for the same book and session supersedes the projection through the existing
suppression path (test 18). With no governed evidence at all the answer is still `ABSENT` (test 19) —
the repair restores a real fact, it does not invent one.

---

## 3. Standing decision vs intraday withheld candidate

The authority ladder is unchanged; this release only makes sure a **withheld** candidate cannot be
mistaken for the absence of a decision.

```
GOVERNED PORTFOLIO DECISION      authoritative until a later governed decision supersedes it
  ^ promoted only by the gate
INTRADAY CANDIDATE (WITHHELD)    visible with its exact blockers; promotes nothing, erases nothing
LIVE INTRADAY ASSESSMENT         current signal context; never authoritative
REVIEW-ONLY PROPOSAL             a priced artifact for manual review; not a decision
```

`govern_latest_intraday_assessment` reports `standing_decision_id` beside the withheld verdict, writes
no ledger row, and leaves the standing decision byte-identical (tests 20–24).

## 4. Standing portfolio decision vs operational catch-up action

These answer **two different questions about two different sessions** and are not contradictory:

| Question | Answer | Owner |
|---|---|---|
| What should the portfolio be? | **HOLD** — `CURRENT_NO_CHANGE` for 2026-09-02 | `api.portfolio_decision` |
| What must the operator do next? | **Run the Portfolio Cycle** for the missed 2026-09-03 session | `api.workflow_state` |

Before the repair, the missing standing decision made the decision lane fall through to
`PROPOSAL_REVIEW_REQUIRED`, so Today rendered *"PORTFOLIO PROPOSAL — MANUAL REVIEW REQUIRED"* directly
above *"The current portfolio remains the best risk-adjusted use of capital. No change is proposed."*
Restoring the standing decision resolves the contradiction at its source — **no presentation code was
changed**:

```
canonical_current_decision.state     PROPOSAL_REVIEW_REQUIRED  ->  NO_CHANGE
headline    PORTFOLIO PROPOSAL — MANUAL REVIEW REQUIRED  ->  NO PORTFOLIO CHANGE REQUIRED
operator action                      unchanged: RUN_DAILY_CLOSE / RUN_PORTFOLIO_CYCLE for 2026-09-03
```

The browser derives neither authority nor alignment (tests 27–28).

---

## 5. Defect 3 — the Windows process-topology truth model

### What happened

Measured in the operator's unelevated shell on 2026-09-03:

```
Get-CimInstance Win32_Process -Filter "Name='python.exe'"   ->  6 rows
  pid  1888 / 2264   CommandLine READABLE   (this shell's own lineage)
  pid  3208 / 58768  CommandLine NULL       (backend, Task Scheduler)
  pid 61108 /  1976  CommandLine NULL       (COLLECTION WORKER, alive)
rows kept by the collector (command line matched): 0
```

`Get-WorkerProcesses` required `$p.CommandLine -and $p.CommandLine -like "*<script>*"`, so every row it
was not permitted to read was dropped **silently**. The snapshot arrived empty, and
`resolve_worker_topology` answered `NO_LOGICAL_WORKER` — *"No process on this machine is running the
collection worker"* — while pid 1976 was alive, heartbeating, holding the singleton lock and reporting
an ALIGNED loaded release.

### Why this was a safety defect, not a cosmetic one

`resolve_abandoned_lock` treats `NO_LOGICAL_WORKER` as **proof** the machine is empty and returns
`may_clear: True`. The same blindness would therefore have authorised clearing the singleton lock of a
**running** worker and permitted a second one. `Stop-Worker` had the mirror-image bug: an empty match set
made it report a successful stop it had not performed.

### The evidence hierarchy

Deterministic, documented once in `WORKER_PRESENCE_EVIDENCE_ORDER`, and resolved top-down — the first
rung that decides, decides.

| # | Rung | What it is | Can it be overridden? |
|---|---|---|---|
| 1 | `PROVEN_MULTIPLE_LINEAGES` | the snapshot resolved ≥2 workers | **never** |
| 2 | `CONFLICTING_RUNTIME_EVIDENCE` | heartbeat pid vs lock owner vs instance id disagree | never (fails closed) |
| 3 | `AUTHORITATIVE_RUNTIME_STATE` | live pid + lock naming it + same instance + fresh heartbeat *or* advancing iteration | only by rungs 1–2 |
| 4 | `OS_PROCESS_CORRELATION` | the optional command-line snapshot | corroborates rung 3; may **not** refute it when unreadable |

Verdicts: `CONFIRMED_SINGLETON` · `CONFIRMED_SINGLETON_OS_METADATA_UNAVAILABLE` · `NO_WORKER` ·
`MULTIPLE_WORKERS` · `INCONSISTENT_TOPOLOGY`.

### CIM / OS-metadata advisory semantics

* Unreadable command lines make the snapshot **not authoritative**. The topology verdict then fails
  closed to `AMBIGUOUS_WORKER_TOPOLOGY`, never `NO_LOGICAL_WORKER` — which is what keeps the abandoned-lock
  path refusing (test 32).
* Presence falls through to rung 3 and reports `CONFIRMED_SINGLETON_OS_METADATA_UNAVAILABLE` with an
  advisory naming exactly what could not be read. **This is not a degraded service**; it is a degraded
  *observation channel*, and the stronger evidence was available.
* A snapshot that **was** authoritative and found nothing, while runtime state proves a live worker, is
  `INCONSISTENT_TOPOLOGY` — reported, never resolved by preferring the comfortable answer (test 39).
* Unreadable metadata never suppresses a proven violation (test 35), and singleton safety is not
  weakened anywhere: `Recover` now refuses on anything that is not a **proven** absence.

### Restart success contract

`Restart -Execute` returns SUCCESS when the singleton is **proven** (rung 3 or better) and BLOCKED only
when it genuinely cannot be. The script gates on the owner's `singleton_proven` flag and re-derives
nothing. For the exact live-shaped condition — new instance, live pid, fresh heartbeat, lock held by the
same pid, CIM command line unavailable — the result is SUCCESS with the OS-metadata advisory
(tests 43–44).

Live read-only proof after the repair:

```
worker presence : CONFIRMED_SINGLETON_OS_METADATA_UNAVAILABLE
  Exactly one collection worker is running: pid 1976 is alive, holds the singleton lock,
  and last heartbeat 46s ago.
  decided on    : AUTHORITATIVE_RUNTIME_STATE   singleton proven: True
  advisory      : OS process-metadata correlation is unavailable - 4 process(es) were visible
                  but their command lines could not be read by this shell ...
worker topology : AMBIGUOUS_WORKER_TOPOLOGY   (optional OS process correlation)
```

---

## 6. Phase J — the HOC identity mismatch is LEGITIMATE

The latest intraday candidate was withheld with `HOC_IDENTITY_MISMATCH` + `HOC_ARTIFACT_IDENTITY_MISMATCH`.
Read-only inspection of the live artifacts:

```
live HOC assessment_hash          a162fca969c93831...
reassessment.hoc_assessment_hash  aa64bd4bd5066be9...      MISMATCH
live HOC artifact_id              hoc_2026-09-02_alpha_paper_book_1_a162fca969c9
reassessment.hoc_artifact_id      hoc_2026-09-02_alpha_paper_book_1_a162fca969c9   match
```

The reassessment (`prs_2026-09-02_..._e1b699577d84`, `REUSED_EXISTING`) claims a dependency on an
opportunity-cost assessment whose hash the named artifact does not carry. That is precisely the R54.3
condition: **a governed decision may not stand on evidence it cannot produce.** It is fail-closed
governance working as designed, not a regression — R55.2 modified none of the producing owners, and its
only change to the decision owner was latency provenance. Additionally the candidate's decision word was
`CHANGE_RECOMMENDED` while the assessment's own state is `CURRENT_NO_CHANGE`.

**Left fail-closed and documented. R54.3 exact-artifact governance is not weakened.**

---

## 7. What this release does not do

No ledger row backfilled · no historical evidence rewritten · no manifest rewritten · no TRUE_FORWARD
state fabricated · no governance backdated · no order, fill, approval, model promotion or sleeve
activation · no automatic process restart · no cadence change · execution automation remains OFF ·
broker NONE · manual review REQUIRED.

---

## 8. Live read-only acceptance — BEFORE the Sep-3 Portfolio Cycle

Run after R55.2.1 is committed and deployed and the backend has been restarted by its canonical owner.

```powershell
& C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe `
    C:\Users\binis\paper_trader\scripts\r55_operator_acceptance.py

& C:\Users\binis\paper_trader\scripts\manage_information_collection.ps1 `
    -RepoRoot C:\Users\binis\paper_trader -Action Status
```

Expect:

| # | Fact | Expected |
|---|---|---|
| 1 | collection runtime alignment (manager) | `ALIGNED` / `LOADED_COMMIT_MATCHES_SOURCE_COMMIT` |
| 2 | Active Manager `runtime_alignment.verdict` | `ALIGNED`, `proven: true` |
| 3 | worker presence | `CONFIRMED_SINGLETON` or `CONFIRMED_SINGLETON_OS_METADATA_UNAVAILABLE` |
| 4 | Sep-2 governed decision | PRESENT, `CURRENT_NO_CHANGE` |
| 5 | `provenance` | `GOVERNED_DAILY_CYCLE` |
| 6 | `persistence_status` | `LEGACY_COMPATIBILITY_PROJECTION` |
| 7 | `is_ledger_row` / `retrievable_through_owner` | `false` / `true` |
| 8 | `backfilled` | `false` |
| 9 | Today headline | `NO PORTFOLIO CHANGE REQUIRED` (no manual-review wording) |
| 10 | operator action | `RUN_PORTFOLIO_CYCLE` for the missed `2026-09-03` |
| 11 | owned data | READY for 2026-09-03 |
| 12 | R55 acceptance | 10/10, `R55_ACCEPTANCE_COMPLETE` |
| 13 | safety | execution automation OFF, broker NONE, manual review REQUIRED |

## 9. Live read-only acceptance — AFTER exactly ONE Portfolio Cycle

Same two commands. Expect:

| # | Fact | Expected |
|---|---|---|
| 1 | operational mark | `2026-09-03` |
| 2 | Sep-3 close | valid |
| 3 | governed decision | PRESENT for session `2026-09-03` |
| 4 | `persistence_status` | **`LEDGER_ROW`** (the first real row) |
| 5 | `provenance` | `GOVERNED_DAILY_CYCLE` |
| 6 | `retrievable_through_owner` | `true` |
| 7 | `legacy_daily_projection_suppressed` | `true` for that session |
| 8 | Sep-2 | still `LEGACY_COMPATIBILITY_PROJECTION`, **not** backfilled |
| 9 | runtime alignment | `ALIGNED`, `proven: true` |
| 10 | safety | no order, no fill, no broker, no automatic approval, no model promotion, no sleeve activation |

**Do not backfill Sep-2.** Its ledger absence is the documented pre-R54.4 legacy path.
