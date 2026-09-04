# Release 55.2 — Runtime release identity, stale-worker detection and decision-latency semantic hardening

**Branch** `stage19-controlled-rebalance` · **over** R55.1 `55d497b`
**Status** implemented, gated, **not committed**
**Owner introduced** `api/runtime_identity.py`
**Regression** `tests/test_release55_2_runtime_release_identity.py` (72 tests)
**Guard** `scripts/audit_architecture.py::check_release55_2_runtime_release_identity` (25 blocking invariants)

---

## 1. The failure mode, in plain English

A Python process resolves its imports **once**, when it starts, and holds that
module graph for the rest of its life. The repository can move underneath it and
nothing tells anyone.

That is exactly what happened:

| Fact | Time |
|---|---|
| Information-collection worker started | **2026-09-01 14:12:09** |
| R54.1 governance gate committed (`0cff378`) | **2026-09-01 23:46:16** |
| Gap | **9 h 34 m** |

For days afterwards the worker executed a **pre-R54.1 snapshot of
`api.event_signal_refresh`**. Every intraday cycle it persisted therefore had no
`GOVERNED_DECISION_GATE` step, no `governed_decision`, no `stage_timestamps` and
no `reassessment_id` — while its heartbeat stayed fresh, its progress stamp kept
advancing, the service reported `RUNNING` and the operator surface showed a
healthy collector. R55 spent a whole release diagnosing this as a governance
defect. It was never a governance defect. **The gate was not silent; it was
never called, because the running code did not contain it.**

The generalisation is the point of this release:

> Repository HEAD is not evidence about a running process. A heartbeat proves a
> process is **alive**; nothing in the system proved which **code** it was
> running.

---

## 2. Three concepts that were previously one

| Concept | Meaning | Dynamic? |
|---|---|---|
| **Source / repository identity** | The revision on disk *right now* | Re-read on every call — deliberately |
| **Loaded runtime identity** | The revision a process loaded *at start* | **Captured once, then frozen for that process's life** |
| **Runtime alignment** | Whether the cooperating runtimes run the same release | Derived from the two above, never from health |

The immutability of the middle row is the whole mechanism. If
`loaded_release_identity` were recomputed at read time it would silently follow
the source tree and always agree with it — reproducing the exact bug. So
`capture_loaded_identity()` memoises per process and every later call returns
the same frozen mapping, whatever the tree has done since.

Commit identity is read from **git's own files** (`HEAD` → ref → `packed-refs`,
following a worktree's `gitdir:` pointer) with **no subprocess**, so a runtime
can capture at start without depending on an external `git`. The tracked-file
dirty flag is a tolerant subprocess that degrades to *unknown* rather than to
*clean*.

---

## 3. Runtimes inventoried (Phase A)

| Runtime | Lifecycle | Identity required | Startup owner | Current live state |
|---|---|---|---|---|
| Backend / API (uvicorn, :8001) | **long-lived** | yes | `scripts/restart_paper_trader_backend.ps1` | pid 56028, started 2026-09-03 14:57:47 |
| Information-collection worker | **long-lived** | yes | `scripts/manage_information_collection.ps1` | pid 19228, instance `f4dae865…`, started 2026-09-03 14:58:17 |
| Prospective research runtime | scheduled invocation | **no** | `PaperTrader-ResearchRuntime` | Ready; last run 08:15 |
| Intraday emission | scheduled invocation | **no** | `PaperTrader-IntradayEmission` | Ready; last run 14:00 |
| AlphaAgent Telegram poller | long-lived, research-only | no (out of the operational decision path) | `AlphaAgent-Telegram` | pid 2264, started 2026-08-28 |

A scheduled task re-imports the entire application on each invocation, so it
**cannot** hold a stale release. That is `NOT_APPLICABLE` — a real answer, not a
permanent `UNKNOWN`.

Nothing pre-existing owned release identity. `alpha_agent/r31/contract.py`
has a research-side `git_head()` helper and the feed registry stamps a
`git_commit` on run rows, but neither is an operational runtime owner, and
`GET /v1/health`'s `version: "1.0.0"` is a hard-coded constant that has never
changed. So one bounded owner was created rather than a concept duplicated.

---

## 4. The alignment contract (Phase E)

```
ALIGNED           two PROVEN, equal commit identities
STALE_RUNTIME     two proven identities that DIFFER — the incident state
UNKNOWN           identity is required and cannot be proven
NOT_APPLICABLE    the runtime exits between invocations
```

Every verdict carries exactly one named reason:
`LOADED_COMMIT_MATCHES_SOURCE_COMMIT`,
`LOADED_COMMIT_DIFFERS_FROM_SOURCE_COMMIT`,
`LOADED_COMMIT_NOT_RECORDED_BY_THIS_RUNTIME`,
`SOURCE_COMMIT_COULD_NOT_BE_RESOLVED`,
`RUNTIME_EXITS_BETWEEN_INVOCATIONS`.

**Fail-closed rules**

* `ALIGNED` is unreachable from liveness. `classify_alignment` cannot even see a
  heartbeat, a pid, an activity token or a service state — the audit scans its
  body for those words and blocks on any of them.
* The composed verdict is the **worst** verdict any required runtime holds; an
  aligned backend never masks a stale worker.
* `proven` is true only when every runtime that needs an identity produced one.
* A **dirty working tree** is a caveat, not a verdict. Commit identity decides
  alignment, because a dirty tree is the normal state during implementation and
  cannot prove that a running process loaded different code. The dirtiness is
  reported beside the verdict so the operator knows how strong the proof is.

---

## 5. What a stale runtime may and may not do (Phase F)

It is a **high-severity research-runtime degradation**, not a new operator
action. Justified from the canonical workflow and safety model:

1. **The action has one owner.** `api.workflow_state._decide_overall` owns the
   priority. Manufacturing a competing top-priority action would create the
   second workflow authority R55 exists to prevent.
2. **The governed evidence did not come from the stale process.** The daily
   close, the governed reassessment and the governed decision are produced in
   the backend. A stale research worker cannot retro-invalidate them, and a
   portfolio decision must never be manufactured out of an infrastructure fact.
3. **Nothing about it is unsafe.** Execution automation is off, there is no
   broker, manual review is mandatory. A stale worker cannot cause an action; it
   can only make research less current than it looks.

So it:

* **does** degrade the live/intraday research lane, with one sentence saying
  what cannot be trusted;
* **does** appear in the operator's STALE / MISSING list as `runtime_alignment`
  with the exact canonical remediation command;
* **does not** change `operator_guidance`, the primary action, the governed
  decision, the operational book, or any allocation;
* **does not** restart anything. Recovery stays an explicit operator act.

`api.active_manager_state.RUNTIME_STALENESS_POLICY` states this in the code, and
the audit blocks if the statement or the three `False` declarations disappear.

---

## 6. Latency semantics (Phase H)

**The finding.** `observation_to_signal_seconds = 6111.7` on a
`NO_NEW_INFORMATION` cycle is *not* a processing delay. The two endpoints come
from **different cycles**:

* `signal_refresh_completed_at` is **this** cycle's stamp;
* `observation_received_at`, on the live read path, is the newest material
  observation the live block had — admitted by an **earlier** cycle, possibly
  hours before. A no-op cycle admits nothing at all, so it cannot own an
  observation.

The measurement was right; the **name** was wrong. It is an **observation age**.

Live confirmation, from three consecutive reads of the real payload during
implementation: `6765.4`, then **`-750.1`**, then `212.5`. A negative value is
impossible for a latency and settles the argument outright — it means the newest
observation arrived *after* the stamp it was measured against.

**The repair — relabel, never rewrite.** Values and keys are untouched, so no
persisted record changes. What is added is what each number *measures*:

| Provenance declared by the caller | `observation_to_signal_seconds` becomes |
|---|---|
| `ADMITTED_BY_THIS_CYCLE` | *Observation → signal refresh* — a real pipeline latency |
| `PREDATES_THIS_CYCLE` | *Age of the newest observation at this signal refresh* |
| `UNKNOWN` (undeclared) | the same age label — **fails closed** |
| any of the above, value < 0 | *Newest observation arrived AFTER this signal refresh* |

Only the caller knows where it took the stamp from, so the caller declares it:
`api.event_signal_refresh` declares `ADMITTED_BY_THIS_CYCLE` at the gate call
(the gate runs only with admitted events); `api.active_manager_state` declares
`PREDATES_THIS_CYCLE` when the cycle recorded `events_admitted == 0` and
`UNKNOWN` otherwise.

Added beside them: **`event_cycle_processing_seconds`** — the cycle owner's own
measured duration, which is the number an operator usually meant by "how long
did the engine take". On the live no-op cycle: **12.5 s**, against a 6765-second
observation age.

R55.1's stage-awareness is untouched: `NOT_REQUIRED` stays `NOT_REQUIRED`, no
stage is zero-filled, and no timestamp is manufactured by the labelling.

---

## 7. Where things live

| Concern | Owner |
|---|---|
| Source identity, loaded capture, comparison, composed contract | `api/runtime_identity.py` |
| Backend capture at process start | `api/app.py` (at application import, before `FastAPI(...)`) |
| Worker capture at process start | `scripts/run_information_collection_service.py` (before the loop) |
| Worker identity persistence | `api/information_collection.py` — `loaded_release` in the **existing** service state; written only by `register_worker_start` |
| Worker identity + alignment for PowerShell | `scripts/collection_service_control.py --action status` |
| Operator display | `scripts/manage_information_collection.ps1 -Action Status` |
| Composition + degradation policy | `api/active_manager_state.py` (`runtime_alignment`, `RUNTIME_STALENESS_POLICY`) |
| Latency semantics | `api/event_signal_refresh.py` (`LATENCY_INTERVAL_SEMANTICS`, `_AGE_RELABEL`) |
| Presentation | `api/ui/index.html` — labels and sentences rendered verbatim |

No second heartbeat store, no second status file, no second scheduler, no second
decision owner, no second acceptance framework, and no cadence change.

---

## 8. Acceptance (Phase J)

The **R55 ten-row vocabulary is unchanged** and every row's PRESENT/MISSING
meaning is unchanged. Runtime facts arrive two ways:

* as **values** on the `COLLECTION` row (`runtime_alignment`, `loaded_release`,
  `deployed_release`, `runtime_alignment_reason`) — never as its key fact, so no
  row's status can flip because of them;
* as a **separately named diagnostic contract**, `state["runtime_alignment"]`,
  which fails closed on its own terms: `verdict` is `UNKNOWN` and `proven` is
  `false` whenever a runtime that needs an identity did not record one.

`scripts/r55_operator_acceptance.py` prints it as its own section.

---

## 9. Live state at implementation time

| | |
|---|---|
| Deployed source | `55d497b50126` on `stage19-controlled-rebalance`, working tree modified |
| Backend | **ALIGNED** |
| Collection worker | **UNKNOWN** — restarted at 14:58 today, *before* this release existed, so it recorded no identity |
| Composed verdict | **UNKNOWN** (`proven: false`) |
| R55 acceptance | **10 / 10 PRESENT** — unchanged |

The `UNKNOWN` is the contract working: the running worker genuinely cannot prove
what it loaded, and the system now says so instead of assuming. It resolves at
the worker's next restart, which is an operator action and deliberately outside
this slice.

---

## 10. Post-close acceptance (Phase M — prepared, NOT executed)

After the operator runs **one** normal Portfolio Cycle for Sep-3:

```powershell
& C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe `
    C:\Users\binis\paper_trader\scripts\r55_operator_acceptance.py

& C:\Users\binis\paper_trader\scripts\manage_information_collection.ps1 `
    -RepoRoot C:\Users\binis\paper_trader -Action Status
```

| # | Must be proven | Where |
|---|---|---|
| 1 | Sep-3 is the authoritative operational close | `OPERATIONAL_BOOK` row · `operational_mark_date` |
| 2 | Owned-data readiness verified canonically | `api.daily_close.provider_covers_session` |
| 3 | DRC completes or names the exact blocker | workflow owner's blocker code |
| 4 | HOC and reassessment exist when required | `HOC` · `REASSESSMENT` rows carry ids/hashes |
| 5 | The daily decision delegates through `api.portfolio_decision` | `GOVERNED_DECISION.owner` |
| 6 | Retrievable from ONE governed ledger | `retrievable_through_owner: yes` |
| 7 | **`persistence_status = LEDGER_ROW`** for the new Sep-3 decision | first real ledger row (R54.4) |
| 8 | `provenance = GOVERNED_DAILY_CYCLE` | `GOVERNED_DECISION.provenance` |
| 9 | HOLD or CHANGE follows current economics | decision owner's own verdict |
| 10 | Re-running the same evidence appends nothing | idempotency — run twice, compare ids |
| 11 | No order, fill, approval, model promotion or sleeve activation | safety block, unchanged |
| 12 | Active Manager and Today agree | one composition, both surfaces |
| 13 | **Relevant live runtimes report aligned loaded identities** | `runtime_alignment.verdict == ALIGNED`, `proven: true` |

Row 13 requires the collection worker to have been restarted **after** this
release is deployed. Until then it truthfully reads `UNKNOWN`.

**Do not backfill Sep-2.** Its absence from the governed ledger is the
documented R54.4 legacy path (the close ran ~16 h before the writer existed) and
was settled in R55.1.
