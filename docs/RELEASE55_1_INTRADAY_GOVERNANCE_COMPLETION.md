# Release 55.1 — Intraday Governance Completion & No-Op Semantic Clarity

**Date:** 2026-09-03 · **Branch:** `stage19-controlled-rebalance` · **Base:** `acdfb77` (R55)
**Scope:** completion + semantics. No new engine, gate, decision writer, ledger,
scheduler, cadence or event path.

---

## 1. What was wrong

R55 shipped an acceptance contract that reported **GOVERNANCE = MISSING** on a
chain that had, in fact, completed correctly. Three separate defects produced
that one misleading row.

### 1.1 The gate's silence was ambiguous

`api.event_signal_refresh` invokes the R54.1 governed-decision gate **if and only
if** the cycle produced a portfolio reassessment candidate:

```python
governance = None
if reassessment is not None:          # api/event_signal_refresh.py
    with _step(GOVERNANCE_GATE_STEP, GOVERNANCE_DELEGATE) as rec:
        governance = gov_call(...)
```

So a cycle that terminated at `NO_NEW_INFORMATION` required **no verdict at
all**. Nothing in the system said so. Four different situations reached the
operator as the same absence:

| Situation | What it means | How it looked |
|---|---|---|
| No candidate existed | Successful terminal no-op | verdict absent |
| Gate ran, did not promote | Successful, standing decision holds | verdict absent |
| Gate never invoked after a reassessment | **A real gap** | verdict absent |
| Gate invoked, recorded nothing | **A real gap** | verdict absent |

`api.active_manager_state` inferred `governance_state: NOT_REQUIRED` from "no
reassessment ran". That was the **right answer reached by the wrong module** — a
presentation layer deciding a governance question — and it had no vocabulary at
all for "the gate ran and did not promote".

### 1.2 A no-op cycle presented as an unreadable reassessment

The live lane enumerated `INFORMATION_NOT_MATERIAL`, `CURRENT_NO_CHANGE` and
`PROPOSAL_READY`, and fell through to `UNKNOWN` for everything else.
`NO_NEW_INFORMATION` and `DUPLICATE_TRIGGER_SUPPRESSED` are neither, so a
perfectly healthy cycle displayed as:

```
Latest reassessment:  Sep 3, 1:47 PM ET
Conclusion:           UNKNOWN
Reassessment artifact: —
```

A reassessment that **never ran**, presented as one whose conclusion could not be
read. The timestamp made it worse: it was the *cycle's* clock, relabelled as a
reassessment's.

### 1.3 `persisted: false` sat beside a real record id

```
record_id: drc_governed_drc_2026-09-02_15abfb01856f
persisted in the governed ledger: no
```

This was **truthful**, and Phase E proved it from the authoritative store:

```
D:\Stock_Prediction_app_data\portfolio_decisions\governed_decisions.json   MISSING
D:\Stock_Prediction_app_data\portfolio_decisions\governed_index.json       MISSING
load_governed_decision_record(alpha_paper_book_1)         -> None
load_persisted_daily_decision(2026-09-02)                 -> None
```

The governed ledger **does not exist yet** — zero rows. The reason is not a
defect:

| Event | Time |
|---|---|
| Sep-2 governed daily close ran | 2026-09-02 23:51:50 UTC |
| R54.4 (the one governed writer) committed | 2026-09-03 12:14:04 −04:00 |

The Sep-2 decision was taken **~16 hours before the writer existed**. R54.4
anticipated exactly this and kept
`project_governed_daily_cycle_decision` as a documented read-time legacy shim,
marked `persisted: False`, suppressed the moment a real row exists. The decision
is authoritative and retrievable; it simply is not a ledger row. Nothing said
that, so a correct value read as a contradiction.

**No history was rewritten and nothing was backfilled.** The first real ledger
row will be written by the next governed cycle.

---

## 2. Phase A — the live Sep-3 chain, read-only

Two cycles, compared directly from the persisted run payloads.

| | **A. `evt_32f3d95a34c68b61`** | **B. `evt_9dc69133094e9b27`** |
|---|---|---|
| State | `REASSESSED_NO_CHANGE` | `NO_NEW_INFORMATION` |
| Started | 17:29:42 UTC | 17:47:48 UTC |
| Events admitted | 29 | 0 |
| Materiality | `MATERIAL_SIGNAL_CHANGED`, required **True** | `SIGNAL_CHANGED`, required **False** |
| Reassessment reason | "13 material change(s) affecting 13 security(ies): ALTERNATIVE_IMPROVEMENT." | "No reassessment: no new information arrived since the last watermark." |
| HOC required / ran | yes / yes (step present) | no / no |
| Reassessment required / ran | yes / yes (step present) | no / no |
| Governance required | **yes** | **no** |
| Governance ran | **no — no `GOVERNED_DECISION_GATE` step** | not required |
| Governed decision produced | no | no |
| Why Sep-2 stayed authoritative | no promotion occurred | no candidate existed |

### 2.1 The root cause behind cycle A

Every R54.x field is absent from **all** of today's persisted cycles:
`governed_decision: null`, `stage_timestamps: null`, `reassessment_id: null`,
`hoc_assessment_hash: null`, and no `GOVERNED_DECISION_GATE` step.

| Fact | Value |
|---|---|
| Collection worker process started | **2026-09-01 14:12:09** |
| R54.1 gate commit `0cff378` | **2026-09-01 23:46:16** |
| Gap | worker predates the gate by **9 h 34 m** |
| Backend (port 8001) started | 2026-09-03 13:56 — current |

The long-lived `run_information_collection_service.py` worker is executing a
**pre-R54.1 snapshot of `api.event_signal_refresh` held in memory**. It cannot
call a gate its loaded code does not contain. The backend is current, so the
daily close path and every read contract are on R55 code; only the intraday
worker is stale.

This is a **runtime-staleness** condition, not a governance-logic defect. R55.1
does not fix it — a worker restart does, and that is an operator action. R55.1
makes it *provable and named* instead of an unexplained blank.

---

## 3. Phase B — the terminal intraday governance contract

`api.portfolio_decision` **remains the one governance authority**. R55.1 adds a
pure classifier inside it — not a gate, not a writer, not a ledger:

```python
def classify_intraday_governance(*, event_cycle) -> dict
```

It reads only facts the gate and the cycle owner already recorded, and issues one
terminal disposition per cycle.

| Disposition | Terminal? | Meaning |
|---|---|---|
| `NOT_REQUIRED_NO_NEW_INFORMATION` | **yes** | The cycle terminated before a candidate could exist. A successful no-op. |
| `EVALUATED_NO_PROMOTION` | **yes** | The gate evaluated a candidate and did not promote it. |
| `WITHHELD` | **yes** | The gate evaluated and withheld; exact codes travel. |
| `PROMOTED` | **yes** | The candidate became a governed decision through the one writer. |
| `INCOMPLETE` | **no** | The system cannot prove what happened. |

### 3.1 The critical distinction

> **NOT_REQUIRED is a valid explicit terminal governance disposition.**
> **MISSING means the system cannot prove what happened.**

`NOT_REQUIRED` is never converted into a fake evaluation, `NO_NEW_INFORMATION` is
never converted into `HOLD`, and **no governed portfolio-decision row is ever
written to make an acceptance row green.** The classifier declares
`writes_nothing`, `creates_no_governed_row`, `recomputes_nothing`,
`decided_here: False` on every path, and the audit bans a write, a store open, a
gate call, a clock read and a threshold from its body.

### 3.2 Fail-closed by construction

* `NOT_REQUIRED` requires **both** a no-candidate cycle state **and** an explicit
  `reassessment_ran is False`. An absent flag proves nothing → `INCOMPLETE`.
* `BLOCKED` is deliberately **not** a no-candidate state: a blocked cycle proves
  nothing about whether governance was required.
* The no-candidate state list lives once, in the cycle owner
  (`esr.NO_CANDIDATE_CYCLE_STATES`), and the gate owner reads it — the two can
  never drift into disagreeing.
* A reassessment with no verdict is `INCOMPLETE`, and names **which** of the two
  provable causes applies:

| Reason | Cause |
|---|---|
| `GATE_NOT_INVOKED_AFTER_REASSESSMENT` | the cycle never reached the gate step |
| `GATE_INVOKED_WITHOUT_RECORDED_VERDICT` | the cycle reached it and recorded nothing |

The proof is the run's **own step list**, published by the cycle owner as
`governance_gate_invoked`.

---

## 4. Phase C — the acceptance contract

`GOVERNANCE` is `PRESENT` when the owner proves a **terminal** disposition, and
`MISSING` for `INCOMPLETE`. The row exposes:

```
disposition · terminal · required · gate_evaluated · gate_invoked_by_cycle
reason · reason_detail · event_cycle_run_id · event_cycle_state
candidate_reassessment_id · promotion_decision_id · at · owner
```

`at` carries only a stamp an owner actually recorded, and is deliberately absent
for a cycle that required no verdict. Nothing is manufactured.

---

## 5. Phases D–F — the three remaining repairs

**D. No-op semantics.** New conclusion token `NO_REASSESSMENT_REQUIRED`. The
backend composes three sentences the surfaces render verbatim:

```
headline              "Latest signal refresh: no new information"
reassessment_summary  "No new portfolio reassessment was required"
governed_summary      "Standing governed decision unchanged"
```

`what_changed_since` publishes `reassessment_ran`, renames the cycle's clock to
`latest_cycle_at`, and returns `None` for `latest_reassessment_at` and
`latest_reassessment_conclusion` when no reassessment ran. The three concepts
stay distinct and separately named:

| Concept | Owner |
|---|---|
| Latest signal / event cycle | `api.event_signal_refresh` |
| Latest actual portfolio reassessment | `api.portfolio_reassessment` |
| Current authoritative governed decision | `api.portfolio_decision` |

The UI reads the flag and the sentences. The audit bans every cycle-state
comparison from the browser.

**E. Persistence.** `classify_decision_persistence` names the state
(`LEDGER_ROW` / `LEGACY_COMPATIBILITY_PROJECTION` / `ABSENT`), reports
`retrievable_through_owner` derived from the canonical owner's own read, and
asserts `backfilled: False`, `history_rewritten: False`.

**F. Stage-aware latency.** `measure_decision_latency` gained
`not_required_stages`, supplied only by the cycle owner's own
`stages_not_required`. `NOT_REQUIRED` and `MISSING` are separate lists with
per-stage and per-interval dispositions. Only an **unstamped** endpoint can be
excused; a stage that recorded a timestamp is measured on its evidence whatever
the caller claimed. No unexecuted stage is ever zero-filled.

---

## 6. Live result (read-only, backend untouched)

Composed against the real production stores with R55.1 code:

| Row | Before | After |
|---|---|---|
| GOVERNANCE | **MISSING** | **PRESENT** — `NOT_REQUIRED_NO_NEW_INFORMATION`, required `false` |
| REASSESSMENT | `latest_live_conclusion: UNKNOWN` | `NO_REASSESSMENT_REQUIRED`, `reassessment_ran: false` |
| GOVERNED_DECISION | `persisted: false` (bare) | `LEGACY_COMPATIBILITY_PROJECTION`, `retrievable_through_owner: true` |
| LATENCY | 5 endpoints MISSING, incomplete | observation→signal **3029.2 s**, 5 endpoints `NOT_REQUIRED`, complete |
| **Total** | **9 / 10** | **10 / 10** |

Fail-closed is preserved: the same code returns `GOVERNANCE = MISSING` with
reason `GATE_NOT_INVOKED_AFTER_REASSESSMENT` for cycle A above.

---

## 7. Gates

| Gate | Result |
|---|---|
| `tests/test_release55_1_intraday_governance_completion.py` | **58 passed** |
| Targeted perimeter | see report |
| `scripts/audit_architecture.py --strict` | **exit 0**, 23 new blocking invariants |
| `git diff --check` | clean |

---

## 8. Post-close acceptance (prepared, NOT executed)

After the operator runs **one** normal Portfolio Cycle once Sep-3 is eligible:

```powershell
& C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe `
    C:\Users\binis\paper_trader\scripts\r55_operator_acceptance.py
```

| # | Prove | Where |
|---|---|---|
| 1 | Sep-3 is the authoritative operational close | `OPERATIONAL_BOOK.latest_completed_close_date == 2026-09-03` |
| 2 | Owned-data readiness verified canonically | `api.daily_close.provider_covers_session` |
| 3 | DRC completed, or named an exact safe blocker | workflow `research_cycle_state` |
| 4 | Persisted HOC exists if required | `HOC.assessment_hash` + `hoc_persisted` |
| 5 | Persisted reassessment exists if required | `REASSESSMENT.reassessment_id` |
| 6 | DAILY producer delegated through `api.portfolio_decision` | `provenance == GOVERNED_DAILY_CYCLE` |
| 7 | Decision retrievable from the ONE governed ledger | `persistence_status == LEDGER_ROW` |
| 8 | Provenance is `GOVERNED_DAILY_CYCLE` | GOVERNED_DECISION row |
| 9 | HOLD or CHANGE reflects current economics | `switching_economics` populated |
| 10 | No order, fill, approval, promotion or activation | `execution_safety`, `safety` |
| 11 | Active Manager State and Today agree | same route feeds both |
| 12 | R55/R55.1 acceptance complete, or names a genuine gap | exit 0, else exit 2 |
| 13 | Repeating the same evidence is idempotent | re-run: identical identity hash, no second row |

**Do not backfill Sep-2.** A pre-R54.4 session legitimately has no ledger row.

---

## 9. Remaining genuine blocker

**The information-collection worker is running pre-R54.1 code.** Until it is
restarted it cannot invoke the governance gate, so any intraday cycle that *does*
run a reassessment will report `GOVERNANCE = MISSING` with reason
`GATE_NOT_INVOKED_AFTER_REASSESSMENT` — which is the honest answer, and now a
named one. This is an operator lifecycle action, outside this slice's authority
and outside its safety envelope.

The **daily** governed path is unaffected: it runs in the backend process, which
is current, so tonight's Sep-3 close will exercise the R54.4 writer and create
the first real governed ledger row.
