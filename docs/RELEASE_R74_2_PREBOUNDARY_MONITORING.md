# Release 74.2 — the forward book could only report losses after they happened

## The defect

R68 gave every canonical forward registration a declared producer. R72 gave the
estate a **permanent loss ledger**, and it works: on 2026-09-26 it counted
**11 decision boundaries** that passed with no frozen decision and no recorded
forfeiture.

Every state in that ledger is a post mortem. The earliest instant
`PERMANENT_MISS_NOT_RECORDED_AS_A_FORFEITURE` can be true is *after* the
opportunity is gone. So the ledger's honesty was bought at the price of being
unable to prevent a single entry in it.

The live proof is `REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1`. It missed **nine
consecutive boundaries**, 2026-09-15 → 2026-09-25. On each of those days the
producer ran, reported `DATA_BLOCKED / NEXT_OPEN_AWAITING_SOURCE_PUBLICATION`
with the exact blocker, and reported `FORFEITED / NEXT_OPEN_MISSED` about an hour
later. The second miss was fully predictable from the first. Nothing anywhere
turned that into a warning while a window was still open, and nothing
distinguished a nine-day structural failure from nine unrelated accidents.

## What was measured, from the producers' own journal

| leg | measurement | consequence |
|---|---|---|
| vendor publication | the venue serves session `t` at ~09:27–09:29 ET on `t+1` | the declared window (00:00 → 09:30 ET on the entry session) is satisfiable only in a **1–3 minute** slice |
| runtime poll cadence | in-window polls on 2026-09-25 at 00:21, 01:36 … 11:56, **12:57 UTC**; next poll **13:57 UTC** | the window shuts at 13:30 UTC, so the ~hourly poll straddles the publication instant and misses it |
| local append | `append_state = APPENDED` **once in 400 retained cycles** | the local surface the producer *scores* fell to 4 sessions behind |

The local leg has a specific cause: the append is reached only when the stage is
not already forfeited. On a weekday the boundary is forfeited before the vendor's
data is noticed, so the acquisition is skipped, and the panel only catches up on
a non-session day (2026-09-26T13:38Z, advancing 2026-09-21 → 2026-09-25 in one
batch). **Acquisition is gated behind a decision outcome**, which is reported
here and deliberately **not repaired** — it changes producer behaviour and is a
governed decision.

## What this release adds

The **prospective half** of the ledger, in the existing canonical owner
`api/forward_producer_health.py`. No new store, no new scheduler, no new
registry. Every fact it consumes was already being written.

* `input_readiness()` — are the inputs the next decision needs in hand, **per the
  producer itself**? Two legs, separated, because they came apart for four days:
  `VENDOR_HAS_NOT_SERVED_THE_SESSION` vs
  `LOCAL_PANEL_HAS_NOT_REACHED_THE_PUBLISHED_SESSION`. The local leg decides,
  because the local panel is what the signal is computed from. A producer that
  declares no input gate is `INPUT_STATE_NOT_DECLARED_BY_PRODUCER`, never
  "present" — silence is not confirmation.
* `chronic_miss()` — a run of misses with no emission since is a statement about
  the **contract**, not bad luck. It is deliberately conservative about recovery:
  an emission dated at or after the newest miss proves the producer can still
  meet a boundary, so FX carry (missed 09-15, emitted 09-22) is a **recovery**,
  not a chronic fault, and is not reported as one.
* `preboundary_readiness()` — can this registration **meet** its next boundary,
  asked while the window is still open. Eight states, each with a severity, and
  four of them actionable.
* `producer_coverage()` now carries a **pre-boundary warning ledger**, counted
  separately from the permanent-loss ledger beside it. Merging them would let
  "about to be lost" read as "already lost".

The boundary always comes from the **producer's own declared grid**. Distance is
reported in calendar days (a unit that needs no calendar), and an
eligible-session distance is added *only* for the asset classes the authoritative
exchange calendar covers — a weekday count for a futures book would be the
fabrication the registry already refuses.

### Live output on 2026-09-26

```
8 registered. Every one has a declared, executable prediction path. 11 decision
boundary(ies) on the producers' OWN grids passed with no frozen decision and no
recorded forfeiture. 1 registration(s) CANNOT MEET their next declared boundary
unless something changes first: REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1
(CHRONIC_MISS_WINDOW_UNREACHABLE_BY_DECLARED_SOURCE, boundary 2026-09-28).
```

The chronic verdict carries the next boundary's own readiness with it, so the
operator is not told "unreachable" about a boundary whose inputs are already in
hand: 2026-09-28 is reported **REACHABLE**.

## Where it runs, and why that placement is the design

The stage `forward_preboundary_monitor` runs in the ONE runtime
(`alpha_agent/r52/runtime.py`), **above the eligibility gate** and **below the
four producers**, and is in **neither** `GATED_STAGES` nor `UNGATED_STAGES`.

* Above the gate, because a boundary crosses into the warning lead as time
  passes with no input changing — which is exactly when the gate skips. A warning
  an efficiency memo can suppress is not a warning.
* Below the producers, because the verdict is computed from their heartbeat for
  this cycle.
* In neither list, because `eligibility.decide` reads an `UNGATED_STAGES` row
  reporting `SUCCESS` as "a per-session owner moved". An observer that reports
  `SUCCESS` on every healthy cycle would answer RUN for ever and **reinstate the
  R64 CPU livelock**. It owns no session and must never signal progress.

It reads the accrual projection the gated stage below refreshes, so on an
emitting cycle the emission counters are one cycle old. That is correct rather
than stale: every field consumed changes only when the accrual emits or forfeits,
and the error direction is a warning that clears one cycle late — never one that
fails to appear.

## The same allow-list defect, for the fourth and fifth time

`heartbeat()` extracted an allow-list that dropped `publication`,
`entry_session`, `information_session` and `entry_state` — journalled by the
producers since R62.3.3. So the only component that asks "are the inputs in
hand?" could not see the answer sitting on disk while nine boundaries were lost.
That is the same defect as R68 (`next_boundaries`) and R72
(`missed_boundaries`), and the module's own comments warn about it twice.

The fifth instance was caught during this release: `_JOURNAL_STAGE_FIELDS` in the
runtime is *also* an allow-list, and the monitor's warning payload would have
been dropped on the way to the journal — a warning written to nowhere. Both are
now enforced by the audit.

## Enforcement

`scripts/audit_architecture.py :: check_release68_forward_producer_health`, with
eight new entries in `BLOCKING_INVARIANTS` (the R68 check previously gated
nothing):

* `preboundary_readiness_is_declared`
* `preboundary_monitor_stage_in_the_runtime`
* `preboundary_monitor_runs_above_the_gate`
* `preboundary_monitor_runs_below_the_producers`
* `preboundary_monitor_signals_no_gate_progress`
* `preboundary_monitor_fields_dropped_by_the_journal` → must be `[]`
* `heartbeat_drops_a_readiness_input` → must be `[]`
* `preboundary_write_paths` → must be `[]`

Regressions: `tests/test_release74_2_preboundary_monitoring.py` (38 tests, all on
injected fixtures — nothing opens a live store) and
`tests/test_release65_maturation_eligibility.py::test_16b`, which fails if the
observer is ever allowed to signal gate progress.

## What this release does NOT do

It writes no prediction, records no forfeiture, changes no strategy's decision or
entry contract, and repairs no missed boundary. A correctly blocked strategy
stays blocked: the nine SPY misses remain permanent, unrecovered and
unrecoverable at any budget. Monitoring is not a remedy — it is the thing that
makes a remedy arguable on evidence.
