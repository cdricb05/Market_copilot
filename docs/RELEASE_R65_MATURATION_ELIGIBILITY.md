# Release 65 — the persistent researcher stops re-deriving an unchanged world

**Workstream A of R65_MULTI_ASSET_ALPHA_ACTIVATION. Research-runtime CPU
correction only.** No alpha claim is made in this document; the alpha findings
are reported separately in
[RELEASE_R65_NON_EQUITY_ALPHA_RECONCILIATION.md](RELEASE_R65_NON_EQUITY_ALPHA_RECONCILIATION.md).

---

## In plain English

The autonomous researcher wakes up on a timer. Every time it woke, it re-ran a
five-minute block of work that asks "has any forward evidence matured, has any
prediction come due, has anything been forfeited?" — and 96% of the time the
answer was identical to the last time it asked, because nothing had changed in
between.

R64 fixed how *often* it woke. This release fixes what it does *when* it wakes.

It now checks, in about four tenths of a second, whether any of the inputs that
block of work reads have moved. If none has, it skips the block and says so.
If anything has moved — a new trading session printed, a new prediction or
outcome was written, a store changed, a decision window opened, the last cycle
failed, or six hours have passed — it runs the block exactly as before.

Three owners are **never** skipped, whatever the check says: the next-open
options challenger, the FX carry cadence and the managed-futures trend owner.
They hold live decision windows and poll a publication state no local check can
see, and a missed window is permanent. They cost about 8 seconds for all three.

---

## What was measured, before and after

The R52 run journal (`runtime_runs.json`, 400 retained rows) is the evidence.

### Before

| measurement | value |
|---|---|
| window | 2026-09-20T21:24Z .. 2026-09-22T20:17Z (46.9 h) |
| invocations | 400 |
| mean duration | 292 s (median 286 s, p90 356 s, max 1014 s) |
| total CPU | 116,747 s = **32.4 CPU-hours** |
| median gap between end of one run and start of the next | **8 s** |
| measured duty cycle | **91–105% of one core**, hour after hour |
| invocations in which ANY substantive stage produced something | **16 / 400 (4%)** |

Per-stage outcomes over those 400 runs:

```
runtime_lock                             SUCCESS 400
timing_contract                          SUCCESS 400
chain_integrity                          SUCCESS 400
tournament_advance                       NOT_DUE 399,  SUCCESS 1
forfeiture_sweep                         SUCCESS 400
next_open_prospective_decision           DATA_BLOCKED 387, FORFEITED 13
fx_carry_cadence_prospective_decision    NOT_DUE 399,  SUCCESS 1
futures_trend_prospective_decision       NOT_DUE 400
stage26_prospective_mark                 NOT_DUE 398,  SUCCESS 2
canonical_forward_accrual                NOT_DUE 399,  SUCCESS 1
velocity_operational                     SUCCESS 400   (unconditional rebuild)
promotion_frontier                       SUCCESS 400   (unconditional rebuild)
```

R64's repair is visible in the same series: the 8-second gaps stop and the
worker's effective sleep becomes 3,600 s. What R64 could not change is that
each of those hourly wakes still costs ~292 s — about 7,000 s/day, ~8% of a
core, permanently, to re-derive an answer it already had.

### The cost split, measured

| part | measured cost | gated? |
|---|---|---|
| import (paid once by a long-lived worker) | 0.88 s | — |
| `MD.last_session` (cold / warm) | 1.71 s / 0.004 s | — |
| `TC.build(write=False)` | 0.005 s | — |
| `_chains_ok()` (already in the cycle) | 0.36 s | — |
| **complete gate input set (watermarks)** | **0.033 s** | — |
| `NOR.advance_daily` (next-open, incl. publication poll) | 3.2 s | **never** |
| `FXR.advance` (FX carry, 9 markets) | 0.5 s | **never** |
| `FTR.advance` (futures trend, 87 markets) | 4.5 s | **never** |
| everything else (tournament advance, forfeiture sweep, Stage-26 mark, canonical accrual, velocity, frontier) | ≈ 283 s | **yes** |

So the gate holds back ~283 s and always pays ~9 s. On the current hourly
cadence that is ~7,000 s/day → ~220 s/day for an idle estate, a **~97%
reduction**, with no owner that holds a live window ever skipped.

---

## The design

### The owner

`alpha_agent/r52/runtime.py :: research_runtime_cycle` remains the ONE
authoritative owner of forward-evidence maturation, exactly as
`docs/architecture/system_inventory.json` declares. It is still the only
definition of that function, still the only caller of `AD.advance`,
`NOR.advance_daily`, `FXR.advance`, `FTR.advance` and
`advance_canonical_forward_accrual`, and it still holds the one runtime lock.

### The gate

`alpha_agent/r52/eligibility.py` is new and is **not a scheduler**. It owns no
cadence, fires nothing, starts no thread, registers no task and cannot make the
runtime run. It answers one question: *has anything changed since the last
completed cycle that could make the expensive stages reach a different answer?*

Every term is read from the owner that already holds it:

| term | owner |
|---|---|
| `clock.eastern_date`, `clock.entry_session_date` | `alpha_agent.r46.clock` |
| `clock.owned_last_session` | the derived timing contract, over `alpha_agent.r46.marketdata` — **this is the "new eligible trading session" signal** |
| `clock.emission_mode` | the derived timing contract's emission policy (captures the weekend and the 21:30 ET final-retry threshold without inventing a schedule) |
| `lanes.due`, `lanes.next_dates` | `alpha_agent.r46.lanes.registry`, through the same contract |
| `chain.*` | the evidence-chain verifiers the cycle already runs before any write — row counts, so a new prediction, outcome, continuation row or forfeiture moves a term at zero extra cost |
| `store.*` | each forward owner's OWN declared store directory, asked of the module that declares it |

### The cycle order

```
 1 runtime_lock
 2 timing_contract
 3 chain_integrity                          (fail-closed, unchanged)
 4 next_open_prospective_decision           NEVER GATED
 5 fx_carry_cadence_prospective_decision    NEVER GATED
 6 futures_trend_prospective_decision       NEVER GATED
 7 maturation_eligibility                   <-- the gate, asked HERE
 8 tournament_advance            ]
 9 forfeiture_sweep              ]
10 stage26_prospective_mark      ]  gated
11 canonical_forward_accrual     ]
12 velocity_operational          ]
13 promotion_frontier            ]
   journal + health + bookmark
```

The gate is asked **after** the three per-session owners on purpose: a decision
one of them has just frozen is new evidence, and it is scored in that same
cycle rather than deferred. The R62.3.5 source-order invariant
(`lock < next_open < fx < accrual`) is preserved and still pinned by
`tests/test_release62_3_5_forward_runtime_integration.py::test_09`.

### Every path that RUNS the expensive work

| reason | when |
|---|---|
| `RUN_FORCED_BY_CALLER` | `research_runtime_cycle(force_maturation=True)` |
| `RUN_INPUT_COULD_NOT_BE_RESOLVED` | any term unresolved — an unestablished input is treated as changed |
| `RUN_A_PER_SESSION_OWNER_PROGRESSED_THIS_CYCLE` | one of the three ungated owners reported SUCCESS or FORFEITED |
| `RUN_NO_PRIOR_FINGERPRINT` | no completed cycle has bookmarked anything |
| `RUN_LAST_CYCLE_DID_NOT_COMPLETE` | the previous cycle was not `RUN_COMPLETED` — **failure recovery** |
| `RUN_MAX_SKIP_INTERVAL_ELAPSED` | 6 h since the last completed cycle, whatever the fingerprint says |
| `RUN_INPUTS_CHANGED` | a term moved; the verdict names which |

`SKIP_INPUTS_UNCHANGED` is the only path to a skip, and it requires **all** of:
every term resolved, every term unchanged, the last cycle completed, and the
ceiling not reached.

### Why six hours

`timing_contract.INVOCATION_PLAN` declares 08:15 / 17:45 / 19:45 / 21:45
Eastern; its widest gap is 10.5 h. Six hours is strictly **tighter** than the
cadence the timing contract itself declares sufficient for every prospective
lane, so a gated runtime is never sparser than an ungated scheduled one. This
is derived, not invented, and
`test_17_the_ceiling_is_tighter_than_the_contract_own_invocation_plan` pins it.

### The bookmark

`maturation_gate.json`, written into the R52 runtime root **after** the cycle's
own journal and health writes, so what it records is the state the expensive
stages brought the world *to*. The next invocation compares its pre-run reading
against it. The R52 runtime root is deliberately **excluded** from the watched
store set — every file in it is written by the cycle, so watching it would make
the runtime permanently look like news to itself. A skip increments a counter
and never moves the recorded digest.

---

## Properties preserved, and how

| property | how |
|---|---|
| immutable TRUE_FORWARD evidence | no evidence path changed; the gate only decides whether to ASK |
| point-in-time eligibility | the three window-owning per-session owners run on every invocation |
| timely evidence capture | same, plus the six-hour ceiling and the owner-progressed rule |
| idempotent processing | unchanged; every gated owner was already first-write-wins |
| failure recovery | a previous cycle that was not `RUN_COMPLETED` forces the expensive set |
| missing-evidence reporting | the skip is journaled by name with its reason, its terms and its counters; health carries a `maturation_gate` block on every invocation |
| champion/challenger governance | untouched; nothing promotes, approves, orders or allocates |

### The health read model may not lie

A gated invocation does **not** rebuild `runtime_health.json`. Every measured
field in it (predictions emitted, outcomes scored, lanes, frontier, Stage-26
marks, canonical accrual) was produced by stages that did not run, and writing
`None` over them would turn *not re-measured* into *measured as absent*. The
prior document is carried forward unchanged; only the clock, the eligible
session, the chain integrity this cycle did verify, the next expected
invocation and the gate verdict are updated. Four new
`last_invocation_*` fields say when the runtime last looked at all, while the
existing `last_run_*` fields keep pointing at the last cycle that did the work.

### New observability

Every stage row now carries `duration_ms`, retained in the run journal, so the
claim "these stages are the expensive ones" can be checked by an operator
without instrumenting anything. `GET /v1/research/runtime-health` surfaces
`maturation_gate`, `maturation_was_gated` and `last_invocation_utc` at the top
level, so a quiet estate can be told from a gated one.

---

## What did NOT change

- No second scheduler, no second timing authority, no new scheduled task.
- No once-a-day rule. The gate is input-driven; the only clock term in it is
  the safety ceiling, and it makes the runtime do MORE work, never less.
- No flag that suppresses a stage the canonical owners say is due.
  `force_maturation` points at more work only.
- `alpha_agent/r59/runtime.py` is unchanged: it still calls
  `research_runtime_cycle` verbatim, and R64's sleep repair is untouched.
- No UI change, no order, no fill, no promotion, no approval, no allocation.

---

## Validation

- `tests/test_release65_maturation_eligibility.py` — **30 tests, all green.**
- Existing suites touching this path — **508 passed**, 1 pre-existing
  environmental failure
  (`test_a_development_worktree_may_never_be_promoted_into_a_service`, which
  asserts the research-worker manager blocks a *development* worktree; on this
  machine `C:\Users\binis\paper_trader` IS the deployed checkout, so the guard
  correctly does not fire). That test shells out to PowerShell only; `scripts/`
  is untouched by this release.
- `scripts/audit_architecture.py` — exit 0, inventory drift 0, canonical docs
  OK, and no R52 invariant regressed.
