# Release 76 — collecting data and deciding are different acts

## The defect

`alpha_agent/alpha_recovery/next_open_runtime.py` states its own contract in the
first lines of its docstring:

```
probe the vendor  ->  append the session  ->  freeze ONE decision
```

The code did not honour it. The append sat **below** the `ALREADY_FROZEN` and
`MISSED` early returns, so the local OPRA panel was advanced only on a cycle
whose entry boundary was still live.

On a weekday that is almost never true:

| instant (ET, entry session `t+1`) | what the producer did |
|---|---|
| ~09:27–09:29 | the vendor serves information session `t` |
| 09:30 | the decision window shuts — `entry_state` becomes `MISSED` |
| ~10:30 onward | every remaining cycle returns `ADV_MISSED` **before reaching the append** |

So the session the vendor had *just published* was never collected. The panel
fell a further session behind, and the next boundary was then unreachable for a
reason that had nothing to do with the vendor. That is how **one miss became
nine** (`REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1`, 2026-09-15 → 2026-09-25),
and why `append_state` read `APPENDED` exactly **once in 400 retained cycles** —
on Saturday 2026-09-26, when no live boundary existed to short-circuit it,
catching up 2026-09-21 → 2026-09-25 in a single batch.

R74.2 measured this leg and deliberately did **not** repair it, because it
changes producer behaviour and is a governed decision. This release takes that
decision and makes the change.

## What changed

**One reordering, in the existing canonical owner.** `advance_daily` now runs
the append immediately after the publication probe and **before** `entry_state`
is read. Nothing else moved.

A forfeited boundary is a statement about a decision that will never be made. It
says nothing about whether the market data behind it belongs in the owned
surface. The append is append-only, idempotent, priced before it spends and
first-write-wins on a key collision, so running it on a forfeited cycle can
neither move a discovery row nor revive a missed entry.

The `source.owned` pre-check was dropped with the reordering, for two reasons:
it required the entry state the block now runs ahead of, and it was never the
authority — `append_information_session` answers the same question itself
(`ALREADY_OWNED`), before any network call and before any dollar.

### What did NOT change

The **entry contract**. A window that shut without a decision is still `MISSED`
permanently, still refuses every backfill, and still writes no decision.
`EXECUTION_BOUNDARY` is still `NEXT_ELIGIBLE_SESSION_OPEN`, `ENTRY_MARK_ET` is
still 09:30, the nine missed boundaries are preserved, and every registration
identity is untouched. Acquisition became unconditional; deciding did not.
`NEXT_CLOSE` is separately governed and was not opened.

## The allow-list, for the sixth time

`acquisition_precedes_decision_state` is journalled rather than assumed, because
the opposite ordering is **invisible from the outside**: every other field reads
exactly the same whether the append ran and found nothing or was never reached
at all. R66 (`blocked_on`/`blocked_owner`), R68 (`next_boundaries`), R72
(`missed_boundaries`) and R74.2 (`publication`, `entry_session`,
`information_session`, `entry_state`, twice) each lost a fact to an allow-list
between the producer and the operator. The field is therefore carried through
all three:

* `alpha_agent/r52/runtime.py` — the stage payload and `_JOURNAL_STAGE_FIELDS`
* `api/forward_producer_health.py` — the heartbeat extraction **and** the
  `producer_heartbeat` projection (`append_state` was missing from both)

## Measured, live, on 2026-09-26

`advance_daily(probe=False, append=True, execute_append=False)` against the live
surface:

```
state                                NEXT_OPEN_AWAITING_DECISION_WINDOW
information_session                  2026-09-25
entry_session                        2026-09-28
acquisition_precedes_decision_state  true
paid_dollars                         0.0
detail   the data is in hand; the window opens at 2026-09-28T04:00:00+00:00
append   ALREADY_OWNED - the owned surface already holds 2026-09-25
         surface_ends 2026-09-25   surface_last_usable 2026-09-25
```

The **2026-09-28 boundary is reachable for its full window** (00:00 → 09:30 ET),
not for a 1–3 minute slice, because the information session it needs is already
in the owned panel. `producer_coverage()` agrees independently:
`input_state = INPUTS_PRESENT`, and the chronic verdict carries
*"The NEXT boundary at 2026-09-28 is REACHABLE."*

## The residual, stated rather than silently fixed

For a **consecutive-weekday** boundary the information session is published at
~09:27 ET on the entry session itself, so the satisfiable slice is still 1–3
minutes even with the panel current. Monday boundaries are structurally safe
(Friday's session is served over the weekend); Tuesday–Friday boundaries are
not.

What this release removes is the **compounding** — a missed boundary can no
longer starve the next one. What remains is the FIT between the declared
emission window and the source's publication latency, which R74.2 named
`CHRONIC_MISS_WINDOW_UNREACHABLE_BY_DECLARED_SOURCE`. Closing it needs either a
denser poll cadence inside the 09:20–09:35 ET band or a change to the declared
window. The first risks the R64 CPU livelock and the second changes the entry
contract; **both are governed decisions and neither is taken here.**

## Enforcement

`tests/test_release76_acquisition_precedes_decision.py` — nine tests. Three pin
the defect (forfeited cycle collects; already-frozen cycle collects; the
`source.owned` pre-check is gone), three pin what may not move (entry contract,
safety boundary, the `append=False` switch), and three pin the fact's survival
through every allow-list between the producer and the operator.

Regression: 546 tests over the impacted set
(`test_release62_3_3`, `62_3_4`, `62_3_5`, `release66_next_open_collection_repair`,
`release74_2_preboundary_monitoring`, `release65_maturation_eligibility`,
`fx_carry_cadence_forward_runtime`, `r72_forward_boundary_declaration`,
`release52`, `release59`, `release62_1`, `release62_2`, `release67`,
`release68`, `s25_prospective_rearm`, `s25_epoch_boundary`,
`trackb_preclose_correction`, `release53`). One pre-existing environmental
failure, unrelated to this change and unchanged by it:
`test_a_development_worktree_may_never_be_promoted_into_a_service` passes
`-RepoRoot` = the run's own root and asserts the deployed-checkout guard BLOCKS;
run from the deployed checkout itself the guard correctly permits.

`scripts/audit_architecture.py --strict` exits 0.
