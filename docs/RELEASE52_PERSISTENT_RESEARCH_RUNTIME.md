# Release 52 — Persistent Prospective Research Runtime

**One derived timing contract · one scheduled runtime path · one append-only
forfeiture ledger · one current promotion frontier · two new independent
challengers · zero backfills · zero promotions · zero operational writes**

Research automation only. Paper only. Manual review mandatory. Track B (the
live portfolio cycle, the daily close, approvals, execution) is a different
owner and stays manual; nothing in this release can call it. Built on R51
(`0fd3f965`).

---

## 1. The defect this release exists to close

R51 named the estate's binding constraint out loud: forward evidence accrues
only when the daily cycle runs, a skipped session forfeits its ~34 emissions
permanently (the entry rule forbids backfill by construction), and the only
scheduled task that could have run anything —
`PaperTrader-InformationCollection` — has a **LogonTrigger only** and never
runs the research advance at all. The 2026-08-25 VX decision was lost exactly
this way, and R46.6.2 could only write the refusal down after the fact.

A missed legal emission window is not an inconvenience; it is TRUE_FORWARD
evidence that will never exist. On 2026-08-31 — the **first month-end
decision** for the adopted R39/R40 continuation lanes — the whole remaining
research programme depended on somebody being at the keyboard at the right
hour of the right evening. R52 removes the "somebody".

## 2. What now runs by itself (and what still never does)

`PaperTrader-ResearchRuntime` — ONE Windows scheduled task, four daily
triggers (08:15 sweep; 17:45 primary post-data emission; 19:45 retry; 21:45
fail-open final retry), `StartWhenAvailable`, `IgnoreNew` overlap policy,
bounded restart, 2-hour execution limit — executes ONE entrypoint
(`scripts/run_research_runtime.py`), which calls ONE orchestration owner:
`alpha_agent.r52.runtime.research_runtime_cycle()`. Per invocation it:

1. derives the timing contract fresh from the canonical owners;
2. verifies every evidence chain and **fails closed** if any is broken;
3. runs the ONE tournament step (`alpha_agent.r46.advance.advance`) — lane
   captures, outcome scoring, boards, money layer, adopted continuation,
   batch emission — under the campaign lock;
4. sweeps forfeitures into the append-only ledger;
5. rebuilds operational evidence velocity;
6. refreshes the R51 promotion frontier and records packet-state
   transitions;
7. writes the ONE runtime health read model
   (`GET /v1/research/runtime-health`).

It **cannot**: call the portfolio cycle, run a daily close, create or approve
a proposal, order, fill, mutate a holding / cash / NAV, promote a model,
activate a sleeve, spend money, or backfill a forward row. The audit
(`check_release52_persistent_research_runtime`, 22 blocking invariants)
enforces every one of those absences structurally.

## 3. Timing is derived, never invented

`alpha_agent/r52/timing_contract.py` quotes its authorities instead of
becoming one: the TRUE_FORWARD ordering and entry rule from
`alpha_agent.r46.clock`, each lane's cadence from
`alpha_agent.r46.lanes.registry()`, the continuation gate from
`alpha_agent.r46.adopted_forward`, freshness from the owned market-data seam.
The one thing it adds is a **slot-quality policy** with a measured basis: a
batch emitted on day D always enters D+1 and the ledger key makes the FIRST
emission win the slot, so a weekday-morning run would freeze yesterday's
inputs into tomorrow's entry. Emission is therefore suppressed on weekdays
until the owned nightly refresh has landed (measured delivery ~17:00 ET,
R38), allowed fresh all evening, and fails OPEN to a legal stale emission at
the final retry — because a stale legal row beats a forfeited slot, and the
row records exactly what it saw. Weekends are duplicate-safe and stay
allowed (the R51 Sunday advance precedent).

The scheduler's four trigger times are consumed from the contract's derived
invocation plan; the audit fails if the two ever drift apart.

## 4. Forfeiture is first-class state

`alpha_agent/r52/forfeiture.py` — ONE research-only, append-only,
chain-hashed ledger (canonical desk primitives), idempotent on
`(lane_id, challenger_scope, decision_date)`. Every row carries the legal
window, the observed invocation time, the upstream-data and scheduler state,
`outcome_window_already_open: true` and **`backfill_refused: true`** — the
append refuses a row without it. Three sources, none of them a new calendar:
recorded continuation refusals mirrored verbatim (the 2026-08-25 VX
structural loss is now row #1), a daily-batch sweep over entry dates whose
window opened with zero rows, and a month-end sweep for lanes that got
neither a row nor a refusal. A forfeited prediction is deliberately absent
evidence, never data to reconstruct.

`alpha_agent/r52/velocity_ops.py` then splits the two bottlenecks that were
previously one blur: **SCIENTIFICALLY_SLOW** (the calendar; only time closes
it) vs **OPERATIONALLY_MISSED** (the runtime; R52 drives it to zero and
measures it per week).

## 5. Concurrency: the collision is now impossible, not unlikely

The DRC and the scheduled runtime call the SAME `advance()`; its ledgers
append via read-modify-write (the R46.5 lost-update class).
`alpha_agent/r46/runlock.py` adds a campaign-scoped create-exclusive lock
with bounded wait and bounded stale recovery (dead-PID reclaim, age cap)
inside `advance()` itself, and the runtime holds its own instance lock — a
second trigger firing mid-run is refused and reported
(`RUN_REFUSED_CONCURRENT`), never interleaved.

## 6. The parallel alpha offensive (the other half of the release)

Two economically independent challengers frozen through the same canonical
door while the runtime was being built — **zero new historical trials**
(canonical constants, no sweep, burden unchanged at 353/355):

* `r52_eqidx_xs_rel_mom_12_1` — relative 12-1 momentum WITHIN the
  equity-index futures complex (&ES &NQ &YM &RTY &EMD &NKD &FDAX &FESX),
  thirds, horizon 20, cluster `EQIDX_XS_PRICE`. A dollar-neutral rotation
  across size / tech tilt / geography that the all-futures books almost
  never express (they hold equity indices as one bloc). Aimed at the sleeve
  the R51 frontier ranks CLOSEST to an operational decision. Declared
  overlap with both futures-momentum books; the realised-correlation layer
  arbitrates.
* `r52_rates_copper_gold_lead` — sign &ZN by the one-quarter change in
  ln(&HG/&GC), horizon 20, NEW information family `CROSS_ASSET_LEAD_LAG`.
  The industrial/safe-haven metal ratio is a real-economy growth print the
  bond market historically reprices with a lag; no live rates cell consumes
  commodity prices.

Six hypotheses **declared and DECLINED** with written reasons
(`R52_DECLINED`): commodity basis-momentum (the canonical Boons-Prado
measure needs a per-session historical front/next reconstruction owner that
does not exist yet; a proxy would be a self-invented parameter), FX
carry×trend and commodity curve×trend (combinations of LIVE cells under the
R44 combination-frontier finding), VIX-regime SPX timing (dependence-cluster
clone), ML futures cross-section and crypto revival (unchanged from R51/R42).

Existing frozen challengers untouched: `retune_free` on registration, zero
prior freeze stamps moved, `r51_fx_xs_carry_cip` preserved exactly.

## 7. What happened on 2026-08-31 itself

* The runtime's first live cycle ran at 13:46Z: emission correctly
  SUPPRESSED (weekday morning, Friday data), one forfeiture mirrored (the
  VX structural loss), frontier refreshed (PROMOTION_READY = 0), chains
  intact, health published.
* The two R52 challengers were frozen and probed CAN_ACCRUE before noon —
  their first TRUE_FORWARD emissions belong to the evening triggers.
* The task was installed and validated before 14:00 ET, so the month-end
  window (data arrival → midnight ET) is protected by automation with two
  retries, whether or not anyone is at the keyboard. S4U logon was denied
  in the unelevated session; the task runs Interactive (like every existing
  PaperTrader task) and the installer records how to upgrade.

## 8. Governance

The frontier refresh can mark a sleeve PROMOTION_READY and record the
transition once; approval remains a human act — no writer for
`model_approval_state` / `capital_eligible` exists anywhere in
`alpha_agent/r52`, and the audit blocks one appearing. Manual review,
preview-first, paper-only boundaries unchanged.
