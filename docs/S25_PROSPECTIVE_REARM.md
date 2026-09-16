# S25 PROSPECTIVE RE-ARM — restoring future evidence collection without backfill

**Run:** `S25_CANONICAL_FORWARD_REARM_SEP16_V1` · 2026-09-16
**Branch:** `s25-forward-rearm-sep16-v1` · **Base:** `5226658` (live
`stage19-controlled-rebalance`)
**Status:** `READY_TO_DEPLOY` — built and hermetically proven; **not deployed**.

This is not a retest of `s25_operating_profitability`. The strategy, factor,
sign, universe, thresholds, membership, spec hash and inception are unchanged,
and no statistic about the signal is recomputed anywhere in this release. What
changes is **who hosts its mark producer**.

---

## 1. The defect, in one sentence

The Stage-26 mark producer was never broken — its host was retired underneath
it, thirteen days before the book it was supposed to advance even existed.

| | |
|---|---|
| candidate | `c9_qualityprofi_e490533606` (`s25_operating_profitability`) |
| frozen spec hash | `67f0314106f9ce56806170669719a8cc3b035cd16469e984939c11c42956245e` |
| shadow book | `sb_c9_qualityprofi_e490533606` |
| inception | 2026-08-16 · $100k · SPY · 50 bps |
| membership | 100 names — 50 LONG / 50 SHORT, dollar-neutral (Σw = 0) |
| horizon | h3m / 63 sessions |
| forward marks | **0** |
| forfeited | 2026-08-17 .. 2026-09-15 — **21 NYSE sessions** |

The only production host of `advance_shadow_books` was the
**`AlphaAgent-Collect`** scheduled task: `Disabled`, last run 2026-08-03,
2,103 missed runs. The replacement runtime — `PaperTrader-ResearchRuntime` —
contains no reference to the tournament, so the advance path was never
re-parented. Every identity fact above was verified on disk in this run.

---

## 2. The repair

The frozen composition path is preserved exactly, with one link replaced:

```
BEFORE (dead)                         AFTER (this release)
AlphaAgent-Collect          [Disabled]   PaperTrader-ResearchRuntime  [Running]
  -> run_tournament_tick                   -> r52.runtime.research_runtime_cycle
  -> run_tournament_cycle                  -> stage26_prospective_mark   (NEW, one stage)
  -> advance_shadow_books     UNCHANGED    -> advance_shadow_books        UNCHANGED
  -> ShadowBook.record_mark   UNCHANGED    -> ShadowBook.record_mark      UNCHANGED
```

`alpha_agent/stage26_forward_runtime.py` is an **adapter, not an owner**. It
decides which one session is legally collectable right now and then asks the
frozen producer to do exactly what it always did. It is the same shape as the
two adapter stages the runtime already hosts (`next_open_prospective_decision`,
`fx_carry_cadence_prospective_decision`): the owner of the rule keeps the rule,
and the runtime supplies the cadence.

`run_tournament_tick` and `run_tournament_cycle` are **not** called and not
imported — this stage advances marks and does not activate books, generate
experiments, score candidates or promote anything. `AlphaAgent-Collect` stays
disabled.

### What was NOT created

no second runtime · no second scheduler or task · no second AlphaAgent · no
second forward ledger · no second ResearchMemory · no second registry · no
second global frontier · no independent daemon · no second candidate for the
same signal · no new accrual owner.

---

## 3. The critical horizon rule — inspected, not invented

The mission required determining whether h63 maturity is defined by **(A)**
elapsed sessions since inception or **(B)** the number of collected marks, and
preserving whichever the frozen contract states.

**The frozen rule is (B).** From
`stage26_challenger_expansion.forward_evidence_contract`:

> `pending_vs_matured_distinguished`: "**forward_observations counts recorded
> marks**; a horizon with fewer marks than its length is PENDING, never scored"
>
> `matures_only_when_the_horizon_arrives`: "horizon-scoped evidence at 63
> trading days is computable only **once that many marks exist**"

and `ShadowBook.replay()` implements precisely that: `forward_observations =
len(marks)`.

**Consequence: the 21-session gap does not corrupt the clock, and prospective
resumption requires no redefinition of it.** The clock was never running — zero
marks is zero progress. h63 evidence becomes computable once 63 marks exist, and
every one of those 63 will have been collected prospectively after re-arm.

`S25_REARM_BLOCKED_BY_FROZEN_HORIZON_CONTRACT` **does not apply.** The gate was
not rewritten, relaxed or reinterpreted.

---

## 4. The finding that changed the design

`ShadowBook.record_mark` alone **does not** protect the forfeited window. Its
guard is "strictly after inception, strictly after the latest mark", and with
zero marks recorded the only bound is inception **2026-08-16** — so the frozen
producer would have happily accepted **2026-09-15**.

And at the moment this repair was written, the owned trailing panel held
sessions `2026-02-25 .. 2026-09-15`. Its newest session *was* 2026-09-15, and
**every one of the 21 post-inception sessions it could price was a forfeited
one** — all 100 names and SPY at 100 % coverage on every single one.

> Wiring the producer back up without an epoch floor would therefore have minted
> a permanently forfeited session as its very first "forward" mark. The floor is
> not belt-and-braces; it is the load-bearing guard.

### Three independent guards

1. **The prospective epoch floor** — stamped once at activation, first-write-wins.
   Derived as `max(2026-09-15, latest completed session in the owned panel)`.
   The first term means a *stale* panel at activation cannot lower the floor into
   the gap; the second means an already-closed session cannot be left collectable.
2. **An explicit refusal of the declared window** `2026-08-17..2026-09-15`,
   applied *before* a session can be chosen, so it holds even if the stored floor
   has been moved or corrupted. A test tampers with the floor and proves it.
3. **The pre-existing frozen guard** — `record_mark` refuses any date at or
   before inception or the latest mark. Untouched, and still proven to raise.

---

## 5. Prospective epoch ≠ inception

Two dates are recorded separately, around the **same** strategy identity:

| | |
|---|---|
| original strategy inception | **2026-08-16** (unchanged, never restated) |
| prospective collection epoch | the first eligible completed session **strictly after** the floor stamped at activation |

Silently pretending collection began in August would make a re-armed clock
indistinguishable from a book that had been accruing for a month. The
observation epoch is governance metadata attached to the existing identity — not
a new candidate, not a new spec hash, not a reset inception.

---

## 6. No catch-up

One invocation marks **at most one** session, and it is the **newest** completed
eligible one — never a walk forward through older unmarked sessions. Sessions
missed while the runtime was down are reported as `sessions_skipped` and are
never marked later.

This is deliberate and it has a cost. A mark computed days after its session is
a retrospective calculation wearing a prospective label, which is the very
defect this repair exists to refuse — so an outage forfeits rather than queues.
Because h63 counts marks, an outage genuinely slows maturity. That is the honest
price, and `sessions_skipped` makes it visible instead of silent.

There is no date-range generation, date arithmetic or replay loop in this
release; `advance_shadow_books` takes exactly one `evidence_date`, and a test
asserts the module hands it precisely one.

---

## 7. Fail-closed by default

Deployment and activation are separate acts:

* with **no** governance record on the store the stage reports
  `AWAITING_ACTIVATION` and writes nothing;
* `scripts/rearm_s25_prospective_collection.py --confirm
  REARM_S25_PROSPECTIVE_COLLECTION` writes the record, once, first-write-wins,
  and stamps the floor from the panel;
* only then does the runtime begin collecting.

So the repair can land, be reviewed, and start accruing on a deliberate human
act — which is why **no real mark was written in this run**.

---

## 8. Visibility — the live-branch equivalent of the frontier fix

The recovery branch's commit `68fa14c` declared the Stage-25 registry as an
owner that `alpha_agent/r59/global_frontier.py` reads. **That module does not
exist on the live branch.**

The live branch `stage19-controlled-rebalance` and the recovery branch diverged
at `89a066f`: the recovery line carries **117 commits** of the Alpha Recovery
Offensive / mechanism-frontier programme that the live branch never received.
`alpha_agent/r59/global_frontier.py`, `research/alpha_agent/MECHANISM_FRONTIER.json`
and `tests/test_alpha_agent_global_multi_asset_frontier.py` are all on that line
only. The live estate artifact
`r59_autonomous_alpha/agent/global_multi_asset_frontier.json` names
`calculation_owner = alpha_agent.r59.global_frontier` — a module the running
live checkout cannot import, so that artifact is produced from a research
worktree, not from the deployed tree.

Cherry-picking `68fa14c` here was therefore rejected: it patches a file this
branch does not have. The **bounded equivalent** delivered instead:

1. **`alpha_agent/r59/stage25_owner.py`** — the read-only seam itself, landed
   verbatim in substance. `mode=ro` URI, never creates a directory, counts marks
   and never writes one, and derives `stream_state` from the book rather than
   declaring it: an ACTIVE book with zero marks more than five days after its own
   inception reads `RETIRED` — a **blocked** forward stream, not a young one.
   Reporting a dead clock as merely pending is what let 31 days pass unnoticed.
2. **An unconditional `stage26_prospective_mark` block in the runtime health read
   model** (`runtime_health.json`) and in every run-journal body — the artifacts
   the *live* runtime actually writes each cycle. This is the enforceable local
   equivalent of the frontier invariant: the omission this repair addresses was
   possible because **no read model the runtime writes had to say anything about
   this owner**, so silence looked like health. A stalled, unauthorised or
   identity-drifted stream must now name itself, every cycle, in the artifact the
   running process produces.

When the research line lands, `68fa14c`'s declaration applies there unchanged;
the seam it depends on is now already present on both.

**Reported honestly:** this branch has no owner-reconciling global frontier, so
`unreconciled` / `owner_conflicts` cannot be recomputed here. See §11.

---

## 9. What was proven

`tests/test_s25_prospective_rearm.py` — **70 tests, all passing.** Every test
builds its own Stage-8 store in `tmp_path` and injects its own panel; none reads
a vendor, a network or the live estate.

| # | Requirement | Proof |
|---|---|---|
| 1 | original identity unchanged | the full inception snapshot is byte-identical after an advance |
| 2 | spec hash unchanged | asserted before and after; a drifted hash fails **closed** |
| 3 | inception unchanged | asserted before and after |
| 4 | membership unchanged | 100 names, 50/50, Σw = 0, list identical |
| 5 | zero marks stay zero | status / seam / replay reads write nothing |
| 6 | one future tick → exactly one mark | `len(marks) == 1`, dated `2026-09-17` |
| 7 | same-session rerun → no duplicate | 5 reruns, one mark |
| 8 | restart → no duplicate | module reloaded, state read from disk |
| 9 | Aug17–Sep15 remain absent | all 21 parametrised; a forfeited-only panel yields nothing; guard 2 holds with a tampered floor |
| 10 | no path iterates the missed dates | resolver returns ONE session; no date arithmetic in the code; `evidence_date=` appears once |
| 11 | SPY / FX / R58 paths compatible | accrual & registry owners byte-asserted; S25 absent from both; all six precedent stages still wired |
| 12 | one runtime owner | one `research_runtime_cycle`; no `Register-ScheduledTask`; no `while True` |
| 13 | AlphaAgent-Collect not enabled | never called, never imported, task untouched |
| 14 | no portfolio / order / fill / promotion | 13 safety flags `False`; the imports are absent, not merely unused |

Additional: the frozen contract still states the mark-counting rule; `replay()`
reports marks as `forward_observations`; coverage below the kernel floor blocks
rather than assuming flat; a missing benchmark axis blocks; the close provider
**never forward-fills** a missing session; a stalled stream reads `RETIRED`; the
git-resident governance record matches the module's constants.

### A test-isolation hazard closed on the way

`tests/conftest.py` gains `_hermetic_stage8_store`, matching the existing
`_hermetic_forward_evidence_stores` fixture. Without it any test driving a
research cycle would open the operator's **live** `tournament.sqlite`
read-write (`CandidateRegistry` runs its schema script on connect), and a test
writing its own governance record there would stamp the **real** epoch floor
first-write-wins — fixing the live observation epoch to a fixture's clock. Marks
are append-only and immutable; neither is undoable.

---

## 10. Safety

```
LIVE_CHECKOUT_MODIFIED   = NO   (C:\Users\binis\paper_trader never written)
REAL_S25_MARK_WRITTEN    = NO   (marks still 0; no activation record on the store)
FORWARD_BACKFILL         = NO
HISTORICAL_PNL_RECONSTRUCTED = NO   (not computed, not even privately)
PORTFOLIO_MUTATION       = NO
ORDERS / FILLS           = NO
PROMOTION                = NO
CAPITAL_ALLOCATED        = NO
RUNTIME_RESTARTED        = NO
ALPHAAGENT_COLLECT       = still Disabled
PAID_DATA_COST_USD       = 0
```

---

## 11. What this release does not settle

* **S25 is still `HUMAN_GATE` and `NOT_ELIGIBLE` for capital.** Re-arming a
  clock is not evidence. The forward floor is 0 of 24 effective independent
  observations, and at h63 with one mark per session the first maturity is a
  quarter away.
* **S25 remains ineligible for the orthogonal composite**, which is `DATA_HOLD`
  at gate 1. It fails E4 (state is `HUMAN_GATE`) and E7/E8 (no per-session
  return path exists yet). The independent blocker stands: the composite's
  frozen 2,268 common-session minimum is unreachable while the SPY skew sleeve
  (path from 2022-09-09) is in the set.
* **The owner-reconciling global frontier is not on this branch**, so
  `unreconciled` / `owner_conflicts` / global rank cannot be recomputed here.
  The last computed values are the recovery branch's, rebuilt from the live
  estate to a worktree-local path: 130 of 130 identities claimed, nothing
  unreconciled, no owner conflicts, no problems, 19 of 19 invariants YES, S25 at
  global rank 3, `HUMAN_GATE`, `NOT_ELIGIBLE`. The live shared artifact still
  reports `COMPLETE` over 123 identities **without reading the Stage-25 owner at
  all** — it was deliberately not rewritten, because the running
  `PaperTrader-ResearchRuntime` owns it.
* **The standing operational gap is wider than this book.** Every registered
  challenger on the estate holds zero matured forward observations: 7 registered
  identities, 4 predictions emitted (all 2026-09-10),
  `matured_observations_total = 0`. S25 was the oldest instance of a pattern,
  not an isolated accident.

**Next single objective:** `FIND_ONE_THIRD_INDEPENDENT_QUALIFIED_SLEEVE` — a
mechanism in a third class with a daily net excess return path from a canonical
owner and enough common history to clear 2,268 sessions.

---

## 12. Deployment (after review — not performed in this run)

```powershell
# 1. land the branch, then confirm the runtime loaded it
&  C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe `
   C:\Users\binis\paper_trader\scripts\run_research_runtime.py --mode status

# 2. inspect before authorising (read-only; writes nothing)
&  C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe `
   C:\Users\binis\paper_trader\scripts\rearm_s25_prospective_collection.py --status

# 3. the deliberate human act that starts the clock
&  C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe `
   C:\Users\binis\paper_trader\scripts\rearm_s25_prospective_collection.py `
   --confirm REARM_S25_PROSPECTIVE_COLLECTION
```

Expect `S25_REARM_ACTIVATED - <floor>`. The first mark appears at the next
research cycle whose newest completed session is strictly after that floor.
Until then `stage26_prospective_mark` reports `NOT_DUE`, which is correct.
