# Release 59 — Autonomous Alpha Engine Reunification

**Status:** research complete, uncommitted. Research-only; no operational change.

## Why this release exists

R56, R57 and R58 each did good work and each ended the same way:

```
human chooses topic -> bounded campaign -> NO_ALPHA_EVIDENCE -> stop -> human asks what next
```

That is not a research limitation. Every capability needed to *not* stop was
already in the tree and carried none of those campaigns' traffic:

| Capability | Owner that already existed | Used by R56–R58 |
|---|---|---|
| Durable, crash-safe, never-idle work queue | `alpha_agent.autonomous_research` | no |
| Governor → mandate → queue → evidence → gate | `api.research_bridge` + `engine.research_bridge` | no |
| Machine discovery (grammar, symbolic trees, targets, trade space, model registry, budget ledger) | `alpha_agent.r39` | no |
| Prospective forward-evidence runtime | `alpha_agent.r46` + `alpha_agent.r52.runtime` | partly |
| Search-burden ledger | `alpha_agent.r39.search_budget`, `alpha_agent.r46.burden` | no — R58 carried `PRIOR_SEARCH_BURDEN = 302` as a hand-copied constant |
| Persistent graveyard / reopen conditions | **did not exist** | — |

R59 therefore adds **no second queue, no second governor, no second tournament
and no second evaluation kernel**. It adds the four things that were genuinely
missing and reconnects what was already built.

## What R59 added

| Module | Owns |
|---|---|
| `alpha_agent/r59/memory.py` | ONE persistent research memory: hypothesis identity, graveyard, reopen conditions, **counted** search burden, frontier state, data opportunities, provider utilisation |
| `alpha_agent/r59/importers.py` | R39 / R46 / R56 / R57 / R58 evidence → memory, idempotently, without ever promoting a historical result to forward evidence |
| `alpha_agent/r59/frontier.py` | The multi-asset frontier, measured from owned panels |
| `alpha_agent/r59/governor.py` | Level 1: mandates, expected information value, cross-asset and cross-kind fairness, stop conditions |
| `alpha_agent/r59/handlers.py` | `r59.*` lanes on the **canonical** Stage-8 queue, plus the search denominator |
| `alpha_agent/r59/engines.py` | Level 3 engines; reconnects R39's grammar; every measurement goes through the R57 kernel |
| `alpha_agent/r59/loop.py` | The session runner that treats a failure as an input, not an exit |
| `alpha_agent/r59/form4.py` | The canonical insider-transaction reader |
| `alpha_agent/r59/opportunities.py` | The persistent data-opportunity frontier |
| `alpha_agent/r59/steele.py` | The analyst-revision sample gate, armed before the sample arrives |
| `alpha_agent/r59/providers.py` | The provider value scoreboard |
| `alpha_agent/r59/report.py` | The machine-readable state artifact (no dashboard) |

One pre-existing module changed: `alpha_agent/r57/futures_tournament.simulate`
gained keyword-only `signal_override` and `market_mask`. Both default to the
original behaviour. They exist so a machine-generated signal is measured by
**that** simulator — same NEXT_CLOSE convention, same inverse-vol sizing, same
leverage bound, same roll and trade costs — instead of by a second, subtly
different copy of it.

## The defect the first live session exposed

The first full session measured 1,240 hypotheses and reported **four
qualified** candidates. Three of them shared a lockbox t-statistic identical to
sixteen digits.

They were the same book. R39's grammar emits monotone transforms of the same
column — `abs(log_adv)`, `tanh(log_adv)`, `log_adv + log_adv`,
`rankxs(log_adv)` — and a rank-based book cannot tell them apart: identical
names, identical dates, identical statistic. Keying novelty on the *expression*
counted one hypothesis eight times.

Measured on that session: **288 of 1,240 hypotheses (23%) were duplicate
books**, across 134 distinct t-values. And the "discovery" was `log_adv` — the
liquidity factor, which R57 had already prosecuted and rejected.

Two fixes, both in this release:

1. **Identity is the realised signal.** `engines._rank_fingerprint` hashes the
   decision-by-decision ordering. Two features that select the same names on
   the same dates are ONE hypothesis, whatever their formulas look like.
2. **A re-expression inherits its family's burden.** `engines._base_equivalent`
   detects when a machine feature is a monotone function of a base column
   (|Spearman| ≥ 0.999) and attributes it to that column's economic family, so
   a machine rediscovery of the liquidity factor is charged for the liquidity
   tests already run. `handlers.search_denominator` additionally charges the
   generative search that produced the candidate — a t-statistic picked out of a
   pile of machine candidates is not worth what an isolated pre-registered test
   would be.

The first session's memory and artifacts were **superseded, not deleted**: they
are under
`D:\Stock_Prediction_app_data\r59_autonomous_alpha\_superseded_defective_identity_*`.
The results reported below come from the re-run on corrected identity.

## Architecture after reunification

```
LEVEL 1  RESEARCH GOVERNOR        what should we learn next?
         alpha_agent.r59.governor            (discovery frontier)
         api.research_agent                  (operational weakness)
         api.research_bridge                 (mandate -> queue bridge)

LEVEL 2  ALPHAAGENT               how do we investigate it?
         alpha_agent.autonomous_research     THE queue (Stage 8)
         alpha_agent.r59.handlers            r59.* lane routing
         alpha_agent.r59.memory              identity, graveyard, burden
         alpha_agent.r59.loop                the session that does not idle

LEVEL 3  ENGINES                  do the work
         alpha_agent.r39.representation_factory   machine grammar
         alpha_agent.r59.engines                  economic / cross-asset
         alpha_agent.r59.form4                    information change
         alpha_agent.r59.opportunities            data opportunity
         alpha_agent.r57.engine                   THE statistical kernel
         alpha_agent.r57.futures_tournament       THE futures simulator
```

Forward evidence stays with `alpha_agent.r46` and `alpha_agent.r52.runtime`.
R59 references inception instants and record hashes only; it never writes,
backfills or rescores a forward challenger.

## Stop conditions

The session ends only on the four conditions in the brief. "The batch drained"
and "a survivor was frozen" are explicitly not among them: after every batch the
runner asks the governor for more work, and only the governor's refusal can stop
it.

| Code | Meaning |
|---|---|
| `A_NO_INDEPENDENT_EXECUTABLE_RESEARCH_REMAINS` | the governor cannot generate a valid mandate without new information |
| `B_EXTERNAL_BLOCKER` | mandates exist but none can be enqueued or claimed |
| `C_SAFETY_CONSTRAINT_REQUIRES_HUMAN` | reserved; not reached |
| `D_ENVIRONMENT_LIMIT` | iteration or wall-clock limit — a **pause**, with work still ready |

## The Form-4 finding

The brief expected a missing parser. Measurement says otherwise, and the
distinction changes the fix.

* `collectors/sec_edgar.parse_form4_xml` already extracts `transaction_code` and
  `acquired_disposed` correctly — but `_collect_form4_transactions` caps XML
  fetches at `form4_xml_cap` (default **8**) per run, so the canonical
  normalized store fills with index rows carrying no direction. That is why R58
  measured direction on 195 of 28,002 records (0.7%) and its insider probe
  scored **0 tickers**.
* `alpha_agent.r46.form4.parse_submission_text` already produces the **complete**
  parse and R46 has been running it daily. Measured: **28,780 transactions,
  100% direction, 2,921 tickers, 15,321 open-market P/S transactions (2,408
  buys / 12,913 sells) across 2,072 tickers**, over 27 business days.

The estate did not need a parser. It needed a **reader** — one canonical way to
turn the owned parse into records any engine can consume, with the SEC
acceptance instant as the point-in-time key. That is `alpha_agent/r59/form4.py`.

The honest limit stands: 27 business days cannot fill a
discovery/validation/lockbox partition. This information can support a
**forward challenger** and cannot support a historical backtest at any coverage
level.

## The second defect: a READY scope with no members

The first corrected session covered six asset classes and left CROSS_ASSET at
zero. The queue said why: every cross-asset job blocked with `NO_MEMBERS`.

`futures_members()` resolved a scope through the Norgate classification map,
which only ever produces per-class buckets. CROSS_ASSET is not a bucket — it is
a cross-section taken *across* the classes — so the frontier reported it READY
with 103 instruments while the engine received an empty list. A scope can be
declared ready and be unrunnable, and only the queue's blocked-reason showed it.

Fixed, tested (`test_cross_asset_scope_spans_the_whole_panel` asserts the scope
is exactly the union of the named scopes) and re-run.

## The session that actually ran

Two invocations, the second resuming the first from the same queue and memory:

```
55 iterations   420 jobs executed   1,314 hypotheses measured
0 handler errors
stop: D_ENVIRONMENT_LIMIT (iteration limit) with 19 mandates still READY
```

| Asset class | Hypotheses measured |
|---|---|
| COMMODITY_FUTURES | 404 |
| EQUITY_INDEX_FUTURES | 293 |
| US_EQUITY | 273 |
| RATES_FUTURES | 135 |
| FX_FUTURES | 105 |
| CROSS_ASSET | 101 |
| VOLATILITY | 3 |

| Generation method | Count |
|---|---|
| `AUTO_TRANSFORM_GRAMMAR` (R39) | 801 |
| `SYMBOLIC_TREE_SEARCH` (R39) | 498 |
| `GOVERNOR_ECONOMIC_MANDATE` | 15 |

Of **1,628 machine candidates generated, 329 were rejected as duplicate books**
before measurement — the identity fix working in the loop, not just in a test.

**Qualified: 0.** The strongest nominal result was a symbolic commodity
expression at lockbox t = 3.199; burden-corrected p = 0.224, so it fails. That
is the correct answer: it was the best of 1,105 machine candidates, and a
statistic chosen from that pile is charged for the pile.

| Strongest | t | Why it failed |
|---|---|---|
| COMMODITY_FUTURES symbolic | 3.199 | burden-corrected significance |
| EQUITY_INDEX_FUTURES auto | 3.155 | validation sign flip + burden |
| COMMODITY_FUTURES auto | 2.916 | validation sign flip + burden |
| US_EQUITY symbolic | 2.762 | burden-corrected significance |
| EQUITY_INDEX_FUTURES `TIME_SERIES_TREND` (governor-named) | 1.688 | materiality + burden |

Search burden moved from **710 counted at import to 2,024 across 254 families**.
R58 had disclosed 302.

Frontier at session end: `US_EQUITY`, `EQUITY_INDEX_FUTURES`, `RATES_FUTURES`,
`COMMODITY_FUTURES`, `FX_FUTURES`, `CROSS_ASSET` all RESEARCH_READY;
`VOLATILITY` EXHAUSTED (a single market with three time-series families, all now
prosecuted); `CREDIT_PROXY` BLOCKED (no owned tradable credit cross-section —
the R46 credit lane reads FRED series, which is a conditioner, not an
instrument panel).

## Continuation — resumed from the persisted state

The first run stopped at `D_ENVIRONMENT_LIMIT — iteration limit` with 19
mandates READY. That was the runner manufacturing its own interruption. The
default is now `RUN_UNTIL_RESEARCH_EXHAUSTED_OR_REAL_EXTERNAL_BLOCKER`; both
caps default to `None` and are reported as `operator_override` when passed.

Nothing was reset: the same `research_memory.sqlite` and `r59_autonomy.sqlite`
carried 2,080 hypotheses, burden 2,024, graveyard 2,025 and 55 frozen
challengers into the continuation.

### Four defects the continuation found, all by running

**1. `close_b` is not the deferred contract.** It is `<SYM>_CCB` — the same
front contract under a second back-adjustment convention (measured daily-change
correlation: median 0.982 across 103 markets). `(close_a − close_b)/|close_b|`
is an accumulated adjustment offset from an arbitrary anchor, not carry. The
feature is retired and the 15 hypotheses built on it are **invalidated in place
— annotated, never deleted, and still counted in search burden**, because the
effort was really spent.

**2. The real term structure was already on disk.** R38 froze a native
contract layer: 69 markets, 1987–2026, carrying the dated contract held
(`held`), the deferred-contract return (`ret2`), a genuine annualised curve
slope, open interest, volume and real roll dates — plus per-market
`cost_bps_per_side` of 2–15bp where R59 had been charging a flat 2bp to every
market. Five new families now run on it: `CALENDAR_TERM_STRUCTURE`,
`RATES_CURVE_RV`, `INTER_COMMODITY_RV`, `AGRICULTURAL_SEASONALITY`,
`ROLL_STATE`.

**3. Overlapping windows were annualised by cadence.** With cadence 5 and
horizon 21 the windows overlap four deep; dividing 252 by the cadence counted
the same holding period four times and inflated an annualised figure from 10%
to 43%. Annualisation is now by holding period; the overlap is paid for by the
Newey-West lag.

**4. A false survivor, and the two gate defects that produced it.**
`CALENDAR_TERM_STRUCTURE` at the 63-session horizon returned lockbox t = 4.739,
+10.6% annualised net, hit rate 0.84 — and was frozen as a challenger. It does
not survive scrutiny:

* its 177 lockbox rows overlap 12.6-fold, so the **effective** sample is ~14
  observations against a floor of 36. The floor had been applied to the raw
  count;
* its validation layer is +0.2% at t = 0.09 against a lockbox of +10.6%. The
  check required only `validation > 0`, which waves that through.

Both are fixed. Re-scored, **all 11 native cells are NO_ALPHA_EVIDENCE and the
freeze is withdrawn at inception** — annotated, with zero forward observations
accrued, so no prospective evidence was rewritten. The 55 prior-release
challengers are untouched.

### Generative exhaustion — so condition A is reachable at all

A generative family is never spent by one verdict, which meant the governor
could always produce a mandate and condition A could never be reached. It is
now spent when **measured**: novelty yield below 15% after at least 8 batches
and 60 candidates in that scope. `EQUITY_INDEX_FUTURES / AUTO` reached 0.147
over 34 batches and was retired automatically. Machine seeds are also
deterministic now — they had come from Python's per-process randomised
`hash()`, making runs unreplayable.

### EODHD — measured against the provider, not inferred

Root cause of the narrow coverage: `sources.eodhd.sample_symbols` is a
seven-name smoke list. A bounded, read-only, 5-request probe then separated
three questions the estate had conflated:

| Question | Measured answer |
|---|---|
| News entitled for off-sample names? | **Yes** (KO.US returned rows) |
| News *history* served? | **No** — 2016 and 2020 windows returned nothing |
| Corporate-action history? | **Yes** — CAT.US, 67 actions, 2010→2026 |
| Earnings session timing? | **Yes** — 6,126 rows / 6,076 symbols, all flagged |

So widening news buys a *prospective* family, not a backtestable one — a
distinction that matters more than the coverage number. Corporate actions are a
genuine 16-year unlock. And the earnings before/after-market flag was being
stored as display text while `available_at` stayed null; the canonical
collector now derives a conservative availability instant from it, and an
unflagged row still gets no instant.

## Alpha verdict

`NO_QUALIFYING_ALPHA` across the whole estate. Cumulatively R59 has measured
**3,561 hypotheses**; 1,265 further machine candidates (41.8% of 3,029
generated) were rejected as duplicate books before evaluation. Search burden is
**4,270 across 259 families** — R58 disclosed 302. Zero candidates survive the
corrected gate, and the one that briefly did was a false positive that the
correction removed.

Twenty mandates remain READY. The symbolic generative spaces still return
75–89% novel books, so that frontier is effectively unbounded and condition A
is not reachable for it within any single session.

### Resuming

```powershell
& C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe `
    D:\paper_trader_r59_autonomous_alpha\scripts\run_r59_autonomous_engine.py `
    --batch 14 --jobs 8
```

No caps, no reset: it continues from the same SQLite queue and memory.

No challenger was frozen this session: nothing crossed the historical
qualification gate, and R59 freezes only what does.

## Safety

Research-only throughout. No R59 module imports `api`, `engine` or `db`; a test
enforces that by AST, and another intercepts `Path.write_text` to prove every
write lands under the R59 research root. No order, no fill, no broker, no model
promotion, no sleeve activation, no proposal approval, no operational-store
write, no automation, no scheduled-task change, no backend restart. The live
`C:` checkout is neither read nor written by any R59 module.

---

## Continuation 2 — the persistent research runtime

R59 restored the autonomous loop but left its LIFETIME bound to an interactive
session. The loop only ever ran inside `scripts/run_r59_autonomous_engine.py`,
started by a person; the state survived (the queue and memory are SQLite) but
nothing ever started it again. Every "resume R59" prompt was a human doing a
scheduler's job.

### What already existed, and what it did not do

`PaperTrader-ResearchRuntime` has existed since Release 52: enabled, S4U, four
daily triggers, `MultipleInstances IgnoreNew`, `StartWhenAvailable`,
`WakeToRun`, `RestartCount 2`, running
`scripts/run_research_runtime.py --trigger SCHEDULED` from the deployed `C:`
checkout. It calls `alpha_agent.r52.runtime.research_runtime_cycle` — ONE
bounded prospective-evidence invocation: verify the evidence chains, advance the
R46 tournament, sweep forfeitures, rebuild the velocity and promotion read
models, write health, exit.

That is a complete owner for FORWARD evidence and carries none of R59's
discovery loop. Nothing in the estate had ever executed an R59 mandate on a
schedule. The task was not broken; it was never asked.

### What was reused

| Concern | Owner (unchanged) |
|---|---|
| Task definition / registration | `scripts/install_research_runtime_task.ps1` — still the ONLY registrar |
| Task validation | `scripts/validate_research_runtime_task.ps1` |
| Disable (deletes nothing) | `scripts/disable_research_runtime_task.ps1` |
| Entrypoint | `scripts/run_research_runtime.py` — one script, now two modes |
| Worker lease | `alpha_agent/r46/runlock.py` |
| Forward evidence | `alpha_agent/r52/runtime.research_runtime_cycle` |
| Queue / memory / governor / kernels | Stage 8, R59, R39, R57 — untouched |

A second scheduled task was deliberately NOT created. Two tasks means two
owners of when research runs.

### `alpha_agent/r59/runtime.py` — process lifecycle, and nothing else

Acquire single-worker ownership, load the persisted state, run the loop,
heartbeat, decide between working and sleeping, recover, stop cleanly, resume
exactly. It defines no statistic, no gate, no queue semantics and no timing
rule; the audit enforces that by token.

**The lease defect it had to fix first.** `runlock` reclaims any lock older
than `stale_after_s` *even when its pid is alive* — correct for a bounded run,
fatal for a persistent one, which would be evicted mid-research for the crime
of working longer than the threshold. `heartbeat_path` is the fix: a rewrite in
place, refused for anyone else's lock, whose mtime IS the liveness proof. A
lease that cannot be refreshed is treated as LOST OWNERSHIP and the worker
stops — it never recreates it, because recreating a lost lease is exactly how
two workers each come to believe they are the only one.

**Sleep is allowed; idling is not.** READY work always beats sleeping. The
runtime sleeps only when the governor cannot issue a mandate from the current
information set, and it wakes on a measured change in one of nine watched
watermarks (queue, blocked sources, both market panels, EODHD collection,
ingestion store, Form-4 history, forward evidence, the analyst sample, and the
data-opportunity frontier).

**Prospective evidence fails closed.** Maturation is delegated verbatim to
R52's cycle and is refused unless the running revision is *known committed and
clean*. Note what is not the test: a path. The question is whether the code is
committed; an unresolved answer counts as no. Historical research continues
either way — only prospective writes are gated.

### Resource governance: novelty is not value

The symbolic grammars still return 75–89% books the estate has never seen, so
novelty can never retire them, and 3,534 of R59's 3,561 measurements came from
two of them. The batch ceiling did not stop it because one machine mandate
expands into `MACHINE_BATCH` candidates while an economic mandate is one test.

Capacity is now allocated on MARGINAL USEFUL yield, measured from this estate's
own record:

| Method | Scored | Reached \|t\| ≥ 2 |
|---|---|---|
| `AUTO_TRANSFORM_GRAMMAR` | 2,003 | 7.0% |
| `SYMBOLIC_TREE_SEARCH` | 1,531 | 8.4% |
| `GOVERNOR_ECONOMIC_MANDATE` | 15 | 20.0% |
| `GOVERNOR_NATIVE_MANDATE` | 11 | 18.2% |

Neither rate has produced a survivor, so "notable" is a WEAK proxy — but it is
the honest one available and the ordering is the point: a machine test is about
a third as likely to produce anything worth looking at, at the same cost.
Measured today: generative share of recent work 97.8% against a 60% ceiling
(crowding 0.61) × marginal yield 0.074/0.106 (0.69) = **multiplier 0.42**,
which cuts `MACHINE_BATCH` from 6 to 3 and the machine's batch ceiling with it.
Floored at 0.25: throttled, never killed.

Two traps closed along the way. The alternative baseline is read from the WHOLE
record, not the same recent window — generative work crowds that window out by
construction, which left only eleven scored alternatives and silently switched
the marginal-yield term off. And reopening is keyed to a FINGERPRINT of the
opportunity set, never a timestamp: every session re-seeds the opportunity
table and rewrites `updated_at` on rows that did not change, so a timestamp
rule would have reopened allocation on every call and the throttle would have
measured as a no-op forever. New information restores full allocation for 100
generative tests, then expires.

### The task, in two modes

`-Mode Cycle` (default) is exactly the R52 definition. `-Mode Persistent` is
the same task, script and four times, plus `--mode persistent`, a logon trigger
for reboot recovery, and `PT0S` — no execution limit, because a persistent
researcher Windows kills after two hours is not persistent. The hung-run
protection a time limit used to give is now the lease heartbeat. The four daily
times are kept as a WATCHDOG: with `IgnoreNew` a trigger that fires while the
worker is healthy is a no-op, and one that fires after it died restarts it.
Release 46.6.2 lost six hours of collection to a logon-only trigger that never
fired again.

### Engineering smoke (foreground, explicit operator caps, from `D:`)

| | hypotheses | burden | completed jobs | READY |
|---|---|---|---|---|
| before | 4,327 | 4,270 | 1,373 | 146 |
| after smoke 1 | 4,469 | 4,412 | 1,433 | 143 |
| after smoke 2 | 4,585 | 4,528 | 1,487 | 141 |
| after crash recovery | 4,651 | 4,594 | 1,554 | 140 |

Each run resumed the same SQLite state, measured real hypotheses, heartbeated,
left READY work persisted and stopped on its explicit debug cap reported as an
operator override. A second worker started against a live lease was REFUSED
(exit 3) and did nothing. After a hard kill of the working process the lease
was left behind with a dead pid and the next worker reclaimed it immediately
and continued. Freezes stayed at 56 throughout; nothing was reset.

Maturation was disabled for every smoke: this source is uncommitted, and the
runtime refuses it anyway.

### Post-merge activation

Nothing was installed. The live task is untouched and still a valid Cycle task;
validated against the Persistent contract it correctly reports the three
differences. After merge and deployment, from an ELEVATED PowerShell:

```powershell
& C:\Users\binis\paper_trader\scripts\manage_research_runtime.ps1 `
    -RepoRoot C:\Users\binis\paper_trader -Action Install -Execute
& C:\Users\binis\paper_trader\scripts\manage_research_runtime.ps1 `
    -RepoRoot C:\Users\binis\paper_trader -Action Start -Execute
```

Status, stop and restart:

```powershell
& C:\Users\binis\paper_trader\scripts\manage_research_runtime.ps1 -Action Status
& C:\Users\binis\paper_trader\scripts\manage_research_runtime.ps1 -Action Stop -Execute
& C:\Users\binis\paper_trader\scripts\manage_research_runtime.ps1 -Action Restart -Execute
```

Every mutating action requires `-Execute`; every source-sensitive action is
refused unless `-RepoRoot` is the deployed checkout, so a development worktree
cannot be promoted into a service by accident.

---

## Continuation 3 — governed reallocation coherence

An operator review of session 2026-09-04 read a headline that contradicted
itself: `+0.081 score points does not clear the 0.050 economic hurdle`, beside
a Today card showing `0.056` against the same hurdle, and `45.7% / $112.24`
beside the proposal's `35.0% / $85.94`. A read-only investigation proved two
defects, both from commit `3d2d311` (R54.2.4), and both are fixed here. The
authoritative economics were correct throughout — what was wrong was what the
system said about them.

### Defect 1 — a narrative asserted a verdict the gate never reached

`explain_portfolio`'s mandatory-exit branch printed "does not clear the %.3f
economic hurdle" whenever `GATE_MANDATORY_EXIT` was present, with no comparison
behind it. Its own reason codes carried
`PORTFOLIO_NET_IMPROVEMENT_CLEARS_HURDLE`. The sibling held-name-breach branch
has always guarded the identical sentence with `net >= hurdle - 1e-12`; the
guard was simply never applied to its twin, and no test covered the twin.

The branch now selects its clause from `_clears_hurdle(net, hurdle)` — the one
comparison both branches share. The trigger is unchanged: a retention breach is
still why a complete target is requested, whatever the economics say.

**The gate was not touched.** No reason code, state, threshold or policy moved.

### The scope label, which was the deeper problem

`45.7% / $112.24 / +0.081` and `35.0% / $85.94 / +0.056` are both correct and
measure different objects, on one NAV of ~$98,214:

| | one-way | cost | net improvement | binding? |
|---|---|---|---|---|
| pre-proposal release-set estimate | 45.7% | $112.24 | 0.081451 | **no** |
| ideal complete target | 55.0% | — | — | infeasible |
| **constrained target (proposed)** | **35.0%** | **$85.94** | **0.055608** | **yes** |

Release 29.3 already declares the first row non-binding
(`turnover_budget_binding_here: False`), but the label lived in the artifact
while the number travelled into a headline alone. Every `PROPOSAL_READY`
sentence now carries `PRE_PROPOSAL_RELEASE_SET_ESTIMATE` / `NON_BINDING` and
names `engine.reallocation_proposal` as the owner of the binding figures.

Note the direction, which the review initially had backwards: constraint
optimisation **reduced** turnover (55.0% → 35.0%, deferring 14 trades). It
never raised it.

### Defect 2 — replacement labels did not follow the repaired weights

The live artifact asserted, simultaneously:

| ticker | action | weight | relationship |
|---|---|---|---|
| EXPD | REPLACE_IN | 0.0 → 0.040 | replaces **HST** |
| HST | **RETAIN** | 0.0393 → **0.0393** | replaced by EXPD |
| SNDK | REPLACE_IN | 0.0 → 0.040 | replaces **DVN** |
| DVN | **RETAIN** | 0.0436 → **0.0436** | replaced by SNDK |

Two `REPLACE_IN` against zero `REPLACE_OUT`. The turnover budget had deferred
the HST and DVN exits while keeping the EXPD and SNDK buys.
`_reoptimised_action` correctly re-derived HST and DVN's *action* to RETAIN,
but `replacement_relationship` was treated as provenance and carried through
untouched — contradicting the module's own docstring, *"a repaired row can
never keep a label its weights no longer support."*

A replacement relationship is not provenance. `source_hoc_recommendation`
already records what HOC asked for; the relationship is a claim about **this**
target, and constraint repair can falsify it.

Both legs are now checked, because they are mirror images:

```
REPLACE_IN  is true only if its counterparty LEAVES  (weight <= band)
REPLACE_OUT is true only if its counterparty ENTERS  (weight >  band)
```

An unprovable counterparty fails closed to the weaker, always-true label — ADD
for an entering name, EXIT for a leaving one. The claim codes
(`FUNDS_REPLACEMENT_OF_*`, `REPLACED_BY_*`) are stripped when the claim dies;
`HOC_REPLACE` stays, because what HOC recommended remains true. Replaced by
`REPLACEMENT_COUNTERPARTY_RETAINED`, `REPLACEMENT_COUNTERPARTY_DEFERRED` or
`REPLACE_DEFERRED_BY_CONSTRAINT_REPAIR` so the trace survives.

`_validate_constraints` now measures the invariant permanently:
`REPLACEMENT_RELATIONSHIP_ON_NON_REPLACE_ROW`,
`REPLACE_IN_COUNTERPARTY_RETAINED`, `REPLACE_OUT_COUNTERPARTY_ABSENT`,
`REPLACE_WITHOUT_COUNTERPARTY` and `ORPHAN_REPLACEMENT_LEGS`, surfaced as
`constraints.replacement_pairing_ok`.

Replaying the exact live rows through the fix:

```
EXPD  REPLACE_IN  -> ADD      0.040000  relationship None
HST   REPLACE_OUT -> RETAIN   0.039289  relationship None
SNDK  REPLACE_IN  -> ADD      0.040000  relationship None
DVN   REPLACE_OUT -> RETAIN   0.043551  relationship None
replacement_pairing_ok: True   violations: []   weights unchanged: True
```

### The regression this patch nearly shipped

The first version added the pairing violation without mirroring the classifier.
Repaired targets then contained orphan `REPLACE_OUT` legs, validation failed,
**the constraint repair was abandoned, and the turnover budget silently stopped
binding** — 0.35 → 0.90 one-way turnover, cost $87.50 → $225.00. No test
caught it; a direct old-versus-new economics diff did.

`test_the_label_fix_changed_no_weight_turnover_or_cost` now runs the deployed
pre-fix code in a subprocess and asserts identical turnover, cost and the full
weight vector at four budgets. After the mirror fix, every economic output is
identical and only the labels differ:

| budget | turnover | cost | weights | counts before → after |
|---|---|---|---|---|
| 0.10 | 0.10 = 0.10 | $25.00 | identical | unchanged |
| 0.20 | 0.20 = 0.20 | $50.00 | identical | REPLACE_OUT 1 → EXIT 1 |
| 0.35 | 0.35 = 0.35 | $87.50 | identical | REPLACE_OUT 2 → EXIT 2 |
| 1.00 | 0.90 = 0.90 | $225.00 | identical | 4/4 pairs preserved |

### HOC assessment authority — intentional, unchanged

The proposal binds HOC `a7d9588f…` while the live read surface reproduces
`3db8c5b5…`, with identical recommendation counts, and
`api.portfolio_decision` reports `ASSESSMENT_AUTHORITY_UNPROVEN`. That code is
emitted when the resolved assessment is not governed, is documented as
fail-closed in both directions ("Anything unprovable → NOT superseded"), and is
covered by `test_release54_2_3_2_decision_supersession.py`. Multiple
same-session HOC assessments are R54.3's designed behaviour. **No change was
made and no identity was rebound.**

### Testing the worktree, not the deployed tree

The venv's editable finder hard-maps `paper_trader` to `C:\Users\binis\paper_trader`,
so the existing suites cannot see a worktree's `engine/` changes. The new test
module loads both edited modules as top-level `engine.*` and asserts the
resolution; the impacted suites were additionally run with a scratchpad-only
pytest plugin that preloads the worktree modules under their canonical dotted
names. 825 impacted tests pass against this worktree's code.

---

## Continuation 4 — the definitive full-repository gate

The whole suite was run once, against this worktree's code: **10,040 passed,
976 skipped, 0 failed** (1:06:17, 11,010 collected). The 976 skips are the
database-backed tests, which `tests/conftest.py` skips whenever
`PAPER_TRADER_TEST_DATABASE_URL` is unset; that is the repository's normal gate
posture and is unchanged by this release.

### Grading the worktree rather than the deployed checkout

188 of the 227 test modules import through the `paper_trader.` prefix, and the
venv's editable finder is a `MetaPathFinder` that hard-binds that prefix to
`C:\Users\binis\paper_trader` — ahead of `sys.path`, so nothing on the path
can win. A full gate run from `D:` would therefore have graded the deployed
checkout, on whatever branch it happens to be sitting on. The run used a
scratchpad-only pytest plugin that re-points the already-installed finder's
`MAPPING` and `NAMESPACES` at the worktree, in-process only, and fails closed if
any `paper_trader` / `engine` / `api` / `alpha_agent` module was imported before
it ran. Nothing on disk — the venv, the editable install, the `C:` checkout — was
modified. All eight shared owners were proved to resolve inside
`D:\paper_trader_r59_autonomous_alpha` before collection began.

### Two defects the gate caught

**1. A coarse session flag cannot prove a point-in-time instant.** The EODHD
utilisation work had turned `before_after_market` into the record's
`available_at` (`AfterMarket` → `<date>T22:00:00Z`). That broke
`test_35_point_in_time_timestamp_separation`, whose fixture row is itself
`AfterMarket`-flagged — so `assert available_at is None` was a deliberate
Stage-2 invariant, not an artifact of the old collector. The bound was also
wrong on its own terms: 22:00Z is 17:00 EST, so a company releasing at 18:00 ET
would have been claimed public an hour *before* it was, which is look-ahead in
the one place the estate's PIT discipline exists to prevent it. Only a **day**
offset makes an after-close bound provable.

Fixed at the source, not in the test. `available_at` stays null;
`_earnings_session_bound()` puts a next-session upper bound in the payload as
`public_by_session_open` alongside the structured `session_timing` flag, and the
original `PUBLICATION_TIME_OF_DAY_UNKNOWN` warning survives beside it. The
research value — a machine-readable before/after-close distinction with a
usable, honest time bound — is kept; the shared PIT field is not overwritten
with a guess.

**2. `api/runtime_identity.py` could not resolve a commit in any linked
worktree.** `_resolve_git_dir` follows the `gitdir:` pointer, so worktree
support was clearly intended, but `_commit_from_git_dir` then looked for
`refs/heads/<branch>` beside that HEAD. A linked worktree keeps HEAD
per-worktree and its refs in the **common** directory, reached through its
`commondir` file. A detached HEAD resolved correctly; every branch checkout in a
worktree silently returned `commit=None` — which R59's `maturation_policy()`
then, correctly but needlessly, refuses as `SOURCE_REVISION_UNRESOLVED`.

`_ref_storage_dirs()` now returns a search list, nearest first. A plain checkout
has no `commondir` file, so its list stays `[git_dir]` and its resolution is
byte-for-byte unchanged — a test asserts exactly that.

### Proving "pre-existing" instead of asserting it

Neither failure was waved through. A `git archive` export of `HEAD` has no
`.git` at all and so cannot answer a git question, so the identity failure was
attributed with a real `git clone --no-hardlinks` of the worktree into `D:\Temp`
— a normal repository with a genuine `.git` directory. The test passes there at
the pristine baseline, and passes again with this release's files copied in,
which isolates the environment from the code. The two trees were confirmed
byte-identical first: 1,039 tracked-plus-untracked files, SHA-256 each, zero
differences. Both new test groups were then run against the unfixed baseline and
observed to fail, so neither is vacuous.
