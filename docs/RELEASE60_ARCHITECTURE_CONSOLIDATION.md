# Release 60 — Architecture Consolidation + AlphaAgent Outcomes Visibility

> Worktree `D:\paper_trader_r60_architecture`, branch
> `r60-architecture-consolidation`, built over `a5c09cb`. The deployed checkout
> `C:\Users\binis\paper_trader` was **read only** throughout: no restart, no
> scheduled-task change, no collection run, no portfolio cycle, no daily close,
> no approval, no order, no promotion, no service started from `D:`. The
> persistent AlphaAgent kept running on `C:` for the whole release and its
> SQLite state was read, never written.
>
> Static analysis does not prove runtime behaviour. Every claim below either
> cites a file, or was **measured** against the live persisted state through a
> read-only handle; measurements are stamped with the instant they were taken.

---

## 0. The problem R60 exists to solve

R59 solved the process-lifetime problem: AlphaAgent now researches independently
of any interactive session. It did **not** solve the legibility problem. Two
facts made that concrete:

1. **There was no API surface of any kind over the R59 persistent research
   memory.** `grep -ri r59 api/` returned nothing. A 21 MB SQLite database
   holding every hypothesis the estate has ever prosecuted, its statistics, its
   refusing gate, its graveyard and its reopen conditions existed only on disk,
   and every question about it had to be answered by a person opening a
   database.
2. **The only thing the product could say about a running researcher was a
   process line** — `worker=RESEARCHING queue=141 hypotheses=4653`. That
   answers none of the operator's questions, and its green word invites exactly
   the wrong inference: a healthy process is not successful research.

R60 closes both, and does so without letting the act of reading the research
record change it — which turned out to be the hard part.

---

## 1. Current architecture map (Workstream A)

Traced from code and from the live persisted state, not from filenames. The
authoritative map now lives in `docs/architecture/system_inventory.json`
(**updated in place**; no competing inventory owner was created). R60 added
**ten** concept rows — the concept list had stopped at R54, so *nothing the
persistent researcher owns* had an inventory row at all.

### 1.1 The operational lane (unchanged by R60)

| Concept | Canonical owner |
|---|---|
| eligible market session | `engine/market_session.py` |
| data-source freshness | `api/data_freshness.py` |
| operational mark / NAV | `api/paper_trading_desk.py` (ledger replay) → read owner `api/portfolio_state.py` |
| information collection | `api/information_collection.py` |
| signal refresh (live/event) | `api/event_signal_refresh.py` |
| universe scoring / rankings | `api/universe_scoring.py` over the `api/multi_horizon_engine.py` kernel |
| holding opportunity cost (HOC) | `api/holding_opportunity_cost.py` |
| portfolio reassessment | `api/portfolio_reassessment.py` over `engine/portfolio_reassessment.py` |
| constrained reallocation proposal | `api/reallocation_proposal.py` over `engine/reallocation_proposal.py` |
| governed portfolio decision | `api/portfolio_decision.py` (single writer, R54.4) |
| daily orchestration | `api/daily_research_cycle.py` |
| operator workflow state | `api/workflow_state.py` |
| order creation / execution | **NONE** — deferred to Milestone 7; `engine/reconciler.py` quarantined |

### 1.2 The research lane (mapped and recorded by R60)

| Concept | Canonical owner | Was in the inventory before R60? |
|---|---|---|
| persistent research runtime lifecycle | `alpha_agent/r59/runtime.py` (the ONE `run_forever`) | no |
| persistent research memory | `alpha_agent/r59/memory.py` (ONE SQLite DB) | no |
| research work queue | `alpha_agent/autonomous_research.py` `ResearchQueue` (Stage 8) | no |
| hypothesis generation / what to learn next | `alpha_agent/r59/governor.py` over `alpha_agent/r39/representation_factory.py` | no |
| search burden | `alpha_agent/r59/memory.py` `burden()` — **counted**, never copied | no |
| graveyard + reopen conditions | `alpha_agent/r59/memory.py` `graveyard()` / `reopenable()` | no |
| prospective challenger freeze | `alpha_agent/r59/memory.py` `freeze_forward()` + `handlers.freeze_qualified()` | no |
| TRUE_FORWARD maturation | `alpha_agent/r52/runtime.py` over `alpha_agent/r46/advance.py` | no |
| data-opportunity frontier | `alpha_agent/r59/opportunities.py` over the memory | no |
| **AlphaAgent research outcomes** | **`api/alphaagent_outcomes.py` (new, R60)** | n/a |

### 1.3 Live measurement (2026-09-07T18:55Z, read-only)

| Quantity | Value | Source |
|---|---|---|
| hypotheses registered | 5,467 (all settled) | research memory |
| by outcome | 5,408 `NO_ALPHA_EVIDENCE`, 56 `FORWARD_FROZEN`, 3 `REJECTED`, **0 `QUALIFIED`** | research memory |
| counted search burden | 5,410 across 259 distinct families | `memory.burden()` |
| by release | R59 4,701 · R39 608 · R39C 77 · R46 45 · R58 18 · R57 12 · R56 6 | research memory |
| queue | 29 QUEUED · 1 RUNNING · 6 BLOCKED_SPECIFIC · 2,472 COMPLETED | Stage-8 queue |
| frontier | 6 scopes RESEARCH_READY, VOLATILITY EXHAUSTED, CREDIT_PROXY BLOCKED (no owned substrate) | frontier table |
| forward challengers | 45 active, 296 TRUE_FORWARD predictions, 41 matured, **18 effective independent**, promotion-ready **0** | R46 board / R52 health |
| worker | `RESEARCHING`, lane `r59.research`, heartbeat 3 s old, source `a5c09cb` clean | runtime status + lease |

**The honest headline that state produces:** `FORWARD_EVIDENCE_MATURING — 5,467
hypotheses measured, 5,467 settled; worker RESEARCHING.` No qualified alpha
exists. The estate is accruing forward evidence and has not yet earned a
governed review.

---

## 2. Duplicated / disconnected logic (Workstream B)

Every workflow below was traced end to end. Findings are classified with the
release's own vocabulary.

### F1 — A healthy AlphaAgent could be declared dead, and replaced — `BLOCKING_DEFECT` (fixed)

`alpha_agent/r46/runlock.pid_alive` treated **every** `OpenProcess` failure on
Windows as "the process does not exist". The persistent researcher runs under a
scheduled task's principal, so an unprivileged reader gets
`ERROR_ACCESS_DENIED` — and `_reclaim_if_stale` deletes the lease whenever
`alive is False`. That is a licence to evict a **healthy** worker and start a
second AlphaAgent, which is precisely what R59's single-worker lease exists to
prevent.

Measured before the fix, against the live worker (pid 60908, heartbeat 4 s old):
`pid_alive → False`. After: `pid_alive → None` (undecidable), and
`_reclaim_if_stale` refuses. `False` is now reserved for
`ERROR_INVALID_PARAMETER`, which is how the Windows kernel says *no such
process*. The POSIX branch always made this distinction — `PermissionError` is
an `OSError`, not a `ProcessLookupError`.

### F2 — Reading the research record wrote to it — `BLOCKING_DEFECT` (fixed)

Three separate write-on-read paths made a naive read model impossible:

* `ResearchMemory.__init__` created the directory, ran the schema script and
  wrote a `memory_meta` row. Constructing a **reader** was constructing a
  **writer**.
* `ResearchQueue.__init__` did the same for the Stage-8 queue.
* `r59.runtime.runtime_dir()` created the artifact directory as a side effect of
  resolving a path, so `read_status()` created a directory.
* And the correct owner of "what should we research next" —
  `governor.generate_mandates` — writes a capacity fingerprint (`set_meta`) and
  a `MANDATES_GENERATED` event on **every** call, as does `frontier.measure`
  (a frontier row + event) and `report.build` (an artifact). A GET that asked
  the governor would make opening a dashboard change the research plan.

R60 added READ-ONLY handles to the canonical owners (§4) and answers *current
intent* and *next research* from the **queue**, whose job payload **is** the
mandate the governor issued.

### F3 — `cumulative_hypotheses` reported the wrong number under the right name — `BLOCKING_DEFECT` (fixed)

`r59.runtime.status()` read `summary.get("n_hypotheses")`. `memory.summary()`
has never carried that key; it carries `hypotheses_total` and
`hypotheses_settled`. The lookup always fell through to
`sum(by_outcome.values())` — the **settled** count — and published it as the
total. Both are now reported, each under its own name.

### F4 — Five prospective freezes accrue no forward evidence — `BLOCKING_DEFECT` (documented, sequenced R61)

R59 `freeze_qualified` writes an inception instant into research memory and an
artifact under the R59 root. It does **not** register the challenger with a
forward-evidence owner, and neither `alpha_agent/r52/runtime.py` nor
`alpha_agent/r46` knows the R59 root exists.

Measured live, cross-referencing every `FORWARD_FROZEN` row against **both**
forward owners:

| Challenger | Release | Inception | Forward evidence |
|---|---|---|---|
| `R59_CALENDAR_TERM_STRUCTURE_F9BE2426` | R59 | 2026-09-04T23:44:35Z | none, ever |
| `R58_SHORT_VOLUME_PRESSURE_V1` | R58 | 2026-09-03 | none, ever |
| `R58_DISCLOSURE_INTENSITY_V1` | R58 | 2026-09-03 | none, ever |
| `R58_FUND_MOMENTUM_VETO_V1` | R58 | 2026-09-03 | none, ever |
| `R58_FCF_PURE_V1` | R58 | 2026-09-03 | none, ever |

The other 51 rows resolve correctly: 45 to `api.prospective_tournament` (R46
signal challengers) and 6 to `api.shadow_portfolio_evidence` (R56 forward paper
portfolios).

R60 does **not** fix this: registering an R59 freeze into the R46 challenger
registry is a research-pipeline change, and R60's mandate forbids changing
research scoring or alpha gates. It reports each orphan as
`NOT_REGISTERED_WITH_FORWARD_EVIDENCE_OWNER` and raises a named human action
rather than showing a hopeful zero, and it is sequenced as **R61 NOW**.

### F5 — The research question had no owner — `ROUTE_TO_CANONICAL_OWNER` (done)

Before R60 the Research workspace could answer *"is the champion model stack
still trustworthy?"* (`api/research_agent.py`, Slice 8) and *"is the scheduled
maturation cycle healthy?"* (`api/research_runtime.py`, R52). Neither answers
*"what has the persistent researcher found?"*, and nothing read the R59 memory.
Four candidate owners were checked before adding one:

| Candidate | Why it is not the owner |
|---|---|
| `api/research_agent.py` | Governs the operational CHAMPION/CHALLENGER stack — a different question, kept separate |
| `api/research_bridge.py` | Commissions bounded research on the Research Agent's behalf; reads the queue, not the memory |
| `api/alpha_opportunity_registry.py` | R56 frozen CITATION catalogue of families across 26 releases; not a live record |
| `alpha_agent/evidence_observatory.py` | Stage 1–7 evidence inventory — a pre-R39 generation of the research OS |

### F6 — Four distinct forward-evidence identities — `KEEP` (documented)

They must never be summed, and now each names its owner in one place:

| Evidence identity | Owner |
|---|---|
| R46 forward SIGNAL challengers | `api/prospective_tournament.py`, advanced by `alpha_agent/r52/runtime.py` |
| R56 forward PAPER PORTFOLIO challengers | `api/shadow_portfolio_evidence.py` |
| daily governed TRUE_FORWARD bundle | `api/forward_prediction_skill.py` |
| R53.1 `PROSPECTIVE_INTRADAY` lane | `api/research_runtime.load_intraday_emission_status` |

### F7 — Two runtimes, deliberately — `KEEP` (documented)

`alpha_agent/r59/runtime.py` is the persistent **discovery** worker;
`alpha_agent/r52/runtime.py` is the prospective **maturation** cycle that the
discovery worker calls. Both were unlabelled in the inventory and both now have
concept rows. There is exactly one `run_forever` in the tree.

### F8 — Contradictory status vocabularies — `ROUTE_TO_CANONICAL_OWNER` (done)

The operator's only research word was a **worker** state. R60 introduces one
six-word EVIDENCE vocabulary — `NO_QUALIFIED_ALPHA_YET`,
`HISTORICAL_CANDIDATE_ONLY`, `FORWARD_EVIDENCE_MATURING`,
`CHALLENGER_WARRANTS_GOVERNED_REVIEW`, `RESEARCH_WAITING_FOR_NEW_INFORMATION`,
`RESEARCH_MEMORY_NOT_PRESENT` — disjoint from `WORKER_STATES`, and binds the
operator badge to it. Worker state is a separate, secondary row.

### F9 — 62 declared routes the UI never calls — `REMOVE_LATER`

The audit's `orphan_endpoint_candidates` lists 62 `/v1/...` routes with no UI
consumer (e.g. `/v1/research/runtime-health`, `/v1/operations/portfolio-cycle`,
`/v1/evidence/*`). These are leads, not authorisation: several are operator
scripts' or acceptance harnesses' entrypoints. R60 deletes none, and sequences a
consumer census as a LATER item. One dangling UI reference exists and is
pre-existing: `/v1/ticker-detail/`.

### F10 — The outcomes read costs ~2.9 s — `ROUTE_TO_CANONICAL_OWNER` (LATER)

`api.prospective_tournament.load_prospective_tournament()` loads the full R46
board (296 predictions, 41 outcomes, every ledger) and takes ~2.9 s. The
outcomes projection needs a handful of scalars and a challenger-id set from it.
R60 calls the canonical owner rather than forking a faster reader; a bounded
`summary()` accessor on that owner is sequenced as LATER.

---

## 3. Authoritative owners after R60

One concept, one owner. `check_release60_alphaagent_outcomes` asserts that every
canonical concept in the inventory carries exactly one `authoritative_owner`
string, and that the ten research concepts exist.

The one new owner:

```
api/alphaagent_outcomes.py          GET /v1/research/alphaagent-outcomes
  reads   alpha_agent.r59.memory              (read-only handle)
          alpha_agent.autonomous_research     (read-only handle, via r59.loop)
          alpha_agent.r59.runtime.status      (read_only=True)
          api.research_runtime                (R52 maturation health)
          api.prospective_tournament          (R46 signal challengers)
          api.shadow_portfolio_evidence       (R56 forward paper portfolios)
  writes  nothing
  owns    presentation
```

---

## 4. Bounded consolidations implemented

Each landed in the **canonical owner**, not in the new module — a read model that
carried its own workaround for a defective owner would have been a second owner.

### C1 — Read-only handles on the two research stores

* `alpha_agent/r59/memory.py`: `ResearchMemory(read_only=True)`,
  `open_memory_readonly()`, `memory_present()`, `memory_db_path()`,
  `ReadOnlyMemory`. No `mkdir`, no schema script, no `memory_meta` row; connects
  `file:…?mode=ro`, falling back to `PRAGMA query_only=ON` when a WAL companion
  file is absent (a cleanly-closed store cannot be opened `mode=ro` because
  SQLite would have to *create* the `-shm` file). `immutable=1` is deliberately
  **not** used — it would return torn reads while the live worker is
  mid-transaction. Eleven mutating methods raise.
* `alpha_agent/autonomous_research.py`: `ResearchQueue(read_only=True)`,
  `ReadOnlyQueue`, the same contract; six transitions raise.
* `alpha_agent/r59/loop.py`: `open_queue(read_only=True)`, `queue_db_path()`,
  `queue_present()`.
* `alpha_agent/r59/runtime.py`: `runtime_dir(create=…)`, `status_path()`,
  `status(read_only=True)`; `read_status()` no longer creates a directory.

**Proven against the live store** (worker running): every write refused, and no
research file changed apart from the live writer's own `-wal`/`-shm`. Proven
**hermetically** in `test_projection_writes_nothing`, which hashes every file in
a private research root before and after the read.

### C2 — The lease no longer calls a healthy worker dead

`pid_alive` returns `None` for `ERROR_ACCESS_DENIED` and `False` only for
`ERROR_INVALID_PARAMETER`. Undecidable fails closed: an undecidable pid keeps
its lease until the age rule expires it.

### C3 — `cumulative_hypotheses` means the total

`r59.runtime.status` reports `cumulative_hypotheses` (total) and
`cumulative_hypotheses_settled` (settled), each from the key the memory summary
actually carries.

### C4 — Narrow read projections on the memory owner

`settled_between()`, `strongest_unqualified()` and `count_unqualified_above_t()`
keep the SQL in the module that owns the schema. The read model contains no
`sqlite3` import and no SQL — asserted by the audit.

### C5 — A false ownership signal removed

The concept-writer heuristic matched `def _unavailable(` as a NAV writer
(`u-**nav**-ailable`), adding the new read model to the `portfolio_nav_valuation`
multi-writer list. Renamed to `_not_available`.

---

## 5. Active portfolio-manager alignment (Workstream F)

**The operational loop is intact and unchanged by R60:**

```
ingest / freshness      api.information_collection → api.data_freshness
  → authoritative marks / NAV      api.paper_trading_desk → api.portfolio_state
  → signals / scores / rankings    api.universe_scoring
  → holding opportunity cost       api.holding_opportunity_cost
  → economic change gate           api.portfolio_reassessment   (gates the next step)
  → constrained target             api.reallocation_proposal
  → governed proposal              api.portfolio_decision       (single writer)
  → MANUAL REVIEW
  → paper execution                only on explicit approval; no order route added
  → forward portfolio evidence     api.reassessment_outcomes / api.forward_evidence
```

**The research loop is separate and terminates at a human:**

```
persistent AlphaAgent    alpha_agent.r59.runtime
  → hypothesis / experiment        r59.governor → Stage-8 queue → r59.engines
  → graveyard or candidate         r59.memory
  → prospective freeze             r59.memory.freeze_forward   (inception only)
  → TRUE_FORWARD evidence          alpha_agent.r46 / r52       [F4: 5 freezes unlinked]
  → governed challenger review     CHALLENGER_WARRANTS_GOVERNED_REVIEW
  → MANUAL PROMOTION ONLY
```

**Proof that no path runs from an AlphaAgent candidate to capital:**

| Proof | Where |
|---|---|
| The research package cannot import the application at all | `check_release59…research_does_not_import_the_app` |
| No execution call term appears anywhere in research code | audit `research_execution_terms` = 0 |
| The R59 runtime has no operational reach and no HTTP client | `check_release59…no_operational_reach` |
| The outcomes owner contains no order / promotion / approval / allocation call | `check_release60…owner_forbidden_calls` = [] |
| The outcomes owner imports no operational write path | `test_owner_imports_no_operational_write_path` |
| The response declares promotion / mutation / allocation disallowed | `test_no_challenger_to_capital_path` |
| The strongest state a challenger reaches is a sentence for a person | `CHALLENGER_WARRANTS_GOVERNED_REVIEW` |
| The route is GET-only with no action sibling | `check_release60…route_get_only`, `forbidden_routes_present` = [] |
| A historical result can never be relabelled forward | `memory.record_result` refuses `FORWARD_CONFIRMED` |

---

## 6. What the operator now sees

One new **primary** section on the **existing** Research workspace —
`Research → AlphaAgent Outcomes` — which is now the Research landing. No second
dashboard; every pre-R60 deep link (`research-agent`, `research-bridge`,
`performance`, `data-expansion`, `stage11/12/13a`, `diagnostics`, …) resolves
unchanged.

The panel answers, in order: **whether human action is required**; what
AlphaAgent is doing now and *why it chose that*; the research **process** (in a
separate box, badged `PROCESS HEALTH ≠ RESEARCH SUCCESS`); what changed over four
windows; forward challengers including the orphaned freezes; the best current
evidence *and which recorded gate refused it*; what failed and what was learned;
data opportunities and whether a purchase is **actually** recommended; and the
next research action read from the queue. Volume counters, the multi-asset
frontier and provider utilisation are behind an **Audit / Advanced** disclosure.

The browser classifies nothing: it renders backend fields, and the audit asserts
it never compares against a governance literal, performs no research arithmetic,
carries no action control, and uses no `alert()` or `confirm()`.

---

## 7. Honesty rules enforced in the read model

| Rule | Why it exists |
|---|---|
| Every window splits `measured_here` from `imported_from_prior_release` | `settled_at` is the instant *this memory* recorded a verdict, so an imported prior-release result carries the **import** instant. A backfill is not a day of research. |
| An unadopted freeze reports `NOT_REGISTERED_WITH_FORWARD_EVIDENCE_OWNER` | A hopeful zero and a structural disconnection look identical in a count. |
| `purchase_actually_recommended` follows the canonical gate verdict | `PURCHASE_CANDIDATE` is a *state*, not a recommendation. |
| `graveyard.rejected_total` comes from the outcome census, not the page size | A cap silently becoming a number an operator reasons with. |
| Undeliverable values are `NOT_AVAILABLE` **with a reason** | `duplicates_rejected_before_evaluation` is decided in flight and leaves no row; it says so, and points at `generator_yield`. |
| An absent memory is `RESEARCH_MEMORY_NOT_PRESENT`, not an empty record | An absence of evidence is not a negative result. |
| Candidates are ranked by the **signed** lockbox t | A large negative t is the opposite finding, not a near miss. |
| Process health and alpha evidence are separate blocks | A healthy worker is not a research result. |

---

## 8. What R60 deliberately did not do

No repository-wide rewrite. No change to the portfolio engine, research scoring,
alpha gates, turnover policy, the operational portfolio, the model champion or
any task schedule. No order route. No cadence enabled. No compatibility surface
deleted for cleanliness. No service run from `D:`. Nothing committed, pushed,
merged or deployed.
