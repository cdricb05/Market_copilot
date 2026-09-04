# Release 55.2.2 — Governed Daily Decision Persistence Closure

Status: implemented, targeted-gate tested, **not committed**.
Base HEAD: `4b88339` (R55.2.1). Branch: `stage19-controlled-rebalance`.

Scope: a **correctness** repair to the daily governed-write path plus an
acceptance-strictness change. No new engine, workflow, decision owner, ledger,
store, route or scheduler; no cadence, economics, allocation or safety-boundary
change; no production history written; and neither the backend nor the
collection worker was restarted.

---

## 1. The observed defect

The 2026-09-03 Portfolio Cycle completed successfully. Everything the operator
saw was correct: NAV $98,361.40, +$427.07 / +0.44%, 234 names scored, 6/6
TRUE_FORWARD snapshots, HOC ran, reassessment ran, portfolio conclusion
`CURRENT_NO_CHANGE`, no proposal required, no order created.

It was also the **first genuinely post-R54.4 daily cycle**, and it left no
governed ledger row:

```
D:\Stock_Prediction_app_data\portfolio_decisions\
    decisions.json      (the MANUAL operator lane, last written 2026-08-12)
    index.json
    # governed_decisions.json — still ABSENT
```

Acceptance nonetheless printed `R55_ACCEPTANCE_COMPLETE`, because the read-time
projection covering the gap was labelled `LEGACY_COMPATIBILITY_PROJECTION` —
the same word used for sessions that legitimately predate the delegating
producer. Two very different states shared one word, so the more permissive one
won.

The compatibility explanation ("this session completed before the R54.4
canonical writer existed") was **false**, and the recorded commit history says
so:

| Fact | Recorded value |
|---|---|
| R54.4 committed (`c0df3b1`) | 2026-09-03T16:14:04Z |
| 2026-09-02 cycle completed | 2026-09-02T23:51:52Z — genuinely pre-cutover |
| 2026-09-03 cycle started / completed | 2026-09-04T01:59:33Z / 02:14:13Z — **post**-cutover |

---

## 2. Reconnaissance — what was actually traced

Every step below is a read of the real production stores; the only writes went
to fresh temp roots.

**1. Which function produced the Sep-3 result?**
`api.daily_research_cycle.run_daily_research_cycle`, whose reassessment step
recorded `portfolio_reassessment_decision = CURRENT_NO_CHANGE` from
`prs_2026-09-03_alpha_paper_book_1_6ea1683ff59d` into the terminal manifest
`drc_2026-09-03_bc5b2eb5ee7b` (`state: COMPLETE`, `reused/resumed: False`).

**2-3. Where should it have delegated, and did it?**
`api/daily_research_cycle.py:3541` — `_delegate_governed_decision`, immediately
after `_persist`. It **did** run: the process serving the cycle had loaded
`c26cffe` (R55.2), which contains R54.4, and nothing else can explain a fresh
run reaching a terminal manifest without it.

**4-5. So why no row?** A pure replay of the real manifest through the real gate
answers it outright:

```
verdict        : DAILY_DECISION_WITHHELD
checks         : 18/19
withheld codes : ['HOC_ARTIFACT_IDENTITY_MISMATCH']
candidate word : CURRENT_NO_CHANGE        <- the manifest's own verdict
temp store     : []                       <- nothing written, correctly
```

The gate refused. `record_governed_decision` was never reached, so no row exists
and — because R54.4 deliberately never rewrites a manifest with its decision —
the delegation's own warning was appended to an in-memory dict after `_persist`
and is not on disk. The manifest is silent about the refusal by design.

**6-7. What does the store hold?** For every session up to and including
2026-09-02 the manifest's `opportunity_cost_assessment_hash` and its
`opportunity_cost_artifact_id` agree (the id embeds the hash's first 12 hex).
For 2026-09-03 alone they do not:

```
manifest artifact_id : hoc_2026-09-03_alpha_paper_book_1_f5f4a5643109
manifest hash        : a7ecdbe5c1744041b6d93127c247ee63a63f38c2630dcf56980848710acbe43a
stored artifact hash : f5f4a56431097b40f8f559fa95da1a3bb18e7b9949d2f65596dfdc376fd7400d
                       -> hoc_artifact_identity_matches: False
```

The stored artifact is `LIVE_PRE_DRC_SIGNAL`, `producer_owner:
api.event_signal_refresh`, written 2026-09-04T01:59:50Z — seventeen seconds
after the daily cycle started, i.e. **before** its opportunity-cost step ran.

**8. How the legacy projection is generated.**
`project_governed_daily_cycle_decision` fires whenever
`research_cycle_state.governed_research_evidence_current` is true and there is
no ledger row, and marks itself `persisted: False` / `projected: True` /
`legacy_compatibility_projection: True`.

**9. What distinguished the three cases before this release.** Nothing. A
legitimate old projection and a missing new write were byte-identical in shape.

**10-12. How it reached acceptance.** `api.active_manager_state`
`_governed_decision_block` carries `api.portfolio_decision`'s
`classify_decision_persistence` verbatim; `build_acceptance_contract` marked the
`GOVERNED_DECISION` row PRESENT on `gd["decision"]` alone, and `complete` was
`not missing`. A projection has a decision, so the row was PRESENT and
acceptance was COMPLETE.

**13-14. Was a HOLD/CURRENT_NO_CHANGE meant to persist?** Yes, explicitly.
R54.4 §5: concluding `CURRENT_NO_CHANGE` *for a session* is the session-terminal
daily producer's prerogative, and §14's own live replay records
`CURRENT_NO_CHANGE` being `CREATED` as a real row. No-change is **not** excluded
from persistence.

**15. Did a test pin the broken behaviour?** No test asserted a missing row was
acceptable. The gap was in coverage, not in a wrong assertion: nothing exercised
a REUSE outcome flowing into the daily manifest.

---

## 3. Root cause

Two facts, both at owner seams, both upstream of the gate.

**(a) The opportunity-cost owner returned an identity for a document it did not
write.** `persist_assessment` has five outcomes. `REUSED_EXISTING` is reached
when the economic state, the assessment evidence and the conclusion are all
unchanged but the document-wide `assessment_hash` differs — the case R54.3 named
the "Stage-21 trap", because that hash embeds the assessment's own output. On
that path the outcome returned `"identity": identity` — the identity of the
**recomputation that was discarded** — while returning the **existing**
artifact's id. `artifact_binding` therefore published:

```
hoc_artifact_id      = <the artifact the store holds>
hoc_assessment_hash  = <a hash that artifact does not carry>
```

Any consumer of the binding inherited the mismatch.

**(b) The daily producer never consumed the binding at all.** R54.3 built the
exact-version seam — `run_and_persist` returns `binding`, and
`portfolio_reassessment.run_and_persist` accepts `hoc_binding` — and wired it
into `api.event_signal_refresh` only:

| | manifest binds | passes `hoc_binding` |
|---|---|---|
| `api.event_signal_refresh` (intraday) | the owner's `binding` | **yes**, since R54.3 |
| `api.daily_research_cycle` (daily) | `assessment["assessment_hash"]` + `persistence["artifact_id"]` | **no** |

So the daily manifest paired the transient kernel hash with the reused
artifact's id, and the reassessment — given no binding — re-resolved one from
the document it was handed, found it was not the persisted artifact, and
correctly recorded `hoc_persisted: False`, `hoc_artifact_id: None`.

R54.4 then made the daily cycle delegate to a gate that enforces exactly this
binding. **The gate was right every time.** The failure is that the daily
producer claimed evidence the store could not produce, and it becomes
inevitable as soon as the intraday event cycle writes a live HOC artifact for
the session before the daily cycle runs — which continuous collection now makes
the normal case.

---

## 4. Call path — before and after

**Before**

```
DRC  -> hoc.run_and_persist            -> {assessment, persistence, binding}
     -> _extract_holding_opp_cost      -> assessment.assessment_hash   (transient)
                                       +  persistence.artifact_id      (reused)
     -> manifest                       -> two objects, one binding
     -> prs.run_and_persist(hoc_assessment=...)          [no hoc_binding]
                                       -> resolve_hoc_binding -> hoc_persisted: False
     -> pdec.govern_daily_cycle_decision
                                       -> resolve_binding -> identity_matches False
                                       -> DAILY_DECISION_WITHHELD 18/19
                                       -> NO ROW
     -> load_governed_portfolio_decision -> LEGACY_COMPATIBILITY_PROJECTION
     -> acceptance                       -> PRESENT -> R55_ACCEPTANCE_COMPLETE
```

**After**

```
DRC  -> hoc.run_and_persist            -> {assessment, persistence, binding}
                                          (a REUSE now reports the STORED identity)
     -> _extract_holding_opp_cost      -> binding.hoc_assessment_hash  (persisted)
                                       +  binding.hoc_artifact_id      (persisted)
                                       +  computed_assessment_hash     (visible)
     -> manifest                       -> ONE object
     -> prs.run_and_persist(..., hoc_binding=<the owner's binding>)
                                       -> records the artifact that exists
     -> pdec.govern_daily_cycle_decision
                                       -> GOVERNED_DAILY_DECISION_ELIGIBLE 19/19
                                       -> record_governed_decision -> CREATED
     -> load_governed_portfolio_decision -> LEDGER_ROW (projection retired)
     -> acceptance                       -> PRESENT, no blocker -> COMPLETE
```

Proven by replaying the **real** Sep-3 manifest with the corrected binding into
a temp decision root:

```
verdict        : GOVERNED_DAILY_DECISION_ELIGIBLE   19/19
recorded       : True | CREATED
record_id      : gdec_2026-09-03_alpha_paper_book_1_0ab55103fcba
decision       : CURRENT_NO_CHANGE | GOVERNED_DAILY_CYCLE
decided_at     : 2026-09-04T02:14:11.087773Z        <- the evidence's own stamp
repeat status  : REUSED_EXISTING | ledger rows = 1
```

---

## 5. Canonical writer and ledger ownership — unchanged

```
ONE BUSINESS CONCEPT      governed portfolio decision
ONE AUTHORITATIVE OWNER   api.portfolio_decision
ONE WRITER                record_governed_decision
ONE LEDGER                governed_decisions.json + governed_index.json
ONE ORDERING              governed_decision_ordering_key
TWO PRODUCERS             GOVERNED_DAILY_CYCLE | GOVERNED_INTRADAY
```

R55.2.2 adds no store, no index, no second writer and no route. The audit
asserts `second_governed_writer == []`, `second_governed_store == []` and
`backfill_routes == []`.

---

## 6. The persistence contract

`api.holding_opportunity_cost`

| Outcome | `identity` returned | Writes |
|---|---|---|
| `CREATED` / `CREATED_NEW_VERSION` / `CREATED_ASSESSMENT_VERSION` | the new artifact's | one new immutable artifact |
| `REUSED_EXISTING` | **the STORED artifact's**, with `recomputed_assessment_hash` beside it | nothing |
| `CONFLICT_REJECTED` / `REJECTED_INCONSISTENT_IDENTITY` | the refused candidate's | nothing |

`api.portfolio_decision` — what is persisted, when, where, and how it is read:

* **What.** One append-only governed decision row per distinct evidence
  identity, carrying the decision word, `decided_at` (the evidence's own stamp),
  the full identity, the bound artifacts and the safety flags.
* **When.** Only after the daily gate returns
  `GOVERNED_DAILY_DECISION_ELIGIBLE` (19/19) for a terminal-COMPLETE,
  read-back-verified manifest whose bound reassessment and opportunity-cost
  artifacts are retrievable **by exact id** and match.
* **Where.** `governed_decisions.json` + `governed_index.json`, under the
  decision root (or a pinned hermetic root).
* **How it is retrieved.** `load_governed_decision_record` (latest for a book),
  `load_persisted_daily_decision` (exact book + session),
  `load_governed_portfolio_decision` (what stands right now, under the one
  ordering). A compatibility projection is **retired** the moment a real row
  exists for the same book and session, so two descriptions of one decision are
  never both candidates for authority.
* **Idempotency vocabulary.** `CREATED` | `REUSED_EXISTING` |
  `SUPERSEDED_BY_NEWER_DECISION`. An exact repeat returns `REUSED_EXISTING`
  with the same `record_id` and the same `decided_at`, and appends nothing.
  Identical evidence identity presenting a contradictory conclusion is refused,
  not versioned.

---

## 7. Legacy compatibility and the cutover classification

The projection survives, unchanged in shape and still read-only. What changed is
that it is no longer *one* thing:

| Situation | Status | `expected_ledger_row` | Blocker |
|---|---|---|---|
| A real row exists | `LEDGER_ROW` | true | — |
| Session predates the delegating producer | `LEGACY_COMPATIBILITY_PROJECTION` | false | — |
| Session ran under the delegating producer, no row | `POST_CUTOVER_NOT_PERSISTED` | true | `GOVERNED_DAILY_DECISION_NOT_PERSISTED` |
| No governed decision at all | `ABSENT` | false | — |

`governed_daily_write_expected` decides it, and reads **no clock**:

1. **`PRODUCER_DECLARATION`** — the manifest's own
   `governed_decision_delegation` block, written by
   `api.daily_research_cycle` since this release
   (`daily_cycle_governed_delegation.v1`). It is a statement about the
   *producer's contract*, known before the handoff — never the decision, its id
   or its outcome — so R54.4's append-only rule (the row names the run; the
   manifest is never rewritten to name the decision) is untouched. The audit
   asserts `declaration_names_a_decision == []`.
2. **`RECORDED_RELEASE_BOUNDARY`** — for manifests persisted before that
   declaration existed, the recorded facts of §1: release `c0df3b1`, first
   delegating session `2026-09-03`. Fixed, auditable, independent of "today",
   and inert once no readable session predates the declaration.
3. **`SESSION_UNKNOWN`** — an undatable session is treated as PRE-cutover.
   Inventing an expectation would turn unknown history into a fabricated defect.

---

## 8. Why Sep-3 history was NOT rewritten

`HISTORICAL_GAP_PRESERVED`.

The Sep-3 governed decision was genuinely reached and remains readable through
the canonical owner as a projection. It has **no ledger row**, and this release
does not give it one:

* writing a row now would fabricate a persistence event that never happened, and
  `decided_at` would name an instant at which nothing was recorded;
* it would erase the only evidence that the governed write failed, on the very
  session that exposed the defect;
* principle 11 (never silently backfill missing historical evidence or
  decisions) and R54.4 §11 (no history is rewritten, no historical row
  fabricated) both forbid it.

The classification says so in the owner's own words: `is_ledger_row: false`,
`retrievable_through_owner: true`, `backfilled: false`,
`history_rewritten: false`, `historical_gap_preserved: true`. The repair is
forward-going: the next governed cycle writes a real row, and the projection
retires itself for that session.

Live, read-only, with the fix applied in-process:

```
session               2026-09-03
decision              CURRENT_NO_CHANGE       provenance GOVERNED_DAILY_CYCLE
persistence status    POST_CUTOVER_NOT_PERSISTED
is a ledger row       no       exact retrieval    yes
expected to persist   yes      cutover basis      RECORDED_RELEASE_BOUNDARY
backfilled            no       historical gap kept yes
acceptance            present 10/10 | blockers: GOVERNED_DAILY_DECISION_NOT_PERSISTED
R55_ACCEPTANCE_INCOMPLETE - blockers: GOVERNED_DAILY_DECISION_NOT_PERSISTED
```

---

## 9. Operator acceptance semantics

`build_acceptance_contract` keeps its ten rows and its PRESENT/MISSING meaning
exactly. It gains one concept:

> A **blocker** is a row that is PRESENT and still not acceptable.

The governed decision forced the distinction: it was genuinely reached, so it is
present, yet its ledger row is missing for a session the delegating producer was
supposed to persist. `complete` is now `not missing and not blockers`, and every
blocker code is named by the **owner of the fact** — the acceptance view invents
none (`report_invents_a_blocker == []`).

`scripts/r55_operator_acceptance.py` prints a dedicated
**GOVERNED DAILY DECISION PERSISTENCE** section stating the session, the
decision, the provenance, the record id, the persistence status, whether it is a
true ledger row, whether exact retrieval succeeded, whether the session was
expected to persist, the cutover basis, and **why acceptance passes or fails**.
It also refuses a stale served contract: a backend on a pre-R55.2.2 runtime
cannot express the blocker at all, so when the payload's decision owner names
one the contract is recomposed locally from that same payload
(`RECOMPOSED_LOCALLY`) rather than trusting a verdict that could not see it.

---

## 10. Daily vs intraday — the distinction is preserved

| | Daily governed cycle | Intraday research/event cycle |
|---|---|---|
| Operates on | the completed eligible session | live material information |
| Terminal conclusion | authoritative, and **must** persist through the owner | may legitimately end `NOT_REQUIRED_NO_NEW_INFORMATION` |
| No-op | there is no daily no-op: a session-terminal cycle reaches a decision | writes **nothing**; the standing decision is unchanged |

R54.4 §5 is unchanged and re-asserted by test 36: the intraday producer promotes
only on a priced R47 outcome and never maps a reassessment's
`CURRENT_NO_CHANGE` to a governed decision. Nothing in this release makes an
intraday no-op write a row to satisfy a counter (tests 35 and 37).

---

## 11. Safety invariants

Unchanged, and asserted:

* execution automation OFF, no broker, manual review required;
* a governed row changes no holding, cash or NAV, creates no order, order plan
  or fill, approves nothing, promotes no model, activates no sleeve, runs no
  close and advances no operational mark (test 53 — all 13 flags);
* the writer touches only its own two files (test 54); the opportunity-cost
  owner only its own store (test 55);
* the daily delegation block reaches no execution surface at token level
  (test 52);
* `api.active_manager_state` remains a read composition and classifies no
  persistence of its own (`ams_classifies_persistence == []`);
* the UI derives no persistence status (`ui_derives_persistence == []`) — it was
  not modified at all.

---

## 12. Files changed

| File | Change |
|---|---|
| `api/holding_opportunity_cost.py` | `_stored_artifact_identity` + `_reuse_outcome`: a REUSE reports the stored artifact's identity, with the recomputation named beside it; `artifact_binding` publishes both. |
| `api/daily_research_cycle.py` | `_extract_holding_opp_cost` consumes the owner's `binding`; the manifest records the binding, its provenance and the recomputed hash; `hoc_binding` is handed to the canonical reassessment; the producer declares its governed delegation. |
| `api/portfolio_decision.py` | The cutover contract (`governed_daily_write_expected`), the `POST_CUTOVER_NOT_PERSISTED` status, the blocker code, and the projection carrying the producer's declaration. |
| `api/workflow_state.py` | Forwards `governed_decision_delegation` into `research_cycle_state`. |
| `api/active_manager_state.py` | Carries the owner's expectation/blocker verdict; the acceptance contract gains `blockers` and accounts for them in `complete`. |
| `scripts/r55_operator_acceptance.py` | The persistence section, blocker-aware verdict, and refusal of a stale served contract. |
| `scripts/audit_architecture.py` | `check_release55_2_2_governed_daily_decision_persistence` + 27 blocking invariants. |
| `tests/test_release55_2_2_governed_daily_decision_persistence.py` | **New**, 56 hermetic tests. |
| `tests/test_release55_2_1_runtime_and_decision_continuity.py` | Fixture repair (see §13). |

---

## 13. Targeted tests

`tests/test_release55_2_2_governed_daily_decision_persistence.py` — 56 tests:

| Group | Tests | Covers |
|---|---|---|
| `TestOpportunityCostReuseIdentity` | 1-8 | the root cause; reuse writes nothing and rewrites nothing; the pre-fix binding is pinned as a guard; conflict and versioning still fail closed |
| `TestDailyProducerBindsWhatItCanRetrieve` | 9-14 | the manifest binds the persisted hash; a refused write keeps its own; the reassessment seam; injected seams keep their contract |
| `TestGovernedDailyWrite` | 15-20 | **A** no-change, **B** change/manual-review and HOLD write real rows; **C** exact retry reuses; **I** identity conflict and unretrievable artifact fail closed |
| `TestCutoverClassification` | 21-26 | declaration is authoritative; recorded boundary; unknown is conservative; no clock; the declaration describes the producer |
| `TestPersistenceClassification` | 27-34 | **D** row outranks and retires the projection; **E** legacy stays readable with no history rewrite; **F** post-cutover gap is a named defect; reading a gap creates nothing |
| `TestIntradayNoOpCreatesNoDecision` | 35-37 | **G** no-op writes no row, and no row is written to satisfy a counter |
| `TestActiveManagerAndAcceptance` | 38-46 | **H** and **K** — all three acceptance cases, the unchanged row vocabulary, and the report's gating |
| `TestBoundariesAndSafety` | 47-56 | **J** plus one writer / one ledger / no backfill / no recover route |

One pre-existing fixture defect was found and repaired while running the
perimeter: `tests/test_release55_2_1_runtime_and_decision_continuity.py` pinned
the live worker's pid (1976) into `_service_state` / `_lock`, and
`resolve_service_lifecycle` probes the **real** operating system for pid
liveness with no injectable seam. Once the operator's worker was restarted, that
pid stopped existing and two lifecycle assertions flipped `RUNNING -> DEGRADED`
— proven directly:

```
fixture pid 1976 -> DEGRADED | worker_alive False
fixture live pid -> RUNNING  | worker_alive True
```

The healthy-worker fixtures now use the test process's own pid, which is alive
by construction. The presence tests keep their explicit pids because they inject
`pid_alive` and never touch the OS. Nothing in R55.2.2 touches
`api/information_collection.py`.

Result of the targeted perimeter:

| Suite group | Result |
|---|---|
| R55.2.2 (new) | 56 passed |
| R54.4 + R55 + R55.1 | 194 passed |
| R54.1 + R54.2 + R54.3 | 211 passed |
| Slice 6 HOC + Slice 3 DRC + Stage 20 reassessment | 250 passed |
| R55.2 + R55.2.1 + R54.2.4 + R54.2.3.2 + R28 | 282 passed |
| R55.2.1 + R29 collection + R55.2.2 (re-run) | 212 passed |
| Stage 18 + R29.3 + Slice 2 workflow + R49 presentation + operator integrity + Track B | 251 passed |
| Architecture contracts + canonical restart + R29 UI | 97 passed |

`scripts/audit_architecture.py --strict` — exit 0, inventory drift 0.
`git diff --check` — clean.

---

## 14. Exact live acceptance procedure for the operator (Sep-4 or the next
completed session)

Run **after** the normal Portfolio Cycle completes for the next eligible
session. Do not run the cycle for this release; it is the operator's action.

```powershell
$SmokePaths = @(
    '/v1/operations/workflow-state',
    '/v1/operations/information-collection',
    '/v1/operations/daily-close',
    '/v1/operational-book',
    '/v1/operations/portfolio-reassessment'
)

& C:\Users\binis\paper_trader\scripts\restart_paper_trader_backend.ps1 `
    -Force `
    -Port 8001 `
    -SmokePath $SmokePaths

& C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe `
    C:\Users\binis\paper_trader\scripts\r55_operator_acceptance.py
```

Expected, for the session the cycle just closed:

| Field | Expected |
|---|---|
| `session` | the newly completed eligible session (2026-09-04 if that is next) |
| `decision` | whichever governed terminal word was legitimately reached |
| `provenance` | `GOVERNED_DAILY_CYCLE` |
| `record id` | `gdec_<session>_alpha_paper_book_1_<hash>` — a real id, not `drc_governed_...` |
| `persistence status` | **`LEDGER_ROW`** |
| `is a ledger row` | **yes** |
| `exact retrieval` | yes |
| `expected to persist` | yes |
| `cutover basis` | `PRODUCER_DECLARATION` |
| `backfilled` | no |
| acceptance | `present 10/10 | blockers: -` then **`R55_ACCEPTANCE_COMPLETE`** |

And, unchanged: execution automation OFF, broker NONE, manual review REQUIRED,
no order, no fill, no approval, no model promotion, no sleeve activation. A
`CHANGE_RECOMMENDED` outcome is equally acceptable — the persistence criterion
applies to whichever governed terminal decision was reached, and a change still
requires the operator approval token and the Stage-19 order-plan confirmation.

Store-level confirmation (read-only):

```powershell
Get-ChildItem 'D:\Stock_Prediction_app_data\portfolio_decisions'
```

Expect `governed_decisions.json` and `governed_index.json` to exist **for the
first time**, beside the untouched manual `decisions.json` / `index.json`. A
second read of the acceptance report must not change the row count.

**Sep-3 must remain `POST_CUTOVER_NOT_PERSISTED` with `backfilled: no`.** If it
ever appears as a ledger row, something backfilled history and that is a defect,
not a repair.

---

## 15. What remains after R55.2.2

The **reassessment artifacts already written** for 2026-09-02 and 2026-09-03
still record `hoc_persisted: false` with an unretrievable `hoc_assessment_hash`.
That is honest history and this release does not rewrite it; R54.3's exact
binding governs it, and the R55.2.1 Phase-J finding stands. Forward-going runs
bind correctly.

The **operational-book cutover** noted in R54.4 §15 is unchanged: an approved
governed CHANGE still does not name the exact governed decision it implements.
