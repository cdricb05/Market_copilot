# Release 54.1 — the governed intraday portfolio decision cycle

**Status:** implemented, tested, not yet committed (operator gate).
**Base:** R54 `8c040ce` on `stage19-controlled-rebalance`.
**Scope:** ONE governance gate + ONE governed decision lane. No new engine, no
new optimizer, no new economics, no scheduler change, no execution.

---

## 1. The gap R54.1 closes

R54 proved the live chain already runs intraday:

```
continuous information -> materiality -> affected calculation refresh
    -> universe scoring / ranking context -> Holding Opportunity Cost
    -> portfolio reassessment -> complete priced target when required
```

What it could **not** do was let that chain change the answer. Release 29.5
declares a decision *governed* only when `api.daily_research_cycle` holds a
validated run manifest (`governed_research_evidence_current`). Everything the
event cycle produces is therefore classified `LIVE_PRE_DRC_SIGNAL` — real,
current, displayable, and never authoritative.

So the system could know at 13:42 that new information had arrived, that the
portfolio had been fully reassessed against it, and that the priced conclusion
was HOLD or CHANGE — while every surface still showed the **previous** DRC
decision as the recommendation. Safe, but not an active manager.

R54.1 adds the missing rung of the ladder, and nothing else.

## 2. What makes an intraday assessment GOVERNABLE

One gate, one question:

> Is this intraday reassessment sufficiently complete, fresh, point-in-time
> bound and internally consistent that it can **replace the prior governed
> portfolio decision as the latest authoritative recommendation?**

That is a different question from *"should the portfolio trade?"*. The answer
to the second is still, always, **no**.

**Owner:** `api.portfolio_decision` — the existing canonical decision owner.
There is no second governance framework, no second decision engine and no
second economics. The gate decides **admissibility**; every hurdle, cost, risk
number, constraint verdict and outcome is read verbatim from the owner that
decided it.

### The gate (38 checks, nine groups)

| Group | What must hold | Owner consulted |
|---|---|---|
| `PORTFOLIO_IDENTITY` | active book present and unchanged; the reassessment's bound `portfolio_state_hash` still equals the live one; the cycle's bound economic hash matches; holdings reconcile; cash + NAV published; corporate-action registry current | `api.portfolio_state`, `api.corporate_actions` |
| `MARKET_DATA_FRESHNESS` | the **book's own** eligible session is owned-confirmed and validly closed; no proposal data gap; reassessment not `BLOCKED_DATA` / `BLOCKED_EVIDENCE`; point-in-time integrity (no ranking dated after its session, one session across every artifact) | `api.daily_close`, `engine.market_session`, `api.reallocation_proposal`, `api.portfolio_reassessment` |
| `SIGNAL_RANKING_IDENTITY` | ranking identity bound; ranking basis date explicit; live input-contract hash unchanged | `api.universe_scoring` |
| `HOC_IDENTITY` | assessment hash bound; the target is bound to the SAME HOC; every current holding assessed | `api.holding_opportunity_cost` |
| `REASSESSMENT_IDENTITY` | reassessment hash bound; the cycle's reassessment IS the candidate; materiality trigger fingerprint bound | `api.portfolio_reassessment`, `engine.event_materiality` |
| `TARGET_IDENTITY` | a CONCLUSIVE priced outcome (`PROPOSAL_READY` or `HOLD_CURRENT_BOOK`); target hash bound; target bound to the active book; a feasible target was computed | `api.reallocation_proposal` |
| `ECONOMIC_CONTROLS` | switching economics complete (hurdle, clears flag, turnover, cost, concentration before/after, net improvement); risk before/after priced; turnover budget evaluated; zero-base incumbency policy intact; not an anti-churn duplicate trigger; the cycle reached a portfolio answer and is not blocked | `engine.constrained_reallocation` |
| `CONCURRENCY` | the candidate adds new evidence; no newer governed decision stands; no in-flight execution holds precedence; the identity is deterministic | `api.portfolio_decision`, `api.rebalance_execution` |
| `SAFETY` | a governed CHANGE requires manual review; automation / broker / approval / promotion / sleeve activation all off | structural |

Verdict: `GOVERNED_INTRADAY_DECISION_ELIGIBLE` only when **every** check
passes; otherwise `INTRADAY_DECISION_WITHHELD` with classified reasons.

## 3. What can become the latest authoritative decision

The **complete priced target** produced by the live event cycle — specifically
the R47 outcome of `api.reallocation_proposal` for the candidate's bound
evidence. Two outcomes are promotable, and **both are real decisions**:

* `HOLD_CURRENT_BOOK` — a complete feasible alternative was built and priced,
  and does not clear the switching hurdle after cost. Holding **is** the
  decision. It carries no position recommendations: the priced target is
  precisely the alternative the system decided *not* to take.
* `CHANGE_RECOMMENDED` — the feasible target clears the hurdle. It carries the
  proposal owner's own allocation actions verbatim as position-level
  recommendations (`HOLD` / `REDUCE` / `EXIT` / `REPLACE_OUT` / `REPLACE_IN` /
  `ADD` / `INCREASE`), and `manual_review_required: true`.

`TRUE_BLOCKER` and a withheld complete target are **not** promotable and are
refused with their own canonical codes.

## 4. What remains research-only

* Any cycle that ends `NO_NEW_INFORMATION`, `INFORMATION_NOT_MATERIAL`,
  `DUPLICATE_TRIGGER_SUPPRESSED` or `BLOCKED`.
* `REASSESSED_NO_CHANGE` **without a priced target**: the reassessment's own
  economics cleared nothing, so the R47 switching economics (transition cost,
  risk before/after, concentration, turnover budget) were never computed. Phase
  F requires them, so a governed HOLD must be a *priced* HOLD. Such a cycle
  remains current signal state and updates no recommendation.
* Everything the R53.1 prospective intraday lane emits. It is
  `PROSPECTIVE_INTRADAY` research evidence and is never summed with, or
  promoted into, the governed decision lane.

## 5. Supersession

ONE total ordering, used by the gate **and** by the read:

```
(eligible session, decision timestamp, provenance rank, identity hash)
```

A later session always outranks an earlier one; within a session the later
decision timestamp wins; a tie on both is broken by provenance (the
session-terminal `GOVERNED_DAILY_CYCLE` outranks a `GOVERNED_INTRADAY`
promotion) and finally by identity hash, so the order is total and
reproducible. A stale, incomplete or older assessment can therefore never
supersede a newer governed decision — the gate refuses it *and* the writer
refuses it.

Supersession is an **append**: the new record names the old one in
`supersedes_decision_id`. The prior record is never mutated.

`load_governed_portfolio_decision` returns the later of (a) the newest
persisted governed record and (b) the DRC-governed decision **projected** from
the Release-29.5 contract, under that same ordering. That is how a newer DRC
decision supersedes an intraday one without the DRC being modified.

### Idempotency

`candidate_identity_hash` covers the **evidence only** — book, session,
portfolio / economic / corporate-action state, ranking identity, HOC,
reassessment, target and outcome. It deliberately excludes the event cycle's
run id, its wall clock and the materiality trigger fingerprint: two different
triggers that reach the same conclusion from the same evidence are the same
decision, and re-deciding it would be churn dressed as governance. The trigger
fingerprint is still **bound into the record** — it is provenance an auditor
needs — it is simply not part of identity.

## 6. Withheld-reason taxonomy (Phase J)

Canonical codes are reused verbatim, never re-spelled:

`PORTFOLIO_DECISION_NO_ACTIVE_BOOK` · `PORTFOLIO_IDENTITY_STALE` ·
`MARKET_DATA_STALE` · `OWNED_DATA_NOT_CONFIRMED` ·
`POINT_IN_TIME_INTEGRITY_FAILURE` · `RANKING_IDENTITY_MISMATCH` ·
`HOC_IDENTITY_MISMATCH` · `REASSESSMENT_IDENTITY_MISMATCH` ·
`TARGET_IDENTITY_MISMATCH` · `SWITCHING_ECONOMICS_INCOMPLETE` ·
`TRUE_BLOCKER` · `CHANGE_CANDIDATE_WITHHELD` · `SUPERSEDED_BY_NEWER_DECISION` ·
`DUPLICATE_CANDIDATE` · `EXECUTION_PRECEDENCE` ·
`CANDIDATE_EVIDENCE_INCOMPLETE`

Each withheld reason names its code, the failing check, the check group, the
owner that decided it and a human detail. A live signal is never a generic
BLOCKED: the operator can see the signal **and** exactly why it was not
promoted.

## 7. The two clocks, and `OWNED_DATA_NOT_CONFIRMED`

The gate distinguishes two entirely different statements:

1. **The book's own session is owned-confirmed.** `operational_close_valid` is
   not false, and both the operational mark and the latest completed close are
   at or beyond the eligible session. If not, the candidate is **withheld** with
   `OWNED_DATA_NOT_CONFIRMED` — the code reused verbatim from
   `api.workflow_state`.
2. **The NEXT expected completed session is not yet owned-confirmed.** This is
   the workflow's `WAITING_FOR_OWNED_DATA` state and it concerns the
   **operational close clock**. It does **not** withhold an intraday decision —
   it is recorded in the record's evidence provenance as
   `expected_session_owned_data_confirmed: false` with an explicit note, and it
   is never cleared, weakened or consumed as intraday decision evidence.

That distinction is the entire point: on 2026-09-01 the book was validly closed
and marked to 2026-08-31 while the workflow correctly waited for 2026-09-01's
owned data. An intraday decision against the 2026-08-31 book using live
2026-09-01 signals is legitimate; advancing the close from that same evidence
would not be. The governed lane has **no** write path to the close: only
`api.daily_close` advances the operational mark, and every governed record
states so.

## 8. Latency (Phase G)

Measured by the existing latency owner (`api.event_signal_refresh`) from
**authoritative persisted timestamps only**:

| Stage | Source |
|---|---|
| `observation_received_at` | newest owner-stamped event ingest |
| `event_cycle_started_at` | run payload `generated_at` |
| `signal_refresh_completed_at` | step `REFRESH_AFFECTED_INPUTS.finished_at` |
| `scoring_completed_at` | step `MEASURE_DELTAS.finished_at` |
| `hoc_completed_at` | step `HOLDING_OPPORTUNITY_COST.finished_at` |
| `reassessment_completed_at` | step `PORTFOLIO_REASSESSMENT.finished_at` |
| `target_completed_at` | step `REALLOCATION_PROPOSAL.finished_at` |
| `governance_gate_completed_at` | gate `evaluated_at` |
| `governed_decision_persisted_at` | record `recorded_at` |

Intervals (`observation_to_signal`, `signal_to_reassessment`,
`reassessment_to_governed`, `observation_to_governed`) are computed **only**
when both endpoints exist. A stage that persists no stamp is named in
`missing_measurements` and `latency_measurement_complete` is false. Nothing is
fabricated.

**Measured today (R53/R54 evidence):** the decision chain itself is ~7.3 s
median; `oldest_event_to_reassessment_seconds` on the live 2026-09-01 cycles is
in the minutes. **Detection dominates** — the collection cadence, not the
decision chain, is the remaining bottleneck to true near-real-time operation.
Cadence is therefore deliberately NOT changed here (`cadence_enabled` stays
false in the audit): raising it is only justified once the measured
`observation_to_governed_seconds` series proves detection is the binding
constraint.

## 9. Zero-base proof (Phase I)

No second optimizer was created. The existing R47/R50 constrained-reallocation
kernel already answers *"if all investable capital were cash at this decision
timestamp, what portfolio should we own?"*: `INCUMBENCY_POLICY =
NO_INVESTMENT_PRIVILEGE_ONLY_PRICED_TRANSITION_COST`, the ideal target comes
from `api.zero_base_target` before any constraint binds, and
`current_holdings_privileged` is false. The gate **binds** all three into every
governed record and **withholds** if the policy is not intact. It never
re-derives them.

## 10. Emission-slot contract (Phase H) — NOT a defect

Investigated against repository and live evidence:

* the frozen R53 contract is `EMISSION_SLOTS_ET = ("10:00", "12:00", "14:00")`
  with `SLOT_GRACE_MINUTES = 15`;
* `scripts/install_intraday_emission_task.ps1` installs **four** triggers and
  documents the fourth verbatim: *"16:20 (post-close scoring pass — emission is
  structurally refused outside a slot; the run only scores matured
  session-close outcomes)"*;
* `scripts/run_intraday_emission.py` scores matured predictions at step 3
  **before** attempting emission at step 4, so the 16:20 run does real work and
  its `NOT_AN_EMISSION_SLOT` emission result is the designed outcome;
* the live attempts ledger for 2026-09-01 confirms it: 12:00 ET EMITTED 36,
  14:00 ET EMITTED 36, a 14:02 re-fire appended 0 (idempotent), and 16:20 ET
  `NOT_AN_EMISSION_SLOT` with 0 appended;
* the Scheduled Task is healthy (`Ready`, S4U, four enabled triggers,
  `LastTaskResult 0`, `NumberOfMissedRuns 0`).

**Conclusion: no defect, no code change, no Scheduled Task change.** The
nominal-slot + bounded-tolerance + idempotency design the brief describes as
preferable is already exactly what is implemented. Exact-minute matching is not
in use: a 15-minute grace window is. (The 10:00 ET slot has no attempt row on
2026-09-01 because the task was installed after it; the scheduler recorded zero
missed runs.)

## 11. Safety boundary — unchanged and enforced

A governed promotion updates the authoritative **recommendation**. It does not,
and structurally cannot: mutate the portfolio, approve anything, create an
order plan, create an order or fill, call a broker, promote a model, activate a
sleeve, run the Daily Close, or advance the operational mark. Recording a
governed decision requires `CONFIRM_GOVERNED_INTRADAY_DECISION` — a **system**
token, deliberately distinct from the operator approval token
`CONFIRM_PORTFOLIO_REBALANCE_DECISION`, which one can never satisfy the other.

## 12. Storage

The governed lane lives in `governed_decisions.json` + `governed_index.json`
under the **same** `PAPER_TRADER_PORTFOLIO_DECISION_DIR` root as the manual
operator lane — same owner, same root, same append-only atomic writer, two
files. They are two different objects: the manual lane records what the
**operator** decided about an approvable proposal; the governed lane records
which **recommendation** is currently authoritative and where it came from.
Sharing one pointer index would make `load_decision_record` return a system
record where a caller expects an operator approval, and `derive_decision_state`
would then demand review of a question the system had already settled.

## 13. Verification

* `tests/test_release54_1_governed_intraday_decision.py` — 95 tests covering
  all 40 required behaviours plus the emission-slot contract.
* `scripts/audit_architecture.py::check_release54_1_governed_intraday_decision`
  — 24 BLOCKING invariants: one gate, one owner, one ordering; no second
  economics engine; no execution / approval / promotion / scheduler reach; the
  withheld taxonomy complete; `OWNED_DATA_NOT_CONFIRMED` reused verbatim; the
  governed lane never touches the manual pointer or the operational mark; the
  R53.1 emission-slot contract unchanged. A duplicate governance owner
  appearing anywhere in `api/` or `engine/` fails the build.

## 14. Live read-only dry run — what the gate says TODAY

Run against the live read models with no write of any kind (candidate + gate
only; `record_governed_decision` was never called):

```
book                 alpha_paper_book_1      eligible session 2026-08-31
workflow             WAITING_FOR_OWNED_DATA  (expected 2026-09-01 unconfirmed)
event cycle          PROPOSAL_AVAILABLE_FOR_MANUAL_REVIEW  evt_9dc21bc6695cbf60
candidate decision   HOLD_CURRENT_BOOK
verdict              INTRADAY_DECISION_WITHHELD    checks 37/38
withheld             REASSESSMENT_IDENTITY_MISMATCH
failing check        CYCLE_REASSESSMENT_IS_THE_CANDIDATE
                     cycle ad61cb61... vs persisted 292f6a53...
```

Everything else passes — including `BOOK_SESSION_OWNED_CONFIRMED` while the
workflow is legitimately waiting for the NEXT session's owned data, which is
exactly the two-clock separation this release exists to make safe.

**The one failing check is real, and it found something.** Proven from the
store and from `api.portfolio_reassessment.persist_reassessment`:

* only ONE reassessment artifact exists for 2026-08-31 — `292f6a53...`,
  written by the governed DRC at `2026-09-01T01:08:13Z`;
* the live event cycle computed a **different** reassessment, `ad61cb61...`,
  from newer signals;
* the economic fingerprint is UNCHANGED (`b57594bb...`), so Stage-21 case (a)
  applies — *"same economic state, different research inputs -> the prior
  artifact still describes the portfolio. Immutability wins: reject, never
  overwrite."* The cycle's result returns `CONFLICT_REJECTED` and is **not
  persisted**.

So the cycle's conclusion has no immutable artifact behind it, and the gate is
right to refuse to govern an assessment that was never persisted. A promotion
would otherwise bind a governed decision to a hash no artifact carries.

This is a **governance precondition**, distinct from the latency bottleneck:

* Stage-20/21 keys the reassessment artifact by `(book, eligible session)` and
  appends a new version only when the **economic** state changed;
* under continuous collection, "the prior artifact still describes the
  portfolio" remains true about the **portfolio** and becomes false about the
  **answer** — the premise of an active manager is precisely that new signals
  can change the answer while the book is unchanged;
* so intraday promotion is available today **after an economic change** (a
  close advances marks/NAV → a new version is appended → the cycle's
  reassessment IS the persisted artifact), and correctly withheld for a
  pure-signal, same-session change.

Fixing this means changing a canonical owner's immutability contract, which is
deliberately out of scope here: R54.1 ships the gate, and the gate reports the
precondition in the operator's own words rather than papering over it.

## 15. Recommended next slice

**R54.2 — version the reassessment artifact on SIGNAL identity as well as
economic identity.** Extend `persist_reassessment`'s Stage-21 case (b) so a
same-session reassessment whose `universe_scoring_hash` / `hoc_assessment_hash`
changed appends a NEW immutable version instead of `CONFLICT_REJECTED` — the
same append-never-rewrite shape Stage 21 already uses for an economic change,
and nothing is lost. That is the single change that turns the gate's 37/38 into
a live governed intraday decision, and it is bounded: one persistence rule, its
Stage-20/21 tests, and the R54.1 invariants already in place.

**R54.3 — make the DRC write its terminal decision through the same governed
writer.** Today the DRC-governed decision is *projected* into the ordering from
the Release-29.5 contract, which is correct for supersession but leaves the
ledger holding only intraday records. Wiring `api.daily_research_cycle` to call
`record_governed_decision(provenance=GOVERNED_DAILY_CYCLE, ...)` at terminal
completion makes the governed lane the single durable history of every
authoritative recommendation, with no projection step. One call site.

**Not yet:** raising detection cadence. Measured today,
`oldest_event_to_reassessment_seconds` is ~35,244 s against a ~348 s cycle and
a ~7 s decision chain — detection still dominates by two orders of magnitude,
but the honest reading is that the *event corpus* is what is old, not that the
poll interval is wrong. Raise cadence only once the
`observation_to_governed_seconds` series this release publishes shows detection
latency, not corpus age, as the binding constraint.
