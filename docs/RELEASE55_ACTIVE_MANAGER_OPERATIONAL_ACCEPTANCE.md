# Release 55 — Active Manager Operational Acceptance & Operator Clarity

**One operator action, three operator answers, one assessment clock.**

Status: implemented, targeted-gate tested, not committed.
Base HEAD: `c0df3b1` (R54.4).
Branch: `stage19-controlled-rebalance`.

This is an **operational-acceptance and presentation slice**. It creates no
portfolio engine, no decision owner, no scheduler, no reassessment framework, no
NAV calculation, no freshness calculation and no workflow framework. Every value
it publishes is an existing owner's value, and every new function is either a
projection of an already-decided state or a delegation to the owner that already
owns the concept.

---

## 1. What was wrong operationally

The system was behaving correctly. The **operator experience** was not.

### 1.1 The Today page published raw material, not answers

To learn what to do on the morning of 2026-09-03 an operator had to hold six
things in their head simultaneously and reconcile them:

| Fact on screen | Owner | What it meant |
|---|---|---|
| Governed portfolio decision | `api.portfolio_decision` | HOLD, decided 2026-09-02 |
| Last portfolio reassessment | `api.portfolio_reassessment` | current for 2026-09-02 |
| Latest live/intraday reassessment | `api.event_signal_refresh` | ran 12:26 PM ET, concluded HOLD |
| Operational vs research clocks | two owners | mark 2026-09-02, research 2026-09-03 |
| Session-close status | `engine.market_session` | Sep-3 session still open |
| Legacy scheduled-review clock | `api.operational_book` | checkpoint 2026-08-01 passed |

Six correct facts, no stated conclusion. The operator was doing the composition
the backend should have done.

### 1.2 A legacy clock was presented as an operator problem

The most misleading line on the page was:

> **Stale / missing:** Scheduled full review due — legacy `api.daily_action_gate`
> clock; the portfolio reassessment itself is current for the eligible session

That sentence is *true* and it was still *wrong to show there*, because the
**STALE / MISSING list asserts that something the operator depends on is not in
the state it should be in** — and nothing was.

The causal chain, all of it provable from the live payload:

1. `api.operational_book` owns a MONTHLY scheduled-review clock (Phase 27B.9)
   and, since Release 46.6, publishes what that clock **is**:

   ```json
   "review_scope": "SCHEDULED_MODEL_RECALIBRATION_CHECKPOINT",
   "review_is_the_governing_portfolio_cadence": false,
   "portfolio_reassessment_cadence": "AFTER_EVERY_MATERIAL_SIGNAL_REFRESH"
   ```

   It is the floor for the **third** operating cycle (model recalibration). It
   is explicitly **not** the cadence at which the portfolio is reassessed.

2. `api.daily_action_gate` forwarded the **date** (`next_scheduled_full_review =
   2026-08-01`) and dropped the **scope**. Every downstream consumer had to guess
   which of the three canonical operating cycles that date belonged to.

3. `api.workflow_state.classify_assessment` guessed the wrong one. Its precedence
   ladder placed `review_overdue` **above** the actual currency test, so:

   ```
   assessment_date        2026-09-02   ==  eligible_date 2026-09-02
   assessment_age_sessions         0
   next_review_date       2026-08-01   <   today 2026-09-03
   -> status = OVERDUE
   ```

   A reassessment that was current for the eligible session was classified
   OVERDUE **because a model-recalibration checkpoint had passed**.

4. `api.active_manager_state._stale_components` put the OVERDUE token on the
   operator's STALE / MISSING list. R54.2.4 had already made the *label*
   truthful; it left the *row* on the operator surface, so a truthful sentence
   still read as a problem.

**This was a second clock competing with the canonical portfolio-reassessment
clock, and it won.**

### 1.3 Measured latency existed and was reported unavailable

`api.event_signal_refresh` persists four real stage timestamps on every event
cycle. `_decision_latency_block` read latency **only** from a governed intraday
*promotion* record. No promotion happened on 2026-09-03 (the conclusion was
HOLD), so the block reported `available: false` — while the same payload carried:

```
signal_refresh_completed_at  2026-09-03T16:28:42.462667+00:00
scoring_completed_at         2026-09-03T16:28:42.468987+00:00
hoc_completed_at             2026-09-03T16:28:48.773940+00:00
reassessment_completed_at    2026-09-03T16:28:50.940532+00:00
```

### 1.4 Eleven internal states were the operator's vocabulary

`OVERALL_STATES` is the right vocabulary for the state machine and the wrong
vocabulary for a human. Nothing published "here is the one thing to do now" in
words an operator could act on without knowing the architecture.

---

## 2. What changed

Five bounded changes, each inside the owner that already owns the concept.

### 2.1 The scheduled-review scope travels with the date (`api.daily_action_gate`)

`evaluate_daily_action_gate` gained `scheduled_review_scope` and publishes
`scheduled_review_scope` — the clock owner's declaration, copied field by field
by `_review_scope`. The loader reads the date and the scope from the **same**
`canonical_state` block, so they can never travel apart again.

An owner that published no declaration yields `available: false` and all-None
fields. **Silence is never read as a repair.**

### 2.2 A non-governing schedule no longer decides the assessment status (`api.workflow_state`)

`classify_assessment` gained `schedule_governs_portfolio_cadence`. Only an
**explicit `False`** from the clock's owner demotes the schedule; `None` and
`True` preserve the pre-R55 ladder exactly.

When demoted:

* `review_due` / `review_overdue` are still returned — the recalibration
  obligation is **scoped, never hidden**;
* the status is decided by assessment currency alone
  (`MISSING > INCONSISTENT > STALE > CURRENT`);
* the verdict carries `status_decided_by`, `schedule_decided_status` and
  `schedule_governs_portfolio_cadence`, so the authority boundary travels in the
  payload.

Proven on the exact live dates:

| | status | decided by |
|---|---|---|
| pre-R55 | `OVERDUE` | `api.operational_book` (checkpoint clock) |
| R55 | `CURRENT` | `api.workflow_state.classify_assessment` |

A genuinely older assessment is still `STALE`, a future-dated one still
`INCONSISTENT`, an absent one still `MISSING`.

### 2.3 One operator action contract (`api.workflow_state`)

Seven codes, one priority order, a **total** map from the eleven overall states:

```
rank 0  BLOCKED                    P1 / P3.6 / P6 — nothing else is safe
rank 1  WAIT_FOR_OWNED_DATA        P3   — no confirmed owned session
rank 2  RUN_PORTFOLIO_CYCLE        P3.7 / P4 / P5 — a session needs processing
rank 3  RESUME_RESEARCH_CYCLE      P4.5 — the close stands, research is owed
rank 4  REVIEW_PORTFOLIO_PROPOSAL  P7   — a required manual review
rank 5  WAIT_FOR_SESSION_CLOSE     P2   — nothing outstanding, session open
rank 6  MONITOR_PORTFOLIO          P8 / P9 — terminal: fully processed
```

`build_operator_action` is a **projection**, not a second priority engine:

* `_decide_overall` remains the ONE authoritative priority order and is untouched;
* the function consults no date, no session status and no clock (guarded by a
  test that greps its body);
* exactly **one** refinement: `RESEARCH_CYCLE_REQUIRED` + an outstanding
  post-close governed-research obligation → `RESUME_RESEARCH_CYCLE`, read
  verbatim from `build_research_obligation`;
* an unknown state **fails closed to BLOCKED** — never to a reassuring action;
* `executes` / `confirmation_required` / `execution_contract` are **copies** of
  the canonical operator command, so the contract can enable no control the
  command has not already authorised.

### 2.4 Three operator answers (`api.active_manager_state`)

`operator_answer` composes, once, the only three questions the first screen may
answer:

| | Question | Source |
|---|---|---|
| 1 | WHAT IS THE CURRENT AUTHORITATIVE PORTFOLIO DECISION? | `latest_governed_portfolio_decision` + `canonical_portfolio_decision` |
| 2 | WHAT HAS CHANGED SINCE THAT DECISION? | `live_reassessment_lane` + `live_information` |
| 3 | WHAT SHOULD THE OPERATOR DO NOW? | `operator_guidance.operator_action` |

The GOVERNED lane and the LIVE / INTRADAY RESEARCH lane stay permanently
distinct. Answer 2 carries `is_authoritative: false` and
`changes_the_authoritative_decision`, which is true only when the backend itself
says the gate promoted the cycle. **The research lane can never masquerade as the
governed decision.**

Two facts operators consistently misread are stated explicitly in the payload:

* *why non-held events matter* — material information about an asset the book
  does not hold still changes the opportunity cost of what it does hold, so zero
  affected holdings is a **normal outcome, not a failure**;
* *why this is not the decision* — the live lane re-underwrites continuously and
  never becomes authoritative without a promotion.

Operator-facing times are spelled in Eastern by the **clock owner**
(`engine.market_session.format_operator_timestamp`, added beside the existing
`to_eastern`). No surface and no browser converts a timezone. A missing or
unparseable stamp yields `None`, never "now".

### 2.5 Two component surfaces, and measured latency

`_stale_components` now returns `(stale, advisory)`:

* `stale_components` — the operator's STALE / MISSING list (a real problem);
* `advisory_components` — AUDIT-ONLY: true, retained in full, and explicitly
  `is_operator_problem: false`.

The legacy scheduled-review row is demoted **only** when both of the currency
owner's own facts hold: the assessment is current for the eligible session, and
the token was attributed to the legacy schedule. Nothing is deleted — the raw
owner token, the truthful display label, the self-explaining detail and the
demotion reason all travel with the advisory row.

`_decision_latency_block` now delegates to
`api.event_signal_refresh.measure_decision_latency` over the persisted stage
timestamps when no governed record carries a latency. A promoted cycle's own
record still wins outright. The projection module performs no timestamp
arithmetic at all (guarded: no `total_seconds`, no `fromisoformat`).

### 2.6 The acceptance contract

`build_acceptance_contract(state)` is a pure function of a composed payload —
ten named rows, each quoting the owner that decided it, `PRESENT` only when that
owner's key fact exists:

```
COLLECTION  SIGNAL  SCORING  HOC  REASSESSMENT  GOVERNANCE
GOVERNED_DECISION  OPERATIONAL_BOOK  NEXT_ACTION  LATENCY
```

A stage that persisted nothing is `MISSING` and says which owner owed it. **A
missing fact is never inferred from a neighbouring stage.**

---

## 3. The canonical owner of the operator action

```
api.workflow_state._decide_overall        THE priority order (P1..P9), unchanged
        |
        v
api.workflow_state.build_operator_action  THE operator action contract (R55)
        |
        +--> api.workflow_state payload         operator_action
        +--> api.active_manager_state           operator_guidance.operator_action
        |         |
        |         +--> operator_answer.what_to_do_now
        |
        +--> api/ui/index.html  R55_REGION      rendered VERBATIM
```

Exactly one module defines `build_operator_action` (asserted). The UI derives,
re-orders and overrides nothing.

---

## 4. Live Sep-3 acceptance findings

Captured by authenticated read-only GETs at ~12:30 PM ET on 2026-09-03 against
the running R54.4 backend on port 8001. **Nothing was mutated.**

### 4.1 The chain, end to end

| Stage | Evidence | Verdict |
|---|---|---|
| Information collection | `RUNNING`, worker `IDLE`, heartbeat 34 s, last observation `15:55:08Z` | ran |
| Material-event determination | 20 material events, 18 newer than the persisted reassessment, 0 affecting held names | ran |
| Signal refresh / scoring | `FULL_UNIVERSE_RECOMPUTE`, 234 scored, snapshot `f44d487122b45d0b1bf05746`, basis `2026-09-02` | ran, **full universe** |
| Holding opportunity cost | `hoc_completed_at 16:28:48.773940Z`, hash `702c599e…` | ran, persisted |
| Portfolio reassessment | `reassessment_completed_at 16:28:50.940532Z`; hash `029df5cd…` — **identical to the Sep-2 governed artifact** | ran; **no new artifact**, evidence identity unchanged |
| Intraday governance gate | `intraday_governance.available = false`, `evaluated = null`, `verdict = null` | **DID NOT RECORD A VERDICT** |
| Governed decision authority | `CURRENT_NO_CHANGE`, `GOVERNED_DAILY_CYCLE`, session `2026-09-02`, `persisted: false` | authority projected, not persisted |
| Active manager state | `WAITING_FOR_SESSION_CLOSE`, 1 stale component | consistent |
| Workflow state | `WAITING_FOR_SESSION_CLOSE`, no blockers | consistent |
| Operator presentation | `HOLD CURRENT PORTFOLIO`, `next_action.kind = NONE` | consistent |

### 4.2 The specific questions answered

* **What triggered it?** `MATERIAL_SIGNAL_CHANGED`, decided by
  `engine.event_materiality` via `api.event_signal_refresh`; run
  `evt_34dbe756217df98f` at `16:26:31.784900Z`.
* **Was full-universe scoring used?** Yes — `FULL_UNIVERSE_RECOMPUTE`, 234 names.
  The `ranking_date` of `2026-09-02` is the **point-in-time data basis**, not the
  recompute wall-clock time.
* **Did HOC run and persist?** Yes, both.
* **Did the reassessment run and persist?** It **ran**; it produced **no new
  persisted artifact**, because its evidence identity hash was byte-identical to
  the standing Sep-2 governed reassessment. Under authority rule 6 (identical
  evidence identity is the SAME decision) that is correct reuse, not a failure.
* **Did the governance gate evaluate it?** **No.** `evaluated` is `null`. The
  lane's `governance_state: ELIGIBLE` is the composition owner's *inference* from
  "a candidate exists and nothing withheld it", and the lane says so:
  *"no persisted gate verdict is recorded on this cycle's payload."* This is the
  one genuine gap the reconnaissance found.
* **Why HOLD?** `expected_net_improvement 0.0` against `net_improvement_hurdle
  0.05`; `expected_one_way_turnover 0.0`; `expected_transaction_cost_usd 0.0`;
  0 positions changing.
* **Why no supersession?** Two independent reasons, either sufficient: the
  candidate's evidence identity equals the standing decision's, and no gate
  verdict promoted anything.
* **Did anything fail or get withheld?** No failure. `withheld_reason_codes` and
  `failing_checks` are both empty — because the gate never ran, not because it
  passed.

### 4.3 Measured latency (R55, from the persisted stamps)

```
observation (15:55:08.827Z) -> signal refresh complete (16:28:42.462Z)   2013.6 s
signal refresh              -> reassessment complete   (16:28:50.940Z)      8.5 s
reassessment                -> governed decision                          MISSING
observation                 -> governed decision                          MISSING
```

`missing_measurements = [governance_gate_completed_at,
governed_decision_persisted_at, target_completed_at]`.
`latency_measurement_complete = false`. **Missing stays MISSING.**

The 2013.6 s figure is the interval between the newest admitted observation and
the completion of the signal refresh that consumed it. It is a real measurement
of this cycle, not a service-level claim: it spans the collection cadence, not
processing time.

### 4.4 Acceptance contract over the live payload

`7 / 10 PRESENT`. Missing: `GOVERNANCE` (no gate verdict — §4.2),
`NEXT_ACTION` and `LATENCY` (the running backend is the pre-R55 R54.4 runtime and
publishes neither field yet; both become PRESENT on the next restart).

---

## 5. What was removed or demoted, and why

| Item | Action | Why |
|---|---|---|
| `portfolio_reassessment (OVERDUE)` on the STALE / MISSING list | **Demoted** to `advisory_components`, surface `AUDIT_ADVANCED_ADVISORY`, `is_operator_problem: false` | The assessment is current for the eligible session; the only thing moving the token was the legacy checkpoint clock, whose owner declares it does not govern the portfolio cadence |
| The three-clock strip, Lane B card, identities and hashes | **Moved** into the collapsed Today `Advanced details` disclosure | Diagnostics belong in Audit / Advanced; nothing is deleted and every field still renders |
| `review_due` / `review_overdue` | **Kept**, and now scoped | The model-recalibration obligation is real; it is a *recalibration* obligation, not a portfolio one |

**Nothing was hidden.** The demoted row keeps its raw owner token, its truthful
label, its self-explaining detail and a quoted reason for the demotion. A row the
legacy clock cannot explain — a genuinely `STALE`, `MISSING`, `NOT_RUN` or
`INCONSISTENT` assessment — stays on the operator surface unchanged.

### The authority boundary, stated

> The **portfolio-reassessment currency clock** is owned by
> `api.workflow_state.classify_assessment` and advances with the eligible market
> session. The **scheduled-review checkpoint clock** is owned by
> `api.operational_book`, is the floor for **model recalibration**, and may never
> decide a portfolio-assessment status, raise an operator obligation, or appear
> on a normal operator surface. Where the two disagree, the reassessment clock is
> authoritative and the checkpoint clock is an audit advisory.

---

## 6. Post-close operator acceptance procedure (Phase F)

Run **after** the Sep-3 session closes and the normal Portfolio Cycle has been
executed through the canonical path. Every step below is read-only.

```powershell
$h = @{ 'X-API-Key' = $env:PAPER_TRADER_SERVICE_API_KEY }
$ams = Invoke-RestMethod -Uri 'http://127.0.0.1:8001/v1/operations/active-manager-state' -Headers $h
```

| # | Assertion | Where to read it |
|---|---|---|
| 1 | Sep-3 operational close is authoritative | `operational_book.latest_completed_close_date == "2026-09-03"` and `operational_close_valid == true` |
| 2 | DRC completed, or names the exact safe blocker | `research_obligation.research_obligation_state` is `NO_RESEARCH_OBLIGATION`, else `true_blockers` names it |
| 3 | An exact persisted HOC exists | `acceptance.rows[HOC].status == "PRESENT"` with a non-null `assessment_hash` |
| 4 | An exact persisted reassessment exists | `acceptance.rows[REASSESSMENT].status == "PRESENT"`; `reassessment_id` carries the Sep-3 session |
| 5 | The DAILY producer delegated to `api.portfolio_decision` | `latest_governed_portfolio_decision.owner == "api.portfolio_decision"` |
| 6 | A persisted governed decision exists in the ONE ledger | `latest_governed_portfolio_decision.persisted == true` **and** `governed_decisions.json` exists under the decisions store |
| 7 | Provenance is the daily cycle | `latest_governed_portfolio_decision.provenance == "GOVERNED_DAILY_CYCLE"` |
| 8 | HOLD or CHANGE follows the economics | compare `decision` against `expected_net_improvement` vs `net_improvement_hurdle` in `portfolio_reassessment` |
| 9 | Re-reading reuses the decision | re-GET; `record_id` is unchanged and no second row appended |
| 10 | Nothing was executed | `safety.created_orders / created_fills / approved_anything / promoted_model` all `false`; `execution_safety.pending_paper_orders` unchanged |
| 11 | Today and Active Manager agree | `operator_answer.current_decision.headline` equals the Today decision headline; `operator_answer.what_to_do_now.action` equals `operator_guidance.operator_action.action` |
| 12 | Intraday ordering stays coherent | any intraday row's `eligible_market_session` ≤ the daily row's; a same-session tie resolves to `GOVERNED_DAILY_CYCLE` |

One command renders rows 1–12 from the live route:

```powershell
& C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe `
    C:\Users\binis\paper_trader\scripts\r55_operator_acceptance.py
```

**Do not backfill Sep-2.** The Sep-2 governed conclusion already exists in the
DRC manifest; R54.4 writes the ledger forward from the next governed run.

---

## 7. Remaining blocker

**The intraday governance gate records no verdict on a non-promoting cycle.**

On 2026-09-03 the gate produced no `evaluated`, no `verdict`, no
`withheld_reason_codes` and no `failing_checks`. The composed lane therefore
infers `ELIGIBLE` and says, honestly, that it is an inference. Operationally the
outcome was correct — the candidate's evidence identity matched the standing
decision, so no promotion was possible — but *"the gate said this candidate was
eligible and declined to promote it"* and *"the gate never ran"* are different
facts, and today only the second is provable.

R55 does not fix this: it is a **decision-lane persistence** change, not a
presentation change, and it belongs to `api.portfolio_decision`. R55 makes it
**visible and unambiguous** instead of silently absent — the acceptance contract
reports `GOVERNANCE: MISSING` and names the owner that owed the verdict.

This does not block R55. It is the natural next slice.

---

## 8. Gates

| Gate | Result |
|---|---|
| `tests/test_release55_active_manager_operational_acceptance.py` | **85 passed** |
| Targeted regression perimeter (27 files) | **1254 passed, 0 failed** |
| `scripts/audit_architecture.py --strict` | **exit 0** (32 new R55 blocking invariants; all 32 reported fields pinned) |
| `git diff --check` | **clean** |
| Browser acceptance, 1920x1080 | three answers lead the page and fit the first screen; diagnostics collapsed; ONE navigation-only button; no blank buttons; no `alert()`/`confirm()`; no run ids / artifact ids / hashes / UTC on the primary surface; safety badges visible; no horizontal scroll |

Not committed, not pushed, backend not restarted.

---

## 9. Safety

Read-only throughout. No write, no order, no fill, no approval, no model
promotion, no sleeve activation, no scheduler, no provider call, no prediction
call. The live backend was never restarted, stopped or mutated; no Daily Close,
Daily Research Cycle or Portfolio Cycle was run. All tests are hermetic.

Because the running backend is still the committed R54.4 runtime, the **backend**
half of this slice is not yet serving: `operator_answer`, `operator_action`,
`advisory_components`, `acceptance` and the repaired assessment status appear
after the next canonical restart. The **UI** half is served immediately from
disk and degrades to exactly the pre-R55 Today page while the old payload is
being served (the answer panel stays hidden when `operator_answer` is absent).
