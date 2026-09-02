# Release 54.2.1 — Missed eligible session recovery

**A daily operating system must not require the operator to be present during one
exact hour.**

Built over the committed R54.2 head `9dbbcdb` ("Version same-session portfolio
reassessments by evidence (R54.2)"). Windows PowerShell only, single agent, no
subagents. Nothing committed, nothing pushed, no live backend restart, no
production store write.

---

## 1. What happened to 2026-09-01

On the evening of **2026-09-01** the operator could not run the Daily Close. The
workflow was correctly fail-closed: owned EOD data for the session had not been
published yet, and the market-session owner reported `WAITING_FOR_OWNED_DATA` /
`OWNED_DATA_NOT_CONFIRMED`. That is the system working as designed — it refuses
to close a session it cannot price.

On the morning of **2026-09-02 at 09:01 ET** the same payload read:

| Owner | Field | Value |
|---|---|---|
| `engine.market_session` | `session_status` | `BEFORE_SESSION_CLOSE` |
| | `expected_completed_market_date` | `2026-09-01` |
| | `latest_eligible_completed_market_date` | `2026-08-31` |
| | `latest_confirmed_owned_data_date` | `2026-08-31` |
| `api.daily_close` | `latest_completed_close_date` | `2026-08-31` |
| `api.workflow_state` | `overall_state` | `WAITING_FOR_SESSION_CLOSE` |
| | `primary_action` | *Wait for the market session to close* |
| | `operator_command.next_text` | *No action required right now.* |

…while the **Daily Close owner**, which probes the owned provider, said in the
same minute:

```
close_status              DAILY_CLOSE_DUE
headline                  SEPTEMBER 1 EOD DATA READY - RUN DAILY CLOSE
provider_readiness        READY   provider_latest_date 2026-09-01
latest_eligible_market_date  2026-09-01
```

The 2026-09-01 obligation was not resolved, not forfeited and not blocked. It
**silently ceased to exist** when the wall clock rolled forward, and no operator
control could recover it. Two authoritative owners disagreed about whether there
was any work to do, and the surface the operator reads believed the wrong one.

Confirmed by filesystem evidence (read-only): the last Daily Close artifact,
Daily Research Cycle manifest, holding-opportunity-cost assessment and portfolio
reassessment all carry `2026-08-31`; there is no `2026-09-01` artifact of any
kind, and no governed portfolio decision for that session.

---

## 2. The cause, precisely

`_decide_overall` gate **P2** asserts *"the latest eligible completed session is
already fully processed; nothing to do but wait"*, and it tested that claim
against `eligible_session_closed`:

```python
eligible_session_closed = (operational_close_valid
                           and close_date >= eligible_market_date)
```

`eligible_market_date` is the **owned-data-CONFIRMED** session. Owned-data
confirmation is the persisted desk-mark date, and the desk-mark date only
advances when a close runs.

> **An unclosed completed session can never confirm itself.**
> The very session the operator missed is the one the eligible date cannot name.

So P2 asked its question about `2026-08-31` — which genuinely *was* fully
processed — and answered truthfully about the wrong session. Every downstream
surface then repeated the claim, including the operator command's
"No action required right now."

This is not a market-session bug. `engine.market_session` answered its own
question correctly: *which completed session do the owned marks confirm?* The
missing concept was a different question that nobody owned.

---

## 3. The rule

The obligation is a **calendar** question asked against the **close journal**,
and both owners already publish their half of the answer:

```
last_closed_session  = api.daily_close        (which session was processed)
through              = engine.market_session  (expected completed session)
recovery_session     = the OLDEST completed session strictly in between
```

Three properties follow from the shape of the rule, not from extra code:

* **Today's still-forming session can never be selected.** The upper bound is the
  EXPECTED COMPLETED session, which is never today's open one.
* **There is no `today - 1` arithmetic anywhere.** Both bounds are owner answers.
* **Weekends and holidays work automatically**, because the enumeration is the
  market-session owner's weekday calendar plus its authoritative non-session set.

`engine.market_session.completed_sessions_after(last_closed, through=…,
non_sessions=…)` performs the enumeration (pure: no clock, no store, no IO).
`api.workflow_state.build_session_recovery(...)` composes the state from it.

### Recovery states (frozen vocabulary)

| State | Meaning | Next action |
|---|---|---|
| `NO_CATCH_UP_REQUIRED` | the close journal has reached the expected session | none |
| `CATCH_UP_REQUIRED` | a completed session was never closed | **Run the Portfolio Cycle** |
| `CATCH_UP_WAITING_FOR_OWNED_DATA` | the session owner reports owned data has not reached the session | wait / refresh owned data |
| `CATCH_UP_BLOCKED` | a named blocker (state inconsistency, blocked research cycle, backlog beyond the calendar owner's bound) | resolve the named blocker |

### Owned-data readiness, stated honestly

`api.workflow_state` is deliberately **probe-free** — it never calls the owned
provider. It therefore reports what the persisted marks say and names the owner
that settles the rest:

| `recovery_data_state` | Meaning |
|---|---|
| `CONFIRMED` | the persisted owned marks already reach the recovery session |
| `UNVERIFIED_UNTIL_CLOSE_REVALIDATES` | probe-free; the Daily Close revalidates the provider server-side and writes nothing if the session is genuinely unpublished |
| `OWNED_DATA_LAGGING` | the SESSION owner affirmatively reports owned data has not reached the expected session |

The distinction matters and is the reason the live case is actionable rather than
stuck: **an un-ingested mark is a publish/ingest gap, not proof that the provider
lacks the session.** (This is the same principle Phase 29D.1 already froze for
holidays: *the absence of same-day owned data is never a non-session.*) The
operator presentation places the close owner's real provider answer beside the
obligation, so Today can say `Owned data: READY` without the workflow owner ever
probing.

---

## 4. Priority

The ladder the release brief asked for is the ladder the state machine already
had; only the P2 predicate and the P3.7 disjunct changed.

```
P1   INCONSISTENT_STATE                       genuine safety blocker
P2   WAITING_FOR_SESSION_CLOSE                ← now suppressed by catch_up_required
P3   WAITING_FOR_OWNED_DATA                   unconfirmed current session
P3.5 RESEARCH_CYCLE_RUNNING                   never interrupt an in-flight cycle
P3.6 RESEARCH_CYCLE_BLOCKED                   a named fix comes first
P3.7 READY_FOR_DAILY_CLOSE                    ← now also fires on catch_up_required
P4…  research / reassessment / terminal region
```

A missed completed session **names real work**, so it outranks every state that
claims nothing is outstanding — exactly as an unclosed *eligible* session already
did. It does **not** outrank an in-flight cycle, a named blocker or an
inconsistency, so no recovery can run over an unresolved problem.

No new overall state was invented: recovery resolves through the existing
`READY_FOR_DAILY_CLOSE`, whose primary action is already the one portfolio cycle.

---

## 5. One orchestration path, with the session BOUND

Recovery runs through the **existing** `POST /v1/operations/portfolio-cycle/run`.
There is no `recover-close`, no `backfill-close`, no force-close control, no
second Daily Close orchestrator and no manual repair script. The architecture
audit fails the build if one appears.

What is new is that the **server binds the session**:

```
api.workflow_state    names session_recovery.recovery_session   (oldest missed)
        ↓ read verbatim
api.portfolio_cycle   recovery_binding(workflow) → bound_market_date
        ↓ passed as target_market_date
api.daily_close       _apply_session_binding() narrows the clock's expectation
        ↓ ONE value
        provider probe · desk-mark completed_through · model-input refresh ·
        idempotency key · progress document
```

`latest_eligible = clock["expected_market_date"]` was already the single value
every date-dependent close step reads, so binding it is surgical rather than
invasive.

**The binding may only ever look backward.** A target newer than the clock's
expected completed session is **refused, never clamped** — a silently-clamped
date is how a bound recovery turns back into an unbound close of a different
session. A refused binding returns `AWAITING_MARKET_CLOSE`, writes nothing, and
stops the cycle with the close owner's own words.

The operator supplies nothing: `target_market_date` is not a request field on any
route, and the UI has no date input.

---

## 6. Oldest first

`recovery_session` is always `missed_completed_sessions[0]`. Machine offline
Monday and Tuesday, operator returns Wednesday → the system requires **Monday**,
then Tuesday. It never skips to the newest session, and it never processes an
older session under a newer session's marks: the bound close fetches owned bars
`completed_through` the bound date, so a recovered session is priced from its own
immutable owned EOD history.

A backlog longer than `engine.market_session.MAX_MISSED_SESSIONS` (15) is not a
missed session — it is an outage. It is reported as `CATCH_UP_BLOCKED` with
`missed_session_backlog_truncated`, rather than silently producing a hundred
obligations.

---

## 7. Idempotency and stage-aware resume

Nothing new was built for this: the composed owners were already idempotent, and
the binding makes their idempotency keys name the right session.

| Situation | What happens |
|---|---|
| the close for the session already ran | the close journal advances → `NO_CATCH_UP_REQUIRED`; the cycle stops before invoking any owner |
| close done, DRC missing | `RESEARCH_CYCLE_REQUIRED` → the cycle resumes at the DRC; the close is not re-run |
| close + DRC done, no decision | the cycle stops at `DECISION_PRESENTED` — governance begins where the cycle ends |
| fully complete | repeated runs invoke no owner and write nothing |
| a re-run of a processed date | `api.daily_close` returns `ALREADY_PROCESSED`; no duplicate mark, performance row or decision row |

History is never rewritten. The recovery projection is a pure read — no store, no
write, no owner mutation — and a close the owner does not classify as
operationally complete is **not** treated as a closed session, so a failed
attempt can never erase the obligation it failed to discharge.

---

## 8. Point-in-time safety

* Sep-1 evidence is never manufactured from Sep-2 data: the bound close prices
  the session from owned EOD bars `completed_through` the bound date.
* A forward binding is refused, so a still-forming session can never be
  substituted for the session being recovered.
* If owned data genuinely is not available, the close probes, fails closed with
  `WAITING_FOR_MARKET_DATA`, and writes nothing — and the recovery state says
  `CATCH_UP_WAITING_FOR_OWNED_DATA` with no safe action offered.
* No hindsight reconstruction, no synthetic backdating, no operator-supplied
  date.

---

## 9. Operator UX

### Today

A conditional banner (`#today-session-recovery`) appears **only** when the
backend reports an unclosed completed session:

```
CATCH UP REQUIRED
Sep 1, 2026 was not closed.

Missed session  Sep 1, 2026     Last closed  2026-08-31     Owned data  READY

                                          Run the Portfolio Cycle ↓
```

Every string, date and verdict is rendered verbatim from
`operator_presentation.session_recovery`. The browser performs **no** date
arithmetic, selects no session and offers no backfill, force-close or date-entry
control. The banner renders **no execution control of its own** — Today still has
exactly ONE primary-action render site, and it already carries the catch-up
wording because the workflow owner specialised the primary action. The banner's
button is navigation to that one button.

When data is not ready the banner turns red, states the reason and says *"No
action is currently safe"* with no button at all.

### The four Phase-J corrections

| # | Was | Now |
|---|---|---|
| J.1 | `TODAY −$270` while the operational mark was 2026-08-31 | `LAST CLOSED SESSION · Aug 31, 2026 −$270`. The label is decided by the backend from `pnl.valuation_date` vs `current_session.calendar_date`; it reads `TODAY` only when the mark really is the current calendar day. |
| J.2 | EXIT / REDUCE / ADD / INCREASE at full prominence beside an authoritative HOLD | the analysis stays (it is what makes HOLD explainable) under `REJECTED FEASIBLE ALTERNATIVE` + *"NOT THE RECOMMENDED PORTFOLIO — the switching hurdle was not cleared"*, with `NOT RECOMMENDED` chips on Changes and Target and no approval CTA. |
| J.3 | a raw proposal-ready token beside a review-looking CTA while the decision was HOLD | Audit keeps the raw tokens under `RAW / NON-AUTHORITATIVE DIAGNOSTIC STATE`, states the authoritative decision beside them, flags the disagreement, and the renderer emits no button at all. |
| J.4 | bare `FRESH` | `Eligible session (governed)` with *"Fresh for the governed session — 2026-08-31"*, plus `Session recovery` as its own readiness row. **No freshness calculation changed** — only what it is called. |

A missed session is stated as **DEGRADED work**, never as an incident: it does
not block the portfolio decision surface.

---

## 10. Ownership (unchanged boundaries)

| Concept | Owner |
|---|---|
| trading calendar, expected session, missed-session enumeration | `engine.market_session` |
| whether a session was operationally processed | `api.daily_close` |
| the catch-up STATE and the recovery session | `api.workflow_state` |
| sequencing the owners; passing the binding | `api.portfolio_cycle` |
| presenting the obligation + the provider answer | `api.operator_presentation` |
| republishing it read-only | `api.active_manager_state` (delegates; computes nothing) |

`scripts/audit_architecture.py :: check_release54_2_1_missed_session_recovery`
fails the build on a second catch-up state owner, a second calendar owner, a
second recovery orchestrator, a recovery/backfill/force-close route, a binding
exposed as a request field, a projection that computes its own session date, UI
recovery date arithmetic, or any new automation/order path. All 25 fields block
`--strict`.

---

## 11. Safety

Unchanged and re-asserted by the tests: paper only, preview first, manual review,
automation OFF, no order, no fill, no broker, no approval, no model promotion, no
recalibration. Recovery decides **which session** the one manual cycle binds —
nothing more.
