# Release 48 — Active Portfolio Manager Operating System

**One canonical operator workflow · Today/Portfolio simplification · governed
activation of R47 reallocation**

Paper-only. Preview-first. Manual review mandatory. No broker, no live order, no
automation, no model promotion.

---

## 1. The defect

The pipeline was consolidated (one workflow owner, one canonical five-stage
cycle, one command bar), but the OPERATOR still walked the sequence by hand:

```
know that the Daily Close comes first  ->  click it (token 1)  ->  wait
    ->  know that the Daily Research Cycle comes second  ->  click it (token 2)
    ->  find the decision
```

Two mutations, two confirmation tokens, and one piece of order-knowledge that
lived only in the operator's head. The workflow owner always knew the sequence;
the operator was the one executing it.

## 2. The replacement

ONE operator concept:

```
RUN PORTFOLIO CYCLE
```

`api/portfolio_cycle.py` is the ONE orchestration entrypoint
(`POST /v1/operations/portfolio-cycle/run`, token `RUN_PORTFOLIO_CYCLE`). It is
a SEQUENCER, never a second decision engine:

* WHAT runs next is decided solely by `api.workflow_state` — re-read verbatim
  between steps, never re-derived;
* HOW each step runs is owned solely by the existing writers
  (`api.daily_close.run_daily_close`, then
  `api.daily_research_cycle.run_daily_research_cycle`), each invoked AT MOST
  once per run, with its own token supplied server-side and every delegated
  write attributed `portfolio_cycle:<requested_by>`;
* WHERE it stops is the governance boundary: the run always halts at the
  governed portfolio decision (`PROPOSAL_READY` / `HOLD_CURRENT_BOOK` /
  `TRUE_BLOCKER` via the canonical decision object). It can reach no approval
  token, no order plan, no desk write, no R46 research (audit-enforced on the
  module's code with docstrings stripped).

Stops are named, never generic: `DECISION_PRESENTED`,
`WAITING_FOR_SESSION_CLOSE`, `CYCLE_ALREADY_RUNNING`, `RECOVERY_REQUIRED`,
`STATE_DID_NOT_ADVANCE`, `OWNER_REPORTED_BLOCKER` (the owner's own words). An
unrecognised state runs nothing (fail closed). `GET /v1/operations/portfolio-cycle`
is the read-only status: what a run would do right now, and why.

## 3. The presentation

`api.workflow_state.build_operator_command` — the unchanged ONE next-action
owner — now PRESENTS the cycle action whenever a normal-path mutation is due:
`primary_action_kind = PORTFOLIO_CYCLE`, label "Run the portfolio cycle", one
token, with the decided underlying step beside it (`cycle_underlying_kind`),
so the presentation is simplified while the decision stays fully visible. The
state-level `primary_action` still names the underlying owner verbatim for
audit surfaces and existing contracts. The UI dispatcher obeys the presented
kind; the off-Today execution refusal is unchanged.

## 4. Today / Portfolio

* **Today** is the command center: a status strip answering readiness
  (operational mark · eligible session · NAV · research severity → Research ·
  collection chip), the portfolio-result card headlined by the CANONICAL close
  state (the legacy membership-difference material now folds into its existing
  compatibility disclosure), the canonical portfolio decision with its
  HOLD/REDUCE/EXIT/REPLACE/ADD summary, and at most one primary action. The
  collection-infrastructure card is demoted off Today (header chip +
  System · Audit keep the state; every id stays a live write target).
* **Portfolio** promotes the R47 card out of the collapsed advanced fold onto
  the primary surface as **Current vs Recommended Portfolio**: current book →
  zero-base ideal (analytical reference) → constraint adjustments → BEST
  FEASIBLE target (the operational recommendation) → switching economics →
  approval + execution state, with the constraint inventory and adjustment
  ledger in Audit/Advanced folds.
* **§15**: `api.operational_book`'s informational string no longer presents the
  monthly checkpoint as a portfolio cadence — it is a "model-governance review
  checkpoint", and the audit blocks regressions.

## 5. What did not change

The Stage-18/19 double manual gate; NEXT_CLOSE-only paper execution; the R47
optimizer, hurdles and outcomes; the R47 decision-outcome ledger; monthly-input
blocking at the month boundary (recalibration never runs inside the cycle);
R46 research (hash-verified byte-identical, 2177 files); every ledger and store
(operational aggregate byte-identical across the whole development window).

## 6. Surfaces

| Route | Method | Owner |
|---|---|---|
| `/v1/operations/portfolio-cycle` | GET | `api.portfolio_cycle.load_portfolio_cycle` |
| `/v1/operations/portfolio-cycle/run` | POST | `api.portfolio_cycle.run_portfolio_cycle` |

## 7. Tests and audit

`tests/test_release48_operator_workflow.py` — 42 tests: contracts, the full
plan map over every overall state (fail-closed unknowns), sequencing,
single-invocation, owner-blocked stops, the operator presentation, structural
governance (code-only scans), and the UI wiring.
`scripts/audit_architecture.py` adds `check_release48_portfolio_cycle` with 23
blocking invariants (strict mode).
