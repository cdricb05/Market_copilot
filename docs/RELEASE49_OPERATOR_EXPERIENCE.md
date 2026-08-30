# Release 49 — Operator Experience Rebuild

**Today command center · Portfolio task workspace · ONE reconciled operator
presentation · Advanced / Audit hard separation**

Paper-only. Preview-first. Manual review mandatory. No broker, no live order, no
automation, no model promotion. Release 49 changed no Alpha science, no optimizer,
no holding, no NAV, no order, no fill, no approval and no historical evidence.

---

## 1. The defect

Release 48 fixed the operating process (ONE portfolio-cycle action, ONE
orchestration path, ONE workflow owner). The application still did not FEEL that
simple. Live acceptance showed: a giant material-information table ahead of the
portfolio decision; a Portfolio route ~2 viewports long mixing the decision with
HOC tables, the model target snapshot, the paper desk, Stage-19 machinery and
methodology; six raw states competing on one screen (`MANUAL_REVIEW_REQUIRED`,
`PORTFOLIO CONSTRAINT BREACH`, `STATE NOT_RUN`, `NO PROPOSAL YET - RUN THE DAILY
RESEARCH CYCLE`, `REBALANCE_NO_PROPOSAL` …); a "Current vs Recommended" card that
rendered dashes and told the operator to rerun an immutable historical session;
and 300 visible badges on Portfolio.

## 2. The replacement

```
authoritative owners  ->  api.operator_presentation (RECONCILE, never recompute)
                      ->  Today: 4 sections, 1 decision, <= 1 action
                      ->  Portfolio: Overview | Reallocation | Performance | Audit & Details
```

### 2.1 ONE reconciled operator presentation

`api/operator_presentation.py` — `GET /v1/operations/operator-presentation`
(read-only). It consumes `api.workflow_state` (overall state, operator command,
canonical portfolio decision, decision lane, reassessment lane, operational
state, evidence, data gaps), the Release-47 constrained-reallocation read
contract (outcome, best feasible target, switching economics, approval and
execution state), the Daily Close P&L block, the material-information feed, the
decision-outcome ledger and the collection lifecycle, and translates their
already-decided states into ONE operator truth:

| Field | Vocabulary |
|---|---|
| `system_readiness.state` | `READY` · `DEGRADED` · `BLOCKED` — every degraded item says whether it blocks the portfolio decision |
| `portfolio_decision.state` | `CYCLE_REQUIRED` · `REALLOCATE` · `HOLD` · `BLOCKED` · `AWAITING_APPROVAL` · `AWAITING_CONFIRMATION` · `AWAITING_NEXT_CLOSE` · `OUTCOME_ACCRUING` |
| `next_action.kind` | `PORTFOLIO_CYCLE` (the only executing kind, carried verbatim from the Release-48 presented contract) · `REVIEW_REALLOCATION` · `REVIEW_ORDER_PLAN` · `REVIEW_BLOCKER` · `WAIT` · `NONE` |

Plus `headline`, `explanation`, `portfolio_snapshot` (NAV, today, cumulative,
cash, positions, vs SPY, drawdown — verbatim), `decision_summary` (exits /
reductions / replacements / additions from the proposal owner's own counts,
turnover, cost, net improvement vs hurdle, risk and concentration before/after),
`alerts_summary` (count, holdings-relevant count, top three), `decision_outcome`,
`historical_context`, `safety` (one mode line), `raw_states` (audit only) and
`sources`. It recomputes NOTHING: no NAV, target, decision, constraint, HOC,
proposal, execution state or research verdict (`recomputes_nothing: true`, audit
enforced on its code).

### 2.2 Historical / pre-R47 reconciliation

The 2026-08-28 session was decided under the prior workflow: seven per-holding cap
breaches recorded as a manual-review blocker, the governed cycle complete, no
target solved. Under Release 47 §7b those codes are reshaping constraints, never
true blockers. The presentation detects this from the owners' own facts (a BLOCKED
canonical decision whose every blocker code lies outside the constraint
inventory's declared `true_blocker_codes`, on a session the governed cycle
completed without a target) and presents:

```
HISTORICAL DECISION — 2026-08-28
The 2026-08-28 session was completed under the prior decision workflow. A
portfolio constraint breach was recorded (7 holdings: ABNB, CVS, DXCM, EXPE, ITW,
LH, MNST). No governed reallocation target exists for that historical session,
and the historical record will not be rewritten.
Next: Run the portfolio cycle after the next eligible market close.
```

No date is hard-coded, no proposal is fabricated, nothing is rerun
(`history_rewritten: false`, `proposal_fabricated: false`,
`rerun_of_historical_session_instructed: false`).

### 2.3 Collection DEGRADED is not a portfolio blocker

`system_readiness` states `DEGRADED — collection degraded. The portfolio decision
remains valid.` with `blocks_portfolio_decision: false` on the collection item;
`BLOCKED` is reserved for an inconsistent state, a named workflow blocker, a
blocking data gap or an operational evidence incident.

## 3. Today — four primary sections

1. **System status band** — READY / DEGRADED / BLOCKED · service · eligible
   session · operational mark · NAV · collection · research (one line).
2. **Portfolio decision** — the dominant element: one state, one sentence,
   decision economics when relevant, at most ONE primary action. An executing
   action (`Run portfolio cycle`) goes through the ONE canonical Release-48
   dispatcher; review actions navigate.
3. **Portfolio snapshot** — NAV, today, cumulative, cash · positions, vs SPY,
   drawdown.
4. **Attention** — `N material events · M affect current holdings`, the top three
   with ticker / description / relevance, and `View all material information →`
   (the full table moved to System · Audit → Operating diagnostics).

The legacy Today cards (`#cc-root`) and the operator command bar stay in the DOM
as live write targets and are hidden on Today by CSS; every legacy id, loader and
test selector still resolves.

## 4. Portfolio — four task views on one route

`#portfolio-manager/overview | reallocation | performance | audit`
(`data-pm-view` on the tab; CSS decides what each view shows).

* **Overview** — current portfolio (KPI hero + book strip), the SAME reconciled
  decision as Today, **Current vs Best Feasible Target** (the R47 card
  re-presented as two columns; an intentional empty state — `NO CURRENT FEASIBLE
  TARGET` — when no target exists, never a grid of dashes; the zero-base ideal as
  a one-line analytical reference), decision economics, and a small decision-
  outcome card only when measured evidence exists.
* **Reallocation** — Decision · Changes (EXIT / REDUCE / REPLACE / ADD / INCREASE
  / RETAIN from the R47 owner's allocation rows, replacement arrows only from the
  owner's `replacement_relationship`) · Target · Economics · Governance
  (REVIEW → APPROVE → CONFIRM → AWAIT NEXT CLOSE → EXECUTED → OUTCOME ACCRUING,
  current step emphasised). The manual gates remain backend endpoints.
* **Performance** — the existing six PTC charts and KPI hero, unchanged owner.
* **Audit & Details** — the raw reassessment card, HOC counts and the full HOC
  table, addition candidates, the raw reallocation proposal, the controlled
  rebalance and its order plan, corporate-action integrity, decision evidence,
  the 13 checks, rebalance lineage, all-holdings audit detail, the model target
  snapshot review, the alpha-book plan, the paper trading desk (maintenance
  buried), the zero-base target, methodology, the raw decision payload and the
  raw owner states behind the presentation.

## 5. Safety presentation

The global header states the mode once (`Paper · Manual review · Automation
off`); the presentation carries `PAPER · MANUAL APPROVAL · AUTOMATION OFF`. Badge
walls are gone from the normal Today and Portfolio Overview surfaces; every badge
stays in the DOM and the full guarantees stay under Audit.

## 6. What did not change

The Stage-18/19 double manual gate; NEXT_CLOSE-only paper execution; the R47
optimizer, hurdles, outcome vocabulary and decision-outcome ledger; the R48
portfolio cycle and its ONE dispatcher; Markets; Research; R46 research
(hash-verified byte-identical); every operational store (byte-identical across the
development window).

## 7. Surfaces

| Route | Method | Owner |
|---|---|---|
| `/v1/operations/operator-presentation` | GET | `api.operator_presentation.load_operator_presentation` |

## 8. Tests and audit

`tests/test_release49_operator_presentation.py` — contracts, builder purity,
historical / true-blocker / cycle / reallocate / hold / execution-lifecycle
reconciliation, fail-closed unknown states, system readiness, verbatim snapshot
and alerts, raw-vocabulary absence, Today / Portfolio structure, demotions, the
R47 / R48 preservation and the strict audit.
`scripts/audit_architecture.py` adds `check_release49_operator_presentation` with
42 blocking invariants (strict mode).
