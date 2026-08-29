# Release 47 — Constraint-Respecting Active Reallocation

**Governed paper execution · Portfolio decision outcome tracking**

Paper-only. Preview-first. Manual review mandatory. No broker, no live order, no
automation, no model promotion.

---

## 1. The defect

Before Release 47 the portfolio pipeline could reach a dead end:

```
unconstrained target  ->  constraint breach  ->  WITHHELD  ->  keep the current book
```

A sector cap, a name cap, a risk-contribution limit, a concentration limit, a
liquidity cap and a turnover budget are all **normal portfolio constraints**. A normal
constraint must **change the solution**. It must not freeze the portfolio, and it must
never hand the incumbent holdings a victory they did not earn: *"we could not compute a
compliant target"* is not a finding that the current book is the best use of capital.

## 2. The replacement

```
unconstrained ideal target
    -> apply the mandatory constraints
    -> SOLVE the best FEASIBLE constrained target
    -> compare it against the current book
    -> account for switching cost, risk, liquidity and turnover
    -> PROPOSAL_READY / HOLD_CURRENT_BOOK / TRUE_BLOCKER
```

## 3. The three authoritative outcomes

| Outcome | Meaning | Approvable? |
|---|---|---|
| `PROPOSAL_READY` | A feasible target exists and is sufficiently better than the current portfolio after risk, cost, liquidity and turnover. | Yes — after explicit manual approval, then a second explicit order-plan confirmation. |
| `HOLD_CURRENT_BOOK` | A feasible alternative exists, was fully computed and priced, and its expected improvement does not justify switching. | **No.** It is a decision the system has already taken, not outstanding work. |
| `TRUE_BLOCKER` | No trustworthy portfolio decision can be made. | No. Fail closed. |

Precedence: a declared true blocker → an empty feasible set → the switching hurdle →
ready. **A reshaping constraint can never reach the first two.**

## 4. The constraint inventory (declared as data, not prose)

`engine.constrained_reallocation.constraint_inventory()` returns every mandatory limit
with its value, the object it is judged on, its owner and — the point of the release —
what it DOES to the solution.

### Reshaping constraints (13) — each one changes the answer

| Code | Judged on | What it does |
|---|---|---|
| `ELIGIBLE_UNIVERSE_ONLY` | per name | Held names outside the eligible universe are exited (mandatory) and their capital is redistributed. |
| `LONG_ONLY` | per name | Negative weight is clipped to zero. |
| `GROSS_EXPOSURE_CAP` | portfolio | Exposure above 100 % is scaled back; the residual is cash. |
| `NAME_WEIGHT_CAP` | per name | The name is capped; the excess goes to the next-best eligible opportunities. |
| `SECTOR_WEIGHT_CAP` | per sector | The sector is capped by trimming its weakest names; the excess is redistributed outside it. |
| `RISK_CONTRIBUTION_CAP` | per name | The position is reduced to the compliant level; the released capital is redistributed. |
| `LIQUIDITY_PARTICIPATION_CAP` | per name | The position is capped at what the book could actually trade. |
| `LIQUIDITY_ADV_FLOOR` | per name | An illiquid candidate is skipped and the **next feasible candidate** is used. |
| `CONCENTRATION_INCREASE_LIMIT` | complete target | Weight moves from the largest positions into the next-best names. |
| `TURNOVER_BUDGET` | complete target | The best feasible target **inside** the budget is solved. |
| `MIN_POSITION_WEIGHT` | per name | Dust is dropped to cash rather than proposed. |
| `MAX_POSITION_COUNT` | complete target | The weakest names beyond the limit are dropped and their capital is redistributed inside it. |
| `CASH_BOUNDS` | portfolio | Cash is a real asset choice; the bound reshapes how much capital may remain unallocated. |

### True blockers (6) — the only conditions that stop a decision

`CRITICAL_STALE_OR_MISSING_MARKET_DATA` · `POINT_IN_TIME_INTEGRITY_FAILURE` ·
`NAV_ACCOUNTING_UNRECONCILED` · `IMPOSSIBLE_LIQUIDITY_OR_CAPACITY` ·
`NO_FEASIBLE_PORTFOLIO_UNDER_MANDATORY_CONSTRAINTS` ·
`REQUIRED_MANUAL_AUTHORIZATION_MISSING`

A code that is not declared a true blocker is **refused** when offered as one
(`misclassified_blockers`), and an unknown code is never promoted to a blocker —
promoting the unknown is exactly how a normal cap became a freeze.

## 5. How the re-optimisation works

Deterministic, stdlib only, every tie broken by ticker:

1. **Eligibility / long-only** — ineligible held names become mandatory exits.
2. **Per-name cap** — the tightest of the name cap, the participation cap and the ADV
   floor. An illiquid name gets a cap of zero and is skipped.
3. **Sector cap** — trim from the weakest name in the sector upward.
4. **Risk-contribution cap** — scale the name by `cap / share`.
5. **Position count** — drop the weakest names beyond the limit.
6. **Gross exposure** — scale back from the weakest name upward.
7. **Redistribute** — greedy in descending value order over a laminar constraint family
   (name inside sector inside the total budget), which is exactly optimal for that
   family. Capital with no feasible destination stays in **cash**.
8. **Minimum position size** — dust to cash.
9. **Concentration** — move weight from the largest positions into the next-best names.
10. **Cash bounds.**
11. **Turnover budget** — see below.
12. **Verify** — full constraint verification, independent of the solver that produced
    the weights. Only an empty feasible set produces `NO_FEASIBLE_PORTFOLIO`.

An **effective cap** is tightened by each repair, so step 7 can never hand capital
straight back to a position a constraint just cut.

### The turnover budget

Trades are split in two, and the split is the whole point:

* **MANDATORY** legs implement a constraint (an ineligible or illiquid holding must
  leave; a name above its cap must come down). They are taken FIRST and, if they alone
  exceed the budget, they are still taken — *a budget may not trap the book in a
  constraint breach.* That case is recorded as
  `budget_subordinated_to_mandatory_constraints`, because two mandatory constraints in
  conflict is a decision a person owns.
* **DISCRETIONARY** legs are ordered by **score improvement per unit of one-way
  turnover** against the current book's own weighted score, and taken while the budget
  lasts. The marginal leg is scaled to fit exactly, so the budget is used, not merely
  respected.

The density is a first-order ordering criterion **only**
(`ordering_is_first_order_only: true`); the final target's economics are then measured
exactly by the canonical owners.

## 6. Incumbency

The canonical question is *"if all investable capital were cash now, what feasible
portfolio should we own?"* A current holding therefore receives **no investment
privilege**. The feasibility solve cannot see which names are held except to measure
distance from them. Incumbency enters in exactly one place, priced:

```
INCUMBENCY_POLICY = "NO_INVESTMENT_PRIVILEGE_ONLY_PRICED_TRANSITION_COST"
switching_economics.incumbency_advantage_applied = "TRANSITION_COST_ONLY"
```

## 7. The switching hurdle

Explicit, frozen, deterministic: the net score improvement after modelled transition
cost must clear `min_switching_net_improvement = 0.05` — the same percentile points the
per-name and portfolio hurdles already use, so a basket of individually-rejected
switches cannot pass in aggregate. It is declared before any decision is measured and
is never tuned on realised outcomes (`hurdle_frozen: true`,
`hurdle_tuned_on_outcomes: false`).

A **mandatory exit is a constraint, not a bet**, and is not subject to it.

**One owner per number.** This kernel owns the HURDLE, not the score, the turnover or
the cost. The proposal engine passes the values its own signal / turnover / risk blocks
already produced, and the payload reports `delegated_inputs`, so the proposal can never
publish two answers for the same quantity. Expected return is still never fabricated
(`EXPECTED_RETURN_STATE = NOT_CALIBRATED`).

## 7b. The per-holding form of the same three limits

**Found by running the release against the live book in a browser.** The 2026-08-28
operator surface read *"MANUAL REVIEW REQUIRED — review the portfolio constraint
breach"* and produced **no proposal at all**, on seven per-name breaches:

```
ABNB:SECTOR_WEIGHT_BREACH   CVS:SECTOR_WEIGHT_BREACH   DXCM:SECTOR_WEIGHT_BREACH
EXPE:SECTOR_WEIGHT_BREACH   ITW:SECTOR_WEIGHT_BREACH   LH:SECTOR_WEIGHT_BREACH
MNST:RISK_CONTRIBUTION_BREACH
```

`engine.portfolio_reassessment` was promoting `CURRENT_NO_CHANGE` to
`MANUAL_REVIEW_REQUIRED` whenever a held name breached its own name-weight,
sector-weight or risk-contribution cap. That stopped the pipeline **before any target
was built**, so the constraint re-optimiser never saw the book — the same dead end this
release exists to remove, one layer upstream of the proposal engine.

Those three limits are `RESHAPES_THE_SOLUTION` in the canonical inventory. The answer
to a breached cap is to **cap that name and redistribute the released capital**, which
is an allocation — and this kernel never allocates. So a per-holding breach now:

* raises `HELD_NAME_CONSTRAINT_BREACH_REQUIRES_TARGET` and routes to
  `PROPOSAL_READY` — it **asks for a target**;
* is **not** a blocker;
* stays fully visible as `held_name_constraint_breaches` (it is the *reason* for the
  ask), travels with the reassessment summary, and is surfaced on the operator's
  portfolio-attention object.

This is the same split Release 29.3 already made for the four portfolio-level limits,
applied to their per-name form, and it is declared in
`constraint_ownership()["per_name_deferred_to_complete_target"]`.

It authorises nothing. `PROPOSAL_READY` only lets the proposal owner build a
**review-only** target, which still faces the complete-target limits and both manual
gates — and if the repaired target still breaches, it is withheld exactly as before.

## 8. WITHHELD is narrowed in scope, never weakened

`STATE_WITHHELD` is now reached only when the **repaired** target still breaches — an
empty feasible set, or two mandatory constraints in conflict. It remains un-approvable
at every layer (kernel, read API, decision owner, workflow surface), exactly as
Release 29.3 requires, and the audit asserts that contract unchanged.

## 9. Governed paper execution (unchanged)

```
RESEARCH CYCLE -> HOC -> REASSESSMENT -> CONSTRAINED OPTIMISATION -> PROPOSAL
    -> MANUAL REVIEW -> APPROVE_FOR_PAPER_REBALANCE (+ CONFIRM_PORTFOLIO_REBALANCE_DECISION)
    -> ORDER PLAN REVIEW
    -> CONFIRM_APPROVED_PORTFOLIO_REBALANCE_ORDER_PLAN
    -> PAPER ORDERS (existing desk lifecycle)
    -> NEXT_CLOSE settlement (no same-close hindsight fill)
    -> RECONCILIATION
```

Two independent manual gates, both backend-enforced. Idempotent: confirming the same
order plan twice creates zero duplicate orders. Release 47 added no execution path and
no execution shortcut.

## 10. Portfolio decision forward evidence

The moment a governed rebalance creates its orders — and only then — ONE immutable
record is frozen:

* previous portfolio, proposed target, executed target;
* reasons, expected improvement, costs, risk, constraints, market date, model state;
* **both forward paths**, each with the decision session's own reference prices:
  * `EXECUTED_PAPER_PORTFOLIO` — carries the transaction cost the switch actually paid;
  * `COUNTERFACTUAL_HOLD_PORTFOLIO` — pays nothing, because not trading costs nothing.

Measured forward against the desk's own settled marks at sessions **strictly after** the
decision session:

| Measure | |
|---|---|
| incremental P&L | executed value − hold value |
| incremental return | the same, over NAV at decision |
| incremental drawdown | executed max drawdown − hold max drawdown |
| transaction cost | what the switch paid |
| risk-adjusted improvement | annualised, only past a minimum observation count |
| holding-period opportunity cost | what holding would have earned that we gave up |

The result is `PORTFOLIO_DECISION_ALPHA`.

**Why the counterfactual is frozen and never reconstructed.** A hold portfolio rebuilt
later is worthless: by then we know which names went up, and every judgement made while
rebuilding it is made with hindsight. So the hold basket, its reference prices and its
NAV are frozen at decision time, before a single forward price exists. A record whose
evidence is not strictly later than its decision session returns
`POINT_IN_TIME_VIOLATION` and measures nothing. There is deliberately **no code path**
that can create a counterfactual after the fact.

**Separation.** `PORTFOLIO_DECISION_ALPHA` measures an executed capital decision on the
operational paper book. Release-46 challenger alpha measures research signals in shadow.
Stage-21 outcome evidence measures whether a *recommendation* was any good. The three
ledgers are separate and are **never summed**.

## 11. Surfaces

| Route | Method | Owner |
|---|---|---|
| `/v1/operations/constrained-reallocation` | GET | `api.reallocation_proposal.load_constrained_reallocation` |
| `/v1/operations/portfolio-decision-outcomes` | GET | `api.portfolio_decision_outcome.load_portfolio_decision_outcomes` |

The UI card `#r47-constrained` renders, in order: CURRENT PAPER BOOK → IDEAL TARGET →
CONSTRAINT ADJUSTMENTS → BEST FEASIBLE TARGET → SWITCHING ECONOMICS → GOVERNED PROPOSAL
→ APPROVAL STATE → EXECUTION STATE, with the constraint inventory and the adjustment
ledger in collapsed Audit / Advanced sections. The browser holds no threshold, no cost
rate, no cap and no hurdle; it never infers an outcome from a state name; and it
contains no approve, order or execute control.

## 12. Safety

Paper only · preview only · review only · manual review · no orders · no live orders ·
no broker · automation off · no model promotion · no recalibration · no policy write ·
no spending.

## 13. Owners

| Module | Role |
|---|---|
| `engine/constrained_reallocation.py` | Pure kernel: constraint inventory, feasible re-optimisation, switching hurdle, the three outcomes. |
| `engine/portfolio_decision_outcome.py` | Pure kernel: freeze the decision record with both paths; measure them forward. |
| `api/portfolio_decision_outcome.py` | Composition + persistence: freeze at the execution boundary (idempotent), read contract. |
| `engine/reallocation_proposal.py` | Unchanged ownership; gained the re-optimisation seam and the outcome. |
| `api/reallocation_proposal.py` | Unchanged ownership; gained the R47 read contract and summary keys. |
| `api/portfolio_decision.py` | Gained the `HOLD_CURRENT_BOOK` lane and its refusal. |
| `api/rebalance_execution.py` | Gained the decision-evidence freeze. Execution semantics unchanged. |
| `api/workflow_state.py` | Gained the canonical `HOLD_CURRENT_BOOK` state and two semantic-consistency violations. |

## 14. Tests

`tests/test_release47_constrained_reallocation.py` — 83 tests covering every scenario
the release specification names (A–M), the live per-holding freeze found in the
browser, and the classification, purity, safety, route and architecture contracts.
`scripts/audit_architecture.py` adds `check_release47_constrained_reallocation` with
42 blocking invariants.
