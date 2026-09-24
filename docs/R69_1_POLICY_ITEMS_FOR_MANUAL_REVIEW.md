# R69.1 — two policy items raised for manual review

Neither item was changed by this release. No threshold, score, obligation or
historical proposal economic was altered. Both need an operator decision.

> **Status after R69.2 (2026-09-24)**
>
> * **Item 1 (the dynamic risk-contribution denominator) is still OPEN.** R69.2
>   changed no threshold and no basis. It made the effect VISIBLE instead:
>   the selected-target panel now publishes the before and after limits, the
>   covariance-universe size on both sides, risk on both the invested and the
>   capital basis, concentration, and names every breach that closed while its
>   position did not move. On 2026-09-22 that is AMD 14.98% → 20.73% and DDOG
>   13.87% → 20.65% at unchanged weights. The operator decision below is
>   unchanged and still theirs to make.
> * **Item 2 (a selected MINIMUM_REPAIR has no implementable order plan) is
>   RESOLVED.** `engine.selected_target` is the owner those weights lacked;
>   `api.portfolio_decision` freezes the complete book into the governed
>   selection, and `api.rebalance_execution` builds the order plan from it. The
>   sentence "the minimum repair is reviewable and selectable but not
>   executable" at the end of §2 no longer describes the system.

---

## 1. The risk-contribution cap has a dynamic denominator (P0-D)

### What the governed policy actually says

`engine.holding_opportunity_cost.risk_contribution_limit` is declared once:

```
limit = risk_contribution_excess_multiple / n_covariance_names     (3.0 / N)
basis = EXCESS_MULTIPLE_OVER_EQUAL_WEIGHT_SHARE_OF_COVARIANCE_NAMES
```

and its docstring states the intent explicitly: *"the before-book and the
after-target may hold a different covariance universe, and the limit follows the
object being judged."* `engine.proposal_decision_review` re-measures the repaired
book round by round *because* "exiting names changes both the shares and the
limit (3/N over a smaller covariance universe)".

**So the dynamic denominator is the declared, intended, single-owner policy,
correctly applied, and both limits are published on the artifact**
(`risk_contribution_limit_before` / `_after`). It is not a bug and not a silent
behaviour.

### What it produced on 2026-09-22 (proposal `c780dbeebf5c`)

| | current book (25 names) | minimum repair (14 names) |
|---|---|---|
| names in covariance universe | 25 | 14 |
| per-name limit (3/N) | 0.12 | **0.21428571** |
| AMD weight | 0.050562 | 0.050562 *(unchanged)* |
| AMD risk contribution | **0.149754 — BREACH** | **0.207331 — compliant** |
| DDOG weight | 0.040120 | 0.040120 *(unchanged)* |
| DDOG risk contribution | **0.138702 — BREACH** | **0.206546 — compliant** |
| breaches remaining | 2 | **0** |
| portfolio volatility, invested basis | 0.1196 | **0.1680** |
| portfolio volatility, capital basis | 0.1142 | **0.0895** |
| cash | 4.5% | 46.7% |

Both breaching names keep their exact original weight, and both see their share
of portfolio risk **rise by roughly 6 percentage points** — AMD 14.98% → 20.73%,
DDOG 13.87% → 20.65% — while moving from breach to compliant, because the limit
rose faster than the shares did.

The repair's own record is unambiguous about how this happened:

```
constraints_repaired            : ["RETENTION_EXIT_BUFFER"]
risk_repair_rounds              : 1 round, breaches []
adjustments                     : 11 x EXIT_TO_ZERO, all RETENTION_EXIT_BUFFER
weight_ceilings                 : the 11 exited names only
```

**AMD and DDOG were never touched.** Their two `RISK_CONTRIBUTION_CAP`
obligations were discharged entirely by the eleven *retention* exits shrinking
the covariance universe from 25 names to 14, which raised the cap from 12% to
21.4% — past the larger shares those two names then carried. The repaired book's
two largest risk contributors are exactly the two names that were in breach, and
together they now carry **41.4% of portfolio risk across 9.1% of NAV**.

### The question for the operator

Is a purely **relative** risk-contribution cap the intended governance for a
repair that only *removes* names? As written, any repair that exits enough
holdings will discharge a risk-contribution breach without reducing the breaching
position at all. On total capital the repaired book is less volatile (46.7% is
cash); on invested capital it is materially more concentrated in risk terms.

Options, none of which this release took:

- **Accept as-is.** The cap is a relative-concentration rule; cash is a real
  asset choice; the capital-basis volatility fell. Both limits are published.
- **Judge the risk-contribution cap against the BEFORE universe** when the repair
  is exit-only, so a breach must be repaired by reducing the breaching name.
- **Add an absolute companion floor** (e.g. no name above X% of portfolio risk
  regardless of N).

Changing any of these is a policy change to `risk_contribution_excess_multiple`
or to the limit's basis, owned by `engine.holding_opportunity_cost`, and would
re-rule historical obligations. It is not a UI decision.

---

## 2. A selected MINIMUM_REPAIR has no implementable order plan

### What was found

`api.rebalance_execution._reconcile_order_plan` reconciles the desk against
`artifact["proposal"]["allocations"]` — the **full target**, and only ever the
full target. It never read the R63 governed target selection.

So the path

```
select MINIMUM_REPAIR  ->  approve  ->  order plan
```

produced the **full target's** order plan: on 2026-09-22 that is 20 names / 22
changes / 35.0% turnover / $86.36, in place of the 14 names / 11 changes / 21.1%
turnover / $52.01 the operator selected. The plan was internally consistent and
bound the right proposal hash. It was simply a different target.

### What R69.1 did about it

Failed closed, and nothing more. `load_rebalance_state` returned
`ORDER_PLAN_BLOCKED_SELECTED_TARGET_NOT_IMPLEMENTABLE`, named the selected
target, offered **no** confirmable plan, and routed the operator to a governed
re-selection.

R69.2 moved that refusal EARLIER still. A selection that carries no implementable
book is now refused at the APPROVAL gate (`SELECTED_TARGET_NOT_IMPLEMENTABLE`),
because R69.1 let the operator be told their choice was approved and only
refused three steps later. The blocked order-plan state survives for the cases
that remain genuinely unimplementable: a CURRENT selection, a target whose
weights the review could not publish, and any selection recorded before R69.2.
`tests/test_r69_2_selected_target_lifecycle.py` is the regression for the whole
path.

### The question for the operator — ANSWERED BY R69.2

Building a real minimum-repair order plan needs an **owner for the repaired
book's weights**. They existed only inside the review *projection*
(`review.states.MINIMUM_REPAIR.weights`), which is recomputed on every read and
never persisted; the selection record froze the repair's *economics* but not its
weight vector.

R69.2 gave them that owner. `engine.selected_target` builds one immutable,
implementable representation per target — reading every weight and every
economic verbatim from the review, taking each allocation row's instrument
metadata from the proposal artifact's own row, and labelling each row's action
with `engine.reallocation_proposal`'s own `reoptimised_action`. It is not an
optimiser: it computes no weight. The FULL TARGET keeps the artifact's
allocation list verbatim, so no historical economic moved.

`api.portfolio_decision` freezes that block into the governed selection, binding
it to the proposal, the review, the opportunity-cost assessment, the portfolio
state, the session, the book and the selection id. `api.rebalance_execution`
builds the order plan from the frozen list. A MINIMUM_REPAIR selection now
produces MINIMUM_REPAIR orders.

What R69.2 did **not** do is touch item 1 above. The dynamic denominator is
unchanged and still awaiting the operator decision on this page.
