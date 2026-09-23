# R69.1 — two policy items raised for manual review

Neither item was changed by this release. No threshold, score, obligation or
historical proposal economic was altered. Both need an operator decision.

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

### What this release did about it

Failed closed, and nothing more. `load_rebalance_state` now returns
`ORDER_PLAN_BLOCKED_SELECTED_TARGET_NOT_IMPLEMENTABLE`, names the selected
target, offers **no** confirmable plan, and routes the operator to a governed
re-selection. `tests/test_r69_1_governed_selection_e2e.py::test_20/21` are the
regression; `test_22/23` prove the FULL_TARGET and no-selection paths are
unchanged.

### The question for the operator

Building a real minimum-repair order plan needs an **owner for the repaired
book's weights**. They exist today only inside the review *projection*
(`review.repair.weight_ceilings` + adjustments), which is recomputed on every
read and never persisted; the selection record freezes the repair's *economics*
but not its weight vector.

That is a genuine design decision — which artifact owns an implementable
non-full target, and how its identity is bound to the approval — and it belongs
in its own release with its own governance, not in a UI repair. Until then, the
minimum repair is reviewable and selectable but **not executable**, and the
system now says so instead of executing something else.
