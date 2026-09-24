# R69.5 — portfolio decision integrity and the risk-policy gate

**Scope.** A bounded repair. No optimiser was rewritten, no second risk
calculation exists, no threshold moved, no exception was granted, and nothing was
selected, approved, ordered or executed. R69.2–R69.4 are preserved.

---

## 1. The question this release makes answerable

An active manager has to distinguish a portfolio that is genuinely better from
one that merely clears a constraint whose threshold moved underneath it.

The governed per-name risk-contribution cap is `3 / N` over the covariance
universe of the book being judged
(`engine.holding_opportunity_cost.risk_contribution_limit`). It is declared once,
it is correctly applied, and it is intentionally relative — the docstring says so
and `engine.proposal_decision_review` re-measures against it deliberately.

Its consequence is that **a target which removes names raises its own cap**, and a
breaching position can become compliant without moving at all. R69.1 raised that
for manual review. R69.2 made it visible. Neither could answer it, and the R69.2
detector answers a narrower question than it appears to:

* `discharged_without_reduction` asks only *"did this name's weight move?"* — so a
  token trim removes a name from the list entirely;
* it can say nothing about a name that was **not** in breach before, because it
  iterates the before-book's breaches.

## 2. What the 2026-09-23 proposal actually shows

Proposal `reap_2026-09-23_alpha_paper_book_1_1b04e5321693`, review hash
`e5212d37…` (the hash the frozen FULL_TARGET selection binds), NAV $97,973.38.

### Governed compliance — each book against its own cap

| | CURRENT | MINIMUM_REPAIR | FULL_TARGET |
|---|---|---|---|
| names in covariance universe | 25 | 14 | 20 |
| per-name cap (3/N) | 12.00% | 21.43% | 15.00% |
| breaches | **2** (AMD, DDOG) | 0 | 0 |

### Held at 12.00% — the cap the current book was judged against

| | CURRENT | MINIMUM_REPAIR | FULL_TARGET |
|---|---|---|---|
| breaches at the reference | 2 | **3** | **4** |
| names | AMD, DDOG | AMD, DDOG, FTNT | ALAB, AMD, DDOG, SNDK |
| portfolio risk they carry | 29.50% | 53.96% | **54.58%** |
| NAV they carry | 9.13% | 13.50% | **14.85%** |

### The full target, name by name, at the 12% reference

| | risk share | weight | what closed the breach |
|---|---|---|---|
| ALAB | 7.43% → 14.34% | 1.84% → 3.21% | **created by this target** (an INCREASE) |
| SNDK | — → 13.95% | 0 → 2.88% | **created by this target** (an ADD) |
| AMD | 15.35% → 14.15% | 5.02% → 4.66% | trimmed; the position change closed **28.5%** of the gap, the cap move closed the rest |
| DDOG | 14.15% → 12.13% | **unchanged** 4.11% | the rest of the book moved; the weight never did |

**The headline finding, which R69.1 did not reach:** the full target's compliance
is not only a denominator story. Two of its four reference breaches are
concentrations it **created**, and no argument about a shrinking universe touches
them. A 15% cap tolerates them; the 12% cap the current book was held to does not.

### Modelled risk, both bases

| | CURRENT | MINIMUM_REPAIR | FULL_TARGET |
|---|---|---|---|
| volatility, capital basis (NAV) | 11.13% | 8.86% | **12.37%** |
| volatility, invested sleeve | 11.67% | 16.70% | **15.47%** |
| concentration (HHI) | 0.0372 | 0.0208 | 0.0329 |
| cash | 4.58% | 46.93% | 20.01% |

The full target raises modelled portfolio risk on **both** bases. The only risk
measures it improves are concentration and the *count* of governed breaches — and
that count improves at a relaxed cap.

### Economics and evidence, as the review already published them

* incremental score over the minimum repair: **−0.00218 net of cost**, against a
  0.050 hurdle it misses by 0.052;
* +13.8pp of one-way turnover and +$33.85 for that;
* the two states hold different invested capital (53.1% vs 80.0%), so their
  scores are **not directly comparable** — the review says so by name
  (`SCORE_BASIS_EXCLUDES_UNINVESTED_CAPITAL`) and refuses to invent a score for
  cash;
* `expected_return_state = NOT_CALIBRATED`, `score_converted_to_dollars = false`;
* matured replacement/addition evidence: **0 wins / 6 losses**, mean spread
  −0.1806, caution `REPLACEMENT_EVIDENCE_ADVERSE`.

## 3. What changed in the source

Four modules, all additive. No threshold, no basis, no obligation and no
historical economic moved.

### `engine/holding_opportunity_cost.py` — **unchanged**

Deliberately. It remains the single risk-policy owner. The counterfactual is
computed by calling **its own** `risk_contribution_breaches` with the before
book's limit, so no second rule exists anywhere.

### `engine/selected_target.py`

* `reference_limit_compliance()` — the after-book judged against the before-book's
  limit, held constant. Publishes the breaching names, whether each was
  `CARRIED_FROM_THE_BEFORE_BOOK` or `OPENED_AGAINST_THE_REFERENCE_LIMIT`, the
  share of portfolio risk and of NAV they carry on both sides, and a clearly
  labelled first-order indicative weight. `solved: false` — reducing one name
  moves every other share, and solving that is the optimiser's job, not this
  kernel's.
* `discharge_attribution()` — for each name in breach before, the two additive
  effects (`exposure_effect` = share fall, `limit_effect` = cap rise) and which
  one closed the breach: `CLOSED_BY_EXPOSURE_REDUCTION_ALONE`,
  `CLOSED_ONLY_BECAUSE_THE_LIMIT_ROSE`, or `STILL_IN_BREACH`.
* `policy_review_state()` — `required` iff the cap **rose** and the target does
  **not** comply at the reference. That is exactly "compliance depends on
  denominator-driven cap relaxation" and nothing else. Carries the declared
  policy, the three R69.1 options as data, and the acknowledgement token.
* **Bug fixed:** `excess_multiple` was read under the policy key's name
  (`risk_contribution_excess_multiple`) instead of the limit block's
  (`excess_multiple`), so it was always `None` — the one number that explains
  *why* the limit is what it is never reached a screen.
* `weight_moved` is decided here, with the owner's materiality band, so no
  surface has to do weight arithmetic.

**The selected-target identity hash does not move.** It covers the weights, the
allocation rows and the economics; this verdict changes none of them. Verified
against the live store: `2d3707b5f597c275…` (FULL_TARGET) and
`78497a7fe7fd2c89…` (MINIMUM_REPAIR) are unchanged.

### `api/portfolio_decision.py`

* `selection_policy_review()` — reads the verdict off the **frozen** selection,
  never from a fresh review: the operator approves the book they froze.
* `validate_risk_policy_acknowledgement()` — fail-closed on token, frozen-book
  hash, reference limit and instrument set. A ruling about a different book is
  not a weaker ruling; it is a ruling about something else.
* New state `SELECTED_TARGET_REQUIRES_RISK_POLICY_REVIEW`, in
  `DECISION_STATE_VOCAB` and **not** in `APPROVABLE_DECISION_STATES`. Scoped to
  APPROVE and skipped on a replay. REJECT and HOLD stay available.
* The decision record now carries `risk_policy_review` and
  `risk_policy_acknowledgement`, so an approval that needed a ruling is findable
  for ever rather than merely indistinguishable from one that did not.

**Three shapes, three behaviours** — the gate engages only where a frozen book
exists to judge:

| selection | behaviour |
|---|---|
| carries a frozen book **with** the verdict | gate applies as described |
| carries a frozen book **without** it (R69.2–R69.4) | fails closed, `RISK_POLICY_REVIEW_NOT_PUBLISHED_BY_SELECTION`; re-select to publish it |
| carries **no** frozen book (pre-R69.2) | unchanged — R69.2 already rules on that shape, and in production session freshness has already made it unapprovable |

### `engine/proposal_decision_review.py`

* `capital_scope()` — the asset classes and sleeves the three targets actually
  range over, **derived** from the states' own allocation maps, plus
  `frontier_optimised: false`. It states in words that a comparison inside one
  sleeve is not a search of the cross-asset frontier. A future proposal carrying
  an FX or rates sleeve changes this block without the block being edited.
* Every `target_selection` option now carries `expected_return_state`,
  `score_is_a_percentile_not_a_return`, `invested_weight` and the matured-evidence
  cautions. The full explanation always said these things; the **chooser** did
  not, and the chooser is what a surface renders. `CURRENT` carries no
  replacement caution, because doing nothing is not a replacement.

### `api/app.py`

`risk_policy_acknowledgement` on the record request. No date, no session — the
server still resolves all of those. It can only fail an approval closed; it can
never make one approvable that was not already approvable on every other gate.

### `api/ui/index.html`

Display only. Reference rows in the existing risk table, the policy banner, and
the scope line. **No new button and no new write path** — see the wireframe:
a policy ruling that can be cleared by reflex is not a ruling.

## 4. The exact manual governance decision required

The currently selected FULL_TARGET **cannot be approved as it stands**, and two
things are in the way.

**Step 1 — re-select the target (a governed act, not an approval).** The frozen
selection `psel_2026-09-23_alpha_paper_book_1_full_target_1b04e5321693_r1` was
made before this verdict existed, so it publishes none and the gate fails it
closed. Re-selecting FULL_TARGET against the current review freezes the same book
— the identity hash `2d3707b5…` does not move — and attaches the verdict. Note
the review hash has moved (`e5212d37…` → `20eb86c1…`) because the review now
carries `capital_scope` and the evidence-bearing options, so this is recorded as
an auditable revision rather than an idempotent reuse.

**Step 2 — rule on the policy.** The decision is:

> Is a purely **relative** per-name risk-contribution cap the intended governance
> for a target that clears it by changing N, and for a target that creates new
> concentrations only the relaxed cap admits?

Applied to this target that means accepting, knowingly, that ALAB, AMD, DDOG and
SNDK together carry **54.6% of portfolio risk on 14.8% of NAV**, and that ALAB
and SNDK are positions this target creates above the current book's own limit.

The three options (unchanged from R69.1, now carried as data):

1. `ACCEPT_AS_IS` — the cap is a relative-concentration rule, cash is a real
   asset choice, both limits are published.
2. `JUDGE_AGAINST_THE_BEFORE_UNIVERSE` — a breach must be repaired by reducing
   the breaching name. **This is a policy change** to
   `engine.holding_opportunity_cost` and would re-rule historical obligations.
3. `ADD_AN_ABSOLUTE_COMPANION_FLOOR` — no name above X% of portfolio risk
   whatever N is. Also a policy change.

Options 2 and 3 are **not** what the acknowledgement records. The
acknowledgement authorises **one target, once**; changing the policy is a
separate, deliberate change to the canonical owner.

## 5. What the reference would demand instead

Indicative, first-order, and explicitly not a re-optimisation (each figure scales
that one name only; reducing it moves every other share):

| | weight now | indicative weight at 12% | change |
|---|---|---|---|
| ALAB | 3.21% | ~2.68% | −0.53pp |
| AMD | 4.66% | ~3.95% | −0.71pp |
| DDOG | 4.11% | ~4.06% | −0.05pp |
| SNDK | 2.88% | ~2.47% | −0.41pp |

## 6. The multi-asset gap, stated plainly

Every dollar in all three targets sits in **US_EQUITY or cash**. One risk asset
class. This review compares allocations inside that opportunity set; it is **not**
evidence that the cross-asset frontier was searched and this target won. No
rates, FX, commodity, volatility or event sleeve produced a candidate for this
session, so none was ranked and none was rejected. Cash is a real asset choice
and is priced as one (Release 32); the rest of the frontier is simply absent from
the comparison. `capital_scope.frontier_optimised` is `false` and says so.

## 7. Validation

* `tests/test_r69_5_risk_policy_gate.py` — 32 tests: the reference comparison
  (including the exact 2026-09-23 shape, the token-trim blind spot, a
  target-created breach, an unmoved limit, additivity, a missing limit), the
  approval gate (withheld and writes nothing; REJECT/HOLD still available; five
  wrong-ruling shapes refused; a bound ruling recorded; the unpublished-verdict
  and stale-book paths; identity preserved; a compliant target needing no
  ruling), capital scope, option-level evidence, and safety.
* Impacted regressions, all green: `test_r69_2_selected_target_lifecycle`,
  `test_r69_1_governed_selection_e2e`, `test_r69_1_reallocation_operator_flow`,
  `test_r69_3_running_close_projection`, `test_r69_proposal_review_availability`,
  `test_r62_proposal_decision_review`, `test_r63_governed_target_selection`,
  `test_release54_2_3_2_decision_supersession`, `test_stage18_portfolio_decision`
  (372 passed), plus `test_release49_operator_presentation`,
  `test_release48_operator_workflow`, `test_release30_read_models_and_ui`,
  `test_release29_3_decision_integrity`, `test_release29_4_session_authority`,
  `test_release54_2_4_reallocation_coherence`,
  `test_multi_asset_capital_activation_r55_v1`,
  `test_release47_constrained_reallocation`, `test_trackb_decision_consistency`,
  `test_slice2_workflow_state` (540 passed).
* `scripts/audit_architecture.py` — exit 0, output identical to the pre-change
  baseline apart from line counts.
* Browser acceptance at 1920×1080 on `http://127.0.0.1:8001/ui/` — panel renders,
  no horizontal scroll, safety badges present, diagnostics still under
  Audit / Advanced, no blank buttons, no `alert()`, no `confirm()`, no Create
  Orders, no automation. Console: one pre-existing `favicon.ico` 404.

**Two failures are pre-existing at HEAD `736f60a` and unrelated** — verified by
stashing this release's changes and re-running them:
`test_r63_live_integration::TestProposalReadSeam::test_02_a_repaired_target_stays_approvable`
and
`test_stage19_1_corporate_action_propagation::test_16_stale_proposal_cannot_be_approved`.

## 8. What was NOT done

No cap changed. No exception granted. No target approved, selected, ordered or
executed. No portfolio cycle run. No model promoted. No historical evidence or
forward evidence fabricated. No second optimiser and no second risk calculation.
