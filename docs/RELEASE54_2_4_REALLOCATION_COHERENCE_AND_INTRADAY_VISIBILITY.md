# Release 54.2.4 — Reallocation Proposal Coherence, Current-Decision Presentation, First-Class Intraday Reassessment Visibility

Status: LANDED (presentation/classification slice on R54.2.3.2). Nothing here
creates an order, a fill, a target, a route, an approval or any automation;
paper-only, preview-first, manual-review boundaries are unchanged. No store was
mutated and no history was rewritten — every repair is a read-composition or a
vocabulary repair in the owner that already decides the concept.

## 1. The live defects (2026-09-02/03)

After R54.2.3.2 correctly established `HOLD CURRENT PORTFOLIO` as the Sep-2
authoritative decision, the surfaces still mixed scopes and lanes:

1. The Today hero rendered the SUPERSEDED proposal's economics (net +0.056,
   35% turnover, $85.69, risk 12.2%→15.6%, 28 positions changing) directly
   under the HOLD headline, as though they described the HOLD.
2. The Reallocation page still displayed Exit 14 / Reduce 1 / Replace 2 /
   Add 6 / Increase 5 / Retain 10 at full prominence — a change-plan look for
   a session whose governed decision changes nothing.
3. Two legitimate economic scopes (+0.018 / 23.5% / $57.46 vs +0.056 / 35% /
   $85.69) rendered without names.
4. Action rows could mislead: display rounding hid real deltas (FTNT
   3.96%→4.0% shown "4.0% → 4.0%" as INCREASE), and a constraint-repaired
   non-held candidate zeroed by the repair kernel produced a 0.0%→0.0% row
   classified EXIT.
5. "Eligible" meant two different rules on one screen (`KEYS no longer meets
   the eligibility rule` beside `HOLDING ELIGIBILITY — PASS`).
6. A reassessment CURRENT for the eligible session was printed
   "Portfolio reassessment (OVERDUE)" because the LEGACY scheduled-review
   clock (api.daily_action_gate, next review 2026-08-01) outranks currency in
   the frozen classifier vocabulary.
7. The live/event-driven pipeline ran (20:30Z proposal cycle, 00:33Z
   INFORMATION_NOT_MATERIAL, 01:33Z HOLD) with no first-class Today answer to
   "did the near-real-time manager run, why, what did it conclude, was it
   governed, does it supersede the standing decision?".

## 2. The three economic scopes (Defect 3 — traced, named, unchanged)

No economic calculation was wrong. Three scopes were being mixed:

| scope token | owner | meaning | example |
| --- | --- | --- | --- |
| `HOC_RELEASE_SET_ESTIMATE` | `engine.portfolio_reassessment` (over `engine.holding_opportunity_cost`) | PRE-PROPOSAL, NON-BINDING economics of executing only the actionable holding subset (the release set). Already stamped `expected_turnover_basis = PRE_PROPOSAL_RELEASE_SET_ESTIMATE`, `turnover_budget_binding_here = False`. | +0.018 net, 23.5% one-way, $57.46, "6 holdings under signal-level review" |
| `COMPLETE_TARGET_PROPOSAL` | `engine.reallocation_proposal` / `engine.constrained_reallocation` (via `api.reallocation_proposal`) | The complete zero-base transition, priced once; the BINDING switching-hurdle verdict is reached here and only here. | score 0.853→0.926, gross +0.073, net +0.056, 35% one-way, $85.69, 28 changed rows |
| `CURRENT_GOVERNED_DECISION` | `api.operator_presentation` (NEW block, from the decision owner's verdict) | What the authoritative decision actually does to capital. A governed HOLD's zeros are DEFINITIONAL (a hold trades nothing), never a computed estimate. | turnover 0, cost $0, positions changing 0, target = current book |

Answers to the ten trace questions: (1) +0.018 = release-set net improvement
estimate; (2) +0.056 = complete-target net-of-cost improvement; (3) 23.5% =
release-set one-way turnover estimate; (4) 35% = complete-target one-way
turnover; (5) $57.46 = release-set modelled cost; (6) $85.69 = complete-target
modelled transition cost; (7) the per-holding action hurdle is decided at the
HOC level, and the PORTFOLIO-level binding hurdle verdict on the complete
target by `engine.reallocation_proposal`; (8) the complete-target scope; (9)
the complete-target verdict feeds the governed decision, and once decided the
CURRENT-DECISION scope is what describes it; (10) once HOLD is authoritative,
both artifact scopes are research/history context — the alternative considered
— and only the current-decision scope describes today's capital action.

`api.operator_presentation` names the vocabulary once
(`ECONOMICS_SCOPE_VOCABULARY`), publishes `decision_summary.economics_scope =
COMPLETE_TARGET_PROPOSAL` + `is_current_decision_economics = False` on the
proposal metrics, and builds the ONE `decision_summary.current_decision` block
(`_current_decision_economics`): HOLD → definitional zeros + "Monitor
portfolio"; review/approved states → the pending change's own numbers scoped
`CHANGE_UNDER_REVIEW` / `APPROVED_CHANGE_IN_EXECUTION`; blocked/not-run →
honestly unavailable. `portfolio_decision.positions_changing` is now
decision-scoped (`positions_changing_scope = CURRENT_GOVERNED_DECISION`): zero
for every non-change state.

## 3. Presentation contract (Defects 1, 2, 9)

* The Today hero (`#today-decision`) renders ONLY the `current_decision`
  block. For the Sep-2 HOLD: current-decision turnover 0%, cost $0, positions
  changing 0, guidance "Monitor portfolio". The considered alternative appears
  as a labelled one-line reference ("SUPERSEDED — HISTORY ONLY · View under
  Reallocation →") with no numbers on the decision card.
* The Reallocation page: a SUPERSEDED banner (chips NOT CURRENT / NOT
  APPROVABLE / NOT AN ACTION PLAN / HISTORICAL-RESEARCH ONLY) with the
  proposal-history facts, and the Changes / Target / Economics analysis
  demoted into a collapsed `<details data-history-only="1">` block ("What was
  considered…"). The Economics section carries its scope line. The rejected
  (non-superseded) alternative keeps the R54.2.1 framing plus the same
  demotion.
* `decision_summary.proposal_history` (new, presentation-only): proposal id /
  hash / session, `created_at` (the immutable artifact's own `generated_at`,
  republished by the constrained composition as `artifact`), `superseded_at`,
  superseded-by decision / artifact / session, `supersession_reason` — all
  read verbatim from the R54.2.3.2 verdict. Live: created 23:38:15Z,
  superseded 23:51:50Z by `CURRENT_NO_CHANGE`
  (`SESSION_DECISION_IS_NO_CHANGE`).
* R54.2.3.2 authority and the server-side approval refusal are untouched.

## 4. Action materiality (Defect 4)

The ONE tolerance remains the proposal kernel's `material_weight_delta`
(1e-4 weight). Two repairs:

* `engine.reallocation_proposal._reoptimised_action`: a NON-HELD name repaired
  to a weight inside the band returns `None` — no action label exists for a
  row that changes nothing — and `_allocation_rows` drops the row (its zero
  weight stays in `proposed_weight`, so turnover / signal / risk / constraint
  measurement is unchanged). A 0.0%→0.0% row can no longer appear in any
  bucket as ADD / EXIT / INCREASE / REDUCE.
* UI display precision: when two genuinely different weights round to the same
  1-decimal string, the row renders 2 decimals, so a labelled INCREASE always
  shows its delta. Formatting only; the label is the backend's.

Changed-position counting is unchanged and correct: `positions_changing` sums
the owner-published non-RETAIN action counts; a REPLACE pair counts exactly
two capital changes (out + in) and nothing is double-counted (the live Sep-2
counts held: 14+1+5+6+2 = 28 rows, each a real capital change of the
alternative).

## 5. Eligibility vocabulary (Defect 5)

Two different rules shared one word:

* `api.daily_action_gate` CHECK_ELIGIBILITY tests SCOREABILITY + CURRENT
  UNIVERSE MEMBERSHIP → label renamed **"Universe membership / scoreability"**;
  summaries say "scoreable, current universe members" and explicitly "not the
  HOC retention rule".
* The HOC exit rule (rank deterioration past the retention band) → operator
  sentences renamed **"the HOC retention rule"**; the mandatory-exit statement
  says "retention-rule exit". Logic, thresholds and codes unchanged.

## 6. Reassessment freshness (Defect 6)

`classify_assessment`'s frozen vocabulary and precedence are unchanged
(OVERDUE still records "the legacy api.daily_action_gate scheduled-review
clock passed"). The repair is presentational and owned:
`api.active_manager_state._stale_components` publishes `display_label` —
for a session-current assessment whose legacy clock passed:
**"Scheduled full review due — legacy api.daily_action_gate clock; the
portfolio reassessment itself is current for the eligible session"** — and the
strip renders it verbatim (raw token kept in the hover/audit detail). A
genuinely stale assessment keeps the raw fallback.

## 7. Lane A / Lane B (Defects 7, 8)

Today separates two lanes:

* **LANE A — GOVERNED PORTFOLIO DECISION** (`#today-decision`, eyebrow
  "Governed portfolio decision"): the authoritative decision with
  current-decision economics (§3).
* **LANE B — LATEST LIVE / INTRADAY REASSESSMENT**:
  `api.active_manager_state.live_reassessment_lane` (new component, composed
  once from the event cycle's own run payload, the R54.1 gate record the
  decision owner wrote into it, and the reassessment head — no re-evaluation,
  no new owner). Fields: run id, timestamp, trigger
  (`materiality_change_level`), material-event count + affected holdings,
  scoring basis date, HOC stage timestamp, reassessment artifact
  id/hash/persistence, `candidate_conclusion`
  (HOLD / CHANGE / PROPOSAL_AVAILABLE / INFORMATION_NOT_MATERIAL / UNKNOWN),
  scope-labelled release-set economics (only when the head artifact is
  provably this cycle's), `governance_state`
  (GOVERNED / WITHHELD / ELIGIBLE / NOT_REQUIRED / UNKNOWN) with the exact
  withheld reason codes and failing checks verbatim,
  `supersedes_standing_decision` (True ONLY on a recorded governed promotion —
  the R54.2.3.2 rule echoed), the standing governed decision id, and
  `manual_review_available`. The UI card (`_amsLaneBHtml`, its own
  `R54_2_4_REGION`) renders it verbatim and is chip-labelled
  "RESEARCH LANE — NOT THE GOVERNED DECISION" unless promoted.

A withheld or non-governed live result can never look authoritative; the exact
withholding reason renders verbatim. The same-session HOC evidence-versioning
limitation stays R54.3 — when the gate withholds for it, Lane B states that
truth; nothing in R54.2.4 weakens or works around the gate.

Live trace at verification time (read-only): cycle `evt_e6e0292f1d15175f`
(00:33:19Z) concluded INFORMATION_NOT_MATERIAL (trigger SIGNAL_CHANGED, no
candidate → governance NOT_REQUIRED); the newer cycle `evt_7dece2a4e47fe608`
(01:33:07Z, trigger MATERIAL_SIGNAL_CHANGED, 14 material events, affecting
CAT/CVS/DDOG) concluded HOLD with no persisted gate verdict on its payload →
governance ELIGIBLE, supersedes standing decision NO; the standing governed
decision remains `drc_governed_drc_2026-09-02_15abfb01856f`.

## 8. Corporate-action scope (Defect 11)

The MNST reconciliation figures are the DESK-BOOK fold — a legitimate
different scope from the authoritative operational NAV, not a defect.
`api.corporate_actions.reconcile_book` now declares it:
`nav_scope = DESK_BOOK_RECONCILIATION_PROJECTION`,
`is_authoritative_nav = False`, `authoritative_nav_owner =
api.operational_book`, and the Audit card renders the scope line. No history
rewrite.

## 9. Outcome-evidence identity (Defect 12)

Rows that look identical (session/holding/recommendation/replacement/horizon)
can be DISTINCT immutable evidence from different same-session reassessment
versions (R54.2) — the differentiating axis is
`reassessment_id`/`reassessment_hash`. `load_outcome_history` annotates each
projected row (`assessment_version`, `same_axis_version_count`,
`repeated_across_assessment_versions`) and the table shows an "Assessment
version" column. Projection only: nothing on disk is deduplicated, deleted or
rewritten (`_annotate_assessment_versions` provably preserves every row).

## 10. Legacy / advanced classification (Defect 10)

Operator-facing bands now carry an explicit flow classification chip
(`data-flow-class`): MODEL TARGET SNAPSHOT REVIEW and ALPHA BOOK
IMPLEMENTATION PLAN → **LEGACY COMPATIBILITY** ("not the canonical next
action"), PAPER TRADING DESK → **MAINTENANCE / RECOVERY**. The normal operator
flow remains: Portfolio Cycle → governed portfolio decision → manual proposal
review only when a current proposal exists → explicit approval / order gates.
No functionality was deleted.

## 11. Guard rails

* `scripts/audit_architecture.py --strict` —
  `check_release54_2_4_reallocation_coherence` (15 blocking fields: one scope
  vocabulary + one current-decision builder, no second calculation, hero
  renders the scoped block and the old unscoped render is gone, history
  demotion present, ONE live-lane composition rendered verbatim with no JS
  governance derivation, truthful freshness label, outcome version identity,
  CA projection scope, eligibility vocabulary split, legacy chips).
* `tests/test_release54_2_4_reallocation_coherence.py` — 51 tests covering the
  44 Tier-1 proofs (hermetic builders + UI statics).
