# R82.2 — THE PRE-RULING APPROVAL-STATE CONTRADICTION

Target viewport **1920x1080**. Surface: the Proposal decision review panel under
Reallocation (`/ui/`, `api/ui/index.html`, `function loadReallocationProposal` …
`window.renderReallocationProposal`).

This release changes **no** portfolio mathematics, **no** risk-policy calculation,
**no** 12.00% / 21.428571% figure, **no** `POLICY_COMPLIANT_REPAIR` solve and **no**
part of the R82.1.1 provenance ceremony. It closes one defect: four surfaces of the
same rendered panel disagreed about whether approval was the operator's current
action.

---

## 1. SCAN

### 1.1 The live frozen state this was observed against

Read from `GET /v1/operations/proposal-decision-review` on 2026-09-28
(session 2026-09-25, book unchanged, nothing written by the read):

| field | live value |
| --- | --- |
| `status` / `review_state` | `OK` / `COMPLETE_CURRENT` |
| `freshness.actionable` / `.approval_allowed` | `True` / `True` |
| `freshness.next_required_action` | `None` |
| `selection.selected_target` | `MINIMUM_REPAIR` |
| `selection.implementable` | `True` |
| `risk_policy_ruling.state` | `RULING_ON_RECORD_BUT_PROVENANCE_UNVERIFIED` |
| `risk_policy_ruling.authoritative` | `False` |
| `risk_policy_ruling.satisfies_the_policy_review` | `False` |
| `risk_policy_ruling.approval_blocked_by_the_ruling` | `False` |
| `risk_policy_decision.state` | `OPERATOR_RULING_REQUIRED_PRIOR_RECORD_UNVERIFIED` |
| `risk_policy_decision.required` | `True` |
| `policy_compliant_successor` | `None` |
| `proposal_approvable` (proposal read contract) | `True` |

### 1.2 The endpoints, state and handlers involved

| surface | function | what it reads today |
| --- | --- | --- |
| Status bar — *Next required action* | `_pdrStatusBar` (index.html:30694) | `d.freshness.next_required_action`, and when that is null **derives its own answer in JavaScript** |
| Step 2 — approve CTA | `_pdrApproveBlock` → `_pdrApproveCta` (index.html:29569 / 29550) | `d.freshness.actionable` + `selection.implementable` only |
| Step 3 — risk policy review | `_pdrPolicyReview` (index.html:29863) | `selected_target_implementation.risk_contribution.policy_review` |
| Step 4 — risk policy decision | `_pdrRiskPolicyDecision` (index.html:30124) | `d.risk_policy_decision` |

### 1.3 Which actions are read-only, preview-only or DB-writing

* read-only: `GET /v1/operations/proposal-decision-review`, `GET
  /v1/operations/portfolio-decision`, every projection named below.
* DB-writing, operator-initiated, unchanged by this release: `POST
  …/portfolio-decision/select-target`, `POST …/portfolio-decision/record`,
  `POST …/portfolio-decision/risk-policy-ruling`, `POST
  …/risk-policy-ruling/confirmation`.
* Create Orders / order execution / automation: **not implemented, not enabled,
  not added here.**

### 1.4 The backend authority that already exists

`api.portfolio_decision` is the canonical decision owner and it **already** rules on
this exact question — on the WRITE path, in `record_decision`:

```
record_decision(APPROVE)
  … session freshness            -> PROPOSAL_SESSION_STALE
  … mandatory repair verdict     -> OBLIGATIONS_UNRESOLVED
  … selection identity match     -> PROPOSAL_STALE
  … selection is CURRENT         -> SELECTED_TARGET_IS_NO_CHANGE
  … selection implementable      -> SELECTED_TARGET_NOT_IMPLEMENTABLE
  … ruling refuses the book      -> SELECTED_TARGET_BREACHES_THE_RULED_REFERENCE_LIMIT
  … ruling owed and unanswered   -> SELECTED_TARGET_REQUIRES_RISK_POLICY_REVIEW
  … otherwise                    -> recorded
```

The last two terms are computed from `selection_policy_review(selection)` and
`risk_policy_ruling_state(selection=…)` — the **same two owners** Step 3 and Step 4
render. So the authority is not missing. What is missing is that the same ordered
verdict is never **published on a read**, so the read surfaces guessed.

### 1.5 The old conflicting read-model owner

Two of them, and both are browser- or proposal-level rather than gate-level:

1. **`_pdrStatusBar` (the browser).** `f.next_required_action ? … : (actionable ? (sel
   ? 'APPROVE SELECTED TARGET' : 'CHOOSE A TARGET') : 'RUN PORTFOLIO CYCLE')`.
   `freshness` only ever names an action when the **session** is stale, so on a
   current session the browser always answered `APPROVE SELECTED TARGET` — whatever
   the risk-policy gate said.
2. **`_pdrApproveBlock` (the browser).** Session actionable + `implementable !== false`
   was treated as sufficient for an armed `APPROVE MINIMUM REPAIR` control. The
   risk-policy gate is not a term in it.

A third, weaker one: `derive_decision_state`'s `approvable` (the separate
portfolio-decision lane, rendered as the cockpit's *Approval* line) also has no
risk-policy term, so it reported `True` while approval was withheld.

---

## 2. REVIEW — critique of the current panel

| check | verdict |
| --- | --- |
| vertical stacked layout | NO — the cockpit grid and the panel's step structure stand |
| empty / useless Overview cards | NO |
| Daily Plan heavy scrolling at 1920x1080 | NO — the status bar and Step 1/2 are above the fold and this release adds **one grid cell**, not a row |
| hidden safety status | NO — badges present in every step |
| diagnostics dominating | NO — evidence stays in `Audit / advanced` |
| `alert()` / `confirm()` | ABSENT and must stay absent |
| **workflow state coherence** | **FAIL — the defect.** The panel presents `APPROVE MINIMUM REPAIR` and `NEXT REQUIRED ACTION = APPROVE SELECTED TARGET` in the same paint as `APPROVAL WITHHELD` and `Step 4 — RISK POLICY DECISION (yours to make)` |
| **backend authority** | **FAIL.** `writeControlsDerivedFromBackendGate = 0`: the approve affordance and the next-action label were both derived in JavaScript from `actionable && sel` |
| blank buttons | NO |
| connected sections saying *Connect to Load* | NO |

The failing rows are the whole scope of this release. Everything else stays.

---

## 3. PLAN

### 3.1 ONE authoritative owner, published once

Add to the canonical decision owner `api.portfolio_decision`:

* **`risk_policy_approval_gate(policy_review, ruling_state)`** — the ordered
  risk-policy answer, extracted from the write path so that
  `record_decision` and every read consume the *same function*, in the same order,
  with the same status literals and the same next-action words. No new policy
  calculation: it reads verdicts the two existing owners already produced.
* **`selected_target_approval_gate(...)`** — the read projection. It composes, in the
  write path's own order: session freshness → selection present → selection identity
  → CURRENT → implementable → `risk_policy_approval_gate`. It returns
  `{available, status, next_required_action, blocking_reason, detail, …}` and it is a
  pure composition — it never re-derives a cap, a share, a weight or an excess.
* the next-action words become module constants with one spelling and one vocabulary
  (`NEXT_ACTION_VOCAB`), including the two literals the write path already used.

`api.proposal_decision_review._governance` publishes the gate beside the selection,
the ruling and the decision it already publishes; the envelope carries it as
`approval_gate` (identical to `governance.approval_gate`).

`derive_decision_state` accepts the gate as an **optional term that can only REMOVE
approvability** — the identical contract it already gives session freshness — and
`load_portfolio_decision` resolves it on the production-default read, so the
cockpit's Approval line cannot contradict the panel either.

### 3.2 Wireframe — 1920x1080, the exact live pre-ruling state

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Proposal decision review        DETERMINISTIC  NO LLM  PREVIEW ONLY  NO ORDERS  MANUAL REVIEW    [↻ Refresh] │
├──────────────┬──────────────────┬───────────────────┬──────────────────┬──────────────────┬──────────────────┤
│ SESSION      │ PROPOSAL STATUS  │ CURRENT           │ SELECTED TARGET  │ APPROVAL         │ NEXT REQUIRED    │
│ 2026-09-25   │ COMPLETE_CURRENT │ ACTIONABILITY     │ MINIMUM REPAIR   │ ▲ WITHHELD       │ ACTION           │
│ latest       │ pprop_…          │ ACTIONABLE        │ psel_…           │ SELECTED_TARGET_ │ RISK POLICY      │
│ eligible …   │                  │ a target may be   │                  │ REQUIRES_RISK_   │ DECISION         │
│              │                  │ selected          │                  │ POLICY_REVIEW    │ (backend)        │
│   ← cell 1   │   ← cell 2       │   ← cell 3        │   ← cell 4       │  ← cell 5 NEW    │  ← cell 6        │
└──────────────┴──────────────────┴───────────────────┴──────────────────┴──────────────────┴──────────────────┘

┌── Step 1 — choose a target ────────────────────────────────────────────────────────────────── (unchanged) ──┐
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

┌── Step 2 — selection result (backend)                          NO ORDERS  PREVIEW ONLY  AUTOMATION OFF ─────┐
│ ✔ SELECTED: MINIMUM REPAIR                                                                                  │
│ Selection ID psel_…  ·  Proposal pprop_…  ·  Status CURRENT · actionable  ·  Target identity 6f2a… frozen    │
│                                                                                                             │
│ ┌─ ▲ APPROVAL WITHHELD — SELECTED_TARGET_REQUIRES_RISK_POLICY_REVIEW ───────────────────────────────────┐   │
│ │  APPROVAL UNAVAILABLE   MANUAL REVIEW   NO ORDERS                                                     │   │
│ │  <the backend's own detail sentence, rendered verbatim>                                                │   │
│ │  Next required action: RISK POLICY DECISION → Step 4 below.                                           │   │
│ │  ┌────────────────────────────┐                                                                       │   │
│ │  │ APPROVE MINIMUM REPAIR     │  ← rendered DISABLED. No onclick. Not armable.                         │   │
│ │  └────────────────────────────┘                                                                       │   │
│ │  Decided by api.portfolio_decision. This screen renders that verdict and derives none of it.           │   │
│ └───────────────────────────────────────────────────────────────────────────────────────────────────────┘   │
│ Selection is not approval. Nothing has been approved, no order plan exists, no order or fill was created.    │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

┌── Step 3 — the exact target you selected (MINIMUM REPAIR)   FROZEN AT SELECTION  PREVIEW ONLY  NO ORDERS ───┐
│ Positions 14 · Changes 12 · Turnover 21.2% · Est. cost $52.33 · Cash 46.9% · Obligations left 0              │
│ … target rows …                                                                                             │
│ ▲ RISK POLICY REVIEW REQUIRED   APPROVAL WITHHELD   NO THRESHOLD CHANGED   MANUAL REVIEW                     │
│   cap rose 12.00% → 21.428571%; held at 12.00%, 2 names breach (AMD, DDOG)                    (unchanged)    │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

┌── Step 4 — RISK POLICY DECISION (yours to make)  ⚖ YOUR DECISION  APPROVAL WITHHELD  NO ORDERS  MANUAL … ───┐
│ ⓘ A PRIOR RULING IS ON RECORD AND ITS PROVENANCE IS NOT VERIFIED   UNVERIFIED  AUDIT EVIDENCE ONLY           │
│   BINDS NOTHING                                                                                (unchanged)   │
│ ( ) ACCEPT_AS_IS …        ( ) JUDGE_AGAINST_THE_BEFORE_UNIVERSE …    ( ) ADD_AN_ABSOLUTE… [UNAVAILABLE]       │
│ [ ] I am the portfolio operator …        [ RECORD RISK POLICY RULING ] (disabled until chosen + confirmed)    │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

(Step 5 — POLICY-COMPLIANT SUCCESSOR renders only after an AUTHORITATIVE JUDGE ruling.)
▸ Why this review path   ▸ Compare the three states   ▸ What the proposal changes   ▸ Audit / advanced
```

Vertical budget: the panel gains **no new row**. Cell 5 is a sixth column in the
existing `.pdr-bar` grid (`repeat(6, 1fr)` at ≥1500px, `repeat(3, 1fr)` below), and the
withheld block replaces the armed button that stood in the same place in Step 2. The
first screen at 1920x1080 still shows the status bar, Step 1 and Step 2.

### 3.3 The state table this renders

`available` and `next_required_action` below are **the backend's**, never the browser's.

| live state | `approval_gate.status` | `next_required_action` | Step 2 control |
| --- | --- | --- | --- |
| no selection | `TARGET_SELECTION_REQUIRED` | `SELECT_A_TARGET` | none (unchanged) |
| session moved on | `PROPOSAL_SESSION_STALE` | `RUN_PORTFOLIO_CYCLE` | none (unchanged) |
| selection identity ≠ current proposal | `PROPOSAL_STALE` | `SELECT_A_TARGET_AGAINST_THE_CURRENT_REVIEW` | **none — fail closed** |
| selection is CURRENT | `SELECTED_TARGET_IS_NO_CHANGE` | `RECORD_THE_NO_CHANGE_DECISION` | none (unchanged) |
| selection not implementable | `SELECTED_TARGET_NOT_IMPLEMENTABLE` | `SELECT_AN_IMPLEMENTABLE_TARGET` | none (unchanged) |
| **ruling owed, none authoritative** (incl. prior UNVERIFIED) | `SELECTED_TARGET_REQUIRES_RISK_POLICY_REVIEW` | `RECORD_RISK_POLICY_RULING` | **disabled + reason** |
| verified ruling refuses the book (JUDGE) | `SELECTED_TARGET_BREACHES_THE_RULED_REFERENCE_LIMIT` | `REVIEW_AND_SELECT_THE_POLICY_COMPLIANT_SUCCESSOR_TARGET` | **disabled + reason** |
| every gate clear | `PROPOSAL_AWAITING_MANUAL_REVIEW` | `APPROVE_SELECTED_TARGET` | armed |

Post-ruling transitions, stated as consequences of the table and of nothing new:

* **`ACCEPT_AS_IS`** records a ruling only. `satisfies_the_policy_review` then becomes
  true, the risk-policy term falls away, and the gate re-runs **every ordinary term**
  above it. Approve appears only if the gate then says `available`. Nothing
  auto-approves — the gate has no write path.
* **`JUDGE_AGAINST_THE_BEFORE_UNIVERSE`** leaves the original `MINIMUM_REPAIR` in
  breach of the binding 12.00% reference cap, so the gate reports
  `SELECTED_TARGET_BREACHES_THE_RULED_REFERENCE_LIMIT` and names the successor as the
  next action. No Approve for the old selection. `POLICY_COMPLIANT_REPAIR` is
  published for review and must be **selected** through the ordinary selection lane
  and **approved** at the ordinary approval gate — two separate manual acts.
* an **UNVERIFIED** ruling never satisfies the review and never makes Approve
  available, because `satisfies_the_policy_review` is False for it by construction.

### 3.4 What is forbidden in the implementation

* no risk-policy arithmetic, cap, share, weight or excess in JavaScript;
* no second gate order, no second state machine, no second vocabulary;
* the audited UI region adds no `new Date(`, `Date.now(`, `.getTime(`, `.reduce(`,
  `Math.`, `cost_rate`, `COST_BPS` or `compute`;
* no `alert()`, no `confirm()`;
* no Create Orders, no order execution, no automation;
* the frozen proposal, the frozen selection and `review_hash` are byte-identical
  before and after — the gate is published *beside* them and folded into neither.

---

## 4. Acceptance criteria

1. In the live pre-ruling frozen state, Step 3 still says `RISK POLICY REVIEW
   REQUIRED` / `APPROVAL WITHHELD`.
2. In the same state, Step 4 still asks for the Risk Policy Decision.
3. In the same state, the status bar's *Next required action* says `RISK POLICY
   DECISION` and **not** `APPROVE SELECTED TARGET`.
4. In the same state, `APPROVE MINIMUM REPAIR` is absent or rendered disabled with the
   backend's blocking reason, and is not armable.
5. All four surfaces derive from one authoritative backend gate state,
   `api.portfolio_decision`'s. `writeControlsDerivedFromBackendGate` is the only
   source; the browser holds no fallback that can invent an action.
6. A prior UNVERIFIED ruling keeps approval unavailable.
7. `ACCEPT_AS_IS` does not automatically approve.
8. After `ACCEPT_AS_IS`, approval is exposed only if every ordinary gate then clears.
9. `JUDGE_AGAINST_THE_BEFORE_UNIVERSE` exposes no approval for the original
   `MINIMUM_REPAIR`.
10. `JUDGE` exposes / references `POLICY_COMPLIANT_REPAIR` for review.
11. `POLICY_COMPLIANT_REPAIR` still requires a separate target selection.
12. A selected compliant successor still requires separate manual approval.
13. A stale / mismatched selection identity fails closed with no approval CTA.
14. No browser-side risk-policy arithmetic.
15. No live ruling is written during implementation or testing.
16. No live decision or approval is written.
17. No order plan, order or fill is created.
18. The frozen proposal and the current selection stay byte-identical.
19. Existing R82 / R82.1 / R82.1.1 tests stay green.
20. `scripts/audit_architecture.py --strict` passes.
21. A regression reproduces the exact contradiction from the live state and proves it
    cannot recur.
