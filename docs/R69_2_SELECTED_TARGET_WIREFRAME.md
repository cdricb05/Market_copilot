# R69.2 — COMPLETE SELECTED-TARGET REBALANCING

SCAN → REVIEW → PLAN (this document) → EXECUTE PREVIEW.
Nothing below was coded before this document existed.

---

## 1. SCAN — what is actually there today

### Endpoints on the path

| Route | Method | Owner | Writes |
|---|---|---|---|
| `/v1/operations/proposal-decision-review` | GET | `api.proposal_decision_review` | nothing |
| `/v1/operations/portfolio-decision` | **GET only** | `api.portfolio_decision` | nothing |
| `/v1/operations/portfolio-decision/record` | POST | `api.portfolio_decision` | decision ledger |
| `/v1/operations/portfolio-decision/select-target` | POST | `api.portfolio_decision` | selection ledger |
| `/v1/operations/rebalance` | GET | `api.rebalance_execution` | nothing |
| `/v1/operations/rebalance/confirm-order-plan` | POST | `api.rebalance_execution` | desk orders |
| `/v1/operations/rebalance/refresh-target-marks` | POST | `api.rebalance_execution` | desk mark cache |

### Where the three targets live

* `engine.proposal_decision_review.solve_minimum_repair` computes the repaired
  **weight vector**. `build_repair_state` publishes it at
  `review.states.MINIMUM_REPAIR.weights` (8 dp, sorted). `build_review` strips
  `weights` from the `repair` block but the STATE keeps them.
* `review.states.CURRENT.weights` and `review.states.FULL_TARGET.weights` are the
  artifact's own, verbatim.
* Nothing persists any of them. The review is a pure projection, recomputed on
  every read.

### Where the order plan gets its target

`api.rebalance_execution._reconcile_order_plan` reads
`artifact["proposal"]["allocations"]` — the full target, always. Three further
functions read the same list: `target_mark_universe`,
`_proposal_one_way_turnover` and `_freeze_decision_evidence`.

### Read-only / preview-only / DB-writing inventory

* READ-ONLY: the review, the rebalance read model, the decision read model.
* PREVIEW-ONLY: the order plan inside the rebalance read model.
* DB / STORE WRITING: `record_target_selection` (selection ledger),
  `record_decision` (decision ledger), `confirm_rebalance_order_plan` (desk
  order ledger + plan artifact + decision evidence), `refresh_target_marks`
  (desk mark cache only).

### Defects found by the scan

| # | Defect | Evidence |
|---|---|---|
| **D1** | A MINIMUM_REPAIR selection has no persisted weights, so no order plan can be built. | `docs/R69_1_POLICY_ITEMS_FOR_MANUAL_REVIEW.md` §2 |
| **D2** | The APPROVE button posts to `/v1/operations/portfolio-decision`, which declares **GET only** → HTTP 405. | `openapi.json` live probe: `get` only |
| **D3** | The same POST sends `decision: 'APPROVE'`, which is not in `DECISION_VOCAB` (`APPROVE_FOR_PAPER_REBALANCE`) → HTTP 400 even on the right route. | `api/ui/index.html:29551` |
| **D4** | The approval carries no selection identity: `expected_selection_id` exists on `record_decision` but no route or browser ever sends it. | `api/app.py:7242-7254` |
| **D5** | A missing selection silently becomes FULL_TARGET at the order plan. | `rebalance_execution._base_plan` gate 2b only fires when `_sel_target is not None` |
| **D6** | The decision record does not say WHICH target was approved. | `record_decision` record dict |
| **D7** | A selection revised after approval is not detected by the order-plan path. | no `selection_id` comparison in `_base_plan` |

**The user's question has three blockers, not one: D1, D2/D3, D5.**

---

## 2. REVIEW — critique of the current surface

Checked explicitly, per the mandatory checklist:

| Check | Verdict |
|---|---|
| vertical stacked layout | **PASS** — R69.1 rebuilt the panel into status bar → step 1 → step 2 → collapsed evidence. |
| empty Overview cards | PASS |
| useless Overview cards | PASS |
| Daily Plan heavy scrolling | PASS — step 2 ends 404 px down at 1920×1080 (R69.1 measurement). |
| hidden safety status | PASS — badges on the panel header and on step 2. |
| diagnostics dominating | PASS — inside `Audit / advanced`. |
| `alert()` | **none** |
| `confirm()` | **none** |

Three real gaps this release must close:

* **R1.** After selecting, the operator is shown a selection ID and an APPROVE
  button — but never the **weights they selected**. The panel shows per-state
  summaries (positions / turnover / cost) and the full target's change rows;
  the minimum repair's actual book is nowhere on screen.
* **R2.** The APPROVE control is offered for a target the system cannot
  implement. That is the "misleading Approve button" the brief forbids.
* **R3.** The risk-contribution effect is invisible. The repaired book's two
  largest risk contributors are the two names that were in breach, carrying
  41.4 % of portfolio risk across 9.1 % of NAV, and the screen says nothing.

---

## 3. PLAN

### 3.1 Architecture — one new owner, no second optimiser

```
engine.proposal_decision_review          (EXISTING — owns the weights)
        │  review.states.<TARGET>.weights
        ▼
engine.selected_target                   (NEW — pure projection kernel)
        │  weights VERBATIM + allocation rows + economics VERBATIM
        │  action labels via engine.reallocation_proposal.reoptimised_action
        ▼
api.proposal_decision_review             (publishes envelope.selected_targets)
        ▼
api.portfolio_decision.record_target_selection
        │  FREEZES the block into the immutable selection record
        ▼
api.portfolio_decision.record_decision   (stamps target + selection_id)
        ▼
api.rebalance_execution                  (reads the FROZEN allocations)
```

`engine.selected_target` **computes no weight**. It reads
`review.states[target].weights` verbatim, merges the artifact's own allocation
row metadata (sector, currency, multiplier, instrument type, margin, cost bps),
re-derives the ACTION label through the reallocation kernel's own public
`reoptimised_action`, and prices the step with `turnover_and_cost`. No search,
no optimisation, no new threshold.

**FULL_TARGET keeps the artifact's allocation list verbatim.** The projection is
proved equivalent by test rather than replacing it, so no historical economic
moves.

### 3.2 Identity

The frozen block binds, in the selection record:

```
proposal_id · proposal_hash · review_hash · hoc_assessment_hash
portfolio_state_hash · corporate_actions_hash · universe_scoring_hash
active_book_id · eligible_market_date · selection_id
selected_target · selected_target_hash          (economics — unchanged from R63)
selected_target_implementation_hash             (NEW: weights + allocations + economics)
```

`selected_targets` is published on the **envelope**, not inside `review`, so
`review_hash` stays byte-stable — a selection made yesterday still verifies. The
block is nonetheless fully pinned: it is a pure function of
(`proposal_hash`, `review_hash`, `target`).

### 3.3 Implementability

| target | implementable | reason |
|---|---|---|
| `FULL_TARGET` | yes | the artifact carries its allocations |
| `MINIMUM_REPAIR` | yes | the frozen projection carries its allocations |
| `CURRENT` | **no** | `TARGET_IS_NO_CHANGE` — there is nothing to implement |

`CURRENT` is refused **before** approval (`PDS_SELECTION_IS_NO_CHANGE`, already
present). No Approve button is ever rendered for it.

### 3.4 Requiring an explicit choice (brief §4)

* A NEW `APPROVE` with no governed selection → `TARGET_SELECTION_REQUIRED`.
  Nothing is written, and the next required action names the selection route.
* An **already recorded** decision stays readable and idempotently re-recordable.
* An approval recorded **before** this release carries no `selected_target`. The
  order-plan path still builds the full target for it, but publishes
  `target_source: LEGACY_APPROVAL_NO_SELECTION` on the read model and in the
  plan. The fallback exists; it is never silent.

### 3.5 Wireframe — 1920×1080, the PDR panel

The panel is the right-hand cockpit column. Steps 1–3 are above the fold.

```
┌─ Proposal decision review ─────────── DETERMINISTIC · NO LLM · PREVIEW ONLY · NO ORDERS · MANUAL REVIEW · [↻] ─┐
│ ┌ STATUS BAR (5 facts, never blank) ────────────────────────────────────────────────────────────────────────┐ │
│ │ SESSION 2026-09-22 │ STATUS READY │ ACTIONABLE │ SELECTED  MINIMUM REPAIR │ NEXT  Approve the selection   │ │
│ │ latest eligible …  │ reap_…c780…  │ …selectable│ psel_…_r0                │                               │ │
│ └───────────────────────────────────────────────────────────────────────────────────────────────────────────┘ │
│ ┌ STEP 1 — choose target ──────── SELECTION IS NOT APPROVAL · NO ORDERS · MANUAL REVIEW ────────────────────┐ │
│ │ ┌ Current (defer) ───────┐ ┌ Minimum repair  RECOMMENDED ┐ ┌ Full zero-base target ────┐                  │ │
│ │ │ Pos 25 Turn 0.0%       │ │ Pos 14  Turn 21.1%          │ │ Pos 20  Turn 35.0%        │                  │ │
│ │ │ Cost $0  Cash 4.5%     │ │ Cost $52.01  Cash 46.7%     │ │ Cost $86.36  Cash 19.7%   │                  │ │
│ │ │ Risk 11.42% Obl 2      │ │ Risk 8.95%   Obl 0          │ │ Risk 12.05%  Obl 0        │                  │ │
│ │ │ NOT IMPLEMENTABLE ⓘ    │ │ IMPLEMENTABLE ✔ 11 orders   │ │ IMPLEMENTABLE ✔ 22 orders │   ← NEW row      │ │
│ │ │ [ Keep current/Defer ] │ │ [ Select minimum repair ]   │ │ [ Select full target ]    │                  │ │
│ │ └────────────────────────┘ └─────────────────────────────┘ └───────────────────────────┘                  │ │
│ └───────────────────────────────────────────────────────────────────────────────────────────────────────────┘ │
│ ┌ STEP 2 — selection result (backend) ── CREATES TRADE DECISIONS ONLY · NO ORDERS · AUTOMATION OFF ─────────┐ │
│ │ ✔ SELECTED: MINIMUM REPAIR                                                                                │ │
│ │ Selection ID psel_2026-09-22_alpha_paper_book_1_minimum_repair_c780dbeebf5c · revision 0                   │ │
│ │ Target identity  sti_9f31c0… (weights + economics, frozen at selection)                                    │ │
│ │ [ APPROVE MINIMUM REPAIR ]   ← rendered ONLY when the frozen target is implementable                       │ │
│ └───────────────────────────────────────────────────────────────────────────────────────────────────────────┘ │
│ ┌ STEP 3 — the exact target you selected ── FROZEN AT SELECTION · PREVIEW ONLY · NO ORDERS ─────────────────┐ │
│ │ 14 positions · one-way turnover 21.08% · est. cost $52.01 · cash 46.70% · 11 changes                       │ │
│ │ ┌ Weights (compact, scrollable, max-height 260px) ───────────────────────────────────────────────────────┐ │ │
│ │ │ TICKER │ NOW    │ TARGET │  Δ      │ ACTION │ REASON                                                   │ │ │
│ │ │ AIZ    │ 3.85 % │ 0.00 % │ −3.85 % │ EXIT   │ RETENTION_EXIT_BUFFER                                    │ │ │
│ │ │ AMD    │ 5.06 % │ 5.06 % │  0.00 % │ RETAIN │ —                                                        │ │ │
│ │ │ …                                                                                                      │ │ │
│ │ └────────────────────────────────────────────────────────────────────────────────────────────────────────┘ │ │
│ │ ┌ RISK CONTRIBUTION — before vs after ───────────────── POLICY UNCHANGED · MANUAL REVIEW ────────────────┐ │ │
│ │ │              BEFORE (25 names)    AFTER (14 names)                                                     │ │ │
│ │ │ per-name cap   12.00 %             21.43 %      ⚠ the cap moved because the universe shrank            │ │ │
│ │ │ breaches       2 (AMD, DDOG)       0                                                                   │ │ │
│ │ │ risk, invested 11.96 %             16.80 %                                                             │ │ │
│ │ │ risk, capital  11.42 %              8.95 %                                                             │ │ │
│ │ │ concentration  0.0372              0.0209        largest 5.06 % → 5.06 %                               │ │ │
│ │ │ ⚠ DISCHARGED WITHOUT BEING REDUCED: AMD 14.98 %→20.73 %, DDOG 13.87 %→20.65 % at unchanged weight.     │ │ │
│ │ │   Declared policy (3/N, engine.holding_opportunity_cost) is UNCHANGED and awaiting manual review —     │ │ │
│ │ │   see docs/R69_1_POLICY_ITEMS_FOR_MANUAL_REVIEW.md §1.                                                 │ │ │
│ │ └────────────────────────────────────────────────────────────────────────────────────────────────────────┘ │ │
│ └───────────────────────────────────────────────────────────────────────────────────────────────────────────┘ │
│ ▸ Why this review path      ▸ Compare the three states      ▸ Why full differs from repair                     │
│ ▸ What the proposal changes ▸ Historical evidence           ▸ Audit / advanced                                 │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Collapsed sections stay collapsed. Step 3 appears **only** once a selection
exists, so the first screen without a selection is unchanged in height.

### 3.6 Acceptance criteria

**Backend**

1. `engine.selected_target` reads weights verbatim; a mutation test proves it
   never recomputes one.
2. The FULL_TARGET projection reproduces the artifact's own `proposed_weight`
   for every allocation row.
3. A MINIMUM_REPAIR selection persists 14 weights, 11 changes, 21.08 % one-way
   turnover, and reads back byte-identical after a fresh process.
4. `_base_plan` builds the MINIMUM_REPAIR order plan from the frozen
   allocations; a FULL_TARGET selection produces exactly the plan it produced
   before this release (hash-compared).
5. `CURRENT` never yields an order plan and never renders an Approve control.
6. A NEW approve with no selection is refused `TARGET_SELECTION_REQUIRED`.
7. A selection revised after approval blocks the order plan by name.
8. Every DB-writing path stays behind its own token; no new token is introduced.

**Browser, 1920×1080, isolated profile, live authenticated READS only**

9. Steps 1–3 above the fold; no heavy Daily Plan scrolling.
10. Safety badges visible on the panel, step 2 and step 3.
11. No blank button; every disabled control states the backend's reason.
12. `alert()` / `confirm()` absent from the selection, approval and step-3 code.
13. Diagnostics remain inside `Audit / advanced`.
14. No Create Orders control, no automation control.

### 3.7 Explicitly out of scope

* No change to `risk_contribution_excess_multiple` or to the 3/N basis.
* No change to any historical proposal, artifact, decision or economic.
* No Create Orders. No order execution. No automation.
* No second optimiser and no weight arithmetic in the browser.
