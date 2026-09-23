# R69.1 — Portfolio → Reallocation operator flow: SCAN / REVIEW / PLAN

Mandatory pre-coding artifact for the Paper Trader UI redesign workflow
(`CLAUDE.md`, `.claude/skills/trading-cockpit-redesign/SKILL.md`).
Target viewport **1920x1080**. Panel host: `#pm-pdr-host`, Portfolio Manager screen.

---

## 1. SCAN — what exists today

### Endpoints this surface touches

| Route | Method | Class | Owner |
|---|---|---|---|
| `/v1/operations/proposal-decision-review` | GET | READ-ONLY | `api.proposal_decision_review` |
| `/v1/operations/portfolio-decision/select-target` | POST | **DB-WRITING** (governance artifact only) | `api.portfolio_decision.record_target_selection` |
| `/v1/operations/portfolio-decision` | POST | **DB-WRITING** (approval record) | `api.portfolio_decision.record_decision` |
| `/v1/operations/rebalance` | GET | READ-ONLY | `api.rebalance_execution` |
| `/v1/operations/rebalance/confirm-order-plan` | POST | **DB-WRITING** (order plan confirm) | `api.rebalance_execution` |

Selecting is **not** approving. No route on this surface creates an order, a fill
or any capital movement. Create Orders and automation are absent and stay absent.

### State the panel reads

`window._pdrData` — the whole review envelope. Top-level: `status`,
`review_state`, `proposal_id`, `proposal_hash`, `review_hash`,
`eligible_market_date`, `freshness`, `actionable`, `target_selection`,
`selection`, `governance`, `review`, `inputs`, `safety`.

The envelope carries **three copies** of the option list:

| Location | Freshness applied? | Read by |
|---|---|---|
| `env.target_selection` | **yes** | the browser (`_pdrSelection`, `_pdrSelect`) |
| `env.governance.target_selection` | **yes** | nothing on this surface |
| `env.review.target_selection` | **no — always `selectable: true`** | `record_target_selection` (backend) |

### Observed defects (evidence, not inference)

1. **`_pdrSelect` omits `X-API-Key`.** Backend access log records six
   `POST /v1/operations/portfolio-decision/select-target … 401 Unauthorized`.
   Every GET on the same screen is 200 because those go through helpers that
   inject the key. This POST hand-rolls `headers: {'Content-Type': …}`.
2. **The result is destroyed by the re-render it triggers.** `_pdrSelect` writes
   the outcome into `#pdr-select-result`, then calls `_pdrLoad()`, whose first
   act is `_pdrRenderLoading()` — which replaces the panel's entire `innerHTML`.
   The authoritative backend response cannot survive. "Recording selection…" is
   what the operator is left looking at.
3. **No pending / error state that survives.** There is no state machine for the
   write; there is one transient string inside a volatile container.
4. **The primary action is the last thing on the panel.** Order today is:
   verdict → freshness line → 3 state columns → explanation paragraphs →
   8-cell increment grid → 33 change rows → **selection controls** → evidence.
5. **Backend selectability guard reads the stale-blind copy.**
   `record_target_selection` reads `review.target_selection`, where
   `selectable` is `true` even on a historical proposal. Session freshness is
   still enforced first, so the gate fails closed — but the documented
   "backend-decided selectability" claim is only half true.
6. **No next-action control after a successful selection.** Nothing renders
   "APPROVE MINIMUM REPAIR" beside the recorded selection.

### Hard-failure checklist against the current panel

| Condition | Status |
|---|---|
| Daily Plan / primary action needs heavy scrolling at 1920x1080 | **FAIL** — selection is ~6 sections down |
| Important workflow button blank | pass |
| `alert()` / `confirm()` | pass — none |
| Create Orders enabled | pass — absent |
| Automation enabled | pass — absent |
| Safety badges visible | pass |
| Diagnostics dominate | **FAIL** — 33 change rows + increment grid sit above the action |
| Connected section says "Connect to Load" | pass |

---

## 2. REVIEW — critique

The panel is an excellent *document* and a poor *cockpit*. It answers "what does
the review think?" thoroughly and never answers "what do I do now?" at a glance.
An operator arriving to choose a target must read a verdict, compare three
columns, absorb five paragraphs and scroll past thirty-three change rows before
reaching the three buttons they came for. When they press one, the only durable
feedback is a spinner string that the panel's own reload erases.

The five facts the operator needs before anything else — **session, proposal
status, actionability, selected target, next required action** — exist in the
payload and are scattered across three regions of the render, one of which only
appears when the proposal is stale.

---

## 3. PLAN — wireframe (1920x1080, above the fold)

```
+= #pm-pdr-host ==============================================================+
| Proposal decision review  [DETERMINISTIC][NO LLM][PREVIEW ONLY][NO ORDERS]  |
|                           [MANUAL REVIEW]                      [↻ Refresh]  |
+-----------------------------------------------------------------------------+
| ── A. DECISION STATUS BAR ── (always rendered, 5 cells, one row) ───────────|
| SESSION        | PROPOSAL STATUS | ACTIONABILITY | SELECTED TARGET | NEXT    |
| 2026-09-22     | READY           | ● NOT         | none            | RUN     |
| latest 09-23   | c780dbeebf5c    |   ACTIONABLE  |                 | PORTFOLIO|
|                |                 |   (STALE)     |                 | CYCLE   |
+-----------------------------------------------------------------------------+
| ── B. SUPERSESSION / STALENESS EXPLAINER ── (only when not actionable) ─────|
| ⚠ This proposal belongs to September 22. September 23 is now the latest     |
|   eligible session. The old proposal is preserved for review but cannot be  |
|   selected or approved.                                                     |
|   [ RUN PORTFOLIO CYCLE ]   ← the single authoritative next action          |
+-----------------------------------------------------------------------------+
| ── C. STEP 1 — CHOOSE TARGET ── (only when actionable) ─────────────────────|
| VERDICT: MINIMAL_REPAIR_PREFERRED · Review the minimum repair first         |
| [SELECTION IS NOT APPROVAL] [NO ORDERS] [MANUAL REVIEW]                     |
| +--------------+ +----------------------+ +---------------------+           |
| | Current /    | | Minimum Repair       | | Full Target         |           |
| | Defer        | |         [RECOMMENDED]| |                     |           |
| | 25 pos  0%   | | 14 pos  21.1% turn   | | 20 pos  35.0% turn  |           |
| | 13 open oblig| | 0 open obligations   | | 0 open obligations  |           |
| | [Keep current| | [Select minimum      | | [Select full        |           |
| |  / Defer]    | |  repair]             | |  target]            |           |
| +--------------+ +----------------------+ +---------------------+           |
+-----------------------------------------------------------------------------+
| ── D. STEP 2 — CONFIRM THE SELECTION RESULT ── (#pdr-select-result) ────────|
| PERSISTS across re-render. One of four states:                              |
|   IDLE    : "No target selected yet. Choose one above."                     |
|   PENDING : "⏳ PENDING — recording MINIMUM_REPAIR. Not yet persisted."      |
|   OK      : "✔ SELECTED: MINIMUM REPAIR                                     |
|              Selection ID psel_2026-09-…   Proposal reap_…_c780dbeebf5c     |
|              Status CREATED · CURRENT · actionable                          |
|              [ APPROVE MINIMUM REPAIR ]  ← correct next control             |
|              Selection is not approval."                                    |
|   ERROR   : "✖ HTTP 401 · Invalid or missing API key.                       |
|              Safe retry: reconnect with your API key, then Select again.    |
|              Nothing was written."                                          |
+-----------------------------------------------------------------------------+
| ── E. WHY THIS REVIEW PATH ── <details> collapsed by default ───────────────|
| ── F. THREE-STATE COMPARISON ── <details> collapsed ────────────────────────|
| ── G. WHAT THE PROPOSAL CHANGES (33 rows) ── <details> collapsed ───────────|
| ── H. HISTORICAL EVIDENCE ── <details> collapsed ───────────────────────────|
| ── I. AUDIT / ADVANCED (composition, hashes, owners) ── <details> collapsed |
+=============================================================================+
```

**Above the fold at 1920x1080:** A, B (or C), and D. Everything that was
previously above the action — verdict prose, state columns, change rows,
evidence — moves into collapsed `<details>` sections E–I.

### Region purpose

- **A. Decision status bar** — the five facts P0-B names, always present, in one
  scannable row. Useful by construction: every cell is a fact the operator must
  have to act. Never empty; when a value is unknown it says which owner did not
  supply it.
- **B. Staleness explainer** — plain English, names both dates, states that
  history is preserved, and carries the one authoritative next action.
- **C. Step 1** — three target cards; a card is only clickable when the backend's
  freshness-aware copy says `selectable`. A blocked card shows the backend's own
  blocker code and detail and carries no click handler at all.
- **D. Step 2** — the write's state machine, rendered from `window._pdrSelectUi`,
  which lives **outside** the panel and is re-rendered after every reload, so a
  result survives refresh and navigation until legitimately superseded.
- **E–I** — evidence, risk tables and diagnostics, collapsed.

### Safety badges

`PREVIEW ONLY`, `NO ORDERS`, `MANUAL REVIEW` in the header; `SELECTION IS NOT
APPROVAL`, `NO ORDERS`, `MANUAL REVIEW` on Step 1; `CREATES TRADE DECISIONS
ONLY`, `ORDERS DISABLED`, `AUTOMATION OFF` on Step 2's success state.

### Out of scope / must stay absent

Create Orders. Order execution. Automation. `alert()`. `confirm()`. Any
browser-side selectability, freshness or economics rule.

---

## 4. Acceptance criteria

| # | Criterion | How verified |
|---|---|---|
| A1 | Status bar renders all five cells in every terminal state | unit + browser |
| A2 | `_pdrSelect` sends `X-API-Key` | source assertion + browser network log |
| A3 | A selection result survives a full `_pdrLoad()` re-render | browser |
| A4 | A failed selection shows HTTP status + backend detail + safe retry | browser |
| A5 | No "Recording selection…" terminal state — PENDING resolves to OK or ERROR | browser |
| A6 | Stale proposal → explainer names both dates + RUN PORTFOLIO CYCLE | unit + browser |
| A7 | Stale proposal → zero enabled target buttons | browser |
| A8 | Successful selection → `APPROVE <TARGET>` control appears | isolated e2e |
| A9 | Step 1 + Step 2 above the fold at 1920x1080 | browser |
| A10 | Change rows / evidence / audit collapsed | browser |
| A11 | No `alert(` / `confirm(` anywhere | audit |
| A12 | No Create Orders, no automation | audit |
| A13 | Backend selectability guard reads the freshness-aware copy | unit |
| A14 | Review → select → read back → approve → order plan → confirm proved end to end | isolated e2e |

## 5. Files changed

- `api/ui/index.html` — regions A–I, `_pdrSelect` state machine, auth header,
  `_pdrStatusBar`, `_pdrStaleExplainer`, `_pdrSelectionResult`, `_pdrApprove*`,
  `_pdrAudit`, `_pdrMore`.
- `api/proposal_decision_review.py` — publishes `selectability_authority`.
- `api/portfolio_decision.py` — the write gate reads the reconciled option copy.
- `api/rebalance_execution.py` — fails closed on a non-implementable selected
  target (`ORDER_PLAN_BLOCKED_SELECTED_TARGET_NOT_IMPLEMENTABLE`).
- `tests/test_r69_1_reallocation_operator_flow.py` — new (50 tests).
- `tests/test_r69_1_governed_selection_e2e.py` — new, isolated end-to-end (21).
- `tests/test_r63_governed_target_selection.py` — slice anchors repaired.

---

## 6. What the evidence showed (P0-A)

The click reached the backend. `paper_trader_8001.stdout.log` records six
`POST /v1/operations/portfolio-decision/select-target → 401 Unauthorized`, with
every GET on the same screen answering 200. No selection was created: the
governed ledger (`target_selections.json` / `target_selection_index.json`) did
not exist at all under `D:\Stock_Prediction_app_data\portfolio_decisions`, and
`load_target_selection` returns `None` for both 2026-09-22 and 2026-09-23.

`_pdrSelect` sent `headers: {'Content-Type': 'application/json'}` and no
`X-API-Key`. The 401 body it received was written into `#pdr-select-result` and
then destroyed ~20 ms later by `_pdrRenderLoading()`, which `_pdrLoad()` — called
by `_pdrSelect` itself — runs first. The operator was left with the verb.

**Three September 22 proposals exist**, not two. All are preserved:

| id | generated | portfolio_state_hash | hoc | universe |
|---|---|---|---|---|
| `f7875b2eba5c` | 18:11:52Z | `a2d6f0c88b64` | `620d1a63c8b6` | `f7756e21ff6a…` |
| `ba1150547920` | 18:28:55Z | `2632630a6c7d` | `a9608a668990` | `6334c2202f2b…` |
| `c780dbeebf5c` | 20:59:08Z | `edc5a31c0f67` | `a9608a668990` | `6334c2202f2b…` |

`ba11 → c780` differ **only** in `portfolio_state_hash`: the portfolio cycle ran
again at 20:59Z with identical research evidence, the live book's marks had moved,
and the proposal identity binds the book state. The index key
`alpha_paper_book_1|2026-09-22` advanced to the newest.

## 7. Acceptance result

- Targeted + regression: `test_r69_1_reallocation_operator_flow.py` (50),
  `test_r69_1_governed_selection_e2e.py` (21),
  `test_r69_proposal_review_availability.py` (29),
  `test_r63_governed_target_selection.py` (65), Stage-19 suites — all pass.
- `scripts\audit_architecture.py --strict` — exit 0, no violations, 0 inventory drift.
- Live backend restart via the canonical owner — `LIVE_SMOKE_OK`, 8/8 authenticated GETs 200.
- Browser acceptance, real `/ui/` at **1920×1080**, isolated profile, real captured
  backend payloads: **46/46 checks pass** (`R691_BROWSER_ACCEPTANCE_OK`), covering
  the historical state, the actionable state, a refused (401) selection, a
  successful selection through to the APPROVE control, and persistence across
  three refreshes plus a full page reload. Panel measured 1678×722 with Step 2
  ending 404 px below the panel top — both steps above the fold.

### A vacuous guard, found and repaired

`test_83` in `test_r63_governed_target_selection.py` asserted that the selection
path contains no `alert(` or `confirm(`, over the slice
`ui[index("function _pdrSelBtn") : index("function _pdrRender")]`. R69 introduced
`function _pdrRenderLoading`, which `"function _pdrRender"` matches as a prefix
and which is defined **above** `_pdrSelBtn`. `str.index` returned the earlier
offset, the slice inverted to `""`, and the assertion passed against an empty
string. The anchors now carry signatures, and
`TestSourceSliceAnchorsAreUnambiguous` makes the anchors themselves the thing
under test.

The same trap was nearly repeated in the browser acceptance: the first run
measured `#pm-pdr-host` while the Reallocation tab was hidden, so the
above-the-fold check passed on a zero-size rect. The script now clicks the tab
and asserts the panel has real dimensions before measuring.
