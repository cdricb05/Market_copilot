# R82.1 — THE RISK POLICY DECISION, END TO END

Mandatory wireframe. Produced **before** any code, as `CLAUDE.md` requires.
Target viewport **1920x1080**. The first screen must not require heavy scrolling.

Route: `http://127.0.0.1:8001/ui/#portfolio-manager/reallocation`
Panel: `#pdr` — Proposal decision review.

---

## 1. SCAN — what exists today

| Thing | Owner | State before R82.1 |
|---|---|---|
| `GET /v1/operations/proposal-decision-review` | `api.proposal_decision_review` | serves review + `selected_targets` + `governance.selection` + `risk_policy_ruling` |
| `POST …/portfolio-decision/select-target` | `api.portfolio_decision.record_target_selection` | **write**, token `CONFIRM_PORTFOLIO_TARGET_SELECTION`, UI control exists (`_pdrSelect`) |
| `POST …/portfolio-decision/record` | `api.portfolio_decision.record_decision` | **write**, token `CONFIRM_PORTFOLIO_REBALANCE_DECISION`, UI control exists (`_pdrApprove`) |
| `POST …/portfolio-decision/risk-policy-ruling` | `api.portfolio_decision.record_risk_policy_ruling` | **write**, token `CONFIRM_RISK_POLICY_RULING`, **NO UI CONTROL AT ALL** |
| Risk-policy panel | `_pdrPolicyReview` / `_pdrRuledPolicy` | read-only. Says "the ruling is recorded through the governed decision API, not from this screen" |
| The repair solver | `engine.proposal_decision_review.solve_minimum_repair` | derives its per-round cap from `_hoc.risk_contribution_limit(n)` — always 3/N, never a held cap |
| Ruling provenance | — | **does not exist.** `ruled_by` is a free string the caller supplies |

Read-only vs write on this screen: `_pdrLoad` (read), `_pdrSelect` (write),
`_pdrApprove` (write). Diagnostics already live in `_pdrAudit` behind
`_pdrMore('Audit / advanced', …)`. No `alert()` / `confirm()` anywhere in the
region. No Create Orders control. Nothing is blank-labelled.

## 2. REVIEW — the critique, before coding

1. **The decision the panel demands cannot be made on the panel.** R69.5 wrote
   "Approval is withheld until an operator rules on the policy", listed three
   courses as plain `<li>` text, and shipped **zero** write controls. The only way
   to rule was a hand-rolled HTTP POST. That is not an operator workflow; it is a
   developer workflow wearing an operator's label. `writeControlsInRuled = 0` was
   reported as a *success* at R82. For the un-ruled decision state it is a
   **failure**.
2. **The three courses were prose, not options.** `POLICY_REVIEW_OPTIONS` is a
   tuple of English sentences. A screen cannot tell from it which are available,
   and R82's own vocabulary (`RULING_AVAILABLE`, `RULING_NOT_AVAILABLE_THIS_RELEASE`)
   was never published to the browser.
3. **`ruled_by` is unauthenticated narration.** Any caller can write
   `ruled_by: "operator"`. A record created from a script is indistinguishable
   from one a human confirmed on screen, so the store cannot tell an authoritative
   human ruling from a development artifact.
4. **JUDGE ends at a dead end.** The gate returns
   `next_required_action: SELECT_A_COMPLIANT_TARGET_OR_REJECT` — and no compliant
   target exists to select. The operator is told to choose something the system
   never built. An active portfolio manager must construct the policy-compliant
   successor, not name a requirement.
5. **The KPI strip and the warn banner disagreed with the body** (fixed at R82:
   `0` vs `2 RULED`, "1 BREACH" vs two attributed). Same family of defect: a
   number rendered from a narrower list than the one the panel argues from.

Not found (checked, clean): vertical stacked cards, empty Overview cards, heavy
Daily Plan scrolling, hidden safety badges, diagnostics on the main surface,
`alert()`, `confirm()`, enabled Create Orders, enabled automation.

## 3. PLAN — the layout at 1920x1080

The panel keeps its existing shape: status bar, then numbered steps, then
collapsed evidence. Two steps are added and one is rewritten. Nothing above the
fold moves, so the first screen's height is unchanged when no ruling is owed.

```
┌─ Proposal decision review ───────── DETERMINISTIC · NO LLM · PREVIEW ONLY · NO ORDERS · MANUAL REVIEW · [↻ Refresh] ─┐
│ Session 2026-09-25 │ Proposal READY │ ACTIONABLE │ Selected MINIMUM REPAIR │ Next: RECORD RISK POLICY RULING        │
├──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Step 1 — choose a target                        Step 2 — selection result (backend)                                  │
│ [CURRENT] [MINIMUM REPAIR ✓] [FULL TARGET]      ✓ SELECTED: MINIMUM REPAIR · psel_…eaee484fa4a0                       │
├──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Step 3 — the exact target you selected (MINIMUM REPAIR)   FROZEN AT SELECTION · PREVIEW ONLY · NO ORDERS             │
│ ┌ Positions 14 ┬ Changes 12 ┬ Turnover 21.19% ┬ Cost $52.33 ┬ Cash 46.92% ┬ HHI 0.0208 ┬ Obligations 0 [2 RULED] ┐  │
│ Risk contribution — current book vs selected target          (existing table, unchanged)                            │
│ ⚠ 2 BREACHES DISCHARGED BY THE CAP MOVING, NOT BY A REDUCTION    (existing banner, unchanged)                       │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
┌─ Step 4 — RISK POLICY DECISION ── ⚖ YOUR DECISION · APPROVAL WITHHELD · NO THRESHOLD CHANGED · MANUAL REVIEW ───────┐
│ This target clears its own 21.43% cap only because that cap rose from 12.00% when the covariance universe went       │
│ from 25 names to 14. Held at 12.00%, AMD (20.33% of risk) and DDOG (21.41%) breach. Choose the governing policy.     │
│                                                                                                                      │
│  ( ) ACCEPT AS IS — keep the target's own dynamic 3/N cap                                                            │
│      The cap is a relative-concentration rule and 46.92% of capital is cash. AMD and DDOG stay as selected.          │
│      → Does NOT approve the proposal. Approval stays a separate manual action.                                       │
│                                                                                                                      │
│  (•) JUDGE AGAINST THE BEFORE UNIVERSE — the 12.00% cap binds this frozen decision                                   │
│      A breach raised under the 25-name universe must be repaired by reducing the name, not by shrinking the          │
│      universe around it. AMD and DDOG reopen as mandatory REDUCE obligations and Paper Trader solves the             │
│      policy-compliant successor target below.                                                                        │
│      → Does NOT approve the proposal.                                                                               │
│                                                                                                                      │
│  ( ) ADD AN ABSOLUTE COMPANION FLOOR                          [UNAVAILABLE IN THIS RELEASE]   (input disabled)      │
│      Needs an absolute per-name risk threshold X that no owner declares. Recording it here would invent that         │
│      threshold inside a ruling recorder. It stays a separate policy decision.                                        │
│                                                                                                                      │
│  [x] I am the portfolio operator and I am recording this ruling for this frozen book.                                 │
│  [ RECORD RISK POLICY RULING ]  [ Cancel ]                                                                           │
│                                                                                                                      │
│  Binds: proposal reap_…eaee484fa4a0 · selection psel_…eaee484fa4a0 · book b099e628c030 · cap 12.00% · AMD, DDOG      │
│  Recorded by api.portfolio_decision. Not an approval. No order plan, order or fill.                                  │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
┌─ Step 5 — POLICY-COMPLIANT SUCCESSOR TARGET ── SOLVED UNDER THE RULED CAP · PREVIEW ONLY · NO ORDERS ──────────────┐
│ ┌ Positions 14 ┬ Changes 14 ┬ Turnover 26.3% ┬ Cost $XX ┬ Cash 52.0% ┬ HHI 0.0XXX ┬ Obligations left 0 ┐            │
│ Ticker │ Weight selected → solved │ Share of risk │ Ruled cap 12.00% │ Obligation                                   │
│ AMD    │ 5.11%  →  2.94%          │ 20.33% → X.X% │ complies         │ CLOSED · REDUCE_TO_LIMIT                     │
│ DDOG   │ 4.24%  →  2.14%          │ 21.41% → X.X% │ complies         │ CLOSED · REDUCE_TO_LIMIT                     │
│ Solved by engine.proposal_decision_review (the same repair owner), risk re-measured each round by the canonical      │
│ covariance owner. N rounds, converged. This is not a new proposal and not a second optimiser.                        │
│ [ SELECT THIS TARGET ]   ← the ordinary governed selection write; then the ordinary APPROVE gate applies             │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
  ▸ Why this review path   ▸ Compare the three states   ▸ What the proposal changes   ▸ Audit / advanced
```

Steps 4 and 5 render **only** when the backend says so, so a book that owes no
ruling keeps today's height exactly:

* **Step 4** renders when `risk_policy_decision.required === true`.
* **Step 5** renders when `policy_compliant_successor` is present, which happens
  only under an authoritative `JUDGE_AGAINST_THE_BEFORE_UNIVERSE`.

When a ruling IS authoritative, Step 4 collapses to the existing
`_pdrRuledPolicy` statement block (⚖ RISK POLICY RULED) and the controls are
gone — a decision already taken is not re-offered as a form.

### The unverified-provenance state

A ruling on record whose provenance was never verified is **not** authoritative.
Step 4 then renders the controls AND an amber disclosure above them:

```
│ ⓘ A PRIOR RULING IS ON RECORD BUT ITS PROVENANCE IS NOT VERIFIED      [UNVERIFIED] [AUDIT EVIDENCE ONLY]            │
│ prul_2026-09-25_…_b099e628c030 · JUDGE_AGAINST_THE_BEFORE_UNIVERSE · recorded 2026-09-28T17:12:13Z                   │
│ It was not submitted through this screen, so it is kept as readable audit evidence and binds nothing. Approval       │
│ stays withheld. Your ruling below supersedes it through the normal governed revision path.                           │
```

### Data contract the browser consumes (no arithmetic in the browser)

| Rendered thing | Backend field |
|---|---|
| option list, order, availability | `risk_policy_decision.options[].{ruling,title,available,unavailable_reason,plain_english[],does_not_approve}` |
| confirm token | `risk_policy_decision.confirm_token` |
| single-use submission token | `risk_policy_decision.submission_token` |
| bound identity shown + sent | `risk_policy_decision.binds.{proposal_id,selection_id,selected_target_implementation_hash,reference_limit,instruments}` |
| every cap / share / weight / excess | `risk_policy_ruling.*`, `selected_target_implementation.risk_contribution.*`, `policy_compliant_successor.*` |
| successor economics | `policy_compliant_successor.implementation.economics.*` |

No cap, share, weight, excess, turnover or cost is derived in the browser. The
region may not contain `new Date(`, `Date.now(`, `.getTime(`, `.reduce(`,
`Math.`, `cost_rate`, `COST_BPS` or the bare word `compute`
(`scripts/audit_architecture.py::UI_RP_FORBIDDEN`).

## 4. Acceptance criteria

1. An un-ruled risk-policy-review state renders the **RISK POLICY DECISION**
   controls: at least one `<input>` and one enabled submit `<button>`.
   `writeControlsInDecisionStep > 0`.
2. No ruling is written before the human presses the button — the panel's own
   load performs no write, and the live store is byte-identical after a page load.
3. Every available option is rendered from `risk_policy_decision.options`, which
   the backend derives from `engine.selected_target.RULING_AVAILABLE`. The browser
   holds no ruling vocabulary of its own.
4. `ADD_AN_ABSOLUTE_COMPANION_FLOOR` renders with a visible
   `UNAVAILABLE IN THIS RELEASE` badge, its input carries `disabled`, and the
   backend refuses it at `RULING_NOT_AVAILABLE_IN_THIS_RELEASE` if sent anyway.
5. The submit button is inert until the confirm checkbox is ticked, and the POST
   carries `confirmation: CONFIRM_RISK_POLICY_RULING`. No `confirm()` / `alert()`.
6. The persisted ruling binds proposal, selection id, frozen-book hash, reference
   limit and instrument set, and is refused on any mismatch.
7. Under `JUDGE`, AMD and DDOG are OPEN `RISK_CONTRIBUTION_CAP` obligations
   against 12.00%, and the successor is solved by
   `engine.proposal_decision_review.solve_minimum_repair` — the existing owner.
8. The successor's weights are the **solved** vector, re-measured by
   `engine.reallocation_proposal.portfolio_volatility` each round. The
   first-order `indicative_weight_at_reference` figures are never published as
   solved weights, and the block says which is which.
9. Approval stays unavailable while any ruled obligation is open.
10. If the successor clears every binding obligation, the panel exposes the
    ordinary **SELECT THIS TARGET** control and, after selection, the ordinary
    APPROVE gate. Neither is invoked by the panel.
11. A verified `ACCEPT_AS_IS` records the choice, creates no target, and does not
    approve.
12. A stale proposal / selection / book / ruling fails closed.
13. A ruling cannot be reused against a different frozen book.
14. No test or implementation step writes to the live decision store.
15. No code path sets `actor="operator"` for a machine-generated decision.
16. The 2026-09-25 proposal and selection stay byte-identical.
17. No order plan, order, fill or broker action anywhere.
18. `scripts/audit_architecture.py --strict` exits 0.
19. Targeted / impacted tests pass.
20. Browser acceptance at 1920x1080: no page scroll beyond the viewport for the
    first screen, no blank buttons, safety badges visible in every new block,
    diagnostics still in Audit / Advanced, Create Orders absent, automation off.

## 5. R82.1.1 — the operator-confirmation ceremony

### The defect this closes

R82.1 derived `OPERATOR_UI_CONFIRMED` from two things: the book-bound
`submission_token` the governed read publishes, and `operator_confirmed: true`.
Its own tests showed what that permitted:

```python
token = ruling_submission_token(selection_id=..., ..., reference_limit=...)
ruling_provenance(submission_token=token, confirmed_in_ui=True, ...)
# -> verified=True, channel=OPERATOR_UI_CONFIRMED
```

`ruling_submission_token` is a **pure function** of the frozen identity, so any
in-process caller could mint one, and every caller that reads the governed panel
is served one. `confirmed_in_ui` is the caller's own word. Token plus claim is
evidence of *knowing which book is on the screen* — not of a confirmation. Calling
it verified UI provenance was an overclaim.

### The ceremony

| Act | Who performs it | What it requires | What it produces |
|---|---|---|---|
| **0 — read** | governed read (`risk_policy_decision`) | — | `submission_token`, salted with a per-process secret, bound to the frozen book. **Not evidence on its own.** |
| **1 — open** | `POST /v1/operations/portfolio-decision/risk-policy-ruling/confirmation` | the book token · the typed phrase `CONFIRM_RISK_POLICY_RULING` · **one available ruling** · the live frozen book (read server-side, never from the request) | a random single-use `confirmation` held **only in this process's memory** |
| **2 — spend** | `POST …/risk-policy-ruling` with `operator_confirmation` | the confirmation, unexpired and unspent, bound to **this** book and **this** ruling | `verified=true`, `channel=OPERATOR_UI_CONFIRMED` |

* **Issuance owner**: `api.portfolio_decision.open_ruling_confirmation` — the only
  place a confirmation comes into existence.
* **Consumption owner**: `api.portfolio_decision.consume_ruling_confirmation`,
  called once by `record_risk_policy_ruling` **after** every identity check, so a
  refused ruling never burns the operator's confirmation.
* **Binding fields**: `ruling`, `selection_id`,
  `selected_target_implementation_hash`, `reference_limit`.
* **Lifetime**: `RULING_CONFIRMATION_TTL_SECONDS = 180`, monotonic-clocked.
* **Replay**: the entry is marked consumed the moment it is presented, *before* its
  bindings are judged — a confirmation aimed at the wrong book or the wrong ruling
  is spent by that attempt and cannot be re-aimed.
* **Persistence**: none. A restart invalidates every open ceremony; evidence that
  outlived the process that issued it would be evidence of nothing.
* **Publication**: no read returns a confirmation. A read that handed one out would
  be back to R82.1.

### What the verified channel proves — and does not

It proves the submission spent a single-use confirmation **this backend process**
issued for **this** ruling on **this** frozen book, within its window, never spent
before, to a client that had read this process's governed review.

It does **not** prove a human pressed a key — no check at this layer can, and the
record says so in `does_not_prove`. A client that performs every act of the
ceremony is treated as the operator, because that is what the ceremony *is*. What
is excluded is the R82.1 bypass: becoming authoritative by obtaining the served
token and asserting a boolean.

`actor`, `surface` and `operator_confirmed` are still recorded — as **claims**,
carried next to `actor_is_evidence: false`, `surface_is_evidence: false` and
`asserted_confirmation_is_evidence: false`.

### Direct API calls

Still possible, still recorded, still auditable — and classified
`API_DIRECT_CALL` / `UNVERIFIED_REQUIRES_OPERATOR_CONFIRMATION`. Such a record
binds nothing: no binding cap, no reopened obligation, no satisfaction of the
R69.5 policy review. Approval stays exactly where it was.

### Additional acceptance criteria

21. A caller holding the served `submission_token` and sending
    `operator_confirmed: true` with no confirmation is recorded
    `API_DIRECT_CALL` / `RULING_RECORDED_WITHOUT_VERIFIED_OPERATOR_PROVENANCE`,
    in process **and** over HTTP.
22. No combination of `actor`, `surface` or `confirmed_in_ui` — including the
    literal string `OPERATOR_UI_CONFIRMED` — verifies.
23. A genuine ceremony verifies, and only then does the ruling bind the book.
24. A spent confirmation replays as `OPERATOR_CONFIRMATION_ALREADY_CONSUMED`; an
    expired one as `OPERATOR_CONFIRMATION_EXPIRED`; an invented one as
    `OPERATOR_CONFIRMATION_WAS_NEVER_ISSUED_BY_THIS_BACKEND`.
25. A confirmation for book A is refused for book B
    (`…BOUND_TO_A_DIFFERENT_FROZEN_BOOK`) and spent by the attempt.
26. A confirmation bound to `ACCEPT_AS_IS` is refused for
    `JUDGE_AGAINST_THE_BEFORE_UNIVERSE` (`…BOUND_TO_A_DIFFERENT_RULING`).
27. Opening a ceremony writes nothing durable and rules on nothing.
28. No read publishes a confirmation, and no confirmation reaches any file.
29. The live R82 ruling stays byte-preserved and UNVERIFIED, with no ruling id
    special-cased anywhere in the owner.
30. The screen performs both acts; it cannot manufacture a confirmation.

Regression: `tests/test_r82_1_1_ruling_provenance_ceremony.py`.
