# Portfolio proposal integrity review — session 2026-09-09 (read-only)

**Verdict: `PORTFOLIO_PROPOSAL_REVIEW_READY`** — with four advisories the operator
must read before approving (section 9).

Conducted 2026-09-10 (UTC) from the R64 worktree against the canonical
checkout and its stores, READ ONLY: no restart, no cycle, no close, no
reassessment, no proposal regeneration, no approval, no snapshot confirmation,
no order, no fill, no holdings / cash / NAV mutation, no corporate-action
registration, no adoption, no promotion. Every number below was read from a
persisted artifact or a live `GET`, and every derived number is shown with its
arithmetic.

## 1. Durable identity (A1)

| object | identity |
|---|---|
| session | `2026-09-09` (eligible market date, valuation date, ranking basis date) |
| governed decision record | `gdec_2026-09-09_alpha_paper_book_1_f90423061b45` (`GOVERNED_PORTFOLIO_DECISION`, provenance `GOVERNED_INTRADAY`, decision `CHANGE_RECOMMENDED`, decided 2026-09-10T00:34:45.562Z, supersedes `gdec_2026-09-08_alpha_paper_book_1_c329a4e57fa9`) |
| candidate | `gcand_2026-09-09_alpha_paper_book_1_f90423061b45`, identity hash `f90423061b45bd3ec1c169ec2ba5a9a2` |
| proposal | `reap_2026-09-09_alpha_paper_book_1_0a29467c0f55`, `proposal_hash 0a29467c0f55009bbe26fc5af0e1556ade434be542ff7ca1d6ba9ea6ad24f280`, written 2026-09-10T00:34:35.82Z, 87,220 bytes under `D:\Stock_Prediction_app_data\reallocation_proposals\artifacts\` |
| constrained target | `solution_hash 8ae9e3e23d7dfbd7334909e6a8ca3b8a883e4889e1bf9881baedcd790abdc931` (`engine.constrained_reallocation`) |
| reassessment | `prs_2026-09-09_alpha_paper_book_1_71e131f4515f`, hash `71e131f4515f03e9425e81434e61eb2c8507b0c970490d79becffa14c992c285` |
| holding opportunity cost | `hoc_2026-09-09_alpha_paper_book_1_4e5bc33f0858`, assessment hash `4e5bc33f0858df944efe73a9523eb2f990d6485d43c7495da6faca3f1ced10db`, evidence hash `320f304f…` |
| scoring | `universe_scoring_hash e5acfed1ee80fa82401fb62b`, `universe_input_contract_hash 9e4a7a4af871179a009a9954` (199 rows) |
| portfolio / economic state | `portfolio_state_hash 0379ad20…`, `economic_state_hash 0c1a0de0…`, `corporate_actions_hash 2d2f3e9694d6dda9c77f54e7179edb10df5c9d3d4c6691a5dbed06287fa90cf8` |
| event cycle | `evt_73914c347367dfed` (00:27:33 → 00:34:35 UTC, 422 s; observation → governed decision 411.6 s) |
| governance gate | `intraday_decision_governance.v1`: 46 passed, 0 failed, 1 NOT_APPLICABLE (`PROPOSAL_BINDING_CONSISTENT`, the no-target rule; `TARGET_HASH_BOUND` applied instead) → `GOVERNED_INTRADAY_DECISION_ELIGIBLE` |

The same `proposal_hash` is bound by the governed record, the reassessment
artifact, the HOC artifact chain, and — live at 03:00 UTC — by
`GET /v1/operations/portfolio-decision` (`PROPOSAL_REVIEW_REQUIRED`, decision
not yet recorded), `GET /v1/operations/reallocation-proposal` (`READY`, not
superseded), `GET /v1/operations/rebalance` (`PROPOSAL_REVIEW_REQUIRED`, bound
to the same proposal) and `GET /v1/operations/workflow-state`
(`REALLOCATION_PROPOSAL_READY`, `consistency_status CONSISTENT`,
`assessment_binding BOUND_AND_CURRENT`). A later event cycle
(`evt_cef26de0b2c51909`, 02:28 UTC) REUSED the same reassessment and was
withheld as `DUPLICATE_CANDIDATE`; the standing governed decision is unchanged.

**The "unavailable" surface.** Classification:
**`PRESENT_AND_READ_TIMEOUT_ONLY`.** The UI renders `Workflow: Unavailable`
only when the `GET /v1/operations/workflow-state` fetch fails or its 45-second
client budget (`_R29_READ_TIMEOUT_MS`) expires, and `COLLECTION READ DID NOT
ANSWER` only when the collection fetch does; both strings are published by the
backend as transport-failure labels and neither is a state. Measured live,
`workflow-state` answers in 7.5 s, `portfolio-reassessment` in 5.7 s,
`active-manager-state` in 3.8 s and `information-collection` in 3.6 s; during
the seven-minute event cycle (scoring, HOC, reassessment, proposal, gate) the
same synchronous process was busy and a queued read can exceed the budget. The
collection service itself reports `RUNNING` (worker pid 89208, heartbeat 18 s
old) and the persisted state is consistent across every owner, so this was a
read that did not answer, not a proposal-state inconsistency. No 5xx appears
in the backend log. Presentation note for later: the workflow-state
`primary_action.explanation` quotes the reassessment's NON-BINDING release-set
estimate (net −0.000716, turnover 0.01847) in a sentence that says the change
"clears the portfolio-level economic gate"; the binding figures are the
constrained target's (section 3).

## 2. MNST corporate action (A2) — `MNST_CORPORATE_ACTION_INTEGRITY_OK`

Immutable fills (`paper_fills.json`, chain-hashed):

| date | side | qty | price | net cash |
|---|---|---|---|---|
| 2026-07-22 | PAPER_BUY | 42 | 95.67 | −4,023.1627 |
| 2026-08-13 | PAPER_BUY | 2 | 46.68 | −93.4767 |

Registry (`corporate_actions.json`): ONE action, `ca_MNST_20260811_5cf0bfa6`,
`FORWARD_SPLIT`, ratio 2.0, ex-date 2026-08-11, registered once on 2026-08-12,
all books. The live registry fingerprint recomputes to `2d2f3e96…`, equal to
the `corporate_actions_hash` every artifact binds — nothing was registered
after the proposal.

Lineage through the ONE projection (`api.corporate_actions.adjust_fills`): the
2026-07-22 fill predates the ex-date → 84 @ 47.835; the 2026-08-13 fill is
post-ex and untouched → 2 @ 46.68; **current quantity 86**, total cost basis
4,023.1627 + 93.4767 = **4,116.64, invariant**. The desk mark series is fully
back-adjusted (2026-07-22 close 47.835; 2026-08-11 45.53; 2026-09-09 42.84),
so 86 × 42.84 = 3,684.24 is the correct market value (proposal row: MNST
`current_market_value` ⇔ `capital_change −3,684.22`, weight 0.037759).

- **Was the split registered once?** Yes. **Applied once?** Yes, in every
  economic owner (`book_nav`, operational book, portfolio state, HOC,
  reassessment, proposal, rebalance). `GET /v1/operational-book` holds MNST 86.
- **Are earlier quantities already split-adjusted?** No: the immutable ledger
  is never rewritten; 42 and 2 are raw; the adjustment is read-time only.
- **Is 44 pre- or post-action?** 44 = 42 + 2 is the RAW unadjusted ledger
  count; it is the pre-action quantity for the first lot.
- **Is 172 economically correct?** No. 172 is the corporate-action REPORT's
  reconciliation preview (`GET /v1/operations/corporate-actions`, scope
  `DESK_BOOK_RECONCILIATION_PROJECTION`, `is_authoritative_nav: false`)
  applying the split TWICE: the registered action (ex 2026-08-11) plus a
  provisional suspect it appends at ex-date 2026-09-09 because its dedupe
  compares `(ticker, latest mark date)` to `(ticker, registered ex-date)`
  while ignoring the scan's own `already_registered: true`. 42 × 2 × 2 + 2 × 2
  = 172. Defect location: `api/corporate_actions.py::load_corporate_action_report`.
  Documented as D-R64-1; not changed in this release.
- **Which NAV is authoritative?** `api.operational_book` via `api.workflow_state`:
  **97,572.00** (cash 4,482.71 + invested 93,089.29; 25 holdings; marks
  2026-09-09).
- **Why three NAVs?** 95,772.72 = raw ledger view (44 shares): 97,572.00 −
  42 × 42.84 = 95,772.72 exactly. 97,572.00 = the authoritative
  corporate-action-corrected replay (86 shares). 101,256.24 = the report's
  double application (172 shares): 95,772.72 + 128 × 42.84 = 101,256.24
  exactly ("phantom removed" 5,483.52 = 128 × 42.84). One economic truth: 86
  shares, 4,116.64 cost basis, NAV 97,572.00.

**Do NOT register the second MNST action the report suggests.** It would
apply the split twice for real.

## 3. Complete target economics (A3)

| quantity | value | check |
|---|---|---|
| current weights (25) | Σ 0.954058; cash 0.045942 (4,482.71 / 97,572) | ✓ 93,089.35 / 97,572 |
| target weights (24) | Σ 0.953412; cash 0.046588 → 4,545.68 | ✓ 24 × w = 93,026.32 |
| gross buys / sells | 34,118.67 / 34,181.72 | Σ 68,300.39 traded notional |
| one-way turnover | 0.350000 | 68,300.39 / 2 / 97,572 = 0.35 (budget 0.35, binding) |
| transaction cost | **85.38** | 68,300.39 × 0.00125 per side, counted ONCE (8.75 bp of NAV) |
| score before / after | 0.852218 / 0.925100 | combined percentile, weighted |
| score improvement | 0.072882 | |
| score-cost hurdle | 0.017500 | two-way 0.70 × 25 bp × 0.001 (see D-R64-2) |
| net improvement | **0.055382** | ≥ hurdle **0.050000** (`min_switching_net_improvement`, frozen, not tuned) |
| complete-target limits | all_ok | turnover 0.35 ≤ 0.35; HHI +0.000873 ≤ 0.02; max sector 0.2373 (IT) ≤ 0.25; largest name 0.0478 ≤ 0.10; 24 ≤ 25 positions; long-only; reconciles; no duplicate; liquidity 25/25 known, ADV floor 10 M and participation ≤ 1.0 ADV pass for every row |
| ideal target | 25 names at 0.04 incl. DDOG at 0.01847, APD, MA, RVTY, TECH, VRTX | withheld: one-way turnover 0.5526 > 0.35 |
| repair | `TRADES_DEFERRED_TO_FIT_TURNOVER_BUDGET`: 15 trades deferred, 23 accepted by score-improvement per unit of turnover | feasible set non-empty; outcome `PROPOSAL_READY` |
| portfolio volatility | 0.11751 → 0.149707 (+3.22 pts, 60-day covariance) | not a declared limit; RISK_DETERIORATION is defined as HHI deterioration and passes |
| expected return | NOT_CALIBRATED (by design; never fabricated) | |

No hidden second cost: the only dollar cost is 85.38; the score-point hurdle is
a conversion of the same trade, applied once in `engine.reallocation_proposal`
and used verbatim by `engine.constrained_reallocation`. The two owners carry
different fallback definitions (0.0175 vs 0.00875); the conservative one was
used and the decision is the same under either (D-R64-2).

**DDOG.** HOC risk contribution 0.122265 at weight 0.03694 (renormalised
0.038719) = 3.16 × weight > the HOC trigger `risk_contribution_excess_multiple`
3.0 → `RISK_CONTRIBUTION_BREACH`, real. Reproduced independently from the
owned trailing panel with the canonical covariance rules (60 aligned sessions,
sample covariance, weights renormalised over the covariance universe): 0.122265
and portfolio variance 5.4796e-5 (vol 0.11751), identical. The ideal target
trimmed DDOG to 0.01847; the turnover budget deferred that trim (DDOG's score
0.99 ranks it 3/199, so the trim buys the least improvement per unit of
turnover) and the constrained target RETAINS DDOG at 0.03694. It is
nevertheless permissible under both rules because the portfolio around it
changes: recomputed on the 24-name target, portfolio volatility rises to
0.149707 and DDOG's risk contribution falls to **0.084391 (2.18 × weight)**,
below the HOC 3× trigger and far below the complete-target cap of 0.25. The
binding risk condition is resolved by restructuring, not by the trim.
Advisory: in the same target SNDK carries 0.2072 of portfolio risk at 0.04
weight (4.94×) and ALAB 0.1776 (4.23×); both pass the 0.25 cap but both would
trigger the HOC 3× REDUCE signal at the next reassessment (D-R64-3).

## 4. Target membership (A4) — 24 names, 23 changes

From the immutable `allocations` block (33 rows = 25 held + 8 added; no
duplicate ticker; every current holding present):

- **EXIT (9):** AIZ, CAT, CVS, DVA, EOG, ITW, KEYS, MNST, SPG
- **REDUCE (1):** LH 0.039491 → 0.033822
- **INCREASE (5):** ABNB → 0.04, ALAB 0.015401 → 0.04, ANET → 0.038945, DXCM → 0.04, EXPE → 0.04
- **ADD (8):** DASH 0.037759, EXPD 0.04, FOX 0.04, FOXA 0.04, IQV 0.04, NWS 0.04, NWSA 0.033565, SNDK 0.04
- **RETAIN (10):** AMD, DDOG, DVN, FANG, FTNT, GWW, HST, LYV, VLO, XYZ
- **REPLACE pairs:** 0 / 0 (the HOC's "REPLACE 6" is a signal-level count before the portfolio gate; the reassessment's post-gate counts are HOLD 24 / REDUCE 1 / ADD 10)

9 + 1 + 5 + 8 = **23 positions change**; 25 − 9 + 8 = **24 target names**;
`membership_change_count 17`, `resize_change_count 6`. Not stale: bound to the
current portfolio state, the current registry fingerprint and the standing
assessment (`PROPOSAL_BOUND_TO_STANDING_ASSESSMENT`). Advisory: FOX/FOXA and
NWS/NWSA are two share classes of two issuers, 8.0 % and 7.36 % of NAV per
issuer; both are inside the 10 % name cap, but the estate has no issuer-level
constraint.

## 5. Legacy target isolation (A5)

The `MODEL TARGET SNAPSHOT REVIEW — LEGACY COMPATIBILITY` card is
`api.alpha_target` / `api.multi_horizon_ledger`: a 25 × 4 % ranked snapshot
(`READY_TO_CONFIRM`, `snapshot_confirmation_allowed: true`, portfolio valuation
date 2026-07-20) whose only confirmed snapshot is `mhz_20260721_d05f6eda96`
(2026-07-21, the book's inception target). It shares no id, hash, store or
token with tonight's proposal: governed approval is
`POST /v1/operations/portfolio-decision/record` with
`CONFIRM_PORTFOLIO_REBALANCE_DECISION` bound to `expected_proposal_hash`; the
order plan is `POST /v1/operations/rebalance/confirm-order-plan` with
`CONFIRM_APPROVED_PORTFOLIO_REBALANCE_ORDER_PLAN` and refuses any proposal
whose hash or registry fingerprint moved (`STALE_PROPOSAL_REVIEW_REQUIRED`).
The legacy path needs `CONFIRM_MHZ_PAPER_SNAPSHOT` and then
`CONFIRM_PAPER_DESK_CREATE_ORDERS`; the desk "Create Paper Orders" button is
hidden while the alpha book exists. Isolation is by identity and by token,
not by a backend refusal of the legacy route while a book is open (D-R64-4):
do not confirm the legacy snapshot tonight.

## 6. Verdict criteria

| criterion | result |
|---|---|
| durable proposal identity | ✓ |
| current NAV reconciled | ✓ 97,572.00 |
| MNST corporate action reconciled | ✓ `MNST_CORPORATE_ACTION_INTEGRITY_OK` |
| target economics valid | ✓ |
| turnover / cost valid | ✓ 0.35 / 85.38 |
| risk constraints valid | ✓ (declared limits all pass; DDOG resolved; advisory on SNDK/ALAB) |
| exact target membership reconciled | ✓ 24 names / 23 changes |
| legacy target cannot be confused | ✓ by identity and token (advisory: no backend refusal) |

## 7. Exact manual operator sequence (NOT performed here)

The cockpit exposes the REVIEW of the proposal (Portfolio → Reallocation
Proposal card, Today hero "Review the proposed portfolio change") and the
Stage-19 lifecycle card; it deliberately has no approval button. The two
DB-writing steps are explicit API calls with the service key
(`X-API-Key`, the value the running server was started with — the User-scope
`PAPER_TRADER_SERVICE_API_KEY`, not the `.env` value).

1. Review in the UI: Portfolio → REALLOCATION PROPOSAL — MANUAL REVIEW
   REQUIRED (24 names, 10/5/1/9/8/0, turnover 0.35, cost 85.38, net +0.055382
   vs 0.050). Confirm the hash shown is `0a29467c0f55…`.
2. Record the governed decision (Stage 18):

   ```powershell
   $h = @{ 'X-API-Key' = [Environment]::GetEnvironmentVariable('PAPER_TRADER_SERVICE_API_KEY','User') }
   $body = @{ decision = 'APPROVE_FOR_PAPER_REBALANCE'; confirmation = 'CONFIRM_PORTFOLIO_REBALANCE_DECISION';
              expected_proposal_hash = '0a29467c0f55009bbe26fc5af0e1556ade434be542ff7ca1d6ba9ea6ad24f280';
              requested_by = 'manual_operator' } | ConvertTo-Json
   Invoke-RestMethod -Method POST -Uri http://127.0.0.1:8001/v1/operations/portfolio-decision/record -Headers $h -ContentType 'application/json' -Body $body
   ```
   Expect `portfolio_decision_state PROPOSAL_APPROVED` bound to that hash.
   (`REJECT` / `HOLD` are the other two legal decisions.)
3. Read the order plan: `GET /v1/operations/rebalance` →
   `PROPOSAL_APPROVED_ORDER_PLAN_REVIEW_REQUIRED` with the deterministic plan
   (24 target rows, NEXT_CLOSE). If it reports
   `ORDER_PLAN_BLOCKED_MISSING_OWNED_MARKS`, use the UI's "Refresh target
   marks" (token `CONFIRM_REBALANCE_TARGET_MARK_REFRESH`) and re-read.
4. Confirm the order plan (Stage 19, the ONLY step that creates paper orders):

   ```powershell
   $body = @{ confirmation = 'CONFIRM_APPROVED_PORTFOLIO_REBALANCE_ORDER_PLAN'; requested_by = 'manual_operator' } | ConvertTo-Json
   Invoke-RestMethod -Method POST -Uri http://127.0.0.1:8001/v1/operations/rebalance/confirm-order-plan -Headers $h -ContentType 'application/json' -Body $body
   ```
   Expect `ORDER_PLAN_CONFIRMED_PAPER_EXECUTION_PENDING`. Fills settle at the
   first completed owned close on/after today through the next Daily Close.
5. Do NOT: register the second MNST action the corporate-action report
   suggests; confirm the legacy alpha-target snapshot; click "Create Paper
   Orders" on the desk.

## 8. Method

Persisted artifacts under `D:\Stock_Prediction_app_data` (portfolio_decisions,
reallocation_proposals, portfolio_reassessments, holding_opportunity_cost,
corporate_actions), the live desk ledger under
`C:\Users\binis\.paper_trader\paper_trading_desk` (fills, marks, books) and the
owned trailing price panel were read directly; fourteen live `GET`s were made
with the service key and their latency recorded; the covariance and the
adjust-fills projection were reproduced in standalone arithmetic and matched
the owners to the last digit. The canonical checkout's tracked tree was not
modified; its untracked files and their hashes were unchanged against the
start-of-run baseline.

## 9. Advisories carried forward (documented, not fixed here)

1. D-R64-1 — corporate-action report double-applies a registered split as a
   provisional suspect (preview only; never register it).
2. D-R64-2 — the score-cost hurdle carries two definitions across two owners;
   the conservative one governs.
3. D-R64-3 — the HOC 3×-weight trigger and the complete-target 25 % cap are
   different rules; SNDK and ALAB would trigger the former next.
4. D-R64-4 — the legacy snapshot / desk order path is UI-hidden, not
   backend-blocked while a book is open.
