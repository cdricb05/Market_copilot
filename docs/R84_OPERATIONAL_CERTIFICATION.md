# R84 — Final operational certification: repairs, wireframe notes, acceptance

Date: 2026-09-29. Branch `stage19-controlled-rebalance` over `8032476`.

R84 is a certification-and-repair release. It adds no screen and removes no
section; every UI change corrects a panel that contradicted an authoritative
backend owner, or retires a control that bypassed governed execution. The
layout (left sidebar, header status bar, Today three-answer row, hero decision
card, snapshot, Portfolio tabs, Audit / Advanced) is unchanged.

## Wireframe notes (1920x1080, changed regions only)

```
TODAY
+-[1 CURRENT AUTHORITATIVE DECISION]-[AUTHORITATIVE][CURRENT SESSION]-+
| PORTFOLIO CHANGE WITHHELD                                            |
| <withheld reason: AMD 0.150305 / SNDK 0.154483 vs 0.15>              |
| Session Sep 28 · Decided Sep 28 7:44 PM ET · Governed daily cycle     |
| Last ledger record (historical): Sep 25 · Change recommended          |
+-----------------------------------------------------------------------+
+-[2 WHAT CHANGED]  It concluded: Target withheld  ----------------------+
+-[HERO]  GUIDANCE: Review the portfolio limit that withheld the change  -+
|          NEXT [Review the portfolio limit that withheld the change]     |

PORTFOLIO > REALLOCATION
| Target selection: No target is selectable for this session ...        |
| ECONOMICS  WITHHELD COMPLETE TARGET · priced, not approvable          |
|   +0.067127 net · 35.0% turnover · $85.97 · 20 positions              |
|   Breaching name | Risk contribution | Limit | Excess                 |
|   AMD            | 0.150305          | 0.15  | +0.000305              |
|   SNDK           | 0.154483          | 0.15  | +0.004483              |

DAILY WORKFLOW (book card)
| Book lifecycle: Monitor Holdings and Performance — ...                 |
|   · Operator action (one): Review the portfolio limit ...             |
```

## Acceptance criteria (all verified in the browser at 1920x1080)

1. Today card 1, hero guidance, hero NEXT, and workflow `today_hero` name the
   same ONE operator action (`api.workflow_state._decide_overall`).
2. A governed ledger row from an earlier session is labelled historical.
3. No surface says PROPOSAL READY / Proposal available for a WITHHELD target.
4. No enabled control creates legacy decisions, orders or fills; no enabled
   bootstrap order path can change a funded book.
5. No `alert()` / `confirm()`; no blank buttons; no "Connect to load".
6. Safety badges (PAPER ONLY, NO ORDERS, MANUAL REVIEW, AUTOMATION OFF) remain.

## Defects repaired (regression: `tests/test_r84_operational_certification.py`)

| ID | Owner | Defect | Repair |
|----|-------|--------|--------|
| D1 | api.workflow_state | withheld complete target's retention exits scored as silence → INCONSISTENT | withheld verdict names them |
| D2 | api.workflow_state, api.operator_presentation | hero CTA / guidance / NEXT contradicted the ONE action | projected from the ONE action |
| D4 | api.app | legacy archive order / fill / decision writes live | HTTP 410 `LEGACY_ARCHIVE_EXECUTION_PATH_RETIRED`; UI never sends them |
| D5 | api.paper_trading_desk, api.alpha_book | token-only order paths could rewrite a live book with no approval | refused for a funded book (`BOOTSTRAP_ORDER_PATH_CLOSED_FOR_A_LIVE_BOOK`) |
| D6 | api.active_manager_state | Sep-25 ledger row presented as the current decision | current withheld verdict answers; ledger row labelled historical |
| D7 | api.active_manager_state | withheld built target read "Proposal available" | `TARGET_WITHHELD` lane conclusion |
| D8 | api.runtime_identity (+2) | published collection-restart command lacked mandatory `-RepoRoot` | command binds `-RepoRoot` |
| D9/D10 | UI | "choose in step 1" with no step 1; "no target was priced" for a priced target | backend-driven wording + breach table |
| D11 | UI | header "Service: Timeout" stuck after load contention | bounded slow re-check |
| D12 | api.app route + UI | assessment card "PORTFOLIO PROPOSAL READY" on a withheld session | lane-aware composition |
| D13/D14 | api.information_collection, api.material_information | read the event pointer, not the run summary | read the recorded summary |
| D15 | api.portfolio_state | claimed Slice 7 "not implemented yet" | truthful note |
| D16 | api.opportunity_frontier | admitted / blocked non-equity counts implicit | explicit counts + gap class |
| D20 | UI | Daily Workflow book card "Current task / Next action: Monitor" | "Book lifecycle" + the ONE action |
| D21 | api.portfolio_decision | approval gate "Select a target" when nothing is selectable | `CHANGE_CANDIDATE_WITHHELD` → review the limit |
| D22 | engine.market_session | "no holiday calendar" warning while the NYSE calendar is supplied | truthful label / warning |
| D23 | api.data_freshness | quarterly input past its 10-Q deadline reported NOT_DUE | filing-deadline-aware rule |
| D26b | api.workflow_state | on the session's own evening the primary headline / Today hero still read "Catch up — <session> was not closed." beside DAILY CLOSE DUE | `word_session_close_action` words `normal_close_due` as "Daily Close due" |
| D27 | tests (harness) | the broad suite opened the LIVE R59 research memory read-write (schema migration by a test run) and nothing mechanically stopped any other test from writing production | `tests/_production_write_guard.py`: audit hook + sqlite authorizer + Postgres statement guard over `D:\Stock_Prediction_app_data`, `~\.paper_trader` and the production database; any refusal fails the session; R59 root served from a per-session snapshot (`tests/test_r84_production_write_guard.py`) |

Legacy archive opt-in for its own hermetic suite only:
`PAPER_TRADER_LEGACY_ARCHIVE_EXECUTION_ENABLED=true` (default false).
