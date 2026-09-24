# R69.3 — RUNNING DAILY CLOSE: OPERATOR WIREFRAME

Target viewport 1920x1080. First screen, no heavy scrolling. This is a DELTA on the
existing R69.x cockpit: no new page, no new navigation, no new tab, no layout rewrite.

## 1. SCAN — what exists today (read from the live code, not assumed)

| Surface | Field read | Value while run `dcr_2026-09-23_alpha_paper_book_1_20260924T145906` was in flight |
|---|---|---|
| `GET /v1/operations/daily-close` | `close_status` | `DAILY_CLOSE_DUE` |
| " | `primary_action.enabled` | `true` ("Run Daily Close") |
| " | `safe_to_rerun_close` | `true` |
| " | `close_run` | **absent — the payload has no field for a run in flight** |
| `GET /v1/operations/workflow-state` | `overall_state` | `READY_FOR_DAILY_CLOSE` |
| " | `primary_action.action_code` | `RUN_DAILY_CLOSE`, `execution_available: true` |
| " | `daily_close_gate.execution_allowed` | `true` |
| " | `operator_action_code` | `RUN_PORTFOLIO_CYCLE` |
| `GET /v1/operations/daily-close/progress` | `outcome` | `RUNNING`, stage 3/9 |
| UI `index.html` | progress poll | started ONLY inside `runDailyClose()` — a reload stops it forever |

Action classification: every close/cycle control is DB-WRITING and token-gated. The
progress route is READ-ONLY. Nothing in this slice adds a write.

## 2. REVIEW — why today's screen fails

1. **The cockpit invites a duplicate run.** Two enabled CTAs ("Run the Daily Close",
   "Run the Portfolio Cycle") while an authoritative run holds the single-flight lock.
   The POST is safe (`DAILY_CLOSE_IN_PROGRESS`, writes nothing) but the operator is
   sent into the repeat-cycle loop.
2. **The RUN_ID is nowhere on screen.** It exists (`progress.run_id`) and is never
   rendered outside the submitting tab.
3. **Progress dies with the page.** `setInterval` lives in the POST's closure.
4. **A live run is reported as a failure at 45 min.** `updated_at` is stamped only at
   stage boundaries, so a legitimately long stage crosses `_PROGRESS_STALE_MINUTES`
   and the GET flips to `RUN_FAILED_RECOVERABLE` + `safe_retry_allowed: true`.
5. **A false claim of validity.** `data_freshness` publishes "the completed close at
   2026-09-23 remains valid" from the IN-FLIGHT run's bound session.
6. No `alert()` / `confirm()` involved — the existing toast/overlay path is kept.

## 3. PLAN — the wireframe (1920x1080, delta only)

```
+-----------------+--------------------------------------------------------------------------------------+
| SIDEBAR         | TOP STATUS BAR                                                                       |
| Command Center  |  DAILY CLOSE RUNNING · 2026-09-23 · [PREVIEW ONLY][NO ORDERS][AUTOMATION OFF]        |
| Daily Workflow  |  [MANUAL REVIEW][ORDERS DISABLED]                                                    |
| Portfolio Mgr   +--------------------------------------------------------------------------------------+
| Portfolio       | KPI ROW (unchanged)  NAV 97,973.38 | Day -0.73% | Cash 4,482.71 | Holdings 25 | DD -4.17% |
| Research        +----------------------------------------------------------+---------------------------+
| Audit/Advanced  | CENTRAL WORKFLOW COCKPIT                                 | RIGHT ACTION / SAFETY     |
|                 |                                                          |                           |
|                 | +-- RUNNING CLOSE CARD (NEW; replaces the CTA card) ---+ | Primary action            |
|                 | | A DAILY CLOSE IS RUNNING — do not start another.     | |  (disabled, labelled      |
|                 | | RUN ID   dcr_2026-09-23_alpha_paper_book_1_2026..    | |   "Daily Close running")  |
|                 | | SESSION  2026-09-23   STARTED 10:59:06 EDT           | |                           |
|                 | | STAGE    3 of 9 · Recalculate the frozen-model       | | Safety badges             |
|                 | |          decision universe                           | |  PREVIEW ONLY             |
|                 | | ELAPSED  00:33:18   LAST HEARTBEAT 14s ago           | |  CREATES SIGNALS ONLY     |
|                 | | [x]Validate EOD data [x]Value holdings [>]Recalc ... | |  NO ORDERS                |
|                 | | ( ) Evaluate gate ( ) Record decision ( ) Capture .. | |  ORDERS DISABLED          |
|                 | | WROTE SO FAR  owned marks + the 2026-09-23           | |  AUTOMATION OFF           |
|                 | |               performance mark. No journal row yet.  | |  MANUAL REVIEW            |
|                 | | Closing this page does not stop the run.             | |                           |
|                 | +------------------------------------------------------+ | Research next             |
|                 |                                                          |  price_score_refresh is    |
|                 | FOUR-STAGE WORKFLOW STRIP                                |  refreshed BY this stage   |
|                 |  SCAN(done) REVIEW(done) PLAN(active) EXECUTE PREVIEW(-) |  (api.alpha_target        |
|                 |                                                          |   .run_refresh)           |
|                 | COMPACT TABLES (unchanged): holdings, proposal preview    |                           |
+-----------------+----------------------------------------------------------+---------------------------+
| AUDIT / ADVANCED (collapsed): progress document JSON, stage timestamps, heartbeat age, close journal |
+------------------------------------------------------------------------------------------------------+
```

### Acceptance criteria

| # | Criterion |
|---|---|
| A1 | While a run is in flight NO enabled close/cycle CTA renders anywhere (`daily_close_gate.execution_allowed == false`, `primary_action.execution_available == false`, `daily_close.primary_action.enabled == false`). |
| A2 | The RUN_ID, bound session, stage ordinal `N of 9`, stage label and started-at render without the operator having submitted the POST in this tab. |
| A3 | A page reload keeps the progress card and the poll alive (driven by backend state, not by a POST closure). |
| A4 | `safe_to_rerun_close == false` and `requires_close_run == false` while in flight. |
| A5 | A live run is never labelled failed/recoverable: the heartbeat keeps `updated_at` fresh, and the payload separates `stage_changed_at` (stage duration) from `updated_at` (liveness). |
| A6 | No surface claims a completed close for the in-flight session. |
| A7 | The Portfolio Cycle orchestrator refuses with `CYCLE_ALREADY_RUNNING`, naming the close run — never `RECOVERY_REQUIRED`. |
| A8 | Diagnostics (progress JSON, timestamps) stay in Audit / Advanced. No `alert()`, no `confirm()`, no blank button, no "Connect to Load", no Create Orders, no automation. |
| A9 | Nothing in this slice writes: no NAV, no journal row, no proposal, no selection, no approval. |

## 4. EXECUTE PREVIEW — implementation boundary

Owners touched: `api.daily_close` (owns the run record — it gains the projection it
already had the data for), `api.workflow_state` (composes it), `api.data_freshness`
(stops calling an in-flight session a completed close), `api.portfolio_cycle` (refuses
by name), `api/ui/index.html` (renders it). No new store, no new route, no second
close implementation, no automation.
