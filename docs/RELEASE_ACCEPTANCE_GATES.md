# Paper Trader — Binding End-to-End Release Acceptance Gates

Version 2026-09-27. These are release criteria, not a new orchestration implementation. Reuse existing canonical owners. Classify every item PASS / FAIL / NOT_DUE / NOT_APPLICABLE with an artifact ID, timestamp and source. Do not convert NOT_DUE or unknown to PASS.

## Gate 0 — Authoritative identity and protected state

- Capture local HEAD and dirty state; backend loaded commit/PID, worker loaded commit/source; latest eligible session, real session end, canonical run ID/final stage; live process/task state. Verify the exact expected branch and the intended source-revision at use.
- Capture authoritative NAV/holdings/cash, proposal ID/hash/decision state, forward registry and durable research rulings. Identify existing records to preserve. Do NOT re-run an already closed cycle or recreate an existing proposal.
- Declare ALREADY DONE / ALREADY TRIED–FAILED / CURRENT BLOCKERS / NEW WORK. Confirm tests cannot stop/reconfigure a real Windows scheduled task or contaminate production research memory. Do not disclose credentials.

## Gate 1 — Data and immutable chronology

- Latest eligible session and input freshness per feed; slower-moving inputs explicitly stale/valid/blocked; correct source event and availability timestamps, inactive/delisted coverage and PIT admissibility.
- Actual owned/non-owned dataset entitlement and formal purchase gate; missing stays missing. No current snapshot masquerading as historical data; no invented missed prediction or silent backfill.

## Gate 2 — Operational book and canonical economics

- Same session and economic-state hash for authoritative marks, NAV, holdings, cash and risk. No duplicate close, ledger event or NAV record on replay.
- Reconcile HOC source retention verdicts, published post-churn actions, hard repair obligations and complete-target treatment ticker by ticker. Reconcile adjustment-log history with FINAL deferred-trade ledger and explain superseded repair rounds. Reconcile cross-asset risk liquidity with HOC and trade capacity; unavailable inputs remain explicit.
- Current book's actual paper P&L, costs and drawdown are measured from canonical ledgers, not inferred from ranking score.

## Gate 3 — Complete allocation and review

- One persisted existing/proposed ID/hash, with CURRENT / MINIMUM_REPAIR / FULL_TARGET on the same state; position weights, cash, dollars, costs, one-/two-way turnover, covariance/risk contribution, concentration, volatility/drawdown and capacity.
- Explain every proposed exit/reduction/addition, every mandatory repair, and any remaining obligation. Report issuer-level class-share aggregation when relevant. Explicitly report non-equity eligible/admitted count and reason if zero.
- Calibrated expected return absent -> NOT_CALIBRATED; do not turn percentile score into dollars/return. Distinguish non-binding pre-proposal estimate from complete-target economics. Manual selection/approval and any execution boundary stay separate; no order/fill or automatic promotion.

## Gate 4 — Alpha discovery and skeptical evidence

- Read the durable ruling store and full mechanism key before choosing. Use existing twelve agents and existing runner; no repeated settled experiment or purchased-data request before gate.
- For executable hypothesis, freeze its spec and execute historical OOS experiment. Report code/experiment ID, certified PIT coverage, training/OOS dates, benchmark and controls, gross and NET P&L at declared capital, trading costs/borrow, turnover, drawdown, effective sample, uncertainty, skeptic and risk verdict, forward eligibility.
- If two economically distinct paths fail binding gates, identify the exact refusal or missing dataset, formal acquisition decision, and independent work that remains executable. Never claim alpha from no-run campaigns.

## Gate 5 — Prospective producer and registry

- Read a COMPLETED live journal and source identity, including scheduled producers, pre-boundary readiness, data publication, information timestamp, and entry/valuation contract feasibility.
- Verify immutable readback from the canonical registry. Separate NEW emitted, pending, matured, effective independent observations and permanently missed/forfeited boundaries. No Saturday/weekend invention or hindsight reconstruction. A date not on the owner's decision grid is NOT_DUE.
- At-use source attestation must accept the loaded worker at the intended HEAD; backend/worker identities agree. Do not restart a dirty checkout and claim this fixed it. Reverify after a governed commit/restart before a legitimate boundary.

## Gate 6 — Tests, live UI and release authority

- Targeted changed-owner tests and impacted regressions; strict `scripts/audit_architecture.py --strict`; `scripts/check_ui_js.py` if UI changed. Full suite only at shared-core/architecture/milestone risk. Capture exact failure and prove pre-existing status against baseline before labeling it pre-existing.
- Actual saved/reopened artifacts and live authenticated owner GET readback; UI displays the same authoritative ID/hash/date/marks/reconciliations with no rival calculations. Running-status or `COMMITTED_CLEAN_SOURCE` at STARTUP is not proof of at-use permission.
- Operator receives four separated Windows PowerShell phases: (1) validation, (2) commit only after `COMMIT_OK`, (3) authorized restart and worker reattestation, (4) read-only/UI smoke. Push is a distinct explicitly authorized phase. No interactive `exit`, no hand-pasted split `if/else`.
- Print `COMMIT_OK` only if all applicable release gates for the changed scope pass; otherwise `DO_NOT_COMMIT: <specific gate and owner>`. Do not assert a release deployed when only a patch/package was prepared.

## Mandatory final report shape

**PORTFOLIO**: session, NAV, existing proposal ID/hash/state; CURRENT / MINIMUM_REPAIR / FULL_TARGET, costs, cash, weights, risk, obligations, non-equity status, outstanding human decision.

**ALPHA**: candidate and four-part mechanism, ruling and data certification, code/experiment ID, historical gross/net/OOS results if actually run, skeptic/risk verdict, exact missing evidence or binding gate.

**FORWARD**: completed live run ID and loaded source, NEW immutable count and registry readback, pending/matured, deadline readiness and forfeitures.

**RELEASE**: local/runtime identities, impacted tests and strict audit, known failures, `COMMIT_OK`/`DO_NOT_COMMIT`, exact phased commands; explicit no-push/no-orders/no-approval/no-purchase/no-promotion status.
