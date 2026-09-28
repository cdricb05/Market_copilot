# Paper Trader — Binding Operating Contract

Version: 2026-09-27. Applies to ChatGPT handoffs, Claude Code, research agents, operator workflows, and release decisions. This is a process contract, not evidence that a live release has been installed or accepted.

## 1. North star and definition of progress

Build an active, research-driven, multi-asset PAPER portfolio manager that continuously compares all existing holdings with the strongest admissible alternatives, produces explainable zero-base reallocation proposals and measures actual paper P&L net of realistic costs. The goal is to find economically defensible alpha and deploy paper cash when governed evidence justifies it. Higher score is not calibrated expected return; modeled or backtested P&L is not realized paper P&L; paper P&L is not alpha unless measured versus a declared benchmark and costs. Never describe code, a test suite, a specification, or a dashboard as delivered investment performance.

A release is progress only if it removes a verified end-to-end blocker or increases demonstrated ability to ingest point-in-time data, maintain the authoritative book, assess opportunity cost, propose a complete portfolio, accrue immutable prospective evidence, execute admissible research, or report actual paper P&L. Avoid cosmetic releases, speculative rewrites, recurring broad plans, and tiny tuning unconnected to those outcomes.

## 2. Mandatory opening on EVERY intervention

Mechanically establish newest authoritative RUN_ID, terminal status, session/date, completed/pending stages, source and runtime identities, authoritative artifact IDs/hashes, exact blocker, and next governed action. Report exactly:

- ALREADY DONE — preserve it; do not redo it.
- ALREADY TRIED / FAILED — include experiment IDs, binding refusals, and why retry would be different.
- CURRENT BLOCKERS — distinguish evidence, inference, safety/entitlement/authorization, and time-dependent constraints.
- NEW WORK — the smallest coherent end-to-end deliverable that advances the north star.

Do not treat a user's historical HEAD, journal excerpt, or stale UI as the latest source of truth. Do not rerun a completed session, duplicate an existing proposal, backfill missed prospective evidence, or silently change a recorded verdict. Read current source, current DB/read model, worker journal and owner outputs before acting. State precisely when access is unavailable.

## 3. One integrated operating system, three different clocks

Signal refresh: often, as data allows, with eligibility/freshness/PIT status. Portfolio reassessment: on every material refresh, compare EVERY holding and validated sleeve against the strongest alternatives; no automatic fixed-daily rebalance. Model recalibration: controlled checkpoints only after adequate new evidence, drift/degradation, or a validated challenger; never automatically promote a model.

Single canonical end-to-end path: latest eligible session and freshness -> authoritative marks / NAV / cash / holdings -> eligible multi-asset frontier and signals -> cross-asset risk, capacity, costs and HOC -> zero-base CURRENT / MINIMUM_REPAIR / FULL_TARGET -> immutable/reviewable proposal and manual decision -> paper-only execution only if separately authorized -> reconciliation, ledger/NAV/P&L -> forward evidence and independent research. Existing owner APIs and orchestration paths must be reused; UI projects their state, never recalculates a rival answer. Preserve one calculation per business concept and idempotency on every state-changing operation.

## 4. No idle project time, but no fabricated time

If a market boundary, publication, forward maturity, data entitlement, or human authorization blocks one path, immediately advance a safe, independent workstream: source consolidation, alternate admissible data, agent-led research, target validation, tests or operator acceptance. Never say merely 'wait'. Never manufacture a weekend prediction, backdate a row, substitute today's snapshot into history, or alter entry/valuation/cadence to obtain an emission without governance.

Parallelism must respect shared resources: one owner for a workflow; do not restart or let tests alter a live Windows scheduled worker. Isolate tests with real task/process effects. A research failure must not invalidate an otherwise valid operational close.

## 5. Mandatory alpha research discipline

Use the existing 12-agent architecture and canonical research memory in `C:\Users\binis\Stock_Prediction_app_push`, the established experiment runner, and relevant director, data-foundation, signal, validation-skeptic and risk-portfolio agents. Do not create an ad hoc competing agent system. Before proposing a hypothesis, inspect full four-part mechanism identity, durable director rulings, novelty/duplicate checks, actual owned dataset and PIT/inactive/delisted coverage, information/decision timestamps, liquidity/capacity, realistic costs/borrow, and existing forward freezes. A census 'UNEXAMINED' entry is not proof of permission when a later durable ruling exists.

Freeze and RUN the first legitimately admissible bounded historical experiment, with valid train/validation/OOS time splits, declared paper capital, gross and NET P&L, controls, turnover, drawdown, effective sample and uncertainty; request independent skeptic/risk review. No automatic model promotion or capital activation. When refused, record the precise binding gate and immediately evaluate an economically distinct CROSS_ASSET / owned native-futures candidate. Do not relabel settled price/volume/open-interest bytes or time a settled premium as 'novel'. If both paths are barred, report the exact missing data or governance condition and the applicable purchase gate; do not invent results or ask for a purchase without the formal gate.

Non-equity opportunity must be explicit in EVERY portfolio review: frontier_eligible_non_equity_count, admitted rows, blockers and economic source. US equities/cash only is an acknowledged product gap, not a completed multi-asset portfolio.

## 6. Portfolio and money truth

Compare CURRENT, MINIMUM_REPAIR and FULL_TARGET against the SAME authoritative session, holdings, risk, cost and eligibility state. Reconcile HOC retention verdicts with churn-withheld actionable verdicts, historical adjustment notes with the FINAL trade ledger, and liquidity states across owners. A missing input is UNKNOWN/BLOCKED, never silent PASS. Explain each trade and every mandatory repair; show allocations and cash, actual weights, notional, costs, one-/two-way turnover, volatility/risk/drawdown/concentration, and remaining obligations. Mark non-calibrated expected returns as such. Surface issuer/class-share concentration, not just ticker weights, when relevant. Preserve existing immutable proposals; no duplicate/replay by convenience.

No live brokerage order, paper order/fill, target approval, model promotion, data purchase, or change of execution/entry contract absent separate explicit authorization. Manual decision remains mandatory; showing a proposal is not approval.

## 7. Release is an end-to-end acceptance, not test green

Follow `docs/RELEASE_ACCEPTANCE_GATES.md`. An intervention may print `COMMIT_OK` only when its affected scope passes targeted and impacted regression, strict architecture audit, saved/reopened artifact checks when relevant, authoritative read-model reconciliation, and safe source attestation. Reserve full repository suite for architecture/shared-core or milestone risk, not every slice. Report pre-existing failures separately with a reproduced baseline. Give EXACT operator validation, commit/push and restart/UI smoke-test Windows PowerShell commands in separate phases, never Bash/WSL, never an interactive `exit`, and never fragile split control-flow. Do not push without release authorization.

A source-attestation guard must be checked at USE, not merely startup. A running worker whose loaded commit differs from checkout HEAD or whose source is dirty may refuse prospective writes. Plan coherent commit, restart and readback BEFORE a legitimate boundary. Do not interrupt an active worker for test convenience. Release acceptance requires loaded backend/worker identity and actual live readback; static tests alone do not qualify.

## 8. 48/72-hour delivery target, not a promise of fabricated evidence

Target <=48 hours to restore and demonstrate the integrated A-to-Z paper workflow on existing records without re-running closed sessions or recreating proposals; target <=72 hours for initial real-session acceptance and an executed admissible alpha experiment OR the exact binding gate that prevents it. These are escalation targets, not claims of completed performance and not grounds to bypass market dates, PIT, authorization or release gates. If a genuine blocker moves the deadline, report its timestamp, owner, evidence and the independent work that continues now.

Final handoff always prints: **WHAT FAILED / WHY / WHAT CHANGED / WHAT ACTUALLY WORKS / WHAT REMAINS / SINGLE NEXT GOVERNED ACTION** and `COMMIT_OK` or `DO_NOT_COMMIT` with the exact reason. 'Success' is never hypothetical.
