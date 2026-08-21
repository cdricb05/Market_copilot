<!-- BEGIN PAPER TRADER MANDATORY UI REDESIGN WORKFLOW -->

# Paper Trader Mandatory UI Redesign Workflow

These rules apply to all Paper Trader UI, dashboard, workflow, and trading cockpit work.

## Architecture rules

- Project path: C:\Users\binis\paper_trader
- Use Windows PowerShell only.
- Do not use Bash or WSL.
- Local backend/UI: http://127.0.0.1:8001
- UI: http://127.0.0.1:8001/ui/
- Prediction service is remote on GCP through the local tunnel: http://127.0.0.1:9000
- Do not run prediction locally.
- Do not install prediction model dependencies locally.
- Do not reset files blindly.
- Preserve existing Paper Trader architecture rules.
- Do not implement Create Orders.
- Do not implement order execution.
- Do not implement automation.

## Mandatory UI redesign workflow

For any UI redesign or dashboard implementation, follow this sequence:

1. SCAN
   - Read the existing code first.
   - Identify current endpoints, state variables, workflow sections, diagnostics, and action handlers.
   - Identify which actions are read-only, preview-only, or DB-writing.
   - Identify stale workflow status, blank buttons, duplicate/stale review rows, and connected sections that still show Connect to Load.

2. REVIEW
   - Critique the current UI before coding.
   - Explicitly check for:
     - vertical stacked layout
     - empty Overview cards
     - useless Overview cards
     - Daily Plan requiring heavy scrolling
     - hidden safety status
     - diagnostics dominating the main dashboard
     - alert()
     - confirm()

3. PLAN
   - Produce a wireframe before coding.
   - The wireframe must target 1920x1080.
   - The first screen must not require heavy scrolling.
   - The planned layout must include:
     - left sidebar
     - top status/header bar
     - useful KPI/Overview cards
     - central workflow cockpit
     - right-side action/safety panel
     - compact trading tables
     - Audit / Advanced area for diagnostics

4. EXECUTE PREVIEW
   - Implement only after wireframe and acceptance criteria exist.
   - Keep the system preview-first.
   - Keep safety visible.
   - Keep DB-writing actions explicit.
   - Never add Create Orders or automation.

## Required four-stage workflow

The UI must organize the trading workflow around:

1. SCAN
2. REVIEW
3. PLAN
4. EXECUTE PREVIEW

## Mandatory visible safety badges

Keep these visible where relevant:

- PREVIEW ONLY
- CREATES SIGNALS ONLY
- CREATES TRADE DECISIONS ONLY
- NO ORDERS
- ORDERS DISABLED
- AUTOMATION OFF
- MANUAL REVIEW

## Hard failure conditions

Fail the work if:

- no wireframe was produced before coding
- the UI remains a vertical stack of cards
- Daily Plan requires heavy page scrolling at 1920x1080
- Overview is empty, decorative, vague, or useless
- safety badges are missing or hard to find
- diagnostics are not moved to Audit / Advanced
- any important workflow button is blank
- connected sections still say Connect to Load
- alert() is used
- confirm() is used
- Create Orders is implemented or enabled
- automation is implemented or enabled
- prediction is assumed to run locally

## Project Claude assets

Use these project-level assets when relevant:

- .claude\agents\ui-ux-architect.md
- .claude\agents\frontend-implementation-reviewer.md
- .claude\agents\browser-acceptance-tester.md
- .claude\skills\trading-cockpit-redesign\SKILL.md

## Browser acceptance requirement

After UI changes, validate with Playwright MCP at:

- URL: http://127.0.0.1:8001/ui/
- Viewport: 1920x1080

The browser validation must check:

- no heavy Daily Plan scrolling
- useful Overview cards
- visible safety badges
- no vertical stacked admin layout
- no blank buttons
- no alert()
- no confirm()
- diagnostics are in Audit / Advanced
- no enabled Create Orders
- no enabled automation

<!-- END PAPER TRADER MANDATORY UI REDESIGN WORKFLOW -->

<!-- BEGIN CANONICAL PROJECT OBJECTIVE (Phase 29A) -->

# Canonical Project Objective

The canonical objective of Paper Trader is to build an **active, research-driven
paper portfolio manager** that continuously determines whether the current
holdings remain the best risk-adjusted use of capital, identifies stronger
alternatives, and produces explainable portfolio-change proposals under strict
safety, point-in-time evidence, and manual-review controls. The system must
eventually operate close to real time. It is **not** a static buy-and-hold
tracker, a monthly-maintenance tool, a set of disconnected research dashboards,
a system that constantly retrains without evidence, or an automated
broker-execution system.

The system runs three operating cycles: **signal refresh (frequent)**,
**portfolio reassessment (frequent)**, and **model recalibration (controlled,
evidence-gated)**. Work is sequenced across seven milestones and governed by
eight architectural principles.

**The objective is asset-agnostic (Release 32).** Equities were the proving
ground, not the goal. The permanent question is: *if every investable dollar
were cash right now, given everything legitimately observable right now, where
should capital be deployed to maximise expected after-cost, risk-adjusted paper
portfolio PnL?* Legitimate answers include cash, equities, indices, sectors,
rates, commodities, FX, volatility and event-driven sleeves. The system need not
allocate to every asset class; capital belongs only where evidence supports it;
a NULL result is valid; **cash is a real asset choice**. Four Release-32
*design rules* follow (derived from the eight principles, and not principles
themselves): strategy sleeves generate opportunities and never own capital; the
global allocator owns capital; asset labels are not risk factors; daily
reassessment does not imply daily trading.

**Before major work, read:** `docs/PROJECT_CHARTER.md`, `PROJECT_STATE.md`,
`docs/TARGET_ARCHITECTURE.md`, `docs/PNL_OPPORTUNITY_FRONTIER.md`,
`docs/DAILY_MULTI_ASSET_GOVERNANCE.md`, `docs/ARCHITECTURE_DECISIONS.md`.

The authoritative statements live in these canonical documents — read them
before architectural or workflow changes, and keep them in sync with the code:

- [docs/PROJECT_CHARTER.md](docs/PROJECT_CHARTER.md) — canonical objective, three
  operating cycles, seven milestones, eight architectural principles, the
  Release-32 multi-asset design rules, safety boundaries, current and deferred
  scope.
- [docs/CURRENT_ARCHITECTURE.md](docs/CURRENT_ARCHITECTURE.md) — the current,
  evidence-based system map (modules, endpoints, UI, stores, workflows, risks).
- [docs/TARGET_ARCHITECTURE.md](docs/TARGET_ARCHITECTURE.md) — the target
  boundaries that support the seven milestones without a big-bang rewrite.
- [docs/CONSOLIDATION_ROADMAP.md](docs/CONSOLIDATION_ROADMAP.md) — the sequenced,
  bounded consolidation slices behind tests.
- [docs/ARCHITECTURE_DECISIONS.md](docs/ARCHITECTURE_DECISIONS.md) — confirmed,
  provisional, and unresolved architectural decisions with evidence.
- [docs/PNL_OPPORTUNITY_FRONTIER.md](docs/PNL_OPPORTUNITY_FRONTIER.md) — the
  asset-agnostic question, the common-overlap rule, and sleeve qualification.
- [docs/STRATEGY_SLEEVE_CONTRACT.md](docs/STRATEGY_SLEEVE_CONTRACT.md) — what a
  sleeve is, and why it may never own capital.
- [docs/DAILY_MULTI_ASSET_GOVERNANCE.md](docs/DAILY_MULTI_ASSET_GOVERNANCE.md) —
  the daily loop, no-churn hysteresis, mixed calendars, one authoritative NAV.
- [docs/INFORMATION_PURCHASE_GATE.md](docs/INFORMATION_PURCHASE_GATE.md) — the
  ten conditions before any dataset becomes a purchase candidate.
- [docs/RELEASE32_ZERO_COST_INFORMATION_EXPANSION.md](docs/RELEASE32_ZERO_COST_INFORMATION_EXPANSION.md)
  — the Release-32 programme, research funnel budgets, artifacts, invariants.

These canonical rules do not replace the UI redesign workflow above; both apply.
Paper-only, preview-first, manual-review, and no-automation safety boundaries
remain in force.

<!-- END CANONICAL PROJECT OBJECTIVE (Phase 29A) -->

<!-- CANONICAL BACKEND RESTART / SMOKE OWNER -->

# Canonical backend restart / smoke owner

Restarting and smoke-testing the backend is owned by exactly ONE script:

    scripts\restart_paper_trader_backend.ps1

Do not write a stage-specific `restart_smoke.ps1` that starts the backend. Nine
stages did, and nine stages reintroduced the same defect: polling
`http://127.0.0.1:8001/health`, which this application has never served.

- The canonical readiness routes are permanently `GET /v1/health` and
  `GET /v1/ready`.
- A stage handoff DELEGATES to the canonical script. It may add stage-specific
  authenticated GET assertions with `-SmokePath`. It may not reimplement process
  stop/start, port handling, health/readiness polling, authentication setup,
  stdout/stderr diagnostics, or production store-root validation.
- `LIVE_SMOKE_OK` is printed by that script alone, exactly once, and only after
  every live check has passed.
- `scripts\audit_architecture.py` (`check_backend_restart_ownership`) fails the
  build on any violation, including a probed `/v1` path the application does not
  declare as GET. `tests\test_canonical_backend_restart.py` is the regression.

## Canonical invocation (Release 29 UX2 — permanent)

Call the owner DIRECTLY. Never re-enter PowerShell to run it.

```powershell
$SmokePaths = @(
    '/v1/operations/workflow-state',
    '/v1/operations/information-collection',
    '/v1/operations/daily-close',
    '/v1/operational-book',
    '/v1/operations/portfolio-reassessment'
)

& C:\Users\binis\paper_trader\scripts\restart_paper_trader_backend.ps1 `
    -Force `
    -Port 8001 `
    -SmokePath $SmokePaths
```

- The owner contains **no** process-terminating statement, so a direct call can
  never end the operator's shell. It reports its outcome as ONE printed terminal
  token (`LIVE_SMOKE_OK` / `RESTART_PREFLIGHT_OK` /
  `RESTART_SMOKE_FAILED - <reason>`), `$LASTEXITCODE`, and
  `$global:PaperTraderRestartResult`. Callers check those, not a process code.
- `-ContractProbe` prints the bound parameter contract as JSON and returns
  without touching a process, a port or an HTTP route.
- FORBIDDEN, and enforced by `check_restart_invocation_hygiene`:
  forwarding `-SmokePath` through `powershell.exe -File` (the `String[]`
  flattens and a URL binds positionally to `-ReadyTimeoutSec:Int32`); building a
  lifecycle command through `powershell.exe -Command` (the outer shell eats
  continuation backticks); collapsing the `String[]` into one comma-joined or
  positional argument; a second restart implementation.
  `tests\test_release29_restart_contract.py` is the regression, and it proves
  the parameter binding with the real PowerShell binder.

<!-- END CANONICAL BACKEND RESTART / SMOKE OWNER -->
