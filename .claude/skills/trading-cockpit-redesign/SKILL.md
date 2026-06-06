---
name: trading-cockpit-redesign
description: Use for all Paper Trader UI redesign, trading cockpit, Daily Plan, dashboard layout, Overview cards, workflow cards, visual hierarchy, and browser acceptance work. Enforces wireframe-first design, preview-first safety, 1920x1080 validation, and no Create Orders or automation.
---

# Paper Trader Trading Cockpit Redesign Skill

This skill governs all Paper Trader UI redesign and dashboard implementation work.

Use this skill whenever the task touches:

- api/ui/index.html
- visual dashboard layout
- trading cockpit
- Daily Plan
- Overview cards
- workflow cards
- review queue
- signal preview
- decision preview
- diagnostics panels
- browser acceptance validation

# Fixed architecture

Paper Trader runs locally on Windows.

Local Paper Trader backend/UI:

- Backend/API: http://127.0.0.1:8001
- UI: http://127.0.0.1:8001/ui/
- Backend module: paper_trader.api.app:app
- Local database: PostgreSQL database paper_trader
- Local dev API key: local-dev-key

Prediction service:

- Does not run locally on the Windows machine.
- Runs remotely on GCP.
- Paper Trader reaches it through local tunnel: http://127.0.0.1:9000
- Do not install prediction models locally.
- Do not run prediction locally.
- Do not change the architecture to run predictions locally.

# Hard safety boundaries

Do not implement:

- Create Orders
- real order placement
- broker integration
- automated execution
- scheduled automation
- auto-trading
- background trading
- alert()
- confirm()

Do not use browser-native blocking modals.

Use styled in-page confirmation and preview-safe UX only.

# Mandatory four-stage workflow

All redesigned UI must organize the main workflow into four stages:

1. SCAN
   Purpose:
   - Check service readiness.
   - Load market/prediction context.
   - Identify candidates.
   - Show prediction preview readiness.

2. REVIEW
   Purpose:
   - Review candidate queue.
   - Identify stale or duplicate candidates.
   - Make human review status obvious.
   - Explain what needs attention.

3. PLAN
   Purpose:
   - Preview signals.
   - Preview trade decisions.
   - Explain risk constraints.
   - Explain rejection reasons such as MAX_POSITIONS_REACHED.

4. EXECUTE PREVIEW
   Purpose:
   - Show preview-safe final actions.
   - Make it explicit that the system creates signals or trade decisions only.
   - Show NO ORDERS, ORDERS DISABLED, AUTOMATION OFF, and MANUAL REVIEW.

# Mandatory UI structure

The target design is a modern dark trading dashboard.

The first screen at 1920x1080 should include:

- Left sidebar
- Top status/header bar
- Useful KPI/Overview cards
- Central workflow cockpit
- Right-side action/safety panel
- Compact trading tables
- Audit / Advanced area for diagnostics

The UI must not be a long vertical admin stack.

# Wireframe-first rule

Before coding, produce a wireframe.

The wireframe must include:

- Page regions
- Relative placement
- What appears above the fold at 1920x1080
- Main workflow flow
- Overview card purpose
- Safety panel placement
- Diagnostics/Audit placement
- Tables to show
- Buttons and their labels
- Disabled/out-of-scope actions

Do not code until the wireframe exists.

# Overview card rules

Overview cards must be useful.

Reject cards that are:

- empty
- decorative only
- generic
- disconnected from the workflow
- impossible to interpret
- filler content

Good Overview cards include things like:

- connection status
- prediction service status
- candidates found
- candidates needing review
- signals previewed
- decisions previewed
- open positions count
- blocked decisions count
- risk limit status
- last refresh time

# Daily Plan rules

The Daily Plan must be usable without heavy scrolling at 1920x1080.

Fail the work if:

- the user must scroll heavily to understand today's workflow
- the main action path is below the fold
- safety context is not visible near actions
- the four-stage workflow is not visible or quickly scannable

# Diagnostics placement

Diagnostics are useful but should not dominate the trading cockpit.

Move or keep these in Audit / Advanced:

- health diagnostics
- connect diagnostics
- runtime diagnostics
- JavaScript diagnostics
- raw action diagnostics
- raw API payloads
- debug logs

The main cockpit should show concise status, not raw diagnostics.

# Required safety badges

Keep these visible where relevant:

- PREVIEW ONLY
- CREATES SIGNALS ONLY
- CREATES TRADE DECISIONS ONLY
- NO ORDERS
- ORDERS DISABLED
- AUTOMATION OFF
- MANUAL REVIEW

# Browser validation requirement

After UI implementation, validate with Playwright MCP.

Required viewport:

- 1920x1080

Required URL:

- http://127.0.0.1:8001/ui/

Acceptance must check:

- the first screen does not require heavy scrolling
- Overview is useful
- workflow is not a vertical stack
- safety badges are visible
- no blank buttons
- no alert()
- no confirm()
- diagnostics are in Audit / Advanced
- Create Orders is not enabled
- automation is not enabled

# Required implementation workflow

For every UI task:

1. SCAN
   - Read current code.
   - Identify architecture-sensitive areas.
   - Identify endpoints and whether actions are read-only, preview-only, or DB-writing.
   - Identify current UX problems.

2. REVIEW
   - Critique the existing UI.
   - Name concrete failures.
   - Check against the hard failure list.

3. PLAN
   - Produce wireframe.
   - Produce acceptance criteria.
   - List exact files to change.
   - List exact validation commands.

4. EXECUTE PREVIEW
   - Make minimal safe changes.
   - Preserve architecture.
   - Keep preview-only safety.
   - Validate with tests and browser review.

# Hard failure list

The work fails if:

- no wireframe was produced before coding
- UI remains a vertical stack
- Daily Plan requires heavy scrolling at 1920x1080
- Overview is empty or useless
- safety badges are missing
- diagnostics dominate the main dashboard
- blank buttons remain
- connected sections say Connect to Load after connection
- alert() exists
- confirm() exists
- Create Orders is implemented
- automation is implemented
- prediction is run locally

# Windows-only command rule

Use Windows PowerShell only.

Do not use:

- Bash
- WSL
- sed
- awk
- grep
- Linux heredocs
- Unix path assumptions

Use complete ready-to-paste PowerShell commands.