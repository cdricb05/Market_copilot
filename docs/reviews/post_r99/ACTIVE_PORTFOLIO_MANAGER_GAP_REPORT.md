# Active portfolio manager — gap report against Milestones 1–3 and the three cycles

**Legend.** **OPERATIONAL**: works end to end from the authoritative owner. **PARTIAL**: works, with a named gap. **DISCONNECTED**: the code exists but is not on the canonical path. **MISSING**: no implementation.

Evidence is cited as `file:line` (see `ARCHITECTURE_CURRENT_STATE.md`). "Operational impact" says what goes wrong for the operator or the book if the gap stays open.

## Three operating cycles

| Cycle | Status | Evidence | Operational impact |
|---|---|---|---|
| 1. Frequent signal refresh | **PARTIAL** | Event leg: collection worker → `information_collection.run_collection_iteration` (:2168) → `event_signal_refresh` (:944), gated on the operator enabling the service and on source attestation (:2370-2380). Model-input leg: `alpha_target.run_refresh` runs **once per session** inside `daily_close` (:3645). A legacy second mark pipeline (`current_alpha_daily_refresh`, `/daily-run/execute`) still exists (D18). | The frozen model's scores move once a day; only event information is intraday. Two mark pipelines can give two "current" champion marks. If the worker is not enabled, the frequent leg silently does not run, which is visible only in collection state. |
| 2. Portfolio reassessment after each valid refresh | **OPERATIONAL (equity), PARTIAL (multi-asset)** | The DRC runs `SCORE_UNIVERSE` → HOC → `REASSESS_PORTFOLIO` in fixed order (`daily_research_cycle.py:170-176`). ESR re-runs HOC → reassessment on material events, with duplicate suppression (`esr:65-73, :342`). The reassessment loop covers **every** HOC holding (`engine/portfolio_reassessment.py:1184`). Non-equity and cash alternatives enter only at the proposal stage (`reallocation_proposal.py:490-494`). | A holding is compared against ranked **equity** replacements, not against admitted non-equity or cash rows, so a better non-equity use of capital shows up only in the full-target proposal, not in the per-holding verdict. A docstring wrongly says there is no scheduler-driven reassessment (`portfolio_reassessment.py:39-43`). |
| 3. Controlled model recalibration | **MISSING (by design, manual only)** | No `promote`/`recalibrate`/`retrain` executor exists in `api/` or `engine/`. `engine/research_agent._recalibration_recommendation` (:922) only recommends. `AUTOMATIC_PROMOTION_ALLOWED=False` in 5+ modules; the challenger registry hard-codes `promotion_allowed: False` (:591). | Correct for safety: challengers cannot replace the champion. But there is no governed checkpoint artifact (evidence pack → human decision → recorded recalibration), so recalibration, when it happens, is out of band. Caveat: `capital_eligibility_gate` can make a research **sleeve** capital-eligible automatically **after** a human writes a conditional pre-approval (:1-3, :253). That is not a champion swap, but evidence alone then moves eligibility. |

## Milestones 1–3

| Milestone | Status | What is operational | Gaps (evidence → impact) |
|---|---|---|---|
| **M1 — Reliable persistent daily research cycle** | **PARTIAL** | One operator action, `POST /v1/operations/portfolio-cycle/run` (`portfolio_cycle.py:422`), runs the daily close (session, freshness, marks/NAV, journal, TRUE_FORWARD capture) and then the DRC (score, target, evidence, HOC, reassess, proposal), each at most once, stopping at the decision boundary. The close is idempotent on (book, market_date) (`daily_close.py:3468`). Forward capture refuses backfill. | (1) **Not automatic.** No scheduler calls `run_portfolio_cycle`; it is API-triggered only. → The cycle depends on an operator click every session; a missed session becomes recovery. (2) **Hidden button order still exists.** The UI also exposes separate execute buttons for the close (`index.html:40016`), the research cycle (:37743) and the legacy `daily-run/execute` (:26130). The dispatcher prefers PORTFOLIO_CYCLE (:37982), but the alternatives remain clickable. (3) **Holiday-blind readiness.** `alpha_target.latest_completed` (D13) → readiness can name a non-session on an exchange holiday. (4) **No per-run cycle record.** `portfolio_cycle` writes no run record of its own; provenance is attribution strings only. (5) **Locks are in-process only.** Locks use `threading`, not cross-process. |
| **M2 — Holding opportunity-cost engine** | **OPERATIONAL (equity) / PARTIAL** | `engine/holding_opportunity_cost.build_assessment` builds one candidate pool and a per-holding shortlist for **every** holding (:884-929, :1140). Persistence is identity-idempotent (`REUSED_EXISTING`, `CONFLICT_REJECTED`; :1209). It runs from the DRC and ESR. | (1) Per-holding alternatives are equity-universe only (see cycle 2). (2) No shared cross-process lock between DRC and ESR on the HOC index. → A concurrent write can lose an index row (UNVERIFIED in practice). (3) About 20 operational scoring callers bypass the canonical input hash (D19). → HOC and other surfaces cannot prove they used the same scores. |
| **M3 — Portfolio reallocation proposal engine** | **PARTIAL (close to operational)** | CURRENT / MINIMUM_REPAIR / FULL_TARGET on one state (`engine/proposal_decision_review.py` :870, :1052, :945). Hash-idempotent proposal persistence (`api/reallocation_proposal.py:611`). Manual selection ≠ approval (`app.py:7342`). Decisions create no orders. Order-plan confirmation is separate, token- and state-gated, with **no UI button**. | (1) **Two target references for a live book.** The bootstrap Top-25 target is still rendered as `target_weight`/`weight_drift` (D04). → The operator can read drift against the wrong target. (2) **Optional staleness hashes** (`app.py:7314, :7679`) → a direct API call can decide on a stale proposal. (3) Expected return is NOT_CALIBRATED, which is correctly labelled, not a defect. (4) The non-equity count is reported only where the frontier supplies rows. |

## Required confirmations

| Requirement | Verdict | Evidence |
|---|---|---|
| Research failure cannot invalidate a valid operational close | **CONFIRMED** | The journal row is written before evidence capture (`daily_close.py:3761` < :3781). Capture exceptions are swallowed (:2137-2148). DRC research steps fail as warnings (`daily_research_cycle.py:4016-4019, :4298-4301`). Residual: `portfolio_cycle.py:485-490` can return HTTP 500 **after** a landed close; the close stays valid. |
| Research recommendations cannot create orders | **CONFIRMED** | No call to `generate_orders`, `confirm_order_plan`, `confirm_rebalance_order_plan`, `record_decision` or `settle_due_orders` exists under `alpha_agent/`, `research/` or `scripts/`. Caveat: research writes its own shadow ledgers through the private `desk._append_ledger` (D24). Whether those roots can never equal the desk root is UNVERIFIED. |
| Proposals require manual review | **CONFIRMED** | `portfolio_decision.py:777-870`; APPROVE requires a governed selection (`app.py:7315-7321`); selection is not approval (:7342). |
| Missing TRUE_FORWARD evidence is never reconstructed with hindsight | **CONFIRMED** | `forward_prediction_skill.py:716-727` refuses an earlier date; `daily_close.py:3491` refuses a retroactive close capture; `prospective_decision.py:329-342`; `canonical_forward_accrual` records forfeitures and sets `backfill_allowed: False` (:1771-1786, :2065); R59 adoption starts its clock at "now". |
| Repeated requests do not duplicate state | **PARTIAL** | Idempotent: daily close per (book, date); snapshots per (model, book, date); emissions per (identity, session); DRC per-session manifest; HOC, proposal and decision identity checks. Gaps: no `portfolio_cycle` run record; `except TypeError` re-invocation (`daily_close.py:3598`, `portfolio_cycle.py:487`); locks are in-process only; emission writes are not create-exclusive. |
| The UI reads authoritative backend state | **PARTIAL** | Workflow dispatch reads payload fields only (`index.html:37960-37989`). But the UI still renders three legacy stage machines (D07–D09), computes max drawdown client-side (:42477), and derives the Create-Paper-Orders **visibility** client-side, fail-open (D31). |
| Challengers cannot automatically replace the champion | **CONFIRMED** | No executor; `promotion_allowed: False`; the champion is static in `multi_horizon_registry` (:161). |
| Portfolio reassessment compares every holding with eligible alternatives | **PARTIAL** | Every holding: yes (:1184 / HOC :1140). "Eligible alternatives": the equity universe only; non-equity and cash appear at the proposal stage. |
| Reallocation remains paper-only and separately authorised | **CONFIRMED** (one UI caveat) | Desk has no broker (`paper_trading_desk.py:11-21`). Plan confirmation needs a token, an APPROVED and unchanged proposal, a fresh session, server revalidation and plan-id idempotency (`rebalance_execution.py:1553-1678`). **Caveat (D31):** the bootstrap "Create Paper Orders" button can appear enabled when `/alpha-book/status` fails. The backend still refuses for a live book; on an empty book it would create PROPOSED paper orders. |
| One operator action runs the daily workflow without hidden button order | **PARTIAL** | The single action exists (`RUN_PORTFOLIO_CYCLE`). Alternative execute buttons remain on screen (M1 gap 2). |

## R100 readiness decision

**`NOT_READY`**

R100 must not launch until these blockers are cleared:

1. **P0-1 not landed.** The PF1/PF2/C04 runner repair is in the working tree, uncommitted. The canonical runner file also carries **pre-existing** uncommitted R91/R92 hunks, so neither the loaded worker identity nor the measured code is attestable. This is a hard blocker.
2. **P0-3 not landed.** Preregistration does not yet embed `parameters.frozen_source`. R100 would repeat R99's hash drift unless its spec sets `require_frozen_source: true` and its preregistrations carry manifests.
3. **R100 spec declarations.**
   - `admission_rulings` must be declared, in order, so that the admission door is active.
   - A fresh cap declaration is required.
   - The spawn plan must come from `scripts/agents_v2_brief.py --spawn-plan`.
4. Fresh director rulings, data certification and preregistration are required **before any return is read**.

When 1–2 have landed and the spec declares 3, the decision becomes READY for a campaign. It does **not** become READY for any particular hypothesis.

### C52F (future preregistration candidate only — NOT promising alpha)

| | |
|---|---|
| Idea | Hog breeding-herd first print → lean-hogs front vs an 8–11-month contract |
| Return-free cost | about **0.96%/yr** (D layer: 0.956% total, of which roll 0.688%) |
| Power | **POWER_MARGINAL** (MDE 0.077, rank-IC units) |
| Prior | **Weak**. All six measured R99 inventory expressions failed (C01/C05 cost-killed; C03/C04/C22 gross-negative; C02 lockbox −4.62%/yr net). |

Before **any** return is read, C52F needs:

- a **new campaign ID**;
- a new cap declaration with an open COMMODITY_FUTURES / PHYSICAL_INVENTORY_FLOW slot;
- an **immutable code snapshot** (`frozen_source`);
- data-foundation certification of BK36 hogs-and-pigs and the far leg (`he_far_leg.npz`, sha256 `25c5c142…`);
- a **fresh director G1/G4/G7 ruling**;
- canonical preregistration.

R99's frozen C52 used the ~2-month second-nearby leg, so C52F is a different expression and must get a new ID.
