# Consolidation roadmap — post-R99 (bounded, sequenced, test-gated)

Every slice is small enough to land on its own, behind targeted tests. Every slice reuses an existing owner; none is a rewrite. Each slice follows the acceptance gates in `docs/RELEASE_ACCEPTANCE_GATES.md`:

- targeted and impacted tests;
- `scripts/audit_architecture.py --strict`;
- `scripts/check_ui_js.py` plus a Playwright check at 1920×1080 when the UI changes;
- live owner-GET readback after the canonical restart (`scripts/restart_paper_trader_backend.ps1`).

Register IDs (D##) refer to `ARCHITECTURE_DUPLICATION_REGISTER.json`.

Rollback for any slice is `git revert <slice commit>` followed by the canonical restart. No slice writes or migrates a production store unless that is stated.

---

## P0 — Research-pipeline correctness and reproducibility

| Slice | Scope | Depends on | Regression tests | Rollback | Acceptance gate |
|---|---|---|---|---|---|
| **P0-1 Land the PF1 / PF2 / C04 runner repair** (implemented in this review, uncommitted) | `alpha_agent/agents_v2/runner.py`, new `agents_v2/provenance.py`, `scripts/build_next_campaign_draft.py`, `tests/test_post_r99_pipeline_repair.py` | First land or deliberately stage the **pre-existing** uncommitted R91/R92 runner hunks (`effective_cost_inputs_of`, event-window check) | the new file (29); impacted agents_v2 / r89 / r91 / r92 / r56 / r61 suites (193) | revert the commit; campaigns that declare nothing are unaffected | strict audit exit 0; partial-stage diff reviewed hunk by hunk |
| **P0-2 R99 errata acknowledgement** | the `docs/reviews/post_r99/*` artifacts; memory note | P0-1 | `post_r99_verify.py` 0 discrepancies | delete the review folder | operator reads `R99_CLOSEOUT_BLOCKED.md` |
| **P0-3 Wire frozen-source binding into preregistration** | `scripts/alpha_agents_v2.py preregister --freeze-source <paths>` calls `provenance.snapshot()` and injects `parameters.frozen_source`; decide the store root (proposed `D:\Stock_Prediction_app_data\r59_autonomous_alpha\frozen_source`) | P0-1 | a tmp-store test that a preregistered manifest round-trips and a later edit refuses | revert; the store is append-only and harmless | an R100 dry-run preregistration in a `tmp_path` memory |
| **P0-4 Canonical per-experiment admission event** | a new pipeline event `AGENTS_V2_DIRECTOR_ADMISSION` (G1/G4/G7, amends) written by the director through `alpha_agents_v2.py`; the runner reads the memory first and campaign JSON as a fallback | P0-1; contract change validated by `contracts.validate()` | admission withdrawal → refusal; post-measurement withdrawal → exception; contract validator | revert; the event type is additive | `contracts.validate()` green; the spawn plan is unchanged |
| **P0-5 Audit correctness** | `_static_prefix` strips a trailing `/` (D27); concept-writer detection from a **declared** writer list instead of a `def`-name regex (D10, D11); `check_direct_ledger_refs` covers `D:\Stock_Prediction_app_data\<store>` (report-only first) | — | new audit unit tests; `/v1/ticker-detail/` no longer dangling | revert | strict audit exit 0, finding counts reported before and after |
| **P0-6 No GET migrates a store** | `autonomous_operating_status.py:584, :589` use `open_memory_readonly` and `paper_trader.alpha_agent` imports (D28); add a blocking audit invariant | — | a test that the GET route, given a read-only file, does not change `memory_meta` | revert | live GET readback; `memory_meta` migration marker unchanged |

## P1 — Authoritative market date, freshness, workflow state, signal-refresh path

| Slice | Scope | Depends on | Tests | Acceptance |
|---|---|---|---|---|
| **P1-0 Fail-closed "Create Paper Orders" (D31)** — a safety slice, so it goes first in P1 | `index.html`: `pd-act-generate` stays hidden and disabled unless `/v1/alpha-book/status` loaded **and** reports an open bootstrap path; update the pin in `test_phase27a1_alpha_book_policy.py:822` | wireframe note (CLAUDE.md UI workflow), however small | `check_ui_js.py`; a Playwright check at 1920×1080 with `/alpha-book/status` forced to fail shows the button hidden | no enabled Create Orders control in any failure mode |
| **P1-1 One session source (D13–D15)** | `alpha_target.latest_completed` / `daily_operating_run.latest_completed_market_date` delegate to `market_session` with the calendar's non-sessions; one `non_sessions` helper | — | holiday cases (Good Friday, Thanksgiving) through `compute_readiness` | readiness and workflow-state report the same session on a holiday |
| **P1-2 One freshness classifier (D16–D17)** | legacy valuation and current-alpha-book freshness delegate to `classify_source` | P1-1 | per-feed parity tests | the freshness GET and the legacy surfaces agree |
| **P1-3 Retire legacy stage machines (D07–D09)** | UI stops calling `/review/workflow-status`, `/dashboard/command-center` and `/dashboard/daily-workflow` for stage; the routes return the `workflow_state` projection or 410 | UI wireframe | `test_slice2_workflow_state`; UI JS check; Playwright | one stage vocabulary on screen |
| **P1-4 One champion-mark pipeline (D18)** | retire the `current_alpha_daily_refresh` subprocess path and `/daily-run/execute` writes (return 410 behind the existing legacy-archive flag) | P1-1 | `test_current_alpha_daily_refresh` updated; daily-close tests | marks come only from `daily_close` |
| **P1-5 Scoring hash everywhere (D19)** | operational callers read `universe_scoring` and carry `input_contract_hash` | — | hash-parity test | proposal and HOC record the same hash |

## P2 — Canonical NAV, portfolio state, holding-opportunity-cost ownership

| Slice | Scope | Tests | Acceptance |
|---|---|---|---|
| **P2-1 One NAV fold (D01, D02)** | extract the desk fold into `api/desk_fold.py`; `book_nav` and `corporate_actions` both call it (breaks the import cycle); `operational_book` displays `book_nav` values verbatim | NAV parity across equity, futures and FX fixtures; corporate-action reconciliation tests | no second NAV formula (audit invariant) |
| **P2-2 Public ledger API + cross-process lock (D21–D26)** | `desk.append_ledger` / `read_ledger` public with a file lock; research shadow ledgers use their own module and a separate root (assert ≠ desk dir); `research_agent` and `r53` read through owner loaders; remove the private research-script import (D25) | concurrent settle test (two processes); shadow-root assertion | private `desk._` access count drops; audit allow-list shrinks |
| **P2-3 HOC / reassessment / proposal index locks** | shared file lock between DRC and ESR on the HOC, reassessment-history and proposal indexes | concurrent DRC + ESR test | no lost index rows |

## P3 — Portfolio reassessment after every valid signal refresh

| Slice | Scope | Tests | Acceptance |
|---|---|---|---|
| **P3-1 Reassessment trigger contract** | declare in one place that a valid model-input refresh (daily close) **and** a material ESR event both trigger HOC → reassessment; correct the `portfolio_reassessment.py:39-43` docstring | trigger matrix test | each valid refresh yields exactly one reassessment per (session, economic hash) |
| **P3-2 Non-equity alternatives per holding** | per-holding comparison includes admitted non-equity and cash alternatives (today they enter only at the proposal stage through `opportunity_frontier`) | fixture with one admitted non-equity row | `frontier_eligible_non_equity_count` is reported in every reassessment |

## P4 — Complete paper-only reallocation proposal with manual review

| Slice | Scope | Tests | Acceptance |
|---|---|---|---|
| **P4-1 One target reference (D04–D06)** | live-book drift is computed against the governed selected target only; the bootstrap Top-25 target is labelled bootstrap and hidden for live books | `operational_book` drift tests | a single target on screen for a live book |
| **P4-2 Required staleness hashes** | `expected_proposal_hash` / `expected_order_plan_hash` are **required** on decision and plan POSTs; the actor is recorded from the authenticated key, not a free string | API tests | a direct API call without a hash is refused |
| **P4-3 Idempotent portfolio-cycle run record** | `portfolio_cycle` writes a per-session run record; `except TypeError` re-invocations are replaced by signature checks; the runner is wrapped so a post-close exception returns a structured partial result | replay and duplicate POST tests | repeated POST → `REUSED`, no duplicate state |

## P5 — Persistent research-agent integration

| Slice | Scope | Acceptance |
|---|---|---|
| **P5-1 Single research writer** | audit which entrypoints write R59 memory (D29); route them through `alpha_agents_v2.py` or the runtime; read the champion id from the registry (D20) | write-attribution test |
| **P5-2 R46 tournament out of the DRC (D12)** | move `ADVANCE_PROSPECTIVE_TOURNAMENT` to the research runtime | DRC no longer touches research-root stores |
| **P5-3 Create-exclusive emissions** | `canonical_forward_accrual` emission uses `O_EXCL` | a duplicate emission is refused at the filesystem |

## P6 — Data expansion, later intraday

Only after P0–P3 are stable. Follows `docs/INFORMATION_PURCHASE_GATE.md`; no purchase without the formal gate. Candidates carried forward by R99: BK41 policy-expectation strips; BK36 USDA certification (needed by C52F).

## P7 — Controlled execution

Only after P0–P4 are stable **and** separate explicit authorisation is given. Not scheduled. Create Orders remains unimplemented in the UI.

---

## First three slices to execute

1. **P0-1**: land the runner repair. It is implemented; it needs commit authorisation and partial staging.
2. **P0-6**: stop the GET route from opening a writable research store (two-line owner fix plus a blocking invariant).
3. **P1-0**: make "Create Paper Orders" fail closed. This is the only CLAUDE.md hard-failure-class defect found. It is a UI slice, so it runs the wireframe-first and Playwright workflow.

P0-5 (audit correctness) can run in parallel with these; it touches only `scripts/audit_architecture.py`.
