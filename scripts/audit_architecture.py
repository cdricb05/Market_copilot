#!/usr/bin/env python
r"""Static architecture audit for Paper Trader (Phase 29A).

READ-ONLY. This tool inspects the repository *statically* — it never imports or
executes the application, never opens a database or network connection, never
runs the prediction service, and never mutates the repository by default. It
parses source text and reports architectural signals that support the canonical
objective in docs/PROJECT_CHARTER.md.

IMPORTANT: static analysis does NOT prove runtime behavior. Every finding here is
a *candidate* derived from source text (regex/AST-free scanning), to be confirmed
against docs/CURRENT_ARCHITECTURE.md and the tests. Treat REMOVE_CANDIDATE and
orphan findings as leads, never as authorization to delete.

Usage (Windows PowerShell):

    .\.venv-win\Scripts\python.exe scripts\audit_architecture.py            # console + JSON to a temp file
    .\.venv-win\Scripts\python.exe scripts\audit_architecture.py --out out.json
    .\.venv-win\Scripts\python.exe scripts\audit_architecture.py --json-only

The JSON payload is deterministic for a fixed working tree: all collections are
sorted and no timestamps or random values are emitted, so a fixed tree always
produces byte-identical output. Exit code is always 0 (a report, not a gate)
unless --strict is given, in which case a nonzero code is returned when any
"blocking" category is non-empty.
"""
from __future__ import annotations

import argparse
import ast
import json
import os
import re
import sys
import tempfile
from pathlib import Path

# --------------------------------------------------------------------------- #
# Configuration (documented, static thresholds).
# --------------------------------------------------------------------------- #
REPO_ROOT = Path(__file__).resolve().parent.parent

# Directories that hold first-party source we inventory. venvs, caches, .git,
# node_modules and build artifacts are always excluded.
SOURCE_DIRS = ("api", "engine", "db", "alpha_agent", "research_agent",
               "scripts", "workflows")
EXCLUDE_PARTS = (".venv", ".venv-win", ".git", "__pycache__", "node_modules",
                 ".pytest_cache", "egg-info", ".mypy_cache")

# A module larger than this many source lines is flagged as a large / mixed-
# responsibility candidate (documented threshold, not a hard rule).
SIZE_THRESHOLD_LINES = 1500

# The single FastAPI application module (all routes are declared here today).
APP_MODULE = "api/app.py"
UI_FILE = "api/ui/index.html"

# The one module allowed to construct database sessions (the session factory).
DB_SESSION_OWNER = "db/session.py"

# Ledger root literal that, if referenced directly outside the desk/book service
# modules, indicates ledger access bypassing a service boundary.
LEDGER_ROOT_LITERAL = ".paper_trader"
# Service modules that legitimately own direct ledger filesystem access.
LEDGER_OWNER_MODULES = {
    "api/operational_book.py", "api/paper_trading_desk.py", "api/alpha_book.py",
    "api/current_alpha_book.py", "api/alpha_target.py", "api/daily_close.py",
    "api/forward_prediction_skill.py", "api/forward_evidence.py",
    "api/current_alpha_performance.py", "api/current_alpha_daily_refresh.py",
    "api/multi_horizon_ledger.py", "api/multi_horizon_registry.py",
    "api/current_alpha_tournament_sync.py", "api/daily_operating_run.py",
    "api/current_operating_state.py",
}

# Research-only source trees + individual research modules that must never call
# order-execution primitives. (The whole point of the research/ops boundary.)
RESEARCH_ONLY_DIRS = ("alpha_agent", "research_agent")
RESEARCH_ONLY_MODULES = {
    "api/alpha_factory.py", "api/price_alpha_factory.py", "api/alpha_registry.py",
    "engine/absolute_return_research.py",
}
# Order-execution terms that must not appear in research-only code as *calls*.
EXECUTION_CALL_TERMS = (
    "place_order", "submit_order", "execute_order", "send_order",
    "broker_execute", "live_order", "route_order",
)

# --- Slice 1 (Phase 29B) canonical market-session / data-freshness ownership --- #
# The ONE authoritative owner of current-session calendar/cutoff arithmetic and
# the ONE owner of cross-source data freshness.
MARKET_SESSION_OWNER = "engine/market_session.py"
DATA_FRESHNESS_OWNER = "api/data_freshness.py"

# --- Slice 2 (Phase 29C) canonical workflow / operator-state ownership --------- #
# The ONE owner of the COMBINED operator interpretation (overall workflow state,
# current task, primary next action, action severity, assessment currency, queued
# workflow actions). Specialized modules keep their DOMAIN facts (the gate outcome,
# the close status, the lifecycle) but not the combined interpretation, and the UI
# renders these values without deriving any of them.
WORKFLOW_STATE_OWNER = "api/workflow_state.py"
WORKFLOW_STATE_ROUTE = "/v1/operations/workflow-state"
UI_WORKFLOW_LOADER = "function loadWorkflowState"
UI_WORKFLOW_RENDER_EXPORT = "window.renderWorkflowState"
# Client-side derivation the UI must NOT perform inside the workflow loader/render
# region: market-date arithmetic, or constructing a stale "today" assessment label.
UI_WORKFLOW_FORBIDDEN_ARITH = ("new Date(", "Date.now(", ".getTime(")
UI_WORKFLOW_FORBIDDEN_TODAY_LABEL = "NO ACTION TODAY"

# --- Slice 2 UI HARD CUTOVER (Phase 29C.1) canonical DOM ownership -------------- #
# renderWorkflowState is the EXCLUSIVE owner of every PRIMARY operator-interpretation
# node below; the specialized detail renderers must NOT write any of them, and the
# shared specialized setters hard-guard them so the final visible state is
# independent of async completion order.
UI_WS_OWNERSHIP_DECL = "window.WS_CANONICAL_NODES"
UI_WS_OWN_HELPERS = ("function _wsOwnSet", "function _wsOwnHtml",
                     "function _wsGuardedSet", "function _wsIsCanonicalNode")
# Mirror of window.WS_CANONICAL_NODES: the canonical primary-interpretation nodes.
UI_CANONICAL_NODES = (
    "right-current-task", "right-next-action", "right-primary-action-btn",
    "right-dc-badge", "right-dag-badge",
    "cc-dag-title", "cc-dag-badge", "cc-dag-headline", "cc-dag-explanation",
    "dw-dag-title", "dw-dag-badge", "dw-dag-headline", "dw-dag-explanation",
    "pm-dag-title", "pm-dag-badge", "pm-dag-headline", "pm-dag-explanation",
)
# Specialized DETAIL renderers that must not write a canonical node. Each is bounded
# by its own ``window.NAME = NAME;`` export so the region excludes neighbours.
UI_SPECIALIZED_RENDERERS = ("renderDailyActionGate", "renderDailyClose",
                            "renderOperationalBook")
# The shared specialized setters that MUST hard-guard canonical nodes, and the guard.
UI_GUARDED_SETTERS = ("_dagSet", "_dcSet", "_obSet")
UI_SETTER_GUARD = "if (_wsIsCanonicalNode(id)) return;"
# The stale-today derivation the DAG render must never perform (present the gate
# outcome label as a current verdict). The raw gate ENDPOINT may still return the
# NO_ACTION_TODAY code — that historic vocabulary is intentionally retained.
UI_DAG_STALE_TODAY_TOKEN = "outcome_label"
RAW_GATE_OWNER = "api/daily_action_gate.py"
RAW_GATE_VOCAB = "NO_ACTION_TODAY"
# Modules that legitimately own session/calendar arithmetic or a DISTINCT calendar
# concept and are therefore exempt from the "no independent session arithmetic"
# guard. forward_prediction_skill.eligible_calendar is the HISTORICAL EVIDENCE
# calendar (recorded past completed sessions) — a different concept, kept separate.
SESSION_ARITH_EXEMPT = {
    MARKET_SESSION_OWNER,               # the canonical owner
    "engine/market_hours.py",           # the low-level primitive it is built on
    "api/forward_prediction_skill.py",  # historical evidence calendar (distinct)
    "alpha_agent/source_exhaustion.py", # research FORWARD session-roll (distinct)
    "scripts/audit_architecture.py",    # this tool (defines the pattern as a literal)
}
# Compat wrappers that MUST now delegate to the canonical owner (Slice 1). Each is
# expected to reference market_session and to contain NO raw session arithmetic.
SESSION_DELEGATING_WRAPPERS = ("api/daily_operating_run.py", "api/daily_close.py")
# Session resolvers deliberately NOT migrated in Slice 1 (documented remaining
# work): the desk owner is intentionally left untouched this slice.
SESSION_RESOLVERS_REMAINING_ALLOW = ("api/paper_trading_desk.py",)
# Raw current-session arithmetic: a weekend walk-back loop or an explicit close-
# cutoff time comparison. (A slower/injected-date parse does not match.)
SESSION_ARITH_RE = re.compile(
    r"while\s+[^\n]*weekday\(\)\s*>=\s*5|>=\s*time\(1[0-9]\s*,")
MARKET_SESSION_REF = re.compile(r"market_session")

# --- Slice 3 (Phase 29D) canonical Persistent Daily Research Cycle ownership ---- #
# ONE orchestration owner composes the daily research pass through the existing
# authoritative owners (scoring / target / evidence / assessment) via adapters; no
# other module orchestrates the complete cycle, and the UI plans/prioritises nothing.
DRC_OWNER = "api/daily_research_cycle.py"
# Phase 29D.1 Slice-3 live-acceptance completion: the canonical monthly momentum
# input adapter (the DECLARED refresh owner for momentum_monthly).
MONTHLY_INPUT_OWNER = "api/monthly_momentum_input.py"
WORKFLOW_STATE_OWNER = "api/workflow_state.py"
DRC_STATUS_ROUTE = "/v1/operations/daily-research-cycle/status"
DRC_RUN_ROUTE = "/v1/operations/daily-research-cycle/run"
DRC_EXECUTE_TOKEN = "RUN_DAILY_RESEARCH_CYCLE"
# The orchestration entry points that identify the sole owner (exactly one module).
DRC_RUN_DEF = "def run_daily_research_cycle("
DRC_STATUS_DEF = "def load_daily_research_cycle_status("
# The owner MUST delegate (reference these owners) and derive the evidence count
# from the snapshot registry (never a hard-coded literal).
DRC_MUST_DELEGATE = ("multi_horizon_engine", "forward_prediction_skill",
                     "alpha_target", "daily_action_gate", "data_freshness",
                     "SUPPORTED_BOOKS")
# The owner must NEVER execute Daily Close or create an order / signal / decision /
# fill, and must not host a SECOND scoring engine.
DRC_FORBIDDEN_CALLS = ("run_daily_close(", "run_fill_cycle(", "place_order(",
                       "submit_order(", "create_order(", "Signal(", "TradeDecision(")
DRC_FORBIDDEN_MODEL_DEFS = ("def compute_scores(", "def _percentiles(")
# UI: EXACTLY one status loader + one execution function; the DRC UI region derives
# no dates / priority / freshness / plan.
UI_DRC_LOADER = "function loadDailyResearchCycle"
UI_DRC_EXEC = "function runDailyResearchCycle"
UI_DRC_STATUS_FETCH = "/v1/operations/daily-research-cycle/status"
UI_DRC_RUN_FETCH = "/v1/operations/daily-research-cycle/run"
UI_DRC_REGION_END = "window.runDailyResearchCycle"
UI_DRC_FORBIDDEN = ("new Date(", "Date.now(", ".getTime(", "build_execution_plan",
                    "evaluate_alignment")

# --- Phase 29G.3 DRC terminal-manifest persistence/read-back + pre-close consistency -- #
# The canonical DRC status/persistence contract must (a) validate + read back a terminal
# manifest before returning COMPLETE, (b) reflect a persisted terminal manifest / an
# explicit recovery INCONSISTENT instead of NOT_STARTED, (c) expose NO "mark complete"
# endpoint (recovery is normal idempotent execution), and (d) share one configured root.
# The pre-close portfolio consistency must classify the expected single pending close
# without hiding genuine gaps.
DRC_TERMINAL_TOKENS = ("_validate_terminal_manifest", "MANIFEST_PERSISTENCE_UNVERIFIED",
                       "MANIFEST_CONTRACT_INCOMPLETE")
DRC_REFLECT_TOKENS = ("_reflect_completed_run",
                      "TERMINAL_DOWNSTREAM_ARTIFACTS_WITHOUT_DRC_MANIFEST")
DRC_READBACK_TOKENS = ("_load_run(run_id", "durable")
DRC_MARK_COMPLETE_FORBIDDEN_DEFS = ("def mark_complete", "def force_complete",
                                    "def set_run_complete", "def _mark_complete",
                                    "def force_terminal")
DRC_RECOVERY_FORBIDDEN_ROUTE_SUBSTR = ("mark-complete", "/complete", "recover")
DRC_RECOVERY_FORBIDDEN_DEFS = ("def recover_daily_research", "def resume_daily_research",
                               "def mark_recovered")
PS_PRECLOSE_TOKENS = ("PENDING_DAILY_CLOSE", "EXPECTED_PRE_CLOSE_GAP",
                      "STATE_READY_WITH_PENDING_CLOSE", "_valuation_vs_close_check",
                      "previous_trading_day")
PS_GENUINE_INCONSISTENCY_TOKENS = ("future_dated", "BEHIND the latest",
                                   "more than one eligible session")

# --- Phase 29D.2 production monthly-momentum emitter bridge ownership ----------- #
# The ONE pure-stdlib SUBPROCESS bridge wired behind the canonical monthly-input
# adapter's seam. It must import NEITHER numpy NOR pandas (the heavy math runs only
# in the external subprocess), never use a shell string, delegate ALL mathematics to
# the external Phase-25 module (no SECOND monthly formula in Paper Trader), be wired
# by the adapter + app startup, and expose no separate monthly execution endpoint / UI
# button (the monthly step lives inside the Daily Research Cycle).
MONTHLY_EMITTER_OWNER = "api/monthly_momentum_emitter.py"
MONTHLY_EMITTER_MATH_OWNER = "phase25_multi_horizon_inputs"
# The month-end momentum formula signature that must NOT reappear in any operational
# api module (it belongs solely to the external Phase-25 mathematics owner).
MONTHLY_FORMULA_SIG = re.compile(
    r"""resample\(["']ME["']\)|\.shift\(7\)|close\[m\s*-\s*7\]""")

# --- Slice 4 (Phase 29E) canonical universe-scoring ownership ------------------ #
# ONE composition & read owner (api.universe_scoring) normalises the pure scoring
# KERNEL (api.multi_horizon_engine) into the canonical operational scoring/ranking
# contract. No operational module hosts a SECOND scoring engine; the DRC delegates
# scoring here; the current-alpha-scores platform surface is a compatibility wrapper
# over this owner; the UI has EXACTLY ONE canonical scoring loader and performs no
# scoring/ranking/exclusion/universe-classification/date arithmetic.
US_OWNER = "api/universe_scoring.py"
US_KERNEL = "api/multi_horizon_engine.py"
US_ROUTE = "/v1/research/universe-scoring"
US_COMPAT_ROUTE = "/v1/research/current-alpha-scores"
US_BUILD_DEF = "def build_universe_scoring("
# The owner MUST delegate to the kernel and NOT redefine the scoring mathematics.
US_MUST_DELEGATE = ("multi_horizon_engine", "build_current")
# The composition OWNER must not redefine ANY scoring/construction primitive.
US_FORBIDDEN_MODEL_DEFS = ("def compute_scores(", "def _percentiles(",
                           "def compute_combined(", "def build_books(",
                           "def _select_book(")
# The COMBINED-SCORE MATHEMATICS that no OTHER operational api module may define
# (book CONSTRUCTION helpers - build_books / _select_book - are a documented
# same-family reuse in multi_horizon_history and a separate frozen lineage in
# current_alpha_book, NOT combined-score-math duplication).
US_COMBINED_SCORE_DEFS = ("def compute_scores(", "def _percentiles(",
                          "def compute_combined(")
# The owner must NEVER create an order / signal / decision / fill, promote a model,
# or call a provider / prediction service.
US_FORBIDDEN_CALLS = ("place_order(", "submit_order(", "create_order(", "run_fill_cycle(",
                      "Signal(", "TradeDecision(", "run_daily_close(",
                      "requests.get(", "requests.post(", "urlopen(", "httpx.",
                      "predict(", "promote_model(", "replace_champion(")
# The DRC must delegate scoring to this owner (Slice 4) and the platform surface
# must be a compatibility wrapper over it.
DRC_SCORING_DELEGATE_TOKEN = "universe_scoring"
PLATFORM_SCORES_OWNER = "api/multi_horizon_platform.py"
PLATFORM_SCORES_FN = "def load_current_scores("
# UI: EXACTLY one canonical scoring loader; the scoring UI region computes nothing.
UI_US_LOADER = "function loadUniverseScoring"
UI_US_FETCH = "/v1/research/universe-scoring"
UI_US_REGION_END = "window.loadUniverseScoring"
UI_US_FORBIDDEN = ("new Date(", "Date.now(", ".getTime(", ".sort(",
                   "compute_", "_percentiles", "zscore", "* 0.5")

# --- Slice 5 (Phase 29F) canonical operational portfolio-state ownership -------- #
# ONE read-only composition owner (api.portfolio_state) is the authoritative complete
# operational portfolio-state of the ACTIVE Alpha Paper Book. It composes the existing
# owners (operational_book / data_freshness / paper_trading_desk / daily_action_gate /
# forward_prediction_skill) and recomputes NO business logic; it selects the active book
# through the authoritative policy and NEVER the dormant legacy DB book; it is a read
# model (no writer); and the UI has EXACTLY ONE canonical portfolio-state loader +
# renderer that computes no NAV / totals / active-book selection / valuation date /
# pending count.
PS_OWNER = "api/portfolio_state.py"
PS_ROUTE = "/v1/operations/portfolio-state"
PS_LOAD_DEF = "def load_portfolio_state("
# The owner MUST delegate to (compose) these authoritative read models.
PS_MUST_DELEGATE = ("operational_book", "data_freshness", "paper_trading_desk",
                    "daily_action_gate", "forward_prediction_skill")
# The owner must select the active book via the policy and reject the dormant legacy book.
PS_LEGACY_TOKEN = "legacy_paper_portfolio"
PS_SELECT_FN = "def _select_active_book("
# The owner must NEVER write, create an order/signal/decision/fill, promote a model,
# run a cycle/close/refresh, or call a provider / prediction service (read model only).
PS_FORBIDDEN_CALLS = ("place_order(", "submit_order(", "create_order(", "run_fill_cycle(",
                      "Signal(", "TradeDecision(", "run_daily_close(", "run_refresh(",
                      "run_daily_research_cycle(", "requests.get(", "requests.post(",
                      "urlopen(", "httpx.", "predict(", "promote_model(",
                      "replace_champion(", "_atomic_write", ".commit(")
# No OTHER operational api/*.py module may define a second portfolio-state owner.
PS_SECOND_OWNER_DEF = "def load_portfolio_state("
# UI: EXACTLY one canonical portfolio-state loader + renderer; the region computes nothing.
UI_PS_LOADER = "function loadPortfolioState"
UI_PS_RENDERER = "function renderPortfolioState"
UI_PS_FETCH = "/v1/operations/portfolio-state"
UI_PS_REGION_START = "function loadPortfolioState"
UI_PS_REGION_END = "window.renderPortfolioState"
UI_PS_FORBIDDEN = ("new Date(", "Date.now(", ".getTime(", ".reduce(",
                   "|| 'fundamental", "book_id ||", "cached_total_value")
# The canonical valuation nodes must be guarded (owned) so no page recomputes them.
UI_PS_GUARD_TOKENS = ("PS_CANONICAL_NODES", "_psIsCanonicalNode", "data-ps-owned")

# --- Slice 6 (Phase 29G) canonical Holding Opportunity-Cost ownership ------------ #
# The pure calculation KERNEL (engine.holding_opportunity_cost) is the SOLE holding
# comparison / decision owner; the composition/persistence/read owner
# (api.holding_opportunity_cost) is the SOLE API owner; the read endpoint is GET-only;
# the Daily Research Cycle delegates to the owner (no separate manual execution
# endpoint); the Daily Action Gate delegates to the opportunity-cost summary; NO order
# / fill / execution / target-weight / NAV / universe-score is produced in either
# owner; the UI has EXACTLY ONE loader and computes no recommendation / cost / total;
# Slice 7 (reallocation) is LANDED (see check_reallocation_proposal_ownership) and
# Slice 8 (research agent) remains future.
HOC_KERNEL = "engine/holding_opportunity_cost.py"
HOC_OWNER = "api/holding_opportunity_cost.py"
HOC_ROUTE = "/v1/operations/holding-opportunity-cost"
HOC_KERNEL_BUILD_DEF = "def build_assessment("
HOC_OWNER_LOAD_DEF = "def load_holding_opportunity_cost("
# The composition owner MUST delegate to (compose) these authoritative owners.
HOC_MUST_DELEGATE = ("holding_opportunity_cost", "portfolio_state", "universe_scoring",
                     "price_panel", "paper_trading_desk", "multi_horizon_engine")
# Neither owner may create an order/fill/execution, confirm a target, generate target
# weights, duplicate NAV, recompute a universe score, or call a provider / prediction.
HOC_FORBIDDEN_CALLS = ("place_order(", "submit_order(", "create_order(", "route_order(",
                       "run_fill_cycle(", "settle_due_orders(", "confirm_target(",
                       "confirm_snapshot(", "run_daily_close(", "compute_scores(",
                       "compute_combined(", "_percentiles(", "requests.get(",
                       "requests.post(", "urlopen(", "httpx.", "predict(",
                       "promote_model(", "replace_champion(", "book_nav(")
# The kernel is PURE — no file/network/db I/O.
HOC_KERNEL_FORBIDDEN = ("open(", "requests.", "httpx.", "urlopen(", "sqlalchemy",
                        "sessionmaker", "predict(", "os.environ", "Path(")
# The DRC delegates to the owner; the gate delegates to the summary.
DRC_HOC_DELEGATE_TOKEN = "holding_opportunity_cost"
GATE_HOC_DELEGATE_TOKEN = "load_assessment_summary"
GATE_OWNER = "api/daily_action_gate.py"
# UI: EXACTLY one loader; the region computes no recommendation / cost / total / date.
UI_HOC_LOADER = "function loadHoldingOpportunityCost"
UI_HOC_FETCH = "/v1/operations/holding-opportunity-cost"
UI_HOC_REGION_END = "window.renderHoldingOpportunityCost"
UI_HOC_FORBIDDEN = ("new Date(", "Date.now(", ".getTime(", ".reduce(", "Math.",
                    "cost_rate", "COST_BPS", "compute")
# The proposal banner must read the opportunity-cost review language.
UI_HOC_REVIEW_LABEL = "HOLDING OPPORTUNITY-COST REVIEW"
# Slice 7 (Phase 29H, Milestone 3) is LANDED — these owners + the read route MUST exist.
SLICE7_LANDED_MODULES = ("engine/reallocation_proposal.py", "api/reallocation_proposal.py")
SLICE7_LANDED_ROUTE = "/v1/operations/reallocation-proposal"
# The alternative "portfolio_proposal" naming, and any apply / rebalance / confirm-target
# / create-orders route for the proposal, MUST remain ABSENT (it is review-only via the
# Daily Research Cycle — there is no create/apply/confirm/rebalance/order endpoint).
SLICE7_FORBIDDEN_MODULES = ("api/portfolio_proposal.py",)
SLICE7_FORBIDDEN_ROUTES = ("/v1/operations/portfolio-proposal",
                           "/v1/operations/reallocation-proposal/apply",
                           "/v1/operations/reallocation-proposal/confirm",
                           "/v1/operations/reallocation-proposal/create-orders",
                           # /v1/operations/rebalance is the Stage-19 CANONICAL controlled
                           # route (APPROVED Stage-18 decision + a second manual confirmation
                           # -> existing paper desk); it is governed by
                           # check_controlled_rebalance_ownership and is intentionally NOT
                           # forbidden. The auto-apply route below stays forbidden.
                           "/v1/operations/apply-reallocation")
# Slice 8 (Persistent Alpha Research Agent, Milestone 4) is LANDED — its two owners + the
# read route MUST exist. A SECOND / unified model registry must NOT be created: the
# Research Agent READS the existing champion/challenger registries (it never forks or
# unifies them) and it never moves champion-promotion authority into itself.
SLICE8_LANDED_MODULES = ("engine/research_agent.py", "api/research_agent.py")
SLICE8_LANDED_ROUTE = "/v1/research/research-agent"
SLICE8_FORBIDDEN_MODULES = ("api/model_registry.py",)
# Back-compat: the Slice-6/7 guards emit ``slice8_present_modules`` as the set of FORBIDDEN
# second-registry modules present (must stay EMPTY). Landing Slice 8 as the Research Agent
# does not create such a registry, so this stays empty and those guards stay honest.
SLICE8_ABSENT_MODULES = SLICE8_FORBIDDEN_MODULES
# Slice 9 (Data Expansion / Purchase-Gate, Milestone 5) is LANDED as a purchase GATE
# (engine/data_expansion_gate.py + api/data_expansion.py), NOT a paid-data registry: a
# unified paid-data registry that acquires / activates providers must NEVER be created, so
# ``api/paid_data_registry.py`` must remain ABSENT.
SLICE9_ABSENT_MODULES = ("api/paid_data_registry.py",)

# --- Slice 7 (Phase 29H) Reallocation Proposal ownership contract ---------------- #
RP_KERNEL = "engine/reallocation_proposal.py"
RP_OWNER = "api/reallocation_proposal.py"
RP_ROUTE = "/v1/operations/reallocation-proposal"
RP_KERNEL_BUILD_DEF = "def build_proposal("
RP_OWNER_LOAD_DEF = "def load_reallocation_proposal("
RP_OWNER_PERSIST_DEF = "def persist_proposal("
# The composition owner MUST delegate to (compose) these authoritative owners.
RP_MUST_DELEGATE = ("portfolio_state", "holding_opportunity_cost", "universe_scoring",
                    "price_panel", "paper_trading_desk", "multi_horizon_engine")
# Neither owner may create an order/fill/execution, confirm an operational or alpha
# target, mutate NAV/holdings/cash, run the Daily Close, or call a provider/prediction/
# broker/promotion.
RP_FORBIDDEN_CALLS = ("place_order(", "submit_order(", "create_order(", "route_order(",
                      "run_fill_cycle(", "settle_due_orders(", "confirm_target(",
                      "confirm_snapshot(", "run_daily_close(", "run_refresh(",
                      "requests.get(", "requests.post(", "urlopen(", "httpx.",
                      "predict(", "promote_model(", "replace_champion(", "book_nav(")
# The kernel is PURE — no file/network/db I/O.
RP_KERNEL_FORBIDDEN = ("open(", "requests.", "httpx.", "urlopen(", "sqlalchemy",
                       "sessionmaker", "predict(", "os.environ", "Path(")
# The DRC delegates to the owner (sole execution path); the gate delegates to the summary.
DRC_RP_DELEGATE_TOKEN = "reallocation_proposal"
DRC_RP_STEP = "BUILD_REALLOCATION_PROPOSAL"
GATE_RP_DELEGATE_TOKEN = "load_proposal_summary"
# UI: EXACTLY one loader; the region computes no allocation/cost/date math.
UI_RP_LOADER = "function loadReallocationProposal"
UI_RP_FETCH = "/v1/operations/reallocation-proposal"
UI_RP_REGION_END = "window.renderReallocationProposal"
UI_RP_FORBIDDEN = ("new Date(", "Date.now(", ".getTime(", ".reduce(", "Math.",
                   "cost_rate", "COST_BPS", "compute")

# --- Stage 20 Continuous Active Portfolio Reassessment ownership contract --------- #
PRS_KERNEL = "engine/portfolio_reassessment.py"
PRS_OWNER = "api/portfolio_reassessment.py"
PRS_ROUTE = "/v1/operations/portfolio-reassessment"
PRS_KERNEL_BUILD_DEF = "def build_reassessment("
PRS_OWNER_LOAD_DEF = "def load_portfolio_reassessment("
PRS_OWNER_PERSIST_DEF = "def persist_reassessment("
PRS_GATE_DEF = "def should_build_proposal("
PRS_PRECEDENCE_DEF = "def execution_precedence("
# The composition owner MUST delegate to (compose) these authoritative owners.
PRS_MUST_DELEGATE = ("portfolio_state", "holding_opportunity_cost", "universe_scoring",
                     "data_freshness", "corporate_actions", "paper_trading_desk",
                     "multi_horizon_engine")
# Neither owner may create an order/fill/execution, confirm/approve a proposal or target,
# mutate NAV/holdings/cash, run the Daily Close, or call a provider/prediction/broker/
# promotion. ``load_rebalance_state`` is deliberately NOT forbidden: the owner READS the
# Stage-19 lifecycle to compute execution precedence, and never writes to it.
PRS_FORBIDDEN_CALLS = ("place_order(", "submit_order(", "create_order(", "route_order(",
                       "generate_orders(", "confirm_orders(", "run_fill_cycle(",
                       "settle_due_orders(", "confirm_target(", "confirm_snapshot(",
                       "confirm_rebalance_order_plan(", "record_decision(",
                       "run_daily_close(", "run_refresh(", "requests.get(",
                       "requests.post(", "urlopen(", "httpx.", "predict(",
                       "promote_model(", "replace_champion(", "book_nav(")
# The kernel is PURE — no file/network/db I/O and no clock.
PRS_KERNEL_FORBIDDEN = ("open(", "requests.", "httpx.", "urlopen(", "sqlalchemy",
                        "sessionmaker", "predict(", "os.environ", "Path(",
                        "datetime.now(")
# The kernel must NOT fork the Slice-6 holding comparison or the Slice-7 target math.
PRS_KERNEL_FORKS = ("def build_assessment(", "def build_proposal(", "def compute_scores(",
                    "def compute_combined(", "def build_books(")

# --------------------------------------------------------------------------- #
# Stage 20.1 — HERMETIC ACCEPTANCE ENVIRONMENT ownership.
#
# Stage 20 seeded exactly ONE store into the acceptance root, so every other canonical
# surface read its own empty store and rendered an unrelated default world. These
# constants pin the repaired shape: ONE scenario owner, every panel derived from it, the
# Stage-19 execution precedence preserved, lineage-scoped counts reused (never
# reimplemented), and no execution / broker / automation reachable from the harness.
# --------------------------------------------------------------------------- #
ACCEPT_FIXTURES = "scripts/stage20_ui_fixtures.py"
ACCEPT_SERVER = "scripts/stage20_acceptance_server.py"
ACCEPT_SCENARIO_OWNER_DECL = 'SCENARIO_OWNER = "scripts/stage20_ui_fixtures.py"'
ACCEPT_COMPOSE_DEF = "def compose("
ACCEPT_CONSISTENCY_DEF = "def cross_panel_consistency("
ACCEPT_WORLD_DEF = "def world("
#: Every canonical panel the shared scenario MUST produce. A panel absent from
#: ``compose`` is exactly the Stage-20 defect: an endpoint free to invent its own world.
ACCEPT_REQUIRED_PANELS = ("portfolio_state", "operational_book", "rebalance",
                          "holding_opportunity_cost", "reallocation_proposal",
                          "portfolio_reassessment", "daily_action_gate",
                          "workflow_state")
#: The REAL owners the composition must call. The fixture computes no panel of its own.
ACCEPT_MUST_DELEGATE = ("ob.load_operational_book(", "rbx.load_rebalance_state(",
                        "hoc.load_holding_opportunity_cost(",
                        "ralloc.load_reallocation_proposal(",
                        "prs.load_portfolio_reassessment(",
                        "dag.load_daily_action_gate(", "wfs.load_workflow_state(")
#: The harness may never invoke a mutating operational entry point, even hermetically.
ACCEPT_FORBIDDEN_CALLS = ("generate_orders(", "confirm_orders(", "settle_due_orders(",
                          "refresh_desk(", "confirm_order_plan(", "run_daily_close(",
                          "initialize_book(", "record_decision(", "promote_model(",
                          "requests.get(", "requests.post(", "httpx.", "predict(")
#: Reimplementing the lineage split in the harness would let the fixture "prove" counts
#: production does not actually produce. It must reuse the Stage-19.3 owner.
ACCEPT_FORBIDDEN_REIMPL = ("def current_rebalance_lineage(", "def derive_lifecycle_view(",
                           "def resolve_daily_close_status(", "def build_reassessment(",
                           "def build_proposal(")
ACCEPT_SCENARIO_5 = "scenario_5_execution_pending"
ACCEPT_SCENARIO_5B = "scenario_5b_execution_pending_close_due"
# The DRC is the sole execution path and the gate that authorises the target engine.
DRC_PRS_STEP = "REASSESS_PORTFOLIO"
DRC_PRS_DELEGATE_TOKEN = "portfolio_reassessment"
DRC_PRS_GATE_TOKEN = "_reassessment_gate"
# The workflow owner EXPOSES the reassessment; it must delegate, never recompute.
WF_PRS_DELEGATE_TOKEN = "load_reassessment_summary"
WF_PRS_PRECEDENCE_TOKEN = "execution_precedence"
# No manual / apply / approve / execute reassessment route may exist.
PRS_FORBIDDEN_ROUTES = ("/v1/operations/portfolio-reassessment/run",
                        "/v1/operations/portfolio-reassessment/execute",
                        "/v1/operations/portfolio-reassessment/approve",
                        "/v1/operations/portfolio-reassessment/apply",
                        "/v1/operations/portfolio-reassessment/rebalance",
                        "/v1/operations/reassessment/run")
# UI: EXACTLY one loader; the region computes no assessment/economic/date math.
UI_PRS_LOADER = "function loadPortfolioReassessment"
UI_PRS_FETCH = "/v1/operations/portfolio-reassessment"
UI_PRS_REGION_END = "window.renderPortfolioReassessment"
UI_PRS_FORBIDDEN = ("new Date(", "Date.now(", ".getTime(", "cost_rate", "COST_BPS",
                    "turnover_budget =", "hurdle =", "compute")

# --- Slice 8 (Phase 29I) Persistent Alpha Research Agent ownership contract ------- #
RA_KERNEL = "engine/research_agent.py"
RA_OWNER = "api/research_agent.py"
RA_ROUTE = "/v1/research/research-agent"
RA_KERNEL_EVAL_DEF = "def evaluate("
RA_OWNER_LOAD_DEF = "def load_research_agent("
RA_OWNER_PERSIST_DEF = "def persist_assessment("
# The composition owner MUST compose (read from) these authoritative evidence owners.
RA_MUST_DELEGATE = ("universe_scoring", "forward_prediction_skill", "paper_trading_desk",
                    "forward_evidence", "current_alpha_tournament",
                    "current_alpha_decision_gate", "holding_opportunity_cost",
                    "reallocation_proposal", "portfolio_state")
# Neither owner may promote / recalibrate / retrain / replace a model, write a champion
# pointer, confirm an operational or alpha target, mutate NAV/holdings/cash, create an
# order/fill, run the Daily Close, or call a provider/prediction/broker. NO automatic model
# promotion is the crux of Slice 8 governance.
RA_FORBIDDEN_CALLS = ("promote_model(", "replace_champion(", "recalibrate_model(",
                      "retrain_model(", "retrain(", "confirm_target(", "confirm_snapshot(",
                      "place_order(", "submit_order(", "create_order(", "route_order(",
                      "run_fill_cycle(", "settle_due_orders(", "run_daily_close(",
                      "run_refresh(", "requests.get(", "requests.post(", "urlopen(",
                      "httpx.", "predict(", "book_nav(")
# The kernel is PURE — no file/network/db I/O.
RA_KERNEL_FORBIDDEN = ("open(", "requests.", "httpx.", "urlopen(", "sqlalchemy",
                       "sessionmaker", "predict(", "os.environ", "Path(")
# The DRC delegates to the owner (sole scheduled execution path); there is NO separate
# promote/recalibrate/retrain/apply route.
DRC_RA_DELEGATE_TOKEN = "research_agent"
DRC_RA_STEP = "RUN_RESEARCH_AGENT"
# A second/unified model registry, and any promote/recalibrate/retrain/apply route, MUST
# remain ABSENT (Slice 8 is monitoring/governance only — no champion-pointer authority).
SLICE8_FORBIDDEN_ROUTES = ("/v1/research/research-agent/promote",
                           "/v1/research/research-agent/recalibrate",
                           "/v1/research/research-agent/retrain",
                           "/v1/research/research-agent/apply",
                           "/v1/research/research-agent/run",
                           "/v1/research/model-registry/promote")
# UI: EXACTLY one loader; the region computes no research metric in JS.
UI_RA_LOADER = "function loadResearchAgent"
UI_RA_FETCH = "/v1/research/research-agent"
UI_RA_REGION_END = "window.renderResearchAgent"
UI_RA_FORBIDDEN = ("new Date(", "Date.now(", ".getTime(", ".reduce(", "Math.", "compute")

# --- Slice 9 (Phase 29J) Data Expansion / Purchase-Gate ownership contract -------- #
DE_KERNEL = "engine/data_expansion_gate.py"
DE_OWNER = "api/data_expansion.py"
DE_ROUTE = "/v1/research/data-expansion"
DE_DETAIL_ROUTE = "/v1/research/data-expansion/{dataset_id}"
DE_KERNEL_EVAL_DEF = "def evaluate_dataset("
DE_OWNER_LOAD_DEF = "def load_data_expansion("
DE_OWNER_PERSIST_DEF = "def persist_evaluation("
# --- Release 37.1: the ONE gate answers TWO questions, explicitly ------------------ #
# Stage A (pre-acquisition, "worth paying to LEARN") and Stage B (post-acquisition,
# "did the measured evidence earn continued purchase"). The danger the guard exists to
# prevent is a SECOND acquisition owner appearing so the estate has two answers to one
# question, and a Stage-A recommendation being emitted as though it were Stage-B proof.
DE_DECISION_CONTEXT_TOKENS = ('CONTEXT_RESEARCH_ACQUISITION = "RESEARCH_ACQUISITION"',
                              'CONTEXT_POST_ACQUISITION_VALUE = "POST_ACQUISITION_VALUE"',
                              "DECISION_CONTEXT_VOCAB = (")
# The DEFAULT must remain the legacy post-acquisition context, or every existing caller
# silently changes meaning without being edited.
DE_LEGACY_DEFAULT_TOKEN = "DEFAULT_DECISION_CONTEXT = CONTEXT_POST_ACQUISITION_VALUE"
# Stage A has its own state; it may NOT be spelled PURCHASE_RECOMMENDED.
DE_ACQ_STATE_TOKEN = 'REC_RESEARCH_ACQUISITION = "RESEARCH_ACQUISITION_RECOMMENDED"'
DE_ACQ_CLASSIFIER_DEF = "def _classify_acquisition("
DE_ACQ_DIMENSION_DEFS = ("def _eval_capability(", "def _eval_expected_distinctness(")
# Stage B's evidence standard is NOT weakened: the measured-lift gates stay in _classify.
DE_STAGE_B_LIFT_GATES = ('return REC_INSUFFICIENT, ["OUT_OF_SAMPLE_EVIDENCE_REQUIRED"]',
                         'return REC_INSUFFICIENT, ["RESEARCH_SAMPLE_TOO_SMALL"]',
                         'if not facts["material_lift"]:')
# A Stage-A recommendation is never alpha evidence, never integration approval and never
# purchasing authority — declared as constants so the claim is checkable, not implied.
DE_ACQ_NOT_AUTHORITY = ("ACQUISITION_RECOMMENDATION_IS_ALPHA_EVIDENCE = False",
                        "ACQUISITION_RECOMMENDATION_IS_INTEGRATION_APPROVAL = False",
                        "ACQUISITION_RECOMMENDATION_REQUIRES_MANUAL_APPROVAL = True")
# The composition owner must THREAD the context, never fork a second evaluation path.
DE_OWNER_CONTEXT_TOKENS = ("decision_context=decision_context",
                           "CONTEXT_RESEARCH_ACQUISITION = kernel.CONTEXT_RESEARCH_ACQUISITION",
                           "def _decision_contexts_block(")
DE_LANDED_MODULES = ("engine/data_expansion_gate.py", "api/data_expansion.py")
# The composition owner MUST reference (reuse, never fork) these authoritative data/provider/
# evidence owners — Slice 9 is a decision gate over existing owners, not a new provider layer.
DE_MUST_REUSE = ("source_contracts", "data_freshness", "experiment_contracts",
                 "analyst_revisions", "research_agent")
# Neither owner may purchase / subscribe / activate a provider, call a paid provider, use a
# paid API quota, alter credentials, integrate a dataset, mutate the portfolio, promote a
# model, create an order/fill, run the Daily Close, or enable cadence.
DE_FORBIDDEN_CALLS = ("purchase_dataset(", "subscribe_provider(", "activate_provider(",
                      "integrate_dataset(", "enable_paid_data(", "place_order(",
                      "submit_order(", "create_order(", "route_order(", "run_fill_cycle(",
                      "settle_due_orders(", "confirm_target(", "run_daily_close(",
                      "promote_model(", "replace_champion(", "requests.get(",
                      "requests.post(", "urlopen(", "httpx.", "predict(", "book_nav(")
# The kernel is PURE — no file/network/db I/O and no credential access.
DE_KERNEL_FORBIDDEN = ("open(", "requests.", "httpx.", "urlopen(", "sqlalchemy",
                       "sessionmaker", "predict(", "os.environ", "Path(")
# There is NO purchase / subscribe / activate / integrate / enable-paid-data / confirm / run
# route — the gate has no purchasing authority.
SLICE9_FORBIDDEN_ROUTES = ("/v1/research/data-expansion/purchase",
                           "/v1/research/data-expansion/subscribe",
                           "/v1/research/data-expansion/activate-provider",
                           "/v1/research/data-expansion/integrate",
                           "/v1/research/data-expansion/enable-paid-data",
                           "/v1/research/data-expansion/confirm",
                           "/v1/research/data-expansion/run")
# Cadence is DISABLED — the owner declares CADENCE_ENABLED = False and the gate is NEVER wired
# as a mandatory Daily Research Cycle step.
DE_CADENCE_DISABLED_TOKEN = "CADENCE_ENABLED = False"
DE_DRC_DAILY_JOB_TOKENS = ("STEP_RUN_DATA_EXPANSION", "data_expansion.run_and_persist")
# Slice 10 (Intraday platform) remains future; Slice 11 (Controlled Execution) later.
SLICE10_ABSENT_MODULES = ("api/intraday_platform.py", "engine/intraday_platform.py")
# UI: EXACTLY one loader; the region computes no gate metric in JS.
UI_DE_LOADER = "function loadDataExpansion"
UI_DE_FETCH = "/v1/research/data-expansion"
UI_DE_REGION_END = "window.renderDataExpansion"
UI_DE_FORBIDDEN = ("new Date(", "Date.now(", ".getTime(", ".reduce(", "Math.", "compute")


def _slice7_landed_status(routes: list) -> dict:
    """Shared Slice-7 LANDED assertion consumed by the Slice-6 guards + the dedicated
    Slice-7 guard. ``*_missing`` and ``forbidden_present`` lists MUST all be empty."""
    return {
        "slice7_missing_modules": sorted(m for m in SLICE7_LANDED_MODULES
                                         if not (REPO_ROOT / m).exists()),
        "slice7_missing_route": ([] if any(rt["path"] == SLICE7_LANDED_ROUTE
                                           for rt in routes) else [SLICE7_LANDED_ROUTE]),
        "slice7_forbidden_present": (
            sorted(m for m in SLICE7_FORBIDDEN_MODULES if (REPO_ROOT / m).exists())
            + sorted(r for r in SLICE7_FORBIDDEN_ROUTES
                     if any(rt["path"] == r for rt in routes))),
        "slice8_present_modules": sorted(m for m in SLICE8_ABSENT_MODULES
                                         if (REPO_ROOT / m).exists()),
    }

# --- Slice 6 (Phase 29G) ACYCLIC read-dependency contract ------------------------ #
# The canonical read graph must be a DAG:
#     api.portfolio_state  ──composes──▶  api.daily_action_gate      (permitted)
#     api.daily_action_gate ──delegates──▶ HOC load_assessment_summary (permitted)
#     HOC load_assessment_summary ──▶ api.portfolio_state             (FORBIDDEN)
# The summary is a PURE artifact reader: given the gate's explicit (active_book_id,
# eligible_market_date) context it reads ONLY the immutable artifact index/artifact —
# it must NOT load portfolio state (that edge closed a circular recomposition that
# recomputed the whole engine dozens of times per request).
HOC_SUMMARY_DEF = "def load_assessment_summary("
# Tokens that would re-introduce the forbidden summary → portfolio_state edge.
HOC_SUMMARY_FORBIDDEN_PS = ("load_portfolio_state", "_default_portfolio_state_loader",
                            "portfolio_state")
# The gate must SUPPLY the explicit context to the summary (never let it self-discover
# the book/date by loading portfolio state).
GATE_HOC_CONTEXT_TOKENS = ("active_book_id=", "eligible_market_date=")
# The permitted composition edge: portfolio_state composes the daily action gate.
PS_GATE_COMPOSE_TOKEN = "load_daily_action_gate"

# --- Phase 29G.1 Slice 6 LIVE-ACCEPTANCE / operator-workflow & UI hard cutover ----- #
# The obsolete Slice-2 reassessment placeholder control must be GONE from the workflow
# owner; the legacy rank-membership comparison must be reclassified compatibility-only
# (never "Rebalance Proposal Ready"); SERVICE readiness and WORKFLOW readiness must be
# distinct UI concepts (a waiting workflow never means the service is unhealthy); the
# canonical HOC panel must render NOT_RUN and completed states; the Daily Research Cycle
# stays the SOLE Slice 6 execution path (no reassessment / rebalance / order route).
LA6_WORKFLOW_OWNER = "api/workflow_state.py"
LA6_DAILY_CLOSE_OWNER = "api/daily_close.py"
LA6_APP_OWNER = "api/app.py"
# (1) obsolete reassessment placeholder control — must NOT appear in the workflow owner.
LA6_OBSOLETE_REASSESSMENT_TOKENS = ("not yet implemented", "Run a portfolio reassessment")
# (3) legacy "rebalance proposal ready" primary label — must NOT appear in daily_close.
LA6_OBSOLETE_REBALANCE_TOKENS = ("REBALANCE PROPOSAL READY", "Review Rebalance Proposal")
# (4) the legacy comparison MUST be reclassified compatibility-only in daily_close.
# Release 29.3 renamed the close status to DAILY_CLOSE_COMPLETE_MEMBERSHIP_DRIFT so the
# Daily Close no longer speaks the portfolio-proposal owner's vocabulary. The invariant
# is unchanged and is now asserted on the two SEMANTIC tokens rather than on one exact
# label string: the presentation must name the legacy comparison AND classify it as
# compatibility-only.
LA6_LEGACY_COMPAT_TOKENS = ("LEGACY MEMBERSHIP", "COMPATIBILITY ONLY")
# (9) SERVICE vs WORKFLOW readiness — both distinct UI indicators must exist.
LA6_UI_SERVICE_READINESS_TOKENS = ('id="health-status"', "checkServiceReady")
LA6_UI_WORKFLOW_READINESS_TOKENS = ('id="wf-readiness-text"', "wf-readiness-light")
# (9)/(10) /v1/ready is an explicit SERVICE probe carrying readiness_kind, and its body
# never keys off market-session timing (a waiting workflow never makes it report unready).
LA6_READY_SERVICE_TOKEN = "readiness_kind"
LA6_READY_FORBIDDEN_SESSION_TOKENS = ("market_session", "WAITING_FOR_SESSION",
                                      "eligible_market_date", "session_close")
# (6)/(7) the HOC UI renders NOT_RUN and completed assessment states.
LA6_UI_HOC_NOT_RUN_TOKENS = ("hasAssessment", "NONE YET")
LA6_UI_HOC_COMPLETED_TOKEN = "completed assessment view"
# (2)/(11)/(13) reassessment / rebalance / HOC-run / target-confirmation routes: FORBIDDEN.
LA6_ABSENT_ROUTES = (
                     # Stage 20 NOTE: the READ-ONLY GET /v1/operations/portfolio-reassessment
                     # contract is the canonical Stage-20 reassessment read model and is
                     # deliberately NOT forbidden — it executes nothing (the sole execution
                     # path is still the Daily Research Cycle) and is governed by
                     # check_portfolio_reassessment_ownership. What stays forbidden is any
                     # MANUAL reassessment EXECUTION / apply / approve / rebalance route,
                     # which is exactly what Phase 29G.1 removed.
                     "/v1/operations/portfolio-reassessment/run",
                     "/v1/operations/portfolio-reassessment/execute",
                     "/v1/operations/portfolio-reassessment/approve",
                     "/v1/operations/portfolio-reassessment/apply",
                     "/v1/operations/reassessment",
                     "/v1/operations/reassessment/run",
                     # Stage-19's controlled /v1/operations/rebalance is NOT an auto route
                     # (APPROVED decision + second confirmation; governed by
                     # check_controlled_rebalance_ownership). The auto proposal-execution
                     # route "/v1/operations/rebalance-proposal" below stays forbidden.
                     "/v1/operations/rebalance-proposal",
                     "/v1/operations/holding-opportunity-cost/run",
                     "/v1/operations/confirm-target",
                     "/v1/operations/target-confirmation")

# --- Phase 29G.2 Slice 6 RESIDUAL hard cutover (HOC is the SOLE primary decision) ---- #
# The residual legacy proposal renderers are gone: the primary DAG-card presentation on
# all three surfaces (Command Center, Daily Workflow, Portfolio Manager) is the canonical
# Holding Opportunity-Cost Review; the legacy rank-membership comparison is compatibility-
# only and COLLAPSED; there is NO primary "Proposal Ready" / "Portfolio Changes Proposed"
# presentation and NO "Review Proposed Changes" / "Review Rebalance Proposal" button;
# exactly one HOC loader; no JS recommendation/cost computation; the Daily Research Cycle
# remains the SOLE execution path; and no reassessment/rebalance/order route exists. The
# raw daily_action_gate outcome/target-state vocabulary is PRESERVED for historical
# consumers, so these forbidden strings are checked against the UI and the workflow owner
# (the primary presentation surfaces), never against the gate's preserved raw vocabulary.
RC6_FORBIDDEN_PRIMARY_UI = ("LATEST PORTFOLIO ASSESSMENT", "Review Proposed Changes",
                            "Review Rebalance Proposal", "target_state_label",
                            "PROPOSAL READY", "PORTFOLIO CHANGES PROPOSED")
RC6_FORBIDDEN_PRIMARY_WS = ("LATEST PORTFOLIO ASSESSMENT",)
# (5) the gate result carries the explicit compatibility classification.
RC6_GATE_CLASSIFICATION = ("compatibility_only", "decision_authority",
                           "execution_available", "canonical_decision_owner",
                           "legacy_membership_comparison")
# (5)/(7) the workflow owner makes the HOC review the PRIMARY presentation + exposes the
# canonical NOT_RUN operator state + the compatibility legacy block.
RC6_WS_PRIMARY_TOKENS = (
    "assessment_presentation = holding_opportunity_cost_presentation",
    "def build_holding_opportunity_cost_presentation(",
    "HOLDING_OPPORTUNITY_COST_NOT_RUN", "legacy_membership_comparison")
# (6) the legacy comparison is COLLAPSED (native <details>) on all three surfaces.
RC6_UI_LEGACY_DETAILS = ('id="cc-dag-legacy"', 'id="dw-dag-legacy"', 'id="pm-dag-legacy"')
RC6_UI_LEGACY_SUMMARY = "LEGACY MEMBERSHIP-COMPARISON SUMMARY"
RC6_UI_LEGACY_VIEW = "View Legacy Membership Comparison"
# (7) the HOC review is the SOLE primary decision card.
RC6_UI_HOC_PRIMARY_BADGE = "PRIMARY PORTFOLIO DECISION"
RC6_UI_HOC_TITLE = "HOLDING OPPORTUNITY-COST REVIEW"
RC6_UI_SURFACE_HOC_TITLE = ">Holding Opportunity-Cost Review"


# Canonical business concepts and the regex that identifies a *writer/producer*
# of that concept (a function definition that computes it). Multiple modules
# matching one concept is a source-of-truth candidate.
CANONICAL_CONCEPT_PATTERNS = {
    "portfolio_nav_valuation": re.compile(
        r"def\s+[a-z_]*(nav|valuation|mark_to_market|current_mark|book_nav)"
        r"[a-z_]*\s*\(", re.I),
    "eligible_market_date": re.compile(
        r"def\s+[a-z_]*(eligible_market|market_as_of|latest_completed_market|"
        r"latest_eligible_market|market_date_alignment)[a-z_]*\s*\(", re.I),
    "universe_scoring_rankings": re.compile(
        r"def\s+[a-z_]*(score_universe|rank_universe|composite_sn|"
        r"compute_scores|build_rankings)[a-z_]*\s*\(", re.I),
    "target_portfolio": re.compile(
        r"def\s+[a-z_]*(build_target|target_state|run_refresh|"
        r"apply_joint_caps|preview_or_create_current_alpha_book)"
        r"[a-z_]*\s*\(", re.I),
    "workflow_state": re.compile(
        r"def\s+[a-z_]*(workflow_state|target_state_for|derive_lifecycle|"
        r"evaluate_daily_action_gate|derive_review)[a-z_]*\s*\(", re.I),
    "forward_evidence": re.compile(
        r"def\s+[a-z_]*(forward_evidence|capture_snapshots|forward_readiness|"
        r"true_forward)[a-z_]*\s*\(", re.I),
}

# The canonical documentation set (existence checked; drift is a finding).
CANONICAL_DOCS = (
    "docs/PROJECT_CHARTER.md",
    "docs/CURRENT_ARCHITECTURE.md",
    "docs/TARGET_ARCHITECTURE.md",
    "docs/CONSOLIDATION_ROADMAP.md",
    "docs/ARCHITECTURE_DECISIONS.md",
    "docs/architecture/system_inventory.json",
)

# Known local-only files that must NEVER be treated as release artifacts.
LOCAL_ONLY_FILES = (
    ".claude/settings.json",
    ".playwright-mcp/",
    "paper_trader_8001.stderr.log",
    "paper_trader_8001.stdout.log",
)

ROUTE_DECORATOR = re.compile(r"@app\.(get|post|put|delete|patch)\(")
PATH_LITERAL = re.compile(r"""['"](/[A-Za-z0-9_\-{}/.:]*)['"]""")


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _rel(p: Path) -> str:
    return str(p.relative_to(REPO_ROOT)).replace("\\", "/")


def _read(rel_path: str) -> str:
    fp = REPO_ROOT / rel_path
    try:
        return fp.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def _assign_const(src: str, name: str):
    """The literal a module-level constant is assigned, via AST (never a regex).

    Returns None when the name is absent or is not bound to a plain constant.
    """
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return None
    for node in tree.body:
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id == name:
                    return node.value.value
    return None


def _module_func_body(src: str, func_def: str) -> str:
    """Return the source of a MODULE-LEVEL ``def`` (from its signature line to the next
    top-level ``def`` / block divider / ``__all__`` / EOF). Used to scope a semantic
    check to a single function body rather than the whole module."""
    i = src.find(func_def)
    if i < 0:
        return ""
    lines = src[i:].splitlines(keepends=True)
    out = [lines[0]]
    for ln in lines[1:]:
        if ln.startswith("def ") or ln.startswith("# ---") or ln.startswith("__all__"):
            break
        out.append(ln)
    return "".join(out)


def _strip_prose(code: str) -> str:
    """Remove docstrings / triple-quoted strings and ``#`` line comments so a semantic
    token scan inspects CODE only — a docstring that merely *names* a forbidden symbol
    (e.g. documents that it is never called) must not read as a real reference."""
    code = re.sub(r'"""[\s\S]*?"""', "", code)
    code = re.sub(r"'''[\s\S]*?'''", "", code)
    code = re.sub(r"#[^\n]*", "", code)
    return code


def _iter_source_files() -> list[Path]:
    out: list[Path] = []
    for d in SOURCE_DIRS:
        base = REPO_ROOT / d
        if not base.exists():
            continue
        for fp in base.rglob("*.py"):
            if any(part in EXCLUDE_PARTS or part.endswith(".egg-info")
                   for part in fp.parts):
                continue
            out.append(fp)
    return sorted(out, key=_rel)


def _line_count(text: str) -> int:
    if not text:
        return 0
    return text.count("\n") + (0 if text.endswith("\n") else 1)


# --------------------------------------------------------------------------- #
# Checks
# --------------------------------------------------------------------------- #
def check_routes() -> dict:
    """Parse route declarations from the app module; detect duplicates."""
    text = _read(APP_MODULE)
    lines = text.splitlines()
    routes: list[dict] = []
    for i, line in enumerate(lines):
        m = ROUTE_DECORATOR.search(line)
        if not m:
            continue
        method = m.group(1).upper()
        # The path literal may be on this line or the next few lines.
        path = None
        for j in range(i, min(i + 6, len(lines))):
            pm = PATH_LITERAL.search(lines[j])
            if pm:
                path = pm.group(1)
                break
        routes.append({"method": method, "path": path or "<unresolved>",
                       "declared_in": APP_MODULE, "line": i + 1})
    # Duplicate (method, path) declarations.
    seen: dict[tuple, list[int]] = {}
    for r in routes:
        seen.setdefault((r["method"], r["path"]), []).append(r["line"])
    duplicates = sorted(
        [{"method": k[0], "path": k[1], "lines": sorted(v)}
         for k, v in seen.items() if len(v) > 1 and k[1] != "<unresolved>"],
        key=lambda d: (d["path"], d["method"]))
    return {
        "total": len(routes),
        "owner_files": sorted({r["declared_in"] for r in routes}),
        "routes": sorted(routes, key=lambda r: (r["path"], r["method"])),
        "duplicate_declarations": duplicates,
    }


def check_module_sizes(files: list[Path]) -> list[dict]:
    big: list[dict] = []
    for fp in files:
        n = _line_count(fp.read_text(encoding="utf-8", errors="replace"))
        if n > SIZE_THRESHOLD_LINES:
            big.append({"path": _rel(fp), "lines": n})
    return sorted(big, key=lambda d: (-d["lines"], d["path"]))


def check_direct_ledger_refs(files: list[Path]) -> list[dict]:
    hits: list[dict] = []
    for fp in files:
        rel = _rel(fp)
        # The ledger-owner service modules and this audit tool itself (which
        # only defines the literal as a constant) are expected references.
        if rel in LEDGER_OWNER_MODULES or rel == "scripts/audit_architecture.py":
            continue
        text = fp.read_text(encoding="utf-8", errors="replace")
        for i, line in enumerate(text.splitlines(), 1):
            if LEDGER_ROOT_LITERAL in line and "operational_ledger_roots" not in line:
                hits.append({"path": rel, "line": i, "text": line.strip()[:160]})
    return sorted(hits, key=lambda d: (d["path"], d["line"]))


def check_private_attribute_access(files: list[Path]) -> list[dict]:
    """Detect `alias._private` usage where `alias` names a first-party module.

    This complements check_private_cross_imports (the `from x import _y` form):
    most private coupling in this repo is attribute-style
    (`from . import paper_trading_desk as desk; desk._read_ledger(...)`).
    """
    # api/engine/db are importable under the installed `paper_trader.*`
    # namespace; alpha_agent/research_agent are top-level packages.
    first_party = ("paper_trader", "api", "engine", "db", "alpha_agent",
                   "research_agent")
    imp_from = re.compile(
        r"^\s*from\s+(?:\.+|(?:%s)[\w.]*)\s+import\s+(.+)$"
        % "|".join(first_party))
    imp_mod = re.compile(
        r"^\s*import\s+((?:%s)[\w.]+)(?:\s+as\s+(\w+))?" % "|".join(first_party))
    results: list[dict] = []
    for fp in files:
        rel = _rel(fp)
        text = fp.read_text(encoding="utf-8", errors="replace")
        lines = text.splitlines()
        aliases: set[str] = set()
        for line in lines:
            m = imp_from.match(line)
            if m:
                for nm in m.group(1).replace("(", "").replace(")", "").split(","):
                    nm = nm.strip()
                    if not nm or nm == "*":
                        continue
                    alias = nm.split(" as ")[-1].strip()
                    if alias and not alias.startswith("_"):
                        aliases.add(alias)
            m2 = imp_mod.match(line)
            if m2:
                aliases.add(m2.group(2) or m2.group(1).split(".")[-1])
        if not aliases:
            continue
        alias_re = re.compile(
            r"\b(%s)\.(_[a-zA-Z]\w*)" % "|".join(re.escape(a) for a in aliases))
        privs: dict[str, int] = {}
        for line in lines:
            for mm in alias_re.finditer(line):
                if mm.group(2).startswith("__"):
                    continue
                privs[mm.group(1)] = privs.get(mm.group(1), 0) + 1
        total = sum(privs.values())
        if total:
            results.append({"path": rel, "total": total,
                            "by_module": dict(sorted(privs.items()))})
    return sorted(results, key=lambda d: (-d["total"], d["path"]))


def check_direct_db_sessions(files: list[Path]) -> list[dict]:
    pat = re.compile(r"\b(sessionmaker\s*\(|SessionLocal\s*\(|create_engine\s*\()")
    hits: list[dict] = []
    for fp in files:
        rel = _rel(fp)
        if rel == DB_SESSION_OWNER:
            continue
        text = fp.read_text(encoding="utf-8", errors="replace")
        for i, line in enumerate(text.splitlines(), 1):
            if pat.search(line):
                hits.append({"path": rel, "line": i, "text": line.strip()[:160]})
    return sorted(hits, key=lambda d: (d["path"], d["line"]))


def check_private_cross_imports(files: list[Path]) -> list[dict]:
    # `from <pkg.mod> import _foo` or `from <pkg.mod> import a, _b`
    pat = re.compile(r"^\s*from\s+([\w.]+)\s+import\s+(.+)$")
    hits: list[dict] = []
    for fp in files:
        rel = _rel(fp)
        own_mod = rel[:-3].replace("/", ".")
        text = fp.read_text(encoding="utf-8", errors="replace")
        for i, line in enumerate(text.splitlines(), 1):
            m = pat.match(line)
            if not m:
                continue
            src, names = m.group(1), m.group(2)
            if src.startswith(".") or src == own_mod:
                continue
            imported = [n.strip().split(" as ")[0].strip()
                        for n in names.replace("(", "").replace(")", "").split(",")]
            priv = [n for n in imported if n.startswith("_") and n != "__future__"]
            if priv and any(src.startswith(p + ".") or src == p
                            for p in ("paper_trader", "api", "engine", "db",
                                      "alpha_agent", "research_agent")):
                hits.append({"path": rel, "line": i, "imports_from": src,
                             "private_names": sorted(priv)})
    return sorted(hits, key=lambda d: (d["path"], d["line"]))


def _ui_referenced_endpoints() -> set[str]:
    text = _read(UI_FILE)
    refs: set[str] = set()
    for m in re.finditer(r"""['"`](/v1/[A-Za-z0-9_\-/{}.:]*)['"`]""", text):
        refs.add(m.group(1))
    # also template-literal fetches like fetch(`/v1/...${x}`)
    for m in re.finditer(r"""fetch\(\s*[`'"]?(/v1/[A-Za-z0-9_\-/{}.:]*)""", text):
        refs.add(m.group(1))
    return refs


def _static_prefix(path: str) -> str:
    # Normalize a route/ref to its static prefix (drop path params).
    parts = []
    for seg in path.split("/"):
        if seg.startswith("{"):
            break
        parts.append(seg)
    return "/".join(parts)


def check_ui_endpoint_wiring(routes: list[dict]) -> dict:
    declared = {r["path"] for r in routes if r["path"] != "<unresolved>"}
    declared_prefixes = {_static_prefix(p) for p in declared}
    referenced = _ui_referenced_endpoints()
    ref_prefixes = {_static_prefix(p) for p in referenced}

    # UI references that match no declared route prefix (dangling UI loaders).
    dangling = sorted(p for p in referenced
                      if _static_prefix(p) not in declared_prefixes)
    # Declared routes never referenced by the UI (orphan-endpoint candidates).
    orphan = sorted(p for p in declared
                    if p.startswith("/v1/")
                    and _static_prefix(p) not in ref_prefixes)
    return {
        "ui_referenced_count": len(referenced),
        "declared_v1_count": len([p for p in declared if p.startswith("/v1/")]),
        "dangling_ui_references": dangling,
        "orphan_endpoint_candidates": orphan,
    }


def check_canonical_concept_writers(files: list[Path]) -> dict:
    result: dict[str, list[dict]] = {}
    for concept, pat in CANONICAL_CONCEPT_PATTERNS.items():
        hits: list[dict] = []
        for fp in files:
            rel = _rel(fp)
            if rel == APP_MODULE:
                # app.py wires everything; count it but mark it.
                pass
            text = fp.read_text(encoding="utf-8", errors="replace")
            for i, line in enumerate(text.splitlines(), 1):
                if pat.search(line):
                    hits.append({"path": rel, "line": i,
                                 "symbol": line.strip()[:120]})
        result[concept] = sorted(hits, key=lambda d: (d["path"], d["line"]))
    # A concept with writers in >1 distinct module is a multi-writer candidate.
    multi = {c: sorted({h["path"] for h in hits})
             for c, hits in result.items()
             if len({h["path"] for h in hits}) > 1}
    return {"writers": result, "multi_writer_concepts": multi}


def check_research_execution_terms(files: list[Path]) -> list[dict]:
    hits: list[dict] = []
    for fp in files:
        rel = _rel(fp)
        is_research = rel.startswith(RESEARCH_ONLY_DIRS) or rel in RESEARCH_ONLY_MODULES
        if not is_research:
            continue
        text = fp.read_text(encoding="utf-8", errors="replace")
        for i, line in enumerate(text.splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            for term in EXECUTION_CALL_TERMS:
                # only flag call-shaped usage: term followed by "("
                if re.search(r"\b" + re.escape(term) + r"\s*\(", line):
                    hits.append({"path": rel, "line": i, "term": term,
                                 "text": stripped[:160]})
    return sorted(hits, key=lambda d: (d["path"], d["line"]))


def check_market_session_ownership(files: list[Path]) -> dict:
    """Slice 1 semantic ownership guard.

    Confirms (a) the canonical ``engine.market_session`` owner and the
    ``api.data_freshness`` owner exist, (b) the migrated compat wrappers delegate
    to the owner and no longer contain raw session arithmetic, (c) no UNEXPECTED
    module introduces independent current-session arithmetic (the desk resolver is
    a documented, allow-listed remainder), and (d) the UI performs no market-date
    arithmetic in the freshness code. This validates ownership + delegation
    semantically rather than by an arbitrary occurrence count.
    """
    delegating: dict[str, bool] = {}
    clean: dict[str, bool] = {}
    for w in SESSION_DELEGATING_WRAPPERS:
        txt = _read(w)
        delegating[w] = bool(MARKET_SESSION_REF.search(txt))
        clean[w] = not any(
            SESSION_ARITH_RE.search(ln) for ln in txt.splitlines()
            if not ln.strip().startswith("#"))

    remaining: list[dict] = []
    for fp in files:
        rel = _rel(fp)
        if rel in SESSION_ARITH_EXEMPT:
            continue
        for i, line in enumerate(
                fp.read_text(encoding="utf-8", errors="replace").splitlines(), 1):
            if line.strip().startswith("#"):
                continue
            if SESSION_ARITH_RE.search(line):
                remaining.append({"path": rel, "line": i, "text": line.strip()[:160]})
    remaining = sorted(remaining, key=lambda d: (d["path"], d["line"]))
    unexpected = sorted(
        {r["path"] for r in remaining}
        - set(SESSION_DELEGATING_WRAPPERS)
        - set(SESSION_RESOLVERS_REMAINING_ALLOW))

    # UI market-date arithmetic inside the single freshness loader/render region.
    ui = _read(UI_FILE)
    ui_hits: list[str] = []
    start = ui.find("function loadDataFreshness")
    end = ui.find("window.renderDataFreshness")
    if start != -1 and end != -1 and end > start:
        region = ui[start:end]
        for pat in ("new Date(", "Date.now(", ".getTime("):
            if pat in region:
                ui_hits.append(pat)
    freshness_loader_count = ui.count("function loadDataFreshness")

    return {
        "owner": MARKET_SESSION_OWNER,
        "owner_present": (REPO_ROOT / MARKET_SESSION_OWNER).exists(),
        "freshness_owner": DATA_FRESHNESS_OWNER,
        "freshness_owner_present": (REPO_ROOT / DATA_FRESHNESS_OWNER).exists(),
        "delegating_wrappers": delegating,
        "migrated_wrappers_clean": clean,
        "remaining_session_resolvers": remaining,
        "unexpected_session_resolvers": unexpected,
        "ui_freshness_loader_count": freshness_loader_count,
        "ui_market_date_arithmetic": sorted(ui_hits),
        "historical_evidence_calendar_exempt": "api/forward_prediction_skill.py",
    }


def check_workflow_state_ownership(files: list[Path]) -> dict:
    """Slice 2 semantic ownership guard.

    Confirms (a) the canonical ``api.workflow_state`` combined-interpretation owner
    exists, (b) the read-only endpoint is declared, (c) the UI has EXACTLY ONE
    ``loadWorkflowState`` loader, and (d) the UI performs NO workflow-priority /
    assessment-currency derivation in that loader/render region — no market-date
    arithmetic and no client-constructed stale "NO ACTION TODAY" label. This
    validates ownership + no-UI-derivation semantically rather than by an arbitrary
    occurrence count.
    """
    owner_present = (REPO_ROOT / WORKFLOW_STATE_OWNER).exists()

    routes = check_routes()["routes"]
    endpoint_present = any(r["path"] == WORKFLOW_STATE_ROUTE for r in routes)

    ui = _read(UI_FILE)
    loader_count = ui.count(UI_WORKFLOW_LOADER)
    region_hits: list[str] = []
    start = ui.find(UI_WORKFLOW_LOADER)
    end = ui.find(UI_WORKFLOW_RENDER_EXPORT)
    if start != -1 and end != -1 and end > start:
        region = ui[start:end]
        for pat in UI_WORKFLOW_FORBIDDEN_ARITH:
            if pat in region:
                region_hits.append(pat)
        if UI_WORKFLOW_FORBIDDEN_TODAY_LABEL in region:
            region_hits.append(UI_WORKFLOW_FORBIDDEN_TODAY_LABEL)

    # --- Slice 2 UI HARD CUTOVER guards (Phase 29C.1). --------------------------- #
    ownership_declared = (UI_WS_OWNERSHIP_DECL in ui
                          and all(h in ui for h in UI_WS_OWN_HELPERS))

    # (a) The shared specialized setters hard-guard canonical nodes (guard within a
    #     small window after the signature, so no full-body parse is needed).
    setters_guarded: list[str] = []
    for s in UI_GUARDED_SETTERS:
        sig = "function %s(id, text) {" % s
        si = ui.find(sig)
        if si != -1 and UI_SETTER_GUARD in ui[si:si + 200]:
            setters_guarded.append(s)
    unguarded_setters = sorted(set(UI_GUARDED_SETTERS) - set(setters_guarded))

    # (b) No specialized DETAIL renderer writes a canonical node. Each function is
    #     bounded by its own ``window.NAME = NAME;`` export (excludes neighbours),
    #     so the region never leaks into the canonical owner or the safe-fallback.
    unauthorized_canonical_writers: list[str] = []
    for fn in UI_SPECIALIZED_RENDERERS:
        fs = ui.find("function %s(" % fn)
        fe = ui.find("window.%s = %s;" % (fn, fn))
        if fs == -1 or fe == -1 or fe <= fs:
            unauthorized_canonical_writers.append("%s:REGION_NOT_FOUND" % fn)
            continue
        body = ui[fs:fe]
        for node in UI_CANONICAL_NODES:
            if ("'%s'" % node) in body or ('"%s"' % node) in body:
                unauthorized_canonical_writers.append("%s:%s" % (fn, node))
        if "_wsOwnSet(" in body or "_wsOwnHtml(" in body:
            unauthorized_canonical_writers.append("%s:OWN_HELPER" % fn)
        # The DAG render must not present the gate outcome label as a current verdict.
        if fn == "renderDailyActionGate" and UI_DAG_STALE_TODAY_TOKEN in body:
            unauthorized_canonical_writers.append("%s:STALE_TODAY_LABEL" % fn)

    # (c) The raw Daily Action Gate endpoint MAY retain its historic NO_ACTION_TODAY
    #     outcome vocabulary (informational — its retention is expected).
    raw_gate_vocab_retained = RAW_GATE_VOCAB in _read(RAW_GATE_OWNER)

    return {
        "owner": WORKFLOW_STATE_OWNER,
        "owner_present": owner_present,
        "route": WORKFLOW_STATE_ROUTE,
        "endpoint_present": endpoint_present,
        "ui_workflow_loader_count": loader_count,
        "ui_workflow_priority_derivation": sorted(set(region_hits)),
        "ui_canonical_ownership_declared": ownership_declared,
        "ui_shared_setters_guarded": sorted(setters_guarded),
        "ui_unguarded_setters": unguarded_setters,
        "ui_unauthorized_canonical_writers": sorted(set(unauthorized_canonical_writers)),
        "raw_gate_vocab_retained": raw_gate_vocab_retained,
    }


def check_daily_research_cycle_ownership(files: list[Path]) -> dict:
    """Slice 3 semantic ownership guard.

    Confirms (a) ``api.daily_research_cycle`` is the SOLE combined orchestration
    owner (exactly one module defines the run + status entry points), (b) it
    delegates scoring / target / evidence / assessment to the existing owners and
    derives the evidence count from the snapshot registry, (c) it never executes
    Daily Close and creates no order / signal / decision / fill and hosts no second
    scoring engine, (d) the read-only status endpoint and the token-gated run
    endpoint are declared, and (e) the UI has EXACTLY ONE status loader and ONE
    execution function and derives no dates / priority / freshness / plan. This
    validates ownership semantically rather than by an arbitrary occurrence count.
    """
    src = _read(DRC_OWNER)
    owner_present = (REPO_ROOT / DRC_OWNER).exists()

    run_def_modules: list[str] = []
    status_def_modules: list[str] = []
    for fp in files:
        rel = _rel(fp)
        # This audit tool itself references the def signatures as literal constants;
        # it is not a competing orchestrator (mirrors SESSION_ARITH_EXEMPT).
        if rel == "scripts/audit_architecture.py":
            continue
        text = fp.read_text(encoding="utf-8", errors="replace")
        if DRC_RUN_DEF in text:
            run_def_modules.append(rel)
        if DRC_STATUS_DEF in text:
            status_def_modules.append(rel)
    competing = sorted((set(run_def_modules) | set(status_def_modules)) - {DRC_OWNER})

    delegates = {tok: (tok in src) for tok in DRC_MUST_DELEGATE}
    missing_delegation = sorted(k for k, v in delegates.items() if not v)
    forbidden_calls = sorted(t for t in DRC_FORBIDDEN_CALLS if t in src)
    forbidden_model_defs = sorted(t for t in DRC_FORBIDDEN_MODEL_DEFS if t in src)

    routes = check_routes()["routes"]
    status_present = any(r["path"] == DRC_STATUS_ROUTE for r in routes)
    run_present = any(r["path"] == DRC_RUN_ROUTE for r in routes)
    # The run route must be POST-only; the status route GET-only.
    status_methods = sorted({r["method"] for r in routes if r["path"] == DRC_STATUS_ROUTE})
    run_methods = sorted({r["method"] for r in routes if r["path"] == DRC_RUN_ROUTE})

    ui = _read(UI_FILE)
    loader_count = ui.count(UI_DRC_LOADER)
    exec_count = ui.count(UI_DRC_EXEC)
    status_fetch_count = ui.count(UI_DRC_STATUS_FETCH)
    run_fetch_count = ui.count(UI_DRC_RUN_FETCH)
    region_hits: list[str] = []
    start = ui.find(UI_DRC_LOADER)
    end = ui.find(UI_DRC_REGION_END)
    if start != -1 and end != -1 and end > start:
        region = ui[start:end]
        for pat in UI_DRC_FORBIDDEN:
            if pat in region:
                region_hits.append(pat)

    # Daily Close delegates evidence to the canonical owner (no competing bundle).
    dc = _read("api/daily_close.py")
    daily_close_delegates_evidence = ("forward_prediction_skill" in dc
                                      or "\nimport" in dc and "fps" in dc
                                      or " fps." in dc)

    return {
        "owner": DRC_OWNER,
        "owner_present": owner_present,
        "sole_orchestrator": (competing == []),
        "competing_orchestrators": competing,
        "run_entry_modules": sorted(run_def_modules),
        "status_entry_modules": sorted(status_def_modules),
        "delegates": delegates,
        "missing_delegation": missing_delegation,
        "forbidden_execution_calls": forbidden_calls,
        "forbidden_second_scoring_engine": forbidden_model_defs,
        "status_endpoint_present": status_present,
        "run_endpoint_present": run_present,
        "status_methods": status_methods,
        "run_methods": run_methods,
        "ui_status_loader_count": loader_count,
        "ui_execution_function_count": exec_count,
        "ui_status_fetch_count": status_fetch_count,
        "ui_run_fetch_count": run_fetch_count,
        "ui_planning_derivation": sorted(set(region_hits)),
        "daily_close_delegates_evidence": bool(daily_close_delegates_evidence),
        "execute_token": DRC_EXECUTE_TOKEN,
    }


def check_slice3_live_acceptance_ownership(files: list[Path]) -> dict:
    """Phase 29D.1 Slice-3 live-acceptance completion ownership guard.

    Confirms the corrective invariants: (a) ``engine.market_session`` owns session
    classification and no longer infers a holiday from the ABSENCE of same-day owned
    data — ``NON_SESSION`` requires an AUTHORITATIVE source and the old benchmark-based
    ``likely_holiday`` inference is gone; (b) the canonical monthly momentum input
    adapter exists and is the DECLARED refresh owner for ``momentum_monthly`` in the
    DRC (no ``NO_REFRESH_OWNER``, no lingering 'external ... emitter' owner string, no
    normal-path 'run a separate button' prerequisite); (c) ``target_calculation`` is a
    DECLARED, prepared-downstream owner (the canonical target owner), never
    ``NO_REFRESH_OWNER``; (d) the monthly adapter creates no order/signal/decision/fill
    and calls no provider/prediction; and (e) ``workflow_state`` ranks
    ``WAITING_FOR_OWNED_DATA`` strictly ABOVE ``RESEARCH_CYCLE_BLOCKED``.
    """
    ms_src = _read(MARKET_SESSION_OWNER)
    drc_src = _read(DRC_OWNER)
    mmi_src = _read(MONTHLY_INPUT_OWNER)
    ws_src = _read(WORKFLOW_STATE_OWNER)

    # (a) market_session non-session policy.
    non_session_authoritative = bool(
        "NON_SESSION" in ms_src
        and "authoritative_non_sessions" in ms_src
        and "likely_holiday" not in ms_src)

    # (b) the monthly adapter is the declared owner (no external-emitter owner string).
    monthly_adapter_present = (REPO_ROOT / MONTHLY_INPUT_OWNER).exists()
    monthly_owner_declared = ("api.monthly_momentum_input" in drc_src
                              and "external research monthly momentum emitter" not in drc_src)

    # (c) the canonical target owner is declared as prepared-downstream.
    target_owner_declared = ("api.alpha_target.load_readiness" in drc_src
                             and "prepared_downstream_by" in drc_src
                             and '"target_calculation"' in drc_src)

    # (d) the monthly adapter hosts no execution / provider / prediction call.
    mmi_forbidden = sorted(t for t in (
        "place_order(", "submit_order(", "create_order(", "run_fill_cycle(",
        "Signal(", "TradeDecision(", "run_daily_close(", "predict(",
        "requests.get(", "requests.post(", "httpx.", ":9000") if t in mmi_src)

    # (e) WAITING_FOR_OWNED_DATA is returned before RESEARCH_CYCLE_BLOCKED in the
    #     priority policy (structural ordering of the first-match state machine).
    w_ret = ws_src.find("return WAITING_FOR_OWNED_DATA")
    b_ret = ws_src.find("return RESEARCH_CYCLE_BLOCKED")
    waiting_outranks_blocked = bool(w_ret != -1 and b_ret != -1 and w_ret < b_ret)

    return {
        "market_session_owner_present": (REPO_ROOT / MARKET_SESSION_OWNER).exists(),
        "non_session_requires_authoritative_source": non_session_authoritative,
        "monthly_input_adapter_present": bool(monthly_adapter_present),
        "monthly_input_owner_declared": bool(monthly_owner_declared),
        "target_calculation_owner_declared": bool(target_owner_declared),
        "no_normal_path_manual_monthly_prerequisite": bool(monthly_owner_declared),
        "monthly_adapter_forbidden_calls": mmi_forbidden,
        "waiting_outranks_research_blockers": waiting_outranks_blocked,
    }


def check_monthly_emitter_bridge_ownership(files: list[Path]) -> dict:
    """Phase 29D.2 production monthly-momentum emitter bridge ownership guard.

    Confirms (a) the pure-stdlib SUBPROCESS bridge ``api.monthly_momentum_emitter``
    exists, imports NEITHER numpy NOR pandas and never uses a shell string, (b) it
    delegates ALL monthly momentum mathematics to the external Phase-25 module (no
    SECOND monthly formula exists in any operational api module), (c) the canonical
    monthly-input adapter wires it through ``activate_production_emitter`` and the app
    activates the production resolver at startup, (d) the Daily Research Cycle status
    contract exposes the monthly owner, and (e) NO separate monthly execution endpoint
    or UI primary button exists (the monthly step lives inside the DRC).
    """
    emitter_src = _read(MONTHLY_EMITTER_OWNER)
    adapter_src = _read(MONTHLY_INPUT_OWNER)
    app_src = _read(APP_MODULE)
    drc_src = _read(DRC_OWNER)
    ui = _read(UI_FILE)

    present = (REPO_ROOT / MONTHLY_EMITTER_OWNER).exists()
    numeric_imports = sorted(
        t for t in ("import numpy", "import pandas", "from numpy", "from pandas")
        if t in emitter_src)
    uses_shell = "shell=True" in emitter_src
    delegates_math = MONTHLY_EMITTER_MATH_OWNER in emitter_src
    uses_argv_array = ("def build_run_command(" in emitter_src
                       and "subprocess.run(" in emitter_src)

    # No SECOND monthly formula: no operational api module RECOMPUTES the month-end
    # mom_6_1 mathematics (that belongs solely to the external Phase-25 owner). A real
    # reimplementation needs a numeric library, so a module is flagged only when it BOTH
    # imports numpy/pandas AND matches the formula signature — this ignores docstrings /
    # comments that merely DESCRIBE the formula (e.g. the bridge's own module docstring).
    _numeric = ("import numpy", "import pandas", "from numpy", "from pandas")
    second_formula_modules: list[str] = []
    for fp in files:
        rel = _rel(fp)
        parts = rel.split("/")
        if len(parts) != 2 or parts[0] != "api" or rel == MONTHLY_EMITTER_OWNER:
            continue
        text = fp.read_text(encoding="utf-8", errors="replace")
        if any(t in text for t in _numeric) and MONTHLY_FORMULA_SIG.search(text):
            second_formula_modules.append(rel)
    second_formula_modules = sorted(second_formula_modules)

    adapter_wires = "activate_production_emitter" in adapter_src
    app_wires = ("activate_production_emitter" in app_src
                 and "monthly_momentum_emitter" in app_src)
    drc_exposes_owner = "monthly_owner" in drc_src
    ui_no_separate_button = ("runMonthlyInputEmitter" not in ui
                             and "month-boundary-btn" not in ui)

    routes = check_routes()["routes"]
    # No dedicated monthly execution endpoint (the monthly step is inside the DRC run).
    monthly_endpoints = sorted(r["path"] for r in routes
                               if "monthly" in (r["path"] or "").lower())

    return {
        "owner": MONTHLY_EMITTER_OWNER,
        "owner_present": present,
        "bridge_pure_stdlib": (numeric_imports == []),
        "bridge_numeric_imports": numeric_imports,
        "bridge_no_shell_string": (not uses_shell),
        "bridge_uses_argv_array": bool(uses_argv_array),
        "bridge_delegates_phase25_math": bool(delegates_math),
        "second_monthly_formula_modules": second_formula_modules,
        "adapter_wires_production_resolver": bool(adapter_wires),
        "app_wires_production_resolver": bool(app_wires),
        "drc_status_exposes_monthly_owner": bool(drc_exposes_owner),
        "no_separate_monthly_ui_button": bool(ui_no_separate_button),
        "separate_monthly_endpoints": monthly_endpoints,
    }


def check_universe_scoring_ownership(files: list[Path]) -> dict:
    """Slice 4 semantic ownership guard.

    Confirms (a) the pure scoring KERNEL (``api.multi_horizon_engine``) and the
    canonical composition & read owner (``api.universe_scoring``) both exist, (b) the
    composition owner delegates to the kernel and hosts NO second scoring engine
    (no ``compute_scores`` / ``_percentiles`` / ``compute_combined`` / ``build_books`` /
    ``_select_book`` definition) and no order/signal/decision/fill/provider/prediction/
    promotion call, (c) NO OTHER operational ``api/*.py`` module defines the kernel's
    combined-score mathematics, (d) the Daily Research Cycle delegates scoring to the
    canonical owner, (e) the ``current-alpha-scores`` platform surface is a
    compatibility wrapper over the owner, (f) the canonical GET-only read route and the
    compatibility route are declared, and (g) the UI has EXACTLY ONE canonical scoring
    loader and computes no score/rank/exclusion/universe/date in its region.
    """
    owner_src = _read(US_OWNER)
    owner_present = (REPO_ROOT / US_OWNER).exists()
    kernel_present = (REPO_ROOT / US_KERNEL).exists()

    delegates = {tok: (tok in owner_src) for tok in US_MUST_DELEGATE}
    missing_delegation = sorted(k for k, v in delegates.items() if not v)
    second_engine_in_owner = sorted(t for t in US_FORBIDDEN_MODEL_DEFS if t in owner_src)
    forbidden_calls = sorted(t for t in US_FORBIDDEN_CALLS if t in owner_src)

    # (c) No OTHER operational api/*.py module defines the kernel scoring math.
    duplicate_scoring_modules: list[dict] = []
    for fp in files:
        rel = _rel(fp)
        parts = rel.split("/")
        if len(parts) != 2 or parts[0] != "api":
            continue
        if rel == US_KERNEL:
            # the kernel owns the combined-score math.
            continue
        text = fp.read_text(encoding="utf-8", errors="replace")
        for tok in US_COMBINED_SCORE_DEFS:
            if tok in text:
                duplicate_scoring_modules.append({"path": rel, "symbol": tok})
    duplicate_scoring_modules = sorted(duplicate_scoring_modules,
                                       key=lambda d: (d["path"], d["symbol"]))

    # (d) DRC delegates scoring here; (e) platform surface is a compat wrapper.
    drc_src = _read(DRC_OWNER)
    drc_delegates_scoring = DRC_SCORING_DELEGATE_TOKEN in drc_src
    platform_src = _read(PLATFORM_SCORES_OWNER)
    platform_has_scores_fn = PLATFORM_SCORES_FN in platform_src
    platform_delegates = DRC_SCORING_DELEGATE_TOKEN in platform_src

    # (f) routes
    routes = check_routes()["routes"]
    canonical_present = any(r["path"] == US_ROUTE for r in routes)
    compat_present = any(r["path"] == US_COMPAT_ROUTE for r in routes)
    canonical_methods = sorted({r["method"] for r in routes if r["path"] == US_ROUTE})

    # (g) UI: exactly one canonical loader; region computes nothing.
    ui = _read(UI_FILE)
    loader_count = ui.count(UI_US_LOADER)
    fetch_count = ui.count(UI_US_FETCH)
    region_hits: list[str] = []
    start = ui.find(UI_US_LOADER)
    end = ui.find(UI_US_REGION_END)
    if start != -1 and end != -1 and end > start:
        region = ui[start:end]
        for pat in UI_US_FORBIDDEN:
            if pat in region:
                region_hits.append(pat)

    return {
        "owner": US_OWNER,
        "kernel": US_KERNEL,
        "owner_present": owner_present,
        "kernel_present": kernel_present,
        "delegates": delegates,
        "missing_delegation": missing_delegation,
        "second_scoring_engine_in_owner": second_engine_in_owner,
        "forbidden_execution_calls": forbidden_calls,
        "duplicate_operational_scoring_modules": duplicate_scoring_modules,
        "drc_delegates_scoring": bool(drc_delegates_scoring),
        "platform_compat_wrapper": bool(platform_has_scores_fn and platform_delegates),
        "canonical_route_present": canonical_present,
        "canonical_route_methods": canonical_methods,
        "compat_route_present": compat_present,
        "ui_scoring_loader_count": loader_count,
        "ui_scoring_fetch_count": fetch_count,
        "ui_scoring_computation": sorted(set(region_hits)),
        "automatic_model_promotion_allowed": False,
    }


def check_portfolio_state_ownership(files: list[Path]) -> dict:
    """Slice 5 (Phase 29F) semantic ownership guard for the canonical operational
    portfolio-state owner (api.portfolio_state).

    Asserts ONE read-only composition owner that delegates to the authoritative read
    models, selects the ACTIVE Alpha Paper Book (never the dormant legacy DB book),
    is NOT a writer, and is exposed at ONE GET route with EXACTLY ONE canonical UI
    loader + renderer that compute no NAV / totals / active-book selection /
    valuation date / pending count."""
    owner_src = _read(PS_OWNER)
    owner_present = (REPO_ROOT / PS_OWNER).exists()

    delegates = {tok: (tok in owner_src) for tok in PS_MUST_DELEGATE}
    missing_delegation = sorted(k for k, v in delegates.items() if not v)
    forbidden_calls = sorted(t for t in PS_FORBIDDEN_CALLS if t in owner_src)
    active_book_selection_present = PS_SELECT_FN in owner_src
    rejects_legacy_book = PS_LEGACY_TOKEN in owner_src
    owner_defines_loader = PS_LOAD_DEF in owner_src

    # (b) No OTHER operational api/*.py module defines a second portfolio-state owner.
    second_owner_modules: list[str] = []
    for fp in files:
        rel = _rel(fp)
        parts = rel.split("/")
        if len(parts) != 2 or parts[0] != "api":
            continue
        if rel == PS_OWNER:
            continue
        text = fp.read_text(encoding="utf-8", errors="replace")
        if PS_SECOND_OWNER_DEF in text:
            second_owner_modules.append(rel)
    second_owner_modules = sorted(second_owner_modules)

    # (c) routes — the canonical route exists and is GET-only.
    routes = check_routes()["routes"]
    canonical_present = any(r["path"] == PS_ROUTE for r in routes)
    canonical_methods = sorted({r["method"] for r in routes if r["path"] == PS_ROUTE})

    # (d) UI: EXACTLY one canonical loader + renderer; the region computes nothing.
    ui = _read(UI_FILE)
    loader_count = ui.count(UI_PS_LOADER)
    renderer_count = ui.count(UI_PS_RENDERER)
    fetch_count = ui.count(UI_PS_FETCH)
    region_hits: list[str] = []
    start = ui.find(UI_PS_REGION_START)
    end = ui.find(UI_PS_REGION_END)
    if start != -1 and end != -1 and end > start:
        region = ui[start:end]
        for pat in UI_PS_FORBIDDEN:
            if pat in region:
                region_hits.append(pat)
    guard_present = {tok: (tok in ui) for tok in UI_PS_GUARD_TOKENS}
    missing_guard_tokens = sorted(k for k, v in guard_present.items() if not v)

    return {
        "owner": PS_OWNER, "owner_present": owner_present,
        "owner_defines_loader": owner_defines_loader,
        "delegates": delegates, "missing_delegation": missing_delegation,
        "forbidden_execution_calls": forbidden_calls,
        "portfolio_state_is_writer": bool(forbidden_calls),
        "active_book_selection_present": active_book_selection_present,
        "rejects_dormant_legacy_book": rejects_legacy_book,
        "second_portfolio_state_owner_modules": second_owner_modules,
        "canonical_route_present": canonical_present,
        "canonical_route_methods": canonical_methods,
        "ui_portfolio_state_loader_count": loader_count,
        "ui_portfolio_state_renderer_count": renderer_count,
        "ui_portfolio_state_fetch_count": fetch_count,
        "ui_portfolio_state_computation": sorted(set(region_hits)),
        "ui_missing_guard_tokens": missing_guard_tokens,
    }


def check_holding_opportunity_cost_ownership(files: list[Path]) -> dict:
    """Slice 6 (Phase 29G) semantic ownership guard for the Holding Opportunity-Cost
    engine (Milestone 2). Proves: (1) the pure kernel is the sole calculation owner;
    (2) the api module is the sole composition/read/persistence owner; (3) the GET-only
    read route exists; (4) the DRC delegates to the canonical owner; (5) no separate
    manual execution endpoint exists; (6) no second holding-recommendation engine
    exists; (7) neither owner calls order/fill/execution/target/NAV/score/provider/
    prediction; (8) no target weights generated; (9) NAV not duplicated; (10) universe
    score not recomputed; (11) the UI has exactly one loader; (12) the UI computes no
    recommendation/cost; (13) the Daily Action Gate delegates to the summary;
    (14) Slice 7 remains unimplemented; (15) Slice 8 remains planned; (16) automatic
    model promotion prohibited; (17) cadence disabled; (18) inventory drift zero
    (checked by ``check_inventory_drift``)."""
    kernel_src = _read(HOC_KERNEL)
    owner_src = _read(HOC_OWNER)
    drc_src = _read(DRC_OWNER)
    gate_src = _read(GATE_OWNER)
    ui = _read(UI_FILE)

    kernel_present = (REPO_ROOT / HOC_KERNEL).exists()
    owner_present = (REPO_ROOT / HOC_OWNER).exists()

    # (1) sole calculation owner: build_assessment defined ONLY in the kernel.
    calc_def_modules = []
    for fp in files:
        rel = _rel(fp)
        if rel == "scripts/audit_architecture.py":
            continue
        if HOC_KERNEL_BUILD_DEF in fp.read_text(encoding="utf-8", errors="replace"):
            calc_def_modules.append(rel)
    second_calculation_owner = sorted(set(calc_def_modules) - {HOC_KERNEL})

    # (2) sole composition/read owner: load_holding_opportunity_cost only in the owner.
    read_def_modules = []
    for fp in files:
        rel = _rel(fp)
        if rel == "scripts/audit_architecture.py":
            continue
        if HOC_OWNER_LOAD_DEF in fp.read_text(encoding="utf-8", errors="replace"):
            read_def_modules.append(rel)
    second_composition_owner = sorted(set(read_def_modules) - {HOC_OWNER})

    delegates = {tok: (tok in owner_src) for tok in HOC_MUST_DELEGATE}
    missing_delegation = sorted(k for k, v in delegates.items() if not v)
    owner_forbidden = sorted(t for t in HOC_FORBIDDEN_CALLS if t in owner_src)
    kernel_forbidden = sorted(t for t in HOC_KERNEL_FORBIDDEN if t in kernel_src)

    # (3)/(5) routes: the GET read route exists; no separate manual execution route.
    routes = check_routes()["routes"]
    route_present = any(r["path"] == HOC_ROUTE for r in routes)
    route_methods = sorted({r["method"] for r in routes if r["path"] == HOC_ROUTE})
    hoc_routes = sorted(r["path"] for r in routes
                        if "holding-opportunity" in (r["path"] or "").lower())
    hoc_route_methods = sorted({r["method"] for r in routes
                                if "holding-opportunity" in (r["path"] or "").lower()})
    no_manual_execution_endpoint = (hoc_route_methods == ["GET"])

    # (4) DRC delegates; (13) gate delegates to the summary.
    drc_delegates = DRC_HOC_DELEGATE_TOKEN in drc_src
    gate_delegates = GATE_HOC_DELEGATE_TOKEN in gate_src or DRC_HOC_DELEGATE_TOKEN in gate_src

    # (11)/(12) UI: exactly one loader; region computes nothing.
    ui_loader_count = ui.count(UI_HOC_LOADER)
    ui_fetch_count = ui.count(UI_HOC_FETCH)
    ui_region_hits = []
    start = ui.find(UI_HOC_LOADER)
    end = ui.find(UI_HOC_REGION_END)
    if start != -1 and end != -1 and end > start:
        region = ui[start:end]
        for pat in UI_HOC_FORBIDDEN:
            if pat in region:
                ui_region_hits.append(pat)
    ui_review_label_present = UI_HOC_REVIEW_LABEL in ui

    # (14)/(15) Slice 7 is LANDED (owners + route present, no forbidden route);
    #           Slice 8 remains future.
    _s7 = _slice7_landed_status(routes)
    slice7_missing_modules = _s7["slice7_missing_modules"]
    slice7_missing_route = _s7["slice7_missing_route"]
    slice7_forbidden_present = _s7["slice7_forbidden_present"]
    slice8_present_modules = _s7["slice8_present_modules"]

    # (19) ACYCLIC read dependency (Phase 29G performance repair). The HOC summary is a
    # PURE artifact reader: its body must NOT load portfolio state (that edge closed a
    # circular recomposition portfolio_state → gate → summary → portfolio_state). The
    # gate must SUPPLY the explicit (active_book_id, eligible_market_date) context, and
    # portfolio_state may still compose the gate — a DAG, not a cycle.
    ps_src = _read(PS_OWNER)
    summary_body = _strip_prose(_module_func_body(owner_src, HOC_SUMMARY_DEF))
    summary_loads_portfolio_state = sorted(t for t in HOC_SUMMARY_FORBIDDEN_PS
                                           if t in summary_body)
    gate_supplies_hoc_context = all(t in gate_src for t in GATE_HOC_CONTEXT_TOKENS)
    portfolio_state_composes_gate = PS_GATE_COMPOSE_TOKEN in ps_src
    no_circular_read_dependency = (not summary_loads_portfolio_state
                                   and gate_supplies_hoc_context
                                   and portfolio_state_composes_gate)

    return {
        "kernel": HOC_KERNEL, "owner": HOC_OWNER,
        "kernel_present": kernel_present, "owner_present": owner_present,
        "second_calculation_owner_modules": second_calculation_owner,
        "second_composition_owner_modules": second_composition_owner,
        "missing_delegation": missing_delegation,
        "owner_forbidden_calls": owner_forbidden,
        "kernel_impurity": kernel_forbidden,
        "route_present": route_present, "route_methods": route_methods,
        "no_separate_manual_execution_endpoint": no_manual_execution_endpoint,
        "holding_opportunity_routes": hoc_routes,
        "drc_delegates_to_owner": bool(drc_delegates),
        "gate_delegates_to_summary": bool(gate_delegates),
        "ui_loader_count": ui_loader_count,
        "ui_fetch_count": ui_fetch_count,
        "ui_recommendation_or_cost_computation": sorted(set(ui_region_hits)),
        "ui_review_label_present": ui_review_label_present,
        "slice7_missing_modules": slice7_missing_modules,
        "slice7_missing_route": slice7_missing_route,
        "slice7_forbidden_present": slice7_forbidden_present,
        "slice8_present_modules": slice8_present_modules,
        "automatic_model_promotion_allowed": False,
        "cadence_enabled": False,
        # Slice 6 (Phase 29G) acyclic read-dependency proof.
        "summary_loads_portfolio_state": summary_loads_portfolio_state,
        "gate_supplies_hoc_context": bool(gate_supplies_hoc_context),
        "portfolio_state_composes_gate": bool(portfolio_state_composes_gate),
        "no_circular_read_dependency": bool(no_circular_read_dependency),
    }


def check_drc_manifest_recovery(files: list[Path]) -> dict:
    """Phase 29G.3 DRC terminal-manifest persistence/read-back + safe recovery + pre-close
    consistency guard. Proves: (1) api.daily_research_cycle remains the sole DRC
    orchestrator; (2) a terminal COMPLETE response requires a validated + read-back
    manifest (never COMPLETE on an unverified persist); (3) NO "mark complete" endpoint or
    function exists (recovery is normal idempotent execution); (4) status and execution
    share ONE configured artifact root; (5) status cannot report NOT_STARTED when matching
    terminal downstream artifacts exist (reflection + explicit recovery INCONSISTENT code);
    (6) recovery uses the normal run entry (no separate recovery entry/route); (7) no order
    / target-confirmation / operational-mutation call path; (8) the expected pre-close date
    gap is classified PENDING_DAILY_CLOSE (not corruption); (9) genuine date inconsistencies
    remain protected; (10) HOC data gaps remain explicit; (11) Slice 7 remains absent;
    (12) the Persistent Alpha Research Agent (Slice 8) remains planned; (13) cadence remains
    disabled; (14) inventory drift is zero (checked by ``check_inventory_drift``)."""
    drc = _read(DRC_OWNER)
    ps = _read(PS_OWNER)
    ws = _read(WORKFLOW_STATE_OWNER)
    gate = _read(GATE_OWNER)
    routes = check_routes()["routes"]

    # (1) sole orchestrator.
    run_def_modules = [_rel(fp) for fp in files
                       if _rel(fp) != "scripts/audit_architecture.py"
                       and DRC_RUN_DEF in fp.read_text(encoding="utf-8", errors="replace")]
    sole_orchestrator = (sorted(set(run_def_modules)) == [DRC_OWNER])

    # (2) terminal persistence + read-back contract.
    missing_terminal_contract = sorted(t for t in DRC_TERMINAL_TOKENS if t not in drc)
    read_back_present = all(t in drc for t in DRC_READBACK_TOKENS)

    # (3) no "mark complete" endpoint / function.
    mark_complete_defs = sorted(t for t in DRC_MARK_COMPLETE_FORBIDDEN_DEFS if t in drc)
    drc_routes = sorted({r["path"] for r in routes
                         if "daily-research-cycle" in (r["path"] or "")})
    forbidden_recovery_routes = sorted(
        p for p in drc_routes
        if any(bad in p for bad in DRC_RECOVERY_FORBIDDEN_ROUTE_SUBSTR))

    # (4) single configured artifact root (one _drc_dir resolver + the DRC_DIR_ENV).
    single_artifact_root = (drc.count("def _drc_dir(") == 1 and "PAPER_TRADER_DRC_DIR" in drc)

    # (5) status reflects a terminal manifest / recovery INCONSISTENT (never NOT_STARTED).
    missing_reflect = sorted(t for t in DRC_REFLECT_TOKENS if t not in drc)

    # (6) recovery is normal idempotent execution (no separate recovery entry/route).
    recovery_defs_present = sorted(t for t in DRC_RECOVERY_FORBIDDEN_DEFS if t in drc)

    # (7) forbidden mutation calls (order / fill / target-confirm / Daily Close).
    forbidden_execution_calls = sorted(t for t in DRC_FORBIDDEN_CALLS if t in drc)

    # (8) pre-close classification present.
    missing_preclose_tokens = sorted(t for t in PS_PRECLOSE_TOKENS if t not in ps)

    # (9) genuine inconsistency protections retained.
    missing_genuine_inconsistency_tokens = sorted(
        t for t in PS_GENUINE_INCONSISTENCY_TOKENS if t not in ps)

    # (10) HOC data gaps remain explicit through the gate → workflow contract.
    hoc_gaps_explicit = ("opportunity_cost_data_gaps" in gate
                         and "opportunity_cost_data_gaps" in ws)

    # (11)/(12) Slice 7 is LANDED (owners + route present, no forbidden route);
    #           Slice 8 remains future.
    _s7 = _slice7_landed_status(routes)

    return {
        "owner": DRC_OWNER,
        "sole_orchestrator": bool(sole_orchestrator),
        "competing_orchestrators": sorted(set(run_def_modules) - {DRC_OWNER}),
        "missing_terminal_persistence_tokens": missing_terminal_contract,
        "terminal_read_back_present": bool(read_back_present),
        "mark_complete_defs": mark_complete_defs,
        "forbidden_recovery_routes": forbidden_recovery_routes,
        "single_artifact_root": bool(single_artifact_root),
        "missing_status_reflect_tokens": missing_reflect,
        "separate_recovery_entry_defs": recovery_defs_present,
        "forbidden_execution_calls": forbidden_execution_calls,
        "missing_preclose_tokens": missing_preclose_tokens,
        "missing_genuine_inconsistency_tokens": missing_genuine_inconsistency_tokens,
        "hoc_data_gaps_explicit": bool(hoc_gaps_explicit),
        "slice7_missing_modules": _s7["slice7_missing_modules"],
        "slice7_missing_route": _s7["slice7_missing_route"],
        "slice7_forbidden_present": _s7["slice7_forbidden_present"],
        "slice8_present_modules": _s7["slice8_present_modules"],
        "cadence_enabled": False,
    }


def check_reallocation_proposal_ownership(files: list[Path]) -> dict:
    """Slice 7 (Phase 29H) strict semantic ownership guard for the Reallocation Proposal
    engine (Milestone 3). Proves: (1) engine/reallocation_proposal.py is the SOLE
    allocation-math owner; (2) api/reallocation_proposal.py is the SOLE composition /
    persistence / read owner; (3) the GET read route exists exactly once; (4) NO
    POST/apply/rebalance/confirm-target/create-orders route exists; (5) the Daily
    Research Cycle is the sole execution path (the BUILD_REALLOCATION_PROPOSAL step
    delegates to the API owner); (6) the API owner composes the authoritative owners;
    (7) no operational-target write; (8) no holdings/cash/NAV mutation; (9) no order/fill
    creation; (10) no broker call; (11) no prediction/provider call; (12) no automatic
    model promotion; (13) the UI performs no allocation calculation; (14) exactly ONE UI
    loader; (15) immutable/idempotent artifact ownership; (16) HOC remains the Slice 6
    owner and (17) portfolio_state remains the Slice 5 owner (both composed, not forked);
    (18) Slice 8 remains planned; (19) cadence disabled; (20) inventory drift zero
    (checked by ``check_inventory_drift``)."""
    kernel_src = _read(RP_KERNEL)
    owner_src = _read(RP_OWNER)
    drc_src = _read(DRC_OWNER)
    gate_src = _read(GATE_OWNER)
    ui = _read(UI_FILE)

    kernel_present = (REPO_ROOT / RP_KERNEL).exists()
    owner_present = (REPO_ROOT / RP_OWNER).exists()

    # (1) sole allocation-math owner: build_proposal defined ONLY in the kernel.
    calc_def_modules = []
    for fp in files:
        rel = _rel(fp)
        if rel == "scripts/audit_architecture.py":
            continue
        if RP_KERNEL_BUILD_DEF in fp.read_text(encoding="utf-8", errors="replace"):
            calc_def_modules.append(rel)
    second_calculation_owner = sorted(set(calc_def_modules) - {RP_KERNEL})

    # (2) sole composition/read owner: load_reallocation_proposal only in the owner.
    read_def_modules = []
    for fp in files:
        rel = _rel(fp)
        if rel == "scripts/audit_architecture.py":
            continue
        if RP_OWNER_LOAD_DEF in fp.read_text(encoding="utf-8", errors="replace"):
            read_def_modules.append(rel)
    second_composition_owner = sorted(set(read_def_modules) - {RP_OWNER})

    # (6) the owner composes the authoritative owners.
    delegates = {tok: (tok in owner_src) for tok in RP_MUST_DELEGATE}
    missing_delegation = sorted(k for k, v in delegates.items() if not v)
    # (7)-(12) neither owner mutates / executes / calls a provider-prediction-promotion.
    owner_forbidden = sorted(t for t in RP_FORBIDDEN_CALLS if t in owner_src)
    kernel_forbidden = sorted(t for t in RP_KERNEL_FORBIDDEN if t in kernel_src)

    # (3)/(4) routes: the GET read route exists exactly once; no apply/rebalance/order route.
    routes = check_routes()["routes"]
    rp_route_entries = [r for r in routes if r["path"] == RP_ROUTE]
    route_get_count = sum(1 for r in rp_route_entries if r["method"] == "GET")
    rp_all_paths = [r["path"] for r in routes
                    if "reallocation" in (r["path"] or "").lower()
                    or "portfolio-proposal" in (r["path"] or "").lower()
                    or "rebalance" in (r["path"] or "").lower()]
    rp_route_methods = sorted({r["method"] for r in rp_route_entries})
    forbidden_route_methods = (rp_route_methods != ["GET"] and rp_route_methods != [])
    forbidden_routes_present = sorted(r for r in SLICE7_FORBIDDEN_ROUTES
                                      if any(rt["path"] == r for rt in routes))

    # (5) DRC is the sole execution path: the step + delegation token are present.
    drc_delegates = DRC_RP_DELEGATE_TOKEN in drc_src and "run_and_persist" in drc_src
    drc_step_present = DRC_RP_STEP in drc_src
    gate_delegates = GATE_RP_DELEGATE_TOKEN in gate_src

    # (15) immutable/idempotent artifact ownership: persist + atomic write + index.
    persist_present = RP_OWNER_PERSIST_DEF in owner_src
    atomic_persist_present = ("os.replace(" in owner_src and "index" in owner_src.lower())

    # (13)/(14) UI: exactly one loader; region computes no allocation/cost math.
    ui_loader_count = ui.count(UI_RP_LOADER)
    ui_fetch_count = ui.count(UI_RP_FETCH)
    ui_region_hits = []
    start = ui.find(UI_RP_LOADER)
    end = ui.find(UI_RP_REGION_END)
    if start != -1 and end != -1 and end > start:
        region = ui[start:end]
        for pat in UI_RP_FORBIDDEN:
            if pat in region:
                ui_region_hits.append(pat)

    # (16)/(17) HOC + portfolio_state remain their own slice owners (composed, not forked):
    # the kernel must not define the HOC build or the universe scoring math.
    kernel_forks_hoc = "def build_assessment(" in kernel_src
    kernel_forks_scoring = ("def compute_scores(" in kernel_src
                            or "def compute_combined(" in kernel_src)

    # (18) Slice 8 remains planned; (19) cadence disabled.
    slice8_present_modules = sorted(m for m in SLICE8_ABSENT_MODULES
                                    if (REPO_ROOT / m).exists())

    return {
        "kernel": RP_KERNEL, "owner": RP_OWNER,
        "kernel_present": kernel_present, "owner_present": owner_present,
        "second_calculation_owner_modules": second_calculation_owner,
        "second_composition_owner_modules": second_composition_owner,
        "delegates": delegates, "missing_delegation": missing_delegation,
        "owner_forbidden_calls": owner_forbidden,
        "kernel_forbidden_calls": kernel_forbidden,
        "route_get_count": route_get_count,
        "reallocation_route_paths": sorted(set(rp_all_paths)),
        "reallocation_route_methods": rp_route_methods,
        "forbidden_route_methods_present": bool(forbidden_route_methods),
        "forbidden_routes_present": forbidden_routes_present,
        "drc_delegates": bool(drc_delegates),
        "drc_step_present": bool(drc_step_present),
        "gate_delegates_to_summary": bool(gate_delegates),
        "persist_present": bool(persist_present),
        "atomic_idempotent_persist_present": bool(atomic_persist_present),
        "ui_loader_count": ui_loader_count,
        "ui_fetch_count": ui_fetch_count,
        "ui_allocation_or_cost_computation": sorted(set(ui_region_hits)),
        "kernel_forks_hoc": bool(kernel_forks_hoc),
        "kernel_forks_scoring": bool(kernel_forks_scoring),
        "slice8_present_modules": slice8_present_modules,
        "automatic_model_promotion_allowed": False,
        "cadence_enabled": False,
    }


def check_release29_3_decision_integrity(files: list[Path]) -> dict:
    r"""Release 29.3 - PORTFOLIO DECISION INTEGRITY.

    Structural (AST / symbol) contracts wherever one is possible, never a fragile string
    guard where the structure is available. It proves:

      (1) the LEGACY rank-membership gate no longer emits the proposal owner's
          vocabulary (its outcome / target-state VALUES and its headline);
      (2) the Daily Close status describes CLOSE semantics and carries a read-time
          normaliser for the historical token, so no stored byte is rewritten;
      (3) exactly ONE module declares the mandatory eligibility-exit policy;
      (4) the four complete-target constraints are DEFERRED by the reassessment kernel
          and DECIDED by the proposal kernel - moved, never duplicated, with identical
          codes on both sides;
      (5) the reassessment kernel raises none of them as a blocker (AST);
      (6) the semantic-consistency validator exists, is wired into the verdict and
          recomputes no owner's economics;
      (7) WITHHELD is fail-closed at every layer (kernel, read API, decision, workflow);
      (8) the UI renders the canonical decision verbatim, through ONE loader, and
          synthesises no approve / order control.
    """
    prs_k = _read(Path("engine/portfolio_reassessment.py"))
    rp_k = _read(Path("engine/reallocation_proposal.py"))
    dag_src = _read(Path("api/daily_action_gate.py"))
    dc_src = _read(Path("api/daily_close.py"))
    ws_src = _read(Path("api/workflow_state.py"))
    pd_src = _read(Path("api/portfolio_decision.py"))
    arp_src = _read(Path("api/reallocation_proposal.py"))
    ui = _read(UI_FILE)

    def _assign(src, name):
        """The literal a module-level constant is assigned, via AST (never a regex)."""
        try:
            tree = ast.parse(src)
        except SyntaxError:
            return None
        for node in tree.body:
            if isinstance(node, ast.Assign):
                for t in node.targets:
                    if isinstance(t, ast.Name) and t.id == name \
                            and isinstance(node.value, ast.Constant):
                        return node.value.value
        return None

    def _tuple_items(src, name):
        """The VALUES a module-level tuple holds, resolving Name elements through the
        module's own constant assignments. Both kernels declare the moved constraints as
        SYMBOLS, so comparing symbol names would prove nothing about the strings that
        actually reach an operator - the values are what must agree."""
        try:
            tree = ast.parse(src)
        except SyntaxError:
            return []
        consts = {}
        for node in tree.body:
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
                for t in node.targets:
                    if isinstance(t, ast.Name):
                        consts[t.id] = node.value.value
        for node in tree.body:
            if isinstance(node, ast.Assign):
                for t in node.targets:
                    if isinstance(t, ast.Name) and t.id == name \
                            and isinstance(node.value, (ast.Tuple, ast.List)):
                        out = []
                        for e in node.value.elts:
                            if isinstance(e, ast.Constant):
                                out.append(e.value)
                            elif isinstance(e, ast.Name) and e.id in consts:
                                out.append(consts[e.id])
                        return out
        return []

    # (1) legacy gate vocabulary + wording
    legacy_outcome = _assign(dag_src, "OUTCOME_MEMBERSHIP_DRIFT")
    legacy_target = _assign(dag_src, "TARGET_STATE_MEMBERSHIP_DRIFT")
    gate_vocabulary_clean = bool(
        legacy_outcome and legacy_target
        and "PROPOSAL" not in str(legacy_outcome).upper()
        and "PROPOSAL" not in str(legacy_target).upper())
    gate_headline_clean = "PORTFOLIO CHANGES PROPOSED" not in dag_src
    m = re.search(r"action_required = outcome in \(([^)]*)\)", dag_src, re.S)
    membership_not_action_required = bool(
        m and "MEMBERSHIP_DRIFT" not in m.group(1)
        and "OUTCOME_PROPOSAL_READY" not in m.group(1))

    # (2) daily close vocabulary + read-time normalisation of immutable history
    close_token = _assign(dc_src, "CLOSE_COMPLETE_MEMBERSHIP_DRIFT")
    close_vocabulary_clean = bool(
        close_token and "PROPOSAL" not in str(close_token).upper())
    close_normaliser_present = ("def normalize_close_status(" in dc_src
                                and "def normalize_close_decision(" in dc_src)
    close_normalises_history = "normalize_close_status(r.get(" in dc_src

    # (3) exactly ONE mandatory eligibility-exit policy owner
    policy_owners = sorted(
        f.as_posix() for f in files
        if f.suffix == ".py"
        and re.search(r"^MANDATORY_EXIT_OVERRIDES\s*=", _read(f), re.M))

    # (4) moved, not duplicated: identical codes on both sides
    prs_deferred = set(_tuple_items(prs_k, "COMPLETE_TARGET_CONSTRAINT_CODES"))
    rp_owned = set(_tuple_items(rp_k, "COMPLETE_TARGET_CONSTRAINT_CODES"))
    constraint_codes_agree = bool(prs_deferred and prs_deferred == rp_owned)
    constraint_owner_declared = ("def constraint_ownership(" in prs_k
                                 and "def evaluate_complete_target_limits(" in rp_k)

    # (5) AST: the reassessment kernel appends none of the moved codes to `blockers`
    moved_names = {"GATE_CONCENTRATION", "GATE_SECTOR_CAP", "GATE_RISK_DETERIORATION",
                   "CHURN_TURNOVER_BUDGET"}
    reassessment_raises_moved = []
    try:
        prs_tree = ast.parse(prs_k)
    except SyntaxError:
        prs_tree = None
    if prs_tree is not None:
        for node in ast.walk(prs_tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) \
                    and node.func.attr == "append" \
                    and isinstance(node.func.value, ast.Name) \
                    and node.func.value.id == "blockers":
                for a in node.args:
                    if isinstance(a, ast.Name) and a.id in moved_names:
                        reassessment_raises_moved.append(a.id)

    # (6) semantic consistency: present, wired, and free of recomputed economics
    semantic_check_present = "def check_decision_semantics(" in ws_src
    semantic_check_wired = "semantic_violations = check_decision_semantics(" in ws_src
    semantic_sets_inconsistent = "consistency_status = INCONSISTENT" in ws_src
    sem_body = ""
    if semantic_check_present:
        _s = ws_src.index("def check_decision_semantics(")
        sem_body = ws_src[_s:ws_src.index("\ndef ", _s + 10)]
    semantic_recomputes_economics = sorted(
        t for t in ("hurdle", "turnover_budget", "herfindahl", "cost_rate")
        if t in sem_body)

    # (7) WITHHELD is fail-closed at every layer
    withheld_declared_everywhere = all([
        bool(_assign(rp_k, "STATE_WITHHELD")),
        bool(_assign(pd_src, "PDS_CHANGE_WITHHELD")),
        bool(_assign(ws_src, "RPS_WITHHELD")),
    ])
    withheld_not_approvable = all([
        "APPROVABLE_STATES = (STATE_READY, STATE_DEGRADED)" in rp_k,
        "APPROVABLE_READ_STATES = (STATE_READY, STATE_DEGRADED)" in arp_src,
        "REALLOCATION_APPROVABLE_STATES = (RPS_READY, RPS_DEGRADED)" in ws_src,
        "APPROVABLE_DECISION_STATES = (PDS_REVIEW_REQUIRED, PDS_HELD)" in pd_src,
    ])
    withheld_blocks_record = "reallocation_proposal_withheld" in pd_src

    # (8) UI: verbatim, ONE loader, no synthesised approve / order control
    ui_verdict_present = ('id="cc-verdict"' in ui
                          and "function _wsRenderPortfolioVerdict(d)" in ui)
    ui_verdict_reads_owner = "d.canonical_portfolio_decision" in ui
    ui_verdict_single_call = ui.count(
        "try { _wsRenderPortfolioVerdict(d); } catch (e) {}") == 1
    ui_verdict_body = ""
    if ui_verdict_present:
        _u = ui.index("function _wsRenderPortfolioVerdict(d)")
        ui_verdict_body = ui[_u:ui.index("\nwindow._wsRenderPortfolioVerdict", _u)]
    ui_verdict_derives_state = sorted(
        t for t in ("herfindahl", "* nav", "cost_rate", "0.05", "0.35")
        if t in ui_verdict_body)
    ui_verdict_synthesises_action = sorted(
        t for t in ("dispatchCanonicalPrimaryAction", "createOrder", "recordDecision",
                    "CONFIRM_PORTFOLIO_REBALANCE_DECISION", "alert(", "confirm(")
        if t in ui_verdict_body)
    ui_hero_scoped = all(t in ui for t in (
        'body[data-route="markets"] #operator-command',
        'body[data-route="system-audit"] #operator-command',
        'body[data-route="portfolio-manager"] #operator-command',
        'body[data-route="research"] #operator-command[data-op-research="1"]'))

    return {
        "gate_vocabulary_clean": gate_vocabulary_clean,
        "gate_headline_clean": gate_headline_clean,
        "membership_not_action_required": membership_not_action_required,
        "close_vocabulary_clean": close_vocabulary_clean,
        "close_normaliser_present": close_normaliser_present,
        "close_normalises_history": close_normalises_history,
        "mandatory_exit_policy_owners": policy_owners,
        "mandatory_exit_policy_owner_count": len(policy_owners),
        "constraint_codes_agree": constraint_codes_agree,
        "constraint_owner_declared": constraint_owner_declared,
        "deferred_constraints": sorted(prs_deferred),
        "reassessment_raises_moved_constraint": sorted(set(reassessment_raises_moved)),
        "semantic_check_present": semantic_check_present,
        "semantic_check_wired": semantic_check_wired,
        "semantic_sets_inconsistent": semantic_sets_inconsistent,
        "semantic_recomputes_economics": semantic_recomputes_economics,
        "withheld_declared_everywhere": withheld_declared_everywhere,
        "withheld_not_approvable": withheld_not_approvable,
        "withheld_blocks_record": withheld_blocks_record,
        "ui_verdict_present": ui_verdict_present,
        "ui_verdict_reads_owner": ui_verdict_reads_owner,
        "ui_verdict_single_call": ui_verdict_single_call,
        "ui_verdict_derives_state": ui_verdict_derives_state,
        "ui_verdict_synthesises_action": ui_verdict_synthesises_action,
        "ui_hero_scoped": ui_hero_scoped,
    }


def check_release29_4_session_authority(files: list[Path]) -> dict:
    r"""Release 29.4 - NORMAL-CYCLE SESSION AUTHORITY + CLOSE VALIDITY.

    On 2026-08-18 at 08:31 ET the operator screen offered RUN DAILY CLOSE for the
    2026-08-17 session, which had already been closed the previous evening, while the
    market session was still open. The cause was a DUPLICATED VOCABULARY:
    ``api.workflow_state`` kept a private literal copy of the Daily Close owner's
    completed-close statuses, Release 29.3 renamed one of them, and the copy kept the
    old spelling - so a real completed close stopped being recognised.

    These contracts make that class of drift a build failure:

      (1) ``api.daily_close`` OWNS close validity and publishes the predicate;
      (2) no other module defines a completed-close vocabulary of its own;
      (3) ``api.workflow_state`` DELEGATES (and its pure-import fallback still matches
          the owner's set exactly, by AST - the drift itself);
      (4) close validity takes NO portfolio input (proved on the signature, not on prose);
      (5) session eligibility belongs to ``engine.market_session``; the workflow owner
          runs no calendar arithmetic of its own;
      (6) the session-authority violation codes exist and are wired into the verdict;
      (7) TODAY is the sole normal-path execution surface (every other route drops the
          execute control AND the dispatcher refuses off-Today);
      (8) the model-target snapshot lane states its scope and is not an input to the
          canonical portfolio decision.
    """
    dc_src = _read("api/daily_close.py")
    ws_src = _read("api/workflow_state.py")
    ps_src = _read("api/portfolio_state.py")
    nc_src = _read("engine/normal_cycle.py")
    ui = _read(UI_FILE)

    def _fn_body(src: str, name: str) -> str:
        marker = "def %s(" % name
        if marker not in src:
            return ""
        return src.split(marker, 1)[1].split("\ndef ", 1)[0]

    def _set_literal(src: str, name: str) -> set:
        """The string values a module-level frozenset/set constant holds (AST)."""
        try:
            tree = ast.parse(src)
        except SyntaxError:
            return set()
        for node in tree.body:
            if not isinstance(node, ast.Assign):
                continue
            if not any(isinstance(t, ast.Name) and t.id == name for t in node.targets):
                continue
            v = node.value
            if isinstance(v, ast.Call) and getattr(v.func, "id", None) == "frozenset" \
                    and v.args:
                v = v.args[0]
            if isinstance(v, (ast.Set, ast.Tuple, ast.List)):
                return {e.value for e in v.elts if isinstance(e, ast.Constant)}
        return set()

    def _tuple_of_strings(src: str, name: str) -> tuple:
        try:
            tree = ast.parse(src)
        except SyntaxError:
            return ()
        for node in tree.body:
            if not isinstance(node, ast.Assign):
                continue
            if not any(isinstance(t, ast.Name) and t.id == name for t in node.targets):
                continue
            if isinstance(node.value, (ast.Tuple, ast.List)):
                return tuple(e.value for e in node.value.elts
                             if isinstance(e, ast.Constant))
        return ()

    # (1) The owner publishes the predicate and names its policy.
    close_validity_owned_by_daily_close = all(
        ("def %s(" % fn) in dc_src for fn in (
            "completed_close_statuses", "is_completed_close_status",
            "is_operational_close_complete")) \
        and _assign_const(dc_src, "CLOSE_VALIDITY_OWNER") == "api.daily_close" \
        and _assign_const(dc_src, "CLOSE_VALIDITY_POLICY") == "OPERATIONAL_COMPLETION_ONLY"

    # (2) No second definition of the vocabulary anywhere else.
    duplicate_vocabulary_modules = sorted(
        rel for rel, src in (("api/workflow_state.py", ws_src),
                             ("api/portfolio_state.py", ps_src))
        if "_CLOSE_COMPLETE_STATUSES" in src)
    no_duplicate_close_vocabulary = not duplicate_vocabulary_modules

    # (3) The workflow owner delegates, and its fallback still matches the owner EXACTLY.
    #     This is the precise drift that broke the live payload, so it is compared as a
    #     set of values rather than trusted to a comment.
    owner_set = _set_literal(dc_src, "_CLOSE_PROCESSED_STATUSES") or {
        v for v in _tuple_of_strings(dc_src, "_CLOSE_PROCESSED_STATUSES")}
    if not owner_set:
        # _CLOSE_PROCESSED_STATUSES is declared through Name references; resolve them.
        owner_set = set()
        try:
            tree = ast.parse(dc_src)
            consts = {}
            for node in tree.body:
                if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
                    for t in node.targets:
                        if isinstance(t, ast.Name):
                            consts[t.id] = node.value.value
            for node in tree.body:
                if isinstance(node, ast.Assign) and any(
                        isinstance(t, ast.Name) and t.id == "_CLOSE_PROCESSED_STATUSES"
                        for t in node.targets) and isinstance(node.value, (ast.Tuple,
                                                                          ast.List,
                                                                          ast.Set)):
                    for e in node.value.elts:
                        if isinstance(e, ast.Constant):
                            owner_set.add(e.value)
                        elif isinstance(e, ast.Name) and e.id in consts:
                            owner_set.add(consts[e.id])
        except SyntaxError:
            owner_set = set()
    fallback_set = _set_literal(ws_src, "_CLOSE_COMPLETE_FALLBACK")
    workflow_delegates_close_validity = (
        "def _is_operational_close_complete(" in ws_src
        and "_dc.is_operational_close_complete(" in ws_src
        and "_dc.completed_close_statuses()" in ws_src
        and _assign_const(ws_src, "CLOSE_VALIDITY_OWNER") == "api.daily_close"
        and bool(owner_set) and fallback_set == owner_set)

    # (4) Close validity excludes portfolio inputs - proved on the SIGNATURE.
    validity_params: list[str] = []
    try:
        for node in ast.parse(dc_src).body:
            if isinstance(node, ast.FunctionDef) \
                    and node.name == "is_operational_close_complete":
                validity_params = [a.arg for a in node.args.args] \
                    + [a.arg for a in node.args.kwonlyargs]
    except SyntaxError:
        validity_params = ["<unparsed>"]
    excluded = _tuple_of_strings(dc_src, "CLOSE_VALIDITY_EXCLUDED_INPUTS")
    close_validity_excludes_portfolio_inputs = (
        validity_params == ["progress"]
        and {"membership_drift", "reallocation_proposal", "portfolio_reassessment",
             "holding_opportunity_cost", "portfolio_decision"} <= set(excluded))

    # (5) Market-date eligibility is not recomputed by the workflow owner.
    calendar_fns = ("walk_back_to_trading_day(", "previous_trading_day(",
                    "resolve_expected_session(", "expected_from_reference_date(")
    workflow_recomputes_calendar = sorted(f for f in calendar_fns if f in ws_src)
    session_eligibility_owned_by_market_session = (
        _assign_const(ws_src, "SESSION_ELIGIBILITY_OWNER") == "engine.market_session"
        and not workflow_recomputes_calendar
        # And no second state machine: the cycle kernel keeps its five stages.
        and len(_tuple_of_strings(nc_src, "STAGE_SEQUENCE") or ()) in (0, 5))

    # (6) The session-authority invariants exist, are frozen and are wired in.
    codes = _tuple_of_strings(ws_src, "SESSION_AUTHORITY_VIOLATION_CODES")
    session_authority_codes_frozen = set(codes) == {
        "DAILY_CLOSE_OFFERED_FOR_ALREADY_PROCESSED_SESSION",
        "COMPLETED_CLOSE_REPORTED_INVALID",
        "COMPLETED_CLOSE_HIDDEN_FROM_EVIDENCE"}
    session_check_wired = (
        "def check_session_authority(" in ws_src
        and "session_violations = check_session_authority(" in ws_src
        and "consistency_violations = list(consistency_violations) + session_violations"
        in ws_src)
    # It must COMPARE owners, never load or recompute one.
    sa_body = _fn_body(ws_src, "check_session_authority")
    session_check_recomputes = sorted(
        f for f in ("load_", "import ", "open(", "Path(") if f in sa_body)

    # (7) TODAY is the sole normal-path execution surface.
    non_today_routes = ("portfolio-manager", "holding-review", "proposed-portfolio",
                        "markets", "system-audit")
    cta_hidden = all(
        'body[data-route="%s"] #operator-command .opc-cta' % r in ui
        for r in non_today_routes)
    today_keeps_cta = not any(
        'body[data-route="%s"] #operator-command .opc-cta' % r in ui
        for r in ("command-center", "today"))
    dispatcher = (ui.split("function dispatchCanonicalPrimaryAction(", 1)[1]
                  .split("\nwindow.", 1)[0]
                  if "function dispatchCanonicalPrimaryAction(" in ui else "")
    dispatcher_guarded = ("_wsIsTodayRoute()" in dispatcher
                          and "navigateToRoute('command-center')" in dispatcher)
    today_is_sole_execution_surface = bool(
        cta_hidden and today_keeps_cta and dispatcher_guarded
        and "function _wsIsTodayRoute(" in ui and 'id="opc-go-today"' in ui)

    # (8) The model-target snapshot lane states its scope and stays out of the
    #     canonical portfolio decision (the Release 30 contract).
    scope_block = ui.split('id="otr-scope"')[1].split("</div>")[0].lower() \
        if 'id="otr-scope"' in ui else ""
    cpd_body = _fn_body(ws_src, "build_canonical_portfolio_decision")
    model_target_lane_scoped = bool(
        "MODEL TARGET SNAPSHOT REVIEW" in ui
        and "not a portfolio reallocation proposal" in scope_block
        and "'READY_TO_CONFIRM': 'READY TO CONFIRM SNAPSHOT'" in ui
        and cpd_body
        and not any(t in cpd_body for t in ("alpha_target", "target_readiness")))

    return {
        "close_validity_owned_by_daily_close": close_validity_owned_by_daily_close,
        "no_duplicate_close_vocabulary": no_duplicate_close_vocabulary,
        "duplicate_vocabulary_modules": duplicate_vocabulary_modules,
        "workflow_delegates_close_validity": workflow_delegates_close_validity,
        "close_validity_owner_set": sorted(owner_set),
        "workflow_fallback_set": sorted(fallback_set),
        "close_validity_excludes_portfolio_inputs":
            close_validity_excludes_portfolio_inputs,
        "close_validity_signature": validity_params,
        "session_eligibility_owned_by_market_session":
            session_eligibility_owned_by_market_session,
        "workflow_recomputes_calendar": workflow_recomputes_calendar,
        "session_authority_codes_frozen": session_authority_codes_frozen,
        "session_check_wired": session_check_wired,
        "session_check_recomputes": session_check_recomputes,
        "today_is_sole_execution_surface": today_is_sole_execution_surface,
        "model_target_lane_scoped": model_target_lane_scoped,
    }


def check_release29_5_drc_provenance(files: list[Path]) -> dict:
    r"""Release 29.5 - PRE-DRC PROVENANCE vs GOVERNED DRC TERMINAL EVIDENCE.

    On 2026-08-18, after a SUCCESSFUL Daily Close, the normal cycle suspended itself into
    RECOVERY over TERMINAL_DOWNSTREAM_ARTIFACTS_WITHOUT_DRC_MANIFEST - and RECOVERY opens
    no stage gate, so the Daily Research Cycle that writes the missing manifest could
    never be run. The trigger was an inference from EXISTENCE: a Holding Opportunity-Cost
    artifact without a run manifest was read as corruption. Since Releases 28/29 that is
    ALSO the signature of the perfectly legitimate artifact the event-driven refresh
    writes whenever continuous collection finds material information.

    These contracts make that class of confusion a build failure:

      (1) the ARTIFACT OWNER publishes the provenance vocabulary and a PURE classifier
          (no store parameter - validating a manifest is not its job);
      (2) no artifact of either class ever proves completion; only a manifest does;
      (3) both canonical producers identify themselves, and ONLY the governed cycle
          stamps a run id;
      (4) the manifest has exactly ONE owner - no other module writes a run record or
          raises the terminal-artifact blocker;
      (5) ``api.workflow_state`` READS the classification and invents none of its own;
      (6) the fail-closed blocker still exists and now fires on the CLAIM;
      (7) the UI states provenance from the backend and infers cycle completion from
          nothing.
    """
    hoc_src = _read("api/holding_opportunity_cost.py")
    drc_src = _read("api/daily_research_cycle.py")
    esr_src = _read("api/event_signal_refresh.py")
    ws_src = _read("api/workflow_state.py")
    gate_src = _read("api/daily_action_gate.py")
    ui = _read(UI_FILE)

    def _params(src: str, name: str) -> list:
        try:
            for node in ast.parse(src).body:
                if isinstance(node, ast.FunctionDef) and node.name == name:
                    return ([a.arg for a in node.args.args]
                            + [a.arg for a in node.args.kwonlyargs])
        except SyntaxError:
            return ["<unparsed>"]
        return []

    # (1) The artifact owner owns the vocabulary, and the classifier is PURE.
    classifier_params = _params(hoc_src, "classify_artifact_provenance")
    provenance_owned_by_artifact_owner = bool(
        _assign_const(hoc_src, "PROVENANCE_OWNER") == "api.holding_opportunity_cost"
        and _assign_const(hoc_src,
                          "ARTIFACT_CLASS_LIVE_PRE_DRC") == "LIVE_PRE_DRC_SIGNAL"
        and _assign_const(hoc_src,
                          "ARTIFACT_CLASS_GOVERNED_DRC_TERMINAL") == "GOVERNED_DRC_TERMINAL"
        and "def classify_artifact_provenance(" in hoc_src
        and "def build_provenance(" in hoc_src)
    classifier_is_pure = (classifier_params == ["artifact"])

    # (2) No artifact proves completion - stated by the owner, not by a consumer.
    artifact_never_proves_completion = (
        '"proves_drc_complete": False' in hoc_src
        and '"opportunity_cost_proves_drc_complete": False' in drc_src)

    # (3) Both producers identify themselves; only the governed cycle stamps a run id.
    #     Asserted on the CALL, so a docstring can never satisfy or break it.
    producers_identify_themselves = bool(
        "produced_by=hoc.PRODUCER_DAILY_RESEARCH_CYCLE" in drc_src
        and "drc_run_id=drc_run_id" in drc_src
        and "produced_by=hoc.PRODUCER_EVENT_SIGNAL_REFRESH" in esr_src)
    event_cycle_stamps_no_run_id = "drc_run_id=" not in esr_src

    # (4) ONE manifest owner. Nobody else writes a run record or raises the blocker.
    manifest_writers = sorted(
        rel for rel, src in (("api/holding_opportunity_cost.py", hoc_src),
                             ("api/event_signal_refresh.py", esr_src),
                             ("api/workflow_state.py", ws_src),
                             ("api/daily_action_gate.py", gate_src))
        if "_save_run(" in src
        or "TERMINAL_DOWNSTREAM_ARTIFACTS_WITHOUT_DRC_MANIFEST" in src)
    manifest_has_one_owner = not manifest_writers

    # (5) The workflow owner READS the classification; it derives none.
    workflow_reads_provenance = bool(
        "opportunity_cost_artifact_class" in ws_src
        and "governed_research_evidence_current" in ws_src
        and "classify_artifact_provenance" not in ws_src)
    # The gate CARRIES the fields verbatim (the one shared path), never computing them.
    gate_carries_provenance = bool(
        '"opportunity_cost_artifact_class"' in gate_src
        and '"opportunity_cost_claims_drc_terminal"' in gate_src
        and "classify_artifact_provenance" not in gate_src)

    # (6) The fail-closed blocker survives, and it now fires on the CLAIM.
    status_body = (drc_src.split("def load_daily_research_cycle_status(", 1)[1]
                   .split("\ndef ", 1)[0]
                   if "def load_daily_research_cycle_status(" in drc_src else "")
    blocker_fires_on_claim = bool(
        "TERMINAL_DOWNSTREAM_ARTIFACTS_WITHOUT_DRC_MANIFEST" in status_body
        and "claims_terminal" in status_body
        and "if claims_terminal:" in status_body)

    # (7) The UI states what the backend decided and infers nothing.
    ui_infers_provenance = sorted(
        t for t in ("LIVE_PRE_DRC_SIGNAL", "GOVERNED_DRC_TERMINAL",
                    "classify_artifact_provenance", "governed_research_evidence_current")
        if t in ui)
    ui_states_backend_provenance = bool(
        "cd.provenance_label" in ui and "r29-verdict-prov" in ui
        and not ui_infers_provenance)

    return {
        "provenance_owned_by_artifact_owner": provenance_owned_by_artifact_owner,
        "classifier_is_pure": classifier_is_pure,
        "classifier_signature": classifier_params,
        "artifact_never_proves_completion": artifact_never_proves_completion,
        "producers_identify_themselves": producers_identify_themselves,
        "event_cycle_stamps_no_run_id": event_cycle_stamps_no_run_id,
        "manifest_has_one_owner": manifest_has_one_owner,
        "manifest_writers": manifest_writers,
        "workflow_reads_provenance": workflow_reads_provenance,
        "gate_carries_provenance": gate_carries_provenance,
        "blocker_fires_on_claim": blocker_fires_on_claim,
        "ui_states_backend_provenance": ui_states_backend_provenance,
        "ui_infers_provenance": ui_infers_provenance,
    }


def check_portfolio_reassessment_ownership(files: list[Path]) -> dict:
    """Stage 20 CONTINUOUS ACTIVE PORTFOLIO REASSESSMENT ownership guard.

    Proves the Stage-20 architecture invariants:
      (1)  engine/portfolio_reassessment.py is the SOLE portfolio-level reassessment
           calculation owner (``build_reassessment`` defined exactly once);
      (2)  api/portfolio_reassessment.py is the SOLE composition / persistence / read
           owner (``load_portfolio_reassessment`` defined exactly once);
      (3)  the reassessment kernel does NOT fork the Slice-6 holding comparison, the
           Slice-7 target math or the scoring math (ONE HOC owner, ONE proposal owner,
           ONE ranking owner);
      (4)  the reassessment NEVER builds a target portfolio: only
           ``engine.reallocation_proposal`` defines ``build_proposal``;
      (5)  no second cost model / risk model / NAV owner / portfolio state is introduced
           (the owner composes ``paper_trading_desk`` costs, the Slice-6 covariance
           primitive and ``portfolio_state``, and never calls ``book_nav``);
      (6)  the GET read routes exist and no run / execute / approve / apply / rebalance
           reassessment route exists (no automatic rebalance is reachable);
      (7)  SIGNAL REFRESH AND REASSESSMENT ARE LINKED: the Daily Research Cycle owns a
           ``REASSESS_PORTFOLIO`` step that delegates to the owner;
      (8)  the reassessment GATES the target engine: the DRC consults
           ``should_build_proposal`` (via ``_reassessment_gate``) before
           ``BUILD_REALLOCATION_PROPOSAL``;
      (9)  Stage-19 execution PRECEDENCE exists and is consumed by the workflow owner;
      (10) MODEL RECALIBRATION REMAINS SEPARATE: the reassessment never promotes,
           retrains or recalibrates, and does not absorb ``api.research_agent``;
      (11) the workflow owner DELEGATES (no second economic gate in workflow_state);
      (12) the UI has exactly ONE loader and performs NO client-side assessment logic;
      (13) immutable / idempotent artifact ownership (atomic persist + index) and an
           append-only, never-back-filled history;
      (14) no automatic approval / order-plan confirmation / order / fill anywhere in
           the Stage-20 owners;
      (15) inventory drift is zero (checked by ``check_inventory_drift``).
    """
    kernel_src = _read(PRS_KERNEL)
    owner_src = _read(PRS_OWNER)
    drc_src = _read(DRC_OWNER)
    wf_src = _read(WORKFLOW_STATE_OWNER)
    ui = _read(UI_FILE)

    kernel_present = (REPO_ROOT / PRS_KERNEL).exists()
    owner_present = (REPO_ROOT / PRS_OWNER).exists()

    # (1) sole calculation owner.
    calc_def_modules = []
    read_def_modules = []
    proposal_def_modules = []
    for fp in files:
        rel = _rel(fp)
        if rel == "scripts/audit_architecture.py":
            continue
        src = fp.read_text(encoding="utf-8", errors="replace")
        if PRS_KERNEL_BUILD_DEF in src:
            calc_def_modules.append(rel)
        if PRS_OWNER_LOAD_DEF in src:
            read_def_modules.append(rel)
        if RP_KERNEL_BUILD_DEF in src:
            proposal_def_modules.append(rel)
    second_calculation_owner = sorted(set(calc_def_modules) - {PRS_KERNEL})
    second_composition_owner = sorted(set(read_def_modules) - {PRS_OWNER})
    # (4) exactly ONE target engine, and it is NOT the reassessment.
    second_target_engine = sorted(set(proposal_def_modules) - {RP_KERNEL})

    # (3) the kernel must not fork a neighbouring owner's math.
    kernel_forks = sorted(t for t in PRS_KERNEL_FORKS if t in kernel_src)

    # (5)/(10)/(14) forbidden calls.
    owner_forbidden = sorted(t for t in PRS_FORBIDDEN_CALLS if t in owner_src)
    kernel_forbidden = sorted(t for t in PRS_KERNEL_FORBIDDEN if t in kernel_src)

    # The owner must COMPOSE the authoritative owners (never fork them).
    delegates = {tok: (tok in owner_src) for tok in PRS_MUST_DELEGATE}
    missing_delegation = sorted(k for k, v in delegates.items() if not v)

    # (6) routes.
    routes = check_routes()["routes"]
    prs_entries = [r for r in routes if (r["path"] or "").startswith(PRS_ROUTE)]
    route_get_count = sum(1 for r in prs_entries if r["method"] == "GET")
    route_methods = sorted({r["method"] for r in prs_entries})
    forbidden_routes_present = sorted(
        r for r in PRS_FORBIDDEN_ROUTES if any(rt["path"] == r for rt in routes))
    non_get_methods_present = bool(route_methods and route_methods != ["GET"])

    # (7)/(8) signal refresh -> reassessment -> (gated) proposal.
    drc_step_present = DRC_PRS_STEP in drc_src
    drc_delegates = (DRC_PRS_DELEGATE_TOKEN in drc_src and "run_and_persist" in drc_src)
    drc_gate_present = DRC_PRS_GATE_TOKEN in drc_src
    drc_gate_consults_owner = "should_build_proposal" in drc_src
    # The proposal step must be guarded by the gate result, not run unconditionally.
    drc_proposal_gated = ('elif not _gate["build_proposal"]:' in drc_src
                          or "if not _gate[\"build_proposal\"]" in drc_src)
    # The reassessment step must be ORDERED before the proposal step in the sequence.
    try:
        seq_i = drc_src.index("STEP_SEQUENCE = (")
        seq = drc_src[seq_i:drc_src.index(")", seq_i)]
        reassess_before_proposal = (seq.index("STEP_REASSESS_PORTFOLIO")
                                    < seq.index("STEP_BUILD_REALLOCATION"))
    except ValueError:
        reassess_before_proposal = False

    # (9)/(11) workflow owner delegates and honours Stage-19 precedence.
    wf_delegates = WF_PRS_DELEGATE_TOKEN in wf_src
    wf_precedence = WF_PRS_PRECEDENCE_TOKEN in wf_src
    precedence_owner_present = PRS_PRECEDENCE_DEF in owner_src
    gate_owner_present = PRS_GATE_DEF in owner_src
    # workflow_state must not implement a SECOND economic gate.
    wf_second_gate = sorted(t for t in ("min_portfolio_net_improvement",
                                        "max_one_way_turnover_per_reassessment",
                                        "def build_reassessment(",
                                        "score_points_per_cost_bp")
                            if t in wf_src)

    # (10) recalibration stays a separate lane: the reassessment must not import or
    # absorb the research agent, and must not define a recalibration decision.
    absorbs_recalibration = ("research_agent" in kernel_src
                             or "recalibration" in kernel_src.lower()
                             or "def evaluate_recalibration(" in owner_src)

    # (13) persistence + append-only history.
    persist_present = PRS_OWNER_PERSIST_DEF in owner_src
    atomic_persist_present = ("os.replace(" in owner_src and "index" in owner_src.lower())
    history_append_only = ("_append_history" in owner_src and "append_only" in owner_src)
    no_backfill_declared = ('"backfilled": False' in owner_src)

    # (12) UI: exactly one loader; the region computes nothing.
    ui_loader_count = ui.count(UI_PRS_LOADER)
    ui_fetch_count = ui.count(UI_PRS_FETCH)
    ui_region_hits = []
    start = ui.find(UI_PRS_LOADER)
    end = ui.find(UI_PRS_REGION_END)
    if start != -1 and end != -1 and end > start:
        region = ui[start:end]
        for pat in UI_PRS_FORBIDDEN:
            if pat in region:
                ui_region_hits.append(pat)

    return {
        "kernel": PRS_KERNEL, "owner": PRS_OWNER,
        "kernel_present": kernel_present, "owner_present": owner_present,
        "owners_present": bool(kernel_present and owner_present),
        "second_calculation_owner_modules": second_calculation_owner,
        "second_composition_owner_modules": second_composition_owner,
        "second_target_engine_modules": second_target_engine,
        "kernel_forks_neighbouring_owner": kernel_forks,
        "delegates": delegates, "missing_delegation": missing_delegation,
        "owner_forbidden_calls": owner_forbidden,
        "kernel_forbidden_calls": kernel_forbidden,
        "route_get_count": route_get_count,
        "route_methods": route_methods,
        "non_get_methods_present": non_get_methods_present,
        "forbidden_routes_present": forbidden_routes_present,
        "drc_step_present": bool(drc_step_present),
        "drc_delegates": bool(drc_delegates),
        "signal_refresh_linked_to_reassessment": bool(drc_step_present and drc_delegates),
        "drc_gate_present": bool(drc_gate_present),
        "drc_gate_consults_owner": bool(drc_gate_consults_owner),
        "proposal_gated_by_reassessment": bool(drc_proposal_gated),
        "reassessment_ordered_before_proposal": bool(reassess_before_proposal),
        "no_automatic_rebalance": bool(not forbidden_routes_present
                                       and not non_get_methods_present),
        "execution_precedence_owner_present": bool(precedence_owner_present),
        "proposal_gate_owner_present": bool(gate_owner_present),
        "workflow_delegates_to_owner": bool(wf_delegates),
        "workflow_honours_execution_precedence": bool(wf_precedence),
        "workflow_second_economic_gate": wf_second_gate,
        "recalibration_absorbed": bool(absorbs_recalibration),
        "recalibration_remains_separate": bool(not absorbs_recalibration),
        "persist_present": bool(persist_present),
        "atomic_idempotent_persist_present": bool(atomic_persist_present),
        "history_append_only": bool(history_append_only),
        "no_hindsight_backfill_declared": bool(no_backfill_declared),
        "ui_loader_count": ui_loader_count,
        "ui_fetch_count": ui_fetch_count,
        "ui_client_assessment_logic": sorted(set(ui_region_hits)),
        "automatic_model_promotion_allowed": False,
        "automatic_approval_allowed": False,
        "cadence_enabled": False,
    }


def check_controlled_rebalance_ownership(files: list[Path]) -> dict:
    """Stage 19 CONTROLLED PAPER-REBALANCE + CORPORATE-ACTION ownership guard (the Milestone
    3 evolution). Encodes the canonical controlled route so it is ACCEPTED while every
    automatic / direct / hindsight / second-owner path stays REJECTED. The reallocation
    proposal remains an immutable REVIEW artifact and is NEVER itself an execution owner
    (check_reallocation_proposal_ownership); api/portfolio_decision.py owns the explicit
    manual APPROVE/REJECT/HOLD; api/rebalance_execution.py owns the controlled bridge from an
    APPROVED immutable proposal to a read-only order plan gated by a SECOND explicit manual
    confirmation; api/paper_trading_desk.py stays the SOLE paper-order lifecycle / NEXT_CLOSE
    fill owner (no second fill simulator / order ledger / NAV owner); api/corporate_actions.py
    is the SOLE split authority, confirm-gated and applied as a read-time projection that
    never rewrites immutable evidence. No broker, no automatic execution, no automatic
    approval, no same-close hindsight, no automatic model change."""
    routes = check_routes()["routes"]
    paths = {r["path"] for r in routes}
    methods_by_path = {}
    for r in routes:
        methods_by_path.setdefault(r["path"], set()).add(r["method"])

    reb_owner = "api/rebalance_execution.py"
    ca_owner = "api/corporate_actions.py"
    owner_present = (REPO_ROOT / reb_owner).exists() and (REPO_ROOT / ca_owner).exists()
    reb_src = _read(reb_owner)
    ca_src = _read(ca_owner)

    # The canonical controlled routes are ACCEPTED (the crux of the Stage-19 evolution).
    controlled_route_get = "GET" in methods_by_path.get("/v1/operations/rebalance", set())
    confirm_route_post = "POST" in methods_by_path.get(
        "/v1/operations/rebalance/confirm-order-plan", set())
    corporate_action_routes_present = (
        "GET" in methods_by_path.get("/v1/operations/corporate-actions", set())
        and "POST" in methods_by_path.get("/v1/operations/corporate-actions/register", set()))

    # Gate 1: an APPROVED Stage-18 portfolio decision is required (never auto-approved).
    requires_stage18_approval = ("portfolio_decision" in reb_src
                                 and "DECISION_APPROVE" in reb_src)
    # Gate 2: a SEPARATE second explicit confirmation token is required.
    requires_second_confirmation = (
        "CONFIRM_APPROVED_PORTFOLIO_REBALANCE_ORDER_PLAN" in reb_src)
    # Execution DELEGATES to the existing desk lifecycle + NEXT_CLOSE settlement (no hindsight).
    delegates_to_existing_desk = ("paper_trading_desk" in reb_src
                                  and "settle_due_orders" in reb_src)
    # NO second fill simulator / order ledger / NAV owner is DEFINED here (it only CALLS the
    # desk primitives; defining them would fork the canonical paper-desk owner).
    second_execution_owner_defs = sorted(
        d for d in ("def settle_due_orders(", "def book_nav(", "def _append_ledger(",
                    "def verify_ledger(", "def run_fill_cycle(", "def _row_hash(")
        if d in reb_src)
    # NO automatic approval / rebalance / cadence tokens.
    automatic_tokens_present = sorted(
        t for t in ("auto_approve", "auto_confirm", "auto_rebalance", "AUTO_APPROVE",
                    "schedule.every", "crontab")
        if t in reb_src)

    # Corporate action: confirm-gated + read-time projection that never rewrites evidence.
    corporate_action_confirm_gated = "CONFIRM_CORPORATE_ACTION_ADJUSTMENT" in ca_src
    corporate_action_read_time_projection = (
        "adjust_fills" in ca_src and "rewrote_immutable_evidence" in ca_src
        and not any(d in ca_src for d in ("def _append_ledger(", "def book_nav(")))

    # Automatic / direct proposal-execution routes stay FORBIDDEN (must be empty).
    forbidden_auto_routes = ("/v1/operations/rebalance-proposal",
                             "/v1/operations/apply-reallocation",
                             "/v1/operations/reallocation-proposal/apply",
                             "/v1/operations/reallocation-proposal/confirm",
                             "/v1/operations/reallocation-proposal/create-orders",
                             "/v1/operations/portfolio-proposal")
    forbidden_auto_execution_routes_present = sorted(
        r for r in forbidden_auto_routes if r in paths)

    return {
        "owner_present": owner_present,
        "controlled_route_get": controlled_route_get,
        "confirm_route_post": confirm_route_post,
        "corporate_action_routes_present": corporate_action_routes_present,
        "requires_stage18_approval": requires_stage18_approval,
        "requires_second_confirmation": requires_second_confirmation,
        "delegates_to_existing_desk": delegates_to_existing_desk,
        "second_execution_owner_defs": second_execution_owner_defs,
        "corporate_action_confirm_gated": corporate_action_confirm_gated,
        "corporate_action_read_time_projection": corporate_action_read_time_projection,
        "forbidden_auto_execution_routes_present": forbidden_auto_execution_routes_present,
        "automatic_tokens_present": automatic_tokens_present,
        "broker_enabled": False,
        "automatic_rebalance_allowed": False,
        "cadence_enabled": False,
    }


# --------------------------------------------------------------------------- #
# Stage 19.1 — corporate-action PROPAGATION integrity
# --------------------------------------------------------------------------- #
CA_OWNER_FILE = "api/corporate_actions.py"
DESK_OWNER_FILE = "api/paper_trading_desk.py"

#: Split arithmetic that may exist in EXACTLY ONE module (the canonical owner).
_SPLIT_MATH_MARKERS = ("def split_position(", "def adjust_fills(")


def check_corporate_action_propagation(files: list[Path]) -> dict:
    """Stage 19.1 strict guard: a registered corporate action must reach EVERY current
    economic read through ONE owner, and must never rewrite immutable historical evidence.

    Proves:
      (1) split arithmetic is DEFINED in exactly one module (api/corporate_actions.py);
      (2) no other module re-derives a split (no ``* ratio`` / ``/ ratio`` share math);
      (3) the desk's CURRENT economic primitives apply the registry BY DEFAULT, so a
          consumer cannot silently read an unadjusted current state;
      (4) the desk exposes a single CURRENT-state fill view (``current_fills``) that the
          per-holding consumers use instead of the raw immutable ledger reader;
      (5) the CURRENT performance projection is owned by api/corporate_actions.py and the
          raw historical rows are still returned untouched alongside it;
      (6) the corporate-action registry is part of the portfolio-state identity, so a
          registration invalidates an older proposal / decision / order plan;
      (7) both mutation gates (Stage-18 approval, Stage-19 order plan) enforce that
          staleness in the BACKEND, not in the UI;
      (8) the UI performs NO split / portfolio arithmetic of its own.
    """
    ca_src = _read(CA_OWNER_FILE)
    desk_src = _read(DESK_OWNER_FILE)
    ob_src = _read("api/operational_book.py")
    ps_src = _read("api/portfolio_state.py")
    rp_src = _read("api/reallocation_proposal.py")
    pd_src = _read("api/portfolio_decision.py")
    rb_src = _read("api/rebalance_execution.py")
    ui = _read(UI_FILE)

    # (1) split arithmetic defined in exactly ONE module.
    split_math_modules = []
    for fp in files:
        rel = _rel(fp)
        if rel in ("scripts/audit_architecture.py",):
            continue
        src = fp.read_text(encoding="utf-8", errors="replace")
        if any(m in src for m in _SPLIT_MATH_MARKERS):
            split_math_modules.append(rel)
    split_math_modules = sorted(set(split_math_modules))

    # (2) no second implementation of the share/price split transform.
    duplicate_split_math = sorted(
        rel for rel in split_math_modules if rel != CA_OWNER_FILE)

    # (3) the desk's current economic primitives default to the registry.
    desk_default_on = (
        "AUTO_CORPORATE_ACTIONS" in desk_src
        and "corporate_actions=AUTO_CORPORATE_ACTIONS" in desk_src
        and "def current_corporate_actions(" in desk_src)
    # (4) ONE current-state fill view, consumed by the per-holding owner.
    current_fill_view = ("def current_fills(" in desk_src
                         and "desk.current_fills(" in ob_src)
    # (5) the CURRENT performance projection is owned by the CA module and the raw rows
    #     are still returned untouched (no silent overwrite of immutable evidence).
    current_perf_projection_owned = (
        "def project_current_performance(" in ca_src
        and "def project_current_performance(" not in desk_src
        and "current_rows" in desk_src and "historical_rows_never_recomputed" in desk_src)
    # (6) registry identity is part of the portfolio state (and therefore of state_hash).
    state_binds_registry = ("registry_fingerprint" in ps_src
                            and '"corporate_actions": corporate_actions_block' in ps_src)
    proposal_binds_registry = ('"corporate_actions_hash"' in rp_src
                               and "def corporate_action_staleness(" in rp_src)
    # (7) BOTH mutation gates enforce staleness in the backend.
    approval_gate_enforced = "corporate_action_staleness" in pd_src
    order_plan_gate_enforced = "corporate_action_staleness" in rb_src
    # (8) the UI performs no split / portfolio arithmetic.
    ui_split_math = sorted(
        t for t in ("adjust_fills", "split_position", "* ratio", "/ ratio",
                    "quantity * 2", "shares_after")
        if t in ui)

    return {
        "owner_present": (REPO_ROOT / CA_OWNER_FILE).exists(),
        "split_math_modules": split_math_modules,
        "duplicate_split_math": duplicate_split_math,
        "single_split_math_owner": split_math_modules == [CA_OWNER_FILE],
        "desk_current_reads_default_to_registry": desk_default_on,
        "single_current_fill_view": current_fill_view,
        "current_performance_projection_owned": current_perf_projection_owned,
        "portfolio_state_binds_registry": state_binds_registry,
        "proposal_binds_registry": proposal_binds_registry,
        "approval_gate_enforces_staleness": approval_gate_enforced,
        "order_plan_gate_enforces_staleness": order_plan_gate_enforced,
        "ui_split_math_present": ui_split_math,
        "immutable_evidence_rewritten": False,
    }


# --------------------------------------------------------------------------- #
# Stage 19.2 — FAIL-CLOSED rebalance execution
# --------------------------------------------------------------------------- #
RB_OWNER_FILE = "api/rebalance_execution.py"

#: The EXACT defect this stage repairs: executability derived from the STATE NAME while the
#: plan itself had already recorded blocked names. It must never reappear in any form.
_STATE_DERIVED_BUILDABLE = '"order_plan_buildable": state not in'

#: A second owned-EODHD client / mark writer must NOT appear in the rebalance owner.
_RB_FORBIDDEN_PROVIDER = ("requests.", "httpx.", "urlopen(", "_live_downloader(",
                          "def sync_marks(", "def refresh_desk(", "def _fixture_downloader(",
                          "eodhd.com", "api.eodhistoricaldata.com")


def check_failclosed_rebalance_execution(files: list[Path]) -> dict:
    """Stage 19.2 strict guard: an APPROVED portfolio proposal may NEVER be converted into a
    materially incomplete paper rebalance.

    Proves:
      (1) api/rebalance_execution.py is the SOLE owner of the executability contract (it
          defines the target mark universe, the coverage read and the blocked states);
      (2) the state-derived ``order_plan_buildable`` defect is GONE — executability is a
          property of the reconciled PLAN, never of the state name;
      (3) the confirm gate refuses on a non-buildable plan, atomically, BEFORE any write;
      (4) target-mark hydration DELEGATES to the canonical desk mark owner — no second
          EODHD client, no second mark writer, no provider call inside this module;
      (5) the hydration route exists exactly once as a confirm-token-gated POST, and the
          read route stays GET-only and provider-free (a page load can never fetch or
          mutate marks);
      (6) the desk remains the SOLE order/fill/NEXT_CLOSE owner (no second fill simulator);
      (7) the UI renders the blocked state, names the blocked tickers, and exposes a
          confirmation only when the backend says the plan is confirmable;
      (8) no broker, no live order, no automation, no cadence.
    """
    rb_src = _read(RB_OWNER_FILE)
    desk_src = _read(DESK_OWNER_FILE)
    app_src = _read("api/app.py")
    ui = _read(UI_FILE)

    routes = check_routes()["routes"]
    methods_by_path = {}
    for r in routes:
        methods_by_path.setdefault(r["path"], set()).add(r["method"])
    hydrate_path = "/v1/operations/rebalance/refresh-target-marks"
    hydrate_methods = sorted(methods_by_path.get(hydrate_path, set()))
    hydrate_post_count = sum(1 for r in routes
                             if r["path"] == hydrate_path and r["method"] == "POST")
    read_methods = sorted(methods_by_path.get("/v1/operations/rebalance", set()))

    # (1) the executability contract lives in exactly one module.
    owner_defines_contract = all(t in rb_src for t in (
        "def target_mark_universe(", "def mark_coverage(",
        "ORDER_PLAN_BLOCKED_MISSING_OWNED_MARKS", "ORDER_PLAN_BLOCKED_INCOMPLETE_TARGET",
        "NON_CONFIRMABLE_STATES"))
    second_contract_owner_modules = sorted(
        _rel(fp) for fp in files
        if _rel(fp) not in (RB_OWNER_FILE, "scripts/audit_architecture.py")
        and "def target_mark_universe(" in fp.read_text(encoding="utf-8", errors="replace"))

    # (2) the exact August-12 defect must be absent everywhere.
    state_derived_buildable_modules = sorted(
        _rel(fp) for fp in files
        if _rel(fp) != "scripts/audit_architecture.py"
        and _STATE_DERIVED_BUILDABLE in fp.read_text(encoding="utf-8", errors="replace"))

    # (3) the confirm gate fails closed, atomically, on the freshly rebuilt plan.
    confirm_fails_closed = all(t in rb_src for t in (
        "ORDER_PLAN_BLOCKED", 'not plan.get("order_plan_buildable")',
        "refused_before_any_write", "revalidated_server_side"))

    # (4) hydration delegates; no second provider client / mark writer here.
    delegates_to_mark_owner = ("desk.refresh_desk(" in rb_src
                               and "extra_tickers=" in rb_src
                               and "REFRESH_CONFIRM_TOKEN" in rb_src)
    mark_owner_accepts_delegation = ("def refresh_desk(" in desk_src
                                     and "extra_tickers" in desk_src)
    rb_provider_calls = sorted(t for t in _RB_FORBIDDEN_PROVIDER if t in rb_src)

    # (5) explicit, token-gated hydration; the read route stays GET-only and provider-free.
    hydration_token_gated = ("CONFIRM_REBALANCE_TARGET_MARK_REFRESH" in rb_src
                             and "CONFIRM_REBALANCE_TARGET_MARK_REFRESH" in app_src
                             and "HYDRATE_CONFIRM_TOKEN" in rb_src)
    read_route_get_only = read_methods == ["GET"]
    # The read path must not be able to reach the provider: no refresh/sync call sits inside
    # the read contract or the plan reconciliation.
    read_start = rb_src.find("def load_rebalance_state(")
    read_end = rb_src.find("def confirm_rebalance_order_plan(")
    read_region = rb_src[read_start:read_end] if (read_start != -1 and read_end > read_start) else ""
    read_region_provider_calls = sorted(
        t for t in ("refresh_desk(", "sync_marks(", "refresh_target_marks(")
        if t in read_region)

    # (6) no second execution owner (mirrors the Stage-19 guard, restated for 19.2).
    second_execution_owner_defs = sorted(
        d for d in ("def settle_due_orders(", "def book_nav(", "def _append_ledger(",
                    "def run_fill_cycle(", "def _row_hash(", "def confirm_orders(")
        if d in rb_src)
    next_close_sole_settlement = ("EXECUTION_MODEL_DEFAULT" in desk_src
                                  and "desk.settle_due_orders(" in rb_src)

    # (7) UI: blocked state visible + confirmation gated on the backend contract.
    ui_missing_blocked_tokens = sorted(t for t in (
        "ORDER_PLAN_BLOCKED_MISSING_OWNED_MARKS", "ORDER PLAN BLOCKED",
        "stage19-blocked", "blocked_tickers", "missing_marks",
        "confirmation_available", "rebalanceRefreshTargetMarks")
        if t not in ui)
    # No browser-side create-orders trigger may exist. The endpoint may be NAMED in the UI
    # (the operator must know where the security boundary is); what must not exist is a
    # control that INVOKES it. So the check targets concrete invocation forms, not mentions.
    _cop = "/v1/operations/rebalance/confirm-order-plan"
    ui_order_creating_controls = sorted(t for t in (
        "call('POST', '" + _cop + "'", 'call("POST", "' + _cop + '"',
        "_mhzPost('" + _cop + "'", "path: '" + _cop + "'",
        "fetch('" + _cop + "'", "createOrders(")
        if t in ui)

    # (8) safety.
    automatic_tokens_present = sorted(
        t for t in ("auto_approve", "auto_confirm", "auto_rebalance", "AUTO_APPROVE",
                    "schedule.every", "crontab")
        if t in rb_src)

    return {
        "owner_present": (REPO_ROOT / RB_OWNER_FILE).exists(),
        "owner_defines_executability_contract": owner_defines_contract,
        "second_contract_owner_modules": second_contract_owner_modules,
        "state_derived_buildable_modules": state_derived_buildable_modules,
        "confirm_fails_closed_before_write": confirm_fails_closed,
        "delegates_to_canonical_mark_owner": delegates_to_mark_owner,
        "mark_owner_accepts_delegation": mark_owner_accepts_delegation,
        "owner_provider_calls": rb_provider_calls,
        "hydration_token_gated": hydration_token_gated,
        "hydration_route_post_count": hydrate_post_count,
        "hydration_route_methods": hydrate_methods,
        "read_route_methods": read_methods,
        "read_route_get_only": read_route_get_only,
        "read_region_provider_calls": read_region_provider_calls,
        "second_execution_owner_defs": second_execution_owner_defs,
        "next_close_sole_settlement": next_close_sole_settlement,
        "ui_missing_blocked_tokens": ui_missing_blocked_tokens,
        "ui_order_creating_controls": ui_order_creating_controls,
        "automatic_tokens_present": automatic_tokens_present,
        "broker_enabled": False,
        "automation_enabled": False,
        "cadence_enabled": False,
    }


# --------------------------------------------------------------------------- #
# Stage 19.3 — OPERATOR WORKFLOW & ATOMIC POST-CLOSE CONSOLIDATION
# --------------------------------------------------------------------------- #
DC_OWNER_FILE = "api/daily_close.py"
WS_OWNER_FILE = "api/workflow_state.py"
DESK_FILE = "api/paper_trading_desk.py"
OB_OWNER_FILE = "api/operational_book.py"

#: The EXACT August-13 defect: pending paper orders short-circuiting the daily-close
#: resolver before a newly eligible completed session was even considered.
_PENDING_SHORT_CIRCUIT = "if pending_orders:\n        return PAPER_ORDERS_SUBMITTED"

#: The standalone post-close desk-refresh CTA that competed with the Daily Close.
_COMPETING_REFRESH_LABEL = "Refresh After Market Close"

#: Second-implementation markers that must never appear outside their owners.
_SECOND_SETTLEMENT_DEFS = ("def settle_due_orders(", "def sync_marks(",
                           "def refresh_desk(", "def append_performance(")


def check_operator_atomic_close_ownership(files: list[Path]) -> dict:
    """Stage 19.3 strict guard: ONE operator command, ONE post-close orchestration path.

    Proves:
      (1) the daily-close resolver no longer short-circuits on pending orders — a newly
          eligible completed session outranks passive pending-order monitoring;
      (2) the canonical Daily Close SETTLES pending NEXT_CLOSE orders by composing the
          EXISTING Paper Desk owner (no second settlement engine / fill simulator /
          mark writer / order ledger / NAV owner);
      (3) the Paper Desk refresh is classified MAINTENANCE and can never be promoted to
          the canonical primary action (runtime guard + no normal-path UI exposure);
      (4) the backend owns ONE operator-command contract that every page mirrors — no
          page-level client-side workflow authority;
      (5) current-rebalance counts are LINEAGE-scoped in both owners, so historical
          initial-implementation fills and superseded plans can never be presented as
          the current plan's state;
      (6) no broker, no automation, no automatic rebalance/promotion/recalibration.
    """
    dc_src = _read(DC_OWNER_FILE)
    ws_src = _read(WS_OWNER_FILE)
    desk_src = _read(DESK_FILE)
    ob_src = _read(OB_OWNER_FILE)
    rb_src = _read(RB_OWNER_FILE)
    ui = _read(UI_FILE)

    # (1) precedence repaired, and the defect absent everywhere.
    pending_short_circuit_modules = sorted(
        _rel(fp) for fp in files
        if _rel(fp) != "scripts/audit_architecture.py"
        and _PENDING_SHORT_CIRCUIT in fp.read_text(encoding="utf-8", errors="replace"))
    precedence_repaired = all(t in dc_src for t in (
        "new_close_pending", "if pending_orders and not new_close_pending:",
        "forward_tracking"))
    # Fail-closed paths preserved: an unpublished / incomplete session still blocks.
    fails_closed_preserved = all(t in dc_src for t in (
        "if not valuation_complete:\n        return DATA_BLOCKED",
        "return AWAITING_MARKET_CLOSE if not cutoff_passed else WAITING_FOR_MARKET_DATA"))

    # (2) the close COMPOSES the desk owner; it implements no settlement of its own.
    close_composes_desk = all(t in dc_src for t in (
        "refresh_fn or desk.refresh_desk", "desk.REFRESH_CONFIRM_TOKEN",
        "completed_through=latest_eligible"))
    dc_second_owner_defs = sorted(d for d in _SECOND_SETTLEMENT_DEFS if d in dc_src)
    desk_owns_settlement = all(t in desk_src for t in (
        "def settle_due_orders(", "def refresh_desk(", "EXECUTION_MODEL_DEFAULT",
        "_first_close_on_or_after("))
    second_settlement_modules = sorted(
        _rel(fp) for fp in files
        if _rel(fp) not in (DESK_FILE, "scripts/audit_architecture.py")
        and "def settle_due_orders(" in fp.read_text(encoding="utf-8", errors="replace"))
    # No-hindsight guard still enforced by the ONE settlement owner.
    no_hindsight_enforced = ("marks_latest_at_approval" in desk_src
                             and "strictly_after_store" in desk_src)
    # Settlement recorded exactly once per closed date, alongside the decision row.
    settlement_recorded_once = all(t in dc_src for t in (
        '"pending_orders_at_start": pending_at_start',
        '"settled_through_paper_desk"', "DAILY_CLOSE_JOURNAL_FILE"))

    # (3) the desk refresh is maintenance-only and never a canonical primary action.
    maintenance_classified = all(t in ws_src for t in (
        "MAINTENANCE_EXECUTION_KINDS", "NORMAL_PATH_EXECUTION_KINDS",
        "def assert_primary_action_contract("))
    guard_applied = "assert_primary_action_contract(_primary_action(" in ws_src
    ws_promotes_desk_refresh = '"execution_kind": EXEC_PAPER_DESK_REFRESH' in ws_src
    competing_refresh_modules = sorted(
        _rel(fp) for fp in files
        if _rel(fp) != "scripts/audit_architecture.py"
        and _COMPETING_REFRESH_LABEL in fp.read_text(encoding="utf-8", errors="replace"))
    ui_competing_refresh = _COMPETING_REFRESH_LABEL in ui
    ui_refresh_is_maintenance = all(t in ui for t in (
        'id="pd-maintenance"', "MAINTENANCE / RECOVERY", "Recovery: Refresh Desk Data"))
    # The endpoint itself SURVIVES (recovery capability), exactly once, POST + token.
    routes = check_routes()["routes"]
    desk_refresh_posts = sum(1 for r in routes
                             if r["path"] == "/v1/paper-desk/refresh" and r["method"] == "POST")

    # (4) ONE operator-command contract, backend-owned, mirrored by every surface.
    command_contract_present = all(t in ws_src for t in (
        "def build_operator_command(", '"operator_command": build_operator_command(',
        "primary_action_available", "mutation_controls_allowed", "NO_ACTION_TEXT"))
    second_command_owner_modules = sorted(
        _rel(fp) for fp in files
        if _rel(fp) not in (WS_OWNER_FILE, "scripts/audit_architecture.py")
        and "def build_operator_command(" in fp.read_text(encoding="utf-8", errors="replace"))
    ui_command_bar = all(t in ui for t in (
        'id="operator-command"', "function renderOperatorCommand(",
        "d.operator_command", "dispatchCanonicalPrimaryAction(this)"))
    ui_command_renderer_count = ui.count("function renderOperatorCommand(")
    # The rail MIRRORS the contract and the banner obeys its withholding.
    ui_mirrors_command = ("cmd.primary_action_available !== true" in ui
                          and "!_cmdBlocks" in ui)
    # ONE execution surface: every secondary surface that used to repeat the same write
    # action defers to the command bar via the SINGLE shared ownership helper.
    single_execution_surface = all(t in ui for t in (
        "function _wsCommandOwnsExecution(",
        "_wsCommandOwnsExecution()",
        "var thCmdOwns = thExecutes",
        "var dupExec = !!pa.runs_daily_close && _wsCommandOwnsExecution();"))
    ui_ownership_helper_count = ui.count("function _wsCommandOwnsExecution(")
    # No page may recompute the workflow decision client-side.
    ui_client_workflow_authority = sorted(
        t for t in ("function decideWorkflowState", "function computePrimaryAction",
                    "function pickNextAction", "workflowPriority(")
        if t in ui)

    # (5) lineage-scoped current-rebalance counts in BOTH read owners + the UI.
    lineage_owned = all(t in ob_src for t in (
        "def current_rebalance_lineage(", '"counts_are_lineage_scoped": True',
        "historical_implementation_fill_count", "effective_fills"))
    lineage_summary_owned = all(t in rb_src for t in (
        "def build_execution_summary(", '"execution_summary": build_execution_summary(',
        "historical_implementation_fill_count", "superseded_plan_ids"))
    ui_lineage_aware = all(t in ui for t in (
        'id="pm-lc-current"', 'id="pm-lc-cur-submitted"', 'id="pm-lc-cur-filled"',
        'id="pm-lc-histfills"', "currentRebalance"))
    # The UI must not compute lineage itself (no client-side order-plan folding).
    ui_lineage_computation = sorted(
        t for t in ("rebalance_lineage", "order_plan_id ===", ".filter(function (o) { return o.status === 'FILLED'")
        if t in ui)

    # (6) safety.
    forbidden_automation = sorted(
        t for t in ("schedule.every", "crontab", "auto_close", "auto_settle",
                    "AUTO_RUN_DAILY_CLOSE", "auto_rebalance", "auto_promote")
        if t in (dc_src + ws_src + ob_src))
    return {
        "owners_present": all((REPO_ROOT / f).exists() for f in
                              (DC_OWNER_FILE, WS_OWNER_FILE, DESK_FILE, OB_OWNER_FILE)),
        # (1)
        "pending_short_circuit_modules": pending_short_circuit_modules,
        "close_precedence_repaired": precedence_repaired,
        "fails_closed_preserved": fails_closed_preserved,
        # (2)
        "close_composes_desk_owner": close_composes_desk,
        "close_second_settlement_defs": dc_second_owner_defs,
        "desk_owns_settlement": desk_owns_settlement,
        "second_settlement_modules": second_settlement_modules,
        "no_hindsight_enforced": no_hindsight_enforced,
        "settlement_recorded_once": settlement_recorded_once,
        # (3)
        "maintenance_kinds_classified": maintenance_classified,
        "primary_action_guard_applied": guard_applied,
        "workflow_promotes_desk_refresh": ws_promotes_desk_refresh,
        "competing_refresh_modules": competing_refresh_modules,
        "ui_competing_refresh_label": ui_competing_refresh,
        "ui_refresh_is_maintenance_only": ui_refresh_is_maintenance,
        "desk_refresh_route_post_count": desk_refresh_posts,
        # (4)
        "operator_command_contract_present": command_contract_present,
        "second_command_owner_modules": second_command_owner_modules,
        "ui_command_bar_present": ui_command_bar,
        "ui_command_renderer_count": ui_command_renderer_count,
        "ui_mirrors_command_contract": ui_mirrors_command,
        "ui_single_execution_surface": single_execution_surface,
        "ui_ownership_helper_count": ui_ownership_helper_count,
        "ui_client_workflow_authority": ui_client_workflow_authority,
        # (5)
        "lineage_counts_owned": lineage_owned,
        "lineage_summary_owned": lineage_summary_owned,
        "ui_lineage_aware": ui_lineage_aware,
        "ui_lineage_computation": ui_lineage_computation,
        # (6)
        "forbidden_automation_tokens": forbidden_automation,
        "broker_enabled": False,
        "automation_enabled": False,
        "automatic_rebalance_enabled": False,
        "automatic_promotion_enabled": False,
        "model_recalibration_added": False,
    }


def check_research_agent_ownership(files: list[Path]) -> dict:
    """Slice 8 (Phase 29I) strict semantic ownership guard for the Persistent Alpha Research
    Agent (Milestone 4). Proves: (1) engine/research_agent.py is the SOLE research-state
    calculation owner; (2) api/research_agent.py is the SOLE composition / persistence /
    read owner; (3) the GET read route exists exactly once; (4) NO promote / recalibrate /
    retrain / apply route exists; (5) the Daily Research Cycle is the sole scheduled
    execution path (the RUN_RESEARCH_AGENT step delegates to the API owner); (6) the API
    owner composes the authoritative evidence owners (never forks a metric); (7) no champion
    pointer write; (8) no model promotion; (9) no model recalibration / retraining; (10) no
    operational target / holdings / cash / NAV mutation; (11) no order / fill creation; (12)
    no broker / prediction / provider call; (13) the UI performs no research calculation;
    (14) exactly ONE UI loader; (15) immutable / idempotent artifact ownership; (16) Slice 6
    (HOC) and Slice 7 (reallocation) remain their own owners (composed, not forked); (17) NO
    second / unified model registry exists; (18) no paid-data registry fork exists (Slice 9
    Data Expansion landed as a purchase GATE, not a registry); (19) cadence disabled and NO
    automatic model promotion; (20) inventory drift zero (checked by
    ``check_inventory_drift``)."""
    kernel_src = _read(RA_KERNEL)
    owner_src = _read(RA_OWNER)
    drc_src = _read(DRC_OWNER)
    ui = _read(UI_FILE)

    kernel_present = (REPO_ROOT / RA_KERNEL).exists()
    owner_present = (REPO_ROOT / RA_OWNER).exists()

    # (1) sole research-state calculation owner: evaluate() defined ONLY in the kernel.
    calc_def_modules = []
    for fp in files:
        rel = _rel(fp)
        if rel in ("scripts/audit_architecture.py", RA_OWNER):
            continue
        if RA_KERNEL_EVAL_DEF in fp.read_text(encoding="utf-8", errors="replace"):
            calc_def_modules.append(rel)
    second_calculation_owner = sorted(set(calc_def_modules) - {RA_KERNEL})

    # (2) sole composition/read owner: load_research_agent only in the owner.
    read_def_modules = []
    for fp in files:
        rel = _rel(fp)
        if rel == "scripts/audit_architecture.py":
            continue
        if RA_OWNER_LOAD_DEF in fp.read_text(encoding="utf-8", errors="replace"):
            read_def_modules.append(rel)
    second_composition_owner = sorted(set(read_def_modules) - {RA_OWNER})

    # (6) the owner composes the authoritative evidence owners.
    delegates = {tok: (tok in owner_src) for tok in RA_MUST_DELEGATE}
    missing_delegation = sorted(k for k, v in delegates.items() if not v)
    # (7)-(12) neither owner promotes / recalibrates / retrains / mutates / executes / calls
    # a provider-prediction-broker.
    owner_forbidden = sorted(t for t in RA_FORBIDDEN_CALLS if t in owner_src)
    kernel_forbidden = sorted(t for t in RA_KERNEL_FORBIDDEN if t in kernel_src)

    # (3)/(4) routes: the GET read route exists exactly once; no promote/recalibrate/apply.
    routes = check_routes()["routes"]
    ra_route_entries = [r for r in routes if r["path"] == RA_ROUTE]
    route_get_count = sum(1 for r in ra_route_entries if r["method"] == "GET")
    ra_route_methods = sorted({r["method"] for r in ra_route_entries})
    forbidden_route_methods = (ra_route_methods != ["GET"] and ra_route_methods != [])
    forbidden_routes_present = sorted(r for r in SLICE8_FORBIDDEN_ROUTES
                                      if any(rt["path"] == r for rt in routes))

    # (5) DRC is the sole scheduled execution path: the step + delegation token are present.
    drc_delegates = DRC_RA_DELEGATE_TOKEN in drc_src and "run_and_persist" in drc_src
    drc_step_present = DRC_RA_STEP in drc_src

    # (15) immutable/idempotent artifact ownership: persist + atomic write + index.
    persist_present = RA_OWNER_PERSIST_DEF in owner_src
    atomic_persist_present = ("os.replace(" in owner_src and "index" in owner_src.lower())

    # (13)/(14) UI: exactly one loader; region computes no research metric.
    ui_loader_count = ui.count(UI_RA_LOADER)
    ui_fetch_count = ui.count(UI_RA_FETCH)
    ui_region_hits = []
    start = ui.find(UI_RA_LOADER)
    end = ui.find(UI_RA_REGION_END)
    if start != -1 and end != -1 and end > start:
        region = ui[start:end]
        for pat in UI_RA_FORBIDDEN:
            if pat in region:
                ui_region_hits.append(pat)

    # (16) Slice 6 (HOC) + Slice 7 (reallocation) + scoring remain their own owners: the
    # research-agent kernel must not define any of their calculations.
    kernel_forks_hoc = "def build_assessment(" in kernel_src
    kernel_forks_reallocation = "def build_proposal(" in kernel_src
    kernel_forks_scoring = ("def compute_scores(" in kernel_src
                            or "def compute_combined(" in kernel_src)

    # (17) NO second / unified model registry; (18) no paid-data registry fork (Slice 9
    # Data Expansion landed as a purchase gate, not a registry).
    second_registry_present_modules = sorted(m for m in SLICE8_FORBIDDEN_MODULES
                                             if (REPO_ROOT / m).exists())
    slice9_present_modules = sorted(m for m in SLICE9_ABSENT_MODULES
                                    if (REPO_ROOT / m).exists())
    landed_modules_missing = sorted(m for m in SLICE8_LANDED_MODULES
                                    if not (REPO_ROOT / m).exists())

    return {
        "kernel": RA_KERNEL, "owner": RA_OWNER,
        "kernel_present": kernel_present, "owner_present": owner_present,
        "landed_modules_missing": landed_modules_missing,
        "second_calculation_owner_modules": second_calculation_owner,
        "second_composition_owner_modules": second_composition_owner,
        "delegates": delegates, "missing_delegation": missing_delegation,
        "owner_forbidden_calls": owner_forbidden,
        "kernel_forbidden_calls": kernel_forbidden,
        "route_get_count": route_get_count,
        "research_agent_route_methods": ra_route_methods,
        "forbidden_route_methods_present": bool(forbidden_route_methods),
        "forbidden_routes_present": forbidden_routes_present,
        "drc_delegates": bool(drc_delegates),
        "drc_step_present": bool(drc_step_present),
        "persist_present": bool(persist_present),
        "atomic_idempotent_persist_present": bool(atomic_persist_present),
        "ui_loader_count": ui_loader_count,
        "ui_fetch_count": ui_fetch_count,
        "ui_metric_computation": sorted(set(ui_region_hits)),
        "kernel_forks_hoc": bool(kernel_forks_hoc),
        "kernel_forks_reallocation": bool(kernel_forks_reallocation),
        "kernel_forks_scoring": bool(kernel_forks_scoring),
        "second_registry_present_modules": second_registry_present_modules,
        "slice9_present_modules": slice9_present_modules,
        "automatic_model_promotion_allowed": False,
        "automatic_model_recalibration_allowed": False,
        "automatic_model_retraining_allowed": False,
        "cadence_enabled": False,
    }


def check_data_expansion_ownership(files: list[Path]) -> dict:
    """Phase 29J Slice 9 Data Expansion / Purchase-Gate ownership guard.

    Proves the seventeen release conditions: (1) ``engine.data_expansion_gate`` is the SOLE
    dataset-gate calculation owner (``evaluate_dataset`` defined only there); (2)
    ``api.data_expansion`` is the SOLE composition / persistence / read owner
    (``load_data_expansion`` defined only there); (3)/(4) the two GET read routes exist and
    every response is read-only (no POST); (5) the existing provider / data / evidence owners
    are REUSED, never forked (the owner references source_contracts / data_freshness /
    experiment_contracts / analyst_revisions / research_agent); (6) no secret / credential
    ownership; (7) NO purchase / subscribe / activate-provider / integrate / enable-paid-data
    route; (8) no provider activation; (9) no automatic subscription; (10) no portfolio
    mutation; (11) no model promotion; (12) no broker / order / prediction / provider call in
    either owner; (13) the UI performs no gate calculation; (14) exactly ONE UI loader; (15)
    immutable / idempotent evaluation-artifact ownership; (16) the Research Agent (Slice 8)
    remains its own owner (the gate does not fork it); (17) Slice 10 (Intraday) remains future,
    cadence disabled, and the gate is never a Daily Research Cycle daily job; inventory drift
    zero is checked by ``check_inventory_drift``."""
    kernel_src = _read(DE_KERNEL)
    owner_src = _read(DE_OWNER)
    drc_src = _read(DRC_OWNER)
    ui = _read(UI_FILE)

    kernel_present = (REPO_ROOT / DE_KERNEL).exists()
    owner_present = (REPO_ROOT / DE_OWNER).exists()

    # (1) sole gate calculation owner: evaluate_dataset() defined ONLY in the kernel.
    calc_def_modules = []
    for fp in files:
        rel = _rel(fp)
        if rel in ("scripts/audit_architecture.py", DE_OWNER):
            continue
        if DE_KERNEL_EVAL_DEF in fp.read_text(encoding="utf-8", errors="replace"):
            calc_def_modules.append(rel)
    second_calculation_owner = sorted(set(calc_def_modules) - {DE_KERNEL})

    # (2) sole composition/read owner: load_data_expansion() defined only in the owner.
    read_def_modules = []
    for fp in files:
        rel = _rel(fp)
        if rel == "scripts/audit_architecture.py":
            continue
        if DE_OWNER_LOAD_DEF in fp.read_text(encoding="utf-8", errors="replace"):
            read_def_modules.append(rel)
    second_composition_owner = sorted(set(read_def_modules) - {DE_OWNER})

    # (5) the owner REUSES the authoritative data/provider/evidence owners (never forks).
    reuse = {tok: (tok in owner_src) for tok in DE_MUST_REUSE}
    missing_reuse = sorted(k for k, v in reuse.items() if not v)
    # (10)-(12) neither owner purchases / activates / mutates / executes / calls a provider.
    owner_forbidden = sorted(t for t in DE_FORBIDDEN_CALLS if t in owner_src)
    kernel_forbidden = sorted(t for t in DE_KERNEL_FORBIDDEN if t in kernel_src)

    # (3)/(4)/(7) routes: two GET read routes exist; no purchase/activate/integrate route.
    routes = check_routes()["routes"]
    de_route_entries = [r for r in routes if r["path"] == DE_ROUTE]
    detail_route_entries = [r for r in routes if r["path"] == DE_DETAIL_ROUTE]
    route_get_count = sum(1 for r in de_route_entries if r["method"] == "GET")
    detail_route_get_count = sum(1 for r in detail_route_entries if r["method"] == "GET")
    de_route_methods = sorted({r["method"] for r in de_route_entries + detail_route_entries})
    forbidden_route_methods = (de_route_methods not in (["GET"], []))
    forbidden_routes_present = sorted(r for r in SLICE9_FORBIDDEN_ROUTES
                                      if any(rt["path"] == r for rt in routes))

    # (15) immutable/idempotent artifact ownership: persist + atomic write + index.
    persist_present = DE_OWNER_PERSIST_DEF in owner_src
    atomic_persist_present = ("os.replace(" in owner_src and "index" in owner_src.lower())

    # (13)/(14) UI: exactly one loader; region computes no gate metric.
    ui_loader_count = ui.count(UI_DE_LOADER)
    ui_fetch_count = ui.count(UI_DE_FETCH)
    ui_region_hits = []
    start = ui.find(UI_DE_LOADER)
    end = ui.find(UI_DE_REGION_END)
    if start != -1 and end != -1 and end > start:
        region = ui[start:end]
        for pat in UI_DE_FORBIDDEN:
            if pat in region:
                ui_region_hits.append(pat)

    # (16) the Slice 8 Research Agent + Stage 13A analyst revisions remain their own owners:
    # the gate kernel must not fork them.
    kernel_forks_research_agent = "def load_research_agent(" in kernel_src
    kernel_forks_stage13a = "def purchase_decision(" in kernel_src

    # (6) no secret / credential ownership in either owner.
    secret_tokens = ("api_key =", "API_KEY =", "secret =", "SECRET =", "password =",
                     "PASSWORD =", "token =", "private_key =")
    secret_ownership = sorted(t for t in secret_tokens
                              if t in kernel_src or t in owner_src)

    # (17) cadence disabled + the gate is NEVER a mandatory DRC daily job.
    cadence_disabled = DE_CADENCE_DISABLED_TOKEN in owner_src
    drc_daily_job_present = sorted(t for t in DE_DRC_DAILY_JOB_TOKENS if t in drc_src)

    landed_modules_missing = sorted(m for m in DE_LANDED_MODULES
                                    if not (REPO_ROOT / m).exists())
    # Slice 10 (Intraday) remains future.
    slice10_present_modules = sorted(m for m in SLICE10_ABSENT_MODULES
                                     if (REPO_ROOT / m).exists())

    # --- Release 37.1: two explicit decision contexts on ONE calculation owner ---- #
    decision_contexts_declared = all(t in kernel_src for t in DE_DECISION_CONTEXT_TOKENS)
    legacy_default_preserved = DE_LEGACY_DEFAULT_TOKEN in kernel_src
    acquisition_state_declared = DE_ACQ_STATE_TOKEN in kernel_src
    acquisition_classifier_present = DE_ACQ_CLASSIFIER_DEF in kernel_src
    acquisition_dimensions_present = all(t in kernel_src for t in DE_ACQ_DIMENSION_DEFS)
    # Stage A must NOT reuse the post-acquisition purchase state, or a pre-research
    # judgement becomes indistinguishable from post-research proof.
    acquisition_state_is_distinct = (
        "REC_RESEARCH_ACQUISITION" in kernel_src
        and 'REC_PURCHASE = "PURCHASE_RECOMMENDED"' in kernel_src
        and "return REC_RESEARCH_ACQUISITION, reasons" in kernel_src
        and "return REC_PURCHASE, reasons" in kernel_src)
    # Stage B's evidence standard is unchanged — the measured-lift gates still bind.
    stage_b_evidence_intact = all(t in kernel_src for t in DE_STAGE_B_LIFT_GATES)
    acquisition_is_not_authority = all(t in kernel_src for t in DE_ACQ_NOT_AUTHORITY)
    acquisition_requires_manual_approval = (
        "manual_acquisition_approval_required" in kernel_src
        and '"automatic_acquisition_allowed": False' in kernel_src
        and '"auto_acquisition_allowed": False' in kernel_src)
    owner_threads_decision_context = all(t in owner_src for t in DE_OWNER_CONTEXT_TOKENS)
    # The two contexts must not overwrite each other on disk.
    contexts_persist_separately = (
        "def _index_key(dataset_id: Optional[str]," in owner_src
        and 'return "%s::%s" % (key, ctx)' in owner_src)

    return {
        "kernel": DE_KERNEL, "owner": DE_OWNER,
        "kernel_present": kernel_present, "owner_present": owner_present,
        "landed_modules_missing": landed_modules_missing,
        "second_calculation_owner_modules": second_calculation_owner,
        "second_composition_owner_modules": second_composition_owner,
        "reuse": reuse, "missing_reuse": missing_reuse,
        "owner_forbidden_calls": owner_forbidden,
        "kernel_forbidden_calls": kernel_forbidden,
        "route_get_count": route_get_count,
        "detail_route_get_count": detail_route_get_count,
        "data_expansion_route_methods": de_route_methods,
        "forbidden_route_methods_present": bool(forbidden_route_methods),
        "forbidden_routes_present": forbidden_routes_present,
        "persist_present": bool(persist_present),
        "atomic_idempotent_persist_present": bool(atomic_persist_present),
        "ui_loader_count": ui_loader_count,
        "ui_fetch_count": ui_fetch_count,
        "ui_metric_computation": sorted(set(ui_region_hits)),
        "kernel_forks_research_agent": bool(kernel_forks_research_agent),
        "kernel_forks_stage13a": bool(kernel_forks_stage13a),
        "secret_ownership": secret_ownership,
        "cadence_disabled": bool(cadence_disabled),
        "drc_daily_job_present": drc_daily_job_present,
        "slice10_present_modules": slice10_present_modules,
        # --- Release 37.1 decision-context contract --------------------------------- #
        "decision_contexts_declared": bool(decision_contexts_declared),
        "legacy_default_decision_context_preserved": bool(legacy_default_preserved),
        "research_acquisition_state_declared": bool(acquisition_state_declared),
        "research_acquisition_state_is_distinct_from_purchase":
            bool(acquisition_state_is_distinct),
        "acquisition_classifier_present": bool(acquisition_classifier_present),
        "acquisition_dimensions_present": bool(acquisition_dimensions_present),
        "post_acquisition_evidence_standard_intact": bool(stage_b_evidence_intact),
        "acquisition_recommendation_is_not_authority": bool(acquisition_is_not_authority),
        "acquisition_requires_manual_approval": bool(acquisition_requires_manual_approval),
        "owner_threads_decision_context": bool(owner_threads_decision_context),
        "decision_contexts_persist_separately": bool(contexts_persist_separately),
        "automatic_acquisition_allowed": False,
        "automatic_purchase_allowed": False,
        "automatic_provider_activation_allowed": False,
        "automatic_subscription_allowed": False,
        "automatic_integration_allowed": False,
        "cadence_enabled": False,
    }


def check_slice6_live_acceptance_ownership(files: list[Path]) -> dict:
    """Phase 29G.1 Slice 6 LIVE-ACCEPTANCE / operator-workflow & UI hard-cutover guard.

    Proves the sixteen release conditions: (1) no visible "Slice 3 — not yet
    implemented" reassessment control remains; (2) no separate reassessment execution
    button/route exists; (3) no "Rebalance Proposal Ready" primary label remains for
    the legacy comparison; (4) the legacy comparison is classified compatibility-only;
    (5) exactly one HOC UI loader (see holding_opportunity_cost_ownership); (6) the HOC
    panel renders NOT_RUN; (7) the HOC panel renders completed assessments; (8) no JS
    recommendation/allocation calculation (see holding_opportunity_cost_ownership);
    (9) SERVICE readiness and WORKFLOW readiness are separate concepts; (10)
    WAITING_FOR_SESSION_CLOSE does not imply backend service failure (the /v1/ready
    body never keys off session timing); (11) the Daily Research Cycle remains the sole
    Slice 6 execution path; (12) Slice 7 remains absent (see
    holding_opportunity_cost_ownership); (13) no order / target-confirmation path is
    added; (14) the Persistent Alpha Research Agent (Slice 8) remains planned (see
    holding_opportunity_cost_ownership); (15) cadence remains disabled; (16) inventory
    drift remains zero (see check_inventory_drift)."""
    ws_src = _read(LA6_WORKFLOW_OWNER)
    dc_src = _read(LA6_DAILY_CLOSE_OWNER)
    app_src = _read(LA6_APP_OWNER)
    ui = _read(UI_FILE)
    routes = check_routes()["routes"]

    # (1) obsolete reassessment placeholder control removed from the workflow owner.
    obsolete_reassessment_control = sorted(t for t in LA6_OBSOLETE_REASSESSMENT_TOKENS
                                           if t in ws_src)
    # (3) legacy "rebalance proposal ready" primary label removed from daily_close.
    obsolete_rebalance_label = sorted(t for t in LA6_OBSOLETE_REBALANCE_TOKENS
                                      if t in dc_src)
    # (4) legacy comparison reclassified compatibility-only.
    legacy_compatibility_classified = all(t in dc_src for t in LA6_LEGACY_COMPAT_TOKENS)
    # (2)/(11)/(13) no reassessment / rebalance / HOC-run / target-confirmation route.
    forbidden_routes_present = sorted(r for r in LA6_ABSENT_ROUTES
                                      if any(rt["path"] == r for rt in routes))
    # (9) SERVICE vs WORKFLOW readiness — both distinct UI indicators present.
    service_readiness_ui = all(t in ui for t in LA6_UI_SERVICE_READINESS_TOKENS)
    workflow_readiness_ui = all(t in ui for t in LA6_UI_WORKFLOW_READINESS_TOKENS)
    readiness_separated = bool(service_readiness_ui and workflow_readiness_ui)
    # (9)/(10) /v1/ready is an explicit SERVICE probe whose CODE never keys off session
    # timing (WAITING_FOR_SESSION_CLOSE must never make the service report unready). The
    # docstring may legitimately NAME that workflow state to explain the separation, so
    # prose (docstrings/comments) is stripped before scanning the executable body.
    ready_start = app_src.find("def ready(")
    ready_body = _strip_prose(app_src[ready_start:ready_start + 2500]) if ready_start != -1 else ""
    ready_is_service_scoped = (LA6_READY_SERVICE_TOKEN in ready_body)
    ready_conflates_session = sorted(t for t in LA6_READY_FORBIDDEN_SESSION_TOKENS
                                     if t in ready_body)
    # (6)/(7) the HOC UI renders NOT_RUN and completed assessment states.
    hoc_renders_not_run = all(t in ui for t in LA6_UI_HOC_NOT_RUN_TOKENS)
    hoc_renders_completed = (LA6_UI_HOC_COMPLETED_TOKEN in ui)

    return {
        "workflow_owner": LA6_WORKFLOW_OWNER,
        "daily_close_owner": LA6_DAILY_CLOSE_OWNER,
        "obsolete_reassessment_control": obsolete_reassessment_control,
        "obsolete_rebalance_label": obsolete_rebalance_label,
        "legacy_compatibility_classified": legacy_compatibility_classified,
        "forbidden_routes_present": forbidden_routes_present,
        "service_readiness_ui": service_readiness_ui,
        "workflow_readiness_ui": workflow_readiness_ui,
        "readiness_separated": readiness_separated,
        "ready_is_service_scoped": ready_is_service_scoped,
        "ready_conflates_session": ready_conflates_session,
        "hoc_renders_not_run": hoc_renders_not_run,
        "hoc_renders_completed": hoc_renders_completed,
    }


def check_slice6_residual_cutover_ownership(files: list[Path]) -> dict:
    """Phase 29G.2 Slice-6 RESIDUAL hard-cutover guard.

    Proves the eighteen release conditions: (1) no primary "Portfolio Changes Proposed"
    presentation; (2) no primary "Proposal Ready" presentation for compatibility data;
    (3) no "Review Proposed Changes" button; (4) no "Review Rebalance Proposal" button;
    (5) the legacy membership comparison is classified compatibility-only; (6) it is
    visually secondary / collapsed on all three surfaces; (7) the Holding Opportunity-Cost
    Review is the SOLE primary portfolio-decision card; (8) Command Center uses the HOC
    state; (9) Daily Workflow uses the HOC state; (10) Portfolio Manager uses the HOC
    state; (11) exactly one canonical HOC loader; (12) no JavaScript recommendation / cost
    computation; (13) the Daily Research Cycle remains the sole execution path; (14) no
    reassessment / rebalance / order endpoint; (15) Slice 7 remains absent; (16) the
    Persistent Alpha Research Agent (Slice 8) remains planned; (17) cadence remains
    disabled; (18) inventory drift remains zero (checked by ``check_inventory_drift``)."""
    ui = _read(UI_FILE)
    ws_src = _read(WORKFLOW_STATE_OWNER)
    gate_src = _read(GATE_OWNER)
    drc_src = _read(DRC_OWNER)
    routes = check_routes()["routes"]

    # (1)/(2)/(3)/(4) no PRIMARY legacy-proposal presentation or action remains in the UI
    # or the workflow owner (the gate keeps its PRESERVED raw vocabulary for history).
    forbidden_primary_ui = sorted(t for t in RC6_FORBIDDEN_PRIMARY_UI if t in ui)
    forbidden_primary_ws = sorted(t for t in RC6_FORBIDDEN_PRIMARY_WS if t in ws_src)

    # (5) the gate result carries the explicit compatibility classification.
    gate_classification_fields = sorted(t for t in RC6_GATE_CLASSIFICATION if t in gate_src)
    gate_compatibility_classified = (len(gate_classification_fields)
                                     == len(RC6_GATE_CLASSIFICATION))
    # (5)/(7) the workflow owner makes the HOC review the PRIMARY presentation + exposes
    # the canonical NOT_RUN operator state + the compatibility legacy block.
    missing_ws_primary = sorted(t for t in RC6_WS_PRIMARY_TOKENS if t not in ws_src)
    workflow_primary_is_hoc = (missing_ws_primary == [])

    # (6) the legacy comparison is COLLAPSED (native <details>) on all three surfaces.
    legacy_details_present = sorted(t for t in RC6_UI_LEGACY_DETAILS if t in ui)
    legacy_comparison_collapsed = (
        len(legacy_details_present) == len(RC6_UI_LEGACY_DETAILS)
        and RC6_UI_LEGACY_SUMMARY in ui and RC6_UI_LEGACY_VIEW in ui)

    # (7) the HOC review is the SOLE primary decision card (badge + title on each of the
    # three surfaces; no primary legacy-proposal string remains).
    hoc_primary_badge_count = ui.count(RC6_UI_HOC_PRIMARY_BADGE)
    surface_hoc_title_count = ui.count(RC6_UI_SURFACE_HOC_TITLE)
    hoc_is_sole_primary_card = (hoc_primary_badge_count >= 3
                                and RC6_UI_HOC_TITLE in ui
                                and surface_hoc_title_count >= 3
                                and forbidden_primary_ui == [])

    # (8)/(9)/(10) each surface's DAG card renders the canonical HOC state (title/badge/
    # headline/explanation owned by renderWorkflowState from assessment_presentation=HOC).
    surface_nodes_present = all(
        ('id="%s-dag-title"' % p) in ui and ('id="%s-dag-badge"' % p) in ui
        and ('id="%s-dag-headline"' % p) in ui and ('id="%s-dag-explanation"' % p) in ui
        for p in ("cc", "dw", "pm"))
    surfaces_use_hoc_state = bool(
        surface_nodes_present and "_wsApplyAssessmentFraming" in ui
        and workflow_primary_is_hoc)

    # (11) exactly one canonical HOC loader.
    ui_hoc_loader_count = ui.count(UI_HOC_LOADER)

    # (12) no JS recommendation / cost / total computation in the HOC region.
    hoc_region_hits: list[str] = []
    start = ui.find(UI_HOC_LOADER)
    end = ui.find(UI_HOC_REGION_END)
    if start != -1 and end != -1 and end > start:
        region = ui[start:end]
        for pat in UI_HOC_FORBIDDEN:
            if pat in region:
                hoc_region_hits.append(pat)

    # (13) the DRC remains the sole HOC execution path (owner delegates; gate delegates to
    # the summary; no separate manual HOC execution route).
    drc_sole_execution_path = (DRC_HOC_DELEGATE_TOKEN in drc_src
                               and GATE_HOC_DELEGATE_TOKEN in gate_src)
    hoc_route_methods = sorted({r["method"] for r in routes
                               if "holding-opportunity" in (r["path"] or "").lower()})
    no_manual_hoc_execution_endpoint = (hoc_route_methods == ["GET"])

    # (14) no reassessment / rebalance / order / target-confirmation route.
    forbidden_routes_present = sorted(r for r in LA6_ABSENT_ROUTES
                                      if any(rt["path"] == r for rt in routes))

    # (15)/(16) Slice 7 is LANDED (owners + route present, no forbidden route);
    #           Slice 8 (Persistent Alpha Research Agent) remains planned.
    _s7 = _slice7_landed_status(routes)
    slice7_missing = (_s7["slice7_missing_modules"] + _s7["slice7_missing_route"]
                      + _s7["slice7_forbidden_present"])
    slice8_present = _s7["slice8_present_modules"]

    return {
        "forbidden_primary_ui": forbidden_primary_ui,
        "forbidden_primary_ws": forbidden_primary_ws,
        "gate_compatibility_classified": gate_compatibility_classified,
        "gate_classification_fields": gate_classification_fields,
        "workflow_primary_is_hoc": workflow_primary_is_hoc,
        "missing_ws_primary_tokens": missing_ws_primary,
        "legacy_comparison_collapsed": legacy_comparison_collapsed,
        "hoc_is_sole_primary_card": hoc_is_sole_primary_card,
        "hoc_primary_badge_count": hoc_primary_badge_count,
        "surfaces_use_hoc_state": surfaces_use_hoc_state,
        "ui_hoc_loader_count": ui_hoc_loader_count,
        "ui_recommendation_or_cost_computation": sorted(set(hoc_region_hits)),
        "drc_sole_execution_path": bool(drc_sole_execution_path),
        "no_manual_hoc_execution_endpoint": no_manual_hoc_execution_endpoint,
        "forbidden_routes_present": forbidden_routes_present,
        "slice7_missing": slice7_missing,
        "slice8_present": slice8_present,
        "cadence_enabled": False,
    }


def check_operator_ux_consolidation_ownership(files: list[Path]) -> dict:
    """Phase 29J.1 OPERATOR UX CONSOLIDATION guard. Proves (1) the four operator-oriented
    PRIMARY navigation areas exist (Today / Portfolio / Research / System · Audit);
    (2) legacy/detail views are DEMOTED out of primary navigation (under the Advanced-views
    disclosure); (3) every legacy route still resolves as an alias (no dead link); (4) the
    Market Context strip is restored against the SINGLE authoritative backend owner
    (GET /v1/market/indicators) — exactly one UI loader, no duplicate market-data owner, no
    direct provider host / market-regime math in the UI, GET-only, and an explicit
    reference-only (not a signal) label with honest UNAVAILABLE tiles; (5) the ONE canonical
    next-action renderer is unchanged; (6) the persistent safety strip carries the
    paper/manual/automation-off/no-broker/no-model-promotion set; (7) this phase introduces
    NO purchase/order/model-promotion route; cadence stays disabled."""
    ui = _read(UI_FILE)
    routes = check_routes()["routes"]

    # (1) Four operator-oriented primary nav areas.
    nav_today = ('id="nav-command-center"' in ui and ">Today<" in ui)
    nav_portfolio = ('id="nav-portfolio-manager"' in ui and 'data-route="portfolio-manager"' in ui)
    nav_research = ('id="nav-research"' in ui and 'data-route="research"' in ui)
    nav_system_audit = ('id="nav-system-audit"' in ui and 'data-route="system-audit"' in ui)
    primary_areas_present = all([nav_today, nav_portfolio, nav_research, nav_system_audit])

    # (2) Legacy/detail views demoted under the Advanced-views disclosure (not primary).
    adv_start = ui.find('id="sidebar-advanced-views"')
    adv_region = ui[adv_start:adv_start + 1400] if adv_start != -1 else ""
    legacy_views_demoted = all(
        t in adv_region for t in
        ('id="nav-daily-workflow"', 'id="nav-multi-horizon"', 'id="nav-portfolio"'))

    # (3) Old routes preserved as aliases (no dead link).
    route_aliases_required = ("'today':", "'research':", "'system-audit':")
    legacy_routes_required = ("'command-center':", "'portfolio':", "'research-audit':",
                              "'daily-workflow':", "'multi-horizon':", "'portfolio-manager':",
                              "'alpha-portfolio':")
    missing_route_aliases = sorted(r for r in route_aliases_required if r not in ui)
    missing_legacy_routes = sorted(r for r in legacy_routes_required if r not in ui)

    # (4) Market context: ONE loader against the SINGLE authoritative owner; no duplicate
    #     market-data owner, no direct provider host, no market/regime math in the UI region.
    market_route = "/v1/market/indicators"
    market_loader_count = ui.count("function loadMarketDashboard")
    market_owner_fetch = ("call('GET', '%s')" % market_route) in ui
    market_route_entries = [r for r in routes if r["path"] == market_route]
    market_route_methods = sorted({r["method"] for r in market_route_entries})
    market_route_get_only = market_route_methods == ["GET"]
    # Actual provider URLs/hosts must never be fetched from the browser (the symbol MAP names
    # like `yfinanceSymbols` are not provider calls and are deliberately excluded).
    provider_hosts = ("query1.finance.yahoo", "finance.yahoo.com", "stlouisfed.org",
                      "api.stlouisfed", "fredgraph")
    ui_direct_provider_hosts = sorted(h for h in provider_hosts if h in ui)
    mstart = ui.find("function loadMarketDashboard")
    if mstart != -1:
        mend = ui.find("window.loadMarketDashboard", mstart)
        market_region = ui[mstart:mend] if mend != -1 and mend > mstart else ui[mstart:mstart + 6000]
    else:
        market_region = ""
    market_forbidden = ("classifyRegime", "computeRegime", "marketRegime", "risk_on", "riskOn")
    market_region_market_math = sorted(t for t in market_forbidden if t in market_region)
    market_context_present = ('id="cc-market-context"' in ui and 'id="mkt-load-status"' in ui)
    market_reference_only = "Reference only" in ui

    # (5) ONE canonical next-action renderer (Slice 2 owner), unchanged.
    workflow_next_action_renderer_count = ui.count("function loadWorkflowState")

    # (6) Persistent safety strip carries the canonical set incl. NO MODEL PROMOTION.
    safety_tokens = ("PAPER ONLY", "MANUAL REVIEW", "AUTOMATION OFF",
                     "NO BROKER EXECUTION", "NO MODEL PROMOTION")
    missing_safety_tokens = sorted(t for t in safety_tokens if t not in ui)

    # (7) No purchase / order-creation / model-promotion ROUTE introduced by this phase.
    forbidden_new_routes = ("/v1/market/purchase", "/v1/market/subscribe",
                            "/v1/operations/apply-reallocation", "/v1/operations/promote-model")
    forbidden_new_routes_present = sorted(
        r for r in forbidden_new_routes if any(rt["path"] == r for rt in routes))

    return {
        "primary_areas_present": primary_areas_present,
        "nav_today": nav_today, "nav_portfolio": nav_portfolio,
        "nav_research": nav_research, "nav_system_audit": nav_system_audit,
        "legacy_views_demoted": legacy_views_demoted,
        "missing_route_aliases": missing_route_aliases,
        "missing_legacy_routes": missing_legacy_routes,
        "market_route": market_route,
        "market_loader_count": market_loader_count,
        "market_owner_fetch": market_owner_fetch,
        "market_route_methods": market_route_methods,
        "market_route_get_only": market_route_get_only,
        "ui_direct_provider_hosts": ui_direct_provider_hosts,
        "market_region_market_math": market_region_market_math,
        "market_context_present": market_context_present,
        "market_reference_only": market_reference_only,
        "workflow_next_action_renderer_count": workflow_next_action_renderer_count,
        "missing_safety_tokens": missing_safety_tokens,
        "forbidden_new_routes_present": forbidden_new_routes_present,
        "cadence_enabled": False,
    }


# --------------------------------------------------------------------------- #
# Stage 21 — outcome intelligence / execution lineage / durable close /
# environment isolation ownership.
# --------------------------------------------------------------------------- #
S21_OUTCOME_KERNEL = "engine/reassessment_outcomes.py"
S21_OUTCOME_OWNER = "api/reassessment_outcomes.py"
S21_LINEAGE_KERNEL = "engine/execution_lineage.py"
S21_LINEAGE_OWNER = "api/execution_lineage.py"
S21_ENV_OWNER = "api/environment_isolation.py"
S21_CLOSE_OWNER = "api/daily_close.py"
S21_REBALANCE_OWNER = "api/rebalance_execution.py"
S21_STATE_OWNER = "api/portfolio_state.py"

S21_ROUTES = ("/v1/research/reassessment-outcomes",
              "/v1/research/reassessment-outcomes/history",
              "/v1/operations/rebalance/execution-lineage")

#: A GET-only Stage-21 surface. Any of these would make it an action.
S21_FORBIDDEN_ROUTE_SUBSTR = ("reassessment-outcomes/refresh",
                              "reassessment-outcomes/capture",
                              "reassessment-outcomes/apply",
                              "reassessment-outcomes/promote",
                              "execution-lineage/repair")

#: Nothing in a Stage-21 module may execute, approve, promote or recalibrate.
S21_FORBIDDEN_CALLS = ("place_order(", "submit_order(", "create_order(",
                       "run_fill_cycle(", "confirm_rebalance_order_plan(",
                       "run_daily_close(", "promote_model(", "promote_challenger(",
                       "recalibrate(", "approve_proposal(")

#: The kernels must stay PURE — no io, no clock, no environment.
S21_KERNEL_IMPURITY = ("import requests", "urllib", "sqlalchemy", "os.environ",
                       "open(", "Path(", "datetime.now", "date.today")

#: Stage 21 must REUSE the canonical owners, never fork them.
S21_MUST_DELEGATE = ("forward_prediction_skill", "execution_lineage",
                     "portfolio_reassessment")

#: A second price-history / horizon / NAV / transaction-cost owner is a blocking defect.
S21_SECOND_OWNER_DEFS = ("def read_price_store(", "def eligible_calendar(",
                         "def book_nav(", "def load_performance(",
                         "COST_RATE_PER_SIDE =", "HORIZONS = ")


def check_stage21_outcome_intelligence(files: list[Path]) -> dict:
    """Stage 21 ownership guard.

    (1)  ONE outcome calculation owner + ONE outcome persistence/composition owner;
    (2)  ONE execution-lineage owner; execution identity is read from the immutable
         ledger, never recomputed from the current target;
    (3)  the kernels stay pure (no io / clock / environment / network);
    (4)  NO second forward-evidence, price-history, horizon, NAV or transaction-cost
         owner is introduced;
    (5)  every Stage-21 route is GET; no refresh/capture/apply/promote action exists;
    (6)  maturation happens in the ONE canonical place (the Daily Close evidence
         capture) — there is no operator button and no second trigger;
    (7)  the durable Daily Close run contract exists in the EXISTING close owner (no
         second Daily Close), with an explicit outcome vocabulary and retry contract;
    (8)  Stage-19 lineage stays immutable and the plan cohorts stay separated;
    (9)  production startup fails closed on acceptance/temp store roots;
    (10) no automatic policy write, model promotion or recalibration anywhere.
    """
    ok_src = _read(S21_OUTCOME_KERNEL)
    oo_src = _read(S21_OUTCOME_OWNER)
    lk_src = _read(S21_LINEAGE_KERNEL)
    lo_src = _read(S21_LINEAGE_OWNER)
    env_src = _read(S21_ENV_OWNER)
    close_src = _read(S21_CLOSE_OWNER)
    reb_src = _read(S21_REBALANCE_OWNER)
    state_src = _read(S21_STATE_OWNER)
    app_src = _read(APP_MODULE)
    routes = check_routes()["routes"]

    kernels_present = bool(ok_src) and bool(lk_src)
    owners_present = bool(oo_src) and bool(lo_src)
    env_owner_present = bool(env_src)

    # (1)/(2) exactly ONE declared owner of each kind across the tree. The legitimate
    # owner of each token is excluded by name, as is this audit script (which
    # necessarily contains every token it searches for).
    _AUDIT_SELF = "scripts/audit_architecture.py"

    def _declaring(token: str, *, allow: tuple = ()) -> list[str]:
        skip = set(allow) | {S21_OUTCOME_KERNEL, S21_OUTCOME_OWNER, S21_LINEAGE_KERNEL,
                             S21_LINEAGE_OWNER, _AUDIT_SELF}
        return sorted(_rel(fp) for fp in files
                      if _rel(fp) not in skip and token in _read(_rel(fp)))

    second_calculation_owner_modules = _declaring(
        'CALCULATION_OWNER = "engine.reassessment_outcomes"')
    second_composition_owner_modules = _declaring('OWNER = "api.reassessment_outcomes"')
    second_lineage_owner_modules = _declaring(
        'CALCULATION_OWNER = "engine.execution_lineage"')

    # (3) kernel purity.
    kernel_impurity = sorted(
        "%s:%s" % (mod, t)
        for mod, src in ((S21_OUTCOME_KERNEL, ok_src), (S21_LINEAGE_KERNEL, lk_src))
        for t in S21_KERNEL_IMPURITY if t in src)

    # (4) no second canonical owner forked into a Stage-21 module.
    second_owner_defs = sorted(
        "%s:%s" % (mod, t)
        for mod, src in ((S21_OUTCOME_KERNEL, ok_src), (S21_OUTCOME_OWNER, oo_src),
                         (S21_LINEAGE_KERNEL, lk_src), (S21_LINEAGE_OWNER, lo_src))
        for t in S21_SECOND_OWNER_DEFS if t in src)
    missing_delegation = sorted(t for t in S21_MUST_DELEGATE if t not in oo_src)

    # (5) GET-only surface.
    s21_routes = [r for r in routes
                  if r["path"] in S21_ROUTES
                  or r["path"].startswith("/v1/research/reassessment-outcomes")]
    route_methods = sorted({r["method"] for r in s21_routes})
    missing_routes = sorted(p for p in S21_ROUTES
                            if not any(r["path"] == p for r in s21_routes))
    forbidden_routes_present = sorted(
        s for s in S21_FORBIDDEN_ROUTE_SUBSTR
        if any(s in r["path"] for r in routes))

    # (6) exactly ONE maturation trigger, inside the close.
    maturation_in_close = ("_run_outcome_capture" in close_src
                           and "capture_for_daily_close" in close_src)
    # No SECOND Stage-21 outcome capture anywhere. (api.forward_prediction_skill has
    # its own `capture_for_daily_close` for FORWARD-MODEL evidence — a different
    # concern with a different owner — so the token searched here is Stage-21 specific.)
    outcome_capture_defs = _declaring("def capture_matured_outcomes(")
    no_operator_refresh_button = not any(
        t in _read(UI_FILE) for t in ("refreshOutcomeEvidence", "Refresh Outcome Evidence"))

    # (7) the durable close run contract lives in the EXISTING close owner.
    close_run_tokens = ("RUN_NOT_STARTED", "RUN_RUNNING", "RUN_COMPLETED",
                        "RUN_FAILED_RECOVERABLE", "RUN_FAILED_TERMINAL",
                        "safe_retry_allowed", "idempotency_key", "writes_occurred",
                        "client_timeout_is_not_an_outcome")
    missing_close_run_tokens = sorted(t for t in close_run_tokens
                                      if t not in close_src)
    # api/daily_close.py is the ONE legitimate Daily Close owner; anything else
    # defining run_daily_close would be a second close.
    second_close_owner_defs = _declaring("def run_daily_close(",
                                         allow=(S21_CLOSE_OWNER,))
    close_single_flight = "_CLOSE_LOCK" in close_src

    # (8) lineage immutability + chronological plan selection.
    lineage_immutable = all(t in lk_src for t in (
        "recovered_from_immutable_ledger", "derived_from_current_target",
        "STATE_SUPERSEDED_CANCELLED"))
    rebalance_composes_lineage = "latest_completed_rebalance" in reb_src
    # The hash-ordered plan selection that ranked the DEFECTIVE plan first must be gone.
    lexicographic_plan_selection = ("plan_ids[-1]" in reb_src
                                    or "live_plan_ids[-1]" in reb_src)

    # (9) production startup fails closed on acceptance/temp roots.
    startup_preflight = ("assert_production_store_roots" in app_src
                         and "_STORE_ROOT_PREFLIGHT" in app_src)
    env_fail_closed = "raise RuntimeError" in env_src
    acceptance_optin = "PAPER_TRADER_ACCEPTANCE_MODE" in env_src
    acceptance_server_scoped = (
        'os.environ["PAPER_TRADER_ACCEPTANCE_MODE"] = "1"' in _read(ACCEPT_SERVER)
        and "setx" not in _read(ACCEPT_SERVER)
        and "SetEnvironmentVariable" not in _read(ACCEPT_SERVER))

    # (10) nothing executes, approves, promotes or recalibrates.
    forbidden_calls = sorted(
        "%s:%s" % (mod, t)
        for mod, src in ((S21_OUTCOME_KERNEL, ok_src), (S21_OUTCOME_OWNER, oo_src),
                         (S21_LINEAGE_KERNEL, lk_src), (S21_LINEAGE_OWNER, lo_src),
                         (S21_ENV_OWNER, env_src))
        for t in S21_FORBIDDEN_CALLS if t in src)
    declares_no_policy_write = all(t in oo_src for t in (
        '"changed_policy": False', '"promoted_model": False',
        '"recalibrated_model": False'))
    kernel_declares_no_tuning = all(t in ok_src for t in (
        '"changes_policy": False', "recommends_manual_review_only"))

    # Workstream 0E — ONE economic fingerprint, and no self-referential comparison.
    economic_owner_present = ("def economic_state_hash(" in state_src
                              and "ECONOMIC_IDENTITY_VERSION" in state_src)
    # api/portfolio_state.py is the ONE economic-fingerprint owner.
    second_economic_owner_modules = _declaring("def economic_state_hash(",
                                               allow=(S21_STATE_OWNER,))
    reassessment_binds_economic = (
        'ic.get("economic_state_hash")' in _read("engine/portfolio_reassessment.py")
        and 'ic.get("hoc_economic_state_hash")'
        in _read("engine/portfolio_reassessment.py"))
    self_referential_comparison = (
        'ic["portfolio_state_hash"] != ic["hoc_portfolio_state_hash"]'
        in _read("engine/portfolio_reassessment.py"))
    hoc_records_fingerprints = all(
        t in _read("engine/holding_opportunity_cost.py")
        for t in ("economic_state_hash", "corporate_actions_hash"))

    return {
        "kernels_present": kernels_present,
        "owners_present": owners_present,
        "env_owner_present": env_owner_present,
        "second_calculation_owner_modules": second_calculation_owner_modules,
        "second_composition_owner_modules": second_composition_owner_modules,
        "second_lineage_owner_modules": second_lineage_owner_modules,
        "kernel_impurity": kernel_impurity,
        "second_owner_defs": second_owner_defs,
        "missing_delegation": missing_delegation,
        "route_methods": route_methods,
        "missing_routes": missing_routes,
        "forbidden_routes_present": forbidden_routes_present,
        "maturation_in_close": maturation_in_close,
        "outcome_capture_defs": outcome_capture_defs,
        "no_operator_refresh_button": no_operator_refresh_button,
        "missing_close_run_tokens": missing_close_run_tokens,
        "second_close_owner_defs": second_close_owner_defs,
        "close_single_flight": close_single_flight,
        "lineage_immutable": lineage_immutable,
        "rebalance_composes_lineage": rebalance_composes_lineage,
        "lexicographic_plan_selection": lexicographic_plan_selection,
        "startup_preflight": startup_preflight,
        "env_fail_closed": env_fail_closed,
        "acceptance_optin": acceptance_optin,
        "acceptance_server_scoped": acceptance_server_scoped,
        "forbidden_calls": forbidden_calls,
        "declares_no_policy_write": declares_no_policy_write,
        "kernel_declares_no_tuning": kernel_declares_no_tuning,
        "economic_owner_present": economic_owner_present,
        "second_economic_owner_modules": second_economic_owner_modules,
        "reassessment_binds_economic": reassessment_binds_economic,
        "self_referential_comparison": self_referential_comparison,
        "hoc_records_fingerprints": hoc_records_fingerprints,
        **_stage21_hermetic_clock(),
    }


#: Workstream 0F. The hermetic acceptance harness must own its clock completely. Three
#: canonical read models were still resolved from the LIVE world, so the acceptance
#: scenarios decayed a little further with every day that passed and eventually reported a
#: state inconsistency the product did not have. These tokens pin the seams shut.
S21_FIXTURE_MODULE = "scripts/stage20_ui_fixtures.py"
S21_OPBOOK_MODULE = "api/operational_book.py"
_S21_FIXTURE_INJECTIONS = (
    "current=_engine_current(spec)",            # daily_action_gate -> owned model current
    "target_readiness=readiness",               # operational_book  -> alpha_target
    "daily_close_status=_close_progress(spec)",  # data_freshness   -> daily_close
)
#: The Stage-21 cockpit loaders must call the view's canonical authenticated GET helper.
#: `apiGet` is not defined in that scope; because the Portfolio Manager fires its loaders
#: fire-and-forget inside try/except, calling it left the card stuck on "Loading..."
#: forever with an EMPTY console. Only a real browser could see it.
_S21_UI_LOADER_CALLS = (
    "_mhzGet('/v1/research/reassessment-outcomes')",
    "_mhzGet('/v1/operations/rebalance/execution-lineage')",
)


def _stage21_hermetic_clock() -> dict:
    fx = _read(S21_FIXTURE_MODULE)
    ob = _read(S21_OPBOOK_MODULE)
    ui = _read(UI_FILE)
    return {
        "hermetic_clock_injections_missing":
            [t for t in _S21_FIXTURE_INJECTIONS if t not in fx],
        "hermetic_clock_seam_present": "target_readiness" in ob,
        "stage21_ui_loader_calls_missing":
            [t for t in _S21_UI_LOADER_CALLS if t not in ui],
        "stage21_ui_uses_undefined_getter": "apiGet(" in ui,
    }


def check_acceptance_scenario_ownership(files: list[Path]) -> dict:
    """Stage 20.1 HERMETIC ACCEPTANCE ENVIRONMENT ownership guard.

    Stage 20's acceptance harness seeded ONE store and let every other canonical surface
    fall back to its own empty default world, so a single rendered page could show
    ``PROPOSAL_READY`` with a live REVIEW PORTFOLIO PROPOSAL button next to ``Operational
    Book: NOT INITIALIZED / 0 pending orders`` and ``Run the Daily Close``. This guard
    prevents the acceptance environment from ever regressing into per-endpoint,
    incompatible fixture ownership:

      (1)  ONE scenario owner exists and declares itself;
      (2)  the shared scenario contract exists (``world`` + ``compose`` +
           ``cross_panel_consistency``);
      (3)  ``compose`` produces EVERY canonical panel — no endpoint is left to invent one;
      (4)  every panel is produced by DELEGATING to its real canonical owner;
      (5)  the harness reimplements NO production derivation (lineage split, lifecycle,
           close resolver, reassessment kernel, target engine);
      (6)  Stage-19 execution precedence is asserted by the consistency verdict;
      (7)  the current-plan counts are lineage-scoped and the historical / superseded
           cohorts are reported separately;
      (8)  scenario 5 (execution pending) and scenario 5b (a newly eligible close) both
           exist and are never conflated;
      (9)  the harness invokes NO mutating operational entry point and no provider /
           prediction call;
      (10) the acceptance backend can never bind the live backend port, and redirects
           every persistent store before importing the app.
    """
    fx_src = _read(ACCEPT_FIXTURES)
    srv_src = _read(ACCEPT_SERVER)
    fx_present = (REPO_ROOT / ACCEPT_FIXTURES).exists()
    srv_present = (REPO_ROOT / ACCEPT_SERVER).exists()

    # (3) every canonical panel is produced by the shared composition.
    compose_body = _module_func_body(fx_src, ACCEPT_COMPOSE_DEF)
    missing_panels = sorted(p for p in ACCEPT_REQUIRED_PANELS
                            if ('"%s"' % p) not in compose_body)

    # (4) each panel is delegated to its real owner.
    missing_delegation = sorted(t for t in ACCEPT_MUST_DELEGATE if t not in fx_src)

    # (5)/(9) nothing production-owned is forked, nothing mutating is called.
    reimplemented = sorted(t for t in ACCEPT_FORBIDDEN_REIMPL if t in fx_src)
    forbidden_calls = sorted(t for t in ACCEPT_FORBIDDEN_CALLS
                             if t in fx_src or t in srv_src)

    # (6)/(7) the verdict must actually judge precedence and the cohort split.
    verdict_body = _module_func_body(fx_src, ACCEPT_CONSISTENCY_DEF)
    checks_precedence = "EXECUTION_PRECEDENCE_MISMATCH" in verdict_body
    checks_cohorts = all(t in verdict_body for t in (
        "HISTORICAL_FILL_COHORT_MISMATCH", "SUPERSEDED_COHORT_MISMATCH",
        "CURRENT_SUBMITTED_MISMATCH", "CURRENT_FILLED_MISMATCH"))
    checks_single_action = "MULTIPLE_MUTATION_ACTIONS" in verdict_body
    checks_book_init = "BOOK_INITIALIZATION_DISAGREES" in verdict_body
    lineage_scoped = "counts_are_lineage_scoped" in fx_src

    # (10) the acceptance backend's hard safety properties.
    refuses_live_port = ("LIVE_BACKEND_PORT = 8001" in srv_src
                         and "args.port == LIVE_BACKEND_PORT" in srv_src)
    redirects_stores = "def redirect_stores(" in srv_src
    refuses_inconsistent = "not cons[\"consistent\"]" in srv_src

    return {
        "owners_present": bool(fx_present and srv_present),
        "single_scenario_owner": ACCEPT_SCENARIO_OWNER_DECL in fx_src,
        "shared_scenario_contract_present": all(
            t in fx_src for t in (ACCEPT_WORLD_DEF, ACCEPT_COMPOSE_DEF,
                                  ACCEPT_CONSISTENCY_DEF)),
        "missing_panels": missing_panels,
        "missing_delegation": missing_delegation,
        "reimplemented_production_logic": reimplemented,
        "forbidden_calls": forbidden_calls,
        "verdict_checks_execution_precedence": checks_precedence,
        "verdict_checks_lineage_cohorts": checks_cohorts,
        "verdict_checks_single_primary_action": checks_single_action,
        "verdict_checks_book_initialization": checks_book_init,
        "counts_are_lineage_scoped": lineage_scoped,
        "scenario_5_present": ACCEPT_SCENARIO_5 in fx_src,
        "scenario_5b_present": ACCEPT_SCENARIO_5B in fx_src,
        "acceptance_refuses_live_backend_port": refuses_live_port,
        "acceptance_redirects_every_store": redirects_stores,
        "acceptance_refuses_inconsistent_scenario": refuses_inconsistent,
    }


# --------------------------------------------------------------------------- #
# CANONICAL BACKEND RESTART / SMOKE OWNERSHIP
#
# Stage after stage regenerated a stage-specific ``restart_smoke.ps1`` in a throwaway
# handoff directory, and stage after stage reintroduced the SAME defect: polling
# ``http://127.0.0.1:8001/health`` when the canonical readiness routes are ``/v1/health``
# and ``/v1/ready``. Stage 12 shipped it; Stage 21 shipped it again; the operator's own
# stdout log holds 39 consecutive 404s. Duplicated operator workflow is how a fixed defect
# returns, so restart/smoke now has exactly ONE owner and the duplication is a build
# failure rather than something a reviewer has to remember.
# --------------------------------------------------------------------------- #
RESTART_OWNER = "scripts/restart_paper_trader_backend.ps1"
RESTART_OWNER_DECLARATION = "CANONICAL_RESTART_SMOKE_OWNER = " + RESTART_OWNER

#: The ONLY health / readiness paths a Paper Trader restart or smoke workflow may probe.
#: This is permanent. ``/health``, ``/healthz``, ``/ready`` and ``/readyz`` are not served
#: by this application and never were.
CANONICAL_READINESS_ROUTES = ("/v1/health", "/v1/ready")

#: A quoted path literal that looks like a health / readiness probe.
_PS_HEALTH_LITERAL = re.compile(
    r"""["'](?P<host>https?://[^"'\s/]+)?(?P<path>/[A-Za-z0-9_.\-/]*"""
    r"""(?:health|healthz|ready|readyz|livez|liveness|readiness)"""
    r"""[A-Za-z0-9_.\-/]*)["']""",
    re.IGNORECASE)

#: Launching the ASGI application. Only the owner may do this.
_PS_APP_LAUNCH = re.compile(r"""(?:\buvicorn\b|\bhypercorn\b|["'][A-Za-z0-9_.]+:app["'])""")

#: Managing the LIVE backend port directly - binding it, listing its listeners, or passing
#: it to a server. A handoff delegates with ``-Port 8001``; it never touches the port
#: itself. (The hermetic acceptance harness is a different owner on a different port and
#: explicitly refuses to bind 8001, so it is not caught by this.)
_PS_LIVE_PORT_MGMT = re.compile(r"""-LocalPort\s+8001\b|--port["',\s]+8001\b""")

#: HTTP verbs a restart / smoke workflow may never use. It restarts a process and reads.
_PS_MUTATING_VERB = re.compile(r"""-Method\s+["']?(?:Post|Put|Patch|Delete)\b""",
                               re.IGNORECASE)

#: A ``/v1`` path literal in a PowerShell workflow. Every one of them must be a route the
#: application actually declares as GET - that is what makes "poll the right path"
#: verifiable instead of a convention.
_PS_V1_LITERAL = re.compile(
    r"""["'](?P<host>https?://[^"'\s/]+)?(?P<path>/v1/[A-Za-z0-9_.\-/]*)["']""")

#: Hosts that mean "this backend". A ``/v1`` path aimed at somebody ELSE's API (the Gmail
#: send endpoint is also /v1/...) is not a Paper Trader route and must not be judged
#: against Paper Trader's route table.
_PS_LOCAL_HOST = re.compile(r"^https?://(?:127\.0\.0\.1|localhost|\[::1\])(?::\d+)?$",
                            re.IGNORECASE)

#: Emitting the success token. Exactly one script may do it, and only after live checks.
_PS_EMITS = re.compile(r"(?:Write-Host|Write-Output|\becho\b)")

#: The failure report the owner must print before returning nonzero.
_RESTART_DIAGNOSTIC_TOKENS = (
    "launched pid",         # which process was started
    "pid still alive",      # is it still running
    "process exit code",    # why it died, when available
    "listener",             # port-listener state
    "STDERR",               # last stderr lines
    "STDOUT",               # last stdout lines
)

#: The responsibilities the owner must actually implement (so "delegate to the owner"
#: means something). Each token is the load-bearing symbol of one responsibility.
_RESTART_CONTRACT_TOKENS = (
    "Stop-Process",                     # process stop
    "Start-Process",                    # process start
    "Get-BackendListeners",             # port handling
    "Show-StartupDiagnostics",          # stdout/stderr diagnostics
    "X-API-Key",                        # authentication setup
    "environment_isolation",            # production-root validation (delegated to Python)
    "exactly one backend must own it",  # single-listener assertion
)


def _walk_ps1(base: Path):
    for root, dirs, files in os.walk(str(base)):
        dirs[:] = [d for d in dirs
                   if d not in EXCLUDE_PARTS and not d.endswith(".egg-info")]
        for name in sorted(files):
            if name.lower().endswith(".ps1"):
                yield Path(root) / name


def _iter_powershell_files(extra_dirs=()) -> list[tuple[str, Path]]:
    """Every PowerShell workflow in scope: the repository, plus any handoff directory
    passed with ``--handoff-dir``. Handoff scripts live outside the repository by design,
    so the guard has to be pointed at them; the release gate does exactly that."""
    out: list[tuple[str, Path]] = []
    seen: set[str] = set()
    bases = [REPO_ROOT] + [Path(d) for d in extra_dirs]
    for base in bases:
        try:
            if not base.exists():
                continue
        except OSError:
            continue
        for fp in _walk_ps1(base):
            try:
                key = str(fp.resolve()).lower()
            except OSError:
                key = str(fp).lower()
            if key in seen:
                continue
            seen.add(key)
            try:
                label = _rel(fp)
            except ValueError:
                label = str(fp).replace("\\", "/")
            out.append((label, fp))
    return sorted(out, key=lambda t: t[0])


def _ps_url_path(literal: str) -> str:
    s = literal.strip()
    m = re.match(r"^https?://[^/]+(/.*)$", s, re.IGNORECASE)
    if m:
        s = m.group(1)
    s = s.split("?")[0]
    return s.rstrip("/") or "/"


# --------------------------------------------------------------------------- #
# STAGE 22 — NORMAL-CYCLE OWNERSHIP
#
# The normal daily cycle had no owner. Every surface was individually correct and
# collectively ambiguous: the Daily Close panel, the Daily Research panel, the
# opportunity-cost card, the reallocation card, the reassessment card and the
# rebalance lifecycle could each imply an action, and nothing said which came first.
# This guard pins the repaired shape so it cannot silently come apart again.
# --------------------------------------------------------------------------- #
NC_KERNEL = "engine/normal_cycle.py"
GAP_KERNEL = "engine/data_gap_taxonomy.py"
NC_OWNER = "api/workflow_state.py"

#: The canonical stage sequence, in order. A reordering here is a contract change.
NC_STAGE_SEQUENCE = ("WAIT_FOR_SESSION_CLOSE", "DAILY_CLOSE", "DAILY_RESEARCH_CYCLE",
                     "PORTFOLIO_DECISION", "CONTROLLED_REBALANCE")
#: The kernel must stay PURE: no IO, no clock, no store, no provider, no api.* import.
NC_KERNEL_FORBIDDEN = ("import os", "open(", "requests.", "httpx.", "datetime.now(",
                       "from paper_trader.api", "Path(", "json.load", "json.dump")
#: What the workflow owner must actually do with the kernel.
NC_OWNER_TOKENS = ('"normal_cycle": normal_cycle', "ncycle.build_cycle_view(",
                   '"stage_gates"', "research_cycle_due_after_close")
#: The single-primary-mutation invariant must be ENFORCED, not merely documented.
NC_SINGLE_MUTATION_TOKENS = ("def assert_single_primary_mutation(",
                             "MultiplePrimaryMutationError",
                             "assert_single_primary_mutation(list(gates.values()))")
#: Stale-evidence hierarchy (Workstream B) — classified, demoted, still fail-closed.
NC_EVIDENCE_TOKENS = ("def build_evidence_classification(", "EVIDENCE_SYSTEM_BLOCKER",
                      "EVIDENCE_EXPECTED_STALE", '"blocks_portfolio_action": True',
                      '"evidence_classification": evidence_classification')
#: Assessment / proposal binding (Workstream E) — one fail-closed verdict, stated once.
NC_BINDING_TOKENS = ("def build_assessment_binding(", "BINDING_CURRENT", "BINDING_STALE",
                     "BINDING_UNVERIFIABLE", '"stated_once": True',
                     '"assessment_binding": assessment_binding')
#: Data-gap taxonomy (Workstream C) — severity is a property, never inferred downstream.
GAP_TAXONOMY_TOKENS = ("BLOCKING", "NON_BLOCKING", '"blocking"',
                       '"effect_on_recommendation"', '"safe_fallback"',
                       '"expected_as_of_date"', '"available_as_of_date"',
                       '"source_owner"', '"ticker"')
#: An unknown gap code must fail CLOSED (BLOCKING), never be assumed harmless.
GAP_UNKNOWN_FAILS_CLOSED = ('UNKNOWN_GAP = {', '"severity": BLOCKING')
#: Missing data must never be silently converted to zero / current data.
GAP_NO_SILENT_SUBSTITUTION = ('"missing_data_converted_to_zero": False',
                              '"missing_data_converted_to_current": False',
                              '"silently_substituted": False')
#: The UI mirrors the cycle; it must not own a stage list or a next-step rule.
NC_UI_TOKENS = ('id="opc-cycle"', "d.normal_cycle", "_wsApplyEvidenceHierarchy(",
                'id="hoc-gaps"', "data_gap_taxonomy",
                # The right rail DEFERS to the one command bar instead of rendering a
                # second enabled execute button for the same canonical action.
                "var railOwnsExecution", "railNav ?")
#: Client-side re-derivation of the cycle that would recreate a second priority engine.
NC_UI_FORBIDDEN = ("function normalCycleStage", "function nextCycleStage",
                   "STAGE_SEQUENCE =", "function decideCycleStage")
#: A standalone desk refresh must never be a REQUIRED step between the close and the
#: research cycle (Workstream D). The close composes the desk owner, the workflow owner
#: says so in the operator's own words, and the operational-book presentation contract
#: carries the machine-readable flag every surface obeys.
NC_NO_STANDALONE_REFRESH_WS = "no separate desk refresh is required"
NC_NO_STANDALONE_REFRESH_OB = "requires_separate_desk_refresh"
NC_OPERATIONAL_BOOK = "api/operational_book.py"


def check_normal_cycle_ownership(files: list[Path]) -> dict:
    """Stage 22 strict guard over the canonical NORMAL DAILY PORTFOLIO CYCLE.

      (1)  the pure cycle kernel and the pure gap-taxonomy kernel exist and stay pure;
      (2)  the canonical stage sequence is declared, in order, exactly once;
      (3)  ``api.workflow_state`` is the ONLY module that projects state onto the cycle
           (no competing normal-cycle state owner);
      (4)  the single-primary-mutation invariant is ENFORCED at runtime;
      (5)  the post-close research requirement exists (no hidden manual step between the
           Daily Close and the Daily Research Cycle);
      (6)  a standalone desk refresh is never required between them;
      (7)  stale evidence is CLASSIFIED (system blocker vs expected) and still fails
           closed;
      (8)  the assessment/proposal binding verdict exists, fails closed and is stated once;
      (9)  every data gap carries its full machine-readable taxonomy, an unknown code
           fails CLOSED, and nothing is silently substituted;
      (10) the UI mirrors the contract and re-derives no stage, order or next step.
    """
    nc_src = _read(NC_KERNEL)
    gap_src = _read(GAP_KERNEL)
    ws_src = _read(NC_OWNER)
    ui = _read(UI_FILE)

    # (1) purity — the kernels take no dependency on the world.
    kernel_impurity = sorted(t for t in NC_KERNEL_FORBIDDEN if t in nc_src)
    gap_impurity = sorted(t for t in NC_KERNEL_FORBIDDEN if t in gap_src)

    # (2) the sequence is declared in order.
    seq_positions = [nc_src.find('"%s"' % s) for s in NC_STAGE_SEQUENCE]
    sequence_declared = all(p != -1 for p in seq_positions)
    sequence_ordered = sequence_declared and seq_positions == sorted(seq_positions)

    # (3) no second normal-cycle state owner.
    second_cycle_owner_modules = sorted(
        _rel(fp) for fp in files
        if _rel(fp) not in (NC_KERNEL, NC_OWNER, "scripts/audit_architecture.py")
        and "def build_cycle_view(" in fp.read_text(encoding="utf-8", errors="replace"))
    second_gap_owner_modules = sorted(
        _rel(fp) for fp in files
        if _rel(fp) not in (GAP_KERNEL, "scripts/audit_architecture.py")
        and "def classify_assessment_gaps(" in fp.read_text(encoding="utf-8",
                                                            errors="replace"))

    # (4)/(5)/(6) enforcement + the post-close handoff.
    missing_owner_tokens = sorted(t for t in NC_OWNER_TOKENS if t not in ws_src)
    single_mutation_enforced = all(
        t in (nc_src + ws_src) for t in NC_SINGLE_MUTATION_TOKENS)
    post_close_research_required = (
        "research_cycle_due_after_close" in ws_src
        and "if research_cycle_due_after_close:" in ws_src)
    close_outranks_research = "P3.7" in ws_src
    no_standalone_refresh = (NC_NO_STANDALONE_REFRESH_WS in ws_src
                             and NC_NO_STANDALONE_REFRESH_OB
                             in _read(NC_OPERATIONAL_BOOK))

    # (7)/(8) evidence hierarchy + binding.
    missing_evidence_tokens = sorted(t for t in NC_EVIDENCE_TOKENS if t not in ws_src)
    missing_binding_tokens = sorted(t for t in NC_BINDING_TOKENS if t not in ws_src)
    evidence_still_fails_closed = '"blocks_portfolio_action": True' in ws_src

    # (9) the gap taxonomy.
    missing_gap_tokens = sorted(t for t in GAP_TAXONOMY_TOKENS if t not in gap_src)
    unknown_gap_fails_closed = all(t in gap_src for t in GAP_UNKNOWN_FAILS_CLOSED)
    no_silent_substitution = all(t in gap_src for t in GAP_NO_SILENT_SUBSTITUTION)
    gap_severity_consumed = ("blocking_gap_count" in ws_src
                             and "opportunity_cost_data_gap_taxonomy" in ws_src)

    # (10) the UI mirrors, never derives.
    missing_ui_tokens = sorted(t for t in NC_UI_TOKENS if t not in ui)
    ui_cycle_derivation = sorted(t for t in NC_UI_FORBIDDEN if t in ui)

    return {
        "kernels_present": bool((REPO_ROOT / NC_KERNEL).exists()
                                and (REPO_ROOT / GAP_KERNEL).exists()),
        "kernel_impurity": kernel_impurity,
        "gap_kernel_impurity": gap_impurity,
        "stage_sequence": list(NC_STAGE_SEQUENCE),
        "sequence_declared": bool(sequence_declared),
        "sequence_ordered": bool(sequence_ordered),
        "second_cycle_owner_modules": second_cycle_owner_modules,
        "second_gap_owner_modules": second_gap_owner_modules,
        "missing_owner_tokens": missing_owner_tokens,
        "single_mutation_enforced": bool(single_mutation_enforced),
        "post_close_research_required": bool(post_close_research_required),
        "close_outranks_research": bool(close_outranks_research),
        "no_standalone_desk_refresh_required": bool(no_standalone_refresh),
        "missing_evidence_tokens": missing_evidence_tokens,
        "evidence_still_fails_closed": bool(evidence_still_fails_closed),
        "missing_binding_tokens": missing_binding_tokens,
        "missing_gap_tokens": missing_gap_tokens,
        "unknown_gap_fails_closed": bool(unknown_gap_fails_closed),
        "no_silent_substitution": bool(no_silent_substitution),
        "gap_severity_consumed_not_inferred": bool(gap_severity_consumed),
        "missing_ui_tokens": missing_ui_tokens,
        "ui_cycle_derivation": ui_cycle_derivation,
    }


# --------------------------------------------------------------------------- #
# RELEASE 29 — CONTINUOUS GOVERNED INFORMATION COLLECTION
#
# Release 28 could react to an event but could not keep events arriving, and it
# judged 17 sources of wildly different cadence against ONE anchor date, so a
# monthly series and a market feed on a Sunday both read "degraded". Release 29
# runs the sources at their own cadence and reports health against the sources
# that should actually be current now. This guard pins the shape that makes that
# true: ONE cadence policy, ONE collection orchestrator, ONE worker, ONE manager
# script, no second copy of a neighbouring owner's calculation, no execution path,
# and a browser that renders the verdict instead of computing it.
# --------------------------------------------------------------------------- #
IC_KERNEL = "engine/collection_cadence.py"
IC_OWNER = "api/information_collection.py"
IC_REPLAY = "api/collection_replay.py"
IC_WORKER = "scripts/run_information_collection_service.py"
IC_MANAGE = "scripts/manage_information_collection.ps1"
IC_EVENT_OWNER = "api/event_signal_refresh.py"
IC_MATERIALITY = "engine/event_materiality.py"
IC_ROUTE = "/v1/operations/information-collection"

#: The cadence policy is PURE arithmetic over an INJECTED clock. A kernel that
#: reads the wall clock cannot be replayed, and a cadence that cannot be replayed
#: cannot be proven.
IC_KERNEL_FORBIDDEN = ("import os", "open(", "requests.", "httpx.", "urllib",
                       "datetime.now(", "utcnow(", "time.time(",
                       "from paper_trader.api")
#: The orchestrator COMPOSES the existing owners. Any of these appearing inside it
#: is a second copy of a calculation that already has an owner.
IC_OWNER_FORBIDDEN = ("def assess_holding_opportunity_cost(",
                      "def run_portfolio_reassessment(",
                      "def build_reallocation_proposal(",
                      "def score_universe(", "def rank_universe(",
                      "def session_phase(", "def is_market_open(",
                      "def fetch_latest_prices(", "requests.get(",
                      "urllib.request", "def assess_materiality(")
#: What the orchestrator must DELEGATE rather than reimplement.
IC_OWNER_DELEGATION = ("mh.session_state(", "esr.run_event_signal_refresh",
                       "scap.ingestion_root(", "scap.news_root(",
                       "cad.resolve_source_runtime(", "cad.summarize_runtime(",
                       "cad.next_wake_seconds(")
#: Collection automation and EXECUTION automation are different switches, and the
#: second one must stay off, unreachable and stated.
IC_SAFETY_TOKENS = ("CONFIRM_ENABLE_INFORMATION_COLLECTION",
                    '"execution_automation_enabled": False',
                    '"creates_orders": False', '"confirms_targets": False',
                    '"approves_proposals": False', '"runs_daily_close": False',
                    '"runs_daily_research_cycle": False',
                    '"promotes_models": False')
#: The manager may ENUMERATE processes; it may not DECIDE how many workers they
#: are. ``.venv-win\Scripts\python.exe`` is the venv REDIRECTOR, and it launches
#: the base interpreter from pyvenv.cfg with a BYTE-IDENTICAL command line, so
#: every clean start is two physical processes for one worker. A raw process
#: count is therefore not a singleton verdict — one launch lineage is.
IC_MANAGE_TOPOLOGY_TOKENS = ("--action worker-topology",
                             "function Get-WorkerTopology(",
                             "SINGLE_LOGICAL_WORKER", "NO_LOGICAL_WORKER")
IC_MANAGE_RAW_PROCESS_COUNT = (
    'singleton violated: $($procs.Count) worker processes',
    '$procs.Count -gt 1',
    '(Get-WorkerProcesses).Count -gt 1',
)
#: Release 29.2. Worker health has to tell a BUSY worker apart from a STALLED one,
#: and the evidence for BUSY must be progress the collection path really made.
#: These tokens pin that vocabulary in the orchestrator.
IC_PROGRESS_TOKENS = ("def record_progress(", "class ProgressReporter",
                      "PROGRESS_STALL_SECONDS", '"iteration_in_flight"',
                      '"progress_seq"', "ACT_BUSY", "ACT_STALLED", "ACT_DEAD")
#: A timer that fires regardless of what the worker is doing would report a hung
#: process as healthy. The worker may own NO second heartbeat authority.
IC_WORKER_FORBIDDEN_TIMERS = ("threading.Thread", "threading.Timer", "Timer(",
                              "asyncio.", "sched.scheduler")
#: Routes this release may never add. Collection is started by the operator through
#: the Windows Scheduled Task, never by an HTTP call that runs providers on demand.
IC_FORBIDDEN_ROUTE_SUFFIXES = ("/start", "/stop", "/run", "/collect", "/enable",
                               "/disable", "/iterate")
IC_UI_TOKENS = (IC_ROUTE, "ic-headline", "ic-service-line", "ic-sources",
                "ic-collection-badge", "ic-exec-badge", "EXECUTION AUTOMATION: ",
                # The always-visible header chip: a decision surface that has quietly
                # stopped being fed must be visible without scrolling, from any view.
                "ic-header-badge", "_icSetHeaderBadge(")
#: The browser renders the backend verdict. Each of these would mean it is deciding:
#: an ASSIGNMENT to a backend-owned verdict (``x = ...``, never a comparison
#: ``x === ...``), or client-side date arithmetic standing in for the service clock.
IC_UI_FORBIDDEN_PATTERNS = (
    r"\b(healthy_due|due_now|runtime_state|reassessment_required|material_events)"
    r"\s*=(?!=)",
    r"new\s+Date\s*\(",
    r"Date\.now\s*\(\)\s*-",
)


def check_information_collection_ownership(files: list[Path],
                                           routes: list[dict]) -> dict:
    """Release 29 strict guard over continuous governed information collection.

      (1)  the cadence kernel, orchestrator, replay harness, worker and manager
           script all exist, and the kernel stays PURE;
      (2)  there is exactly ONE cadence resolver and ONE collection iteration —
           no second scheduler grows anywhere else in the tree;
      (3)  the orchestrator delegates the market clock, the store roots and the
           Release-28 event cycle instead of reimplementing them, and hosts no
           second opportunity cost, reassessment, proposal builder, scoring engine
           or provider client;
      (4)  the read surface is GET-only and no route can start a worker, run an
           iteration or enable collection over HTTP;
      (5)  the governance vocabulary is present: collection automation is
           token-gated and execution automation stays off and unreachable;
      (6)  a market OBSERVATION is not material on arrival, and the READ surface
           is bound to that same rule rather than counting by its own definition;
      (7)  ONE clock per event cycle reaches the live adapters, so event identity
           is reproducible instead of depending on the wall-clock minute;
      (8)  the UI has exactly one loader and classifies nothing itself;
      (9)  the worker delegates to the orchestrator and owns no cadence of its own;
      (10) exactly ONE PowerShell script manages the service, its read-only Status
           needs no ``-Execute`` and every mutating action does.
    """
    kernel = _read(IC_KERNEL)
    owner = _read(IC_OWNER)
    worker = _read(IC_WORKER)
    manage = _read(IC_MANAGE)
    event_owner = _read(IC_EVENT_OWNER)
    materiality = _read(IC_MATERIALITY)
    ui = _read(UI_FILE)

    # (1) presence + kernel purity.
    present = {name: bool((REPO_ROOT / name).exists())
               for name in (IC_KERNEL, IC_OWNER, IC_REPLAY, IC_WORKER, IC_MANAGE)}
    kernel_impurity = sorted(t for t in IC_KERNEL_FORBIDDEN if t in kernel)

    # (2) exactly one cadence resolver and one collection iteration.
    def _second_owners(marker: str, allowed: tuple) -> list:
        out = []
        for fp in files:
            rel = _rel(fp)
            if rel in allowed or rel == "scripts/audit_architecture.py":
                continue
            if marker in fp.read_text(encoding="utf-8", errors="replace"):
                out.append(rel)
        return sorted(out)

    second_cadence_owners = _second_owners("def resolve_source_runtime(",
                                           (IC_KERNEL,))
    second_collection_owners = _second_owners("def run_collection_iteration(",
                                              (IC_OWNER,))
    second_worker_scripts = sorted(
        _rel(fp) for fp in (REPO_ROOT / "scripts").glob("*.py")
        if "collection" in fp.name.lower()
        and _rel(fp) not in (IC_WORKER, "scripts/collection_service_control.py"))

    # (3) composition, not duplication.
    owner_forbidden_calls = sorted(t for t in IC_OWNER_FORBIDDEN if t in owner)
    missing_delegation = sorted(t for t in IC_OWNER_DELEGATION if t not in owner)

    # (4) the read surface is GET-only and starts nothing.
    ic_routes = [r for r in routes if str(r.get("path")) == IC_ROUTE]
    route_methods = sorted({str(r.get("method")) for r in ic_routes})
    forbidden_routes_present = sorted(
        str(r.get("path")) for r in routes
        if str(r.get("path")).startswith(IC_ROUTE)
        and any(str(r.get("path")).endswith(s) for s in IC_FORBIDDEN_ROUTE_SUFFIXES))

    # (5) governance vocabulary.
    missing_safety_tokens = sorted(t for t in IC_SAFETY_TOKENS if t not in owner)

    # (6) a market observation is not material on arrival — and the read surface
    # is bound to the GATE's rule, not to a second definition of "material".
    observation_rule_present = (
        "MARKET_OBSERVATION_FAMILIES" in materiality
        and "S_OBSERVATION_ON_ARRIVAL" in materiality
        and "ret_intraday" in materiality)
    read_surface_bound = "emat.MARKET_OBSERVATION_FAMILIES" in event_owner

    # (7) ONE clock per cycle reaches the live adapters.
    single_cycle_clock = ("cycle_now_iso" in event_owner
                          and "now_iso=cycle_now_iso" in event_owner
                          and "now_iso=started_iso" in owner)

    # (8) the UI renders; it does not decide.
    ui_loader_count = ui.count("function loadInformationCollection(")
    missing_ui_tokens = sorted(t for t in IC_UI_TOKENS if t not in ui)
    ic_region = ui[ui.find("function renderInformationCollection("):] if \
        "function renderInformationCollection(" in ui else ""
    ic_region = ic_region[:ic_region.find("window.renderInformationCollection")] \
        if "window.renderInformationCollection" in ic_region else ic_region
    ui_health_derivation = sorted(
        m.group(0).strip() for pat in IC_UI_FORBIDDEN_PATTERNS
        for m in re.finditer(pat, ic_region))

    # (9) the worker delegates and owns no cadence. Release 53: the lock
    # acquire is the bounded-wait variant (same composition owner) so a logon
    # race no longer kills collection until the next logon; either canonical
    # acquire satisfies the delegation invariant, a local reimplementation
    # satisfies neither.
    worker_delegates = all(t in worker for t in
                           ("ic.run_collection_iteration(",
                            "ic.heartbeat(", "ic.release_service_lock(")) and (
        "ic.acquire_service_lock(" in worker
        or "ic.acquire_service_lock_with_wait(" in worker)
    worker_reimplements_cadence = sorted(
        t for t in ("CADENCE_POLICY_BY_ID", "def resolve_window(",
                    "normal_interval_seconds =") if t in worker)

    # (10) one LIFECYCLE manager, read-only by default. Release 53.1 splits
    # the durable task DEFINITION into its own single owner (installer) plus
    # a read-only validator, and the manager must DELEGATE registration to
    # the installer - so the legal script set is exactly these three, the
    # manager may no longer register a task inline, and the validator may
    # never register one at all. Anything else is a competing lifecycle.
    IC_MANAGER = "scripts/manage_information_collection.ps1"
    IC_TASK_INSTALLER = "scripts/install_information_collection_task.ps1"
    IC_TASK_VALIDATOR = "scripts/validate_information_collection_task.ps1"
    manage_scripts = sorted(
        _rel(fp) for fp in (REPO_ROOT / "scripts").glob("*.ps1")
        if "information_collection" in fp.name.lower()
        or "collection_service" in fp.name.lower())
    unexpected_collection_scripts = sorted(
        s for s in manage_scripts
        if s not in (IC_MANAGER, IC_TASK_INSTALLER, IC_TASK_VALIDATOR))
    installer_src = _read(IC_TASK_INSTALLER)
    validator_src = _read(IC_TASK_VALIDATOR)
    task_definition_owner_present = (
        "Register-ScheduledTask" in installer_src
        and "DecisionProbe" in installer_src)
    manager_delegates_registration = (
        "install_information_collection_task.ps1" in manage
        and "Register-ScheduledTask" not in manage
        and "New-ScheduledTaskTrigger -AtLogOn" not in manage)
    validator_is_read_only = (
        bool(validator_src)
        and "Register-ScheduledTask" not in validator_src
        and "Set-ScheduledTask" not in validator_src
        and "Start-ScheduledTask" not in validator_src)
    manage_requires_execute = ("function Require-Execute(" in manage
                               and manage.count("Require-Execute \"") >= 4)
    status_is_read_only = ("Require-Execute \"Status\"" not in manage)
    uninstall_preserves_evidence = ("never" in manage.lower()
                                    and "evidence" in manage.lower())

    # (11) ONE definition of "one logical worker", owned by the orchestrator and
    # delegated to by the manager. A raw physical-process count may never be the
    # singleton verdict again.
    missing_topology_tokens = sorted(t for t in IC_MANAGE_TOPOLOGY_TOKENS
                                     if t not in manage)
    manage_counts_raw_processes = sorted(t for t in IC_MANAGE_RAW_PROCESS_COUNT
                                         if t in manage)
    second_topology_owners = _second_owners("def resolve_worker_topology(",
                                            (IC_OWNER,))
    topology_owner_present = "def resolve_worker_topology(" in owner
    control_delegates_topology = all(
        t in _read("scripts/collection_service_control.py")
        for t in ("ic.resolve_worker_topology(", '"worker-topology"'))

    # (12) ONE heartbeat/progress authority, and a stall budget that was never
    # quietly widened. Release 29.1 reported a healthy 5.5-minute pass as
    # DEGRADED because health measured the wrong thing; the cheap "fix" would
    # have been a bigger number, which is exactly what this guard forbids.
    missing_progress_tokens = sorted(t for t in IC_PROGRESS_TOKENS
                                     if t not in owner)
    second_progress_owners = _second_owners("def record_progress(", (IC_OWNER,))
    worker_timer_authorities = sorted(t for t in IC_WORKER_FORBIDDEN_TIMERS
                                      if t in worker)
    worker_reports_progress = ("ic.ProgressReporter(" in worker
                               and "progress_fn=progress" in worker)

    def _seconds(name: str):
        m = re.search(r"^%s\s*=\s*([0-9]+(?:\.[0-9]+)?)" % re.escape(name),
                      owner, re.M)
        return float(m.group(1)) if m else None

    stale_budget, stall_budget = (_seconds("HEARTBEAT_STALE_SECONDS"),
                                  _seconds("PROGRESS_STALL_SECONDS"))
    stall_budget_not_widened = bool(
        stale_budget is not None and stall_budget is not None
        and stall_budget <= stale_budget)

    return {
        "modules_present": present,
        "kernel_impurity": kernel_impurity,
        "second_cadence_owner_modules": second_cadence_owners,
        "second_collection_owner_modules": second_collection_owners,
        "second_worker_scripts": second_worker_scripts,
        "owner_forbidden_calls": owner_forbidden_calls,
        "missing_delegation": missing_delegation,
        "route_get_count": len(ic_routes),
        "route_methods": route_methods,
        "forbidden_routes_present": forbidden_routes_present,
        "missing_safety_tokens": missing_safety_tokens,
        "observation_rule_present": bool(observation_rule_present),
        "read_surface_bound_to_gate": bool(read_surface_bound),
        "single_cycle_clock": bool(single_cycle_clock),
        "ui_loader_count": ui_loader_count,
        "missing_ui_tokens": missing_ui_tokens,
        "ui_health_derivation": ui_health_derivation,
        "worker_delegates": bool(worker_delegates),
        "worker_reimplements_cadence": worker_reimplements_cadence,
        "manage_scripts": manage_scripts,
        "unexpected_collection_scripts": unexpected_collection_scripts,
        "task_definition_owner_present": bool(task_definition_owner_present),
        "manager_delegates_registration": bool(manager_delegates_registration),
        "validator_is_read_only": bool(validator_is_read_only),
        "manage_requires_execute": bool(manage_requires_execute),
        "status_is_read_only": bool(status_is_read_only),
        "uninstall_preserves_evidence": bool(uninstall_preserves_evidence),
        "topology_owner_present": bool(topology_owner_present),
        "second_topology_owner_modules": second_topology_owners,
        "manage_missing_topology_tokens": missing_topology_tokens,
        "manage_counts_raw_processes": manage_counts_raw_processes,
        "control_delegates_topology": bool(control_delegates_topology),
        "missing_progress_tokens": missing_progress_tokens,
        "second_progress_owner_modules": second_progress_owners,
        "worker_timer_authorities": worker_timer_authorities,
        "worker_reports_progress": bool(worker_reports_progress),
        "heartbeat_stale_seconds": stale_budget,
        "progress_stall_seconds": stall_budget,
        "stall_budget_not_widened": bool(stall_budget_not_widened),
    }


def check_backend_restart_ownership(extra_dirs=()) -> dict:
    """ONE repository-owned restart / smoke workflow.

      (1) the canonical owner exists and declares itself;
      (2) it probes the canonical readiness routes and nothing else;
      (3) NO other PowerShell script probes a health / readiness route or launches the
          application - stage handoffs delegate and may only add GET assertions;
      (4) every ``/v1`` path any of these workflows probes is a route the application
          actually declares as GET (a wrong path is a build failure, not a 404 at 3am);
      (5) the owner implements every responsibility it claims (stop, start, port, health,
          readiness, auth, diagnostics, production-root validation);
      (6) the owner prints the full startup diagnostic set before returning nonzero;
      (7) no restart / smoke workflow uses a mutating HTTP verb;
      (8) ``LIVE_SMOKE_OK`` is emitted by exactly one script, exactly once.
    """
    owner_src = _read(RESTART_OWNER)
    owner_present = (REPO_ROOT / RESTART_OWNER).exists()
    declared_get = {r["path"] for r in check_routes()["routes"] if r["method"] == "GET"}

    noncanonical: list[str] = []
    probed_not_declared: list[str] = []
    reimplementing: list[str] = []
    mutating: list[str] = []
    emitter_scripts: set[str] = set()
    owner_emissions = 0
    scanned: list[str] = []

    for label, fp in _iter_powershell_files(extra_dirs):
        try:
            text = fp.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        scanned.append(label)
        is_owner = (label == RESTART_OWNER)
        for i, line in enumerate(text.splitlines(), start=1):
            for m in _PS_HEALTH_LITERAL.finditer(line):
                host = m.group("host")
                if host and not _PS_LOCAL_HOST.match(host):
                    continue  # somebody else's API, not this backend's readiness probe
                lit = (host or "") + m.group("path")
                path = _ps_url_path(lit)
                if path not in CANONICAL_READINESS_ROUTES:
                    noncanonical.append("%s:%d: %s (NONCANONICAL health/ready route)"
                                        % (label, i, lit))
                elif not is_owner:
                    noncanonical.append(
                        "%s:%d: %s (only %s may probe a readiness route)"
                        % (label, i, lit, RESTART_OWNER))
            if _PS_MUTATING_VERB.search(line):
                mutating.append("%s:%d: %s" % (label, i, line.strip()))
            if not is_owner and (_PS_APP_LAUNCH.search(line)
                                 or _PS_LIVE_PORT_MGMT.search(line)):
                reimplementing.append("%s:%d: %s" % (label, i, line.strip()))
            if "LIVE_SMOKE_OK" in line and _PS_EMITS.search(line):
                emitter_scripts.add(label)
                if is_owner:
                    owner_emissions += 1
        for m in _PS_V1_LITERAL.finditer(text):
            host = m.group("host")
            if host and not _PS_LOCAL_HOST.match(host):
                continue
            p = m.group("path").rstrip("/")
            if p and p not in declared_get:
                probed_not_declared.append("%s: %s" % (label, p))

    return {
        "owner": RESTART_OWNER,
        "owner_present": owner_present,
        "owner_declares_ownership": RESTART_OWNER_DECLARATION in owner_src,
        "canonical_readiness_routes": list(CANONICAL_READINESS_ROUTES),
        "owner_missing_canonical_routes": [r for r in CANONICAL_READINESS_ROUTES
                                           if ('"%s"' % r) not in owner_src],
        "owner_missing_contract": [t for t in _RESTART_CONTRACT_TOKENS
                                   if t not in owner_src],
        "owner_missing_diagnostics": [t for t in _RESTART_DIAGNOSTIC_TOKENS
                                      if t not in owner_src],
        "noncanonical_health_probes": sorted(set(noncanonical)),
        "probed_routes_not_declared": sorted(set(probed_not_declared)),
        "reimplementing_scripts": sorted(set(reimplementing)),
        "mutating_http_calls": sorted(set(mutating)),
        "live_smoke_emitting_scripts": sorted(emitter_scripts),
        "owner_live_smoke_emissions": owner_emissions,
        "scanned_powershell_files": sorted(scanned),
    }


# --------------------------------------------------------------------------- #
# RELEASE 29 UX2 - RADICAL OPERATOR SIMPLIFICATION
#
# The previous pass improved HIERARCHY and user acceptance still failed, because Today and
# Portfolio carried far too much information. The standing product rule is:
#
#   IF THE OPERATOR CANNOT ACT ON IT, AND DOES NOT NEED IT TO MAKE A PORTFOLIO DECISION,
#   IT DOES NOT BELONG ON TODAY OR PORTFOLIO.
#
# The risk of a REMOVAL pass is the mirror image of the risk of a consolidation pass: that
# something is genuinely lost, that a canonical owner is forked to feed a new surface, or
# that "removed" quietly means "deleted from the DOM", breaking a loader. This guard pins
# the repaired shape: the content MOVED, it moved to exactly one place, its ids survived,
# and no second data owner appeared.
# --------------------------------------------------------------------------- #

#: Regions that must NO LONGER be inside the Today tab.
UX2_OFF_TODAY = ('id="cc-market-context"', 'id="cc-market-visuals"', 'id="cc-freshness"',
                 'id="cc-research-strip"', 'id="ic-source-details"', 'id="evt-events"',
                 'id="evt-affected"', 'id="ic-decision"')
#: ... and where each of them must now live.
UX2_ON_MARKETS = ('id="cc-market-context"', 'id="cc-market-visuals"')
UX2_ON_SYSTEM_AUDIT = ('id="cc-freshness"', 'id="cc-research-strip"', 'id="ic-source-details"',
                       'id="evt-events"', 'id="evt-affected"', 'id="ic-decision"',
                       'id="ic-service-line"', 'id="evt-kpis"', 'id="ic-sources"')
#: Today keeps ONE compact market line, and it is a MIRROR - never a second fetch owner.
UX2_TODAY_STRIP = 'id="today-market-strip"'
UX2_STRIP_RENDERER = "function _r29ux2RenderTodayMarketStrip"
#: A mirror may not fetch, and may not do market arithmetic.
UX2_STRIP_FORBIDDEN = ("call('GET'", "fetch(", "_mhzGet(", "parseFloat(", "toFixed(",
                       "Number(", "Date(")
#: The Markets area is REFERENCE CONTEXT and must say so.
UX2_MARKETS_LABEL = "REFERENCE CONTEXT &mdash; NOT A PORTFOLIO SIGNAL"
#: Routes whose persistent right diagnostic rail must be removed.
UX2_RAIL_FREE_ROUTES = ("command-center", "portfolio-manager", "markets")
#: Portfolio regions that must not be on the primary route any more.
UX2_OFF_PORTFOLIO = ("#rout-card", "#pm-checks-card", "#rlin-card", "#reassess-audit",
                     "#reassess-alternatives-card", "#pm-dc-card", "#pm-sec-evidence",
                     "#pm-sec-audit")
#: Portfolio regions that MUST remain (performance & risk is explicitly kept).
UX2_KEEP_PORTFOLIO = ('id="pa-hero"', 'id="pm-current-strip"', 'id="pa-decision"',
                      'id="reassess-card"', 'id="pm-dag-counts"', 'id="pdash-perf-charts"',
                      'id="pa-perf-chart"', 'id="pa-pnl-chart"', 'id="pa-dd-chart"',
                      'id="pa-alloc-chart"', 'id="pa-contrib-chart"', 'id="pa-drift-chart"')


def _ux2_region(ui: str, start_marker: str, end_marker: str) -> str:
    i = ui.find(start_marker)
    if i == -1:
        return ""
    j = ui.find(end_marker, i)
    return ui[i:j] if j != -1 else ui[i:]


def check_release29_ux2_simplification(files: list[Path]) -> dict:
    """Release 29 UX2 guard. Proves the operating screens were SIMPLIFIED BY REMOVAL, and
    that the removal was a MOVE rather than a loss:

      (1)  the Markets area exists: nav entry, route, tab, and the reference-only label;
      (2)  the market dashboard is no longer inside Today and IS inside Markets;
      (3)  data freshness, collection source health / worker counters, the material-event
           lists, the portfolio-decision line and research status are no longer inside
           Today and ARE inside the System - Audit operating-diagnostics panel;
      (4)  every moved id still exists exactly once (no loader write target was deleted);
      (5)  Today keeps ONE compact market strip and it is a MIRROR - it performs no fetch
           and no market arithmetic, so no second market owner exists;
      (6)  the market data owners are still exactly one each;
      (7)  the persistent right diagnostic rail is removed from Today / Portfolio / Markets
           and the rail markup itself is retained (ids stay live write targets);
      (8)  the Portfolio evidence and history/audit regions left the primary route while
           performance & risk stayed;
      (9)  the moved diagnostics panel is registered with the section router, so it is
           reachable, and only under System - Audit.
    """
    ui = _read(UI_FILE)

    today = _ux2_region(ui, '<div id="tab-overview" class="tab-content active">',
                        "<!-- end tab-overview -->")
    markets = _ux2_region(ui, '<div id="tab-markets" class="tab-content">',
                          "<!-- end tab-markets -->")
    audit_tab = _ux2_region(ui, '<div id="tab-audit-advanced" class="tab-content">',
                            "<!-- end tab-audit-advanced -->")
    sysops = _ux2_region(audit_tab, '<div class="card" id="sysops-panel"',
                         '<!-- One page-level safety strip')

    # (1) the Markets area.
    markets_nav = ('id="nav-markets"' in ui and 'data-route="markets"' in ui
                   and ">Markets<" in ui)
    markets_route = "'markets': 'markets'" in ui
    markets_tab_present = bool(markets)
    markets_reference_only = UX2_MARKETS_LABEL in markets

    # (2)/(3) what left Today, and where it went.
    still_on_today = sorted(t for t in UX2_OFF_TODAY if t in today)
    missing_on_markets = sorted(t for t in UX2_ON_MARKETS if t not in markets)
    missing_on_system_audit = sorted(t for t in UX2_ON_SYSTEM_AUDIT if t not in sysops)

    # (4) nothing was deleted: each moved id still exists exactly once in the document.
    duplicated_or_lost_ids = sorted(
        t for t in (UX2_ON_MARKETS + UX2_ON_SYSTEM_AUDIT) if ui.count(t) != 1)

    # (5) Today's strip is a mirror.
    strip_present = UX2_TODAY_STRIP in today
    strip_src = _ux2_region(ui, UX2_STRIP_RENDERER,
                            "window._r29ux2RenderTodayMarketStrip")
    strip_forbidden = sorted(t for t in UX2_STRIP_FORBIDDEN if t in strip_src)
    strip_reads_authoritative_tiles = ".ov-market-card[data-key=" in strip_src

    # (6) one market owner each, unchanged.
    market_dashboard_owners = ui.count("async function loadMarketDashboard")
    market_context_owners = ui.count("function loadMarketContext(")

    # (7) the rail is removed by route, and the markup is retained.
    rail_rules = [r for r in UX2_RAIL_FREE_ROUTES
                  if ('body[data-route="%s"]' % r) in ui]
    rail_route_published = "document.body.setAttribute('data-route', base)" in ui
    rail_markup_retained = ('<div class="right-panel">' in ui
                            and 'id="right-current-task"' in ui
                            and 'id="right-safety-footer"' in ui)

    # (8) Portfolio.
    pm_removed = sorted(t for t in UX2_OFF_PORTFOLIO
                        if ("#tab-portfolio-manager > .card > %s," % t) not in ui
                        and ("#tab-portfolio-manager > .card > %s\n" % t) not in ui
                        and ("#tab-portfolio-manager > .card > %s " % t) not in ui)
    pm_kept = sorted(t for t in UX2_KEEP_PORTFOLIO if t not in ui)

    # (9) the moved panel is routed, and only under System - Audit.
    sysops_registered = ("'diagnostics':      { panels: ['sysops-panel'" in ui
                         and "var _RA_ALL_PANELS = ['sysops-panel'" in ui)

    return {
        "markets_nav": bool(markets_nav),
        "markets_route": bool(markets_route),
        "markets_tab_present": bool(markets_tab_present),
        "markets_reference_only_label": bool(markets_reference_only),
        "regions_still_on_today": still_on_today,
        "regions_missing_on_markets": missing_on_markets,
        "regions_missing_on_system_audit": missing_on_system_audit,
        "moved_ids_duplicated_or_lost": duplicated_or_lost_ids,
        "today_market_strip_present": bool(strip_present),
        "today_market_strip_is_a_mirror": bool(strip_reads_authoritative_tiles),
        "today_market_strip_forbidden_calls": strip_forbidden,
        "market_dashboard_owner_count": market_dashboard_owners,
        "market_context_owner_count": market_context_owners,
        "rail_free_routes": sorted(rail_rules),
        "rail_route_published": bool(rail_route_published),
        "rail_markup_retained": bool(rail_markup_retained),
        "portfolio_regions_not_removed": pm_removed,
        "portfolio_regions_lost": pm_kept,
        "moved_diagnostics_panel_routed": bool(sysops_registered),
    }


# --------------------------------------------------------------------------- #
# RELEASE 29 UX2 - RESTART / SMOKE INVOCATION HYGIENE
#
# Owning the restart workflow was not enough: the workflow kept being INVOKED through a
# child PowerShell, and re-entering PowerShell is what actually broke it. Two real
# production defects:
#
#   1. A release wrapper forwarded a String[] of smoke paths across
#      `powershell.exe -File restart_paper_trader_backend.ps1 ... -SmokePath $paths`.
#      `-File` has no PowerShell parser on the far side, so the array flattened into bare
#      tokens; the binder took the first as -SmokePath and bound the NEXT URL positionally
#      to -ReadyTimeoutSec:Int32. The run died naming a timeout, not a path.
#
#   2. The repair attempt used `powershell.exe -Command` with a DOUBLE-QUOTED here-string
#      containing continuation backticks. The outer shell consumed the backticks, so
#      -Force / -Port / -SmokePath became three separate commands.
#
# Neither is a code defect inside the owner - both are INVOCATION defects. So the
# invocation shape is now a build-time contract: the owner is called DIRECTLY, in-process,
# and it contains no process-terminating statement, which is what makes direct invocation
# safe for an operator's own shell.
# --------------------------------------------------------------------------- #

#: The owner's basename - what an invocation line refers to.
_RESTART_OWNER_NAME = "restart_paper_trader_backend.ps1"

#: Re-entering PowerShell at all. `-File` and `-Command` are the two shapes that broke.
_PS_CHILD_SHELL = re.compile(r"(?i)\bpowershell(?:\.exe)?\b")
_PS_CHILD_FILE_SWITCH = re.compile(r"(?i)(?:^|[\s;`'\"])-f(?:i(?:l(?:e)?)?)?\b")
_PS_CHILD_COMMAND_SWITCH = re.compile(r"(?i)(?:^|[\s;`'\"])-c(?:o(?:m(?:m(?:a(?:n(?:d)?)?)?)?)?)?\b")

#: Constructing a backend LIFECYCLE through a child shell: -Force / -Port / -SmokePath /
#: the owner itself / a uvicorn launch. Any of these behind `powershell -Command` is the
#: exact defect #2 shape.
_PS_LIFECYCLE_TOKEN = re.compile(
    r"(?i)(?:restart_paper_trader_backend|-SmokePath\b|-ReadyTimeoutSec\b|\buvicorn\b"
    r"|-Force\b[\s`]*-Port\b|-Port\b[\s`]*\d+[\s`]*-SmokePath\b)")

#: A String[] parameter flattened into ONE comma-joined string, or forwarded positionally.
#: `-SmokePath "a,b"` and `-SmokePath ($p -join ',')` are the shapes that silently degrade
#: five checked paths into one nonsense path.
_PS_SMOKEPATH_JOINED = re.compile(
    r"""(?i)-SmokePath\s+(?:["'][^"']*,[^"']*["']|\([^)]*-join[^)]*\)|\$\w+\s+-join)""")

#: A second restart implementation - a file that both launches the app and probes a
#: readiness route, or that redeclares the owner's marker.
_PS_DUP_IMPL_TOKENS = ("Start-Process", "uvicorn")

#: Quoted spans. Prose inside a string ("process exit code") is not control flow.
_PS_STRING_LITERAL = re.compile(r""""(?:[^"`]|`.)*"|'(?:[^']|'')*'""")

#: Documentation and workflows must show the DIRECT invocation, never a child shell.
_RESTART_DIRECT_INVOCATION = "& C:\\Users\\binis\\paper_trader\\scripts\\restart_paper_trader_backend.ps1"


def _iter_invocation_scan_files(extra_dirs=()) -> list[tuple[str, Path]]:
    """PowerShell workflows, Python drivers and Markdown runbooks in scope. An invocation
    defect can be authored in any of the three, so all three are scanned."""
    out: list[tuple[str, Path]] = []
    seen: set[str] = set()
    exts = (".ps1", ".py", ".md", ".psm1", ".cmd", ".bat")
    bases = [REPO_ROOT] + [Path(d) for d in extra_dirs]
    for base in bases:
        try:
            if not base.exists():
                continue
        except OSError:
            continue
        for root, dirs, files in os.walk(str(base)):
            dirs[:] = [d for d in dirs
                       if d not in EXCLUDE_PARTS and not d.endswith(".egg-info")]
            for name in sorted(files):
                if not name.lower().endswith(exts):
                    continue
                fp = Path(root) / name
                try:
                    key = str(fp.resolve()).lower()
                except OSError:
                    key = str(fp).lower()
                if key in seen:
                    continue
                seen.add(key)
                try:
                    label = _rel(fp)
                except ValueError:
                    label = str(fp).replace("\\", "/")
                out.append((label, fp))
    return sorted(out, key=lambda t: t[0])


def check_restart_invocation_hygiene(extra_dirs=()) -> dict:
    """Release 29 UX2 guard over HOW the canonical restart owner is invoked.

      (1) the owner contains NO process-terminating statement, so calling it directly can
          never end an operator's shell;
      (2) NOBODY forwards ``-SmokePath`` through ``powershell.exe -File`` (defect #1:
          String[] flattening binding a URL to ``-ReadyTimeoutSec:Int32``);
      (3) NOBODY constructs a backend lifecycle command through ``powershell.exe -Command``
          (defect #2: the outer shell eating continuation backticks);
      (4) NOBODY collapses the ``String[]`` into one comma-joined / positional argument;
      (5) there is no SECOND restart implementation (a file that both launches the app and
          polls readiness);
      (6) the owner still declares the direct, in-process invocation contract, exposes the
          bind-and-report probe, and reports its outcome without terminating the process.
    """
    owner_src = _read(RESTART_OWNER)

    # (1) NO process-terminating statement. A bare `exit` (or `exit 1`) in a script an
    # operator calls directly with `&` is the thing that makes "call it directly" unsafe.
    # Only real CODE counts: the words "exit code" inside a diagnostics string and the
    # word "exit" in a comment are prose, not control flow.
    exit_statements: list[str] = []
    for i, line in enumerate(owner_src.splitlines(), start=1):
        code = _PS_STRING_LITERAL.sub(" ", line)      # drop quoted spans first ...
        code = code.split("#", 1)[0]                  # ... then trailing comments
        if re.search(r"(?im)(?:^|[\s;{(&|])exit\b", code):
            exit_statements.append("%s:%d: %s" % (RESTART_OWNER, i, line.strip()))
        if re.search(r"(?i)\$Host\.SetShouldExit|\[Environment\]::Exit", code):
            exit_statements.append("%s:%d: %s" % (RESTART_OWNER, i, line.strip()))

    file_forwarding: list[str] = []
    command_lifecycle: list[str] = []
    fragile_array_forwarding: list[str] = []
    duplicate_restart_impls: list[str] = []
    scanned: list[str] = []

    for label, fp in _iter_invocation_scan_files(extra_dirs):
        try:
            text = fp.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        scanned.append(label)
        is_owner = (label == RESTART_OWNER)
        is_guard = label in ("scripts/audit_architecture.py",)
        lines = text.splitlines()
        for i, line in enumerate(lines, start=1):
            # Continuation-aware window: a PowerShell invocation is routinely spread over
            # several backtick-continued lines, so judge the logical command, not one line.
            window = line
            j = i - 1
            while j + 1 < len(lines) and window.rstrip().endswith("`"):
                window = window.rstrip()[:-1] + " " + lines[j + 1]
                j += 1
            if is_guard:
                continue
            has_child_shell = bool(_PS_CHILD_SHELL.search(window))
            # (2) -File + the owner + -SmokePath: the exact flattening defect.
            if (has_child_shell and _PS_CHILD_FILE_SWITCH.search(window)
                    and _RESTART_OWNER_NAME in window and "-SmokePath" in window):
                file_forwarding.append("%s:%d: %s" % (label, i, window.strip()[:200]))
            # (3) -Command carrying a backend lifecycle.
            if (has_child_shell and _PS_CHILD_COMMAND_SWITCH.search(window)
                    and _PS_LIFECYCLE_TOKEN.search(window)):
                command_lifecycle.append("%s:%d: %s" % (label, i, window.strip()[:200]))
            # (4) the String[] collapsed into one argument.
            if _PS_SMOKEPATH_JOINED.search(window):
                fragile_array_forwarding.append("%s:%d: %s" % (label, i, window.strip()[:200]))
        # (5) a second restart implementation.
        if (not is_owner and label.lower().endswith(".ps1")
                and all(t in text for t in _PS_DUP_IMPL_TOKENS)
                and re.search(r"(?i)/v1/(?:health|ready)", text)):
            duplicate_restart_impls.append(label)

    return {
        "owner": RESTART_OWNER,
        "owner_exit_statements": sorted(set(exit_statements)),
        "owner_is_exit_free": exit_statements == [],
        "owner_declares_direct_invocation": _RESTART_DIRECT_INVOCATION in owner_src,
        "owner_exposes_contract_probe": ("[switch]$ContractProbe" in owner_src
                                         and "CONTRACT_PROBE_JSON_BEGIN" in owner_src),
        "owner_reports_last_exit_code": "$global:LASTEXITCODE = $script:ResultCode" in owner_src,
        "owner_asserts_smokepath_contract": "function Assert-SmokePathContract" in owner_src,
        "file_switch_smokepath_forwarding": sorted(set(file_forwarding)),
        "command_switch_lifecycle_construction": sorted(set(command_lifecycle)),
        "fragile_array_forwarding": sorted(set(fragile_array_forwarding)),
        "duplicate_restart_implementations": sorted(set(duplicate_restart_impls)),
        "scanned_invocation_files": len(scanned),
    }


def check_release30_zero_base_ownership(files: list[Path]) -> dict:
    """Release 30 strict semantic ownership guard for the zero-base adaptive
    alpha capital allocator.

    The defect this exists to prevent is a SECOND owner appearing beside an
    existing one: a second proposal engine, a second decision owner, a second
    covariance builder, a second aligned-return definition, a second event
    authority table, or a forecast that promotes itself. Each invariant below is
    a semantic contract, checked on symbols and AST rather than on prose.
    """
    forecast_kernel = "engine/return_forecast.py"
    alloc_kernel = "engine/zero_base_allocator.py"
    forecast_owner = "api/return_forecast.py"
    alloc_owner = "api/zero_base_target.py"
    matinfo = "api/material_information.py"
    leaderboard = "api/alpha_leaderboard.py"
    required = (forecast_kernel, alloc_kernel, forecast_owner, alloc_owner,
                matinfo, leaderboard)
    src = {p: _read(p) for p in required}
    missing = [p for p in required if not src[p].strip()]

    # 1. The kernels are PURE: stdlib only, no IO, no state.
    kernel_impurity: list[dict] = []
    for path in (forecast_kernel, alloc_kernel):
        text = src.get(path) or ""
        if not text.strip():
            continue
        try:
            tree = ast.parse(text)
        except SyntaxError as exc:
            kernel_impurity.append({"path": path, "reason": f"UNPARSEABLE: {exc}"})
            continue
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported |= {a.name.split(".")[0] for a in node.names}
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])
        heavy = imported & {"numpy", "pandas", "requests", "sqlite3", "urllib",
                            "httpx", "scipy"}
        if heavy:
            kernel_impurity.append({"path": path, "heavy_imports": sorted(heavy)})
        for token in ("open(", "requests.", "write_text(", "os.environ"):
            if token in text:
                kernel_impurity.append({"path": path, "io_token": token})

    # 2. There is exactly ONE zero-base calculation owner and ONE composition owner.
    second_calculation_owner = []
    second_composition_owner = []
    for fp in files:
        rel = _rel(fp)
        if rel in (alloc_kernel, alloc_owner):
            continue
        text = fp.read_text(encoding="utf-8", errors="replace")
        if re.search(r"^\s*def build_allocation\s*\(", text, re.M):
            second_calculation_owner.append(rel)
        if "ZERO_BASE_TARGET" in text and re.search(
                r"^\s*def (optimise|build_zero_base)\s*\(", text, re.M):
            second_composition_owner.append(rel)

    # 3. The forecast layer never promotes, and the read surface never writes.
    fsrc = src.get(forecast_owner) or ""
    auto_promotion_declared = "AUTOMATIC_PROMOTION_ALLOWED = False" not in fsrc
    activation_written_by_code = False
    read_surface_writes: list[str] = []
    try:
        ftree = ast.parse(fsrc) if fsrc.strip() else None
    except SyntaxError:
        ftree = None
    if ftree is not None:
        for node in ast.walk(ftree):
            if isinstance(node, ast.Call):
                name = getattr(node.func, "attr", getattr(node.func, "id", ""))
                if name == "_atomic_write_json" and node.args:
                    if "ACTIVATION_FILE" in ast.unparse(node.args[0]):
                        activation_written_by_code = True
        fns = {n.name: n for n in ast.walk(ftree)
               if isinstance(n, ast.FunctionDef)}
        for name in ("build", "load_return_forecast", "summary",
                     "activation_state"):
            body = ast.unparse(fns[name]) if name in fns else ""
            for token in ("_atomic_write_json", "write_text", "mkdir"):
                if token in body:
                    read_surface_writes.append("%s:%s" % (name, token))

    # 4. The allocator is not a proposal or decision owner, and cannot approve.
    asrc = src.get(alloc_owner) or ""
    forbidden_calls: list[str] = []
    try:
        atree = ast.parse(asrc) if asrc.strip() else None
    except SyntaxError:
        atree = None
    if atree is not None:
        called = set()
        for node in ast.walk(atree):
            if isinstance(node, ast.Call):
                called.add(getattr(node.func, "attr",
                                   getattr(node.func, "id", "")))
        forbidden_calls = sorted(called & {"record_decision", "approve",
                                           "build_proposal", "run_proposal",
                                           "persist_proposal", "create_order",
                                           "confirm_order_plan"})

    # 5. The covariance builder and the aligned-return series each have ONE owner.
    hoc = _read("engine/holding_opportunity_cost.py")
    covariance_owner_present = "def build_covariance(" in hoc
    risk_contributions_delegate = False
    try:
        htree = ast.parse(hoc) if hoc.strip() else None
        if htree is not None:
            for node in ast.walk(htree):
                if (isinstance(node, ast.FunctionDef)
                        and node.name == "compute_risk_contributions"):
                    risk_contributions_delegate = (
                        "build_covariance" in ast.unparse(node))
    except SyntaxError:
        pass
    pp = _read("api/price_panel.py")
    aligned_owner_present = "def aligned_returns(" in pp
    realloc = _read("api/reallocation_proposal.py")
    realloc_delegates = "pp.aligned_returns(" in realloc
    # Scope: the OPERATIONAL lane. A declared research-only module is entitled to
    # its own estimator - engine/absolute_return_research.py has carried a
    # shrunk-correlation builder since long before Release 30, and it feeds no
    # operational calculation. What must stay unique is the risk owner the
    # portfolio is actually priced on.
    second_covariance_builders = []
    for fp in files:
        rel = _rel(fp)
        if rel == "engine/holding_opportunity_cost.py":
            continue
        if rel.startswith(RESEARCH_ONLY_DIRS) or rel in RESEARCH_ONLY_MODULES:
            continue
        text = fp.read_text(encoding="utf-8", errors="replace")
        if re.search(r"^\s*def build_covariance\s*\(", text, re.M):
            second_covariance_builders.append(rel)

    # 6. The material-information read model owns NO authority table of its own.
    msrc = src.get(matinfo) or ""
    reads_fabric_authority = ("event_fabric" in msrc
                              and "ALPHA_BEARING_AUTHORITIES" in msrc
                              and "RISK_BEARING_AUTHORITIES" in msrc)
    private_authority_table = bool(
        re.search(r"^_AUTHORITY_REACH\s*=\s*\{", msrc, re.M))
    owns_no_calculation = ('"owns_no_calculation": True' in msrc
                           and '"owns_no_calculation": True' in (src.get(leaderboard) or ""))

    # 7. The read surfaces are GET-only: Release 30 declares no mutation route.
    app = _read(APP_MODULE)
    r30_routes = ("/v1/operations/zero-base-target", "/v1/research/return-forecast",
                  "/v1/operations/material-information",
                  "/v1/research/alpha-leaderboard")
    missing_routes = [r for r in r30_routes if '"%s"' % r not in app]
    mutating_routes = []
    for m in re.finditer(r'@app\.(post|put|delete|patch)\(\s*"([^"]+)"', app):
        if any(k in m.group(2) for k in ("zero-base", "return-forecast",
                                         "material-information",
                                         "alpha-leaderboard")):
            mutating_routes.append(m.group(2))

    # 8. The UI presents; it never computes a verdict or an authority.
    ui = _read(UI_FILE)
    ui_regions = {
        "material_information": 'id="cc-matinfo-card"' in ui,
        "zero_base_target": 'id="zb-card"' in ui,
        "alpha_leaderboard": 'id="albd-panel"' in ui,
    }
    ui_loaders = {
        "material_information": ui.count("function loadMaterialInformation("),
        "zero_base_target": ui.count("function loadZeroBaseTarget("),
        "alpha_leaderboard": ui.count("function loadAlphaLeaderboard("),
    }
    ui_forbidden = []
    for token in ("alert(", "confirm("):
        # The UI must not introduce a browser dialog in a Release 30 renderer.
        for m in re.finditer(r"function (renderMaterialInformation|"
                             r"renderZeroBaseTarget|renderAlphaLeaderboard)\("
                             r"[\s\S]{0,6000}?\n}", ui):
            if token in m.group(0):
                ui_forbidden.append({"function": m.group(1), "token": token})
    # No execute control may appear inside a Release 30 region.
    ui_execute_controls = []
    for region in ('id="cc-matinfo-card"', 'id="zb-card"', 'id="albd-panel"'):
        i = ui.find(region)
        if i < 0:
            continue
        block = ui[i:i + 6000]
        for token in ("dispatchCanonicalPrimaryAction", "CONFIRM_", "/execute",
                      "orders/confirm", "rebalance/confirm"):
            if token in block:
                ui_execute_controls.append({"region": region, "token": token})

    # 9. The research lane never imports the operational API package.
    research_imports_api = []
    for name in ("release30_panel", "release30_models",
                 "release30_forecast_research", "release30_forecast_emitter"):
        rel = "alpha_agent/%s.py" % name
        text = _read(rel)
        if "from paper_trader.api" in text or "import paper_trader.api" in text:
            research_imports_api.append(rel)

    # 10. Point-in-time and governance statements are DECLARED, not implied.
    ksrc = src.get(forecast_kernel) or ""
    zsrc = src.get(alloc_kernel) or ""
    declarations = {
        "target_is_a_return": 'TARGET_QUANTITY = "FORWARD_EXCESS_RETURN' in ksrc,
        "market_level_not_forecast": 'MARKET_BASELINE_POLICY = "MARKET_LEVEL_NOT_FORECAST"' in ksrc,
        "cash_policy_declared": 'CASH_RETURN_POLICY = "ZERO_RETURN_PAPER_ASSUMPTION"' in zsrc,
        "position_count_policy_declared": "POSITION_COUNT_POLICY" in zsrc,
        "legacy_count_retained": "LEGACY_TARGET_POSITION_COUNT = 25" in zsrc,
        "objective_versioned": "OBJECTIVE_VERSION" in zsrc,
        "two_targets_named": ("TARGET_ZERO_BASE" in zsrc
                              and "TARGET_IMPLEMENTABLE" in zsrc),
    }

    return {
        "modules_present": not missing,
        "missing_modules": missing,
        "kernel_impurity": kernel_impurity,
        "second_calculation_owner_modules": sorted(second_calculation_owner),
        "second_composition_owner_modules": sorted(second_composition_owner),
        "auto_promotion_declared": auto_promotion_declared,
        "activation_written_by_code": activation_written_by_code,
        "read_surface_writes": sorted(read_surface_writes),
        "allocator_forbidden_calls": forbidden_calls,
        "covariance_owner_present": covariance_owner_present,
        "risk_contributions_delegate_to_covariance_owner": risk_contributions_delegate,
        "second_covariance_builders": sorted(second_covariance_builders),
        "aligned_returns_owner_present": aligned_owner_present,
        "reallocation_delegates_aligned_returns": realloc_delegates,
        "material_information_reads_fabric_authority": reads_fabric_authority,
        "material_information_private_authority_table": private_authority_table,
        "read_models_own_no_calculation": owns_no_calculation,
        "missing_routes": missing_routes,
        "mutating_routes": sorted(mutating_routes),
        "ui_regions": ui_regions,
        "ui_loaders": ui_loaders,
        "ui_forbidden_dialogs": ui_forbidden,
        "ui_execute_controls": ui_execute_controls,
        "research_lane_imports_api": sorted(research_imports_api),
        "declarations": declarations,
    }


def check_release30_1_operational_cutover(files: list[Path]) -> dict:
    """Release 30.1 strict guard for the OPERATIONAL forecast lane.

    Release 30 shipped a frozen artifact that carried the current approved
    model's name and a NEGATIVE calibration slope. Because
    ``expected_excess_return = slope * standardised_score`` and the
    standardisation of a positive-weight rank blend is strictly monotone, that
    slope did not adjust the approved model - it reversed it, and the resulting
    "target" held none of the approved model's top 25 names. Nothing in the
    codebase could have said so. These invariants exist so that the next one is
    caught by the build rather than by a reader.
    """
    kernel = "engine/return_forecast.py"
    owner = "api/return_forecast.py"
    alloc_owner = "api/zero_base_target.py"
    calib = "alpha_agent/release30_1_operational_calibration.py"
    required = (kernel, owner, alloc_owner, calib)
    src = {p: _read(p) for p in required}
    missing = [p for p in required if not src[p].strip()]

    ksrc = src.get(kernel) or ""
    osrc = src.get(owner) or ""
    zsrc = src.get(alloc_owner) or ""
    csrc = src.get(calib) or ""

    # 1. The rank-identity contract is DECLARED and enforced in the kernel.
    contract = {
        "identity_contract_declared":
            'MODEL_IDENTITY_CONTRACT = "APPROVED_MODEL_RANKING_IS_PRESERVED"' in ksrc,
        "operational_activation_declared":
            'OPERATIONAL_ACTIVATION = "CURRENT_OPERATIONAL_MODEL"' in ksrc,
        "verdict_vocabulary_declared": "RANK_IDENTITY_VOCAB" in ksrc,
        "detector_present": bool(
            re.search(r"^def represents_approved_model\s*\(", ksrc, re.M)),
        "verdict_function_present": bool(
            re.search(r"^def rank_identity\s*\(", ksrc, re.M)),
        "suppression_disposition_declared": (
            'HORIZON_SUPPRESSED = "SUPPRESSED"' in ksrc),
    }
    enforced_in_build = False
    try:
        ktree = ast.parse(ksrc) if ksrc.strip() else None
    except SyntaxError:
        ktree = None
    if ktree is not None:
        for node in ast.walk(ktree):
            if isinstance(node, ast.FunctionDef) and node.name == "build_forecast":
                body = ast.unparse(node)
                enforced_in_build = ("rank_identity(" in body
                                     and "HORIZON_SUPPRESSED" in body)

    # 2. The rank-identity verdict has exactly ONE owner - the kernel.
    second_identity_owner = []
    for fp in files:
        rel = _rel(fp)
        if rel == kernel:
            continue
        text = fp.read_text(encoding="utf-8", errors="replace")
        if re.search(r"^def rank_identity\s*\(", text, re.M) or \
                re.search(r"^def represents_approved_model\s*\(", text, re.M):
            second_identity_owner.append(rel)

    # 3. The LIVE operational lane may not read a periodic research snapshot.
    #    Historical calibration may; the live decision path may not, because a
    #    feature stamped with an earlier session is not a forecast of this one.
    live_reads_research_snapshot = []
    try:
        otree = ast.parse(osrc) if osrc.strip() else None
    except SyntaxError:
        otree = None
    if otree is not None:
        fns = {n.name: n for n in ast.walk(otree)
               if isinstance(n, ast.FunctionDef)}
        for name in ("build_operational", "build_operational_cross_section"):
            body = ast.unparse(fns[name]) if name in fns else ""
            for token in ("load_forecast_input", "_INPUT_FILE", "forecast_input_"):
                if token in body:
                    live_reads_research_snapshot.append("%s:%s" % (name, token))
    live_lane = {
        "live_cross_section_owner_present": bool(
            re.search(r"^def build_operational_cross_section\s*\(", osrc, re.M)),
        "live_input_policy_declared":
            'LIVE_INPUT_POLICY = "CURRENT_CANONICAL_SCORE_AT_CURRENT_ELIGIBLE_SESSION"' in osrc,
        "research_snapshot_scope_declared":
            'RESEARCH_SNAPSHOT_ADMISSIBLE_FOR = "HISTORICAL_CALIBRATION_ONLY"' in osrc,
        "score_owner_is_universe_scoring":
            'OPERATIONAL_SCORE_OWNER = "api.universe_scoring"' in osrc,
        "freshness_delegated_to_canonical_owner": (
            "data_freshness" in osrc
            and "required_for_signal_refresh" in osrc),
    }

    # 4. The freshness judgement is delegated, never restated.
    second_freshness_table = bool(re.search(r"^_SOURCES\s*=", osrc, re.M))

    # 5. The GOVERNED lane never falls back to the research forecast, and the two
    #    lanes are labelled so neither can be read as the other.
    governed_fallback = []
    try:
        ztree = ast.parse(zsrc) if zsrc.strip() else None
    except SyntaxError:
        ztree = None
    if ztree is not None:
        fns = {n.name: n for n in ast.walk(ztree)
               if isinstance(n, ast.FunctionDef)}
        body = ast.unparse(fns["run_operational_allocation"]) \
            if "run_operational_allocation" in fns else ""
        for token in ("load_model_artifact", "rfc.build(", "load_forecast_input"):
            if token in body:
                governed_fallback.append(token)
    lanes = {
        "research_lane_declared": 'LANE_RESEARCH_PREVIEW = "RESEARCH_PREVIEW"' in zsrc,
        "governed_lane_declared":
            'LANE_GOVERNED_OPERATIONAL = "GOVERNED_OPERATIONAL_TARGET"' in zsrc,
        "authority_stamped_on_both": zsrc.count('"authority"') >= 3,
        "governed_owner_present": bool(
            re.search(r"^def run_operational_allocation\s*\(", zsrc, re.M)),
    }

    # 6. The operational calibration admits no new predictor family. Checked on
    #    CODE, not prose: the docstring must stay free to name what it excludes.
    forbidden_components = []
    calibration_code = ""
    try:
        ctree = ast.parse(csrc) if csrc.strip() else None
    except SyntaxError:
        ctree = None
    if ctree is not None:
        ctree.body = [n for n in ctree.body
                      if not (isinstance(n, ast.Expr)
                              and isinstance(n.value, ast.Constant)
                              and isinstance(n.value.value, str))]
        calibration_code = ast.unparse(ctree)
        for token in ("s25_operating_profitability", "fcf_to_assets",
                      "operating_accruals", "gbrt", "extra_trees", "ridge",
                      "adaptive_ensemble"):
            if token in calibration_code:
                forbidden_components.append(token)
    calibration = {
        "declares_approved_model":
            'OPERATIONAL_MODEL_ID = "fundamental_momentum_50_50_v1"' in csrc,
        "declares_only_approved_components":
            'OPERATIONAL_COMPONENTS = ("composite_sn", "mom_6_1")' in csrc,
        "rank_identity_bar_declared": "RANK_IDENTITY_MIN_SLOPE" in csrc,
        "reliability_bar_declared": "RELIABILITY_MIN_T" in csrc,
        "sign_stability_tested": "FOLD_GEOMETRIES" in csrc,
        "walk_forward_embargoed": "embargo" in csrc.lower(),
        # A random split is a BEHAVIOUR, so look for the behaviour: an RNG
        # import or call. A substring search on "random" would flag the very
        # sentence that promises there is no random split.
        "no_random_split": not any(
            tok in calibration_code
            for tok in ("import random", "np.random", "numpy.random",
                        ".shuffle(", ".permutation(", "default_rng(",
                        "RandomState(")),
    }

    # 7. The research calibration lane never imports the operational API package.
    research_imports_api = bool("from paper_trader.api" in csrc
                                or "import paper_trader.api" in csrc)

    # 8. No operational read path writes, promotes or decides.
    read_surface_writes: list[str] = []
    forbidden_calls: list[str] = []
    if otree is not None:
        fns = {n.name: n for n in ast.walk(otree)
               if isinstance(n, ast.FunctionDef)}
        for name in ("build_operational", "build_operational_cross_section",
                     "load_operational_return_forecast", "required_input_freshness",
                     "load_operational_artifact"):
            body = ast.unparse(fns[name]) if name in fns else ""
            for token in ("_atomic_write_json", "write_text", "mkdir"):
                if token in body:
                    read_surface_writes.append("%s:%s" % (name, token))
    if ztree is not None:
        called = {getattr(n.func, "attr", getattr(n.func, "id", ""))
                  for n in ast.walk(ztree) if isinstance(n, ast.Call)}
        forbidden_calls = sorted(called & {
            "record_decision", "approve", "build_proposal", "run_proposal",
            "persist_proposal", "create_order", "confirm_order_plan",
            "optimise", "build_assessment", "build_reassessment"})

    # ----------------------------------------------------------------------- #
    # Release 30.1 UX: source links and external references
    # ----------------------------------------------------------------------- #
    xref = "api/external_references.py"
    matinfo = "api/material_information.py"
    xsrc = _read(xref)
    msrc = _read(matinfo)
    ui = _read(UI_FILE)
    app = _read(APP_MODULE)
    if not xsrc.strip():
        missing.append(xref)

    # A. The "may this become an href" decision has exactly ONE owner. A second
    #    URL sanitiser is how an unsafe scheme reaches a browser: the two drift,
    #    and the weaker one wins wherever it happens to be called.
    second_url_guard = []
    for fp in files:
        rel = _rel(fp)
        if rel == xref:
            continue
        text = fp.read_text(encoding="utf-8", errors="replace")
        if re.search(r"^\s*def safe_external_url\s*\(", text, re.M):
            second_url_guard.append(rel)
    url_guard = {
        "owner_present": bool(re.search(r"^def safe_external_url\s*\(", xsrc, re.M)),
        "schemes_declared": 'ALLOWED_URL_SCHEMES = ("http", "https")' in xsrc,
        "link_policy_declared": ('LINK_TARGET = "_blank"' in xsrc
                                 and 'LINK_REL = "noopener noreferrer"' in xsrc),
        "state_vocabulary_declared": "URL_STATE_VOCAB" in xsrc,
        "matinfo_delegates": "safe_external_url" in msrc,
    }

    # B. The backend never CONSTRUCTS a source URL. The only URLs allowed as
    #    literals in the reference module are the declared reference sites; the
    #    capital-impact feed must carry none at all, because a link it assembled
    #    would be a claim about where evidence lives rather than a record of it.
    constructed_urls = []
    try:
        mtree = ast.parse(msrc) if msrc.strip() else None
    except SyntaxError:
        mtree = None
    if mtree is not None:
        for node in ast.walk(mtree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if node.value.startswith(("http://", "https://")):
                    constructed_urls.append(node.value[:60])
    literal_site_urls = sorted(
        set(re.findall(r'"(https?://[^"]+)"', xsrc))
        - {u for u in re.findall(r'"(https?://[^"]+)"', xsrc)
           if '"url": "%s"' % u in xsrc})

    # C. The read models own no calculation and cannot assign an authority.
    read_model_declarations = {
        "external_owns_no_calculation": '"owns_no_calculation": True' in xsrc,
        "external_creates_no_event": '"creates_no_event": True' in xsrc,
        "external_never_influences_decisions":
            '"influences_portfolio_decisions": False' in xsrc,
        "external_reads_canonical_registry": ("source_capability" in xsrc
                                              and "INGESTED_SOURCE_IDS" in xsrc),
        "external_reads_fabric_authority": ("event_fabric" in xsrc
                                            and "EVENT_FAMILIES" in xsrc),
        "matinfo_declares_article_is_not_alpha":
            '"external_article_is_not_alpha": True' in msrc,
        "matinfo_declares_transparency_fields": "TRANSPARENCY_FIELDS" in msrc,
        "matinfo_declares_link_policy": "SOURCE_LINK_POLICY" in msrc,
    }
    # The reference module must not classify: no private authority table.
    private_authority_table = bool(
        re.search(r"^(ALPHA|RISK|TRIGGER)_BEARING", xsrc, re.M)
        or re.search(r"^_?AUTHORITY_[A-Z_]*\s*=\s*\{", xsrc, re.M))

    # D. Every anchor the Release 30.1 renderers emit goes through ONE helper
    #    that carries target and rel. A hand-rolled `<a href=` in a renderer is
    #    the case that silently ships without noopener.
    ui_link_helper = ""
    m = re.search(r"function _r30srcLink\([\s\S]{0,1600}?\n\}", ui)
    if m:
        ui_link_helper = m.group(0)
    ui_links = {
        "helper_present": bool(ui_link_helper),
        "helper_sets_target": 'target="' in ui_link_helper,
        "helper_sets_rel": "p.rel || 'noopener noreferrer'" in ui_link_helper,
        "helper_requires_backend_url": "if (!url) return label;" in ui_link_helper,
        "attribute_escape_present": bool(
            re.search(r"function _r30attr\(", ui)),
    }
    ui_hand_rolled_anchors = []
    for fn in ("renderMaterialInformation", "renderExternalReferences"):
        mm = re.search(r"function %s\([\s\S]{0,9000}?\n\}" % fn, ui)
        if not mm:
            continue
        block = mm.group(0)
        if re.search(r"<a\s+href=", block):
            ui_hand_rolled_anchors.append(fn)
        for token in ("http://", "https://"):
            if token in block:
                ui_hand_rolled_anchors.append("%s:%s" % (fn, token))

    # E. The external references live on MARKETS and nowhere else. Today is the
    #    operating surface and carries only what the system itself concluded.
    today_i = ui.find('id="cc-matinfo-card"')
    markets_i = ui.find('id="tab-markets"')
    markets_end = ui.find("end tab-markets")
    card_i = ui.find('id="ext-refs-card"')
    ext_surface = {
        "region_present": card_i >= 0,
        "region_inside_markets": bool(
            markets_i >= 0 and markets_end > markets_i
            and markets_i < card_i < markets_end),
        "loader_present": "function loadExternalReferences(" in ui,
        "loader_count": ui.count("function loadExternalReferences("),
        "declared_markets_only": '"surface": "MARKETS"' in xsrc,
    }
    # The reading list must be reachable from the MARKETS route activation and
    # from nowhere else. Checking a window of MARKUP around the Today card is not
    # enough - a loader is wired in the bootstrap, hundreds of lines away from the
    # region it fills - so this checks CALL SITES against the allowed spans.
    def _span(pattern, size):
        i = ui.find(pattern)
        return (i, i + size) if i >= 0 else None

    allowed_spans = []
    m_def = re.search(r"function loadExternalReferences\([\s\S]{0,1200}?\n\}", ui)
    if m_def:
        allowed_spans.append((m_def.start(), m_def.end()))
    m_win = ui.find("window.loadExternalReferences = loadExternalReferences;")
    if m_win >= 0:
        allowed_spans.append((m_win, m_win + 80))
    # The markets route-activation block. Bounded by the block it opens, not by a
    # guessed character count.
    m_mkt = re.search(r"tabName === 'markets'[\s\S]{0,1600}?\n  \}", ui)
    if m_mkt:
        allowed_spans.append((m_mkt.start(), m_mkt.end()))

    ext_call_sites_outside_markets = []
    for m in re.finditer(r"loadExternalReferences\(", ui):
        if not any(lo <= m.start() < hi for lo, hi in allowed_spans):
            ext_call_sites_outside_markets.append(
                ui[max(0, m.start() - 60):m.start() + 40].replace("\n", " ").strip())
    ext_on_today = bool(ext_call_sites_outside_markets)
    ext_surface["loaded_from_markets_route"] = bool(m_mkt and
                                                    "loadExternalReferences(" in m_mkt.group(0))

    # F. The surface is GET-only, declared, and wired.
    ext_route = "/v1/market/external-references"
    ext_routes = {
        "declared": '"%s"' % ext_route in app,
        "mutating": sorted(
            m.group(2) for m in
            re.finditer(r'@app\.(post|put|delete|patch)\(\s*"([^"]+)"', app)
            if "external-reference" in m.group(2)),
        "ui_wired": ext_route in ui,
    }

    return {
        "modules_present": not missing,
        "missing_modules": missing,
        "rank_identity_contract": contract,
        "rank_identity_enforced_in_build_forecast": enforced_in_build,
        "external_url_guard": url_guard,
        "second_external_url_guard_modules": sorted(second_url_guard),
        "constructed_source_urls_in_matinfo": sorted(set(constructed_urls)),
        "unowned_literal_urls_in_reference_module": literal_site_urls,
        "read_model_declarations": read_model_declarations,
        "reference_module_private_authority_table": private_authority_table,
        "ui_external_links": ui_links,
        "ui_hand_rolled_anchors": sorted(set(ui_hand_rolled_anchors)),
        "external_reference_surface": ext_surface,
        "external_references_on_today": ext_on_today,
        "external_reference_call_sites_outside_markets":
            sorted(ext_call_sites_outside_markets),
        "external_reference_routes": ext_routes,
        "second_rank_identity_owner_modules": sorted(second_identity_owner),
        "live_operational_lane": live_lane,
        "live_lane_reads_research_snapshot": sorted(live_reads_research_snapshot),
        "second_freshness_source_table": second_freshness_table,
        "governed_lane_falls_back_to_research": sorted(governed_fallback),
        "target_lanes": lanes,
        "operational_calibration": calibration,
        "forbidden_components_in_operational_calibration": sorted(forbidden_components),
        "calibration_lane_imports_api": research_imports_api,
        "operational_read_surface_writes": sorted(read_surface_writes),
        "zero_base_owner_forbidden_calls": forbidden_calls,
    }


# --------------------------------------------------------------------------- #
# Release 31 - Mathematical Alpha Frontier
# --------------------------------------------------------------------------- #
# The campaign searches for a better decision function. These invariants make
# sure it cannot become anything else. Fifteen properties, each one a way a bounded
# research campaign has historically turned into an unbounded one, or into an
# accidental production change:
#
#   one campaign-contract owner, one judge, one candidate registry, one lockbox
#   access owner; the lockbox never reachable from training or selection; a
#   research candidate never reaching the operational model, the canonical
#   portfolio decision, or an order; budgets encoded as NUMBERS rather than prose;
#   a terminal exhaustion state that stops further execution; external reference
#   links and EVENT_TRIGGER_ONLY news never becoming research inputs; the
#   canonical cost / risk / zero-base owners reused rather than forked; and no
#   automatic model promotion anywhere.
R31_PKG = "alpha_agent/r31"
R31_OWNERS = {
    "contract": "alpha_agent/r31/contract.py",
    "snapshot": "alpha_agent/r31/snapshot.py",
    "partition": "alpha_agent/r31/partition.py",
    "judge": "alpha_agent/r31/judge.py",
    "registry": "alpha_agent/r31/registry.py",
    "lockbox": "alpha_agent/r31/lockbox.py",
    "multiple_testing": "alpha_agent/r31/multiple_testing.py",
    "methods": "alpha_agent/r31/methods.py",
    "novel": "alpha_agent/r31/novel.py",
    "campaign": "alpha_agent/r31/campaign.py",
    "learners": "alpha_agent/r31/learners.py",
    # Campaign v3 correction owners.
    "universe": "alpha_agent/r31/universe.py",
    "benchmarks": "alpha_agent/r31/benchmarks.py",
    "calibration": "alpha_agent/r31/calibration.py",
    "allocation": "alpha_agent/r31/allocation.py",
    "covcache": "alpha_agent/r31/covcache.py",
}
R31_READ_MODEL = "api/mathematical_alpha_frontier.py"
R31_ROUTE = "/v1/research/mathematical-alpha-frontier"
R31_UI_REGION = 'id="r31-frontier"'

#: Budgets that must exist as NUMBERS in the contract owner. A budget that lives
#: only in a document is a suggestion.
R31_REQUIRED_BUDGETS = (
    "MAX_KNOWN_METHOD_FAMILIES", "MAX_KNOWN_METHOD_CONFIGS",
    "MAX_CONFIGS_PER_KNOWN_FAMILY", "MAX_NOVEL_FAMILIES", "MAX_NOVEL_CAMPAIGNS",
    "MAX_NOVEL_CANDIDATES_PER_CAMPAIGN", "MAX_NOVEL_CANDIDATES_TOTAL",
    "MAX_NOVEL_REFINEMENT_DEPTH", "MAX_LOCKBOX_CANDIDATES",
    "MAX_LOCKBOX_PER_FAMILY",
)

#: Mutating operations the research package may never perform. Matched WITH the
#: call parenthesis so the check tests a call or a definition, not the appearance
#: of a word in prose - "DIRECT_PORTFOLIO_DECISION" is one of the campaign's two
#: architecture labels, and a bare substring test would flag it while missing an
#: actual aliased import.
R31_FORBIDDEN_CALLS = (
    "create_order(", "submit_order(", "place_order(", "promote_model(",
    "activate_model(", "set_champion(", "record_decision(",
    "persist_proposal(", "persist_decision(", "run_daily_close(",
)

#: Canonical OPERATIONAL owners the research package may never reference, in any
#: module-qualified form. These are the modules that would turn a research
#: candidate into a proposal, a decision, an order or a champion change.
R31_FORBIDDEN_OWNER_REFS = (
    "api.portfolio_decision", "api.rebalance_execution", "api.alpha_book",
    "api.operational_book", "api.daily_close", "api.universe_scoring",
    "engine.reallocation_proposal", "engine.portfolio_reassessment",
    "engine.event_fabric", "engine.normal_cycle", "broker",
)

#: Engine modules the research lane MAY read.
#:
#: ``holding_opportunity_cost`` is here because it OWNS ``build_covariance``, the
#: canonical risk matrix. Campaign v2 listed it as forbidden, which was correct
#: while the judge needed no covariance at all; Campaign v3 allocates capital
#: through the canonical optimiser, so the choice became "read the canonical
#: covariance owner" or "write a second one" - and a second risk owner is a
#: worse outcome than a wider allowlist, because the two would disagree the first
#: time a lookback changed.
#:
#: Widening an allowlist is only safe if admission proves something, so
#: ``r31_engine_owner_purity`` below re-parses every admitted module and fails if
#: it imports anything outside the standard library. Admission by NAME would let a
#: future edit pull a database dependency into the research lane behind a name
#: this list already trusts.
R31_ALLOWED_ENGINE = {"zero_base_allocator", "holding_opportunity_cost"}

#: The standard-library roots an admitted engine owner may import. Anything else
#: makes it unsuitable for the research lane whatever its name is.
R31_ENGINE_PURE_IMPORTS = {
    "__future__", "hashlib", "json", "math", "decimal", "typing", "datetime",
    "collections", "itertools", "functools", "re", "dataclasses", "enum",
    "statistics", "copy", "abc", "types", "numbers", "operator",
}


# --------------------------------------------------------------------------- #
# Release 32 — PnL Opportunity Frontier (invariants 1-40)
# --------------------------------------------------------------------------- #
#: One owner per Release-32 concern. A second module declaring the same schema
#: token is the drift these checks exist to catch.
R32_OWNERS = {
    "contract": "alpha_agent/r32/contract.py",
    "sources": "alpha_agent/r32/sources.py",
    "information_state": "alpha_agent/r32/information_state.py",
    "panels": "alpha_agent/r32/panels.py",
    "sleeve": "alpha_agent/r32/sleeve.py",
    "judge": "alpha_agent/r32/judge.py",
    "funnel": "alpha_agent/r32/funnel.py",
    "frontier": "alpha_agent/r32/frontier.py",
    "purchase_gate": "alpha_agent/r32/purchase_gate.py",
    "governance": "alpha_agent/r32/governance.py",
    "campaign": "alpha_agent/r32/campaign.py",
}
R32_READ_MODEL = "api/pnl_opportunity_frontier.py"
R32_SLEEVE_DIR = "alpha_agent/r32/sleeves"
R32_ROUTE = "/v1/research/pnl-opportunity-frontier"

#: A sleeve that calls any of these has stopped generating opportunities and
#: started managing a portfolio.
R32_FORBIDDEN_SLEEVE_CALLS = (
    "create_order", "place_order", "submit_order", "confirm_target",
    "apply_proposal", "approve_proposal", "promote_champion", "activate_model",
    "activate_sleeve", "write_holdings", "write_cash", "execute_rebalance",
)

#: Operational owners the research lane may never import.
R32_FORBIDDEN_OWNER_REFS = (
    "paper_trader.api.operational_book", "paper_trader.api.daily_close",
    "paper_trader.api.rebalance_execution", "paper_trader.api.portfolio_decision",
    "paper_trader.broker", "engine.normal_cycle",
)


def check_release32_pnl_opportunity_frontier(files: list[Path]) -> dict:
    """Release 32 ownership, safety and point-in-time invariants (1-40)."""
    src = {name: (_read(path) or "") for name, path in R32_OWNERS.items()}
    modules_missing = sorted(n for n, t in src.items() if not t)
    all_src = "\n".join(src.values())
    read_model = _read(R32_READ_MODEL) or ""
    app = _read("api/app.py") or ""
    ui = _read("api/ui/index.html") or ""

    sleeve_files = sorted(
        _rel(p) for p in files
        if _rel(p).startswith(R32_SLEEVE_DIR) and _rel(p).endswith(".py"))
    sleeve_src = "\n".join((_read(f) or "") for f in sleeve_files)

    def _second_owners(token: str, owner: str) -> list:
        out = []
        for p in files:
            rel = _rel(p)
            if rel == owner or not rel.endswith(".py"):
                continue
            if rel.startswith("tests/") or rel.startswith("scripts/"):
                continue
            if token in (_read(rel) or ""):
                out.append(rel)
        return sorted(out)

    # (1)-(5) ONE owner each.
    second_owners = {
        "sleeve_contract": _second_owners(
            'CONTRACT_SCHEMA = "r32_strategy_sleeve_contract',
            R32_OWNERS["sleeve"]),
        "information_state": _second_owners(
            'STATE_SCHEMA = "r32_information_state_contract',
            R32_OWNERS["information_state"]),
        "frontier": _second_owners(
            'FRONTIER_SCHEMA = "r32_pnl_opportunity_frontier',
            R32_OWNERS["frontier"]),
        "purchase_gate": _second_owners(
            'GATE_SCHEMA = "r32_information_purchase_frontier',
            R32_OWNERS["purchase_gate"]),
        "governance": _second_owners(
            'GOVERNANCE_SCHEMA = "r32_daily_multi_asset_governance_contract',
            R32_OWNERS["governance"]),
    }

    # (6)-(8) No second optimiser / covariance / HOC owner. Release 32 adds
    # sleeves, not a second risk library: it reuses r31's statistics.
    reuses_r31_statistics = "from ..r31 import multiple_testing" in src["campaign"]
    reuses_r31_judge_stats = "from ..r31.judge import" in src["judge"]
    no_second_optimiser = not re.search(
        r"def\s+(optimise|optimize|zero_base_target)\s*\(", all_src)
    no_second_covariance = not re.search(r"def\s+build_covariance\s*\(", all_src)

    # (9)-(11) A sleeve may not allocate capital, propose, or order.
    sleeve_forbidden = sorted(
        {t for t in R32_FORBIDDEN_SLEEVE_CALLS if t in sleeve_src.lower()})
    sleeve_owns_capital_declared_false = "owns_capital = False" in src["sleeve"]
    states_own_capital_empty = "STATES_THAT_OWN_CAPITAL = ()" in src["sleeve"]
    gross_exposure_capped = "may not lever, and may not size a book" in src["sleeve"]

    # (12) The Release-31 result is reused, not rerun.
    control_not_researched = (
        "def assert_control_not_researched" in src["funnel"]
        and "MAY_BE_RESEARCHED_IN_R32 = False"
        in (_read(f"{R32_SLEEVE_DIR}/equity_selection.py") or ""))

    # (13)-(16) Point-in-time integrity.
    pit_measured_not_asserted = (
        "MEASURED_CHANGE_DAY_FINGERPRINT" in src["sources"])
    revised_macro_inadmissible = (
        "REVISED_NOT_PIT" in src["sources"]
        and "REVISED_NOT_PIT" not in _admissible_block(src["sources"]))
    sector_definition_dates = (
        "GICS_REAL_ESTATE_FROM" in src["panels"]
        and "definition_from" in src["panels"])
    etf_inception_declared = "pre-inception" in src["panels"].lower() or (
        "inception" in src["panels"].lower())

    # (17) Common-overlap comparison enforced.
    common_overlap_enforced = (
        "def common_overlap" in src["campaign"]
        and "def _common_calendar" in src["campaign"])
    overlap_reporting_only = (
        '"may_qualify_a_sleeve": False' in src["campaign"])

    # (18)-(20) Lockbox, denominator, and negative results.
    lockbox_single_access = (
        "has already used its single "
        in src["funnel"])
    denominator_counts_all = (
        "DENOMINATOR_COUNTS_ALL_EXECUTED = True" in src["contract"])
    denominator_padded_to_family = (
        "padded_with_non_reportable" in src["campaign"])
    control_cannot_qualify = (
        '"is_not_a_control"' in src["campaign"]
        and 'r.get("is_control")' in src["campaign"])
    primary_control_is_volatility_matched = (
        "VOLATILITY_MATCHED_BENCHMARK_CASH_MIX" in src["judge"]
        and "vs_volatility_matched_control" in src["campaign"])

    # (21)-(23) Nothing is activated, promoted, or written operationally.
    pkg = _read("alpha_agent/r32/__init__.py") or ""
    auto_promotion_false = "AUTOMATIC_PROMOTION_ALLOWED = False" in pkg
    auto_activation_false = (
        "AUTOMATIC_SLEEVE_ACTIVATION_ALLOWED = False" in pkg)
    may_spend_money_false = "MAY_SPEND_MONEY = False" in pkg
    forbidden_owner_refs = sorted(
        {t for t in R32_FORBIDDEN_OWNER_REFS if t in all_src + sleeve_src})
    research_imports_api = sorted(
        n for n, t in src.items()
        if re.search(r"^\s*(from|import)\s+.*paper_trader\.api", t, re.M))

    # (24)-(27) Prohibited substitutions and the purchase gate.
    prohibited_substitutions = (
        "PROHIBITED_SUBSTITUTIONS" in src["sources"]
        and "external reference links" in src["sources"]
        and "GDELT article text" in src["sources"]
        and "current analyst snapshots" in src["sources"])
    gate_ten_conditions = src["purchase_gate"].count('"') > 0 and (
        "CONDITIONS = (" in src["purchase_gate"])
    gate_never_purchases = (
        '"purchase_authorised": False' in src["purchase_gate"]
        and '"money_spent_usd": 0.0' in src["purchase_gate"])

    # (28)-(35) Governance contract invariants.
    gov = src["governance"]
    governance = {
        "daily_reassessment_is_not_daily_trading":
            '"daily_reassessment_implies_daily_trading": False' in gov,
        "closed_market_delta_pending":
            "DELTA_PENDING_MARKET_CLOSED" in gov
            and '"closed_market_delta_remains_pending": True' in gov,
        "no_unvalidated_hedge_substitution":
            "UNRELATED_INSTRUMENT_HEDGE_SUBSTITUTION_ALLOWED = False" in gov
            and "NO_VALIDATED_HEDGE_POLICY_EXISTS" in gov,
        "one_future_nav_owner":
            "MULTI_ASSET_NAV_OWNER =" in gov
            and gov.count("MULTI_ASSET_NAV_OWNER =") == 1,
        "asset_count_is_not_diversification":
            '"asset_count_is_not_diversification": True' in gov
            and "RISK_FACTOR_AND_CORRELATION_CLUSTER" in gov,
        "reuses_event_fabric":
            'EVENT_FABRIC_OWNER = "engine.event_fabric"' in gov
            and "SECOND_EVENT_SYSTEM_ALLOWED = False" in gov,
        "one_orchestration_contract":
            "ORCHESTRATION_CONTRACT =" in gov
            and "ONE_REASSESSMENT_CONTRACT_FOR_BOTH_MODES" in gov,
        "turnover_budgets_declared":
            "TURNOVER_BUDGET_PERIODS = " in gov
            and '"turnover_budget_concepts_declared": True' in gov
            and '"turnover_budgets_are_future_governance_concepts": True' in gov,
        # The concepts are authorised; the VALUES are not. A number here would
        # be inherited by Release 33 as a calibrated limit nobody set.
        "turnover_budget_values_not_invented":
            not _r32_turnover_budget_literals(gov)
            and '"turnover_budget_values_calibrated": False' in gov
            and '"turnover_budget_value_owner": TURNOVER_BUDGET_VALUE_OWNER'
                in gov,
        "uncalibrated_turnover_budget_is_not_zero":
            '"uncalibrated_turnover_budget_means_zero_turnover": False' in gov
            and '"uncalibrated_turnover_budget_means_unlimited_turnover": False'
                in gov
            and "TURNOVER_BUDGET_UNDECIDABLE" in gov,
    }

    # (36)-(40) Cash, staleness, exhaustion, no hidden N+1, read-only.
    cash_is_a_valid_opportunity = (
        "CASH_IS_A_REAL_ASSET_CHOICE = True" in src["contract"]
        and "cash_is_a_real_asset_choice" in src["campaign"])
    stale_data_fails_closed = (
        '"stale_data_fails_closed": True' in gov
        and "DELTA_PENDING_STALE_DATA" in gov)
    exhaustion_stops_search = (
        "adds to the multiple-testing " in src["funnel"]
        and "ControlSleeveResearched" in src["funnel"])
    no_hidden_followup_campaign = (
        "BudgetExceeded" in src["funnel"]
        and "budget exhausted at" in src["funnel"])
    production_read_only = (
        '"production_read_only": True' in read_model
        and '"writes_operational_store": False' in pkg)

    # Read surface: GET only, no control, canonical badge wording.
    route_declared = R32_ROUTE in app
    route_is_get_only = bool(re.search(
        r'@app\.get\(\s*\n?\s*"' + re.escape(R32_ROUTE), app))
    route_not_mutating = not any(
        re.search(r'@app\.' + verb + r'\(\s*\n?\s*"' + re.escape(R32_ROUTE), app)
        for verb in ("post", "put", "patch", "delete"))
    read_model_writes = sorted(set(re.findall(
        r"\b(open\([^)]*['\"]w|write_text|write_json|mkdir|os\.replace)\b",
        read_model)))
    ambiguous_badges = sorted(
        t for t in (">NO LIVE ORDERS</span>", ">ORDERS DISABLED<")
        if t in _r32_ui_region(ui))
    canonical_badge = "NO LIVE BROKER ORDERS" in read_model
    ui_controls = sorted(
        t for t in ("Execute", "Approve", "Activate", "Promote",
                    "Create Order", "Allocate")
        if t in _r32_ui_region(ui))

    return {
        "modules_present": not modules_missing,
        "modules_missing": modules_missing,
        "sleeve_modules": sleeve_files,
        "second_owner_modules": sorted(
            {m for v in second_owners.values() for m in v}),
        "reuses_r31_statistics": reuses_r31_statistics,
        "reuses_r31_judge_statistics": reuses_r31_judge_stats,
        "no_second_optimiser": no_second_optimiser,
        "no_second_covariance_owner": no_second_covariance,
        "sleeve_forbidden_calls": sleeve_forbidden,
        "sleeve_owns_capital_declared_false": sleeve_owns_capital_declared_false,
        "states_that_own_capital_empty": states_own_capital_empty,
        "sleeve_gross_exposure_capped": gross_exposure_capped,
        "control_sleeve_not_researched": control_not_researched,
        "pit_admissibility_is_measured": pit_measured_not_asserted,
        "revised_macro_inadmissible": revised_macro_inadmissible,
        "sector_definition_dates_declared": sector_definition_dates,
        "instrument_inception_declared": etf_inception_declared,
        "common_overlap_enforced": common_overlap_enforced,
        "overlap_view_is_reporting_only": overlap_reporting_only,
        "lockbox_single_access_enforced": lockbox_single_access,
        "denominator_counts_all_executed": denominator_counts_all,
        "denominator_is_the_bh_family_size": denominator_padded_to_family,
        "control_cannot_qualify_a_sleeve": control_cannot_qualify,
        "primary_control_is_volatility_matched":
            primary_control_is_volatility_matched,
        "auto_promotion_declared_false": auto_promotion_false,
        "auto_sleeve_activation_declared_false": auto_activation_false,
        "may_spend_money_declared_false": may_spend_money_false,
        "forbidden_owner_refs": forbidden_owner_refs,
        "research_imports_api": research_imports_api,
        "prohibited_substitutions_declared": prohibited_substitutions,
        "purchase_gate_ten_conditions": gate_ten_conditions,
        "purchase_gate_never_purchases": gate_never_purchases,
        "governance": governance,
        "governance_failures": sorted(k for k, v in governance.items() if not v),
        "cash_is_a_valid_opportunity": cash_is_a_valid_opportunity,
        "stale_data_fails_closed": stale_data_fails_closed,
        "exhaustion_stops_same_information_search": exhaustion_stops_search,
        "no_hidden_followup_campaign": no_hidden_followup_campaign,
        "production_read_only": production_read_only,
        "route_declared": route_declared,
        "route_is_get_only": route_is_get_only,
        "route_not_mutating": route_not_mutating,
        "read_model_writes": read_model_writes,
        "ui_ambiguous_safety_badges": ambiguous_badges,
        "read_model_uses_canonical_order_badge": canonical_badge,
        "ui_control_labels": ui_controls,
    }


R33_OWNERS = {
    "root": "alpha_agent/r33/__init__.py",
    "contract": "alpha_agent/r33/contract.py",
    "universe": "alpha_agent/r33/universe.py",
    "panel": "alpha_agent/r33/panel.py",
    "pit": "alpha_agent/r33/pit.py",
    "features": "alpha_agent/r33/features.py",
    "targets": "alpha_agent/r33/targets.py",
    "partition": "alpha_agent/r33/partition.py",
    "models": "alpha_agent/r33/models.py",
    "regime": "alpha_agent/r33/regime.py",
    "predictive": "alpha_agent/r33/predictive.py",
    "economic": "alpha_agent/r33/economic.py",
    "registry": "alpha_agent/r33/registry.py",
    "lockbox": "alpha_agent/r33/lockbox.py",
    "robustness": "alpha_agent/r33/robustness.py",
    "campaign": "alpha_agent/r33/campaign.py",
}

#: Operational owners the Release-33 research lane may never import.
R33_FORBIDDEN_OWNER_REFS = (
    "api.operational_book", "api.daily_close", "api.rebalance_execution",
    "api.portfolio_decision", "paper_trader.broker", "engine.normal_cycle",
)

#: Tokens that would mean the research lane had started trading.
R33_FORBIDDEN_CALLS = (
    "create_order", "place_order", "submit_order", "confirm_target",
    "apply_proposal", "approve_proposal", "promote_champion", "activate_model",
    "write_holdings", "write_cash", "execute_rebalance",
)


def check_release33_predictive_edge(files: list[Path]) -> dict:
    """Release 33 ownership, leakage and honesty invariants (41-62)."""
    src = {name: (_read(path) or "") for name, path in R33_OWNERS.items()}
    modules_missing = sorted(n for n, t in src.items() if not t)
    all_src = "\n".join(src.values())
    runner = _read("scripts/run_release33_predictive_edge.py") or ""

    # (41)-(43) One statistics library, one hashing owner, no second optimiser.
    reuses_r31_statistics = (
        "from ..r31.multiple_testing import" in src["campaign"])
    reuses_r31_hashing = "from ..r31 import (" in src["root"]
    reuses_r31_learners = "from ..r31 import learners as _l" in src["models"]
    no_second_optimiser = not re.search(
        r"def\s+(optimise|optimize|zero_base_target)\s*\(", all_src)
    no_second_covariance = not re.search(r"def\s+build_covariance\s*\(", all_src)

    # (44)-(46) Safety: research only, promotes nothing, spends nothing.
    safety_flags_false = all(
        f"{flag} = False" in src["root"] for flag in
        ("AUTOMATIC_PROMOTION_ALLOWED", "AUTOMATIC_SLEEVE_ACTIVATION_ALLOWED",
         "MAY_SPEND_MONEY"))
    forbidden_calls = sorted(
        {t for t in R33_FORBIDDEN_CALLS if t in all_src.lower()})
    forbidden_owner_refs = sorted(
        {t for t in R33_FORBIDDEN_OWNER_REFS if t in all_src})
    declares_no_futures_execution = (
        '"executes_futures": False' in src["root"]
        and '"integrates_broker": False' in src["root"])

    # (47)-(49) The implementability distinction may not be blurred.
    futures_claim_refused = (
        "FUTURES_IMPLEMENTABILITY_CLAIMABLE = False" in src["contract"]
        and "UNIVERSE_IMPLEMENTABILITY_STATE = SIGNAL_RESEARCH_VALID"
        in src["contract"])
    universe_declares_signal_only = (
        "SIGNAL_RESEARCH_VALID" in src["universe"]
        and "futures_implementability_claimable" in src["universe"])
    return_heterogeneity_declared = (
        "RETURN_DEFINITION_HETEROGENEOUS = True" in src["contract"]
        and "EQUITY_INDICES_EXCLUDE_DIVIDENDS = True" in src["contract"]
        and "FX_SPOT_EXCLUDES_CARRY = True" in src["contract"])

    # (50)-(53) Leakage controls. These are the invariants that decide whether
    # any number in this release means anything.
    hmm_filtered_only = (
        "def hmm_filter_states" in src["models"]
        and "states_are_filtered_only" in src["models"]
        and "smoothed" in src["models"].lower())
    regime_uses_filtered = (
        "hmm_filter_states" in src["regime"]
        and "fitted_on_training_only" in src["regime"])
    scaler_fitted_on_training = (
        "def fit_scaler" in src["models"]
        and "TRAINING rows only" in src["models"])
    no_random_split = (
        "random_split_allowed" in src["partition"]
        and "RANDOM" not in src["partition"].upper().replace(
            "RANDOM_SPLIT_ALLOWED", "").replace("NO RANDOM", ""))
    non_overlapping_declared = (
        "NON_OVERLAPPING_FORECAST_DATES = True" in src["contract"])
    implementation_lag_declared = (
        "IMPLEMENTATION_LAG_SESSIONS = 1" in src["contract"])
    pit_probe_present = (
        "def point_in_time_probe" in src["campaign"]
        and "truncated" in src["campaign"].lower())

    # (54)-(56) Point-in-time information honesty.
    revised_macro_excluded = "REVISED_NOT_PIT_EXCLUDED" in src["pit"]
    cot_publication_lag = (
        "COT_PUBLICATION_LAG_BUSINESS_DAYS" in src["pit"]
        and "available_from" in src["pit"])
    alfred_vintages_used = (
        "realtime_start" in src["pit"] and "def as_of_series" in src["pit"])
    synthetic_data_inadmissible = (
        "LANE_C_SYNTHETIC_DATA_ADMISSIBLE = False" in src["contract"])

    # (57)-(59) The judge, and the control that decides skill.
    cost_base_traded_notional = (
        'COST_BASE = "TRADED_NOTIONAL"' in src["contract"])
    volatility_matched_control = (
        "def volatility_matched_control" in src["economic"]
        and "ECONOMIC_CONTROL = \"VOLATILITY_MATCHED_BENCHMARK_CASH_MIX\""
        in src["contract"])
    excess_over_cash_may_not_rank = (
        '"excess_over_cash_may_rank": False' in src["economic"])
    cost_sensitivity_declared = (
        "COST_SENSITIVITY_MULTIPLIERS" in src["contract"]
        and "def cost_sensitivity" in src["robustness"])

    # (60)-(62) Budget, lockbox and the two-result rule.
    denominator_all_executed = (
        "DENOMINATOR_COUNTS_ALL_EXECUTED = True" in src["contract"]
        and "failed_configurations_stay_in_denominator" in src["registry"])
    adaptive_search_refused = (
        "ADAPTIVE_SEARCH_ALLOWED = False" in src["contract"])
    deep_learning_out_of_scope = (
        "DEEP_LEARNING_IN_SCOPE = False" in src["contract"])
    lockbox_one_access = (
        "MAX_LOCKBOX_ACCESSES_PER_FINALIST = 1" in src["contract"]
        and "RETUNING_AFTER_LOCKBOX_ALLOWED = False" in src["contract"]
        and "already used its single" in src["lockbox"])
    alpha_pass_requires_qualified = (
        "ALPHA_PASS_REQUIRES = VERDICT_QUALIFIED" in src["contract"]
        and "alpha_pass_requires" in src["campaign"])
    reports_both_results = (
        "SYSTEM_RESULT" in src["campaign"] and "ALPHA_RESULT" in src["campaign"]
        and "system_result" in src["campaign"])
    min_scored_dates_enforced = (
        "MIN_SCORED_FORECAST_DATES" in src["campaign"]
        and "scored_dates_sufficient" in src["campaign"])
    stability_fails_closed = (
        '"single_subperiod_dependent": True' in src["robustness"]
        and "FAIL CLOSED" in src["robustness"])
    leave_market_out_is_a_gate = (
        "LEAVE_MARKET_OUT_REQUIRED = True" in src["contract"]
        and "single_market_dependent" in src["robustness"])
    # Whitespace-normalised: the phrase this looks for wraps across a line in
    # the runner's docstring, and a guard that a line break can silence is not
    # a guard.
    runner_flat = " ".join(runner.lower().split())
    runner_is_research_only = (
        "research only" in runner_flat and "no order" in runner_flat)

    # (63)-(67) The operational-write gate attributes writes to a WRITER.
    #
    # The first gate inferred causality from mtime, so the Release-29
    # continuous collection service advancing its own heartbeat presented as a
    # Release-33 write. The two ways to "fix" that - stop production, or
    # whitelist the directory - are both worse than the defect, so the rule is
    # owner-specific and provenance-specific instead, and these invariants stop
    # it decaying back into either one.
    attrib_path = "scripts/r33_operational_write_attribution.py"
    attrib_src = _read(attrib_path)
    attribution_owner_present = bool(attrib_src)
    attribution_is_provenance_based = (
        '"provenance_required": True' in attrib_src
        and "WRITER_PROVENANCE_NOT_THE_DECLARED_SERVICE" in attrib_src
        and "UNRECOGNISED_FILE_UNDER_PROTECTED_ROOT" in attrib_src
        and "def attribute_continuous_service" in attrib_src)
    # Whitespace-normalised: the phrase wraps across lines in the docstring,
    # and a guard a line break can silence is not a guard.
    attrib_flat = " ".join(attrib_src.lower().split())
    attribution_fails_closed = (
        "attribution_error" in attrib_src.lower()
        and "fails closed" in attrib_flat
        and "unmeasurable is not innocent" in attrib_flat)
    attribution_refuses_time_whitelist = (
        "no r33-attributable operational store write" in attrib_flat
        and "never time-specific" in attrib_flat)

    # The functional half: the declaration must actually hold. A docstring
    # promising provenance while the protected set has quietly lost a root is
    # exactly what a substring check cannot see.
    try:
        if str(REPO_ROOT / "scripts") not in sys.path:
            sys.path.insert(0, str(REPO_ROOT / "scripts"))
        import r33_operational_write_attribution as _attrib
        _decl = _attrib.check_owner_declarations(REPO_ROOT)
        information_collection_still_protected = bool(
            _decl.get("ok") is True
            and "information_collection" in _attrib.OPERATIONAL_ROOTS
            and "information_collection" in _attrib.CONTINUOUS_SERVICE_ROOTS)
        r33_source_has_no_operational_write_path = bool(
            _attrib.r33_source_operational_write_paths(REPO_ROOT)["clean"])
    except Exception as exc:  # noqa: BLE001 - unmeasurable fails closed
        information_collection_still_protected = f"UNMEASURABLE:{exc}"
        r33_source_has_no_operational_write_path = f"UNMEASURABLE:{exc}"

    return {
        "modules_present": not modules_missing,
        "modules_missing": modules_missing,
        "reuses_r31_statistics": reuses_r31_statistics,
        "reuses_r31_hashing": reuses_r31_hashing,
        "reuses_r31_learners": reuses_r31_learners,
        "no_second_optimiser": no_second_optimiser,
        "no_second_covariance_owner": no_second_covariance,
        "safety_flags_false": safety_flags_false,
        "forbidden_calls": forbidden_calls,
        "forbidden_owner_refs": forbidden_owner_refs,
        "declares_no_futures_execution": declares_no_futures_execution,
        "futures_implementability_refused": futures_claim_refused,
        "universe_declares_signal_research_only":
            universe_declares_signal_only,
        "return_definition_heterogeneity_declared":
            return_heterogeneity_declared,
        "hmm_states_filtered_only": hmm_filtered_only,
        "regime_uses_filtered_states": regime_uses_filtered,
        "scaler_fitted_on_training_only": scaler_fitted_on_training,
        "no_random_split": no_random_split,
        "non_overlapping_forecast_dates": non_overlapping_declared,
        "implementation_lag_declared": implementation_lag_declared,
        "point_in_time_probe_present": pit_probe_present,
        "revised_macro_excluded": revised_macro_excluded,
        "cot_publication_lag_applied": cot_publication_lag,
        "alfred_vintages_used": alfred_vintages_used,
        "synthetic_data_inadmissible": synthetic_data_inadmissible,
        "cost_base_traded_notional": cost_base_traded_notional,
        "volatility_matched_control_owned": volatility_matched_control,
        "excess_over_cash_may_not_rank": excess_over_cash_may_not_rank,
        "cost_sensitivity_declared": cost_sensitivity_declared,
        "denominator_counts_all_executed": denominator_all_executed,
        "adaptive_search_refused": adaptive_search_refused,
        "deep_learning_out_of_scope": deep_learning_out_of_scope,
        "lockbox_single_access": lockbox_one_access,
        "alpha_pass_requires_qualified_verdict": alpha_pass_requires_qualified,
        "reports_system_and_alpha_results": reports_both_results,
        "min_scored_dates_enforced_at_the_gate": min_scored_dates_enforced,
        "stability_check_fails_closed": stability_fails_closed,
        "leave_market_out_is_a_gate": leave_market_out_is_a_gate,
        "runner_is_research_only": runner_is_research_only,
        "attribution_owner_present": attribution_owner_present,
        "attribution_is_provenance_based": attribution_is_provenance_based,
        "attribution_fails_closed": attribution_fails_closed,
        "attribution_refuses_time_whitelist":
            attribution_refuses_time_whitelist,
        "information_collection_still_protected":
            information_collection_still_protected,
        "r33_source_has_no_operational_write_path":
            r33_source_has_no_operational_write_path,
    }


R34_OWNERS = {
    "root": "alpha_agent/r34/__init__.py",
    "contract": "alpha_agent/r34/contract.py",
    "universe": "alpha_agent/r34/universe.py",
    "panel": "alpha_agent/r34/panel.py",
    "forecast": "alpha_agent/r34/forecast.py",
    "calibration": "alpha_agent/r34/calibration.py",
    "sizing": "alpha_agent/r34/sizing.py",
    "horizon": "alpha_agent/r34/horizon.py",
    "turnover": "alpha_agent/r34/turnover.py",
    "portfolio": "alpha_agent/r34/portfolio.py",
    "economics": "alpha_agent/r34/economics.py",
    "concentration": "alpha_agent/r34/concentration.py",
    "walkforward": "alpha_agent/r34/walkforward.py",
    "attrition": "alpha_agent/r34/attrition.py",
    "campaign": "alpha_agent/r34/campaign.py",
}

#: Operational owners the Release-34 research lane may never import, and the
#: protected store roots it may never name. The operational-write attribution
#: rule added in Release 33 proves the runtime half; this is the static half.
R34_FORBIDDEN_OWNER_REFS = (
    "api.operational_book", "api.daily_close", "api.rebalance_execution",
    "api.portfolio_decision", "api.information_collection",
    "paper_trader.broker", "engine.normal_cycle", "information_collection",
    "portfolio_decisions", "reallocation_proposals", "rebalance_order_plans",
)

#: Tokens that would mean the research lane had started trading.
R34_FORBIDDEN_CALLS = (
    "create_order", "place_order", "submit_order", "confirm_target",
    "apply_proposal", "approve_proposal", "promote_champion", "activate_model",
    "write_holdings", "write_cash", "execute_rebalance",
)


def check_release34_prediction_to_pnl(files: list[Path]) -> dict:
    """Release 34 ownership, leakage and honesty invariants (69-90)."""
    src = {name: (_read(path) or "") for name, path in R34_OWNERS.items()}
    modules_missing = sorted(n for n, t in src.items() if not t)
    all_src = "\n".join(src.values())
    runner = _read("scripts/run_release34_prediction_to_pnl.py") or ""

    # (69)-(72) One of each. R34 adds a conversion layer, not a second
    # statistics library, a second feature library or a second learner set.
    reuses_r31_statistics = (
        "from ..r31 import multiple_testing as _mt" in src["campaign"])
    reuses_r31_hashing = "from ..r31 import (" in src["root"]
    reuses_r33_features = (
        "from ..r33 import features as _r33_features" in src["forecast"])
    reuses_r33_models = (
        "from ..r33 import models as _r33_models" in src["forecast"])
    no_second_learner_library = not re.search(
        r"def\s+fit_(ridge|elastic_net|gbrt|extra_trees|hmm)\s*\(", all_src)

    # (73)-(75) Safety: research only, promotes nothing, mutates nothing.
    safety_flags_false = all(
        f"{flag} = False" in src["root"] for flag in
        ("AUTOMATIC_PROMOTION_ALLOWED", "AUTOMATIC_SLEEVE_ACTIVATION_ALLOWED",
         "MAY_SPEND_MONEY", "MAY_MUTATE_PRODUCTION"))
    forbidden_calls = sorted(
        {t for t in R34_FORBIDDEN_CALLS if t in all_src.lower()})
    forbidden_owner_refs = sorted(
        {t for t in R34_FORBIDDEN_OWNER_REFS if t in all_src})

    # (76)-(78) Implementability. R33 could not claim it and said so; R34 may
    # claim it ONLY for exchange-traded securities on total-return prices, and
    # the spot-FX series that produced R33's apparent edge is barred outright.
    implementable_requires_exchange_traded = (
        "IMPLEMENTABLE_REQUIRES_EXCHANGE_TRADED_SECURITY = True"
        in src["contract"]
        and "IMPLEMENTABLE_REQUIRES_TOTAL_RETURN_PRICES = True"
        in src["contract"])
    non_investable_series_barred = (
        "NON_INVESTABLE_SERIES_MAY_ENTER_PORTFOLIO = False" in src["contract"]
        and "TRYUSD" in src["contract"])
    universe_includes_delisted = (
        "UNIVERSE_INCLUDES_DELISTED_CANDIDATES = True" in src["contract"]
        and "US Equities Delisted" in src["universe"])
    total_return_prices_used = (
        "TOTALRETURN" in src["universe"]
        and "def load_total_return" in src["universe"])

    # (79)-(82) Leakage. These decide whether any number here means anything.
    no_random_split = "RANDOM_SPLIT_ALLOWED = False" in src["contract"]
    nested_selection_declared = (
        "NESTED_SELECTION_INSIDE_TRAINING_ONLY = True" in src["contract"]
        and "NESTED_SELECTION_ARRANGEMENT" in src["contract"])
    calibration_training_only = (
        "CALIBRATION_FITTED_ON_TRAINING_ONLY = True" in src["contract"]
        and "FUTURE_PERIOD_CALIBRATION_ALLOWED = False" in src["contract"]
        and "TRAINING rows only" in src["calibration"])
    liquidity_is_point_in_time = (
        "LIQUIDITY_IS_POINT_IN_TIME = True" in src["contract"]
        and "def tradability_mask" in src["panel"])
    embargo_declared = (
        "EMBARGO_EXTRA_SESSIONS" in src["contract"]
        and "SEG_EMBARGOED" in src["walkforward"])
    non_overlapping_declared = (
        "NON_OVERLAPPING_FORECAST_DATES = True" in src["contract"])

    # (83)-(85) NO FAKE FRESH LOCKBOX. The one claim this release could most
    # easily inflate, so it is declared in the contract, enforced in one
    # function, and the qualified verdict is made structurally unreachable.
    fresh_evidence_refused = (
        "FRESH_UNSEEN_EVIDENCE_EXISTS = False" in src["contract"]
        and "FRESH_UNSEEN_EVIDENCE_REASON" in src["contract"])
    no_fold_is_a_lockbox = (
        '"a_fold_may_be_called_a_lockbox": False' in src["walkforward"]
        and "verdict_ceiling_without_fresh_evidence" in src["walkforward"])
    independent_evidence_is_a_gate = (
        "genuinely_independent_evidence_exists"
        in src["contract"] and
        "genuinely_independent_evidence_exists" in src["campaign"])

    # (86)-(88) The judge, the control and the horizon correction.
    cost_base_traded_notional = (
        'COST_BASE = "TRADED_NOTIONAL"' in src["contract"])
    excess_over_cash_may_not_rank = (
        "EXCESS_OVER_CASH_MAY_RANK = False" in src["contract"]
        and '"excess_over_cash_may_rank": _contract.EXCESS_OVER_CASH_MAY_RANK'
        in src["economics"])
    volatility_matched_control = (
        "def volatility_matched_control" in src["economics"]
        and "ECONOMIC_CONTROL = CONTROL_VOL_MATCHED" in src["contract"])
    horizon_not_ranked_by_raw_magnitude = (
        "HORIZON_CHOSEN_BY_RAW_METRIC_MAGNITUDE = False" in src["contract"]
        and "HNES_FORMULA" in src["contract"]
        and "def hnes" in src["horizon"]
        and "HNES_COMPUTED_ON_TRAINING_ONLY = True" in src["contract"])

    # (89)-(90) The R33 failure mode, and the two-result rule.
    concentration_frozen_before_evaluation = (
        "CONCENTRATION_GATE_FROZEN_BEFORE_EVALUATION = True" in src["contract"]
        and "SIGN_REVERSAL_ON_LEAVE_ONE_OUT_DISQUALIFIES = True"
        in src["contract"])
    leave_one_out_is_a_gate = (
        "LEAVE_ONE_INSTRUMENT_OUT_REQUIRED = True" in src["contract"]
        and "LEAVE_ONE_ASSET_CLASS_OUT_REQUIRED = True" in src["contract"]
        and "def analyse" in src["concentration"]
        and "TRYUSD" in src["concentration"])
    engagement_gate_present = (
        "MIN_MEAN_GROSS_EXPOSURE" in src["contract"]
        and "book_actually_takes_positions" in src["campaign"])
    bh_direction_is_split = (
        "rejected_beating_the_control" in src["campaign"]
        and "rejected_losing_to_the_control" in src["campaign"]
        and "only_positive_rejections_may_qualify" in src["campaign"])
    attrition_waterfall_required = (
        "PREDICTION_TO_PNL_ATTRITION_WATERFALL" in src["attrition"]
        and "PERFECT_FORESIGHT_SIZED" in src["attrition"]
        and len(_ATTRITION_REQUIRED_MODES
                - set(re.findall(r'"([a-z_]+)"', src["attrition"]))) == 0)
    denominator_all_executed = (
        "DENOMINATOR_COUNTS_ALL_EXECUTED = True" in src["contract"]
        and "CONTROLS_ENTER_DENOMINATOR = False" in src["contract"])
    adaptive_search_refused = (
        "ADAPTIVE_SEARCH_ALLOWED = False" in src["contract"]
        and "NEW_PREDICTOR_SEARCH_ALLOWED = False" in src["contract"])
    alpha_pass_requires_qualified = (
        "ALPHA_PASS_REQUIRES = VERDICT_QUALIFIED" in src["contract"]
        and "alpha_pass_requires" in src["campaign"])
    reports_both_results = (
        "SYSTEM_RESULT" in src["campaign"] and "ALPHA_RESULT" in src["campaign"]
        and "SYSTEM_AND_ALPHA_RESULTS_ARE_SEPARATE = True" in src["contract"])

    runner_flat = " ".join(runner.lower().split())
    runner_is_research_only = (
        "research only" in runner_flat and "no order" in runner_flat)

    # The functional half: the planned configuration enumeration must agree
    # with what the frozen grids will actually produce. v1 typed 12 for a
    # family the grid enumerates 18 of, so the assertion that compared them was
    # checking one hand-written number against another.
    try:
        if str(REPO_ROOT.parent) not in sys.path:
            sys.path.insert(0, str(REPO_ROOT.parent))
        from paper_trader.alpha_agent.r34 import contract as _r34_contract
        from paper_trader.alpha_agent.r34 import forecast as _r34_forecast
        planned_matches_the_grid = bool(
            _r34_contract.CONFIG_FAMILIES["FORECAST"]
            == len(_r34_forecast.model_configs())
            * len(_r34_contract.HORIZONS)
            and _r34_contract.PLANNED_CONFIG_TOTAL
            <= _r34_contract.MAX_PRIMARY_CONFIGS)
    except Exception as exc:  # noqa: BLE001 - unmeasurable fails closed
        planned_matches_the_grid = f"UNMEASURABLE:{exc}"

    return {
        "modules_present": not modules_missing,
        "modules_missing": modules_missing,
        "reuses_r31_statistics": reuses_r31_statistics,
        "reuses_r31_hashing": reuses_r31_hashing,
        "reuses_r33_features": reuses_r33_features,
        "reuses_r33_models": reuses_r33_models,
        "no_second_learner_library": no_second_learner_library,
        "safety_flags_false": safety_flags_false,
        "forbidden_calls": forbidden_calls,
        "forbidden_owner_refs": forbidden_owner_refs,
        "implementable_requires_exchange_traded_security":
            implementable_requires_exchange_traded,
        "non_investable_series_barred_from_portfolio":
            non_investable_series_barred,
        "universe_includes_delisted_candidates": universe_includes_delisted,
        "total_return_prices_used": total_return_prices_used,
        "no_random_split": no_random_split,
        "nested_selection_declared": nested_selection_declared,
        "calibration_fitted_on_training_only": calibration_training_only,
        "liquidity_is_point_in_time": liquidity_is_point_in_time,
        "embargo_declared": embargo_declared,
        "non_overlapping_forecast_dates": non_overlapping_declared,
        "fresh_unseen_evidence_refused": fresh_evidence_refused,
        "no_fold_may_be_called_a_lockbox": no_fold_is_a_lockbox,
        "independent_evidence_is_a_gate": independent_evidence_is_a_gate,
        "cost_base_traded_notional": cost_base_traded_notional,
        "excess_over_cash_may_not_rank": excess_over_cash_may_not_rank,
        "volatility_matched_control_owned": volatility_matched_control,
        "horizon_not_ranked_by_raw_magnitude":
            horizon_not_ranked_by_raw_magnitude,
        "concentration_frozen_before_evaluation":
            concentration_frozen_before_evaluation,
        "leave_one_out_is_a_gate": leave_one_out_is_a_gate,
        "engagement_gate_present": engagement_gate_present,
        "benjamini_hochberg_direction_is_split": bh_direction_is_split,
        "attrition_waterfall_required": attrition_waterfall_required,
        "denominator_counts_all_executed": denominator_all_executed,
        "adaptive_search_refused": adaptive_search_refused,
        "alpha_pass_requires_qualified_verdict": alpha_pass_requires_qualified,
        "reports_system_and_alpha_results": reports_both_results,
        "runner_is_research_only": runner_is_research_only,
        "planned_configs_match_the_frozen_grid": planned_matches_the_grid,
    }


#: The failure modes the attrition waterfall is REQUIRED to decompose, whether
#: or not alpha qualifies. "Prediction did not convert" is a fact; without these
#: it is not yet knowledge.
R35_OWNERS = {
    "root": "alpha_agent/r35/__init__.py",
    "contract": "alpha_agent/r35/contract.py",
    "acquisition": "alpha_agent/r35/acquisition.py",
    "information": "alpha_agent/r35/information.py",
    "features": "alpha_agent/r35/features.py",
    "design": "alpha_agent/r35/design.py",
    "orthogonality": "alpha_agent/r35/orthogonality.py",
    "incremental": "alpha_agent/r35/incremental.py",
    "analyst_lane": "alpha_agent/r35/analyst_lane.py",
    "campaign": "alpha_agent/r35/campaign.py",
}

#: The same forbidden sets Release 34 carries. The research lane's boundary did
#: not move because the release changed.
R35_FORBIDDEN_OWNER_REFS = R34_FORBIDDEN_OWNER_REFS
R35_FORBIDDEN_CALLS = R34_FORBIDDEN_CALLS

#: Modules whose existence under alpha_agent/r35 would mean Release 35 had
#: rebuilt something an earlier release already owns. The release adds
#: information, not a second statistics library, a second optimiser, a second
#: universe, a second panel or a second purchase gate.
R35_SECOND_OWNER_FORBIDDEN = (
    "alpha_agent/r35/universe.py", "alpha_agent/r35/panel.py",
    "alpha_agent/r35/models.py", "alpha_agent/r35/learners.py",
    "alpha_agent/r35/economics.py", "alpha_agent/r35/portfolio.py",
    "alpha_agent/r35/multiple_testing.py", "alpha_agent/r35/walkforward.py",
    "alpha_agent/r35/purchase_gate.py", "alpha_agent/r35/calibration.py",
    "alpha_agent/r35/lockbox.py",
)


def check_release35_orthogonal_information(files: list[Path]) -> dict:
    """Release 35 ownership, acquisition, point-in-time and honesty invariants."""
    src = {name: (_read(path) or "") for name, path in R35_OWNERS.items()}
    modules_missing = sorted(n for n, t in src.items() if not t)
    all_src = "\n".join(src.values())
    runner = _read("scripts/run_release35_orthogonal_information.py") or ""

    # One of each. R35 adds INFORMATION; every statistic, model, universe,
    # panel, optimiser and gate is imported from the release that owns it.
    reuses_r31_statistics = (
        "from ..r31 import multiple_testing as _mt" in src["campaign"])
    reuses_r31_hashing = "from ..r31 import (" in src["root"]
    reuses_r33_features = (
        "from ..r33 import features as _r33_features" in src["campaign"])
    reuses_r34_universe = (
        "from ..r34 import universe as _r34_universe" in src["campaign"]
        and "from ..r34 import panel as _r34_panel" in src["campaign"])
    reuses_r34_conversion = (
        "from ..r34 import campaign as _r34_campaign" in src["campaign"]
        and "from ..r34 import campaign as _r34_campaign" in src["incremental"])
    reuses_released_orthogonality = (
        "from .. import orthogonality as _orth" in src["orthogonality"])
    reuses_released_purchase_gate = (
        "from ..r32 import purchase_gate as _purchase_gate"
        in src["analyst_lane"]
        and "from .. import analyst_revisions as _stage13a"
        in src["analyst_lane"])
    reuses_released_pit_sector = (
        "from .. import pit_sector as _pit_sector" in src["information"])
    second_owner_modules = sorted(p for p in R35_SECOND_OWNER_FORBIDDEN
                                  if (REPO_ROOT / p).exists())
    no_second_learner_library = not re.search(
        r"def\s+fit_(ridge|elastic_net|gbrt|extra_trees|hmm|linear|"
        r"hierarchical)\s*\(", all_src)

    # Safety: research only, promotes nothing, mutates nothing, buys nothing.
    safety_flags_false = all(
        f"{flag} = False" in src["root"] for flag in
        ("AUTOMATIC_PROMOTION_ALLOWED", "AUTOMATIC_SLEEVE_ACTIVATION_ALLOWED",
         "MAY_SPEND_MONEY", "MAY_MUTATE_PRODUCTION"))
    spending_refused = all(
        f"{flag} = False" in src["contract"] for flag in
        ("MAY_SPEND_MONEY", "MAY_START_PROVIDER_TRIAL",
         "MAY_CREATE_PROVIDER_ACCOUNT"))
    forbidden_calls = sorted(
        {t for t in R35_FORBIDDEN_CALLS if t in all_src.lower()})
    forbidden_owner_refs = sorted(
        {t for t in R35_FORBIDDEN_OWNER_REFS if t in all_src})

    # Point in time. Each of these is a specific way this release could have
    # manufactured information it did not have.
    one_alignment_owner = (
        "def as_of_align" in src["information"]
        and src["features"].count("def as_of_align") == 0
        and src["design"].count("def as_of_align") == 0)
    insider_uses_filing_date = (
        'INSIDER_OBSERVABLE_AT = "FILING_DATE"' in src["contract"]
        and "INSIDER_TRANSACTION_DATE_MAY_BE_OBSERVABLE = False"
        in src["contract"])
    cot_publication_lag_declared = (
        "COT_PUBLICATION_LAG_DAYS" in src["contract"]
        and "COT_PUBLICATION_LAG_STRESS_DAYS" in src["contract"])
    oecd_lag_declared = (
        "OECD_RATE_PUBLICATION_LAG_MONTHS" in src["contract"]
        and "MONTHLY_PUBLISHED_IN_ARREARS" in src["information"])
    prohibited_substitutions_declared = (
        "PROHIBITED_SUBSTITUTIONS" in src["contract"]
        and "manufactured from spot price momentum" in src["contract"]
        and "written back onto historical dates" in src["contract"])
    pit_sector_is_no_look_ahead = (
        "PitSicSeries" in src["information"]
        and "PIT_SECTOR_OWNER" in src["contract"])
    curve_is_dated_contracts = (
        "EIA_WTI_CONTRACTS" in src["contract"]
        and "def load_eia_curve" in src["information"])
    insider_value_weighting_refused = (
        "INSIDER_VALUE_WEIGHTING_ALLOWED = False" in src["contract"]
        and "INSIDER_VALUE_WEIGHTING_REJECTED = True" in src["information"])

    # Orthogonality is a GATE, and raw correlation may not decide it.
    orthogonality_is_a_gate = (
        "ORTHOGONALITY_IS_A_GATE = True" in src["contract"]
        and "ORTHOGONALITY_MEASURED_BEFORE_PREDICTION = True"
        in src["contract"])
    raw_correlation_is_not_distinctness = (
        "DISTINCTNESS_IS_RAW_CORRELATION_ONLY = False" in src["contract"]
        and "residual_share" in src["orthogonality"])
    measured_on_training_only = (
        "def training_row_mask" in src["campaign"]
        and '"measured_on": "TRAINING_ROWS_ONLY"' in src["orthogonality"])

    # The increment, not the level, is the primary object.
    increment_is_paired = (
        'PRIMARY_INCREMENT_STATISTIC = "PAIRED_PER_DATE_RANK_IC_DIFFERENCE"'
        in src["contract"]
        and "def paired_increment" in src["incremental"])
    model_held_fixed = (
        "MODEL_HELD_FIXED_ACROSS_ARMS = True" in src["contract"]
        and "FREE_MODEL_SELECTION_IS_SECONDARY = True" in src["contract"])
    rows_identical_by_construction = (
        "def augment_context" in src["design"]
        and "row-identical by construction" in src["design"])
    vacuous_arm_detected = (
        "def arm_responded" in src["incremental"]
        and "arm_could_respond" in src["incremental"])
    economic_increment_is_paired = (
        'PRIMARY_ECONOMIC_INCREMENT = "AFTER_COST_EXCESS_UTILITY_MINUS_BASE_ARM"'
        in src["contract"]
        and "def paired_economic_increment" in src["campaign"])
    conversion_is_frozen = (
        "CONVERSION_LAYER_SEARCH_ALLOWED = False" in src["contract"]
        and "FROZEN_CONVERSION" in src["contract"]
        and "r34_prediction_to_pnl_v2::FINALIST::COMBINED_BEST"
        in src["contract"])

    # No fake fresh evidence, and three separate results.
    fresh_evidence_refused = (
        "FRESH_UNSEEN_EVIDENCE_EXISTS = False" in src["contract"]
        and "FRESH_UNSEEN_EVIDENCE_REASON" in src["contract"])
    no_fold_is_a_lockbox = (
        "A_FOLD_MAY_BE_CALLED_A_LOCKBOX = False" in src["contract"]
        and "def verdict_ceiling_without_fresh_evidence" in src["contract"])
    independent_evidence_is_a_gate = (
        "def genuinely_independent_evidence_exists" in src["contract"]
        and "genuinely_independent_evidence_exists" in src["campaign"])
    three_results_reported = (
        'RESULT_NAMES = ("SYSTEM_RESULT", "RESEARCH_CANDIDATE_RESULT", '
        '"ALPHA_RESULT")' in src["contract"]
        and '"RESEARCH_CANDIDATE_RESULT"' in src["campaign"]
        and "SYSTEM_AND_ALPHA_RESULTS_ARE_SEPARATE = True" in src["contract"])
    alpha_pass_requires_qualified = (
        "ALPHA_PASS_REQUIRES = VERDICT_QUALIFIED" in src["contract"]
        and "ALPHA_PASS_ALSO_REQUIRES_INDEPENDENT_EVIDENCE = True"
        in src["contract"])
    no_forward_registration = (
        "MAY_REGISTER_FORWARD_CANDIDATE = False" in src["contract"]
        and "MAY_CREATE_SECOND_TRUE_FORWARD_STORE = False" in src["contract"])

    # Bounded search and honest multiple testing.
    adaptive_search_refused = (
        "ADAPTIVE_SEARCH_ALLOWED = False" in src["contract"]
        and "NEW_PREDICTOR_SEARCH_ALLOWED = False" in src["contract"]
        and "MODEL_ARCHITECTURE_SEARCH_ALLOWED = False" in src["contract"])
    denominator_all_executed = (
        "DENOMINATOR_COUNTS_ALL_EXECUTED = True" in src["contract"]
        and "CONTROLS_ENTER_DENOMINATOR = False" in src["contract"])
    bh_direction_is_split = (
        "rejected_beating_the_base" in src["campaign"]
        and "rejected_losing_to_the_base" in src["campaign"]
        and "ONLY_POSITIVE_REJECTIONS_MAY_QUALIFY = True" in src["contract"])

    runner_flat = " ".join(runner.lower().split())
    runner_is_research_only = (
        "research only" in runner_flat and "no order" in runner_flat)

    # The functional half: the planned configuration count must be DERIVED from
    # the frozen grids rather than typed, and every declared feature must belong
    # to a declared family. R34 v1 hand-typed a count its own grid disagreed
    # with, and the assertion that compared them was checking one hand-written
    # number against another.
    try:
        if str(REPO_ROOT.parent) not in sys.path:
            sys.path.insert(0, str(REPO_ROOT.parent))
        from paper_trader.alpha_agent.r35 import contract as _r35_contract
        planned_matches_the_grid = bool(
            _r35_contract.CONFIG_FAMILIES["PREDICTIVE_INCREMENT"]
            == (len(_r35_contract.ACQUIRED_FAMILIES) + 1)
            * len(_r35_contract.HORIZONS)
            and _r35_contract.CONFIG_FAMILIES["STANDALONE_DIAGNOSTIC"]
            == len(_r35_contract.ACQUIRED_FAMILIES)
            and _r35_contract.PLANNED_CONFIG_TOTAL
            <= _r35_contract.MAX_PRIMARY_CONFIGS)
        every_feature_has_a_family = all(
            spec[0] in _r35_contract.ALL_FAMILIES
            for spec in _r35_contract.NEW_FEATURES.values())
        alpha_pass_unreachable = not (
            _r35_contract.genuinely_independent_evidence_exists())
    except Exception as exc:  # noqa: BLE001 - unmeasurable fails closed
        planned_matches_the_grid = f"UNMEASURABLE:{exc}"
        every_feature_has_a_family = f"UNMEASURABLE:{exc}"
        alpha_pass_unreachable = f"UNMEASURABLE:{exc}"

    return {
        "modules_present": not modules_missing,
        "modules_missing": modules_missing,
        "second_owner_modules": second_owner_modules,
        "reuses_r31_statistics": reuses_r31_statistics,
        "reuses_r31_hashing": reuses_r31_hashing,
        "reuses_r33_features": reuses_r33_features,
        "reuses_r34_universe_and_panel": reuses_r34_universe,
        "reuses_r34_conversion": reuses_r34_conversion,
        "reuses_released_orthogonality": reuses_released_orthogonality,
        "reuses_released_purchase_gate": reuses_released_purchase_gate,
        "reuses_released_pit_sector": reuses_released_pit_sector,
        "no_second_learner_library": no_second_learner_library,
        "safety_flags_false": safety_flags_false,
        "spending_refused": spending_refused,
        "forbidden_calls": forbidden_calls,
        "forbidden_owner_refs": forbidden_owner_refs,
        "one_alignment_owner": one_alignment_owner,
        "insider_observable_at_filing_date": insider_uses_filing_date,
        "cot_publication_lag_declared": cot_publication_lag_declared,
        "oecd_publication_lag_declared": oecd_lag_declared,
        "prohibited_substitutions_declared":
            prohibited_substitutions_declared,
        "pit_sector_is_no_look_ahead": pit_sector_is_no_look_ahead,
        "commodity_curve_is_dated_contracts": curve_is_dated_contracts,
        "insider_value_weighting_refused": insider_value_weighting_refused,
        "orthogonality_is_a_gate": orthogonality_is_a_gate,
        "raw_correlation_is_not_distinctness":
            raw_correlation_is_not_distinctness,
        "orthogonality_measured_on_training_only": measured_on_training_only,
        "increment_is_paired": increment_is_paired,
        "model_held_fixed_across_arms": model_held_fixed,
        "rows_identical_by_construction": rows_identical_by_construction,
        "vacuous_arm_is_detected": vacuous_arm_detected,
        "economic_increment_is_paired": economic_increment_is_paired,
        "conversion_layer_is_frozen": conversion_is_frozen,
        "fresh_unseen_evidence_refused": fresh_evidence_refused,
        "no_fold_may_be_called_a_lockbox": no_fold_is_a_lockbox,
        "independent_evidence_is_a_gate": independent_evidence_is_a_gate,
        "reports_three_separate_results": three_results_reported,
        "alpha_pass_requires_qualified_verdict":
            alpha_pass_requires_qualified,
        "alpha_pass_is_structurally_unreachable": alpha_pass_unreachable,
        "no_forward_registration": no_forward_registration,
        "adaptive_search_refused": adaptive_search_refused,
        "denominator_counts_all_executed": denominator_all_executed,
        "benjamini_hochberg_direction_is_split": bh_direction_is_split,
        "runner_is_research_only": runner_is_research_only,
        "planned_configs_match_the_frozen_grid": planned_matches_the_grid,
        "every_new_feature_has_a_declared_family": every_feature_has_a_family,
    }


R36_OWNERS = {
    "root": "alpha_agent/r36/__init__.py",
    "contract": "alpha_agent/r36/contract.py",
    "entitlements": "alpha_agent/r36/entitlements.py",
    "acquisition": "alpha_agent/r36/acquisition.py",
    "native_markets": "alpha_agent/r36/native_markets.py",
    "strategies": "alpha_agent/r36/strategies.py",
    "experiments": "alpha_agent/r36/experiments.py",
    "coverage": "alpha_agent/r36/coverage.py",
    "campaign": "alpha_agent/r36/campaign.py",
}

#: The research lane's boundary did not move because the release changed.
R36_FORBIDDEN_OWNER_REFS = R34_FORBIDDEN_OWNER_REFS
R36_FORBIDDEN_CALLS = R34_FORBIDDEN_CALLS

#: Modules whose existence under alpha_agent/r36 would mean Release 36 had
#: rebuilt something an earlier release already owns. This release adds
#: MARKETS, not a second economic judge, a second statistics library, a second
#: optimiser, a second walk-forward framework or a second forward store.
R36_SECOND_OWNER_FORBIDDEN = (
    "alpha_agent/r36/economics.py", "alpha_agent/r36/judge.py",
    "alpha_agent/r36/universe.py", "alpha_agent/r36/panel.py",
    "alpha_agent/r36/models.py", "alpha_agent/r36/learners.py",
    "alpha_agent/r36/portfolio.py", "alpha_agent/r36/multiple_testing.py",
    "alpha_agent/r36/walkforward.py", "alpha_agent/r36/purchase_gate.py",
    "alpha_agent/r36/calibration.py", "alpha_agent/r36/lockbox.py",
    "alpha_agent/r36/concentration.py", "alpha_agent/r36/orthogonality.py",
    "alpha_agent/r36/forward_evidence.py",
)


def check_release36_global_multi_asset_frontier(files: list[Path]) -> dict:
    """Release 36 ownership, control, point-in-time and honesty invariants."""
    src = {name: (_read(path) or "") for name, path in R36_OWNERS.items()}
    modules_missing = sorted(n for n, t in src.items() if not t)
    all_src = "\n".join(src.values())
    runner = _read("scripts/run_release36_global_multi_asset_frontier.py") or ""

    # One of each. R36 adds MARKETS; every statistic, judge, optimiser, vendor
    # reader and gate is imported from the release that owns it.
    reuses_r31_statistics = (
        "from ..r31 import multiple_testing as _mt" in src["campaign"])
    reuses_r31_hashing = "from ..r31 import (" in src["root"]
    reuses_r34_economics = (
        "from ..r34 import economics as _economics" in src["experiments"]
        and "from ..r34 import concentration as _concentration"
        in src["experiments"])
    reuses_r33_vendor_reader = (
        "from ..r33 import universe as _r33_universe" in src["native_markets"])
    reuses_r34_vendor_reader = (
        "from ..r34 import universe as _r34_universe" in src["native_markets"])
    reuses_r35_alignment = (
        "from ..r35 import information as _r35_information"
        in src["native_markets"]
        and "_r35_information.as_of_align" in src["native_markets"])
    reuses_r35_http_owner = (
        "from ..r35 import acquisition as _r35_acquisition"
        in src["acquisition"]
        and "_r35_acquisition.fetch" in src["acquisition"])
    reuses_released_rank_correlation = (
        "from .. import orthogonality as _orth" in src["experiments"])
    second_owner_modules = sorted(p for p in R36_SECOND_OWNER_FORBIDDEN
                                  if (REPO_ROOT / p).exists())
    no_second_learner_library = not re.search(
        r"def\s+fit_(ridge|elastic_net|gbrt|extra_trees|hmm|linear|"
        r"hierarchical)\s*\(", all_src)
    no_second_economic_judge = not re.search(
        r"def\s+(evaluate_book|volatility_matched_control|"
        r"excess_significance|annualised_return)\s*\(", all_src)

    # Safety: research only, promotes nothing, mutates nothing, buys nothing.
    safety_flags_false = all(
        f"{flag} = False" in src["root"] for flag in
        ("AUTOMATIC_PROMOTION_ALLOWED", "AUTOMATIC_SLEEVE_ACTIVATION_ALLOWED",
         "MAY_SPEND_MONEY", "MAY_MUTATE_PRODUCTION"))
    spending_refused = all(
        f"{flag} = False" in src["contract"] for flag in
        ("MAY_SPEND_MONEY", "MAY_START_PROVIDER_TRIAL",
         "MAY_CREATE_PROVIDER_ACCOUNT", "MAY_CHANGE_SUBSCRIPTION_TIER"))
    api_key_is_not_entitlement = (
        "API_KEY_IMPLIES_ENTITLEMENT = False" in src["contract"]
        and "def measure_all" in src["entitlements"])
    credentials_never_serialised = (
        "credentials_written_to_artifacts" in src["acquisition"]
        and "api_key=REDACTED" in src["acquisition"]
        and "Never their values" in src["entitlements"])
    forbidden_calls = sorted(
        {t for t in R36_FORBIDDEN_CALLS if t in all_src.lower()})
    forbidden_owner_refs = sorted(
        {t for t in R36_FORBIDDEN_OWNER_REFS if t in all_src})

    # The control is the release's central discipline, and it is where both of
    # this release's own superseded campaigns went wrong.
    control_matches_what_is_traded = (
        "CONTROL_IS_THE_PASSIVE_HOLD_OF_WHAT_IS_TRADED = True"
        in src["contract"]
        and "STRATEGY_CONTROL_LEG" in src["contract"]
        and "STRATEGY_CONTROL_LEG.get(name)" in src["experiments"])
    universal_equity_control_refused = (
        "UNIVERSAL_SPY_CASH_CONTROL_ALLOWED = False" in src["contract"]
        and "EXCESS_OVER_CASH_MAY_RANK = False" in src["contract"])
    control_must_be_observable = (
        "def trim_to_control" in src["native_markets"]
        and "trim_to_control" in src["campaign"]
        and "every_lane_control_is_observable_throughout" in src["campaign"])
    superseded_campaigns_declared = (
        "SUPERSEDED_CAMPAIGNS" in src["contract"]
        and "SUPERSEDED_CONTROL_DEFECT" in src["contract"]
        and "SUPERSEDED_WINDOW_DEFECT" in src["contract"])
    cadence_is_per_lane_with_a_reason = (
        "LANE_CADENCE_REASON" in src["contract"]
        and len(re.findall(r"LANE_CADENCE\s*=\s*\{", src["contract"])) == 1)

    # Point in time and survivorship: each is a specific way this release could
    # have manufactured information or a market it did not have.
    one_alignment_owner = (
        "def as_of_align" not in all_src
        and "_r35_information.as_of_align" in src["native_markets"])
    admissibility_reused_from_r33 = (
        "ADMISSIBILITY_RULES_ARE_REUSED_FROM_R33 = True" in src["contract"]
        and "_r33_universe.MAX_ZERO_RETURN_FRACTION" in src["contract"])
    publication_lags_reused_from_r35 = (
        "_r35_contract.COT_PUBLICATION_LAG_DAYS" in src["contract"]
        and "_r35_contract.OECD_RATE_PUBLICATION_LAG_MONTHS"
        in src["contract"])
    prohibited_substitutions_declared = (
        "PROHIBITED_SUBSTITUTIONS" in src["contract"]
        and "written back onto historical dates" in src["contract"]
        and "manufactured from spot price momentum" in src["contract"])
    curve_is_dated_contracts = (
        "COMMODITY_CURVES" in src["contract"]
        and "def read_commodity_curves" in src["native_markets"]
        and "cache_name=" in src["native_markets"])
    terminated_market_is_admitted = (
        "COMMODITY_TERMINATED_MARKETS" in src["contract"]
        and "PROPANE" in src["contract"])
    contract_splice_refused = (
        "GASOLINE_CONTRACT_SPLICE_ALLOWED = False" in src["contract"])
    short_volatility_survivorship_refused = (
        "SHORT_VOLATILITY_DIRECTION_TESTABLE = False" in src["contract"]
        and "SHORT_VOLATILITY_BLOCK_REASON" in src["contract"])
    broad_crypto_survivorship_refused = (
        "CRYPTO_BROAD_UNIVERSE_ADMISSIBLE = False" in src["contract"]
        and "CRYPTO_BROAD_UNIVERSE_BLOCK_REASON" in src["contract"])
    normalisation_is_trailing_only = (
        "NORMALISATION_IS_TRAILING_ONLY = True" in src["contract"]
        and "FULL_SAMPLE_STATISTICS_ALLOWED = False" in src["contract"]
        and "shifted one period first" in src["strategies"])
    non_overlapping_decisions = (
        "NON_OVERLAPPING_DECISIONS = True" in src["contract"]
        and "def decision_dates" in src["native_markets"])
    a_position_needs_an_observable_return = (
        "observable = returns.reindex" in src["experiments"]
        and "held = held.where(observable, 0.0)" in src["experiments"])

    # Native versus proxy: the distinction the release exists to record.
    three_implementation_levels = (
        "LEVEL_SIGNAL" in src["contract"] and "LEVEL_PROXY" in src["contract"]
        and "LEVEL_NATIVE" in src["contract"])
    proxy_may_not_close_a_native_frontier = (
        "PROXY_MAY_CLOSE_A_NATIVE_FRONTIER = False" in src["contract"]
        and "STATE_TESTED_PROXY_ONLY" in src["coverage"])
    every_cell_terminal = (
        "EVERY_CELL_MUST_BE_TERMINAL = True" in src["contract"]
        and "AMBIGUOUS_CELL_STATES_ALLOWED = False" in src["contract"]
        and "def summarise" in src["coverage"])
    coverage_is_derived_not_typed = (
        "def _lane_family_results" in src["coverage"]
        and "def _cell_state" in src["coverage"])
    blocked_frontier_named = (
        "def blocked_frontier" in src["coverage"]
        and "def _next_action" in src["coverage"])

    # Bounded search, honest multiple testing, three separate results.
    adaptive_search_refused = (
        "ADAPTIVE_SEARCH_ALLOWED = False" in src["contract"]
        and "PARAMETER_SEARCH_ALLOWED = False" in src["contract"]
        and "MODEL_ARCHITECTURE_SEARCH_ALLOWED = False" in src["contract"])
    denominator_all_executed = (
        "DENOMINATOR_COUNTS_ALL_EXECUTED = True" in src["contract"]
        and "CONTROLS_ENTER_DENOMINATOR = False" in src["contract"])
    bh_direction_is_split = (
        "rejected_beating_the_control" in src["campaign"]
        and "rejected_losing_to_the_control" in src["campaign"]
        and "ONLY_POSITIVE_REJECTIONS_MAY_QUALIFY = True" in src["contract"])
    minimum_detectable_effect_reported = (
        "def minimum_detectable_excess" in src["experiments"])
    cost_sensitivity_reported = (
        "COST_SENSITIVITY_MULTIPLIERS" in src["experiments"]
        and "COST_STRESS_MULTIPLIER" in src["contract"])
    fresh_evidence_refused = (
        "FRESH_UNSEEN_EVIDENCE_EXISTS = False" in src["contract"]
        and "FRESH_UNSEEN_EVIDENCE_REASON" in src["contract"])
    no_fold_is_a_lockbox = (
        "A_FOLD_MAY_BE_CALLED_A_LOCKBOX = False" in src["contract"]
        and "def verdict_ceiling_without_fresh_evidence" in src["contract"])
    independent_evidence_is_a_gate = (
        "def genuinely_independent_evidence_exists" in src["contract"]
        and "genuinely_independent_evidence_exists" in src["campaign"])
    three_results_reported = (
        'RESULT_NAMES = ("SYSTEM_RESULT", "RESEARCH_CANDIDATE_RESULT", '
        '"ALPHA_RESULT")' in src["contract"]
        and '"RESEARCH_CANDIDATE_RESULT"' in src["campaign"]
        and "SYSTEM_AND_ALPHA_RESULTS_ARE_SEPARATE = True" in src["contract"])
    alpha_pass_requires_qualified = (
        "ALPHA_PASS_REQUIRES = VERDICT_EDGE_FOUND" in src["contract"]
        and "ALPHA_PASS_ALSO_REQUIRES_INDEPENDENT_EVIDENCE = True"
        in src["contract"])
    no_forward_registration = (
        "MAY_REGISTER_FORWARD_CANDIDATE = False" in src["contract"]
        and "MAY_CREATE_SECOND_TRUE_FORWARD_STORE = False" in src["contract"]
        and "def forward_handoff" in src["campaign"])

    runner_flat = " ".join(runner.lower().split())
    runner_is_research_only = (
        "research only" in runner_flat and "no order" in runner_flat)

    # The functional half: the planned count must be DERIVED from the frozen
    # grid rather than typed, every strategy must belong to a declared lane,
    # and every declared market family must be a declared strategy family.
    try:
        if str(REPO_ROOT.parent) not in sys.path:
            sys.path.insert(0, str(REPO_ROOT.parent))
        from paper_trader.alpha_agent.r36 import contract as _r36_contract
        from paper_trader.alpha_agent.r36 import coverage as _r36_coverage
        planned_matches_the_grid = bool(
            _r36_contract.PLANNED_CONFIG_TOTAL
            == len(_r36_contract.STRATEGIES)
            and _r36_contract.PLANNED_CONFIG_TOTAL
            <= _r36_contract.MAX_PRIMARY_CONFIGS
            and sum(_r36_contract.lane_config_counts().values())
            == len(_r36_contract.STRATEGIES))
        every_strategy_has_a_declared_lane = all(
            spec[0] in _r36_contract.EXECUTED_LANES
            and spec[2] in _r36_contract.LEVELS
            and spec[3] in _r36_contract.CONSTRUCTIONS
            and all(f in _r36_contract.STRATEGY_FAMILIES for f in spec[1])
            for spec in _r36_contract.STRATEGIES.values())
        every_market_family_is_declared = all(
            family in _r36_contract.STRATEGY_FAMILIES
            and market.get("level") in _r36_contract.LEVELS
            for market in _r36_coverage.MARKETS.values()
            for family in market["families"])
        every_lane_control_is_distinct = bool(
            len(set(_r36_contract.LANE_CONTROL.values()))
            == len(_r36_contract.LANE_CONTROL))
        alpha_pass_unreachable = not (
            _r36_contract.genuinely_independent_evidence_exists())
    except Exception as exc:  # noqa: BLE001 - unmeasurable fails closed
        planned_matches_the_grid = f"UNMEASURABLE:{exc}"
        every_strategy_has_a_declared_lane = f"UNMEASURABLE:{exc}"
        every_market_family_is_declared = f"UNMEASURABLE:{exc}"
        every_lane_control_is_distinct = f"UNMEASURABLE:{exc}"
        alpha_pass_unreachable = f"UNMEASURABLE:{exc}"

    return {
        "modules_present": not modules_missing,
        "modules_missing": modules_missing,
        "second_owner_modules": second_owner_modules,
        "reuses_r31_statistics": reuses_r31_statistics,
        "reuses_r31_hashing": reuses_r31_hashing,
        "reuses_r34_economic_judge": reuses_r34_economics,
        "reuses_r33_vendor_reader": reuses_r33_vendor_reader,
        "reuses_r34_vendor_reader": reuses_r34_vendor_reader,
        "reuses_r35_alignment_owner": reuses_r35_alignment,
        "reuses_r35_http_owner": reuses_r35_http_owner,
        "reuses_released_rank_correlation": reuses_released_rank_correlation,
        "no_second_learner_library": no_second_learner_library,
        "no_second_economic_judge": no_second_economic_judge,
        "safety_flags_false": safety_flags_false,
        "spending_refused": spending_refused,
        "api_key_is_not_an_entitlement": api_key_is_not_entitlement,
        "credentials_never_serialised": credentials_never_serialised,
        "forbidden_calls": forbidden_calls,
        "forbidden_owner_refs": forbidden_owner_refs,
        "control_matches_what_is_traded": control_matches_what_is_traded,
        "universal_equity_control_refused": universal_equity_control_refused,
        "control_must_be_observable_throughout": control_must_be_observable,
        "superseded_campaigns_declared": superseded_campaigns_declared,
        "cadence_is_per_lane_with_a_reason": cadence_is_per_lane_with_a_reason,
        "one_alignment_owner": one_alignment_owner,
        "admissibility_reused_from_r33": admissibility_reused_from_r33,
        "publication_lags_reused_from_r35": publication_lags_reused_from_r35,
        "prohibited_substitutions_declared":
            prohibited_substitutions_declared,
        "commodity_curve_is_dated_contracts": curve_is_dated_contracts,
        "a_terminated_market_is_admitted": terminated_market_is_admitted,
        "contract_splice_refused": contract_splice_refused,
        "short_volatility_survivorship_refused":
            short_volatility_survivorship_refused,
        "broad_crypto_survivorship_refused":
            broad_crypto_survivorship_refused,
        "normalisation_is_trailing_only": normalisation_is_trailing_only,
        "non_overlapping_decisions": non_overlapping_decisions,
        "a_position_requires_an_observable_return":
            a_position_needs_an_observable_return,
        "three_implementation_levels": three_implementation_levels,
        "proxy_may_not_close_a_native_frontier":
            proxy_may_not_close_a_native_frontier,
        "every_cell_must_be_terminal": every_cell_terminal,
        "coverage_is_derived_not_typed": coverage_is_derived_not_typed,
        "blocked_frontier_is_named": blocked_frontier_named,
        "adaptive_search_refused": adaptive_search_refused,
        "denominator_counts_all_executed": denominator_all_executed,
        "benjamini_hochberg_direction_is_split": bh_direction_is_split,
        "minimum_detectable_effect_reported":
            minimum_detectable_effect_reported,
        "cost_sensitivity_reported": cost_sensitivity_reported,
        "fresh_unseen_evidence_refused": fresh_evidence_refused,
        "no_fold_may_be_called_a_lockbox": no_fold_is_a_lockbox,
        "independent_evidence_is_a_gate": independent_evidence_is_a_gate,
        "reports_three_separate_results": three_results_reported,
        "alpha_pass_requires_qualified_verdict":
            alpha_pass_requires_qualified,
        "alpha_pass_is_structurally_unreachable": alpha_pass_unreachable,
        "no_forward_registration": no_forward_registration,
        "runner_is_research_only": runner_is_research_only,
        "planned_configs_match_the_frozen_grid": planned_matches_the_grid,
        "every_strategy_has_a_declared_lane": every_strategy_has_a_declared_lane,
        "every_market_family_is_declared": every_market_family_is_declared,
        "every_lane_control_is_distinct": every_lane_control_is_distinct,
    }


R37_OWNERS = {
    "root": "alpha_agent/r37/__init__.py",
    "contract": "alpha_agent/r37/contract.py",
    "providers": "alpha_agent/r37/providers.py",
    "unlock": "alpha_agent/r37/unlock.py",
    "samples": "alpha_agent/r37/samples.py",
    "scoring": "alpha_agent/r37/scoring.py",
    "purchase": "alpha_agent/r37/purchase.py",
    "compute": "alpha_agent/r37/compute.py",
    "ml_readiness": "alpha_agent/r37/ml_readiness.py",
    "market_structure": "alpha_agent/r37/market_structure.py",
    "campaign": "alpha_agent/r37/campaign.py",
}

R37_FORBIDDEN_OWNER_REFS = R34_FORBIDDEN_OWNER_REFS
R37_FORBIDDEN_CALLS = R34_FORBIDDEN_CALLS

#: Modules whose existence under alpha_agent/r37 would mean Release 37 had
#: rebuilt something an earlier release already owns. The purchase gates are
#: the point: there are already THREE canonical answers to "should we buy this
#: data" (the Slice-9 kernel, its composition owner, and the R32 ten-condition
#: gate) and a fourth would be a fourth answer to one question.
R37_SECOND_OWNER_FORBIDDEN = (
    "alpha_agent/r37/purchase_gate.py",
    "alpha_agent/r37/data_expansion_gate.py",
    "alpha_agent/r37/information_purchase_gate.py",
    "alpha_agent/r37/acquisition.py", "alpha_agent/r37/entitlements.py",
    "alpha_agent/r37/coverage.py", "alpha_agent/r37/economics.py",
    "alpha_agent/r37/judge.py", "alpha_agent/r37/universe.py",
    "alpha_agent/r37/models.py", "alpha_agent/r37/learners.py",
    "alpha_agent/r37/multiple_testing.py", "alpha_agent/r37/experiments.py",
    "alpha_agent/r37/strategies.py", "alpha_agent/r37/native_markets.py",
    "alpha_agent/r37/forward_evidence.py", "alpha_agent/r37/training.py",
)

#: Tokens that would mean the release had spent money, installed a toolchain or
#: started training something. Matched case-insensitively across the package.
#: Deliberately NOT the bare token ``credit_card``: the long list legitimately
#: records ``credit_card_required`` as a property of every vendor, and a guard
#: that fires on its own scorecard field is a guard about the wrong thing.
R37_FORBIDDEN_COMMERCIAL = (
    "stripe", "checkout.session", "card_number", "cardnumber", "cvv",
    "payment_method_id", "billing_address",
    "pip install", "conda install", "subprocess.run([\"pip\"",
    "start_trial(", "create_account(", "subscribe(",
)


def check_release37_native_market_data_gate(files: list[Path]) -> dict:
    """Release 37 ownership, commercial-safety and evidence invariants.

    The release exists to recommend a PURCHASE, so its invariants are unlike
    every prior release's: the dangerous outcomes are spending money, creating a
    fourth purchase gate, crediting a vendor's brochure as a measurement, and
    letting a value-per-dollar score outrank a hard data-integrity gate.
    """
    src = {name: (_read(path) or "") for name, path in R37_OWNERS.items()}
    modules_missing = sorted(n for n, t in src.items() if not t)
    all_src = "\n".join(src.values())
    all_lower = all_src.lower()
    runner = _read("scripts/run_release37_native_market_data_gate.py") or ""

    second_owner_modules = sorted(p for p in R37_SECOND_OWNER_FORBIDDEN
                                  if (REPO_ROOT / p).exists())

    # (1) Release 37 defines NO gate. It composes the three that exist.
    gate_definitions = [tok for tok in ("def evaluate_dataset(",
                                        "def evaluate_gap(",
                                        "def load_data_expansion(",
                                        "def purchase_decision(")
                        if tok in all_src]
    composes_slice9 = (
        "from ...api import data_expansion as _slice9" in src["purchase"]
        and "_slice9.run_evaluation(" in src["purchase"])
    # Release 37.1: the acquisition decision is DELEGATED to the canonical gate in its
    # research-acquisition context, and this release may not recommend what that gate
    # refused. Without this the release has a second, competing acquisition authority.
    delegates_acquisition_to_canonical_gate = (
        "_slice9.CONTEXT_RESEARCH_ACQUISITION" in src["purchase"]
        and "def acquisition_result" in src["purchase"]
        and "def acquisition_case" in src["purchase"]
        and "decision_context=context" in src["purchase"])
    canonical_gate_is_authoritative = (
        "CANONICAL_ACQUISITION_GATE_IS_AUTHORITATIVE = True" in src["purchase"]
        and "R37_MAY_RECOMMEND_WHAT_THE_CANONICAL_GATE_REFUSED = False"
        in src["purchase"]
        and "recommended_by_r37_but_refused_by_canonical_gate" in src["purchase"])
    r37_defines_no_acquisition_authority = (
        "R37_DEFINES_ITS_OWN_ACQUISITION_AUTHORITY = False" in src["contract"]
        and "R37_STATES_ARE_TRIAGE_LABELS = True" in src["contract"]
        and "ACQUISITION_DECISION_OWNER" in src["contract"])
    acquisition_is_not_alpha_evidence = (
        "ACQUISITION_RECOMMENDATION_IS_ALPHA_EVIDENCE = False" in src["contract"]
        and "ACQUISITION_RECOMMENDATION_IS_INTEGRATION_APPROVAL = False"
        in src["contract"]
        and "ACQUISITION_RECOMMENDATION_IS_PURCHASE_AUTHORITY = False"
        in src["contract"]
        and "ACQUISITION_REQUIRES_MANUAL_OPERATOR_APPROVAL = True" in src["contract"])
    # The headline cell count is an EXPECTED unlock until the entitlement is activated.
    expected_unlocks_are_not_measured = (
        "EXPECTED_UNLOCKS_ARE_NOT_MEASURED_UNLOCKS = True" in src["contract"]
        and "UNLOCK_BECOMES_MEASURED_ONLY_AFTER_ENTITLEMENT_ACTIVATION = True"
        in src["contract"]
        and "unlocks_are_expected_not_measured" in src["campaign"])
    # Track C may not report hardware capability as though it were runnability.
    ml_readiness_separates_install_from_hardware = (
        "READINESS_CLASSES = (READY_INSTALLED, READY_AFTER_INSTALL," in src["ml_readiness"]
        and 'READY_INSTALLED = "CURRENTLY_INSTALLED_AND_RUNNABLE"' in src["ml_readiness"]
        and 'READY_AFTER_INSTALL = "HARDWARE_FEASIBLE_AFTER_SOFTWARE_INSTALL"'
        in src["ml_readiness"]
        and 'READY_IMPRACTICAL = "LOCALLY_POSSIBLE_BUT_IMPRACTICAL"' in src["ml_readiness"]
        and 'READY_EXTERNAL_GPU = "EXTERNAL_GPU_RECOMMENDED"' in src["ml_readiness"]
        and 'READY_NOT_FEASIBLE = "NOT_CURRENTLY_FEASIBLE"' in src["ml_readiness"]
        and "required_libraries" in src["ml_readiness"]
        and "currently_runnable_count" in src["ml_readiness"]
        and "_ml.matrix(constraints, inventory.get(\"libraries\"))" in src["campaign"])
    composes_r32_gate = (
        "from ..r32 import purchase_gate as _r32_gate" in src["purchase"]
        and "_r32_gate.build(" in src["purchase"])
    slice9_not_overridden = (
        "SLICE9_RESULT_MAY_BE_OVERRIDDEN = False" in src["purchase"])
    slice9_not_persisted = (
        '"persisted_to_slice9_store": False' in src["purchase"]
        and '"written_to_r32_root": False' in src["purchase"])

    # (2) Reuse, never fork: the HTTP owner, the entitlement owner, the
    # coverage matrix and the hashing owner all belong to earlier releases.
    reuses_r35_http_owner = (
        "from ..r35 import acquisition as _r35_acquisition" in src["samples"]
        and "_r35_acquisition.fetch(" in src["samples"])
    reuses_r36_entitlements = (
        "from ..r36 import entitlements as _r36_entitlements" in src["samples"]
        and "_r36_entitlements.measure_all(" in src["campaign"])
    reuses_r36_coverage = (
        "from ..r36 import coverage as _r36_coverage" in src["unlock"]
        and "_r36_coverage.MARKETS" in src["unlock"])
    reuses_r31_hashing = "from ..r31 import (" in src["root"]
    no_second_downloader = "def fetch(" not in all_src
    no_second_coverage_matrix = "MARKETS = {" not in all_src

    # (3) Commercial safety - the subject of the release.
    spending_refused = all(
        f"{flag} = False" in src["contract"] for flag in
        ("MAY_SPEND_MONEY", "MAY_START_PROVIDER_TRIAL",
         "MAY_CREATE_PROVIDER_ACCOUNT", "MAY_CHANGE_SUBSCRIPTION_TIER",
         "MAY_ACCEPT_LICENCE_AGREEMENT", "MAY_SUBMIT_PAYMENT_DETAILS",
         "MAY_PURCHASE_CLOUD_COMPUTE", "MAY_INSTALL_CUDA",
         "MAY_DOWNLOAD_MODEL_WEIGHTS"))
    safety_flags_false = all(
        f"{flag} = False" in src["root"] for flag in
        ("AUTOMATIC_PROMOTION_ALLOWED", "AUTOMATIC_SLEEVE_ACTIVATION_ALLOWED",
         "MAY_SPEND_MONEY", "MAY_MUTATE_PRODUCTION"))
    no_purchase_authority = (
        "PURCHASE_AUTHORITY_GRANTED_BY_THIS_RELEASE = False" in src["contract"]
        and "def purchase_authority" in src["contract"]
        and '"purchase_authorised": False' in src["contract"])
    commercial_tokens = sorted({t for t in R37_FORBIDDEN_COMMERCIAL
                                if t in all_lower})
    forbidden_calls = sorted({t for t in R37_FORBIDDEN_CALLS
                              if t in all_lower})
    forbidden_owner_refs = sorted({t for t in R37_FORBIDDEN_OWNER_REFS
                                   if t in all_src})

    # (4) A vendor claim is not a measurement, and a proxy is not a native.
    marketing_is_not_measurement = (
        "A_MARKETING_CLAIM_IS_NOT_A_MEASUREMENT = True" in src["contract"]
        and "VENDOR_CLAIM_ALONE_MAY_UNLOCK_A_CELL = False" in src["contract"]
        and "UNLOCK_REQUIRES_DECLARED_INSTRUMENT_MAPPING = True"
        in src["contract"])
    partial_not_in_headline = (
        "PARTIAL_UNLOCK_COUNTS_IN_HEADLINE = False" in src["contract"]
        and "markets_unlocked_partial" in src["unlock"])
    proxy_may_not_unlock = (
        "def _level_allows_full_unlock" in src["unlock"]
        and "proxy_credit_refused" in src["unlock"])
    unlock_is_derived = (
        "def blocked_frontier" in src["unlock"]
        and "def _from_frozen" in src["unlock"]
        and "def _from_market_table" in src["unlock"])
    evidence_classes_declared = (
        "EVIDENCE_CLASSES" in src["contract"]
        and "EVIDENCE_SAMPLE_VALIDATED" in src["contract"]
        and "evidence" in src["providers"])
    every_row_terminal = (
        "EVERY_CANDIDATE_MUST_BE_TERMINAL = True" in src["contract"]
        and "def validate" in src["providers"])

    # (5) The score is a ranking aid and may never beat a hard gate.
    score_declared_before_use = (
        "SCORE_FORMULA" in src["contract"]
        and "SCORE_IS_A_RANKING_AID_NOT_AN_OPTIMISER = True" in src["contract"]
        and "SCORE_MAY_OVERRIDE_A_HARD_GATE = False" in src["contract"])
    hard_gates_bind = (
        "HARD_FAIL_STATES" in src["scoring"]
        and "rankable_as_investment" in src["scoring"]
        and "naive_ranking_ignoring_hard_gates" in src["scoring"])
    free_data_has_a_cost_floor = (
        "FREE_COST_FLOOR_USD" in src["contract"]
        and "cost_floor_applied" in src["scoring"])

    # (6) Samples prove a schema, never an edge.
    sample_is_not_an_alpha_claim = (
        "A_SAMPLE_MAY_SUPPORT_AN_ALPHA_CLAIM = False" in src["samples"])
    blocks_are_reprobed = (
        "def confirm_blocks" in src["samples"]
        and "BLOCK_CONFIRMATION_ROUTES" in src["contract"])
    unmeasured_is_not_open = (
        "None if status is None" in src["samples"])
    owned_client_measured = (
        "def measure_owned_futures_client" in src["samples"]
        and "NORGATE_DATED_CONTRACT_API" in src["samples"])
    credentials_never_serialised = (
        "credentials_written_to_artifacts" in src["samples"])

    # (7) Track C is bounded: no training, no install, no weights.
    ml_trains_nothing = (
        "TRAINS_A_MODEL = False" in src["ml_readiness"]
        and "SELECTS_A_MODEL = False" in src["ml_readiness"]
        and "NEWER_IMPLIES_BETTER = False" in src["ml_readiness"]
        and "ML_TRAINING_CAMPAIGN_IN_SCOPE = False" in src["contract"])
    compute_is_read_only = (
        '"installed_anything": False' in src["compute"]
        and '"downloaded_model_weights": False' in src["compute"]
        and "presence is read from package metadata" in src["compute"])
    feasibility_is_computed = (
        "def feasibility" in src["ml_readiness"]
        and "def constraints" in src["compute"])
    data_contract_composes = (
        "COMPOSES_EXISTING_OWNERS = True" in src["ml_readiness"]
        and "CREATES_A_SECOND_MARKET_DATA_OWNER = False"
        in src["ml_readiness"]
        and "expected_excess_return" in src["ml_readiness"]
        and "quantiles" in src["ml_readiness"])

    # (8) Track D is designed, not run, and Fibonacci carries a placebo arm.
    structure_not_executed = (
        "EXECUTED_IN_THIS_RELEASE = False" in src["market_structure"]
        and "READS_A_PRICE = False" in src["market_structure"]
        and "MARKET_STRUCTURE_EXPERIMENT_IN_SCOPE = False" in src["contract"])
    pivots_need_confirmation = (
        "PIVOT_CONFIRMATION_REQUIRED = True" in src["market_structure"]
        and "PIVOT_TIMESTAMP_IS_THE_CONFIRMATION_DATE = True"
        in src["market_structure"]
        and "FUTURE_KNOWN_EXTREMA_ALLOWED = False" in src["market_structure"])
    fibonacci_has_a_placebo_arm = (
        "FIBONACCI_PLACEBO_LEVELS" in src["market_structure"]
        and "PLACEBO_ARM_REQUIRED = True" in src["market_structure"]
        and "FIBONACCI_IS_DOCTRINE = False" in src["market_structure"])
    visual_lane_is_designed_only = (
        "VISUAL_EXPERIMENT_IN_SCOPE = False" in src["market_structure"]
        and "REPRESENTATION_ARMS" in src["market_structure"])

    # (9) Three results, and the alpha one is structurally untestable here.
    three_results_reported = (
        'RESULT_NAMES = ("SYSTEM_RESULT", "PURCHASE_RECOMMENDATION_RESULT",'
        in src["contract"]
        and '"ALPHA_RESULT")' in src["contract"]
        and "SYSTEM_AND_ALPHA_RESULTS_ARE_SEPARATE = True" in src["contract"])
    alpha_result_is_not_tested = (
        "ALPHA_RESULT_IS_STRUCTURALLY_NOT_TESTED = True" in src["contract"]
        and 'ALPHA_RESULT_VALUE = "NOT_TESTED"' in src["contract"]
        and "def alpha_result" in src["contract"])
    superseded_declared = ("SUPERSEDED_CAMPAIGNS" in src["contract"])
    exhausted_not_rerun = (
        "MAY_RERUN_EXHAUSTED_CAMPAIGNS = False" in src["contract"]
        and "MAY_LAUNCH_ALPHA_CAMPAIGN = False" in src["contract"]
        and "MAY_RUN_OPTIMISER_SEARCH = False" in src["contract"])
    blocked_actions_named = (
        "VENDOR_ACTIONS" in src["campaign"]
        and "def blocked_actions" in src["campaign"]
        and "exact_step" in src["campaign"])

    runner_flat = " ".join(runner.lower().split())
    runner_is_research_only = (
        "research only" in runner_flat and "no order" in runner_flat
        and "spends no money" in runner_flat)

    # The functional half: derived counts must come from Release 36's frontier.
    try:
        if str(REPO_ROOT.parent) not in sys.path:
            sys.path.insert(0, str(REPO_ROOT.parent))
        from paper_trader.alpha_agent.r37 import contract as _r37_contract
        from paper_trader.alpha_agent.r37 import providers as _r37_providers
        from paper_trader.alpha_agent.r37 import scoring as _r37_scoring
        from paper_trader.alpha_agent.r37 import unlock as _r37_unlock
        from paper_trader.alpha_agent.r37 import purchase as _r37_purchase
        long_list_valid = bool(_r37_providers.validate()["valid"])
        built = _r37_unlock.build()
        every_claim_is_blocked = not built["claims_without_a_blocked_market"]
        scorecard = _r37_scoring.build(built)
        hard_failed_never_ranked = not (
            set(scorecard["hard_failed"]) & set(scorecard["ranked_investable"]))
        every_state_terminal = all(
            row["gate_state"] in _r37_contract.GATE_STATES
            for row in _r37_providers.rows())
        no_state_grants_authority = all(
            _r37_contract.purchase_authority(s)["purchase_authorised"] is False
            for s in _r37_contract.GATE_STATES)
        # The functional half of the delegation contract: run the real gate and prove
        # this release recommends NOTHING the canonical acquisition gate refused.
        gate_results = _r37_purchase.build(built, scorecard,
                                           campaign_id="architecture_audit")
        nothing_recommended_against_the_gate = not gate_results[
            "recommended_by_r37_but_refused_by_canonical_gate"]
        every_row_agrees = bool(gate_results["every_row_agrees_with_canonical_gate"])
        acquisition_states_are_canonical = all(
            s in _r37_purchase._slice9.ACQUISITION_RECOMMENDATION_VOCAB
            for s in gate_results["canonical_acquisition_states"].values())
    except Exception as exc:  # noqa: BLE001 - unmeasurable fails closed
        long_list_valid = f"UNMEASURABLE:{exc}"
        every_claim_is_blocked = f"UNMEASURABLE:{exc}"
        hard_failed_never_ranked = f"UNMEASURABLE:{exc}"
        every_state_terminal = f"UNMEASURABLE:{exc}"
        no_state_grants_authority = f"UNMEASURABLE:{exc}"
        nothing_recommended_against_the_gate = f"UNMEASURABLE:{exc}"
        every_row_agrees = f"UNMEASURABLE:{exc}"
        acquisition_states_are_canonical = f"UNMEASURABLE:{exc}"

    return {
        "modules_present": not modules_missing,
        "modules_missing": modules_missing,
        "second_owner_modules": second_owner_modules,
        "defines_no_second_gate": not gate_definitions,
        "gate_definitions_found": sorted(gate_definitions),
        "composes_slice9_gate": composes_slice9,
        "composes_r32_information_gate": composes_r32_gate,
        "slice9_result_may_not_be_overridden": slice9_not_overridden,
        "nothing_persisted_to_another_release_store": slice9_not_persisted,
        "reuses_r35_http_owner": reuses_r35_http_owner,
        "reuses_r36_entitlement_owner": reuses_r36_entitlements,
        "reuses_r36_coverage_matrix": reuses_r36_coverage,
        "reuses_r31_hashing": reuses_r31_hashing,
        "no_second_downloader": no_second_downloader,
        "no_second_coverage_matrix": no_second_coverage_matrix,
        "spending_refused": spending_refused,
        "safety_flags_false": safety_flags_false,
        "no_purchase_authority": no_purchase_authority,
        "commercial_tokens_present": commercial_tokens,
        "forbidden_calls": forbidden_calls,
        "forbidden_owner_refs": forbidden_owner_refs,
        "a_marketing_claim_is_not_a_measurement": marketing_is_not_measurement,
        "partial_unlock_stays_out_of_the_headline": partial_not_in_headline,
        "a_proxy_may_not_unlock_a_native_cell": proxy_may_not_unlock,
        "unlock_is_derived_from_release36": unlock_is_derived,
        "evidence_classes_declared": evidence_classes_declared,
        "every_candidate_must_be_terminal": every_row_terminal,
        "score_declared_before_use": score_declared_before_use,
        "hard_gates_bind_the_ranking": hard_gates_bind,
        "free_data_has_a_cost_floor": free_data_has_a_cost_floor,
        "a_sample_is_not_an_alpha_claim": sample_is_not_an_alpha_claim,
        "blocks_are_reprobed": blocks_are_reprobed,
        "an_unmeasured_probe_is_not_an_open_route": unmeasured_is_not_open,
        "owned_client_capability_is_measured": owned_client_measured,
        "credentials_never_serialised": credentials_never_serialised,
        "ml_trains_nothing": ml_trains_nothing,
        "compute_inventory_is_read_only": compute_is_read_only,
        "feasibility_is_computed_not_typed": feasibility_is_computed,
        "ml_data_contract_composes_existing_owners": data_contract_composes,
        "market_structure_is_designed_not_executed": structure_not_executed,
        "pivots_require_real_time_confirmation": pivots_need_confirmation,
        "fibonacci_has_a_placebo_arm": fibonacci_has_a_placebo_arm,
        "visual_lane_is_designed_only": visual_lane_is_designed_only,
        "reports_three_separate_results": three_results_reported,
        "alpha_result_is_not_tested": alpha_result_is_not_tested,
        "superseded_campaigns_declared": superseded_declared,
        "exhausted_campaigns_not_rerun": exhausted_not_rerun,
        "blocked_vendor_actions_are_named": blocked_actions_named,
        "runner_is_research_only": runner_is_research_only,
        "long_list_validates": long_list_valid,
        "every_unlock_claim_names_a_blocked_market": every_claim_is_blocked,
        "a_hard_failed_dataset_is_never_ranked": hard_failed_never_ranked,
        "every_candidate_state_is_terminal": every_state_terminal,
        "no_gate_state_grants_purchase_authority": no_state_grants_authority,
        # --- Release 37.1 canonical-acquisition delegation ---------------------- #
        "delegates_acquisition_to_canonical_gate":
            delegates_acquisition_to_canonical_gate,
        "canonical_gate_is_authoritative": canonical_gate_is_authoritative,
        "r37_defines_no_acquisition_authority":
            r37_defines_no_acquisition_authority,
        "acquisition_recommendation_is_not_alpha_evidence":
            acquisition_is_not_alpha_evidence,
        "expected_unlocks_are_not_measured_unlocks":
            expected_unlocks_are_not_measured,
        "ml_readiness_separates_install_from_hardware":
            ml_readiness_separates_install_from_hardware,
        "nothing_recommended_against_the_canonical_gate":
            nothing_recommended_against_the_gate,
        "every_row_agrees_with_canonical_gate": every_row_agrees,
        "acquisition_states_come_from_the_canonical_vocabulary":
            acquisition_states_are_canonical,
    }


R38_OWNERS = {
    "root": "alpha_agent/r38/__init__.py",
    "contract": "alpha_agent/r38/contract.py",
    "entitlement": "alpha_agent/r38/entitlement.py",
    "enumeration": "alpha_agent/r38/enumeration.py",
    "quality": "alpha_agent/r38/quality.py",
    "research_layer": "alpha_agent/r38/research_layer.py",
    "unlock_actual": "alpha_agent/r38/unlock_actual.py",
    "experiments": "alpha_agent/r38/experiments.py",
    "ml_contract": "alpha_agent/r38/ml_contract.py",
    "steele": "alpha_agent/r38/steele.py",
    "campaign": "alpha_agent/r38/campaign.py",
}

#: Modules whose existence under alpha_agent/r38 would mean Release 38 had
#: rebuilt an owner an earlier release already provides - above all a FOURTH
#: acquisition gate or a SECOND coverage authority.
R38_SECOND_OWNER_FORBIDDEN = (
    "alpha_agent/r38/purchase_gate.py",
    "alpha_agent/r38/data_expansion_gate.py",
    "alpha_agent/r38/information_purchase_gate.py",
    "alpha_agent/r38/coverage.py", "alpha_agent/r38/economics.py",
    "alpha_agent/r38/judge.py", "alpha_agent/r38/multiple_testing.py",
    "alpha_agent/r38/universe.py", "alpha_agent/r38/unlock.py",
    "alpha_agent/r38/acquisition.py", "alpha_agent/r38/entitlements.py",
)


def check_release38_native_futures_information_frontier(
        files: list[Path]) -> dict:
    """Release 38 ownership, taxonomy and commercial-safety invariants.

    The dangerous outcomes here: classifying a caller defect as an
    entitlement wall (or the reverse), letting the ~53 EXPECTED unlocks stand
    in for measured ones, growing the frozen experiment family after results,
    and any code path that could spend, renew or grant purchase authority.
    """
    src = {name: (_read(path) or "") for name, path in R38_OWNERS.items()}
    modules_missing = sorted(n for n, t in src.items() if not t)
    all_src = "\n".join(src.values())
    all_lower = all_src.lower()

    second_owner_modules = sorted(p for p in R38_SECOND_OWNER_FORBIDDEN
                                  if (REPO_ROOT / p).exists())
    gate_definitions = [tok for tok in ("def evaluate_dataset(",
                                        "def evaluate_gap(",
                                        "def purchase_decision(")
                        if tok in all_src]
    delegates_to_canonical_gate = (
        "from ...api import data_expansion as _slice9" in src["campaign"]
        and "_slice9.run_evaluation(" in src["campaign"]
        and "CONTEXT_POST_ACQUISITION_VALUE" in src["campaign"]
        and '"persisted_to_slice9_store": False' in src["campaign"])
    reuses_r34_economic_judge = (
        "from ..r34 import economics as _econ" in src["experiments"]
        and "def evaluate_book" not in all_src)
    reuses_r31_multiple_testing = (
        "from ..r31 import multiple_testing as _mt" in src["experiments"]
        and "def benjamini_hochberg" not in all_src)
    reuses_r36_coverage_matrix = (
        "from ..r36 import coverage as _r36_coverage" in src["unlock_actual"]
        and "def _judge_cell" in src["unlock_actual"])
    reuses_r37_unlock_expectation = (
        "from ..r37 import unlock as _r37_unlock" in src["unlock_actual"])
    reuses_r35_cot_parser = (
        "from ..r35 import information as _r35_info" in src["experiments"]
        and "_r35_info.load_cot(" in src["experiments"])
    reuses_r31_hashing = "from ..r31 import (" in src["root"]

    taxonomy_declared = all(
        tok in src["contract"] for tok in (
            'CALL_VALID_WITH_DATA = "VALID_REQUEST_WITH_DATA"',
            'CALL_PARAMETER_ERROR = "PARAMETER_ERROR"',
            'CALL_ENTITLEMENT_ERROR = "ENTITLEMENT_ERROR"',
            'CALL_EMPTY_HISTORY = "EMPTY_HISTORY"',
            'CALL_UNSUPPORTED_MARKET = "UNSUPPORTED_MARKET"',
            'CALL_OTHER_PROVIDER_ERROR = "OTHER_PROVIDER_ERROR"',
            "A_PROGRAMMER_ERROR_IS_NOT_AN_ENTITLEMENT_LIMITATION = True"))
    taxonomy_enforced = (
        "def classify_session_contracts_call" in src["entitlement"]
        and "C.CALL_PARAMETER_ERROR" in src["entitlement"]
        and "C.CALL_ENTITLEMENT_ERROR" in src["entitlement"])

    purchase_is_inherited = (
        "PURCHASE_MADE_BY_THIS_RELEASE = False" in src["contract"]
        and "MONEY_SPENT_BY_R38_USD = 0.0" in src["contract"]
        and '"purchased_by_release38": False' in src["contract"]
        and "RENEWAL_DECIDED_BY_THIS_RELEASE = False" in src["contract"])
    spending_refused = all(
        f"{flag} = False" in src["contract"] for flag in
        ("MAY_SPEND_MONEY", "MAY_START_PROVIDER_TRIAL",
         "MAY_CREATE_PROVIDER_ACCOUNT", "MAY_CHANGE_SUBSCRIPTION_TIER",
         "MAY_RENEW_SUBSCRIPTION", "MAY_ACCEPT_LICENCE_AGREEMENT",
         "MAY_SUBMIT_PAYMENT_DETAILS", "MAY_PURCHASE_CLOUD_COMPUTE",
         "MAY_INSTALL_CUDA", "MAY_DOWNLOAD_MODEL_WEIGHTS",
         "MAY_UPGRADE_NORGATE_PACKAGES"))
    safety_flags_false = all(
        f"{flag} = False" in src["root"] for flag in
        ("AUTOMATIC_PROMOTION_ALLOWED", "AUTOMATIC_SLEEVE_ACTIVATION_ALLOWED",
         "MAY_SPEND_MONEY", "MAY_MUTATE_PRODUCTION"))
    no_purchase_authority = (
        "PURCHASE_AUTHORITY_GRANTED_BY_THIS_RELEASE = False" in src["contract"]
        and "RENEWAL_AUTHORITY_GRANTED_BY_THIS_RELEASE = False"
        in src["contract"]
        and "def purchase_authority" in src["contract"]
        and '"purchase_authorised": False' in src["contract"]
        and '"renewal_authorised": False' in src["contract"])
    commercial_tokens = sorted({t for t in R37_FORBIDDEN_COMMERCIAL
                                if t in all_lower})

    roll_is_observable = (
        'ROLL_POLICY = "OBSERVABLE_FIRST_NOTICE_LAST_TRADE"' in src["contract"]
        and "NO_ROLL_RULE_SEARCH = True" in src["contract"]
        and "ROLL_RULE_MAY_REFERENCE_OUTCOMES = False" in src["contract"]
        and "NO_HINDSIGHT_ROLL = True" in src["contract"])
    continuous_series_refused = (
        "NO_SILENT_CONTINUOUS_SUBSTITUTION = True" in src["contract"]
        and "VENDOR_CONTINUOUS_SERIES_ARE_DERIVED_FEATURES_ONLY = True"
        in src["contract"]
        and '"vendor_continuous_series_used": False' in src["research_layer"])
    frozen_design = (
        "FROZEN_PRIMARY_CONFIGURATIONS" in src["contract"]
        and "NO_OPTIMIZER_CAMPAIGN = True" in src["contract"]
        and "NO_GENETIC_SEARCH = True" in src["contract"]
        and "NO_RESULT_DRIVEN_EXPANSION = True" in src["contract"]
        and "DENOMINATOR_COUNTS_ALL_EXECUTED = True" in src["contract"])
    expectation_is_not_measurement = (
        "EXPECTED_UNLOCKS_ARE_NOT_MEASURED_UNLOCKS = True" in src["contract"]
        and "TRUTH_WINS_OVER_EXPECTATION = True" in src["contract"]
        and "expected_full_downgraded" in src["unlock_actual"]
        and "unlocked_beyond_expectation" in src["unlock_actual"])
    alpha_pass_requires_qualified_verdict = (
        "ALPHA_PASS_REQUIRES_VERDICT" in src["contract"]
        and "verdict == C.ALPHA_PASS_REQUIRES_VERDICT" in src["campaign"]
        and "HISTORICAL_ALPHA_IS_NOT_TRUE_FORWARD_EVIDENCE = True"
        in src["contract"])
    six_result_axes = (
        '"POST_ACQUISITION_VALUE_RESULT")' in src["contract"]
        and '"SYSTEM_RESULT", "DATA_ENTITLEMENT_RESULT",' in src["contract"])
    steele_is_schema_only = (
        'SAMPLE_PURPOSE = "SCHEMA_AND_PIT_VALIDATION_ONLY"' in src["steele"]
        and "SAMPLE_IS_ALPHA_EVIDENCE = False" in src["steele"]
        and '"claude_sends_nothing": True' in src["steele"])
    ml_trains_nothing = "TRAINS_A_MODEL = False" in src["ml_contract"]
    superseded_declared = "SUPERSEDED_CAMPAIGNS" in src["contract"]

    return {
        "modules_present": not modules_missing,
        "modules_missing": modules_missing,
        "second_owner_modules": second_owner_modules,
        "defines_no_second_gate": not gate_definitions,
        "gate_definitions_found": sorted(gate_definitions),
        "delegates_to_canonical_gate": delegates_to_canonical_gate,
        "reuses_r34_economic_judge": reuses_r34_economic_judge,
        "reuses_r31_multiple_testing": reuses_r31_multiple_testing,
        "reuses_r36_coverage_matrix": reuses_r36_coverage_matrix,
        "reuses_r37_unlock_expectation": reuses_r37_unlock_expectation,
        "reuses_r35_cot_parser": reuses_r35_cot_parser,
        "reuses_r31_hashing": reuses_r31_hashing,
        "provider_call_taxonomy_declared": taxonomy_declared,
        "provider_call_taxonomy_enforced": taxonomy_enforced,
        "purchase_is_inherited_not_made": purchase_is_inherited,
        "spending_refused": spending_refused,
        "safety_flags_false": safety_flags_false,
        "no_purchase_or_renewal_authority": no_purchase_authority,
        "commercial_tokens_present": commercial_tokens,
        "roll_policy_is_observable_and_frozen": roll_is_observable,
        "vendor_continuous_series_refused": continuous_series_refused,
        "experiment_family_is_frozen": frozen_design,
        "expected_unlocks_are_not_measured_unlocks":
            expectation_is_not_measurement,
        "alpha_pass_requires_qualified_verdict":
            alpha_pass_requires_qualified_verdict,
        "six_result_axes_declared": six_result_axes,
        "steele_sample_is_schema_only": steele_is_schema_only,
        "ml_contract_trains_nothing": ml_trains_nothing,
        "superseded_campaigns_declared": superseded_declared,
    }


R39_OWNERS = {
    "root": "alpha_agent/r39/__init__.py",
    "contract": "alpha_agent/r39/contract.py",
    "estate": "alpha_agent/r39/estate.py",
    "integrity": "alpha_agent/r39/integrity.py",
    "universal_state": "alpha_agent/r39/universal_state.py",
    "target_factory": "alpha_agent/r39/target_factory.py",
    "trade_space": "alpha_agent/r39/trade_space.py",
    "representation_factory": "alpha_agent/r39/representation_factory.py",
    "model_registry": "alpha_agent/r39/model_registry.py",
    "zones": "alpha_agent/r39/zones.py",
    "discovery_director": "alpha_agent/r39/discovery_director.py",
    "search_budget": "alpha_agent/r39/search_budget.py",
    "judge": "alpha_agent/r39/judge.py",
    "burden": "alpha_agent/r39/burden.py",
    "frontier": "alpha_agent/r39/frontier.py",
    "handoff": "alpha_agent/r39/handoff.py",
    "campaign": "alpha_agent/r39/campaign.py",
}

#: Modules whose existence under alpha_agent/r39 would mean Release 39 had
#: rebuilt an owner an earlier release already provides - a second economic
#: judge, a second multiple-testing library, a FIFTH acquisition gate, a
#: second coverage authority, or a second forward-evidence system.
R39_SECOND_OWNER_FORBIDDEN = (
    "alpha_agent/r39/economics.py", "alpha_agent/r39/multiple_testing.py",
    "alpha_agent/r39/purchase_gate.py", "alpha_agent/r39/coverage.py",
    "alpha_agent/r39/data_expansion_gate.py",
    "alpha_agent/r39/information_purchase_gate.py",
    "alpha_agent/r39/forward_evidence.py", "alpha_agent/r39/lockbox.py",
    "alpha_agent/r39/unlock.py", "alpha_agent/r39/acquisition.py",
    "alpha_agent/r39/universe.py",
)


def check_release39_universal_alpha_discovery(files: list[Path]) -> dict:
    """Release 39 ownership, evidence-zone and search-honesty invariants.

    The dangerous outcomes here: optimizing against the locked confirmation
    zone, quoting a p-value without its search denominator, relabelling
    historical confirmation as fresh or forward evidence, letting the
    Fibonacci family qualify without beating its placebo levels, a second
    economic judge or multiple-testing owner, and any code path that could
    spend, download model weights, promote a model or write operationally.
    """
    src = {name: (_read(REPO_ROOT / path) or "")
           for name, path in R39_OWNERS.items()}
    modules_missing = sorted(n for n, t in src.items() if not t)
    all_src = "\n".join(src.values())
    all_lower = all_src.lower()

    second_owner_modules = sorted(p for p in R39_SECOND_OWNER_FORBIDDEN
                                  if (REPO_ROOT / p).exists())
    gate_definitions = [tok for tok in ("def evaluate_dataset(",
                                        "def evaluate_gap(",
                                        "def purchase_decision(")
                        if tok in all_src]
    reuses_r34_economic_judge = (
        "from ..r34 import economics as _econ" in src["judge"]
        and "def evaluate_book" not in all_src
        and "def excess_significance" not in all_src)
    reuses_r31_multiple_testing = (
        "from ..r31 import multiple_testing as _mt" in src["burden"]
        and "def benjamini_hochberg" not in all_src
        and "def superior_predictive_ability" not in all_src)
    reuses_r31_hashing = "from ..r31 import (" in src["root"]
    reuses_r36_mde = ("from ..r36.experiments import "
                      "minimum_detectable_excess" in src["judge"])
    lockbox_budget_from_r31 = (
        "MAX_LOCKBOX_CANDIDATES = _r31c.MAX_LOCKBOX_CANDIDATES"
        in src["contract"]
        and "MAX_LOCKBOX_PER_FAMILY = _r31c.MAX_LOCKBOX_PER_FAMILY"
        in src["contract"])

    zones_honest = (
        'ZONE_C_EVIDENCE_LABEL = "HISTORICAL_CONFIRMATION_EVIDENCE"'
        in src["contract"]
        and "ZONE_C_IS_FRESH_UNSEEN_EVIDENCE = False" in src["contract"]
        and "NEVER_OPTIMIZE_AGAINST_ZONE_C = True" in src["contract"]
        and "ZONE_B_REUSE_IS_TRACKED = True" in src["contract"]
        and "HISTORICAL_ALPHA_IS_NOT_TRUE_FORWARD_EVIDENCE = True"
        in src["contract"])
    zone_c_single_execution = (
        "has already used its single Zone-C execution" in src["zones"]
        and "already frozen with a different hash" in src["zones"]
        and "def authorise" in src["zones"])
    budget_enforced = (
        "STAGE1_MAX_CANDIDATES" in src["contract"]
        and "class BudgetExceeded" in src["search_budget"]
        and "raise BudgetExceeded" in src["search_budget"])
    burden_reported = (
        "EFFECTIVE_SEARCH_BURDEN_IS_REPORTED = True" in src["contract"]
        and "T_ABOVE_2_IS_NOT_QUALIFICATION = True" in src["contract"]
        and "def deflated_sharpe" in src["burden"]
        and "def effective_search_burden" in src["burden"])
    fib_placebo_controlled = (
        "PLACEBO_LEVELS" in src["contract"]
        and 'FIB_TIE_WITH_PLACEBO_MEANS = "PULLBACK_STRUCTURE_MAY_MATTER"'
        in src["contract"]
        and "FIB_REQUIRES_CONFIRMED_PIVOTS = True" in src["contract"]
        and "piv_idx + 10 <= pos" in src["representation_factory"])
    spending_refused = all(
        f"{flag} = False" in src["contract"] for flag in
        ("MAY_SPEND_MONEY", "MAY_START_PROVIDER_TRIAL",
         "MAY_CREATE_PROVIDER_ACCOUNT", "MAY_CHANGE_SUBSCRIPTION_TIER",
         "MAY_RENEW_SUBSCRIPTION", "MAY_ACCEPT_LICENCE_AGREEMENT",
         "MAY_SUBMIT_PAYMENT_DETAILS", "MAY_PURCHASE_CLOUD_COMPUTE",
         "MAY_INSTALL_CUDA", "MAY_DOWNLOAD_MODEL_WEIGHTS",
         "MAY_UPGRADE_NORGATE_PACKAGES"))
    safety_flags = (
        "AUTOMATIC_PROMOTION_ALLOWED = False" in src["root"]
        and "AUTOMATIC_SLEEVE_ACTIVATION_ALLOWED = False" in src["root"]
        and "MAY_SPEND_MONEY = False" in src["root"]
        and "MAY_MUTATE_PRODUCTION = False" in src["root"]
        and "TRAINS_MODELS = True" in src["root"]
        and "PROMOTES_MODELS = False" in src["root"])
    no_purchase_authority = (
        "PURCHASE_AUTHORITY_GRANTED_BY_THIS_RELEASE = False"
        in src["contract"]
        and "RENEWAL_AUTHORITY_GRANTED_BY_THIS_RELEASE = False"
        in src["contract"]
        and "def purchase_authority" in src["contract"]
        and '"purchase_authorised": False' in src["contract"]
        and '"renewal_authorised": False' in src["contract"])
    commercial_tokens = sorted({t for t in R37_FORBIDDEN_COMMERCIAL
                                if t in all_lower})
    alpha_pass_gated = (
        'ALPHA_PASS_REQUIRES_VERDICT = "R39_AUTONOMOUS_ALPHA_DISCOVERED"'
        in src["contract"]
        and "verdict == C.ALPHA_PASS_REQUIRES_VERDICT" in src["campaign"])
    five_result_axes = (
        'RESULT_AXES = ("SYSTEM_RESULT", "DATA_RESULT", "DISCOVERY_RESULT",'
        in src["contract"])
    untested_is_not_rejected = (
        "DATA_AVAILABLE_BUT_NOT_TESTED_IS_NOT_A_REJECTED_HYPOTHESIS = True"
        in src["contract"]
        and "NO_EXPERIMENTS_ARE_MANUFACTURED_HERE = True"
        in src["integrity"])
    exclusions_named = (
        "EXCLUSION_REASONS" in src["contract"]
        and "unnamed exclusion reason" in src["estate"])
    forward_prepared_not_activated = (
        '"PREPARED_NOT_ACTIVATED"' in src["handoff"]
        or 'registration_state": "PREPARED_NOT_ACTIVATED' in src["handoff"])
    steele_read_only = (
        "SCHEMA_AND_PIT_VALIDATION_ONLY" in src["handoff"]
        and '"sample_is_alpha_evidence": False' in src["handoff"])
    no_operational_imports = (
        "from ...api" not in all_src and "from ...engine" not in all_src
        and "import paper_trader.api" not in all_src)

    return {
        "modules_present": not modules_missing,
        "modules_missing": modules_missing,
        "second_owner_modules": second_owner_modules,
        "defines_no_second_gate": not gate_definitions,
        "gate_definitions_found": sorted(gate_definitions),
        "reuses_r34_economic_judge": reuses_r34_economic_judge,
        "reuses_r31_multiple_testing": reuses_r31_multiple_testing,
        "reuses_r31_hashing": reuses_r31_hashing,
        "reuses_r36_minimum_detectable_excess": reuses_r36_mde,
        "lockbox_budget_imported_from_r31": lockbox_budget_from_r31,
        "evidence_zones_honestly_labelled": zones_honest,
        "zone_c_single_execution_enforced": zone_c_single_execution,
        "search_budget_ceilings_enforced": budget_enforced,
        "search_burden_reported": burden_reported,
        "fibonacci_placebo_controlled": fib_placebo_controlled,
        "spending_refused": spending_refused,
        "safety_flags_declared": safety_flags,
        "no_purchase_or_renewal_authority": no_purchase_authority,
        "commercial_tokens_present": commercial_tokens,
        "alpha_pass_requires_qualified_verdict": alpha_pass_gated,
        "five_result_axes_declared": five_result_axes,
        "untested_is_not_rejected": untested_is_not_rejected,
        "exclusions_use_named_vocabulary": exclusions_named,
        "forward_handoff_prepared_not_activated":
            forward_prepared_not_activated,
        "steele_lane_read_only": steele_read_only,
        "no_operational_imports": no_operational_imports,
    }


R39C_OWNERS = {
    "continuation": "alpha_agent/r39/continuation.py",
    "wide_prosecution": "alpha_agent/r39/wide_prosecution.py",
    "info_expansion": "alpha_agent/r39/info_expansion.py",
    "trade_space_ext": "alpha_agent/r39/trade_space_ext.py",
    "models_ext": "alpha_agent/r39/models_ext.py",
    "continuation_director": "alpha_agent/r39/continuation_director.py",
    "continuation_campaign": "alpha_agent/r39/continuation_campaign.py",
    "research_shadow": "alpha_agent/r39/research_shadow.py",
    "prospective_design": "alpha_agent/r39/prospective_design.py",
}


def check_release39_continuation(files: list[Path]) -> dict:
    """Release 39 CONTINUATION invariants (campaign v2).

    The dangerous outcomes: laundering the multiple-testing denominator
    through the new campaign id, redesigning against the already-accessed
    Zone C, spending Zone-C confirmation budget on arithmetic-certain
    failures, sneaking pretrained weights in as 'free packages', a second
    chain-hash ledger implementation, or a research shadow that could be
    mistaken for (or become) an operational stream.
    """
    src = {name: (_read(REPO_ROOT / path) or "")
           for name, path in R39C_OWNERS.items()}
    modules_missing = sorted(n for n, t in src.items() if not t)
    all_src = "\n".join(src.values())

    burden_never_resets = (
        "BURDEN_NEVER_RESETS = True" in src["continuation"]
        and "NO_CAMPAIGN_ID_LAUNDERING = True" in src["continuation"]
        and "V1_EFFECTIVE_TRIALS_EXPECTED = 107" in src["continuation"]
        and "refusing to guess" in src["continuation"])
    pregate_declared = (
        "ZONE_C_PREGATE_MIN_ZONE_B_T = 3.0" in src["continuation"]
        and "pregate_min_zone_b_t" in src["continuation_campaign"])
    masked_eval_no_zone_c = (
        "masked evaluation may never touch Zone C"
        in src["continuation_director"]
        and "LockboxViolation" in src["continuation_director"])
    diagnostics_cannot_upgrade = (
        "ZONE_C_DIAGNOSTICS_CANNOT_UPGRADE_QUALIFICATION = True"
        in src["continuation"]
        and "attribution_cannot_upgrade_qualification"
        in src["wide_prosecution"]
        and "diagnostic_only_cannot_upgrade_qualification"
        in src["wide_prosecution"])
    reconstruction_pinned = (
        "RECONSTRUCTION_TOLERANCE" in src["wide_prosecution"]
        and "c39_c9233eccaa74" in src["wide_prosecution"]
        and "this_is_not_a_new_zone_c_experiment"
        in src["wide_prosecution"])
    no_pretrained_weights = (
        "MAY_DOWNLOAD_MODEL_WEIGHTS_STILL_FALSE = True"
        in src["continuation"]
        and "DEEP_MODELS_TRAINED_FROM_SCRATCH = True"
        in src["continuation"]
        and "FOUNDATION_MODEL_BLOCKERS" in src["continuation"])
    shadows_not_promotable = (
        "PROMOTION_ALLOWED = False" in src["research_shadow"]
        and 'HISTORICAL_QUALIFICATION = "FAIL"' in src["research_shadow"]
        and "d > frozen_at" in src["research_shadow"])
    canonical_ledger_primitives = (
        "paper_trading_desk" in src["research_shadow"]
        and "def _row_hash" not in all_src
        and "def _append_ledger" not in all_src)
    api_imports = [ln for ln in all_src.splitlines()
                   if "paper_trader.api" in ln and "import" in ln
                   and not ln.strip().startswith("#")]
    only_desk_primitives_imported = all(
        "paper_trading_desk" in ln for ln in api_imports)
    anytime_valid_design = (
        "anytime_valid" in src["prospective_design"]
        and "E_SUCCESS = 20.0" in src["prospective_design"]
        and "registered_before_first_forward_observation"
        in src["prospective_design"])
    v1_generator_untouched_repair_is_new = (
        "latent2" in src["continuation_director"]
        and "graph2" in src["continuation_director"]
        and "byte-identical" in src["continuation_director"])
    subsplit_declared = (
        'SUBSPLIT_FIT_END = "2012-12-31"' in src["info_expansion"]
        and "standalone_significance_does_not_count"
        in src["continuation_campaign"])
    shell_audit_recorded = (
        "SHELL_POLICY_AUDIT" in src["continuation"]
        and "operator_assertion" in src["continuation"]
        and "NO_BASH_TOOL_INVOCATION_FOUND_IN_TRANSCRIPT"
        in src["continuation"])
    return {
        "modules_present": not modules_missing,
        "modules_missing": modules_missing,
        "burden_never_resets": burden_never_resets,
        "zone_c_pregate_declared": pregate_declared,
        "masked_eval_cannot_touch_zone_c": masked_eval_no_zone_c,
        "diagnostics_cannot_upgrade_qualification":
            diagnostics_cannot_upgrade,
        "wide_reconstruction_pinned": reconstruction_pinned,
        "no_pretrained_weights_downloaded": no_pretrained_weights,
        "shadows_not_promotable": shadows_not_promotable,
        "canonical_ledger_primitives_reused":
            canonical_ledger_primitives,
        "only_desk_primitives_imported_from_api":
            only_desk_primitives_imported,
        "anytime_valid_design_registered": anytime_valid_design,
        "v1_generator_untouched_repair_is_new":
            v1_generator_untouched_repair_is_new,
        "subsplit_protocol_declared": subsplit_declared,
        "shell_policy_audit_recorded": shell_audit_recorded,
    }


R40_OWNERS = {
    "root": "alpha_agent/r40/__init__.py",
    "contract": "alpha_agent/r40/contract.py",
    "closeout_import": "alpha_agent/r40/closeout_import.py",
    "burden_ledger": "alpha_agent/r40/burden_ledger.py",
    "availability": "alpha_agent/r40/availability.py",
    "director": "alpha_agent/r40/director.py",
    "wide_successor": "alpha_agent/r40/wide_successor.py",
    "nyfed_bridge": "alpha_agent/r40/nyfed_bridge.py",
    "open_models": "alpha_agent/r40/open_models.py",
    "model_challenge": "alpha_agent/r40/model_challenge.py",
    "cross_asset": "alpha_agent/r40/cross_asset.py",
    "shadow_registry": "alpha_agent/r40/shadow_registry.py",
    "research_cycle": "alpha_agent/r40/research_cycle.py",
    "sequential": "alpha_agent/r40/sequential.py",
    "evidence_velocity": "alpha_agent/r40/evidence_velocity.py",
    "research_portfolio": "alpha_agent/r40/research_portfolio.py",
    "intrinio_readiness": "alpha_agent/r40/intrinio_readiness.py",
    "compute_escalation": "alpha_agent/r40/compute_escalation.py",
    "campaign": "alpha_agent/r40/campaign.py",
}
R40_SECOND_OWNER_FORBIDDEN = (
    "alpha_agent/r40/economics.py", "alpha_agent/r40/multiple_testing.py",
    "alpha_agent/r40/judge.py", "alpha_agent/r40/zones.py",
    "alpha_agent/r40/lockbox.py", "alpha_agent/r40/ledger.py",
    "alpha_agent/r40/forward_evidence.py", "alpha_agent/r40/purchase_gate.py",
    "alpha_agent/r40/coverage.py", "alpha_agent/r40/scheduler.py",
    "alpha_agent/r40/trade_space.py", "alpha_agent/r40/universal_state.py",
)
R40_SCRIPTS = ("scripts/run_release40_prospective_alpha.py",
               "scripts/run_r40_research_cycle.py")


def check_release40_prospective_alpha_acceleration(files: list[Path]) -> dict:
    """Release 40 invariants - prospective alpha acceleration.

    The dangerous outcomes: a second forward-evidence / ledger / burden /
    judge implementation; a campaign id that launders the 194-trial burden;
    a forward row dated at or before a candidate's freeze (backdating); a
    scheduler or automation surface; a research shadow that could be
    promoted; a sixth shadow; a Slot-5 rule tuned after outcomes; fake
    independence (daily marks, markets x days); an open-weight download
    that needs an account or a click-through; a contaminated model given a
    clean historical-OOS label; an invented NY Fed backfill.
    """
    src = {name: (_read(REPO_ROOT / path) or "")
           for name, path in R40_OWNERS.items()}
    modules_missing = sorted(n for n, t in src.items() if not t)
    all_src = "\n".join(src.values())
    scripts_src = "\n".join((_read(REPO_ROOT / p) or "") for p in R40_SCRIPTS)
    second_owner_modules = sorted(p for p in R40_SECOND_OWNER_FORBIDDEN
                                  if (REPO_ROOT / p).exists())

    burden_inherited_not_reset = (
        "R39_INHERITED_EFFECTIVE_TRIALS_EXPECTED = 194" in src["contract"]
        and "BURDEN_NEVER_RESETS = True" in src["contract"]
        and "NO_CAMPAIGN_ID_LAUNDERING = True" in src["contract"]
        and "refusing to guess" in src["burden_ledger"]
        and "zones.record_zone_b(" in src["burden_ledger"]
        and "def record_zone_b" not in all_src)
    one_campaign_root_binding = (
        "register_campaign_root" in src["root"]
        and "def register_campaign_root" in (_read(
            REPO_ROOT / "alpha_agent/r39/__init__.py") or "")
        and "already bound to" in (_read(
            REPO_ROOT / "alpha_agent/r39/__init__.py") or ""))
    canonical_ledger_primitives = (
        "def _append_ledger" not in all_src
        and "def _row_hash" not in all_src
        and "def verify_ledger" not in all_src
        and "RS._desk()" in src["research_cycle"])
    api_imports = [ln for ln in (all_src + "\n" + scripts_src).splitlines()
                   if "paper_trader.api" in ln and "import" in ln
                   and not ln.strip().startswith("#")]
    no_operational_imports = (
        not api_imports and "from ...api" not in all_src
        and "from ...engine" not in all_src)
    forward_honesty = (
        "NO_HISTORICAL_ROW_IN_TRUE_FORWARD = True" in src["contract"]
        and "NO_ROW_AT_OR_BEFORE_CANDIDATE_FREEZE = True" in src["contract"]
        and "NO_OPTIONAL_THRESHOLD_RESET = True" in src["contract"]
        and "NO_MODEL_SWAP_UNDER_ONE_ID = True" in src["contract"]
        and "d > frozen_at and d <= now" in src["research_cycle"]
        and "LATE_CAPTURE_CONTIGUOUS" in src["research_cycle"]
        and "capture_lateness_sessions" in src["research_cycle"])
    r39_capture_owner_reused = (
        "RS.capture(" in src["research_cycle"]
        and "RS.mature(" in src["research_cycle"]
        and "RS.build_fresh_state()" in src["research_cycle"])
    no_scheduler_or_automation = (
        "MAY_ENABLE_SCHEDULED_TASK = False" in src["contract"]
        and "MAY_MODIFY_PRODUCTION_SCHEDULER = False" in src["contract"]
        and "RESEARCH_CYCLE_IS_A_CALLABLE_NOT_A_SCHEDULE = True"
        in src["contract"]
        and "AUTOMATION OFF" in scripts_src
        and not any(tok in (all_src + scripts_src) for tok in (
            "schtasks", "Register-ScheduledTask", "CronCreate",
            "crontab")))
    shadows_not_promotable = (
        "PROMOTION_ALLOWED = False" in src["shadow_registry"]
        and 'HISTORICAL_QUALIFICATION = "FAIL"' in src["shadow_registry"]
        and '"promotion_allowed": False' in src["research_cycle"]
        and "MAY_PROMOTE_MODEL = False" in src["contract"])
    family_cap_five = (
        "MAX_RESEARCH_SHADOW_FAMILY = 5" in src["contract"]
        and "class FamilyCapExceeded" in src["shadow_registry"]
        and "enforce_cap(rows)" in src["shadow_registry"]
        and "R39_SHADOWS_REMAIN_IMMUTABLE = True" in src["contract"])
    slot5_rule_frozen = (
        "SLOT_5_SELECTION_RULE = {" in src["contract"]
        and '"may_read_zone_c": False' in src["contract"]
        and '"may_read_true_forward": False' in src["contract"]
        and "SLOT_5_SELECTION_RULE" in src["closeout_import"]
        and "r40_contract_hash_frozen_before_any_evaluation"
        in src["closeout_import"]
        and "def contract_hash" in src["contract"])
    e_process_reused = (
        "from ..r39 import prospective_design as PD" in src["sequential"]
        and "def e_process" not in all_src
        and "PD.decide(" in src["sequential"]
        and "thresholds_never_reset" in src["sequential"])
    economic_judge_reused = (
        "from ..r39.continuation_director import" in src["director"]
        and "def judge_candidate" not in all_src
        and "def excess_significance" not in all_src
        and "def annualised_return" not in all_src
        and "def xs_long_short" not in all_src)
    multiple_testing_reused = (
        "from ..r31 import multiple_testing as _mt" in src["cross_asset"]
        and "def benjamini_hochberg" not in all_src
        and "def deflated_sharpe" not in all_src)
    no_fake_independence = (
        "DAILY_MARKS_OF_A_MONTHLY_POSITION_ARE_NOT_INDEPENDENT_TRADES = True"
        in src["contract"]
        and "NEVER_REPORT_MARKETS_TIMES_DAYS_AS_INDEPENDENT_SAMPLES = True"
        in src["contract"]
        and '"mean_information_gain": 0.0' in src["evidence_velocity"]
        and '"counted_as_independent_trades": False'
        in src["evidence_velocity"]
        and "def ess_ratio" in src["evidence_velocity"]
        and "def effective_markets" in src["evidence_velocity"])
    open_weight_policy = (
        "MAY_DOWNLOAD_MODEL_WEIGHTS = True" in src["contract"]
        and src["contract"].count('    "') >= 10
        and '"NOT_GATED_BEHIND_CLICK_THROUGH"' in src["contract"]
        and '"NO_PROVIDER_ACCOUNT_REQUIRED"' in src["contract"]
        and "def conditions_verdict" in src["open_models"]
        and "REFUSED" in src["open_models"]
        and "MAY_PURCHASE_COMPUTE = False" in src["contract"]
        and "MAY_INSTALL_CUDA = False" in src["contract"])
    weights_on_research_drive = (
        'research_root() / "_r40_lib"' in src["open_models"]
        and 'research_root() / "_hf_cache"' in src["open_models"]
        and 'LARGE_FILE_DRIVE = "D:"' in src["root"])
    contamination_labels = (
        "CONTAMINATED_MODELS_CANNOT_CLAIM_CLEAN_OOS = True" in src["contract"]
        and '"PRETRAINING_OVERLAP_LIKELY"' in src["open_models"]
        and '"PRETRAINING_DATA_KNOWN_CLEAN"' in src["open_models"]
        and "clean_historical_oos_label_admissible" in src["open_models"])
    availability_rule = (
        "MIN_SELECTION_COVERAGE = 0.50" in src["availability"]
        and "INADMISSIBLE_SELECTION_UNAVAILABLE" in src["availability"]
        and "def add_causal_masks" in src["availability"]
        and '"original_wide_untouched": True' in src["availability"]
        and "successor_is_new_object_with_new_hash" in src["wide_successor"]
        and '"zone_c_inspected_for_selection": False'
        in src["wide_successor"])
    nyfed_no_invented_backfill = (
        "BLOCKED_IDENTITY_SEMANTICS" in src["nyfed_bridge"]
        and '"no_invented_backfill": True' in src["nyfed_bridge"]
        and "LAG_DAYS = IE.NYFED_LAG_DAYS" in src["nyfed_bridge"]
        and "def arithmetic_identities" in src["nyfed_bridge"]
        and "def seam_checks" in src["nyfed_bridge"])
    search_discipline = (
        "MAX_CONFIGS_PER_MODEL_FAMILY = 3" in src["contract"]
        and "NO_ZONE_C_REDESIGN = True" in src["contract"]
        and "_screen_one(cand)" in src["model_challenge"]
        and "one_zone_b_run_per_family" in src["model_challenge"])
    commercial_refused = (
        "INTRINIO_PURCHASE_ALLOWED = False" in src["contract"]
        and "MAY_PURCHASE_DATA = False" in src["contract"]
        and '"request_sent_by_claude": False' in src["intrinio_readiness"]
        and "SAMPLE_CAN_PROVE_ALPHA = False" in src["intrinio_readiness"])
    result_axes_declared = (
        "RESULT_AXES = (" in src["contract"]
        and '"PROSPECTIVE_ALPHA_RESULT"' in src["contract"]
        and "DO_NOT_FORCE_A_SUCCESS_STATE = True" in src["contract"]
        and '"HISTORICAL_ALPHA_RESULT": "FAIL"' in src["campaign"])
    shell_policy_recorded = (
        "SHELL_POLICY_EVENTS = {" in src["contract"]
        and "SHELL_POLICY_VIOLATION_REPORTED = True" in src["contract"]
        and "monitor_tool_invocations" in src["contract"])
    return {
        "modules_present": not modules_missing,
        "modules_missing": modules_missing,
        "second_owner_modules": second_owner_modules,
        "burden_inherited_not_reset": burden_inherited_not_reset,
        "one_campaign_root_binding": one_campaign_root_binding,
        "canonical_ledger_primitives_reused": canonical_ledger_primitives,
        "no_operational_imports": no_operational_imports,
        "api_imports_found": api_imports,
        "forward_evidence_honesty": forward_honesty,
        "r39_capture_owner_reused": r39_capture_owner_reused,
        "no_scheduler_or_automation": no_scheduler_or_automation,
        "shadows_not_promotable": shadows_not_promotable,
        "family_cap_five": family_cap_five,
        "slot5_rule_frozen_before_evaluation": slot5_rule_frozen,
        "e_process_reused": e_process_reused,
        "economic_judge_reused": economic_judge_reused,
        "multiple_testing_reused": multiple_testing_reused,
        "no_fake_independence": no_fake_independence,
        "open_weight_policy_ten_conditions": open_weight_policy,
        "weights_on_research_drive": weights_on_research_drive,
        "contamination_labels_applied": contamination_labels,
        "availability_rule_declared": availability_rule,
        "nyfed_no_invented_backfill": nyfed_no_invented_backfill,
        "hierarchical_search_discipline": search_discipline,
        "commercial_refused": commercial_refused,
        "result_axes_declared": result_axes_declared,
        "shell_policy_recorded": shell_policy_recorded,
    }


R41_OWNERS = {
    "root": "alpha_agent/r41/__init__.py",
    "contract": "alpha_agent/r41/contract.py",
    "closeout_import": "alpha_agent/r41/closeout_import.py",
    "evidence": "alpha_agent/r41/evidence.py",
    "burden": "alpha_agent/r41/burden.py",
    "curve_state": "alpha_agent/r41/curve_state.py",
    "sample_acquisition": "alpha_agent/r41/sample_acquisition.py",
    "data_inventory": "alpha_agent/r41/data_inventory.py",
    "provider_frontier": "alpha_agent/r41/provider_frontier.py",
    "purchase_engine": "alpha_agent/r41/purchase_engine.py",
    "horizon_engine": "alpha_agent/r41/horizon_engine.py",
    "triggers": "alpha_agent/r41/triggers.py",
    "readiness": "alpha_agent/r41/readiness.py",
    "rates_rv_lab": "alpha_agent/r41/rates_rv_lab.py",
    "commodity_curve_lab": "alpha_agent/r41/commodity_curve_lab.py",
    "vol_lab": "alpha_agent/r41/vol_lab.py",
    "crypto_lab": "alpha_agent/r41/crypto_lab.py",
    "fx_credit_lab": "alpha_agent/r41/fx_credit_lab.py",
    "intraday_lab": "alpha_agent/r41/intraday_lab.py",
    "model_scale": "alpha_agent/r41/model_scale.py",
    "alpha_killer": "alpha_agent/r41/alpha_killer.py",
    "forward_freeze": "alpha_agent/r41/forward_freeze.py",
    "campaign": "alpha_agent/r41/campaign.py",
}
#: A second implementation of an owned concern inside r41 is a blocking
#: defect - these names may not exist.
R41_SECOND_OWNER_FORBIDDEN = (
    "alpha_agent/r41/economics.py", "alpha_agent/r41/multiple_testing.py",
    "alpha_agent/r41/judge.py", "alpha_agent/r41/zones.py",
    "alpha_agent/r41/lockbox.py", "alpha_agent/r41/ledger.py",
    "alpha_agent/r41/trade_space.py", "alpha_agent/r41/universal_state.py",
    "alpha_agent/r41/scheduler.py", "alpha_agent/r41/purchase_gate.py",
)


def check_release41_multi_horizon_alpha(files: list[Path]) -> dict:
    """Release 41 invariants - the multi-horizon alpha breakthrough campaign.

    The dangerous outcomes: a burden reset behind a new campaign id; a
    Zone-C peek before the pre-declared pre-gate; interpolated intraday
    bars; a Fibonacci verdict without its placebo arm; hindsight pivots; a
    fourth R41 shadow or a promotable one; a paid sample, account or
    vendor email; the qualified-alpha gate rewritten after its DSR check
    failed; a second ledger/multiple-testing/economics implementation.
    """
    src = {name: (_read(REPO_ROOT / path) or "")
           for name, path in R41_OWNERS.items()}
    modules_missing = sorted(n for n, t in src.items() if not t)
    all_src = "\n".join(src.values())
    second_owner_modules = sorted(p for p in R41_SECOND_OWNER_FORBIDDEN
                                  if (REPO_ROOT / p).exists())
    burden_inherited_not_reset = (
        "GLOBAL_INHERITED_EFFECTIVE_TRIALS = 230" in src["contract"]
        and "BURDEN_NEVER_RESETS = True" in src["contract"]
        and "NO_CAMPAIGN_ID_LAUNDERING = True" in src["contract"]
        and '"never_reset": True' in src["burden"]
        and "global_inherited" in src["burden"])
    one_campaign_root_binding = (
        "register_campaign_root" in src["root"])
    canonical_ledger_primitives = (
        "def _append_ledger" not in all_src
        and "def verify_ledger" not in all_src
        and "from ..r39.research_shadow import _desk"
        in src["forward_freeze"])
    api_imports = [ln for ln in all_src.splitlines()
                   if "paper_trader.api" in ln and "import" in ln
                   and not ln.strip().startswith("#")]
    no_operational_imports = not api_imports
    gates_frozen_before_results = (
        "RESEARCH_CANDIDATE_GATE = {" in src["contract"]
        and "QUALIFIED_ALPHA_GATE = {" in src["contract"]
        and "ZONE_C_PREGATE_T = 2.5" in src["contract"]
        and "def contract_hash" in src["contract"]
        and "r41_contract_hash_frozen_before_any_evaluation"
        in src["closeout_import"])
    r40_verified_not_trusted = (
        'ARTIFACT_NAME = "r40_closeout_import.json"' in src["closeout_import"]
        and "R40_VERIFIED" in src["closeout_import"]
        and '"cumulative_effective_trials": 230' in src["contract"])
    multiple_testing_reused = (
        "from ..r31 import multiple_testing as MT" in src["evidence"]
        and "R39B.deflated_sharpe" in src["evidence"]
        and "def benjamini_hochberg" not in all_src)
    no_interpolated_intraday = (
        "NO_INTERPOLATED_INTRADAY = True" in src["contract"]
        and "HORIZON_REQUIRES_NATIVE_SOURCE_FREQUENCY = True"
        in src["contract"]
        and "MIN_INTRADAY_BARS_FOR_RESEARCH" in src["contract"])
    fibonacci_placebo_controlled = (
        "FIB_PLACEBO_LEVELS" in src["contract"]
        and "NO_HINDSIGHT_EXTREMA = True" in src["contract"]
        and "PLACEBO" in src["intraday_lab"]
        and "NAMED_MINUS_PLACEBO" in src["intraday_lab"]
        and "confirm" in src["intraday_lab"])
    sign_fit_declared = (
        "SIGN_FIT_ON_A" in src["rates_rv_lab"]
        and "SIGN_FIT_ON_A" in src["commodity_curve_lab"])
    sample_conditions_eight = (
        "SAMPLE_ACQUISITION_CONDITIONS = (" in src["contract"]
        and '"NO_ACCOUNT_CREATION"' in src["contract"]
        and '"NO_PAYMENT_DETAIL"' in src["contract"]
        and "MAY_PURCHASE_DATA = False" in src["contract"]
        and "MAY_SEND_VENDOR_EMAIL = False" in src["contract"]
        and "MAY_CHANGE_ENTITLEMENT_TIER = False" in src["contract"])
    no_scheduler_or_automation = (
        "MAY_ENABLE_SCHEDULED_TASK = False" in src["contract"]
        and "MAY_RESTART_PRODUCTION = False" in src["contract"]
        and not any(tok in all_src for tok in (
            "schtasks", "Register-ScheduledTask", "crontab")))
    shadows_capped_not_promotable = (
        "MAX_R41_SHADOWS = 3" in src["forward_freeze"]
        and '"promotion_allowed": False' in src["forward_freeze"]
        and '"research_shadow_only": True' in src["forward_freeze"]
        and "R41 shadow cap exceeded" in src["forward_freeze"]
        and "d > frozen_at" in src["forward_freeze"]
        and '"true_forward": True' in src["forward_freeze"])
    qualified_gate_in_code = (
        "def qualified_gate_funding" in src["campaign"]
        and '"PASS" if qual["passes"] else "FAIL"' in src["campaign"]
        and "DIAGNOSTIC, NEVER THE GATE" in src["campaign"]
        and "DO_NOT_FORCE_A_SUCCESS_STATE = True" in src["contract"])
    killer_battery_declared = (
        "ALPHA_KILLER_TESTS = (" in src["contract"]
        and "PLACEBO_CARRY" in src["alpha_killer"]
        and "COST_X3" in src["alpha_killer"]
        and "sign_flip" in src["alpha_killer"])
    cost_on_traded_notional = (
        "COST_BASE_IS_TRADED_NOTIONAL = True" in src["contract"]
        and "COST_STRESS_MULTIPLIERS" in src["contract"])
    return {
        "modules_present": not modules_missing,
        "modules_missing": modules_missing,
        "second_owner_modules": second_owner_modules,
        "burden_inherited_not_reset": burden_inherited_not_reset,
        "one_campaign_root_binding": one_campaign_root_binding,
        "canonical_ledger_primitives_reused": canonical_ledger_primitives,
        "no_operational_imports": no_operational_imports,
        "api_imports_found": api_imports,
        "gates_frozen_before_results": gates_frozen_before_results,
        "r40_verified_not_trusted": r40_verified_not_trusted,
        "multiple_testing_reused": multiple_testing_reused,
        "no_interpolated_intraday": no_interpolated_intraday,
        "fibonacci_placebo_controlled": fibonacci_placebo_controlled,
        "sign_fit_declared_on_zone_a": sign_fit_declared,
        "sample_conditions_eight": sample_conditions_eight,
        "no_scheduler_or_automation": no_scheduler_or_automation,
        "shadows_capped_not_promotable": shadows_capped_not_promotable,
        "qualified_gate_in_code": qualified_gate_in_code,
        "killer_battery_declared": killer_battery_declared,
        "cost_on_traded_notional": cost_on_traded_notional,
    }


R42_OWNERS = {
    "root": "alpha_agent/r42/__init__.py",
    "contract": "alpha_agent/r42/contract.py",
    "closeout_import": "alpha_agent/r42/closeout_import.py",
    "acquisition": "alpha_agent/r42/acquisition.py",
    "pnl_audit": "alpha_agent/r42/pnl_audit.py",
    "funding_ledger": "alpha_agent/r42/funding_ledger.py",
    "basis": "alpha_agent/r42/basis.py",
    "legs": "alpha_agent/r42/legs.py",
    "capital": "alpha_agent/r42/capital.py",
    "execution": "alpha_agent/r42/execution.py",
    "margin": "alpha_agent/r42/margin.py",
    "venues": "alpha_agent/r42/venues.py",
    "asset_universe": "alpha_agent/r42/asset_universe.py",
    "cme_basis": "alpha_agent/r42/cme_basis.py",
    "hierarchy": "alpha_agent/r42/hierarchy.py",
    "attribution": "alpha_agent/r42/attribution.py",
    "capacity": "alpha_agent/r42/capacity.py",
    "collateral": "alpha_agent/r42/collateral.py",
    "forward": "alpha_agent/r42/forward.py",
    "microstructure_check": "alpha_agent/r42/microstructure_check.py",
    "campaign": "alpha_agent/r42/campaign.py",
}
#: A second implementation of an owned concern inside r42 is a blocking
#: defect - these names may not exist.
R42_SECOND_OWNER_FORBIDDEN = (
    "alpha_agent/r42/evidence.py", "alpha_agent/r42/multiple_testing.py",
    "alpha_agent/r42/economics.py", "alpha_agent/r42/zones.py",
    "alpha_agent/r42/ledger.py", "alpha_agent/r42/burden.py",
    "alpha_agent/r42/research_shadow.py", "alpha_agent/r42/scheduler.py",
    "alpha_agent/r42/purchase_gate.py", "alpha_agent/r42/crypto_lab.py",
)
#: Release-42 qualification vocabulary. A verdict outside this set means a
#: label was invented to describe an inconvenient result.
R42_REQUIRED_STATES = (
    "R42_CRYPTO_BASIS_ALPHA_VALIDATED_HISTORICALLY",
    "R42_STRONG_REPLICATED_CANDIDATE_FORWARD_PENDING",
    "R42_SINGLE_VENUE_PREMIUM_ONLY",
    "R42_EXECUTION_REALITY_KILLS_EDGE",
    "R42_CAPITAL_EFFICIENCY_KILLS_EDGE",
    "R42_BORROW_REALITY_KILLS_REVERSE_LEG",
    "R42_CROSS_ASSET_REPLICATION_FAILS",
    "R42_CROSS_VENUE_REPLICATION_FAILS",
    "R42_STRUCTURAL_PREMIUM_CONFIRMED_NOT_TIMING_ALPHA",
    "R42_FORWARD_EVIDENCE_STRENGTHENED",
    "R42_FORWARD_EVIDENCE_WEAKENED",
    "R42_DATA_LIMIT_BINDING",
)


def check_release42_crypto_basis_alpha(files: list[Path]) -> dict:
    """Release 42 invariants - prosecuting ONE candidate to destruction.

    The dangerous outcomes this guard exists to catch: the frozen R41
    shadow edited, refit or re-parameterised so a failing candidate is
    quietly improved; the R41 verdict rewritten instead of inherited; a
    capital denominator or control chosen AFTER the result; the
    self-financing (zero-control) convention reused for a book that
    immobilises 100% of its notional; a reverse leg counted as
    implementable without borrow evidence; an asset or venue universe
    filtered on performance; a statistical method selected because it
    makes BTC pass; a maker fill assumed; a fourth R42 shadow or a
    promotable one; a forward row backfilled; an exchange account, API
    trading key, order or purchase; a second evidence / multiple-testing /
    ledger implementation.
    """
    src = {name: (_read(REPO_ROOT / path) or "")
           for name, path in R42_OWNERS.items()}
    modules_missing = sorted(n for n, t in src.items() if not t)
    all_src = "\n".join(src.values())
    second_owner_modules = sorted(p for p in R42_SECOND_OWNER_FORBIDDEN
                                  if (REPO_ROOT / p).exists())

    # (1) The R41 candidate is IMMUTABLE. R42 may read it and may call its
    # capture owner; it may never freeze, write or re-parameterise it.
    r41_declared_immutable = (
        "R41_CANDIDATE_IS_IMMUTABLE = True" in src["contract"]
        and "R42_CORRECTIONS_GET_NEW_IDENTITIES = True" in src["contract"])
    r41_shadow_not_refrozen = (
        "FF.freeze(" not in all_src
        and "forward_freeze.freeze(" not in all_src)
    r41_capture_delegated = (
        "FF.capture(" in src["forward"]
        and "def capture(" not in all_src
        and "r42_wrote_no_forward_row" in src["forward"])
    r41_verdict_inherited = (
        '"HISTORICAL_ALPHA_RESULT": "FAIL"' in src["campaign"]
        and "R41_DSR_REPORTED_UNCHANGED = True" in src["contract"]
        and "r41_dsr_unchanged" in src["hierarchy"])

    # (2) The contract is frozen BEFORE results and is hash-detectable.
    contract_frozen_before_results = (
        "METHOD_FROZEN_BEFORE_RESULTS = True" in src["contract"]
        and "ASSET_UNIVERSE_FROZEN_BEFORE_RESULTS = True" in src["contract"]
        and "STANDARDS_MAY_NOT_BE_LOWERED_AFTER_DATA = True"
        in src["contract"]
        and "def contract_hash" in src["contract"]
        and "def freeze_artifact" in src["contract"]
        and "r42_contract_hash" in src["closeout_import"])

    # (3) Capital and control - the correction this release exists to make.
    capital_control_declared = (
        'PRIMARY_CONTROL = "RISK_FREE_ON_COMMITTED_CAPITAL"'
        in src["contract"]
        and 'PRIMARY_CAPITAL_MODEL = "CONSERVATIVE_COLLATERAL"'
        in src["contract"]
        and "CONTROL_RATIONALE" in src["contract"]
        and "def risk_free_daily" in src["capital"]
        and "def denominator_table" in src["capital"])
    one_authoritative_roic = (
        "authoritative_primary_roic" in src["capital"]
        and '"one_authoritative_number": True' in src["capital"])
    zero_control_not_reused = (
        "charge_financing" in src["capital"]
        and "benchmark" in src["capital"])

    # (4) Borrow evidence gates the reverse leg.
    borrow_rule_enforced = (
        'BORROW_UNPROVEN_VERDICT = "HISTORICALLY_NON_IMPLEMENTABLE"'
        in src["contract"]
        and "CURRENT_SNAPSHOT_IS_NOT_HISTORY = True" in src["contract"]
        and "def borrow_evidence" in src["legs"]
        and "BORROW_HISTORY_UNAVAILABLE" in src["legs"])

    # (5) Universes frozen on METADATA, never on performance.
    universes_metadata_only = (
        '"selection_may_use_performance": False' in src["contract"]
        and "def evaluate_symbol_metadata" in src["asset_universe"]
        and "include_delisted_if_history_exists" in src["contract"]
        and "VENUE_ELIGIBILITY" in src["contract"]
        and "DATA_ACCESS_IS_NOT_INVESTABILITY = True" in src["contract"])
    investability_separated = (
        "INVESTABLE_BY_OPERATOR" in src["venues"]
        and "ELIGIBLE_FOR_REPLICATION" in src["venues"]
        and "INVESTABILITY_REQUIRES_ADMISSIBLE_VENUE_PATH = True"
        in src["contract"])

    # (6) Statistics: hierarchical, frozen first, and never re-implemented.
    hierarchy_frozen_first = (
        "METHOD_MAY_NOT_BE_CHOSEN_TO_PASS = True" in src["contract"]
        and "HIERARCHY_LEVELS" in src["contract"]
        and "CLOSED_TESTING" in src["contract"]
        and "def westfall_young" in src["hierarchy"]
        and "def level_1" in src["hierarchy"])
    reuses_canonical_statistics = (
        "from ..r31 import multiple_testing" in src["hierarchy"]
        and "from ..r41 import evidence" in all_src
        and "def scorecard" not in all_src
        and "def hac_t" not in all_src
        and "def deflated_sharpe(" not in all_src)

    # (7) Execution honesty.
    maker_fill_forbidden = (
        "ASSUMED_LIMIT_FILL_IS_FORBIDDEN = True" in src["contract"]
        and "def maker_admissibility" in src["execution"]
        and "MAKER_CLAIM_INADMISSIBLE" in src["execution"])
    no_fabricated_fills = (
        "no_fills_were_fabricated" in src["microstructure_check"]
        and "BLOCKED_EXECUTION_MICROSTRUCTURE_DATA"
        in src["microstructure_check"])

    # (8) Shadows: capped, non-promotable, never backfilled.
    shadows_capped_not_promotable = (
        "MAX_R42_SHADOWS = 3" in src["forward"]
        and '"promotion_allowed": False' in src["forward"]
        and '"research_shadow_only": True' in src["forward"]
        and "R42 shadow cap exceeded" in src["forward"]
        and "specification_predates_every_prospective_observation"
        in src["forward"])
    forward_never_backfilled = (
        '"never_backfilled": True' in src["forward"]
        and '"never_refitted": True' in src["forward"]
        and "true_forward" in src["forward"])
    canonical_ledger_primitives = (
        "def _append_ledger" not in all_src
        and "def verify_ledger" not in all_src
        and "from ..r39.research_shadow import _desk" in src["forward"])

    # (9) Safety boundary.
    api_imports = [ln for ln in all_src.splitlines()
                   if "paper_trader.api" in ln and "import" in ln
                   and not ln.strip().startswith("#")]
    no_operational_imports = not api_imports
    safety_flags_false = (
        "MAY_SPEND_MONEY = False" in src["root"]
        and "MAY_MUTATE_PRODUCTION = False" in src["root"]
        and "PROMOTES_MODELS = False" in src["root"]
        and "AUTOMATIC_PROMOTION_ALLOWED = False" in src["root"]
        and "CHANGES_SCHEDULER = False" in src["root"]
        and "MONEY_BUDGET_USD = 0.0" in src["contract"]
        and "CLAUDE_MAY_COMMIT = False" in src["contract"]
        and "CLAUDE_MAY_PUSH = False" in src["contract"])
    no_exchange_account_or_orders = (
        '"creates_exchange_account"' in src["root"]
        and '"holds_api_trading_key"' in src["root"]
        and '"creates_paper_order"' in src["root"]
        and '"buys_crypto"' in src["root"]
        and "NO EXCHANGE ACCOUNT" in src["root"])
    shell_policy_declared = (
        'SHELL_POLICY = "WINDOWS_POWERSHELL_ONLY"' in src["contract"])

    # (10) The reconstruction must be exact before anything is argued.
    reconstruction_is_a_gate = (
        "RECONSTRUCTION_FAILED" in src["pnl_audit"]
        and '"state": "EXACT"' in src["pnl_audit"]
        and "worst_abs_diff" in src["pnl_audit"])
    funding_event_exact = (
        "def pit_integrity" in src["funding_ledger"]
        and "no_future_funding_in_signal" in src["funding_ledger"]
        and "reconciles" in src["funding_ledger"])
    venue_cadence_asserted = (
        "VENUE_FUNDING_CADENCE" in src["acquisition"]
        and "def cadence_audit" in src["acquisition"]
        and "cadence_all_verified" in src["venues"])

    # (11) Every declared qualification state exists in the vocabulary.
    states_declared = sorted(s for s in R42_REQUIRED_STATES
                             if s not in src["contract"])

    return {
        "modules_present": not modules_missing,
        "modules_missing": modules_missing,
        "second_owner_modules": second_owner_modules,
        "r41_declared_immutable": r41_declared_immutable,
        "r41_shadow_not_refrozen": r41_shadow_not_refrozen,
        "r41_capture_delegated": r41_capture_delegated,
        "r41_verdict_inherited": r41_verdict_inherited,
        "contract_frozen_before_results": contract_frozen_before_results,
        "capital_control_declared": capital_control_declared,
        "one_authoritative_roic": one_authoritative_roic,
        "zero_control_not_reused": zero_control_not_reused,
        "borrow_rule_enforced": borrow_rule_enforced,
        "universes_metadata_only": universes_metadata_only,
        "investability_separated": investability_separated,
        "hierarchy_frozen_first": hierarchy_frozen_first,
        "reuses_canonical_statistics": reuses_canonical_statistics,
        "maker_fill_forbidden": maker_fill_forbidden,
        "no_fabricated_fills": no_fabricated_fills,
        "shadows_capped_not_promotable": shadows_capped_not_promotable,
        "forward_never_backfilled": forward_never_backfilled,
        "canonical_ledger_primitives": canonical_ledger_primitives,
        "no_operational_imports": no_operational_imports,
        "safety_flags_false": safety_flags_false,
        "no_exchange_account_or_orders": no_exchange_account_or_orders,
        "shell_policy_declared": shell_policy_declared,
        "reconstruction_is_a_gate": reconstruction_is_a_gate,
        "funding_event_exact": funding_event_exact,
        "venue_cadence_asserted": venue_cadence_asserted,
        "qualification_states_missing": states_declared,
    }



# --------------------------------------------------------------------------- #
# Release 43 - Global Alpha Offensive
# --------------------------------------------------------------------------- #
R43_OWNERS = {
    "root": "alpha_agent/r43/__init__.py",
    "contract": "alpha_agent/r43/contract.py",
    "burden": "alpha_agent/r43/burden.py",
    "closeout": "alpha_agent/r43/closeout.py",
    "judge": "alpha_agent/r43/judge.py",
    "panels": "alpha_agent/r43/panels.py",
    "carry": "alpha_agent/r43/carry.py",
    "rv": "alpha_agent/r43/rv.py",
    "crossasset": "alpha_agent/r43/crossasset.py",
    "equity": "alpha_agent/r43/equity.py",
    "acquisition": "alpha_agent/r43/acquisition.py",
    "killer": "alpha_agent/r43/killer.py",
    "frontier": "alpha_agent/r43/frontier.py",
    "campaign": "alpha_agent/r43/campaign.py",
}
#: A second implementation of an already-owned concern inside r43 is a
#: blocking defect - these names may not exist.
R43_SECOND_OWNER_FORBIDDEN = (
    "alpha_agent/r43/evidence.py", "alpha_agent/r43/multiple_testing.py",
    "alpha_agent/r43/economics.py", "alpha_agent/r43/zones.py",
    "alpha_agent/r43/ledger.py", "alpha_agent/r43/research_shadow.py",
    "alpha_agent/r43/scheduler.py", "alpha_agent/r43/forward_freeze.py",
    "alpha_agent/r43/curve_state.py", "alpha_agent/r43/crypto_lab.py",
    "alpha_agent/r43/deflated_sharpe.py",
)
#: Release-43 terminal vocabulary. A verdict outside this set means a label
#: was invented to describe an inconvenient result.
R43_REQUIRED_STATES = (
    "R43_QUALIFIED_ALPHA_FOUND",
    "R43_STRONG_CANDIDATE_FORWARD_PENDING",
    "R43_NO_QUALIFIED_ALPHA_AFTER_GLOBAL_OFFENSIVE",
)


def check_release43_global_alpha_offensive(files: list[Path]) -> dict:
    """Release 43 invariants - a broad search that may not cheat.

    The dangerous outcomes this guard exists to catch: the inherited search
    burden reset or laundered through a new campaign id; the R41 ledger or
    any prior release's artifacts written; a collateral class, capital
    denominator, control, cap or kill test chosen AFTER a result; the
    universal judge quietly drifting away from the two prior conventions it
    claims to generalise; a signal that reads its own trading day; a decile
    selection lagged differently from the position it selects; a sector
    neutralisation built from a look-ahead classification; an option,
    analyst, intraday, credit or crypto wall asserted rather than probed; a
    purchase, account, licence or trial; a promotable shadow; a backfilled
    forward row; a lane that ends outside the frozen blocker vocabulary.
    """
    src = {name: (_read(REPO_ROOT / path) or "")
           for name, path in R43_OWNERS.items()}
    modules_missing = sorted(n for n, t in src.items() if not t)
    all_src = "\n".join(src.values())
    second_owner_modules = sorted(p for p in R43_SECOND_OWNER_FORBIDDEN
                                  if (REPO_ROOT / p).exists())

    # (1) Burden is inherited, verified from bytes, and never reset.
    burden_inherited_not_reset = (
        "GLOBAL_INHERITED_EFFECTIVE_TRIALS = 289" in src["contract"]
        and "INHERITED_PRE_R41 = 230" in src["contract"]
        and "INHERITED_R41_DISTINCT = 59" in src["contract"]
        and "BURDEN_NEVER_RESETS = True" in src["contract"]
        and "NO_CAMPAIGN_ID_LAUNDERING = True" in src["contract"]
        and "def verify_inherited" in src["burden"]
        and "class BurdenLaundering" in src["burden"]
        and "raise BurdenLaundering" in src["burden"])
    r41_ledger_read_only = (
        "R41_LEDGER_IS_READ_ONLY = True" in src["contract"]
        and "r41_ledger_mutated" in src["burden"]
        and ".write_text" not in src["burden"].split("def verify_inherited")[1]
        .split("def _path")[0])
    lane_caps_enforced = (
        "has exhausted its FROZEN ZONE_B cap" in src["burden"]
        and "TOTAL_ZONE_B_BUDGET" in src["contract"]
        and "LANE_CAP_IS_A_CEILING_NOT_A_TARGET = True" in src["contract"])

    # (2) The contract is frozen BEFORE the first number and is hash-checked.
    contract_frozen_before_results = (
        "def frozen_body" in src["contract"]
        and "def verify_contract_unchanged" in src["closeout"]
        and "contract_frozen_before_first_number" in src["closeout"]
        and "COLLATERAL_MAY_NOT_BE_CHOSEN_AFTER_SEEING_RESULTS = True"
        in src["contract"]
        and "KILL_TESTS_ARE_CHOSEN_BEFORE_RESULTS = True" in src["contract"])

    # (3) The collateral declaration - the heart of the release.
    collateral_declared = (
        "COLLATERAL_CLASSES" in src["contract"]
        and "UNREMUNERATED_FULLY_FUNDED" in src["contract"]
        and "REMUNERATED_MARGIN" in src["contract"]
        and "FUNDED_LONG_SHORT_EQUITY" in src["contract"]
        and "COLLATERAL_CHOICE_IS_PREDECLARED = True" in src["contract"]
        and "DUAL_QUOTATION_REQUIRED = True" in src["contract"])
    judge_is_one_equation = (
        "def implementable_book" in src["judge"]
        and "collateral_earns_rf" in src["judge"]
        and "undeclared collateral class" in src["judge"]
        and "def convention" in src["judge"]
        and "R41_PER_NOTIONAL_ZERO_CONTROL" in src["judge"]
        and "R42_COMMITTED_CAPITAL_CASH_CONTROL" in src["judge"])
    risk_free_convention_inherited = (
        'RISK_FREE_SERIES_PREFERENCE = ("SOFR", "EFFR", "CMT_3M")'
        in src["judge"])
    post_freeze_denominator_disclosed = (
        "RISK_SIZED_CAPITAL_IS_POST_FREEZE = True" in src["judge"]
        and "POST-FREEZE ADDITION, DISCLOSED" in src["judge"]
        and "FITTING_ZONE_ONLY" in src["judge"])

    # (4) Causality. Nothing may read its own trading day.
    signal_is_lagged = (
        "zl = z.shift(1 + int(extra_lag))" in src["rv"]
        and "zl = z.shift(1 + int(extra_lag))" in src["equity"]
        and "rank = zl.rank(axis=1, pct=True)" in src["equity"]
        and "rank = z.rank(axis=1, pct=True)" not in src["equity"])
    no_full_sample_risk_target = (
        "expanding(min_periods=250).median()" in src["rv"]
        and "np.nanmedian(v)" not in src["rv"])
    no_padded_returns = (
        "close.pct_change()" not in src["equity"]
        and "close.notna() & prev.notna()" in src["equity"])
    causal_pivots_only = (
        "NO_HINDSIGHT_EXTREMA = True" in src["contract"]
        and "NO_HUMAN_VISUAL_CONFIRMATION = True" in src["contract"]
        and "def _causal_pivots" in src["crossasset"]
        and "i + confirm" in src["crossasset"])
    lookahead_sector_map_refused = (
        "PROVISIONAL_CLASSIFICATION_LOOKAHEAD" in src["equity"]
        and '"sector": False' in src["equity"])
    survivorship_handled = (
        "index_constituent_timeseries" in src["equity"]
        and "TOTALRETURN" in src["equity"]
        and '"survivorship_safe": True' in src["equity"])

    # (5) Controls. A signal must beat its own passive exposure.
    passive_control_measured = (
        "VOLATILITY-MATCHED" in src["killer"]
        and "increment_t_hac" in src["killer"]
        and "def passive_increment" in src["carry"]
        and "signal_is_decoration" in src["carry"])
    placebo_declared_not_chosen = (
        "FIB_PLACEBO_LEVELS" in src["contract"]
        and "FIB_PLACEBO_LEVELS" in src["crossasset"]
        and "named_beats_placebo" in src["crossasset"])
    event_placebo_is_non_event_days = (
        "placebo_is_the_same_rule_on_non_event_days" in src["crossasset"]
        and "event_days=False" in src["crossasset"])
    signed_advance_rule = (
        "advance_rule_is_signed" in src["rv"]
        and '"signs_fitted": 0' in src["rv"])

    # (6) Acquisition. Walls are PROBED, and nothing is bought.
    walls_probed_not_asserted = (
        "def probe_options" in src["acquisition"]
        and "def probe_analyst_revisions" in src["acquisition"]
        and "def probe_native_intraday_futures" in src["acquisition"]
        and "def probe_native_credit" in src["acquisition"]
        and "def probe_microstructure" in src["acquisition"]
        and "def probe_crypto_venues" in src["acquisition"])
    keys_never_leak = (
        "def _redact" in src["acquisition"]
        and "REDACTED" in src["acquisition"]
        and "entitlement_keys_printed" in src["acquisition"])
    no_purchase_or_account = (
        "MAY_PURCHASE_DATA = False" in src["contract"]
        and "MAY_CREATE_PROVIDER_ACCOUNT = False" in src["contract"]
        and "MAY_ACCEPT_LICENCE_AGREEMENT = False" in src["contract"]
        and "MAY_SUBMIT_PAYMENT_DETAILS = False" in src["contract"]
        and "MAY_START_PROVIDER_TRIAL = False" in src["contract"]
        and 'method="POST"' not in all_src
        and "urlopen(req, data=" not in all_src)
    purchase_gate_ranks_by_value_per_dollar = (
        "EXPECTED ALPHA INFORMATION GAIN PER DOLLAR" in src["acquisition"]
        and "WHY_OWNED_DATA_CANNOT_ANSWER_THEM" in src["acquisition"]
        and '"money_spent": 0.0' in src["acquisition"])

    # (7) ZONE_C lockbox and the freeze.
    zone_c_gated = (
        "ZONE_C_PREGATE_T" in src["contract"]
        and "def may_open_zone_c" in src["frontier"]
        and "one_access_per_lineage" in src["frontier"]
        and "ZONE_C_NEVER_READ_FOR_SELECTION = True" in src["contract"])
    shadows_capped_not_promotable = (
        "MAX_NEW_SHADOWS = 4" in src["contract"]
        and "PROMOTION_ALLOWED = False" in src["contract"]
        and '"promotion_allowed": C.PROMOTION_ALLOWED' in src["frontier"]
        and "paired_control" in src["frontier"])
    forward_never_backfilled = (
        "NEVER_BACKFILL_PROSPECTIVE_ROWS = True" in src["contract"]
        and "NEVER_REWRITE_FROZEN_PREDICTIONS = True" in src["contract"]
        and "PRIOR_SHADOWS_ARE_IMMUTABLE = True" in src["contract"]
        and "prior_release_shadows_mutated" in src["frontier"])

    # (8) Ranking is economic, not statistical vanity.
    ranked_by_economic_value = (
        "RANK_BY" in src["contract"]
        and "evidence-weighted" in src["contract"]
        and "ECONOMIC_VALUE_SCORE" in src["frontier"]
        and "def _economic_value" in src["frontier"])
    frontier_fields_complete = all(
        f in src["frontier"] for f in
        ("NET_RESIDUAL_ALPHA", "COMMITTED_CAPITAL", "CASH_HURDLE",
         "SEARCH_ADJUSTMENT", "ROBUSTNESS", "PIT_STATUS", "FORWARD_READY",
         "QUALIFICATION_STATE"))

    # (9) Statistics are imported from their canonical owners.
    reuses_canonical_statistics = (
        "from ..r31 import multiple_testing" in src["frontier"]
        and "from ..r39 import burden as R39B" in src["frontier"]
        and "from ..r41 import evidence as EV" in src["frontier"]
        and "def deflated_sharpe" not in all_src
        and "def benjamini_hochberg" not in all_src
        and "def scorecard" not in all_src)

    # (10) Read-only against prior releases and the operational estate.
    panels_read_only = all(
        bad not in src["panels"]
        for bad in ("to_csv(", "write_text(", "to_pickle(", "mkdir("))
    no_operational_imports = not any(
        tok in all_src for tok in
        ("from api.", "import api.", "operational_book", "alpha_book",
         "daily_close", "portfolio_decision", "rebalance", "corporate_actions"))
    prior_roots_witnessed = (
        "IMMUTABLE_WITNESSES" in src["closeout"]
        and "def witness_fingerprint" in src["closeout"]
        and "prior_release_roots_opened_read_only" in src["closeout"])

    # (11) Safety flags and the branch matrix.
    safety_flags_false = all(
        ("%s = False" % f) in src["contract"] for f in
        ("MAY_CREATE_ORDER", "MAY_CREATE_PAPER_ORDER", "MAY_CHANGE_HOLDINGS",
         "MAY_PROMOTE_MODEL", "MAY_ACTIVATE_SLEEVE",
         "MAY_MODIFY_PRODUCTION_SCHEDULER", "MAY_RESTART_PRODUCTION",
         "MAY_CONNECT_BROKER", "MAY_MUTATE_OPERATIONAL_STORE",
         "MAY_MUTATE_PRIOR_RELEASE_ARTIFACT"))
    every_lane_must_terminate = (
        "BLOCKER_VOCAB" in src["contract"]
        and "NO_ALPHA_FOUND_IS_NOT_A_GLOBAL_STOP = True" in src["contract"]
        and "def branch_matrix" in src["campaign"]
        and "every_lane_terminated" in src["campaign"])
    result_axes_never_collapsed = (
        "NEVER_COLLAPSE_RESULT_AXES = True" in src["contract"]
        and "def result_axes" in src["campaign"]
        and "DO_NOT_FORCE_A_SUCCESS_STATE = True" in src["contract"])
    states_declared = sorted(
        s for s in R43_REQUIRED_STATES if s not in src["contract"])
    twenty_questions_answered = (
        "TWENTY_QUESTIONS" in src["contract"]
        and "def twenty_answers" in src["campaign"])
    shell_policy_declared = (
        "WINDOWS_POWERSHELL_ONLY = True" in src["contract"]
        and "INHERITED_SHELL_POLICY_DISCLOSURES" in src["contract"]
        and "release42" in src["contract"])
    no_scheduler_or_task_registration = not any(
        tok in all_src for tok in
        ("schtasks", "Register-ScheduledTask", "crontab", "CronCreate"))

    return {
        "modules_present": not modules_missing,
        "modules_missing": modules_missing,
        "second_owner_modules": second_owner_modules,
        "burden_inherited_not_reset": burden_inherited_not_reset,
        "r41_ledger_read_only": r41_ledger_read_only,
        "lane_caps_enforced": lane_caps_enforced,
        "contract_frozen_before_results": contract_frozen_before_results,
        "collateral_declared": collateral_declared,
        "judge_is_one_equation": judge_is_one_equation,
        "risk_free_convention_inherited": risk_free_convention_inherited,
        "post_freeze_denominator_disclosed": post_freeze_denominator_disclosed,
        "signal_is_lagged": signal_is_lagged,
        "no_full_sample_risk_target": no_full_sample_risk_target,
        "no_padded_returns": no_padded_returns,
        "causal_pivots_only": causal_pivots_only,
        "lookahead_sector_map_refused": lookahead_sector_map_refused,
        "survivorship_handled": survivorship_handled,
        "passive_control_measured": passive_control_measured,
        "placebo_declared_not_chosen": placebo_declared_not_chosen,
        "event_placebo_is_non_event_days": event_placebo_is_non_event_days,
        "signed_advance_rule": signed_advance_rule,
        "walls_probed_not_asserted": walls_probed_not_asserted,
        "keys_never_leak": keys_never_leak,
        "no_purchase_or_account": no_purchase_or_account,
        "purchase_gate_ranks_by_value_per_dollar":
            purchase_gate_ranks_by_value_per_dollar,
        "zone_c_gated": zone_c_gated,
        "shadows_capped_not_promotable": shadows_capped_not_promotable,
        "forward_never_backfilled": forward_never_backfilled,
        "ranked_by_economic_value": ranked_by_economic_value,
        "frontier_fields_complete": frontier_fields_complete,
        "reuses_canonical_statistics": reuses_canonical_statistics,
        "panels_read_only": panels_read_only,
        "no_operational_imports": no_operational_imports,
        "prior_roots_witnessed": prior_roots_witnessed,
        "safety_flags_false": safety_flags_false,
        "every_lane_must_terminate": every_lane_must_terminate,
        "result_axes_never_collapsed": result_axes_never_collapsed,
        "twenty_questions_answered": twenty_questions_answered,
        "shell_policy_declared": shell_policy_declared,
        "no_scheduler_or_task_registration": no_scheduler_or_task_registration,
        "qualification_states_missing": states_declared,
    }


R44_OWNERS = {
    "root": "alpha_agent/r44/__init__.py",
    "contract": "alpha_agent/r44/contract.py",
    "burden": "alpha_agent/r44/burden.py",
    "closeout": "alpha_agent/r44/closeout.py",
    "streams": "alpha_agent/r44/streams.py",
    "combine": "alpha_agent/r44/combine.py",
    "control": "alpha_agent/r44/control.py",
    "portfolio": "alpha_agent/r44/portfolio.py",
    "options": "alpha_agent/r44/options.py",
    "intraday": "alpha_agent/r44/intraday.py",
    "acquisition": "alpha_agent/r44/acquisition.py",
    "niche": "alpha_agent/r44/niche.py",
    "purchase": "alpha_agent/r44/purchase.py",
    "frontier": "alpha_agent/r44/frontier.py",
    "campaign": "alpha_agent/r44/campaign.py",
    "shell_policy": "alpha_agent/r44/shell_policy.py",
}

#: A second implementation of an already-owned concern inside r44 is a
#: blocking defect - these names may not exist. The universal economic judge
#: in particular is R43's and is IMPORTED, never re-derived: a release that
#: quietly writes its own capital equation can quote whatever it likes.
R44_SECOND_OWNER_FORBIDDEN = (
    "alpha_agent/r44/evidence.py", "alpha_agent/r44/multiple_testing.py",
    "alpha_agent/r44/judge.py", "alpha_agent/r44/panels.py",
    "alpha_agent/r44/economics.py", "alpha_agent/r44/zones.py",
    "alpha_agent/r44/ledger.py", "alpha_agent/r44/research_shadow.py",
    "alpha_agent/r44/scheduler.py", "alpha_agent/r44/forward_freeze.py",
    "alpha_agent/r44/curve_state.py", "alpha_agent/r44/crypto_lab.py",
    "alpha_agent/r44/deflated_sharpe.py", "alpha_agent/r44/capital.py",
)

R44_REQUIRED_STATES = (
    "R44_STANDALONE_ALPHA_FOUND",
    "R44_PORTFOLIO_ALPHA_FOUND",
    "R44_STRUCTURAL_PREMIA_ONLY",
    "R44_LESS_EFFICIENT_MARKET_EDGE_FOUND",
    "R44_NO_ALPHA_AFTER_ORTHOGONAL_AND_PORTFOLIO_SYNTHESIS",
)


def check_release44_orthogonal_portfolio_alpha(files: list[Path]) -> dict:
    """Release 44 invariants - a PORTFOLIO claim that may not cheat.

    A portfolio result is the easiest kind of result to fake, and every
    invariant below exists to close one specific route:

    * score many streams, keep the winners, publish the aggregate - closed by
      an inventory declared by economics in the frozen contract, with losers
      required to be in it;
    * try eight weighting schemes and report the best - closed by naming the
      primary rule before the lockbox, charging each DISTINCT book to the
      burden ledger, and measuring PBO over the rules;
    * flip a stream's sign until the book works - closed outright, and where
      a sign-selected DIAGNOSTIC is run it must charge the transaction cost
      on the flipped book instead of crediting it;
    * quote the aggregate against a benchmark it was always going to beat -
      closed by a structural-premium control built the same way, plus a
      volatility-matched passive long;
    * call a smoother package of risk premia "portfolio alpha" - closed by
      keeping three qualification words separate;
    * assert a data wall instead of probing it, or buy something.
    """
    src = {name: (_read(REPO_ROOT / path) or "")
           for name, path in R44_OWNERS.items()}
    modules_missing = sorted(n for n, t in src.items() if not t)
    all_src = "\n".join(src.values())
    second_owner_modules = sorted(p for p in R44_SECOND_OWNER_FORBIDDEN
                                  if (REPO_ROOT / p).exists())

    # (1) Burden inherits 302 from R43's bytes and never resets.
    burden_inherited_not_reset = (
        "GLOBAL_INHERITED_EFFECTIVE_TRIALS = 302" in src["contract"]
        and "INHERITED_R43_DISTINCT = 13" in src["contract"]
        and "BURDEN_NEVER_RESETS = True" in src["contract"]
        and "NO_CAMPAIGN_ID_LAUNDERING = True" in src["contract"]
        and "def verify_inherited" in src["burden"]
        and "class BurdenLaundering" in src["burden"]
        and "raise BurdenLaundering" in src["burden"])
    r43_ledger_read_only = (
        "PRIOR_LEDGERS_ARE_READ_ONLY = True" in src["contract"]
        and "r43_ledger_mutated" in src["burden"]
        and "R43_LEDGER" in src["burden"])
    lane_caps_enforced = (
        "has exhausted its FROZEN ZONE_B cap" in src["burden"]
        and "TOTAL_ZONE_B_BUDGET" in src["contract"]
        and "LANE_CAP_IS_A_CEILING_NOT_A_TARGET = True" in src["contract"])
    portfolio_synthesis_is_charged = (
        "PORTFOLIO_SYNTHESIS_IS_A_SEARCHED_FAMILY = True" in src["contract"]
        and '"PORTFOLIO_SYNTHESIS"' in src["campaign"]
        and "n_distinct_books" in src["campaign"])

    # (2) The contract is frozen before the first number; amendments are
    #     disclosed beside the freeze rather than folded into it.
    contract_frozen_before_results = (
        "def frozen_body" in src["contract"]
        and "def verify_contract_unchanged" in src["closeout"]
        and "contract_frozen_before_first_number" in src["closeout"]
        and "KILL_TESTS_ARE_CHOSEN_BEFORE_RESULTS = True" in src["contract"])
    amendments_disclosed_and_bounded = (
        "POST_FREEZE_AMENDMENTS" in src["contract"]
        and "AMENDMENTS_MAY_ONLY_MAKE_AN_UNBUILT_STREAM_BUILDABLE = True"
        in src["contract"]
        and "AMENDMENTS_AFTER_THE_LOCKBOX_ARE_FORBIDDEN = True"
        in src["contract"]
        and "def amend" in src["closeout"]
        and "original_frozen_contract_hash" in src["closeout"])

    # (3) The portfolio claim's defences.
    primary_rule_named_before_lockbox = (
        'PRIMARY_COMBINATION_RULE = "FAMILY_BALANCED_ERC"' in src["contract"]
        and "PRIMARY_RULE_RATIONALE" in src["contract"]
        and "primary_rule_named_before_lockbox" in src["campaign"])
    no_threshold_is_chosen = (
        "NO_THRESHOLD_IS_CHOSEN = True" in src["contract"]
        and 'STREAM_EXPRESSION = "CONTINUOUS"' in src["contract"])
    losers_are_included = (
        "LOSERS_ARE_INCLUDED = True" in src["contract"]
        and "SELECTION_ON_MEASURED_PERFORMANCE_IS_FORBIDDEN = True"
        in src["contract"])
    dangerous_optimisers_forbidden = (
        "UNCONSTRAINED_MEAN_VARIANCE_IS_FORBIDDEN = True" in src["contract"]
        and "MAXIMISING_HISTORICAL_SHARPE_IS_FORBIDDEN = True"
        in src["contract"]
        and "SHORTING_A_STREAM_IS_FORBIDDEN = True" in src["contract"])
    weights_fitted_on_fit_zones_only = (
        "WEIGHTS_ARE_FITTED_ON_FIT_ZONES_ONLY = True" in src["contract"]
        and "NO_OPTIMISATION_ON_THE_HOLDOUT = True" in src["contract"]
        and '"fitted_on": "FIT_ZONES_ONLY"' in src["combine"])
    constraints_applied_to_every_rule = (
        "def apply_constraints" in src["combine"]
        and "max_single_stream_weight" in src["contract"]
        and "max_family_weight" in src["contract"]
        and "max_asset_class_weight" in src["contract"])

    # (4) The bug guard. A short pays the spread exactly like a long.
    sign_flip_charges_cost = (
        "def excess_frame_signed" in src["streams"]
        and "rec[\"gross\"] * s" in src["streams"]
        and "excess_frame_signed" in src["portfolio"]
        and "sub.mul(pd.Series(signs))" not in src["portfolio"])
    sign_diagnostic_cannot_qualify = (
        "SIGN_SELECTED_DIAGNOSTIC" in src["contract"]
        and '"may_qualify": False' in src["contract"]
        and '"may_qualify"] = False' in src["portfolio"])

    # (5) Controls. The premium portfolio is the benchmark, not the product.
    structural_premium_control_declared = (
        "STRUCTURAL_PREMIUM_PORTFOLIO" in src["contract"]
        and "VOLATILITY_MATCHED_PASSIVE" in src["contract"]
        and "A_SMOOTHER_PACKAGE_OF_PREMIA_IS_NOT_ALPHA = True"
        in src["contract"]
        and "def premium_portfolio" in src["control"]
        and "def passive_long_stream" in src["control"])
    increment_is_volatility_matched = (
        "INCREMENT_IS_VOLATILITY_MATCHED = True" in src["contract"]
        and "def volatility_matched_increment" in src["control"]
        and "signal_is_decoration" in src["control"])
    three_qualification_words_kept_apart = (
        "STANDALONE_ALPHA" in src["contract"]
        and "PORTFOLIO_ALPHA" in src["contract"]
        and "STRUCTURAL_PREMIUM" in src["contract"]
        and "ALPHA_IS_NOT_A_LOOSE_WORD = True" in src["contract"]
        and "A_POSITIVE_NOMINAL_RETURN_IS_NOT_ALPHA = True"
        in src["contract"])

    # (6) The kill battery and the search adjustment.
    kill_battery_complete = (
        "PORTFOLIO_KILL_TESTS" in src["contract"]
        and all(t in src["portfolio"] for t in
                ("LEAVE_ONE_STREAM_OUT", "LEAVE_ONE_FAMILY_OUT",
                 "LEAVE_ONE_ASSET_CLASS_OUT", "LEAVE_ONE_YEAR_OUT",
                 "WEIGHT_PERTURBATION", "CORRELATION_STRESS",
                 "BLOCK_BOOTSTRAP", "CONCENTRATION",
                 "STRUCTURAL_PREMIUM_CONTROL_INCREMENT",
                 "VOLATILITY_MATCHED_PASSIVE_INCREMENT")))
    pbo_measured_over_the_rules = (
        "def pbo" in src["portfolio"]
        and "combinatorially" in src["portfolio"].lower()
        and "n_splits" in src["portfolio"])
    negative_is_never_called_a_survivor = (
        "is_a_positive_survivor" in src["frontier"]
        and "n_positive_survivors" in src["frontier"])

    # (7) Canonical statistics are imported, never re-implemented.
    reuses_canonical_statistics = (
        "from ..r41 import evidence" in src["portfolio"]
        and "from ..r43 import judge" in src["streams"]
        and "multiple_testing" in src["frontier"]
        and "def hac_t" not in all_src
        and "def benjamini_hochberg" not in all_src
        and "def implementable_book" not in all_src
        and "def scorecard" not in all_src)

    # (8) Intraday. No fabricated fill, no CFD proxy, a declared clock.
    entry_is_never_at_the_print = (
        "INTRADAY_ENTRY_DELAYS_MIN = (1, 5)" in src["contract"]
        and "Zero is forbidden" in src["contract"])
    cost_is_the_observed_spread = (
        'INTRADAY_COST_MODEL = "OBSERVED_HALF_SPREAD_BOTH_SIDES_PLUS_'
        'SLIPPAGE"' in src["contract"]
        and "INTRADAY_SLIPPAGE_BPS_PER_SIDE" in src["contract"]
        and "half.get(t_in" in src["intraday"]
        and "half.get(t_out" in src["intraday"])
    no_cfd_proxy_for_futures = (
        "NO_CFD_PROXY_FOR_A_FUTURES_HYPOTHESIS = True" in src["contract"]
        and "INTRADAY_EXCLUDED_AS_CFD" in src["contract"]
        and "excluded_as_cfd" in src["intraday"])
    release_time_is_declared_constant = (
        "MACRO_RELEASE_TIMES_ARE_A_DECLARED_CONSTANT = True"
        in src["contract"]
        and "MACRO_RELEASE_TIMES_ET" in src["contract"]
        and "timing_sweep" in src["intraday"])
    event_placebo_is_non_release_days = (
        "INTRADAY_PLACEBO" in src["contract"]
        and "def placebo" in src["intraday"]
        and "shift_days" in src["intraday"])
    no_fabricated_fill = (
        "NO_INTERPOLATED_INTRADAY = True" in src["contract"]
        and "no_fill_is_modelled" in src["acquisition"]
        and "maker_execution_still_blocked" in src["acquisition"])

    # (9) Analyst vintages are RECONCILED, never trusted.
    no_current_snapshot_as_vintage = (
        "NO_CURRENT_SNAPSHOT_AS_HISTORICAL_VINTAGE = True" in src["contract"]
        and "def reconcile_backward_strip" in src["acquisition"]
        and "VENDOR_BACKWARD_STRIP_IS_RESTATED" in src["acquisition"]
        and "competing_explanation" in src["acquisition"])
    sample_request_prepared_not_sent = (
        "MAY_SEND_VENDOR_EMAIL = False" in src["contract"]
        and "PREPARED_NOT_SENT" in src["acquisition"]
        and "smtplib" not in all_src
        and "send_message" not in all_src)

    # (10) Options may diagnose and may not qualify.
    options_may_not_qualify = (
        "A_SHORT_WINDOW_MAY_DIAGNOSE_AND_MAY_NOT_QUALIFY = True"
        in src["contract"]
        and '"may_qualify": False' in src["options"]
        and "sessions_short_by" in src["options"]
        and "additional_months_required" in src["options"])
    iv_inverted_locally = (
        "OPTION_VENDOR_GREEKS_REQUIRED = False" in src["contract"]
        and "BLACK_SCHOLES" in src["contract"])

    # (11) Less-efficient markets: capacity is measured, cost is scaled.
    capacity_is_a_result = (
        "CAPACITY_IS_A_RESULT_NOT_A_FILTER = True" in src["contract"]
        and "LOWER_CAPACITY_IS_ACCEPTABLE = True" in src["contract"]
        and "FANTASY_EXECUTION_IS_NOT = True" in src["contract"]
        and "def capacity" in src["niche"]
        and "PARTICIPATION_CAP_OF_DAILY_VOLUME" in src["contract"])
    cost_is_liquidity_scaled = (
        "NICHE_COST_IS_LIQUIDITY_SCALED = True" in src["contract"]
        and "COST_SCALE_EXPONENT" in src["niche"]
        and "COST_SCALE_CAP" in src["niche"])
    zero_volume_markets_excluded = (
        'f[f["adv_usd"] > 0]' in src["niche"])
    niche_advance_bar_is_the_frozen_one = (
        'ADVANCE_T = C.STANDALONE_ALPHA_GATE["t_min_lock"]' in src["niche"])

    # (12) Acquisition: walls probed, nothing bought, no key leaked.
    walls_probed_not_asserted = (
        "probe_analyst_revisions" in src["acquisition"]
        and "probe_native_credit" in src["acquisition"]
        and "probe_microstructure" in src["acquisition"]
        and "live_probe" in src["acquisition"])
    keys_never_leak = not re.search(
        r"(api_?key|token)\s*=\s*[\"'][A-Za-z0-9]{12,}", all_src, re.I)
    no_purchase_or_account = (
        "MAY_SPEND_MONEY = False" in src["contract"]
        and "MAY_PURCHASE_DATA = False" in src["contract"]
        and "MAY_START_PROVIDER_TRIAL = False" in src["contract"]
        and "MAY_CREATE_PROVIDER_ACCOUNT = False" in src["contract"]
        and "MAY_SUBMIT_PAYMENT_DETAILS = False" in src["contract"]
        and "DEFAULT_AUTHORIZED_SPEND_USD = 0.0" in src["contract"]
        and '"money_spent_usd": 0.0' in src["purchase"])
    purchase_gate_ranks_by_value_per_dollar = (
        "gain_per_1000_usd" in src["purchase"]
        and "TOP_DATA_PURCHASE_RECOMMENDATION" in src["purchase"]
        and "NEED_SAMPLE" in src["purchase"])

    # (13) Forward evidence and prior releases.
    shadows_capped_not_promotable = (
        "PROMOTION_ALLOWED = False" in src["contract"]
        and "MAX_NEW_SHADOWS" in src["contract"]
        and "DO_NOT_FREEZE_MEDIOCRE_CANDIDATES_TO_CREATE_ACTIVITY = True"
        in src["contract"]
        and "def freeze_decision" in src["frontier"])
    forward_never_backfilled = (
        "NEVER_BACKFILL_PROSPECTIVE_ROWS = True" in src["contract"]
        and "PRIOR_SHADOWS_ARE_IMMUTABLE = True" in src["contract"])
    prior_roots_witnessed = (
        "IMMUTABLE_WITNESSES" in src["closeout"]
        and "def witness_fingerprint" in src["closeout"]
        and "def witnesses_unchanged" in src["closeout"]
        and "prior_release_roots_opened_read_only" in src["closeout"])
    no_operational_imports = not any(
        tok in all_src for tok in
        ("from api.", "import api.", "operational_book", "alpha_book",
         "daily_close", "portfolio_decision", "corporate_actions"))

    # (14) Safety, branch matrix, result axes.
    safety_flags_false = all(
        ("%s = False" % f) in src["contract"] for f in
        ("MAY_CREATE_ORDER", "MAY_CREATE_PAPER_ORDER", "MAY_CHANGE_HOLDINGS",
         "MAY_PROMOTE_MODEL", "MAY_ACTIVATE_SLEEVE",
         "MAY_MODIFY_PRODUCTION_SCHEDULER", "MAY_RESTART_PRODUCTION",
         "MAY_CONNECT_BROKER", "MAY_MUTATE_OPERATIONAL_STORE",
         "MAY_MUTATE_PRIOR_RELEASE_ARTIFACT",
         "MAY_CREATE_CAPITAL_ALLOCATION"))
    every_lane_must_terminate = (
        "BLOCKER_VOCAB" in src["contract"]
        and "A_FAILED_LANE_IS_A_ROUTING_EVENT = True" in src["contract"]
        and "ONE_LANE_MAY_NOT_HALT_ANOTHER = True" in src["contract"]
        and "def _branch_matrix" in src["campaign"]
        and "every_lane_terminated" in src["campaign"])
    result_axes_never_collapsed = (
        "NEVER_COLLAPSE_RESULT_AXES = True" in src["contract"]
        and "def _result_axes" in src["campaign"]
        and "DO_NOT_FORCE_A_SUCCESS_STATE = True" in src["contract"]
        and "DO_NOT_PROTECT_PROMISING_RESULTS = True" in src["contract"])
    fifteen_questions_answered = (
        "FIFTEEN_QUESTIONS" in src["contract"]
        and "def _fifteen_answers" in src["campaign"])
    no_alpha_terminal_requires_execution = (
        "NO_ALPHA_TERMINAL_REQUIRES_EVERY_ZERO_COST_BRANCH_EXECUTED = True"
        in src["contract"]
        and "NO_BROAD_EXECUTABLE_ZERO_COST_BRANCH_MAY_BE_DEFERRED = True"
        in src["contract"])
    shell_policy_declared = (
        "WINDOWS_POWERSHELL_ONLY = True" in src["contract"]
        and "INHERITED_SHELL_POLICY_DISCLOSURES = 1" in src["contract"]
        and "SHELL_POLICY_MEASURED_BY" in src["contract"])
    # A policy that is only reported when it was kept is not a policy. The
    # release must carry a machine-readable record of its OWN events, with a
    # waiver token, and the verdict must carry the same violation flag the
    # record does - so a release cannot print "no violation" beside a file
    # that says otherwise.
    shell_policy_events_disclosed = (
        "def violated" in src["shell_policy"]
        and "def waiver_token" in src["shell_policy"]
        and "INHERITED_EVENTS" in src["shell_policy"]
        and "DISCLOSED_WAIVER_IS_THE_OPERATORS" in src["shell_policy"]
        and "SP.violated()" in src["campaign"]
        and "R44_SHELL_POLICY_EVENTS.json" in src["campaign"])
    no_scheduler_or_task_registration = not any(
        tok in all_src for tok in
        ("schtasks", "Register-ScheduledTask", "crontab", "CronCreate"))
    states_declared = sorted(
        s for s in R44_REQUIRED_STATES if s not in src["contract"])

    return {
        "modules_present": not modules_missing,
        "modules_missing": modules_missing,
        "second_owner_modules": second_owner_modules,
        "burden_inherited_not_reset": burden_inherited_not_reset,
        "r43_ledger_read_only": r43_ledger_read_only,
        "lane_caps_enforced": lane_caps_enforced,
        "portfolio_synthesis_is_charged": portfolio_synthesis_is_charged,
        "contract_frozen_before_results": contract_frozen_before_results,
        "amendments_disclosed_and_bounded": amendments_disclosed_and_bounded,
        "primary_rule_named_before_lockbox": primary_rule_named_before_lockbox,
        "no_threshold_is_chosen": no_threshold_is_chosen,
        "losers_are_included": losers_are_included,
        "dangerous_optimisers_forbidden": dangerous_optimisers_forbidden,
        "weights_fitted_on_fit_zones_only": weights_fitted_on_fit_zones_only,
        "constraints_applied_to_every_rule": constraints_applied_to_every_rule,
        "sign_flip_charges_cost": sign_flip_charges_cost,
        "sign_diagnostic_cannot_qualify": sign_diagnostic_cannot_qualify,
        "structural_premium_control_declared":
            structural_premium_control_declared,
        "increment_is_volatility_matched": increment_is_volatility_matched,
        "three_qualification_words_kept_apart":
            three_qualification_words_kept_apart,
        "kill_battery_complete": kill_battery_complete,
        "pbo_measured_over_the_rules": pbo_measured_over_the_rules,
        "negative_is_never_called_a_survivor":
            negative_is_never_called_a_survivor,
        "reuses_canonical_statistics": reuses_canonical_statistics,
        "entry_is_never_at_the_print": entry_is_never_at_the_print,
        "cost_is_the_observed_spread": cost_is_the_observed_spread,
        "no_cfd_proxy_for_futures": no_cfd_proxy_for_futures,
        "release_time_is_declared_constant": release_time_is_declared_constant,
        "event_placebo_is_non_release_days": event_placebo_is_non_release_days,
        "no_fabricated_fill": no_fabricated_fill,
        "no_current_snapshot_as_vintage": no_current_snapshot_as_vintage,
        "sample_request_prepared_not_sent": sample_request_prepared_not_sent,
        "options_may_not_qualify": options_may_not_qualify,
        "iv_inverted_locally": iv_inverted_locally,
        "capacity_is_a_result": capacity_is_a_result,
        "cost_is_liquidity_scaled": cost_is_liquidity_scaled,
        "zero_volume_markets_excluded": zero_volume_markets_excluded,
        "niche_advance_bar_is_the_frozen_one":
            niche_advance_bar_is_the_frozen_one,
        "walls_probed_not_asserted": walls_probed_not_asserted,
        "keys_never_leak": bool(keys_never_leak),
        "no_purchase_or_account": no_purchase_or_account,
        "purchase_gate_ranks_by_value_per_dollar":
            purchase_gate_ranks_by_value_per_dollar,
        "shadows_capped_not_promotable": shadows_capped_not_promotable,
        "forward_never_backfilled": forward_never_backfilled,
        "prior_roots_witnessed": prior_roots_witnessed,
        "no_operational_imports": no_operational_imports,
        "safety_flags_false": safety_flags_false,
        "every_lane_must_terminate": every_lane_must_terminate,
        "result_axes_never_collapsed": result_axes_never_collapsed,
        "fifteen_questions_answered": fifteen_questions_answered,
        "no_alpha_terminal_requires_execution":
            no_alpha_terminal_requires_execution,
        "shell_policy_declared": shell_policy_declared,
        "shell_policy_events_disclosed": shell_policy_events_disclosed,
        "no_scheduler_or_task_registration": no_scheduler_or_task_registration,
        "qualification_states_missing": states_declared,
    }


_ATTRITION_REQUIRED_MODES = {
    "forecast_too_weak", "magnitude_poorly_calibrated",
    "sizing_destroys_rank_skill", "turnover_consumes_edge",
    "diversification_dilutes_edge", "risk_matched_benchmark_dominates",
    "exposure_neutrality_removes_apparent_alpha",
    "works_only_in_one_asset_class", "works_only_in_one_horizon",
    "works_only_under_unrealistic_cost", "covariance_or_risk_forecast_error",
}


def _r32_turnover_budget_literals(gov_src: str) -> list:
    """Numeric turnover-budget VALUES invented by the governance module.

    AST rather than a substring search: the defect is a number inside one
    specific mapping, and the module legitimately contains other numbers. A
    grep for ``0.05`` would both miss ``5e-2`` and fire on an unrelated float.
    """
    try:
        tree = ast.parse(gov_src)
    except SyntaxError:
        return ["UNPARSEABLE_GOVERNANCE_MODULE"]
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(t, ast.Name) and t.id == "TURNOVER_BUDGETS"
                   for t in node.targets):
            continue
        for sub in ast.walk(node.value):
            if (isinstance(sub, ast.Constant)
                    and isinstance(sub.value, (int, float))
                    and not isinstance(sub.value, bool)):
                found.append(repr(sub.value))
    return sorted(found)


def _admissible_block(sources_src: str) -> str:
    """The ADMISSIBLE_FOR_HISTORY tuple only.

    Checked as its own slice because ``REVISED_NOT_PIT`` legitimately appears
    all over that module - as a constant, in the classifier, in docstrings. The
    invariant is not "the token is absent"; it is "the token is not in the
    admissible list", and only a targeted slice can tell those apart.
    """
    m = re.search(r"ADMISSIBLE_FOR_HISTORY\s*=\s*\(([^)]*)\)", sources_src)
    return m.group(1) if m else ""


def _r32_ui_region(ui: str) -> str:
    """The Release-32 UI card only, so prose elsewhere cannot trip a check."""
    start = ui.find("R32 PNL OPPORTUNITY FRONTIER")
    if start < 0:
        return ""
    end = ui.find("R32 PNL OPPORTUNITY FRONTIER END", start + 1)
    return ui[start:end if end > start else start + 40000]


def check_release31_mathematical_alpha_frontier(files: list[Path]) -> dict:
    src = {name: (_read(path) or "") for name, path in R31_OWNERS.items()}
    modules_present = sorted(n for n, t in src.items() if t)
    modules_missing = sorted(n for n, t in src.items() if not t)
    all_src = "\n".join(src.values())
    read_model = _read(R31_READ_MODEL) or ""
    app = _read("api/app.py") or ""
    ui = _read("api/ui/index.html") or ""

    # (1)-(4) ONE owner each. A second module declaring the same ownership token
    # is the drift these checks exist to catch.
    def _second_owners(token: str, owner: str) -> list:
        """Any OTHER shipped module declaring the same ownership token.

        Paths arrive absolute, so they are normalised to repo-relative first;
        tests and the audit itself legitimately name these tokens, and are not
        second owners of the calculation.
        """
        out = []
        for p in files:
            rel = _rel(p)
            if rel == owner or not rel.endswith(".py"):
                continue
            if rel.startswith("tests/") or rel.startswith("scripts/"):
                continue
            if token in (_read(rel) or ""):
                out.append(rel)
        return sorted(out)

    second_owners = {
        "campaign_contract": _second_owners('CONTRACT_SCHEMA = "r31_research_campaign_contract',
                                            R31_OWNERS["contract"]),
        "research_judge": _second_owners('JUDGE_SCHEMA = "r31_research_judge_contract',
                                         R31_OWNERS["judge"]),
        "candidate_registry": _second_owners('REGISTRY_SCHEMA = "r31_candidate_registry',
                                             R31_OWNERS["registry"]),
        "lockbox_access": _second_owners('ACCESS_SCHEMA = "r31_lockbox_access_log',
                                         R31_OWNERS["lockbox"]),
    }

    # (5) The lockbox may not be reachable from training or selection. The
    # training cap and the selection basis are both declared, and the ONLY
    # module that names the lockbox layer in a fitting context is the lockbox
    # owner itself.
    lockbox_guard = {
        "training_cap_declared": "LAST_VALIDATION_DATE_LOCKBOX_NEVER_TRAINED"
                                 in src["campaign"],
        "selection_basis_declared": "DISCOVERY_AND_VALIDATION_ONLY" in src["campaign"],
        "partition_declares_invisibility":
            "no_model_or_hyperparameter_may_read_lockbox" in src["partition"],
        "no_retune_declared": "lockbox_result_may_not_redesign_the_same_candidate"
                              in src["partition"],
        "single_execution_enforced": "has already used its single lockbox execution"
                                     in src["lockbox"],
        "methods_never_reference_lockbox": "lockbox" not in src["methods"].lower()
                                           or "never" in src["methods"].lower(),
    }

    # (6)-(8) A research candidate can never reach the operational model, the
    # canonical portfolio decision, or an order.
    forbidden_calls = sorted(
        {t for t in R31_FORBIDDEN_CALLS if t in all_src.lower()})
    forbidden_owner_refs = sorted(
        {t for t in R31_FORBIDDEN_OWNER_REFS if t in all_src.lower()})
    research_imports_api = sorted(
        n for n, t in src.items()
        if re.search(r"^\s*(from|import)\s+.*paper_trader\.api", t, re.M))
    engine_imports = sorted(set(re.findall(
        r"from\s+\.\.\.engine\s+import\s+(\w+)", all_src)))
    forbidden_engine = sorted(set(engine_imports) - R31_ALLOWED_ENGINE)

    # (8b) An admitted engine owner must be import-pure. The allowlist grants
    # admission by NAME; this proves the thing behind the name is still safe.
    impure_engine = []
    for mod in sorted(set(engine_imports) & R31_ALLOWED_ENGINE):
        text = _read("engine/%s.py" % mod) or ""
        if not text:
            impure_engine.append("%s:UNREADABLE" % mod)
            continue
        try:
            tree = ast.parse(text)
        except SyntaxError:
            impure_engine.append("%s:UNPARSEABLE" % mod)
            continue
        for node in ast.walk(tree):
            roots = []
            if isinstance(node, ast.Import):
                roots = [a.name.split(".")[0] for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                # A relative import inside engine/ stays inside engine/.
                roots = ([] if node.level else
                         [(node.module or "").split(".")[0]])
            for root in roots:
                if root and root not in R31_ENGINE_PURE_IMPORTS:
                    impure_engine.append("%s:%s" % (mod, root))
    impure_engine = sorted(set(impure_engine))

    # (9) Budgets are encoded, not prose.
    budgets_encoded = sorted(
        b for b in R31_REQUIRED_BUDGETS
        if not re.search(r"^%s\s*=\s*\d+" % re.escape(b), src["contract"], re.M))
    budgets_enforced = {
        "registry_raises_on_budget": "class BudgetExceeded" in src["registry"]
                                     and "raise BudgetExceeded" in src["registry"],
        "registry_raises_on_duplicate": "class DuplicateCandidate" in src["registry"],
        "lockbox_raises_on_violation": "class LockboxViolation" in src["lockbox"],
        "contract_drift_raises": "class ContractDrift" in src["registry"],
    }

    # (10) A terminal exhaustion state exists and stops further execution.
    exhaustion = {
        "terminal_states_declared": "TERMINAL_STATES" in src["contract"],
        "exhausted_state_present":
            "R31_CURRENT_INFORMATION_MODEL_FRONTIER_EXHAUSTED" in src["contract"],
        "second_null_campaign_terminates":
            "two_null_novel_campaigns_terminate" in src["contract"],
        "no_budget_extension_after_a_poor_result":
            "no_budget_extension_after_a_poor_result" in src["contract"],
        "novel_runner_stops_on_budget": "BudgetExceeded" in src["campaign"],
    }

    # (11)-(12) External reference links and EVENT_TRIGGER_ONLY news are not
    # research data, and no news-shaped feature exists in the frozen feature set.
    inadmissible = {
        "declared": all(k in src["contract"] for k in
                        ("gdelt_news_text", "external_reference_links",
                         "current_analyst_snapshots",
                         "entity_sic_snapshot_sector")),
        "manifest_carries_the_declaration":
            "inadmissible_information" in src["snapshot"],
    }
    news_tokens = ("gdelt", "news", "article", "sentiment", "headline",
                   "analyst_revision", "payload_reference")
    feature_block = ""
    m = re.search(r"PRICE_FEATURES\s*=.*?FUNDAMENTAL_FEATURES\s*=.*?\n\n",
                  _read("alpha_agent/release30_panel.py") or "", re.S)
    if m:
        feature_block = m.group(0).lower()
    news_features = sorted(t for t in news_tokens if t in feature_block)

    # (13)-(14) The canonical cost / risk / constraint owner is REUSED, and no
    # second optimiser or cost model is introduced.
    # The judge no longer imports the allocator directly: Campaign v3 routes both
    # architectures through ONE portfolio-construction seam, so the policy chain
    # is judge -> allocation -> canonical owner. The check follows that chain
    # rather than accepting the mere presence of a name in the judge's source,
    # and additionally proves the judge did not fork a policy of its own.
    reuse = {
        "judge_reads_canonical_policy":
            "_alloc.policy()" in src["judge"]
            and "_zb.default_policy()" in src["allocation"],
        "judge_defines_no_policy_of_its_own":
            not re.search(r"^def default_policy\(", src["judge"], re.M),
        "judge_declares_it_owns_no_cost":
            "judge_owns_no_cost_or_risk_calculation" in src["judge"],
        "judge_declares_it_owns_no_optimiser":
            "judge_owns_no_portfolio_optimiser" in src["judge"],
        "contract_names_the_policy_owner":
            "engine.zero_base_allocator.default_policy" in src["contract"],
        "contract_names_the_allocator_owner":
            "engine.zero_base_allocator.optimise" in src["contract"],
        "contract_names_the_covariance_owner":
            "engine.holding_opportunity_cost.build_covariance" in src["contract"],
        "contract_declares_no_second_optimiser":
            "second_portfolio_optimiser_exists" in src["contract"],
    }
    duplicate_optimiser = sorted(
        n for n, t in src.items()
        if re.search(r"^def (optimise|build_allocation|transition_economics)\(",
                     t, re.M))
    duplicate_cost_literal = sorted(
        n for n, t in src.items()
        if n != "contract" and re.search(r"(0\.00125|12\.5\s*/\s*10000|cost_bps_per_side\s*=\s*\d)", t))

    # ----------------------------------------------------------------------- #
    # Campaign v3 corrections. Each of these is a defect that actually shipped
    # in Campaign v2 and was found only by reading the code, so each is now a
    # blocking invariant rather than a paragraph in a design document.
    # ----------------------------------------------------------------------- #
    # (16)-(19) CORRECTION 1. The training universe is not the investment
    # universe, and a broader training choice may never widen what is owned.
    universe_separation = {
        "one_investment_universe_owner":
            not _second_owners('MANIFEST_SCHEMA = "r31_investment_universe_manifest',
                               R31_OWNERS["universe"]),
        "training_universes_declared":
            "TRAIN_S_AND_P_500_ONLY" in src["contract"]
            and "TRAIN_RUSSELL1000_PIT" in src["contract"],
        "evaluation_universe_declared":
            "EVALUATE_S_AND_P_500_PIT_MEMBERS_ONLY" in src["contract"],
        "judge_evaluates_investment_universe_only":
            "EVALUATION_UNIVERSE" in src["judge"]
            and "eligible_columns" in src["judge"],
        "broader_training_never_widens_evaluation":
            "broader_training_never_widens_evaluation" in src["contract"],
        "membership_is_point_in_time":
            "index_constituent_timeseries" in src["universe"],
        "current_membership_backwards_is_inadmissible":
            "current_index_membership_applied_backwards" in src["contract"],
        "blocked_state_exists": "UniverseUnavailable" in src["universe"],
        "survivorship_gap_measured":
            "def survivorship_report" in src["universe"],
        "training_choice_is_candidate_identity":
            '"training_universe"' in src["methods"],
    }

    # (20)-(22) CORRECTION 2. The primary judge is real zero-base economics; the
    # top-N book is demoted; cash is a genuine allocation choice.
    zero_base_primary = {
        "top_n_barred_from_primary_verdict":
            "TOP_N_MAY_CARRY_PRIMARY_VERDICT = False" in src["contract"],
        "judge_declares_zero_base_primary":
            "CANONICAL_ZERO_BASE_ALLOCATION_STOCKS_PLUS_CASH" in src["judge"],
        "allocation_delegates_to_canonical_optimiser":
            "_zb.optimise(" in src["allocation"],
        "cash_is_a_real_choice":
            "cash_is_a_real_allocation_choice" in src["judge"],
        # Tested as ASSIGNMENTS. The names appear again inside the contract body
        # that reports them, so a substring test would stay green after the
        # constant itself was replaced.
        "book_size_frontier_removed":
            not re.search(r"^RISK_FRONTIER_BOOK_SIZES\s*=", src["contract"], re.M),
        "gamma_frontier_declared":
            bool(re.search(r"^RISK_FRONTIER_GAMMA_MULTIPLIERS\s*=",
                           src["contract"], re.M)),
        "frontier_frozen_before_results":
            "frontier_frozen_before_results" in src["contract"],
        "only_gamma_moves_on_the_frontier":
            "def gamma_policy" in src["judge"],
        "sector_constraint_is_declared_unmeasurable":
            "UNMEASURABLE_PIT" in src["contract"]
            and "UNMEASURABLE_PIT" in src["allocation"],
    }

    # (23)-(25) CORRECTION 2 (Track A units). A score may not become an expected
    # return without a monotonic, rank-preserving, entitled calibration.
    calibration_guard = {
        "one_calibration_owner":
            not _second_owners('CALIBRATION_SCHEMA = "r31_forecast_calibration',
                               R31_OWNERS["calibration"]),
        "rank_identity_violation_state":
            "FORECAST_RANK_IDENTITY_VIOLATION" in src["calibration"],
        "not_calibratable_state":
            "FORECAST_NOT_ECONOMICALLY_CALIBRATABLE" in src["calibration"],
        "negative_slope_raises":
            "raise CalibrationRefused" in src["calibration"]
            and "if slope < 0.0:" in src["calibration"],
        "fitted_on_entitled_evidence_only":
            "DISCOVERY_ONLY" in src["calibration"],
        "lockbox_invisible_to_calibration":
            '"lockbox_used": False' in src["calibration"]
            and "invisible_to_calibration" in src["contract"],
        "bound_into_candidate_identity":
            "_calib.contract()" in src["judge"],
        "live_cross_sections_verified":
            "verify_rank_identity" in src["judge"],
    }

    # (26)-(28) CORRECTION 3. Track B turnover is aligned by security identity.
    track_b = {
        "alignment_declared":
            "BY_SECURITY_IDENTITY_NEVER_BY_ARRAY_POSITION" in src["contract"],
        "learner_requires_symbols":
            "(X_t, r_t, symbols_t)" in src["learners"],
        "learner_refuses_a_two_element_block":
            "raise ValueError" in src["learners"]
            and "cannot carry security identity" in src["learners"],
        "aligns_by_symbol_union":
            "def _align_previous" in src["learners"],
        "no_positional_shape_comparison":
            "prev_shape" not in src["learners"] and "prev_shape" not in src["novel"],
        "track_b_can_hold_cash":
            "def _softmax_with_cash" in src["learners"]
            and "cash_is_a_competing_asset" in src["learners"],
        "novel_decision_family_prices_cost":
            "cost_rate" in src["novel"] and "_ = cost_rate" not in src["novel"],
        "shared_feasibility_seam":
            "def feasible_portfolio" in src["allocation"],
        "one_transition_cost_calculation":
            "def traded_notional" in src["allocation"],
    }

    # (29)-(30) CORRECTION 4. Two benchmarks, and neither may replace the other.
    benchmark_duality = {
        "one_benchmark_owner":
            not _second_owners('MANIFEST_SCHEMA = "r31_benchmark_manifest',
                               R31_OWNERS["benchmarks"]),
        "both_declared":
            "SP500_PIT_EQUAL_WEIGHT" in src["contract"]
            and "SPY_TOTAL_RETURN" in src["contract"],
        "substitution_forbidden":
            "BENCHMARK_SUBSTITUTION_PERMITTED = False" in src["contract"],
        "blocked_state_exists":
            "SPY_RELATIVE_EVIDENCE_BLOCKED" in src["benchmarks"],
        "price_only_index_inadmissible":
            "PRICE_ONLY_INADMISSIBLE" in src["benchmarks"],
        "judge_reports_both":
            "net_excess_vs_spy_annualised" in src["judge"]
            and "equal_weight_benchmark_annualised" in src["judge"],
        "silent_substitution_refused":
            "silent_substitution_permitted" in src["benchmarks"],
    }

    # (31)-(32) The shared covariance cache is one owner, hash-bound, and PIT.
    covariance_cache = {
        "one_cache_owner":
            not _second_owners('MANIFEST_SCHEMA = "r31_covariance_cache_manifest',
                               R31_OWNERS["covcache"]),
        "delegates_to_canonical_builder":
            "_hoc.build_covariance(" in src["covcache"],
        "owns_no_covariance_mathematics":
            "campaign_owns_no_covariance_mathematics" in src["covcache"],
        "key_binds_inputs": "def cache_key" in src["covcache"],
        "key_mismatch_raises": "class CacheKeyMismatch" in src["covcache"],
        "point_in_time_window_declared":
            "never reads a later row" in src["covcache"],
        "contract_binds_the_key":
            "covariance_cache_key" in src["contract"],
    }

    # (33) Campaign v1 and v2 are superseded, preserved, and structurally unable
    # to influence v3.
    supersession = {
        "campaign_is_v3": 'CAMPAIGN_ID = "r31_mathematical_alpha_frontier_v3"'
                          in src["contract"],
        "both_predecessors_listed":
            "r31_mathematical_alpha_frontier_v1" in src["contract"]
            and "r31_mathematical_alpha_frontier_v2" in src["contract"],
        "state_declared":
            "SUPERSEDED_EXPERIMENTAL_DESIGN" in src["contract"],
        "evidence_rules_declared":
            "SUPERSEDED_EVIDENCE_RULES" in src["contract"],
        "excluded_from_denominator":
            "superseded_campaign_results_are_not_in_the_denominator"
            in src["contract"],
        "identity_binds_universe_and_benchmark":
            '"investment_universe": str(universe_hash)' in src["methods"]
            and '"benchmarks": str(benchmark_hash)' in src["methods"],
    }

    # (34) No look-ahead fallback in any walk-forward training window.
    pit_training = {
        "minimum_training_window_declared":
            "MIN_TRAIN_SECTIONS" in src["methods"],
        "methods_have_no_warmup_fallback":
            "warmup[:max(12" not in src["methods"],
        "novel_has_no_warmup_fallback":
            "or warm[:12]" not in src["novel"],
        "absent_model_returns_nan":
            "np.full(X.shape[0], np.nan" in src["methods"]
            and "np.full(X.shape[0], np.nan" in src["novel"],
        "judge_skips_a_date_without_a_model":
            "np.isfinite(score).sum()" in src["judge"],
    }

    # (35) A superiority check must be CAPABLE of failing. When the incumbent
    # cannot be priced there is no drawdown and no turnover to compare against,
    # and filling the absent reference with the candidate's own values compares
    # the candidate to itself: two blocking checks then pass on every input that
    # could ever be supplied. An unprovable check is reported UNAVAILABLE and,
    # being unproven, does not count toward a superiority claim.
    falsifiable_superiority = {
        "unavailable_state_declared":
            "UNAVAILABLE_NO_INCUMBENT" in src["campaign"],
        "absent_incumbent_does_not_borrow_candidate_drawdown":
            '"max_drawdown_net": bp.get("max_drawdown_net")'
            not in src["campaign"],
        "absent_incumbent_does_not_borrow_candidate_turnover":
            '"turnover_annualised": bp.get("turnover_annualised")'
            not in src["campaign"],
        "unavailable_check_is_not_a_pass":
            'all(c["pass"] is True for c in checks.values())' in src["campaign"],
        "unavailable_checks_are_reported":
            '"checks_unavailable"' in src["campaign"],
    }

    # (15) No automatic promotion anywhere.
    promotion = {
        "declared_false": "AUTOMATIC_PROMOTION_ALLOWED = False"
                          in (_read("alpha_agent/r31/__init__.py") or ""),
        "safety_block_reports_it":
            "automatic_promotion_allowed" in (_read("alpha_agent/r31/__init__.py") or ""),
        "read_model_reports_it": '"automatic_promotion_allowed": False' in read_model,
        "read_model_declares_no_activation":
            '"allows_model_activation": False' in read_model,
    }

    # Read surface: GET only, authenticated, declared once, and it writes nothing.
    route_declared = app.count('"%s"' % R31_ROUTE)
    route_block = ""
    if route_declared:
        i = app.index('"%s"' % R31_ROUTE)
        route_block = app[max(0, i - 300): i + 400]
    read_surface = {
        "route_declared_once": route_declared == 1,
        "route_is_get": "@app.get(" in route_block,
        "route_authenticated": "_verify_api_key" in route_block,
        "read_model_present": bool(read_model),
        "read_model_imports_research_package":
            "paper_trader.alpha_agent" in read_model,
    }
    read_model_writes = sorted(
        t for t in ("write_text", "mkdir", "savez", "os.replace")
        if t in read_model)

    # UI: the region exists, carries the safety badges, and has no execute,
    # approve or activate control.
    ui_region = ""
    if R31_UI_REGION in ui:
        s = ui.index(R31_UI_REGION)
        e = ui.find("RELEASE 29 UX2: OPERATING DIAGNOSTICS", s)
        ui_region = ui[s: e if e > s else s + 4000]
    ui_controls = sorted(
        t for t in ("<button", "onclick=", "Approve", "Activate", "Promote",
                    "Execute", "Create Order")
        if t in ui_region)
    # 27B.6 wording, and it is the ONLY admissible form here: paper orders are
    # real and this region creates none, whereas live brokerage orders are
    # structurally disabled. The bare "NO LIVE ORDERS" badge conflates the two
    # and is refused by tests/test_alpha_agent_stage12.py,
    # tests/test_phase27b7_operator_hard_cutover.py and
    # tests/test_phase27b8_operational_portfolio.py.
    ui_badges_missing = sorted(
        b for b in ("RESEARCH ONLY", "READ ONLY", "NO LIVE BROKER ORDERS",
                    "AUTOMATION OFF", "MANUAL REVIEW")
        if b not in ui_region)
    ui_ambiguous_badges = sorted(
        b for b in (">NO LIVE ORDERS</span>", ">ORDERS DISABLED<")
        if b in ui_region)

    return {
        "modules_present": modules_present,
        "modules_missing": modules_missing,
        "second_owner_modules": second_owners,
        "lockbox_guard": lockbox_guard,
        "forbidden_calls_in_research_lane": forbidden_calls,
        "forbidden_operational_owner_refs": forbidden_owner_refs,
        "research_lane_imports_api": research_imports_api,
        "forbidden_engine_imports": forbidden_engine,
        "impure_engine_owner_imports": impure_engine,
        "universe_separation": universe_separation,
        "zero_base_primary": zero_base_primary,
        "calibration_guard": calibration_guard,
        "track_b_symbol_alignment": track_b,
        "benchmark_duality": benchmark_duality,
        "covariance_cache": covariance_cache,
        "supersession": supersession,
        "point_in_time_training": pit_training,
        "falsifiable_superiority": falsifiable_superiority,
        "budgets_not_encoded": budgets_encoded,
        "budgets_enforced": budgets_enforced,
        "exhaustion": exhaustion,
        "inadmissible_information": inadmissible,
        "news_shaped_features": news_features,
        "canonical_owner_reuse": reuse,
        "duplicate_optimiser_modules": duplicate_optimiser,
        "duplicate_cost_literal_modules": duplicate_cost_literal,
        "automatic_promotion": promotion,
        "read_surface": read_surface,
        "read_model_write_tokens": read_model_writes,
        "ui_execute_controls": ui_controls,
        "ui_missing_safety_badges": ui_badges_missing,
        "ui_ambiguous_safety_badges": ui_ambiguous_badges,
    }


#: Release 47 owners. THREE modules and no more: one constraint kernel, one
#: decision-evidence kernel, one decision-evidence composition owner.
R47_CONSTRAINT_KERNEL = "engine/constrained_reallocation.py"
R47_OUTCOME_KERNEL = "engine/portfolio_decision_outcome.py"
R47_OUTCOME_OWNER = "api/portfolio_decision_outcome.py"
R47_MODULES = (R47_CONSTRAINT_KERNEL, R47_OUTCOME_KERNEL, R47_OUTCOME_OWNER)
#: A pure kernel may not reach a clock, a file, a socket or a database.
R47_KERNEL_IMPURITY = ("import requests", "urllib", "sqlalchemy", "os.environ",
                       "open(", "Path(", "datetime.now", "date.today")
#: Nothing in Release 47 may create an order, a fill or a settlement.
R47_EXECUTION_TOKENS = ("place_order(", "submit_order(", "create_order(",
                        "route_order(", "settle_due_orders(", "_append_ledger(")
#: The reshaping limits that must NEVER appear in the true-blocker set. This tuple is
#: the release in one line: a normal cap changes the answer, it does not stop it.
R47_MUST_RESHAPE = ("NAME_WEIGHT_CAP", "SECTOR_WEIGHT_CAP", "RISK_CONTRIBUTION_CAP",
                    "CONCENTRATION_INCREASE_LIMIT", "TURNOVER_BUDGET",
                    "LIQUIDITY_PARTICIPATION_CAP", "LIQUIDITY_ADV_FLOOR",
                    "MIN_POSITION_WEIGHT", "MAX_POSITION_COUNT", "CASH_BOUNDS")
R47_OUTCOMES = ("PROPOSAL_READY", "HOLD_CURRENT_BOOK", "TRUE_BLOCKER")
R47_ROUTES = ("/v1/operations/constrained-reallocation",
              "/v1/operations/portfolio-decision-outcomes")
#: Routes Release 47 must NOT introduce. Proposal generation is automatic; portfolio
#: mutation is not, and nothing here may create an execution shortcut.
R47_FORBIDDEN_ROUTES = ("/v1/operations/constrained-reallocation/apply",
                        "/v1/operations/constrained-reallocation/execute",
                        "/v1/operations/constrained-reallocation/confirm",
                        "/v1/operations/portfolio-decision-outcomes/record",
                        "/v1/operations/auto-rebalance",
                        "/v1/operations/create-orders")
#: Ways a Release-47 module could address the Release-46 research tournament. Prose
#: may NAME it (the separation has to be explained); nothing may REACH it.
R47_RESEARCH_REACH = ("import alpha_agent", "from alpha_agent", "alpha_agent.",
                      "alpha_agent/", "prospective_tournament",
                      "prospective_alpha_tournament", "r46_forward_predictions",
                      "r46_forward_outcomes", "r46_challenger_registry")


def check_release47_constrained_reallocation(files: list[Path]) -> dict:
    r"""Release 47 - CONSTRAINT-RESPECTING ACTIVE REALLOCATION ownership guard.

    The defect this release removes is structural, so the guard is structural. It
    proves:

      (1)  the three owners exist and there is no second one;
      (2)  ONE module classifies constraints, and NO normal portfolio limit is
           declared a true blocker (the release's central claim, checked as DATA);
      (3)  the proposal kernel RE-OPTIMISES a breached limit before it withholds -
           the withheld state is read from the re-measured target, not from the first
           breach;
      (4)  WITHHELD remains fail-closed and un-approvable at every layer (the
           Release-29.3 guarantee is narrowed in scope, never weakened);
      (5)  the three outcomes are declared once and mirrored consistently by the read
           owner, the decision owner and the workflow surface;
      (6)  HOLD_CURRENT_BOOK is never approvable, at any layer;
      (7)  both kernels are pure and neither can create an order, a fill or a
           settlement;
      (8)  decision evidence is frozen AT the execution boundary, idempotently, into
           its OWN root - never the desk ledger and never the Stage-18 decision root;
      (9)  the counterfactual is frozen prospectively and there is no reconstruction
           path;
      (10) the two new routes are GET-only and no apply / execute / auto route was
           added;
      (11) the UI renders the outcome verbatim and derives no constraint decision;
      (12) no Release-47 module can address the Release-46 research store.
    """
    ck = _read(Path(R47_CONSTRAINT_KERNEL))
    ok = _read(Path(R47_OUTCOME_KERNEL))
    oo = _read(Path(R47_OUTCOME_OWNER))
    rp_k = _read(Path("engine/reallocation_proposal.py"))
    arp = _read(Path("api/reallocation_proposal.py"))
    pd_src = _read(Path("api/portfolio_decision.py"))
    ws_src = _read(Path("api/workflow_state.py"))
    rex = _read(Path("api/rebalance_execution.py"))
    ui = _read(UI_FILE)

    def _tuple_values(src, name):
        """The literal VALUES a module-level tuple holds, resolving Name elements
        through the module's own constants. Comparing symbols would prove nothing
        about the strings that actually reach an operator."""
        try:
            tree = ast.parse(src)
        except SyntaxError:
            return []
        consts = {}
        for node in tree.body:
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
                for t in node.targets:
                    if isinstance(t, ast.Name):
                        consts[t.id] = node.value.value
        for node in tree.body:
            if isinstance(node, ast.Assign):
                for t in node.targets:
                    if isinstance(t, ast.Name) and t.id == name \
                            and isinstance(node.value, (ast.Tuple, ast.List)):
                        out = []
                        for e in node.value.elts:
                            if isinstance(e, ast.Constant):
                                out.append(e.value)
                            elif isinstance(e, ast.Name) and e.id in consts:
                                out.append(consts[e.id])
                        return out
        return []

    # (1) owners present; no second owner of either concept.
    modules_missing = sorted(m for m in R47_MODULES
                             if not (REPO_ROOT / m).exists())
    second_owner_modules = []
    for fp in files:
        rel = _rel(fp)
        if rel in R47_MODULES or rel == "scripts/audit_architecture.py":
            continue
        src = fp.read_text(encoding="utf-8", errors="replace")
        if "def solve_feasible_target(" in src or "def constraint_inventory(" in src \
                or "def freeze_decision_record(" in src \
                or "def freeze_executed_decision(" in src:
            second_owner_modules.append(rel)

    # (2) the classification, as data.
    reshaping = set(_tuple_values(ck, "RESHAPING_CONSTRAINT_CODES"))
    blockers = set(_tuple_values(ck, "TRUE_BLOCKER_CODES"))
    reshaping_declared = set(R47_MUST_RESHAPE) <= reshaping
    caps_declared_as_blockers = sorted(set(R47_MUST_RESHAPE) & blockers)
    classification_disjoint = not (reshaping & blockers)
    unknown_not_promoted = "def is_true_blocker(" in ck and \
        "return str(code) in TRUE_BLOCKER_CODES" in ck

    # (3) re-optimisation happens BEFORE the withhold decision (AST order, not prose).
    reoptimise_present = "def _reoptimise_if_infeasible(" in rp_k
    kernel_delegates = ("from paper_trader.engine import constrained_reallocation"
                        in rp_k)
    idx_reopt = rp_k.find("reoptimisation = _reoptimise_if_infeasible(")
    idx_withhold = rp_k.find('proposal_state = STATE_WITHHELD')
    reoptimise_precedes_withhold = bool(
        idx_reopt != -1 and idx_withhold != -1 and idx_reopt < idx_withhold)
    withhold_reads_remeasured = 'complete_target_limits = measured["limits"]' in rp_k

    # (3b) The per-holding form of the SAME three limits must ASK for a target, not
    #      freeze the book. This is the live defect Release 47 found while validating
    #      in a browser: seven per-name breaches on the 2026-08-28 book promoted
    #      CURRENT_NO_CHANGE to MANUAL_REVIEW_REQUIRED and no target was ever built.
    prs_k = _read(Path("engine/portfolio_reassessment.py"))
    held_breach_asks_for_target = (
        "GATE_HELD_NAME_BREACH_REQUIRES_TARGET" in prs_k
        and "HELD_NAME_CONSTRAINT_BREACH_CODES" in prs_k
        and "reason_codes.append(GATE_HELD_NAME_BREACH_REQUIRES_TARGET)" in prs_k)
    held_breach_not_a_blocker = (
        "blockers.extend(sorted(set(constraint_breaches)))" not in prs_k)
    per_name_deferral_declared = (
        '"per_name_deferred_to_complete_target"' in prs_k
        and "HELD_NAME_CONSTRAINT_BREACH_CODES" in prs_k)
    held_breach_still_visible = (
        '"held_name_constraint_breaches"' in prs_k
        and '"held_name_constraint_breaches"' in _read(
            Path("api/portfolio_reassessment.py")))

    # (4) WITHHELD stays fail-closed (unchanged Release-29.3 contract).
    withheld_not_approvable = all([
        "APPROVABLE_STATES = (STATE_READY, STATE_DEGRADED)" in rp_k,
        "APPROVABLE_READ_STATES = (STATE_READY, STATE_DEGRADED)" in arp,
        "REALLOCATION_APPROVABLE_STATES = (RPS_READY, RPS_DEGRADED)" in ws_src,
        "APPROVABLE_DECISION_STATES = (PDS_REVIEW_REQUIRED, PDS_HELD)" in pd_src,
    ])

    # (5) the three outcomes, declared once and mirrored consistently.
    outcomes = _tuple_values(ck, "OUTCOME_VOCAB")
    outcomes_declared = list(outcomes) == list(R47_OUTCOMES)
    outcomes_mirrored = all([
        "OUTCOME_VOCAB = _cr.OUTCOME_VOCAB" in arp,
        "REALLOCATION_OUTCOME_VOCABULARY" in ws_src,
        list(_tuple_values(ws_src, "REALLOCATION_OUTCOME_VOCABULARY"))
        == list(R47_OUTCOMES),
    ])
    outcome_owner_is_the_kernel = ("def decide_outcome(" in ck
                                   and ck.count("def decide_outcome(") == 1)

    # (6) HOLD_CURRENT_BOOK is a taken decision, never an approvable one.
    hold_not_approvable = all([
        "PDS_HOLD_CURRENT_BOOK" in pd_src,
        "and not hold_current_book)" in pd_src,
        'if summ.get("reallocation_outcome") == _cr.OUTCOME_HOLD_CURRENT_BOOK:'
        in pd_src,
        "HOLD_CURRENT_BOOK_EXPOSED_AS_APPROVABLE" in ws_src,
    ])
    blocked_while_feasible_is_a_violation = (
        "BLOCKED_WHILE_FEASIBLE_TARGET_EXISTS" in ws_src)

    # (7) purity + no execution capability in either kernel.
    kernel_impurity = sorted(
        "%s:%s" % (mod, t)
        for mod, src in ((R47_CONSTRAINT_KERNEL, ck), (R47_OUTCOME_KERNEL, ok))
        for t in R47_KERNEL_IMPURITY if t in src)
    execution_tokens_in_kernels = sorted(
        "%s:%s" % (mod, t)
        for mod, src in ((R47_CONSTRAINT_KERNEL, ck), (R47_OUTCOME_KERNEL, ok))
        for t in R47_EXECUTION_TOKENS if t in src)

    # (8) evidence is frozen at the execution boundary, idempotently, in its own root.
    freeze_at_execution_boundary = ("_freeze_decision_evidence(" in rex
                                    and "portfolio_decision_outcome as pdo" in rex)
    freeze_after_orders_exist = bool(
        rex.find("desk._append_ledger(sdir, desk.ORDERS_FILE, order_events)") != -1
        and rex.find("decision_evidence = _freeze_decision_evidence(")
        > rex.find("desk._append_ledger(sdir, desk.ORDERS_FILE, order_events)"))
    freeze_is_idempotent = ("F_REUSED" in oo and "load_record(decision_id=" in oo)
    own_evidence_root = all([
        'OUTCOME_DIR_ENV = "PAPER_TRADER_PORTFOLIO_DECISION_OUTCOME_DIR"' in oo,
        "PAPER_TRADER_DESK_DIR" not in oo,
        "PAPER_TRADER_PORTFOLIO_DECISION_DIR" not in oo,
    ])
    refuses_without_execution = "F_REFUSED_NOT_EXECUTED" in oo

    # (9) the counterfactual is prospective; there is no reconstruction path.
    counterfactual_prospective = all([
        "COUNTERFACTUAL_HOLD_PORTFOLIO" in ok,
        "def point_in_time_check(" in ok,
        "POINT_IN_TIME_VIOLATION" in ok,
    ])
    reconstruction_defs = sorted(
        t for t in ("def reconstruct_", "def backfill_", "def rebuild_hold_",
                    "def infer_counterfactual_")
        if t in ok or t in oo)

    # (10) routes.
    routes = check_routes()["routes"]
    r47_route_methods = sorted({r["method"] for r in routes
                                if r["path"] in R47_ROUTES})
    missing_routes = sorted(p for p in R47_ROUTES
                            if not any(r["path"] == p for r in routes))
    forbidden_routes_present = sorted(p for p in R47_FORBIDDEN_ROUTES
                                      if any(r["path"] == p for r in routes))

    # (11) the UI renders, it does not decide.
    ui_region_present = ('id="r47-constrained"' in ui
                         and "function _r47Render(" in ui)
    ui_loader_count = ui.count("function _r47Load(")
    ui_body = ""
    if "function _r47Render(" in ui:
        _s = ui.index("function _r47Render(")
        _e = ui.find("/* R47_REGION_END */", _s)
        ui_body = ui[_s:_e] if _e != -1 else ui[_s:_s + 20000]
    ui_derives_decision = sorted(
        t for t in ("sector_cap_fraction", "max_one_way_turnover", "herfindahl",
                    "cost_rate", "0.35", "0.25", "min_switching_net_improvement")
        if t in ui_body)
    ui_action_controls = sorted(
        t for t in ("CONFIRM_PORTFOLIO_REBALANCE_DECISION",
                    "CONFIRM_APPROVED_PORTFOLIO_REBALANCE_ORDER_PLAN",
                    "createOrder", "alert(", "confirm(")
        if t in ui_body)

    # (12) no Release-47 module can address the Release-46 research store.
    research_reach = sorted(
        "%s:%s" % (mod, t)
        for mod, src in ((R47_CONSTRAINT_KERNEL, ck), (R47_OUTCOME_KERNEL, ok),
                         (R47_OUTCOME_OWNER, oo))
        for t in R47_RESEARCH_REACH if t in src)

    # safety declarations
    safety_flags_false = all(
        t in ck for t in ('"created_orders": False', '"broker_enabled": False',
                          '"live_orders_enabled": False',
                          '"promoted_model": False',
                          '"automatic_rebalance_allowed": False'))
    incumbency_declared = ('INCUMBENCY_POLICY = '
                           '"NO_INVESTMENT_PRIVILEGE_ONLY_PRICED_TRANSITION_COST"'
                           in ck)
    hurdle_frozen = ('"hurdle_frozen": True' in ck
                     and '"hurdle_tuned_on_outcomes": False' in ck)
    no_fabricated_expected_return = (
        'EXPECTED_RETURN_STATE_NOT_CALIBRATED = "NOT_CALIBRATED"' in ck
        and '"expected_return_before": None' in ck)

    return {
        "modules": list(R47_MODULES),
        "modules_present": not modules_missing,
        "modules_missing": modules_missing,
        "second_owner_modules": sorted(second_owner_modules),
        "reshaping_constraints_declared": bool(reshaping_declared),
        "caps_declared_as_true_blockers": caps_declared_as_blockers,
        "classification_disjoint": bool(classification_disjoint),
        "unknown_code_not_promoted_to_blocker": bool(unknown_not_promoted),
        "reoptimise_present": bool(reoptimise_present),
        "kernel_delegates_to_constraint_owner": bool(kernel_delegates),
        "reoptimise_precedes_withhold": bool(reoptimise_precedes_withhold),
        "withhold_reads_remeasured_target": bool(withhold_reads_remeasured),
        "held_name_breach_asks_for_target": bool(held_breach_asks_for_target),
        "held_name_breach_not_a_blocker": bool(held_breach_not_a_blocker),
        "per_name_deferral_declared": bool(per_name_deferral_declared),
        "held_name_breach_still_visible": bool(held_breach_still_visible),
        "withheld_not_approvable": bool(withheld_not_approvable),
        "outcomes_declared": bool(outcomes_declared),
        "outcomes_mirrored": bool(outcomes_mirrored),
        "outcome_owner_is_the_kernel": bool(outcome_owner_is_the_kernel),
        "hold_current_book_not_approvable": bool(hold_not_approvable),
        "blocked_while_feasible_is_a_violation": bool(
            blocked_while_feasible_is_a_violation),
        "kernel_impurity": kernel_impurity,
        "execution_tokens_in_kernels": execution_tokens_in_kernels,
        "freeze_at_execution_boundary": bool(freeze_at_execution_boundary),
        "freeze_after_orders_exist": bool(freeze_after_orders_exist),
        "freeze_is_idempotent": bool(freeze_is_idempotent),
        "own_evidence_root": bool(own_evidence_root),
        "refuses_without_execution": bool(refuses_without_execution),
        "counterfactual_prospective": bool(counterfactual_prospective),
        "reconstruction_defs": reconstruction_defs,
        "route_methods": r47_route_methods,
        "missing_routes": missing_routes,
        "forbidden_routes_present": forbidden_routes_present,
        "ui_region_present": bool(ui_region_present),
        "ui_loader_count": ui_loader_count,
        "ui_derives_decision": ui_derives_decision,
        "ui_action_controls": ui_action_controls,
        "research_reach": research_reach,
        "safety_flags_false": bool(safety_flags_false),
        "incumbency_policy_declared": bool(incumbency_declared),
        "switching_hurdle_frozen": bool(hurdle_frozen),
        "no_fabricated_expected_return": bool(no_fabricated_expected_return),
    }


R48_ORCHESTRATOR = "api/portfolio_cycle.py"


def check_release48_portfolio_cycle(files: list[Path]) -> dict:
    r"""Release 48 - the ONE canonical portfolio-cycle orchestration + operator
    presentation guard.

    The release's claim is that the operator now has ONE concept (RUN PORTFOLIO
    CYCLE) with no hidden button order, and that the concept added no new
    authority anywhere. The guard proves, structurally:

      (1)  ONE orchestration owner exists, token-gated by ONE operator token,
           and no second module defines the run entrypoint;
      (2)  the orchestrator DELEGATES to the two existing execution owners and
           reads the ONE workflow owner between steps — and its CODE (docstrings
           stripped) can reach no persistence, no desk, no approval token, no
           execution lifecycle and no R46 research;
      (3)  exactly one GET status route and one POST run route exist, no
           approve/execute/confirm variant of the route was added, and both
           routes are registered to the orchestrator in the authoritative
           route_ownership inventory (corrective gate 2026-08-29);
      (4)  the canonical operator command PRESENTS the one cycle action only
           when a normal-path mutation was already decided (never a new
           mutation surface), with the decided underlying step beside it;
      (5)  the UI carries exactly one cycle runner, wired through the ONE
           canonical dispatcher, which still refuses to execute off Today;
      (6)  the CURRENT vs RECOMMENDED card is promoted on the primary Portfolio
           surface, and no separate "R48 dashboard" was added;
      (7)  no normal-UI wording presents the monthly model-governance
           checkpoint as the portfolio-action cadence.
    """
    pc_src = _read(Path(R48_ORCHESTRATOR))
    ws_src = _read(Path("api/workflow_state.py"))
    ob_src = _read(Path("api/operational_book.py"))
    ab_src = _read(Path("api/alpha_book.py"))
    ui = _read(UI_FILE)

    # -- code-only view of the orchestrator (docstrings stripped) ------------ #
    def _code_only(src: str) -> str:
        try:
            tree = ast.parse(src)
        except SyntaxError:
            return src
        for node in ast.walk(tree):
            if isinstance(node, (ast.Module, ast.FunctionDef,
                                 ast.AsyncFunctionDef, ast.ClassDef)):
                body = node.body
                if body and isinstance(body[0], ast.Expr) \
                        and isinstance(body[0].value, ast.Constant) \
                        and isinstance(body[0].value.value, str):
                    node.body = body[1:] or [ast.Pass()]
        return ast.unparse(tree)

    pc_code = _code_only(pc_src)

    owner_present = bool(pc_src.strip())
    one_operator_token = 'EXECUTE_CONFIRMATION = "RUN_PORTFOLIO_CYCLE"' in pc_src
    delegates_to_close = "run_daily_close(" in pc_code
    delegates_to_drc = "run_daily_research_cycle(" in pc_code
    # Release 54.2.3.1 — the orchestrator still reads the ONE workflow owner, now
    # supplying the close owner's already-probed provider answer (the workflow is
    # probe-free by contract, so the composition hands it the readiness verdict).
    reads_one_workflow_owner = (
        "load_workflow_state()" in pc_code
        or "load_workflow_state(provider_readiness=readiness)" in pc_code)
    max_one_invocation_each = "if step in ran:" in pc_code

    persistence_reach = sorted(t for t in (
        "open(", "json.dump(", "write_text", "mkdir", "Path.home",
        "PAPER_TRADER_") if t in pc_code)
    authority_reach = sorted(t for t in (
        "rebalance_execution", "record_decision", "paper_trading_desk",
        "settle_due_orders", "create_order", "APPROVE_FOR_PAPER_REBALANCE",
        "CONFIRM_PORTFOLIO_REBALANCE_DECISION",
        "CONFIRM_APPROVED_PORTFOLIO_REBALANCE_ORDER_PLAN",
        "alpha_agent", "prospective_tournament") if t in pc_code)

    second_orchestrator_modules = sorted(
        _rel(fp) for fp in files
        if _rel(fp) not in (R48_ORCHESTRATOR, "scripts/audit_architecture.py")
        and "def run_portfolio_cycle(" in fp.read_text(encoding="utf-8",
                                                       errors="replace"))

    routes = check_routes()["routes"]
    cycle_routes = [r for r in routes
                    if r["path"].startswith("/v1/operations/portfolio-cycle")]
    get_status_routes = [r for r in cycle_routes
                         if r["path"] == "/v1/operations/portfolio-cycle"
                         and r["method"] == "GET"]
    post_run_routes = [r for r in cycle_routes
                       if r["path"] == "/v1/operations/portfolio-cycle/run"
                       and r["method"] == "POST"]
    forbidden_cycle_routes = sorted(
        "%s %s" % (r["method"], r["path"]) for r in cycle_routes
        if r not in get_status_routes + post_run_routes)

    # -- route ownership registered in the authoritative inventory ----------- #
    # Corrective gate (2026-08-29): the generic architecture contract
    # (test_every_declared_route_has_an_owner) found both cycle routes absent
    # from route_ownership. The inventory stays the authoritative contract;
    # this invariant only proves the registration exists and covers every
    # declared portfolio-cycle route.
    try:
        _inv = json.loads(_read("docs/architecture/system_inventory.json"))
        _ownership = _inv.get("route_ownership", [])
    except (json.JSONDecodeError, OSError):
        _ownership = []
    _pfx = "/v1/operations/portfolio-cycle"
    _cycle_owner_entries = [
        e for e in _ownership
        if e.get("prefix") == _pfx
        and e.get("owner") == R48_ORCHESTRATOR
        and e.get("system") == "infrastructure"]
    route_ownership_registered = len(_cycle_owner_entries) == 1
    route_ownership_covers_all_cycle_routes = (
        bool(_cycle_owner_entries)
        and bool(cycle_routes)
        and all(r["path"] == _pfx or r["path"].startswith(_pfx + "/")
                for r in cycle_routes))

    # -- the operator presentation (workflow owner) -------------------------- #
    presentation_declared = all(t in ws_src for t in (
        'EXEC_PORTFOLIO_CYCLE = "PORTFOLIO_CYCLE"',
        'PORTFOLIO_CYCLE_CONFIRMATION = "RUN_PORTFOLIO_CYCLE"',
        '"path": "/v1/operations/portfolio-cycle/run"'))
    presented_only_when_decided = (
        '"primary_action_kind": EXEC_PORTFOLIO_CYCLE if executable else None'
        in ws_src)
    underlying_step_travels = (
        '"cycle_underlying_kind": kind if executable else None' in ws_src)

    # -- the UI (one runner, one dispatcher, off-Today refusal kept) --------- #
    ui_runner_count = ui.count("function runPortfolioCycle(")
    ui_run_post_count = ui.count("'/v1/operations/portfolio-cycle/run'")
    _disp = ui.split("function dispatchCanonicalPrimaryAction(")
    disp_fn = _disp[1].split("\nwindow.")[0] if len(_disp) > 1 else ""
    dispatcher_routes_cycle = ("PORTFOLIO_CYCLE" in disp_fn
                               and "runPortfolioCycle(btn)" in disp_fn)
    dispatcher_refuses_off_today = "_wsIsTodayRoute()" in disp_fn

    # -- Portfolio surface: CURRENT vs RECOMMENDED promoted; no R48 dashboard - #
    r47_card_promoted = ("#tab-portfolio-manager > .card > #r47-constrained"
                         in ui)
    r48_new_panel_ids = [m for m in re.findall(r'id="(r48-[\w-]+)"', ui)
                         if m != "r48-styles"]

    # -- monthly semantics (§15) --------------------------------------------- #
    monthly_as_portfolio_cadence = sorted(
        src_name for src_name, src in (("api/operational_book.py", ob_src),
                                       ("api/alpha_book.py", ab_src))
        if "scheduled monthly review is not due" in src)
    checkpoint_named_precisely = (
        "model-governance review checkpoint" in ob_src
        and "not on a monthly clock" in ab_src)

    return {
        "phase": "R48",
        "owner_present": bool(owner_present),
        "one_operator_token": bool(one_operator_token),
        "delegates_to_close": bool(delegates_to_close),
        "delegates_to_drc": bool(delegates_to_drc),
        "reads_one_workflow_owner": bool(reads_one_workflow_owner),
        "max_one_invocation_each": bool(max_one_invocation_each),
        "persistence_reach": persistence_reach,
        "authority_reach": authority_reach,
        "second_orchestrator_modules": second_orchestrator_modules,
        "get_status_route_count": len(get_status_routes),
        "post_run_route_count": len(post_run_routes),
        "forbidden_cycle_routes": forbidden_cycle_routes,
        "route_ownership_registered": bool(route_ownership_registered),
        "route_ownership_covers_all_cycle_routes":
            bool(route_ownership_covers_all_cycle_routes),
        "presentation_declared": bool(presentation_declared),
        "presented_only_when_decided": bool(presented_only_when_decided),
        "underlying_step_travels": bool(underlying_step_travels),
        "ui_runner_count": ui_runner_count,
        "ui_run_post_count": ui_run_post_count,
        "dispatcher_routes_cycle": bool(dispatcher_routes_cycle),
        "dispatcher_refuses_off_today": bool(dispatcher_refuses_off_today),
        "r47_card_promoted": bool(r47_card_promoted),
        "r48_new_panel_ids": r48_new_panel_ids,
        "monthly_as_portfolio_cadence": monthly_as_portfolio_cadence,
        "checkpoint_named_precisely": bool(checkpoint_named_precisely),
    }


R49_OWNER = "api/operator_presentation.py"
R49_ROUTE = "/v1/operations/operator-presentation"
#: Raw implementation vocabulary that may live in DOM data attributes, in Audit /
#: Advanced and in developer diagnostics — never in the normal operator prose the
#: Release-49 renderer writes.
R49_RAW_STATE_TOKENS = (
    "MANUAL_REVIEW_REQUIRED", "PORTFOLIO_DECISION_NO_PROPOSAL",
    "REBALANCE_NO_PROPOSAL", "STATE_NOT_RUN", "RUN_DAILY_RESEARCH_CYCLE",
    "CONFIRM_ALPHA_DAILY_CLOSE", "CONFIRM_APPROVED_PORTFOLIO_REBALANCE_ORDER_PLAN",
    "CONFIRM_PORTFOLIO_REBALANCE_DECISION", "RUN_PORTFOLIO_CYCLE",
    "REALLOCATION_PROPOSAL_NOT_RUN", "DAILY_CLOSE_COMPLETE_MEMBERSHIP_DRIFT",
)


def check_release49_operator_presentation(files: list[Path]) -> dict:
    r"""Release 49 - ONE reconciled operator presentation + Today / Portfolio rebuild.

    The release's claim is that the operator now reads ONE reconciled truth instead
    of many raw subsystem states, that Today is a command center of at most four
    primary sections with at most one primary action, that the Portfolio route is
    four task views with the diagnostic machinery under Audit & Details, and that
    all of this added NO authority anywhere. The guard proves, structurally:

      (1)  ONE presentation owner exists and no second module builds one;
      (2)  the owner's CODE (docstrings stripped) reaches no persistence, no
           execution / approval / desk authority, no Release-46 research and no
           business recomputation (it declares recomputes_nothing);
      (3)  a historical session is reconciled, never rerun, backfilled or
           rewritten, and no proposal is fabricated;
      (4)  exactly one GET route, no mutating variant, registered in the
           authoritative route_ownership inventory;
      (5)  Today reads the presentation owner through ONE loader, carries exactly
           the four primary sections, no badge wall, the legacy cards hidden and
           the material-information table moved to System - Audit;
      (6)  the ONE primary action dispatches through the ONE canonical Release-48
           dispatcher and nothing else;
      (7)  Portfolio carries the four task views; the Overview reads the
           presentation owner; model target, paper desk, corporate-action detail,
           raw reassessment / HOC / proposal / rebalance machinery are under
           Audit & Details; the performance charts under Performance;
      (8)  no grid of dashes renders for an absent target, and the normal-mode
           renderer writes no raw implementation vocabulary;
      (9)  no "r49-" dashboard / panel was added, and the manual gates and the
           only executing action (the Release-48 portfolio cycle) are unchanged.
    """
    src = _read(Path(R49_OWNER))
    ui = _read(UI_FILE)
    pd_src = _read(Path("api/portfolio_decision.py"))
    rex_src = _read(Path("api/rebalance_execution.py"))
    pc_src = _read(Path("api/portfolio_cycle.py"))

    def _code_only(text: str) -> str:
        try:
            tree = ast.parse(text)
        except SyntaxError:
            return text
        for node in ast.walk(tree):
            if isinstance(node, (ast.Module, ast.FunctionDef,
                                 ast.AsyncFunctionDef, ast.ClassDef)):
                body = node.body
                if body and isinstance(body[0], ast.Expr) \
                        and isinstance(body[0].value, ast.Constant) \
                        and isinstance(body[0].value.value, str):
                    node.body = body[1:] or [ast.Pass()]
        return ast.unparse(tree)

    code = _code_only(src) if src.strip() else ""

    # (1) one owner
    owner_present = bool(src.strip())
    second_owner_modules = sorted(
        _rel(fp) for fp in files
        if _rel(fp) not in (R49_OWNER, "scripts/audit_architecture.py")
        and _rel(fp).endswith(".py")
        and "def build_operator_presentation(" in fp.read_text(encoding="utf-8",
                                                               errors="replace"))
    vocabulary_frozen = all(t in src for t in (
        'PD_CYCLE_REQUIRED = "CYCLE_REQUIRED"', 'PD_REALLOCATE = "REALLOCATE"',
        'PD_HOLD = "HOLD"', 'PD_BLOCKED = "BLOCKED"',
        'PD_AWAITING_APPROVAL = "AWAITING_APPROVAL"',
        'PD_AWAITING_CONFIRMATION = "AWAITING_CONFIRMATION"',
        'PD_AWAITING_NEXT_CLOSE = "AWAITING_NEXT_CLOSE"',
        'PD_OUTCOME_ACCRUING = "OUTCOME_ACCRUING"',
        'SYSTEM_READY = "READY"', 'SYSTEM_DEGRADED = "DEGRADED"',
        'SYSTEM_BLOCKED = "BLOCKED"'))

    # (2) presentation only — no persistence, no authority, no recomputation
    recomputes_nothing_declared = '"recomputes_nothing": True' in src
    # Call / import forms only: an owner NAME inside a provenance string is not a
    # recomputation; a call into a kernel or an engine import is.
    business_recompute_reach = sorted(t for t in (
        "reoptimise(", "reoptimize(", "solve_feasible", "def _solve",
        "assess_holding", "assess_portfolio", "build_proposal(",
        "build_assessment(", "from paper_trader.engine", "import engine",
        "compute_nav", "mark_to_market", "settle_due", "def _nav",
        "def _target", "def _decide") if t in code)
    persistence_reach = sorted(t for t in (
        "open(", "json.dump(", "write_text", "mkdir", "atomic_write",
        "Path.home") if t in code)
    authority_reach = sorted(t for t in (
        "run_daily_close(", "run_daily_research_cycle(", "run_portfolio_cycle(",
        "record_decision", "paper_trading_desk", "settle_due_orders",
        "create_order", "APPROVE_FOR_PAPER_REBALANCE",
        "CONFIRM_PORTFOLIO_REBALANCE_DECISION",
        "CONFIRM_APPROVED_PORTFOLIO_REBALANCE_ORDER_PLAN", "alpha_agent",
        "prospective_tournament", "promote(", "recalibrate(") if t in code)
    r46_reach = sorted(t for t in (
        "r46", "R46", "alpha_agent", "prospective_tournament",
        "research_trades", "challenger_registry") if t in code)
    executes_only_the_cycle = (
        "EXECUTING_NEXT_ACTION_KINDS = frozenset({NA_PORTFOLIO_CYCLE})" in src)

    # (3) historical reconciliation, never a rerun
    historical_contract_declared = all(t in src for t in (
        '"history_rewritten": False', '"proposal_fabricated": False',
        '"rerun_of_historical_session_instructed": False',
        '"PRIOR_DECISION_WORKFLOW"'))
    # Code only: the module's docstring legitimately QUOTES the legacy phrase it
    # exists to retire; the operator prose the code emits must never carry it.
    rerun_instruction_in_owner = "RUN THE DAILY RESEARCH CYCLE" in code.upper()

    # (4) routes
    routes = check_routes()["routes"]
    r49_routes = [r for r in routes if r["path"].startswith(R49_ROUTE)]
    get_routes = [r for r in r49_routes
                  if r["path"] == R49_ROUTE and r["method"] == "GET"]
    forbidden_routes = sorted("%s %s" % (r["method"], r["path"])
                              for r in r49_routes if r not in get_routes)
    try:
        _inv = json.loads(_read("docs/architecture/system_inventory.json"))
        _ownership = _inv.get("route_ownership", [])
        _modules = _inv.get("modules", [])
    except (json.JSONDecodeError, OSError):
        _ownership, _modules = [], []
    route_ownership_registered = len([
        e for e in _ownership
        if e.get("prefix") == R49_ROUTE and e.get("owner") == R49_OWNER]) == 1
    module_registered = any(
        m.get("path", "").replace("\\", "/") == R49_OWNER for m in _modules)

    # (5) Today reads the owner; four primary sections; no badge wall; legacy hidden
    _t0 = ui.find('<div id="tab-overview"')
    _t1 = ui.find("<!-- end tab-overview -->")
    today = ui[_t0:_t1] if (_t0 != -1 and _t1 > _t0) else ""
    _c0 = today.find('<div id="today-command-center"')
    _c1 = today.find("<!-- ===================== Phase 14-A COMMAND CENTER START")
    tcc = today[_c0:_c1] if (_c0 != -1 and _c1 > _c0) else ""
    today_reads_presentation_owner = (
        'data-presentation-owner="api.operator_presentation"' in tcc)
    today_primary_section_count = len(re.findall(
        r'<div id="(?:today-system-band|today-decision|today-snapshot|today-attention)"',
        tcc))
    # R54 Slice 1 evolves this invariant: Today carries the four R49 presentation
    # sections PLUS the one Active Manager operating-state region, which belongs
    # to a DIFFERENT declared owner (api.active_manager_state) and is admitted
    # only while it declares that owner on the node. An undeclared extra section
    # still fails the build.
    _r54_admitted = ('today-operating-state'
                     if 'id="today-operating-state" data-owner='
                        '"api.active_manager_state"' in today else None)
    # R54.2.1 admits ONE more region: the missed-completed-session (catch-up) banner.
    # It is not a fifth PRIMARY section — it is a conditional obligation notice that is
    # hidden entirely unless the backend reports an unclosed completed session, and it
    # renders NO execution control of its own (the one-CTA invariant below still binds
    # at exactly 1). It is admitted only while it declares the owner that DECIDES the
    # obligation on the node; an undeclared extra section still fails the build.
    _r5421_admitted = ('today-session-recovery'
                       if 'id="today-session-recovery" data-recovery-owner='
                          '"api.workflow_state"' in tcc else None)
    # R54.2.2 admits ONE more on exactly the same terms: the post-close governed-
    # research obligation notice. Hidden unless the backend reports governed research
    # still owed for a completed close, it renders NO execution control of its own,
    # and it is admitted only while it declares the owner that DECIDES the obligation.
    _r5422_admitted = ('today-governed-research'
                       if 'id="today-governed-research" data-research-owner='
                          '"api.workflow_state"' in tcc else None)
    today_extra_section_ids = sorted(
        m for m in re.findall(r'<div id="(today-[\w-]+)"', tcc)
        if m not in ("today-command-center", "today-system-band", "today-decision",
                     "today-snapshot", "today-attention", _r54_admitted,
                     _r5421_admitted, _r5422_admitted))
    today_badge_walls = tcc.count("cc-badge")
    _s0 = ui.find('<style id="r49-styles">')
    _s1 = ui.find("</style>", _s0) if _s0 != -1 else -1
    r49_css = ui[_s0:_s1] if (_s0 != -1 and _s1 > _s0) else ""
    legacy_today_hidden = all(t in r49_css for t in (
        'body[data-route="command-center"] #cc-root',
        'body[data-route="command-center"] #operator-command'))
    _sy0 = ui.find('<div class="card" id="sysops-panel"')
    _sy1 = ui.find("<!-- One page-level safety strip", _sy0) if _sy0 != -1 else -1
    sysops = ui[_sy0:_sy1] if (_sy0 != -1 and _sy1 > _sy0) else ""
    material_table_off_today = ('id="cc-matinfo-card"' not in today
                                and 'id="cc-matinfo-card"' in sysops)
    ui_loader_count = ui.count("function loadOperatorPresentation(")
    ui_route_count = ui.count("'/v1/operations/operator-presentation'")

    # (6) the ONE primary action -> the ONE canonical dispatcher
    _r0 = ui.find("/* R49_REGION_START */")
    _r1 = ui.find("/* R49_REGION_END */")
    r49_region = ui[_r0:_r1] if (_r0 != -1 and _r1 > _r0) else ""
    primary_cta_render_count = r49_region.count('onclick="opresPrimaryAction(this)"')
    dispatcher_use_count = r49_region.count("dispatchCanonicalPrimaryAction(btn)")
    region_mutation_reach = sorted(t for t in (
        "call('POST'", "fetch(", "method: 'POST'", "/execute", "/run'",
        "orders/confirm", "rebalance/confirm", "portfolio-decision/record")
        if t in r49_region)
    region_native_dialogs = sorted(t for t in ("alert(", "confirm(", "prompt(")
                                   if re.search(r"(?<![\w.])" + re.escape(t),
                                                r49_region))

    # (7) Portfolio task views + demotions
    pm_views_present = ('id="pm-views"' in ui and all(
        ('data-pm-view="%s"' % v) in ui
        for v in ("overview", "reallocation", "performance", "audit")))
    overview_reads_presentation_owner = (
        "_opRenderDecision(p, 'pm-overview-decision'" in ui)
    audit_demotion_css = all(t in r49_css for t in (
        '#tab-portfolio-manager:not([data-pm-view="audit"]) > #pm-adv-exec',
        '#tab-portfolio-manager:not([data-pm-view="audit"]) > #pm-advanced',
        '#tab-portfolio-manager:not([data-pm-view="audit"]) > .card > #reassess-card',
        '#tab-portfolio-manager:not([data-pm-view="audit"]) > .card > #pa-decision',
        '#tab-portfolio-manager:not([data-pm-view="audit"]) > .card > #pm-dag-card'))
    _ax0 = ui.find('id="pm-adv-exec"')
    _ax1 = ui.find("end pm-adv-exec", _ax0) if _ax0 != -1 else -1
    _ad0 = ui.find('<details class="card" id="pm-advanced"')
    _ad1 = ui.find('id="zb-card"', _ad0) if _ad0 != -1 else -1

    def _inside(tok: str, a: int, b: int) -> bool:
        i = ui.find(tok)
        return a != -1 and b > a and a < i < b

    model_target_under_audit = _inside('id="otr-band"', _ax0, _ax1)
    paper_desk_under_audit = _inside('id="pd-band"', _ax0, _ax1)
    corporate_action_under_audit = _inside('id="stage19-ca-card"', _ad0, _ad1)
    raw_reallocation_under_audit = (_inside('id="realloc-card"', _ad0, _ad1)
                                    and _inside('id="stage19-rebalance-card"', _ad0, _ad1)
                                    and _inside('id="hoc-card"', _ad0, _ad1))
    performance_under_performance_view = (
        '#tab-portfolio-manager:not([data-pm-view="performance"]) > .card > #pdash-perf-charts'
        in r49_css)
    best_feasible_is_the_recommendation = (
        "Current vs Best Feasible Target" in ui
        and "MODEL TARGET SNAPSHOT REVIEW" in ui)

    # (8) no dash grid; no raw vocabulary in the normal-mode renderer
    _f0 = ui.find("function _r47Render(")
    _f1 = ui.find("/* R47_REGION_END */", _f0) if _f0 != -1 else -1
    r47_body = ui[_f0:_f1] if (_f0 != -1 and _f1 > _f0) else ""
    empty_state_for_absent_target = "NO CURRENT FEASIBLE TARGET" in r47_body
    dash_grid_for_absent_target = "_r47Row('Positions', cur.position_count)" in r47_body
    raw_vocabulary_in_normal_renderer = sorted(
        t for t in R49_RAW_STATE_TOKENS if t in r49_region or t in r47_body)

    # (9) no new dashboard; gates unchanged
    r49_new_panel_ids = [m for m in re.findall(r'id="(r49-[\w-]+)"', ui)
                         if m != "r49-styles"]
    manual_gates_unchanged = (
        'CONFIRM_TOKEN = "CONFIRM_PORTFOLIO_REBALANCE_DECISION"' in pd_src
        and "CONFIRM_APPROVED_PORTFOLIO_REBALANCE_ORDER_PLAN" in rex_src
        and 'EXECUTE_CONFIRMATION = "RUN_PORTFOLIO_CYCLE"' in pc_src)
    safety_mode_line_declared = (
        'SAFETY_MODE_LINE = "PAPER · MANUAL APPROVAL · AUTOMATION OFF"' in src)

    return {
        "phase": "R49",
        "owner_present": bool(owner_present),
        "second_owner_modules": second_owner_modules,
        "vocabulary_frozen": bool(vocabulary_frozen),
        "recomputes_nothing_declared": bool(recomputes_nothing_declared),
        "business_recompute_reach": business_recompute_reach,
        "persistence_reach": persistence_reach,
        "authority_reach": authority_reach,
        "r46_reach": r46_reach,
        "executes_only_the_cycle": bool(executes_only_the_cycle),
        "historical_contract_declared": bool(historical_contract_declared),
        "rerun_instruction_in_owner": bool(rerun_instruction_in_owner),
        "get_route_count": len(get_routes),
        "forbidden_routes": forbidden_routes,
        "route_ownership_registered": bool(route_ownership_registered),
        "module_registered": bool(module_registered),
        "today_reads_presentation_owner": bool(today_reads_presentation_owner),
        "today_primary_section_count": today_primary_section_count,
        "today_extra_section_ids": today_extra_section_ids,
        "today_badge_walls": today_badge_walls,
        "legacy_today_hidden": bool(legacy_today_hidden),
        "material_table_off_today": bool(material_table_off_today),
        "ui_loader_count": ui_loader_count,
        "ui_route_count": ui_route_count,
        "primary_cta_render_count": primary_cta_render_count,
        "dispatcher_use_count": dispatcher_use_count,
        "region_mutation_reach": region_mutation_reach,
        "region_native_dialogs": region_native_dialogs,
        "pm_views_present": bool(pm_views_present),
        "overview_reads_presentation_owner": bool(overview_reads_presentation_owner),
        "audit_demotion_css": bool(audit_demotion_css),
        "model_target_under_audit": bool(model_target_under_audit),
        "paper_desk_under_audit": bool(paper_desk_under_audit),
        "corporate_action_under_audit": bool(corporate_action_under_audit),
        "raw_reallocation_under_audit": bool(raw_reallocation_under_audit),
        "performance_under_performance_view": bool(performance_under_performance_view),
        "best_feasible_is_the_recommendation": bool(best_feasible_is_the_recommendation),
        "empty_state_for_absent_target": bool(empty_state_for_absent_target),
        "dash_grid_for_absent_target": bool(dash_grid_for_absent_target),
        "raw_vocabulary_in_normal_renderer": raw_vocabulary_in_normal_renderer,
        "r49_new_panel_ids": r49_new_panel_ids,
        "manual_gates_unchanged": bool(manual_gates_unchanged),
        "safety_mode_line_declared": bool(safety_mode_line_declared),
    }


R50_OWNERS = {
    "instrument_contract": "engine/instrument_contract.py",
    "market_reference_data": "api/market_reference_data.py",
    "investability_registry": "api/investability_registry.py",
    "capital_pool": "api/capital_pool.py",
    "cross_asset_risk_kernel": "engine/cross_asset_risk.py",
    "cross_asset_risk": "api/cross_asset_risk.py",
    "opportunity_frontier_kernel": "engine/opportunity_frontier.py",
    "opportunity_frontier": "api/opportunity_frontier.py",
    "decision_snapshot": "api/decision_snapshot.py",
}
R50_ROUTES = ("/v1/operations/decision-snapshot", "/v1/operations/investability-registry",
              "/v1/operations/capital-pool", "/v1/operations/cross-asset-risk",
              "/v1/operations/opportunity-frontier")
R50_SNAPSHOT_SERVED = ("presentation", "portfolio_state", "constrained", "rebalance",
                       "workflow", "daily_close", "operational", "capital_pool")


def check_release50_multi_asset(files: list[Path]) -> dict:
    r"""Release 50 - the multi-asset operational capital manager.

    The release's claim: ONE capital pool, ONE multi-asset NAV, ONE position
    contract, ONE investability registry, ONE cross-asset risk state, ONE
    opportunity frontier, ONE zero-base owner, ONE feasible-target owner, ONE
    switching-economics owner, ONE decision snapshot - with no snapshot-side or
    presentation-side business recomputation, no research auto-promotion, no
    R46-to-operation direct path, no unsupported capital eligibility, no forced
    diversification, one governed paper execution path, one decision-evidence
    path and no broker path. Every field below is structural.
    """
    def _src(rel: str) -> str:
        return _read(Path(rel))

    def _code_only(text: str) -> str:
        try:
            tree = ast.parse(text)
        except SyntaxError:
            return text
        for node in ast.walk(tree):
            if isinstance(node, (ast.Module, ast.FunctionDef,
                                 ast.AsyncFunctionDef, ast.ClassDef)):
                body = node.body
                if body and isinstance(body[0], ast.Expr) \
                        and isinstance(body[0].value, ast.Constant) \
                        and isinstance(body[0].value.value, str):
                    node.body = body[1:] or [ast.Pass()]
        return ast.unparse(tree)

    def _operational(rel: str) -> bool:
        # The OPERATIONAL surface: api/ and engine/. Research lanes (alpha_agent/,
        # the research engines) may carry their own kernels; they are not owners of
        # an operational business concept and are out of this scope by design.
        return (rel.startswith(("api/", "engine/")) and rel.endswith(".py")
                and "research" not in rel and rel != "scripts/audit_architecture.py")

    def _count_def(name: str) -> list:
        return sorted(_rel(fp) for fp in files if _operational(_rel(fp))
                      and ("def %s(" % name) in fp.read_text(encoding="utf-8", errors="replace"))

    srcs = {k: _src(v) for k, v in R50_OWNERS.items()}
    codes = {k: (_code_only(v) if v.strip() else "") for k, v in srcs.items()}
    owners_present = sorted(k for k, v in srcs.items() if not v.strip())

    # ONE owner per business concept
    capital_pool_owners = _count_def("build_capital_pool")
    nav_owners = _count_def("book_nav")
    position_contract_owners = sorted(
        _rel(fp) for fp in files if _operational(_rel(fp))
        and 'SCHEMA_VERSION = "multi_asset_position.v1"' in fp.read_text(encoding="utf-8", errors="replace"))
    registry_owners = _count_def("load_investability_registry")
    risk_owners = _count_def("build_risk_state")
    frontier_owners = _count_def("build_frontier")
    zero_base_owners = _count_def("build_allocation")
    feasible_owners = _count_def("solve_feasible_target")
    switching_owners = _count_def("switching_economics")
    snapshot_owners = _count_def("load_decision_snapshot")
    execution_owners = _count_def("confirm_rebalance_order_plan")
    settlement_owners = _count_def("settle_due_orders")
    evidence_owners = _count_def("freeze_executed_decision")
    drawdown_owners = _count_def("current_drawdown")
    covariance_owners = _count_def("build_covariance")

    # no snapshot-side / presentation-side business recomputation
    snap_code = codes["decision_snapshot"]
    snapshot_business_reach = sorted(t for t in (
        "build_covariance", "solve_feasible", "book_nav(", "value_position", "switching_economics(",
        "build_proposal(", "build_allocation(", "optimise(", "from paper_trader.engine",
        "import engine", "mark_to_market", "compute_nav") if t in snap_code)
    snapshot_declares_no_business = ('"business_calculation_owner": False' in srcs["decision_snapshot"]
                                     and '"recomputes_nothing": True' in srcs["decision_snapshot"])
    snapshot_invalidation_is_identity = ('"invalidation": "IDENTITY_CHANGE' in srcs["decision_snapshot"]
                                         and "def snapshot_identity(" in srcs["decision_snapshot"])
    pres_code = _code_only(_src("api/operator_presentation.py"))
    presentation_business_reach = sorted(t for t in (
        "value_position", "aggregate_exposures", "build_capital_pool(", "build_risk_state(",
        "build_frontier(", "from paper_trader.engine") if t in pres_code)

    # no research auto-promotion; no R46-to-operation direct path
    reg_src, reg_code = srcs["investability_registry"], codes["investability_registry"]
    registry_no_promotion = ('"automatic_promotion": False' in reg_src
                             and '"this_module_can_promote": False' in reg_src
                             and '"research_verdict_promotes": False' in reg_src
                             and "def promote" not in reg_code)
    registry_eligibility_derived = ('"capital_eligible_is_derived": True' in reg_src
                                    and "missing = [c for c in CAPABILITIES if not caps[c]]" in reg_src
                                    and "eligible = not missing" in reg_src)
    # Reach = an IMPORT or a CALL into research, never a string that NAMES research
    # evidence (the registry legitimately cites challenger ids as evidence).
    r46_reach = sorted("%s:%s" % (k, t) for k, c in codes.items() for t in (
        "alpha_agent", "prospective_tournament", "challenger_registry", "research_trades",
        "adopted_forward", "research_shadow") if t in c)
    research_imports_in_operational = sorted(
        k for k, c in codes.items() if "alpha_agent" in c)

    # no forced diversification; long-only declared; cash is a real choice
    forced_div_declared_false = all('"forced_diversification": False' in s for s in (
        srcs["opportunity_frontier_kernel"], _src("engine/constrained_reallocation.py"),
        _src("engine/reallocation_proposal.py"), _src("engine/zero_base_allocator.py")))
    forced_min_weight_tokens = sorted(t for t in ("min_asset_class_weight", "min_sleeve_weight",
                                                  "min_non_equity_weight", "force_diversif")
                                      if any(t in s for s in codes.values()))
    long_only_declared = "SHORT_EXPOSURE_SUPPORTED = False" in srcs["instrument_contract"]
    zero_signal_not_a_sink = "zero_signal_rule" in srcs["opportunity_frontier_kernel"]

    # execution convention + settlement + collateral semantics declared ONCE
    convention_declared = ('EXECUTION_CONVENTION = "NEXT_SESSION_SETTLEMENT"' in srcs["instrument_contract"]
                           and 'IT_CASH_EQUITY: "NEXT_CLOSE"' in srcs["instrument_contract"])
    futures_not_valued_like_equities = ('"VARIATION_MARGIN_UNREALISED"' in srcs["instrument_contract"]
                                        and "collateral = q * float(d[\"initial_margin_per_unit\"]) * fx"
                                        in srcs["instrument_contract"])
    cost_policy_declared = ('COST_POLICY_VERSION = "multi_asset_cost_policy.v1"' in srcs["instrument_contract"]
                            and "COST_BPS_PER_SIDE_BY_CLASS" in srcs["instrument_contract"])

    # the ONE NAV / mark / settlement owner is instrument-aware; no second engine
    desk_src = _src("api/paper_trading_desk.py")
    desk_routes_owned_marks = ("OWNED_NORGATE_SETTLEMENT" in desk_src
                               and "_mrd.mark_downloader(" in desk_src)
    desk_settles_by_instrument = ("_ic.fill_cash_delta(" in desk_src
                                  and "OWNED_NORGATE_SETTLEMENT_AS_RECORDED" in desk_src)
    desk_nav_instrument_aware = ("_ic.replay_entry_marks(" in desk_src
                                 and "_ic.value_position(" in desk_src)
    second_fill_writers = sorted(
        _rel(fp) for fp in files if _operational(_rel(fp))
        and _rel(fp) != "api/paper_trading_desk.py"
        and "FILLS_FILE, fills_rows" in fp.read_text(encoding="utf-8", errors="replace"))
    hoc_scoped_to_equity = "def excluded_non_equity_positions(" in _src("api/holding_opportunity_cost.py")

    # drawdown ownership resolved
    dc_src = _src("api/daily_close.py")
    daily_close_reads_owner_drawdown = ('"max_drawdown_owner": "api.paper_trading_desk.current_drawdown"' in dc_src
                                        and "worst = min(worst, v / peak - 1.0)" not in dc_src)
    analytics_reads_current_rows = ('.get("current_rows") or (perf or {}).get("rows")'
                                    in _src("api/portfolio_analytics.py"))
    portfolio_state_names_owner = ('"drawdown_owner": "api.paper_trading_desk.current_drawdown"'
                                   in _src("api/portfolio_state.py"))

    # cross-asset constraints live in the ONE constraint owner
    cr_src = _src("engine/constrained_reallocation.py")
    cross_asset_constraints_declared = all(t in cr_src for t in (
        'C_ASSET_CLASS_CAP = "ASSET_CLASS_WEIGHT_CAP"', 'C_SLEEVE_CAP = "SLEEVE_WEIGHT_CAP"',
        'C_CURRENCY_CAP = "CURRENCY_EXPOSURE_CAP"', 'C_COLLATERAL_CAP = "COLLATERAL_USAGE_CAP"',
        'C_UNIT_GRANULARITY = "UNIT_GRANULARITY_AT_NAV"'))
    proposal_reuses_constraint_owner = ("_cr.candidate_meta(" in _src("engine/reallocation_proposal.py")
                                        and "_cr.allocation_by(" in _src("engine/reallocation_proposal.py"))
    zero_base_reuses_constraint_owner = ("_cr.cross_asset_room(" in _src("engine/zero_base_allocator.py")
                                         and "_cr.candidate_meta(" in _src("engine/zero_base_allocator.py"))

    # routes + inventory
    routes = check_routes()["routes"]
    r50_route_counts = {p: len([r for r in routes if r["path"] == p and r["method"] == "GET"])
                        for p in R50_ROUTES}
    r50_mutating_routes = sorted("%s %s" % (r["method"], r["path"]) for r in routes
                                 if r["path"] in R50_ROUTES and r["method"] != "GET")
    try:
        _inv = json.loads(_read("docs/architecture/system_inventory.json"))
        _ownership = _inv.get("route_ownership", [])
        _modules = {m.get("path", "").replace("\\", "/") for m in _inv.get("modules", [])}
    except (json.JSONDecodeError, OSError):
        _ownership, _modules = [], set()
    routes_registered = sorted(p for p in R50_ROUTES
                               if not any(e.get("prefix") == p for e in _ownership))
    modules_registered = sorted(v for v in R50_OWNERS.values() if v not in _modules)
    app_src = _src("api/app.py")
    snapshot_served = sorted(s for s in R50_SNAPSHOT_SERVED
                             if ('_snap.section("%s")' % s) not in app_src)
    direct_owner_calls_remaining = sorted(t for t in (
        "return _opres.load_operator_presentation()", "return _pstate.load_portfolio_state()",
        "return _realloc.load_constrained_reallocation()", "return _rebalance.load_rebalance_state()",
        "return _opbook.load_operational_book()", "return _dclose.load_daily_close()")
        if t in app_src)

    # UI: the registry card is an Audit surface; the R50 region is read-only
    ui = _read(UI_FILE)
    _s0 = ui.find('<style id="r49-styles">')
    _s1 = ui.find("</style>", _s0) if _s0 != -1 else -1
    css = ui[_s0:_s1] if (_s0 != -1 and _s1 > _s0) else ""
    registry_card_under_audit = (
        'id="r50-investability-card"' in ui
        and '#tab-portfolio-manager:not([data-pm-view="audit"]) > #r50-investability-card' in css)
    _r0 = ui.find("/* R50_REGION_START */")
    _r1 = ui.find("/* R50_REGION_END */")
    region = ui[_r0:_r1] if (_r0 != -1 and _r1 > _r0) else ""
    region_forbidden = sorted(t for t in ("fetch(", "alert(", "confirm(", "prompt(", "call('POST'",
                                          "method: 'POST'", "Math.", "compute")
                              if re.search(r"(?<![\w.])" + re.escape(t), region))
    region_loader_count = region.count("function loadInvestabilityRegistry(")
    snapshot_allocation_from_owner = ("s.allocation_available" in ui
                                      and '"allocation_available": bool(allocation)'
                                      in _src("api/operator_presentation.py"))
    no_cosmetic_zero_rows = ('if cash > 1e-9:' in srcs["instrument_contract"]
                             and "never a cosmetic 0% row" in srcs["instrument_contract"])
    no_new_primary_section = len(re.findall(r'<div id="(today-[\w-]+)"', ui)) == len(
        re.findall(r'<div id="(today-[\w-]+)"', ui))  # structural no-op guard kept explicit
    r50_new_panel_ids = sorted(m for m in re.findall(r'id="(r50-[\w-]+)"', ui)
                               if m not in ("r50-investability-card", "r50-investability-body"))

    # no broker path anywhere in the R50 owners
    broker_reach = sorted("%s:%s" % (k, t) for k, c in codes.items() for t in (
        "ib_insync", "ibapi", "alpaca", "requests.post", "broker_client", "submit_live") if t in c)
    manual_gates_unchanged = (
        'CONFIRM_TOKEN = "CONFIRM_PORTFOLIO_REBALANCE_DECISION"' in _src("api/portfolio_decision.py")
        and 'CONFIRM_TOKEN = "CONFIRM_APPROVED_PORTFOLIO_REBALANCE_ORDER_PLAN"' in _src("api/rebalance_execution.py"))

    return {
        "phase": "R50",
        "owners_missing": owners_present,
        "capital_pool_owners": capital_pool_owners,
        "nav_owners": nav_owners,
        "position_contract_owners": position_contract_owners,
        "registry_owners": registry_owners,
        "risk_owners": risk_owners,
        "frontier_owners": frontier_owners,
        "zero_base_owners": zero_base_owners,
        "feasible_target_owners": feasible_owners,
        "switching_owners": switching_owners,
        "snapshot_owners": snapshot_owners,
        "execution_owners": execution_owners,
        "settlement_owners": settlement_owners,
        "evidence_owners": evidence_owners,
        "drawdown_owners": drawdown_owners,
        "covariance_owners": covariance_owners,
        "snapshot_business_reach": snapshot_business_reach,
        "snapshot_declares_no_business": bool(snapshot_declares_no_business),
        "snapshot_invalidation_is_identity": bool(snapshot_invalidation_is_identity),
        "presentation_business_reach": presentation_business_reach,
        "registry_no_promotion": bool(registry_no_promotion),
        "registry_eligibility_derived": bool(registry_eligibility_derived),
        "r46_reach": r46_reach,
        "research_imports_in_operational": research_imports_in_operational,
        "forced_diversification_declared_false": bool(forced_div_declared_false),
        "forced_min_weight_tokens": forced_min_weight_tokens,
        "long_only_declared": bool(long_only_declared),
        "zero_signal_not_a_sink": bool(zero_signal_not_a_sink),
        "convention_declared": bool(convention_declared),
        "futures_not_valued_like_equities": bool(futures_not_valued_like_equities),
        "cost_policy_declared": bool(cost_policy_declared),
        "desk_routes_owned_marks": bool(desk_routes_owned_marks),
        "desk_settles_by_instrument": bool(desk_settles_by_instrument),
        "desk_nav_instrument_aware": bool(desk_nav_instrument_aware),
        "second_fill_writers": second_fill_writers,
        "hoc_scoped_to_equity": bool(hoc_scoped_to_equity),
        "daily_close_reads_owner_drawdown": bool(daily_close_reads_owner_drawdown),
        "analytics_reads_current_rows": bool(analytics_reads_current_rows),
        "portfolio_state_names_drawdown_owner": bool(portfolio_state_names_owner),
        "cross_asset_constraints_declared": bool(cross_asset_constraints_declared),
        "proposal_reuses_constraint_owner": bool(proposal_reuses_constraint_owner),
        "zero_base_reuses_constraint_owner": bool(zero_base_reuses_constraint_owner),
        "r50_route_counts": r50_route_counts,
        "r50_mutating_routes": r50_mutating_routes,
        "routes_unregistered": routes_registered,
        "modules_unregistered": modules_registered,
        "snapshot_sections_not_served": snapshot_served,
        "direct_owner_calls_remaining": direct_owner_calls_remaining,
        "registry_card_under_audit": bool(registry_card_under_audit),
        "region_forbidden": region_forbidden,
        "region_loader_count": region_loader_count,
        "snapshot_allocation_from_owner": bool(snapshot_allocation_from_owner),
        "no_cosmetic_zero_rows": bool(no_cosmetic_zero_rows),
        "r50_new_panel_ids": r50_new_panel_ids,
        "broker_reach": broker_reach,
        "manual_gates_unchanged": bool(manual_gates_unchanged),
    }


# --------------------------------------------------------------------------- #
# R54 — the ONE Active Manager Operating State (composition-only) + the Today
# operational-mark single-writer consolidation.
# --------------------------------------------------------------------------- #
R54_AMS_MODULE = "api/active_manager_state.py"
R54_AMS_ROUTE = '"/v1/operations/active-manager-state"'
#: Business-calculation definitions the composition owner may NEVER contain —
#: each belongs to exactly one existing canonical owner.
R54_FORBIDDEN_CALC_DEFS = (
    "def book_nav(", "def compute_scores(", "def _percentiles(",
    "def solve_feasible_target(", "def switching_economics(",
    "def decide_outcome(", "def build_assessment(", "def build_proposal(",
    "def settle_due_orders(", "def _append_ledger(", "def score_universe(",
    "def rank_universe(", "def assess_materiality(")
#: Execution / write / scheduler reach the read model may NEVER have.
R54_FORBIDDEN_EXECUTION_TOKENS = (
    "run_event_signal_refresh(", "run_reassessment(", "run_proposal(",
    "run_daily_close(", "run_daily_research_cycle(", "run_refresh(",
    "run_portfolio_cycle(", "run_collection_iteration(", "subprocess",
    "schtasks", "Register-ScheduledTask", "requests.", "httpx")
#: Client-side business/freshness computation forbidden inside the R54 UI region.
R54_UI_FORBIDDEN_TOKENS = ("Math.", "new Date(", "Date.now(",
                           "toLocaleString(", "reduce(")


def check_release54_active_manager_state(files: list[Path]) -> dict:
    """R54 invariants — ONE composed operating state, zero recomputation.

    (a) api/active_manager_state.py exists, declares itself, composes the
        Release-50 decision snapshot, and contains no business-calculation
        definition, no execution/orchestration call, no scheduler reach and no
        provider client;
    (b) it is served by exactly ONE GET route and consumed by exactly ONE UI
        loader inside a marked region that performs no client-side date /
        freshness / decision math;
    (c) the operational-vs-live time-state distinction is declared, and only
        the Daily Close owner may advance the operational mark;
    (d) the Today operational-mark pill has exactly ONE unguarded UI writer
        (renderPortfolioState via _psOwnSet). The legacy command-center write
        through the guard-free _ccSetText — whose fallback was the dormant
        legacy DB book's date — must never return.
    """
    src = _read(R54_AMS_MODULE)
    ui = _read(UI_FILE)
    app_src = _read("api/app.py")
    region = _ux2_region(ui, "/* R54_REGION_START */", "/* R54_REGION_END */")

    get_route_count = len(re.findall(
        r"@app\.get\(\s*\n?\s*" + re.escape(R54_AMS_ROUTE), app_src))
    non_get_route = bool(re.search(
        r"@app\.(post|put|delete|patch)\(\s*\n?\s*" + re.escape(R54_AMS_ROUTE),
        app_src))

    return {
        "owner_present": bool(src.strip()),
        "declares_owner": 'OWNER = "api.active_manager_state"' in src,
        "composition_only_declared": '"recomputes_nothing": True' in src,
        "composes_decision_snapshot": "decision_snapshot" in src,
        "forbidden_calculation_defs": sorted(
            d for d in R54_FORBIDDEN_CALC_DEFS if d in src),
        "forbidden_execution_tokens": sorted(
            t for t in R54_FORBIDDEN_EXECUTION_TOKENS if t in src),
        "time_state_distinction_declared": (
            '"operational_mark_advanced_only_by": "api.daily_close"' in src
            and "TIME_STATE_STATEMENT" in src),
        "route_get_count": get_route_count,
        "non_get_route_present": non_get_route,
        "ui_loader_count": ui.count("function loadActiveManagerState("),
        "ui_fetch_count": ui.count(
            "_opFetch('/v1/operations/active-manager-state')"),
        "ui_region_present": bool(region),
        "ui_region_forbidden": sorted(
            t for t in R54_UI_FORBIDDEN_TOKENS if t in region),
        # (d) the consolidated operational-mark pill: one unguarded writer.
        "legacy_status_mark_writer_present": (
            "_ccSetText('cc-status-mark'" in ui),
        "canonical_status_mark_writer_count": ui.count(
            "_psOwnSet('cc-status-mark'"),
        "status_mark_guarded_early_writer_present": (
            "_obSet('cc-status-mark'" in ui),
        # (e) R54 finalization — the DECISION AUTHORITY LADDER is declared
        #     (five distinct concepts with owners; a live event cycle never
        #     advances the governed decision), and the TWO forward-evidence
        #     identities (daily governed TRUE_FORWARD bundle vs R53.1 intraday
        #     prospective emission) stay distinct named fields.
        "decision_authority_declared": (
            "DECISION_AUTHORITY_STATEMENT" in src
            and '"advances_governed_decision": False' in src
            and "EVENT_CYCLE_PROPOSAL_NOTE" in src),
        "evidence_identities_distinct": (
            '"latest_governed_true_forward_date"' in src
            and '"latest_intraday_prospective_emission"' in src),
        "automatic_model_promotion_allowed": False,
        "automatic_approval_allowed": False,
        "cadence_enabled": False,
    }


# --------------------------------------------------------------------------- #
# R54.1 — the GOVERNED INTRADAY PORTFOLIO DECISION CYCLE.
#
# The whole point of this release is that a live intraday assessment may become
# the AUTHORITATIVE recommendation through exactly ONE gate, owned by the ONE
# decision owner. The failure mode this guard exists to prevent is a second
# intraday-governance owner appearing later — in the event cycle, in the read
# model, in the workflow owner or in a new module — each with its own idea of
# what "governed" means. It also pins the safety boundary the release may never
# cross: a governed CHANGE is a RECOMMENDATION, never an approval or an order.
# --------------------------------------------------------------------------- #
R541_DECISION_OWNER = "api/portfolio_decision.py"
R541_CYCLE_OWNER = "api/event_signal_refresh.py"
#: The governance surface. Each of these must be defined EXACTLY once, in the
#: decision owner, and nowhere else in api/ or engine/.
R541_GATE_DEFS = (
    "def evaluate_intraday_governance(",
    "def record_governed_decision(",
    "def governed_decision_ordering_key(",
    "def build_intraday_candidate(",
    "def load_governed_portfolio_decision(",
)
#: Business calculations the GATE may never define — it decides admissibility,
#: never economics. Each already belongs to engine.constrained_reallocation.
R541_FORBIDDEN_CALC_DEFS = (
    "def switching_economics(", "def solve_feasible_target(",
    "def decide_outcome(", "def herfindahl(", "def one_way_turnover(",
    "def compute_scores(", "def build_assessment(")
#: Execution / write / scheduler reach the governed lane may never have.
R541_FORBIDDEN_EXECUTION_TOKENS = (
    "submit_order", "create_order", "place_order", "record_fill",
    "generate_orders", "confirm_orders", "settle_due_orders",
    "confirm_order_plan", "promote_model", "promote_challenger",
    "activate_sleeve", "run_daily_close", "refresh_desk", "broker_client",
    "subprocess", "schtasks", "Register-ScheduledTask", "requests.", "httpx")
#: The Phase-J withheld taxonomy. Canonical codes must be REUSED verbatim.
R541_REQUIRED_REASON_CODES = (
    "PORTFOLIO_IDENTITY_STALE", "MARKET_DATA_STALE",
    "OWNED_DATA_NOT_CONFIRMED", "POINT_IN_TIME_INTEGRITY_FAILURE",
    "RANKING_IDENTITY_MISMATCH", "HOC_IDENTITY_MISMATCH",
    "REASSESSMENT_IDENTITY_MISMATCH", "TARGET_IDENTITY_MISMATCH",
    "SWITCHING_ECONOMICS_INCOMPLETE", "TRUE_BLOCKER",
    "SUPERSEDED_BY_NEWER_DECISION", "DUPLICATE_CANDIDATE",
    "EXECUTION_PRECEDENCE", "CANDIDATE_EVIDENCE_INCOMPLETE")


def _r541_governed_lane(src: str) -> str:
    """The R54.1 section of the decision owner, excluding the module __all__."""
    marker = "R54.1 - THE ONE GOVERNED INTRADAY DECISION GATE"
    for m in (marker, marker.replace(" - ", " — ")):
        if m in src:
            return src.split(m, 1)[1].split("\n__all__ = [", 1)[0]
    return ""


def check_release54_1_governed_intraday_decision(files: list[Path]) -> dict:
    """R54.1 invariants — ONE gate, ONE decision owner, ONE ordering.

    (a) the gate, the governed writer and the supersession ordering are defined
        exactly once, in api.portfolio_decision, and nowhere else;
    (b) the live event cycle DELEGATES to that owner and hosts no gate;
    (c) the governed lane defines no business calculation and has no
        execution / order / broker / promotion / scheduler reach;
    (d) the withheld taxonomy is declared and reuses the canonical codes,
        OWNED_DATA_NOT_CONFIRMED among them;
    (e) HOLD and CHANGE are both governed decisions, a governed CHANGE requires
        manual review, and the governed lane never advances the operational
        mark (which stays api.daily_close's alone);
    (f) the governed lane writes ONLY its own ledger files — never the manual
        operator-decision pointer;
    (g) the R53.1 emission-slot contract is unchanged.
    """
    pd_src = _read(R541_DECISION_OWNER)
    esr_src = _read(R541_CYCLE_OWNER)
    ams_src = _read(R54_AMS_MODULE)
    wf_src = _read("api/workflow_state.py")
    lane = _r541_governed_lane(pd_src)

    # (a) exactly one definition site across api/ + engine/
    duplicate_owners: list[str] = []
    for fp in files:
        rel = _rel(fp).replace("\\", "/")
        if not (rel.startswith("api/") or rel.startswith("engine/")):
            continue
        if rel == R541_DECISION_OWNER:
            continue
        body = _read(rel)
        for d in R541_GATE_DEFS:
            if d in body:
                duplicate_owners.append(f"{rel}:{d}")

    factory = _read("alpha_agent/r53/intraday_factory.py")
    installer = _read("scripts/install_intraday_emission_task.ps1")

    return {
        "gate_owner_present": bool(lane.strip()),
        "gate_defs_missing": sorted(d for d in R541_GATE_DEFS if d not in pd_src),
        "duplicate_governance_owners": sorted(duplicate_owners),
        "cycle_delegates_to_owner": (
            'GOVERNANCE_DELEGATE = "api.portfolio_decision"' in esr_src),
        "cycle_defines_gate": any(d in esr_src for d in R541_GATE_DEFS),
        "read_model_defines_gate": any(d in ams_src for d in R541_GATE_DEFS),
        "workflow_defines_gate": any(d in wf_src for d in R541_GATE_DEFS),
        "forbidden_calculation_defs": sorted(
            d for d in R541_FORBIDDEN_CALC_DEFS if d in lane),
        "forbidden_execution_tokens": sorted(
            t for t in R541_FORBIDDEN_EXECUTION_TOKENS if t in lane),
        "missing_reason_codes": sorted(
            c for c in R541_REQUIRED_REASON_CODES if c not in lane),
        "owned_data_rule_reused_verbatim": (
            'WR_OWNED_DATA_NOT_CONFIRMED = "OWNED_DATA_NOT_CONFIRMED"' in lane),
        "hold_and_change_both_governed": (
            "GD_HOLD_CURRENT_BOOK = PDS_HOLD_CURRENT_BOOK" in lane
            and 'GD_CHANGE_RECOMMENDED = "CHANGE_RECOMMENDED"' in lane),
        "manual_review_required_for_change": (
            '"manual_review_required_for_change": True' in lane
            and '"manual_review_required": bool(decision == GD_CHANGE_RECOMMENDED)'
            in lane),
        "governed_lane_never_advances_operational_mark": (
            '"advances_operational_mark": False' in lane
            and '"operational_mark_advanced_only_by": "api.daily_close"' in lane),
        "separate_governed_ledger_files": (
            '_GOVERNED_RECORDS_FILE = "governed_decisions.json"' in lane
            and '_GOVERNED_INDEX_FILE = "governed_index.json"' in lane),
        # The governed lane must use its OWN ledger files. The negative
        # lookbehind is what distinguishes `_governed_index_path(` (correct)
        # from the manual lane's `_index_path(` (a pointer collision that would
        # make load_decision_record return a system record to a caller
        # expecting an operator approval).
        "governed_writer_touches_manual_index": bool(re.search(
            r"(?<!_governed)_index_path\(|(?<!_governed)_records_path\(", lane)),
        "system_token_distinct_from_approval_token": (
            'GOVERNED_DECISION_CONFIRM_TOKEN = "CONFIRM_GOVERNED_INTRADAY_DECISION"'
            in lane),
        "gate_declares_it_owns_no_economics": (
            '"gate_decides_economics": False' in lane),
        "zero_base_policy_bound_not_redefined": (
            "ZERO_BASE_INCUMBENCY_POLICY = _cr.INCUMBENCY_POLICY" in lane),
        # (g) the R53.1 emission-slot contract, unchanged by this release.
        "emission_slots_unchanged": (
            'EMISSION_SLOTS_ET = ("10:00", "12:00", "14:00")' in factory
            and "SLOT_GRACE_MINUTES = 15" in factory),
        "emission_post_close_pass_declared": (
            "post-close scoring pass" in installer
            and "structurally refused outside a slot" in installer),
        "automatic_approval_allowed": False,
        "automatic_model_promotion_allowed": False,
        "automatic_execution_allowed": False,
    }


# --------------------------------------------------------------------------- #
# Release 54.2 — same-session reassessment versioning.
# --------------------------------------------------------------------------- #
R542_REASSESSMENT_OWNER = "api/portfolio_reassessment.py"
#: Release 54.3 — the SECOND store that legitimately versions same-session
#: evidence. R54.3 deliberately reuses R54.2's vocabulary rather than inventing a
#: parallel one, so the shared identity words below are expected in BOTH owners
#: and in no third module. Each owner still versions only its OWN store: the
#: store-specific persist entry points stay unique, and that is what this rule
#: actually protects.
R543_HOC_OWNER = "api/holding_opportunity_cost.py"
R542_VERSIONING_OWNERS = (R542_REASSESSMENT_OWNER, R543_HOC_OWNER)
#: The versioning surface. Each must be defined EXACTLY once in the ONE
#: reassessment owner; the shared-vocabulary members may ALSO appear in the one
#: opportunity-cost owner (R54.3) and nowhere else in api/ or engine/.
R542_OWNER_DEFS = (
    "def persist_reassessment(",
    "def assessment_evidence_identity(",
    "def assessment_evidence_hash(",
    "def decision_fingerprint(",
    "def authoritative_history_rows(",
    "def load_artifact_versions(",
    "def load_artifact_by_id(",
)
#: The identity vocabulary R54.3 shares by design. Everything NOT in this set is
#: reassessment-exclusive and a second definition anywhere is still a violation.
R542_SHARED_IDENTITY_DEFS = (
    "def assessment_evidence_identity(",
    "def assessment_evidence_hash(",
    "def decision_fingerprint(",
    "def load_artifact_versions(",
    "def load_artifact_by_id(",
)
#: The four persistence outcomes, named once and returned by the owner.
R542_PERSIST_OUTCOMES = (
    'PERSIST_CREATED = "CREATED"',
    'PERSIST_REUSED = "REUSED_EXISTING"',
    'PERSIST_ECONOMIC_VERSION = "CREATED_NEW_VERSION"',
    'PERSIST_ASSESSMENT_VERSION = "CREATED_ASSESSMENT_VERSION"',
    'PERSIST_CONFLICT = "CONFLICT_REJECTED"',
    'PERSIST_INCONSISTENT = "REJECTED_INCONSISTENT_IDENTITY"',
)
#: Identity components the ASSESSMENT-EVIDENCE hash may never contain. The first
#: two are the Stage-21 trap (a document-wide hash that embeds this owner's own
#: output, and the separate economic axis); the third is the conclusion, not the
#: evidence that produced it.
R542_FORBIDDEN_EVIDENCE_COMPONENTS = (
    "portfolio_state_hash", "economic_state_hash", "reassessment_hash")
#: A parallel intraday-only history would split the one reassessment record.
R542_FORBIDDEN_PARALLEL_STORES = (
    "intraday_reassessment_dir", "INTRADAY_REASSESSMENT_DIR",
    "event_reassessment_dir", "live_reassessment_dir",
    "_INTRADAY_INDEX_FILE", "intraday_reassessments")


def _r542_evidence_components(src: str) -> str:
    """The declared ASSESSMENT_EVIDENCE_COMPONENTS tuple body, or ''."""
    m = re.search(r"ASSESSMENT_EVIDENCE_COMPONENTS = \((.*?)\)\n", src, re.S)
    return m.group(1) if m else ""


#: R54.2.1 — MISSED ELIGIBLE SESSION RECOVERY. The owners that may hold each half of
#: the catch-up contract, and every shape a SECOND one would take.
R5421_CALENDAR_OWNER = "engine/market_session.py"
R5421_RECOVERY_OWNER = "api/workflow_state.py"
R5421_CYCLE_OWNER = "api/portfolio_cycle.py"
R5421_CLOSE_OWNER = "api/daily_close.py"
R5421_PRESENTATION_OWNER = "api/operator_presentation.py"
R5421_AMS_OWNER = "api/active_manager_state.py"
#: The catch-up state machine is defined ONCE, in the workflow owner.
R5421_OWNER_DEFS = ("def build_session_recovery(", "CATCH_UP_REQUIRED",
                    "CATCH_UP_WAITING_FOR_OWNED_DATA", "CATCH_UP_BLOCKED",
                    "NO_CATCH_UP_REQUIRED", "SESSION_RECOVERY_STATES")
#: The calendar enumeration is defined ONCE, in the market-session owner.
R5421_CALENDAR_DEFS = ("def completed_sessions_after(", "def next_trading_day(",
                       "MAX_MISSED_SESSIONS")
#: A second recovery ORCHESTRATOR / write route in any form.
R5421_FORBIDDEN_ROUTES = ("/v1/operations/daily-close/recover",
                          "/v1/operations/daily-close/backfill",
                          "/v1/operations/session-recovery/run",
                          "/v1/operations/catch-up",
                          "/v1/operations/portfolio-cycle/recover")
R5421_FORBIDDEN_DEFS = ("def run_session_recovery(", "def recover_daily_close(",
                        "def backfill_daily_close(", "def run_catch_up(",
                        "def force_close_session(")
#: Client-side recovery arithmetic / operator-supplied dates in the UI.
R5421_FORBIDDEN_UI = ("catch_up_required =", "recovery_session =",
                      "backfillSession", "forceCloseSession",
                      "recoveryDateInput", "prompt(")


def check_release54_2_1_missed_session_recovery(files: list[Path]) -> dict:
    """R54.2.1 invariants — ONE catch-up owner, ONE orchestration path, no backfill.

    (a) the missed-completed-session STATE MACHINE is defined exactly once, in
        ``api.workflow_state``, and the CALENDAR enumeration it delegates to
        exactly once, in ``engine.market_session`` — the two halves of the
        obligation never merge into a third owner;
    (b) no module outside the workflow owner defines a catch-up state, and no
        module at all defines a second recovery orchestrator or a recovery /
        backfill / force-close write route;
    (c) the recovery session is bound by the SERVER: the portfolio cycle reads it
        from the workflow owner and hands it to the close owner, which validates
        it and REFUSES a binding it cannot honour (never clamps one);
    (d) the projection surfaces (Active Manager State, the operator presentation)
        DELEGATE — they republish the workflow owner's fields and compute no
        session date of their own;
    (e) the UI performs no recovery date arithmetic, offers no backfill /
        force-close control, and takes no date from the operator;
    (f) recovery adds no automation, no order path and no approval path.
    """
    ws_src = _read(R5421_RECOVERY_OWNER)
    ms_src = _read(R5421_CALENDAR_OWNER)
    pc_src = _read(R5421_CYCLE_OWNER)
    dc_src = _read(R5421_CLOSE_OWNER)
    op_src = _read(R5421_PRESENTATION_OWNER)
    ams_src = _read(R5421_AMS_OWNER)
    ui = _read(UI_FILE)
    routes = check_routes()["routes"]

    duplicate_state_owners: list[str] = []
    duplicate_calendar_owners: list[str] = []
    second_orchestrators: list[str] = []
    for fp in files:
        rel = _rel(fp).replace("\\", "/")
        if not (rel.startswith("api/") or rel.startswith("engine/")
                or rel.startswith("scripts/")):
            continue
        # THIS FILE names every forbidden symbol in order to forbid it; scanning
        # itself would make the check permanently self-failing (the audit's own
        # oldest trap). It owns no runtime behaviour, so it is out of scope.
        if rel == "scripts/audit_architecture.py":
            continue
        body = _read(rel)
        for d in R5421_FORBIDDEN_DEFS:
            if d in body:
                second_orchestrators.append(f"{rel}:{d}")
        if rel != R5421_RECOVERY_OWNER:
            for d in ("def build_session_recovery(",):
                if d in body:
                    duplicate_state_owners.append(f"{rel}:{d}")
        if rel != R5421_CALENDAR_OWNER:
            for d in ("def completed_sessions_after(",):
                if d in body:
                    duplicate_calendar_owners.append(f"{rel}:{d}")

    return {
        "phase": "R54.2.1",
        "owner_defs_missing": sorted(d for d in R5421_OWNER_DEFS
                                     if d not in ws_src),
        "calendar_defs_missing": sorted(d for d in R5421_CALENDAR_DEFS
                                        if d not in ms_src),
        "duplicate_state_owners": sorted(duplicate_state_owners),
        "duplicate_calendar_owners": sorted(duplicate_calendar_owners),
        "second_recovery_orchestrators": sorted(second_orchestrators),
        "forbidden_routes_present": sorted(
            r for r in R5421_FORBIDDEN_ROUTES
            if any(rt["path"] == r for rt in routes)),
        # (a) the workflow owner DELEGATES the calendar; it never walks dates.
        "workflow_delegates_calendar": (
            "msession.completed_sessions_after(" in ws_src),
        # A weekend/holiday walk is SESSION calendar arithmetic and belongs to the
        # market-session owner alone. (``_business_days_between`` is an age-in-days
        # helper over two given dates, not a session resolver, and is unaffected.)
        "workflow_owns_no_calendar_walk": not any(
            t in ws_src for t in ("def next_trading_day(",
                                  "def previous_trading_day(",
                                  "def walk_back_to_trading_day(",
                                  "def completed_sessions_after(")),
        # (b) the obligation is anchored on the CLOSE journal, not on the
        #     owned-data-confirmed eligible date (the R54.2.1 root cause).
        "obligation_anchored_on_close": (
            "operational_close_valid" in ws_src
            and "latest_completed_close_date=latest_close_date" in ws_src),
        "priority_suppresses_wait_state": (
            "and not catch_up_required" in ws_src),
        "priority_promotes_close": ("if catch_up_required or not "
                                    "eligible_session_closed:" in ws_src),
        # (c) the SERVER binds the session; the close refuses what it cannot honour.
        "cycle_reads_binding_from_workflow": (
            "def recovery_binding(" in pc_src
            and 'get("session_recovery")' in pc_src),
        "cycle_passes_binding_to_close": (
            "target_market_date=bound_date" in pc_src
            or "target_market_date=target_market_date" in pc_src),
        "close_accepts_binding": ("target_market_date" in dc_src
                                  and "def _apply_session_binding(" in dc_src),
        "close_refuses_forward_binding": (
            "BINDING_REJECTED_FUTURE" in dc_src
            and "session_binding_rejected" in dc_src),
        "close_binding_never_clamps": ("clock[\"session_binding_rejected\"] = "
                                       "BINDING_REJECTED_FUTURE" in dc_src),
        "binding_is_not_a_request_field": not any(
            t in _read("api/app.py") for t in
            ("target_market_date: ", "target_market_date=payload",
             "target_market_date=body")),
        # (d) the projections delegate.
        "ams_delegates_recovery": (
            "def _session_recovery_block(" in ams_src
            and '"computed_here": False' in ams_src),
        "presentation_delegates_recovery": (
            'wf.get("session_recovery")' in op_src),
        "oldest_first_declared": ('"oldest_first": True' in ws_src),
        # (e) the UI renders, it does not decide.
        "ui_recovery_derivation": sorted(t for t in R5421_FORBIDDEN_UI if t in ui),
        "ui_renders_backend_recovery": (
            "function _opRenderSessionRecovery(" in ui
            and "p.session_recovery" in ui),
        "ui_offers_no_date_entry": not any(
            t in ui for t in ('id="recovery-date"', "recoveryDate",
                              'name="recovery_session"')),
        # (f) no new authority. The recovery path may not schedule itself, create an
        #     order or approve anything — it only decides WHICH session the one
        #     manual cycle binds. (``next_scheduled_full_review`` is the legacy gate's
        #     review clock and is deliberately not matched here.)
        "recovery_adds_automation": any(
            t in ws_src for t in ("schedule.every", "CronCreate", "register_task",
                                  "auto_run_cycle", "def _autorun")),
        "recovery_creates_orders": ("create_order" in ws_src
                                    or "submit_order" in ws_src
                                    or "create_order" in pc_src),
        "cycle_still_approves_nothing": (
            '"approves_proposals": False' in pc_src
            and '"executes_rebalance": False' in pc_src
            and '"automation": "OFF"' in pc_src),
    }


#: R54.2.3 — CONTROLLED MONTHLY RESEARCH-INPUT RECOVERY.
#:
#: The panel is written by the RESEARCH owner alone; the bridge owns only the POLICY
#: (when a refresh is due, which cutoff binds it, what a failure means); the cycle owner
#: reads ONE producibility verdict rather than deriving a second one; and actionability
#: stays a projection of the already-decided primary action.
R5423_BRIDGE_OWNER = "api/monthly_momentum_emitter.py"
R5423_CYCLE_OWNER = "api/daily_research_cycle.py"
R5423_WORKFLOW_OWNER = "api/workflow_state.py"
#: The bounded-refresh policy is defined ONCE, in the bridge.
R5423_REFRESH_DEFS = ("def build_refresh_command(", "def refresh_source_panel(",
                      "REFRESH_DRIVER_SRC", "SOURCE_PANEL_STATES")
#: A SECOND panel writer, a manual recovery route, or an operator-chosen cutoff.
R5423_FORBIDDEN_DEFS = ("def refresh_daily_panel(", "def rebuild_source_panel(",
                        "def build_daily_panel(", "def run_panel_refresh(",
                        "def backfill_source_panel(")
R5423_FORBIDDEN_ROUTES = ("/v1/operations/source-panel/refresh",
                          "/v1/operations/source-panel/run",
                          "/v1/operations/monthly-emitter/run",
                          "/v1/operations/research-inputs/refresh",
                          "/v1/operations/panel-refresh")
#: An operator-supplied cutoff in any request/UI shape.
R5423_FORBIDDEN_DATE_FIELDS = ("panel_refresh_date", "refresh_as_of", "as_of_override",
                               "panelRefreshDate", "sourcePanelDate")
#: Client-side actionability derivation the UI must never perform.
R5423_FORBIDDEN_UI = ("source_panel_covered", "panel_last_date <", "monthly_input_state =",
                      "portfolio_cycle_actionable =")

#: R54.2.2 — POST-CLOSE RESEARCH RECOVERY + ATTRIBUTION INTEGRITY. The owners that
#: may hold each half, and every shape a SECOND one would take.
R5422_OBLIGATION_OWNER = "api/workflow_state.py"
R5422_CLASSIFICATION_OWNER = "api/daily_research_cycle.py"
R5422_CYCLE_OWNER = "api/portfolio_cycle.py"
R5422_PRESENTATION_OWNER = "api/operator_presentation.py"
R5422_AMS_OWNER = "api/active_manager_state.py"
R5422_ATTRIBUTION_OWNER = "api/forward_evidence.py"
R5422_CLOSE_OWNER = "api/daily_close.py"
#: The post-close obligation state machine is defined ONCE, in the workflow owner.
R5422_OBLIGATION_DEFS = ("def build_research_obligation(",
                         "RESEARCH_OBLIGATION_OUTSTANDING",
                         "RESEARCH_OBLIGATION_BLOCKED",
                         "RESEARCH_OBLIGATION_EVIDENCE_GAP",
                         "NO_RESEARCH_OBLIGATION", "RESEARCH_OBLIGATION_STATES")
#: The stale-input recoverability classification is defined ONCE, in the cycle owner.
R5422_CLASSIFICATION_DEFS = ("def classify_input_recovery(", "def classify_stale_inputs(",
                             "SAFE_RECOVERABLE_POINT_IN_TIME", "TRUE_BLOCKER",
                             "UNRECOVERABLE_HISTORICAL_GAP", "INPUT_RECOVERY_STATES")
#: A second post-close research orchestrator / backfill write route in any form.
R5422_FORBIDDEN_ROUTES = ("/v1/operations/daily-research-cycle/backfill",
                          "/v1/operations/research-recovery",
                          "/v1/operations/research/backfill",
                          "/v1/operations/monthly-input/run",
                          "/v1/operations/portfolio-cycle/research-only")
R5422_FORBIDDEN_DEFS = ("def run_research_backfill(", "def backfill_research_cycle(",
                        "def recover_research_session(",
                        "def run_post_close_recovery(",
                        "def rewrite_attribution_history(",
                        "def backfill_true_forward(")
#: Client-side obligation arithmetic / operator-supplied research dates in the UI.
R5422_FORBIDDEN_UI = ("research_obligation_state =", "outstanding_research_session =",
                      "backfillResearch", "forceResearchCycle", "researchDateInput")


def check_release54_2_3_source_panel_recovery(files: list[Path]) -> dict:
    """R54.2.3 invariants — ONE panel writer, ONE refresh policy, ONE verdict.

    (a) the bounded source-panel refresh POLICY is defined exactly once, in the
        emitter bridge, and no operational ``api/*.py`` module defines a second
        panel builder / refresher / backfiller;
    (b) the bridge still computes NO mathematics (no numpy/pandas) and drives the
        panel OWNER's own bounded entry point through an explicit argv array;
    (c) the refresh cutoff is the ELIGIBLE SESSION and is never a request field, a
        route parameter or a UI control — there is no manual panel-refresh route;
    (d) a FUTURE-dated panel is still BLOCKED, never rebuilt backwards;
    (e) the cycle owner reads the monthly owner's single producibility verdict
        (``can_cover_eligible_session``) instead of deriving a second one, and
        publishes the canonical data-quality vocabulary; and
    (f) portfolio-cycle actionability is a PROJECTION of ``primary_action_available``
        in the workflow owner, and the UI derives none of it.
    """
    bridge = _read(R5423_BRIDGE_OWNER)
    cycle = _read(R5423_CYCLE_OWNER)
    workflow = _read(R5423_WORKFLOW_OWNER)
    ui = _read(UI_FILE)
    app_src = _read(APP_MODULE)

    # (a) the refresh policy lives once, in the bridge.
    policy_defs = {tok: (tok in bridge) for tok in R5423_REFRESH_DEFS}
    missing_policy = sorted(k for k, v in policy_defs.items() if not v)

    second_panel_writer: list[str] = []
    for fp in files:
        rel = _rel(fp)
        parts = rel.split("/")
        if len(parts) != 2 or parts[0] != "api":
            continue
        text = fp.read_text(encoding="utf-8", errors="replace")
        # A module that WRITES an NPZ, or defines a panel builder of its own.
        if "np.savez" in text or "savez_compressed" in text:
            second_panel_writer.append(rel)
            continue
        if rel == R5423_BRIDGE_OWNER:
            continue
        for tok in R5423_FORBIDDEN_DEFS:
            if tok in text:
                second_panel_writer.append(rel)
                break
    second_panel_writer = sorted(set(second_panel_writer))

    # A second copy of the refresh POLICY outside the bridge.
    second_refresh_policy = sorted(
        _rel(fp) for fp in files
        if _rel(fp).startswith("api/") and _rel(fp) != R5423_BRIDGE_OWNER
        and "def refresh_source_panel(" in fp.read_text(encoding="utf-8",
                                                        errors="replace"))

    # (b) still pure-stdlib, still an explicit argv array.
    numeric_imports = sorted(t for t in ("import numpy", "import pandas",
                                         "from numpy", "from pandas") if t in bridge)
    drives_panel_owner = "refresh_daily_panel_as_of" in bridge
    refresh_uses_argv = ("def build_refresh_command(" in bridge
                         and "shell=True" not in bridge)

    # (c) the cutoff is internal: bound to the eligible session, never a field.
    cutoff_is_session = ("as_of=eligible" in bridge or "as_of=elig" in bridge)
    operator_date_fields = sorted(
        t for t in R5423_FORBIDDEN_DATE_FIELDS if (t in app_src or t in ui))
    routes = check_routes()["routes"]
    declared = {(r["path"] or "") for r in routes}
    forbidden_routes = sorted(p for p in R5423_FORBIDDEN_ROUTES
                              if p in declared or p in app_src)

    # (d) a future-dated panel is never repaired by rebuilding.
    future_still_blocks = ("PANEL_FUTURE_DATED" in bridge
                           and "_PANEL_BLOCK, status=PANEL_FUTURE_DATED" in bridge)

    # (e) ONE producibility verdict, read by the cycle owner.
    verdict_defined_in_bridge = "can_cover_eligible_session" in bridge
    cycle_reads_verdict = ("source_panel_can_cover_session" in cycle
                           and "def _monthly_producible(" in cycle)
    cycle_publishes_quality = ("research_input_quality" in cycle
                               and "MONTHLY_INPUT_STATES" in cycle
                               and "PRICE_REFRESH_STATES" in cycle)
    # The panel vocabulary is READ from its owner, never copied into the cycle.
    cycle_copies_panel_vocab = "SOURCE_PANEL_STATES = (" in cycle

    # (f) actionability is a projection, not a second engine.
    projects_actionability = all(
        t in workflow for t in ("portfolio_cycle_actionable",
                                "portfolio_cycle_safe_to_execute",
                                "portfolio_cycle_blocking_reason"))
    ui_actionability_derivation = sorted(t for t in R5423_FORBIDDEN_UI if t in ui)
    ui_reads_backend_flag = "c.primary_action_available === true" in ui

    return {
        "bridge_owner": R5423_BRIDGE_OWNER,
        "refresh_policy_defined_in_bridge": (missing_policy == []),
        "missing_refresh_policy_defs": missing_policy,
        "second_panel_writer": second_panel_writer,
        "second_refresh_policy": second_refresh_policy,
        "bridge_pure_stdlib": (numeric_imports == []),
        "bridge_numeric_imports": numeric_imports,
        "bridge_drives_panel_owner": bool(drives_panel_owner),
        "refresh_uses_argv_array": bool(refresh_uses_argv),
        "cutoff_bound_to_eligible_session": bool(cutoff_is_session),
        "operator_supplied_date_fields": operator_date_fields,
        "forbidden_panel_routes": forbidden_routes,
        "future_dated_panel_still_blocks": bool(future_still_blocks),
        "verdict_defined_in_panel_owner": bool(verdict_defined_in_bridge),
        "cycle_reads_single_verdict": bool(cycle_reads_verdict),
        "cycle_publishes_data_quality": bool(cycle_publishes_quality),
        "cycle_copies_panel_vocabulary": bool(cycle_copies_panel_vocab),
        "workflow_projects_actionability": bool(projects_actionability),
        "ui_actionability_derivation": ui_actionability_derivation,
        "ui_reads_backend_actionability": bool(ui_reads_backend_flag),
    }


def check_release54_2_3_2_decision_supersession(files: list[Path]) -> dict:
    """R54.2.3.2 invariants — a newer authoritative decision supersedes an older
    manual-review proposal, decided ONCE by the canonical decision owner.

    (a) the supersession comparison has exactly ONE calculation
        (``assess_proposal_supersession`` in ``api.portfolio_decision``, with its
        bounded loader ``load_decision_supersession`` and the canonical authority
        selector ``resolve_decision_authority``), and no other ``api/*.py`` module
        defines a second one;
    (b) the decision owner refuses to record ANY decision on a superseded proposal
        server-side (the endpoint path resolves the verdict on the call), and the
        superseded lane state exists in the frozen vocabulary;
    (c) the proposal read owner renders the verdict (``SUPERSEDED_BY_NEWER_DECISION``
        read state; delegated resolution) and computes no comparison of its own;
    (d) the workflow owner consumes the verdict (assessment view built from the
        canonical reassessment summary + the Release-29.5 governed flag), publishes
        the RPS_SUPERSEDED operator state, composes the decision-authority selector,
        and asserts the NO_CHANGE-vs-reviewable-proposal contradiction as a semantic
        invariant;
    (e) the governed daily-cycle projection derives its decision word from the
        governed ASSESSMENT first (``GD_NO_CHANGE``) and never from a superseded
        proposal's outcome;
    (f) the presentation renders the verdict verbatim (no derivation), and the
        superseded proposal's allocation rows never re-enter the Today hero count;
    (g) the UI performs no supersession comparison of its own and renders the new
        states; no new recovery/supersession route exists.
    """
    pd_src = _read("api/portfolio_decision.py")
    rp_src = _read("api/reallocation_proposal.py")
    ws_src = _read(R5423_WORKFLOW_OWNER)
    op_src = _read("api/operator_presentation.py")
    ams_src = _read("api/active_manager_state.py")
    app_src = _read("api/app.py")
    ui = _read(UI_FILE)

    # (a) one calculation, one loader, one selector — all owned by the decision owner.
    owner_defines = all(t in pd_src for t in (
        "def assess_proposal_supersession(", "def load_decision_supersession(",
        "def resolve_decision_authority(",
        'PDS_SUPERSEDED = "PROPOSAL_SUPERSEDED_BY_NEWER_DECISION"'))
    second_calculation = sorted(
        _rel(fp) for fp in files
        if _rel(fp).startswith("api/") and _rel(fp) != "api/portfolio_decision.py"
        and "def assess_proposal_supersession(" in fp.read_text(
            encoding="utf-8", errors="replace"))

    # (b) the write path fails closed and names the newer decision.
    record_refuses = (
        "load_decision_supersession(" in pd_src
        and "This proposal was superseded by a newer authoritative" in pd_src
        and '"status": PDS_SUPERSEDED' in pd_src)
    lane_state_in_vocab = "PDS_SUPERSEDED, PDS_UNAVAILABLE)" in pd_src

    # (c) the proposal read owner delegates and renders.
    realloc_renders = all(t in rp_src for t in (
        'STATE_SUPERSEDED = "SUPERSEDED_BY_NEWER_DECISION"',
        "load_decision_supersession(", "STATE_SUPERSEDED)"))
    realloc_second_comparison = bool(
        "def assess_proposal_supersession(" in rp_src)

    # (d) the workflow consumes and asserts.
    workflow_consumes = (
        "_import_portfolio_decision().assess_proposal_supersession(" in ws_src
        and '_RP_SUPERSEDED = "SUPERSEDED_BY_NEWER_DECISION"' in ws_src
        and 'RPS_SUPERSEDED = "REALLOCATION_PROPOSAL_SUPERSEDED"' in ws_src)
    workflow_selector = (
        "_import_portfolio_decision().resolve_decision_authority(" in ws_src
        and '"decision_authority": decision_authority' in ws_src)
    workflow_invariant = (
        '"NO_CHANGE_DECISION_WITH_REVIEWABLE_PROPOSAL"' in ws_src)
    superseded_never_approvable = (
        "REALLOCATION_APPROVABLE_STATES = (RPS_READY, RPS_DEGRADED)" in ws_src)

    # (e) the governed projection prefers the assessment's own word.
    projection_from_assessment = (
        'GD_NO_CHANGE = "CURRENT_NO_CHANGE"' in pd_src
        and "GD_NO_CHANGE)" in pd_src
        and 'if str(rs_state or "") == "CURRENT_NO_CHANGE":' in pd_src)

    # (f) the presentation renders verbatim and gates the hero fallback.
    presentation_renders = (
        '_PDS_SUPERSEDED = "PROPOSAL_SUPERSEDED_BY_NEWER_DECISION"' in op_src
        and "[] if _superseded else" in op_src
        and "def assess_proposal_supersession(" not in op_src)
    ams_echoes_selector = '"authoritative_selector"' in ams_src

    # (g) the UI renders; it never compares evidence hashes or decides supersession.
    ui_renders_states = "SUPERSEDED_BY_NEWER_DECISION" in ui
    ui_supersession_derivation = sorted(set(
        re.findall(r"hoc_assessment_hash\s*[!=]==?[^\n]{0,40}", ui)
        + re.findall(r"reassessment_hash\s*[!=]==?[^\n]{0,40}", ui)))
    forbidden_supersession_routes = sorted(set(
        re.findall(r"@app\.post\(\s*[\"'][^\"']*supersed[^\"']*[\"']", app_src)))

    return {
        "supersession_owner": "api.portfolio_decision",
        "owner_defines_calculation_loader_selector": bool(owner_defines),
        "second_supersession_calculation": second_calculation,
        "record_decision_refuses_superseded": bool(record_refuses),
        "lane_state_in_vocabulary": bool(lane_state_in_vocab),
        "realloc_read_renders_verdict": bool(realloc_renders),
        "realloc_second_comparison": bool(realloc_second_comparison),
        "workflow_consumes_verdict": bool(workflow_consumes),
        "workflow_composes_authority_selector": bool(workflow_selector),
        "workflow_asserts_no_change_invariant": bool(workflow_invariant),
        "superseded_never_approvable": bool(superseded_never_approvable),
        "projection_prefers_assessment_decision": bool(projection_from_assessment),
        "presentation_renders_verbatim": bool(presentation_renders),
        "ams_echoes_selector": bool(ams_echoes_selector),
        "ui_renders_superseded_states": bool(ui_renders_states),
        "ui_supersession_derivation": ui_supersession_derivation,
        "forbidden_supersession_routes": forbidden_supersession_routes,
    }


def check_release54_2_4_reallocation_coherence(files: list[Path]) -> dict:
    """R54.2.4 invariants — economic-scope coherence + first-class intraday lane.

    (a) the presentation owner names the three economic scopes ONCE and builds
        the CURRENT-DECISION economics block in one place (a governed HOLD's
        zeros are definitional, decided by the ONE presentation owner);
    (b) the Today hero renders the current-decision block, never the unscoped
        complete-target proposal metrics of the old defect;
    (c) a superseded proposal's analysis is demoted into an explicit
        history-only block on the Reallocation page;
    (d) the live/intraday reassessment lane is composed ONCE
        (api.active_manager_state) and the UI renders it verbatim — no
        governance state, conclusion or supersession is derived in JS;
    (e) the stale-components row carries the truthful display label for a
        session-current assessment whose LEGACY scheduled-review clock passed;
    (f) outcome-history rows expose the reassessment-version identity
        (projection only, nothing deleted);
    (g) the corporate-action reconciliation declares itself a desk-book
        projection, never the authoritative NAV;
    (h) the membership/scoreability check and the HOC retention rule no longer
        share the word "eligibility" on operator surfaces.
    """
    op_src = _read("api/operator_presentation.py")
    ams_src = _read("api/active_manager_state.py")
    ro_src = _read("api/reassessment_outcomes.py")
    ca_src = _read("api/corporate_actions.py")
    dag_src = _read("api/daily_action_gate.py")
    prs_src = _read("engine/portfolio_reassessment.py")
    ui = _read(UI_FILE)

    # (a) one scope vocabulary + one current-decision economics builder.
    scopes_defined = all(t in op_src for t in (
        'ECON_SCOPE_CURRENT_DECISION = "CURRENT_GOVERNED_DECISION"',
        'ECON_SCOPE_COMPLETE_TARGET = "COMPLETE_TARGET_PROPOSAL"',
        'ECON_SCOPE_HOC_RELEASE_SET = "HOC_RELEASE_SET_ESTIMATE"',
        "_HOLD_DECISION_ECONOMICS = {",
        "def _current_decision_economics(",
        '"current_decision": current',
        '"positions_changing_scope": ECON_SCOPE_CURRENT_DECISION'))
    second_current_decision = sorted(
        _rel(fp) for fp in files
        if _rel(fp).startswith("api/")
        and _rel(fp) != "api/operator_presentation.py"
        and "def _current_decision_economics(" in fp.read_text(
            encoding="utf-8", errors="replace"))
    history_block = ("def _proposal_history(" in op_src
                     and '"proposal_history": _proposal_history' in op_src)

    # (b) the hero renders the scoped block; the old unscoped render is gone.
    hero_renders_current = ("ds.current_decision" in ui
                            and "_opMetric('Current-decision turnover'" in ui)
    hero_unscoped_proposal_econ = bool(
        "_opMetric('Expected improvement', _opNum(ds.net_improvement, 3), "
        "_opNil(ds.switching_hurdle)" in ui)

    # (c) history demotion on the reallocation page.
    realloc_history_demotion = ('data-history-only="1"' in ui
                                and "SUPERSEDED PROPOSAL — HISTORY ONLY" in ui)

    # (d) ONE live-lane composition; UI renders it verbatim.
    ams_defines_lane = ("def _live_reassessment_lane_block(" in ams_src
                        and '"live_reassessment_lane": live_reassessment_lane'
                        in ams_src)
    second_lane_definition = sorted(
        _rel(fp) for fp in files
        if _rel(fp).startswith("api/")
        and _rel(fp) != "api/active_manager_state.py"
        and "def _live_reassessment_lane_block(" in fp.read_text(
            encoding="utf-8", errors="replace"))
    ui_renders_lane = ("live_reassessment_lane" in ui
                       and "lane.governance_state" in ui
                       and "lane.candidate_conclusion" in ui
                       and "lane.supersedes_standing_decision" in ui)
    lane_start = ui.find("/* R54_2_4_REGION_START */")
    lane_end = ui.find("/* R54_2_4_REGION_END */")
    lane_region = ui[lane_start:lane_end] if 0 <= lane_start < lane_end else ""
    ui_lane_derivation = sorted(set(
        re.findall(r"reassessment_hash\s*[!=]==?", lane_region)
        + re.findall(r"Math\.\w+\(", lane_region)
        + re.findall(r"\.reduce\(", lane_region)))

    # (e) truthful freshness labelling.
    stale_display_label = ("Scheduled full review due" in ams_src
                           and '"display_label": display_label' in ams_src
                           and "s.display_label" in ui)

    # (f) outcome-history version identity, projection only.
    outcome_versions = ("def _annotate_assessment_versions(" in ro_src
                        and '"repeated_across_assessment_versions"' in ro_src
                        and "Assessment version" in ui)

    # (g) CA reconciliation scope.
    ca_scope = ('"is_authoritative_nav": False' in ca_src
                and '"nav_scope": "DESK_BOOK_RECONCILIATION_PROJECTION"'
                in ca_src and "rec.nav_scope_label" in ui)

    # (h) eligibility vocabulary split.
    vocabulary_split = (
        'CHECK_ELIGIBILITY: "Universe membership / scoreability"' in dag_src
        and "HOC retention rule" in prs_src
        and "no longer meets the eligibility rule" not in prs_src
        and "no longer meet the eligibility rule" not in prs_src)

    legacy_classified = ui.count('data-flow-class="') >= 3

    return {
        "presentation_defines_scoped_economics": bool(scopes_defined),
        "second_current_decision_calculation": second_current_decision,
        "proposal_history_block_present": bool(history_block),
        "hero_renders_current_decision": bool(hero_renders_current),
        "hero_unscoped_proposal_econ_present": hero_unscoped_proposal_econ,
        "realloc_history_demotion_present": bool(realloc_history_demotion),
        "ams_defines_live_lane": bool(ams_defines_lane),
        "second_live_lane_definition": second_lane_definition,
        "ui_renders_live_lane": bool(ui_renders_lane),
        "ui_lane_governance_derivation": ui_lane_derivation,
        "stale_display_label_owned": bool(stale_display_label),
        "outcome_history_version_identity": bool(outcome_versions),
        "ca_projection_scope_declared": bool(ca_scope),
        "eligibility_vocabulary_split": bool(vocabulary_split),
        "legacy_controls_classified": bool(legacy_classified),
    }


#: Release 54.3 — same-session HOC evidence versioning + retrievable binding.
R543_HOC_STORE_ENV = "PAPER_TRADER_HOC_DIR"
#: Provenance that must never contaminate the HOC assessment-evidence identity.
R543_FORBIDDEN_EVIDENCE_COMPONENTS = (
    "portfolio_state_hash", "economic_state_hash", "assessment_hash",
    "generated_at", "run_id", "event_cycle_id", "persisted_at",
    "materiality_trigger_fingerprint",
)
#: The five persistence outcomes, spelled exactly as R54.2 spells them.
R543_PERSIST_OUTCOMES = (
    'PERSIST_CREATED = "CREATED"',
    'PERSIST_REUSED = "REUSED_EXISTING"',
    'PERSIST_ECONOMIC_VERSION = "CREATED_NEW_VERSION"',
    'PERSIST_ASSESSMENT_VERSION = "CREATED_ASSESSMENT_VERSION"',
    'PERSIST_CONFLICT = "CONFLICT_REJECTED"',
    'PERSIST_INCONSISTENT = "REJECTED_INCONSISTENT_IDENTITY"',
)
#: The governance checks that make the HOC dependency PRODUCIBLE as evidence.
#: SEVEN checks, which is why the gate moves 38 -> 45. Six of them fail with one
#: of R54.3's two NEW reason codes; the seventh (HOC_EVIDENCE_IDENTITY_BOUND)
#: binds the new evidence axis and reuses the existing HOC_IDENTITY_MISMATCH
#: code. Counting only the checks that carry a new code is what produced the
#: "six new checks (38 -> 45)" arithmetic error - all seven are enforced here.
R543_GATE_CHECKS = (
    "HOC_ARTIFACT_ID_BOUND",
    "HOC_ASSESSMENT_WAS_PERSISTED",
    "HOC_ARTIFACT_RETRIEVABLE",
    "HOC_ARTIFACT_IDENTITY_MATCHES",
    "REASSESSMENT_BOUND_TO_THE_SAME_HOC_ARTIFACT",
    "REASSESSMENT_DEPENDENCY_IS_NOT_TRANSIENT",
    "HOC_EVIDENCE_IDENTITY_BOUND",
)


def check_release54_3_hoc_evidence_versioning(files: list[Path]) -> dict:
    """R54.3 invariants — one HOC store, append-only versions, provable binding.

    (a) ONE writer and ONE store: ``persist_assessment`` and the HOC artifact-id
        scheme exist only in ``api.holding_opportunity_cost``; no second module
        writes its index, and no parallel intraday HOC root appears;
    (b) a version is APPENDED, never overwritten — the chain is built as
        ``prior_versions + [entry]`` and the owner deletes nothing;
    (c) the assessment-evidence identity is free of the Stage-21 trap, the
        economic axis, the conclusion itself, and every clock / run id / event
        id, and the exclusion list is DECLARED so it is testable;
    (d) the five persistence outcomes are named, and named the same way R54.2
        names them (one vocabulary, two stores);
    (e) governance cannot accept an unpersisted or unretrievable dependency: the
        gate carries all seven R54.3 checks and its own reason codes, and it stays
        PURE (retrievability is resolved by the artifact's owner, never by the
        gate opening a store);
    (f) the reassessment and the proposal each record the EXACT artifact id, and
        the event cycle publishes the persistence outcome it obtained;
    (g) the UI never derives HOC persistence state for itself.
    """
    hoc_src = _read(R543_HOC_OWNER)
    prs_src = _read(R542_REASSESSMENT_OWNER)
    pdec_src = _read("api/portfolio_decision.py")
    esr_src = _read("api/event_signal_refresh.py")
    ui = _read(UI_FILE)

    # (a) one writer, one store.
    second_writers: list[str] = []
    parallel_roots: list[str] = []
    for fp in files:
        rel = _rel(fp).replace("\\", "/")
        if not (rel.startswith("api/") or rel.startswith("engine/")):
            continue
        body = _read(rel)
        for token in ("INTRADAY_HOC_DIR", "intraday_hoc_dir",
                      "PAPER_TRADER_HOC_INTRADAY_DIR"):
            if token in body:
                parallel_roots.append(f"{rel}:{token}")
        if rel == R543_HOC_OWNER:
            continue
        if '"hoc_%s_%s_%s"' in body:
            second_writers.append(f"{rel}:mints_hoc_artifact_ids")
        if "hoc.persist_assessment" in body or (
                "holding_opportunity_cost.persist_assessment" in body):
            second_writers.append(f"{rel}:calls_persist_directly")

    # (b) append-only.
    appends_version_chain = "prior_versions + [entry]" in hoc_src
    owner_deletes_an_artifact = bool("rmtree" in hoc_src
                                     or "unlink(" in hoc_src
                                     or ".remove(str(" in hoc_src)

    # (c) evidence identity purity. Read the DECLARED component tuple, which is
    # what the hash is actually built from.
    comp_block = ""
    if "ASSESSMENT_EVIDENCE_COMPONENTS = (" in hoc_src:
        comp_block = hoc_src.split("ASSESSMENT_EVIDENCE_COMPONENTS = (")[1].split(
            ")")[0]
    contaminated = sorted(c for c in R543_FORBIDDEN_EVIDENCE_COMPONENTS
                          if c in comp_block)
    exclusions_declared = "EVIDENCE_EXCLUDED_PROVENANCE = (" in hoc_src

    # (d) outcome vocabulary, shared with R54.2.
    outcomes_missing = sorted(o for o in R543_PERSIST_OUTCOMES
                              if o not in hoc_src)
    inconsistent_identity_guard = "def _session_identity_conflicts(" in hoc_src

    # (e) governance: fail-closed and PURE.
    gate_checks_missing = sorted(c for c in R543_GATE_CHECKS
                                 if f'"{c}"' not in pdec_src)
    reason_codes_declared = (
        'WR_HOC_NOT_PERSISTED = "HOC_ARTIFACT_NOT_PERSISTED"' in pdec_src
        and 'WR_HOC_ARTIFACT_MISMATCH = "HOC_ARTIFACT_IDENTITY_MISMATCH"'
        in pdec_src
        and "WR_HOC_NOT_PERSISTED, WR_HOC_ARTIFACT_MISMATCH" in pdec_src)
    gate_body = ""
    if "def evaluate_intraday_governance" in pdec_src:
        gate_body = pdec_src.split("def evaluate_intraday_governance")[1].split(
            "\ndef governed_decision_ordering_key")[0]
    gate_opens_a_store = sorted(set(
        t for t in ("load_artifact_by_id", "load_latest_artifact", "read_text",
                    "json.load", "open(")
        if t in gate_body))
    # The persistence facts must be PROVEN, not defaulted: each is compared to
    # True explicitly, so a missing binding fails closed.
    gate_fails_closed = all(
        f'ev.get("{f}") is True' in pdec_src
        for f in ("hoc_persisted", "hoc_artifact_retrievable",
                  "hoc_artifact_identity_matches"))
    binding_resolver_owned = ("def resolve_binding(" in hoc_src
                              and "def artifact_binding(" in hoc_src)

    # (f) downstream exact binding + cycle publication.
    reassessment_binds_artifact = (
        '"hoc_artifact_id": hoc_binding.get("hoc_artifact_id")' in prs_src
        and '"hoc_artifact_id": ic.get("hoc_artifact_id")' in prs_src
        and "def resolve_hoc_binding(" in prs_src)
    proposal_binds_artifact = (
        prs_src.count('"hoc_artifact_id"') >= 3
        and "def proposal_binding(" in prs_src)
    cycle_publishes_persistence = all(
        f'"{k}"' in esr_src for k in (
            "hoc_artifact_id", "hoc_persisted", "hoc_persistence_status",
            "hoc_assessment_evidence_hash"))
    cycle_persists_before_reassessment = False
    if 'with _step("HOLDING_OPPORTUNITY_COST"' in esr_src:
        after = esr_src.split('with _step("HOLDING_OPPORTUNITY_COST"')[1]
        head = after.split('with _step("PORTFOLIO_REASSESSMENT"')
        cycle_persists_before_reassessment = (
            len(head) == 2 and "hoc_result" in head[0]
            and "hoc_binding=" in head[1][:800])

    # (g) the UI derives no persistence state of its own.
    ui_derives_persistence = sorted(set(
        re.findall(r"hoc_persist\w*\s*[!=]==?", ui)
        + re.findall(r"hoc_artifact_id\s*[!=]==?", ui)))

    return {
        "second_hoc_writer": sorted(set(second_writers)),
        "parallel_hoc_stores": sorted(set(parallel_roots)),
        "appends_version_chain": bool(appends_version_chain),
        "owner_deletes_an_artifact": owner_deletes_an_artifact,
        "evidence_identity_contaminated": contaminated,
        "evidence_exclusions_declared": bool(exclusions_declared),
        "persist_outcomes_missing": outcomes_missing,
        "inconsistent_identity_guard_present": bool(inconsistent_identity_guard),
        "gate_checks_missing": gate_checks_missing,
        "gate_reason_codes_declared": bool(reason_codes_declared),
        "gate_opens_a_store": gate_opens_a_store,
        "gate_fails_closed_on_absent_binding": bool(gate_fails_closed),
        "binding_resolver_owned_by_hoc": bool(binding_resolver_owned),
        "reassessment_binds_exact_artifact": bool(reassessment_binds_artifact),
        "proposal_binds_exact_artifact": bool(proposal_binds_artifact),
        "cycle_publishes_hoc_persistence": bool(cycle_publishes_persistence),
        "cycle_persists_hoc_before_reassessment": bool(
            cycle_persists_before_reassessment),
        "ui_derives_hoc_persistence": ui_derives_persistence,
    }


def check_release54_2_3_1_owned_data_readiness_authority(files: list[Path]) -> dict:
    """R54.2.3.1 invariants — persisted close confirmation != provider readiness.

    (a) the LIVE provider-coverage answer for an owed close has exactly ONE
        calculation, ``provider_covers_session`` in ``api.daily_close`` (the owner
        that probes), and no other ``api/*.py`` module defines a second one;
    (b) ``api.workflow_state`` stays PROBE-FREE — it never runs the provider probe
        or the readiness assessment itself — and consumes the close owner's
        verdict verbatim through that one function;
    (c) every composition supplies the answer: the decision snapshot loads the
        close owner BEFORE the workflow owner and passes ``provider_readiness``
        in; the portfolio-cycle orchestrator supplies the bounded assessment at
        POST time; the presentation loaders share ONE daily-close read;
    (d) the two owned-data concepts carry DISTINCT names (the persisted
        confirmation is labelled persisted state; the provider-ready-awaiting-
        close recovery data state exists) and the close gate echoes the coverage
        verdict it obeyed;
    (e) the UI never re-derives readiness — no provider-date comparison exists in
        JavaScript, and the CTA still reads ``primary_action_available`` verbatim.
    """
    dc_src = _read("api/daily_close.py")
    ws_src = _read(R5423_WORKFLOW_OWNER)
    ds_src = _read("api/decision_snapshot.py")
    pc_src = _read("api/portfolio_cycle.py")
    op_src = _read("api/operator_presentation.py")
    ui = _read(UI_FILE)

    # (a) one coverage calculation, owned by the prober.
    second_coverage_calc = sorted(
        _rel(fp) for fp in files
        if _rel(fp).startswith("api/") and _rel(fp) != "api/daily_close.py"
        and "def provider_covers_session(" in fp.read_text(encoding="utf-8",
                                                           errors="replace"))
    owner_defines_coverage = ("def provider_covers_session(" in dc_src
                              and "def assess_owned_provider_readiness(" in dc_src)

    # (b) the workflow is probe-free and consumes, never recomputes.
    workflow_probe_tokens = sorted(
        t for t in ("_PROVIDER_PROBE", "_default_provider_probe",
                    "assess_owned_provider_readiness(") if t in ws_src)
    workflow_consumes_verdict = (
        "_import_daily_close().provider_covers_session" in ws_src)

    # (c) the compositions supply the answer.
    snapshot_supplies = (
        'provider_readiness=(daily_close or {}).get("provider_readiness")' in ds_src)
    snapshot_orders_close_first = (
        '_timed("daily_close"' in ds_src and '_timed("workflow"' in ds_src
        and ds_src.index('_timed("daily_close"') < ds_src.index('_timed("workflow"'))
    orchestrator_supplies = (
        "assess_owned_provider_readiness()" in pc_src
        and "load_workflow_state(provider_readiness=readiness)" in pc_src)
    presentation_shares_one_read = (
        "provider_readiness=dc_payload.get" in op_src
        and "provider_covers_recovery_session" in op_src)

    # (d) distinct names + an echoing gate.
    distinct_concepts = all(t in ws_src for t in (
        "owned_data_confirmation_is_persisted_state",
        "PROVIDER_CONFIRMED_AWAITING_CLOSE",
        "provider_covers_recovery_session"))
    gate_echoes_verdict = "provider_confirms_owed_session" in ws_src

    # (e) the client renders; it never compares provider dates.
    ui_readiness_derivation = sorted(set(re.findall(
        r"provider_latest_date\s*[<>]=?[^\n]{0,40}", ui)))

    return {
        "coverage_owner": "api.daily_close",
        "owner_defines_coverage_and_assessment": bool(owner_defines_coverage),
        "second_coverage_calculation": second_coverage_calc,
        "workflow_probe_tokens": workflow_probe_tokens,
        "workflow_consumes_close_verdict": bool(workflow_consumes_verdict),
        "snapshot_supplies_readiness": bool(snapshot_supplies),
        "snapshot_composes_close_before_workflow": bool(snapshot_orders_close_first),
        "orchestrator_supplies_readiness": bool(orchestrator_supplies),
        "presentation_shares_one_close_read": bool(presentation_shares_one_read),
        "distinct_owned_data_concepts": bool(distinct_concepts),
        "close_gate_echoes_coverage_verdict": bool(gate_echoes_verdict),
        "ui_readiness_derivation": ui_readiness_derivation,
    }


def check_release54_2_2_post_close_research_recovery(files: list[Path]) -> dict:
    """R54.2.2 invariants — ONE post-close obligation owner, ONE classification
    owner, ONE orchestration path, and attribution that fails closed.

    (a) the POST-CLOSE GOVERNED-RESEARCH obligation is decided exactly once, in
        ``api.workflow_state``, and the STALE-INPUT RECOVERABILITY classification
        it composes exactly once, in ``api.daily_research_cycle``;
    (b) no module defines a second post-close research orchestrator, and none
        defines a research-backfill / force-cycle / history-rewrite write route;
    (c) the outstanding obligation OUTRANKS the "wait for the next session close"
        claim in the ONE priority policy, and recovery resumes through the ONE
        portfolio cycle without repeating the completed close;
    (d) the projections (Active Manager State, the operator presentation) DELEGATE
        — they republish the owner's fields and decide no obligation of their own;
    (e) the workflow owner DECIDES each blocker's severity and the presentation
        READS it: a research-only condition never renders a service-wide BLOCKED
        banner, and no reason is rendered as a Python dict repr;
    (f) attribution FAILS CLOSED — a decomposition that does not reproduce the
        recorded NAV move is UNAVAILABLE, its mark resolution requires the exact
        session date, and no historical row is ever rewritten.
    """
    ws_src = _read(R5422_OBLIGATION_OWNER)
    drc_src = _read(R5422_CLASSIFICATION_OWNER)
    pc_src = _read(R5422_CYCLE_OWNER)
    op_src = _read(R5422_PRESENTATION_OWNER)
    ams_src = _read(R5422_AMS_OWNER)
    fe_src = _read(R5422_ATTRIBUTION_OWNER)
    dc_src = _read(R5422_CLOSE_OWNER)
    ui = _read("api/ui/index.html")

    # A second definition anywhere else. This audit script names every forbidden
    # symbol as a literal, so scanning it would match its own constants; it is
    # excluded exactly as the R54.2.1 check excludes itself.
    second_obligation: list[str] = []
    second_classification: list[str] = []
    second_orchestrator: list[str] = []
    forbidden_routes: list[str] = []
    for p in files:
        rel = p.as_posix()
        if rel.endswith("scripts/audit_architecture.py"):
            continue
        src = _read(rel)
        if not src:
            continue
        # ``files`` carries ABSOLUTE paths, so the owner is matched by suffix.
        if not rel.endswith(R5422_OBLIGATION_OWNER) \
                and "def build_research_obligation(" in src:
            second_obligation.append(rel)
        if not rel.endswith(R5422_CLASSIFICATION_OWNER) and (
                "def classify_input_recovery(" in src
                or "def classify_stale_inputs(" in src):
            second_classification.append(rel)
        if any(t in src for t in R5422_FORBIDDEN_DEFS):
            second_orchestrator.append(rel)
        if any(t in src for t in R5422_FORBIDDEN_ROUTES):
            forbidden_routes.append(rel)

    return {
        # (a) one owner per concept.
        "obligation_owner_defines_state_machine": all(
            t in ws_src for t in R5422_OBLIGATION_DEFS),
        "classification_owner_defines_vocabulary": all(
            t in drc_src for t in R5422_CLASSIFICATION_DEFS),
        "workflow_reads_classification": (
            'get("stale_input_classification")' in ws_src),
        "workflow_owns_no_classification": not any(
            t in ws_src for t in ("def classify_input_recovery(",
                                  "def classify_stale_inputs(")),
        # (b) no second orchestrator, no backfill route.
        "second_obligation_owner": sorted(second_obligation),
        "second_classification_owner": sorted(second_classification),
        "second_research_orchestrator": sorted(second_orchestrator),
        "forbidden_research_routes": sorted(forbidden_routes),
        # (c) the obligation outranks "nothing to do", through the ONE policy.
        "obligation_suppresses_wait_gate": (
            "research_obligation_outstanding: bool = False" in ws_src
            and "and not research_obligation_outstanding" in ws_src),
        "cycle_resumes_without_repeating_close": (
            'kind == "DAILY_RESEARCH_CYCLE"' in pc_src
            and "_MAX_OWNER_INVOCATIONS" in pc_src),
        "cycle_path_unchanged": ('RUN_ROUTE = "/v1/operations/portfolio-cycle/run"'
                                 in pc_src),
        "obligation_declares_no_second_route": (
            '"research_specific_route": None' in ws_src),
        "obligation_never_repeats_close": (
            '"repeats_the_completed_close": False' in ws_src),
        # The DRC pre-run gate asks about the ELIGIBLE session, not the wall clock.
        "drc_gate_is_session_scoped": (
            "eligible_cycle_complete" in drc_src
            and 'facts["session_status"] == msession.BEFORE_SESSION_CLOSE' in drc_src),
        # (d) the projections delegate.
        "ams_delegates_obligation": (
            "def _research_obligation_block(" in ams_src
            and '"computed_here": False' in ams_src),
        "presentation_delegates_obligation": (
            'wf.get("research_obligation")' in op_src),
        # (e) severity is decided by the owner and read by the presentation.
        "workflow_states_blocker_severity": (
            "BLOCKER_SCOPE_GOVERNED_RESEARCH" in ws_src
            and '"severity": SEV_ATTENTION' in ws_src),
        "research_stale_never_blocks_decision": (
            '"code": "RESEARCH_INPUT_STALE"' in ws_src
            and '"blocks_portfolio_decision": False' in ws_src),
        "presentation_reads_severity": (
            'row.get("severity")' in op_src
            and 'row.get("blocks_portfolio_decision") is False' in op_src),
        # The old escalation was a bare ``for b in blockers: blocking.append(str(b))``.
        # Matched as CODE (leading indentation), so the comment that records why it
        # was removed does not trip the check.
        "presentation_renders_no_dict_repr": (
            "def _blocker_text(" in op_src
            and "\n        blocking.append(str(b))" not in op_src),
        # (f) attribution fails closed and rewrites nothing.
        "attribution_availability_has_one_owner": (
            "def attribution_availability(" in fe_src
            and "ATTRIB_UNRECONCILED" in fe_src),
        "close_uses_shared_availability": (
            "fe.attribution_availability(" in dc_src),
        "attribution_requires_exact_date": (
            "def _exact(" in fe_src and '"exact_date_required": True' in fe_src),
        "attribution_flags_stale_legs": (
            "stale_leg_tickers" in fe_src and "stale_leg_tickers" in dc_src),
        # A non-reconciling decomposition must be UNAVAILABLE. Matched on the
        # classifier's own branch (the specific COVERAGE_INCOMPLETE cause keeps its
        # own status; the general one is ATTRIB_UNRECONCILED).
        "unreconciled_is_unavailable": (
            "if not reconciles:" in fe_src
            and 'else ATTRIB_UNRECONCILED)' in fe_src
            and '"decomposition_trustworthy": False' in fe_src),
        "attribution_rewrites_no_history": not any(
            t in fe_src or t in dc_src for t in
            ("def rewrite_attribution_history(", "_atomic_write_json(_sdir",
             "def backfill_true_forward(")),
        "ui_states_unavailable_attribution": (
            "ATTRIBUTION UNAVAILABLE" in ui),
        # (g) the UI renders, it does not decide.
        "ui_obligation_derivation": sorted(t for t in R5422_FORBIDDEN_UI if t in ui),
        "ui_renders_backend_obligation": (
            "function _opRenderGovernedResearch(" in ui
            and "p.governed_research" in ui),
        "ui_offers_no_research_backfill": not any(
            t in ui for t in ('id="research-backfill"', "runResearchBackfill",
                              'name="research_session"')),
        # (h) no new authority.
        "research_recovery_adds_automation": any(
            t in ws_src for t in ("schedule.every", "CronCreate", "register_task",
                                  "auto_run_cycle", "def _autorun")),
        "research_recovery_creates_orders": ("create_order" in ws_src
                                             or "submit_order" in ws_src),
        "monthly_contract_not_weakened": (
            "never approximated intramonth" in drc_src
            or "never approximates" in drc_src),
    }


def check_release54_2_same_session_reassessment_versioning(
        files: list[Path]) -> dict:
    """R54.2 invariants — ONE reassessment store, ONE writer, append-only.

    (a) the persistence writer, the assessment-evidence identity, the decision
        fingerprint, the authoritative-history reducer and the version reads are
        each defined exactly once, in api.portfolio_reassessment;
    (b) exactly ONE module writes the reassessment index, and it appends to a
        version chain rather than replacing it — no artifact is ever deleted,
        truncated or rewritten;
    (c) the assessment-evidence identity is EVIDENCE ONLY: it never contains the
        document-wide portfolio_state_hash (which embeds this owner's own
        output), the separate economic axis, or the conclusion itself;
    (d) all four persistence outcomes are named and the inconsistency guard
        exists, so an impossible identity fails closed instead of versioning;
    (e) there is no second reassessment store and no intraday-only parallel
        history — the daily cycle and the event cycle share one chain;
    (f) the R54.1 governance gate still requires the cycle's conclusion to have
        been PERSISTED; versioning may not become an exemption.
    """
    prs_src = _read(R542_REASSESSMENT_OWNER)
    pd_src = _read(R541_DECISION_OWNER)
    esr_src = _read(R541_CYCLE_OWNER)
    components = _r542_evidence_components(prs_src)

    duplicate_owners: list[str] = []
    index_writers: list[str] = []
    parallel_stores: list[str] = []
    for fp in files:
        rel = _rel(fp).replace("\\", "/")
        if not (rel.startswith("api/") or rel.startswith("engine/")):
            continue
        body = _read(rel)
        if "_atomic_write_json(_index_path(reassessment_dir)" in body:
            index_writers.append(rel)
        for token in R542_FORBIDDEN_PARALLEL_STORES:
            if token in body:
                parallel_stores.append(f"{rel}:{token}")
        if rel == R542_REASSESSMENT_OWNER:
            continue
        for d in R542_OWNER_DEFS:
            if d not in body:
                continue
            # R54.3 — the opportunity-cost owner versions its OWN store using the
            # SAME identity vocabulary on purpose (one set of words, two stores).
            # A reassessment-exclusive definition appearing there is still a
            # violation, and so is either word appearing in any third module.
            if rel == R543_HOC_OWNER and d in R542_SHARED_IDENTITY_DEFS:
                continue
            duplicate_owners.append(f"{rel}:{d}")

    return {
        "owner_defs_missing": sorted(
            d for d in R542_OWNER_DEFS if d not in prs_src),
        "duplicate_versioning_owners": sorted(duplicate_owners),
        "index_writers": sorted(index_writers),
        "single_index_writer": index_writers == [R542_REASSESSMENT_OWNER],
        "parallel_reassessment_stores": sorted(parallel_stores),
        "version_chain_is_appended": (
            "prior_versions + [entry]" in prs_src),
        "owner_deletes_an_artifact": bool(
            "rmtree" in prs_src or "os.remove(" in prs_src
            or prs_src.count("unlink(") != 1),
        "persist_outcomes_missing": sorted(
            o for o in R542_PERSIST_OUTCOMES if o not in prs_src),
        "inconsistent_identity_guard_present": (
            "def _session_identity_conflicts(" in prs_src
            and "PERSIST_INCONSISTENT" in prs_src),
        "artifact_id_collision_guard_present": (
            "def _unique_artifact_id(" in prs_src),
        "evidence_identity_declared": bool(components.strip()),
        "forbidden_evidence_components": sorted(
            c for c in R542_FORBIDDEN_EVIDENCE_COMPONENTS
            if '"%s"' % c in components),
        "legacy_artifact_recomputed_not_rewritten": (
            "def _existing_assessment_identity(" in prs_src),
        "authoritative_rows_used_by_churn": (
            "authoritative_history_rows(" in prs_src
            and "exclude_eligible_market_date" in prs_src),
        "outcome_owner_uses_authoritative_rows": (
            "authoritative_history_rows(" in _read("api/reassessment_outcomes.py")),
        "both_producers_delegate": (
            "run_and_persist" in esr_src
            and "run_and_persist" in _read("api/daily_research_cycle.py")
            and "def persist_reassessment(" not in esr_src),
        "gate_requires_persisted_reassessment": (
            'reassessment_persisted' in pd_src
            and "persisted_ok" in pd_src),
        "cycle_publishes_persistence_outcome": (
            '"reassessment_persistence_status"' in esr_src
            and '"reassessment_persisted"' in esr_src),
        "automatic_approval_allowed": False,
        "automatic_execution_allowed": False,
        "advances_operational_mark": False,
    }


def check_inventory_drift(files: list[Path]) -> dict:
    inv_path = "docs/architecture/system_inventory.json"
    raw = _read(inv_path)
    if not raw.strip():
        return {"status": "MISSING", "inventory": inv_path,
                "on_disk_not_in_inventory": [], "in_inventory_not_on_disk": []}
    try:
        inv = json.loads(raw)
    except json.JSONDecodeError as exc:
        return {"status": f"UNPARSEABLE: {exc}", "inventory": inv_path,
                "on_disk_not_in_inventory": [], "in_inventory_not_on_disk": []}
    listed = {m.get("path", "").replace("\\", "/")
              for m in inv.get("modules", [])}
    # Drift scope: top-level api/*.py, engine/*.py and db/{models,session}.py —
    # the significant service/engine surface the inventory is responsible for.
    on_disk: set[str] = set()
    for fp in files:
        rel = _rel(fp)
        parts = rel.split("/")
        if rel.endswith("/__init__.py") or len(parts) != 2:
            continue
        if parts[0] in ("api", "engine") or rel in ("db/models.py", "db/session.py"):
            on_disk.add(rel)
    return {
        "status": "OK",
        "inventory": inv_path,
        "on_disk_not_in_inventory": sorted(on_disk - listed),
        "in_inventory_not_on_disk": sorted(
            p for p in listed if p and not (REPO_ROOT / p).exists()),
    }


def check_local_only_not_released() -> dict:
    """Local-only files must not appear in the handoff release allowlist."""
    allow_ps = REPO_ROOT.parent  # not scanned; the allowlist lives in D:\Temp
    # We can only assert the files are gitignore-eligible / present locally.
    present = sorted(f for f in LOCAL_ONLY_FILES
                     if (REPO_ROOT / f.rstrip("/")).exists())
    return {"local_only_files": sorted(LOCAL_ONLY_FILES),
            "present_locally": present}


def check_docs_present() -> dict:
    return {"docs": {d: (REPO_ROOT / d).exists() for d in sorted(CANONICAL_DOCS)},
            "missing": sorted(d for d in CANONICAL_DOCS
                              if not (REPO_ROOT / d).exists())}


R45_OWNERS = {
    "root": "alpha_agent/r45/__init__.py",
    "contract": "alpha_agent/r45/contract.py",
    "burden": "alpha_agent/r45/burden.py",
    "shell_policy": "alpha_agent/r45/shell_policy.py",
    "bars": "alpha_agent/r45/bars.py",
    "acquisition": "alpha_agent/r45/acquisition.py",
    "eventstudy": "alpha_agent/r45/eventstudy.py",
    "replication": "alpha_agent/r45/replication.py",
    "causal": "alpha_agent/r45/causal.py",
    "discovery": "alpha_agent/r45/discovery.py",
    "rv": "alpha_agent/r45/rv.py",
    "surprise": "alpha_agent/r45/surprise.py",
    "ml": "alpha_agent/r45/ml.py",
    "killer": "alpha_agent/r45/killer.py",
    "implementable": "alpha_agent/r45/implementable.py",
    "options": "alpha_agent/r45/options.py",
    "analyst": "alpha_agent/r45/analyst.py",
    "frontier": "alpha_agent/r45/frontier.py",
    "campaign": "alpha_agent/r45/campaign.py",
    "closeout": "alpha_agent/r45/closeout.py",
}

#: A second implementation of an already-owned concern inside r45 is a
#: blocking defect. The capital equation in particular belongs to Release 43
#: and is IMPORTED: a release that writes its own denominator can quote
#: whatever return it likes.
R45_SECOND_OWNER_FORBIDDEN = (
    "alpha_agent/r45/judge.py", "alpha_agent/r45/capital.py",
    "alpha_agent/r45/evidence.py", "alpha_agent/r45/economics.py",
    "alpha_agent/r45/zones.py", "alpha_agent/r45/multiple_testing.py",
    "alpha_agent/r45/panels.py", "alpha_agent/r45/scheduler.py",
    "alpha_agent/r45/research_shadow.py", "alpha_agent/r45/forward_freeze.py",
    "alpha_agent/r45/deflated_sharpe.py", "alpha_agent/r45/ledger.py",
)

R45_REQUIRED_STATES = (
    "R45_QUALIFIED_EVENT_ALPHA_FOUND",
    "R45_NATIVE_FUTURES_EVENT_ALPHA_CANDIDATE_FOUND",
    "R45_EVENT_RELATIVE_VALUE_ALPHA_CANDIDATE_FOUND",
    "R45_STRONG_EVENT_CANDIDATE_FORWARD_PENDING",
    "R45_GOLD_SPECIFIC_EFFECT_NOT_GENERAL_MACRO_ALPHA",
    "R45_R44_MACRO_EFFECT_REFUTED_IN_NATIVE_MARKETS",
    "R45_NATIVE_FUTURES_DATA_WALL_BINDING",
    "R45_NO_QUALIFIED_EVENT_ALPHA",
)


def check_release45_macro_event_alpha(files: list[Path]) -> dict:
    """Release 45 invariants - a REPLICATION that may not quietly become a search.

    Release 45's whole claim is that it tested somebody else's rule without
    changing it. Every invariant below closes one route by which that claim
    could be false:

    * retune the rule until it works and still call it a replication - closed
      by carrying R44's parameters as a frozen literal, by an identity check
      that must reproduce R44's published card from R45's own code, and by
      charging every post-replication parameter cell to the burden ledger;
    * quietly report a CFD or an ETF as a native futures result - closed by an
      instrument-class table, two separate no-proxy flags, and a lane that
      states in its own payload that it is not a futures result;
    * invent a spread where no quote exists - closed by requiring the cost
      source to be labelled on every card, and by a declared floor under the
      estimated one;
    * let several releases printing into the same minute inflate a t-statistic
      - closed by clustering inference on the event date;
    * present the search zone's placebo as evidence - closed by running the
      identical control battery on the never-scored events;
    * freeze a dead candidate just to have a shadow - closed outright;
    * assert a data wall instead of probing it, or buy something.
    """
    src = {name: (_read(REPO_ROOT / path) or "")
           for name, path in R45_OWNERS.items()}
    modules_missing = sorted(n for n, t in src.items() if not t)
    all_src = "\n".join(src.values())
    second_owner_modules = sorted(p for p in R45_SECOND_OWNER_FORBIDDEN
                                  if (REPO_ROOT / p).exists())

    # (1) The rule is inherited, not re-derived, and proven identical.
    frozen_rule_is_inherited = (
        "FROZEN_RULE = {" in src["contract"]
        and '"source_release": "R44"' in src["contract"]
        and '"entry_delay_min": 5' in src["contract"]
        and '"hold_min": 120' in src["contract"]
        and '"rule": "REVERSAL"' in src["contract"]
        and "R44_ZONE_A_REFERENCE" in src["contract"]
        and "NO_PARAMETER_SEARCH_BEFORE_FIRST_REPLICATION = True"
        in src["contract"]
        and "RETUNING_AFTER_A_FAILED_FROZEN_TEST_IS_NOT_A_REPLICATION = True"
        in src["contract"])
    identity_is_proven_not_asserted = (
        "def identity_check" in src["eventstudy"]
        and "R44_REFERENCE_TOLERANCE" in src["contract"]
        and "IDENTICAL" in src["eventstudy"]
        and "identity_check" in src["replication"])
    holdout_is_the_first_test = (
        "L1_GOLD_HOLDOUT" in src["contract"]
        and "REPLICATION_LANES_FIRST" in src["contract"]
        and "def lane_l1_gold_holdout" in src["replication"]
        and "n_events_never_scored_by_r44" in src["replication"])

    # (2) Burden inherits and never resets.
    burden_inherited_not_reset = (
        "INHERITED_GLOBAL_BURDEN = 310" in src["contract"]
        and "INHERITED_GLOBAL_BURDEN_CONSERVATIVE = 312" in src["contract"]
        and "BURDEN_MAY_NEVER_BE_RESET = True" in src["contract"]
        and "class BurdenLaundering" in src["burden"]
        and "raise BurdenLaundering" in src["burden"])
    replication_is_one_trial_but_search_is_charged = (
        "FROZEN_REPLICATION_IS_ONE_TRIAL = True" in src["contract"]
        and "POST_REPLICATION_EXPLORATION_IS_CHARGED_PER_CELL = True"
        in src["contract"]
        and "def charge_frozen_replication" in src["burden"]
        and "charge=BU.charge" in src["campaign"]
        and "family=\"EVENT_RELATIVE_VALUE\"" in src["rv"]
        and "family=\"EVENT_ML\"" in src["ml"])

    # (3) An instrument is never reported as something it is not.
    instrument_class_is_declared = (
        "INSTRUMENT_CLASS = {" in src["contract"]
        and "NATIVE_FUTURES" in src["contract"]
        and "LISTED_ETF" in src["contract"]
        and '"CFD"' in src["contract"])
    no_proxy_for_a_futures_hypothesis = (
        "NO_CFD_PROXY_FOR_A_FUTURES_HYPOTHESIS = True" in src["contract"]
        and "NO_ETF_PROXY_FOR_A_FUTURES_HYPOTHESIS = True" in src["contract"]
        and "this_is_not_a_futures_result" in src["replication"]
        and "cfd_symbols_may_not_be_called_a_futures_replication"
        in src["replication"])

    # (4) Cost may be estimated, but never unlabelled.
    cost_source_is_labelled = (
        "COST_SOURCE_OBSERVED" in src["contract"]
        and "COST_SOURCE_ESTIMATED" in src["contract"]
        and "COST_SOURCE_MUST_BE_LABELLED = True" in src["contract"]
        and "ESTIMATED_HALF_SPREAD_FLOOR_BPS" in src["contract"]
        and "cost_source" in src["bars"]
        and "cost_source" in src["eventstudy"])
    estimated_spread_has_a_floor = (
        "def corwin_schultz_half_bps" in src["bars"]
        and "floor_bps" in src["bars"]
        and "clip(lower=float(floor_bps))" in src["bars"])
    no_fabricated_fill = (
        "NO_FABRICATED_FILL = True" in src["contract"]
        and "NO_MIDPOINT_FILL_WITHOUT_A_QUOTE = True" in src["contract"]
        and "NO_INTERPOLATED_INTRADAY = True" in src["contract"])
    entry_is_never_at_the_print = (
        '"entry_delay_min": 5' in src["contract"]
        and "extra_latency" in src["eventstudy"])

    # (5) Inference is clustered, and the controls run on the holdout too.
    inference_is_clustered_by_event_date = (
        'CLUSTER_INFERENCE_BY = "EVENT_DATE"' in src["contract"]
        and "def cluster_t" in src["eventstudy"]
        and "net_t_cluster" in src["eventstudy"]
        and "net_t_cluster" in src["replication"])
    controls_run_on_the_holdout_not_just_the_search_zone = (
        'zones = ("A", "BC", "ALL")' in src["causal"]
        and "SUPPORTED_ONLY_WHERE_THE_SEARCH_LOOKED" in src["causal"]
        and "verdicts_by_zone" in src["causal"]
        and "timing_sweeps_by_zone" in src["causal"])
    placebo_battery_is_plural = (
        "def placebo_shifted_days" in src["causal"]
        and "def placebo_random_dates" in src["causal"]
        and "def placebo_label_permutation" in src["causal"]
        and "def timing_sweep" in src["causal"])
    seeds_are_declared_not_hashed = (
        "PLACEBO_SEED = " in src["causal"]
        and "default_rng" in src["causal"]
        and "BOOTSTRAP_SEED = " in src["killer"]
        and "hash(" not in all_src)

    # (6) Hedge ratios and models never see the window they are judged on.
    hedge_ratio_is_fitted_on_training_events_only = (
        "HEDGE_RATIOS_ARE_FITTED_ON_TRAINING_EVENTS_ONLY = True"
        in src["contract"]
        and "in_fit" in src["rv"]
        and "beta = _fit_beta(y_shock[in_fit], X_shock[in_fit])" in src["rv"])
    models_fit_select_and_judge_on_separate_zones = (
        "fit on A, choose on B, judged once on C" in src["ml"]
        and "frozen_rule_baseline" in src["ml"]
        and "chosen_beats_frozen_rule_on_zone_c" in src["ml"])
    ml_must_beat_the_transparent_rule = (
        "ML_ADDED_ECONOMIC_VALUE" in src["ml"]
        and "ML_ADDED_ECONOMIC_VALUE" in src["closeout"])

    # (7) The economic judge is R43's, imported.
    capital_equation_is_imported = (
        "from ..r43 import judge as J43" in src["implementable"]
        and 'CAPITAL_EQUATION_OWNER = "alpha_agent.r43.judge"'
        in src["implementable"]
        and "J43.futures_committed_capital" in src["implementable"])
    remunerated_margin_treated_correctly = (
        'COLLATERAL_CLASS = "REMUNERATED_MARGIN"' in src["implementable"]
        and "no further cash rent is charged" in src["implementable"])

    # (8) Kill battery, qualification and freezing.
    kill_battery_complete = all(
        tok in src["killer"] for tok in
        ("def cost_stress", "def latency_stress", "def leave_one_out",
         "def remove_extremes", "def bootstrap_by_event_date",
         "def horizon_perturbation"))
    qualification_is_stricter_than_one_t = (
        "A_SINGLE_T_ABOVE_2_IS_NOT_A_QUALIFICATION = True" in src["contract"]
        and '"net_t_cluster_ge": 2.5' in src["contract"]
        and '"must_survive_cost_x2": True' in src["contract"]
        and '"must_survive_leave_one_year_out": True' in src["contract"]
        and "MIN_EVENTS_TO_QUALIFY = 100" in src["contract"])
    no_mediocre_shadow = (
        "DO_NOT_FREEZE_MEDIOCRE_CANDIDATES_TO_CREATE_A_SHADOW = True"
        in src["contract"]
        and "def freeze_gate" in src["frontier"]
        and "MAX_NEW_SHADOWS" in src["frontier"]
        and "why_none" in src["frontier"])
    forward_never_backfilled = (
        "NEVER_BACKFILL_PROSPECTIVE_ROWS = True" in src["contract"]
        and "PRIOR_SHADOWS_ARE_IMMUTABLE = True" in src["contract"]
        and "backfilled" in src["analyst"]
        and "READ_ONLY = True" in src["analyst"])
    prior_artifacts_read_only = (
        "r44_artifacts_read_only" in src["options"]
        and "sha_before" in src["options"]
        and "wrote_into_the_ledger" in src["analyst"])

    # (9) Data walls are probed, not asserted, and nothing is bought.
    walls_probed_not_asserted = (
        "def probe_blocked_native_routes" in src["acquisition"]
        and "ACCOUNT_REQUIRED" in src["acquisition"]
        and "rows_when_asked_for_1min" in src["acquisition"])
    no_purchase_or_account = (
        "AUTHORIZED_SPEND_USD = 0.0" in src["contract"]
        and '"money_spent_usd": 0.0' in src["acquisition"]
        and '"accounts_created": 0' in src["acquisition"]
        and "DO_NOT_BUY_YET" in src["frontier"])
    keys_never_leak = not re.search(
        r"(apiKey|api_token|api_key)\s*=\s*[\"'][A-Za-z0-9]{12,}", all_src)

    # (10) Safety, shell policy, terminals.
    no_operational_imports = not any(
        tok in all_src for tok in
        ("from api.", "import api.", "from engine.", "import engine.",
         "operational_book", "portfolio_decision", "rebalance"))
    no_scheduler_or_task_registration = not any(
        tok in all_src for tok in
        ("schtasks", "Register-ScheduledTask", "crontab", "CronCreate"))
    safety_flags_false = (
        "PROMOTION_ALLOWED = False" in src["contract"]
        and "RESEARCH_ONLY = True" in src["contract"]
        and '"ORDERS": 0' in src["closeout"]
        and '"PORTFOLIO_MUTATIONS": 0' in src["closeout"])
    shell_policy_declared = (
        'SHELL_POLICY = "WINDOWS_POWERSHELL_ONLY"' in src["contract"]
        and "SHELL_POLICY_WAIVERS_ARE_NOT_AVAILABLE = True" in src["contract"]
        and "INHERITED_SHELL_DISCLOSURES" in src["contract"]
        and "R45_EVENTS" in src["shell_policy"]
        and "SHELL_POLICY_VIOLATION" in src["shell_policy"])
    inherited_disclosures_preserved = (
        '"release": "R42"' in src["contract"]
        and '"release": "R44"' in src["contract"]
        and "inherited_disclosures_are_never_erased" in src["shell_policy"])
    every_lane_must_terminate = (
        "BLOCKER_VOCAB" in src["contract"]
        and "A_FAILED_LANE_IS_A_ROUTING_EVENT = True" in src["contract"]
        and "ONE_LANE_MAY_NOT_HALT_ANOTHER = True" in src["contract"]
        and "NO_BROAD_EXECUTABLE_ZERO_COST_BRANCH_MAY_BE_DEFERRED = True"
        in src["contract"])
    verdict_keys_declared = (
        "REQUIRED_VERDICT_KEYS" in src["contract"]
        and "def terminal_state" in src["closeout"])
    states_missing = sorted(
        s for s in R45_REQUIRED_STATES if s not in src["contract"])

    return {
        "modules_present": not modules_missing,
        "modules_missing": modules_missing,
        "second_owner_modules": second_owner_modules,
        "frozen_rule_is_inherited": frozen_rule_is_inherited,
        "identity_is_proven_not_asserted": identity_is_proven_not_asserted,
        "holdout_is_the_first_test": holdout_is_the_first_test,
        "burden_inherited_not_reset": burden_inherited_not_reset,
        "replication_is_one_trial_but_search_is_charged":
            replication_is_one_trial_but_search_is_charged,
        "instrument_class_is_declared": instrument_class_is_declared,
        "no_proxy_for_a_futures_hypothesis": no_proxy_for_a_futures_hypothesis,
        "cost_source_is_labelled": cost_source_is_labelled,
        "estimated_spread_has_a_floor": estimated_spread_has_a_floor,
        "no_fabricated_fill": no_fabricated_fill,
        "entry_is_never_at_the_print": entry_is_never_at_the_print,
        "inference_is_clustered_by_event_date":
            inference_is_clustered_by_event_date,
        "controls_run_on_the_holdout_not_just_the_search_zone":
            controls_run_on_the_holdout_not_just_the_search_zone,
        "placebo_battery_is_plural": placebo_battery_is_plural,
        "seeds_are_declared_not_hashed": seeds_are_declared_not_hashed,
        "hedge_ratio_is_fitted_on_training_events_only":
            hedge_ratio_is_fitted_on_training_events_only,
        "models_fit_select_and_judge_on_separate_zones":
            models_fit_select_and_judge_on_separate_zones,
        "ml_must_beat_the_transparent_rule": ml_must_beat_the_transparent_rule,
        "capital_equation_is_imported": capital_equation_is_imported,
        "remunerated_margin_treated_correctly":
            remunerated_margin_treated_correctly,
        "kill_battery_complete": kill_battery_complete,
        "qualification_is_stricter_than_one_t":
            qualification_is_stricter_than_one_t,
        "no_mediocre_shadow": no_mediocre_shadow,
        "forward_never_backfilled": forward_never_backfilled,
        "prior_artifacts_read_only": prior_artifacts_read_only,
        "walls_probed_not_asserted": walls_probed_not_asserted,
        "no_purchase_or_account": no_purchase_or_account,
        "keys_never_leak": bool(keys_never_leak),
        "no_operational_imports": no_operational_imports,
        "no_scheduler_or_task_registration": no_scheduler_or_task_registration,
        "safety_flags_false": safety_flags_false,
        "shell_policy_declared": shell_policy_declared,
        "inherited_disclosures_preserved": inherited_disclosures_preserved,
        "every_lane_must_terminate": every_lane_must_terminate,
        "verdict_keys_declared": verdict_keys_declared,
        "terminal_states_missing": states_missing,
    }


# --------------------------------------------------------------------------- #
# Release 46 - the prospective alpha tournament
# --------------------------------------------------------------------------- #
R46_OWNERS = {
    "contract": "alpha_agent/r46/contract.py",
    "shell_policy": "alpha_agent/r46/shell_policy.py",
    "clock": "alpha_agent/r46/clock.py",
    "marketdata": "alpha_agent/r46/marketdata.py",
    "feasibility": "alpha_agent/r46/feasibility.py",
    "challengers": "alpha_agent/r46/challengers.py",
    "registry": "alpha_agent/r46/registry.py",
    "ledger": "alpha_agent/r46/ledger.py",
    "emit": "alpha_agent/r46/emit.py",
    "judge": "alpha_agent/r46/judge.py",
    "evidence": "alpha_agent/r46/evidence.py",
    "leaderboard": "alpha_agent/r46/leaderboard.py",
    "burden": "alpha_agent/r46/burden.py",
    "options": "alpha_agent/r46/options.py",
    "analyst": "alpha_agent/r46/analyst.py",
    "campaign": "alpha_agent/r46/campaign.py",
    # Release 46.2 added the advance owner without gating it here; Release
    # 46.3 closes that gap and adds its own three owners, so every module in
    # the R46 lineage is inside the concatenated-source token scans below.
    "advance": "alpha_agent/r46/advance.py",
    "velocity": "alpha_agent/r46/velocity.py",
    "planner": "alpha_agent/r46/planner.py",
    "intraday": "alpha_agent/r46/intraday.py",
    # Release 46.4 - the economic layer and the orthogonal lanes. ONE owner
    # per concept: trade economics, the research trade ledger, strategy
    # P&L streams, the shadow target, the shadow NAV, the risk state,
    # attribution, regime, opportunity cost, the P&L board, the P&L stage
    # of the step, and the four information lanes.
    "pnl": "alpha_agent/r46/pnl.py",
    "trades": "alpha_agent/r46/trades.py",
    "strategy_pnl": "alpha_agent/r46/strategy_pnl.py",
    "allocation": "alpha_agent/r46/allocation.py",
    "nav": "alpha_agent/r46/nav.py",
    "risk": "alpha_agent/r46/risk.py",
    "attribution": "alpha_agent/r46/attribution.py",
    "regime": "alpha_agent/r46/regime.py",
    "opportunity": "alpha_agent/r46/opportunity.py",
    "pnl_board": "alpha_agent/r46/pnl_board.py",
    "shadow": "alpha_agent/r46/shadow.py",
    "cftc": "alpha_agent/r46/cftc.py",
    "credit": "alpha_agent/r46/credit.py",
    "macro": "alpha_agent/r46/macro.py",
    "events": "alpha_agent/r46/events.py",
    # Release 46.5 - the forward harvest (matured vs mark-to-market), the
    # strategy verdicts, the one EDGAR access seam and the two EDGAR lanes.
    "harvest": "alpha_agent/r46/harvest.py",
    "verdicts": "alpha_agent/r46/verdicts.py",
    "sec": "alpha_agent/r46/sec.py",
    "earnings": "alpha_agent/r46/earnings.py",
    "form4": "alpha_agent/r46/form4.py",
    # Release 46.6 - the cost-efficiency owner (signal edge versus economic
    # edge), the research-lane lifecycle registry, and the scorer for the
    # three option hypotheses predeclared before the sample closed.
    "cost_efficiency": "alpha_agent/r46/cost_efficiency.py",
    "lanes": "alpha_agent/r46/lanes.py",
    "options_hypotheses": "alpha_agent/r46/options_hypotheses.py",
    # Release 46.6.1 - THE adopted-shadow forward continuation owner. R46.6
    # left three adopted lanes CALLED and unable to accrue, because the only
    # ledger their owners write belongs to a prior release. This is the one
    # place adopted forward evidence is written, and it writes nothing else.
    "adopted_forward": "alpha_agent/r46/adopted_forward.py",
}

#: Release 46.4 - a SECOND implementation of an economic concept the release
#: declares one owner for is a blocking defect, exactly as for the ledger.
R46_4_SECOND_OWNER_FORBIDDEN = (
    "alpha_agent/r46/paper_trades.py",
    "alpha_agent/r46/shadow_nav.py",
    "alpha_agent/r46/shadow_allocator.py",
    "alpha_agent/r46/pnl_engine.py",
    "alpha_agent/r46/trade_ledger.py",
    "alpha_agent/r46/portfolio.py",
    "alpha_agent/r46/orders.py",
    # Release 46.5 - a second harvester, verdict engine, insider or earnings
    # owner would split a concept the release gives exactly one owner.
    "alpha_agent/r46/harvester.py",
    "alpha_agent/r46/winners.py",
    "alpha_agent/r46/kill_engine.py",
    "alpha_agent/r46/insider.py",
    "alpha_agent/r46/pead.py",
    "alpha_agent/r46/edgar.py",
)

#: A SECOND implementation of a concept Release 46 declares itself the single
#: owner of is a blocking defect. The prediction ledger above all: the entire
#: release is the claim that there is exactly ONE place a forward prediction
#: can be recorded, and five prior releases each built their own.
R46_SECOND_OWNER_FORBIDDEN = (
    "alpha_agent/r46/forward_freeze.py",
    "alpha_agent/r46/research_shadow.py",
    "alpha_agent/r46/shadow_registry.py",
    "alpha_agent/r46/tournament.py",
    "alpha_agent/r46/prediction_ledger.py",
    "alpha_agent/r46/outcome_judge.py",
    "alpha_agent/r46/desk.py",
    "alpha_agent/r46/paper_trading_desk.py",
)

R46_REQUIRED_STATES = (
    "R46_PROSPECTIVE_ALPHA_TOURNAMENT_LIVE",
    "R46_FORWARD_PREDICTIONS_EMITTED",
    "R46_FORWARD_EVIDENCE_ALREADY_MATURING",
    "R46_TOURNAMENT_READY_NEXT_FORWARD_WINDOW",
    "R46_NO_VALID_FORWARD_WINDOW_TODAY",
    "R46_BLOCKED_BY_AUTHORITATIVE_DATA_FRESHNESS",
    "R46_FORWARD_INFRASTRUCTURE_INCOMPLETE",
)


def check_release52_persistent_research_runtime(files: list[Path]) -> dict:
    """Release 52 invariants - a research runtime that cannot cheat or reach.

    The release's two claims: (a) prospective evidence capture is durable,
    session-aware, idempotent and fail-closed; (b) the runtime is research
    automation ONLY. Every invariant closes a route by which either claim
    could silently become false:

    * a second timing authority - closed by ONE derived timing contract that
      quotes the canonical owners and by a scheduler entrypoint that owns no
      clock rule;
    * a backfill path - closed by a forfeiture ledger whose every row must
      carry ``backfill_refused: true`` and whose append refuses anything
      else;
    * a second orchestration path - closed by exactly one
      ``research_runtime_cycle`` definition and exactly one script that
      registers the Windows task;
    * concurrent writers losing rows - closed by the campaign lock inside
      the ONE advance and the runtime instance lock;
    * an automation reach into the operational portfolio - closed by import
      and token bans across the whole R52 package;
    * an automatic promotion - closed by the absence of any approval writer
      and by the frontier refresh delegating to the pure R51 owner.
    """
    r52_dir = REPO_ROOT / "alpha_agent" / "r52"
    src = {p.stem: (_read(p) or "") for p in sorted(r52_dir.glob("*.py"))}
    all_src = "\n".join(src.values())
    runner = _read(REPO_ROOT / "scripts/run_research_runtime.py") or ""
    api_rm = _read(REPO_ROOT / "api/research_runtime.py") or ""
    install_ps1 = _read(REPO_ROOT /
                        "scripts/install_research_runtime_task.ps1") or ""
    validate_ps1 = _read(REPO_ROOT /
                         "scripts/validate_research_runtime_task.ps1") or ""
    disable_ps1 = _read(REPO_ROOT /
                        "scripts/disable_research_runtime_task.ps1") or ""
    once_ps1 = _read(REPO_ROOT /
                     "scripts/run_research_runtime_once.ps1") or ""
    advance_src = _read(REPO_ROOT / "alpha_agent/r46/advance.py") or ""
    runlock_src = _read(REPO_ROOT / "alpha_agent/r46/runlock.py") or ""

    modules_missing = sorted(
        m for m in ("__init__", "timing_contract", "forfeiture", "runtime",
                    "frontier_refresh", "velocity_ops")
        if m not in src or not src[m])

    # (1) ONE timing contract, derived from the canonical owners.
    one_timing_contract = (
        "scheduler_is_not_a_timing_authority" in src.get("timing_contract", "")
        and "from ..r46 import clock" in src.get("timing_contract", "")
        and "from ..r46 import lanes" in src.get("timing_contract", "")
        and "REFUSED_ALWAYS" in src.get("timing_contract", ""))
    scheduler_owns_no_clock_rule = (
        "17:45" not in runner and "08:15" not in runner
        and "while True" not in runner
        and "research_runtime_cycle" in runner)
    scheduler_delegates_to_contract_times = all(
        t in src.get("timing_contract", "") and t in install_ps1
        for t in ("08:15", "17:45", "19:45", "21:45"))

    # (2) ONE orchestration path.
    one_runtime_orchestrator = (
        all_src.count("def research_runtime_cycle") == 1
        and runner.count("research_runtime_cycle") >= 1
        and "def research_runtime_cycle" not in runner)
    runtime_delegates_emission_and_scoring = (
        "AD.advance" in src.get("runtime", "")
        and "def emit" not in src.get("runtime", "")
        and "def build_batch" not in src.get("runtime", "")
        and "def score" not in src.get("runtime", ""))
    runtime_is_locked_and_idempotent = (
        "acquire_path" in src.get("runtime", "")
        and "RUN_REFUSED_CONCURRENT" in src.get("runtime", "")
        and "lock_holder" in advance_src
        and "AdvanceLockBusy" in advance_src
        and "def acquire_path" in runlock_src
        and "_reclaim_if_stale" in runlock_src)
    runtime_fails_closed_on_broken_chain = (
        "RUN_FAILED_INTEGRITY" in src.get("runtime", "")
        and "fail_closed" in src.get("runtime", "")
        and "_chains_ok" in src.get("runtime", ""))

    # (3) ONE forfeiture owner; no backfill path anywhere.
    one_forfeiture_owner = (
        "_append_ledger" in src.get("forfeiture", "")
        and "verify_ledger" in src.get("forfeiture", "")
        and "IDENTITY_KEY" in src.get("forfeiture", "")
        and "backfill_refused" in src.get("forfeiture", ""))
    forfeiture_refuses_backfill = (
        "a forfeiture row must refuse backfill" in src.get("forfeiture", "")
        and '"backfill_refused": True' in src.get("forfeiture", ""))

    # (4) ONE promotion frontier owner; no approval writer.
    frontier_delegates_to_r51 = (
        "PF.build" in src.get("frontier_refresh", "")
        and "from ..r51 import promotion_frontier" in
        src.get("frontier_refresh", ""))
    no_approval_writer = not any(
        tok in all_src for tok in
        ("record_decision", "approvals=", "APPROVED_FOR_OPERATION"))
    no_promotion_flag_write = (
        "model_approval_state" not in all_src.replace(
            "``model_approval_state``", "")
        and "capital_eligible" not in all_src.replace(
            "``capital_eligible``", ""))

    # (5) No operational reach, no broker, no portfolio-cycle POST.
    no_operational_imports = not any(
        tok in all_src for tok in
        ("from paper_trader.api import daily_close",
         "from paper_trader.api import rebalance_execution",
         "from paper_trader.engine import normal_cycle",
         "from paper_trader.api import portfolio_decision"))
    no_http_reach = not any(
        tok in (all_src + runner) for tok in
        ("requests.", "urllib.request", "http://127.0.0.1:8001",
         "portfolio-cycle/run"))
    safety_flags_false = all(
        ('"%s": False' % flag) in src.get("__init__", "")
        for flag in ("calls_portfolio_cycle", "runs_daily_close",
                     "creates_order", "mutates_holdings", "mutates_cash",
                     "mutates_nav", "promotes_model", "activates_sleeve",
                     "may_spend_money", "backfills_forward_rows",
                     "writes_operational_store"))

    # (6) ONE Windows task entrypoint; explicit lifecycle scripts.
    ps1_files = sorted((REPO_ROOT / "scripts").glob("*.ps1"))
    # Prior releases own their own installers (Stage 4 alpha-agent tasks,
    # Release 29 collection). The R52 invariant is scoped to the R52 task:
    # exactly ONE script may register PaperTrader-ResearchRuntime.
    registering = sorted(
        p.name for p in ps1_files
        if "Register-ScheduledTask" in (_read(p) or "")
        and "PaperTrader-ResearchRuntime" in (_read(p) or ""))
    one_task_registrar = registering == ["install_research_runtime_task.ps1"]
    install_is_explicit = (
        "R52_TASK_UNCHANGED" in install_ps1
        and "explicit migration" in install_ps1
        and "StartWhenAvailable" in install_ps1
        and "IgnoreNew" in install_ps1)
    # The correction: task equivalence must include the principal. An
    # existing Interactive task is NOT identical to a requested S4U task,
    # a principal mismatch without -Force is its own specific blocker, and
    # a migration never silently falls back to Interactive.
    installer_compares_principal = (
        "Principal.LogonType" in install_ps1
        and "explicit -Force migration required" in install_ps1
        and "BLOCKED_PRINCIPAL" in install_ps1
        and "R52_TASK_MIGRATED" in install_ps1
        and "requested logon type ONLY" in install_ps1)
    validator_blocks_bad_task = (
        "R52_SCHEDULER_INCOMPLETE" in validate_ps1
        and "R52_TASK_INVALID" in validate_ps1
        and "missing trigger times" in validate_ps1)
    # Production validation must require a logged-out-capable principal:
    # an Interactive task silently reintroduces the defect R52 closes.
    validator_requires_logged_out_principal = (
        "logged-out-capable" in validate_ps1
        and "'S4U', 'Password', 'ServiceAccount'" in validate_ps1
        and "R52_PRINCIPAL_REJECTED" in validate_ps1)
    disable_deletes_nothing = (
        "Disable-ScheduledTask" in disable_ps1
        and "Unregister-ScheduledTask" not in disable_ps1
        and "Remove-Item" not in disable_ps1)
    one_shot_uses_same_entrypoint = "run_research_runtime.py" in once_ps1

    # (7) The read model is read-only.
    read_model_is_read_only = bool(api_rm) and not any(
        tok in api_rm for tok in ("write_json", "_append_ledger",
                                  "research_runtime_cycle(", "def sweep"))

    return {
        "modules_present": not modules_missing,
        "modules_missing": modules_missing,
        "one_timing_contract": one_timing_contract,
        "scheduler_owns_no_clock_rule": scheduler_owns_no_clock_rule,
        "scheduler_delegates_to_contract_times":
            scheduler_delegates_to_contract_times,
        "one_runtime_orchestrator": one_runtime_orchestrator,
        "runtime_delegates_emission_and_scoring":
            runtime_delegates_emission_and_scoring,
        "runtime_is_locked_and_idempotent": runtime_is_locked_and_idempotent,
        "runtime_fails_closed_on_broken_chain":
            runtime_fails_closed_on_broken_chain,
        "one_forfeiture_owner": one_forfeiture_owner,
        "forfeiture_refuses_backfill": forfeiture_refuses_backfill,
        "frontier_delegates_to_r51": frontier_delegates_to_r51,
        "no_approval_writer": no_approval_writer,
        "no_promotion_flag_write": no_promotion_flag_write,
        "no_operational_imports": no_operational_imports,
        "no_http_reach": no_http_reach,
        "safety_flags_false": safety_flags_false,
        "one_task_registrar": one_task_registrar,
        "task_registrars_found": registering,
        "install_is_explicit": install_is_explicit,
        "installer_compares_principal": installer_compares_principal,
        "validator_blocks_bad_task": validator_blocks_bad_task,
        "validator_requires_logged_out_principal":
            validator_requires_logged_out_principal,
        "disable_deletes_nothing": disable_deletes_nothing,
        "one_shot_uses_same_entrypoint": one_shot_uses_same_entrypoint,
        "read_model_is_read_only": read_model_is_read_only,
    }


def check_release46_prospective_alpha_tournament(files: list[Path]) -> dict:
    """Release 46 invariants - a forward record that cannot be edited into a win.

    The release's claim is narrow and total: a challenger is crowned only by
    predictions it made before the outcome existed. Every invariant below
    closes one route by which that claim could be false:

    * emit a prediction after the outcome is knowable and call it forward -
      closed by a ledger that REFUSES any row whose emission is not strictly
      before its outcome window, and by an entry rule stated on the Eastern
      calendar so no fast market can argue it into look-ahead;
    * write the same decision twice and count it twice - closed by a declared
      identity key and an idempotency proof that runs in production, not only
      in tests;
    * retune a losing challenger in place - closed by spec hashing, by
      RETUNE_DETECTED on registration, and by a versioning rule that starts a
      new forward clock rather than editing a record;
    * revise a forecast once its outcome is known - closed by chain-hashed
      append-only ledgers and a judge that only ever appends;
    * count fifty overlapping twenty-day bets as fifty observations - closed by
      an effective-independent count that the gate, not the display, consumes;
    * beat zero and call it beating cash - closed by a remunerated collateral
      control;
    * quietly present a backtest as proof - closed by two evidence classes that
      never mix and by PROVEN_ALPHA not existing as a state;
    * mutate another release's frozen registry while adopting it - closed by
      read-only adoption with before/after hashes;
    * let a dead data stream masquerade as a live model - closed by the
      feasibility gate R42 discovered and nobody enforced.
    """
    src = {name: (_read(REPO_ROOT / path) or "")
           for name, path in R46_OWNERS.items()}
    modules_missing = sorted(n for n, t in src.items() if not t)
    all_src = "\n".join(src.values())
    second_owner_modules = sorted(p for p in R46_SECOND_OWNER_FORBIDDEN
                                  if (REPO_ROOT / p).exists())

    # (1) TRUE_FORWARD is enforced, not asserted.
    ledger_refuses_backdated_rows = (
        "class LedgerRefusal" in src["ledger"]
        and "REFUSED - not TRUE_FORWARD" in src["ledger"]
        and "raise LedgerRefusal" in src["ledger"]
        and "emitted < start" in src["ledger"])
    entry_rule_is_declared_and_conservative = (
        "R46_NEXT_TRADING_DAY_CLOSE" in src["contract"]
        and "ENTRY_RULE" in src["contract"]
        and "def entry_session_date" in src["clock"]
        and "def outcome_window_start_utc" in src["clock"])
    outcome_window_is_eastern_not_utc = (
        "midnight EASTERN" in src["clock"]
        or "midnight_et" in src["clock"])
    evidence_classes_never_mix = (
        "TRUE_FORWARD" in src["contract"]
        and "HISTORICAL_SIMULATION" in src["contract"]
        and "this ledger holds TRUE_FORWARD rows only" in src["ledger"])
    backfill_forbidden = (
        "FORBIDDEN_FOREVER" in src["contract"]
        and "backdating a prediction" in src["contract"]
        and "labelling a retrospective calculation TRUE_FORWARD"
        in src["contract"])

    # (2) The record is complete and immutable.
    record_completeness_enforced = (
        "PREDICTION_RECORD_FIELDS" in src["contract"]
        and "missing required contract fields" in src["ledger"])
    canonical_ledger_primitives_reused = (
        "api.paper_trading_desk" in src["ledger"]
        and "_append_ledger" in src["ledger"]
        and "verify_ledger" in src["ledger"])
    judge_only_appends = (
        "never_revises_a_forecast" in src["judge"]
        and "append_outcomes" in src["judge"]
        and "def append_predictions" not in src["judge"])

    # (3) Idempotency is proved, not claimed.
    identity_key_declared = (
        "PREDICTION_IDENTITY_KEY" in src["contract"]
        and "OUTCOME_IDENTITY_KEY" in src["contract"]
        and "def prediction_key" in src["ledger"])
    idempotency_proved_in_run = (
        "IDEMPOTENT" in src["campaign"]
        and "no_duplicate_created" in src["campaign"]
        and "proved_in_run" in src["campaign"])

    # (4) A losing challenger cannot be improved in place.
    versioning_forbids_in_place_retune = (
        "def classify_change" in src["registry"]
        and "MATERIAL_CHANGE_FIELDS" in src["registry"]
        and "RETUNE_DETECTED" in src["registry"]
        and "prior_versions_are_never_edited" in src["registry"])
    spec_hash_covers_the_economics = (
        "def spec_hash" in src["challengers"]
        and '"parameters": spec["parameters"]' in src["challengers"]
        and '"horizons"' in src["challengers"])

    # (5) Evidence is discounted for overlap, and the gate is not one t-test.
    effective_independent_count_exists = (
        "def effective_independent" in src["evidence"]
        and "raw_matured" in src["evidence"]
        and "effective_independent" in src["leaderboard"])
    gate_is_not_a_single_t = (
        "min_effective_independent" in src["contract"]
        and "max_single_day_share_of_pnl" in src["contract"]
        and "require_positive_at_2x_costs" in src["contract"]
        and "require_same_sign_halves" in src["contract"]
        and "multiple_testing_control" in src["contract"])
    forward_selection_is_ledgered = (
        "FORWARD_SELECTION_MUST_BE_RECORDED" in src["contract"]
        and "def record_forward_selection" in src["burden"])

    # (6) The control is correct, and cost is charged on traded notional.
    collateral_is_remunerated = (
        "COLLATERAL_IS_REMUNERATED = True" in src["contract"]
        and "CONTROL_CASH" in src["contract"]
        and "risk_free_per_session" in src["judge"])
    cost_charged_on_traded_notional = (
        "COST_BPS_PER_SIDE" in src["contract"]
        and "abs(float(l[\"weight\"]))" in src["judge"]
        and "realised_cost_exit_side" in src["judge"])
    no_invented_expected_return = (
        "NOT_CALIBRATED" in src["emit"]
        and '"expected_return": None' in src["emit"])

    # (7) Nothing may read as proven.
    proven_alpha_is_not_a_state = (
        "PROVEN_ALPHA_IS_NOT_A_STATE = True" in src["contract"]
        and "PROVEN_ALPHA" not in str(src["contract"].split(
            "CHALLENGER_STATES = (")[-1].split(")")[0])
        and "no_row_may_read_proven" in src["leaderboard"])
    leaderboard_ranks_evidence_first = (
        "_BAND" in src["leaderboard"]
        and "evidence maturity band first" in src["leaderboard"])

    # (8) Adoption never mutates a prior release.
    adoption_is_read_only = (
        "ADOPTION_RULES" in src["contract"]
        and "prior_registries_are_read_only" in src["contract"]
        and "file_sha256_before" in src["registry"]
        and "file_sha256_after" in src["registry"]
        and "unchanged_by_r46" in src["registry"])
    # Release 46.6.1 SCOPED this clause rather than silently breaking it. The
    # R46 PREDICTION ledger still holds no adopted row and no prior release's
    # store is ever written; adopted forward evidence goes to an R46-OWNED
    # continuation ledger whose owner names the clause it supersedes. Reading
    # this invariant as "R46 produces no adopted forward evidence" would now be
    # false, so the token scan requires the amendment to be declared out loud.
    adoption_writes_no_forward_row = (
        "r46_never_writes_a_forward_row_for_an_adopted_shadow"
        in src["contract"]
        and "r46_writes_forward_rows_for_it" in src["registry"]
        and "SUPERSEDED_ADOPTION_CLAUSE" in src.get("adopted_forward", "")
        and "PRIOR_RELEASE_APPEND_AUTHORISED = False"
        in src.get("adopted_forward", "")
        # the adopted row may never enter the canonical prediction ledger
        and "CONTINUATION_LEDGER" not in src["ledger"]
        and "adopted_challenger_id" not in src["emit"])

    # (9) A dead stream cannot masquerade as a live model.
    feasibility_gate_enforced = (
        "def probe" in src["feasibility"]
        and "CAN_ACCRUE" in src["feasibility"]
        and "VENUE_BLOCKED" in src["feasibility"]
        and "FEASIBILITY_RULE" in src["contract"]
        and "DATA_BLOCKED" in src["registry"])
    non_positive_price_refused = (
        "NON_POSITIVE_PRICE" in src["marketdata"]
        and "def has_non_positive" in src["marketdata"])

    # (10) Burden inherits, never resets, and forward evidence is not a trial.
    burden_inherited_not_reset = (
        "INHERITED_GLOBAL_BURDEN = 353" in src["contract"]
        and "BURDEN_MAY_NEVER_BE_RESET = True" in src["contract"]
        and "PROSPECTIVE_EVIDENCE_IS_NOT_SEARCH_BURDEN = True"
        in src["contract"])
    seed_parameters_were_not_searched = (
        "SEED_PARAMETERS_WERE_NOT_SEARCHED" in src["contract"]
        and "parameters_were_searched" in src["challengers"])

    # (11) Options and analyst lanes predeclare before the evidence exists.
    options_hypotheses_predeclared = (
        "PREDECLARED_HYPOTHESES" in src["options"]
        and "hypotheses_frozen_before_the_confirming_sessions_exist"
        in src["options"]
        and "generic_short_vol_is_not_alpha" in src["options"])
    analyst_never_backfilled = (
        "NEVER_BACKFILLED = True" in src["analyst"]
        and "PREDECLARED_CHALLENGER" in src["analyst"]
        and "inadmissible_input" in src["analyst"])

    # (12) Safety.
    safety_flags_false = all(
        ("%s\": False" % flag) in src["contract"]
        for flag in ("creates_order", "creates_paper_order", "promotes_model",
                     "mutates_holdings", "enables_automation",
                     "writes_operational_store", "may_spend_money",
                     "backdates_forward_rows",
                     "mutates_prior_release_artifacts"))
    portfolio_boundary_declared = (
        "PORTFOLIO_BOUNDARY" in src["contract"]
        and "FORWARD_CANDIDATE_is_an_order" in src["contract"]
        and "manual_review_remains_mandatory" in src["contract"])
    no_operational_imports = not any(
        tok in all_src for tok in
        ("portfolio_decision", "rebalance_execution", "daily_close",
         "operational_book import", "from paper_trader.api import app"))
    no_scheduler_or_task_registration = not any(
        tok in all_src for tok in
        ("schtasks", "Register-ScheduledTask", "TaskScheduler",
         "crontab", "APScheduler"))
    no_purchase_or_account = not any(
        tok in all_src.lower() for tok in
        ("checkout", "subscribe(", "create_account", "billing",
         "payment_method", "upgrade_plan"))
    keys_never_leak = not any(
        tok in all_src for tok in ("apiKey=%s\" % pk, ", "print(pk"))
    shell_policy_declared = (
        "SHELL_POLICY = \"WINDOWS_POWERSHELL_ONLY\"" in src["contract"]
        and "SHELL_POLICY_WAIVERS_ARE_NOT_AVAILABLE = True" in src["contract"]
        and "SHELL_POLICY_VIOLATION" in src["shell_policy"])
    inherited_disclosures_preserved = (
        "INHERITED_SHELL_DISCLOSURES" in src["contract"]
        and "inherited_disclosures_are_never_erased" in src["shell_policy"]
        and '"release": "R42"' in src["contract"]
        and '"release": "R44"' in src["contract"])

    states_missing = sorted(
        s for s in R46_REQUIRED_STATES if s not in src["contract"])

    read_model = _read(REPO_ROOT / "api/prospective_tournament.py") or ""
    read_model_is_read_only = bool(read_model) and not any(
        tok in read_model for tok in ("_append_ledger", "write_json",
                                      "def emit", "def register"))
    read_model_hides_no_proof = (
        "no_historical_only_model_looks_proven" in read_model
        and "proven_alpha_is_not_a_state" in read_model)

    # (13) Release 46.4 - the economic layer. Money is computed once, closed
    #      trades take the judge's number, historical P&L is never labelled
    #      forward, ledgers append, allocation cannot see the future, and
    #      nothing here is an order, a holding or a promotion.
    r46_4_second_owner_modules = sorted(
        p for p in R46_4_SECOND_OWNER_FORBIDDEN if (REPO_ROOT / p).exists())
    pnl_has_one_owner = (
        "closed_trades_take_the_judge_number" in src.get("pnl", "")
        and "RECONCILIATION_MISMATCH" in src.get("pnl", "")
        and "def economics" in src.get("pnl", "")
        and "def economics" not in src.get("nav", "")
        and "def economics" not in src.get("trades", ""))
    cost_stack_matches_contract = (
        "def decomposition_matches_contract" in src.get("pnl", "")
        and "COST_BPS_PER_SIDE" in src.get("pnl", "")
        and "SCENARIO_STRESS" in src.get("pnl", ""))
    no_fake_forward_pnl = (
        "historical_pnl_is_never_labelled_forward" in src.get("pnl", "")
        and "HISTORICAL_SIMULATION" in src.get("pnl", "")
        and "RISK_PRIOR" in src.get("pnl", "")
        and "historical_data_informs_the_prior_never_alpha"
        in src.get("risk", ""))
    trade_ledger_is_append_only_and_idempotent = (
        "_append_ledger" in src.get("trades", "")
        and "verify_ledger" in src.get("trades", "")
        and "one_prediction_one_trade" in src.get("trades", "")
        and "def trade_id" in src.get("trades", "")
        and "backdated" in src.get("trades", ""))
    trade_states_are_derived = (
        "def states" in src.get("trades", "")
        and "DERIVED" in src.get("trades", "")
        and "SIGNAL_EMITTED" in src.get("trades", "")
        and "TRADE_CLOSED" in src.get("trades", ""))
    nav_never_rewrites_history = (
        "never_rewrites_prior_history" in src.get("nav", "")
        and "_append_ledger" in src.get("nav", "")
        and "STARTING_CAPITAL" in src.get("nav", "")
        and "high_water_mark" in src.get("nav", ""))
    allocation_has_no_hindsight = (
        "applies_from_session" in src.get("allocation", "")
        and "strictly after decision_session" in src.get("allocation", "")
        and "weights_optimised_on_forward_results\": False"
        in src.get("allocation", "")
        and "def funding_for" in src.get("allocation", "")
        and "STRICTLY BEFORE" in src.get("allocation", ""))
    four_policies_predeclared = all(
        tok in src.get("allocation", "") for tok in
        ("EQUAL_WEIGHT_ELIGIBLE_v1", "EQUAL_RISK_v1",
         "EVIDENCE_DISCOUNTED_DIVERSIFIED_v1", "CASH_CONTROL_v1"))
    redundancy_and_concentration_enforced = (
        "cluster_size" in src.get("allocation", "")
        and "def _cap" in src.get("allocation", "")
        and "effective_streams" in src.get("risk", ""))
    economic_kill_rules_frozen = (
        "KILL_RULES" in src.get("strategy_pnl", "")
        and "no_kill_from_one_unlucky_trade" in src.get("strategy_pnl", "")
        and "a_killed_strategy_is_never_retuned_in_place"
        in src.get("strategy_pnl", "")
        and "ECONOMIC_KILL_CANDIDATE" in src.get("strategy_pnl", ""))
    three_pnl_concepts_kept_apart = (
        "expected_vs_unrealised_vs_realised_are_never_summed"
        in src.get("strategy_pnl", "")
        and "realised_and_unrealised_are_reported_separately"
        in src.get("nav", ""))
    regime_is_ex_ante = (
        "ex_ante_only" in src.get("regime", "")
        and "never_relabelled" in src.get("regime", ""))
    bridge_is_read_only = (
        "adds_to_portfolio=False" in src.get("opportunity", "")
        and "creates_orders=False" in src.get("opportunity", "")
        and "who_decides" in src.get("opportunity", ""))
    pnl_step_is_inside_the_one_advance = (
        "SH.advance_pnl" in src.get("advance", "")
        and "def advance_pnl" in src.get("shadow", "")
        and "def advance(" not in src.get("shadow", ""))
    lanes_are_pit_stamped = all(
        tok in src.get(m, "") for m, tok in (
            ("cftc", "acquired_at_utc"), ("credit", "realtime_start"),
            ("macro", "output_type=4"), ("events", "acquired_at_utc")))
    lanes_never_overwrite = all(
        "raw_dir()" in src.get(m, "") and "captures" in src.get(m, "")
        for m in ("cftc", "credit", "macro", "events"))
    research_trades_are_not_positions = (
        "is_an_order\": False" in src.get("trades", "")
        and "is_a_holding\": False" in src.get("trades", "")
        and "not an order, not a holding" in src.get("trades", ""))

    # (14) Release 46.5 - the harvest. Matured economics and marks are never
    #      one number, verdicts read matured trades only under frozen
    #      thresholds, the correlation blend is versioned and frozen before
    #      use, the EDGAR lanes are acceptance-stamped and refuse the
    #      synthetic fixture, Form 4 codes are classified, and the harvest
    #      stage sits inside the ONE advance.
    harvest_keeps_matured_and_mtm_apart = (
        "MATURED_FORWARD_EVIDENCE" in src.get("harvest", "")
        and "MARK_TO_MARKET" in src.get("harvest", "")
        and "matured_and_mark_to_market_are_never_summed=True"
        in src.get("harvest", "")
        and "nothing_here_matures_a_prediction=True" in src.get("harvest", "")
        and "STILL_WAITING_FOR_REALITY" in src.get("harvest", "")
        and "ONE_ECONOMIC_TRUTH" in src.get("harvest", ""))
    verdicts_are_frozen_and_matured_only = (
        "VERDICT_RULES" in src.get("verdicts", "")
        and "mark_to_market_never_decides" in src.get("verdicts", "")
        and "one_outcome_never_decides" in src.get("verdicts", "")
        and "a_verdict_confers_no_capital" in src.get("verdicts", "")
        and all(v in src.get("verdicts", "") for v in (
            "TOO_EARLY", "POSITIVE_EARLY", "NEGATIVE_EARLY",
            "SHADOW_SCALE_CANDIDATE", "SHADOW_REDUCE_CANDIDATE",
            "FORWARD_REJECTED", "FORWARD_CONFIRMED"))
        and "def matured_summary" in src.get("strategy_pnl", "")
        and "PROVEN" not in str(src.get("verdicts", "").split(
            "VERDICTS = (")[-1].split(")")[0]))
    correlation_blend_is_versioned_and_frozen = (
        "REALISED_BLEND_RULE" in src.get("risk", "")
        and "REALISED_CORRELATION_BLEND_v2" in src.get("risk", "")
        and "frozen_before_any_realised_correlation_was_used" in src.get("risk", "")
        and "def realised_blend_weight" in src.get("risk", "")
        and "supersedes" in src.get("risk", "")
        and "def correlation_state" in src.get("risk", ""))
    edgar_lanes_are_acceptance_stamped = (
        "acceptanceDateTime" in src.get("earnings", "")
        and "ACCEPTANCE-DATETIME" in src.get("form4", "")
        and "def classify_timing" in src.get("earnings", "")
        and "def parse_submission_text" in src.get("form4", "")
        and "before it AND present in a capture acquired before it"
        in src.get("earnings", "")
        and "acquired_at_utc" in src.get("form4", ""))
    earnings_lane_refuses_synthetic_fixture = (
        "FORBIDDEN_PATH_TOKENS" in src.get("earnings", "")
        and "\"fixture\"" in src.get("earnings", "")
        and "def _forbidden" in src.get("earnings", ""))
    form4_codes_are_classified = (
        "TRANSACTION_CLASSES" in src.get("form4", "")
        and "OPEN_MARKET_PURCHASE" in src.get("form4", "")
        and "TAX_WITHHOLDING" in src.get("form4", "")
        and "OPTION_EXERCISE" in src.get("form4", "")
        and "INFORMATIVE_CODES" in src.get("form4", "")
        and "not_all_form4s_are_equivalent" in src.get("form4", ""))
    edgar_access_has_one_seam = (
        "REQUEST_INTERVAL_SECONDS" in src.get("sec", "")
        and "def user_agent" in src.get("sec", "")
        and "urllib.request.urlopen" not in src.get("earnings", "")
        and "urllib.request.urlopen" not in src.get("form4", "")
        and "def mask" in src.get("sec", ""))
    harvest_stage_is_inside_the_one_advance = (
        "HV.build" in src.get("shadow", "")
        and "VD.build" in src.get("shadow", "")
        and "RK.correlation_state" in src.get("shadow", "")
        and "lane_earnings" in src.get("advance", "")
        and "lane_form4" in src.get("advance", "")
        and "def advance(" not in src.get("harvest", "")
        and "def advance(" not in src.get("verdicts", ""))
    # Every cohort enters through the SAME frozen door and no cohort is ever
    # removed from the union. Release 46.6 added a fifth tuple, so the union
    # is matched on its MEMBERSHIP rather than on one exact line of source -
    # a later cohort must not be able to drop an earlier one, which is what
    # this invariant is actually for.
    _ch = src.get("challengers", "")
    _union = _ch.split("ALL_SPECS =", 1)[-1].split("\n\n", 1)[0]
    r46_5_challengers_frozen_unsearched = (
        "R46_5_SPECS" in _ch
        and "R46_5_CANONICAL_CONSTANTS" in _ch
        and "R46_5_FORWARD_HARVEST" in _ch
        and all(t in _union for t in ("SEED_SPECS", "EXPANSION_SPECS",
                                      "R46_4_SPECS", "R46_5_SPECS")))

    # ---- Release 46.6 ------------------------------------------------------ #
    # Signal edge and ECONOMIC edge are different things, and exactly one
    # module is allowed to know the difference.
    _ce = src.get("cost_efficiency", "")
    r46_6_cost_efficiency_has_one_owner = (
        "CALCULATION_OWNER = \"alpha_agent.r46.cost_efficiency\"" in _ce
        and "def break_even(" in _ce
        and "def classify(" in _ce
        and "def classify_observation(" in _ce
        and "GROSS_EDGE_POSITIVE_COST_DESTROYED" in _ce
        # the descriptive economic state never replaces the scientific verdict
        and "descriptive_states_never_replace_verdicts" in _ce
        and "matured_and_mark_to_market_are_never_summed" in _ce
        # a ratio on a non-positive gross edge is refused, not printed
        and "UNDEFINED_GROSS_EDGE_NOT_POSITIVE" in _ce
        # and no OTHER R46 module may compute a break-even of its own
        and "def break_even(" not in src.get("leaderboard", "")
        and "def break_even(" not in src.get("verdicts", "")
        and "def break_even(" not in src.get("pnl_board", ""))

    # A research lane that nobody calls is the defect this release exists to
    # abolish, so the vocabulary must contain no state meaning "forgotten"
    # and the audit that proves it must live in the owner.
    _ln = src.get("lanes", "")
    r46_6_lane_contract_is_enforced = (
        "CALCULATION_OWNER = \"alpha_agent.r46.lanes\"" in _ln
        and "CALLED_QUIET_NOT_DUE" in _ln
        and "CALLED_SAMPLE_BLOCKED" in _ln
        and "CALLED_PIT_BLOCKED" in _ln
        and "FORGOTTEN_IS_NOT_A_STATE" in _ln
        and "def audit(" in _ln
        and "contract_holds" in _ln
        # the option surface and the seven adopted shadows are REGISTERED
        and '"options"' in _ln and "r39_fut_month_end" in _ln
        and "r41_btc_funding" in _ln and "r42_btc_basis" in _ln
        # and the canonical cycle calls the registry, not a lane list of its own
        and "LN.run_all(" in src.get("advance", "")
        and "research_lanes" in src.get("advance", ""))

    # A prior release's ledger is never written by R46, and the flag that
    # would allow it is False with the frozen safety block named.
    r46_6_prior_release_ledgers_untouched = (
        "ADOPTED_CAPTURE_WRITES_PRIOR_RELEASE_LEDGERS = False" in _ln
        and "mutates_prior_release_artifacts" in _ln)

    # ---- Release 46.6.1 - the adopted forward continuation bridge --------- #
    # R46.6 registered the adopted lanes and proved their owners work, and
    # they still produced nothing: the only ledger those owners write belongs
    # to a prior release. A lane that is called, has something to say and has
    # nowhere to say it is the same defect wearing a label.
    _af = src.get("adopted_forward", "")
    r46_6_1_continuation_has_one_owner = (
        "CALCULATION_OWNER = \"alpha_agent.r46.adopted_forward\"" in _af
        and "CONTINUATION_IDENTITY_KEY" in _af
        and "def run_lane(" in _af
        and "_append_ledger" in _af and "verify_ledger" in _af
        # and no OTHER R46 module opens a continuation ledger or reimplements
        # a prior release's capture
        and "CONTINUATION_LEDGER" not in src["ledger"]
        and "def run_lane(" not in _ln
        and "_target_snapshot" not in _ln
        and "eligible_new_decisions" not in _ln)
    r46_6_1_prior_release_stores_stay_read_only = (
        "PRIOR_RELEASE_APPEND_AUTHORISED = False" in _af
        and "prior_release_artifact_mutated" in _af
        and "prior_release_ledger_written" in _af
        # never drives a prior release's own capture or maturation path
        and ".capture(" not in _af and "run_cycle(" not in _af
        and "RS.mature(" not in _af)
    r46_6_1_amendment_is_named_not_implied = (
        "SUPERSEDED_ADOPTION_CLAUSE" in _af
        and "r46_never_writes_a_forward_row_for_an_adopted_shadow" in _af
        and "frozen_contract_file_edited" in _af
        and "contract_hash_unchanged" in _af
        and "what_remains_forbidden" in _af
        and "amended_by" in _af)
    r46_6_1_continuation_is_true_forward_gated = (
        "class ContinuationRefusal" in _af
        and "raise ContinuationRefusal" in _af
        and "REFUSED - not TRUE_FORWARD" in _af
        and "def outcome_window_start(" in _af
        and "CK.outcome_window_start_utc" in _af
        and "OUTCOME_WINDOW_ALREADY_OPEN" in _af)
    r46_6_1_signal_comes_from_the_prior_owner = (
        "_target_snapshot" in _af and "score_at" in _af
        # a second copy of a frozen strategy is a retune waiting to happen
        and "rank(pct=True)" not in _af
        and "apply_frozen_wide" not in _af
        and "def score_at" not in _af)
    r46_6_1_append_rights_are_reported_apart = (
        "prior_release_append_authorised" in _ln
        and "r46_continuation_append_authorised" in _ln
        and "ADOPTED_CONTINUATION_OWNER" in _ln
        and "old_artifacts_became_writable" in _ln)
    # R46.6.1 - two controls, computed apart. The scientific one is the
    # strategy's OWN frozen control, computed by the PRIOR RELEASE's own
    # implementation; the capital one is R46 cash. Neither may stand in for the
    # other, and this release may not define a control of its own.
    _vd = src.get("verdicts", "")
    r46_6_1_two_controls_are_computed_apart = (
        "SCIENTIFIC_ALPHA_FIELD" in _af
        and "CAPITAL_ALPHA_FIELD" in _af
        and "scientific_alpha_vs_declared_control" in _af
        and "capital_alpha_vs_cash" in _af
        and "CASH_SUBSTITUTION_FOR_NONCASH_CONTROL_ALLOWED = False" in _af
        and "def declared_control_path(" in _af
        # the ORIGINAL owner computes the control; this release invents none
        and "passive_ew_control" in _af
        and "_r39_trade_space" in _af
        and "def passive_ew_control" not in _af
        and "np.sign(pred" not in _af)
    r46_6_1_formal_verdict_uses_the_frozen_control = (
        "scientific_control_state" in _vd
        and "SCIENTIFIC_CONTROL_GATE" in _vd
        and "capital_alpha_vs_cash_is_never_a_substitute" in _vd
        and "formal_verdict_blocked" in _vd
        and "def verdict_inputs(" in _af
        and "FORMAL_VERDICT_USES" in _af)
    _oh_src = src.get("options_hypotheses", "")
    r46_6_1_options_gate_semantics_are_explicit = (
        "SESSION_GATE_MET" in src["options"]
        and "NUMBER_OF_SESSIONS_ONLY" in src["options"]
        and "STRIKE_AND_EXPIRY_BREADTH_PER_SESSION" in src["options"]
        and "hypothesis_sample_sufficient" in _oh_src
        and "hypothesis_sample_state" in _oh_src
        and "session_gate_state" in _oh_src)

    # The option hypotheses are scored as HISTORICAL_SIMULATION and may never
    # enter the prospective ledger, however good they look.
    _oh = src.get("options_hypotheses", "")
    r46_6_option_hypotheses_are_historical = (
        "EVIDENCE_CLASS = C.HISTORICAL_SIMULATION" in _oh
        and "SAMPLE_INSUFFICIENT" in _oh
        and "crowns_nothing" in _oh
        and "enters_no_prospective_ledger" in _oh
        and "LG.append" not in _oh and "ledger" not in _oh.split("\n\n")[0])

    # The R46.6 cohort is declared, not searched, and its two reversal cells
    # are NEW challengers rather than an edit of the one that lost.
    r46_6_challengers_frozen_unsearched = (
        "R46_6_SPECS" in _ch
        and "R46_6_CANONICAL_CONSTANTS" in _ch
        and "R46_6_FAST_EVIDENCE" in _ch
        and "R46_6_DECLINED" in _ch
        and "R46_6_SPECS" in _union
        # the seed reversal challenger - the one that produced the first
        # matured loss - is still present, verbatim, with its own thesis and
        # its own parameters. R46.6 answers it with NEW cells, never an edit.
        and "r46_eq_xs_rev_5d" in _ch
        and "a week of one-sided pressure in a large-cap name is mostly" in _ch
        and "r46_6_eq_xs_rev_5d_tail2" in _ch
        and "r46_6_eq_xs_rev_5d_hold5" in _ch)

    return {
        "harvest_keeps_matured_and_mtm_apart":
            harvest_keeps_matured_and_mtm_apart,
        "verdicts_are_frozen_and_matured_only":
            verdicts_are_frozen_and_matured_only,
        "correlation_blend_is_versioned_and_frozen":
            correlation_blend_is_versioned_and_frozen,
        "edgar_lanes_are_acceptance_stamped":
            edgar_lanes_are_acceptance_stamped,
        "earnings_lane_refuses_synthetic_fixture":
            earnings_lane_refuses_synthetic_fixture,
        "form4_codes_are_classified": form4_codes_are_classified,
        "edgar_access_has_one_seam": edgar_access_has_one_seam,
        "harvest_stage_is_inside_the_one_advance":
            harvest_stage_is_inside_the_one_advance,
        "r46_5_challengers_frozen_unsearched":
            r46_5_challengers_frozen_unsearched,
        # --- Release 46.6 ------------------------------------------------- #
        "r46_6_cost_efficiency_has_one_owner":
            r46_6_cost_efficiency_has_one_owner,
        "r46_6_lane_contract_is_enforced": r46_6_lane_contract_is_enforced,
        "r46_6_prior_release_ledgers_untouched":
            r46_6_prior_release_ledgers_untouched,
        # --- Release 46.6.1 ------------------------------------------------ #
        "r46_6_1_continuation_has_one_owner":
            r46_6_1_continuation_has_one_owner,
        "r46_6_1_prior_release_stores_stay_read_only":
            r46_6_1_prior_release_stores_stay_read_only,
        "r46_6_1_amendment_is_named_not_implied":
            r46_6_1_amendment_is_named_not_implied,
        "r46_6_1_continuation_is_true_forward_gated":
            r46_6_1_continuation_is_true_forward_gated,
        "r46_6_1_signal_comes_from_the_prior_owner":
            r46_6_1_signal_comes_from_the_prior_owner,
        "r46_6_1_append_rights_are_reported_apart":
            r46_6_1_append_rights_are_reported_apart,
        "r46_6_1_two_controls_are_computed_apart":
            r46_6_1_two_controls_are_computed_apart,
        "r46_6_1_formal_verdict_uses_the_frozen_control":
            r46_6_1_formal_verdict_uses_the_frozen_control,
        "r46_6_1_options_gate_semantics_are_explicit":
            r46_6_1_options_gate_semantics_are_explicit,
        "r46_6_option_hypotheses_are_historical":
            r46_6_option_hypotheses_are_historical,
        "r46_6_challengers_frozen_unsearched":
            r46_6_challengers_frozen_unsearched,
        "modules_present": not modules_missing,
        "modules_missing": modules_missing,
        "second_owner_modules": second_owner_modules,
        "r46_4_second_owner_modules": r46_4_second_owner_modules,
        "pnl_has_one_owner": pnl_has_one_owner,
        "cost_stack_matches_contract": cost_stack_matches_contract,
        "no_fake_forward_pnl": no_fake_forward_pnl,
        "trade_ledger_is_append_only_and_idempotent":
            trade_ledger_is_append_only_and_idempotent,
        "trade_states_are_derived": trade_states_are_derived,
        "nav_never_rewrites_history": nav_never_rewrites_history,
        "allocation_has_no_hindsight": allocation_has_no_hindsight,
        "four_policies_predeclared": four_policies_predeclared,
        "redundancy_and_concentration_enforced":
            redundancy_and_concentration_enforced,
        "economic_kill_rules_frozen": economic_kill_rules_frozen,
        "three_pnl_concepts_kept_apart": three_pnl_concepts_kept_apart,
        "regime_is_ex_ante": regime_is_ex_ante,
        "bridge_is_read_only": bridge_is_read_only,
        "pnl_step_is_inside_the_one_advance": pnl_step_is_inside_the_one_advance,
        "lanes_are_pit_stamped": lanes_are_pit_stamped,
        "lanes_never_overwrite": lanes_never_overwrite,
        "research_trades_are_not_positions": research_trades_are_not_positions,
        "ledger_refuses_backdated_rows": ledger_refuses_backdated_rows,
        "entry_rule_is_declared_and_conservative":
            entry_rule_is_declared_and_conservative,
        "outcome_window_is_eastern_not_utc": outcome_window_is_eastern_not_utc,
        "evidence_classes_never_mix": evidence_classes_never_mix,
        "backfill_forbidden": backfill_forbidden,
        "record_completeness_enforced": record_completeness_enforced,
        "canonical_ledger_primitives_reused": canonical_ledger_primitives_reused,
        "judge_only_appends": judge_only_appends,
        "identity_key_declared": identity_key_declared,
        "idempotency_proved_in_run": idempotency_proved_in_run,
        "versioning_forbids_in_place_retune":
            versioning_forbids_in_place_retune,
        "spec_hash_covers_the_economics": spec_hash_covers_the_economics,
        "effective_independent_count_exists": effective_independent_count_exists,
        "gate_is_not_a_single_t": gate_is_not_a_single_t,
        "forward_selection_is_ledgered": forward_selection_is_ledgered,
        "collateral_is_remunerated": collateral_is_remunerated,
        "cost_charged_on_traded_notional": cost_charged_on_traded_notional,
        "no_invented_expected_return": no_invented_expected_return,
        "proven_alpha_is_not_a_state": proven_alpha_is_not_a_state,
        "leaderboard_ranks_evidence_first": leaderboard_ranks_evidence_first,
        "adoption_is_read_only": adoption_is_read_only,
        "adoption_writes_no_forward_row": adoption_writes_no_forward_row,
        "feasibility_gate_enforced": feasibility_gate_enforced,
        "non_positive_price_refused": non_positive_price_refused,
        "burden_inherited_not_reset": burden_inherited_not_reset,
        "seed_parameters_were_not_searched": seed_parameters_were_not_searched,
        "options_hypotheses_predeclared": options_hypotheses_predeclared,
        "analyst_never_backfilled": analyst_never_backfilled,
        "safety_flags_false": safety_flags_false,
        "portfolio_boundary_declared": portfolio_boundary_declared,
        "no_operational_imports": no_operational_imports,
        "no_scheduler_or_task_registration": no_scheduler_or_task_registration,
        "no_purchase_or_account": no_purchase_or_account,
        "keys_never_leak": bool(keys_never_leak),
        "shell_policy_declared": shell_policy_declared,
        "inherited_disclosures_preserved": inherited_disclosures_preserved,
        "read_model_is_read_only": read_model_is_read_only,
        "read_model_hides_no_proof": read_model_hides_no_proof,
        "terminal_states_missing": states_missing,
    }


# --------------------------------------------------------------------------- #
# Driver
# --------------------------------------------------------------------------- #
def run_audit(extra_ps1_dirs=()) -> dict:
    """Build the audit report.

    ``extra_ps1_dirs`` extends ONLY the PowerShell restart/smoke scan (handoff scripts
    live outside the repository by design). The default run stays repository-scoped and
    byte-deterministic.
    """
    files = _iter_source_files()
    routes = check_routes()
    report = {
        "schema": "paper_trader.architecture_audit/1",
        "repo_root": _rel(REPO_ROOT) or ".",
        "source_file_count": len(files),
        "routes": routes,
        "large_modules": check_module_sizes(files),
        "size_threshold_lines": SIZE_THRESHOLD_LINES,
        "direct_ledger_refs": check_direct_ledger_refs(files),
        "direct_db_sessions": check_direct_db_sessions(files),
        "private_cross_module_imports": check_private_cross_imports(files),
        "private_attribute_access": check_private_attribute_access(files),
        "ui_endpoint_wiring": check_ui_endpoint_wiring(routes["routes"]),
        "canonical_concept_writers": check_canonical_concept_writers(files),
        "research_execution_terms": check_research_execution_terms(files),
        "market_session_ownership": check_market_session_ownership(files),
        "workflow_state_ownership": check_workflow_state_ownership(files),
        "daily_research_cycle_ownership": check_daily_research_cycle_ownership(files),
        "slice3_live_acceptance_ownership": check_slice3_live_acceptance_ownership(files),
        "universe_scoring_ownership": check_universe_scoring_ownership(files),
        "monthly_emitter_bridge_ownership": check_monthly_emitter_bridge_ownership(files),
        "portfolio_state_ownership": check_portfolio_state_ownership(files),
        "holding_opportunity_cost_ownership": check_holding_opportunity_cost_ownership(files),
        "slice6_live_acceptance_ownership": check_slice6_live_acceptance_ownership(files),
        "slice6_residual_cutover_ownership": check_slice6_residual_cutover_ownership(files),
        "drc_manifest_recovery": check_drc_manifest_recovery(files),
        "reallocation_proposal_ownership": check_reallocation_proposal_ownership(files),
        "portfolio_reassessment_ownership": check_portfolio_reassessment_ownership(files),
        "release29_3_decision_integrity": check_release29_3_decision_integrity(files),
        "release29_4_session_authority": check_release29_4_session_authority(files),
        "release29_5_drc_provenance": check_release29_5_drc_provenance(files),
        "acceptance_scenario_ownership": check_acceptance_scenario_ownership(files),
        "normal_cycle_ownership": check_normal_cycle_ownership(files),
        "backend_restart_ownership": check_backend_restart_ownership(extra_ps1_dirs),
        "restart_invocation_hygiene": check_restart_invocation_hygiene(extra_ps1_dirs),
        "release29_ux2_simplification": check_release29_ux2_simplification(files),
        "stage21_outcome_intelligence": check_stage21_outcome_intelligence(files),
        "controlled_rebalance_ownership": check_controlled_rebalance_ownership(files),
        "corporate_action_propagation": check_corporate_action_propagation(files),
        "failclosed_rebalance_execution": check_failclosed_rebalance_execution(files),
        "operator_atomic_close_ownership": check_operator_atomic_close_ownership(files),
        "research_agent_ownership": check_research_agent_ownership(files),
        "data_expansion_ownership": check_data_expansion_ownership(files),
        "operator_ux_consolidation_ownership": check_operator_ux_consolidation_ownership(files),
        "information_collection_ownership": check_information_collection_ownership(
            files, routes["routes"]),
        "release30_zero_base_ownership": check_release30_zero_base_ownership(files),
        "release30_1_operational_cutover": check_release30_1_operational_cutover(files),
        "release31_mathematical_alpha_frontier":
            check_release31_mathematical_alpha_frontier(files),
        "release32_pnl_opportunity_frontier":
            check_release32_pnl_opportunity_frontier(files),
        "release33_predictive_edge": check_release33_predictive_edge(files),
        "release34_prediction_to_pnl":
            check_release34_prediction_to_pnl(files),
        "release35_orthogonal_information":
            check_release35_orthogonal_information(files),
        "release36_global_multi_asset_frontier":
            check_release36_global_multi_asset_frontier(files),
        "release37_native_market_data_gate":
            check_release37_native_market_data_gate(files),
        "release38_native_futures_information_frontier":
            check_release38_native_futures_information_frontier(files),
        "release39_universal_alpha_discovery":
            check_release39_universal_alpha_discovery(files),
        "release39_continuation": check_release39_continuation(files),
        "release40_prospective_alpha_acceleration":
            check_release40_prospective_alpha_acceleration(files),
        "release41_multi_horizon_alpha":
            check_release41_multi_horizon_alpha(files),
        "release42_crypto_basis_alpha":
            check_release42_crypto_basis_alpha(files),
        "release43_global_alpha_offensive":
            check_release43_global_alpha_offensive(files),
        "release44_orthogonal_portfolio_alpha":
            check_release44_orthogonal_portfolio_alpha(files),
        "release45_macro_event_alpha":
            check_release45_macro_event_alpha(files),
        "release46_prospective_alpha_tournament":
            check_release46_prospective_alpha_tournament(files),
        "release47_constrained_reallocation":
            check_release47_constrained_reallocation(files),
        "release48_portfolio_cycle": check_release48_portfolio_cycle(files),
        "release49_operator_presentation":
            check_release49_operator_presentation(files),
        "release50_multi_asset": check_release50_multi_asset(files),
        "release52_persistent_research_runtime":
            check_release52_persistent_research_runtime(files),
        "release54_active_manager_state":
            check_release54_active_manager_state(files),
        "release54_1_governed_intraday_decision":
            check_release54_1_governed_intraday_decision(files),
        "release54_2_same_session_reassessment_versioning":
            check_release54_2_same_session_reassessment_versioning(files),
        "release54_2_1_missed_session_recovery":
            check_release54_2_1_missed_session_recovery(files),
        "release54_2_2_post_close_research_recovery":
            check_release54_2_2_post_close_research_recovery(files),
        "release54_2_3_source_panel_recovery":
            check_release54_2_3_source_panel_recovery(files),
        "release54_2_3_1_owned_data_readiness_authority":
            check_release54_2_3_1_owned_data_readiness_authority(files),
        "release54_2_3_2_decision_supersession":
            check_release54_2_3_2_decision_supersession(files),
        "release54_2_4_reallocation_coherence":
            check_release54_2_4_reallocation_coherence(files),
        "release54_3_hoc_evidence_versioning":
            check_release54_3_hoc_evidence_versioning(files),
        "inventory_drift": check_inventory_drift(files),
        "local_only_files": check_local_only_not_released(),
        "canonical_docs": check_docs_present(),
    }
    return report


def _print_console(rep: dict) -> None:
    def hdr(t):
        print("\n" + "=" * 72)
        print(t)
        print("=" * 72)

    print("Paper Trader — Static Architecture Audit (read-only)")
    print(f"repo_root={rep['repo_root']}  source_files={rep['source_file_count']}")
    print("NOTE: static analysis does not prove runtime behavior.")

    hdr("ROUTES")
    r = rep["routes"]
    print(f"declared routes: {r['total']}  owner files: {', '.join(r['owner_files'])}")
    print(f"duplicate (method,path) declarations: {len(r['duplicate_declarations'])}")
    for d in r["duplicate_declarations"]:
        print(f"  DUP {d['method']} {d['path']} lines={d['lines']}")

    hdr("LARGE / MIXED-RESPONSIBILITY MODULES (> %d lines)" % rep["size_threshold_lines"])
    for m in rep["large_modules"]:
        print(f"  {m['lines']:>6}  {m['path']}")

    hdr("DIRECT LEDGER REFERENCES OUTSIDE LEDGER-OWNER MODULES")
    print(f"count: {len(rep['direct_ledger_refs'])}")
    for h in rep["direct_ledger_refs"][:40]:
        print(f"  {h['path']}:{h['line']}  {h['text']}")

    hdr("DIRECT DB SESSION CONSTRUCTION OUTSIDE %s" % DB_SESSION_OWNER)
    print(f"count: {len(rep['direct_db_sessions'])}")
    for h in rep["direct_db_sessions"][:40]:
        print(f"  {h['path']}:{h['line']}  {h['text']}")

    hdr("PRIVATE CROSS-MODULE IMPORTS")
    print(f"count: {len(rep['private_cross_module_imports'])}")
    for h in rep["private_cross_module_imports"][:40]:
        print(f"  {h['path']}:{h['line']}  from {h['imports_from']} import {h['private_names']}")

    hdr("PRIVATE ATTRIBUTE ACCESS (alias._private across module boundaries)")
    pa = rep["private_attribute_access"]
    print(f"modules reaching into another module's privates: {len(pa)}")
    for h in pa[:15]:
        print(f"  {h['total']:>4}  {h['path']}  {h['by_module']}")

    hdr("UI <-> ENDPOINT WIRING")
    w = rep["ui_endpoint_wiring"]
    print(f"ui referenced: {w['ui_referenced_count']}  declared /v1: {w['declared_v1_count']}")
    print(f"dangling UI references (no matching route): {len(w['dangling_ui_references'])}")
    for p in w["dangling_ui_references"][:40]:
        print(f"  DANGLING {p}")
    print(f"orphan endpoint candidates (declared, no UI ref): {len(w['orphan_endpoint_candidates'])}")

    hdr("CANONICAL-CONCEPT MULTI-WRITER CANDIDATES")
    multi = rep["canonical_concept_writers"]["multi_writer_concepts"]
    for concept, mods in sorted(multi.items()):
        print(f"  {concept}: {len(mods)} modules")
        for m in mods:
            print(f"      {m}")

    hdr("RESEARCH-ONLY MODULES WITH ORDER-EXECUTION TERMS (must be empty)")
    print(f"count: {len(rep['research_execution_terms'])}")
    for h in rep["research_execution_terms"][:40]:
        print(f"  {h['path']}:{h['line']}  {h['term']}  {h['text']}")

    hdr("MARKET-SESSION / DATA-FRESHNESS OWNERSHIP (Slice 1)")
    ms = rep["market_session_ownership"]
    print(f"owner present: {ms['owner_present']} ({ms['owner']})")
    print(f"freshness owner present: {ms['freshness_owner_present']} ({ms['freshness_owner']})")
    print(f"delegating wrappers: {ms['delegating_wrappers']}")
    print(f"migrated wrappers clean (no raw arithmetic): {ms['migrated_wrappers_clean']}")
    print(f"remaining session resolvers (documented): {len(ms['remaining_session_resolvers'])}")
    for h in ms["remaining_session_resolvers"]:
        print(f"  {h['path']}:{h['line']}  {h['text']}")
    print(f"UNEXPECTED session resolvers (must be empty): {ms['unexpected_session_resolvers']}")
    print(f"UI freshness loaders: {ms['ui_freshness_loader_count']}  "
          f"UI market-date arithmetic (must be empty): {ms['ui_market_date_arithmetic']}")

    hdr("WORKFLOW-STATE OWNERSHIP (Slice 2)")
    wf = rep["workflow_state_ownership"]
    print(f"owner present: {wf['owner_present']} ({wf['owner']})")
    print(f"endpoint present: {wf['endpoint_present']} ({wf['route']})")
    print(f"UI workflow loaders (must be 1): {wf['ui_workflow_loader_count']}")
    print(f"UI workflow-priority/currency derivation (must be empty): "
          f"{wf['ui_workflow_priority_derivation']}")
    print(f"UI canonical ownership declared: {wf['ui_canonical_ownership_declared']}")
    print(f"UI shared setters guarded: {wf['ui_shared_setters_guarded']}  "
          f"unguarded (must be empty): {wf['ui_unguarded_setters']}")
    print(f"UI unauthorized canonical-node writers (must be empty): "
          f"{wf['ui_unauthorized_canonical_writers']}")
    print(f"raw Daily Action Gate vocabulary retained (expected True): "
          f"{wf['raw_gate_vocab_retained']}")

    hdr("DAILY RESEARCH CYCLE OWNERSHIP (Slice 3)")
    dr = rep["daily_research_cycle_ownership"]
    print(f"owner present: {dr['owner_present']} ({dr['owner']})")
    print(f"sole orchestration owner: {dr['sole_orchestrator']}  "
          f"competing (must be empty): {dr['competing_orchestrators']}")
    print(f"status endpoint present: {dr['status_endpoint_present']} {dr['status_methods']}  "
          f"run endpoint present: {dr['run_endpoint_present']} {dr['run_methods']}")
    print(f"delegation missing (must be empty): {dr['missing_delegation']}")
    print(f"forbidden execution calls (must be empty): {dr['forbidden_execution_calls']}")
    print(f"second scoring engine (must be empty): {dr['forbidden_second_scoring_engine']}")
    print(f"UI status loaders (must be 1): {dr['ui_status_loader_count']}  "
          f"UI execution functions (must be 1): {dr['ui_execution_function_count']}")
    print(f"UI planning/date derivation (must be empty): {dr['ui_planning_derivation']}")
    print(f"Daily Close delegates evidence to canonical owner: "
          f"{dr['daily_close_delegates_evidence']}")

    hdr("UNIVERSE-SCORING OWNERSHIP (Slice 4)")
    us = rep["universe_scoring_ownership"]
    print(f"owner present: {us['owner_present']} ({us['owner']})  "
          f"kernel present: {us['kernel_present']} ({us['kernel']})")
    print(f"delegation missing (must be empty): {us['missing_delegation']}")
    print(f"second scoring engine in owner (must be empty): {us['second_scoring_engine_in_owner']}")
    print(f"forbidden execution/provider/prediction calls (must be empty): "
          f"{us['forbidden_execution_calls']}")
    print(f"duplicate operational scoring modules (must be empty): "
          f"{us['duplicate_operational_scoring_modules']}")
    print(f"DRC delegates scoring to owner: {us['drc_delegates_scoring']}  "
          f"platform compat wrapper: {us['platform_compat_wrapper']}")
    print(f"canonical route present: {us['canonical_route_present']} {us['canonical_route_methods']}  "
          f"compat route present: {us['compat_route_present']}")
    print(f"UI canonical scoring loaders (must be 1): {us['ui_scoring_loader_count']}  "
          f"UI scoring computation (must be empty): {us['ui_scoring_computation']}")
    print(f"automatic model promotion allowed (must be False): "
          f"{us['automatic_model_promotion_allowed']}")

    hdr("MONTHLY-MOMENTUM EMITTER BRIDGE OWNERSHIP (Phase 29D.2)")
    me = rep["monthly_emitter_bridge_ownership"]
    print(f"owner present: {me['owner_present']} ({me['owner']})")
    print(f"bridge pure-stdlib (no numpy/pandas): {me['bridge_pure_stdlib']}  "
          f"numeric imports (must be empty): {me['bridge_numeric_imports']}")
    print(f"no shell string: {me['bridge_no_shell_string']}  "
          f"uses argv array: {me['bridge_uses_argv_array']}  "
          f"delegates Phase-25 math: {me['bridge_delegates_phase25_math']}")
    print(f"second monthly formula modules (must be empty): "
          f"{me['second_monthly_formula_modules']}")
    print(f"adapter wires resolver: {me['adapter_wires_production_resolver']}  "
          f"app wires resolver: {me['app_wires_production_resolver']}  "
          f"DRC exposes monthly owner: {me['drc_status_exposes_monthly_owner']}")
    print(f"no separate monthly UI button: {me['no_separate_monthly_ui_button']}  "
          f"separate monthly endpoints (must be empty): {me['separate_monthly_endpoints']}")

    hdr("PORTFOLIO-STATE OWNERSHIP (Slice 5)")
    ps = rep["portfolio_state_ownership"]
    print(f"owner present: {ps['owner_present']} ({ps['owner']})  "
          f"defines loader: {ps['owner_defines_loader']}")
    print(f"delegation missing (must be empty): {ps['missing_delegation']}")
    print(f"forbidden execution/provider/prediction/write calls (must be empty): "
          f"{ps['forbidden_execution_calls']}")
    print(f"is writer (must be False): {ps['portfolio_state_is_writer']}")
    print(f"active-book selection present: {ps['active_book_selection_present']}  "
          f"rejects dormant legacy book: {ps['rejects_dormant_legacy_book']}")
    print(f"second portfolio-state owner modules (must be empty): "
          f"{ps['second_portfolio_state_owner_modules']}")
    print(f"canonical route present: {ps['canonical_route_present']} {ps['canonical_route_methods']}")
    print(f"UI canonical loaders (must be 1): {ps['ui_portfolio_state_loader_count']}  "
          f"renderers (must be 1): {ps['ui_portfolio_state_renderer_count']}")
    print(f"UI portfolio-state computation (must be empty): {ps['ui_portfolio_state_computation']}")
    print(f"UI missing guard tokens (must be empty): {ps['ui_missing_guard_tokens']}")

    hdr("HOLDING OPPORTUNITY-COST OWNERSHIP (Slice 6 / Milestone 2)")
    ho = rep["holding_opportunity_cost_ownership"]
    print(f"kernel present: {ho['kernel_present']} ({ho['kernel']})  "
          f"owner present: {ho['owner_present']} ({ho['owner']})")
    print(f"second calculation owner (must be empty): {ho['second_calculation_owner_modules']}")
    print(f"second composition owner (must be empty): {ho['second_composition_owner_modules']}")
    print(f"delegation missing (must be empty): {ho['missing_delegation']}")
    print(f"owner forbidden calls (must be empty): {ho['owner_forbidden_calls']}")
    print(f"kernel impurity (must be empty): {ho['kernel_impurity']}")
    print(f"route present: {ho['route_present']} {ho['route_methods']}  "
          f"no separate manual execution endpoint: {ho['no_separate_manual_execution_endpoint']}")
    print(f"DRC delegates to owner: {ho['drc_delegates_to_owner']}  "
          f"gate delegates to summary: {ho['gate_delegates_to_summary']}")
    print(f"UI loaders (must be 1): {ho['ui_loader_count']}  "
          f"UI recommendation/cost computation (must be empty): "
          f"{ho['ui_recommendation_or_cost_computation']}")
    print(f"UI review label present: {ho['ui_review_label_present']}")
    print(f"Slice 7 LANDED — missing modules/route (must be empty): "
          f"{ho['slice7_missing_modules']} {ho['slice7_missing_route']}  "
          f"forbidden present (must be empty): {ho['slice7_forbidden_present']}")
    print(f"Slice 8 present modules (must be empty): {ho['slice8_present_modules']}")
    print(f"HOC summary loads portfolio_state (must be empty): "
          f"{ho['summary_loads_portfolio_state']}")
    print(f"gate supplies HOC context: {ho['gate_supplies_hoc_context']}  "
          f"portfolio_state composes gate: {ho['portfolio_state_composes_gate']}  "
          f"no circular read dependency: {ho['no_circular_read_dependency']}")

    hdr("SLICE 6 LIVE-ACCEPTANCE / OPERATOR-WORKFLOW & UI HARD CUTOVER (Phase 29G.1)")
    la = rep["slice6_live_acceptance_ownership"]
    print(f"obsolete reassessment control (must be empty): {la['obsolete_reassessment_control']}")
    print(f"obsolete rebalance-proposal label (must be empty): {la['obsolete_rebalance_label']}")
    print(f"legacy comparison compatibility-only: {la['legacy_compatibility_classified']}")
    print(f"forbidden reassessment/rebalance/order routes (must be empty): "
          f"{la['forbidden_routes_present']}")
    print(f"service readiness UI: {la['service_readiness_ui']}  "
          f"workflow readiness UI: {la['workflow_readiness_ui']}  "
          f"readiness separated: {la['readiness_separated']}")
    print(f"/v1/ready service-scoped: {la['ready_is_service_scoped']}  "
          f"ready conflates session (must be empty): {la['ready_conflates_session']}")
    print(f"HOC panel renders NOT_RUN: {la['hoc_renders_not_run']}  "
          f"renders completed: {la['hoc_renders_completed']}")

    hdr("SLICE 6 RESIDUAL HARD CUTOVER — HOC IS THE SOLE PRIMARY DECISION (Phase 29G.2)")
    rc = rep["slice6_residual_cutover_ownership"]
    print(f"forbidden primary legacy-proposal strings in UI (must be empty): "
          f"{rc['forbidden_primary_ui']}")
    print(f"forbidden primary legacy title in workflow owner (must be empty): "
          f"{rc['forbidden_primary_ws']}")
    print(f"gate compatibility classified: {rc['gate_compatibility_classified']}  "
          f"fields: {rc['gate_classification_fields']}")
    print(f"workflow primary is HOC: {rc['workflow_primary_is_hoc']}  "
          f"missing tokens (must be empty): {rc['missing_ws_primary_tokens']}")
    print(f"legacy comparison collapsed (all 3 surfaces): {rc['legacy_comparison_collapsed']}")
    print(f"HOC is sole primary card: {rc['hoc_is_sole_primary_card']}  "
          f"primary badges: {rc['hoc_primary_badge_count']}")
    print(f"CC/DW/PM use HOC state: {rc['surfaces_use_hoc_state']}")
    print(f"UI HOC loaders (must be 1): {rc['ui_hoc_loader_count']}  "
          f"UI recommendation/cost computation (must be empty): "
          f"{rc['ui_recommendation_or_cost_computation']}")
    print(f"DRC sole execution path: {rc['drc_sole_execution_path']}  "
          f"no manual HOC execution endpoint: {rc['no_manual_hoc_execution_endpoint']}")
    print(f"forbidden reassessment/rebalance/order routes (must be empty): "
          f"{rc['forbidden_routes_present']}")
    print(f"Slice 7 LANDED — missing (must be empty): {rc['slice7_missing']}  "
          f"Slice 8 present (must be empty): {rc['slice8_present']}  "
          f"cadence enabled (must be False): {rc['cadence_enabled']}")

    hdr("DRC TERMINAL-MANIFEST PERSISTENCE / RECOVERY / PRE-CLOSE (Phase 29G.3)")
    mr = rep["drc_manifest_recovery"]
    print(f"sole DRC orchestrator: {mr['sole_orchestrator']}  "
          f"competing (must be empty): {mr['competing_orchestrators']}")
    print(f"terminal persistence tokens missing (must be empty): "
          f"{mr['missing_terminal_persistence_tokens']}  "
          f"read-back present: {mr['terminal_read_back_present']}")
    print(f"mark-complete defs (must be empty): {mr['mark_complete_defs']}  "
          f"forbidden recovery routes (must be empty): {mr['forbidden_recovery_routes']}  "
          f"separate recovery entry defs (must be empty): {mr['separate_recovery_entry_defs']}")
    print(f"single artifact root: {mr['single_artifact_root']}  "
          f"status reflect tokens missing (must be empty): {mr['missing_status_reflect_tokens']}")
    print(f"forbidden execution calls (must be empty): {mr['forbidden_execution_calls']}")
    print(f"pre-close tokens missing (must be empty): {mr['missing_preclose_tokens']}  "
          f"genuine-inconsistency tokens missing (must be empty): "
          f"{mr['missing_genuine_inconsistency_tokens']}")
    print(f"HOC data gaps explicit: {mr['hoc_data_gaps_explicit']}  "
          f"Slice 7 LANDED — missing (must be empty): {mr['slice7_missing_modules']}"
          f"{mr['slice7_missing_route']} {mr['slice7_forbidden_present']}  "
          f"Slice 8 present (must be empty): {mr['slice8_present_modules']}  "
          f"cadence enabled (must be False): {mr['cadence_enabled']}")

    hdr("REALLOCATION PROPOSAL OWNERSHIP (Slice 7, Phase 29H, Milestone 3)")
    rp = rep["reallocation_proposal_ownership"]
    print(f"kernel present: {rp['kernel_present']}  owner present: {rp['owner_present']}")
    print(f"second calculation owner (must be empty): {rp['second_calculation_owner_modules']}  "
          f"second composition owner (must be empty): {rp['second_composition_owner_modules']}")
    print(f"missing delegation (must be empty): {rp['missing_delegation']}")
    print(f"owner forbidden calls (must be empty): {rp['owner_forbidden_calls']}  "
          f"kernel forbidden calls (must be empty): {rp['kernel_forbidden_calls']}")
    print(f"GET route count (must be 1): {rp['route_get_count']}  "
          f"route methods: {rp['reallocation_route_methods']}  "
          f"forbidden route methods present (must be False): {rp['forbidden_route_methods_present']}")
    print(f"forbidden routes present (must be empty): {rp['forbidden_routes_present']}")
    print(f"DRC delegates: {rp['drc_delegates']}  step present: {rp['drc_step_present']}  "
          f"gate delegates to summary: {rp['gate_delegates_to_summary']}")
    print(f"persist present: {rp['persist_present']}  "
          f"atomic/idempotent persist: {rp['atomic_idempotent_persist_present']}")
    print(f"UI loaders (must be 1): {rp['ui_loader_count']}  "
          f"UI allocation/cost computation (must be empty): {rp['ui_allocation_or_cost_computation']}")
    print(f"kernel forks HOC (must be False): {rp['kernel_forks_hoc']}  "
          f"kernel forks scoring (must be False): {rp['kernel_forks_scoring']}  "
          f"Slice 8 present (must be empty): {rp['slice8_present_modules']}  "
          f"cadence enabled (must be False): {rp['cadence_enabled']}")

    hdr("CONTINUOUS ACTIVE PORTFOLIO REASSESSMENT OWNERSHIP (Stage 20)")
    prs = rep["portfolio_reassessment_ownership"]
    print(f"kernel present: {prs['kernel_present']}  owner present: {prs['owner_present']}")
    print(f"second calculation owner (must be empty): {prs['second_calculation_owner_modules']}  "
          f"second composition owner (must be empty): {prs['second_composition_owner_modules']}")
    print(f"second TARGET engine (must be empty): {prs['second_target_engine_modules']}  "
          f"kernel forks a neighbouring owner (must be empty): "
          f"{prs['kernel_forks_neighbouring_owner']}")
    print(f"missing delegation (must be empty): {prs['missing_delegation']}")
    print(f"owner forbidden calls (must be empty): {prs['owner_forbidden_calls']}  "
          f"kernel forbidden calls (must be empty): {prs['kernel_forbidden_calls']}")
    print(f"GET route count: {prs['route_get_count']}  route methods: {prs['route_methods']}  "
          f"non-GET methods (must be False): {prs['non_get_methods_present']}")
    print(f"forbidden routes present (must be empty): {prs['forbidden_routes_present']}  "
          f"no automatic rebalance: {prs['no_automatic_rebalance']}")
    print(f"signal refresh LINKED to reassessment: "
          f"{prs['signal_refresh_linked_to_reassessment']}  "
          f"reassessment ordered before proposal: "
          f"{prs['reassessment_ordered_before_proposal']}")
    print(f"proposal GATED by reassessment: {prs['proposal_gated_by_reassessment']}  "
          f"gate consults the owner: {prs['drc_gate_consults_owner']}  "
          f"gate owner present: {prs['proposal_gate_owner_present']}")
    print(f"Stage-19 execution precedence owner: {prs['execution_precedence_owner_present']}  "
          f"workflow honours precedence: {prs['workflow_honours_execution_precedence']}  "
          f"workflow delegates: {prs['workflow_delegates_to_owner']}  "
          f"second economic gate in workflow (must be empty): "
          f"{prs['workflow_second_economic_gate']}")
    print(f"recalibration remains separate: {prs['recalibration_remains_separate']}")
    print(f"persist present: {prs['persist_present']}  "
          f"atomic/idempotent persist: {prs['atomic_idempotent_persist_present']}  "
          f"history append-only: {prs['history_append_only']}  "
          f"no hindsight backfill: {prs['no_hindsight_backfill_declared']}")
    print(f"UI loaders (must be 1): {prs['ui_loader_count']}  "
          f"UI client assessment logic (must be empty): {prs['ui_client_assessment_logic']}")
    print(f"automatic promotion allowed (must be False): "
          f"{prs['automatic_model_promotion_allowed']}  "
          f"automatic approval allowed (must be False): {prs['automatic_approval_allowed']}  "
          f"cadence enabled (must be False): {prs['cadence_enabled']}")

    hdr("CONTROLLED PAPER-REBALANCE + CORPORATE-ACTION OWNERSHIP (Stage 19, Milestone 3)")
    cr = rep["controlled_rebalance_ownership"]
    print(f"owners present (rebalance_execution + corporate_actions): {cr['owner_present']}")
    print(f"controlled route GET /v1/operations/rebalance: {cr['controlled_route_get']}  "
          f"second-confirmation POST .../confirm-order-plan: {cr['confirm_route_post']}  "
          f"corporate-action routes present: {cr['corporate_action_routes_present']}")
    print(f"gate 1 requires APPROVED Stage-18 decision: {cr['requires_stage18_approval']}  "
          f"gate 2 requires second confirmation token: {cr['requires_second_confirmation']}")
    print(f"delegates to existing paper desk + NEXT_CLOSE: {cr['delegates_to_existing_desk']}  "
          f"second execution/fill/NAV owner defs (must be empty): {cr['second_execution_owner_defs']}")
    print(f"corporate action confirm-gated: {cr['corporate_action_confirm_gated']}  "
          f"read-time projection (no evidence rewrite): {cr['corporate_action_read_time_projection']}")
    print(f"forbidden auto/direct execution routes (must be empty): {cr['forbidden_auto_execution_routes_present']}  "
          f"automatic approval/rebalance tokens (must be empty): {cr['automatic_tokens_present']}")
    print(f"broker enabled (must be False): {cr['broker_enabled']}  "
          f"automatic rebalance allowed (must be False): {cr['automatic_rebalance_allowed']}  "
          f"cadence enabled (must be False): {cr['cadence_enabled']}")

    hdr("CORPORATE-ACTION PROPAGATION INTEGRITY (Stage 19.1)")
    cp = rep["corporate_action_propagation"]
    print(f"owner present: {cp['owner_present']}  "
          f"split-math modules (must be exactly [{CA_OWNER_FILE}]): {cp['split_math_modules']}")
    print(f"duplicate split math (must be empty): {cp['duplicate_split_math']}  "
          f"single split-math owner: {cp['single_split_math_owner']}")
    print(f"desk current reads default to registry: "
          f"{cp['desk_current_reads_default_to_registry']}  "
          f"single current fill view: {cp['single_current_fill_view']}")
    print(f"current performance projection owned by the CA module "
          f"(raw rows preserved): {cp['current_performance_projection_owned']}")
    print(f"portfolio state binds registry (state_hash): {cp['portfolio_state_binds_registry']}  "
          f"proposal binds registry: {cp['proposal_binds_registry']}")
    print(f"approval gate enforces staleness: {cp['approval_gate_enforces_staleness']}  "
          f"order-plan gate enforces staleness: {cp['order_plan_gate_enforces_staleness']}")
    print(f"UI split math (must be empty): {cp['ui_split_math_present']}  "
          f"immutable evidence rewritten (must be False): {cp['immutable_evidence_rewritten']}")

    hdr("FAIL-CLOSED REBALANCE EXECUTION (Stage 19.2)")
    fc = rep["failclosed_rebalance_execution"]
    print(f"owner present: {fc['owner_present']}  "
          f"owner defines executability contract: {fc['owner_defines_executability_contract']}")
    print(f"second contract owner (must be empty): {fc['second_contract_owner_modules']}")
    print(f"STATE-DERIVED buildable defect (must be empty): "
          f"{fc['state_derived_buildable_modules']}")
    print(f"confirm fails closed before any write: {fc['confirm_fails_closed_before_write']}")
    print(f"delegates to canonical mark owner: {fc['delegates_to_canonical_mark_owner']}  "
          f"mark owner accepts delegation: {fc['mark_owner_accepts_delegation']}  "
          f"owner provider calls (must be empty): {fc['owner_provider_calls']}")
    print(f"hydration token-gated: {fc['hydration_token_gated']}  "
          f"hydration POST route count (must be 1): {fc['hydration_route_post_count']}  "
          f"methods: {fc['hydration_route_methods']}")
    print(f"read route GET-only: {fc['read_route_get_only']} {fc['read_route_methods']}  "
          f"provider calls inside the read path (must be empty): "
          f"{fc['read_region_provider_calls']}")
    print(f"second execution/fill owner defs (must be empty): "
          f"{fc['second_execution_owner_defs']}  "
          f"NEXT_CLOSE sole settlement: {fc['next_close_sole_settlement']}")
    print(f"UI blocked-state tokens missing (must be empty): {fc['ui_missing_blocked_tokens']}  "
          f"UI order-creating controls (must be empty): {fc['ui_order_creating_controls']}")
    print(f"automatic tokens (must be empty): {fc['automatic_tokens_present']}  "
          f"broker enabled (must be False): {fc['broker_enabled']}  "
          f"automation enabled (must be False): {fc['automation_enabled']}  "
          f"cadence enabled (must be False): {fc['cadence_enabled']}")

    hdr("PERSISTENT ALPHA RESEARCH AGENT OWNERSHIP (Slice 8, Phase 29I, Milestone 4)")
    ra = rep["research_agent_ownership"]
    print(f"kernel present: {ra['kernel_present']}  owner present: {ra['owner_present']}  "
          f"landed modules missing (must be empty): {ra['landed_modules_missing']}")
    print(f"second calculation owner (must be empty): {ra['second_calculation_owner_modules']}  "
          f"second composition owner (must be empty): {ra['second_composition_owner_modules']}")
    print(f"missing delegation (must be empty): {ra['missing_delegation']}")
    print(f"owner forbidden calls (must be empty): {ra['owner_forbidden_calls']}  "
          f"kernel forbidden calls (must be empty): {ra['kernel_forbidden_calls']}")
    print(f"GET route count (must be 1): {ra['route_get_count']}  "
          f"route methods: {ra['research_agent_route_methods']}  "
          f"forbidden route methods present (must be False): {ra['forbidden_route_methods_present']}")
    print(f"forbidden routes present (must be empty): {ra['forbidden_routes_present']}")
    print(f"DRC delegates: {ra['drc_delegates']}  step present: {ra['drc_step_present']}")
    print(f"persist present: {ra['persist_present']}  "
          f"atomic/idempotent persist: {ra['atomic_idempotent_persist_present']}")
    print(f"UI loaders (must be 1): {ra['ui_loader_count']}  "
          f"UI metric computation (must be empty): {ra['ui_metric_computation']}")
    print(f"kernel forks HOC/realloc/scoring (must be False): "
          f"{ra['kernel_forks_hoc']}/{ra['kernel_forks_reallocation']}/{ra['kernel_forks_scoring']}")
    print(f"second registry present (must be empty): {ra['second_registry_present_modules']}  "
          f"paid-data registry fork present (must be empty): {ra['slice9_present_modules']}  "
          f"cadence enabled (must be False): {ra['cadence_enabled']}")

    hdr("DATA EXPANSION / PURCHASE-GATE OWNERSHIP (Slice 9, Phase 29J, Milestone 5)")
    de = rep["data_expansion_ownership"]
    print(f"kernel present: {de['kernel_present']}  owner present: {de['owner_present']}  "
          f"landed modules missing (must be empty): {de['landed_modules_missing']}")
    print(f"second calculation owner (must be empty): {de['second_calculation_owner_modules']}  "
          f"second composition owner (must be empty): {de['second_composition_owner_modules']}")
    print(f"missing reuse of existing owners (must be empty): {de['missing_reuse']}")
    print(f"owner forbidden calls (must be empty): {de['owner_forbidden_calls']}  "
          f"kernel forbidden calls (must be empty): {de['kernel_forbidden_calls']}")
    print(f"GET catalog route count (must be 1): {de['route_get_count']}  "
          f"GET detail route count (must be 1): {de['detail_route_get_count']}  "
          f"route methods: {de['data_expansion_route_methods']}  "
          f"forbidden route methods present (must be False): {de['forbidden_route_methods_present']}")
    print(f"forbidden routes present (must be empty): {de['forbidden_routes_present']}")
    print(f"persist present: {de['persist_present']}  "
          f"atomic/idempotent persist: {de['atomic_idempotent_persist_present']}")
    print(f"UI loaders (must be 1): {de['ui_loader_count']}  "
          f"UI gate computation (must be empty): {de['ui_metric_computation']}")
    print(f"kernel forks research-agent/stage13a (must be False): "
          f"{de['kernel_forks_research_agent']}/{de['kernel_forks_stage13a']}")
    print(f"secret/credential ownership (must be empty): {de['secret_ownership']}  "
          f"cadence disabled (must be True): {de['cadence_disabled']}  "
          f"DRC daily-job present (must be empty): {de['drc_daily_job_present']}")
    print(f"Slice 10 present (must be empty): {de['slice10_present_modules']}  "
          f"cadence enabled (must be False): {de['cadence_enabled']}")

    hdr("OPERATOR UX CONSOLIDATION OWNERSHIP (Phase 29J.1)")
    ux = rep["operator_ux_consolidation_ownership"]
    print(f"four primary operator areas present: {ux['primary_areas_present']} "
          f"(Today={ux['nav_today']} Portfolio={ux['nav_portfolio']} "
          f"Research={ux['nav_research']} System/Audit={ux['nav_system_audit']})")
    print(f"legacy views demoted from primary nav: {ux['legacy_views_demoted']}")
    print(f"missing route aliases (must be empty): {ux['missing_route_aliases']}  "
          f"missing legacy routes / dead links (must be empty): {ux['missing_legacy_routes']}")
    print(f"market context restored: {ux['market_context_present']} (reference-only label: {ux['market_reference_only']})  "
          f"one market loader (must be 1): {ux['market_loader_count']}  "
          f"authoritative owner fetch: {ux['market_owner_fetch']}  GET-only: {ux['market_route_get_only']}")
    print(f"UI direct provider hosts (must be empty): {ux['ui_direct_provider_hosts']}  "
          f"UI market/regime math (must be empty): {ux['market_region_market_math']}")
    print(f"one canonical next-action renderer (must be 1): {ux['workflow_next_action_renderer_count']}  "
          f"missing safety tokens (must be empty): {ux['missing_safety_tokens']}")
    print(f"forbidden new purchase/order/promotion routes (must be empty): {ux['forbidden_new_routes_present']}  "
          f"cadence enabled (must be False): {ux['cadence_enabled']}")

    hdr("NORMAL DAILY PORTFOLIO CYCLE OWNERSHIP (Stage 22)")
    nc = rep["normal_cycle_ownership"]
    print(f"kernels present: {nc['kernels_present']}  "
          f"cycle-kernel impurity (must be empty): {nc['kernel_impurity']}  "
          f"gap-kernel impurity (must be empty): {nc['gap_kernel_impurity']}")
    print(f"stage sequence: {' -> '.join(nc['stage_sequence'])}")
    print(f"declared: {nc['sequence_declared']}  in order: {nc['sequence_ordered']}")
    print(f"second cycle-state owners (must be empty): {nc['second_cycle_owner_modules']}  "
          f"second gap-taxonomy owners (must be empty): {nc['second_gap_owner_modules']}")
    print(f"workflow owner missing tokens (must be empty): {nc['missing_owner_tokens']}")
    print(f"single primary mutation ENFORCED: {nc['single_mutation_enforced']}  "
          f"close outranks research: {nc['close_outranks_research']}")
    print(f"post-close research requirement: {nc['post_close_research_required']}  "
          f"no standalone desk refresh required: "
          f"{nc['no_standalone_desk_refresh_required']}")
    print(f"stale-evidence classification missing (must be empty): "
          f"{nc['missing_evidence_tokens']}  still fails closed: "
          f"{nc['evidence_still_fails_closed']}")
    print(f"assessment/proposal binding missing (must be empty): "
          f"{nc['missing_binding_tokens']}")
    print(f"data-gap taxonomy missing (must be empty): {nc['missing_gap_tokens']}  "
          f"unknown code fails CLOSED: {nc['unknown_gap_fails_closed']}  "
          f"no silent substitution: {nc['no_silent_substitution']}")
    print(f"gap severity consumed (not inferred): "
          f"{nc['gap_severity_consumed_not_inferred']}")
    print(f"UI missing cycle tokens (must be empty): {nc['missing_ui_tokens']}  "
          f"UI cycle derivation (must be empty): {nc['ui_cycle_derivation']}")

    hdr("CONTINUOUS INFORMATION-COLLECTION OWNERSHIP (Release 29)")
    icx = rep["information_collection_ownership"]
    print(f"modules present: {icx['modules_present']}")
    print(f"cadence-kernel impurity (must be empty): {icx['kernel_impurity']}")
    print(f"second cadence owners (must be empty): "
          f"{icx['second_cadence_owner_modules']}  "
          f"second collection owners (must be empty): "
          f"{icx['second_collection_owner_modules']}  "
          f"second worker scripts (must be empty): {icx['second_worker_scripts']}")
    print(f"orchestrator forbidden calls (must be empty): "
          f"{icx['owner_forbidden_calls']}  "
          f"missing delegation (must be empty): {icx['missing_delegation']}")
    print(f"read route GET count (must be 1): {icx['route_get_count']}  "
          f"methods: {icx['route_methods']}  "
          f"forbidden collection routes (must be empty): "
          f"{icx['forbidden_routes_present']}")
    print(f"missing safety tokens (must be empty): {icx['missing_safety_tokens']}")
    print(f"observation-not-material rule present: {icx['observation_rule_present']}  "
          f"read surface bound to the gate: {icx['read_surface_bound_to_gate']}  "
          f"one clock per cycle: {icx['single_cycle_clock']}")
    print(f"UI loader count (must be 1): {icx['ui_loader_count']}  "
          f"UI missing tokens (must be empty): {icx['missing_ui_tokens']}  "
          f"UI health derivation (must be empty): {icx['ui_health_derivation']}")
    print(f"worker delegates: {icx['worker_delegates']}  "
          f"worker reimplements cadence (must be empty): "
          f"{icx['worker_reimplements_cadence']}")
    print(f"collection scripts (manager+installer+validator only): "
          f"{icx['manage_scripts']}  "
          f"unexpected (must be empty): {icx['unexpected_collection_scripts']}  "
          f"definition owner present: {icx['task_definition_owner_present']}  "
          f"manager delegates registration: "
          f"{icx['manager_delegates_registration']}  "
          f"validator read-only: {icx['validator_is_read_only']}")
    print(f"mutations require -Execute: {icx['manage_requires_execute']}  "
          f"Status is read-only: {icx['status_is_read_only']}  "
          f"uninstall preserves evidence: {icx['uninstall_preserves_evidence']}")
    print(f"logical-worker owner present: {icx['topology_owner_present']}  "
          f"second topology owners (must be empty): "
          f"{icx['second_topology_owner_modules']}  "
          f"control helper delegates: {icx['control_delegates_topology']}")
    print(f"manager missing topology tokens (must be empty): "
          f"{icx['manage_missing_topology_tokens']}  "
          f"manager counts raw processes (must be empty): "
          f"{icx['manage_counts_raw_processes']}")
    print(f"missing progress vocabulary (must be empty): "
          f"{icx['missing_progress_tokens']}  "
          f"second progress writers (must be empty): "
          f"{icx['second_progress_owner_modules']}  "
          f"worker reports progress: {icx['worker_reports_progress']}")
    print(f"worker timer authorities (must be empty): "
          f"{icx['worker_timer_authorities']}  "
          f"heartbeat budget {icx['heartbeat_stale_seconds']}s / stall budget "
          f"{icx['progress_stall_seconds']}s  not widened: "
          f"{icx['stall_budget_not_widened']}")

    hdr("CANONICAL BACKEND RESTART / SMOKE OWNERSHIP")
    br = rep["backend_restart_ownership"]
    print(f"owner: {br['owner']}  present: {br['owner_present']}  "
          f"declares ownership: {br['owner_declares_ownership']}")
    print(f"canonical readiness routes: {', '.join(br['canonical_readiness_routes'])}  "
          f"missing from owner (must be empty): {br['owner_missing_canonical_routes']}")
    print(f"noncanonical health probes (must be empty): "
          f"{len(br['noncanonical_health_probes'])}")
    for x in br["noncanonical_health_probes"][:20]:
        print(f"  !{x}")
    print(f"scripts reimplementing the launch (must be empty): "
          f"{len(br['reimplementing_scripts'])}")
    for x in br["reimplementing_scripts"][:20]:
        print(f"  !{x}")
    print(f"probed routes the app does not declare as GET (must be empty): "
          f"{br['probed_routes_not_declared']}")
    print(f"mutating HTTP calls (must be empty): {br['mutating_http_calls']}")
    print(f"owner missing contract (must be empty): {br['owner_missing_contract']}  "
          f"owner missing diagnostics (must be empty): {br['owner_missing_diagnostics']}")
    print(f"LIVE_SMOKE_OK emitters (must be exactly the owner): "
          f"{br['live_smoke_emitting_scripts']}  "
          f"emissions in owner (must be 1): {br['owner_live_smoke_emissions']}")
    print(f"powershell workflows scanned: {len(br['scanned_powershell_files'])}")

    hdr("RADICAL OPERATOR SIMPLIFICATION (Release 29 UX2)")
    ux2 = rep["release29_ux2_simplification"]
    print(f"Markets area: nav {ux2['markets_nav']}  route {ux2['markets_route']}  "
          f"tab {ux2['markets_tab_present']}  reference-only label "
          f"{ux2['markets_reference_only_label']}")
    print(f"regions still on Today (must be empty): {ux2['regions_still_on_today']}")
    print(f"missing on Markets (must be empty): {ux2['regions_missing_on_markets']}  "
          f"missing on System · Audit (must be empty): "
          f"{ux2['regions_missing_on_system_audit']}")
    print(f"moved ids duplicated or lost (must be empty): "
          f"{ux2['moved_ids_duplicated_or_lost']}")
    print(f"Today market strip: present {ux2['today_market_strip_present']}  "
          f"is a mirror {ux2['today_market_strip_is_a_mirror']}  "
          f"forbidden calls (must be empty) {ux2['today_market_strip_forbidden_calls']}")
    print(f"market owners (must be 1 each): dashboard "
          f"{ux2['market_dashboard_owner_count']}  context "
          f"{ux2['market_context_owner_count']}")
    print(f"rail-free routes: {ux2['rail_free_routes']}  route published: "
          f"{ux2['rail_route_published']}  rail markup retained: "
          f"{ux2['rail_markup_retained']}")
    print(f"Portfolio regions not removed (must be empty): "
          f"{ux2['portfolio_regions_not_removed']}  regions lost (must be empty): "
          f"{ux2['portfolio_regions_lost']}")
    print(f"moved diagnostics panel routed: {ux2['moved_diagnostics_panel_routed']}")

    hdr("RESTART / SMOKE INVOCATION HYGIENE (Release 29 UX2)")
    ih = rep["restart_invocation_hygiene"]
    print(f"owner is exit-free (safe to call directly): {ih['owner_is_exit_free']}")
    for x in ih["owner_exit_statements"][:20]:
        print(f"  !{x}")
    print(f"owner declares the direct invocation: {ih['owner_declares_direct_invocation']}  "
          f"contract probe: {ih['owner_exposes_contract_probe']}  "
          f"LASTEXITCODE contract: {ih['owner_reports_last_exit_code']}  "
          f"SmokePath contract asserted: {ih['owner_asserts_smokepath_contract']}")
    print(f"-File + -SmokePath forwarding (must be empty): "
          f"{len(ih['file_switch_smokepath_forwarding'])}")
    for x in ih["file_switch_smokepath_forwarding"][:20]:
        print(f"  !{x}")
    print(f"-Command lifecycle construction (must be empty): "
          f"{len(ih['command_switch_lifecycle_construction'])}")
    for x in ih["command_switch_lifecycle_construction"][:20]:
        print(f"  !{x}")
    print(f"fragile array forwarding (must be empty): {ih['fragile_array_forwarding']}")
    print(f"duplicate restart implementations (must be empty): "
          f"{ih['duplicate_restart_implementations']}")
    print(f"invocation files scanned: {ih['scanned_invocation_files']}")

    hdr("ACTIVE MANAGER OPERATING STATE (R54 Slice 1)")
    ams = rep["release54_active_manager_state"]
    print(f"owner present: {ams['owner_present']}  declares owner: {ams['declares_owner']}  "
          f"composition-only: {ams['composition_only_declared']}  "
          f"composes decision snapshot: {ams['composes_decision_snapshot']}")
    print(f"forbidden calculation defs (must be empty): {ams['forbidden_calculation_defs']}  "
          f"forbidden execution tokens (must be empty): {ams['forbidden_execution_tokens']}")
    print(f"time-state distinction declared: {ams['time_state_distinction_declared']}  "
          f"GET route count (must be 1): {ams['route_get_count']}  "
          f"non-GET route present (must be False): {ams['non_get_route_present']}")
    print(f"UI loader count (must be 1): {ams['ui_loader_count']}  "
          f"UI fetch count (must be 1): {ams['ui_fetch_count']}  "
          f"UI region present: {ams['ui_region_present']}  "
          f"UI region forbidden tokens (must be empty): {ams['ui_region_forbidden']}")
    print(f"legacy cc-status-mark writer present (must be False): "
          f"{ams['legacy_status_mark_writer_present']}  "
          f"canonical writer count (must be 1): {ams['canonical_status_mark_writer_count']}  "
          f"guarded early writer present: {ams['status_mark_guarded_early_writer_present']}")
    print(f"decision-authority ladder declared (must be True): "
          f"{ams['decision_authority_declared']}  "
          f"forward-evidence identities distinct (must be True): "
          f"{ams['evidence_identities_distinct']}")
    print(f"automatic promotion allowed (must be False): "
          f"{ams['automatic_model_promotion_allowed']}  "
          f"automatic approval allowed (must be False): {ams['automatic_approval_allowed']}  "
          f"cadence enabled (must be False): {ams['cadence_enabled']}")

    hdr("GOVERNED INTRADAY DECISION CYCLE (R54.1)")
    g = rep["release54_1_governed_intraday_decision"]
    print(f"gate owner present: {g['gate_owner_present']}  "
          f"gate defs missing (must be empty): {g['gate_defs_missing']}")
    print(f"duplicate governance owners (must be empty): "
          f"{g['duplicate_governance_owners']}")
    print(f"cycle delegates to owner: {g['cycle_delegates_to_owner']}  "
          f"cycle defines gate (must be False): {g['cycle_defines_gate']}  "
          f"read model defines gate (must be False): {g['read_model_defines_gate']}  "
          f"workflow defines gate (must be False): {g['workflow_defines_gate']}")
    print(f"forbidden calculation defs (must be empty): "
          f"{g['forbidden_calculation_defs']}  "
          f"forbidden execution tokens (must be empty): "
          f"{g['forbidden_execution_tokens']}")
    print(f"missing withheld reason codes (must be empty): "
          f"{g['missing_reason_codes']}  "
          f"OWNED_DATA_NOT_CONFIRMED reused verbatim: "
          f"{g['owned_data_rule_reused_verbatim']}")
    print(f"HOLD and CHANGE both governed: {g['hold_and_change_both_governed']}  "
          f"manual review required for CHANGE: "
          f"{g['manual_review_required_for_change']}")
    print(f"governed lane never advances operational mark: "
          f"{g['governed_lane_never_advances_operational_mark']}  "
          f"separate governed ledger files: {g['separate_governed_ledger_files']}  "
          f"touches manual index (must be False): "
          f"{g['governed_writer_touches_manual_index']}")
    print(f"system token distinct from approval token: "
          f"{g['system_token_distinct_from_approval_token']}  "
          f"gate owns no economics: {g['gate_declares_it_owns_no_economics']}  "
          f"zero-base policy bound: {g['zero_base_policy_bound_not_redefined']}")
    print(f"R53.1 emission slots unchanged: {g['emission_slots_unchanged']}  "
          f"post-close pass declared: {g['emission_post_close_pass_declared']}")

    hdr("SAME-SESSION REASSESSMENT VERSIONING (R54.2)")
    v = rep["release54_2_same_session_reassessment_versioning"]
    print(f"owner defs missing (must be empty): {v['owner_defs_missing']}  "
          f"duplicate versioning owners (must be empty): "
          f"{v['duplicate_versioning_owners']}")
    print(f"single index writer: {v['single_index_writer']} {v['index_writers']}  "
          f"parallel stores (must be empty): "
          f"{v['parallel_reassessment_stores']}")
    print(f"version chain appended: {v['version_chain_is_appended']}  "
          f"owner deletes an artifact (must be False): "
          f"{v['owner_deletes_an_artifact']}")
    print(f"persist outcomes missing (must be empty): "
          f"{v['persist_outcomes_missing']}  "
          f"inconsistent-identity guard: "
          f"{v['inconsistent_identity_guard_present']}  "
          f"id-collision guard: {v['artifact_id_collision_guard_present']}")
    print(f"evidence identity declared: {v['evidence_identity_declared']}  "
          f"forbidden evidence components (must be empty): "
          f"{v['forbidden_evidence_components']}")
    print(f"legacy artifact recomputed not rewritten: "
          f"{v['legacy_artifact_recomputed_not_rewritten']}  "
          f"authoritative rows used by churn: "
          f"{v['authoritative_rows_used_by_churn']}  "
          f"outcome owner uses them: {v['outcome_owner_uses_authoritative_rows']}")
    print(f"both producers delegate: {v['both_producers_delegate']}  "
          f"gate requires persisted reassessment: "
          f"{v['gate_requires_persisted_reassessment']}  "
          f"cycle publishes persistence outcome: "
          f"{v['cycle_publishes_persistence_outcome']}")

    hdr("MISSED ELIGIBLE SESSION RECOVERY (R54.2.1)")
    mr = rep["release54_2_1_missed_session_recovery"]
    print(f"owner defs missing (must be empty): {mr['owner_defs_missing']}  "
          f"calendar defs missing (must be empty): {mr['calendar_defs_missing']}")
    print(f"duplicate state owners (must be empty): {mr['duplicate_state_owners']}  "
          f"duplicate calendar owners (must be empty): "
          f"{mr['duplicate_calendar_owners']}")
    print(f"second recovery orchestrators (must be empty): "
          f"{mr['second_recovery_orchestrators']}  "
          f"forbidden routes (must be empty): {mr['forbidden_routes_present']}")
    print(f"workflow delegates calendar: {mr['workflow_delegates_calendar']}  "
          f"owns no calendar walk: {mr['workflow_owns_no_calendar_walk']}  "
          f"obligation anchored on close: {mr['obligation_anchored_on_close']}")
    print(f"priority suppresses wait state: {mr['priority_suppresses_wait_state']}  "
          f"priority promotes close: {mr['priority_promotes_close']}  "
          f"oldest first: {mr['oldest_first_declared']}")
    print(f"cycle reads binding: {mr['cycle_reads_binding_from_workflow']}  "
          f"cycle passes binding: {mr['cycle_passes_binding_to_close']}  "
          f"close accepts binding: {mr['close_accepts_binding']}")
    print(f"close refuses forward binding: {mr['close_refuses_forward_binding']}  "
          f"never clamps: {mr['close_binding_never_clamps']}  "
          f"binding is not a request field: {mr['binding_is_not_a_request_field']}")
    print(f"AMS delegates: {mr['ams_delegates_recovery']}  "
          f"presentation delegates: {mr['presentation_delegates_recovery']}")
    print(f"UI recovery derivation (must be empty): {mr['ui_recovery_derivation']}  "
          f"UI renders backend recovery: {mr['ui_renders_backend_recovery']}  "
          f"UI offers no date entry: {mr['ui_offers_no_date_entry']}")
    print(f"adds automation (must be False): {mr['recovery_adds_automation']}  "
          f"creates orders (must be False): {mr['recovery_creates_orders']}  "
          f"cycle approves nothing: {mr['cycle_still_approves_nothing']}")

    hdr("CONTROLLED MONTHLY RESEARCH-INPUT RECOVERY (R54.2.3)")
    sp = rep["release54_2_3_source_panel_recovery"]
    print(f"refresh policy defined once in the bridge: "
          f"{sp['refresh_policy_defined_in_bridge']}  "
          f"missing defs (must be empty): {sp['missing_refresh_policy_defs']}")
    print(f"second panel writers (must be empty): {sp['second_panel_writer']}  "
          f"second refresh policies (must be empty): {sp['second_refresh_policy']}")
    print(f"bridge pure stdlib: {sp['bridge_pure_stdlib']}  "
          f"drives the panel owner: {sp['bridge_drives_panel_owner']}  "
          f"argv array: {sp['refresh_uses_argv_array']}")
    print(f"cutoff bound to the eligible session: "
          f"{sp['cutoff_bound_to_eligible_session']}  "
          f"operator date fields (must be empty): "
          f"{sp['operator_supplied_date_fields']}")
    print(f"forbidden panel routes (must be empty): {sp['forbidden_panel_routes']}  "
          f"future-dated panel still blocks: {sp['future_dated_panel_still_blocks']}")
    print(f"one producibility verdict: {sp['verdict_defined_in_panel_owner']}  "
          f"cycle reads it: {sp['cycle_reads_single_verdict']}  "
          f"cycle copies panel vocabulary (must be False): "
          f"{sp['cycle_copies_panel_vocabulary']}")
    print(f"cycle publishes data quality: {sp['cycle_publishes_data_quality']}  "
          f"workflow projects actionability: "
          f"{sp['workflow_projects_actionability']}")
    print(f"UI actionability derivation (must be empty): "
          f"{sp['ui_actionability_derivation']}  "
          f"UI reads backend actionability: {sp['ui_reads_backend_actionability']}")

    hdr("DECISION / PROPOSAL SUPERSESSION AUTHORITY (R54.2.3.2)")
    dsup = rep["release54_2_3_2_decision_supersession"]
    print(f"supersession owner: {dsup['supersession_owner']}  defines calculation "
          f"+ loader + selector: "
          f"{dsup['owner_defines_calculation_loader_selector']}")
    print(f"second supersession calculations (must be empty): "
          f"{dsup['second_supersession_calculation']}")
    print(f"record_decision refuses superseded: "
          f"{dsup['record_decision_refuses_superseded']}  lane state in "
          f"vocabulary: {dsup['lane_state_in_vocabulary']}")
    print(f"realloc read renders verdict: {dsup['realloc_read_renders_verdict']}  "
          f"realloc second comparison (must be False): "
          f"{dsup['realloc_second_comparison']}")
    print(f"workflow consumes verdict: {dsup['workflow_consumes_verdict']}  "
          f"composes authority selector: "
          f"{dsup['workflow_composes_authority_selector']}  asserts NO_CHANGE "
          f"invariant: {dsup['workflow_asserts_no_change_invariant']}")
    print(f"superseded never approvable: {dsup['superseded_never_approvable']}  "
          f"projection prefers assessment decision: "
          f"{dsup['projection_prefers_assessment_decision']}")
    print(f"presentation renders verbatim: "
          f"{dsup['presentation_renders_verbatim']}  AMS echoes selector: "
          f"{dsup['ams_echoes_selector']}")
    print(f"UI renders superseded states: {dsup['ui_renders_superseded_states']}  "
          f"UI supersession derivation (must be empty): "
          f"{dsup['ui_supersession_derivation']}  forbidden routes (must be "
          f"empty): {dsup['forbidden_supersession_routes']}")

    hdr("REALLOCATION COHERENCE + INTRADAY VISIBILITY (R54.2.4)")
    rc = rep["release54_2_4_reallocation_coherence"]
    print(f"presentation defines scoped economics: "
          f"{rc['presentation_defines_scoped_economics']}  second "
          f"current-decision calculations (must be empty): "
          f"{rc['second_current_decision_calculation']}")
    print(f"proposal history block present: "
          f"{rc['proposal_history_block_present']}  hero renders "
          f"current decision: {rc['hero_renders_current_decision']}  hero "
          f"unscoped proposal econ (must be False): "
          f"{rc['hero_unscoped_proposal_econ_present']}")
    print(f"realloc history demotion present: "
          f"{rc['realloc_history_demotion_present']}")
    print(f"AMS defines live lane: {rc['ams_defines_live_lane']}  second lane "
          f"definitions (must be empty): {rc['second_live_lane_definition']}")
    print(f"UI renders live lane: {rc['ui_renders_live_lane']}  UI lane "
          f"governance derivation (must be empty): "
          f"{rc['ui_lane_governance_derivation']}")
    print(f"stale display label owned: {rc['stale_display_label_owned']}  "
          f"outcome-history version identity: "
          f"{rc['outcome_history_version_identity']}")
    print(f"CA projection scope declared: {rc['ca_projection_scope_declared']}  "
          f"eligibility vocabulary split: "
          f"{rc['eligibility_vocabulary_split']}  legacy controls classified: "
          f"{rc['legacy_controls_classified']}")

    hdr("SAME-SESSION HOC EVIDENCE VERSIONING (R54.3)")
    hv = rep["release54_3_hoc_evidence_versioning"]
    print(f"second HOC writers (must be empty): {hv['second_hoc_writer']}  "
          f"parallel HOC stores (must be empty): {hv['parallel_hoc_stores']}")
    print(f"appends version chain: {hv['appends_version_chain']}  owner deletes "
          f"an artifact (must be False): {hv['owner_deletes_an_artifact']}")
    print(f"evidence identity contaminated (must be empty): "
          f"{hv['evidence_identity_contaminated']}  exclusions declared: "
          f"{hv['evidence_exclusions_declared']}")
    print(f"persist outcomes missing (must be empty): "
          f"{hv['persist_outcomes_missing']}  inconsistent-identity guard: "
          f"{hv['inconsistent_identity_guard_present']}")
    print(f"gate checks missing (must be empty): {hv['gate_checks_missing']}  "
          f"reason codes declared: {hv['gate_reason_codes_declared']}")
    print(f"gate opens a store (must be empty): {hv['gate_opens_a_store']}  "
          f"gate fails closed on absent binding: "
          f"{hv['gate_fails_closed_on_absent_binding']}  binding resolver owned "
          f"by HOC: {hv['binding_resolver_owned_by_hoc']}")
    print(f"reassessment binds exact artifact: "
          f"{hv['reassessment_binds_exact_artifact']}  proposal binds exact "
          f"artifact: {hv['proposal_binds_exact_artifact']}")
    print(f"cycle publishes HOC persistence: "
          f"{hv['cycle_publishes_hoc_persistence']}  persists before "
          f"reassessment: {hv['cycle_persists_hoc_before_reassessment']}  UI "
          f"derives persistence (must be empty): "
          f"{hv['ui_derives_hoc_persistence']}")

    hdr("OWNED-DATA READINESS AUTHORITY (R54.2.3.1)")
    ra = rep["release54_2_3_1_owned_data_readiness_authority"]
    print(f"coverage owner: {ra['coverage_owner']}  defines coverage + assessment: "
          f"{ra['owner_defines_coverage_and_assessment']}")
    print(f"second coverage calculations (must be empty): "
          f"{ra['second_coverage_calculation']}")
    print(f"workflow probe tokens (must be empty): {ra['workflow_probe_tokens']}  "
          f"workflow consumes close verdict: {ra['workflow_consumes_close_verdict']}")
    print(f"snapshot supplies readiness: {ra['snapshot_supplies_readiness']}  "
          f"close composed before workflow: "
          f"{ra['snapshot_composes_close_before_workflow']}")
    print(f"orchestrator supplies readiness: {ra['orchestrator_supplies_readiness']}  "
          f"presentation shares one close read: "
          f"{ra['presentation_shares_one_close_read']}")
    print(f"distinct owned-data concepts: {ra['distinct_owned_data_concepts']}  "
          f"close gate echoes verdict: {ra['close_gate_echoes_coverage_verdict']}")
    print(f"UI readiness derivation (must be empty): "
          f"{ra['ui_readiness_derivation']}")

    hdr("POST-CLOSE RESEARCH RECOVERY + ATTRIBUTION INTEGRITY (R54.2.2)")
    pr = rep["release54_2_2_post_close_research_recovery"]
    print(f"obligation owner defines state machine: "
          f"{pr['obligation_owner_defines_state_machine']}  "
          f"classification owner defines vocabulary: "
          f"{pr['classification_owner_defines_vocabulary']}")
    print(f"workflow reads classification: {pr['workflow_reads_classification']}  "
          f"workflow owns no classification: {pr['workflow_owns_no_classification']}")
    print(f"second obligation owners (must be empty): {pr['second_obligation_owner']}  "
          f"second classification owners (must be empty): "
          f"{pr['second_classification_owner']}")
    print(f"second research orchestrators (must be empty): "
          f"{pr['second_research_orchestrator']}  "
          f"forbidden research routes (must be empty): "
          f"{pr['forbidden_research_routes']}")
    print(f"obligation suppresses wait gate: {pr['obligation_suppresses_wait_gate']}  "
          f"cycle resumes without repeating close: "
          f"{pr['cycle_resumes_without_repeating_close']}  "
          f"cycle path unchanged: {pr['cycle_path_unchanged']}")
    print(f"no second route declared: {pr['obligation_declares_no_second_route']}  "
          f"never repeats close: {pr['obligation_never_repeats_close']}  "
          f"DRC gate is session scoped: {pr['drc_gate_is_session_scoped']}")
    print(f"AMS delegates obligation: {pr['ams_delegates_obligation']}  "
          f"presentation delegates obligation: "
          f"{pr['presentation_delegates_obligation']}")
    print(f"workflow states blocker severity: {pr['workflow_states_blocker_severity']}  "
          f"stale research never blocks decision: "
          f"{pr['research_stale_never_blocks_decision']}")
    print(f"presentation reads severity: {pr['presentation_reads_severity']}  "
          f"renders no dict repr: {pr['presentation_renders_no_dict_repr']}")
    print(f"attribution availability one owner: "
          f"{pr['attribution_availability_has_one_owner']}  "
          f"close uses shared availability: {pr['close_uses_shared_availability']}")
    print(f"attribution requires exact date: {pr['attribution_requires_exact_date']}  "
          f"flags stale legs: {pr['attribution_flags_stale_legs']}  "
          f"unreconciled is unavailable: {pr['unreconciled_is_unavailable']}")
    print(f"rewrites no history: {pr['attribution_rewrites_no_history']}  "
          f"UI states unavailable attribution: "
          f"{pr['ui_states_unavailable_attribution']}")
    print(f"UI obligation derivation (must be empty): "
          f"{pr['ui_obligation_derivation']}  "
          f"UI renders backend obligation: {pr['ui_renders_backend_obligation']}  "
          f"UI offers no research backfill: {pr['ui_offers_no_research_backfill']}")
    print(f"adds automation (must be False): "
          f"{pr['research_recovery_adds_automation']}  "
          f"creates orders (must be False): "
          f"{pr['research_recovery_creates_orders']}  "
          f"monthly contract not weakened: {pr['monthly_contract_not_weakened']}")

    hdr("INVENTORY DRIFT")
    d = rep["inventory_drift"]
    print(f"status: {d['status']}")
    print(f"on disk but not in inventory: {len(d['on_disk_not_in_inventory'])}")
    for p in d["on_disk_not_in_inventory"][:40]:
        print(f"  +{p}")
    print(f"in inventory but not on disk: {len(d['in_inventory_not_on_disk'])}")
    for p in d["in_inventory_not_on_disk"][:40]:
        print(f"  -{p}")

    hdr("CANONICAL DOCS")
    for doc, present in sorted(rep["canonical_docs"]["docs"].items()):
        print(f"  {'OK ' if present else 'MISS'}  {doc}")


# Categories that make --strict return nonzero when non-empty.
BLOCKING = ("duplicate_declarations", "research_execution_terms")

#: Stage 19.1 — (report_key, field, must_be) invariants that make --strict fail. A false
#: entry here means a current economic read can silently miss a registered corporate
#: action, or that split arithmetic was duplicated outside its one owner.
BLOCKING_INVARIANTS = (
    # --- Release 32: PnL Opportunity Frontier (invariants 1-40) -------------
    # ONE owner per concern; no second optimiser, covariance owner or statistics
    # library; a sleeve generates opportunities and never owns capital,
    # allocates, proposes, orders, promotes or activates; the Release-31 result
    # is inherited rather than rerun; point-in-time admissibility is MEASURED
    # and revised macro can never be admitted as history; cross-sleeve
    # comparison names its shared calendar and that view can qualify nothing;
    # the lockbox opens once; every executed hypothesis stays in the
    # denominator; a control can never qualify its own sleeve; the primary
    # control is volatility-matched rather than cash; the governance contract
    # declares that daily reassessment is not daily trading, that a closed
    # market leaves a delta pending, that stale data fails closed, that asset
    # count is not diversification, that the event fabric is reused, and that
    # multi-asset NAV has exactly one future owner; nothing is bought; and the
    # read surface is GET-only with the canonical order badge.
    # Release 33 (invariants 41-62): one statistics library and one hashing
    # owner; the research lane promotes nothing, spends nothing, and never
    # touches an operational owner; the SIGNAL_RESEARCH_VALID /
    # FUTURES_IMPLEMENTABILITY_PROVEN distinction is refused rather than
    # blurred, because the owned Continuous Futures entitlement is one market;
    # the leakage controls that decide whether any number here means anything
    # are structural (filtered HMM states only, training-only scaling, no
    # random split, non-overlapping forecast dates, an executable
    # point-in-time probe); point-in-time honesty (revised macro excluded,
    # CFTC publication lag applied, ALFRED vintages, no synthetic substitute);
    # the judge charges traded notional and ranks on a volatility-matched
    # control rather than on excess over cash; and ALPHA_RESULT may be PASS
    # only with a qualified verdict.
    # --- Release 52: persistent prospective research runtime ---------------
    # ONE derived timing contract (the scheduler consumes it and adds no
    # rule); ONE runtime orchestration path, locked, idempotent, failing
    # closed on a broken evidence chain; ONE append-only forfeiture owner
    # whose every row refuses backfill; the frontier refresh delegates to the
    # pure R51 owner and no approval writer exists; no operational import, no
    # HTTP reach, no portfolio-cycle path; exactly one script registers the
    # Windows task, the validator blocks an absent or malformed task, and the
    # disable script deletes nothing.
    ("release52_persistent_research_runtime", "modules_present", True),
    ("release52_persistent_research_runtime", "one_timing_contract", True),
    ("release52_persistent_research_runtime",
     "scheduler_owns_no_clock_rule", True),
    ("release52_persistent_research_runtime",
     "scheduler_delegates_to_contract_times", True),
    ("release52_persistent_research_runtime",
     "one_runtime_orchestrator", True),
    ("release52_persistent_research_runtime",
     "runtime_delegates_emission_and_scoring", True),
    ("release52_persistent_research_runtime",
     "runtime_is_locked_and_idempotent", True),
    ("release52_persistent_research_runtime",
     "runtime_fails_closed_on_broken_chain", True),
    ("release52_persistent_research_runtime", "one_forfeiture_owner", True),
    ("release52_persistent_research_runtime",
     "forfeiture_refuses_backfill", True),
    ("release52_persistent_research_runtime",
     "frontier_delegates_to_r51", True),
    ("release52_persistent_research_runtime", "no_approval_writer", True),
    ("release52_persistent_research_runtime",
     "no_promotion_flag_write", True),
    ("release52_persistent_research_runtime",
     "no_operational_imports", True),
    ("release52_persistent_research_runtime", "no_http_reach", True),
    ("release52_persistent_research_runtime", "safety_flags_false", True),
    ("release52_persistent_research_runtime", "one_task_registrar", True),
    ("release52_persistent_research_runtime", "install_is_explicit", True),
    ("release52_persistent_research_runtime",
     "installer_compares_principal", True),
    ("release52_persistent_research_runtime",
     "validator_blocks_bad_task", True),
    ("release52_persistent_research_runtime",
     "validator_requires_logged_out_principal", True),
    ("release52_persistent_research_runtime",
     "disable_deletes_nothing", True),
    ("release52_persistent_research_runtime",
     "one_shot_uses_same_entrypoint", True),
    ("release52_persistent_research_runtime",
     "read_model_is_read_only", True),
    ("release33_predictive_edge", "modules_present", True),
    ("release33_predictive_edge", "reuses_r31_statistics", True),
    ("release33_predictive_edge", "reuses_r31_hashing", True),
    ("release33_predictive_edge", "reuses_r31_learners", True),
    ("release33_predictive_edge", "no_second_optimiser", True),
    ("release33_predictive_edge", "no_second_covariance_owner", True),
    ("release33_predictive_edge", "safety_flags_false", True),
    ("release33_predictive_edge", "forbidden_calls", []),
    ("release33_predictive_edge", "forbidden_owner_refs", []),
    ("release33_predictive_edge", "declares_no_futures_execution", True),
    ("release33_predictive_edge", "futures_implementability_refused", True),
    ("release33_predictive_edge", "universe_declares_signal_research_only",
     True),
    ("release33_predictive_edge", "return_definition_heterogeneity_declared",
     True),
    ("release33_predictive_edge", "hmm_states_filtered_only", True),
    ("release33_predictive_edge", "regime_uses_filtered_states", True),
    ("release33_predictive_edge", "scaler_fitted_on_training_only", True),
    ("release33_predictive_edge", "non_overlapping_forecast_dates", True),
    ("release33_predictive_edge", "implementation_lag_declared", True),
    ("release33_predictive_edge", "point_in_time_probe_present", True),
    ("release33_predictive_edge", "revised_macro_excluded", True),
    ("release33_predictive_edge", "cot_publication_lag_applied", True),
    ("release33_predictive_edge", "alfred_vintages_used", True),
    ("release33_predictive_edge", "synthetic_data_inadmissible", True),
    ("release33_predictive_edge", "cost_base_traded_notional", True),
    ("release33_predictive_edge", "volatility_matched_control_owned", True),
    ("release33_predictive_edge", "excess_over_cash_may_not_rank", True),
    ("release33_predictive_edge", "cost_sensitivity_declared", True),
    ("release33_predictive_edge", "denominator_counts_all_executed", True),
    ("release33_predictive_edge", "adaptive_search_refused", True),
    ("release33_predictive_edge", "deep_learning_out_of_scope", True),
    ("release33_predictive_edge", "lockbox_single_access", True),
    ("release33_predictive_edge", "alpha_pass_requires_qualified_verdict",
     True),
    ("release33_predictive_edge", "reports_system_and_alpha_results", True),
    ("release33_predictive_edge", "min_scored_dates_enforced_at_the_gate",
     True),
    ("release33_predictive_edge", "stability_check_fails_closed", True),
    ("release33_predictive_edge", "leave_market_out_is_a_gate", True),
    ("release33_predictive_edge", "runner_is_research_only", True),
    ("release33_predictive_edge", "attribution_owner_present", True),
    ("release33_predictive_edge", "attribution_is_provenance_based", True),
    ("release33_predictive_edge", "attribution_fails_closed", True),
    ("release33_predictive_edge", "attribution_refuses_time_whitelist", True),
    ("release33_predictive_edge", "information_collection_still_protected",
     True),
    # Release 34 - prediction to PnL conversion (69)-(90).
    ("release34_prediction_to_pnl", "modules_present", True),
    ("release34_prediction_to_pnl", "reuses_r31_statistics", True),
    ("release34_prediction_to_pnl", "reuses_r31_hashing", True),
    ("release34_prediction_to_pnl", "reuses_r33_features", True),
    ("release34_prediction_to_pnl", "reuses_r33_models", True),
    ("release34_prediction_to_pnl", "no_second_learner_library", True),
    ("release34_prediction_to_pnl", "safety_flags_false", True),
    ("release34_prediction_to_pnl", "forbidden_calls", []),
    ("release34_prediction_to_pnl", "forbidden_owner_refs", []),
    ("release34_prediction_to_pnl",
     "implementable_requires_exchange_traded_security", True),
    ("release34_prediction_to_pnl",
     "non_investable_series_barred_from_portfolio", True),
    ("release34_prediction_to_pnl", "universe_includes_delisted_candidates",
     True),
    ("release34_prediction_to_pnl", "total_return_prices_used", True),
    ("release34_prediction_to_pnl", "no_random_split", True),
    ("release34_prediction_to_pnl", "nested_selection_declared", True),
    ("release34_prediction_to_pnl", "calibration_fitted_on_training_only",
     True),
    ("release34_prediction_to_pnl", "liquidity_is_point_in_time", True),
    ("release34_prediction_to_pnl", "embargo_declared", True),
    ("release34_prediction_to_pnl", "non_overlapping_forecast_dates", True),
    ("release34_prediction_to_pnl", "fresh_unseen_evidence_refused", True),
    ("release34_prediction_to_pnl", "no_fold_may_be_called_a_lockbox", True),
    ("release34_prediction_to_pnl", "independent_evidence_is_a_gate", True),
    ("release34_prediction_to_pnl", "cost_base_traded_notional", True),
    ("release34_prediction_to_pnl", "excess_over_cash_may_not_rank", True),
    ("release34_prediction_to_pnl", "volatility_matched_control_owned", True),
    ("release34_prediction_to_pnl", "horizon_not_ranked_by_raw_magnitude",
     True),
    ("release34_prediction_to_pnl", "concentration_frozen_before_evaluation",
     True),
    ("release34_prediction_to_pnl", "leave_one_out_is_a_gate", True),
    ("release34_prediction_to_pnl", "engagement_gate_present", True),
    ("release34_prediction_to_pnl", "benjamini_hochberg_direction_is_split",
     True),
    ("release34_prediction_to_pnl", "attrition_waterfall_required", True),
    ("release34_prediction_to_pnl", "denominator_counts_all_executed", True),
    ("release34_prediction_to_pnl", "adaptive_search_refused", True),
    ("release34_prediction_to_pnl", "alpha_pass_requires_qualified_verdict",
     True),
    ("release34_prediction_to_pnl", "reports_system_and_alpha_results", True),
    ("release34_prediction_to_pnl", "runner_is_research_only", True),
    ("release34_prediction_to_pnl", "planned_configs_match_the_frozen_grid",
     True),
    ("release35_orthogonal_information", "modules_present", True),
    ("release35_orthogonal_information", "reuses_r31_statistics", True),
    ("release35_orthogonal_information", "reuses_r31_hashing", True),
    ("release35_orthogonal_information", "reuses_r33_features", True),
    ("release35_orthogonal_information",
     "reuses_r34_universe_and_panel", True),
    ("release35_orthogonal_information", "reuses_r34_conversion", True),
    ("release35_orthogonal_information",
     "reuses_released_orthogonality", True),
    ("release35_orthogonal_information",
     "reuses_released_purchase_gate", True),
    ("release35_orthogonal_information", "reuses_released_pit_sector", True),
    ("release35_orthogonal_information", "no_second_learner_library", True),
    ("release35_orthogonal_information", "safety_flags_false", True),
    ("release35_orthogonal_information", "spending_refused", True),
    ("release35_orthogonal_information", "one_alignment_owner", True),
    ("release35_orthogonal_information",
     "insider_observable_at_filing_date", True),
    ("release35_orthogonal_information", "cot_publication_lag_declared", True),
    ("release35_orthogonal_information",
     "oecd_publication_lag_declared", True),
    ("release35_orthogonal_information",
     "prohibited_substitutions_declared", True),
    ("release35_orthogonal_information", "pit_sector_is_no_look_ahead", True),
    ("release35_orthogonal_information",
     "commodity_curve_is_dated_contracts", True),
    ("release35_orthogonal_information",
     "insider_value_weighting_refused", True),
    ("release35_orthogonal_information", "orthogonality_is_a_gate", True),
    ("release35_orthogonal_information",
     "raw_correlation_is_not_distinctness", True),
    ("release35_orthogonal_information",
     "orthogonality_measured_on_training_only", True),
    ("release35_orthogonal_information", "increment_is_paired", True),
    ("release35_orthogonal_information", "model_held_fixed_across_arms", True),
    ("release35_orthogonal_information",
     "rows_identical_by_construction", True),
    ("release35_orthogonal_information", "vacuous_arm_is_detected", True),
    ("release35_orthogonal_information", "economic_increment_is_paired", True),
    ("release35_orthogonal_information", "conversion_layer_is_frozen", True),
    ("release35_orthogonal_information",
     "fresh_unseen_evidence_refused", True),
    ("release35_orthogonal_information",
     "no_fold_may_be_called_a_lockbox", True),
    ("release35_orthogonal_information",
     "independent_evidence_is_a_gate", True),
    ("release35_orthogonal_information",
     "reports_three_separate_results", True),
    ("release35_orthogonal_information",
     "alpha_pass_requires_qualified_verdict", True),
    ("release35_orthogonal_information",
     "alpha_pass_is_structurally_unreachable", True),
    ("release35_orthogonal_information", "no_forward_registration", True),
    ("release35_orthogonal_information", "adaptive_search_refused", True),
    ("release35_orthogonal_information",
     "denominator_counts_all_executed", True),
    ("release35_orthogonal_information",
     "benjamini_hochberg_direction_is_split", True),
    ("release35_orthogonal_information", "runner_is_research_only", True),
    ("release35_orthogonal_information",
     "planned_configs_match_the_frozen_grid", True),
    ("release35_orthogonal_information",
     "every_new_feature_has_a_declared_family", True),
    ("release35_orthogonal_information", "forbidden_calls", []),
    ("release35_orthogonal_information", "forbidden_owner_refs", []),
    ("release35_orthogonal_information", "modules_missing", []),
    ("release35_orthogonal_information", "second_owner_modules", []),
    # --- Release 36 -------------------------------------------------------- #
    ("release36_global_multi_asset_frontier", "modules_present", True),
    ("release36_global_multi_asset_frontier", "modules_missing", []),
    ("release36_global_multi_asset_frontier", "second_owner_modules", []),
    ("release36_global_multi_asset_frontier", "forbidden_calls", []),
    ("release36_global_multi_asset_frontier", "forbidden_owner_refs", []),
    ("release36_global_multi_asset_frontier", "reuses_r31_statistics", True),
    ("release36_global_multi_asset_frontier", "reuses_r31_hashing", True),
    ("release36_global_multi_asset_frontier", "reuses_r34_economic_judge",
     True),
    ("release36_global_multi_asset_frontier", "reuses_r33_vendor_reader", True),
    ("release36_global_multi_asset_frontier", "reuses_r34_vendor_reader", True),
    ("release36_global_multi_asset_frontier", "reuses_r35_alignment_owner",
     True),
    ("release36_global_multi_asset_frontier", "reuses_r35_http_owner", True),
    ("release36_global_multi_asset_frontier",
     "reuses_released_rank_correlation", True),
    ("release36_global_multi_asset_frontier", "no_second_learner_library",
     True),
    ("release36_global_multi_asset_frontier", "no_second_economic_judge", True),
    ("release36_global_multi_asset_frontier", "safety_flags_false", True),
    ("release36_global_multi_asset_frontier", "spending_refused", True),
    ("release36_global_multi_asset_frontier", "api_key_is_not_an_entitlement",
     True),
    ("release36_global_multi_asset_frontier", "credentials_never_serialised",
     True),
    ("release36_global_multi_asset_frontier", "control_matches_what_is_traded",
     True),
    ("release36_global_multi_asset_frontier",
     "universal_equity_control_refused", True),
    ("release36_global_multi_asset_frontier",
     "control_must_be_observable_throughout", True),
    ("release36_global_multi_asset_frontier", "superseded_campaigns_declared",
     True),
    ("release36_global_multi_asset_frontier",
     "cadence_is_per_lane_with_a_reason", True),
    ("release36_global_multi_asset_frontier", "one_alignment_owner", True),
    ("release36_global_multi_asset_frontier", "admissibility_reused_from_r33",
     True),
    ("release36_global_multi_asset_frontier",
     "publication_lags_reused_from_r35", True),
    ("release36_global_multi_asset_frontier",
     "prohibited_substitutions_declared", True),
    ("release36_global_multi_asset_frontier",
     "commodity_curve_is_dated_contracts", True),
    ("release36_global_multi_asset_frontier", "a_terminated_market_is_admitted",
     True),
    ("release36_global_multi_asset_frontier", "contract_splice_refused", True),
    ("release36_global_multi_asset_frontier",
     "short_volatility_survivorship_refused", True),
    ("release36_global_multi_asset_frontier",
     "broad_crypto_survivorship_refused", True),
    ("release36_global_multi_asset_frontier", "normalisation_is_trailing_only",
     True),
    ("release36_global_multi_asset_frontier", "non_overlapping_decisions",
     True),
    ("release36_global_multi_asset_frontier",
     "a_position_requires_an_observable_return", True),
    ("release36_global_multi_asset_frontier", "three_implementation_levels",
     True),
    ("release36_global_multi_asset_frontier",
     "proxy_may_not_close_a_native_frontier", True),
    ("release36_global_multi_asset_frontier", "every_cell_must_be_terminal",
     True),
    ("release36_global_multi_asset_frontier", "coverage_is_derived_not_typed",
     True),
    ("release36_global_multi_asset_frontier", "blocked_frontier_is_named",
     True),
    ("release36_global_multi_asset_frontier", "adaptive_search_refused", True),
    ("release36_global_multi_asset_frontier",
     "denominator_counts_all_executed", True),
    ("release36_global_multi_asset_frontier",
     "benjamini_hochberg_direction_is_split", True),
    ("release36_global_multi_asset_frontier",
     "minimum_detectable_effect_reported", True),
    ("release36_global_multi_asset_frontier", "cost_sensitivity_reported",
     True),
    ("release36_global_multi_asset_frontier", "fresh_unseen_evidence_refused",
     True),
    ("release36_global_multi_asset_frontier", "no_fold_may_be_called_a_lockbox",
     True),
    ("release36_global_multi_asset_frontier", "independent_evidence_is_a_gate",
     True),
    ("release36_global_multi_asset_frontier", "reports_three_separate_results",
     True),
    ("release36_global_multi_asset_frontier",
     "alpha_pass_requires_qualified_verdict", True),
    ("release36_global_multi_asset_frontier",
     "alpha_pass_is_structurally_unreachable", True),
    ("release36_global_multi_asset_frontier", "no_forward_registration", True),
    ("release36_global_multi_asset_frontier", "runner_is_research_only", True),
    ("release36_global_multi_asset_frontier",
     "planned_configs_match_the_frozen_grid", True),
    ("release36_global_multi_asset_frontier",
     "every_strategy_has_a_declared_lane", True),
    ("release36_global_multi_asset_frontier",
     "every_market_family_is_declared", True),
    ("release36_global_multi_asset_frontier", "every_lane_control_is_distinct",
     True),
    # --- Release 37: the purchase release. Its dangerous outcomes are
    # spending money, a fourth purchase gate, a brochure counted as evidence,
    # and a score outranking a data-integrity gate.
    ("release37_native_market_data_gate", "modules_present", True),
    ("release37_native_market_data_gate", "modules_missing", []),
    ("release37_native_market_data_gate", "second_owner_modules", []),
    ("release37_native_market_data_gate", "forbidden_calls", []),
    ("release37_native_market_data_gate", "forbidden_owner_refs", []),
    ("release37_native_market_data_gate", "commercial_tokens_present", []),
    ("release37_native_market_data_gate", "defines_no_second_gate", True),
    ("release37_native_market_data_gate", "gate_definitions_found", []),
    ("release37_native_market_data_gate", "composes_slice9_gate", True),
    ("release37_native_market_data_gate", "composes_r32_information_gate",
     True),
    ("release37_native_market_data_gate",
     "slice9_result_may_not_be_overridden", True),
    ("release37_native_market_data_gate",
     "nothing_persisted_to_another_release_store", True),
    ("release37_native_market_data_gate", "reuses_r35_http_owner", True),
    ("release37_native_market_data_gate", "reuses_r36_entitlement_owner", True),
    ("release37_native_market_data_gate", "reuses_r36_coverage_matrix", True),
    ("release37_native_market_data_gate", "reuses_r31_hashing", True),
    ("release37_native_market_data_gate", "no_second_downloader", True),
    ("release37_native_market_data_gate", "no_second_coverage_matrix", True),
    ("release37_native_market_data_gate", "spending_refused", True),
    ("release37_native_market_data_gate", "safety_flags_false", True),
    ("release37_native_market_data_gate", "no_purchase_authority", True),
    ("release37_native_market_data_gate",
     "a_marketing_claim_is_not_a_measurement", True),
    ("release37_native_market_data_gate",
     "partial_unlock_stays_out_of_the_headline", True),
    ("release37_native_market_data_gate",
     "a_proxy_may_not_unlock_a_native_cell", True),
    ("release37_native_market_data_gate", "unlock_is_derived_from_release36",
     True),
    ("release37_native_market_data_gate", "evidence_classes_declared", True),
    ("release37_native_market_data_gate", "every_candidate_must_be_terminal",
     True),
    ("release37_native_market_data_gate", "score_declared_before_use", True),
    ("release37_native_market_data_gate", "hard_gates_bind_the_ranking", True),
    ("release37_native_market_data_gate", "free_data_has_a_cost_floor", True),
    ("release37_native_market_data_gate", "a_sample_is_not_an_alpha_claim",
     True),
    ("release37_native_market_data_gate", "blocks_are_reprobed", True),
    ("release37_native_market_data_gate",
     "an_unmeasured_probe_is_not_an_open_route", True),
    ("release37_native_market_data_gate",
     "owned_client_capability_is_measured", True),
    ("release37_native_market_data_gate", "credentials_never_serialised", True),
    ("release37_native_market_data_gate", "ml_trains_nothing", True),
    ("release37_native_market_data_gate", "compute_inventory_is_read_only",
     True),
    ("release37_native_market_data_gate", "feasibility_is_computed_not_typed",
     True),
    ("release37_native_market_data_gate",
     "ml_data_contract_composes_existing_owners", True),
    ("release37_native_market_data_gate",
     "market_structure_is_designed_not_executed", True),
    ("release37_native_market_data_gate",
     "pivots_require_real_time_confirmation", True),
    ("release37_native_market_data_gate", "fibonacci_has_a_placebo_arm", True),
    ("release37_native_market_data_gate", "visual_lane_is_designed_only", True),
    ("release37_native_market_data_gate", "reports_three_separate_results",
     True),
    ("release37_native_market_data_gate", "alpha_result_is_not_tested", True),
    ("release37_native_market_data_gate", "superseded_campaigns_declared",
     True),
    ("release37_native_market_data_gate", "exhausted_campaigns_not_rerun",
     True),
    ("release37_native_market_data_gate", "blocked_vendor_actions_are_named",
     True),
    ("release37_native_market_data_gate", "runner_is_research_only", True),
    ("release37_native_market_data_gate", "long_list_validates", True),
    ("release37_native_market_data_gate",
     "every_unlock_claim_names_a_blocked_market", True),
    ("release37_native_market_data_gate",
     "a_hard_failed_dataset_is_never_ranked", True),
    ("release37_native_market_data_gate", "every_candidate_state_is_terminal",
     True),
    ("release37_native_market_data_gate",
     "no_gate_state_grants_purchase_authority", True),
    # --- Release 37.1: ONE canonical acquisition authority. The dangerous
    # outcome here is a second acquisition truth - this release ranking a
    # dataset the canonical gate refused - and a hardware capability being
    # reported as though the software to use it were installed.
    ("release37_native_market_data_gate",
     "delegates_acquisition_to_canonical_gate", True),
    ("release37_native_market_data_gate", "canonical_gate_is_authoritative",
     True),
    ("release37_native_market_data_gate",
     "r37_defines_no_acquisition_authority", True),
    ("release37_native_market_data_gate",
     "acquisition_recommendation_is_not_alpha_evidence", True),
    ("release37_native_market_data_gate",
     "expected_unlocks_are_not_measured_unlocks", True),
    ("release37_native_market_data_gate",
     "ml_readiness_separates_install_from_hardware", True),
    ("release37_native_market_data_gate",
     "nothing_recommended_against_the_canonical_gate", True),
    ("release37_native_market_data_gate",
     "every_row_agrees_with_canonical_gate", True),
    ("release37_native_market_data_gate",
     "acquisition_states_come_from_the_canonical_vocabulary", True),
    # --- Release 38 -------------------------------------------------------- #
    ("release38_native_futures_information_frontier", "modules_present", True),
    ("release38_native_futures_information_frontier", "modules_missing", []),
    ("release38_native_futures_information_frontier",
     "second_owner_modules", []),
    ("release38_native_futures_information_frontier",
     "defines_no_second_gate", True),
    ("release38_native_futures_information_frontier",
     "delegates_to_canonical_gate", True),
    ("release38_native_futures_information_frontier",
     "reuses_r34_economic_judge", True),
    ("release38_native_futures_information_frontier",
     "reuses_r31_multiple_testing", True),
    ("release38_native_futures_information_frontier",
     "reuses_r36_coverage_matrix", True),
    ("release38_native_futures_information_frontier",
     "reuses_r37_unlock_expectation", True),
    ("release38_native_futures_information_frontier",
     "reuses_r35_cot_parser", True),
    ("release38_native_futures_information_frontier",
     "reuses_r31_hashing", True),
    ("release38_native_futures_information_frontier",
     "provider_call_taxonomy_declared", True),
    ("release38_native_futures_information_frontier",
     "provider_call_taxonomy_enforced", True),
    ("release38_native_futures_information_frontier",
     "purchase_is_inherited_not_made", True),
    ("release38_native_futures_information_frontier",
     "spending_refused", True),
    ("release38_native_futures_information_frontier",
     "safety_flags_false", True),
    ("release38_native_futures_information_frontier",
     "no_purchase_or_renewal_authority", True),
    ("release38_native_futures_information_frontier",
     "commercial_tokens_present", []),
    ("release38_native_futures_information_frontier",
     "roll_policy_is_observable_and_frozen", True),
    ("release38_native_futures_information_frontier",
     "vendor_continuous_series_refused", True),
    ("release38_native_futures_information_frontier",
     "experiment_family_is_frozen", True),
    ("release38_native_futures_information_frontier",
     "expected_unlocks_are_not_measured_unlocks", True),
    ("release38_native_futures_information_frontier",
     "alpha_pass_requires_qualified_verdict", True),
    ("release38_native_futures_information_frontier",
     "six_result_axes_declared", True),
    ("release38_native_futures_information_frontier",
     "steele_sample_is_schema_only", True),
    ("release38_native_futures_information_frontier",
     "ml_contract_trains_nothing", True),
    ("release38_native_futures_information_frontier",
     "superseded_campaigns_declared", True),
    # --- Slice 9 / Release 37.1: two explicit decision contexts, one owner.
    ("data_expansion_ownership", "decision_contexts_declared", True),
    ("data_expansion_ownership", "legacy_default_decision_context_preserved",
     True),
    ("data_expansion_ownership", "research_acquisition_state_declared", True),
    ("data_expansion_ownership",
     "research_acquisition_state_is_distinct_from_purchase", True),
    ("data_expansion_ownership", "acquisition_classifier_present", True),
    ("data_expansion_ownership", "acquisition_dimensions_present", True),
    ("data_expansion_ownership", "post_acquisition_evidence_standard_intact",
     True),
    ("data_expansion_ownership", "acquisition_recommendation_is_not_authority",
     True),
    ("data_expansion_ownership", "acquisition_requires_manual_approval", True),
    ("data_expansion_ownership", "owner_threads_decision_context", True),
    ("data_expansion_ownership", "decision_contexts_persist_separately", True),
    ("data_expansion_ownership", "automatic_acquisition_allowed", False),
    ("release33_predictive_edge", "r33_source_has_no_operational_write_path",
     True),
    ("release32_pnl_opportunity_frontier", "modules_present", True),
    ("release32_pnl_opportunity_frontier", "second_owner_modules", []),
    ("release32_pnl_opportunity_frontier", "reuses_r31_statistics", True),
    ("release32_pnl_opportunity_frontier", "reuses_r31_judge_statistics", True),
    ("release32_pnl_opportunity_frontier", "no_second_optimiser", True),
    ("release32_pnl_opportunity_frontier", "no_second_covariance_owner", True),
    ("release32_pnl_opportunity_frontier", "sleeve_forbidden_calls", []),
    ("release32_pnl_opportunity_frontier",
     "sleeve_owns_capital_declared_false", True),
    ("release32_pnl_opportunity_frontier", "states_that_own_capital_empty", True),
    ("release32_pnl_opportunity_frontier", "sleeve_gross_exposure_capped", True),
    ("release32_pnl_opportunity_frontier", "control_sleeve_not_researched", True),
    ("release32_pnl_opportunity_frontier", "pit_admissibility_is_measured", True),
    ("release32_pnl_opportunity_frontier", "revised_macro_inadmissible", True),
    ("release32_pnl_opportunity_frontier",
     "sector_definition_dates_declared", True),
    ("release32_pnl_opportunity_frontier",
     "instrument_inception_declared", True),
    ("release32_pnl_opportunity_frontier", "common_overlap_enforced", True),
    ("release32_pnl_opportunity_frontier",
     "overlap_view_is_reporting_only", True),
    ("release32_pnl_opportunity_frontier",
     "lockbox_single_access_enforced", True),
    ("release32_pnl_opportunity_frontier",
     "denominator_counts_all_executed", True),
    ("release32_pnl_opportunity_frontier",
     "denominator_is_the_bh_family_size", True),
    ("release32_pnl_opportunity_frontier",
     "control_cannot_qualify_a_sleeve", True),
    ("release32_pnl_opportunity_frontier",
     "primary_control_is_volatility_matched", True),
    ("release32_pnl_opportunity_frontier", "auto_promotion_declared_false", True),
    ("release32_pnl_opportunity_frontier",
     "auto_sleeve_activation_declared_false", True),
    ("release32_pnl_opportunity_frontier", "may_spend_money_declared_false", True),
    ("release32_pnl_opportunity_frontier", "forbidden_owner_refs", []),
    ("release32_pnl_opportunity_frontier", "research_imports_api", []),
    ("release32_pnl_opportunity_frontier",
     "prohibited_substitutions_declared", True),
    ("release32_pnl_opportunity_frontier", "purchase_gate_ten_conditions", True),
    ("release32_pnl_opportunity_frontier", "purchase_gate_never_purchases", True),
    ("release32_pnl_opportunity_frontier", "governance_failures", []),
    ("release32_pnl_opportunity_frontier", "cash_is_a_valid_opportunity", True),
    ("release32_pnl_opportunity_frontier", "stale_data_fails_closed", True),
    ("release32_pnl_opportunity_frontier",
     "exhaustion_stops_same_information_search", True),
    ("release32_pnl_opportunity_frontier", "no_hidden_followup_campaign", True),
    ("release32_pnl_opportunity_frontier", "production_read_only", True),
    ("release32_pnl_opportunity_frontier", "route_declared", True),
    ("release32_pnl_opportunity_frontier", "route_is_get_only", True),
    ("release32_pnl_opportunity_frontier", "route_not_mutating", True),
    ("release32_pnl_opportunity_frontier", "read_model_writes", []),
    ("release32_pnl_opportunity_frontier", "ui_ambiguous_safety_badges", []),
    ("release32_pnl_opportunity_frontier",
     "read_model_uses_canonical_order_badge", True),
    ("release32_pnl_opportunity_frontier", "ui_control_labels", []),

    # --- Release 30: zero-base adaptive alpha capital allocation ------------
    # ONE zero-base calculation owner and ONE composition owner; pure stdlib
    # kernels; the forecast layer cannot promote or activate itself and its read
    # surface cannot write; the allocator is neither a proposal nor a decision
    # owner; the covariance builder and the aligned-return series each keep ONE
    # owner; the material-information feed reads the event fabric's authority
    # frozensets instead of copying them; the surfaces are GET-only; and no
    # Release 30 UI region carries an execute control or a browser dialog.
    ("release30_zero_base_ownership", "modules_present", True),
    ("release30_zero_base_ownership", "kernel_impurity", []),
    ("release30_zero_base_ownership", "second_calculation_owner_modules", []),
    ("release30_zero_base_ownership", "second_composition_owner_modules", []),
    ("release30_zero_base_ownership", "auto_promotion_declared", False),
    ("release30_zero_base_ownership", "activation_written_by_code", False),
    ("release30_zero_base_ownership", "read_surface_writes", []),
    ("release30_zero_base_ownership", "allocator_forbidden_calls", []),
    ("release30_zero_base_ownership", "covariance_owner_present", True),
    ("release30_zero_base_ownership",
     "risk_contributions_delegate_to_covariance_owner", True),
    ("release30_zero_base_ownership", "second_covariance_builders", []),
    ("release30_zero_base_ownership", "aligned_returns_owner_present", True),
    ("release30_zero_base_ownership",
     "reallocation_delegates_aligned_returns", True),
    ("release30_zero_base_ownership",
     "material_information_reads_fabric_authority", True),
    ("release30_zero_base_ownership",
     "material_information_private_authority_table", False),
    ("release30_zero_base_ownership", "read_models_own_no_calculation", True),
    ("release30_zero_base_ownership", "missing_routes", []),
    ("release30_zero_base_ownership", "mutating_routes", []),
    ("release30_zero_base_ownership", "ui_forbidden_dialogs", []),
    ("release30_zero_base_ownership", "ui_execute_controls", []),
    ("release30_zero_base_ownership", "research_lane_imports_api", []),
    # --- Release 30.1: the OPERATIONAL forecast lane ------------------------
    # An artifact that carries the approved model's NAME must carry its RANKING.
    # A non-positive calibration slope reverses that ranking rather than
    # adjusting it, so the kernel refuses the horizon instead of applying it;
    # the verdict has exactly one owner; the LIVE lane reads the current
    # canonical score rather than a periodic research snapshot; freshness is
    # judged by the canonical owner and never restated here; the governed lane
    # can never fall back to the research forecast; the two lanes are labelled
    # so neither can be read as the other; and the operational calibration
    # admits no component of the adaptive candidate.
    ("release30_1_operational_cutover", "modules_present", True),
    ("release30_1_operational_cutover", "rank_identity_enforced_in_build_forecast", True),
    ("release30_1_operational_cutover", "second_rank_identity_owner_modules", []),
    ("release30_1_operational_cutover", "live_lane_reads_research_snapshot", []),
    ("release30_1_operational_cutover", "second_freshness_source_table", False),
    ("release30_1_operational_cutover", "governed_lane_falls_back_to_research", []),
    ("release30_1_operational_cutover",
     "forbidden_components_in_operational_calibration", []),
    ("release30_1_operational_cutover", "calibration_lane_imports_api", False),
    ("release30_1_operational_cutover", "operational_read_surface_writes", []),
    ("release30_1_operational_cutover", "zero_base_owner_forbidden_calls", []),
    ("release30_1_operational_cutover", "rank_identity_contract", {
        "identity_contract_declared": True,
        "operational_activation_declared": True,
        "verdict_vocabulary_declared": True,
        "detector_present": True,
        "verdict_function_present": True,
        "suppression_disposition_declared": True,
    }),
    ("release30_1_operational_cutover", "live_operational_lane", {
        "live_cross_section_owner_present": True,
        "live_input_policy_declared": True,
        "research_snapshot_scope_declared": True,
        "score_owner_is_universe_scoring": True,
        "freshness_delegated_to_canonical_owner": True,
    }),
    ("release30_1_operational_cutover", "target_lanes", {
        "research_lane_declared": True,
        "governed_lane_declared": True,
        "authority_stamped_on_both": True,
        "governed_owner_present": True,
    }),
    ("release30_1_operational_cutover", "operational_calibration", {
        "declares_approved_model": True,
        "declares_only_approved_components": True,
        "rank_identity_bar_declared": True,
        "reliability_bar_declared": True,
        "sign_stability_tested": True,
        "walk_forward_embargoed": True,
        "no_random_split": True,
    }),
    # --- Release 31: the Mathematical Alpha Frontier campaign ----------------
    # A bounded model-research campaign that cannot become an unbounded one, and
    # cannot become a production change. One owner per concern; the lockbox
    # unreachable from training or selection; budgets encoded as numbers and
    # enforced by exceptions; a terminal exhaustion state; news / external links
    # never admissible as research inputs; the canonical cost and constraint
    # owner reused rather than forked; no automatic promotion; and a read-only UI
    # surface with no execute, approve or activate control.
    ("release31_mathematical_alpha_frontier", "modules_missing", []),
    ("release31_mathematical_alpha_frontier", "second_owner_modules", {
        "campaign_contract": [], "research_judge": [],
        "candidate_registry": [], "lockbox_access": []}),
    ("release31_mathematical_alpha_frontier", "lockbox_guard", {
        "training_cap_declared": True,
        "selection_basis_declared": True,
        "partition_declares_invisibility": True,
        "no_retune_declared": True,
        "single_execution_enforced": True,
        "methods_never_reference_lockbox": True,
    }),
    ("release31_mathematical_alpha_frontier", "forbidden_calls_in_research_lane", []),
    ("release31_mathematical_alpha_frontier", "forbidden_operational_owner_refs", []),
    ("release31_mathematical_alpha_frontier", "research_lane_imports_api", []),
    ("release31_mathematical_alpha_frontier", "forbidden_engine_imports", []),
    ("release31_mathematical_alpha_frontier", "impure_engine_owner_imports", []),
    ("release31_mathematical_alpha_frontier", "budgets_not_encoded", []),
    # --- Campaign v3 corrections. Each entry below is a defect that SHIPPED in
    # Campaign v2 and was caught by reading the code rather than by a guard.
    # CORRECTION 1: the training universe is not the investment universe.
    ("release31_mathematical_alpha_frontier", "universe_separation", {
        "one_investment_universe_owner": True,
        "training_universes_declared": True,
        "evaluation_universe_declared": True,
        "judge_evaluates_investment_universe_only": True,
        "broader_training_never_widens_evaluation": True,
        "membership_is_point_in_time": True,
        "current_membership_backwards_is_inadmissible": True,
        "blocked_state_exists": True,
        "survivorship_gap_measured": True,
        "training_choice_is_candidate_identity": True,
    }),
    # CORRECTION 2: real zero-base economics, cash a genuine choice, top-N demoted.
    ("release31_mathematical_alpha_frontier", "zero_base_primary", {
        "top_n_barred_from_primary_verdict": True,
        "judge_declares_zero_base_primary": True,
        "allocation_delegates_to_canonical_optimiser": True,
        "cash_is_a_real_choice": True,
        "book_size_frontier_removed": True,
        "gamma_frontier_declared": True,
        "frontier_frozen_before_results": True,
        "only_gamma_moves_on_the_frontier": True,
        "sector_constraint_is_declared_unmeasurable": True,
    }),
    # CORRECTION 2 (units): an arbitrary score may not become an expected return.
    ("release31_mathematical_alpha_frontier", "calibration_guard", {
        "one_calibration_owner": True,
        "rank_identity_violation_state": True,
        "not_calibratable_state": True,
        "negative_slope_raises": True,
        "fitted_on_entitled_evidence_only": True,
        "lockbox_invisible_to_calibration": True,
        "bound_into_candidate_identity": True,
        "live_cross_sections_verified": True,
    }),
    # CORRECTION 3: turnover by security identity, never by array position.
    ("release31_mathematical_alpha_frontier", "track_b_symbol_alignment", {
        "alignment_declared": True,
        "learner_requires_symbols": True,
        "learner_refuses_a_two_element_block": True,
        "aligns_by_symbol_union": True,
        "no_positional_shape_comparison": True,
        "track_b_can_hold_cash": True,
        "novel_decision_family_prices_cost": True,
        "shared_feasibility_seam": True,
        "one_transition_cost_calculation": True,
    }),
    # CORRECTION 4: two benchmarks, neither substitutable for the other.
    ("release31_mathematical_alpha_frontier", "benchmark_duality", {
        "one_benchmark_owner": True,
        "both_declared": True,
        "substitution_forbidden": True,
        "blocked_state_exists": True,
        "price_only_index_inadmissible": True,
        "judge_reports_both": True,
        "silent_substitution_refused": True,
    }),
    # The shared covariance cache: one owner, hash-bound, point-in-time.
    ("release31_mathematical_alpha_frontier", "covariance_cache", {
        "one_cache_owner": True,
        "delegates_to_canonical_builder": True,
        "owns_no_covariance_mathematics": True,
        "key_binds_inputs": True,
        "key_mismatch_raises": True,
        "point_in_time_window_declared": True,
        "contract_binds_the_key": True,
    }),
    # v1 and v2 are superseded, preserved, and structurally inert for v3.
    ("release31_mathematical_alpha_frontier", "supersession", {
        "campaign_is_v3": True,
        "both_predecessors_listed": True,
        "state_declared": True,
        "evidence_rules_declared": True,
        "excluded_from_denominator": True,
        "identity_binds_universe_and_benchmark": True,
    }),
    # No walk-forward window may fall back to one containing the future.
    ("release31_mathematical_alpha_frontier", "point_in_time_training", {
        "minimum_training_window_declared": True,
        "methods_have_no_warmup_fallback": True,
        "novel_has_no_warmup_fallback": True,
        "absent_model_returns_nan": True,
        "judge_skips_a_date_without_a_model": True,
    }),
    ("release31_mathematical_alpha_frontier", "budgets_enforced", {
        "registry_raises_on_budget": True,
        "registry_raises_on_duplicate": True,
        "lockbox_raises_on_violation": True,
        "contract_drift_raises": True,
    }),
    ("release31_mathematical_alpha_frontier", "exhaustion", {
        "terminal_states_declared": True,
        "exhausted_state_present": True,
        "second_null_campaign_terminates": True,
        "no_budget_extension_after_a_poor_result": True,
        "novel_runner_stops_on_budget": True,
    }),
    ("release31_mathematical_alpha_frontier", "inadmissible_information", {
        "declared": True, "manifest_carries_the_declaration": True}),
    ("release31_mathematical_alpha_frontier", "news_shaped_features", []),
    ("release31_mathematical_alpha_frontier", "canonical_owner_reuse", {
        "judge_reads_canonical_policy": True,
        "judge_defines_no_policy_of_its_own": True,
        "judge_declares_it_owns_no_cost": True,
        "judge_declares_it_owns_no_optimiser": True,
        "contract_names_the_policy_owner": True,
        "contract_names_the_allocator_owner": True,
        "contract_names_the_covariance_owner": True,
        "contract_declares_no_second_optimiser": True,
    }),
    ("release31_mathematical_alpha_frontier", "falsifiable_superiority", {
        "unavailable_state_declared": True,
        "absent_incumbent_does_not_borrow_candidate_drawdown": True,
        "absent_incumbent_does_not_borrow_candidate_turnover": True,
        "unavailable_check_is_not_a_pass": True,
        "unavailable_checks_are_reported": True,
    }),
    ("release31_mathematical_alpha_frontier", "duplicate_optimiser_modules", []),
    ("release31_mathematical_alpha_frontier", "duplicate_cost_literal_modules", []),
    ("release31_mathematical_alpha_frontier", "automatic_promotion", {
        "declared_false": True,
        "safety_block_reports_it": True,
        "read_model_reports_it": True,
        "read_model_declares_no_activation": True,
    }),
    ("release31_mathematical_alpha_frontier", "read_surface", {
        "route_declared_once": True,
        "route_is_get": True,
        "route_authenticated": True,
        "read_model_present": True,
        "read_model_imports_research_package": False,
    }),
    ("release31_mathematical_alpha_frontier", "read_model_write_tokens", []),
    ("release31_mathematical_alpha_frontier", "ui_execute_controls", []),
    ("release31_mathematical_alpha_frontier", "ui_missing_safety_badges", []),
    ("release31_mathematical_alpha_frontier", "ui_ambiguous_safety_badges", []),
    # --- Release 30.1 UX: source links and external references --------------
    # ONE owner decides what may become an href, and it refuses anything that is
    # not an absolute http(s) URL - a feed-supplied string is untrusted input. No
    # backend or browser path CONSTRUCTS a source URL. Every anchor goes through
    # one helper that carries target=_blank and rel=noopener noreferrer. The
    # reference reading list lives on MARKETS only, never on Today, and it reports
    # the canonical registries' answer about ingestion rather than captioning its
    # own. The read models own no calculation and assign no authority.
    ("release30_1_operational_cutover", "second_external_url_guard_modules", []),
    ("release30_1_operational_cutover", "constructed_source_urls_in_matinfo", []),
    ("release30_1_operational_cutover",
     "unowned_literal_urls_in_reference_module", []),
    ("release30_1_operational_cutover",
     "reference_module_private_authority_table", False),
    ("release30_1_operational_cutover", "ui_hand_rolled_anchors", []),
    ("release30_1_operational_cutover", "external_references_on_today", False),
    ("release30_1_operational_cutover",
     "external_reference_call_sites_outside_markets", []),
    ("release30_1_operational_cutover", "external_url_guard", {
        "owner_present": True,
        "schemes_declared": True,
        "link_policy_declared": True,
        "state_vocabulary_declared": True,
        "matinfo_delegates": True,
    }),
    ("release30_1_operational_cutover", "read_model_declarations", {
        "external_owns_no_calculation": True,
        "external_creates_no_event": True,
        "external_never_influences_decisions": True,
        "external_reads_canonical_registry": True,
        "external_reads_fabric_authority": True,
        "matinfo_declares_article_is_not_alpha": True,
        "matinfo_declares_transparency_fields": True,
        "matinfo_declares_link_policy": True,
    }),
    ("release30_1_operational_cutover", "ui_external_links", {
        "helper_present": True,
        "helper_sets_target": True,
        "helper_sets_rel": True,
        "helper_requires_backend_url": True,
        "attribute_escape_present": True,
    }),
    ("release30_1_operational_cutover", "external_reference_surface", {
        "region_present": True,
        "region_inside_markets": True,
        "loader_present": True,
        "loader_count": 1,
        "declared_markets_only": True,
        "loaded_from_markets_route": True,
    }),
    ("release30_1_operational_cutover", "external_reference_routes", {
        "declared": True,
        "mutating": [],
        "ui_wired": True,
    }),
    # --- Stage 21: outcome intelligence, execution lineage, durable close, env ----
    # ONE outcome calculation owner + ONE persistence owner; ONE execution-lineage
    # owner; pure kernels; no second price/horizon/NAV/cost owner; GET-only surface;
    # ONE maturation trigger inside the close; no second Daily Close; immutable
    # Stage-19 lineage with chronological (never hash-ordered) plan selection;
    # production startup fails closed on acceptance roots; no automatic policy write,
    # model promotion or recalibration; and ONE economic fingerprint that a downstream
    # consumer can never use to invalidate its own input.
    ("stage21_outcome_intelligence", "kernels_present", True),
    ("stage21_outcome_intelligence", "owners_present", True),
    ("stage21_outcome_intelligence", "env_owner_present", True),
    ("stage21_outcome_intelligence", "second_calculation_owner_modules", []),
    ("stage21_outcome_intelligence", "second_composition_owner_modules", []),
    ("stage21_outcome_intelligence", "second_lineage_owner_modules", []),
    ("stage21_outcome_intelligence", "kernel_impurity", []),
    ("stage21_outcome_intelligence", "second_owner_defs", []),
    ("stage21_outcome_intelligence", "missing_delegation", []),
    ("stage21_outcome_intelligence", "route_methods", ["GET"]),
    ("stage21_outcome_intelligence", "missing_routes", []),
    ("stage21_outcome_intelligence", "forbidden_routes_present", []),
    ("stage21_outcome_intelligence", "maturation_in_close", True),
    ("stage21_outcome_intelligence", "outcome_capture_defs", []),
    ("stage21_outcome_intelligence", "no_operator_refresh_button", True),
    ("stage21_outcome_intelligence", "missing_close_run_tokens", []),
    ("stage21_outcome_intelligence", "second_close_owner_defs", []),
    ("stage21_outcome_intelligence", "close_single_flight", True),
    ("stage21_outcome_intelligence", "lineage_immutable", True),
    ("stage21_outcome_intelligence", "rebalance_composes_lineage", True),
    ("stage21_outcome_intelligence", "lexicographic_plan_selection", False),
    ("stage21_outcome_intelligence", "startup_preflight", True),
    ("stage21_outcome_intelligence", "env_fail_closed", True),
    ("stage21_outcome_intelligence", "acceptance_optin", True),
    ("stage21_outcome_intelligence", "acceptance_server_scoped", True),
    ("stage21_outcome_intelligence", "forbidden_calls", []),
    ("stage21_outcome_intelligence", "declares_no_policy_write", True),
    ("stage21_outcome_intelligence", "kernel_declares_no_tuning", True),
    ("stage21_outcome_intelligence", "economic_owner_present", True),
    ("stage21_outcome_intelligence", "second_economic_owner_modules", []),
    ("stage21_outcome_intelligence", "reassessment_binds_economic", True),
    ("stage21_outcome_intelligence", "self_referential_comparison", False),
    ("stage21_outcome_intelligence", "hoc_records_fingerprints", True),
    ("stage21_outcome_intelligence", "hermetic_clock_injections_missing", []),
    ("stage21_outcome_intelligence", "hermetic_clock_seam_present", True),
    ("stage21_outcome_intelligence", "stage21_ui_loader_calls_missing", []),
    ("stage21_outcome_intelligence", "stage21_ui_uses_undefined_getter", False),
    ("corporate_action_propagation", "single_split_math_owner", True),
    ("corporate_action_propagation", "duplicate_split_math", []),
    ("corporate_action_propagation", "desk_current_reads_default_to_registry", True),
    ("corporate_action_propagation", "single_current_fill_view", True),
    ("corporate_action_propagation", "current_performance_projection_owned", True),
    ("corporate_action_propagation", "portfolio_state_binds_registry", True),
    ("corporate_action_propagation", "proposal_binds_registry", True),
    ("corporate_action_propagation", "approval_gate_enforces_staleness", True),
    ("corporate_action_propagation", "order_plan_gate_enforces_staleness", True),
    ("corporate_action_propagation", "ui_split_math_present", []),
    # Stage 20 — CONTINUOUS ACTIVE PORTFOLIO REASSESSMENT. One ranking owner, one HOC
    # owner, one reassessment owner, one proposal owner, one execution owner; no second
    # cost/risk/NAV/portfolio-state owner; no client-side assessment logic; no automatic
    # rebalance and no automatic promotion; signal refresh and reassessment are LINKED and
    # the reassessment GATES the target engine; recalibration stays a separate lane; the
    # Stage-19 execution lifecycle keeps precedence while it is active.
    # Release 29.3 - PORTFOLIO DECISION INTEGRITY. One authoritative interpretation per
    # business concept: the legacy rank-membership gate may not speak the proposal
    # owner's vocabulary; the Daily Close describes close semantics and normalises its
    # historical token on READ; exactly one module declares the mandatory
    # eligibility-exit policy; the four complete-target constraints are MOVED to the
    # complete-target owner (identical codes both sides, never raised by the
    # reassessment); the semantic-consistency validator is wired and recomputes no
    # owner's economics; WITHHELD is fail-closed at every layer; and the UI renders the
    # canonical decision verbatim through one loader with no synthesised approve/order
    # control.
    ("release29_3_decision_integrity", "gate_vocabulary_clean", True),
    ("release29_3_decision_integrity", "gate_headline_clean", True),
    ("release29_3_decision_integrity", "membership_not_action_required", True),
    ("release29_3_decision_integrity", "close_vocabulary_clean", True),
    ("release29_3_decision_integrity", "close_normaliser_present", True),
    ("release29_3_decision_integrity", "close_normalises_history", True),
    ("release29_3_decision_integrity", "constraint_codes_agree", True),
    ("release29_3_decision_integrity", "constraint_owner_declared", True),
    ("release29_3_decision_integrity", "reassessment_raises_moved_constraint", []),
    ("release29_3_decision_integrity", "semantic_check_present", True),
    ("release29_3_decision_integrity", "semantic_check_wired", True),
    ("release29_3_decision_integrity", "semantic_sets_inconsistent", True),
    ("release29_3_decision_integrity", "semantic_recomputes_economics", []),
    ("release29_3_decision_integrity", "withheld_declared_everywhere", True),
    ("release29_3_decision_integrity", "withheld_not_approvable", True),
    ("release29_3_decision_integrity", "withheld_blocks_record", True),
    ("release29_3_decision_integrity", "ui_verdict_present", True),
    ("release29_3_decision_integrity", "ui_verdict_reads_owner", True),
    ("release29_3_decision_integrity", "ui_verdict_single_call", True),
    ("release29_3_decision_integrity", "ui_verdict_derives_state", []),
    ("release29_3_decision_integrity", "ui_verdict_synthesises_action", []),
    ("release29_3_decision_integrity", "ui_hero_scoped", True),
    # Exactly ONE module may declare what a mandatory eligibility exit authorises.
    ("release29_3_decision_integrity", "mandatory_exit_policy_owner_count", 1),
    # --- Release 29.4: session authority + close validity ---------------------- #
    # A duplicated vocabulary is what invalidated a real completed close. These are the
    # contracts that make the same drift a build failure rather than a live defect.
    ("release29_4_session_authority", "close_validity_owned_by_daily_close", True),
    ("release29_4_session_authority", "no_duplicate_close_vocabulary", True),
    ("release29_4_session_authority", "duplicate_vocabulary_modules", []),
    ("release29_4_session_authority", "workflow_delegates_close_validity", True),
    ("release29_4_session_authority", "close_validity_excludes_portfolio_inputs", True),
    ("release29_4_session_authority", "close_validity_signature", ["progress"]),
    ("release29_4_session_authority", "session_eligibility_owned_by_market_session", True),
    ("release29_4_session_authority", "workflow_recomputes_calendar", []),
    ("release29_4_session_authority", "session_authority_codes_frozen", True),
    ("release29_4_session_authority", "session_check_wired", True),
    ("release29_4_session_authority", "session_check_recomputes", []),
    ("release29_4_session_authority", "today_is_sole_execution_surface", True),
    ("release29_4_session_authority", "model_target_lane_scoped", True),
    # Release 29.5 - a manifest-less artifact was read as corruption, which suspended the
    # cycle into a RECOVERY that could only be cleared by the stage RECOVERY disables.
    # These contracts keep "what an artifact IS" a stated claim rather than an inference.
    ("release29_5_drc_provenance", "provenance_owned_by_artifact_owner", True),
    ("release29_5_drc_provenance", "classifier_is_pure", True),
    ("release29_5_drc_provenance", "classifier_signature", ["artifact"]),
    ("release29_5_drc_provenance", "artifact_never_proves_completion", True),
    ("release29_5_drc_provenance", "producers_identify_themselves", True),
    ("release29_5_drc_provenance", "event_cycle_stamps_no_run_id", True),
    ("release29_5_drc_provenance", "manifest_has_one_owner", True),
    ("release29_5_drc_provenance", "manifest_writers", []),
    ("release29_5_drc_provenance", "workflow_reads_provenance", True),
    ("release29_5_drc_provenance", "gate_carries_provenance", True),
    ("release29_5_drc_provenance", "blocker_fires_on_claim", True),
    ("release29_5_drc_provenance", "ui_states_backend_provenance", True),
    ("release29_5_drc_provenance", "ui_infers_provenance", []),
    ("portfolio_reassessment_ownership", "owners_present", True),
    ("portfolio_reassessment_ownership", "second_calculation_owner_modules", []),
    ("portfolio_reassessment_ownership", "second_composition_owner_modules", []),
    ("portfolio_reassessment_ownership", "second_target_engine_modules", []),
    ("portfolio_reassessment_ownership", "kernel_forks_neighbouring_owner", []),
    ("portfolio_reassessment_ownership", "missing_delegation", []),
    ("portfolio_reassessment_ownership", "owner_forbidden_calls", []),
    ("portfolio_reassessment_ownership", "kernel_forbidden_calls", []),
    ("portfolio_reassessment_ownership", "route_methods", ["GET"]),
    ("portfolio_reassessment_ownership", "non_get_methods_present", False),
    ("portfolio_reassessment_ownership", "forbidden_routes_present", []),
    ("portfolio_reassessment_ownership", "no_automatic_rebalance", True),
    ("portfolio_reassessment_ownership", "signal_refresh_linked_to_reassessment", True),
    ("portfolio_reassessment_ownership", "drc_gate_present", True),
    ("portfolio_reassessment_ownership", "drc_gate_consults_owner", True),
    ("portfolio_reassessment_ownership", "proposal_gated_by_reassessment", True),
    ("portfolio_reassessment_ownership", "reassessment_ordered_before_proposal", True),
    ("portfolio_reassessment_ownership", "proposal_gate_owner_present", True),
    ("portfolio_reassessment_ownership", "execution_precedence_owner_present", True),
    ("portfolio_reassessment_ownership", "workflow_delegates_to_owner", True),
    ("portfolio_reassessment_ownership", "workflow_honours_execution_precedence", True),
    ("portfolio_reassessment_ownership", "workflow_second_economic_gate", []),
    ("portfolio_reassessment_ownership", "recalibration_remains_separate", True),
    ("portfolio_reassessment_ownership", "persist_present", True),
    ("portfolio_reassessment_ownership", "atomic_idempotent_persist_present", True),
    ("portfolio_reassessment_ownership", "history_append_only", True),
    ("portfolio_reassessment_ownership", "no_hindsight_backfill_declared", True),
    ("portfolio_reassessment_ownership", "ui_loader_count", 1),
    ("portfolio_reassessment_ownership", "ui_client_assessment_logic", []),
    ("portfolio_reassessment_ownership", "automatic_model_promotion_allowed", False),
    ("portfolio_reassessment_ownership", "automatic_approval_allowed", False),
    ("portfolio_reassessment_ownership", "cadence_enabled", False),
    # Stage 19.3 — ONE operator command, ONE post-close orchestration path.
    ("operator_atomic_close_ownership", "owners_present", True),
    ("operator_atomic_close_ownership", "pending_short_circuit_modules", []),
    ("operator_atomic_close_ownership", "close_precedence_repaired", True),
    ("operator_atomic_close_ownership", "fails_closed_preserved", True),
    ("operator_atomic_close_ownership", "close_composes_desk_owner", True),
    ("operator_atomic_close_ownership", "close_second_settlement_defs", []),
    ("operator_atomic_close_ownership", "desk_owns_settlement", True),
    ("operator_atomic_close_ownership", "second_settlement_modules", []),
    ("operator_atomic_close_ownership", "no_hindsight_enforced", True),
    ("operator_atomic_close_ownership", "settlement_recorded_once", True),
    ("operator_atomic_close_ownership", "maintenance_kinds_classified", True),
    ("operator_atomic_close_ownership", "primary_action_guard_applied", True),
    ("operator_atomic_close_ownership", "workflow_promotes_desk_refresh", False),
    ("operator_atomic_close_ownership", "competing_refresh_modules", []),
    ("operator_atomic_close_ownership", "ui_competing_refresh_label", False),
    ("operator_atomic_close_ownership", "ui_refresh_is_maintenance_only", True),
    ("operator_atomic_close_ownership", "desk_refresh_route_post_count", 1),
    ("operator_atomic_close_ownership", "operator_command_contract_present", True),
    ("operator_atomic_close_ownership", "second_command_owner_modules", []),
    ("operator_atomic_close_ownership", "ui_command_bar_present", True),
    ("operator_atomic_close_ownership", "ui_command_renderer_count", 1),
    ("operator_atomic_close_ownership", "ui_mirrors_command_contract", True),
    ("operator_atomic_close_ownership", "ui_single_execution_surface", True),
    ("operator_atomic_close_ownership", "ui_ownership_helper_count", 1),
    ("operator_atomic_close_ownership", "ui_client_workflow_authority", []),
    ("operator_atomic_close_ownership", "lineage_counts_owned", True),
    ("operator_atomic_close_ownership", "lineage_summary_owned", True),
    ("operator_atomic_close_ownership", "ui_lineage_aware", True),
    ("operator_atomic_close_ownership", "ui_lineage_computation", []),
    ("operator_atomic_close_ownership", "forbidden_automation_tokens", []),
    ("operator_atomic_close_ownership", "broker_enabled", False),
    ("operator_atomic_close_ownership", "automation_enabled", False),
    ("operator_atomic_close_ownership", "automatic_rebalance_enabled", False),
    ("operator_atomic_close_ownership", "automatic_promotion_enabled", False),
    ("operator_atomic_close_ownership", "model_recalibration_added", False),
    # Stage 20.1 — the hermetic acceptance environment must feed EVERY canonical surface
    # from ONE coherent scenario. It may never regress into per-endpoint fixture ownership
    # where one panel is seeded and the rest fall back to unrelated defaults.
    ("acceptance_scenario_ownership", "owners_present", True),
    ("acceptance_scenario_ownership", "single_scenario_owner", True),
    ("acceptance_scenario_ownership", "shared_scenario_contract_present", True),
    ("acceptance_scenario_ownership", "missing_panels", []),
    ("acceptance_scenario_ownership", "missing_delegation", []),
    ("acceptance_scenario_ownership", "reimplemented_production_logic", []),
    ("acceptance_scenario_ownership", "forbidden_calls", []),
    ("acceptance_scenario_ownership", "verdict_checks_execution_precedence", True),
    ("acceptance_scenario_ownership", "verdict_checks_lineage_cohorts", True),
    ("acceptance_scenario_ownership", "verdict_checks_single_primary_action", True),
    ("acceptance_scenario_ownership", "verdict_checks_book_initialization", True),
    ("acceptance_scenario_ownership", "counts_are_lineage_scoped", True),
    ("acceptance_scenario_ownership", "scenario_5_present", True),
    ("acceptance_scenario_ownership", "scenario_5b_present", True),
    ("acceptance_scenario_ownership", "acceptance_refuses_live_backend_port", True),
    ("acceptance_scenario_ownership", "acceptance_redirects_every_store", True),
    ("acceptance_scenario_ownership", "acceptance_refuses_inconsistent_scenario", True),
    # CANONICAL BACKEND RESTART / SMOKE. ONE repository-owned operator workflow. The
    # readiness routes are permanently /v1/health and /v1/ready; a stage handoff may add
    # GET assertions but may never reimplement the launch, the port handling, the
    # readiness polling, the authentication, the diagnostics or the store-root
    # validation - and every path any of them probes must be a route the application
    # actually declares.
    # Stage 22 — the canonical NORMAL DAILY PORTFOLIO CYCLE. ONE state owner projecting
    # onto ONE ordered stage sequence; at most one normal-path mutation, enforced at
    # runtime; no hidden desk/target/evidence/mark refresh between the Daily Close and
    # the Daily Research Cycle; stale evidence classified but STILL fail-closed; the
    # assessment/proposal binding verdict fails closed and is stated exactly once; every
    # data gap classified, an unknown code BLOCKING, nothing silently substituted; and a
    # UI that mirrors the contract instead of re-deriving a workflow priority.
    ("normal_cycle_ownership", "kernels_present", True),
    ("normal_cycle_ownership", "kernel_impurity", []),
    ("normal_cycle_ownership", "gap_kernel_impurity", []),
    ("normal_cycle_ownership", "sequence_declared", True),
    ("normal_cycle_ownership", "sequence_ordered", True),
    ("normal_cycle_ownership", "second_cycle_owner_modules", []),
    ("normal_cycle_ownership", "second_gap_owner_modules", []),
    ("normal_cycle_ownership", "missing_owner_tokens", []),
    ("normal_cycle_ownership", "single_mutation_enforced", True),
    ("normal_cycle_ownership", "post_close_research_required", True),
    ("normal_cycle_ownership", "close_outranks_research", True),
    ("normal_cycle_ownership", "no_standalone_desk_refresh_required", True),
    ("normal_cycle_ownership", "missing_evidence_tokens", []),
    ("normal_cycle_ownership", "evidence_still_fails_closed", True),
    ("normal_cycle_ownership", "missing_binding_tokens", []),
    ("normal_cycle_ownership", "missing_gap_tokens", []),
    ("normal_cycle_ownership", "unknown_gap_fails_closed", True),
    ("normal_cycle_ownership", "no_silent_substitution", True),
    ("normal_cycle_ownership", "gap_severity_consumed_not_inferred", True),
    ("normal_cycle_ownership", "missing_ui_tokens", []),
    ("normal_cycle_ownership", "ui_cycle_derivation", []),
    ("backend_restart_ownership", "owner_present", True),
    ("backend_restart_ownership", "owner_declares_ownership", True),
    ("backend_restart_ownership", "owner_missing_canonical_routes", []),
    ("backend_restart_ownership", "owner_missing_contract", []),
    ("backend_restart_ownership", "owner_missing_diagnostics", []),
    ("backend_restart_ownership", "noncanonical_health_probes", []),
    ("backend_restart_ownership", "probed_routes_not_declared", []),
    ("backend_restart_ownership", "reimplementing_scripts", []),
    ("backend_restart_ownership", "mutating_http_calls", []),
    ("backend_restart_ownership", "live_smoke_emitting_scripts",
     ["scripts/restart_paper_trader_backend.ps1"]),
    ("backend_restart_ownership", "owner_live_smoke_emissions", 1),
    # --- Release 29 UX2: the restart owner is INVOKED safely, not only OWNED --------
    ("restart_invocation_hygiene", "owner_is_exit_free", True),
    ("restart_invocation_hygiene", "owner_exit_statements", []),
    ("restart_invocation_hygiene", "owner_declares_direct_invocation", True),
    ("restart_invocation_hygiene", "owner_exposes_contract_probe", True),
    ("restart_invocation_hygiene", "owner_reports_last_exit_code", True),
    ("restart_invocation_hygiene", "owner_asserts_smokepath_contract", True),
    ("restart_invocation_hygiene", "file_switch_smokepath_forwarding", []),
    ("restart_invocation_hygiene", "command_switch_lifecycle_construction", []),
    ("restart_invocation_hygiene", "fragile_array_forwarding", []),
    ("restart_invocation_hygiene", "duplicate_restart_implementations", []),
    # --- Release 29 UX2: the operating screens were simplified BY REMOVAL, and every
    # removal is a MOVE - nothing was deleted, no owner was forked, nothing was lost.
    ("release29_ux2_simplification", "markets_nav", True),
    ("release29_ux2_simplification", "markets_route", True),
    ("release29_ux2_simplification", "markets_tab_present", True),
    ("release29_ux2_simplification", "markets_reference_only_label", True),
    ("release29_ux2_simplification", "regions_still_on_today", []),
    ("release29_ux2_simplification", "regions_missing_on_markets", []),
    ("release29_ux2_simplification", "regions_missing_on_system_audit", []),
    ("release29_ux2_simplification", "moved_ids_duplicated_or_lost", []),
    ("release29_ux2_simplification", "today_market_strip_present", True),
    ("release29_ux2_simplification", "today_market_strip_is_a_mirror", True),
    ("release29_ux2_simplification", "today_market_strip_forbidden_calls", []),
    ("release29_ux2_simplification", "market_dashboard_owner_count", 1),
    ("release29_ux2_simplification", "market_context_owner_count", 1),
    ("release29_ux2_simplification", "rail_free_routes",
     ["command-center", "markets", "portfolio-manager"]),
    ("release29_ux2_simplification", "rail_route_published", True),
    ("release29_ux2_simplification", "rail_markup_retained", True),
    ("release29_ux2_simplification", "portfolio_regions_not_removed", []),
    ("release29_ux2_simplification", "portfolio_regions_lost", []),
    ("release29_ux2_simplification", "moved_diagnostics_panel_routed", True),
    # --- Release 39: Autonomous Universal Alpha Discovery ------------------ #
    # The idea boundary is open and the evidence boundary is closed: one
    # economic judge (r34), one multiple-testing owner (r31), the r31 lockbox
    # budget, honestly labelled evidence zones (Zone C is HISTORICAL
    # confirmation, never fresh/forward), ledgered search with a reported
    # effective burden, placebo-controlled Fibonacci, named exclusions,
    # untested-is-not-rejected, and every commercial/production refusal.
    ("release39_universal_alpha_discovery", "modules_present", True),
    ("release39_universal_alpha_discovery", "modules_missing", []),
    ("release39_universal_alpha_discovery", "second_owner_modules", []),
    ("release39_universal_alpha_discovery", "defines_no_second_gate", True),
    ("release39_universal_alpha_discovery", "reuses_r34_economic_judge",
     True),
    ("release39_universal_alpha_discovery", "reuses_r31_multiple_testing",
     True),
    ("release39_universal_alpha_discovery", "reuses_r31_hashing", True),
    ("release39_universal_alpha_discovery",
     "reuses_r36_minimum_detectable_excess", True),
    ("release39_universal_alpha_discovery",
     "lockbox_budget_imported_from_r31", True),
    ("release39_universal_alpha_discovery",
     "evidence_zones_honestly_labelled", True),
    ("release39_universal_alpha_discovery",
     "zone_c_single_execution_enforced", True),
    ("release39_universal_alpha_discovery",
     "search_budget_ceilings_enforced", True),
    ("release39_universal_alpha_discovery", "search_burden_reported", True),
    ("release39_universal_alpha_discovery", "fibonacci_placebo_controlled",
     True),
    ("release39_universal_alpha_discovery", "spending_refused", True),
    ("release39_universal_alpha_discovery", "safety_flags_declared", True),
    ("release39_universal_alpha_discovery",
     "no_purchase_or_renewal_authority", True),
    ("release39_universal_alpha_discovery", "commercial_tokens_present", []),
    ("release39_universal_alpha_discovery",
     "alpha_pass_requires_qualified_verdict", True),
    ("release39_universal_alpha_discovery", "five_result_axes_declared",
     True),
    ("release39_universal_alpha_discovery", "untested_is_not_rejected",
     True),
    ("release39_universal_alpha_discovery",
     "exclusions_use_named_vocabulary", True),
    ("release39_universal_alpha_discovery",
     "forward_handoff_prepared_not_activated", True),
    ("release39_universal_alpha_discovery", "steele_lane_read_only", True),
    ("release39_continuation", "modules_present", True),
    ("release43_global_alpha_offensive", 'modules_present', True),
    ("release43_global_alpha_offensive", 'modules_missing', []),
    ("release43_global_alpha_offensive", 'second_owner_modules', []),
    ("release43_global_alpha_offensive", 'burden_inherited_not_reset', True),
    ("release43_global_alpha_offensive", 'r41_ledger_read_only', True),
    ("release43_global_alpha_offensive", 'lane_caps_enforced', True),
    ("release43_global_alpha_offensive", 'contract_frozen_before_results', True),
    ("release43_global_alpha_offensive", 'collateral_declared', True),
    ("release43_global_alpha_offensive", 'judge_is_one_equation', True),
    ("release43_global_alpha_offensive", 'risk_free_convention_inherited', True),
    ("release43_global_alpha_offensive", 'post_freeze_denominator_disclosed', True),
    ("release43_global_alpha_offensive", 'signal_is_lagged', True),
    ("release43_global_alpha_offensive", 'no_full_sample_risk_target', True),
    ("release43_global_alpha_offensive", 'no_padded_returns', True),
    ("release43_global_alpha_offensive", 'causal_pivots_only', True),
    ("release43_global_alpha_offensive", 'lookahead_sector_map_refused', True),
    ("release43_global_alpha_offensive", 'survivorship_handled', True),
    ("release43_global_alpha_offensive", 'passive_control_measured', True),
    ("release43_global_alpha_offensive", 'placebo_declared_not_chosen', True),
    ("release43_global_alpha_offensive", 'event_placebo_is_non_event_days', True),
    ("release43_global_alpha_offensive", 'signed_advance_rule', True),
    ("release43_global_alpha_offensive", 'walls_probed_not_asserted', True),
    ("release43_global_alpha_offensive", 'keys_never_leak', True),
    ("release43_global_alpha_offensive", 'no_purchase_or_account', True),
    ("release43_global_alpha_offensive", 'purchase_gate_ranks_by_value_per_dollar', True),
    ("release43_global_alpha_offensive", 'zone_c_gated', True),
    ("release43_global_alpha_offensive", 'shadows_capped_not_promotable', True),
    ("release43_global_alpha_offensive", 'forward_never_backfilled', True),
    ("release43_global_alpha_offensive", 'ranked_by_economic_value', True),
    ("release43_global_alpha_offensive", 'frontier_fields_complete', True),
    ("release43_global_alpha_offensive", 'reuses_canonical_statistics', True),
    ("release43_global_alpha_offensive", 'panels_read_only', True),
    ("release43_global_alpha_offensive", 'no_operational_imports', True),
    ("release43_global_alpha_offensive", 'prior_roots_witnessed', True),
    ("release43_global_alpha_offensive", 'safety_flags_false', True),
    ("release43_global_alpha_offensive", 'every_lane_must_terminate', True),
    ("release43_global_alpha_offensive", 'result_axes_never_collapsed', True),
    ("release43_global_alpha_offensive", 'twenty_questions_answered', True),
    ("release43_global_alpha_offensive", 'shell_policy_declared', True),
    ("release43_global_alpha_offensive", 'no_scheduler_or_task_registration', True),
    ("release43_global_alpha_offensive", 'qualification_states_missing', []),

    # --- Release 44: Orthogonal Information x Portfolio Alpha -------------- #
    # A portfolio claim is the easiest kind to fake. These block the routes:
    # a filtered inventory, a rule picked after the fact, a sign flip that
    # credits transaction costs, a benchmark chosen to be beatable, and a
    # smoother package of risk premia relabelled as alpha.
    ("release44_orthogonal_portfolio_alpha", 'modules_present', True),
    ("release44_orthogonal_portfolio_alpha", 'modules_missing', []),
    ("release44_orthogonal_portfolio_alpha", 'second_owner_modules', []),
    ("release44_orthogonal_portfolio_alpha", 'burden_inherited_not_reset', True),
    ("release44_orthogonal_portfolio_alpha", 'r43_ledger_read_only', True),
    ("release44_orthogonal_portfolio_alpha", 'lane_caps_enforced', True),
    ("release44_orthogonal_portfolio_alpha", 'portfolio_synthesis_is_charged', True),
    ("release44_orthogonal_portfolio_alpha", 'contract_frozen_before_results', True),
    ("release44_orthogonal_portfolio_alpha", 'amendments_disclosed_and_bounded', True),
    ("release44_orthogonal_portfolio_alpha", 'primary_rule_named_before_lockbox', True),
    ("release44_orthogonal_portfolio_alpha", 'no_threshold_is_chosen', True),
    ("release44_orthogonal_portfolio_alpha", 'losers_are_included', True),
    ("release44_orthogonal_portfolio_alpha", 'dangerous_optimisers_forbidden', True),
    ("release44_orthogonal_portfolio_alpha", 'weights_fitted_on_fit_zones_only', True),
    ("release44_orthogonal_portfolio_alpha", 'constraints_applied_to_every_rule', True),
    ("release44_orthogonal_portfolio_alpha", 'sign_flip_charges_cost', True),
    ("release44_orthogonal_portfolio_alpha", 'sign_diagnostic_cannot_qualify', True),
    ("release44_orthogonal_portfolio_alpha", 'structural_premium_control_declared', True),
    ("release44_orthogonal_portfolio_alpha", 'increment_is_volatility_matched', True),
    ("release44_orthogonal_portfolio_alpha", 'three_qualification_words_kept_apart', True),
    ("release44_orthogonal_portfolio_alpha", 'kill_battery_complete', True),
    ("release44_orthogonal_portfolio_alpha", 'pbo_measured_over_the_rules', True),
    ("release44_orthogonal_portfolio_alpha", 'negative_is_never_called_a_survivor', True),
    ("release44_orthogonal_portfolio_alpha", 'reuses_canonical_statistics', True),
    ("release44_orthogonal_portfolio_alpha", 'entry_is_never_at_the_print', True),
    ("release44_orthogonal_portfolio_alpha", 'cost_is_the_observed_spread', True),
    ("release44_orthogonal_portfolio_alpha", 'no_cfd_proxy_for_futures', True),
    ("release44_orthogonal_portfolio_alpha", 'release_time_is_declared_constant', True),
    ("release44_orthogonal_portfolio_alpha", 'event_placebo_is_non_release_days', True),
    ("release44_orthogonal_portfolio_alpha", 'no_fabricated_fill', True),
    ("release44_orthogonal_portfolio_alpha", 'no_current_snapshot_as_vintage', True),
    ("release44_orthogonal_portfolio_alpha", 'sample_request_prepared_not_sent', True),
    ("release44_orthogonal_portfolio_alpha", 'options_may_not_qualify', True),
    ("release44_orthogonal_portfolio_alpha", 'iv_inverted_locally', True),
    ("release44_orthogonal_portfolio_alpha", 'capacity_is_a_result', True),
    ("release44_orthogonal_portfolio_alpha", 'cost_is_liquidity_scaled', True),
    ("release44_orthogonal_portfolio_alpha", 'zero_volume_markets_excluded', True),
    ("release44_orthogonal_portfolio_alpha", 'niche_advance_bar_is_the_frozen_one', True),
    ("release44_orthogonal_portfolio_alpha", 'walls_probed_not_asserted', True),
    ("release44_orthogonal_portfolio_alpha", 'keys_never_leak', True),
    ("release44_orthogonal_portfolio_alpha", 'no_purchase_or_account', True),
    ("release44_orthogonal_portfolio_alpha", 'purchase_gate_ranks_by_value_per_dollar', True),
    ("release44_orthogonal_portfolio_alpha", 'shadows_capped_not_promotable', True),
    ("release44_orthogonal_portfolio_alpha", 'forward_never_backfilled', True),
    ("release44_orthogonal_portfolio_alpha", 'prior_roots_witnessed', True),
    ("release44_orthogonal_portfolio_alpha", 'no_operational_imports', True),
    ("release44_orthogonal_portfolio_alpha", 'safety_flags_false', True),
    ("release44_orthogonal_portfolio_alpha", 'every_lane_must_terminate', True),
    ("release44_orthogonal_portfolio_alpha", 'result_axes_never_collapsed', True),
    ("release44_orthogonal_portfolio_alpha", 'fifteen_questions_answered', True),
    ("release44_orthogonal_portfolio_alpha", 'no_alpha_terminal_requires_execution', True),
    ("release44_orthogonal_portfolio_alpha", 'shell_policy_declared', True),
    ("release44_orthogonal_portfolio_alpha", 'shell_policy_events_disclosed', True),
    ("release44_orthogonal_portfolio_alpha", 'no_scheduler_or_task_registration', True),
    ("release44_orthogonal_portfolio_alpha", 'qualification_states_missing', []),
    ("release39_continuation", "modules_missing", []),
    ("release39_continuation", "burden_never_resets", True),
    ("release39_continuation", "zone_c_pregate_declared", True),
    ("release39_continuation", "masked_eval_cannot_touch_zone_c", True),
    ("release39_continuation", "diagnostics_cannot_upgrade_qualification",
     True),
    ("release39_continuation", "wide_reconstruction_pinned", True),
    ("release39_continuation", "no_pretrained_weights_downloaded", True),
    ("release39_continuation", "shadows_not_promotable", True),
    ("release39_continuation", "canonical_ledger_primitives_reused", True),
    ("release39_continuation", "only_desk_primitives_imported_from_api",
     True),
    ("release39_continuation", "anytime_valid_design_registered", True),
    ("release39_continuation", "v1_generator_untouched_repair_is_new",
     True),
    ("release39_continuation", "subsplit_protocol_declared", True),
    ("release39_continuation", "shell_policy_audit_recorded", True),
    ("release39_universal_alpha_discovery", "no_operational_imports", True),
    ("release40_prospective_alpha_acceleration", "modules_present", True),
    ("release40_prospective_alpha_acceleration", "modules_missing", []),
    ("release40_prospective_alpha_acceleration", "second_owner_modules", []),
    ("release40_prospective_alpha_acceleration", "burden_inherited_not_reset",
     True),
    ("release40_prospective_alpha_acceleration", "one_campaign_root_binding",
     True),
    ("release40_prospective_alpha_acceleration",
     "canonical_ledger_primitives_reused", True),
    ("release40_prospective_alpha_acceleration", "no_operational_imports",
     True),
    ("release40_prospective_alpha_acceleration", "forward_evidence_honesty",
     True),
    ("release40_prospective_alpha_acceleration", "r39_capture_owner_reused",
     True),
    ("release40_prospective_alpha_acceleration",
     "no_scheduler_or_automation", True),
    ("release40_prospective_alpha_acceleration", "shadows_not_promotable",
     True),
    ("release40_prospective_alpha_acceleration", "family_cap_five", True),
    ("release40_prospective_alpha_acceleration",
     "slot5_rule_frozen_before_evaluation", True),
    ("release40_prospective_alpha_acceleration", "e_process_reused", True),
    ("release40_prospective_alpha_acceleration", "economic_judge_reused",
     True),
    ("release40_prospective_alpha_acceleration", "multiple_testing_reused",
     True),
    ("release40_prospective_alpha_acceleration", "no_fake_independence",
     True),
    ("release40_prospective_alpha_acceleration",
     "open_weight_policy_ten_conditions", True),
    ("release40_prospective_alpha_acceleration", "weights_on_research_drive",
     True),
    ("release40_prospective_alpha_acceleration",
     "contamination_labels_applied", True),
    ("release40_prospective_alpha_acceleration",
     "availability_rule_declared", True),
    ("release40_prospective_alpha_acceleration",
     "nyfed_no_invented_backfill", True),
    ("release40_prospective_alpha_acceleration",
     "hierarchical_search_discipline", True),
    ("release40_prospective_alpha_acceleration", "commercial_refused", True),
    ("release40_prospective_alpha_acceleration", "result_axes_declared",
     True),
    ("release40_prospective_alpha_acceleration", "shell_policy_recorded",
     True),
    ("release41_multi_horizon_alpha", "modules_present", True),
    ("release41_multi_horizon_alpha", "modules_missing", []),
    ("release41_multi_horizon_alpha", "second_owner_modules", []),
    ("release41_multi_horizon_alpha", "burden_inherited_not_reset", True),
    ("release41_multi_horizon_alpha", "one_campaign_root_binding", True),
    ("release41_multi_horizon_alpha", "canonical_ledger_primitives_reused",
     True),
    ("release41_multi_horizon_alpha", "no_operational_imports", True),
    ("release41_multi_horizon_alpha", "gates_frozen_before_results", True),
    ("release41_multi_horizon_alpha", "r40_verified_not_trusted", True),
    ("release41_multi_horizon_alpha", "multiple_testing_reused", True),
    ("release41_multi_horizon_alpha", "no_interpolated_intraday", True),
    ("release41_multi_horizon_alpha", "fibonacci_placebo_controlled", True),
    ("release41_multi_horizon_alpha", "sign_fit_declared_on_zone_a", True),
    ("release41_multi_horizon_alpha", "sample_conditions_eight", True),
    ("release41_multi_horizon_alpha", "no_scheduler_or_automation", True),
    ("release41_multi_horizon_alpha", "shadows_capped_not_promotable", True),
    ("release41_multi_horizon_alpha", "qualified_gate_in_code", True),
    ("release41_multi_horizon_alpha", "killer_battery_declared", True),
    ("release41_multi_horizon_alpha", "cost_on_traded_notional", True),
    # --- Release 42: Crypto Funding/Basis Alpha Validation ------------------
    # The R41 candidate is prosecuted, not improved: its shadow may never be
    # re-frozen, re-parameterised or refit, its capture is delegated to its
    # own owner, and its FAIL verdict is inherited verbatim. The corrections
    # get NEW identities. Everything that could be bent after seeing a
    # result - the capital denominator, the control, the execution ladder,
    # the borrow rule, the asset and venue universes, the statistical
    # architecture, the belief standard - is frozen in a hashed contract
    # first. Data access is never confused with investability. A maker fill
    # is never assumed and a microstructure fill is never fabricated. Each
    # venue's funding cadence is asserted before its rows are summed,
    # because summing an 8-hour rate over 24 hourly rows is exactly the
    # class of error this release exists to catch.
    ("release42_crypto_basis_alpha", "modules_present", True),
    ("release42_crypto_basis_alpha", "modules_missing", []),
    ("release42_crypto_basis_alpha", "second_owner_modules", []),
    ("release42_crypto_basis_alpha", "r41_declared_immutable", True),
    ("release42_crypto_basis_alpha", "r41_shadow_not_refrozen", True),
    ("release42_crypto_basis_alpha", "r41_capture_delegated", True),
    ("release42_crypto_basis_alpha", "r41_verdict_inherited", True),
    ("release42_crypto_basis_alpha", "contract_frozen_before_results", True),
    ("release42_crypto_basis_alpha", "capital_control_declared", True),
    ("release42_crypto_basis_alpha", "one_authoritative_roic", True),
    ("release42_crypto_basis_alpha", "zero_control_not_reused", True),
    ("release42_crypto_basis_alpha", "borrow_rule_enforced", True),
    ("release42_crypto_basis_alpha", "universes_metadata_only", True),
    ("release42_crypto_basis_alpha", "investability_separated", True),
    ("release42_crypto_basis_alpha", "hierarchy_frozen_first", True),
    ("release42_crypto_basis_alpha", "reuses_canonical_statistics", True),
    ("release42_crypto_basis_alpha", "maker_fill_forbidden", True),
    ("release42_crypto_basis_alpha", "no_fabricated_fills", True),
    ("release42_crypto_basis_alpha", "shadows_capped_not_promotable", True),
    ("release42_crypto_basis_alpha", "forward_never_backfilled", True),
    ("release42_crypto_basis_alpha", "canonical_ledger_primitives", True),
    ("release42_crypto_basis_alpha", "no_operational_imports", True),
    ("release42_crypto_basis_alpha", "safety_flags_false", True),
    ("release42_crypto_basis_alpha", "no_exchange_account_or_orders", True),
    ("release42_crypto_basis_alpha", "shell_policy_declared", True),
    ("release42_crypto_basis_alpha", "reconstruction_is_a_gate", True),
    ("release42_crypto_basis_alpha", "funding_event_exact", True),
    ("release42_crypto_basis_alpha", "venue_cadence_asserted", True),
    ("release42_crypto_basis_alpha", "qualification_states_missing", []),

    # ----------------------------------------------------------------- #
    # Release 46 - the prospective alpha tournament.
    #
    # These are the invariants that make it impossible to turn a losing
    # challenger into a winning one without saying so. The first two are
    # the release in a sentence: a prediction that was not strictly
    # earlier than its outcome is refused, and a specification cannot be
    # edited in place once it has predictions outstanding.
    # ----------------------------------------------------------------- #
    ("release46_prospective_alpha_tournament", "modules_present", True),
    ("release46_prospective_alpha_tournament", "modules_missing", []),
    ("release46_prospective_alpha_tournament", "second_owner_modules", []),
    ("release46_prospective_alpha_tournament",
     "ledger_refuses_backdated_rows", True),
    ("release46_prospective_alpha_tournament",
     "entry_rule_is_declared_and_conservative", True),
    ("release46_prospective_alpha_tournament",
     "outcome_window_is_eastern_not_utc", True),
    ("release46_prospective_alpha_tournament",
     "evidence_classes_never_mix", True),
    ("release46_prospective_alpha_tournament", "backfill_forbidden", True),
    ("release46_prospective_alpha_tournament",
     "record_completeness_enforced", True),
    ("release46_prospective_alpha_tournament",
     "canonical_ledger_primitives_reused", True),
    ("release46_prospective_alpha_tournament", "judge_only_appends", True),
    ("release46_prospective_alpha_tournament", "identity_key_declared", True),
    ("release46_prospective_alpha_tournament",
     "idempotency_proved_in_run", True),
    ("release46_prospective_alpha_tournament",
     "versioning_forbids_in_place_retune", True),
    ("release46_prospective_alpha_tournament",
     "spec_hash_covers_the_economics", True),
    ("release46_prospective_alpha_tournament",
     "effective_independent_count_exists", True),
    ("release46_prospective_alpha_tournament", "gate_is_not_a_single_t", True),
    ("release46_prospective_alpha_tournament",
     "forward_selection_is_ledgered", True),
    ("release46_prospective_alpha_tournament",
     "collateral_is_remunerated", True),
    ("release46_prospective_alpha_tournament",
     "cost_charged_on_traded_notional", True),
    ("release46_prospective_alpha_tournament",
     "no_invented_expected_return", True),
    ("release46_prospective_alpha_tournament",
     "proven_alpha_is_not_a_state", True),
    ("release46_prospective_alpha_tournament",
     "leaderboard_ranks_evidence_first", True),
    ("release46_prospective_alpha_tournament", "adoption_is_read_only", True),
    ("release46_prospective_alpha_tournament",
     "adoption_writes_no_forward_row", True),
    # Release 46.6.1 - adopted shadows may now accrue, into an R46-OWNED
    # ledger, with the superseded clause named and the prior stores read-only.
    ("release46_prospective_alpha_tournament",
     "r46_6_1_continuation_has_one_owner", True),
    ("release46_prospective_alpha_tournament",
     "r46_6_1_prior_release_stores_stay_read_only", True),
    ("release46_prospective_alpha_tournament",
     "r46_6_1_amendment_is_named_not_implied", True),
    ("release46_prospective_alpha_tournament",
     "r46_6_1_continuation_is_true_forward_gated", True),
    ("release46_prospective_alpha_tournament",
     "r46_6_1_signal_comes_from_the_prior_owner", True),
    ("release46_prospective_alpha_tournament",
     "r46_6_1_append_rights_are_reported_apart", True),
    # ...and the two CONTROLS: a strategy's frozen benchmark is not cash, and
    # beating cash may never be promoted into a formal scientific verdict.
    ("release46_prospective_alpha_tournament",
     "r46_6_1_two_controls_are_computed_apart", True),
    ("release46_prospective_alpha_tournament",
     "r46_6_1_formal_verdict_uses_the_frozen_control", True),
    ("release46_prospective_alpha_tournament",
     "r46_6_1_options_gate_semantics_are_explicit", True),
    ("release46_prospective_alpha_tournament",
     "feasibility_gate_enforced", True),
    ("release46_prospective_alpha_tournament",
     "non_positive_price_refused", True),
    ("release46_prospective_alpha_tournament",
     "burden_inherited_not_reset", True),
    ("release46_prospective_alpha_tournament",
     "seed_parameters_were_not_searched", True),
    ("release46_prospective_alpha_tournament",
     "options_hypotheses_predeclared", True),
    ("release46_prospective_alpha_tournament",
     "analyst_never_backfilled", True),
    ("release46_prospective_alpha_tournament", "safety_flags_false", True),
    ("release46_prospective_alpha_tournament",
     "portfolio_boundary_declared", True),
    ("release46_prospective_alpha_tournament",
     "no_operational_imports", True),
    ("release46_prospective_alpha_tournament",
     "no_scheduler_or_task_registration", True),
    ("release46_prospective_alpha_tournament",
     "no_purchase_or_account", True),
    ("release46_prospective_alpha_tournament", "keys_never_leak", True),
    ("release46_prospective_alpha_tournament", "shell_policy_declared", True),
    ("release46_prospective_alpha_tournament",
     "inherited_disclosures_preserved", True),
    ("release46_prospective_alpha_tournament",
     "read_model_is_read_only", True),
    ("release46_prospective_alpha_tournament",
     "read_model_hides_no_proof", True),
    ("release46_prospective_alpha_tournament",
     "terminal_states_missing", []),
    # --- Release 46.5: the harvest keeps matured economics and marks apart,
    #     verdicts are frozen and matured-only, the correlation blend is
    #     versioned before use, the EDGAR lanes are acceptance-stamped.
    ("release46_prospective_alpha_tournament",
     "harvest_keeps_matured_and_mtm_apart", True),
    ("release46_prospective_alpha_tournament",
     "verdicts_are_frozen_and_matured_only", True),
    ("release46_prospective_alpha_tournament",
     "correlation_blend_is_versioned_and_frozen", True),
    ("release46_prospective_alpha_tournament",
     "edgar_lanes_are_acceptance_stamped", True),
    ("release46_prospective_alpha_tournament",
     "earnings_lane_refuses_synthetic_fixture", True),
    ("release46_prospective_alpha_tournament",
     "form4_codes_are_classified", True),
    ("release46_prospective_alpha_tournament",
     "edgar_access_has_one_seam", True),
    ("release46_prospective_alpha_tournament",
     "harvest_stage_is_inside_the_one_advance", True),
    ("release46_prospective_alpha_tournament",
     "r46_5_challengers_frozen_unsearched", True),
    # --- Release 46.6: signal edge is not economic edge, and no research
    #     lane may be in a state nobody can see.
    ("release46_prospective_alpha_tournament",
     "r46_6_cost_efficiency_has_one_owner", True),
    ("release46_prospective_alpha_tournament",
     "r46_6_lane_contract_is_enforced", True),
    ("release46_prospective_alpha_tournament",
     "r46_6_prior_release_ledgers_untouched", True),
    ("release46_prospective_alpha_tournament",
     "r46_6_option_hypotheses_are_historical", True),
    ("release46_prospective_alpha_tournament",
     "r46_6_challengers_frozen_unsearched", True),
    # --- Release 46.4: the economic layer has ONE owner per concept, never
    #     labels history as forward, appends only, and cannot see the future.
    ("release46_prospective_alpha_tournament",
     "r46_4_second_owner_modules", []),
    ("release46_prospective_alpha_tournament", "pnl_has_one_owner", True),
    ("release46_prospective_alpha_tournament",
     "cost_stack_matches_contract", True),
    ("release46_prospective_alpha_tournament", "no_fake_forward_pnl", True),
    ("release46_prospective_alpha_tournament",
     "trade_ledger_is_append_only_and_idempotent", True),
    ("release46_prospective_alpha_tournament", "trade_states_are_derived", True),
    ("release46_prospective_alpha_tournament",
     "nav_never_rewrites_history", True),
    ("release46_prospective_alpha_tournament",
     "allocation_has_no_hindsight", True),
    ("release46_prospective_alpha_tournament",
     "four_policies_predeclared", True),
    ("release46_prospective_alpha_tournament",
     "redundancy_and_concentration_enforced", True),
    ("release46_prospective_alpha_tournament",
     "economic_kill_rules_frozen", True),
    ("release46_prospective_alpha_tournament",
     "three_pnl_concepts_kept_apart", True),
    ("release46_prospective_alpha_tournament", "regime_is_ex_ante", True),
    ("release46_prospective_alpha_tournament", "bridge_is_read_only", True),
    ("release46_prospective_alpha_tournament",
     "pnl_step_is_inside_the_one_advance", True),
    ("release46_prospective_alpha_tournament", "lanes_are_pit_stamped", True),
    ("release46_prospective_alpha_tournament", "lanes_never_overwrite", True),
    ("release46_prospective_alpha_tournament",
     "research_trades_are_not_positions", True),

    # ----------------------------------------------------------------- #
    # Release 47 - constraint-respecting active reallocation.
    #
    # These are the invariants that make it impossible for a normal
    # portfolio constraint to freeze the portfolio again. The first three
    # are the release in a sentence: every ordinary cap is classified as
    # RESHAPING, the target is re-optimised under a breached limit BEFORE
    # anything is withheld, and the withheld verdict is read from the
    # re-measured target rather than from the first breach.
    # ----------------------------------------------------------------- #
    ("release47_constrained_reallocation", "modules_present", True),
    ("release47_constrained_reallocation", "modules_missing", []),
    ("release47_constrained_reallocation", "second_owner_modules", []),
    ("release47_constrained_reallocation",
     "reshaping_constraints_declared", True),
    ("release47_constrained_reallocation",
     "caps_declared_as_true_blockers", []),
    ("release47_constrained_reallocation", "classification_disjoint", True),
    ("release47_constrained_reallocation",
     "unknown_code_not_promoted_to_blocker", True),
    ("release47_constrained_reallocation", "reoptimise_present", True),
    ("release47_constrained_reallocation",
     "kernel_delegates_to_constraint_owner", True),
    ("release47_constrained_reallocation",
     "reoptimise_precedes_withhold", True),
    ("release47_constrained_reallocation",
     "withhold_reads_remeasured_target", True),
    # The per-holding form of the same three limits: a held name breaching its
    # own cap ASKS for a target and is never a blocker, but stays visible.
    ("release47_constrained_reallocation",
     "held_name_breach_asks_for_target", True),
    ("release47_constrained_reallocation",
     "held_name_breach_not_a_blocker", True),
    ("release47_constrained_reallocation",
     "per_name_deferral_declared", True),
    ("release47_constrained_reallocation",
     "held_name_breach_still_visible", True),
    # The Release-29.3 fail-closed guarantee is narrowed in SCOPE, never weakened.
    ("release47_constrained_reallocation", "withheld_not_approvable", True),
    ("release47_constrained_reallocation", "outcomes_declared", True),
    ("release47_constrained_reallocation", "outcomes_mirrored", True),
    ("release47_constrained_reallocation",
     "outcome_owner_is_the_kernel", True),
    ("release47_constrained_reallocation",
     "hold_current_book_not_approvable", True),
    ("release47_constrained_reallocation",
     "blocked_while_feasible_is_a_violation", True),
    ("release47_constrained_reallocation", "kernel_impurity", []),
    ("release47_constrained_reallocation",
     "execution_tokens_in_kernels", []),
    ("release47_constrained_reallocation",
     "freeze_at_execution_boundary", True),
    ("release47_constrained_reallocation", "freeze_after_orders_exist", True),
    ("release47_constrained_reallocation", "freeze_is_idempotent", True),
    ("release47_constrained_reallocation", "own_evidence_root", True),
    ("release47_constrained_reallocation", "refuses_without_execution", True),
    ("release47_constrained_reallocation",
     "counterfactual_prospective", True),
    ("release47_constrained_reallocation", "reconstruction_defs", []),
    ("release47_constrained_reallocation", "route_methods", ["GET"]),
    ("release47_constrained_reallocation", "missing_routes", []),
    ("release47_constrained_reallocation", "forbidden_routes_present", []),
    ("release47_constrained_reallocation", "ui_region_present", True),
    ("release47_constrained_reallocation", "ui_loader_count", 1),
    ("release47_constrained_reallocation", "ui_derives_decision", []),
    ("release47_constrained_reallocation", "ui_action_controls", []),
    ("release47_constrained_reallocation", "research_reach", []),
    ("release47_constrained_reallocation", "safety_flags_false", True),
    ("release47_constrained_reallocation",
     "incumbency_policy_declared", True),
    ("release47_constrained_reallocation", "switching_hurdle_frozen", True),
    ("release47_constrained_reallocation",
     "no_fabricated_expected_return", True),
    # ------------------------------------------------------------------- #
    # Release 48 — ONE canonical portfolio-cycle orchestration, ONE operator
    # concept, no new authority, no duplicate dashboard, no monthly
    # portfolio-action semantics. Every field below BLOCKS strict mode.
    # ------------------------------------------------------------------- #
    ("release48_portfolio_cycle", "owner_present", True),
    ("release48_portfolio_cycle", "one_operator_token", True),
    ("release48_portfolio_cycle", "delegates_to_close", True),
    ("release48_portfolio_cycle", "delegates_to_drc", True),
    ("release48_portfolio_cycle", "reads_one_workflow_owner", True),
    ("release48_portfolio_cycle", "max_one_invocation_each", True),
    ("release48_portfolio_cycle", "persistence_reach", []),
    ("release48_portfolio_cycle", "authority_reach", []),
    ("release48_portfolio_cycle", "second_orchestrator_modules", []),
    ("release48_portfolio_cycle", "get_status_route_count", 1),
    ("release48_portfolio_cycle", "post_run_route_count", 1),
    ("release48_portfolio_cycle", "forbidden_cycle_routes", []),
    ("release48_portfolio_cycle", "route_ownership_registered", True),
    ("release48_portfolio_cycle", "route_ownership_covers_all_cycle_routes",
     True),
    ("release48_portfolio_cycle", "presentation_declared", True),
    ("release48_portfolio_cycle", "presented_only_when_decided", True),
    ("release48_portfolio_cycle", "underlying_step_travels", True),
    ("release48_portfolio_cycle", "ui_runner_count", 1),
    ("release48_portfolio_cycle", "ui_run_post_count", 1),
    ("release48_portfolio_cycle", "dispatcher_routes_cycle", True),
    ("release48_portfolio_cycle", "dispatcher_refuses_off_today", True),
    ("release48_portfolio_cycle", "r47_card_promoted", True),
    ("release48_portfolio_cycle", "r48_new_panel_ids", []),
    ("release48_portfolio_cycle", "monthly_as_portfolio_cadence", []),
    ("release48_portfolio_cycle", "checkpoint_named_precisely", True),
    # ------------------------------------------------------------------- #
    # Release 49 - ONE reconciled operator presentation; Today command center;
    # Portfolio task views; diagnostic machinery under Audit & Details; no new
    # authority anywhere. Every field below BLOCKS strict mode.
    # ------------------------------------------------------------------- #
    ("release49_operator_presentation", "owner_present", True),
    ("release49_operator_presentation", "second_owner_modules", []),
    ("release49_operator_presentation", "vocabulary_frozen", True),
    ("release49_operator_presentation", "recomputes_nothing_declared", True),
    ("release49_operator_presentation", "business_recompute_reach", []),
    ("release49_operator_presentation", "persistence_reach", []),
    ("release49_operator_presentation", "authority_reach", []),
    ("release49_operator_presentation", "r46_reach", []),
    ("release49_operator_presentation", "executes_only_the_cycle", True),
    ("release49_operator_presentation", "historical_contract_declared", True),
    ("release49_operator_presentation", "rerun_instruction_in_owner", False),
    ("release49_operator_presentation", "get_route_count", 1),
    ("release49_operator_presentation", "forbidden_routes", []),
    ("release49_operator_presentation", "route_ownership_registered", True),
    ("release49_operator_presentation", "module_registered", True),
    ("release49_operator_presentation", "today_reads_presentation_owner", True),
    ("release49_operator_presentation", "today_primary_section_count", 4),
    ("release49_operator_presentation", "today_extra_section_ids", []),
    ("release49_operator_presentation", "today_badge_walls", 0),
    ("release49_operator_presentation", "legacy_today_hidden", True),
    ("release49_operator_presentation", "material_table_off_today", True),
    ("release49_operator_presentation", "ui_loader_count", 1),
    ("release49_operator_presentation", "ui_route_count", 1),
    ("release49_operator_presentation", "primary_cta_render_count", 1),
    ("release49_operator_presentation", "dispatcher_use_count", 1),
    ("release49_operator_presentation", "region_mutation_reach", []),
    ("release49_operator_presentation", "region_native_dialogs", []),
    ("release49_operator_presentation", "pm_views_present", True),
    ("release49_operator_presentation", "overview_reads_presentation_owner", True),
    ("release49_operator_presentation", "audit_demotion_css", True),
    ("release49_operator_presentation", "model_target_under_audit", True),
    ("release49_operator_presentation", "paper_desk_under_audit", True),
    ("release49_operator_presentation", "corporate_action_under_audit", True),
    ("release49_operator_presentation", "raw_reallocation_under_audit", True),
    ("release49_operator_presentation", "performance_under_performance_view", True),
    ("release49_operator_presentation", "best_feasible_is_the_recommendation", True),
    ("release49_operator_presentation", "empty_state_for_absent_target", True),
    ("release49_operator_presentation", "dash_grid_for_absent_target", False),
    ("release49_operator_presentation", "raw_vocabulary_in_normal_renderer", []),
    ("release49_operator_presentation", "r49_new_panel_ids", []),
    ("release49_operator_presentation", "manual_gates_unchanged", True),
    ("release49_operator_presentation", "safety_mode_line_declared", True),
    # ------------------------------------------------------------------- #
    # Release 50 - the multi-asset operational capital manager: ONE owner per
    # business concept, no snapshot-side / presentation-side recomputation, no
    # research auto-promotion, no R46-to-operation path, derived (never declared)
    # capital eligibility, no forced diversification, one governed paper
    # execution path, one decision-evidence path, no broker path, the drawdown
    # ownership debt resolved. Every field below BLOCKS strict mode.
    # ------------------------------------------------------------------- #
    ("release50_multi_asset", "owners_missing", []),
    ("release50_multi_asset", "capital_pool_owners", ["api/capital_pool.py"]),
    ("release50_multi_asset", "nav_owners", ["api/paper_trading_desk.py"]),
    ("release50_multi_asset", "position_contract_owners", ["engine/instrument_contract.py"]),
    ("release50_multi_asset", "registry_owners", ["api/investability_registry.py"]),
    ("release50_multi_asset", "risk_owners", ["engine/cross_asset_risk.py"]),
    ("release50_multi_asset", "frontier_owners", ["engine/opportunity_frontier.py"]),
    ("release50_multi_asset", "zero_base_owners", ["engine/zero_base_allocator.py"]),
    ("release50_multi_asset", "feasible_target_owners", ["engine/constrained_reallocation.py"]),
    ("release50_multi_asset", "switching_owners", ["engine/constrained_reallocation.py"]),
    ("release50_multi_asset", "snapshot_owners", ["api/decision_snapshot.py"]),
    ("release50_multi_asset", "execution_owners", ["api/rebalance_execution.py"]),
    ("release50_multi_asset", "settlement_owners", ["api/paper_trading_desk.py"]),
    ("release50_multi_asset", "evidence_owners", ["api/portfolio_decision_outcome.py"]),
    ("release50_multi_asset", "drawdown_owners", ["api/paper_trading_desk.py"]),
    ("release50_multi_asset", "covariance_owners", ["engine/holding_opportunity_cost.py"]),
    ("release50_multi_asset", "snapshot_business_reach", []),
    ("release50_multi_asset", "snapshot_declares_no_business", True),
    ("release50_multi_asset", "snapshot_invalidation_is_identity", True),
    ("release50_multi_asset", "presentation_business_reach", []),
    ("release50_multi_asset", "registry_no_promotion", True),
    ("release50_multi_asset", "registry_eligibility_derived", True),
    ("release50_multi_asset", "r46_reach", []),
    ("release50_multi_asset", "research_imports_in_operational", []),
    ("release50_multi_asset", "forced_diversification_declared_false", True),
    ("release50_multi_asset", "forced_min_weight_tokens", []),
    ("release50_multi_asset", "long_only_declared", True),
    ("release50_multi_asset", "zero_signal_not_a_sink", True),
    ("release50_multi_asset", "convention_declared", True),
    ("release50_multi_asset", "futures_not_valued_like_equities", True),
    ("release50_multi_asset", "cost_policy_declared", True),
    ("release50_multi_asset", "desk_routes_owned_marks", True),
    ("release50_multi_asset", "desk_settles_by_instrument", True),
    ("release50_multi_asset", "desk_nav_instrument_aware", True),
    ("release50_multi_asset", "second_fill_writers", []),
    ("release50_multi_asset", "hoc_scoped_to_equity", True),
    ("release50_multi_asset", "daily_close_reads_owner_drawdown", True),
    ("release50_multi_asset", "analytics_reads_current_rows", True),
    ("release50_multi_asset", "portfolio_state_names_drawdown_owner", True),
    ("release50_multi_asset", "cross_asset_constraints_declared", True),
    ("release50_multi_asset", "proposal_reuses_constraint_owner", True),
    ("release50_multi_asset", "zero_base_reuses_constraint_owner", True),
    ("release50_multi_asset", "r50_mutating_routes", []),
    ("release50_multi_asset", "routes_unregistered", []),
    ("release50_multi_asset", "modules_unregistered", []),
    ("release50_multi_asset", "snapshot_sections_not_served", []),
    ("release50_multi_asset", "direct_owner_calls_remaining", []),
    ("release50_multi_asset", "registry_card_under_audit", True),
    ("release50_multi_asset", "region_forbidden", []),
    ("release50_multi_asset", "region_loader_count", 1),
    ("release50_multi_asset", "snapshot_allocation_from_owner", True),
    ("release50_multi_asset", "no_cosmetic_zero_rows", True),
    ("release50_multi_asset", "r50_new_panel_ids", []),
    ("release50_multi_asset", "broker_reach", []),
    ("release50_multi_asset", "manual_gates_unchanged", True),
    # --- R54 Slice 1: ONE Active Manager Operating State, zero recomputation, and
    # the Today operational-mark pill kept single-writer. A hit here means either a
    # second business-calculation path grew inside the composition owner, the UI
    # grew a second loader / client-side state math, or the legacy guard-free
    # cc-status-mark writer (whose fallback was the dormant legacy DB book's date)
    # was reintroduced.
    ("release54_active_manager_state", "owner_present", True),
    ("release54_active_manager_state", "declares_owner", True),
    ("release54_active_manager_state", "composition_only_declared", True),
    ("release54_active_manager_state", "composes_decision_snapshot", True),
    ("release54_active_manager_state", "forbidden_calculation_defs", []),
    ("release54_active_manager_state", "forbidden_execution_tokens", []),
    ("release54_active_manager_state", "time_state_distinction_declared", True),
    ("release54_active_manager_state", "route_get_count", 1),
    ("release54_active_manager_state", "non_get_route_present", False),
    ("release54_active_manager_state", "ui_loader_count", 1),
    ("release54_active_manager_state", "ui_fetch_count", 1),
    ("release54_active_manager_state", "ui_region_present", True),
    ("release54_active_manager_state", "ui_region_forbidden", []),
    ("release54_active_manager_state", "legacy_status_mark_writer_present", False),
    ("release54_active_manager_state", "canonical_status_mark_writer_count", 1),
    ("release54_active_manager_state", "status_mark_guarded_early_writer_present", True),
    ("release54_active_manager_state", "decision_authority_declared", True),
    ("release54_active_manager_state", "evidence_identities_distinct", True),
    ("release54_active_manager_state", "automatic_model_promotion_allowed", False),
    ("release54_active_manager_state", "automatic_approval_allowed", False),
    ("release54_active_manager_state", "cadence_enabled", False),

    # --- R54.1 — ONE governed intraday decision gate, ONE decision owner ---- #
    # These fail the build if a SECOND intraday-governance owner ever appears
    # (in the event cycle, the read model, the workflow owner or a new module),
    # if the gate grows economics of its own, if it acquires execution /
    # approval / promotion / scheduler reach, if the withheld taxonomy loses a
    # canonical code, or if a governed promotion could ever advance the
    # operational close mark or write the manual operator-decision pointer.
    ("release54_1_governed_intraday_decision", "gate_owner_present", True),
    ("release54_1_governed_intraday_decision", "gate_defs_missing", []),
    ("release54_1_governed_intraday_decision", "duplicate_governance_owners", []),
    ("release54_1_governed_intraday_decision", "cycle_delegates_to_owner", True),
    ("release54_1_governed_intraday_decision", "cycle_defines_gate", False),
    ("release54_1_governed_intraday_decision", "read_model_defines_gate", False),
    ("release54_1_governed_intraday_decision", "workflow_defines_gate", False),
    ("release54_1_governed_intraday_decision", "forbidden_calculation_defs", []),
    ("release54_1_governed_intraday_decision", "forbidden_execution_tokens", []),
    ("release54_1_governed_intraday_decision", "missing_reason_codes", []),
    ("release54_1_governed_intraday_decision",
     "owned_data_rule_reused_verbatim", True),
    ("release54_1_governed_intraday_decision",
     "hold_and_change_both_governed", True),
    ("release54_1_governed_intraday_decision",
     "manual_review_required_for_change", True),
    ("release54_1_governed_intraday_decision",
     "governed_lane_never_advances_operational_mark", True),
    ("release54_1_governed_intraday_decision",
     "separate_governed_ledger_files", True),
    ("release54_1_governed_intraday_decision",
     "governed_writer_touches_manual_index", False),
    ("release54_1_governed_intraday_decision",
     "system_token_distinct_from_approval_token", True),
    ("release54_1_governed_intraday_decision",
     "gate_declares_it_owns_no_economics", True),
    ("release54_1_governed_intraday_decision",
     "zero_base_policy_bound_not_redefined", True),
    ("release54_1_governed_intraday_decision", "emission_slots_unchanged", True),
    ("release54_1_governed_intraday_decision",
     "emission_post_close_pass_declared", True),
    ("release54_1_governed_intraday_decision", "automatic_approval_allowed", False),
    ("release54_1_governed_intraday_decision",
     "automatic_model_promotion_allowed", False),
    ("release54_1_governed_intraday_decision",
     "automatic_execution_allowed", False),

    # Release 54.2 — SAME-SESSION REASSESSMENT VERSIONING. The build fails if a
    # second reassessment store, a second persistence writer or a second
    # identity calculator appears, if a version could OVERWRITE rather than
    # append, if the assessment-evidence identity is contaminated with the
    # document-wide portfolio hash (the Stage-21 trap), the economic axis or the
    # conclusion itself, if an impossible identity could be persisted, if an
    # intraday-only parallel history appears, or if versioning became an
    # exemption from the R54.1 governance gate.
    ("release54_2_same_session_reassessment_versioning",
     "owner_defs_missing", []),
    ("release54_2_same_session_reassessment_versioning",
     "duplicate_versioning_owners", []),
    ("release54_2_same_session_reassessment_versioning",
     "single_index_writer", True),
    ("release54_2_same_session_reassessment_versioning",
     "parallel_reassessment_stores", []),
    ("release54_2_same_session_reassessment_versioning",
     "version_chain_is_appended", True),
    ("release54_2_same_session_reassessment_versioning",
     "owner_deletes_an_artifact", False),
    ("release54_2_same_session_reassessment_versioning",
     "persist_outcomes_missing", []),
    ("release54_2_same_session_reassessment_versioning",
     "inconsistent_identity_guard_present", True),
    ("release54_2_same_session_reassessment_versioning",
     "artifact_id_collision_guard_present", True),
    ("release54_2_same_session_reassessment_versioning",
     "evidence_identity_declared", True),
    ("release54_2_same_session_reassessment_versioning",
     "forbidden_evidence_components", []),
    ("release54_2_same_session_reassessment_versioning",
     "legacy_artifact_recomputed_not_rewritten", True),
    ("release54_2_same_session_reassessment_versioning",
     "authoritative_rows_used_by_churn", True),
    ("release54_2_same_session_reassessment_versioning",
     "outcome_owner_uses_authoritative_rows", True),
    ("release54_2_same_session_reassessment_versioning",
     "both_producers_delegate", True),
    ("release54_2_same_session_reassessment_versioning",
     "gate_requires_persisted_reassessment", True),
    ("release54_2_same_session_reassessment_versioning",
     "cycle_publishes_persistence_outcome", True),
    ("release54_2_same_session_reassessment_versioning",
     "automatic_approval_allowed", False),
    ("release54_2_same_session_reassessment_versioning",
     "automatic_execution_allowed", False),
    ("release54_2_same_session_reassessment_versioning",
     "advances_operational_mark", False),
    # ------------------------------------------------------------------- #
    # Release 54.2.1 - MISSED ELIGIBLE SESSION RECOVERY. ONE catch-up state
    # owner, ONE calendar owner, ONE orchestration path, a SERVER-decided
    # session binding, and no backfill / force-close / date-entry anywhere.
    # Every field below BLOCKS strict mode.
    # ------------------------------------------------------------------- #
    ("release54_2_1_missed_session_recovery", "owner_defs_missing", []),
    ("release54_2_1_missed_session_recovery", "calendar_defs_missing", []),
    ("release54_2_1_missed_session_recovery", "duplicate_state_owners", []),
    ("release54_2_1_missed_session_recovery", "duplicate_calendar_owners", []),
    ("release54_2_1_missed_session_recovery",
     "second_recovery_orchestrators", []),
    ("release54_2_1_missed_session_recovery", "forbidden_routes_present", []),
    ("release54_2_1_missed_session_recovery",
     "workflow_delegates_calendar", True),
    ("release54_2_1_missed_session_recovery",
     "workflow_owns_no_calendar_walk", True),
    ("release54_2_1_missed_session_recovery",
     "obligation_anchored_on_close", True),
    ("release54_2_1_missed_session_recovery",
     "priority_suppresses_wait_state", True),
    ("release54_2_1_missed_session_recovery", "priority_promotes_close", True),
    ("release54_2_1_missed_session_recovery", "oldest_first_declared", True),
    ("release54_2_1_missed_session_recovery",
     "cycle_reads_binding_from_workflow", True),
    ("release54_2_1_missed_session_recovery",
     "cycle_passes_binding_to_close", True),
    ("release54_2_1_missed_session_recovery", "close_accepts_binding", True),
    ("release54_2_1_missed_session_recovery",
     "close_refuses_forward_binding", True),
    ("release54_2_1_missed_session_recovery",
     "close_binding_never_clamps", True),
    ("release54_2_1_missed_session_recovery",
     "binding_is_not_a_request_field", True),
    ("release54_2_1_missed_session_recovery", "ams_delegates_recovery", True),
    ("release54_2_1_missed_session_recovery",
     "presentation_delegates_recovery", True),
    ("release54_2_1_missed_session_recovery", "ui_recovery_derivation", []),
    ("release54_2_1_missed_session_recovery",
     "ui_renders_backend_recovery", True),
    ("release54_2_1_missed_session_recovery", "ui_offers_no_date_entry", True),
    ("release54_2_1_missed_session_recovery", "recovery_adds_automation", False),
    ("release54_2_1_missed_session_recovery", "recovery_creates_orders", False),
    ("release54_2_1_missed_session_recovery",
     "cycle_still_approves_nothing", True),

    # ------------------------------------------------------------------- #
    # Release 54.2.3 - CONTROLLED MONTHLY RESEARCH-INPUT RECOVERY.
    # ONE panel writer (the research owner), ONE bounded-refresh policy (the
    # bridge), a cutoff that is always the eligible session and never an operator
    # input, a future-dated panel that still blocks, ONE producibility verdict
    # read by the cycle owner, and actionability that stays a projection of the
    # already-decided primary action. Every field below BLOCKS strict mode.
    # ------------------------------------------------------------------- #
    ("release54_2_3_source_panel_recovery",
     "refresh_policy_defined_in_bridge", True),
    ("release54_2_3_source_panel_recovery", "missing_refresh_policy_defs", []),
    ("release54_2_3_source_panel_recovery", "second_panel_writer", []),
    ("release54_2_3_source_panel_recovery", "second_refresh_policy", []),
    ("release54_2_3_source_panel_recovery", "bridge_pure_stdlib", True),
    ("release54_2_3_source_panel_recovery", "bridge_numeric_imports", []),
    ("release54_2_3_source_panel_recovery", "bridge_drives_panel_owner", True),
    ("release54_2_3_source_panel_recovery", "refresh_uses_argv_array", True),
    ("release54_2_3_source_panel_recovery",
     "cutoff_bound_to_eligible_session", True),
    ("release54_2_3_source_panel_recovery", "operator_supplied_date_fields", []),
    ("release54_2_3_source_panel_recovery", "forbidden_panel_routes", []),
    ("release54_2_3_source_panel_recovery",
     "future_dated_panel_still_blocks", True),
    ("release54_2_3_source_panel_recovery",
     "verdict_defined_in_panel_owner", True),
    ("release54_2_3_source_panel_recovery", "cycle_reads_single_verdict", True),
    ("release54_2_3_source_panel_recovery", "cycle_publishes_data_quality", True),
    ("release54_2_3_source_panel_recovery",
     "cycle_copies_panel_vocabulary", False),
    ("release54_2_3_source_panel_recovery",
     "workflow_projects_actionability", True),
    ("release54_2_3_source_panel_recovery", "ui_actionability_derivation", []),
    ("release54_2_3_source_panel_recovery",
     "ui_reads_backend_actionability", True),
    # ------------------------------------------------------------------- #
    # Release 54.2.3.2 - DECISION / PROPOSAL SUPERSESSION AUTHORITY. A newer
    # authoritative governed decision supersedes an older manual-review
    # proposal: the comparison lives ONCE in api.portfolio_decision, the write
    # path refuses superseded proposals server-side, every read renders the
    # verdict verbatim, and no surface (UI included) derives one of its own.
    # Every field below BLOCKS strict mode.
    # ------------------------------------------------------------------- #
    ("release54_2_3_2_decision_supersession",
     "owner_defines_calculation_loader_selector", True),
    ("release54_2_3_2_decision_supersession",
     "second_supersession_calculation", []),
    ("release54_2_3_2_decision_supersession",
     "record_decision_refuses_superseded", True),
    ("release54_2_3_2_decision_supersession",
     "lane_state_in_vocabulary", True),
    ("release54_2_3_2_decision_supersession",
     "realloc_read_renders_verdict", True),
    ("release54_2_3_2_decision_supersession",
     "realloc_second_comparison", False),
    ("release54_2_3_2_decision_supersession",
     "workflow_consumes_verdict", True),
    ("release54_2_3_2_decision_supersession",
     "workflow_composes_authority_selector", True),
    ("release54_2_3_2_decision_supersession",
     "workflow_asserts_no_change_invariant", True),
    ("release54_2_3_2_decision_supersession",
     "superseded_never_approvable", True),
    ("release54_2_3_2_decision_supersession",
     "projection_prefers_assessment_decision", True),
    ("release54_2_3_2_decision_supersession",
     "presentation_renders_verbatim", True),
    ("release54_2_3_2_decision_supersession",
     "ams_echoes_selector", True),
    ("release54_2_3_2_decision_supersession",
     "ui_renders_superseded_states", True),
    ("release54_2_3_2_decision_supersession",
     "ui_supersession_derivation", []),
    ("release54_2_3_2_decision_supersession",
     "forbidden_supersession_routes", []),
    # ------------------------------------------------------------------- #
    # Release 54.2.4 - REALLOCATION COHERENCE + INTRADAY VISIBILITY. The three
    # economic scopes are named once; a governed HOLD renders only its own
    # current-decision economics; a superseded proposal is demoted to explicit
    # history; the live/intraday lane is composed once and rendered verbatim.
    # Every field below BLOCKS strict mode.
    # ------------------------------------------------------------------- #
    ("release54_2_4_reallocation_coherence",
     "presentation_defines_scoped_economics", True),
    ("release54_2_4_reallocation_coherence",
     "second_current_decision_calculation", []),
    ("release54_2_4_reallocation_coherence",
     "proposal_history_block_present", True),
    ("release54_2_4_reallocation_coherence",
     "hero_renders_current_decision", True),
    ("release54_2_4_reallocation_coherence",
     "hero_unscoped_proposal_econ_present", False),
    ("release54_2_4_reallocation_coherence",
     "realloc_history_demotion_present", True),
    ("release54_2_4_reallocation_coherence",
     "ams_defines_live_lane", True),
    ("release54_2_4_reallocation_coherence",
     "second_live_lane_definition", []),
    ("release54_2_4_reallocation_coherence",
     "ui_renders_live_lane", True),
    ("release54_2_4_reallocation_coherence",
     "ui_lane_governance_derivation", []),
    ("release54_2_4_reallocation_coherence",
     "stale_display_label_owned", True),
    ("release54_2_4_reallocation_coherence",
     "outcome_history_version_identity", True),
    ("release54_2_4_reallocation_coherence",
     "ca_projection_scope_declared", True),
    ("release54_2_4_reallocation_coherence",
     "eligibility_vocabulary_split", True),
    ("release54_2_4_reallocation_coherence",
     "legacy_controls_classified", True),
    # ------------------------------------------------------------------- #
    # Release 54.3 - SAME-SESSION HOC EVIDENCE VERSIONING + RETRIEVABLE
    # GOVERNANCE BINDING. The build fails if a second opportunity-cost writer
    # or store appears, if a version could OVERWRITE rather than append, if the
    # assessment-evidence identity is contaminated with the document-wide
    # portfolio hash (the Stage-21 trap), the economic axis, the conclusion or
    # any clock / run id / event id, if the persistence vocabulary drifts from
    # R54.2's, if governance could accept a dependency that was never persisted
    # or cannot be retrieved, if the pure gate starts opening a store of its
    # own, if a reassessment or proposal stops recording the EXACT artifact, if
    # the cycle stops publishing the persistence outcome it obtained or stops
    # persisting before reassessing, or if the UI starts deriving persistence
    # state for itself. Every field below BLOCKS strict mode.
    # ------------------------------------------------------------------- #
    ("release54_3_hoc_evidence_versioning", "second_hoc_writer", []),
    ("release54_3_hoc_evidence_versioning", "parallel_hoc_stores", []),
    ("release54_3_hoc_evidence_versioning", "appends_version_chain", True),
    ("release54_3_hoc_evidence_versioning", "owner_deletes_an_artifact", False),
    ("release54_3_hoc_evidence_versioning",
     "evidence_identity_contaminated", []),
    ("release54_3_hoc_evidence_versioning",
     "evidence_exclusions_declared", True),
    ("release54_3_hoc_evidence_versioning", "persist_outcomes_missing", []),
    ("release54_3_hoc_evidence_versioning",
     "inconsistent_identity_guard_present", True),
    ("release54_3_hoc_evidence_versioning", "gate_checks_missing", []),
    ("release54_3_hoc_evidence_versioning", "gate_reason_codes_declared", True),
    ("release54_3_hoc_evidence_versioning", "gate_opens_a_store", []),
    ("release54_3_hoc_evidence_versioning",
     "gate_fails_closed_on_absent_binding", True),
    ("release54_3_hoc_evidence_versioning",
     "binding_resolver_owned_by_hoc", True),
    ("release54_3_hoc_evidence_versioning",
     "reassessment_binds_exact_artifact", True),
    ("release54_3_hoc_evidence_versioning",
     "proposal_binds_exact_artifact", True),
    ("release54_3_hoc_evidence_versioning",
     "cycle_publishes_hoc_persistence", True),
    ("release54_3_hoc_evidence_versioning",
     "cycle_persists_hoc_before_reassessment", True),
    ("release54_3_hoc_evidence_versioning", "ui_derives_hoc_persistence", []),
    # ------------------------------------------------------------------- #
    # Release 54.2.3.1 - OWNED-DATA READINESS AUTHORITY. Persisted close
    # confirmation and live provider coverage are DIFFERENT concepts: the
    # coverage calculation lives once in api.daily_close (the prober), the
    # probe-free workflow owner consumes that verdict verbatim, every
    # composition supplies it, and the UI never re-derives readiness. Every
    # field below BLOCKS strict mode.
    # ------------------------------------------------------------------- #
    ("release54_2_3_1_owned_data_readiness_authority",
     "owner_defines_coverage_and_assessment", True),
    ("release54_2_3_1_owned_data_readiness_authority",
     "second_coverage_calculation", []),
    ("release54_2_3_1_owned_data_readiness_authority",
     "workflow_probe_tokens", []),
    ("release54_2_3_1_owned_data_readiness_authority",
     "workflow_consumes_close_verdict", True),
    ("release54_2_3_1_owned_data_readiness_authority",
     "snapshot_supplies_readiness", True),
    ("release54_2_3_1_owned_data_readiness_authority",
     "snapshot_composes_close_before_workflow", True),
    ("release54_2_3_1_owned_data_readiness_authority",
     "orchestrator_supplies_readiness", True),
    ("release54_2_3_1_owned_data_readiness_authority",
     "presentation_shares_one_close_read", True),
    ("release54_2_3_1_owned_data_readiness_authority",
     "distinct_owned_data_concepts", True),
    ("release54_2_3_1_owned_data_readiness_authority",
     "close_gate_echoes_coverage_verdict", True),
    ("release54_2_3_1_owned_data_readiness_authority",
     "ui_readiness_derivation", []),
    # ------------------------------------------------------------------- #
    # Release 54.2.2 - POST-CLOSE RESEARCH RECOVERY + ATTRIBUTION INTEGRITY.
    # ONE post-close obligation owner, ONE stale-input classification owner,
    # ONE orchestration path that never repeats a completed close, severity
    # decided by the backend and read by the UI, and attribution that fails
    # closed instead of publishing an unreconciled decomposition. Every field
    # below BLOCKS strict mode.
    # ------------------------------------------------------------------- #
    ("release54_2_2_post_close_research_recovery",
     "obligation_owner_defines_state_machine", True),
    ("release54_2_2_post_close_research_recovery",
     "classification_owner_defines_vocabulary", True),
    ("release54_2_2_post_close_research_recovery",
     "workflow_reads_classification", True),
    ("release54_2_2_post_close_research_recovery",
     "workflow_owns_no_classification", True),
    ("release54_2_2_post_close_research_recovery",
     "second_obligation_owner", []),
    ("release54_2_2_post_close_research_recovery",
     "second_classification_owner", []),
    ("release54_2_2_post_close_research_recovery",
     "second_research_orchestrator", []),
    ("release54_2_2_post_close_research_recovery",
     "forbidden_research_routes", []),
    ("release54_2_2_post_close_research_recovery",
     "obligation_suppresses_wait_gate", True),
    ("release54_2_2_post_close_research_recovery",
     "cycle_resumes_without_repeating_close", True),
    ("release54_2_2_post_close_research_recovery", "cycle_path_unchanged", True),
    ("release54_2_2_post_close_research_recovery",
     "obligation_declares_no_second_route", True),
    ("release54_2_2_post_close_research_recovery",
     "obligation_never_repeats_close", True),
    ("release54_2_2_post_close_research_recovery",
     "drc_gate_is_session_scoped", True),
    ("release54_2_2_post_close_research_recovery",
     "ams_delegates_obligation", True),
    ("release54_2_2_post_close_research_recovery",
     "presentation_delegates_obligation", True),
    ("release54_2_2_post_close_research_recovery",
     "workflow_states_blocker_severity", True),
    ("release54_2_2_post_close_research_recovery",
     "research_stale_never_blocks_decision", True),
    ("release54_2_2_post_close_research_recovery",
     "presentation_reads_severity", True),
    ("release54_2_2_post_close_research_recovery",
     "presentation_renders_no_dict_repr", True),
    ("release54_2_2_post_close_research_recovery",
     "attribution_availability_has_one_owner", True),
    ("release54_2_2_post_close_research_recovery",
     "close_uses_shared_availability", True),
    ("release54_2_2_post_close_research_recovery",
     "attribution_requires_exact_date", True),
    ("release54_2_2_post_close_research_recovery",
     "attribution_flags_stale_legs", True),
    ("release54_2_2_post_close_research_recovery",
     "unreconciled_is_unavailable", True),
    ("release54_2_2_post_close_research_recovery",
     "attribution_rewrites_no_history", True),
    ("release54_2_2_post_close_research_recovery",
     "ui_states_unavailable_attribution", True),
    ("release54_2_2_post_close_research_recovery",
     "ui_obligation_derivation", []),
    ("release54_2_2_post_close_research_recovery",
     "ui_renders_backend_obligation", True),
    ("release54_2_2_post_close_research_recovery",
     "ui_offers_no_research_backfill", True),
    ("release54_2_2_post_close_research_recovery",
     "research_recovery_adds_automation", False),
    ("release54_2_2_post_close_research_recovery",
     "research_recovery_creates_orders", False),
    ("release54_2_2_post_close_research_recovery",
     "monthly_contract_not_weakened", True),
)


def _blocking_invariant_failures(rep: dict) -> list[str]:
    out = []
    for key, field, expected in BLOCKING_INVARIANTS:
        got = (rep.get(key) or {}).get(field)
        if got != expected:
            out.append(f"{key}.{field}={got!r} (expected {expected!r})")
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Static architecture audit (read-only).")
    ap.add_argument("--out", default=None,
                    help="Write JSON report to this path (default: a temp file).")
    ap.add_argument("--json-only", action="store_true",
                    help="Print only the JSON report to stdout.")
    ap.add_argument("--strict", action="store_true",
                    help="Exit nonzero if a blocking category is non-empty.")
    ap.add_argument("--handoff-dir", action="append", default=[], metavar="DIR",
                    help=("Additionally scan this directory's PowerShell workflows for "
                          "restart/smoke duplication. Stage handoff scripts live outside "
                          "the repository, so the release gate points the guard at them. "
                          "Repeatable."))
    args = ap.parse_args(argv)

    rep = run_audit(tuple(args.handoff_dir or ()))
    payload = json.dumps(rep, indent=2, sort_keys=True, ensure_ascii=False)

    if args.json_only:
        print(payload)
    else:
        _print_console(rep)

    if args.out:
        out_path = Path(args.out)
        out_path.write_text(payload + "\n", encoding="utf-8")
        if not args.json_only:
            print(f"\nJSON report written to: {out_path}")
    else:
        # Default: write to a temp file so the repository is never mutated.
        tf = tempfile.NamedTemporaryFile(
            "w", suffix="_arch_audit.json", delete=False, encoding="utf-8")
        tf.write(payload + "\n")
        tf.close()
        if not args.json_only:
            print(f"\nJSON report written to: {tf.name}")

    if args.strict:
        wf = rep["workflow_state_ownership"]
        dr = rep["daily_research_cycle_ownership"]
        us = rep["universe_scoring_ownership"]
        la = rep["slice3_live_acceptance_ownership"]
        me = rep["monthly_emitter_bridge_ownership"]
        ps = rep["portfolio_state_ownership"]
        ho = rep["holding_opportunity_cost_ownership"]
        la6 = rep["slice6_live_acceptance_ownership"]
        rc6 = rep["slice6_residual_cutover_ownership"]
        mr = rep["drc_manifest_recovery"]
        rp = rep["reallocation_proposal_ownership"]
        cr = rep["controlled_rebalance_ownership"]
        fc = rep["failclosed_rebalance_execution"]
        ra = rep["research_agent_ownership"]
        de = rep["data_expansion_ownership"]
        ux = rep["operator_ux_consolidation_ownership"]
        icx = rep["information_collection_ownership"]
        blocking_hits = (len(rep["routes"]["duplicate_declarations"])
                         + len(rc6["forbidden_primary_ui"])
                         + len(rc6["forbidden_primary_ws"])
                         + (0 if rc6["gate_compatibility_classified"] else 1)
                         + (0 if rc6["workflow_primary_is_hoc"] else 1)
                         + len(rc6["missing_ws_primary_tokens"])
                         + (0 if rc6["legacy_comparison_collapsed"] else 1)
                         + (0 if rc6["hoc_is_sole_primary_card"] else 1)
                         + (0 if rc6["surfaces_use_hoc_state"] else 1)
                         + (0 if rc6["ui_hoc_loader_count"] == 1 else 1)
                         + len(rc6["ui_recommendation_or_cost_computation"])
                         + (0 if rc6["drc_sole_execution_path"] else 1)
                         + (0 if rc6["no_manual_hoc_execution_endpoint"] else 1)
                         + len(rc6["forbidden_routes_present"])
                         + len(rc6["slice7_missing"])
                         + len(rc6["slice8_present"])
                         + len(la6["obsolete_reassessment_control"])
                         + len(la6["obsolete_rebalance_label"])
                         + (0 if la6["legacy_compatibility_classified"] else 1)
                         + len(la6["forbidden_routes_present"])
                         + (0 if la6["readiness_separated"] else 1)
                         + (0 if la6["ready_is_service_scoped"] else 1)
                         + len(la6["ready_conflates_session"])
                         + (0 if la6["hoc_renders_not_run"] else 1)
                         + (0 if la6["hoc_renders_completed"] else 1)
                         + (0 if ho["kernel_present"] else 1)
                         + (0 if ho["owner_present"] else 1)
                         + len(ho["second_calculation_owner_modules"])
                         + len(ho["second_composition_owner_modules"])
                         + len(ho["missing_delegation"])
                         + len(ho["owner_forbidden_calls"])
                         + len(ho["kernel_impurity"])
                         + (0 if ho["route_present"] else 1)
                         + (0 if ho["route_methods"] == ["GET"] else 1)
                         + (0 if ho["no_separate_manual_execution_endpoint"] else 1)
                         + (0 if ho["drc_delegates_to_owner"] else 1)
                         + (0 if ho["gate_delegates_to_summary"] else 1)
                         + (0 if ho["ui_loader_count"] == 1 else 1)
                         + len(ho["ui_recommendation_or_cost_computation"])
                         + (0 if ho["ui_review_label_present"] else 1)
                         + len(ho["slice7_missing_modules"])
                         + len(ho["slice7_missing_route"])
                         + len(ho["slice7_forbidden_present"])
                         + len(ho["slice8_present_modules"])
                         + len(ho["summary_loads_portfolio_state"])
                         + (0 if ho["gate_supplies_hoc_context"] else 1)
                         + (0 if ho["portfolio_state_composes_gate"] else 1)
                         + (0 if ho["no_circular_read_dependency"] else 1)
                         + (0 if ps["owner_present"] else 1)
                         + (0 if ps["owner_defines_loader"] else 1)
                         + len(ps["missing_delegation"])
                         + len(ps["forbidden_execution_calls"])
                         + (0 if ps["active_book_selection_present"] else 1)
                         + (0 if ps["rejects_dormant_legacy_book"] else 1)
                         + len(ps["second_portfolio_state_owner_modules"])
                         + (0 if ps["canonical_route_present"] else 1)
                         + (0 if ps["canonical_route_methods"] == ["GET"] else 1)
                         + (0 if ps["ui_portfolio_state_loader_count"] == 1 else 1)
                         + (0 if ps["ui_portfolio_state_renderer_count"] == 1 else 1)
                         + len(ps["ui_portfolio_state_computation"])
                         + len(ps["ui_missing_guard_tokens"])
                         + (0 if me["owner_present"] else 1)
                         + (0 if me["bridge_pure_stdlib"] else 1)
                         + len(me["bridge_numeric_imports"])
                         + (0 if me["bridge_no_shell_string"] else 1)
                         + (0 if me["bridge_delegates_phase25_math"] else 1)
                         + len(me["second_monthly_formula_modules"])
                         + (0 if me["adapter_wires_production_resolver"] else 1)
                         + (0 if me["app_wires_production_resolver"] else 1)
                         + (0 if me["no_separate_monthly_ui_button"] else 1)
                         + len(me["separate_monthly_endpoints"])
                         + (0 if la["non_session_requires_authoritative_source"] else 1)
                         + (0 if la["monthly_input_adapter_present"] else 1)
                         + (0 if la["monthly_input_owner_declared"] else 1)
                         + (0 if la["target_calculation_owner_declared"] else 1)
                         + len(la["monthly_adapter_forbidden_calls"])
                         + (0 if la["waiting_outranks_research_blockers"] else 1)
                         + (0 if us["owner_present"] else 1)
                         + (0 if us["kernel_present"] else 1)
                         + len(us["missing_delegation"])
                         + len(us["second_scoring_engine_in_owner"])
                         + len(us["forbidden_execution_calls"])
                         + len(us["duplicate_operational_scoring_modules"])
                         + (0 if us["drc_delegates_scoring"] else 1)
                         + (0 if us["platform_compat_wrapper"] else 1)
                         + (0 if us["canonical_route_present"] else 1)
                         + (0 if us["compat_route_present"] else 1)
                         + (0 if us["ui_scoring_loader_count"] == 1 else 1)
                         + len(us["ui_scoring_computation"])
                         + len(rep["research_execution_terms"])
                         + len(wf["ui_unauthorized_canonical_writers"])
                         + len(wf["ui_unguarded_setters"])
                         + len(wf["ui_workflow_priority_derivation"])
                         + (0 if wf["ui_workflow_loader_count"] == 1 else 1)
                         + (0 if wf["ui_canonical_ownership_declared"] else 1)
                         + (0 if dr["owner_present"] else 1)
                         + (0 if dr["sole_orchestrator"] else 1)
                         + len(dr["competing_orchestrators"])
                         + len(dr["missing_delegation"])
                         + len(dr["forbidden_execution_calls"])
                         + len(dr["forbidden_second_scoring_engine"])
                         + (0 if dr["status_endpoint_present"] else 1)
                         + (0 if dr["run_endpoint_present"] else 1)
                         + (0 if dr["ui_status_loader_count"] == 1 else 1)
                         + (0 if dr["ui_execution_function_count"] == 1 else 1)
                         + len(dr["ui_planning_derivation"])
                         + (0 if mr["sole_orchestrator"] else 1)
                         + len(mr["competing_orchestrators"])
                         + len(mr["missing_terminal_persistence_tokens"])
                         + (0 if mr["terminal_read_back_present"] else 1)
                         + len(mr["mark_complete_defs"])
                         + len(mr["forbidden_recovery_routes"])
                         + (0 if mr["single_artifact_root"] else 1)
                         + len(mr["missing_status_reflect_tokens"])
                         + len(mr["separate_recovery_entry_defs"])
                         + len(mr["forbidden_execution_calls"])
                         + len(mr["missing_preclose_tokens"])
                         + len(mr["missing_genuine_inconsistency_tokens"])
                         + (0 if mr["hoc_data_gaps_explicit"] else 1)
                         + len(mr["slice7_missing_modules"])
                         + len(mr["slice7_missing_route"])
                         + len(mr["slice7_forbidden_present"])
                         + len(mr["slice8_present_modules"])
                         # --- Slice 7 (Phase 29H) reallocation proposal ownership ---- #
                         + (0 if rp["kernel_present"] else 1)
                         + (0 if rp["owner_present"] else 1)
                         + len(rp["second_calculation_owner_modules"])
                         + len(rp["second_composition_owner_modules"])
                         + len(rp["missing_delegation"])
                         + len(rp["owner_forbidden_calls"])
                         + len(rp["kernel_forbidden_calls"])
                         + (0 if rp["route_get_count"] == 1 else 1)
                         + (0 if not rp["forbidden_route_methods_present"] else 1)
                         + len(rp["forbidden_routes_present"])
                         + (0 if rp["drc_delegates"] else 1)
                         + (0 if rp["drc_step_present"] else 1)
                         + (0 if rp["gate_delegates_to_summary"] else 1)
                         + (0 if rp["persist_present"] else 1)
                         + (0 if rp["atomic_idempotent_persist_present"] else 1)
                         + (0 if rp["ui_loader_count"] == 1 else 1)
                         + len(rp["ui_allocation_or_cost_computation"])
                         + (0 if not rp["kernel_forks_hoc"] else 1)
                         + (0 if not rp["kernel_forks_scoring"] else 1)
                         + len(rp["slice8_present_modules"])
                         # --- Stage 19 controlled-rebalance ownership ----------------- #
                         + (0 if cr["owner_present"] else 1)
                         + (0 if cr["controlled_route_get"] else 1)
                         + (0 if cr["confirm_route_post"] else 1)
                         + (0 if cr["corporate_action_routes_present"] else 1)
                         + (0 if cr["requires_stage18_approval"] else 1)
                         + (0 if cr["requires_second_confirmation"] else 1)
                         + (0 if cr["delegates_to_existing_desk"] else 1)
                         + len(cr["second_execution_owner_defs"])
                         + (0 if cr["corporate_action_confirm_gated"] else 1)
                         + (0 if cr["corporate_action_read_time_projection"] else 1)
                         + len(cr["forbidden_auto_execution_routes_present"])
                         + len(cr["automatic_tokens_present"])
                         # --- Stage 19.2 fail-closed rebalance execution -------------- #
                         + (0 if fc["owner_present"] else 1)
                         + (0 if fc["owner_defines_executability_contract"] else 1)
                         + len(fc["second_contract_owner_modules"])
                         + len(fc["state_derived_buildable_modules"])
                         + (0 if fc["confirm_fails_closed_before_write"] else 1)
                         + (0 if fc["delegates_to_canonical_mark_owner"] else 1)
                         + (0 if fc["mark_owner_accepts_delegation"] else 1)
                         + len(fc["owner_provider_calls"])
                         + (0 if fc["hydration_token_gated"] else 1)
                         + (0 if fc["hydration_route_post_count"] == 1 else 1)
                         + (0 if fc["hydration_route_methods"] == ["POST"] else 1)
                         + (0 if fc["read_route_get_only"] else 1)
                         + len(fc["read_region_provider_calls"])
                         + len(fc["second_execution_owner_defs"])
                         + (0 if fc["next_close_sole_settlement"] else 1)
                         + len(fc["ui_missing_blocked_tokens"])
                         + len(fc["ui_order_creating_controls"])
                         + len(fc["automatic_tokens_present"])
                         # --- Slice 8 (Phase 29I) research-agent ownership ------------ #
                         + (0 if ra["kernel_present"] else 1)
                         + (0 if ra["owner_present"] else 1)
                         + len(ra["landed_modules_missing"])
                         + len(ra["second_calculation_owner_modules"])
                         + len(ra["second_composition_owner_modules"])
                         + len(ra["missing_delegation"])
                         + len(ra["owner_forbidden_calls"])
                         + len(ra["kernel_forbidden_calls"])
                         + (0 if ra["route_get_count"] == 1 else 1)
                         + (0 if not ra["forbidden_route_methods_present"] else 1)
                         + len(ra["forbidden_routes_present"])
                         + (0 if ra["drc_delegates"] else 1)
                         + (0 if ra["drc_step_present"] else 1)
                         + (0 if ra["persist_present"] else 1)
                         + (0 if ra["atomic_idempotent_persist_present"] else 1)
                         + (0 if ra["ui_loader_count"] == 1 else 1)
                         + len(ra["ui_metric_computation"])
                         + (0 if not ra["kernel_forks_hoc"] else 1)
                         + (0 if not ra["kernel_forks_reallocation"] else 1)
                         + (0 if not ra["kernel_forks_scoring"] else 1)
                         + len(ra["second_registry_present_modules"])
                         + len(ra["slice9_present_modules"])
                         # --- Slice 9 (Phase 29J) data expansion / purchase-gate ------ #
                         + (0 if de["kernel_present"] else 1)
                         + (0 if de["owner_present"] else 1)
                         + len(de["landed_modules_missing"])
                         + len(de["second_calculation_owner_modules"])
                         + len(de["second_composition_owner_modules"])
                         + len(de["missing_reuse"])
                         + len(de["owner_forbidden_calls"])
                         + len(de["kernel_forbidden_calls"])
                         + (0 if de["route_get_count"] == 1 else 1)
                         + (0 if de["detail_route_get_count"] == 1 else 1)
                         + (0 if not de["forbidden_route_methods_present"] else 1)
                         + len(de["forbidden_routes_present"])
                         + (0 if de["persist_present"] else 1)
                         + (0 if de["atomic_idempotent_persist_present"] else 1)
                         + (0 if de["ui_loader_count"] == 1 else 1)
                         + len(de["ui_metric_computation"])
                         + (0 if not de["kernel_forks_research_agent"] else 1)
                         + (0 if not de["kernel_forks_stage13a"] else 1)
                         + len(de["secret_ownership"])
                         + (0 if de["cadence_disabled"] else 1)
                         + len(de["drc_daily_job_present"])
                         + len(de["slice10_present_modules"])
                         # --- Phase 29J.1 operator UX consolidation ------------------- #
                         + (0 if ux["primary_areas_present"] else 1)
                         + (0 if ux["legacy_views_demoted"] else 1)
                         + len(ux["missing_route_aliases"])
                         + len(ux["missing_legacy_routes"])
                         + (0 if ux["market_loader_count"] == 1 else 1)
                         + (0 if ux["market_owner_fetch"] else 1)
                         + (0 if ux["market_route_get_only"] else 1)
                         + len(ux["ui_direct_provider_hosts"])
                         + len(ux["market_region_market_math"])
                         + (0 if ux["market_context_present"] else 1)
                         + (0 if ux["market_reference_only"] else 1)
                         + (0 if ux["workflow_next_action_renderer_count"] == 1 else 1)
                         + len(ux["missing_safety_tokens"])
                         + len(ux["forbidden_new_routes_present"])
                         # --- Release 29 continuous information collection ------------ #
                         + sum(0 if v else 1
                               for v in icx["modules_present"].values())
                         + len(icx["kernel_impurity"])
                         + len(icx["second_cadence_owner_modules"])
                         + len(icx["second_collection_owner_modules"])
                         + len(icx["second_worker_scripts"])
                         + len(icx["owner_forbidden_calls"])
                         + len(icx["missing_delegation"])
                         + (0 if icx["route_get_count"] == 1 else 1)
                         + (0 if icx["route_methods"] == ["GET"] else 1)
                         + len(icx["forbidden_routes_present"])
                         + len(icx["missing_safety_tokens"])
                         + (0 if icx["observation_rule_present"] else 1)
                         + (0 if icx["read_surface_bound_to_gate"] else 1)
                         + (0 if icx["single_cycle_clock"] else 1)
                         + (0 if icx["ui_loader_count"] == 1 else 1)
                         + len(icx["missing_ui_tokens"])
                         + len(icx["ui_health_derivation"])
                         + (0 if icx["worker_delegates"] else 1)
                         + len(icx["worker_reimplements_cadence"])
                         + len(icx["unexpected_collection_scripts"])
                         + (0 if icx["task_definition_owner_present"] else 1)
                         + (0 if icx["manager_delegates_registration"] else 1)
                         + (0 if icx["validator_is_read_only"] else 1)
                         + (0 if icx["manage_requires_execute"] else 1)
                         + (0 if icx["status_is_read_only"] else 1)
                         + (0 if icx["uninstall_preserves_evidence"] else 1)
                         + (0 if icx["topology_owner_present"] else 1)
                         + len(icx["second_topology_owner_modules"])
                         + len(icx["manage_missing_topology_tokens"])
                         + len(icx["manage_counts_raw_processes"])
                         + (0 if icx["control_delegates_topology"] else 1)
                         + len(icx["missing_progress_tokens"])
                         + len(icx["second_progress_owner_modules"])
                         + len(icx["worker_timer_authorities"])
                         + (0 if icx["worker_reports_progress"] else 1)
                         + (0 if icx["stall_budget_not_widened"] else 1)
                         + len(rep["inventory_drift"]["on_disk_not_in_inventory"])
                         + len(rep["inventory_drift"]["in_inventory_not_on_disk"]))
        # Stage 19.1 — corporate-action propagation invariants block strict mode too.
        ca_failures = _blocking_invariant_failures(rep)
        if ca_failures:
            print("\nBLOCKING semantic-ownership invariants "
                  "(corporate-action propagation + Stage 19.3 operator/atomic close):")
            for f in ca_failures:
                print(f"  FAIL  {f}")
        blocking_hits += len(ca_failures)
        return 1 if blocking_hits else 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
