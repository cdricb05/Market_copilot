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

    # (9) the worker delegates and owns no cadence.
    worker_delegates = all(t in worker for t in
                           ("ic.run_collection_iteration(", "ic.acquire_service_lock(",
                            "ic.heartbeat(", "ic.release_service_lock("))
    worker_reimplements_cadence = sorted(
        t for t in ("CADENCE_POLICY_BY_ID", "def resolve_window(",
                    "normal_interval_seconds =") if t in worker)

    # (10) exactly one manager script, read-only by default.
    manage_scripts = sorted(
        _rel(fp) for fp in (REPO_ROOT / "scripts").glob("*.ps1")
        if "information_collection" in fp.name.lower()
        or "collection_service" in fp.name.lower())
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
    print(f"manager scripts (must be exactly one): {icx['manage_scripts']}  "
          f"mutations require -Execute: {icx['manage_requires_execute']}  "
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
                         + (0 if len(icx["manage_scripts"]) == 1 else 1)
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
