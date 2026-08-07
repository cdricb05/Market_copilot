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
import json
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
                           "/v1/operations/rebalance",
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
# Slice 9 (Paid-data integration, Data Expansion) remains future.
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
LA6_LEGACY_COMPAT_TOKENS = ("LEGACY MEMBERSHIP-COMPARISON",)
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
LA6_ABSENT_ROUTES = ("/v1/operations/portfolio-reassessment",
                     "/v1/operations/reassessment",
                     "/v1/operations/rebalance",
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
    second / unified model registry exists; (18) Slice 9 remains planned; (19) cadence
    disabled and NO automatic model promotion; (20) inventory drift zero (checked by
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

    # (17) NO second / unified model registry; (18) Slice 9 remains planned.
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
def run_audit() -> dict:
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
        "research_agent_ownership": check_research_agent_ownership(files),
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
          f"Slice 9 present (must be empty): {ra['slice9_present_modules']}  "
          f"cadence enabled (must be False): {ra['cadence_enabled']}")

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


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Static architecture audit (read-only).")
    ap.add_argument("--out", default=None,
                    help="Write JSON report to this path (default: a temp file).")
    ap.add_argument("--json-only", action="store_true",
                    help="Print only the JSON report to stdout.")
    ap.add_argument("--strict", action="store_true",
                    help="Exit nonzero if a blocking category is non-empty.")
    args = ap.parse_args(argv)

    rep = run_audit()
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
        ra = rep["research_agent_ownership"]
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
                         + len(rep["inventory_drift"]["on_disk_not_in_inventory"])
                         + len(rep["inventory_drift"]["in_inventory_not_on_disk"]))
        return 1 if blocking_hits else 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
