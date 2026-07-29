"""
alpha_agent — the permanent autonomous alpha agent's local packages.

Stage 1 (research_registry / research_importers): Unified Research Memory and
Canonical Registry — a deterministic, versioned, queryable SQLite registry that
consolidates the fragmented Paper Trader / research history into a single memory
layer that any future research agent stage consults before scheduling work.
Read-only with respect to every source root: no network, no model API, no
PostgreSQL connection, no source mutation, no operational-ledger mutation.

Stage 2 (source_contracts / collectors / ingestion): Autonomous Data Acquisition
Foundation — deterministic, incremental, bounded ingestion of real source data
(Norgate local, EODHD, SEC EDGAR, FINRA, Nasdaq Trader, FRED/ALFRED, GDELT
deferred) with immutable content-addressed raw archival, a common point-in-time
normalized record contract, resumable per-source checkpoints in a local sqlite3
state database, entitlement audits, source-health/data-quality evidence and the
daily-ingestion report contract. No LLM calls, no PostgreSQL, no Paper Trader
mutation, no orders, no automation.

Stage 3 (llm_contracts / llm_budget / llm_providers / research_director):
Grounded LLM Research Director — deterministic orchestration of a strictly
bounded LLM (event interpretation, hypothesis generation, prioritization,
report narrative) over the verified Stage 1 registry and verified Stage 2
normalized records, with grounding validation, Stage 1 duplicate prevention,
an immutable prioritized research queue and hard token/cost budgets. The LLM
has no tools, no code execution, no web and no file access; no experiment,
promotion, order or Paper Trader mutation ever occurs.
"""
from __future__ import annotations

from . import (event_clustering, experiment_contracts, experiment_factory,
               experiment_runner, feed_contracts, feed_registry, ingestion,
               llm_budget, llm_contracts, llm_providers, report_renderer,
               research_director, research_importers, research_registry,
               runtime, runtime_contracts, source_contracts)

SCHEMA_VERSION = research_registry.SCHEMA_VERSION
IMPORTER_VERSION = research_importers.IMPORTER_VERSION
STAGE = research_registry.STAGE

READY = research_registry.READY
BLOCKED = research_registry.BLOCKED
VERIFIED = research_registry.VERIFIED
NO_CHANGES = research_registry.NO_CHANGES

build_registry = research_registry.build_registry
classify_candidate_experiment = research_registry.classify_candidate_experiment

INGESTION_SCHEMA_VERSION = source_contracts.INGESTION_SCHEMA_VERSION
COLLECTOR_VERSION = source_contracts.COLLECTOR_VERSION
STAGE2_READY = ingestion.READY
STAGE2_PARTIAL = ingestion.PARTIAL
STAGE2_BLOCKED = ingestion.BLOCKED
STAGE2_VERIFIED = ingestion.VERIFIED
NO_NEW_SOURCE_DATA = ingestion.NO_NEW
run_ingestion = ingestion.run_ingestion

FEED_SCHEMA_VERSION = feed_contracts.FEED_SCHEMA_VERSION
RSS_COLLECTOR_VERSION = feed_contracts.RSS_COLLECTOR_VERSION
STAGE35_READY = feed_registry.READY
STAGE35_PARTIAL = feed_registry.PARTIAL
STAGE35_NO_NEW = feed_registry.NO_NEW
STAGE35_VERIFIED = feed_registry.VERIFIED
STAGE35_BLOCKED = feed_registry.BLOCKED
run_news_rss = feed_registry.run_news_rss
cluster_events = event_clustering.cluster_events

DIRECTOR_SCHEMA_VERSION = llm_contracts.DIRECTOR_SCHEMA_VERSION
PROMPT_VERSION = llm_contracts.PROMPT_VERSION
STAGE3_READY = research_director.READY
STAGE3_DEV_READY = research_director.DEV_READY
STAGE3_BUDGET_EXHAUSTED = research_director.BUDGET_EXHAUSTED
STAGE3_VERIFIED = research_director.VERIFIED
STAGE3_PARTIAL = research_director.PARTIAL
STAGE3_BLOCKED = research_director.BLOCKED
NO_NEW_DIRECTOR_INPUT = research_director.NO_NEW
run_director = research_director.run_director

# Stage 4 — persistent Windows research runtime + friendly email reports.
RUNTIME_SCHEMA_VERSION = runtime_contracts.RUNTIME_SCHEMA_VERSION
RUNTIME_VERSION = runtime_contracts.RUNTIME_VERSION
STAGE4_READY = runtime_contracts.READY
STAGE4_DEGRADED = runtime_contracts.DEGRADED
STAGE4_EMAIL_CREDENTIAL_REQUIRED = runtime_contracts.EMAIL_CREDENTIAL_REQUIRED
STAGE4_NO_NEW_RESEARCH_INPUT = runtime_contracts.NO_NEW_RESEARCH_INPUT
STAGE4_VERIFIED = runtime_contracts.VERIFIED
STAGE4_BLOCKED = runtime_contracts.BLOCKED
ALPHA_AGENT_TASK_NAMES = runtime_contracts.ALPHA_AGENT_TASK_NAMES
Runtime = runtime.Runtime
RealStageDrivers = runtime.RealStageDrivers

# Stage 5 — autonomous experiment & evidence engine.
STAGE5_SCHEMA_VERSION = experiment_contracts.STAGE5_SCHEMA_VERSION
STAGE5_ENGINE_VERSION = experiment_contracts.STAGE5_ENGINE_VERSION
STAGE5_READY = experiment_contracts.READY
STAGE5_VERIFIED = experiment_contracts.VERIFIED
STAGE5_NO_EXPERIMENTABLE_HYPOTHESES = \
    experiment_contracts.NO_EXPERIMENTABLE_HYPOTHESES
STAGE5_DATA_HOLD = experiment_contracts.DATA_HOLD
STAGE5_PARTIAL = experiment_contracts.PARTIAL
STAGE5_BLOCKED = experiment_contracts.BLOCKED
STAGE5_TEMPLATES = experiment_contracts.SUPPORTED_TEMPLATES
run_stage5_cycle = experiment_factory.run_stage5_cycle
verify_stage5_cycle = experiment_factory.verify_cycle

__all__ = [
    "research_importers", "research_registry", "source_contracts", "ingestion",
    "llm_contracts", "llm_budget", "llm_providers", "research_director",
    "feed_contracts", "feed_registry", "event_clustering",
    "FEED_SCHEMA_VERSION", "RSS_COLLECTOR_VERSION", "STAGE35_READY",
    "STAGE35_PARTIAL", "STAGE35_NO_NEW", "STAGE35_VERIFIED", "STAGE35_BLOCKED",
    "run_news_rss", "cluster_events",
    "SCHEMA_VERSION", "IMPORTER_VERSION", "STAGE",
    "READY", "BLOCKED", "VERIFIED", "NO_CHANGES",
    "build_registry", "classify_candidate_experiment",
    "INGESTION_SCHEMA_VERSION", "COLLECTOR_VERSION",
    "STAGE2_READY", "STAGE2_PARTIAL", "STAGE2_BLOCKED", "STAGE2_VERIFIED",
    "NO_NEW_SOURCE_DATA", "run_ingestion",
    "DIRECTOR_SCHEMA_VERSION", "PROMPT_VERSION",
    "STAGE3_READY", "STAGE3_DEV_READY", "STAGE3_BUDGET_EXHAUSTED",
    "STAGE3_VERIFIED", "STAGE3_PARTIAL", "STAGE3_BLOCKED",
    "NO_NEW_DIRECTOR_INPUT", "run_director",
    "report_renderer", "runtime", "runtime_contracts",
    "RUNTIME_SCHEMA_VERSION", "RUNTIME_VERSION", "STAGE4_READY",
    "STAGE4_DEGRADED", "STAGE4_EMAIL_CREDENTIAL_REQUIRED",
    "STAGE4_NO_NEW_RESEARCH_INPUT", "STAGE4_VERIFIED", "STAGE4_BLOCKED",
    "ALPHA_AGENT_TASK_NAMES", "Runtime", "RealStageDrivers",
    "experiment_contracts", "experiment_factory", "experiment_runner",
    "STAGE5_SCHEMA_VERSION", "STAGE5_ENGINE_VERSION", "STAGE5_READY",
    "STAGE5_VERIFIED", "STAGE5_NO_EXPERIMENTABLE_HYPOTHESES",
    "STAGE5_DATA_HOLD", "STAGE5_PARTIAL", "STAGE5_BLOCKED", "STAGE5_TEMPLATES",
    "run_stage5_cycle", "verify_stage5_cycle",
]
