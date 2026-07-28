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
"""
from __future__ import annotations

from . import ingestion, research_importers, research_registry, source_contracts

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

__all__ = [
    "research_importers", "research_registry", "source_contracts", "ingestion",
    "SCHEMA_VERSION", "IMPORTER_VERSION", "STAGE",
    "READY", "BLOCKED", "VERIFIED", "NO_CHANGES",
    "build_registry", "classify_candidate_experiment",
    "INGESTION_SCHEMA_VERSION", "COLLECTOR_VERSION",
    "STAGE2_READY", "STAGE2_PARTIAL", "STAGE2_BLOCKED", "STAGE2_VERIFIED",
    "NO_NEW_SOURCE_DATA", "run_ingestion",
]
