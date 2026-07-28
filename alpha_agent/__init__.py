"""
alpha_agent — Alpha Agent Stage 1: Unified Research Memory and Canonical Registry.

This package builds a deterministic, versioned, queryable SQLite registry that
consolidates the fragmented Paper Trader / research history (datasets, hypotheses,
features, experiments, signals, evidence, decisions, duplicates, coverage and current
operational state) into a single memory layer that any future research agent stage can
consult before scheduling work.

Read-only by construction with respect to every source root: no network, no model API,
no PostgreSQL connection, no source mutation, no operational-ledger mutation. The only
database it writes is the SQLite file under its own output root.
"""
from __future__ import annotations

from . import research_importers, research_registry

SCHEMA_VERSION = research_registry.SCHEMA_VERSION
IMPORTER_VERSION = research_importers.IMPORTER_VERSION
STAGE = research_registry.STAGE

READY = research_registry.READY
BLOCKED = research_registry.BLOCKED
VERIFIED = research_registry.VERIFIED
NO_CHANGES = research_registry.NO_CHANGES

build_registry = research_registry.build_registry
classify_candidate_experiment = research_registry.classify_candidate_experiment

__all__ = [
    "research_importers", "research_registry",
    "SCHEMA_VERSION", "IMPORTER_VERSION", "STAGE",
    "READY", "BLOCKED", "VERIFIED", "NO_CHANGES",
    "build_registry", "classify_candidate_experiment",
]
