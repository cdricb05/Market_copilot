"""alpha_agent.r40 - Release 40, Prospective Alpha Acceleration and the Open
Intelligence Frontier.

RESEARCH ONLY. Release 39 left the estate with one historically interesting
machine-generated near miss (WIDE), a representation-availability defect
that stops its historical confirmation from being clean proof, several
materially distinct near misses, three immutable non-promotable research
shadows, a universal historical search engine, a cumulative search burden of
194 effective trials, and a first eligible TRUE_FORWARD decision approaching.
Release 40's objective:

    MAXIMISE INDEPENDENT INFORMATION GAIN PER UNIT OF CALENDAR TIME while
    finishing the materially distinct information and model branches that
    remain open - without manufacturing sample size, without waiting for
    work that can be done today, and without moving anything toward
    production.

Ownership map - each concern has exactly one owner in this package; every
already-owned concern (PIT alignment, market identity, economic controls,
transaction costs, multiple testing, the search-burden ledger, TRUE_FORWARD
evidence capture, chain-hashed ledgers, workflow/portfolio state, model
promotion) is IMPORTED from its canonical owner and never re-implemented:

===============================  =============================================
concern                          owner
===============================  =============================================
release contract                 :mod:`alpha_agent.r40.contract`
R39 closeout import/verification :mod:`alpha_agent.r40.closeout_import`
search-burden inheritance        :mod:`alpha_agent.r40.burden_ledger`
                                 (records through ``alpha_agent.r39.zones``)
feature availability integrity   :mod:`alpha_agent.r40.availability`
corrected WIDE successor         :mod:`alpha_agent.r40.wide_successor`
NY Fed legacy dealer bridge      :mod:`alpha_agent.r40.nyfed_bridge`
open-weight model frontier       :mod:`alpha_agent.r40.open_models`
temporal / relational challenge  :mod:`alpha_agent.r40.model_challenge`
cross-asset relational research  :mod:`alpha_agent.r40.cross_asset`
shadow registry v2 (cap FIVE)    :mod:`alpha_agent.r40.shadow_registry`
prospective research cycle       :mod:`alpha_agent.r40.research_cycle`
                                 (captures through
                                 ``alpha_agent.r39.research_shadow``)
evidence velocity                :mod:`alpha_agent.r40.evidence_velocity`
always-valid sequential designs  :mod:`alpha_agent.r40.sequential`
                                 (e-process from ``r39.prospective_design``)
joint predictive/economic table  :mod:`alpha_agent.r40.joint_evidence`
research portfolio               :mod:`alpha_agent.r40.research_portfolio`
Intrinio sample readiness        :mod:`alpha_agent.r40.intrinio_readiness`
compute escalation               :mod:`alpha_agent.r40.compute_escalation`
orchestration + verdict          :mod:`alpha_agent.r40.campaign`
===============================  =============================================

This package creates NO signal authority, NO portfolio target, NO proposal,
NO decision, NO order, NO model promotion, NO sleeve activation, NO
scheduler change, NO production restart and NO operational write. It may
download open model weights ONLY under the ten conditions declared in
:mod:`contract` (all $0, no account, no licence acceptance on the
operator's behalf, weights on the research drive, provenance hashed).
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from .. import r39 as _r39
from ..r31 import (  # noqa: F401  (re-exported: ONE hashing/artifact owner)
    ArtifactImmutable,
    file_fingerprint,
    read_json,
    sha,
    sha_file,
    write_json,
)

RELEASE = "release40"
CAMPAIGN_FAMILY = "prospective_alpha_acceleration"
CAMPAIGN_ID = "r40_prospective_alpha_acceleration_v1"

RESEARCH_ROOT_ENV = "PAPER_TRADER_R40_RESEARCH_ROOT"
DEFAULT_RESEARCH_ROOT = Path(r"D:\Stock_Prediction_app_data\prospective_alpha_r40")

#: Large files (model weights, package caches, panels) live on the research
#: drive. C: had ~5.6 GB free when this release started; nothing large may
#: be written there.
LARGE_FILE_DRIVE = "D:"

SAFETY = list(_r39.SAFETY) + ["NO SCHEDULER CHANGE", "NO BACKDATED FORWARD ROW"]

AUTOMATIC_PROMOTION_ALLOWED = False
AUTOMATIC_SLEEVE_ACTIVATION_ALLOWED = False
MAY_SPEND_MONEY = False
MAY_MUTATE_PRODUCTION = False
TRAINS_MODELS = True
PROMOTES_MODELS = False
CHANGES_SCHEDULER = False


def research_root() -> Path:
    return Path(os.environ.get(RESEARCH_ROOT_ENV) or DEFAULT_RESEARCH_ROOT)


def campaign_dir(campaign_id: str = CAMPAIGN_ID) -> Path:
    """The R40 campaign directory, ALSO registered with the R39 owner package
    so the R39 reuse ledger / lockbox / artifact writer serve this campaign
    under the R40 root (one owner, two releases)."""
    d = research_root() / str(campaign_id)
    _r39.register_campaign_root(campaign_id, research_root())
    return d


def safety_block() -> dict:
    body = _r39.safety_block()
    body["safety"] = list(SAFETY)
    body["changes_scheduler"] = CHANGES_SCHEDULER
    body["may_download_open_model_weights"] = "ONLY_UNDER_CONTRACT_CONDITIONS"
    body["backdates_forward_rows"] = False
    return body


def artifact_body(schema: str, payload: dict, **extra: Any) -> dict:
    body = {"schema": schema, "release": RELEASE, "campaign_id": CAMPAIGN_ID}
    body.update(payload)
    body.update(extra)
    body["safety_block"] = safety_block()
    return body


__all__ = [
    "RELEASE", "CAMPAIGN_FAMILY", "CAMPAIGN_ID", "RESEARCH_ROOT_ENV",
    "DEFAULT_RESEARCH_ROOT", "LARGE_FILE_DRIVE", "SAFETY",
    "AUTOMATIC_PROMOTION_ALLOWED", "AUTOMATIC_SLEEVE_ACTIVATION_ALLOWED",
    "MAY_SPEND_MONEY", "MAY_MUTATE_PRODUCTION", "TRAINS_MODELS",
    "PROMOTES_MODELS", "CHANGES_SCHEDULER",
    "research_root", "campaign_dir", "safety_block", "artifact_body",
    "sha", "sha_file", "file_fingerprint", "write_json", "read_json",
    "ArtifactImmutable",
]
