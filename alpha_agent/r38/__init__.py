"""alpha_agent.r38 - Release 38, the Native Futures Information Frontier.

RESEARCH ONLY. Release 37 priced the wall (``R37_DATA_INVESTMENT_RECOMMENDED``)
and the operator manually purchased the Norgate World Futures (Silver) package.
Release 38 inherits that purchase as a FACT - it spends nothing, changes no
subscription, and its first duty is to replace Release 37's EXPECTED unlocks
with DELIVERED_AND_VERIFIED ones, or to say plainly that it cannot.

The release answers five questions, in order, and refuses to conflate them:

1. **What did Norgate actually deliver?** The website confirmation is not local
   bytes. Phase 1 proves (or disproves) that the local Norgate Data Updater has
   synchronized the newly purchased package before anything else runs.
2. **Which Release-36 blocked cells are ACTUALLY open?** The ~53 expected
   unlocks are an expectation, never propagated as fact
   (``EXPECTED_UNLOCKS_ARE_NOT_MEASURED_UNLOCKS`` is inherited True).
3. **Does native contract-level data contain robust predictive or economic
   alpha the ETF/proxy research could not see?** Measured with frozen designs,
   lane-correct controls, traded-notional costs and multiple-testing control.
4. **Does the data justify keeping/renewing the subscription?** Answered by the
   ONE canonical gate ``engine.data_expansion_gate`` in its
   ``POST_ACQUISITION_VALUE`` context - never by a gate defined here.
5. **What clean foundation should the modern-ML challenger consume next?**

Ownership map - each concern has exactly one owner and the gate is NOT here:

===========================  =================================================
concern                      owner
===========================  =================================================
campaign contract            :mod:`alpha_agent.r38.contract`
entitlement sync proof +
provider-call taxonomy       :mod:`alpha_agent.r38.entitlement`
market/contract enumeration  :mod:`alpha_agent.r38.enumeration`
contract data quality        :mod:`alpha_agent.r38.quality`
dated-contract research layer:mod:`alpha_agent.r38.research_layer`
R36 unlock recomputation     :mod:`alpha_agent.r38.unlock_actual`
frozen experiments           :mod:`alpha_agent.r38.experiments`
ML-ready data contract       :mod:`alpha_agent.r38.ml_contract`
Intrinio/Steele sample ask   :mod:`alpha_agent.r38.steele`
orchestration + verdict      :mod:`alpha_agent.r38.campaign`
===========================  =================================================

**There is no Release-38 purchase gate and no Release-38 coverage authority.**
The renewal/value decision is calculated by ``engine.data_expansion_gate``
through ``api.data_expansion`` (Stage B, ``POST_ACQUISITION_VALUE``); the
Release-36 frozen coverage matrix is read through ``alpha_agent.r36.coverage``
and never re-typed; the Release-37 unlock expectation is read through
``alpha_agent.r37.unlock`` and never re-derived. ``alpha_agent/r38/
purchase_gate.py`` and ``alpha_agent/r38/coverage.py`` are forbidden by the
architecture audit.

Everything else is IMPORTED, never rebuilt: hashing and artifact writing from
:mod:`alpha_agent.r31`; the economic judge, controls and traded-notional cost
model from :mod:`alpha_agent.r34.economics`; multiple testing from
:mod:`alpha_agent.r31.multiple_testing`; the minimum detectable effect from
:mod:`alpha_agent.r36.experiments`.

This package creates NO signal authority, NO portfolio target, NO proposal, NO
decision, NO order, NO model promotion, NO sleeve activation and NO operational
write. It READS the already purchased entitlement and writes immutable research
artifacts under its own research root. It spends nothing and renews nothing.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from ..r31 import (  # noqa: F401  (re-exported: ONE hashing/artifact owner)
    ArtifactImmutable,
    file_fingerprint,
    read_json,
    sha,
    sha_file,
    write_json,
)

RELEASE = "release38"
CAMPAIGN_FAMILY = "native_futures_information_frontier"

#: The isolated research root. NO operational store is ever written by this
#: package; the release regression asserts every written path is under this
#: root, and ``scripts/r33_operational_write_attribution.py`` (R38 profile)
#: proves it against the live operational stores.
RESEARCH_ROOT_ENV = "PAPER_TRADER_R38_RESEARCH_ROOT"
DEFAULT_RESEARCH_ROOT = Path(r"D:\Stock_Prediction_app_data\native_futures_r38")

#: Safety badges every artifact carries.
SAFETY = ["RESEARCH ONLY", "READ ONLY", "PREVIEW ONLY",
          "NO LIVE BROKER ORDERS", "AUTOMATION OFF", "MANUAL REVIEW",
          "NO OPERATIONAL WRITE", "NO MODEL PROMOTION",
          "NO SLEEVE ACTIVATION", "NO PORTFOLIO ACTIVATION",
          "NO PURCHASE", "NO RENEWAL", "NO TRIAL", "NO NEW ACCOUNT",
          "NO SUBSCRIPTION CHANGE"]

#: Declared once, here, so no module can quietly widen them.
AUTOMATIC_PROMOTION_ALLOWED = False
AUTOMATIC_SLEEVE_ACTIVATION_ALLOWED = False
MAY_SPEND_MONEY = False
MAY_MUTATE_PRODUCTION = False


def research_root() -> Path:
    return Path(os.environ.get(RESEARCH_ROOT_ENV) or DEFAULT_RESEARCH_ROOT)


def campaign_dir(campaign_id: str) -> Path:
    return research_root() / str(campaign_id)


def evidence_root() -> Path:
    """Where raw local-entitlement evidence (probe snapshots, updater log
    extracts) lives.

    Outside any campaign directory on purpose: the delivered-entitlement facts
    are inputs to every campaign version, not artifacts of one.
    """
    return research_root() / "phase1_entitlement"


def safety_block() -> dict:
    """The safety declaration carried by every Release-38 artifact.

    Every flag is FALSE. The commercial flags are explicit because this release
    is the first to RUN ON a paid entitlement someone else purchased: reading
    the purchased data is allowed, and every commercial mutation is not.
    """
    return {"safety": list(SAFETY),
            "automatic_promotion_allowed": AUTOMATIC_PROMOTION_ALLOWED,
            "automatic_sleeve_activation_allowed":
                AUTOMATIC_SLEEVE_ACTIVATION_ALLOWED,
            "may_spend_money": MAY_SPEND_MONEY,
            "may_mutate_production": MAY_MUTATE_PRODUCTION,
            "creates_signal_authority": False,
            "creates_portfolio_target": False,
            "creates_capital_allocation": False,
            "creates_proposal": False,
            "creates_decision": False,
            "creates_order": False,
            "activates_sleeve": False,
            "activates_portfolio": False,
            "promotes_model": False,
            "mutates_holdings": False,
            "mutates_cash": False,
            "enables_automation": False,
            "restarts_production": False,
            "writes_operational_store": False,
            "changes_scheduler": False,
            "integrates_broker": False,
            "purchases_data": False,
            "renews_subscription": False,
            "starts_provider_trial": False,
            "creates_provider_account": False,
            "changes_subscription_tier": False,
            "accepts_licence_agreement": False,
            "submits_payment_details": False,
            "spends_cloud_compute": False,
            "installs_cuda": False,
            "trains_a_model": False}


def artifact_body(schema: str, payload: dict, **extra: Any) -> dict:
    """Wrap one artifact payload with its schema, owner and safety block."""
    body = {"schema": schema, "release": RELEASE}
    body.update(payload)
    body.update(extra)
    body["safety_block"] = safety_block()
    return body


__all__ = [
    "RELEASE", "CAMPAIGN_FAMILY", "RESEARCH_ROOT_ENV", "DEFAULT_RESEARCH_ROOT",
    "SAFETY", "AUTOMATIC_PROMOTION_ALLOWED",
    "AUTOMATIC_SLEEVE_ACTIVATION_ALLOWED", "MAY_SPEND_MONEY",
    "MAY_MUTATE_PRODUCTION",
    "research_root", "campaign_dir", "evidence_root", "safety_block",
    "artifact_body",
    "sha", "sha_file", "file_fingerprint", "write_json", "read_json",
    "ArtifactImmutable",
]
