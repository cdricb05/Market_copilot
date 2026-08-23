"""alpha_agent.r39 - Release 39, the Autonomous Universal Alpha Discovery Engine.

RESEARCH ONLY. Releases 31-38 asked hand-enumerated questions and answered them
honestly - mostly with FAIL. Release 39 removes the IDEA boundary (no artificial
asset, model-family, representation or trade-structure limit) and strengthens
the EVIDENCE boundary (three evidence zones, a locked confirmation window,
search-burden-adjusted statistics, one economic judge, named exclusions).

The central research question:

    GIVEN EVERYTHING THE ESTATE KNOWS POINT-IN-TIME, WHAT PREDICTIVE
    STRUCTURES AND TRADE EXPRESSIONS CAN MACHINE INTELLIGENCE DISCOVER THAT
    PRODUCE ROBUST, AFTER-COST EXCESS RETURN VERSUS THE CORRECT ECONOMIC
    CONTROL?

Search capacity is not evidence. The engine may generate any number of
candidates; a candidate becomes evidence only by surviving the staged budget,
the frozen finalist set, ONE locked confirmation execution, and the
multiple-discovery correction whose denominator counts everything the search
touched.

Ownership map - each concern has exactly one owner in this package:

===========================  =================================================
concern                      owner
===========================  =================================================
campaign contract            :mod:`alpha_agent.r39.contract`
universal data estate        :mod:`alpha_agent.r39.estate`
R38 cell/experiment map      :mod:`alpha_agent.r39.integrity`
universal PIT market state   :mod:`alpha_agent.r39.universal_state`
target factory               :mod:`alpha_agent.r39.target_factory`
trade-space generator        :mod:`alpha_agent.r39.trade_space`
representation factory       :mod:`alpha_agent.r39.representation_factory`
model technology + adapters  :mod:`alpha_agent.r39.model_registry`
evidence zones + lockbox     :mod:`alpha_agent.r39.zones`
autonomous director          :mod:`alpha_agent.r39.discovery_director`
economic judge (delegating)  :mod:`alpha_agent.r39.judge`
search-burden correction     :mod:`alpha_agent.r39.burden`
alpha frontier + combination :mod:`alpha_agent.r39.frontier`
forward handoff + Intrinio   :mod:`alpha_agent.r39.handoff`
orchestration + verdict      :mod:`alpha_agent.r39.campaign`
===========================  =================================================

Reused, never rebuilt: hashing and immutable artifact writing from
:mod:`alpha_agent.r31`; Benjamini-Hochberg, Hansen SPA and the stationary
bootstrap from :mod:`alpha_agent.r31.multiple_testing`; the lockbox BUDGET
constants from :mod:`alpha_agent.r31.contract`; annualisation, Newey-West
excess significance and the volatility-matched control from
:mod:`alpha_agent.r34.economics`. Release 39 defines NO purchase gate, NO
coverage authority and NO second forward-evidence system.

This package creates NO signal authority, NO portfolio target, NO proposal,
NO decision, NO order, NO model promotion, NO sleeve activation and NO
operational write. It reads OWNED data within existing entitlements, spends
nothing, and writes immutable research artifacts under its own research root.
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

RELEASE = "release39"
CAMPAIGN_FAMILY = "universal_alpha_discovery"

#: The isolated research root. NO operational store is ever written by this
#: package; the release regression asserts every written path is under this
#: root, and ``scripts/r33_operational_write_attribution.py`` (R39 profile)
#: proves it against the live operational stores.
RESEARCH_ROOT_ENV = "PAPER_TRADER_R39_RESEARCH_ROOT"
DEFAULT_RESEARCH_ROOT = Path(r"D:\Stock_Prediction_app_data\universal_alpha_r39")

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

#: Release 39 DOES fit models - that is its purpose - and still promotes none.
TRAINS_MODELS = True
PROMOTES_MODELS = False


def research_root() -> Path:
    return Path(os.environ.get(RESEARCH_ROOT_ENV) or DEFAULT_RESEARCH_ROOT)


#: Campaigns of LATER releases that reuse this package's owners (reuse
#: ledger, lockbox, artifact writer) under their OWN research root. A later
#: release registers its campaign here instead of re-implementing the
#: owners; an unregistered campaign id resolves under the R39 root exactly
#: as before, so every R39 path is unchanged.
_EXTERNAL_CAMPAIGN_ROOTS: dict = {}


def register_campaign_root(campaign_id: str, root) -> Path:
    """Bind ``campaign_id`` to ``root/campaign_id`` for every owner in this
    package. Idempotent for the same root; a different root for an already
    bound campaign is refused (a campaign has ONE home)."""
    target = Path(root) / str(campaign_id)
    bound = _EXTERNAL_CAMPAIGN_ROOTS.get(str(campaign_id))
    if bound is not None and Path(bound) != target:
        raise ValueError("campaign %s is already bound to %s"
                         % (campaign_id, bound))
    _EXTERNAL_CAMPAIGN_ROOTS[str(campaign_id)] = target
    return target


def campaign_dir(campaign_id: str) -> Path:
    ext = _EXTERNAL_CAMPAIGN_ROOTS.get(str(campaign_id))
    if ext is not None:
        return Path(ext)
    return research_root() / str(campaign_id)


def state_dir(campaign_id: str) -> Path:
    """Where the assembled universal-state panels live (parquet/csv/npz).

    Inside the campaign directory: the state is an artifact of one campaign
    version, bound by content hash into the campaign manifest.
    """
    return campaign_dir(campaign_id) / "universal_state"


def safety_block() -> dict:
    """The safety declaration carried by every Release-39 artifact.

    ``trains_a_model`` is True - this release exists to fit models - and every
    promotion, activation, spend and operational-write flag stays False.
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
            "promotes_model": PROMOTES_MODELS,
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
            "trains_a_model": TRAINS_MODELS}


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
    "MAY_MUTATE_PRODUCTION", "TRAINS_MODELS", "PROMOTES_MODELS",
    "research_root", "campaign_dir", "state_dir", "safety_block",
    "artifact_body", "register_campaign_root",
    "sha", "sha_file", "file_fingerprint", "write_json", "read_json",
    "ArtifactImmutable",
]
