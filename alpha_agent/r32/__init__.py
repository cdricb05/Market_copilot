"""alpha_agent.r32 - Release 32, the PnL Opportunity Frontier campaign.

RESEARCH ONLY. This package answers one question, and it is deliberately not an
equity question:

    "If every investable dollar were cash right now, given everything
     legitimately observable right now, where should capital be deployed to
     maximise expected after-cost, risk-adjusted paper portfolio PnL?"

Release 31 answered the narrower version of that question - the best decision
function from OWNED equity information to an S&P 500 book - and returned
``R31_CURRENT_INFORMATION_MODEL_FRONTIER_EXHAUSTED`` with the dominant
constraint ``INFORMATION_NOT_METHOD``. Release 32 therefore does not search
harder in the same place. It widens the INFORMATION, the PREDICTION TARGET and
the ASSET/STRATEGY OPPORTUNITY SET, and compares every sleeve under ONE common
economic judge, including cash.

It creates NO signal authority, NO portfolio target, NO proposal, NO decision,
NO order, NO sleeve activation and NO model promotion. It reads OWNED data and
writes immutable research artifacts under its own research root.

Ownership map - each concern has exactly one owner in this package:

===========================  =================================================
concern                      owner
===========================  =================================================
campaign contract            :mod:`alpha_agent.r32.contract`
data source registry         :mod:`alpha_agent.r32.sources`
canonical information state  :mod:`alpha_agent.r32.information_state`
cross-asset PIT panels       :mod:`alpha_agent.r32.panels`
strategy sleeve contract     :mod:`alpha_agent.r32.sleeve`
the six sleeves              :mod:`alpha_agent.r32.sleeves`
common economic judge        :mod:`alpha_agent.r32.judge`
bounded research funnel      :mod:`alpha_agent.r32.funnel`
opportunity frontier         :mod:`alpha_agent.r32.frontier`
information purchase gate    :mod:`alpha_agent.r32.purchase_gate`
daily governance contract    :mod:`alpha_agent.r32.governance`
orchestration + verdict      :mod:`alpha_agent.r32.campaign`
===========================  =================================================

There is deliberately no second portfolio optimiser, no second covariance owner,
no second HOC engine, no second cost model, no second event system and no second
forward-evidence system here. Multiple-testing control and the lockbox
DISCIPLINE are reused from :mod:`alpha_agent.r31` rather than reimplemented -
Release 32 adds sleeves, not a second statistics library.

A sleeve in this package GENERATES OPPORTUNITIES. A sleeve never owns capital.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional

from ..r31 import (  # noqa: F401  (re-exported: ONE hashing/artifact owner)
    ArtifactImmutable,
    file_fingerprint,
    read_json,
    sha,
    sha_file,
    write_json,
)

RELEASE = "release32"
CAMPAIGN_FAMILY = "pnl_opportunity_frontier"

#: The isolated research root. NO operational store is ever written by this
#: package; ``tests/test_release32_pnl_opportunity_frontier.py`` asserts that
#: every path this package writes is under this root.
RESEARCH_ROOT_ENV = "PAPER_TRADER_R32_RESEARCH_ROOT"
DEFAULT_RESEARCH_ROOT = Path(
    r"D:\Stock_Prediction_app_data\pnl_opportunity_frontier")

#: Safety badges every artifact carries. ``NO LIVE BROKER ORDERS`` is the
#: canonical Phase 27B.6 wording: paper orders are REAL and exist in the
#: operational book under a governed, manually reviewed workflow; only LIVE
#: BROKERAGE orders are structurally disabled. A badge that says "no orders"
#: without saying which kind understates what this system does.
SAFETY = ["RESEARCH ONLY", "READ ONLY", "PREVIEW ONLY",
          "NO LIVE BROKER ORDERS", "AUTOMATION OFF", "MANUAL REVIEW",
          "NO OPERATIONAL WRITE", "NO MODEL PROMOTION",
          "NO SLEEVE ACTIVATION"]

#: Declared once, here, so no module can quietly widen them.
AUTOMATIC_PROMOTION_ALLOWED = False
AUTOMATIC_SLEEVE_ACTIVATION_ALLOWED = False
MAY_SPEND_MONEY = False


def research_root() -> Path:
    return Path(os.environ.get(RESEARCH_ROOT_ENV) or DEFAULT_RESEARCH_ROOT)


def campaign_dir(campaign_id: str) -> Path:
    return research_root() / str(campaign_id)


def safety_block() -> dict:
    """The safety declaration carried by every Release-32 artifact.

    Every flag is FALSE and is asserted false by the architecture audit. The
    list is exhaustive on purpose: a reader should not have to infer that a
    research package did not activate a sleeve.
    """
    return {"safety": list(SAFETY),
            "automatic_promotion_allowed": AUTOMATIC_PROMOTION_ALLOWED,
            "automatic_sleeve_activation_allowed":
                AUTOMATIC_SLEEVE_ACTIVATION_ALLOWED,
            "may_spend_money": MAY_SPEND_MONEY,
            "creates_signal_authority": False,
            "creates_portfolio_target": False,
            "creates_capital_allocation": False,
            "creates_proposal": False,
            "creates_decision": False,
            "creates_order": False,
            "activates_sleeve": False,
            "promotes_model": False,
            "mutates_holdings": False,
            "mutates_cash": False,
            "enables_automation": False,
            "restarts_production": False,
            "writes_operational_store": False}


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
    "research_root", "campaign_dir", "safety_block", "artifact_body",
    "sha", "sha_file", "file_fingerprint", "write_json", "read_json",
    "ArtifactImmutable",
]
