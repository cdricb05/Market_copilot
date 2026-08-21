"""alpha_agent.r33 - Release 33, the Predictive Edge Acquisition sprint.

RESEARCH ONLY. This package exists to answer one question and to answer it
without euphemism:

    Does any bounded, pre-registered candidate produce BOTH measurable
    out-of-sample predictive improvement over a pre-registered forecast
    baseline AND positive after-cost economic improvement over an appropriate
    risk-matched investable control, on frozen evidence?

Release 32 returned ``R32_ZERO_COST_OPPORTUNITY_FRONTIER_EXHAUSTED``: a
legitimate negative result over 104 bounded hypotheses. Its ``SYSTEM_RESULT``
was PASS and its ``ALPHA_RESULT`` was FAIL, and this package is required to
report those two outcomes separately for Release 33 rather than letting a
completed campaign stand in for an investment result.

What is different here, and it is the point of the release:

* the universe is BROAD and CROSS-MARKET rather than a handful of aggregate
  series - the largest defensible liquid research universe the OWNED data
  supports, across equity indices, government and credit bonds, commodities,
  precious metals and FX;
* the candidates are STATISTICAL LEARNERS with partial pooling across markets,
  not only handcrafted rules;
* qualification requires a FORECAST SCORE improvement first. Economic PnL
  alone cannot qualify anything, because a strategy can earn money by carrying
  risk without predicting anything at all.

Ownership map - each concern has exactly one owner:

===========================  =================================================
concern                      owner
===========================  =================================================
campaign contract            :mod:`alpha_agent.r33.contract`
universe + data inventory    :mod:`alpha_agent.r33.universe`
price panel + returns        :mod:`alpha_agent.r33.panel`
PIT information (Lane B)     :mod:`alpha_agent.r33.pit`
feature registry             :mod:`alpha_agent.r33.features`
forecast targets             :mod:`alpha_agent.r33.targets`
chronological partition      :mod:`alpha_agent.r33.partition`
model families               :mod:`alpha_agent.r33.models`
regime models                :mod:`alpha_agent.r33.regime`
predictive scoring           :mod:`alpha_agent.r33.predictive`
economic judge               :mod:`alpha_agent.r33.economic`
candidate registry           :mod:`alpha_agent.r33.registry`
lockbox discipline           :mod:`alpha_agent.r33.lockbox`
robustness                   :mod:`alpha_agent.r33.robustness`
orchestration + verdict      :mod:`alpha_agent.r33.campaign`
===========================  =================================================

Hashing, artifact writing and the multiple-testing statistics are IMPORTED from
:mod:`alpha_agent.r31`. Release 33 adds prediction problems, not a second
statistics library or a second artifact format.

This package creates NO signal authority, NO portfolio target, NO proposal, NO
decision, NO order, NO model promotion and NO operational write. It reads owned
data plus free point-in-time public sources and writes immutable research
artifacts under its own research root.
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

RELEASE = "release33"
CAMPAIGN_FAMILY = "predictive_edge"

#: The isolated research root. NO operational store is ever written by this
#: package; ``tests/test_release33_predictive_edge.py`` asserts that every path
#: this package writes is under this root.
RESEARCH_ROOT_ENV = "PAPER_TRADER_R33_RESEARCH_ROOT"
DEFAULT_RESEARCH_ROOT = Path(r"D:\Stock_Prediction_app_data\predictive_edge_r33")

#: Safety badges every artifact carries. ``NO LIVE BROKER ORDERS`` is the
#: canonical Phase 27B.6 wording: paper orders are REAL and exist in the
#: operational book under a governed, manually reviewed workflow; only LIVE
#: BROKERAGE orders are structurally disabled.
SAFETY = ["RESEARCH ONLY", "READ ONLY", "PREVIEW ONLY",
          "NO LIVE BROKER ORDERS", "AUTOMATION OFF", "MANUAL REVIEW",
          "NO OPERATIONAL WRITE", "NO MODEL PROMOTION",
          "NO SLEEVE ACTIVATION", "NO FUTURES EXECUTION"]

#: Declared once, here, so no module can quietly widen them.
AUTOMATIC_PROMOTION_ALLOWED = False
AUTOMATIC_SLEEVE_ACTIVATION_ALLOWED = False
MAY_SPEND_MONEY = False


def research_root() -> Path:
    return Path(os.environ.get(RESEARCH_ROOT_ENV) or DEFAULT_RESEARCH_ROOT)


def campaign_dir(campaign_id: str) -> Path:
    return research_root() / str(campaign_id)


def safety_block() -> dict:
    """The safety declaration carried by every Release-33 artifact.

    Every flag is FALSE and is asserted false by the architecture audit. The
    list is exhaustive on purpose: a reader should not have to infer that a
    research package did not promote a model or mutate the operational book.
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
            "writes_operational_store": False,
            "executes_futures": False,
            "integrates_broker": False}


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
