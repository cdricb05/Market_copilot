"""alpha_agent.r34 - Release 34, the Prediction-to-PnL Conversion sprint.

RESEARCH ONLY. Release 33 returned ``SYSTEM_RESULT = PASS`` and
``ALPHA_RESULT = FAIL``: 46 of 105 configurations survived Benjamini-Hochberg on
validation forecast score, and ZERO of the economically testable configurations
beat a volatility-matched control. The only attractive lockbox economics were
dominated by ``TRYUSD`` and reversed when that single market was removed.

Release 34 asks ONE question:

    Can verified predictive information be converted into robust,
    implementable, after-cost excess PnL?

It is not another feature search. The predictive families are FROZEN from
Release 33 and the release tests the CONVERSION LAYER between a forecast and a
portfolio:

    forecast skill -> expected-return calibration -> forecast uncertainty
    -> position size -> cross-asset risk -> turnover/cost
    -> implementable portfolio -> after-cost excess PnL

Three things are different from Release 33, and each corrects a measured defect
rather than a matter of taste:

* **The universe is actually implementable.** R33's panel of spot FX and
  economic indices was ``SIGNAL_RESEARCH_VALID`` and never
  ``FUTURES_IMPLEMENTABILITY_PROVEN``. R34 trades exchange-listed US ETFs on
  TOTAL-RETURN adjusted prices, selected by measured rule from an enumeration
  that includes DELISTED products, so the universe carries no survivorship
  selection and no instrument is a non-investable index.
* **Horizons are not ranked by raw metric magnitude.** R33's finalists were
  biased toward ``h=60`` because a 60-session rank IC is mechanically larger
  and rests on fewer observations. R34 declares a horizon-normalised evidence
  score BEFORE evaluation.
* **Single-instrument dependency is impossible to hide.** Leave-one-instrument-
  out and leave-one-asset-class-out run for every finalist against thresholds
  frozen in the contract before any economic result is observed.

Ownership map - each concern has exactly one owner:

===========================  =================================================
concern                      owner
===========================  =================================================
campaign contract            :mod:`alpha_agent.r34.contract`
implementable universe       :mod:`alpha_agent.r34.universe`
price panel + returns        :mod:`alpha_agent.r34.panel`
forecast models (FROZEN)     :mod:`alpha_agent.r34.forecast`
expected-return calibration  :mod:`alpha_agent.r34.calibration`
position sizing              :mod:`alpha_agent.r34.sizing`
horizon combination          :mod:`alpha_agent.r34.horizon`
turnover / cost control      :mod:`alpha_agent.r34.turnover`
portfolio construction       :mod:`alpha_agent.r34.portfolio`
concentration gates          :mod:`alpha_agent.r34.concentration`
nested walk-forward          :mod:`alpha_agent.r34.walkforward`
attrition waterfall          :mod:`alpha_agent.r34.attrition`
orchestration + verdict      :mod:`alpha_agent.r34.campaign`
===========================  =================================================

Hashing, artifact writing and the multiple-testing statistics are IMPORTED from
:mod:`alpha_agent.r31`. The feature families and the model families are
IMPORTED from :mod:`alpha_agent.r33`. Release 34 adds a conversion layer, not a
second statistics library, a second feature library or a second artifact format.

This package creates NO signal authority, NO portfolio target, NO proposal, NO
decision, NO order, NO model promotion, NO sleeve activation and NO operational
write. It reads owned data and writes immutable research artifacts under its own
research root.
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

RELEASE = "release34"
CAMPAIGN_FAMILY = "prediction_to_pnl"

#: The isolated research root. NO operational store is ever written by this
#: package; ``tests/test_release34_prediction_to_pnl.py`` asserts that every
#: path this package writes is under this root, and the canonical
#: operational-write attribution rule added in Release 33
#: (``scripts/r34_operational_write_attribution.py``'s owner,
#: ``scripts/r33_operational_write_attribution.py``) proves it against the live
#: operational stores.
RESEARCH_ROOT_ENV = "PAPER_TRADER_R34_RESEARCH_ROOT"
DEFAULT_RESEARCH_ROOT = Path(
    r"D:\Stock_Prediction_app_data\prediction_to_pnl_r34")

#: Safety badges every artifact carries. ``NO LIVE BROKER ORDERS`` is the
#: canonical Phase 27B.6 wording: paper orders are REAL and exist in the
#: operational book under a governed, manually reviewed workflow; only LIVE
#: BROKERAGE orders are structurally disabled.
SAFETY = ["RESEARCH ONLY", "READ ONLY", "PREVIEW ONLY",
          "NO LIVE BROKER ORDERS", "AUTOMATION OFF", "MANUAL REVIEW",
          "NO OPERATIONAL WRITE", "NO MODEL PROMOTION",
          "NO SLEEVE ACTIVATION", "NO PORTFOLIO ACTIVATION"]

#: Declared once, here, so no module can quietly widen them.
AUTOMATIC_PROMOTION_ALLOWED = False
AUTOMATIC_SLEEVE_ACTIVATION_ALLOWED = False
MAY_SPEND_MONEY = False
MAY_MUTATE_PRODUCTION = False


def research_root() -> Path:
    return Path(os.environ.get(RESEARCH_ROOT_ENV) or DEFAULT_RESEARCH_ROOT)


def campaign_dir(campaign_id: str) -> Path:
    return research_root() / str(campaign_id)


def safety_block() -> dict:
    """The safety declaration carried by every Release-34 artifact.

    Every flag is FALSE and is asserted false by the architecture audit. The
    list is exhaustive on purpose: a reader should not have to infer that a
    research package did not promote a model or mutate the operational book.
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
    "MAY_MUTATE_PRODUCTION",
    "research_root", "campaign_dir", "safety_block", "artifact_body",
    "sha", "sha_file", "file_fingerprint", "write_json", "read_json",
    "ArtifactImmutable",
]
