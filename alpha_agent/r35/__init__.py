"""alpha_agent.r35 - Release 35, the Orthogonal Information Acquisition sprint.

RESEARCH ONLY. Release 34 returned ``SYSTEM_RESULT = PASS`` and
``ALPHA_RESULT = FAIL``: on a genuinely implementable universe of 47 US-listed
ETFs, the frozen Release-33 model families reproduced real out-of-sample ranking
skill (rank IC 0.0647, t = 3.39 over 233 monthly decisions) and the best of 55
pre-registered conversion configurations turned it into +5.25 %/yr net that a
0.351 x SPY + cash mix earned anyway. After-cost excess +0.002 %/yr, t = 0.004.
The same machinery driven by the realised return earns +37.3 %/yr, so the real
forecast captured 14.9 % of the perfect-information opportunity.

R34's conclusion is the premise of this release: the conversion layer is not the
binding constraint. **The binding constraint is information content.**

Release 35 therefore asks ONE question, and it is a question about DATA:

    Does genuinely NEW, economically orthogonal, point-in-time information add
    incremental out-of-sample predictive information AND incremental after-cost
    risk-matched economic value BEYOND the frozen R34 information set?

The object of study is the INCREMENT:

    NEW INFORMATION -> delta PREDICTION -> delta AFTER-COST RISK-MATCHED PNL

A dataset is not a success because it has a significant standalone relationship
with returns. It is a success only if, conditional on everything R34 already
knew, it changes the forecast and then changes the money.

Three things are different from Release 34, and each is a consequence of the
question rather than a matter of taste:

* **Nothing about the model is searched.** The universe, the panel, the base
  features, the model families, the walk-forward partition and the entire
  conversion configuration are FROZEN from R33/R34 and imported. The only thing
  that varies is WHICH INFORMATION the model is allowed to see. If a number
  moves, information moved it.
* **Orthogonality is measured before prediction, and it is a gate.** A source
  that reproduces momentum, volatility or beta is labelled REDUNDANT on the
  measured residual share of its variance conditional on the 28 base features -
  not on raw correlation, which is the wrong test and the easy one to pass.
* **Acquisition is real.** This release does not stop at a provider-planning
  document. Five external information families are downloaded, checksummed,
  stamped with their true publication times and normalised, and the family that
  cannot be acquired is recorded as blocked with a measured gate rather than
  approximated by something that would look like analyst revisions and measure
  something else.

Ownership map - each concern has exactly one owner:

===========================  =================================================
concern                      owner
===========================  =================================================
campaign contract            :mod:`alpha_agent.r35.contract`
external acquisition         :mod:`alpha_agent.r35.acquisition`
raw -> PIT normalised        :mod:`alpha_agent.r35.information`
new information features     :mod:`alpha_agent.r35.features`
base (+) new design matrix   :mod:`alpha_agent.r35.design`
redundancy / distinctness    :mod:`alpha_agent.r35.orthogonality`
paired predictive increment  :mod:`alpha_agent.r35.incremental`
analyst lane + purchase gate :mod:`alpha_agent.r35.analyst_lane`
orchestration + verdict      :mod:`alpha_agent.r35.campaign`
===========================  =================================================

Everything else is IMPORTED, never rebuilt: hashing, artifact writing and the
multiple-testing statistics from :mod:`alpha_agent.r31`; the feature registry and
the model families from :mod:`alpha_agent.r33`; the implementable universe, the
price panel, the forecast harness, the calibration, sizing, horizon, turnover,
portfolio, economics, concentration and walk-forward owners from
:mod:`alpha_agent.r34`; the residualisation and partial-rank-IC primitives from
:mod:`alpha_agent.orthogonality`; the Information Purchase Gate from
:mod:`alpha_agent.r32.purchase_gate`; the analyst-revision data contract and its
point-in-time validator from :mod:`alpha_agent.analyst_revisions`; and the
point-in-time SIC sector classification from :mod:`alpha_agent.pit_sector`.

This package creates NO signal authority, NO portfolio target, NO proposal, NO
decision, NO order, NO model promotion, NO sleeve activation and NO operational
write. It reads owned data, downloads free public data, and writes immutable
research artifacts under its own research root.
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

RELEASE = "release35"
CAMPAIGN_FAMILY = "orthogonal_information"

#: The isolated research root. NO operational store is ever written by this
#: package; ``tests/test_release35_orthogonal_information.py`` asserts that every
#: path this package writes is under this root, and the canonical
#: operational-write attribution rule added in Release 33
#: (``scripts/r33_operational_write_attribution.py``, R35 profile) proves it
#: against the live operational stores.
RESEARCH_ROOT_ENV = "PAPER_TRADER_R35_RESEARCH_ROOT"
DEFAULT_RESEARCH_ROOT = Path(
    r"D:\Stock_Prediction_app_data\orthogonal_information_r35")

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


def acquisition_root() -> Path:
    """Where downloaded RAW third-party payloads live.

    Outside any campaign directory on purpose: a raw vendor archive is an input
    to every campaign version, not an artifact of one, and re-downloading it for
    a superseding run would change bytes that must not change.
    """
    return research_root() / "acquired"


def safety_block() -> dict:
    """The safety declaration carried by every Release-35 artifact.

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
            "integrates_broker": False,
            "purchases_data": False,
            "starts_provider_trial": False}


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
    "research_root", "campaign_dir", "acquisition_root", "safety_block",
    "artifact_body",
    "sha", "sha_file", "file_fingerprint", "write_json", "read_json",
    "ArtifactImmutable",
]
