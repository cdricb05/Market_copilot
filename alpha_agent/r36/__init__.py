"""alpha_agent.r36 - Release 36, the Global Multi-Asset Alpha Frontier sprint.

RESEARCH ONLY. Five releases have now looked for investable alpha and none has
found it. Each looked somewhere specific, and the honest reading of the five
together is not "there is no alpha" but "we have not looked in most places":

* **R31** searched the mathematical/price/fundamental frontier on US equities.
* **R32** tested six simple multi-asset sleeves and found risk premia, not skill.
* **R33** predicted 66 markets across 6 asset classes - and had to label the
  whole universe ``SIGNAL_RESEARCH_VALID``, because the owned Continuous Futures
  entitlement contains exactly ONE market.
* **R34** made the universe implementable by moving to 47 US-listed ETFs, proved
  a real forecast (rank IC 0.0647, t 3.39) and proved it does not convert.
* **R35** bought six new free information families and proved none of them adds
  anything conditional on what R34 already knew.

R34 and R35 are both statements about **one decision problem**: a 47-fund
cross-section rebalanced monthly. They are not statements about the FX market,
the commodity curve, the rates curve, the credit market or the volatility
surface. ``FXE`` is not the FX market, ``USO`` is not the WTI futures curve,
``HYG`` is not credit, ``TLT`` is not the Treasury curve, and VIX as a feature
is not a volatility sleeve. Release 36 exists because that distinction had never
been written down, let alone measured.

So this release has two inseparable jobs.

**One: tell the truth about the global frontier.** Every asset class x strategy
family cell gets a TERMINAL state backed by a measurement - what native market
exists, what would implement it, what point-in-time history is owned, what is
free, what needs money, what has been tested, what was only ever tested through
a proxy, and what the blocker actually is. No cell is allowed to end in
"interesting" or "future work".

**Two: execute the highest-value untested NATIVE lanes.** An inventory is not a
release. Where owned or free point-in-time data supports a genuine native
market, Release 36 builds the instrument, freezes an economically interpretable
strategy, and judges it after cost against a control appropriate to THAT asset
class - never against SPY plus cash.

Three levels are kept apart everywhere, because collapsing them is how a proxy
result silently closes a native frontier:

===========  ==========================  =====================================
level        meaning                     example
===========  ==========================  =====================================
1 SIGNAL     predictive, not tradable    a yield series, a spot FX rate
2 PROXY      tradable, structure lost    ``USO``, ``FXE``, ``HYG``, ``TLT``
3 NATIVE     the market itself           an FX forward, a dated NYMEX contract
===========  ==========================  =====================================

Ownership map - each concern has exactly one owner:

===========================  =================================================
concern                      owner
===========================  =================================================
campaign contract            :mod:`alpha_agent.r36.contract`
entitlement measurement      :mod:`alpha_agent.r36.entitlements`
external acquisition         :mod:`alpha_agent.r36.acquisition`
raw -> PIT native instrument :mod:`alpha_agent.r36.native_markets`
frozen strategy rules        :mod:`alpha_agent.r36.strategies`
per-config execution spine   :mod:`alpha_agent.r36.experiments`
global coverage matrix       :mod:`alpha_agent.r36.coverage`
orchestration + verdict      :mod:`alpha_agent.r36.campaign`
===========================  =================================================

Everything else is IMPORTED, never rebuilt: hashing, artifact writing and the
multiple-testing statistics from :mod:`alpha_agent.r31`; the Norgate readers,
the FX quote-direction rule and the market admissibility rules from
:mod:`alpha_agent.r33.universe`; the total-return reader from
:mod:`alpha_agent.r34.universe`; the economic judge, its controls and its
after-cost statistics from :mod:`alpha_agent.r34.economics`; the concentration
and leave-one-out diagnostics from :mod:`alpha_agent.r34.concentration`; the
HTTP acquisition primitive and the point-in-time alignment convention from
:mod:`alpha_agent.r35`; and the rank-correlation primitive from
:mod:`alpha_agent.orthogonality`.

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

RELEASE = "release36"
CAMPAIGN_FAMILY = "global_multi_asset_frontier"

#: The isolated research root. NO operational store is ever written by this
#: package; ``tests/test_release36_global_multi_asset_frontier.py`` asserts that
#: every path this package writes is under this root, and the canonical
#: operational-write attribution rule added in Release 33
#: (``scripts/r33_operational_write_attribution.py``, R36 profile) proves it
#: against the live operational stores.
RESEARCH_ROOT_ENV = "PAPER_TRADER_R36_RESEARCH_ROOT"
DEFAULT_RESEARCH_ROOT = Path(
    r"D:\Stock_Prediction_app_data\global_multi_asset_frontier_r36")

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
    """The safety declaration carried by every Release-36 artifact.

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
            "starts_provider_trial": False,
            "changes_subscription_tier": False}


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
