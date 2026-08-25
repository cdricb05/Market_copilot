"""alpha_agent.r44 - Release 44, ORTHOGONAL INFORMATION x PORTFOLIO ALPHA.

RESEARCH ONLY.

Releases 31-43 searched for A SIGNAL. Thirteen R43 lanes, 302 effective
trials, and the same sentence three times over: *the premium is real and
the timing rule laid on top of it is worth zero*. R42's crypto book, every
R43 carry family and the R43 rates candidate all ended there.

Release 44 stops asking the question that keeps returning the same answer
and asks three different ones, together:

    ENGINE 1  Is there information the estate has never actually held?
              Options surfaces, native intraday event-time prices, analyst
              expectation vintages, native credit, deeper microstructure.

    ENGINE 2  Do several INDEPENDENTLY WEAK residual streams combine into a
              portfolio that is strong, even though no single sleeve is?
              And - the question that decides whether the answer means
              anything - does that portfolio beat a STRUCTURAL-PREMIUM
              CONTROL built the same way from the premia alone?

    ENGINE 3  Do less-crowded, lower-capacity markets carry a better frontier
              than the very liquid markets that dominated R31-R43? A book
              with $500k of credible capacity is a real answer here.

The scientific hazard of Engine 2 is obvious and is designed against
explicitly. A portfolio assembled from streams that were SELECTED because
they scored well is not a portfolio test; it is the same search wearing a
different hat. Release 44 therefore:

  * defines the stream inventory by ECONOMICS, in the frozen contract,
    BEFORE any stream is scored - and includes the streams R43 already
    killed, because excluding losers is the selection bias;
  * predeclares eight combination rules and names ONE as primary before
    the lockbox is opened;
  * fits every weight on ZONE_A+ZONE_B only and opens ZONE_C exactly once;
  * charges the portfolio-synthesis experiment its own search burden, on
    top of the inherited 302, because combination is a searched family too.

Ownership map - every already-owned concern is IMPORTED from its canonical
owner and never re-implemented here:

===============================  =============================================
concern                          owner
===============================  =============================================
release contract (frozen)        :mod:`alpha_agent.r44.contract`
R43 closeout verification        :mod:`alpha_agent.r44.closeout`
search burden (global + family)  :mod:`alpha_agent.r44.burden`
residual stream inventory        :mod:`alpha_agent.r44.streams`
predeclared combination rules    :mod:`alpha_agent.r44.combine`
structural-premium control       :mod:`alpha_agent.r44.control`
portfolio qualification battery  :mod:`alpha_agent.r44.portfolio`
Engine 1A option surface         :mod:`alpha_agent.r44.options`
Engine 1B intraday event time    :mod:`alpha_agent.r44.intraday`
Engine 1C/1D/1E data lanes       :mod:`alpha_agent.r44.acquisition`
Engine 3 less-efficient markets  :mod:`alpha_agent.r44.niche`
purchase gate                    :mod:`alpha_agent.r44.purchase`
frontier + freeze                :mod:`alpha_agent.r44.frontier`
orchestration + final answers    :mod:`alpha_agent.r44.campaign`
------------------------------   ---------------------------------------------
universal economic judge         :mod:`alpha_agent.r43.judge`
owned panels / loaders           :mod:`alpha_agent.r43.panels`
carry / RV / cross-asset books   :mod:`alpha_agent.r43.carry`, `.rv`,
                                 :mod:`alpha_agent.r43.crossasset`,
                                 :mod:`alpha_agent.r43.equity`
alpha killer battery             :mod:`alpha_agent.r43.killer`
hashing / immutable artifacts    :mod:`alpha_agent.r31` (via r41)
BH / SPA / deflated Sharpe       :mod:`alpha_agent.r31.multiple_testing`
zones / HAC / scorecards         :mod:`alpha_agent.r41.evidence`
dated-contract curve state       :mod:`alpha_agent.r41.curve_state`
crypto capital equation          :mod:`alpha_agent.r42.capital`
research-shadow ledger           :mod:`alpha_agent.r39.research_shadow`
===============================  =============================================

This package creates NO signal authority, NO portfolio target, NO proposal,
NO decision, NO allocation, NO order, NO paper order, NO model promotion, NO
sleeve activation, NO scheduler change, NO production restart and NO
operational write. It spends $0, creates no account, starts no trial,
accepts no licence on the operator's behalf and purchases nothing.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from ..r41 import (  # noqa: F401  (re-exported: ONE hashing/artifact owner)
    ArtifactImmutable,
    file_fingerprint,
    read_json,
    sha,
    sha_file,
    write_json,
)
from .. import r39 as _r39

RELEASE = "release44"
CAMPAIGN_FAMILY = "orthogonal_portfolio_alpha"
CAMPAIGN_ID = "r44_orthogonal_portfolio_alpha_v1"

RESEARCH_ROOT_ENV = "PAPER_TRADER_R44_RESEARCH_ROOT"
DEFAULT_RESEARCH_ROOT = Path(
    r"D:\Stock_Prediction_app_data\orthogonal_portfolio_alpha_r44")

#: Owned research roots this release READS and never writes.
R41_RESEARCH_ROOT = Path(
    r"D:\Stock_Prediction_app_data\multi_horizon_alpha_r41")
R42_RESEARCH_ROOT = Path(r"D:\Stock_Prediction_app_data\crypto_basis_r42")
R43_RESEARCH_ROOT = Path(
    r"D:\Stock_Prediction_app_data\global_alpha_offensive_r43")
R38_RESEARCH_ROOT = Path(
    r"D:\Stock_Prediction_app_data\native_futures_r38"
    r"\r38_native_futures_information_frontier_v4")

LARGE_FILE_DRIVE = "D:"

SAFETY = list(_r39.SAFETY) + [
    "NO SCHEDULER CHANGE", "NO BACKDATED FORWARD ROW", "NO PAID SAMPLE",
    "NO PAID COMPUTE", "NO OPERATIONAL WRITE", "NO PROMOTION",
    "NO CAPITAL ALLOCATION", "NO PORTFOLIO MUTATION",
    "READ-ONLY AGAINST EVERY PRIOR RELEASE ROOT",
]

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
    d = research_root() / str(campaign_id)
    _r39.register_campaign_root(campaign_id, research_root())
    return d


def data_dir(name: str) -> Path:
    d = research_root() / ("_data_%s" % name)
    d.mkdir(parents=True, exist_ok=True)
    return d


def safety_block() -> dict:
    body = _r39.safety_block()
    body["safety"] = list(SAFETY)
    body["changes_scheduler"] = CHANGES_SCHEDULER
    body["backdates_forward_rows"] = False
    body["purchases_data"] = False
    body["spends_cloud_compute"] = False
    body["mutates_prior_release_artifacts"] = False
    body["creates_capital_allocation"] = False
    body["mutates_portfolio"] = False
    return body


def artifact_body(schema: str, payload: dict, **extra: Any) -> dict:
    body = {"schema": schema, "release": RELEASE, "campaign_id": CAMPAIGN_ID}
    body.update(payload)
    body.update(extra)
    body["safety_block"] = safety_block()
    return body


def write_artifact(name: str, body: dict, campaign_id: str = CAMPAIGN_ID,
                   *, overwrite: bool = False) -> Path:
    path = campaign_dir(campaign_id) / name
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not overwrite:
        existing = read_json(path)
        if existing is not None:
            return path
    write_json(path, body, immutable=not overwrite)
    return path


__all__ = [
    "RELEASE", "CAMPAIGN_FAMILY", "CAMPAIGN_ID", "RESEARCH_ROOT_ENV",
    "DEFAULT_RESEARCH_ROOT", "R41_RESEARCH_ROOT", "R42_RESEARCH_ROOT",
    "R43_RESEARCH_ROOT", "R38_RESEARCH_ROOT", "LARGE_FILE_DRIVE", "SAFETY",
    "AUTOMATIC_PROMOTION_ALLOWED", "AUTOMATIC_SLEEVE_ACTIVATION_ALLOWED",
    "MAY_SPEND_MONEY", "MAY_MUTATE_PRODUCTION", "TRAINS_MODELS",
    "PROMOTES_MODELS", "CHANGES_SCHEDULER",
    "research_root", "campaign_dir", "data_dir", "safety_block",
    "artifact_body", "write_artifact",
    "sha", "sha_file", "file_fingerprint", "write_json", "read_json",
    "ArtifactImmutable",
]
