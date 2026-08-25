"""alpha_agent.r43 - Release 43, the GLOBAL ALPHA OFFENSIVE.

RESEARCH ONLY.

Release 42 finished with a result that is easy to misread. It priced the
crypto perpetual-funding carry for the capital it immobilises and the cash
that capital forgoes, and the edge disappeared. The tempting generalisation
- "capital cost kills carry" - is FALSE, and Release 43 exists partly to
say so precisely:

    R42's capital kill is a property of FULLY-FUNDED, NON-INTEREST-BEARING
    COLLATERAL, not of carry. A cash-and-carry in coin immobilises 135% of
    notional in assets paying zero. An exchange-traded futures spread
    immobilises SPAN margin in T-bills that PAY the risk-free rate. The
    correct capital treatment of the second is a RESCALE of the return on
    capital, not a subtraction of the risk-free rate.

Getting this wrong in either direction is expensive. Scoring a coin carry
against zero (R41) invents the whole risk-free rate. Charging a
margin-financed futures book the risk-free rate (a naive reading of R42)
destroys real edges that do not exist. The judge in :mod:`judge` therefore
makes the collateral remuneration an EXPLICIT, per-asset-class, predeclared
contract term, and every candidate in this release is quoted BOTH on traded
notional (R41's convention, so results are comparable) and on committed
capital against its correct control.

The second correction is structural. R41 searched broadly and judged
per-notional; R42 judged deeply and searched one family. Release 43 does
both: thirteen predeclared research lanes across rates, commodities, FX,
volatility, equities, cross-asset relations and market structure, each
ending in EXECUTED or one of the contract's blocker states, all scored by
ONE economic judge, all charged to ONE never-reset search-burden ledger
that starts at the 289 effective trials this estate has actually spent.

Ownership map - every already-owned concern is IMPORTED from its canonical
owner and never re-implemented here:

===============================  =============================================
concern                          owner
===============================  =============================================
release contract (frozen)        :mod:`alpha_agent.r43.contract`
search burden (global + family)  :mod:`alpha_agent.r43.burden`
universal economic judge         :mod:`alpha_agent.r43.judge`
owned panels / loaders           :mod:`alpha_agent.r43.panels`
Track A capital-adjusted carry   :mod:`alpha_agent.r43.carry`
Tracks E/F curve relative value  :mod:`alpha_agent.r43.rv`
Tracks H/I/J cross-asset+events  :mod:`alpha_agent.r43.crossasset`
Tracks B/C/D/G/M/T acquisition   :mod:`alpha_agent.r43.acquisition`
Track Q alpha killer             :mod:`alpha_agent.r43.killer`
Tracks P/R/S/U frontier+freeze   :mod:`alpha_agent.r43.frontier`
orchestration + twenty answers   :mod:`alpha_agent.r43.campaign`
------------------------------   ---------------------------------------------
hashing / immutable artifacts    :mod:`alpha_agent.r31` (via r41)
BH / SPA / deflated Sharpe       :mod:`alpha_agent.r31.multiple_testing`
zones / HAC / scorecards         :mod:`alpha_agent.r41.evidence`
dated-contract curve state       :mod:`alpha_agent.r41.curve_state`
crypto capital equation          :mod:`alpha_agent.r42.capital`
research-shadow ledger           :mod:`alpha_agent.r39.research_shadow`
===============================  =============================================

This package creates NO signal authority, NO portfolio target, NO proposal,
NO decision, NO order, NO paper order, NO model promotion, NO sleeve
activation, NO scheduler change, NO production restart and NO operational
write. It spends $0, creates no account, starts no trial, accepts no licence
on the operator's behalf and purchases nothing.
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

RELEASE = "release43"
CAMPAIGN_FAMILY = "global_alpha_offensive"
CAMPAIGN_ID = "r43_global_alpha_offensive_v1"

RESEARCH_ROOT_ENV = "PAPER_TRADER_R43_RESEARCH_ROOT"
DEFAULT_RESEARCH_ROOT = Path(
    r"D:\Stock_Prediction_app_data\global_alpha_offensive_r43")

#: Owned research roots this release READS and never writes.
R41_RESEARCH_ROOT = Path(
    r"D:\Stock_Prediction_app_data\multi_horizon_alpha_r41")
R42_RESEARCH_ROOT = Path(r"D:\Stock_Prediction_app_data\crypto_basis_r42")
R38_RESEARCH_ROOT = Path(
    r"D:\Stock_Prediction_app_data\native_futures_r38"
    r"\r38_native_futures_information_frontier_v4")

LARGE_FILE_DRIVE = "D:"

SAFETY = list(_r39.SAFETY) + [
    "NO SCHEDULER CHANGE", "NO BACKDATED FORWARD ROW", "NO PAID SAMPLE",
    "NO PAID COMPUTE", "NO OPERATIONAL WRITE", "NO PROMOTION",
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
    "R38_RESEARCH_ROOT", "LARGE_FILE_DRIVE", "SAFETY",
    "AUTOMATIC_PROMOTION_ALLOWED", "AUTOMATIC_SLEEVE_ACTIVATION_ALLOWED",
    "MAY_SPEND_MONEY", "MAY_MUTATE_PRODUCTION", "TRAINS_MODELS",
    "PROMOTES_MODELS", "CHANGES_SCHEDULER",
    "research_root", "campaign_dir", "data_dir", "safety_block",
    "artifact_body", "write_artifact",
    "sha", "sha_file", "file_fingerprint", "write_json", "read_json",
    "ArtifactImmutable",
]
