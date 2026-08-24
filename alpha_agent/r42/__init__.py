"""alpha_agent.r42 - Release 42, the Crypto Funding/Basis Alpha Validation
and Execution-Reality Campaign.

Release 41 produced the strongest research candidate in the estate's
history: a delta-neutral BTC spot / perpetual-futures funding-basis carry
book with Zone-B t 10.2 (+8.7 %/yr) and Zone-C t 6.91 (+3.15 %/yr,
Sharpe 7.8), 0 alpha-killer sign reversals, and an exact-parameter
out-of-asset replication on ETH (Zone-B t 9.47, Zone-C t 4.50). R41
correctly did NOT call it qualified Alpha - its frozen family-level
deflated-Sharpe gate failed.

Release 42 does not search for another candidate. It prosecutes this one:

    IS THIS A REAL STRUCTURAL ALPHA PREMIUM, OR IS THE BACKTEST HIDING
    EXECUTION, FINANCING, CAPITAL, VENUE, SELECTION OR STATISTICAL RISK?

The R41 shadow ``shadow_btc_funding_carry_1d`` is IMMUTABLE. Nothing in
this package may change its parameters, windows, threshold, cadence, cost
model, signal timing, history, forward evidence or candidate id. Every
correction, alternative implementation and improved economic model
receives a NEW Release-42 identity.

Ownership map - each concern has exactly one owner here; every
already-owned concern (hashing/immutability, multiple testing, HAC
economics, zones/scorecards, the desk ledger primitives, the research
shadow capture owner, free-sample acquisition) is IMPORTED from its
canonical owner and never re-implemented:

===============================  =============================================
concern                          owner
===============================  =============================================
release contract (frozen first)  :mod:`alpha_agent.r42.contract`
R41 closeout verification        :mod:`alpha_agent.r42.closeout_import`
$0 public data acquisition       :mod:`alpha_agent.r42.acquisition`
exact R41 PnL reconstruction     :mod:`alpha_agent.r42.pnl_audit`
event-exact funding cashflow     :mod:`alpha_agent.r42.funding_ledger`
funding / basis / execution      :mod:`alpha_agent.r42.basis`
positive vs negative leg         :mod:`alpha_agent.r42.legs`
capital denominator / ROIC       :mod:`alpha_agent.r42.capital`
execution reality                :mod:`alpha_agent.r42.execution`
margin / liquidation / path      :mod:`alpha_agent.r42.margin`
venue implementability + venues  :mod:`alpha_agent.r42.venues`
frozen asset universe            :mod:`alpha_agent.r42.asset_universe`
regulated CME basis              :mod:`alpha_agent.r42.cme_basis`
hierarchical statistics          :mod:`alpha_agent.r42.hierarchy`
unconditional vs timing          :mod:`alpha_agent.r42.attribution`
capacity                         :mod:`alpha_agent.r42.capacity`
collateral / stablecoin risk     :mod:`alpha_agent.r42.collateral`
forward capture + R42 shadows    :mod:`alpha_agent.r42.forward`
bounded maker-execution check    :mod:`alpha_agent.r42.microstructure_check`
orchestration + verdict          :mod:`alpha_agent.r42.campaign`
===============================  =============================================

This package creates NO signal authority, NO portfolio target, NO proposal,
NO decision, NO order, NO paper order, NO model promotion, NO sleeve
activation, NO scheduler change, NO production restart and NO operational
write. It spends $0, creates no exchange or provider account, deposits
nothing, holds no API trading key, starts no trial, accepts no licence on
the operator's behalf and purchases nothing. It may read FREE, PUBLIC,
account-free venue endpoints and archives to the research drive under the
conditions in :mod:`contract`.
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

RELEASE = "release42"
CAMPAIGN_FAMILY = "crypto_basis_alpha_validation"
CAMPAIGN_ID = "r42_crypto_basis_alpha_validation_v1"

RESEARCH_ROOT_ENV = "PAPER_TRADER_R42_RESEARCH_ROOT"
DEFAULT_RESEARCH_ROOT = Path(r"D:\Stock_Prediction_app_data\crypto_basis_r42")

#: The R41 research root is READ-ONLY to this package: R41 artifacts, the
#: R41 Binance archive and the R41 frozen shadow are evidence, never
#: outputs. Nothing here writes under it except the R41-owned forward
#: capture, which is delegated to :mod:`alpha_agent.r41.forward_freeze`.
R41_RESEARCH_ROOT = Path(
    r"D:\Stock_Prediction_app_data\multi_horizon_alpha_r41")
R41_CAMPAIGN_ID = "r41_multi_horizon_alpha_breakthrough_v1"

LARGE_FILE_DRIVE = "D:"

SAFETY = list(_r39.SAFETY) + [
    "NO SCHEDULER CHANGE", "NO BACKDATED FORWARD ROW", "NO PAID SAMPLE",
    "NO PAID COMPUTE", "NO CRYPTO PURCHASE", "NO EXCHANGE ACCOUNT",
    "NO DEPOSIT", "NO WITHDRAWAL", "NO API TRADING KEY", "NO PAPER ORDER",
    "NO R41 SHADOW MUTATION",
]

AUTOMATIC_PROMOTION_ALLOWED = False
AUTOMATIC_SLEEVE_ACTIVATION_ALLOWED = False
MAY_SPEND_MONEY = False
MAY_MUTATE_PRODUCTION = False
TRAINS_MODELS = False
PROMOTES_MODELS = False
CHANGES_SCHEDULER = False


def research_root() -> Path:
    return Path(os.environ.get(RESEARCH_ROOT_ENV) or DEFAULT_RESEARCH_ROOT)


def campaign_dir(campaign_id: str = CAMPAIGN_ID) -> Path:
    d = research_root() / str(campaign_id)
    _r39.register_campaign_root(campaign_id, research_root())
    return d


def data_dir(name: str) -> Path:
    """A named large-data directory under the R42 research root."""
    d = research_root() / ("_data_%s" % name)
    d.mkdir(parents=True, exist_ok=True)
    return d


def r41_campaign_dir() -> Path:
    """READ-ONLY view of the R41 campaign directory."""
    return R41_RESEARCH_ROOT / R41_CAMPAIGN_ID


def r41_data_dir(name: str) -> Path:
    """READ-ONLY view of an R41 acquired-data directory (e.g. binance)."""
    return R41_RESEARCH_ROOT / ("_data_%s" % name)


def safety_block() -> dict:
    body = _r39.safety_block()
    body["safety"] = list(SAFETY)
    body["changes_scheduler"] = CHANGES_SCHEDULER
    body["downloads_free_public_samples"] = "ONLY_UNDER_CONTRACT_CONDITIONS"
    body["backdates_forward_rows"] = False
    body["purchases_data"] = False
    body["spends_cloud_compute"] = False
    body["trains_a_model"] = TRAINS_MODELS
    body["buys_crypto"] = False
    body["creates_exchange_account"] = False
    body["deposits_funds"] = False
    body["withdraws_funds"] = False
    body["holds_api_trading_key"] = False
    body["creates_paper_order"] = False
    body["mutates_r41_shadow"] = False
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
    "DEFAULT_RESEARCH_ROOT", "R41_RESEARCH_ROOT", "R41_CAMPAIGN_ID",
    "LARGE_FILE_DRIVE", "SAFETY", "AUTOMATIC_PROMOTION_ALLOWED",
    "AUTOMATIC_SLEEVE_ACTIVATION_ALLOWED", "MAY_SPEND_MONEY",
    "MAY_MUTATE_PRODUCTION", "TRAINS_MODELS", "PROMOTES_MODELS",
    "CHANGES_SCHEDULER", "research_root", "campaign_dir", "data_dir",
    "r41_campaign_dir", "r41_data_dir", "safety_block", "artifact_body",
    "write_artifact", "sha", "sha_file", "file_fingerprint", "write_json",
    "read_json", "ArtifactImmutable",
]
