"""alpha_agent.r37 - Release 37, the Native-Market Data Expansion & Purchase Gate.

RESEARCH ONLY, and for the first time in six releases the question is not "is
there alpha here?" but **"what should we buy?"**.

Release 36 classified 608 asset x strategy cells and found that **95 of the 200
economically applicable ones are blocked** - not by method, not by statistics,
but by an entitlement, a licence, a point-in-time gap or a survivorship gap.
Roughly half the investable world sat behind a paywall this programme had never
priced. Pricing it is a purchase decision, and it belongs to the operator.

So this release does four things, in strict priority order:

**TRACK A - challenge the global provider market.** Every serious candidate
dataset is recorded with its measured properties, its licence, its cost and the
exact Release-36 cells it would unlock. A marketing page is not a measurement:
``A_MARKETING_CLAIM_IS_NOT_A_MEASUREMENT`` is a constant, and no cell may be
claimed as unlocked unless the dataset's declared instruments actually implement
that market's native instrument.

**TRACK B - acquire and validate real samples at zero cost.** Free public
archives are downloaded, checksummed and validated. Owned entitlements are
re-measured against the vendor's own API rather than against a document. Nothing
is purchased, no trial is started, no account is created, no licence is accepted
and no payment detail is submitted.

**TRACK C - advanced ML/AI readiness (secondary).** The workstation is
inventoried read-only and a model-family readiness matrix plus one canonical
research input/output data contract are declared, so the next dataset is not fed
into the model families this estate happens to already use. **No training
campaign runs here** - ``ML_TRAINING_CAMPAIGN_IN_SCOPE`` is False.

**TRACK D - market-structure / visual-intelligence backlog (secondary).** The
discretionary trading-floor lane - trend structure, swing points, retracements,
Fibonacci - is DESIGNED and recorded, with its anti-hindsight rules and its
placebo controls, and is deliberately **not executed**.

Ownership map - each concern has exactly one owner, and the purchase gate is
NOT one of them:

===========================  =================================================
concern                      owner
===========================  =================================================
campaign contract            :mod:`alpha_agent.r37.contract`
provider / dataset long list :mod:`alpha_agent.r37.providers`
R36 cell unlock arithmetic   :mod:`alpha_agent.r37.unlock`
sample acquisition + proof   :mod:`alpha_agent.r37.samples`
cost / value scoring         :mod:`alpha_agent.r37.scoring`
purchase-gate COMPOSITION    :mod:`alpha_agent.r37.purchase`
compute inventory            :mod:`alpha_agent.r37.compute`
ML readiness + data contract :mod:`alpha_agent.r37.ml_readiness`
market-structure backlog     :mod:`alpha_agent.r37.market_structure`
orchestration + verdict      :mod:`alpha_agent.r37.campaign`
===========================  =================================================

**There is no Release-37 purchase gate.** The dataset purchase/integration
decision is calculated by the released Slice-9 kernel
:mod:`paper_trader.engine.data_expansion_gate` through its composition owner
:mod:`paper_trader.api.data_expansion`, and the ten-condition information gate
remains :mod:`alpha_agent.r32.purchase_gate`. This package composes both and
defines neither; ``alpha_agent/r37/purchase_gate.py`` is forbidden by the
architecture audit precisely so a fourth gate cannot appear by accident.

Everything else is IMPORTED, never rebuilt: hashing and artifact writing from
:mod:`alpha_agent.r31`; the HTTP acquisition primitive from
:mod:`alpha_agent.r35.acquisition`; the entitlement-measurement discipline and
the coverage matrix from :mod:`alpha_agent.r36`.

This package creates NO signal authority, NO portfolio target, NO proposal, NO
decision, NO order, NO model promotion, NO sleeve activation and NO operational
write. It reads owned data, downloads free public data, and writes immutable
research artifacts under its own research root. It spends nothing.
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

RELEASE = "release37"
CAMPAIGN_FAMILY = "native_market_data_gate"

#: The isolated research root. NO operational store is ever written by this
#: package; ``tests/test_release37_native_market_data_gate.py`` asserts that
#: every path this package writes is under this root, and
#: ``scripts/r33_operational_write_attribution.py`` (R37 profile) proves it
#: against the live operational stores.
RESEARCH_ROOT_ENV = "PAPER_TRADER_R37_RESEARCH_ROOT"
DEFAULT_RESEARCH_ROOT = Path(
    r"D:\Stock_Prediction_app_data\native_market_data_gate_r37")

#: Safety badges every artifact carries. ``NO LIVE BROKER ORDERS`` is the
#: canonical Phase 27B.6 wording.
SAFETY = ["RESEARCH ONLY", "READ ONLY", "PREVIEW ONLY",
          "NO LIVE BROKER ORDERS", "AUTOMATION OFF", "MANUAL REVIEW",
          "NO OPERATIONAL WRITE", "NO MODEL PROMOTION",
          "NO SLEEVE ACTIVATION", "NO PORTFOLIO ACTIVATION",
          "NO PURCHASE", "NO TRIAL", "NO NEW ACCOUNT",
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


def acquisition_root() -> Path:
    """Where downloaded RAW third-party sample payloads live.

    Outside any campaign directory on purpose: a raw vendor sample is an input
    to every campaign version, not an artifact of one, and re-downloading it for
    a superseding run would change bytes an earlier artifact was hashed against.
    """
    return research_root() / "acquired"


def safety_block() -> dict:
    """The safety declaration carried by every Release-37 artifact.

    Every flag is FALSE and is asserted false by the architecture audit. The
    commercial flags are listed explicitly because this is the first release
    whose whole subject is spending money, and a reader should not have to infer
    from prose that it did not.
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
    "research_root", "campaign_dir", "acquisition_root", "safety_block",
    "artifact_body",
    "sha", "sha_file", "file_fingerprint", "write_json", "read_json",
    "ArtifactImmutable",
]
