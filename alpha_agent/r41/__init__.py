"""alpha_agent.r41 - Release 41, the Multi-Horizon Alpha Breakthrough Campaign.

RESEARCH ONLY. Releases 31-40 established, honestly, that the estate's
monthly, directional, price-and-public-macro research over the owned
information has no qualified Alpha at a cumulative search burden of 230
effective trials. Release 41 attacks the FOUR binding blockers together:

    BLOCKER 1  INFORMATION QUALITY   - price-derived / public-macro / simple
                                        carry information is nearly exhausted;
    BLOCKER 2  DECISION CADENCE      - monthly targets produce too few
                                        independent decisions per year;
    BLOCKER 3  ECONOMIC EXPRESSION   - up/down prediction, not relative
                                        prices, curves, spreads, butterflies,
                                        basis, term structure, volatility;
    BLOCKER 4  SEARCH BURDEN         - another search over the SAME
                                        information manufactures near misses.

The correction this release makes explicit: Paper Trader is NOT a monthly
system. The canonical architecture has THREE clocks - signal refresh (as
often as the information supports), portfolio reassessment (after every
MATERIAL update) and model recalibration (evidence-gated). A candidate's
decision cadence is a property of the CANDIDATE (its information's source
frequency and its economic logic), never of the system.

Ownership map - each concern has exactly one owner here; every
already-owned concern (hashing/immutability, Benjamini-Hochberg / SPA /
bootstrap, Newey-West economics, the dated-contract roll policy, the R39
zones and reuse ledger, the desk ledger primitives, the research-shadow
capture owner) is IMPORTED from its canonical owner and never re-implemented:

===============================  =============================================
concern                          owner
===============================  =============================================
release contract                 :mod:`alpha_agent.r41.contract`
R40 closeout verification        :mod:`alpha_agent.r41.closeout_import`
multi-horizon target engine      :mod:`alpha_agent.r41.horizon_engine`
material-update triggers         :mod:`alpha_agent.r41.triggers`
owned-data frequency inventory   :mod:`alpha_agent.r41.data_inventory`
provider / sample frontier       :mod:`alpha_agent.r41.provider_frontier`
free sample acquisition          :mod:`alpha_agent.r41.sample_acquisition`
dated-contract curve state       :mod:`alpha_agent.r41.curve_state`
multi-horizon evidence           :mod:`alpha_agent.r41.evidence`
rates relative-value lab         :mod:`alpha_agent.r41.rates_rv_lab`
commodity curve lab              :mod:`alpha_agent.r41.commodity_curve_lab`
volatility / options lab         :mod:`alpha_agent.r41.vol_lab`
intraday market-structure lab    :mod:`alpha_agent.r41.intraday_lab`
crypto + microstructure labs     :mod:`alpha_agent.r41.crypto_lab`
FX + credit labs                 :mod:`alpha_agent.r41.fx_credit_lab`
sequence-model scale test        :mod:`alpha_agent.r41.model_scale`
alpha killer                     :mod:`alpha_agent.r41.alpha_killer`
search burden (global + family)  :mod:`alpha_agent.r41.burden`
forward freeze registry          :mod:`alpha_agent.r41.forward_freeze`
data purchase decision engine    :mod:`alpha_agent.r41.purchase_engine`
near-real-time readiness         :mod:`alpha_agent.r41.readiness`
orchestration + verdict          :mod:`alpha_agent.r41.campaign`
===============================  =============================================

This package creates NO signal authority, NO portfolio target, NO proposal,
NO decision, NO order, NO model promotion, NO sleeve activation, NO scheduler
change, NO production restart and NO operational write. It spends $0,
creates no account, starts no trial, accepts no licence on the operator's
behalf and purchases nothing; it may download FREE, PUBLIC, account-free
samples to the research drive under the conditions in :mod:`contract`.
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

RELEASE = "release41"
CAMPAIGN_FAMILY = "multi_horizon_alpha_breakthrough"
CAMPAIGN_ID = "r41_multi_horizon_alpha_breakthrough_v1"

RESEARCH_ROOT_ENV = "PAPER_TRADER_R41_RESEARCH_ROOT"
DEFAULT_RESEARCH_ROOT = Path(
    r"D:\Stock_Prediction_app_data\multi_horizon_alpha_r41")

#: Large files (tick archives, minute bars, curve caches, model weights) live
#: on the research drive; nothing large may be written to C:.
LARGE_FILE_DRIVE = "D:"

#: torch (CPU-only) was installed by the R39 continuation to the research
#: drive; this release reuses that install and installs nothing.
TORCH_LIB_DIR = Path(r"D:\Stock_Prediction_app_data\universal_alpha_r39"
                     r"\_torch_cpu_lib")

SAFETY = list(_r39.SAFETY) + ["NO SCHEDULER CHANGE", "NO BACKDATED FORWARD ROW",
                              "NO PAID SAMPLE", "NO PAID COMPUTE"]

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
    """The R41 campaign directory, ALSO registered with the R39 owner package
    so the R39 reuse ledger / artifact writer serve this campaign under the
    R41 root (one owner, three releases)."""
    d = research_root() / str(campaign_id)
    _r39.register_campaign_root(campaign_id, research_root())
    return d


def data_dir(name: str) -> Path:
    """A named large-data directory under the research root (curves, ticks,
    samples). Shared across campaign versions; content-hashed by consumers."""
    d = research_root() / ("_data_%s" % name)
    d.mkdir(parents=True, exist_ok=True)
    return d


def ensure_torch_path() -> bool:
    import sys
    p = str(TORCH_LIB_DIR)
    if TORCH_LIB_DIR.exists() and p not in sys.path:
        sys.path.insert(0, p)
    try:
        import torch  # noqa: F401
        return True
    except Exception:
        return False


def safety_block() -> dict:
    body = _r39.safety_block()
    body["safety"] = list(SAFETY)
    body["changes_scheduler"] = CHANGES_SCHEDULER
    body["downloads_free_public_samples"] = "ONLY_UNDER_CONTRACT_CONDITIONS"
    body["backdates_forward_rows"] = False
    body["purchases_data"] = False
    body["spends_cloud_compute"] = False
    return body


def artifact_body(schema: str, payload: dict, **extra: Any) -> dict:
    body = {"schema": schema, "release": RELEASE, "campaign_id": CAMPAIGN_ID}
    body.update(payload)
    body.update(extra)
    body["safety_block"] = safety_block()
    return body


def write_artifact(name: str, body: dict, campaign_id: str = CAMPAIGN_ID,
                   *, overwrite: bool = False) -> Path:
    """Write one campaign artifact (immutable unless ``overwrite``)."""
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
    "DEFAULT_RESEARCH_ROOT", "LARGE_FILE_DRIVE", "TORCH_LIB_DIR", "SAFETY",
    "AUTOMATIC_PROMOTION_ALLOWED", "AUTOMATIC_SLEEVE_ACTIVATION_ALLOWED",
    "MAY_SPEND_MONEY", "MAY_MUTATE_PRODUCTION", "TRAINS_MODELS",
    "PROMOTES_MODELS", "CHANGES_SCHEDULER",
    "research_root", "campaign_dir", "data_dir", "ensure_torch_path",
    "safety_block", "artifact_body", "write_artifact",
    "sha", "sha_file", "file_fingerprint", "write_json", "read_json",
    "ArtifactImmutable",
]
