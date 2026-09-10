"""alpha_agent.r64 - Release 64: information-directed alpha.

R63 measured which economic information carries value CONDITIONAL on what the
estate already uses and found one incremental candidate (FX carry from the
dated-contract slope, 1 session) plus several cells whose conditional value
was real but whose economic verdict was decided by a pathological book. R64
asks four bounded questions, pre-registered in
``research/r64/R64_RESEARCH_PROTOCOL.json`` BEFORE any R64 number was computed:

    1. does the exact R63 FX-carry artifact reproduce from the same substrate,
       and does it survive as ONE economic family across 1, 5, 21 and 63
       sessions under family-aware multiple testing?
    2. does economically distinct carry information carry conditional value
       across FX, rates, commodity, equity-index, volatility and cross-asset
       scopes?
    3. does information that failed only on construction survive a bounded,
       realistic, risk-controlled portfolio construction?
    4. can the persistent AlphaAgent governor consume the R63 information gap
       frontier without a second memory, queue, scheduler, frontier or
       TRUE_FORWARD owner?

Modules (one question each; no second owner of anything that exists):

    handoff_validation  the exact R63 FX-carry artifact and evidence, verified
    carry               genuine carry from DISTINCT dated contracts; the R59
                        close_b pseudo-curve is measured and refused
    construction        the ONE bounded, realistic, risk-controlled research
                        book (volatility target, leverage cap, instrument
                        contribution cap, asset-class risk budgets, no-trade
                        band, turnover-aware costs) applied to BOTH arms
    experiments         the R64 cells: the R63 scorer keeps its OOS scores and
                        R64 prices them; R63 conditional statistics are
                        REPRODUCED, never recomputed differently
    family              family-aware multiple testing (campaign BH + Holm
                        within the FX CARRY family)
    challenger          immutable governed research candidates with a
                        freeze_record_hash over the complete forward
                        specification; never promoted, never registered
    frontier            an OVERLAY of R64 verdicts keyed by information need,
                        consumed by the R59 governor through the R63 frontier;
                        never a second frontier
    report              the human-readable result rendered from the artifacts

RESEARCH ONLY. Nothing here creates an order, a fill, a proposal, a promotion,
a sleeve activation, a registration, a purchase, a subscription or an
operational-store write. The live ``C:\\Users\\binis\\paper_trader`` checkout is
read-only; live state, where needed, is READ and never mutated. Every R64
write lands under the R64 research root, which a test can redirect.
"""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from alpha_agent import r63 as _r63

CAMPAIGN_ID = "r64_information_directed_alpha_v1"
PHASE = "R64"
RELEASE = "R64"

RESEARCH_ROOT_ENV = "PAPER_TRADER_R64_RESEARCH_ROOT"
DEFAULT_RESEARCH_ROOT = Path(r"D:\Stock_Prediction_app_data\r64_information_directed_alpha")

#: Where the R63 artifacts are READ from (never written). A test redirects it.
R63_RESULTS_ROOT_ENV = "PAPER_TRADER_R64_R63_RESULTS_ROOT"
DEFAULT_R63_RESULTS_ROOT = _r63.DEFAULT_RESEARCH_ROOT

REPO_ROOT = Path(__file__).resolve().parents[2]
PROTOCOL_PATH = REPO_ROOT / "research" / "r64" / "R64_RESEARCH_PROTOCOL.json"

SAFETY = {
    "research_only": True,
    "paper_only": True,
    "creates_orders": False,
    "creates_fills": False,
    "broker_enabled": False,
    "promotes_model": False,
    "activates_sleeve": False,
    "approves_proposal": False,
    "registers_forward_challenger": False,
    "mutates_operational_store": False,
    "mutates_live_research_store": False,
    "purchases_data": False,
    "starts_trial_or_subscription": False,
    "automation_enabled": False,
    "live_checkout_read_only": True,
}

SAFETY_BADGES = ("RESEARCH ONLY", "PREVIEW ONLY", "NO ORDERS", "ORDERS DISABLED",
                 "AUTOMATION OFF", "MANUAL REVIEW", "NO PURCHASE",
                 "NO LIVE REGISTRATION", "NO PROMOTION")

# --------------------------------------------------------------------------- #
# Scopes and horizons: reused verbatim from R63 so every vocabulary lines up.
# --------------------------------------------------------------------------- #
AC_US_EQUITY = _r63.AC_US_EQUITY
AC_EQUITY_INDEX = _r63.AC_EQUITY_INDEX
AC_RATES = _r63.AC_RATES
AC_COMMODITY = _r63.AC_COMMODITY
AC_FX = _r63.AC_FX
AC_VOLATILITY = _r63.AC_VOLATILITY
AC_CROSS_ASSET = _r63.AC_CROSS_ASSET
AC_CREDIT = _r63.AC_CREDIT
CARRY_SCOPES = (AC_FX, AC_RATES, AC_COMMODITY, AC_EQUITY_INDEX, AC_VOLATILITY,
                AC_CROSS_ASSET)
HORIZONS = _r63.HORIZONS

#: The ONE economic family every carry construction belongs to.
CARRY_FAMILY = "CARRY"
#: The FX carry family: 4 horizons x 2 modes, counted as ONE family.
FX_CARRY_FAMILY_ID = "FX_FUTURES|CARRY"
FX_CARRY_FAMILY_MODES = ("XS", "TS")

#: Carry constructions (protocol section "carry_constructions"). Formula
#: variants of ONE dimension; all counted inside the CARRY family.
DIM_CURVE_CARRY = "CARRY"
DIM_CARRY_TO_RISK = "CARRY_TO_RISK"
DIM_CARRY_CLASS_NEUTRAL = "CARRY_CLASS_NEUTRAL"
CARRY_VARIANTS = (DIM_CURVE_CARRY, DIM_CARRY_TO_RISK, DIM_CARRY_CLASS_NEUTRAL)

# --------------------------------------------------------------------------- #
# Pre-registered risk-controlled construction (protocol section
# "risk_controlled_construction"). None of these is tuned on any result.
# --------------------------------------------------------------------------- #
TARGET_VOL = 0.10
MAX_GROSS_LEVERAGE = 5.0
MAX_INSTRUMENT_RISK_SHARE = 0.25
MAX_NAME_WEIGHT = 0.25                 # R63's per-side cap, reused
NO_TRADE_BAND = 0.25
VOL_LOOKBACK_PERIODS = 63
VOL_MIN_PERIODS = 12
VOL_FLOOR = 0.02
DEGENERATE_DD = -0.60
LEVERAGE_AT_CAP_SHARE_MAX = 0.50

# Pre-registered statistical conventions (inherited from R63 where they exist).
LOCKBOX_START = _r63.LOCKBOX_START
BH_Q = _r63.BH_Q
HOLM_ALPHA = 0.05
MIN_EFFECTIVE_PERIODS = _r63.MIN_EFFECTIVE_PERIODS
MATERIALITY_ANN_NET = _r63.MATERIALITY_ANN_NET
MATERIALITY_SHARPE = 0.15
CONDITIONAL_T_FLOOR = _r63.CONDITIONAL_T_FLOOR
PARTIAL_RESIDUAL_SHARE_MAX = _r63.PARTIAL_RESIDUAL_SHARE_MAX
REPRODUCTION_TOL = 1e-9

# R64 verdict ladder (protocol section "verdict_ladder").
V_REPRO_FAILED = "REPRODUCTION_FAILED"
V_NO_VALUE = "NO_CONDITIONAL_VALUE"
V_NOT_ECON = "NOT_ECONOMIC_UNDER_CONTROLS"
V_UNSTABLE = "ECONOMIC_UNDER_CONTROLS_UNSTABLE"
V_NOT_FDR = "ECONOMIC_UNDER_CONTROLS_NOT_FDR"
V_ECON = "ECONOMIC_UNDER_CONTROLS"
V_DATA_HOLD = "DATA_HOLD"
VERDICTS = (V_REPRO_FAILED, V_NO_VALUE, V_NOT_ECON, V_UNSTABLE, V_NOT_FDR, V_ECON,
            V_DATA_HOLD)

# Challenger classification, reused verbatim from R63.
CH_READY = _r63.CH_READY
CH_MORE = _r63.CH_MORE
CH_REJECTED = _r63.CH_REJECTED
CHALLENGER_STATES = _r63.CHALLENGER_STATES

stable_hash = _r63.stable_hash
short_hash = _r63.short_hash


def research_root() -> Path:
    return Path(os.environ.get(RESEARCH_ROOT_ENV) or DEFAULT_RESEARCH_ROOT)


def r63_results_root() -> Path:
    """The R63 research root R64 READS its artifacts from. Never written."""
    return Path(os.environ.get(R63_RESULTS_ROOT_ENV) or DEFAULT_R63_RESULTS_ROOT)


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def protocol() -> dict:
    return json.loads(PROTOCOL_PATH.read_text(encoding="utf-8"))


def normalised_sha256(path: Path) -> str:
    """sha256 of a text file with CRLF normalised to LF, so the SAME committed
    protocol hashes identically on a core.autocrlf checkout and on the LF
    checkout that produced an artifact."""
    return hashlib.sha256(Path(path).read_bytes().replace(b"\r\n", b"\n")).hexdigest()


def protocol_hash() -> str:
    return normalised_sha256(PROTOCOL_PATH)


def write_artifact(name: str, body: dict, subdir: str = "results") -> Path:
    """Atomically write a hashed, safety-stamped, deterministic artifact.

    Same contract as ``alpha_agent.r63.write_artifact``: sorted keys; the
    content hash excludes ``generated_at``; the R64 protocol hash is stamped.
    """
    d = research_root() / subdir
    d.mkdir(parents=True, exist_ok=True)
    caller = body
    body = dict(body)
    body.setdefault("campaign_id", CAMPAIGN_ID)
    body.setdefault("phase", PHASE)
    body.setdefault("generated_at", now_iso())
    body.setdefault("protocol_sha256", protocol_hash() if PROTOCOL_PATH.exists() else None)
    body.setdefault("safety", dict(SAFETY))
    body["artifact_hash"] = stable_hash(
        {k: v for k, v in body.items() if k not in ("artifact_hash", "generated_at")})
    p = d / name
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(body, indent=1, sort_keys=True, default=str), encoding="utf-8")
    tmp.replace(p)
    for k in ("campaign_id", "phase", "generated_at", "protocol_sha256", "safety",
              "artifact_hash"):
        caller[k] = body[k]
    return p


def read_artifact(name: str, subdir: str = "results"):
    p = research_root() / subdir / name
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def read_r63_artifact(name: str, subdir: str = "results"):
    """READ one R63 artifact from the R63 results root. Never writes there."""
    p = r63_results_root() / subdir / name
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def assert_worktree_import() -> str:
    """Fail closed if ``alpha_agent`` resolved to a different checkout (the
    venv's editable finder maps ``paper_trader`` to the live C: checkout)."""
    here = Path(__file__).resolve().parents[1]
    import alpha_agent as _aa
    got = Path(_aa.__file__).resolve().parent
    if got != here:
        raise RuntimeError(
            "R64 import integrity: alpha_agent resolved to %s but r64 lives in "
            "%s - the editable-install finder captured the import" % (got, here))
    return str(here)


def assert_research_root_is_not_live(root: Path | None = None) -> Path:
    """Refuse a research root inside the live checkout or a production store.
    Delegates to the R63 owner of the rule with the R64 root."""
    return _r63.assert_research_root_is_not_live(Path(root or research_root()))
