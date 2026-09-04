"""alpha_agent.r58 - Release 58: the orthogonal alpha offensive.

R57 proved the PRICE information set is exhausted for stable cross-sectional
equity edges (12/12 NO_ALPHA_EVIDENCE on a 20-year survivorship-safe panel).
R58 asks a different question, pre-registered in
``research/r58/R58_RESEARCH_PROTOCOL.json`` BEFORE any experiment ran:

    fundamentals   does the operational champion's FUNDAMENTAL leg carry real
                   point-in-time, survivorship-safe skill that the momentum leg
                   is destroying?
    panel_f        the substrate that makes the question answerable at all: the
                   Norgate S&P 500 Current & Past price panel joined to the
                   owned SEC EDGAR companyfacts store through the resolved CIK
                   bridge - 885 symbols, 237 of them delisted
    families       13 pre-registered FDR-counted families in three groups
                   (fundamental rescue / blend-and-gating / information change)
                   plus one diagnostic reference for the incumbent blend
    tournament     validation selection persisted BEFORE an untouched lockbox
    inventory      what orthogonal information we actually own, classified
    challengers    immutable prospective freezes for information whose owned
                   history is too short to backtest honestly

Everything here is RESEARCH ONLY: no order, no fill, no broker, no promotion,
no sleeve activation, no operational-store write. The live C: checkout and the
production desk ledgers are READ ONLY. R46/R56/R57 evidence is never rewritten.
"""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

CAMPAIGN_ID = "r58_orthogonal_alpha_v1"
PHASE = "R58"

RESEARCH_ROOT_ENV = "PAPER_TRADER_R58_RESEARCH_ROOT"
DEFAULT_RESEARCH_ROOT = Path(r"D:\Stock_Prediction_app_data\r58_orthogonal_alpha")

REPO_ROOT = Path(__file__).resolve().parents[2]
PROTOCOL_PATH = REPO_ROOT / "research" / "r58" / "R58_RESEARCH_PROTOCOL.json"

SAFETY = {
    "research_only": True, "paper_only": True, "creates_orders": False,
    "creates_fills": False, "broker_enabled": False, "promotes_model": False,
    "activates_sleeve": False, "mutates_operational_store": False,
    "automation_enabled": False,
}

# --- owned read-only sources (measured, not assumed) ------------------------
R57_ROOT = Path(r"D:\Stock_Prediction_app_data\r57_alpha_discovery")
SEC_FACTS_DB = Path(r"D:\Stock_Prediction_app_data\stage24_pit_fundamental_alpha"
                    r"\_index\sec_companyfacts_stage24.sqlite")
IDENTITY_DB = Path(r"D:\Stock_Prediction_app_data\alpha_agent\identity"
                   r"\historical_identity.sqlite")
INGEST_ROOT = Path(r"D:\Stock_Prediction_app_data\alpha_agent\ingestion\normalized")
FROZEN_FUND_PANEL = Path(
    r"C:\Users\binis\Stock_Prediction_app_push\research\output"
    r"\phase10l_historical_sector_neutral_scored_panel_reconstruction"
    r"\historical_sector_neutral_scored_panel.csv")
DESK_DIR = Path(r"C:\Users\binis\.paper_trader\paper_trading_desk")

# --- pre-registered partition (protocol section "partition") ----------------
DISCOVERY_START = "2011-07-01"
VALIDATION_START = "2018-01-01"
LOCKBOX_START = "2023-01-01"

# --- pre-registered conventions (protocol section "conventions") ------------
CADENCE = 21
HORIZON = 21
EQ_COST_RATE_PER_SIDE = 0.00125
EQ_TOP_N = 50
EQ_MIN_PRICE = 5.0
EQ_MIN_ADV = 1.0e7
EQ_MIN_HISTORY = 260
MIN_SCORED_FRACTION = 0.60
MIN_SCORED_DATE_FRACTION = 0.90
BH_Q = 0.10
OBS_FLOOR = 36

# --- pre-registered gates ---------------------------------------------------
GATE_MATERIALITY = 0.015
GATE_HALF_FLOOR = -0.005
GATE_MAX_TURNOVER = 0.40
GATE_DD_MULTIPLE = 1.5

FDR_FAMILIES = ("A1", "A2", "A3", "A4", "B2", "B3", "B4", "B5",
                "C1", "C2", "C3", "C4", "C5")
DIAGNOSTIC_FAMILIES = ("B0",)

PRIOR_SEARCH_BURDEN = 302


def research_root() -> Path:
    return Path(os.environ.get(RESEARCH_ROOT_ENV) or DEFAULT_RESEARCH_ROOT)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_hash(obj) -> str:
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def file_hash(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def protocol() -> dict:
    return json.loads(PROTOCOL_PATH.read_text(encoding="utf-8"))


def protocol_hash() -> str:
    return hashlib.sha256(PROTOCOL_PATH.read_bytes()).hexdigest()


def write_artifact(name: str, body: dict, subdir: str = "results") -> Path:
    d = research_root() / subdir
    d.mkdir(parents=True, exist_ok=True)
    body = dict(body)
    body.setdefault("campaign_id", CAMPAIGN_ID)
    body.setdefault("phase", PHASE)
    body.setdefault("generated_at", now_iso())
    body.setdefault("protocol_sha256", protocol_hash())
    body.setdefault("safety", dict(SAFETY))
    body["artifact_hash"] = stable_hash(
        {k: v for k, v in body.items()
         if k not in ("artifact_hash", "generated_at")})
    p = d / name
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(body, indent=1, default=str), encoding="utf-8")
    tmp.replace(p)
    return p


def read_artifact(name: str, subdir: str = "results"):
    p = research_root() / subdir / name
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))
