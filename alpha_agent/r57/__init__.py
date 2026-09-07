"""alpha_agent.r57 - Release 57: the alpha discovery offensive.

One pre-registered campaign (``research/r57/R57_RESEARCH_PROTOCOL.json``,
written BEFORE any experiment ran) prosecuted across four fronts:

    track1      why is the incumbent buy engine weak (DIAGNOSTIC only)
    tournament  equity buy-side families on the survivorship-safe Norgate
                S&P 500 Current & Past panel, walk-forward with an untouched
                lockbox
    futures     trend / breakout / cross-market momentum on Norgate
                Continuous Futures with a roll-methodology contamination check
    labs        score->return calibration, construction attribution and
                turnover control, each pre-registered

Everything here is RESEARCH ONLY: no order, no fill, no broker, no promotion,
no sleeve activation, no operational-store write. Artifacts live under the
R57-owned research root; the six R56 shadow-portfolio records are read-only
inputs and are never modified.
"""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

CAMPAIGN_ID = "r57_alpha_discovery_v1"
PHASE = "R57"

RESEARCH_ROOT_ENV = "PAPER_TRADER_R57_RESEARCH_ROOT"
DEFAULT_RESEARCH_ROOT = Path(r"D:\Stock_Prediction_app_data\r57_alpha_discovery")

PROTOCOL_PATH = Path(__file__).resolve().parents[2] / "research" / "r57" / "R57_RESEARCH_PROTOCOL.json"

SAFETY = {
    "research_only": True, "paper_only": True, "creates_orders": False,
    "creates_fills": False, "broker_enabled": False, "promotes_model": False,
    "activates_sleeve": False, "mutates_operational_store": False,
    "automation_enabled": False,
}

# Pre-registered partition (calendar dates; session indices resolved on the
# SPY calendar at build time).
DISCOVERY_START = "2006-01-01"
VALIDATION_START = "2018-01-01"
LOCKBOX_START = "2023-01-01"
PANEL_START = "2004-06-01"      # warmup for 252+21-session lookbacks
PANEL_END = "2026-09-03"        # last completed session at registration

EQ_COST_RATE_PER_SIDE = 0.00125          # 12.5bp, the desk convention
FUT_COST_RATE_PER_SIDE = 0.0002          # 2bp of notional
EQ_TOP_N = 50
EQ_MIN_PRICE = 5.0
EQ_MIN_ADV = 1.0e7
EQ_MIN_HISTORY = 260

BH_Q = 0.10


def research_root() -> Path:
    return Path(os.environ.get(RESEARCH_ROOT_ENV) or DEFAULT_RESEARCH_ROOT)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_hash(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def write_artifact(name: str, body: dict, subdir: str = "results") -> Path:
    d = research_root() / subdir
    d.mkdir(parents=True, exist_ok=True)
    body = dict(body)
    body.setdefault("campaign_id", CAMPAIGN_ID)
    body.setdefault("phase", PHASE)
    body.setdefault("generated_at", now_iso())
    body.setdefault("safety", dict(SAFETY))
    body["artifact_hash"] = stable_hash({k: v for k, v in body.items()
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


def protocol() -> dict:
    return json.loads(PROTOCOL_PATH.read_text(encoding="utf-8"))
