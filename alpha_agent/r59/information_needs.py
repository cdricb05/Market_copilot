"""alpha_agent.r59.information_needs - the governor's second question.

The persistent research governor asks "what should we learn next?" and, until
R64, answered only from a frontier of PRICE families and a list of DATASETS.
R63 published a third answer - the ranked INFORMATION GAP FRONTIER (asset
class x horizon x information dimension, with remaining research value) - as
an interface that nothing consumed. This adapter makes it consumable:

    * it READS the frontier artifact by path (and an optional R64 overlay that
      multiplies a need's remaining value by what R64 measured);
    * it yields information-need candidates in the governor's own mandate
      shape (kind DATA_OPPORTUNITY, family ``DATA:INFORMATION_NEED:<cell_key>``)
      so the existing lane, fairness cap and queue carry them;
    * it dedupes with a memory_meta WATERMARK per (cell_key, frontier
      artifact hash), written by the existing DATA_VALIDATION handler, so an
      unchanged frontier is never re-mandated (the R61 busy-loop lesson).

It imports NO research package: the artifact is the interface. It creates no
memory, queue, scheduler, frontier or forward owner; it executes nothing.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

from .. import r59
from . import memory as M

CALCULATION_OWNER = "alpha_agent.r59.information_needs"

FRONTIER_PATH_ENV = "PAPER_TRADER_INFORMATION_FRONTIER_PATH"
DEFAULT_FRONTIER_PATH = Path(
    r"D:\Stock_Prediction_app_data\r63_information_sensitivity\results"
    r"\information_gap_frontier.json")
OVERLAY_PATH_ENV = "PAPER_TRADER_INFORMATION_NEED_OVERLAY_PATH"
DEFAULT_OVERLAY_PATH = Path(
    r"D:\Stock_Prediction_app_data\r64_information_directed_alpha\results"
    r"\r64_information_need_updates.json")

SOURCE = "R63_INFORMATION_GAP_FRONTIER"
FAMILY_PREFIX = "DATA:INFORMATION_NEED:"
WATERMARK_META_PREFIX = "information_need_watermark:"
EVENT_MANDATED = "INFORMATION_NEED_MANDATED"
#: Sourcing steps a RESEARCH mandate can act on without a purchase decision.
ACTIONABLE_SOURCING = ("OWNED", "FREE")
DEFAULT_LIMIT = 10
#: expected_information_value on the governor's [0, 1] scale from the
#: frontier's remaining_research_value (top of the R63 frontier ~ 0.13).
EIV_BASE = 0.55
EIV_SLOPE = 3.0
EIV_CAP = 0.95


def frontier_path() -> Path:
    return Path(os.environ.get(FRONTIER_PATH_ENV) or DEFAULT_FRONTIER_PATH)


def overlay_path() -> Path:
    return Path(os.environ.get(OVERLAY_PATH_ENV) or DEFAULT_OVERLAY_PATH)


def _read(p: Path) -> Optional[dict]:
    try:
        if not p.exists():
            return None
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def load_frontier(path: Optional[Path] = None) -> Optional[dict]:
    return _read(Path(path) if path else frontier_path())


def load_overlay(path: Optional[Path] = None) -> Optional[dict]:
    return _read(Path(path) if path else overlay_path())


def eiv_for(remaining_research_value: float) -> float:
    return round(min(EIV_CAP, EIV_BASE + EIV_SLOPE * float(remaining_research_value or 0.0)), 4)


def watermark_key(cell_key: str) -> str:
    return WATERMARK_META_PREFIX + str(cell_key)


def need_rows(frontier: Optional[dict], overlay: Optional[dict] = None, *,
              limit: int = DEFAULT_LIMIT) -> list:
    """Actionable information needs, highest remaining value first, with the
    overlay's multiplier applied. Deterministic: ties break on cell_key."""
    rows = list((frontier or {}).get("all_needs") or [])
    mult = {u["cell_key"]: u for u in ((overlay or {}).get("updates") or [])
            if isinstance(u, dict) and u.get("cell_key")}
    out = []
    for r in rows:
        key = r.get("cell_key")
        if not key or r.get("asset_class") not in r59.ASSET_CLASSES:
            continue
        if str(r.get("sourcing_step") or "").upper() not in ACTIONABLE_SOURCING:
            continue
        rrv = float(r.get("remaining_research_value") or 0.0)
        u = mult.get(key)
        if u is not None:
            rrv *= float(u.get("remaining_research_value_multiplier", 1.0))
        if rrv <= 0.0:
            continue
        out.append({**r, "remaining_research_value_effective": rrv,
                    "overlay": ({"r64_verdict": u.get("r64_verdict"),
                                 "next_action": u.get("next_action"),
                                 "cell_id": u.get("cell_id")} if u else None)})
    out.sort(key=lambda r: (-r["remaining_research_value_effective"], r["cell_key"]))
    return out[:int(limit)]


def candidates(mem: Optional[M.ResearchMemory] = None, *, frontier: Optional[dict] = None,
               overlay: Optional[dict] = None, limit: int = DEFAULT_LIMIT) -> list:
    """Information-need candidates for the governor: rows in the shape
    ``_mandate`` consumes (asset_class / family / eiv / reason / payload),
    excluding needs already mandated for THIS frontier version."""
    fr = frontier if frontier is not None else load_frontier()
    if not fr:
        return []
    ov = overlay if overlay is not None else load_overlay()
    fr_hash = fr.get("artifact_hash")
    out = []
    for r in need_rows(fr, ov, limit=limit):
        key = r["cell_key"]
        if mem is not None and fr_hash and mem.get_meta(watermark_key(key)) == fr_hash:
            continue
        next_action = ((r.get("overlay") or {}).get("next_action")) or r.get("next_action")
        out.append({
            "asset_class": r["asset_class"],
            "family": FAMILY_PREFIX + key,
            "eiv": eiv_for(r["remaining_research_value_effective"]),
            "reason": ("information gap frontier: observation_state=%s best_verdict=%s "
                       "next_action=%s remaining_value=%.4f"
                       % (r.get("observation_state"), r.get("best_verdict"), next_action,
                          r["remaining_research_value_effective"])),
            "payload": {"source": SOURCE, "cell_key": key,
                        "horizon_sessions": r.get("horizon"),
                        "information_family": r.get("dimension"),
                        "economic_family": "INFORMATION_%s" % r.get("dimension"),
                        "next_action": next_action,
                        "sourcing_step": r.get("sourcing_step"),
                        "frontier_artifact_hash": fr_hash,
                        "overlay": r.get("overlay")}})
    return out


def is_information_need(mandate_payload: Optional[dict]) -> bool:
    p = (mandate_payload or {}).get("payload") or {}
    return p.get("source") == SOURCE and bool(p.get("cell_key"))


def record_mandated(mem: M.ResearchMemory, *, mandate: dict) -> dict:
    """The DATA_VALIDATION handler's work for an information need: record the
    need in the ONE research memory and set the watermark so this frontier
    version never re-mandates it. Executes no research and buys nothing."""
    p = (mandate or {}).get("payload") or {}
    key = p.get("cell_key")
    fr_hash = p.get("frontier_artifact_hash")
    mem.set_meta(watermark_key(key), fr_hash)
    mem.event(EVENT_MANDATED, subject=str(key),
              detail={"mandate_id": mandate.get("mandate_id"),
                      "asset_class": mandate.get("asset_class"),
                      "horizon_sessions": p.get("horizon_sessions"),
                      "information_family": p.get("information_family"),
                      "next_action": p.get("next_action"),
                      "sourcing_step": p.get("sourcing_step"),
                      "frontier_artifact_hash": fr_hash,
                      "overlay": p.get("overlay"),
                      "executes_research": False, "purchases": False})
    return {"real_work": "r64_information_need", "cell_key": key,
            "frontier_artifact_hash": fr_hash, "watermark_set": True,
            "next_action": p.get("next_action"),
            "detail": ("information need recorded in research memory; the "
                       "information-sensitivity measurement runs in the offline research "
                       "campaign, not in the live runtime")}


__all__ = ["CALCULATION_OWNER", "FRONTIER_PATH_ENV", "DEFAULT_FRONTIER_PATH",
           "OVERLAY_PATH_ENV", "DEFAULT_OVERLAY_PATH", "SOURCE", "FAMILY_PREFIX",
           "WATERMARK_META_PREFIX", "EVENT_MANDATED", "ACTIONABLE_SOURCING", "load_frontier",
           "load_overlay", "need_rows", "candidates", "eiv_for", "watermark_key",
           "is_information_need", "record_mandated"]
