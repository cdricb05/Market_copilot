"""alpha_agent.r38.ml_contract - Phase 16: the ML-ready native foundation.

Release 39 is expected to challenge the statistical architecture with modern
models. This module makes sure it will not have to rebuild data engineering:
it declares ONE versioned, point-in-time-safe panel contract over the native
futures layer and EMITS the panel - decision-stamped rows, forward targets
realised strictly after each decision, missingness masks instead of silent
fills, per-row costs, a control return for every row, and a chronological
train/validation/test partition with an embargo.

It deliberately does NOT train anything (``TRAINS_A_MODEL`` is False), does
not install any ML library, and spends no compute beyond assembling the
panel. Sequence-level inputs (full OHLC/settlement paths) are REFERENCED by
layer-file checksum rather than duplicated.
"""
from __future__ import annotations

import datetime as _dt
import hashlib as _hashlib
from typing import Optional

from .. import r38
from . import contract as C
from . import enumeration as EN
from . import experiments as EX
from . import research_layer as RL

CALCULATION_OWNER = "alpha_agent.r38.ml_contract"
SCHEMA = "r38_ml_ready_native_futures_contract/1"
ARTIFACT_NAME = C.ARTIFACT_NAMES["ml_ready_native_futures_contract"]
PANEL_NAME = "ml_ready_native_futures_panel.csv"

TRAINS_A_MODEL = False
CADENCE_SESSIONS = 21
EMBARGO_SESSIONS = 21
PARTITION_SHARES = (0.6, 0.2, 0.2)  # chronological TRAIN / VALIDATION / TEST

PANEL_COLUMNS = (
    "market_id", "contract_id", "asset_class", "economic_group", "exchange",
    "currency", "decision_date", "observation_date",
    "fwd_return", "ret_1m", "ret_3m", "ret_12m", "vol_63",
    "carry_slope_ann", "volume", "open_interest", "cot_commercial_z",
    "has_carry", "has_cot", "has_volume", "has_open_interest",
    "market_activity_state", "pit_state", "cost_bps_per_side",
    "control_fwd_return", "partition",
)


def _pd():
    import pandas as pd
    return pd


def _np():
    import numpy as np
    return np


def build_panel(*, campaign_id: str = C.CAMPAIGN_ID):
    """Assemble the ML panel DataFrame from the frozen layer. Leak-free by
    the same alignment rule as the experiments: features at decision date d
    use data <= d; ``fwd_return`` is realised over (d, d+cadence]."""
    np, pd = _np(), _pd()
    registry = EN.load_market_registry(campaign_id)
    manifest = RL.load_manifest(campaign_id) or {"markets": {}}
    markets = sorted(m for m, row in manifest.get("markets", {}).items()
                     if row.get("state") == "OK")
    panel = EX.load_panel(markets, campaign_id)
    decisions = EX.decision_calendar(panel, CADENCE_SESSIONS)
    forward = EX.forward_return_matrix(panel, decisions)
    forward = forward.dropna(how="all")
    cot = EX.cot_signal_matrix(panel, forward.index)
    cot_matrix = cot.get("matrix") if cot.get("ok") else None

    # control: equal-weight basket forward return over live markets per date
    control = {}
    for d in forward.index:
        live = forward.loc[d].dropna()
        control[d] = float(live.mean()) if len(live) else float("nan")

    rows = []
    for market in markets:
        reg = registry["markets"][market]
        daily = panel[market]
        ret = daily["ret"]
        slope = daily["slope_ann"]
        cost_group = reg["cost_group"]
        for d in forward.index:
            fwd = forward.loc[d, market] if market in forward.columns \
                else float("nan")
            hist = ret.loc[ret.index <= d]
            if len(hist) < 63:
                continue
            held = daily["held"].loc[daily.index <= d]
            row_slope = slope.loc[slope.index <= d].dropna()
            cot_z = float("nan")
            if cot_matrix is not None and market in cot_matrix.columns \
                    and d in cot_matrix.index:
                cot_z = cot_matrix.loc[d, market]
                cot_z = float(cot_z) if np.isfinite(cot_z) else float("nan")
                if np.isfinite(cot_z):
                    cot_z = -cot_z  # store the raw commercial z, not the signal
            vol_row = daily["volume"].loc[daily.index <= d].dropna()
            oi_row = daily["open_interest"].loc[daily.index <= d].dropna()
            rows.append({
                "market_id": market,
                "contract_id": held.iloc[-1] if len(held) else None,
                "asset_class": reg["asset_class"],
                "economic_group": reg["economic_group"],
                "exchange": (reg["metadata"] or {}).get("exchange"),
                "currency": (reg["metadata"] or {}).get("currency"),
                "decision_date": str(d.date()),
                "observation_date": str(d.date()),
                "fwd_return": fwd,
                "ret_1m": EX._compound(hist.iloc[-21:]),
                "ret_3m": EX._compound(hist.iloc[-63:]),
                "ret_12m": EX._compound(hist.iloc[-252:]),
                "vol_63": float(hist.iloc[-63:].std()),
                "carry_slope_ann": (float(row_slope.iloc[-1])
                                    if len(row_slope) else float("nan")),
                "volume": float(vol_row.iloc[-1]) if len(vol_row) else float("nan"),
                "open_interest": (float(oi_row.iloc[-1])
                                  if len(oi_row) else float("nan")),
                "cot_commercial_z": cot_z,
                "has_carry": bool(len(row_slope)),
                "has_cot": bool(np.isfinite(cot_z)),
                "has_volume": bool(len(vol_row)),
                "has_open_interest": bool(len(oi_row)),
                "market_activity_state": reg["activity_state"],
                "pit_state": "SETTLEMENT_PUBLISHED_NEXT_MORNING",
                "cost_bps_per_side": C.COST_BPS_PER_SIDE.get(cost_group, 10.0),
                "control_fwd_return": control[d],
            })
    frame = pd.DataFrame(rows)
    if not len(frame):
        return frame

    # chronological partition with embargo GAPS between the splits
    dates = sorted(frame["decision_date"].unique())
    n = len(dates)
    train_end = int(n * PARTITION_SHARES[0])
    val_end = int(n * (PARTITION_SHARES[0] + PARTITION_SHARES[1]))
    embargo = max(EMBARGO_SESSIONS // CADENCE_SESSIONS, 1)

    def _part(date_str):
        i = dates.index(date_str)
        if i < train_end:
            return "TRAIN"
        if i < train_end + embargo:
            return "EMBARGO"
        if i < val_end:
            return "VALIDATION"
        if i < val_end + embargo:
            return "EMBARGO"
        return "TEST"

    frame["partition"] = frame["decision_date"].map(_part)
    return frame[list(PANEL_COLUMNS)]


def build(*, campaign_id: str = C.CAMPAIGN_ID,
          created_at: Optional[str] = None) -> dict:
    created = created_at or _dt.datetime.now(_dt.timezone.utc).isoformat()
    frame = build_panel(campaign_id=campaign_id)
    panel_path = r38.campaign_dir(campaign_id) / PANEL_NAME
    panel_path.parent.mkdir(parents=True, exist_ok=True)
    csv_bytes = frame.to_csv(index=False).encode("utf-8")
    panel_path.write_bytes(csv_bytes)

    manifest = RL.load_manifest(campaign_id) or {"markets": {}}
    payload = {
        "campaign_id": campaign_id,
        "created_at": created,
        "calculation_owner": CALCULATION_OWNER,
        "trains_a_model": TRAINS_A_MODEL,
        "panel_file": PANEL_NAME,
        "panel_sha256": _hashlib.sha256(csv_bytes).hexdigest(),
        "panel_rows": int(len(frame)),
        "panel_markets": (int(frame["market_id"].nunique())
                          if len(frame) else 0),
        "panel_first_decision": (str(frame["decision_date"].min())
                                 if len(frame) else None),
        "panel_last_decision": (str(frame["decision_date"].max())
                                if len(frame) else None),
        "columns": list(PANEL_COLUMNS),
        "cadence_sessions": CADENCE_SESSIONS,
        "embargo_sessions": EMBARGO_SESSIONS,
        "partition_shares": list(PARTITION_SHARES),
        "partition_counts": (frame["partition"].value_counts().to_dict()
                             if len(frame) else {}),
        "alignment_rule": (
            "features at decision date d use data dated <= d; fwd_return is "
            "realised over (d, d+cadence] and is the ONLY target column"),
        "missingness_policy": "masks, never silent fills",
        "sequence_inputs": {
            "note": "full OHLC/settlement/volume/OI sequences live in the "
                    "native contract layer, one CSV per market, checksummed",
            "layer_manifest": {
                m: {"sha256": row.get("sha256"), "rows": row.get("rows")}
                for m, row in manifest.get("markets", {}).items()
                if row.get("state") == "OK"},
        },
        "survivorship_note": (
            "each market's contract series is complete including expired "
            "contracts; the MARKET list is current-composition (terminated "
            "markets absent from the delivered package)"),
        "roll_policy": C.ROLL_POLICY,
        "cost_model_state": C.COST_MODEL_STATE,
    }
    return r38.artifact_body(SCHEMA, payload)


def path_for(campaign_id: str = C.CAMPAIGN_ID):
    return r38.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict) -> None:
    path = path_for(body["campaign_id"])
    path.parent.mkdir(parents=True, exist_ok=True)
    r38.write_json(path, body)


def load(campaign_id: str = C.CAMPAIGN_ID) -> Optional[dict]:
    path = path_for(campaign_id)
    if not path.exists():
        return None
    return r38.read_json(path)
