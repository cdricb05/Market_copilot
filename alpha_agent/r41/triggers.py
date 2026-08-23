"""alpha_agent.r41.triggers - Track 2: the material-update trigger
architecture (research design; R41 mutates nothing operational).

The eventual operating sequence is UPDATE -> INCREMENTAL FEATURE REFRESH ->
AFFECTED SIGNAL RE-SCORE -> FULL OPPORTUNITY FRONTIER -> PORTFOLIO
REASSESSMENT, possibly several times per session. This module freezes the
per-family MATERIAL_UPDATE definitions (from the contract) and MEASURES
how often each trigger actually fires on the owned history, so the future
near-real-time loop is sized by evidence rather than by enthusiasm.
"""
from __future__ import annotations

import datetime as _dt

import numpy as np
import pandas as pd

from . import artifact_body, campaign_dir, sha, write_json
from . import contract as C
from . import curve_state as CS

CALCULATION_OWNER = "alpha_agent.r41.triggers"
ARTIFACT_NAME = "material_update_triggers.json"


def _fires_per_year(mask: pd.Series) -> float:
    m = mask.dropna()
    if not len(m):
        return float("nan")
    years = max((m.index[-1] - m.index[0]).days / 365.25, 0.1)
    return float(m.sum() / years)


def measure() -> dict:
    out = {}
    zn = CS.load_daily("ZN")
    if zn is not None:
        s = zn["ret1"]
        vol = s.rolling(60, min_periods=30).std()
        idxp = (1.0 + s.fillna(0)).cumprod()
        z = (idxp - idxp.rolling(120, min_periods=60).mean()) \
            / idxp.rolling(120, min_periods=60).std()
        out["RATES_price_z_break_|z|>2"] = _fires_per_year(z.abs() > 2)
        out["RATES_vol_regime_shift_2x"] = _fires_per_year(
            (vol / vol.shift(21) > 1.5) | (vol / vol.shift(21) < 0.67))
    cl = CS.load_daily("CL")
    if cl is not None:
        sl = cl["slope_ann"]
        out["COMMODITY_slope_1sd_move"] = _fires_per_year(
            (sl.diff().abs() > sl.diff().rolling(250).std()))
        out["COMMODITY_EIA_reports"] = 52.0
        out["COMMODITY_COT_reports"] = 52.0
    vx = CS.load_daily("VX")
    if vx is not None:
        out["VOL_term_structure_sign_change"] = _fires_per_year(
            np.sign(vx["slope_ann"]).diff().abs() > 0)
    out["CRYPTO_funding_intervals_per_year"] = 3 * 365.0
    out["MACRO_scheduled_releases_per_year"] = "CPI/NFP/FOMC etc: ~40 "
    return out


def build() -> dict:
    body = artifact_body("r41_material_update_triggers/1", {
        "calculation_owner": CALCULATION_OWNER,
        "built_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "definitions": C.MATERIAL_UPDATE,
        "operating_sequence": C.OPERATING_SEQUENCE,
        "measured_trigger_frequencies_per_year": measure(),
        "research_only": True,
        "operational_wiring": "NONE - attaching any trigger to the "
                              "operational reassessment loop is a separate "
                              "governed release",
    })
    body["triggers_hash"] = sha(body)
    write_json(campaign_dir() / ARTIFACT_NAME, body, immutable=False)
    return body
