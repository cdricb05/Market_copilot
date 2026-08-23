"""alpha_agent.r39.target_factory - the TARGET_REGISTRY owner.

Learning is not restricted to next-period raw return, and it is not allowed
to explode combinatorially either. This module declares the frozen target
set, materialises each target as a panel column, and freezes the registry.

Every target is defined on information realised STRICTLY after the decision
date (the ``fwd_*`` columns built by universal_state), and the tail /
distributional targets are expressed as losses over the excess target
rather than as separate label columns, which is what keeps the registry
small (``NO_COMBINATORIAL_TARGET_EXPLOSION``).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .. import r39
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r39.target_factory"
SCHEMA = "r39_target_registry/1"
ARTIFACT_NAME = "target_registry.json"

#: target_id -> (horizon sessions, definition, label column produced)
TARGET_DEFINITIONS = {
    "EXCESS_21": (21, "fwd_21 - control_fwd_21 (after-control excess; costs "
                      "are charged at the EXPRESSION level on traded "
                      "notional)", "tgt_excess_21"),
    "EXCESS_63": (63, "fwd_63 - control_fwd_63", "tgt_excess_63"),
    "EXCESS_5": (5, "fwd_5 - control_fwd_5 (VX weekly lane)", "tgt_excess_5"),
    "RANK_21": (21, "cross-sectional percentile rank of fwd_21 within "
                    "(decision month, lane scope)", "tgt_rank_21"),
    "SIGN_21": (21, "1{fwd_21 - control_fwd_21 > 0}", "tgt_sign_21"),
    "VOL_21": (21, "realised volatility over the 21 sessions after the "
                   "decision (fwd_vol_21)", "tgt_vol_21"),
    "Q10_21": (21, "the 10% conditional quantile of EXCESS_21, learned via "
                   "pinball loss on tgt_excess_21 - a loss, not a new "
                   "label", "tgt_excess_21"),
}


def materialise(panel: pd.DataFrame, *, scope_col: str = "asset_class"
                ) -> pd.DataFrame:
    """Add every target column this panel's inputs support. Masks stay NaN."""
    p = panel
    if {"fwd_21", "control_fwd_21"} <= set(p.columns):
        p["tgt_excess_21"] = p["fwd_21"] - p["control_fwd_21"]
        p["tgt_sign_21"] = np.where(
            np.isfinite(p["tgt_excess_21"]),
            (p["tgt_excess_21"] > 0).astype(float), np.nan)
        key = [p["decision_date"].dt.to_period("M"), p[scope_col]]
        p["tgt_rank_21"] = p.groupby(key)["fwd_21"].rank(pct=True)
        p.loc[~np.isfinite(p["fwd_21"]), "tgt_rank_21"] = np.nan
    if {"fwd_63", "control_fwd_63"} <= set(p.columns):
        p["tgt_excess_63"] = p["fwd_63"] - p["control_fwd_63"]
    if {"fwd_5", "control_fwd_5"} <= set(p.columns):
        p["tgt_excess_5"] = p["fwd_5"] - p["control_fwd_5"]
    if "fwd_vol_21" in p.columns:
        p["tgt_vol_21"] = p["fwd_vol_21"]
    return p


def build_registry(campaign_id: str = C.CAMPAIGN_ID) -> dict:
    body = r39.artifact_body(SCHEMA, {
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "targets": {
            tid: {"horizon_sessions": h, "definition": d, "column": col}
            for tid, (h, d, col) in TARGET_DEFINITIONS.items()},
        "n_targets": len(TARGET_DEFINITIONS),
        "distributional_targets_are_losses": True,
        "no_combinatorial_explosion": C.NO_COMBINATORIAL_TARGET_EXPLOSION,
        "targets_realised_strictly_after_decision": True,
    })
    body["target_registry_hash"] = r39.sha(body)
    r39.write_json(r39.campaign_dir(campaign_id) / ARTIFACT_NAME, body)
    return body
