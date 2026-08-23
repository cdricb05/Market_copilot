"""alpha_agent.r39.trade_space_ext - Track F: the trade-expression frontier.

Extends the frozen v1 trade space with the economically distinct structures
the untested R36/R38 cells require, WITHOUT an unconstrained pair
explosion - every new structure trades only inside declared economic
groups or against a declared observable gate:

* ``GROUP_RV``      - within-group relative value: demeaned prediction
                      ranks across ALL members of each declared economic
                      group (not only best-vs-worst), scaled by inverse
                      trailing volatility so low-vol legs (short-duration
                      rates) do not free-ride, self-financed per group,
                      versus risk-matched cash. This is the curve-RV /
                      inter-commodity-RV / cross-country-RV structure.
* ``XS_LS_GATED`` /
  ``TS_GATED``      - the v1 expressions gated by a declared OBSERVABLE
                      macro regime (expanding-median split of a lagged
                      macro column); the control is the control of the
                      base expression under the SAME gate.
* ``XS_LS_ABSTAIN`` - the v1 cross-section with a declared abstention
                      rule: names whose |cross-sectional prediction z| is
                      below 0.5 are not traded (the engine may prefer NO
                      TRADE).

Blocked structures are named, never silent:

* butterflies need a third contract; the frozen R38 layer carries front
  and second series only (``NO_THIRD_CONTRACT_IN_FROZEN_LAYER``);
* sector-neutral equity books need a sector column the frozen R30 dataset
  does not carry (``NO_SECTOR_COLUMN_IN_FROZEN_DATASET``);
* free pairwise cointegration outside declared groups stays excluded by
  the frozen no-pair-explosion contract; the residual-relationship
  hypothesis is carried by the walk-forward LATENT residual-momentum
  representation and by GROUP_RV inside declared groups.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .trade_space import _book_path, passive_ew_control, ts_outright, \
    xs_long_short

CALCULATION_OWNER = "alpha_agent.r39.trade_space_ext"

ABSTAIN_Z = 0.5

EXPRESSION_CONTROLS_EXT = {
    "GROUP_RV": "RISK_MATCHED_CASH",
    "XS_LS_GATED": "RISK_MATCHED_CASH",
    "TS_GATED": "VOL_MATCHED_PASSIVE_EW_SAME_SCOPE_SAME_GATE",
    "XS_LS_ABSTAIN": "RISK_MATCHED_CASH",
    "XS_LS_SECTOR_NEUTRAL": "RISK_MATCHED_CASH",
}

BLOCKED_STRUCTURES = {
    "CALENDAR_BUTTERFLY": "NO_THIRD_CONTRACT_IN_FROZEN_LAYER",
    "FREE_PAIRWISE_COINTEGRATION":
        "EXCLUDED_BY_NO_PAIR_EXPLOSION_CONTRACT - carried inside declared "
        "groups by GROUP_RV and by the latent residual-momentum "
        "representation",
}

#: An earlier draft blocked sector-neutral books as
#: NO_SECTOR_COLUMN_IN_FROZEN_DATASET; the phase-24 source panel in fact
#: carries a sectors axis, so the structure is EXECUTED instead - the
#: corrected claim is recorded here, not silently rewritten.
SECTOR_NEUTRAL_UNBLOCKED_BY = "phase-24 panel sectors axis via the " \
    "position-indexed identity bridge"


def vol_scaled_group_rv(pred: pd.DataFrame, fwd: pd.DataFrame,
                        cost_bps: pd.Series, groups: dict,
                        vol: pd.DataFrame, *,
                        cost_multiplier: float = 1.0) -> dict:
    """Within each declared group: weights proportional to the demeaned
    prediction rank, scaled by inverse trailing vol, self-financed, equal
    risk budget per live group."""
    W = pd.DataFrame(0.0, index=pred.index, columns=pred.columns)
    iv = (1.0 / vol.replace(0.0, np.nan)).reindex_like(pred)
    live = []
    for gname, members in groups.items():
        cols = [c for c in members if c in pred.columns]
        if len(cols) < 2:
            continue
        sub = pred[cols].where(np.isfinite(fwd[cols]))
        ranks = sub.rank(axis=1, pct=True)
        centered = ranks.sub(ranks.mean(axis=1), axis=0)
        scaled = centered * iv[cols].div(iv[cols].mean(axis=1), axis=0) \
            .fillna(1.0)
        # re-centre after scaling so every group leg is self-financed
        scaled = scaled.sub(scaled.mean(axis=1), axis=0)
        gross = scaled.abs().sum(axis=1).replace(0.0, np.nan)
        ok = sub.notna().sum(axis=1) >= 2
        w = scaled.div(gross, axis=0).where(ok, 0.0).fillna(0.0)
        W[cols] = W[cols].add(w, fill_value=0.0)
        live.append(gname)
    W = W / max(len(live), 1)
    out = _book_path(W, fwd, cost_bps, cost_multiplier=cost_multiplier)
    out["expression"] = "GROUP_RV"
    out["groups_used"] = live
    return out


def regime_gate(macro: pd.Series, index: pd.Index, columns,
                mode: str) -> pd.DataFrame:
    """Boolean gate from an OBSERVABLE (already-lagged) macro column: the
    expanding median split, broadcast over the book's instruments."""
    s = macro.dropna().sort_index()
    med = s.expanding(min_periods=24).median()
    if mode == "below_median":
        flag = s < med
    elif mode == "above_median":
        flag = s > med
    else:
        raise ValueError(mode)
    aligned = flag.reindex(pd.DatetimeIndex(index), method="ffill") \
        .fillna(False)
    return pd.DataFrame({c: aligned.to_numpy() for c in columns},
                        index=index)


def xs_gated(pred, fwd, cost_bps, gate, *, cost_multiplier: float = 1.0):
    out = xs_long_short(pred, fwd, cost_bps, gate=gate,
                        cost_multiplier=cost_multiplier)
    out["expression"] = "XS_LS_GATED"
    return out


def ts_gated(pred, fwd, cost_bps, gate, *, cost_multiplier: float = 1.0):
    out = ts_outright(pred, fwd, cost_bps, gate=gate,
                      cost_multiplier=cost_multiplier)
    out["expression"] = "TS_GATED"
    ctrl_fwd = fwd.where(gate.reindex_like(fwd).fillna(False))
    ctrl = passive_ew_control(ctrl_fwd, cost_bps,
                              cost_multiplier=cost_multiplier)
    out["gated_control_net"] = ctrl["net"]
    return out


def xs_abstain(pred, fwd, cost_bps, *, cost_multiplier: float = 1.0):
    """The v1 cross-section, refusing to trade weak-conviction names."""
    z = pred.sub(pred.mean(axis=1), axis=0).div(
        pred.std(axis=1, ddof=1).replace(0.0, np.nan), axis=0)
    gate = z.abs() >= ABSTAIN_Z
    out = xs_long_short(pred, fwd, cost_bps, gate=gate,
                        cost_multiplier=cost_multiplier)
    out["expression"] = "XS_LS_ABSTAIN"
    return out
