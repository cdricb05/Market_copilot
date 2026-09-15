"""alpha_agent.r64.carry - genuine carry information from DISTINCT dated contracts.

Carry is the return earned if nothing changes: the interest differential in
FX, the calendar-spread carry in rates, the roll yield in commodities,
dividend-minus-financing in equity index futures, contango in volatility. It
can only be observed from TWO DISTINCT CONTRACTS of the same market. R59's
carry features were invalidated because the "second contract" turned out to be
a back-adjusted continuous series standing in for the front contract (the
``close_b`` pseudo-curve). R64 measures that defect per market and refuses it.

Three constructions, all formula variants of the ONE ``CARRY`` dimension:

    CARRY               the R63 block verbatim (slope_ann, 21-session change)
    CARRY_TO_RISK       slope_ann / trailing 63-session realised volatility
    CARRY_CLASS_NEUTRAL slope_ann standardised within its asset class per
                        session (CROSS_ASSET scope only) - the genuinely
                        distinct implementation of the cross-asset carry book,
                        whose R63 form was decided by the scale of one class

Read-only over the R41 store through the R63 panel owner; nothing is fetched.
"""
from __future__ import annotations

import numpy as np

from alpha_agent.r63 import features as FE
from alpha_agent.r63 import panels as P

from . import (AC_CROSS_ASSET, DIM_CARRY_CLASS_NEUTRAL, DIM_CARRY_TO_RISK,
               DIM_CURVE_CARRY, write_artifact)

CALCULATION_OWNER = "alpha_agent.r64.carry"
ARTIFACT_NAME = "r64_distinct_contract_evidence.json"
PPY = 252.0

#: A market whose front and second dated settlements coincide on more than this
#: share of sessions is not a curve; it is the R59 pseudo-curve and is refused.
PSEUDO_CURVE_SHARE_MAX = 0.50
STATE_OK = "DISTINCT_CONTRACTS"
STATE_PSEUDO = "PSEUDO_CURVE_REFUSED"
MIN_CLASS_MEMBERS = 3


class PseudoCurveError(RuntimeError):
    """Raised when a market's carry would be read from a non-distinct contract."""


def market_evidence(df) -> dict:
    """Distinct-contract evidence for ONE market's daily curve frame."""
    ok = df["c1"].notna() & df["c2"].notna()
    n = int(ok.sum())
    if n == 0:
        return {"rows": int(len(df)), "rows_with_two_contracts": 0,
                "share_c1_equals_c2": None, "share_slope_zero": None,
                "ret1_ret2_correlation": None, "state": STATE_PSEUDO,
                "why": "no session carries two dated settlements"}
    same = float((df.loc[ok, "c1"].to_numpy() == df.loc[ok, "c2"].to_numpy()).mean())
    zero = float((np.abs(df.loc[ok, "slope_ann"].to_numpy(dtype=float)) < 1e-12).mean())
    r1, r2 = df["ret1"].to_numpy(dtype=float), df["ret2"].to_numpy(dtype=float)
    fin = np.isfinite(r1) & np.isfinite(r2)
    corr = float(np.corrcoef(r1[fin], r2[fin])[0, 1]) if fin.sum() > 30 else None
    state = STATE_OK if same <= PSEUDO_CURVE_SHARE_MAX else STATE_PSEUDO
    return {"rows": int(len(df)), "rows_with_two_contracts": n,
            "share_c1_equals_c2": same, "share_slope_zero": zero,
            "ret1_ret2_correlation": corr, "state": state,
            "why": ("front and second dated settlements are distinct on %.1f%% of sessions"
                    % (100.0 * (1.0 - same)) if state == STATE_OK else
                    "front and second settlements coincide on %.1f%% of sessions: a "
                    "pseudo-curve, not a term structure" % (100.0 * same))}


def distinct_contract_evidence(*, markets: list | None = None, write: bool = True,
                               refuse: bool = True) -> dict:
    """Measure every admitted market (or ``markets``) and publish the evidence.

    ``refuse`` raises :class:`PseudoCurveError` when any market is a
    pseudo-curve, so no carry cell can be built on one by accident.
    """
    import pandas as pd
    uni = P.futures_universe()
    admitted = {a["market"]: a for a in uni["admitted"]}
    names = sorted(markets or admitted)
    per = {}
    for m in names:
        p = P.R41_CURVES / ("%s_daily.csv" % m)
        if not p.exists():
            per[m] = {"state": STATE_PSEUDO, "why": "no daily curve file"}
            continue
        df = pd.read_csv(p, usecols=["date", "ret1", "ret2", "c1", "c2", "slope_ann"])
        ev = market_evidence(df)
        ev["asset_class"] = (admitted.get(m) or {}).get("asset_class")
        per[m] = ev
    refused = sorted(m for m, e in per.items() if e["state"] != STATE_OK)
    body = {"schema": "r64_distinct_contract_evidence/1", "calculation_owner": CALCULATION_OWNER,
            "rule": ("carry is read from the SECOND dated contract against the FRONT; a "
                     "market whose two settlements coincide on more than %.0f%% of sessions "
                     "is a pseudo-curve (the R59 close_b defect) and is refused"
                     % (100.0 * PSEUDO_CURVE_SHARE_MAX)),
            "forbidden_construction": "R59 close_b back-adjusted pseudo-curve",
            "n_markets": len(per), "n_refused": len(refused), "refused": refused,
            "max_share_c1_equals_c2": max((e.get("share_c1_equals_c2") or 0.0)
                                          for e in per.values()) if per else None,
            "markets": per, "verdict": ("ALL_DISTINCT" if not refused else "PSEUDO_CURVE_PRESENT")}
    if write:
        write_artifact(ARTIFACT_NAME, body)
    if refuse and refused:
        raise PseudoCurveError("pseudo-curve markets refused: %s" % refused)
    return body


# --------------------------------------------------------------------------- #
# Blocks (n_inst x n_dates x k), aligned to the R63 feature grid
# --------------------------------------------------------------------------- #
def curve_carry_block(F: dict) -> np.ndarray:
    """The R63 CARRY block, byte-identical (delegated, never re-derived)."""
    return FE.futures_carry_block(F)


def carry_to_risk_block(F: dict) -> np.ndarray:
    """slope_ann per unit of trailing 63-session realised volatility, and its
    21-session change. Uses data through each column's own session only."""
    s = F["slope_ann"]
    vol = FE._roll(F["ret1"], 63, "std") * np.sqrt(PPY)
    with np.errstate(invalid="ignore", divide="ignore"):
        ctr = s / np.where(np.isfinite(vol) & (vol > 0), vol, np.nan)
    return FE._stack(ctr, ctr - FE._lag(ctr, 21))


def class_neutral_carry_block(F: dict, class_of: np.ndarray) -> np.ndarray:
    """slope_ann standardised WITHIN asset class on each session.

    A class with fewer than ``MIN_CLASS_MEMBERS`` finite members on a session
    yields NaN for that session (coverage is measured, never filled). Uses the
    session's own cross-section only: nothing from a later date."""
    s = np.asarray(F["slope_ann"], dtype=float)
    z = np.full_like(s, np.nan)
    classes = np.asarray(class_of)
    for c in sorted(set(classes.tolist())):
        rows = np.where(classes == c)[0]
        block = s[rows]
        fin = np.isfinite(block)
        n = fin.sum(axis=0)
        with np.errstate(invalid="ignore", divide="ignore"):
            mean = np.where(n > 0, np.nansum(block, axis=0) / np.maximum(n, 1), np.nan)
            dev = block - mean[None, :]
            var = np.where(n > 1, np.nansum(dev * dev, axis=0) / np.maximum(n - 1, 1), np.nan)
            sd = np.sqrt(var)
            zz = dev / np.where(sd > 0, sd, np.nan)
        zz[:, n < MIN_CLASS_MEMBERS] = np.nan
        z[rows] = zz
    return FE._stack(z, z - FE._lag(z, 21))


def scope_frame(F: dict, rows: list) -> dict:
    """The 2-D substrate arrays restricted to ``rows`` (row indices into F)."""
    n_m = len(F["markets"])
    return {k: (v[rows] if isinstance(v, np.ndarray) and v.ndim == 2 and v.shape[0] == n_m else v)
            for k, v in F.items()}


def inject_variant_blocks(ds: dict, F: dict, rows: list, class_of: np.ndarray) -> dict:
    """Return a COPY of an R63 dataset with the R64 carry variants added as
    blocks. The R63 blocks are untouched; the CARRY block is the R63 one."""
    sub = scope_frame(F, rows)
    blocks = dict(ds["blocks"])
    assert DIM_CURVE_CARRY in blocks, "the R63 CARRY block must exist"
    blocks[DIM_CARRY_TO_RISK] = carry_to_risk_block(sub)
    if ds.get("scope") == AC_CROSS_ASSET:
        blocks[DIM_CARRY_CLASS_NEUTRAL] = class_neutral_carry_block(sub, class_of)
    out = dict(ds)
    out["blocks"] = blocks
    out["class_of"] = np.asarray(class_of)
    return out
