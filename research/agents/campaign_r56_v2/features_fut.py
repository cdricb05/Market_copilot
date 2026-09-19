r"""features_fut - the four futures / cross-asset FEATURE SETS of campaign R56_V2.

RESEARCH ONLY. PAPER ONLY. NO ORDERS. ORDERS DISABLED. AUTOMATION OFF. MANUAL REVIEW.

    FS1  FS_FUT_R38_BASIS_MOM_V1       on FUT_R38_COMMODITY_V1      (P1)  bm_252 + 2 skeptic diagnostics
    FS2  FS_FUT_R38_OI_GROWTH_V1       on FUT_R38_COMMODITY_OI_V1   (P2)  oi_agg_growth_252
    FS3  FS_XA_TOT_RV_V1               on XA_TOT_PAIRS_V1           (P3)  basket_ret, beta_volmatch_63,
                                                                          tot_cheapness_z_63
    FS4  FS_FUT_R38_BASIS_MOM_FIN_V1   on FUT_R38_FINANCIALS_V1     (P4)  bm_252 (identical construction),
                                                                          asset_class_group, 1 diagnostic

This module computes FEATURE VALUES and nothing else. It never reads a forward return, never builds a weight,
a book, an IC or any performance number, and owns no registry, gate, queue, evaluator, mask or forward clock.
``ret`` / ``ret2`` enter ONLY as inputs over TRAILING slot windows. The durable record of a feature set is the
AGENTS_V2_FEATURES_PUBLISHED event written through ``scripts\alpha_agents_v2.py publish_features``.

The definitions are the director's and are FROZEN (director\agenda.json P1-P4, director\foundation_requests.json
FS1-FS4, and director\phase1b_rulings.json R2 + ``amendments_to_work_orders``, which GOVERNS where they differ).
No lookback, threshold, filter or variant here may be tuned.

Definitions used by every feature (rulings, ``definitions_used_by_every_ruling``)
--------------------------------------------------------------------------------
* grid slot t     an index of the union session grid of ``data_r38.load_certified_layer()`` (15 calendars).
* own session     ``ret[i, s]`` finite. The ONLY definition of a market's session in this campaign.
* slot window     a lookback window is ALWAYS a range of grid SLOTS [t-far, t-near]; completeness is ALWAYS
                  counted on the market's OWN sessions inside it. A holiday is not missing data.

TIMING (agenda G6). A feature at decision slot t reads layer columns <= t-1 ONLY and aggregate open interest
columns <= t-2 ONLY (``data_r38.SIGNAL_LAG_SESSIONS`` = 1, ``data_r38.OI_LATEST_USABLE_OFFSET`` = 2). The
evaluator enters the position at the settlement of slot t (R1: or at the market's own next settlement), i.e.
strictly after the newest input was observable.

NEGATIVE-INDEX GUARD. Every window is cut with explicit non-negative bounds. A ``t`` whose window would start
before column 0, or would need a column the array does not hold, is REFUSED (``FeatureWindowError``): a numpy
negative index silently reads the END of the array - the future - and a negative slice start silently yields
an empty or wrapped window (an empty product is a fabricated 0.0).

THE ONE MASK. Features are computed for every LAYER row (float64[n_layer_markets], NaN where the feature is
invalid). Validity is the feature's own completeness rule (R2) - NOT eligibility. TRADED_MAJORITY (R3a),
certification and cost are UNIVERSE rules: a feature can be finite on a market that is not eligible (AFB, AWM,
EUA ...). Signal agents rank on ``universe_fut.eligible_at(u, t) & live``, NEVER on ``isfinite(feature)``.
This module neither copies nor rebuilds a mask; ``mask_reference`` names the universe agent's artifact + sha256.

Survivorship (R4). Measured on the vendor's CURRENT-composition market list (expired contracts retained:
17,195 held contracts; discontinued markets NOT retained). No futures feature set is survivorship-free.

Usage (PowerShell)
    & C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe `
        C:\Users\binis\paper_trader\research\agents\campaign_r56_v2\features_fut.py selftest
    & ... features_fut.py catalog

Import
    import sys
    sys.path.insert(0, r"C:\Users\binis\paper_trader")
    sys.path.insert(0, r"C:\Users\binis\paper_trader\research\agents\campaign_r56_v2")
    import data_r38, universe_fut, features_fut as FF
    layer = data_r38.load_certified_layer()
    agg = data_r38.load_aggregate_oi(layer)
    u3 = universe_fut.load_universe("XA_TOT_PAIRS_V1", layer=layer)
    bm = FF.feature_bm_252(layer, t)                   # float64[69]
    g = FF.feature_oi_growth_252(layer, agg, t)        # float64[69]
    tot = FF.feature_tot_rv(layer, u3, t)              # {"S": f8[3], "beta": f8[3], "n_common": i8[3]}
"""
from __future__ import annotations

import hashlib
import json
import math
import sys
from pathlib import Path
from typing import Optional

import numpy as np

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parents[2]
for _p in (str(_REPO), str(_HERE)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import data_r38                                                   # noqa: E402
import universe_fut as UF                                         # noqa: E402

SAFETY = ("RESEARCH ONLY", "PAPER ONLY", "NO ORDERS", "ORDERS DISABLED", "AUTOMATION OFF", "MANUAL REVIEW")

FS1 = "FS_FUT_R38_BASIS_MOM_V1"
FS2 = "FS_FUT_R38_OI_GROWTH_V1"
FS3 = "FS_XA_TOT_RV_V1"
FS4 = "FS_FUT_R38_BASIS_MOM_FIN_V1"
FEATURE_SET_IDS = (FS1, FS2, FS3, FS4)
FEATURE_SET_VERSION = 1
FEATURE_SET_UNIVERSE = {FS1: UF.U1, FS2: UF.U2, FS3: UF.U3, FS4: UF.U4}
FEATURE_SET_WORK_ORDER = {FS1: "FS1", FS2: "FS2", FS3: "FS3", FS4: "FS4"}
FEATURE_SET_USED_BY = {FS1: ["P1"], FS2: ["P2"], FS3: ["P3"], FS4: ["P4"]}

LAYER_DATASET_ID = data_r38.DATASET_ID
AGG_OI_DATASET_ID = data_r38.AGG_OI_DATASET_ID

# --------------------------------------------------------------------------- #
# FROZEN by the director before any signal result existed. NOT tunable.
# Windows are (far, near): grid slots [t - far, t - near] inclusive. The rule constants are the universe
# agent's (ONE definition for the mask and the feature); the literal values are re-asserted below so that a
# silent change on either side is caught at import.
# --------------------------------------------------------------------------- #
BM_WINDOW = UF.PAIRED_WINDOW                  # (252, 1)
BM_MIN_PAIRED = UF.PAIRED_MIN_SESSIONS        # 227, an ABSOLUTE count (R2)
MOM_WINDOW = (252, 22)                        # FS1 work order: "compounded ret over sessions t-252..t-22"
SLOPE_LAG = 1                                 # "slope_ann at t-1"
OI_RECENT_WINDOW = UF.OI_RECENT_WINDOW        # (22, 2)
OI_BASE_WINDOW = UF.OI_BASE_WINDOW            # (274, 254)
OI_MIN_OWN_SESSIONS = UF.OI_MIN_OWN_SESSIONS  # 15
OI_MIN_FINITE_SHARE = UF.OI_MIN_FINITE_SHARE  # (9, 10)
TOT_WINDOW = UF.LEG_WINDOW                    # (63, 1)
TOT_MIN_OWN_SESSIONS = UF.LEG_MIN_OWN_SESSIONS  # 55
TOT_CLIP = 2.0
#: The ONLY normaliser WRITTEN anywhere is the agenda's sqrt(63) (agenda.json P3 ``signal``,
#: foundation_requests.json FS3). phase1b_rulings.json R2 / FS3_tot restate which sessions every std and the
#: numerator run over and are SILENT on the sqrt factor, so the agenda's literal formula is implemented.
#: FLAGGED for the director (see catalog ``open_question``). Not chosen by looking at any value.
TOT_NORMALISER_SLOTS = 63
#: numpy population std (ddof = 0): the estate's convention (alpha_agent.r59.native._slope_z uses np.nanstd,
#: evaluator.py uses ndarray.std()). The agenda does not state ddof. beta is a ratio of two stds over the same
#: sessions, so ddof cancels in beta; it scales the z denominator by sqrt((n-1)/n).
TOT_STD_DDOF = 0

assert BM_WINDOW == (252, 1) and BM_MIN_PAIRED == 227
assert OI_RECENT_WINDOW == (22, 2) and OI_BASE_WINDOW == (274, 254)
assert OI_MIN_OWN_SESSIONS == 15 and OI_MIN_FINITE_SHARE == (9, 10)
assert TOT_WINDOW == (63, 1) and TOT_MIN_OWN_SESSIONS == 55
assert data_r38.SIGNAL_LAG_SESSIONS == 1 and data_r38.OI_LATEST_USABLE_OFFSET == 2
for _w in (BM_WINDOW, MOM_WINDOW, TOT_WINDOW):
    assert _w[0] >= _w[1] >= data_r38.SIGNAL_LAG_SESSIONS         # G6: newest layer input is t-1
for _w in (OI_RECENT_WINDOW, OI_BASE_WINDOW):
    assert _w[0] >= _w[1] >= data_r38.OI_LATEST_USABLE_OFFSET     # G6: newest open interest is t-2

#: Smallest decision slot at which the feature's window lies inside the array. Below it the call is REFUSED.
MIN_SAFE_T = {"bm_252": BM_WINDOW[0], "mom_252_21_lag1": MOM_WINDOW[0], "slope_ann_lag1": SLOPE_LAG,
              "oi_agg_growth_252": OI_BASE_WINDOW[0], "tot_cheapness_z_63": TOT_WINDOW[0],
              "beta_volmatch_63": TOT_WINDOW[0], "basket_ret": TOT_WINDOW[0]}

#: Offset of the NEWEST declared input: the feature reads nothing later than slot t - offset, so an array of
#: n columns serves every t <= n - 1 + offset. Derived from the frozen windows; no new literal.
_NEWEST_INPUT_OFFSET = {"bm_252": BM_WINDOW[1], "mom_252_21_lag1": MOM_WINDOW[1], "slope_ann_lag1": SLOPE_LAG,
                        "oi_agg_growth_252": OI_RECENT_WINDOW[1], "tot_cheapness_z_63": TOT_WINDOW[1],
                        "beta_volmatch_63": TOT_WINDOW[1], "basket_ret": TOT_WINDOW[1]}

TIMING_STATEMENT = (
    "a feature at decision slot t reads r38_native_contract_layer_v4 columns <= t-1 only and r38_aggregate_oi_v1 "
    "columns <= t-2 only (agenda G6; data_r38.SIGNAL_LAG_SESSIONS = 1, OI_LATEST_USABLE_OFFSET = 2). The "
    "evaluator enters at the settlement of slot t (R1 ENTER_NEXT_OWN_SETTLE: a market closed at t is entered at "
    "its own next settlement), strictly after the newest input was observable.")


class FeatureWindowError(ValueError):
    """The feature's slot window does not lie inside the array at this ``t``. A governed refusal: a negative
    index would wrap to the END of the array (the future)."""


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
def file_sha256(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def module_sha256() -> str:
    return file_sha256(Path(__file__).resolve())


def code_bindings() -> dict:
    """sha256 of this module and of every module whose behaviour a feature value depends on."""
    from alpha_agent.r59 import native

    return {"features_fut.py": module_sha256(),
            "universe_fut.py": file_sha256(Path(UF.__file__).resolve()),
            "data_r38.py": file_sha256(Path(data_r38.__file__).resolve()),
            "alpha_agent/r59/native.py": file_sha256(Path(native.__file__).resolve())}


def _t_int(t) -> int:
    if isinstance(t, (bool, np.bool_)) or not isinstance(t, (int, np.integer)):
        raise TypeError("decision slot t must be an integer grid slot, got %r" % (type(t),))
    return int(t)


def _f8(arr, what: str) -> np.ndarray:
    a = np.asarray(arr)
    if a.dtype != np.float64 or a.ndim != 2:
        raise ValueError("%s must be float64[n_markets, n_slots], got %s%s" % (what, a.dtype, list(a.shape)))
    return a


def _window(arr: np.ndarray, t: int, window, what: str) -> np.ndarray:
    """``arr[:, t-far : t-near+1]`` with EXPLICIT non-negative bounds. Never a negative index, never a clamp:
    a window that does not lie inside the array is refused, not shortened."""
    far, near = int(window[0]), int(window[1])
    lo, hi = t - far, t - near + 1
    n = arr.shape[1]
    if lo < 0:
        raise FeatureWindowError(
            "%s at t=%d needs column %d: the window [t-%d, t-%d] starts before column 0 (a negative index would "
            "wrap to the END of the array = the future)" % (what, t, lo, far, near))
    if hi > n:
        raise FeatureWindowError(
            "%s at t=%d needs column %d but the array holds columns 0..%d" % (what, t, hi - 1, n - 1))
    return arr[:, lo:hi]


def _same_rows(*arrays) -> int:
    rows = {int(a.shape[0]) for a in arrays}
    if len(rows) != 1:
        raise ValueError("arrays disagree on the number of layer rows: %s" % sorted(rows))
    return rows.pop()


# --------------------------------------------------------------------------- #
# FS1 / FS4  bm_252  (SIGNAL, lag 1)
# --------------------------------------------------------------------------- #
def bm_252_detail(layer: dict, t: int) -> dict:
    """Every quantity behind ``bm_252`` at decision slot t (all float64/int64[n_layer_markets]).

    ``n_paired``   own sessions in slots [t-252, t-1] on which BOTH ret and ret2 are finite
    ``prod_held`` / ``prod_deferred``   prod(1 + ret) / prod(1 + ret2) over those PAIRED sessions
    ``valid``      n_paired >= 227
    ``bm``         prod_held - prod_deferred where valid, else NaN
    """
    t = _t_int(t)
    ret, ret2 = _f8(layer["ret"], "layer['ret']"), _f8(layer["ret2"], "layer['ret2']")
    _same_rows(ret, ret2)
    r1 = _window(ret, t, BM_WINDOW, "bm_252 (ret)")
    r2 = _window(ret2, t, BM_WINDOW, "bm_252 (ret2)")
    paired = np.isfinite(r1) & np.isfinite(r2)
    n_paired = paired.sum(axis=1).astype(np.int64)
    p1 = np.prod(1.0 + np.where(paired, r1, 0.0), axis=1)
    p2 = np.prod(1.0 + np.where(paired, r2, 0.0), axis=1)
    valid = n_paired >= BM_MIN_PAIRED
    bm = np.where(valid, p1 - p2, np.nan)
    return {"bm": bm, "n_paired": n_paired, "valid": valid, "prod_held": p1, "prod_deferred": p2}


def feature_bm_252(layer: dict, t: int) -> np.ndarray:
    """``bm_252`` = BM_i(t) = prod_{s in [t-252, t-1]}(1 + ret_i,s) - prod_{s in [t-252, t-1]}(1 + ret2_i,s).

    value        over the SLOTS [t-252, t-1]; a slot that is not an own session contributes nothing; an own
                 session on which either leg is NaN contributes 0 to BOTH legs (i.e. both products run over the
                 PAIRED own sessions only). Higher = the held contract out-compounded the deferred contract.
    valid        iff paired own sessions in [t-252, t-1] >= 227 (ABSOLUTE count, R2); else NaN.
    lag          1 (newest input: slot t-1).
    source       r38_native_contract_layer_v4: ret, ret2 (slots t-252 .. t-1). Never close, never close_b/_CCB.
    availability the settlement of each market's last own session at or before slot t-1.
    refused      t < 252, or a layer that does not hold column t-1 (FeatureWindowError).
    returns      float64[n_layer_markets] (FS1 and FS4 read the SAME function on their own universe's rows).
    """
    return bm_252_detail(layer, t)["bm"]


# --------------------------------------------------------------------------- #
# FS1 / FS4  skeptic-only diagnostics  (NOT signals)
# --------------------------------------------------------------------------- #
def diag_slope_ann_lag1(layer: dict, t: int) -> np.ndarray:
    """``slope_ann_lag1`` = the layer's ``slope_ann`` AT slot t-1, literally (SKEPTIC_DIAGNOSTIC_ONLY).

    NaN where the market has no own session at slot t-1 (a holiday on its calendar) or where the deferred
    month is not listed. Nothing is forward filled: that would be a second definition.
    source r38_native_contract_layer_v4: slope_ann (layer key ``slope``), slot t-1. lag 1.
    """
    t = _t_int(t)
    slope = _f8(layer["slope"], "layer['slope']")
    return _window(slope, t, (SLOPE_LAG, SLOPE_LAG), "slope_ann_lag1")[:, 0].copy()


def diag_mom_252_21_lag1(layer: dict, t: int) -> np.ndarray:
    """``mom_252_21_lag1`` = prod_{s in [t-252, t-22]}(1 + ret_i,s) - 1 (SKEPTIC_DIAGNOSTIC_ONLY).

    A slot that is not an own session contributes nothing (the owner's ``native._trailing`` convention). No
    completeness rule was frozen for this diagnostic and none is invented: NaN only when the window holds NO
    own session (an empty product is not a return). The skeptic compares it with bm_252 on the universe MASK.
    source r38_native_contract_layer_v4: ret (slots t-252 .. t-22). lag 1 (newest input: slot t-22).
    """
    t = _t_int(t)
    ret = _f8(layer["ret"], "layer['ret']")
    r = _window(ret, t, MOM_WINDOW, "mom_252_21_lag1")
    fin = np.isfinite(r)
    out = np.prod(1.0 + np.where(fin, r, 0.0), axis=1) - 1.0
    return np.where(fin.any(axis=1), out, np.nan)


def asset_class_group(layer: dict) -> np.ndarray:
    """``asset_class_group`` (FS4 GROUPING, static, lag 0): the R38 ml-panel ``asset_class`` of every layer row
    ('' when the market is not in the R38 meta, e.g. the FGBL orphan). P4 ranks WITHIN
    FX / RATES / INTERNATIONAL_EQUITY. The U4 mask carries the same labels (``group_r38``)."""
    return np.array([str(a) for a in layer["r38_asset_class"]])


# --------------------------------------------------------------------------- #
# FS2  oi_agg_growth_252  (SIGNAL, lag 2)
# --------------------------------------------------------------------------- #
def _oi_window_stats(ret: np.ndarray, oi: np.ndarray, t: int, window, what: str) -> dict:
    o = np.isfinite(_window(ret, t, window, what + " (own sessions = ret finiteness)"))
    x = _window(oi, t, window, what + " (oi_aggregate)")
    fin = o & np.isfinite(x)
    n_own = o.sum(axis=1).astype(np.int64)
    n_fin = fin.sum(axis=1).astype(np.int64)
    num, den = OI_MIN_FINITE_SHARE
    ok = (n_own >= OI_MIN_OWN_SESSIONS) & (den * n_fin >= num * n_own)
    total = np.where(fin, x, 0.0).sum(axis=1)
    mean = np.where(n_fin > 0, total / np.where(n_fin > 0, n_fin, 1), np.nan)
    return {"n_own": n_own, "n_finite": n_fin, "pass": ok, "mean": mean}


def oi_growth_detail(layer: dict, agg: dict, t: int) -> dict:
    """Every quantity behind ``oi_agg_growth_252`` at decision slot t (per layer row)."""
    t = _t_int(t)
    ret = _f8(layer["ret"], "layer['ret']")
    oi = _f8(agg["oi_aggregate"], "agg['oi_aggregate']")
    _same_rows(ret, oi)
    # own sessions are read through the SAME <= t-2 windows as the open interest: nothing at t-1 is touched
    rec = _oi_window_stats(ret, oi, t, OI_RECENT_WINDOW, "oi_agg_growth_252 recent window")
    base = _oi_window_stats(ret, oi, t, OI_BASE_WINDOW, "oi_agg_growth_252 base window")
    with np.errstate(invalid="ignore"):
        positive = (rec["mean"] > 0) & (base["mean"] > 0)
    valid = rec["pass"] & base["pass"] & positive
    ratio = np.where(valid, rec["mean"], 1.0) / np.where(valid, base["mean"], 1.0)
    g = np.where(valid, np.log(ratio), np.nan)
    return {"g": g, "valid": valid, "windows_pass": rec["pass"] & base["pass"], "means_positive": positive,
            "mean_recent": rec["mean"], "mean_base": base["mean"],
            "n_own_recent": rec["n_own"], "n_finite_recent": rec["n_finite"],
            "n_own_base": base["n_own"], "n_finite_base": base["n_finite"]}


def feature_oi_growth_252(layer: dict, agg: dict, t: int) -> np.ndarray:
    """``oi_agg_growth_252`` = g_i(t) = ln( mean OIagg over own sessions in slots [t-22, t-2]
                                           / mean OIagg over own sessions in slots [t-274, t-254] ).

    value        OIagg = ``r38_aggregate_oi_v1.oi_aggregate`` (ALL listed contract months) and NOTHING ELSE -
                 never the layer's ``open_interest`` (held contract only: its growth is the roll cycle). Each
                 window mean is the mean of the FINITE aggregate-OI values on the market's OWN sessions.
    valid        iff in EACH window: >= 15 own sessions AND aggregate OI finite on >= 90% of them (R2); and
                 both means > 0 (the domain of ln; 0.0 is finite, so a window of zeros passes R2). Else NaN.
    lag          2 (newest input: slot t-2 = data_r38.OI_LATEST_USABLE_OFFSET). Own sessions are read in the
                 same <= t-2 windows; column t-1 of the layer is never touched.
    source       r38_aggregate_oi_v1: oi_aggregate; own-session calendar from r38_native_contract_layer_v4: ret
                 finiteness (the SECOND dataset; the pipeline binds FS2 to r38_aggregate_oi_v1 through U2).
    availability exchange open interest for session s is published on s+1; slot t-2 is observable during t-1.
    refused      t < 274, or arrays that do not hold column t-2 (FeatureWindowError).
    returns      float64[n_layer_markets]; rows without an aggregate-OI file are NaN.
    """
    return oi_growth_detail(layer, agg, t)["g"]


# --------------------------------------------------------------------------- #
# FS3  terms-of-trade relative value  (basket_ret INPUT, beta_volmatch_63 INPUT, tot_cheapness_z_63 SIGNAL)
# --------------------------------------------------------------------------- #
def _pair_structure(universe_u3: dict) -> list:
    """[(pair_id, fx_row, basket_rows, basket_weights)] from the U3 mask artifact (static arrays only),
    asserted equal to the director's ``pairs_fixed_by_fiat`` (universe_fut.TOT_PAIRS)."""
    ids = [str(p) for p in universe_u3["pair_ids"]]
    syms = [str(s) for s in universe_u3["layer_symbols"]]
    if tuple(ids) != tuple(UF.PAIR_IDS):
        raise ValueError("U3 pair order %r differs from %r" % (ids, UF.PAIR_IDS))
    out = []
    for k, fx in enumerate(ids):
        f = int(universe_u3["pair_fx_row"][k])
        w = np.asarray(universe_u3["pair_basket_weights"][k], dtype=np.float64)
        rows = np.where(w != 0.0)[0]
        got = {syms[int(j)]: float(w[int(j)]) for j in rows}
        if syms[f] != fx or got != {a: float(b) for a, b in UF.TOT_PAIRS[fx].items()}:
            raise ValueError("U3 pair %s = %s|%r differs from the agenda's pairs_fixed_by_fiat" % (fx, syms[f], got))
        out.append((fx, f, rows.astype(np.int64), w[rows].copy()))
    return out


def tot_rv_detail(layer: dict, universe_u3: dict, t: int) -> dict:
    """Every quantity behind the FS3 features at decision slot t, in ``pair_ids`` order (6A, 6C, 6N)."""
    t = _t_int(t)
    ret = _f8(layer["ret"], "layer['ret']")
    win = _window(ret, t, TOT_WINDOW, "tot_cheapness_z_63")
    n_slots = win.shape[1]
    pairs = _pair_structure(universe_u3)
    n_p = len(pairs)
    out = {"pair_ids": [p[0] for p in pairs],
           "S": np.full(n_p, np.nan), "z_unclipped": np.full(n_p, np.nan), "beta": np.full(n_p, np.nan),
           "n_common": np.zeros(n_p, dtype=np.int64), "n_own_min_leg": np.zeros(n_p, dtype=np.int64),
           "legs_complete": np.zeros(n_p, dtype=bool), "valid": np.zeros(n_p, dtype=bool),
           "std_fx_common": np.full(n_p, np.nan), "std_basket_common": np.full(n_p, np.nan),
           "std_spread_common": np.full(n_p, np.nan), "sum_spread_all_slots": np.full(n_p, np.nan),
           "basket_ret": np.zeros((n_p, n_slots)), "common": np.zeros((n_p, n_slots), dtype=bool),
           "slots": (t - TOT_WINDOW[0], t - TOT_WINDOW[1])}
    for k, (_fx, f, rows, w) in enumerate(pairs):
        r_fx = win[f]
        r_b = win[rows]
        own_fx = np.isfinite(r_fx)
        own_b = np.isfinite(r_b)
        n_own = np.concatenate([[own_fx.sum()], own_b.sum(axis=1)]).astype(np.int64)
        common = own_fx & own_b.all(axis=0)
        fx0 = np.where(own_fx, r_fx, 0.0)                       # a closed leg contributes a 0 return
        b0 = (w[:, None] * np.where(own_b, r_b, 0.0)).sum(axis=0)
        out["basket_ret"][k] = b0
        out["common"][k] = common
        out["n_common"][k] = int(common.sum())
        out["n_own_min_leg"][k] = int(n_own.min())
        complete = bool((n_own >= TOT_MIN_OWN_SESSIONS).all())
        out["legs_complete"][k] = complete
        if not complete or int(common.sum()) < 2:
            continue
        fx_c = fx0[common]
        b_c = b0[common]                                        # on a common session no leg is zero-padded
        sd_fx = float(fx_c.std(ddof=TOT_STD_DDOF))
        sd_b = float(b_c.std(ddof=TOT_STD_DDOF))
        out["std_fx_common"][k], out["std_basket_common"][k] = sd_fx, sd_b
        if not (math.isfinite(sd_fx) and math.isfinite(sd_b) and sd_b > 0.0):
            continue
        beta = sd_fx / sd_b
        sd_s = float((fx_c - beta * b_c).std(ddof=TOT_STD_DDOF))
        total = float((fx0 - beta * b0).sum())                  # ALL 63 slots
        out["beta"][k] = beta
        out["std_spread_common"][k], out["sum_spread_all_slots"][k] = sd_s, total
        if not (math.isfinite(sd_s) and sd_s > 0.0):
            continue
        z = -total / (sd_s * math.sqrt(float(TOT_NORMALISER_SLOTS)))
        if not math.isfinite(z):
            continue
        out["z_unclipped"][k] = z
        out["S"][k] = min(TOT_CLIP, max(-TOT_CLIP, z))
        out["valid"][k] = True
    # beta is published with the signal: a pair whose signal is invalid publishes no beta either
    out["beta"] = np.where(out["valid"], out["beta"], np.nan)
    return out


def feature_tot_rv(layer: dict, universe_u3: dict, t: int) -> dict:
    """FS3 at decision slot t -> ``{"S": float64[3], "beta": float64[3], "n_common": int64[3]}`` in
    ``pair_ids`` order (6A|0.5 HG + 0.5 GC, 6C|CL, 6N|0.5 DC + 0.5 LE).

    basket_ret          fixed-weight daily return of the pair's commodity basket.
    beta_volmatch_63    ``beta`` = std(ret_FX) / std(basket_ret) over the pair's COMMON sessions in slots
                        [t-63, t-1] (slots on which EVERY leg of the pair has an own session), ddof = 0.
    tot_cheapness_z_63  ``S`` = - [ sum over ALL 63 slots of (ret_FX - beta x basket_ret), a closed leg
                        contributing a 0 return ] / [ std over the COMMON sessions of (ret_FX - beta x
                        basket_ret) x sqrt(63) ], clipped to [-2, 2]; beta held at its value at t.
                        Positive = the currency is CHEAP versus its terms-of-trade basket.
    valid               iff EVERY leg has >= 55 own sessions in [t-63, t-1] (R2) and both stds are > 0; else
                        S and beta are NaN. ``n_common`` is always reported.
    lag 1; source r38_native_contract_layer_v4: ret (slots t-63 .. t-1) of 6A 6C 6N HG GC CL DC LE;
    ``universe_u3`` supplies ONLY the static pair structure (``pair_fx_row``, ``pair_basket_weights``), never
    eligibility. Decide a pair with ``universe_fut.pairs_live_at(u3, t)``, never with isfinite(S).
    refused             t < 63, or a layer that does not hold column t-1 (FeatureWindowError).
    """
    d = tot_rv_detail(layer, universe_u3, t)
    return {"S": d["S"], "beta": d["beta"], "n_common": d["n_common"]}


def tot_basket_returns(layer: dict, universe_u3: dict, t: int) -> dict:
    """``basket_ret`` (FS3 INPUT) on the window: ``{"basket_ret": float64[3, 63], "common": bool[3, 63],
    "slots": (t-63, t-1)}``; a closed basket leg contributes a 0 return; ``common`` marks the pair's common
    sessions (where no leg is zero-padded)."""
    d = tot_rv_detail(layer, universe_u3, t)
    return {"basket_ret": d["basket_ret"], "common": d["common"], "slots": d["slots"]}


# --------------------------------------------------------------------------- #
# The ONE mask: a reference, never a copy
# --------------------------------------------------------------------------- #
def mask_reference(universe_id: str) -> dict:
    """The universe agent's mask artifact of ``universe_id``: path + sha256, re-hashed here. The mask is NOT
    copied, rebuilt or re-derived by this module."""
    d = UF.universe_dir()
    npz = d / (UF.mask_name(universe_id) + ".npz")
    meta_path = d / (UF.mask_name(universe_id) + ".meta.json")
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    got = file_sha256(npz)
    if got != meta["npz_sha256"]:
        raise RuntimeError("%s mask sha256 %s != meta %s" % (universe_id, got, meta["npz_sha256"]))
    return {"universe_id": universe_id, "owner": "universe-construction-agent (universe_fut.py)",
            "npz": str(npz), "npz_sha256": got, "meta": str(meta_path),
            "content_sha256": meta["content_sha256"], "eligible_sha256": meta["eligible_sha256"],
            "orientation": meta["orientation"],
            "reader": "universe_fut.load_universe(universe_id, layer=layer); eligible_at(u, t); "
                      "pairs_live_at(u3, t)"}


# --------------------------------------------------------------------------- #
# Catalog: lag / source / availability instant per feature
# --------------------------------------------------------------------------- #
_LAG_UNIT = "grid slots versus the decision slot t (1 = the newest input is slot t-1; 2 = slot t-2)"
_AVAIL_T1 = ("the settlement of the market's last own session at or before slot t-1 (vendor distribution no "
             "later than the next morning); position entered at the settlement of slot t (R1: or the market's "
             "own next settlement)")
_LAYER_SRC = LAYER_DATASET_ID


def _bm_rows(fs: str) -> list:
    rows = [{
        "name": "bm_252", "lag": 1, "role": "SIGNAL " + FEATURE_SET_USED_BY[fs][0],
        "source": "%s: ret, ret2 (slots t-252 .. t-1)" % _LAYER_SRC,
        "definition": "BM_i(t) = prod(1+ret) - prod(1+ret2) over the SLOTS [t-252, t-1]; a slot that is not an "
                      "own session contributes nothing; an own session with either leg NaN contributes 0 to BOTH "
                      "legs; NaN iff paired own sessions < 227 (ABSOLUTE count)",
        "availability": _AVAIL_T1, "function": "feature_bm_252(layer, t) -> float64[n_layer_markets]",
        "detail_function": "bm_252_detail(layer, t)", "inputs": ["ret", "ret2"],
        "input_slots": "[t-252, t-1]", "min_safe_t": MIN_SAFE_T["bm_252"], "expected_sign": 1,
        "orientation": "HIGHER = the held contract out-compounded the deferred contract; the book is LONG high",
    }, {
        "name": "slope_ann_lag1", "lag": 1, "role": "SKEPTIC_DIAGNOSTIC_ONLY (duplicate-identity attack versus "
                                                     "the slope level) - NOT a signal",
        "source": "%s: slope_ann (layer key 'slope'), slot t-1" % _LAYER_SRC,
        "definition": "slope_ann AT slot t-1, literally; NaN when the market has no own session at t-1 or the "
                      "deferred month is not listed; never forward filled",
        "availability": "the settlement of slot t-1", "function": "diag_slope_ann_lag1(layer, t)",
        "inputs": ["slope"], "input_slots": "[t-1, t-1]", "min_safe_t": MIN_SAFE_T["slope_ann_lag1"],
    }]
    if fs == FS1:
        rows.append({
            "name": "mom_252_21_lag1", "lag": 1, "role": "SKEPTIC_DIAGNOSTIC_ONLY (duplicate-identity attack "
                                                          "versus XS momentum) - NOT a signal",
            "source": "%s: ret (slots t-252 .. t-22)" % _LAYER_SRC,
            "definition": "prod(1+ret) - 1 over the SLOTS [t-252, t-22]; a slot that is not an own session "
                          "contributes nothing; NaN only when the window holds no own session (no completeness "
                          "rule was frozen for this diagnostic and none is invented)",
            "availability": "the settlement of slot t-22 (newest input)",
            "function": "diag_mom_252_21_lag1(layer, t)", "inputs": ["ret"], "input_slots": "[t-252, t-22]",
            "min_safe_t": MIN_SAFE_T["mom_252_21_lag1"],
        })
    if fs == FS4:
        rows.insert(1, {
            "name": "asset_class_group", "lag": 0, "role": "GROUPING (P4 ranks WITHIN group)",
            "source": "ml_ready_native_futures_panel.csv: asset_class (via data_r38.load_certified_layer()"
                      "['r38_asset_class'] = alpha_agent.r59.native.load_meta)",
            "definition": "R38 asset_class label of the market: FX / RATES / INTERNATIONAL_EQUITY "
                          "(short label EQUITY_INDEX); identical to the U4 mask's group_r38",
            "availability": "static", "function": "asset_class_group(layer) -> str[n_layer_markets]",
            "inputs": ["r38_asset_class"], "input_slots": "static", "min_safe_t": 0,
        })
    return rows


def catalog(feature_set_id: Optional[str] = None, *, with_mask: bool = True) -> dict:
    """{feature_set_id: [feature rows]}; every row has name, lag, source, availability, definition, role and
    the function that computes it. Only features that this module actually computes are listed."""
    sets = {
        FS1: _bm_rows(FS1),
        FS2: [{
            "name": "oi_agg_growth_252", "lag": 2, "role": "SIGNAL P2",
            "source": "%s: oi_aggregate (slots [t-22, t-2] and [t-274, t-254]); own-session calendar from %s: "
                      "ret finiteness in the same slots (SECOND dataset - the pipeline binds FS2 to %s through "
                      "U2; returns, costs and the grid of P2 come from %s)"
                      % (AGG_OI_DATASET_ID, _LAYER_SRC, AGG_OI_DATASET_ID, _LAYER_SRC),
            "definition": "ln( mean over own sessions with finite OI in slots [t-22, t-2] / the same in slots "
                          "[t-274, t-254] ); NaN unless EACH window has >= 15 own sessions with aggregate OI "
                          "finite on >= 90% of them, and both means > 0 (domain of ln)",
            "availability": "exchange open interest for session s is published on s+1; the newest OI read is "
                            "slot t-2, observable during slot t-1; position entered at the settlement of slot t",
            "function": "feature_oi_growth_252(layer, agg, t) -> float64[n_layer_markets]",
            "detail_function": "oi_growth_detail(layer, agg, t)", "inputs": ["oi_aggregate", "ret (finiteness)"],
            "input_slots": "[t-274, t-254] and [t-22, t-2]", "min_safe_t": MIN_SAFE_T["oi_agg_growth_252"],
            "expected_sign": 1, "orientation": "HIGHER = aggregate open interest grew; the book is LONG high",
            "forbidden_substitute": "the layer's open_interest column (held contract only)",
        }],
        FS3: [{
            "name": "basket_ret", "lag": 1, "role": "INPUT",
            "source": "%s: ret of HG GC CL DC LE (slots t-63 .. t-1)" % _LAYER_SRC,
            "definition": "fixed-weight daily return of the pair's commodity basket (6A: 0.5 HG + 0.5 GC; "
                          "6C: 1.0 CL; 6N: 0.5 DC + 0.5 LE); a closed leg contributes a 0 return",
            "availability": _AVAIL_T1,
            "function": "tot_basket_returns(layer, universe_u3, t) -> {'basket_ret': f8[3, 63], 'common': "
                        "bool[3, 63]}", "inputs": ["ret"], "input_slots": "[t-63, t-1]",
            "min_safe_t": MIN_SAFE_T["basket_ret"],
        }, {
            "name": "beta_volmatch_63", "lag": 1, "role": "INPUT (hedge ratio of the pair's spread)",
            "source": "%s: ret of the pair's legs (slots t-63 .. t-1)" % _LAYER_SRC,
            "definition": "std(ret_FX) / std(basket_ret) over the pair's COMMON sessions in slots [t-63, t-1] "
                          "(ddof = 0); NaN unless every leg has >= 55 own sessions in the window",
            "availability": _AVAIL_T1, "function": "feature_tot_rv(layer, universe_u3, t)['beta'] -> f8[3]",
            "inputs": ["ret"], "input_slots": "[t-63, t-1]", "min_safe_t": MIN_SAFE_T["beta_volmatch_63"],
        }, {
            "name": "tot_cheapness_z_63", "lag": 1, "role": "SIGNAL P3",
            "source": "%s: ret of 6A 6C 6N HG GC CL DC LE (slots t-63 .. t-1)" % _LAYER_SRC,
            "definition": "S_c(t) = - [sum over ALL 63 slots of (ret_FX - beta x basket_ret), a closed leg "
                          "contributing 0] / [std over the COMMON sessions of (ret_FX - beta x basket_ret) x "
                          "sqrt(63)], clipped to [-2, 2]; beta held at its value at t; NaN unless every leg has "
                          ">= 55 own sessions in [t-63, t-1]",
            "availability": _AVAIL_T1,
            "function": "feature_tot_rv(layer, universe_u3, t) -> {'S': f8[3], 'beta': f8[3], 'n_common': "
                        "i8[3]}", "detail_function": "tot_rv_detail(layer, universe_u3, t)",
            "inputs": ["ret"], "input_slots": "[t-63, t-1]", "min_safe_t": MIN_SAFE_T["tot_cheapness_z_63"],
            "expected_sign": 1, "orientation": "POSITIVE = currency CHEAP versus its basket = LONG currency, "
                                               "SHORT beta x basket",
            "open_question": "NORMALISER: the only WRITTEN normaliser is the agenda's sqrt(63); the rulings "
                             "restate the sessions of every std and of the numerator and are silent on the "
                             "sqrt factor. Implemented literally as sqrt(63). n_common is published so the "
                             "director can rule on sqrt(n_common) without a second computation.",
        }, {
            "name": "tot_n_common_63", "lag": 1, "role": "DIAGNOSTIC (count)",
            "source": "%s: ret finiteness of the pair's legs (slots t-63 .. t-1)" % _LAYER_SRC,
            "definition": "number of slots in [t-63, t-1] on which EVERY leg of the pair has an own session",
            "availability": _AVAIL_T1, "function": "feature_tot_rv(layer, universe_u3, t)['n_common'] -> i8[3]",
            "inputs": ["ret (finiteness)"], "input_slots": "[t-63, t-1]", "min_safe_t": TOT_WINDOW[0],
        }],
        FS4: _bm_rows(FS4),
    }
    bindings = code_bindings()
    out = {}
    for fs, rows in sets.items():
        if feature_set_id is not None and fs != feature_set_id:
            continue
        uid = FEATURE_SET_UNIVERSE[fs]
        full = []
        for r in rows:
            row = {"feature_set_id": fs, "feature_set_version": FEATURE_SET_VERSION, "universe_id": uid,
                   "work_order": FEATURE_SET_WORK_ORDER[fs], "used_by": FEATURE_SET_USED_BY[fs],
                   "module": "research/agents/campaign_r56_v2/features_fut.py", "lag_unit": _LAG_UNIT}
            row.update(r)
            full.append(row)
        if with_mask:
            ref = mask_reference(uid)
            full.append({
                "feature_set_id": fs, "feature_set_version": FEATURE_SET_VERSION, "universe_id": uid,
                "work_order": FEATURE_SET_WORK_ORDER[fs], "used_by": FEATURE_SET_USED_BY[fs],
                "module": "research/agents/campaign_r56_v2/universe_fut.py (NOT this module)",
                "lag_unit": _LAG_UNIT,
                "name": "eligible_mask" if fs != FS3 else "eligible_mask_and_pair_live", "lag": 1,
                "role": "ELIGIBILITY MASK - a REFERENCE to the universe agent's artifact (never copied or "
                        "rebuilt here). Rank on THIS, never on isfinite(feature).",
                "source": "%s sha256 %s (universe-construction-agent; inputs: layer ret/ret2 <= t-1, volume "
                          "and aggregate open interest <= t-2)" % (ref["npz"], ref["npz_sha256"]),
                "definition": "bool[n_decisions, n_layer_markets] eligibility of %s per owner decision "
                              "(certified AND cost_known AND the R2 completeness rule AND TRADED_MAJORITY R3a)"
                              % uid,
                "availability": "inputs <= t-1 (volume / open interest <= t-2)",
                "function": ref["reader"], "mask": ref, "min_safe_t": None,
            })
        for row in full:
            row["code_sha256"] = bindings
            row["survivorship"] = UF.SURVIVORSHIP_WORDING
        out[fs] = full
    return out


# --------------------------------------------------------------------------- #
# Leak-test helpers (used by the evidence scripts; reusable by the validation-skeptic-agent)
# --------------------------------------------------------------------------- #
LAYER_KEYS_2D = ("ret", "ret2", "slope", "open_interest", "volume", "roll")
AGG_KEYS_2D = ("oi_aggregate", "n_contracts_summed", "volume_aggregate")


def truncate_layer(layer: dict, n_cols: int) -> dict:
    """A PHYSICAL copy of the layer holding columns 0 .. n_cols-1 ONLY (fresh buffers: no later slot exists
    anywhere in the returned object). ``truncate_layer(layer, t)`` = the layer truncated AFTER column t-1."""
    n_cols = int(n_cols)
    if n_cols < 0:
        raise ValueError("n_cols must be >= 0")
    out = {}
    for k, v in layer.items():
        if k in LAYER_KEYS_2D:
            out[k] = np.array(v[:, :n_cols], copy=True)
        elif k == "dates":
            out[k] = np.array(v[:n_cols], copy=True)
        else:
            out[k] = v
    return out


def truncate_agg(agg: dict, n_cols: int) -> dict:
    """The aggregate-OI arrays truncated to columns 0 .. n_cols-1 (``truncate_agg(agg, t-1)`` = truncated
    AFTER column t-2)."""
    n_cols = int(n_cols)
    if n_cols < 0:
        raise ValueError("n_cols must be >= 0")
    out = {}
    for k, v in agg.items():
        out[k] = np.array(v[:, :n_cols], copy=True) if k in AGG_KEYS_2D else v
    return out


def compare_values(a, b) -> dict:
    """Exact comparison: NaN pattern and BITWISE equality of the finite cells."""
    a = np.asarray(a, dtype=np.float64)
    b = np.asarray(b, dtype=np.float64)
    fa, fb = np.isfinite(a), np.isfinite(b)
    both = fa & fb
    return {"cells": int(a.size), "finite_a": int(fa.sum()), "finite_b": int(fb.sum()),
            "nan_pattern_mismatch": int((np.isnan(a) != np.isnan(b)).sum()),
            "finite_pattern_mismatch": int((fa != fb).sum()),
            "differing_finite_cells": int((a[both] != b[both]).sum())}


def all_features_at(layer: dict, agg: dict, universe_u3: dict, t: int) -> dict:
    """Every published value at decision slot t as flat float64 vectors (n_common as float for comparison)."""
    tot = tot_rv_detail(layer, universe_u3, t)
    return {"bm_252": feature_bm_252(layer, t),
            "slope_ann_lag1": diag_slope_ann_lag1(layer, t),
            "mom_252_21_lag1": diag_mom_252_21_lag1(layer, t),
            "oi_agg_growth_252": feature_oi_growth_252(layer, agg, t),
            "tot_cheapness_z_63": tot["S"], "beta_volmatch_63": tot["beta"],
            "tot_n_common_63": tot["n_common"].astype(np.float64),
            "basket_ret": tot["basket_ret"].ravel()}


FEATURE_NAMES = ("bm_252", "slope_ann_lag1", "mom_252_21_lag1", "oi_agg_growth_252", "tot_cheapness_z_63",
                 "beta_volmatch_63", "tot_n_common_63", "basket_ret")


# --------------------------------------------------------------------------- #
# Module selftest
# --------------------------------------------------------------------------- #
def _toy(n_m: int, n_d: int, seed: int) -> tuple:
    rng = np.random.default_rng(seed)
    ret = rng.normal(0.0, 0.01, size=(n_m, n_d))
    ret2 = ret + rng.normal(0.0, 0.002, size=(n_m, n_d))
    hol = rng.random((n_m, n_d)) < 0.03                           # holidays: not an own session
    ret[hol] = np.nan
    ret2[hol] = np.nan
    ret2[rng.random((n_m, n_d)) < 0.02] = np.nan                   # deferred month not listed
    slope = rng.normal(0.0, 0.05, size=(n_m, n_d))
    slope[~np.isfinite(ret2)] = np.nan
    oi = np.abs(rng.normal(1e5, 2e4, size=(n_m, n_d)))
    oi[rng.random((n_m, n_d)) < 0.02] = np.nan
    layer = {"ret": ret, "ret2": ret2, "slope": slope, "volume": np.ones((n_m, n_d)),
             "open_interest": oi.copy(), "roll": np.zeros((n_m, n_d), dtype=np.uint8),
             "dates": np.array(["d%05d" % i for i in range(n_d)]),
             "symbols": ["6A", "6C", "6N", "CL", "DC", "GC", "HG", "LE"][:n_m]}
    return layer, {"oi_aggregate": oi}


def _toy_u3(symbols: list) -> dict:
    n_m = len(symbols)
    w = np.zeros((3, n_m))
    fx_row = np.zeros(3, dtype=np.int64)
    for k, fx in enumerate(UF.PAIR_IDS):
        fx_row[k] = symbols.index(fx)
        for leg, wgt in UF.TOT_PAIRS[fx].items():
            w[k, symbols.index(leg)] = wgt
    return {"pair_ids": np.array(UF.PAIR_IDS), "layer_symbols": np.array(symbols), "pair_fx_row": fx_row,
            "pair_basket_weights": w}


def _ref_bm(ret, ret2, i, t):
    p1 = p2 = 1.0
    n = 0
    for s in range(t - 252, t):
        a, b = ret[i, s], ret2[i, s]
        if math.isfinite(a) and math.isfinite(b):
            p1 *= 1.0 + a
            p2 *= 1.0 + b
            n += 1
    return (p1 - p2) if n >= 227 else float("nan"), n


def _ref_oi(ret, oi, i, t):
    means = []
    for far, near in ((22, 2), (274, 254)):
        own = fin = 0
        tot = 0.0
        for s in range(t - far, t - near + 1):
            if math.isfinite(ret[i, s]):
                own += 1
                if math.isfinite(oi[i, s]):
                    fin += 1
                    tot += oi[i, s]
        if own < 15 or 10 * fin < 9 * own or tot <= 0:
            return float("nan")
        means.append(tot / fin)
    return math.log(means[0] / means[1])


def _ref_tot(ret, symbols, fx, t):
    legs = UF.TOT_PAIRS[fx]
    f = symbols.index(fx)
    rows = [(symbols.index(a), w) for a, w in legs.items()]
    sl = list(range(t - 63, t))
    for r in [f] + [j for j, _ in rows]:
        if sum(1 for s in sl if math.isfinite(ret[r, s])) < 55:
            return float("nan"), float("nan"), None
    com = [s for s in sl if math.isfinite(ret[f, s]) and all(math.isfinite(ret[j, s]) for j, _ in rows)]
    fx_c = [ret[f, s] for s in com]
    b_c = [sum(w * ret[j, s] for j, w in rows) for s in com]

    def sd(x):
        m = sum(x) / len(x)
        return math.sqrt(sum((v - m) ** 2 for v in x) / len(x))

    beta = sd(fx_c) / sd(b_c)
    sp = [a - beta * b for a, b in zip(fx_c, b_c)]
    tot = 0.0
    for s in sl:
        a = ret[f, s] if math.isfinite(ret[f, s]) else 0.0
        b = sum(w * (ret[j, s] if math.isfinite(ret[j, s]) else 0.0) for j, w in rows)
        tot += a - beta * b
    z = -tot / (sd(sp) * math.sqrt(63.0))
    return max(-2.0, min(2.0, z)), beta, len(com)


def selftest(real_layer: bool = True) -> dict:
    checks = []

    def ok(name, cond, **extra):
        checks.append({"check": name, "pass": bool(cond), **extra})

    layer, agg = _toy(8, 400, 7)
    syms = layer["symbols"]
    u3 = _toy_u3(syms)
    # make the completeness rules bind on some rows
    layer["ret2"][1, 100:140] = np.nan                           # row 1: < 227 paired sessions at t = 352
    layer["ret"][4, 300:312] = np.nan                            # DC: 12 closed slots -> < 55 own sessions
    agg["oi_aggregate"][2, 335:345] = np.nan                     # row 2: < 90% finite OI in the recent window
    t = 352
    bm = bm_252_detail(layer, t)
    worst = 0.0
    nan_ok = True
    for i in range(8):
        ref, n = _ref_bm(layer["ret"], layer["ret2"], i, t)
        nan_ok &= (math.isnan(ref) == bool(np.isnan(bm["bm"][i]))) and n == int(bm["n_paired"][i])
        if math.isfinite(ref):
            worst = max(worst, abs(ref - float(bm["bm"][i])))
    ok("bm_252 equals an independent pure-python loop (value, NaN pattern, paired count)",
       nan_ok and worst < 1e-12 and int(np.isfinite(bm["bm"]).sum()) >= 4, max_abs_diff=worst,
       n_nan=int(np.isnan(bm["bm"]).sum()), n_finite=int(np.isfinite(bm["bm"]).sum()))
    ok("bm_252 is NaN below 227 paired sessions and finite at/above", bool(np.isnan(bm["bm"][1]))
       and bool((np.isfinite(bm["bm"]) == (bm["n_paired"] >= 227)).all()))
    g = oi_growth_detail(layer, agg, t)
    worst = 0.0
    nan_ok = True
    for i in range(8):
        ref = _ref_oi(layer["ret"], agg["oi_aggregate"], i, t)
        nan_ok &= math.isnan(ref) == bool(np.isnan(g["g"][i]))
        if math.isfinite(ref):
            worst = max(worst, abs(ref - float(g["g"][i])))
    ok("oi_agg_growth_252 equals an independent pure-python loop",
       nan_ok and worst < 1e-12 and int(np.isfinite(g["g"]).sum()) >= 4, max_abs_diff=worst,
       n_nan=int(np.isnan(g["g"]).sum()), n_finite=int(np.isfinite(g["g"]).sum()))
    ok("oi_agg_growth_252 is NaN when < 90% of own sessions carry a finite OI", bool(np.isnan(g["g"][2])))
    tot = tot_rv_detail(layer, u3, t)
    worst = 0.0
    nan_ok = True
    for k, fx in enumerate(UF.PAIR_IDS):
        s_ref, b_ref, n_ref = _ref_tot(layer["ret"], syms, fx, t)
        nan_ok &= math.isnan(s_ref) == bool(np.isnan(tot["S"][k]))
        if math.isfinite(s_ref):
            worst = max(worst, abs(s_ref - float(tot["S"][k])), abs(b_ref - float(tot["beta"][k])))
            nan_ok &= n_ref == int(tot["n_common"][k])
    ok("tot_cheapness_z_63 / beta_volmatch_63 / n_common equal an independent pure-python loop",
       nan_ok and worst < 1e-12 and int(np.isfinite(tot["S"]).sum()) == 2, max_abs_diff=worst,
       n_finite_pairs=int(np.isfinite(tot["S"]).sum()))
    ok("the 6N pair is NaN when DC has < 55 own sessions in [t-63, t-1]", bool(np.isnan(tot["S"][2]))
       and bool(np.isnan(tot["beta"][2])))
    ok("S is clipped to [-2, 2]", bool(np.nanmax(np.abs(tot["S"])) <= 2.0))
    # hand-computed constants: 252 paired sessions of +1% held / +0.5% deferred
    lay2 = {"ret": np.full((1, 300), 0.01), "ret2": np.full((1, 300), 0.005), "slope": np.zeros((1, 300))}
    want = 1.01 ** 252 - 1.005 ** 252
    got = float(feature_bm_252(lay2, 300)[0])
    ok("bm_252 hand value 1.01^252 - 1.005^252", abs(got - want) < 1e-9, got=got, want=want)
    got = float(diag_mom_252_21_lag1(lay2, 300)[0])
    ok("mom_252_21_lag1 hand value 1.01^231 - 1", abs(got - (1.01 ** 231 - 1.0)) < 1e-9, got=got)
    oi2 = {"oi_aggregate": np.concatenate([np.full((1, 150), 100.0), np.full((1, 150), 200.0)], axis=1)}
    got = float(feature_oi_growth_252(lay2, oi2, 300)[0])
    ok("oi_agg_growth_252 hand value ln(200/100)", abs(got - math.log(2.0)) < 1e-12, got=got)
    # refusals
    refused = {}
    for name, fn in (("bm_252", lambda tt: feature_bm_252(layer, tt)),
                     ("mom_252_21_lag1", lambda tt: diag_mom_252_21_lag1(layer, tt)),
                     ("oi_agg_growth_252", lambda tt: feature_oi_growth_252(layer, agg, tt)),
                     ("tot_cheapness_z_63", lambda tt: feature_tot_rv(layer, u3, tt)),
                     ("slope_ann_lag1", lambda tt: diag_slope_ann_lag1(layer, tt))):
        lo = MIN_SAFE_T[name]
        try:
            fn(lo - 1)
            below = False
        except FeatureWindowError:
            below = True
        try:
            fn(lo)
            at = True
        except FeatureWindowError:
            at = False
        # The newest declared input of a feature is slot t-near, so the first t the array cannot serve is
        # n_cols + near (NOT a fixed n_cols + 2: mom_252_21_lag1 ends at t-22 and legitimately computes at
        # n_cols + 2). Probe corrected after selftest run 1, which used a fixed t and failed on mom only.
        near = _NEWEST_INPUT_OFFSET[name]
        first_unservable = 400 + near
        try:
            fn(first_unservable)
            beyond = False
        except FeatureWindowError:
            beyond = True
        try:
            fn(first_unservable - 1)
            last_ok = True
        except FeatureWindowError:
            last_ok = False
        refused[name] = {"refused_at_min_safe_t_minus_1": below, "computed_at_min_safe_t": at,
                         "newest_input_offset": near, "first_unservable_t": first_unservable,
                         "refused_at_first_unservable_t": beyond, "computed_at_last_servable_t": last_ok}
    ok("every feature refuses a window that starts before column 0 or whose newest input is beyond the array",
       all(v["refused_at_min_safe_t_minus_1"] and v["computed_at_min_safe_t"]
           and v["refused_at_first_unservable_t"] and v["computed_at_last_servable_t"]
           for v in refused.values()), per_feature=refused)
    naive = layer["ret"][:, np.arange(100 - 252, 100)]            # what an unguarded fancy index would read
    ok("an UNGUARDED negative index reads the END of the array (why the guard exists)",
       bool(np.array_equal(naive[:, 0], layer["ret"][:, 400 - 152], equal_nan=True)),
       naive_slice_width=int(layer["ret"][:, 100 - 252:100].shape[1]))
    try:
        feature_bm_252(layer, True)
        typed = False
    except TypeError:
        typed = True
    ok("a non-integer t is refused", typed)
    # truncation on the toy
    full = all_features_at(layer, agg, u3, t)
    cut = all_features_at(truncate_layer(layer, t), truncate_agg(agg, t - 1), u3, t)
    bad = sum(compare_values(full[k], cut[k])["nan_pattern_mismatch"]
              + compare_values(full[k], cut[k])["differing_finite_cells"] for k in FEATURE_NAMES)
    ok("toy: every feature is identical on a layer truncated after t-1 (OI after t-2)", bad == 0)
    real = None
    if real_layer:
        lay = data_r38.load_certified_layer()
        ag = data_r38.load_aggregate_oi(lay)
        ru3 = UF.load_universe(UF.U3, layer=lay)
        idx = UF.decision_grid(lay)
        rows = []
        bad = 0
        for tt in (int(idx[0]), int(idx[len(idx) // 2]), int(idx[-1])):
            a = all_features_at(lay, ag, ru3, tt)
            b = all_features_at(truncate_layer(lay, tt), truncate_agg(ag, tt - 1), ru3, tt)
            for k in FEATURE_NAMES:
                c = compare_values(a[k], b[k])
                bad += c["nan_pattern_mismatch"] + c["differing_finite_cells"]
                rows.append({"feature": k, "t": tt, "date": str(lay["dates"][tt]), **c})
        ok("real layer: first / middle / last owner decision identical under physical truncation", bad == 0)
        real = rows
    return {"kind": "FEATURES_FUT_SELFTEST", "module_sha256": module_sha256(),
            "feature_set_ids": list(FEATURE_SET_IDS), "checks": checks, "real_layer_rows": real,
            "pass": all(c["pass"] for c in checks)}


def main(argv) -> int:
    cmd = argv[1] if len(argv) > 1 else "catalog"
    if cmd == "catalog":
        print(json.dumps({"feature_set_ids": list(FEATURE_SET_IDS), "module_sha256": module_sha256(),
                          "min_safe_t": MIN_SAFE_T, "timing": TIMING_STATEMENT, "features": catalog()},
                         indent=1))
        return 0
    if cmd == "selftest":
        res = selftest(real_layer="--toy-only" not in argv)
        print(json.dumps(res, indent=1, default=str))
        print("FEATURES_FUT_SELFTEST_OK" if res["pass"] else "FEATURES_FUT_SELFTEST_FAILED")
        return 0 if res["pass"] else 2
    print("usage: features_fut.py catalog | selftest [--toy-only]")
    return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
