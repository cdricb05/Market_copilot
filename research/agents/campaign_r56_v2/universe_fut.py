r"""universe_fut - the four futures / cross-asset universes of campaign R56_V2.

RESEARCH ONLY. PAPER ONLY. NO ORDERS. ORDERS DISABLED. AUTOMATION OFF. MANUAL REVIEW.

    U1  FUT_R38_COMMODITY_V1      P1   39 certified R38 COMMODITY markets
    U2  FUT_R38_COMMODITY_OI_V1   P2   U1's markets + the aggregate-OI window rule
    U3  XA_TOT_PAIRS_V1           P3   8 legs in 3 fixed pairs 6A|HG+GC  6C|CL  6N|DC+LE
    U4  FUT_R38_FINANCIALS_V1     P4   FX 8 + RATES 6 + INTERNATIONAL_EQUITY 14, ranked within group

This module builds UNIVERSES and nothing else. It never reads a forward return,
never computes a signal, a weight, an IC or a performance number, and owns no
registry, gate, queue, evaluator or forward clock. The durable record of a
universe is the AGENTS_V2_UNIVERSE_DEFINED event written through
``scripts\alpha_agents_v2.py define_universe``; the npz written here is the mask
that event names, so that the feature agent, the four signal agents and the
skeptic all read ONE mask.

The rules are the director's and are FROZEN (director\phase1b_rulings.json,
rulings R1-R4 and ``amendments_to_work_orders``). They are implemented
mechanically: no market is admitted or excluded by name, and no threshold here
may be tuned. Membership comes from the R38 meta
(``load_certified_layer()['r38_asset_class']``); only the P3 legs are named,
because agenda.json P3 fixes them by fiat (``pairs_fixed_by_fiat``, asserted
equal to ``TOT_PAIRS`` at every U3 build). ``certified`` and ``cost_known`` are
kept as separate, attributable rules rather than folded into membership.

Definitions (rulings, ``definitions_used_by_every_ruling``)
-----------------------------------------------------------
* grid slot t        an index of the union session grid of
                     ``data_r38.load_certified_layer()`` (15 exchange calendars).
* own session        ``data_r38.own_sessions(layer)[i, s]``: ``ret[i, s]`` finite.
* slot window        always a range of grid SLOTS; completeness is always
                     counted on the market's OWN sessions inside it.
* decision grid      the OWNER's, never moved (R1):
                     ``alpha_agent.r59.native.decision_indices(dates, 21, 21)``.

Eligibility at decision slot t (``futures_universes_common``)
------------------------------------------------------------
    member  AND  layer['certified']  AND  layer['cost_known']
            AND  the universe's completeness rule (R2)  AND  TRADED_MAJORITY (R3a)

    PAIRED_227       U1 U2 U4   paired own sessions (ret AND ret2 finite) in slots
                                [t-252, t-1] >= 227, an ABSOLUTE count.
    OI_WINDOW        U2         in EACH of [t-22, t-2] and [t-274, t-254]:
                                >= 15 own sessions AND aggregate open interest
                                finite on >= 90% of those own sessions.
    LEG_55           U3         >= 55 own sessions in slots [t-63, t-1].
    TRADED_MAJORITY  all        held-contract ``volume`` > 0 on >= 50% of the own
                                sessions in slots [t-253, t-2] (a window with no
                                own session FAILS; a NaN volume is not a trade).
    U3 pair          a pair is live iff EVERY one of its legs is complete
                     (LEG_55 AND TRADED_MAJORITY AND certified AND cost_known).

Timing. Every input of row t is at a slot <= t-1 (volume and open interest
<= t-2, ``data_r38.OI_LATEST_USABLE_OFFSET``). LIVENESS at t
(``evaluator.live_markets(layer, t, closed_rule='ENTER_NEXT_OWN_SETTLE',
stale_slots=5)``) is NOT part of the mask: the evaluator applies it (R1). A
weights_fn must therefore rank among ``eligible_at(u, t) & live`` (R7).

Survivorship (R4). The market list is the vendor's CURRENT composition (expired
contracts retained: 17,195 held contracts; discontinued markets NOT retained).
No futures universe may be described as survivorship-free.

Arrays are in the LAYER's row space (``len(layer['symbols'])`` columns, FGBL and
VX included and always False), so ``eligible[j]`` can be passed straight to
``evaluator.rank_weights``. The static member list is ``symbols`` /
``member_rows`` in the layer's row order.

Usage (PowerShell)
    & C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe `
        C:\Users\binis\paper_trader\research\agents\campaign_r56_v2\universe_fut.py verify

Import
    import sys
    sys.path.insert(0, r"C:\Users\binis\paper_trader")
    sys.path.insert(0, r"C:\Users\binis\paper_trader\research\agents\campaign_r56_v2")
    import data_r38, universe_fut
    layer = data_r38.load_certified_layer()
    u1 = universe_fut.load_universe("FUT_R38_COMMODITY_V1", layer=layer)
    elig_t = universe_fut.eligible_at(u1, t)          # bool[n_layer_markets]
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parents[2]
for _p in (str(_REPO), str(_HERE)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

SAFETY = ("RESEARCH ONLY", "PAPER ONLY", "NO ORDERS", "ORDERS DISABLED",
          "AUTOMATION OFF", "MANUAL REVIEW")

U1 = "FUT_R38_COMMODITY_V1"
U2 = "FUT_R38_COMMODITY_OI_V1"
U3 = "XA_TOT_PAIRS_V1"
U4 = "FUT_R38_FINANCIALS_V1"
UNIVERSE_IDS = (U1, U2, U3, U4)

LAYER_DATASET_ID = "r38_native_contract_layer_v4"
AGG_OI_DATASET_ID = "r38_aggregate_oi_v1"

#: The pipeline binds ONE dataset_id per universe (the work order's). U2 reads
#: BOTH datasets; the second one is stated in ``rules`` and in ``also_reads``.
SPEC = {
    U1: {"work_order": "U1", "used_by": ["P1"],
         "dataset_id": LAYER_DATASET_ID, "also_reads": [],
         "asset_class": "COMMODITY_FUTURES",
         "execution_representation":
             "FUTURES_NOTIONAL_LONG_SHORT_NATIVE_DATED_CONTRACT",
         "short_leg_expressible": True},
    U2: {"work_order": "U2", "used_by": ["P2"],
         "dataset_id": AGG_OI_DATASET_ID, "also_reads": [LAYER_DATASET_ID],
         "asset_class": "COMMODITY_FUTURES",
         "execution_representation":
             "FUTURES_NOTIONAL_LONG_SHORT_NATIVE_DATED_CONTRACT",
         "short_leg_expressible": True},
    U3: {"work_order": "U3", "used_by": ["P3"],
         "dataset_id": LAYER_DATASET_ID, "also_reads": [],
         "asset_class": "CROSS_ASSET",
         "execution_representation": "FUTURES_NOTIONAL_PAIR_SPREADS",
         "short_leg_expressible": True},
    U4: {"work_order": "U4", "used_by": ["P4"],
         "dataset_id": LAYER_DATASET_ID, "also_reads": [],
         "asset_class": "CROSS_ASSET",
         "execution_representation":
             "FUTURES_NOTIONAL_LONG_SHORT_NATIVE_DATED_CONTRACT",
         "short_leg_expressible": True},
}

# --------------------------------------------------------------------------- #
# FROZEN by the director before any signal result existed. NOT tunable.
# --------------------------------------------------------------------------- #
CADENCE = 21
HORIZON = 21

#: slot windows as (far offset, near offset): slots [t - far, t - near] inclusive
PAIRED_WINDOW = (252, 1)
PAIRED_MIN_SESSIONS = 227                 # absolute count (R2)
TRADED_WINDOW = (253, 2)                  # ends at t-2: volume is provisional
TRADED_MIN_SHARE = (1, 2)                 # volume > 0 on >= 1/2 of own sessions
OI_RECENT_WINDOW = (22, 2)
OI_BASE_WINDOW = (274, 254)
OI_MIN_OWN_SESSIONS = 15
OI_MIN_FINITE_SHARE = (9, 10)             # finite on >= 9/10 of own sessions
LEG_WINDOW = (63, 1)
LEG_MIN_OWN_SESSIONS = 55

#: agenda.json P3 ``pairs_fixed_by_fiat``: FX leg -> {basket leg: weight}.
TOT_PAIRS = {"6A": {"HG": 0.5, "GC": 0.5},
             "6C": {"CL": 1.0},
             "6N": {"DC": 0.5, "LE": 0.5}}
PAIR_IDS = ("6A", "6C", "6N")

#: U4 groups = the R38 asset_class (ruling U4). Short label used by the task.
U4_GROUPS_R38 = ("FX", "RATES", "INTERNATIONAL_EQUITY")
GROUP_SHORT_LABEL = {"FX": "FX", "RATES": "RATES",
                     "INTERNATIONAL_EQUITY": "EQUITY_INDEX"}

#: Book floors (agenda.json / R7). REPORTED against; never used to build a mask.
FLOOR_MARKETS_P1_P2 = 12
FLOOR_MARKETS_PER_GROUP_P4 = 4
FLOOR_LIVE_PAIRS_P3 = 2

#: The frozen liveness rule (R1). Applied by the evaluator, NOT by the mask.
FROZEN_CLOSED_RULE = "ENTER_NEXT_OWN_SETTLE"
FROZEN_STALE_SLOTS = 5

SURVIVORSHIP_WORDING = (
    "measured on the vendor's CURRENT-composition market list (expired "
    "contracts retained: 17,195 held contracts; discontinued markets NOT "
    "retained). This universe is NOT survivorship-free.")

UNIVERSE_DIR_ENV = "PAPER_TRADER_R56V2_UNIVERSE_DIR"
DEFAULT_UNIVERSE_DIR = Path(r"D:\Stock_Prediction_app_data\r59_autonomous_alpha"
                            r"\agents_v2\campaign_R56_V2\universes")
AGENDA_PATH = Path(r"D:\Stock_Prediction_app_data\r59_autonomous_alpha"
                   r"\agents_v2\campaign_R56_V2\director\agenda.json")
MASK_VERSION = "v1"


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def file_sha256(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def array_sha256(a) -> str:
    a = np.ascontiguousarray(a)
    h = hashlib.sha256()
    h.update(a.dtype.str.encode("ascii"))
    h.update(repr(tuple(a.shape)).encode("ascii"))
    h.update(a.tobytes())
    return h.hexdigest()


def content_sha256(arrays: dict) -> str:
    """Container-independent hash of a set of named arrays."""
    h = hashlib.sha256()
    for name in sorted(arrays):
        h.update(name.encode("utf-8"))
        h.update(array_sha256(arrays[name]).encode("ascii"))
    return h.hexdigest()


def universe_dir() -> Path:
    return Path(os.environ.get(UNIVERSE_DIR_ENV) or DEFAULT_UNIVERSE_DIR)


def mask_name(universe_id: str) -> str:
    return "%s_mask_%s" % (universe_id, MASK_VERSION)


def _check_id(universe_id: str) -> None:
    if universe_id not in SPEC:
        raise KeyError("%r is not one of %s" % (universe_id, UNIVERSE_IDS))


def _strs(values) -> np.ndarray:
    """Fixed-width unicode array (never dtype=object: the npz needs no pickle)."""
    values = [str(v) for v in values]
    width = max([1] + [len(v) for v in values])
    return np.array(values, dtype="<U%d" % width)


# --------------------------------------------------------------------------- #
# The owner's decision grid and the slot-window counter
# --------------------------------------------------------------------------- #
def decision_grid(layer: dict) -> np.ndarray:
    """The OWNER's H21 grid, unmodified (R1)."""
    from alpha_agent.r59 import native

    return np.asarray(native.decision_indices(layer["dates"], CADENCE, HORIZON),
                      dtype=np.int64)


def window_count(indicator: np.ndarray, idx: np.ndarray, window) -> np.ndarray:
    """int[n_decisions, n_markets]: number of True cells of ``indicator``
    (bool[n_markets, n_slots]) in slots [t - far, t - near], for every t of
    ``idx``. Reads nothing at a slot > t - near."""
    far, near = int(window[0]), int(window[1])
    if not far >= near >= 1:
        raise ValueError("a window must end at t-1 or earlier: %r" % (window,))
    idx = np.asarray(idx, dtype=np.int64)
    n_m, n_d = indicator.shape
    if len(idx) and (int(idx.min()) - far < 0 or int(idx.max()) > n_d):
        raise ValueError("window %r leaves the grid for idx [%d, %d], n=%d"
                         % (window, int(idx.min()), int(idx.max()), n_d))
    csum = np.zeros((n_m, n_d + 1), dtype=np.int64)
    np.cumsum(indicator.astype(np.int64), axis=1, out=csum[:, 1:])
    hi = idx - near + 1            # exclusive end  -> slot t-near is the last read
    lo = idx - far
    return (csum[:, hi] - csum[:, lo]).T.copy()


# --------------------------------------------------------------------------- #
# The four rules. Each returns the pass mask AND the counts behind it.
# --------------------------------------------------------------------------- #
def rule_paired_227(layer: dict, idx: np.ndarray) -> dict:
    paired = np.isfinite(layer["ret"]) & np.isfinite(layer["ret2"])
    n = window_count(paired, idx, PAIRED_WINDOW)
    return {"pass": n >= PAIRED_MIN_SESSIONS, "n_paired": n}


def rule_traded_majority(layer: dict, idx: np.ndarray) -> dict:
    own = np.isfinite(layer["ret"])
    vol = layer["volume"]
    with np.errstate(invalid="ignore"):
        traded = own & (vol > 0)                       # NaN > 0 is False
    n_own = window_count(own, idx, TRADED_WINDOW)
    n_tr = window_count(traded, idx, TRADED_WINDOW)
    num, den = TRADED_MIN_SHARE
    return {"pass": (n_own > 0) & (den * n_tr >= num * n_own),
            "n_own": n_own, "n_traded": n_tr}


def rule_oi_window(layer: dict, oi_aggregate: np.ndarray, idx: np.ndarray,
                   window) -> dict:
    own = np.isfinite(layer["ret"])
    fin = own & np.isfinite(oi_aggregate)
    n_own = window_count(own, idx, window)
    n_fin = window_count(fin, idx, window)
    num, den = OI_MIN_FINITE_SHARE
    return {"pass": (n_own >= OI_MIN_OWN_SESSIONS) & (den * n_fin >= num * n_own),
            "n_own": n_own, "n_finite": n_fin}


def rule_leg_55(layer: dict, idx: np.ndarray) -> dict:
    n = window_count(np.isfinite(layer["ret"]), idx, LEG_WINDOW)
    return {"pass": n >= LEG_MIN_OWN_SESSIONS, "n_own": n}


# --------------------------------------------------------------------------- #
# Static membership (from the R38 meta; only the P3 legs are named, by fiat)
# --------------------------------------------------------------------------- #
def _class_rows(layer: dict, r38_classes) -> np.ndarray:
    keep = set(r38_classes)
    return np.array([a in keep for a in layer["r38_asset_class"]], dtype=bool)


def tot_legs() -> list:
    legs = []
    for fx in PAIR_IDS:
        legs.append(fx)
        legs.extend(TOT_PAIRS[fx])
    return legs


def static_members(layer: dict, universe_id: str) -> np.ndarray:
    """bool[n_layer_markets]: the universe's market list before any PIT rule.
    Certification and cost are separate, attributable rules - not folded in."""
    _check_id(universe_id)
    if universe_id in (U1, U2):
        return _class_rows(layer, ("COMMODITY",))
    if universe_id == U4:
        return _class_rows(layer, U4_GROUPS_R38)
    legs = set(tot_legs())
    return np.array([s in legs for s in layer["symbols"]], dtype=bool)


def assert_agenda_pairs() -> Optional[bool]:
    """The hard-coded pairs must equal agenda.json P3 ``pairs_fixed_by_fiat``.
    Returns None when the agenda file is not reachable (nothing is assumed)."""
    if not AGENDA_PATH.exists():
        return None
    doc = json.loads(AGENDA_PATH.read_text(encoding="utf-8"))
    for row in doc.get("primary_experiments", []):
        if row.get("universe_id") == U3:
            fiat = row["parameters"]["pairs_fixed_by_fiat"]
            if fiat != TOT_PAIRS:
                raise AssertionError("TOT_PAIRS differs from agenda.json: %r" % fiat)
            return True
    raise AssertionError("agenda.json has no experiment on %s" % U3)


# --------------------------------------------------------------------------- #
# Builders
# --------------------------------------------------------------------------- #
def _common(layer: dict, universe_id: str, decision_idx) -> dict:
    overridden = decision_idx is not None
    idx = (np.asarray(decision_idx, dtype=np.int64) if overridden
           else decision_grid(layer))
    syms = list(layer["symbols"])
    member = static_members(layer, universe_id)
    certified = np.asarray(layer["certified"], dtype=bool)
    cost_known = np.asarray(layer["cost_known"], dtype=bool)
    tm = rule_traded_majority(layer, idx)
    arrays = {
        "universe_id": _strs([universe_id]),
        "dataset_id": _strs([SPEC[universe_id]["dataset_id"]]),
        "layer_symbols": _strs(syms),
        "layer_n_sessions": np.array([len(layer["dates"])], dtype=np.int64),
        "decision_idx": idx,
        "decision_dates": _strs(layer["dates"][idx]),
        "member": member,
        "member_rows": np.where(member)[0].astype(np.int64),
        "symbols": _strs([s for s, m in zip(syms, member) if m]),
        "rule_certified": certified,
        "rule_cost_known": cost_known,
        "rule_traded_majority": tm["pass"],
        "n_own_traded_window": tm["n_own"].astype(np.int16),
        "n_traded_traded_window": tm["n_traded"].astype(np.int16),
    }
    static_ok = member & certified & cost_known
    return {"universe_id": universe_id, "arrays": arrays,
            "static_ok": static_ok, "idx": idx,
            "decision_idx_overridden": bool(overridden)}


def _finish(built: dict, eligible: np.ndarray) -> dict:
    built["arrays"]["eligible"] = np.asarray(eligible, dtype=bool)
    built.pop("static_ok", None)
    built.pop("idx", None)
    return built


def build_u1(layer: Optional[dict] = None, *, decision_idx=None,
             universe_id: str = U1) -> dict:
    """U1 FUT_R38_COMMODITY_V1: PAIRED_227 AND TRADED_MAJORITY."""
    import data_r38

    layer = layer if layer is not None else data_r38.load_certified_layer()
    b = _common(layer, universe_id, decision_idx)
    p = rule_paired_227(layer, b["idx"])
    a = b["arrays"]
    a["rule_paired_227"] = p["pass"]
    a["n_paired_252"] = p["n_paired"].astype(np.int16)
    elig = b["static_ok"][None, :] & p["pass"] & a["rule_traded_majority"]
    return _finish(b, elig)


def build_u2(layer: Optional[dict] = None, agg: Optional[dict] = None, *,
             decision_idx=None) -> dict:
    """U2 FUT_R38_COMMODITY_OI_V1: U1's rule AND both aggregate-OI windows."""
    import data_r38

    layer = layer if layer is not None else data_r38.load_certified_layer()
    agg = agg if agg is not None else data_r38.load_aggregate_oi(layer)
    b = _common(layer, U2, decision_idx)
    a = b["arrays"]
    p = rule_paired_227(layer, b["idx"])
    a["rule_paired_227"] = p["pass"]
    a["n_paired_252"] = p["n_paired"].astype(np.int16)
    oi = agg["oi_aggregate"]
    rec = rule_oi_window(layer, oi, b["idx"], OI_RECENT_WINDOW)
    base = rule_oi_window(layer, oi, b["idx"], OI_BASE_WINDOW)
    a["rule_oi_recent"] = rec["pass"]
    a["rule_oi_base"] = base["pass"]
    a["rule_oi_window"] = rec["pass"] & base["pass"]
    a["n_own_oi_recent"] = rec["n_own"].astype(np.int16)
    a["n_finite_oi_recent"] = rec["n_finite"].astype(np.int16)
    a["n_own_oi_base"] = base["n_own"].astype(np.int16)
    a["n_finite_oi_base"] = base["n_finite"].astype(np.int16)
    elig = (b["static_ok"][None, :] & p["pass"] & a["rule_traded_majority"]
            & a["rule_oi_window"])
    return _finish(b, elig)


def build_u3(layer: Optional[dict] = None, *, decision_idx=None) -> dict:
    """U3 XA_TOT_PAIRS_V1: LEG_55 AND TRADED_MAJORITY per leg; a pair is live
    iff every leg is complete; a leg is eligible iff its pair is live."""
    import data_r38

    layer = layer if layer is not None else data_r38.load_certified_layer()
    assert_agenda_pairs()
    b = _common(layer, U3, decision_idx)
    a = b["arrays"]
    syms = list(layer["symbols"])
    missing = [s for s in tot_legs() if s not in syms]
    if missing:
        raise RuntimeError("P3 legs absent from the layer: %s" % missing)
    lg = rule_leg_55(layer, b["idx"])
    a["rule_leg_55"] = lg["pass"]
    a["n_own_leg_63"] = lg["n_own"].astype(np.int16)
    complete = b["static_ok"][None, :] & lg["pass"] & a["rule_traded_majority"]
    a["leg_complete"] = complete
    n_m = len(syms)
    pair_fx_row = np.zeros(len(PAIR_IDS), dtype=np.int64)
    basket_w = np.zeros((len(PAIR_IDS), n_m), dtype=np.float64)
    leg_member = np.zeros((len(PAIR_IDS), n_m), dtype=bool)
    pair_live = np.zeros((len(b["idx"]), len(PAIR_IDS)), dtype=bool)
    elig = np.zeros_like(complete)
    for k, fx in enumerate(PAIR_IDS):
        pair_fx_row[k] = syms.index(fx)
        leg_member[k, pair_fx_row[k]] = True
        for leg, wgt in TOT_PAIRS[fx].items():
            basket_w[k, syms.index(leg)] = float(wgt)
            leg_member[k, syms.index(leg)] = True
        pair_live[:, k] = complete[:, leg_member[k]].all(axis=1)
        elig[:, leg_member[k]] |= pair_live[:, k][:, None]
    if int(leg_member.sum(axis=0).max()) > 1:
        raise AssertionError("a leg is shared by two pairs")
    a["pair_ids"] = _strs(PAIR_IDS)
    a["pair_fx_row"] = pair_fx_row
    a["pair_basket_weights"] = basket_w
    a["pair_leg_member"] = leg_member
    a["pair_live"] = pair_live
    return _finish(b, elig)


def build_u4(layer: Optional[dict] = None, *, decision_idx=None) -> dict:
    """U4 FUT_R38_FINANCIALS_V1: U1's eligibility on FX / RATES /
    INTERNATIONAL_EQUITY, plus the group label P4 ranks within."""
    import data_r38

    layer = layer if layer is not None else data_r38.load_certified_layer()
    b = build_u1(layer, decision_idx=decision_idx, universe_id=U4)
    member = b["arrays"]["member"]
    r38 = [c if m else "" for c, m in zip(layer["r38_asset_class"], member)]
    b["arrays"]["group_r38"] = _strs(r38)
    b["arrays"]["group"] = _strs([GROUP_SHORT_LABEL.get(c, "") for c in r38])
    b["arrays"]["group_r59"] = _strs(
        [data_r38.R38_TO_R59_ASSET_CLASS.get(c, "") for c in r38])
    return b


_BUILDERS = {U1: build_u1, U2: build_u2, U3: build_u3, U4: build_u4}


def build_universe(universe_id: str, layer: Optional[dict] = None,
                   agg: Optional[dict] = None, *, decision_idx=None) -> dict:
    """Rebuild one universe from the certified layer (deterministic)."""
    _check_id(universe_id)
    if universe_id == U2:
        return build_u2(layer, agg, decision_idx=decision_idx)
    return _BUILDERS[universe_id](layer, decision_idx=decision_idx)


# --------------------------------------------------------------------------- #
# Deterministic persistence
# --------------------------------------------------------------------------- #
def _npy_bytes(arr: np.ndarray) -> bytes:
    buf = io.BytesIO()
    np.lib.format.write_array(buf, np.ascontiguousarray(arr), version=(1, 0),
                              allow_pickle=False)
    return buf.getvalue()


def npz_bytes(arrays: dict) -> bytes:
    """An ``np.load``-readable archive whose bytes depend on the arrays ONLY
    (sorted members, fixed timestamp, stored uncompressed). ``np.savez`` stamps
    the wall clock into every member, so its sha256 is not reproducible."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_STORED) as zf:
        for name in sorted(arrays):
            zi = zipfile.ZipInfo(name + ".npy", date_time=(1980, 1, 1, 0, 0, 0))
            zi.compress_type = zipfile.ZIP_STORED
            zi.create_system = 0
            zi.external_attr = 0
            zf.writestr(zi, _npy_bytes(arrays[name]))
    return buf.getvalue()


def source_bindings(universe_id: str) -> dict:
    """sha256 of the manifests the mask was built from (the data vintage).

    The layer manifest lists the sha256 of every market CSV and the ml contract
    names the ml panel's; ``data_r38.load_certified_layer`` verifies the files
    themselves against both, so binding the two manifests binds the data."""
    import data_r38

    out = {"layer_manifest": str(data_r38.LAYER_MANIFEST),
           "layer_manifest_sha256": file_sha256(data_r38.LAYER_MANIFEST),
           "ml_contract": str(data_r38.ML_CONTRACT),
           "ml_contract_sha256": file_sha256(data_r38.ML_CONTRACT)}
    if universe_id == U2:
        out["aggregate_oi_manifest"] = str(data_r38.AGG_OI_MANIFEST)
        out["aggregate_oi_manifest_sha256"] = file_sha256(data_r38.AGG_OI_MANIFEST)
    return out


def persist_universe(built: dict, *, out_dir: Optional[Path] = None,
                     extra_meta: Optional[dict] = None) -> dict:
    """Write ``<universe_id>_mask_v1.npz`` + ``.meta.json``. Refuses a build on
    an overridden decision grid: the persisted mask is always the OWNER's."""
    if built.get("decision_idx_overridden"):
        raise RuntimeError("refusing to persist a universe on a non-owner grid")
    uid = built["universe_id"]
    _check_id(uid)
    arrays = built["arrays"]
    out = Path(out_dir) if out_dir else universe_dir()
    out.mkdir(parents=True, exist_ok=True)
    body = npz_bytes(arrays)
    npz_path = out / (mask_name(uid) + ".npz")
    tmp = npz_path.with_suffix(".npz.tmp")
    tmp.write_bytes(body)
    tmp.replace(npz_path)
    spec = SPEC[uid]
    meta = {
        "kind": "FUTURES_UNIVERSE_MASK",
        "agent_system_version": "PAPER_TRADER_ALPHA_AGENTS_V2",
        "run_id": "PAPER_TRADER_MULTI_AGENT_ALPHA_CAMPAIGN_R56_V2",
        "agent": "universe-construction-agent",
        "safety": list(SAFETY),
        "universe_id": uid, "work_order": spec["work_order"],
        "used_by": spec["used_by"],
        "dataset_id": spec["dataset_id"], "also_reads": spec["also_reads"],
        "asset_class": spec["asset_class"],
        "execution_representation": spec["execution_representation"],
        "short_leg_expressible": spec["short_leg_expressible"],
        "npz": npz_path.name,
        "npz_sha256": hashlib.sha256(body).hexdigest(),
        "npz_bytes": len(body),
        "content_sha256": content_sha256(arrays),
        "eligible_sha256": array_sha256(arrays["eligible"]),
        "arrays": {k: {"dtype": str(np.asarray(v).dtype),
                       "shape": list(np.asarray(v).shape),
                       "sha256": array_sha256(v)}
                   for k, v in sorted(arrays.items())},
        "orientation": "eligible[j, i]: j = decision (decision_idx[j] is the "
                       "grid slot t), i = LAYER row (layer_symbols[i]); "
                       "non-members are always False",
        "decision_grid": {
            "owner": "alpha_agent.r59.native.decision_indices(dates, 21, 21)",
            "cadence": CADENCE, "horizon": HORIZON,
            "n_decisions": int(len(arrays["decision_idx"])),
            "first": str(arrays["decision_dates"][0]),
            "last": str(arrays["decision_dates"][-1])},
        "n_members": int(arrays["member"].sum()),
        "members": [str(s) for s in arrays["symbols"]],
        "frozen_rule_constants": rule_constants(),
        "timing": "row t reads ret/ret2 at slots <= t-1 and volume / aggregate "
                  "open interest at slots <= t-2; liveness at t is applied by "
                  "the evaluator (R1), never by this mask",
        "survivorship": SURVIVORSHIP_WORDING,
        "sources": source_bindings(uid),
        "builder": "research.agents.campaign_r56_v2.universe_fut.build_universe",
        "module_sha256": file_sha256(Path(__file__).resolve()),
        "built_at": now_iso(),
    }
    if extra_meta:
        meta["extra"] = extra_meta
    meta_path = out / (mask_name(uid) + ".meta.json")
    tmp = meta_path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(meta, indent=1), encoding="utf-8")
    tmp.replace(meta_path)
    return meta


def rule_constants() -> dict:
    return {"PAIRED_WINDOW_SLOTS": "[t-%d, t-%d]" % PAIRED_WINDOW,
            "PAIRED_MIN_SESSIONS": PAIRED_MIN_SESSIONS,
            "TRADED_WINDOW_SLOTS": "[t-%d, t-%d]" % TRADED_WINDOW,
            "TRADED_MIN_SHARE": "%d/%d" % TRADED_MIN_SHARE,
            "OI_RECENT_WINDOW_SLOTS": "[t-%d, t-%d]" % OI_RECENT_WINDOW,
            "OI_BASE_WINDOW_SLOTS": "[t-%d, t-%d]" % OI_BASE_WINDOW,
            "OI_MIN_OWN_SESSIONS": OI_MIN_OWN_SESSIONS,
            "OI_MIN_FINITE_SHARE": "%d/%d" % OI_MIN_FINITE_SHARE,
            "LEG_WINDOW_SLOTS": "[t-%d, t-%d]" % LEG_WINDOW,
            "LEG_MIN_OWN_SESSIONS": LEG_MIN_OWN_SESSIONS,
            "TOT_PAIRS": TOT_PAIRS,
            "FROZEN_LIVENESS_APPLIED_BY_EVALUATOR":
                "%s, stale_slots=%d" % (FROZEN_CLOSED_RULE, FROZEN_STALE_SLOTS)}


# --------------------------------------------------------------------------- #
# The ONE reader
# --------------------------------------------------------------------------- #
def load_universe(universe_id: str, verify_hash: bool = True, *,
                  layer: Optional[dict] = None,
                  directory: Optional[Path] = None) -> dict:
    """The persisted mask of one universe.

    ``verify_hash``  npz sha256 AND the container-independent content hash are
                     checked against ``.meta.json``; the source manifests are
                     re-hashed, so a rebuilt layer / open-interest dataset is
                     refused rather than silently mis-aligned.
    ``layer``        when given, row and slot alignment with it is asserted.

    Returns every npz array plus ``meta``, ``row_of_slot`` ({grid slot t: row
    j}), ``eligible_members`` (``eligible[:, member_rows]``) and, for U4,
    ``groups`` ({label: bool[n_layer_markets]}, R38 and short labels).
    """
    _check_id(universe_id)
    d = Path(directory) if directory else universe_dir()
    npz_path = d / (mask_name(universe_id) + ".npz")
    meta = json.loads((d / (mask_name(universe_id) + ".meta.json"))
                      .read_text(encoding="utf-8"))
    with np.load(npz_path, allow_pickle=False) as z:
        arrays = {k: z[k] for k in z.files}
    if verify_hash:
        got = file_sha256(npz_path)
        if got != meta["npz_sha256"]:
            raise RuntimeError("%s npz sha256 %s != meta %s"
                               % (universe_id, got, meta["npz_sha256"]))
        if content_sha256(arrays) != meta["content_sha256"]:
            raise RuntimeError("%s content hash mismatch" % universe_id)
        now = source_bindings(universe_id)
        for key, val in meta["sources"].items():
            if key.endswith("sha256") and now.get(key) != val:
                raise RuntimeError("%s: source %s changed since the mask was "
                                   "built" % (universe_id, key))
    if str(arrays["universe_id"][0]) != universe_id:
        raise RuntimeError("npz holds %s" % arrays["universe_id"][0])
    if layer is not None:
        if [str(s) for s in arrays["layer_symbols"]] != list(layer["symbols"]):
            raise RuntimeError("layer rows differ from the mask's rows")
        if int(arrays["layer_n_sessions"][0]) != len(layer["dates"]):
            raise RuntimeError("layer grid length differs from the mask's")
        if not np.array_equal(layer["dates"][arrays["decision_idx"]],
                              arrays["decision_dates"]):
            raise RuntimeError("decision slots map to different dates")
        if not np.array_equal(arrays["decision_idx"], decision_grid(layer)):
            raise RuntimeError("the owner's decision grid differs from the mask's")
    out = dict(arrays)
    out["meta"] = meta
    out["row_of_slot"] = {int(t): j for j, t in enumerate(arrays["decision_idx"])}
    out["eligible_members"] = arrays["eligible"][:, arrays["member_rows"]]
    if universe_id == U4:
        groups = {}
        for lab in U4_GROUPS_R38:
            rows = arrays["group_r38"] == lab
            groups[lab] = rows
            groups[GROUP_SHORT_LABEL[lab]] = rows
        out["groups"] = groups
    return out


def eligible_at(universe: dict, t: int) -> np.ndarray:
    """bool[n_layer_markets] at decision slot t. A slot that is not one of the
    owner's decisions raises: eligibility is defined on the decision grid only."""
    j = universe["row_of_slot"].get(int(t))
    if j is None:
        raise KeyError("slot %d is not a decision of the owner's H21 grid" % int(t))
    return universe["eligible"][j].copy()


def pairs_live_at(universe: dict, t: int) -> np.ndarray:
    """U3 only: bool[3] in ``pair_ids`` order at decision slot t."""
    j = universe["row_of_slot"].get(int(t))
    if j is None:
        raise KeyError("slot %d is not a decision of the owner's H21 grid" % int(t))
    return universe["pair_live"][j].copy()


def rebuild_and_compare(universe_id: str, layer: Optional[dict] = None,
                        agg: Optional[dict] = None) -> dict:
    """Rebuild from the certified layer and compare with the persisted mask."""
    stored = load_universe(universe_id, verify_hash=True, layer=layer)
    built = build_universe(universe_id, layer, agg)
    body = npz_bytes(built["arrays"])
    return {"universe_id": universe_id,
            "stored_npz_sha256": stored["meta"]["npz_sha256"],
            "rebuilt_npz_sha256": hashlib.sha256(body).hexdigest(),
            "stored_content_sha256": stored["meta"]["content_sha256"],
            "rebuilt_content_sha256": content_sha256(built["arrays"]),
            "identical": hashlib.sha256(body).hexdigest()
                         == stored["meta"]["npz_sha256"]}


def main(argv) -> int:
    cmd = argv[1] if len(argv) > 1 else "verify"
    if cmd != "verify":
        print("usage: universe_fut.py verify")
        return 2
    import data_r38

    layer = data_r38.load_certified_layer()
    agg = data_r38.load_aggregate_oi(layer)
    rows = [rebuild_and_compare(u, layer, agg) for u in UNIVERSE_IDS]
    print(json.dumps(rows, indent=1))
    ok = all(r["identical"] for r in rows)
    print("UNIVERSE_FUT_VERIFY_OK" if ok else "UNIVERSE_FUT_VERIFY_FAILED")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
