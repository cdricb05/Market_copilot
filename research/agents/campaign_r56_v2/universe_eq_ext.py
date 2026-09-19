r"""campaign_r56_v2.universe_eq_ext - the point-in-time equity EXTENSION universe (work order U5).

Universe id : ``EQ_EXT_SMALLMID_PIT_V1``   (PAPER_TRADER_MULTI_AGENT_ALPHA_CAMPAIGN_R56_V2)
Dataset id  : ``eq_ext_smallmid_pit_panel_v1`` (certified PIT_SAFE by the data-foundation-agent, DF2)
Owner       : universe-construction-agent.  RESEARCH ONLY - no order, no fill, no broker, no promotion.
Used by     : P6 (amihud liquidity premium, H21), P7 (residual momentum, H21), P8 (H5 reversal, next OPEN).

THE FROZEN RULE (director, foundation_requests.json, universe_construction_agent order 5 - never tuned here)
-------------------------------------------------------------------------------------------------------------
    at decision t: PIT member of the certified small/mid index set (Russell 2000) AND NOT a PIT S&P 500
    member; unadjusted close >= 5.00; 63-session median dollar volume >= 5,000,000; >= 260 sessions of
    history with >= 90% finite total return; finite total return at t.
    "This is alpha_agent.r57.engine.eligibility with EQ_MIN_ADV lowered from 1e7 to 5e6 and the S&P 500
    exclusion added - nothing else changes."

It is implemented TWICE and the two are proved bit-identical on every session before the mask is persisted:

* ``u5_eligibility(panel, t)``  - THE CANONICAL OWNER ITSELF: ``alpha_agent.r57.engine.eligibility`` called on
  ``data_eq_ext.as_r57_panel(panel, exclude_sp500=True)`` while ``engine.EQ_MIN_ADV`` is bound to 5e6
  in-process (``r57_engine_for_u5`` - restored on exit, no source file is edited).
* ``build_u5_mask(panel)``       - an independent vectorised implementation (cumulative-sum history count,
  sort-based rolling NaN-median) that produces the whole (n_symbols, n_dates) mask in one pass.

TIMING (state it once, exactly)
-------------------------------
Column ``t`` of the mask is computed from data with session index <= t ONLY (the close of session t: ``mem``,
``sp500_mem``, ``un``, ``tr`` at t; ``un*vol`` over [t-62, t]; ``tr`` finiteness over [t-259, t]). That is the
r57 convention "signal at the close of t". The mask is ACTIONABLE no earlier than session t+1 (P6/P7 enter at
the close of t+1, P8 at the open of t+1). In entry-session labelling: the universe used for an entry on
session e is ``mask[:, e-1]`` and uses data <= e-1 only  ->  ``mask_for_entry_session(u5, e)``.

ORIENTATION: the panel's native one - rows = securities (``panel['symbols']`` / ``panel['assetid']`` order),
columns = sessions (``panel['dates']`` order). ``elig[i, t]``.

Traps (inherited from the data agent - all respected here, all binding downstream)
----------------------------------------------------------------------------------
1. ``mem`` / ``sp500_mem`` are NEVER forward-filled or reindexed (1,733 dead names carry flag 1 on their last
   priced session). This module only ANDs them column by column.
2. Identity = ``assetid``. A symbol is not a PIT ticker; 113 base tickers are shared by two carried
   securities. Nothing here de-duplicates by ticker; rows stay aligned to the panel.
3. ``tr`` / ``op_tr`` LEVELS are back-adjusted: only finiteness of ``tr`` is used here. The price floor uses
   ``un`` (PIT price level) and dollar volume is ``un * vol`` (``dv`` is never read).
4. ``sectors`` = CURRENT GICS, not PIT: not used in any rule.
5. ``op_tr`` is NEVER read by the universe rule. A "valid open at t+1" filter would be LOOK-AHEAD (the open of
   t+1 is not observable at the close of t) and is deliberately absent.
6. The mask is defined on ``load_eq_ext_panel(dtype=float64)`` (the loader default); ``un * vol`` in float32
   could move a borderline median across the 5e6 floor.

Usage (PowerShell):
    & C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe `
        C:\Users\binis\paper_trader\research\agents\campaign_r56_v2\universe_eq_ext.py verify

Import:
    import sys
    sys.path.insert(0, r"C:\Users\binis\paper_trader")
    sys.path.insert(0, r"C:\Users\binis\paper_trader\research\agents\campaign_r56_v2")
    from data_eq_ext import load_eq_ext_panel, as_r57_panel
    from universe_eq_ext import load_u5_mask, r57_engine_for_u5, u5_r57_view
    panel = load_eq_ext_panel()
    u5 = load_u5_mask(panel=panel)              # sha256-verified, alignment-checked; u5["elig"][i, t]
    with r57_engine_for_u5() as engine:         # H21 books: the canonical evaluator with the U5 rule
        res = engine.run_topn(u5_r57_view(panel), score_fn, cadence=21, horizon=21, top_n=100,
                              cost_rate=0.0025, first_date="2011-07-01")
"""
from __future__ import annotations

import contextlib
import hashlib
import json
import os
import sys
import warnings
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parents[2]
for _p in (str(_REPO), str(_HERE)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

UNIVERSE_ID = "EQ_EXT_SMALLMID_PIT_V1"
DATASET_ID = "eq_ext_smallmid_pit_panel_v1"
ASSET_CLASS = "US_EQUITY"
EXECUTION_REPRESENTATION = "LONG_ONLY_EQUAL_WEIGHT_TOP_N_VS_EW_UNIVERSE"
SHORT_LEG_EXPRESSIBLE = False

# Frozen by the director (work order U5). Declared before any signal result exists. NOT tunable.
U5_MIN_PRICE = 5.0                 # unadjusted close, == alpha_agent.r57.EQ_MIN_PRICE (asserted)
U5_MIN_ADV = 5.0e6                 # 63-session median of un*vol; r57 default 1e7 LOWERED by the director
U5_ADV_WINDOW = 63                 # sessions [t-62, t], as alpha_agent.r57.engine.eligibility
U5_MIN_HISTORY = 260               # == alpha_agent.r57.EQ_MIN_HISTORY (asserted)
U5_MIN_HISTORY_FINITE_SHARE = 0.9  # finite tr on >= 234 of the sessions [t-259, t]
CAMPAIGN_FIRST_DECISION_DATE = "2011-07-01"
CAMPAIGN_TOP_N = 100

MASK_NAME = "EQ_EXT_SMALLMID_PIT_V1_mask_v1"
UNIVERSE_DIR_ENV = "PAPER_TRADER_R56V2_UNIVERSE_DIR"
DEFAULT_UNIVERSE_DIR = Path(r"D:\Stock_Prediction_app_data\r59_autonomous_alpha\agents_v2"
                            r"\campaign_R56_V2\universes")

TIMING_STATEMENT = ("mask[:, t] uses data with session index <= t only (close of session t) and is actionable "
                    "no earlier than session t+1; for an entry on session e the universe is mask[:, e-1] "
                    "(data <= e-1 only)")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 22), b""):
            h.update(chunk)
    return h.hexdigest()


def array_sha256(a: np.ndarray) -> str:
    return hashlib.sha256(np.ascontiguousarray(a).tobytes()).hexdigest()


def strings_sha256(a) -> str:
    return hashlib.sha256("\n".join(str(x) for x in a).encode("utf-8")).hexdigest()


def universe_dir() -> Path:
    return Path(os.environ.get(UNIVERSE_DIR_ENV) or DEFAULT_UNIVERSE_DIR)


# --------------------------------------------------------------------------- #
# The canonical owner, bound to the director's one changed constant
# --------------------------------------------------------------------------- #
def _assert_r57_constants(engine) -> None:
    """"Nothing else changes": refuse to run if the canonical owner's other constants have drifted."""
    if float(engine.EQ_MIN_PRICE) != U5_MIN_PRICE or int(engine.EQ_MIN_HISTORY) != U5_MIN_HISTORY:
        raise RuntimeError("alpha_agent.r57 constants drifted from the frozen U5 rule: EQ_MIN_PRICE=%r "
                           "EQ_MIN_HISTORY=%r" % (engine.EQ_MIN_PRICE, engine.EQ_MIN_HISTORY))


@contextlib.contextmanager
def r57_engine_for_u5():
    """``alpha_agent.r57.engine`` with ``EQ_MIN_ADV`` bound to the U5 floor (5e6) for the duration of the block.

    In-process binding only - no source file is touched, the r57 default (1e7) is restored on exit. Not
    thread-safe: one evaluation per process. Pass ``u5_r57_view(panel)`` as the panel so that the membership
    gate is ``mem AND NOT sp500_mem``; ``engine.eligibility`` is then EXACTLY the U5 rule."""
    from alpha_agent.r57 import engine                       # type: ignore
    _assert_r57_constants(engine)
    old = engine.EQ_MIN_ADV
    engine.EQ_MIN_ADV = U5_MIN_ADV
    try:
        yield engine
    finally:
        engine.EQ_MIN_ADV = old


def _require_float64(panel: dict) -> None:
    for k in ("tr", "un", "vol"):
        if panel[k].dtype != np.float64:
            raise ValueError("U5 is defined on load_eq_ext_panel(dtype=float64); panel[%r] is %s"
                             % (k, panel[k].dtype))


def u5_r57_view(panel: dict) -> dict:
    """The r57-shaped view whose ``mem`` is ``mem AND NOT sp500_mem`` (``data_eq_ext.as_r57_panel``)."""
    from data_eq_ext import as_r57_panel                     # type: ignore
    _require_float64(panel)
    return as_r57_panel(panel, exclude_sp500=True)


def u5_eligibility(panel: dict, t: int, view: dict | None = None) -> np.ndarray:
    """U5 eligibility at decision index ``t`` from THE CANONICAL OWNER. Uses data <= t only."""
    v = view if view is not None else u5_r57_view(panel)
    with r57_engine_for_u5() as engine:
        return engine.eligibility(v, int(t))


# --------------------------------------------------------------------------- #
# Independent vectorised build
# --------------------------------------------------------------------------- #
def _rolling_nanmedian(x: np.ndarray, window: int, chunk_symbols: int = 128) -> np.ndarray:
    """NaN-aware median of x[:, t-window+1 : t+1] for every t (shorter window at the left edge, as r57)."""
    n, n_dates = x.shape
    out = np.full((n, n_dates), np.nan, dtype=np.float64)
    for a in range(0, n, chunk_symbols):
        b = min(n, a + chunk_symbols)
        blk = np.concatenate([np.full((b - a, window - 1), np.nan), x[a:b]], axis=1)
        w = np.lib.stride_tricks.sliding_window_view(blk, window, axis=1)      # (k, n_dates, window) view
        s = np.sort(w, axis=2)                                                  # copy; NaN sorts last
        cnt = window - np.isnan(s).sum(axis=2)
        lo = np.clip((cnt - 1) // 2, 0, window - 1)
        hi = np.clip(cnt // 2, 0, window - 1)
        vlo = np.take_along_axis(s, lo[:, :, None], axis=2)[:, :, 0]
        vhi = np.take_along_axis(s, hi[:, :, None], axis=2)[:, :, 0]
        med = (vlo + vhi) / 2.0
        med[cnt == 0] = np.nan
        out[a:b] = med
    return out


def build_u5_stages(panel: dict, chunk_symbols: int = 128) -> dict:
    """Every stage of the frozen rule as a boolean (n_symbols, n_dates) array. Column t uses data <= t only.

    Keys: member (PIT Russell 2000), member_ex_sp500, price_ok, adv_ok, history_ok, tr_ok, elig (the AND)."""
    _require_float64(panel)
    from alpha_agent.r57 import engine                       # type: ignore
    _assert_r57_constants(engine)
    tr, un, vol = panel["tr"], panel["un"], panel["vol"]
    member = panel["mem"] > 0
    member_ex = member & ~(panel["sp500_mem"] > 0)
    with np.errstate(invalid="ignore"), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        price_ok = np.isfinite(un) & (un >= U5_MIN_PRICE)
        dvol = un * vol
        dvol = np.where(np.isfinite(dvol), dvol, np.nan)
        med = _rolling_nanmedian(dvol, U5_ADV_WINDOW, chunk_symbols)
        del dvol
        adv_ok = np.isfinite(med) & (med >= U5_MIN_ADV)
        del med
    tr_ok = np.isfinite(tr)
    c = np.cumsum(tr_ok, axis=1, dtype=np.int32)
    cnt = c.copy()
    h = U5_MIN_HISTORY
    if c.shape[1] > h:
        cnt[:, h:] = c[:, h:] - c[:, :-h]
    history_ok = cnt >= U5_MIN_HISTORY * U5_MIN_HISTORY_FINITE_SHARE
    elig = member_ex & price_ok & adv_ok & history_ok & tr_ok
    return {"member": member, "member_ex_sp500": member_ex, "price_ok": price_ok, "adv_ok": adv_ok,
            "history_ok": history_ok, "tr_ok": tr_ok, "elig": elig}


def build_u5_mask(panel: dict, chunk_symbols: int = 128) -> np.ndarray:
    """The U5 PIT eligibility mask, bool (n_symbols, n_dates), panel-native orientation. ~1 GB peak."""
    return build_u5_stages(panel, chunk_symbols)["elig"]


def build_u5_mask_canonical(panel: dict, t_indices=None, progress_every: int = 0) -> np.ndarray:
    """The same mask from the canonical owner, one ``engine.eligibility`` call per session (slow: minutes).

    Columns not in ``t_indices`` stay False."""
    view = u5_r57_view(panel)
    n, n_dates = panel["tr"].shape
    ts = range(n_dates) if t_indices is None else [int(t) for t in t_indices]
    out = np.zeros((n, n_dates), dtype=bool)
    with r57_engine_for_u5() as engine, warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for k, t in enumerate(ts):
            if progress_every and k % progress_every == 0:
                print("canonical eligibility %d/%d" % (k, len(ts)), flush=True)
            out[:, t] = engine.eligibility(view, t)
    return out


# --------------------------------------------------------------------------- #
# Persist / load
# --------------------------------------------------------------------------- #
def persist_u5_mask(panel: dict, elig: np.ndarray, extra_meta: dict | None = None,
                    directory: Path | None = None) -> dict:
    """Write the mask npz + meta json. Immutable: an existing artifact is never rewritten."""
    d = Path(directory) if directory else universe_dir()
    d.mkdir(parents=True, exist_ok=True)
    npz_path, meta_path = d / (MASK_NAME + ".npz"), d / (MASK_NAME + ".meta.json")
    if npz_path.exists() or meta_path.exists():
        raise FileExistsError("U5 mask artifact already present (immutable): %s" % npz_path)
    if elig.shape != panel["tr"].shape or elig.dtype != bool:
        raise ValueError("mask must be bool with the panel's (n_symbols, n_dates) shape")
    arrays = {"elig": elig.astype(np.uint8), "n_eligible": elig.sum(axis=0).astype(np.int32),
              "dates": np.asarray(panel["dates"]).astype("U10"),
              "symbols": np.asarray(panel["symbols"]).astype("U"),
              "assetid": np.asarray(panel["assetid"]).astype("U")}
    tmp = d / (MASK_NAME + ".building.npz")
    np.savez_compressed(tmp, **arrays)
    tmp.replace(npz_path)
    first = int(np.searchsorted(panel["dates"], CAMPAIGN_FIRST_DECISION_DATE))
    meta = {
        "artifact": MASK_NAME, "universe_id": UNIVERSE_ID, "dataset_id": DATASET_ID,
        "asset_class": ASSET_CLASS, "execution_representation": EXECUTION_REPRESENTATION,
        "short_leg_expressible": SHORT_LEG_EXPRESSIBLE,
        "built_at": now_iso(), "builder": "research/agents/campaign_r56_v2/universe_eq_ext.py",
        "builder_sha256": file_sha256(Path(__file__).resolve()),
        "panel_npz_sha256": panel["meta"]["npz_sha256"],
        "orientation": "elig[i, t]: rows = securities in panel['symbols'] / panel['assetid'] order, "
                       "columns = sessions in panel['dates'] order (the panel's native orientation)",
        "timing": TIMING_STATEMENT,
        "rule": {"member": "PIT Russell 2000 (mem > 0) AND NOT PIT S&P 500 (sp500_mem > 0), at t",
                 "min_unadjusted_close": U5_MIN_PRICE, "min_median_dollar_volume": U5_MIN_ADV,
                 "dollar_volume": "un * vol", "adv_window_sessions": U5_ADV_WINDOW,
                 "min_history_sessions": U5_MIN_HISTORY,
                 "min_finite_tr_share_in_history": U5_MIN_HISTORY_FINITE_SHARE,
                 "finite_tr_at_t": True,
                 "canonical_owner": "alpha_agent.r57.engine.eligibility with EQ_MIN_ADV 1e7 -> 5e6 and "
                                    "mem := mem AND NOT sp500_mem; nothing else changes"},
        "n_symbols": int(elig.shape[0]), "n_dates": int(elig.shape[1]),
        "date_start": str(panel["dates"][0]), "date_end": str(panel["dates"][-1]),
        "campaign_first_decision_date": CAMPAIGN_FIRST_DECISION_DATE,
        "campaign_first_decision_index": first,
        "eligible_name_days_all_sessions": int(elig.sum()),
        "eligible_name_days_campaign_span": int(elig[:, first:].sum()),
        "npz_keys": sorted(arrays),
        "array_sha256": {"elig": array_sha256(arrays["elig"]), "n_eligible": array_sha256(arrays["n_eligible"])},
        "symbols_sha256": strings_sha256(panel["symbols"]), "dates_sha256": strings_sha256(panel["dates"]),
        "assetid_sha256": strings_sha256(panel["assetid"]),
        "npz_sha256": file_sha256(npz_path), "npz_bytes": npz_path.stat().st_size,
    }
    meta.update(extra_meta or {})
    tmpm = meta_path.with_suffix(".json.tmp")
    tmpm.write_text(json.dumps(meta, indent=1), encoding="utf-8")
    tmpm.replace(meta_path)
    return meta


def load_u5_mask(directory: Path | None = None, verify_hash: bool = True, panel: dict | None = None) -> dict:
    """The persisted U5 mask. ``verify_hash`` recomputes the npz sha256 against the meta and refuses a
    mismatch; with ``panel`` the row/column alignment and the panel hash binding are asserted too.

    Returns {"meta", "elig" (bool, n_symbols x n_dates), "n_eligible", "dates", "symbols", "assetid"}."""
    d = Path(directory) if directory else universe_dir()
    npz_path, meta_path = d / (MASK_NAME + ".npz"), d / (MASK_NAME + ".meta.json")
    if not npz_path.exists() or not meta_path.exists():
        raise FileNotFoundError("U5 mask not present: %s" % npz_path)
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    if verify_hash:
        got = file_sha256(npz_path)
        if got != meta["npz_sha256"]:
            raise ValueError("U5 mask hash mismatch: meta %s file %s" % (meta["npz_sha256"], got))
    z = np.load(npz_path)
    u5 = {"meta": meta, "elig": z["elig"] > 0, "n_eligible": z["n_eligible"],
          "dates": z["dates"], "symbols": z["symbols"], "assetid": z["assetid"]}
    if verify_hash and array_sha256(z["elig"]) != meta["array_sha256"]["elig"]:
        raise ValueError("U5 mask array hash mismatch")
    if panel is not None:
        if panel["meta"]["npz_sha256"] != meta["panel_npz_sha256"]:
            raise ValueError("U5 mask was built on a different panel file (npz sha256 differs)")
        if (u5["elig"].shape != panel["tr"].shape
                or not np.array_equal(u5["dates"], np.asarray(panel["dates"]).astype("U10"))
                or not np.array_equal(u5["symbols"], np.asarray(panel["symbols"]).astype("U"))
                or not np.array_equal(u5["assetid"], np.asarray(panel["assetid"]).astype("U"))):
            raise ValueError("U5 mask rows/columns are not aligned with the panel")
    return u5


def eligible_at(u5: dict, t: int) -> np.ndarray:
    """Eligible securities (bool over symbols) for the decision taken at the close of session index t."""
    return u5["elig"][:, int(t)]


def mask_for_entry_session(u5: dict, e: int) -> np.ndarray:
    """The universe that may be ENTERED on session index e: the decision of e-1 (data <= e-1 only)."""
    if int(e) < 1:
        raise IndexError("no decision session precedes entry session %d" % e)
    return u5["elig"][:, int(e) - 1]


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def main(argv) -> int:
    from data_eq_ext import load_eq_ext_panel                # type: ignore
    cmd = argv[1] if len(argv) > 1 else "verify"
    panel = load_eq_ext_panel(verify_hash=True)
    if cmd == "build":
        elig = build_u5_mask(panel)
        meta = persist_u5_mask(panel, elig)
        print(json.dumps({k: meta[k] for k in ("artifact", "universe_id", "n_symbols", "n_dates",
                                                "eligible_name_days_campaign_span", "npz_sha256")}, indent=1))
        return 0
    u5 = load_u5_mask(verify_hash=True, panel=panel)         # hash + alignment
    rebuilt = build_u5_mask(panel)                           # content recomputed from the certified panel
    same = bool(np.array_equal(rebuilt, u5["elig"]))
    print(json.dumps({"artifact": MASK_NAME, "npz_sha256": u5["meta"]["npz_sha256"],
                      "rebuilt_equals_persisted": same,
                      "rebuilt_array_sha256": array_sha256(rebuilt.astype(np.uint8)),
                      "persisted_array_sha256": u5["meta"]["array_sha256"]["elig"]}, indent=1))
    return 0 if same else 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
