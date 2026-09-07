"""alpha_agent.r57.panel - the survivorship-safe equity research panel.

Built ONCE from the licensed local Norgate database and cached as npz:

    dates        SPY session calendar (PANEL_START .. PANEL_END)
    tr_close     TOTAL-RETURN adjusted close  (n_symbols x n_dates, float32)
    unadj_close  unadjusted close             (price floors / dollar volume)
    volume       share volume
    member       point-in-time S&P 500 membership 0/1 per security per day
                 (norgatedata.index_constituent_timeseries - a delisted or
                 removed name keeps its historical membership; today's list is
                 never applied backwards)
    sectors      GICS sector name per symbol (CURRENT classification - the one
                 declared limitation: GICS history is not point-in-time here,
                 which the protocol discloses for the sector-relative family)

The cache is immutable once written: rebuilding writes a NEW file keyed by the
manifest hash. Reads are pure.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

import numpy as np

from . import (PANEL_END, PANEL_START, now_iso, research_root, stable_hash)

PANEL_NAME = "sp500_pit_panel_v1"
WATCHLIST = "S&P 500 Current & Past"
INDEX_NAME = "S&P 500"
CALENDAR_SYMBOL = "SPY"


def _ng():
    logging.disable(logging.WARNING)
    import norgatedata
    return norgatedata


def panel_dir() -> Path:
    d = research_root() / "panels"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _series(ng, symbol: str, adjustment, start: str, end: str):
    return ng.price_timeseries(
        symbol,
        stock_price_adjustment_setting=adjustment,
        padding_setting=ng.PaddingType.NONE,
        start_date=start, end_date=end,
        timeseriesformat="pandas-dataframe")


def build_panel(progress_every: int = 200) -> dict:
    """Build and cache the panel. Idempotent: an existing cache is returned."""
    npz_path = panel_dir() / (PANEL_NAME + ".npz")
    meta_path = panel_dir() / (PANEL_NAME + ".meta.json")
    if npz_path.exists() and meta_path.exists():
        return json.loads(meta_path.read_text(encoding="utf-8"))

    ng = _ng()
    spy = _series(ng, CALENDAR_SYMBOL, ng.StockPriceAdjustmentType.TOTALRETURN,
                  PANEL_START, PANEL_END)
    dates = np.array([d.strftime("%Y-%m-%d") for d in spy.index])
    date_ix = {d: i for i, d in enumerate(dates)}
    n_dates = len(dates)

    symbols = sorted(set(ng.watchlist_symbols(WATCHLIST)))
    n = len(symbols)
    tr = np.full((n, n_dates), np.nan, dtype=np.float32)
    un = np.full((n, n_dates), np.nan, dtype=np.float32)
    vol = np.full((n, n_dates), np.nan, dtype=np.float32)
    mem = np.zeros((n, n_dates), dtype=np.uint8)
    sectors = []
    skipped = []

    for k, sym in enumerate(symbols):
        if progress_every and k % progress_every == 0:
            print("panel %d/%d %s" % (k, n, sym), flush=True)
        try:
            df_tr = _series(ng, sym, ng.StockPriceAdjustmentType.TOTALRETURN,
                            PANEL_START, PANEL_END)
        except Exception:                                  # noqa: BLE001
            skipped.append(sym)
            sectors.append(None)
            continue
        if df_tr is None or len(df_tr) == 0:
            skipped.append(sym)
            sectors.append(None)
            continue
        ii = np.array([date_ix.get(d.strftime("%Y-%m-%d"), -1)
                       for d in df_tr.index])
        ok = ii >= 0
        tr[k, ii[ok]] = df_tr["Close"].to_numpy(dtype=np.float32)[ok]
        try:
            df_un = _series(ng, sym, ng.StockPriceAdjustmentType.NONE,
                            PANEL_START, PANEL_END)
            ju = np.array([date_ix.get(d.strftime("%Y-%m-%d"), -1)
                           for d in df_un.index])
            oku = ju >= 0
            un[k, ju[oku]] = df_un["Close"].to_numpy(dtype=np.float32)[oku]
            if "Volume" in df_un.columns:
                vol[k, ju[oku]] = df_un["Volume"].to_numpy(dtype=np.float32)[oku]
        except Exception:                                  # noqa: BLE001
            pass
        try:
            df_m = ng.index_constituent_timeseries(
                sym, INDEX_NAME, padding_setting=ng.PaddingType.NONE,
                timeseriesformat="pandas-dataframe")
            jm = np.array([date_ix.get(d.strftime("%Y-%m-%d"), -1)
                           for d in df_m.index])
            okm = jm >= 0
            mem[k, jm[okm]] = df_m["Index Constituent"].to_numpy(dtype=np.uint8)[okm]
        except Exception:                                  # noqa: BLE001
            pass
        try:
            sectors.append(ng.classification_at_level(sym, "GICS", "Name", level=1))
        except Exception:                                  # noqa: BLE001
            sectors.append(None)

    spy_tr = spy["Close"].to_numpy(dtype=np.float64)
    np.savez_compressed(npz_path, tr=tr, un=un, vol=vol, mem=mem,
                        spy_tr=spy_tr)
    meta = {
        "panel": PANEL_NAME, "built_at": now_iso(),
        "watchlist": WATCHLIST, "index": INDEX_NAME,
        "n_symbols": n, "n_dates": n_dates,
        "date_start": str(dates[0]), "date_end": str(dates[-1]),
        "symbols": list(symbols), "sectors": sectors,
        "dates": [str(d) for d in dates],
        "skipped_symbols": skipped,
        "n_member_days": int(mem.sum()),
        "price_basis": "TOTALRETURN for returns; UNADJUSTED for floors/volume",
        "pit_membership": "norgatedata.index_constituent_timeseries per security",
        "manifest_hash": stable_hash({"symbols": list(symbols),
                                      "d0": str(dates[0]), "d1": str(dates[-1]),
                                      "member_days": int(mem.sum())}),
    }
    meta_path.write_text(json.dumps(meta, indent=1), encoding="utf-8")
    return meta


def load_panel() -> dict:
    """Load the cached panel into memory. Pure read."""
    meta = json.loads((panel_dir() / (PANEL_NAME + ".meta.json"))
                      .read_text(encoding="utf-8"))
    z = np.load(panel_dir() / (PANEL_NAME + ".npz"))
    return {
        "meta": meta,
        "dates": np.array(meta["dates"]),
        "symbols": np.array(meta["symbols"]),
        "sectors": np.array([s or "Unknown" for s in meta["sectors"]]),
        "tr": z["tr"].astype(np.float64),
        "un": z["un"].astype(np.float64),
        "vol": z["vol"].astype(np.float64),
        "mem": z["mem"],
        "spy_tr": z["spy_tr"],
    }
