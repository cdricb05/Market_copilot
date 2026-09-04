"""alpha_agent.r57.futures - the Norgate Continuous Futures research panel.

Per market, per continuous methodology ('&MKT' default back-adjusted and
'&MKT_CCB'), the panel carries the daily DOLLAR P&L of holding one contract:

    pnl[t] = (adj_close[t] - adj_close[t-1]) * point_value

Percent-return arithmetic on back-adjusted series is forbidden by the protocol
(back-adjustment can drive prices through zero and fabricate returns), so every
downstream calculation works in dollars against declared risk capital.

Roll days are detected from the 'Delivery Month' column; each roll charges two
sides on the rolled notional in the evaluation engine.

The mechanical universe rule (protocol): every market with >= 2000 sessions of
history and a bar on/after 2026-08-01. No hand-picking.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

import numpy as np

from . import PANEL_END, PANEL_START, now_iso, research_root, stable_hash

FUT_PANEL_NAME = "futures_panel_v1"
MIN_SESSIONS = 2000
MUST_TRADE_ON_OR_AFTER = "2026-08-01"


def _ng():
    logging.disable(logging.WARNING)
    import norgatedata
    return norgatedata


def futures_dir() -> Path:
    d = research_root() / "panels"
    d.mkdir(parents=True, exist_ok=True)
    return d


def build_futures_panel(progress_every: int = 25) -> dict:
    npz_path = futures_dir() / (FUT_PANEL_NAME + ".npz")
    meta_path = futures_dir() / (FUT_PANEL_NAME + ".meta.json")
    if npz_path.exists() and meta_path.exists():
        return json.loads(meta_path.read_text(encoding="utf-8"))

    ng = _ng()
    spy = ng.price_timeseries("SPY", padding_setting=ng.PaddingType.NONE,
                              start_date=PANEL_START, end_date=PANEL_END,
                              timeseriesformat="pandas-dataframe")
    dates = np.array([d.strftime("%Y-%m-%d") for d in spy.index])
    date_ix = {d: i for i, d in enumerate(dates)}
    n_dates = len(dates)

    all_syms = sorted(ng.database_symbols("Continuous Futures"))
    bases = sorted({s for s in all_syms if not s.endswith("_CCB")})
    markets, rows_a, rows_b, rolls_rows, meta_rows = [], [], [], [], []
    skipped = []
    for k, sym in enumerate(bases):
        if progress_every and k % progress_every == 0:
            print("futures %d/%d %s" % (k, len(bases), sym), flush=True)
        try:
            df = ng.price_timeseries(sym, padding_setting=ng.PaddingType.NONE,
                                     start_date=PANEL_START, end_date=PANEL_END,
                                     timeseriesformat="pandas-dataframe")
        except Exception:                                  # noqa: BLE001
            skipped.append({"symbol": sym, "reason": "LOAD_FAILED"})
            continue
        if df is None or len(df) < MIN_SESSIONS:
            skipped.append({"symbol": sym, "reason": "INSUFFICIENT_HISTORY"})
            continue
        last = df.index[-1].strftime("%Y-%m-%d")
        if last < MUST_TRADE_ON_OR_AFTER:
            skipped.append({"symbol": sym, "reason": "STALE_MARKET",
                            "last": last})
            continue
        try:
            pv = float(ng.point_value(sym))
        except Exception:                                  # noqa: BLE001
            skipped.append({"symbol": sym, "reason": "NO_POINT_VALUE"})
            continue

        close_a = np.full(n_dates, np.nan)
        roll = np.zeros(n_dates, dtype=np.uint8)
        ii = np.array([date_ix.get(d.strftime("%Y-%m-%d"), -1) for d in df.index])
        ok = ii >= 0
        close_a[ii[ok]] = df["Close"].to_numpy(dtype=np.float64)[ok]
        if "Delivery Month" in df.columns:
            dm = df["Delivery Month"].to_numpy()
            ch = np.zeros(len(dm), dtype=np.uint8)
            ch[1:] = (dm[1:] != dm[:-1]).astype(np.uint8)
            roll[ii[ok]] = ch[ok]

        close_b = np.full(n_dates, np.nan)
        try:
            dfb = ng.price_timeseries(sym + "_CCB",
                                      padding_setting=ng.PaddingType.NONE,
                                      start_date=PANEL_START, end_date=PANEL_END,
                                      timeseriesformat="pandas-dataframe")
            jj = np.array([date_ix.get(d.strftime("%Y-%m-%d"), -1)
                           for d in dfb.index])
            okb = jj >= 0
            close_b[jj[okb]] = dfb["Close"].to_numpy(dtype=np.float64)[okb]
        except Exception:                                  # noqa: BLE001
            pass

        try:
            name = ng.security_name(sym)
        except Exception:                                  # noqa: BLE001
            name = sym
        try:
            cls = ng.classification_at_level(sym, "NorgateDataFuturesClassification",
                                             "Name", level=1)
        except Exception:                                  # noqa: BLE001
            cls = None

        markets.append(sym)
        rows_a.append(close_a)
        rows_b.append(close_b)
        rolls_rows.append(roll)
        meta_rows.append({"symbol": sym, "name": name, "point_value": pv,
                          "classification": cls,
                          "first": str(dates[int(ii[ok][0])]),
                          "last": last, "sessions": int(ok.sum())})

    close_a = np.vstack(rows_a)
    close_b = np.vstack(rows_b)
    rolls = np.vstack(rolls_rows)
    np.savez_compressed(npz_path, close_a=close_a, close_b=close_b, rolls=rolls)
    meta = {
        "panel": FUT_PANEL_NAME, "built_at": now_iso(),
        "n_markets": len(markets), "n_dates": n_dates,
        "date_start": str(dates[0]), "date_end": str(dates[-1]),
        "dates": [str(d) for d in dates],
        "markets": meta_rows, "skipped": skipped,
        "universe_rule": ">=%d sessions and a bar on/after %s"
                          % (MIN_SESSIONS, MUST_TRADE_ON_OR_AFTER),
        "pnl_basis": "delta(back-adjusted close) x point value, in dollars",
        "manifest_hash": stable_hash({"markets": [m["symbol"] for m in meta_rows],
                                      "d0": str(dates[0]), "d1": str(dates[-1])}),
    }
    meta_path.write_text(json.dumps(meta, indent=1), encoding="utf-8")
    return meta


def load_futures_panel() -> dict:
    meta = json.loads((futures_dir() / (FUT_PANEL_NAME + ".meta.json"))
                      .read_text(encoding="utf-8"))
    z = np.load(futures_dir() / (FUT_PANEL_NAME + ".npz"))
    return {"meta": meta,
            "dates": np.array(meta["dates"]),
            "markets": [m["symbol"] for m in meta["markets"]],
            "point_values": np.array([m["point_value"] for m in meta["markets"]]),
            "classifications": [m.get("classification") for m in meta["markets"]],
            "close_a": z["close_a"], "close_b": z["close_b"], "rolls": z["rolls"]}
