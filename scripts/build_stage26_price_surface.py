"""scripts/build_stage26_price_surface.py — Stage 26 owned UNADJUSTED price surface.

Stage 25 proved point-in-time market cap was blocked by TWO independent gaps, and
this script closes the second one. The only owned daily price surface
(``phase25_fast_ohlc``) is **TOTALRETURN** adjusted, so ``price x shares`` is
wrong by the cumulative split-and-dividend factor. It never needed a purchase:
the same owned, entitled local Norgate installation that produced that surface
also serves ``NONE`` (raw traded price) and ``CAPITAL`` (capital-events-only)
adjustments.

Two closes are pulled per symbol:

* ``close_none``    — the price actually printed on the tape that day. Multiplied
  by the share count in force that day this is exactly market equity.
* ``close_capital`` — the same series back-adjusted for capital events ONLY (no
  dividends). The ratio ``close_capital / close_none`` is therefore the
  cumulative capital-event factor at that date, which is what lets a share count
  reported at one date be carried to a formation date at another **without
  guessing whether a split happened in between**.

Read-only with respect to Norgate (no upgrade, no write) and writes one npz plus
one manifest under the Stage-26 research root. No operational store is touched.

    .venv-win\\Scripts\\python.exe scripts\\build_stage26_price_surface.py
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from pathlib import Path

import numpy as np

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

DEFAULT_PANEL = (r"D:\Stock_Prediction_app_data\phase25_multi_horizon_alpha"
                 r"\_inputs\momentum_monthly_panel.csv")
DEFAULT_OUT = (r"D:\Stock_Prediction_app_data\stage26_alpha_challenger_expansion"
               r"\_inputs\price_surface_unadjusted.npz")
DEFAULT_START = "2009-01-01"
DEFAULT_FIRST_MONTH = "2009-06"


def panel_symbols(panel_path: Path, first_month: str) -> "list[str]":
    """Every symbol that was an index MEMBER at some month at/after the first
    formation month. Delisted names are retained (survivorship-safe)."""
    syms: set = set()
    with panel_path.open(newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            if r.get("is_member") == "1" and (r.get("month") or "") >= first_month:
                t = (r.get("ticker") or "").strip()
                if t:
                    syms.add(t)
    return sorted(syms)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--panel", default=DEFAULT_PANEL)
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--start", default=DEFAULT_START)
    ap.add_argument("--first-month", default=DEFAULT_FIRST_MONTH)
    ap.add_argument("--limit", type=int, default=0,
                    help="debug: cap the number of symbols pulled")
    args = ap.parse_args(argv)

    try:
        import norgatedata as nd
    except ImportError:
        print("BLOCKED: norgatedata package not importable")
        return 2
    if not nd.status():
        print("BLOCKED: Norgate Data Director is not running / not reachable")
        return 2

    symbols = panel_symbols(Path(args.panel), args.first_month)
    if args.limit:
        symbols = symbols[:args.limit]
    print("symbols: %d   start: %s" % (len(symbols), args.start))

    adjust = {
        "none": nd.StockPriceAdjustmentType.NONE,
        "capital": nd.StockPriceAdjustmentType.CAPITAL,
    }

    started = time.monotonic()
    per_symbol: "dict[str, dict]" = {}
    all_dates: set = set()
    missing: list = []
    for i, sym in enumerate(symbols):
        got = {}
        for key, setting in adjust.items():
            try:
                ts = nd.price_timeseries(
                    sym, stock_price_adjustment_setting=setting,
                    padding_setting=nd.PaddingType.NONE,
                    start_date=args.start, timeseriesformat="numpy-recarray")
            except Exception:  # noqa: BLE001 - one bad symbol never stops the pull
                ts = None
            if ts is None or len(ts) == 0:
                continue
            got[key] = (np.asarray(ts["Date"], dtype="datetime64[D]"),
                        np.asarray(ts["Close"], dtype="float64"))
        if "none" not in got or "capital" not in got:
            missing.append(sym)
            continue
        per_symbol[sym] = got
        all_dates.update(got["none"][0].tolist())
        if (i + 1) % 200 == 0:
            print("  %d/%d  elapsed %.0fs" % (i + 1, len(symbols),
                                              time.monotonic() - started),
                  flush=True)

    kept = sorted(per_symbol)
    dates = np.array(sorted(all_dates), dtype="datetime64[D]")
    didx = {d: i for i, d in enumerate(dates.tolist())}
    n_d, n_s = len(dates), len(kept)
    close_none = np.full((n_d, n_s), np.nan, dtype=np.float32)
    close_capital = np.full((n_d, n_s), np.nan, dtype=np.float32)
    for j, sym in enumerate(kept):
        for key, mat in (("none", close_none), ("capital", close_capital)):
            d, c = per_symbol[sym][key]
            rows = np.fromiter((didx[x] for x in d.tolist()), dtype=np.int64,
                               count=len(d))
            mat[rows, j] = c.astype(np.float32)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(out, dates=dates, symbols=np.array(kept),
                        close_none=close_none, close_capital=close_capital)

    finite = int(np.isfinite(close_none).sum())
    manifest = {
        "contract_version": "stage26-price-surface-1.0.0",
        "source": "Norgate Data (owned, local NDU v%s): US Equities + US Equities "
                  "Delisted" % getattr(nd, "__version__", "?"),
        "entitlement": "reachable+entitled; read-only; package NOT upgraded",
        "adjustments": {
            "close_none": "StockPriceAdjustmentType.NONE - raw traded price; the "
                          "correct multiplicand for an as-reported share count",
            "close_capital": "StockPriceAdjustmentType.CAPITAL - capital events "
                             "only, NO dividends; close_capital/close_none is the "
                             "cumulative capital-event factor at that date",
        },
        "why_not_totalreturn": "TOTALRETURN mixes splits AND dividends, so it "
                               "cannot be inverted into a share-count adjustment; "
                               "the phase25 surface is unusable for market equity",
        "symbols_requested": len(symbols),
        "symbols_resolved": n_s,
        "symbols_missing": len(missing),
        "missing_sample": missing[:25],
        "first_date": str(dates[0]) if n_d else None,
        "last_date": str(dates[-1]) if n_d else None,
        "trading_days": n_d,
        "finite_close_cells": finite,
        "npz_path": str(out),
        "npz_bytes": out.stat().st_size,
        "build_seconds": round(time.monotonic() - started, 1),
        "universe_note": "every symbol that was an index MEMBER at some month at "
                         "or after %s in the owned survivorship-safe momentum "
                         "panel; delisted names retained" % args.first_month,
    }
    mp = out.with_suffix(".manifest.json")
    mp.write_text(json.dumps(manifest, indent=1, sort_keys=True), encoding="utf-8")
    print("resolved %d/%d symbols, %d trading days, %.1f MB"
          % (n_s, len(symbols), n_d, out.stat().st_size / 1e6))
    print("manifest: %s" % mp)
    print("STAGE26_PRICE_SURFACE_COMPLETE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
