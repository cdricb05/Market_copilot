"""alpha_agent.r63.panels - the R63 research substrates, from OWNED data only.

Every loader here reads an owned, already-acquired store and says where it
read it from. Nothing is fetched here (acquisition lives in ``acquire``), and
nothing is written outside ``research_root()/_derived`` (speed caches that are
reproducible from the owned bytes and are never evidence).

Substrates:

    futures         R41 dated-contract curve store: 107 markets, front/second/
                    third dated-contract returns under the observable roll
                    rule, settlements c1..c3, curve slopes, OI, volume, dte
    policy_path     ZQ / SR3 dated contracts (c1..c8) -> implied policy path
    equity          R57 PIT S&P 500 panel + R58 PANEL-F fundamentals
    fred_daily      unrevised daily market series (yields, OAS, breakevens,
                    real yields, VIX/OVX/GVZ, EFFR, SOFR)
    cboe            VIX9D / VIX / VIX3M / VIX6M / VVIX / SKEW closes
    alfred          true vintages for UNRATE, CPIAUCSL, ICSA, NFCI
    cot             CFTC Commitments of Traders, mapped to markets
    eia             weekly petroleum stocks (owned) and natural-gas storage
                    (acquired by R63 if reachable)
    insider         SEC Form 3/4/5 structured data sets -> per-CIK filing counts
    submissions     SEC per-issuer filing histories (acquired by R63)
"""
from __future__ import annotations

import csv
import glob
import io
import json
import os
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

from . import (AC_COMMODITY, AC_EQUITY_INDEX, AC_FX, AC_RATES, AC_VOLATILITY,
               COT_ARCHIVE_DIR, EIA_PET_BULK, FORM345_DIR, FRED_R35_DIR,
               FUT_DEFAULT_COST_BPS,
               INGEST_NORMALIZED, R38_ML_PANEL, R41_CBOE_DIR, R41_FRED_DAILY,
               R41_ROOT, R57_FUTURES_PANEL_META, research_root, stable_hash)

CALCULATION_OWNER = "alpha_agent.r63.panels"

R41_CURVES = R41_ROOT / "_data_curves"
MIN_SESSIONS = 2000
MUST_TRADE_ON_OR_AFTER = "2026-08-01"
EXCLUDED_MARKETS = {
    "MES": "MICRO_DUPLICATE_OF_ES", "MNQ": "MICRO_DUPLICATE_OF_NQ",
    "M2K": "MICRO_DUPLICATE_OF_RTY", "MYM": "MICRO_DUPLICATE_OF_YM",
    "MBT": "MICRO_CRYPTO", "MET": "MICRO_CRYPTO",
    "BTC": "CRYPTO_NOT_IN_SUPPORTED_ESTATE", "ETH": "CRYPTO_NOT_IN_SUPPORTED_ESTATE",
    "MHI": "MINI_DUPLICATE_OF_HSI",
}
STIR_MARKETS = ("ZQ", "SR3", "LEU", "SO3", "YIB", "YIR")
VOLATILITY_MARKETS = ("VX",)
# Norgate classification -> R63 scope (mirrors R59's NORGATE_CLASS_MAP).
NORGATE_CLASS_MAP = {
    "Stock Index": AC_EQUITY_INDEX, "Interest Rate": AC_RATES,
    "Agriculture & Livestock": AC_COMMODITY, "Energy": AC_COMMODITY,
    "Metal": AC_COMMODITY, "Currency": AC_FX, "Other": AC_COMMODITY,
}
R38_CLASS_MAP = {"COMMODITY": AC_COMMODITY, "FX": AC_FX, "RATES": AC_RATES,
                 "INTERNATIONAL_EQUITY": AC_EQUITY_INDEX, "VOLATILITY": AC_VOLATILITY}

_CACHE: dict = {}


def derived_dir() -> Path:
    d = research_root() / "_derived"
    d.mkdir(parents=True, exist_ok=True)
    return d


# --------------------------------------------------------------------------- #
# Futures
# --------------------------------------------------------------------------- #
def _r38_meta() -> dict:
    if "r38_meta" in _CACHE:
        return _CACHE["r38_meta"]
    out = {}
    if R38_ML_PANEL.exists():
        df = pd.read_csv(R38_ML_PANEL, usecols=["market_id", "asset_class",
                                                "economic_group",
                                                "cost_bps_per_side"])
        g = df.groupby("market_id").agg(asset_class=("asset_class", "first"),
                                        economic_group=("economic_group", "first"),
                                        cost=("cost_bps_per_side", "median"))
        for m, r in g.iterrows():
            out[str(m)] = {"asset_class": R38_CLASS_MAP.get(str(r["asset_class"])),
                           "economic_group": str(r["economic_group"]),
                           "cost_bps_per_side": float(r["cost"])}
    _CACHE["r38_meta"] = out
    return out


def _r57_classes() -> dict:
    if "r57_cls" in _CACHE:
        return _CACHE["r57_cls"]
    out = {}
    if R57_FUTURES_PANEL_META.exists():
        meta = json.loads(R57_FUTURES_PANEL_META.read_text(encoding="utf-8"))
        for m in meta.get("markets") or []:
            sym = str(m.get("symbol")).lstrip("&")
            out[sym] = {"classification": m.get("classification"),
                        "name": m.get("name"), "point_value": m.get("point_value")}
    _CACHE["r57_cls"] = out
    return out


def futures_market_meta(market: str) -> dict | None:
    """Scope, group and cost for one market, or None with no honest class."""
    r38 = _r38_meta().get(market)
    r57 = _r57_classes().get(market)
    if market in VOLATILITY_MARKETS:
        ac = AC_VOLATILITY
    elif market in STIR_MARKETS:
        ac = AC_RATES
    elif r38 and r38.get("asset_class"):
        ac = r38["asset_class"]
    elif r57 and r57.get("classification") in NORGATE_CLASS_MAP:
        ac = NORGATE_CLASS_MAP[r57["classification"]]
    else:
        return None
    group = (r38 or {}).get("economic_group") or (
        "STIR_FUTURES" if market in STIR_MARKETS else
        "US_INDEX_FUTURES" if ac == AC_EQUITY_INDEX else
        "%s_UNGROUPED" % ac)
    cost = (r38 or {}).get("cost_bps_per_side", FUT_DEFAULT_COST_BPS)
    return {"asset_class": ac, "economic_group": group,
            "cost_bps_per_side": float(cost),
            "cost_source": "R38_MEASURED" if r38 else "R38_MAXIMUM_DEFAULT",
            "name": (r57 or {}).get("name")}


def futures_universe() -> dict:
    """Markets admitted by the protocol's mechanical rule, with reasons for
    every exclusion. Pure function of the on-disk store."""
    admitted, excluded = [], []
    for p in sorted(glob.glob(str(R41_CURVES / "*_daily.csv"))):
        m = Path(p).name[:-len("_daily.csv")]
        if m in EXCLUDED_MARKETS:
            excluded.append({"market": m, "reason": EXCLUDED_MARKETS[m]})
            continue
        meta = futures_market_meta(m)
        if meta is None:
            excluded.append({"market": m, "reason": "NO_HONEST_ASSET_CLASS"})
            continue
        df = pd.read_csv(p, usecols=["date", "ret1"])
        n = int(df["ret1"].notna().sum())
        last = str(df["date"].max())
        if n < MIN_SESSIONS:
            excluded.append({"market": m, "reason": "INSUFFICIENT_HISTORY", "sessions": n})
            continue
        if last < MUST_TRADE_ON_OR_AFTER:
            excluded.append({"market": m, "reason": "STALE_MARKET", "last": last})
            continue
        admitted.append({"market": m, "sessions": n, "first": str(df["date"].min()),
                         "last": last, **meta})
    return {"admitted": admitted, "excluded": excluded,
            "universe_rule": ">=%d sessions and a bar on/after %s; declared "
                             "exclusions only" % (MIN_SESSIONS, MUST_TRADE_ON_OR_AFTER)}


def load_futures() -> dict:
    """Aligned futures substrate on the union session grid (NaN = no bar)."""
    if "futures" in _CACHE:
        return _CACHE["futures"]
    uni = futures_universe()
    markets = [a["market"] for a in uni["admitted"]]
    frames = {}
    for m in markets:
        df = pd.read_csv(R41_CURVES / ("%s_daily.csv" % m), parse_dates=["date"])
        frames[m] = df
    all_dates = sorted({d for df in frames.values()
                        for d in df["date"].dt.strftime("%Y-%m-%d")})
    dates = np.array(all_dates)
    dix = {d: i for i, d in enumerate(all_dates)}
    n_m, n_d = len(markets), len(dates)
    cols = ("ret1", "ret2", "ret3", "c1", "c2", "c3", "slope_ann", "slope23_ann",
            "oi1", "oi2", "oi3", "v1", "v2", "dte1")
    arrs = {c: np.full((n_m, n_d), np.nan) for c in cols}
    for i, m in enumerate(markets):
        df = frames[m]
        ii = np.array([dix[d] for d in df["date"].dt.strftime("%Y-%m-%d")])
        for c in cols:
            if c in df.columns:
                arrs[c][i, ii] = df[c].to_numpy(dtype=float)
    meta = {a["market"]: a for a in uni["admitted"]}
    out = {"dates": dates, "markets": markets, "meta": meta,
           "universe": uni, **arrs,
           "manifest_hash": stable_hash({"markets": markets, "d0": all_dates[0],
                                         "d1": all_dates[-1]})}
    _CACHE["futures"] = out
    return out


def load_policy_path() -> pd.DataFrame:
    """Implied policy path from ZQ (30-day fed funds) and SR3 (3-month SOFR).

    implied rate = 100 - settlement. path_6m = implied rate of the contract
    whose days-to-expiry is closest to 180 minus the front implied rate.
    Indexed by session (market observable)."""
    out = {}
    for m in ("ZQ", "SR3"):
        p = R41_CURVES / ("%s_panel.csv" % m)
        if not p.exists():
            continue
        df = pd.read_csv(p, parse_dates=["date"]).set_index("date").sort_index()
        c = np.column_stack([df["c%d" % k].to_numpy(float) for k in range(1, 9)])
        dte = np.column_stack([df["dte%d" % k].to_numpy(float) for k in range(1, 9)])
        rate = 100.0 - c
        front = rate[:, 0]
        # contract nearest 180 days out, requiring a settlement
        target = np.abs(np.where(np.isfinite(c), dte, np.nan) - 180.0)
        target = np.where(np.isfinite(target), target, np.inf)
        j = np.argmin(target, axis=1)
        far = rate[np.arange(len(df)), j]
        ok = np.isfinite(far) & np.isfinite(front) & (np.min(target, axis=1) < 120)
        out["%s_front_rate" % m] = pd.Series(np.where(ok, front, np.nan), index=df.index)
        out["%s_path_6m" % m] = pd.Series(np.where(ok, far - front, np.nan), index=df.index)
    return pd.DataFrame(out).sort_index()


# --------------------------------------------------------------------------- #
# Market-level daily series
# --------------------------------------------------------------------------- #
def load_fred_daily() -> pd.DataFrame:
    if "fred" in _CACHE:
        return _CACHE["fred"]
    if not R41_FRED_DAILY.exists():
        _CACHE["fred"] = pd.DataFrame()
        return _CACHE["fred"]
    df = pd.read_csv(R41_FRED_DAILY, index_col=0, parse_dates=True).sort_index()
    df = df.drop(columns=[c for c in ("DTWEXBGS",) if c in df.columns])
    _CACHE["fred"] = df
    return df


def load_fred_r35(series_id: str) -> pd.Series:
    """One FRED observation payload from the R35 acquisition (e.g. BAA10Y,
    the Moody's Baa minus 10-year Treasury spread, daily from 1986 - the free
    long-history credit observable; the ICE BofA OAS series in the R41 panel
    are licence-capped to a rolling three-year window)."""
    key = ("fred_r35", series_id)
    if key in _CACHE:
        return _CACHE[key]
    p = FRED_R35_DIR / ("%s.json" % series_id)
    s = pd.Series(dtype=float)
    if p.exists():
        try:
            payload = json.loads(p.read_text(encoding="utf-8"))
            dates, vals = [], []
            for rec in payload.get("observations") or []:
                v = rec.get("value")
                if v in (None, ".", ""):
                    continue
                try:
                    vals.append(float(v))
                except (TypeError, ValueError):
                    continue
                dates.append(rec.get("date"))
            s = pd.Series(vals, index=pd.to_datetime(pd.Index(dates), errors="coerce"),
                          dtype=float).dropna().sort_index()
        except (OSError, ValueError):
            s = pd.Series(dtype=float)
    _CACHE[key] = s
    return s


def load_norgate_total_return(symbols: tuple) -> dict:
    """Total-return closes for licensed local Norgate US equity symbols (used
    for the HYG / LQD credit proxy). Read only; an unavailable symbol is
    omitted, never fabricated."""
    key = ("norgate_tr", tuple(symbols))
    if key in _CACHE:
        return _CACHE[key]
    out = {}
    try:
        import logging
        logging.disable(logging.WARNING)
        import norgatedata as ng
        for s in symbols:
            try:
                df = ng.price_timeseries(
                    s, stock_price_adjustment_setting=ng.StockPriceAdjustmentType.TOTALRETURN,
                    padding_setting=ng.PaddingType.NONE, timeseriesformat="pandas-dataframe")
                if df is not None and len(df):
                    out[s] = pd.Series(df["Close"].to_numpy(dtype=float),
                                       index=pd.to_datetime(df.index)).sort_index()
            except Exception:                            # noqa: BLE001
                continue
    except Exception:                                    # noqa: BLE001
        out = {}
    _CACHE[key] = out
    return out


def load_cboe() -> pd.DataFrame:
    if "cboe" in _CACHE:
        return _CACHE["cboe"]
    out = {}
    for name in ("VIX9D", "VIX", "VIX3M", "VIX6M", "VVIX", "SKEW"):
        p = R41_CBOE_DIR / ("%s_History.csv" % name)
        if not p.exists():
            continue
        df = pd.read_csv(p)
        cols = {str(c).strip().upper(): c for c in df.columns}
        dcol, ccol = cols.get("DATE"), cols.get("CLOSE")
        if dcol is None or ccol is None:
            continue
        s = pd.Series(pd.to_numeric(df[ccol], errors="coerce").values,
                      index=pd.to_datetime(df[dcol], errors="coerce")).dropna()
        out[name] = s[s > 0].sort_index()
    _CACHE["cboe"] = pd.DataFrame(out).sort_index()
    return _CACHE["cboe"]


ALFRED_SERIES = ("UNRATE", "CPIAUCSL", "ICSA", "NFCI")


def load_alfred() -> pd.DataFrame:
    """First-release-as-known vintages: rows (series_id, observation_date,
    available_at, value). Cached under the R63 derived dir; the cache is
    reproducible from the owned normalized store and is not evidence."""
    if "alfred" in _CACHE:
        return _CACHE["alfred"]
    cache = derived_dir() / "alfred_vintages.csv"
    if cache.exists():
        df = pd.read_csv(cache, parse_dates=["observation_date", "available_at"])
        _CACHE["alfred"] = df
        return df
    rows = []
    root = INGEST_NORMALIZED / "MACRO_OBSERVATION"
    if root.exists():
        for ydir in sorted(root.iterdir()):
            if not ydir.is_dir() or not ydir.name.isdigit() or int(ydir.name) < 1990:
                continue
            for p in ydir.rglob("*.jsonl"):
                with open(p, encoding="utf-8") as fh:
                    for line in fh:
                        if '"fred_alfred"' not in line:
                            continue
                        try:
                            d = json.loads(line)
                        except ValueError:
                            continue
                        pl = d.get("normalized_payload") or {}
                        sid = pl.get("series_id")
                        if sid not in ALFRED_SERIES or not d.get("available_at"):
                            continue
                        try:
                            v = float(pl.get("value"))
                        except (TypeError, ValueError):
                            continue
                        rows.append((sid, pl.get("observation_date"),
                                     str(d["available_at"])[:10], v))
    df = pd.DataFrame(rows, columns=["series_id", "observation_date",
                                     "available_at", "value"])
    if len(df):
        df["observation_date"] = pd.to_datetime(df["observation_date"])
        df["available_at"] = pd.to_datetime(df["available_at"])
        df = df.sort_values(["series_id", "available_at", "observation_date"])
        df.to_csv(cache, index=False)
    _CACHE["alfred"] = df
    return df


def alfred_first_release(series_id: str) -> pd.Series:
    """The value of each observation as FIRST published, indexed by the
    availability date (realtime_start of the earliest vintage)."""
    df = load_alfred()
    s = df[df["series_id"] == series_id]
    if s.empty:
        return pd.Series(dtype=float)
    first = s.sort_values("available_at").groupby("observation_date").first()
    return pd.Series(first["value"].values, index=pd.to_datetime(first["available_at"])
                     ).sort_index()


def alfred_latest_known(series_id: str) -> pd.DataFrame:
    """Every vintage row, so a caller can build 'the latest value known at t'
    for a REVISED series (level features) rather than first releases."""
    df = load_alfred()
    return df[df["series_id"] == series_id].copy()


# --------------------------------------------------------------------------- #
# CFTC Commitments of Traders
# --------------------------------------------------------------------------- #
def cot_code_map() -> dict:
    """CFTC contract code -> market, from the R46 keyword-verified map."""
    from ..r46 import cftc as R46C
    out = {}
    for code, (sym, _kw, _cls) in R46C.MARKET_MAP.items():
        out[code] = sym.lstrip("&")
    return out


def load_cot() -> pd.DataFrame:
    """Weekly positioning per market indexed by REPORT date (Tuesday); the
    publication lag is applied by the feature builder through pit.as_of."""
    if "cot" in _CACHE:
        return _CACHE["cot"]
    cache = derived_dir() / "cot_by_market.csv"
    if cache.exists():
        df = pd.read_csv(cache, parse_dates=["as_of"])
        _CACHE["cot"] = df
        return df
    from ..r35 import information as R35I
    files = {}
    for p in sorted(glob.glob(str(COT_ARCHIVE_DIR / "deacot*.zip"))):
        y = Path(p).stem[len("deacot"):]
        if y.isdigit():
            files[y] = p
    codes = cot_code_map()
    res = R35I.load_cot(files, codes=list(codes)) if files else {"ok": False}
    if not res.get("ok"):
        df = pd.DataFrame(columns=["market", "as_of", "open_interest", "spec_net",
                                   "comm_net"])
        _CACHE["cot"] = df
        return df
    frame = res["frame"].copy()
    frame["market"] = frame["code"].map(codes)
    g = frame.groupby(["market", "as_of"]).agg(
        open_interest=("open_interest", "sum"), nc_long=("nc_long", "sum"),
        nc_short=("nc_short", "sum"), comm_long=("comm_long", "sum"),
        comm_short=("comm_short", "sum")).reset_index()
    g = g[g["open_interest"] > 0]
    g["spec_net"] = (g["nc_long"] - g["nc_short"]) / g["open_interest"]
    g["comm_net"] = (g["comm_long"] - g["comm_short"]) / g["open_interest"]
    df = g[["market", "as_of", "open_interest", "spec_net", "comm_net"]]
    df = df.sort_values(["market", "as_of"]).reset_index(drop=True)
    df.to_csv(cache, index=False)
    _CACHE["cot"] = df
    return df


# --------------------------------------------------------------------------- #
# EIA weekly inventories
# --------------------------------------------------------------------------- #
EIA_PET_SERIES = {
    "CRUDE_EX_SPR": "PET.WCESTUS1.W",
    "TOTAL_GASOLINE": "PET.WGTSTUS1.W",
    "DISTILLATE": "PET.WDISTUS1.W",
}
EIA_NG_SERIES = {"NG_STORAGE_L48": "NG.NW2_EPG0_SWO_R48_BCF.W"}
# market -> inventory series key it prices
EIA_MARKET_MAP = {"CL": "CRUDE_EX_SPR", "WBS": "CRUDE_EX_SPR", "BRN": "CRUDE_EX_SPR",
                  "RB": "TOTAL_GASOLINE", "HO": "DISTILLATE", "GAS": "DISTILLATE",
                  "NG": "NG_STORAGE_L48"}


def _eia_bulk_series(zip_path: Path, wanted: dict) -> dict:
    out = {}
    if not zip_path.exists():
        return out
    ids = {v: k for k, v in wanted.items()}
    try:
        z = zipfile.ZipFile(zip_path)
    except (OSError, zipfile.BadZipFile):
        return out
    with z.open(z.namelist()[0]) as fh:
        for raw in fh:
            hit = None
            for sid in ids:
                if ('"%s"' % sid).encode("ascii") in raw:
                    hit = sid
                    break
            if hit is None:
                continue
            try:
                rec = json.loads(raw)
            except ValueError:
                continue
            if rec.get("series_id") != hit:
                continue
            pts = rec.get("data") or []
            dates = [p[0] for p in pts if len(p) == 2 and p[1] is not None]
            vals = [p[1] for p in pts if len(p) == 2 and p[1] is not None]
            idx = pd.to_datetime(pd.Index(dates), format="%Y%m%d", errors="coerce")
            s = pd.Series(pd.to_numeric(pd.Series(vals), errors="coerce").values,
                          index=idx, dtype=float).dropna().sort_index()
            out[ids[hit]] = s[s.index.notna()]
            if len(out) == len(wanted):
                break
    return out


def load_eia_weekly() -> pd.DataFrame:
    """Weekly ending stocks indexed by PERIOD date (week ending); the 7-day
    publication lag is applied by the feature builder."""
    if "eia" in _CACHE:
        return _CACHE["eia"]
    cache = derived_dir() / "eia_weekly_stocks.csv"
    if cache.exists():
        df = pd.read_csv(cache, index_col=0, parse_dates=True)
        _CACHE["eia"] = df
        return df
    series = _eia_bulk_series(EIA_PET_BULK, EIA_PET_SERIES)
    ng_zip = research_root() / "_data_eia" / "NG.zip"
    series.update(_eia_bulk_series(ng_zip, EIA_NG_SERIES))
    df = pd.DataFrame(series).sort_index()
    if len(df):
        df.to_csv(cache)
    _CACHE["eia"] = df
    return df


# --------------------------------------------------------------------------- #
# SEC Form 3/4/5 insider filings (R35 archives) -> per-CIK filing-date counts
# --------------------------------------------------------------------------- #
def load_insider() -> pd.DataFrame:
    """Columns: cik, filed, direction, purchase_transactions, sale_transactions.
    Counted, never valued (R35's measured decision is inherited verbatim)."""
    if "insider" in _CACHE:
        return _CACHE["insider"]
    from ..r35 import information as R35I
    files = {}
    for p in sorted(glob.glob(str(FORM345_DIR / "*_form345.zip"))):
        files[Path(p).stem.replace("_form345", "")] = p
    res = R35I.load_insider_filings(files) if files else {"ok": False}
    if not res.get("ok"):
        df = pd.DataFrame(columns=["cik", "filed", "direction",
                                   "purchase_transactions", "sale_transactions"])
    else:
        df = res["frame"].copy()
        df["cik"] = df["cik"].astype(str).str.lstrip("0")
        df["filed"] = pd.to_datetime(df["filed"])
    _CACHE["insider"] = df
    return df


# --------------------------------------------------------------------------- #
# SEC submissions (acquired by acquire.sec_submissions)
# --------------------------------------------------------------------------- #
def submissions_dir() -> Path:
    return research_root() / "_data_sec_submissions"


def load_submissions() -> pd.DataFrame:
    """Columns: cik, form, filing_date, acceptance (UTC ISO or None).
    Reads the R63 acquisition store; empty when nothing was acquired."""
    if "submissions" in _CACHE:
        return _CACHE["submissions"]
    cache = submissions_dir() / "filings_index.csv"
    if cache.exists():
        df = pd.read_csv(cache, dtype={"cik": str}, parse_dates=["filing_date"])
        _CACHE["submissions"] = df
        return df
    df = pd.DataFrame(columns=["cik", "form", "filing_date", "acceptance"])
    _CACHE["submissions"] = df
    return df


# --------------------------------------------------------------------------- #
# Equities
# --------------------------------------------------------------------------- #
def load_equity() -> dict:
    """R57 PIT price panel + R58 PANEL-F, plus the symbol -> CIK bridge."""
    if "equity" in _CACHE:
        return _CACHE["equity"]
    from ..r58 import panel_f as PF
    from ..r58 import fundamentals as FU
    pf = PF.load()
    price = pf["price"]
    bridge = FU.cik_bridge()
    status = FU.security_status()
    syms = list(price["symbols"])
    sym2cik = {s: str(bridge[s]).lstrip("0") for s in syms if s in bridge}
    out = {"price": price, "panel_f": pf, "sym2cik": sym2cik, "status": status,
           "dates": price["dates"], "symbols": np.array(syms)}
    _CACHE["equity"] = out
    return out


# --------------------------------------------------------------------------- #
# Substrate report
# --------------------------------------------------------------------------- #
def _range(df: pd.DataFrame) -> dict:
    if df is None or len(df) == 0:
        return {"present": False}
    idx = df.index if isinstance(df.index, pd.DatetimeIndex) else None
    return {"present": True, "rows": int(len(df)),
            "first": str(idx.min())[:10] if idx is not None else None,
            "last": str(idx.max())[:10] if idx is not None else None,
            "columns": [str(c) for c in df.columns][:40]}


def substrate_report(*, include_equity: bool = True) -> dict:
    uni = futures_universe()
    rep = {
        "calculation_owner": CALCULATION_OWNER,
        "futures_store": {"path": str(R41_CURVES), "present": R41_CURVES.exists(),
                          "admitted": len(uni["admitted"]),
                          "excluded": uni["excluded"],
                          "by_asset_class": {}},
        "fred_daily": _range(load_fred_daily()),
        "cboe": _range(load_cboe()),
        "policy_path": _range(load_policy_path()),
        "eia_weekly": _range(load_eia_weekly()),
    }
    for a in uni["admitted"]:
        rep["futures_store"]["by_asset_class"].setdefault(a["asset_class"], []).append(a["market"])
    cot = load_cot()
    rep["cot"] = {"present": len(cot) > 0, "rows": int(len(cot)),
                  "markets": sorted(cot["market"].dropna().unique().tolist()) if len(cot) else [],
                  "first": str(cot["as_of"].min())[:10] if len(cot) else None,
                  "last": str(cot["as_of"].max())[:10] if len(cot) else None}
    al = load_alfred()
    rep["alfred"] = {"present": len(al) > 0, "rows": int(len(al)),
                     "series": sorted(al["series_id"].unique().tolist()) if len(al) else [],
                     "first_available": str(al["available_at"].min())[:10] if len(al) else None}
    ins = load_insider()
    rep["insider"] = {"present": len(ins) > 0, "rows": int(len(ins)),
                      "ciks": int(ins["cik"].nunique()) if len(ins) else 0,
                      "first": str(ins["filed"].min())[:10] if len(ins) else None,
                      "last": str(ins["filed"].max())[:10] if len(ins) else None}
    sub = load_submissions()
    rep["submissions"] = {"present": len(sub) > 0, "rows": int(len(sub)),
                          "ciks": int(sub["cik"].nunique()) if len(sub) else 0}
    if include_equity:
        try:
            eq = load_equity()
            rep["equity"] = {"present": True, "symbols": int(len(eq["symbols"])),
                             "sessions": int(len(eq["dates"])),
                             "first": str(eq["dates"][0]), "last": str(eq["dates"][-1]),
                             "panel_f_joined": int(eq["panel_f"]["meta"]["n_joined"]),
                             "cik_bridged": len(eq["sym2cik"])}
        except Exception as exc:                                  # noqa: BLE001
            rep["equity"] = {"present": False, "error": type(exc).__name__}
    return rep
