"""alpha_agent.r39.info_expansion - Track E: consume the owned information
Release 39 v1 left unused.

Four families, each PIT-aligned with a DECLARED availability lag, joined to
the frozen universal state as features, and judged ONLY by their paired
increment over the same observations (standalone significance does not
count):

* NYFED_PRIMARY_DEALER_POSITIONING - weekly net outright dealer positions
  in US government securities (current-format ``PDPOSGS*`` codes summed,
  change-variant twins excluded), z-scored on trailing windows, joined to
  RATES futures with a 9-calendar-day publication lag.
* EIA_PHYSICAL_SUPPLY_DEMAND - weekly US petroleum stocks (crude ex-SPR,
  total crude, gasoline, distillate) from the owned PET bulk, z-scored
  levels vs their own trailing seasonal and 4-week changes, joined to the
  ENERGY economic group with a 9-calendar-day lag.
* SEC_INSIDER_TRANSACTIONS - Form 3/4/5 open-market purchases (P) and
  sales (S) aggregated by FILING_DATE (the PIT date): a market-level
  net-buy breadth z for equity-index timing, and a per-name 6-month
  net-activity feature joined to the R30 equity panel through the Norgate
  assetid -> symbol identity (match rate measured and recorded).
* SEC_COMPANYFACTS_FUNDAMENTALS (direct) - the R30 fundamental-matched
  dataset's 7 PIT fundamental columns, paired against the price-only
  features on IDENTICAL rows.

Structural constraint, recorded rather than hidden: these families begin
in 1982 (EIA), 1998 (NYFed), 2008 (insider), ~2010 (fundamentals). Where a
family has no ZONE_A coverage, a fitted model cannot learn its weights
point-in-time from the discovery zone, so the paired fitted test runs on a
DECLARED Zone-B sub-split (fit <= 2012-12-31, evaluate after), all
evaluations ledger-counted, and Zone C is never touched.
"""
from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

from .. import r39
from .estate import DATA_ROOT, INSIDER_DIR, NYFED_CSV

CALCULATION_OWNER = "alpha_agent.r39.info_expansion"

EIA_ZIP = DATA_ROOT / "orthogonal_information_r35" / "acquired" / \
    "eia_petroleum_bulk" / "PET.zip"

#: Declared availability lags (calendar days after the observation stamp).
NYFED_LAG_DAYS = 9      # published the Thursday of the following week
EIA_LAG_DAYS = 9        # weekly report published the following Wednesday
INSIDER_LAG_DAYS = 1    # FILING_DATE is itself the publication date

#: Declared EIA series (weekly US stocks).
EIA_SERIES = {
    "PET.WCESTUS1.W": "eia_crude_exspr",
    "PET.WCRSTUS1.W": "eia_crude_total",
    "PET.WGTSTUS1.W": "eia_gasoline",
    "PET.WDISTUS1.W": "eia_distillate",
}

#: The Zone-B sub-split for families without Zone-A coverage (declared
#: BEFORE any outcome was computed).
SUBSPLIT_FIT_END = "2012-12-31"

NYFED_FEATURES = ("nyfed_net_ust_z", "nyfed_net_ust_chg13_z")
EIA_FEATURES = ("eia_crude_z", "eia_gasoline_z", "eia_distillate_z",
                "eia_crude_chg4_z")
INSIDER_MARKET_FEATURES = ("insider_net_breadth_z",)
INSIDER_NAME_FEATURE = "insider_net_6m_z"


def _roll_z(s: pd.Series, window: int, min_periods: int) -> pd.Series:
    m = s.rolling(window, min_periods=min_periods).mean()
    sd = s.rolling(window, min_periods=min_periods).std()
    return (s - m) / sd.replace(0.0, np.nan)


# --------------------------------------------------------------------------- #
# NY Fed primary-dealer positioning
# --------------------------------------------------------------------------- #
def load_nyfed() -> tuple:
    """(weekly feature frame indexed by AVAILABLE date, provenance dict)."""
    ny = pd.read_csv(NYFED_CSV)
    ny.columns = ["as_of", "series", "value"]
    ny["as_of"] = pd.to_datetime(ny["as_of"])
    ny["value"] = pd.to_numeric(ny["value"], errors="coerce")
    codes = [c for c in ny["series"].unique() if str(c).startswith("PDPOSGS")]
    # drop the change-variant twin of any code (trailing 'C' with the same
    # stem also present), so levels are never mixed with weekly changes
    keep = [c for c in codes
            if not (c.endswith("C") and c[:-1] in set(codes))]
    sub = ny[ny["series"].isin(keep)]
    weekly = sub.groupby("as_of")["value"].sum(min_count=1).sort_index()
    feats = pd.DataFrame({
        "nyfed_net_ust_z": _roll_z(weekly, 156, 52),
        "nyfed_net_ust_chg13_z": _roll_z(weekly.diff(13), 156, 52)})
    feats.index = feats.index + pd.Timedelta(days=NYFED_LAG_DAYS)
    prov = {"source": str(NYFED_CSV),
            "series_summed": sorted(keep),
            "n_series_summed": len(keep),
            "coverage": [str(weekly.index.min().date()),
                         str(weekly.index.max().date())],
            "lag_days": NYFED_LAG_DAYS,
            "note": "legacy pre-2013 mnemonics are NOT decoded here; "
                    "feature coverage is the summed current-format span, "
                    "measured and recorded, never back-filled"}
    return feats, prov


# --------------------------------------------------------------------------- #
# EIA petroleum physical balances
# --------------------------------------------------------------------------- #
def load_eia() -> tuple:
    found = {}
    with zipfile.ZipFile(EIA_ZIP) as z:
        name = z.namelist()[0]
        with z.open(name) as fh:
            for line in io.TextIOWrapper(fh, encoding="utf-8"):
                if len(found) == len(EIA_SERIES):
                    break
                if '"series_id"' not in line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                sid = obj.get("series_id")
                if sid in EIA_SERIES and sid not in found:
                    d = pd.DataFrame(obj["data"], columns=["date", "value"])
                    d["date"] = pd.to_datetime(d["date"], format="%Y%m%d")
                    d["value"] = pd.to_numeric(d["value"], errors="coerce")
                    found[sid] = d.set_index("date")["value"].sort_index()
    frame = pd.DataFrame({EIA_SERIES[k]: v for k, v in found.items()})
    feats = pd.DataFrame({
        "eia_crude_z": _roll_z(frame["eia_crude_exspr"], 260, 104),
        "eia_gasoline_z": _roll_z(frame["eia_gasoline"], 260, 104),
        "eia_distillate_z": _roll_z(frame["eia_distillate"], 260, 104),
        "eia_crude_chg4_z": _roll_z(frame["eia_crude_exspr"].diff(4),
                                    156, 52)})
    feats.index = feats.index + pd.Timedelta(days=EIA_LAG_DAYS)
    prov = {"source": str(EIA_ZIP),
            "series": {k: v for k, v in EIA_SERIES.items()},
            "coverage": [str(frame.index.min().date()),
                         str(frame.index.max().date())],
            "lag_days": EIA_LAG_DAYS}
    return feats, prov


# --------------------------------------------------------------------------- #
# SEC insider transactions
# --------------------------------------------------------------------------- #
def _insider_cache_path(campaign_id: str) -> Path:
    return r39.campaign_dir(campaign_id) / "insider_monthly_cache.csv"


def load_insider(campaign_id: str) -> tuple:
    """(per-(symbol, filing month) net activity frame, provenance).

    Parsed once from the 73 quarterly archives and cached; the cache is a
    derived research artifact under the continuation campaign directory.
    """
    cache = _insider_cache_path(campaign_id)
    if cache.exists():
        agg = pd.read_csv(cache, parse_dates=["month"],
                          dtype={"symbol": str})
        agg["symbol"] = agg["symbol"].astype(str)
        prov = {"source": str(INSIDER_DIR), "cache": str(cache),
                "cache_hit": True}
        return agg, prov
    frames = []
    zips = sorted(INSIDER_DIR.glob("*.zip"))
    for zp in zips:
        with zipfile.ZipFile(zp) as z:
            with z.open("SUBMISSION.tsv") as fh:
                sub = pd.read_csv(
                    io.TextIOWrapper(fh, encoding="utf-8", errors="replace"),
                    sep="\t", usecols=["ACCESSION_NUMBER", "FILING_DATE",
                                       "ISSUERTRADINGSYMBOL"],
                    low_memory=False)
            with z.open("NONDERIV_TRANS.tsv") as fh:
                tr = pd.read_csv(
                    io.TextIOWrapper(fh, encoding="utf-8", errors="replace"),
                    sep="\t", usecols=["ACCESSION_NUMBER", "TRANS_CODE",
                                       "TRANS_SHARES",
                                       "TRANS_PRICEPERSHARE"],
                    low_memory=False)
        tr = tr[tr["TRANS_CODE"].isin(["P", "S"])]
        if tr.empty:
            continue
        tr["TRANS_SHARES"] = pd.to_numeric(tr["TRANS_SHARES"],
                                           errors="coerce")
        tr["TRANS_PRICEPERSHARE"] = pd.to_numeric(
            tr["TRANS_PRICEPERSHARE"], errors="coerce")
        tr["dollars"] = (tr["TRANS_SHARES"] * tr["TRANS_PRICEPERSHARE"]) \
            .clip(lower=0.0)
        m = tr.merge(sub, on="ACCESSION_NUMBER", how="left")
        m["FILING_DATE"] = pd.to_datetime(m["FILING_DATE"], errors="coerce")
        m = m.dropna(subset=["FILING_DATE"])
        m["month"] = m["FILING_DATE"].dt.to_period("M").dt.to_timestamp("M")
        m["symbol"] = m["ISSUERTRADINGSYMBOL"].astype(str).str.upper() \
            .str.strip()
        g = m.groupby(["symbol", "month", "TRANS_CODE"]).agg(
            n=("ACCESSION_NUMBER", "nunique"),
            dollars=("dollars", "sum")).reset_index()
        frames.append(g)
    allg = pd.concat(frames, ignore_index=True)
    wide = allg.pivot_table(index=["symbol", "month"],
                            columns="TRANS_CODE",
                            values=["n", "dollars"], aggfunc="sum",
                            fill_value=0.0)
    wide.columns = ["%s_%s" % (a, b) for a, b in wide.columns]
    agg = wide.reset_index()
    for col in ("n_P", "n_S", "dollars_P", "dollars_S"):
        if col not in agg.columns:
            agg[col] = 0.0
    agg.to_csv(cache, index=False)
    prov = {"source": str(INSIDER_DIR), "n_archives": len(zips),
            "cache": str(cache), "cache_hit": False,
            "pit_basis": "FILING_DATE (publication date), never "
                         "TRANS_DATE"}
    return agg, prov


def insider_market_series(agg: pd.DataFrame) -> pd.Series:
    """Market-wide net-buy breadth z by filing month (PIT: available at the
    month's end plus one day)."""
    m = agg.groupby("month")[["n_P", "n_S"]].sum()
    breadth = (m["n_P"] - m["n_S"]) / (m["n_P"] + m["n_S"]).replace(0, np.nan)
    z = _roll_z(breadth, 36, 18)
    z.index = pd.DatetimeIndex(z.index) + pd.Timedelta(days=INSIDER_LAG_DAYS)
    return z.rename("insider_net_breadth_z")


PHASE24_PANEL = Path(r"D:\Stock_Prediction_app_data\phase24_cache"
                     r"\daily_panel\russell1000_cp_daily.npz")


def eq_symbol_bridge() -> tuple:
    """(per-base-symbol bearer lists, sectors-by-position).

    The R30 ``security_id`` is the POSITION into the phase-24 panel's
    ``symbols`` axis. Delisted names carry a ``-YYYYMM`` suffix, so one
    base ticker can have several bearers over time; the bearer of a filing
    month is the one whose delisting month is the earliest at or after
    that month (the currently-listed name is the final bearer).
    """
    z = np.load(PHASE24_PANEL, allow_pickle=True)
    symbols = [str(s) for s in z["symbols"]]
    sectors = [str(s) for s in z["sectors"]] if "sectors" in z.files \
        else [""] * len(symbols)
    bearers: dict = {}
    for pos, sym in enumerate(symbols):
        if "-" in sym:
            base, _, suffix = sym.rpartition("-")
            if suffix.isdigit() and len(suffix) == 6:
                end = int(suffix)
            else:
                base, end = sym, 999912
        else:
            base, end = sym, 999912
        bearers.setdefault(base.upper(), []).append((end, pos))
    for base in bearers:
        bearers[base].sort()
    return bearers, sectors


def _bearer_position(bearers: dict, symbol: str, month: pd.Timestamp):
    lst = bearers.get(symbol)
    if not lst:
        return None
    ym = month.year * 100 + month.month
    for end, pos in lst:
        if end >= ym:
            return pos
    return None


def insider_name_feature(agg: pd.DataFrame, eq: pd.DataFrame) -> tuple:
    """Join per-name insider activity to the R30 equity panel.

    Identity: R30 security_id is the phase-24 panel POSITION; insider
    ticker symbols map through the panel's own symbols axis with
    time-aware bearer resolution (delisting suffixes). The measured match
    statistics are returned; unmatched rows keep NaN (mask, never fill).
    """
    bearers, _sectors = eq_symbol_bridge()
    syms = sorted({str(s) for s in agg["symbol"].unique()
                   if str(s) not in ("", "NONE", "NAN", "N/A", "nan")
                   and len(str(s)) <= 6 and str(s).isalnum()})
    matched = {s for s in syms if s in bearers}
    sym_rate = len(matched) / max(len(syms), 1)
    agg2 = agg[agg["symbol"].isin(matched)].copy()
    agg2["security_id"] = [
        _bearer_position(bearers, s, m)
        for s, m in zip(agg2["symbol"], agg2["month"])]
    agg2 = agg2.dropna(subset=["security_id"])
    agg2["security_id"] = agg2["security_id"].astype(np.int64)
    agg2["net_n"] = agg2["n_P"] - agg2["n_S"]
    # trailing 6-month net activity per name, stamped at month end
    piv = agg2.pivot_table(index="month", columns="security_id",
                           values="net_n", aggfunc="sum").sort_index()
    piv6 = piv.rolling(6, min_periods=1).sum()
    longf = piv6.stack().rename("insider_net_6m").reset_index()
    longf.columns = ["month", "security_id", "insider_net_6m"]
    longf["month"] = pd.DatetimeIndex(longf["month"]) \
        + pd.Timedelta(days=INSIDER_LAG_DAYS)

    eq = eq.copy()
    eq["_month"] = eq["decision_date"].dt.to_period("M")
    longf["_month"] = (longf["month"] - pd.Timedelta(days=INSIDER_LAG_DAYS)
                       ).dt.to_period("M")
    eq = eq.merge(longf[["_month", "security_id", "insider_net_6m"]],
                  on=["_month", "security_id"], how="left")
    eq = eq.drop(columns=["_month"])
    # names with a mapped symbol but no filings genuinely had no P/S
    # activity; distinguish covered-zero from unmapped-unknown
    mapped_ids = {pos for s in matched for _end, pos in bearers[s]}
    covered = eq["security_id"].isin(mapped_ids)
    eq.loc[covered & eq["insider_net_6m"].isna(), "insider_net_6m"] = 0.0
    # cross-sectional z within date over covered names
    def _xz(s):
        sd = s.std(ddof=1)
        return (s - s.mean()) / sd if sd and np.isfinite(sd) and sd > 0 \
            else s * np.nan
    eq[INSIDER_NAME_FEATURE] = eq.groupby("decision_date")[
        "insider_net_6m"].transform(_xz)
    panel_ids = set(eq["security_id"].unique())
    stats = {
        "identity_bridge": "phase-24 panel symbols axis with time-aware "
                           "delisting-suffix bearer resolution",
        "insider_symbols_considered": len(syms),
        "symbols_matched_to_panel": len(matched),
        "symbol_match_rate": round(sym_rate, 4),
        "eq_panel_ids": len(panel_ids),
        "eq_panel_ids_covered": len(panel_ids & mapped_ids),
        "eq_panel_coverage_rate": round(
            len(panel_ids & mapped_ids) / max(len(panel_ids), 1), 4),
    }
    return eq, stats


def attach_eq_sector(eq: pd.DataFrame) -> pd.DataFrame:
    """Sector by panel position, from the phase-24 panel's own sector
    axis - the column the earlier blocker claim said did not exist."""
    _bearers, sectors = eq_symbol_bridge()
    n = len(sectors)
    eq = eq.copy()
    eq["sector"] = [sectors[i] if 0 <= i < n else ""
                    for i in eq["security_id"].astype(int)]
    return eq


# --------------------------------------------------------------------------- #
# Direct PIT fundamentals (R30 fundamental-matched dataset)
# --------------------------------------------------------------------------- #
FUND_COLS = ("s25_operating_profitability", "gross_profitability",
             "fcf_to_assets", "operating_accruals", "asset_growth",
             "roe", "leverage")


def load_fundamental_panel() -> pd.DataFrame:
    from .estate import R30_FUND_DATASET
    z = np.load(R30_FUND_DATASET, allow_pickle=True)
    meta = json.loads(bytes(z["meta"]).decode("utf-8"))
    feats = list(meta["feature_names"])
    dates = [pd.Timestamp(s) for s in meta["dates"]]
    frames = []
    for i, dt in enumerate(dates):
        key = "X_%d" % i
        if key not in z:
            continue
        df = pd.DataFrame(z[key], columns=feats)
        df["security_id"] = z["cols_%d" % i].astype(np.int64)
        df["decision_date"] = dt
        y21 = z["y_%d_21" % i]
        tr21 = z["tr_%d_21" % i]
        df["fwd_21"] = np.where(tr21.astype(bool), np.nan, y21)
        frames.append(df)
    eqf = pd.concat(frames, ignore_index=True)
    eqf["lane"] = "EQ"
    eqf["asset_class"] = "US_EQUITY"
    eqf["economic_group"] = "US_SINGLE_NAME_FUND"
    eqf["cost_bps_per_side"] = 10.0
    eqf["control_fwd_21"] = eqf.groupby("decision_date")["fwd_21"] \
        .transform("mean")
    from .target_factory import materialise
    from .universal_state import zone_of
    eqf["zone"] = zone_of(eqf["decision_date"])
    return materialise(eqf, scope_col="economic_group")


# --------------------------------------------------------------------------- #
# Feature joins to the futures panel
# --------------------------------------------------------------------------- #
def join_weekly_to_fut(fut: pd.DataFrame, feats: pd.DataFrame,
                       cols: tuple) -> pd.DataFrame:
    """As-of join of AVAILABLE-dated weekly features to decision dates."""
    f = feats.reset_index()
    f.columns = ["available"] + list(feats.columns)
    f["available"] = pd.to_datetime(f["available"]).astype("datetime64[ns]")
    f = f.sort_values("available")
    fut = fut.copy()
    fut["decision_date"] = pd.to_datetime(fut["decision_date"]) \
        .astype("datetime64[ns]")
    merged = pd.merge_asof(
        fut.sort_values("decision_date"), f,
        left_on="decision_date", right_on="available",
        direction="backward", tolerance=pd.Timedelta(days=40))
    merged = merged.drop(columns=["available"])
    for c in cols:
        if c not in merged.columns:
            merged[c] = np.nan
    return merged.sort_values(["decision_date", "market_id"]) \
        .reset_index(drop=True)


def join_monthly_to_fut(fut: pd.DataFrame, series: pd.Series) -> pd.DataFrame:
    f = series.reset_index()
    f.columns = ["available", series.name]
    f["available"] = pd.to_datetime(f["available"]).astype("datetime64[ns]")
    fut = fut.copy()
    fut["decision_date"] = pd.to_datetime(fut["decision_date"]) \
        .astype("datetime64[ns]")
    merged = pd.merge_asof(
        fut.sort_values("decision_date"), f.sort_values("available"),
        left_on="decision_date", right_on="available",
        direction="backward", tolerance=pd.Timedelta(days=62))
    return merged.drop(columns=["available"]).sort_values(
        ["decision_date", "market_id"]).reset_index(drop=True)
