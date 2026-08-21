"""alpha_agent.r32.panels - the ONE Release 32 cross-asset panel owner.

Builds frozen, content-hashed daily panels from OWNED data for each sleeve, and
declares - in data, not prose - every point-in-time limitation that constrains
what a sleeve may conclude.

Three limitations are measured here rather than assumed:

* **Metadata is not availability.** ``$USTSY`` advertises 1990 and delivers data
  from 2022. Every leg's window is the window its bytes actually cover.
* **A sector index can be restated.** GICS added Real Estate on 2016-08-31 (its
  constituents lived inside Financials before that) and restructured Telecom
  into Communication Services on 2018-09-28. The vendor supplies the RESTATED
  history back to 1989, so a naive 11-sector panel from 1989 both double-counts
  Real Estate inside Financials and uses a sector definition nobody could have
  traded. Both effects are handled by admitting each sector only from the date
  its definition existed, and by reporting a conservative post-restatement view
  alongside maximum history.
* **A calendar event is not a macro forecast.** The EVENT_DRIVEN panel uses only
  DETERMINISTIC calendar structure - turn of month, quarter boundaries, triple
  witching - which is computable from the date alone and therefore carries no
  look-ahead whatsoever. It deliberately does not use owned earnings or analyst
  files, which are synthetic fixtures rather than measurements.
"""
from __future__ import annotations

import datetime as _dt
import logging
from pathlib import Path
from typing import Optional

import numpy as np

from .. import r32
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r32.panels"
PANEL_SCHEMA = "r32_cross_asset_panel_manifest/1"
ARTIFACT_NAME = "cross_asset_panel_manifest.json"

# --------------------------------------------------------------------------- #
# Instrument declarations
# --------------------------------------------------------------------------- #
#: Cash. A market observable, quoted daily since the 1960s, expressed as an
#: annualised percentage yield. Cash is a real asset choice, so it must earn a
#: real, observable return rather than be pinned at zero.
CASH_YIELD = _contract.CASH_YIELD_SYMBOL          # %IRX

#: The cross-asset trend panel. Total-return legs wherever a total-return index
#: legitimately exists; the dollar leg is a price index and is declared as one.
CROSS_ASSET_LEGS = (
    {"leg": "EQUITY_US", "symbol": "$SPXTR", "kind": "TOTAL_RETURN"},
    {"leg": "BOND_US_IG", "symbol": "$USBIG", "kind": "TOTAL_RETURN"},
    {"leg": "COMMODITY", "symbol": "$BCOMTR", "kind": "TOTAL_RETURN"},
    {"leg": "DOLLAR", "symbol": "$USDX", "kind": "PRICE_INDEX"},
)

#: GICS sector total-return indices, each with the date its DEFINITION became
#: tradable as a distinct sector.
SECTOR_LEGS = (
    {"leg": "ENERGY", "symbol": "$SPXETR", "definition_from": "1989-09-11"},
    {"leg": "MATERIALS", "symbol": "$SPXMTR", "definition_from": "1989-09-11"},
    {"leg": "INDUSTRIALS", "symbol": "$SPXITR", "definition_from": "1989-09-11"},
    {"leg": "CONSUMER_DISCRETIONARY", "symbol": "$SPXDTR",
     "definition_from": "1989-09-11"},
    {"leg": "CONSUMER_STAPLES", "symbol": "$SPXSTR",
     "definition_from": "1989-09-11"},
    {"leg": "HEALTH_CARE", "symbol": "$SPXATR", "definition_from": "1989-09-11"},
    {"leg": "FINANCIALS", "symbol": "$SPXFTR", "definition_from": "1989-09-11"},
    {"leg": "INFORMATION_TECHNOLOGY", "symbol": "$SPXTTR",
     "definition_from": "1989-09-11"},
    {"leg": "UTILITIES", "symbol": "$SPXUTR", "definition_from": "1989-09-11"},
    # Telecom Services was restructured into Communication Services on
    # 2018-09-28, absorbing constituents from IT and Consumer Discretionary.
    # The vendor's pre-2018 history is the RESTATED series.
    {"leg": "COMMUNICATION_SERVICES", "symbol": "$SPXLTR",
     "definition_from": "1989-09-11", "restated_on": "2018-09-28"},
    # Real Estate was carved out of Financials on 2016-08-31. Before that date
    # it is not a separate investable sector and admitting it double-counts.
    {"leg": "REAL_ESTATE", "symbol": "$SPXRTR", "definition_from": "2016-08-31"},
)

GICS_REAL_ESTATE_FROM = "2016-08-31"
GICS_COMMUNICATION_SERVICES_RESTATED = "2018-09-28"

#: Conservative window in which every sector definition is the one in force.
SECTOR_POST_RESTATEMENT_FROM = "2018-10-01"

#: Equity beta-timing state variables. All market observables.
BETA_TIMING_STATE = (
    {"name": "VIX", "symbol": "$VIX"},
    {"name": "VIX3M", "symbol": "$VIX3M"},
    {"name": "SKEW", "symbol": "$SKEW"},
    {"name": "YIELD_10Y", "symbol": "%TNX"},
    {"name": "YIELD_3M", "symbol": "%IRX"},
    {"name": "YIELD_2Y", "symbol": "%2YTCM"},
    {"name": "CREDIT_BAA", "symbol": "%COBAA"},
    {"name": "CREDIT_AAA", "symbol": "%COAAA"},
    {"name": "BREADTH_SPX_MA200", "symbol": "#SPX%MA200"},
    {"name": "PUTCALL_EQUITY", "symbol": "#CBOEPCE"},
)

#: Volatility / risk-regime instruments.
VOLATILITY_STATE = (
    {"name": "VIX", "symbol": "$VIX"},
    {"name": "VIX3M", "symbol": "$VIX3M"},
    {"name": "VVIX", "symbol": "$VVIX"},
    {"name": "SKEW", "symbol": "$SKEW"},
    {"name": "MOVE", "symbol": "$MOVE"},
    {"name": "VXN", "symbol": "$VXN"},
    {"name": "RVX", "symbol": "$RVX"},
)

PANEL_CROSS_ASSET = "CROSS_ASSET"
PANEL_SECTOR = "SECTOR"
PANEL_BETA_TIMING = "BETA_TIMING"
PANEL_VOLATILITY = "VOLATILITY"
PANEL_CALENDAR = "CALENDAR"
PANELS = (PANEL_CROSS_ASSET, PANEL_SECTOR, PANEL_BETA_TIMING,
          PANEL_VOLATILITY, PANEL_CALENDAR)


# --------------------------------------------------------------------------- #
# Deterministic calendar features - zero external data, zero look-ahead
# --------------------------------------------------------------------------- #
def calendar_features(dates) -> dict:
    """Deterministic calendar structure for each session. Pure function.

    Every value here is computable from the date itself, which is why this is
    the ONE event family Release 32 can study without owning event data. It is
    not a proxy for earnings or macro surprises and is never described as one.
    """
    dates = list(dates)
    n = len(dates)
    out = {
        "turn_of_month": np.zeros(n),
        "month_start": np.zeros(n),
        "month_end": np.zeros(n),
        "quarter_end": np.zeros(n),
        "triple_witching": np.zeros(n),
        "day_of_week": np.zeros(n),
        "month_of_year": np.zeros(n),
        "santa_window": np.zeros(n),
    }
    for i, d in enumerate(dates):
        dd = _as_date(d)
        nxt = _as_date(dates[i + 1]) if i + 1 < n else None
        prv = _as_date(dates[i - 1]) if i > 0 else None
        is_month_end = bool(nxt is not None and nxt.month != dd.month)
        is_month_start = bool(prv is not None and prv.month != dd.month)
        out["month_end"][i] = 1.0 if is_month_end else 0.0
        out["month_start"][i] = 1.0 if is_month_start else 0.0
        out["quarter_end"][i] = (
            1.0 if is_month_end and dd.month in (3, 6, 9, 12) else 0.0)
        # Triple witching: the third Friday of March, June, September, December.
        out["triple_witching"][i] = (
            1.0 if (dd.month in (3, 6, 9, 12) and dd.weekday() == 4
                    and 15 <= dd.day <= 21) else 0.0)
        out["day_of_week"][i] = float(dd.weekday())
        out["month_of_year"][i] = float(dd.month)
        out["santa_window"][i] = (
            1.0 if (dd.month == 12 and dd.day >= 24) or
                   (dd.month == 1 and dd.day <= 3) else 0.0)
    # Turn of month = the last 1 and first 3 sessions of a month.
    me = np.nonzero(out["month_end"])[0]
    for idx in me:
        for j in range(int(idx), min(n, int(idx) + 4)):
            out["turn_of_month"][j] = 1.0
        out["turn_of_month"][int(idx)] = 1.0
    return out


def _as_date(d):
    if isinstance(d, _dt.datetime):
        return d.date()
    if isinstance(d, _dt.date):
        return d
    return _dt.date.fromisoformat(str(d)[:10])


# --------------------------------------------------------------------------- #
# Norgate loading (read-only)
# --------------------------------------------------------------------------- #
def _norgate():
    logging.disable(logging.WARNING)
    import norgatedata as nd
    return nd


def load_series(symbol: str, *, nd=None, start: str = "1970-01-01") -> dict:
    """Load one owned daily series. Returns dates + close, or an error row."""
    nd = nd or _norgate()
    try:
        df = nd.price_timeseries(symbol, format="pandas-dataframe",
                                 start_date=start)
    except Exception as exc:  # noqa: BLE001
        return {"symbol": symbol, "available": False,
                "error": f"{type(exc).__name__}: {str(exc)[:200]}"}
    if df is None or not len(df):
        return {"symbol": symbol, "available": False, "error": "EMPTY"}
    col = "Close" if "Close" in df.columns else df.columns[-1]
    return {"symbol": symbol, "available": True,
            "dates": [str(d)[:10] for d in df.index],
            "close": np.asarray(df[col], dtype=float)}


def align(series_by_name: dict, *, require_all: bool = True) -> dict:
    """Align several series onto their COMMON date index.

    ``require_all`` expresses the common-overlap rule at the panel level: a
    cross-asset comparison that quietly drops a leg for the years it lacks is
    comparing different portfolios in different eras and calling the difference
    alpha.

    In union mode a leg is NaN on every date it did not exist. NaN means "not
    observable then" and is never forward-filled: a state variable that had not
    been invented cannot be given a value, so a configuration that needs it must
    shorten its own sample instead.
    """
    usable = {k: v for k, v in series_by_name.items() if v.get("available")}
    if not usable:
        return {"ok": False, "reason": "NO_AVAILABLE_SERIES", "dates": []}
    date_sets = [set(v["dates"]) for v in usable.values()]
    common = sorted(set.intersection(*date_sets)) if require_all else sorted(
        set.union(*date_sets))
    if not common:
        return {"ok": False, "reason": "EMPTY_COMMON_OVERLAP", "dates": []}
    cols = {}
    for name, v in usable.items():
        pos = {d: i for i, d in enumerate(v["dates"])}
        col = np.full(len(common), np.nan, dtype=float)
        for i, d in enumerate(common):
            j = pos.get(d)
            if j is not None:
                col[i] = v["close"][j]
        cols[name] = col
    missing = [k for k in series_by_name if k not in usable]
    return {"ok": True, "dates": common, "columns": cols,
            "n_dates": len(common), "missing_legs": missing,
            "first": common[0], "last": common[-1]}


def to_returns(levels: np.ndarray) -> np.ndarray:
    """Simple period returns from an index level. First element is NaN."""
    out = np.full(levels.shape, np.nan, dtype=float)
    out[1:] = (levels[1:] / levels[:-1]) - 1.0
    return out


def cash_returns(yield_pct: np.ndarray, dates) -> np.ndarray:
    """Daily cash return from an annualised bill yield, actual/365.

    The yield quoted on day t-1 is what a holder earns into day t, so the series
    is lagged by one session. Using the same day's quote would credit cash with
    a rate that was not yet observable.
    """
    n = len(yield_pct)
    out = np.zeros(n, dtype=float)
    for i in range(1, n):
        days = max(1, (_as_date(dates[i]) - _as_date(dates[i - 1])).days)
        out[i] = (float(yield_pct[i - 1]) / 100.0) * (days / 365.0)
    return out


# --------------------------------------------------------------------------- #
# Panel construction
# --------------------------------------------------------------------------- #
def build_panel(name: str, *, nd=None, start: str = "1970-01-01") -> dict:
    """Build one named panel from owned data."""
    nd = nd or _norgate()
    if name == PANEL_CROSS_ASSET:
        legs = {l["leg"]: load_series(l["symbol"], nd=nd, start=start)
                for l in CROSS_ASSET_LEGS}
        legs["CASH_YIELD"] = load_series(CASH_YIELD, nd=nd, start=start)
        aligned = align(legs)
        aligned["legs"] = [dict(l) for l in CROSS_ASSET_LEGS]
        aligned["cash_symbol"] = CASH_YIELD
        return aligned
    if name == PANEL_SECTOR:
        legs = {l["leg"]: load_series(l["symbol"], nd=nd, start=start)
                for l in SECTOR_LEGS}
        legs["CASH_YIELD"] = load_series(CASH_YIELD, nd=nd, start=start)
        legs["BENCHMARK"] = load_series(_contract.BENCHMARK_TOTAL_RETURN,
                                        nd=nd, start=start)
        # Real Estate does not exist as a sector before its GICS introduction,
        # so requiring it in the common overlap would truncate 27 years of
        # legitimate history. It is admitted by DATE, not by intersection.
        without_re = {k: v for k, v in legs.items() if k != "REAL_ESTATE"}
        aligned = align(without_re)
        aligned["legs"] = [dict(l) for l in SECTOR_LEGS]
        aligned["real_estate"] = legs.get("REAL_ESTATE", {})
        aligned["gics_real_estate_from"] = GICS_REAL_ESTATE_FROM
        aligned["gics_communication_services_restated"] = (
            GICS_COMMUNICATION_SERVICES_RESTATED)
        aligned["post_restatement_from"] = SECTOR_POST_RESTATEMENT_FROM
        return aligned
    if name in (PANEL_BETA_TIMING, PANEL_VOLATILITY):
        decl = (BETA_TIMING_STATE if name == PANEL_BETA_TIMING
                else VOLATILITY_STATE)
        legs = {d["name"]: load_series(d["symbol"], nd=nd, start=start)
                for d in decl}
        legs["BENCHMARK"] = load_series(_contract.BENCHMARK_TOTAL_RETURN,
                                        nd=nd, start=start)
        legs["CASH_YIELD"] = load_series(CASH_YIELD, nd=nd, start=start)
        # State variables have very different inception dates; requiring all of
        # them would shrink the sample to the youngest one. Each configuration
        # declares which state variables it uses, and alignment happens there.
        aligned = align(legs, require_all=False)
        aligned["declared"] = [dict(d) for d in decl]
        aligned["per_leg"] = {
            k: {"available": v.get("available", False),
                "first": (v.get("dates") or [None])[0],
                "last": (v.get("dates") or [None])[-1],
                "n": len(v.get("dates") or [])}
            for k, v in legs.items()}
        return aligned
    if name == PANEL_CALENDAR:
        legs = {"BENCHMARK": load_series(_contract.BENCHMARK_TOTAL_RETURN,
                                         nd=nd, start=start),
                "CASH_YIELD": load_series(CASH_YIELD, nd=nd, start=start)}
        aligned = align(legs)
        if aligned.get("ok"):
            aligned["calendar"] = calendar_features(aligned["dates"])
        return aligned
    raise ValueError(f"unknown panel: {name}")


def decision_dates(dates: list, *, step: int = _contract.STEP_SESSIONS,
                   min_history: int = _contract.MIN_HISTORY,
                   hold: int = _contract.HOLD_SESSIONS) -> list:
    """Indices of the decision dates, every ``step`` sessions.

    Starts only after ``min_history`` sessions so no feature is computed from a
    window that does not exist, and stops ``hold`` sessions before the end so
    every decision has a fully observed outcome.
    """
    n = len(dates)
    last = n - hold - 1
    return [i for i in range(min_history, max(min_history, last + 1), step)]


# --------------------------------------------------------------------------- #
# Hold-window returns - the units the common judge consumes
# --------------------------------------------------------------------------- #
def hold_return(levels: np.ndarray, i: int, hold: int) -> float:
    """Total return of an index level from decision date ``i`` to ``i+hold``."""
    j = i + hold
    if j >= len(levels):
        return float("nan")
    a, b = float(levels[i]), float(levels[j])
    if not np.isfinite(a) or not np.isfinite(b) or a <= 0.0:
        return float("nan")
    return (b / a) - 1.0


def hold_cash_return(yield_pct: np.ndarray, dates: list, i: int,
                     hold: int) -> float:
    """Cash earned across one hold window at the yield observable at ``i``.

    The quote used is the one at the DECISION date, which is the last yield the
    decider could see. Averaging the window's yields would use quotes from the
    future.
    """
    j = min(i + hold, len(dates) - 1)
    if i >= len(yield_pct):
        return float("nan")
    y = float(yield_pct[i])
    if not np.isfinite(y):
        return float("nan")
    days = max(1, (_as_date(dates[j]) - _as_date(dates[i])).days)
    return (y / 100.0) * (days / 365.0)


def masked_hold_return(levels: np.ndarray, mask: np.ndarray, i: int, hold: int,
                       *, cash_daily: np.ndarray) -> float:
    """Return of holding an index only on the sessions ``mask`` selects.

    This is what lets a calendar-event opinion be scored by the SAME judge as a
    monthly asset-allocation opinion: the event book is compressed into one
    hold-window return, earning cash on every session it is not invested. Without
    it, a within-month effect would be invisible at a monthly decision cadence,
    and the sleeve would have to be judged by a second, incomparable judge.
    """
    j = min(i + hold, len(levels) - 1)
    if j <= i:
        return float("nan")
    total = 1.0
    for t in range(i + 1, j + 1):
        a, b = float(levels[t - 1]), float(levels[t])
        if mask[t] and np.isfinite(a) and np.isfinite(b) and a > 0.0:
            total *= (b / a)
        else:
            c = float(cash_daily[t]) if np.isfinite(cash_daily[t]) else 0.0
            total *= (1.0 + c)
    return total - 1.0


def build_manifest(*, campaign_id: str = _contract.CAMPAIGN_ID,
                   panels: dict, created_at: Optional[str] = None) -> dict:
    """Assemble the panel manifest artifact (metadata only, never the data)."""
    summary = {}
    for name, p in panels.items():
        if not p.get("ok"):
            summary[name] = {"ok": False,
                             "reason": p.get("reason", "UNKNOWN")}
            continue
        row = {"ok": True, "n_dates": p["n_dates"], "first": p["first"],
               "last": p["last"], "missing_legs": p.get("missing_legs", []),
               "columns": sorted(p.get("columns", {}).keys())}
        if name == PANEL_SECTOR:
            row["gics_real_estate_from"] = p.get("gics_real_estate_from")
            row["gics_communication_services_restated"] = p.get(
                "gics_communication_services_restated")
            row["post_restatement_from"] = p.get("post_restatement_from")
        if name in (PANEL_BETA_TIMING, PANEL_VOLATILITY):
            row["per_leg"] = p.get("per_leg", {})
        summary[name] = row
    payload = {
        "calculation_owner": CALCULATION_OWNER,
        "campaign_id": campaign_id,
        "created_at": created_at or _dt.datetime.now().isoformat(timespec="seconds"),
        "panels": summary,
        "declared_instruments": {
            "cross_asset": [dict(l) for l in CROSS_ASSET_LEGS],
            "sector": [dict(l) for l in SECTOR_LEGS],
            "beta_timing_state": [dict(d) for d in BETA_TIMING_STATE],
            "volatility_state": [dict(d) for d in VOLATILITY_STATE],
            "cash_yield": CASH_YIELD,
            "benchmark": _contract.BENCHMARK_TOTAL_RETURN,
        },
        "point_in_time_limitations": [
            {"limitation": "VENDOR_METADATA_OVERSTATES_AVAILABILITY",
             "evidence": "$USTSY advertises a 1990 first-quoted date and "
                         "delivers data from 2022-01-03",
             "handling": "every window is measured from the delivered bytes"},
            {"limitation": "GICS_REAL_ESTATE_DID_NOT_EXIST_BEFORE_2016",
             "evidence": GICS_REAL_ESTATE_FROM,
             "handling": "Real Estate is admitted only from its GICS "
                         "introduction; before that its constituents are "
                         "inside Financials and admitting both double-counts"},
            {"limitation": "GICS_COMMUNICATION_SERVICES_RESTATED_2018",
             "evidence": GICS_COMMUNICATION_SERVICES_RESTATED,
             "handling": "the vendor supplies restated history, so a "
                         "conservative post-restatement window is reported "
                         "alongside maximum history"},
            {"limitation": "OWNED_EVENT_DATA_IS_SYNTHETIC_OR_TINY",
             "evidence": "earnings and analyst-revision stores carry "
                         "provider_id synthetic_test / PROXY_LOCAL; SEC filing "
                         "timestamps cover 63 tickers",
             "handling": "the event sleeve uses DETERMINISTIC CALENDAR "
                         "structure only and the gap is escalated to the "
                         "information purchase frontier"},
        ],
    }
    body = r32.artifact_body(PANEL_SCHEMA, payload)
    body["manifest_hash"] = r32.sha(payload)
    return body


def path_for(campaign_id: str = _contract.CAMPAIGN_ID) -> Path:
    return r32.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict) -> Path:
    return r32.write_json(path_for(body["campaign_id"]), body)
