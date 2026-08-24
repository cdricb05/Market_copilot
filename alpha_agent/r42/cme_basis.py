"""alpha_agent.r42.cme_basis - Track K: is the premium visible off-venue?

The perpetual funding premium was found on ONE offshore venue whose own
trading API refuses the operator's location. The structural question is
therefore not "does Binance pay funding" but:

    does crypto derivative demand create a broader basis/carry premium
    OUTSIDE Binance perpetuals?

CME lists regulated, dated, cash-settled Bitcoin and Ether futures. If the
same demand exists, it must show up as a positive annualised spot-futures
basis there too - and that basis is harvestable by a US-admissible entity,
which the perpetual is not.

Data: the estate's OWNED Norgate entitlement (113 dated BTC contracts from
2018, 76 dated ETH from 2021) and exact dated contracts only. Spot is
marked from the Binance 1-minute archive at the CME settlement minute so
the basis is not a stale-mark artefact.
"""
from __future__ import annotations

import datetime as _dt

import numpy as np
import pandas as pd

from . import CAMPAIGN_ID, artifact_body, sha, write_artifact
from . import capital as CAP
from . import contract as C
from . import pnl_audit as PA
from ..r41 import crypto_lab as CRL
from ..r41 import evidence as EV

CALCULATION_OWNER = "alpha_agent.r42.cme_basis"
ARTIFACT = "REGULATED_MARKET_REPLICATION.json"

#: CME crypto futures settle at 15:00 America/Chicago. The corresponding
#: UTC minute is 20:00 in CDT and 21:00 in CST; both are probed and the
#: nearer available minute is used, with the choice recorded.
CME_SETTLE_UTC_CANDIDATES = (20, 21)
MONTH_CODES = {"F": 1, "G": 2, "H": 3, "J": 4, "K": 5, "M": 6, "N": 7,
               "Q": 8, "U": 9, "V": 10, "X": 11, "Z": 12}


def _nd():
    import norgatedata
    return norgatedata


def dated_contracts(root: str) -> list:
    nd = _nd()
    syms = [s for s in nd.database_symbols("Futures")
            if s.startswith("%s-" % root)]
    return sorted(syms)


def contract_frame(symbol: str) -> pd.DataFrame:
    nd = _nd()
    df = nd.price_timeseries(symbol, format="pandas-dataframe",
                             timeseriesformat="pandas-dataframe")
    if df is None or not len(df):
        return pd.DataFrame()
    df.index = pd.to_datetime(df.index, utc=True)
    return df


def expiry_of(symbol: str) -> pd.Timestamp:
    nd = _nd()
    try:
        d = nd.last_quoted_date(symbol)
        if d:
            return pd.Timestamp(d, tz="UTC")
    except Exception:
        pass
    root, tail = symbol.split("-")
    year, code = int(tail[:4]), tail[4]
    month = MONTH_CODES.get(code, 12)
    return pd.Timestamp(year=year, month=month, day=28, tz="UTC")


def spot_at_settlement(index: pd.DatetimeIndex,
                       symbol: str = "BTCUSDT") -> pd.Series:
    """Binance spot marked at the CME settlement minute of each session."""
    m = CRL.load_minute(symbol, "spot")["close"]
    m.index = pd.to_datetime(m.index, utc=True)
    m = m[~m.index.duplicated(keep="last")].sort_index()
    best, best_n = None, -1
    for hour in CME_SETTLE_UTC_CANDIDATES:
        want = pd.DatetimeIndex([d.normalize() + pd.Timedelta(hours=hour)
                                 for d in index])
        got = m.reindex(m.index.union(want)).ffill().reindex(want)
        n = int(got.notna().sum())
        if n > best_n:
            best, best_n, chosen = got, n, hour
    out = pd.Series(best.to_numpy(), index=index)
    out.attrs["settle_hour_utc"] = chosen
    return out


def basis_panel(root: str, spot_symbol: str) -> pd.DataFrame:
    """Annualised spot/dated-futures basis, per session, front contract."""
    rows = []
    for sym in dated_contracts(root):
        df = contract_frame(sym)
        if not len(df) or "Close" not in df.columns:
            continue
        exp = expiry_of(sym)
        d = df[["Close", "Volume", "Open Interest"]].copy()
        d.columns = ["fut", "volume", "oi"]
        d["contract"] = sym
        d["expiry"] = exp
        d["dte"] = (exp - d.index).days
        d = d[(d["dte"] > 3) & (d["dte"] <= 400)]
        rows.append(d)
    if not rows:
        return pd.DataFrame()
    allc = pd.concat(rows)
    allc = allc[allc["fut"].notna() & (allc["fut"] > 0)]
    # front contract = smallest positive dte on each session
    allc = allc.sort_values(["dte"])
    front = allc[~allc.index.duplicated(keep="first")].sort_index()
    second = allc[allc.index.duplicated(keep="first")]
    second = second[~second.index.duplicated(keep="first")].sort_index()
    spot = spot_at_settlement(front.index, spot_symbol)
    front["spot"] = spot.to_numpy()
    front = front[front["spot"].notna()]
    front["basis_raw"] = front["fut"] / front["spot"] - 1.0
    front["basis_ann"] = front["basis_raw"] * 365.0 / front["dte"].clip(
        lower=1)
    front["second_fut"] = second["fut"].reindex(front.index)
    front["second_dte"] = second["dte"].reindex(front.index)
    gap = (front["second_dte"] - front["dte"]).clip(lower=1)
    front["calendar_ann"] = (front["second_fut"] / front["fut"]
                             - 1.0) * 365.0 / gap
    front.attrs["settle_hour_utc"] = spot.attrs.get("settle_hour_utc")
    return front


#: Round-trip execution for one CME cash-and-carry leg pair, in bps of
#: notional: spot purchase + futures sale + the eventual unwind. Declared
#: here rather than in the frozen contract, because the frozen contract
#: was sealed before this track existed and may not be edited.
COST_ROUND_TRIP_CME_BPS = 12.0

#: A regulated FCM pays interest on cash margin, and a US-admissible
#: entity holds its spot inventory at a regulated custodian. Only the SPOT
#: leg's cash is genuinely immobilised in a non-interest-bearing asset.
#: This is the FAIREST possible treatment of the regulated route and is
#: reported beside the conservative crypto-venue treatment so the
#: comparison cannot be accused of stacking the deck.
FAIR_REGULATED_CAPITAL = 1.0


def carry_book(panel: pd.DataFrame, *, capital: float = None,
               margin_earns_rf: bool = False) -> pd.DataFrame:
    """Cash-and-carry: buy spot, sell the dated future, hold to expiry.

    The realised return of that trade, annualised, IS the annualised basis
    at entry, less costs, on the committed capital. Modelled here as a
    daily-entered book so it is comparable with the perpetual candidate.
    """
    K = float(capital if capital is not None
              else C.CAPITAL_MODELS[C.PRIMARY_CAPITAL_MODEL]["denominator"])
    idx = panel.index
    rf = CAP.risk_free_daily(idx).fillna(0.0)
    # cost of establishing the round trip once, amortised over the hold
    amort = (COST_ROUND_TRIP_CME_BPS / 1e4) * 365.0 \
        / panel["dte"].clip(lower=1)
    roc_ann = (panel["basis_ann"] - amort) / K
    # If margin earns interest, only the spot leg's cash forgoes the
    # risk-free rate, so the benchmark is charged on 1.0, not on K.
    bench_mult = 1.0 if margin_earns_rf else K
    bench_ann = rf * 365.0 * bench_mult / K
    return pd.DataFrame({
        "basis_ann": panel["basis_ann"],
        "dte": panel["dte"],
        "roll_cost_ann": amort,
        "roc_ann": roc_ann,
        "rf_ann": bench_ann,
        "excess_ann": roc_ann - bench_ann,
        "daily_roc": roc_ann / 365.0,
        "daily_rf": bench_ann / 365.0,
    }, index=idx)


def summarise(book: pd.DataFrame, label: str,
              window: tuple = None) -> dict:
    b = book if window is None else book.loc[str(window[0]):str(window[1])]
    b = b.dropna(subset=["excess_ann"])
    if len(b) < 60:
        return {"label": label, "state": "TOO_SHORT", "n": int(len(b))}
    card = EV.scorecard(b["daily_roc"].to_numpy(), np.zeros(len(b)),
                        b["daily_rf"].to_numpy(),
                        periods_per_year=PA.R41_PPY, overlap=1)
    return {
        "label": label, "state": "OK", "n_sessions": int(len(b)),
        "first": str(b.index.min().date()), "last": str(b.index.max().date()),
        "mean_annualised_basis": float(b["basis_ann"].mean()),
        "median_annualised_basis": float(b["basis_ann"].median()),
        "share_sessions_basis_positive": float((b["basis_ann"] > 0).mean()),
        "mean_roc_ann": float(b["roc_ann"].mean()),
        "mean_rf_ann": float(b["rf_ann"].mean()),
        "mean_excess_ann": float(b["excess_ann"].mean()),
        "share_sessions_excess_positive": float((b["excess_ann"] > 0).mean()),
        "excess_t_hac": card.get("excess_t_hac"),
        "mean_dte": float(b["dte"].mean()),
    }


def run() -> dict:
    out = {}
    for root, spot in (("BTC", "BTCUSDT"), ("ETH", "ETHUSDT")):
        try:
            panel = basis_panel(root, spot)
        except Exception as exc:
            out[root] = {"state": "ERROR",
                         "error": "%s: %s" % (type(exc).__name__, exc)}
            continue
        if not len(panel):
            out[root] = {"state": "NO_DATA"}
            continue
        book = carry_book(panel)
        fair = carry_book(panel, capital=FAIR_REGULATED_CAPITAL,
                          margin_earns_rf=True)
        out[root] = {
            "state": "OK",
            "n_dated_contracts": len(dated_contracts(root)),
            "settle_hour_utc": panel.attrs.get("settle_hour_utc"),
            "roll_cost_round_trip_bps": COST_ROUND_TRIP_CME_BPS,
            "mean_roll_cost_ann": float(book["roll_cost_ann"].mean()),
            "full_history": summarise(book, "%s_FULL" % root),
            "recent_window": summarise(
                book, "%s_RECENT" % root,
                window=("2025-04-14", "2026-07-31")),
            "zone_b_window": summarise(
                book, "%s_ZONE_B" % root,
                window=("2023-04-24", "2025-04-06")),
            "FAIR_REGULATED_full_history": summarise(
                fair, "%s_FAIR_FULL" % root),
            "FAIR_REGULATED_recent_window": summarise(
                fair, "%s_FAIR_RECENT" % root,
                window=("2025-04-14", "2026-07-31")),
            "fair_regulated_note":
                "capital = spot notional only; FCM cash margin earns "
                "interest. This is the most favourable defensible "
                "treatment of the regulated route.",
            "calendar_basis_mean_ann": float(
                panel["calendar_ann"].dropna().mean())
            if panel["calendar_ann"].notna().any() else None,
        }
    body = artifact_body("r42_regulated_market_replication/1", {
        "calculation_owner": CALCULATION_OWNER,
        "track": "K - regulated-market (CME) basis replication",
        "design": C.CME_REPLICATION,
        "is_not_a_perp_funding_clone":
            C.CME_REPLICATION["is_not_a_perp_funding_clone"],
        # NOT "results": the canonical artifact reader unwraps a top-level
        # "results" key, which would hide this payload behind it.
        "markets": out,
        "why_this_matters": "a CME basis is harvestable through a regulated "
                            "US futures account. If the premium exists "
                            "there, single-venue dependence is broken; if "
                            "it does not, the perpetual premium is a "
                            "property of offshore leverage demand, not of "
                            "crypto carry as such.",
        "verdict": _verdict(out),
        "generated_at": _dt.datetime.now(_dt.timezone.utc)
        .strftime("%Y-%m-%dT%H:%M:%SZ"),
    })
    body["regulated_market_replication_hash"] = sha(body)
    write_artifact(ARTIFACT, body, CAMPAIGN_ID, overwrite=True)
    return body


def _verdict(out: dict) -> dict:
    btc = (out.get("BTC") or {})
    rec = btc.get("recent_window") or {}
    full = btc.get("full_history") or {}
    fair_rec = btc.get("FAIR_REGULATED_recent_window") or {}
    fair_full = btc.get("FAIR_REGULATED_full_history") or {}
    present = (full.get("mean_annualised_basis") or 0) > 0
    beats = (rec.get("mean_excess_ann") or 0) > 0 \
        and (rec.get("excess_t_hac") or 0) >= 2.0
    fair_beats = (fair_rec.get("mean_excess_ann") or 0) > 0 \
        and (fair_rec.get("excess_t_hac") or 0) >= 2.0
    return {
        "fair_regulated_recent_excess_ann": fair_rec.get("mean_excess_ann"),
        "fair_regulated_recent_excess_t": fair_rec.get("excess_t_hac"),
        "fair_regulated_full_excess_ann": fair_full.get("mean_excess_ann"),
        "fair_regulated_beats_cash": fair_beats,
        "mean_roll_cost_ann": btc.get("mean_roll_cost_ann"),
        "decisive_arithmetic":
            "the CME basis and the roll cost are the same order of "
            "magnitude: a ~%.1f%%/yr basis over a ~%.0f-day contract is "
            "harvested through a round trip that costs ~%.1f%%/yr to keep "
            "rolling, against a risk-free rate of ~%.1f%%/yr."
            % ((rec.get("mean_annualised_basis") or 0) * 100,
               rec.get("mean_dte") or 0,
               (btc.get("mean_roll_cost_ann") or 0) * 100,
               (rec.get("mean_rf_ann") or 0) * 100),
        "state": ("REGULATED_PREMIUM_PRESENT_AND_BEATS_CASH" if beats
                  else ("REGULATED_PREMIUM_PRESENT_BUT_DOES_NOT_BEAT_CASH"
                        if present else "NO_REGULATED_PREMIUM")),
        "structural_premium_visible_off_binance": present,
        "beats_cash_in_recent_window": beats,
        "btc_full_mean_basis_ann": full.get("mean_annualised_basis"),
        "btc_full_excess_ann": full.get("mean_excess_ann"),
        "btc_recent_mean_basis_ann": rec.get("mean_annualised_basis"),
        "btc_recent_excess_ann": rec.get("mean_excess_ann"),
        "btc_recent_excess_t": rec.get("excess_t_hac"),
    }
