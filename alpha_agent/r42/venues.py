"""alpha_agent.r42.venues - Tracks H and J: venues, and what a venue is for.

R41 is single-venue Binance research. Two separate questions follow, and
they are never conflated here:

* Track H INVESTABILITY - is there a legally admissible account path for
  THIS operator at THIS venue? Research may use a venue's public data;
  that is not permission to trade there.
* Track J REPLICATION - does the same economic hypothesis appear on
  venues other than the one it was found on?

The venue universe is frozen from DATA-ONLY criteria in
:data:`contract.VENUE_ELIGIBILITY` before any venue's strategy outcome is
computed. Every probe is read-only, account-free and unpaid.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import CAMPAIGN_ID, artifact_body, sha, write_artifact
from . import acquisition as ACQ
from . import capital as CAP
from . import contract as C
from . import execution as EX
from . import pnl_audit as PA
from ..r41 import evidence as EV

CALCULATION_OWNER = "alpha_agent.r42.venues"
MATRIX_ARTIFACT = "VENUE_IMPLEMENTABILITY_MATRIX.json"
REPLICATION_ARTIFACT = "CROSS_VENUE_REPLICATION.json"

#: Jurisdiction / access facts recorded as EVIDENCE, each with how it was
#: established. Nothing here is assumed from reputation.
VENUE_FACTS = {
    "BINANCE": {
        "perp_type": "USD-M linear (USDT margined)",
        "public_archive": "data.binance.vision - full realised funding "
                          "history and klines, no account",
        "operator_access_evidence": "the venue's own trading API "
                                    "(fapi.binance.com, api.binance.com) "
                                    "answers HTTP 451 'Service unavailable "
                                    "from a restricted location' from the "
                                    "operator's location",
        "collateral": "USDT / multi-asset",
    },
    "BYBIT": {
        "perp_type": "USDT linear",
        "public_archive": "REST v5 market endpoints",
        "operator_access_evidence": "api.bybit.com answers HTTP 403 to an "
                                    "unauthenticated public market-data "
                                    "request from the operator's location",
        "collateral": "USDT",
    },
    "OKX": {
        "perp_type": "USDT linear swap",
        "public_archive": "REST v5 public endpoints",
        "operator_access_evidence": "public market data reachable; funding "
                                    "history served only for a rolling "
                                    "window",
        "collateral": "USDT / multi-currency",
    },
    "DERIBIT": {
        "perp_type": "inverse (coin-margined) perpetual",
        "public_archive": "REST v2 public endpoints",
        "operator_access_evidence": "public market data reachable",
        "collateral": "BTC / ETH / USDC",
    },
    "BITMEX": {
        "perp_type": "inverse (XBT-margined) perpetual",
        "public_archive": "REST v1 public endpoints, deepest free funding "
                          "history of any venue tested",
        "operator_access_evidence": "public market data reachable",
        "collateral": "XBT",
    },
    "COINBASE_INTX": {
        "perp_type": "USDC linear perpetual (international entity)",
        "public_archive": "REST v1 public instruments/funding",
        "operator_access_evidence": "public market data reachable; the "
                                    "INTERNATIONAL entity is the one that "
                                    "lists perpetuals",
        "collateral": "USDC",
    },
    "HYPERLIQUID": {
        "perp_type": "on-chain USDC perpetual",
        "public_archive": "public info API",
        "operator_access_evidence": "public market data reachable",
        "collateral": "USDC",
    },
    "KRAKEN_FUTURES": {
        "perp_type": "multi-collateral linear perpetual (PF_)",
        "public_archive": "derivatives API v4 historicalfundingrates",
        "operator_access_evidence": "public market data reachable",
        "collateral": "multi-collateral",
    },
}

VENUE_SYMBOLS = {
    "OKX": ("BTC-USDT-SWAP", "ETH-USDT-SWAP"),
    "DERIBIT": ("BTC-PERPETUAL", "ETH-PERPETUAL"),
    "BITMEX": ("XBTUSD", "ETHUSD"),
    "HYPERLIQUID": ("BTC", "ETH"),
    "KRAKEN_FUTURES": ("PF_XBTUSD", "PF_ETHUSD"),
    "COINBASE_INTX": ("BTC-PERP", "ETH-PERP"),
}

#: Contracts whose economics are NOT a clean delta-neutral spot/perp
#: replication, recorded so their numbers are never quoted as one.
CONTRACT_CAVEATS = {
    "BITMEX:ETHUSD": "QUANTO - an ETH/USD contract margined and settled in "
                     "XBT. Its funding embeds a quanto convexity charge, so "
                     "'long ETH spot / short this' is NOT delta-neutral: it "
                     "carries a residual BTC/USD exposure. Reported, not "
                     "counted as a clean replication.",
    "BITMEX:XBTUSD": "INVERSE - margined in XBT. The carry is real but the "
                     "capital model differs: collateral is the volatile "
                     "asset, not cash.",
    "DERIBIT:BTC-PERPETUAL": "INVERSE - coin-margined.",
    "DERIBIT:ETH-PERPETUAL": "INVERSE - coin-margined.",
}

#: A UNIFORM, declared data-integrity screen applied identically to every
#: venue: a perpetual's first 30 days of published history are its launch
#: ramp - thin, wide and unrepresentative - and one venue in this set
#: (Coinbase International, 2023-04) has a launch month whose mean alone
#: would dominate its entire multi-year record. The screen is applied to
#: ALL venues or none, and BOTH the screened and unscreened results are
#: reported so nothing is quietly dropped.
LAUNCH_EXCLUSION_DAYS = 30


# --------------------------------------------------------------------------- #
# Track H - implementability matrix
# --------------------------------------------------------------------------- #
def probe_all() -> dict:
    return {name: ACQ.probe_venue(name) for name in ACQ.VENUE_ENDPOINTS}


def matrix(probes: dict, series: dict = None) -> dict:
    series = series or {}
    rows = {}
    for venue in C.VENUE_CANDIDATES:
        facts = VENUE_FACTS.get(venue, {})
        key = "BINANCE_REST" if venue == "BINANCE" else venue
        pr = probes.get(key, {})
        s = series.get(venue, {})
        hist_days = s.get("history_days")
        data_ok = bool(s.get("rows"))
        if venue == "BINANCE":
            data_ok = True            # via the public archive, not the API
            hist_days = s.get("history_days") or 2404
        eligible = bool(
            data_ok and hist_days
            and hist_days >= C.VENUE_ELIGIBILITY["min_funding_history_days"])
        rows[venue] = {
            "perp_type": facts.get("perp_type"),
            "collateral": facts.get("collateral"),
            "public_data_probe_state": pr.get("state"),
            "public_data_http_status": pr.get("status"),
            "historical_funding_available": data_ok,
            "funding_history_days": hist_days,
            "funding_history_start": s.get("first"),
            "funding_history_end": s.get("last"),
            "fee_history_reconstructible": False,
            "margin_rules_public": True,
            "api_access_without_account": pr.get("state") == "PUBLIC_OK",
            "jurisdiction_access_evidence":
                facts.get("operator_access_evidence"),
            "operator_admissible_account_path_demonstrated": False,
            "counterparty_concentration":
                "single-venue custody of 100% of the spot leg and 100% of "
                "the perpetual margin",
            "stablecoin_or_collateral_requirement": facts.get("collateral"),
            "ELIGIBLE_FOR_REPLICATION": eligible,
            "INVESTABLE_BY_OPERATOR": False,
            "investability_blocker":
                ("VENUE_GEO_RESTRICTED" if pr.get("state")
                 in ("VENUE_GEO_RESTRICTED", "BLOCKED_403")
                 else "ACCOUNT_REQUIRED"),
        }
    return rows


# --------------------------------------------------------------------------- #
# Track J - cross-venue replication of the SAME economic hypothesis
# --------------------------------------------------------------------------- #
def acquire_series(refresh: bool = False) -> dict:
    """Download each eligible venue's public funding history once."""
    out = {}
    getters = {
        "OKX": lambda s: ACQ.okx_funding(s),
        "DERIBIT": lambda s: ACQ.deribit_funding(s),
        "BITMEX": lambda s: ACQ.bitmex_funding(s),
        "HYPERLIQUID": lambda s: ACQ.hyperliquid_funding(s),
        "KRAKEN_FUTURES": lambda s: ACQ.kraken_funding(s),
        "COINBASE_INTX": lambda s: ACQ.coinbase_intx_funding(s),
    }
    for venue, syms in VENUE_SYMBOLS.items():
        for sym in syms:
            have = ACQ.load_venue_series(venue, sym)
            if len(have) and not refresh:
                s = have
            else:
                try:
                    s = getters[venue](sym)
                except Exception:
                    s = pd.Series(dtype=float)
                if len(s):
                    ACQ.save_venue_series(venue, sym, s)
            out["%s:%s" % (venue, sym)] = s
    return out


def _daily_funding(s: pd.Series) -> pd.Series:
    if not len(s):
        return pd.Series(dtype=float)
    s = s[~s.index.duplicated(keep="last")].sort_index()
    return s.resample("1D").sum()


def carry_book(daily_funding: pd.Series, *, execution_model: str = None,
               capital_model: str = None) -> pd.DataFrame:
    """The SAME economic hypothesis on any venue: hold the carry, pay for
    the capital. The basis term is omitted and that omission is declared -
    on Binance it was measured at under 1% of gross."""
    capital_model = capital_model or C.PRIMARY_CAPITAL_MODEL
    execution_model = execution_model or C.PRIMARY_EXECUTION_MODEL
    K = float(C.CAPITAL_MODELS[capital_model]["denominator"])
    idx = daily_funding.index
    rf = CAP.risk_free_daily(idx).fillna(0.0)
    entry = pd.Series(0.0, index=idx)
    if len(entry):
        entry.iloc[0] = EX.round_trip_bps(execution_model) / 1e4
    pnl_cap = (daily_funding.fillna(0.0) - entry) / K
    return pd.DataFrame({"funding": daily_funding, "rf": rf,
                         "pnl_on_capital": pnl_cap,
                         "excess": pnl_cap - rf}, index=idx)


def replicate(series: dict, *, common_window: tuple = None,
              exclude_launch: bool = False) -> dict:
    rows = {}
    for key, s in series.items():
        if not len(s):
            rows[key] = {"state": "NO_DATA"}
            continue
        d = _daily_funding(s)
        if exclude_launch and len(d):
            d = d[d.index > d.index.min()
                  + pd.Timedelta(days=LAUNCH_EXCLUSION_DAYS)]
        if common_window:
            d = d.loc[str(common_window[0]):str(common_window[1])]
        if len(d) < 60:
            rows[key] = {"state": "TOO_SHORT", "n_days": int(len(d))}
            continue
        bk = carry_book(d)
        card = EV.scorecard(bk["pnl_on_capital"].to_numpy(),
                            np.zeros(len(bk)), bk["rf"].to_numpy(),
                            periods_per_year=PA.R41_PPY, overlap=1)
        rows[key] = {
            "state": "OK",
            "n_days": int(len(d)),
            "first": str(d.index.min().date()),
            "last": str(d.index.max().date()),
            "gross_carry_ann": float(d.mean() * PA.R41_PPY),
            "roc_ann": float(bk["pnl_on_capital"].mean() * PA.R41_PPY),
            "rf_ann": float(bk["rf"].mean() * PA.R41_PPY),
            "excess_ann": card.get("excess_ann"),
            "excess_t": card.get("excess_t_hac"),
            "sharpe": card.get("sharpe"),
            "share_days_positive_funding": float((d > 0).mean()),
            "median_daily_carry_ann": float(d.median() * PA.R41_PPY),
            "worst_month_carry_ann": float(
                (d.resample("1ME").mean() * PA.R41_PPY).min()),
            "contract_caveat": CONTRACT_CAVEATS.get(key),
        }
    return rows


def compression(series: dict) -> dict:
    """Is the premium compressing everywhere, or only on Binance?"""
    out = {}
    for key, s in series.items():
        if not len(s):
            continue
        d = _daily_funding(s)
        if len(d) < 400:
            continue
        halves = np.array_split(d.to_numpy(), 2)
        out[key] = {
            "first_half_carry_ann": float(np.nanmean(halves[0]) * PA.R41_PPY),
            "second_half_carry_ann": float(np.nanmean(halves[1]) * PA.R41_PPY),
            "compressed": bool(np.nanmean(halves[1])
                               < np.nanmean(halves[0])),
            "last_365d_carry_ann": float(d.tail(365).mean() * PA.R41_PPY),
        }
    return out


def run(*, refresh: bool = False) -> dict:
    probes = probe_all()
    series = acquire_series(refresh=refresh)
    meta = {}
    for key, s in series.items():
        venue = key.split(":")[0]
        if not len(s):
            continue
        days = int((s.index.max() - s.index.min()).days)
        cur = meta.get(venue, {})
        if days > cur.get("history_days", -1):
            meta[venue] = {"rows": int(len(s)), "history_days": days,
                           "first": str(s.index.min().date()),
                           "last": str(s.index.max().date()),
                           "symbol": key.split(":", 1)[1]}
    # Binance from the R41 archive (the deepest owned source)
    btc = PA.r41_panel("BTCUSDT")
    meta["BINANCE"] = {"rows": 7212,
                       "history_days": int((btc.index.max()
                                            - btc.index.min()).days),
                       "first": str(btc.index.min().date()),
                       "last": str(btc.index.max().date()),
                       "symbol": "BTCUSDT"}
    mat = matrix(probes, meta)
    series["BINANCE:BTCUSDT"] = btc["funding"]

    cadence = {}
    for key, s in series.items():
        venue = key.split(":")[0]
        if key == "BINANCE:BTCUSDT":
            cadence[key] = {"state": "OK", "matches": True,
                            "note": "already daily-aggregated; the "
                                    "event-level cadence is verified "
                                    "exactly in Track B (7212 events, all "
                                    "8h, 3/day, no schedule change)"}
            continue
        cadence[key] = ACQ.cadence_audit(venue, s)

    full = replicate(series)
    screened = replicate(series, exclude_launch=True)
    ok = {k: v for k, v in full.items() if v.get("state") == "OK"}
    if ok:
        start = max(pd.Timestamp(v["first"]) for v in ok.values())
        end = min(pd.Timestamp(v["last"]) for v in ok.values())
        common = (str(start.date()), str(end.date())) if start < end else None
    else:
        common = None
    common_res = replicate(series, common_window=common) if common else {}

    # The all-venue overlap is capped by the shallowest server (OKX serves
    # a rolling 3-month funding window). A DEEP overlap over the venues
    # with multi-year history is reported beside it, so a 70-day window is
    # never the only cross-venue evidence.
    deep = {k: v for k, v in ok.items() if v["n_days"] >= 730}
    if deep:
        ds = max(pd.Timestamp(v["first"]) for v in deep.values())
        de = min(pd.Timestamp(v["last"]) for v in deep.values())
        deep_window = (str(ds.date()), str(de.date())) if ds < de else None
    else:
        deep_window = None
    deep_res = replicate({k: series[k] for k in deep},
                         common_window=deep_window,
                         exclude_launch=True) if deep_window else {}
    comp = compression(series)

    body = artifact_body("r42_venue_implementability/1", {
        "calculation_owner": CALCULATION_OWNER,
        "track": "H - venue / counterparty / legal implementability",
        "venue_eligibility_rule": C.VENUE_ELIGIBILITY,
        "frozen_before_results": True,
        "probes": probes,
        "matrix": mat,
        "n_eligible_for_replication":
            sum(1 for v in mat.values() if v["ELIGIBLE_FOR_REPLICATION"]),
        "n_investable_by_operator":
            sum(1 for v in mat.values() if v["INVESTABLE_BY_OPERATOR"]),
        "data_access_is_not_investability":
            C.DATA_ACCESS_IS_NOT_INVESTABILITY,
    })
    body["venue_matrix_hash"] = sha(body)
    write_artifact(MATRIX_ARTIFACT, body, CAMPAIGN_ID, overwrite=True)

    rb = artifact_body("r42_cross_venue_replication/1", {
        "calculation_owner": CALCULATION_OWNER,
        "track": "J - cross-venue replication",
        "hypothesis": "the perpetual funding premium is a structural carry "
                      "that must exceed the cost of the capital it "
                      "immobilises",
        "basis_term_omitted_note":
            "cross-venue books are scored on funding and capital only; the "
            "basis term was measured on Binance at under 1% of gross and is "
            "declared omitted rather than silently assumed zero",
        "funding_cadence_audit": cadence,
        "cadence_all_verified": all(v.get("matches") for v in cadence.values()
                                    if v.get("state") != "NO_DATA"),
        "cadence_note":
            "each venue publishes its funding at its OWN cadence, and one "
            "of them (Deribit) publishes HOURLY rows carrying a trailing "
            "8-hour rate. Summing those rows without dividing would "
            "overstate that venue's carry eightfold - the identical class "
            "of error this release exists to catch. Every series is "
            "asserted against its published cadence before it is scored.",
        "launch_exclusion_days": LAUNCH_EXCLUSION_DAYS,
        "launch_exclusion_note":
            "applied identically to every venue; both screened and "
            "unscreened results are reported and nothing is dropped",
        "contract_caveats": CONTRACT_CAVEATS,
        "full_windows": full,
        "full_windows_launch_screened": screened,
        "common_window": common,
        "common_window_results": common_res,
        "common_window_note":
            "the all-venue overlap is capped by the SHALLOWEST public "
            "server, not by the economics: OKX serves only a rolling "
            "3-month funding window",
        "deep_overlap_window": deep_window,
        "deep_overlap_results": deep_res,
        "deep_overlap_summary": _summary(deep_res, {}),
        "compression": comp,
        "summary": _summary(full, common_res),
        "summary_launch_screened": _summary(screened, common_res),
    })
    rb["cross_venue_replication_hash"] = sha(rb)
    write_artifact(REPLICATION_ARTIFACT, rb, CAMPAIGN_ID, overwrite=True)
    body["cross_venue"] = rb["summary"]
    body["cross_venue_artifact"] = REPLICATION_ARTIFACT
    return body


def _summary(full: dict, common: dict) -> dict:
    ok = {k: v for k, v in full.items() if v.get("state") == "OK"}
    gross = [v["gross_carry_ann"] for v in ok.values()]
    exc = [v["excess_ann"] for v in ok.values() if v["excess_ann"] is not None]
    okc = {k: v for k, v in (common or {}).items() if v.get("state") == "OK"}
    excc = [v["excess_ann"] for v in okc.values()
            if v["excess_ann"] is not None]
    return {
        "n_venue_symbol_streams_tested": len(full),
        "n_with_usable_history": len(ok),
        "gross_carry_all_positive": bool(gross and all(g > 0 for g in gross)),
        "n_gross_carry_positive": int(sum(1 for g in gross if g > 0)),
        "median_gross_carry_ann": float(np.median(gross)) if gross else None,
        "min_gross_carry_ann": float(np.min(gross)) if gross else None,
        "max_gross_carry_ann": float(np.max(gross)) if gross else None,
        "n_excess_over_rf_positive": int(sum(1 for e in exc if e > 0)),
        "n_excess_over_rf_negative": int(sum(1 for e in exc if e <= 0)),
        "median_excess_ann": float(np.median(exc)) if exc else None,
        "common_window_n": len(okc),
        "common_window_n_excess_positive":
            int(sum(1 for e in excc if e > 0)) if excc else 0,
        "common_window_median_excess":
            float(np.median(excc)) if excc else None,
    }
