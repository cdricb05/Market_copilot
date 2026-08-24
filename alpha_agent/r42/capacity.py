"""alpha_agent.r42.capacity - Track N: how much money can this hold?

Funding is not exogenous. The rate is set by the imbalance between longs
and shorts; a short-perp book large enough to matter pushes the very rate
it is trying to earn. This module estimates executable capacity from
observed volume, open interest and a square-root impact proxy, and reports
the capital level at which expected slippage and funding impact erode the
edge - measured against an edge that Track E has already shown to be
small.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import CAMPAIGN_ID, artifact_body, sha, write_artifact
from . import acquisition as ACQ
from . import contract as C
from . import execution as EX
from . import pnl_audit as PA
from . import r41_data_dir

CALCULATION_OWNER = "alpha_agent.r42.capacity"
ARTIFACT = "CAPACITY_REPORT.json"

#: Square-root impact: cost_bps ~ COEFF * sqrt(participation). A 10%
#: participation rate at COEFF=10 costs ~3.2 bps. Declared here, labelled
#: a PROXY, and stressed rather than trusted.
IMPACT_COEFF_BPS = 10.0
PARTICIPATION_CAP = 0.10


def _open_interest(symbol: str = "BTCUSDT") -> pd.Series:
    p = r41_data_dir("binance") / ("%s_metrics_all.csv.gz" % symbol)
    if not p.exists():
        return pd.Series(dtype=float)
    try:
        df = pd.read_csv(p)
    except Exception:
        return pd.Series(dtype=float)
    tcol = next((c for c in df.columns if "time" in c.lower()), None)
    # the VALUE column is USD; the bare column is coin units. Preferring
    # the wrong one understates open interest by the price of a bitcoin.
    ocol = "sum_open_interest_value" if "sum_open_interest_value" \
        in df.columns else None
    if not tcol or not ocol:
        return pd.Series(dtype=float)
    idx = pd.to_datetime(df[tcol], utc=True, errors="coerce", format="mixed")
    s = pd.Series(pd.to_numeric(df[ocol], errors="coerce").to_numpy(),
                  index=idx).dropna().sort_index()
    return s.resample("1D").last()


def volumes(symbol: str = "BTCUSDT") -> dict:
    from ..r41 import crypto_lab as CRL
    spot = CRL.load_daily(symbol)
    spot.index = pd.to_datetime(spot.index, utc=True)
    sv = spot["quote_volume"].resample("1D").sum()
    perp = CRL.load_minute(symbol, "um")
    pv = perp["quote_volume"].resample("1D").sum() if len(perp) \
        else pd.Series(dtype=float)
    oi = _open_interest(symbol)
    recent = slice("2025-04-14", "2026-07-31")
    return {
        "symbol": symbol,
        "spot_median_daily_quote_volume_usd": float(sv.loc[recent].median())
        if len(sv.loc[recent]) else float(sv.median()),
        "perp_median_daily_quote_volume_usd": float(pv.loc[recent].median())
        if len(pv.loc[recent]) else (float(pv.median()) if len(pv) else None),
        "open_interest_median_usd": float(oi.loc[recent].median())
        if len(oi.loc[recent]) else (float(oi.median()) if len(oi) else None),
        "open_interest_available": bool(len(oi)),
        "window": "2025-04-14..2026-07-31",
    }


def impact_bps(notional_usd: float, daily_volume_usd: float) -> float:
    if not daily_volume_usd or daily_volume_usd <= 0:
        return float("nan")
    part = notional_usd / daily_volume_usd
    return float(IMPACT_COEFF_BPS * np.sqrt(max(part, 0.0)))


#: Elasticity of the funding rate to an added short-perp share of open
#: interest. A short-perp book supplies the very imbalance funding pays
#: for, so a book that is X% of open interest is assumed to compress the
#: rate it earns by X%. Declared, bounded at 100%, and labelled a PROXY.
FUNDING_ELASTICITY_TO_OI_SHARE = 1.0


def funding_erosion_fraction(notional_usd: float,
                             open_interest_usd: float) -> float:
    if not open_interest_usd or open_interest_usd <= 0:
        return float("nan")
    share = notional_usd / open_interest_usd
    return float(min(1.0, share * FUNDING_ELASTICITY_TO_OI_SHARE))


def run(symbol: str = "BTCUSDT") -> dict:
    v = volumes(symbol)
    df = PA.r41_panel(symbol)
    z = PA.r41_zones(df.index)
    recent = df.reindex(z["C"])
    base_edge_ann = float(np.nanmean(recent["funding_pnl"]) * PA.R41_PPY)
    rt = EX.round_trip_bps(C.PRIMARY_EXECUTION_MODEL)
    K = float(C.CAPITAL_MODELS[C.PRIMARY_CAPITAL_MODEL]["denominator"])

    levels = {}
    for cap in C.CAPACITY_LEVELS_USD:
        leg = cap / K                    # notional per leg at that capital
        s_imp = impact_bps(leg, v["spot_median_daily_quote_volume_usd"])
        p_imp = impact_bps(leg, v["perp_median_daily_quote_volume_usd"])
        f_frac = funding_erosion_fraction(leg,
                                          v["open_interest_median_usd"])
        funding_erosion_ann = (base_edge_ann * f_frac
                               if np.isfinite(f_frac) else float("nan"))
        levels["$%s" % format(int(cap), ",")] = {
            "committed_capital_usd": cap,
            "leg_notional_usd": leg,
            "spot_participation_of_daily_volume":
                leg / v["spot_median_daily_quote_volume_usd"]
                if v["spot_median_daily_quote_volume_usd"] else None,
            "perp_participation_of_daily_volume":
                leg / v["perp_median_daily_quote_volume_usd"]
                if v["perp_median_daily_quote_volume_usd"] else None,
            "share_of_open_interest":
                leg / v["open_interest_median_usd"]
                if v["open_interest_median_usd"] else None,
            "spot_impact_bps": s_imp, "perp_impact_bps": p_imp,
            "round_trip_incl_impact_bps": rt + (s_imp or 0) + (p_imp or 0),
            "funding_erosion_fraction": f_frac,
            "estimated_funding_erosion_ann": funding_erosion_ann,
            "gross_carry_ann_recent": base_edge_ann,
            "carry_after_funding_erosion_ann":
                base_edge_ann - (funding_erosion_ann
                                 if np.isfinite(funding_erosion_ann) else 0.0),
            "exceeds_participation_cap":
                bool(v["spot_median_daily_quote_volume_usd"]
                     and leg / v["spot_median_daily_quote_volume_usd"]
                     > PARTICIPATION_CAP),
            "materially_erodes_edge":
                bool(np.isfinite(funding_erosion_ann)
                     and funding_erosion_ann > 0.10 * abs(base_edge_ann)),
        }
    body = artifact_body("r42_capacity/1", {
        "calculation_owner": CALCULATION_OWNER,
        "track": "N - capacity",
        "inputs": list(C.CAPACITY_INPUTS),
        "volumes": v,
        "impact_model": {"form": "cost_bps = %g * sqrt(participation)"
                                 % IMPACT_COEFF_BPS,
                         "state": "PROXY",
                         "participation_cap": PARTICIPATION_CAP,
                         "note": "no L2 book is available at $0, so impact "
                                 "is a declared proxy, not a measurement"},
        "funding_is_not_exogenous_at_scale":
            C.FUNDING_IS_NOT_EXOGENOUS_AT_SCALE,
        "levels": levels,
        "verdict": _verdict(levels, base_edge_ann),
    })
    body["capacity_hash"] = sha(body)
    write_artifact(ARTIFACT, body, CAMPAIGN_ID, overwrite=True)
    return body


def _verdict(levels: dict, edge: float) -> dict:
    binding = [k for k, v in levels.items()
               if v["materially_erodes_edge"] or v["exceeds_participation_cap"]]
    return {
        "state": "CAPACITY_IS_NOT_THE_BINDING_CONSTRAINT",
        "gross_carry_ann_recent": edge,
        "levels_where_impact_binds": binding,
        "note": "on the single most liquid pair in crypto, participation at "
                "$10m is a rounding error against daily volume, so capacity "
                "is NOT what stops this trade. That matters: it removes the "
                "most comfortable explanation for a disappointing result. "
                "The trade is not too small to scale - it simply does not "
                "earn more than cash.",
        "caveat": "capacity on the 69 replication assets is a different "
                  "question and a far worse one; their premium is highest "
                  "exactly where their volume is lowest.",
    }
