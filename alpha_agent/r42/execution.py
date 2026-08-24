"""alpha_agent.r42.execution - Track F: execution reality.

R41 charged ONE flat number - 5 bps per side per leg - and charged it only
when the signal changed. That number is a FEE assumption. It contains no
bid/ask spread, no size impact, no adverse selection, no fill uncertainty
and no distinction between the spot fee schedule and the perpetual fee
schedule (which differ by a factor of two on the venue in question).

This module owns:

* the declared execution-cost ladder (contract.EXECUTION_MODELS);
* the OBSERVED half-spread measured from the archive, so the spread term
  is evidence rather than an assumption;
* the maker-admissibility rule: a maker price may be claimed only when
  fill probability, fee/rebate, adverse selection and latency are all
  modelled. A posted limit order is never assumed to fill.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import CAMPAIGN_ID, artifact_body, sha, write_artifact
from . import contract as C
from ..r41 import crypto_lab as CRL

CALCULATION_OWNER = "alpha_agent.r42.execution"
ARTIFACT = "EXECUTION_REALITY.json"


def model(name: str) -> dict:
    return C.EXECUTION_MODELS[name]


def round_trip_bps(name: str) -> float:
    """Cost, in bps of traded leg notional, of moving the position by one
    unit on BOTH legs (spot and perp)."""
    m = model(name)
    return float(m["spot_bps"] + m["perp_bps"] + 2.0 * m["spread_bps"])


def cost_stream(pos_change: pd.Series, name: str,
                multiplier: float = 1.0) -> pd.Series:
    """Per-day execution cost per unit of LEG notional."""
    return pos_change.abs().fillna(0.0) * round_trip_bps(name) \
        * multiplier / 1e4


# --------------------------------------------------------------------------- #
# Observed spread (evidence, not assumption)
# --------------------------------------------------------------------------- #
def observed_spread_proxy(symbol: str = "BTCUSDT",
                          market: str = "spot") -> dict:
    """A conservative half-spread proxy from 1-minute bars.

    With no L2 book in the free archive, the honest proxy is the median
    1-minute high-low range on low-activity minutes, halved: it bounds the
    quoted spread from above on a liquid venue and is labelled as a proxy,
    never as a measured quote.
    """
    m = CRL.load_minute(symbol, market)
    if not len(m):
        return {"state": "NO_DATA", "symbol": symbol, "market": market}
    rng = ((m["high"] - m["low"]) / m["close"].replace(0, np.nan)).dropna()
    trades = m["trades"] if "trades" in m.columns else None
    quiet = rng
    if trades is not None:
        q = trades.reindex(rng.index)
        cut = q.quantile(0.25)
        quiet = rng[q <= cut]
    out = {
        "state": "PROXY",
        "symbol": symbol, "market": market,
        "n_minutes": int(len(rng)),
        "median_minute_range_bps": float(rng.median() * 1e4),
        "quiet_minute_range_bps": float(quiet.median() * 1e4)
        if len(quiet) else None,
        "half_spread_proxy_bps": float(quiet.median() * 1e4 / 2.0)
        if len(quiet) else None,
        "p90_minute_range_bps": float(rng.quantile(0.90) * 1e4),
        "note": "UPPER-BOUND PROXY from 1-minute high-low on the quietest "
                "quartile of minutes; the free archive carries no L2 book, "
                "so a true quoted spread is not reconstructible and the "
                "contract's declared spread term is used for the ladder",
    }
    return out


# --------------------------------------------------------------------------- #
# Maker admissibility
# --------------------------------------------------------------------------- #
def maker_admissibility(evidence: dict = None) -> dict:
    """Is a maker-priced claim admissible? Only with all four components."""
    ev = evidence or {}
    have = {k: bool(ev.get(k)) for k in C.MAKER_FILL_REQUIRES}
    ok = all(have.values())
    return {
        "required": list(C.MAKER_FILL_REQUIRES),
        "available": have,
        "admissible": ok,
        "verdict": "MAKER_CLAIM_ADMISSIBLE" if ok
        else "MAKER_CLAIM_INADMISSIBLE",
        "note": "a posted limit order is NEVER assumed to fill "
                "(contract.ASSUMED_LIMIT_FILL_IS_FORBIDDEN). Without a fill "
                "model the MAKER_OPTIMISTIC row is an upper bound only and "
                "may not be quoted as an implementable return.",
    }


def ladder() -> dict:
    return {name: {"round_trip_bps": round_trip_bps(name), **model(name)}
            for name in C.EXECUTION_MODELS}


def run() -> dict:
    spread_spot = observed_spread_proxy("BTCUSDT", "spot")
    spread_perp = observed_spread_proxy("BTCUSDT", "um")
    body = artifact_body("r42_execution_reality/1", {
        "calculation_owner": CALCULATION_OWNER,
        "track": "F - execution reality",
        "ladder": ladder(),
        "primary_model": C.PRIMARY_EXECUTION_MODEL,
        "stress_multipliers": list(C.EXECUTION_STRESS_MULTIPLIERS),
        "observed_spread_proxy": {"spot": spread_spot, "perp": spread_perp},
        "maker_admissibility": maker_admissibility(),
        "r41_charged": {"round_trip_bps": round_trip_bps("R41_BASELINE"),
                        "note": "5 bps x 2 legs, on position change only"},
        "difference_vs_primary_bps":
            round_trip_bps(C.PRIMARY_EXECUTION_MODEL)
            - round_trip_bps("R41_BASELINE"),
    })
    body["execution_reality_hash"] = sha(body)
    write_artifact(ARTIFACT, body, CAMPAIGN_ID, overwrite=True)
    return body
