"""alpha_agent.r42.legs - Track D: the two legs are not equally real.

The R41 rule takes two economically opposite positions:

    z > +0.5   POSITIVE-FUNDING CASH-AND-CARRY   LONG SPOT  / SHORT PERP
    z < -0.5   NEGATIVE-FUNDING REVERSE CARRY    SHORT SPOT / LONG PERP

The first needs cash. The second needs a BORROW: someone must lend the
coin so it can be sold short, at a rate, with capacity, subject to recall.
R41 charged nothing for that and proved nothing about it.

This module splits the historical PnL by leg, gathers what borrow evidence
is actually obtainable at $0, and applies the frozen rule: if historical
borrow cannot be proven from evidence dated INSIDE the sample, the reverse
leg is HISTORICALLY_NON_IMPLEMENTABLE and its PnL may not count toward an
implementable claim.
"""
from __future__ import annotations

import json

import numpy as np
import pandas as pd

from . import CAMPAIGN_ID, artifact_body, sha, write_artifact
from . import acquisition as ACQ
from . import contract as C
from . import pnl_audit as PA

CALCULATION_OWNER = "alpha_agent.r42.legs"
ARTIFACT = "LEG_SEPARATION_AND_BORROW.json"

BINANCE_MARGIN_SPEC = ("https://www.binance.com/bapi/margin/v1/public/"
                       "margin/vip/spec/list-all")


# --------------------------------------------------------------------------- #
# 1. Leg separation
# --------------------------------------------------------------------------- #
def split(df: pd.DataFrame = None) -> dict:
    df = PA.r41_panel("BTCUSDT") if df is None else df
    z = PA.r41_zones(df.index)
    out = {}
    for name in ("A", "B", "C"):
        d = df.reindex(z[name])
        held = d["held"]
        gross = d["gross"]
        cost = d["cost_r41"]
        pos = held > 0
        neg = held < 0
        flat = held == 0
        n = len(d)

        def leg(mask, label):
            g = gross.where(mask, 0.0)
            k = cost.where(mask, 0.0)
            return {
                "label": label,
                "n_days": int(mask.sum()),
                "share_of_days": float(mask.sum() / n) if n else None,
                "gross_ann_contribution": float(np.nanmean(g) * PA.R41_PPY),
                "cost_ann_contribution": float(np.nanmean(k) * PA.R41_PPY),
                "net_ann_contribution": float(np.nanmean(g - k)
                                              * PA.R41_PPY),
                "funding_ann_contribution":
                    float(np.nanmean(d["funding_pnl"].where(mask, 0.0))
                          * PA.R41_PPY),
                "basis_ann_contribution":
                    float(np.nanmean(d["basis_pnl"].where(mask, 0.0))
                          * PA.R41_PPY),
            }

        total_net = float(np.nanmean(gross - cost) * PA.R41_PPY)
        pos_leg = leg(pos, "POSITIVE_FUNDING_LEG")
        neg_leg = leg(neg, "NEGATIVE_FUNDING_LEG")
        flat_leg = leg(flat, "FLAT")
        for lg in (pos_leg, neg_leg, flat_leg):
            lg["share_of_net"] = (None if not total_net
                                  else lg["net_ann_contribution"] / total_net)
        out[name] = {
            "range": z["%s_range" % name.lower()],
            "n_days": int(n),
            "total_net_ann": total_net,
            "POSITIVE_FUNDING_LEG": pos_leg,
            "NEGATIVE_FUNDING_LEG": neg_leg,
            "FLAT": flat_leg,
        }
    return out


# --------------------------------------------------------------------------- #
# 2. Borrow evidence
# --------------------------------------------------------------------------- #
def borrow_evidence() -> dict:
    """Everything obtainable about spot borrow at $0 and without an account.

    Binance publishes a CURRENT margin interest-rate and borrow-limit
    table on a public endpoint. It publishes no historical vintage of it,
    and its authenticated ``interestRateHistory`` route needs an account
    and an API key - both forbidden by the R42 safety boundary.
    """
    ev = {"queried_at": ACQ._now(), "sources": []}
    raw = ACQ.fetch(BINANCE_MARGIN_SPEC, timeout=60)
    current = {"state": "UNAVAILABLE"}
    if raw:
        try:
            d = json.loads(raw.decode("utf-8")).get("data") or []
            rows = {}
            for a in d:
                nm = a.get("assetName")
                if nm not in ("BTC", "ETH", "USDT", "USDC", "SOL", "BNB"):
                    continue
                v0 = [s for s in (a.get("specs") or [])
                      if str(s.get("vipLevel")) == "0"]
                if not v0:
                    continue
                dr = float(v0[0]["dailyInterestRate"])
                rows[nm] = {"daily_rate": dr,
                            "annualised_rate": dr * 365.0,
                            "borrow_limit": float(v0[0]["borrowLimit"])}
            current = {"state": "CURRENT_SNAPSHOT_ONLY", "vip_level": 0,
                       "assets": rows,
                       "url": BINANCE_MARGIN_SPEC,
                       "sha256": ACQ.hashlib.sha256(raw).hexdigest()}
        except Exception as exc:
            current = {"state": "PARSE_ERROR", "error": str(exc)}
    ev["sources"].append({"name": "BINANCE_PUBLIC_MARGIN_SPEC",
                          "result": current})

    ev["sources"].append({
        "name": "BINANCE_AUTHENTICATED_INTEREST_RATE_HISTORY",
        "result": {"state": "ACCOUNT_REQUIRED",
                   "route": "GET /sapi/v1/margin/interestRateHistory",
                   "note": "needs an exchange account and a signed API key; "
                           "both are forbidden by the R42 safety boundary "
                           "and neither would be point-in-time evidence for "
                           "2020-2025 in any case"}})
    ev["sources"].append({
        "name": "PUBLIC_ARCHIVE_MARGIN_RATES",
        "result": {"state": "HISTORICAL_DATA_UNAVAILABLE",
                   "note": "data.binance.vision publishes klines, funding "
                           "rates, trades, book ticker and futures metrics. "
                           "It publishes NO margin interest-rate history "
                           "and NO borrowable-supply history."}})
    ev["sources"].append({
        "name": "VENUE_GEO_ELIGIBILITY",
        "result": {"state": "VENUE_GEO_RESTRICTED",
                   "note": "the venue's own trading API answers HTTP 451 "
                           "from the operator's location, so even the "
                           "CURRENT borrow table is not a rate this "
                           "operator could transact at"}})

    have = {
        "historical_borrow_availability": False,
        "historical_borrow_rate": False,
        "recall_risk": False,
        "short_sale_mechanics": True,     # documented: cross/isolated margin
        "borrow_capacity": False,         # current limit known, history not
    }
    proven = all(have[k] for k in C.BORROW_EVIDENCE_REQUIRED)
    ev["required"] = list(C.BORROW_EVIDENCE_REQUIRED)
    ev["available"] = have
    ev["current_snapshot"] = current
    ev["current_snapshot_is_not_history"] = C.CURRENT_SNAPSHOT_IS_NOT_HISTORY
    ev["verdict"] = ("HISTORICALLY_IMPLEMENTABLE" if proven
                     else C.BORROW_UNPROVEN_VERDICT)
    ev["blocker"] = None if proven else "BORROW_HISTORY_UNAVAILABLE"
    return ev


# --------------------------------------------------------------------------- #
# 3. The positive-funding-only candidate (NEW R42 identity)
# --------------------------------------------------------------------------- #
def positive_only_signal(df: pd.DataFrame) -> pd.Series:
    """The predeclared implementability-first baseline (contract).

    LONG SPOT / SHORT PERP whenever the trailing 30-day mean funding
    observed through t-1 is strictly positive; otherwise CASH. Never short
    spot. Note this uses NO z-score, NO standard-deviation window and NO
    threshold: it is deliberately simpler than the R41 rule and was
    declared before any R42 outcome was computed.
    """
    f = df["funding"]
    mean30 = f.rolling(30, min_periods=15).mean().shift(1)
    sig = pd.Series(0.0, index=df.index)
    sig[mean30 > 0] = 1.0
    return sig


def r41_signal_positive_clipped(df: pd.DataFrame) -> pd.Series:
    """The R41 rule with the unproven reverse leg REMOVED (not retuned).

    Identical z, windows and threshold; the -1 state becomes 0. This is a
    NEW R42 identity - the R41 candidate itself is untouched.
    """
    return df["signal"].clip(lower=0.0)


def run() -> dict:
    df = PA.r41_panel("BTCUSDT")
    sp = split(df)
    ev = borrow_evidence()
    reverse_admissible = ev["verdict"] == "HISTORICALLY_IMPLEMENTABLE"
    body = artifact_body("r42_leg_separation_and_borrow/1", {
        "calculation_owner": CALCULATION_OWNER,
        "track": "D - positive vs negative funding legs",
        "leg_split": sp,
        "borrow_evidence": ev,
        "reverse_leg_admissible": reverse_admissible,
        "reverse_leg_state": ev["verdict"],
        "new_r42_identities": {
            "R42_POSITIVE_ONLY_CASH_AND_CARRY":
                C.POSITIVE_ONLY_BASELINE["rule"],
            "R42_R41RULE_POSITIVE_CLIPPED":
                "the R41 z-gate with the -1 state removed; z, windows and "
                "threshold IDENTICAL to R41, nothing retuned",
        },
        "r41_candidate_modified": False,
        "verdict": _verdict(sp, ev),
    })
    body["leg_separation_hash"] = sha(body)
    write_artifact(ARTIFACT, body, CAMPAIGN_ID, overwrite=True)
    return body


def _verdict(sp: dict, ev: dict) -> dict:
    b, c = sp["B"], sp["C"]
    return {
        "reverse_leg": ev["verdict"],
        "reverse_leg_days_zone_b": b["NEGATIVE_FUNDING_LEG"]["n_days"],
        "reverse_leg_days_zone_c": c["NEGATIVE_FUNDING_LEG"]["n_days"],
        "reverse_leg_share_of_net_zone_b":
            b["NEGATIVE_FUNDING_LEG"]["share_of_net"],
        "reverse_leg_share_of_net_zone_c":
            c["NEGATIVE_FUNDING_LEG"]["share_of_net"],
        "state": ("R42_BORROW_REALITY_KILLS_REVERSE_LEG"
                  if ev["verdict"] != "HISTORICALLY_IMPLEMENTABLE"
                  else "REVERSE_LEG_ADMISSIBLE"),
        "note": "the reverse leg is excluded from every implementable R42 "
                "claim. How much that costs is measured, not assumed.",
    }
