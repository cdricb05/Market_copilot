"""alpha_agent.r46.judge - THE outcome judge. One calculation per concept.

A prediction matures when the instrument's OWN realised bar calendar has
printed ``horizon`` sessions after the entry close. Nothing is scored on an
assumed holiday table and nothing is scored early.

What every matured prediction is charged and credited:

* **gross** - the book's return from entry close to maturity close, per leg,
  weighted as the immutable prediction row recorded it;
* **cost** - BOTH sides of the round trip, on TRADED NOTIONAL, at the
  declared per-class half-spread plus slippage. Release 31's correction: cost
  scales with what you trade, not with what you earn;
* **control** - for a collateralised book, the risk-free rate on the capital
  it ties up; for a benchmark-relative challenger, the benchmark's own return
  over the identical window. Release 42's lesson made a clause: beating zero
  is not beating cash, and a premium priced below cash is not alpha;
* **residual** - the return net of the benchmark's own move, where a benchmark
  is defined.

``net_alpha_vs_control`` is the only number that decides anything. Gross is
reported because hiding it would be dishonest, not because it means much:
Release 43 watched a real premium disappear entirely into two-legged cost.

The judge never revises a forecast. It appends an outcome row keyed by
``prediction_id`` and the original prediction stays byte-identical under its
chain hash.
"""
from __future__ import annotations

import datetime as _dt
import math
from typing import Optional

import numpy as np

from . import CAMPAIGN_ID, artifact_body
from . import clock as CK
from . import contract as C
from . import ledger as LG
from . import marketdata as MD

CALCULATION_OWNER = "alpha_agent.r46.judge"

MIN_LEGS_FOR_IC = 8


# --------------------------------------------------------------------------- #
def _series(symbol: str):
    return MD.closes(symbol)


def _mark_on_or_after(series, d: _dt.date):
    """(date, price) of the first bar on or after ``d``; ``(None, None)``."""
    if series is None or not len(series):
        return None, None
    for ts, px in series.items():
        if ts.date() >= d:
            return ts.date(), float(px)
    return None, None


def _mark_n_sessions_after(series, entry_date: _dt.date, horizon: int):
    """(date, price) of the close ``horizon`` realised sessions after entry."""
    if series is None or not len(series):
        return None, None
    dates = [ts.date() for ts in series.index]
    idx = None
    for i, d in enumerate(dates):
        if d >= entry_date:
            idx = i
            break
    if idx is None:
        return None, None
    j = idx + int(horizon)
    if j >= len(dates):
        return None, None
    return dates[j], float(series.iloc[j])


def _spearman(a, b) -> Optional[float]:
    if len(a) != len(b) or len(a) < MIN_LEGS_FOR_IC:
        return None
    ra = np.argsort(np.argsort(np.asarray(a, dtype=float)))
    rb = np.argsort(np.argsort(np.asarray(b, dtype=float)))
    if np.std(ra) == 0 or np.std(rb) == 0:
        return None
    v = float(np.corrcoef(ra, rb)[0, 1])
    return v if math.isfinite(v) else None


def _cost_bps(legs, spec_cost_class: str) -> float:
    total = 0.0
    for l in legs:
        klass = l.get("cost_class") or spec_cost_class or "US_EQUITY"
        half = C.COST_BPS_PER_SIDE.get(klass, 5.0) + C.SLIPPAGE_BPS_PER_SIDE
        total += abs(float(l["weight"])) * half
    return float(total)


# --------------------------------------------------------------------------- #
def resolve(prediction: dict) -> dict:
    """Score ONE prediction, or explain exactly why it is not scoreable yet."""
    pid = prediction.get("prediction_id")
    horizon = int(prediction.get("horizon") or 0)
    try:
        entry_date = _dt.date.fromisoformat(
            str(prediction.get("effective_as_of"))[:10])
    except ValueError:
        return {"prediction_id": pid, "state": "INVALID_EFFECTIVE_AS_OF"}

    legs = ((prediction.get("position_expression") or {}).get("legs")) or []
    if not legs:
        return {"prediction_id": pid, "state": "NO_LEGS"}

    per_leg, missing = [], []
    for l in legs:
        sym = l["instrument"]
        s = _series(sym)
        e_date, e_px = _mark_on_or_after(s, entry_date)
        m_date, m_px = _mark_n_sessions_after(s, entry_date, horizon)
        if e_px is None or m_px is None or e_px <= 0:
            missing.append({"instrument": sym,
                            "entry_available": e_px is not None,
                            "maturity_available": m_px is not None})
            continue
        r = m_px / e_px - 1.0
        per_leg.append({
            "instrument": sym, "weight": float(l["weight"]),
            "score": float(l.get("score")) if l.get("score") is not None
                     else None,
            "side": l.get("side"), "cost_class": l.get("cost_class"),
            "entry_date": str(e_date), "entry_price": e_px,
            "maturity_date": str(m_date), "maturity_price": m_px,
            "leg_return": r,
            "weighted_return": float(l["weight"]) * r,
        })

    if missing:
        return {"prediction_id": pid, "state": "NOT_MATURED",
                "horizon": horizon, "effective_as_of": str(entry_date),
                "n_legs_resolved": len(per_leg),
                "n_legs_missing": len(missing), "missing": missing[:10],
                "reason": "at least one leg has not printed the entry close "
                          "and %d subsequent sessions yet" % horizon}

    maturity_dates = sorted({l["maturity_date"] for l in per_leg})
    entry_dates = sorted({l["entry_date"] for l in per_leg})
    gross = float(sum(l["weighted_return"] for l in per_leg))
    gross_notional = float(sum(abs(l["weight"]) for l in per_leg))

    entry_cost = _cost_bps(legs, prediction.get("cost_class")) / 1e4
    exit_cost = entry_cost                      # both sides of the round trip
    cost = entry_cost + exit_cost
    net = gross - cost

    control = prediction.get("control")
    rf = MD.risk_free_per_session(horizon)
    rf_state = MD.risk_free_annual().get("state")
    bench_sym = prediction.get("benchmark")
    bench_ret = None
    if bench_sym and bench_sym not in ("CASH",):
        bs = _series(bench_sym)
        _, b_e = _mark_on_or_after(bs, entry_date)
        _, b_m = _mark_n_sessions_after(bs, entry_date, horizon)
        if b_e and b_m and b_e > 0:
            bench_ret = b_m / b_e - 1.0

    if control == C.CONTROL_BENCHMARK:
        control_return = bench_ret
        control_desc = "%s buy-and-hold over the identical window" % bench_sym
    else:
        control_return = rf
        control_desc = ("risk-free accrual on the capital the book ties up "
                        "(%s)" % C.RISK_FREE_SERIES)

    net_alpha = (None if control_return is None else net - control_return)
    residual = (None if bench_ret is None else gross - bench_ret)

    longs = [l["leg_return"] for l in per_leg if l["weight"] > 0]
    shorts = [l["leg_return"] for l in per_leg if l["weight"] < 0]
    spread = ((float(np.mean(longs)) - float(np.mean(shorts)))
              if longs and shorts else None)
    rank_ic = _spearman([l["score"] for l in per_leg
                         if l["score"] is not None],
                        [l["leg_return"] for l in per_leg
                         if l["score"] is not None])

    contributions = [abs(l["weighted_return"]) for l in per_leg]
    tot_contrib = float(sum(contributions)) or 1.0
    top_leg_share = float(max(contributions) / tot_contrib)

    net_at_2x = gross - 2.0 * cost
    net_alpha_at_2x = (None if control_return is None
                       else net_at_2x - control_return)

    return {
        "prediction_id": pid,
        "state": "SCOREABLE",
        "challenger_id": prediction.get("challenger_id"),
        "challenger_version": prediction.get("challenger_version"),
        "challenger_spec_hash": prediction.get("challenger_spec_hash"),
        "asset_class": prediction.get("asset_class"),
        "horizon": horizon,
        "effective_as_of": str(entry_date),
        "entry_dates": entry_dates,
        "maturity_dates": maturity_dates,
        "maturity_date": maturity_dates[-1] if maturity_dates else None,

        "realised_gross_return": gross,
        "realised_benchmark_return": bench_ret,
        "realised_residual_return": residual,
        "realised_cost": cost,
        "realised_cost_entry_side": entry_cost,
        "realised_cost_exit_side": exit_cost,
        "realised_net_return": net,
        "realised_net_return_at_2x_costs": net_at_2x,

        "control": control,
        "control_description": control_desc,
        "control_return": control_return,
        "risk_free_state": rf_state,
        "net_alpha_vs_control": net_alpha,
        "net_alpha_vs_control_at_2x_costs": net_alpha_at_2x,

        "gross_notional": gross_notional,
        "turnover": gross_notional * 2.0,
        "n_legs": len(per_leg),
        "rank_ic": rank_ic,
        "top_minus_bottom_spread": spread,
        "top_leg_share_of_absolute_contribution": top_leg_share,
        "hit": bool(net_alpha is not None and net_alpha > 0),
        "per_leg": per_leg,
        "forward_evidence_type": C.TRUE_FORWARD,
        "calculation_owner": CALCULATION_OWNER,
    }


# --------------------------------------------------------------------------- #
def score_pending(campaign_id: str = CAMPAIGN_ID,
                  scored_at: _dt.datetime = None) -> dict:
    """Find, score and persist every prediction that has matured. Idempotent."""
    now = scored_at or CK.now_utc()
    preds = LG.predictions(campaign_id)
    already = {str(o.get("prediction_id")) for o in LG.outcomes(campaign_id)}

    rows, not_matured, invalid = [], [], []
    for p in preds:
        if str(p.get("prediction_id")) in already:
            continue
        r = resolve(p)
        if r.get("state") == "SCOREABLE":
            row = dict(r)
            row.pop("state", None)
            row["scored_at_utc"] = CK.iso(now)
            row["status"] = C.STATUS_SCORED
            rows.append(row)
        elif r.get("state") == "NOT_MATURED":
            not_matured.append(r)
        else:
            invalid.append(r)

    result = LG.append_outcomes(rows, campaign_id) if rows else {
        "n_offered": 0, "n_appended": 0, "n_duplicates_skipped": 0,
        "duplicates": [], "appended": [], "idempotent": True}

    return artifact_body(
        "r46_judge_run/1", CALCULATION_OWNER,
        scored_at_utc=CK.iso(now),
        n_predictions=len(preds),
        n_already_scored=len(already),
        n_newly_scored=result["n_appended"],
        n_still_pending=len(not_matured),
        n_invalid=len(invalid),
        invalid=invalid[:20],
        duplicates_skipped=result["n_duplicates_skipped"],
        idempotent=True,
        never_revises_a_forecast=True,
        pending_detail=[{"prediction_id": r["prediction_id"],
                         "horizon": r.get("horizon"),
                         "effective_as_of": r.get("effective_as_of"),
                         "reason": r.get("reason")}
                        for r in not_matured[:50]],
    )
