"""alpha_agent.r45.killer - Tracks J and K. Execution reality, then the battery.

Nothing here protects a good result. The battery runs on the strongest
candidate the release has, whatever it is, and every test is applied whether
or not it is likely to hurt.

Track J is the honest-execution half: cost multiplied, entry delayed. Both
re-enter the SAME code path the headline used, with one argument changed, so
a stress number can never come from a second implementation that quietly
disagrees with the first.

Track K is the removal half: take out a year, a release family, the single
biggest winner, the single biggest year, and see what is left. A result that
needs one January to exist is a January, not an edge.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import contract as C
from . import eventstudy as ES

CALCULATION_OWNER = "alpha_agent.r45.killer"
BOOTSTRAP_SEED = 45_000_046


# --------------------------------------------------------------------------- #
# Track J
# --------------------------------------------------------------------------- #
def cost_stress(ev: pd.DataFrame, *, multipliers=None) -> dict:
    rows = []
    for m in (multipliers or C.COST_STRESS_MULTIPLIERS):
        card = ES.score(ev, cost_mult=float(m), label=f"COST_X{m:g}")
        if card.get("state") != "MEASURED":
            continue
        rows.append({"cost_multiplier": float(m),
                     "cost_bps_per_event": card["cost_bps_per_event"],
                     "net_bps_per_event": card["net_bps_per_event"],
                     "net_t_cluster": card["net_t_cluster"]})
    alive = [r for r in rows if r["net_bps_per_event"] > 0]
    return {"rows": rows,
            "survives_x2": any(r["cost_multiplier"] == 2.0
                               and r["net_bps_per_event"] > 0 for r in rows),
            "survives_x3": any(r["cost_multiplier"] == 3.0
                               and r["net_bps_per_event"] > 0 for r in rows),
            "highest_surviving_multiplier": (max(r["cost_multiplier"]
                                                 for r in alive)
                                             if alive else 0.0)}


def latency_stress(symbol: str, stamps: pd.DataFrame, *, zone: str = None,
                   extras=None) -> dict:
    rows = []
    for extra in (extras or C.LATENCY_STRESS_EXTRA_MINUTES):
        ev = ES.event_book(symbol, stamps, extra_latency=int(extra))
        if ev is None:
            continue
        if zone:
            ev = ES.slice_zone(ev, zone)
        card = ES.score(ev, label=f"LATENCY_PLUS_{extra}M")
        if card.get("state") != "MEASURED":
            continue
        rows.append({"extra_latency_min": int(extra),
                     "n_events": card["n_events"],
                     "net_bps_per_event": card["net_bps_per_event"],
                     "net_t_cluster": card["net_t_cluster"]})
    return {"rows": rows,
            "survives_plus_1min": any(r["extra_latency_min"] == 1
                                      and r["net_bps_per_event"] > 0
                                      for r in rows),
            "latest_useful_entry_min": (
                max([r["extra_latency_min"] for r in rows
                     if r["net_bps_per_event"] > 0], default=None))}


def horizon_perturbation(symbol: str, stamps: pd.DataFrame, *,
                         zone: str = None, charge=None) -> dict:
    """Move the entry and the hold AFTER the frozen test, and pay for it."""
    rows, charged = [], []
    for delay in (1, 5, 10):
        for hold in (30, 60, 120, 240):
            ev = ES.event_book(symbol, stamps, entry_delay=delay, hold=hold)
            if ev is None:
                continue
            if zone:
                ev = ES.slice_zone(ev, zone)
            card = ES.score(ev, label=f"D{delay}_H{hold}")
            if card.get("state") != "MEASURED":
                continue
            rows.append({"entry_delay_min": delay, "hold_min": hold,
                         "n_events": card["n_events"],
                         "net_bps_per_event": card["net_bps_per_event"],
                         "net_t_cluster": card["net_t_cluster"]})
            if charge is not None:
                charged.append(charge(
                    {"lane": "HORIZON_PERTURBATION", "symbol": symbol,
                     "zone": zone or "ALL", "entry_delay_min": delay,
                     "hold_min": hold},
                    family="EVENT_FAMILY", lane="L12_KILL",
                    label=f"{symbol} d{delay} h{hold}"))
    rows.sort(key=lambda r: -(r["net_t_cluster"] or -9))
    return {"rows": rows, "n_cells": len(rows), "burden_charged": charged,
            "note": "these cells are charged in full: the frozen test is "
                    "over and this is a parameter search"}


# --------------------------------------------------------------------------- #
# Track K
# --------------------------------------------------------------------------- #
def leave_one_out(ev: pd.DataFrame, by: str) -> dict:
    if ev is None or len(ev) < 40:
        return {"state": "NO_EVENTS"}
    if by == "year":
        keys = pd.to_datetime(ev["date"]).dt.year
    elif by == "family":
        keys = ev["event"].astype(str)
    else:
        raise ValueError(by)
    rows = []
    for k in sorted(keys.unique()):
        sub = ev[keys != k].copy()
        sub.attrs.update(ev.attrs)
        if len(sub) < 40:
            continue
        card = ES.score(sub, label=f"WITHOUT_{k}")
        rows.append({"left_out": str(k), "n_events": card["n_events"],
                     "net_bps_per_event": card["net_bps_per_event"],
                     "net_t_cluster": card["net_t_cluster"]})
    if not rows:
        return {"state": "NO_EVENTS"}
    worst = min(rows, key=lambda r: r["net_bps_per_event"])
    return {"state": "MEASURED", "by": by, "rows": rows,
            "worst_when_removed": worst["left_out"],
            "worst_net_bps": worst["net_bps_per_event"],
            "all_remain_positive": all(r["net_bps_per_event"] > 0
                                       for r in rows)}


def remove_extremes(ev: pd.DataFrame) -> dict:
    _, _, net = ES.net_series(ev)
    order = np.argsort(net)
    out = {}
    for label, drop in (("largest_single_winner", order[-1:]),
                        ("largest_5_winners", order[-5:]),
                        ("largest_single_loser", order[:1])):
        keep = np.ones(len(net), dtype=bool)
        keep[drop] = False
        sub = ev[keep].copy()
        sub.attrs.update(ev.attrs)
        card = ES.score(sub, label=f"WITHOUT_{label}")
        out[label] = {"n_events": card["n_events"],
                      "net_bps_per_event": card["net_bps_per_event"],
                      "net_t_cluster": card["net_t_cluster"]}
    yr = pd.to_datetime(ev["date"]).dt.year
    pnl = pd.Series(net, index=yr.values).groupby(level=0).sum()
    if len(pnl) > 1:
        big = pnl.idxmax()
        sub = ev[yr.to_numpy() != big].copy()
        sub.attrs.update(ev.attrs)
        card = ES.score(sub, label=f"WITHOUT_YEAR_{big}")
        out["largest_year"] = {"year": int(big),
                               "n_events": card["n_events"],
                               "net_bps_per_event":
                                   card["net_bps_per_event"],
                               "net_t_cluster": card["net_t_cluster"]}
    return out


def bootstrap_by_event_date(ev: pd.DataFrame, *, draws: int = None) -> dict:
    """Cluster bootstrap: resample event DATES, not individual events."""
    _, _, net = ES.net_series(ev)
    keys = pd.to_datetime(ev["date"]).dt.normalize()
    groups = {}
    for i, k in enumerate(keys):
        groups.setdefault(k, []).append(i)
    idx_by_day = [np.asarray(v) for v in groups.values()]
    if len(idx_by_day) < 20:
        return {"state": "NO_EVENTS"}
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    n_days = len(idx_by_day)
    draws = int(draws or C.BOOTSTRAP_DRAWS)
    sims = np.empty(draws, dtype=float)
    for i in range(draws):
        pick = rng.integers(0, n_days, size=n_days)
        sel = np.concatenate([idx_by_day[j] for j in pick])
        sims[i] = float(np.nanmean(net[sel]) * 1e4)
    return {"state": "MEASURED", "draws": draws, "seed": BOOTSTRAP_SEED,
            "n_clusters": n_days,
            "mean_bps": float(sims.mean()),
            "p025_bps": float(np.percentile(sims, 2.5)),
            "p975_bps": float(np.percentile(sims, 97.5)),
            "share_of_draws_positive": float(np.mean(sims > 0)),
            "ci_excludes_zero": bool(np.percentile(sims, 2.5) > 0
                                     or np.percentile(sims, 97.5) < 0)}


# --------------------------------------------------------------------------- #
def run(symbol: str, stamps, *, zone: str = "BC", charge=None) -> dict:
    ev_all = ES.event_book(symbol, stamps)
    if ev_all is None:
        return {"track": "J+K", "state": "HISTORICAL_DATA_UNAVAILABLE"}
    ev = ES.slice_zone(ev_all, zone) if zone else ev_all
    head = ES.score(ev, label=f"{symbol}_{zone or 'ALL'}_HEADLINE")
    out = {
        "track": "J+K", "state": "EXECUTED",
        "calculation_owner": CALCULATION_OWNER,
        "symbol": symbol, "zone": zone or "ALL",
        "headline": head,
        "cost_stress": cost_stress(ev),
        "latency_stress": latency_stress(symbol, stamps, zone=zone),
        "leave_one_year_out": leave_one_out(ev, "year"),
        "leave_one_family_out": leave_one_out(ev, "family"),
        "remove_extremes": remove_extremes(ev),
        "cluster_bootstrap": bootstrap_by_event_date(ev),
        "horizon_perturbation": horizon_perturbation(
            symbol, stamps, zone=zone, charge=charge),
    }
    cs, ls = out["cost_stress"], out["latency_stress"]
    lo_y, lo_f = out["leave_one_year_out"], out["leave_one_family_out"]
    bs = out["cluster_bootstrap"]
    checks = {
        "positive_at_declared_cost": bool(
            (head.get("net_bps_per_event") or 0) > 0),
        "survives_cost_x2": bool(cs.get("survives_x2")),
        "survives_cost_x3": bool(cs.get("survives_x3")),
        "survives_latency_plus_1min": bool(ls.get("survives_plus_1min")),
        "survives_leave_one_year_out": bool(lo_y.get("all_remain_positive")),
        "survives_leave_one_family_out": bool(lo_f.get("all_remain_positive")),
        "bootstrap_ci_excludes_zero": bool(bs.get("ci_excludes_zero")),
    }
    out["checks"] = checks
    out["n_checks_passed"] = int(sum(checks.values()))
    out["n_checks"] = len(checks)
    out["KILL_BATTERY_RESULT"] = (
        "SURVIVED" if all(checks.values())
        else ("KILLED_AT_THE_FIRST_HURDLE"
              if not checks["positive_at_declared_cost"]
              else "PARTIALLY_SURVIVED"))
    out["first_failure"] = next((k for k, v in checks.items() if not v), None)
    return out
