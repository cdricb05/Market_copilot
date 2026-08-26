"""alpha_agent.r46.evidence - how much a pile of forward rows is actually worth.

A twenty-session challenger emitting daily produces twenty overlapping bets on
largely the same twenty days. Counting those as twenty independent
observations is the fastest way to manufacture a t-statistic, and it is a
mistake this project has the scar tissue to recognise: Release 45 measured a
median net t near -1.0 across sixty cells and a maximum near +2.0 purely
because the grid was noisy and large.

So every count in this release comes in two flavours and the pair is always
reported together:

``raw_matured``
    rows the judge has scored.

``effective_independent``
    ``raw_matured / horizon`` for overlapping horizons - the standard
    correction for a fixed-horizon overlapping-return series - and never more
    than the number of distinct decision dates.

The gate reads the effective number. The leaderboard displays both, so nobody
can mistake fifty overlapping twenty-day bets for fifty independent ones.

Concentration is checked the same way and for the same reason: a challenger
whose entire forward P&L came from one day or one leg has not demonstrated a
repeatable edge, whatever its mean.
"""
from __future__ import annotations

import math
from typing import Optional

import numpy as np

from . import contract as C

CALCULATION_OWNER = "alpha_agent.r46.evidence"


def effective_independent(n_raw: int, horizon: int,
                          n_distinct_dates: int = None) -> int:
    """Overlapping fixed-horizon returns, discounted by the overlap."""
    if n_raw <= 0:
        return 0
    eff = int(math.floor(float(n_raw) / max(1, int(horizon))))
    if n_distinct_dates is not None:
        eff = min(eff, int(n_distinct_dates))
    return max(0, eff)


def _t_stat(x) -> Optional[float]:
    a = np.asarray([v for v in x if v is not None and math.isfinite(v)],
                   dtype=float)
    if len(a) < 2:
        return None
    sd = float(a.std(ddof=1))
    if sd <= 0:
        return None
    v = float(a.mean() / (sd / math.sqrt(len(a))))
    return v if math.isfinite(v) else None


def _ci95(x):
    a = np.asarray([v for v in x if v is not None and math.isfinite(v)],
                   dtype=float)
    if len(a) < 2:
        return None, None
    se = float(a.std(ddof=1) / math.sqrt(len(a)))
    m = float(a.mean())
    return m - 1.96 * se, m + 1.96 * se


def _max_drawdown(returns) -> Optional[float]:
    a = [v for v in returns if v is not None and math.isfinite(v)]
    if not a:
        return None
    eq, peak, dd = 1.0, 1.0, 0.0
    for r in a:
        eq *= (1.0 + float(r))
        peak = max(peak, eq)
        dd = min(dd, eq / peak - 1.0)
    return float(dd)


def summarise(outcomes: list, horizon: int) -> dict:
    """Everything the gate and the leaderboard need for ONE challenger-horizon."""
    rows = [o for o in outcomes if int(o.get("horizon") or 0) == int(horizon)]
    n_raw = len(rows)
    dates = sorted({str(o.get("effective_as_of")) for o in rows})
    eff = effective_independent(n_raw, horizon, len(dates))

    alpha = [o.get("net_alpha_vs_control") for o in rows]
    alpha = [a for a in alpha if a is not None]
    net = [o.get("realised_net_return") for o in rows
           if o.get("realised_net_return") is not None]
    gross = [o.get("realised_gross_return") for o in rows
             if o.get("realised_gross_return") is not None]
    alpha2x = [o.get("net_alpha_vs_control_at_2x_costs") for o in rows
               if o.get("net_alpha_vs_control_at_2x_costs") is not None]
    ics = [o.get("rank_ic") for o in rows if o.get("rank_ic") is not None]

    mean_alpha = float(np.mean(alpha)) if alpha else None
    lo, hi = _ci95(alpha)
    t = _t_stat(alpha)
    hits = [1.0 for o in rows if o.get("hit")]
    hit_rate = (float(len(hits)) / n_raw) if n_raw else None

    wins = [a for a in alpha if a > 0]
    losses = [-a for a in alpha if a < 0]
    payoff = ((float(np.mean(wins)) / float(np.mean(losses)))
              if wins and losses and float(np.mean(losses)) > 0 else None)

    sharpe = None
    if alpha and len(alpha) > 1:
        sd = float(np.std(alpha, ddof=1))
        if sd > 0:
            sharpe = float(np.mean(alpha) / sd
                           * math.sqrt(252.0 / max(1, int(horizon))))
    downside = [a for a in alpha if a < 0]
    downside_dev = float(np.std(downside, ddof=1)) if len(downside) > 1 \
        else None

    by_date: dict = {}
    for o in rows:
        d = str(o.get("effective_as_of"))
        by_date[d] = by_date.get(d, 0.0) + abs(
            float(o.get("net_alpha_vs_control") or 0.0))
    total_abs = sum(by_date.values()) or 1.0
    single_day_share = (max(by_date.values()) / total_abs) if by_date else None
    leg_shares = [o.get("top_leg_share_of_absolute_contribution")
                  for o in rows
                  if o.get("top_leg_share_of_absolute_contribution")
                  is not None]

    halves_same_sign = None
    if len(alpha) >= 4:
        h = len(alpha) // 2
        a1, a2 = float(np.mean(alpha[:h])), float(np.mean(alpha[h:]))
        halves_same_sign = bool((a1 > 0) == (a2 > 0))

    return {
        "horizon": int(horizon),
        "raw_matured": n_raw,
        "effective_independent": eff,
        "n_distinct_decision_dates": len(dates),
        "first_decision": dates[0] if dates else None,
        "last_decision": dates[-1] if dates else None,
        "mean_net_alpha_vs_control": mean_alpha,
        "mean_net_alpha_bps": (None if mean_alpha is None
                               else mean_alpha * 1e4),
        "mean_net_return": float(np.mean(net)) if net else None,
        "mean_gross_return": float(np.mean(gross)) if gross else None,
        "mean_net_alpha_at_2x_costs": (float(np.mean(alpha2x))
                                       if alpha2x else None),
        "positive_at_2x_costs": (bool(np.mean(alpha2x) > 0)
                                 if alpha2x else None),
        "t_stat_net_vs_control": t,
        "ci95_low": lo, "ci95_high": hi,
        "ci95_excludes_zero": (None if lo is None else (lo > 0 or hi < 0)),
        "hit_rate": hit_rate,
        "payoff_ratio": payoff,
        "sharpe_annualised": sharpe,
        "downside_deviation": downside_dev,
        "max_drawdown": _max_drawdown(alpha),
        "mean_rank_ic": float(np.mean(ics)) if ics else None,
        "turnover_per_decision": (float(np.mean(
            [o.get("turnover") or 0.0 for o in rows])) if rows else None),
        "single_day_share_of_pnl": single_day_share,
        "max_single_leg_share": (float(np.max(leg_shares))
                                 if leg_shares else None),
        "halves_same_sign": halves_same_sign,
    }


# --------------------------------------------------------------------------- #
def gate(summary: dict, pit_ok: bool = True,
         retune_free: bool = True) -> dict:
    """Apply the declared forward-evidence gate to ONE challenger-horizon."""
    G = C.FORWARD_EVIDENCE_GATES
    h = int(summary.get("horizon") or 1)
    need_eff = G["min_effective_independent"].get(h, 40)
    need_raw = G["min_raw_matured"].get(h, 60)
    need_days = G["min_calendar_days"].get(h, 180)

    raw = int(summary.get("raw_matured") or 0)
    eff = int(summary.get("effective_independent") or 0)
    mean_bps = summary.get("mean_net_alpha_bps")
    t = summary.get("t_stat_net_vs_control")

    checks = {
        "enough_effective_independent": eff >= need_eff,
        "enough_raw_matured": raw >= need_raw,
        "positive_net_edge": (mean_bps is not None
                              and mean_bps >= G["min_net_edge_bps_per_decision"]),
        "significant_vs_control": (t is not None
                                   and t >= G["min_t_stat_net_vs_control"]),
        "confidence_interval_excludes_zero":
            bool(summary.get("ci95_excludes_zero")),
        "positive_at_2x_costs": bool(summary.get("positive_at_2x_costs")),
        "same_sign_halves": bool(summary.get("halves_same_sign")),
        "no_single_day_domination": (
            summary.get("single_day_share_of_pnl") is None
            or summary["single_day_share_of_pnl"]
            <= G["max_single_day_share_of_pnl"]),
        "no_single_leg_domination": (
            summary.get("max_single_leg_share") is None
            or summary["max_single_leg_share"]
            <= G["max_single_leg_share_of_pnl"]),
        "no_pit_violation": bool(pit_ok),
        "no_retune_since_freeze": bool(retune_free),
    }
    passed = all(checks.values())

    R = C.FORWARD_REJECTION_RULES
    rejected = False
    reject_reason = None
    if not pit_ok:
        rejected, reject_reason = True, "PIT violation"
    elif not retune_free:
        rejected, reject_reason = True, "specification hash changed in place"
    elif raw >= R["min_raw_matured_before_rejection"]:
        if t is not None and t <= R["reject_if_net_t_below"]:
            rejected = True
            reject_reason = ("net alpha significantly NEGATIVE against the "
                             "control (t %.2f)" % t)
        elif (mean_bps is not None
              and mean_bps <= R["reject_if_net_edge_below_bps_after_min_raw"]):
            rejected = True
            reject_reason = ("net edge %.2f bps per decision after %d matured "
                             "observations" % (mean_bps, raw))

    if rejected:
        state = C.FORWARD_REJECTED
    elif passed:
        state = C.FORWARD_CONFIRMED
    elif raw == 0:
        state = C.FORWARD_PENDING
    elif eff >= need_eff * C.CANDIDATE_MIN_EFFECTIVE_SHARE and \
            checks["positive_net_edge"] and \
            (t is not None and t > 0):
        state = C.FORWARD_CANDIDATE
    elif raw >= C.EARLY_EVIDENCE_MIN_MATURED:
        state = C.EARLY_FORWARD_EVIDENCE
    else:
        state = C.FORWARD_PENDING

    return {
        "state": state,
        "checks": checks,
        "all_checks_passed": passed,
        "rejected": rejected,
        "reject_reason": reject_reason,
        "required_effective_independent": need_eff,
        "required_raw_matured": need_raw,
        "required_calendar_days": need_days,
        "shortfall_effective": max(0, need_eff - eff),
        "shortfall_raw": max(0, need_raw - raw),
        "gate_owner": CALCULATION_OWNER,
        "proven_alpha_is_not_a_state": True,
    }


def benjamini_hochberg(pvalues: list, fdr: float = 0.10) -> dict:
    """FDR control across every challenger-horizon cell on the board."""
    pairs = sorted((p, i) for i, p in enumerate(pvalues)
                   if p is not None and math.isfinite(p))
    m = len(pairs)
    if not m:
        return {"n_tests": 0, "n_survivors": 0, "threshold": None,
                "survivors": []}
    survivors, thresh = [], None
    for rank, (p, i) in enumerate(pairs, start=1):
        if p <= fdr * rank / m:
            thresh = p
            survivors = [j for q, j in pairs[:rank]]
    return {"n_tests": m, "n_survivors": len(survivors),
            "threshold": thresh, "survivors": survivors, "fdr": fdr}
