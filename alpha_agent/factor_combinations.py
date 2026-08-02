"""
alpha_agent.factor_combinations - Stage 9.4 Track G: controlled, pre-registered
factor combinations with transparent weighted ranks.

A small, fixed set of two-leg combinations is pre-registered before evaluation.
Weights are NOT fitted - only three transparent weight sets (50/50, 67/33, 33/67)
are ever tested, and each leg is combined as a cross-sectionally standardized
rank. A combination is only evaluated when BOTH legs are computable from owned
data; if a leg needs data the owned close panel does not carry (fundamentals,
insider, earnings events, volume), the combination is an HONEST DATA_HOLD naming
the unavailable leg - never approximated.

Every combination reports the standalone component evidence, the component
correlation, the incremental information over the stronger leg, turnover, cost
sensitivity, drawdown and subperiod/regime consistency, so a combination is never
credited merely for restating a single leg.

Pure stdlib; deterministic; no state writes.
"""
from __future__ import annotations

from typing import Optional

from . import experiment_contracts as ec
from . import experiment_runner as er
from . import price_factors as pf

# Fixed, transparent weight sets - NO unrestricted fitting.
WEIGHT_SETS: tuple[tuple[float, float], ...] = (
    (0.50, 0.50), (0.67, 0.33), (0.33, 0.67))

COMBINATION_VERSION = "stage9_4-combinations-1.0.0"

# Held legs whose data the owned close panel does not carry (combination held).
_HELD_LEGS = {
    "gross_profitability": "owned point-in-time fundamentals insufficient",
    "net_insider_buys": "owned Form-4 insider coverage sparse",
    "earnings_8k_drift": "owned 8-K earnings-event dates insufficient",
    "volatility_compression_liquidity": "owned volume/turnover unavailable",
    "liquidity_confirmation": "owned volume/turnover unavailable",
}

# The pre-registered combinations (Track G). ``leg_a``/``leg_b`` are owned
# computable feature keys when available, else a held-leg token.
PRE_REGISTERED_COMBINATIONS: tuple[dict, ...] = (
    {"combo_id": "resmom_x_volcompression",
     "name": "Residual momentum + volatility compression",
     "leg_a": "market_residual_momentum", "leg_b": "short_long_vol_ratio",
     "hypothesis": "market-neutral trend confirmed by volatility compression"},
    {"combo_id": "trendquality_x_downsidebeta",
     "name": "Trend quality + downside beta",
     "leg_a": "trend_slope_t", "leg_b": "downside_beta",
     "hypothesis": "high-quality trends in low-downside-beta names"},
    {"combo_id": "breakout_x_liquidity",
     "name": "Breakout + liquidity confirmation",
     "leg_a": "channel_breakout", "leg_b": "liquidity_confirmation",
     "hypothesis": "breakouts confirmed by liquidity expansion"},
    {"combo_id": "resmom_x_grossprofitability",
     "name": "Residual momentum + gross profitability",
     "leg_a": "market_residual_momentum", "leg_b": "gross_profitability",
     "hypothesis": "trend in high-quality (profitable) names"},
    {"combo_id": "insider_x_resmom",
     "name": "Insider purchase + residual momentum",
     "leg_a": "net_insider_buys", "leg_b": "market_residual_momentum",
     "hypothesis": "insider conviction confirmed by market-neutral trend"},
    {"combo_id": "earningsdrift_x_resmom",
     "name": "Earnings-event drift + residual momentum",
     "leg_a": "earnings_8k_drift", "leg_b": "market_residual_momentum",
     "hypothesis": "post-earnings drift confirmed by trend"},
)


def _leg_computable(leg: str) -> bool:
    return leg in pf.COMPUTABLE_FEATURES


def _standardized_ranks(vals: list) -> list:
    """Cross-sectional standardized ranks (mean 0, unit sd); None preserved."""
    finite_idx = [i for i, v in enumerate(vals) if ec._is_num(v)]
    if len(finite_idx) < 3:
        return [None] * len(vals)
    r = ec.ranks([float(vals[i]) for i in finite_idx])
    m = sum(r) / len(r)
    sd = ec.stdev(r, sample=True)
    out: list = [None] * len(vals)
    if not sd:
        return out
    for pos, i in enumerate(finite_idx):
        out[i] = (r[pos] - m) / sd
    return out


def weighted_rank_blend(vals_a: list, vals_b: list, w_a: float, w_b: float
                        ) -> list:
    """Transparent weighted-rank blend: ``w_a*z(rank(a)) + w_b*z(rank(b))`` per
    name (cross-sectional). A name is None unless BOTH legs are present."""
    za = _standardized_ranks(vals_a)
    zb = _standardized_ranks(vals_b)
    out: list = []
    for a, b in zip(za, zb):
        out.append((w_a * a + w_b * b) if (a is not None and b is not None)
                   else None)
    return out


def _build_combo_cross_sections(panel: dict, *, feature_a: str, feature_b: str,
                                w_a: float, w_b: float, horizon_days: int,
                                rebalance: str) -> dict:
    """Build blended-factor cross-sections + component cross-sections for
    correlation/incremental analysis. Leakage-safe (both legs use data through
    the formation date)."""
    step = pf._STEP_DAYS.get(rebalance, 21)
    mret = pf.build_market_return_series(panel)
    all_dates = sorted({d for series in panel.values() for d, _ in series})
    tk_close = {t: [c for _, c in sorted(set(s), key=lambda p: p[0])]
                for t, s in panel.items()}
    tk_dates = {t: [d for d, _ in sorted(set(s), key=lambda p: p[0])]
                for t, s in panel.items()}
    tk_index = {t: {d: i for i, d in enumerate(tk_dates[t])} for t in panel}

    combo_cs: list[tuple] = []
    leg_a_cs: list[tuple] = []
    leg_b_cs: list[tuple] = []
    port: list[float] = []
    turns: list[float] = []
    bench: list[float] = []
    corr_pairs_a: list[float] = []
    corr_pairs_b: list[float] = []
    prev_book: dict[str, float] = {}
    observations = 0

    for fi in range(pf._MAX_LOOKBACK, len(all_dates), step):
        fd = all_dates[fi]
        a_vals, b_vals, fwds, names = [], [], [], []
        for t, closes in tk_close.items():
            idx = tk_index[t].get(fd)
            if idx is None:
                continue
            fidx = idx + horizon_days
            if fidx >= len(closes):
                continue
            base = closes[idx]
            if base is None or base <= 0:
                continue
            va = pf.factor_value(feature_a, name_dates=tk_dates[t],
                                 name_closes=closes, idx=idx, mret_by_date=mret)
            vb = pf.factor_value(feature_b, name_dates=tk_dates[t],
                                 name_closes=closes, idx=idx, mret_by_date=mret)
            if va is None or vb is None:
                continue
            a_vals.append(float(va))
            b_vals.append(float(vb))
            fwds.append(closes[fidx] / base - 1.0)
            names.append(t)
        if len(a_vals) < 4:
            continue
        blended = weighted_rank_blend(a_vals, b_vals, w_a, w_b)
        sel = [(blended[i], fwds[i], names[i]) for i in range(len(blended))
               if blended[i] is not None]
        if len(sel) < 4:
            continue
        bf = [s[0] for s in sel]
        bfw = [s[1] for s in sel]
        bn = [s[2] for s in sel]
        combo_cs.append((bf, bfw))
        leg_a_cs.append((a_vals, fwds))
        leg_b_cs.append((b_vals, fwds))
        corr_pairs_a.extend(a_vals)
        corr_pairs_b.extend(b_vals)
        observations += len(bf)
        bench.append(sum(bfw) / len(bfw))
        order = sorted(range(len(bf)), key=lambda i: bf[i])
        size = max(1, len(order) // 10)
        top = order[-size:]
        book = {bn[i]: 1.0 / size for i in top}
        port.append(sum(bfw[i] for i in top) / size)
        turns.append(ec.turnover(prev_book, book))
        prev_book = book

    universe = (sorted(len(cs[0]) for cs in combo_cs)[len(combo_cs) // 2]
                if combo_cs else 0)
    return {"cross_sections": combo_cs, "leg_a_cs": leg_a_cs,
            "leg_b_cs": leg_b_cs, "portfolio_returns": port, "turnovers": turns,
            "benchmark_returns": bench, "observations": observations,
            "periods": len(combo_cs), "universe": universe,
            "corr_a": corr_pairs_a, "corr_b": corr_pairs_b}


def evaluate_combination(panel: dict, combo: dict, *, horizon_days: int = 21,
                         rebalance: str = "monthly", cost_grid=None,
                         min_periods: int = 12) -> dict:
    """Evaluate one pre-registered combination across the three fixed weight
    sets. Returns a report with, per weight set, the blended metrics, the
    standalone component IC/spread, the component correlation, and the
    incremental IC/spread over the stronger leg. A held leg yields an honest
    DATA_HOLD naming the blocker."""
    a, b = combo["leg_a"], combo["leg_b"]
    if not _leg_computable(a) or not _leg_computable(b):
        held = a if not _leg_computable(a) else b
        return {"combo_id": combo["combo_id"], "name": combo["name"],
                "status": "DATA_HOLD", "held_leg": held,
                "blocker": "DATA_HOLD_COMBINATION_LEG_UNAVAILABLE",
                "detail": _HELD_LEGS.get(held, "leg data unavailable"),
                "weight_sets_evaluated": []}
    cost_grid = cost_grid or [5, 10, 25, 50]
    ppy = ec.periods_per_year_for(rebalance)
    per_weight: list[dict] = []
    for (wa, wb) in WEIGHT_SETS:
        built = _build_combo_cross_sections(
            panel, feature_a=a, feature_b=b, w_a=wa, w_b=wb,
            horizon_days=horizon_days, rebalance=rebalance)
        if built["periods"] < min_periods:
            per_weight.append({"weights": [wa, wb], "status": "NEED_MORE_DATA",
                               "periods": built["periods"]})
            continue
        spec = {"template": "combined_factor_ranking", "feature": combo["combo_id"],
                "study_kind": "cross_sectional_rank", "horizon_days": horizon_days,
                "rebalance": rebalance, "benchmark": "equal_weight_universe",
                "transaction_cost_bps": 10.0}
        m = er.score_experiment(spec, built, gates=ec.DEFAULT_GATES,
                                cost_grid=cost_grid, periods_per_year=ppy,
                                missing_data_rate=0.0)
        # Component standalone evidence.
        a_ic = ec.tstat(ec.rank_ic_series(built["leg_a_cs"]))
        b_ic = ec.tstat(ec.rank_ic_series(built["leg_b_cs"]))
        combo_ic = m.get("rank_ic_t")
        comp_corr = ec.pearson(built["corr_a"], built["corr_b"]) \
            if len(built["corr_a"]) == len(built["corr_b"]) else None
        stronger = max([x for x in (a_ic, b_ic) if x is not None], default=None)
        incremental_ic_t = (combo_ic - stronger) if (
            combo_ic is not None and stronger is not None) else None
        per_weight.append({
            "weights": [wa, wb], "status": "SCORED",
            "periods": built["periods"], "universe": built["universe"],
            "rank_ic_t": combo_ic, "spread_t": m.get("spread_t"),
            "net_annualized_return": m.get("net_annualized_return"),
            "turnover": m.get("turnover"), "max_drawdown": m.get("max_drawdown"),
            "subperiod_consistency": m.get("subperiod_consistency"),
            "regime_consistency": m.get("regime_consistency"),
            "cost_flips_sign": m.get("cost_flips_sign"),
            "component_leg_a_ic_t": a_ic, "component_leg_b_ic_t": b_ic,
            "component_correlation": comp_corr,
            "incremental_ic_t_over_stronger_leg": incremental_ic_t,
            "decision": ec.evaluate_evidence(m, ec.DEFAULT_GATES)["decision"]})
    return {"combo_id": combo["combo_id"], "name": combo["name"],
            "hypothesis": combo["hypothesis"], "status": "EVALUATED",
            "leg_a": a, "leg_b": b, "weight_sets_evaluated": per_weight,
            "combination_version": COMBINATION_VERSION}


def evaluate_all(panel: dict, *, horizon_days: int = 21,
                 rebalance: str = "monthly", min_periods: int = 12) -> list[dict]:
    return [evaluate_combination(panel, c, horizon_days=horizon_days,
                                 rebalance=rebalance, min_periods=min_periods)
            for c in PRE_REGISTERED_COMBINATIONS]


def combinations_summary() -> dict:
    computable = [c for c in PRE_REGISTERED_COMBINATIONS
                  if _leg_computable(c["leg_a"]) and _leg_computable(c["leg_b"])]
    return {"combination_version": COMBINATION_VERSION,
            "weight_sets": [list(w) for w in WEIGHT_SETS],
            "total_combinations": len(PRE_REGISTERED_COMBINATIONS),
            "both_legs_computable": len(computable),
            "data_hold_combinations":
                len(PRE_REGISTERED_COMBINATIONS) - len(computable)}


__all__ = ["WEIGHT_SETS", "COMBINATION_VERSION", "PRE_REGISTERED_COMBINATIONS",
           "weighted_rank_blend", "evaluate_combination", "evaluate_all",
           "combinations_summary"]
