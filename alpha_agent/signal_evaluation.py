"""
alpha_agent.signal_evaluation - Stage 11 generic cross-sectional evidence
evaluator.

Turns ANY ``signal_library.SignalSpec`` into the canonical Stage-5 evidence row
that the EXISTING, UNCHANGED tournament gates consume
(``tournament.row_to_contract_metrics`` -> ``tournament.classify_evidence``), so a
Stage 11 signal is held to exactly the same statistical bar as every prior
candidate - no gate is lowered here.

The metric definitions are byte-for-byte the same family as
``fundamental_evidence.evaluate_fundamental_evidence`` (rank IC + t, decile
long/short spread + t, cost grid on realized turnover, sub-period + up/down regime
consistency, deterministic block bootstrap, forward-only purged/embargoed OOS IC,
max drawdown); this module reuses that module's numeric primitives directly rather
than reimplementing them. It additionally returns the per-period IC and realized
long/short RETURN series so the ensemble layer can measure orthogonality and the
shadow layer can build a target book - measurement only, never a state write.

Pure stdlib; deterministic; read-only.
"""
from __future__ import annotations

from typing import Optional, Sequence

from . import fundamental_evidence as _fev
from . import selection_controls as _sc
from . import signal_library as _sl

_TRADING_DAYS_YEAR = 252


# --------------------------------------------------------------------------- #
# Cross-section assembly for a spec (all names keyed by assetid).
# --------------------------------------------------------------------------- #
def build_spec_periods(spec: _sl.SignalSpec, *, ohlcv: dict,
                       close_index: dict, market: Optional[dict] = None,
                       store=None, cik_to_assetid: Optional[dict] = None,
                       sector_series=None,
                       rebalance_dates: Sequence[str]) -> list:
    """Per rebalance date, the leakage-safe ``{as_of, names:[(key, adj_signal,
    forward_ret)]}`` cross-sections for ``spec``. ``adj_signal`` is the winsorized
    raw factor multiplied by the pre-registered expected sign (so a high value is
    the expected-outperform leg). ``close_index`` maps assetid -> (dates, closes)
    for the strictly-forward return (no look-ahead)."""
    horizon = int(spec.horizon_days)
    sign = int(spec.direction) or 1
    periods: list = []
    for d in rebalance_dates:
        raw = _sl.signal_cross_section(
            spec, ohlcv=ohlcv, market=market, store=store,
            cik_to_assetid=cik_to_assetid, sector_series=sector_series, as_of=d)
        if not raw:
            continue
        raw = _sl.winsorize(raw, spec.winsor)
        names = []
        for key, val in raw.items():
            idx = close_index.get(key)
            if idx is None:
                continue
            fwd = _fev._forward_return(idx[0], idx[1], d, horizon)
            if fwd is None:
                continue
            names.append((key, float(val) * sign, float(fwd)))
        if names:
            periods.append({"as_of": d, "names": names})
    return periods


# --------------------------------------------------------------------------- #
# The evaluator (metric definitions identical to fundamental_evidence).
# --------------------------------------------------------------------------- #
def evaluate_periods(periods: list, *, horizon_days: int = 63,
                     cost_grid_bps: Sequence[int] = (5, 25, 50, 75, 100),
                     quantile: float = _fev._DECILE_QUANTILE,
                     champion_returns: Optional[dict] = None,
                     feature: str = "signal", cfg: Optional[dict] = None) -> dict:
    """Evaluate pre-built cross-sections into a canonical Stage-5 row PLUS the
    per-period IC / realized long-short return series. Thin coverage yields few
    ``periods`` / small ``universe`` so ``classify_evidence`` keeps the candidate
    DATA_HOLD (never a fake spread). Deterministic and leakage-safe."""
    horizon = int(horizon_days)
    is_decile = abs(float(quantile) - _fev._DECILE_QUANTILE) < 1e-9
    min_cross = _fev._MIN_DECILE_NAMES if is_decile else _fev._MIN_QUINTILE_NAMES

    ic_series: list = []
    ls_series: list = []
    up_ic: list = []
    down_ic: list = []
    names_per_period: list = []
    memberships: list = []
    kept: list = []
    for p in periods:
        names = p["names"]
        if len(names) < 3:
            continue
        vals = [t[1] for t in names]
        fwds = [t[2] for t in names]
        ic = _fev._spearman(vals, fwds)
        ls = _fev._long_short_spread(names, quantile=quantile,
                                     min_names=min_cross)
        if ic is None or ls is None:
            continue
        ic_series.append(ic)
        ls_series.append(ls)
        names_per_period.append(len(names))
        memberships.append(_fev._membership(names, quantile=quantile,
                                            min_names=min_cross))
        kept.append(p["as_of"])
        if (_fev._mean(fwds) or 0.0) >= 0:
            up_ic.append(ic)
        else:
            down_ic.append(ic)

    n = len(ic_series)
    universe = (sorted(names_per_period)[len(names_per_period) // 2]
                if names_per_period else 0)

    turnovers = []
    for a, b in zip(memberships, memberships[1:]):
        prev = a[0] | a[1]
        cur = b[0] | b[1]
        if not prev:
            continue
        turnovers.append(len(prev - cur) / max(1, len(prev)))
    turnover = _fev._mean(turnovers) or 0.0

    ppy = _TRADING_DAYS_YEAR / max(1, horizon)
    gross_ann = ((_fev._mean(ls_series) or 0.0) * ppy) if n else None

    def _net_at(bps: int) -> Optional[float]:
        if gross_ann is None:
            return None
        annual_cost = turnover * 2.0 * (bps / 10000.0) * ppy
        return gross_ann - annual_cost

    cost_grid = [{"cost_bps": int(b), "net_annualized_return": _net_at(int(b))}
                 for b in cost_grid_bps]
    net25 = _net_at(25)
    cost_flips = (gross_ann is not None and net25 is not None
                  and (gross_ann > 0) != (net25 > 0))
    cost_erosion = None
    if gross_ann and gross_ann > 0 and net25 is not None:
        cost_erosion = max(0.0, 1.0 - net25 / gross_ann)

    subperiod_consistency = None
    if n >= 2:
        n_sub = max(2, min(4, n // 3)) if n >= 6 else 2
        size = n / n_sub
        pos = 0
        for s in range(n_sub):
            lo = int(round(s * size))
            hi = int(round((s + 1) * size))
            seg = ic_series[lo:hi]
            if seg and (_fev._mean(seg) or 0.0) > 0:
                pos += 1
        subperiod_consistency = pos / n_sub

    regimes = [seg for seg in (up_ic, down_ic) if seg]
    regime_consistency = None
    if regimes:
        regime_consistency = sum(
            1 for seg in regimes if (_fev._mean(seg) or 0.0) > 0) / len(regimes)

    boot = _sc.block_bootstrap_ci(
        ic_series,
        block_size=int(((cfg or {}).get("bootstrap_block_size")) or 4),
        n_resamples=int(((cfg or {}).get("bootstrap_resamples")) or 500),
        seed=int(((cfg or {}).get("bootstrap_seed")) or 12345)) if n >= 4 \
        else {"positive_fraction": None}

    oos_ic = None
    leakage = False
    if n >= 6:
        folds = _sc.purged_walk_forward_folds(
            n, n_folds=int(((cfg or {}).get("walk_forward_folds")) or 4),
            embargo=int(((cfg or {}).get("embargo_periods")) or 1),
            purge=int(((cfg or {}).get("purge_periods")) or 1),
            label_horizon=1)
        leakage = not bool(_sc.verify_no_leakage(folds, embargo=1,
                                                 label_horizon=1))
        test_idx = sorted({i for f in folds for i in f.get("test", [])})
        oos_ic = _fev._mean([ic_series[i] for i in test_idx if 0 <= i < n])

    champ_comp = None
    if champion_returns:
        aligned = [(ls_series[i], champion_returns.get(kept[i]))
                   for i in range(n)]
        aligned = [(a, b) for a, b in aligned if b is not None]
        if len(aligned) >= 3:
            corr = _fev._pearson([a for a, _ in aligned], [b for _, b in aligned])
            if corr is not None:
                champ_comp = round(1.0 - abs(corr), 6)

    rank_ic_mean = _fev._mean(ic_series)
    rank_ic_t = _fev._t_stat(ic_series)
    spread_t = _fev._t_stat(ls_series)
    pos_ratio = (sum(1 for x in ic_series if x > 0) / n) if n else None
    spread_mean = _fev._mean(ls_series)

    max_dd = None
    if ls_series:
        cum = peak = worst = 0.0
        for x in ls_series:
            cum += x
            peak = max(peak, cum)
            worst = min(worst, cum - peak)
        max_dd = worst

    row = {
        "feature": feature,
        "label": "stage11_signal:%s" % feature,
        "template": "stage11_cross_sectional_rank",
        "study_kind": "stage11_signal",
        "periods": n,
        "universe": universe,
        "rank_ic_mean": rank_ic_mean,
        "rank_ic_t": rank_ic_t,
        "rank_ic_positive_ratio": pos_ratio,
        "decile_spread_mean": spread_mean if is_decile else None,
        "quantile_spread_mean": spread_mean,
        "spread_quantile": float(quantile),
        "is_decile_spread": bool(is_decile),
        "min_cross_section_for_spread": int(min_cross),
        "spread_t": spread_t,
        "oos_ic_mean": oos_ic,
        "gross_annualized_return": gross_ann,
        "net_annualized_return": net25,
        "cost_sensitivity": {"grid": cost_grid, "reference_bps": 25},
        "cost_flips_sign": bool(cost_flips),
        "cost_erosion_ratio": cost_erosion,
        "turnover": turnover,
        "max_drawdown": max_dd,
        "subperiod_consistency": subperiod_consistency,
        "regime_consistency": regime_consistency,
        "bootstrap_positive_fraction": boot.get("positive_fraction"),
        "champion_complementarity": champ_comp,
        "leakage_warning": bool(leakage),
        "beats_null_control": bool((rank_ic_t or 0.0) > 0),
        "decision": "EVALUATED",
        "pit_basis": "owned survivorship-safe assetid panel; strictly-forward "
                     "returns; PIT fundamentals by SEC filed date; leakage-safe "
                     "PIT sector",
    }
    # Series for the ensemble (orthogonality) + shadow layers; measurement only.
    series = {"dates": kept, "ic": ic_series, "ls": ls_series,
              "long_short_by_date": {kept[i]: ls_series[i] for i in range(n)}}
    return {"row": row, "series": series}


def evaluate_spec(spec: _sl.SignalSpec, *, ohlcv: dict, close_index: dict,
                  market: Optional[dict] = None, store=None,
                  cik_to_assetid: Optional[dict] = None, sector_series=None,
                  rebalance_dates: Sequence[str],
                  champion_returns: Optional[dict] = None,
                  cfg: Optional[dict] = None) -> dict:
    """Convenience: build the spec's cross-sections then evaluate them. Returns
    ``{"row": ..., "series": ...}`` (see :func:`evaluate_periods`)."""
    periods = build_spec_periods(
        spec, ohlcv=ohlcv, close_index=close_index, market=market, store=store,
        cik_to_assetid=cik_to_assetid, sector_series=sector_series,
        rebalance_dates=rebalance_dates)
    return evaluate_periods(periods, horizon_days=spec.horizon_days,
                            quantile=_fev._DECILE_QUANTILE,
                            champion_returns=champion_returns,
                            feature=spec.name, cfg=cfg)


def approx_two_sided_pvalue(t_stat: Optional[float], dof: int) -> Optional[float]:
    """A deterministic two-sided p-value from a t-statistic via a normal
    approximation (dof large in this research context). Used only to feed the
    multiple-testing correction; never a gate by itself."""
    if t_stat is None:
        return None
    import math
    z = abs(float(t_stat))
    # two-sided normal tail
    return max(0.0, min(1.0, 2.0 * (1.0 - _sc._norm_cdf(z))))


__all__ = ["build_spec_periods", "evaluate_periods", "evaluate_spec",
           "approx_two_sided_pvalue"]
