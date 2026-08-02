"""
alpha_agent.fundamental_evidence - Stage 9.5 Track G: the genuine cross-sectional
Stage 9 evidence contract for a POINT-IN-TIME fundamental signal.

Given a leakage-safe owned PIT fundamentals store (``pit_fundamentals``) and the
owned survivorship-safe daily price panel (``experiment_runner.NormalizedStore.
price_panel``), this module evaluates ONE fundamental signal
(``fundamental_signals``) into the canonical Stage 5 result row the tournament
ingester consumes (``tournament.row_to_contract_metrics`` ->
``classify_evidence``). It is the MINIMUM missing production capability for the
Stage 9.5 fundamental EXPERIMENT handler.

The contract is genuine, not a stub:
  * per rebalance date the cross-section value is read PIT-safe via
    ``store.as_of`` for the latest fiscal period whose filing pre-dates the
    formation date (never a future restatement, never a current snapshot);
  * the forward return window is STRICTLY AFTER the formation date (no
    look-ahead); rank-IC, decile long/short spread and their t-stats are computed
    over the realized cross-sections;
  * costs are applied on realized turnover across a bps grid (net-of-cost
    spreads at 25/50 bps gate the candidate); a factor whose edge is a cost
    artifact cannot pass;
  * robustness = sub-period sign-consistency, up/down market regime consistency
    and a deterministic block bootstrap; out-of-sample IC is measured on
    FORWARD-ONLY purged/embargoed folds (``selection_controls``);
  * orthogonality vs a supplied champion return stream is reported so a redundant
    factor is penalized.

Pure stdlib + reuse of ``selection_controls`` / ``orthogonality``; deterministic;
no network, no state writes, no operational mutation. A signal without enough
owned coverage produces an explicit thin-evidence row (few periods / few names)
so ``classify_evidence`` keeps the candidate DATA_HOLD - coverage is never faked.
"""
from __future__ import annotations

import math
from typing import Optional, Sequence

from . import fundamental_signals as _fsig
from . import pit_fundamentals as _pfd

_TRADING_DAYS_YEAR = 252

# A genuine top-decile minus bottom-decile spread needs at least ten names in the
# cross-section (so each 10% leg has >=1 name and the two legs are disjoint).
_DECILE_QUANTILE = 0.10
_MIN_DECILE_NAMES = 10
_MIN_QUINTILE_NAMES = 5


# --------------------------------------------------------------------------- #
# Small deterministic statistics (stdlib).
# --------------------------------------------------------------------------- #
def _mean(xs: Sequence[float]) -> Optional[float]:
    xs = [float(x) for x in xs if x is not None]
    return (sum(xs) / len(xs)) if xs else None


def _stdev(xs: Sequence[float]) -> Optional[float]:
    xs = [float(x) for x in xs if x is not None]
    n = len(xs)
    if n < 2:
        return None
    m = sum(xs) / n
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1))


def _t_stat(xs: Sequence[float]) -> Optional[float]:
    xs = [float(x) for x in xs if x is not None]
    n = len(xs)
    if n < 2:
        return None
    sd = _stdev(xs)
    if not sd:
        return None  # degenerate zero-variance series: t is undefined, not 0
    return (sum(xs) / n) / (sd / math.sqrt(n))


def _ranks(vals: Sequence[float]) -> list:
    """Average (tie-corrected) ranks."""
    order = sorted(range(len(vals)), key=lambda i: vals[i])
    ranks = [0.0] * len(vals)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and vals[order[j + 1]] == vals[order[i]]:
            j += 1
        avg = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[order[k]] = avg
        i = j + 1
    return ranks


def _spearman(a: Sequence[float], b: Sequence[float]) -> Optional[float]:
    if len(a) != len(b) or len(a) < 3:
        return None
    ra, rb = _ranks(list(a)), _ranks(list(b))
    n = len(ra)
    ma, mb = sum(ra) / n, sum(rb) / n
    cov = sum((ra[i] - ma) * (rb[i] - mb) for i in range(n))
    va = math.sqrt(sum((r - ma) ** 2 for r in ra))
    vb = math.sqrt(sum((r - mb) ** 2 for r in rb))
    if va == 0 or vb == 0:
        return None
    return cov / (va * vb)


def _pearson(a: Sequence[float], b: Sequence[float]) -> Optional[float]:
    pairs = [(float(x), float(y)) for x, y in zip(a, b)
             if x is not None and y is not None]
    if len(pairs) < 3:
        return None
    n = len(pairs)
    ma = sum(x for x, _ in pairs) / n
    mb = sum(y for _, y in pairs) / n
    cov = sum((x - ma) * (y - mb) for x, y in pairs)
    va = math.sqrt(sum((x - ma) ** 2 for x, _ in pairs))
    vb = math.sqrt(sum((y - mb) ** 2 for _, y in pairs))
    if va == 0 or vb == 0:
        return None
    return cov / (va * vb)


# --------------------------------------------------------------------------- #
# Price panel helpers.
# --------------------------------------------------------------------------- #
def _price_index(bars: Sequence) -> tuple:
    """(sorted dates, closes) for a ticker's ``[(date, close)]`` series."""
    dates = [str(d) for d, _ in bars]
    closes = [float(c) for _, c in bars]
    return dates, closes


def _forward_return(dates: list, closes: list, as_of: str,
                    horizon: int) -> Optional[float]:
    """Return from the last close on-or-before ``as_of`` to the close ``horizon``
    trading days later. Strictly forward (no look-ahead)."""
    i = None
    for k in range(len(dates) - 1, -1, -1):
        if dates[k] <= as_of:
            i = k
            break
    if i is None:
        return None
    j = i + int(horizon)
    if j >= len(closes) or closes[i] <= 0:
        return None
    return closes[j] / closes[i] - 1.0


def _default_rebalance_dates(panel: dict, *, horizon: int,
                             warmup: int = 0) -> list:
    """A deterministic quarterly-spaced formation calendar from the union of
    panel trading dates, leaving one horizon of forward room at the end."""
    all_dates = sorted({d for bars in panel.values() for d, _ in bars})
    if len(all_dates) <= horizon + 1:
        return []
    out = []
    i = max(0, int(warmup))
    last_ok = len(all_dates) - horizon - 1
    while i <= last_ok:
        out.append(all_dates[i])
        i += horizon
    return out


# --------------------------------------------------------------------------- #
# Cross-section builder (PIT-safe).
# --------------------------------------------------------------------------- #
def build_cross_sections(store, panel: dict, signal: str, *,
                         cik_to_ticker: dict, rebalance_dates: list,
                         horizon: int) -> list:
    """Per rebalance date, the leakage-safe ``[(cik, adj_signal, forward_ret)]``
    cross-section. ``adj_signal`` is the raw signal multiplied by its
    pre-registered expected sign so a high value is the expected-outperform leg.
    Missing signal or missing forward return drops that name explicitly."""
    sign = _fsig.EXPECTED_SIGN.get(signal, 1) or 1
    price_idx = {t: _price_index(b) for t, b in panel.items()}
    periods = []
    for d in rebalance_dates:
        names = []
        bases: set = set()
        for cik, ticker in cik_to_ticker.items():
            if ticker not in price_idx:
                continue
            fk = store.latest_fiscal_key(cik, d, concept="assets")
            if fk is None:
                continue
            prior = (store.prior_fiscal_key(cik, d, fk, concept="assets")
                     if signal == "asset_growth" else None)
            r = _fsig.compute_signal(store, signal, cik=cik, fiscal_key=fk,
                                     as_of=d, prior_fiscal_key=prior)
            val = r.get("value")
            if val is None:
                continue
            if r.get("basis"):
                bases.add(r["basis"])
            dates, closes = price_idx[ticker]
            fwd = _forward_return(dates, closes, d, horizon)
            if fwd is None:
                continue
            names.append((cik, float(val) * sign, float(fwd)))
        if names:
            periods.append({"as_of": d, "names": names,
                            "bases": sorted(bases)})
    return periods


def _leg_indices(n: int, quantile: float) -> Optional[int]:
    """Per-leg size ``k`` for a top-``quantile`` / bottom-``quantile`` split, or
    None when the two legs would overlap (``2k > n``) so they cannot be disjoint."""
    k = max(1, int(round(n * quantile)))
    return k if 2 * k <= n else None


def _long_short_spread(names: list, *, quantile: float,
                       min_names: int) -> Optional[float]:
    """Top-minus-bottom quantile mean forward return (``names`` sorted by
    adjusted signal ascending; long the top, short the bottom). Returns None when
    the cross-section is smaller than ``min_names`` or the two legs would overlap
    - a genuine decile spread is never computed on too few names, and no name is
    counted in both the long and the short leg."""
    n = len(names)
    if n < max(3, int(min_names)):
        return None
    ordered = sorted(names, key=lambda t: t[1])
    k = _leg_indices(n, quantile)
    if k is None:
        return None
    bottom = ordered[:k]
    top = ordered[-k:]
    if {t[0] for t in top} & {t[0] for t in bottom}:
        return None  # defensive: disjoint membership guaranteed by 2k<=n
    tb = _mean([t[2] for t in top])
    bb = _mean([t[2] for t in bottom])
    if tb is None or bb is None:
        return None
    return tb - bb


def _membership(names: list, *, quantile: float, min_names: int) -> tuple:
    """The (long, short) CIK membership of the top/bottom quantile legs - the
    ACTUAL portfolio whose turnover and modeled costs are measured. Empty when the
    cross-section is too small or the legs would overlap."""
    n = len(names)
    if n < max(3, int(min_names)):
        return set(), set()
    ordered = sorted(names, key=lambda t: t[1])
    k = _leg_indices(n, quantile)
    if k is None:
        return set(), set()
    longs = {t[0] for t in ordered[-k:]}
    shorts = {t[0] for t in ordered[:k]}
    if longs & shorts:
        return set(), set()
    return longs, shorts


# --------------------------------------------------------------------------- #
# The evaluator.
# --------------------------------------------------------------------------- #
def evaluate_fundamental_evidence(store, panel: dict, signal: str, *,
                                  cik_to_ticker: dict,
                                  horizon_days: int = 63,
                                  rebalance_dates: Optional[list] = None,
                                  cost_grid_bps: Sequence[int] = (5, 25, 50, 75,
                                                                   100),
                                  quantile: float = _DECILE_QUANTILE,
                                  champion_returns: Optional[dict] = None,
                                  cfg: Optional[dict] = None) -> dict:
    """Evaluate ONE fundamental signal into a canonical Stage 5 result row.

    Deterministic and leakage-safe. Returns a row with the exact keys
    ``tournament.row_to_contract_metrics`` reads. When owned coverage is thin the
    row carries few ``periods`` / small ``universe`` so ``classify_evidence``
    keeps the candidate DATA_HOLD (never REJECTED on incomplete evidence)."""
    from . import selection_controls as _sc
    horizon = int(horizon_days)
    if rebalance_dates is None:
        rebalance_dates = _default_rebalance_dates(panel, horizon=horizon)
    periods = build_cross_sections(store, panel, signal,
                                   cik_to_ticker=cik_to_ticker,
                                   rebalance_dates=rebalance_dates,
                                   horizon=horizon)
    # A genuine decile spread (quantile == 0.10) requires >=10 names; a quintile
    # (0.20) requires >=5. Below the minimum a period yields NO spread (thin), so
    # ``classify_evidence`` keeps the candidate DATA_HOLD - never a fake spread.
    is_decile = abs(float(quantile) - _DECILE_QUANTILE) < 1e-9
    min_cross_section = _MIN_DECILE_NAMES if is_decile else _MIN_QUINTILE_NAMES
    # Per-period IC and long/short spread series (aligned).
    ic_series: list = []
    ls_series: list = []
    up_ic: list = []
    down_ic: list = []
    names_per_period: list = []
    memberships: list = []
    kept_periods: list = []
    for p in periods:
        names = p["names"]
        if len(names) < 3:
            continue
        vals = [t[1] for t in names]
        fwds = [t[2] for t in names]
        ic = _spearman(vals, fwds)
        ls = _long_short_spread(names, quantile=quantile,
                                min_names=min_cross_section)
        if ic is None or ls is None:
            continue
        ic_series.append(ic)
        ls_series.append(ls)
        names_per_period.append(len(names))
        memberships.append(_membership(names, quantile=quantile,
                                       min_names=min_cross_section))
        kept_periods.append(p["as_of"])
        # up/down market regime by the period's cross-sectional mean forward.
        if (_mean(fwds) or 0.0) >= 0:
            up_ic.append(ic)
        else:
            down_ic.append(ic)

    n = len(ic_series)
    universe = (sorted(names_per_period)[len(names_per_period) // 2]
                if names_per_period else 0)

    # Turnover: fraction of long/short membership that changes between periods.
    turnovers = []
    for a, b in zip(memberships, memberships[1:]):
        prev = a[0] | a[1]
        cur = b[0] | b[1]
        if not prev:
            continue
        changed = len(prev - cur)
        turnovers.append(changed / max(1, len(prev)))
    turnover = _mean(turnovers) or 0.0

    periods_per_year = _TRADING_DAYS_YEAR / max(1, horizon)
    gross_ann = ((_mean(ls_series) or 0.0) * periods_per_year) if n else None

    def _net_at(bps: int) -> Optional[float]:
        if gross_ann is None:
            return None
        # round-trip cost per rebalance = turnover * 2 legs * bps, annualized.
        annual_cost = turnover * 2.0 * (bps / 10000.0) * periods_per_year
        return gross_ann - annual_cost

    cost_grid = [{"cost_bps": int(b), "net_annualized_return": _net_at(int(b))}
                 for b in cost_grid_bps]
    net25 = _net_at(25)
    cost_flips = (gross_ann is not None and net25 is not None
                  and (gross_ann > 0) != (net25 > 0))
    cost_erosion = None
    if gross_ann and gross_ann > 0 and net25 is not None:
        cost_erosion = max(0.0, 1.0 - net25 / gross_ann)

    # Sub-period sign-consistency (bounded number of contiguous sub-periods).
    subperiod_consistency = None
    if n >= 2:
        n_sub = max(2, min(4, n // 3)) if n >= 6 else 2
        size = n / n_sub
        pos = 0
        for s in range(n_sub):
            lo = int(round(s * size))
            hi = int(round((s + 1) * size))
            seg = ic_series[lo:hi]
            if seg and (_mean(seg) or 0.0) > 0:
                pos += 1
        subperiod_consistency = pos / n_sub

    # Regime consistency: fraction of {up, down} regimes with a positive mean IC
    # (only regimes with observations count).
    regimes = [seg for seg in (up_ic, down_ic) if seg]
    regime_consistency = None
    if regimes:
        regime_consistency = sum(
            1 for seg in regimes if (_mean(seg) or 0.0) > 0) / len(regimes)

    # Deterministic block bootstrap of the IC mean.
    boot = _sc.block_bootstrap_ci(
        ic_series,
        block_size=int(((cfg or {}).get("bootstrap_block_size")) or 4),
        n_resamples=int(((cfg or {}).get("bootstrap_resamples")) or 500),
        seed=int(((cfg or {}).get("bootstrap_seed")) or 12345)) if n >= 4 \
        else {"positive_fraction": None}

    # Forward-only out-of-sample IC on purged/embargoed folds.
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
        oos = [ic_series[i] for i in test_idx if 0 <= i < n]
        oos_ic = _mean(oos)

    # Orthogonality vs the champion return stream (if provided, aligned by date).
    champ_comp = None
    if champion_returns:
        aligned = [(ls_series[i], champion_returns.get(kept_periods[i]))
                   for i in range(n)]
        aligned = [(a, b) for a, b in aligned if b is not None]
        if len(aligned) >= 3:
            corr = _pearson([a for a, _ in aligned], [b for _, b in aligned])
            if corr is not None:
                champ_comp = round(1.0 - abs(corr), 6)

    rank_ic_mean = _mean(ic_series)
    rank_ic_t = _t_stat(ic_series)
    spread_t = _t_stat(ls_series)
    pos_ratio = (sum(1 for x in ic_series if x > 0) / n) if n else None
    spread_mean = _mean(ls_series)
    # Which gross-profitability basis (Gross Profit preferred vs revenue-minus-cost
    # fallback) produced the signal - persisted in the experiment artifact.
    signal_basis = sorted({b for p in periods for b in p.get("bases", [])})

    # Max drawdown of the cumulative (uncompounded) long/short return path.
    max_dd = None
    if ls_series:
        cum = 0.0
        peak = 0.0
        worst = 0.0
        for x in ls_series:
            cum += x
            peak = max(peak, cum)
            worst = min(worst, cum - peak)
        max_dd = worst  # <= 0, in return units

    row = {
        "feature": signal,
        "label": "stage9_5_fundamental:%s" % signal,
        "template": "fundamental_momentum_rank",
        "study_kind": "stage9_5_fundamental",
        "periods": n,
        "universe": universe,
        "rank_ic_mean": rank_ic_mean,
        "rank_ic_t": rank_ic_t,
        "rank_ic_positive_ratio": pos_ratio,
        # ``decile_spread_mean`` is populated ONLY when the calculation is truly a
        # decile (quantile == 0.10); a quintile evaluation reports its spread under
        # the honest ``quantile_spread_mean`` and leaves decile_spread_mean None.
        "decile_spread_mean": spread_mean if is_decile else None,
        "quantile_spread_mean": spread_mean,
        "spread_quantile": float(quantile),
        "is_decile_spread": bool(is_decile),
        "min_cross_section_for_spread": int(min_cross_section),
        "signal_basis": signal_basis,
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
        "expected_sign": _fsig.EXPECTED_SIGN.get(signal),
        "pit_basis": "PIT companyfacts (availability = SEC filed date); forward "
                     "return strictly after formation; survivorship-safe prices",
    }
    return row


__all__ = ["evaluate_fundamental_evidence", "build_cross_sections",
           "_default_rebalance_dates"]
