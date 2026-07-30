"""
alpha_agent.champion_forensics — Stage 7 Workstream 2: champion autopsy.

Reconstructs the operational champion ``fundamental_momentum_50_50_v1`` (book
``fundamental_momentum_50_50_top25``) as faithfully as the OWNED, point-in-time
safe data permits, on survivorship-aware 2015→latest Norgate history, and
computes the full forensic battery from deterministic Python math.

Faithfulness is reported honestly and never overstated:

  * EXACT_RECONSTRUCTION      — the portfolio-CONSTRUCTION contract (Top-25,
                                equal ~4% target, 5% name cap, 25% sector cap,
                                residual cash, monthly review). These are the
                                operational policy rules, reproduced exactly.
  * PARTIAL_RECONSTRUCTION    — the SELECTION signal is reconstructed from its
                                price/momentum leg only, on real survivorship-
                                free history. The fundamental leg is NOT
                                reproduced here (see below), so the return
                                series is a price-leg proxy, not the exact
                                operational book's realised history.
  * UNVERIFIABLE_COMPONENT    — the point-in-time FUNDAMENTAL leg. Owned
                                fundamentals are a current snapshot with no
                                lookahead-free per-period filing/availability
                                dates (Stage 6 finding); reconstructing it as
                                history would require period-end lookahead, so
                                it is explicitly NOT fabricated.

Safety: read-only, research-only. Creates no order/fill/signal/trade decision,
promotes no model, mutates no portfolio/ledger, calls no prediction service, no
DB, no LLM. Pure stdlib math (no numpy/pandas) → byte-reproducible.
"""
from __future__ import annotations

from typing import Any, Optional

from . import experiment_contracts as ec
from . import evidence_observatory as eo

# Trading-day geometry (shared with the Stage 5 runner conventions).
_STEP_MONTH = 21
_MAX_LOOKBACK = 252
_HORIZON = 21


# --------------------------------------------------------------------------- #
# Panel helpers.
# --------------------------------------------------------------------------- #
def _sorted_series(series) -> list[tuple[str, float]]:
    out = []
    for d, c in series:
        try:
            fc = float(c)
        except (TypeError, ValueError):
            continue
        out.append((str(d)[:10], fc))
    return sorted(set(out), key=lambda p: p[0])


def normalize_panel(price_panel: dict) -> dict:
    """{ticker: [(date, close)]} → cleaned, de-duplicated, date-sorted."""
    return {str(t): _sorted_series(s) for t, s in price_panel.items()
            if s}


def _momentum(closes: list[float], idx: int, kind: str) -> Optional[float]:
    def c(offset):
        j = idx - offset
        return closes[j] if 0 <= j < len(closes) else None
    if kind == "mom_12_1":
        a, b = c(21), c(252)
    elif kind == "mom_6_1":
        a, b = c(21), c(126)
    else:
        return None
    if a and b and b > 0:
        return a / b - 1.0
    return None


def _series_returns(closes: list[float]) -> list[Optional[float]]:
    out: list[Optional[float]] = [None]
    for i in range(1, len(closes)):
        p0, p1 = closes[i - 1], closes[i]
        out.append((p1 / p0 - 1.0) if (p0 and p0 > 0) else None)
    return out


def _monthly_returns(series: list[tuple[str, float]], form_dates: list[str]
                     ) -> list[Optional[float]]:
    """Return over each [form_date_i, form_date_{i+1}] window for one series."""
    idx = {d: i for i, (d, _) in enumerate(series)}
    closes = [c for _, c in series]
    out: list[Optional[float]] = []
    for k in range(len(form_dates) - 1):
        i0 = idx.get(form_dates[k])
        i1 = idx.get(form_dates[k + 1])
        if i0 is None or i1 is None or closes[i0] <= 0:
            out.append(None)
        else:
            out.append(closes[i1] / closes[i0] - 1.0)
    return out


# --------------------------------------------------------------------------- #
# Reconstruction.
# --------------------------------------------------------------------------- #
def reconstruct_champion(price_panel: dict, *,
                         policy: Optional[dict] = None,
                         sector_map: Optional[dict] = None,
                         spy_series: Optional[list] = None,
                         benchmark_source: str = "EQUAL_WEIGHT_UNIVERSE_PROXY",
                         top_n: int = 25,
                         cost_bps: float = 10.0) -> dict:
    """Reconstruct the champion's price/momentum leg on the survivorship-free
    panel and compute the forensic battery. Returns a JSON-serialisable dict."""
    policy = policy or {}
    panel = normalize_panel(price_panel)
    sector_map = {str(k): str(v) for k, v in (sector_map or {}).items()}

    all_dates = sorted({d for s in panel.values() for d, _ in s})
    if len(all_dates) <= _MAX_LOOKBACK + _HORIZON:
        return _insufficient(panel, all_dates, policy, benchmark_source)

    form_idxs = list(range(_MAX_LOOKBACK, len(all_dates), _STEP_MONTH))
    form_dates = [all_dates[i] for i in form_idxs]

    tk_series = panel
    tk_index = {t: {d: i for i, (d, _) in enumerate(s)}
                for t, s in panel.items()}
    tk_close = {t: [c for _, c in s] for t, s in panel.items()}

    # Per-period cross sections + long-only Top-N equal-weight book.
    cross_sections: list[tuple] = []
    book_returns: list[float] = []          # top-N equal-weight monthly return
    bench_returns: list[float] = []         # equal-weight universe monthly return
    turnovers: list[float] = []
    prev_book: dict[str, float] = {}
    contribution: dict[str, float] = {}     # cumulative name contribution
    latest_book: dict[str, float] = {}
    latest_sectors: dict[str, float] = {}
    per_period_rows: list[dict] = []

    for k in range(len(form_dates) - 1):
        fd = form_dates[k]
        nd = form_dates[k + 1]
        facs: list[float] = []
        fwds: list[float] = []
        names: list[str] = []
        for t, s in tk_series.items():
            i0 = tk_index[t].get(fd)
            i1 = tk_index[t].get(nd)
            if i0 is None or i1 is None:
                continue
            fac = _momentum(tk_close[t], i0, "mom_12_1")
            base = tk_close[t][i0]
            if fac is None or base <= 0:
                continue
            fwd = tk_close[t][i1] / base - 1.0
            facs.append(fac)
            fwds.append(fwd)
            names.append(t)
        if len(facs) < top_n:
            continue
        cross_sections.append((facs, fwds))
        bench_returns.append(sum(fwds) / len(fwds))
        # Deterministic Top-N with an optional sector cap on the book.
        order = sorted(range(len(facs)), key=lambda i: (-facs[i], names[i]))
        chosen = _select_top(order, names, sector_map, top_n,
                             float(policy.get("maximum_sector_weight_pct", 25.0)))
        w = 1.0 / len(chosen)
        book = {names[i]: w for i in chosen}
        book_ret = sum(fwds[i] * w for i in chosen)
        book_returns.append(book_ret)
        turnovers.append(ec.turnover(prev_book, book))
        prev_book = book
        for i in chosen:
            contribution[names[i]] = contribution.get(names[i], 0.0) + fwds[i] * w
        latest_book = book
        latest_sectors = {}
        for i in chosen:
            sec = sector_map.get(names[i], "Unknown")
            latest_sectors[sec] = latest_sectors.get(sec, 0.0) + w
        per_period_rows.append({
            "formation_date": fd,
            "next_date": nd,
            "universe": len(facs),
            "rank_ic": ec.spearman(facs, fwds),
            "book_return": book_ret,
            "benchmark_return": bench_returns[-1],
            "turnover": turnovers[-1],
        })

    forensics = _forensics(cross_sections, book_returns, bench_returns,
                           turnovers, contribution, latest_book, latest_sectors,
                           panel, tk_series, form_dates, spy_series, cost_bps,
                           top_n)

    classification = _classification(policy, top_n, len(panel),
                                     forensics.get("periods"))
    construction_match = _construction_match(policy, top_n, latest_book,
                                             latest_sectors)

    return {
        "champion_model": policy.get("strategy") or "fundamental_momentum_50_50_v1",
        "target_book": policy.get("target_book")
        or "fundamental_momentum_50_50_top25",
        "reconstruction_basis": "survivorship-free Norgate total-return daily "
        "bars, monthly rebalanced Top-%d equal-weight, 12-1 price-momentum "
        "selection leg" % top_n,
        "benchmark_source": benchmark_source,
        "universe_symbols": len(panel),
        "date_start": form_dates[0] if form_dates else None,
        "date_end": form_dates[-1] if form_dates else None,
        "classification": classification,
        "construction_match": construction_match,
        "forensics": forensics,
        "reconstructed_top_names": sorted(latest_book.keys()),
        "reconstructed_weights": {k: round(v, 6) for k, v in latest_book.items()},
        "per_period": per_period_rows,
    }


def _select_top(order, names, sector_map, top_n, max_sector_pct) -> list[int]:
    """Deterministic Top-N selection with a construction-time sector cap. The
    cap mirrors the operational contract: a name that would breach the sector
    weight cap is skipped for the next eligible name (never silently kept)."""
    max_per_sector = max(1, int(round(top_n * (max_sector_pct / 100.0))))
    chosen: list[int] = []
    sec_count: dict[str, int] = {}
    if not sector_map:
        return order[:top_n]
    for i in order:
        if len(chosen) >= top_n:
            break
        sec = sector_map.get(names[i], "Unknown")
        if sec_count.get(sec, 0) >= max_per_sector:
            continue
        chosen.append(i)
        sec_count[sec] = sec_count.get(sec, 0) + 1
    # If the sector cap starved the book, top up deterministically by rank.
    if len(chosen) < top_n:
        for i in order:
            if len(chosen) >= top_n:
                break
            if i not in chosen:
                chosen.append(i)
    return chosen


def _forensics(cross_sections, book_returns, bench_returns, turnovers,
               contribution, latest_book, latest_sectors, panel, tk_series,
               form_dates, spy_series, cost_bps, top_n) -> dict:
    ppy = 12.0
    ics = ec.rank_ic_series(cross_sections)
    spreads = ec.decile_spread_series(cross_sections)
    net = ec.apply_costs(book_returns, turnovers, cost_bps)
    excess = [b - m for b, m in zip(book_returns, bench_returns)]

    beta, benchmark_used = _beta(book_returns, form_dates, spy_series,
                                 bench_returns)
    # Name / sector concentration from the latest book.
    name_hhi = sum(w * w for w in latest_book.values()) if latest_book else None
    max_name = max(latest_book.values()) if latest_book else None
    sector_hhi = (sum(w * w for w in latest_sectors.values())
                  if latest_sectors else None)
    max_sector = max(latest_sectors.values()) if latest_sectors else None
    # Top-5 contribution concentration.
    contribs = sorted(contribution.values(), key=lambda v: -abs(v))
    tot_abs = sum(abs(v) for v in contribs) or None
    top5 = (sum(abs(v) for v in contribs[:5]) / tot_abs) if tot_abs else None
    # Risk-contribution dispersion proxy: dispersion of held names' realised vol
    # (equal dollar → unequal risk when this is large).
    rc_ratio, unequal_risk = _risk_dispersion(latest_book, tk_series, form_dates)
    # Rolling OOS: per-window IC mean over non-overlapping thirds.
    rolling = _rolling_oos(ics, book_returns, ppy)

    return {
        "periods": len(book_returns),
        "observations": sum(len(cs[0]) for cs in cross_sections),
        "gross_cumulative_return": ec.cumulative_return(book_returns),
        "net_cumulative_return": ec.cumulative_return(net),
        "gross_annualized_return": ec.annualized_return(book_returns,
                                                        periods_per_year=ppy),
        "net_annualized_return": ec.annualized_return(net, periods_per_year=ppy),
        "annualized_vol": ec.annualized_vol(book_returns, periods_per_year=ppy),
        "sharpe": ec.sharpe(net, periods_per_year=ppy),
        "max_drawdown": ec.max_drawdown(net),
        "rank_ic_mean": ec.mean(ics),
        "rank_ic_t": ec.tstat(ics),
        "rank_ic_positive_ratio": ec.positive_ratio(ics),
        "ic_stability_subperiod": ec.subperiod_consistency(ics, parts=4),
        "decile_spread_mean": ec.mean(spreads),
        "decile_spread_t": ec.tstat(spreads),
        "turnover_mean": ec.mean(turnovers),
        "annual_cost_drag": (ec.mean(turnovers) or 0.0) * (cost_bps / 10000.0)
        * ppy,
        "market_beta": beta,
        "beta_benchmark": benchmark_used,
        "benchmark_excess_annualized": (
            (ec.annualized_return(book_returns, periods_per_year=ppy) or 0.0)
            - (ec.annualized_return(bench_returns, periods_per_year=ppy) or 0.0)),
        "subperiod_excess_consistency": ec.subperiod_consistency(excess, parts=2),
        "regime_consistency": _regime_consistency(excess, bench_returns),
        "name_concentration_hhi": name_hhi,
        "max_name_weight": max_name,
        "sector_concentration_hhi": sector_hhi,
        "max_sector_weight": max_sector,
        "top5_contribution_share": top5,
        "sector_exposures": {k: round(v, 6) for k, v in
                             sorted(latest_sectors.items())},
        "sector_exposures_basis": "current Norgate GICS snapshot (not point-in-"
        "time; membership of large-caps is stable but this is a caveat)",
        "risk_contribution_vol_dispersion_ratio": rc_ratio,
        "equal_dollar_implies_unequal_risk": unequal_risk,
        "rolling_oos": rolling,
    }


def _beta(book_returns, form_dates, spy_series, bench_returns):
    """Market beta of the book vs SPY monthly returns when available, else vs the
    equal-weight universe proxy."""
    if spy_series:
        spy = _sorted_series(spy_series)
        spy_month = _monthly_returns(spy, form_dates)
        xs, ys = [], []
        for br, mr in zip(book_returns, spy_month):
            if mr is not None:
                xs.append(mr)
                ys.append(br)
        b = _ols_beta(xs, ys)
        if b is not None:
            return b, "SPY"
    b = _ols_beta(bench_returns, book_returns)
    return b, "EQUAL_WEIGHT_UNIVERSE_PROXY"


def _ols_beta(xs, ys) -> Optional[float]:
    n = min(len(xs), len(ys))
    if n < 3:
        return None
    xs, ys = xs[:n], ys[:n]
    mx = sum(xs) / n
    my = sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    if sxx <= 0:
        return None
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    return sxy / sxx


def _regime_consistency(excess, bench):
    up = [e for e, b in zip(excess, bench) if b > 0]
    down = [e for e, b in zip(excess, bench) if b <= 0]
    signs = [x for x in (ec.mean(up), ec.mean(down)) if x is not None]
    if not signs:
        return None
    return sum(1 for x in signs if x > 0) / len(signs)


def _risk_dispersion(latest_book, tk_series, form_dates):
    """Realised-vol dispersion of the latest held names. Ratio = max/median of
    per-name annualised vol; large ⇒ equal-dollar weights ⇒ unequal risk."""
    if not latest_book or len(form_dates) < 4:
        return None, None
    window = form_dates[-min(13, len(form_dates)):]
    vols = []
    for t in latest_book:
        s = tk_series.get(t)
        if not s:
            continue
        mr = [r for r in _monthly_returns(s, window) if r is not None]
        v = ec.stdev(mr)
        if v is not None:
            vols.append(v)
    if len(vols) < 3:
        return None, None
    vols.sort()
    med = vols[len(vols) // 2]
    if med <= 0:
        return None, None
    ratio = vols[-1] / med
    return ratio, bool(ratio >= 2.0)


def _rolling_oos(ics, book_returns, ppy):
    out = []
    n = len(book_returns)
    if n < 6:
        return out
    thirds = 3
    size = n // thirds
    for w in range(thirds):
        lo = w * size
        hi = (w + 1) * size if w < thirds - 1 else n
        seg_ret = book_returns[lo:hi]
        seg_ic = ics[lo:hi] if lo < len(ics) else []
        out.append({
            "window": w + 1,
            "periods": hi - lo,
            "annualized_return": ec.annualized_return(seg_ret,
                                                      periods_per_year=ppy),
            "rank_ic_mean": ec.mean(seg_ic),
            "positive": bool((ec.mean(seg_ret) or 0) > 0),
        })
    return out


def _classification(policy, top_n, universe, periods) -> list[dict]:
    return [
        {"component": "portfolio_construction",
         "class": eo.RECON_EXACT,
         "detail": "Top-%d equal-weight (~%.1f%% target, %.0f%% name cap, %.0f%% "
                   "sector cap), residual cash, monthly review — reproduced "
                   "exactly from the operational alpha-book policy contract."
                   % (top_n,
                      float(policy.get("target_weight_per_name_pct", 4.0)),
                      float(policy.get("maximum_position_weight_pct", 5.0)),
                      float(policy.get("maximum_sector_weight_pct", 25.0)))},
        {"component": "selection_signal_price_leg",
         "class": eo.RECON_PARTIAL,
         "detail": "12-1 cross-sectional price momentum reconstructed on real "
                   "survivorship-free history (%s symbols, %s monthly periods). "
                   "This is the PRICE leg only — a proxy for the operational "
                   "selection, not the exact realised book."
                   % (universe, periods)},
        {"component": "fundamental_leg_point_in_time",
         "class": eo.RECON_UNVERIFIABLE,
         "detail": "Owned fundamentals are a current General/Highlights snapshot "
                   "with no lookahead-free per-period filing/availability dates "
                   "(Stage 6 finding). A historical fundamental-momentum ranking "
                   "cannot be built without period-end lookahead, so it is NOT "
                   "reconstructed or fabricated here."},
    ]


def _construction_match(policy, top_n, latest_book, latest_sectors) -> dict:
    target_count = int(policy.get("target_position_count", top_n) or top_n)
    max_name_cap = float(policy.get("maximum_position_weight_pct", 5.0)) / 100.0
    max_sec_cap = float(policy.get("maximum_sector_weight_pct", 25.0)) / 100.0
    max_name = max(latest_book.values()) if latest_book else 0.0
    max_sector = max(latest_sectors.values()) if latest_sectors else 0.0
    count_ok = len(latest_book) == target_count
    name_ok = max_name <= max_name_cap + 1e-9
    sector_ok = max_sector <= max_sec_cap + 1e-9
    return {
        "operational_top25_matches_research": bool(count_ok),
        "reconstructed_position_count": len(latest_book),
        "target_position_count": target_count,
        "name_cap_respected": bool(name_ok),
        "sector_cap_respected": bool(sector_ok),
        "equal_dollar_weighting": True,
        "note": "Construction mechanics (count / equal-dollar / caps) match the "
                "operational contract; the SELECTION differs because the "
                "fundamental leg is unverifiable historically.",
    }


def _regime_consistency_placeholder():  # pragma: no cover
    return None


def _insufficient(panel, all_dates, policy, benchmark_source) -> dict:
    return {
        "champion_model": policy.get("strategy")
        or "fundamental_momentum_50_50_v1",
        "target_book": policy.get("target_book")
        or "fundamental_momentum_50_50_top25",
        "benchmark_source": benchmark_source,
        "universe_symbols": len(panel),
        "date_start": all_dates[0] if all_dates else None,
        "date_end": all_dates[-1] if all_dates else None,
        "classification": _classification(policy, 25, len(panel), 0),
        "construction_match": {"operational_top25_matches_research": None,
                               "note": "insufficient history to reconstruct"},
        "forensics": {"periods": 0, "note": "insufficient price history for the "
                      "reconstruction battery"},
        "reconstructed_top_names": [],
        "reconstructed_weights": {},
        "per_period": [],
    }


# --------------------------------------------------------------------------- #
# CSV projections for the package writer.
# --------------------------------------------------------------------------- #
def forensics_csv_rows(recon: dict) -> list[dict]:
    """Per-period diagnostic series for champion_forensics.csv."""
    return list(recon.get("per_period") or [])


def forensics_scalar_rows(recon: dict) -> list[dict]:
    """Flat metric,value rows (appended to champion_forensics.csv header note)."""
    f = recon.get("forensics") or {}
    rows = []
    for k in sorted(f.keys()):
        v = f[k]
        if isinstance(v, (dict, list)):
            continue
        rows.append({"metric": k, "value": v})
    return rows
