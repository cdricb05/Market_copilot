"""
alpha_agent.experiment_runner — Stage 5 bounded deterministic experiment engine.

Executes an allowlisted experiment specification against real Stage 2 normalized
historical data (or an injected fake store in tests): resolves the required
dataset families, checks point-in-time historical coverage, builds the
deterministic factor cross-sections, and computes the full evidence-metric
battery (rank IC, decile spread, turnover, cost-adjusted return, drawdown,
benchmark/champion comparison, regime + subperiod consistency, cost sensitivity).

Hard safety invariants (structural):
  * RESEARCH-ONLY, read-only. Reads normalized JSONL data; never writes source
    data, never opens PostgreSQL, never calls the prediction service, never
    creates trading activity, never executes or imports LLM-authored code.
  * Only the allowlisted factor keys in ``experiment_contracts.FEATURE_ALLOWLIST``
    are ever computed, from deterministic source code here.
  * When required point-in-time / historical data is unavailable, a structured
    data gap is returned — never a silent approximation.
  * Pure Python numeric math (no numpy/pandas) → byte-reproducible results.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Optional

from . import experiment_contracts as ec

# Trading days per rebalance step.
_STEP_DAYS = {"daily": 1, "weekly": 5, "monthly": 21, "quarterly": 63,
              "event": 21}
# Momentum lookback (trading days) required before a formation date.
_MAX_LOOKBACK = 252


# --------------------------------------------------------------------------- #
# Real normalized-data store (read-only over Stage 2 output).
# --------------------------------------------------------------------------- #
class NormalizedStore:
    """Read-only reader of the Stage 2 normalized record tree.

    Layout: ``<root>/normalized/<RECORD_TYPE>/YYYY/MM/DD/<run>.jsonl`` with one
    normalized record per line (top-level ``ticker`` / ``effective_at`` /
    ``normalized_payload``). Bounded by ``max_files`` / ``max_symbols``.
    """

    def __init__(self, ingestion_root: str, *, max_files: int = 4000,
                 max_symbols: int = 800):
        self.root = Path(ingestion_root) / "normalized"
        self.max_files = max_files
        self.max_symbols = max_symbols

    def _family_dirs(self, record_types) -> list[Path]:
        return [self.root / rt for rt in record_types
                if (self.root / rt).exists()]

    def coverage(self, record_types) -> dict:
        files: list[Path] = []
        for d in self._family_dirs(record_types):
            files.extend(sorted(d.rglob("*.jsonl"))[:self.max_files])
        dates = set()
        for f in files:
            # .../YYYY/MM/DD/<run>.jsonl
            parts = f.parts
            if len(parts) >= 4:
                y, m, d = parts[-4], parts[-3], parts[-2]
                if y.isdigit() and m.isdigit() and d.isdigit():
                    dates.add("%s-%s-%s" % (y, m, d))
        sdates = sorted(dates)
        return {"files": len(files),
                "date_start": sdates[0] if sdates else None,
                "date_end": sdates[-1] if sdates else None,
                "days": len(sdates),
                "months": len({d[:7] for d in sdates})}

    def price_panel(self, date_start: Optional[str] = None,
                    date_end: Optional[str] = None) -> dict:
        panel: dict[str, list] = {}
        rts = ec.FAMILY_TO_RECORD_TYPES["prices"]
        files: list[Path] = []
        for d in self._family_dirs(rts):
            files.extend(sorted(d.rglob("*.jsonl"))[:self.max_files])
        for f in files[:self.max_files]:
            try:
                text = f.read_text(encoding="utf-8-sig")
            except OSError:
                continue
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except ValueError:
                    continue
                tkr = rec.get("ticker")
                pay = rec.get("normalized_payload") or {}
                date = rec.get("effective_at") or pay.get("Date")
                close = pay.get("Close", pay.get("close"))
                if not tkr or not date or close is None:
                    continue
                if date_start and date < date_start:
                    continue
                if date_end and date > date_end:
                    continue
                if tkr not in panel and len(panel) >= self.max_symbols:
                    continue
                panel.setdefault(tkr, []).append((str(date), float(close)))
        for tkr in panel:
            panel[tkr] = sorted(set(panel[tkr]), key=lambda p: p[0])
        return panel


# --------------------------------------------------------------------------- #
# Deterministic factor functions (allowlisted, price-based).
# --------------------------------------------------------------------------- #
def _factor_value(feature: str, closes: list, idx: int) -> Optional[float]:
    """Factor at formation index ``idx`` from a ticker's own close series.

    Uses only data through ``idx`` (leakage-safe: the return window is strictly
    after ``idx``). Returns None when the lookback is unavailable.
    """
    def c(offset):
        j = idx - offset
        return closes[j] if 0 <= j < len(closes) else None
    if feature in ("mom_12_1", "combo_momentum_quality", "combo_value_momentum",
                   "sector_relative_momentum"):
        a, b = c(21), c(252)
        if a and b and b > 0:
            return a / b - 1.0
    elif feature in ("mom_6_1",):
        a, b = c(21), c(126)
        if a and b and b > 0:
            return a / b - 1.0
    elif feature in ("reversal_1m", "sector_relative_reversal"):
        a, b = c(0), c(21)
        if a and b and b > 0:
            return -(a / b - 1.0)
    return None


_PRICE_FEATURES = frozenset({
    "mom_12_1", "mom_6_1", "reversal_1m", "sector_relative_momentum",
    "sector_relative_reversal", "combo_momentum_quality",
    "combo_value_momentum"})


# --------------------------------------------------------------------------- #
# Cross-section construction + scoring.
# --------------------------------------------------------------------------- #
def build_cross_sections(panel: dict, *, feature: str, horizon_days: int,
                         rebalance: str) -> dict:
    """Build per-rebalance (factor, forward-return) cross-sections from a panel.

    Deterministic long-short decile book: also returns the per-period equal
    long-short decile return, one-sided turnover, and equal-weight benchmark.
    """
    step = _STEP_DAYS.get(rebalance, 21)
    # Master trading-day calendar = union of all dates, sorted.
    all_dates = sorted({d for series in panel.values() for d, _ in series})
    # Per-ticker date→index and close list.
    tk_close: dict[str, list] = {}
    tk_index: dict[str, dict] = {}
    for tkr, series in panel.items():
        tk_close[tkr] = [c for _, c in series]
        tk_index[tkr] = {d: i for i, (d, _) in enumerate(series)}

    cross_sections: list[tuple] = []
    portfolio_returns: list[float] = []
    turnovers: list[float] = []
    benchmark_returns: list[float] = []
    prev_book: dict[str, float] = {}
    observations = 0

    # Formation dates stepped along the master calendar.
    form_idxs = list(range(_MAX_LOOKBACK, len(all_dates), step))
    for fi in form_idxs:
        form_date = all_dates[fi]
        fac_vals: list[float] = []
        fwd_vals: list[float] = []
        tickers: list[str] = []
        for tkr, series in panel.items():
            idx = tk_index[tkr].get(form_date)
            if idx is None:
                continue
            closes = tk_close[tkr]
            fac = _factor_value(feature, closes, idx)
            fidx = idx + horizon_days
            if fac is None or fidx >= len(closes):
                continue
            base = closes[idx]
            if base is None or base <= 0:
                continue
            fwd = closes[fidx] / base - 1.0
            fac_vals.append(fac)
            fwd_vals.append(fwd)
            tickers.append(tkr)
        if len(fac_vals) < 4:
            continue
        cross_sections.append((fac_vals, fwd_vals))
        observations += len(fac_vals)
        benchmark_returns.append(sum(fwd_vals) / len(fwd_vals))
        # Long-only top-decile book — directly comparable to the equal-weight
        # benchmark. The top-minus-bottom decile spread is reported separately
        # (via decile_spread_series) as a diagnostic of monotonicity.
        order = sorted(range(len(fac_vals)), key=lambda i: fac_vals[i])
        size = max(1, len(order) // 10)
        top = order[-size:]
        book = {tickers[i]: 1.0 / size for i in top}
        portfolio_returns.append(sum(fwd_vals[i] for i in top) / size)
        turnovers.append(ec.turnover(prev_book, book))
        prev_book = book

    universe = (sorted(len(cs[0]) for cs in cross_sections)[len(cross_sections)
                // 2] if cross_sections else 0)
    return {
        "cross_sections": cross_sections,
        "portfolio_returns": portfolio_returns,
        "turnovers": turnovers,
        "benchmark_returns": benchmark_returns,
        "observations": observations,
        "periods": len(cross_sections),
        "universe": universe,
    }


def score_experiment(spec: dict, built: dict, *, gates: dict,
                     cost_grid: list, periods_per_year: float,
                     champion_returns: Optional[list] = None,
                     leakage_warning: bool = False,
                     leakage_detail: str = "",
                     missing_data_rate: Optional[float] = None) -> dict:
    """Compute the full deterministic evidence-metric battery from built
    cross-sections + portfolio/benchmark return series."""
    cross = built["cross_sections"]
    port = built["portfolio_returns"]
    turns = built["turnovers"]
    bench = built["benchmark_returns"]

    ics = ec.rank_ic_series(cross)
    spreads = ec.decile_spread_series(cross)
    primary_cost = spec.get("transaction_cost_bps", 25)
    net_port = ec.apply_costs(port, turns, primary_cost)

    gross_ann = ec.annualized_return(port, periods_per_year=periods_per_year)
    net_ann = ec.annualized_return(net_port, periods_per_year=periods_per_year)
    bench_ann = ec.annualized_return(bench, periods_per_year=periods_per_year)

    # Out-of-sample IC = mean IC over the second half of periods.
    half = len(ics) // 2
    oos_ic = ec.mean(ics[half:]) if half else ec.mean(ics)

    # Excess of the long-only book over the equal-weight benchmark; the edge
    # (not raw long return) drives the consistency + regime checks.
    excess_series = [p - b for p, b in zip(port, bench)]
    up_excess, down_excess = _regime_excess(excess_series, bench)
    reg_signs = [x for x in (up_excess, down_excess) if x is not None]
    regime_consistency = (sum(1 for x in reg_signs if x > 0) / len(reg_signs)
                          if reg_signs else None)

    cost_grid_rows, cost_flip, erosion = _cost_grid(
        port, turns, cost_grid, periods_per_year, gross_ann)

    bench_excess_ann = None
    if gross_ann is not None and bench_ann is not None:
        bench_excess_ann = gross_ann - bench_ann

    champ_comp = None
    if champion_returns:
        n = min(len(port), len(champion_returns))
        if n >= 3:
            corr = ec.pearson(port[:n], champion_returns[:n])
            champ_comp = None if corr is None else (1.0 - corr)

    metrics = {
        "observations": built["observations"],
        "universe": built["universe"],
        "periods": built["periods"],
        "rank_ic_mean": ec.mean(ics),
        "rank_ic_std": ec.stdev(ics),
        "rank_ic_t": ec.tstat(ics),
        "rank_ic_positive_ratio": ec.positive_ratio(ics),
        "oos_ic_mean": oos_ic,
        "decile_spread_mean": ec.mean(spreads),
        "spread_t": ec.tstat(spreads),
        "gross_return": ec.cumulative_return(port),
        "net_return": ec.cumulative_return(net_port),
        "gross_annualized_return": gross_ann,
        "net_annualized_return": net_ann,
        "annualized_vol": ec.annualized_vol(port,
                                            periods_per_year=periods_per_year),
        "sharpe": ec.sharpe(net_port, periods_per_year=periods_per_year),
        "max_drawdown": ec.max_drawdown(net_port),
        "turnover": ec.mean(turns),
        "avg_holding_count": built["universe"],
        "hit_rate": ec.hit_rate(net_port),
        "benchmark_name": spec.get("benchmark"),
        "benchmark_return": ec.cumulative_return(bench),
        "benchmark_excess_annualized": bench_excess_ann,
        "benchmark_excess_mean": ec.mean(excess_series),
        "champion_complementarity": champ_comp,
        "regime_consistency": regime_consistency,
        "subperiod_consistency": ec.subperiod_consistency(excess_series,
                                                          parts=2),
        "missing_data_rate": missing_data_rate,
        "concentration": (1.0 / built["universe"]) if built["universe"] else None,
        "cost_flips_sign": cost_flip,
        "cost_erosion_ratio": erosion,
        "leakage_warning": bool(leakage_warning),
        "leakage_detail": leakage_detail,
        "regime": {"up_regime_excess": up_excess,
                   "down_regime_excess": down_excess},
        "cost_sensitivity": {"grid": cost_grid_rows},
    }
    return metrics


def _regime_excess(excess_series, bench):
    up = [e for e, b in zip(excess_series, bench) if b > 0]
    down = [e for e, b in zip(excess_series, bench) if b <= 0]
    return ec.mean(up), ec.mean(down)


def _cost_grid(port, turns, cost_grid, ppy, gross_ann):
    rows = []
    flip = False
    for cb in (cost_grid or [25]):
        net = ec.apply_costs(port, turns, cb)
        net_ann = ec.annualized_return(net, periods_per_year=ppy)
        net_mean = ec.mean(net)
        flips = bool(gross_ann is not None and gross_ann > 0
                     and net_mean is not None and net_mean <= 0)
        flip = flip or flips
        rows.append({"cost_bps": cb, "net_annualized_return": net_ann,
                     "flips_sign": flips})
    erosion = None
    if gross_ann is not None and gross_ann > 0 and rows:
        mid = rows[len(rows) // 2]
        na = mid.get("net_annualized_return")
        if na is not None:
            erosion = (gross_ann - na) / gross_ann
    return rows, flip, erosion


# --------------------------------------------------------------------------- #
# Runner facade.
# --------------------------------------------------------------------------- #
class ExperimentRunner:
    def __init__(self, cfg: dict, *, store: Optional[Any] = None):
        self.cfg = cfg
        # max_files is configurable so a deep Stage 6 backfill (thousands of
        # daily partition files) is not silently truncated at read time.
        self.store = store or NormalizedStore(
            cfg.get("stage2_ingestion_root", ""),
            max_symbols=int((cfg.get("bounds") or {}).get(
                "max_symbols", 800)),
            max_files=int((cfg.get("bounds") or {}).get(
                "max_files", 4000)))
        self.cost_grid = list(cfg.get("cost_bps") or [10, 25, 50])

    def check_coverage(self, hyp: dict, template: str) -> dict:
        """Resolve required datasets and verify point-in-time historical
        coverage. Returns a coverage dict; on shortfall a ``data_gap`` reason."""
        tpl = ec.TEMPLATES[template]
        step = _STEP_DAYS.get(tpl["rebalance"], 21)
        need_span = tpl["min_periods"] * step + tpl["horizon_days"] \
            + _MAX_LOOKBACK
        date_start = date_end = None
        for family in tpl["required_datasets"]:
            rts = ec.FAMILY_TO_RECORD_TYPES.get(family, ())
            cov = self.store.coverage(rts)
            if cov.get("files", 0) == 0:
                return {"data_gap": ec.DATA_HOLD_MISSING_DATASET,
                        "detail": "no normalized data for family '%s' (%s)" %
                        (family, ",".join(rts))}
            if family == "prices":
                date_start = cov.get("date_start")
                date_end = cov.get("date_end")
                if cov.get("days", 0) < need_span:
                    return {"data_gap": ec.DATA_HOLD_INSUFFICIENT_HISTORY,
                            "detail": "prices span %s trading days < %s "
                            "required for %s" % (cov.get("days"), need_span,
                                                 template),
                            "date_start": date_start, "date_end": date_end}
            elif cov.get("days", 0) < tpl["min_periods"]:
                return {"data_gap": ec.DATA_HOLD_INSUFFICIENT_COVERAGE,
                        "detail": "family '%s' has only %s dated points" %
                        (family, cov.get("days"))}
        return {"data_gap": None, "date_start": date_start,
                "date_end": date_end,
                "data_version": ec.data_version_id(
                    self.cfg_source_fp(), date_start or "", date_end or "")}

    def cfg_source_fp(self) -> str:
        return str((self.cfg.get("stage2_ingestion_root") or ""))[:24]

    def run_experiment(self, spec: dict, *, gates: dict,
                       champion: Optional[str] = None) -> dict:
        """Run one bounded historical experiment and return its metrics."""
        feature = spec.get("feature")
        if feature not in ec.FEATURE_ALLOWLIST:
            return {"experiment_failed": True,
                    "failure_reason": "feature '%s' not in allowlist" % feature}
        if spec.get("study_kind") != "cross_sectional_rank" \
                or feature not in _PRICE_FEATURES:
            # Event-window / specialized-signal execution requires signal fields
            # not present in the owned normalized payload — honest data gap.
            return {"data_gap": ec.DATA_HOLD_MISSING_DATASET,
                    "failure_reason": None,
                    "detail": "template %s needs a signal field absent from "
                    "owned normalized data" % spec.get("template")}
        panel = self.store.price_panel(spec.get("date_start"),
                                       spec.get("date_end"))
        if len(panel) < spec.get("min_universe", 30):
            return {"data_gap": ec.DATA_HOLD_INSUFFICIENT_COVERAGE,
                    "detail": "panel has %d symbols < min_universe %d" %
                    (len(panel), spec.get("min_universe", 30))}
        built = build_cross_sections(
            panel, feature=feature, horizon_days=spec.get("horizon_days", 21),
            rebalance=spec.get("rebalance", "monthly"))
        if built["periods"] < spec.get("min_periods", 12):
            return {"data_gap": ec.DATA_HOLD_INSUFFICIENT_HISTORY,
                    "detail": "only %d rebalance periods < min %d" %
                    (built["periods"], spec.get("min_periods", 12))}
        ppy = ec.periods_per_year_for(spec.get("rebalance", "monthly"))
        return score_experiment(spec, built, gates=gates,
                                cost_grid=self.cost_grid,
                                periods_per_year=ppy,
                                missing_data_rate=0.0)


# --------------------------------------------------------------------------- #
# Stage 7 Workstream 4 — bounded, deterministic owned-price factor campaign.
#
# Broadens the executable, point-in-time-safe PRICE factor set (all computed
# from owned Norgate bars through the formation date only; the return window is
# strictly after t) so the recovery campaign runs REAL experiments rather than
# only data holds. Factors requiring non-owned or non-point-in-time inputs
# (sector-neutral needs a PIT GICS time series; insider / news / earnings need
# signal fields absent from owned normalized data) are HELD honestly, never
# approximated. Additive: the shared Stage 5 template/feature allowlist and every
# existing function above are left unchanged.
# --------------------------------------------------------------------------- #
CAMPAIGN_FEATURES = (
    "residual_momentum", "momentum_accel", "vol_scaled_momentum",
    "short_term_reversal", "low_volatility", "combo_mom_lowvol",
)
CAMPAIGN_HELD_FEATURES = {
    "sector_neutral_momentum": ec.DATA_HOLD_NO_POINT_IN_TIME,
    "insider_event": ec.DATA_HOLD_MISSING_DATASET,
    "news_event": ec.DATA_HOLD_MISSING_DATASET,
}

CAMPAIGN_STATEMENTS = {
    "residual_momentum": "Cross-sectionally market-neutral (residual) 12-1 price "
    "momentum ranks forward returns in the survivorship-free S&P 500 universe.",
    "momentum_accel": "Momentum acceleration (12-1 minus 6-1 trailing return) "
    "ranks forward returns.",
    "vol_scaled_momentum": "Volatility-scaled 12-1 momentum (momentum divided by "
    "trailing realised volatility) ranks forward returns.",
    "short_term_reversal": "Short-term (1-month) reversal ranks forward returns.",
    "low_volatility": "Low trailing realised volatility (the low-vol anomaly) "
    "ranks forward returns.",
    "combo_mom_lowvol": "An orthogonal combination of standardized 12-1 momentum "
    "and standardized low-volatility ranks forward returns (ablation vs pure "
    "momentum).",
    "sector_neutral_momentum": "Sector-neutral 12-1 momentum ranks forward "
    "returns.",
}


def _campaign_components(closes: list, idx: int):
    def c(o):
        j = idx - o
        return closes[j] if 0 <= j < len(closes) else None
    mom12 = None
    a, b = c(21), c(252)
    if a and b and b > 0:
        mom12 = a / b - 1.0
    mom6 = None
    a6, b6 = c(21), c(126)
    if a6 and b6 and b6 > 0:
        mom6 = a6 / b6 - 1.0
    ret1m = None
    a1, b1 = c(0), c(21)
    if a1 and b1 and b1 > 0:
        ret1m = a1 / b1 - 1.0
    seg = closes[max(0, idx - 63):idx + 1]
    rets = [seg[i] / seg[i - 1] - 1.0 for i in range(1, len(seg))
            if seg[i - 1] and seg[i - 1] > 0]
    vol = ec.stdev(rets) if len(rets) >= 20 else None
    return {"mom12": mom12, "mom6": mom6, "ret1m": ret1m, "vol": vol}


def _zscores(vals: list) -> list:
    finite = [v for v in vals if v is not None and ec._is_num(v)]
    if len(finite) < 3:
        return [None] * len(vals)
    m = sum(finite) / len(finite)
    sd = ec.stdev(finite)
    if not sd:
        return [None] * len(vals)
    return [((v - m) / sd) if (v is not None and ec._is_num(v)) else None
            for v in vals]


def _campaign_feature_array(feature: str, comps: list) -> list:
    """Deterministic feature transform from per-name components. Cross-sectional
    (residual / combo) transforms use only same-formation data (no leakage)."""
    if feature == "momentum_accel":
        return [(cc["mom12"] - cc["mom6"])
                if (cc["mom12"] is not None and cc["mom6"] is not None)
                else None for cc in comps]
    if feature == "vol_scaled_momentum":
        return [(cc["mom12"] / cc["vol"])
                if (cc["mom12"] is not None and cc["vol"] and cc["vol"] > 0)
                else None for cc in comps]
    if feature == "short_term_reversal":
        return [(-cc["ret1m"]) if cc["ret1m"] is not None else None
                for cc in comps]
    if feature == "low_volatility":
        return [(-cc["vol"]) if cc["vol"] is not None else None for cc in comps]
    if feature == "residual_momentum":
        mom = [cc["mom12"] for cc in comps]
        fin = [v for v in mom if v is not None]
        mu = (sum(fin) / len(fin)) if fin else None
        return [(v - mu) if (v is not None and mu is not None) else None
                for v in mom]
    if feature == "combo_mom_lowvol":
        zm = _zscores([cc["mom12"] for cc in comps])
        zv = _zscores([(-cc["vol"]) if cc["vol"] is not None else None
                       for cc in comps])
        return [(a + b) if (a is not None and b is not None) else None
                for a, b in zip(zm, zv)]
    if feature == "null_control":
        return [_null_value(cc.get("_name", ""), cc.get("_fd", ""))
                for cc in comps]
    return [None] * len(comps)


def _null_value(name: str, fd: str) -> float:
    h = hashlib.sha256(("null|%s|%s" % (name, fd)).encode("utf-8")).hexdigest()
    return int(h[:8], 16) / 0xFFFFFFFF


def _build_campaign_cross_sections(panel: dict, *, feature: str,
                                   horizon_days: int, rebalance: str) -> dict:
    step = _STEP_DAYS.get(rebalance, 21)
    all_dates = sorted({d for series in panel.values() for d, _ in series})
    tk_close = {t: [c for _, c in s] for t, s in panel.items()}
    tk_index = {t: {d: i for i, (d, _) in enumerate(s)}
                for t, s in panel.items()}
    cross_sections: list[tuple] = []
    portfolio_returns: list[float] = []
    turnovers: list[float] = []
    benchmark_returns: list[float] = []
    prev_book: dict[str, float] = {}
    observations = 0
    for fi in range(_MAX_LOOKBACK, len(all_dates), step):
        fd = all_dates[fi]
        comps: list[dict] = []
        fwds: list[float] = []
        names: list[str] = []
        for t, s in panel.items():
            idx = tk_index[t].get(fd)
            if idx is None:
                continue
            closes = tk_close[t]
            fidx = idx + horizon_days
            if fidx >= len(closes):
                continue
            base = closes[idx]
            if base is None or base <= 0:
                continue
            cc = _campaign_components(closes, idx)
            cc["_name"] = t
            cc["_fd"] = fd
            comps.append(cc)
            fwds.append(closes[fidx] / base - 1.0)
            names.append(t)
        if len(comps) < 4:
            continue
        feats = _campaign_feature_array(feature, comps)
        fac_vals, fwd_vals, sel_names = [], [], []
        for v, r, nm in zip(feats, fwds, names):
            if v is not None and ec._is_num(v):
                fac_vals.append(float(v))
                fwd_vals.append(float(r))
                sel_names.append(nm)
        if len(fac_vals) < 4:
            continue
        cross_sections.append((fac_vals, fwd_vals))
        observations += len(fac_vals)
        benchmark_returns.append(sum(fwd_vals) / len(fwd_vals))
        order = sorted(range(len(fac_vals)), key=lambda i: fac_vals[i])
        size = max(1, len(order) // 10)
        top = order[-size:]
        book = {sel_names[i]: 1.0 / size for i in top}
        portfolio_returns.append(sum(fwd_vals[i] for i in top) / size)
        turnovers.append(ec.turnover(prev_book, book))
        prev_book = book
    universe = (sorted(len(cs[0]) for cs in cross_sections)[
        len(cross_sections) // 2] if cross_sections else 0)
    return {"cross_sections": cross_sections,
            "portfolio_returns": portfolio_returns, "turnovers": turnovers,
            "benchmark_returns": benchmark_returns,
            "observations": observations, "periods": len(cross_sections),
            "universe": universe}


def _spy_annualized(spy_series, ppy=252.0):
    if not spy_series:
        return None
    lv = sorted({(str(d)[:10], float(c)) for d, c in spy_series},
                key=lambda p: p[0])
    rets = [lv[i][1] / lv[i - 1][1] - 1.0 for i in range(1, len(lv))
            if lv[i - 1][1] > 0]
    return ec.annualized_return(rets, periods_per_year=ppy)


def default_campaign_specs(cfg: Optional[dict] = None) -> list[dict]:
    """The fixed, bounded campaign design: 6 hypotheses (≤2 specs each, ≤12
    experiments). Deterministic; never tuned to a target outcome."""
    specs = [
        {"hypothesis_id": "hyp_campaign_residual_momentum",
         "feature": "residual_momentum", "horizon_days": 21,
         "rebalance": "monthly", "variant": "21d"},
        {"hypothesis_id": "hyp_campaign_residual_momentum",
         "feature": "residual_momentum", "horizon_days": 63,
         "rebalance": "monthly", "variant": "63d"},
        {"hypothesis_id": "hyp_campaign_momentum_accel",
         "feature": "momentum_accel", "horizon_days": 21,
         "rebalance": "monthly", "variant": "21d"},
        {"hypothesis_id": "hyp_campaign_vol_scaled_momentum",
         "feature": "vol_scaled_momentum", "horizon_days": 21,
         "rebalance": "monthly", "variant": "21d"},
        {"hypothesis_id": "hyp_campaign_short_term_reversal",
         "feature": "short_term_reversal", "horizon_days": 21,
         "rebalance": "monthly", "variant": "21d"},
        {"hypothesis_id": "hyp_campaign_low_volatility",
         "feature": "low_volatility", "horizon_days": 21,
         "rebalance": "monthly", "variant": "21d"},
        {"hypothesis_id": "hyp_campaign_combo_mom_lowvol",
         "feature": "combo_mom_lowvol", "horizon_days": 21,
         "rebalance": "monthly", "variant": "orthogonal"},
    ]
    return specs


def run_price_factor_campaign(panel: dict, *, specs: Optional[list] = None,
                              gates: Optional[dict] = None,
                              cost_grid: Optional[list] = None,
                              cost_bps: float = 10.0,
                              champion_returns: Optional[list] = None,
                              spy_series: Optional[list] = None,
                              bounds: Optional[dict] = None) -> dict:
    """Run the bounded owned-price factor campaign deterministically and return
    plan + per-experiment metrics + evidence decisions. Never forces a positive
    result; honest data holds for factors that need absent/non-PIT inputs."""
    gates = gates or ec.DEFAULT_GATES
    cost_grid = cost_grid or [5, 10, 25, 50]
    bounds = bounds or {}
    max_hyp = int(bounds.get("max_new_hypotheses", 6))
    max_specs = int(bounds.get("max_specs_per_hypothesis", 2))
    max_exp = int(bounds.get("max_experiments", 12))
    specs = specs or default_campaign_specs()

    # Enforce bounds deterministically.
    per_hyp: dict[str, int] = {}
    seen_hyp: list[str] = []
    bounded: list[dict] = []
    for s in specs:
        hid = s["hypothesis_id"]
        if hid not in seen_hyp:
            if len(seen_hyp) >= max_hyp:
                continue
            seen_hyp.append(hid)
        if per_hyp.get(hid, 0) >= max_specs:
            continue
        if len(bounded) >= max_exp:
            break
        per_hyp[hid] = per_hyp.get(hid, 0) + 1
        bounded.append(s)

    spy_ann = _spy_annualized(spy_series)
    # Null-control baseline (validates the harness; a real factor must beat it).
    null_built = _build_campaign_cross_sections(
        panel, feature="null_control", horizon_days=21, rebalance="monthly")
    null_ic_t = ec.tstat(ec.rank_ic_series(null_built["cross_sections"]))

    results: list[dict] = []
    decisions: list[dict] = []
    keep = completed = failed = 0
    for s in bounded:
        feature = s["feature"]
        label = "%s_%s" % (feature, s.get("variant", ""))
        exp_id = "exp_" + ec.sha("campaign", s["hypothesis_id"], feature,
                                 str(s.get("horizon_days")),
                                 str(s.get("variant")))[:16]
        built = _build_campaign_cross_sections(
            panel, feature=feature, horizon_days=s.get("horizon_days", 21),
            rebalance=s.get("rebalance", "monthly"))
        min_periods = int(gates.get("min_periods", 12))
        if built["periods"] < min_periods:
            dec = {"experiment_id": exp_id, "hypothesis_id": s["hypothesis_id"],
                   "feature": feature, "decision": ec.NEED_MORE_DATA,
                   "reasons": ["insufficient periods (%d < %d)" %
                               (built["periods"], min_periods)]}
            decisions.append(dec)
            results.append({"experiment_id": exp_id, "label": label,
                            "hypothesis_id": s["hypothesis_id"],
                            "feature": feature, "periods": built["periods"],
                            "decision": ec.NEED_MORE_DATA})
            failed += 1
            continue
        spec = {"template": "campaign_price_factor", "feature": feature,
                "study_kind": "cross_sectional_rank",
                "horizon_days": s.get("horizon_days", 21),
                "rebalance": s.get("rebalance", "monthly"),
                "benchmark": "equal_weight_universe",
                "transaction_cost_bps": cost_bps}
        ppy = ec.periods_per_year_for(s.get("rebalance", "monthly"))
        metrics = score_experiment(spec, built, gates=gates,
                                   cost_grid=cost_grid, periods_per_year=ppy,
                                   champion_returns=champion_returns,
                                   missing_data_rate=0.0)
        gross_ann = metrics.get("gross_annualized_return")
        metrics["spy_annualized_return"] = spy_ann
        metrics["spy_excess_annualized"] = (
            (gross_ann - spy_ann) if (gross_ann is not None
                                      and spy_ann is not None) else None)
        metrics["null_control_rank_ic_t"] = null_ic_t
        ict = metrics.get("rank_ic_t")
        metrics["beats_null_control"] = bool(
            ict is not None and null_ic_t is not None
            and abs(ict) > abs(null_ic_t))
        verdict = ec.evaluate_evidence(metrics, gates)
        metrics["experiment_id"] = exp_id
        metrics["label"] = label
        metrics["hypothesis_id"] = s["hypothesis_id"]
        metrics["feature"] = feature
        metrics["decision"] = verdict["decision"]
        results.append(metrics)
        decisions.append({"experiment_id": exp_id,
                          "hypothesis_id": s["hypothesis_id"],
                          "feature": feature,
                          "decision": verdict["decision"],
                          "reasons": verdict["reasons"]})
        completed += 1
        if verdict["decision"] == ec.KEEP_FOR_RESEARCH:
            keep += 1

    held = []
    for feat, reason in CAMPAIGN_HELD_FEATURES.items():
        held.append({"feature": feat, "hypothesis_id": "hyp_campaign_%s" % feat,
                     "gap": reason,
                     "statement": CAMPAIGN_STATEMENTS.get(feat, ""),
                     "detail": _held_reason(feat)})
        decisions.append({"hypothesis_id": "hyp_campaign_%s" % feat,
                          "feature": feat, "decision": "DATA_HOLD",
                          "reasons": [_held_reason(feat)]})

    plan = {
        "hypotheses": seen_hyp,
        "experiments_planned": len(bounded),
        "bounds": {"max_new_hypotheses": max_hyp,
                   "max_specs_per_hypothesis": max_specs,
                   "max_experiments": max_exp},
        "null_control_rank_ic_t": null_ic_t,
        "held": held,
        "priority_order": list(CAMPAIGN_FEATURES),
    }
    return {"plan": plan, "results": results, "decisions": decisions,
            "held": held, "experiments_completed": completed,
            "experiments_failed": failed, "keep_for_research": keep,
            "spy_annualized_return": spy_ann}


def _held_reason(feature: str) -> str:
    return {
        "sector_neutral_momentum": "sector-neutral momentum needs a point-in-"
        "time GICS sector time series; the owned sector map is a current "
        "snapshot (not PIT) — held, not approximated (period/sector lookahead "
        "risk).",
        "insider_event": "insider-event studies need Form-4 transaction/filing "
        "timestamps with adequate history; owned coverage is sparse/recent — "
        "held.",
        "news_event": "regulatory/news-event studies need timestamped event "
        "fields with forward windows; owned company-direct coverage is sparse — "
        "held.",
    }.get(feature, "required owned point-in-time data is unavailable — held.")
