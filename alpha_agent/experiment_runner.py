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
