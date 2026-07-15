"""
api/current_alpha_operations.py — Phase 13-C/D/E daily-operations loaders.

This module adds the "Current Alpha Daily Operations Cockpit" read-only data
services on top of the Phase 13-B current-alpha preview. It loads the committed
*Phase 13-A current champion alpha paper-test package* (and, for the simulator,
the frozen Phase 10-L scored panel) and returns safe, preview-only payloads for
three Paper Trader endpoints:

    load_current_alpha_pnl(...)                 -> GET /v1/research/current-alpha/pnl
    load_current_alpha_actions_preview(...)     -> GET /v1/research/current-alpha/actions-preview
    load_current_alpha_rebalance_simulation(...)-> GET /v1/research/current-alpha/rebalance-simulator

Scope (read-only, by design — identical contract to Phase 13-B):
    - Reads ONLY local research-repo files (the Phase 13-A package CSV/JSON and,
      for the simulator, the frozen scored panel CSV). Writes no files.
    - Touches no database. Creates no signals, trade decisions, orders, or
      automation. Connects to no broker.
    - Never calls the prediction service or any external market-data provider,
      and requires no Nasdaq / Intrinio / FMP data.
    - Every metric is COMPUTED from real package/panel values; nothing is faked.
      The six safety badges and the safety flags are enforced constants (reused
      from the Phase 13-B module).

Operating-model honesty (the simulator):
    ``composite_sn`` is a quarterly (~63 trading-day) fundamental signal. The
    quarterly rebalance is genuinely simulated from the frozen panel. Daily
    rebalancing is REJECTED and weekly/monthly are marked "not justified by the
    signal frequency" rather than fabricated — rebalancing faster than the signal
    refreshes changes no holdings and only multiplies transaction cost. Daily
    *monitoring* remains valid; daily *trading* is not recommended here.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional, Union

from paper_trader.api.current_alpha_preview import (
    CurrentAlphaPreviewError,
    PACKAGE_JSON_NAME,
    SAFETY_BADGES,
    SAFETY_FLAGS,
    _read_csv_rows,
    _resolve_package_dir,
    load_current_alpha_preview,
)

# ---------------------------------------------------------------------------
# Frozen scored panel (Phase 10-L) — read-only simulator input
# ---------------------------------------------------------------------------

#: Environment variable that overrides the default frozen-panel path.
PANEL_PATH_ENV_VAR = "PAPER_TRADER_CURRENT_ALPHA_PANEL_PATH"

#: Default frozen Phase 10-L sector-neutral scored panel (research repo, read-only).
DEFAULT_PANEL_PATH = Path(
    r"C:\Users\binis\Stock_Prediction_app_push\research\output"
    r"\phase10l_historical_sector_neutral_scored_panel_reconstruction"
    r"\historical_sector_neutral_scored_panel.csv"
)

#: Portfolio side-cars used for PnL / action computation.
_PORTFOLIO_TOP25 = "current_alpha_paper_portfolio_top25.csv"
_PORTFOLIO_TOP50 = "current_alpha_paper_portfolio_top50.csv"
_TRACKING_TEMPLATE = "current_alpha_tracking_template.csv"
_MISSING_DATA = "current_alpha_missing_data_report.csv"
_SCORECARD = "current_alpha_go_no_go_scorecard.csv"

#: The full set of preview-only action labels (Phase 13-D).
ACTION_LABELS: tuple[str, ...] = (
    "ADD_PREVIEW",
    "HOLD_PREVIEW",
    "REMOVE_PREVIEW",
    "WAIT_FOR_PRICE_PREVIEW",
    "AVOID_PREVIEW",
    "REBALANCE_PREVIEW",
)

#: Every action row carries this — nothing here can imply live execution.
ORDER_ACTION_NONE = "NO_ORDER"

#: Checkpoint horizons (label, trading-day horizon, tracking-template column).
_CHECKPOINTS: tuple[tuple[str, int, str], ...] = (
    ("1 week", 5, "chk_1w_return"),
    ("1 month", 21, "chk_1m_return"),
    ("2 months", 42, "chk_2m_return"),
    ("63 trading days", 63, "chk_63d_return"),
)

_ACTION_SAFETY_NOTE = (
    "paper-only preview; no order, no signal, no trade decision; manual review required"
)


# ---------------------------------------------------------------------------
# Small numeric / date helpers (all pure, read-only)
# ---------------------------------------------------------------------------

def _to_float(value: Any) -> Optional[float]:
    """Parse a CSV cell into a float, or None if blank / non-numeric."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> Optional[int]:
    f = _to_float(value)
    return int(f) if f is not None else None


def _median(values: list[float]) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    n = len(ordered)
    mid = n // 2
    if n % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def _round(value: Optional[float], digits: int = 4) -> Optional[float]:
    return round(value, digits) if isinstance(value, (int, float)) else None


def _parse_date(text: Any) -> Optional[date]:
    if not text:
        return None
    try:
        return date.fromisoformat(str(text).strip())
    except (TypeError, ValueError):
        return None


def _add_trading_days(start: Optional[date], n: int) -> Optional[str]:
    """Add ``n`` trading days (Mon-Fri, holidays ignored) — an approximation."""
    if start is None:
        return None
    current = start
    added = 0
    # step forward one calendar day at a time, counting only weekdays
    from datetime import timedelta

    while added < n:
        current = current + timedelta(days=1)
        if current.weekday() < 5:  # 0=Mon .. 4=Fri
            added += 1
    return current.isoformat()


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safety_block() -> dict[str, Any]:
    """Enforced, always-on safety surface (identical to Phase 13-B)."""
    return {
        "safety_badges": list(SAFETY_BADGES),
        "safety": dict(SAFETY_FLAGS),
        # mirror each flag at the top level for simple consumers
        **dict(SAFETY_FLAGS),
    }


# ---------------------------------------------------------------------------
# Phase 13-C — Daily paper PnL
# ---------------------------------------------------------------------------

def _summarize_book(rows: list[dict[str, str]], label: str) -> dict[str, Any]:
    """Aggregate a paper-portfolio CSV into a PnL summary + per-name rows."""
    parsed: list[dict[str, Any]] = []
    covered: list[dict[str, Any]] = []
    missing = 0

    for r in rows:
        ticker = str(r.get("ticker", "")).strip()
        if not ticker or ticker.startswith("_"):
            continue  # skip blanks / aggregate rows like _EW_TOP25_PORTFOLIO
        status = str(r.get("price_status", "")).strip()
        ret = _to_float(r.get("paper_return_pct"))
        row = {
            "ticker": ticker,
            "sector": (r.get("sector") or None),
            "signal_composite_sn": _to_float(r.get("signal_composite_sn")),
            "target_weight": _to_float(r.get("target_weight")),
            "entry_price": _to_float(r.get("entry_price")),
            "current_price": _to_float(r.get("current_price")),
            "current_price_date": (str(r.get("current_price_date", "")).strip() or None),
            "paper_return_pct": _round(ret, 4),
            "price_status": (status or None),
            "order_action": (str(r.get("order_action", "")).strip() or ORDER_ACTION_NONE),
        }
        parsed.append(row)
        if status == "MARKED" and ret is not None:
            covered.append(row)
        else:
            missing += 1

    returns = [row["paper_return_pct"] for row in covered]
    n_up = sum(1 for x in returns if x is not None and x > 0)
    n_down = sum(1 for x in returns if x is not None and x < 0)
    avg = (sum(returns) / len(returns)) if returns else None
    med = _median(returns) if returns else None
    ranked = sorted(covered, key=lambda x: x["paper_return_pct"], reverse=True)

    def _slim(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [{"ticker": x["ticker"], "paper_return_pct": x["paper_return_pct"]} for x in items]

    return {
        "book": label,
        "covered_count": len(covered),
        "missing_count": missing,
        "total_count": len(parsed),
        "average_paper_return_pct": _round(avg, 4),
        "median_paper_return_pct": _round(med, 4),
        "min_return_pct": (min(returns) if returns else None),
        "max_return_pct": (max(returns) if returns else None),
        "n_up": n_up,
        "n_down": n_down,
        "hit_rate_pct": (_round(100.0 * n_up / len(returns), 2) if returns else None),
        "best_performers": _slim(ranked[:5]),
        "worst_performers": _slim(list(reversed(ranked))[:5]),
        "rows": parsed,
    }


def _checkpoint_plan(preview: dict[str, Any], tracking_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    """Build the 1w / 1m / 2m / 63d checkpoint plan (status derived from real fields)."""
    signal_date = _parse_date(preview.get("signal_date"))
    days_since = preview.get("days_since_signal")
    out: list[dict[str, Any]] = []
    for label, horizon_td, column in _CHECKPOINTS:
        captured_count = sum(
            1 for r in tracking_rows if str(r.get(column, "")).strip() not in ("", None)
        )
        elapsed = isinstance(days_since, (int, float)) and days_since >= horizon_td
        out.append({
            "label": label,
            "horizon_trading_days": horizon_td,
            "approx_target_date": _add_trading_days(signal_date, horizon_td),
            "window_elapsed": bool(elapsed),
            "captured": captured_count > 0,
            "captured_count": captured_count,
            "tracking_column": column,
            "status": (
                "WINDOW_ELAPSED_AWAITING_PRICE_REFRESH" if (elapsed and captured_count == 0)
                else ("CAPTURED" if captured_count > 0 else "PENDING")
            ),
            "note": "checkpoint returns are captured on a future owned-price refresh; none faked",
        })
    return out


def load_current_alpha_pnl(
    package_dir: Optional[Union[str, Path]] = None,
) -> dict[str, Any]:
    """
    Phase 13-C: compute the read-only daily paper PnL for the champion book.

    Loads the Phase 13-A package (which validates it and raises
    :class:`CurrentAlphaPreviewError` if it is missing/incomplete), then computes
    top25 / top50 PnL summaries from the paper-portfolio CSVs (``paper_return_pct``
    is already marked from owned local EOD prices — no market call is made).
    """
    preview = load_current_alpha_preview(package_dir)
    base = _resolve_package_dir(package_dir)

    top25_rows = _read_csv_rows(base / _PORTFOLIO_TOP25)
    top50_rows = _read_csv_rows(base / _PORTFOLIO_TOP50)
    tracking_rows = _read_csv_rows(base / _TRACKING_TEMPLATE)

    top25 = _summarize_book(top25_rows, "TOP25")
    top50 = _summarize_book(top50_rows, "TOP50")

    source_file_paths = [
        str(base / PACKAGE_JSON_NAME),
        str(base / _PORTFOLIO_TOP25),
        str(base / _PORTFOLIO_TOP50),
        str(base / _TRACKING_TEMPLATE),
        str(base / _MISSING_DATA),
        str(base / _SCORECARD),
    ]

    payload: dict[str, Any] = {
        "alpha_name": preview.get("alpha_name"),
        "signal_date": preview.get("signal_date"),
        "cross_section_month": preview.get("cross_section_month"),
        "decision": preview.get("decision"),
        "go_no_go": preview.get("go_no_go"),
        "package_date": preview.get("package_date"),
        "days_since_signal": preview.get("days_since_signal"),
        "holding_horizon_trading_days": preview.get("holding_horizon_trading_days"),
        "rebalance_cadence": preview.get("rebalance_cadence"),
        "next_rebalance_target": preview.get("next_rebalance_target"),
        "price_coverage": preview.get("price_coverage"),
        "price_source": "EODHD_LOCAL_EOD(adjusted_close)",
        "mark_method": "adjusted-close ratio, entry at/just-before signal date vs latest local mark",
        "top25": top25,
        "top50": top50,
        "checkpoint_plan": _checkpoint_plan(preview, tracking_rows),
        "missing_data_report": preview.get("missing_data_report") or [],
        "go_no_go_scorecard": preview.get("go_no_go_scorecard") or [],
        "caveats": preview.get("caveats") or [],
        "source_file_paths": source_file_paths,
        "loaded_at": _iso_now(),
    }
    payload.update(_safety_block())
    return payload


# ---------------------------------------------------------------------------
# Phase 13-D — Preview-only action plan
# ---------------------------------------------------------------------------

def _actions_from_portfolio(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    """Derive ADD_PREVIEW / WAIT_FOR_PRICE_PREVIEW rows from a portfolio CSV."""
    out: list[dict[str, Any]] = []
    rank = 0
    for r in rows:
        ticker = str(r.get("ticker", "")).strip()
        if not ticker or ticker.startswith("_"):
            continue
        rank += 1
        status = str(r.get("price_status", "")).strip()
        priced = status == "MARKED"
        if priced:
            action = "ADD_PREVIEW"
            reason = (
                f"top-ranked (rank {rank}) by composite_sn with a local entry price; "
                "paper add candidate"
            )
        else:
            action = "WAIT_FOR_PRICE_PREVIEW"
            reason = (
                f"top-ranked (rank {rank}) but no local price yet; wait for an owned "
                "price refresh before any paper entry"
            )
        out.append({
            "action_type": action,
            "ticker": ticker,
            "source_rank": rank,
            "composite_sn": _to_float(r.get("signal_composite_sn")),
            "sector": (r.get("sector") or None),
            "side": (str(r.get("side", "")).strip() or "LONG"),
            "price_status": (status or None),
            "reason": reason,
            "safety_note": _ACTION_SAFETY_NOTE,
            "order_action": ORDER_ACTION_NONE,
        })
    return out


def _avoid_actions(bottom25: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Derive AVOID_PREVIEW rows from the bottom-25 short-only diagnostic."""
    out: list[dict[str, Any]] = []
    for r in bottom25:
        ticker = str(r.get("ticker", "")).strip()
        if not ticker:
            continue
        out.append({
            "action_type": "AVOID_PREVIEW",
            "ticker": ticker,
            "source_rank": _to_int(r.get("rank_from_bottom")),
            "composite_sn": _to_float(r.get("composite_sn")),
            "sector": (r.get("sector") or None),
            "side": "AVOID",
            "price_status": None,
            "reason": (
                "bottom-ranked short-only diagnostic; avoid for the long book — "
                "NOT a live short recommendation"
            ),
            "safety_note": _ACTION_SAFETY_NOTE,
            "order_action": ORDER_ACTION_NONE,
        })
    return out


def _count_actions(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {label: 0 for label in ACTION_LABELS}
    for r in rows:
        key = r.get("action_type")
        if key in counts:
            counts[key] += 1
    return counts


def load_current_alpha_actions_preview(
    package_dir: Optional[Union[str, Path]] = None,
) -> dict[str, Any]:
    """
    Phase 13-D: derive a paper-only action plan from the Phase 13-A package.

    Initial-state logic (no persistent current-alpha book exists yet):
      - priced top names       -> ADD_PREVIEW
      - unpriced top names     -> WAIT_FOR_PRICE_PREVIEW
      - bottom-25 diagnostic   -> AVOID_PREVIEW
    Every row carries ``order_action = NO_ORDER``. HOLD / REMOVE / REBALANCE
    previews are defined labels that only activate once a paper book is tracked.
    """
    preview = load_current_alpha_preview(package_dir)
    base = _resolve_package_dir(package_dir)

    top25_rows = _read_csv_rows(base / _PORTFOLIO_TOP25)
    top50_rows = _read_csv_rows(base / _PORTFOLIO_TOP50)

    top25_actions = _actions_from_portfolio(top25_rows)
    top50_actions = _actions_from_portfolio(top50_rows)
    avoid_actions = _avoid_actions(preview.get("bottom25_avoid") or [])

    # Canonical plan universe = top50 (superset of top25) + avoid list.
    counts = _count_actions(top50_actions + avoid_actions)

    warnings: list[str] = [
        "No persistent current-alpha paper book exists yet; this is the INITIAL "
        "action plan derived from the Phase 13-A package.",
        "HOLD_PREVIEW / REMOVE_PREVIEW / REBALANCE_PREVIEW activate only once a "
        "paper book has been tracked over time.",
    ]
    warnings.extend(preview.get("caveats") or [])

    payload: dict[str, Any] = {
        "alpha_name": preview.get("alpha_name"),
        "signal_date": preview.get("signal_date"),
        "cross_section_month": preview.get("cross_section_month"),
        "decision": preview.get("decision"),
        "go_no_go": preview.get("go_no_go"),
        "action_labels": list(ACTION_LABELS),
        "top25_action_plan": top25_actions,
        "top50_action_plan": top50_actions,
        "avoid_list": avoid_actions,
        "counts_by_action_type": counts,
        "top25_counts": _count_actions(top25_actions),
        "top50_counts": _count_actions(top50_actions),
        "avoid_count": len(avoid_actions),
        "paper_only": True,
        "order_action_all": ORDER_ACTION_NONE,
        "explicit_notice": (
            "These are paper-only preview actions. No order is created. No signal "
            "is created. No trade decision is created. Manual review required."
        ),
        "warnings": warnings,
        "source_file_paths": [
            str(base / PACKAGE_JSON_NAME),
            str(base / _PORTFOLIO_TOP25),
            str(base / _PORTFOLIO_TOP50),
            str(base / "current_alpha_bottom25_avoid_list.csv"),
        ],
        "loaded_at": _iso_now(),
    }
    payload.update(_safety_block())
    return payload


# ---------------------------------------------------------------------------
# Phase 13-E — Rebalance-frequency simulator
# ---------------------------------------------------------------------------

def _resolve_panel_path(panel_path: Optional[Union[str, Path]]) -> Path:
    if panel_path is not None:
        return Path(panel_path)
    env_value = os.environ.get(PANEL_PATH_ENV_VAR)
    if env_value:
        return Path(env_value)
    return DEFAULT_PANEL_PATH


def _read_panel(path: Path) -> list[dict[str, Any]]:
    """Read only the columns the simulator needs from the frozen scored panel."""
    rows: list[dict[str, Any]] = []
    for r in _read_csv_rows(path):
        reb = str(r.get("rebalance_date", "")).strip()
        ticker = str(r.get("ticker", "")).strip()
        if not reb or not ticker:
            continue
        has_fwd = str(r.get("has_forward_return", "")).strip().lower() in ("true", "1", "yes")
        rows.append({
            "date": reb,
            "ticker": ticker,
            "composite_sn": _to_float(r.get("composite_sn")),
            "fwd": _to_float(r.get("forward_63d_return")),
            "has_fwd": has_fwd,
        })
    return rows


def _quarter_key(d: date) -> str:
    """Calendar-quarter bucket key, e.g. 2026Q2 (sortable within a century)."""
    return f"{d.year}Q{(d.month - 1) // 3 + 1}"


def _signal_refresh_trading_days(rows: list[dict[str, Any]]) -> Optional[int]:
    """Median per-ticker gap between rebalance dates -> trading days (~63 = quarterly)."""
    by_ticker: dict[str, list[date]] = {}
    for r in rows:
        d = _parse_date(r["date"])
        if d is not None:
            by_ticker.setdefault(r["ticker"], []).append(d)
    gaps: list[float] = []
    for dates in by_ticker.values():
        if len(dates) < 2:
            continue
        dates.sort()
        gaps.extend(float((dates[i] - dates[i - 1]).days)
                    for i in range(1, len(dates)) if (dates[i] - dates[i - 1]).days > 0)
    median_gap = _median(gaps)
    if median_gap is None:
        return None
    return int(round(median_gap * 5.0 / 7.0))  # calendar -> trading days


def _quarter_buckets(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Bucket per-ticker events into calendar quarters (dedup to latest event/ticker).

    The frozen panel is an EVENT panel: each row is a ticker's own fundamental
    rebalance date. A 63-trading-day (one-quarter) holding horizon requires
    NON-overlapping periods, so events are grouped by calendar quarter — the exact
    shape of the Phase 10-D quarterly backtest.
    """
    latest: dict[tuple[str, str], tuple[date, dict[str, Any]]] = {}
    for r in rows:
        d = _parse_date(r["date"])
        if d is None or r["composite_sn"] is None:
            continue
        key = (_quarter_key(d), r["ticker"])
        prev = latest.get(key)
        if prev is None or d > prev[0]:
            latest[key] = (d, r)
    buckets: dict[str, list[dict[str, Any]]] = {}
    for (qkey, _ticker), (_d, r) in latest.items():
        buckets.setdefault(qkey, []).append(r)
    return buckets


def _sorted_quarter_keys(buckets: dict[str, list[dict[str, Any]]]) -> list[str]:
    return sorted(buckets.keys(), key=lambda k: (int(k[:4]), int(k[5:])))


def _simulate_quarterly(
    buckets: dict[str, list[dict[str, Any]]],
    quarter_keys: list[str],
    top_n: int,
) -> dict[str, Any]:
    """Real quarterly EW long-only backtest of composite_sn from the frozen panel."""
    period_returns: list[float] = []
    selected_sets: list[set] = []
    total_covered = 0
    total_missing = 0

    for qkey in quarter_keys:
        cross = [x for x in buckets.get(qkey, []) if x["composite_sn"] is not None]
        if not cross:
            continue
        ranked = sorted(cross, key=lambda x: x["composite_sn"], reverse=True)
        selection = ranked[:top_n]
        covered = [x for x in selection if x["has_fwd"] and x["fwd"] is not None]
        if not covered:
            continue  # most-recent quarter(s) have no forward return yet
        period_returns.append(sum(x["fwd"] for x in covered) / len(covered))
        selected_sets.append({x["ticker"] for x in selection})
        total_covered += len(covered)
        total_missing += (len(selection) - len(covered))

    n = len(period_returns)
    if n == 0:
        return {
            "n_periods": 0, "avg_return_pct": None, "cumulative_return_pct": None,
            "max_drawdown_pct": None, "hit_rate_pct": None, "turnover": None,
            "txn_cost_25bps_pct": None, "txn_cost_50bps_pct": None,
            "net_cumulative_25bps_pct": None, "net_cumulative_50bps_pct": None,
            "coverage_count": total_covered, "missing_price_count": total_missing,
        }

    avg = sum(period_returns) / n

    equity = 1.0
    curve: list[float] = []
    for r in period_returns:
        equity *= (1.0 + r)
        curve.append(equity)
    cumulative = equity - 1.0

    peak = curve[0]
    max_dd = 0.0
    for v in curve:
        peak = max(peak, v)
        dd = v / peak - 1.0
        max_dd = min(max_dd, dd)

    hit = sum(1 for r in period_returns if r > 0) / n

    one_way = []
    for i in range(1, len(selected_sets)):
        added = len(selected_sets[i] - selected_sets[i - 1])
        one_way.append(added / top_n)
    avg_turnover = (sum(one_way) / len(one_way)) if one_way else 1.0

    def _net_cumulative(bps: float) -> float:
        rate = bps / 10000.0
        eq = 1.0
        for i, r in enumerate(period_returns):
            turnover = 1.0 if i == 0 else (len(selected_sets[i] - selected_sets[i - 1]) / top_n)
            eq *= (1.0 + r - turnover * rate)
        return eq - 1.0

    return {
        "n_periods": n,
        "avg_return_pct": _round(avg * 100.0, 4),
        "cumulative_return_pct": _round(cumulative * 100.0, 4),
        "max_drawdown_pct": _round(max_dd * 100.0, 4),
        "hit_rate_pct": _round(hit * 100.0, 2),
        "turnover": _round(avg_turnover, 4),
        "txn_cost_25bps_pct": _round(avg_turnover * 25.0 / 10000.0 * 100.0, 4),
        "txn_cost_50bps_pct": _round(avg_turnover * 50.0 / 10000.0 * 100.0, 4),
        "net_cumulative_25bps_pct": _round(_net_cumulative(25.0) * 100.0, 4),
        "net_cumulative_50bps_pct": _round(_net_cumulative(50.0) * 100.0, 4),
        "coverage_count": total_covered,
        "missing_price_count": total_missing,
    }


def _faster_than_signal_freq(
    name: str,
    per_year: int,
    verdict: str,
    signal_refresh_td: Optional[int],
) -> dict[str, Any]:
    """Structured 'not simulated — signal too slow' result for daily/weekly/monthly."""
    refresh = signal_refresh_td if signal_refresh_td else 63
    return {
        "frequency": name,
        "status": "NOT_SIMULATED_SIGNAL_TOO_SLOW",
        "supported_by_signal_frequency": False,
        "rebalance_events_per_year": per_year,
        "top25": None,
        "top50": None,
        "verdict": verdict,
        "note": (
            f"composite_sn refreshes about every {refresh} trading days (quarterly); "
            f"{name} rebalancing changes no holdings between refreshes and only multiplies "
            "transaction cost. Not simulated — a genuine "
            f"{name} signal would be a separate, unvalidated system, so no returns are fabricated."
        ),
    }


def load_current_alpha_rebalance_simulation(
    package_dir: Optional[Union[str, Path]] = None,
    panel_path: Optional[Union[str, Path]] = None,
) -> dict[str, Any]:
    """
    Phase 13-E: evaluate rebalance frequency for the champion alpha.

    Quarterly is genuinely simulated from the frozen Phase 10-L scored panel
    (EW long-only top25 / top50). Daily is rejected and weekly/monthly are marked
    not-justified-by-signal-frequency rather than fabricated. Robust: a missing or
    too-thin panel yields ``SIMULATION_INSUFFICIENT_DATA`` with warnings, never a
    crash. Still raises :class:`CurrentAlphaPreviewError` (mapped to 503) only when
    the Phase 13-A package itself is absent.
    """
    preview = load_current_alpha_preview(package_dir)
    panel_p = _resolve_panel_path(panel_path)

    warnings: list[str] = []
    signal_refresh_td: Optional[int] = None
    quarterly_result: dict[str, Any]
    simulation_status = "SIMULATION_INSUFFICIENT_DATA"
    recommendation = "SIMULATION_INSUFFICIENT_DATA"
    panel_meta: dict[str, Any] = {
        "panel_available": False,
        "panel_path": str(panel_p),
        "n_rows": 0,
        "n_rebalance_dates": 0,
    }

    if not panel_p.is_file():
        warnings.append(
            f"Frozen scored panel not found at {panel_p}; the quarterly simulation "
            "is unavailable. (No external data is fetched to compensate.)"
        )
        quarterly_result = {
            "frequency": "quarterly", "status": "SIMULATION_INSUFFICIENT_DATA",
            "supported_by_signal_frequency": True, "rebalance_events_per_year": 4,
            "top25": None, "top50": None, "verdict": "SIMULATION_INSUFFICIENT_DATA",
            "note": "frozen scored panel unavailable locally",
        }
    else:
        panel_rows = _read_panel(panel_p)
        distinct_dates = {row["date"] for row in panel_rows}
        buckets = _quarter_buckets(panel_rows)
        quarter_keys = _sorted_quarter_keys(buckets)
        panel_meta.update({
            "panel_available": True,
            "n_rows": len(panel_rows),
            "n_rebalance_dates": len(distinct_dates),
            "n_quarters": len(quarter_keys),
        })
        signal_refresh_td = _signal_refresh_trading_days(panel_rows)

        if not panel_rows or len(quarter_keys) < 4:
            warnings.append(
                "Frozen scored panel is empty or has too few calendar quarters for a "
                "stable simulation."
            )
            quarterly_result = {
                "frequency": "quarterly", "status": "SIMULATION_INSUFFICIENT_DATA",
                "supported_by_signal_frequency": True, "rebalance_events_per_year": 4,
                "top25": None, "top50": None, "verdict": "SIMULATION_INSUFFICIENT_DATA",
                "note": "insufficient calendar quarters in the frozen panel",
            }
        else:
            q25 = _simulate_quarterly(buckets, quarter_keys, 25)
            q50 = _simulate_quarterly(buckets, quarter_keys, 50)
            panel_meta["n_periods_used"] = q25["n_periods"]
            if q25["n_periods"] >= 4:
                simulation_status = "SIMULATED"
                recommendation = "QUARTERLY_REBALANCE_CANDIDATE"
                quarterly_result = {
                    "frequency": "quarterly",
                    "status": "SIMULATED",
                    "supported_by_signal_frequency": True,
                    "rebalance_events_per_year": 4,
                    "top25": q25,
                    "top50": q50,
                    "verdict": "QUARTERLY_REBALANCE_CANDIDATE",
                    "note": (
                        f"quarterly matches the ~{signal_refresh_td or 63}-trading-day signal "
                        "refresh; real EW long-only backtest from the frozen panel."
                    ),
                }
            else:
                warnings.append(
                    f"Only {q25['n_periods']} quarterly periods had forward returns; "
                    "insufficient for a stable simulation."
                )
                quarterly_result = {
                    "frequency": "quarterly", "status": "SIMULATION_INSUFFICIENT_DATA",
                    "supported_by_signal_frequency": True, "rebalance_events_per_year": 4,
                    "top25": q25, "top50": q50, "verdict": "SIMULATION_INSUFFICIENT_DATA",
                    "note": "too few periods with forward returns",
                }

    frequencies = {
        "daily": _faster_than_signal_freq("daily", 252, "DAILY_REBALANCE_REJECTED", signal_refresh_td),
        "weekly": _faster_than_signal_freq("weekly", 52, "NOT_JUSTIFIED_BY_SIGNAL_FREQUENCY", signal_refresh_td),
        "monthly": _faster_than_signal_freq("monthly", 12, "NOT_JUSTIFIED_BY_SIGNAL_FREQUENCY", signal_refresh_td),
        "quarterly": quarterly_result,
    }

    payload: dict[str, Any] = {
        "alpha_name": preview.get("alpha_name"),
        "signal_date": preview.get("signal_date"),
        "decision": preview.get("decision"),
        "go_no_go": preview.get("go_no_go"),
        "holding_horizon_trading_days": preview.get("holding_horizon_trading_days"),
        "rebalance_cadence": preview.get("rebalance_cadence"),
        "simulation_status": simulation_status,
        "recommendation": recommendation,
        "signal_refresh_trading_days": signal_refresh_td,
        "daily_rebalance_supported": False,
        "daily_trading_recommended": False,
        "daily_monitoring_valid": True,
        "frequencies": frequencies,
        "explanation": (
            "composite_sn is a quarterly (~63 trading-day) fundamental signal. Rebalancing "
            "faster than it refreshes changes no holdings and only adds transaction cost, so "
            "daily rebalancing is rejected and weekly/monthly are not justified by the signal "
            "frequency. Daily MONITORING of the paper book stays valid; daily TRADING is not "
            "recommended without a separate, independently validated daily signal."
        ),
        "panel": panel_meta,
        "warnings": warnings,
        "caveats": preview.get("caveats") or [],
        "source_file_paths": [
            str(_resolve_package_dir(package_dir) / PACKAGE_JSON_NAME),
            str(panel_p),
        ],
        "loaded_at": _iso_now(),
    }
    payload.update(_safety_block())
    return payload


__all__ = [
    "load_current_alpha_pnl",
    "load_current_alpha_actions_preview",
    "load_current_alpha_rebalance_simulation",
    "CurrentAlphaPreviewError",
    "ACTION_LABELS",
    "ORDER_ACTION_NONE",
    "PANEL_PATH_ENV_VAR",
    "DEFAULT_PANEL_PATH",
]
