"""
api/current_alpha_decision_gate.py — Phase 13-J paper book decision gate.

Read-only *daily operating decision layer* over the existing Phase 13-I historical
daily mark backfill artifacts (``GET /v1/research/current-alpha/decision-gate``). It
reads ONLY the local Phase 13-I reconstruction (the same artifact the performance
loader reads) and computes, without changing anything about the champion:

    - which book is the PROVISIONAL primary paper book (never production / live),
    - a single operating decision (continue monitoring / provisional primary /
      no-clear-primary / performance-deteriorating-review / risk-breach-review /
      insufficient-forward-history),
    - per-book rolling 5 / 10 / 20-mark return + excess changes, current drawdown,
      volatility, positive-day %, outperform-SPY %, concentration, coverage,
    - paper-review risk triggers (coverage < 90%, current drawdown <= -10%, current
      excess <= -5pp, concentration >= 50%, stale latest mark > 3 calendar days),
    - quarterly (63 trading-day) rebalance-review readiness.

Strict scope (read-only, paper-only — enforced):
    - It ONLY reads local JSON files written by the Phase 13-I runner (via the
      Phase 13-I performance loader plus the per-date history files). It writes no
      files, launches no subprocess, and touches no database.
    - It changes nothing about ``composite_sn``: no reranking, no rebalancing, no
      orders, no signals, no trade decisions. It connects to no broker, runs no
      automation, enables no live trading, and calls neither the prediction service
      nor any external / paid market-data provider.
    - Every trigger it raises is a *paper-review* trigger — it never produces a
      trading action, and it promotes no book to live trading. A missing / rejected
      backfill yields a controlled status (HTTP 200), never a crash.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Optional, Union

from paper_trader.api.current_alpha_preview import SAFETY_FLAGS
from paper_trader.api.current_alpha_book import (
    _calendar_age_days,
    _iso_now,
    _mark_freshness,
    _read_json_file,
    _today_iso,
)
from paper_trader.api.current_alpha_performance import (
    _TOP25_HISTORY,
    _TOP50_HISTORY,
    _resolve_backfill_dir,
    load_current_alpha_performance,
)

PHASE = "13-J"

# --------------------------------------------------------------------------- #
# Decision enum (one is always returned)
# --------------------------------------------------------------------------- #
DEC_CONTINUE = "CONTINUE_PAPER_MONITORING"
DEC_TOP50 = "PROVISIONAL_TOP50_PRIMARY"
DEC_TOP25 = "PROVISIONAL_TOP25_PRIMARY"
DEC_NO_CLEAR = "NO_CLEAR_PRIMARY"
DEC_DETERIORATING = "PERFORMANCE_DETERIORATING_REVIEW"
DEC_RISK = "RISK_THRESHOLD_BREACH_REVIEW"
DEC_INSUFFICIENT = "INSUFFICIENT_FORWARD_HISTORY"

# --------------------------------------------------------------------------- #
# Book-role status
# --------------------------------------------------------------------------- #
ROLE_TOP50 = "PROVISIONAL_PRIMARY_TOP50"
ROLE_TOP25 = "PROVISIONAL_PRIMARY_TOP25"
ROLE_NONE = "NO_PRIMARY_BOOK_YET"

# --------------------------------------------------------------------------- #
# Quarterly rebalance-readiness enum
# --------------------------------------------------------------------------- #
READY_EARLY = "MONITORING_EARLY"
READY_MID = "MONITORING_MID_CYCLE"
READY_APPROACHING = "REBALANCE_REVIEW_APPROACHING"
READY_DUE = "REBALANCE_REVIEW_DUE"
READY_OVERDUE = "REBALANCE_REVIEW_OVERDUE"

# --------------------------------------------------------------------------- #
# Gate status (data-availability, distinct from the decision)
# --------------------------------------------------------------------------- #
GATE_READY = "DECISION_READY"
GATE_NO_BACKFILL = "NO_BACKFILL_YET"
GATE_NOT_PUBLISHED = "BACKFILL_NOT_PUBLISHED"

# --------------------------------------------------------------------------- #
# Thresholds (all documented; risk thresholds are the exact Phase 13-J spec)
# --------------------------------------------------------------------------- #
#: Minimum completed forward marks before any provisional-book call is made.
MIN_FORWARD_OBS = 20
#: Champion cadence: a 63-trading-day / quarterly holding period.
REBALANCE_TARGET_TRADING_DAYS = 63
#: Rolling look-back windows (in completed marks).
ROLLING_WINDOWS = (5, 10, 20)

#: Paper-review RISK triggers (never a trading action).
RISK_MIN_COVERAGE_PCT = 90.0      # latest coverage below 90%
RISK_DRAWDOWN_PCT = -10.0         # current drawdown <= -10%
RISK_EXCESS_PP = -5.0             # current excess vs SPY <= -5 percentage points
RISK_CONCENTRATION_PCT = 50.0     # contributor concentration >= 50%
RISK_STALE_MARK_DAYS = 3          # stale latest mark greater than 3 calendar days

#: PERFORMANCE-DETERIORATION review triggers (recent momentum turning down).
DETERIORATION_ROLLING_RETURN_PCT = -3.0   # rolling 10-mark return change <= -3%
DETERIORATION_ROLLING_EXCESS_PP = -3.0    # rolling 10-mark excess change <= -3pp
DETERIORATION_WINDOW = 10

#: The six required safety phrases for the decision-gate surface.
DECISION_GATE_SAFETY_BADGES = (
    "PROVISIONAL PAPER BOOK ONLY",
    "NOT LIVE-TRADING APPROVAL",
    "NO ORDERS",
    "NO BROKER",
    "NO AUTOMATION",
    "MANUAL REVIEW REQUIRED",
)

_NOT_LIVE_NOTICE = (
    "This is a provisional paper-book comparison for manual review only. It is NOT "
    "live-trading approval and promotes no book to live trading. It creates no "
    "orders, no signals, and no trade decisions; it does not rerank or rebalance the "
    "champion. A short forward window is not alpha validation."
)


# --------------------------------------------------------------------------- #
# Small pure helpers
# --------------------------------------------------------------------------- #

def _num(x: Any) -> Optional[float]:
    return float(x) if isinstance(x, (int, float)) else None


def _round(x: Any, nd: int = 4) -> Optional[float]:
    return round(x, nd) if isinstance(x, (int, float)) else None


def _safety_block() -> dict[str, Any]:
    """Enforced safety surface. ``promotes_to_live`` stays False; nothing here
    implies execution, live trading, or a production book."""
    return {
        "safety_badges": list(DECISION_GATE_SAFETY_BADGES),
        "safety": dict(SAFETY_FLAGS),
        **dict(SAFETY_FLAGS),
        "frozen_holdings": True,
        "daily_rebalancing": False,
        "reranking": False,
        "promotes_to_live": False,
        "is_production_book": False,
        "is_live_trading_approval": False,
        "order_action_all": "NO_ORDER",
        "not_live_approval_notice": _NOT_LIVE_NOTICE,
    }


def _rolling_change(series: list[float], window: int) -> Optional[float]:
    """Change in a cumulative series over the last ``window`` marks (current minus
    the value ``window`` marks ago). None when there are not enough marks."""
    if len(series) <= window:
        return None
    return _round(series[-1] - series[-1 - window], 4)


def _current_drawdown_pct(cum_series: list[float]) -> Optional[float]:
    """Current drawdown from the running peak, from a cumulative-return series
    (equity = 1 + cum/100; drawdown = last_equity / peak_equity - 1)."""
    if not cum_series:
        return None
    peak = None
    for cum in cum_series:
        eq = 1.0 + cum / 100.0
        if peak is None or eq > peak:
            peak = eq
    last_eq = 1.0 + cum_series[-1] / 100.0
    return _round((last_eq / peak - 1.0) * 100.0, 4) if peak else 0.0


# --------------------------------------------------------------------------- #
# Per-book scorecard
# --------------------------------------------------------------------------- #

def _history_series(rows: list[dict[str, Any]]) -> tuple[list[float], list[float]]:
    """Cumulative-return and cumulative-excess series from the per-date history
    rows (ordered as stored: signal date first, latest mark last)."""
    cum: list[float] = []
    exc: list[float] = []
    for r in rows:
        c = _num(r.get("cumulative_return_pct"))
        if c is None:
            c = _num(r.get("average_return_pct"))
        e = _num(r.get("excess_return_vs_spy_pct_points"))
        if c is not None:
            cum.append(c)
        if e is not None:
            exc.append(e)
    return cum, exc


def _latest_coverage_pct(rows: list[dict[str, Any]], analytics: dict[str, Any]) -> Optional[float]:
    """Latest completed-mark coverage percentage (from the last history row)."""
    for r in reversed(rows):
        cov = _num(r.get("coverage_pct"))
        if cov is not None:
            return cov
    return None


def _book_scorecard(
    label: str,
    analytics: Optional[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Compute the compact per-book scorecard used by the decision + the UI."""
    a = analytics or {}
    cum, exc = _history_series(rows)
    rolling: dict[str, Any] = {}
    for w in ROLLING_WINDOWS:
        rolling["rolling_%d_return_change_pct_points" % w] = _rolling_change(cum, w)
        rolling["rolling_%d_excess_change_pct_points" % w] = _rolling_change(exc, w)

    current_return = _num(a.get("current_cumulative_return_pct"))
    if current_return is None and cum:
        current_return = cum[-1]
    current_excess = _num(a.get("current_excess_return_pct_points"))
    if current_excess is None and exc:
        current_excess = exc[-1]

    return {
        "label": label,
        "book_id": a.get("book_id"),
        "book_size": a.get("book_size"),
        "n_observations": a.get("n_observations") if a.get("n_observations") is not None else len(rows),
        "current_return_pct": current_return,
        "current_excess_return_pct_points": current_excess,
        "spy_cumulative_return_pct": _num(a.get("spy_cumulative_return_pct")),
        "max_drawdown_pct": _num(a.get("max_drawdown_pct")),
        "current_drawdown_pct": _current_drawdown_pct(cum),
        "daily_change_volatility_pct_points": _num(a.get("daily_change_volatility_pct_points")),
        "pct_positive_daily_changes": _num(a.get("pct_positive_daily_changes")),
        "pct_days_outperforming_spy": _num(a.get("pct_days_outperforming_spy")),
        "contributor_concentration_top5_pct": _num(a.get("contributor_concentration_top5_pct")),
        "n_coverage_warning_dates": a.get("n_coverage_warning_dates"),
        "n_insufficient_coverage_dates": a.get("n_insufficient_coverage_dates"),
        "latest_coverage_pct": _latest_coverage_pct(rows, a),
        "order_action": "NO_ORDER",
        **rolling,
    }


# --------------------------------------------------------------------------- #
# Risk-review triggers (paper-review only — never a trading action)
# --------------------------------------------------------------------------- #

def _risk_triggers(book: dict[str, Any], *, stale_mark_days: Optional[int]) -> list[dict[str, Any]]:
    triggers: list[dict[str, Any]] = []
    cov = book.get("latest_coverage_pct")
    if isinstance(cov, (int, float)) and cov < RISK_MIN_COVERAGE_PCT:
        triggers.append({"trigger": "LATEST_COVERAGE_BELOW_MIN", "value": cov,
                         "threshold": RISK_MIN_COVERAGE_PCT,
                         "detail": "Latest completed-mark coverage is below %.0f%%." % RISK_MIN_COVERAGE_PCT})
    dd = book.get("current_drawdown_pct")
    if isinstance(dd, (int, float)) and dd <= RISK_DRAWDOWN_PCT:
        triggers.append({"trigger": "CURRENT_DRAWDOWN_BREACH", "value": dd,
                         "threshold": RISK_DRAWDOWN_PCT,
                         "detail": "Current drawdown is at or beyond %.0f%%." % RISK_DRAWDOWN_PCT})
    exc = book.get("current_excess_return_pct_points")
    if isinstance(exc, (int, float)) and exc <= RISK_EXCESS_PP:
        triggers.append({"trigger": "CURRENT_EXCESS_BREACH", "value": exc,
                         "threshold": RISK_EXCESS_PP,
                         "detail": "Current excess vs SPY is at or below %.0fpp." % RISK_EXCESS_PP})
    conc = book.get("contributor_concentration_top5_pct")
    if isinstance(conc, (int, float)) and conc >= RISK_CONCENTRATION_PCT:
        triggers.append({"trigger": "CONCENTRATION_BREACH", "value": conc,
                         "threshold": RISK_CONCENTRATION_PCT,
                         "detail": "Top-5 contributor concentration is at or above %.0f%%." % RISK_CONCENTRATION_PCT})
    if isinstance(stale_mark_days, int) and stale_mark_days > RISK_STALE_MARK_DAYS:
        triggers.append({"trigger": "STALE_LATEST_MARK", "value": stale_mark_days,
                         "threshold": RISK_STALE_MARK_DAYS,
                         "detail": "Latest financial mark is more than %d calendar days old." % RISK_STALE_MARK_DAYS})
    return triggers


def _deterioration_reasons(book: dict[str, Any]) -> list[dict[str, Any]]:
    reasons: list[dict[str, Any]] = []
    ret = book.get("rolling_%d_return_change_pct_points" % DETERIORATION_WINDOW)
    exc = book.get("rolling_%d_excess_change_pct_points" % DETERIORATION_WINDOW)
    if isinstance(ret, (int, float)) and ret <= DETERIORATION_ROLLING_RETURN_PCT:
        reasons.append({"trigger": "ROLLING_RETURN_DECLINE", "value": ret,
                        "threshold": DETERIORATION_ROLLING_RETURN_PCT,
                        "detail": "Rolling %d-mark return change is at or below %.0f%%."
                                  % (DETERIORATION_WINDOW, DETERIORATION_ROLLING_RETURN_PCT)})
    if isinstance(exc, (int, float)) and exc <= DETERIORATION_ROLLING_EXCESS_PP:
        reasons.append({"trigger": "ROLLING_EXCESS_DECLINE", "value": exc,
                        "threshold": DETERIORATION_ROLLING_EXCESS_PP,
                        "detail": "Rolling %d-mark excess change is at or below %.0fpp."
                                  % (DETERIORATION_WINDOW, DETERIORATION_ROLLING_EXCESS_PP)})
    return reasons


# --------------------------------------------------------------------------- #
# Book role: the evidence-driven provisional-primary rule
# --------------------------------------------------------------------------- #

def _dominance(a: dict[str, Any], b: dict[str, Any]) -> tuple[bool, list[dict[str, Any]]]:
    """Does book ``a`` qualify as PROVISIONAL primary over book ``b``?

    All five evidence criteria must hold: no insufficient-coverage dates; current
    excess vs SPY > 0; shallower max drawdown than ``b``; lower daily volatility
    than ``b``; lower contributor concentration than ``b``.
    """
    a_insuff = a.get("n_insufficient_coverage_dates")
    a_exc = a.get("current_excess_return_pct_points")
    a_dd, b_dd = a.get("max_drawdown_pct"), b.get("max_drawdown_pct")
    a_vol, b_vol = a.get("daily_change_volatility_pct_points"), b.get("daily_change_volatility_pct_points")
    a_conc, b_conc = a.get("contributor_concentration_top5_pct"), b.get("contributor_concentration_top5_pct")

    checks = [
        ("no_insufficient_coverage_dates", a_insuff == 0),
        ("current_excess_positive",
         isinstance(a_exc, (int, float)) and a_exc > 0),
        ("shallower_max_drawdown_than_other",
         isinstance(a_dd, (int, float)) and isinstance(b_dd, (int, float)) and a_dd > b_dd),
        ("lower_daily_volatility_than_other",
         isinstance(a_vol, (int, float)) and isinstance(b_vol, (int, float)) and a_vol < b_vol),
        ("lower_contributor_concentration_than_other",
         isinstance(a_conc, (int, float)) and isinstance(b_conc, (int, float)) and a_conc < b_conc),
    ]
    detail = [{"criterion": name, "passed": bool(ok)} for name, ok in checks]
    return all(ok for _, ok in checks), detail


def _book_role(top25: dict[str, Any], top50: dict[str, Any]) -> dict[str, Any]:
    """Resolve the provisional-primary book (Top50 evaluated first, per the
    evidence rule; the symmetric test lets Top25 win if it dominates instead)."""
    top50_ok, top50_checks = _dominance(top50, top25)
    if top50_ok:
        return {"status": ROLE_TOP50, "primary_label": "TOP50", "criteria": top50_checks}
    top25_ok, top25_checks = _dominance(top25, top50)
    if top25_ok:
        return {"status": ROLE_TOP25, "primary_label": "TOP25", "criteria": top25_checks}
    return {"status": ROLE_NONE, "primary_label": None,
            "criteria_top50": top50_checks, "criteria_top25": top25_checks}


def _book_ref(book: dict[str, Any], *, role: str) -> dict[str, Any]:
    return {
        "role": role,
        "label": book.get("label"),
        "book_id": book.get("book_id"),
        "book_size": book.get("book_size"),
        "current_return_pct": book.get("current_return_pct"),
        "current_excess_return_pct_points": book.get("current_excess_return_pct_points"),
    }


# --------------------------------------------------------------------------- #
# Quarterly rebalance-review readiness
# --------------------------------------------------------------------------- #

def _rebalance_readiness(
    completed_marks: Optional[int],
    signal_date: Optional[str],
    latest_mark_date: Optional[str],
) -> dict[str, Any]:
    """Quarterly (63 trading-day) rebalance-review readiness from the completed
    mark count. The signal date is mark #1 (0 return), so trading days elapsed is
    ``completed_marks - 1``. Never rebalances and proposes no orders."""
    completed = completed_marks if isinstance(completed_marks, int) else 0
    elapsed = max(0, completed - 1)
    raw_remaining = REBALANCE_TARGET_TRADING_DAYS - elapsed
    remaining = max(0, raw_remaining)

    if raw_remaining > 42:
        status = READY_EARLY
    elif raw_remaining >= 11:
        status = READY_MID
    elif raw_remaining >= 1:
        status = READY_APPROACHING
    elif raw_remaining == 0:
        status = READY_DUE
    else:
        status = READY_OVERDUE

    return {
        "signal_date": signal_date,
        "latest_mark_date": latest_mark_date,
        "completed_financial_marks": completed,
        "target_holding_period_trading_days": REBALANCE_TARGET_TRADING_DAYS,
        "estimated_trading_days_elapsed": elapsed,
        "remaining_trading_days": remaining,
        "readiness_status": status,
        "cadence": "QUARTERLY_63_TRADING_DAYS",
        "note": ("Rebalance REVIEW timing only — this proposes no orders and performs "
                 "no rebalancing; a review is a manual decision."),
    }


# --------------------------------------------------------------------------- #
# Decision resolution
# --------------------------------------------------------------------------- #

def _resolve_decision(
    *,
    observation_count: Optional[int],
    any_risk: bool,
    any_deterioration: bool,
    role_status: str,
    top25: dict[str, Any],
    top50: dict[str, Any],
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    n = observation_count if isinstance(observation_count, int) else 0
    if n < MIN_FORWARD_OBS:
        reasons.append("Only %d completed forward marks (< %d required to name a "
                       "provisional primary book)." % (n, MIN_FORWARD_OBS))
        return DEC_INSUFFICIENT, reasons
    if any_risk:
        reasons.append("A paper-review risk threshold was breached — manual review "
                       "required before any book is treated as primary.")
        return DEC_RISK, reasons
    if any_deterioration:
        reasons.append("Recent rolling momentum is deteriorating — flagged for manual "
                       "review; no trading action is taken.")
        return DEC_DETERIORATING, reasons
    if role_status == ROLE_TOP50:
        reasons.append("Top50 satisfies all five evidence criteria (no insufficient "
                       "coverage, positive excess, shallower drawdown, lower volatility, "
                       "lower concentration than Top25).")
        return DEC_TOP50, reasons
    if role_status == ROLE_TOP25:
        reasons.append("Top25 satisfies all five evidence criteria versus Top50.")
        return DEC_TOP25, reasons
    exc25 = top25.get("current_excess_return_pct_points")
    exc50 = top50.get("current_excess_return_pct_points")
    if (isinstance(exc25, (int, float)) and exc25 > 0
            and isinstance(exc50, (int, float)) and exc50 > 0):
        reasons.append("Both books show positive excess vs SPY but neither dominates on "
                       "all evidence criteria — no clear provisional primary.")
        return DEC_NO_CLEAR, reasons
    reasons.append("No risk or deterioration flags; evidence does not yet favour a "
                   "provisional primary book — continue paper monitoring.")
    return DEC_CONTINUE, reasons


# --------------------------------------------------------------------------- #
# Public service — GET decision-gate (read-only)
# --------------------------------------------------------------------------- #

def load_current_alpha_decision_gate(
    *,
    backfill_dir: Optional[Union[str, Path]] = None,
    mark_dir: Optional[Union[str, Path]] = None,
    today: Optional[str] = None,
) -> dict[str, Any]:
    """Read-only Phase 13-J paper book decision gate.

    Reads ONLY the Phase 13-I backfill artifacts (through the performance loader,
    plus the per-date history files) and returns the operating decision, the
    provisional primary / challenger books, per-book scorecards, paper-review risk
    triggers, and the quarterly rebalance-review readiness. Returns a controlled
    status in every case (never raises); ``today`` is injectable for deterministic
    mark-freshness tests (defaults to the current UTC date).
    """
    loaded_at = _iso_now()
    perf = load_current_alpha_performance(backfill_dir=backfill_dir, mark_dir=mark_dir)
    perf_status = perf.get("status")

    base = {
        "phase": PHASE,
        "loaded_at": loaded_at,
        "backfill_decision": perf.get("backfill_decision"),
        "reconciliation_status": perf.get("reconciliation_status"),
        "provenance": perf.get("provenance"),
    }

    # --- no artifact / not published -> controlled, no decision ---------------
    if perf_status != "PERFORMANCE_READY":
        gate_status = GATE_NO_BACKFILL if perf_status == "NO_BACKFILL_YET" else GATE_NOT_PUBLISHED
        guidance = perf.get("guidance") or (
            "The Phase 13-I reconstruction is not published (%s); no paper-book "
            "decision can be made until it reconciles." % (perf.get("backfill_decision") or perf_status))
        payload = {
            **base,
            "status": gate_status,
            "decision": DEC_INSUFFICIENT,
            "decision_label": "Insufficient forward history",
            "decision_reasons": [guidance],
            "book_role_status": ROLE_NONE,
            "primary_paper_book": {"status": ROLE_NONE, "book": None, "book_id": None},
            "challenger_paper_book": None,
            "quarterly_rebalance_readiness": _rebalance_readiness(
                perf.get("observation_count"), (perf.get("provenance") or {}).get("signal_date"),
                perf.get("latest_mark_date")),
            "observation_count": perf.get("observation_count"),
            "warnings": perf.get("warnings") or [guidance],
        }
        payload.update(_safety_block())
        return payload

    # --- decision-ready: read the per-date history for coverage + rolling -----
    bdir = _resolve_backfill_dir(backfill_dir, mark_dir)
    top25_hist, _e1 = _read_json_file(bdir / _TOP25_HISTORY)
    top50_hist, _e2 = _read_json_file(bdir / _TOP50_HISTORY)
    rows25 = (top25_hist or {}).get("rows") or []
    rows50 = (top50_hist or {}).get("rows") or []

    top25 = _book_scorecard("TOP25", perf.get("top25_analytics"), rows25)
    top50 = _book_scorecard("TOP50", perf.get("top50_analytics"), rows50)

    signal_date = (perf.get("provenance") or {}).get("signal_date")
    latest_mark_date = perf.get("latest_mark_date")
    observation_count = perf.get("observation_count")

    # Mark freshness relative to today (injectable for tests).
    ref_today = today or _today_iso()
    mark_age = _calendar_age_days(ref_today, latest_mark_date)
    _age, freshness_status = _mark_freshness(latest_mark_date, ref_today)
    mark_freshness = {
        "latest_mark_date": latest_mark_date,
        "as_of_today": ref_today,
        "mark_age_calendar_days": mark_age,
        "mark_freshness_status": freshness_status,
        "stale_threshold_calendar_days": RISK_STALE_MARK_DAYS,
    }

    # Risk + deterioration (stale-mark trigger is book-agnostic; applied to both).
    top25["risk_triggers"] = _risk_triggers(top25, stale_mark_days=mark_age)
    top50["risk_triggers"] = _risk_triggers(top50, stale_mark_days=mark_age)
    top25["deterioration_reasons"] = _deterioration_reasons(top25)
    top50["deterioration_reasons"] = _deterioration_reasons(top50)
    any_risk = bool(top25["risk_triggers"] or top50["risk_triggers"])
    any_deterioration = bool(top25["deterioration_reasons"] or top50["deterioration_reasons"])

    role = _book_role(top25, top50)
    role_status = role["status"]

    decision, decision_reasons = _resolve_decision(
        observation_count=observation_count, any_risk=any_risk,
        any_deterioration=any_deterioration, role_status=role_status,
        top25=top25, top50=top50,
    )

    # Primary / challenger book identities (never production / live).
    if role_status == ROLE_TOP50:
        primary = {"status": "PROVISIONAL_PRIMARY", "book": "TOP50",
                   "book_id": top50.get("book_id"), "reasons": role["criteria"],
                   "qualifier": "PROVISIONAL — evidence-driven, paper-only, not production, not approved, not live"}
        challenger = _book_ref(top25, role="CHALLENGER")
    elif role_status == ROLE_TOP25:
        primary = {"status": "PROVISIONAL_PRIMARY", "book": "TOP25",
                   "book_id": top25.get("book_id"), "reasons": role["criteria"],
                   "qualifier": "PROVISIONAL — evidence-driven, paper-only, not production, not approved, not live"}
        challenger = _book_ref(top50, role="CHALLENGER")
    else:
        primary = {"status": ROLE_NONE, "book": None, "book_id": None,
                   "reasons": role, "qualifier": "No book qualifies as provisional primary yet."}
        challenger = None

    readiness = _rebalance_readiness(observation_count, signal_date, latest_mark_date)

    warnings: list[str] = list(perf.get("warnings") or [])
    if freshness_status in ("STALE_MARK_WARNING", "STALE_MARK_REJECT"):
        warnings.append("Latest financial mark is %s calendar days old — refresh the "
                        "daily mark before relying on this decision." % mark_age)
    if any_risk:
        warnings.append("Paper-review risk threshold breached — see per-book risk triggers.")

    payload = {
        **base,
        "status": GATE_READY,
        "decision": decision,
        "decision_label": decision.replace("_", " ").title(),
        "decision_reasons": decision_reasons,
        "book_role_status": role_status,
        "book_role_detail": role,
        "primary_paper_book": primary,
        "challenger_paper_book": challenger,
        "risk_review": {
            "any_breach": any_risk,
            "top25_triggers": top25["risk_triggers"],
            "top50_triggers": top50["risk_triggers"],
            "mark_freshness": mark_freshness,
            "thresholds": {
                "min_coverage_pct": RISK_MIN_COVERAGE_PCT,
                "drawdown_pct": RISK_DRAWDOWN_PCT,
                "excess_pct_points": RISK_EXCESS_PP,
                "concentration_pct": RISK_CONCENTRATION_PCT,
                "stale_mark_calendar_days": RISK_STALE_MARK_DAYS,
            },
        },
        "deterioration_review": {
            "any": any_deterioration,
            "top25_reasons": top25["deterioration_reasons"],
            "top50_reasons": top50["deterioration_reasons"],
            "window_marks": DETERIORATION_WINDOW,
        },
        "quarterly_rebalance_readiness": readiness,
        "top25": top25,
        "top50": top50,
        "latest_mark_date": latest_mark_date,
        "mark_freshness": mark_freshness,
        "observation_count": observation_count,
        "signal_date": signal_date,
        "stability_comparison": perf.get("stability_comparison"),
        "not_alpha_validation": perf.get("not_alpha_validation"),
        "warnings": warnings,
    }
    payload.update(_safety_block())
    return payload


__all__ = [
    "load_current_alpha_decision_gate",
    "DECISION_GATE_SAFETY_BADGES",
    "MIN_FORWARD_OBS",
    "REBALANCE_TARGET_TRADING_DAYS",
]
