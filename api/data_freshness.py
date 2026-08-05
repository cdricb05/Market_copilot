"""api/data_freshness.py — Slice 1 canonical cross-source data-freshness contract.

READ-ONLY composition service (Consolidation Roadmap Slice 1; TARGET_ARCHITECTURE
context "Market Session and Data Freshness"). It answers ONE question with one
vocabulary: for the canonical market session, which inputs are current, stale,
missing, future-dated, inconsistent, or not yet due under their declared cadence,
and which stale conditions block which operation.

It **composes** the canonical market-session domain (``engine.market_session``)
and the existing authoritative read loaders; it does not re-derive their
calculations and it owns no persistent state. It performs NO provider network
call, NO prediction call, NO Daily Close, and NO write of any kind (file, ledger,
database, snapshot, order, signal, decision, cache).

The service deliberately keeps two readinesses SEPARATE (Architecture Decision
D-7): research/model staleness may block a NEW signal refresh or TRUE_FORWARD
capture, but never invalidates an already-completed operational close.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Optional

from paper_trader.engine import market_session as msession

PHASE = "29B-Slice1"

# --------------------------------------------------------------------------- #
# Frozen freshness vocabulary (part of the tested contract).
# --------------------------------------------------------------------------- #
FRESH = "FRESH"
STALE = "STALE"
MISSING = "MISSING"
FUTURE_DATED = "FUTURE_DATED"
INCONSISTENT = "INCONSISTENT"
NOT_DUE = "NOT_DUE"
NOT_APPLICABLE = "NOT_APPLICABLE"
UNKNOWN = "UNKNOWN"

FRESHNESS_VOCAB = (FRESH, STALE, MISSING, FUTURE_DATED, INCONSISTENT,
                   NOT_DUE, NOT_APPLICABLE, UNKNOWN)
# Statuses that satisfy a required-input gate (UNKNOWN is NOT fresh).
_SATISFIED = frozenset({FRESH, NOT_DUE, NOT_APPLICABLE})

# Frozen cadence vocabulary.
DAILY = "DAILY"
MONTHLY = "MONTHLY"
QUARTERLY = "QUARTERLY"
EVENT_DRIVEN = "EVENT_DRIVEN"
STATIC = "STATIC"

SAFETY_BADGES = ["READ ONLY", "NO PROVIDER CALL", "NO ORDERS", "AUTOMATION OFF",
                 "MANUAL REVIEW"]

# Deterministic clock seam (tests / explicit callers).
NOW_ENV = "PAPER_TRADER_DATA_FRESHNESS_NOW"
_now_override: Optional[datetime] = None


def _now() -> datetime:
    if _now_override is not None:
        return _now_override
    raw = os.environ.get(NOW_ENV)
    if raw:
        try:
            parsed = datetime.fromisoformat(raw)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return datetime.now(tz=timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


def _coerce_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _iso(d: Optional[date]) -> Optional[str]:
    return d.isoformat() if d else None


def _business_days_between(a: date, b: date) -> int:
    """Count weekdays in (a, b] when b > a (weekday-only, no holidays)."""
    if a is None or b is None or b <= a:
        return 0
    n = 0
    cur = a
    while cur < b:
        cur += timedelta(days=1)
        if cur.weekday() < 5:
            n += 1
    return n


def _month_index(d: date) -> int:
    return d.year * 12 + (d.month - 1)


def _quarter_index(d: date) -> int:
    return d.year * 4 + (d.month - 1) // 3


# --------------------------------------------------------------------------- #
# Pure freshness classifier (cadence-aware). Fully deterministic over injected
# dates — this is the tested core of the month-boundary behaviour.
# --------------------------------------------------------------------------- #
def classify_source(*, cadence: str, as_of: Any, anchor: Any) -> dict[str, Any]:
    """Classify one source's freshness against the canonical anchor date.

    ``anchor`` is the canonical eligible market date (falls back to the expected
    completed session when nothing is confirmed). Returns status, expected-through
    date, lag, and a human reason. A slower-cadence input is judged under its OWN
    cadence: a monthly input is not "stale" merely because it is older than a
    daily price date — only when a new monthly period is due.
    """
    a = _coerce_date(as_of)
    anc = _coerce_date(anchor)

    if cadence == STATIC:
        if a is None:
            return {"status": NOT_APPLICABLE, "expected_through_date": None,
                    "lag_sessions": None, "lag_calendar_days": None,
                    "reason": "Static input; no time-based freshness."}
        return {"status": FRESH, "expected_through_date": _iso(a),
                "lag_sessions": 0, "lag_calendar_days": 0,
                "reason": "Static input present."}

    if anc is None:
        return {"status": UNKNOWN, "expected_through_date": None,
                "lag_sessions": None, "lag_calendar_days": None,
                "reason": "No canonical anchor date available to judge freshness."}

    if a is None:
        return {"status": MISSING, "expected_through_date": _iso(anc),
                "lag_sessions": None, "lag_calendar_days": None,
                "reason": "Source date is absent."}

    lag_days = (anc - a).days
    lag_sessions = _business_days_between(a, anc)

    if cadence in (DAILY, EVENT_DRIVEN):
        through = _iso(anc)
        if a > anc:
            return {"status": FUTURE_DATED, "expected_through_date": through,
                    "lag_sessions": 0, "lag_calendar_days": (a - anc).days,
                    "reason": "Source date %s is newer than the eligible session %s."
                              % (a.isoformat(), anc.isoformat())}
        if a == anc:
            return {"status": FRESH, "expected_through_date": through,
                    "lag_sessions": 0, "lag_calendar_days": 0,
                    "reason": "Current through the eligible session %s." % anc.isoformat()}
        return {"status": STALE, "expected_through_date": through,
                "lag_sessions": lag_sessions, "lag_calendar_days": lag_days,
                "reason": "Behind the eligible session %s by %d session(s)."
                          % (anc.isoformat(), lag_sessions)}

    if cadence == MONTHLY:
        through = date(anc.year, anc.month, 1).isoformat()
        am, em = _month_index(a), _month_index(anc)
        if am > em:
            return {"status": FUTURE_DATED, "expected_through_date": through,
                    "lag_sessions": None, "lag_calendar_days": (a - anc).days,
                    "reason": "Monthly input month is ahead of the eligible month."}
        if am == em:
            return {"status": FRESH, "expected_through_date": through,
                    "lag_sessions": None, "lag_calendar_days": lag_days,
                    "reason": "Current for the eligible month %s." % anc.strftime("%Y-%m")}
        return {"status": STALE, "expected_through_date": through,
                "lag_sessions": None, "lag_calendar_days": lag_days,
                "reason": "A new monthly refresh is due: input month %s is behind the "
                          "eligible month %s." % (a.strftime("%Y-%m"), anc.strftime("%Y-%m"))}

    if cadence == QUARTERLY:
        through = _iso(anc)
        aq, eq = _quarter_index(a), _quarter_index(anc)
        if aq > eq:
            return {"status": FUTURE_DATED, "expected_through_date": through,
                    "lag_sessions": None, "lag_calendar_days": (a - anc).days,
                    "reason": "Quarterly input is ahead of the eligible quarter."}
        if aq == eq:
            return {"status": FRESH, "expected_through_date": through,
                    "lag_sessions": None, "lag_calendar_days": lag_days,
                    "reason": "Current for the eligible quarter."}
        if aq == eq - 1:
            return {"status": NOT_DUE, "expected_through_date": through,
                    "lag_sessions": None, "lag_calendar_days": lag_days,
                    "reason": "Prior-quarter input; the current quarter's data is not "
                              "yet due under the reporting cadence."}
        return {"status": STALE, "expected_through_date": through,
                "lag_sessions": None, "lag_calendar_days": lag_days,
                "reason": "Quarterly input is more than one quarter behind."}

    return {"status": UNKNOWN, "expected_through_date": _iso(anc),
            "lag_sessions": None, "lag_calendar_days": None,
            "reason": "Unrecognised cadence."}


# --------------------------------------------------------------------------- #
# Source registry — declarative. Each entry names its cadence, purpose, owner,
# and which operations it is required for. ``date_key`` selects the as-of date
# from the resolved date map.
# --------------------------------------------------------------------------- #
_SOURCES: tuple[dict[str, Any], ...] = (
    {"source_id": "owned_daily_prices", "display_name": "Owned daily market prices",
     "business_purpose": "Point-in-time owned EOD prices for the holdings universe.",
     "cadence": DAILY, "date_key": "owned_price_date",
     "authoritative_owner": "api.portfolio_valuation (owned EOD marks)",
     "provenance": "portfolio_valuation.current_mark.as_of_market_date",
     "req": ("signal", "reassess", "true_forward", "close")},
    {"source_id": "desk_marks", "display_name": "Desk marks",
     "business_purpose": "Owned-EODHD marks for the operational paper book holdings.",
     "cadence": DAILY, "date_key": "desk_mark_date",
     "authoritative_owner": "api.paper_trading_desk",
     "provenance": "operational current mark (desk mark cache)",
     "req": ("reassess", "true_forward", "close")},
    {"source_id": "benchmark", "display_name": "Benchmark (SPY)",
     "business_purpose": "Owned SPY closes for excess-return attribution.",
     "cadence": DAILY, "date_key": "benchmark_date",
     "authoritative_owner": "api.paper_trading_desk (SPY)",
     "provenance": "current mark spy.mark_date",
     "req": ("true_forward", "close")},
    {"source_id": "research_model_mark", "display_name": "Research / model market date",
     "business_purpose": "Latest completed daily model mark (Top25/Top50/SPY).",
     "cadence": DAILY, "date_key": "research_mark_date",
     "authoritative_owner": "api.current_alpha_daily_refresh",
     "provenance": "current_alpha_daily_status.latest_valid_mark_date",
     "req": ("signal", "reassess", "true_forward")},
    {"source_id": "target_calculation", "display_name": "Target-calculation date",
     "business_purpose": "Date the current portfolio target was computed for.",
     "cadence": DAILY, "date_key": "target_calc_date",
     "authoritative_owner": "api.alpha_target",
     "provenance": "operating run required_market_date",
     "req": ("reassess",)},
    {"source_id": "momentum_monthly", "display_name": "Momentum / risk input (monthly)",
     "business_purpose": "Frozen monthly-momentum model input for scoring.",
     "cadence": MONTHLY, "date_key": "monthly_input_date",
     "authoritative_owner": "engine.multi_horizon_engine (frozen monthly momentum)",
     "provenance": "proxied from the model refresh month (monthly input refreshes "
                   "with the model; not separately persisted in Slice 1)",
     "req": ("signal", "reassess", "true_forward")},
    {"source_id": "fundamental_quarterly", "display_name": "Fundamental data (quarterly)",
     "business_purpose": "Quarterly fundamentals under their reporting/availability cadence.",
     "cadence": QUARTERLY, "date_key": "fundamental_date",
     "authoritative_owner": "feature_service (deferred — Slice 9)",
     "provenance": "not wired into the operational path in Slice 1 (informational)",
     "req": ()},
    {"source_id": "operational_valuation", "display_name": "Operational valuation date",
     "business_purpose": "Date the operational NAV / holdings valuation is marked to.",
     "cadence": DAILY, "date_key": "valuation_date",
     "authoritative_owner": "api.portfolio_valuation",
     "provenance": "portfolio_valuation.current_mark.as_of_market_date",
     "req": ("reassess", "close")},
    {"source_id": "latest_daily_close", "display_name": "Latest Daily Close date",
     "business_purpose": "Most recent completed operational Daily Close (state indicator).",
     "cadence": EVENT_DRIVEN, "date_key": "daily_close_date",
     "authoritative_owner": "api.daily_close",
     "provenance": "daily_close.last_processed_market_date",
     "req": ()},
    {"source_id": "latest_true_forward", "display_name": "Latest TRUE_FORWARD snapshot date",
     "business_purpose": "Most recent immutable forward-evidence snapshot (output).",
     "cadence": EVENT_DRIVEN, "date_key": "true_forward_date",
     "authoritative_owner": "api.forward_prediction_skill",
     "provenance": "prediction_skill.latest_snapshot_date",
     "req": ()},
    {"source_id": "prediction_research_mark", "display_name": "Latest valid research mark",
     "business_purpose": "Latest valid research/prediction mark where applicable.",
     "cadence": DAILY, "date_key": "prediction_mark_date",
     "authoritative_owner": "api.current_alpha_daily_refresh",
     "provenance": "current_alpha_daily_status.latest_valid_mark_date",
     "req": ()},
)

_REQ_FIELD = {
    "signal": "required_for_signal_refresh",
    "reassess": "required_for_portfolio_reassessment",
    "true_forward": "required_for_true_forward_capture",
    "close": "required_for_operational_close",
}


# --------------------------------------------------------------------------- #
# Date extraction from existing loaders (degrade-safe; never raises).
# --------------------------------------------------------------------------- #
def _safe(loader: Callable, warnings: list, label: str) -> Any:
    try:
        return loader()
    except Exception as exc:  # noqa: BLE001
        warnings.append("%s unavailable: %s" % (label, str(exc)[:160]))
        return None


def _extract_dates(*, state: Optional[dict], daily_close_status: Optional[dict],
                   forward_status: Optional[dict], overrides: dict,
                   warnings: list) -> dict[str, Optional[str]]:
    """Resolve every source's as-of date from the (already-loaded) read models.

    Every value can be overridden explicitly (tests). Missing pieces degrade to
    None (→ MISSING/UNKNOWN), never to a fabricated date.
    """
    cur = ((state or {}).get("current_operating_mark") or {})
    pf = (cur.get("portfolio") or {})
    spy = (cur.get("spy") or {})

    owned_price = pf.get("as_of_market_date")
    valuation = pf.get("as_of_market_date")
    benchmark = spy.get("mark_date")
    research_mark = None
    # The current operating mark exposes the model mark via top25/top50 books.
    for bk in ("top25", "top50"):
        b = cur.get(bk) or {}
        research_mark = research_mark or b.get("mark_date") or b.get("market_date")
    target_calc = cur.get("latest_completed_market_date")
    desk_mark = owned_price  # operational valuation is marked from the desk marks

    close_date = None
    if daily_close_status is not None:
        # load_close_progress → market_date (last processed / in-flight close date).
        # Fallbacks accept an injected status dict shape in tests.
        close_date = (daily_close_status.get("market_date")
                      or daily_close_status.get("last_processed_market_date")
                      or daily_close_status.get("latest_eligible_market_date"))
    tf_date = None
    if forward_status is not None:
        tf_date = forward_status.get("latest_snapshot_date")

    # Monthly momentum input date is refreshed with the model — proxy from the
    # model mark's month unless explicitly supplied.
    monthly_input = research_mark or target_calc

    resolved = {
        "owned_price_date": owned_price,
        "desk_mark_date": desk_mark,
        "benchmark_date": benchmark,
        "research_mark_date": research_mark,
        "target_calc_date": target_calc,
        "monthly_input_date": monthly_input,
        "fundamental_date": None,
        "valuation_date": valuation,
        "daily_close_date": close_date,
        "true_forward_date": tf_date,
        "prediction_mark_date": research_mark,
    }
    for k, v in (overrides or {}).items():
        if k in resolved:
            resolved[k] = v
    return {k: (msession._coerce_date(v).isoformat() if msession._coerce_date(v) else None)
            for k, v in resolved.items()}


# --------------------------------------------------------------------------- #
# Public entry point.
# --------------------------------------------------------------------------- #
def load_data_freshness(
    *,
    now: Optional[datetime] = None,
    reference_today: Any = None,
    close_cutoff_et: Any = msession.DEFAULT_CLOSE_CUTOFF_ET,
    state: Optional[dict] = None,
    daily_close_status: Optional[dict] = None,
    forward_status: Optional[dict] = None,
    date_overrides: Optional[dict] = None,
) -> dict[str, Any]:
    """Return the canonical cross-source data-freshness contract (read-only).

    Clock: pass ``now`` (datetime) or ``reference_today`` (offline date), else the
    real ET clock is resolved. All read models can be injected for deterministic
    tests; otherwise the existing authoritative loaders are called degrade-safely.
    """
    warnings: list[str] = []

    if state is None:
        def _load_state():
            from paper_trader.api.current_operating_state import load_current_operating_state
            return load_current_operating_state()
        state = _safe(_load_state, warnings, "Current operating state") or {}
    if daily_close_status is None:
        def _load_close():
            # PROBE-FREE: load_close_progress reads the close journal/progress file
            # only. (load_daily_close() would run the owned-EOD provider probe when
            # the book is active and the session is unprocessed — forbidden here.)
            from paper_trader.api import daily_close as dc
            return dc.load_close_progress()
        daily_close_status = _safe(_load_close, warnings, "Daily Close progress")
    if forward_status is None:
        def _load_fwd():
            from paper_trader.api import forward_prediction_skill as fps
            return fps.load_prediction_skill()
        forward_status = _safe(_load_fwd, warnings, "Forward-prediction skill status")

    dates = _extract_dates(state=state, daily_close_status=daily_close_status,
                           forward_status=forward_status,
                           overrides=(date_overrides or {}), warnings=warnings)

    # Owned-data confirmation for the market session: the operational owned mark
    # (holdings) confirms the session; the SPY mark is the independent benchmark
    # series used only for the holiday cross-check.
    confirmed = dates.get("desk_mark_date") or dates.get("owned_price_date")
    benchmark = dates.get("benchmark_date")

    if now is None and reference_today is None:
        now = _now()
    session = msession.evaluate_session(
        now=now, reference_today=reference_today,
        latest_confirmed_owned_data_date=confirmed,
        latest_benchmark_date=benchmark,
        close_cutoff_et=close_cutoff_et,
        require_confirmation=True,
    )
    warnings.extend(session.warnings)

    anchor = (session.eligible_market_date
              or session.expected_completed_market_date)

    rows: list[dict[str, Any]] = []
    for spec in _SOURCES:
        as_of = dates.get(spec["date_key"])
        cls = classify_source(cadence=spec["cadence"], as_of=as_of, anchor=anchor)
        req = {field: (tag in spec["req"]) for tag, field in _REQ_FIELD.items()}
        blocks = (cls["status"] not in _SATISFIED) and any(req.values())
        action = ""
        if cls["status"] not in _SATISFIED:
            if cls["status"] == MISSING:
                action = "Restore %s (source date is absent)." % spec["display_name"]
            elif cls["status"] == STALE:
                action = "Refresh %s to the eligible session/period." % spec["display_name"]
            elif cls["status"] == FUTURE_DATED:
                action = "Investigate %s — it is dated ahead of the eligible session." % spec["display_name"]
            elif cls["status"] == UNKNOWN:
                action = "Determine the as-of date for %s (currently unknown)." % spec["display_name"]
            elif cls["status"] == INCONSISTENT:
                action = "Reconcile %s — it is inconsistent with the market session." % spec["display_name"]
        rows.append({
            "source_id": spec["source_id"],
            "display_name": spec["display_name"],
            "business_purpose": spec["business_purpose"],
            "cadence": spec["cadence"],
            "as_of_date": as_of,
            "expected_through_date": cls["expected_through_date"],
            "status": cls["status"],
            "lag_sessions": cls["lag_sessions"],
            "lag_calendar_days": cls["lag_calendar_days"],
            "required_for_signal_refresh": req["required_for_signal_refresh"],
            "required_for_portfolio_reassessment": req["required_for_portfolio_reassessment"],
            "required_for_true_forward_capture": req["required_for_true_forward_capture"],
            "required_for_operational_close": req["required_for_operational_close"],
            "blocks_current_operation": bool(blocks),
            "reason": cls["reason"],
            "operator_action": action,
            "authoritative_owner": spec["authoritative_owner"],
            "provenance": spec["provenance"],
        })

    by_id = {r["source_id"]: r for r in rows}

    def _sources_for(tag_field: str) -> list[dict]:
        return [r for r in rows if r[tag_field]]

    def _all_satisfied(tag_field: str) -> bool:
        return all(r["status"] in _SATISFIED for r in _sources_for(tag_field))

    session_signal_ready = session.ready_for_daily_signal_refresh
    session_close_ready = session.ready_for_operational_close
    session_tf_ready = session.ready_for_true_forward_capture

    signal_refresh_ready = bool(session_signal_ready and _all_satisfied("required_for_signal_refresh"))
    portfolio_reassessment_ready = bool(session_signal_ready and _all_satisfied("required_for_portfolio_reassessment"))
    true_forward_capture_ready = bool(session_tf_ready and _all_satisfied("required_for_true_forward_capture"))
    operational_close_ready = bool(session_close_ready and _all_satisfied("required_for_operational_close"))

    all_daily_inputs_fresh = all(r["status"] == FRESH for r in rows if r["cadence"] == DAILY)
    slower_inputs_due = [r["source_id"] for r in rows
                         if r["cadence"] in (MONTHLY, QUARTERLY) and r["status"] == STALE]

    # Weakest gate + required actions. Session first (if it blocks), then every
    # blocking required source (all reported — none hidden).
    required_actions: list[dict[str, Any]] = []
    if session.weakest_gate != msession.GATE_NONE:
        required_actions.append({
            "gate": "market_session", "source_id": None,
            "status": session.session_status, "action": session.operator_action})
    for r in rows:
        if r["blocks_current_operation"]:
            required_actions.append({
                "gate": "source", "source_id": r["source_id"],
                "status": r["status"], "action": r["operator_action"]})

    if session.weakest_gate != msession.GATE_NONE:
        weakest_gate = "MARKET_SESSION:%s" % session.weakest_gate
    elif required_actions:
        weakest_gate = "SOURCE:%s" % required_actions[0]["source_id"]
    else:
        weakest_gate = "NONE"

    # D-7: research staleness never invalidates a completed operational close.
    if by_id["latest_daily_close"]["as_of_date"] and not signal_refresh_ready:
        warnings.append(
            "Research/signal readiness is separate from operational-close validity: "
            "the completed close at %s remains valid."
            % by_id["latest_daily_close"]["as_of_date"])

    # De-duplicate warnings preserving order.
    seen: set[str] = set()
    uniq_warnings = [w for w in warnings if not (w in seen or seen.add(w))]

    return {
        "status": "OK",
        "phase": PHASE,
        "evaluated_at": _now_iso(),
        "market_session": session.as_dict(),
        "expected_completed_market_date": session.expected_completed_market_date,
        "eligible_market_date": session.eligible_market_date,
        "source_freshness": rows,
        "all_daily_inputs_fresh": bool(all_daily_inputs_fresh),
        "slower_inputs_due": slower_inputs_due,
        "signal_refresh_ready": signal_refresh_ready,
        "portfolio_reassessment_ready": portfolio_reassessment_ready,
        "true_forward_capture_ready": true_forward_capture_ready,
        "operational_close_ready": operational_close_ready,
        "weakest_gate": weakest_gate,
        "required_actions": required_actions,
        "warnings": uniq_warnings,
        "safety": {
            "read_only": True,
            "wrote_to_database": False,
            "wrote_to_ledger": False,
            "called_provider": False,
            "called_prediction": False,
            "ran_daily_close": False,
            "created_orders": False,
            "created_signals": False,
            "created_trade_decisions": False,
            "created_fills": False,
            "paper_only": True,
            "automation_off": True,
            "manual_review": True,
            "safety_badges": list(SAFETY_BADGES),
        },
        "provenance": {
            "phase": PHASE,
            "market_session_owner": "engine.market_session",
            "freshness_owner": "api.data_freshness",
            "composed_read_models": [
                "current_operating_state.load_current_operating_state",
                "daily_close.load_close_progress",
                "forward_prediction_skill.load_prediction_skill",
            ],
            "note": "Read-only composition; no calculation duplicated, no write performed.",
        },
    }


__all__ = [
    "PHASE",
    "FRESH", "STALE", "MISSING", "FUTURE_DATED", "INCONSISTENT",
    "NOT_DUE", "NOT_APPLICABLE", "UNKNOWN", "FRESHNESS_VOCAB",
    "DAILY", "MONTHLY", "QUARTERLY", "EVENT_DRIVEN", "STATIC",
    "classify_source",
    "load_data_freshness",
]
