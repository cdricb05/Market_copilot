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

Phase 29B.1 corrective patch — ACTIVE OPERATIONAL BOOK ALIGNMENT
----------------------------------------------------------------
Every OPERATIONAL date concept is now owned by the ACTIVE operational book
(``api.operational_book`` — the authoritative book-selection policy) and its
owned desk marks (``api.paper_trading_desk``), NOT by the dormant legacy /
current-alpha research book (``api.portfolio_valuation`` /
``api.current_operating_state``, whose stale mark previously leaked in as the
owned-data confirmation). Distinct RESEARCH dates — the champion research
evaluation mark, the latest daily price/score refresh, the frozen monthly
momentum input, the fundamental panel and the latest TRUE_FORWARD snapshot — are
resolved from their OWN owners and are never collapsed into one "research date".
The frozen monthly momentum input is read directly from its persisted source
(``multi_horizon_engine.load_inputs`` → ``month_label``); it is NEVER proxied
from the target date, valuation date, champion mark or expected session. A
deterministic cross-surface consistency validator reports CONSISTENT /
INCONSISTENT / UNKNOWN and names every violation.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Optional

from paper_trader.engine import market_session as msession

PHASE = "29B.1-Slice1"

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

# Each source is either an OPERATIONAL concept (owned by the active operational
# book / desk / close) or a RESEARCH concept (owned by the model input / research
# pipeline). Research staleness is never rendered as an operational-close failure.
KIND_OPERATIONAL = "OPERATIONAL"
KIND_RESEARCH = "RESEARCH"

# Cross-surface consistency vocabulary (part of the tested contract).
CONSISTENT = "CONSISTENT"
CONSISTENCY_VOCAB = (CONSISTENT, INCONSISTENT, UNKNOWN)

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


def _coerce_iso(value: Any) -> Optional[str]:
    """Best-effort coercion of any date-like value to an ISO date string."""
    return _iso(_coerce_date(value))


def _month_label_to_date(value: Any) -> Any:
    """Normalize a persisted ``YYYY-MM`` month label to a real date (first of the
    month) so the MONTHLY classifier can judge it. Any other value passes through
    unchanged (already a date / full ISO string / None)."""
    if isinstance(value, str):
        s = value.strip()
        if len(s) == 7 and s[4] == "-" and s[:4].isdigit() and s[5:].isdigit():
            return s + "-01"
    return value


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
# Source registry — declarative. Each entry names its cadence, kind, purpose,
# owner, and which operations it is required for. ``date_key`` selects the as-of
# date from the resolved date map; ``owned`` marks the session-confirming owned
# operational marks (the "daily inputs" the operator watches for the close).
# --------------------------------------------------------------------------- #
_SOURCES: tuple[dict[str, Any], ...] = (
    {"source_id": "owned_daily_prices", "display_name": "Owned daily market prices",
     "business_purpose": "Point-in-time owned EOD prices for the active operational book holdings.",
     "cadence": DAILY, "kind": KIND_OPERATIONAL, "owned": True, "date_key": "owned_price_date",
     "authoritative_owner": "api.operational_book (active book owned desk marks)",
     "provenance": "operational_book.desk_mark_date (owned EODHD marks for Alpha Paper Book)",
     "req": ("signal", "reassess", "true_forward", "close")},
    {"source_id": "desk_marks", "display_name": "Desk marks",
     "business_purpose": "Owned-EODHD marks for the active operational paper-book holdings.",
     "cadence": DAILY, "kind": KIND_OPERATIONAL, "owned": True, "date_key": "desk_mark_date",
     "authoritative_owner": "api.paper_trading_desk (active operational desk marks)",
     "provenance": "operational_book.desk_mark_date / paper_trading_desk.latest_completed_date",
     "req": ("reassess", "true_forward", "close")},
    {"source_id": "benchmark", "display_name": "Benchmark (SPY)",
     "business_purpose": "Owned SPY closes attached to the active operational mark.",
     "cadence": DAILY, "kind": KIND_OPERATIONAL, "owned": True, "date_key": "benchmark_date",
     "authoritative_owner": "api.paper_trading_desk (SPY attached to the active operational mark)",
     "provenance": "paper_trading_desk desk-mark SPY series (latest close at/through the desk mark date)",
     "req": ("true_forward", "close")},
    {"source_id": "operational_valuation", "display_name": "Operational valuation date",
     "business_purpose": "Date the active operational NAV / holdings valuation is marked to.",
     "cadence": DAILY, "kind": KIND_OPERATIONAL, "owned": True, "date_key": "valuation_date",
     "authoritative_owner": "api.operational_book (ledger-replayed NAV as-of date)",
     "provenance": "operational_book.nav_as_of_date",
     "req": ("reassess", "close")},
    {"source_id": "latest_daily_close", "display_name": "Latest Daily Close date",
     "business_purpose": "Most recent completed operational Daily Close (state indicator).",
     "cadence": EVENT_DRIVEN, "kind": KIND_OPERATIONAL, "owned": False, "date_key": "daily_close_date",
     "authoritative_owner": "api.daily_close (probe-free close progress)",
     "provenance": "daily_close.load_close_progress().market_date",
     "req": ()},
    {"source_id": "target_calculation", "display_name": "Target-calculation date",
     "business_purpose": "Market date the current operational portfolio target was computed for.",
     "cadence": DAILY, "kind": KIND_OPERATIONAL, "owned": False, "date_key": "target_calc_date",
     "authoritative_owner": "api.alpha_target (current target alpha_market_date)",
     "provenance": "operational_book.current_target.alpha_market_date",
     "req": ("reassess",)},
    {"source_id": "price_score_refresh", "display_name": "Latest price / score refresh",
     "business_purpose": "Market date the owned alpha price/score inputs were refreshed to.",
     "cadence": DAILY, "kind": KIND_RESEARCH, "owned": False, "date_key": "price_score_refresh_date",
     "authoritative_owner": "api.multi_horizon_engine (owned model inputs)",
     "provenance": "multi_horizon_engine.load_inputs().market_as_of_date",
     "req": ("signal", "reassess", "true_forward")},
    {"source_id": "momentum_monthly", "display_name": "Frozen monthly momentum input",
     "business_purpose": "Frozen monthly-momentum (mom_6_1) model input month for scoring.",
     "cadence": MONTHLY, "kind": KIND_RESEARCH, "owned": False, "date_key": "monthly_input_date",
     "authoritative_owner": "api.multi_horizon_engine (frozen monthly momentum)",
     "provenance": "multi_horizon_engine.load_inputs().momentum_month (current_momentum_scores.csv "
                   "month_label — persisted directly, never proxied)",
     "req": ("signal", "reassess", "true_forward")},
    {"source_id": "champion_research_mark", "display_name": "Champion research-evaluation mark",
     "business_purpose": "Latest completed champion Top25/Top50 research-evaluation mark (research book).",
     "cadence": DAILY, "kind": KIND_RESEARCH, "owned": False, "date_key": "champion_research_mark_date",
     "authoritative_owner": "api.current_alpha_daily_refresh (research champion daily mark)",
     "provenance": "current_alpha_daily_status.latest_valid_mark_date (research only — NEVER supplies "
                   "operational owned-data confirmation)",
     "req": ()},
    {"source_id": "fundamental_quarterly", "display_name": "Fundamental data (quarterly)",
     "business_purpose": "Frozen fundamental panel under its OWN quarterly reporting cadence.",
     "cadence": QUARTERLY, "kind": KIND_RESEARCH, "owned": False, "date_key": "fundamental_date",
     "authoritative_owner": "api.multi_horizon_engine (frozen fundamental panel as-of)",
     "provenance": "multi_horizon_engine.load_inputs().fundamental_as_of_date",
     "req": ()},
    {"source_id": "latest_true_forward", "display_name": "Latest TRUE_FORWARD snapshot date",
     "business_purpose": "Most recent immutable forward-evidence snapshot (output).",
     "cadence": EVENT_DRIVEN, "kind": KIND_RESEARCH, "owned": False, "date_key": "true_forward_date",
     "authoritative_owner": "api.forward_prediction_skill (immutable forward evidence)",
     "provenance": "prediction_skill.latest_snapshot_date",
     "req": ()},
)

_REQ_FIELD = {
    "signal": "required_for_signal_refresh",
    "reassess": "required_for_portfolio_reassessment",
    "true_forward": "required_for_true_forward_capture",
    "close": "required_for_operational_close",
}

# The research-only source ids (must always carry KIND_RESEARCH — self-checked by
# the consistency validator so a research date can never masquerade as operational).
_RESEARCH_SOURCE_IDS = tuple(s["source_id"] for s in _SOURCES if s["kind"] == KIND_RESEARCH)


# --------------------------------------------------------------------------- #
# Date extraction from the AUTHORITATIVE read models (degrade-safe; never raises).
# --------------------------------------------------------------------------- #
def _safe(loader: Callable, warnings: list, label: str) -> Any:
    try:
        return loader()
    except Exception as exc:  # noqa: BLE001
        warnings.append("%s unavailable: %s" % (label, str(exc)[:160]))
        return None


def _op_book(operational: Optional[dict]) -> dict:
    """Unwrap the active operational-book dict from ``operational_book.load_operational_book``
    (which nests the book under the ``operational_book`` key). A book dict injected
    directly (tests) is returned unchanged."""
    op = operational or {}
    inner = op.get("operational_book")
    return inner if isinstance(inner, dict) else op


def _spy_date_at_or_before(desk_marks: Optional[dict], as_of: Any) -> Optional[str]:
    """Latest owned SPY close date at/through ``as_of`` (the benchmark attached to
    the active operational mark). Pure read of the desk mark store's SPY series."""
    series = ((desk_marks or {}).get("series") or {}).get("SPY") or []
    limit = _coerce_iso(as_of)
    best: Optional[str] = None
    for row in series:
        try:
            d = str(row[0])[:10]
            v = row[1]
        except (IndexError, TypeError):
            continue
        if v is None:
            continue
        if limit is not None and d > limit:
            continue
        if best is None or d > best:
            best = d
    return best


def _extract_dates(*, operational: Optional[dict], inputs: Optional[dict],
                   daily_status: Optional[dict], desk_marks: Optional[dict],
                   daily_close_status: Optional[dict], forward_status: Optional[dict],
                   overrides: dict) -> dict[str, Optional[str]]:
    """Resolve every source's as-of date from its OWN authoritative read model.

    Operational dates come from the ACTIVE operational book (``api.operational_book``)
    and its owned desk marks (``api.paper_trading_desk``). Research dates come from
    their own owners. Every value can be overridden explicitly (tests). Missing
    pieces degrade to None (→ MISSING/UNKNOWN), never to a fabricated or proxied date.
    """
    op = _op_book(operational)
    ct = op.get("current_target") or {}
    inp = inputs or {}

    # --- OPERATIONAL: active operational book + owned desk marks ------------ #
    desk_mark = op.get("desk_mark_date") or op.get("latest_desk_mark_date")
    # The owned daily prices ARE the owned-EODHD desk marks that confirm the session.
    owned_price = desk_mark
    valuation = op.get("nav_as_of_date")
    benchmark = _spy_date_at_or_before(desk_marks, desk_mark)
    target_calc = ct.get("alpha_market_date")

    close_date = None
    if daily_close_status is not None:
        # load_close_progress → market_date (last processed / in-flight close date).
        # Fallbacks accept an injected status-dict shape in tests.
        close_date = (daily_close_status.get("market_date")
                      or daily_close_status.get("last_processed_market_date")
                      or daily_close_status.get("latest_eligible_market_date"))

    # --- RESEARCH: model inputs / research pipeline ------------------------ #
    price_score = inp.get("market_as_of_date")
    # Frozen monthly momentum input — DIRECTLY persisted (month_label). Never proxied.
    monthly_input = _month_label_to_date(inp.get("momentum_month"))
    fundamental = inp.get("fundamental_as_of_date")
    champion_mark = (daily_status or {}).get("latest_valid_mark_date")
    tf_date = None
    if forward_status is not None:
        tf_date = forward_status.get("latest_snapshot_date")

    resolved = {
        "owned_price_date": owned_price,
        "desk_mark_date": desk_mark,
        "benchmark_date": benchmark,
        "valuation_date": valuation,
        "daily_close_date": close_date,
        "target_calc_date": target_calc,
        "price_score_refresh_date": price_score,
        "monthly_input_date": monthly_input,
        "fundamental_date": fundamental,
        "champion_research_mark_date": champion_mark,
        "true_forward_date": tf_date,
    }
    for k, v in (overrides or {}).items():
        if k in resolved:
            resolved[k] = v
    return {k: _coerce_iso(v) for k, v in resolved.items()}


# --------------------------------------------------------------------------- #
# Active-book identity (Workstream C). Resolve the ACTIVE operational book through
# the existing authoritative book-selection policy (api.operational_book). A
# dormant / legacy research book can never override it, and multiple candidates
# degrade to INCONSISTENT rather than silently selecting one.
# --------------------------------------------------------------------------- #
def _expected_operational_book_id() -> Optional[str]:
    try:
        from paper_trader.api import operational_book as ob
        return getattr(ob, "OPERATIONAL_BOOK_ID", None)
    except Exception:  # noqa: BLE001
        return None


def _resolve_active_book(operational: Optional[dict],
                         override: Any) -> dict[str, Any]:
    owner = "api.operational_book"
    if override is not None:
        cands = override if isinstance(override, list) else [override]
        cands = [c for c in cands if c]
        if len(cands) > 1:
            return {
                "active_book_id": None, "active_book_name": None,
                "active_book_status": INCONSISTENT,
                "active_book_authoritative_owner": owner,
                "operational_mark_date": None, "ambiguous": True,
                "candidate_book_ids": sorted(str(_op_book(c).get("book_id"))
                                             for c in cands),
                "note": "Multiple active-book candidates; refusing to select one silently.",
            }
        op = _op_book(cands[0]) if cands else {}
    else:
        op = _op_book(operational)

    book_id = op.get("book_id")
    name = op.get("book_label")
    status = op.get("current_status")
    initialized = op.get("initialized", book_id is not None)
    mark = op.get("desk_mark_date") or op.get("nav_as_of_date")
    if not book_id or not initialized:
        return {
            "active_book_id": book_id, "active_book_name": name,
            "active_book_status": status or "UNAVAILABLE",
            "active_book_authoritative_owner": owner,
            "operational_mark_date": mark, "ambiguous": False,
            "candidate_book_ids": [book_id] if book_id else [],
            "note": "Active operational book is not resolvable / not initialized.",
        }
    return {
        "active_book_id": book_id, "active_book_name": name,
        "active_book_status": status, "active_book_authoritative_owner": owner,
        "operational_mark_date": mark, "ambiguous": False,
        "candidate_book_ids": [book_id],
        "note": "Active operational book resolved via the authoritative selection policy.",
    }


# --------------------------------------------------------------------------- #
# Cross-surface consistency validator (Workstream G). Read-only; no provider call.
# Compares the canonical freshness contract with the authoritative read models and
# reports CONSISTENT / INCONSISTENT / UNKNOWN, naming every violation.
# --------------------------------------------------------------------------- #
def _build_consistency(*, by_id: dict, operational: Optional[dict],
                       daily_close_status: Optional[dict], desk_marks: Optional[dict],
                       active_book: dict, session,
                       expected_book_id: Optional[str]) -> tuple[str, list]:
    op = _op_book(operational)
    ct = op.get("current_target") or {}
    violations: list[dict[str, Any]] = []
    saw_unknown = False

    def _cmp(concept: str, freshness_val: Any, authoritative_val: Any,
             owner: str, code: str) -> None:
        nonlocal saw_unknown
        fv = _coerce_iso(freshness_val)
        av = _coerce_iso(authoritative_val)
        if fv is None or av is None:
            saw_unknown = True
            return
        if fv != av:
            violations.append({"code": code, "concept": concept,
                               "freshness_value": fv, "authoritative_value": av,
                               "authoritative_owner": owner})

    _cmp("operational_valuation", by_id["operational_valuation"]["as_of_date"],
         op.get("nav_as_of_date"), "api.operational_book",
         "OPERATIONAL_VALUATION_MISMATCH")
    _cmp("desk_mark", by_id["desk_marks"]["as_of_date"],
         op.get("desk_mark_date") or op.get("latest_desk_mark_date"),
         "api.paper_trading_desk", "DESK_MARK_MISMATCH")
    _cmp("benchmark", by_id["benchmark"]["as_of_date"],
         _spy_date_at_or_before(desk_marks, op.get("desk_mark_date")
                                or op.get("latest_desk_mark_date")),
         "api.paper_trading_desk", "BENCHMARK_MISMATCH")
    _cmp("latest_daily_close", by_id["latest_daily_close"]["as_of_date"],
         (daily_close_status or {}).get("market_date"),
         "api.daily_close", "DAILY_CLOSE_MISMATCH")
    _cmp("target_calculation", by_id["target_calculation"]["as_of_date"],
         ct.get("alpha_market_date"), "api.alpha_target", "TARGET_DATE_MISMATCH")

    # Active-book identity matches the authoritative operational book.
    ab_id = active_book.get("active_book_id")
    if active_book.get("ambiguous"):
        violations.append({"code": "MULTIPLE_ACTIVE_BOOK_CANDIDATES",
                           "concept": "active_book",
                           "candidate_book_ids": active_book.get("candidate_book_ids"),
                           "authoritative_owner": "api.operational_book"})
    elif not ab_id:
        saw_unknown = True
    elif expected_book_id and ab_id != expected_book_id:
        violations.append({"code": "ACTIVE_BOOK_ID_MISMATCH", "concept": "active_book",
                           "freshness_value": ab_id, "authoritative_value": expected_book_id,
                           "authoritative_owner": "api.operational_book"})

    # No dormant / non-operational book supplied the owned-data confirmation: the
    # session's confirmed owned date MUST equal the active operational desk mark.
    confirmed = _coerce_iso(session.latest_confirmed_owned_data_date)
    op_desk = _coerce_iso(op.get("desk_mark_date") or op.get("latest_desk_mark_date"))
    if confirmed is not None and op_desk is not None and confirmed != op_desk:
        violations.append({
            "code": "NON_OPERATIONAL_OWNED_DATA_CONFIRMATION", "concept": "owned_data_confirmation",
            "freshness_value": confirmed, "authoritative_value": op_desk,
            "authoritative_owner": "api.operational_book",
            "detail": "Owned-data confirmation did not come from the active operational desk mark."})

    # Research-only dates are explicitly labelled research-only.
    for sid in _RESEARCH_SOURCE_IDS:
        if by_id.get(sid, {}).get("kind") != KIND_RESEARCH:
            violations.append({"code": "RESEARCH_LABEL_MISSING", "concept": sid,
                               "authoritative_owner": "api.data_freshness"})

    if violations:
        return INCONSISTENT, violations
    if saw_unknown:
        return UNKNOWN, violations
    return CONSISTENT, violations


# --------------------------------------------------------------------------- #
# Public entry point.
# --------------------------------------------------------------------------- #
def load_data_freshness(
    *,
    now: Optional[datetime] = None,
    reference_today: Any = None,
    close_cutoff_et: Any = msession.DEFAULT_CLOSE_CUTOFF_ET,
    operational: Optional[dict] = None,
    inputs: Optional[dict] = None,
    daily_status: Optional[dict] = None,
    desk_marks: Optional[dict] = None,
    daily_close_status: Optional[dict] = None,
    forward_status: Optional[dict] = None,
    date_overrides: Optional[dict] = None,
    active_book_override: Any = None,
) -> dict[str, Any]:
    """Return the canonical cross-source data-freshness contract (read-only).

    Clock: pass ``now`` (datetime) or ``reference_today`` (offline date), else the
    real ET clock is resolved. Every read model can be injected for deterministic
    tests; otherwise the existing authoritative loaders are called degrade-safely:

      * ``operational``  → ``operational_book.load_operational_book`` (active book)
      * ``inputs``       → ``multi_horizon_engine.load_inputs`` (research inputs)
      * ``daily_status`` → ``current_alpha_daily_refresh.load_current_alpha_daily_status``
      * ``desk_marks``   → ``paper_trading_desk.read_marks`` (benchmark series)
      * ``daily_close_status`` → ``daily_close.load_close_progress`` (probe-free)
      * ``forward_status``     → ``forward_prediction_skill.load_prediction_skill``
    """
    warnings: list[str] = []

    if operational is None:
        def _load_operational():
            from paper_trader.api import operational_book as ob
            return ob.load_operational_book()
        operational = _safe(_load_operational, warnings, "Active operational book") or {}
    if inputs is None:
        def _load_inputs():
            from paper_trader.api import multi_horizon_engine as eng
            return eng.load_inputs()
        inputs = _safe(_load_inputs, warnings, "Owned model inputs") or {}
    if daily_status is None:
        def _load_daily_status():
            from paper_trader.api.current_alpha_daily_refresh import (
                load_current_alpha_daily_status)
            return load_current_alpha_daily_status()
        daily_status = _safe(_load_daily_status, warnings, "Champion research daily status") or {}
    if desk_marks is None:
        def _load_desk_marks():
            from paper_trader.api import paper_trading_desk as desk
            return desk.read_marks()
        desk_marks = _safe(_load_desk_marks, warnings, "Desk marks") or {}
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

    dates = _extract_dates(operational=operational, inputs=inputs,
                           daily_status=daily_status, desk_marks=desk_marks,
                           daily_close_status=daily_close_status,
                           forward_status=forward_status,
                           overrides=(date_overrides or {}))

    active_book = _resolve_active_book(operational, active_book_override)

    # Owned-data confirmation for the market session comes from the ACTIVE
    # operational book's owned desk mark (holdings). A dormant / legacy research
    # book can never supply it. The SPY mark is the independent benchmark series
    # used only for the holiday cross-check.
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
    for idx, spec in enumerate(_SOURCES):
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
            "kind": spec["kind"],
            "owned_operational_mark": bool(spec.get("owned")),
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
            "_registry_index": idx,
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

    # "Daily inputs" = the owned OPERATIONAL daily marks that confirm the session.
    all_daily_inputs_fresh = all(r["status"] == FRESH for r in rows if r["owned_operational_mark"])
    slower_inputs_due = [r["source_id"] for r in rows
                         if r["cadence"] in (MONTHLY, QUARTERLY) and r["status"] == STALE]

    # Weakest gate + required actions. Session first (if it blocks), then every
    # blocking required source (ALL reported — none hidden). Blocking sources are
    # ordered so a slower-cadence PACING input (a monthly/quarterly refresh that is
    # due, which gates the daily research refreshes at a period boundary) is named
    # as the root research gate ahead of the daily inputs it paces.
    def _gate_rank(r: dict) -> tuple[int, int]:
        cadence_rank = 0 if r["cadence"] in (MONTHLY, QUARTERLY) else 1
        return (cadence_rank, r["_registry_index"])

    blocking_sources = sorted([r for r in rows if r["blocks_current_operation"]],
                              key=_gate_rank)
    required_actions: list[dict[str, Any]] = []
    if session.weakest_gate != msession.GATE_NONE:
        required_actions.append({
            "gate": "market_session", "source_id": None,
            "status": session.session_status, "action": session.operator_action})
    for r in blocking_sources:
        required_actions.append({
            "gate": "source", "source_id": r["source_id"],
            "kind": r["kind"], "status": r["status"], "action": r["operator_action"]})

    if session.weakest_gate != msession.GATE_NONE:
        weakest_gate = "MARKET_SESSION:%s" % session.weakest_gate
    elif blocking_sources:
        weakest_gate = "SOURCE:%s" % blocking_sources[0]["source_id"]
    else:
        weakest_gate = "NONE"

    # D-7: research staleness never invalidates a completed operational close.
    if by_id["latest_daily_close"]["as_of_date"] and not signal_refresh_ready:
        warnings.append(
            "Research/signal readiness is separate from operational-close validity: "
            "the completed close at %s remains valid."
            % by_id["latest_daily_close"]["as_of_date"])

    # Cross-surface consistency (Workstream G) — read-only, no provider call.
    consistency_status, consistency_violations = _build_consistency(
        by_id=by_id, operational=operational, daily_close_status=daily_close_status,
        desk_marks=desk_marks, active_book=active_book, session=session,
        expected_book_id=_expected_operational_book_id())
    if consistency_status == INCONSISTENT:
        warnings.append(
            "Cross-surface consistency check found %d violation(s); see "
            "consistency_violations." % len(consistency_violations))

    # Strip the internal ordering key from the public rows.
    for r in rows:
        r.pop("_registry_index", None)

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
        "active_book": active_book,
        "source_freshness": rows,
        "all_daily_inputs_fresh": bool(all_daily_inputs_fresh),
        "slower_inputs_due": slower_inputs_due,
        "signal_refresh_ready": signal_refresh_ready,
        "portfolio_reassessment_ready": portfolio_reassessment_ready,
        "true_forward_capture_ready": true_forward_capture_ready,
        "operational_close_ready": operational_close_ready,
        "weakest_gate": weakest_gate,
        "required_actions": required_actions,
        "consistency_status": consistency_status,
        "consistency_violations": consistency_violations,
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
            "active_book_owner": "api.operational_book",
            "composed_read_models": [
                "operational_book.load_operational_book",
                "multi_horizon_engine.load_inputs",
                "current_alpha_daily_refresh.load_current_alpha_daily_status",
                "paper_trading_desk.read_marks",
                "daily_close.load_close_progress",
                "forward_prediction_skill.load_prediction_skill",
            ],
            "note": "Read-only composition; no calculation duplicated, no write performed. "
                    "Operational dates are owned by the active operational book; a dormant "
                    "legacy/current-alpha research book never supplies operational readiness.",
        },
    }


__all__ = [
    "PHASE",
    "FRESH", "STALE", "MISSING", "FUTURE_DATED", "INCONSISTENT",
    "NOT_DUE", "NOT_APPLICABLE", "UNKNOWN", "FRESHNESS_VOCAB",
    "DAILY", "MONTHLY", "QUARTERLY", "EVENT_DRIVEN", "STATIC",
    "KIND_OPERATIONAL", "KIND_RESEARCH",
    "CONSISTENT", "CONSISTENCY_VOCAB",
    "classify_source",
    "load_data_freshness",
]
