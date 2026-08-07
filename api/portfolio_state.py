"""api/portfolio_state.py — Slice 5 canonical operational portfolio-state owner.

READ-ONLY composition service (Consolidation Roadmap Slice 5; TARGET_ARCHITECTURE
context "Canonical portfolio state / one NAV"). It is the ONE authoritative owner
of the *complete current operational state of the active Alpha Paper Book*: the
active-book identity, the operational dates, the capital block (cash / invested /
NAV / cost basis / P&L / cumulative return / benchmark / drawdown), the per-holding
positions, the order / fill summaries, the current target reference, the latest
portfolio-assessment reference, the forward-evidence reference, the operational
safety mode, and a deterministic read-only consistency verdict.

It **composes** existing authoritative read models and NEVER recomputes their
business logic (Principle 1 / Principle 6 / Decision D-4). It owns no persistent
state, performs NO provider network call, NO prediction call, NO Daily Close, NO
research refresh, NO portfolio reassessment, and NO write of any kind (file,
ledger, database, snapshot, order, signal, decision, fill, cache).

One concept, one owner — the delegated sources:

  * active operational book + capital + positions + orders + target ref
        → ``api.operational_book.load_operational_book`` (the authoritative
          book-selection policy; NAV/cash/holdings/pending orders each produced
          exactly once by the append-only desk-ledger replay ``desk.book_nav``).
  * operational + research dates + active-book selection + freshness consistency
        → ``api.data_freshness.load_data_freshness`` (built on
          ``engine.market_session``): eligible market date, valuation, desk mark,
          benchmark, latest Daily Close and target-calculation dates.
  * cumulative performance (cumulative return, benchmark value/date, excess vs
    benchmark, drawdown)  → ``api.paper_trading_desk.load_performance`` (the
          append-only forward-performance ledger; historical rows never recomputed).
  * portfolio assessment (date / outcome / proposed-change count / target state)
        → ``api.daily_action_gate.load_daily_action_gate`` (the paper-only,
          preview-first reassessment bridge — this slice never runs it).
  * forward evidence (state / latest snapshot / counts)
        → ``api.forward_prediction_skill.load_prediction_skill``.

Active-book selection (Workstream C): the ACTIVE operational Alpha Paper Book #1
is selected through the authoritative ``operational_book`` policy (mirrored by
``data_freshness.active_book``). The dormant legacy DB book
(``portfolio_valuation`` — ``legacy_paper_portfolio``, marked 2026-07-20) is
NEVER selected merely because it exists or has a convenient loader; it is reported
only as an explicitly ignored legacy archive.

Separation of concerns (Decision D-7 upheld): research/model staleness or a
documented forward-evidence gap never invalidates a completed operational close or
the current portfolio valuation. The preliminary reassessment proposal is
review-only and unapproved: the Holding Opportunity-Cost engine (Slice 6) and the
Reallocation Proposal engine (Slice 7) do not exist yet.

Slice 5 scope: this service INTERPRETS state only. It performs no workflow action.
"""
from __future__ import annotations

import hashlib
import json
import os
from datetime import date, datetime, timezone
from typing import Any, Callable, Optional

PHASE = "29F-Slice5"
SCHEMA_VERSION = "portfolio_state.v1"

# --------------------------------------------------------------------------- #
# Frozen top-level portfolio-state vocabulary (part of the tested contract).
# --------------------------------------------------------------------------- #
STATE_READY = "PORTFOLIO_STATE_READY"
STATE_READY_WITH_PENDING_CLOSE = "PORTFOLIO_STATE_READY_WITH_PENDING_CLOSE"
STATE_DEGRADED = "PORTFOLIO_STATE_DEGRADED"
STATE_INCONSISTENT = "PORTFOLIO_STATE_INCONSISTENT"
STATE_NO_ACTIVE_BOOK = "NO_ACTIVE_BOOK"
STATE_UNAVAILABLE = "PORTFOLIO_STATE_UNAVAILABLE"
PORTFOLIO_STATES = (STATE_READY, STATE_READY_WITH_PENDING_CLOSE, STATE_DEGRADED,
                    STATE_INCONSISTENT, STATE_NO_ACTIVE_BOOK, STATE_UNAVAILABLE)

# Phase 29G.3 (Workstream E) explicit pre-close classification. A valuation that is
# exactly ONE eligible session ahead of the latest Daily Close — while the current
# session is SESSION_READY and its close is simply due and not yet run — is the EXPECTED
# operational state after an owned-data refresh and before the Daily Close. It is
# classified PENDING_DAILY_CLOSE (attention-level), NEVER a corrupted/INCONSISTENT state.
PENDING_DAILY_CLOSE = "PENDING_DAILY_CLOSE"
EXPECTED_PRE_CLOSE_GAP = "EXPECTED_PRE_CLOSE_GAP"

# Frozen consistency-verdict vocabulary (Workstream D).
CONSISTENT = "CONSISTENT"
DEGRADED = "DEGRADED"
INCONSISTENT = "INCONSISTENT"
UNAVAILABLE = "UNAVAILABLE"
CONSISTENCY_VOCAB = (CONSISTENT, DEGRADED, INCONSISTENT, UNAVAILABLE)

# Per-check result vocabulary.
CHECK_PASS = "PASS"
CHECK_FAIL = "FAIL"
CHECK_UNKNOWN = "UNKNOWN"

# The dormant legacy DB book that must NEVER be selected as the active book.
LEGACY_BOOK_ID = "legacy_paper_portfolio"

# Non-terminal desk order statuses = "pending" from the operator's viewpoint.
_PENDING_STATUSES = ("PROPOSED", "APPROVED", "SUBMITTED")

# Decimal-safe money tolerance (the project-wide reconciliation epsilon, ±$0.01).
MONEY_TOL = 0.01

# Assessment outcomes that constitute a (review-only) proposal.
_PROPOSAL_OUTCOMES = frozenset({
    "PROPOSAL_READY", "APPROVAL_REQUIRED", "PORTFOLIO_CHANGES_PROPOSED", "PROPOSED"})

# Daily-close final statuses that mark a valid completed operational close.
_CLOSE_COMPLETE_STATUSES = frozenset({
    "DAILY_CLOSE_COMPLETE_HOLD", "REBALANCE_PROPOSAL_READY",
    "PAPER_ORDERS_SUBMITTED", "INITIAL_BASELINE_RECORDED", "ALREADY_PROCESSED"})

# The canonical review-only proposal banner. Slice 7 (Phase 29H): the reassessment is
# backed by the Holding Opportunity-Cost review (Slice 6) and the Reallocation Proposal
# engine (Slice 7, LANDED). It remains REVIEW ONLY — a research proposal that confirms no
# target and creates no order; it is NEVER an approved reallocation.
PRELIMINARY_PROPOSAL_LABEL = (
    "REALLOCATION PROPOSAL — MANUAL REVIEW REQUIRED (REVIEW ONLY, NO ORDERS)")

SAFETY_BADGES = ["READ ONLY", "NO PROVIDER CALL", "NO PREDICTION CALL",
                 "NO ORDERS", "NO FILLS", "AUTOMATION OFF", "MANUAL REVIEW",
                 "PREVIEW ONLY"]

# Keys stripped before hashing so the state hash is stable for unchanged source
# state (Workstream I): generated_at is explicitly excluded, along with every
# other transient timestamp/derived field.
_VOLATILE_KEYS = frozenset({
    "generated_at", "evaluated_at", "loaded_at", "updated_at", "built_at",
    "recorded_at", "started_at", "state_hash", "source_hashes", "warnings"})

# Deterministic clock seam (tests / explicit callers).
NOW_ENV = "PAPER_TRADER_PORTFOLIO_STATE_NOW"
_now_override: Optional[datetime] = None


# --------------------------------------------------------------------------- #
# Small pure helpers.
# --------------------------------------------------------------------------- #
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


def _f(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        return float(str(x))
    except (TypeError, ValueError):
        return None


def _coerce_iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    try:
        return date.fromisoformat(str(value)[:10]).isoformat()
    except (TypeError, ValueError):
        return None


def _safe(loader: Callable, warnings: list, label: str) -> Any:
    try:
        return loader()
    except Exception as exc:  # noqa: BLE001 — the read model must always load
        warnings.append("%s unavailable: %s" % (label, str(exc)[:160]))
        return None


def _op_book(operational: Optional[dict]) -> dict:
    """Unwrap the active operational-book dict from
    ``operational_book.load_operational_book`` (which nests it under
    ``operational_book``). A book dict injected directly (tests) is returned
    unchanged."""
    op = operational or {}
    inner = op.get("operational_book")
    return inner if isinstance(inner, dict) else op


def _canonical(operational: Optional[dict]) -> dict:
    """The active book's canonical_state block (or the injected dict itself)."""
    op = operational or {}
    cs = op.get("canonical_state")
    if isinstance(cs, dict):
        return cs
    inner = _op_book(operational)
    cs = inner.get("canonical_state")
    return cs if isinstance(cs, dict) else inner


def _row(freshness: Optional[dict], source_id: str) -> Any:
    for r in ((freshness or {}).get("source_freshness") or []):
        if r.get("source_id") == source_id:
            return r.get("as_of_date")
    return None


def _strip_volatile(obj: Any) -> Any:
    """Deep copy of ``obj`` with every transient/derived key removed so a stable
    content hash can be computed (generated_at is excluded — Workstream I)."""
    if isinstance(obj, dict):
        return {k: _strip_volatile(v) for k, v in obj.items()
                if k not in _VOLATILE_KEYS}
    if isinstance(obj, (list, tuple)):
        return [_strip_volatile(v) for v in obj]
    return obj


def _stable_hash(obj: Any) -> str:
    payload = json.dumps(_strip_volatile(obj), sort_keys=True, default=str,
                         separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- #
# Lazy import seams (avoid an api.app import cycle; each is monkeypatchable).
# --------------------------------------------------------------------------- #
def _import_operational():
    from paper_trader.api import operational_book
    return operational_book


def _import_freshness():
    from paper_trader.api import data_freshness
    return data_freshness


def _import_desk():
    from paper_trader.api import paper_trading_desk
    return paper_trading_desk


def _import_gate():
    from paper_trader.api import daily_action_gate
    return daily_action_gate


def _import_fps():
    from paper_trader.api import forward_prediction_skill
    return forward_prediction_skill


def _previous_trading_day_iso(iso_date: Optional[str]) -> Optional[str]:
    """The immediately-preceding trading session for an ISO date, via the authoritative
    calendar owner (``engine.market_session.previous_trading_day``). Degrade-safe: returns
    None if the date is unparseable or the calendar owner is unavailable."""
    if not iso_date:
        return None
    try:
        from paper_trader.engine import market_session as ms
        return ms.previous_trading_day(date.fromisoformat(iso_date)).isoformat()
    except Exception:  # noqa: BLE001
        return None


def _valuation_vs_close_check(*, valuation: Any, latest_close: Any,
                              eligible: Any) -> dict:
    """Classified valuation-vs-Daily-Close consistency check (Workstream E).

    * PASS                     — valuation aligned with the latest completed Daily Close.
    * PASS + EXPECTED_PRE_CLOSE_GAP — valuation is exactly ONE eligible session ahead of the
      latest close AND the current session is SESSION_READY (valuation == eligible): the
      Daily Close for the current session is due and not yet complete. Attention-level, not
      a corruption; carries ``pending_daily_close: True``.
    * FAIL                     — valuation BEHIND the latest close, a valuation dated AHEAD of
      the current eligible session (future-dated), a gap larger than one eligible session,
      or a missing close while a valuation exists. Genuine inconsistency, never hidden.
    * UNKNOWN                  — a side is unavailable.
    """
    owners = ["api.operational_book", "api.daily_close"]
    v = _coerce_iso(valuation)
    c = _coerce_iso(latest_close)
    e = _coerce_iso(eligible)
    base = {"code": "VALUATION_VS_DAILY_CLOSE", "concept": "valuation_vs_daily_close",
            "value_a": v, "value_b": c, "eligible_market_date": e,
            "authoritative_owners": owners}
    if v is None or c is None:
        return {**base, "result": CHECK_UNKNOWN, "detail": "One side is unavailable."}
    if v == c:
        return {**base, "result": CHECK_PASS,
                "detail": "Valuation is aligned with the latest completed Daily Close."}
    if v < c:
        return {**base, "result": CHECK_FAIL,
                "detail": "Valuation is BEHIND the latest completed Daily Close."}
    if e is not None and v > e:
        return {**base, "result": CHECK_FAIL, "future_dated": True,
                "detail": "Valuation is dated AHEAD of the current eligible session."}
    one_session_gap = (_previous_trading_day_iso(v) == c)
    session_ready = (e is not None and v == e)
    if one_session_gap and session_ready:
        return {**base, "result": CHECK_PASS,
                "classification": EXPECTED_PRE_CLOSE_GAP,
                "pending_daily_close": True,
                "detail": ("Valuation is exactly one eligible session ahead of the latest "
                           "Daily Close; the current session is SESSION_READY and its Daily "
                           "Close is due and not yet complete (expected pre-close state).")}
    return {**base, "result": CHECK_FAIL,
            "detail": ("Valuation is more than one eligible session ahead of the latest "
                       "Daily Close (not an expected single pending close).")}


# --------------------------------------------------------------------------- #
# Active-book selection (Workstream C). The ACTIVE operational book is resolved
# through the authoritative policy (api.operational_book / data_freshness). The
# dormant legacy DB book is NEVER selected; it is reported only as ignored.
# --------------------------------------------------------------------------- #
def _select_active_book(*, operational: dict, freshness: Optional[dict]) -> dict:
    ob = _op_book(operational)
    cs = _canonical(operational)
    fresh_ab = (freshness or {}).get("active_book") or {}

    book_id = ob.get("book_id") or cs.get("operational_book_id")
    book_label = ob.get("book_label") or cs.get("operational_book_name")
    initialized = bool(ob.get("initialized", book_id is not None))
    status = ob.get("current_status") or cs.get("operational_book_status")

    legacy = (cs.get("legacy_archive_summary") or {})
    legacy_id = legacy.get("book_id") or LEGACY_BOOK_ID
    legacy_ignored = {
        "book_id": legacy_id,
        "label": legacy.get("label"),
        "as_of_market_date": legacy.get("as_of_market_date"),
        "positions_count": legacy.get("positions_count"),
        "reason": ("Dormant legacy DB paper book (portfolio_valuation) is a "
                   "read-only historical archive and is never selected as the "
                   "active operational book."),
    }

    ambiguous = bool(fresh_ab.get("ambiguous"))
    if not book_id or not initialized:
        selection_reason = ("No initialized active operational book is resolvable "
                            "through the authoritative selection policy.")
    elif ambiguous:
        selection_reason = ("Multiple active-book candidates were reported by the "
                            "selection policy; refusing to select one silently.")
    elif book_id == legacy_id:
        # Defensive: the selection policy must never return the dormant legacy book.
        selection_reason = ("Selection policy returned the dormant legacy book — "
                            "rejected as the active operational book.")
    else:
        selection_reason = ("Active operational book resolved via the authoritative "
                            "operational_book selection policy.")

    return {
        "book_id": book_id,
        "book_label": book_label,
        "status": status or "UNAVAILABLE",
        "initialized": initialized,
        "execution_model": ob.get("execution_model") or cs.get("execution_model"),
        "holdings_count": int(cs.get("holdings_count") or ob.get("holdings_count") or 0),
        "pending_order_count": int(cs.get("pending_order_count")
                                   or ob.get("pending_order_count") or 0),
        "owner_module": "api.operational_book",
        "selection_policy_owner": (fresh_ab.get("active_book_authoritative_owner")
                                   or "api.operational_book"),
        "selection_reason": selection_reason,
        "ambiguous": ambiguous,
        "freshness_active_book_id": fresh_ab.get("active_book_id"),
        "legacy_book_ignored": legacy_ignored,
        "is_dormant_legacy_book": bool(book_id == legacy_id),
    }


# --------------------------------------------------------------------------- #
# Position normalization (Workstream B). Reads the ledger-replayed per-holding
# detail from the owner; performs NO valuation math of its own.
# --------------------------------------------------------------------------- #
def _build_positions(operational: dict) -> list[dict]:
    cs = _canonical(operational)
    rows = cs.get("holdings_detail")
    if not isinstance(rows, list):
        inner = _op_book(operational)
        rows = inner.get("holdings_detail") if isinstance(
            inner.get("holdings_detail"), list) else []
    positions: list[dict] = []
    for r in rows:
        positions.append({
            "ticker": r.get("ticker"),
            "sector": r.get("sector") or "Unknown",
            "quantity": r.get("quantity"),
            "average_cost": _f(r.get("average_cost")),
            "cost_basis": _f(r.get("cost_basis")),
            "price": _f(r.get("latest_price")),
            "market_value": _f(r.get("market_value")),
            "unrealized_pnl": _f(r.get("unrealized_pnl")),
            "unrealized_return": _f(r.get("unrealized_pnl_pct")),
            "portfolio_weight": _f(r.get("current_weight")),
            "target_weight": _f(r.get("target_weight")),
            "drift": _f(r.get("weight_drift")),
            "daily_pnl": _f(r.get("daily_pnl")),
            "opened_date": r.get("opened_date"),
            "operational_status": r.get("status"),
            "provenance": "api.operational_book.build_holdings_detail",
        })
    positions.sort(key=lambda p: (p["portfolio_weight"] is None,
                                  -(p["portfolio_weight"] or 0.0),
                                  str(p["ticker"])))
    return positions


# --------------------------------------------------------------------------- #
# Consistency engine (Workstream D). Read-only cross-source cross-checks; never
# silently repairs. Returns (verdict, checks[]) with explicit reason codes.
# --------------------------------------------------------------------------- #
def _cmp_date(code: str, concept: str, a: Any, b: Any,
              owner_a: str, owner_b: str) -> dict:
    av, bv = _coerce_iso(a), _coerce_iso(b)
    if av is None or bv is None:
        return {"code": code, "concept": concept, "result": CHECK_UNKNOWN,
                "value_a": av, "value_b": bv,
                "authoritative_owners": [owner_a, owner_b],
                "detail": "One side is unavailable."}
    return {"code": code, "concept": concept,
            "result": CHECK_PASS if av == bv else CHECK_FAIL,
            "value_a": av, "value_b": bv,
            "authoritative_owners": [owner_a, owner_b]}


def _cmp_int(code: str, concept: str, a: Any, b: Any,
             owner_a: str, owner_b: str) -> dict:
    if a is None or b is None:
        return {"code": code, "concept": concept, "result": CHECK_UNKNOWN,
                "value_a": a, "value_b": b,
                "authoritative_owners": [owner_a, owner_b],
                "detail": "One side is unavailable."}
    return {"code": code, "concept": concept,
            "result": CHECK_PASS if int(a) == int(b) else CHECK_FAIL,
            "value_a": int(a), "value_b": int(b),
            "authoritative_owners": [owner_a, owner_b]}


def _cmp_money(code: str, concept: str, a: Any, b: Any,
               owner_a: str, owner_b: str) -> dict:
    av, bv = _f(a), _f(b)
    if av is None or bv is None:
        return {"code": code, "concept": concept, "result": CHECK_UNKNOWN,
                "value_a": av, "value_b": bv,
                "authoritative_owners": [owner_a, owner_b],
                "detail": "One side is unavailable."}
    return {"code": code, "concept": concept,
            "result": CHECK_PASS if abs(av - bv) <= MONEY_TOL else CHECK_FAIL,
            "value_a": round(av, 2), "value_b": round(bv, 2),
            "difference": round(av - bv, 4), "tolerance": MONEY_TOL,
            "authoritative_owners": [owner_a, owner_b]}


def _build_consistency(*, active_book: dict, capital: dict, dates: dict,
                       positions: list, orders: dict, fills: dict,
                       target: dict, assessment: dict,
                       operational: dict, freshness: Optional[dict],
                       performance: Optional[dict],
                       gate: Optional[dict]) -> tuple[str, list, dict]:
    if not active_book.get("initialized") or not active_book.get("book_id"):
        return (UNAVAILABLE,
                [{"code": "NO_ACTIVE_BOOK", "concept": "active_book",
                  "result": CHECK_UNKNOWN,
                  "detail": "No initialized active operational book is resolvable."}],
                {"pass": 0, "fail": 0, "unknown": 1}, False)

    checks: list[dict] = []

    # 1. Operational valuation date vs active-book desk mark.
    checks.append(_cmp_date("VALUATION_VS_DESK_MARK", "valuation_vs_desk_mark",
                            dates.get("valuation_date"), dates.get("desk_mark_date"),
                            "api.operational_book", "api.paper_trading_desk"))
    # 2. Operational valuation date vs latest valid Daily Close. Phase 29G.3 (Workstream E):
    #    an expected single pending close (valuation one eligible session ahead of the latest
    #    close, current session SESSION_READY) is classified PENDING_DAILY_CLOSE, not FAIL; a
    #    larger gap, a future-dated valuation, or a valuation behind the close stays a FAIL.
    checks.append(_valuation_vs_close_check(
        valuation=dates.get("valuation_date"),
        latest_close=dates.get("latest_daily_close_date"),
        eligible=dates.get("eligible_market_date")))
    # 3. Benchmark date vs valuation date.
    checks.append(_cmp_date("BENCHMARK_VS_VALUATION", "benchmark_vs_valuation",
                            dates.get("benchmark_date"), dates.get("valuation_date"),
                            "api.paper_trading_desk", "api.operational_book"))
    # 4. Holdings count vs position rows.
    checks.append(_cmp_int("HOLDINGS_COUNT_VS_ROWS", "holdings_count_vs_rows",
                           active_book.get("holdings_count"), len(positions),
                           "api.operational_book", "api.portfolio_state"))
    # 5. NAV vs cash + total market value.
    mv_total = None
    if positions and all(p.get("market_value") is not None for p in positions):
        mv_total = sum(_f(p["market_value"]) for p in positions)
    nav = capital.get("nav")
    cash = capital.get("cash")
    cash_plus_mv = ((cash + mv_total) if (cash is not None and mv_total is not None)
                    else None)
    checks.append(_cmp_money("NAV_RECONCILIATION", "nav_vs_cash_plus_market_value",
                             nav, cash_plus_mv, "api.paper_trading_desk (book_nav)",
                             "api.portfolio_state (cash + Σ market_value)"))
    # 6. Invested value vs summed position market value.
    checks.append(_cmp_money("INVESTED_VS_POSITIONS", "invested_vs_positions",
                             capital.get("invested_value"), mv_total,
                             "api.operational_book", "api.portfolio_state (Σ market_value)"))
    # 7. Pending-order count vs pending-order rows.
    by_status = orders.get("by_status") or {}
    pending_rows = sum(int(by_status.get(s, 0)) for s in _PENDING_STATUSES)
    checks.append(_cmp_int("PENDING_ORDER_COUNT_VS_ROWS", "pending_order_count_vs_rows",
                           orders.get("pending_count"), pending_rows,
                           "api.operational_book", "api.paper_trading_desk (order fold)"))
    # 8. Fill count vs fill rows.
    checks.append(_cmp_int("FILL_COUNT_VS_ROWS", "fill_count_vs_rows",
                           fills.get("count"), fills.get("rows_count"),
                           "api.operational_book", "api.paper_trading_desk (fills)"))
    # 9. Target membership count vs target rows.
    checks.append(_cmp_int("TARGET_MEMBERSHIP_COUNT_VS_ROWS",
                           "target_membership_count_vs_rows",
                           target.get("count"), len(target.get("members") or []),
                           "api.operational_book", "api.portfolio_state"))
    # 10. Current target date vs canonical target owner.
    checks.append(_cmp_date("TARGET_DATE_VS_OWNER", "target_calculation_vs_owner",
                            dates.get("target_calculation_date"),
                            ((_op_book(operational).get("current_target") or {})
                             .get("alpha_market_date")),
                            "api.data_freshness", "api.alpha_target"))
    # 11. Assessment date vs proposal reference (must not be ahead of eligible).
    a_date = _coerce_iso(assessment.get("assessment_date"))
    elig = _coerce_iso(dates.get("eligible_market_date"))
    if a_date is None or elig is None:
        checks.append({"code": "ASSESSMENT_DATE_VS_REFERENCE",
                       "concept": "assessment_vs_eligible", "result": CHECK_UNKNOWN,
                       "value_a": a_date, "value_b": elig,
                       "authoritative_owners": ["api.daily_action_gate",
                                                "engine.market_session"]})
    else:
        checks.append({"code": "ASSESSMENT_DATE_VS_REFERENCE",
                       "concept": "assessment_vs_eligible",
                       "result": CHECK_PASS if a_date <= elig else CHECK_FAIL,
                       "value_a": a_date, "value_b": elig,
                       "authoritative_owners": ["api.daily_action_gate",
                                                "engine.market_session"],
                       "detail": ("Assessment date must not be ahead of the eligible "
                                  "completed session.")})
    # 12. Active-book identity across all contributing sources.
    ids = {
        "operational_book": _op_book(operational).get("book_id"),
        "data_freshness": ((freshness or {}).get("active_book") or {}).get("active_book_id"),
        "performance": (performance or {}).get("book_id"),
    }
    known = {k: v for k, v in ids.items() if v}
    if len(known) < 2:
        checks.append({"code": "ACTIVE_BOOK_IDENTITY", "concept": "active_book_identity",
                       "result": CHECK_UNKNOWN, "book_ids": ids,
                       "detail": "Too few sources reported a book id to cross-check."})
    else:
        distinct = set(known.values())
        checks.append({"code": "ACTIVE_BOOK_IDENTITY", "concept": "active_book_identity",
                       "result": CHECK_PASS if len(distinct) == 1 else CHECK_FAIL,
                       "book_ids": ids,
                       "authoritative_owners": ["api.operational_book",
                                                "api.data_freshness",
                                                "api.paper_trading_desk"]})

    # Inherit the upstream freshness consistency verdict verbatim (no conflict hidden).
    fresh_status = (freshness or {}).get("consistency_status")
    if fresh_status == INCONSISTENT:
        for v in ((freshness or {}).get("consistency_violations") or []):
            checks.append({"code": "UPSTREAM_FRESHNESS_INCONSISTENT",
                           "concept": "data_freshness", "result": CHECK_FAIL,
                           "detail": v.get("code"), "violation": v,
                           "authoritative_owners": ["api.data_freshness"]})

    n_fail = sum(1 for c in checks if c["result"] == CHECK_FAIL)
    n_unknown = sum(1 for c in checks if c["result"] == CHECK_UNKNOWN)
    n_pass = sum(1 for c in checks if c["result"] == CHECK_PASS)
    pending_daily_close = any(c.get("pending_daily_close") for c in checks)
    if n_fail:
        verdict = INCONSISTENT
    elif n_unknown:
        verdict = DEGRADED
    else:
        verdict = CONSISTENT
    counts = {"pass": n_pass, "fail": n_fail, "unknown": n_unknown}
    return verdict, checks, counts, pending_daily_close


# --------------------------------------------------------------------------- #
# Public entry point.
# --------------------------------------------------------------------------- #
def load_portfolio_state(
    *,
    now: Optional[datetime] = None,
    operational: Optional[dict] = None,
    freshness: Optional[dict] = None,
    performance: Optional[dict] = None,
    gate: Optional[dict] = None,
    forward_status: Optional[dict] = None,
    fills: Optional[dict] = None,
    active_book_override: Any = None,
) -> dict[str, Any]:
    """Return the canonical operational portfolio-state contract (read-only).

    Every contributing read model may be injected for deterministic tests;
    otherwise the existing authoritative loaders are called degrade-safely (each
    failure becomes a ``warnings[]`` entry, never an exception):

      * ``operational``    → ``operational_book.load_operational_book`` (active book)
      * ``freshness``      → ``data_freshness.load_data_freshness`` (dates/consistency)
      * ``performance``    → ``paper_trading_desk.load_performance`` (cumulative perf)
      * ``gate``           → ``daily_action_gate.load_daily_action_gate`` (assessment)
      * ``forward_status`` → ``forward_prediction_skill.load_prediction_skill`` (evidence)
      * ``fills``          → ``paper_trading_desk.load_fills`` (fill rows for reconcile)
    """
    global _now_override
    prev_override = _now_override
    if now is not None:
        _now_override = now
    warnings: list[str] = []
    try:
        # --- Load the raw domain read models ONCE (degrade-safe), unless injected. -- #
        if operational is None:
            operational = _safe(lambda: _import_operational().load_operational_book(),
                                warnings, "Active operational book") or {}
        if freshness is None:
            freshness = _safe(
                lambda: _import_freshness().load_data_freshness(operational=operational),
                warnings, "Data freshness") or {}
        if performance is None:
            performance = _safe(lambda: _import_desk().load_performance(),
                                warnings, "Book performance")
        if gate is None:
            gate = _safe(lambda: _import_gate().load_daily_action_gate(),
                         warnings, "Daily Action Gate assessment")
        if forward_status is None:
            forward_status = _safe(lambda: _import_fps().load_prediction_skill(),
                                   warnings, "Forward-prediction evidence")
        if fills is None:
            fills = _safe(lambda: _import_desk().load_fills(),
                          warnings, "Desk fills")

        return _compose(operational=operational, freshness=freshness,
                        performance=performance, gate=gate,
                        forward_status=forward_status, fills=fills,
                        active_book_override=active_book_override,
                        warnings=warnings)
    finally:
        _now_override = prev_override


def _compose(*, operational: dict, freshness: Optional[dict],
             performance: Optional[dict], gate: Optional[dict],
             forward_status: Optional[dict], fills: Optional[dict],
             active_book_override: Any, warnings: list) -> dict[str, Any]:
    cs = _canonical(operational)
    ob = _op_book(operational)

    # --- Active-book selection (Workstream C). --------------------------------- #
    active_book = _select_active_book(operational=operational, freshness=freshness)

    # --- Dates block (Workstream B). Owned by data_freshness / their owners. --- #
    perf_rows = (performance or {}).get("rows") or []
    perf_last = perf_rows[-1] if perf_rows else {}
    dates = {
        "eligible_market_date": (freshness or {}).get("eligible_market_date"),
        "valuation_date": cs.get("valuation_date") or ob.get("nav_as_of_date"),
        "desk_mark_date": cs.get("desk_mark_date") or ob.get("desk_mark_date"),
        "benchmark_date": (perf_last.get("date") or _row(freshness, "benchmark")),
        "latest_daily_close_date": _row(freshness, "latest_daily_close"),
        "target_calculation_date": (_row(freshness, "target_calculation")
                                    or (ob.get("current_target") or {}).get("alpha_market_date")),
        "portfolio_assessment_date": (gate or {}).get("latest_completed_market_date"),
        "provenance": {
            "eligible_market_date": "api.data_freshness (engine.market_session)",
            "valuation_date": "api.operational_book",
            "desk_mark_date": "api.paper_trading_desk",
            "benchmark_date": "api.paper_trading_desk (forward-performance ledger)",
            "latest_daily_close_date": "api.daily_close",
            "target_calculation_date": "api.alpha_target / api.data_freshness",
            "portfolio_assessment_date": "api.daily_action_gate",
        },
    }

    # --- Capital block (Workstream B). No formula is recomputed here. ---------- #
    initial_capital = _f(ob.get("starting_capital")) or _f(ob.get("initial_capital"))
    nav = _f(cs.get("nav"))
    cumulative_pnl = ((nav - initial_capital)
                      if (nav is not None and initial_capital is not None) else None)
    perf_summary = (performance or {}).get("summary") or {}
    capital = {
        "cash": _f(cs.get("cash")),
        "invested_value": _f(cs.get("invested_value")),
        "nav": nav,
        "cost_basis": _f(cs.get("cost_basis")),
        "unrealized_pnl": _f(cs.get("unrealized_pnl")),
        "unrealized_return": _f(cs.get("unrealized_pnl_pct")),
        "daily_pnl": _f(cs.get("daily_pnl")),
        "daily_return": _f(cs.get("daily_pnl_pct")),
        "cumulative_pnl": (round(cumulative_pnl, 2) if cumulative_pnl is not None else None),
        "cumulative_return": _f(perf_summary.get("cumulative_return_pct")),
        "cumulative_return_units": "PERCENT_POINTS",
        "cumulative_excess_vs_benchmark": _f(perf_summary.get("excess_vs_benchmark_pct_points")),
        "drawdown": _f(perf_last.get("drawdown_pct")),
        "max_drawdown": _f(perf_summary.get("max_drawdown_pct")),
        "benchmark_ticker": perf_last.get("benchmark_ticker") or "SPY",
        "benchmark_value": _f(perf_last.get("benchmark_close")),
        "benchmark_cumulative_return": _f(perf_last.get("benchmark_cumulative_return_pct")),
        "benchmark_date": perf_last.get("date"),
        "initial_capital": initial_capital,
        "currency": ob.get("currency"),
        "provenance": {
            "cash": "api.paper_trading_desk.book_nav (ledger replay)",
            "invested_value": "api.operational_book",
            "nav": "api.paper_trading_desk.book_nav (ledger replay)",
            "cost_basis": "api.operational_book (BUY-fill replay)",
            "unrealized_pnl": "api.operational_book",
            "daily_pnl": "api.operational_book",
            "cumulative_pnl": "api.portfolio_state (nav − initial_capital)",
            "cumulative_return": "api.paper_trading_desk.load_performance",
            "cumulative_excess_vs_benchmark": "api.paper_trading_desk.load_performance",
            "drawdown": "api.paper_trading_desk.load_performance",
            "benchmark_value": "api.paper_trading_desk.load_performance (SPY close)",
        },
    }

    # --- Positions (Workstream B). --------------------------------------------- #
    positions = _build_positions(operational)

    # --- Orders (Workstream B). ------------------------------------------------ #
    po = ob.get("pending_orders") or {}
    by_status = po.get("by_status") or {}
    orders = {
        "submitted": int(by_status.get("APPROVED", 0)) + int(by_status.get("SUBMITTED", 0)),
        "proposed": int(by_status.get("PROPOSED", 0)),
        "pending_count": int(po.get("pending_count") or ob.get("pending_order_count") or 0),
        "filled": int(by_status.get("FILLED", 0)),
        "cancelled": int(by_status.get("CANCELLED", 0)),
        "expired": int(by_status.get("EXPIRED", 0)),
        "total_orders": int(po.get("total_orders") or 0),
        "by_status": by_status,
        "latest_submission_date": po.get("latest_submission_date"),
        "provenance": "api.paper_trading_desk (operational_book order fold)",
    }

    # --- Fills (Workstream B). ------------------------------------------------- #
    book_id = active_book.get("book_id")
    fill_rows = [r for r in ((fills or {}).get("fills") or [])
                 if r.get("book_id") == book_id]
    fill_dates = [r.get("fill_date") for r in fill_rows if r.get("fill_date")]
    fills_block = {
        "count": int(ob.get("fills_count") or ob.get("fill_count") or 0),
        "rows_count": len(fill_rows),
        "latest_fill_date": (max(fill_dates) if fill_dates else None),
        "provenance": "api.paper_trading_desk.load_fills / api.operational_book",
    }

    # --- Target reference (Workstream B). Membership + weights the book tracks. - #
    tw = ob.get("frozen_target_weights") or {}
    members = [{"ticker": tk, "target_weight": _f(w)} for tk, w in sorted(tw.items())]
    target = {
        "target_calculation_date": dates["target_calculation_date"],
        "target_state": (ob.get("target_confirmation_status")
                         or (ob.get("current_target") or {}).get("state")),
        "confirmed_target_date": ob.get("target_market_date"),
        "count": int(ob.get("target_count") or 0) or (len(members) or None),
        "members": members,
        "membership_count": len(members),
        "target_name": ob.get("target_name"),
        "provenance": "api.operational_book (frozen target snapshot) / api.alpha_target",
    }

    # --- Assessment reference (Workstream B). Review-only; NEVER an approval. --- #
    outcome = (gate or {}).get("outcome")
    is_proposal = outcome in _PROPOSAL_OUTCOMES
    assessment = {
        "assessment_date": (gate or {}).get("latest_completed_market_date"),
        "outcome": outcome,
        "outcome_label": (gate or {}).get("outcome_label"),
        "target_state": (gate or {}).get("target_state"),
        "proposed_change_count": (gate or {}).get("proposed_change_count"),
        "proposed_additions": len((gate or {}).get("proposed_additions") or []),
        "proposed_removals": len((gate or {}).get("proposed_removals") or []),
        "proposed_resizes": len((gate or {}).get("proposed_resizes") or []),
        "estimated_turnover": (gate or {}).get("estimated_turnover"),
        "headline": (gate or {}).get("headline"),
        "is_preliminary_proposal": bool(is_proposal),
        "proposal_status": ("PRELIMINARY_REVIEW_ONLY_UNAPPROVED" if is_proposal
                            else "NO_PROPOSAL"),
        "preliminary_proposal_label": PRELIMINARY_PROPOSAL_LABEL,
        "proposal_note": ("The reassessment proposal is review-only. It is backed by the "
                          "Slice 6 Holding Opportunity-Cost review but is NOT an approved "
                          "reallocation: the Reallocation Proposal engine (Slice 7) is not "
                          "implemented yet. Manual review remains mandatory; no paper orders "
                          "are created."),
        "confirmation_allowed": False,
        # Slice 6 (Phase 29G) opportunity-cost compatibility summary from the gate.
        "opportunity_cost_available": bool((gate or {}).get("opportunity_cost_available")),
        "opportunity_cost_assessment_hash": (gate or {}).get("opportunity_cost_assessment_hash"),
        "opportunity_cost_recommendation_counts": (
            (gate or {}).get("opportunity_cost_recommendation_counts") or {}),
        "opportunity_cost_data_gaps": list((gate or {}).get("opportunity_cost_data_gaps") or []),
        "provenance": "api.daily_action_gate.load_daily_action_gate",
    }

    # --- Evidence reference (Workstream B). ------------------------------------ #
    evidence = {
        "evidence_state": (forward_status or {}).get("evidence_state"),
        "interpretation": (forward_status or {}).get("interpretation"),
        "latest_snapshot_date": (forward_status or {}).get("latest_snapshot_date"),
        "snapshot_count": (forward_status or {}).get("snapshot_count"),
        "matured_outcome_count": (forward_status or {}).get("matured_outcome_count"),
        "provenance": "api.forward_prediction_skill.load_prediction_skill",
    }

    # --- Consistency engine (Workstream D). ------------------------------------ #
    consistency_status, checks, counts, pending_daily_close = _build_consistency(
        active_book=active_book, capital=capital, dates=dates, positions=positions,
        orders=orders, fills=fills_block, target=target, assessment=assessment,
        operational=operational, freshness=freshness, performance=performance,
        gate=gate)
    consistency = {
        "status": consistency_status,
        "vocabulary": list(CONSISTENCY_VOCAB),
        "checks": checks,
        "counts": counts,
        # Workstream E: the expected pre-close condition is surfaced explicitly and is NOT a
        # corruption (the Daily Close for the current session is simply due and not yet run).
        "pending_daily_close": bool(pending_daily_close),
        "pending_close_reason": (EXPECTED_PRE_CLOSE_GAP if pending_daily_close else None),
        "inherited_freshness_consistency": (freshness or {}).get("consistency_status"),
    }
    if consistency_status == INCONSISTENT:
        warnings.append("Portfolio-state consistency found %d failing check(s); see "
                        "consistency.checks." % counts["fail"])
    if pending_daily_close:
        warnings.append("The current session is SESSION_READY and its Daily Close is due "
                        "and not yet complete (expected pre-close state); this is not a "
                        "portfolio inconsistency.")

    # --- Top-level portfolio state (Workstream E). ----------------------------- #
    # A genuine inconsistency / degradation always wins. When the ONLY deviation from a
    # fully-aligned book is the expected pending Daily Close, the state is the explicit,
    # readable READY_WITH_PENDING_CLOSE — never INCONSISTENT (corruption).
    if not active_book.get("initialized") or not active_book.get("book_id"):
        state = STATE_NO_ACTIVE_BOOK
    elif consistency_status == UNAVAILABLE:
        state = STATE_UNAVAILABLE
    elif consistency_status == INCONSISTENT:
        state = STATE_INCONSISTENT
    elif consistency_status == DEGRADED:
        state = STATE_DEGRADED
    elif pending_daily_close:
        state = STATE_READY_WITH_PENDING_CLOSE
    else:
        state = STATE_READY

    # --- Safety (Workstream H). ------------------------------------------------ #
    safety = {
        "read_only": True,
        "wrote_to_database": False,
        "wrote_to_ledger": False,
        "called_provider": False,
        "called_prediction": False,
        "ran_daily_close": False,
        "ran_research_refresh": False,
        "ran_portfolio_reassessment": False,
        "created_orders": False,
        "created_signals": False,
        "created_trade_decisions": False,
        "created_fills": False,
        "created_proposals": False,
        "promoted_model": False,
        "automatic_promotion_allowed": False,
        "paper_only": True,
        "automation_off": True,
        "manual_review": True,
        "operational_safety_mode": cs.get("safety_mode"),
        "safety_badges": list(SAFETY_BADGES),
    }

    # --- Assemble the state content, then hash it (generated_at excluded). ------ #
    seen: set[str] = set()
    uniq_warnings = [w for w in warnings if not (w in seen or seen.add(w))]

    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "phase": PHASE,
        "generated_at": _now_iso(),
        "state": state,
        "state_vocabulary": list(PORTFOLIO_STATES),
        "consistency": consistency,
        "active_book": active_book,
        "dates": dates,
        "capital": capital,
        "positions": positions,
        "orders": orders,
        "fills": fills_block,
        "target": target,
        "assessment": assessment,
        "evidence": evidence,
        "safety": safety,
        "warnings": uniq_warnings,
        "provenance": {
            "phase": PHASE,
            "portfolio_state_owner": "api.portfolio_state",
            "active_book_selection_owner": "api.operational_book",
            "legacy_archive_owner": "api.portfolio_valuation (explicitly scoped, never active)",
            "composed_read_models": [
                "operational_book.load_operational_book",
                "data_freshness.load_data_freshness (engine.market_session)",
                "paper_trading_desk.load_performance",
                "daily_action_gate.load_daily_action_gate",
                "forward_prediction_skill.load_prediction_skill",
                "paper_trading_desk.load_fills",
            ],
            "source_of_truth": {
                "nav": "api.paper_trading_desk.book_nav (ledger replay)",
                "cash": "api.paper_trading_desk.book_nav (ledger replay)",
                "holdings": "api.operational_book (ledger-replayed holdings)",
                "positions": "api.operational_book.build_holdings_detail",
                "cumulative_performance": "api.paper_trading_desk.load_performance",
                "operational_dates": "api.data_freshness (engine.market_session)",
                "active_book_selection": "api.operational_book",
                "assessment": "api.daily_action_gate",
                "forward_evidence": "api.forward_prediction_skill",
                "legacy_archive": "api.portfolio_valuation (dormant; never selected)",
            },
            "note": ("Read-only composition of the complete operational portfolio "
                     "state; every business calculation is delegated to its "
                     "authoritative owner and never recomputed. No provider / "
                     "prediction call, no Daily Close, no research refresh, no "
                     "reassessment, no write. The preliminary reassessment proposal "
                     "is review-only and unapproved."),
        },
    }

    # Deterministic content hashes (Workstream I): generated_at is NOT in the hash.
    result["source_hashes"] = {
        "operational_book": _stable_hash(operational),
        "data_freshness": _stable_hash(freshness or {}),
        "performance": _stable_hash(performance or {}),
        "assessment_gate": _stable_hash(gate or {}),
        "forward_evidence": _stable_hash(forward_status or {}),
        "fills": _stable_hash({"rows_count": fills_block["rows_count"],
                               "count": fills_block["count"]}),
    }
    result["state_hash"] = _stable_hash(result)
    return result


__all__ = [
    "PHASE", "SCHEMA_VERSION",
    "STATE_READY", "STATE_READY_WITH_PENDING_CLOSE", "STATE_DEGRADED",
    "STATE_INCONSISTENT", "STATE_NO_ACTIVE_BOOK", "STATE_UNAVAILABLE", "PORTFOLIO_STATES",
    "CONSISTENT", "DEGRADED", "INCONSISTENT", "UNAVAILABLE", "CONSISTENCY_VOCAB",
    "PENDING_DAILY_CLOSE", "EXPECTED_PRE_CLOSE_GAP",
    "PRELIMINARY_PROPOSAL_LABEL", "LEGACY_BOOK_ID",
    "load_portfolio_state",
]
