r"""api/shadow_portfolio_evidence.py - Release 56: the composition, freeze and
read owner of the FORWARD PAPER PORTFOLIO CHALLENGERS.

Release 46 competes SIGNALS forward. This owner competes PORTFOLIOS forward:
six complete books, frozen on the same decision session, scored on the same
calendar, against each other and against the incumbent operational book.

    r56_zero_base_research_v1     the Release-30 zero-base target
    r56_implementable_research_v1 the Release-30 implementable target
    r56_governed_score_top25_v1   equal-weight top-25 on the APPROVED model's
                                  own ranking (the governed ordering lane)
    r56_incumbent_book_v1         the live operational book (control)
    r56_all_cash_v1               100% cash (control)
    r56_benchmark_spy_v1          SPY (control)

WHY A SEPARATE STORE, AND WHY IT IS NOT A SECOND P&L OWNER
----------------------------------------------------------
Each challenger names the OWNER that values it, and this module re-derives
none of them: the equity books are priced by ``api.price_panel``, the incumbent
book by its own performance owner ``api.paper_trading_desk``, the benchmark by
the same desk's benchmark closes, and cash by the declared zero-return paper
policy. The arithmetic that turns those levels into forward P&L lives in ONE
pure kernel, ``engine.shadow_portfolio_evidence``.

Records are written to a Release-56 RESEARCH root that no operational owner
reads. Writes are first-write-wins per (challenger, inception session) and a
record is never rewritten - not to correct it, not to improve it. Freezing is an
explicit call; no GET in this module writes anything.

NO BACKFILL, EVER
-----------------
``freeze_challengers`` refuses any inception session that is not the eligible
session it was handed, and the kernel refuses to score a bar dated on or before
inception. A challenger frozen today therefore holds zero forward observations
today, and that is the correct answer rather than a defect.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from paper_trader.engine import shadow_portfolio_evidence as kernel

SCHEMA_VERSION = kernel.SCHEMA_VERSION
COMPOSITION_OWNER = "api.shadow_portfolio_evidence"
CALCULATION_OWNER = kernel.CALCULATION_OWNER
PHASE = "R56"
ROUTE = "/v1/research/shadow-portfolio-evidence"

SHADOW_DIR_ENV = "PAPER_TRADER_R56_SHADOW_DIR"
_DEFAULT_SHADOW_DIR = Path(r"D:\Stock_Prediction_app_data\r56_shadow_portfolios")

STATE_READY = "READY"
STATE_NOT_STARTED = "NOT_STARTED"
STATE_UNAVAILABLE = "UNAVAILABLE"
READ_STATE_VOCAB = (STATE_READY, STATE_NOT_STARTED, STATE_UNAVAILABLE)

FREEZE_CREATED = "FROZEN"
FREEZE_ALREADY = "ALREADY_FROZEN"
FREEZE_REFUSED = "REFUSED"
FREEZE_VOCAB = (FREEZE_CREATED, FREEZE_ALREADY, FREEZE_REFUSED)

#: The research scale. Deliberately the live book's own starting capital so a
#: dollar figure here is directly readable against the operational book, and
#: deliberately NOT the live NAV, so the scale carries no claim of its own.
STARTING_CAPITAL = 100000.0

CONTROL_ID = "r56_incumbent_book_v1"
BENCHMARK_ID = "r56_benchmark_spy_v1"
CASH_ID = "r56_all_cash_v1"

SAFETY_BADGES = list(kernel.SAFETY_BADGES)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _f(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def shadow_dir(shadow_dir_override=None) -> Path:
    return Path(shadow_dir_override or os.environ.get(SHADOW_DIR_ENV)
                or _DEFAULT_SHADOW_DIR)


def _records_dir(shadow_dir_override=None) -> Path:
    return shadow_dir(shadow_dir_override) / "records"


def _record_path(challenger_id: str, session: str, shadow_dir_override=None) -> Path:
    return _records_dir(shadow_dir_override) / ("%s__%s.json" % (challenger_id, session))


def load_records(shadow_dir_override=None) -> list:
    """Every frozen record on disk, oldest inception first. Read-only."""
    d = _records_dir(shadow_dir_override)
    out = []
    if not d.exists():
        return out
    for p in sorted(d.glob("*.json")):
        try:
            out.append(json.loads(p.read_text(encoding="utf-8")))
        except (OSError, ValueError):
            continue
    out.sort(key=lambda r: (str(r.get("inception_session") or ""),
                            str(r.get("challenger_id") or "")))
    return out


# --------------------------------------------------------------------------- #
# Challenger construction - what each book claims, and from whose evidence
# --------------------------------------------------------------------------- #
def governed_top25_weights(scoring: Optional[dict], *, entry_rank: int = 25) -> dict:
    """Equal weight over the APPROVED model's own top-N eligible names.

    This is the GOVERNED ordering lane expressed as a portfolio: it uses no
    forecast, no covariance and no utility - only the ranking the operational
    model actually publishes - so it isolates exactly how much of the book's
    result is the ranking and how much is everything built on top of it.
    """
    rows = []
    for r in (scoring or {}).get("rankings") or []:
        tk, rk = r.get("ticker"), r.get("rank")
        if not tk or rk is None or not r.get("eligible", True):
            continue
        rows.append((int(rk), str(tk)))
    rows.sort()
    picked = [tk for _rk, tk in rows[:int(entry_rank)]]
    if not picked:
        return {}
    w = round(1.0 / len(picked), 8)
    return {tk: w for tk in picked}


def build_specs(*, portfolio_state: dict, scoring: dict, zero_base: dict,
                capital_pool: Optional[dict] = None,
                entry_rank: int = 25) -> list:
    """The six challenger specifications for one decision session.

    Every weight vector is READ from an owner that already published it. None
    of them is invented here, and none of them is re-optimised.
    """
    from paper_trader.api import cash_deployment_frontier as cdf

    zb_w = cdf.weights_from_rows((zero_base.get("zero_base_target") or {}).get("rows"))
    impl_w = cdf.weights_from_rows(
        (zero_base.get("implementable_target") or {}).get("rows"))
    cur_w = {}
    for p in (portfolio_state or {}).get("positions") or []:
        tk, w = p.get("ticker"), _f(p.get("portfolio_weight"))
        if tk and w and w > 0:
            cur_w[str(tk)] = w
    gov_w = governed_top25_weights(scoring, entry_rank=entry_rank)

    pit = {
        "portfolio_state_hash": (portfolio_state or {}).get("state_hash"),
        "economic_state_hash": (portfolio_state or {}).get("economic_state_hash"),
        "universe_scoring_hash": (scoring or {}).get("output_hash"),
        "universe_scoring_model_id": (scoring or {}).get("primary_model_id"),
        "forecast_model_spec_hash": (zero_base or {}).get("forecast_model_spec_hash"),
        "feature_snapshot_hash": (zero_base or {}).get("feature_snapshot_hash"),
        "allocation_hash": (zero_base or {}).get("allocation_hash"),
        "capital_pool_nav": (capital_pool or {}).get("nav"),
        "capital_pool_cash": (capital_pool or {}).get("cash"),
    }
    cost_bps = 12.5
    try:
        from paper_trader.api import paper_trading_desk as desk
        cost_bps = float(desk.COST_BPS_PER_SIDE)
    except Exception:                                              # noqa: BLE001
        pass

    return [
        {"challenger_id": "r56_zero_base_research_v1",
         "label": "Zero-base target (research forecast)",
         "family": "ZERO_BASE_UTILITY_OPTIMISED",
         "weights": zb_w,
         "strategy_identity": {
             "target_owner": "api.zero_base_target",
             "objective_owner": "engine.zero_base_allocator",
             "lane": "RESEARCH_PREVIEW",
             "forecast": "api.return_forecast (NOT ACTIVATED)"},
         "valuation_source": kernel.VALUATION_PRICE_PANEL,
         "notes": ("the portfolio the Release-30 objective would hold if every "
                   "dollar were cash and incumbency counted for nothing")},
        {"challenger_id": "r56_implementable_research_v1",
         "label": "Implementable target (research forecast, costed)",
         "family": "TRANSITION_AWARE_UTILITY_OPTIMISED",
         "weights": impl_w,
         "strategy_identity": {
             "target_owner": "api.zero_base_target",
             "objective_owner": "engine.zero_base_allocator",
             "lane": "RESEARCH_PREVIEW",
             "forecast": "api.return_forecast (NOT ACTIVATED)"},
         "valuation_source": kernel.VALUATION_PRICE_PANEL,
         "notes": ("the same objective solved FROM the current book with the "
                   "canonical switching cost inside the economics")},
        {"challenger_id": "r56_governed_score_top25_v1",
         "label": "Governed score lane: equal-weight top 25",
         "family": "SCORE_RANK_EQUAL_WEIGHT",
         "weights": gov_w,
         "strategy_identity": {
             "ranking_owner": "api.universe_scoring",
             "model_id": (scoring or {}).get("primary_model_id"),
             "lane": "GOVERNED_SCORE_ELIGIBILITY",
             "construction": "EQUAL_WEIGHT_TOP_%d" % int(entry_rank)},
         "valuation_source": kernel.VALUATION_PRICE_PANEL,
         "notes": ("the approved model's ranking with nothing built on top of "
                   "it: isolates the ranking's own contribution")},
        {"challenger_id": CONTROL_ID,
         "label": "Incumbent operational book (control)",
         "family": "INCUMBENT_CONTROL",
         "weights": cur_w,
         "strategy_identity": {
             "book_id": ((portfolio_state or {}).get("active_book") or {}).get("book_id"),
             "nav_owner": "api.paper_trading_desk.book_nav",
             "lane": "OPERATIONAL"},
         "valuation_source": kernel.VALUATION_PRICE_PANEL,
         "notes": ("the live book's weights valued on the SAME panel as every "
                   "challenger, so the comparison is arithmetic rather than "
                   "coincidence. The book's authoritative NAV remains the "
                   "desk's.")},
        {"challenger_id": CASH_ID,
         "label": "All cash (control)",
         "family": "CASH_CONTROL",
         "weights": {},
         "strategy_identity": {"lane": "CONTROL",
                               "cash_policy": kernel.CASH_RETURN_POLICY},
         "valuation_source": kernel.VALUATION_CASH_POLICY,
         "notes": ("cash is a real asset choice and competes on the board "
                   "rather than being the residual of one")},
        {"challenger_id": BENCHMARK_ID,
         "label": "Passive SPY (control)",
         "family": "PASSIVE_BENCHMARK",
         "weights": {"SPY": 1.0},
         "strategy_identity": {"lane": "CONTROL", "benchmark_ticker": "SPY"},
         "valuation_source": kernel.VALUATION_DESK_PERFORMANCE,
         "notes": "the benchmark the operational book is already judged against"},
    ], pit, cost_bps


# --------------------------------------------------------------------------- #
# Freeze
# --------------------------------------------------------------------------- #
def freeze_challengers(*, eligible_market_date: str,
                       portfolio_state: dict, scoring: dict, zero_base: dict,
                       capital_pool: Optional[dict] = None,
                       entry_rank: int = 25,
                       shadow_dir_override=None,
                       inception_timestamp: Optional[str] = None) -> dict:
    """Freeze this session's forward paper portfolio challengers.

    EXPLICIT: no GET calls this. FIRST-WRITE-WINS: a record that already exists
    for (challenger, session) is returned untouched and is never rewritten.
    NO BACKFILL: the caller must hand the session it is actually freezing on;
    the record's forward evidence can only ever start after it.
    """
    if not eligible_market_date:
        return {"state": FREEZE_REFUSED, "reason": "NO_ELIGIBLE_MARKET_DATE",
                "results": []}
    specs, pit, cost_bps = build_specs(
        portfolio_state=portfolio_state, scoring=scoring, zero_base=zero_base,
        capital_pool=capital_pool, entry_rank=entry_rank)
    ts = inception_timestamp or _now_iso()
    d = _records_dir(shadow_dir_override)
    results = []
    for spec in specs:
        cid = spec["challenger_id"]
        path = _record_path(cid, eligible_market_date, shadow_dir_override)
        if path.exists():
            results.append({"challenger_id": cid, "state": FREEZE_ALREADY,
                            "path": str(path)})
            continue
        if not spec["weights"] and cid != CASH_ID:
            results.append({"challenger_id": cid, "state": FREEZE_REFUSED,
                            "reason": "NO_WEIGHTS_AVAILABLE_FROM_OWNER"})
            continue
        rec = kernel.make_inception_record(
            challenger_id=cid, label=spec["label"], family=spec["family"],
            strategy_identity=spec["strategy_identity"], weights=spec["weights"],
            inception_session=str(eligible_market_date), inception_timestamp=ts,
            starting_capital=STARTING_CAPITAL, pit_input_identity=pit,
            cost_bps_per_side=cost_bps, valuation_source=spec["valuation_source"],
            benchmark_id=BENCHMARK_ID, notes=spec.get("notes"))
        d.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(rec, indent=1, default=str), encoding="utf-8")
        tmp.replace(path)
        results.append({"challenger_id": cid, "state": FREEZE_CREATED,
                        "path": str(path), "record_hash": rec["record_hash"],
                        "position_count": rec["position_count"],
                        "cash_weight": rec["cash_weight"]})
    return {
        "state": FREEZE_CREATED if any(r["state"] == FREEZE_CREATED for r in results)
                 else FREEZE_ALREADY,
        "state_vocabulary": list(FREEZE_VOCAB),
        "inception_session": eligible_market_date,
        "inception_timestamp": ts,
        "shadow_dir": str(shadow_dir(shadow_dir_override)),
        "n_created": sum(1 for r in results if r["state"] == FREEZE_CREATED),
        "n_already": sum(1 for r in results if r["state"] == FREEZE_ALREADY),
        "n_refused": sum(1 for r in results if r["state"] == FREEZE_REFUSED),
        "results": results,
        "records_are_immutable": True,
        "backfill_allowed": False,
        "writes_operational_store": False,
        "safety_badges": list(SAFETY_BADGES),
    }


# --------------------------------------------------------------------------- #
# Read
# --------------------------------------------------------------------------- #
def _desk_curves() -> dict:
    """The benchmark close series and the operational book's own NAV series,
    from the ONE performance owner. Degrade-safe."""
    out = {"benchmark": None, "book": None, "source": None}
    try:
        from paper_trader.api import paper_trading_desk as desk
        perf = desk.load_performance()
        # The CURRENT ECONOMIC STATE, not the raw historical rows: a registered
        # corporate action must contribute exactly zero economic P&L here, and
        # the desk already owns that correction.
        rows = perf.get("current_rows") or perf.get("rows") or []
        bench = [(r.get("date"), _f(r.get("benchmark_close"))) for r in rows
                 if r.get("date") and r.get("benchmark_close") is not None]
        book = [(r.get("date"), _f(r.get("nav"))) for r in rows
                if r.get("date") and r.get("nav") is not None]
        out.update({"benchmark": bench or None, "book": book or None,
                    "source": kernel.VALUATION_DESK_PERFORMANCE,
                    "rows_basis": ("current_rows" if perf.get("current_rows")
                                   else "rows"),
                    "n_rows": len(rows)})
    except Exception as exc:                                       # noqa: BLE001
        out["error"] = type(exc).__name__
    return out


def load_shadow_portfolio_evidence(*, price_panel: Optional[dict] = None,
                                   as_of: Optional[str] = None,
                                   shadow_dir_override=None,
                                   desk_curves: Optional[dict] = None) -> dict:
    """The GET read model: every frozen challenger, accrued forward. Read-only."""
    try:
        records = load_records(shadow_dir_override)
    except Exception as exc:                                       # noqa: BLE001
        return _unavailable(type(exc).__name__)
    if not records:
        return {
            "schema_version": SCHEMA_VERSION, "composition_owner": COMPOSITION_OWNER,
            "calculation_owner": CALCULATION_OWNER, "phase": PHASE, "route": ROUTE,
            "state": STATE_NOT_STARTED, "state_vocabulary": list(READ_STATE_VOCAB),
            "generated_at": _now_iso(),
            "shadow_dir": str(shadow_dir(shadow_dir_override)),
            "headline": ("No forward paper portfolio challenger has been frozen "
                         "yet. Freezing is an explicit act and creates no "
                         "forward evidence at the moment it happens."),
            "challengers": [], "leaderboard": [], "n_challengers": 0,
            "safety": _safety(),
        }
    if price_panel is None:
        try:
            from paper_trader.api import price_panel as pp
            price_panel = pp.load_operational_price_panel()
        except Exception:                                          # noqa: BLE001
            price_panel = {"series": {}}
    series = (price_panel or {}).get("series") or {}
    curves = desk_curves if desk_curves is not None else _desk_curves()

    accrued = []
    for rec in records:
        src = rec.get("valuation_source")
        if src == kernel.VALUATION_DESK_PERFORMANCE:
            a = kernel.accrue_forward(record=rec, external_curve=curves.get("benchmark") or [],
                                      as_of=as_of)
        elif src == kernel.VALUATION_CASH_POLICY:
            a = _cash_accrual(rec, reference=accrued, series=series, as_of=as_of)
        else:
            a = kernel.accrue_forward(record=rec, price_series=series, as_of=as_of)
        a["notes"] = rec.get("notes")
        a["strategy_identity"] = rec.get("strategy_identity")
        a["pit_input_identity"] = rec.get("pit_input_identity")
        accrued.append(a)

    by_id: dict = {}
    for a in accrued:
        by_id.setdefault(a["challenger_id"], []).append(a)
    turnover = {cid: kernel.implied_turnover(
        [r for r in records if r.get("challenger_id") == cid])
        for cid in by_id}

    lb = kernel.leaderboard(accrued, control_id=CONTROL_ID)
    vs_bench = {}
    bench = next((a for a in accrued if a["challenger_id"] == BENCHMARK_ID), None)
    if bench:
        for a in accrued:
            if a["challenger_id"] != BENCHMARK_ID:
                vs_bench[a["challenger_id"]] = kernel.compare_on_common_window(a, bench)
    scored = [a for a in accrued if (a.get("sessions_scored") or 0) > 0]
    return {
        "schema_version": SCHEMA_VERSION,
        "composition_owner": COMPOSITION_OWNER,
        "calculation_owner": CALCULATION_OWNER,
        "phase": PHASE, "route": ROUTE,
        "state": STATE_READY, "state_vocabulary": list(READ_STATE_VOCAB),
        "generated_at": _now_iso(),
        "shadow_dir": str(shadow_dir(shadow_dir_override)),
        "as_of": as_of,
        "n_records": len(records),
        "n_challengers": len(by_id),
        "n_with_forward_evidence": len(scored),
        "inception_sessions": sorted({str(r.get("inception_session")) for r in records}),
        "starting_capital": STARTING_CAPITAL,
        "control_id": CONTROL_ID,
        "benchmark_id": BENCHMARK_ID,
        "cash_id": CASH_ID,
        "challengers": accrued,
        "leaderboard": lb,
        "vs_benchmark": vs_bench,
        "implied_turnover": turnover,
        "headline": _headline(scored, lb),
        "comparison_framing": {
            "question": ("which of these portfolios, CONSTRUCTED FROM CASH at "
                         "the inception session, earns more forward?"),
            "not_the_question": ("should the book SWITCH to one of them? That is "
                                 "a switching-cost question and it is answered by "
                                 "the payback horizon in "
                                 "api.cash_deployment_frontier, not here."),
            "cost_model": ("every challenger, including the incumbent control, "
                           "pays a one-off entry cost at the canonical per-side "
                           "rate as if it were bought at inception. Applied "
                           "uniformly it shifts each fully-invested book by "
                           "about the same 12.5bps and leaves the ranking "
                           "between them intact, while cash correctly pays "
                           "nothing."),
            "incumbent_control_note": ("the live book is entered here on the same "
                                       "terms as every challenger so the "
                                       "comparison is arithmetic rather than "
                                       "coincidence; its AUTHORITATIVE realised "
                                       "P&L remains the desk's, not this."),
        },
        "evidence_rules": {
            "scored_only_after_inception": True,
            "records_immutable": True,
            "backfilled": False,
            "rebalanced_after_inception": False,
            "min_priced_weight": kernel.MIN_PRICED_WEIGHT,
            "min_sessions_for_ratios": kernel.MIN_SESSIONS_FOR_RATIOS,
            "comparisons_use_equal_time_windows": True,
        },
        "valuation_owners": {
            "equity_books": kernel.VALUATION_PRICE_PANEL,
            "benchmark": kernel.VALUATION_DESK_PERFORMANCE,
            "cash": kernel.VALUATION_CASH_POLICY,
            "operational_book_authoritative_nav": "api.paper_trading_desk.book_nav",
        },
        "desk_curve_state": {"benchmark_rows": len(curves.get("benchmark") or []),
                             "book_rows": len(curves.get("book") or []),
                             "error": curves.get("error")},
        "safety": _safety(),
    }


def _cash_accrual(rec: dict, *, reference: list, series: dict,
                  as_of: Optional[str]) -> dict:
    """Cash accrues the declared zero paper return on the SAME calendar the
    priced books are scored on, so a comparison against cash is a comparison
    over equal time rather than over an empty one."""
    dates: set = set()
    for a in reference:
        for c in a.get("curve") or []:
            dates.add(c["date"])
    if not dates:
        t0 = str(rec.get("inception_session") or "")
        for s in series.values():
            for d in s.get("dates") or []:
                if d > t0 and (as_of is None or d <= as_of):
                    dates.add(d)
    start = float(rec.get("starting_capital") or 0.0)
    curve = [(d, start) for d in sorted(dates)]
    if curve:
        curve = [(str(rec.get("inception_session")), start)] + curve
    return kernel.accrue_forward(record=rec, external_curve=curve, as_of=as_of)


def _headline(scored: list, lb: list) -> str:
    if not scored:
        return ("Forward paper portfolio challengers are frozen and hold ZERO "
                "forward observations. That is the correct state on the day "
                "they were created, not a defect: evidence starts after "
                "inception.")
    top = lb[0] if lb else {}
    return ("%d of %d challengers have forward evidence. Leader on maturity "
            "then measured edge: %s (%s scored sessions, net %s)."
            % (len(scored), len(lb), top.get("label") or "-",
               top.get("sessions_scored"), top.get("net_cumulative_return")))


def _safety() -> dict:
    return {
        "badges": list(SAFETY_BADGES),
        "research_only": True, "shadow_only": True, "read_only": True,
        "paper_only": True, "manual_review_only": True,
        "creates_orders": False, "creates_fills": False,
        "creates_signals": False, "creates_trade_decisions": False,
        "mutates_holdings": False, "mutates_cash": False,
        "mutates_operational_book": False, "writes_operational_store": False,
        "promotes_model": False, "activates_sleeve": False,
        "enables_automation": False, "broker_enabled": False,
        "automatic_promotion_allowed": False,
        "promotion_owner": "a human, through the existing governance",
    }


def _unavailable(detail: str) -> dict:
    return {
        "schema_version": SCHEMA_VERSION, "composition_owner": COMPOSITION_OWNER,
        "calculation_owner": CALCULATION_OWNER, "phase": PHASE, "route": ROUTE,
        "state": STATE_UNAVAILABLE, "state_vocabulary": list(READ_STATE_VOCAB),
        "generated_at": _now_iso(),
        "blockers": [{"code": "SHADOW_PORTFOLIO_EVIDENCE_UNAVAILABLE",
                      "detail": detail}],
        "challengers": [], "leaderboard": [], "n_challengers": 0,
        "safety": _safety(),
    }


def summary(payload: Optional[dict] = None, **kwargs) -> dict:
    p = payload if payload is not None else load_shadow_portfolio_evidence(**kwargs)
    lb = p.get("leaderboard") or []
    # A leaderboard whose every row has zero scored sessions has no leader, and
    # naming its first row one would be an ordering artefact presented as a
    # result. The refusal is explicit so a surface cannot render it as a winner.
    scored = [r for r in lb if (r.get("sessions_scored") or 0) > 0]
    return {
        "state": p.get("state"),
        "n_challengers": p.get("n_challengers"),
        "n_with_forward_evidence": p.get("n_with_forward_evidence"),
        "inception_sessions": p.get("inception_sessions"),
        "headline": p.get("headline"),
        "leader": (scored[0] if scored else None),
        "leader_withheld_reason": (None if scored else
                                   "no challenger has a scored forward session yet"),
        "control_id": p.get("control_id"),
        "benchmark_id": p.get("benchmark_id"),
    }


__all__ = ["SCHEMA_VERSION", "COMPOSITION_OWNER", "CALCULATION_OWNER", "PHASE",
           "ROUTE", "SHADOW_DIR_ENV", "STARTING_CAPITAL", "CONTROL_ID",
           "BENCHMARK_ID", "CASH_ID", "STATE_READY", "STATE_NOT_STARTED",
           "STATE_UNAVAILABLE", "READ_STATE_VOCAB", "FREEZE_VOCAB",
           "FREEZE_CREATED", "FREEZE_ALREADY", "FREEZE_REFUSED",
           "SAFETY_BADGES", "shadow_dir", "load_records",
           "governed_top25_weights", "build_specs", "freeze_challengers",
           "load_shadow_portfolio_evidence", "summary"]
