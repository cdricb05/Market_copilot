"""
api/forward_evidence.py — Phase 28A: FORWARD EVIDENCE, ATTRIBUTION AND
SHADOW-BOOK COMPARISON (read-only, deterministic, idempotent, replayable).

This module turns the EXISTING append-only daily-close data of Alpha Paper Book #1
into a professional forward-evidence and attribution cockpit. It is strictly
DIAGNOSTIC and evidence-building only: it creates no orders, no signals and no
trade decisions; it never mutates the active book, its holdings, cash, model
selection or model weights; it writes nothing. Every derived number is computed
from data that already exists as of the relevant close, with no look-ahead.

It answers, for the operator:

    A. CANONICAL DAILY ATTRIBUTION — what moved today, per holding and per sector,
       reconciled to the NAV change, from the immutable desk marks.
    B. "WHY P&L MOVED" — a deterministic, fully traceable operator summary (no LLM,
       no opaque score); every sentence maps to a displayed number.
    C. ACTIVE vs SHADOW — the active book's FORWARD OPERATIONAL evidence next to the
       existing research/shadow books, which are HISTORICAL RECONSTRUCTION. The two
       evidence classes are labelled and NEVER silently mixed.
    D. ROLLING EVIDENCE — 5 / 20 / since-inception windows for the active book, with
       explicit availability before enough observations exist and no annualisation
       of a tiny sample without a clear warning.

Provenance is the central honesty rule. There is exactly ONE true forward
operational series — the desk's append-only ``forward_performance.json`` (real marks
accumulated day by day, never recomputed). Every "shadow" book (the Phase 18/19
tournament reconstruction and the six multi-horizon backtest books) is a HISTORICAL
RECONSTRUCTION and is tagged as such. We do not reconstruct unavailable operational
dates using hindsight, and we do not annualise or rank on an insufficient forward
sample.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from paper_trader.api import operational_book as ob
from paper_trader.api import paper_trading_desk as desk

PHASE = "28A"
BENCHMARK_TICKER = desk.BENCHMARK_TICKER  # "SPY"

# --------------------------------------------------------------------------- #
# Sample floors + tolerances (aligned with the Phase 27H forward monitor).
# --------------------------------------------------------------------------- #
# Risk-adjusted ratios / annualised vol are statistically meaningless on a handful
# of observations; they are withheld (or warned) below this many daily returns.
_FORWARD_MIN_RATIO_OBS = 20
_ROLL_WINDOWS = (5, 20)
# Per-position contributions must reconcile to the NAV move within this $ tolerance.
_ATTRIB_RECONCILE_TOL = 1.00
# Execution cost is "material" to cumulative P&L when its magnitude is at least this
# fraction of the absolute cumulative P&L.
_COST_MATERIAL_FRACTION = 0.25

# The daily-close decision journal (owned by api/daily_close.py) — read only, to
# label today's recorded decision and count proposed changes (turnover). Duplicated
# as local constants to avoid importing the heavy daily-close dependency chain.
_DAILY_CLOSE_JOURNAL_FILE = "daily_close_journal.json"
_DAILY_CLOSE_EVENT = "DAILY_CLOSE"

# --------------------------------------------------------------------------- #
# Canonical statuses.
# --------------------------------------------------------------------------- #
ATTRIB_READY = "ATTRIBUTION_READY"
ATTRIB_NO_PRIOR = "NO_PRIOR_OPERATIONAL_MARK"
ATTRIB_INSUFFICIENT = "INSUFFICIENT_MARKS"
ATTRIB_COVERAGE_INCOMPLETE = "COVERAGE_INCOMPLETE"
ATTRIB_DATE_NOT_FOUND = "MARKET_DATE_NOT_FOUND"

INSUFFICIENT_FORWARD_SAMPLE = "INSUFFICIENT_FORWARD_SAMPLE"
FORWARD_SAMPLE_SUFFICIENT = "FORWARD_SAMPLE_SUFFICIENT"

# Evidence classes (never mixed).
FORWARD_OPERATIONAL = "FORWARD_OPERATIONAL"
HISTORICAL_RECONSTRUCTION = "HISTORICAL_RECONSTRUCTION"

_INSUFFICIENT_MSG = "INSUFFICIENT FORWARD SAMPLE — NO MODEL CONCLUSION"


# --------------------------------------------------------------------------- #
# Small numeric helpers.
# --------------------------------------------------------------------------- #
def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _f(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        return float(str(x))
    except (TypeError, ValueError):
        return None


def _r2(x: Optional[float]) -> Optional[float]:
    return None if x is None else round(float(x), 2)


def _r4(x: Optional[float]) -> Optional[float]:
    return None if x is None else round(float(x), 4)


def _safety() -> dict:
    """The invariant safety contract for every forward-evidence payload."""
    return {
        "read_only": True,
        "performed_write": False,
        "paper_only": True,
        "diagnostic_only": True,
        "creates_orders": False,
        "creates_signals": False,
        "creates_trade_decisions": False,
        "mutates_holdings": False,
        "mutates_cash": False,
        "model_selection_changed": False,
        "model_weights_changed": False,
        "champion_replaced": False,
        "promotes_to_live": False,
        "broker_enabled": False,
        "automation_enabled": False,
        "prediction_service_used": False,
        "safety_badges": ["FORWARD EVIDENCE", "PAPER ONLY", "MANUAL REVIEW",
                          "NO ORDERS", "AUTOMATION OFF", "NO PROMOTION"],
    }


# --------------------------------------------------------------------------- #
# Injectable seams (tests swap these to run fully offline / hermetic).
# --------------------------------------------------------------------------- #
_PERF_LOADER: Callable = desk.load_performance
_MARKS_LOADER: Callable = desk.read_marks
_OPS_LOADER: Callable = ob.load_operational_book


def _perf_rows(desk_dir, perf_loader: Optional[Callable]) -> tuple[list[dict], dict]:
    """Sorted append-only forward-performance rows (NAV present) + the desk summary."""
    try:
        perf = (perf_loader or _PERF_LOADER)(desk_dir)
    except Exception:  # noqa: BLE001 — degrade to an empty forward series
        return [], {}
    rows = [r for r in (perf.get("rows") or []) if _f(r.get("nav")) is not None]
    rows.sort(key=lambda r: r.get("date") or "")
    return rows, (perf.get("summary") or {})


def _mark_series(desk_dir, marks_loader: Optional[Callable]) -> dict:
    try:
        return (marks_loader or _MARKS_LOADER)(desk_dir).get("series") or {}
    except Exception:  # noqa: BLE001
        return {}


def _holdings(ops: dict) -> list[dict]:
    """The current operational holdings detail (ticker/qty/sector/avg_cost/weight/
    cumulative unrealized P&L). The operational book holds a static set between
    monthly reviews, so current quantities apply to every forward mark to date."""
    cs = (ops or {}).get("canonical_state") or {}
    ob_book = (ops or {}).get("operational_book") or {}
    detail = cs.get("holdings_detail") or ob_book.get("holdings_detail") or []
    out: list[dict] = []
    for r in detail:
        tk = r.get("ticker")
        if not tk:
            continue
        out.append({
            "ticker": str(tk).upper(),
            "quantity": _f(r.get("quantity")),
            "sector": r.get("sector") or "Unknown",
            "average_cost": _f(r.get("average_cost")),
            "weight": _f(r.get("current_weight") if r.get("current_weight") is not None
                         else r.get("weight")),
            "cost_basis": _f(r.get("cost_basis")),
        })
    if not out:
        for tk, q in (ob_book.get("holdings") or {}).items():
            out.append({"ticker": str(tk).upper(), "quantity": _f(q),
                        "sector": "Unknown", "average_cost": None, "weight": None,
                        "cost_basis": None})
    return out


def _starting_capital(ops: dict) -> Optional[float]:
    ob_book = (ops or {}).get("operational_book") or {}
    return _f(ob_book.get("starting_capital") or ob_book.get("initial_capital"))


def _price_at(series: dict, ticker: str, as_of: Optional[str]) -> Optional[float]:
    if not as_of:
        return None
    hit = desk._series_price_at_or_before(series.get(ticker) or [], as_of)
    return hit[1] if hit else None


def _spy_daily_from_cum(cum_prev: Optional[float], cum_last: Optional[float]) -> Optional[float]:
    """Benchmark daily return (percent) from two cumulative-return-percent points."""
    if cum_prev is None or cum_last is None:
        return None
    return ((1.0 + cum_last / 100.0) / (1.0 + cum_prev / 100.0) - 1.0) * 100.0


def _daily_close_rows(desk_dir, book_id: str) -> list[dict]:
    """Read-only daily-close journal rows for the operational book (decision + turnover)."""
    try:
        sdir = desk._desk_dir(desk_dir)
        rows = [r for r in desk._read_ledger(sdir, _DAILY_CLOSE_JOURNAL_FILE)
                if r.get("event") == _DAILY_CLOSE_EVENT and r.get("book_id") == book_id]
        return sorted(rows, key=lambda r: (r.get("market_date") or "", r.get("seq") or 0))
    except Exception:  # noqa: BLE001
        return []


# --------------------------------------------------------------------------- #
# Part A — CANONICAL DAILY ATTRIBUTION (per processed operational close).
# --------------------------------------------------------------------------- #
def _pick_index(rows: list[dict], market_date: Optional[str]) -> Optional[int]:
    """Index of the target close (needs a prior row). Default = latest with a prior."""
    if len(rows) < 2:
        return None
    if market_date is None:
        return len(rows) - 1
    md = str(market_date)[:10]
    for i, r in enumerate(rows):
        if str(r.get("date") or "")[:10] == md:
            return i if i >= 1 else None
    return None


def _portfolio_block(rows: list[dict], i: int, starting_capital: Optional[float]) -> dict:
    last, prev = rows[i], rows[i - 1]
    navs = [_f(r.get("nav")) for r in rows[:i + 1]]
    nav1, nav0 = _f(last.get("nav")), _f(prev.get("nav"))
    sc = _f(starting_capital)
    daily_pnl = (nav1 - nav0) if (nav1 is not None and nav0 is not None) else None
    daily_ret = ((nav1 / nav0 - 1.0) * 100.0) if (nav0 and nav1 is not None) else None
    cum_pnl = (nav1 - sc) if (nav1 is not None and sc is not None) else None
    cum_ret = _f(last.get("cumulative_return_pct"))
    spy_cum1 = _f(last.get("benchmark_cumulative_return_pct"))
    spy_cum0 = _f(prev.get("benchmark_cumulative_return_pct"))
    spy_daily = _spy_daily_from_cum(spy_cum0, spy_cum1)
    daily_excess = (daily_ret - spy_daily) if (daily_ret is not None and spy_daily is not None) else None
    cum_excess = (cum_ret - spy_cum1) if (cum_ret is not None and spy_cum1 is not None) else None
    peak = max((v for v in navs if v is not None), default=None)
    return {
        "beginning_nav": _r2(nav0),
        "ending_nav": _r2(nav1),
        "daily_pnl": _r2(daily_pnl),
        "daily_return_pct": _r4(daily_ret),
        "cumulative_pnl": _r2(cum_pnl),
        "cumulative_return_pct": _r4(cum_ret),
        "spy_daily_return_pct": _r4(spy_daily),
        "spy_cumulative_return_pct": _r4(spy_cum1),
        "daily_excess_return_pct": _r4(daily_excess),
        "cumulative_excess_return_pct": _r4(cum_excess),
        "drawdown_pct": _r4(_f(last.get("drawdown_pct"))),
        "rolling_peak_nav": _r2(peak),
    }


def build_daily_attribution(*, market_date: Optional[str] = None, desk_dir=None,
                            ops: Optional[dict] = None, today: Optional[str] = None,
                            perf_loader: Optional[Callable] = None,
                            marks_loader: Optional[Callable] = None,
                            ops_loader: Optional[Callable] = None) -> dict:
    """Deterministic per-close attribution reconciled to the NAV move (Part A).

    Holding contribution = quantity x (completed close on the market date - completed
    close on the prior market date), from the append-only desk mark store. Nothing is
    invented: when the required prior marks do not exist the block reports the explicit
    availability/coverage status instead of a fabricated decomposition."""
    rows, summary = _perf_rows(desk_dir, perf_loader)
    base = {"phase": PHASE, "source": "operational_desk_marks",
            "calculation_method": (
                "position_contribution = quantity x (completed_close[market_date] - "
                "completed_close[prior_market_date]); sector_contribution = sum of "
                "positions by owned GICS sector; reconciled to the NAV change."),
            "generated_at": _now_iso()}
    if len(rows) < 2:
        status = ATTRIB_NO_PRIOR if len(rows) == 1 else ATTRIB_INSUFFICIENT
        return {**base, "status": status, "available": False,
                "market_date": (rows[-1].get("date") if rows else None),
                "prior_market_date": None,
                "reason": ("Daily attribution needs a prior completed operational mark and "
                           "today's mark. The baseline mark has no prior day."),
                "coverage": {"priced": 0, "total": 0, "missing_tickers": []}}
    i = _pick_index(rows, market_date)
    if i is None:
        return {**base, "status": ATTRIB_DATE_NOT_FOUND, "available": False,
                "market_date": market_date, "prior_market_date": None,
                "reason": ("No processed operational close with a prior mark matches the "
                           "requested market date."),
                "coverage": {"priced": 0, "total": 0, "missing_tickers": []}}

    last, prev = rows[i], rows[i - 1]
    d1 = str(last.get("date") or "")[:10] or None
    d0 = str(prev.get("date") or "")[:10] or None
    nav0 = _f(prev.get("nav"))

    holdings_ops = ops if ops is not None else _safe_ops(ops_loader, today)
    holds = _holdings(holdings_ops)
    series = _mark_series(desk_dir, marks_loader)

    positions: list[dict] = []
    missing: list[str] = []
    priced = 0
    for h in sorted(holds, key=lambda x: x["ticker"]):
        tk, qty = h["ticker"], h["quantity"]
        p1 = _price_at(series, tk, d1)
        p0 = _price_at(series, tk, d0)
        avg = h.get("average_cost")
        contrib = ret = pmv = cum_unrl = pp = None
        if qty is not None and p1 is not None and p0 is not None:
            contrib = qty * (p1 - p0)
            ret = ((p1 / p0 - 1.0) * 100.0) if p0 else None
            pmv = qty * p0
            pp = (contrib / nav0 * 100.0) if nav0 else None
            priced += 1
        else:
            missing.append(tk)
        if qty is not None and p1 is not None and avg is not None:
            cum_unrl = qty * (p1 - avg)
        positions.append({
            "ticker": tk, "sector": h["sector"], "quantity": qty,
            "prior_price": _r2(p0), "current_price": _r2(p1),
            "prior_market_value": _r2(pmv),
            "pnl_contribution": _r2(contrib),
            "daily_return_pct": _r4(ret),
            "contribution_pp": _r4(pp),
            "cumulative_unrealized_pnl": _r2(cum_unrl),
            "portfolio_weight": _r4(h.get("weight")),
        })

    portfolio = _portfolio_block(rows, i, _starting_capital(holdings_ops))
    total = len(positions)
    coverage = {"priced": priced, "total": total, "missing_tickers": sorted(missing),
                "complete": bool(total and priced == total)}
    if priced == 0:
        return {**base, "status": ATTRIB_COVERAGE_INCOMPLETE, "available": False,
                "market_date": d1, "prior_market_date": d0,
                "reason": ("Per-ticker completed marks for the two dates are not available, "
                           "so a position-level decomposition cannot be supported."),
                "portfolio": portfolio, "coverage": coverage}

    sum_contrib = sum(p["pnl_contribution"] for p in positions
                      if p["pnl_contribution"] is not None)
    market_move = (portfolio["ending_nav"] - portfolio["beginning_nav"]
                   if (portfolio["ending_nav"] is not None
                       and portfolio["beginning_nav"] is not None) else None)
    residual = (market_move - sum_contrib) if market_move is not None else None

    # sector aggregation (known contributions only)
    sec: dict[str, dict] = {}
    for p in positions:
        s = sec.setdefault(p["sector"], {"pnl": 0.0, "pp": 0.0, "weight": 0.0,
                                         "cum": 0.0, "n": 0, "has_cum": False})
        s["n"] += 1
        if p["portfolio_weight"] is not None:
            s["weight"] += p["portfolio_weight"]
        if p["pnl_contribution"] is not None:
            s["pnl"] += p["pnl_contribution"]
        if p["contribution_pp"] is not None:
            s["pp"] += p["contribution_pp"]
        if p["cumulative_unrealized_pnl"] is not None:
            s["cum"] += p["cumulative_unrealized_pnl"]
            s["has_cum"] = True
    sectors = [{"sector": s, "sector_weight_pct": _r4(v["weight"]),
                "pnl_contribution": _r2(v["pnl"]),
                "return_contribution_pp": _r4(v["pp"]),
                "cumulative_contribution": (_r2(v["cum"]) if v["has_cum"] else None),
                "n_holdings": v["n"]}
               for s, v in sec.items()]
    sectors.sort(key=lambda r: (r["pnl_contribution"] is None, -(r["pnl_contribution"] or 0.0)))

    ranked = sorted((p for p in positions if p["pnl_contribution"] is not None),
                    key=lambda p: p["pnl_contribution"], reverse=True)
    winners = ranked[:3]
    losers = [p for p in ranked[::-1] if p["pnl_contribution"] < 0][:3]

    status = ATTRIB_READY if coverage["complete"] else ATTRIB_COVERAGE_INCOMPLETE
    return {
        **base,
        "status": status,
        "available": True,
        "market_date": d1,
        "prior_market_date": d0,
        "portfolio": portfolio,
        "holdings": positions,
        "sectors": sectors,
        "winners": winners,
        "losers": losers,
        "coverage": coverage,
        "reconciliation": {
            "position_contribution_sum": _r2(sum_contrib),
            "market_movement": _r2(market_move),
            "residual": _r2(residual),
            "tolerance": _ATTRIB_RECONCILE_TOL,
            "reconciles": bool(residual is not None
                              and abs(residual) <= _ATTRIB_RECONCILE_TOL),
        },
        "cost_note": ("The modeled 12.5 bps/side paper execution cost is embedded once at "
                      "fill and carried in the baseline NAV; it is never re-charged on a "
                      "daily mark, so a daily mark's P&L is pure market movement."),
    }


def build_attribution_history(*, desk_dir=None, ops: Optional[dict] = None,
                              today: Optional[str] = None, limit: int = 60,
                              perf_loader: Optional[Callable] = None,
                              marks_loader: Optional[Callable] = None,
                              ops_loader: Optional[Callable] = None) -> dict:
    """Compact per-close attribution across every eligible processed close (Part A/G)."""
    rows, _summary = _perf_rows(desk_dir, perf_loader)
    holdings_ops = ops if ops is not None else _safe_ops(ops_loader, today)
    series = _mark_series(desk_dir, marks_loader)
    holds = _holdings(holdings_ops)
    sc = _starting_capital(holdings_ops)
    out: list[dict] = []
    for i in range(1, len(rows)):
        pb = _portfolio_block(rows, i, sc)
        d1 = str(rows[i].get("date") or "")[:10] or None
        d0 = str(rows[i - 1].get("date") or "")[:10] or None
        contribs = []
        for h in holds:
            p1 = _price_at(series, h["ticker"], d1)
            p0 = _price_at(series, h["ticker"], d0)
            if h["quantity"] is not None and p1 is not None and p0 is not None:
                contribs.append((h["ticker"], h["quantity"] * (p1 - p0)))
        contribs.sort(key=lambda t: t[1], reverse=True)
        sum_c = sum(c for _, c in contribs)
        mv = (pb["ending_nav"] - pb["beginning_nav"]
              if (pb["ending_nav"] is not None and pb["beginning_nav"] is not None) else None)
        resid = (mv - sum_c) if mv is not None else None
        out.append({
            "market_date": d1, "prior_market_date": d0,
            "daily_pnl": pb["daily_pnl"], "daily_return_pct": pb["daily_return_pct"],
            "spy_daily_return_pct": pb["spy_daily_return_pct"],
            "daily_excess_return_pct": pb["daily_excess_return_pct"],
            "cumulative_pnl": pb["cumulative_pnl"], "drawdown_pct": pb["drawdown_pct"],
            "top_positive": ({"ticker": contribs[0][0], "pnl_contribution": _r2(contribs[0][1])}
                             if contribs else None),
            "top_negative": ({"ticker": contribs[-1][0], "pnl_contribution": _r2(contribs[-1][1])}
                             if contribs and contribs[-1][1] < 0 else None),
            "reconciles": bool(resid is not None and abs(resid) <= _ATTRIB_RECONCILE_TOL),
        })
    return {"phase": PHASE, "status": "ATTRIBUTION_HISTORY_READY" if out else "NO_ATTRIBUTION_HISTORY",
            "count": len(out), "rows": out[-limit:][::-1], "source": "operational_desk_marks",
            "generated_at": _now_iso()}


# --------------------------------------------------------------------------- #
# Part B — DETERMINISTIC "WHY P&L MOVED" (no LLM, every statement traceable).
# --------------------------------------------------------------------------- #
def _money(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    return ("-$%s" % format(abs(x), ",.2f")) if x < 0 else "$%s" % format(x, ",.2f")


def _pct_txt(x: Optional[float]) -> str:
    return "n/a" if x is None else ("%+.2f%%" % x)


def build_why_pnl_moved(attribution: dict, *, perf_summary: Optional[dict] = None,
                        decision_row: Optional[dict] = None) -> dict:
    """Deterministic operator narrative built ONLY from the attribution numbers +
    the recorded daily-close decision + the stored cumulative execution cost. Every
    statement is traceable to a displayed value (Part B)."""
    if not attribution or not attribution.get("available"):
        return {"available": False,
                "narrative": ("Daily attribution is not available yet — a prior completed "
                              "operational mark and today's mark are required. No P&L "
                              "explanation can be produced without them."),
                "statements": [], "reason": (attribution or {}).get("reason")}
    p = attribution["portfolio"]
    winners = attribution.get("winners") or []
    losers = attribution.get("losers") or []
    sectors = attribution.get("sectors") or []
    daily_pnl = p.get("daily_pnl")
    daily_ret = p.get("daily_return_pct")
    spy_ret = p.get("spy_daily_return_pct")
    excess = p.get("daily_excess_return_pct")
    outperformed = bool(excess is not None and excess >= 0)

    # Execution-cost materiality vs cumulative P&L.
    total_cost = _f((perf_summary or {}).get("total_transaction_cost"))
    cum_pnl = p.get("cumulative_pnl")
    cost_material = None
    if total_cost is not None and cum_pnl is not None:
        cost_material = bool(cum_pnl == 0 or abs(total_cost) >= _COST_MATERIAL_FRACTION * abs(cum_pnl))

    # Did an existing daily-action trigger fire on the recorded decision?
    dec = (decision_row or {}).get("decision")
    pcount = int((decision_row or {}).get("proposed_change_count") or 0)
    trigger_fired = bool(dec == "REBALANCE_PROPOSAL_READY" or pcount > 0)

    strongest = sectors[0] if sectors else None
    weakest = sectors[-1] if sectors else None

    st: list[str] = []
    st.append("The book returned %s (%s) on %s."
              % (_pct_txt(daily_ret), _money(daily_pnl), attribution.get("market_date")))
    if spy_ret is not None:
        st.append("SPY returned %s, so the book %s SPY by %s."
                  % (_pct_txt(spy_ret), "OUTPERFORMED" if outperformed else "UNDERPERFORMED",
                     _pct_txt(abs(excess)) if excess is not None else "n/a"))
    if winners:
        st.append("Top contributors: %s." % ", ".join(
            "%s %s" % (w["ticker"], _money(w["pnl_contribution"])) for w in winners))
    if losers:
        st.append("Largest detractors: %s." % ", ".join(
            "%s %s" % (l["ticker"], _money(l["pnl_contribution"])) for l in losers))
    if strongest and strongest.get("pnl_contribution") is not None:
        st.append("Strongest sector: %s (%s). Weakest sector: %s (%s)."
                  % (strongest["sector"], _money(strongest["pnl_contribution"]),
                     weakest["sector"], _money(weakest.get("pnl_contribution"))))
    if cost_material is not None:
        st.append("Cumulative execution cost (%s) %s material relative to cumulative P&L (%s)."
                  % (_money(total_cost),
                     "remains" if cost_material else "is no longer",
                     _money(cum_pnl)))
    st.append("Recorded decision: %s — %s daily-action trigger fired."
              % (dec or "HOLD_CURRENT_PORTFOLIO", "a" if trigger_fired else "no"))

    return {
        "available": True,
        "market_date": attribution.get("market_date"),
        "prior_market_date": attribution.get("prior_market_date"),
        "absolute_result_pnl": daily_pnl,
        "absolute_result_return_pct": daily_ret,
        "spy_return_pct": spy_ret,
        "excess_return_pct": excess,
        "outperformed_spy": outperformed,
        "top_positive": winners,
        "top_negative": losers,
        "strongest_sector": strongest,
        "weakest_sector": weakest,
        "execution_cost_total": _r2(total_cost),
        "execution_cost_material": cost_material,
        "recorded_decision": dec or "HOLD_CURRENT_PORTFOLIO",
        "daily_action_trigger_fired": trigger_fired,
        "proposed_change_count": pcount,
        "narrative": " ".join(st),
        "statements": st,
        "generation": "DETERMINISTIC_NO_LLM",
    }


# --------------------------------------------------------------------------- #
# Part D — ROLLING EVIDENCE (5 / 20 / since-inception) for the active book.
# --------------------------------------------------------------------------- #
def _window_metrics(rows: list[dict], *, window: Optional[int], label: str,
                    turnover_avg: Optional[float], concentration_top5: Optional[float],
                    coverage: dict) -> dict:
    """Descriptive window stats. ``window`` is a number of DAILY RETURNS (None = since
    inception). Availability is explicit; annualised vol below the ratio floor carries a
    warning and no risk-adjusted ratio is emitted on a tiny sample."""
    navs = [_f(r.get("nav")) for r in rows if _f(r.get("nav")) is not None]
    n_returns_total = max(len(navs) - 1, 0)
    need = window if window is not None else 1
    available = n_returns_total >= need
    base = {"label": label, "window_returns": window, "available": available,
            "observations_available": n_returns_total, "observations_required": need}
    if not available:
        base["message"] = ("Rolling %s evidence is unavailable — %d of %d required daily "
                            "observations accumulated." % (label, n_returns_total, need))
        return base

    seg = navs if window is None else navs[-(window + 1):]
    rets = [seg[i] / seg[i - 1] - 1.0 for i in range(1, len(seg)) if seg[i - 1]]
    n = len(rets)
    spy_cum = [_f(r.get("benchmark_cumulative_return_pct")) for r in rows]
    spy_seg = spy_cum if window is None else spy_cum[-(window + 1):]
    spy_rets = []
    for j in range(1, len(spy_seg)):
        a, b = spy_seg[j - 1], spy_seg[j]
        spy_rets.append((1.0 + b / 100.0) / (1.0 + a / 100.0) - 1.0
                        if (a is not None and b is not None) else None)

    ret = (seg[-1] / seg[0] - 1.0) * 100.0 if seg[0] else None
    spy_ret = None
    if spy_seg and spy_seg[0] is not None and spy_seg[-1] is not None:
        spy_ret = (1.0 + spy_seg[-1] / 100.0) / (1.0 + spy_seg[0] / 100.0) * 100.0 - 100.0
    excess = (ret - spy_ret) if (ret is not None and spy_ret is not None) else None

    vol_ann = warn = None
    if n >= 2:
        m = sum(rets) / n
        sd = math.sqrt(sum((r - m) ** 2 for r in rets) / (n - 1))
        vol_ann = round(sd * math.sqrt(252) * 100.0, 4)
        if n < _FORWARD_MIN_RATIO_OBS:
            warn = ("Annualised from only %d daily observations (< %d) — indicative, not "
                    "a reliable volatility estimate." % (n, _FORWARD_MIN_RATIO_OBS))
    up = sum(1 for r in rets if r > 0)
    out_days = sum(1 for k in range(n) if k < len(spy_rets) and spy_rets[k] is not None
                   and rets[k] > spy_rets[k])
    peak, worst = seg[0], 0.0
    for v in seg:
        peak = max(peak, v)
        if peak:
            worst = min(worst, v / peak - 1.0)
    base.update({
        "n_daily_returns": n,
        "return_pct": _r4(ret),
        "spy_return_pct": _r4(spy_ret),
        "excess_return_pct": _r4(excess),
        "annualized_volatility_pct": vol_ann,
        "annualized_volatility_warning": warn,
        "max_drawdown_pct": round(worst * 100.0, 4),
        "hit_rate_pct": round(up / n * 100.0, 2) if n else None,
        "spy_outperformance_rate_pct": round(out_days / n * 100.0, 2) if n else None,
        "avg_daily_turnover": turnover_avg,
        "concentration_top5_pct": concentration_top5,
        "coverage": coverage,
    })
    return base


def build_rolling_evidence(*, desk_dir=None, ops: Optional[dict] = None,
                           today: Optional[str] = None,
                           perf_loader: Optional[Callable] = None,
                           marks_loader: Optional[Callable] = None,
                           ops_loader: Optional[Callable] = None) -> dict:
    """Rolling 5 / 20 / since-inception windows for the active operational book (Part D)."""
    rows, _summary = _perf_rows(desk_dir, perf_loader)
    holdings_ops = ops if ops is not None else _safe_ops(ops_loader, today)
    holds = _holdings(holdings_ops)
    book_id = ((holdings_ops.get("operational_book") or {}).get("book_id")
               or ob.OPERATIONAL_BOOK_ID)

    # top-5 concentration from current weights
    weights = sorted((h["weight"] for h in holds if h["weight"] is not None), reverse=True)
    concentration = round(sum(weights[:5]) * 100.0, 4) if weights else None

    # coverage on the latest mark date
    series = _mark_series(desk_dir, marks_loader)
    latest = rows[-1].get("date") if rows else None
    priced = sum(1 for h in holds if _price_at(series, h["ticker"], latest) is not None) if latest else 0
    coverage = {"priced": priced, "total": len(holds),
                "complete": bool(holds and priced == len(holds))}

    # average daily turnover from the recorded proposed-change counts
    dec_rows = _daily_close_rows(desk_dir, book_id)
    changes = [int(r.get("proposed_change_count") or 0) for r in dec_rows]
    turnover_avg = round(sum(changes) / len(changes), 4) if changes else None

    windows = []
    for w in _ROLL_WINDOWS:
        windows.append(_window_metrics(rows, window=w, label="%d closes" % w,
                                       turnover_avg=turnover_avg, concentration_top5=concentration,
                                       coverage=coverage))
    inception = _window_metrics(rows, window=None, label="since inception",
                                turnover_avg=turnover_avg, concentration_top5=concentration,
                                coverage=coverage)
    n_returns = max(len(rows) - 1, 0)
    return {
        "phase": PHASE,
        "status": "ROLLING_EVIDENCE_READY" if rows else "NO_FORWARD_MARKS",
        "n_marks": len(rows),
        "n_daily_returns": n_returns,
        "sample_status": (FORWARD_SAMPLE_SUFFICIENT if n_returns >= _FORWARD_MIN_RATIO_OBS
                          else INSUFFICIENT_FORWARD_SAMPLE),
        "min_ratio_observations": _FORWARD_MIN_RATIO_OBS,
        "windows": windows,
        "since_inception": inception,
        "note": ("Rolling evidence is descriptive monitoring of the live paper book only. "
                 "Windows are unavailable until enough eligible closes accumulate, and vol "
                 "annualised from a short sample carries an explicit warning."),
        "generated_at": _now_iso(),
    }


# --------------------------------------------------------------------------- #
# Part C — ACTIVE vs SHADOW BOOKS (FORWARD OPERATIONAL vs HISTORICAL RECON).
# --------------------------------------------------------------------------- #
def _active_forward_summary(rows: list[dict], summary: dict, *,
                            turnover_avg: Optional[float], concentration: Optional[float],
                            coverage: dict) -> dict:
    """The ACTIVE book's genuine forward operational evidence (real accumulated marks)."""
    navs = [_f(r.get("nav")) for r in rows if _f(r.get("nav")) is not None]
    n_returns = max(len(navs) - 1, 0)
    inception = _window_metrics(rows, window=None, label="since inception",
                                turnover_avg=turnover_avg, concentration_top5=concentration,
                                coverage=coverage)
    sufficient = n_returns >= _FORWARD_MIN_RATIO_OBS
    return {
        "book_id": ob.OPERATIONAL_BOOK_ID,
        "book_label": ob.OPERATIONAL_BOOK_LABEL,
        "evidence_type": FORWARD_OPERATIONAL,
        "observations": n_returns,
        "n_marks": len(rows),
        "start_date": rows[0].get("date") if rows else None,
        "end_date": rows[-1].get("date") if rows else None,
        "cumulative_return_pct": inception.get("return_pct"),
        "cumulative_excess_return_pct": inception.get("excess_return_pct"),
        "annualized_volatility_pct": inception.get("annualized_volatility_pct"),
        "annualized_volatility_warning": inception.get("annualized_volatility_warning"),
        "max_drawdown_pct": inception.get("max_drawdown_pct"),
        "positive_day_rate_pct": inception.get("hit_rate_pct"),
        "spy_outperformance_rate_pct": inception.get("spy_outperformance_rate_pct"),
        "avg_daily_turnover": turnover_avg,
        "estimated_cost": _r4(_f(summary.get("total_transaction_cost"))),
        "concentration_top5_pct": concentration,
        "coverage": coverage,
        "sample_status": FORWARD_SAMPLE_SUFFICIENT if sufficient else INSUFFICIENT_FORWARD_SAMPLE,
        "insufficient_message": None if sufficient else _INSUFFICIENT_MSG,
    }


def _shadow_from_tournament(bv: dict, name: str) -> dict:
    """Normalise one Phase 18/19 tournament book view into a shadow row."""
    return {
        "book": name,
        "signal": bv.get("signal"),
        "evidence_type": HISTORICAL_RECONSTRUCTION,
        "reconstruction_basis": "phase18_19_tournament_owned_eodhd_daily_strip",
        "cadence": "daily_reconstruction",
        "date_comparable_to_forward": False,
        "observations": bv.get("n_marks"),
        "start_date": bv.get("start_date"),
        "end_date": bv.get("end_date"),
        "cumulative_return_pct": bv.get("cumulative_return_pct"),
        "cumulative_excess_return_pct": bv.get("excess_return_vs_spy_pct_points"),
        "annualized_volatility_pct": None,
        "daily_volatility_pct_points": bv.get("daily_volatility_pct_points"),
        "max_drawdown_pct": bv.get("max_drawdown_pct"),
        "positive_day_rate_pct": bv.get("positive_day_rate_pct"),
        "spy_outperformance_rate_pct": bv.get("days_outperforming_spy_pct"),
        "concentration_top5_pct": bv.get("contributor_concentration_top5_pct"),
        "coverage_pct": bv.get("coverage_pct"),
    }


def _shadow_from_multi_history(name: str, book: dict) -> dict:
    """Normalise one multi-horizon backtest book (system C) into a shadow row.
    These are MONTH/QUARTER backtests — returns are fractions; NOT date-comparable
    to the daily forward marks."""
    m = book.get("metrics") or {}

    def _pc(x):
        return round(x * 100.0, 4) if isinstance(x, (int, float)) else None
    return {
        "book": name,
        "signal": name,
        "evidence_type": HISTORICAL_RECONSTRUCTION,
        "reconstruction_basis": "multi_horizon_backtest_month_end_panels",
        "cadence": book.get("cadence"),
        "date_comparable_to_forward": False,
        "observations": m.get("n_periods"),
        "start_date": m.get("first_month"),
        "end_date": m.get("last_month"),
        "cumulative_return_pct": _pc(m.get("net_cumulative_return")),
        "annualized_return_pct": _pc(m.get("annualized_net_return")),
        "annualized_volatility_pct": _pc(m.get("annualized_vol")),
        "sharpe": m.get("sharpe"),
        "max_drawdown_pct": _pc(m.get("max_drawdown")),
        "positive_period_rate_pct": _pc(m.get("hit_rate")),
        "mean_turnover_pct": _pc(m.get("mean_turnover")),
        "sufficient_history": m.get("sufficient_history"),
    }


_SIX_BOOK_NAMES = (
    "composite_sn_top25", "composite_sn_top50",
    "mom_6_1_top25", "mom_6_1_top50",
    "fundamental_momentum_50_50_top25", "fundamental_momentum_50_50_top50",
)


def _default_tournament() -> dict:
    from paper_trader.api.current_alpha_tournament import load_current_alpha_tournament
    return load_current_alpha_tournament()


def _default_multi_history() -> dict:
    from paper_trader.api import multi_horizon_history as mhh
    return mhh.build_history()


def build_active_vs_shadow(*, desk_dir=None, ops: Optional[dict] = None,
                           today: Optional[str] = None,
                           perf_loader: Optional[Callable] = None,
                           marks_loader: Optional[Callable] = None,
                           ops_loader: Optional[Callable] = None,
                           tournament_loader: Optional[Callable] = None,
                           multi_history_loader: Optional[Callable] = None) -> dict:
    """Active book (FORWARD OPERATIONAL) beside the existing shadow books (HISTORICAL
    RECONSTRUCTION), clearly separated and never mixed (Part C). The active book is
    never changed by anything computed here."""
    rows, summary = _perf_rows(desk_dir, perf_loader)
    holdings_ops = ops if ops is not None else _safe_ops(ops_loader, today)
    holds = _holdings(holdings_ops)
    book_id = ((holdings_ops.get("operational_book") or {}).get("book_id")
               or ob.OPERATIONAL_BOOK_ID)

    weights = sorted((h["weight"] for h in holds if h["weight"] is not None), reverse=True)
    concentration = round(sum(weights[:5]) * 100.0, 4) if weights else None
    series = _mark_series(desk_dir, marks_loader)
    latest = rows[-1].get("date") if rows else None
    priced = sum(1 for h in holds if _price_at(series, h["ticker"], latest) is not None) if latest else 0
    coverage = {"priced": priced, "total": len(holds),
                "complete": bool(holds and priced == len(holds))}
    dec_rows = _daily_close_rows(desk_dir, book_id)
    changes = [int(r.get("proposed_change_count") or 0) for r in dec_rows]
    turnover_avg = round(sum(changes) / len(changes), 4) if changes else None

    active = _active_forward_summary(rows, summary, turnover_avg=turnover_avg,
                                     concentration=concentration, coverage=coverage)

    shadow: list[dict] = []
    warnings: list[str] = []
    # System B — Phase 18/19 tournament reconstruction (degrade-safe).
    try:
        t = (tournament_loader or _default_tournament)()
        summaries = t.get("book_summaries") or {}
        for key in ("champion_top25", "champion_top50", "challenger_top25", "challenger_top50"):
            bv = summaries.get(key)
            if bv:
                shadow.append(_shadow_from_tournament(bv, key))
        if not summaries:
            warnings.append("Tournament reconstruction unavailable (%s)."
                            % (t.get("status") or "no artifact"))
    except Exception as exc:  # noqa: BLE001
        warnings.append("Tournament shadow books unavailable: %s" % str(exc)[:140])
    # System C — six multi-horizon backtest books (degrade-safe).
    try:
        h = (multi_history_loader or _default_multi_history)()
        books = h.get("books") or {}
        for name in _SIX_BOOK_NAMES:
            if name in books:
                shadow.append(_shadow_from_multi_history(name, books[name]))
        if not books:
            warnings.append("Multi-horizon backtest books unavailable (%s)."
                            % (h.get("status") or "no panels"))
    except Exception as exc:  # noqa: BLE001
        warnings.append("Multi-horizon shadow books unavailable: %s" % str(exc)[:140])

    # A genuine like-for-like FORWARD comparison requires real forward marks on
    # BOTH sides over identical dates. Only the active book has true forward marks;
    # every shadow book is a reconstruction. We say so explicitly rather than
    # aligning a backtest onto the forward calendar (which would be hindsight).
    forward_overlap = {
        "status": INSUFFICIENT_FORWARD_SAMPLE,
        "common_forward_dates": 0,
        "message": ("No shadow book has an independent FORWARD operational mark series; "
                    "each is a historical reconstruction. A like-for-like forward "
                    "comparison over identical eligible dates will become possible only "
                    "as the active book accumulates a sufficient forward sample."),
    }

    return {
        "phase": PHASE,
        "status": "SHADOW_COMPARISON_READY",
        "active_book": active,
        "shadow_books": shadow,
        "forward_operational_overlap": forward_overlap,
        "evidence_classes": {
            FORWARD_OPERATIONAL: ("Real marks accumulated day by day in the append-only "
                                  "desk ledger; never recomputed."),
            HISTORICAL_RECONSTRUCTION: ("Backtest / reconstruction series rebuilt from owned "
                                        "data; NOT forward operational observations. Shown as "
                                        "research context only — the active book is never "
                                        "changed by these comparisons."),
        },
        "separation_note": ("FORWARD OPERATIONAL EVIDENCE and HISTORICAL RECONSTRUCTION are "
                            "reported separately and never silently mixed. Shadow rows are "
                            "research-only and promote nothing."),
        "research_only": True,
        "operational_recommendation": "NO_OPERATIONAL_CHANGE",
        "active_book_unchanged": True,
        "warnings": warnings,
        "generated_at": _now_iso(),
    }


# --------------------------------------------------------------------------- #
# Degrade-safe ops loader.
# --------------------------------------------------------------------------- #
def _safe_ops(ops_loader: Optional[Callable], today: Optional[str]) -> dict:
    try:
        return (ops_loader or _OPS_LOADER)(today)
    except Exception:  # noqa: BLE001
        return {}


# --------------------------------------------------------------------------- #
# Public — the ONE aggregator + the granular read-only entry points (Part G).
# --------------------------------------------------------------------------- #
def _todays_review(attribution: dict, why: dict, decision_row: Optional[dict]) -> dict:
    """The compact Portfolio 'Today's Review' summary."""
    if not attribution.get("available"):
        return {"available": False, "status": attribution.get("status"),
                "message": attribution.get("reason"),
                "market_date": attribution.get("market_date"),
                "prior_market_date": attribution.get("prior_market_date")}
    p = attribution["portfolio"]
    winners = attribution.get("winners") or []
    losers = attribution.get("losers") or []
    return {
        "available": True,
        "market_date": attribution.get("market_date"),
        "prior_market_date": attribution.get("prior_market_date"),
        "decision": (decision_row or {}).get("decision") or "HOLD_CURRENT_PORTFOLIO",
        "daily_pnl": p.get("daily_pnl"),
        "daily_return_pct": p.get("daily_return_pct"),
        "spy_daily_return_pct": p.get("spy_daily_return_pct"),
        "daily_excess_return_pct": p.get("daily_excess_return_pct"),
        "cumulative_pnl": p.get("cumulative_pnl"),
        "drawdown_pct": p.get("drawdown_pct"),
        "outperformed_spy": why.get("outperformed_spy"),
        "top_positive": (winners[0] if winners else None),
        "top_negative": (losers[0] if losers else None),
        "coverage": attribution.get("coverage"),
    }


def load_forward_evidence(*, today: Optional[str] = None, desk_dir=None,
                          ops: Optional[dict] = None,
                          perf_loader: Optional[Callable] = None,
                          marks_loader: Optional[Callable] = None,
                          ops_loader: Optional[Callable] = None,
                          tournament_loader: Optional[Callable] = None,
                          multi_history_loader: Optional[Callable] = None) -> dict:
    """The ONE read-only forward-evidence payload (Portfolio + Research & Audit).

    Returns today's review, the canonical attribution, the deterministic Why-P&L-Moved
    narrative, rolling evidence and the active-vs-shadow comparison — every value
    derived only from stored data, no look-ahead, nothing written."""
    holdings_ops = ops if ops is not None else _safe_ops(ops_loader, today)
    book_id = ((holdings_ops.get("operational_book") or {}).get("book_id")
               or ob.OPERATIONAL_BOOK_ID)
    _rows, summary = _perf_rows(desk_dir, perf_loader)
    dec_rows = _daily_close_rows(desk_dir, book_id)
    latest_decision = dec_rows[-1] if dec_rows else None

    attribution = build_daily_attribution(
        desk_dir=desk_dir, ops=holdings_ops, today=today,
        perf_loader=perf_loader, marks_loader=marks_loader)
    why = build_why_pnl_moved(attribution, perf_summary=summary, decision_row=latest_decision)
    rolling = build_rolling_evidence(
        desk_dir=desk_dir, ops=holdings_ops, today=today,
        perf_loader=perf_loader, marks_loader=marks_loader)
    shadow = build_active_vs_shadow(
        desk_dir=desk_dir, ops=holdings_ops, today=today,
        perf_loader=perf_loader, marks_loader=marks_loader,
        tournament_loader=tournament_loader, multi_history_loader=multi_history_loader)

    # Phase 28B — concise TRUE_FORWARD prediction-skill summary (read-only; the
    # full detail lives at /v1/evidence/prediction-skill). Degrade-safe.
    try:
        from paper_trader.api import forward_prediction_skill as _fps
        skill_summary = _fps.prediction_skill_summary(desk_dir=desk_dir)
    except Exception:  # noqa: BLE001
        skill_summary = None

    return {
        "status": "FORWARD_EVIDENCE_READY",
        "phase": PHASE,
        "operational_book_id": book_id,
        "operational_book_label": ob.OPERATIONAL_BOOK_LABEL,
        "todays_review": _todays_review(attribution, why, latest_decision),
        "attribution": attribution,
        "why_pnl_moved": why,
        "rolling_evidence": rolling,
        "active_vs_shadow": shadow,
        "prediction_skill": skill_summary,
        "generated_at": _now_iso(),
        **_safety(),
    }


def load_daily_attribution(*, market_date: Optional[str] = None, today: Optional[str] = None,
                           desk_dir=None) -> dict:
    attribution = build_daily_attribution(market_date=market_date, today=today, desk_dir=desk_dir)
    return {"status": "DAILY_ATTRIBUTION_READY", "phase": PHASE,
            "attribution": attribution, "generated_at": _now_iso(), **_safety()}


def load_attribution_history(*, today: Optional[str] = None, limit: int = 60, desk_dir=None) -> dict:
    hist = build_attribution_history(today=today, limit=limit, desk_dir=desk_dir)
    return {**hist, **_safety()}


def load_holding_contributions(*, market_date: Optional[str] = None, today: Optional[str] = None,
                               desk_dir=None) -> dict:
    a = build_daily_attribution(market_date=market_date, today=today, desk_dir=desk_dir)
    return {"status": "HOLDING_CONTRIBUTIONS_READY" if a.get("available")
            else a.get("status"), "phase": PHASE,
            "market_date": a.get("market_date"), "prior_market_date": a.get("prior_market_date"),
            "available": bool(a.get("available")),
            "coverage": a.get("coverage"),
            "reconciliation": a.get("reconciliation"),
            "holdings": a.get("holdings") or [], "generated_at": _now_iso(), **_safety()}


def load_sector_contributions(*, market_date: Optional[str] = None, today: Optional[str] = None,
                              desk_dir=None) -> dict:
    a = build_daily_attribution(market_date=market_date, today=today, desk_dir=desk_dir)
    return {"status": "SECTOR_CONTRIBUTIONS_READY" if a.get("available")
            else a.get("status"), "phase": PHASE,
            "market_date": a.get("market_date"), "prior_market_date": a.get("prior_market_date"),
            "available": bool(a.get("available")),
            "sectors": a.get("sectors") or [], "generated_at": _now_iso(), **_safety()}


def load_rolling_evidence(*, today: Optional[str] = None, desk_dir=None) -> dict:
    r = build_rolling_evidence(today=today, desk_dir=desk_dir)
    return {**r, **_safety()}


def load_shadow_comparison(*, today: Optional[str] = None, desk_dir=None) -> dict:
    s = build_active_vs_shadow(today=today, desk_dir=desk_dir)
    return {**s, **_safety()}


__all__ = [
    "PHASE", "FORWARD_OPERATIONAL", "HISTORICAL_RECONSTRUCTION",
    "INSUFFICIENT_FORWARD_SAMPLE", "FORWARD_SAMPLE_SUFFICIENT",
    "ATTRIB_READY", "ATTRIB_NO_PRIOR", "ATTRIB_INSUFFICIENT",
    "ATTRIB_COVERAGE_INCOMPLETE", "ATTRIB_DATE_NOT_FOUND",
    "build_daily_attribution", "build_attribution_history", "build_why_pnl_moved",
    "build_rolling_evidence", "build_active_vs_shadow", "load_forward_evidence",
    "load_daily_attribution", "load_attribution_history", "load_holding_contributions",
    "load_sector_contributions", "load_rolling_evidence", "load_shadow_comparison",
]
