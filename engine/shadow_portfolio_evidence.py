r"""engine/shadow_portfolio_evidence.py - Release 56: the PURE kernel for
FORWARD PAPER PORTFOLIO CHALLENGERS.

Release 46 already runs a forward tournament between SIGNALS. This kernel runs
one between PORTFOLIOS: a complete weight vector plus its cash, frozen at a real
decision timestamp, competing against the incumbent operational book, against
cash and against the benchmark on identical windows.

THE ONE RULE THIS FILE EXISTS TO ENFORCE
----------------------------------------
A shadow portfolio's forward evidence starts when the portfolio was frozen, and
never one session earlier. Every function here refuses to read a bar dated on or
before the inception session, so there is no code path through which a
challenger can be scored on a return that already existed when it was created.
If forward evidence starts today, it starts today.

WHAT IT DOES NOT DO
-------------------
It does not rebalance. A frozen record is a buy-and-hold claim, because a
rebalance is a DECISION and no decision was taken after inception; inventing one
would silently give the challenger information it never had. Successive frozen
records of the same challenger are compared by
:func:`implied_turnover`, which is the honest measure of how much trading the
strategy would really have demanded.

It does not manufacture prices. A session where a material share of the
portfolio has no bar is reported as insufficiently covered and is NOT scored,
rather than being carried flat - a flat mark is a claim that the position did
not move, and we do not know that.

Pure: no I/O, no clock, no randomness, no network, no database. It creates no
order, no fill, no signal, no decision and no operational NAV, and it changes no
holding, no cash and nothing in the operational book.
"""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Optional

CALCULATION_OWNER = "engine.shadow_portfolio_evidence"
SCHEMA_VERSION = "shadow_portfolio_evidence.v1"
RECORD_SCHEMA_VERSION = "shadow_portfolio_record.v1"
PHASE = "R56"

TRADING_DAYS_YEAR = 252

#: Cash is modelled exactly as the operational allocator models it. A shadow
#: book that paid itself a risk-free rate the operational book does not earn
#: would beat it on arithmetic rather than on skill.
CASH_RETURN = 0.0
CASH_RETURN_POLICY = "ZERO_RETURN_PAPER_ASSUMPTION"

#: A session is scored only if at least this share of the invested weight has a
#: real bar. Below it the session is reported UNCOVERED and skipped.
MIN_PRICED_WEIGHT = 0.95

EVIDENCE_NOT_STARTED = "FORWARD_EVIDENCE_NOT_STARTED"
EVIDENCE_ACCRUING = "FORWARD_EVIDENCE_ACCRUING"
EVIDENCE_SUFFICIENT_FOR_RATIOS = "FORWARD_EVIDENCE_SUFFICIENT_FOR_RATIOS"
EVIDENCE_VOCAB = (EVIDENCE_NOT_STARTED, EVIDENCE_ACCRUING,
                  EVIDENCE_SUFFICIENT_FOR_RATIOS)

#: Below this many scored sessions a Sharpe ratio is noise wearing a decimal
#: point, so it is withheld rather than printed.
MIN_SESSIONS_FOR_RATIOS = 20

VALUATION_PRICE_PANEL = "api.price_panel.load_operational_price_panel"
VALUATION_DESK_PERFORMANCE = "api.paper_trading_desk.load_performance"
VALUATION_CASH_POLICY = "engine.shadow_portfolio_evidence.CASH_RETURN"

SAFETY_BADGES = ["RESEARCH ONLY", "SHADOW ONLY", "READ ONLY", "PAPER ONLY",
                 "NO ORDERS", "NO LIVE BROKER ORDERS", "NO BROKER",
                 "AUTOMATION OFF", "MANUAL REVIEW", "NO MODEL PROMOTION"]


def _f(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _r(x: Optional[float], nd: int) -> Optional[float]:
    return None if x is None else round(float(x), nd)


def _money(x: Optional[float]) -> Optional[float]:
    return None if x is None else round(float(x), 2)


def stable_hash(obj: Any) -> str:
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, default=str).encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- #
# Inception - the immutable record
# --------------------------------------------------------------------------- #
def make_inception_record(*, challenger_id: str, label: str, family: str,
                          strategy_identity: dict, weights: dict,
                          inception_session: str, inception_timestamp: str,
                          starting_capital: float,
                          pit_input_identity: dict,
                          cost_bps_per_side: float,
                          valuation_source: str,
                          benchmark_id: Optional[str] = None,
                          notes: Optional[str] = None) -> dict:
    """Build ONE immutable forward-paper-portfolio record.

    The record carries everything needed to score it later WITHOUT consulting
    anything that did not exist at ``inception_timestamp``: the weights, the
    cash, the point-in-time identity of every input, the cost model and the
    named valuation owner. Its hash covers exactly that body, so a record that
    was edited afterwards cannot pass as the record that was frozen.
    """
    w = {}
    for k, v in (weights or {}).items():
        fv = _f(v)
        if fv is not None and fv > 1e-9:
            w[str(k)] = round(fv, 8)
    invested = round(sum(w.values()), 8)
    cash_w = round(max(0.0, 1.0 - invested), 8)
    rate = float(cost_bps_per_side or 0.0) / 10000.0
    body = {
        "record_schema_version": RECORD_SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "phase": PHASE,
        "challenger_id": str(challenger_id),
        "label": label,
        "family": family,
        "strategy_identity": dict(strategy_identity or {}),
        "inception_session": str(inception_session),
        "inception_timestamp": str(inception_timestamp),
        "weights": dict(sorted(w.items())),
        "position_count": len(w),
        "invested_weight": invested,
        "cash_weight": cash_w,
        "cash_return_policy": CASH_RETURN_POLICY,
        "starting_capital": float(starting_capital),
        "pit_input_identity": dict(pit_input_identity or {}),
        "cost_model": {
            "cost_bps_per_side": float(cost_bps_per_side or 0.0),
            "cost_rate_per_side": rate,
            "entry_cost_weight": round(invested * rate, 10),
            "entry_cost_usd": _money(invested * rate * float(starting_capital)),
            "basis": "ENTRY_ONLY_BUY_AND_HOLD_NO_EXIT_COST_UNTIL_CLOSED",
        },
        "valuation_source": valuation_source,
        "benchmark_id": benchmark_id,
        "rebalancing": "NONE_BUY_AND_HOLD",
        "rebalancing_doc": ("a rebalance is a decision, and no decision was "
                            "taken after inception; simulating one would give "
                            "the challenger information it never had"),
        "notes": notes,
        "immutable": True,
        "backfill_allowed": False,
        "forward_evidence_starts_after": str(inception_session),
        "safety_badges": list(SAFETY_BADGES),
        "creates_orders": False,
        "creates_fills": False,
        "mutates_operational_book": False,
        "promotes_model": False,
    }
    body["record_hash"] = stable_hash(body)
    return body


# --------------------------------------------------------------------------- #
# Forward accrual
# --------------------------------------------------------------------------- #
def _asof_map(series: dict) -> dict:
    """{date: adjusted price} for one panel series (pure projection)."""
    dates = series.get("dates") or []
    adj = series.get("adj") or []
    out = {}
    for i, d in enumerate(dates):
        if i < len(adj) and adj[i] is not None:
            out[d] = float(adj[i])
    return out


def forward_sessions(record: dict, price_series: dict,
                     as_of: Optional[str] = None) -> list:
    """Every session STRICTLY AFTER inception that the panel can price.

    The strictness is the whole point: a bar dated on the inception session was
    already known when the portfolio was frozen and can never be forward
    evidence for it.
    """
    t0 = str(record.get("inception_session") or "")
    held = list((record.get("weights") or {}).keys())
    dates: set = set()
    for tk in held:
        for d in (price_series.get(tk) or {}).get("dates") or []:
            if d > t0 and (as_of is None or d <= as_of):
                dates.add(d)
    return sorted(dates)


def accrue_forward(*, record: dict, price_series: Optional[dict] = None,
                   external_curve: Optional[list] = None,
                   as_of: Optional[str] = None,
                   min_priced_weight: float = MIN_PRICED_WEIGHT) -> dict:
    """Accrue one frozen portfolio's forward paper P&L.

    ``price_series`` is the owned panel ``{ticker: {dates, adj, ...}}``.
    ``external_curve`` is an alternative for challengers whose value is owned
    elsewhere (the incumbent book's own NAV series, or the benchmark's close
    series) as ``[(date, level)]`` - the kernel then measures that curve rather
    than re-deriving a second opinion about it.
    """
    t0 = str(record.get("inception_session") or "")
    start = float(record.get("starting_capital") or 0.0)
    entry_cost_w = float((record.get("cost_model") or {}).get("entry_cost_weight") or 0.0)
    weights = record.get("weights") or {}
    cash_w = float(record.get("cash_weight") or 0.0)

    rows: list = []
    uncovered: list = []
    if external_curve is not None:
        base = None
        for d, lvl in external_curve:
            if d is None or lvl is None or d <= t0:
                if d is not None and d <= t0 and lvl is not None:
                    base = float(lvl)
                continue
            if as_of is not None and d > as_of:
                continue
            if base is None:
                base = float(lvl)
                continue
            gross = float(lvl) / base - 1.0
            rows.append({"date": d, "gross_cumulative_return": gross,
                         "priced_weight": 1.0})
    else:
        ps = price_series or {}
        maps = {tk: _asof_map(ps.get(tk) or {}) for tk in weights}
        base = {}
        for tk in weights:
            m = maps.get(tk) or {}
            prior = [d for d in m if d <= t0]
            if prior:
                base[tk] = m[max(prior)]
        for d in forward_sessions(record, ps, as_of=as_of):
            gross = 0.0
            priced = 0.0
            for tk, w in weights.items():
                p0 = base.get(tk)
                p1 = (maps.get(tk) or {}).get(d)
                if p0 is None or not p0 or p1 is None:
                    continue
                gross += float(w) * (float(p1) / float(p0) - 1.0)
                priced += float(w)
            invested = float(record.get("invested_weight") or 0.0)
            share = (priced / invested) if invested > 0 else 1.0
            if share < float(min_priced_weight):
                uncovered.append({"date": d, "priced_share": _r(share, 4)})
                continue
            rows.append({"date": d,
                         "gross_cumulative_return": gross + cash_w * CASH_RETURN,
                         "priced_weight": _r(priced, 6)})

    return _measure(record=record, rows=rows, uncovered=uncovered,
                    start=start, entry_cost_w=entry_cost_w, t0=t0)


def _measure(*, record: dict, rows: list, uncovered: list, start: float,
             entry_cost_w: float, t0: str) -> dict:
    curve = []
    prev = None
    daily: list = []
    for r in rows:
        net_cum = r["gross_cumulative_return"] - entry_cost_w
        level = start * (1.0 + net_cum)
        if prev is not None and prev > 0:
            daily.append(level / prev - 1.0)
        prev = level
        curve.append({"date": r["date"],
                      "gross_cumulative_return": _r(r["gross_cumulative_return"], 8),
                      "net_cumulative_return": _r(net_cum, 8),
                      "level": _money(level),
                      "priced_weight": r.get("priced_weight")})

    n = len(curve)
    last = curve[-1] if curve else None
    peak = None
    max_dd = None
    for c in curve:
        lv = c["level"]
        peak = lv if peak is None else max(peak, lv)
        if peak and peak > 0:
            dd = lv / peak - 1.0
            max_dd = dd if max_dd is None else min(max_dd, dd)
    vol = _stdev(daily)
    mean = (sum(daily) / len(daily)) if daily else None
    ann_vol = (vol * math.sqrt(TRADING_DAYS_YEAR)) if vol else None
    sharpe = None
    if (vol and vol > 0 and mean is not None
            and n >= MIN_SESSIONS_FOR_RATIOS):
        sharpe = (mean / vol) * math.sqrt(TRADING_DAYS_YEAR)

    state = (EVIDENCE_NOT_STARTED if n == 0
             else (EVIDENCE_SUFFICIENT_FOR_RATIOS if n >= MIN_SESSIONS_FOR_RATIOS
                   else EVIDENCE_ACCRUING))
    return {
        "challenger_id": record.get("challenger_id"),
        "label": record.get("label"),
        "family": record.get("family"),
        "inception_session": t0,
        "inception_timestamp": record.get("inception_timestamp"),
        "record_hash": record.get("record_hash"),
        "valuation_source": record.get("valuation_source"),
        "evidence_state": state,
        "evidence_vocabulary": list(EVIDENCE_VOCAB),
        "sessions_scored": n,
        "first_forward_session": curve[0]["date"] if curve else None,
        "last_forward_session": last["date"] if last else None,
        "starting_capital": _money(start),
        "current_level": (last or {}).get("level"),
        "gross_cumulative_return": (last or {}).get("gross_cumulative_return"),
        "net_cumulative_return": (last or {}).get("net_cumulative_return"),
        "net_cumulative_pnl_usd": (_money(start * (last["net_cumulative_return"]))
                                   if last else None),
        "entry_cost_weight": _r(entry_cost_w, 10),
        "entry_cost_usd": _money(entry_cost_w * start),
        "max_drawdown": _r(max_dd, 6),
        "realised_daily_volatility": _r(vol, 8),
        "realised_annualised_volatility": _r(ann_vol, 6),
        "sharpe": _r(sharpe, 4),
        "sharpe_withheld_reason": (None if sharpe is not None else
                                   ("fewer than %d scored sessions"
                                    % MIN_SESSIONS_FOR_RATIOS)),
        "position_count": record.get("position_count"),
        "invested_weight": record.get("invested_weight"),
        "cash_weight": record.get("cash_weight"),
        "turnover_since_inception": 0.0,
        "turnover_doc": ("a frozen record is buy-and-hold, so its own turnover "
                         "after inception is zero by construction; the turnover "
                         "the STRATEGY implies is measured across successive "
                         "frozen records by implied_turnover()"),
        "uncovered_sessions": uncovered,
        "n_uncovered_sessions": len(uncovered),
        "curve": curve,
        "no_hindsight": True,
        "scored_only_after_inception": True,
        "research_only": True,
    }


def _stdev(xs: list) -> Optional[float]:
    if len(xs) < 2:
        return None
    m = sum(xs) / len(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))


# --------------------------------------------------------------------------- #
# Comparison on identical windows
# --------------------------------------------------------------------------- #
def compare_on_common_window(a: dict, b: dict) -> dict:
    """Compare two accrued challengers over the sessions BOTH of them scored.

    Two forward books with different inception dates or different coverage are
    not comparable end-to-end, and quoting their headline returns side by side
    would be a category error. This intersects their calendars and re-bases
    both, so the excess is measured on equal time.
    """
    ca = {c["date"]: c for c in (a.get("curve") or [])}
    cb = {c["date"]: c for c in (b.get("curve") or [])}
    common = sorted(set(ca) & set(cb))
    if not common:
        return {"comparable": False,
                "reason": "no session was scored by both books",
                "n_common_sessions": 0}
    d0, d1 = common[0], common[-1]

    def _seg(cm):
        r0 = cm[d0]["net_cumulative_return"] or 0.0
        r1 = cm[d1]["net_cumulative_return"] or 0.0
        return (1.0 + r1) / (1.0 + r0) - 1.0

    ra, rb = _seg(ca), _seg(cb)
    return {
        "comparable": True,
        "n_common_sessions": len(common),
        "window_start": d0,
        "window_end": d1,
        "a_id": a.get("challenger_id"), "b_id": b.get("challenger_id"),
        "a_net_return": _r(ra, 8), "b_net_return": _r(rb, 8),
        "excess_return": _r(ra - rb, 8),
        "excess_pct_points": _r(100.0 * (ra - rb), 4),
        "equal_time_window": True,
    }


def implied_turnover(records: list) -> dict:
    """The turnover successive frozen targets of one challenger really imply.

    Each consecutive pair of records is one rebalance the strategy would have
    demanded; the one-way turnover between them is what it would have cost.
    """
    recs = sorted([r for r in (records or []) if r.get("inception_session")],
                  key=lambda r: r["inception_session"])
    legs = []
    for i in range(1, len(recs)):
        prev, cur = recs[i - 1].get("weights") or {}, recs[i].get("weights") or {}
        names = set(prev) | set(cur)
        traded = sum(abs((cur.get(t) or 0.0) - (prev.get(t) or 0.0)) for t in names)
        legs.append({"from_session": recs[i - 1]["inception_session"],
                     "to_session": recs[i]["inception_session"],
                     "one_way_turnover": _r(traded / 2.0, 6),
                     "two_way_traded_weight": _r(traded, 6)})
    total = sum((l["two_way_traded_weight"] or 0.0) for l in legs)
    return {"n_records": len(recs), "n_rebalances": len(legs), "legs": legs,
            "cumulative_two_way_traded_weight": _r(total, 6),
            "mean_one_way_turnover": (_r(total / (2.0 * len(legs)), 6)
                                      if legs else None)}


def leaderboard(accrued: list, *, control_id: Optional[str] = None) -> list:
    """Rank accrued challengers. Evidence maturity FIRST, measured edge second.

    Two scored sessions never outrank two hundred: a book with almost no
    evidence cannot be at the top of a leaderboard merely because its two days
    were good.
    """
    control = None
    for a in accrued or []:
        if control_id and a.get("challenger_id") == control_id:
            control = a
    rows = []
    for a in accrued or []:
        cmp_ = compare_on_common_window(a, control) if control else {}
        rows.append({
            "challenger_id": a.get("challenger_id"),
            "label": a.get("label"),
            "family": a.get("family"),
            "evidence_state": a.get("evidence_state"),
            "sessions_scored": a.get("sessions_scored"),
            "net_cumulative_return": a.get("net_cumulative_return"),
            "net_cumulative_pnl_usd": a.get("net_cumulative_pnl_usd"),
            "max_drawdown": a.get("max_drawdown"),
            "realised_annualised_volatility": a.get("realised_annualised_volatility"),
            "sharpe": a.get("sharpe"),
            "cash_weight": a.get("cash_weight"),
            "position_count": a.get("position_count"),
            "entry_cost_usd": a.get("entry_cost_usd"),
            "vs_control": cmp_ if cmp_ else None,
            "excess_vs_control_pct_points": (cmp_ or {}).get("excess_pct_points"),
            "promotion_allowed": False,
        })
    order = {EVIDENCE_SUFFICIENT_FOR_RATIOS: 0, EVIDENCE_ACCRUING: 1,
             EVIDENCE_NOT_STARTED: 2}
    rows.sort(key=lambda r: (order.get(r["evidence_state"], 3),
                             -(r["net_cumulative_return"] or float("-inf"))))
    for i, r in enumerate(rows, 1):
        r["rank"] = i
    return rows


__all__ = [
    "CALCULATION_OWNER", "SCHEMA_VERSION", "RECORD_SCHEMA_VERSION", "PHASE",
    "TRADING_DAYS_YEAR", "CASH_RETURN", "CASH_RETURN_POLICY",
    "MIN_PRICED_WEIGHT", "MIN_SESSIONS_FOR_RATIOS", "EVIDENCE_VOCAB",
    "EVIDENCE_NOT_STARTED", "EVIDENCE_ACCRUING",
    "EVIDENCE_SUFFICIENT_FOR_RATIOS", "SAFETY_BADGES",
    "VALUATION_PRICE_PANEL", "VALUATION_DESK_PERFORMANCE",
    "VALUATION_CASH_POLICY",
    "stable_hash", "make_inception_record", "forward_sessions",
    "accrue_forward", "compare_on_common_window", "implied_turnover",
    "leaderboard",
]
