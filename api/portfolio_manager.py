"""api/portfolio_manager.py - Phase 26 Portfolio Manager decision-intelligence aggregation.

Read-only composition layer that turns the validated Phase 25 multi-horizon platform plus the
canonical Phase 14-C portfolio valuation into ONE portfolio-manager view: "WHAT SHOULD I DO TODAY?".

It aggregates existing sources only - it is NOT another model engine:
    * paper_trader.api.multi_horizon_engine     (current scores / books / operating state / recs)
    * paper_trader.api.multi_horizon_ledger     (append-only confirmed paper-alpha snapshots)
    * paper_trader.api.multi_horizon_platform   (fast-spec availability)
    * paper_trader.api.portfolio_valuation      (canonical current executed paper portfolio mark)

No model formula is changed, no weight is optimized, no expected-return estimate is invented and
no opaque confidence score is manufactured.  Every derived classification (decision headline,
action bucket, agreement class, health status) is a deterministic rule over stored fields, with
its rule documented next to the constant.

Every payload carries the Phase 26 safety block: paper_only=True, orders_enabled=False,
automation_enabled=False, broker_enabled=False, champion_replaced=False, performed_write=False.
Nothing here writes anywhere - snapshot preview/confirm remain the existing explicit Phase 25
endpoints (ledger-only).  A missing input degrades to a *_UNAVAILABLE status with HTTP 200.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Optional

from paper_trader.api import multi_horizon_engine as eng
from paper_trader.api import multi_horizon_ledger as ledger
from paper_trader.api import multi_horizon_platform as plat
from paper_trader.api import multi_horizon_registry as mreg
from paper_trader.api import portfolio_valuation

PHASE = "26"

# --------------------------------------------------------------------------- #
# Decision headline vocabulary (exactly the six Phase 26 phrases)
# --------------------------------------------------------------------------- #
HEADLINE_NO_CHANGE = "NO PORTFOLIO CHANGE REQUIRED"
HEADLINE_REVIEW_NEW = "REVIEW NEW PORTFOLIO CANDIDATES"
HEADLINE_REVIEW_REBALANCE = "REVIEW PORTFOLIO REBALANCE"
HEADLINE_REVIEW_RISK = "REVIEW RISK EXCEPTIONS"
HEADLINE_DATA_REFRESH = "DATA REFRESH REQUIRED"
HEADLINE_MANUAL_CONFIRMATION = "MANUAL CONFIRMATION REQUIRED"
# Phase 27B.1: truthful operational-implementation headlines. A confirmed target
# that the operational Alpha Paper Book #1 has not implemented yet must never be
# summarized as "NO PORTFOLIO CHANGE REQUIRED".
HEADLINE_IMPLEMENTATION_PENDING = "TARGET CONFIRMED — INITIAL IMPLEMENTATION PENDING"
HEADLINE_READY_FOR_ORDER_PLAN = "ALPHA BOOK READY FOR INITIAL ORDER PLAN"
# Phase 27B.2: the executable order plan EXISTS — the one next action is to
# review it and manually confirm it (REVIEW_AND_CONFIRM_ORDER_PLAN).
HEADLINE_ORDER_PLAN_REVIEW = "ORDER PLAN READY FOR REVIEW"

ALL_HEADLINES = [
    HEADLINE_NO_CHANGE, HEADLINE_REVIEW_NEW, HEADLINE_REVIEW_REBALANCE,
    HEADLINE_REVIEW_RISK, HEADLINE_DATA_REFRESH, HEADLINE_MANUAL_CONFIRMATION,
    HEADLINE_IMPLEMENTATION_PENDING, HEADLINE_READY_FOR_ORDER_PLAN,
    HEADLINE_ORDER_PLAN_REVIEW,
]

# Portfolio-manager action vocabulary (visible), mapped 1:1 from the internal
# deterministic recommendation vocabulary. Never plain BUY/SELL.
ACTION_ADD = "ADD_CANDIDATE"
ACTION_HOLD = "HOLD"
ACTION_WATCH = "WATCH"
ACTION_REDUCE = "REDUCE_CANDIDATE"
ACTION_EXIT = "EXIT_CANDIDATE"
ACTION_WAIT_BLOCKED = "WAIT_DATA_BLOCKED"

ALL_ACTIONS = [ACTION_ADD, ACTION_HOLD, ACTION_WATCH, ACTION_REDUCE, ACTION_EXIT,
               ACTION_WAIT_BLOCKED]

ACTION_DISPLAY_LABELS = {
    ACTION_ADD: "ADD CANDIDATES",
    ACTION_HOLD: "HOLD",
    ACTION_WATCH: "WATCH",
    ACTION_REDUCE: "REDUCE CANDIDATES",
    ACTION_EXIT: "EXIT CANDIDATES",
    ACTION_WAIT_BLOCKED: "WAIT / DATA BLOCKED",
}

# Agreement classification (deterministic; percentiles within the common universe).
# Rule: BOTH_STRONG if both legs >= 0.6; FUNDAMENTAL_LED if fund >= 0.6 > mom;
# MOMENTUM_LED if mom >= 0.6 > fund; MIXED if both < 0.6.
AGREE_BOTH_STRONG = "BOTH_STRONG"
AGREE_FUNDAMENTAL_LED = "FUNDAMENTAL_LED"
AGREE_MOMENTUM_LED = "MOMENTUM_LED"
AGREE_MIXED = "MIXED"
_AGREE_THRESHOLD = 0.6

# Health status vocabulary.
HEALTH_HEALTHY = "HEALTHY"
HEALTH_REVIEW = "REVIEW"
HEALTH_BLOCKED = "BLOCKED"

# Deterministic health thresholds (documented, not tuned):
# a target-book mean 63d realized vol above this is flagged for review.
_VOL_REVIEW_THRESHOLD = 0.40
# a rank move of at least this many places counts as a "rank mover".
_RANK_MOVER_THRESHOLD = 5

CHANGE_BASIS_INITIAL = "INITIAL_PORTFOLIO_PROPOSAL"
CHANGE_BASIS_SNAPSHOT = "VS_LAST_CONFIRMED_SNAPSHOT"

PRIMARY_MODEL_ID = "fundamental_momentum_50_50_v1"
PRIMARY_BOOK_ID = "fundamental_momentum_50_50_top25"

# Injection seam for tests: the canonical valuation loader (DB read).
_VALUATION_LOADER = portfolio_valuation.load_portfolio_valuation


# --------------------------------------------------------------------------- #
# Phase 27B.1 - operational-book implementation state (the ONE canonical source)
# --------------------------------------------------------------------------- #
def _default_operational_book_loader() -> dict:
    from paper_trader.api import operational_book as ob
    return ob.load_operational_book()


# Injection seam for tests (mirrors _VALUATION_LOADER); degrade-only.
_OPERATIONAL_BOOK_LOADER = _default_operational_book_loader


def _operational_book_block() -> dict:
    """Compact implementation-state view of Alpha Paper Book #1 straight from the
    canonical /v1/operational-book payload. Degrades to unavailable; never raises."""
    try:
        d = _OPERATIONAL_BOOK_LOADER()
        o = (d or {}).get("operational_book") or {}
        if not o:
            return {"available": False}
        return {
            "available": True,
            "book_id": o.get("book_id"),
            "book_label": o.get("book_label"),
            "initialized": bool(o.get("initialized")),
            "current_status": o.get("current_status"),
            "cash": o.get("cash"), "nav": o.get("nav"),
            "holdings_count": o.get("holdings_count"),
            "holdings": o.get("holdings") or {},
            "pending_order_count": o.get("pending_order_count"),
            "fill_count": o.get("fill_count"),
            "target_count": o.get("target_count"),
            "target_market_date": o.get("target_market_date"),
            "target_confirmed": o.get("target_confirmation_status") == "CONFIRMED",
            "target_confirmation_status": o.get("target_confirmation_status"),
            "desk_mark_date": o.get("desk_mark_date"),
            "desk_mark_status": o.get("desk_mark_status"),
            "implementation_count": o.get("implementation_count"),
            "implementation_percentage": o.get("implementation_percentage"),
            "order_plan_ready": bool(o.get("order_plan_ready")),
            "workflow_stage": o.get("workflow_stage"),
            "next_action_code": o.get("next_action_code"),
            "next_action": o.get("next_action"),
            "blockers": o.get("blockers") or [],
            "informational": o.get("informational") or [],
            "ledger_integrity_ok": o.get("ledger_integrity_ok"),
            "canonical_state": o.get("canonical_state"),
        }
    except Exception:  # noqa: BLE001 - the PM page must load even without the desk
        return {"available": False}


def _implementation_gap(ops: dict) -> bool:
    """True when a confirmed target awaits its INITIAL implementation: the book is
    initialized, the target is confirmed, yet nothing is held, pending or filled."""
    return bool(ops.get("available") and ops.get("initialized")
                and ops.get("target_confirmed")
                and (ops.get("target_count") or 0) > 0
                and (ops.get("implementation_count") or 0) < (ops.get("target_count") or 0)
                and not (ops.get("pending_order_count") or 0)
                and not (ops.get("fill_count") or 0))


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _today() -> str:
    return date.today().isoformat()


def _f(x) -> Optional[float]:
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _round(x, nd=6):
    return None if x is None else round(float(x), nd)


def _pm_safety() -> dict:
    """The Phase 26 safety block attached to every payload (all read-only)."""
    return {
        "paper_only": True,
        "orders_enabled": False,
        "automation_enabled": False,
        "broker_enabled": False,
        "champion_replaced": False,
        "performed_write": False,
        "read_only": True,
        "manual_review_only": True,
        "no_orders": True,
        "no_broker": True,
        "no_fills": True,
        "no_signals": True,
        "no_trade_decisions": True,
        "no_position_changes": True,
        "no_live_promotion": True,
        "safety_badges": list(mreg.SAFETY_BADGES),
    }


# --------------------------------------------------------------------------- #
# Valuation (current EXECUTED paper portfolio - canonical Phase 14-C mark)
# --------------------------------------------------------------------------- #
def _valuation_block() -> dict:
    """Compact view of the canonical portfolio valuation. Degrades, never raises."""
    try:
        v = _VALUATION_LOADER()
    except Exception as exc:  # noqa: BLE001
        return {"available": False, "warnings": ["Portfolio valuation unavailable: %s" % str(exc)[:160]],
                "positions": [], "open_position_count": None}
    cm = v.get("current_mark") or {}
    total = _f(cm.get("current_total_value"))
    cash = _f(cm.get("current_cash"))
    invested = _f(cm.get("current_positions_value"))
    cash_pct = _round(100.0 * cash / total, 2) if (cash is not None and total) else None
    invested_pct = _round(100.0 * invested / total, 2) if (invested is not None and total) else None
    positions = []
    for p in (v.get("positions") or []):
        positions.append({
            "ticker": p.get("ticker"),
            "weight_pct": _f(p.get("weight_pct")),
            "market_value": _f(p.get("market_value")),
            "unrealized_pnl": _f(p.get("unrealized_pnl")),
            "unrealized_pnl_pct": _f(p.get("unrealized_pnl_pct")),
            "status": p.get("status"),
            "reason": p.get("reason"),
        })
    return {
        "available": bool(v.get("seeded")),
        "current_total_value": total,
        "current_cash": cash,
        "current_invested_value": invested,
        "cash_pct": cash_pct,
        "invested_pct": invested_pct,
        "current_unrealized_pnl": _f(cm.get("current_unrealized_pnl")),
        "current_total_return_pct": cm.get("current_total_return_pct"),
        "open_position_count": cm.get("open_position_count"),
        "as_of_market_date": cm.get("as_of_market_date"),
        "freshness_status": cm.get("freshness_status"),
        "age_calendar_days": cm.get("age_calendar_days"),
        "positions": positions,
        "warnings": list(v.get("warnings") or []),
    }


# --------------------------------------------------------------------------- #
# Shared context (assembled once per request; engine result is mtime-cached)
# --------------------------------------------------------------------------- #
def _context(*, panel_path=None, inputs_dir=None, ledger_dir=None, fast_spec_path=None,
             size: int = 25, with_valuation: bool = True) -> dict:
    cur = eng.build_current(panel_path=panel_path, inputs_dir=inputs_dir)
    fast_ok, fast_info = plat._validated_fast_available(fast_spec_path)
    prior = ledger.latest_confirmed_by_sleeve(ledger_dir)
    ready = cur.get("status") == eng.STATUS_READY
    state = eng.compute_operating_state(cur, prior, validated_fast_alpha_available=fast_ok)
    rec = eng.compute_recommendations(cur, prior, mreg.SLEEVE_COMBINED, size=size) if ready else None
    snaps = ledger.list_snapshots(ledger_dir)
    prior_combined = prior.get(mreg.SLEEVE_COMBINED) or {}
    prior_snap_full = None
    if prior_combined.get("snapshot_id"):
        prior_snap_full = ledger.get_snapshot(prior_combined["snapshot_id"], ledger_dir)
    valuation = _valuation_block() if with_valuation else {"available": False, "positions": [],
                                                           "open_position_count": None,
                                                           "warnings": []}
    return {"cur": cur, "ready": ready, "state": state, "rec": rec, "prior": prior,
            "prior_combined": prior_combined, "prior_snap_full": prior_snap_full,
            "snaps": snaps, "fast_ok": fast_ok, "fast_info": fast_info,
            "valuation": valuation, "size": size, "ledger_dir": ledger_dir}


# --------------------------------------------------------------------------- #
# Labeled dates + misalignment warnings (Workstream L)
# --------------------------------------------------------------------------- #
DATE_LABELS = {
    "decision_date": "Decision date (today's calendar date)",
    "latest_alpha_market_date": "Current alpha calculation market date (owned research store)",
    "portfolio_valuation_date": "Portfolio EOD valuation date (executed paper portfolio mark)",
    "fundamental_as_of_date": "Fundamental data as-of date (frozen owned panel)",
    "last_confirmed_snapshot_date": "Last confirmed paper-alpha snapshot market date",
    "next_manual_review_date": "Next scheduled manual review date (combined sleeve)",
}


def _dates_block(ctx: dict) -> tuple[dict, list[str]]:
    cur, state, val = ctx["cur"], ctx["state"], ctx["valuation"]
    combined_sleeve = None
    for s in state.get("sleeves", []):
        if s.get("sleeve_id") == mreg.SLEEVE_COMBINED:
            combined_sleeve = s
    dates = {
        "decision_date": _today(),
        "latest_alpha_market_date": cur.get("market_as_of_date"),
        "portfolio_valuation_date": val.get("as_of_market_date"),
        "fundamental_as_of_date": cur.get("fundamental_as_of_date"),
        "last_confirmed_snapshot_date": (ctx["prior_combined"] or {}).get("market_as_of_date"),
        "next_manual_review_date": (combined_sleeve or {}).get("next_manual_review_date"),
    }
    warnings: list[str] = []
    amd, pvd = dates["latest_alpha_market_date"], dates["portfolio_valuation_date"]
    if amd and pvd and amd[:10] != str(pvd)[:10]:
        warnings.append(
            "Date misalignment: the current alpha calculation market date (%s) and the portfolio "
            "EOD valuation date (%s) differ. Each figure on this page is labeled with its own date; "
            "they are intentionally NOT collapsed into one date." % (amd[:10], str(pvd)[:10]))
    fund_month = cur.get("fundamental_month")
    if ctx["ready"] and eng._is_fundamental_stale(fund_month, cur.get("market_as_of_date")):
        warnings.append(
            "Fundamental data is stale: the fundamental panel month (%s) lags the market date (%s) "
            "by more than one quarter. Fundamental-led decisions are blocked until refresh."
            % (fund_month, cur.get("market_as_of_date")))
    if val.get("available") and val.get("freshness_status") == portfolio_valuation.STALE:
        warnings.append(
            "The executed-portfolio valuation mark is STALE (%s calendar days old). Run the manual "
            "daily operating session to refresh owned EOD prices." % val.get("age_calendar_days"))
    if not val.get("available"):
        warnings.append("Portfolio valuation is unavailable (database not reachable or not seeded); "
                        "executed-portfolio figures are missing, not zero.")
    return dates, warnings


def _fundamental_stale(ctx: dict) -> bool:
    cur = ctx["cur"]
    return bool(ctx["ready"] and eng._is_fundamental_stale(cur.get("fundamental_month"),
                                                           cur.get("market_as_of_date")))


# --------------------------------------------------------------------------- #
# Action classification (Workstream B)
# --------------------------------------------------------------------------- #
# Reason codes that mean the row is blocked by data quality / eligibility rather
# than waiting for a review cycle.
_BLOCK_CODES = {"DATA_QUALITY_BLOCK", "LIQUIDITY_FILTER_FAILED", "MISSING_MOMENTUM",
                "MISSING_COMPOSITE_SN", "MOMENTUM_HISTORY_INSUFFICIENT", "NOT_CURRENT_MEMBER",
                "FUNDAMENTAL_DATA_STALE"}


def _pm_action(rec_row: dict) -> str:
    """Deterministic internal-recommendation -> portfolio-manager action mapping."""
    rec = rec_row.get("recommendation")
    codes = rec_row.get("reason_codes") or []
    if rec == eng.REC_BUY:
        return ACTION_ADD
    if rec == eng.REC_HOLD:
        if any(str(c).startswith("WITHIN_EXIT_BUFFER") for c in codes):
            return ACTION_WATCH
        return ACTION_HOLD
    if rec == eng.REC_REDUCE:
        return ACTION_REDUCE
    if rec == eng.REC_EXIT:
        return ACTION_EXIT
    return ACTION_WAIT_BLOCKED


def _agreement(fund_pct: Optional[float], mom_pct: Optional[float]) -> Optional[str]:
    if fund_pct is None or mom_pct is None:
        return None
    if fund_pct >= _AGREE_THRESHOLD and mom_pct >= _AGREE_THRESHOLD:
        return AGREE_BOTH_STRONG
    if fund_pct >= _AGREE_THRESHOLD > mom_pct:
        return AGREE_FUNDAMENTAL_LED
    if mom_pct >= _AGREE_THRESHOLD > fund_pct:
        return AGREE_MOMENTUM_LED
    return AGREE_MIXED


_AGREEMENT_PHRASE = {
    AGREE_BOTH_STRONG: "Both fundamental and momentum signals are positive.",
    AGREE_FUNDAMENTAL_LED: "Strong fundamentals offset weaker momentum.",
    AGREE_MOMENTUM_LED: "Strong momentum offsets a weaker fundamental rank.",
    AGREE_MIXED: "Mixed alpha-leg evidence (neither leg is strong).",
}


def _reason_text(rec_row: dict, action: str, size: int) -> str:
    """Deterministic plain-English reason assembled from stored reason codes only."""
    codes = rec_row.get("reason_codes") or []
    parts: list[str] = []
    if action == ACTION_ADD:
        parts.append("Ranked in the combined Top-%d; entered the target book." % size)
    elif action == ACTION_WATCH:
        parts.append("Position remains inside the hold buffer (slipped below the Top-%d "
                     "but not below the exit buffer)." % size)
    elif action == ACTION_HOLD and any(str(c).startswith("REMAINS_TOP_") for c in codes):
        parts.append("Remains in the combined Top-%d." % size)
    elif action == ACTION_EXIT:
        if "FELL_BELOW_EXIT_BUFFER" in codes:
            parts.append("Position fell below the exit buffer.")
        else:
            excl = [c for c in codes if c in _BLOCK_CODES]
            if excl:
                parts.append("No longer eligible: %s." % excl[0])
            else:
                parts.append("Left the target book.")
    if "REVIEW_NOT_DUE" in codes:
        parts.append("Review is not due; monitor only.")
    if "LIQUIDITY_FILTER_FAILED" in codes or "LOW_LIQUIDITY" in (rec_row.get("risk_flags") or []):
        parts.append("Liquidity requirement failed.")
    if "FUNDAMENTAL_DATA_STALE" in codes:
        parts.append("Fundamental data is stale; no action permitted.")
    if "SECTOR_LIMIT_REACHED" in codes:
        parts.append("Sector concentration limit reached for this name.")
    ag = _agreement((rec_row.get("component_contributions") or {}).get("fund_percentile"),
                    (rec_row.get("component_contributions") or {}).get("mom_percentile"))
    if ag and action in (ACTION_ADD, ACTION_HOLD, ACTION_WATCH):
        parts.append(_AGREEMENT_PHRASE[ag])
    return " ".join(parts) if parts else "No change against the last confirmed book."


def _action_rows(ctx: dict) -> list[dict]:
    rec = ctx["rec"] or {}
    size = rec.get("size") or ctx["size"]
    rows = []
    for r in rec.get("recommendations") or []:
        action = _pm_action(r)
        comp = r.get("component_contributions") or {}
        fund_pct, mom_pct = comp.get("fund_percentile"), comp.get("mom_percentile")
        ranks = r.get("model_ranks") or {}
        rows.append({
            "ticker": r.get("ticker"),
            "action": action,
            "action_label": ACTION_DISPLAY_LABELS[action],
            "engine_recommendation": r.get("recommendation"),
            "target_weight": r.get("target_weight"),
            "current_weight": r.get("current_theoretical_weight"),
            "combined_rank": ranks.get("current"),
            "fund_rank": ranks.get("fundamental"),
            "mom_rank": ranks.get("momentum"),
            "fund_percentile": fund_pct,
            "mom_percentile": mom_pct,
            # fixed 50/50 contributions of the percentile-rank blend - NOT optimized.
            "fund_contribution": _round(0.5 * fund_pct, 6) if fund_pct is not None else None,
            "mom_contribution": _round(0.5 * mom_pct, 6) if mom_pct is not None else None,
            "combined_score": r.get("combined_score"),
            "agreement": _agreement(fund_pct, mom_pct),
            "sector": r.get("sector"),
            "risk_flags": r.get("risk_flags") or [],
            "reason_codes": r.get("reason_codes") or [],
            "reason_text": _reason_text(r, action, size),
            "review_due": bool(rec.get("review_due")),
            "review_cadence": r.get("review_cadence"),
        })
    return rows


def _action_counts(rows: list[dict]) -> dict:
    counts = {a: 0 for a in ALL_ACTIONS}
    for r in rows:
        counts[r["action"]] = counts.get(r["action"], 0) + 1
    return counts


# --------------------------------------------------------------------------- #
# Changeset (Workstream D)
# --------------------------------------------------------------------------- #
def _changeset(ctx: dict) -> dict:
    cur, rec = ctx["cur"], ctx["rec"] or {}
    book = cur["books"]["books"].get(PRIMARY_BOOK_ID) if ctx["ready"] else None
    if book is None:
        return {"available": False}
    prior_combined = ctx["prior_combined"] or {}
    prior_names: list[str] = list(prior_combined.get("constituents_top25") or [])
    has_prior = bool(prior_names)
    basis = CHANGE_BASIS_SNAPSHOT if has_prior else CHANGE_BASIS_INITIAL

    prior_block = {}
    if ctx["prior_snap_full"]:
        prior_block = (ctx["prior_snap_full"].get("sleeves") or {}).get(mreg.SLEEVE_COMBINED) or {}
    prior_weight = None
    if has_prior:
        pw = (prior_block.get("target_weights") or {}).get("top25")
        prior_weight = pw if pw is not None else round(min(1.0 / len(prior_names), eng.MAX_INDIVIDUAL_WEIGHT), 6)
    prior_sector_exposure = dict(prior_block.get("sector_exposure_top25") or {})

    target = {c["ticker"]: c for c in book["constituents"]}
    weight = book["equal_weight"]
    tset, pset = set(target), set(prior_names)

    additions = [{"ticker": t, "target_weight": weight, "sector": target[t].get("sector"),
                  "combined_rank": target[t].get("rank")}
                 for t in sorted(tset - pset, key=lambda x: target[x].get("rank") or 999)]
    rec_by_tk = {r.get("ticker"): r for r in (rec.get("recommendations") or [])}
    removals = []
    for t in sorted(pset - tset):
        rrow = rec_by_tk.get(t) or {}
        removals.append({"ticker": t, "prior_weight": prior_weight,
                         "sector": rrow.get("sector"),
                         "reason": _reason_text(rrow, _pm_action(rrow), rec.get("size") or 25)
                         if rrow else "Left the target book."})
    retained, increases, decreases, unchanged = [], [], [], []
    for t in sorted(tset & pset):
        delta = _round((weight or 0.0) - (prior_weight or 0.0), 6)
        row = {"ticker": t, "prior_weight": prior_weight, "target_weight": weight,
               "weight_delta": delta, "sector": target[t].get("sector")}
        retained.append(row)
        if delta and delta > 0:
            increases.append(row)
        elif delta and delta < 0:
            decreases.append(row)
        else:
            unchanged.append(row)

    blocked = [{"ticker": t, "reason": "SECTOR_LIMIT_REACHED - the sector concentration cap "
                                        "blocked this higher-ranked name from entering."}
               for t in (book.get("sector_capped_out") or [])]
    for t in sorted(pset - tset):
        rrow = rec_by_tk.get(t) or {}
        codes = set(rrow.get("reason_codes") or [])
        hard = codes & _BLOCK_CODES
        if hard:
            blocked.append({"ticker": t, "reason": "%s - prior holding is now ineligible." % sorted(hard)[0]})

    sectors = sorted(set(book["sector_exposure"]) | set(prior_sector_exposure))
    sector_changes = []
    for s in sectors:
        before = prior_sector_exposure.get(s, 0.0) if has_prior else 0.0
        after = book["sector_exposure"].get(s, 0.0)
        sector_changes.append({"sector": s, "before": _round(before, 6), "after": _round(after, 6),
                               "delta": _round(after - before, 6)})
    sector_changes.sort(key=lambda r: -(r["after"] or 0.0))

    prior_cash = _round(max(0.0, 1.0 - (prior_weight or 0.0) * len(prior_names)), 6) if has_prior else 1.0
    after_cash = book.get("unallocated_weight", 0.0)
    largest_sector_after = max(book["sector_exposure"].items(), key=lambda kv: kv[1])[0] \
        if book["sector_exposure"] else None
    largest_sector_before = max(prior_sector_exposure.items(), key=lambda kv: kv[1])[0] \
        if prior_sector_exposure else None

    est_turnover = rec.get("estimated_turnover")
    # Cost display: the engine's 25 bps round-trip assumption; the one-way figure is
    # cost_bps x one-way turnover (same stored assumption, no new estimate invented).
    return {
        "available": True,
        "change_basis": basis,
        "is_initial_portfolio_proposal": not has_prior,
        "prior_snapshot_id": prior_combined.get("snapshot_id"),
        "prior_snapshot_market_date": prior_combined.get("market_as_of_date"),
        "target_book_id": PRIMARY_BOOK_ID,
        "n_target": len(target), "n_prior": len(prior_names),
        "additions": additions, "removals": removals, "retained": retained,
        "weight_increases": increases, "weight_decreases": decreases, "unchanged": unchanged,
        "blocked_changes": blocked,
        "estimated_turnover": est_turnover,
        "estimated_transaction_cost_round_trip": rec.get("estimated_transaction_cost"),
        "estimated_transaction_cost_one_way": _round(eng.COST_BPS * est_turnover, 6)
        if est_turnover is not None else None,
        "cost_assumption_bps": 25,
        "sector_weight_changes": sector_changes,
        "cash_weight_change": {"before": prior_cash, "after": after_cash,
                               "delta": _round((after_cash or 0.0) - (prior_cash or 0.0), 6)},
        "concentration_change": {
            "largest_sector_before": largest_sector_before,
            "largest_sector_after": largest_sector_after,
            "largest_position_weight_before": prior_weight,
            "largest_position_weight_after": weight,
            "sector_cap_fraction": eng.SECTOR_CAP_FRACTION,
        },
    }


def _executed_vs_proposed(ctx: dict) -> dict:
    """Explicit three-way distinction: executed paper portfolio vs proposed alpha target
    vs confirmed alpha-snapshot ledger. Critical: these are NOT the same thing."""
    val = ctx["valuation"]
    cur = ctx["cur"]
    book = cur["books"]["books"].get(PRIMARY_BOOK_ID) if ctx["ready"] else None
    executed = [p.get("ticker") for p in (val.get("positions") or []) if p.get("ticker")]
    target = [c["ticker"] for c in (book or {}).get("constituents", [])]
    tset, eset = set(target), set(executed)
    snaps = ctx["snaps"] or {}
    return {
        "current_executed_paper_portfolio": {
            "source": "Executed paper positions (legacy manual trade workflow; canonical Phase 14-C mark)",
            "open_position_count": val.get("open_position_count"),
            "tickers": executed,
            "note": "These are the ACTUAL executed paper positions. They are NOT the confirmed "
                    "25-name alpha book and are never modified by this page.",
        },
        "proposed_alpha_target_portfolio": {
            "source": "Current primary operational Top-25 target book (fixed 50/50 ensemble)",
            "book_id": PRIMARY_BOOK_ID,
            "n_names": len(target),
            "note": "This is the PROPOSED paper target. Confirming it writes only a paper-alpha "
                    "snapshot to the dedicated local ledger - no positions change.",
        },
        "confirmed_alpha_snapshot_ledger": {
            "source": "Append-only paper-alpha snapshot ledger (Phase 25)",
            "n_confirmed": snaps.get("n_confirmed", 0),
            "last_confirmed_snapshot_id": (ctx["prior_combined"] or {}).get("snapshot_id"),
            "last_confirmed_market_date": (ctx["prior_combined"] or {}).get("market_as_of_date"),
        },
        "overlap_tickers": sorted(tset & eset),
        "executed_only_tickers": sorted(eset - tset),
        "target_only_count": len(tset - eset),
    }


# --------------------------------------------------------------------------- #
# Portfolio health (Workstream E)
# --------------------------------------------------------------------------- #
def _health_items(ctx: dict) -> list[dict]:
    items: list[dict] = []
    val = ctx["valuation"]
    cur = ctx["cur"]
    ready = ctx["ready"]
    book = cur["books"]["books"].get(PRIMARY_BOOK_ID) if ready else None

    # Phase 27B.1: every item carries its scope - the executed-valuation items are
    # the ARCHIVED legacy portfolio, the proposed-book items are MODEL TARGET
    # diagnostics. Neither is Alpha Paper Book #1 health (see _alpha_health_items).
    scope_ref = {"scope": "LEGACY_ARCHIVE"}

    def item(key, label, status, value, explanation):
        items.append({"key": key, "label": label, "status": status,
                      "value": value, "explanation": explanation,
                      "scope": scope_ref["scope"]})

    # --- executed portfolio (canonical valuation) ---------------------------- #
    if val.get("available"):
        item("cash_pct", "Cash percentage", HEALTH_HEALTHY, val.get("cash_pct"),
             "Share of the executed paper portfolio held as cash (canonical current mark).")
        item("invested_pct", "Invested percentage", HEALTH_HEALTHY, val.get("invested_pct"),
             "Share of the executed paper portfolio invested in open positions.")
        item("position_count", "Open positions (executed) vs proposed target",
             HEALTH_HEALTHY,
             "%s executed / %s proposed" % (val.get("open_position_count"),
                                            (book or {}).get("size_actual")),
             "The executed paper portfolio and the proposed 25-name alpha target are separate; "
             "confirming the target writes a paper snapshot only, never positions.")
        positions = val.get("positions") or []
        if positions:
            largest = max(positions, key=lambda p: (p.get("weight_pct") or 0.0))
            lw = largest.get("weight_pct")
            st = HEALTH_REVIEW if (lw is not None and lw > 25.0) else HEALTH_HEALTHY
            item("largest_position", "Largest executed position", st,
                 "%s (%s%%)" % (largest.get("ticker"), lw),
                 ("Position weight exceeds the 25% concentration threshold; manual review "
                  "recommended." if st == HEALTH_REVIEW else
                  "Largest single-name weight of the executed paper portfolio."))
        upnl = val.get("current_unrealized_pnl")
        item("unrealized_pnl", "Current unrealized P&L", HEALTH_HEALTHY, upnl,
             "Unrealized P&L of the executed paper positions at the canonical current mark.")
        exceptions = [p for p in positions if p.get("status") in
                      (portfolio_valuation.POS_REVIEW_FOR_EXIT, portfolio_valuation.POS_WATCH,
                       portfolio_valuation.POS_PRICE_UNAVAILABLE)]
        if exceptions:
            worst = HEALTH_BLOCKED if any(p.get("status") == portfolio_valuation.POS_PRICE_UNAVAILABLE
                                          for p in exceptions) else HEALTH_REVIEW
            item("risk_exceptions", "Position risk exceptions", worst,
                 ", ".join("%s: %s" % (p.get("ticker"), p.get("status")) for p in exceptions),
                 "; ".join("%s - %s" % (p.get("ticker"), p.get("reason")) for p in exceptions))
        else:
            item("risk_exceptions", "Position risk exceptions", HEALTH_HEALTHY, "none",
                 "No executed position is in WATCH / REVIEW_FOR_EXIT / PRICE_UNAVAILABLE state.")
        if val.get("freshness_status") == portfolio_valuation.STALE:
            item("stale_data", "Valuation freshness", HEALTH_REVIEW,
                 "%s (%s days old)" % (val.get("freshness_status"), val.get("age_calendar_days")),
                 "The executed-portfolio mark is older than 4 calendar days; refresh owned EOD "
                 "prices via the manual daily operating session.")
        else:
            item("stale_data", "Valuation freshness", HEALTH_HEALTHY,
                 val.get("freshness_status"),
                 "The executed-portfolio mark is within the freshness window.")
        cash_pct = val.get("cash_pct")
        item("capacity", "Portfolio capacity", HEALTH_HEALTHY,
             ("%s%% cash available" % cash_pct) if cash_pct is not None else "unknown",
             "Cash share of the executed paper portfolio available for future manual paper "
             "decisions. Informational - no order is ever created here.")
    else:
        item("valuation", "Executed portfolio valuation", HEALTH_BLOCKED, "UNAVAILABLE",
             "The canonical portfolio valuation could not be loaded (database unreachable or "
             "portfolio not seeded), so executed-portfolio health cannot be assessed.")

    # --- proposed target book (owned research inputs) ------------------------ #
    scope_ref["scope"] = "MODEL_TARGET"
    if ready and book:
        sector_exp = book.get("sector_exposure") or {}
        if sector_exp:
            top_sector, top_w = max(sector_exp.items(), key=lambda kv: kv[1])
            within = top_w <= eng.SECTOR_CAP_FRACTION + 1e-9
            item("largest_sector", "Largest proposed-book sector",
                 HEALTH_HEALTHY if within else HEALTH_REVIEW,
                 "%s (%.1f%%)" % (top_sector, 100.0 * top_w),
                 ("Sector exposure remains below the portfolio cap (25%)." if within else
                  "Sector exposure exceeds the 25% construction cap - review the book."))
        risk = cur.get("inputs", {}).get("risk", {})
        vols = [risk[c["ticker"]]["realized_vol_63d"] for c in book["constituents"]
                if risk.get(c["ticker"]) and risk[c["ticker"]].get("realized_vol_63d") is not None]
        if vols:
            mean_vol = sum(vols) / len(vols)
            st = HEALTH_REVIEW if mean_vol > _VOL_REVIEW_THRESHOLD else HEALTH_HEALTHY
            item("volatility", "Proposed-book mean 63d realized volatility", st,
                 _round(mean_vol, 4),
                 ("Mean realized volatility exceeds the %.2f review threshold." % _VOL_REVIEW_THRESHOLD
                  if st == HEALTH_REVIEW else
                  "Mean 63d realized volatility of the proposed Top-25 (risk overlay diagnostic)."))
        else:
            item("volatility", "Proposed-book mean 63d realized volatility", HEALTH_REVIEW,
                 "no risk data", "Owned risk stats unavailable for the proposed book.")
        low_liq = [c["ticker"] for c in book["constituents"]
                   if c.get("adv_dollar") is not None and c["adv_dollar"] < eng.MIN_ADV_DOLLAR]
        item("liquidity", "Proposed-book liquidity", HEALTH_BLOCKED if low_liq else HEALTH_HEALTHY,
             ("below $10M ADV: " + ", ".join(low_liq)) if low_liq else "all names >= $10M ADV",
             ("Liquidity requirement failed for the listed names." if low_liq else
              "Every proposed constituent meets the $10M average-dollar-volume minimum."))
        dds = [risk[c["ticker"]]["max_drawdown_252d"] for c in book["constituents"]
               if risk.get(c["ticker"]) and risk[c["ticker"]].get("max_drawdown_252d") is not None]
        item("drawdown", "Proposed-book mean 252d max drawdown", HEALTH_HEALTHY,
             _round(sum(dds) / len(dds), 4) if dds else None,
             "Mean trailing 252d max drawdown of the proposed constituents (diagnostic only).")
    else:
        item("proposed_book", "Proposed alpha target book", HEALTH_BLOCKED, "UNAVAILABLE",
             "Owned research-store inputs are unavailable; no proposed book can be computed. "
             "Refresh the owned data inputs.")

    if _fundamental_stale(ctx):
        item("fundamental_staleness", "Fundamental data staleness", HEALTH_BLOCKED,
             ctx["cur"].get("fundamental_month"),
             "Fundamental data is stale (more than one quarter behind the market date); "
             "no fundamental-led action is permitted until the owned panel refreshes.")
    return items


def _overall_health(items: list[dict]) -> str:
    statuses = {i["status"] for i in items}
    if HEALTH_BLOCKED in statuses:
        return HEALTH_BLOCKED
    if HEALTH_REVIEW in statuses:
        return HEALTH_REVIEW
    return HEALTH_HEALTHY


def _alpha_health_items(ops: dict) -> list[dict]:
    """Phase 27B.1: Portfolio Health of the ONE operational book (Alpha Paper
    Book #1) - the DEFAULT health scope of this page. Legacy CDW/HUM valuation
    health is archived reference and never populates these items."""
    items: list[dict] = []

    def item(key, label, status, value, explanation):
        items.append({"key": key, "label": label, "status": status, "value": value,
                      "explanation": explanation, "scope": "ALPHA_BOOK"})

    if not ops.get("available"):
        item("alpha_book", "Alpha Paper Book #1", HEALTH_BLOCKED, "UNAVAILABLE",
             "The canonical operational-book payload could not be loaded; operational "
             "health cannot be assessed.")
        return items
    if not ops.get("initialized"):
        item("alpha_book", "Alpha Paper Book #1", HEALTH_REVIEW, "NOT INITIALIZED",
             "The operational book is not initialized yet; cash, NAV and holdings are "
             "honestly empty until the manual token-gated initialization.")
        return items
    item("alpha_book_state", "Alpha book workflow state", HEALTH_HEALTHY,
         ops.get("current_status"),
         "Workflow state of the operational book, replayed from the append-only desk "
         "ledgers.")
    item("alpha_nav", "Alpha book NAV", HEALTH_HEALTHY, ops.get("nav"),
         "NAV of Alpha Paper Book #1 (cash %s + marked holdings), produced once by the "
         "desk ledger replay." % ops.get("cash"))
    n_target = ops.get("target_count") or 0
    n_impl = ops.get("implementation_count") or 0
    impl_pending = bool(n_target and n_impl < n_target and not (ops.get("fill_count") or 0))
    item("alpha_implementation", "Target implementation",
         HEALTH_REVIEW if impl_pending else HEALTH_HEALTHY,
         "%d of %d implemented (%s%%)" % (n_impl, n_target,
                                          ops.get("implementation_percentage")),
         ("The confirmed target is not yet executed by the operational book; a target "
          "weight is not an owned position. Initial implementation is pending."
          if impl_pending else
          "Implemented positions of the confirmed target actually held by the book."))
    marks_ready = ops.get("desk_mark_status") == "DESK_MARK_READY"
    item("alpha_desk_marks", "Desk sizing marks",
         HEALTH_HEALTHY if marks_ready else HEALTH_REVIEW,
         (ops.get("desk_mark_date") if marks_ready else
          (ops.get("desk_mark_status") or "UNAVAILABLE")),
         ("Completed owned closes are recorded through the required market date."
          if marks_ready else
          "The desk mark store cannot size the target yet - run the manual Refresh "
          "Desk Marks (paper desk) first."))
    item("alpha_pending_orders", "Pending paper orders", HEALTH_HEALTHY,
         ops.get("pending_order_count"),
         "Paper orders awaiting manual confirmation or their eligible NEXT_CLOSE fill.")
    lio = ops.get("ledger_integrity_ok")
    item("alpha_ledger_integrity", "Ledger integrity",
         HEALTH_BLOCKED if lio is False else HEALTH_HEALTHY,
         "INTACT" if lio else ("BROKEN" if lio is False else "UNKNOWN"),
         "Chain-hash verification of the append-only desk ledgers.")
    return items


# --------------------------------------------------------------------------- #
# Decision headline (Workstream A) - deterministic, no manufactured urgency
# --------------------------------------------------------------------------- #
def _decision(ctx: dict, changes: dict, health_items: list[dict],
              ops: Optional[dict] = None) -> tuple[str, str]:
    ops = ops or {"available": False}
    if not ctx["ready"]:
        return (HEADLINE_DATA_REFRESH,
                "Owned research-store inputs are unavailable; scores and the proposed book cannot "
                "be computed. Refresh the owned data inputs before making any decision.")
    if _fundamental_stale(ctx):
        return (HEADLINE_DATA_REFRESH,
                "Fundamental data is stale (panel month %s vs market %s). No fundamental-led "
                "decision is permitted until the owned panel refreshes."
                % (ctx["cur"].get("fundamental_month"), ctx["cur"].get("market_as_of_date")))
    rec = ctx["rec"] or {}
    review_due = bool(rec.get("review_due"))
    if review_due:
        if changes.get("is_initial_portfolio_proposal"):
            return (HEADLINE_REVIEW_NEW,
                    "No confirmed paper-alpha snapshot exists yet. This is the INITIAL PORTFOLIO "
                    "PROPOSAL for the primary %d-name book - review the candidates, then preview "
                    "and manually confirm the first paper snapshot." % (changes.get("n_target") or 25))
        n_changes = len(changes.get("additions") or []) + len(changes.get("removals") or [])
        if n_changes > 0:
            return (HEADLINE_REVIEW_REBALANCE,
                    "The %s review is due and the target book changed vs the last confirmed "
                    "snapshot (%d additions, %d removals). Review the rebalance, then preview and "
                    "confirm manually." % (rec.get("book_id") or "primary",
                                           len(changes.get("additions") or []),
                                           len(changes.get("removals") or [])))
        return (HEADLINE_MANUAL_CONFIRMATION,
                "The review period rolled over but the target book is unchanged vs the last "
                "confirmed snapshot. A manual confirmation keeps the paper ledger current; no "
                "portfolio change is proposed.")
    # Phase 27B.1: a confirmed-but-unimplemented target is an OPERATIONAL state, not
    # "no change required". Model target state (unchanged) and implementation state
    # (0 of N executed) are distinct - never collapse them.
    if _implementation_gap(ops):
        n_target = ops.get("target_count") or 0
        n_impl = ops.get("implementation_count") or 0
        if (ops.get("order_plan_ready")
                and ops.get("current_status") in ("ORDER_PLAN_READY",
                                                  "ORDER_PLAN_REVIEW_REQUIRED")):
            # Phase 27B.2: the deterministic executable order plan already exists.
            return (HEADLINE_ORDER_PLAN_REVIEW,
                    "The executable paper-order plan for the confirmed %d-name alpha "
                    "target is ready (sizing marks %s; the book holds %d of %d target "
                    "positions). Review the order plan, then confirm it manually to "
                    "create the dedicated alpha paper orders — paper only, no broker, "
                    "nothing fills yet. Next: REVIEW_AND_CONFIRM_ORDER_PLAN."
                    % (n_target, ops.get("desk_mark_date"), n_impl, n_target))
        if ops.get("order_plan_ready"):
            return (HEADLINE_READY_FOR_ORDER_PLAN,
                    "The %d-name alpha target is confirmed and the desk sizing marks are "
                    "valid (%s), but Alpha Paper Book #1 holds %d of %d target positions. "
                    "Generate and review the executable order plan (read-only preview, "
                    "then explicit manual confirms - paper only, no broker)."
                    % (n_target, ops.get("desk_mark_date"), n_impl, n_target))
        return (HEADLINE_IMPLEMENTATION_PENDING,
                "The %d-name alpha target is confirmed (%s) but Alpha Paper Book #1 has "
                "implemented %d of %d target positions (%.0f%%). The model target being "
                "unchanged does NOT mean the operational book owns these names. Next: "
                "%s." % (n_target, ops.get("target_market_date") or "confirmed",
                         n_impl, n_target,
                         float(ops.get("implementation_percentage") or 0.0),
                         ops.get("next_action_code") or "REFRESH_DESK"))
    blocked = [i for i in health_items if i["status"] == HEALTH_BLOCKED]
    if blocked:
        return (HEADLINE_REVIEW_RISK,
                "No model review is due, but %d health check(s) are BLOCKED: %s. Review the "
                "exceptions in the Portfolio Health panel."
                % (len(blocked), "; ".join(i["label"] for i in blocked)))
    return (HEADLINE_NO_CHANGE,
            "No sleeve review is due and no risk, eligibility or data issue requires action "
            "today. The correct action is to do nothing.")


# --------------------------------------------------------------------------- #
# Public loaders (the six read-only endpoints)
# --------------------------------------------------------------------------- #
def load_summary(*, panel_path=None, inputs_dir=None, ledger_dir=None, fast_spec_path=None) -> dict:
    ctx = _context(panel_path=panel_path, inputs_dir=inputs_dir, ledger_dir=ledger_dir,
                   fast_spec_path=fast_spec_path)
    changes = _changeset(ctx) if ctx["ready"] else {"available": False,
                                                    "is_initial_portfolio_proposal": None}
    health_items = _health_items(ctx)
    ops = _operational_book_block()
    headline, reason = _decision(ctx, changes, health_items, ops)
    rows = _action_rows(ctx)
    counts = _action_counts(rows)
    dates, date_warnings = _dates_block(ctx)
    val = ctx["valuation"]
    rec = ctx["rec"] or {}
    book = ctx["cur"]["books"]["books"].get(PRIMARY_BOOK_ID) if ctx["ready"] else None
    combined_sleeve = None
    for s in ctx["state"].get("sleeves", []):
        if s.get("sleeve_id") == mreg.SLEEVE_COMBINED:
            combined_sleeve = s
    warnings = list(ctx["cur"].get("warnings", [])) + list(val.get("warnings") or [])
    return {
        "phase": PHASE,
        "status": "PM_SUMMARY_READY" if ctx["ready"] else "PM_INPUTS_UNAVAILABLE",
        "decision_headline": headline,
        "decision_reason": reason,
        "decision_headline_vocabulary": list(ALL_HEADLINES),
        "dates": dates, "date_labels": dict(DATE_LABELS), "date_warnings": date_warnings,
        "primary_model_id": PRIMARY_MODEL_ID,
        "primary_book_id": PRIMARY_BOOK_ID,
        "model_roles": {"research_champion": "composite_sn",
                        "paper_challenger": "mom_6_1",
                        "operational_primary_ensemble": PRIMARY_MODEL_ID},
        "operating_state": ctx["state"].get("operating_state"),
        "manual_review_status": ctx["state"].get("operating_state"),
        "review_due": bool(rec.get("review_due")) if ctx["ready"] else None,
        "next_manual_review_date": (combined_sleeve or {}).get("next_manual_review_date"),
        # Phase 27B.1: the ONE operational book + its implementation state. The
        # legacy "portfolio" block below is the ARCHIVED executed paper portfolio.
        "operational_book": ops,
        "implementation_state": ({
            "target_count": ops.get("target_count"),
            "implementation_count": ops.get("implementation_count"),
            "implementation_percentage": ops.get("implementation_percentage"),
            "order_plan_generated": bool((ops.get("pending_order_count") or 0)
                                         or (ops.get("fill_count") or 0)),
            "order_plan_ready": ops.get("order_plan_ready"),
            "operational_book_status": ops.get("current_status"),
            "desk_mark_status": ops.get("desk_mark_status"),
            "desk_mark_date": ops.get("desk_mark_date"),
            "next_action_code": ops.get("next_action_code"),
            "note": ("MODEL TARGET STATE (confirmed / unchanged), IMPLEMENTATION STATE "
                     "(what Alpha Paper Book #1 has actually executed) and OPERATIONAL "
                     "HOLDINGS STATE (cash / NAV / holdings) are three distinct states - "
                     "an unchanged target does not mean the book owns the names."),
        } if ops.get("available") else {"available": False}),
        "portfolio_scope_note": ("The 'portfolio' block below is the ARCHIVED legacy "
                                 "executed paper portfolio (read-only history). The "
                                 "operational portfolio is 'operational_book'."),
        "portfolio": {
            "valuation_available": val.get("available"),
            "current_portfolio_value": val.get("current_total_value"),
            "current_cash": val.get("current_cash"),
            "current_invested_value": val.get("current_invested_value"),
            "cash_pct": val.get("cash_pct"),
            "invested_pct": val.get("invested_pct"),
            "open_paper_positions": val.get("open_position_count"),
            "available_capacity_pct": val.get("cash_pct"),
            "current_unrealized_pnl": val.get("current_unrealized_pnl"),
            "freshness_status": val.get("freshness_status"),
        },
        "proposed_target_position_count": (book or {}).get("size_actual"),
        "estimated_turnover": rec.get("estimated_turnover"),
        "estimated_transaction_cost": rec.get("estimated_transaction_cost"),
        "cost_assumption_bps": 25,
        "action_counts": counts,
        "action_display_labels": dict(ACTION_DISPLAY_LABELS),
        "changes_summary": {
            "change_basis": changes.get("change_basis"),
            "is_initial_portfolio_proposal": changes.get("is_initial_portfolio_proposal"),
            "n_additions": len(changes.get("additions") or []),
            "n_removals": len(changes.get("removals") or []),
            "n_retained": len(changes.get("retained") or []),
            "n_blocked": len(changes.get("blocked_changes") or []),
        },
        "health_status": _overall_health(health_items),
        "n_health_review": sum(1 for i in health_items if i["status"] == HEALTH_REVIEW),
        "n_health_blocked": sum(1 for i in health_items if i["status"] == HEALTH_BLOCKED),
        "snapshot_ledger": {
            "n_confirmed": (ctx["snaps"] or {}).get("n_confirmed", 0),
            "last_confirmed_snapshot_id": (ctx["prior_combined"] or {}).get("snapshot_id"),
            "last_confirmed_market_date": (ctx["prior_combined"] or {}).get("market_as_of_date"),
        },
        "fast_sleeve": {"active": bool(ctx["fast_ok"]),
                        "status": "VALIDATED" if ctx["fast_ok"] else mreg.NO_VALIDATED_FAST_ALPHA,
                        "note": "Fast sleeve is inactive and contributes no recommendation."
                        if not ctx["fast_ok"] else "Validated fast sleeve active."},
        "safety_state": "PAPER_ONLY_MANUAL_REVIEW",
        "warnings": warnings,
        "loaded_at": _iso_now(),
        **_pm_safety(),
    }


def load_actions(*, panel_path=None, inputs_dir=None, ledger_dir=None, fast_spec_path=None) -> dict:
    ctx = _context(panel_path=panel_path, inputs_dir=inputs_dir, ledger_dir=ledger_dir,
                   fast_spec_path=fast_spec_path, with_valuation=False)
    if not ctx["ready"]:
        return {"phase": PHASE, "status": "PM_INPUTS_UNAVAILABLE",
                "warnings": ctx["cur"].get("warnings", []), "actions": [],
                "counts": {a: 0 for a in ALL_ACTIONS},
                "action_display_labels": dict(ACTION_DISPLAY_LABELS),
                "loaded_at": _iso_now(), **_pm_safety()}
    rows = _action_rows(ctx)
    rec = ctx["rec"] or {}
    # Phase 27B.1: annotate each model row with whether the OPERATIONAL book
    # actually holds the name - model HOLD is target membership, not ownership.
    ops = _operational_book_block()
    op_holdings = ops.get("holdings") or {}
    for r in rows:
        r["in_operational_holdings"] = bool(op_holdings.get(r.get("ticker")))
    return {
        "phase": PHASE, "status": "PM_ACTIONS_READY",
        "review_due": bool(rec.get("review_due")),
        "book_id": rec.get("book_id"), "size": rec.get("size"),
        "counts": _action_counts(rows),
        "action_display_labels": dict(ACTION_DISPLAY_LABELS),
        "internal_vocabulary": [eng.REC_BUY, eng.REC_HOLD, eng.REC_REDUCE, eng.REC_EXIT, eng.REC_WAIT],
        "vocabulary_note": "Visible headings use portfolio-manager language; the internal "
                           "deterministic vocabulary is retained per row. Never plain BUY or SELL.",
        "hold_semantics_note": ("HOLD means the name remains in the model target book "
                                "(unchanged target membership). It does NOT mean Alpha "
                                "Paper Book #1 currently owns the security - current "
                                "target weight and current executed weight are distinct; "
                                "see in_operational_holdings per row."),
        "operational_holdings_count": ops.get("holdings_count") if ops.get("available") else None,
        "actions": rows,
        "estimated_turnover": rec.get("estimated_turnover"),
        "estimated_transaction_cost": rec.get("estimated_transaction_cost"),
        "fast_sleeve_note": "Fast sleeve is inactive and contributes no recommendation."
        if not ctx["fast_ok"] else "Validated fast sleeve active.",
        "market_as_of_date": ctx["cur"].get("market_as_of_date"),
        "fundamental_as_of_date": ctx["cur"].get("fundamental_as_of_date"),
        "warnings": ctx["cur"].get("warnings", []),
        "loaded_at": _iso_now(), **_pm_safety(),
    }


def load_changes(*, panel_path=None, inputs_dir=None, ledger_dir=None, fast_spec_path=None) -> dict:
    ctx = _context(panel_path=panel_path, inputs_dir=inputs_dir, ledger_dir=ledger_dir,
                   fast_spec_path=fast_spec_path)
    if not ctx["ready"]:
        return {"phase": PHASE, "status": "PM_INPUTS_UNAVAILABLE",
                "warnings": ctx["cur"].get("warnings", []), "loaded_at": _iso_now(), **_pm_safety()}
    changes = _changeset(ctx)
    dates, date_warnings = _dates_block(ctx)
    return {
        "phase": PHASE, "status": "PM_CHANGES_READY",
        **changes,
        "executed_vs_proposed": _executed_vs_proposed(ctx),
        "dates": dates, "date_warnings": date_warnings,
        "warnings": ctx["cur"].get("warnings", []),
        "loaded_at": _iso_now(), **_pm_safety(),
    }


def load_health(*, panel_path=None, inputs_dir=None, ledger_dir=None, fast_spec_path=None) -> dict:
    ctx = _context(panel_path=panel_path, inputs_dir=inputs_dir, ledger_dir=ledger_dir,
                   fast_spec_path=fast_spec_path)
    items = _health_items(ctx)
    dates, date_warnings = _dates_block(ctx)
    # Phase 27B.1: the DEFAULT health scope is the operational Alpha Paper Book #1;
    # the legacy/model items above stay available as scoped reference.
    ops = _operational_book_block()
    alpha_items = _alpha_health_items(ops)
    return {
        "phase": PHASE,
        "status": "PM_HEALTH_READY" if ctx["ready"] or ctx["valuation"].get("available")
        else "PM_INPUTS_UNAVAILABLE",
        "overall_status": _overall_health(items),
        "status_vocabulary": [HEALTH_HEALTHY, HEALTH_REVIEW, HEALTH_BLOCKED],
        "default_health_scope": "ALPHA_BOOK",
        "alpha_book_items": alpha_items,
        "alpha_book_health_status": _overall_health(alpha_items),
        "health_scope_note": ("Portfolio Health defaults to Alpha Paper Book #1 "
                              "(alpha_book_items). The legacy executed-portfolio items "
                              "(scope LEGACY_ARCHIVE) and the model-target diagnostics "
                              "(scope MODEL_TARGET) are reference only - legacy CDW/HUM "
                              "risk and P&L are never Alpha Book health."),
        "items": items,
        "n_healthy": sum(1 for i in items if i["status"] == HEALTH_HEALTHY),
        "n_review": sum(1 for i in items if i["status"] == HEALTH_REVIEW),
        "n_blocked": sum(1 for i in items if i["status"] == HEALTH_BLOCKED),
        "dates": dates, "date_warnings": date_warnings,
        "note": "Every non-HEALTHY item carries its explanation. No single opaque confidence "
                "score is produced anywhere in this platform.",
        "warnings": list(ctx["cur"].get("warnings", [])) + list(ctx["valuation"].get("warnings") or []),
        "loaded_at": _iso_now(), **_pm_safety(),
    }


# --------------------------------------------------------------------------- #
# Explanations (Workstream C + F) - deterministic templates over stored fields
# --------------------------------------------------------------------------- #
def _explanations(ctx: dict) -> list[dict]:
    cur, rec = ctx["cur"], ctx["rec"] or {}
    size = rec.get("size") or 25
    book = cur["books"]["books"].get(PRIMARY_BOOK_ID) or {}
    combined = cur["combined"]["combined"]
    risk = cur.get("inputs", {}).get("risk", {})
    books = cur["books"]["books"]

    def _members(bid):
        return {c["ticker"] for c in (books.get(bid) or {}).get("constituents", [])}
    fund25, mom25, comb25 = _members("composite_sn_top25"), _members("mom_6_1_top25"), \
        _members(PRIMARY_BOOK_ID)

    prior_names = set((ctx["prior_combined"] or {}).get("constituents_top25") or [])
    prior_block = {}
    if ctx["prior_snap_full"]:
        prior_block = (ctx["prior_snap_full"].get("sleeves") or {}).get(mreg.SLEEVE_COMBINED) or {}
    prior_sector_exposure = dict(prior_block.get("sector_exposure_top25") or {})
    sector_exp = book.get("sector_exposure") or {}
    executed = {p.get("ticker") for p in (ctx["valuation"].get("positions") or [])}
    combined_sleeve = None
    for s in ctx["state"].get("sleeves", []):
        if s.get("sleeve_id") == mreg.SLEEVE_COMBINED:
            combined_sleeve = s
    stale = _fundamental_stale(ctx)

    out = []
    for r in rec.get("recommendations") or []:
        tk = r.get("ticker")
        action = _pm_action(r)
        comp = r.get("component_contributions") or {}
        fund_pct, mom_pct = comp.get("fund_percentile"), comp.get("mom_percentile")
        ag = _agreement(fund_pct, mom_pct)
        c = combined.get(tk) or {}
        rk = risk.get(tk) or {}
        in_target = (r.get("target_weight") or 0.0) > 0.0
        sector = r.get("sector")
        sec_after = sector_exp.get(sector, 0.0) if sector else 0.0
        sec_before = prior_sector_exposure.get(sector, 0.0) if sector else 0.0
        ranks = r.get("model_ranks") or {}

        phrases: list[str] = []
        if in_target:
            phrases.append("Ranked in the combined Top-%d." % size)
        if ag:
            phrases.append(_AGREEMENT_PHRASE[ag])
        if sector and in_target:
            if sec_after <= eng.SECTOR_CAP_FRACTION + 1e-9:
                phrases.append("Sector exposure remains below the portfolio cap.")
            else:
                phrases.append("Candidate would increase concentration in %s." % sector)
        if action == ACTION_WATCH:
            phrases.append("Position remains inside the hold buffer.")
        if "FELL_BELOW_EXIT_BUFFER" in (r.get("reason_codes") or []):
            phrases.append("Position fell below the exit buffer.")
        if "REVIEW_NOT_DUE" in (r.get("reason_codes") or []):
            phrases.append("Review is not due; monitor only.")
        if stale:
            phrases.append("Fundamental data is stale; no action permitted.")
        if "LIQUIDITY_FILTER_FAILED" in (r.get("reason_codes") or []):
            phrases.append("Liquidity requirement failed.")

        retention = ("NEW_ENTRY" if in_target and tk not in prior_names
                     else "RETAINED" if in_target and tk in prior_names
                     else "REMOVED" if (not in_target) and tk in prior_names
                     else "NOT_IN_TARGET")
        out.append({
            "ticker": tk,
            "action": action,
            "action_label": ACTION_DISPLAY_LABELS[action],
            "agreement": ag,
            "phrases": phrases,
            "alpha_evidence": {
                "fund_percentile": fund_pct, "fund_rank": ranks.get("fundamental"),
                "mom_percentile": mom_pct, "mom_rank": ranks.get("momentum"),
                "combined_percentile": c.get("percentile"), "combined_rank": ranks.get("current"),
                "combined_score": r.get("combined_score"),
                "legs_agree": (fund_pct is not None and mom_pct is not None
                               and (fund_pct > 0.5) == (mom_pct > 0.5)),
            },
            "portfolio_fit": {
                "in_target_book": in_target,
                "retention_status": retention,
                "target_weight": r.get("target_weight"),
                "sector": sector,
                "sector_weight_before": _round(sec_before, 6),
                "sector_weight_after": _round(sec_after, 6),
                "sector_within_cap": bool(sec_after <= eng.SECTOR_CAP_FRACTION + 1e-9),
                "overlaps_executed_portfolio": tk in executed,
            },
            "risk": {
                "realized_vol_63d": rk.get("realized_vol_63d"),
                "beta_universe": rk.get("beta_universe"),
                "max_drawdown_252d": rk.get("max_drawdown_252d"),
                "adv_dollar": c.get("adv_dollar"),
                "liquidity_ok": (c.get("adv_dollar") is None
                                 or c.get("adv_dollar") >= eng.MIN_ADV_DOLLAR),
                "stale_flags": (["FUNDAMENTAL_DATA_STALE"] if stale else []),
                "risk_flags": r.get("risk_flags") or [],
                "risk_overlay_status": "DIAGNOSTIC_ONLY - the low-volatility overlay cannot "
                                       "independently create BUY or EXIT recommendations.",
            },
            "timing": {
                "review_due": bool(rec.get("review_due")),
                "next_manual_review_date": (combined_sleeve or {}).get("next_manual_review_date"),
                "signal_market_as_of_date": cur.get("market_as_of_date"),
                "fundamental_as_of_date": cur.get("fundamental_as_of_date"),
            },
            "contribution": {
                "weights": dict(eng.PRIMARY_WEIGHTS),
                "fund_contribution": _round(0.5 * fund_pct, 6) if fund_pct is not None else None,
                "mom_contribution": _round(0.5 * mom_pct, 6) if mom_pct is not None else None,
                "combined_score": r.get("combined_score"),
                "note": "Fixed 50/50 percentile-rank blend - never weight-optimized.",
            },
            "would_qualify": {
                "fundamental_top25": tk in fund25,
                "momentum_top25": tk in mom25,
                "combined_top25": tk in comb25,
            },
        })
    return out


def load_explanations(*, panel_path=None, inputs_dir=None, ledger_dir=None,
                      fast_spec_path=None) -> dict:
    ctx = _context(panel_path=panel_path, inputs_dir=inputs_dir, ledger_dir=ledger_dir,
                   fast_spec_path=fast_spec_path)
    if not ctx["ready"]:
        return {"phase": PHASE, "status": "PM_INPUTS_UNAVAILABLE", "explanations": [],
                "warnings": ctx["cur"].get("warnings", []), "loaded_at": _iso_now(), **_pm_safety()}
    return {
        "phase": PHASE, "status": "PM_EXPLANATIONS_READY",
        "explanations": _explanations(ctx),
        "agreement_vocabulary": [AGREE_BOTH_STRONG, AGREE_FUNDAMENTAL_LED,
                                 AGREE_MOMENTUM_LED, AGREE_MIXED],
        "method_note": "Deterministic templates over stored fields only - no language model, no "
                       "invented expected returns, no manufactured confidence scores.",
        "fast_sleeve_note": "Fast sleeve is inactive and contributes no recommendation."
        if not ctx["fast_ok"] else "Validated fast sleeve active.",
        "warnings": ctx["cur"].get("warnings", []),
        "loaded_at": _iso_now(), **_pm_safety(),
    }


# --------------------------------------------------------------------------- #
# Since last review (Workstream G)
# --------------------------------------------------------------------------- #
def load_since_last_review(*, panel_path=None, inputs_dir=None, ledger_dir=None,
                           fast_spec_path=None) -> dict:
    ctx = _context(panel_path=panel_path, inputs_dir=inputs_dir, ledger_dir=ledger_dir,
                   fast_spec_path=fast_spec_path)
    if not ctx["ready"]:
        return {"phase": PHASE, "status": "PM_INPUTS_UNAVAILABLE",
                "warnings": ctx["cur"].get("warnings", []), "loaded_at": _iso_now(), **_pm_safety()}
    cur, rec = ctx["cur"], ctx["rec"] or {}
    book = cur["books"]["books"].get(PRIMARY_BOOK_ID) or {}
    target_ranked = [c["ticker"] for c in book.get("constituents", [])]
    changes = _changeset(ctx)
    prior_combined = ctx["prior_combined"] or {}
    prior_names: list[str] = list(prior_combined.get("constituents_top25") or [])

    if not prior_names:
        return {
            "phase": PHASE, "status": "NO_PRIOR_CONFIRMED_ALPHA_SNAPSHOT",
            "message": "NO PRIOR CONFIRMED ALPHA SNAPSHOT. This is the initial target portfolio; "
                       "there is no unrelated historical research book to compare against.",
            "initial_target_portfolio": [
                {"ticker": c["ticker"], "rank": c["rank"], "weight": c["weight"],
                 "sector": c["sector"]} for c in book.get("constituents", [])],
            "initial_estimated_turnover": rec.get("estimated_turnover"),
            "initial_estimated_cost": rec.get("estimated_transaction_cost"),
            "cost_assumption_bps": 25,
            "next_required_manual_action": "Review the initial proposal, preview the paper "
                                           "snapshot, then confirm it manually (writes only to "
                                           "the dedicated paper-alpha snapshot ledger).",
            "loaded_at": _iso_now(), **_pm_safety(),
        }

    prior_rank = {tk: i + 1 for i, tk in enumerate(prior_names)}
    cur_rank = {tk: i + 1 for i, tk in enumerate(target_ranked)}
    movers = []
    for tk in set(prior_rank) & set(cur_rank):
        delta = prior_rank[tk] - cur_rank[tk]  # positive = moved up
        if abs(delta) >= _RANK_MOVER_THRESHOLD:
            movers.append({"ticker": tk, "prior_rank": prior_rank[tk],
                           "current_rank": cur_rank[tk], "delta": delta})
    movers.sort(key=lambda m: -abs(m["delta"]))

    prior_snap = ctx["prior_snap_full"] or {}
    prior_fps = prior_snap.get("input_fingerprints") or {}
    cur_fps = cur.get("inputs", {}).get("fingerprints") or {}
    fp_changes = sorted(k for k in set(prior_fps) | set(cur_fps)
                        if prior_fps.get(k) != cur_fps.get(k))
    prior_risks = prior_snap.get("risks") or {}
    cur_risks = ledger._snapshot_risks(cur)
    risk_changes = {k: {"before": prior_risks.get(k), "after": cur_risks.get(k)}
                    for k in cur_risks if prior_risks.get(k) != cur_risks.get(k)}
    prior_turn = prior_snap.get("estimated_turnover_primary")
    prior_cost = prior_snap.get("estimated_transaction_cost_primary")
    return {
        "phase": PHASE, "status": "PM_SINCE_LAST_REVIEW_READY",
        "prior_snapshot_id": prior_combined.get("snapshot_id"),
        "prior_snapshot_market_date": prior_combined.get("market_as_of_date"),
        "prior_confirmed_at": prior_combined.get("confirmed_at"),
        "new_entrants": changes.get("additions") or [],
        "dropped_names": changes.get("removals") or [],
        "rank_movers": movers,
        "rank_mover_threshold": _RANK_MOVER_THRESHOLD,
        "sector_allocation_changes": changes.get("sector_weight_changes") or [],
        "risk_status_changes": risk_changes,
        "data_quality_changes": {"changed_input_fingerprints": fp_changes},
        "expected_turnover_change": {"before": prior_turn,
                                     "after": rec.get("estimated_turnover"),
                                     "delta": _round((rec.get("estimated_turnover") or 0.0)
                                                     - (prior_turn or 0.0), 6)
                                     if prior_turn is not None else None},
        "estimated_cost_change": {"before": prior_cost,
                                  "after": rec.get("estimated_transaction_cost"),
                                  "delta": _round((rec.get("estimated_transaction_cost") or 0.0)
                                                  - (prior_cost or 0.0), 6)
                                  if prior_cost is not None else None},
        "loaded_at": _iso_now(), **_pm_safety(),
    }


__all__ = [
    "PHASE",
    "HEADLINE_NO_CHANGE", "HEADLINE_REVIEW_NEW", "HEADLINE_REVIEW_REBALANCE",
    "HEADLINE_REVIEW_RISK", "HEADLINE_DATA_REFRESH", "HEADLINE_MANUAL_CONFIRMATION",
    "HEADLINE_IMPLEMENTATION_PENDING", "HEADLINE_READY_FOR_ORDER_PLAN",
    "HEADLINE_ORDER_PLAN_REVIEW",
    "ALL_HEADLINES",
    "ACTION_ADD", "ACTION_HOLD", "ACTION_WATCH", "ACTION_REDUCE", "ACTION_EXIT",
    "ACTION_WAIT_BLOCKED", "ALL_ACTIONS", "ACTION_DISPLAY_LABELS",
    "AGREE_BOTH_STRONG", "AGREE_FUNDAMENTAL_LED", "AGREE_MOMENTUM_LED", "AGREE_MIXED",
    "HEALTH_HEALTHY", "HEALTH_REVIEW", "HEALTH_BLOCKED",
    "CHANGE_BASIS_INITIAL", "CHANGE_BASIS_SNAPSHOT",
    "PRIMARY_MODEL_ID", "PRIMARY_BOOK_ID",
    "load_summary", "load_actions", "load_changes", "load_health",
    "load_explanations", "load_since_last_review",
]
