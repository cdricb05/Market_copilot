"""api/alpha_book.py - Phase 27A.1 Alpha Book capital, capacity and execution policy.

Bridges the validated 25-name alpha target (the confirmed Phase 25 paper-alpha snapshot) to
the Phase 27A paper trading desk through ONE dedicated, policy-governed paper book:

    Confirm target snapshot -> Initialize Alpha Paper Book #1 -> Generate executable
    25-name order plan -> Review blocked / rounded / residual-cash detail -> Preview
    NEXT_CLOSE execution -> Confirm paper orders -> Wait for the eligible close -> Fill
    (append-only) -> Forward performance tracking.

DOMAIN SEPARATION (Workstream A/C):
    * The LEGACY paper portfolio (PostgreSQL signals / trade decisions / orders / fills /
      holdings, e.g. CDW + HUM) is NEVER read-modified here. Its five-position risk-engine
      limit (config.max_positions, engine/risk.py) continues to govern ONLY the legacy
      candidate/signal workflow. Nothing in this module writes to the database.
    * Alpha Paper Book #1 lives entirely in the Phase 27A local append-only desk ledgers
      (chain-hashed JSON). Its capacity is governed by the immutable ALPHA BOOK POLICY
      below - target 25 names, temporary rebalance capacity 30 - never by the legacy
      five-slot limit.
    * The legacy executed holdings are NEVER used as the starting holdings of the alpha
      book; the book starts from dedicated virtual capital in cash.

POLICY (Workstream B): stored append-only/versioned in ``alpha_book_policy.json`` at
initialization; once the book is initialized the active policy version is immutable -
changes require a new book or an explicit new appended version (none is implemented).

EXECUTION: the desk's single deterministic NEXT_CLOSE model (no hindsight, fills never
rewritten) - this module CREATES the dedicated alpha paper orders (PROPOSED); submission,
settlement, performance and attribution are the proven Phase 27A desk machinery operating
on the same append-only ledgers.
"""
from __future__ import annotations

import math
import re as _re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from paper_trader.api import multi_horizon_engine as eng
from paper_trader.api import multi_horizon_ledger as mhz_ledger
from paper_trader.api import multi_horizon_registry as mreg
from paper_trader.api import paper_trading_desk as desk

PHASE = "27A1"

# --------------------------------------------------------------------------- #
# Dedicated append-only alpha ledgers (same desk dir, same chain-hash scheme)
# --------------------------------------------------------------------------- #
POLICY_FILE = "alpha_book_policy.json"
RECORDS_FILE = "alpha_book_records.json"        # initialization + snapshot linkage
PLANS_FILE = "alpha_order_plans.json"           # confirmed executable order plans
STATE_FILE = "alpha_state_transitions.json"     # write-driven workflow transitions

ALPHA_LEDGER_FILES = (POLICY_FILE, RECORDS_FILE, PLANS_FILE, STATE_FILE)

ALPHA_BOOK_ID = "alpha_paper_book_1"

# Explicit manual confirmation tokens - one per mutating alpha-book action.
INIT_CONFIRM_TOKEN = "CONFIRM_ALPHA_BOOK_INITIALIZE"
PLAN_CONFIRM_TOKEN = "CONFIRM_ALPHA_BOOK_ORDER_PLAN"

# --------------------------------------------------------------------------- #
# The immutable default policy (Workstream B). Fractions are stored alongside
# the display percentages so no consumer re-derives them differently.
# --------------------------------------------------------------------------- #
DEFAULT_POLICY = {
    "book_name": "Alpha Paper Book #1",
    "book_id": ALPHA_BOOK_ID,
    "strategy": "fundamental_momentum_50_50_v1",
    "target_book": "fundamental_momentum_50_50_top25",
    "starting_virtual_capital": 100000.00,
    "currency": desk.BOOK_CURRENCY,
    "target_position_count": 25,
    "temporary_rebalance_capacity": 30,
    "target_weight_per_name_pct": 4.0,
    "maximum_position_weight_pct": 5.0,
    "maximum_sector_weight_pct": 25.0,
    "minimum_adv_usd": 10000000,
    "execution_model": desk.EXECUTION_MODEL_DEFAULT,
    "one_way_transaction_cost_bps": desk.COST_BPS_PER_SIDE,
    "review_cadence": "monthly",
    "manual_confirmation_required": True,
    "automation_enabled": False,
    "broker_enabled": False,
    "live_orders_enabled": False,
    "sizing_weight_rule": ("per-name sizing weight = min(confirmed-snapshot target weight, "
                           "maximum_position_weight); integer shares = floor(weight x capital "
                           "/ reference close)."),
    "capital_reduction_rule": ("if total estimated outflow (gross + one-way costs) exceeds "
                               "available capital, reduce ONE share at a time from the "
                               "executable buy with the largest gross notional (ties: "
                               "alphabetically first ticker) until the plan fits; a position "
                               "reduced to zero is blocked CAPITAL_INSUFFICIENT. Deterministic; "
                               "never exceeds capital."),
    "blocked_target_rule": ("an execution-blocked target is NOT replaced by an unvalidated "
                            "lower-ranked name; its allocation is held as residual cash and "
                            "reported honestly. (Construction-time sector-cap replacement "
                            "inside the validated Phase 25 engine book build is preserved "
                            "unchanged - see blocked-targets.)"),
    "position_count_rule": ("position count is governed by this validated portfolio design "
                            "and risk policy - never by recent profitability and never by "
                            "the legacy five-position signal-workflow limit."),
}

# Exhaustive blocked-target classification vocabulary (Workstream E).
BLOCK_DATA_MISSING = "DATA_MISSING"
BLOCK_PRICE_UNAVAILABLE = "PRICE_UNAVAILABLE"
BLOCK_LIQUIDITY_FAILED = "LIQUIDITY_FAILED"
BLOCK_SECTOR_LIMIT = "SECTOR_LIMIT"
BLOCK_POSITION_LIMIT = "POSITION_LIMIT"
BLOCK_STALE_DATA = "STALE_DATA"
BLOCK_INVALID_SYMBOL = "INVALID_SYMBOL"
BLOCK_ROUNDING_ZERO = "ROUNDING_ZERO"
BLOCK_CAPITAL_INSUFFICIENT = "CAPITAL_INSUFFICIENT"
BLOCK_OTHER = "OTHER_EXPLAINED"
BLOCK_CLASSES = (BLOCK_DATA_MISSING, BLOCK_PRICE_UNAVAILABLE, BLOCK_LIQUIDITY_FAILED,
                 BLOCK_SECTOR_LIMIT, BLOCK_POSITION_LIMIT, BLOCK_STALE_DATA,
                 BLOCK_INVALID_SYMBOL, BLOCK_ROUNDING_ZERO, BLOCK_CAPITAL_INSUFFICIENT,
                 BLOCK_OTHER)

#: a ticker whose own latest mark lags the store's latest completed date by more than this
#: many calendar days is STALE_DATA rather than silently sized on an old close.
STALE_MARK_CALENDAR_DAYS = 7

# Workflow states (Workstream G) - every step distinct, never collapsed.
S_NO_CONFIRMED_TARGET = "NO_CONFIRMED_TARGET"
S_TARGET_CONFIRMED = "TARGET_CONFIRMED"
S_BOOK_NOT_INITIALIZED = "BOOK_NOT_INITIALIZED"
S_BOOK_INITIALIZED = "BOOK_INITIALIZED"
S_ORDER_PLAN_READY = "ORDER_PLAN_READY"
S_ORDER_PLAN_REVIEW_REQUIRED = "ORDER_PLAN_REVIEW_REQUIRED"
S_ORDERS_CONFIRMED = "ORDERS_CONFIRMED"
S_WAITING_FOR_ELIGIBLE_CLOSE = "WAITING_FOR_ELIGIBLE_CLOSE"
S_PARTIALLY_FILLED = "PARTIALLY_FILLED"
S_FULLY_FILLED = "FULLY_FILLED"
S_FORWARD_TRACKING_ACTIVE = "FORWARD_TRACKING_ACTIVE"
S_BLOCKED = "BLOCKED"
WORKFLOW_STATES = (S_NO_CONFIRMED_TARGET, S_TARGET_CONFIRMED, S_BOOK_NOT_INITIALIZED,
                   S_BOOK_INITIALIZED, S_ORDER_PLAN_READY, S_ORDER_PLAN_REVIEW_REQUIRED,
                   S_ORDERS_CONFIRMED, S_WAITING_FOR_ELIGIBLE_CLOSE, S_PARTIALLY_FILLED,
                   S_FULLY_FILLED, S_FORWARD_TRACKING_ACTIVE, S_BLOCKED)

# Statuses returned by the endpoints.
A_OK = "ALPHA_BOOK_OK"
A_CONFIRM_REQUIRED = "ALPHA_BOOK_CONFIRM_REQUIRED"
A_NO_TARGET = "NO_CONFIRMED_TARGET"
A_NOT_INITIALIZED = "ALPHA_BOOK_NOT_INITIALIZED"
A_ALREADY_INITIALIZED = "ALPHA_BOOK_ALREADY_INITIALIZED"
A_MARKS_REQUIRED = "ALPHA_DESK_MARKS_REQUIRED"
A_DUPLICATE = "ALPHA_ORDERS_ALREADY_OPEN"
A_NO_CHANGES = "ALPHA_NO_CHANGES_REQUIRED"
A_NOT_EXECUTABLE = "ALPHA_PLAN_NOT_EXECUTABLE"

# Injection seam for tests: the canonical legacy valuation loader (DB read; degrade-only).
def _default_valuation_loader():
    from paper_trader.api import portfolio_valuation
    return portfolio_valuation.load_portfolio_valuation()


_VALUATION_LOADER = _default_valuation_loader

_VALID_SYMBOL_RE = _re.compile(r"^[A-Z0-9.\-]{1,12}$")


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def alpha_safety(performed_write: bool = False) -> dict:
    """Phase 27A.1 safety block - the desk contract plus the alpha-book guarantees."""
    s = desk.desk_safety(performed_write)
    s.update({
        "legacy_portfolio_modified": False,
        "legacy_history_rewritten": False,
        "legacy_five_slot_limit_applies_to_alpha_book": False,
        "alpha_capacity_governed_by": "alpha_book_policy",
    })
    return s


# --------------------------------------------------------------------------- #
# Policy (append-only versioned)
# --------------------------------------------------------------------------- #
def _policy_rows(sdir: Path) -> list[dict]:
    return [r for r in desk._read_ledger(sdir, POLICY_FILE) if r.get("event") == "POLICY_VERSION"]


def active_policy(desk_dir=None) -> tuple[dict, Optional[int]]:
    """(policy, version). Before initialization the immutable default is the proposed
    policy (version None); after initialization the latest appended version is active."""
    sdir = desk._desk_dir(desk_dir)
    rows = _policy_rows(sdir)
    if rows:
        last = rows[-1]
        return dict(last["policy"]), int(last["policy_version"])
    return dict(DEFAULT_POLICY), None


def load_policy(desk_dir=None) -> dict:
    policy, version = active_policy(desk_dir)
    sdir = desk._desk_dir(desk_dir)
    return {
        "status": A_OK, "phase": PHASE,
        "policy": policy,
        "policy_version": version,
        "policy_active": version is not None,
        "policy_note": ("Version %d is the active immutable policy of %s."
                        % (version, policy["book_name"])) if version is not None else
                       ("Proposed default policy - it becomes immutable version 1 when "
                        "Alpha Paper Book #1 is initialized."),
        "amendment_rule": ("The active policy is never silently changed. A future change "
                           "requires a NEW book or an explicitly appended new policy "
                           "version (none is implemented)."),
        "n_policy_versions": len(_policy_rows(sdir)),
        "ledger_integrity": verify_alpha_ledgers(desk_dir),
        **alpha_safety(),
    }


def verify_alpha_ledgers(desk_dir=None) -> dict:
    sdir = desk._desk_dir(desk_dir)
    base = desk.verify_all_ledgers(desk_dir)
    extra = [desk.verify_ledger(sdir, f) for f in ALPHA_LEDGER_FILES]
    return {"all_intact": base["all_intact"] and all(r["intact"] for r in extra),
            "desk_ledgers": base["ledgers"], "alpha_ledgers": extra}


# --------------------------------------------------------------------------- #
# Book / target lookups
# --------------------------------------------------------------------------- #
def alpha_book_record(desk_dir=None) -> Optional[dict]:
    """The desk book row of Alpha Paper Book #1 (None before initialization)."""
    sdir = desk._desk_dir(desk_dir)
    for b in desk._books(sdir):
        if b.get("book_id") == ALPHA_BOOK_ID:
            return b
    return None


def initialization_record(desk_dir=None) -> Optional[dict]:
    sdir = desk._desk_dir(desk_dir)
    for r in desk._read_ledger(sdir, RECORDS_FILE):
        if r.get("event") == "ALPHA_BOOK_INITIALIZED":
            return r
    return None


def _confirmed_target(ledger_dir=None) -> Optional[dict]:
    snap = desk._latest_confirmed_snapshot(ledger_dir)
    if snap is None:
        return None
    cons, weight, market_date = desk._snapshot_target(snap)
    if not cons:
        return None
    return {"snapshot_id": snap.get("snapshot_id"), "market_as_of_date": market_date,
            "constituents": cons, "target_weight": weight,
            "confirmed_at": snap.get("confirmed_at")}


def _engine_lookup() -> dict[str, dict]:
    """Best-effort per-ticker {sector, adv_dollar} from the current engine build plus the
    construction-time sector-capped-out list. Degrades to empty; never raises."""
    try:
        cur = eng.build_current()
        if cur.get("status") != eng.STATUS_READY:
            return {"per_ticker": {}, "sector_capped_out": [], "available": False}
        per = {}
        for tk, c in (cur.get("combined", {}).get("combined") or {}).items():
            per[tk] = {"sector": c.get("sector") or "Unknown", "adv_dollar": c.get("adv_dollar")}
        book = cur["books"]["books"].get(DEFAULT_POLICY["target_book"]) or {}
        for c in book.get("constituents", []):
            per.setdefault(c["ticker"], {"sector": c.get("sector") or "Unknown",
                                         "adv_dollar": c.get("adv_dollar")})
        return {"per_ticker": per,
                "sector_capped_out": list(book.get("sector_capped_out") or []),
                "available": True}
    except Exception:  # noqa: BLE001 - lookup is advisory; the plan degrades honestly
        return {"per_ticker": {}, "sector_capped_out": [], "available": False}


# --------------------------------------------------------------------------- #
# Initialization (Workstream G steps 1-2)
# --------------------------------------------------------------------------- #
def initialize_book(*, confirm: Optional[str] = None, desk_dir=None, ledger_dir=None,
                    today: Optional[str] = None) -> dict:
    """Create Alpha Paper Book #1: policy version 1 + desk book record + linkage record.
    Idempotent-safe: a second call performs no write and reports ALREADY_INITIALIZED."""
    if confirm != INIT_CONFIRM_TOKEN:
        return {"status": A_CONFIRM_REQUIRED, "performed_write": False,
                "message": "Initializing the alpha book requires confirm='%s'." % INIT_CONFIRM_TOKEN,
                **alpha_safety()}
    sdir = desk._desk_dir(desk_dir)
    if alpha_book_record(desk_dir) is not None or initialization_record(desk_dir) is not None:
        return {"status": A_ALREADY_INITIALIZED, "performed_write": False,
                "book_id": ALPHA_BOOK_ID,
                "message": ("Alpha Paper Book #1 is already initialized. Its policy is "
                            "immutable; a new configuration requires a new book."),
                **alpha_safety()}
    target = _confirmed_target(ledger_dir)
    if target is None:
        return {"status": A_NO_TARGET, "performed_write": False,
                "message": ("No confirmed alpha target snapshot exists. Confirm the "
                            "paper-alpha snapshot first (Portfolio Manager manual flow)."),
                **alpha_safety()}
    init_date = desk._today(today)
    policy = dict(DEFAULT_POLICY)
    weight_cap = policy["maximum_position_weight_pct"] / 100.0
    sizing_weight = min(float(target["target_weight"]), weight_cap)
    n = len(desk._books(sdir)) + 1
    book = {
        "book_id": ALPHA_BOOK_ID,
        "book_number": n,
        "display_name": policy["book_name"],
        "creation_date": init_date,
        "created_at": _iso_now(),
        "model_id": policy["strategy"],
        "model_version": (mreg.model_by_id(policy["strategy"]) or {}).get("model_version", "v1"),
        "execution_model": policy["execution_model"],
        "execution_model_doc": desk.EXECUTION_MODEL_DOC,
        "transaction_cost_bps_per_side": policy["one_way_transaction_cost_bps"],
        "transaction_cost_bps_round_trip": 2 * policy["one_way_transaction_cost_bps"],
        "review_cadence": policy["review_cadence"],
        "initial_capital": float(policy["starting_virtual_capital"]),
        "currency": policy["currency"],
        "frozen_target_weights": {tk: desk._r6(sizing_weight) for tk in target["constituents"]},
        "snapshot_id": target["snapshot_id"],
        "snapshot_market_date": target["market_as_of_date"],
        "benchmark": desk.BENCHMARK_TICKER,
        "status": "OPEN",
        "immutable_record": True,
        "alpha_book": True,
        "policy_version": 1,
        "target_book": policy["target_book"],
        "target_position_count": policy["target_position_count"],
        "temporary_rebalance_capacity": policy["temporary_rebalance_capacity"],
    }
    desk._append_ledger(sdir, POLICY_FILE,
                        [{"event": "POLICY_VERSION", "policy_version": 1,
                          "book_id": ALPHA_BOOK_ID, "policy": policy,
                          "note": "Initial immutable policy of Alpha Paper Book #1."}])
    desk._append_ledger(sdir, RECORDS_FILE,
                        [{"event": "ALPHA_BOOK_INITIALIZED", "book_id": ALPHA_BOOK_ID,
                          "initialization_date": init_date, "policy_version": 1,
                          "target_snapshot_id": target["snapshot_id"],
                          "target_snapshot_market_date": target["market_as_of_date"],
                          "starting_virtual_capital": float(policy["starting_virtual_capital"]),
                          "starting_holdings": {},
                          "starting_holdings_note": ("The book starts 100% in dedicated virtual "
                                                     "cash. Legacy executed holdings are NEVER "
                                                     "migrated into the alpha book.")}])
    desk._append_ledger(sdir, desk.BOOKS_FILE, [{"event": "BOOK_CREATED", "book": book}])
    desk._append_ledger(sdir, STATE_FILE,
                        [{"event": "STATE_TRANSITION", "book_id": ALPHA_BOOK_ID,
                          "from_state": S_BOOK_NOT_INITIALIZED, "to_state": S_BOOK_INITIALIZED,
                          "on_date": init_date,
                          "detail": "Alpha Paper Book #1 initialized (policy v1)."}])
    desk._append_ledger(sdir, desk.JOURNAL_FILE,
                        [desk._journal_core(book, "BOOK_CREATED", None,
                                            "%s initialized with %.0f %s dedicated virtual "
                                            "capital from confirmed target snapshot %s "
                                            "(policy v1: target %d names, rebalance cap %d)."
                                            % (book["display_name"], book["initial_capital"],
                                               book["currency"], target["snapshot_id"],
                                               policy["target_position_count"],
                                               policy["temporary_rebalance_capacity"]))])
    desk._append_ledger(sdir, desk.TIMELINE_FILE,
                        [desk._timeline_core(book, "ALPHA_BOOK_INITIALIZED",
                                             "%s initialized on %s (%s, %.0f %s, NEXT_CLOSE, "
                                             "%.1f bps/side, monthly review). No orders exist "
                                             "yet." % (book["display_name"], init_date,
                                                       book["model_id"], book["initial_capital"],
                                                       book["currency"],
                                                       policy["one_way_transaction_cost_bps"]))])
    return {"status": A_OK, "performed_write": True, "wrote_to_desk_ledgers_only": True,
            "book_id": ALPHA_BOOK_ID, "book": book, "policy_version": 1,
            "initialization_date": init_date,
            "target_snapshot_id": target["snapshot_id"],
            "message": ("Alpha Paper Book #1 initialized (%.0f %s, policy v1). Next: generate "
                        "the executable order plan." % (book["initial_capital"], book["currency"])),
            **alpha_safety(True)}


# --------------------------------------------------------------------------- #
# Executable order plan (Workstreams D/E/F) - pure, deterministic, read-only
# --------------------------------------------------------------------------- #
def _block(ticker, side, classification, reason, source_field, temporary, consequence,
           replacement_allowed=False):
    return {"ticker": ticker, "side": side, "classification": classification,
            "exact_reason": reason, "source_field": source_field,
            "temporary": bool(temporary), "replacement_allowed": bool(replacement_allowed),
            "replacement_rule": ("Construction-time only: the validated Phase 25 engine book "
                                 "build replaces sector-capped names with the next-ranked "
                                 "eligible name. No execution-time replacement rule exists; "
                                 "the allocation is held as residual cash."
                                 if replacement_allowed else
                                 "No validated deterministic replacement rule exists for this "
                                 "block; the allocation is held as residual cash."),
            "operational_consequence": consequence}


def build_order_plan(*, desk_dir=None, ledger_dir=None, today: Optional[str] = None) -> dict:
    """Deterministic executable order plan for Alpha Paper Book #1. NO writes, NO
    randomization, NO hindsight (sizing uses only the latest completed owned closes
    already recorded in the desk mark store)."""
    target = _confirmed_target(ledger_dir)
    if target is None:
        return {"status": A_NO_TARGET,
                "message": "No confirmed alpha target snapshot exists."}
    book = alpha_book_record(desk_dir)
    if book is None:
        return {"status": A_NOT_INITIALIZED,
                "message": "Alpha Paper Book #1 is not initialized yet."}
    policy, policy_version = active_policy(desk_dir)
    marks = desk.read_marks(desk_dir)
    latest = desk.marks_latest_date(marks)
    if latest is None:
        return {"status": A_MARKS_REQUIRED,
                "message": ("The desk mark store is empty. Run the manual desk data refresh "
                            "first (confirm='%s')." % desk.REFRESH_CONFIRM_TOKEN)}
    series = marks.get("series") or {}
    lookup = _engine_lookup()
    per_ticker_info = lookup["per_ticker"]

    sdir = desk._desk_dir(desk_dir)
    fills = desk._fills(sdir)
    cash, held = desk.book_cash_holdings(book, fills)

    cost_rate = float(policy["one_way_transaction_cost_bps"]) / 10000.0
    max_pos_w = float(policy["maximum_position_weight_pct"]) / 100.0
    max_sector_w = float(policy["maximum_sector_weight_pct"]) / 100.0
    min_adv = float(policy["minimum_adv_usd"])
    capacity = int(policy["temporary_rebalance_capacity"])
    sizing_weight = min(float(target["target_weight"]), max_pos_w)
    capital_basis = float(book["initial_capital"]) if not fills else \
        (desk.book_nav(book, fills, marks)["nav"] or float(book["initial_capital"]))

    tset = list(target["constituents"])
    rows: list[dict] = []
    blocked: list[dict] = []
    sector_w: dict[str, float] = {}
    import datetime as _dt
    latest_d = _dt.date.fromisoformat(latest)

    # -- sells first: current alpha holdings that left the target (full close-out) ----- #
    sell_proceeds = 0.0
    for tk in sorted(held):
        if tk in tset:
            continue
        qty = int(held[tk])
        at = desk._series_price_at_or_before(series.get(tk) or [], latest)
        info = per_ticker_info.get(tk) or {}
        if at is None:
            blocked.append(_block(tk, desk.SIDE_SELL, BLOCK_PRICE_UNAVAILABLE,
                                  "No owned close for %s in the desk mark store." % tk,
                                  "desk_marks.series", True,
                                  "The close-out order cannot be sized; refresh desk data."))
            continue
        gross = qty * at[1]
        est_cost = gross * cost_rate
        sell_proceeds += gross - est_cost
        rows.append({
            "ticker": tk, "side": desk.SIDE_SELL, "action": "CLOSE_POSITION",
            "target_weight": 0.0, "target_dollar_value": 0.0,
            "price_used_for_sizing": desk._r6(at[1]), "price_date": at[0],
            "quantity": qty, "gross_notional": desk._r2(gross),
            "estimated_transaction_cost": desk._r4(est_cost),
            "cash_impact": desk._r4(gross - est_cost),
            "sector": info.get("sector", "Unknown"),
            "liquidity_status": "NOT_CHECKED_FOR_CLOSE_OUT",
            "data_status": "OK", "executable": True, "block_reason": None,
        })

    available_cash = cash + sell_proceeds

    # -- buys / holds over the target, in validated rank order ------------------------- #
    n_open_after = sum(1 for tk in tset if tk in held)   # retained names
    for tk in tset:
        info = per_ticker_info.get(tk)
        sector = (info or {}).get("sector", "Unknown")
        adv = (info or {}).get("adv_dollar")
        w = sizing_weight
        target_dollar = w * capital_basis
        base = {"ticker": tk, "target_weight": desk._r6(w),
                "target_dollar_value": desk._r2(target_dollar), "sector": sector}
        if tk in held:
            sector_w[sector] = sector_w.get(sector, 0.0) + w
            rows.append({**base, "side": None, "action": "HOLD_EXISTING_POSITION",
                         "price_used_for_sizing": None, "price_date": None,
                         "quantity": int(held[tk]), "gross_notional": None,
                         "estimated_transaction_cost": 0.0, "cash_impact": 0.0,
                         "liquidity_status": "HELD", "data_status": "OK",
                         "executable": True, "block_reason": None})
            continue
        # classification gates, most fundamental first (exactly one class per block)
        if not _VALID_SYMBOL_RE.match(str(tk or "").strip().upper()):
            blocked.append(_block(tk, desk.SIDE_BUY, BLOCK_INVALID_SYMBOL,
                                  "Target symbol %r is not a valid owned-store symbol." % tk,
                                  "snapshot.constituents_top25", False,
                                  "Allocation held as cash; investigate the snapshot."))
            continue
        at = desk._series_price_at_or_before(series.get(tk) or [], latest)
        if info is None and at is None:
            blocked.append(_block(tk, desk.SIDE_BUY, BLOCK_DATA_MISSING,
                                  "%s is missing from both the engine input build and the "
                                  "desk mark store." % tk,
                                  "engine.combined + desk_marks.series", True,
                                  "Allocation held as cash until owned data covers the name."))
            continue
        if at is None:
            blocked.append(_block(tk, desk.SIDE_BUY, BLOCK_PRICE_UNAVAILABLE,
                                  "No owned completed close for %s in the desk mark store." % tk,
                                  "desk_marks.series", True,
                                  "Allocation held as cash; run the manual desk refresh."))
            continue
        mark_age = (latest_d - _dt.date.fromisoformat(at[0])).days
        if mark_age > STALE_MARK_CALENDAR_DAYS:
            blocked.append(_block(tk, desk.SIDE_BUY, BLOCK_STALE_DATA,
                                  "The latest owned close for %s (%s) lags the store's latest "
                                  "completed date (%s) by %d calendar days (max %d)."
                                  % (tk, at[0], latest, mark_age, STALE_MARK_CALENDAR_DAYS),
                                  "desk_marks.series", True,
                                  "Allocation held as cash until a fresh owned close exists."))
            continue
        if adv is not None and adv < min_adv:
            blocked.append(_block(tk, desk.SIDE_BUY, BLOCK_LIQUIDITY_FAILED,
                                  "Average dollar volume %.0f is below the policy minimum "
                                  "%.0f USD." % (adv, min_adv),
                                  "engine.combined.adv_dollar vs policy.minimum_adv_usd", True,
                                  "Allocation held as cash; the name is not bought."))
            continue
        if sector_w.get(sector, 0.0) + w > max_sector_w + 1e-9:
            blocked.append(_block(tk, desk.SIDE_BUY, BLOCK_SECTOR_LIMIT,
                                  "Adding %.1f%% of %s would push the %s sector to %.1f%% "
                                  "(policy cap %.1f%%)."
                                  % (100 * w, tk, sector,
                                     100 * (sector_w.get(sector, 0.0) + w), 100 * max_sector_w),
                                  "policy.maximum_sector_weight_pct", True,
                                  "Allocation held as cash; no unvalidated replacement."))
            continue
        if n_open_after + 1 > capacity:
            blocked.append(_block(tk, desk.SIDE_BUY, BLOCK_POSITION_LIMIT,
                                  "Opening %s would exceed the temporary rebalance capacity "
                                  "of %d holdings." % (tk, capacity),
                                  "policy.temporary_rebalance_capacity", True,
                                  "Allocation held as cash until capacity frees up."))
            continue
        qty = int(math.floor(target_dollar / at[1]))
        if qty < 1:
            blocked.append(_block(tk, desk.SIDE_BUY, BLOCK_ROUNDING_ZERO,
                                  "The %.2f USD target allocation buys zero whole shares at "
                                  "the %.2f reference close (no fractional shares)."
                                  % (target_dollar, at[1]),
                                  "integer-share sizing rule", True,
                                  "Allocation held as cash (fractional shares are not "
                                  "supported by the paper desk and are not invented)."))
            continue
        gross = qty * at[1]
        est_cost = gross * cost_rate
        sector_w[sector] = sector_w.get(sector, 0.0) + w
        n_open_after += 1
        rows.append({**base, "side": desk.SIDE_BUY, "action": "OPEN_NEW_POSITION",
                     "price_used_for_sizing": desk._r6(at[1]), "price_date": at[0],
                     "quantity": qty, "gross_notional": desk._r2(gross),
                     "estimated_transaction_cost": desk._r4(est_cost),
                     "cash_impact": desk._r4(-(gross + est_cost)),
                     "liquidity_status": ("OK" if adv is not None else
                                          "UNVERIFIED_AT_PLAN_TIME (validated at engine book "
                                          "construction)"),
                     "data_status": "OK" if info is not None else "ENGINE_LOOKUP_UNAVAILABLE",
                     "executable": True, "block_reason": None})

    # -- capital reconciliation (Workstream F): documented deterministic reduction ----- #
    reductions = []
    def _buy_rows():
        return [r for r in rows if r["side"] == desk.SIDE_BUY and r["executable"]]
    def _total_outflow():
        # raw (unrounded) outflow so the never-exceed-capital rule is exact
        return sum(r["quantity"] * r["price_used_for_sizing"] * (1.0 + cost_rate)
                   for r in _buy_rows())
    guard = 0
    while _total_outflow() > available_cash + 1e-9:
        guard += 1
        if guard > 100000:
            break
        buys = _buy_rows()
        if not buys:
            break
        victim = max(buys, key=lambda r: (r["quantity"] * r["price_used_for_sizing"],
                                          # ties: alphabetically FIRST ticker loses the share
                                          tuple(-ord(ch) for ch in r["ticker"])))
        victim["quantity"] -= 1
        reductions.append(victim["ticker"])
        if victim["quantity"] < 1:
            victim["executable"] = False
            victim["block_reason"] = BLOCK_CAPITAL_INSUFFICIENT
            blocked.append(_block(victim["ticker"], desk.SIDE_BUY, BLOCK_CAPITAL_INSUFFICIENT,
                                  "Integer-share rounding plus transaction costs left "
                                  "insufficient capital for even one share.",
                                  "capital_reduction_rule", True,
                                  "Allocation held as cash."))
    raw_outflow = _total_outflow()
    raw_gross_buys = sum(r["quantity"] * r["price_used_for_sizing"] for r in _buy_rows())
    for r in rows:
        if r["side"] == desk.SIDE_BUY and r["executable"]:
            gross = r["quantity"] * r["price_used_for_sizing"]
            est_cost = gross * cost_rate
            r["gross_notional"] = desk._r2(gross)
            r["estimated_transaction_cost"] = desk._r4(est_cost)
            r["cash_impact"] = desk._r4(-(gross + est_cost))
    rows = [r for r in rows if not (r["side"] == desk.SIDE_BUY and not r["executable"])]

    buy_rows = _buy_rows()
    sell_rows = [r for r in rows if r["side"] == desk.SIDE_SELL]
    hold_rows = [r for r in rows if r["action"] == "HOLD_EXISTING_POSITION"]
    gross_buys = raw_gross_buys
    est_costs = (raw_outflow - raw_gross_buys) + \
        sum(r["estimated_transaction_cost"] for r in sell_rows)
    residual_cash = available_cash - raw_outflow
    largest_pos = max(((r["gross_notional"] or 0.0) / capital_basis for r in buy_rows),
                      default=0.0)
    sector_weights = {s: desk._r6(w) for s, w in sorted(sector_w.items(), key=lambda kv: -kv[1])}
    largest_sector = max(sector_w.items(), key=lambda kv: kv[1]) if sector_w else (None, 0.0)
    n_rounding_zero = sum(1 for b in blocked if b["classification"] == BLOCK_ROUNDING_ZERO)

    reconciliation = {
        "starting_cash": desk._r2(cash),
        "estimated_sell_proceeds_net": desk._r2(sell_proceeds),
        "available_cash": desk._r2(available_cash),
        "gross_buy_notional": desk._r2(gross_buys),
        "estimated_transaction_costs": desk._r4(est_costs),
        "residual_cash": desk._r2(residual_cash),
        "invested_pct": desk._r4(100.0 * gross_buys / capital_basis) if capital_basis else None,
        "residual_cash_pct": desk._r4(100.0 * residual_cash / capital_basis)
        if capital_basis else None,
        "target_count": len(tset),
        "executable_count": len(buy_rows) + len(sell_rows),
        "executable_buy_count": len(buy_rows),
        "executable_sell_count": len(sell_rows),
        "held_count": len(hold_rows),
        "blocked_count": len(blocked),
        "rounded_zero_count": n_rounding_zero,
        "sector_weights": sector_weights,
        "largest_position_weight": desk._r6(largest_pos),
        "largest_position_weight_cap": desk._r6(max_pos_w),
        "largest_sector": largest_sector[0],
        "largest_sector_weight": desk._r6(largest_sector[1]),
        "largest_sector_weight_cap": desk._r6(max_sector_w),
        "share_reduction_steps": len(reductions),
        "reduction_rule": policy["capital_reduction_rule"],
        "negative_cash": bool(residual_cash < -1e-6),
    }
    assert reconciliation["negative_cash"] is False, "plan must never exceed capital"

    return {
        "status": A_OK,
        "book_id": ALPHA_BOOK_ID,
        "policy_version": policy_version,
        "target_snapshot_id": target["snapshot_id"],
        "target_snapshot_market_date": target["market_as_of_date"],
        "sizing_marks_date": latest,
        "capital_basis": desk._r2(capital_basis),
        "sizing_weight": desk._r6(sizing_weight),
        "sizing_weight_rule": policy["sizing_weight_rule"],
        "orders": rows,
        "blocked_targets": blocked,
        "reconciliation": reconciliation,
        "engine_lookup_available": lookup["available"],
        "no_hindsight_note": ("Sizing uses only completed owned closes already recorded in "
                              "the desk mark store; fills resolve later at the deterministic "
                              "NEXT_CLOSE - no randomization, no hindsight."),
    }


def load_order_plan_preview(desk_dir=None, ledger_dir=None, today=None) -> dict:
    plan = build_order_plan(desk_dir=desk_dir, ledger_dir=ledger_dir, today=today)
    review_required = bool(plan.get("status") == A_OK and
                           plan["reconciliation"]["blocked_count"] > 0)
    return {**plan,
            "plan_state": (S_ORDER_PLAN_REVIEW_REQUIRED if review_required else
                           S_ORDER_PLAN_READY) if plan.get("status") == A_OK else None,
            "read_only_preview": True,
            "confirm_required_token": PLAN_CONFIRM_TOKEN,
            **alpha_safety()}


# --------------------------------------------------------------------------- #
# Plan confirmation -> dedicated alpha paper orders (PROPOSED; desk lifecycle)
# --------------------------------------------------------------------------- #
def confirm_order_plan(*, confirm: Optional[str] = None, desk_dir=None, ledger_dir=None,
                       today: Optional[str] = None) -> dict:
    if confirm != PLAN_CONFIRM_TOKEN:
        return {"status": A_CONFIRM_REQUIRED, "performed_write": False,
                "message": "Confirming the order plan requires confirm='%s'." % PLAN_CONFIRM_TOKEN,
                **alpha_safety()}
    sdir = desk._desk_dir(desk_dir)
    plan = build_order_plan(desk_dir=desk_dir, ledger_dir=ledger_dir, today=today)
    if plan.get("status") != A_OK:
        return {**plan, "performed_write": False, **alpha_safety()}
    book = alpha_book_record(desk_dir)
    orders = desk._orders_state(sdir)
    open_orders = [o for o in orders.values() if o["status"] not in desk._TERMINAL]
    if open_orders:
        return {"status": A_DUPLICATE, "performed_write": False,
                "message": ("Open paper orders already exist. Confirm or cancel them before "
                            "confirming a new order plan."), **alpha_safety()}
    actionable = [r for r in plan["orders"] if r["side"] in (desk.SIDE_BUY, desk.SIDE_SELL)]
    if not actionable:
        return {"status": A_NO_CHANGES, "performed_write": False,
                "message": ("Holdings already match the confirmed alpha target - no paper "
                            "order is required."), **alpha_safety()}

    plan_date = desk._today(today)
    desk._append_ledger(sdir, PLANS_FILE,
                        [{"event": "ORDER_PLAN_CONFIRMED", "book_id": ALPHA_BOOK_ID,
                          "plan_date": plan_date, "policy_version": plan["policy_version"],
                          "target_snapshot_id": plan["target_snapshot_id"],
                          "sizing_marks_date": plan["sizing_marks_date"],
                          "orders": plan["orders"], "blocked_targets": plan["blocked_targets"],
                          "reconciliation": plan["reconciliation"]}])
    seq = len(orders) + 1
    order_events, journal_rows = [], []
    for r in sorted(actionable, key=lambda x: (x["side"], x["ticker"])):
        reason = ("Added because it entered the combined Top-25."
                  if r["side"] == desk.SIDE_BUY else
                  "Removed because it exited the combined Top-25.")
        order = {
            "order_id": "ord_%s_%03d_%s" % (ALPHA_BOOK_ID, seq, r["ticker"]),
            "book_id": ALPHA_BOOK_ID,
            "snapshot_id": plan["target_snapshot_id"],
            "ticker": r["ticker"],
            "side": r["side"],
            "quantity": int(r["quantity"]),
            "target_weight": r["target_weight"],
            "reference_close": r["price_used_for_sizing"],
            "reference_close_date": r["price_date"],
            "sector": r.get("sector", "Unknown"),
            "execution_model": book["execution_model"],
            "reason": reason,
            "alpha_order_plan_date": plan_date,
            "created_at": _iso_now(),
        }
        seq += 1
        order_events.append({"event": "ORDER_CREATED", "order": order})
        journal_rows.append(desk._journal_core(book, "ORDER_CREATED", r["ticker"],
                                               "%s Alpha paper order %s proposed: %s %d shares."
                                               % (reason, order["order_id"], r["side"],
                                                  int(r["quantity"]))))
    for r in plan["orders"]:
        if r["action"] == "HOLD_EXISTING_POSITION":
            journal_rows.append(desk._journal_core(book, "HOLD", r["ticker"],
                                                   "Held because it remains inside the hold "
                                                   "buffer."))
    for b in plan["blocked_targets"]:
        journal_rows.append(desk._journal_core(book, "ORDER_BLOCKED", b["ticker"],
                                               "Alpha target %s blocked (%s): %s Allocation "
                                               "held as cash." % (b["ticker"],
                                                                  b["classification"],
                                                                  b["exact_reason"])))
    desk._append_ledger(sdir, desk.ORDERS_FILE, order_events)
    desk._append_ledger(sdir, desk.JOURNAL_FILE, journal_rows)
    desk._append_ledger(sdir, desk.TIMELINE_FILE,
                        [desk._timeline_core(book, "ALPHA_ORDER_PLAN_CONFIRMED",
                                             "Executable order plan confirmed on %s: %d paper "
                                             "order(s) PROPOSED (%d blocked, residual cash "
                                             "%.2f). Manual order confirmation required next."
                                             % (plan_date, len(order_events),
                                                plan["reconciliation"]["blocked_count"],
                                                plan["reconciliation"]["residual_cash"]))])
    desk._append_ledger(sdir, STATE_FILE,
                        [{"event": "STATE_TRANSITION", "book_id": ALPHA_BOOK_ID,
                          "from_state": S_ORDER_PLAN_REVIEW_REQUIRED
                          if plan["reconciliation"]["blocked_count"] else S_ORDER_PLAN_READY,
                          "to_state": S_ORDER_PLAN_READY, "on_date": plan_date,
                          "detail": "Order plan confirmed; %d paper order(s) PROPOSED."
                                    % len(order_events)}])
    return {"status": A_OK, "performed_write": True, "wrote_to_desk_ledgers_only": True,
            "book_id": ALPHA_BOOK_ID, "plan_date": plan_date,
            "n_orders_created": len(order_events),
            "orders_created": [e["order"]["order_id"] for e in order_events],
            "n_blocked": plan["reconciliation"]["blocked_count"],
            "reconciliation": plan["reconciliation"],
            "message": ("%d dedicated alpha paper order(s) created in PROPOSED. Preview the "
                        "NEXT_CLOSE execution, then confirm the paper orders manually - "
                        "nothing fills until then." % len(order_events)),
            **alpha_safety(True)}


# --------------------------------------------------------------------------- #
# Workflow state (Workstream G) + status / capacity / blocked-target views
# --------------------------------------------------------------------------- #
def _derive_states(desk_dir=None, ledger_dir=None, today: Optional[str] = None) -> dict:
    sdir = desk._desk_dir(desk_dir)
    target = _confirmed_target(ledger_dir)
    book = alpha_book_record(desk_dir)
    integrity = verify_alpha_ledgers(desk_dir)
    orders = [o for o in desk._orders_state(sdir).values() if o.get("book_id") == ALPHA_BOOK_ID]
    fills = [f for f in desk._fills(sdir) if f.get("book_id") == ALPHA_BOOK_ID]
    perf_rows = [r["row"] for r in desk._read_ledger(sdir, desk.PERFORMANCE_FILE)
                 if r.get("row", {}).get("book_id") == ALPHA_BOOK_ID]
    marks_latest = desk.marks_latest_date(desk.read_marks(desk_dir))
    proposed = [o for o in orders if o["status"] == desk.ST_PROPOSED]
    submitted = [o for o in orders if o["status"] == desk.ST_SUBMITTED]
    open_orders = [o for o in orders if o["status"] not in desk._TERMINAL]
    tref = desk._today(today)

    target_state = S_TARGET_CONFIRMED if target else S_NO_CONFIRMED_TARGET
    book_state = S_BOOK_INITIALIZED if book else S_BOOK_NOT_INITIALIZED

    if not integrity["all_intact"]:
        current = S_BLOCKED
    elif target is None:
        current = S_NO_CONFIRMED_TARGET
    elif book is None:
        current = S_TARGET_CONFIRMED
    elif not orders:
        if marks_latest is None:
            current = S_BOOK_INITIALIZED
        else:
            plan = build_order_plan(desk_dir=desk_dir, ledger_dir=ledger_dir, today=today)
            current = (S_ORDER_PLAN_REVIEW_REQUIRED
                       if plan.get("status") == A_OK and
                       plan["reconciliation"]["blocked_count"] > 0
                       else S_ORDER_PLAN_READY if plan.get("status") == A_OK
                       else S_BOOK_INITIALIZED)
    elif proposed:
        current = S_ORDER_PLAN_READY
    elif submitted and not fills:
        appr = max((o.get("approval_date") or "") for o in submitted)
        current = S_ORDERS_CONFIRMED if tref == appr else S_WAITING_FOR_ELIGIBLE_CLOSE
    elif open_orders and fills:
        current = S_PARTIALLY_FILLED
    else:
        # every order terminal - has a NEWER confirmed target arrived since the last
        # order cycle? If so the plan states re-emerge (monthly rebalance path).
        last_snap = (orders[-1].get("snapshot_id") or "") if orders else ""
        if target["snapshot_id"] and last_snap and target["snapshot_id"] != last_snap:
            plan = build_order_plan(desk_dir=desk_dir, ledger_dir=ledger_dir, today=today)
            current = (S_ORDER_PLAN_REVIEW_REQUIRED
                       if plan.get("status") == A_OK and
                       plan["reconciliation"]["blocked_count"] > 0
                       else S_ORDER_PLAN_READY)
        elif fills:
            current = S_FORWARD_TRACKING_ACTIVE if perf_rows else S_FULLY_FILLED
        else:
            current = S_ORDER_PLAN_READY  # orders cancelled/expired without any fill

    return {"current_state": current, "target_state": target_state, "book_state": book_state,
            "state_vocabulary": list(WORKFLOW_STATES),
            "orders_pending_manual_confirmation": len(proposed),
            "orders_awaiting_fill": len(submitted),
            "n_alpha_fills": len(fills), "n_performance_rows": len(perf_rows),
            "target": target, "book": book, "marks_latest": marks_latest,
            "ledger_integrity": integrity, "_sdir": sdir}


def _next_action(states: dict) -> str:
    cur = states["current_state"]
    if cur == S_BLOCKED:
        return ("BLOCKED: an append-only ledger failed its chain-hash verification. "
                "Investigate before any further action.")
    if cur == S_NO_CONFIRMED_TARGET:
        return ("CONFIRM_TARGET_SNAPSHOT: confirm the paper-alpha target snapshot "
                "(Portfolio Manager manual flow) - the alpha book needs a confirmed "
                "25-name target first.")
    if cur == S_TARGET_CONFIRMED:
        return ("INITIALIZE_ALPHA_BOOK: initialize Alpha Paper Book #1 (dedicated "
                "100,000 USD_PAPER virtual capital, immutable policy v1).")
    if cur == S_BOOK_INITIALIZED:
        if states["marks_latest"] is None:
            return ("REFRESH_DESK: run the manual desk data refresh to record owned "
                    "completed closes - the executable order plan needs sizing prices.")
        return ("GENERATE_ORDER_PLAN: generate and review the executable order plan "
                "(read-only), then confirm it to create the alpha paper orders.")
    if cur in (S_ORDER_PLAN_READY, S_ORDER_PLAN_REVIEW_REQUIRED):
        if states["orders_pending_manual_confirmation"]:
            return ("CONFIRM_PAPER_ORDERS: preview the NEXT_CLOSE execution, then manually "
                    "confirm the PROPOSED alpha paper orders (paper desk below).")
        if cur == S_ORDER_PLAN_REVIEW_REQUIRED:
            return ("REVIEW_ORDER_PLAN: review the blocked targets, residual cash, sector "
                    "exposure and costs, then confirm the executable order plan.")
        return ("CONFIRM_ORDER_PLAN: confirm the executable order plan to create the "
                "dedicated alpha paper orders (PROPOSED; nothing fills yet).")
    if cur in (S_ORDERS_CONFIRMED, S_WAITING_FOR_ELIGIBLE_CLOSE):
        return ("REFRESH_DESK: the confirmed paper orders await the first eligible "
                "completed owned close - run the manual desk refresh on a later day.")
    if cur == S_PARTIALLY_FILLED:
        return ("REFRESH_DESK: some paper orders are filled, others still await their "
                "eligible close - run the manual desk refresh.")
    if cur == S_FULLY_FILLED:
        return ("REFRESH_DESK: fills are complete - the next manual refresh appends the "
                "first forward-performance rows.")
    return ("MONITOR: forward tracking is active. Monitor NAV, fills and attribution; "
            "the next portfolio action is the monthly review.")


def _legacy_summary() -> dict:
    """Read-only summary of the SEPARATE legacy executed paper portfolio. Degrades."""
    try:
        from paper_trader.config import get_settings
        legacy_max = int(get_settings().max_positions)
    except Exception:  # noqa: BLE001
        legacy_max = None
    try:
        v = _VALUATION_LOADER()
        positions = [p.get("ticker") for p in (v.get("positions") or []) if p.get("ticker")]
        return {"available": True, "open_positions": len(positions), "tickers": positions,
                "max_positions": legacy_max,
                "label": "LEGACY SIGNAL PORTFOLIO CAPACITY",
                "note": ("Existing executed paper positions of the legacy manual signal "
                         "workflow. They are NOT part of Alpha Paper Book #1 and are never "
                         "modified by the alpha-book workflow.")}
    except Exception:  # noqa: BLE001
        return {"available": False, "open_positions": None, "tickers": [],
                "max_positions": legacy_max,
                "label": "LEGACY SIGNAL PORTFOLIO CAPACITY",
                "note": ("Legacy portfolio valuation unavailable (database unreachable). "
                         "The legacy portfolio is separate from Alpha Paper Book #1 either "
                         "way.")}


def load_alpha_status(desk_dir=None, ledger_dir=None, today: Optional[str] = None) -> dict:
    states = _derive_states(desk_dir=desk_dir, ledger_dir=ledger_dir, today=today)
    policy, policy_version = active_policy(desk_dir)
    book = states["book"]
    init = initialization_record(desk_dir)
    sdir = desk._desk_dir(desk_dir)
    plan_summary, plan_source = None, None
    plans = [r for r in desk._read_ledger(sdir, PLANS_FILE)
             if r.get("event") == "ORDER_PLAN_CONFIRMED"]
    if plans:
        last = plans[-1]
        plan_summary = dict(last["reconciliation"])
        plan_summary["plan_date"] = last.get("plan_date")
        plan_source = "CONFIRMED_PLAN"
    elif states["current_state"] in (S_ORDER_PLAN_READY, S_ORDER_PLAN_REVIEW_REQUIRED) \
            and book is not None:
        live = build_order_plan(desk_dir=desk_dir, ledger_dir=ledger_dir, today=today)
        if live.get("status") == A_OK:
            plan_summary = live["reconciliation"]
            plan_source = "LIVE_PREVIEW"
    step_status = []
    reached = list(WORKFLOW_STATES).index(states["current_state"]) \
        if states["current_state"] in WORKFLOW_STATES else 0
    for i, s in enumerate(WORKFLOW_STATES):
        if s == S_BLOCKED:
            st = "CURRENT" if states["current_state"] == S_BLOCKED else "NOT_ACTIVE"
        elif s == states["current_state"]:
            st = "CURRENT"
        elif i < reached:
            st = "COMPLETE_OR_PASSED"
        else:
            st = "PENDING"
        step_status.append({"state": s, "status": st})
    valuation = None
    if book is not None:
        valuation = desk.book_nav(book, [f for f in desk._fills(sdir)
                                         if f.get("book_id") == ALPHA_BOOK_ID],
                                  desk.read_marks(desk_dir))
    return {
        "status": A_OK, "phase": PHASE,
        "current_state": states["current_state"],
        "target_state": states["target_state"],
        "book_state": states["book_state"],
        "workflow_states": step_status,
        "state_vocabulary": states["state_vocabulary"],
        "next_required_action": _next_action(states),
        "book": book, "book_valuation": valuation,
        "initialization": ({"initialization_date": init.get("initialization_date"),
                            "policy_version": init.get("policy_version"),
                            "target_snapshot_id": init.get("target_snapshot_id"),
                            "target_snapshot_market_date":
                                init.get("target_snapshot_market_date"),
                            "starting_virtual_capital": init.get("starting_virtual_capital")}
                           if init else None),
        "policy": policy, "policy_version": policy_version,
        "target": (states["target"] and
                   {"snapshot_id": states["target"]["snapshot_id"],
                    "market_as_of_date": states["target"]["market_as_of_date"],
                    "n_names": len(states["target"]["constituents"]),
                    "target_weight": desk._r6(states["target"]["target_weight"]),
                    "complete_portfolio_note":
                        ("These %d names are the complete validated target portfolio, not "
                         "optional ideas to select manually."
                         % len(states["target"]["constituents"]))}),
        "orders_pending_manual_confirmation": states["orders_pending_manual_confirmation"],
        "orders_awaiting_fill": states["orders_awaiting_fill"],
        "n_alpha_fills": states["n_alpha_fills"],
        "n_performance_rows": states["n_performance_rows"],
        "latest_desk_mark_date": states["marks_latest"],
        "plan_summary": plan_summary, "plan_summary_source": plan_source,
        "legacy_portfolio": _legacy_summary(),
        "legacy_separation_note": ("The legacy executed paper portfolio and Alpha Paper "
                                   "Book #1 are fully separate. Legacy holdings are never "
                                   "the starting holdings of the alpha book; the legacy "
                                   "five-position limit never governs the alpha book."),
        "capacity": _capacity_block(states, plan_summary, policy),
        "confirm_tokens": {"initialize": INIT_CONFIRM_TOKEN,
                           "order_plan": PLAN_CONFIRM_TOKEN,
                           "submit_orders": desk.EXEC_CONFIRM_TOKEN,
                           "refresh": desk.REFRESH_CONFIRM_TOKEN},
        "ledger_integrity": states["ledger_integrity"],
        **alpha_safety(),
    }


def _capacity_block(states: dict, plan_summary: Optional[dict], policy: dict) -> dict:
    legacy = _legacy_summary()
    book = states.get("book")
    holdings_count = None
    if book is not None:
        _c, held = desk.book_cash_holdings(
            book, [f for f in desk._fills(states["_sdir"])
                   if f.get("book_id") == ALPHA_BOOK_ID])
        holdings_count = len(held)
    return {
        "legacy_signal_portfolio": {
            "label": "LEGACY SIGNAL PORTFOLIO CAPACITY",
            "max_positions": legacy.get("max_positions"),
            "open_positions": legacy.get("open_positions"),
            "source": "config.max_positions via the legacy risk engine (engine/risk.py)",
            "applies_to_alpha_book": False,
            "note": ("Governs ONLY the legacy candidate/signal workflow. It does not "
                     "constrain the 25-name alpha target."),
        },
        "alpha_book": {
            "label": "ALPHA BOOK CAPACITY",
            "target_position_count": policy["target_position_count"],
            "temporary_rebalance_capacity": policy["temporary_rebalance_capacity"],
            "current_holdings_count": holdings_count,
            "executable_count": (plan_summary or {}).get("executable_count"),
            "blocked_count": (plan_summary or {}).get("blocked_count"),
            "governed_by": "alpha_book_policy (immutable once initialized)",
            "applies_to_legacy_workflow": False,
        },
    }


def load_capacity(desk_dir=None, ledger_dir=None, today: Optional[str] = None) -> dict:
    states = _derive_states(desk_dir=desk_dir, ledger_dir=ledger_dir, today=today)
    policy, _v = active_policy(desk_dir)
    plan_summary = None
    if states["book"] is not None and states["marks_latest"] is not None:
        live = build_order_plan(desk_dir=desk_dir, ledger_dir=ledger_dir, today=today)
        if live.get("status") == A_OK:
            plan_summary = live["reconciliation"]
    return {"status": A_OK, "phase": PHASE,
            **_capacity_block(states, plan_summary, policy),
            "separation_note": ("Two independent capacity domains. The legacy five-position "
                                "risk-engine limit protects only the legacy signal workflow; "
                                "Alpha Paper Book #1 targets %d positions with a temporary "
                                "rebalance capacity of %d, governed by its immutable policy."
                                % (policy["target_position_count"],
                                   policy["temporary_rebalance_capacity"])),
            **alpha_safety()}


# --------------------------------------------------------------------------- #
# Phase 27B.1 - desk-mark readiness (read-only sizing-marks gate)
# --------------------------------------------------------------------------- #
DESK_MARK_MISSING = "DESK_MARK_MISSING"
DESK_MARK_BEHIND = "DESK_MARK_BEHIND"
DESK_MARK_READY = "DESK_MARK_READY"
DESK_MARK_STATUSES = (DESK_MARK_MISSING, DESK_MARK_BEHIND, DESK_MARK_READY)


def load_desk_mark_readiness(desk_dir=None, ledger_dir=None) -> dict:
    """Read-only: can the confirmed target be SIZED from the desk mark store?

    Backs the operational header, Daily Workflow stage 1 and the order-plan gate.
    Compares the persisted desk mark date against the SAME clock-resolved latest
    completed market date the alpha-target readiness uses, and reconciles per-name
    coverage over the confirmed constituents. Writes nothing; no provider call."""
    from paper_trader.api import alpha_target as at  # lazy: at imports desk
    target = _confirmed_target(ledger_dir)
    cons = list(target["constituents"]) if target else []
    marks = desk.read_marks(desk_dir)
    series = marks.get("series") or {}
    latest = desk.marks_latest_date(marks)
    required = at.latest_completed()
    missing = [tk for tk in cons if latest is None or
               desk._series_price_at_or_before(series.get(tk) or [], latest) is None]
    priced = len(cons) - len(missing)
    if latest is None:
        mark_status = DESK_MARK_MISSING
    elif latest < required:
        mark_status = DESK_MARK_BEHIND
    else:
        mark_status = DESK_MARK_READY
    book = alpha_book_record(desk_dir)
    blockers: list[str] = []
    if target is None:
        blockers.append("NO_CONFIRMED_TARGET: no confirmed alpha target snapshot exists; "
                        "there is nothing to size.")
    if book is None:
        blockers.append("ALPHA_BOOK_NOT_INITIALIZED: Alpha Paper Book #1 is not "
                        "initialized; the order plan has no book to size for.")
    if latest is None:
        blockers.append("DESK_MARKS_MISSING: the desk mark store has no completed owned "
                        "close - run the manual desk data refresh (paper desk, "
                        "token-gated).")
    elif latest < required:
        blockers.append("DESK_MARK_DATE_BEHIND_REQUIRED: the desk mark date (%s) is behind "
                        "the latest completed market date (%s) - run the manual desk data "
                        "refresh." % (latest, required))
    if latest is not None:
        for tk in missing:
            blockers.append("TICKER_MARKS_MISSING: %s has no completed owned close at or "
                            "before %s." % (tk, latest))
    order_plan_ready = bool(target is not None and book is not None
                            and mark_status == DESK_MARK_READY and not missing)
    if target is None:
        next_action = "CONFIRM_TARGET_SNAPSHOT"
    elif book is None:
        next_action = "INITIALIZE_ALPHA_BOOK"
    elif mark_status != DESK_MARK_READY or missing:
        next_action = "REFRESH_DESK"
    else:
        next_action = "GENERATE_ORDER_PLAN"
    return {
        "status": "ALPHA_DESK_MARK_READINESS_OK", "phase": "27B1",
        "confirmed_target_ticker_count": len(cons),
        "target_tickers": cons,
        "target_snapshot_id": target.get("snapshot_id") if target else None,
        "target_market_date": target.get("market_as_of_date") if target else None,
        "priced_ticker_count": priced,
        "missing_ticker_count": len(missing),
        "missing_tickers": missing,
        "desk_mark_date": latest,
        "latest_completed_market_date": required,
        "desk_mark_status": mark_status,
        "desk_mark_status_vocabulary": list(DESK_MARK_STATUSES),
        "benchmark_priced": bool(latest and desk._series_price_at_or_before(
            series.get(desk.BENCHMARK_TICKER) or [], latest) is not None),
        "book_initialized": book is not None,
        "order_plan_ready": order_plan_ready,
        "order_plan_readiness_note": ("The executable order plan sizes every name from the "
                                      "desk mark store; readiness requires a mark date at "
                                      "the latest completed market date with every "
                                      "confirmed constituent priced."),
        "blockers": blockers,
        "next_action": next_action,
        "refresh_token": desk.REFRESH_CONFIRM_TOKEN,
        **alpha_safety(),
    }


def load_blocked_targets(desk_dir=None, ledger_dir=None, today: Optional[str] = None) -> dict:
    """Every blocked name with its exact reason, source field and consequence.
    Combines construction-time blocks (validated engine book build) with execution-time
    blocks from the current executable order plan."""
    lookup = _engine_lookup()
    construction = [
        {"ticker": tk, "classification": BLOCK_SECTOR_LIMIT,
         "stage": "TARGET_CONSTRUCTION",
         "exact_reason": ("Higher-ranked name skipped by the 25%% sector concentration cap "
                          "during the validated engine book construction."),
         "source_field": "engine.books.%s.sector_capped_out" % DEFAULT_POLICY["target_book"],
         "temporary": True, "replacement_allowed": True,
         "replacement_rule": ("VALIDATED existing rule (preserved, not invented): the engine "
                              "book build fills the vacated slot with the next-ranked "
                              "eligible name, keeping the target at 25."),
         "operational_consequence": ("Not in the 25-name target; its slot was already filled "
                                     "by the next eligible name at construction. Nothing to "
                                     "do at execution time.")}
        for tk in lookup["sector_capped_out"]]
    plan = build_order_plan(desk_dir=desk_dir, ledger_dir=ledger_dir, today=today)
    execution = []
    plan_status = plan.get("status")
    if plan_status == A_OK:
        execution = [{**b, "stage": "EXECUTION_PLAN"} for b in plan["blocked_targets"]]
    return {"status": A_OK, "phase": PHASE,
            "classification_vocabulary": list(BLOCK_CLASSES),
            "construction_blocked": construction,
            "execution_blocked": execution,
            "n_construction_blocked": len(construction),
            "n_execution_blocked": len(execution),
            "plan_status": plan_status,
            "default_rule": ("An execution-blocked target is never replaced by an "
                             "unvalidated lower-ranked name; its allocation is held as "
                             "residual cash and reported honestly."),
            "engine_lookup_available": lookup["available"],
            **alpha_safety()}


__all__ = [
    "PHASE", "ALPHA_BOOK_ID", "DEFAULT_POLICY",
    "POLICY_FILE", "RECORDS_FILE", "PLANS_FILE", "STATE_FILE", "ALPHA_LEDGER_FILES",
    "INIT_CONFIRM_TOKEN", "PLAN_CONFIRM_TOKEN",
    "WORKFLOW_STATES", "BLOCK_CLASSES",
    "alpha_safety", "active_policy", "load_policy", "verify_alpha_ledgers",
    "alpha_book_record", "initialization_record",
    "initialize_book", "build_order_plan", "load_order_plan_preview", "confirm_order_plan",
    "load_alpha_status", "load_capacity", "load_blocked_targets",
    "load_desk_mark_readiness",
    "DESK_MARK_MISSING", "DESK_MARK_BEHIND", "DESK_MARK_READY", "DESK_MARK_STATUSES",
]
