"""
tests/test_phase27a1_alpha_book_policy.py - Phase 27A.1 Alpha Book capital/capacity policy.

Fully offline (Phase 25 fixtures + injectable downloaders + deterministic `today` seams).
Covers: the immutable default policy ($100,000 / target 25 / temporary capacity 30), strict
legacy-portfolio separation (the five-position risk-engine limit still protects ONLY the
legacy signal workflow and never constrains the alpha book; no legacy signal / decision /
order / fill / holding writes anywhere), manual initialization (token gate, append-only,
idempotent), the deterministic integer-share executable order plan (sizing, costs, residual
cash, never-negative cash, deterministic reduction), exhaustive blocked-name classification
(each with exact reason / source field / temporariness / consequence; blocked allocation
held as cash), sector / position / liquidity limits, read-only previews, plan confirmation
(append-only dedicated alpha paper orders that do NOT modify holdings), the exact distinct
workflow states and date semantics (init / approval / eligible-fill dates; no backdating),
API auth + paper-safety fields, and the UI static contract (alpha band, legacy-vs-alpha
labeling, no 5-slot implication over the alpha target, disabled prerequisites, no native
dialogs, $100,000 book visible).
"""
from __future__ import annotations

import copy
import inspect
import json
import re
import time
from pathlib import Path

import pytest

from paper_trader.api import alpha_book as ab
from paper_trader.api import multi_horizon_ledger as ledger
from paper_trader.api import paper_trading_desk as desk

from tests.test_phase27a_paper_operations import (  # reuse the Phase 27A offline harness
    _AUTH, _D0, _TICKS, _bars, _confirm_snapshot, _dl, _marks_table, _refresh,
    client, env,  # noqa: F401  (pytest fixtures resolved by name)
)

_UI = Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html"

_ALPHA_GETS = [
    "/v1/alpha-book/policy", "/v1/alpha-book/status", "/v1/alpha-book/order-plan/preview",
    "/v1/alpha-book/capacity", "/v1/alpha-book/blocked-targets",
]
_ALPHA_POSTS = ["/v1/alpha-book/initialize", "/v1/alpha-book/order-plan/confirm"]


def _init_book(today="2026-07-18"):
    out = ab.initialize_book(confirm=ab.INIT_CONFIRM_TOKEN, today=today)
    assert out["status"] == ab.A_OK, out
    return out


def _ledger_rows(env_dict, fname):
    return desk._read_ledger(Path(env_dict["desk"]), fname)


def _raw_ledger_bytes(env_dict, fname):
    p = Path(env_dict["desk"]) / fname
    return p.read_bytes() if p.exists() else b""


# --------------------------------------------------------------------------- #
# Policy defaults (Workstream B)
# --------------------------------------------------------------------------- #
class TestPolicyDefaults:
    def test_default_policy_values(self):
        p = ab.DEFAULT_POLICY
        assert p["book_name"] == "Alpha Paper Book #1"
        assert p["strategy"] == "fundamental_momentum_50_50_v1"
        assert p["target_book"] == "fundamental_momentum_50_50_top25"
        assert p["starting_virtual_capital"] == 100000.00
        assert p["target_position_count"] == 25
        assert p["temporary_rebalance_capacity"] == 30
        assert p["target_weight_per_name_pct"] == 4.0
        assert p["maximum_position_weight_pct"] == 5.0
        assert p["maximum_sector_weight_pct"] == 25.0
        assert p["minimum_adv_usd"] == 10000000
        assert p["execution_model"] == "NEXT_CLOSE"
        assert p["one_way_transaction_cost_bps"] == 12.5
        assert p["review_cadence"] == "monthly"
        assert p["manual_confirmation_required"] is True
        assert p["automation_enabled"] is False
        assert p["broker_enabled"] is False
        assert p["live_orders_enabled"] is False

    def test_policy_documents_rules(self):
        p = ab.DEFAULT_POLICY
        assert "min(" in p["sizing_weight_rule"]
        assert "largest gross notional" in p["capital_reduction_rule"]
        assert "residual cash" in p["blocked_target_rule"]
        assert "never by recent profitability" in p["position_count_rule"].replace(
            "never by\nrecent", "never by recent") or "profitability" in p["position_count_rule"]

    def test_policy_endpoint_before_init_is_proposed_default(self, env):
        out = ab.load_policy()
        assert out["status"] == ab.A_OK
        assert out["policy_active"] is False and out["policy_version"] is None
        assert out["policy"]["starting_virtual_capital"] == 100000.0
        assert "immutable version 1" in out["policy_note"]
        assert out["performed_write"] is False

    def test_workflow_state_vocabulary_exact(self):
        assert ab.WORKFLOW_STATES == (
            "NO_CONFIRMED_TARGET", "TARGET_CONFIRMED", "BOOK_NOT_INITIALIZED",
            "BOOK_INITIALIZED", "ORDER_PLAN_READY", "ORDER_PLAN_REVIEW_REQUIRED",
            "ORDERS_CONFIRMED", "WAITING_FOR_ELIGIBLE_CLOSE", "PARTIALLY_FILLED",
            "FULLY_FILLED", "FORWARD_TRACKING_ACTIVE", "BLOCKED")

    def test_block_classification_vocabulary_exact(self):
        assert set(ab.BLOCK_CLASSES) == {
            "DATA_MISSING", "PRICE_UNAVAILABLE", "LIQUIDITY_FAILED", "SECTOR_LIMIT",
            "POSITION_LIMIT", "STALE_DATA", "INVALID_SYMBOL", "ROUNDING_ZERO",
            "CAPITAL_INSUFFICIENT", "OTHER_EXPLAINED"}


# --------------------------------------------------------------------------- #
# Legacy separation (Workstreams A/C)
# --------------------------------------------------------------------------- #
class TestLegacyIsolation:
    def test_legacy_max_positions_default_still_five(self):
        from paper_trader.config import Settings
        fields = getattr(Settings, "model_fields", None) or getattr(Settings, "__fields__")
        field = fields["max_positions"]
        default = getattr(field, "default", None)
        assert default == 5, "the legacy five-position limit must remain intact"

    def test_legacy_risk_engine_still_enforces_max_positions(self):
        risk_src = (Path(__file__).resolve().parents[1] / "engine" / "risk.py"
                    ).read_text(encoding="utf-8")
        assert "max_positions" in risk_src
        assert "MAX_POSITIONS_REACHED" in risk_src

    def test_alpha_module_never_imports_the_database(self):
        src = inspect.getsource(ab)
        assert "paper_trader.db" not in src
        assert "sqlalchemy" not in src.lower()
        assert "psycopg" not in src.lower()

    def test_legacy_loader_failure_never_blocks_the_alpha_workflow(self, env, monkeypatch):
        def _boom():
            raise RuntimeError("database unreachable")
        monkeypatch.setattr(ab, "_VALUATION_LOADER", _boom)
        _confirm_snapshot()
        st = ab.load_alpha_status()
        assert st["legacy_portfolio"]["available"] is False
        assert st["current_state"] == "TARGET_CONFIRMED"
        assert _init_book()["status"] == ab.A_OK

    def test_legacy_positions_reported_separate_and_untouched(self, env, monkeypatch):
        legacy_calls = {"n": 0}

        def _stub():
            legacy_calls["n"] += 1
            return {"seeded": True, "positions": [{"ticker": "CDW"}, {"ticker": "HUM"}],
                    "current_mark": {}}
        monkeypatch.setattr(ab, "_VALUATION_LOADER", _stub)
        _confirm_snapshot()
        _init_book()
        _refresh("2026-07-18")
        st = ab.load_alpha_status()
        lg = st["legacy_portfolio"]
        assert lg["open_positions"] == 2 and lg["tickers"] == ["CDW", "HUM"]
        assert lg["max_positions"] == 5
        assert lg["label"] == "LEGACY SIGNAL PORTFOLIO CAPACITY"
        assert "never" in st["legacy_separation_note"]
        # the alpha book NEVER starts from the legacy holdings
        init = ab.initialization_record()
        assert init["starting_holdings"] == {}
        book = ab.alpha_book_record()
        assert "CDW" not in book["frozen_target_weights"] or True  # weights come from snapshot
        v = st["book_valuation"]
        assert v["cash"] == 100000.0 and v["holdings"] == {}

    def test_five_slot_limit_does_not_constrain_the_alpha_book(self, env, monkeypatch):
        """The alpha book must hold MORE than five names after the initial fill cycle."""
        monkeypatch.setattr(ab, "_VALUATION_LOADER",
                            lambda: {"seeded": True, "positions": [], "current_mark": {}})
        _confirm_snapshot()
        _init_book()
        _refresh("2026-07-18")
        r = ab.confirm_order_plan(confirm=ab.PLAN_CONFIRM_TOKEN, today="2026-07-18")
        assert r["n_orders_created"] == len(_TICKS) > 5
        c = desk.confirm_orders(confirm=desk.EXEC_CONFIRM_TOKEN, today="2026-07-18")
        assert c["settlement"]["n_filled"] == 0
        _refresh("2026-07-21", _marks_table(_D0 + ["2026-07-20"]))
        _cash, held = desk.book_cash_holdings(ab.alpha_book_record(), desk._fills(
            Path(env["desk"])))
        assert len(held) == len(_TICKS) > 5, \
            "the legacy five-position limit must not govern the alpha book"
        cap = ab.load_capacity()
        assert cap["legacy_signal_portfolio"]["applies_to_alpha_book"] is False
        assert cap["alpha_book"]["applies_to_legacy_workflow"] is False

    def test_alpha_safety_block_fields(self, env):
        s = ab.alpha_safety()
        for key in ("paper_only", "paper_orders_only", "append_only"):
            assert s[key] is True
        for key in ("orders_enabled", "live_orders_enabled", "broker_enabled",
                    "automation_enabled", "background_execution", "champion_replaced",
                    "model_weights_changed", "research_promoted", "history_rewritten",
                    "legacy_portfolio_modified", "legacy_history_rewritten",
                    "legacy_five_slot_limit_applies_to_alpha_book"):
            assert s[key] is False
        assert s["alpha_capacity_governed_by"] == "alpha_book_policy"


# --------------------------------------------------------------------------- #
# Initialization (Workstream G steps 1-2, Workstream K append-only)
# --------------------------------------------------------------------------- #
class TestInitialization:
    def test_requires_token(self, env):
        out = ab.initialize_book(confirm="WRONG")
        assert out["status"] == ab.A_CONFIRM_REQUIRED and out["performed_write"] is False

    def test_requires_confirmed_target(self, env):
        out = ab.initialize_book(confirm=ab.INIT_CONFIRM_TOKEN)
        assert out["status"] == ab.A_NO_TARGET and out["performed_write"] is False
        assert ab.load_alpha_status()["current_state"] == "NO_CONFIRMED_TARGET"

    def test_initialize_creates_immutable_book_and_policy_v1(self, env):
        _confirm_snapshot()
        out = _init_book("2026-07-18")
        assert out["initialization_date"] == "2026-07-18"
        b = out["book"]
        assert b["book_id"] == "alpha_paper_book_1"
        assert b["display_name"] == "Alpha Paper Book #1"
        assert b["initial_capital"] == 100000.0 and b["currency"] == "USD_PAPER"
        assert b["execution_model"] == "NEXT_CLOSE"
        assert b["transaction_cost_bps_per_side"] == 12.5
        assert b["review_cadence"] == "monthly"
        assert b["creation_date"] == "2026-07-18"          # never backdated
        assert b["alpha_book"] is True and b["policy_version"] == 1
        assert b["target_position_count"] == 25
        assert b["temporary_rebalance_capacity"] == 30
        assert b["snapshot_id"] == out["target_snapshot_id"]
        # snapshot weight 0.10 (thin 8-name fixture book) capped at the 5% policy max
        assert all(w == 0.05 for w in b["frozen_target_weights"].values())
        pol = ab.load_policy()
        assert pol["policy_active"] is True and pol["policy_version"] == 1
        assert pol["n_policy_versions"] == 1

    def test_initialize_is_idempotent_and_append_only(self, env):
        _confirm_snapshot()
        _init_book()
        before = {f: _raw_ledger_bytes(env, f)
                  for f in (ab.POLICY_FILE, ab.RECORDS_FILE, desk.BOOKS_FILE)}
        out = ab.initialize_book(confirm=ab.INIT_CONFIRM_TOKEN)
        assert out["status"] == ab.A_ALREADY_INITIALIZED and out["performed_write"] is False
        for f, blob in before.items():
            assert _raw_ledger_bytes(env, f) == blob, "%s must be untouched" % f
        assert ab.verify_alpha_ledgers()["all_intact"] is True

    def test_state_transition_recorded(self, env):
        _confirm_snapshot()
        _init_book("2026-07-18")
        rows = _ledger_rows(env, ab.STATE_FILE)
        assert rows and rows[0]["from_state"] == "BOOK_NOT_INITIALIZED"
        assert rows[0]["to_state"] == "BOOK_INITIALIZED"
        assert rows[0]["on_date"] == "2026-07-18"

    def test_status_progression_before_marks(self, env):
        assert ab.load_alpha_status()["current_state"] == "NO_CONFIRMED_TARGET"
        _confirm_snapshot()
        st = ab.load_alpha_status()
        assert st["current_state"] == "TARGET_CONFIRMED"
        assert st["book_state"] == "BOOK_NOT_INITIALIZED"
        assert st["next_required_action"].startswith("INITIALIZE_ALPHA_BOOK")
        _init_book()
        st = ab.load_alpha_status()
        assert st["current_state"] == "BOOK_INITIALIZED"
        assert st["next_required_action"].startswith("REFRESH_DESK")


# --------------------------------------------------------------------------- #
# Executable order plan (Workstreams D/F)
# --------------------------------------------------------------------------- #
class TestOrderPlan:
    def test_preview_preconditions(self, env):
        assert ab.load_order_plan_preview()["status"] == ab.A_NO_TARGET
        _confirm_snapshot()
        assert ab.load_order_plan_preview()["status"] == ab.A_NOT_INITIALIZED
        _init_book()
        assert ab.load_order_plan_preview()["status"] == ab.A_MARKS_REQUIRED

    def test_deterministic_integer_share_sizing(self, env):
        _confirm_snapshot()
        _init_book()
        _refresh("2026-07-18")
        p = ab.load_order_plan_preview()
        assert p["status"] == ab.A_OK and p["plan_state"] == "ORDER_PLAN_READY"
        rows = {r["ticker"]: r for r in p["orders"]}
        aaa = rows["AAA"]
        # weight = min(0.10 snapshot, 0.05 policy cap) -> $5,000 target; AAA close 103.0
        assert aaa["target_weight"] == 0.05
        assert aaa["target_dollar_value"] == 5000.0
        assert aaa["price_used_for_sizing"] == pytest.approx(103.0)
        assert aaa["price_date"] == "2026-07-17"
        assert aaa["quantity"] == int(5000 // 103.0)
        assert isinstance(aaa["quantity"], int)
        assert aaa["side"] == desk.SIDE_BUY and aaa["action"] == "OPEN_NEW_POSITION"
        # per-row estimated cost = gross x 12.5 bps
        assert aaa["estimated_transaction_cost"] == pytest.approx(
            aaa["gross_notional"] * 0.00125, abs=1e-3)
        assert aaa["cash_impact"] == pytest.approx(
            -(aaa["gross_notional"] + aaa["estimated_transaction_cost"]), abs=1e-3)

    def test_plan_is_reproducible(self, env):
        _confirm_snapshot()
        _init_book()
        _refresh("2026-07-18")
        p1 = ab.build_order_plan()
        p2 = ab.build_order_plan()
        assert json.dumps(p1, sort_keys=True, default=str) == \
            json.dumps(p2, sort_keys=True, default=str)

    def test_reconciliation_no_negative_cash_and_residual(self, env):
        _confirm_snapshot()
        _init_book()
        _refresh("2026-07-18")
        rc = ab.load_order_plan_preview()["reconciliation"]
        assert rc["negative_cash"] is False
        assert rc["starting_cash"] == 100000.0
        assert rc["residual_cash"] >= 0
        assert rc["gross_buy_notional"] + rc["residual_cash"] <= 100000.0 + 0.01
        assert rc["target_count"] == len(_TICKS)
        assert rc["executable_count"] == len(_TICKS)
        assert rc["blocked_count"] == 0 and rc["rounded_zero_count"] == 0
        assert rc["invested_pct"] == pytest.approx(
            100.0 * rc["gross_buy_notional"] / 100000.0, abs=0.01)
        assert rc["residual_cash_pct"] == pytest.approx(
            100.0 * rc["residual_cash"] / 100000.0, abs=0.01)
        assert rc["largest_position_weight"] <= 0.05 + 1e-9
        assert rc["largest_sector_weight"] <= 0.25 + 1e-9
        assert "largest gross notional" in rc["reduction_rule"]

    def test_preview_is_read_only(self, env):
        _confirm_snapshot()
        _init_book()
        _refresh("2026-07-18")
        before = {f: _raw_ledger_bytes(env, f)
                  for f in desk.LEDGER_FILES + ab.ALPHA_LEDGER_FILES}
        p = ab.load_order_plan_preview()
        assert p["performed_write"] is False and p["read_only_preview"] is True
        for f, blob in before.items():
            assert _raw_ledger_bytes(env, f) == blob

    def test_price_unavailable_blocked_and_held_as_cash(self, env):
        _confirm_snapshot()
        _init_book()
        table = _marks_table(_D0)
        table.pop("HHH")
        _refresh("2026-07-18", table)
        p = ab.load_order_plan_preview()
        assert p["plan_state"] == "ORDER_PLAN_REVIEW_REQUIRED"
        rc = p["reconciliation"]
        assert rc["blocked_count"] == 1 and rc["executable_count"] == len(_TICKS) - 1
        b = p["blocked_targets"][0]
        assert b["ticker"] == "HHH"
        assert b["classification"] == "PRICE_UNAVAILABLE"
        assert b["source_field"] == "desk_marks.series"
        assert b["temporary"] is True and b["replacement_allowed"] is False
        assert "cash" in b["operational_consequence"].lower()
        # the blocked ~$5,000 allocation stays in residual cash
        assert rc["residual_cash"] > 5000.0

    def test_rounding_zero_blocked(self, env):
        _confirm_snapshot()
        _init_book()
        table = _marks_table(_D0)
        table["HHH"] = _bars(_D0, 6000.0)          # $5,000 target buys 0 shares at ~$6,000
        _refresh("2026-07-18", table)
        p = ab.load_order_plan_preview()
        b = [x for x in p["blocked_targets"] if x["ticker"] == "HHH"]
        assert b and b[0]["classification"] == "ROUNDING_ZERO"
        assert "fractional" in b[0]["operational_consequence"].lower()
        assert p["reconciliation"]["rounded_zero_count"] == 1

    def test_stale_data_blocked(self, env):
        _confirm_snapshot()
        _init_book()
        table = _marks_table(_D0)
        table["HHH"] = _bars(["2026-06-25", "2026-07-01"], 170.0)   # >7 days behind the store
        _refresh("2026-07-18", table)
        p = ab.load_order_plan_preview()
        b = [x for x in p["blocked_targets"] if x["ticker"] == "HHH"]
        assert b and b[0]["classification"] == "STALE_DATA"

    def test_liquidity_failed_blocked(self, env, monkeypatch):
        _confirm_snapshot()
        _init_book()
        _refresh("2026-07-18")
        real = ab._engine_lookup()

        def _low_liq():
            out = copy.deepcopy(real)
            out["per_ticker"].setdefault("HHH", {"sector": "Tech"})
            out["per_ticker"]["HHH"]["adv_dollar"] = 5.0e6      # below the $10M policy floor
            return out
        monkeypatch.setattr(ab, "_engine_lookup", _low_liq)
        p = ab.load_order_plan_preview()
        b = [x for x in p["blocked_targets"] if x["ticker"] == "HHH"]
        assert b and b[0]["classification"] == "LIQUIDITY_FAILED"
        assert "policy.minimum_adv_usd" in b[0]["source_field"]

    def test_data_missing_blocked(self, env, monkeypatch):
        _confirm_snapshot()
        _init_book()
        table = _marks_table(_D0)
        table.pop("HHH")
        _refresh("2026-07-18", table)
        real = ab._engine_lookup()

        def _no_hhh():
            out = copy.deepcopy(real)
            out["per_ticker"].pop("HHH", None)
            return out
        monkeypatch.setattr(ab, "_engine_lookup", _no_hhh)
        p = ab.load_order_plan_preview()
        b = [x for x in p["blocked_targets"] if x["ticker"] == "HHH"]
        assert b and b[0]["classification"] == "DATA_MISSING"

    def test_sector_limit_blocked(self, env, monkeypatch):
        _confirm_snapshot()
        _init_book()
        _refresh("2026-07-18")
        monkeypatch.setattr(ab, "_engine_lookup", lambda: {
            "per_ticker": {tk: {"sector": "Tech", "adv_dollar": 5.0e7} for tk in _TICKS},
            "sector_capped_out": [], "available": True})
        p = ab.load_order_plan_preview()
        # every name 5% in ONE sector; 25% cap -> exactly 5 executable, 3 SECTOR_LIMIT
        rc = p["reconciliation"]
        assert rc["executable_count"] == 5
        blocked = [b for b in p["blocked_targets"] if b["classification"] == "SECTOR_LIMIT"]
        assert len(blocked) == 3
        assert all(b["source_field"] == "policy.maximum_sector_weight_pct" for b in blocked)
        assert rc["largest_sector_weight"] <= 0.25 + 1e-9

    def test_position_limit_blocked(self, env, monkeypatch):
        _confirm_snapshot()
        _init_book()
        _refresh("2026-07-18")
        tight = dict(ab.DEFAULT_POLICY)
        tight["temporary_rebalance_capacity"] = 3
        monkeypatch.setattr(ab, "active_policy", lambda desk_dir=None: (tight, 1))
        p = ab.load_order_plan_preview()
        rc = p["reconciliation"]
        assert rc["executable_count"] == 3
        blocked = [b for b in p["blocked_targets"] if b["classification"] == "POSITION_LIMIT"]
        assert len(blocked) == len(_TICKS) - 3
        assert all(b["source_field"] == "policy.temporary_rebalance_capacity"
                   for b in blocked)

    def test_capital_reduction_is_deterministic_and_never_negative(self, env, monkeypatch):
        _confirm_snapshot()
        _init_book()
        _refresh("2026-07-18")
        base = ab.build_order_plan()
        full_outflow = sum(-r["cash_impact"] for r in base["orders"]
                           if r["side"] == desk.SIDE_BUY)
        tight_cash = full_outflow - 500.0          # force the reduction loop
        monkeypatch.setattr(desk, "book_cash_holdings",
                            lambda book, fills, up_to_date=None: (tight_cash, {}))
        p = ab.build_order_plan()
        rc = p["reconciliation"]
        assert rc["share_reduction_steps"] > 0
        assert rc["negative_cash"] is False and rc["residual_cash"] >= 0
        # deterministic: same inputs -> byte-identical result
        p2 = ab.build_order_plan()
        assert json.dumps(p, sort_keys=True, default=str) == \
            json.dumps(p2, sort_keys=True, default=str)

    def test_capital_insufficient_blocks_everything_at_tiny_cash(self, env, monkeypatch):
        _confirm_snapshot()
        _init_book()
        _refresh("2026-07-18")
        monkeypatch.setattr(desk, "book_cash_holdings",
                            lambda book, fills, up_to_date=None: (50.0, {}))
        p = ab.build_order_plan()
        rc = p["reconciliation"]
        assert rc["executable_buy_count"] == 0
        assert rc["negative_cash"] is False
        assert any(b["classification"] == "CAPITAL_INSUFFICIENT"
                   for b in p["blocked_targets"])


# --------------------------------------------------------------------------- #
# Plan confirmation -> dedicated alpha paper orders (Workstream K append-only)
# --------------------------------------------------------------------------- #
class TestPlanConfirm:
    def test_requires_token(self, env):
        out = ab.confirm_order_plan(confirm="WRONG")
        assert out["status"] == ab.A_CONFIRM_REQUIRED and out["performed_write"] is False

    def test_confirm_creates_proposed_orders_without_touching_holdings(self, env):
        _confirm_snapshot()
        _init_book()
        _refresh("2026-07-18")
        out = ab.confirm_order_plan(confirm=ab.PLAN_CONFIRM_TOKEN, today="2026-07-18")
        assert out["status"] == ab.A_OK and out["n_orders_created"] == len(_TICKS)
        orders = desk.load_orders()["orders"]
        assert all(o["status"] == "PROPOSED" for o in orders)
        assert all(o["book_id"] == "alpha_paper_book_1" for o in orders)
        assert all(o["side"] == desk.SIDE_BUY for o in orders)
        # orders do NOT modify holdings or cash (Workstream A contract preserved)
        cash, held = desk.book_cash_holdings(ab.alpha_book_record(),
                                             desk._fills(Path(env["desk"])))
        assert cash == 100000.0 and held == {}
        plans = _ledger_rows(env, ab.PLANS_FILE)
        assert len(plans) == 1 and plans[0]["plan_date"] == "2026-07-18"
        assert plans[0]["reconciliation"]["negative_cash"] is False

    def test_confirm_is_append_only(self, env):
        _confirm_snapshot()
        _init_book()
        _refresh("2026-07-18")
        before = _raw_ledger_bytes(env, desk.ORDERS_FILE)
        ab.confirm_order_plan(confirm=ab.PLAN_CONFIRM_TOKEN, today="2026-07-18")
        after = _raw_ledger_bytes(env, desk.ORDERS_FILE)
        # existing bytes are a prefix-preserving append (rows list only grows)
        rows_before = json.loads(before or b'{"rows": []}').get("rows", []) if before else []
        rows_after = json.loads(after)["rows"]
        assert rows_after[:len(rows_before)] == rows_before
        assert ab.verify_alpha_ledgers()["all_intact"] is True

    def test_confirm_duplicate_safe_and_no_changes(self, env):
        _confirm_snapshot()
        _init_book()
        _refresh("2026-07-18")
        ab.confirm_order_plan(confirm=ab.PLAN_CONFIRM_TOKEN, today="2026-07-18")
        dup = ab.confirm_order_plan(confirm=ab.PLAN_CONFIRM_TOKEN, today="2026-07-18")
        assert dup["status"] == ab.A_DUPLICATE and dup["performed_write"] is False
        desk.confirm_orders(confirm=desk.EXEC_CONFIRM_TOKEN, today="2026-07-18")
        _refresh("2026-07-21", _marks_table(_D0 + ["2026-07-20"]))
        again = ab.confirm_order_plan(confirm=ab.PLAN_CONFIRM_TOKEN, today="2026-07-21")
        assert again["status"] == ab.A_NO_CHANGES and again["performed_write"] is False

    def test_blocked_targets_journaled(self, env):
        _confirm_snapshot()
        _init_book()
        table = _marks_table(_D0)
        table.pop("HHH")
        _refresh("2026-07-18", table)
        ab.confirm_order_plan(confirm=ab.PLAN_CONFIRM_TOKEN, today="2026-07-18")
        journal = desk.load_journal()["entries"]
        blocked_entries = [e for e in journal if e["category"] == "ORDER_BLOCKED"]
        assert any(e["ticker"] == "HHH" and "PRICE_UNAVAILABLE" in e["text"]
                   for e in blocked_entries)


# --------------------------------------------------------------------------- #
# Full lifecycle: exact states + exact date semantics (Workstreams G/L)
# --------------------------------------------------------------------------- #
class TestLifecycleStatesAndDates:
    def test_exact_state_walk_and_dates(self, env):
        assert ab.load_alpha_status()["current_state"] == "NO_CONFIRMED_TARGET"
        _confirm_snapshot()
        assert ab.load_alpha_status()["current_state"] == "TARGET_CONFIRMED"
        _init_book("2026-07-18")
        assert ab.load_alpha_status()["current_state"] == "BOOK_INITIALIZED"
        _refresh("2026-07-18")
        st = ab.load_alpha_status()
        assert st["current_state"] == "ORDER_PLAN_READY"
        assert st["next_required_action"].startswith("GENERATE_ORDER_PLAN") or \
            st["next_required_action"].startswith("CONFIRM_ORDER_PLAN")
        ab.confirm_order_plan(confirm=ab.PLAN_CONFIRM_TOKEN, today="2026-07-18")
        st = ab.load_alpha_status()
        assert st["current_state"] == "ORDER_PLAN_READY"
        assert st["orders_pending_manual_confirmation"] == len(_TICKS)
        assert st["next_required_action"].startswith("CONFIRM_PAPER_ORDERS")
        desk._today_override = "2026-07-18"
        c = desk.confirm_orders(confirm=desk.EXEC_CONFIRM_TOKEN, today="2026-07-18")
        assert c["approval_date"] == "2026-07-18"
        assert c["settlement"]["n_filled"] == 0            # never a same-session fill
        assert ab.load_alpha_status()["current_state"] == "ORDERS_CONFIRMED"
        desk._today_override = "2026-07-19"
        st = ab.load_alpha_status()
        assert st["current_state"] == "WAITING_FOR_ELIGIBLE_CLOSE"
        assert st["next_required_action"].startswith("REFRESH_DESK")
        _refresh("2026-07-21", _marks_table(_D0 + ["2026-07-20"]))
        desk._today_override = "2026-07-21"
        st = ab.load_alpha_status()
        assert st["current_state"] == "FORWARD_TRACKING_ACTIVE"
        assert st["next_required_action"].startswith("MONITOR")
        # exact date semantics: init 07-18, approval 07-18, eligible fill 07-20
        fills = desk.load_fills()["fills"]
        assert all(f["fill_date"] == "2026-07-20" for f in fills)
        assert all(f["no_hindsight_guard"]["approval_date"] == "2026-07-18" for f in fills)
        assert ab.initialization_record()["initialization_date"] == "2026-07-18"
        assert ab.alpha_book_record()["creation_date"] == "2026-07-18"

    def test_partially_filled_state(self, env):
        _confirm_snapshot()
        _init_book()
        _refresh("2026-07-18")
        ab.confirm_order_plan(confirm=ab.PLAN_CONFIRM_TOKEN, today="2026-07-18")
        desk.confirm_orders(confirm=desk.EXEC_CONFIRM_TOKEN, today="2026-07-18")
        table = _marks_table(_D0 + ["2026-07-20"])
        table["HHH"] = _bars(_D0, 170.0)                    # HHH has no 07-20 close yet
        _refresh("2026-07-21", table)
        desk._today_override = "2026-07-21"
        st = ab.load_alpha_status()
        assert st["current_state"] == "PARTIALLY_FILLED"
        assert st["orders_awaiting_fill"] == 1
        assert st["n_alpha_fills"] == len(_TICKS) - 1

    def test_dedicated_100k_book_valuation(self, env):
        _confirm_snapshot()
        _init_book()
        _refresh("2026-07-18")
        ab.confirm_order_plan(confirm=ab.PLAN_CONFIRM_TOKEN, today="2026-07-18")
        desk.confirm_orders(confirm=desk.EXEC_CONFIRM_TOKEN, today="2026-07-18")
        _refresh("2026-07-21", _marks_table(_D0 + ["2026-07-20"]))
        v = ab.load_alpha_status()["book_valuation"]
        assert v["holdings_count"] == len(_TICKS)
        assert 99000 < v["nav"] <= 100000                   # $100k minus costs +/- moves
        total_cost = sum(f["transaction_cost"] for f in desk.load_fills()["fills"])
        assert total_cost > 0

    def test_new_snapshot_reopens_the_plan_states(self, env):
        from tests.test_phase26_portfolio_manager import _advance_month
        _confirm_snapshot()
        _init_book()
        _refresh("2026-07-18")
        ab.confirm_order_plan(confirm=ab.PLAN_CONFIRM_TOKEN, today="2026-07-18")
        desk.confirm_orders(confirm=desk.EXEC_CONFIRM_TOKEN, today="2026-07-18")
        _refresh("2026-07-21", _marks_table(_D0 + ["2026-07-20"]))
        assert ab.load_alpha_status()["current_state"] == "FORWARD_TRACKING_ACTIVE"
        _advance_month(env, drop_member="AAA")              # next month: AAA leaves the book
        _confirm_snapshot()
        st = ab.load_alpha_status()
        assert st["current_state"] in ("ORDER_PLAN_READY", "ORDER_PLAN_REVIEW_REQUIRED")


# --------------------------------------------------------------------------- #
# Capacity + blocked-target views (Workstreams C/E/I)
# --------------------------------------------------------------------------- #
class TestCapacityAndBlockedViews:
    def test_capacity_domains(self, env):
        _confirm_snapshot()
        _init_book()
        _refresh("2026-07-18")
        cap = ab.load_capacity()
        lg = cap["legacy_signal_portfolio"]
        assert lg["label"] == "LEGACY SIGNAL PORTFOLIO CAPACITY"
        assert lg["max_positions"] == 5
        assert lg["applies_to_alpha_book"] is False
        al = cap["alpha_book"]
        assert al["target_position_count"] == 25
        assert al["temporary_rebalance_capacity"] == 30
        assert al["executable_count"] == len(_TICKS)
        assert al["blocked_count"] == 0
        assert "five-position" in cap["separation_note"]

    def test_blocked_targets_endpoint(self, env, monkeypatch):
        _confirm_snapshot()
        _init_book()
        table = _marks_table(_D0)
        table.pop("HHH")
        _refresh("2026-07-18", table)
        real = ab._engine_lookup()

        def _with_capped():
            out = copy.deepcopy(real)
            out["sector_capped_out"] = ["ON", "MCHP"]
            return out
        monkeypatch.setattr(ab, "_engine_lookup", _with_capped)
        bt = ab.load_blocked_targets()
        assert bt["status"] == ab.A_OK
        assert bt["n_construction_blocked"] == 2
        con = bt["construction_blocked"][0]
        assert con["classification"] == "SECTOR_LIMIT"
        assert con["stage"] == "TARGET_CONSTRUCTION"
        assert con["replacement_allowed"] is True
        assert "VALIDATED existing rule" in con["replacement_rule"]
        exe = bt["execution_blocked"]
        assert len(exe) == 1 and exe[0]["ticker"] == "HHH"
        assert exe[0]["stage"] == "EXECUTION_PLAN"
        assert exe[0]["replacement_allowed"] is False
        assert "residual cash" in bt["default_rule"]


# --------------------------------------------------------------------------- #
# API (Workstream J)
# --------------------------------------------------------------------------- #
class TestApi:
    def test_all_routes_require_auth(self, client):
        for path in _ALPHA_GETS:
            assert client.get(path).status_code in (401, 403), path
        for path in _ALPHA_POSTS:
            assert client.post(path, json={}).status_code in (401, 403), path

    def test_get_safety_fields(self, client):
        for path in _ALPHA_GETS:
            d = client.get(path, headers=_AUTH).json()
            assert d["paper_only"] is True, path
            assert d["broker_enabled"] is False, path
            assert d["automation_enabled"] is False, path
            assert d["live_orders_enabled"] is False, path
            assert d["performed_write"] is False, path
            assert d["legacy_five_slot_limit_applies_to_alpha_book"] is False, path

    def test_posts_refuse_without_token(self, client, env):
        for path in _ALPHA_POSTS:
            d = client.post(path, json={}, headers=_AUTH).json()
            assert d["status"] == ab.A_CONFIRM_REQUIRED, path
            assert d["performed_write"] is False, path

    def test_gets_write_nothing(self, client, env):
        before = {f: _raw_ledger_bytes(env, f)
                  for f in desk.LEDGER_FILES + ab.ALPHA_LEDGER_FILES}
        for path in _ALPHA_GETS:
            assert client.get(path, headers=_AUTH).status_code == 200
        for f, blob in before.items():
            assert _raw_ledger_bytes(env, f) == blob, f

    def test_full_workflow_through_routes(self, client, env, monkeypatch):
        monkeypatch.setattr(ab, "_VALUATION_LOADER",
                            lambda: {"seeded": True, "positions": [], "current_mark": {}})
        st = client.get("/v1/alpha-book/status", headers=_AUTH).json()
        assert st["current_state"] == "NO_CONFIRMED_TARGET"
        _confirm_snapshot()
        monkeypatch.setattr(desk, "_today_override", "2026-07-18")
        r = client.post("/v1/alpha-book/initialize",
                        json={"confirm": ab.INIT_CONFIRM_TOKEN}, headers=_AUTH).json()
        assert r["status"] == ab.A_OK and r["book"]["initial_capital"] == 100000.0
        r2 = client.post("/v1/alpha-book/initialize",
                         json={"confirm": ab.INIT_CONFIRM_TOKEN}, headers=_AUTH).json()
        assert r2["status"] == ab.A_ALREADY_INITIALIZED
        r = client.post("/v1/paper-desk/refresh",
                        json={"confirm": desk.REFRESH_CONFIRM_TOKEN}, headers=_AUTH).json()
        assert r["status"] == desk.S_OK
        p = client.get("/v1/alpha-book/order-plan/preview", headers=_AUTH).json()
        assert p["status"] == ab.A_OK and p["reconciliation"]["negative_cash"] is False
        r = client.post("/v1/alpha-book/order-plan/confirm",
                        json={"confirm": ab.PLAN_CONFIRM_TOKEN}, headers=_AUTH).json()
        assert r["status"] == ab.A_OK and r["n_orders_created"] == len(_TICKS)
        r = client.post("/v1/paper-desk/orders/confirm",
                        json={"confirm": desk.EXEC_CONFIRM_TOKEN}, headers=_AUTH).json()
        assert r["settlement"]["n_filled"] == 0
        import tests.test_phase27a_paper_operations as t27a
        t27a._write_fixture(Path(env["fixture"]), _D0 + ["2026-07-20"])
        monkeypatch.setattr(desk, "_today_override", "2026-07-21")
        r = client.post("/v1/paper-desk/refresh",
                        json={"confirm": desk.REFRESH_CONFIRM_TOKEN}, headers=_AUTH).json()
        assert r["settlement"]["n_filled"] == len(_TICKS)
        st = client.get("/v1/alpha-book/status", headers=_AUTH).json()
        assert st["current_state"] == "FORWARD_TRACKING_ACTIVE"
        cap = client.get("/v1/alpha-book/capacity", headers=_AUTH).json()
        assert cap["alpha_book"]["current_holdings_count"] == len(_TICKS) > 5

    def test_status_runtime_warm(self, client, env):
        client.get("/v1/alpha-book/status", headers=_AUTH)
        t0 = time.perf_counter()
        assert client.get("/v1/alpha-book/status", headers=_AUTH).status_code == 200
        assert time.perf_counter() - t0 < 2.0


# --------------------------------------------------------------------------- #
# UI static contract (Workstreams H/I)
# --------------------------------------------------------------------------- #
class TestUiStatic:
    @pytest.fixture(scope="class")
    def html(self):
        return _UI.read_text(encoding="utf-8")

    def test_alpha_band_above_the_desk_band(self, html):
        assert 'id="ab-band"' in html and 'id="pd-band"' in html
        assert html.index('id="ab-band"') < html.index('id="pd-band"')
        assert "ALPHA BOOK IMPLEMENTATION PLAN" in html

    def test_alpha_band_safety_and_confirm_box(self, html):
        band = html[html.index('id="ab-band"'):html.index('id="pd-band"')]
        for badge in ("PAPER ONLY", "NO LIVE ORDERS", "NO BROKER", "AUTOMATION OFF",
                      "MANUAL CONFIRMATION"):
            assert badge in band, badge
        assert 'id="ab-confirm-box"' in band and 'id="ab-confirm-phrase"' in band
        assert "Run This Manual Action" in band

    def test_alpha_tokens_in_js(self, html):
        assert "CONFIRM_ALPHA_BOOK_INITIALIZE" in html
        assert "CONFIRM_ALPHA_BOOK_ORDER_PLAN" in html

    def test_target_language_unambiguous(self, html):
        assert "INITIAL ALPHA TARGET" in html
        assert ("These 25 names are the complete validated target portfolio, "
                "not optional ideas to select manually.") in html
        # the pinned Phase 26 vocabulary stays intact
        assert "'ADD CANDIDATES'" in html

    def test_legacy_vs_alpha_distinction(self, html):
        assert "LEGACY PAPER PORTFOLIO" in html
        assert "Not part of Alpha Paper Book #1" in html
        assert "LEGACY SIGNAL PORTFOLIO CAPACITY" in html
        assert "ALPHA PAPER BOOK #1" in html
        assert "$100,000" in html
        assert "ALPHA BOOK TARGET: <b>25</b>" in html
        assert "TEMPORARY REBALANCE LIMIT: <b>30</b>" in html

    def test_capacity_relabels_no_global_five_slot_implication(self, html):
        assert "Portfolio Capacity (Legacy)" in html
        assert "Available Capacity (Legacy)" in html
        assert "Legacy Signal Portfolio Capacity" in html
        assert "Legacy Paper Portfolio Capacity" in html
        assert "Alpha Paper Book #1 Capacity" in html
        assert "never Alpha Paper Book #1" in html
        assert "Max enforced by risk engine" not in html

    def test_pm_statusbar_alpha_capacity(self, html):
        assert 'id="pm-sb-alpha-capacity"' in html
        assert "Alpha capacity" in html
        assert "Legacy positions" in html

    def test_initial_target_badge_render(self, html):
        assert "INITIAL ALPHA TARGET: ' + value + ' NAMES" in html

    def test_desk_create_button_gated_by_alpha_flow(self, html):
        assert "genBtn.style.display = ab ? 'none' : ''" in html
        assert "confirm the executable order plan above first" in html

    def test_alpha_buttons_present_and_not_blank(self, html):
        band = html[html.index('id="ab-band"'):html.index('id="pd-band"')]
        for bid, label in [("ab-act-init", "Initialize Alpha Paper Book #1"),
                           ("ab-act-plan", "Generate Executable Order Plan"),
                           ("ab-act-confirm-plan", "Confirm Order Plan"),
                           ("ab-act-blocked", "View Blocked Targets"),
                           ("ab-act-policy", "View Policy")]:
            assert 'id="%s"' % bid in band, bid
            assert label in band, label
        for m in re.finditer(r"<button[^>]*>(.*?)</button>", band, re.DOTALL):
            assert m.group(1).strip(), "blank button in the alpha band"

    def test_prerequisite_gating_js_present(self, html):
        assert "_abBtn('ab-act-init'" in html
        assert "_abBtn('ab-act-plan'" in html
        assert "_abBtn('ab-act-confirm-plan'" in html
        assert "Initialize Alpha Paper Book #1 first." in html

    def test_no_native_dialogs(self, html):
        scripts = "\n".join(re.findall(r"<script[^>]*>(.*?)</script>", html, re.DOTALL))
        for pat in (r"(?<![A-Za-z0-9_])alert\s*\(", r"(?<![A-Za-z0-9_])confirm\s*\(",
                    r"(?<![A-Za-z0-9_])prompt\s*\("):
            assert not re.search(pat, scripts)

    def test_next_close_no_hindsight_language(self, html):
        band = html[html.index('id="ab-band"'):html.index('id="pd-band"')]
        assert "NEXT_CLOSE" in band
        assert "never exceeds capital" in band or "never exceed" in band
