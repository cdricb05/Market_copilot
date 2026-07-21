"""
tests/test_phase27a_paper_operations.py - Phase 27A production-like PAPER trading workflow.

Fully offline: Phase 25 owned-style CSV fixtures via env seams, the paper-alpha ledger and the
desk ledgers redirected to tmp dirs, marks supplied by an injectable/fixture downloader, and
deterministic dates via the module `today` seams.  Covers: proposal generation (confirmed
snapshot precondition), paper-order creation (sizing, sides, reference closes, blocked names,
duplicate guard), the manual approval chain (PROPOSED -> APPROVED -> SUBMITTED as append-only
events), the read-only execution preview, deterministic NEXT_CLOSE fill generation with the
no-hindsight guard (never a same-session fill), ledger append + chain-hash tamper detection,
holdings / cash / NAV updates, per-side transaction costs, forward-performance history append
(never recomputed), the rebalance (sell) path, expiry, cancellation, attribution, the operator
workflow (deterministic next required action), API auth + paper-safety fields on every payload,
warm runtime, and the UI static contract (desk band, pills, badges, no plain BUY/SELL, no
native dialogs, LAB/ADMIN clutter, six-step operator flow).
"""
from __future__ import annotations

import json
import re
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from paper_trader.api import multi_horizon_engine as eng
from paper_trader.api import multi_horizon_ledger as ledger
from paper_trader.api import multi_horizon_platform as plat
from paper_trader.api import paper_trading_desk as desk
from paper_trader.api.app import app
from paper_trader.config import get_settings

from tests.test_multi_horizon_platform import (  # reuse the Phase 25 owned-style fixtures
    _write_fund_panel, _write_mom_current, _write_risk, _write_mom_monthly, _write_sector_map,
)
from tests.test_phase26_portfolio_manager import _advance_month

_KEY = "desk-test-key"
_AUTH = {"X-API-Key": _KEY}
_UI = Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html"

_TICKS = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "GGG", "HHH"]
_D0 = ["2026-07-14", "2026-07-15", "2026-07-16", "2026-07-17"]

_DESK_GETS = [
    "/v1/paper-desk/status", "/v1/paper-desk/books", "/v1/paper-desk/orders",
    "/v1/paper-desk/fills", "/v1/paper-desk/journal", "/v1/paper-desk/timeline",
    "/v1/paper-desk/performance", "/v1/paper-desk/attribution",
    "/v1/paper-desk/execution-preview",
]
_DESK_POSTS = [
    "/v1/paper-desk/orders/generate", "/v1/paper-desk/orders/confirm",
    "/v1/paper-desk/orders/cancel", "/v1/paper-desk/refresh",
]


def _bars(dates, base):
    return [{"date": d, "adjusted_close": round(base * (1 + 0.01 * j), 4)}
            for j, d in enumerate(dates)]


def _marks_table(dates, tickers=None, drop=()):
    table = {}
    for i, tk in enumerate((tickers or _TICKS) + ["SPY"]):
        if tk in drop:
            continue
        table[tk] = _bars(dates, 100.0 + 10 * i)
    return table


def _write_fixture(path: Path, dates, drop=()):
    path.write_text(json.dumps(_marks_table(dates, drop=drop)), encoding="utf-8")


def _dl(table):
    def get(symbol, _start):
        return table.get(symbol, [])
    return get


@pytest.fixture
def env(monkeypatch, tmp_path):
    panel = tmp_path / "panel.csv"
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    led = tmp_path / "ledger"
    smap = tmp_path / "sector_map.csv"
    desk_dir = tmp_path / "desk"
    fixture = tmp_path / "marks_fixture.json"
    _write_fund_panel(panel)
    _write_mom_current(inputs / eng.CUR_MOM_FILE)
    _write_risk(inputs / eng.RISK_FILE)
    _write_mom_monthly(inputs / eng.MONTHLY_PANEL_FILE)
    _write_sector_map(smap)
    _write_fixture(fixture, _D0)
    monkeypatch.setenv(eng.PANEL_ENV, str(panel))
    monkeypatch.setenv(eng.INPUTS_ENV, str(inputs))
    monkeypatch.setenv(eng.SECTOR_MAP_ENV, str(smap))
    monkeypatch.setenv(ledger.LEDGER_DIR_ENV, str(led))
    monkeypatch.setenv(plat.FAST_SPEC_ENV, str(tmp_path / "no_fast_spec.json"))
    monkeypatch.setenv(desk.DESK_DIR_ENV, str(desk_dir))
    monkeypatch.setenv(desk.MARKS_FIXTURE_ENV, str(fixture))
    eng.clear_cache()
    plat.clear_caches()
    yield {"panel": panel, "inputs": inputs, "ledger": led, "tmp": tmp_path,
           "desk": desk_dir, "fixture": fixture}
    eng.clear_cache()
    plat.clear_caches()
    desk._today_override = None


@pytest.fixture
def client(env, monkeypatch) -> TestClient:
    monkeypatch.setenv("PAPER_TRADER_SERVICE_API_KEY", _KEY)
    monkeypatch.setenv("PAPER_TRADER_DATABASE_URL",
                       "postgresql+psycopg2://unused:unused@localhost:5432/unused")
    get_settings.cache_clear()
    with TestClient(app) as c:
        yield c


def _confirm_snapshot():
    out = ledger.confirm_snapshot(confirm=ledger.CONFIRM_TOKEN)
    assert out["status"] == ledger.STATUS_CONFIRMED
    return out


def _refresh(today, table=None):
    return desk.refresh_desk(confirm=desk.REFRESH_CONFIRM_TOKEN,
                             downloader=_dl(table or _marks_table(_D0)), today=today)


def _full_first_cycle(today_gen="2026-07-18", today_fill="2026-07-21"):
    """snapshot -> marks -> orders -> submit -> next-close fills. Returns the fill report."""
    _confirm_snapshot()
    r = _refresh(today_gen)
    assert r["status"] == desk.S_OK
    g = desk.generate_orders(confirm=desk.GEN_CONFIRM_TOKEN, today=today_gen)
    assert g["status"] == desk.S_OK, g
    c = desk.confirm_orders(confirm=desk.EXEC_CONFIRM_TOKEN, today=today_gen)
    assert c["status"] == desk.S_OK, c
    assert c["settlement"]["n_filled"] == 0          # never a same-session fill
    r2 = _refresh(today_fill, _marks_table(_D0 + ["2026-07-20"]))
    return g, c, r2


# --------------------------------------------------------------------------- #
# Proposal generation + order creation (Workstream A)
# --------------------------------------------------------------------------- #
class TestOrderCreation:
    def test_generate_requires_token(self, env):
        out = desk.generate_orders(confirm="WRONG")
        assert out["status"] == desk.S_CONFIRM_REQUIRED
        assert out["performed_write"] is False

    def test_generate_requires_confirmed_snapshot(self, env):
        out = desk.generate_orders(confirm=desk.GEN_CONFIRM_TOKEN)
        assert out["status"] == desk.S_NO_PROPOSAL

    def test_generate_requires_marks(self, env):
        _confirm_snapshot()
        out = desk.generate_orders(confirm=desk.GEN_CONFIRM_TOKEN)
        assert out["status"] == desk.S_MARKS_REQUIRED

    def test_generate_creates_book_and_proposed_orders(self, env):
        _confirm_snapshot()
        _refresh("2026-07-18")
        g = desk.generate_orders(confirm=desk.GEN_CONFIRM_TOKEN, today="2026-07-18")
        assert g["status"] == desk.S_OK
        assert g["book_created"] is True and g["book_id"] == "paper_book_1"
        assert g["n_orders_created"] == len(_TICKS)
        orders = desk.load_orders()["orders"]
        assert all(o["status"] == desk.ST_PROPOSED for o in orders)
        assert all(o["side"] == desk.SIDE_BUY for o in orders)

    def test_order_sizing_uses_reference_close(self, env):
        _confirm_snapshot()
        _refresh("2026-07-18")
        desk.generate_orders(confirm=desk.GEN_CONFIRM_TOKEN, today="2026-07-18")
        o = {x["ticker"]: x for x in desk.load_orders()["orders"]}["AAA"]
        # AAA base 100 -> close on 2026-07-17 (4th bar) = 100*1.03 = 103.0; weight 0.10 (thin book cap)
        assert o["reference_close"] == pytest.approx(103.0)
        assert o["reference_close_date"] == "2026-07-17"
        assert o["quantity"] == int(0.10 * desk.DEFAULT_INITIAL_CAPITAL // 103.0)
        assert o["target_weight"] == pytest.approx(0.10)

    def test_book_record_fields_frozen(self, env):
        _confirm_snapshot()
        _refresh("2026-07-18")
        desk.generate_orders(confirm=desk.GEN_CONFIRM_TOKEN, today="2026-07-18")
        b = desk.load_books()["books"][0]
        assert b["display_name"] == "Paper Book #1"
        assert b["model_id"] == "fundamental_momentum_50_50_v1"
        assert b["execution_model"] == "NEXT_CLOSE"
        assert b["transaction_cost_bps_per_side"] == 12.5
        assert b["review_cadence"] == "monthly"
        assert b["snapshot_id"] and b["creation_date"] == "2026-07-18"
        assert set(b["frozen_target_weights"]) == set(_TICKS)
        assert b["status"] == "OPEN" and b["immutable_record"] is True

    def test_missing_mark_blocks_that_order_only(self, env):
        _confirm_snapshot()
        table = _marks_table(_D0, drop=("HHH",))
        desk.refresh_desk(confirm=desk.REFRESH_CONFIRM_TOKEN, downloader=_dl(table),
                          today="2026-07-18")
        g = desk.generate_orders(confirm=desk.GEN_CONFIRM_TOKEN, today="2026-07-18")
        assert g["n_orders_created"] == len(_TICKS) - 1
        assert g["n_blocked"] == 1
        assert g["blocked"][0]["ticker"] == "HHH"
        assert "NO_OWNED_MARK" in g["blocked"][0]["reason"]

    def test_duplicate_generation_guarded(self, env):
        _confirm_snapshot()
        _refresh("2026-07-18")
        desk.generate_orders(confirm=desk.GEN_CONFIRM_TOKEN, today="2026-07-18")
        g2 = desk.generate_orders(confirm=desk.GEN_CONFIRM_TOKEN, today="2026-07-18")
        assert g2["status"] == desk.S_DUPLICATE
        assert g2["performed_write"] is False

    def test_no_changes_after_book_matches_target(self, env):
        _full_first_cycle()
        g = desk.generate_orders(confirm=desk.GEN_CONFIRM_TOKEN, today="2026-07-22")
        assert g["status"] == desk.S_NO_CHANGES

    def test_orders_never_plain_buy_sell(self, env):
        _confirm_snapshot()
        _refresh("2026-07-18")
        desk.generate_orders(confirm=desk.GEN_CONFIRM_TOKEN, today="2026-07-18")
        blob = json.dumps(desk.load_orders())
        assert '"BUY"' not in blob and '"SELL"' not in blob
        assert desk.SIDE_BUY in blob


# --------------------------------------------------------------------------- #
# Approval chain + execution preview (Workstreams A + C)
# --------------------------------------------------------------------------- #
class TestApprovalAndPreview:
    def test_confirm_requires_token(self, env):
        out = desk.confirm_orders(confirm="WRONG")
        assert out["status"] == desk.S_CONFIRM_REQUIRED

    def test_confirm_without_orders(self, env):
        out = desk.confirm_orders(confirm=desk.EXEC_CONFIRM_TOKEN)
        assert out["status"] == desk.S_NO_OPEN_ORDERS

    def test_transitions_are_appended_events(self, env):
        _confirm_snapshot()
        _refresh("2026-07-18")
        desk.generate_orders(confirm=desk.GEN_CONFIRM_TOKEN, today="2026-07-18")
        c = desk.confirm_orders(confirm=desk.EXEC_CONFIRM_TOKEN, today="2026-07-18")
        assert c["n_submitted"] == len(_TICKS)
        assert c["approval_date"] == "2026-07-18"
        assert c["marks_latest_at_approval"] == "2026-07-17"
        orders = desk.load_orders()["orders"]
        for o in orders:
            assert o["status"] == desk.ST_SUBMITTED
            chain = [h["to_status"] for h in o["history"]]
            assert chain == [desk.ST_PROPOSED, desk.ST_APPROVED, desk.ST_SUBMITTED]

    def test_execution_preview_is_read_only(self, env):
        _confirm_snapshot()
        _refresh("2026-07-18")
        desk.generate_orders(confirm=desk.GEN_CONFIRM_TOKEN, today="2026-07-18")
        before = (env["desk"] / desk.ORDERS_FILE).read_text(encoding="utf-8")
        p = desk.load_execution_preview()
        assert p["performed_write"] is False and p["read_only_preview"] is True
        assert p["n_open_orders"] == len(_TICKS)
        assert p["indicative_marks_date"] == "2026-07-17"
        assert p["estimated_total_transaction_cost"] > 0
        assert "INDICATIVE" in p["note"]
        assert (env["desk"] / desk.ORDERS_FILE).read_text(encoding="utf-8") == before

    def test_cancel_open_orders(self, env):
        _confirm_snapshot()
        _refresh("2026-07-18")
        desk.generate_orders(confirm=desk.GEN_CONFIRM_TOKEN, today="2026-07-18")
        out = desk.cancel_orders(confirm=desk.CANCEL_CONFIRM_TOKEN)
        assert out["status"] == desk.S_OK and out["n_cancelled"] == len(_TICKS)
        orders = desk.load_orders()["orders"]
        assert all(o["status"] == desk.ST_CANCELLED for o in orders)
        assert desk.load_fills()["n_fills"] == 0


# --------------------------------------------------------------------------- #
# Fills (Workstreams B + C): deterministic NEXT_CLOSE, no hindsight, immutable
# --------------------------------------------------------------------------- #
class TestFills:
    def test_no_same_session_fill(self, env):
        _confirm_snapshot()
        _refresh("2026-07-18")
        desk.generate_orders(confirm=desk.GEN_CONFIRM_TOKEN, today="2026-07-18")
        c = desk.confirm_orders(confirm=desk.EXEC_CONFIRM_TOKEN, today="2026-07-18")
        assert c["settlement"]["n_filled"] == 0
        assert desk.load_status()["order_counts"][desk.ST_SUBMITTED] == len(_TICKS)

    def test_next_close_fill_after_refresh(self, env):
        _g, _c, r2 = _full_first_cycle()
        assert r2["settlement"]["n_filled"] == len(_TICKS)
        fills = desk.load_fills()["fills"]
        assert all(f["fill_date"] == "2026-07-20" for f in fills)
        assert all(f["execution_model"] == "NEXT_CLOSE" for f in fills)
        assert all(f["immutable"] is True for f in fills)

    def test_no_hindsight_guard_recorded(self, env):
        _full_first_cycle()
        f = desk.load_fills()["fills"][0]
        g = f["no_hindsight_guard"]
        assert g["approval_date"] == "2026-07-18"
        assert g["marks_latest_at_approval"] == "2026-07-17"
        assert f["fill_date"] > g["marks_latest_at_approval"]
        assert f["fill_date"] >= g["approval_date"]

    def test_fill_price_and_cost_deterministic(self, env):
        _full_first_cycle()
        f = {x["ticker"]: x for x in desk.load_fills()["fills"]}["AAA"]
        # AAA base 100, 2026-07-20 is the 5th bar -> 100 * 1.04 = 104.0
        assert f["fill_price"] == pytest.approx(104.0)
        assert f["gross_value"] == pytest.approx(f["quantity"] * 104.0)
        assert f["transaction_cost"] == pytest.approx(f["gross_value"] * 0.00125, rel=1e-6)
        assert f["net_cash_delta"] == pytest.approx(-(f["gross_value"] + f["transaction_cost"]),
                                                    rel=1e-9)

    def test_fills_are_reproducible_from_ledgers(self, env):
        _full_first_cycle()
        book = desk.open_book(env["desk"])
        fills = desk.load_fills()["fills"]
        cash, qty = desk.book_cash_holdings(book, fills)
        expected_cash = book["initial_capital"] + sum(f["net_cash_delta"] for f in fills)
        assert cash == pytest.approx(expected_cash)
        assert set(qty) == set(_TICKS)

    def test_rerun_refresh_is_idempotent(self, env):
        _full_first_cycle()
        again = _refresh("2026-07-21", _marks_table(_D0 + ["2026-07-20"]))
        assert again["settlement"]["n_filled"] == 0
        assert again["performance"]["n_appended"] == 0
        assert desk.load_fills()["n_fills"] == len(_TICKS)

    def test_expiry_after_missing_marks(self, env):
        _confirm_snapshot()
        _refresh("2026-07-18")
        desk.generate_orders(confirm=desk.GEN_CONFIRM_TOKEN, today="2026-07-18")
        desk.confirm_orders(confirm=desk.EXEC_CONFIRM_TOKEN, today="2026-07-18")
        # AAA never gets another mark; SPY advances far beyond the expiry window
        later = ["2026-07-20", "2026-07-21", "2026-07-22", "2026-07-23", "2026-07-24",
                 "2026-07-27", "2026-07-28"]
        table = _marks_table(_D0 + later, drop=("AAA",))
        table["AAA"] = _bars(_D0, 100.0)      # stale: nothing after approval
        r = desk.refresh_desk(confirm=desk.REFRESH_CONFIRM_TOKEN, downloader=_dl(table),
                              today="2026-07-29")
        assert r["settlement"]["n_filled"] == len(_TICKS) - 1
        assert desk.load_orders()["counts_by_status"][desk.ST_EXPIRED] == 1
        expired = [o for o in desk.load_orders()["orders"] if o["status"] == desk.ST_EXPIRED]
        assert expired[0]["ticker"] == "AAA"


# --------------------------------------------------------------------------- #
# Holdings / cash / NAV + forward performance (Workstream E)
# --------------------------------------------------------------------------- #
class TestBookAndPerformance:
    def test_holdings_cash_nav_updated(self, env):
        _full_first_cycle()
        v = desk.load_books()["books"][0]["valuation"]
        assert v["holdings_count"] == len(_TICKS)
        assert v["cash"] < desk.DEFAULT_INITIAL_CAPITAL
        assert v["nav"] == pytest.approx(v["cash"] + v["invested"], abs=0.02)
        assert v["as_of_date"] == "2026-07-20"

    def test_performance_rows_appended(self, env):
        _full_first_cycle()
        perf = desk.load_performance()
        assert perf["n_rows"] == 1
        row = perf["rows"][0]
        assert row["date"] == "2026-07-20"
        assert row["benchmark_ticker"] == "SPY"
        assert row["holdings_count"] == len(_TICKS)
        assert row["turnover_pct"] > 0 and row["transaction_cost"] > 0
        for key in ("nav", "cash", "holdings", "benchmark_close", "daily_return_pct",
                    "cumulative_return_pct", "drawdown_pct"):
            assert key in row

    def test_history_rows_never_recomputed(self, env):
        _full_first_cycle()
        raw1 = json.loads((env["desk"] / desk.PERFORMANCE_FILE).read_text(encoding="utf-8"))
        _refresh("2026-07-22", _marks_table(_D0 + ["2026-07-20", "2026-07-21"]))
        raw2 = json.loads((env["desk"] / desk.PERFORMANCE_FILE).read_text(encoding="utf-8"))
        assert len(raw2["rows"]) == len(raw1["rows"]) + 1
        assert raw2["rows"][0] == raw1["rows"][0]        # byte-identical history
        perf = desk.load_performance()
        assert perf["summary"]["n_rows"] == 2
        assert perf["historical_rows_never_recomputed"] is True

    def test_rebalance_sell_path(self, env):
        _full_first_cycle()
        # next month: AAA drops out of the eligible universe -> new confirmed snapshot
        _advance_month(env, month="2026-08", market_date="2026-08-14", drop_member="AAA")
        _confirm_snapshot()
        table = _marks_table(_D0 + ["2026-07-20", "2026-07-21", "2026-08-14"])
        desk.refresh_desk(confirm=desk.REFRESH_CONFIRM_TOKEN, downloader=_dl(table),
                          today="2026-08-15")
        g = desk.generate_orders(confirm=desk.GEN_CONFIRM_TOKEN, today="2026-08-15")
        assert g["status"] == desk.S_OK
        assert g["removals"] == ["AAA"] and g["additions"] == []
        sell = [o for o in desk.load_orders()["orders"]
                if o["status"] == desk.ST_PROPOSED]
        assert len(sell) == 1 and sell[0]["side"] == desk.SIDE_SELL
        assert sell[0]["ticker"] == "AAA"
        held_before = desk.book_nav(desk.open_book(env["desk"]),
                                    desk.load_fills()["fills"], desk.read_marks())["holdings"]
        assert sell[0]["quantity"] == held_before["AAA"]
        desk.confirm_orders(confirm=desk.EXEC_CONFIRM_TOKEN, today="2026-08-15")
        table2 = _marks_table(_D0 + ["2026-07-20", "2026-07-21", "2026-08-14", "2026-08-17"])
        r = desk.refresh_desk(confirm=desk.REFRESH_CONFIRM_TOKEN, downloader=_dl(table2),
                              today="2026-08-18")
        assert r["settlement"]["n_filled"] == 1
        v = desk.load_books()["books"][0]["valuation"]
        assert v["holdings_count"] == len(_TICKS) - 1
        assert "AAA" not in v["holdings"]
        sell_fill = [f for f in desk.load_fills()["fills"] if f["side"] == desk.SIDE_SELL][0]
        assert sell_fill["net_cash_delta"] == pytest.approx(
            sell_fill["gross_value"] - sell_fill["transaction_cost"], rel=1e-9)


# --------------------------------------------------------------------------- #
# Append-only ledgers + tamper detection (Workstream K)
# --------------------------------------------------------------------------- #
class TestLedgerIntegrity:
    def test_all_ledgers_intact_after_full_cycle(self, env):
        _full_first_cycle()
        integ = desk.verify_all_ledgers()
        assert integ["all_intact"] is True

    def test_tampering_history_is_detected(self, env):
        _full_first_cycle()
        raw = json.loads((env["desk"] / desk.FILLS_FILE).read_text(encoding="utf-8"))
        raw["rows"][0]["fill"]["fill_price"] = 1.0     # rewrite history -> chain breaks
        (env["desk"] / desk.FILLS_FILE).write_text(json.dumps(raw), encoding="utf-8")
        integ = desk.verify_all_ledgers()
        assert integ["all_intact"] is False
        broken = [r for r in integ["ledgers"] if not r["intact"]]
        assert broken[0]["ledger"] == desk.FILLS_FILE

    def test_ledger_rows_have_monotonic_seq_and_chain(self, env):
        _full_first_cycle()
        rows = desk._read_ledger(env["desk"], desk.ORDERS_FILE)
        assert [r["seq"] for r in rows] == list(range(1, len(rows) + 1))
        assert all(r.get("chain_hash") for r in rows)

    def test_no_database_tables_touched(self, env):
        # the desk writes ONLY local JSON under the desk dir
        _full_first_cycle()
        files = {p.name for p in env["desk"].iterdir()}
        assert files <= set(desk.LEDGER_FILES) | {desk.MARKS_FILE}


# --------------------------------------------------------------------------- #
# Decision journal + timeline (Workstream G)
# --------------------------------------------------------------------------- #
class TestJournalAndTimeline:
    def test_journal_entries_are_rule_based(self, env):
        _full_first_cycle()
        j = desk.load_journal()
        assert j["rule_based_only"] is True and j["llm_generated"] is False
        texts = [e["text"] for e in j["entries"]]
        assert any("Added because it entered the combined Top-25." in t for t in texts)
        assert any(t.startswith("Paper fill:") for t in texts)
        cats = {e["category"] for e in j["entries"]}
        assert {"BOOK_CREATED", "ORDER_CREATED", "ORDER_SUBMITTED", "PAPER_FILL"} <= cats

    def test_journal_removed_and_hold_phrases(self, env):
        _full_first_cycle()
        _advance_month(env, month="2026-08", market_date="2026-08-14", drop_member="AAA")
        _confirm_snapshot()
        table = _marks_table(_D0 + ["2026-07-20", "2026-07-21", "2026-08-14"])
        desk.refresh_desk(confirm=desk.REFRESH_CONFIRM_TOKEN, downloader=_dl(table),
                          today="2026-08-15")
        desk.generate_orders(confirm=desk.GEN_CONFIRM_TOKEN, today="2026-08-15")
        texts = [e["text"] for e in desk.load_journal()["entries"]]
        assert any("Removed because it exited the combined Top-25." in t for t in texts)
        assert any("Held because it remains inside the hold buffer." in t for t in texts)

    def test_timeline_covers_lifecycle(self, env):
        _full_first_cycle()
        kinds = {e["kind"] for e in desk.load_timeline()["events"]}
        assert {"BOOK_CREATED", "ORDERS_PROPOSED", "ORDERS_SUBMITTED", "SETTLEMENT",
                "PERFORMANCE_APPENDED", "DESK_REFRESH"} <= kinds


# --------------------------------------------------------------------------- #
# Attribution (Workstream F)
# --------------------------------------------------------------------------- #
class TestAttribution:
    def test_attribution_unavailable_before_performance(self, env):
        out = desk.load_attribution()
        assert out["status"] == "ATTRIBUTION_UNAVAILABLE"

    def test_attribution_daily(self, env):
        _full_first_cycle()
        _refresh("2026-07-22", _marks_table(_D0 + ["2026-07-20", "2026-07-21"]))
        at = desk.load_attribution(window="daily")
        assert at["status"] == desk.S_OK
        assert at["start_date"] == "2026-07-20" and at["end_date"] == "2026-07-21"
        assert len(at["top_contributors"]) == 5 and len(at["worst_contributors"]) == 5
        assert at["benchmark_return_pct"] is not None
        assert at["cash_drag_pct_points"] is not None
        assert at["transaction_cost_in_window"] == 0.0   # no fills inside the window
        sectors = {s["sector"] for s in at["sector_contribution"]}
        assert "Tech" in sectors or "Health" in sectors

    def test_model_split_is_fixed_50_50(self, env):
        _full_first_cycle()
        _refresh("2026-07-22", _marks_table(_D0 + ["2026-07-20", "2026-07-21"]))
        at = desk.load_attribution(window="weekly")
        mc = at["model_contribution"]
        assert mc["fundamental_contribution_pct_points"] == \
            mc["momentum_contribution_pct_points"]
        assert "FIXED 50/50" in mc["convention"]

    def test_risk_overlay_never_applied(self, env):
        _full_first_cycle()
        _refresh("2026-07-22", _marks_table(_D0 + ["2026-07-20", "2026-07-21"]))
        at = desk.load_attribution()
        assert at["risk_overlay_effect"]["applied"] is False
        assert at["risk_overlay_effect"]["effect_pct_points"] == 0.0


# --------------------------------------------------------------------------- #
# Operator workflow (Workstream I): the ONE deterministic next required action
# --------------------------------------------------------------------------- #
class TestOperatorWorkflow:
    def test_next_action_chain(self, env):
        st = desk.load_status()
        assert st["next_required_action"].startswith("APPROVE_PROPOSAL")
        _confirm_snapshot()
        st = desk.load_status()
        assert st["next_required_action"].startswith("REFRESH_DESK")
        _refresh("2026-07-18")
        st = desk.load_status()
        assert st["next_required_action"].startswith("CREATE_ORDERS")
        desk.generate_orders(confirm=desk.GEN_CONFIRM_TOKEN, today="2026-07-18")
        st = desk.load_status()
        assert st["next_required_action"].startswith("CONFIRM_ORDERS")
        desk.confirm_orders(confirm=desk.EXEC_CONFIRM_TOKEN, today="2026-07-18")
        st = desk.load_status()
        assert st["next_required_action"].startswith("REFRESH_DESK")
        _refresh("2026-07-21", _marks_table(_D0 + ["2026-07-20"]))
        st = desk.load_status()
        assert st["next_required_action"].startswith("MONITOR")

    def test_status_reports_execution_model_and_tokens(self, env):
        st = desk.load_status()
        assert st["execution_model"] == "NEXT_CLOSE"
        assert st["execution_models"]["NEXT_OPEN"].startswith("NOT_IMPLEMENTED")
        assert st["execution_models"]["NEXT_VWAP"].startswith("NOT_IMPLEMENTED")
        assert st["confirm_tokens"]["refresh"] == desk.REFRESH_CONFIRM_TOKEN
        assert st["cost_bps_per_side"] == 12.5


# --------------------------------------------------------------------------- #
# API routes: auth + safety + end-to-end through the client
# --------------------------------------------------------------------------- #
class TestApi:
    def test_auth_required_everywhere(self, client):
        for path in _DESK_GETS:
            assert client.get(path).status_code in (401, 403), path
        for path in _DESK_POSTS:
            assert client.post(path, json={}).status_code in (401, 403), path

    def test_safety_fields_on_every_get(self, client):
        for path in _DESK_GETS:
            d = client.get(path, headers=_AUTH).json()
            assert d["paper_only"] is True, path
            assert d["orders_enabled"] is False, path
            assert d["live_orders_enabled"] is False, path
            assert d["broker_enabled"] is False, path
            assert d["automation_enabled"] is False, path
            assert d["champion_replaced"] is False, path
            assert d["background_execution"] is False, path
            assert d["append_only"] is True, path

    def test_posts_require_token(self, client):
        for path in _DESK_POSTS:
            d = client.post(path, json={}, headers=_AUTH).json()
            assert d["status"] == desk.S_CONFIRM_REQUIRED, path
            assert d["performed_write"] is False, path

    def test_full_workflow_through_routes(self, client, monkeypatch):
        _confirm_snapshot()
        monkeypatch.setattr(desk, "_today_override", "2026-07-18")
        r = client.post("/v1/paper-desk/refresh",
                        json={"confirm": desk.REFRESH_CONFIRM_TOKEN}, headers=_AUTH).json()
        assert r["status"] == desk.S_OK and r["marks"]["source"] == "FIXTURE"
        g = client.post("/v1/paper-desk/orders/generate",
                        json={"confirm": desk.GEN_CONFIRM_TOKEN}, headers=_AUTH).json()
        assert g["status"] == desk.S_OK and g["n_orders_created"] == len(_TICKS)
        p = client.get("/v1/paper-desk/execution-preview", headers=_AUTH).json()
        assert p["n_open_orders"] == len(_TICKS)
        c = client.post("/v1/paper-desk/orders/confirm",
                        json={"confirm": desk.EXEC_CONFIRM_TOKEN}, headers=_AUTH).json()
        assert c["status"] == desk.S_OK and c["settlement"]["n_filled"] == 0
        # the next completed close arrives; refresh again on a later day
        monkeypatch.setattr(desk, "_today_override", "2026-07-21")
        import os as _os
        _write_fixture(Path(_os.environ[desk.MARKS_FIXTURE_ENV]), _D0 + ["2026-07-20"])
        r2 = client.post("/v1/paper-desk/refresh",
                         json={"confirm": desk.REFRESH_CONFIRM_TOKEN}, headers=_AUTH).json()
        assert r2["settlement"]["n_filled"] == len(_TICKS)
        st = client.get("/v1/paper-desk/status", headers=_AUTH).json()
        assert st["order_counts"][desk.ST_FILLED] == len(_TICKS)
        assert st["next_required_action"].startswith("MONITOR")
        perf = client.get("/v1/paper-desk/performance", headers=_AUTH).json()
        assert perf["n_rows"] == 1
        att = client.get("/v1/paper-desk/attribution?window=daily", headers=_AUTH).json()
        assert att["status"] in (desk.S_OK, "ATTRIBUTION_UNAVAILABLE")
        books = client.get("/v1/paper-desk/books", headers=_AUTH).json()
        assert books["books"][0]["valuation"]["holdings_count"] == len(_TICKS)

    def test_orders_filter_by_status(self, client, monkeypatch):
        _confirm_snapshot()
        monkeypatch.setattr(desk, "_today_override", "2026-07-18")
        client.post("/v1/paper-desk/refresh",
                    json={"confirm": desk.REFRESH_CONFIRM_TOKEN}, headers=_AUTH)
        client.post("/v1/paper-desk/orders/generate",
                    json={"confirm": desk.GEN_CONFIRM_TOKEN}, headers=_AUTH)
        d = client.get("/v1/paper-desk/orders?order_status=PROPOSED", headers=_AUTH).json()
        assert d["n_orders"] == len(_TICKS)
        d2 = client.get("/v1/paper-desk/orders?order_status=FILLED", headers=_AUTH).json()
        assert d2["n_orders"] == 0

    def test_gets_write_nothing(self, client, env):
        for path in _DESK_GETS:
            client.get(path, headers=_AUTH)
        assert not (env["desk"]).exists() or not any(
            f.name in desk.LEDGER_FILES for f in env["desk"].iterdir())

    def test_no_expected_return_or_confidence_keys(self, client):
        for path in _DESK_GETS:
            blob = client.get(path, headers=_AUTH).text
            assert '"expected_return' not in blob, path
            assert '"confidence' not in blob, path


class TestRuntime:
    def test_warm_status_under_two_seconds(self, client):
        client.get("/v1/paper-desk/status", headers=_AUTH)
        t0 = time.perf_counter()
        assert client.get("/v1/paper-desk/status", headers=_AUTH).status_code == 200
        assert time.perf_counter() - t0 < 2.0


# --------------------------------------------------------------------------- #
# UI static contract (Workstreams H + I + J)
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def html() -> str:
    return _UI.read_text(encoding="utf-8")


class TestUiDeskBand:
    def _pm_region(self, html):
        start = html.index('id="tab-portfolio-manager"')
        end = html.index("Phase 26 PORTFOLIO MANAGER END")
        return html[start:end]

    def test_desk_band_inside_portfolio_manager(self, html):
        region = self._pm_region(html)
        assert 'id="pd-band"' in region
        assert "PAPER TRADING DESK" in region
        assert "NO PAPER BOOK YET" in region

    def test_desk_safety_badges(self, html):
        region = self._pm_region(html)
        for badge in ("PAPER ORDERS ONLY", "NO LIVE ORDERS", "NO BROKER",
                      "AUTOMATION OFF", "MANUAL CONFIRMATION"):
            assert badge in region, badge

    def test_all_operator_sections_present(self, html):
        for label in ("Today's Orders", "Pending Orders", "Paper Fills",
                      "Execution Timeline", "Decision Journal", "Forward Performance",
                      "Attribution", "Order History"):
            assert label in html, label
        assert 'id="pd-pills"' in html and 'id="pd-panel"' in html

    def test_action_buttons_and_inpage_confirm_box(self, html):
        region = self._pm_region(html)
        for bid in ("pd-act-refresh", "pd-act-generate", "pd-act-preview",
                    "pd-act-confirm", "pd-act-cancel"):
            assert 'id="%s"' % bid in region, bid
        assert 'id="pd-confirm-box"' in region
        assert 'id="pd-confirm-phrase"' in region

    def test_desk_tokens_wired_in_js(self, html):
        for tok in (desk.GEN_CONFIRM_TOKEN, desk.EXEC_CONFIRM_TOKEN,
                    desk.REFRESH_CONFIRM_TOKEN, desk.CANCEL_CONFIRM_TOKEN):
            assert tok in html, tok

    def test_sides_never_plain_buy_sell(self, html):
        assert "PAPER BUY" in html and "PAPER SELL" in html
        assert not re.search(r">\s*BUY\s*</button>", html)
        assert not re.search(r">\s*SELL\s*</button>", html)

    def test_fill_language_never_executed(self, html):
        assert "PAPER FILLS RECORDED" in html
        assert "PORTFOLIO EXECUTED" not in html

    def test_execution_model_documented_on_page(self, html):
        region = self._pm_region(html)
        assert "NEXT_CLOSE" in region
        assert "no hindsight" in region

    def test_six_step_operator_flow(self, html):
        region = self._pm_region(html)
        m = re.search(r'<ol id="pm-flow-steps".*?</ol>', region, re.DOTALL)
        assert m
        assert m.group(0).count("<li>") == 6
        assert "Refresh desk market data" in m.group(0)
        assert "Monitor paper fills" in m.group(0)

    def test_no_native_dialogs_still_zero(self, html):
        scripts = "\n".join(re.findall(r"<script[^>]*>(.*?)</script>", html, re.DOTALL))
        for pat in (r"(?<![A-Za-z0-9_])alert\s*\(", r"(?<![A-Za-z0-9_])confirm\s*\(",
                    r"(?<![A-Za-z0-9_])prompt\s*\("):
            assert not re.search(pat, scripts), pat

    def test_no_blank_buttons_in_desk_band(self, html):
        region = self._pm_region(html)
        for m in re.finditer(r"<button[^>]*>(.*?)</button>", region, re.DOTALL):
            label = re.sub(r"<[^>]+>", "", m.group(1))
            label = re.sub(r"&[a-z#0-9]+;", "x", label)
            assert label.strip(), m.group(0)[:120]

    def test_lab_admin_clutter_collapsed(self, html):
        summary = "LAB / ADMIN &mdash; legacy manual tools"
        assert summary in html
        idx = html.index(summary)
        # the legacy tools live INSIDE a details element (collapsed by default)
        before = html[:idx]
        assert before.rfind("<details") > before.rfind("</details>")
        m = re.search(r"<details[^>]*>\s*<summary>\s*LAB / ADMIN", html)
        assert m and "open" not in m.group(0).split(">")[0]
        for legacy in ("Run SMA Strategy", "Submit Manual Signal"):
            assert legacy in html  # preserved, but only inside the collapsed LAB / ADMIN
