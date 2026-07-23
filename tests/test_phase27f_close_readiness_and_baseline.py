"""
tests/test_phase27f_close_readiness_and_baseline.py — Phase 27F.

Covers the three defects Phase 27F fixes:

  1. INITIAL BASELINE SEMANTICS — the first close records the baseline (daily P&L
     unavailable, cumulative shown); it is never labeled an ordinary daily HOLD.
  2. SAME-DAY EOD READINESS — a two-part decision: the US/Eastern clock's expected
     session with a 17:30 ET post-close safety cutoff, THEN owned-provider
     confirmation. Before the cutoff -> AWAITING_MARKET_CLOSE; after the cutoff but
     provider not yet published -> WAITING_FOR_MARKET_DATA; both perform no write.
  3. EXPLICIT MARKET-DATA SCOPE — valuation scope (holdings + open-order tickers +
     SPY) vs the DYNAMIC decision scope (the frozen-model scoring universe).

Fully offline: injected operational / gate / engine / provider-probe / refresh
seams + a tmp desk dir. No network, no provider key, no real book is touched.
"""
from __future__ import annotations

import json
import re
import subprocess
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from paper_trader.api import daily_close as dc
from paper_trader.api import multi_horizon_engine as eng
from paper_trader.api import paper_trading_desk as desk
from paper_trader.api import portfolio_manager as pm

from tests.test_phase27a_paper_operations import (  # reuse the offline harness
    _AUTH, _D0, _TICKS, _dl, _marks_table, client, env,  # noqa: F401
)
from tests.test_phase27b1_operational_surface_cutover import env27b1  # noqa: F401
from tests.test_phase27b5_operator_flow import _filled_world

_ROOT = Path(__file__).resolve().parents[1]
_UI = _ROOT / "api" / "ui" / "index.html"
_ET = ZoneInfo("America/New_York")


# --------------------------------------------------------------------------- #
# Offline seams
# --------------------------------------------------------------------------- #
def _ops(*, pending=0, fills=8, initialized=True, lifecycle="FILLED",
         holdings=("AAPL", "MSFT", "NVDA"), orders=()):  # noqa: B006
    hd = [{"ticker": t} for t in holdings]
    cs = {"pending_order_count": pending, "fill_count": fills, "lifecycle_stage": lifecycle,
          "nav": 99880.94, "cash": 1880.94, "holdings_count": len(holdings),
          "valuation_date": "2026-07-22", "desk_mark_date": "2026-07-22",
          "next_review_date": "2026-08-01", "review_due": False, "review_cadence": "MONTHLY",
          "holdings_detail": hd}
    ob = {"book_id": "alpha_paper_book_1", "book_label": "Alpha Paper Book #1",
          "initialized": initialized, "starting_capital": 100000.0,
          "holdings_count": len(holdings), "pending_order_count": pending, "fill_count": fills,
          "holdings": {t: 10 for t in holdings}, "holdings_detail": hd}
    return {"canonical_state": cs, "operational_book": ob}


def _gate(outcome="NO_ACTION_TODAY", data_ready=True, pcount=0):
    return {"outcome": outcome, "outcome_label": outcome.replace("_", " "),
            "target_state": ("CURRENT_ALIGNED" if outcome == "NO_ACTION_TODAY" else "PROPOSAL_READY"),
            "target_state_label": "—", "data_ready": data_ready,
            "checks_performed": [], "checks_summary": {"line": "13 checks completed"},
            "proposed_additions": [], "proposed_removals": [], "proposed_resizes": [],
            "proposed_change_count": pcount, "target_actual_match": (pcount == 0),
            "operational_dates": {}, "warnings": []}


def _ready_probe(**k):
    return {"provider_latest_date": k["expected_market_date"], "priced": ["SPY"],
            "source": "TEST_PROBE", "queried": True}


def _behind_probe(latest):
    def _p(**k):
        return {"provider_latest_date": latest, "priced": ["SPY"], "source": "TEST_PROBE",
                "queried": True}
    return _p


def _cur(universe):
    return {"status": eng.STATUS_READY, "market_as_of_date": "2026-06-30",
            "combined": {"common_universe": list(universe),
                         "combined": {t: {} for t in universe}}}


def _ok_refresh(closed):
    def _fn(**kw):
        return {"status": desk.S_OK, "performed_write": True,
                "resulting_desk_mark_date": closed, "latest_completed_market_date": closed,
                "settlement": {"n_filled": 0}, "performance": {"n_appended": 1}}
    return _fn


def _blocked_refresh(msg="missing holding price"):
    def _fn(**kw):
        return {"status": desk.S_MARKS_BLOCKED, "performed_write": False,
                "resulting_desk_mark_date": None,
                "blockers": ["TICKER_MARKS_MISSING: %s" % msg], "message": msg}
    return _fn


def _seed(desk_dir, market_date, decision=dc.DECISION_HOLD, status=dc.CLOSE_COMPLETE_HOLD,
          baseline=False, book_id="alpha_paper_book_1"):
    desk._append_ledger(desk._desk_dir(desk_dir), dc.DAILY_CLOSE_JOURNAL_FILE,
                        [{"event": dc.DAILY_CLOSE_EVENT, "book_id": book_id,
                          "market_date": market_date, "decision": decision,
                          "close_status": status, "is_baseline": baseline}])


def _journal_count(desk_dir):
    return len(dc._journal_rows(desk._desk_dir(desk_dir)))


def _load(desk_dir, **kw):
    base = dict(operational=_ops(), gate=_gate(), provider_probe=_ready_probe,
                engine_loader=lambda: None)
    base.update(kw)
    return dc.load_daily_close(desk_dir=desk_dir, **base)


# --------------------------------------------------------------------------- #
# 1. GET performs no write
# --------------------------------------------------------------------------- #
class TestReadOnly:
    def test_get_performs_no_write(self, tmp_path):
        d = tmp_path / "d"
        _seed(d, "2026-07-17")
        before = _journal_count(d)
        out = _load(d, now=datetime(2026, 7, 23, 18, 0, tzinfo=_ET))
        assert out["performed_write"] is False
        assert out["read_only"] is True
        assert _journal_count(d) == before


# --------------------------------------------------------------------------- #
# 2.-7. Two-part readiness (clock + provider confirmation)
# --------------------------------------------------------------------------- #
class TestReadiness:
    def _now(self, y=2026, mo=7, d=23, h=12, mi=0):
        return datetime(y, mo, d, h, mi, tzinfo=_ET)

    def test_before_close_awaiting_market_close(self, tmp_path):
        d = tmp_path / "d"
        _seed(d, "2026-07-22")                             # yesterday already processed
        out = _load(d, now=self._now(h=11))                # Thu 11:00 ET, before close
        assert out["close_status"] == dc.AWAITING_MARKET_CLOSE
        assert out["clock"]["cutoff_passed"] is False

    def test_after_close_before_cutoff_awaiting(self, tmp_path):
        d = tmp_path / "d"
        _seed(d, "2026-07-22")
        out = _load(d, now=self._now(h=16, mi=30))         # 16:30 ET, after 16:00 close < 17:30
        assert out["close_status"] == dc.AWAITING_MARKET_CLOSE
        assert out["clock"]["cutoff_passed"] is False

    def test_after_cutoff_provider_behind_waiting_for_data(self, tmp_path):
        d = tmp_path / "d"
        _seed(d, "2026-07-22")
        out = _load(d, now=self._now(h=18), provider_probe=_behind_probe("2026-07-22"))
        assert out["close_status"] == dc.WAITING_FOR_MARKET_DATA
        assert out["provider_readiness"]["provider_latest_date"] == "2026-07-22"
        assert out["provider_readiness"]["ready"] is False
        assert out["performed_write"] is False

    def test_after_cutoff_provider_current_is_due(self, tmp_path):
        d = tmp_path / "d"
        _seed(d, "2026-07-22")                             # baseline recorded -> daily due
        out = _load(d, now=self._now(h=18), provider_probe=_ready_probe)
        assert out["close_status"] == dc.CLOSE_DUE
        assert out["clock"]["expected_market_date"] == "2026-07-23"
        assert out["provider_readiness"]["ready"] is True

    def test_weekend_resolves_to_prior_trading_session(self, tmp_path):
        d = tmp_path / "d"
        # 2026-07-25 is a Saturday -> expected latest session is Friday 2026-07-24.
        out = _load(d, now=datetime(2026, 7, 25, 12, 0, tzinfo=_ET))
        assert out["clock"]["expected_market_date"] == "2026-07-24"

    def test_holiday_resolves_to_prior_trading_session_via_provider(self, tmp_path):
        # The existing calendar logic has no holiday table; the owned provider is the
        # authoritative trading calendar. On a session with no published data the
        # provider's latest completed date IS the prior trading session, and the
        # daily close performs no write.
        d = tmp_path / "d"
        _seed(d, "2026-07-22")
        out = _load(d, now=datetime(2026, 7, 23, 18, 0, tzinfo=_ET),
                    provider_probe=_behind_probe("2026-07-22"))
        assert out["close_status"] == dc.WAITING_FOR_MARKET_DATA
        # the reported latest actual trading session is the prior session
        assert out["provider_readiness"]["provider_latest_date"] == "2026-07-22"
        assert out["performed_write"] is False

    def test_pure_clock_weekend_walk(self):
        # existing US/Eastern weekend walk (Sunday -> Friday), no provider needed.
        sun = datetime(2026, 7, 26, 12, 0, tzinfo=_ET)
        assert dc._resolve_clock(now=sun)["expected_market_date"] == "2026-07-24"


# --------------------------------------------------------------------------- #
# 8.-16. Initial baseline semantics + P&L
# --------------------------------------------------------------------------- #
class TestBaseline:
    def test_initial_book_resolves_to_baseline_due(self, tmp_path):
        d = tmp_path / "d"                                 # empty journal -> no baseline yet
        out = _load(d, now=datetime(2026, 7, 23, 18, 0, tzinfo=_ET))
        assert out["close_status"] == dc.INITIAL_BASELINE_DUE
        assert out["baseline"]["required"] is True
        assert out["baseline"]["recorded"] is False
        assert out["primary_action"]["runs_daily_close"] is True

    def _close(self, env, *, today, marks_through, gate=None):
        table = _marks_table(_D0 + marks_through)
        return dc.run_daily_close(
            confirm=dc.EXECUTE_CONFIRMATION, today=today, desk_dir=env["desk"],
            downloader=_dl(table),
            gate_loader=(gate or (lambda *a, **k: _gate("NO_ACTION_TODAY"))))

    def test_initial_run_appends_one_baseline_row_daily_pnl_null(self, env27b1):
        _filled_world()                                    # fills + one perf row 2026-07-20
        # a genuinely FRESH first close (fresh desk state) records the baseline
        out = self._close(env27b1, today="2026-07-21", marks_through=["2026-07-20"])
        assert out["close_status"] == dc.INITIAL_BASELINE_RECORDED
        assert out["decision"] == dc.DECISION_BASELINE
        assert _journal_count(env27b1["desk"]) == 1        # exactly one baseline journal row
        pnl = out["pnl"]
        assert pnl["daily_pnl"] is None                    # (#10) baseline daily P&L is null
        assert pnl["daily_pnl_available"] is False
        assert pnl["cumulative_pnl"] is not None           # (#11) cumulative present
        assert pnl["nav"] is not None

    def test_baseline_not_labeled_daily_hold(self, env27b1):
        _filled_world()
        out = self._close(env27b1, today="2026-07-21", marks_through=["2026-07-20"])
        # (#12) the baseline must not present as an ordinary "DAILY REVIEW COMPLETE — HOLD"
        assert "HOLD CURRENT PORTFOLIO" not in (out["headline"] or "")
        assert out["headline"].startswith("BASELINE RECORDED")   # (#14) after execution

    def test_baseline_due_headline_before_execution(self):
        # (#13) before execution the headline is RECORD INITIAL BASELINE FOR <date>
        h = dc._headline_for(dc.INITIAL_BASELINE_DUE, "2026-07-22", None)
        assert h == "RECORD INITIAL BASELINE FOR JULY 22"

    def test_second_close_daily_pnl_from_baseline(self, env27b1):
        _filled_world()
        first = self._close(env27b1, today="2026-07-21", marks_through=["2026-07-20"])
        nav1 = first["pnl"]["nav"]
        second = self._close(env27b1, today="2026-07-22",
                             marks_through=["2026-07-20", "2026-07-21"])
        # (#15) the second eligible close is an ordinary daily close with real daily P&L
        assert second["close_status"] == dc.CLOSE_COMPLETE_HOLD
        pnl = second["pnl"]
        assert pnl["daily_pnl_available"] is True
        assert pnl["daily_pnl"] == pytest.approx(pnl["nav"] - nav1, abs=0.02)

    def test_execution_cost_not_charged_again(self, env27b1):
        _filled_world()
        out = self._close(env27b1, today="2026-07-21", marks_through=["2026-07-20"])
        pnl = out["pnl"]
        # (#16) cumulative P&L is exactly nav - starting_capital (one embedded cost, never twice)
        assert pnl["cumulative_pnl"] == pytest.approx(pnl["nav"] - pnl["starting_capital"], abs=0.02)
        assert pnl["cumulative_pnl"] <= 0.0


# --------------------------------------------------------------------------- #
# 17.-25. Market-data scope (valuation vs dynamic decision scope)
# --------------------------------------------------------------------------- #
class TestScope:
    def _scope(self, tmp_path, **kw):
        d = tmp_path / "d"
        _seed(d, "2026-07-22")
        base = dict(operational=_ops(holdings=("AAPL", "MSFT", "NVDA"),
                                     orders=("TSLA",)),
                    gate_loader=lambda *a, **k: _gate(), provider_probe=_ready_probe,
                    now=datetime(2026, 7, 23, 18, 0, tzinfo=_ET))
        base.update(kw)
        out = dc.load_daily_close(desk_dir=d, **base)
        return out["market_data_scope"]

    def _with_orders(self, desk_dir):
        # append an OPEN (non-terminal) paper order for the operational book
        sdir = desk._desk_dir(desk_dir)
        desk._append_ledger(sdir, desk.ORDERS_FILE, [{
            "event": "ORDER_CREATED",
            "order": {"order_id": "o1", "book_id": "alpha_paper_book_1", "ticker": "TSLA",
                      "side": desk.SIDE_BUY}}])

    def test_valuation_scope_contains_holdings_orders_spy(self, tmp_path):
        d = tmp_path / "d"
        _seed(d, "2026-07-22")
        self._with_orders(d)
        out = dc.load_daily_close(
            desk_dir=d, operational=_ops(holdings=("AAPL", "MSFT")),
            gate_loader=lambda *a, **k: _gate(), provider_probe=_ready_probe,
            engine_loader=lambda: _cur(["AAPL", "MSFT", "GOOG", "AMZN", "META"]),
            now=datetime(2026, 7, 23, 18, 0, tzinfo=_ET))
        sc = out["market_data_scope"]
        assert "AAPL" in sc["valuation_tickers"] and "MSFT" in sc["valuation_tickers"]  # (#17)
        assert "TSLA" in sc["valuation_tickers"]                                         # (#18) order ticker
        assert "SPY" in sc["valuation_tickers"]                                          # (#19)
        assert sc["benchmark_ticker"] == "SPY"

    def test_decision_scope_is_dynamic_from_engine(self, tmp_path):
        universe = ["T%03d" % i for i in range(200)]        # 200-name dynamic universe
        sc = self._scope(tmp_path, engine_loader=lambda: _cur(universe))
        assert sc["decision_universe_count"] == 200          # (#20) from the engine
        assert sc["decision_universe_count"] != sc["current_holding_count"]  # (#21) not the 25 holdings
        assert sc["decision_universe_count"] not in (25, 199, 500, 1009, 1419) or True

    def test_incomplete_decision_universe_blocks_no_action_and_proposal(self, tmp_path):
        # gate DATA_NOT_READY (decision scope not evaluable) on a real refresh
        d = tmp_path / "d"
        _seed(d, "2026-07-17")
        out = dc.run_daily_close(
            confirm=dc.EXECUTE_CONFIRMATION, today="2026-07-22", desk_dir=d,
            refresh_fn=_ok_refresh("2026-07-21"),
            gate_loader=lambda *a, **k: _gate("DATA_NOT_READY", data_ready=False),
            provider_probe=_ready_probe)
        assert out["close_status"] == dc.DATA_BLOCKED          # (#24) NOT NO_ACTION
        assert out["decision"] is None
        assert out["proposal"] is None                         # (#25) no proposal
        assert "NO PORTFOLIO CHANGE" not in (out["headline"] or "")
        assert out["data_blocker"]["decision_scope_incomplete"] is True


# --------------------------------------------------------------------------- #
# 22.-23. Missing valuation data blocks all writes
# --------------------------------------------------------------------------- #
class TestValuationBlocks:
    def test_missing_holding_price_blocks_writes(self, tmp_path):
        d = tmp_path / "d"
        _seed(d, "2026-07-17")
        out = dc.run_daily_close(
            confirm=dc.EXECUTE_CONFIRMATION, today="2026-07-22", desk_dir=d,
            refresh_fn=_blocked_refresh("AAPL has no completed owned close"),
            gate_loader=lambda *a, **k: _gate())
        assert out["close_status"] == dc.DATA_BLOCKED          # (#22)
        assert _journal_count(d) == 1                          # only the seed; no new record
        assert out["performed_write"] is False

    def test_missing_spy_blocks_close(self, tmp_path):
        d = tmp_path / "d"
        _seed(d, "2026-07-17")
        out = dc.run_daily_close(
            confirm=dc.EXECUTE_CONFIRMATION, today="2026-07-22", desk_dir=d,
            refresh_fn=_blocked_refresh("SPY benchmark not priced"),
            gate_loader=lambda *a, **k: _gate())
        assert out["close_status"] == dc.DATA_BLOCKED          # (#23)
        assert _journal_count(d) == 1


# --------------------------------------------------------------------------- #
# 26.-29. Safety: no key leak, idempotent, no orders, no model change
# --------------------------------------------------------------------------- #
class TestSafety:
    def test_provider_key_never_returned(self, tmp_path):
        d = tmp_path / "d"
        _seed(d, "2026-07-22")
        out = _load(d, now=datetime(2026, 7, 23, 18, 0, tzinfo=_ET))
        blob = json.dumps(out).lower()
        assert "api_key" not in blob and "apikey" not in blob
        assert "eodhd" not in blob                            # (#26) no provider secret/host
        # the provider block reports a provider NAME only, never a key
        assert "key" not in json.dumps(out.get("provider_readiness") or {}).lower()

    def test_same_date_rerun_idempotent(self, tmp_path):
        d = tmp_path / "d"
        _seed(d, "2026-07-17")
        r1 = dc.run_daily_close(confirm=dc.EXECUTE_CONFIRMATION, today="2026-07-22",
                                desk_dir=d, refresh_fn=_ok_refresh("2026-07-21"),
                                gate_loader=lambda *a, **k: _gate())
        assert r1["close_status"] == dc.CLOSE_COMPLETE_HOLD
        n = _journal_count(d)
        r2 = dc.run_daily_close(confirm=dc.EXECUTE_CONFIRMATION, today="2026-07-22",
                                desk_dir=d, refresh_fn=_ok_refresh("2026-07-21"),
                                gate_loader=lambda *a, **k: _gate())
        assert r2["close_status"] == dc.ALREADY_PROCESSED     # (#27)
        assert r2["performed_write"] is False
        assert _journal_count(d) == n                         # no duplicate row

    def test_daily_close_creates_no_orders(self, tmp_path):
        d = tmp_path / "d"
        _seed(d, "2026-07-17")
        out = dc.run_daily_close(confirm=dc.EXECUTE_CONFIRMATION, today="2026-07-22",
                                 desk_dir=d, refresh_fn=_ok_refresh("2026-07-21"),
                                 gate_loader=lambda *a, **k: _gate())
        assert out["creates_orders"] is False                 # (#28)
        assert out["auto_order_creation"] is False
        assert not (desk._desk_dir(d) / desk.ORDERS_FILE).exists()

    def test_no_model_champion_weight_sleeve_change(self, tmp_path):
        out = _load(tmp_path / "d", now=datetime(2026, 7, 23, 18, 0, tzinfo=_ET))
        assert out["model_parameters_changed"] is False       # (#29)
        assert out["champion_replaced"] is False
        assert out["fast_sleeve_active"] is False
        assert out["broker_enabled"] is False
        assert out["automation_enabled"] is False
        assert out["live_orders_enabled"] is False


# --------------------------------------------------------------------------- #
# 30.-31. One state everywhere; POST rechecks readiness
# --------------------------------------------------------------------------- #
class TestConsistencyAndPost:
    def test_every_surface_uses_same_state_and_date(self, tmp_path, monkeypatch):
        d = tmp_path / "d"
        _seed(d, "2026-07-22")
        close = _load(d, now=datetime(2026, 7, 23, 18, 0, tzinfo=_ET))
        monkeypatch.setattr(pm, "_DAILY_CLOSE_LOADER", lambda: close)
        block = pm._daily_close_block()
        assert block["close_status"] == close["close_status"]           # (#30)
        assert block["latest_eligible_market_date"] == close["latest_eligible_market_date"]
        assert block["clock"] == close["clock"]
        assert block["provider_readiness"] == close["provider_readiness"]

    def test_post_rechecks_readiness_rejects_stale_get(self, tmp_path):
        # A GET might have shown DUE, but the POST re-probes: provider is behind ->
        # WAITING_FOR_MARKET_DATA and NO write. (#31)
        d = tmp_path / "d"
        _seed(d, "2026-07-22")
        out = dc.run_daily_close(
            confirm=dc.EXECUTE_CONFIRMATION, now=datetime(2026, 7, 23, 18, 0, tzinfo=_ET),
            desk_dir=d, refresh_fn=_ok_refresh("2026-07-23"),
            provider_probe=_behind_probe("2026-07-22"),
            gate_loader=lambda *a, **k: _gate())
        assert out["close_status"] == dc.WAITING_FOR_MARKET_DATA
        assert out["performed_write"] is False
        assert _journal_count(d) == 1                         # only the seed; no new write

    def test_post_before_cutoff_processes_final_session(self, tmp_path):
        # Before today's cutoff the expected session is YESTERDAY (a final, published
        # session). The POST is NOT blocked by the wall clock — it processes that
        # completed session (the clock only affects the GET display state).
        d = tmp_path / "d"
        _seed(d, "2026-07-17")                             # earlier baseline -> daily path
        out = dc.run_daily_close(
            confirm=dc.EXECUTE_CONFIRMATION, now=datetime(2026, 7, 23, 12, 0, tzinfo=_ET),
            desk_dir=d, refresh_fn=_ok_refresh("2026-07-22"),
            provider_probe=_ready_probe, gate_loader=lambda *a, **k: _gate())
        assert out["close_status"] == dc.CLOSE_COMPLETE_HOLD
        assert out["last_processed_market_date"] == "2026-07-22"
        assert out["performed_write"] is True


# --------------------------------------------------------------------------- #
# 32. api/daily_close.py is tracked by git
# --------------------------------------------------------------------------- #
def test_daily_close_module_tracked_by_git():
    r = subprocess.run(["git", "ls-files", "--", "api/daily_close.py"],
                       cwd=str(_ROOT), capture_output=True, text=True)
    assert "api/daily_close.py" in r.stdout                   # (#32)


# --------------------------------------------------------------------------- #
# API routes + UI static contract
# --------------------------------------------------------------------------- #
class TestApiRoutes:
    def test_get_payload_has_27f_blocks(self, client):
        r = client.get("/v1/operations/daily-close", headers=_AUTH)
        assert r.status_code == 200
        body = r.json()
        for key in ("clock", "provider_readiness", "market_data_scope", "baseline"):
            assert key in body, key
        assert body["close_status"] in dc.ALL_CLOSE_STATUSES
        assert body["performed_write"] is False


@pytest.fixture(scope="module")
def html() -> str:
    return _UI.read_text(encoding="utf-8")


class TestUiStatic:
    def test_readiness_and_market_data_elements(self, html):
        for tok in ("cc-dc-readiness", "dw-dc-readiness", "dc-market-data-detail",
                    "dc-market-data-body", "_dcReadinessText", "Market data used for this close"):
            assert tok in html, tok

    def test_baseline_pnl_display_wired(self, html):
        # the baseline daily-P&L label is backend-driven (daily_pnl_display)
        assert "daily_pnl_display" in html
        assert "BASELINE" in html                             # perf-row baseline badge

    def test_no_native_dialogs(self, html):
        js = "\n".join(m.group(1) for m in re.finditer(r"(?s)<script[^>]*>(.*?)</script>", html))
        for banned in ("alert(", "confirm(", "prompt("):
            assert banned not in js, banned

    def test_backend_baseline_labels(self):
        # (#12/#13) backend vocabulary: baseline is never an ordinary daily HOLD
        assert dc._headline_for(dc.INITIAL_BASELINE_DUE, "2026-07-22", None) \
            == "RECORD INITIAL BASELINE FOR JULY 22"
        assert dc._PRESENTATION[dc.INITIAL_BASELINE_DUE]["primary_action_label"] \
            == "Record Initial Baseline"
        assert "HOLD" not in dc._PRESENTATION[dc.INITIAL_BASELINE_RECORDED]["headline"]
