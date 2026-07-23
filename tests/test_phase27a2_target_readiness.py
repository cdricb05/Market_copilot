"""
tests/test_phase27a2_target_readiness.py — Phase 27A.2 fresh alpha-target gate.

Fully offline: the Phase 25 owned-style CSV fixtures are wired via env seams, the
append-only ledger and the paper-desk store are redirected to tmp dirs, the
freshness-gate clock is pinned through the ``alpha_target._now_override`` seam,
the canonical valuation is injected through the module seam, and every refresh
uses an injected/fixture downloader (never the network, never a key).

Covers: the six-field authoritative date contract (never collapsed), the exact
``ALPHA_MARKET_DATE_BEHIND_LATEST_COMPLETED_MARKET_DATE`` blocker, the HARD
backend confirmation gate (stale blocks with no write; aligned confirms through
the existing manual token; duplicate / count / weights / ledger-integrity /
platform-not-ready all block), fundamental data allowed to be older on its own
cadence, the manual owned-data refresh (token gate, exact previous/resulting
dates, momentum scores and month frozen, no snapshot / order / signal /
decision / fill / book side effects, month-boundary and provider-blocked and
insufficient-coverage refusals, nothing written on any refusal), the absence of
any GCP/tunnel dependency, the 25-name review payload (complete target table +
approval checklist), API auth + safety fields, and the UI static contract
(operational panel order, stale/ready states, prominent stale-state refresh,
disabled confirm, header that never implies the tunnel is required, no native
dialogs, no blank buttons).
"""
from __future__ import annotations

import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from paper_trader.api import alpha_target as at
from paper_trader.api import multi_horizon_engine as eng
from paper_trader.api import multi_horizon_ledger as ledger
from paper_trader.api import multi_horizon_platform as plat
from paper_trader.api import paper_trading_desk as desk
from paper_trader.api.app import app
from paper_trader.config import get_settings

from tests.test_multi_horizon_platform import (  # reuse the Phase 25 owned-style fixtures
    _write_fund_panel, _write_mom_current, _write_risk, _write_mom_monthly,
    _write_sector_map,
)

_KEY = "at-test-key"
_AUTH = {"X-API-Key": _KEY}
_UI = Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html"

# Fixture clocks (2026-07-17 = Friday, 2026-07-20 = Monday, 2026-07-21 = Tuesday).
_FRI_AFTER_CLOSE = datetime(2026, 7, 17, 21, 0, tzinfo=timezone.utc)   # latest completed 07-17
_TUE_MORNING = datetime(2026, 7, 21, 14, 0, tzinfo=timezone.utc)       # latest completed 07-20
_AUG_TUE = datetime(2026, 8, 4, 14, 0, tzinfo=timezone.utc)            # latest completed 08-03


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
@pytest.fixture
def env(monkeypatch, tmp_path):
    """Small 8-name aligned world (REQUIRED_TARGET_COUNT scaled to 8)."""
    panel = tmp_path / "panel.csv"
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    led = tmp_path / "ledger"
    desk_dir = tmp_path / "desk"
    smap = tmp_path / "sector_map.csv"
    _write_fund_panel(panel)
    _write_mom_current(inputs / eng.CUR_MOM_FILE)
    _write_risk(inputs / eng.RISK_FILE)
    _write_mom_monthly(inputs / eng.MONTHLY_PANEL_FILE)
    _write_sector_map(smap)
    monkeypatch.setenv(eng.PANEL_ENV, str(panel))
    monkeypatch.setenv(eng.INPUTS_ENV, str(inputs))
    monkeypatch.setenv(eng.SECTOR_MAP_ENV, str(smap))
    monkeypatch.setenv(ledger.LEDGER_DIR_ENV, str(led))
    monkeypatch.setenv(desk.DESK_DIR_ENV, str(desk_dir))
    monkeypatch.setenv(plat.FAST_SPEC_ENV, str(tmp_path / "no_fast_spec.json"))
    monkeypatch.setattr(at, "_now_override", _FRI_AFTER_CLOSE)
    monkeypatch.setattr(at, "REQUIRED_TARGET_COUNT", 8)
    monkeypatch.setattr(at, "_VALUATION_LOADER",
                        lambda: {"current_mark": {"as_of_market_date": "2026-07-17"}})
    eng.clear_cache()
    plat.clear_caches()
    yield {"panel": panel, "inputs": inputs, "ledger": led, "desk": desk_dir,
           "tmp": tmp_path}
    eng.clear_cache()
    plat.clear_caches()


_BIG_TICKERS = ["T%02d" % i for i in range(30)]
_BIG_SECTORS = {tk: "Sector%d" % (i % 5) for i, tk in enumerate(_BIG_TICKERS)}


def _write_big_fund_panel(path: Path, month="2026-05") -> None:
    cols = ["as_of_date", "rebalance_date", "ticker", "sector", "liquidity_proxy",
            "composite_sn", "forward_63d_return", "has_forward_return"]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        for i, tk in enumerate(_BIG_TICKERS):
            w.writerow({"as_of_date": "2026-06-30", "rebalance_date": f"{month}-22",
                        "ticker": tk, "sector": _BIG_SECTORS[tk],
                        "liquidity_proxy": 1e6 * (i + 1),
                        "composite_sn": round(3.0 - 0.05 * i, 4),
                        "forward_63d_return": 0.01, "has_forward_return": "True"})


def _write_big_mom_current(path: Path, market_date="2026-07-17", month="2026-07") -> None:
    cols = ["ticker", "mom_6_1", "is_member", "adv_dollar", "realized_vol_63d",
            "trailing_obs_126", "eligible_history", "extreme_flag", "sector",
            "market_as_of_date", "month_label"]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        for i, tk in enumerate(_BIG_TICKERS):
            w.writerow({"ticker": tk, "mom_6_1": round(1.5 - 0.03 * i, 4), "is_member": 1,
                        "adv_dollar": 5e7 * (i + 1), "realized_vol_63d": 0.25,
                        "trailing_obs_126": 126, "eligible_history": 1, "extreme_flag": 0,
                        "sector": _BIG_SECTORS[tk], "market_as_of_date": market_date,
                        "month_label": month})


def _write_big_risk(path: Path) -> None:
    cols = ["ticker", "realized_vol_63d", "beta_universe", "adv_dollar_20d",
            "max_drawdown_252d", "is_current_member", "last_price_date", "sector"]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        for tk in _BIG_TICKERS:
            w.writerow({"ticker": tk, "realized_vol_63d": 0.25, "beta_universe": 1.0,
                        "adv_dollar_20d": 5e8, "max_drawdown_252d": -0.1,
                        "is_current_member": 1, "last_price_date": "2026-07-17",
                        "sector": _BIG_SECTORS[tk]})


@pytest.fixture
def big_env(monkeypatch, tmp_path):
    """30-name aligned world - the real 25-name complete-target contract."""
    panel = tmp_path / "panel.csv"
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    led = tmp_path / "ledger"
    desk_dir = tmp_path / "desk"
    _write_big_fund_panel(panel)
    _write_big_mom_current(inputs / eng.CUR_MOM_FILE)
    _write_big_risk(inputs / eng.RISK_FILE)
    monkeypatch.setenv(eng.PANEL_ENV, str(panel))
    monkeypatch.setenv(eng.INPUTS_ENV, str(inputs))
    monkeypatch.setenv(eng.SECTOR_MAP_ENV, str(tmp_path / "no_sector_map.csv"))
    monkeypatch.setenv(ledger.LEDGER_DIR_ENV, str(led))
    monkeypatch.setenv(desk.DESK_DIR_ENV, str(desk_dir))
    monkeypatch.setenv(plat.FAST_SPEC_ENV, str(tmp_path / "no_fast_spec.json"))
    monkeypatch.setattr(at, "_now_override", _FRI_AFTER_CLOSE)
    monkeypatch.setattr(at, "_VALUATION_LOADER",
                        lambda: {"current_mark": {"as_of_market_date": "2026-07-17"}})
    eng.clear_cache()
    plat.clear_caches()
    yield {"panel": panel, "inputs": inputs, "ledger": led, "desk": desk_dir,
           "tmp": tmp_path}
    eng.clear_cache()
    plat.clear_caches()


@pytest.fixture
def client(env, monkeypatch) -> TestClient:
    monkeypatch.setenv("PAPER_TRADER_SERVICE_API_KEY", _KEY)
    monkeypatch.setenv("PAPER_TRADER_DATABASE_URL",
                       "postgresql+psycopg2://unused:unused@localhost:5432/unused")
    get_settings.cache_clear()
    with TestClient(app) as c:
        yield c


@pytest.fixture
def big_client(big_env, monkeypatch) -> TestClient:
    monkeypatch.setenv("PAPER_TRADER_SERVICE_API_KEY", _KEY)
    monkeypatch.setenv("PAPER_TRADER_DATABASE_URL",
                       "postgresql+psycopg2://unused:unused@localhost:5432/unused")
    get_settings.cache_clear()
    with TestClient(app) as c:
        yield c


def _weekday_bars(start="2026-01-02", end="2026-07-20", price=100.0, volume=1e6):
    """Deterministic weekday EOD payload (gently rising close; EODHD-shaped rows)."""
    from datetime import date, timedelta
    d0 = date.fromisoformat(start)
    d1 = date.fromisoformat(end)
    bars, i = [], 0
    cur = d0
    while cur <= d1:
        if cur.weekday() < 5:
            px = round(price * (1.0 + 0.001 * i), 4)
            bars.append({"date": cur.isoformat(), "close": px, "adjusted_close": px,
                         "volume": volume})
            i += 1
        cur = cur + timedelta(days=1)
    return bars


def _fake_downloader(table):
    def _get(symbol, _start):
        return table.get(symbol, [])
    return _get


def _mom_csv_text(env_dict) -> str:
    return (env_dict["inputs"] / eng.CUR_MOM_FILE).read_text(encoding="utf-8")


def _stale_clock(monkeypatch):
    monkeypatch.setattr(at, "_now_override", _TUE_MORNING)
    eng.clear_cache()
    plat.clear_caches()


# --------------------------------------------------------------------------- #
# Workstream A — the authoritative date contract
# --------------------------------------------------------------------------- #
class TestDateContract:
    def test_all_six_dates_exposed_independently(self, env):
        r = at.compute_readiness()
        d = r["dates"]
        assert set(d) == {"latest_completed_market_date", "alpha_market_date",
                          "portfolio_valuation_date", "fundamental_as_of_date",
                          "snapshot_preview_date", "desk_mark_date"}
        assert d["latest_completed_market_date"] == "2026-07-17"
        assert d["alpha_market_date"] == "2026-07-17"
        assert d["portfolio_valuation_date"] == "2026-07-17"
        assert d["fundamental_as_of_date"] == "2026-05-22"
        assert d["snapshot_preview_date"] == "2026-07-17"
        assert d["desk_mark_date"] is None   # empty tmp desk store

    def test_alignment_flags(self, env):
        r = at.compute_readiness()
        assert r["alpha_market_aligned"] is True
        assert r["portfolio_mark_aligned"] is True
        assert r["snapshot_confirmation_allowed"] is True
        assert r["confirmation_blockers"] == []
        assert r["state"] == at.STATE_READY

    def test_valuation_is_a_separate_field_not_collapsed(self, env, monkeypatch):
        monkeypatch.setattr(at, "_VALUATION_LOADER",
                            lambda: {"current_mark": {"as_of_market_date": "2026-07-15"}})
        r = at.compute_readiness()
        # different valuation date changes ONLY its own field + flag - never the gate
        assert r["dates"]["portfolio_valuation_date"] == "2026-07-15"
        assert r["portfolio_mark_aligned"] is False
        assert r["alpha_market_aligned"] is True
        assert r["snapshot_confirmation_allowed"] is True

    def test_fundamental_older_is_fresh_for_its_cadence(self, env):
        r = at.compute_readiness()
        # fundamental 2026-05-22 vs market 2026-07-17: two months lag = within cadence
        assert r["fundamental_freshness_status"] == at.FUND_FRESH
        assert r["snapshot_confirmation_allowed"] is True

    def test_fundamental_beyond_cadence_flagged_but_never_a_gate_blocker(self, env):
        _write_fund_panel(env["panel"], month="2026-02", extra_months=("2025-11",))
        eng.clear_cache()
        plat.clear_caches()
        r = at.compute_readiness()
        assert r["fundamental_freshness_status"] == at.FUND_STALE
        # WS-A: fundamental follows its own cadence - it never blocks the snapshot gate
        assert at.B_ALPHA_STALE not in r["confirmation_blockers"]
        assert all(b != "FUNDAMENTAL" for b in r["confirmation_blockers"])
        assert r["snapshot_confirmation_allowed"] is True


# --------------------------------------------------------------------------- #
# Workstream C — the HARD backend confirmation gate
# --------------------------------------------------------------------------- #
class TestHardGate:
    def test_stale_alpha_blocks_with_the_exact_code(self, env, monkeypatch):
        _stale_clock(monkeypatch)
        r = at.compute_readiness()
        assert r["dates"]["latest_completed_market_date"] == "2026-07-20"
        assert r["dates"]["alpha_market_date"] == "2026-07-17"
        assert r["confirmation_blockers"] == [
            "ALPHA_MARKET_DATE_BEHIND_LATEST_COMPLETED_MARKET_DATE"]
        assert r["state"] == at.STATE_STALE
        assert r["required_next_action"] == at.ACT_REFRESH
        assert r["snapshot_confirmation_allowed"] is False

    def test_endpoint_rejects_stale_confirm_and_writes_nothing(self, client, env,
                                                               monkeypatch):
        _stale_clock(monkeypatch)
        resp = client.post("/v1/research/alpha-paper-snapshots/confirm", headers=_AUTH,
                           json={"confirm": ledger.CONFIRM_TOKEN})
        assert resp.status_code == 200
        d = resp.json()
        assert d["status"] == "SNAPSHOT_CONFIRMATION_BLOCKED"
        assert d["performed_write"] is False
        assert d["confirmation_blockers"] == [
            "ALPHA_MARKET_DATE_BEHIND_LATEST_COMPLETED_MARKET_DATE"]
        assert d["required_next_action"] == "REFRESH_ALPHA_TARGET"
        assert not (env["ledger"] / ledger.SNAPSHOTS_FILE).exists()

    def test_aligned_confirm_proceeds_through_the_manual_token(self, client, env):
        d = client.post("/v1/research/alpha-paper-snapshots/confirm", headers=_AUTH,
                        json={"confirm": ledger.CONFIRM_TOKEN}).json()
        assert d["status"] == "MHZ_SNAPSHOT_CONFIRMED"
        assert d["performed_write"] is True
        assert (env["ledger"] / ledger.SNAPSHOTS_FILE).exists()

    def test_wrong_token_is_still_http_400(self, client, env):
        r = client.post("/v1/research/alpha-paper-snapshots/confirm", headers=_AUTH,
                        json={"confirm": "WRONG"})
        assert r.status_code == 400

    def test_missing_token_when_aligned_requires_confirmation(self, client, env):
        d = client.post("/v1/research/alpha-paper-snapshots/confirm", headers=_AUTH,
                        json={}).json()
        assert d["status"] == ledger.STATUS_CONFIRM_REQUIRED
        assert d["performed_write"] is False
        assert not (env["ledger"] / ledger.SNAPSHOTS_FILE).exists()

    def test_duplicate_snapshot_protected(self, client, env):
        ledger.confirm_snapshot(confirm=ledger.CONFIRM_TOKEN)
        r = at.compute_readiness()
        assert at.B_DUPLICATE in r["confirmation_blockers"]
        assert r["state"] == at.STATE_CONFIRMED
        assert r["already_confirmed_for_current_target"] is True
        d = client.post("/v1/research/alpha-paper-snapshots/confirm", headers=_AUTH,
                        json={"confirm": ledger.CONFIRM_TOKEN}).json()
        assert d["status"] == "SNAPSHOT_CONFIRMATION_BLOCKED"
        assert d["performed_write"] is False
        # exactly one snapshot remains on file
        snaps = json.loads((env["ledger"] / ledger.SNAPSHOTS_FILE).read_text("utf-8"))
        assert len(snaps["snapshots"]) == 1

    def test_target_count_25_required(self, env, monkeypatch):
        # restore the REAL contract: the 8-name fixture world is an incomplete target
        monkeypatch.setattr(at, "REQUIRED_TARGET_COUNT", 25)
        r = at.compute_readiness()
        assert at.B_TARGET_COUNT in r["confirmation_blockers"]
        assert r["snapshot_confirmation_allowed"] is False

    def test_weights_reconcile_checks(self, env):
        r = at.compute_readiness()
        assert r["weights_reconcile"] is True
        good = {"equal_weight": 0.04, "size_actual": 25, "unallocated_weight": 0.0,
                "max_individual_weight_cap": 0.10,
                "constituents": [{"ticker": "A", "weight": 0.04}]}
        assert at._weights_valid(good)[0] is True
        assert at._weights_valid({**good, "equal_weight": 0.2})[0] is False       # cap
        assert at._weights_valid({**good, "unallocated_weight": 0.5})[0] is False  # sum
        bad_rows = {**good, "constituents": [{"ticker": "A", "weight": 0.05}]}
        assert at._weights_valid(bad_rows)[0] is False                             # rows

    def test_ledger_integrity_required(self, client, env):
        env["ledger"].mkdir(parents=True, exist_ok=True)
        (env["ledger"] / ledger.SNAPSHOTS_FILE).write_text(json.dumps({
            "snapshots": [
                {"snapshot_id": "dup", "confirmation_status": "MHZ_SNAPSHOT_CONFIRMED",
                 "market_as_of_date": "2026-07-17"},
                {"snapshot_id": "dup", "confirmation_status": "MHZ_SNAPSHOT_CONFIRMED",
                 "market_as_of_date": "2026-07-17"},
            ]}), encoding="utf-8")
        r = at.compute_readiness()
        assert at.B_LEDGER_INTEGRITY in r["confirmation_blockers"]
        d = client.post("/v1/research/alpha-paper-snapshots/confirm", headers=_AUTH,
                        json={"confirm": ledger.CONFIRM_TOKEN}).json()
        assert d["status"] == "SNAPSHOT_CONFIRMATION_BLOCKED"
        assert d["performed_write"] is False

    def test_platform_not_ready_blocks(self, env, monkeypatch, tmp_path):
        monkeypatch.setenv(eng.PANEL_ENV, str(tmp_path / "missing.csv"))
        eng.clear_cache()
        plat.clear_caches()
        r = at.compute_readiness()
        assert at.B_PLATFORM_NOT_READY in r["confirmation_blockers"]
        assert at.B_INPUTS_MISSING in r["confirmation_blockers"]
        assert r["snapshot_confirmation_allowed"] is False


# --------------------------------------------------------------------------- #
# Workstream B — REFRESH ALPHA TARGET (owned data; explicit token)
# --------------------------------------------------------------------------- #
class TestRefresh:
    def _table(self):
        tickers = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "GGG", "HHH",
                   "XLOW", "XEXT", "XNON", "XHIS"]
        return {tk: _weekday_bars(price=100.0 + 3 * i)
                for i, tk in enumerate(tickers)}

    def test_refresh_requires_the_exact_token(self, env, monkeypatch):
        _stale_clock(monkeypatch)
        before = _mom_csv_text(env)
        out = at.run_refresh(confirm=None, downloader=_fake_downloader(self._table()))
        assert out["status"] == at.R_CONFIRM_REQUIRED
        assert out["performed_write"] is False
        assert _mom_csv_text(env) == before

    def test_refresh_advances_the_exact_dates_and_freezes_momentum(self, env, monkeypatch):
        _stale_clock(monkeypatch)
        before_rows = list(csv.DictReader(_mom_csv_text(env).splitlines()))
        out = at.run_refresh(confirm=at.REFRESH_CONFIRM_TOKEN,
                             downloader=_fake_downloader(self._table()))
        assert out["status"] == at.R_REFRESHED
        assert out["performed_write"] is True
        assert out["previous_alpha_market_date"] == "2026-07-17"
        assert out["resulting_alpha_market_date"] == "2026-07-20"
        assert out["latest_completed_market_date"] == "2026-07-20"
        after_rows = list(csv.DictReader(_mom_csv_text(env).splitlines()))
        assert [r["ticker"] for r in after_rows] == [r["ticker"] for r in before_rows]
        for b, a in zip(before_rows, after_rows):
            assert a["mom_6_1"] == b["mom_6_1"]              # frozen momentum formula
            assert a["month_label"] == b["month_label"]      # frozen month
            assert a["is_member"] == b["is_member"]          # frozen monthly membership
            assert a["extreme_flag"] == b["extreme_flag"]
            assert a["market_as_of_date"] == "2026-07-20"
        assert out["model_formulas_changed"] is False
        assert out["model_weights_changed"] is False
        assert out["historical_evidence_modified"] is False
        # the monthly panel (historical evidence) is untouched
        assert (env["inputs"] / eng.MONTHLY_PANEL_FILE).read_text("utf-8")

    def test_refresh_makes_the_gate_pass(self, env, monkeypatch):
        _stale_clock(monkeypatch)
        assert at.compute_readiness()["state"] == at.STATE_STALE
        out = at.run_refresh(confirm=at.REFRESH_CONFIRM_TOKEN,
                             downloader=_fake_downloader(self._table()))
        ra = out["readiness_after"]
        assert ra["alpha_market_aligned"] is True
        assert ra["snapshot_confirmation_allowed"] is True
        assert ra["state"] == at.STATE_READY

    def test_refresh_never_confirms_and_creates_nothing(self, env, monkeypatch):
        _stale_clock(monkeypatch)
        out = at.run_refresh(confirm=at.REFRESH_CONFIRM_TOKEN,
                             downloader=_fake_downloader(self._table()))
        assert out["snapshot_confirmed"] is False
        assert out["orders_created"] is False
        assert out["signals_created"] is False
        assert out["trade_decisions_created"] is False
        assert out["fills_created"] is False
        assert out["alpha_book_initialized"] is False
        assert out["prediction_service_called"] is False
        assert not (env["ledger"] / ledger.SNAPSHOTS_FILE).exists()   # no snapshot
        assert not env["desk"].exists() or not list(env["desk"].iterdir())  # no desk rows

    def test_refresh_already_fresh_is_a_no_write(self, env):
        before = _mom_csv_text(env)
        out = at.run_refresh(confirm=at.REFRESH_CONFIRM_TOKEN,
                             downloader=_fake_downloader(self._table()))
        assert out["status"] == at.R_ALREADY_FRESH
        assert out["performed_write"] is False
        assert _mom_csv_text(env) == before

    def test_refresh_month_boundary_defers_to_research(self, env, monkeypatch):
        monkeypatch.setattr(at, "_now_override", _AUG_TUE)
        eng.clear_cache()
        plat.clear_caches()
        before = _mom_csv_text(env)
        out = at.run_refresh(confirm=at.REFRESH_CONFIRM_TOKEN,
                             downloader=_fake_downloader(self._table()))
        assert out["status"] == at.R_MONTH_BOUNDARY
        assert out["performed_write"] is False
        assert out["required_next_action"] == "RUN_RESEARCH_MONTHLY_INPUT_EMITTER"
        assert _mom_csv_text(env) == before

    def test_refresh_provider_blocked_writes_nothing(self, env, monkeypatch):
        _stale_clock(monkeypatch)
        before = _mom_csv_text(env)

        class _KeyErr(Exception):
            error_type = "invalid_key"

        def _dl(_symbol, _start):
            raise _KeyErr("blocked")

        out = at.run_refresh(confirm=at.REFRESH_CONFIRM_TOKEN, downloader=_dl)
        assert out["status"] == at.R_PROVIDER_BLOCKED
        assert out["performed_write"] is False
        assert _mom_csv_text(env) == before

    def test_refresh_insufficient_coverage_writes_nothing(self, env, monkeypatch):
        _stale_clock(monkeypatch)
        before = _mom_csv_text(env)
        out = at.run_refresh(confirm=at.REFRESH_CONFIRM_TOKEN,
                             downloader=_fake_downloader({}))
        assert out["status"] == at.R_INSUFFICIENT
        assert out["performed_write"] is False
        assert _mom_csv_text(env) == before

    def test_refresh_endpoint_token_gate_and_fixture_source(self, client, env,
                                                            monkeypatch, tmp_path):
        _stale_clock(monkeypatch)
        d = client.post("/v1/alpha-target/refresh", headers=_AUTH, json={}).json()
        assert d["status"] == at.R_CONFIRM_REQUIRED and d["performed_write"] is False
        fixture = tmp_path / "refresh_fixture.json"
        fixture.write_text(json.dumps(self._table()), encoding="utf-8")
        monkeypatch.setenv(at.REFRESH_FIXTURE_ENV, str(fixture))
        d = client.post("/v1/alpha-target/refresh", headers=_AUTH,
                        json={"confirm": at.REFRESH_CONFIRM_TOKEN}).json()
        assert d["status"] == at.R_REFRESHED
        assert d["source"] == "FIXTURE"
        assert d["resulting_alpha_market_date"] == "2026-07-20"

    def test_no_gcp_or_tunnel_dependency_in_the_module(self):
        src = (Path(__file__).resolve().parents[1] / "api" / "alpha_target.py"
               ).read_text(encoding="utf-8").lower()
        for needle in ("prediction_client", "fetch_predictions", "127.0.0.1:9000",
                       ":9000", "requests.get", "requests.post", "httpx",
                       "prediction/health", "iap-tunnel"):
            assert needle not in src, needle


# --------------------------------------------------------------------------- #
# Workstream D/G — the 25-name review payload + endpoint safety
# --------------------------------------------------------------------------- #
class TestReviewPayload:
    def test_complete_25_name_target_and_checklist(self, big_env):
        d = at.load_review()
        assert d["status"] == "ALPHA_TARGET_REVIEW_READY"
        assert d["state"] == at.STATE_READY
        rows = d["target_table"]
        assert len(rows) == 25
        assert [r["rank"] for r in rows] == list(range(1, 26))
        assert all(abs(r["target_weight"] - 0.04) < 1e-9 for r in rows)
        for col in ("ticker", "fund_rank", "mom_rank", "agreement", "sector",
                    "risk_status", "reason"):
            assert all(col in r for r in rows), col
        items = {c["item"] for c in d["approval_checklist"]}
        for label in ("Latest market data acquired", "Alpha target recalculated",
                      "Target count = 25", "Weights reconcile", "Sector cap passed",
                      "Liquidity passed", "Ledger integrity passed",
                      "No orders created", "No broker", "Automation off"):
            assert label in items, label
        assert d["checklist_all_pass"] is True
        s = d["target_summary"]
        assert s["target_count"] == 25
        assert abs(s["target_weight_per_name"] - 0.04) < 1e-9
        assert s["largest_sector"] in {v for v in _BIG_SECTORS.values()}
        assert s["estimated_turnover"] is not None

    def test_stale_state_fails_the_market_data_checklist_item(self, big_env, monkeypatch):
        _stale_clock(monkeypatch)
        d = at.load_review()
        assert d["state"] == at.STATE_STALE
        by_key = {c["key"]: c for c in d["approval_checklist"]}
        assert by_key["market_data"]["status"] == "FAIL"
        assert d["checklist_all_pass"] is False
        assert d["readiness"]["confirmation_blockers"] == [
            "ALPHA_MARKET_DATE_BEHIND_LATEST_COMPLETED_MARKET_DATE"]

    def test_aligned_25_name_confirm_flow_through_endpoint(self, big_client, big_env):
        d = big_client.post("/v1/research/alpha-paper-snapshots/confirm", headers=_AUTH,
                            json={"confirm": ledger.CONFIRM_TOKEN}).json()
        assert d["status"] == "MHZ_SNAPSHOT_CONFIRMED"
        assert d["performed_write"] is True


class TestEndpointsAuthAndSafety:
    def test_auth_required(self, client):
        assert client.get("/v1/alpha-target/readiness").status_code in (401, 403)
        assert client.get("/v1/alpha-target/review").status_code in (401, 403)
        assert client.post("/v1/alpha-target/refresh").status_code in (401, 403)

    def test_gets_carry_the_workstream_g_safety_fields(self, client, env):
        for path in ("/v1/alpha-target/readiness", "/v1/alpha-target/review"):
            d = client.get(path, headers=_AUTH).json()
            assert d["paper_only"] is True, path
            assert d["broker_enabled"] is False, path
            assert d["automation_enabled"] is False, path
            assert d["live_orders_enabled"] is False, path
            assert d["performed_write"] is False, path
            assert d["prediction_tunnel_required"] is False, path

    def test_gets_write_nothing(self, client, env):
        client.get("/v1/alpha-target/readiness", headers=_AUTH)
        client.get("/v1/alpha-target/review", headers=_AUTH)
        assert not (env["ledger"] / ledger.SNAPSHOTS_FILE).exists()
        assert not env["desk"].exists() or not list(env["desk"].iterdir())

    def test_readiness_endpoint_exposes_the_contract(self, client, env):
        d = client.get("/v1/alpha-target/readiness", headers=_AUTH).json()
        assert d["status"] == "ALPHA_TARGET_READINESS_READY"
        assert "alpha_market_aligned" in d
        assert "portfolio_mark_aligned" in d
        assert "fundamental_freshness_status" in d
        assert "snapshot_confirmation_allowed" in d
        assert isinstance(d["confirmation_blockers"], list)
        assert d["refresh_required_token"] == at.REFRESH_CONFIRM_TOKEN


# --------------------------------------------------------------------------- #
# UI static contract (Workstreams D/E/F)
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def html() -> str:
    return _UI.read_text(encoding="utf-8")


class TestUiOperationalPanel:
    def _pm_region(self, html):
        start = html.index('id="tab-portfolio-manager"')
        end = html.index("Phase 26 PORTFOLIO MANAGER END")
        return html[start:end]

    def test_review_panel_present_with_all_sections(self, html):
        region = self._pm_region(html)
        assert 'id="otr-band"' in region
        assert "OPERATIONAL TARGET REVIEW" in region
        for did in ("otr-state", "otr-next-action", "otr-blockers", "otr-dates",
                    "otr-summary", "otr-checklist", "otr-table", "otr-act-refresh",
                    "otr-act-preview", "otr-act-confirm", "otr-confirm-box",
                    "otr-confirm-phrase", "otr-preview-facts", "otr-action-result"):
            assert 'id="%s"' % did in region, did
        assert "DATE READINESS" in region
        assert "TARGET SUMMARY" in region
        assert "APPROVAL CHECKLIST" in region
        assert "COMPLETE TARGET PORTFOLIO" in region

    def test_operational_vertical_sequence(self, html):
        # 1. review  2. alpha book plan  3. paper desk  4. advanced/audit
        assert (html.index('id="otr-band"') < html.index('id="ab-band"')
                < html.index('id="pd-band"') < html.index('id="pm-advanced"')
                < html.index('id="pm-audit"'))

    def test_advanced_details_collapsed_below_operational_flow(self, html):
        region = self._pm_region(html)
        m = re.search(r'<details[^>]*id="pm-advanced"[^>]*>', region)
        assert m and "open" not in m.group(0)
        # the old cockpit (action summary etc.) lives inside the advanced details
        assert region.index('id="pm-advanced"') < region.index('id="pm-actions"')
        assert region.index('id="pm-advanced"') < region.index('id="pm-audit"')

    def test_state_vocabulary_and_tokens_wired(self, html):
        assert "'STALE_TARGET': 'STALE TARGET'" in html
        assert "'READY_TO_CONFIRM': 'READY TO CONFIRM'" in html
        assert "'CONFIRMED': 'CONFIRMED'" in html
        assert "CONFIRM_ALPHA_TARGET_REFRESH" in html
        assert "CONFIRM_MHZ_PAPER_SNAPSHOT" in html
        assert "/v1/alpha-target/review" in html
        assert "/v1/alpha-target/refresh" in html

    def test_exact_blocker_visible_on_the_page(self, html):
        region = self._pm_region(html)
        assert "ALPHA_MARKET_DATE_BEHIND_LATEST_COMPLETED_MARKET_DATE" in region

    def test_refresh_is_the_prominent_stale_action(self, html):
        assert "_style(refreshBtn, state === 'STALE_TARGET')" in html

    def test_confirm_disabled_by_default_and_after_preview_only(self, html):
        m = re.search(r'<button[^>]*id="otr-act-confirm"[^>]*>', html)
        assert m and "disabled" in m.group(0)
        assert "window._otrPreviewDone" in html
        assert "SNAPSHOT_CONFIRMATION_BLOCKED" in html

    def test_fundamental_cadence_explained(self, html):
        region = self._pm_region(html)
        assert "OWN quarterly cadence" in region

    def test_safety_badges_on_the_review_panel(self, html):
        region = self._pm_region(html)
        band = region[region.index('id="otr-band"'):region.index('id="ab-band"')]
        for badge in ("LOCAL ALPHA DATA", "NO PREDICTION TUNNEL REQUIRED",
                      "PREVIEW ONLY", "NO ORDERS", "AUTOMATION OFF", "MANUAL REVIEW"):
            assert badge in band, badge

    def test_no_blank_buttons_in_review_panel(self, html):
        region = self._pm_region(html)
        band = region[region.index('id="otr-band"'):region.index('id="ab-band"')]
        for m in re.finditer(r"<button[^>]*>(.*?)</button>", band, re.DOTALL):
            label = re.sub(r"<[^>]+>", "", m.group(1))
            label = re.sub(r"&[a-z#0-9]+;", "x", label)
            assert label.strip(), m.group(0)[:120]
        assert "Connect to load" not in band

    def test_no_native_dialogs_anywhere(self, html):
        scripts = "\n".join(re.findall(r"<script[^>]*>(.*?)</script>", html, re.DOTALL))
        for pat in (r"(?<![A-Za-z0-9_])alert\s*\(", r"(?<![A-Za-z0-9_])confirm\s*\(",
                    r"(?<![A-Za-z0-9_])prompt\s*\("):
            assert not re.search(pat, scripts), pat


class TestUiHeaderNeverImpliesTunnelRequired:
    def test_header_shows_local_alpha_data(self, html):
        header = html[html.index("<header>"):html.index("</header>")]
        assert 'id="alpha-data-badge"' in header
        assert "LOCAL ALPHA DATA" in header
        assert "NO PREDICTION TUNNEL REQUIRED" in header

    def test_prediction_path_badge_removed_phase27c(self, html):
        # Phase 27C hard cutover: the technical LEGACY PREDICTION PATH header badge
        # was REMOVED from the operator header. The header still carries the
        # LOCAL ALPHA DATA reassurance and never implies the tunnel is required.
        header = html[html.index("<header>"):html.index("</header>")]
        assert "LEGACY PREDICTION PATH" not in header
        assert 'id="pred-health-badge"' not in header
        assert "LOCAL ALPHA DATA" in header
        assert "el.textContent = 'LEGACY PREDICTION PATH'" not in html

    def test_lab_check_prediction_states_preserved(self, html):
        # The user-triggered Lab/Admin prediction check keeps its explicit states
        # (the header badge is gone, but the Lab/Admin availability check remains).
        for state in ("PREDICTION AVAILABLE", "PREDICTION UNAVAILABLE"):
            assert state in html, state
