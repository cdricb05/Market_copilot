"""
tests/test_current_alpha_operations.py — Phase 13-C/D/E operations loaders.

Self-contained: every test builds a temporary Phase 13-A fixture package (with
FULL paper-portfolio schema, so PnL / action metrics are deterministic) and a
small frozen scored panel for the rebalance simulator. Behavioural tests assert
the computed numbers; static source-scan guards assert the read-only, paper-only
safety contract (no DB / network / order / broker / prediction imports or write
calls, no FastAPI route, writes no files).
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from paper_trader.api.current_alpha_operations import (
    ACTION_LABELS,
    CurrentAlphaPreviewError,
    load_current_alpha_actions_preview,
    load_current_alpha_pnl,
    load_current_alpha_rebalance_simulation,
)

# Reuse the Phase 13-B fixture builder as the package base, then overwrite the
# portfolio / tracking side-cars with the richer, full-schema versions here.
from tests.test_current_alpha_preview import _write_fixture_package

_REPO_ROOT = Path(__file__).resolve().parents[1]
_MODULE_PATH = _REPO_ROOT / "api" / "current_alpha_operations.py"
_APP_PATH = _REPO_ROOT / "api" / "app.py"

_PORTFOLIO_COLS = [
    "ticker", "side", "target_weight", "sector", "signal_composite_sn",
    "signal_date", "price_source", "entry_reference_date", "entry_price",
    "current_price", "current_price_date", "paper_return_pct", "price_status",
    "order_action", "review_status",
]


def _prow(**fields) -> str:
    return ",".join(str(fields.get(c, "")) for c in _PORTFOLIO_COLS)


def _portfolio_csv(weight: float) -> str:
    header = ",".join(_PORTFOLIO_COLS)
    rows = [
        _prow(ticker="AAA", side="LONG", target_weight=weight, sector="Health Care",
              signal_composite_sn=5.0, signal_date="2026-05-22", price_source="EODHD",
              entry_reference_date="2026-05-22", entry_price=100, current_price=110,
              current_price_date="2026-06-26", paper_return_pct=10.0, price_status="MARKED",
              order_action="NO_ORDER", review_status="PAPER_REVIEW_ONLY"),
        _prow(ticker="BBB", side="LONG", target_weight=weight, sector="Unknown",
              signal_composite_sn=4.0, signal_date="2026-05-22", price_source="EODHD",
              entry_reference_date="2026-05-22", entry_price=50, current_price=48,
              current_price_date="2026-06-26", paper_return_pct=-4.0, price_status="MARKED",
              order_action="NO_ORDER", review_status="PAPER_REVIEW_ONLY"),
        _prow(ticker="CCC", side="LONG", target_weight=weight, sector="Unknown",
              signal_composite_sn=3.0, signal_date="2026-05-22", price_source="EODHD",
              price_status="NO_LOCAL_PRICE", order_action="NO_ORDER",
              review_status="PAPER_REVIEW_ONLY"),
        _prow(ticker="DDD", side="LONG", target_weight=weight, sector="Unknown",
              signal_composite_sn=2.0, signal_date="2026-05-22", price_source="EODHD",
              entry_reference_date="2026-05-22", entry_price=200, current_price=212,
              current_price_date="2026-06-26", paper_return_pct=6.0, price_status="MARKED",
              order_action="NO_ORDER", review_status="PAPER_REVIEW_ONLY"),
    ]
    return header + "\n" + "\n".join(rows) + "\n"


_PANEL_HEADER = "as_of_date,rebalance_date,ticker,sector,composite_sn,forward_63d_return,has_forward_return"


def _panel_csv(*, quarters: int = 5, with_forward: int = 4) -> str:
    """A tiny event panel: `quarters` calendar quarters, X/Y/Z each; the last
    `quarters - with_forward` quarters have no forward return (most-recent)."""
    dates = ["2025-01-15", "2025-04-15", "2025-07-15", "2025-10-15", "2026-01-15",
             "2026-04-15", "2026-07-15", "2026-10-15"][:quarters]
    names = [("X", 3.0, 0.03), ("Y", 2.0, 0.02), ("Z", 1.0, 0.01)]
    lines = [_PANEL_HEADER]
    for i, d in enumerate(dates):
        has_fwd = i < with_forward
        for ticker, comp, fwd in names:
            fwd_val = fwd if has_fwd else ""
            lines.append(f"2026-06-26,{d},{ticker},Unknown,{comp},{fwd_val},{has_fwd}")
    return "\n".join(lines) + "\n"


def _write_ops_fixture(base: Path) -> Path:
    """Full-schema Phase 13-A package for operations tests."""
    _write_fixture_package(base)
    (base / "current_alpha_paper_portfolio_top25.csv").write_text(_portfolio_csv(0.04), encoding="utf-8")
    (base / "current_alpha_paper_portfolio_top50.csv").write_text(_portfolio_csv(0.02), encoding="utf-8")
    (base / "current_alpha_tracking_template.csv").write_text(
        "ticker,entry_price,chk_1w_return,chk_1m_return,chk_2m_return,chk_63d_return\n"
        "AAA,100.0,,,,\n", encoding="utf-8")
    return base


@pytest.fixture
def ops_package(tmp_path: Path) -> Path:
    return _write_ops_fixture(tmp_path / "pkg")


@pytest.fixture
def ops_panel(tmp_path: Path) -> Path:
    p = tmp_path / "panel.csv"
    p.write_text(_panel_csv(), encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# Phase 13-C — PnL
# ---------------------------------------------------------------------------

def test_pnl_core_fields(ops_package: Path) -> None:
    p = load_current_alpha_pnl(ops_package)
    assert p["alpha_name"] == "composite_sn"
    assert p["signal_date"] == "2026-05-22"
    assert p["decision"] == "CURRENT_ALPHA_READY_FOR_PAPER_TEST"
    assert p["go_no_go"] == "GO_PAPER_ONLY_WITH_CAVEATS_NOT_LIVE"


def test_pnl_top25_summary_math(ops_package: Path) -> None:
    p = load_current_alpha_pnl(ops_package)
    t25 = p["top25"]
    # covered = AAA(+10), BBB(-4), DDD(+6); missing = CCC (no local price)
    assert t25["covered_count"] == 3
    assert t25["missing_count"] == 1
    assert t25["total_count"] == 4
    assert t25["average_paper_return_pct"] == pytest.approx(4.0)  # (10 - 4 + 6) / 3
    assert t25["median_paper_return_pct"] == pytest.approx(6.0)
    assert t25["min_return_pct"] == pytest.approx(-4.0)
    assert t25["max_return_pct"] == pytest.approx(10.0)
    assert t25["n_up"] == 2
    assert t25["n_down"] == 1
    assert t25["hit_rate_pct"] == pytest.approx(66.67, abs=0.01)


def test_pnl_best_and_worst_performers(ops_package: Path) -> None:
    p = load_current_alpha_pnl(ops_package)
    best = p["top25"]["best_performers"]
    worst = p["top25"]["worst_performers"]
    assert best[0]["ticker"] == "AAA" and best[0]["paper_return_pct"] == pytest.approx(10.0)
    assert worst[0]["ticker"] == "BBB" and worst[0]["paper_return_pct"] == pytest.approx(-4.0)


def test_pnl_top50_summary(ops_package: Path) -> None:
    p = load_current_alpha_pnl(ops_package)
    assert p["top50"]["covered_count"] == 3
    assert p["top50"]["average_paper_return_pct"] == pytest.approx(4.0)


def test_pnl_checkpoint_plan(ops_package: Path) -> None:
    p = load_current_alpha_pnl(ops_package)
    plan = p["checkpoint_plan"]
    labels = [c["label"] for c in plan]
    assert labels == ["1 week", "1 month", "2 months", "63 trading days"]
    horizons = [c["horizon_trading_days"] for c in plan]
    assert horizons == [5, 21, 42, 63]
    # days_since_signal == 35 -> 1w/1m elapsed & uncaptured, 2m/63d pending
    by_label = {c["label"]: c for c in plan}
    assert by_label["1 week"]["status"] == "WINDOW_ELAPSED_AWAITING_PRICE_REFRESH"
    assert by_label["2 months"]["status"] == "PENDING"
    assert all(c["approx_target_date"] for c in plan)


def test_pnl_safety_surface(ops_package: Path) -> None:
    p = load_current_alpha_pnl(ops_package)
    for badge in ("PREVIEW ONLY", "NO ORDERS", "NO BROKER", "NO AUTOMATION",
                  "MANUAL REVIEW ONLY", "PAPER TEST ONLY"):
        assert badge in p["safety_badges"]
    assert p["no_orders"] is True and p["no_broker"] is True and p["no_automation"] is True
    assert p["wrote_to_paper_trader"] is False and p["live_trading"] is False


def test_pnl_missing_package_raises(tmp_path: Path) -> None:
    with pytest.raises(CurrentAlphaPreviewError, match="Phase 13-A package not found"):
        load_current_alpha_pnl(tmp_path / "nope")


# ---------------------------------------------------------------------------
# Phase 13-D — Action preview
# ---------------------------------------------------------------------------

def test_actions_labels_and_counts(ops_package: Path) -> None:
    a = load_current_alpha_actions_preview(ops_package)
    assert a["action_labels"] == list(ACTION_LABELS)
    counts = a["counts_by_action_type"]
    # top50 (AAA,BBB priced; CCC unpriced; DDD priced) + avoid (CTVA)
    assert counts["ADD_PREVIEW"] == 3
    assert counts["WAIT_FOR_PRICE_PREVIEW"] == 1
    assert counts["AVOID_PREVIEW"] == 1
    assert counts["HOLD_PREVIEW"] == 0
    assert counts["REMOVE_PREVIEW"] == 0
    assert counts["REBALANCE_PREVIEW"] == 0


def test_actions_every_row_is_no_order(ops_package: Path) -> None:
    a = load_current_alpha_actions_preview(ops_package)
    rows = a["top25_action_plan"] + a["top50_action_plan"] + a["avoid_list"]
    assert rows
    for r in rows:
        assert r["order_action"] == "NO_ORDER"
        assert "action_type" in r and "ticker" in r and "source_rank" in r
        assert "composite_sn" in r and "reason" in r and "safety_note" in r


def test_actions_missing_price_becomes_wait(ops_package: Path) -> None:
    a = load_current_alpha_actions_preview(ops_package)
    waits = [r for r in a["top25_action_plan"] if r["action_type"] == "WAIT_FOR_PRICE_PREVIEW"]
    assert [r["ticker"] for r in waits] == ["CCC"]
    adds = [r["ticker"] for r in a["top25_action_plan"] if r["action_type"] == "ADD_PREVIEW"]
    assert adds == ["AAA", "BBB", "DDD"]


def test_actions_bottom25_becomes_avoid(ops_package: Path) -> None:
    a = load_current_alpha_actions_preview(ops_package)
    assert a["avoid_list"]
    for r in a["avoid_list"]:
        assert r["action_type"] == "AVOID_PREVIEW"
        assert r["order_action"] == "NO_ORDER"
    assert a["avoid_list"][0]["ticker"] == "CTVA"


def test_actions_explicit_paper_only_language(ops_package: Path) -> None:
    a = load_current_alpha_actions_preview(ops_package)
    assert a["paper_only"] is True
    assert a["order_action_all"] == "NO_ORDER"
    notice = a["explicit_notice"]
    assert "No order is created" in notice
    assert "No signal is created" in notice
    assert "No trade decision is created" in notice
    assert "Manual review required" in notice


def test_actions_missing_package_raises(tmp_path: Path) -> None:
    with pytest.raises(CurrentAlphaPreviewError, match="Phase 13-A package not found"):
        load_current_alpha_actions_preview(tmp_path / "nope")


# ---------------------------------------------------------------------------
# Phase 13-E — Rebalance simulator
# ---------------------------------------------------------------------------

def test_simulator_quarterly_is_simulated(ops_package: Path, ops_panel: Path) -> None:
    s = load_current_alpha_rebalance_simulation(ops_package, panel_path=ops_panel)
    assert s["simulation_status"] == "SIMULATED"
    assert s["recommendation"] == "QUARTERLY_REBALANCE_CANDIDATE"
    q = s["frequencies"]["quarterly"]
    assert q["status"] == "SIMULATED"
    assert q["verdict"] == "QUARTERLY_REBALANCE_CANDIDATE"
    # 4 quarters carry forward returns; the 5th (most recent) does not
    assert q["top25"]["n_periods"] == 4
    assert q["top25"]["avg_return_pct"] == pytest.approx(2.0, abs=1e-6)  # mean(3,2,1)% = 2%
    assert q["top25"]["hit_rate_pct"] == pytest.approx(100.0)
    assert q["top25"]["turnover"] == pytest.approx(0.0)  # same names every quarter
    assert q["top25"]["missing_price_count"] == 0


def test_simulator_daily_rejected_weekly_monthly_not_justified(ops_package: Path, ops_panel: Path) -> None:
    s = load_current_alpha_rebalance_simulation(ops_package, panel_path=ops_panel)
    assert s["frequencies"]["daily"]["verdict"] == "DAILY_REBALANCE_REJECTED"
    assert s["frequencies"]["daily"]["supported_by_signal_frequency"] is False
    for name in ("weekly", "monthly"):
        f = s["frequencies"][name]
        assert f["supported_by_signal_frequency"] is False
        assert f["top25"] is None and f["top50"] is None
    assert s["daily_rebalance_supported"] is False
    assert s["daily_trading_recommended"] is False
    assert s["daily_monitoring_valid"] is True


def test_simulator_signal_refresh_is_quarterly(ops_package: Path, ops_panel: Path) -> None:
    s = load_current_alpha_rebalance_simulation(ops_package, panel_path=ops_panel)
    # ~91 calendar-day gaps -> ~65 trading days (quarterly)
    assert 55 <= s["signal_refresh_trading_days"] <= 75


def test_simulator_missing_panel_is_insufficient_not_crash(ops_package: Path, tmp_path: Path) -> None:
    s = load_current_alpha_rebalance_simulation(ops_package, panel_path=tmp_path / "absent.csv")
    assert s["simulation_status"] == "SIMULATION_INSUFFICIENT_DATA"
    assert s["recommendation"] == "SIMULATION_INSUFFICIENT_DATA"
    assert s["warnings"]
    # daily is still rejected even without a panel; still read-only, no crash
    assert s["frequencies"]["daily"]["verdict"] == "DAILY_REBALANCE_REJECTED"
    assert s["daily_monitoring_valid"] is True


def test_simulator_too_few_quarters_insufficient(ops_package: Path, tmp_path: Path) -> None:
    thin = tmp_path / "thin.csv"
    thin.write_text(_panel_csv(quarters=2, with_forward=2), encoding="utf-8")
    s = load_current_alpha_rebalance_simulation(ops_package, panel_path=thin)
    assert s["simulation_status"] == "SIMULATION_INSUFFICIENT_DATA"


def test_simulator_missing_package_raises(tmp_path: Path) -> None:
    with pytest.raises(CurrentAlphaPreviewError, match="Phase 13-A package not found"):
        load_current_alpha_rebalance_simulation(tmp_path / "nope")


def test_simulator_safety_surface(ops_package: Path, ops_panel: Path) -> None:
    s = load_current_alpha_rebalance_simulation(ops_package, panel_path=ops_panel)
    for badge in ("PREVIEW ONLY", "NO ORDERS", "NO BROKER", "NO AUTOMATION",
                  "MANUAL REVIEW ONLY", "PAPER TEST ONLY"):
        assert badge in s["safety_badges"]
    assert s["no_orders"] is True and s["no_automation"] is True


# ---------------------------------------------------------------------------
# Static safety-contract guards (source scans)
# ---------------------------------------------------------------------------

def _module_source() -> str:
    return _MODULE_PATH.read_text(encoding="utf-8")


def _import_targets(source: str) -> list[str]:
    targets: list[str] = []
    for line in source.splitlines():
        m = re.match(r"\s*(?:from|import)\s+([\w\.]+)", line)
        if m:
            targets.append(m.group(1))
    return targets


def test_no_database_or_network_imports() -> None:
    for t in _import_targets(_module_source()):
        root = t.split(".")[0]
        assert root not in {"sqlalchemy", "psycopg2", "alembic", "requests", "httpx",
                            "urllib", "http", "socket", "aiohttp"}, f"forbidden import: {t}"
        assert "session" not in t.lower(), f"session import: {t}"
        assert not t.startswith("paper_trader.db"), f"db import: {t}"


def test_no_write_or_db_calls() -> None:
    src = _module_source()
    for needle in (".commit(", "INSERT INTO", "UPDATE ", "DELETE FROM", "get_settings",
                   "get_session", "to_csv", "json.dump", ".write(", "open("):
        assert needle not in src, f"forbidden write/db token: {needle!r}"


def test_no_order_broker_prediction_or_route() -> None:
    src = _module_source()
    low = src.lower()
    for t in _import_targets(src):
        assert "fastapi" not in t.lower() and "starlette" not in t.lower()
        for needle in ("broker", "prediction_client", "engine.decision", "engine.order"):
            assert needle not in t.lower(), f"forbidden import: {t}"
    assert "@app." not in src and "@router." not in src
    assert "apirouter" not in low and "add_api_route" not in src


def test_app_wires_operations_read_only_get() -> None:
    app_src = _APP_PATH.read_text(encoding="utf-8")
    for path in ("/v1/research/current-alpha/pnl",
                 "/v1/research/current-alpha/actions-preview",
                 "/v1/research/current-alpha/rebalance-simulator"):
        assert f'@app.get(\n    "{path}"' in app_src, f"missing GET route: {path}"
        assert f'@app.post(\n    "{path}"' not in app_src, f"unexpected POST route: {path}"
