"""
tests/test_current_alpha_preview.py — Phase 13-B current-alpha preview loader.

Fully self-contained: every load test builds a temporary Phase 13-A fixture
package in tmp_path, so the suite does not depend on the research repo being
present on disk. In addition to behavioural tests, a set of static source-scan
guards assert the Phase 13-B safety contract: the loader imports no database /
network / broker / order / trade / prediction / FastAPI code, writes no files,
adds no route, and does not touch the UI. It only READS the Phase 13-A package.
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

import pytest

from paper_trader.api.current_alpha_preview import (
    CHAMPION_SIGNAL,
    CurrentAlphaPreviewError,
    REQUIRED_PACKAGE_FILES,
    SAFETY_BADGES,
    SAFETY_FLAGS,
    load_current_alpha_preview,
)

_REPO_ROOT = Path(__file__).resolve().parents[1]
_MODULE_PATH = _REPO_ROOT / "api" / "current_alpha_preview.py"
_APP_PATH = _REPO_ROOT / "api" / "app.py"
_UI_PATH = _REPO_ROOT / "api" / "ui" / "index.html"

_REQUIRED_BADGES = (
    "PREVIEW ONLY",
    "NO ORDERS",
    "NO BROKER",
    "NO AUTOMATION",
    "MANUAL REVIEW ONLY",
    "PAPER TEST ONLY",
)

_CAND_HEADER = (
    "rank,ticker,sector,composite_sn,fcf_leg_z,accruals_leg_z,"
    "liquidity_proxy,has_local_price\n"
)


# ---------------------------------------------------------------------------
# Fixture package builder (minimal-but-valid Phase 13-A package)
# ---------------------------------------------------------------------------

def _write_fixture_package(base: Path, *, phase: str = "13-A",
                           decision: str = "CURRENT_ALPHA_READY_FOR_PAPER_TEST",
                           omit: str | None = None) -> Path:
    """Write a minimal-but-valid Phase 13-A package into ``base``.

    ``omit`` optionally skips writing one required file (to exercise the
    incomplete-package error path).
    """
    base.mkdir(parents=True, exist_ok=True)

    def _w(name: str, text: str) -> None:
        if name == omit:
            return
        (base / name).write_text(text, encoding="utf-8")

    package = {
        "phase": phase,
        "decision": decision,
        "decision_rationale": "panel loaded; latest cross-section ranked",
        "go_no_go": "GO_PAPER_ONLY_WITH_CAVEATS_NOT_LIVE",
        "go_no_go_note": "paper-only recommendation; never a live-trading signal",
        "champion_signal": "composite_sn",
        "champion_definition": "sector-neutral composite; quarterly 63d rank",
        "signal_date": "2026-05-22",
        "cross_section_month": "2026-05",
        "cross_section_unit": "calendar_month (within_month_z)",
        "n_ranked": 234,
        "holding_horizon_trading_days": 63,
        "rebalance_cadence": "QUARTERLY",
        "next_rebalance_target": "2026-08-22",
        "weighting": "EQUAL_WEIGHT_LONG_ONLY",
        "package_date": "2026-06-26",
        "days_since_signal": 35,
        "stale_warning": True,
        "stale_thresholds": {"warn_days": 30, "reject_days": 120},
        "price_coverage": {"top25": 14, "top50": 24, "bottom25": 17},
        "sector_coverage": {"Unknown": 195, "Health Care": 10},
        "spy_benchmark_available_locally": False,
        "expected_benchmark": {
            "benchmark_signal": "composite_sn",
            "ic_t_63d": 2.665,
            "quarterly_net_25bps": 0.00401,
        },
        "expected_benchmark_caveat": "10-D composite_sn is a full-rank L/S backtest; a book only approximates it.",
    }
    _w(
        "phase13a_current_champion_alpha_paper_test_package.json",
        json.dumps(package, indent=2),
    )

    _w("current_alpha_full_ranked_universe.csv",
       _CAND_HEADER + "1,EXPE,Unknown,9.21,4.08,5.13,319234945.76,True\n")
    _w("current_alpha_top25_candidates.csv",
       _CAND_HEADER
       + "1,EXPE,Unknown,9.212906,4.083177,5.129729,319234945.76,True\n"
       + "2,EA,Unknown,8.296191,3.817114,4.479077,356483975.59,True\n")
    _w("current_alpha_top50_candidates.csv",
       _CAND_HEADER
       + "1,EXPE,Unknown,9.212906,4.083177,5.129729,319234945.76,True\n"
       + "2,EA,Unknown,8.296191,3.817114,4.479077,356483975.59,True\n")
    _w("current_alpha_bottom25_avoid_list.csv",
       "rank_from_bottom,ticker,sector,composite_sn,fcf_leg_z,accruals_leg_z,"
       "liquidity_proxy,has_local_price,note\n"
       "1,CTVA,Unknown,-6.125849,-2.785119,-3.34073,193732931.55,True,"
       "AVOID / short-only diagnostic (NOT a live recommendation)\n")
    _w("current_alpha_sector_exposure.csv",
       "book,sector,n_names,weight_pct,flag\n"
       "TOP25,Unknown,25,100.0,CONCENTRATED\n")
    _w("current_alpha_missing_data_report.csv",
       "issue,scope,n_names,tickers,note\n"
       "NO_LOCAL_PRICE,top25,11,APP;SNDK,no owned local EOD file\n")
    _w("current_alpha_paper_portfolio_top25.csv",
       "rank,ticker,side,target_weight,order_action\n"
       "1,EXPE,LONG,0.04,NO_ORDER\n")
    _w("current_alpha_paper_portfolio_top50.csv",
       "rank,ticker,side,target_weight,order_action\n"
       "1,EXPE,LONG,0.02,NO_ORDER\n")
    _w("current_alpha_tracking_template.csv",
       "ticker,entry_price,chk_1w_return,chk_63d_return\n"
       "EXPE,100.0,,\n")
    _w("current_alpha_risk_limits.csv",
       "limit,value,enforcement,note\n"
       "PREVIEW ONLY,YES,HARD,candidate list only; nothing is executed\n"
       "NO ORDERS,CONFIRMED,HARD,order_action=NO_ORDER on every row\n")
    _w("current_alpha_go_no_go_scorecard.csv",
       "criterion,status,value,threshold,note\n"
       "panel_loaded,PASS,True,required,frozen 10-L panel\n"
       "signal_freshness,WARN,35d,warn>30d,stale warning\n")
    return base


@pytest.fixture
def fixture_package(tmp_path: Path) -> Path:
    return _write_fixture_package(tmp_path / "pkg")


# ---------------------------------------------------------------------------
# Behavioural tests
# ---------------------------------------------------------------------------

def test_loader_imports() -> None:
    assert callable(load_current_alpha_preview)
    assert issubclass(CurrentAlphaPreviewError, Exception)


def test_loader_returns_normalized_payload(fixture_package: Path) -> None:
    payload = load_current_alpha_preview(fixture_package)
    assert payload["alpha_name"] == "composite_sn" == CHAMPION_SIGNAL
    assert payload["decision"] == "CURRENT_ALPHA_READY_FOR_PAPER_TEST"
    assert payload["go_no_go"] == "GO_PAPER_ONLY_WITH_CAVEATS_NOT_LIVE"
    assert payload["signal_date"] == "2026-05-22"
    assert payload["cross_section_month"] == "2026-05"
    assert payload["n_ranked"] == 234
    # candidate books are present and copied verbatim
    assert len(payload["top25_candidates"]) == 2
    assert payload["top25_candidates"][0]["ticker"] == "EXPE"
    assert len(payload["top50_candidates"]) == 2
    assert payload["bottom25_avoid"][0]["ticker"] == "CTVA"
    # top-10 tickers derived from the top-25 book, in order
    assert payload["top10_tickers"] == ["EXPE", "EA"]
    # diagnostics side-cars
    assert payload["sector_exposure"] and payload["risk_limits"]
    assert payload["go_no_go_scorecard"]
    assert payload["caveats"], "caveats must be derived from the package"


def test_env_var_default_path(fixture_package: Path, monkeypatch) -> None:
    monkeypatch.setenv(
        "PAPER_TRADER_CURRENT_ALPHA_PACKAGE_DIR", str(fixture_package)
    )
    payload = load_current_alpha_preview()
    assert payload["signal_date"] == "2026-05-22"


def test_missing_directory_raises(tmp_path: Path) -> None:
    with pytest.raises(CurrentAlphaPreviewError, match="Phase 13-A package not found"):
        load_current_alpha_preview(tmp_path / "does_not_exist")


def test_missing_required_file_raises(tmp_path: Path) -> None:
    pkg = _write_fixture_package(
        tmp_path / "pkg", omit="current_alpha_top25_candidates.csv"
    )
    with pytest.raises(CurrentAlphaPreviewError, match="missing required file"):
        load_current_alpha_preview(pkg)


def test_wrong_phase_raises(tmp_path: Path) -> None:
    pkg = _write_fixture_package(tmp_path / "pkg", phase="9-Z")
    with pytest.raises(CurrentAlphaPreviewError, match="not a Phase 13-A"):
        load_current_alpha_preview(pkg)


def test_required_safety_badges_exactly_six(fixture_package: Path) -> None:
    payload = load_current_alpha_preview(fixture_package)
    badges = payload["safety_badges"]
    assert tuple(badges) == _REQUIRED_BADGES
    assert len(badges) == 6
    assert tuple(SAFETY_BADGES) == _REQUIRED_BADGES


def test_safety_flags_contract(fixture_package: Path) -> None:
    payload = load_current_alpha_preview(fixture_package)
    safety = payload["safety"]
    # positive guarantees are True
    for flag in ("preview_only", "paper_test_only", "manual_review_only",
                 "read_only", "no_orders", "no_broker", "no_automation"):
        assert safety[flag] is True, f"safety guarantee not True: {flag}"
    # "does not do X" guarantees are False
    for flag in ("creates_signals", "creates_trade_decisions",
                 "wrote_to_paper_trader", "calls_prediction_service",
                 "calls_external_providers", "uses_paid_data", "live_trading"):
        assert safety[flag] is False, f"action flag not False: {flag}"


def test_top_level_safety_flags_mirror(fixture_package: Path) -> None:
    payload = load_current_alpha_preview(fixture_package)
    for flag, value in SAFETY_FLAGS.items():
        assert payload.get(flag) is value, f"top-level flag mismatch: {flag}"


def test_source_files_reported(fixture_package: Path) -> None:
    payload = load_current_alpha_preview(fixture_package)
    reported = payload["source_file_paths"]
    assert len(reported) == len(REQUIRED_PACKAGE_FILES)
    reported_names = {Path(p).name for p in reported}
    assert reported_names == set(REQUIRED_PACKAGE_FILES)
    for path in reported:
        assert Path(path).is_file()


def test_loaded_at_is_iso_timestamp(fixture_package: Path) -> None:
    from datetime import datetime
    payload = load_current_alpha_preview(fixture_package)
    datetime.fromisoformat(payload["loaded_at"])


def test_loader_does_not_modify_package_files(fixture_package: Path) -> None:
    def _digest(p: Path) -> str:
        return hashlib.sha256(p.read_bytes()).hexdigest()

    before = {n: _digest(fixture_package / n) for n in REQUIRED_PACKAGE_FILES}
    names_before = sorted(p.name for p in fixture_package.iterdir())

    load_current_alpha_preview(fixture_package)

    after = {n: _digest(fixture_package / n) for n in REQUIRED_PACKAGE_FILES}
    names_after = sorted(p.name for p in fixture_package.iterdir())

    assert before == after, "loader mutated package file contents"
    assert names_before == names_after, "loader added or removed package files"


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


def test_no_database_imports() -> None:
    for t in _import_targets(_module_source()):
        root = t.split(".")[0]
        assert root not in {"sqlalchemy", "psycopg2", "alembic"}, f"DB import: {t}"
        assert "session" not in t.lower(), f"session import: {t}"
        assert not t.startswith("paper_trader.db"), f"db import: {t}"


def test_no_database_write_calls() -> None:
    src = _module_source()
    for needle in (".commit(", ".add(", "INSERT INTO", "UPDATE ", "DELETE FROM",
                   "get_settings", "get_session", "to_csv", "json.dump", ".write("):
        assert needle not in src, f"forbidden DB/write token present: {needle!r}"


def test_no_network_imports() -> None:
    for t in _import_targets(_module_source()):
        root = t.split(".")[0]
        assert root not in {"requests", "httpx", "urllib", "http", "socket",
                            "aiohttp"}, f"network import: {t}"


def test_no_order_trade_broker_prediction_imports() -> None:
    for t in _import_targets(_module_source()):
        low = t.lower()
        for needle in ("broker", "prediction_client", "engine.decision",
                       "engine.order", "engine.trade"):
            assert needle not in low, f"forbidden import: {t}"


def test_no_fastapi_route_added() -> None:
    src = _module_source()
    for t in _import_targets(src):
        assert "fastapi" not in t.lower(), f"forbidden FastAPI import: {t}"
        assert "starlette" not in t.lower(), f"forbidden ASGI import: {t}"
    low = src.lower()
    assert "apirouter" not in low
    assert "add_api_route" not in src
    assert "@app." not in src and "@router." not in src


def test_loader_writes_no_files_in_source() -> None:
    src = _module_source()
    assert '"w"' not in src and "'w'" not in src
    assert '"wb"' not in src and "'wb'" not in src
    assert '"a"' not in src and "'a'" not in src


def test_app_wires_loader_read_only() -> None:
    app_src = _APP_PATH.read_text(encoding="utf-8")
    assert "load_current_alpha_preview" in app_src
    assert "/v1/research/current-alpha/preview" in app_src
    assert '@app.get(\n    "/v1/research/current-alpha/preview"' in app_src
    assert '@app.post(\n    "/v1/research/current-alpha/preview"' not in app_src


def test_ui_not_referenced_by_loader() -> None:
    src = _module_source().lower()
    assert "index.html" not in src
    assert "/ui/" not in src
    assert _UI_PATH.is_file()
