"""
tests/test_research_candidate_preview.py — Phase 4-D candidate preview loader.

These tests are fully self-contained: every load test builds a temporary
fixture package in tmp_path, so the suite does not depend on the research repo
being present on disk. In addition to behavioural tests, a set of static
source-scan guards assert the Phase 4-D safety contract: the loader imports no
database / network / broker / order / trade / prediction / FastAPI code, writes
no files, adds no route, and does not wire itself into the app or the UI.
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

import pytest

from paper_trader.api.research_candidate_preview import (
    CandidatePreviewError,
    REQUIRED_PACKAGE_FILES,
    REQUIRED_RECOMMENDATION,
    SAFETY_BADGES,
    SAFETY_FLAGS,
    load_candidate_preview,
)

# Repo root = tests/ parent. Used to locate the module + app/UI files on disk.
_REPO_ROOT = Path(__file__).resolve().parents[1]
_MODULE_PATH = _REPO_ROOT / "api" / "research_candidate_preview.py"
_APP_PATH = _REPO_ROOT / "api" / "app.py"
_UI_PATH = _REPO_ROOT / "api" / "ui" / "index.html"

_REQUIRED_BADGES = (
    "PREVIEW ONLY",
    "NON-PRODUCTION CANDIDATE",
    "RESEARCH ONLY",
    "NO ORDERS",
    "NO BROKER EXECUTION",
    "NO AUTOMATION",
    "NO LIVE PORTFOLIO WEIGHTS",
    "MANUAL REVIEW REQUIRED",
    "OVERLAPPING LABEL WARNING",
    "SURVIVORSHIP BIAS WARNING",
    "2024 DRAWDOWN WARNING",
)


# ---------------------------------------------------------------------------
# Fixture package builder
# ---------------------------------------------------------------------------

def _write_fixture_package(base: Path, *, recommendation: str | None = None,
                           no_go_rows: bool = True) -> Path:
    """Write a minimal-but-valid Phase 4-B package into ``base``."""
    base.mkdir(parents=True, exist_ok=True)
    rec = REQUIRED_RECOMMENDATION if recommendation is None else recommendation

    package = {
        "phase": "4-B",
        "candidate_id": "NPC-RIDGE-CRI-126D-TOP10EW-25BPS",
        "candidate_name": "Ridge Combined Regime-Interactions (126d) / Top-10 Equal-Weight",
        "selected_candidate": {
            "model_name": "ridge_combined_regime_interactions",
            "horizon": "126d",
            "strategy_name": "top_10_equal_weight",
            "holdings_target": 10,
            "max_single_name_weight": 0.1,
            "orders_allowed": False,
            "automation_allowed": False,
            "paper_trader_preview_only": True,
        },
        "evidence_summary": {
            "mean_rank_ic": 0.038434,
            "sharpe_at_25bps": 0.83317,
            "annualized_return_at_25bps": 0.18426994,
            "max_drawdown_at_25bps": -0.33770963,
            "leakage_failures": 0,
        },
        "recommendation": {"recommendation": rec, "research_only": True},
    }
    (base / "phase4b_nonproduction_candidate_package.json").write_text(
        json.dumps(package, indent=2), encoding="utf-8"
    )

    (base / "candidate_summary_card.csv").write_text(
        "field,value\n"
        "candidate_id,NPC-RIDGE-CRI-126D-TOP10EW-25BPS\n"
        "model_name,ridge_combined_regime_interactions\n"
        "horizon,126d\n"
        "strategy_name,top_10_equal_weight\n"
        "status,RESEARCH_NONPRODUCTION_CANDIDATE\n"
        f"recommendation,{rec}\n",
        encoding="utf-8",
    )
    (base / "model_candidate_spec.csv").write_text(
        "spec_item,value,note\n"
        "model_name,ridge_combined_regime_interactions,best audited model\n",
        encoding="utf-8",
    )
    (base / "selected_strategy_spec.csv").write_text(
        "spec_item,value,note\n"
        "strategy_name,top_10_equal_weight,highest Sharpe at 25 bps gate\n",
        encoding="utf-8",
    )
    (base / "evidence_scorecard.csv").write_text(
        "metric,value,source,note\n"
        "mean_rank_ic,0.038434,phase3z,mean daily rank IC\n",
        encoding="utf-8",
    )
    (base / "risk_guardrails.csv").write_text(
        "guardrail,required,status,note\n"
        "preview_only,True,ENFORCED,Preview only; nothing executed.\n"
        "no_orders,True,ENFORCED,No order creation ever.\n",
        encoding="utf-8",
    )
    (base / "known_failure_modes.csv").write_text(
        "failure_mode,severity,note\n"
        "weak_2024_year_drawdown,HIGH,2024 was the weakest year.\n",
        encoding="utf-8",
    )
    (base / "preview_integration_contract.csv").write_text(
        "contract_item,allowed,category,note\n"
        "no_orders,False,must_not,Paper Trader must not create orders.\n",
        encoding="utf-8",
    )
    no_go_body = (
        "create_orders,True,No orders may be created.\n"
        "automation,True,No automation.\n"
        if no_go_rows
        else ""
    )
    (base / "no_go_items.csv").write_text(
        "no_go_item,enforced,note\n" + no_go_body, encoding="utf-8"
    )
    (base / "readiness_decision_table.csv").write_text(
        "decision_item,value,passed,note\n"
        f"final_recommendation,{rec},True,research-only\n",
        encoding="utf-8",
    )
    return base


@pytest.fixture
def fixture_package(tmp_path: Path) -> Path:
    return _write_fixture_package(tmp_path / "pkg")


# ---------------------------------------------------------------------------
# Behavioural tests
# ---------------------------------------------------------------------------

def test_loader_imports() -> None:
    """The loader and its error type import successfully."""
    assert callable(load_candidate_preview)
    assert issubclass(CandidatePreviewError, Exception)


def test_loader_returns_normalized_payload(fixture_package: Path) -> None:
    payload = load_candidate_preview(fixture_package)
    assert payload["candidate_id"] == "NPC-RIDGE-CRI-126D-TOP10EW-25BPS"
    assert payload["model_name"] == "ridge_combined_regime_interactions"
    assert payload["horizon"] == "126d"
    assert payload["strategy_name"] == "top_10_equal_weight"
    assert payload["recommendation"] == REQUIRED_RECOMMENDATION
    assert payload["status"] == "RESEARCH_NONPRODUCTION_CANDIDATE"
    # nested research evidence is copied verbatim
    assert payload["evidence_summary"]["leakage_failures"] == 0
    assert payload["selected_strategy_summary"]["holdings_target"] == 10
    # side-car CSV rows are present
    assert payload["risk_guardrails"] and isinstance(payload["risk_guardrails"], list)
    assert payload["known_failure_modes"]
    assert payload["no_go_items"]


def test_env_var_default_path(fixture_package: Path, monkeypatch) -> None:
    """With no explicit arg, the loader honours the env-var package dir."""
    monkeypatch.setenv("PAPER_TRADER_CANDIDATE_PACKAGE_DIR", str(fixture_package))
    payload = load_candidate_preview()
    assert payload["candidate_id"] == "NPC-RIDGE-CRI-126D-TOP10EW-25BPS"


def test_missing_required_file_raises(fixture_package: Path) -> None:
    (fixture_package / "evidence_scorecard.csv").unlink()
    with pytest.raises(CandidatePreviewError, match="missing required file"):
        load_candidate_preview(fixture_package)


def test_missing_directory_raises(tmp_path: Path) -> None:
    with pytest.raises(CandidatePreviewError, match="directory not found"):
        load_candidate_preview(tmp_path / "does_not_exist")


def test_missing_no_go_items_raises(tmp_path: Path) -> None:
    pkg = _write_fixture_package(tmp_path / "pkg", no_go_rows=False)
    with pytest.raises(CandidatePreviewError, match="no-go"):
        load_candidate_preview(pkg)


def test_wrong_recommendation_raises(tmp_path: Path) -> None:
    pkg = _write_fixture_package(
        tmp_path / "pkg", recommendation="NONPROD_CANDIDATE_BLOCKED_RISK"
    )
    with pytest.raises(CandidatePreviewError, match="not preview-ready"):
        load_candidate_preview(pkg)


def test_payload_has_core_identity_fields(fixture_package: Path) -> None:
    payload = load_candidate_preview(fixture_package)
    for key in ("candidate_id", "model_name", "horizon", "strategy_name"):
        assert payload.get(key), f"missing/empty payload field: {key}"


def test_all_required_safety_badges_present(fixture_package: Path) -> None:
    payload = load_candidate_preview(fixture_package)
    badges = payload["safety_badges"]
    for badge in _REQUIRED_BADGES:
        assert badge in badges, f"missing safety badge: {badge}"
    assert len(badges) == len(_REQUIRED_BADGES)
    # module constant agrees with the asserted list
    assert tuple(SAFETY_BADGES) == _REQUIRED_BADGES


def test_safety_flags_are_true(fixture_package: Path) -> None:
    payload = load_candidate_preview(fixture_package)
    safety = payload["safety"]
    for flag in (
        "preview_only",
        "no_orders",
        "no_automation",
        "no_live_portfolio_weights",
        "manual_review_required",
        "no_broker_execution",
        "nonproduction_candidate",
        "research_only",
    ):
        assert safety[flag] is True, f"safety flag not True: {flag}"
    assert all(v is True for v in SAFETY_FLAGS.values())


def test_top_level_safety_flags_are_true(fixture_package: Path) -> None:
    """The required safety flags are mirrored at the top level of the payload."""
    payload = load_candidate_preview(fixture_package)
    for flag in (
        "preview_only",
        "no_orders",
        "no_automation",
        "no_live_portfolio_weights",
        "manual_review_required",
    ):
        assert payload.get(flag) is True, f"top-level safety flag not True: {flag}"
    # every nested safety flag is also surfaced at the top level
    for flag, value in SAFETY_FLAGS.items():
        assert payload.get(flag) is value, f"top-level flag mismatch: {flag}"


def test_source_files_reported(fixture_package: Path) -> None:
    payload = load_candidate_preview(fixture_package)
    reported = payload["source_files"]
    assert len(reported) == len(REQUIRED_PACKAGE_FILES)
    reported_names = {Path(p).name for p in reported}
    assert reported_names == set(REQUIRED_PACKAGE_FILES)
    for path in reported:
        assert Path(path).is_file()


def test_loaded_at_is_iso_timestamp(fixture_package: Path) -> None:
    payload = load_candidate_preview(fixture_package)
    # parseable as ISO-8601 (raises ValueError otherwise)
    from datetime import datetime

    datetime.fromisoformat(payload["loaded_at"])


def test_loader_does_not_modify_package_files(fixture_package: Path) -> None:
    """Hash every package file before and after loading; nothing may change."""
    def _digest(p: Path) -> str:
        return hashlib.sha256(p.read_bytes()).hexdigest()

    before = {name: _digest(fixture_package / name) for name in REQUIRED_PACKAGE_FILES}
    names_before = sorted(p.name for p in fixture_package.iterdir())

    load_candidate_preview(fixture_package)

    after = {name: _digest(fixture_package / name) for name in REQUIRED_PACKAGE_FILES}
    names_after = sorted(p.name for p in fixture_package.iterdir())

    assert before == after, "loader mutated package file contents"
    assert names_before == names_after, "loader added or removed package files"


# ---------------------------------------------------------------------------
# Static safety-contract guards (source scans)
# ---------------------------------------------------------------------------

def _module_source() -> str:
    return _MODULE_PATH.read_text(encoding="utf-8")


def _import_targets(source: str) -> list[str]:
    """Return every imported module target (the dotted path after import/from)."""
    targets: list[str] = []
    for line in source.splitlines():
        m = re.match(r"\s*(?:from|import)\s+([\w\.]+)", line)
        if m:
            targets.append(m.group(1))
    return targets


def test_no_database_imports() -> None:
    targets = _import_targets(_module_source())
    forbidden_roots = {"sqlalchemy", "psycopg2", "alembic"}
    for t in targets:
        root = t.split(".")[0]
        assert root not in forbidden_roots, f"forbidden DB import: {t}"
        assert "session" not in t.lower(), f"forbidden session import: {t}"
        assert not t.startswith("paper_trader.db"), f"forbidden db import: {t}"


def test_no_database_write_calls() -> None:
    src = _module_source()
    for needle in (".commit(", ".add(", "INSERT INTO", "UPDATE ", "DELETE FROM",
                   "get_settings", "get_session", "to_csv", "json.dump", ".write("):
        assert needle not in src, f"forbidden DB/write token present: {needle!r}"


def test_no_network_imports() -> None:
    targets = _import_targets(_module_source())
    forbidden_roots = {"requests", "httpx", "urllib", "http", "socket", "aiohttp"}
    for t in targets:
        root = t.split(".")[0]
        assert root not in forbidden_roots, f"forbidden network import: {t}"


def test_no_order_trade_broker_prediction_imports() -> None:
    targets = _import_targets(_module_source())
    for t in targets:
        low = t.lower()
        for needle in ("broker", "prediction_client", "engine.decision",
                       "engine.order", "engine.trade"):
            assert needle not in low, f"forbidden import: {t}"


def test_no_fastapi_route_added() -> None:
    src = _module_source()
    # No FastAPI import (prose in the docstring may mention it; imports may not).
    targets = _import_targets(src)
    for t in targets:
        assert "fastapi" not in t.lower(), f"forbidden FastAPI import: {t}"
        assert "starlette" not in t.lower(), f"forbidden ASGI import: {t}"
    # No route/router definitions anywhere in the module.
    low = src.lower()
    assert "apirouter" not in low, "Phase 4-D must not define a router"
    assert "add_api_route" not in src
    assert "@app." not in src and "@router." not in src


def test_loader_writes_no_files_in_source() -> None:
    """No write modes appear in the loader's open() calls."""
    src = _module_source()
    assert '"w"' not in src and "'w'" not in src
    assert '"wb"' not in src and "'wb'" not in src
    assert '"a"' not in src and "'a'" not in src


def test_app_wires_loader_read_only() -> None:
    """Phase 4-E wires the loader into app.py as a single read-only GET route.

    The app may import and call the loader, but it must not turn the preview
    into a write/order/automation path: the route is a GET, and the loader is
    never used to create signals / trade decisions / orders.
    """
    app_src = _APP_PATH.read_text(encoding="utf-8")
    # Phase 4-E: the loader is now referenced and exposed via a GET endpoint.
    assert "load_candidate_preview" in app_src
    assert "/v1/research/candidate-preview" in app_src
    # The candidate-preview route is read-only: declared with @app.get, never
    # @app.post. Confirm the path is introduced by a GET decorator block.
    assert '@app.get(\n    "/v1/research/candidate-preview"' in app_src
    assert '@app.post(\n    "/v1/research/candidate-preview"' not in app_src


def test_ui_not_referenced_by_loader() -> None:
    """The loader does not touch or reference the UI file."""
    src = _module_source().lower()
    assert "index.html" not in src
    assert "/ui/" not in src
    # the UI file still exists and is untouched by this module's import surface
    assert _UI_PATH.is_file()
