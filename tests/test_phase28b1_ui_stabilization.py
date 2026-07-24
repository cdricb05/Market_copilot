"""
tests/test_phase28b1_ui_stabilization.py

Phase 28B.1 — LIVE UI STABILIZATION, ATTRIBUTION CONSISTENCY AND CLEAN CHECKPOINT.

Two live defects are pinned by regression tests here:

  * NULL-innerHTML (Part B): generateDailyReviewSummary targeted the
    #daily-review-summary-result card that was physically removed in the Phase
    27B.7 legacy cutover, so every dashboard refresh raised "Cannot set
    properties of null (setting 'innerHTML')" and surfaced an error toast.
    The renderer now resolves optional targets through the _setHtml helper
    (update only when present) and keeps its durable effects (fetch +
    _syncDrsToUi) on every page.

  * CONTRADICTORY TODAY'S REVIEW (Part C): forward_evidence._safe_ops invoked
    the default operational-book loader positionally — load_operational_book()
    takes keyword-only arguments — so every live call raised TypeError, was
    silently swallowed into {}, and the attribution reported zero holdings with
    a misleading "per-ticker completed marks" reason while the Phase 27H
    daily-close attribution rendered a fully reconciled result beside it.
    _safe_ops now calls the loader with today= as a keyword, the priced==0
    branch distinguishes "no holdings resolved" from "marks missing", and
    todays_review carries the cumulative excess + sector contributors so both
    surfaces derive from one canonical attribution.

Also proven: explicit Daily vs Cumulative excess labels (Part D), the nested
safety contract mirroring the top-level fields (Part E), the preserved
NO_FORWARD_SNAPSHOTS empty state with no read-path capture (Part F), and that
nothing operational (holdings / cash / orders / model) is ever written.
"""
from __future__ import annotations

import copy
import inspect
import re
from pathlib import Path

import pytest

from paper_trader.api import daily_action_gate as dag
from paper_trader.api import forward_evidence as fe
from paper_trader.api import forward_prediction_skill as fps
from paper_trader.api import operational_book as ob
from paper_trader.api import paper_trading_desk as desk

from tests.test_phase27a_paper_operations import (  # reuse the offline harness
    _AUTH, client, env,  # noqa: F401
)
from tests.test_phase28a_forward_evidence_attribution import (  # 28A world
    _PERF_ROWS, _SERIES, _fake_multi_history, _fake_tournament, _holds_2,
    _marks, _ops, _perf,
)

_UI = Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html"
_DAG_SRC = Path(__file__).resolve().parents[1] / "api" / "daily_action_gate.py"


@pytest.fixture(scope="module")
def ui_html() -> str:
    return _UI.read_text(encoding="utf-8")


def _load(tmp_path, **kw):
    """The one aggregator with the deterministic offline 28A world."""
    return fe.load_forward_evidence(
        desk_dir=str(tmp_path),
        perf_loader=_perf(kw.pop("rows", _PERF_ROWS)),
        marks_loader=_marks(kw.pop("series", _SERIES)),
        ops=kw.pop("ops", _ops(holdings=_holds_2())),
        tournament_loader=_fake_tournament(),
        multi_history_loader=_fake_multi_history())


def _fps_files(tmp_path) -> set:
    sdir = desk._desk_dir(str(tmp_path))
    if not sdir.exists():
        return set()
    return {p.name for p in sdir.iterdir()} & {
        fps.SNAPSHOT_LEDGER_FILE, fps.OUTCOME_LEDGER_FILE, fps.PRICE_STORE_FILE}


# =========================================================================== #
# PART B — the null-DOM root cause is fixed at the source (scenarios 1, 2, 19)
# =========================================================================== #
class TestNullDomFix:
    def test_renderer_uses_safe_helper_not_bare_target(self, ui_html):  # (1)
        # The exact failing statement is gone: no unguarded handle taken before
        # the fetch, and the card renders through the present-only helper.
        assert "const el = document.getElementById('daily-review-summary-result');" \
            not in ui_html
        assert "_setHtml('daily-review-summary-result', html)" in ui_html

    def test_safe_helper_updates_only_when_present(self, ui_html):  # (1)
        assert "function _setHtml(id, html)" in ui_html
        assert "if (el) el.innerHTML = html;" in ui_html

    def test_summary_card_element_is_genuinely_absent(self, ui_html):  # (1)
        # Phase 27B.7 physically removed the card; the fix must NOT have
        # reintroduced an invisible dummy element just to avoid the exception.
        assert 'id="daily-review-summary-result"' not in ui_html

    def test_durable_effects_survive_missing_card(self, ui_html):  # (2)
        # The fetch + workflow-state sync stay unconditional; only the card
        # render is conditional — rapid route changes cannot lose state.
        fn = ui_html[ui_html.index("async function generateDailyReviewSummary"):]
        fn = fn[:fn.index("\n}") + 2]
        assert "window._lastDailyReviewSummary = d;" in fn
        assert "_syncDrsToUi(d);" in fn
        assert "if (el) el.style.display = 'block';" in fn

    def test_route_renderers_null_guard_their_panels(self, ui_html):  # (2, 19)
        # Every Phase 28A/28B renderer bails or skips when its page-specific
        # panel is absent (Portfolio-first vs Research-first ordering).
        assert "var todayPanel = document.getElementById('fe-today');" in ui_html
        assert "if (todayPanel) {" in ui_html
        assert "if (!panel) return;" in ui_html          # _feRenderResearch
        assert "if (!d) { panel.style.display = 'none'; return; }" in ui_html

    def test_loaders_coalesce_inflight_requests(self, ui_html):  # (2)
        assert "if (window._feInFlight) return window._feInFlight;" in ui_html


# =========================================================================== #
# PART C — one canonical Today's Review state (scenarios 3-8, 20)
# =========================================================================== #
class TestCanonicalTodaysReview:
    def test_populated_attribution_renders_todays_review(self, tmp_path):  # (3)
        out = _load(tmp_path)
        tr = out["todays_review"]
        assert tr["available"] is True
        for k in ("market_date", "daily_pnl", "daily_return_pct",
                  "spy_daily_return_pct", "daily_excess_return_pct",
                  "cumulative_pnl", "cumulative_excess_return_pct",
                  "top_positive", "top_negative", "strongest_sector",
                  "weakest_sector", "decision"):
            assert tr.get(k) is not None, k

    def test_unavailable_no_holdings_reason_is_honest(self):  # (4)
        a = fe.build_daily_attribution(
            perf_loader=_perf(_PERF_ROWS), marks_loader=_marks(_SERIES),
            ops=_ops(holdings=[]))
        assert a["available"] is False
        assert a["status"] == fe.ATTRIB_COVERAGE_INCOMPLETE
        assert "operational holdings could not be resolved" in a["reason"]
        # It must NOT blame per-ticker marks when no holdings were found.
        assert "Per-ticker completed marks" not in a["reason"]

    def test_unavailable_missing_marks_reason_is_kept(self):  # (4)
        a = fe.build_daily_attribution(
            perf_loader=_perf(_PERF_ROWS), marks_loader=_marks({}),
            ops=_ops(holdings=_holds_2()))
        assert a["available"] is False
        assert "Per-ticker completed marks" in a["reason"]
        assert a["coverage"]["missing_tickers"] == ["AAA", "BBB"]

    def test_populated_state_has_no_stale_message(self, tmp_path):  # (5)
        tr = _load(tmp_path)["todays_review"]
        assert tr["available"] is True
        assert "message" not in tr
        assert tr.get("status") is None

    def test_populated_branch_hides_empty_note_in_ui(self, ui_html):  # (5)
        assert "if (empty) empty.style.display = 'none';" in ui_html
        assert "empty.textContent = (tr && tr.message)" in ui_html

    def test_same_market_date_across_surfaces(self, tmp_path):  # (6)
        out = _load(tmp_path)
        assert out["todays_review"]["market_date"] \
            == out["attribution"]["market_date"] == "2026-07-22"
        assert out["todays_review"]["prior_market_date"] \
            == out["attribution"]["prior_market_date"] == "2026-07-21"

    def test_position_contributors_match_attribution(self, tmp_path):  # (7)
        out = _load(tmp_path)
        tr, at = out["todays_review"], out["attribution"]
        assert tr["top_positive"] == at["winners"][0]
        assert tr["top_negative"] == at["losers"][0]
        assert tr["top_positive"]["ticker"] == "BBB"
        assert tr["top_positive"]["pnl_contribution"] == pytest.approx(7.5)
        assert tr["top_negative"]["ticker"] == "AAA"
        assert tr["top_negative"]["pnl_contribution"] == pytest.approx(-10.0)

    def test_sector_contributors_match_attribution(self, tmp_path):  # (8)
        out = _load(tmp_path)
        tr, sectors = out["todays_review"], out["attribution"]["sectors"]
        assert tr["strongest_sector"]["sector"] == sectors[0]["sector"] == "Energy"
        assert tr["strongest_sector"]["pnl_contribution"] \
            == sectors[0]["pnl_contribution"] == pytest.approx(7.5)
        assert tr["weakest_sector"]["sector"] == sectors[-1]["sector"] == "Tech"
        assert tr["weakest_sector"]["pnl_contribution"] \
            == sectors[-1]["pnl_contribution"] == pytest.approx(-10.0)

    def test_phase28a_attribution_behavior_intact(self, tmp_path):  # (20)
        out = _load(tmp_path)
        at = out["attribution"]
        assert at["status"] == fe.ATTRIB_READY and at["available"] is True
        rc = at["reconciliation"]
        assert rc["reconciles"] is True
        assert rc["position_contribution_sum"] == pytest.approx(-2.5, abs=1e-6)
        assert rc["market_movement"] == pytest.approx(-2.5, abs=1e-6)
        assert out["why_pnl_moved"]["available"] is True


# =========================================================================== #
# THE _safe_ops ROOT CAUSE — regression pins (Part C)
# =========================================================================== #
class TestSafeOpsInvocation:
    def test_default_loader_called_with_keyword_today(self, monkeypatch):
        seen = {}

        def kw_only_loader(*, desk_dir=None, ledger_dir=None,
                           today=None, panel_path=None, inputs_dir=None):
            seen["today"] = today
            return _ops(holdings=_holds_2())

        monkeypatch.setattr(fe, "_OPS_LOADER", kw_only_loader)
        out = fe._safe_ops(None, "2026-07-23")
        assert seen == {"today": "2026-07-23"}
        assert len(fe._holdings(out)) == 2      # never silently {}

    def test_real_loader_signature_accepts_keyword_today(self):
        # Guards against the exact live failure recurring: the way _safe_ops
        # invokes the default loader must bind against the REAL signature.
        inspect.signature(ob.load_operational_book).bind(today="2026-07-23")
        with pytest.raises(TypeError):
            inspect.signature(ob.load_operational_book).bind("2026-07-23")

    def test_injected_loader_contract_unchanged(self):
        out = fe._safe_ops(lambda today: {"marker": today}, "X")
        assert out == {"marker": "X"}

    def test_loader_failure_still_degrades_to_empty(self, monkeypatch):
        def _boom(**_kw):
            raise RuntimeError("store offline")
        monkeypatch.setattr(fe, "_OPS_LOADER", _boom)
        assert fe._safe_ops(None, None) == {}


# =========================================================================== #
# PART D — daily vs cumulative excess (scenarios 9, 10)
# =========================================================================== #
class TestExcessLabels:
    def test_daily_and_cumulative_separately_labeled(self, ui_html):  # (9)
        assert ui_html.count("Daily Excess vs SPY") >= 2    # fe-today + dc-attr
        assert "Cumulative Excess vs SPY" in ui_html        # dc-perf KPI
        assert "Cum Excess vs SPY" in ui_html               # command center KPI
        assert "Cumulative excess vs SPY " in ui_html       # fe-k-cumx sub text

    def test_no_unlabeled_generic_excess_remains(self, ui_html):  # (9)
        assert ">Excess vs SPY<" not in ui_html
        assert "'Excess vs SPY'" not in ui_html

    def test_ui_binds_each_label_to_its_own_field(self, ui_html):  # (10)
        assert "_fePp(tr.daily_excess_return_pct)" in ui_html
        assert "tr.cumulative_excess_return_pct" in ui_html
        # Percentage-POINT formatter exists and is null-safe.
        assert "function _fePp(x)" in ui_html
        assert "+ 'pp'" in ui_html.replace('"', "'")

    def test_daily_and_cumulative_values_not_swapped(self, tmp_path):  # (10)
        # Hand-computed 28A world at 2026-07-22:
        #   daily:  port 612.5/615-1 = -0.4065%; SPY (1.005/1.01)-1 = -0.4950%
        #           daily excess = +0.0885pp
        #   cumulative: port 2.0833% - SPY 0.5% = +1.5833pp
        tr = _load(tmp_path)["todays_review"]
        assert tr["daily_excess_return_pct"] == pytest.approx(0.0885, abs=1e-3)
        assert tr["cumulative_excess_return_pct"] == pytest.approx(1.5833, abs=1e-3)
        assert tr["daily_excess_return_pct"] < 1.0 < tr["cumulative_excess_return_pct"]

    def test_gate_wording_intact_and_old_wording_absent(self, ui_html):  # (22, 23)
        h = dag._PRESENTATION[dag.OUTCOME_NO_ACTION_TODAY]["headline"]
        assert h.startswith("NO PORTFOLIO CHANGE REQUIRED FROM THE LATEST "
                            "COMPLETED CLOSE")
        assert "NO PORTFOLIO CHANGE REQUIRED TODAY" \
            not in _DAG_SRC.read_text(encoding="utf-8")
        assert "Target composition date" in ui_html
        assert "Latest price/score refresh" in ui_html
        assert "Model target calculated as of" not in ui_html


# =========================================================================== #
# PART E — nested safety contract (scenarios 14, 15, 16)
# =========================================================================== #
_NESTED_KEYS = ("read_only", "paper_only", "diagnostic_only", "creates_orders",
                "broker_execution", "automation_enabled",
                "changes_operational_model", "changes_operational_holdings",
                "changes_model_weights", "promotes_challenger", "retrains_model")


class TestNestedSafetyContract:
    def test_nested_safety_object_exists(self, tmp_path):  # (14)
        payload = fps.load_prediction_skill(desk_dir=tmp_path)
        assert isinstance(payload.get("safety"), dict)
        assert set(payload["safety"].keys()) == set(_NESTED_KEYS)

    def test_nested_values_match_top_level(self, tmp_path):  # (15)
        payload = fps.load_prediction_skill(desk_dir=tmp_path)
        for k in _NESTED_KEYS:
            assert payload["safety"][k] == payload[k], k

    def test_top_level_compatibility_intact(self, tmp_path):  # (16)
        payload = fps.load_prediction_skill(desk_dir=tmp_path)
        assert payload["read_only"] is True
        assert payload["performed_write"] is False
        assert payload["paper_only"] is True
        assert payload["diagnostic_only"] is True
        assert payload["creates_orders"] is False
        assert payload["broker_execution"] is False
        assert payload["broker_enabled"] is False
        assert payload["automation_enabled"] is False
        assert payload["changes_operational_model"] is False
        assert payload["changes_operational_holdings"] is False
        assert payload["changes_model_weights"] is False
        assert payload["promotes_challenger"] is False
        assert payload["retires_model"] is False
        assert payload["retrains_model"] is False
        assert payload["prediction_service_used"] is False
        assert payload["uses_future_information"] is False
        assert "PAPER ONLY" in payload["safety_badges"]

    def test_route_exposes_nested_safety(self, client):  # (14, 15 via API)
        r = client.get("/v1/evidence/prediction-skill", headers=_AUTH)
        assert r.status_code == 200
        d = r.json()
        assert isinstance(d.get("safety"), dict)
        for k in _NESTED_KEYS:
            assert d["safety"][k] == d[k], k

    def test_no_secret_material_in_payload(self, tmp_path):
        import json
        dump = json.dumps(fps.load_prediction_skill(desk_dir=tmp_path)).lower()
        for banned in ("api_key", "api-key", "authorization", "password",
                       "local-dev-key", "traceback"):
            assert banned not in dump


# =========================================================================== #
# PART F — the correct empty state is preserved (scenarios 11, 12, 13)
# =========================================================================== #
class TestEmptyStatePreserved:
    def test_no_forward_snapshots_is_a_valid_state(self, tmp_path):  # (11)
        payload = fps.load_prediction_skill(desk_dir=tmp_path)
        assert payload["status"] == fps.EV_NO_SNAPSHOTS
        assert payload["snapshot_count"] == 0
        assert payload["matured_outcome_count"] == 0
        assert payload["pending_outcome_count"] == 0
        s = fps.prediction_skill_summary(desk_dir=tmp_path)
        assert s["evidence_state"] == fps.EV_NO_SNAPSHOTS
        assert s["latest_snapshot_date"] is None

    def test_empty_state_explains_next_close_in_ui(self, ui_html):  # (11)
        assert "No forward snapshots yet" in ui_html
        assert "never backfilled" in ui_html

    def test_read_paths_never_create_snapshots(self, tmp_path):  # (12)
        assert _fps_files(tmp_path) == set()
        fps.load_prediction_skill(desk_dir=tmp_path)
        fps.load_prediction_snapshots(desk_dir=tmp_path)
        fps.prediction_skill_summary(desk_dir=tmp_path)
        _load(tmp_path)
        assert _fps_files(tmp_path) == set()

    def test_get_endpoints_read_only(self, client):  # (13)
        for route in ("/v1/evidence/prediction-skill",
                      "/v1/evidence/prediction-skill/snapshots",
                      "/v1/evidence/forward"):
            r = client.get(route, headers=_AUTH)
            assert r.status_code == 200, route
            d = r.json()
            assert d.get("read_only") is True, route
            assert d.get("performed_write") in (False, None), route

    def test_phase28b_skill_behavior_intact(self, tmp_path):  # (21)
        payload = fps.load_prediction_skill(desk_dir=tmp_path)
        assert payload["horizons"] == [1, 5, 20, 63]
        assert payload["active_book"]["model_id"] == fps.ACTIVE_MODEL_ID
        assert payload["active_book"]["book_id"] == fps.ACTIVE_BOOK_ID
        gates = payload["evidence_gates"]
        assert "NEVER promote" in gates["note"]
        v = fps.verify_prediction_ledgers(desk_dir=tmp_path)
        assert v["all_intact"] is True

    def test_forward_evidence_embeds_skill_summary(self, tmp_path):
        out = _load(tmp_path)
        ps = out["prediction_skill"]
        assert ps is not None
        assert ps["evidence_state"] == fps.EV_NO_SNAPSHOTS
        assert ps["detail_route"] == "/v1/evidence/prediction-skill"


# =========================================================================== #
# UI STATIC — dialogs, buttons, wiring (scenarios 17, 18)
# =========================================================================== #
class TestUiStatic:
    def test_no_native_dialogs(self, ui_html):  # (17)
        for pat in ("alert(", "confirm(", "prompt("):
            assert pat not in ui_html, pat

    def test_no_blank_action_buttons(self, ui_html):  # (18)
        # Any literally-empty button must carry an id (a renderer fills it);
        # anonymous empty buttons would render blank.
        blanks = re.findall(r"<button(?![^>]*\bid=)[^>]*>\s*</button>", ui_html)
        assert blanks == []

    def test_todays_review_ui_shows_sector_line(self, ui_html):
        assert 'id="fe-today-sectors"' in ui_html
        assert "Strongest sector:" in ui_html
        assert "Weakest sector:" in ui_html

    def test_todays_review_ui_keeps_decision_badge(self, ui_html):
        assert 'id="fe-today-decision"' in ui_html
        assert "tr.decision" in ui_html


# =========================================================================== #
# NO OPERATIONAL MUTATION (scenario 24)
# =========================================================================== #
class TestNoOperationalMutation:
    def test_aggregator_never_mutates_ops_payload(self, tmp_path):  # (24)
        ops = _ops(holdings=_holds_2())
        before = copy.deepcopy(ops)
        _load(tmp_path, ops=ops)
        assert ops == before

    def test_reads_leave_desk_store_untouched(self, tmp_path):  # (24)
        sdir = desk._desk_dir(str(tmp_path))
        before = sorted(p.name for p in sdir.iterdir()) if sdir.exists() else []
        fps.load_prediction_skill(desk_dir=tmp_path)
        _load(tmp_path)
        after = sorted(p.name for p in sdir.iterdir()) if sdir.exists() else []
        assert after == before

    def test_safety_flags_deny_every_mutation(self, tmp_path):  # (24)
        payload = fps.load_prediction_skill(desk_dir=tmp_path)
        for k in ("creates_orders", "broker_execution", "automation_enabled",
                  "changes_operational_model", "changes_operational_holdings",
                  "changes_model_weights", "promotes_challenger",
                  "retrains_model"):
            assert payload[k] is False, k
            assert payload["safety"].get(k, payload[k]) is False, k
