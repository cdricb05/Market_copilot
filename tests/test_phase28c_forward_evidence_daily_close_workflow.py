"""
tests/test_phase28c_forward_evidence_daily_close_workflow.py

Phase 28C — COORDINATED DAILY CLOSE + TRUE_FORWARD EVIDENCE WORKFLOW.

Fully offline (reuses the deterministic Phase 28B 8-ticker world + the Phase 27A
client harness). Proves the coordinated-close contract:

  * ROOT CAUSE (month boundary): the operational close succeeds while the frozen
    monthly momentum input is a whole month behind the closed session, so a
    point-in-time-honest TRUE_FORWARD snapshot cannot be built — the gap is AMBER
    and documented, never the operational-red "capture failed".
  * SEVERITY SPLIT (Workstream E): operational close and forward evidence are
    reported SEPARATELY; an evidence gap on a successful close is amber.
  * READINESS CONTRACT (Workstream D): the structured evidence-readiness model.
  * API CONTRACT (Workstream I): operational_close vs forward_evidence,
    overall_status, safe_to_rerun_close (False after a processed close),
    safe_to_retry_evidence.
  * TRUE_FORWARD SAFETY (Workstream C): never fabricated / backdated; the
    month-boundary gap must remain until the research-side monthly input exists.
  * RECOVERY (Workstream F): token-gated, evidence-only, gated on classification;
    an unrecoverable gap is preserved, never faked.
  * IDEMPOTENCY / SAFETY: no duplicate closes/snapshots; no orders / signals /
    trade-decisions / automation; a read-only readiness GET writes nothing.
  * UI (Workstreams G/H): amber-not-red banner rendered from the split block,
    recovery button gated on classification, panel refresh incl. Daily Alpha Run,
    no native dialogs, never suggests re-running a completed close.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from paper_trader.api import daily_close as dc
from paper_trader.api import forward_prediction_skill as fps
from paper_trader.api import paper_trading_desk as desk

from tests.test_phase27a_paper_operations import (  # reuse the offline harness
    _AUTH, client, env,  # noqa: F401
)
from tests.test_phase28b_forward_prediction_skill import (  # deterministic world
    _D1, _capture_seam, _cur, _dl, _hold_gate, _ledger_rows,
    _ok_refresh, _ops, _run_close, _table,
)

_UI = Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html"

# A NEW-MONTH close whose frozen momentum input is still the prior month — the
# exact 2026-08-04 production shape (here compressed to March -> April).
_MB_CLOSED = "2026-04-01"   # Wednesday in the new month (the completed session)
_MB_TODAY = "2026-04-02"    # Thursday -> expected completed session = 2026-04-01
_MB_MODEL = "2026-03-31"    # the frozen monthly momentum input is still March


@pytest.fixture(scope="module")
def ui_html():
    return _UI.read_text(encoding="utf-8")


def _month_boundary_seam():
    """A capture seam whose model inputs are a whole month behind the closed
    session — the frozen mom_6_1 contract cannot advance without a research-side
    monthly rebuild, so no snapshot is (or should be) fabricated."""
    def _fn(*, market_date, desk_dir=None, current=None, downloader=None, ops=None):
        return fps.capture_for_daily_close(
            market_date=market_date, desk_dir=desk_dir,
            current=_cur(_MB_MODEL), downloader=_dl(_table()), ops=ops or _ops())
    return _fn


def _run_mb_close(tmp, *, seed_prior=True):
    """A prior in-month close (establishes the baseline / prior processed date),
    then a NEW-MONTH close whose forward evidence is blocked at the month
    boundary — a HOLD close with an amber evidence gap (the production shape)."""
    if seed_prior:
        _run_close(tmp, capture_fn=_capture_seam())   # healthy March close (baseline)
    return dc.run_daily_close(
        confirm=dc.EXECUTE_CONFIRMATION, today=_MB_TODAY, desk_dir=tmp,
        operational_loader=(lambda _t: _ops()), gate_loader=_hold_gate,
        refresh_fn=_ok_refresh(_MB_CLOSED), prediction_capture_fn=_month_boundary_seam())


# =========================================================================== #
# ROOT CAUSE + SEVERITY SPLIT (Workstreams A/E) — the production failure shape.
# =========================================================================== #
class TestMonthBoundaryRootCause:
    def test_operational_close_succeeds_at_month_boundary(self, tmp_path):
        out = _run_mb_close(tmp_path)
        # The operational close is a valid, durable HOLD at the new-month session.
        assert out["close_status"] == dc.CLOSE_COMPLETE_HOLD
        assert out["operational_close"]["status"] == dc.CLOSE_COMPLETE_HOLD
        assert out["operational_close"]["market_date"] == _MB_CLOSED
        assert out["operational_close"]["severity"] != dc.SEV_RED
        assert out["decision_recorded"] is True

    def test_forward_evidence_is_amber_not_operational_red(self, tmp_path):
        out = _run_mb_close(tmp_path)
        fe = out["forward_evidence"]
        assert fe["severity"] == dc.EV_SEV_AMBER
        assert fe["severity"] != "red"
        assert fe["recovery_classification"] == dc.EVIDENCE_GAP_MUST_REMAIN
        assert fe["gap_kind"] == "RESEARCH_MONTHLY_INPUT_REQUIRED"
        assert fe["missing"] == 6
        assert fe["active_book_present"] is False

    def test_no_scary_capture_failed_wording(self, tmp_path):
        out = _run_mb_close(tmp_path)
        assert "FORWARD EVIDENCE CAPTURE FAILED" not in out["explanation"]
        assert "CAPTURE FAILED" not in out["operator_message"]
        assert "evidence gap was documented" in out["operator_message"]

    def test_overall_status_is_evidence_gap_not_close_failed(self, tmp_path):
        out = _run_mb_close(tmp_path)
        assert out["overall_status"] == dc.OVERALL_EVIDENCE_GAP
        assert out["overall_status"] != dc.OVERALL_CLOSE_FAILED

    def test_capture_reports_research_monthly_input_required(self, tmp_path):
        out = _run_mb_close(tmp_path)
        fpc = out["forward_prediction_capture"]
        assert fpc["capture_block_reason"] == "MODEL_MONTH_BEHIND"
        assert fpc["model_month_behind"] is True
        assert fpc["research_monthly_input_required"] is True
        assert fpc["model_calc_date"] == _MB_MODEL
        assert fpc["closed_market_date"] == _MB_CLOSED


# =========================================================================== #
# NAV / P&L / DECISION UNAFFECTED BY THE EVIDENCE GAP (Workstream E)
# =========================================================================== #
class TestEvidenceGapDoesNotAffectOperational:
    def test_decision_journal_row_persisted(self, tmp_path):
        _run_mb_close(tmp_path)
        journal = [r for r in desk._read_ledger(desk._desk_dir(tmp_path),
                                                 dc.DAILY_CLOSE_JOURNAL_FILE)
                   if r.get("event") == dc.DAILY_CLOSE_EVENT]
        assert any(r["market_date"] == _MB_CLOSED
                   and r["decision"] == dc.DECISION_HOLD for r in journal)

    def test_safe_to_rerun_close_false_after_processed_close(self, tmp_path):
        out = _run_mb_close(tmp_path)
        assert out["safe_to_rerun_close"] is False

    def test_evidence_gap_creates_no_orders(self, tmp_path):
        out = _run_mb_close(tmp_path)
        assert out["creates_orders"] is False
        assert out["proposed_change_count"] == 0


# =========================================================================== #
# HEALTHY CLOSE — EVIDENCE COMPLETE (Workstream D success path)
# =========================================================================== #
class TestHealthyCloseEvidenceComplete:
    def test_six_snapshots_and_green_evidence(self, tmp_path):
        out = _run_close(tmp_path, capture_fn=_capture_seam())
        fe = out["forward_evidence"]
        assert fe["severity"] == dc.EV_SEV_OK
        assert fe["captured"] == 6
        assert fe["missing"] == 0
        assert fe["active_book_present"] is True
        assert out["overall_status"] == dc.OVERALL_EVIDENCE_COMPLETE
        assert "Forward research evidence captured" in fe["headline"]

    def test_mandatory_active_book_present(self, tmp_path):
        out = _run_close(tmp_path, capture_fn=_capture_seam())
        assert out["forward_evidence"]["active_book_present"] is True
        assert out["evidence_readiness"]["active_book_snapshot_present"] is True

    def test_evidence_message_stays_quiet_on_success(self, tmp_path):
        out = _run_close(tmp_path, capture_fn=_capture_seam())
        assert "gap was documented" not in out["explanation"]
        assert "PARTIAL" not in out["explanation"]


# =========================================================================== #
# READINESS CONTRACT (Workstream D) — the structured model + read-only loader.
# =========================================================================== #
_READINESS_KEYS = (
    "eligible_market_date", "operational_close_status", "research_mark_status",
    "research_mark_date", "research_mark_freshness", "required_snapshot_count",
    "captured_snapshot_count", "missing_snapshot_count",
    "active_book_snapshot_present", "missing_book_ids", "capture_recoverable",
    "recovery_classification", "weakest_gate", "operator_action", "safe_to_close",
    "safe_to_capture_true_forward", "state",
)


class TestReadinessContract:
    def test_all_readiness_fields_present(self, tmp_path):
        out = _run_mb_close(tmp_path)
        rd = out["evidence_readiness"]
        for k in _READINESS_KEYS:
            assert k in rd, k

    def test_month_boundary_marks_stale_new_month(self, tmp_path):
        out = _run_mb_close(tmp_path)
        rd = out["evidence_readiness"]
        assert rd["research_mark_freshness"] == "STALE_NEW_MONTH"
        assert rd["safe_to_capture_true_forward"] is False
        assert rd["weakest_gate"] == "RESEARCH_MONTHLY_INPUT"
        assert rd["missing_snapshot_count"] == 6
        assert rd["operator_action"] is not None

    def test_readiness_loader_is_read_only(self, tmp_path):
        _run_mb_close(tmp_path)
        sdir = desk._desk_dir(tmp_path)
        before = sorted(p.name for p in sdir.iterdir())
        rd = dc.load_forward_evidence_readiness(
            today=_MB_TODAY, desk_dir=tmp_path, operational=_ops(), gate=_hold_gate())
        assert rd["performed_write"] is False
        assert rd["read_only"] is True
        assert "evidence_readiness" in rd and "forward_evidence" in rd
        assert "operational_close" in rd and "overall_status" in rd
        assert sorted(p.name for p in sdir.iterdir()) == before


# =========================================================================== #
# TRUE_FORWARD SAFETY (Workstream C) — never fabricated / backdated / relabelled.
# =========================================================================== #
class TestTrueForwardSafety:
    def test_month_boundary_writes_no_snapshot(self, tmp_path):
        _run_mb_close(tmp_path)
        # Only the seeded March snapshots exist; NO April snapshot was fabricated.
        rows = fps._book_snapshots(_ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE))
        assert all(r["market_date"] == _D1 for r in rows)
        assert not any(r["market_date"] == _MB_CLOSED for r in rows)

    def test_month_boundary_not_recoverable_from_frozen_artifacts(self, tmp_path):
        _run_mb_close(tmp_path)
        rs = fps.load_recovery_status(market_date=_MB_CLOSED, desk_dir=tmp_path)
        assert rs["close_processed"] is True
        assert rs["frozen_artifacts_found"] is False
        assert rs["recovery_status"] == fps.REC_NOT_RECOVERABLE
        assert rs["requires_recalculation"] is True

    def test_retroactive_snapshot_is_refused(self, tmp_path):
        # A date at/before the latest captured snapshot would be backfilled, not
        # forward — capture refuses it (no hindsight TRUE_FORWARD).
        _run_close(tmp_path, capture_fn=_capture_seam())    # snapshots through _D1
        out = fps.capture_snapshots(market_date="2026-03-02", desk_dir=tmp_path,
                                    current=_cur("2026-03-02"), ops=_ops(),
                                    downloader=_dl(_table()))
        assert out["status"] == "SNAPSHOTS_UNAVAILABLE"
        assert all("NO_RETROACTIVE_TRUE_FORWARD" in r
                   for r in out["unavailable_reasons"].values())

    def test_unsafe_recovery_preserves_documented_gap(self, tmp_path):
        _run_mb_close(tmp_path)
        out = fps.recover_missed_close(market_date=_MB_CLOSED,
                                       confirmation=fps.RECOVERY_CONFIRM_TOKEN,
                                       desk_dir=tmp_path)
        assert out["status"] == "RECOVERY_REJECTED_NOT_RECOVERABLE"
        assert out["recovered_books"] == []
        # A documented FORWARD_CAPTURE_MISSED incident is recorded — no fabrication.
        incidents = [r for r in fps.list_evidence_incidents(tmp_path)
                     if r.get("kind") == fps.KIND_CAPTURE_MISSED
                     and r.get("market_date") == _MB_CLOSED]
        assert incidents and incidents[0]["snapshot_fabricated"] is False


# =========================================================================== #
# _evidence_view classification matrix (Workstream C/D/E) — pure-function unit.
# =========================================================================== #
def _fpc(**kw):
    base = {"snapshots_expected": 6, "snapshots_created": 0,
            "snapshots_already_present": 0, "mandatory_active_snapshot_persisted": False,
            "market_date": "2026-08-04", "model_calc_date": "2026-08-04"}
    base.update(kw)
    return base


class TestEvidenceViewMatrix:
    def test_complete(self):
        ev = dc._evidence_view(
            close_status=dc.CLOSE_COMPLETE_HOLD,
            fpc=_fpc(snapshots_created=6, mandatory_active_snapshot_persisted=True),
            evidence_status=fps.EVIDENCE_COMPLETE,
            model_recalc={"model_calc_date": "2026-08-04"},
            evidence_date="2026-08-04", desk_dir=None)
        assert ev["forward_evidence"]["severity"] == dc.EV_SEV_OK
        assert ev["overall_status"] == dc.OVERALL_EVIDENCE_COMPLETE

    def test_month_boundary(self):
        ev = dc._evidence_view(
            close_status=dc.CLOSE_COMPLETE_HOLD,
            fpc=_fpc(model_month_behind=True, model_calc_date="2026-07-31",
                     unavailable_reasons={b[1]: "x" for b in fps.SUPPORTED_BOOKS}),
            evidence_status=fps.EVIDENCE_BLOCKED,
            model_recalc={"model_calc_date": "2026-07-31"},
            evidence_date="2026-08-04", desk_dir=None)
        assert ev["forward_evidence"]["severity"] == dc.EV_SEV_AMBER
        assert ev["forward_evidence"]["recovery_classification"] == dc.EVIDENCE_GAP_MUST_REMAIN
        assert ev["forward_evidence"]["gap_kind"] == "RESEARCH_MONTHLY_INPUT_REQUIRED"
        assert ev["safe_to_rerun_close"] is False

    def test_not_processed_is_info_and_rerunnable(self):
        ev = dc._evidence_view(
            close_status=dc.CLOSE_DUE, fpc=None, evidence_status=None,
            model_recalc=None, evidence_date="2026-08-04", desk_dir=None)
        assert ev["forward_evidence"]["severity"] == dc.EV_SEV_INFO
        assert ev["safe_to_rerun_close"] is True
        assert ev["overall_status"] == dc.OVERALL_PREPARATION_REQUIRED

    def test_partial_shadow_keeps_partial_label(self):
        ev = dc._evidence_view(
            close_status=dc.CLOSE_COMPLETE_HOLD,
            fpc=_fpc(snapshots_created=5, mandatory_active_snapshot_persisted=True,
                     unavailable_reasons={"mom_6_1_top50": "BOOK_UNAVAILABLE"}),
            evidence_status=fps.EVIDENCE_PARTIAL,
            model_recalc={"model_calc_date": "2026-08-04"},
            evidence_date="2026-08-04", desk_dir=None)
        assert ev["forward_evidence"]["severity"] == dc.EV_SEV_AMBER
        assert "PARTIAL" in ev["forward_evidence"]["headline"]
        assert ev["forward_evidence"]["missing"] == 1


# =========================================================================== #
# RECOVERY (Workstream F) — token-gated, gated on classification.
# =========================================================================== #
def _fail_snapshot_append(mp):
    real = desk._append_ledger

    def wrapper(sdir, fname, rows):
        if fname == fps.SNAPSHOT_LEDGER_FILE:
            raise OSError("simulated append failure")
        return real(sdir, fname, rows)
    mp.setattr(desk, "_append_ledger", wrapper)


def _recoverable_close(tmp):
    mp = pytest.MonkeyPatch()
    try:
        _fail_snapshot_append(mp)
        return _run_close(tmp, capture_fn=_capture_seam())
    finally:
        mp.undo()


class TestRecoveryGating:
    def test_recoverable_world_flags_recovery_available(self, tmp_path):
        out = _recoverable_close(tmp_path)
        fe = out["forward_evidence"]
        assert fe["severity"] == dc.EV_SEV_AMBER
        assert fe["recovery_available"] is True
        assert fe["recovery_classification"] == dc.EVIDENCE_RECOVERY_AVAILABLE
        assert out["safe_to_retry_evidence"] is True
        assert out["overall_status"] == dc.OVERALL_EVIDENCE_RECOVERABLE

    def test_month_boundary_does_not_offer_recovery(self, tmp_path):
        out = _run_mb_close(tmp_path)
        assert out["forward_evidence"]["recovery_available"] is False
        assert out["safe_to_retry_evidence"] is False

    def test_safe_recovery_restores_only_missing_snapshots(self, tmp_path):
        _recoverable_close(tmp_path)
        out = fps.recover_missed_close(market_date=_D1,
                                       confirmation=fps.RECOVERY_CONFIRM_TOKEN,
                                       desk_dir=tmp_path)
        assert out["status"] == "RECOVERED_FROM_FROZEN_ARTIFACTS"
        assert len(out["recovered_books"]) == 6
        assert out["changes_operational_state"] is False

    def test_recovery_is_token_gated(self, tmp_path):
        _recoverable_close(tmp_path)
        out = fps.recover_missed_close(market_date=_D1, confirmation="WRONG",
                                       desk_dir=tmp_path)
        assert out["status"] == "RECOVERY_CONFIRM_REQUIRED"
        assert out["performed_write"] is False


# =========================================================================== #
# IDEMPOTENCY (Workstream B)
# =========================================================================== #
class TestIdempotency:
    def test_repeated_close_is_idempotent(self, tmp_path):
        _run_mb_close(tmp_path)
        n = len([r for r in desk._read_ledger(desk._desk_dir(tmp_path),
                                              dc.DAILY_CLOSE_JOURNAL_FILE)
                 if r.get("event") == dc.DAILY_CLOSE_EVENT])
        out2 = dc.run_daily_close(
            confirm=dc.EXECUTE_CONFIRMATION, today=_MB_TODAY, desk_dir=tmp_path,
            operational_loader=(lambda _t: _ops()), gate_loader=_hold_gate,
            refresh_fn=_ok_refresh(_MB_CLOSED),
            prediction_capture_fn=_month_boundary_seam())
        assert out2["close_status"] == dc.ALREADY_PROCESSED
        assert out2["safe_to_rerun_close"] is False
        n2 = len([r for r in desk._read_ledger(desk._desk_dir(tmp_path),
                                               dc.DAILY_CLOSE_JOURNAL_FILE)
                  if r.get("event") == dc.DAILY_CLOSE_EVENT])
        assert n2 == n

    def test_no_duplicate_snapshots_on_rerun(self, tmp_path):
        _run_close(tmp_path, capture_fn=_capture_seam())
        snaps = len(_ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE))
        _run_close(tmp_path, capture_fn=_capture_seam())
        assert len(_ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE)) == snaps


# =========================================================================== #
# API CONTRACT (Workstream I) + SAFETY
# =========================================================================== #
class TestApiContract:
    def test_readiness_endpoint_read_only_and_shaped(self, client, env):
        r = client.get("/v1/operations/daily-close/forward-evidence-readiness",
                       headers=_AUTH)
        assert r.status_code == 200
        d = r.json()
        for k in ("operational_close", "forward_evidence", "evidence_readiness",
                  "overall_status", "safe_to_rerun_close", "safe_to_retry_evidence"):
            assert k in d, k
        assert d["performed_write"] is False
        assert d["creates_orders"] is False

    def test_readiness_endpoint_auth_enforced(self, client, env):
        r = client.get("/v1/operations/daily-close/forward-evidence-readiness")
        assert r.status_code in (401, 403)

    def test_readiness_endpoint_writes_nothing(self, client, env):
        client.get("/v1/operations/daily-close/forward-evidence-readiness",
                   headers=_AUTH)
        assert not (env["desk"] / dc.DAILY_CLOSE_JOURNAL_FILE).exists()
        assert not (env["desk"] / fps.SNAPSHOT_LEDGER_FILE).exists()

    def test_daily_close_get_carries_split_contract(self, client, env):
        d = client.get("/v1/operations/daily-close", headers=_AUTH).json()
        assert "operational_close" in d and "forward_evidence" in d
        assert "overall_status" in d and "safe_to_rerun_close" in d

    def test_no_automation_or_scheduled_tasks(self, tmp_path):
        out = _run_mb_close(tmp_path)
        assert out["automation_enabled"] is False
        assert out["scheduled_tasks"] is False
        assert out["broker_enabled"] is False
        assert out["live_orders_enabled"] is False


# =========================================================================== #
# UI (Workstreams G/H) — static wiring checks.
# =========================================================================== #
class TestUiStatic:
    def test_banner_rendered_from_split_block(self, ui_html):
        assert "d.forward_evidence" in ui_html
        assert 'id="dc-evidence-banner"' in ui_html
        assert "fe.severity !== 'green'" in ui_html

    def test_no_operational_red_capture_failed_wording(self, ui_html):
        assert "FORWARD EVIDENCE CAPTURE FAILED" not in ui_html

    def test_recovery_button_gated_on_classification(self, ui_html):
        assert "recoverForwardEvidence" in ui_html
        assert "fe.recovery_available" in ui_html
        assert "CONFIRM_RECOVER_FROZEN_FORWARD_EVIDENCE" in ui_html

    def test_daily_alpha_run_refreshed_after_close(self, ui_html):
        assert "loadDailyAlphaStatus" in ui_html
        assert "loadOperationalBook()" in ui_html

    def test_no_native_dialogs(self, ui_html):
        for pat in ("alert(", "confirm(", "prompt("):
            assert pat not in ui_html, pat

    def test_duplicate_submission_guard(self, ui_html):
        assert "if (_dcRunInFlight) return;" in ui_html

    def test_amber_toast_never_red_on_evidence_gap(self, ui_html):
        # The evidence-gap toast is a warning, never an error, and never a
        # suggestion to re-run a completed close.
        assert "sev === 'amber'" in ui_html
        assert "NAV, P&L and the recorded decision are valid" in ui_html
