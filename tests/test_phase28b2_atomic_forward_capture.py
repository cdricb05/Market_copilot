"""
tests/test_phase28b2_atomic_forward_capture.py

Phase 28B.2 — ATOMIC TRUE-FORWARD SNAPSHOT CAPTURE, MISSED-CLOSE RECOVERY AND
CLOSE PROGRESS.

Fully offline (reuses the deterministic Phase 28B 8-ticker world + the Phase 27A
client harness). Proves the Part J matrix:

  * FRESH CAPTURE (1-10): mandatory active Top-25 + shadow capture, no
    read_only_presence_check on the write path, immediate storage verification,
    same-date/point-in-time discipline, no GET/page-load capture.
  * ATOMIC ORDERING: snapshots are durable BEFORE the slow maturity-price fetch
    (the July-24 incident class), frozen artifact bundle written first.
  * FAILURE SEMANTICS (11-16): zero-of-six is never ordinary success, mandatory
    vs shadow failures are explicit, operational close and evidence state are
    separate, exceptions surface, per-book reasons.
  * IDEMPOTENCY (17-20): no duplicate marks/snapshots, no hindsight backfill,
    maturation idempotent.
  * RECOVERY (21-32): read-only dry run, token gate, absent/tampered/incomplete
    bundle rejections, unprocessed/already-present rejections, verbatim replay,
    no market-data or model access, no operational change, repeat idempotency.
  * MISSED-CAPTURE RECORD (33-35): never a snapshot, never an outcome, close
    stays valid.
  * SAFETY (36-43): holdings/cash/orders/broker/automation/weights/champion
    unchanged; no credential exposure.
  * UI (44-49): duplicate-click guard, elapsed-time indicator, split outcomes,
    prominent zero-of-six failure, no native dialogs, null-safe wiring.
"""
from __future__ import annotations

import copy
import json
import re
from pathlib import Path

import pytest

from paper_trader.api import daily_close as dc
from paper_trader.api import forward_prediction_skill as fps
from paper_trader.api import multi_horizon_engine as eng
from paper_trader.api import paper_trading_desk as desk

from tests.test_phase27a_paper_operations import (  # reuse the offline harness
    _AUTH, client, env,  # noqa: F401
)
from tests.test_phase28b_forward_prediction_skill import (  # deterministic world
    _D1, _D2, _SESS, _cap, _capture_seam, _cur, _dl, _extend, _hold_gate,
    _ledger_rows, _ok_refresh, _ops, _run_close, _table,
)

_UI = Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html"

_ALL_BOOK_KEYS = ("fundamental_momentum_50_50_top25", "fundamental_momentum_50_50_top50",
                  "mom_6_1_top25", "mom_6_1_top50",
                  "composite_sn_top25", "composite_sn_top50")


@pytest.fixture(scope="module")
def ui_html():
    return _UI.read_text(encoding="utf-8")


def _fail_snapshot_append(monkeypatch):
    """Make ONLY the snapshot-ledger append fail (journal / bundle appends keep
    working) — the recoverable-close world."""
    real = desk._append_ledger

    def wrapper(sdir, fname, rows):
        if fname == fps.SNAPSHOT_LEDGER_FILE:
            raise OSError("simulated append failure")
        return real(sdir, fname, rows)

    monkeypatch.setattr(desk, "_append_ledger", wrapper)


def _blocked_close(tmp, _monkeypatch=None):
    """A processed operational close whose snapshot persistence failed but whose
    frozen artifact bundle survived — the canonical recoverable world. Uses a
    PRIVATE MonkeyPatch so undoing it never clobbers the test's own patches or
    the shared env fixture."""
    mp = pytest.MonkeyPatch()
    try:
        _fail_snapshot_append(mp)
        return _run_close(tmp, capture_fn=_capture_seam())
    finally:
        mp.undo()


def _no_books_seam():
    """A capture seam whose frozen-model build produced NO books (zero-of-six)."""
    def _fn(*, market_date, desk_dir=None, current=None, downloader=None, ops=None):
        return fps.capture_for_daily_close(
            market_date=market_date, desk_dir=desk_dir,
            current=_cur(market_date, drop_books=_ALL_BOOK_KEYS),
            downloader=_dl(_table()), ops=ops or _ops())
    return _fn


def _drop_books_seam(drop):
    def _fn(*, market_date, desk_dir=None, current=None, downloader=None, ops=None):
        return fps.capture_for_daily_close(
            market_date=market_date, desk_dir=desk_dir,
            current=_cur(market_date, drop_books=drop),
            downloader=_dl(_table()), ops=ops or _ops())
    return _fn


# =========================================================================== #
# FRESH CAPTURE (Part J 1-10)
# =========================================================================== #
class TestFreshCapture:
    def test_mandatory_active_snapshot_captured(self, tmp_path):  # (1)
        out = _run_close(tmp_path, capture_fn=_capture_seam())
        fpc = out["forward_prediction_capture"]
        assert fpc["mandatory_book_id"] == "fundamental_momentum_50_50_top25"
        assert fpc["mandatory_active_snapshot_created"] is True
        assert fpc["mandatory_active_snapshot_persisted"] is True
        assert out["forward_evidence_status"] == fps.EVIDENCE_COMPLETE

    def test_all_shadow_snapshots_captured(self, tmp_path):  # (2)
        out = _run_close(tmp_path, capture_fn=_capture_seam())
        fpc = out["forward_prediction_capture"]
        assert fpc["snapshots_created"] == 6
        ids = set(fpc["persisted_snapshot_ids"])
        for _model, book, _size, _role in fps.SUPPORTED_BOOKS:
            assert "fps_%s_%s" % (book, _D1) in ids

    def test_fresh_close_is_not_a_presence_check(self, tmp_path):  # (3)
        out = _run_close(tmp_path, capture_fn=_capture_seam())
        fpc = out["forward_prediction_capture"]
        assert fpc.get("read_only_presence_check") is not True
        assert fpc["status"] == "SNAPSHOTS_CAPTURED"

    def test_performed_write_true_on_append(self, tmp_path):  # (4)
        out = _run_close(tmp_path, capture_fn=_capture_seam())
        assert out["forward_prediction_capture"]["performed_write"] is True

    def test_snapshots_verified_from_storage(self, tmp_path):  # (5)
        out = _run_close(tmp_path, capture_fn=_capture_seam())
        fpc = out["forward_prediction_capture"]
        assert fpc["verification_complete"] is True
        assert len(fpc["persisted_snapshot_ids"]) == 6
        stored = {r["snapshot_id"] for r in fps._book_snapshots(
            _ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE))}
        assert stored == set(fpc["persisted_snapshot_ids"])

    def test_same_date_duplicate_impossible(self, tmp_path):  # (6)
        _cap(tmp_path, 4)
        before = len(_ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE))
        out = _cap(tmp_path, 4)
        assert out["snapshots_created"] == 0
        assert out["snapshots_already_present"] == 6
        assert len(_ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE)) == before
        assert fps.verify_prediction_ledgers(tmp_path)["all_intact"] is True

    def test_capture_uses_exact_close_market_date(self, tmp_path):  # (7)
        out = _run_close(tmp_path, capture_fn=_capture_seam())
        assert out["forward_prediction_capture"]["market_date"] == _D1
        rows = _ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE)
        assert all(r["market_date"] == _D1 for r in rows)

    def test_point_in_time_model_inputs_required(self, tmp_path):  # (8)
        stale = _cur(_SESS[3])  # model inputs one session behind the close
        out = fps.capture_snapshots(market_date=_D1, desk_dir=tmp_path,
                                    current=stale, ops=_ops(),
                                    downloader=_dl(_table()))
        assert out["status"] == "SNAPSHOTS_UNAVAILABLE"
        assert out["evidence_status"] == fps.EVIDENCE_BLOCKED
        assert all("MODEL_DATE_MISMATCH" in r
                   for r in out["unavailable_reasons"].values())
        assert not (desk._desk_dir(tmp_path) / fps.SNAPSHOT_LEDGER_FILE).exists()

    def test_no_get_endpoint_creates_snapshots(self, client, env):  # (9)
        for route in ("/v1/operations/daily-close",
                      "/v1/operations/daily-close/progress",
                      "/v1/evidence/prediction-skill",
                      "/v1/evidence/prediction-skill/recovery-status?market_date=%s" % _D1):
            r = client.get(route, headers=_AUTH)
            assert r.status_code == 200, route
        assert not (env["desk"] / fps.SNAPSHOT_LEDGER_FILE).exists()
        assert not (env["desk"] / fps.ARTIFACT_LEDGER_FILE).exists()

    def test_page_load_creates_no_snapshots(self, client, env):  # (10)
        r = client.get("/ui/")
        assert r.status_code == 200
        assert not (env["desk"] / fps.SNAPSHOT_LEDGER_FILE).exists()


# =========================================================================== #
# ATOMIC ORDERING (Part C — the July-24 incident class)
# =========================================================================== #
class TestAtomicOrdering:
    def test_snapshots_survive_total_price_fetch_failure(self, tmp_path):
        def broken_dl(_symbol, _start):
            raise ConnectionError("provider unreachable")
        out = fps.capture_snapshots(market_date=_D1, desk_dir=tmp_path,
                                    current=_cur(_D1), ops=_ops(),
                                    downloader=broken_dl)
        # The evidence is durable even though every price fetch failed.
        assert out["snapshots_created"] == 6
        assert out["verification_complete"] is True
        assert out["evidence_status"] == fps.EVIDENCE_COMPLETE
        assert out["price_capture"]["tickers_priced"] == 0
        rows = fps._book_snapshots(_ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE))
        assert len(rows) == 6

    def test_capture_stage_precedes_price_stage(self, tmp_path):
        stages: list = []
        fps.capture_snapshots(market_date=_D1, desk_dir=tmp_path,
                              current=_cur(_D1), ops=_ops(),
                              downloader=_dl(_table()), progress=stages.append)
        assert stages.index("CAPTURE_FORWARD_BOOKS") < stages.index("CAPTURE_MATURITY_PRICES")

    def test_artifact_bundle_frozen_with_capture(self, tmp_path):
        out = _cap(tmp_path, 4)
        assert out["artifact_bundle_id"] == "fca_%s" % _D1
        bundle = fps.find_artifact_bundle(_D1, tmp_path)
        assert bundle is not None
        assert bundle["artifact_hash"] == out["artifact_hash"]
        arts = bundle["artifacts"]
        assert len(arts["book_snapshots"]) == 6
        assert len(arts["cross_sections"]) == 3
        # Content hash is valid across the JSON round trip.
        assert fps._content_hash(arts) == bundle["artifact_hash"]

    def test_close_journal_filename_pinned(self):
        # fps reads the daily-close journal read-only; the constants must agree.
        assert fps._CLOSE_JOURNAL_FILE == dc.DAILY_CLOSE_JOURNAL_FILE


# =========================================================================== #
# FAILURE SEMANTICS (Part J 11-16)
# =========================================================================== #
class TestFailureSemantics:
    def test_zero_of_six_is_not_ordinary_success(self, tmp_path):  # (11)
        out = _run_close(tmp_path, capture_fn=_no_books_seam())
        # the operational close itself remains valid (this world's first close
        # records the initial baseline)…
        assert out["close_status"] == dc.INITIAL_BASELINE_RECORDED
        # …but the evidence state is explicitly BLOCKED and loudly reported.
        assert out["forward_evidence_status"] == fps.EVIDENCE_BLOCKED
        fpc = out["forward_prediction_capture"]
        assert fpc["snapshots_created"] == 0
        assert fpc["evidence_status"] == fps.EVIDENCE_BLOCKED
        assert "FORWARD EVIDENCE CAPTURE FAILED" in out["explanation"]

    def test_mandatory_failure_is_explicit(self, tmp_path):  # (12)
        out = _run_close(tmp_path, capture_fn=_drop_books_seam(
            ("fundamental_momentum_50_50_top25",)))
        fpc = out["forward_prediction_capture"]
        assert fpc["snapshots_created"] == 5
        assert fpc["mandatory_active_snapshot_persisted"] is False
        assert out["forward_evidence_status"] == fps.EVIDENCE_BLOCKED
        assert "fundamental_momentum_50_50_top25" in fpc["unavailable_reasons"]

    def test_shadow_only_failure_is_partial_and_pnl_intact(self, tmp_path):  # (13)
        out = _run_close(tmp_path, capture_fn=_drop_books_seam(("mom_6_1_top50",)))
        fpc = out["forward_prediction_capture"]
        assert fpc["snapshots_created"] == 5
        assert fpc["mandatory_active_snapshot_persisted"] is True
        assert out["forward_evidence_status"] == fps.EVIDENCE_PARTIAL
        assert "PARTIAL" in out["explanation"]
        assert out["close_status"] == dc.INITIAL_BASELINE_RECORDED
        assert out["decision_recorded"] is True             # operational close intact
        journal = desk._read_ledger(desk._desk_dir(tmp_path),
                                    dc.DAILY_CLOSE_JOURNAL_FILE)
        assert len(journal) == 1 and journal[0]["market_date"] == _D1

    def test_operational_and_evidence_states_are_separate(self, tmp_path):  # (14)
        out = _run_close(tmp_path, capture_fn=_no_books_seam())
        assert out["close_status"] in dc.ALL_CLOSE_STATUSES
        assert out["forward_evidence_status"] not in dc.ALL_CLOSE_STATUSES
        assert out["forward_evidence_status"] == fps.EVIDENCE_BLOCKED

    def test_capture_exception_surfaces(self, tmp_path):  # (15)
        def boom(**_kw):
            raise ValueError("capture exploded")
        out = _run_close(tmp_path, capture_fn=boom)
        fpc = out["forward_prediction_capture"]
        assert fpc["status"] == "CAPTURE_ERROR"
        assert fpc["evidence_status"] == fps.EVIDENCE_BLOCKED
        assert out["forward_evidence_status"] == fps.EVIDENCE_BLOCKED
        assert any("Forward prediction capture failed" in w for w in out["warnings"])
        assert "FORWARD EVIDENCE CAPTURE FAILED" in out["explanation"]

    def test_missing_books_have_exact_reasons(self, tmp_path):  # (16)
        out = _run_close(tmp_path, capture_fn=_drop_books_seam(
            ("composite_sn_top25", "composite_sn_top50")))
        reasons = out["forward_prediction_capture"]["unavailable_reasons"]
        assert set(reasons) == {"composite_sn_top25", "composite_sn_top50"}
        assert all("BOOK_UNAVAILABLE" in r for r in reasons.values())


# =========================================================================== #
# IDEMPOTENCY (Part J 17-20)
# =========================================================================== #
class TestIdempotency:
    def test_rerun_creates_no_duplicate_marks_or_snapshots(self, tmp_path):  # (17, 18)
        _run_close(tmp_path, capture_fn=_capture_seam())
        journal = len(desk._read_ledger(desk._desk_dir(tmp_path), dc.DAILY_CLOSE_JOURNAL_FILE))
        snaps = len(_ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE))
        out2 = _run_close(tmp_path, capture_fn=_capture_seam())
        assert out2["close_status"] == dc.ALREADY_PROCESSED
        assert len(desk._read_ledger(desk._desk_dir(tmp_path),
                                     dc.DAILY_CLOSE_JOURNAL_FILE)) == journal
        assert len(_ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE)) == snaps

    def test_rerun_never_backfills_a_missed_date(self, tmp_path):  # (19)
        _run_close(tmp_path, capture_fn=_no_books_seam())    # processed, 0 snapshots
        out2 = _run_close(tmp_path, capture_fn=_capture_seam())
        assert out2["close_status"] == dc.ALREADY_PROCESSED
        fpc = out2["forward_prediction_capture"]
        assert fpc["status"] == "SNAPSHOTS_NOT_CAPTURED"     # presence check only
        assert fpc["read_only_presence_check"] is True
        assert out2["forward_evidence_status"] == fps.EVIDENCE_BLOCKED
        assert "FORWARD EVIDENCE IS MISSING" in out2["explanation"]
        assert not (desk._desk_dir(tmp_path) / fps.SNAPSHOT_LEDGER_FILE).exists()

    def test_outcome_maturation_is_idempotent(self, tmp_path):  # (20)
        _cap(tmp_path, 4)
        _extend(tmp_path, [5, 6])
        first = fps.mature_outcomes(desk_dir=tmp_path)
        assert first["outcomes_newly_matured"] > 0
        again = fps.mature_outcomes(desk_dir=tmp_path)
        assert again["outcomes_newly_matured"] == 0
        assert again["performed_write"] is False


# =========================================================================== #
# RECOVERY (Part J 21-32)
# =========================================================================== #
class TestRecovery:
    def test_recovery_status_is_read_only(self, tmp_path, monkeypatch):  # (21)
        _blocked_close(tmp_path, monkeypatch)
        sdir = desk._desk_dir(tmp_path)
        before = sorted(p.name for p in sdir.iterdir())
        rs = fps.load_recovery_status(market_date=_D1, desk_dir=tmp_path)
        assert rs["performed_write"] is False
        assert rs["changes_operational_state"] is False
        assert sorted(p.name for p in sdir.iterdir()) == before

    def test_recoverable_world_reports_frozen_bundle(self, tmp_path, monkeypatch):
        out = _blocked_close(tmp_path, monkeypatch)
        assert out["forward_evidence_status"] == fps.EVIDENCE_BLOCKED
        fpc = out["forward_prediction_capture"]
        assert all("SNAPSHOT_APPEND_FAILED" in r
                   for r in fpc["unavailable_reasons"].values())
        rs = fps.load_recovery_status(market_date=_D1, desk_dir=tmp_path)
        assert rs["recovery_status"] == fps.REC_RECOVERABLE
        assert rs["close_processed"] is True
        assert rs["frozen_artifacts_found"] is True
        assert rs["artifact_hash_valid"] is True
        assert len(rs["recoverable_books"]) == 6
        assert rs["requires_recalculation"] is False

    def test_recovery_rejects_absent_artifacts(self, tmp_path):  # (22)
        _run_close(tmp_path, capture_fn=_no_books_seam())    # processed, no bundle
        rs = fps.load_recovery_status(market_date=_D1, desk_dir=tmp_path)
        assert rs["recovery_status"] == fps.REC_NOT_RECOVERABLE
        assert rs["frozen_artifacts_found"] is False
        assert rs["requires_recalculation"] is True
        out = fps.recover_missed_close(market_date=_D1,
                                       confirmation=fps.RECOVERY_CONFIRM_TOKEN,
                                       desk_dir=tmp_path)
        assert out["status"] == "RECOVERY_REJECTED_NOT_RECOVERABLE"
        assert out["recovered_books"] == []
        assert not (desk._desk_dir(tmp_path) / fps.SNAPSHOT_LEDGER_FILE).exists()

    def test_recovery_rejects_tampered_bundle(self, tmp_path, monkeypatch):  # (23)
        _blocked_close(tmp_path, monkeypatch)
        path = desk._desk_dir(tmp_path) / fps.ARTIFACT_LEDGER_FILE
        doc = json.loads(path.read_text(encoding="utf-8"))
        row = next(r for r in doc["rows"] if r["kind"] == fps.KIND_ARTIFACT_BUNDLE)
        row["artifacts"]["book_snapshots"][0]["membership"].append("ZZZ")  # tamper
        path.write_text(json.dumps(doc), encoding="utf-8")
        rs = fps.load_recovery_status(market_date=_D1, desk_dir=tmp_path)
        assert rs["artifact_hash_valid"] is False
        assert rs["recovery_status"] == fps.REC_NOT_RECOVERABLE
        out = fps.recover_missed_close(market_date=_D1,
                                       confirmation=fps.RECOVERY_CONFIRM_TOKEN,
                                       desk_dir=tmp_path)
        assert out["status"] == "RECOVERY_REJECTED_NOT_RECOVERABLE"
        assert not (desk._desk_dir(tmp_path) / fps.SNAPSHOT_LEDGER_FILE).exists()

    def test_recovery_rejects_required_recalculation(self, tmp_path, monkeypatch):  # (24)
        _blocked_close(tmp_path, monkeypatch)
        path = desk._desk_dir(tmp_path) / fps.ARTIFACT_LEDGER_FILE
        doc = json.loads(path.read_text(encoding="utf-8"))
        row = next(r for r in doc["rows"] if r["kind"] == fps.KIND_ARTIFACT_BUNDLE)
        for b in row["artifacts"]["book_snapshots"]:
            b.pop("membership", None)                       # strip a required field
        row["artifact_hash"] = fps._content_hash(row["artifacts"])  # keep hash valid
        path.write_text(json.dumps(doc), encoding="utf-8")
        rs = fps.load_recovery_status(market_date=_D1, desk_dir=tmp_path)
        assert rs["artifact_hash_valid"] is True
        assert rs["recovery_status"] == fps.REC_NOT_RECOVERABLE
        assert rs["requires_recalculation"] is True
        assert all("BUNDLE_ROW_INCOMPLETE" in r
                   for r in rs["unavailable_books"].values())

    def test_recovery_rejects_unprocessed_date(self, tmp_path):  # (25)
        rs = fps.load_recovery_status(market_date=_D1, desk_dir=tmp_path)
        assert rs["recovery_status"] == fps.REC_DATE_NOT_PROCESSED
        out = fps.recover_missed_close(market_date=_D1,
                                       confirmation=fps.RECOVERY_CONFIRM_TOKEN,
                                       desk_dir=tmp_path)
        assert out["status"] == "RECOVERY_REJECTED_DATE_NOT_PROCESSED"
        assert out["performed_write"] is False

    def test_recovery_rejects_when_snapshots_exist(self, tmp_path):  # (26)
        _run_close(tmp_path, capture_fn=_capture_seam())     # healthy close
        rs = fps.load_recovery_status(market_date=_D1, desk_dir=tmp_path)
        assert rs["recovery_status"] == fps.REC_ALREADY_PRESENT
        out = fps.recover_missed_close(market_date=_D1,
                                       confirmation=fps.RECOVERY_CONFIRM_TOKEN,
                                       desk_dir=tmp_path)
        assert out["status"] == "RECOVERY_REJECTED_SNAPSHOTS_ALREADY_PRESENT"
        assert out["performed_write"] is False

    def test_valid_recovery_appends_exact_frozen_snapshots(self, tmp_path, monkeypatch):  # (27)
        _blocked_close(tmp_path, monkeypatch)
        bundle = fps.find_artifact_bundle(_D1, tmp_path)
        out = fps.recover_missed_close(market_date=_D1,
                                       confirmation=fps.RECOVERY_CONFIRM_TOKEN,
                                       desk_dir=tmp_path)
        assert out["status"] == "RECOVERED_FROM_FROZEN_ARTIFACTS"
        assert len(out["recovered_books"]) == 6
        assert out["verification_complete"] is True
        stored = fps._book_snapshots(_ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE))
        frozen = {b["book_id"]: b for b in bundle["artifacts"]["book_snapshots"]}
        assert len(stored) == 6
        for row in stored:
            src = frozen[row["book_id"]]
            for key in ("snapshot_id", "market_date", "membership", "target_weights",
                        "created_at", "price_data_through", "model_version"):
                assert row[key] == src[key], key
        assert fps.verify_prediction_ledgers(tmp_path)["all_intact"] is True

    def test_recovery_never_queries_market_data(self, tmp_path, monkeypatch):  # (28)
        _blocked_close(tmp_path, monkeypatch)

        def forbidden(*_a, **_k):
            raise AssertionError("recovery must never touch a provider")
        monkeypatch.setattr(desk, "_resolve_downloader", forbidden)
        out = fps.recover_missed_close(market_date=_D1,
                                       confirmation=fps.RECOVERY_CONFIRM_TOKEN,
                                       desk_dir=tmp_path)
        assert out["status"] == "RECOVERED_FROM_FROZEN_ARTIFACTS"
        assert not (desk._desk_dir(tmp_path) / fps.PRICE_STORE_FILE).exists()

    def test_recovery_never_recalculates_models(self, tmp_path, monkeypatch):  # (29)
        _blocked_close(tmp_path, monkeypatch)

        def forbidden(*_a, **_k):
            raise AssertionError("recovery must never rebuild the model")
        monkeypatch.setattr(eng, "build_current", forbidden)
        out = fps.recover_missed_close(market_date=_D1,
                                       confirmation=fps.RECOVERY_CONFIRM_TOKEN,
                                       desk_dir=tmp_path)
        assert out["status"] == "RECOVERED_FROM_FROZEN_ARTIFACTS"

    def test_recovery_changes_no_operational_state(self, tmp_path, monkeypatch):  # (30)
        _blocked_close(tmp_path, monkeypatch)
        sdir = desk._desk_dir(tmp_path)
        journal_before = desk._read_ledger(sdir, dc.DAILY_CLOSE_JOURNAL_FILE)
        fps.recover_missed_close(market_date=_D1,
                                 confirmation=fps.RECOVERY_CONFIRM_TOKEN,
                                 desk_dir=tmp_path)
        assert desk._read_ledger(sdir, dc.DAILY_CLOSE_JOURNAL_FILE) == journal_before
        for f in (desk.BOOKS_FILE, desk.ORDERS_FILE, desk.FILLS_FILE,
                  desk.PERFORMANCE_FILE):
            assert not (sdir / f).exists()

    def test_recovery_is_token_gated(self, tmp_path, monkeypatch, client, env):  # (31)
        _blocked_close(tmp_path, monkeypatch)
        out = fps.recover_missed_close(market_date=_D1, confirmation="WRONG",
                                       desk_dir=tmp_path)
        assert out["status"] == "RECOVERY_CONFIRM_REQUIRED"
        assert out["performed_write"] is False
        assert not (desk._desk_dir(tmp_path) / fps.SNAPSHOT_LEDGER_FILE).exists()
        r = client.post("/v1/evidence/prediction-skill/recover-missed-close",
                        headers=_AUTH,
                        json={"market_date": _D1, "confirmation": "WRONG"})
        assert r.status_code == 400

    def test_repeated_valid_recovery_is_idempotent(self, tmp_path, monkeypatch):  # (32)
        _blocked_close(tmp_path, monkeypatch)
        first = fps.recover_missed_close(market_date=_D1,
                                         confirmation=fps.RECOVERY_CONFIRM_TOKEN,
                                         desk_dir=tmp_path)
        assert first["status"] == "RECOVERED_FROM_FROZEN_ARTIFACTS"
        rows = len(_ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE))
        second = fps.recover_missed_close(market_date=_D1,
                                          confirmation=fps.RECOVERY_CONFIRM_TOKEN,
                                          desk_dir=tmp_path)
        assert second["status"] == "RECOVERY_REJECTED_SNAPSHOTS_ALREADY_PRESENT"
        assert second["performed_write"] is False
        assert len(_ledger_rows(tmp_path, fps.SNAPSHOT_LEDGER_FILE)) == rows


# =========================================================================== #
# MISSED-CAPTURE RECORD (Part J 33-35)
# =========================================================================== #
class TestMissedCaptureRecord:
    def _missed_world(self, tmp):
        _run_close(tmp, capture_fn=_no_books_seam())         # processed, no bundle
        out = fps.recover_missed_close(market_date=_D1,
                                       confirmation=fps.RECOVERY_CONFIRM_TOKEN,
                                       desk_dir=tmp)
        assert out["status"] == "RECOVERY_REJECTED_NOT_RECOVERABLE"
        assert out["missed_capture_record"]["status"] == "MISSED_CAPTURE_RECORDED"
        return out

    def test_missed_record_is_not_a_snapshot(self, tmp_path):  # (33)
        self._missed_world(tmp_path)
        incidents = fps.list_evidence_incidents(tmp_path)
        assert any(r["kind"] == fps.KIND_CAPTURE_MISSED for r in incidents)
        summary = fps.prediction_skill_summary(desk_dir=tmp_path)
        assert summary["snapshot_count"] == 0
        assert summary["evidence_state"] == fps.EV_NO_SNAPSHOTS

    def test_missed_record_is_not_an_outcome(self, tmp_path):  # (34)
        self._missed_world(tmp_path)
        summary = fps.prediction_skill_summary(desk_dir=tmp_path)
        assert summary["matured_outcome_count"] == 0
        assert fps._outcome_rows(tmp_path) == []

    def test_missed_record_preserves_valid_close(self, tmp_path):  # (35)
        self._missed_world(tmp_path)
        sdir = desk._desk_dir(tmp_path)
        journal = desk._read_ledger(sdir, dc.DAILY_CLOSE_JOURNAL_FILE)
        assert len(journal) == 1
        assert journal[0]["market_date"] == _D1
        assert journal[0]["decision"] == dc.DECISION_BASELINE
        rec = next(r for r in fps.list_evidence_incidents(tmp_path)
                   if r["kind"] == fps.KIND_CAPTURE_MISSED)
        assert rec["close_remained_operationally_valid"] is True
        assert rec["snapshot_fabricated"] is False
        assert rec["next_eligible_snapshot_date"] is None

    def test_missed_record_is_idempotent(self, tmp_path):
        self._missed_world(tmp_path)
        again = fps.record_missed_capture(market_date=_D1, missing_books=["x"],
                                          reason="dup", desk_dir=tmp_path)
        assert again["status"] == "MISSED_CAPTURE_ALREADY_RECORDED"
        rows = [r for r in fps.list_evidence_incidents(tmp_path)
                if r["kind"] == fps.KIND_CAPTURE_MISSED]
        assert len(rows) == 1


# =========================================================================== #
# CLOSE PROGRESS + SINGLE FLIGHT (Part I)
# =========================================================================== #
class TestCloseProgress:
    def test_empty_progress_is_controlled(self, tmp_path):
        prog = dc.load_close_progress(tmp_path)
        assert prog["status"] == "NO_CLOSE_PROGRESS"
        assert prog["running"] is False
        assert [s["key"] for s in prog["stages"]] == [k for k, _ in dc.CLOSE_STAGES]

    def test_progress_written_and_finalized_by_close(self, tmp_path):
        out = _run_close(tmp_path, capture_fn=_capture_seam())
        prog = dc.load_close_progress(tmp_path)
        assert prog["status"] == "CLOSE_FINISHED"
        assert prog["running"] is False
        assert prog["done"] is True
        assert prog["final_close_status"] == out["close_status"]
        assert prog["final_evidence_status"] == out["forward_evidence_status"]
        assert prog["started_at"] and prog["updated_at"]

    def test_duplicate_post_rejected_while_running(self, tmp_path):
        assert dc._CLOSE_LOCK.acquire(blocking=False)
        try:
            out = dc.run_daily_close(
                confirm=dc.EXECUTE_CONFIRMATION, today=_D2, desk_dir=tmp_path,
                operational_loader=(lambda _t: _ops()), gate_loader=_hold_gate,
                refresh_fn=_ok_refresh(_D1))
        finally:
            dc._CLOSE_LOCK.release()
        assert out["status"] == dc.CLOSE_IN_PROGRESS
        assert out["close_status"] is None
        assert out["performed_write"] is False
        assert "progress" in out
        assert not (desk._desk_dir(tmp_path) / dc.DAILY_CLOSE_JOURNAL_FILE).exists()

    def test_get_daily_close_writes_no_progress(self, tmp_path):
        dc.load_daily_close(today=_D2, desk_dir=tmp_path,
                            operational=_ops(), gate=_hold_gate())
        assert not (desk._desk_dir(tmp_path) / dc.CLOSE_PROGRESS_FILE).exists()

    def test_progress_route_read_only(self, client, env):
        r = client.get("/v1/operations/daily-close/progress", headers=_AUTH)
        assert r.status_code == 200
        d = r.json()
        assert d["performed_write"] is False
        assert d["creates_orders"] is False
        r2 = client.get("/v1/operations/daily-close/progress")
        assert r2.status_code in (401, 403)
        assert not (env["desk"] / dc.CLOSE_PROGRESS_FILE).exists()

    def test_evidence_status_semantics(self):
        blocked = {"status": "SNAPSHOTS_NOT_CAPTURED",
                   "evidence_status": fps.EVIDENCE_BLOCKED}
        assert dc._forward_evidence_status(blocked, close_processed=True) \
            == fps.EVIDENCE_BLOCKED
        assert dc._forward_evidence_status(blocked, close_processed=False) \
            == dc.EVIDENCE_PENDING_CLOSE
        assert dc._forward_evidence_status(blocked, close_processed=True,
                                           capture_in_flight=True) \
            == dc.EVIDENCE_IN_PROGRESS
        assert dc._forward_evidence_status(
            {"status": "CAPTURE_INACTIVE_OFFLINE_SEAM"}, close_processed=True) \
            == dc.EVIDENCE_INACTIVE_OFFLINE
        assert dc._forward_evidence_status(None, close_processed=True) is None

    def test_get_reports_in_progress_during_running_capture(self, tmp_path):
        # The July-24 incident regression pin: a processed close whose capture is
        # STILL RUNNING must read as IN PROGRESS, never as a missed capture.
        _run_close(tmp_path, capture_fn=_no_books_seam())    # processed, 0 snapshots
        dc._CloseProgress(tmp_path, market_date=_D1, evaluation_date=_D2)
        out = dc.load_daily_close(today=_D2, desk_dir=tmp_path,
                                  operational=_ops(), gate=_hold_gate())
        assert out["forward_evidence_status"] == dc.EVIDENCE_IN_PROGRESS
        dc._progress_finalize(tmp_path, {"close_status": dc.CLOSE_COMPLETE_HOLD,
                                         "forward_evidence_status": fps.EVIDENCE_BLOCKED})
        out2 = dc.load_daily_close(today=_D2, desk_dir=tmp_path,
                                   operational=_ops(), gate=_hold_gate())
        assert out2["forward_evidence_status"] == fps.EVIDENCE_BLOCKED


# =========================================================================== #
# SAFETY (Part J 36-43)
# =========================================================================== #
class TestSafety:
    def test_holdings_cash_orders_unchanged(self, tmp_path, monkeypatch):  # (36-38)
        ops_before = _ops()
        frozen = copy.deepcopy(ops_before)
        _blocked_close(tmp_path, monkeypatch)
        fps.recover_missed_close(market_date=_D1,
                                 confirmation=fps.RECOVERY_CONFIRM_TOKEN,
                                 desk_dir=tmp_path)
        assert ops_before == frozen
        sdir = desk._desk_dir(tmp_path)
        for f in (desk.BOOKS_FILE, desk.ORDERS_FILE, desk.FILLS_FILE):
            assert not (sdir / f).exists()

    def test_no_broker_no_automation_no_weights_no_promotion(self, tmp_path, monkeypatch):  # (39-42)
        _blocked_close(tmp_path, monkeypatch)
        out = fps.recover_missed_close(market_date=_D1,
                                       confirmation=fps.RECOVERY_CONFIRM_TOKEN,
                                       desk_dir=tmp_path)
        for key in ("broker_execution", "automation_enabled",
                    "changes_model_weights", "promotes_challenger",
                    "changes_operational_model", "creates_orders"):
            assert out[key] is False, key
        assert out["changes_operational_state"] is False
        rs = fps.load_recovery_status(market_date=_D1, desk_dir=tmp_path)
        for key in ("creates_orders", "broker_execution", "automation_enabled"):
            assert rs[key] is False, key

    def test_no_credentials_exposed(self, tmp_path, monkeypatch):  # (43)
        _blocked_close(tmp_path, monkeypatch)
        payloads = [
            fps.load_recovery_status(market_date=_D1, desk_dir=tmp_path),
            fps.recover_missed_close(market_date=_D1,
                                     confirmation=fps.RECOVERY_CONFIRM_TOKEN,
                                     desk_dir=tmp_path),
            dc.load_close_progress(tmp_path),
        ]
        dump = json.dumps(payloads).lower()
        for banned in ("api_key", "api-key", "authorization", "secret",
                       "password", "local-dev-key", "eodhd_api"):
            assert banned not in dump, banned


# =========================================================================== #
# UI (Part J 44-49) — static wiring checks
# =========================================================================== #
class TestUiStatic:
    def test_duplicate_submission_guard(self, ui_html):  # (44)
        assert "if (_dcRunInFlight) return;" in ui_html
        assert "_dcRunInFlight = true;" in ui_html
        assert "b.disabled = true;" in ui_html

    def test_elapsed_time_indicator(self, ui_html):  # (45)
        assert 'id="dc-run-elapsed"' in ui_html
        assert 'id="dc-run-overlay"' in ui_html
        assert "_dcFmtElapsed" in ui_html
        assert "do not click Run Daily Close again" in ui_html

    def test_operational_and_evidence_outcomes_distinct(self, ui_html):  # (46)
        assert "forward_evidence_status" in ui_html
        assert 'id="dc-evidence-banner"' in ui_html
        assert "Operational close: " in ui_html

    def test_zero_of_six_failure_is_prominent(self, ui_html):  # (47)
        assert "FORWARD EVIDENCE CAPTURE FAILED" in ui_html
        assert "FORWARD_EVIDENCE_BLOCKED" in ui_html

    def test_no_native_dialogs(self, ui_html):  # (48)
        for pat in ("alert(", "confirm(", "prompt("):
            assert pat not in ui_html, pat

    def test_null_safe_wiring(self, ui_html):  # (49)
        assert ".filter(function (b) { return !!b; })" in ui_html
        assert "if (ov) ov.style.display" in ui_html
        assert "if (p && p.stages)" in ui_html

    def test_progress_poll_is_single(self, ui_html):
        assert "if (polling) return;" in ui_html
        assert "'/v1/operations/daily-close/progress'" in ui_html
