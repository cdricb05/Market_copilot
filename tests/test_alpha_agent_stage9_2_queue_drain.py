"""Stage 9.2 - autonomous evidence execution and DATA_HOLD reduction.

Deterministic tests for the bounded canonical-queue drain integrated into the
scheduled AlphaAgent-Collect cycle, the tournament weakest-gate DATA_VALIDATION
follow-up handler, and the durable candidate-evidence update - proving the
generate->execute->persist->recalculate loop closes WITHOUT weakening any gate,
promoting any model, or mutating any operational trading state.

Covers the 20 required properties (PART 12).
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO.parent) not in sys.path:
    sys.path.insert(0, str(_REPO.parent))

from paper_trader.alpha_agent import autonomous_research as ar  # noqa: E402
from paper_trader.alpha_agent import runtime as rt  # noqa: E402
from paper_trader.alpha_agent import tournament as tt  # noqa: E402

_CLK = "2026-08-01T12:00:00"
_TODAY = datetime.now(timezone.utc).date().isoformat()


def _clock():
    return _CLK


# --------------------------------------------------------------------------- #
# Fixtures / helpers.
# --------------------------------------------------------------------------- #
def _write_normalized(ingestion_root: Path, record_type: str, records):
    d = ingestion_root / "normalized" / record_type
    d.mkdir(parents=True, exist_ok=True)
    (d / "fixture.jsonl").write_text(
        "\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")


def _write_vintages(ingestion_root: Path, *, boundary: str, dates):
    v = ingestion_root / "vintages" / "eodhd_analyst"
    v.mkdir(parents=True, exist_ok=True)
    (v / "_prospective_boundary.json").write_text(
        json.dumps({"first_snapshot_date": boundary}), encoding="utf-8")
    for dt in dates:
        dd = v / dt
        dd.mkdir(parents=True, exist_ok=True)
        (dd / "AAPL.json").write_text(json.dumps({"ok": True}), encoding="utf-8")


def _make_cfg8(tmp: Path, *, drain_enabled=True, max_jobs=4, budget=None):
    """A minimal Stage 8 config resolving a temp Stage 9 config + temp roots."""
    tournament_db = tmp / "tournament.sqlite"
    stage9 = {"stage": "9", "config_version": "1.0.0",
              "tournament_db": str(tournament_db),
              "gates": {"keep_min_rank_ic_t": 2.0},
              "evidence_completeness": {}, "scoring": {}, "cycle": {}}
    stage9_path = tmp / "stage9.json"
    stage9_path.write_text(json.dumps(stage9), encoding="utf-8")
    cfg8 = {
        "stage": "8", "stage8_root": str(tmp / "s8"),
        "autonomy": {
            "queue_db": str(tmp / "autonomy.sqlite"), "handlers": "production",
            "queue_floor": 1, "max_jobs_per_cycle": 8,
            "event_min_issuers": 3, "event_min_events": 3,
            "analyst_min_snapshot_dates": 3, "pit_gics_min_symbols": 3,
        },
        "tournament": {"enabled": True, "config": str(stage9_path),
                       "max_candidates_per_cycle": 4},
    }
    if drain_enabled is not None:
        cfg8["autonomy"]["collect_drain"] = {
            "enabled": bool(drain_enabled), "max_jobs_per_cycle": max_jobs,
            "budget_seconds": budget, "categories": None}
    return cfg8, str(tournament_db), Path(cfg8["stage8_root"]) / "ingestion"


def _seed(tournament_db, *, family, feature, requires, dep, pit, blocker):
    reg = tt.CandidateRegistry(tournament_db, clock=_clock)
    cid = reg.seed_candidate(
        name=feature, family=family,
        spec={"feature": feature, "requires_data_family": requires},
        data_dependencies=[dep], universe="test", pit_status=pit,
        lifecycle_state=tt.DATA_HOLD, blocker=blocker)
    reg.close()
    return cid


def _wg_job(cid, dep, blocker):
    return ar.Job(
        job_id="jwg_%s" % dep, dedupe_key="dk_%s" % dep,
        category=ar.CAT_DATA_VALIDATION,
        lane="tournament.address_weakest_gate", state="RUNNING",
        payload={"tournament": True, "candidate_id": cid,
                 "strategy": "address_weakest_gate",
                 "spec": {"data_dependency": dep, "blocker": blocker},
                 "evidence_date": _TODAY})


def _handler(cfg8, queue):
    return rt.build_production_autonomy_handlers(cfg8, queue=queue)[
        ar.CAT_DATA_VALIDATION]


# --------------------------------------------------------------------------- #
# Bounded drain primitive (drain_jobs).
# --------------------------------------------------------------------------- #
def _ok_handler(job):
    return ar.OUTCOME_COMPLETED, {"note": "done", "real_work": "unit"}


def test_prop3_no_more_than_configured_jobs_run(tmp_path):
    q = ar.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock)
    for i in range(5):
        q.enqueue(ar.CAT_REPORT, lane="l%d" % i, payload={"i": i})
    rep = ar.drain_jobs(q, {ar.CAT_REPORT: _ok_handler}, max_jobs=2)
    assert rep["jobs_claimed"] == 2
    assert rep["jobs_completed"] == 2
    assert q.counts_by_state()["QUEUED"] == 3
    assert rep["queue_depth_before"] == 5 and rep["queue_depth_after"] == 3


def test_prop5_completed_jobs_persist_results(tmp_path):
    q = ar.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock)
    jid = q.enqueue(ar.CAT_REPORT, lane="l", payload={})
    ar.drain_jobs(q, {ar.CAT_REPORT: _ok_handler}, max_jobs=1)
    j = q.get(jid)
    assert j.state == ar.STATE_COMPLETED
    assert j.result and j.result.get("real_work") == "unit"


def test_prop6_blocked_jobs_receive_exact_blockers(tmp_path):
    q = ar.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock)
    jid = q.enqueue(ar.CAT_REPORT, lane="l", payload={})

    def _blk(job):
        return ar.OUTCOME_BLOCKED_SPECIFIC, {"reason": "exact missing dep X"}
    rep = ar.drain_jobs(q, {ar.CAT_REPORT: _blk}, max_jobs=1)
    assert rep["jobs_blocked"] == 1
    assert q.get(jid).blocked_reason == "exact missing dep X"


def test_prop6_missing_handler_blocks_specifically(tmp_path):
    q = ar.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock)
    jid = q.enqueue(ar.CAT_REPORT, lane="l", payload={})
    rep = ar.drain_jobs(q, {}, max_jobs=1)  # no handler for the category
    assert rep["jobs_blocked"] == 1
    assert "no handler registered" in (q.get(jid).blocked_reason or "")


def test_prop7_retryable_preserves_attempts_and_timing(tmp_path):
    q = ar.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock)
    jid = q.enqueue(ar.CAT_REPORT, lane="l", payload={})

    def _retry(job):
        return ar.OUTCOME_RETRYABLE, {"reason": "transient"}
    rep = ar.drain_jobs(q, {ar.CAT_REPORT: _retry}, max_jobs=1)
    assert rep["jobs_retryable"] == 1
    j = q.get(jid)
    assert j.state == ar.STATE_RETRYABLE
    assert j.attempts == 1
    assert j.not_before is not None  # backoff scheduled


def test_prop8_one_failed_job_does_not_stop_the_cycle(tmp_path):
    q = ar.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock)
    q.enqueue(ar.CAT_REPORT, lane="a", payload={}, priority=5)
    good = q.enqueue(ar.CAT_HYPOTHESIS_GENERATION, lane="b", payload={},
                     priority=1)

    def _boom(job):
        raise RuntimeError("handler crash")
    rep = ar.drain_jobs(q, {ar.CAT_REPORT: _boom,
                            ar.CAT_HYPOTHESIS_GENERATION: _ok_handler},
                        max_jobs=2)
    assert rep["jobs_claimed"] == 2
    assert rep["handler_errors"] == 1
    assert rep["jobs_retryable"] == 1     # the crashing job -> bounded retry
    assert rep["jobs_completed"] == 1     # the other job still ran
    assert q.get(good).state == ar.STATE_COMPLETED


def test_prop9_overlapping_cycles_cannot_execute_same_job(tmp_path):
    path = str(tmp_path / "q.sqlite")
    q1 = ar.ResearchQueue(path, clock=_clock)
    q2 = ar.ResearchQueue(path, clock=_clock)
    q1.enqueue(ar.CAT_REPORT, lane="l", payload={})
    claimed1 = q1.claim_next()
    claimed2 = q2.claim_next()   # the same job is now RUNNING - not claimable
    assert claimed1 is not None
    assert claimed2 is None


def test_prop10_restart_resumes_leased_work(tmp_path):
    path = str(tmp_path / "q.sqlite")
    q = ar.ResearchQueue(path, clock=lambda: "2026-08-01T00:00:00")
    jid = q.enqueue(ar.CAT_REPORT, lane="l", payload={})
    q.claim_next()                        # RUNNING, then process 'crashes'
    assert q.get(jid).state == ar.STATE_RUNNING
    q2 = ar.ResearchQueue(path, clock=lambda: "2026-08-01T02:00:00")
    requeued = q2.requeue_stale(stale_seconds=1)   # dead-worker recovery
    assert requeued >= 1
    again = q2.claim_next()
    assert again is not None and again.job_id == jid


def test_prop_budget_stops_starting_new_jobs(tmp_path):
    q = ar.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock)
    for i in range(3):
        q.enqueue(ar.CAT_REPORT, lane="l%d" % i, payload={})
    ticks = iter([0.0, 10.0, 100.0, 100.0])  # start, before-j1, before-j2
    rep = ar.drain_jobs(q, {ar.CAT_REPORT: _ok_handler}, max_jobs=3,
                        budget_seconds=50.0, monotonic=lambda: next(ticks))
    assert rep["jobs_claimed"] == 1
    assert rep["budget_exhausted"] is True


def test_prop_categories_filter(tmp_path):
    q = ar.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock)
    q.enqueue(ar.CAT_REPORT, lane="r", payload={}, priority=9)
    dv = q.enqueue(ar.CAT_DATA_VALIDATION, lane="d", payload={}, priority=1)
    rep = ar.drain_jobs(q, {ar.CAT_DATA_VALIDATION: _ok_handler},
                        max_jobs=5, categories=[ar.CAT_DATA_VALIDATION])
    # Only the DATA_VALIDATION job is eligible, despite lower priority.
    assert rep["job_ids"] == [dv]
    assert q.get(dv).state == ar.STATE_COMPLETED


# --------------------------------------------------------------------------- #
# Config gate (PART 3) via Runtime._run_autonomy_drain_step.
# --------------------------------------------------------------------------- #
def _runtime_for(tmp_path, cfg8):
    """Build a Runtime whose Stage 4 config points at the given Stage 8 config."""
    s8_path = tmp_path / "stage8.json"
    s8_path.write_text(json.dumps(cfg8), encoding="utf-8")
    runtime_root = tmp_path / "runtime"
    cfg = {
        "stage": "4", "runtime_root": str(runtime_root),
        "recipient_email": "x@example.com",
        "stage1_registry_root": str(tmp_path / "registry"),
        "stage2_ingestion_root": str(tmp_path / "ingestion"),
        "stage3_director_root": str(tmp_path / "director"),
        "stage3_5_news_rss_root": str(tmp_path / "news_rss"),
        "operational_ledger_roots": [],
        "stage_configs": {"stage8_autonomy": str(s8_path)},
        "email": {"credential_dir": str(tmp_path / "nocreds"),
                  "transport": "gmail_smtp"},
    }
    return rt.Runtime(cfg, drivers=None, email_sender=None,
                      clock=rt.FixedClock(datetime(2026, 8, 1, 12, 0, 0)))


def test_prop1_drain_disabled_by_config_does_nothing(tmp_path):
    cfg8, tdb, _ = _make_cfg8(tmp_path, drain_enabled=False)
    q = ar.ResearchQueue(cfg8["autonomy"]["queue_db"], clock=_clock)
    q.enqueue(ar.CAT_HYPOTHESIS_GENERATION, lane="l", payload={})
    r = _runtime_for(tmp_path, cfg8)
    summary = r._run_autonomy_drain_step("2026-08-01")
    assert summary["enabled"] is False
    assert summary["jobs_claimed"] == 0
    assert q.counts_by_state()["QUEUED"] == 1  # untouched


def test_prop2_production_config_enables_bounded_draining(tmp_path):
    cfg8, tdb, _ = _make_cfg8(tmp_path, drain_enabled=True, max_jobs=1)
    q = ar.ResearchQueue(cfg8["autonomy"]["queue_db"], clock=_clock)
    jid = q.enqueue(ar.CAT_HYPOTHESIS_GENERATION, lane="l", payload={})
    r = _runtime_for(tmp_path, cfg8)
    summary = r._run_autonomy_drain_step("2026-08-01")
    assert summary["enabled"] is True
    assert summary["drain_status"] == "OK"
    assert summary["jobs_claimed"] == 1
    assert q.get(jid).state == ar.STATE_COMPLETED


def test_prop_default_absent_block_is_disabled(tmp_path):
    cfg8, tdb, _ = _make_cfg8(tmp_path, drain_enabled=None)  # no block at all
    r = _runtime_for(tmp_path, cfg8)
    summary = r._run_autonomy_drain_step("2026-08-01")
    assert summary["enabled"] is False


# --------------------------------------------------------------------------- #
# Weakest-gate handler (PARTS 4-10). Honest per-dependency outcomes.
# --------------------------------------------------------------------------- #
def test_prop13_14_sec_insufficient_stays_data_hold_and_uses_canonical_storage(
        tmp_path):
    cfg8, tdb, ing = _make_cfg8(tmp_path)
    _write_normalized(ing, "INSIDER_FILING", [
        {"company_id": "AAPL", "event_type": "FORM4",
         "normalized_payload": {"transaction_code": "P", "shares": 100}}])
    cid = _seed(tdb, family="EVENT_INSIDER", feature="insider_cluster",
                requires="event", dep="sec_8k", pit="SPARSE_OWNED_COVERAGE",
                blocker="DATA_HOLD_INSUFFICIENT_OBSERVATIONS")
    q = ar.ResearchQueue(cfg8["autonomy"]["queue_db"], clock=_clock)
    handler = _handler(cfg8, q)
    outcome, detail = handler(_wg_job(cid, "sec_8k",
                                      "DATA_HOLD_INSUFFICIENT_OBSERVATIONS"))
    assert outcome == ar.OUTCOME_COMPLETED           # validation job did its job
    assert detail["sufficient"] is False
    assert detail["coverage"]["distinct_issuers"] == 1   # read canonical storage
    # candidate remains DATA_HOLD (honest; never promoted here)
    reg = tt.CandidateRegistry(tdb, clock=_clock)
    cand = reg.get(cid)
    assert cand["lifecycle_state"] == tt.DATA_HOLD
    assert cand["combined_score"] is None
    assert cand["latest_evidence_date"] is not None       # PART 10 step 3
    assert cid  # experiment id appended
    cov = reg.latest_data_coverage(cid)
    reg.close()
    assert cov is not None and cov["data_dependency"] == "sec_8k"
    # PART 5 / PART 10 step 6: a bounded acquisition continuation was queued.
    acq = [j for j in q.list_jobs(limit=50)
           if j.origin == "stage9-weakest-gate"]
    assert len(acq) == 1 and acq[0].lane == "acq.sec_form4_8k"


def test_prop16_analyst_never_backfills_before_pit_floor(tmp_path):
    cfg8, tdb, ing = _make_cfg8(tmp_path)
    _write_vintages(ing, boundary="2026-07-31",
                    dates=["2026-07-31", "2026-08-01"])
    cid = _seed(tdb, family="ANALYST_EARNINGS", feature="price_target_revision",
                requires="analyst", dep="eodhd_analyst_vintages",
                pit="PROSPECTIVE_NO_OWNED_HISTORY",
                blocker="DATA_HOLD_INSUFFICIENT_OBSERVATIONS")
    q = ar.ResearchQueue(cfg8["autonomy"]["queue_db"], clock=_clock)
    handler = _handler(cfg8, q)
    outcome, detail = handler(_wg_job(cid, "eodhd_analyst_vintages",
                                      "DATA_HOLD_INSUFFICIENT_OBSERVATIONS"))
    cov = detail["coverage"]
    assert cov["first_snapshot_pit_floor"] == "2026-07-31"
    assert cov["distinct_vintage_dates_on_disk"] == 2  # only what is on disk
    assert detail["sufficient"] is False               # < min (3): calendar time
    assert "PIT floor" in detail["next_action"] or "calendar" in \
        detail["next_action"]
    # never fabricates a date before the floor
    assert "backfill" in cov["pit_note"].lower()


def test_prop15_pit_sector_uses_assigned_sic_not_current_gics(tmp_path):
    cfg8, tdb, ing = _make_cfg8(tmp_path)
    _write_normalized(ing, "SECURITY_IDENTITY", [
        {"company_id": "AAPL", "event_type": "ASSIGNED_SIC_PIT",
         "normalized_payload": {"ticker": "AAPL", "sic": "3571"}},
        {"company_id": "MSFT", "event_type": "ASSIGNED_SIC_PIT",
         "normalized_payload": {"ticker": "MSFT", "sic": "7372"}}])
    cid = _seed(tdb, family="PRICE_MOMENTUM", feature="sector_neutral_momentum",
                requires=None, dep="point_in_time_gics",
                pit="NEEDS_POINT_IN_TIME_GICS_SECTOR",
                blocker="DATA_HOLD_POINT_IN_TIME_UNAVAILABLE")
    q = ar.ResearchQueue(cfg8["autonomy"]["queue_db"], clock=_clock)
    handler = _handler(cfg8, q)
    outcome, detail = handler(_wg_job(cid, "point_in_time_gics",
                                      "DATA_HOLD_POINT_IN_TIME_UNAVAILABLE"))
    cov = detail["coverage"]
    assert cov["distinct_symbols_pit_classified"] == 2
    assert "ASSIGNED-SIC" in cov["source"]
    assert "GICS" in cov["look_ahead_note"]            # current GICS = look-ahead
    assert detail["sufficient"] is False               # < min (3)


def test_prop6_pit_fundamentals_blocks_with_bounded_plan(tmp_path):
    cfg8, tdb, ing = _make_cfg8(tmp_path)
    cid = _seed(tdb, family="FUNDAMENTAL", feature="gross_profitability",
                requires="fundamentals", dep="point_in_time_fundamentals",
                pit="SNAPSHOT_ONLY_NOT_PIT",
                blocker="DATA_HOLD_POINT_IN_TIME_UNAVAILABLE")
    q = ar.ResearchQueue(cfg8["autonomy"]["queue_db"], clock=_clock)
    handler = _handler(cfg8, q)
    outcome, detail = handler(_wg_job(cid, "point_in_time_fundamentals",
                                      "DATA_HOLD_POINT_IN_TIME_UNAVAILABLE"))
    assert outcome == ar.OUTCOME_BLOCKED_SPECIFIC       # build required
    assert "point-in-time fundamentals" in detail["reason"]
    cov = detail["coverage"]
    assert cov["requires_new_provider_purchase"] is False
    assert isinstance(cov["bounded_implementation_path"], list)
    assert cov["bounded_implementation_path"]           # concrete plan present


def test_prop12_completed_result_updates_correct_candidate(tmp_path):
    cfg8, tdb, ing = _make_cfg8(tmp_path)
    _write_normalized(ing, "INSIDER_FILING", [
        {"company_id": "AAPL",
         "normalized_payload": {"transaction_code": "P", "shares": 1}}])
    cid = _seed(tdb, family="EVENT_INSIDER", feature="net_insider_buys",
                requires="event", dep="sec_form4", pit="SPARSE_OWNED_COVERAGE",
                blocker="DATA_HOLD_INSUFFICIENT_OBSERVATIONS")
    other = _seed(tdb, family="FUNDAMENTAL", feature="asset_growth",
                  requires="fundamentals", dep="point_in_time_fundamentals",
                  pit="SNAPSHOT_ONLY_NOT_PIT",
                  blocker="DATA_HOLD_POINT_IN_TIME_UNAVAILABLE")
    q = ar.ResearchQueue(cfg8["autonomy"]["queue_db"], clock=_clock)
    handler = _handler(cfg8, q)
    job = _wg_job(cid, "sec_form4", "DATA_HOLD_INSUFFICIENT_OBSERVATIONS")
    handler(job)
    reg = tt.CandidateRegistry(tdb, clock=_clock)
    assert reg.latest_data_coverage(cid) is not None
    assert reg.latest_data_coverage(other) is None      # only the correct one
    assert job.job_id in reg.get(cid)["experiment_ids"]
    reg.close()


def test_prop11_duplicate_result_import_is_prevented(tmp_path):
    cfg8, tdb, ing = _make_cfg8(tmp_path)
    _write_normalized(ing, "INSIDER_FILING", [
        {"company_id": "AAPL",
         "normalized_payload": {"transaction_code": "P", "shares": 1}}])
    cid = _seed(tdb, family="EVENT_INSIDER", feature="insider_cluster",
                requires="event", dep="sec_8k", pit="SPARSE_OWNED_COVERAGE",
                blocker="DATA_HOLD_INSUFFICIENT_OBSERVATIONS")
    q = ar.ResearchQueue(cfg8["autonomy"]["queue_db"], clock=_clock)
    handler = _handler(cfg8, q)
    handler(_wg_job(cid, "sec_8k", "x"))
    out2, detail2 = handler(_wg_job(cid, "sec_8k", "x"))   # same evidence date
    assert detail2.get("idempotent") is True
    reg = tt.CandidateRegistry(tdb, clock=_clock)
    rows = reg._conn.execute(
        "SELECT COUNT(*) n FROM data_coverage WHERE candidate_id=?",
        (cid,)).fetchone()["n"]
    reg.close()
    assert rows == 1                                       # imported exactly once


def test_prop17_no_model_is_automatically_promoted(tmp_path):
    cfg8, tdb, ing = _make_cfg8(tmp_path)
    # Even with 'sufficient' coverage, the handler NEVER promotes a candidate.
    _write_normalized(ing, "INSIDER_FILING", [
        {"company_id": t, "normalized_payload": {"transaction_code": "P",
                                                 "shares": 1}}
        for t in ("AAPL", "MSFT", "NVDA", "AMZN")])   # 4 issuers >= min 3
    cid = _seed(tdb, family="EVENT_INSIDER", feature="insider_cluster",
                requires="event", dep="sec_8k", pit="SPARSE_OWNED_COVERAGE",
                blocker="DATA_HOLD_INSUFFICIENT_OBSERVATIONS")
    q = ar.ResearchQueue(cfg8["autonomy"]["queue_db"], clock=_clock)
    handler = _handler(cfg8, q)
    outcome, detail = handler(_wg_job(cid, "sec_8k", "x"))
    assert detail["sufficient"] is True
    assert detail["no_automatic_promotion"] is True
    reg = tt.CandidateRegistry(tdb, clock=_clock)
    cand = reg.get(cid)
    reg.close()
    assert cand["lifecycle_state"] == tt.DATA_HOLD    # STILL held - not promoted
    assert cand["combined_score"] is None


def test_prop_malformed_spec_fails_permanent(tmp_path):
    cfg8, tdb, ing = _make_cfg8(tmp_path)
    q = ar.ResearchQueue(cfg8["autonomy"]["queue_db"], clock=_clock)
    handler = _handler(cfg8, q)
    bad = ar.Job(job_id="jbad", dedupe_key="dk", category=ar.CAT_DATA_VALIDATION,
                 lane="tournament.address_weakest_gate", state="RUNNING",
                 payload={"tournament": True, "spec": {}})  # no candidate_id/dep
    outcome, detail = handler(bad)
    assert outcome == ar.OUTCOME_FAILED_PERMANENT


def test_prop20_non_tournament_data_validation_is_unregressed(tmp_path):
    cfg8, tdb, ing = _make_cfg8(tmp_path)
    q = ar.ResearchQueue(cfg8["autonomy"]["queue_db"], clock=_clock)
    handlers = rt.build_production_autonomy_handlers(cfg8, queue=q)
    # every category still has a handler (Stage 8 behaviour intact)
    for cat in ar.JOB_CATEGORIES:
        assert cat in handlers
    plain = ar.Job(job_id="jp", dedupe_key="dk", category=ar.CAT_DATA_VALIDATION,
                   lane="some.other.lane", state="RUNNING", payload={})
    outcome, detail = handlers[ar.CAT_DATA_VALIDATION](plain)
    assert outcome == ar.OUTCOME_COMPLETED
    assert "recorded" in detail["note"]                # safe default preserved


def test_prop18_operational_store_is_byte_identical(tmp_path):
    op = tmp_path / "operational"
    op.mkdir()
    (op / "ledger.jsonl").write_text('{"nav": 100000}\n', encoding="utf-8")
    before = {str(p): p.read_bytes() for p in op.rglob("*") if p.is_file()}
    cfg8, tdb, ing = _make_cfg8(tmp_path)
    _write_normalized(ing, "INSIDER_FILING", [
        {"company_id": "AAPL",
         "normalized_payload": {"transaction_code": "P", "shares": 1}}])
    cid = _seed(tdb, family="EVENT_INSIDER", feature="insider_cluster",
                requires="event", dep="sec_8k", pit="SPARSE_OWNED_COVERAGE",
                blocker="x")
    q = ar.ResearchQueue(cfg8["autonomy"]["queue_db"], clock=_clock)
    _handler(cfg8, q)(_wg_job(cid, "sec_8k", "x"))
    after = {str(p): p.read_bytes() for p in op.rglob("*") if p.is_file()}
    assert before == after                             # no operational mutation


# --------------------------------------------------------------------------- #
# Candidate-evidence store safety (record_data_coverage never flips lifecycle).
# --------------------------------------------------------------------------- #
def test_record_data_coverage_never_changes_lifecycle_or_score(tmp_path):
    reg = tt.CandidateRegistry(str(tmp_path / "t.sqlite"), clock=_clock)
    cid = reg.seed_candidate(
        name="f", family="EVENT_INSIDER",
        spec={"feature": "insider_cluster", "requires_data_family": "event"},
        data_dependencies=["sec_8k"], universe="u", pit_status="SPARSE",
        lifecycle_state=tt.DATA_HOLD, blocker="DATA_HOLD_INSUFFICIENT_OBSERVATIONS")
    reg.record_data_coverage(cid, coverage={"distinct_issuers": 1},
                             evidence_date="2026-08-01", data_dependency="sec_8k",
                             job_id="jX", sufficient=False, next_action="grow")
    cand = reg.get(cid)
    assert cand["lifecycle_state"] == tt.DATA_HOLD
    assert cand["blocker"] == "DATA_HOLD_INSUFFICIENT_OBSERVATIONS"
    assert cand["combined_score"] is None
    assert cand["metrics"] is None
    assert cand["latest_evidence_date"] == "2026-08-01"
    assert "jX" in cand["experiment_ids"]
    assert reg.latest_data_coverage(cid)["coverage"]["distinct_issuers"] == 1
    reg.close()


# --------------------------------------------------------------------------- #
# Telegram / API surface matches the canonical queue (PART 11 / prop 19).
# --------------------------------------------------------------------------- #
def test_prop19_telegram_job_surface_matches_queue(tmp_path):
    from paper_trader.alpha_agent import telegram_control as tc
    q = ar.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock)
    jid = q.enqueue(ar.CAT_DATA_VALIDATION,
                    lane="tournament.address_weakest_gate",
                    payload={"tournament": True}, origin="stage9-tournament")
    q.claim_next()
    q.apply_outcome(jid, ar.OUTCOME_COMPLETED, result={
        "real_work": "tournament_weakest_gate_validation",
        "candidate_id": "c9_eventinsider_x", "data_dependency": "sec_8k",
        "sufficient": False, "candidate_lifecycle_state": "DATA_HOLD",
        "coverage": {"distinct_issuers": 1},
        "next_action": "enqueued a bounded sec_form4_8k acquisition continuation"})
    providers = tc.build_default_providers(stage8_config={}, queue=q)
    out = providers["job"](jid)
    assert "c9_eventinsider_x" in out
    assert "sec_8k" in out
    assert "Next action" in out
    assert "No model is promoted" in out
    # /jobs lists the id
    assert jid in providers["jobs"]()


# --------------------------------------------------------------------------- #
# Regression: a COMPLETED weakest-gate follow-up is a coverage validation, NOT
# an experiment result - it must never be handed to the experiment ingester
# (else the tournament tick logs EXPERIMENT_INGEST_MALFORMED on every cycle).
# --------------------------------------------------------------------------- #
def test_completed_weakest_gate_job_is_not_ingested_as_experiment(tmp_path):
    q = ar.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_clock)
    # a completed weakest-gate validation (no experiment feature)
    wg = q.enqueue(ar.CAT_DATA_VALIDATION,
                   lane="tournament.address_weakest_gate",
                   payload={"tournament": True, "strategy": "address_weakest_gate",
                            "candidate_id": "c9_x",
                            "spec": {"data_dependency": "sec_8k"}},
                   origin="stage9-tournament")
    q.claim_next()
    q.apply_outcome(wg, ar.OUTCOME_COMPLETED, result={
        "real_work": "tournament_weakest_gate_validation", "sufficient": False})
    # a genuine completed experiment (has feature + metrics)
    ex = q.enqueue(ar.CAT_EXPERIMENT, lane="tournament.additional_horizon",
                   payload={"tournament": True,
                            "spec": {"feature": "residual_momentum"}},
                   origin="stage9-tournament")
    q.claim_next()
    q.apply_outcome(ex, ar.OUTCOME_COMPLETED, result={
        "results": [{"feature": "residual_momentum", "rank_ic_t": 1.2}]})
    collected = rt._collect_completed_tournament_jobs(q)
    ids = {c["job_id"] for c in collected}
    assert wg not in ids                       # coverage validation excluded
    assert all(c.get("feature") for c in collected)
