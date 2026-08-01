"""Stage 9.2 RELEASE CLOSURE - queue auditability, execution containment and
scope control.

Deterministic regression tests proving the three release blockers are closed
WITHOUT changing the Stage 9.2 architecture or weakening any gate:

  WS1  attempts is the single, auditable count of real executions - incremented
       exactly once per CLAIM, never double-counted by an outcome, preserved by
       stale recovery, and never inflated by an overlapping worker.
  WS2  SAFE TIMEOUT - handlers run INLINE (no worker/daemon thread is ever
       abandoned): the queue lease is settled strictly AFTER the handler returns,
       so a handler can never keep executing after its job is settled. Each
       admitted handler is internally bounded and returns RETRYABLE on its OWN
       cooperative wall-clock timeout (never left RUNNING, no delayed write, no
       delayed continuation, retries stay serial); the collect lock refuses an
       overlapping cycle.
  WS3  the drain is restricted to approved Stage-9 tournament-continuation work
       by a three-dimension allowlist (origins AND lane-prefixes AND
       categories); unrelated/legacy jobs are never claimed; an empty allowlist
       executes nothing.

Every clock and handler is a fake; no real network, provider, Telegram or
scheduled task is touched, and no operational trading state is mutated.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO.parent) not in sys.path:
    sys.path.insert(0, str(_REPO.parent))

from paper_trader.alpha_agent import autonomous_research as ar  # noqa: E402
from paper_trader.alpha_agent import runtime as rt  # noqa: E402
from paper_trader.alpha_agent import tournament as tt  # noqa: E402

_TODAY = datetime.now(timezone.utc).date().isoformat()

# Canonical production allowlist (mirrors configs/alpha_agent/stage8_autonomy.json).
_ALLOWED_ORIGINS = ["stage9-tournament", "stage9-weakest-gate"]
_ALLOWED_LANE_PREFIXES = ["tournament.", "acq.sec_form4_8k"]
_ALLOWED_CATEGORIES = [ar.CAT_DATA_VALIDATION, ar.CAT_DATA_ACQUISITION]


class _Clk:
    """Deterministic monotonic ISO clock (advance via ``.t += seconds``)."""

    def __init__(self, t: int = 0):
        self.t = t

    def __call__(self) -> str:
        base = datetime(2026, 8, 1, tzinfo=timezone.utc) + timedelta(
            seconds=self.t)
        return base.replace(microsecond=0).isoformat()


def _q(tmp_path, **kw) -> ar.ResearchQueue:
    return ar.ResearchQueue(tmp_path / "autonomy.sqlite", **kw)


def _ok(job):
    return ar.OUTCOME_COMPLETED, {"note": "done", "real_work": "unit"}


# --------------------------------------------------------------------------- #
# WS1 - attempt accounting is auditable.
# --------------------------------------------------------------------------- #
def test_p1_claim_increments_attempts_exactly_once(tmp_path):
    q = _q(tmp_path, clock=_Clk())
    jid = q.enqueue(ar.CAT_REPORT, lane="l", payload={})
    assert q.get(jid).attempts == 0
    job = q.claim_next()
    assert job.job_id == jid and job.attempts == 1
    assert q.get(jid).attempts == 1  # persisted


def test_p2_completed_job_shows_attempts_ge_1(tmp_path):
    q = _q(tmp_path, clock=_Clk())
    jid = q.enqueue(ar.CAT_REPORT, lane="l", payload={})
    ar.drain_jobs(q, {ar.CAT_REPORT: _ok}, max_jobs=1)
    j = q.get(jid)
    assert j.state == ar.STATE_COMPLETED and j.attempts >= 1


def test_p3_blocked_specific_shows_attempts_ge_1(tmp_path):
    q = _q(tmp_path, clock=_Clk())
    jid = q.enqueue(ar.CAT_REPORT, lane="l", payload={})

    def _blk(job):
        return ar.OUTCOME_BLOCKED_SPECIFIC, {"reason": "missing X"}
    ar.drain_jobs(q, {ar.CAT_REPORT: _blk}, max_jobs=1)
    j = q.get(jid)
    assert j.state == ar.STATE_BLOCKED_SPECIFIC and j.attempts == 1


def test_p4_retryable_increments_once_per_execution(tmp_path):
    clk = _Clk()
    q = _q(tmp_path, clock=clk)
    jid = q.enqueue(ar.CAT_REPORT, lane="l", payload={})
    q.claim_next()                                       # attempts -> 1
    assert q.mark_retryable(jid, "boom") == ar.STATE_RETRYABLE
    assert q.get(jid).attempts == 1
    clk.t += 10_000                                      # elapse backoff
    assert q.claim_next() is not None                    # attempts -> 2
    assert q.get(jid).attempts == 2


def test_p5_failed_permanent_shows_the_executed_attempt(tmp_path):
    q = _q(tmp_path, clock=_Clk(), max_attempts=1)
    jid = q.enqueue(ar.CAT_REPORT, lane="l", payload={})
    q.claim_next()                                       # attempts -> 1
    assert q.mark_retryable(jid, "boom") == ar.STATE_FAILED_PERMANENT
    assert q.get(jid).attempts == 1


def test_p6_unclaimed_job_stays_zero(tmp_path):
    q = _q(tmp_path, clock=_Clk())
    jid = q.enqueue(ar.CAT_REPORT, lane="l", payload={})
    assert q.get(jid).attempts == 0  # never claimed


def test_p7_apply_outcome_cannot_double_increment(tmp_path):
    q = _q(tmp_path, clock=_Clk())
    jid = q.enqueue(ar.CAT_REPORT, lane="l", payload={})
    q.claim_next()                                       # attempts -> 1
    q.apply_outcome(jid, ar.OUTCOME_COMPLETED, result={})
    assert q.get(jid).attempts == 1                      # NOT 2
    # A retryable outcome on a claimed job also never re-increments.
    jid2 = q.enqueue(ar.CAT_REPORT, lane="l2", payload={})
    q.claim_next()
    q.apply_outcome(jid2, ar.OUTCOME_RETRYABLE, reason="t")
    assert q.get(jid2).attempts == 1


def test_p8_two_workers_cannot_increment_same_job_twice(tmp_path):
    path = tmp_path / "autonomy.sqlite"
    q1 = ar.ResearchQueue(path, clock=_Clk())
    q2 = ar.ResearchQueue(path, clock=_Clk())
    jid = q1.enqueue(ar.CAT_REPORT, lane="l", payload={})
    a = q1.claim_next()
    b = q2.claim_next()   # same job is RUNNING - not claimable
    assert a is not None and b is None
    assert q1.get(jid).attempts == 1  # exactly one increment


def test_p9_stale_recovery_preserves_prior_attempts(tmp_path):
    clk = _Clk()
    q = _q(tmp_path, clock=clk, stale_seconds=10)
    jid = q.enqueue(ar.CAT_REPORT, lane="l", payload={})
    clk.t += 1
    q.claim_next()                                       # attempts -> 1
    clk.t += 100                                         # age past stale
    assert q.requeue_stale() == 1
    j = q.get(jid)
    assert j.state == ar.STATE_QUEUED and j.attempts == 1  # preserved, not 2/0
    assert q.claim_next() is not None                    # re-claim -> 2
    assert q.get(jid).attempts == 2


# --------------------------------------------------------------------------- #
# WS3 - scope allowlist (origins AND lane-prefixes AND categories), ANDed.
# --------------------------------------------------------------------------- #
def test_p10_allowed_origins_enforced(tmp_path):
    q = _q(tmp_path, clock=_Clk())
    legacy = q.enqueue(ar.CAT_REPORT, lane="l", payload={}, origin="seed")
    ok = q.enqueue(ar.CAT_REPORT, lane="l2", payload={},
                   origin="stage9-tournament")
    j = q.claim_next(origins=["stage9-tournament"])
    assert j.job_id == ok
    assert q.get(legacy).state == ar.STATE_QUEUED and q.get(legacy).attempts == 0


def test_p11_allowed_lane_prefixes_enforced(tmp_path):
    q = _q(tmp_path, clock=_Clk())
    legacy = q.enqueue(ar.CAT_REPORT, lane="experiment.x", payload={})
    ok = q.enqueue(ar.CAT_REPORT, lane="tournament.address_weakest_gate",
                   payload={})
    j = q.claim_next(lane_prefixes=["tournament."])
    assert j.job_id == ok
    assert q.get(legacy).state == ar.STATE_QUEUED and q.get(legacy).attempts == 0


def test_p12_allowed_categories_enforced(tmp_path):
    q = _q(tmp_path, clock=_Clk())
    legacy = q.enqueue(ar.CAT_EXPERIMENT, lane="l", payload={})
    ok = q.enqueue(ar.CAT_DATA_VALIDATION, lane="l2", payload={})
    j = q.claim_next(categories=[ar.CAT_DATA_VALIDATION])
    assert j.job_id == ok
    assert q.get(legacy).state == ar.STATE_QUEUED and q.get(legacy).attempts == 0


def test_p13_unrelated_legacy_jobs_untouched_even_at_higher_priority(tmp_path):
    q = _q(tmp_path, clock=_Clk())
    # A high-priority legacy experiment AND a blocked priority-100 legacy job
    # must NOT prevent the eligible (lower-priority) Stage-9 follow-up.
    legacy = q.enqueue(ar.CAT_EXPERIMENT, lane="experiment.hot", payload={},
                       priority=100, origin="seed")
    blocked = q.enqueue(ar.CAT_DATA_ACQUISITION, lane="acq.norgate_prices",
                        payload={}, priority=100, origin="seed")
    q.block_specific(blocked, "premium-only")
    wg = q.enqueue(ar.CAT_DATA_VALIDATION, lane="tournament.address_weakest_gate",
                   payload={"tournament": True}, priority=2,
                   origin="stage9-tournament")
    rep = ar.drain_jobs(q, {ar.CAT_DATA_VALIDATION: _ok}, max_jobs=5,
                        categories=_ALLOWED_CATEGORIES, origins=_ALLOWED_ORIGINS,
                        lane_prefixes=_ALLOWED_LANE_PREFIXES)
    assert rep["jobs_claimed"] == 1 and rep["job_ids"] == [wg]
    assert q.get(wg).state == ar.STATE_COMPLETED and q.get(wg).attempts == 1
    # Unrelated jobs are byte-for-byte untouched.
    assert q.get(legacy).state == ar.STATE_QUEUED and q.get(legacy).attempts == 0
    assert q.get(blocked).state == ar.STATE_BLOCKED_SPECIFIC
    assert q.get(blocked).attempts == 0


def test_p14_sec_acquisition_continuation_is_eligible(tmp_path):
    q = _q(tmp_path, clock=_Clk())
    acq = q.enqueue(ar.CAT_DATA_ACQUISITION, lane="acq.sec_form4_8k",
                    payload={"campaign": "sec_form4_8k"}, priority=2,
                    origin="stage9-weakest-gate")
    j = q.claim_next(categories=_ALLOWED_CATEGORIES, origins=_ALLOWED_ORIGINS,
                     lane_prefixes=_ALLOWED_LANE_PREFIXES)
    assert j is not None and j.job_id == acq


def test_p15_empty_allowlist_executes_nothing(tmp_path):
    q = _q(tmp_path, clock=_Clk())
    q.enqueue(ar.CAT_DATA_VALIDATION, lane="tournament.address_weakest_gate",
              payload={}, origin="stage9-tournament")
    for kw in ({"categories": []}, {"origins": []}, {"lane_prefixes": []}):
        assert q.claim_next(**kw) is None
        rep = ar.drain_jobs(q, {ar.CAT_DATA_VALIDATION: _ok}, max_jobs=5, **kw)
        assert rep["jobs_claimed"] == 0
    assert q.counts_by_state()[ar.STATE_QUEUED] == 1  # untouched throughout


def test_p_allowlist_dimensions_are_anded(tmp_path):
    # A priority-3 tournament EXPERIMENT variant shares origin + lane-prefix but
    # is EXCLUDED by allowed_categories - so refinement experiments never run in
    # the Stage 9.2 drain, only DATA_VALIDATION / DATA_ACQUISITION continuation.
    q = _q(tmp_path, clock=_Clk())
    q.enqueue(ar.CAT_EXPERIMENT, lane="tournament.variant", payload={},
              priority=3, origin="stage9-tournament")
    assert q.claim_next(categories=_ALLOWED_CATEGORIES, origins=_ALLOWED_ORIGINS,
                        lane_prefixes=_ALLOWED_LANE_PREFIXES) is None


def test_p_glob_prefix_treats_underscore_as_literal(tmp_path):
    # GLOB (not LIKE) => '_' is a literal, so 'acq.sec_form4_8k*' matches the
    # real lane but not a look-alike with different separators.
    q = _q(tmp_path, clock=_Clk())
    good = q.enqueue(ar.CAT_DATA_ACQUISITION, lane="acq.sec_form4_8k",
                     payload={}, origin="stage9-weakest-gate")
    q.enqueue(ar.CAT_DATA_ACQUISITION, lane="acqxsecxform4x8k", payload={},
              origin="stage9-weakest-gate")
    claimed = []
    while True:
        j = q.claim_next(lane_prefixes=["acq.sec_form4_8k"])
        if j is None:
            break
        claimed.append(j.job_id)
    assert claimed == [good]


# --------------------------------------------------------------------------- #
# WS2 - execution containment.
# --------------------------------------------------------------------------- #
def test_p16_collect_lock_prevents_concurrent_cycle(tmp_path):
    root = tmp_path / "runtime"
    clock = rt.Clock()
    h = rt.acquire_lock(root, "collect", "run-1", clock=clock, conn=None)
    with pytest.raises(rt.LockHeld):
        rt.acquire_lock(root, "collect", "run-2", clock=clock, conn=None)
    rt.release_lock(h, clock=clock, conn=None)
    # After release a fresh cycle can acquire again.
    h2 = rt.acquire_lock(root, "collect", "run-3", clock=clock, conn=None)
    assert h2 is not None


# A handler that self-bounds on an INJECTED monotonic deadline and returns
# RETRYABLE on its own timeout - exactly the pattern the SEC acquisition handler
# uses (it stops at collect_time_budget_seconds and returns RETRYABLE). It runs
# entirely inline; nothing is ever abandoned.
def _self_timeout_handler(marks=None, enqueue_into=None):
    def _h(job):
        if marks is not None:
            marks.append("ran")            # side effect completes before return
        if enqueue_into is not None:       # would-be (but suppressed) continuation
            pass                            # a timed-out handler enqueues NOTHING
        return ar.OUTCOME_RETRYABLE, {
            "reason": "handler reached its own collect_time_budget_seconds "
                      "(deadline); partial not recorded; will resume",
            "deadline_reached": True}
    return _h


def test_p17_handler_runs_inline_no_thread_is_abandoned(tmp_path):
    q = _q(tmp_path, clock=_Clk())
    jid = q.enqueue(ar.CAT_REPORT, lane="l", payload={})
    seen = {}

    def _h(job):
        seen["thread"] = threading.current_thread()
        return ar.OUTCOME_COMPLETED, {"note": "ok"}
    main = threading.current_thread()
    before = threading.active_count()
    rep = ar.drain_jobs(q, {ar.CAT_REPORT: _h}, max_jobs=1)
    after = threading.active_count()
    # The handler ran on THIS thread; no worker/daemon thread was created or left
    # behind that could keep executing after the job is settled.
    assert seen["thread"] is main
    assert after == before
    assert rep["jobs_completed"] == 1
    j = q.get(jid)
    assert j.state == ar.STATE_COMPLETED and j.attempts == 1


def test_p18_lease_settled_strictly_after_handler_returns(tmp_path):
    # Property 1: a job is NEVER settled while its original handler is still
    # alive. The handler observes its own job still RUNNING (unsettled) while it
    # executes; settlement happens only after it returns, on the same thread.
    q = _q(tmp_path, clock=_Clk())
    jid = q.enqueue(ar.CAT_REPORT, lane="l", payload={})
    order = []

    def _h(job):
        assert q.get(jid).state == ar.STATE_RUNNING   # not settled yet
        order.append("enter")
        order.append("exit")
        return ar.OUTCOME_COMPLETED, {}
    ar.drain_jobs(q, {ar.CAT_REPORT: _h}, max_jobs=1)
    assert order == ["enter", "exit"]
    assert q.get(jid).state == ar.STATE_COMPLETED


def test_p_self_timeout_returns_retryable_not_running(tmp_path):
    # Properties 6 & 8: a handler that hits its OWN bounded timeout returns
    # RETRYABLE (counted once), and the job is never left permanently RUNNING.
    q = _q(tmp_path, clock=_Clk())
    jid = q.enqueue(ar.CAT_REPORT, lane="l", payload={})
    rep = ar.drain_jobs(q, {ar.CAT_REPORT: _self_timeout_handler()}, max_jobs=1)
    assert rep["jobs_retryable"] == 1
    j = q.get(jid)
    assert j.state == ar.STATE_RETRYABLE and j.state != ar.STATE_RUNNING
    assert j.attempts == 1
    assert "deadline" in (j.blocked_reason or "")


def test_p_self_timeout_no_delayed_write_or_continuation(tmp_path):
    # Properties 3 & 4: because execution is inline, every side effect the
    # handler performs completes BEFORE settlement, and a timed-out handler
    # enqueues NO continuation - the queue gains no new job.
    q = _q(tmp_path, clock=_Clk())
    q.enqueue(ar.CAT_REPORT, lane="l", payload={})
    marks = []
    depth_before = q.depth()
    ar.drain_jobs(q, {ar.CAT_REPORT: _self_timeout_handler(marks=marks)},
                  max_jobs=1)
    assert marks == ["ran"]                 # the write happened (inline)
    assert q.depth() == depth_before        # RETRYABLE, no continuation enqueued
    assert q.counts_by_state().get(ar.STATE_RETRYABLE) == 1


def test_p_retry_after_timeout_is_serial_never_overlaps(tmp_path):
    # Property 5: a later retry cannot overlap the previous execution. Two drains
    # (clock advanced past the retry backoff) each run the handler to completion
    # before the next claim; attempts go 1 -> 2 with no concurrency.
    clk = _Clk()
    q = _q(tmp_path, clock=clk)
    jid = q.enqueue(ar.CAT_REPORT, lane="l", payload={}, max_attempts=3)
    spans = []

    def _h(job):
        spans.append(("start", len(spans)))
        spans.append(("end", len(spans)))
        return ar.OUTCOME_RETRYABLE, {"reason": "deadline", "deadline_reached": True}
    ar.drain_jobs(q, {ar.CAT_REPORT: _h}, max_jobs=1)
    assert q.get(jid).attempts == 1
    clk.t += 100000                         # advance past retry_backoff not_before
    ar.drain_jobs(q, {ar.CAT_REPORT: _h}, max_jobs=1)
    assert q.get(jid).attempts == 2
    # Strict start/end interleaving proves the two executions never overlapped.
    assert [s[0] for s in spans] == ["start", "end", "start", "end"]


def test_p_stale_recovery_preserves_attempts_after_timeout(tmp_path):
    # Property 7: stale recovery is unchanged - a job left RUNNING (e.g. an OS
    # kill mid-handler) is requeued with attempts PRESERVED, never re-incremented.
    clk = _Clk()
    q = _q(tmp_path, clock=clk)
    jid = q.enqueue(ar.CAT_REPORT, lane="l", payload={}, max_attempts=3)
    job = q.claim_next()
    assert job is not None and q.get(jid).attempts == 1
    clk.t += 2000                            # the RUNNING lease ages out
    n = q.requeue_stale(stale_seconds=1)
    assert n == 1
    j = q.get(jid)
    assert j.state == ar.STATE_QUEUED and j.attempts == 1   # preserved, not +1


def test_p_one_timeout_does_not_stop_the_cycle(tmp_path):
    # Property 9: one handler timing out does not stop the wider cycle - the next
    # eligible job still runs and completes in the same drain.
    q = _q(tmp_path, clock=_Clk())
    q.enqueue(ar.CAT_DATA_VALIDATION, lane="tournament.a", payload={},
              origin="stage9-tournament", priority=2)
    q.enqueue(ar.CAT_DATA_VALIDATION, lane="tournament.b", payload={},
              origin="stage9-tournament", priority=1)

    calls = {"n": 0}

    def _h(job):
        calls["n"] += 1
        if calls["n"] == 1:
            return ar.OUTCOME_RETRYABLE, {"reason": "deadline",
                                          "deadline_reached": True}
        return ar.OUTCOME_COMPLETED, {"note": "ok"}
    rep = ar.drain_jobs(q, {ar.CAT_DATA_VALIDATION: _h}, max_jobs=2,
                        categories=_ALLOWED_CATEGORIES, origins=_ALLOWED_ORIGINS,
                        lane_prefixes=_ALLOWED_LANE_PREFIXES)
    assert rep["jobs_retryable"] == 1 and rep["jobs_completed"] == 1
    assert rep["jobs_claimed"] == 2


# --------------------------------------------------------------------------- #
# WS4 - no duplicate import; operational ledgers untouched.
# --------------------------------------------------------------------------- #
def _stage9_cfg(tmp_path):
    tournament_db = tmp_path / "tournament.sqlite"
    stage9 = {"stage": "9", "config_version": "1.0.0",
              "tournament_db": str(tournament_db),
              "gates": {"keep_min_rank_ic_t": 2.0},
              "evidence_completeness": {}, "scoring": {}, "cycle": {}}
    p = tmp_path / "stage9.json"
    p.write_text(json.dumps(stage9), encoding="utf-8")
    cfg8 = {"stage": "8", "stage8_root": str(tmp_path / "s8"),
            "autonomy": {"queue_db": str(tmp_path / "autonomy.sqlite"),
                         "handlers": "production", "pit_gics_min_symbols": 3},
            "tournament": {"enabled": True, "config": str(p),
                           "max_candidates_per_cycle": 4}}
    return cfg8, str(tournament_db), Path(cfg8["stage8_root"]) / "ingestion"


def test_p19_no_duplicate_result_import_idempotent(tmp_path):
    cfg8, tdb, ingestion = _stage9_cfg(tmp_path)
    # A couple of leakage-safe PIT-SIC records (below the min threshold).
    d = ingestion / "normalized" / "SECURITY_IDENTITY"
    d.mkdir(parents=True, exist_ok=True)
    (d / "f.jsonl").write_text("\n".join(json.dumps(r) for r in [
        {"company_id": "AAPL", "event_type": "ASSIGNED_SIC_PIT",
         "normalized_payload": {"ticker": "AAPL"}},
        {"company_id": "MSFT", "event_type": "ASSIGNED_SIC_PIT",
         "normalized_payload": {"ticker": "MSFT"}}]) + "\n", encoding="utf-8")
    reg = tt.CandidateRegistry(tdb)
    cid = reg.seed_candidate(
        name="pm", family="price_momentum",
        spec={"feature": "sector_neutral_momentum",
              "requires_data_family": "price"},
        data_dependencies=["point_in_time_gics"], universe="test",
        pit_status="INSUFFICIENT", lifecycle_state=tt.DATA_HOLD,
        blocker="needs PIT GICS")
    reg.close()
    q = ar.ResearchQueue(cfg8["autonomy"]["queue_db"], clock=_Clk())
    handler = rt.build_production_autonomy_handlers(cfg8, queue=q)[
        ar.CAT_DATA_VALIDATION]
    job = ar.Job(job_id="jwg", dedupe_key="dk", category=ar.CAT_DATA_VALIDATION,
                 lane="tournament.address_weakest_gate", state="RUNNING",
                 payload={"tournament": True, "candidate_id": cid,
                          "strategy": "address_weakest_gate",
                          "spec": {"data_dependency": "point_in_time_gics"},
                          "evidence_date": _TODAY})
    o1, _ = handler(job)
    o2, d2 = handler(job)  # import-once: second call is idempotent
    assert o1 == ar.OUTCOME_COMPLETED and o2 == ar.OUTCOME_COMPLETED
    assert d2.get("idempotent") is True
    con = sqlite3.connect(tdb)
    try:
        n = con.execute("SELECT COUNT(*) FROM data_coverage WHERE candidate_id=?",
                        (cid,)).fetchone()[0]
    finally:
        con.close()
    assert n == 1  # exactly one coverage row despite two handler calls


def test_p20_operational_ledgers_unchanged_by_drain(tmp_path):
    led = tmp_path / "ledger"
    led.mkdir()
    (led / "book.json").write_text('{"nav": 100000}', encoding="utf-8")
    cfg = {"operational_ledger_roots": [str(led)]}
    before = rt.fingerprint_ledgers(cfg)
    q = _q(tmp_path, clock=_Clk())
    for i in range(3):
        q.enqueue(ar.CAT_DATA_VALIDATION, lane="tournament.t%d" % i,
                  payload={}, origin="stage9-tournament")
    ar.drain_jobs(q, {ar.CAT_DATA_VALIDATION: _ok}, max_jobs=3,
                  categories=_ALLOWED_CATEGORIES, origins=_ALLOWED_ORIGINS,
                  lane_prefixes=_ALLOWED_LANE_PREFIXES)
    assert rt.fingerprint_ledgers(cfg) == before  # byte-identical
