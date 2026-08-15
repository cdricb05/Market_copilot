"""
tests/test_alpha_agent_stage8_autonomy.py - Alpha Agent Stage 8.

Deterministic coverage of the autonomous data-exhaustion + never-idle research
queue + secure Telegram control plane. Every clock, HTTP client and provider is
a FAKE - no real network, Telegram, provider credential or scheduled task is
ever touched, and no operational trading state is mutated.

Proves the Stage 8 contract:
  * every purchased + free source is represented in the source registry;
  * entitlement probes are independent (one provider failure isolates);
  * the durable queue persists across restart and auto-replenishes;
  * BLOCKED_SPECIFIC jobs never block unrelated jobs; stale jobs are requeued;
  * a sent report never terminates research; premium-blocked data never stops
    other experiments;
  * point-in-time boundaries are enforced and no look-ahead is introduced;
  * provider + Telegram credentials are never exposed;
  * only the allowed Telegram user/chat is accepted; arbitrary commands are
    rejected; Telegram cannot mutate trading state; natural-language requests
    create BOUNDED research jobs; duplicate updates/jobs are idempotent;
  * the honest data-completeness contract forbids an unqualified NO_ALPHA;
  * Gmail SMTP + the existing observatory contract remain compatible.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO.parent) not in sys.path:
    sys.path.insert(0, str(_REPO.parent))

from paper_trader.alpha_agent import autonomous_research as ar  # noqa: E402
from paper_trader.alpha_agent import evidence_observatory as eo  # noqa: E402
from paper_trader.alpha_agent import report_renderer as rr  # noqa: E402
from paper_trader.alpha_agent import runtime as rt  # noqa: E402
from paper_trader.alpha_agent import runtime_contracts as rc  # noqa: E402
from paper_trader.alpha_agent import source_exhaustion as se  # noqa: E402
from paper_trader.alpha_agent import telegram_control as tc  # noqa: E402

_STAGE8_CFG = _REPO / "configs" / "alpha_agent" / "stage8_autonomy.json"


class Clock:
    """Deterministic monotonic ISO clock for reproducible queue timestamps."""

    def __init__(self, start_s: int = 0):
        self.t = start_s

    def __call__(self) -> str:
        base = datetime(2026, 7, 30, tzinfo=timezone.utc) + timedelta(
            seconds=self.t)
        return base.replace(microsecond=0).isoformat()


def _q(tmp_path, name="autonomy.sqlite", **kw) -> ar.ResearchQueue:
    return ar.ResearchQueue(tmp_path / name, **kw)


def _planner():
    return se.make_planner(lambda: se.build_registry_snapshot())


def _settle(q: ar.ResearchQueue, job_id: str) -> str:
    """Drive the only live job to a TERMINAL state.

    The live-dedupe index deliberately stops seeing a settled row, which is
    precisely when re-adding the same dedupe_key is legitimate new work."""
    q.claim_next()
    return q.apply_outcome(job_id, ar.OUTCOME_COMPLETED, reason="done")


# --------------------------------------------------------------------------- #
# Durable queue: idempotency, persistence, replenishment, isolation.
# --------------------------------------------------------------------------- #
class TestDurableQueue:
    def test_categories_and_states_are_the_stage8_contract(self):
        assert len(ar.JOB_CATEGORIES) == 12
        assert len(ar.JOB_STATES) == 7
        for c in ("SOURCE_DISCOVERY", "ENTITLEMENT_PROBE", "DATA_ACQUISITION",
                  "COVERAGE_REPAIR", "DATA_VALIDATION", "HYPOTHESIS_GENERATION",
                  "EXPERIMENT", "ROBUSTNESS_TEST", "SIGNAL_COMBINATION",
                  "PROSPECTIVE_SNAPSHOT", "REPORT", "TELEGRAM_REQUEST"):
            assert c in ar.JOB_CATEGORIES
        for s in ("QUEUED", "RUNNING", "RETRYABLE", "BLOCKED_SPECIFIC",
                  "COMPLETED", "REJECTED", "FAILED_PERMANENT"):
            assert s in ar.JOB_STATES

    def test_enqueue_is_idempotent_on_identity(self, tmp_path):
        q = _q(tmp_path, clock=Clock())
        a = q.enqueue(ar.CAT_EXPERIMENT, lane="x", payload={"f": 1})
        b = q.enqueue(ar.CAT_EXPERIMENT, lane="x", payload={"f": 1})
        assert a == b and q.depth() == 1

    def test_distinct_payloads_are_distinct_jobs(self, tmp_path):
        q = _q(tmp_path, clock=Clock())
        q.enqueue(ar.CAT_EXPERIMENT, lane="x", payload={"f": 1})
        q.enqueue(ar.CAT_EXPERIMENT, lane="x", payload={"f": 2})
        assert q.depth() == 2

    def test_queue_persists_across_restart(self, tmp_path):
        q = _q(tmp_path, clock=Clock())
        jid = q.enqueue(ar.CAT_DATA_ACQUISITION, lane="eodhd.eod", payload={})
        del q
        q2 = _q(tmp_path, clock=Clock())  # reopen same file
        assert q2.depth() == 1
        assert q2.get(jid) is not None and q2.get(jid).state == ar.STATE_QUEUED

    def test_claim_is_atomic_and_marks_running(self, tmp_path):
        q = _q(tmp_path, clock=Clock())
        q.enqueue(ar.CAT_EXPERIMENT, lane="x", payload={})
        job = q.claim_next()
        assert job is not None and job.state == ar.STATE_RUNNING
        # a second claim finds nothing runnable (the only job is RUNNING)
        assert q.claim_next() is None

    def test_priority_orders_claims(self, tmp_path):
        q = _q(tmp_path, clock=Clock())
        q.enqueue(ar.CAT_EXPERIMENT, lane="low", payload={}, priority=1)
        q.enqueue(ar.CAT_EXPERIMENT, lane="high", payload={}, priority=9)
        assert q.claim_next().lane == "high"

    def test_blocked_specific_does_not_block_unrelated(self, tmp_path):
        clk = Clock()
        q = _q(tmp_path, clock=clk)
        blocked = q.enqueue(ar.CAT_DATA_ACQUISITION, lane="eodhd.earnings",
                            payload={})
        q.enqueue(ar.CAT_EXPERIMENT, lane="price.mom", payload={})
        q.block_specific(blocked, "premium-only")
        clk.t += 1
        job = q.claim_next()
        assert job is not None and job.job_id != blocked
        # the blocked job stays parked and is never claimed
        assert q.get(blocked).state == ar.STATE_BLOCKED_SPECIFIC

    def test_retry_is_bounded_then_failed_permanent(self, tmp_path):
        # Stage 9.2 contract: ``attempts`` is incremented once per EXECUTION at
        # claim time, so the bounded retry budget is consumed by claim ->
        # mark_retryable cycles (not by mark_retryable alone). max_attempts=2
        # means two executions, the second escalating to FAILED_PERMANENT.
        clk = Clock()
        q = _q(tmp_path, clock=clk, max_attempts=2)
        jid = q.enqueue(ar.CAT_EXPERIMENT, lane="x", payload={})
        clk.t += 1
        assert q.claim_next().job_id == jid                 # attempts -> 1
        assert q.mark_retryable(jid, "boom") == ar.STATE_RETRYABLE
        clk.t += 1000                                       # elapse backoff
        assert q.claim_next() is not None                   # re-claim -> 2
        assert q.mark_retryable(jid, "boom") == ar.STATE_FAILED_PERMANENT
        assert q.get(jid).attempts == 2                     # two executions

    def test_stale_running_is_safely_requeued(self, tmp_path):
        clk = Clock()
        q = _q(tmp_path, clock=clk, stale_seconds=10)
        q.enqueue(ar.CAT_EXPERIMENT, lane="x", payload={})
        clk.t += 1
        q.claim_next()          # -> RUNNING
        clk.t += 100            # age past stale
        assert q.requeue_stale() == 1
        assert q.counts_by_state()[ar.STATE_QUEUED] == 1

    def test_never_idle_replenishes_from_empty(self, tmp_path):
        q = _q(tmp_path, clock=Clock())
        assert q.depth() == 0
        rep = ar.ensure_never_idle(q, planner=_planner())
        assert rep["added"] >= 1 and q.depth() >= 1

    def test_never_idle_with_minimal_fallback_when_planner_empty(self, tmp_path):
        q = _q(tmp_path, clock=Clock())
        added = ar.replenish(q, planner=lambda _q: [], floor=1)
        assert added >= 1 and q.depth() >= 1  # fallback guarantees work

    def test_apply_outcome_routes_all_tokens(self, tmp_path):
        q = _q(tmp_path, clock=Clock())
        for outcome, expect in (
                (ar.OUTCOME_COMPLETED, ar.STATE_COMPLETED),
                (ar.OUTCOME_REJECTED, ar.STATE_REJECTED),
                (ar.OUTCOME_BLOCKED_SPECIFIC, ar.STATE_BLOCKED_SPECIFIC),
                (ar.OUTCOME_FAILED_PERMANENT, ar.STATE_FAILED_PERMANENT)):
            jid = q.enqueue(ar.CAT_EXPERIMENT, lane="l" + expect,
                            payload={"o": outcome})
            q.claim_next()
            assert q.apply_outcome(jid, outcome, reason="r") == expect


# --------------------------------------------------------------------------- #
# Job identity: one (dedupe_key, created_at second) holds UNBOUNDED re-adds.
#
# ``created_at`` has one-second resolution and the live-dedupe index only covers
# non-terminal states, so a SETTLED key is legitimately re-enqueued - repeatedly
# and within one second, because the never-idle floor re-adds two CONSTANT specs
# every time the queue drains. The queue previously had exactly two identities
# per pair (primary + fallback), so the THIRD same-second re-add collided on the
# ``jobs.job_id`` primary key and raised ``sqlite3.IntegrityError`` out of
# enqueue -> replenish -> ensure_never_idle -> run_cycle.
# --------------------------------------------------------------------------- #
class TestJobIdCollisionSequence:
    _PAYLOAD = {"f": 1}

    def _dk(self, payload=None) -> str:
        return ar.make_dedupe_key(ar.CAT_EXPERIMENT, "x",
                                  self._PAYLOAD if payload is None else payload)

    def test_first_add_keeps_the_original_primary_id(self, tmp_path):
        clk = Clock()
        q = _q(tmp_path, clock=clk)
        jid = q.enqueue(ar.CAT_EXPERIMENT, lane="x", payload=self._PAYLOAD)
        assert jid == ar.make_job_id(self._dk(), clk())

    def test_second_same_second_readd_keeps_the_legacy_fallback_id(self, tmp_path):
        clk = Clock()
        q = _q(tmp_path, clock=clk)
        first = q.enqueue(ar.CAT_EXPERIMENT, lane="x", payload=self._PAYLOAD)
        _settle(q, first)
        second = q.enqueue(ar.CAT_EXPERIMENT, lane="x", payload=self._PAYLOAD)
        assert first == ar.make_job_id(self._dk(), clk())
        assert second == ar.make_fallback_job_id(self._dk(), clk(), 0)

    def test_third_same_second_readd_succeeds_on_the_sequence(self, tmp_path):
        clk = Clock()
        q = _q(tmp_path, clock=clk)
        ids = []
        for _ in range(3):
            jid = q.enqueue(ar.CAT_EXPERIMENT, lane="x", payload=self._PAYLOAD)
            ids.append(jid)
            _settle(q, jid)
        assert ids[0] == ar.make_job_id(self._dk(), clk())
        assert ids[1] == ar.make_fallback_job_id(self._dk(), clk(), 0)
        assert ids[2] == ar.make_sequenced_job_id(self._dk(), clk(), 0, 2)
        assert len(set(ids)) == 3

    def test_many_same_second_readds_are_unique_and_deterministic(self, tmp_path):
        def run(name):
            q = _q(tmp_path, name=name, clock=Clock())
            out = []
            for _ in range(50):
                jid = q.enqueue(ar.CAT_EXPERIMENT, lane="x",
                                payload=self._PAYLOAD)
                out.append(jid)
                _settle(q, jid)
            return out

        ids = run("many_a.sqlite")
        assert len(set(ids)) == 50          # every identity distinct
        assert ids == run("many_b.sqlite")  # identical replay, no randomness

    def test_live_duplicate_still_dedupes_after_settled_history(self, tmp_path):
        q = _q(tmp_path, clock=Clock())
        for _ in range(3):
            _settle(q, q.enqueue(ar.CAT_EXPERIMENT, lane="x",
                                 payload=self._PAYLOAD))
        live = q.enqueue(ar.CAT_EXPERIMENT, lane="x", payload=self._PAYLOAD)
        again = q.enqueue(ar.CAT_EXPERIMENT, lane="x", payload=self._PAYLOAD)
        assert again == live and q.depth() == 1   # no duplicate LIVE work

    def test_separate_dedupe_keys_stay_independent(self, tmp_path):
        clk = Clock()
        q = _q(tmp_path, clock=clk)
        ids = []
        for payload in ({"f": 1}, {"f": 2}, {"f": 1}, {"f": 2}, {"f": 1}):
            jid = q.enqueue(ar.CAT_EXPERIMENT, lane="x", payload=payload)
            ids.append(jid)
            _settle(q, jid)
        one, two = self._dk({"f": 1}), self._dk({"f": 2})
        # each key walks its OWN sequence; neither consumes the other's slots
        assert ids[0] == ar.make_job_id(one, clk())
        assert ids[1] == ar.make_job_id(two, clk())
        assert ids[2] == ar.make_fallback_job_id(one, clk(), 0)
        assert ids[3] == ar.make_fallback_job_id(two, clk(), 0)
        assert ids[4] == ar.make_sequenced_job_id(one, clk(), 0, 2)
        assert len(set(ids)) == 5

    def test_persisted_job_ids_remain_unique(self, tmp_path):
        q = _q(tmp_path, clock=Clock())
        for _ in range(25):
            _settle(q, q.enqueue(ar.CAT_EXPERIMENT, lane="x",
                                 payload=self._PAYLOAD))
        conn = sqlite3.connect(str(tmp_path / "autonomy.sqlite"))
        try:
            total, distinct = conn.execute(
                "SELECT COUNT(job_id), COUNT(DISTINCT job_id) FROM jobs"
            ).fetchone()
        finally:
            conn.close()
        assert total == 25 and distinct == 25   # full audit trail, no overwrite

    def test_never_idle_floor_survives_a_long_same_second_idle_stretch(
            self, tmp_path):
        """The production path that used to crash: a frozen-second clock plus a
        planner with nothing to propose, so the floor re-adds its two constant
        specs cycle after cycle inside one ``created_at`` second."""
        q = _q(tmp_path, clock=Clock())
        handlers = {c: (lambda job: (ar.OUTCOME_COMPLETED, {}))
                    for c in ar.JOB_CATEGORIES}
        for _ in range(40):
            ar.run_cycle(q, handlers, planner=lambda _q: [], max_jobs=1,
                         floor=1)
        assert q.depth() >= 1   # never idle, and never an IntegrityError


# --------------------------------------------------------------------------- #
# Bounded cycle + never-stop.
# --------------------------------------------------------------------------- #
class TestCycleNeverStop:
    def test_cycle_processes_and_stays_non_empty(self, tmp_path):
        q = _q(tmp_path, clock=Clock())
        handlers = {c: (lambda job: (ar.OUTCOME_COMPLETED, {"ok": True}))
                    for c in ar.JOB_CATEGORIES}
        summ = ar.run_cycle(q, handlers, planner=_planner(), max_jobs=5)
        assert summ["processed"] >= 1
        assert summ["depth"] >= 1  # replenished after draining

    def test_report_job_does_not_terminate_research(self, tmp_path):
        q = _q(tmp_path, clock=Clock())
        q.enqueue(ar.CAT_REPORT, lane="report.daily", payload={})
        handlers = {c: (lambda job: (ar.OUTCOME_COMPLETED, {}))
                    for c in ar.JOB_CATEGORIES}
        summ = ar.run_cycle(q, handlers, planner=_planner(), max_jobs=1)
        # after a report completes, useful work still exists (never terminal)
        assert summ["depth"] >= 1

    def test_handler_exception_is_bounded_not_fatal(self, tmp_path):
        q = _q(tmp_path, clock=Clock())
        q.enqueue(ar.CAT_EXPERIMENT, lane="x", payload={})

        def boom(job):
            raise RuntimeError("handler blew up")
        handlers = {c: boom for c in ar.JOB_CATEGORIES}
        summ = ar.run_cycle(q, handlers, planner=lambda _q: [], max_jobs=1)
        assert summ["handler_errors"] >= 1  # loop survived

    def test_missing_handler_parks_job_without_stopping_cycle(self, tmp_path):
        q = _q(tmp_path, clock=Clock())
        q.enqueue(ar.CAT_EXPERIMENT, lane="x", payload={})
        summ = ar.run_cycle(q, {}, planner=lambda _q: [], max_jobs=1)
        assert ar.CAT_EXPERIMENT in summ["missing_handler"]

    def test_premium_blocked_data_does_not_stop_other_experiments(self, tmp_path):
        q = _q(tmp_path, clock=Clock())
        q.enqueue(ar.CAT_DATA_ACQUISITION, lane="premium.revisions", payload={})
        q.enqueue(ar.CAT_EXPERIMENT, lane="price.mom", payload={})

        def handler(job):
            if job.lane.startswith("premium"):
                return ar.OUTCOME_BLOCKED_SPECIFIC, {"reason": "PAID_NOT_OWNED"}
            return ar.OUTCOME_COMPLETED, {"ran": True}
        handlers = {c: handler for c in ar.JOB_CATEGORIES}
        summ = ar.run_cycle(q, handlers, planner=lambda _q: [], max_jobs=5)
        states = [h["state"] for h in summ["handled"]]
        assert ar.STATE_COMPLETED in states  # the non-premium experiment ran
        assert ar.STATE_BLOCKED_SPECIFIC in states


# --------------------------------------------------------------------------- #
# Watchdog.
# --------------------------------------------------------------------------- #
class TestWatchdog:
    def test_watchdog_requeues_stale_and_reports_progress(self, tmp_path):
        clk = Clock()
        q = _q(tmp_path, clock=clk, stale_seconds=10)
        q.enqueue(ar.CAT_EXPERIMENT, lane="x", payload={})
        clk.t += 1
        q.claim_next()
        clk.t += 100
        rep = ar.watchdog_scan(q, planner=_planner())
        assert rep["stale_requeued"] >= 1 and rep["hard_blocker"] is False

    def test_watchdog_keeps_queue_non_empty(self, tmp_path):
        q = _q(tmp_path, clock=Clock())
        rep = ar.watchdog_scan(q, planner=_planner())
        assert rep["queue_depth"] >= 1 and rep["never_idle"] is True


# --------------------------------------------------------------------------- #
# Source registry + probes + coverage + completeness.
# --------------------------------------------------------------------------- #
class TestSourceExhaustion:
    def test_registry_covers_owned_and_free_sources(self):
        cat = se.source_catalog()
        providers = {e["provider"] for e in cat}
        for owned in ("Norgate Data", "EODHD"):
            assert owned in providers
        for free in ("SEC EDGAR", "FRED / ALFRED", "FINRA"):
            assert free in providers
        # every entry carries the full metadata contract
        for e in cat:
            assert set(e) == set(se.REGISTRY_ENTRY_FIELDS)

    def test_classifications_are_the_contract(self):
        # WS1 added PROSPECTIVE_COLLECTION_ACTIVE (a prospective family whose
        # production forward-snapshot collector is wired and operational).
        assert len(se.CLASSIFICATIONS) == 9
        for c in ("ACCESSIBLE_NOW", "ACCESSIBLE_AFTER_REPAIR", "PROSPECTIVE_ONLY",
                  "PROSPECTIVE_COLLECTION_ACTIVE", "PAID_NOT_OWNED",
                  "LEGALLY_RESTRICTED", "INVALID_CREDENTIAL",
                  "PROVIDER_OUTAGE", "NOT_RELEVANT"):
            assert c in se.CLASSIFICATIONS

    def test_health_maps_to_classifications(self):
        import paper_trader.alpha_agent.source_contracts as sc
        assert se.classify_from_health(sc.SH_HEALTHY) == se.ACCESSIBLE_NOW
        assert se.classify_from_health(
            None, entitlement_states=[sc.ENT_NOT_ENTITLED]) == se.PAID_NOT_OWNED
        assert se.classify_from_health(
            sc.SH_BLOCKED_CREDENTIAL) == se.INVALID_CREDENTIAL
        assert se.classify_from_health(sc.SH_FAILED) == se.PROVIDER_OUTAGE

    def test_entitlement_probes_are_independent(self):
        # one provider ENTITLED, one AUTH-failed, one NOT-entitled -> three
        # distinct classifications; no provider's verdict changes another's.
        import paper_trader.alpha_agent.source_contracts as sc
        probe = {"status": "OK", "health": {
            "eodhd": {"overall_state": sc.SH_HEALTHY,
                      "entitlement_states": [sc.ENT_ENTITLED]},
            "sec_edgar": {"overall_state": sc.SH_BLOCKED_CREDENTIAL,
                          "entitlement_states": []},
            "fred_alfred": {"overall_state": sc.SH_HEALTHY,
                            "entitlement_states": [sc.ENT_NOT_ENTITLED]},
        }}
        snap = se.build_registry_snapshot(probe=probe)
        by_sid = {e["collector_source_id"]: e for e in snap["sources"]
                  if e["collector_source_id"]}
        assert by_sid["eodhd"]["classification"] == se.ACCESSIBLE_NOW
        assert by_sid["sec_edgar"]["classification"] == se.INVALID_CREDENTIAL
        assert by_sid["fred_alfred"]["classification"] == se.PAID_NOT_OWNED

    def test_probe_error_never_raises(self):
        # a broken config must not crash the probe (isolation).
        out = se.probe_sources({"sources": {}}, output_root=None)
        assert isinstance(out, dict) and "status" in out

    def test_coverage_matrix(self):
        recs = [{"ticker": "AAPL", "field": "eod", "date": "2026-07-30"},
                {"ticker": "MSFT", "field": "eod", "date": "2026-07-30"},
                {"ticker": "AAPL", "field": "fundamentals", "date": "2026-06-30"}]
        m = se.coverage_matrix(recs)
        assert m["by_symbol"]["AAPL"] == 2 and m["field_count"] == 2

    def test_completeness_rejects_unqualified_no_alpha(self):
        with pytest.raises(ValueError):
            se.assert_no_unqualified_no_alpha("verdict: NO_ALPHA")
        # a graded qualifier is allowed
        se.assert_no_unqualified_no_alpha(
            "NO_ALPHA but PROVISIONAL_COVERAGE_INCOMPLETE")

    def test_completeness_requires_full_evidence(self):
        out = se.data_completeness_conclusion({"fields_acquired": []})
        assert out["reconciles"] is False and out["missing_evidence"]

    def test_completeness_grades_levels(self):
        full = {k: 0 for k in se.COMPLETENESS_EVIDENCE_KEYS}
        full.update({"historical_coverage": 0.9, "universe_coverage": 0.9,
                     "experiment_sample_size": 40,
                     "inaccessible_fields_could_change_conclusion": False})
        assert se.data_completeness_conclusion(full)["level"] in \
            se.CONCLUSION_LEVELS
        prem = dict(full)
        prem.update({"inaccessible_fields_could_change_conclusion": True,
                     "material_field_is_premium": True})
        assert se.data_completeness_conclusion(prem)["level"] == \
            se.PREMIUM_DATA_MATERIAL


# --------------------------------------------------------------------------- #
# Point-in-time / no look-ahead.
# --------------------------------------------------------------------------- #
class TestPointInTime:
    def test_after_close_filing_effective_next_session(self):
        # Thursday 16:30 local -> Friday
        assert se.effective_session_for_filing("2026-07-30T16:30:00") == \
            "2026-07-31"

    def test_after_close_friday_rolls_to_monday(self):
        assert se.effective_session_for_filing("2026-07-31T18:00:00") == \
            "2026-08-03"

    def test_intraday_filing_same_session(self):
        assert se.effective_session_for_filing("2026-07-30T10:00:00") == \
            "2026-07-30"

    def test_prospective_floor_blocks_backfill(self):
        b = se.prospective_boundary("analyst_revisions", "2026-07-30")
        assert b["backfill_before_floor_allowed"] is False
        assert se.violates_prospective_floor(b, "2026-07-29") is True
        assert se.violates_prospective_floor(b, "2026-07-31") is False


# --------------------------------------------------------------------------- #
# Count-consistency fix (7 vs 10).
# --------------------------------------------------------------------------- #
class TestCountConsistency:
    _REC = {"campaign_experiments": 7,
            "campaign_decision_counts": {"REJECT_NOISE": 4,
                                         "REJECT_UNSTABLE": 3,
                                         "NEED_MORE_DATA": 3}}

    def test_reconciliation_always_reconciles(self):
        recon = rr.research_decision_reconciliation(self._REC)
        assert recon["reconciles"] is True
        assert recon["evaluated"] == recon["accounted"] == 10
        assert recon["completed"] == 7

    def test_progress_text_uses_reconciling_number(self):
        prog = rr.research_progress({"recovery_readiness": self._REC})
        joined = " ".join(prog["lines"])
        assert "10 recovery idea" in joined
        assert prog["reconciliation"]["reconciles"] is True

    def test_observatory_and_email_agree(self):
        # the observatory count logic (partition sum) equals the email's.
        counts = self._REC["campaign_decision_counts"]
        evaluated = sum(counts.values())
        recon = rr.research_decision_reconciliation(self._REC)
        assert recon["evaluated"] == evaluated == 10


# --------------------------------------------------------------------------- #
# Telegram control-plane security.
# --------------------------------------------------------------------------- #
class TestTelegramSecurity:
    def _cfg(self, user=111, chat=111):
        return {"telegram": {"enabled": True, "credential_dir": None,
                             "allowed_user_ids": [user],
                             "allowed_chat_ids": [chat]}}

    def _update(self, uid, cid, text, update_id=1, ctype="private"):
        return {"update_id": update_id,
                "message": {"from": {"id": uid},
                            "chat": {"id": cid, "type": ctype}, "text": text}}

    def test_only_allowed_user_and_chat_accepted(self):
        cfg = tc.TelegramConfig(self._cfg())
        assert tc.authorize(self._update(111, 111, "/status"), cfg)[0] is True
        assert tc.authorize(self._update(222, 111, "/status"), cfg)[0] is False
        assert tc.authorize(self._update(111, 222, "/status"), cfg)[0] is False

    def test_group_chat_denied(self):
        cfg = tc.TelegramConfig(self._cfg())
        assert tc.authorize(
            self._update(111, 111, "/status", ctype="group"), cfg)[0] is False

    def test_arbitrary_and_injection_commands_rejected(self):
        for text in ("rm -rf C:\\ ; DROP TABLE holdings",
                     "os.system('calc')", "exec(open('x').read())",
                     "sudo shutdown", "just some gibberish nonsense"):
            intent = tc.resolve_intent(text)
            assert intent["kind"] == tc.KIND_HELP

    def test_natural_language_creates_bounded_research_job(self, tmp_path):
        q = _q(tmp_path, clock=Clock())
        router = tc.ControlRouter(providers={}, queue=q, secrets=[])
        before = q.counts_by_state()[ar.STATE_QUEUED]
        router.handle(self._update(111, 111,
                                   "test residual momentum excluding financials"))
        after = q.counts_by_state()[ar.STATE_QUEUED]
        assert after == before + 1
        job = q.list_jobs(category=ar.CAT_TELEGRAM_REQUEST)[0]
        assert job.category == ar.CAT_TELEGRAM_REQUEST  # bounded research only

    def test_duplicate_research_request_is_idempotent(self, tmp_path):
        q = _q(tmp_path, clock=Clock())
        router = tc.ControlRouter(providers={}, queue=q)
        u = self._update(111, 111, "run an earnings-surprise experiment",
                         update_id=7)
        router.handle(u)
        router.handle(u)  # identical update id + text
        assert q.counts_by_state()[ar.STATE_QUEUED] == 1

    def test_read_only_command_never_enqueues(self, tmp_path):
        q = _q(tmp_path, clock=Clock())
        router = tc.ControlRouter(
            providers=tc.build_default_providers(queue=q), queue=q)
        before = q.counts_by_state()[ar.STATE_QUEUED]
        router.handle(self._update(111, 111, "/queue"))
        assert q.counts_by_state()[ar.STATE_QUEUED] == before

    def test_router_cannot_mutate_trading_state(self):
        # the router exposes exactly two effect classes: read-only providers and
        # queue.enqueue. It holds no reference to any trade/order/promotion API.
        router = tc.ControlRouter(providers={}, queue=None)
        attrs = set(vars(router))
        assert attrs <= {"providers", "queue", "secrets"}

    def test_secret_never_echoed(self, tmp_path):
        q = _q(tmp_path, clock=Clock())
        # A synthetic value in the SHAPE of a bot token, never a real credential -
        # the local is named for what it is so this file holds no literal that
        # reads as a credential assignment.
        fake_token = "123456789:AAterribleTokenValueThatMustNeverLeak000"
        router = tc.ControlRouter(providers={"status": lambda: fake_token},
                                  queue=q, secrets=[fake_token])
        chunks = router.handle(self._update(111, 111, "/status"))
        assert all(fake_token not in c for c in chunks)

    def test_bot_token_shape_is_redacted(self):
        leaked = "here is 987654321:AAbbccddeeffgghhiijjkkllmmnnooppqqrr and more"
        assert "987654321:" not in tc.redact(leaked)

    def test_dedupe_store_is_idempotent(self, tmp_path):
        store = tc.TelegramStore(tmp_path / "tg.sqlite", clock=Clock())
        assert store.mark_seen(100) is True
        assert store.mark_seen(100) is False  # duplicate update -> ignored
        assert store.seen(100) is True

    def test_poll_offset_advances_and_dedupes(self, tmp_path):
        cfg = tc.TelegramConfig(self._cfg())
        store = tc.TelegramStore(tmp_path / "tg.sqlite", clock=Clock())
        q = _q(tmp_path, clock=Clock())
        router = tc.ControlRouter(providers=tc.build_default_providers(queue=q),
                                  queue=q)

        class FakeClient:
            def __init__(self):
                self.sent = []
                self.batches = [
                    {"result": [
                        {"update_id": 10, "message": {"from": {"id": 111},
                         "chat": {"id": 111, "type": "private"},
                         "text": "/status"}},
                        {"update_id": 11, "message": {"from": {"id": 999},
                         "chat": {"id": 999, "type": "private"},
                         "text": "/status"}}]},
                    {"result": []}]

            def get_updates(self, *, token, offset, timeout):
                return self.batches.pop(0) if self.batches else {"result": []}

            def send_message(self, *, token, chat_id, text):
                self.sent.append((chat_id, text))
                return {"ok": True}

        client = FakeClient()
        s1 = tc.poll_once(client=client, token="tkn", cfg=cfg, store=store,
                          router=router)
        assert s1["processed"] == 1 and s1["denied"] == 1
        assert store.get_offset() == 12  # advanced past max update id
        # the denied user got no reply
        assert all(cid == 111 for cid, _ in client.sent)

    def test_diagnose_classifications(self):
        class C:
            def __init__(self, resp):
                self.resp = resp

            def get_me(self, *, token):
                return self.resp
        assert tc.diagnose(client=C({"ok": True, "result": {"id": 1}}),
                           token="x") == tc.TELEGRAM_AUTH_OK
        assert tc.diagnose(client=C({"ok": False}), token="x") == \
            tc.TELEGRAM_AUTH_REJECTED
        assert tc.diagnose(client=C({}), token="") == tc.TELEGRAM_TOKEN_MISSING

    def test_config_holds_no_token(self):
        cfg8 = json.loads(_STAGE8_CFG.read_text(encoding="utf-8-sig"))
        blob = json.dumps(cfg8)
        import re
        assert re.search(r"\b\d{6,}:[A-Za-z0-9_\-]{30,}\b", blob) is None
        assert rc.scan_for_secrets(cfg8) == []


# --------------------------------------------------------------------------- #
# Observatory autonomy + safety + config compatibility.
# --------------------------------------------------------------------------- #
class TestObservabilityAndSafety:
    def test_autonomy_snapshot_is_read_only_and_shaped(self, tmp_path):
        cfg = {"autonomy": {"queue_db": str(tmp_path / "none.sqlite")},
               "sources": {}, "telegram": {}}
        snap = eo.autonomy_snapshot(cfg)
        assert snap["status"] == "OK"
        assert "queue" in snap and "sources" in snap and "telegram" in snap
        assert "NO ORDERS" in snap["safety_badges"]
        # queue db did not exist -> NOT_INITIALIZED, and was NOT created
        assert snap["queue"].get("status") == "NOT_INITIALIZED"
        assert not (tmp_path / "none.sqlite").exists()

    def test_autonomy_snapshot_reads_live_queue(self, tmp_path):
        db = tmp_path / "autonomy.sqlite"
        q = ar.ResearchQueue(db, clock=Clock())
        q.enqueue(ar.CAT_EXPERIMENT, lane="x", payload={})
        snap = eo.autonomy_snapshot({"autonomy": {"queue_db": str(db)}})
        assert snap["queue"]["depth"] == 1

    def test_stage8_config_lists_five_tasks(self):
        cfg8 = json.loads(_STAGE8_CFG.read_text(encoding="utf-8-sig"))
        wt = cfg8["windows_tasks"]
        all_tasks = wt["cadence_tasks"] + wt["control_tasks"]
        assert sorted(all_tasks) == sorted(list(rc.ALPHA_AGENT_TASK_NAMES)
                                           + ["AlphaAgent-Telegram"])
        assert wt["all_disabled_until_final_validation"] is True

    def test_stage4_config_still_loads_and_smtp_intact(self):
        cfg = rc.load_config(_REPO / "configs" / "alpha_agent"
                             / "stage4_runtime.json")
        assert cfg["email"]["transport"] == "gmail_smtp"
        assert cfg.get("stage8_enabled") is True
        # Stage 8 additions did NOT change the strict cadence-task contract.
        assert sorted(cfg["allowed_task_names"]) == sorted(
            list(rc.ALPHA_AGENT_TASK_NAMES))

    def test_stage8_stores_live_off_the_operational_ledger_dir(self, tmp_path):
        # the queue + telegram stores must never sit in the desk ledger dir.
        cfg8 = json.loads(_STAGE8_CFG.read_text(encoding="utf-8-sig"))
        qdb = cfg8["autonomy"]["queue_db"].lower()
        tgdb = cfg8["telegram"]["state_db"].lower()
        assert "paper_trading_desk" not in qdb
        assert "paper_trading_desk" not in tgdb
        assert "stage8" in qdb and "stage8" in tgdb

    def test_runtime_autonomy_entrypoints_are_research_only(self, tmp_path):
        cfg = {"stage8_autonomy_root": str(tmp_path / "s8"),
               "autonomy": {"max_jobs_per_cycle": 3}}
        summ = rt.run_autonomy_cycle(cfg)
        assert summ["processed"] >= 1 and summ["depth"] >= 1
        wd = rt.run_autonomy_watchdog(cfg)
        assert wd["never_idle"] is True


# --------------------------------------------------------------------------- #
# Telegram id contract: large ids (> Int32) are handled as normalized decimal
# strings end-to-end, without overflow or precision loss. Regression for the
# "Cannot convert value 8284912423 to type System.Int32" configuration failure.
# --------------------------------------------------------------------------- #
class TestTelegramLargeIdContract:
    BIG = "8284912423"          # the real user/chat id; > 2**31-1 (2147483647)
    BIG_INT = 8284912423        # as Telegram delivers it (JSON integer)
    HUGE = "9999999999999999"   # > 2**53; would lose precision as a JS/JSON number

    def _cfg(self, user, chat):
        return {"telegram": {"enabled": True, "credential_dir": None,
                             "allowed_user_ids": [user],
                             "allowed_chat_ids": [chat]}}

    def _update(self, uid, cid, text="/status", update_id=1, ctype="private"):
        return {"update_id": update_id,
                "message": {"from": {"id": uid},
                            "chat": {"id": cid, "type": ctype}, "text": text}}

    # 1 + 2: the exact real user id AND chat id are accepted.
    def test_real_large_user_and_chat_id_are_accepted(self):
        cfg = tc.TelegramConfig(self._cfg(self.BIG, self.BIG))
        ok, reason = tc.authorize(
            self._update(self.BIG_INT, self.BIG_INT), cfg)
        assert ok is True, reason

    # 3: an id above the signed 32-bit range never overflows (Python int is
    # arbitrary precision) and a > 2**53 id round-trips exactly as a string.
    def test_ids_above_int32_and_2pow53_do_not_overflow(self):
        assert int(tc.normalize_telegram_id(self.BIG)) > 2147483647
        assert tc.normalize_telegram_id(self.HUGE) == self.HUGE
        assert tc.normalize_telegram_id(self.BIG_INT) == self.BIG
        # canonicalization: leading '+', leading zeros and a '-' sign.
        assert tc.normalize_telegram_id(" +008284912423 ") == self.BIG
        assert tc.normalize_telegram_id("-1001234567890") == "-1001234567890"

    # 4: stored ids are normalized STRINGS, never ints.
    def test_stored_ids_are_normalized_strings(self):
        cfg = tc.TelegramConfig(self._cfg(self.BIG_INT, self.BIG_INT))
        assert cfg.allowed_user_ids == [self.BIG]
        assert cfg.allowed_chat_ids == [self.BIG]
        assert all(isinstance(x, str) for x in cfg.allowed_user_ids)
        assert all(isinstance(x, str) for x in cfg.allowed_chat_ids)

    # 5: an integer id in the Telegram update matches a string id in the store.
    def test_integer_update_matches_string_allowlist(self):
        cfg = tc.TelegramConfig(self._cfg(self.BIG, self.BIG))  # stored strings
        assert tc.authorize(                                    # int in update
            self._update(self.BIG_INT, self.BIG_INT), cfg)[0] is True

    # 6: a different large id is rejected (no accidental match/overflow-collapse).
    def test_unauthorized_large_id_is_rejected(self):
        cfg = tc.TelegramConfig(self._cfg(self.BIG, self.BIG))
        assert tc.authorize(self._update(8284912424, self.BIG_INT), cfg)[0] \
            is False
        assert tc.authorize(self._update(self.BIG_INT, 8284912424), cfg)[0] \
            is False

    # 7: letters, decimals and blank input are rejected by normalization, and a
    # single scalar allowlist value is never iterated character-by-character.
    def test_invalid_values_are_rejected(self):
        for bad in ("abc", "12.5", "8284912423.0", "", "  ", "1,234", None,
                    "0x1F", "8284912423a"):
            assert tc.normalize_telegram_id(bad) is None
        # a scalar (not a list) allowlist entry stays ONE id, not many digits.
        cfg = tc.TelegramConfig({"telegram": {"allowed_user_ids": self.BIG,
                                              "allowed_chat_ids": self.BIG}})
        assert cfg.allowed_user_ids == [self.BIG]
        assert "8" not in cfg.allowed_user_ids  # not split into digits

    # 8: the configure script never [int]-casts an id and never persists or
    # prints the bot token in plaintext.
    def test_configure_script_avoids_int_cast_and_token_leak(self):
        ps1 = (_REPO / "scripts" / "configure_alpha_agent_telegram.ps1"
               ).read_text(encoding="utf-8")
        assert "@([int]$UserIdRaw)" not in ps1     # the overflow bug is gone
        # no fixed-width int CAST of an id variable survives (prose mentions of
        # the type in comments are fine; a cast is "[type]$var").
        for cast in ("[int]$UserIdRaw", "[int]$ChatIdRaw", "[System.Int32]$",
                     "[Int32]$", "[long]$ChatIdRaw", "[long]$UserIdRaw"):
            assert cast not in ps1
        assert "allowed_user_ids = @([string]$UserIdRaw)" in ps1
        assert "allowed_chat_ids = @([string]$ChatIdRaw)" in ps1
        # the plaintext token variables are never echoed to the console...
        for line in ps1.splitlines():
            if line.strip().startswith("Write-Host"):
                assert "$Normalized" not in line and "$Raw" not in line
        # ...and only the DPAPI blob (never the token) is written to disk.
        assert "$Normalized" not in ps1.split("$Allow")[1]

    # 9: duplicate updates from the large-id user remain deduplicated.
    def test_duplicate_large_id_update_is_deduped(self, tmp_path):
        store = tc.TelegramStore(tmp_path / "tg.sqlite", clock=Clock())
        assert store.mark_seen(self.BIG_INT) is True
        assert store.mark_seen(self.BIG_INT) is False
        store.audit(update_id=self.BIG_INT, user_id=self.BIG_INT,
                    chat_id=self.BIG_INT, kind="READ_ONLY_QUERY",
                    command="/status", allowed=True)
        last = store.last_request()
        assert last is not None
        assert str(last["user_id"]) == self.BIG   # persisted as normalized text

    # 10: a large-id user's only possible effect is a bounded research enqueue;
    # no operational trading state can change.
    def test_large_id_user_only_enqueues_bounded_research(self, tmp_path):
        q = _q(tmp_path, clock=Clock())
        router = tc.ControlRouter(providers={}, queue=q, secrets=[])
        assert set(vars(router)) <= {"providers", "queue", "secrets"}
        before = q.counts_by_state()[ar.STATE_QUEUED]
        router.handle(self._update(self.BIG_INT, self.BIG_INT,
                                   text="run a momentum experiment"))
        after = q.counts_by_state()[ar.STATE_QUEUED]
        assert after == before + 1
        job = q.list_jobs(category=ar.CAT_TELEGRAM_REQUEST)[0]
        assert job.category == ar.CAT_TELEGRAM_REQUEST  # research only, no trade


# --------------------------------------------------------------------------- #
# Production handlers: the durable queue is wired to REAL Stage 2/5/6 work.
# --------------------------------------------------------------------------- #
class TestProductionHandlers:
    def _cfg(self):
        return json.loads(_STAGE8_CFG.read_text(encoding="utf-8-sig"))

    def test_builder_overrides_the_real_work_categories(self):
        # Building the handler map is offline (only reads config + defines
        # closures); it must OVERRIDE the offline `_record` stub for every
        # real-work category with a distinct real handler.
        cfg = self._cfg()
        prod = rt.build_production_autonomy_handlers(cfg)
        default = rt._default_autonomy_handlers(cfg)
        assert set(prod) == set(ar.JOB_CATEGORIES)
        for c in (ar.CAT_DATA_ACQUISITION, ar.CAT_COVERAGE_REPAIR,
                  ar.CAT_PROSPECTIVE_SNAPSHOT, ar.CAT_EXPERIMENT,
                  ar.CAT_ROBUSTNESS_TEST, ar.CAT_SIGNAL_COMBINATION,
                  ar.CAT_TELEGRAM_REQUEST):
            assert prod[c] is not default[c]

    def test_build_autonomy_queue_honors_canonical_queue_db(self, tmp_path):
        # The Telegram control plane and the autonomy cycle MUST share one queue:
        # build_autonomy_queue honors autonomy.queue_db (canonical) over the
        # derived root, so an enqueue and a drain never split across two DBs.
        qdb = tmp_path / "canonical" / "autonomy.sqlite"
        q = rt.build_autonomy_queue(
            {"autonomy": {"queue_db": str(qdb)},
             "stage8_autonomy_root": str(tmp_path / "derived")})
        q.enqueue(ar.CAT_EXPERIMENT, lane="x", payload={})
        assert qdb.exists()                          # canonical path was used
        assert not (tmp_path / "derived").exists()   # derived path was NOT used

    def test_config_enables_production_handlers_with_stage_pointers(self):
        cfg = self._cfg()
        assert (cfg.get("autonomy") or {}).get("handlers") == "production"
        ph = cfg.get("production_handlers") or {}
        for k in ("stage2_ingestion_config", "stage5_experiment_config",
                  "stage6_backfill_config"):
            assert (_REPO / ph[k]).is_file()
        assert ph.get("bounded_universe") and ph.get("coverage_repair_universe")

    def test_cycle_selects_production_handlers_when_configured(self, tmp_path,
                                                              monkeypatch):
        # When cfg asks for production handlers the cycle BUILDS them (proven by
        # a spy that returns trivial offline handlers so no network is touched).
        built = {}

        def _spy(cfg, **kw):
            built["yes"] = True
            return {c: (lambda job: (ar.OUTCOME_COMPLETED, {"stub": True}))
                    for c in ar.JOB_CATEGORIES}

        monkeypatch.setattr(rt, "build_production_autonomy_handlers", _spy)
        rt.run_autonomy_cycle({"stage8_autonomy_root": str(tmp_path / "s8"),
                               "autonomy": {"handlers": "production",
                                            "max_jobs_per_cycle": 1}})
        assert built.get("yes") is True

    def test_default_handlers_used_when_not_configured(self, tmp_path,
                                                       monkeypatch):
        # Without the production flag the offline defaults are used (the spy must
        # NOT fire) — preserving the safe, network-free default behaviour.
        fired = {}
        monkeypatch.setattr(rt, "build_production_autonomy_handlers",
                            lambda cfg, **kw: fired.setdefault("yes", True) or {})
        summ = rt.run_autonomy_cycle(
            {"stage8_autonomy_root": str(tmp_path / "s8"),
             "autonomy": {"max_jobs_per_cycle": 1}})
        assert "yes" not in fired and summ["processed"] >= 1


# --------------------------------------------------------------------------- #
# Enriched read-only Telegram providers (substantive /status, /sources, ...).
# --------------------------------------------------------------------------- #
class TestTelegramReadOnlyProviders:
    def test_all_providers_return_strings_and_never_mutate(self, tmp_path):
        q = _q(tmp_path, clock=Clock())
        q.enqueue(ar.CAT_EXPERIMENT, lane="x", payload={})
        before = q.counts_by_state()
        providers = tc.build_default_providers(stage8_config={"sources": {}},
                                               queue=q)
        for key in ("status", "sources", "coverage", "data", "experiments",
                    "health", "queue", "blocked"):
            assert key in providers
            assert isinstance(providers[key](), str)
        # reading evidence never mutated the durable queue
        assert q.counts_by_state() == before

    def test_diagnose_meta_and_id_are_public_nonsecret(self):
        class C:
            def get_me(self, *, token):
                return {"ok": True, "result": {"id": 8284912423,
                                               "username": "PaperTrader05_bot"}}
        assert tc.diagnose(client=C(), token="x") == tc.TELEGRAM_AUTH_OK
        assert tc.normalize_telegram_id(8284912423) == "8284912423"
