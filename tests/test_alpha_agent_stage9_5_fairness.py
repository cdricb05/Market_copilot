"""
Stage 9.5 FINAL RELEASE BLOCKER - DETERMINISTIC COMPANYFACTS CONTINUATION
FAIRNESS. The bounded collect drain runs AT MOST ONE job per cycle in three
passes (A tournament-continuation allowlist; B SEC Form4/8-K continuation iff A
idle; C SEC companyfacts continuation iff A and B idle). The Stage 9.4 tournament
revalidation loop registers one PASS-A job every cycle, so without a fairness
rule the companyfacts PASS-C continuation could wait an unbounded number of
cycles. These deterministic tests prove the fix (alpha_agent/drain_fairness.py)
through the SAME ``run_fair_drain`` the runtime uses:

  1  continuous tournament generation cannot starve companyfacts
  2  companyfacts executes within the configured fairness bound
  3  companyfacts cannot monopolize every cycle
  4  exactly one job executes per cycle
  5  ineligible campaign jobs remain untouched
  6  attempts increment exactly once
  7  restart preserves fairness state
  8  daily / no-progress campaign limits still win over fairness
  9  historical fundamental experiments remain disabled
  10 candidate states remain DATA_HOLD
  11 operational ledgers remain unchanged

Plus unit coverage of the pure fairness rule and the durable counter.
"""
import json
from pathlib import Path

from alpha_agent import autonomous_research as ar
from alpha_agent import drain_fairness as df
from alpha_agent import fundamental_readiness as fr
from alpha_agent import runtime as rt
from alpha_agent import tournament as tt

REPO = Path(__file__).resolve().parents[1]
STAGE9_CFG = json.loads((REPO / "configs/alpha_agent/stage9_tournament.json")
                        .read_text(encoding="utf-8-sig"))

_CLOCK = lambda: "2026-08-01T00:00:00+00:00"          # noqa: E731 - test clock
_TOURN_LANE = "tournament.stage9_4_revalidation"
_CF_LANE = "acq.sec_companyfacts"


def _drain_cfg(**cf_overrides):
    """A drain config mirroring the production companyfacts continuation scope,
    with PASS B (SEC Form4/8-K) disabled so the tests isolate the tournament vs
    companyfacts fairness interaction."""
    cf = {
        "enabled": True,
        "allowed_origins": ["campaign-continuation"],
        "allowed_lane_prefixes": ["acq.sec_companyfacts"],
        "allowed_categories": ["DATA_ACQUISITION"],
        "fairness_enabled": True,
        "fairness_max_idle_cycles": 2,
        "daily_completed_batch_cap": 4,
        "max_consecutive_no_progress_batches": 2,
    }
    cf.update(cf_overrides)
    return {
        "enabled": True,
        "max_jobs_per_cycle": 1,
        "allowed_origins": ["stage9-tournament", "stage9-weakest-gate"],
        "allowed_lane_prefixes": ["tournament.", "acq.sec_form4_8k",
                                  "acq.sec_companyfacts"],
        "allowed_categories": ["DATA_VALIDATION", "DATA_ACQUISITION",
                               "EXPERIMENT"],
        "sec_continuation": {"enabled": False},
        "companyfacts_continuation": cf,
    }


class _Harness:
    """A minimal, network-free stand-in for the real drain. The tournament
    handler simulates the Stage 9.4 revalidation loop; the companyfacts handler
    simulates a bounded XBRL batch that makes progress and chains the next
    continuation (payload carries the advancing cursor so the chained job is a
    DISTINCT live job, exactly like _run_companyfacts_campaign)."""

    def __init__(self, tmp_path):
        self.queue = ar.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_CLOCK)
        self.fair = df.FairnessStore(str(tmp_path / "fair.sqlite"),
                                     clock=_CLOCK)
        self.tournament_runs = 0
        self.companyfacts_runs = 0
        self.tournament_seq = 0               # distinct revalidation per cycle
        self.cursor = 5                       # simulated campaign cursor
        # The initial companyfacts continuation already sits QUEUED (mirrors the
        # live job_1cddbc2f... state), claimable ONLY by PASS C / a promotion.
        self._enqueue_continuation()

    def _enqueue_continuation(self):
        return self.queue.enqueue(
            ar.CAT_DATA_ACQUISITION, lane=_CF_LANE,
            payload={"campaign": "sec_companyfacts", "seq": self.cursor},
            origin="campaign-continuation", priority=5)

    def _enqueue_tournament(self):
        # The Stage 9.4 revalidation loop registers a DISTINCT computable-feature
        # revalidation each cycle - continuous PASS-A generation. (A distinct
        # payload per cycle also avoids a fixed-test-clock job_id collision; the
        # live runtime advances its clock so real revalidations never collide.)
        self.tournament_seq += 1
        return self.queue.enqueue(
            ar.CAT_EXPERIMENT, lane=_TOURN_LANE,
            payload={"strategy": "stage9_4_revalidation",
                     "feature_seq": self.tournament_seq},
            origin="stage9-tournament", priority=3)

    def _tournament_handler(self, job):
        self.tournament_runs += 1
        return ar.OUTCOME_COMPLETED, {"real_work": "stage9_4_revalidation"}

    def _companyfacts_handler(self, job):
        self.companyfacts_runs += 1
        self.cursor += 5                      # bounded batch made progress
        self._enqueue_continuation()          # chain the next batch
        return ar.OUTCOME_COMPLETED, {
            "real_work": "sec_companyfacts_pit_campaign", "progress": True}

    def _handlers(self):
        return {ar.CAT_EXPERIMENT: self._tournament_handler,
                ar.CAT_DATA_VALIDATION: self._tournament_handler,
                ar.CAT_DATA_ACQUISITION: self._companyfacts_handler}

    def cycle(self, drain_cfg, *, campaign_stopped=False,
              generate_tournament=True):
        if generate_tournament:
            self._enqueue_tournament()
        return df.run_fair_drain(
            self.queue, self._handlers(), drain_cfg=drain_cfg,
            fair_store=self.fair, campaign_stopped=campaign_stopped,
            cursor_after=lambda: self.cursor)


# --------------------------------------------------------------------------- #
# Pure fairness rule + durable counter (unit level).
# --------------------------------------------------------------------------- #
def test_rule_promote_only_when_due_and_eligible():
    # Due + eligible -> promote.
    assert df.should_promote_companyfacts(
        idle_cycles=2, max_idle_cycles=2, continuation_queued=True,
        campaign_stopped=False) is True
    # Not yet due.
    assert df.should_promote_companyfacts(
        idle_cycles=1, max_idle_cycles=2, continuation_queued=True,
        campaign_stopped=False) is False
    # No eligible continuation queued.
    assert df.should_promote_companyfacts(
        idle_cycles=9, max_idle_cycles=2, continuation_queued=False,
        campaign_stopped=False) is False
    # Campaign stopped ALWAYS wins over fairness.
    assert df.should_promote_companyfacts(
        idle_cycles=9, max_idle_cycles=2, continuation_queued=True,
        campaign_stopped=True) is False
    # A bound below 1 never promotes (guards against a monopoly misconfig).
    assert df.should_promote_companyfacts(
        idle_cycles=9, max_idle_cycles=0, continuation_queued=True,
        campaign_stopped=False) is False


def test_store_record_cycle_reset_and_increment(tmp_path):
    s = df.FairnessStore(str(tmp_path / "f.sqlite"), clock=_CLOCK)
    assert s.idle_cycles(df.COMPANYFACTS_KEY) == 0
    s.record_cycle(df.COMPANYFACTS_KEY, executed=False)
    s.record_cycle(df.COMPANYFACTS_KEY, executed=False)
    assert s.idle_cycles(df.COMPANYFACTS_KEY) == 2
    st = s.record_cycle(df.COMPANYFACTS_KEY, executed=True, cursor=10)
    assert st["idle_cycles"] == 0 and st["total_executions"] == 1
    assert st["last_execution_cursor"] == 10
    s.record_cycle(df.COMPANYFACTS_KEY, executed=False)
    assert s.idle_cycles(df.COMPANYFACTS_KEY) == 1
    assert s.state(df.COMPANYFACTS_KEY)["total_cycles"] == 4


# --------------------------------------------------------------------------- #
# 1 + 2  continuous tournament generation cannot starve companyfacts, and
#        companyfacts executes within the configured fairness bound.
# --------------------------------------------------------------------------- #
def test_prop1_2_companyfacts_executes_within_bound_despite_tournament(tmp_path):
    h = _Harness(tmp_path)
    cfg = _drain_cfg(fairness_max_idle_cycles=2)
    reports = [h.cycle(cfg) for _ in range(3)]
    executed = [r["companyfacts_fairness"]["executed"] for r in reports]
    # Bound = 2: two no-progress cycles, then the 3rd MUST execute companyfacts.
    assert executed == [False, False, True]
    assert h.companyfacts_runs == 1               # executed within the bound
    assert h.tournament_runs == 2                 # tournament NOT starved
    assert reports[2]["companyfacts_fairness_promoted"] is True
    # The idle counter never exceeds the bound before the forced execution.
    assert [r["companyfacts_fairness"]["idle_cycles_before"] for r in reports] \
        == [0, 1, 2]


# --------------------------------------------------------------------------- #
# 3  companyfacts cannot monopolize every cycle (nor can tournament starve it).
# --------------------------------------------------------------------------- #
def test_prop3_companyfacts_does_not_monopolize(tmp_path):
    h = _Harness(tmp_path)
    cfg = _drain_cfg(fairness_max_idle_cycles=2)
    n = 9
    for _ in range(n):
        h.cycle(cfg)
    # Steady state with bound=2: companyfacts runs once per 3 cycles (3,6,9),
    # tournament the other two-thirds. Neither is starved; neither monopolizes.
    assert h.companyfacts_runs == 3
    assert h.tournament_runs == 6
    assert h.companyfacts_runs < n and h.tournament_runs < n


# --------------------------------------------------------------------------- #
# 4  exactly one job executes per cycle (promoted AND non-promoted).
# --------------------------------------------------------------------------- #
def test_prop4_exactly_one_job_per_cycle(tmp_path):
    h = _Harness(tmp_path)
    cfg = _drain_cfg(fairness_max_idle_cycles=2)
    for _ in range(6):
        r = h.cycle(cfg)
        assert r["jobs_claimed"] == 1             # never 0, never >1
        assert len(r["job_ids"]) == 1
    # Sum of both handlers' executions equals the six single-job cycles.
    assert h.companyfacts_runs + h.tournament_runs == 6


# --------------------------------------------------------------------------- #
# 5  ineligible campaign jobs remain untouched (even during a promotion).
# --------------------------------------------------------------------------- #
def test_prop5_ineligible_jobs_untouched(tmp_path):
    h = _Harness(tmp_path)
    cfg = _drain_cfg(fairness_max_idle_cycles=2)
    # (a) a campaign-continuation job on a DIFFERENT lane (PASS B disabled).
    j_form4 = h.queue.enqueue(
        ar.CAT_DATA_ACQUISITION, lane="acq.sec_form4_8k",
        payload={"campaign": "sec_form4_8k"}, origin="campaign-continuation",
        priority=9)
    # (b) a companyfacts-lane job with the WRONG origin (not campaign-continuation
    #     and not an admitted PASS-A origin).
    j_wrong = h.queue.enqueue(
        ar.CAT_DATA_ACQUISITION, lane=_CF_LANE,
        payload={"campaign": "sec_companyfacts", "seq": 999},
        origin="some-other-origin", priority=9)
    for _ in range(3):                            # includes the promoted cycle
        h.cycle(cfg)
    assert h.companyfacts_runs == 1               # promotion still fired
    assert h.queue.get(j_form4).state == ar.STATE_QUEUED
    assert h.queue.get(j_wrong).state == ar.STATE_QUEUED


# --------------------------------------------------------------------------- #
# 6  attempts increment exactly once for the executed continuation.
# --------------------------------------------------------------------------- #
def test_prop6_attempts_increment_exactly_once(tmp_path):
    h = _Harness(tmp_path)
    cfg = _drain_cfg(fairness_max_idle_cycles=2)
    reports = [h.cycle(cfg) for _ in range(3)]
    promoted = reports[2]
    assert promoted["companyfacts_fairness_promoted"] is True
    jid = promoted["job_ids"][0]
    job = h.queue.get(jid)
    assert job.state == ar.STATE_COMPLETED
    assert job.attempts == 1                      # claimed+executed exactly once
    assert str(job.lane).startswith(_CF_LANE)


# --------------------------------------------------------------------------- #
# 7  restart preserves fairness state (durable across process restart).
# --------------------------------------------------------------------------- #
def test_prop7_restart_preserves_fairness_state(tmp_path):
    path = str(tmp_path / "fair.sqlite")
    s1 = df.FairnessStore(path, clock=_CLOCK)
    s1.record_cycle(df.COMPANYFACTS_KEY, executed=False)
    s1.record_cycle(df.COMPANYFACTS_KEY, executed=False)
    assert s1.idle_cycles(df.COMPANYFACTS_KEY) == 2
    del s1
    s2 = df.FairnessStore(path, clock=_CLOCK)      # "restart"
    assert s2.idle_cycles(df.COMPANYFACTS_KEY) == 2
    assert s2.is_due(df.COMPANYFACTS_KEY, max_idle_cycles=2) is True
    # A drain after restart therefore promotes immediately (idle survived).
    q = ar.ResearchQueue(str(tmp_path / "q.sqlite"), clock=_CLOCK)
    q.enqueue(ar.CAT_DATA_ACQUISITION, lane=_CF_LANE,
              payload={"campaign": "sec_companyfacts", "seq": 5},
              origin="campaign-continuation", priority=5)
    q.enqueue(ar.CAT_EXPERIMENT, lane=_TOURN_LANE, payload={},
              origin="stage9-tournament", priority=3)
    ran = {"cf": 0}

    def _cf(job):
        ran["cf"] += 1
        return ar.OUTCOME_COMPLETED, {"real_work": "sec_companyfacts_pit_campaign"}

    def _tourn(job):
        return ar.OUTCOME_COMPLETED, {"real_work": "t"}
    rep = df.run_fair_drain(
        q, {ar.CAT_DATA_ACQUISITION: _cf, ar.CAT_EXPERIMENT: _tourn},
        drain_cfg=_drain_cfg(), fair_store=s2, campaign_stopped=False,
        cursor_after=lambda: 10)
    assert rep["companyfacts_fairness_promoted"] is True and ran["cf"] == 1


# --------------------------------------------------------------------------- #
# 8  daily / no-progress campaign limits STILL win over fairness.
# --------------------------------------------------------------------------- #
def test_prop8_campaign_stop_wins_over_fairness(tmp_path):
    h = _Harness(tmp_path)
    cfg = _drain_cfg(fairness_max_idle_cycles=2)
    # Drive the idle counter to the bound WITHOUT executing companyfacts.
    h.fair.record_cycle(df.COMPANYFACTS_KEY, executed=False)
    h.fair.record_cycle(df.COMPANYFACTS_KEY, executed=False)
    assert h.fair.idle_cycles(df.COMPANYFACTS_KEY) == 2
    # The campaign is stopped (daily cap / no-progress) -> fairness must NOT
    # promote; PASS A keeps the slot and the continuation stays queued.
    r = h.cycle(cfg, campaign_stopped=True)
    assert r["companyfacts_fairness_promoted"] is False
    assert r["companyfacts_fairness"]["executed"] is False
    assert h.companyfacts_runs == 0
    assert h.tournament_runs == 1                  # PASS A ran instead
    queued_cf = [j for j in h.queue.list_jobs(state=ar.STATE_QUEUED, limit=50)
                 if j.lane == _CF_LANE and j.origin == "campaign-continuation"]
    assert len(queued_cf) == 1                     # continuation untouched


# --------------------------------------------------------------------------- #
# 9 + 10  the fairness change does NOT weaken the Stage 9.5B safety switch:
#         historical fundamental experiments stay disabled and candidates stay
#         DATA_HOLD.
# --------------------------------------------------------------------------- #
def test_prop9_10_safety_switch_and_candidate_state_unchanged(tmp_path):
    gate = fr.historical_fundamental_experiment_allowed(STAGE9_CFG)
    assert gate["allowed"] is False
    assert gate["diagnostic"] == fr.HISTORICAL_FUNDAMENTAL_UNIVERSE_NOT_READY
    reg = tt.CandidateRegistry(str(tmp_path / "t.sqlite"))
    tt.seed_families(reg)
    cand = tt._candidate_for_feature(reg, "gross_profitability")
    q = ar.ResearchQueue(str(tmp_path / "q.sqlite"))
    res = tt.generate_stage9_5_fundamental_followups(reg, STAGE9_CFG, queue=q)
    st = reg.get(cand["candidate_id"])["lifecycle_state"]
    reg.close()
    assert res["count"] == 0
    assert res["blocker"] == fr.HISTORICAL_FUNDAMENTAL_UNIVERSE_NOT_READY
    assert st in (tt.PROPOSED, tt.DATA_HOLD, tt.TESTING)
    assert st not in (tt.KEEP_FOR_RESEARCH, tt.SHADOW_BOOK_ACTIVE)
    # No fundamental experiment job was created by the follow-up generator.
    assert [j for j in q.list_jobs(limit=50)
            if j.lane == "tournament.stage9_5_fundamental"] == []


# --------------------------------------------------------------------------- #
# 11  operational ledgers remain unchanged: the durable fairness state lives
#     OUTSIDE any operational-ledger root, so many fairness cycles never change
#     the operational-ledger fingerprint.
# --------------------------------------------------------------------------- #
def test_prop11_operational_ledger_unchanged(tmp_path):
    ledger_dir = tmp_path / "operational_ledger"
    ledger_dir.mkdir()
    (ledger_dir / "book.json").write_text(json.dumps({"nav": 100000}),
                                          encoding="utf-8")
    cfg = {"operational_ledger_roots": [str(ledger_dir)]}
    before = rt.fingerprint_ledgers(cfg)
    # Run several fairness cycles; the fair store writes to tmp_path/fair.sqlite,
    # NOT under ledger_dir.
    h = _Harness(tmp_path)
    for _ in range(6):
        h.cycle(_drain_cfg())
    after = rt.fingerprint_ledgers(cfg)
    assert before == after
    assert not (ledger_dir / "scheduler_fairness.sqlite").exists()
    assert h.companyfacts_runs >= 1               # work really happened
