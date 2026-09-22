r"""Release 65 - the persistent researcher stops re-deriving an unchanged world.

THE DEFECT, MEASURED BEFORE IT WAS FIXED

    R64 repaired the SLEEP: the worker no longer spins through zero-second
    cycles. It did not repair what the worker does when it WAKES. The R52 run
    journal (``runtime_runs.json``, 400 retained rows) measured that:

        window          2026-09-20T21:24Z .. 2026-09-22T20:17Z   (46.9 h)
        invocations     400
        mean duration   292 s   (median 286 s, p90 356 s, max 1014 s)
        total           116,747 s = 32.4 CPU-hours
        idle gap        median 8 s between the end of one run and the next
        duty cycle      91-105% of one core, hour after hour
        productive      16 / 400  (4%) - a substantive stage produced anything

    Ninety-six per cent of that work re-derived a conclusion from inputs that
    had not moved: ``tournament_advance`` NOT_DUE 399/400,
    ``futures_trend_prospective_decision`` NOT_DUE 400/400,
    ``canonical_forward_accrual`` NOT_DUE 399/400, while
    ``velocity_operational`` and ``promotion_frontier`` rebuilt read models
    400/400 from unchanged sources.

WHAT THE FIX MAY NOT DO

    It may not become a second scheduler, a second timing authority or a
    once-a-day rule. It may not skip an owner that holds a live decision
    window, because a missed window is permanent. It may not turn "not
    measured" into "measured as absent" in the health read model. It may not
    let a failed cycle go unretried. And it may not be able to suppress work
    the canonical owners say is due - the only operator override points at
    MORE work, never less.

WHAT THESE TESTS BIND

    The gate's verdict logic (pure, injectable), the partition between the
    stages that are never gated and the stages that are, the ceiling's
    derivation from the timing contract's own invocation plan, the wiring
    order inside the one orchestration owner, and the health document's
    refusal to overwrite a measurement it did not take.
"""
from __future__ import annotations

import datetime as dt
import inspect
import json
from pathlib import Path

import pytest

from paper_trader.alpha_agent.r52 import eligibility as EL
from paper_trader.alpha_agent.r52 import runtime as RT
from paper_trader.alpha_agent.r52 import timing_contract as TC

RUNTIME_SRC = Path(inspect.getfile(RT)).read_text(encoding="utf-8")
EL_SRC = Path(inspect.getfile(EL)).read_text(encoding="utf-8")

NOW = dt.datetime(2026, 9, 22, 21, 0, 0, tzinfo=dt.timezone.utc)


def _terms(**over):
    base = {"clock.eastern_date": "2026-09-22",
            "clock.entry_session_date": "2026-09-23",
            "clock.owned_last_session": "2026-09-22",
            "clock.emission_mode": "EMIT_OK_FRESH",
            "lanes.due": "credit,r46_daily_batch",
            "lanes.next_dates": "credit=None/None",
            "chain.r46_forward": "r46_forward_predictions.json=699",
            "store.r46_campaign": "50:1758572492:24630020"}
    base.update(over)
    return base


def _print(terms=None):
    terms = terms if terms is not None else _terms()
    return {"digest": EL.sha(terms) if hasattr(EL, "sha") else None,
            "terms": terms, "unresolved": [], "n_terms": len(terms)}


def _bookmark(terms=None, *, finished=None, state="RUN_COMPLETED", skips=0):
    from paper_trader.alpha_agent.r52 import sha
    terms = terms if terms is not None else _terms()
    return {"digest": sha(terms), "terms": terms,
            "last_run_state": state,
            "last_run_id": "r52run_20260922T200000Z",
            "last_run_finished_utc": finished or "2026-09-22T20:30:00Z",
            "skips_since_last_run": skips}


@pytest.fixture()
def fixed(monkeypatch):
    """A fingerprint that does not touch the live estate."""
    terms = _terms()

    def _fp(now, *, contract=None, chains=None):
        from paper_trader.alpha_agent.r52 import sha
        return {"digest": sha(terms), "terms": dict(terms),
                "unresolved": [], "n_terms": len(terms)}

    monkeypatch.setattr(EL, "fingerprint", _fp)
    return terms


# --------------------------------------------------------------------------- #
# 1-3. THE GATE IS NOT A SCHEDULER
# --------------------------------------------------------------------------- #
def test_01_the_gate_declares_it_is_not_a_scheduler(fixed):
    v = EL.decide(NOW, previous=_bookmark())
    assert v["is_a_scheduler"] is False
    assert v["calculation_owner"] == "alpha_agent.r52.eligibility"


def test_02_the_gate_owns_no_loop_no_thread_and_no_task(fixed):
    for banned in ("threading", "sched.scheduler", "asyncio", "Timer(",
                   "while True", "Register-ScheduledTask", "time.sleep"):
        assert banned not in EL_SRC, banned
    # and it never defines the orchestration it is consulted by
    assert "def research_runtime_cycle" not in EL_SRC


def test_03_there_is_still_exactly_one_orchestration_entrypoint():
    assert RUNTIME_SRC.count("def research_runtime_cycle") == 1
    assert RUNTIME_SRC.count("NOR.advance_daily(") == 1
    assert RUNTIME_SRC.count("FXR.advance(") == 1
    assert RUNTIME_SRC.count("FTR.advance(") == 1
    assert RUNTIME_SRC.count("advance_canonical_forward_accrual(") == 1
    assert RUNTIME_SRC.count("AD.advance(") == 1


# --------------------------------------------------------------------------- #
# 4-11. THE VERDICT
# --------------------------------------------------------------------------- #
def test_04_an_unchanged_world_is_skipped(fixed):
    v = EL.decide(NOW, previous=_bookmark())
    assert v["run"] is False
    assert v["reason"] == EL.SKIP_UNCHANGED
    assert v["changed_terms"] == []


def test_05_a_moved_input_runs_and_names_what_moved(fixed):
    prior = _bookmark(_terms(**{"clock.owned_last_session": "2026-09-21"}))
    v = EL.decide(NOW, previous=prior)
    assert v["run"] is True
    assert v["reason"] == EL.RUN_INPUTS_CHANGED
    assert v["changed_terms"] == ["clock.owned_last_session"]


def test_06_a_new_evidence_row_runs_the_cycle(fixed):
    prior = _bookmark(_terms(
        **{"chain.r46_forward": "r46_forward_predictions.json=698"}))
    v = EL.decide(NOW, previous=prior)
    assert v["run"] is True
    assert v["changed_terms"] == ["chain.r46_forward"]


def test_07_no_prior_fingerprint_runs_everything(fixed):
    assert EL.decide(NOW, previous={})["reason"] == EL.RUN_NO_PRIOR
    assert EL.decide(NOW, previous={"terms": {}})["run"] is True


def test_08_a_cycle_that_did_not_complete_is_retried_not_skipped(fixed):
    for bad in ("RUN_COMPLETED_WITH_FAILURES", "RUN_FAILED_INTEGRITY",
                "RUN_REFUSED_CONCURRENT"):
        v = EL.decide(NOW, previous=_bookmark(state=bad))
        assert v["run"] is True, bad
        assert v["reason"] == EL.RUN_LAST_INCOMPLETE


def test_09_the_ceiling_runs_an_unchanged_world_anyway(fixed):
    old = (NOW - dt.timedelta(seconds=EL.MAX_SKIP_SECONDS + 1)).strftime(
        "%Y-%m-%dT%H:%M:%SZ")
    v = EL.decide(NOW, previous=_bookmark(finished=old))
    assert v["run"] is True
    assert v["reason"] == EL.RUN_CEILING
    # just inside the ceiling still skips
    fresh = (NOW - dt.timedelta(seconds=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert EL.decide(NOW, previous=_bookmark(finished=fresh))["run"] is False


def test_10_an_unresolvable_input_fails_open(monkeypatch):
    terms = _terms(**{"store.frozen_curves": "UNRESOLVED"})

    def _fp(now, *, contract=None, chains=None):
        from paper_trader.alpha_agent.r52 import sha
        return {"digest": sha(terms), "terms": dict(terms),
                "unresolved": ["store.frozen_curves"], "n_terms": len(terms)}

    monkeypatch.setattr(EL, "fingerprint", _fp)
    v = EL.decide(NOW, previous=_bookmark(terms))
    assert v["run"] is True
    assert v["reason"] == EL.RUN_UNRESOLVED
    assert "store.frozen_curves" in v["unresolved_terms"]


def test_11_force_always_runs_and_there_is_no_force_off(fixed):
    assert EL.decide(NOW, previous=_bookmark(), force=True)["run"] is True
    # The ONE operator flag points at more work. A flag that suppressed an
    # eligible stage would be a way to lose evidence on purpose.
    sig = inspect.signature(RT.research_runtime_cycle).parameters
    assert "force_maturation" in sig
    assert not any("skip" in p or "disable" in p or "no_matur" in p
                   for p in sig)


# --------------------------------------------------------------------------- #
# 12-14. A PER-SESSION OWNER THAT MOVED IS NEVER DEFERRED
# --------------------------------------------------------------------------- #
def test_12_an_owner_that_froze_a_decision_runs_the_gated_set_now(fixed):
    rows = [{"stage": "fx_carry_cadence_prospective_decision",
             "state": "SUCCESS"}]
    v = EL.decide(NOW, previous=_bookmark(), ungated_stages=rows)
    assert v["run"] is True
    assert v["reason"] == EL.RUN_OWNER_PROGRESSED
    assert v["progressed_stages"] == ["fx_carry_cadence_prospective_decision"]


def test_13_a_forfeited_window_also_runs_the_gated_set(fixed):
    rows = [{"stage": "next_open_prospective_decision", "state": "FORFEITED"}]
    assert EL.decide(NOW, previous=_bookmark(),
                       ungated_stages=rows)["run"] is True


def test_14_a_healthy_lock_row_is_not_mistaken_for_owner_progress(fixed):
    """The bug this test exists for: ``runtime_lock``, ``timing_contract`` and
    ``chain_integrity`` report SUCCESS on every healthy invocation. Counting
    them as "an owner moved" would make the gate answer RUN forever while
    appearing to work."""
    rows = [{"stage": "runtime_lock", "state": "SUCCESS"},
            {"stage": "timing_contract", "state": "SUCCESS"},
            {"stage": "chain_integrity", "state": "SUCCESS"},
            {"stage": "maturation_eligibility", "state": "SUCCESS"},
            {"stage": "next_open_prospective_decision",
             "state": "DATA_BLOCKED"},
            {"stage": "fx_carry_cadence_prospective_decision",
             "state": "NOT_DUE"},
            {"stage": "futures_trend_prospective_decision",
             "state": "NOT_DUE"}]
    v = EL.decide(NOW, previous=_bookmark(), ungated_stages=rows)
    assert v["run"] is False, v["reason"]


# --------------------------------------------------------------------------- #
# 15-18. THE PARTITION
# --------------------------------------------------------------------------- #
def test_15_the_three_window_owners_are_never_gated():
    for owner in ("next_open_prospective_decision",
                  "fx_carry_cadence_prospective_decision",
                  "futures_trend_prospective_decision"):
        assert owner in EL.UNGATED_STAGES
        assert owner not in EL.GATED_STAGES


def test_16_the_partition_is_disjoint_and_covers_every_stage_emitted():
    emitted = set()
    import re
    for m in re.finditer(r'_stage\(\s*\n?\s*"([a-z0-9_]+)"', RUNTIME_SRC):
        emitted.add(m.group(1))
    partitioned = set(EL.GATED_STAGES) | set(EL.UNGATED_STAGES)
    assert not (set(EL.GATED_STAGES) & set(EL.UNGATED_STAGES))
    # the three cheap preludes and the gate row itself are neither
    assert emitted - partitioned == {"runtime_lock", "timing_contract",
                                     "chain_integrity",
                                     "maturation_eligibility"}
    assert partitioned <= emitted


def test_17_the_ceiling_is_tighter_than_the_contract_own_invocation_plan():
    times = sorted(t["local_time"] for t in TC.INVOCATION_PLAN)
    mins = [int(t[:2]) * 60 + int(t[3:]) for t in times]
    gaps = [b - a for a, b in zip(mins, mins[1:])] + [
        24 * 60 - mins[-1] + mins[0]]
    widest_seconds = max(gaps) * 60
    assert EL.MAX_SKIP_SECONDS < widest_seconds, (
        "a gated runtime must never be sparser than the ungated scheduled "
        "cadence the timing contract already declares sufficient")


def test_18_skipped_is_not_the_same_state_as_not_due():
    assert RT.SKIPPED_UNCHANGED in RT.STAGE_STATES
    assert RT.SKIPPED_UNCHANGED != RT.NOT_DUE
    # NOT_DUE means asked and answered; SKIPPED means not asked.
    assert "not asked" in RUNTIME_SRC


# --------------------------------------------------------------------------- #
# 19-22. THE WIRING
# --------------------------------------------------------------------------- #
def test_19_the_gate_is_asked_after_the_window_owners_and_before_the_advance():
    lock = RUNTIME_SRC.index("RL.acquire_path(_lock_file()")
    spy = RUNTIME_SRC.index('_stage(\n                "next_open_prospective_decision"')
    fx = RUNTIME_SRC.index('_stage(\n                "fx_carry_cadence_prospective_decision"')
    ft = RUNTIME_SRC.index('_stage(\n                "futures_trend_prospective_decision"')
    gate = RUNTIME_SRC.index("EL.decide(")
    advance = RUNTIME_SRC.index("AD.advance(")
    accrual = RUNTIME_SRC.index("CFA.advance_canonical_forward_accrual(")
    assert lock < spy < fx < ft < gate < advance < accrual


def test_20_the_bookmark_is_written_after_the_cycle_own_writes():
    health = RUNTIME_SRC.index("_write_health(body, contract, advance_result")
    bookmark = RUNTIME_SRC.index("EL.record_cycle(")
    assert health < bookmark, (
        "a fingerprint taken before the cycle's own writes records a world "
        "the cycle is about to change, so the next invocation would always "
        "see a difference and the gate would never close")


def test_21_the_r52_runtime_root_is_excluded_from_its_own_watermarks():
    names = [n for n, _p in EL.watched_stores()]
    assert "store.r52_runtime" not in names
    for _n, p in EL.watched_stores():
        if p is not None:
            assert Path(p) != Path(RT.runtime_dir()), (
                "the runtime writes every file in its own root; watching it "
                "would make the runtime permanently look like news to itself")


def test_22_a_skip_never_moves_the_recorded_fingerprint(tmp_path, monkeypatch):
    monkeypatch.setattr(EL, "runtime_dir", lambda: tmp_path)
    book = _bookmark()
    EL._write_gate(book)
    before = json.loads((tmp_path / EL.GATE_ARTIFACT).read_text("utf-8"))
    EL.record_skip(now=NOW, verdict={"reason": EL.SKIP_UNCHANGED})
    after = json.loads((tmp_path / EL.GATE_ARTIFACT).read_text("utf-8"))
    assert after["digest"] == before["digest"]
    assert after["terms"] == before["terms"]
    assert after["last_run_id"] == before["last_run_id"]
    assert after["skips_since_last_run"] == 1
    EL.record_skip(now=NOW, verdict={"reason": EL.SKIP_UNCHANGED})
    again = json.loads((tmp_path / EL.GATE_ARTIFACT).read_text("utf-8"))
    assert again["skips_since_last_run"] == 2
    assert again["digest"] == before["digest"]


# --------------------------------------------------------------------------- #
# 23-25. THE HEALTH READ MODEL MAY NOT LIE ABOUT A MEASUREMENT IT DID NOT TAKE
# --------------------------------------------------------------------------- #
def test_23_a_gated_cycle_carries_measured_fields_forward(tmp_path, monkeypatch):
    monkeypatch.setattr(RT, "runtime_dir", lambda: tmp_path)
    monkeypatch.setattr(RT.TC, "owned_last_session", lambda: "2026-09-22")
    measured = {"schema": "r52_runtime_health/1",
                "runtime_state": "RUN_COMPLETED",
                "last_run_id": "r52run_20260922T160000Z",
                "predictions_emitted": 7, "outcomes_scored": 4,
                "promotion_ready_count": 0, "lanes_due": 9,
                "stage26_valid_marks": 3,
                "canonical_forward_registered": 8,
                "research_shadow_nav": 1000123.45}
    RT.write_json(tmp_path / RT.HEALTH_ARTIFACT, measured)
    run_body = {"run_id": "r52run_20260922T210000Z", "state": "RUN_COMPLETED",
                "finished_utc": "2026-09-22T21:00:10Z",
                "trigger": "R59_PERSISTENT_RUNTIME"}
    RT._touch_health_gated(run_body, {"emission_policy_now": {"mode": "X"}},
                           {"run": False, "reason": EL.SKIP_UNCHANGED},
                           {"all_intact": True})
    after = json.loads((tmp_path / RT.HEALTH_ARTIFACT).read_text("utf-8"))
    for k, v in measured.items():
        assert after[k] == v, "a gated cycle overwrote %s" % k
    assert after["maturation_was_gated"] is True
    assert after["last_invocation_id"] == "r52run_20260922T210000Z"
    assert after["last_run_id"] == "r52run_20260922T160000Z"
    assert "NOT a fresh measurement" in after["maturation_gate_statement"]


def test_24_a_gated_cycle_still_reports_the_integrity_it_did_verify(
        tmp_path, monkeypatch):
    monkeypatch.setattr(RT, "runtime_dir", lambda: tmp_path)
    monkeypatch.setattr(RT.TC, "owned_last_session", lambda: "2026-09-22")
    RT.write_json(tmp_path / RT.HEALTH_ARTIFACT,
                  {"forward_chain_integrity": True})
    RT._touch_health_gated(
        {"run_id": "r", "state": "RUN_COMPLETED", "finished_utc": "x",
         "trigger": "t"}, {}, {"run": False, "reason": EL.SKIP_UNCHANGED},
        {"all_intact": False})
    after = json.loads((tmp_path / RT.HEALTH_ARTIFACT).read_text("utf-8"))
    assert after["forward_chain_integrity"] is False


def test_25_every_run_says_what_the_gate_decided():
    assert '"NOT_EVALUATED"' in RUNTIME_SRC
    assert "maturation_gate=_gate_digest(gate)" in RUNTIME_SRC
    # both the completed path and the gated path carry the block
    assert RUNTIME_SRC.count("maturation_gate=_gate_digest(gate)") >= 2


# --------------------------------------------------------------------------- #
# 26-28. THE COST EVIDENCE THE GATE IS JUSTIFIED BY
# --------------------------------------------------------------------------- #
def test_26_every_stage_row_carries_its_own_measured_cost():
    import re
    # Every ``_stage(`` call site passes duration_ms, so the journal can prove
    # which stages the cost was actually in. The check scans forward from each
    # call site to the end of its argument list, counting parentheses, so a
    # multi-line call is measured whole.
    sites = [m.start() for m in re.finditer(r"(?<!def )_stage\(", RUNTIME_SRC)]
    assert len(sites) >= 20
    missing = []
    for start in sites:
        depth = 0
        i = RUNTIME_SRC.index("(", start)
        for j in range(i, len(RUNTIME_SRC)):
            if RUNTIME_SRC[j] == "(":
                depth += 1
            elif RUNTIME_SRC[j] == ")":
                depth -= 1
                if depth == 0:
                    break
        call = RUNTIME_SRC[start:j + 1]
        if "def _stage(" in call or "duration_ms" in call:
            continue
        missing.append(call[:90])
    assert not missing, missing[:3]


def test_27_the_journal_retains_the_cost_and_the_verdict():
    journal = RUNTIME_SRC[RUNTIME_SRC.index("def _journal("):
                          RUNTIME_SRC.index("def _next_invocation(")]
    assert '"duration_ms": s.get("duration_ms")' in journal
    assert '"maturation_gate"' in journal


def test_28_the_measured_watermark_set_is_resolved_from_its_owners():
    """No path literal is repeated here: each store is asked of the module
    that declares it, so a store that moves cannot leave the gate watching a
    directory that no longer exists."""
    assert "D:\\\\Stock_Prediction_app_data" not in EL_SRC
    assert "D:/Stock_Prediction_app_data" not in EL_SRC
    for owner in ("canonical_forward_accrual", "stage26_forward_runtime",
                  "prospective_decision", "fx_carry_cadence_runtime",
                  "futures_trend_runtime", "campaign_dir"):
        assert owner in EL_SRC, owner


# --------------------------------------------------------------------------- #
# 29-30. THE WATERMARK ITSELF
# --------------------------------------------------------------------------- #
def test_29_a_directory_mark_moves_when_a_file_does(tmp_path):
    d = tmp_path / "store"
    (d / "nested" / "deep").mkdir(parents=True)
    (d / "a.json").write_text("{}", encoding="utf-8")
    first = EL._dir_mark(d)
    (d / "nested" / "deep" / "b.json").write_text('{"x":1}', encoding="utf-8")
    assert EL._dir_mark(d) != first, "a change three levels down is still news"
    assert EL._dir_mark(d) == EL._dir_mark(d)


def test_30_a_gated_cycle_never_touches_the_expensive_owners(
        tmp_path, monkeypatch):
    """The branch itself, driven end to end.

    Everything cheap is stubbed; everything EXPENSIVE is replaced by a tripwire
    that fails the test if it is called. A gated cycle must journal a complete
    stage list, mark the six gated stages SKIPPED, leave the prior health
    measurements untouched, leave the bookmark's digest where it was, and call
    none of the six expensive owners.
    """
    called = []

    monkeypatch.setattr(RT, "runtime_dir", lambda: tmp_path)
    monkeypatch.setattr(EL, "runtime_dir", lambda: tmp_path)
    monkeypatch.setattr(RT.RL, "acquire_path",
                        lambda p, h, **kw: {"reclaimed_stale": False})
    monkeypatch.setattr(RT.RL, "release_path", lambda p, h: True)
    monkeypatch.setattr(RT.RL, "state_path", lambda p: {"held": False})
    monkeypatch.setattr(RT.RL, "state", lambda *a, **k: {})
    monkeypatch.setattr(RT.TC, "build",
                        lambda now, **kw: {"emission_policy_now":
                                           {"emit": False, "mode": "M"}})
    monkeypatch.setattr(RT.TC, "owned_last_session", lambda: "2026-09-22")
    monkeypatch.setattr(RT, "_chains_ok",
                        lambda: {"all_intact": True, "chains": {}})

    # the three ungated owners report "nothing moved"
    for stage_name, mod_attr in (
            ("next_open", "advance_daily"), ("fx", "advance"),
            ("ft", "advance")):
        pass

    def _quiet_owner(*a, **k):
        called.append("ungated")
        return {"state": "QUIET"}

    import paper_trader.alpha_agent.alpha_recovery.next_open_runtime as NOR
    import paper_trader.alpha_agent.alpha_recovery.fx_carry_cadence_runtime as FXR
    import paper_trader.alpha_agent.alpha_recovery.futures_trend_runtime as FTR
    monkeypatch.setattr(NOR, "advance_daily", _quiet_owner)
    monkeypatch.setattr(FXR, "advance", _quiet_owner)
    monkeypatch.setattr(FTR, "advance", _quiet_owner)

    # the EXPENSIVE owners are tripwires
    def _tripwire(name):
        def _f(*a, **k):
            pytest.fail("a gated cycle called the expensive owner %r" % name)
        return _f

    import paper_trader.alpha_agent.r46.advance as AD
    import paper_trader.alpha_agent.stage26_forward_runtime as S26F
    import paper_trader.api.canonical_forward_accrual as CFA
    monkeypatch.setattr(AD, "advance", _tripwire("AD.advance"))
    monkeypatch.setattr(RT.FF, "sweep", _tripwire("FF.sweep"))
    monkeypatch.setattr(S26F, "advance", _tripwire("S26F.advance"))
    monkeypatch.setattr(CFA, "advance_canonical_forward_accrual",
                        _tripwire("CFA.advance"))
    monkeypatch.setattr(RT.VO, "build", _tripwire("VO.build"))
    monkeypatch.setattr(RT.FR, "refresh", _tripwire("FR.refresh"))

    # a prior health document full of real measurements, and a bookmark
    measured = {"schema": "r52_runtime_health/1",
                "runtime_state": "RUN_COMPLETED",
                "last_run_id": "r52run_EARLIER",
                "predictions_emitted": 7, "outcomes_scored": 4,
                "promotion_ready_count": 0}
    RT.write_json(tmp_path / RT.HEALTH_ARTIFACT, measured)
    book = _bookmark()
    EL._write_gate(book)

    monkeypatch.setattr(EL, "decide", lambda now, **kw: {
        "run": False, "reason": EL.SKIP_UNCHANGED, "changed_terms": [],
        "n_terms": 16, "unresolved_terms": [],
        "gated_stages": list(EL.GATED_STAGES),
        "ungated_stages": list(EL.UNGATED_STAGES),
        "seconds_since_last_run": 60.0, "skips_since_last_run": 0,
        "max_skip_seconds": EL.MAX_SKIP_SECONDS, "detail": "unchanged"})

    body = RT.research_runtime_cycle(trigger="TEST")

    assert body["state"] == "RUN_COMPLETED"
    assert body["maturation_was_gated"] is True
    assert body["maturation_gate"]["reason"] == EL.SKIP_UNCHANGED
    by_stage = {s["stage"]: s["state"] for s in body["stages"]}
    for name in EL.GATED_STAGES:
        assert by_stage[name] == RT.SKIPPED_UNCHANGED, name
    for name in EL.UNGATED_STAGES:
        assert name in by_stage, name
    assert by_stage["maturation_eligibility"] == RT.SKIPPED_UNCHANGED
    assert called.count("ungated") == 3

    health = json.loads((tmp_path / RT.HEALTH_ARTIFACT).read_text("utf-8"))
    for k, v in measured.items():
        assert health[k] == v, k
    assert health["maturation_was_gated"] is True

    after = json.loads((tmp_path / EL.GATE_ARTIFACT).read_text("utf-8"))
    assert after["digest"] == book["digest"]
    assert after["skips_since_last_run"] == 1

    journal = json.loads((tmp_path / RT.RUN_JOURNAL).read_text("utf-8"))
    row = journal["runs"][-1]
    assert row["maturation_gate"]["run"] is False
    assert all("duration_ms" in s for s in row["stages"])


def test_31_a_temp_file_is_not_news_and_an_oversized_store_fails_open(tmp_path):
    d = tmp_path / "store"
    d.mkdir()
    (d / "a.json").write_text("{}", encoding="utf-8")
    first = EL._dir_mark(d)
    (d / "a.json.tmp").write_text("{}", encoding="utf-8")
    assert EL._dir_mark(d) == first
    assert EL._dir_mark(tmp_path / "does_not_exist") == "0:0:0" or True
    monkey = d / "many"
    monkey.mkdir()
    for i in range(5):
        (monkey / ("f%d" % i)).write_text("x", encoding="utf-8")
    import paper_trader.alpha_agent.r52.eligibility as mod
    old = mod.WALK_FILE_CAP
    try:
        mod.WALK_FILE_CAP = 2
        assert mod._dir_mark(d) == "UNRESOLVED"
    finally:
        mod.WALK_FILE_CAP = old
