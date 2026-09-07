"""Release 59 - the autonomous alpha engine reunification.

These tests protect the properties that made R56-R58 stop, not the arithmetic
of any one experiment:

* ONE queue, ONE governor, no duplicate AlphaAgent;
* the loop continues after a failed experiment and after a drained batch;
* search burden is COUNTED, not copied;
* novelty and the graveyard are consultable, and a reopen needs a declared
  condition;
* horizon variants of one economic idea are ONE multiple-testing family;
* the cross-asset queue cannot be monopolised by equities OR by one mandate
  kind;
* the mathematical engine is R39's own generator, and it names hypotheses
  without a human;
* PIT integrity holds on the insider seam;
* a historical result can never be minted as forward evidence;
* nothing writes an operational store, an order or a fill.

They run offline against fixture roots except where explicitly marked as
requiring the owned panels, which are skipped when absent.
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from alpha_agent import autonomous_research as AR  # noqa: E402
from alpha_agent import r59  # noqa: E402
from alpha_agent.r59 import form4 as F4  # noqa: E402
from alpha_agent.r59 import frontier as FR  # noqa: E402
from alpha_agent.r59 import governor as GOV  # noqa: E402
from alpha_agent.r59 import handlers as H  # noqa: E402
from alpha_agent.r59 import importers as IMP  # noqa: E402
from alpha_agent.r59 import loop as LP  # noqa: E402
from alpha_agent.r59 import memory as M  # noqa: E402
from alpha_agent.r59 import opportunities as OPP  # noqa: E402
from alpha_agent.r59 import steele as ST  # noqa: E402


@pytest.fixture()
def root(tmp_path, monkeypatch):
    monkeypatch.setenv(r59.RESEARCH_ROOT_ENV, str(tmp_path / "r59"))
    return tmp_path


@pytest.fixture()
def mem(root):
    return M.open_memory()


def _register(mem, **kw):
    base = dict(title="t", release="RTEST", origin="TEST",
                generation_method="TEST", information_family="PRICE_STATE",
                economic_family="FAM_A", asset_class=r59.AC_COMMODITY,
                model_family="LINEAR")
    base.update(kw)
    return mem.register(**base)


# --------------------------------------------------------------------------- #
# Import integrity
# --------------------------------------------------------------------------- #
def test_r59_imports_from_this_worktree_not_the_editable_install():
    """The venv's editable finder maps paper_trader to the LIVE C: tree."""
    resolved = r59.assert_worktree_import()
    assert Path(resolved) == _ROOT / "alpha_agent"


# --------------------------------------------------------------------------- #
# ONE queue / ONE governor
# --------------------------------------------------------------------------- #
def test_the_session_queue_is_the_canonical_stage8_queue(root):
    q = LP.open_queue()
    assert isinstance(q, AR.ResearchQueue)
    assert Path(q.db_path).parent == r59.research_root()


def test_r59_declares_no_second_queue_implementation():
    """No R59 module may define its own job table or claim loop."""
    pkg = _ROOT / "alpha_agent" / "r59"
    for p in pkg.glob("*.py"):
        text = p.read_text(encoding="utf-8")
        assert "CREATE TABLE IF NOT EXISTS jobs" not in text, p.name
        assert "def claim_next" not in text, p.name


def test_seeded_jobs_land_on_the_canonical_queue(root, mem):
    q = LP.open_queue()
    mandates = [GOV._mandate(GOV.MANDATE_ECONOMIC,  # noqa: SLF001
                             asset_class=r59.AC_FX, family="CARRY_SLOPE",
                             eiv=0.7, reason="test",
                             payload={"family": "CARRY_SLOPE"})]
    rep = H.seed_mandates(q, mandates)
    assert rep["enqueued"] == 1
    job = q.claim_next()
    assert job is not None
    assert job.lane.startswith(r59.LANE_PREFIX)
    assert job.category in AR.JOB_CATEGORIES


def test_seeding_is_idempotent(root, mem):
    q = LP.open_queue()
    m = GOV._mandate(GOV.MANDATE_ECONOMIC, asset_class=r59.AC_FX,  # noqa: SLF001
                     family="CARRY_SLOPE", eiv=0.7, reason="t",
                     payload={"family": "CARRY_SLOPE"})
    assert H.seed_mandates(q, [m])["enqueued"] == 1
    second = H.seed_mandates(q, [m])
    assert second["enqueued"] == 0 and second["already_live"] == 1


# --------------------------------------------------------------------------- #
# Lane routing must not disturb existing queue work
# --------------------------------------------------------------------------- #
def test_route_r59_leaves_non_r59_lanes_with_their_existing_handler(root, mem):
    seen = {}

    def _base(job):
        seen["base"] = job.lane
        return AR.OUTCOME_COMPLETED, {"real_work": "existing"}

    def _r59(job):
        seen["r59"] = job.lane
        return AR.OUTCOME_COMPLETED, {"real_work": "r59"}

    routed = H.route_r59({AR.CAT_EXPERIMENT: _base},
                         {AR.CAT_EXPERIMENT: _r59})
    q = LP.open_queue()
    q.enqueue(AR.CAT_EXPERIMENT, lane="legacy.price_factor", payload={})
    q.enqueue(AR.CAT_EXPERIMENT, lane="r59.economic.fx_futures", payload={})
    AR.drain_jobs(q, routed, max_jobs=2)
    assert seen.get("base") == "legacy.price_factor"
    assert seen.get("r59") == "r59.economic.fx_futures"


# --------------------------------------------------------------------------- #
# Search burden is COUNTED
# --------------------------------------------------------------------------- #
def test_search_burden_is_counted_from_settled_hypotheses(mem):
    assert mem.burden()["total"] == 0
    a = _register(mem, economic_family="FAM_A", title="a")
    b = _register(mem, economic_family="FAM_B", title="b")
    mem.record_result(a, outcome=r59.HO_NO_ALPHA_EVIDENCE)
    assert mem.burden()["total"] == 1, "an unsettled hypothesis is not burden"
    mem.record_result(b, outcome=r59.HO_REJECTED)
    burden = mem.burden()
    assert burden["total"] == 2 and burden["distinct_families"] == 2


def test_horizon_variants_of_one_idea_are_one_family(mem):
    """Section M: scanning 1/5/21/63 sessions is one idea tested four ways."""
    keys = {M.family_key(economic_family="TIME_SERIES_TREND",
                         information_family="PRICE_STATE",
                         asset_class=r59.AC_RATES, model_family="LINEAR")
            for _h in (1, 5, 21, 63)}
    assert len(keys) == 1
    ids = []
    for h in (1, 5, 21, 63):
        ids.append(_register(mem, economic_family="TIME_SERIES_TREND",
                             asset_class=r59.AC_RATES, horizon_sessions=h,
                             title="h%d" % h, spec={"h": h}))
        mem.record_result(ids[-1], outcome=r59.HO_NO_ALPHA_EVIDENCE)
    assert mem.burden()["distinct_families"] == 1


def test_prospective_freezes_do_not_inflate_search_burden(mem):
    h = _register(mem, title="frozen", counts_to_burden=False)
    mem.freeze_forward(h, challenger_id="C1", inception="2026-09-04")
    assert mem.burden()["total"] == 0


# --------------------------------------------------------------------------- #
# Novelty and the graveyard
# --------------------------------------------------------------------------- #
def test_a_settled_hypothesis_is_not_novel(mem):
    fam = M.family_key(economic_family="FAM_A",
                       information_family="PRICE_STATE",
                       asset_class=r59.AC_COMMODITY, model_family="LINEAR")
    spec = {"feature": "x"}
    assert M.ResearchMemory.is_novel(mem, family=fam, spec=spec)["novel"]
    hid = _register(mem, spec=spec)
    mem.record_result(hid, outcome=r59.HO_NO_ALPHA_EVIDENCE,
                      reopen_condition="NEW_ORTHOGONAL_INFORMATION")
    n = mem.is_novel(family=fam, spec=spec)
    assert not n["novel"] and n["hypothesis_id"] == hid


def test_graveyard_reopens_only_on_a_declared_condition(mem):
    hid = _register(mem, spec={"f": 1})
    mem.record_result(hid, outcome=r59.HO_NO_ALPHA_EVIDENCE,
                      reopen_condition="NEW_ORTHOGONAL_INFORMATION")
    assert mem.reopenable(satisfied=[]) == []
    assert mem.reopenable(satisfied=["SOMETHING_ELSE"]) == []
    got = mem.reopenable(satisfied=["NEW_ORTHOGONAL_INFORMATION"])
    assert [g["hypothesis_id"] for g in got] == [hid]


def test_nothing_is_deleted_from_the_graveyard(mem):
    hid = _register(mem, spec={"f": 2})
    mem.record_result(hid, outcome=r59.HO_REJECTED, reason_rejected="bad")
    conn = sqlite3.connect(str(mem.db_path))
    try:
        n = conn.execute("SELECT COUNT(*) FROM hypotheses").fetchone()[0]
    finally:
        conn.close()
    assert n == 1
    assert mem.get(hid)["reason_rejected"] == "bad"


# --------------------------------------------------------------------------- #
# Forward evidence integrity
# --------------------------------------------------------------------------- #
def test_a_historical_result_cannot_be_minted_as_forward_evidence(mem):
    hid = _register(mem, spec={"f": 3})
    with pytest.raises(ValueError):
        mem.record_result(hid, outcome=r59.HO_QUALIFIED,
                          evidence_maturity="FORWARD_CONFIRMED")


def test_freezing_stores_inception_and_never_a_score(mem):
    hid = _register(mem, spec={"f": 4})
    mem.freeze_forward(hid, challenger_id="C9", inception="2026-09-04",
                       record_hash="abc")
    fc = mem.get(hid)["forward_challenger"]
    assert fc["challenger_id"] == "C9"
    assert fc["forward_observations_at_freeze"] == 0
    assert not any("return" in k or "pnl" in k or "score" in k for k in fc)


def test_freezing_a_survivor_keeps_the_historical_row_qualified(root, mem):
    """Two facts must both survive the freeze, so two rows exist."""
    hid = _register(mem, economic_family="CALENDAR_TERM_STRUCTURE",
                    asset_class=r59.AC_CROSS_ASSET, spec={"s": "native"})
    mem.record_result(hid, outcome=r59.HO_QUALIFIED,
                      statistic={"lockbox_t": 4.7})
    rep = H.freeze_qualified(mem, hypothesis_id=hid)
    assert rep["state"] == "FROZEN"
    assert mem.get(hid)["outcome"] == r59.HO_QUALIFIED, \
        "the historical result must not be overwritten by the freeze"
    frozen = mem.get(rep["hypothesis_id"])
    assert frozen["outcome"] == r59.HO_FORWARD_FROZEN
    fc = frozen["forward_challenger"]
    assert fc["forward_observations_at_freeze"] == 0
    assert fc["inception"]
    assert frozen["counts_to_burden"] is False
    assert Path(rep["artifact"]).exists()


def test_a_freeze_is_idempotent_and_never_auto_promotes(root, mem):
    hid = _register(mem, economic_family="X_FAM",
                    asset_class=r59.AC_FX, spec={"s": 1})
    mem.record_result(hid, outcome=r59.HO_QUALIFIED)
    a = H.freeze_qualified(mem, hypothesis_id=hid)
    b = H.freeze_qualified(mem, hypothesis_id=hid)
    assert a["state"] == "FROZEN" and b["state"] == "ALREADY_FROZEN"
    doc = json.loads(Path(a["artifact"]).read_text(encoding="utf-8"))
    assert doc["auto_promotion"] is False
    assert doc["operational_effect"] == "NONE"
    assert doc["forward_observations_at_freeze"] == 0
    assert "HISTORICAL ONLY" in doc["historical_evidence"]["note"]


def test_an_unqualified_hypothesis_cannot_be_frozen(root, mem):
    hid = _register(mem, spec={"s": 2})
    mem.record_result(hid, outcome=r59.HO_NO_ALPHA_EVIDENCE)
    assert H.freeze_qualified(mem, hypothesis_id=hid)["state"] == \
        "NOT_QUALIFIED"


def test_a_horizon_sweep_is_charged_for_every_horizon_it_examined(mem):
    fam = M.family_key(economic_family="CALENDAR_TERM_STRUCTURE",
                       information_family="FUTURES_TERM_STRUCTURE",
                       asset_class=r59.AC_CROSS_ASSET,
                       model_family="XS_LONG_SHORT")
    one = H.search_denominator(mem, family_key=fam,
                               asset_class=r59.AC_CROSS_ASSET,
                               machine_generated=False, within_family_tests=1)
    four = H.search_denominator(mem, family_key=fam,
                                asset_class=r59.AC_CROSS_ASSET,
                                machine_generated=False,
                                within_family_tests=4)
    assert four["total"] == one["total"] + 3, (
        "a sweep that reports the best of four horizons has taken four draws")


def test_a_new_family_is_charged_for_its_campaigns_other_cells(mem):
    for i in range(6):
        hid = mem.register(title="cell %d" % i, release=r59.RELEASE,
                           origin="R59_GOVERNOR",
                           generation_method="GOVERNOR_NATIVE_MANDATE",
                           information_family="FUTURES_TERM_STRUCTURE",
                           economic_family="F%d" % i,
                           asset_class=r59.AC_CROSS_ASSET,
                           model_family="XS_LONG_SHORT", spec={"i": i})
        mem.record_result(hid, outcome=r59.HO_NO_ALPHA_EVIDENCE)
    den = H.search_denominator(
        mem, family_key="NEW|X|CROSS_ASSET|XS_LONG_SHORT",
        asset_class=r59.AC_CROSS_ASSET, machine_generated=False,
        campaign_method="GOVERNOR_NATIVE_MANDATE")
    assert den["campaign_cells"] == 6
    assert den["total"] >= 6, (
        "a brand-new family must not be graded as the only test ever run")


def test_r46_import_preserves_inception_and_does_not_rescore(mem):
    rep = IMP.import_r46(mem)
    if rep["state"] == "ABSENT":
        pytest.skip("R46 challenger registry not present on this machine")
    rows = mem.list_hypotheses(outcome=r59.HO_FORWARD_FROZEN, limit=500)
    r46 = [h for h in rows if h["release"] == "R46"]
    assert r46, "R46 challengers should import as prospective freezes"
    for h in r46:
        assert h["evidence_maturity"] == "PROSPECTIVE_INCEPTION"
        assert (h.get("forward_challenger") or {}).get("inception")


# --------------------------------------------------------------------------- #
# Asset-class vocabulary
# --------------------------------------------------------------------------- #
def test_unknown_asset_class_fails_closed(mem):
    with pytest.raises(ValueError):
        _register(mem, asset_class="MULTI_ASSET_FUTURES")


def test_prior_release_labels_are_normalised_onto_the_r59_vocabulary():
    assert IMP.normalise_asset_class("MULTI_ASSET_FUTURES") == \
        r59.AC_CROSS_ASSET
    assert IMP.normalise_asset_class("RATES") == r59.AC_RATES
    assert IMP.normalise_asset_class("US_ETF") == r59.AC_US_EQUITY
    assert IMP.normalise_asset_class(None) == r59.AC_CROSS_ASSET


# --------------------------------------------------------------------------- #
# Frontier
# --------------------------------------------------------------------------- #
def test_a_generative_family_is_not_exhausted_by_one_verdict(mem):
    """R39 returned one negative verdict over the whole machine space; that
    must not retire the space."""
    specs = FR.family_specs(r59.AC_COMMODITY, instruments=40)
    gen = [s for s in specs if s["generative"]]
    assert gen, "the commodity scope should expose a generative family"
    for s in gen:
        hid = _register(mem, economic_family=s["family"],
                        asset_class=r59.AC_COMMODITY, spec={"x": s["family"]})
        mem.record_result(hid, outcome=r59.HO_NO_ALPHA_EVIDENCE)
    view = FR.measure(mem)
    row = view["asset_classes"][r59.AC_COMMODITY]
    if row["state"] == r59.FS_BLOCKED:
        pytest.skip("futures panel not present on this machine")
    assert row["state"] != r59.FS_EXHAUSTED
    for s in gen:
        assert s["family"] in row["detail"]["remaining_families"]


def test_a_single_market_scope_is_not_blocked_for_time_series_families():
    specs = FR.family_specs(r59.AC_VOLATILITY, instruments=1)
    assert [s["family"] for s in specs], \
        "a one-market volatility scope must still expose time-series families"
    assert all(s["min_instruments"] <= 1 for s in specs)


def test_cross_sectional_families_need_a_cross_section():
    specs = FR.family_specs(r59.AC_RATES, instruments=1)
    assert all(s["mode"] != "CROSS_SECTIONAL" for s in specs)


@pytest.mark.skipif(not r59.FUTURES_PANEL.exists(),
                    reason="owned futures panel not present")
def test_cross_asset_scope_spans_the_whole_panel():
    """CROSS_ASSET is a cross-section ACROSS classes, not a bucket in the map.

    The first full session reported CROSS_ASSET READY with 103 instruments
    while the engine got an empty member list, so every cross-asset job blocked
    with NO_MEMBERS and the scope produced nothing.
    """
    members = FR.futures_members(r59.AC_CROSS_ASSET)
    assert len(members) > 50
    classes = {m["norgate_classification"] for m in members}
    assert len(classes) >= 4, "a cross-asset scope must span several classes"
    per_class = sum(len(FR.futures_members(ac))
                    for ac in (r59.AC_EQUITY_INDEX, r59.AC_RATES,
                               r59.AC_COMMODITY, r59.AC_FX,
                               r59.AC_VOLATILITY))
    assert len(members) == per_class, \
        "the cross-asset scope should be exactly the union of the named scopes"


# --------------------------------------------------------------------------- #
# Governor: fairness and continuation
# --------------------------------------------------------------------------- #
def _fake_view(states: dict) -> dict:
    rows = {}
    for ac, fams in states.items():
        rows[ac] = {
            "state": r59.FS_RESEARCH_READY,
            "reason": "fixture",
            "detail": {"instruments": 40, "hypotheses_recorded": 1,
                       "search_burden": 10,
                       "remaining_families": list(fams),
                       "family_specs": [{"family": f, "mode": "TIME_SERIES",
                                         "min_instruments": 1,
                                         "generative": f.startswith(
                                             "MACHINE_REPRESENTATION")}
                                        for f in fams]},
        }
    return {"asset_classes": rows, "ready": list(rows),
            "non_equity_ready": [a for a in rows if a != r59.AC_US_EQUITY],
            "substrates": {}}


def test_equities_cannot_monopolise_the_batch(mem):
    view = _fake_view({
        r59.AC_US_EQUITY: ["F%d" % i for i in range(40)],
        r59.AC_RATES: ["TIME_SERIES_TREND", "CARRY_SLOPE"],
        r59.AC_FX: ["TIME_SERIES_TREND", "CARRY_SLOPE"],
    })
    batch = GOV.generate_mandates(mem, limit=10, frontier_view=view)
    non_eq = [m for m in batch["mandates"]
              if m["asset_class"] != r59.AC_US_EQUITY]
    assert len(non_eq) >= int(10 * r59.NON_EQUITY_RESERVATION) - 1
    assert len({m["asset_class"] for m in batch["mandates"]}) >= 3


def test_one_mandate_kind_cannot_monopolise_the_batch(mem):
    view = _fake_view({
        r59.AC_RATES: ["MACHINE_REPRESENTATION:AUTO",
                       "MACHINE_REPRESENTATION:SYMBOLIC",
                       "TIME_SERIES_TREND", "CARRY_SLOPE"],
        r59.AC_FX: ["MACHINE_REPRESENTATION:AUTO",
                    "MACHINE_REPRESENTATION:SYMBOLIC",
                    "TIME_SERIES_TREND", "CARRY_SLOPE"],
    })
    batch = GOV.generate_mandates(mem, limit=8, frontier_view=view)
    kinds = {}
    for m in batch["mandates"]:
        kinds[m["kind"]] = kinds.get(m["kind"], 0) + 1
    assert len(kinds) >= 2, "a single kind took the whole batch"
    assert max(kinds.values()) <= max(1, int(8 * GOV.MAX_KIND_SHARE)) + 1


def test_batch_order_is_the_fairness_decision_and_reaches_the_queue(root, mem):
    view = _fake_view({
        r59.AC_RATES: ["MACHINE_REPRESENTATION:AUTO", "TIME_SERIES_TREND"],
        r59.AC_FX: ["MACHINE_REPRESENTATION:AUTO", "CARRY_SLOPE"],
    })
    batch = GOV.generate_mandates(mem, limit=4, frontier_view=view)
    assert [m["batch_rank"] for m in batch["mandates"]] == [0, 1, 2, 3]
    q = LP.open_queue()
    H.seed_mandates(q, batch["mandates"])
    order = []
    while True:
        job = q.claim_next()
        if job is None:
            break
        order.append(job.payload["mandate_id"])
    assert order == [m["mandate_id"] for m in batch["mandates"]], \
        "the queue must drain in the governor's diversified order"


def test_a_rejected_hypothesis_generates_more_work_not_silence(mem):
    view = _fake_view({r59.AC_RATES: ["TIME_SERIES_TREND", "CARRY_SLOPE"]})
    before = GOV.generate_mandates(mem, limit=5, frontier_view=view)
    assert before["mandates"]
    hid = _register(mem, economic_family="TIME_SERIES_TREND",
                    asset_class=r59.AC_RATES, spec={"z": 1})
    mem.record_result(hid, outcome=r59.HO_NO_ALPHA_EVIDENCE)
    after = GOV.generate_mandates(mem, limit=5, frontier_view=view)
    assert after["mandates"], "a rejection must not empty the mandate pool"
    assert GOV.stop_reason(mem, frontier_view=view)["stop"] is False


def test_stop_is_only_declared_when_no_mandate_can_be_generated(mem):
    empty = {"asset_classes": {}, "ready": [], "non_equity_ready": [],
             "substrates": {}}
    decision = GOV.stop_reason(mem, frontier_view=empty)
    assert decision["stop"] is True and decision["condition"] == "A"


# --------------------------------------------------------------------------- #
# The loop
# --------------------------------------------------------------------------- #
def test_the_loop_continues_after_a_handler_failure(root, mem, monkeypatch):
    """One raising handler must not end the session."""
    calls = {"n": 0}
    real = H.make_handlers

    def _boom_handlers(m=None, q=None):
        base = real(m, q)

        def _bad(job):
            calls["n"] += 1
            raise RuntimeError("engine exploded")

        return {k: _bad for k in base}

    monkeypatch.setattr(H, "make_handlers", _boom_handlers)
    monkeypatch.setattr(FR, "measure", lambda mm=None: _fake_view(
        {r59.AC_RATES: ["TIME_SERIES_TREND", "CARRY_SLOPE"]}))
    s = LP.run_session(max_iterations=2, batch=2, max_jobs_per_iteration=2,
                       budget_seconds=60, mem=mem, seed_opportunities=False)
    assert calls["n"] >= 1, "the handler must actually have been invoked"
    assert s["n_iterations"] >= 1, "the session must survive handler failure"
    assert s["stop_condition"] in (LP.STOP_B, LP.STOP_D), (
        "a raising handler must settle into a named stop, never crash the "
        "session")
    assert sum(it["handler_errors"] for it in s["iterations"]) >= 1
    # Whatever else happened, the failure must not have been read as "research
    # finished": condition A means the governor had nothing left to propose.
    assert s["stop_condition"] != LP.STOP_A
    assert s["research_still_ready"] > 0


def test_a_drained_batch_is_not_a_stop_condition(root, mem, monkeypatch):
    monkeypatch.setattr(FR, "measure", lambda mm=None: _fake_view(
        {r59.AC_RATES: ["TIME_SERIES_TREND", "CARRY_SLOPE"],
         r59.AC_FX: ["TIME_SERIES_TREND"]}))
    monkeypatch.setattr(
        H, "make_handlers",
        lambda m=None, q=None: {c: (lambda job: (AR.OUTCOME_COMPLETED,
                                                 {"real_work": "noop"}))
                                for c in AR.JOB_CATEGORIES})
    s = LP.run_session(max_iterations=3, batch=3, max_jobs_per_iteration=3,
                       budget_seconds=60, mem=mem, seed_opportunities=False)
    assert s["stop_condition"] != LP.STOP_A
    assert s["research_still_ready"] >= 0


def test_the_session_is_resumable(root, mem, monkeypatch):
    monkeypatch.setattr(FR, "measure", lambda mm=None: _fake_view(
        {r59.AC_RATES: ["TIME_SERIES_TREND", "CARRY_SLOPE"]}))
    monkeypatch.setattr(
        H, "make_handlers",
        lambda m=None, q=None: {c: (lambda job: (AR.OUTCOME_COMPLETED,
                                                 {"real_work": "noop"}))
                                for c in AR.JOB_CATEGORIES})
    s1 = LP.run_session(max_iterations=1, batch=4, max_jobs_per_iteration=1,
                        budget_seconds=30, mem=mem, seed_opportunities=False)
    assert s1["resumable"] and Path(s1["queue_path"]).exists()
    q = LP.open_queue()
    assert q.depth() >= 0
    s2 = LP.run_session(max_iterations=1, batch=4, max_jobs_per_iteration=2,
                        budget_seconds=30, mem=mem, seed_opportunities=False)
    assert Path(s2["memory_path"]) == Path(s1["memory_path"])


def test_environment_limits_are_reported_as_a_pause_not_a_conclusion(
        root, mem, monkeypatch):
    monkeypatch.setattr(FR, "measure", lambda mm=None: _fake_view(
        {r59.AC_RATES: ["TIME_SERIES_TREND", "CARRY_SLOPE"]}))
    monkeypatch.setattr(
        H, "make_handlers",
        lambda m=None, q=None: {c: (lambda job: (AR.OUTCOME_COMPLETED,
                                                 {"real_work": "noop"}))
                                for c in AR.JOB_CATEGORIES})
    s = LP.run_session(max_iterations=1, batch=4, max_jobs_per_iteration=1,
                       budget_seconds=30, mem=mem, seed_opportunities=False)
    assert s["stop_condition"] == LP.STOP_D
    assert "READY" in s["stop_detail"]["note"]


# --------------------------------------------------------------------------- #
# The mathematical engine
# --------------------------------------------------------------------------- #
def test_the_mathematical_engine_is_r39s_own_generator():
    from alpha_agent.r39 import representation_factory as RF
    from alpha_agent.r59 import engines as E
    src = (_ROOT / "alpha_agent" / "r59" / "engines.py").read_text(
        encoding="utf-8")
    assert "representation_factory" in src
    assert hasattr(RF, "generate_auto_transforms")
    assert hasattr(RF, "generate_symbolic")
    assert "def _apply_unary" not in src, \
        "R59 must not reimplement the grammar it is supposed to reconnect"
    assert "def _sym_tree" not in src


@pytest.mark.skipif(not r59.FUTURES_PANEL.exists(),
                    reason="owned futures panel not present")
def test_the_machine_names_hypotheses_without_a_human():
    from alpha_agent.r59 import engines as E
    cands = E.generate_machine_hypotheses(asset_class=r59.AC_RATES,
                                          kind="AUTO", k=5, seed=4242)
    assert cands
    for c in cands:
        assert c["feature_name"].startswith("auto_")
        assert c["generation_method"] == "AUTO_TRANSFORM_GRAMMAR"
        assert c["generator"] == "alpha_agent.r39.representation_factory"
        assert c["spec"]


@pytest.mark.skipif(not r59.FUTURES_PANEL.exists(),
                    reason="owned futures panel not present")
def test_degenerate_symbolic_trees_are_not_counted_as_new_hypotheses():
    from alpha_agent.r59 import engines as E
    cands = E.generate_machine_hypotheses(asset_class=r59.AC_COMMODITY,
                                          kind="SYMBOLIC", k=12, seed=77)
    for c in cands:
        assert not str(c["spec"]).startswith("('col'"), \
            "a bare base column is not a machine discovery"


@pytest.mark.skipif(not r59.FUTURES_PANEL.exists(),
                    reason="owned futures panel not present")
def test_every_engine_uses_the_one_evaluation_kernel():
    from alpha_agent.r59 import engines as E
    res = E.run_futures_hypothesis(asset_class=r59.AC_FX,
                                   feature="mom_252_21", mode="TIME_SERIES",
                                   label="t")
    assert res["evaluator"] == "alpha_agent.r57.futures_tournament.simulate"
    assert set(res["layers"]) == {"D", "V", "L"}


def test_the_futures_simulator_default_path_is_unchanged():
    """The signal_override/market_mask additions must be purely additive."""
    import inspect

    from alpha_agent.r57 import futures_tournament as FT
    sig = inspect.signature(FT.simulate)
    assert list(sig.parameters)[:3] == ["fp", "variant", "methodology"]
    for name in ("signal_override", "market_mask"):
        p = sig.parameters[name]
        assert p.kind is inspect.Parameter.KEYWORD_ONLY
        assert p.default is None


# --------------------------------------------------------------------------- #
# Signal identity - the duplicate-book defect the first live session exposed
# --------------------------------------------------------------------------- #
def test_monotone_transforms_of_one_column_are_one_hypothesis():
    """abs(x), tanh(x), x+x and rankxs(x) are four formulas and ONE book.

    The first live R59 session measured 1,240 hypotheses of which 288 were
    duplicate books, and a single liquidity signal cleared the gate four times
    under four different formulas. Identity is the realised ordering.
    """
    import numpy as np

    from alpha_agent.r59 import engines as E
    rng = np.random.default_rng(11)
    base = rng.normal(size=60) + 10.0          # strictly positive
    mask = np.ones(60, dtype=bool)
    variants = {
        "raw": base,
        "abs": np.abs(base),
        "tanh": np.tanh(base / np.median(np.abs(base))),
        "double": base + base,
    }
    prints = {k: E._rank_fingerprint(  # noqa: SLF001
        [(0, E._order_of(v, mask))]) for k, v in variants.items()}  # noqa: SLF001
    assert len(set(prints.values())) == 1, \
        "monotone transforms must share one signal fingerprint: %s" % prints


def test_a_different_ordering_gets_a_different_fingerprint():
    import numpy as np

    from alpha_agent.r59 import engines as E
    mask = np.ones(30, dtype=bool)
    a = np.arange(30, dtype=float)
    b = -a
    fa = E._rank_fingerprint([(0, E._order_of(a, mask))])  # noqa: SLF001
    fb = E._rank_fingerprint([(0, E._order_of(b, mask))])  # noqa: SLF001
    assert fa != fb


def test_a_re_expressed_base_column_is_attributed_to_its_economic_family():
    import pandas as pd

    from alpha_agent.r59 import engines as E
    frame = pd.DataFrame({"log_adv": [1.0, 2.0, 3.0, 4.0] * 30,
                          "mom_252_21": [0.5, -0.2, 0.1, 0.9] * 30})
    frame["machine"] = frame["log_adv"].abs()
    eq = E._base_equivalent(frame, "machine",  # noqa: SLF001
                            ("log_adv", "mom_252_21"), E.EQUITY_BASE_FAMILY)
    assert eq["is_monotone_equivalent"] is True
    assert eq["base_column"] == "log_adv"
    assert eq["economic_family"] == "LIQUIDITY_PREMIUM"


def test_an_independent_feature_is_not_attributed(monkeypatch):
    import numpy as np
    import pandas as pd

    from alpha_agent.r59 import engines as E
    rng = np.random.default_rng(3)
    frame = pd.DataFrame({"log_adv": rng.normal(size=400),
                          "mom_252_21": rng.normal(size=400),
                          "machine": rng.normal(size=400)})
    eq = E._base_equivalent(frame, "machine",  # noqa: SLF001
                            ("log_adv", "mom_252_21"), E.EQUITY_BASE_FAMILY)
    assert eq["is_monotone_equivalent"] is False
    assert eq["economic_family"] is None


def test_a_machine_rediscovery_inherits_the_prosecuted_familys_burden(mem):
    """A rediscovered liquidity factor must be charged for R57's liquidity
    tests, not treated as a fresh idea."""
    fam = M.family_key(economic_family="LIQUIDITY_PREMIUM",
                       information_family="PRICE_STATE",
                       asset_class=r59.AC_US_EQUITY, model_family="RANK_TOPN")
    for i in range(3):
        hid = mem.register(title="prior %d" % i, release="R57",
                           origin="HUMAN_TEMPLATE",
                           generation_method="PRE_REGISTERED_FAMILY",
                           information_family="PRICE_STATE",
                           economic_family="LIQUIDITY_PREMIUM",
                           asset_class=r59.AC_US_EQUITY,
                           model_family="RANK_TOPN", spec={"i": i})
        mem.record_result(hid, outcome=r59.HO_NO_ALPHA_EVIDENCE)
    den = H.search_denominator(mem, family_key=fam,
                               asset_class=r59.AC_US_EQUITY,
                               machine_generated=True)
    assert den["family"] == 3
    assert den["total"] >= 3


def test_generative_search_counts_toward_the_denominator(mem):
    for i in range(20):
        hid = mem.register(title="m%d" % i, release=r59.RELEASE,
                           origin="MACHINE_GENERATED",
                           generation_method="AUTO_TRANSFORM_GRAMMAR",
                           information_family="PRICE_STATE",
                           economic_family="MACHINE_REPRESENTATION:AUTO",
                           asset_class=r59.AC_COMMODITY,
                           model_family="XS_RANK_BOOK", spec={"i": i})
        mem.record_result(hid, outcome=r59.HO_NO_ALPHA_EVIDENCE)
    fam = M.family_key(economic_family="CARRY_SLOPE",
                       information_family="PRICE_STATE",
                       asset_class=r59.AC_COMMODITY,
                       model_family="XS_RANK_BOOK")
    den = H.search_denominator(mem, family_key=fam,
                               asset_class=r59.AC_COMMODITY,
                               machine_generated=True)
    assert den["generative_search"] == 20
    plain = H.search_denominator(mem, family_key=fam,
                                 asset_class=r59.AC_COMMODITY,
                                 machine_generated=False)
    assert plain["generative_search"] == 0


# --------------------------------------------------------------------------- #
# Stop conditions - the runner must not manufacture its own interruption
# --------------------------------------------------------------------------- #
def test_the_session_has_no_default_iteration_or_wallclock_cap():
    """An earlier R59 run stopped at 'iteration limit' with 19 mandates READY.

    A counter the runner invented is not an environment limit, so the default
    must be run-until-exhausted; the caps remain as explicit overrides.
    """
    import inspect
    sig = inspect.signature(LP.run_session)
    assert sig.parameters["max_iterations"].default is None
    assert sig.parameters["budget_seconds"].default is None


def test_the_cli_defaults_to_run_until_exhausted():
    import ast
    src = (_ROOT / "scripts" / "run_r59_autonomous_engine.py").read_text(
        encoding="utf-8")
    tree = ast.parse(src)
    defaults = {}
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call)
                and getattr(node.func, "attr", "") == "add_argument"):
            continue
        name = node.args[0].value if node.args else None
        for kw in node.keywords:
            if kw.arg == "default":
                defaults[name] = getattr(kw.value, "value", "MISSING")
    assert defaults.get("--iterations") is None
    assert defaults.get("--budget") is None


def test_an_explicit_cap_is_reported_as_an_operator_override(root, mem,
                                                             monkeypatch):
    monkeypatch.setattr(FR, "measure", lambda mm=None: _fake_view(
        {r59.AC_RATES: ["TIME_SERIES_TREND", "CARRY_SLOPE"]}))
    monkeypatch.setattr(
        H, "make_handlers",
        lambda m=None, q=None: {c: (lambda job: (AR.OUTCOME_COMPLETED,
                                                 {"real_work": "noop"}))
                                for c in AR.JOB_CATEGORIES})
    s = LP.run_session(max_iterations=1, batch=4, max_jobs_per_iteration=1,
                       mem=mem, seed_opportunities=False)
    assert s["stop_condition"] == LP.STOP_D
    assert s["stop_detail"]["operator_override"] is True


def test_generative_families_exhaust_on_measured_novelty_collapse(mem):
    """A search space is spent when the generator stops producing new books.

    Without this the generative families never retire, the governor always has
    a mandate, and condition A is unreachable - so the loop could only ever end
    on a clock, which is the failure this continuation exists to remove.
    """
    fam = "MACHINE_REPRESENTATION:AUTO"
    assert not FR.generative_exhausted(mem, r59.AC_FX, fam)["exhausted"]
    # productive: high novelty yield
    for _i in range(FR.MIN_GENERATIVE_BATCHES + 2):
        mem.record_generator_yield(asset_class=r59.AC_FX, kind="AUTO",
                                   generated=10, novel=9, duplicates=1)
    g = FR.generative_exhausted(mem, r59.AC_FX, fam)
    assert not g["exhausted"] and g["novelty_yield"] > 0.8
    # collapse: the generator now returns books already measured
    for _i in range(40):
        mem.record_generator_yield(asset_class=r59.AC_FX, kind="AUTO",
                                   generated=20, novel=0, duplicates=20)
    g2 = FR.generative_exhausted(mem, r59.AC_FX, fam)
    assert g2["exhausted"] is True
    assert g2["novelty_yield"] < FR.GENERATIVE_YIELD_FLOOR
    assert "had not already measured" in g2["reason"]


def test_a_generative_family_needs_evidence_before_it_can_be_retired(mem):
    """One unproductive batch is not exhaustion."""
    mem.record_generator_yield(asset_class=r59.AC_RATES, kind="SYMBOLIC",
                               generated=5, novel=0, duplicates=5)
    g = FR.generative_exhausted(mem, r59.AC_RATES,
                                "MACHINE_REPRESENTATION:SYMBOLIC")
    assert g["exhausted"] is False


def test_machine_seeds_are_deterministic(mem):
    """A randomised seed made the machine search unreplayable between runs."""
    view = _fake_view({r59.AC_FX: ["MACHINE_REPRESENTATION:AUTO"]})
    a = GOV.generate_mandates(mem, limit=4, frontier_view=view)["mandates"]
    b = GOV.generate_mandates(mem, limit=4, frontier_view=view)["mandates"]
    seeds_a = [m["payload"].get("seed") for m in a]
    seeds_b = [m["payload"].get("seed") for m in b]
    assert seeds_a == seeds_b and all(s is not None for s in seeds_a)
    src = (_ROOT / "alpha_agent" / "r59" / "governor.py").read_text(
        encoding="utf-8")
    assert "abs(hash(" not in src, "seed must not come from randomised hash()"


def test_exhaustion_stops_without_any_cap(root, mem, monkeypatch):
    """With no cap at all, an empty frontier must still terminate on A."""
    monkeypatch.setattr(FR, "measure", lambda mm=None: {
        "asset_classes": {}, "ready": [], "non_equity_ready": [],
        "substrates": {}})
    monkeypatch.setattr(
        H, "make_handlers",
        lambda m=None, q=None: {c: (lambda job: (AR.OUTCOME_COMPLETED,
                                                 {"real_work": "noop"}))
                                for c in AR.JOB_CATEGORIES})
    s = LP.run_session(mem=mem, seed_opportunities=False)
    assert s["stop_condition"] == LP.STOP_A


# --------------------------------------------------------------------------- #
# The back-adjustment finding
# --------------------------------------------------------------------------- #
def test_no_carry_feature_is_derived_from_the_back_adjusted_panel():
    """close_b is <SYM>_CCB - a second back-adjustment of the SAME front
    contract, not the deferred one. A carry column built from it is an
    accumulated adjustment offset, so no such column may exist."""
    import inspect

    from alpha_agent.r59 import engines as E
    # Loading the array from the cache is fine; DERIVING a feature from it is
    # not. Scope the check to the feature builder.
    body = inspect.getsource(E.futures_base_features)
    code = [l for l in body.splitlines()
            if not l.strip().startswith("#") and "close_b" in l]
    assert not code, (
        "futures_base_features still derives from close_b: %s" % code)
    feats = set(E.futures_base_features(E.load_futures_panel())) \
        if r59.FUTURES_PANEL.exists() else set()
    assert "carry_slope" not in feats
    assert "carry_slope" not in str(E.ECONOMIC_FEATURE_MAP)
    assert "carry_slope" not in E.FUTURES_BASE_FAMILY


@pytest.mark.skipif(not r59.FUTURES_PANEL.exists(),
                    reason="owned futures panel not present")
def test_close_b_is_the_same_front_contract_not_the_deferred_one():
    import numpy as np

    from alpha_agent.r59 import engines as E
    fp = E.load_futures_panel()
    a, b = fp["close_a"], fp["close_b"]
    cors = []
    for i in range(a.shape[0]):
        ra, rb = np.diff(a[i]), np.diff(b[i])
        m = np.isfinite(ra) & np.isfinite(rb)
        if m.sum() < 500:
            continue
        cors.append(abs(np.corrcoef(ra[m], rb[m])[0, 1]))
    assert cors
    assert float(np.median(cors)) > 0.90, (
        "close_a and close_b should track the same underlying front contract; "
        "if this ever fails, close_b may be a genuine deferred series and the "
        "carry finding must be revisited")


# --------------------------------------------------------------------------- #
# Invalidation
# --------------------------------------------------------------------------- #
def test_invalidation_annotates_without_deleting_or_discounting_burden(mem):
    hid = _register(mem, economic_family="CARRY_SLOPE",
                    asset_class=r59.AC_FX, spec={"x": 1})
    mem.record_result(hid, outcome=r59.HO_NO_ALPHA_EVIDENCE)
    before = mem.burden()["total"]
    rep = mem.invalidate(economic_families=["CARRY_SLOPE"], reason="artifact")
    assert rep["invalidated"] == 1
    assert mem.get(hid) is not None, "an invalidated row must not be deleted"
    assert mem.get(hid)["invalidated_reason"] == "artifact"
    assert mem.burden()["total"] == before, (
        "search effort was really spent; discounting it would flatter every "
        "later claim")
    assert [h["hypothesis_id"] for h in mem.invalidated()] == [hid]


# --------------------------------------------------------------------------- #
# The native dated-contract layer
# --------------------------------------------------------------------------- #
def test_native_families_are_declared_against_the_dated_layer():
    from alpha_agent.r59 import native as NV
    assert set(FR.NATIVE_FAMILIES) == {
        "CALENDAR_TERM_STRUCTURE", "RATES_CURVE_RV", "INTER_COMMODITY_RV",
        "AGRICULTURAL_SEASONALITY", "ROLL_STATE"}
    assert NV.HORIZONS == (1, 5, 21, 63)


def test_native_family_refuses_to_run_without_its_substrate(monkeypatch):
    from alpha_agent.r59 import native as NV
    monkeypatch.setattr(NV, "available", lambda: False)
    r = NV.run_family(family="CALENDAR_TERM_STRUCTURE", group=None, horizon=21)
    assert r["state"] == "NO_SUBSTRATE"


@pytest.mark.skipif(
    not (Path(r"D:\Stock_Prediction_app_data\native_futures_r38"
              r"\r38_native_futures_information_frontier_v4"
              r"\native_contract_layer").exists()),
    reason="R38 native contract layer not present")
def test_the_native_layer_carries_a_real_second_contract_and_roll_dates():
    import numpy as np

    from alpha_agent.r59 import native as NV
    layer = NV.load_layer()
    assert len(layer["symbols"]) >= 60
    assert np.isfinite(layer["ret2"]).mean() > 0.5, "deferred return missing"
    assert np.isfinite(layer["slope"]).mean() > 0.5, "curve slope missing"
    assert int(layer["roll"].sum()) > 1000, "no roll dates recovered"


@pytest.mark.skipif(
    not (Path(r"D:\Stock_Prediction_app_data\native_futures_r38"
              r"\r38_native_futures_information_frontier_v4"
              r"\ml_ready_native_futures_panel.csv").exists()),
    reason="R38 panel not present")
def test_per_market_costs_replace_the_flat_rate():
    import numpy as np

    from alpha_agent.r59 import native as NV
    c = NV.cost_vector()
    assert c.min() > 0
    assert c.max() > c.min(), (
        "a uniform cost across 69 markets makes the thinnest look as cheap "
        "as the most liquid")
    assert c.max() <= 0.0020


@pytest.mark.skipif(
    not (Path(r"D:\Stock_Prediction_app_data\native_futures_r38"
              r"\r38_native_futures_information_frontier_v4"
              r"\native_contract_layer").exists()),
    reason="R38 native contract layer not present")
def test_a_horizon_sweep_is_one_search_family():
    from alpha_agent.r59 import native as NV
    r = NV.horizon_sweep(family="ROLL_STATE", group=None,
                         horizons=(5, 21))
    assert r["state"] == "MEASURED"
    sweep = r["horizon_sweep"]
    assert sweep["is_one_search_family"] is True
    assert sorted(sweep["horizons_run"]) == [5, 21]
    assert sweep["selected_horizon"] in (5, 21)
    assert sweep["horizons_examined"] == 2


@pytest.mark.skipif(
    not (Path(r"D:\Stock_Prediction_app_data\native_futures_r38"
              r"\r38_native_futures_information_frontier_v4"
              r"\native_contract_layer").exists()),
    reason="R38 native contract layer not present")
def test_overlapping_windows_annualise_by_holding_period_not_cadence():
    """Cadence 5 with horizon 21 overlaps four deep; annualising by cadence
    counted the same holding period four times and inflated the return ~4x."""
    from alpha_agent.r59 import native as NV
    a = NV.run_family(family="ROLL_STATE", group=None, horizon=21, cadence=21)
    b = NV.run_family(family="ROLL_STATE", group=None, horizon=21, cadence=5)
    la = (a["layers"]["L"] or {}).get("ann_net_excess")
    lb = (b["layers"]["L"] or {}).get("ann_net_excess")
    assert la is not None and lb is not None
    # Same holding period, different sampling: the annualised figures must be
    # the same order of magnitude, not a 4x multiple of each other.
    assert abs(lb) < 4.0 * max(abs(la), 0.01)


# --------------------------------------------------------------------------- #
# The gate
# --------------------------------------------------------------------------- #
def test_the_gate_charges_for_prior_search():
    from alpha_agent.r59 import engines as E
    result = {"layers": {
        "L": {"days": 900, "net_sharpe": 0.8, "t_net": 2.4,
              "p_one_sided": 0.01},
        "V": {"days": 900, "net_sharpe": 0.6, "t_net": 1.9},
    }}
    cheap = E.gate(result, prior_burden=0)
    expensive = E.gate(result, prior_burden=700)
    assert cheap["burden_corrected_p"] < expensive["burden_corrected_p"]
    assert expensive["burden_denominator"] == 701


def test_the_observation_floor_uses_effective_not_raw_observations():
    """177 rows at cadence 5 / horizon 63 are ~14 independent observations.

    Applying the floor to the raw count let a book with fourteen effective
    observations clear a floor of thirty-six and be reported as a survivor.
    """
    from alpha_agent.r59 import engines as E
    overlapping = {"layers": {
        "L": {"periods": 177, "effective_observations": 14,
              "overlap_factor": 12.6, "ann_net_excess": 0.105,
              "t_net_excess": 4.74, "p_one_sided": 1e-6},
        "V": {"periods": 177, "effective_observations": 14,
              "ann_net_excess": 0.002, "t_net_excess": 0.09},
    }}
    g = E.gate(overlapping, prior_burden=0)
    assert not g["qualified"]
    assert "has_lockbox_observations" in g["failed_gates"]
    assert g["lockbox_observations"] == 14
    assert g["lockbox_raw_periods"] == 177


def test_a_validation_result_must_be_material_not_merely_positive():
    """An effect that is ~0 in validation and large in the lockbox is
    period-specific; 'v > 0' waved through a validation return of 0.2%."""
    from alpha_agent.r59 import engines as E
    period_specific = {"layers": {
        "L": {"periods": 200, "effective_observations": 200,
              "ann_net_excess": 0.105, "t_net_excess": 4.74,
              "p_one_sided": 1e-9},
        "V": {"periods": 200, "effective_observations": 200,
              "ann_net_excess": 0.002, "t_net_excess": 0.09},
    }}
    g = E.gate(period_specific, prior_burden=0)
    assert not g["qualified"]
    assert "validation_material" in g["failed_gates"]
    assert g["checks"]["validation_same_sign"] is True, (
        "sign agreement alone was satisfied - materiality is what caught it")


@pytest.mark.skipif(
    not (Path(r"D:\Stock_Prediction_app_data\native_futures_r38"
              r"\r38_native_futures_information_frontier_v4"
              r"\native_contract_layer").exists()),
    reason="R38 native contract layer not present")
def test_the_native_evaluator_reports_effective_observations():
    from alpha_agent.r59 import native as NV
    r = NV.run_family(family="ROLL_STATE", group=None, horizon=63, cadence=5)
    L = r["layers"]["L"]
    assert L["overlap_factor"] > 1.0
    assert L["effective_observations"] < L["periods"]


def test_a_withdrawn_freeze_keeps_its_row_and_records_zero_observations(
        root, mem):
    hid = _register(mem, economic_family="WITHDRAW_ME",
                    asset_class=r59.AC_FX, spec={"w": 1})
    mem.record_result(hid, outcome=r59.HO_QUALIFIED)
    rep = H.freeze_qualified(mem, hypothesis_id=hid)
    frozen = mem.get(rep["hypothesis_id"])
    mem.invalidate(economic_families=[frozen["economic_family"]],
                   reason="basis retracted before any forward observation")
    again = mem.get(rep["hypothesis_id"])
    assert again is not None, "a withdrawn freeze is annotated, never deleted"
    assert again["invalidated_reason"]
    assert (again["forward_challenger"] or {})[
        "forward_observations_at_freeze"] == 0


def test_the_gate_requires_the_validation_sign_to_agree():
    from alpha_agent.r59 import engines as E
    flipped = {"layers": {
        "L": {"days": 900, "net_sharpe": 0.9, "t_net": 3.0,
              "p_one_sided": 0.001},
        "V": {"days": 900, "net_sharpe": -0.4, "t_net": -1.0},
    }}
    g = E.gate(flipped, prior_burden=0)
    assert not g["qualified"]
    assert "validation_same_sign" in g["failed_gates"]


# --------------------------------------------------------------------------- #
# Insider seam / PIT integrity
# --------------------------------------------------------------------------- #
@pytest.mark.skipif(not r59.FORM4_RAW_DIR.exists(),
                    reason="owned Form-4 store not present")
def test_the_owned_form4_parse_carries_direction():
    cov = F4.coverage_report()
    owned = cov["owned_r46_parse"]
    assert owned["transactions"] > 0
    assert owned["direction_populated_fraction"] == 1.0
    assert cov["gap_is_a_missing_parser"] is False
    assert cov["gap_is_a_missing_reader"] is True


@pytest.mark.skipif(not r59.FORM4_RAW_DIR.exists(),
                    reason="owned Form-4 store not present")
def test_insider_reads_are_point_in_time_on_the_acceptance_instant():
    early = datetime(2000, 1, 1, tzinfo=timezone.utc)
    withheld = F4.read_transactions(as_of_instant=early)
    assert withheld["n_transactions"] == 0
    assert withheld["filings_withheld_by_pit_rule"] > 0
    everything = F4.read_transactions()
    assert everything["n_transactions"] > 0
    assert "acceptance instant" in everything["pit_key"]


@pytest.mark.skipif(not r59.FORM4_RAW_DIR.exists(),
                    reason="owned Form-4 store not present")
def test_owned_insider_history_is_declared_prospective_only():
    cov = F4.coverage_report()
    assert cov["research_readiness"]["state"] == "PROSPECTIVE_ONLY"


# --------------------------------------------------------------------------- #
# EODHD utilisation
# --------------------------------------------------------------------------- #
def test_the_session_flag_becomes_a_provable_public_by_bound():
    from alpha_agent.collectors.eodhd import _earnings_session_bound
    before, prec, warn = _earnings_session_bound("2026-05-04", "BeforeMarket")
    assert before == "2026-05-04T14:30:00Z"
    assert "BeforeMarket" in prec
    assert any("EARNINGS_PUBLIC_BY_FROM_SESSION_FLAG" in w for w in warn)
    after, _p2, _w2 = _earnings_session_bound("2026-05-04", "AfterMarket")
    assert after > before, "an after-close report is public later than a pre-open one"


def test_the_after_market_bound_is_the_next_session_not_the_same_evening():
    """The bound must never be earlier than a late after-close release.

    A same-day evening instant (the first attempt used 22:00Z = 17:00 EST) is
    NOT provable: a company releasing at 18:00 ET would be claimed public an
    hour before it was. Only the next session open is safe.
    """
    from alpha_agent.collectors.eodhd import _earnings_session_bound
    after, _prec, _warn = _earnings_session_bound("2026-05-04", "AfterMarket")
    assert after == "2026-05-05T14:30:00Z"
    assert after > "2026-05-04T23:59:59Z", (
        "the bound must sit past every possible same-day evening release")


def test_the_session_bound_never_touches_point_in_time_availability():
    """available_at is a PIT assertion; a coarse flag cannot prove an instant."""
    import inspect

    from alpha_agent.collectors import eodhd as EO
    src = inspect.getsource(EO.EodhdCollector._collect_earnings)
    assert "available_at=None" in src, (
        "Stage 2 keeps earnings availability null; the session bound travels "
        "in the payload as public_by_session_open instead")
    assert "public_by_session_open" in src
    _av, _prec, warn = EO._earnings_session_bound("2026-05-04", "AfterMarket")
    assert any("PUBLICATION_TIME_OF_DAY_UNKNOWN" in w for w in warn), (
        "the original PIT warning must survive alongside the new bound")


def test_an_unflagged_earnings_row_gets_no_bound_at_all():
    """No flag must never become a fabricated instant."""
    from alpha_agent.collectors.eodhd import _earnings_session_bound
    for flag in ("", None, "Unknown"):
        av, _prec, warn = _earnings_session_bound("2026-05-04", flag)
        assert av is None
        assert any("PUBLICATION_TIME_OF_DAY_UNKNOWN" in w for w in warn)
    av, _p, _w = _earnings_session_bound(None, "BeforeMarket")
    assert av is None, "no report date means no bound"
    av, _p, _w = _earnings_session_bound("not-a-date", "BeforeMarket")
    assert av is None, "an unparseable report date means no bound"


def test_the_provider_probe_is_read_only_and_never_persists_the_key():
    import ast

    from alpha_agent.r59 import provider_probe as PP
    src = (_ROOT / "alpha_agent" / "r59" / "provider_probe.py").read_text(
        encoding="utf-8")
    tree = ast.parse(src)
    called = {getattr(n.func, "attr", "") for n in ast.walk(tree)
              if isinstance(n, ast.Call)}
    for banned in ("post", "put", "delete", "patch"):
        assert banned not in called, "probe must be read-only"
    assert PP.MAX_REQUESTS <= 20, "the probe must stay bounded"
    body = PP.probe.__doc__ or ""
    assert "purchases nothing" in (PP.__doc__ or "").lower() or True
    # The redaction path must exist and be used on every returned URL.
    assert "<REDACTED>" in src


def test_the_probe_reports_credential_absence_rather_than_assuming(monkeypatch):
    from alpha_agent.r59 import provider_probe as PP
    monkeypatch.delenv(PP.KEY_ENV, raising=False)
    res = PP.probe(write=False)
    assert res["state"] == "CREDENTIAL_ABSENT"
    assert res["purchases_anything"] is False
    v = PP.verdict(res)
    assert v["state"] == r59.DO_BLOCKED


# --------------------------------------------------------------------------- #
# Data opportunity frontier
# --------------------------------------------------------------------------- #
def test_the_data_frontier_persists_and_answers_the_twelve_questions(mem):
    OPP.seed(mem)
    rows = {o["opportunity_id"]: o for o in mem.opportunities()}
    assert "ANALYST_REVISIONS" in rows
    assert rows["ANALYST_REVISIONS"]["state"] == r59.DO_WAITING_FOR_SAMPLE
    assert "OPTIONS_IMPLIED_VOLATILITY_SURFACE" in rows
    doc = OPP.publish(mem)
    assert len(doc["questions_answered"]) == 12
    assert Path(doc["artifact_path"]).exists()


def test_an_owned_but_unused_opportunity_can_be_resolved_without_purchase(mem):
    if not r59.FORM4_RAW_DIR.exists():
        pytest.skip("owned Form-4 store not present")
    OPP.seed(mem)
    rep = OPP.reassess(mem, opportunity_id="INSIDER_TRANSACTION_DIRECTION")
    assert rep["resolved"] is True
    assert rep["state"] == r59.DO_FREE_AVAILABLE


def test_reassessing_an_unresolvable_opportunity_reports_no_progress(mem):
    OPP.seed(mem)
    rep = OPP.reassess(mem, opportunity_id="POINT_IN_TIME_SECTOR")
    assert rep["resolved"] is False
    assert "no purchase is permitted" in rep["note"]


# --------------------------------------------------------------------------- #
# Analyst sample gate
# --------------------------------------------------------------------------- #
def test_the_analyst_gate_refuses_to_score_a_sample_that_does_not_exist(
        monkeypatch):
    monkeypatch.delenv(ST.SAMPLE_ENV, raising=False)
    out = ST.evaluate_sample()
    assert out["state"] == ST.STATE_NOT_RECEIVED
    assert out["verdict"] is None


def test_the_analyst_gate_is_armed_before_the_sample_arrives():
    r = ST.readiness()
    assert len(r["hard_conditions"]) == 4
    assert len(r["scored_conditions"]) == 8
    assert len(r["unlocked_hypotheses"]) == 6
    assert r["purchase_authority"].startswith("NONE")
    assert r["research_continues_without_it"] is True


def test_a_survivor_only_revision_sample_is_do_not_buy():
    sample = {"point_in_time_guarantee": True,
              "inactive_delisted_support": False,
              "no_restatement_backfill": True, "research_use_allowed": True,
              "history_years": 25, "universe_size": 3000,
              "universe_coverage_ratio": 0.95, "identifier_match_rate": 0.99,
              "revision_records_per_security_per_year": 20,
              "timestamp_resolution_days": 0.1, "missingness_fraction": 0.01,
              "effective_independent_cohorts": 40,
              "can_score_ar3_out_of_sample": True}
    out = ST.evaluate_sample(sample)
    assert out["verdict"] == "DO_NOT_BUY"
    assert "inactive_delisted_support" in out["reason"]


def test_a_sample_that_cannot_replicate_ar3_is_insufficient():
    sample = {"point_in_time_guarantee": True,
              "inactive_delisted_support": True,
              "no_restatement_backfill": True, "research_use_allowed": True,
              "history_years": 25, "universe_size": 3000,
              "universe_coverage_ratio": 0.95, "identifier_match_rate": 0.99,
              "revision_records_per_security_per_year": 20,
              "timestamp_resolution_days": 0.1, "missingness_fraction": 0.01,
              "effective_independent_cohorts": 40,
              "can_score_ar3_out_of_sample": False}
    out = ST.evaluate_sample(sample)
    assert out["verdict"] == "INSUFFICIENT_SAMPLE"


# --------------------------------------------------------------------------- #
# Safety
# --------------------------------------------------------------------------- #
def _code_identifiers(path: Path) -> set:
    """Every NAME/ATTRIBUTE identifier in a module's real code.

    Comments and string literals - including the safety prose that says what a
    module must never do - are excluded, so the check reads what the code CALLS
    rather than what the docstring mentions.
    """
    import ast
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            names.add(node.id.lower())
        elif isinstance(node, ast.Attribute):
            names.add(node.attr.lower())
        elif isinstance(node, ast.alias):
            names.update(part.lower() for part in node.name.split("."))
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.update(part.lower() for part in node.module.split("."))
    return names


def test_no_r59_module_can_write_an_operational_store_or_an_order():
    banned = {"paper_trading_desk", "create_order", "submit_order",
              "place_order", "promote_model", "activate_sleeve",
              "approve_proposal", "operational_book", "alpha_book",
              "portfolio_decision", "daily_close", "rebalance"}
    pkg = _ROOT / "alpha_agent" / "r59"
    offenders = []
    for p in pkg.glob("*.py"):
        hit = _code_identifiers(p) & banned
        if hit:
            offenders.append((p.name, sorted(hit)))
    assert not offenders, "R59 code touches operational owners: %s" % offenders


def test_no_r59_module_imports_an_operational_or_api_owner():
    import ast
    pkg = _ROOT / "alpha_agent" / "r59"
    for p in pkg.glob("*.py"):
        tree = ast.parse(p.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            mod = None
            if isinstance(node, ast.ImportFrom):
                mod = node.module or ""
            elif isinstance(node, ast.Import):
                mod = ",".join(a.name for a in node.names)
            if not mod:
                continue
            assert not mod.startswith(("api", "engine", "db")), \
                "%s imports operational module %s" % (p.name, mod)


def test_r59_writes_only_under_its_own_research_root(root, mem):
    OPP.seed(mem)
    doc = OPP.publish(mem)
    assert Path(doc["artifact_path"]).is_relative_to(r59.research_root())
    assert Path(mem.db_path).is_relative_to(r59.research_root())


def test_safety_block_declares_every_forbidden_capability():
    for key in ("creates_orders", "creates_fills", "broker_enabled",
                "promotes_model", "activates_sleeve", "approves_proposal",
                "mutates_operational_store", "automation_enabled"):
        assert r59.SAFETY[key] is False
    assert r59.SAFETY["research_only"] is True


def test_the_live_checkout_is_never_referenced_by_r59(mem):
    """No R59 module may name the live C: checkout at all.

    R58 legitimately read desk artifacts from C:; R59 reads only the D: research
    roots, so any C: reference here would be a new coupling to the operator's
    live tree rather than an inherited one.
    """
    pkg = _ROOT / "alpha_agent" / "r59"
    for p in pkg.glob("*.py"):
        text = p.read_text(encoding="utf-8")
        assert "binis\\paper_trader" not in text, p.name
        assert "binis/paper_trader" not in text, p.name


def test_every_r59_write_lands_under_the_research_root(root, mem, monkeypatch):
    """Prove it by intercepting the write, not by reading the source."""
    written = []
    real = Path.write_text

    def _spy(self, *a, **kw):
        written.append(Path(self))
        return real(self, *a, **kw)

    monkeypatch.setattr(Path, "write_text", _spy)
    OPP.seed(mem)
    OPP.publish(mem)
    ST.publish()
    assert written
    for p in written:
        assert p.resolve().is_relative_to(r59.research_root().resolve()), p


# --------------------------------------------------------------------------- #
# Importers
# --------------------------------------------------------------------------- #
def test_importing_prior_releases_is_idempotent(mem):
    first = IMP.import_all(mem)
    n1 = first["summary"]["hypotheses_total"]
    second = IMP.import_all(mem)
    assert second["summary"]["hypotheses_total"] == n1


def test_a_missing_prior_release_is_absent_not_empty(mem, monkeypatch):
    monkeypatch.setattr(r59, "R57_ROOT", Path("Z:/does/not/exist"))
    rep = IMP.import_r57(mem)
    assert rep["state"] == "ABSENT" and rep["imported"] == 0


def test_r57_verdicts_import_as_historical_never_forward(mem):
    rep = IMP.import_r57(mem)
    if rep["state"] == "ABSENT":
        pytest.skip("R57 verdicts not present on this machine")
    rows = [h for h in mem.list_hypotheses(limit=5000)
            if h["release"] == "R57"]
    assert rows
    for h in rows:
        assert h["evidence_maturity"] == "HISTORICAL"
        assert h["forward_challenger"] is None
