r"""PAPER_TRADER_AGENT_EFFICIENCY_AND_MODEL_ROUTING_V1 - the operational pass.

Hermetic. Every test runs against a research memory inside ``tmp_path``. No
test measures a new alpha experiment, reveals a lockbox that the estate has not
already settled, spends statistical budget or consumes a research campaign.

What is proven here:

    model routing        the committed routing is the routing the installed
                         Claude Code will load, and it is expressed only in
                         fields that Claude Code's own agent schema declares
    context contracts    every brief carries the handoff envelope, stays
                         inside the prose budget, and never carries the estate
    skeptic efficiency   the skeptic's bundle arrives with the machine tests
                         ALREADY measured, and the definition no longer tells
                         it to re-measure
    director efficiency  the director's compact state answers an agenda
                         question without a full memory or census read
    spawn discipline     a role that cannot legally write is not spawned:
                         halted discovery stops downstream work, risk sees
                         only survivors, meta needs a real combination
                         question, publishing needs a clearance
    local execution      every deterministic task names an importable local
                         owner, and no agent is granted a delegation tool
    resume               a campaign resumes from a machine-readable file after
                         a simulated context reset; the transcript is not the
                         system of record
    safety               no new verb, brief or artifact can create an order, a
                         fill, a promotion or an adoption
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from paper_trader.alpha_agent import agents_v2 as A
from paper_trader.alpha_agent import r59
from paper_trader.alpha_agent.agents_v2 import briefs as BR
from paper_trader.alpha_agent.agents_v2 import contracts as C
from paper_trader.alpha_agent.agents_v2 import efficiency as EF
from paper_trader.alpha_agent.agents_v2 import pipeline as P
from paper_trader.alpha_agent.agents_v2 import routing as RT
from paper_trader.alpha_agent.agents_v2 import runner as RUN
from paper_trader.alpha_agent.r59 import memory as M

REPO = Path(__file__).resolve().parents[1]
RUN_ID = "PAPER_TRADER_AGENT_EFFICIENCY_AND_MODEL_ROUTING_V1"
CAMPAIGN = "HERMETIC_EFFICIENCY_ACCEPTANCE"

FUT_COST = {"rate_per_side": 0.0002, "basis": "traded notional"}


# --------------------------------------------------------------------------- #
# Hermetic campaign builders
# --------------------------------------------------------------------------- #
@pytest.fixture()
def pipe(tmp_path, monkeypatch):
    monkeypatch.setenv(r59.RESEARCH_ROOT_ENV, str(tmp_path / "r59_root"))
    mem = M.ResearchMemory(tmp_path / "research_memory.sqlite")
    return P.AgentPipeline(mem, artifact_root=tmp_path / "agents_v2")


def _strong() -> dict:
    return {"D": {"net_sharpe": 0.9, "t_net": 4.0, "days": 1600},
            "V": {"net_sharpe": 0.8, "t_net": 3.0, "days": 1200},
            "L": {"net_sharpe": 1.1, "t_net": 4.2, "p_one_sided": 1e-6,
                  "days": 700}}


def _halts_at_d() -> dict:
    out = _strong()
    out["D"] = {"net_sharpe": 0.0, "t_net": 0.1, "days": 1600}
    return out


def _halts_at_v() -> dict:
    out = _strong()
    out["V"] = {"net_sharpe": 0.0, "t_net": 0.1, "days": 1200}
    return out


def _checks() -> dict:
    return {cid: {"passed": True, "measured": 1.0,
                  "evidence": "runner.adversarial_pack#%s" % cid}
            for cid in ("pit_integrity", "leakage_pass", "placebo_clean",
                        "cost_robust", "subperiod_stable",
                        "not_a_duplicate_identity")}


def _foundation(pipe) -> None:
    pipe.perform(A.DATA_FOUNDATION, "certify_data", dict(
        dataset_id="r38_native_futures_fx", asset_classes=[r59.AC_FX],
        pit_status=P.PIT_SAFE,
        availability_rule="value stamped at the instant it became observable",
        survivorship="expired contracts retained"))
    pipe.perform(A.UNIVERSE, "define_universe", dict(
        universe_id="fx_futures_liquid", dataset_id="r38_native_futures_fx",
        asset_class=r59.AC_FX, execution_representation="LONG_SHORT",
        short_leg_expressible=True,
        rules="liquidity floor from trailing data only"))
    pipe.perform(A.FEATURES, "publish_features", dict(
        feature_set_id="fx_trend_v1", universe_id="fx_futures_liquid",
        features=[{"name": "f1", "lag": 1, "source": "r38_native_futures_fx"}],
        leakage_check="PASS"))


def _prereg(pipe, *, owner: str, family: str, hypothesis: str) -> dict:
    return pipe.perform(A.DIRECTOR, "preregister", dict(
        owning_agent=owner, hypothesis=hypothesis, asset_class=r59.AC_FX,
        family=family, feature_set_id="fx_trend_v1", horizon_sessions=21,
        parameters={"lookback": 63}, long_short=True,
        discovery_sample={"start": r59.DISCOVERY_START,
                          "end": r59.VALIDATION_START},
        evaluation_sample={"validation": r59.VALIDATION_START,
                           "lockbox": r59.LOCKBOX_START},
        cost_model=FUT_COST, expected_sign=1,
        information_family="PRICE_STATE",
        instrument_scope=["&6E", "&6J", "&6B"], venue="RESEARCH"))


def _walk(pipe, owner: str, reg: dict, layers: dict) -> str:
    """D -> V -> L through the governed reveal; stops where it halts."""
    for stage in r59.STAGES:
        out = pipe.perform(owner, "reveal_stage", dict(
            experiment_id=reg["experiment_id"], spec_hash=reg["spec_hash"],
            stage=stage, stats=layers[stage],
            evaluator=RUN.CALCULATION_OWNER))
        if not out["advance"]:
            return stage
    return "L"


def _submit(pipe, owner: str, reg: dict, layers: dict) -> None:
    pipe.perform(owner, "submit_candidate", dict(
        experiment_id=reg["experiment_id"], spec_hash=reg["spec_hash"],
        signal_sign=1, evidence_kind="HISTORICAL", cost_model=FUT_COST,
        turnover=0.20, layers=layers, evaluator=RUN.CALCULATION_OWNER))


def _spec(*ids) -> dict:
    return {"campaign_id": CAMPAIGN,
            "experiments": [{"experiment_id": i, "executor": "e", "book": "b"}
                            for i in ids]}


def _runner_result(eid: str) -> dict:
    """A recorded runner summary, in ``adversarial_pack``'s own shape."""
    return {"experiment_id": eid, "adversarial": {
        "placebo_clean": {"passed": True, "measured": 0.004,
                          "evidence": "cost-neutral placebo, 3 permutations"},
        "cost_robust": {"passed": True, "measured": 0.031,
                        "evidence": "lockbox re-run at 2x cost"},
        "subperiod_stable": {"passed": True, "measured": 0.018,
                             "evidence": "both halves of the lockbox"}}}


# --------------------------------------------------------------------------- #
# A. Model routing
# --------------------------------------------------------------------------- #
def test_01_routing_uses_only_fields_the_local_claude_code_declares():
    declared = set(RT.MODEL_ROUTING_EVIDENCE["agent_frontmatter_fields"])
    for key in RT.ROUTED_KEYS:
        assert key in declared, "%s is not in the agent schema" % key


def test_02_every_role_is_routed_to_a_supported_model_and_effort():
    assert set(RT.ROUTING) == set(A.ROSTER)
    for name, row in RT.ROUTING.items():
        assert row["model"] in RT.SUPPORTED_MODEL_ALIASES, name
        assert row["model"] != "inherit", (
            "%s still inherits the session model" % name)
        assert row["effort"] in RT.SUPPORTED_EFFORTS, name
        assert isinstance(row["maxTurns"], int) and row["maxTurns"] > 0, name


def test_03_the_expensive_model_is_reserved_for_judgment():
    opus = {n for n, r in RT.ROUTING.items() if r["model"] == "opus"}
    assert opus == {A.DIRECTOR, A.SKEPTIC}, (
        "only the agenda owner and the door may use the strongest model")
    for name in (A.DATA_FOUNDATION, A.UNIVERSE, A.FEATURES, A.PUBLISHING):
        assert RT.ROUTING[name]["model"] == "haiku", name
    for name in A.SIGNAL_AGENTS + (A.RISK, A.META):
        assert RT.ROUTING[name]["model"] == "sonnet", name


def test_04_the_committed_definitions_match_the_routing_policy():
    assert RT.routing_problems() == []


def test_05_the_routing_check_bites_on_drift(tmp_path):
    ddir = tmp_path / "agents"
    ddir.mkdir()
    for name in A.ROSTER:
        src = Path(A.AGENT_DEFINITION_DIR) / ("%s.md" % name)
        (ddir / ("%s.md" % name)).write_bytes(src.read_bytes())
    target = ddir / ("%s.md" % A.PUBLISHING)
    target.write_bytes(target.read_bytes().replace(b"model: haiku",
                                                   b"model: opus"))
    problems = RT.routing_problems(ddir)
    assert any(A.PUBLISHING in p and "model" in p for p in problems), problems


def test_06_no_definition_pins_an_unsupported_model(tmp_path):
    ddir = tmp_path / "agents"
    ddir.mkdir()
    for name in A.ROSTER:
        src = Path(A.AGENT_DEFINITION_DIR) / ("%s.md" % name)
        (ddir / ("%s.md" % name)).write_bytes(src.read_bytes())
    target = ddir / ("%s.md" % A.RISK)
    target.write_bytes(target.read_bytes().replace(b"model: sonnet",
                                                   b"model: gpt-4o"))
    assert any("gpt-4o" in p for p in RT.routing_problems(ddir))


def test_07_the_contract_validator_now_owns_routing():
    assert C.validate() == []


# --------------------------------------------------------------------------- #
# B. Context contracts and handoffs
# --------------------------------------------------------------------------- #
def test_08_every_definition_states_its_context_contract():
    for name in A.ROSTER:
        body = C.parse_agent_definition(
            Path(A.AGENT_DEFINITION_DIR) / ("%s.md" % name))["body"]
        assert "## Context contract" in body, name
        assert "PROJECT_STATE.md" in body, (
            "%s does not name the file it must not read" % name)
        assert "omitClaudeMd" in body, (
            "%s does not tell the agent CLAUDE.md is absent" % name)


def test_09_no_definition_inherits_the_project_claude_md():
    for name in A.ROSTER:
        front = C.parse_agent_definition(
            Path(A.AGENT_DEFINITION_DIR) / ("%s.md" % name))["frontmatter"]
        assert front.get("omitClaudeMd") == "true", name


def test_09b_dropping_claude_md_is_safe_because_each_definition_is_complete():
    """``omitClaudeMd: true`` is only safe while the definition carries every
    non-negotiable CLAUDE.md would have supplied. This is the test that makes
    that true rather than hoped for."""
    required = {
        "venv interpreter": r".venv-win\Scripts\python.exe",
        "repository path": r"C:\Users\binis\paper_trader",
        "PowerShell-only rule": "## PowerShell-only rule",
        "no Bash": "no Bash tool",
        "no-orders rule": "## No-production/order/automation rule",
        "research only": "Research only",
    }
    for name in A.ROSTER:
        body = (Path(A.AGENT_DEFINITION_DIR)
                / ("%s.md" % name)).read_text(encoding="utf-8")
        missing = [k for k, v in required.items() if v not in body]
        assert not missing, "%s runs without CLAUDE.md but omits %s" % (
            name, missing)


def test_10_every_brief_carries_the_handoff_envelope(pipe):
    _foundation(pipe)
    reg = _prereg(pipe, owner=A.TREND_BREADTH, family="TREND",
                  hypothesis="FX trend persists over 21 sessions net of cost")
    eid = reg["experiment_id"]
    _walk(pipe, A.TREND_BREADTH, reg, _strong())
    _submit(pipe, A.TREND_BREADTH, reg, _strong())
    spec = _spec(eid)

    built = [
        BR.director_brief(pipe, run_id=RUN_ID, campaign_id=CAMPAIGN, spec=spec),
        BR.foundation_brief(pipe, run_id=RUN_ID, campaign_id=CAMPAIGN,
                            target=A.DATA_FOUNDATION, spec=spec),
        BR.signal_brief(pipe, run_id=RUN_ID, campaign_id=CAMPAIGN,
                        target=A.TREND_BREADTH, spec=spec),
        BR.skeptic_brief(pipe, run_id=RUN_ID, campaign_id=CAMPAIGN,
                         experiment_id=eid, result=_runner_result(eid)),
    ]
    for brief in built:
        assert BR.brief_problems(brief) == [], brief["TARGET_AGENT"]
        for key in BR.ENVELOPE_KEYS:
            assert key in brief


def test_11_a_brief_that_blows_the_prose_budget_is_refused():
    brief = BR.envelope(run_id=RUN_ID, campaign_id=CAMPAIGN,
                        source=A.DIRECTOR, target=A.RISK,
                        decision="rule on it",
                        facts={"essay": "word " * 900})
    problems = BR.brief_problems(brief)
    assert any("prose" in p for p in problems), problems


def test_12_a_signal_agent_receives_only_its_own_experiments(pipe):
    _foundation(pipe)
    mine = _prereg(pipe, owner=A.TREND_BREADTH, family="TREND",
                   hypothesis="FX trend persists over 21 sessions net of cost")
    theirs = _prereg(pipe, owner=A.VOL_LIQUIDITY, family="CARRY",
                     hypothesis="FX forward carry predicts 21-session returns")
    spec = _spec(mine["experiment_id"], theirs["experiment_id"])
    brief = BR.signal_brief(pipe, run_id=RUN_ID, campaign_id=CAMPAIGN,
                            target=A.TREND_BREADTH, spec=spec)
    ids = [e["experiment_id"] for e in brief["FACTS"]["assigned_experiments"]]
    assert ids == [mine["experiment_id"]]
    assert theirs["experiment_id"] not in json.dumps(brief)


# --------------------------------------------------------------------------- #
# C. Skeptic efficiency
# --------------------------------------------------------------------------- #
def test_13_the_skeptic_bundle_arrives_with_the_machine_tests_measured(pipe):
    _foundation(pipe)
    reg = _prereg(pipe, owner=A.TREND_BREADTH, family="TREND",
                  hypothesis="FX trend persists over 21 sessions net of cost")
    eid = reg["experiment_id"]
    _walk(pipe, A.TREND_BREADTH, reg, _strong())
    _submit(pipe, A.TREND_BREADTH, reg, _strong())

    brief = BR.skeptic_brief(pipe, run_id=RUN_ID, campaign_id=CAMPAIGN,
                             experiment_id=eid, result=_runner_result(eid))
    m = brief["METRICS"]
    for layer in ("D_RESULT", "V_RESULT", "L_RESULT"):
        assert m[layer], layer
    for attack in ("PLACEBO_RESULT", "DOUBLE_COST_RESULT", "SUBPERIOD_RESULT"):
        assert m[attack]["passed"] is True, attack
        assert m[attack]["measured"] is not None, attack
    assert m["GATE_RESULT"].get("checks"), "the canonical gate did not rule"
    assert m["BURDEN"].get("total") is not None
    assert brief["SKEPTIC_RECOMPUTES_MACHINE_TESTS"] is False
    assert list(brief["FACTS"]["QUESTIONS"]) == list(BR.SKEPTIC_QUESTIONS)


def test_14_the_skeptic_definition_no_longer_orders_a_re_measurement():
    body = C.parse_agent_definition(
        Path(A.AGENT_DEFINITION_DIR) / ("%s.md" % A.SKEPTIC))["body"]
    assert "to re-measure independently" not in body
    assert "ALREADY MEASURED" in body
    assert "never recompute it" in body


def test_15_the_skeptic_bundle_is_smaller_than_the_result_it_replaces(pipe):
    _foundation(pipe)
    reg = _prereg(pipe, owner=A.TREND_BREADTH, family="TREND",
                  hypothesis="FX trend persists over 21 sessions net of cost")
    eid = reg["experiment_id"]
    _walk(pipe, A.TREND_BREADTH, reg, _strong())
    _submit(pipe, A.TREND_BREADTH, reg, _strong())
    brief = BR.skeptic_brief(pipe, run_id=RUN_ID, campaign_id=CAMPAIGN,
                             experiment_id=eid, result=_runner_result(eid))
    # The committed contract set alone - which the skeptic used to read - is
    # already larger than the whole bundle it now receives.
    contract_bytes = sum(
        (Path(A.CONTRACT_DIR) / n).stat().st_size for n in A.CONTRACT_FILES)
    assert len(json.dumps(brief).encode("utf-8")) < contract_bytes


# --------------------------------------------------------------------------- #
# D. Director efficiency
# --------------------------------------------------------------------------- #
def test_16_the_director_state_is_compact_and_needs_no_full_dump(pipe):
    _foundation(pipe)
    reg = _prereg(pipe, owner=A.TREND_BREADTH, family="TREND",
                  hypothesis="FX trend persists over 21 sessions net of cost")
    brief = BR.director_brief(pipe, run_id=RUN_ID, campaign_id=CAMPAIGN,
                              spec=_spec(reg["experiment_id"]))
    assert brief["FULL_MEMORY_DUMP_REQUIRED"] is False
    assert brief["FULL_CENSUS_READ_REQUIRED"] is False
    assert BR.brief_problems(brief) == []
    facts = brief["FACTS"]
    for key in ("open_information_families", "exhausted_families",
                "available_datasets", "blocked_datasets",
                "queued_hypotheses", "current_campaign"):
        assert key in facts, key
    assert "census" in brief["ARTIFACT_POINTERS"]


def test_17_the_director_brief_points_at_the_census_instead_of_copying_it(pipe):
    brief = BR.director_brief(pipe, run_id=RUN_ID, campaign_id=CAMPAIGN,
                              spec=_spec())
    census = Path(A.CONTRACT_DIR) / BR.CENSUS_FILE
    if census.exists():
        assert (len(json.dumps(brief).encode("utf-8"))
                < census.stat().st_size), "the brief copied the census"


# --------------------------------------------------------------------------- #
# E. Spawn discipline
# --------------------------------------------------------------------------- #
def test_18_a_halted_discovery_does_not_spawn_anything_downstream(pipe):
    _foundation(pipe)
    reg = _prereg(pipe, owner=A.TREND_BREADTH, family="TREND",
                  hypothesis="FX trend persists over 21 sessions net of cost")
    stopped = _walk(pipe, A.TREND_BREADTH, reg, _halts_at_d())
    assert stopped == "D"

    state = BR.campaign_state(pipe, CAMPAIGN, _spec(reg["experiment_id"]))
    plan = RT.spawn_plan(state)
    for role in (A.SKEPTIC, A.RISK, A.META, A.PUBLISHING):
        assert role in plan["skip"], role
    skeptic = next(d for d in plan["roles"] if d["role"] == A.SKEPTIC)
    assert skeptic["reason"] == RT.R_NO_CANDIDATE


def test_19_the_lockbox_is_never_reached_when_validation_did_not_earn_it(pipe):
    _foundation(pipe)
    reg = _prereg(pipe, owner=A.TREND_BREADTH, family="TREND",
                  hypothesis="FX trend persists over 21 sessions net of cost")
    assert _walk(pipe, A.TREND_BREADTH, reg, _halts_at_v()) == "V"
    stages = [e["detail"]["stage"]
              for e in pipe._events(P.EV_STAGE, reg["experiment_id"])]
    assert stages == ["D", "V"], stages
    assert "L" not in stages


def test_20_risk_is_not_spawned_when_the_skeptic_killed_everything(pipe):
    _foundation(pipe)
    reg = _prereg(pipe, owner=A.TREND_BREADTH, family="TREND",
                  hypothesis="FX trend persists over 21 sessions net of cost")
    eid = reg["experiment_id"]
    _walk(pipe, A.TREND_BREADTH, reg, _strong())
    _submit(pipe, A.TREND_BREADTH, reg, _strong())
    pipe.perform(A.SKEPTIC, "skeptic_review", dict(
        experiment_id=eid, checks=_checks(),
        kill_reason="the mechanism is a factor proxy"))

    plan = RT.spawn_plan(BR.campaign_state(pipe, CAMPAIGN, _spec(eid)))
    risk = next(d for d in plan["roles"] if d["role"] == A.RISK)
    assert risk["decision"] == RT.SKIP
    assert risk["reason"] == RT.R_NO_SURVIVOR


def test_21_meta_is_skipped_until_there_is_a_real_combination_question(pipe):
    _foundation(pipe)
    reg = _prereg(pipe, owner=A.TREND_BREADTH, family="TREND",
                  hypothesis="FX trend persists over 21 sessions net of cost")
    eid = reg["experiment_id"]
    _walk(pipe, A.TREND_BREADTH, reg, _strong())
    _submit(pipe, A.TREND_BREADTH, reg, _strong())
    pipe.perform(A.SKEPTIC, "skeptic_review",
                 dict(experiment_id=eid, checks=_checks()))
    pipe.perform(A.RISK, "risk_review", dict(
        experiment_id=eid, verdict=P.RISK_ACCEPTABLE,
        metrics={"beta": 0.1, "max_drawdown": -0.12}))

    plan = RT.spawn_plan(BR.campaign_state(pipe, CAMPAIGN, _spec(eid)))
    meta = next(d for d in plan["roles"] if d["role"] == A.META)
    assert meta["decision"] == RT.SKIP
    assert meta["reason"] == RT.R_NO_META_QUESTION
    assert meta["facts"]["validated_survivors"] == 1
    # ... and publishing is skipped too, because nothing was cleared.
    pub = next(d for d in plan["roles"] if d["role"] == A.PUBLISHING)
    assert pub["decision"] == RT.SKIP
    assert pub["reason"] == RT.R_NOT_CLEARED


def test_22_two_validated_survivors_do_create_the_meta_question():
    state = {"campaign_id": CAMPAIGN,
             "validated_survivors": ["H_a", "H_b"], "awaiting_risk": [],
             "awaiting_skeptic": [], "director_cleared": []}
    meta = next(d for d in RT.spawn_plan(state)["roles"] if d["role"] == A.META)
    assert meta["decision"] == RT.SPAWN
    assert meta["reason"] == RT.R_META_QUESTION


def test_23_a_signal_agent_with_no_open_work_is_not_spawned():
    state = {"campaign_id": CAMPAIGN,
             "assigned_by_agent": {A.MOMENTUM: 2, A.REVERSAL: 1},
             "unmeasured_by_agent": {A.MOMENTUM: 1}}
    plan = RT.spawn_plan(state)
    by = {d["role"]: d for d in plan["roles"]}
    assert by[A.MOMENTUM]["decision"] == RT.SPAWN
    assert by[A.REVERSAL]["reason"] == RT.R_ALL_MEASURED
    assert by[A.TREND_BREADTH]["reason"] == RT.R_NOT_ASSIGNED
    for role in (A.REVERSAL, A.TREND_BREADTH, A.VOL_LIQUIDITY):
        assert by[role]["decision"] == RT.SKIP


def test_24_the_spawn_plan_is_pure_and_every_skip_is_explained():
    plan = RT.spawn_plan({"campaign_id": CAMPAIGN})
    assert plan == RT.spawn_plan({"campaign_id": CAMPAIGN})
    assert {d["role"] for d in plan["roles"]} == set(A.ROSTER)
    for d in plan["roles"]:
        assert d["reason"], d["role"]
        assert d["model"] in RT.SUPPORTED_MODEL_ALIASES
    assert plan["agent_may_spawn_agent"] is False
    assert plan["spawned_by"] == "SESSION_ORCHESTRATOR_ONLY"


def test_25_no_research_agent_may_launch_another_agent():
    for name in A.ROSTER:
        tools = C.parse_agent_definition(
            Path(A.AGENT_DEFINITION_DIR) / ("%s.md" % name))["tools"]
        for tool in RT.DELEGATION_TOOLS:
            assert tool not in tools, "%s may spawn agents" % name


# --------------------------------------------------------------------------- #
# F. Local execution
# --------------------------------------------------------------------------- #
def test_26_every_deterministic_task_names_an_importable_local_owner():
    """A DO_NOT_RECOMPUTE entry is a promise that local code already did the
    work. An owner that does not resolve turns the promise into a dead end."""
    import importlib

    checked = 0
    for task, owner in RT.DETERMINISTIC_LOCAL_OWNERS.items():
        if not owner.startswith("alpha_agent"):
            continue                      # named for a per-campaign module
        parts = owner.split(".")
        resolved = None
        # The owner is either a module or an attribute of one. Walk inwards
        # from the full path until a module imports, then require that the
        # remaining segments exist as attributes on it.
        for cut in range(len(parts), 0, -1):
            try:
                mod = importlib.import_module(
                    "paper_trader." + ".".join(parts[:cut]))
            except ModuleNotFoundError:
                continue
            obj = mod
            for attr in parts[cut:]:
                obj = getattr(obj, attr, None)
                if obj is None:
                    break
            resolved = obj
            break
        assert resolved is not None, \
            "%s names %s, which resolves to nothing" % (task, owner)
        checked += 1
    assert checked >= 10, "the deterministic owner table went missing"


def test_27_the_runner_owns_the_three_machine_attacks():
    assert hasattr(RUN, "adversarial_pack")
    src = (Path(A.__file__).parent / "runner.py").read_text(encoding="utf-8")
    for attack in ("placebo_clean", "cost_robust", "subperiod_stable"):
        assert attack in src, attack


def test_28_the_brief_tells_each_role_what_not_to_recompute(pipe):
    brief = BR.director_brief(pipe, run_id=RUN_ID, campaign_id=CAMPAIGN,
                              spec=_spec())
    assert brief["DO_NOT_RECOMPUTE"] == dict(RT.DETERMINISTIC_LOCAL_OWNERS)
    assert brief["MODEL_ROUTING"]["model"] == RT.ROUTING[A.DIRECTOR]["model"]


# --------------------------------------------------------------------------- #
# G. Resume without a transcript
# --------------------------------------------------------------------------- #
def test_29_a_campaign_resumes_from_state_after_a_context_reset(
        tmp_path, monkeypatch):
    monkeypatch.setenv(r59.RESEARCH_ROOT_ENV, str(tmp_path / "r59_root"))
    db = tmp_path / "research_memory.sqlite"
    first = P.AgentPipeline(M.ResearchMemory(db),
                            artifact_root=tmp_path / "agents_v2")
    _foundation(first)
    reg = _prereg(first, owner=A.TREND_BREADTH, family="TREND",
                  hypothesis="FX trend persists over 21 sessions net of cost")
    eid = reg["experiment_id"]
    _walk(first, A.TREND_BREADTH, reg, _strong())
    _submit(first, A.TREND_BREADTH, reg, _strong())
    spec = _spec(eid)

    state = BR.resume_state(first, run_id=RUN_ID, campaign_id=CAMPAIGN,
                            spec=spec)
    path = tmp_path / "CAMPAIGN_RESUME_STATE.json"
    path.write_text(json.dumps(state, indent=1, default=str),
                    encoding="utf-8")
    first.mem.close()

    # --- simulated context reset: a NEW session, no transcript ------------- #
    reloaded = json.loads(path.read_text(encoding="utf-8"))
    assert reloaded["kind"] == BR.RESUME_KIND
    assert reloaded["full_transcript_required_to_resume"] is False
    assert A.SKEPTIC in reloaded["next_roles_to_spawn"]
    assert reloaded["experiments"][0]["stages_revealed"] == ["D", "V", "L"]
    assert reloaded["model_routing"][A.SKEPTIC]["model"] == "opus"

    second = P.AgentPipeline(M.ResearchMemory(db),
                             artifact_root=tmp_path / "agents_v2")
    again = BR.resume_state(second, run_id=RUN_ID, campaign_id=CAMPAIGN,
                            spec=spec)
    assert again["state"] == reloaded["state"]
    assert again["next_roles_to_spawn"] == reloaded["next_roles_to_spawn"]


def test_30_the_resume_state_names_every_role_it_refuses_to_spawn(pipe):
    _foundation(pipe)
    reg = _prereg(pipe, owner=A.TREND_BREADTH, family="TREND",
                  hypothesis="FX trend persists over 21 sessions net of cost")
    _walk(pipe, A.TREND_BREADTH, reg, _halts_at_d())
    state = BR.resume_state(pipe, run_id=RUN_ID, campaign_id=CAMPAIGN,
                            spec=_spec(reg["experiment_id"]))
    refused = {r["role"]: r["reason"] for r in state["roles_not_to_spawn"]}
    for role in (A.SKEPTIC, A.RISK, A.META, A.PUBLISHING):
        assert role in refused, role
    assert state["deterministic_execution_owner"] == \
        "alpha_agent.agents_v2.runner"


# --------------------------------------------------------------------------- #
# H. Benchmark
# --------------------------------------------------------------------------- #
def test_31_the_benchmark_reports_a_proxy_and_never_claims_tokens(pipe,
                                                                  tmp_path):
    _foundation(pipe)
    reg = _prereg(pipe, owner=A.TREND_BREADTH, family="TREND",
                  hypothesis="FX trend persists over 21 sessions net of cost")
    eid = reg["experiment_id"]
    _walk(pipe, A.TREND_BREADTH, reg, _strong())
    _submit(pipe, A.TREND_BREADTH, reg, _strong())
    cdir = tmp_path / "campaign"
    cdir.mkdir()
    (cdir / "campaign_agenda.json").write_text("{}", encoding="utf-8")
    (cdir / "results.json").write_text("{}", encoding="utf-8")

    out = EF.measure_campaign(pipe, run_id=RUN_ID, campaign_id=CAMPAIGN,
                              repo=REPO, campaign_dir=cdir, spec=_spec(eid))
    assert out["token_counts_available"] is False
    assert out["proxy"] == EF.PROXY
    cp = out["context_proxy"]
    assert cp["after_bytes"] < cp["before_bytes"]
    # The two effects are priced separately, so neither can be overclaimed.
    assert cp["brief_effect_only"]["after_bytes"] >= cp["after_bytes"]
    assert out["invocations"]["before_agent_invocations"] == len(A.ROSTER)
    assert out["invocations"]["after_agent_invocations"] <= len(A.ROSTER)
    assert out["skeptic_recomputes_machine_tests"] is False
    assert out["deterministic_tasks_done_by_agent"] == 0


# --------------------------------------------------------------------------- #
# I. Safety: the pass added no operational verb
# --------------------------------------------------------------------------- #
FORBIDDEN = ("create_order", "submit_order", "place_order", "create_fill",
             "execute_trade", "promote_champion", "approve_proposal",
             "make_capital_eligible", "rebalance", "broker")


@pytest.mark.parametrize("module", ["routing.py", "briefs.py",
                                    "efficiency.py"])
def test_32_the_new_modules_contain_no_operational_verb(module):
    src = (Path(A.__file__).parent / module).read_text(encoding="utf-8")
    lowered = src.lower()
    for verb in FORBIDDEN:
        # ``rebalance`` and ``broker`` appear in no new module at all; the
        # others must not appear as a callable name.
        assert ("def %s" % verb) not in lowered, "%s defines %s" % (module,
                                                                    verb)
    assert "import api" not in lowered
    assert "from api" not in lowered


def test_33_every_brief_carries_the_safety_state(pipe):
    brief = BR.director_brief(pipe, run_id=RUN_ID, campaign_id=CAMPAIGN,
                              spec=_spec())
    safety = brief["SAFETY_STATE"]
    for flag in ("creates_orders", "creates_fills", "broker_enabled",
                 "promotes_model", "automatic_model_promotion_allowed",
                 "approves_proposal", "makes_sleeve_capital_eligible",
                 "automation_enabled"):
        assert safety[flag] is False, flag
    assert safety["research_only"] is True
    assert safety["manual_review_required"] is True


def test_35_one_bounded_hermetic_campaign_proves_the_acceptance_list(pipe):
    """The twelve acceptance points, in ONE bounded campaign.

    Three experiments, shaped like the estate really behaves: one halts at
    discovery, one halts at validation, one reaches the lockbox and is killed
    by the skeptic. No new alpha is revealed: every layer is synthetic and the
    memory lives in ``tmp_path``.
    """
    proof: dict = {}
    _foundation(pipe)

    # (2) the director assigns experiments - and is the only role that may.
    halt_d = _prereg(pipe, owner=A.MOMENTUM, family="CROSS_ASSET_MOMENTUM",
                     hypothesis="FX cross-asset momentum persists over 21 "
                                "sessions net of per-market cost")
    halt_v = _prereg(pipe, owner=A.REVERSAL, family="SHORT_TERM_REVERSAL",
                     hypothesis="A 3-session FX overreaction reverses over "
                                "the next 3 sessions at the next open")
    reach_l = _prereg(pipe, owner=A.TREND_BREADTH, family="TREND",
                      hypothesis="FX time-series trend persists over 21 "
                                 "sessions net of per-market cost")
    spec = _spec(halt_d["experiment_id"], halt_v["experiment_id"],
                 reach_l["experiment_id"])
    proof["2_director_assigns"] = len(spec["experiments"]) == 3

    # (1) the director's state is compact and needs no full dump.
    d_brief = BR.director_brief(pipe, run_id=RUN_ID, campaign_id=CAMPAIGN,
                                spec=spec)
    proof["1_director_compact_state"] = (
        BR.brief_problems(d_brief) == []
        and d_brief["FULL_MEMORY_DUMP_REQUIRED"] is False
        and d_brief["FULL_CENSUS_READ_REQUIRED"] is False)

    # (3) the deterministic evaluator is the local runner, recorded as such.
    assert _walk(pipe, A.MOMENTUM, halt_d, _halts_at_d()) == "D"
    assert _walk(pipe, A.REVERSAL, halt_v, _halts_at_v()) == "V"
    assert _walk(pipe, A.TREND_BREADTH, reach_l, _strong()) == "L"
    evaluators = {e["detail"]["evaluator"] for e in pipe._events(P.EV_STAGE)}
    proof["3_local_runner_computes"] = evaluators == {RUN.CALCULATION_OWNER}

    # (5) validation only when D advanced; (6) lockbox only when V advanced.
    stages = {eid: [e["detail"]["stage"]
                    for e in pipe._events(P.EV_STAGE, eid)]
              for eid in (halt_d["experiment_id"], halt_v["experiment_id"],
                          reach_l["experiment_id"])}
    proof["5_validation_needs_discovery"] = \
        stages[halt_d["experiment_id"]] == ["D"]
    proof["6_lockbox_needs_validation"] = (
        stages[halt_v["experiment_id"]] == ["D", "V"]
        and stages[reach_l["experiment_id"]] == ["D", "V", "L"])

    # (4) a halted discovery spawns nothing downstream.
    mid = RT.spawn_plan(BR.campaign_state(pipe, CAMPAIGN, spec))
    proof["4_halt_stops_downstream"] = all(
        r in mid["skip"] for r in (A.RISK, A.META, A.PUBLISHING))

    _submit(pipe, A.TREND_BREADTH, reach_l, _strong())
    state = BR.campaign_state(pipe, CAMPAIGN, spec)
    plan = RT.spawn_plan(state)
    proof["7a_only_the_measured_reach_the_skeptic"] = (
        state["awaiting_skeptic"] == [reach_l["experiment_id"]]
        and A.SKEPTIC in plan["spawn"])

    # (7) the skeptic's bundle is compact and already measured.
    s_brief = BR.skeptic_brief(
        pipe, run_id=RUN_ID, campaign_id=CAMPAIGN,
        experiment_id=reach_l["experiment_id"],
        result=_runner_result(reach_l["experiment_id"]))
    proof["7b_skeptic_bundle_compact"] = (
        BR.brief_problems(s_brief) == []
        and s_brief["SKEPTIC_RECOMPUTES_MACHINE_TESTS"] is False
        and s_brief["METRICS"]["PLACEBO_RESULT"]["measured"] is not None
        and s_brief["METRICS"]["GATE_RESULT"].get("checks") is not None)

    pipe.perform(A.SKEPTIC, "skeptic_review", dict(
        experiment_id=reach_l["experiment_id"], checks=_checks(),
        kill_reason="the mechanism is a known factor proxy"))

    # (8) risk sees only survivors - and there are none.
    final = RT.spawn_plan(BR.campaign_state(pipe, CAMPAIGN, spec))
    by = {d["role"]: d for d in final["roles"]}
    proof["8_risk_sees_only_survivors"] = (
        by[A.RISK]["decision"] == RT.SKIP
        and by[A.RISK]["reason"] == RT.R_NO_SURVIVOR)
    with pytest.raises(P.PipelineRefusal, match="NOT_A_SKEPTIC_SURVIVOR"):
        pipe.perform(A.RISK, "risk_review", dict(
            experiment_id=reach_l["experiment_id"],
            verdict=P.RISK_ACCEPTABLE, metrics={"beta": 0.1}))

    # (9) and (10): meta and publishing are skipped, with reasons.
    proof["9_meta_skipped"] = (by[A.META]["decision"] == RT.SKIP
                               and by[A.META]["reason"] == RT.R_NO_META_QUESTION)
    proof["10_publishing_skipped"] = (
        by[A.PUBLISHING]["decision"] == RT.SKIP
        and by[A.PUBLISHING]["reason"] == RT.R_NOT_CLEARED)

    # (11) resume from a machine-readable file, not a transcript.
    resume = BR.resume_state(pipe, run_id=RUN_ID, campaign_id=CAMPAIGN,
                             spec=spec)
    round_tripped = json.loads(json.dumps(resume, default=str))
    proof["11_resume_without_transcript"] = (
        round_tripped["full_transcript_required_to_resume"] is False
        and len(round_tripped["experiments"]) == 3
        and round_tripped["next_roles_to_spawn"] == final["spawn"])

    # (12) nothing in this campaign created an order, a fill or a promotion.
    proof["12_zero_operational_paths"] = (
        A.SAFETY["creates_orders"] is False
        and A.SAFETY["creates_fills"] is False
        and A.SAFETY["automatic_model_promotion_allowed"] is False
        and pipe._events(P.EV_PUBLISHED) == []
        and pipe._events(P.EV_FORWARD) == [])

    assert all(proof.values()), {k: v for k, v in proof.items() if not v}
    assert len(proof) == 13        # twelve points, one split into 7a/7b
    # The whole campaign spawned five roles, not twelve.
    assert len(set(mid["spawn"]) | set(final["spawn"])) < len(A.ROSTER)


def test_34_the_brief_cli_is_read_only():
    src = (REPO / "scripts" / "agents_v2_brief.py").read_text(encoding="utf-8")
    assert "open_memory_readonly" in src
    assert "ResearchMemory(" not in src, "the CLI opened a writable memory"
    for verb in ("preregister", "submit_candidate", "director_clear",
                 "publish_candidate", "request_forward_registration"):
        assert ('"%s"' % verb) not in src, "the CLI can perform %s" % verb
