"""PAPER_TRADER_ALPHA_AGENTS_V2 - the ported twelve-agent research system.

Hermetic. Every test runs against a research memory, an artifact root, an
adoption store and a forward registry inside ``tmp_path``; none of them can
reach ``D:\\Stock_Prediction_app_data``.

What is proven here, in the order the release brief asks for it:

    contract schema          the seven contracts and twelve definitions cohere,
                             and the validator BITES when they do not
    PowerShell-only policy   no definition grants Bash; every one grants PowerShell
    orchestration            director -> data -> universe -> features -> signal
    experiment registry      ONE registry (R59 ResearchMemory); eighteen fields
    handoffs / skeptic gate  a signal agent cannot bypass the skeptic; the
                             skeptic can kill; risk sees survivors only; meta
                             receives validated survivors only
    publishing safety        no order, fill, approval, promotion or capital verb
    Paper Trader integration a research-qualified candidate reaches the REAL
                             api.prospective_adoption + forward registrar,
                             with no backfill, and failures stay recorded
"""
from __future__ import annotations

import ast
import datetime as _dt
import json
import shutil
from pathlib import Path

import pytest

from paper_trader.alpha_agent import agents_v2 as A
from paper_trader.alpha_agent import r59
from paper_trader.alpha_agent.agents_v2 import contracts as C
from paper_trader.alpha_agent.agents_v2 import pipeline as P
from paper_trader.alpha_agent.r59 import memory as M
from paper_trader.api import forward_challenger_registry as FCR
from paper_trader.api import prospective_adoption as PA

REPO = Path(__file__).resolve().parents[1]
PACKAGE_DIR = REPO / "alpha_agent" / "agents_v2"

EQ_COST = {"rate_per_side": 0.00125, "basis": "traded notional"}
FUT_COST = {"rate_per_side": 0.0002, "basis": "traded notional"}


# --------------------------------------------------------------------------- #
# Fixtures and builders
# --------------------------------------------------------------------------- #
@pytest.fixture()
def pipe(tmp_path, monkeypatch):
    monkeypatch.setenv(r59.RESEARCH_ROOT_ENV, str(tmp_path / "r59_root"))
    monkeypatch.setenv(PA.ADOPTION_DIR_ENV, str(tmp_path / "adoption"))
    mem = M.ResearchMemory(tmp_path / "research_memory.sqlite")
    return P.AgentPipeline(mem, artifact_root=tmp_path / "agents_v2")


def _strong(kind: str) -> dict:
    if kind == "equity":
        return {"D": {"ann_net_excess": 0.05, "t_net_excess": 4.0,
                      "periods": 70},
                "V": {"ann_net_excess": 0.04, "t_net_excess": 3.0,
                      "periods": 55},
                "L": {"ann_net_excess": 0.06, "t_net_excess": 4.5,
                      "p_one_sided": 1e-6, "periods": 40}}
    return {"D": {"net_sharpe": 0.9, "t_net": 4.0, "days": 1600},
            "V": {"net_sharpe": 0.8, "t_net": 3.0, "days": 1200},
            "L": {"net_sharpe": 1.1, "t_net": 4.2, "p_one_sided": 1e-6,
                  "days": 700}}


def _weak() -> dict:
    """Advances D and V - so the lockbox is legitimately reached - and then
    fails the gate ON THE LOCKBOX, which is the only layer that decides."""
    return {"D": {"net_sharpe": 0.5, "t_net": 2.0, "days": 1600},
            "V": {"net_sharpe": 0.15, "t_net": 1.1, "days": 1200},
            "L": {"net_sharpe": 0.10, "t_net": 0.6, "p_one_sided": 0.27,
                  "days": 700}}


def _halts_at(stage: str) -> dict:
    """Layers whose ``stage`` is too weak to earn the next one."""
    out = _weak()
    out[stage] = dict(out[stage])
    out[stage]["net_sharpe"] = 0.0
    return out


def _all_checks_pass() -> dict:
    return {cid: {"passed": True, "measured": 1.0,
                  "evidence": "skeptic_review.json#%s" % cid}
            for cid in ("pit_integrity", "leakage_pass", "placebo_clean",
                        "cost_robust", "subperiod_stable",
                        "not_a_duplicate_identity")}


CASES = {
    "equity": dict(
        dataset="r58_pit_fundamental_panel_v1", universe="sp500_pit",
        features="eq_fundamental_momentum_v1",
        asset_class=r59.AC_US_EQUITY, owner=A.MOMENTUM,
        family="FUNDAMENTAL_MOMENTUM", horizon=21, long_short=False,
        cost=EQ_COST, layers="equity", information_family="PIT_FUNDAMENTALS",
        hypothesis="Improving PIT gross profitability predicts 21-session "
                   "excess return in S&P 500 members",
        scope=["SP500_PIT_MEMBERS"]),
    "fx": dict(
        dataset="r38_native_futures_fx", universe="fx_futures_liquid",
        features="fx_trend_v1", asset_class=r59.AC_FX, owner=A.TREND_BREADTH,
        family="TREND", horizon=21, long_short=True, cost=FUT_COST,
        layers="futures", information_family="PRICE_STATE",
        hypothesis="Time-series trend in G10 FX futures persists over 21 "
                   "sessions net of per-market cost",
        scope=["&6E", "&6J", "&6B", "&6A", "&6C", "&6S"]),
    "commodity": dict(
        dataset="r38_native_futures_commodity", universe="commodity_liquid",
        features="commodity_carry_v1", asset_class=r59.AC_COMMODITY,
        owner=A.VOL_LIQUIDITY, family="CARRY", horizon=21, long_short=True,
        cost=FUT_COST, layers="futures", information_family="TERM_STRUCTURE",
        hypothesis="Dated-contract roll yield predicts 21-session commodity "
                   "futures returns cross-sectionally",
        scope=["&CL", "&GC", "&HG", "&ZC", "&ZS"]),
    "h1_h5": dict(
        dataset="r38_native_futures_index", universe="index_futures_liquid",
        features="index_reversal_v1", asset_class=r59.AC_EQUITY_INDEX,
        owner=A.REVERSAL, family="SHORT_TERM_REVERSAL", horizon=3,
        long_short=True, cost=FUT_COST, layers="futures",
        information_family="PRICE_STATE",
        hypothesis="A 3-session overreaction in equity-index futures reverses "
                   "over the next 3 sessions, entered at the next open",
        scope=["&ES", "&NQ", "&RTY", "&YM"]),
}


def _foundation(pipe, case: dict) -> None:
    pipe.perform(A.DATA_FOUNDATION, "certify_data", dict(
        dataset_id=case["dataset"], asset_classes=[case["asset_class"]],
        pit_status=P.PIT_SAFE,
        availability_rule="value stamped at the instant it became observable",
        survivorship="delisted names / expired contracts retained"))
    pipe.perform(A.UNIVERSE, "define_universe", dict(
        universe_id=case["universe"], dataset_id=case["dataset"],
        asset_class=case["asset_class"],
        execution_representation="LONG_SHORT" if case["long_short"]
        else "LONG_ONLY",
        short_leg_expressible=case["long_short"],
        rules="liquidity floor from trailing data only"))
    pipe.perform(A.FEATURES, "publish_features", dict(
        feature_set_id=case["features"], universe_id=case["universe"],
        features=[{"name": "f1", "lag": 1, "source": case["dataset"]}],
        leakage_check="PASS"))


def _prereg_payload(case: dict) -> dict:
    return dict(
        owning_agent=case["owner"], hypothesis=case["hypothesis"],
        asset_class=case["asset_class"], family=case["family"],
        feature_set_id=case["features"], horizon_sessions=case["horizon"],
        parameters={"lookback": 63}, long_short=case["long_short"],
        discovery_sample={"start": r59.DISCOVERY_START,
                          "end": r59.VALIDATION_START},
        evaluation_sample={"validation": r59.VALIDATION_START,
                           "lockbox": r59.LOCKBOX_START},
        cost_model=case["cost"], expected_sign=1,
        information_family=case["information_family"],
        instrument_scope=case["scope"], venue="RESEARCH")


def _preregister(pipe, name: str) -> dict:
    case = CASES[name]
    _foundation(pipe, case)
    return pipe.perform(A.DIRECTOR, "preregister", _prereg_payload(case))


def _reveal(pipe, name: str, reg: dict, *, layers=None) -> dict:
    """Walk D -> V -> L through the governed reveal. Returns the last result."""
    case = CASES[name]
    layers = layers if layers is not None else _strong(case["layers"])
    out: dict = {}
    for stage in r59.STAGES:
        out = pipe.perform(case["owner"], "reveal_stage", dict(
            experiment_id=reg["experiment_id"], spec_hash=reg["spec_hash"],
            stage=stage, stats=layers.get(stage) or {},
            evaluator="alpha_agent.r57.engine"))
        if not out["advance"]:
            break
    return out


def _submit(pipe, name: str, reg: dict, *, layers=None, sign=1, cost=None,
            turnover=0.20, reveal=True) -> dict:
    case = CASES[name]
    layers = layers if layers is not None else _strong(case["layers"])
    if reveal:
        _reveal(pipe, name, reg, layers=layers)
    return pipe.perform(case["owner"], "submit_candidate", dict(
        experiment_id=reg["experiment_id"], spec_hash=reg["spec_hash"],
        signal_sign=sign, evidence_kind="HISTORICAL",
        cost_model=cost or case["cost"], turnover=turnover,
        layers=layers,
        evaluator="alpha_agent.r57.engine"))


def _to_cleared(pipe, name: str) -> str:
    reg = _preregister(pipe, name)
    eid = reg["experiment_id"]
    _submit(pipe, name, reg)
    pipe.perform(A.SKEPTIC, "skeptic_review",
                 dict(experiment_id=eid, checks=_all_checks_pass()))
    pipe.perform(A.RISK, "risk_review", dict(
        experiment_id=eid, verdict=P.RISK_ACCEPTABLE,
        metrics={"beta": 0.1, "max_drawdown": -0.12}))
    pipe.perform(A.META, "meta_review",
                 dict(experiment_ids=[eid], verdict="STANDALONE"))
    pipe.perform(A.DIRECTOR, "director_clear", dict(
        experiment_id=eid, decision="CLEARED",
        rationale="sole validated survivor of the campaign"))
    return eid


def _refused(code: str):
    return pytest.raises(P.PipelineRefusal, match=code)


# --------------------------------------------------------------------------- #
# 1. Agent contract schema
# --------------------------------------------------------------------------- #
def test_01_the_committed_specification_is_coherent():
    assert C.validate() == []


def test_02_every_contract_declares_the_v2_identity():
    docs = C.load_all()
    assert set(docs) == set(A.CONTRACT_FILES) and len(docs) == 7
    for name, doc in docs.items():
        assert doc["agent_system_version"] == "PAPER_TRADER_ALPHA_AGENTS_V2", name


def test_03_twelve_agents_are_ported_in_canonical_order():
    assert len(A.ROSTER) == 12 and len(set(A.ROSTER)) == 12
    for name in A.ROSTER:
        d = C.parse_agent_definition(A.AGENT_DEFINITION_DIR / ("%s.md" % name))
        assert d["frontmatter"]["name"] == name
        assert A.AGENT_SYSTEM_VERSION in d["body"]
    order = C.load_contract("agent_manifest.json")["orchestration"]["order"]
    assert order[0] == A.DIRECTOR and order[-1] == A.PUBLISHING
    assert sorted(order[4]) == sorted(A.SIGNAL_AGENTS)
    assert order.index(A.SKEPTIC) < order.index(A.RISK) < order.index(A.META)


def test_04_governance_retires_restrictions_explicitly_and_keeps_safety():
    gov = C.load_contract("governance_contract.json")
    assert gov["supersedes"]["identity"] == A.SUPERSEDES
    assert gov["authority"]["authorising_run_id"] == A.AUTHORISING_RUN_ID
    retired = " | ".join(r["phase_8a_rule"] for r in gov["retired_restrictions"])
    for fragment in ("S&P 500", "long-only", "monthly", "fundamentals",
                     "regime", "touch Paper Trader", "commit and push",
                     "Bash", "preview-only"):
        assert fragment in retired, fragment
    kept = " | ".join(p["rule"] for p in gov["preserved_safety_principles"])
    for fragment in (
            "no broker execution", "no operational order creation",
            "no fills", "no automatic model promotion",
            "no automatic portfolio approval", "no hindsight reconstruction",
            "no fabricated TRUE_FORWARD evidence",
            "no backfill masquerading as forward evidence",
            "hypothesis declared before evaluation",
            "thresholds frozen before final evaluation", "failure recording",
            "experiment id", "skeptic rejects by default",
            "realistic transaction costs", "point-in-time integrity",
            "no silent sign flipping", "no rounding weak evidence into a pass",
            "no hidden deletion of failed experiments",
            "manual review remains mandatory"):
        assert fragment in kept, fragment
    allowed = gov["explicit_allowances"]
    for item in ("multi-asset research", "FX", "commodities", "rates",
                 "equity-index futures", "PIT-safe fundamentals",
                 "regime hypotheses", "cross-asset signals", "H1-H5 research",
                 "meta-model research", "governed forward registration",
                 "research shadow P&L",
                 "commit / push of research code and agent definitions"):
        assert item in allowed, item


def _copy_spec(tmp_path) -> tuple:
    cdir, ddir = tmp_path / "contracts", tmp_path / "definitions"
    shutil.copytree(A.CONTRACT_DIR, cdir)
    ddir.mkdir()
    for name in A.ROSTER:
        shutil.copy(A.AGENT_DEFINITION_DIR / ("%s.md" % name), ddir)
    return cdir, ddir


def test_05_the_validator_bites(tmp_path):
    cdir, ddir = _copy_spec(tmp_path)
    assert C.validate(cdir, ddir) == []

    # a definition that quietly regains the Bash tool
    p = ddir / ("%s.md" % A.MOMENTUM)
    p.write_text(p.read_text(encoding="utf-8").replace(
        "tools: Read, Grep, Glob, PowerShell",
        "tools: Read, Grep, Glob, Bash, PowerShell"), encoding="utf-8")
    # a handoff edge that lets a signal agent skip the skeptic
    hp = cdir / "handoff_contracts.json"
    doc = json.loads(hp.read_text(encoding="utf-8"))
    doc["edges"].append({"from": A.REVERSAL, "to": A.RISK,
                         "artifact": "x", "gate": "candidate_submitted"})
    hp.write_text(json.dumps(doc), encoding="utf-8")
    # a publishing boundary that lost a prohibition
    gp = cdir / "governance_contract.json"
    gov = json.loads(gp.read_text(encoding="utf-8"))
    gov["signal_publishing_boundary"]["may_not"].remove("create fills")
    gp.write_text(json.dumps(gov), encoding="utf-8")

    problems = " | ".join(C.validate(cdir, ddir))
    assert "grants the Bash tool" in problems
    assert "SKEPTIC BYPASS" in problems
    assert "publishing boundary lost a prohibition: create fills" in problems


def test_06_a_copied_phase_8a_definition_is_rejected(tmp_path):
    cdir, ddir = _copy_spec(tmp_path)
    p = ddir / ("%s.md" % A.PUBLISHING)
    p.write_text(p.read_text(encoding="utf-8")
                 + "\nNo Paper Trader integration. No commit, no push.\n",
                 encoding="utf-8")
    assert any("retired Phase 8-A text" in x for x in C.validate(cdir, ddir))


# --------------------------------------------------------------------------- #
# 2. PowerShell-only policy
# --------------------------------------------------------------------------- #
def test_07_every_agent_is_powershell_only():
    for name in A.ROSTER:
        d = C.parse_agent_definition(A.AGENT_DEFINITION_DIR / ("%s.md" % name))
        assert "Bash" not in d["tools"], name
        assert "PowerShell" in d["tools"], name
        assert "## PowerShell-only rule" in d["body"], name
        assert "```bash" not in d["body"] and "```sh" not in d["body"], name
        assert ".venv-win\\Scripts\\python.exe" in d["body"], name
    gov = C.load_contract("governance_contract.json")
    assert gov["shell_policy"]["shell"] == "WINDOWS_POWERSHELL_ONLY"
    assert "Bash" in gov["shell_policy"]["forbidden_tools"]
    assert A.SAFETY["shell"] == "WINDOWS_POWERSHELL_ONLY"


# --------------------------------------------------------------------------- #
# 3. Orchestration + experiment registry
# --------------------------------------------------------------------------- #
def test_08_director_mints_distinct_ids_for_four_asset_classes(pipe):
    regs = {n: _preregister(pipe, n) for n in CASES}
    ids = [r["experiment_id"] for r in regs.values()]
    assert len(set(ids)) == 4
    ledger = {row["EXPERIMENT_ID"]: row for row in pipe.ledger()}
    assert set(ledger) == set(ids)
    assert {row["ASSET_CLASS"] for row in ledger.values()} == {
        r59.AC_US_EQUITY, r59.AC_FX, r59.AC_COMMODITY, r59.AC_EQUITY_INDEX}
    h = ledger[regs["h1_h5"]["experiment_id"]]
    assert 1 <= h["HORIZON"] <= 5 and h["AGENT"] == A.REVERSAL
    for row in ledger.values():
        assert row["SURVIVOR_STATE"] == "PREREGISTERED"
        assert row["PIT_STATUS"] == "PIT_SAFE"
        assert row["SKEPTIC_VERDICT"] == "NOT_REVIEWED"
        assert row["FORWARD_STATE"] == "NONE"
    # idempotent: the same frozen spec is the same experiment, never a second
    again = pipe.perform(A.DIRECTOR, "preregister",
                         _prereg_payload(CASES["fx"]))
    assert again["experiment_id"] == regs["fx"]["experiment_id"]
    assert again["already_registered"] is True
    assert len(pipe.ledger()) == 4


def test_09_the_ledger_is_the_one_research_memory_not_a_second_registry(pipe):
    reg = _preregister(pipe, "equity")
    row = pipe.mem.get(reg["experiment_id"])
    assert row["release"] == A.RELEASE
    assert row["generation_method"] == A.GENERATION_METHOD
    assert row["origin"] == A.ORIGIN_PREFIX + A.MOMENTUM
    assert A.RESEARCH_REGISTRY_OWNER == "alpha_agent.r59.memory.ResearchMemory"
    schema = C.load_contract("experiment_registry_schema.json")
    assert schema["registry_owner"] == A.RESEARCH_REGISTRY_OWNER
    assert set(schema["fields"]) == set(pipe.ledger()[0])
    # exactly the fields the release brief requires of every experiment, in
    # order and FIRST; R57's sequential reveal appends three derived fields
    # after them, so the brief's eighteen stay exactly where they were.
    assert list(pipe.ledger()[0])[:18] == [
        "EXPERIMENT_ID", "AGENT", "HYPOTHESIS", "ASSET_CLASS", "FAMILY",
        "FEATURE_SET", "HORIZON", "PARAMETERS", "DISCOVERY_SAMPLE",
        "EVALUATION_SAMPLE", "PIT_STATUS", "TURNOVER", "COST_MODEL", "RESULT",
        "SKEPTIC_VERDICT", "RISK_VERDICT", "SURVIVOR_STATE", "FORWARD_STATE"]
    assert list(pipe.ledger()[0])[18:] == [
        "STAGES_REVEALED", "HALTED_AT", "LOCKBOX_COMPUTED"]
    # the package creates no database and no table of its own
    for py in PACKAGE_DIR.glob("*.py"):
        src = py.read_text(encoding="utf-8")
        assert "sqlite3" not in src and "CREATE TABLE" not in src, py.name


def test_10_only_the_director_preregisters_and_the_record_must_be_complete(pipe):
    case = CASES["fx"]
    _foundation(pipe, case)
    with _refused("VERB_NOT_IN_AGENT_CONTRACT"):
        pipe.perform(case["owner"], "preregister", _prereg_payload(case))
    for missing in ("hypothesis", "cost_model", "expected_sign",
                    "evaluation_sample", "feature_set_id"):
        body = _prereg_payload(case)
        body.pop(missing)
        with _refused("PREREGISTRATION_INCOMPLETE"):
            pipe.perform(A.DIRECTOR, "preregister", body)
    with _refused("SIGN_NOT_DECLARED"):
        pipe.perform(A.DIRECTOR, "preregister",
                     dict(_prereg_payload(case), expected_sign=0))
    with _refused("FAMILY_NOT_ROUTED_TO_AGENT"):
        pipe.perform(A.DIRECTOR, "preregister",
                     dict(_prereg_payload(case), family="CARRY"))
    with _refused("OWNER_NOT_A_SIGNAL_AGENT"):
        pipe.perform(A.DIRECTOR, "preregister",
                     dict(_prereg_payload(case), owning_agent=A.SKEPTIC))
    assert pipe.ledger() == []


def test_11_the_foundation_chain_cannot_be_skipped(pipe):
    with _refused("DATA_NOT_CERTIFIED"):
        pipe.perform(A.UNIVERSE, "define_universe", dict(
            universe_id="u", dataset_id="nope", asset_class=r59.AC_FX,
            execution_representation="LONG_SHORT", rules="r"))
    pipe.perform(A.DATA_FOUNDATION, "certify_data", dict(
        dataset_id="restated", asset_classes=[r59.AC_US_EQUITY],
        pit_status=P.NOT_PIT_SAFE, availability_rule="",
        survivorship="", notes="restated fundamentals"))
    with _refused("DATA_NOT_PIT_SAFE"):
        pipe.perform(A.UNIVERSE, "define_universe", dict(
            universe_id="u", dataset_id="restated",
            asset_class=r59.AC_US_EQUITY,
            execution_representation="LONG_ONLY", rules="r"))
    with _refused("UNIVERSE_NOT_DEFINED"):
        pipe.perform(A.FEATURES, "publish_features", dict(
            feature_set_id="f", universe_id="u",
            features=[{"name": "x", "lag": 1, "source": "s"}],
            leakage_check="PASS"))
    with _refused("FEATURES_NOT_PUBLISHED"):
        pipe.perform(A.DIRECTOR, "preregister", _prereg_payload(CASES["fx"]))
    _foundation(pipe, CASES["fx"])
    with _refused("FEATURES_NOT_LEAK_SAFE"):
        pipe.perform(A.FEATURES, "publish_features", dict(
            feature_set_id="leaky", universe_id=CASES["fx"]["universe"],
            features=[{"name": "x", "lag": 0, "source": "s"}],
            leakage_check="FAIL"))


# --------------------------------------------------------------------------- #
# 4. Handoffs and the skeptic gate
# --------------------------------------------------------------------------- #
def test_12_a_signal_agent_cannot_bypass_the_skeptic(pipe):
    reg = _preregister(pipe, "commodity")
    eid = reg["experiment_id"]
    owner = CASES["commodity"]["owner"]
    _submit(pipe, "commodity", reg)
    for verb, body in (
            ("skeptic_review", dict(experiment_id=eid,
                                    checks=_all_checks_pass())),
            ("risk_review", dict(experiment_id=eid, verdict="ACCEPTABLE",
                                 metrics={"beta": 0})),
            ("meta_review", dict(experiment_ids=[eid], verdict="STANDALONE")),
            ("director_clear", dict(experiment_id=eid, decision="CLEARED",
                                    rationale="x")),
            ("publish_candidate", dict(experiment_id=eid)),
            ("request_forward_registration", dict(experiment_id=eid))):
        with _refused("VERB_NOT_IN_AGENT_CONTRACT"):
            pipe.perform(owner, verb, body)
    # and the downstream agents themselves refuse an un-reviewed candidate
    with _refused("NOT_A_SKEPTIC_SURVIVOR"):
        pipe.perform(A.RISK, "risk_review", dict(
            experiment_id=eid, verdict="ACCEPTABLE", metrics={"beta": 0}))
    with _refused("NOT_VALIDATED_SURVIVORS"):
        pipe.perform(A.META, "meta_review",
                     dict(experiment_ids=[eid], verdict="STANDALONE"))
    with _refused("NOT_A_VALIDATED_SURVIVOR"):
        pipe.perform(A.DIRECTOR, "director_clear", dict(
            experiment_id=eid, decision="CLEARED", rationale="impatient"))
    with _refused("NOT_DIRECTOR_CLEARED"):
        pipe.perform(A.PUBLISHING, "publish_candidate",
                     dict(experiment_id=eid))
    assert pipe.survivors() == []


def test_13_a_result_must_be_the_preregistered_experiment(pipe):
    reg = _preregister(pipe, "fx")
    with _refused("NOT_THE_OWNING_AGENT"):
        pipe.perform(A.MOMENTUM, "submit_candidate", dict(
            experiment_id=reg["experiment_id"], spec_hash=reg["spec_hash"],
            signal_sign=1, cost_model=FUT_COST, turnover=0.2,
            layers=_strong("futures")))
    with _refused("SPEC_CHANGED_AFTER_PREREGISTRATION"):
        pipe.perform(A.TREND_BREADTH, "submit_candidate", dict(
            experiment_id=reg["experiment_id"], spec_hash="0" * 64,
            signal_sign=1, cost_model=FUT_COST, turnover=0.2,
            layers=_strong("futures")))
    with _refused("FORWARD_EVIDENCE_CANNOT_BE_SUBMITTED"):
        pipe.perform(A.TREND_BREADTH, "submit_candidate", dict(
            experiment_id=reg["experiment_id"], spec_hash=reg["spec_hash"],
            signal_sign=1, evidence_kind="TRUE_FORWARD", cost_model=FUT_COST,
            turnover=0.2, layers=_strong("futures")))
    with _refused("UNKNOWN_EXPERIMENT"):
        pipe.perform(A.TREND_BREADTH, "submit_candidate", dict(
            experiment_id="H_never_registered", spec_hash="x", signal_sign=1,
            cost_model=FUT_COST, turnover=0.2, layers=_strong("futures")))
    _submit(pipe, "fx", reg)
    with _refused("CANDIDATE_ALREADY_SUBMITTED"):
        _submit(pipe, "fx", reg)


def test_14_the_skeptic_can_kill_and_rejects_by_default(pipe):
    # (a) statistically weak -> NO_ALPHA_EVIDENCE
    weak = _preregister(pipe, "fx")
    _submit(pipe, "fx", weak, layers=_weak())
    out = pipe.perform(A.SKEPTIC, "skeptic_review", dict(
        experiment_id=weak["experiment_id"], checks=_all_checks_pass()))
    assert out["verdict"] == P.SK_KILLED and out["next"] is None
    assert out["outcome"] == r59.HO_NO_ALPHA_EVIDENCE

    # (b) statistically strong, but NO adversarial evidence -> killed by default
    bare = _preregister(pipe, "commodity")
    _submit(pipe, "commodity", bare)
    out = pipe.perform(A.SKEPTIC, "skeptic_review",
                       dict(experiment_id=bare["experiment_id"]))
    assert out["verdict"] == P.SK_KILLED
    assert out["outcome"] == r59.HO_REJECTED
    assert "placebo_clean" in out["failed"]

    # (c) a check asserted passed WITHOUT a measured value is a failure
    unmeasured = _preregister(pipe, "h1_h5")
    _submit(pipe, "h1_h5", unmeasured)
    checks = _all_checks_pass()
    checks["leakage_pass"] = {"passed": True, "measured": None,
                              "evidence": "trust me"}
    out = pipe.perform(A.SKEPTIC, "skeptic_review", dict(
        experiment_id=unmeasured["experiment_id"], checks=checks))
    assert out["verdict"] == P.SK_KILLED and "leakage_pass" in out["failed"]

    # a verdict is final: a killed candidate is never re-reviewed into a pass
    with _refused("ALREADY_REVIEWED"):
        pipe.perform(A.SKEPTIC, "skeptic_review", dict(
            experiment_id=weak["experiment_id"], checks=_all_checks_pass()))
    assert pipe.survivors() == []


def test_15_sign_flips_cost_swaps_and_turnover_are_machine_checked(pipe):
    flipped = _preregister(pipe, "fx")
    _submit(pipe, "fx", flipped, sign=-1)
    out = pipe.perform(A.SKEPTIC, "skeptic_review", dict(
        experiment_id=flipped["experiment_id"], checks=_all_checks_pass()))
    assert out["verdict"] == P.SK_KILLED and "sign_consistent" in out["failed"]

    cheap = _preregister(pipe, "commodity")
    _submit(pipe, "commodity", cheap,
            cost={"rate_per_side": 0.0, "basis": "traded notional"})
    out = pipe.perform(A.SKEPTIC, "skeptic_review", dict(
        experiment_id=cheap["experiment_id"], checks=_all_checks_pass()))
    assert "cost_model_frozen" in out["failed"]

    churn = _preregister(pipe, "h1_h5")
    _submit(pipe, "h1_h5", churn, turnover=0.95)
    out = pipe.perform(A.SKEPTIC, "skeptic_review", dict(
        experiment_id=churn["experiment_id"], checks=_all_checks_pass()))
    assert "turnover_within_ceiling" in out["failed"]


def test_16_thresholds_are_frozen_at_preregistration(pipe):
    reg = _preregister(pipe, "fx")
    _submit(pipe, "fx", reg)
    softened = C.Contracts()
    softened.gate_schema_hash = "f" * 64          # the schema file was edited
    late = P.AgentPipeline(pipe.mem, contracts=softened,
                           artifact_root=pipe.artifact_root)
    with _refused("THRESHOLDS_CHANGED_AFTER_PREREGISTRATION"):
        late.perform(A.SKEPTIC, "skeptic_review", dict(
            experiment_id=reg["experiment_id"], checks=_all_checks_pass()))


def test_17_the_statistical_verdict_is_delegated_to_the_canonical_gate(
        pipe, monkeypatch):
    calls = []
    real = P.E.gate

    def spy(result, *, prior_burden, family_tests=1):
        calls.append(prior_burden)
        return real(result, prior_burden=prior_burden,
                    family_tests=family_tests)
    monkeypatch.setattr(P.E, "gate", spy)
    first = _preregister(pipe, "fx")
    _submit(pipe, "fx", first, layers=_weak())
    pipe.perform(A.SKEPTIC, "skeptic_review", dict(
        experiment_id=first["experiment_id"], checks=_all_checks_pass()))
    second = _preregister(pipe, "commodity")
    _submit(pipe, "commodity", second)
    pipe.perform(A.SKEPTIC, "skeptic_review", dict(
        experiment_id=second["experiment_id"], checks=_all_checks_pass()))
    # called once per review, and the SECOND candidate is charged for the first
    assert calls == [0, 1]
    src = (PACKAGE_DIR / "pipeline.py").read_text(encoding="utf-8")
    assert "def gate(" not in src and "def evaluate(" not in src
    assert "E.gate(" in src and "H.search_denominator(" in src


def test_18_risk_sees_survivors_and_meta_receives_validated_survivors(pipe):
    killed = _preregister(pipe, "fx")
    _submit(pipe, "fx", killed, layers=_weak())
    pipe.perform(A.SKEPTIC, "skeptic_review", dict(
        experiment_id=killed["experiment_id"], checks=_all_checks_pass()))

    passed = {}
    for name in ("equity", "h1_h5"):
        passed[name] = _preregister(pipe, name)
        _submit(pipe, name, passed[name])
        out = pipe.perform(A.SKEPTIC, "skeptic_review", dict(
            experiment_id=passed[name]["experiment_id"],
            checks=_all_checks_pass()))
        assert out["verdict"] == P.SK_SURVIVED
        assert out["next"] == A.RISK
    eq, h3 = (passed[n]["experiment_id"] for n in ("equity", "h1_h5"))
    assert sorted(pipe.survivors()) == sorted([eq, h3])
    assert killed["experiment_id"] not in pipe.survivors()

    with _refused("NOT_A_SKEPTIC_SURVIVOR"):
        pipe.perform(A.RISK, "risk_review", dict(
            experiment_id=killed["experiment_id"], verdict="ACCEPTABLE",
            metrics={"beta": 0.0}))
    with _refused("RISK_METRICS_MISSING"):
        pipe.perform(A.RISK, "risk_review",
                     dict(experiment_id=eq, verdict="ACCEPTABLE"))
    pipe.perform(A.RISK, "risk_review", dict(
        experiment_id=eq, verdict="ACCEPTABLE", metrics={"beta": 0.2}))
    pipe.perform(A.RISK, "risk_review", dict(
        experiment_id=h3, verdict="REJECTED",
        breach="liquidity load 31% of ADV at the next open"))
    assert pipe.validated_survivors() == [eq]

    with _refused("NOT_VALIDATED_SURVIVORS"):
        pipe.perform(A.META, "meta_review",
                     dict(experiment_ids=[eq, h3], verdict="ENSEMBLE_READY"))
    with _refused("NOT_VALIDATED_SURVIVORS"):
        pipe.perform(A.META, "meta_review", dict(
            experiment_ids=[killed["experiment_id"]], verdict="STANDALONE"))
    out = pipe.perform(A.META, "meta_review",
                       dict(experiment_ids=[eq], verdict="STANDALONE"))
    assert out["members"] == [eq]
    with _refused("NOT_A_VALIDATED_SURVIVOR"):
        pipe.perform(A.DIRECTOR, "director_clear", dict(
            experiment_id=h3, decision="CLEARED", rationale="overrule risk"))


def test_19_failures_remain_recorded(pipe):
    killed = _preregister(pipe, "fx")
    _submit(pipe, "fx", killed, layers=_weak())
    pipe.perform(A.SKEPTIC, "skeptic_review", dict(
        experiment_id=killed["experiment_id"], checks=_all_checks_pass()))
    held = _preregister(pipe, "commodity")
    pipe.perform(CASES["commodity"]["owner"], "submit_candidate", dict(
        experiment_id=held["experiment_id"], spec_hash=held["spec_hash"],
        state="DATA_HOLD", reason="open interest absent before 2009"))
    rejected = _preregister(pipe, "h1_h5")
    _submit(pipe, "h1_h5", rejected)
    pipe.perform(A.SKEPTIC, "skeptic_review", dict(
        experiment_id=rejected["experiment_id"], checks=_all_checks_pass()))
    pipe.perform(A.RISK, "risk_review", dict(
        experiment_id=rejected["experiment_id"], verdict="REJECTED",
        breach="drawdown -41%"))

    ledger = {r["EXPERIMENT_ID"]: r for r in pipe.ledger()}
    assert ledger[killed["experiment_id"]]["SURVIVOR_STATE"] == \
        "KILLED_BY_SKEPTIC"
    assert ledger[killed["experiment_id"]]["RESULT"]["reason"]
    assert ledger[held["experiment_id"]]["SURVIVOR_STATE"] == "DATA_HOLD"
    assert ledger[rejected["experiment_id"]]["SURVIVOR_STATE"] == \
        "REJECTED_BY_RISK"
    assert ledger[rejected["experiment_id"]]["RESULT"]["outcome"] == \
        r59.HO_REJECTED
    # they are in the graveyard and in the burden, and re-asking is refused
    grave = {g["hypothesis_id"] for g in pipe.mem.graveyard()}
    assert {killed["experiment_id"], rejected["experiment_id"]} <= grave
    assert pipe.mem.burden()["total"] == 3
    with _refused("ALREADY_SETTLED_DO_NOT_REPEAT"):
        pipe.perform(A.DIRECTOR, "preregister", _prereg_payload(CASES["fx"]))
    # no verb deletes anything
    src = (PACKAGE_DIR / "pipeline.py").read_text(encoding="utf-8")
    assert "DELETE" not in src and ".unlink(" not in src


# --------------------------------------------------------------------------- #
# 5. Signal-publishing safety / no order, no fill, no promotion
# --------------------------------------------------------------------------- #
def test_20_publishing_cannot_create_orders_fills_approvals_or_promotions(pipe):
    eid = _to_cleared(pipe, "equity")
    for verb in ("create_order", "create_orders", "create_fill",
                 "stage_order", "approve_proposal", "promote_champion",
                 "promote_model", "make_capital_eligible",
                 "activate_sleeve", "enable_automation"):
        with _refused("UNKNOWN_VERB"):
            pipe.perform(A.PUBLISHING, verb, dict(experiment_id=eid))
    refusals = pipe.mem.events(kind=P.EV_REFUSED, limit=100)
    assert len(refusals) == 10                    # every attempt is journalled
    assert set(P._VERBS) == set(
        C.load_contract("agent_contracts.json")["pipeline_verbs"])
    assert C.Contracts().verbs_for(A.PUBLISHING) == (
        "publish_candidate", "request_forward_registration")

    out = pipe.perform(A.PUBLISHING, "publish_candidate",
                       dict(experiment_id=eid))
    body = json.loads(Path(out["artifact"]).read_text(encoding="utf-8"))
    assert body["operational_effect"] == "NONE"
    for flag in ("creates_orders", "creates_fills", "broker_enabled",
                 "promotes_model", "automatic_model_promotion_allowed",
                 "approves_proposal", "makes_sleeve_capital_eligible",
                 "mutates_operational_store", "automation_enabled",
                 "backfills_forward_evidence"):
        assert body["safety"][flag] is False, flag
    assert body["safety"]["manual_review_required"] is True
    for label in ("NO ORDERS", "ORDERS DISABLED", "AUTOMATION OFF",
                  "MANUAL REVIEW", "PREVIEW ONLY"):
        assert label in body["safety_labels"]
    assert "never forward evidence" in body["historical_evidence"]["note"]


def test_21_the_package_cannot_reach_an_operational_owner():
    banned_roots = ("api", "db", "engine", "broker", "paper_trader")
    execution_terms = ("place_order", "submit_order", "execute_order",
                       "send_order", "broker_execute", "live_order",
                       "route_order", "create_order(", "create_fill(",
                       "persist_proposal", "approve_proposal(",
                       "declare_conditional_operational_approval")
    for py in sorted(PACKAGE_DIR.glob("*.py")):
        src = py.read_text(encoding="utf-8")
        for node in ast.walk(ast.parse(src)):
            if isinstance(node, ast.Import):
                roots = [a.name.split(".")[0] for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                roots = [(node.module or "").split(".")[0]]
            else:
                continue
            assert not set(roots) & set(banned_roots), (py.name, roots)
        for term in execution_terms:
            assert term not in src, (py.name, term)


def test_22_only_the_director_writes_qualified_and_nothing_is_promoted(pipe):
    reg = _preregister(pipe, "equity")
    eid = reg["experiment_id"]
    _submit(pipe, "equity", reg)
    pipe.perform(A.SKEPTIC, "skeptic_review",
                 dict(experiment_id=eid, checks=_all_checks_pass()))
    # a skeptic pass is NOT a qualification
    assert pipe.mem.get(eid)["outcome"] == r59.HO_NEEDS_MORE_EVIDENCE
    pipe.perform(A.RISK, "risk_review", dict(
        experiment_id=eid, verdict="ACCEPTABLE", metrics={"beta": 0.1}))
    assert pipe.mem.get(eid)["outcome"] == r59.HO_NEEDS_MORE_EVIDENCE
    with _refused("META_REVIEW_REQUIRED"):
        pipe.perform(A.DIRECTOR, "director_clear", dict(
            experiment_id=eid, decision="CLEARED", rationale="skip meta"))
    pipe.perform(A.META, "meta_review",
                 dict(experiment_ids=[eid], verdict="STANDALONE"))
    with _refused("RATIONALE_REQUIRED"):
        pipe.perform(A.DIRECTOR, "director_clear", dict(
            experiment_id=eid, decision="CLEARED", rationale=" "))
    pipe.perform(A.DIRECTOR, "director_clear", dict(
        experiment_id=eid, decision="CLEARED", rationale="final tournament"))
    row = pipe.mem.get(eid)
    assert row["outcome"] == r59.HO_QUALIFIED
    assert row["evidence_maturity"] == "HISTORICAL"
    assert row["statistic"]["lockbox_t"] == 4.5    # evidence carried, not lost
    assert row["robustness"]["skeptic_verdict"] == P.SK_SURVIVED
    assert A.SAFETY["automatic_model_promotion_allowed"] is False


# --------------------------------------------------------------------------- #
# 6. Paper Trader integration: canonical forward accrual, no backfill
# --------------------------------------------------------------------------- #
def test_23_a_request_without_the_adoption_owner_freezes_and_says_so(pipe):
    eid = _to_cleared(pipe, "fx")
    with _refused("NOT_PUBLISHED"):
        pipe.perform(A.PUBLISHING, "request_forward_registration",
                     dict(experiment_id=eid))
    pipe.perform(A.PUBLISHING, "publish_candidate", dict(experiment_id=eid))
    out = pipe.perform(A.PUBLISHING, "request_forward_registration",
                       dict(experiment_id=eid))
    assert out["forward_state"] == "FROZEN_AWAITING_ADOPTION_OWNER"
    assert out["freeze_state"] == "FROZEN"
    assert out["forward_observations_at_request"] == 0
    assert out["frozen_decision_producer"] == "FORWARD_PRODUCER_REQUIRED"
    assert out["capital_eligible"] is False and out["auto_promotion"] is False
    assert out["owners"]["evidence"] == "api.canonical_forward_accrual"
    assert out["owners"]["capital_eligibility"] == \
        "api.capital_eligibility_gate"
    assert pipe.ledger()[0]["FORWARD_STATE"] == \
        "FROZEN_AWAITING_ADOPTION_OWNER"


@pytest.mark.parametrize("name,clock_state", [
    ("commodity", FCR.CLOCK_INSTRUMENT_OWNED),
    ("equity", FCR.CLOCK_RESOLVED)])
def test_24_a_qualified_candidate_reaches_the_real_forward_registrar(
        pipe, tmp_path, name, clock_state):
    eid = _to_cleared(pipe, name)
    pipe.perform(A.PUBLISHING, "publish_candidate", dict(experiment_id=eid))
    registry_dir = tmp_path / "forward_registry"
    today = _dt.datetime.now(_dt.timezone.utc).date().isoformat()

    def adopter(*, freeze_row, observation_clock_starts):
        # the REAL governed owner and the REAL canonical registrar; only their
        # stores are redirected into tmp_path
        return PA.adopt_prospective_freeze(
            freeze_row=freeze_row,
            observation_clock_starts=observation_clock_starts,
            confirm=PA.ADOPT_CONFIRM_TOKEN,
            adoption_dir_override=tmp_path / "adoption",
            registry_dir_override=registry_dir)

    out = pipe.perform(
        A.PUBLISHING, "request_forward_registration",
        dict(experiment_id=eid,
             frozen_decision_producer="alpha_agent.agents_v2 producer TBD"),
        adopt_forward=adopter)
    assert out["forward_state"] == "FORWARD_REGISTERED", out
    assert out["adoption_outcome"] == PA.ADOPTED

    regs = FCR.load_registrations(registry_dir_override=registry_dir)
    assert len(regs) == 1
    reg = regs[0]
    ident = reg["identity"]
    assert ident["challenger_id"] == out["challenger_id"]
    assert ident["asset_class"] == CASES[name]["asset_class"]
    assert ident["instrument_scope"] == CASES[name]["scope"]
    assert ident["cost_model"] == CASES[name]["cost"]

    # NO BACKFILL - asserted on the REGISTRAR'S OWN record, field by field.
    assert reg["registration_session"] >= today
    assert str(ident["inception"])[:10] >= today
    assert reg["backfilled"] is False
    clock = reg["observation_clock"]
    assert clock["state"] == clock_state
    assert clock["backfilled"] is False
    assert clock["effective_from_session"] == reg["registration_session"]
    assert clock["accrued_eligible_sessions"] == 0
    assert clock[
        "sessions_between_inception_and_registration_are_never_synthesised"]
    if clock_state == FCR.CLOCK_RESOLVED:
        # an exchange-calendar asset: the first observation is STRICTLY after
        # the registration session, so no completed session can be a prediction
        assert clock["first_eligible_observation_session"] > \
            reg["registration_session"]
    # registration starts a MEASUREMENT: zero evidence, no promotion
    for zero in ("predictions_emitted", "matured_observations",
                 "pending_observations", "forward_observations_at_registration",
                 "effective_independent_observations"):
        assert reg[zero] == 0, zero
    assert reg["promotion_allowed"] is False
    assert reg["automatic_promotion_allowed"] is False
    assert reg["manual_review_required"] is True
    assert reg["maturation_owner"] == A.FORWARD_MATURATION_OWNER
    assert reg["safety"]["created_orders"] is False
    assert reg["safety"]["created_fills"] is False

    # two rows, two facts: the historical result and the prospective freeze
    hist = pipe.mem.get(eid)
    assert hist["outcome"] == r59.HO_QUALIFIED
    assert hist["evidence_maturity"] == "HISTORICAL"
    frozen = [h for h in pipe.mem.list_hypotheses(
        outcome=r59.HO_FORWARD_FROZEN)]
    assert len(frozen) == 1
    assert frozen[0]["forward_challenger"]["forward_observations_at_freeze"] == 0
    assert frozen[0]["counts_to_burden"] == 0
    assert frozen[0]["spec"]["frozen_from"] == eid

    # idempotent: asking again resolves the SAME registration
    again = pipe.perform(
        A.PUBLISHING, "request_forward_registration",
        dict(experiment_id=eid), adopt_forward=adopter)
    assert again["forward_state"] == "FORWARD_REGISTERED"
    assert len(FCR.load_registrations(registry_dir_override=registry_dir)) == 1


def test_25_a_backfill_cannot_even_be_asked_for(pipe):
    eid = _to_cleared(pipe, "h1_h5")
    pipe.perform(A.PUBLISHING, "publish_candidate", dict(experiment_id=eid))
    for key in P.BACKFILL_KEYS:
        with _refused("BACKFILL_CANNOT_BE_REQUESTED"):
            pipe.perform(A.PUBLISHING, "request_forward_registration",
                         {"experiment_id": eid, key: "2024-01-02"})
    # and the adoption owner itself refuses a clock before the inception
    out = pipe.perform(A.PUBLISHING, "request_forward_registration",
                       dict(experiment_id=eid))
    frozen = pipe.mem.list_hypotheses(outcome=r59.HO_FORWARD_FROZEN)[0]
    refused = PA.adopt_prospective_freeze(
        freeze_row=frozen, observation_clock_starts="2020-01-02",
        confirm=PA.ADOPT_CONFIRM_TOKEN)
    assert refused["outcome"] == PA.REFUSED_BACKDATED
    assert refused["adopted"] is False
    assert out["forward_state"] == "FROZEN_AWAITING_ADOPTION_OWNER"
    # and the registry owner refuses to mint forward evidence from a backtest
    with pytest.raises(ValueError, match="cannot mint forward evidence"):
        pipe.mem.record_result(eid, outcome=r59.HO_QUALIFIED,
                               evidence_maturity="FORWARD_CONFIRMED")


def test_26_an_unqualified_experiment_can_never_be_frozen(pipe):
    reg = _preregister(pipe, "fx")
    _submit(pipe, "fx", reg, layers=_weak())
    pipe.perform(A.SKEPTIC, "skeptic_review", dict(
        experiment_id=reg["experiment_id"], checks=_all_checks_pass()))
    with _refused("NOT_DIRECTOR_CLEARED"):
        pipe.perform(A.PUBLISHING, "publish_candidate",
                     dict(experiment_id=reg["experiment_id"]))
    with _refused("NOT_PUBLISHED"):
        pipe.perform(A.PUBLISHING, "request_forward_registration",
                     dict(experiment_id=reg["experiment_id"]))
    assert pipe.mem.list_hypotheses(outcome=r59.HO_FORWARD_FROZEN) == []


# --------------------------------------------------------------------------- #
# 7. The preparatory census is executable under the contract it was written for
# --------------------------------------------------------------------------- #
def test_27a_the_queued_campaign_is_routable_diverse_and_spent_nothing():
    census = json.loads((A.CONTRACT_DIR / "NEXT_CAMPAIGN_CENSUS.json")
                        .read_text(encoding="utf-8"))
    assert census["agent_system_version"] == A.AGENT_SYSTEM_VERSION
    assert "No experiment was run" in census["method"]
    assert census["estate_facts"]["agent_v2_experiments_in_memory"] == 0
    contracts = C.Contracts()
    queue = census["queued_hypotheses"]
    assert 10 <= len(queue) <= 15
    assert [q["rank"] for q in queue] == list(range(1, len(queue) + 1))
    for q in queue:
        assert q["agent"] in A.SIGNAL_AGENTS, q["proposal"]
        assert q["family"] in contracts.families_for(q["agent"]), q["proposal"]
        assert q["asset_class"] in r59.ASSET_CLASSES, q["proposal"]
        assert q["expected_sign"] in (1, -1), q["proposal"]
        assert q["hypothesis"] and q["dataset"] and q["prior"], q["proposal"]
    # the recommended campaign obeys the director's own diversity protocol
    rule = C.load_contract("research_director_protocol.json")[
        "hypothesis_diversity"]
    picked = [q for q in queue if q["rank"]
              in census["recommended_first_campaign"]["members_by_rank"]]
    classes = [q["asset_class"] for q in picked]
    assert len(set(classes)) >= rule["min_asset_classes_per_campaign"]
    assert max(classes.count(c) for c in set(classes)) / len(picked) <= \
        rule["max_share_single_asset_class"]
    non_equity = sum(1 for c in classes if c != r59.AC_US_EQUITY)
    assert non_equity / len(picked) >= rule["non_equity_reservation"]
    assert any(q["horizon_sessions"] <= 5 for q in picked)
    assert any(q["asset_class"] == r59.AC_CROSS_ASSET for q in picked)
    budget = C.load_contract("research_director_protocol.json")[
        "experiment_budget"]["default_max_experiments_per_campaign"]
    assert len(picked) <= budget


# --------------------------------------------------------------------------- #
# 8. The PowerShell entrypoint
# --------------------------------------------------------------------------- #
def test_27_the_cli_never_adopts_and_is_not_a_second_operator_door():
    """Paper Trader pins exactly ONE operator adoption entrypoint and a closed
    list of adoption injectors (test_release62_1_1 15c). The agents' command
    must be neither: it freezes, names the one door, and stops."""
    src = (REPO / "scripts" / "alpha_agents_v2.py").read_text(encoding="utf-8")
    assert "adopt_prospective_freeze(" not in src
    assert "from api import" not in src and "import prospective_adoption" \
        not in src
    assert PA.OPERATOR_ADOPT_CONFIRM_TOKEN not in src
    assert PA.ADOPT_CONFIRM_TOKEN not in src
    for banned in ("--execute", "--confirm", "--effective-from", "--date",
                   "--backfill", "--inception", "--as-of"):
        assert ('"%s"' % banned) not in src, banned
    assert "read_only=True" in src
    assert "assert_worktree_import()" in src
    assert A.OPERATOR_ADOPTION_ENTRYPOINT == PA.OPERATOR_ADOPTION_ENTRYPOINT
    # and the research package itself names no confirmation token either
    for py in PACKAGE_DIR.glob("*.py"):
        text = py.read_text(encoding="utf-8")
        assert PA.OPERATOR_ADOPT_CONFIRM_TOKEN not in text, py.name
        assert PA.ADOPT_CONFIRM_TOKEN not in text, py.name


def test_28_the_one_operator_door_can_adopt_an_agent_freeze(pipe, tmp_path):
    """The handoff to canonical forward accrual, as it happens in production:
    the agents freeze DRY, and the human operator's one door resolves that
    freeze by challenger id and adopts it through the real owners."""
    eid = _to_cleared(pipe, "fx")
    pipe.perform(A.PUBLISHING, "publish_candidate", dict(experiment_id=eid))
    req = pipe.perform(A.PUBLISHING, "request_forward_registration",
                       dict(experiment_id=eid))
    assert req["forward_state"] == "FROZEN_AWAITING_ADOPTION_OWNER"
    door = req["operator_adoption"]
    assert door["entrypoint"] == PA.OPERATOR_ADOPTION_ENTRYPOINT
    assert door["performed_by"] == "the human operator"
    assert req["challenger_id"] in door["argument"]

    # exactly what scripts/adopt_prospective_freeze.py does with that id
    rows = pipe.mem.list_hypotheses(outcome=r59.HO_FORWARD_FROZEN)
    resolved = PA.resolve_freeze_by_challenger_id(req["challenger_id"], rows)
    assert resolved["outcome"] == PA.FREEZE_RESOLVED and resolved["matches"] == 1
    registry_dir = tmp_path / "operator_registry"
    out = PA.adopt_prospective_freeze(
        freeze_row=resolved["freeze_row"],
        observation_clock_starts=PA.current_prospective_boundary(),
        confirm=PA.ADOPT_CONFIRM_TOKEN,
        adoption_dir_override=tmp_path / "operator_adoption",
        registry_dir_override=registry_dir)
    assert out["outcome"] == PA.ADOPTED and out["adopted"] is True
    assert out["challenger_class"] == PA.CLASS_SIGNAL_CANONICAL
    regs = FCR.load_registrations(registry_dir_override=registry_dir)
    assert [r["challenger_id"] for r in regs] == [req["challenger_id"]]
    assert regs[0]["backfilled"] is False
    assert regs[0]["forward_observations_at_registration"] == 0
