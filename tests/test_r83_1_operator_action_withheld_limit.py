"""R83.1 — the ONE ACTION lane agrees with the governed WITHHELD verdict.

After R83 every portfolio-decision surface correctly read the 2026-09-28 state as
PORTFOLIO CHANGE WITHHELD (RISK_CONTRIBUTION_CAP_BREACH_BLOCKS_CHANGE, AMD / SNDK over
the 15% per-name limit) with the outstanding act REVIEW THE PORTFOLIO LIMIT THAT
WITHHELD THE CHANGE — while the Today "WHAT SHOULD THE OPERATOR DO NOW? — ONE ACTION"
panel said "No action now", because ``api.workflow_state.build_operator_action``
mapped DAILY_CYCLE_COMPLETE straight to MONITOR_PORTFOLIO.

Cycle completion is not the absence of human work. R83.1 keeps the lifecycle state
(DAILY_CYCLE_COMPLETE) and makes the ONE operator action publish the withheld-outcome
owner's own next act, from the backend, for exactly that governed condition.

Everything here is hermetic: the R74 harness runs a REAL Daily Research Cycle over a
tmp store with a withheld reallocation seam. No live store is opened.
"""
from __future__ import annotations

import copy
import hashlib
import inspect
from pathlib import Path

import pytest

from paper_trader.api import active_manager_state as ams
from paper_trader.api import reallocation_proposal as rp
from paper_trader.api import workflow_state as ws

from tests.test_r74_withheld_terminal_contract import _withheld_run  # noqa: E402
from tests.test_r83_governed_proposal_handoff import (  # noqa: E402
    _artifacts, _decision, _index, _read)

REVIEW = "REVIEW_THE_WITHHELDING_PORTFOLIO_LIMIT"
UI = Path(__file__).resolve().parent.parent / "api" / "ui" / "index.html"


# --------------------------------------------------------------------------- #
# Hermetic helpers.
# --------------------------------------------------------------------------- #
def _tree_hash(root: Path) -> str:
    h = hashlib.sha256()
    for p in sorted(root.rglob("*")):
        if p.is_file():
            h.update(str(p.relative_to(root)).encode())
            h.update(p.read_bytes())
    return h.hexdigest()


def _complete_cmd():
    """The canonical operator command in a fully-processed state (not executable)."""
    return ws.build_operator_command(
        overall=ws.DAILY_CYCLE_COMPLETE,
        primary={"label": "Monitor", "headline": "Daily cycle complete",
                 "current_task": "Monitor", "execution_available": False,
                 "destination": ws.DEST_COMMAND_CENTER},
        eligible_date="2026-08-04", latest_close_date="2026-08-04")


def _act(cpd, *, overall=ws.DAILY_CYCLE_COMPLETE, **kw):
    return ws.build_operator_action(overall=overall, command=_complete_cmd(),
                                    canonical_decision=cpd, **kw)


@pytest.fixture()
def withheld(tmp_path):
    _withheld_run(tmp_path)
    cpd = _decision(tmp_path)
    return tmp_path, cpd


# =========================================================================== #
# The governed state itself (precondition, not the fix).
# =========================================================================== #
def test_00_the_hermetic_state_is_the_governed_withheld_verdict(withheld):
    tmp, cpd = withheld
    assert cpd["state"] == ws.CPD_CHANGE_WITHHELD
    assert cpd["headline"] == "PORTFOLIO CHANGE WITHHELD"
    assert cpd["complete_target_withheld_on_portfolio_limits"] is True
    wh = cpd["governed_withheld_outcome"]
    assert wh["terminal"] is True and wh["approvable"] is False
    assert wh["next_required_action"] == REVIEW
    assert wh["proposal_id"] is None and wh["persisted"] is False


# =========================================================================== #
# Assertions 1-10.
# =========================================================================== #
def test_01_the_cycle_lifecycle_stays_complete(withheld):
    _, cpd = withheld
    a = _act(cpd)
    assert a["overall_state"] == ws.DAILY_CYCLE_COMPLETE
    assert a["cycle_lifecycle_state_unchanged"] is True


def test_02_the_one_action_is_not_no_action_now(withheld):
    _, cpd = withheld
    a = _act(cpd)
    assert a["action"] != ws.OP_ACTION_MONITOR
    assert a["action_label"] != "No action now"
    assert a["is_passive"] is False and a["requires_operator_work"] is True


def test_03_the_one_action_names_the_portfolio_limit_review(withheld):
    _, cpd = withheld
    a = _act(cpd)
    assert a["action"] == REVIEW == ws.OP_ACTION_REVIEW_WITHHELDING_LIMIT
    assert a["action_label"] == "Review the portfolio limit that withheld the change"
    assert a["withheld_limit_review_outstanding"] is True
    # The owner's own sentence, verbatim, names the breaching positions.
    wh = cpd["governed_withheld_outcome"]
    assert a["why"] == wh["outstanding_governance_requirement"]
    for t in wh["withheld_breaching_tickers"]:
        assert t in a["why"]
    assert a["withheld_breaching_tickers"] == wh["withheld_breaching_tickers"]
    assert a["withheld_codes"] == wh["withheld_codes"]
    assert a["withheld_governed_run_id"] == wh["governed_run_id"]


def test_04_to_08_the_action_is_read_only_navigation(withheld):
    tmp, cpd = withheld
    before = _tree_hash(tmp)
    a = _act(cpd)
    assert a["review_only"] is True
    assert a["executes"] is False
    assert a["execution_kind"] is None and a["execution_contract"] is None
    assert a["execution_label"] is None and a["confirmation_required"] is None
    assert a["records_a_decision"] is False
    assert a["creates_target_selection"] is False
    assert a["approves_anything"] is False
    assert a["creates_orders"] is False and a["creates_order_plan"] is False
    assert a["automation_enabled"] is False
    # The review lands on the reallocation card that states the verdict.
    assert a["destination"] == ws.DEST_PORTFOLIO_MANAGER
    assert a["destination"] in ws.VALID_DESTINATIONS
    assert a["focus"] == "realloc-card"
    # Nothing was written by composing it.
    assert _tree_hash(tmp) == before
    assert _artifacts(tmp) == [] and _index(tmp) == {}


def test_09_the_action_is_backend_owned(withheld):
    _, cpd = withheld
    a = _act(cpd)
    assert a["owner"] == ws.WORKFLOW_STATE_OWNER
    assert a["derived_in_ui"] is False
    assert a["withheld_limit_review_source"] == rp.COMPOSITION_OWNER
    assert REVIEW in a["action_vocabulary"]
    assert a["priority_rank"] == ws.OPERATOR_ACTION_PRIORITY.index(REVIEW)


def test_09b_the_composer_feeds_the_canonical_decision_to_the_owner():
    src = inspect.getsource(ws.load_workflow_state)
    call = src[src.find("operator_action = build_operator_action("):]
    call = call[:call.find("return {")]
    assert "canonical_decision=canonical_portfolio_decision" in call
    assert "execution_active=" in call


def test_10_active_manager_and_the_ui_render_it_verbatim(withheld):
    _, cpd = withheld
    a = _act(cpd)
    block = ams._operator_answer_block(
        governed={}, canonical=cpd, lane={}, live_information={},
        operational_book={},
        operator_guidance={"operator_action": a, "operator_command": _complete_cmd()})
    now = block["what_to_do_now"]
    assert now["action"] == REVIEW
    assert now["action_label"] == a["action_label"]
    assert now["requires_operator_work"] is True
    assert now["destination"] == ws.DEST_PORTFOLIO_MANAGER
    # The browser: renders action_label when work is required; its only knowledge
    # of the new code is a TONE lookup. No decision-state branching in the panel.
    src = UI.read_text(encoding="utf-8", errors="replace")
    start = src.find("function _r55RenderOperatorAnswer")
    body = src[start:src.find("host.setAttribute('data-operator-action'", start)]
    assert "act.requires_operator_work ? (act.action_label" in body
    assert "CHANGE_CANDIDATE_WITHHELD" not in body.split("ANSWER 3")[1]
    assert REVIEW not in body
    tone = src[src.find("var _R55_ACTION_TONE"):src.find("var _R55_DECISION_TONE")]
    assert "%s: 'tone-warn'" % REVIEW in tone


# =========================================================================== #
# Assertions 11-13 — coherence with R83; no persistence; no recomputation.
# =========================================================================== #
def test_11_today_and_portfolio_stay_coherent_with_r83(withheld):
    from paper_trader.api import operator_presentation as op
    _, cpd = withheld
    a = _act(cpd)
    pd = op._portfolio_decision({"canonical_portfolio_decision": cpd}, {}, {},
                                {"historical": False})
    assert "WITHHELD" in pd["headline"]
    na = pd.get("next_action") or {}
    assert na.get("kind") == op.NA_REVIEW_REALLOCATION
    assert na.get("executes") is False and a["executes"] is False
    # Every surface names the same act, from the same owner field.
    assert (cpd["governed_withheld_outcome"]["next_required_action"]
            == a["action"] == REVIEW)


def test_12_withheld_remains_unpersisted_and_unapprovable(withheld):
    tmp, cpd = withheld
    _act(cpd)
    d = _read(tmp)
    assert d["state"] == rp.STATE_WITHHELD
    assert d["approvable"] is False and d["executable"] is False
    assert rp.STATE_WITHHELD not in rp.APPROVABLE_READ_STATES
    assert _artifacts(tmp) == []


def test_13_the_owner_recomputes_no_limit_or_risk_contribution():
    src = inspect.getsource(ws.build_operator_action)
    for token in ("0.15", "risk_contribution_pct", "limit\"]", "* 100"):
        assert token not in src, token


# =========================================================================== #
# Assertions 14-17 — the narrowing and the precedence.
# =========================================================================== #
@pytest.mark.parametrize("state", [ws.CPD_HOLD_CURRENT_BOOK, ws.CPD_NO_CHANGE,
                                   ws.CPD_NOT_RUN])
def test_14_to_16_benign_states_keep_no_action_now(state):
    a = _act({"state": state, "complete_target_withheld_on_portfolio_limits": False,
              "governed_withheld_outcome": None})
    assert a["action"] == ws.OP_ACTION_MONITOR
    assert a["action_label"] == "No action now"
    assert a["withheld_limit_review_outstanding"] is False


def test_16b_no_canonical_decision_is_unchanged_behaviour():
    a = ws.build_operator_action(overall=ws.DAILY_CYCLE_COMPLETE)
    assert a["action"] == ws.OP_ACTION_MONITOR


def test_16c_an_economic_withholding_is_not_a_limit_review(withheld):
    _, cpd = withheld
    econ = dict(cpd, complete_target_withheld_on_portfolio_limits=False,
                governed_withheld_outcome=None)
    assert _act(econ)["action"] == ws.OP_ACTION_MONITOR


@pytest.mark.parametrize("field,value", [
    ("terminal", False), ("governed_outcome_complete", False),
    ("approvable", True), ("withheld", False),
    ("next_required_action", "RUN_DAILY_RESEARCH_CYCLE")])
def test_16d_an_unproven_verdict_raises_no_review(withheld, field, value):
    _, cpd = withheld
    bad = copy.deepcopy(cpd)
    bad["governed_withheld_outcome"][field] = value
    assert _act(bad)["action"] == ws.OP_ACTION_MONITOR


@pytest.mark.parametrize("overall", [
    ws.INCONSISTENT_STATE, ws.RESEARCH_CYCLE_BLOCKED, ws.WAITING_FOR_OWNED_DATA,
    ws.READY_FOR_DAILY_CLOSE, ws.RESEARCH_CYCLE_REQUIRED,
    ws.PORTFOLIO_REASSESSMENT_REQUIRED, ws.RESEARCH_CYCLE_RUNNING,
    ws.DAILY_CLOSE_RUNNING, ws.MANUAL_REVIEW_REQUIRED])
def test_17_every_higher_priority_state_still_wins(withheld, overall):
    _, cpd = withheld
    a = ws.build_operator_action(overall=overall, canonical_decision=cpd)
    assert a["action"] == ws.OPERATOR_ACTION_BY_OVERALL[overall]
    assert a["withheld_limit_review_outstanding"] is False


def test_17b_resuming_owed_research_still_outranks_the_review(withheld):
    _, cpd = withheld
    a = ws.build_operator_action(
        overall=ws.RESEARCH_CYCLE_REQUIRED, canonical_decision=cpd,
        research_obligation={"obligation_outstanding": True,
                             "operational_close_valid": True})
    assert a["action"] == ws.OP_ACTION_RESUME_RESEARCH


def test_17c_working_paper_orders_keep_execution_precedence(withheld):
    _, cpd = withheld
    assert _act(cpd, execution_active=True)["action"] == ws.OP_ACTION_MONITOR


def test_17d_the_review_also_stands_while_the_next_session_is_open(withheld):
    _, cpd = withheld
    for overall in (ws.WAITING_FOR_SESSION_CLOSE,
                    ws.DAILY_CYCLE_COMPLETE_EVIDENCE_GAP):
        assert _act(cpd, overall=overall)["action"] == REVIEW, overall


def test_17e_the_published_rank_order():
    p = ws.OPERATOR_ACTION_PRIORITY
    assert len(p) == len(set(p)) and set(p) == set(ws.OPERATOR_ACTIONS)
    for higher in (ws.OP_ACTION_BLOCKED, ws.OP_ACTION_WAIT_OWNED_DATA,
                   ws.OP_ACTION_RUN_CYCLE, ws.OP_ACTION_RESUME_RESEARCH,
                   ws.OP_ACTION_REVIEW_PROPOSAL):
        assert p.index(higher) < p.index(REVIEW), higher
    for passive in (ws.OP_ACTION_WAIT_SESSION_CLOSE, ws.OP_ACTION_MONITOR):
        assert p.index(REVIEW) < p.index(passive), passive
    assert REVIEW not in ws.OPERATOR_ACTION_PASSIVE


# =========================================================================== #
# Assertions 18-19 — idempotency and hermeticity.
# =========================================================================== #
def test_18_three_refreshes_are_identical_and_write_nothing(withheld):
    tmp, _ = withheld
    before = _tree_hash(tmp)
    outs = [_act(_decision(tmp)) for _ in range(3)]
    assert outs[0] == outs[1] == outs[2]
    assert _tree_hash(tmp) == before


def test_19_the_harness_is_hermetic(withheld):
    tmp, cpd = withheld
    wh = cpd["governed_withheld_outcome"]
    # The verdict under test came from the tmp manifest, not the live store.
    assert wh["governed_run_id"]
    assert any(wh["governed_run_id"] in p.name or wh["governed_run_id"] in str(p)
               for p in tmp.rglob("*"))


def test_20_no_dialog_was_introduced():
    src = UI.read_text(encoding="utf-8", errors="replace")
    tone = src[src.find("var _R55_ACTION_TONE"):src.find("function _r55RenderOperatorAnswer")]
    assert "alert(" not in tone and "confirm(" not in tone
