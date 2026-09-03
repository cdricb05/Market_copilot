r"""R54.2.3.2 — authoritative decision / proposal supersession reconciliation.

The live 2026-09-02 defect this release removes: the Sep-2 Portfolio Cycle
completed and the governed Daily Research Cycle concluded CURRENT_NO_CHANGE at
23:51:50Z (manifest ``drc_2026-09-02_15abfb01856f``, reallocation step
NOT_REQUIRED) — while a live event-cycle proposal produced thirteen minutes
earlier (23:38Z, 28 changes, 35% turnover, $85.69) remained the proposal-index
head and every surface presented it as reviewable: Today rendered
"REALLOCATE — 28 POSITIONS CHANGE" beside its own narrative "No change is
proposed", and the backend would have recorded an approval on it.

These tests prove the canonical authority rule end to end, hermetically (every
store under tmp_path; no production root is ever read or written):

    newer governed completed-session decision
        > older governed completed-session decision
        > any older proposal awaiting manual review

and that a non-governed / governance-withheld intraday research result never
supersedes a governed decision.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from paper_trader.api import operator_presentation as op
from paper_trader.api import portfolio_decision as pdec
from paper_trader.api import reallocation_proposal as rp
from paper_trader.api import workflow_state as ws

REPO = Path(__file__).resolve().parents[1]

BOOK = "alpha_paper_book_1"
SEP1, SEP2 = "2026-09-01", "2026-09-02"
HOC_V1 = "702c599ee5b38535f19651792b8d59bcd0bafc322cc1cac4ecbe9c72bdbc23f7"
HOC_V2 = "a162fca969c93831be7e3b22121af9f7b1378de875a72bb09801f9950e22a8c3"
RHASH_V1 = "74776dda34acced4d378aef5a104c9d6f0713115833148f61f896ec9c7cf28ff"
RHASH_V2 = "029df5cdcda5b4bf948475337bd596e2f30f1159c18a92b84c794bdae7cc5e22"
PHASH = "dcf85725a02e564757ecb6b2cbba3e0f9c582d6f1230900250e313ae03f5f59c"
COUNTS_28 = {"ADD": 6, "EXIT": 14, "INCREASE": 5, "REDUCE": 1,
             "REPLACE_IN": 2, "REPLACE_OUT": 0, "RETAIN": 10}


def _write(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True),
                    encoding="utf-8")


# --------------------------------------------------------------------------- #
# Hermetic store builders (replaying the live artifact shapes)
# --------------------------------------------------------------------------- #
def make_proposal(realloc_dir: Path, *, session: str, phash: str = PHASH,
                  hoc_hash: str = HOC_V1,
                  generated_at: str = "2026-09-02T23:38:15+00:00",
                  state: str = "READY", outcome: str = "PROPOSAL_READY") -> str:
    pid = "reap_%s_%s_%s" % (session, BOOK, phash[:12])
    artifact = {
        "proposal_id": pid,
        "generated_at": generated_at,
        "identity": {"eligible_market_date": session, "active_book_id": BOOK,
                     "proposal_hash": phash, "hoc_assessment_hash": hoc_hash,
                     "portfolio_state_hash": "ps" + session,
                     "corporate_actions_hash": None,
                     "universe_scoring_hash": "us1",
                     "allocation_policy_version": "reallocation_allocation_policy.v1"},
        "input_contract": {"eligible_market_date": session,
                           "active_book_id": BOOK,
                           "hoc_assessment_hash": hoc_hash},
        "proposal": {
            "proposal_state": state, "proposal_hash": phash, "outcome": outcome,
            "approvable": True, "action_counts": dict(COUNTS_28),
            "turnover": {"one_way_turnover": 0.35, "gross_sells": 1.0,
                         "gross_buys": 1.0, "estimated_transaction_cost": 85.69},
            "signal": {"score_improvement": 0.0732,
                       "score_improvement_net_of_cost": 0.0557},
            "portfolio": {"proposed_holding_count": 23},
            "reallocation_outcome": {"feasible_target_exists": True},
            "data_gaps": [], "withheld_reasons": [],
        },
    }
    path = realloc_dir / "artifacts" / ("%s.json" % pid)
    _write(path, artifact)
    index = {}
    ipath = realloc_dir / "index.json"
    if ipath.exists():
        index = json.loads(ipath.read_text(encoding="utf-8"))
    index["%s|%s" % (BOOK, session)] = {
        "proposal_id": pid, "path": str(path), "proposal_hash": phash,
        "hoc_assessment_hash": hoc_hash, "portfolio_state_hash": "ps" + session,
        "universe_scoring_hash": "us1",
        "allocation_policy_version": "reallocation_allocation_policy.v1",
        "eligible_market_date": session, "active_book_id": BOOK,
        "proposal_state": state, "generated_at": generated_at}
    _write(ipath, index)
    return pid


def make_assessment(reassessment_dir: Path, *, session: str, decision: str,
                    rhash: str, hoc_hash: str, generated_at: str,
                    binding_hoc: str | None = None) -> str:
    aid = "prs_%s_%s_%s" % (session, BOOK, rhash[:12])
    entry = {"active_book_id": BOOK, "artifact_id": aid, "decision": decision,
             "eligible_market_date": session, "generated_at": generated_at,
             "hoc_assessment_hash": hoc_hash, "reassessment_hash": rhash,
             "path": str(reassessment_dir / "artifacts" / ("%s.json" % aid))}
    index = {}
    ipath = reassessment_dir / "index.json"
    if ipath.exists():
        index = json.loads(ipath.read_text(encoding="utf-8"))
    index["%s|%s" % (BOOK, session)] = entry
    _write(ipath, index)
    reassess = {"reassessment_state": decision, "reassessment_hash": rhash,
                "eligible_market_date": session, "active_book_id": BOOK,
                "decision": {"proposal_required": decision == "PROPOSAL_READY"},
                "explanation": "hermetic"}
    if binding_hoc:
        reassess["proposal_binding"] = {"hoc_assessment_hash": binding_hoc}
    _write(Path(entry["path"]),
           {"reassessment_id": aid, "generated_at": generated_at,
            "identity": {"hoc_assessment_hash": hoc_hash,
                         "reassessment_hash": rhash,
                         "eligible_market_date": session},
            "reassessment": reassess})
    return aid


def make_manifest(drc_dir: Path, *, session: str, reassessment_hash: str,
                  reassessment_state: str, state: str = "COMPLETE",
                  proposal_hash: str | None = None) -> str:
    run_id = "drc_%s_hermetic" % session
    index = {}
    ipath = drc_dir / "index.json"
    if ipath.exists():
        index = json.loads(ipath.read_text(encoding="utf-8"))
    index[session] = {"run_id": run_id, "state": state,
                      "idempotency_key": "k", "input_contract_hash": "h"}
    _write(ipath, index)
    _write(drc_dir / "runs" / ("%s.json" % run_id), {
        "run_id": run_id, "state": state, "eligible_market_date": session,
        "portfolio_reassessment_id": "prs_%s_%s" % (session, reassessment_hash[:12]),
        "portfolio_reassessment_hash": reassessment_hash,
        "portfolio_reassessment_state": reassessment_state,
        "reallocation_proposal_id": None,
        "reallocation_proposal_hash": proposal_hash,
        "reallocation_proposal_state": ("NOT_REQUIRED" if proposal_hash is None
                                        else "READY"),
        "completed_at": "2026-09-02T23:51:52+00:00"})
    return run_id


@pytest.fixture()
def stores(tmp_path):
    return {"realloc": tmp_path / "realloc", "reassess": tmp_path / "reassess",
            "drc": tmp_path / "drc", "decisions": tmp_path / "decisions"}


@pytest.fixture()
def live_world(stores):
    """The exact live pattern: Sep-1 proposal READY awaiting review, then the
    Sep-2 governed CURRENT_NO_CHANGE decision (manifest-bound, proposal
    NOT_REQUIRED)."""
    pid = make_proposal(stores["realloc"], session=SEP1, hoc_hash=HOC_V1,
                        generated_at="2026-09-02T20:31:25+00:00")
    make_assessment(stores["reassess"], session=SEP2, decision="CURRENT_NO_CHANGE",
                    rhash=RHASH_V2, hoc_hash=HOC_V2,
                    generated_at="2026-09-02T23:51:50+00:00")
    make_manifest(stores["drc"], session=SEP2, reassessment_hash=RHASH_V2,
                  reassessment_state="CURRENT_NO_CHANGE")
    return {"proposal_id": pid, **stores}


def _summary(world):
    return rp.load_proposal_summary(active_book_id=BOOK,
                                    eligible_market_date=SEP1,
                                    reallocation_dir=world["realloc"])


def _verdict(world, summ=None):
    return pdec.load_decision_supersession(
        active_book_id=BOOK, proposal_summary=summ or _summary(world),
        reassessment_dir=world["reassess"], drc_dir=world["drc"],
        decision_dir=world["decisions"])


# =========================================================================== #
# 1-6 — the store-level contract
# =========================================================================== #
def test_01_older_proposal_ready_exists(live_world):
    summ = _summary(live_world)
    assert summ["reallocation_proposal_available"] is True
    assert summ["reallocation_proposal_state"] == "READY"
    assert summ["reallocation_proposal_id"] == live_world["proposal_id"]
    assert summ["reallocation_one_way_turnover"] == 0.35


def test_02_newer_governed_no_change_decision_exists(live_world):
    from paper_trader.api import daily_research_cycle as drc
    from paper_trader.api import portfolio_reassessment as prs
    ptr = prs.load_latest_assessment_pointer(
        active_book_id=BOOK, reassessment_dir=live_world["reassess"])
    assert ptr["decision"] == "CURRENT_NO_CHANGE"
    assert ptr["eligible_market_date"] == SEP2
    ref = drc.load_governed_manifest_reference(
        eligible_market_date=SEP2, drc_dir=live_world["drc"])
    assert ref["governed"] is True
    assert ref["portfolio_reassessment_hash"] == RHASH_V2
    assert ref["reallocation_proposal_state"] == "NOT_REQUIRED"


def test_03_older_proposal_becomes_superseded(live_world):
    v = _verdict(live_world)
    assert v["superseded"] is True
    assert v["reason"] == pdec.SUP_NEWER_SESSION_DECISION
    by = v["superseded_by"]
    assert by["decision"] == "CURRENT_NO_CHANGE"
    assert by["session"] == SEP2
    assert by["governed_provenance"] == "GOVERNED_DAILY_CYCLE"


def test_04_superseded_proposal_stays_immutable_history(live_world):
    art_path = (live_world["realloc"] / "artifacts"
                / ("%s.json" % live_world["proposal_id"]))
    before = art_path.read_bytes()
    payload = rp.load_reallocation_proposal(
        portfolio_state={"active_book": {"book_id": BOOK},
                         "dates": {"eligible_market_date": SEP1}},
        reallocation_dir=live_world["realloc"],
        reassessment_dir=live_world["reassess"], drc_dir=live_world["drc"],
        decision_dir=live_world["decisions"])
    # History-visible: the artifact metadata and full proposal body still render.
    assert payload["artifact"]["proposal_id"] == live_world["proposal_id"]
    assert payload["artifact"]["immutable"] is True
    assert payload["action_counts"] == COUNTS_28
    # And immutable: the read changed not one byte of the artifact.
    assert art_path.read_bytes() == before


def test_05_superseded_proposal_is_not_current(live_world):
    payload = rp.load_reallocation_proposal(
        portfolio_state={"active_book": {"book_id": BOOK},
                         "dates": {"eligible_market_date": SEP1}},
        reallocation_dir=live_world["realloc"],
        reassessment_dir=live_world["reassess"], drc_dir=live_world["drc"],
        decision_dir=live_world["decisions"])
    assert payload["state"] == rp.STATE_SUPERSEDED
    assert payload["superseded"] is True
    assert payload["approvable"] is False
    assert payload["executable"] is False
    assert "SUPERSEDED" in payload["message"]
    # The decision lane agrees: superseded, no review, nothing approvable.
    summ = _summary(live_world)
    v = _verdict(live_world, summ)
    lane = pdec.derive_decision_state(
        has_active_book=True,
        proposal_summary={**summ, "reallocation_proposal_superseded": True,
                          "reallocation_proposal_supersession": v},
        decision_record=None)
    assert lane["portfolio_decision_state"] == pdec.PDS_SUPERSEDED
    assert lane["requires_manual_review"] is False
    assert lane["approvable"] is False


def test_06_approval_is_rejected_backend_side(live_world):
    art = rp.load_latest_artifact(active_book_id=BOOK, eligible_market_date=SEP1,
                                  reallocation_dir=live_world["realloc"])
    res = pdec.record_decision(
        decision=pdec.DECISION_APPROVE, confirm=pdec.CONFIRM_TOKEN,
        artifact=art, expected_proposal_hash=PHASH,
        decision_dir=live_world["decisions"],
        reallocation_dir=live_world["realloc"],
        reassessment_dir=live_world["reassess"], drc_dir=live_world["drc"])
    assert res["status"] == pdec.PDS_SUPERSEDED
    assert res["recorded"] is False
    # The rejection identifies the newer decision and session.
    assert "CURRENT_NO_CHANGE" in res["message"]
    assert SEP2 in res["message"]
    # Nothing was written to the decision ledger.
    assert not (live_world["decisions"] / "decisions.json").exists()
    assert not (live_world["decisions"] / "index.json").exists()
    # REJECT and HOLD are refused identically — there is no current decision to
    # record on a superseded proposal.
    for word in (pdec.DECISION_REJECT, pdec.DECISION_HOLD):
        r2 = pdec.record_decision(
            decision=word, confirm=pdec.CONFIRM_TOKEN, artifact=art,
            decision_dir=live_world["decisions"],
            reallocation_dir=live_world["realloc"],
            reassessment_dir=live_world["reassess"], drc_dir=live_world["drc"])
        assert r2["status"] == pdec.PDS_SUPERSEDED and r2["recorded"] is False


# =========================================================================== #
# 7-12 — Today / presentation consistency (backend-decided, rendered verbatim)
# =========================================================================== #
def _wf_after_fix():
    """The workflow fields the presentation reads, as the fixed composition
    publishes them for the live world (CPD NO_CHANGE + lane SUPERSEDED)."""
    return {
        "overall_state": "DAILY_CYCLE_COMPLETE",
        "canonical_portfolio_decision": {
            "state": "NO_CHANGE", "headline": "NO PORTFOLIO CHANGE REQUIRED",
            "eligible_market_date": SEP2,
            "reassessment_state": "CURRENT_NO_CHANGE",
            "proposal_superseded": True,
            "superseded_by": {"decision": "CURRENT_NO_CHANGE", "session": SEP2},
            "no_proposal_reason": ("the reassessment ran and found the current "
                                   "holdings remain the best available use of "
                                   "capital"),
            "explanation": "No change is proposed."},
        "portfolio_decision_state": {
            "portfolio_decision_state": pdec.PDS_SUPERSEDED,
            "requires_manual_review": False, "approvable": False,
            "proposal_superseded": True,
            "materiality": {"material": False, "action_counts": {}},
            "one_way_turnover": None, "estimated_transaction_cost": None},
        "portfolio_reassessment": {"reassessment_state": "CURRENT_NO_CHANGE"},
        "operator_command": {"primary_action_available": False},
        "primary_action": {},
        "operational_state": {"eligible_market_date": SEP2, "pending_orders": 0},
        "reallocation_proposal_presentation": {
            "state": "SUPERSEDED_BY_NEWER_DECISION",
            "badge": "SUPERSEDED — HISTORY ONLY"},
    }


def _constrained_with_stale_target():
    """The constrained read still publishes the superseded target's analysis
    (history), including its 28 allocation rows."""
    allocations = ([{"ticker": "T%02d" % i, "action": "EXIT"} for i in range(14)]
                   + [{"ticker": "A%02d" % i, "action": "ADD"} for i in range(6)]
                   + [{"ticker": "I%02d" % i, "action": "INCREASE"} for i in range(5)]
                   + [{"ticker": "R0", "action": "REDUCE"},
                      {"ticker": "P0", "action": "REPLACE_IN"},
                      {"ticker": "P1", "action": "REPLACE_IN"}])
    return {"outcome": "PROPOSAL_READY", "superseded": True,
            "superseded_by": {"decision": "CURRENT_NO_CHANGE", "session": SEP2},
            "best_feasible_target": {"allocations": allocations},
            "switching_economics": {"one_way_turnover": 0.35,
                                    "estimated_transaction_cost": 85.69},
            "execution": {}, "feasible_target_exists": True}


def test_07_today_displays_no_change(live_world):
    hero = op._portfolio_decision(_wf_after_fix(), _constrained_with_stale_target(),
                                  {}, {})
    assert hero["state"] == "HOLD"
    assert hero["headline"] == "HOLD CURRENT PORTFOLIO"
    assert "REALLOCATE" not in hero["headline"]


def test_08_today_shows_no_old_proposal_economics(live_world):
    hero = op._portfolio_decision(_wf_after_fix(), _constrained_with_stale_target(),
                                  {}, {})
    # The superseded proposal's 28 rows never re-enter through the fallback.
    assert hero["positions_changing"] == 0


def test_09_today_has_no_review_reallocation_cta(live_world):
    hero = op._portfolio_decision(_wf_after_fix(), _constrained_with_stale_target(),
                                  {}, {})
    action = hero["next_action"]
    assert action["kind"] != "REVIEW_REALLOCATION"
    assert action["available"] is False
    assert action["executes"] is False


def test_10_reallocation_page_marks_history_only(live_world):
    payload = rp.load_reallocation_proposal(
        portfolio_state={"active_book": {"book_id": BOOK},
                         "dates": {"eligible_market_date": SEP1}},
        reallocation_dir=live_world["realloc"],
        reassessment_dir=live_world["reassess"], drc_dir=live_world["drc"],
        decision_dir=live_world["decisions"])
    assert payload["state"] == "SUPERSEDED_BY_NEWER_DECISION"
    assert "history" in payload["message"].lower()
    # The R47 composition renders the supersession headline, not PROPOSAL READY.
    con = rp.load_constrained_reallocation(
        portfolio_state={"active_book": {"book_id": BOOK},
                         "dates": {"eligible_market_date": SEP1}},
        reallocation_dir=live_world["realloc"],
        reassessment_dir=live_world["reassess"], drc_dir=live_world["drc"],
        decision_dir=live_world["decisions"], include_execution=False,
        decision_lane={"portfolio_decision_state": pdec.PDS_SUPERSEDED,
                       "approvable": False, "requires_manual_review": False})
    assert con["superseded"] is True
    assert "SUPERSEDED" in con["headline"]
    assert "HISTORY ONLY" in con["headline"]
    # The decision-summary framing follows the backend verdict verbatim.
    ds = op._decision_summary(_wf_after_fix(), _constrained_with_stale_target())
    assert ds["superseded"] is True
    assert ds["target_class"] == "SUPERSEDED_HISTORY_ONLY"
    assert ds["renders_approval_cta"] is False


def test_11_no_change_cannot_coexist_with_reallocate_headline():
    # The exact live contradiction, expressed as the payload-level invariant:
    # a CURRENT_NO_CHANGE decision beside a still-reviewable proposal violates.
    violations = ws.check_decision_semantics(
        reallocation_operator_state=ws.RPS_READY, reallocation_approvable=True,
        reassessment_state="CURRENT_NO_CHANGE",
        reassessment_proposal_required=False,
        portfolio_decision_state=pdec.PDS_REVIEW_REQUIRED,
        portfolio_decision_requires_review=True,
        portfolio_decision_approvable=True,
        proposal_bound_reassessment_hash=HOC_V1,
        current_reassessment_hash=None,
        mandatory_exit_tickers=[], mandatory_exit_obligation=None)
    codes = {v["code"] for v in violations}
    assert "NO_CHANGE_DECISION_WITH_REVIEWABLE_PROPOSAL" in codes
    # And the FIXED composition (lane SUPERSEDED, nothing reviewable) is clean.
    clean = ws.check_decision_semantics(
        reallocation_operator_state=ws.RPS_SUPERSEDED,
        reallocation_approvable=False,
        reassessment_state="CURRENT_NO_CHANGE",
        reassessment_proposal_required=False,
        portfolio_decision_state=pdec.PDS_SUPERSEDED,
        portfolio_decision_requires_review=False,
        portfolio_decision_approvable=False,
        proposal_bound_reassessment_hash=HOC_V1,
        current_reassessment_hash=None,
        mandatory_exit_tickers=[], mandatory_exit_obligation=None)
    assert [v for v in clean
            if v["code"] == "NO_CHANGE_DECISION_WITH_REVIEWABLE_PROPOSAL"] == []


def test_12_no_change_cannot_coexist_with_nonzero_current_economics(live_world):
    summ = _summary(live_world)
    v = _verdict(live_world, summ)
    lane = pdec.derive_decision_state(
        has_active_book=True,
        proposal_summary={**summ, "reallocation_proposal_superseded": True,
                          "reallocation_proposal_supersession": v},
        decision_record=None)
    # Current-work economics are quiet; the numbers live on as explicit history.
    assert lane["one_way_turnover"] is None
    assert lane["estimated_transaction_cost"] is None
    assert lane["score_improvement_net_of_cost"] is None
    assert lane["material"] is False
    assert lane["materiality"]["action_counts"] == {}
    hist = lane["superseded_proposal"]
    assert hist["history_only"] is True
    assert hist["one_way_turnover"] == 0.35
    assert hist["estimated_transaction_cost"] == 85.69
    assert hist["action_counts"] == COUNTS_28


# =========================================================================== #
# 13-16 — direction table for the authority rule
# =========================================================================== #
def test_13_newer_change_decision_supersedes_older_proposal(stores):
    # Cross-session: a NEWER session's governed PROPOSAL_READY decision
    # supersedes the older session's proposal outright.
    make_proposal(stores["realloc"], session=SEP1, hoc_hash=HOC_V1,
                  generated_at="2026-09-02T20:31:25+00:00")
    make_assessment(stores["reassess"], session=SEP2, decision="PROPOSAL_READY",
                    rhash=RHASH_V2, hoc_hash=HOC_V2, binding_hoc=HOC_V2,
                    generated_at="2026-09-02T23:51:50+00:00")
    make_manifest(stores["drc"], session=SEP2, reassessment_hash=RHASH_V2,
                  reassessment_state="PROPOSAL_READY", proposal_hash="new")
    summ = rp.load_proposal_summary(active_book_id=BOOK,
                                    eligible_market_date=SEP1,
                                    reallocation_dir=stores["realloc"])
    v = pdec.load_decision_supersession(
        active_book_id=BOOK, proposal_summary=summ,
        reassessment_dir=stores["reassess"], drc_dir=stores["drc"],
        decision_dir=stores["decisions"])
    assert v["superseded"] is True
    assert v["reason"] == pdec.SUP_NEWER_SESSION_DECISION
    # Same-session: newer evidence requested a FRESH proposal — the stale
    # artifact awaiting replacement is superseded too.
    v2 = pdec.assess_proposal_supersession(
        proposal_summary={"reallocation_proposal_available": True,
                          "reallocation_proposal_id": "reap_old",
                          "reallocation_bound_eligible_market_date": SEP2,
                          "reallocation_bound_hoc_assessment_hash": HOC_V1,
                          "reallocation_proposal_generated_at":
                              "2026-09-02T23:38:15+00:00"},
        assessment={"available": True, "decision": "PROPOSAL_READY",
                    "eligible_market_date": SEP2, "reassessment_hash": RHASH_V2,
                    "artifact_id": "prs_new", "hoc_assessment_hash": HOC_V2,
                    "generated_at": "2026-09-02T23:51:50+00:00",
                    "is_governed": True})
    assert v2["superseded"] is True
    assert v2["reason"] == pdec.SUP_NEWER_EVIDENCE_REQUESTED_FRESH_PROPOSAL


def test_14_newer_no_change_decision_supersedes_older_proposal(stores):
    # Same-session replay of the live world: the session's governed conclusion
    # is CURRENT_NO_CHANGE, so the standing artifact is unendorsed.
    make_proposal(stores["realloc"], session=SEP2, hoc_hash=HOC_V1,
                  generated_at="2026-09-02T23:38:15+00:00")
    make_assessment(stores["reassess"], session=SEP2, decision="CURRENT_NO_CHANGE",
                    rhash=RHASH_V2, hoc_hash=HOC_V2,
                    generated_at="2026-09-02T23:51:50+00:00")
    make_manifest(stores["drc"], session=SEP2, reassessment_hash=RHASH_V2,
                  reassessment_state="CURRENT_NO_CHANGE")
    summ = rp.load_proposal_summary(active_book_id=BOOK,
                                    eligible_market_date=SEP2,
                                    reallocation_dir=stores["realloc"])
    v = pdec.load_decision_supersession(
        active_book_id=BOOK, proposal_summary=summ,
        reassessment_dir=stores["reassess"], drc_dir=stores["drc"],
        decision_dir=stores["decisions"])
    assert v["superseded"] is True
    assert v["reason"] == pdec.SUP_NO_CHANGE_DECISION
    assert v["superseded_by"]["governed_manifest_run_id"] == "drc_2026-09-02_hermetic"


def test_15_withheld_or_non_governed_intraday_never_supersedes(stores):
    # An intraday cycle advanced the assessment head with new NO-CHANGE evidence
    # but was NOT promoted by the R54.1 gate and is bound to NO governed
    # manifest: it must not tear down the standing governed proposal.
    make_proposal(stores["realloc"], session=SEP2, hoc_hash=HOC_V1,
                  generated_at="2026-09-02T20:31:25+00:00")
    make_assessment(stores["reassess"], session=SEP2, decision="CURRENT_NO_CHANGE",
                    rhash="livehash0000", hoc_hash=HOC_V2,
                    generated_at="2026-09-02T21:00:00+00:00")
    # No manifest for the session; no governed record. Authority is unprovable.
    summ = rp.load_proposal_summary(active_book_id=BOOK,
                                    eligible_market_date=SEP2,
                                    reallocation_dir=stores["realloc"])
    v = pdec.load_decision_supersession(
        active_book_id=BOOK, proposal_summary=summ,
        reassessment_dir=stores["reassess"], drc_dir=stores["drc"],
        decision_dir=stores["decisions"])
    assert v["superseded"] is False
    assert v["reason"] == pdec.SUP_AUTHORITY_UNPROVEN
    # The proposal stays reviewable exactly as before.
    lane = pdec.derive_decision_state(
        has_active_book=True,
        proposal_summary={**summ, "reallocation_proposal_superseded": False,
                          "reallocation_proposal_supersession": v},
        decision_record=None)
    assert lane["portfolio_decision_state"] == pdec.PDS_REVIEW_REQUIRED
    # An explicitly non-governed assessment is refused by the pure predicate too.
    v2 = pdec.assess_proposal_supersession(
        proposal_summary=summ,
        assessment={"available": True, "decision": "CURRENT_NO_CHANGE",
                    "eligible_market_date": SEP2, "is_governed": False,
                    "hoc_assessment_hash": HOC_V2,
                    "generated_at": "2026-09-02T21:00:00+00:00"})
    assert v2["superseded"] is False
    assert v2["reason"] == pdec.SUP_AUTHORITY_UNPROVEN


def test_16_governed_intraday_supersedes_per_existing_authority(stores):
    # The SAME intraday world, but the R54.1 gate DID promote it: a persisted
    # governed record binds the head's hash, so authority is proven through the
    # existing governed lane — no new authority rule.
    make_proposal(stores["realloc"], session=SEP2, hoc_hash=HOC_V1,
                  generated_at="2026-09-02T20:31:25+00:00")
    make_assessment(stores["reassess"], session=SEP2, decision="CURRENT_NO_CHANGE",
                    rhash="livehash0000", hoc_hash=HOC_V2,
                    generated_at="2026-09-02T21:00:00+00:00")
    record = {"record_id": "gdec_hermetic", "decision": "HOLD_CURRENT_BOOK",
              "provenance": "GOVERNED_INTRADAY",
              "decided_at": "2026-09-02T21:00:05+00:00",
              "eligible_market_session": SEP2,
              "candidate_identity_hash": "cid",
              "identity": {"active_book_id": BOOK,
                           "eligible_market_session": SEP2,
                           "reassessment_hash": "livehash0000"}}
    _write(stores["decisions"] / "governed_index.json",
           {BOOK: {"record_id": "gdec_hermetic", "record": record}})
    summ = rp.load_proposal_summary(active_book_id=BOOK,
                                    eligible_market_date=SEP2,
                                    reallocation_dir=stores["realloc"])
    v = pdec.load_decision_supersession(
        active_book_id=BOOK, proposal_summary=summ,
        reassessment_dir=stores["reassess"], drc_dir=stores["drc"],
        decision_dir=stores["decisions"])
    assert v["superseded"] is True
    assert v["superseded_by"]["governed_provenance"] == "GOVERNED_INTRADAY"
    assert v["superseded_by"]["governed_manifest_run_id"] == "gdec_hermetic"


# =========================================================================== #
# 17-20 — safety: no order, no fill, no broker, automation off
# =========================================================================== #
def test_17_18_19_20_no_order_no_fill_no_broker_automation_off(live_world):
    art = rp.load_latest_artifact(active_book_id=BOOK, eligible_market_date=SEP1,
                                  reallocation_dir=live_world["realloc"])
    res = pdec.record_decision(
        decision=pdec.DECISION_APPROVE, confirm=pdec.CONFIRM_TOKEN,
        artifact=art, decision_dir=live_world["decisions"],
        reallocation_dir=live_world["realloc"],
        reassessment_dir=live_world["reassess"], drc_dir=live_world["drc"])
    assert res["created_orders"] is False
    assert res["created_fills"] is False
    assert res["changed_holdings"] is False
    assert res["changed_cash"] is False and res["changed_nav"] is False
    assert res["automation_off"] is True and res["paper_only"] is True
    # The supersession machinery itself contains no broker/order/fill path.
    src = (REPO / "api" / "portfolio_decision.py").read_text(encoding="utf-8")
    block = src[src.index("def assess_proposal_supersession"):
                src.index("def _binding_from_artifact")]
    for token in ("create_order", "create_fill", "broker", "submit_order"):
        assert token not in block
    # And every read remains read-only by contract.
    payload = rp.load_reallocation_proposal(
        portfolio_state={"active_book": {"book_id": BOOK},
                         "dates": {"eligible_market_date": SEP1}},
        reallocation_dir=live_world["realloc"],
        reassessment_dir=live_world["reassess"], drc_dir=live_world["drc"],
        decision_dir=live_world["decisions"])
    assert payload["review_only"] is True


# =========================================================================== #
# 21+ — direction guards, vocabulary, selector, projection, wiring
# =========================================================================== #
def test_21_direction_guards_fail_closed():
    summ = {"reallocation_proposal_available": True,
            "reallocation_proposal_id": "reap_x",
            "reallocation_bound_eligible_market_date": SEP2,
            "reallocation_bound_hoc_assessment_hash": HOC_V1,
            "reallocation_proposal_generated_at": "2026-09-02T23:38:15+00:00"}
    base = {"available": True, "decision": "CURRENT_NO_CHANGE",
            "eligible_market_date": SEP2, "hoc_assessment_hash": HOC_V2,
            "generated_at": "2026-09-02T23:51:50+00:00", "is_governed": True}
    # An assessment for an EARLIER session never supersedes anything.
    v = pdec.assess_proposal_supersession(
        proposal_summary=summ, assessment={**base,
                                           "eligible_market_date": SEP1})
    assert (v["superseded"], v["reason"]) == (False, pdec.SUP_ASSESSMENT_OLDER)
    # A blocked / inconclusive state never supersedes.
    for word in ("BLOCKED_DATA", "BLOCKED_EVIDENCE", "MANUAL_REVIEW_REQUIRED",
                 "NOT_RUN"):
        v = pdec.assess_proposal_supersession(
            proposal_summary=summ, assessment={**base, "decision": word})
        assert v["superseded"] is False
        assert v["reason"] == pdec.SUP_ASSESSMENT_NOT_CONCLUSIVE
    # No assessment observed -> nothing changes.
    v = pdec.assess_proposal_supersession(proposal_summary=summ, assessment=None)
    assert (v["superseded"], v["reason"]) == (False, pdec.SUP_NO_ASSESSMENT)
    # No proposal -> nothing to supersede.
    v = pdec.assess_proposal_supersession(
        proposal_summary={"reallocation_proposal_available": False},
        assessment=base)
    assert (v["superseded"], v["reason"]) == (False, pdec.SUP_NO_PROPOSAL)
    # Same-session PROPOSAL_READY bound to the SAME evidence is the requested
    # proposal — never superseded by its own requester.
    v = pdec.assess_proposal_supersession(
        proposal_summary=summ,
        assessment={**base, "decision": "PROPOSAL_READY",
                    "hoc_assessment_hash": HOC_V1})
    assert (v["superseded"], v["reason"]) == (
        False, pdec.SUP_NOT_SUPERSEDED_CURRENT)
    # Unprovable evidence direction keeps the review (fail-closed toward review).
    v = pdec.assess_proposal_supersession(
        proposal_summary={**summ, "reallocation_bound_hoc_assessment_hash": None},
        assessment={**base, "decision": "PROPOSAL_READY"})
    assert (v["superseded"], v["reason"]) == (
        False, pdec.SUP_DIRECTION_UNPROVEN)
    # A provably OLDER PROPOSAL_READY assessment never outranks a newer artifact.
    v = pdec.assess_proposal_supersession(
        proposal_summary=summ,
        assessment={**base, "decision": "PROPOSAL_READY",
                    "generated_at": "2026-09-02T23:00:00+00:00"})
    assert (v["superseded"], v["reason"]) == (False, pdec.SUP_ASSESSMENT_OLDER)


def test_22_vocabularies_carry_the_new_states():
    assert pdec.PDS_SUPERSEDED in pdec.DECISION_STATE_VOCAB
    assert pdec.PDS_SUPERSEDED not in pdec.APPROVABLE_DECISION_STATES
    assert rp.STATE_SUPERSEDED in rp.READ_STATE_VOCAB
    assert rp.STATE_SUPERSEDED not in rp.APPROVABLE_READ_STATES
    assert ws.RPS_SUPERSEDED in ws.REALLOCATION_OPERATOR_STATES
    assert ws.RPS_SUPERSEDED not in ws.REALLOCATION_APPROVABLE_STATES
    assert "NO_CHANGE_DECISION_WITH_REVIEWABLE_PROPOSAL" in ws.SEMANTIC_VIOLATION_CODES
    assert pdec.GD_NO_CHANGE in pdec.GOVERNED_DECISION_VOCAB


def test_23_authority_selector_answers_the_contract(live_world):
    summ = _summary(live_world)
    v = _verdict(live_world, summ)
    sel = pdec.resolve_decision_authority(
        assessment={"available": True, "decision": "CURRENT_NO_CHANGE",
                    "eligible_market_date": SEP2,
                    "artifact_id": "prs_2026-09-02_%s_%s" % (BOOK, RHASH_V2[:12]),
                    "is_governed": True},
        proposal_summary={**summ, "reallocation_proposal_superseded": True},
        supersession=v)
    assert sel["current_authoritative_decision_id"] \
        == "prs_2026-09-02_%s_%s" % (BOOK, RHASH_V2[:12])
    assert sel["current_authoritative_session"] == SEP2
    assert sel["current_authoritative_decision_type"] == "CURRENT_NO_CHANGE"
    assert sel["current_reviewable_proposal_id"] is None
    assert sel["superseded_proposal_ids"] == [live_world["proposal_id"]]
    assert sel["supersession_reason"] == pdec.SUP_NEWER_SESSION_DECISION
    # With nothing superseded and a reviewable proposal, the selector says so.
    sel2 = pdec.resolve_decision_authority(
        assessment={"available": True, "decision": "PROPOSAL_READY",
                    "eligible_market_date": SEP1, "artifact_id": "prs_old",
                    "is_governed": True},
        proposal_summary={**summ, "reallocation_proposal_approvable": True},
        supersession={"superseded": False})
    assert sel2["current_reviewable_proposal_id"] == live_world["proposal_id"]
    assert sel2["superseded_proposal_ids"] == []


def test_24_governed_projection_prefers_assessment_decision():
    wf = {"research_cycle_state": {"governed_research_evidence_current": True,
                                   "governed_manifest_run_id": "drc_x"}}
    rs = {"state": "CURRENT_NO_CHANGE", "eligible_market_date": SEP2,
          "reassessment_hash": RHASH_V2,
          "active_book": {"book_id": BOOK},
          "artifact": {"reassessment_id": "prs_v2",
                       "generated_at": "2026-09-02T23:51:50+00:00",
                       "identity": {"hoc_assessment_hash": HOC_V2}}}
    stale_summ = {"reallocation_proposal_available": True,
                  "reallocation_proposal_hash": PHASH,
                  "reallocation_outcome": "PROPOSAL_READY",
                  "reallocation_bound_eligible_market_date": SEP2,
                  "reallocation_bound_hoc_assessment_hash": HOC_V1,
                  "reallocation_proposal_generated_at":
                      "2026-09-02T23:38:15+00:00"}
    proj = pdec.project_governed_daily_cycle_decision(
        workflow=wf, reassessment=rs, proposal_summary=stale_summ)
    # The live defect: this used to say CHANGE_RECOMMENDED from the stale
    # proposal, stamped with the NO-CHANGE assessment's own timestamp.
    assert proj["decision"] == pdec.GD_NO_CHANGE
    assert proj["manual_review_required"] is False
    # The stale proposal's hash never enters the governed identity.
    assert proj["identity"]["proposal_hash"] is None
    assert proj["identity"]["target_outcome"] is None
    # A genuinely requested-and-bound CHANGE still projects as before.
    rs2 = {**rs, "state": "PROPOSAL_READY"}
    bound_summ = {**stale_summ,
                  "reallocation_bound_hoc_assessment_hash": HOC_V2}
    proj2 = pdec.project_governed_daily_cycle_decision(
        workflow=wf, reassessment=rs2, proposal_summary=bound_summ)
    assert proj2["decision"] == pdec.GD_CHANGE_RECOMMENDED
    assert proj2["identity"]["proposal_hash"] == PHASH


def test_25_workflow_wiring_consumes_the_one_calculation():
    src = (REPO / "api" / "workflow_state.py").read_text(encoding="utf-8")
    assert "_import_portfolio_decision().assess_proposal_supersession(" in src
    assert "_import_portfolio_decision().resolve_decision_authority(" in src
    assert '"decision_authority": decision_authority' in src
    # The RPS map renders the read state; nothing recomputes a comparison.
    assert "_RP_SUPERSEDED: (RPS_SUPERSEDED" in src
    assert "def assess_proposal_supersession(" not in src
    # The presentation renders; it never derives.
    op_src = (REPO / "api" / "operator_presentation.py").read_text(encoding="utf-8")
    assert "def assess_proposal_supersession(" not in op_src
    assert "[] if _superseded else" in op_src


def test_26_rpp_presentation_renders_superseded_history_only():
    rpp = ws.build_reallocation_proposal_presentation(
        state="SUPERSEDED_BY_NEWER_DECISION", available=True,
        eligible_date=SEP2, cycle_complete=True,
        reassessment_state="CURRENT_NO_CHANGE",
        superseded_by_decision="CURRENT_NO_CHANGE",
        superseded_by_session=SEP2)
    assert rpp["canonical_operator_state"] == ws.RPS_SUPERSEDED
    assert rpp["badge"] == "SUPERSEDED — HISTORY ONLY"
    assert rpp["available"] is False and rpp["has_proposal"] is False
    assert rpp["superseded"] is True
    assert rpp["outstanding_action"] == "NO_OUTSTANDING_ACTION_NEWER_DECISION_STANDS"
    assert "SUPERSEDED" in rpp["headline"]
    assert SEP2 in rpp["headline"]


def test_27_ui_renders_and_never_derives_supersession():
    ui = (REPO / "api" / "ui" / "index.html").read_text(encoding="utf-8",
                                                        errors="replace")
    assert "SUPERSEDED_BY_NEWER_DECISION" in ui
    # No JavaScript evidence-hash comparison decides supersession.
    assert re.findall(r"hoc_assessment_hash\s*[!=]==?", ui) == []
    assert re.findall(r"reassessment_hash\s*[!=]==?", ui) == []


def test_28_no_new_route_and_no_operator_date_field():
    app_src = (REPO / "api" / "app.py").read_text(encoding="utf-8")
    assert re.findall(r"@app\.post\(\s*[\"'][^\"']*supersed[^\"']*[\"']",
                      app_src) == []
    m = re.search(r"class PortfolioDecisionRecordRequest\(BaseModel\):(.*?)"
                  r"(?:^@app\.|^class )", app_src, re.S | re.M)
    body = m.group(1)
    fields = set(re.findall(r"^\s{4}(\w+)\s*:", body, re.M))
    assert fields == {"decision", "confirmation", "expected_proposal_hash",
                      "requested_by"}


def test_29_cross_session_never_runs_backwards(stores):
    # A newer-session PROPOSAL awaiting review is never superseded by the OLDER
    # session's governed decision (authority never points backwards in time).
    make_proposal(stores["realloc"], session=SEP2, hoc_hash=HOC_V2,
                  generated_at="2026-09-02T23:38:15+00:00")
    make_assessment(stores["reassess"], session=SEP1, decision="CURRENT_NO_CHANGE",
                    rhash=RHASH_V1, hoc_hash=HOC_V1,
                    generated_at="2026-09-01T23:51:50+00:00")
    make_manifest(stores["drc"], session=SEP1, reassessment_hash=RHASH_V1,
                  reassessment_state="CURRENT_NO_CHANGE")
    summ = rp.load_proposal_summary(active_book_id=BOOK,
                                    eligible_market_date=SEP2,
                                    reallocation_dir=stores["realloc"])
    v = pdec.load_decision_supersession(
        active_book_id=BOOK, proposal_summary=summ,
        reassessment_dir=stores["reassess"], drc_dir=stores["drc"],
        decision_dir=stores["decisions"])
    assert v["superseded"] is False
    assert v["reason"] == pdec.SUP_ASSESSMENT_OLDER
