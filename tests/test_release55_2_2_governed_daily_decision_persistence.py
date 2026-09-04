"""Release 55.2.2 — GOVERNED DAILY DECISION PERSISTENCE CLOSURE.

The defect, stated once. The 2026-09-03 Portfolio Cycle was the first genuinely
post-R54.4 daily cycle. It completed, reached ``CURRENT_NO_CHANGE``, and left NO
governed ledger row: the production decision root still held only the manual
lane's ``decisions.json`` / ``index.json``. Acceptance nonetheless reported
``R55_ACCEPTANCE_COMPLETE``, because the read-time projection that covered the
gap was labelled ``LEGACY_COMPATIBILITY_PROJECTION`` — the same word used for
sessions that legitimately predate the delegating producer.

Replaying the real manifest through the daily gate gave the answer:

    verdict DAILY_DECISION_WITHHELD   18/19   HOC_ARTIFACT_IDENTITY_MISMATCH

and the two facts behind it:

  * ``persist_assessment`` REUSED the opportunity-cost artifact already held for
    the session (same economic state, same evidence, same conclusion) but
    returned the identity of the DISCARDED recomputation, so
    ``artifact_binding`` paired the EXISTING ``artifact_id`` with a hash that
    artifact does not carry;
  * ``api.daily_research_cycle`` never consumed that binding at all. R54.3 built
    the exact-version seam and wired it into the INTRADAY producer only, so the
    daily manifest recorded the transient kernel hash beside the reused
    artifact's id, and the reassessment re-resolved a binding of its own and
    correctly reported ``hoc_persisted: False``.

The gate was right to refuse. Everything here proves the CURRENT and FUTURE path
writes a real row, that Sep-3's absence is preserved rather than backfilled, and
that acceptance can no longer call a missing governed write complete.

Every write below lands in a pytest ``tmp_path``. Nothing here reads or mutates a
production store, creates an order, a fill or an approval, or restarts anything.
"""
from __future__ import annotations

import inspect
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from paper_trader.api import active_manager_state as ams
from paper_trader.api import daily_research_cycle as drc
from paper_trader.api import holding_opportunity_cost as hocm
from paper_trader.api import portfolio_decision as pdec
from paper_trader.api import portfolio_reassessment as prs
from paper_trader.engine import constrained_reallocation as cr

BOOK = "alpha_paper_book_1"
SESSION = "2026-09-03"
LEGACY_SESSION = "2026-09-02"
RUN_ID = "drc_2026-09-03_bc5b2eb5ee7b"
STAMP = "2026-09-03T23:51:52+00:00"

PD_SRC = Path(pdec.__file__).read_text(encoding="utf-8")
DRC_SRC = Path(drc.__file__).read_text(encoding="utf-8")
HOC_SRC = Path(hocm.__file__).read_text(encoding="utf-8")
AMS_SRC = Path(ams.__file__).read_text(encoding="utf-8")
ACC_SRC = (Path(drc.__file__).parents[2] / "paper_trader" / "scripts"
           / "r55_operator_acceptance.py")
ACC_SRC = (ACC_SRC.read_text(encoding="utf-8") if ACC_SRC.exists() else
           (Path(__file__).parents[1] / "scripts"
            / "r55_operator_acceptance.py").read_text(encoding="utf-8"))


# =========================================================================== #
# Opportunity-cost fixtures — the REAL reuse shape.
#
# ``decision_fingerprint`` excludes ``provenance`` and ``assessment_hash``, and
# ``assessment_evidence_hash`` excludes both plus every clock. Two runs that
# differ ONLY inside ``provenance`` are therefore the SAME assessment on all
# three axes while hashing differently document-wide — which is precisely the
# production case that produced the Sep-3 mismatch.
# =========================================================================== #
def _ic(**kw):
    d = {"eligible_market_date": SESSION, "active_book_id": BOOK,
         "portfolio_state_hash": "PSH_DOC", "economic_state_hash": "ESH1",
         "corporate_actions_hash": "CA1", "universe_scoring_hash": "US1",
         "universe_input_contract_hash": "UIC1",
         "scoring_ranking_date": SESSION,
         "inputs_as_of_eligible_date": True,
         "holding_reviews": [], "addition_candidates": []}
    d.update(kw)
    return d


def _result(assessment_hash, *, state="READY", decision_extra=None, **kw):
    d = {"assessment_state": state, "eligible_market_date": SESSION,
         "active_book_id": BOOK, "assessment_hash": assessment_hash,
         "holding_reviews": [], "addition_candidates": [],
         "recommendation_counts": {"HOLD": 25},
         "data_quality": {"data_gaps": []},
         "provenance": {"calculation_owner": "engine.holding_opportunity_cost",
                        "portfolio_state_hash": "PSH_DOC"}}
    if decision_extra:
        d.update(decision_extra)
    d.update(kw)
    return d


def _seed_reused_artifact(tmp_path):
    """Persist an artifact, then re-run the SAME assessment with a different
    document-wide hash. Returns (first_outcome, reuse_outcome)."""
    hoc_dir = str(tmp_path / "hoc")
    first = hocm.persist_assessment(
        result=_result("HASH_STORED"), input_contract=_ic(), hoc_dir=hoc_dir,
        produced_by=hocm.PRODUCER_EVENT_SIGNAL_REFRESH)
    again = hocm.persist_assessment(
        result=_result("HASH_RECOMPUTED",
                       provenance={"calculation_owner":
                                   "engine.holding_opportunity_cost",
                                   "portfolio_state_hash": "PSH_DOC",
                                   "note": "a later re-derivation"}),
        input_contract=_ic(), hoc_dir=hoc_dir,
        produced_by=hocm.PRODUCER_DAILY_RESEARCH_CYCLE, drc_run_id=RUN_ID)
    return hoc_dir, first, again


# =========================================================================== #
# Governed-decision fixtures — the shapes the real owners publish.
# =========================================================================== #
def _delegation():
    return {"owner": "api.portfolio_decision",
            "contract": drc.GOVERNED_DELEGATION_CONTRACT,
            "delegates_terminal_decision": True,
            "decision_is_not_recorded_here": True}


def _ps(**kw):
    d = {"active_book": {"book_id": BOOK},
         "dates": {"eligible_market_date": SESSION},
         "state_hash": "PSH_DOC", "economic_state_hash": "ESH1"}
    d.update(kw)
    return d


def _man(hoc_hash="HOC_STORED", hoc_aid="hoc_a", **kw):
    d = {"run_id": RUN_ID, "state": "COMPLETE",
         "active_book_id": BOOK, "eligible_market_date": SESSION,
         "completed_at": STAMP,
         "session_contract_hash": "SCH1", "input_contract_hash": "ICH1",
         "portfolio_reassessment_id": "ra_1",
         "portfolio_reassessment_hash": "RA1",
         "portfolio_reassessment_state": "CURRENT_NO_CHANGE",
         "reallocation_proposal_id": "", "reallocation_proposal_hash": "",
         "reallocation_proposal_state": "NOT_REQUIRED",
         "opportunity_cost_artifact_id": hoc_aid,
         "opportunity_cost_assessment_hash": hoc_hash,
         "governed_decision_delegation": _delegation()}
    d.update(kw)
    return d


def _reas(state="CURRENT_NO_CHANGE", hoc_hash="HOC_STORED", hoc_aid="hoc_a",
          **kw):
    d = {"state": state, "eligible_market_date": SESSION,
         "active_book": {"book_id": BOOK},
         "reassessment_id": "ra_1", "reassessment_hash": "RA1",
         "artifact": {"reassessment_id": "ra_1", "generated_at": STAMP,
                      "identity": {"economic_state_hash": "ESH1"}},
         "proposal_binding": {"reassessment_id": "ra_1",
                              "reassessment_hash": "RA1",
                              "hoc_assessment_hash": hoc_hash,
                              "hoc_artifact_id": hoc_aid,
                              "hoc_assessment_evidence_hash": "HOCEV1",
                              "hoc_persisted": True,
                              "universe_scoring_hash": "US1",
                              "universe_input_contract_hash": "UIC1",
                              "portfolio_state_hash": "PSH1",
                              "corporate_actions_hash": "CA1",
                              "eligible_market_date": SESSION,
                              "active_book_id": BOOK}}
    d.update(kw)
    return d


def _summ(outcome=None, with_proposal=False, **kw):
    d = {"reallocation_proposal_available": with_proposal,
         "reallocation_proposal_stale": False,
         "reallocation_proposal_withheld": False,
         "reallocation_corporate_actions_hash": "CA1",
         "reallocation_proposal_hash": "PR1" if with_proposal else None,
         "reallocation_proposal_id": "prop_1" if with_proposal else None,
         "reallocation_outcome": outcome, "reallocation_data_gaps": []}
    d.update(kw)
    return d


def _con(outcome=None, **kw):
    econ = {"switching_hurdle": 0.02,
            "clears_switching_hurdle": outcome == cr.OUTCOME_PROPOSAL_READY,
            "score_improvement_net_of_cost": 0.004, "one_way_turnover": 0.11,
            "estimated_transaction_cost": 214.5}
    d = {"outcome": outcome, "feasible_target_exists": bool(outcome),
         "calculation_owner": cr.CALCULATION_OWNER, "switching_economics": econ,
         "ideal_target": {"zero_base_owner": "api.zero_base_target"},
         "multi_asset": {"current_holdings_privileged": False},
         "best_feasible_target": {"allocations": [
             {"ticker": "T00", "action": "REDUCE", "current_weight": 0.06,
              "proposed_weight": 0.03, "delta_weight": -0.03,
              "capital_change": -2900.0}]}}
    d.update(kw)
    return d


def _hocb(hoc_hash="HOC_STORED", hoc_aid="hoc_a", **kw):
    d = {"hoc_artifact_id": hoc_aid, "hoc_assessment_hash": hoc_hash,
         "hoc_persisted": True, "hoc_persistence_status": "REUSED_EXISTING",
         "hoc_assessment_evidence_hash": "HOCEV1",
         "hoc_artifact_retrievable": True,
         "hoc_artifact_identity_matches": True,
         "hoc_binding_detail": "artifact opened by id",
         "hoc_binding_resolved_by": "api.holding_opportunity_cost.resolve_binding"}
    d.update(kw)
    return d


def _sc(**kw):
    d = {"ranking_date": SESSION, "input_contract_hash": "UIC1"}
    d.update(kw)
    return d


def _daily(*, man=None, reas=None, summ=None, con=None, hocb=None,
           standing=None):
    man = _man() if man is None else man
    reas = _reas() if reas is None else reas
    summ = _summ() if summ is None else summ
    con = _con() if con is None else con
    cand = pdec.build_daily_cycle_candidate(
        portfolio_state=_ps(), drc_manifest=man, reassessment=reas,
        proposal_summary=summ, constrained=con, scoring_identity=_sc(),
        hoc_binding=_hocb() if hocb is None else hocb)
    gate = pdec.evaluate_daily_cycle_governance(
        candidate=cand, drc_manifest=man, portfolio_state=_ps(),
        reassessment=reas, proposal_summary=summ, constrained=con,
        current_governed=standing)
    return cand, gate


def _record(cand, gate, tmp_path):
    return pdec.record_governed_decision(
        candidate=cand, gate=gate, provenance=pdec.PROV_GOVERNED_DAILY_CYCLE,
        confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN,
        decision_dir=str(tmp_path))


def _change_kit():
    return dict(man=_man(portfolio_reassessment_state="PROPOSAL_READY",
                         reallocation_proposal_id="prop_1",
                         reallocation_proposal_hash="PR1",
                         reallocation_proposal_state="READY"),
                reas=_reas(state="PROPOSAL_READY"),
                summ=_summ(outcome=cr.OUTCOME_PROPOSAL_READY,
                           with_proposal=True),
                con=_con(outcome=cr.OUTCOME_PROPOSAL_READY))


def _workflow(*, delegation=None, session=SESSION, run_id=RUN_ID):
    return {"research_cycle_state": {
        "governed_research_evidence_current": True,
        "governed_manifest_run_id": run_id,
        "governed_evidence_owner": "api.daily_research_cycle",
        "governed_decision_delegation": delegation,
        "eligible_market_date": session}}


# =========================================================================== #
# THE ROOT CAUSE — 1-8.
# =========================================================================== #
class TestOpportunityCostReuseIdentity:

    def test_01_reuse_returns_the_identity_of_the_artifact_actually_held(
            self, tmp_path):
        """The defect itself. Reuse means the caller's document was NOT written."""
        _, first, again = _seed_reused_artifact(tmp_path)
        assert first["status"] == hocm.PERSIST_CREATED
        assert again["status"] == hocm.PERSIST_REUSED
        assert again["artifact_id"] == first["artifact_id"]
        assert again["identity"]["assessment_hash"] == "HASH_STORED"
        assert again["recomputed_assessment_hash"] == "HASH_RECOMPUTED"
        assert again["reused_recomputed_document"] is True

    def test_02_reuse_never_writes_a_second_artifact(self, tmp_path):
        hoc_dir, first, again = _seed_reused_artifact(tmp_path)
        files = sorted(p.name for p in (Path(hoc_dir) / "artifacts").iterdir())
        assert files == ["%s.json" % first["artifact_id"]]
        idx = json.loads((Path(hoc_dir) / "index.json").read_text("utf-8"))
        assert len(idx) == 1

    def test_03_reuse_leaves_the_stored_artifact_byte_identical(self, tmp_path):
        hoc_dir = str(tmp_path / "hoc")
        first = hocm.persist_assessment(result=_result("HASH_STORED"),
                                        input_contract=_ic(), hoc_dir=hoc_dir)
        path = Path(hoc_dir) / "artifacts" / ("%s.json" % first["artifact_id"])
        before = path.read_bytes()
        hocm.persist_assessment(
            result=_result("HASH_RECOMPUTED",
                           provenance={"note": "re-derived"}),
            input_contract=_ic(), hoc_dir=hoc_dir)
        assert path.read_bytes() == before

    def test_04_the_binding_after_a_reuse_is_self_consistent(self, tmp_path):
        """artifact_id and assessment_hash must describe ONE object."""
        hoc_dir, first, again = _seed_reused_artifact(tmp_path)
        b = hocm.artifact_binding(again)
        assert b["hoc_artifact_id"] == first["artifact_id"]
        assert b["hoc_assessment_hash"] == "HASH_STORED"
        assert b["hoc_persisted"] is True
        assert b["hoc_reused_recomputed_document"] is True
        assert b["hoc_recomputed_assessment_hash"] == "HASH_RECOMPUTED"

    def test_05_the_binding_after_a_reuse_resolves_against_the_real_store(
            self, tmp_path):
        hoc_dir, _, again = _seed_reused_artifact(tmp_path)
        resolved = hocm.resolve_binding(
            binding=hocm.artifact_binding(again), active_book_id=BOOK,
            eligible_market_date=SESSION, hoc_dir=hoc_dir)
        assert resolved["hoc_artifact_retrievable"] is True
        assert resolved["hoc_artifact_identity_matches"] is True

    def test_06_the_pre_fix_binding_would_not_have_resolved(self, tmp_path):
        """Guard the exact production shape: the reused id + the recomputed hash."""
        hoc_dir, first, again = _seed_reused_artifact(tmp_path)
        broken = dict(hocm.artifact_binding(again))
        broken["hoc_assessment_hash"] = "HASH_RECOMPUTED"
        resolved = hocm.resolve_binding(
            binding=broken, active_book_id=BOOK, eligible_market_date=SESSION,
            hoc_dir=hoc_dir)
        assert resolved["hoc_artifact_retrievable"] is True
        assert resolved["hoc_artifact_identity_matches"] is False

    def test_07_a_genuinely_different_conclusion_is_still_a_conflict(
            self, tmp_path):
        """Fail-closed is not relaxed: same evidence, different answer, refused."""
        hoc_dir = str(tmp_path / "hoc")
        hocm.persist_assessment(result=_result("H1"), input_contract=_ic(),
                                hoc_dir=hoc_dir)
        out = hocm.persist_assessment(
            result=_result("H2", recommendation_counts={"HOLD": 24,
                                                        "REPLACE": 1}),
            input_contract=_ic(), hoc_dir=hoc_dir)
        assert out["status"] == hocm.PERSIST_CONFLICT
        assert out["persisted"] is False

    def test_08_materially_new_evidence_still_creates_a_version(self, tmp_path):
        hoc_dir = str(tmp_path / "hoc")
        hocm.persist_assessment(result=_result("H1"), input_contract=_ic(),
                                hoc_dir=hoc_dir)
        out = hocm.persist_assessment(
            result=_result("H2"),
            input_contract=_ic(universe_scoring_hash="US2"), hoc_dir=hoc_dir)
        assert out["status"] == hocm.PERSIST_ASSESSMENT_VERSION
        assert out["identity"]["assessment_hash"] == "H2"


# =========================================================================== #
# THE DAILY PRODUCER WIRING — 9-14.
# =========================================================================== #
class TestDailyProducerBindsWhatItCanRetrieve:

    def test_09_the_daily_producer_consumes_the_owners_binding(self):
        assert 'binding = b.get("binding") or {}' in DRC_SRC
        assert '"binding": dict(binding)' in DRC_SRC

    def test_10_the_manifest_records_the_bound_hash_not_the_transient_one(
            self, tmp_path):
        hoc_dir, first, again = _seed_reused_artifact(tmp_path)
        built = {"assessment": _result("HASH_RECOMPUTED"),
                 "persistence": again, "binding": hocm.artifact_binding(again)}
        out = drc._extract_holding_opp_cost(built, SESSION)
        assert out["assessment_hash"] == "HASH_STORED"
        assert out["computed_assessment_hash"] == "HASH_RECOMPUTED"
        assert out["artifact_id"] == first["artifact_id"]
        assert out["persisted"] is True
        assert out["reused_recomputed_document"] is True

    def test_11_an_unpersisted_assessment_keeps_its_own_hash_and_says_so(self):
        """A refused write stays visible AS a refused write; nothing is repaired."""
        refused = {"status": hocm.PERSIST_CONFLICT, "artifact_id": None,
                   "persisted": False}
        built = {"assessment": _result("HASH_TRANSIENT"),
                 "persistence": refused,
                 "binding": hocm.artifact_binding(refused)}
        out = drc._extract_holding_opp_cost(built, SESSION)
        assert out["assessment_hash"] == "HASH_TRANSIENT"
        assert out["persisted"] is False

    def test_12_the_canonical_reassessment_seam_receives_the_binding(self):
        sig = inspect.signature(drc._default_reassessment_fn)
        assert "hoc_binding" in sig.parameters
        assert "hoc_dir=hoc_dir, hoc_binding=hoc_binding" in DRC_SRC
        assert 'prs_kwargs["hoc_binding"]' in DRC_SRC

    def test_13_an_injected_reassessment_seam_keeps_its_existing_contract(self):
        """Only the CANONICAL default is handed the binding (the R54.4 rule)."""
        assert "if reassessment_fn is None:" in DRC_SRC
        assert inspect.signature(prs.run_and_persist).parameters.get(
            "hoc_binding") is not None

    def test_14_the_reassessment_owner_records_the_binding_it_is_handed(self):
        """The seam R54.3 built, now used by both producers."""
        prs_src = Path(prs.__file__).read_text(encoding="utf-8")
        assert '"hoc_artifact_id": hoc_binding.get("hoc_artifact_id")' in prs_src
        esr_src = (Path(drc.__file__).parent / "event_signal_refresh.py"
                   ).read_text(encoding="utf-8")
        assert "hoc_binding=(hoc_binding or None)" in esr_src


# =========================================================================== #
# A / B / C / I — the governed write itself.
# =========================================================================== #
class TestGovernedDailyWrite:

    def test_15_a_daily_current_no_change_writes_a_real_ledger_row(
            self, tmp_path):
        cand, gate = _daily()
        assert gate["eligible"] is True, gate.get("withheld_reason_codes")
        out = _record(cand, gate, tmp_path)
        assert out["recorded"] is True and out["status"] == "CREATED"
        assert out["record"]["decision"] == pdec.GD_NO_CHANGE
        assert out["record"]["provenance"] == pdec.PROV_GOVERNED_DAILY_CYCLE
        rows = json.loads((tmp_path / "governed_decisions.json"
                           ).read_text("utf-8"))
        assert len(rows) == 1

    def test_16_a_daily_hold_writes_a_real_ledger_row(self, tmp_path):
        cand, gate = _daily(
            man=_man(portfolio_reassessment_state="PROPOSAL_READY",
                     reallocation_proposal_id="prop_1",
                     reallocation_proposal_hash="PR1",
                     reallocation_proposal_state="READY"),
            reas=_reas(state="PROPOSAL_READY"),
            summ=_summ(outcome=cr.OUTCOME_HOLD_CURRENT_BOOK,
                       with_proposal=True),
            con=_con(outcome=cr.OUTCOME_HOLD_CURRENT_BOOK))
        assert gate["eligible"] is True, gate.get("withheld_reason_codes")
        out = _record(cand, gate, tmp_path)
        assert out["record"]["decision"] == pdec.GD_HOLD_CURRENT_BOOK
        assert out["record"]["manual_review_required"] is False

    def test_17_a_daily_change_writes_a_manual_review_ledger_row(self, tmp_path):
        cand, gate = _daily(**_change_kit())
        assert gate["eligible"] is True, gate.get("withheld_reason_codes")
        out = _record(cand, gate, tmp_path)
        assert out["record"]["decision"] == pdec.GD_CHANGE_RECOMMENDED
        assert out["record"]["manual_review_required"] is True
        assert out["record"]["safety"]["created_orders"] is False

    def test_18_an_exact_retry_reuses_the_same_row(self, tmp_path):
        cand, gate = _daily()
        first = _record(cand, gate, tmp_path)
        again = _record(*_daily(), tmp_path)
        assert again["status"] == "REUSED_EXISTING"
        assert again["record"]["record_id"] == first["record"]["record_id"]
        assert again["record"]["decided_at"] == first["record"]["decided_at"]
        rows = json.loads((tmp_path / "governed_decisions.json"
                           ).read_text("utf-8"))
        assert len(rows) == 1

    def test_19_a_contradictory_identity_still_fails_closed(self, tmp_path):
        """The Sep-3 shape: the manifest's hash and artifact name two objects."""
        cand, gate = _daily(hocb=_hocb(hoc_artifact_identity_matches=False))
        assert gate["eligible"] is False
        assert "HOC_ARTIFACT_IDENTITY_MISMATCH" in gate["withheld_reason_codes"]
        out = pdec.record_governed_decision(
            candidate=cand, gate=gate,
            provenance=pdec.PROV_GOVERNED_DAILY_CYCLE,
            confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN,
            decision_dir=str(tmp_path))
        assert out["recorded"] is False
        assert not (tmp_path / "governed_decisions.json").exists()

    def test_20_an_unretrievable_artifact_still_fails_closed(self, tmp_path):
        cand, gate = _daily(hocb=_hocb(hoc_artifact_retrievable=False))
        assert gate["eligible"] is False
        assert not (tmp_path / "governed_decisions.json").exists()


# =========================================================================== #
# THE CUTOVER — 21-26.
# =========================================================================== #
class TestCutoverClassification:

    def test_21_the_producer_declaration_is_authoritative(self):
        got = pdec.governed_daily_write_expected(
            eligible_market_session="1999-01-04", delegation=_delegation())
        assert got["expected_ledger_row"] is True
        assert got["cutover_basis"] == "PRODUCER_DECLARATION"

    def test_22_a_pre_declaration_manifest_uses_the_recorded_boundary(self):
        after = pdec.governed_daily_write_expected(
            eligible_market_session=SESSION, delegation=None)
        before = pdec.governed_daily_write_expected(
            eligible_market_session=LEGACY_SESSION, delegation=None)
        assert after["expected_ledger_row"] is True
        assert before["expected_ledger_row"] is False
        assert after["cutover_basis"] == "RECORDED_RELEASE_BOUNDARY"

    def test_23_an_undatable_session_is_treated_as_pre_cutover(self):
        got = pdec.governed_daily_write_expected(eligible_market_session=None)
        assert got["expected_ledger_row"] is False
        assert got["cutover_basis"] == "SESSION_UNKNOWN"

    def test_24_the_cutover_reads_no_clock(self):
        src = PD_SRC.split("def governed_daily_write_expected")[1].split(
            "\n_PERSISTENCE_DETAIL")[0]
        for banned in ("datetime.now", "utcnow", "date.today", "time.time"):
            assert banned not in src

    def test_25_the_daily_producer_declares_its_delegation(self):
        assert 'GOVERNED_DELEGATION_CONTRACT = "daily_cycle_governed_delegation.v1"' \
            in DRC_SRC
        assert '"governed_decision_delegation": {' in DRC_SRC
        assert '"delegates_terminal_decision": True' in DRC_SRC

    def test_26_the_declaration_is_about_the_producer_not_the_decision(self):
        """R54.4's append-only rule survives: no manifest names its decision."""
        assert '"decision_is_not_recorded_here": True' in DRC_SRC
        block = DRC_SRC.split('"governed_decision_delegation": {')[1].split(
            "},")[0]
        for banned in ("record_id", "decided_at", "decision_record",
                       "persist_status"):
            assert banned not in block


# =========================================================================== #
# D / E / F — how the decision READS.
# =========================================================================== #
class TestPersistenceClassification:

    def test_27_a_ledger_row_classifies_as_a_ledger_row(self, tmp_path):
        cand, gate = _daily()
        rec = _record(cand, gate, tmp_path)["record"]
        got = pdec.classify_decision_persistence(record=rec)
        assert got["persistence_status"] == pdec.DECISION_PERSISTENCE_LEDGER_ROW
        assert got["is_ledger_row"] is True
        assert got["persistence_blocker"] is None

    def test_28_a_pre_cutover_projection_stays_legitimate(self):
        got = pdec.classify_decision_persistence(record={
            "decision": pdec.GD_NO_CHANGE, "projected": True,
            "legacy_compatibility_projection": True,
            "eligible_market_session": LEGACY_SESSION,
            "producer_governed_write_delegation": None})
        assert got["persistence_status"] == \
            pdec.DECISION_PERSISTENCE_LEGACY_PROJECTION
        assert got["expected_ledger_row"] is False
        assert got["persistence_blocker"] is None
        assert got["backfilled"] is False

    def test_29_a_post_cutover_projection_is_a_named_defect(self):
        got = pdec.classify_decision_persistence(record={
            "decision": pdec.GD_NO_CHANGE, "projected": True,
            "legacy_compatibility_projection": True,
            "eligible_market_session": SESSION,
            "producer_governed_write_delegation": _delegation()})
        assert got["persistence_status"] == \
            pdec.DECISION_PERSISTENCE_UNPERSISTED
        assert got["expected_ledger_row"] is True
        assert got["is_ledger_row"] is False
        assert got["retrievable_through_owner"] is True
        assert got["persistence_blocker"] == \
            pdec.GOVERNED_DAILY_NOT_PERSISTED_BLOCKER
        assert got["historical_gap_preserved"] is True
        assert got["backfilled"] is False
        assert got["history_rewritten"] is False

    def test_30_absent_is_still_absent(self):
        got = pdec.classify_decision_persistence(record=None, available=False)
        assert got["persistence_status"] == pdec.DECISION_PERSISTENCE_ABSENT
        assert got["retrievable_through_owner"] is False
        assert got["persistence_blocker"] is None

    def test_31_a_real_row_outranks_and_retires_the_projection(self, tmp_path):
        cand, gate = _daily()
        _record(cand, gate, tmp_path)
        got = pdec.load_governed_portfolio_decision(
            workflow=_workflow(delegation=_delegation()),
            reassessment=_reas(), proposal_summary=_summ(),
            constrained=_con(), active_book_id=BOOK,
            decision_dir=str(tmp_path))
        assert got["legacy_daily_projection_suppressed"] is True
        assert got["persistence_status"] == \
            pdec.DECISION_PERSISTENCE_LEDGER_ROW
        assert got["is_ledger_row"] is True
        assert got["persistence_blocker"] is None

    def test_32_a_legacy_session_remains_readable_with_no_row(self, tmp_path):
        got = pdec.load_governed_portfolio_decision(
            workflow=_workflow(delegation=None, session=LEGACY_SESSION,
                               run_id="drc_2026-09-02_15abfb01856f"),
            reassessment=_reas(hoc_hash="HOC_STORED",
                               eligible_market_date=LEGACY_SESSION),
            proposal_summary=_summ(), constrained=_con(),
            active_book_id=BOOK, decision_dir=str(tmp_path))
        assert got["available"] is True
        assert got["persistence_status"] == \
            pdec.DECISION_PERSISTENCE_LEGACY_PROJECTION
        assert got["retrievable_through_owner"] is True
        assert not (tmp_path / "governed_decisions.json").exists()

    def test_33_a_post_cutover_session_with_no_row_reports_the_blocker(
            self, tmp_path):
        got = pdec.load_governed_portfolio_decision(
            workflow=_workflow(delegation=_delegation()),
            reassessment=_reas(), proposal_summary=_summ(), constrained=_con(),
            active_book_id=BOOK, decision_dir=str(tmp_path))
        assert got["available"] is True
        assert got["persistence_status"] == \
            pdec.DECISION_PERSISTENCE_UNPERSISTED
        assert got["persistence_blocker"] == \
            pdec.GOVERNED_DAILY_NOT_PERSISTED_BLOCKER
        assert not (tmp_path / "governed_decisions.json").exists()

    def test_34_reading_a_gap_never_creates_a_row(self, tmp_path):
        for _ in range(3):
            pdec.load_governed_portfolio_decision(
                workflow=_workflow(delegation=_delegation()),
                reassessment=_reas(), proposal_summary=_summ(),
                constrained=_con(), active_book_id=BOOK,
                decision_dir=str(tmp_path))
        assert sorted(p.name for p in tmp_path.iterdir()) == []


# =========================================================================== #
# G — the intraday no-op writes nothing.
# =========================================================================== #
class TestIntradayNoOpCreatesNoDecision:

    def test_35_a_not_required_intraday_cycle_writes_no_decision(self, tmp_path):
        """A cycle in which no candidate can exist is TERMINAL and writes none."""
        got = pdec.classify_intraday_governance(event_cycle={
            "state": "NO_NEW_INFORMATION", "run_id": "evt_1",
            "reassessment_ran": False, "governance_gate_invoked": False,
            "governed_decision": None})
        assert got["terminal"] is True
        assert got["required"] is False
        assert got.get("governed_record_id") in (None, "")
        assert sorted(p.name for p in tmp_path.iterdir()) == []

    def test_36_the_intraday_lane_never_maps_current_no_change_to_a_decision(
            self):
        """R54.4 §5, unchanged: concluding for a SESSION is the daily lane's."""
        assert pdec.GD_NO_CHANGE == "CURRENT_NO_CHANGE"
        block = PD_SRC.split("def build_intraday_candidate")[1][:8000]
        # The intraday producer promotes only on a PRICED target outcome.
        assert "GD_NO_CHANGE" not in block

    def test_37_no_no_op_row_is_written_to_satisfy_a_counter(self, tmp_path):
        cand, gate = _daily(hocb=_hocb(hoc_artifact_identity_matches=False))
        pdec.record_governed_decision(
            candidate=cand, gate=gate,
            provenance=pdec.PROV_GOVERNED_DAILY_CYCLE,
            confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN,
            decision_dir=str(tmp_path))
        assert sorted(p.name for p in tmp_path.iterdir()) == []


# =========================================================================== #
# H / K — Active Manager + operator acceptance.
# =========================================================================== #
def _state(governed):
    return ams.build_active_manager_state(
        workflow={}, portfolio_state=_ps(), governed_decision=governed)


class TestActiveManagerAndAcceptance:

    def test_38_active_manager_exposes_a_real_ledger_row(self, tmp_path):
        cand, gate = _daily()
        _record(cand, gate, tmp_path)
        gov = pdec.load_governed_portfolio_decision(
            workflow=_workflow(delegation=_delegation()), reassessment=_reas(),
            proposal_summary=_summ(), constrained=_con(), active_book_id=BOOK,
            decision_dir=str(tmp_path))
        block = _state(gov)["latest_governed_portfolio_decision"]
        assert block["is_ledger_row"] is True
        assert block["expected_ledger_row"] is True
        assert block["persistence_blocker"] is None

    def test_39_active_manager_exposes_the_missing_write(self, tmp_path):
        gov = pdec.load_governed_portfolio_decision(
            workflow=_workflow(delegation=_delegation()), reassessment=_reas(),
            proposal_summary=_summ(), constrained=_con(), active_book_id=BOOK,
            decision_dir=str(tmp_path))
        block = _state(gov)["latest_governed_portfolio_decision"]
        assert block["persistence_status"] == \
            pdec.DECISION_PERSISTENCE_UNPERSISTED
        assert block["expected_ledger_row"] is True
        assert block["persistence_blocker"] == \
            pdec.GOVERNED_DAILY_NOT_PERSISTED_BLOCKER
        assert block["retrievable_through_owner"] is True

    def test_40_acceptance_is_complete_for_a_real_row(self, tmp_path):
        cand, gate = _daily()
        _record(cand, gate, tmp_path)
        gov = pdec.load_governed_portfolio_decision(
            workflow=_workflow(delegation=_delegation()), reassessment=_reas(),
            proposal_summary=_summ(), constrained=_con(), active_book_id=BOOK,
            decision_dir=str(tmp_path))
        acc = ams.build_acceptance_contract(_state(gov))
        row = next(r for r in acc["rows"] if r["row"] == "GOVERNED_DECISION")
        assert row["status"] == ams.ACCEPTANCE_PRESENT
        assert row["is_ledger_row"] is True
        assert acc["blocker_codes"] == []

    def test_41_acceptance_blocks_on_a_missing_post_cutover_row(self, tmp_path):
        gov = pdec.load_governed_portfolio_decision(
            workflow=_workflow(delegation=_delegation()), reassessment=_reas(),
            proposal_summary=_summ(), constrained=_con(), active_book_id=BOOK,
            decision_dir=str(tmp_path))
        acc = ams.build_acceptance_contract(_state(gov))
        row = next(r for r in acc["rows"] if r["row"] == "GOVERNED_DECISION")
        # It is PRESENT — the decision was genuinely reached — and still blocks.
        assert row["status"] == ams.ACCEPTANCE_PRESENT
        assert row["expected_ledger_row"] is True
        assert acc["blocker_codes"] == [
            pdec.GOVERNED_DAILY_NOT_PERSISTED_BLOCKER]
        assert acc["complete"] is False
        assert acc["missing_rows"] != ["GOVERNED_DECISION"]

    def test_42_acceptance_accepts_a_legitimate_legacy_projection(
            self, tmp_path):
        gov = pdec.load_governed_portfolio_decision(
            workflow=_workflow(delegation=None, session=LEGACY_SESSION,
                               run_id="drc_2026-09-02_15abfb01856f"),
            reassessment=_reas(eligible_market_date=LEGACY_SESSION),
            proposal_summary=_summ(), constrained=_con(), active_book_id=BOOK,
            decision_dir=str(tmp_path))
        acc = ams.build_acceptance_contract(_state(gov))
        row = next(r for r in acc["rows"] if r["row"] == "GOVERNED_DECISION")
        assert row["status"] == ams.ACCEPTANCE_PRESENT
        assert row["is_ledger_row"] is False
        assert acc["blocker_codes"] == []

    def test_43_the_acceptance_row_vocabulary_is_unchanged(self):
        assert ams.ACCEPTANCE_ROWS == (
            "COLLECTION", "SIGNAL", "SCORING", "HOC", "REASSESSMENT",
            "GOVERNANCE", "GOVERNED_DECISION", "OPERATIONAL_BOOK",
            "NEXT_ACTION", "LATENCY")

    def test_44_the_acceptance_view_still_recomputes_nothing(self, tmp_path):
        gov = pdec.load_governed_portfolio_decision(
            workflow=_workflow(delegation=_delegation()), reassessment=_reas(),
            proposal_summary=_summ(), constrained=_con(), active_book_id=BOOK,
            decision_dir=str(tmp_path))
        acc = ams.build_acceptance_contract(_state(gov))
        assert acc["recomputes_nothing"] is True and acc["read_only"] is True
        # the blocker is the DECISION OWNER's word, echoed
        assert acc["blockers"][0]["owner"] == ams.COMPONENT_OWNERS[
            "latest_governed_portfolio_decision"]

    def test_45_the_operator_report_gates_on_the_blocker(self):
        assert "_print_governed_persistence" in ACC_SRC
        assert "blocker_codes" in ACC_SRC
        assert "R55_ACCEPTANCE_INCOMPLETE" in ACC_SRC
        assert "expected to persist" in ACC_SRC

    def test_46_the_operator_report_refuses_a_stale_served_contract(self):
        assert 'if "blocker_codes" in served or not gd.get(' in ACC_SRC
        assert "RECOMPOSED_LOCALLY" in ACC_SRC


# =========================================================================== #
# ARCHITECTURE + SAFETY — 47-56.
# =========================================================================== #
class TestBoundariesAndSafety:

    def test_47_exactly_one_governed_decision_writer(self):
        assert "def record_governed_decision(" in PD_SRC
        for src in (DRC_SRC, AMS_SRC, HOC_SRC):
            assert "def record_governed_decision(" not in src

    def test_48_no_second_governed_store_or_index(self):
        for src in (DRC_SRC, AMS_SRC, HOC_SRC):
            assert "governed_decisions.json" not in src
            assert "governed_index.json" not in src

    def test_49_no_backfill_or_recover_decision_route(self):
        for src in (PD_SRC, DRC_SRC, AMS_SRC):
            low = src.lower()
            for banned in ("backfill_decision", "recover_governed_decision",
                           "rebuild_governed_ledger", "repair_decision"):
                assert banned not in low

    def test_50_the_daily_producer_still_owns_no_decision_persistence(self):
        assert "_DECISION_OWNER = \"api.portfolio_decision\"" in DRC_SRC
        for banned in ("def record_governed_decision",
                       "def governed_decision_ordering_key",
                       "def resolve_decision_authority"):
            assert banned not in DRC_SRC

    def test_51_active_manager_remains_a_read_composition(self):
        for banned in ("def record_governed_decision", "_atomic_write_json(",
                       "def persist_", "open("):
            assert banned not in AMS_SRC

    def test_52_the_daily_lane_reaches_no_execution_surface(self):
        """Token-level, so English prose ("ordered", "fulfil") never masks a
        real reach and never falsely fails one."""
        import re
        block = DRC_SRC.split("def _delegate_governed_decision")[1].split(
            "\n_REQUIRED_TERMINAL_FIELDS")[0]
        tokens = set(re.findall(r"[A-Za-z_][A-Za-z0-9_]*", block.lower()))
        for banned in ("order", "orders", "order_plan", "fill", "fills",
                       "broker", "promote", "promotion", "daily_close",
                       "scheduler", "approve", "approval"):
            assert banned not in tokens, banned

    def test_53_a_governed_row_advances_no_operational_fact(self, tmp_path):
        cand, gate = _daily()
        rec = _record(cand, gate, tmp_path)["record"]
        s = rec["safety"]
        for flag in ("changed_holdings", "changed_cash", "changed_nav",
                     "created_orders", "created_order_plan", "created_fills",
                     "broker_enabled", "approved_anything", "promoted_model",
                     "activated_sleeve", "ran_daily_close",
                     "advances_operational_mark", "rewrote_history"):
            assert s[flag] is False, flag

    def test_54_the_writer_touches_only_its_own_two_files(self, tmp_path):
        cand, gate = _daily()
        _record(cand, gate, tmp_path)
        assert sorted(p.name for p in tmp_path.iterdir()) == [
            "governed_decisions.json", "governed_index.json"]

    def test_55_the_opportunity_cost_owner_writes_only_its_own_store(
            self, tmp_path):
        hoc_dir, _, _ = _seed_reused_artifact(tmp_path)
        assert sorted(p.name for p in Path(hoc_dir).iterdir()) == [
            "artifacts", "index.json"]
        assert sorted(p.name for p in tmp_path.iterdir()) == ["hoc"]

    def test_56_the_persistence_vocabulary_is_closed_and_published(self):
        assert pdec.DECISION_PERSISTENCE_VOCAB == (
            "LEDGER_ROW", "LEGACY_COMPATIBILITY_PROJECTION",
            "POST_CUTOVER_NOT_PERSISTED", "ABSENT")
        for name in ("DECISION_PERSISTENCE_UNPERSISTED",
                     "GOVERNED_DAILY_NOT_PERSISTED_BLOCKER",
                     "governed_daily_write_expected"):
            assert name in pdec.__all__


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
