"""Release 55.2.3 — THE REUSED REASSESSMENT ARTIFACT IS THE BOUND ARTIFACT.

The defect, stated once. The 2026-09-10 Portfolio Cycle completed — Daily Close,
Daily Research Cycle COMPLETE, reassessment ``PROPOSAL_READY``, a reviewable
proposal — and left NO governed ledger row. Replaying the real manifest through
the daily gate gave the answer:

    verdict DAILY_DECISION_WITHHELD   18/19   REASSESSMENT_IDENTITY_MISMATCH
    failed check: REASSESSMENT_MATCHES_MANIFEST
    manifest reassessment 3857d4ab84b9... vs candidate 7471fe99c542...

and the fact behind it. The post-close intraday lane persisted the session's
reassessment artifact first. The Daily Research Cycle then reached the SAME
conclusion from the SAME evidence about the SAME economic state, so
``persist_reassessment`` correctly REUSED the stored artifact — but returned the
identity of the DISCARDED recomputation, so the manifest published the existing
``artifact_id`` beside a ``reassessment_hash`` that artifact does not carry. The
gate, reading the artifact the store actually holds, was right to refuse.

This is the R55.2.2 defect class exactly: an idempotent REUSE must report the
identity of the artifact the store HOLDS, not the identity of the recomputation
it threw away. R55.2.2 fixed it in the opportunity-cost owner; this fixes the
second owner that had it, in the same shape and with the same vocabulary.

Note the reuse resolves on the DECISION-FINGERPRINT branch, not the hash branch:
``reassessment_hash`` covers the whole result document INCLUDING ``provenance``,
while ``decision_fingerprint`` excludes it. Two lanes that differ only inside
``provenance`` are therefore the same assessment on all three axes while hashing
differently document-wide — which is why 09-04, 09-08 and 09-09 matched and only
09-10 did not.

Sep-10's absent row is NOT backfilled here and NOT repaired here. It stays a
documented historical gap; these tests prove the current and future path binds
what the store can produce.

Every write below lands in a pytest ``tmp_path``. Nothing here reads or mutates a
production store, creates an order, a fill or an approval, or restarts anything.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from paper_trader.api import daily_research_cycle as drc
from paper_trader.api import portfolio_decision as pdec
from paper_trader.api import portfolio_reassessment as PRS
from paper_trader.engine import constrained_reallocation as cr
from paper_trader.engine import portfolio_reassessment as K

BOOK = "alpha_paper_book_1"
SESSION = "2026-09-10"
RUN_ID = "drc_2026-09-10_8f888661ca46"
STAMP = "2026-09-10T23:49:41+00:00"

PRS_SRC = Path(PRS.__file__).read_text(encoding="utf-8")
DRC_SRC = Path(drc.__file__).read_text(encoding="utf-8")


# =========================================================================== #
# Deterministic Stage-20 fixtures (structurally faithful, fully synthetic).
# =========================================================================== #
def _review(ticker, **kw):
    base = {
        "ticker": ticker, "sector": "Tech", "current_quantity": 100,
        "current_weight": 0.04, "market_value": 4000.0,
        "current_rank": 10, "previous_rank": 10, "rank_change": 0,
        "current_score": 0.80, "score_components": {}, "signal_strength": 0.80,
        "deterioration_state": K.hoc_kernel.DET_STABLE,
        "deterioration_reason_codes": [],
        "return_5d": 0.01, "return_20d": 0.02, "return_60d": 0.05,
        "volatility_20d": 0.20, "volatility_60d": 0.22, "drawdown_60d": -0.05,
        "risk_contribution_pct": 0.04, "concentration_contribution": 0.04,
        "median_dollar_volume_20d": 5.0e7, "estimated_days_to_liquidate": 0.1,
        "liquidity_state": K.hoc_kernel.LIQ_LIQUID,
        "strongest_replacement_ticker": None, "replacement_rank": None,
        "replacement_score": None, "replacement_sector": None,
        "gross_score_improvement": None, "risk_adjusted_improvement": None,
        "switching_cost_bps": 25.0, "switching_cost_usd": 10.0,
        "net_improvement": None, "recommendation": K.REC_HOLD,
        "recommendation_confidence": "HIGH", "reason_codes": [],
        "explanation": "seed", "required_data_complete": True,
    }
    base.update(kw)
    return base


def _hoc(*, assessment_hash="HOC_STORED", ps_hash="PSH_A", econ_hash="ECON_A"):
    reviews = [_review("T%02d" % i, current_weight=0.04, current_rank=i + 1)
               for i in range(25)]
    cands = [{"ticker": "NEW1", "rank": 3, "score": 0.95, "combined_score": 0.95,
              "sector": "Health", "recommendation": "ADD"},
             {"ticker": "NEW2", "rank": 5, "score": 0.93, "combined_score": 0.93,
              "sector": "Fin", "recommendation": "ADD"}]
    invested = sum((r.get("market_value") or 0.0) for r in reviews)
    return {
        "schema_version": "holding_opportunity_cost.v1",
        "eligible_market_date": SESSION, "active_book_id": BOOK,
        "assessment_state": "READY", "assessment_hash": assessment_hash,
        "policy": {"policy_version": "hoc_decision_policy.v1"},
        "portfolio_summary": {
            "nav": 100000.0, "cash": 0.0, "invested_value": invested,
            "holdings_count": len(reviews),
            "max_name_weight": 0.04, "max_name_ticker": "T00",
            "max_sector_weight": 1.0, "max_sector": "Tech",
            "sector_weights": {"Tech": 1.0},
            "herfindahl_index": sum(r["current_weight"] ** 2 for r in reviews),
            "portfolio_variance_daily": 0.0001,
            "risk_contribution_state": "AVAILABLE"},
        "recommendation_counts": {"HOLD": len(reviews), "REDUCE": 0, "EXIT": 0,
                                  "REPLACE": 0, "ADD": len(cands)},
        "holding_reviews": reviews, "addition_candidates": cands,
        "diagnostics": {"eligible_universe_size": 503},
        "data_quality": {"data_gaps": []},
        "provenance": {"portfolio_state_hash": ps_hash,
                       "economic_state_hash": econ_hash,
                       "corporate_actions_hash": None,
                       "universe_scoring_hash": "US_A",
                       "hoc_assessment_hash": assessment_hash},
    }


def _pstate(*, ps_hash="PSH_A", econ_hash="ECON_A"):
    return {
        "dates": {"eligible_market_date": SESSION, "valuation_date": SESSION},
        "active_book": {"book_id": BOOK, "book_label": "Alpha Paper Book #1",
                        "status": "ACTIVE", "initialized": True,
                        "holdings_count": 25},
        "capital": {"nav": 100000.0, "cash": 0.0},
        "state_hash": ps_hash, "economic_state_hash": econ_hash,
    }


def _scoring(output_hash="US_A", input_contract_hash="UIC_A"):
    return {"output_hash": output_hash, "input_contract_hash": input_contract_hash,
            "strategy_id": "fundamental_momentum_50_50_v1", "strategy_version": "v1",
            "primary_model_id": "fundamental_momentum_50_50_v1",
            "champion_model_id": "composite_sn", "model_registry_version": "29",
            "universe_id": "phase8v_combined_eodhd_price_fundamentals_universe"}


def _freshness():
    return {"eligible_market_date": SESSION, "source_freshness": [
        {"source_id": "owned_daily_prices", "status": "FRESH",
         "as_of_date": SESSION, "cadence": "DAILY",
         "required_for_portfolio_reassessment": True,
         "authoritative_owner": "api.operational_book", "reason": "current",
         "expected_through_date": SESSION},
        {"source_id": "price_score_refresh", "status": "FRESH",
         "as_of_date": SESSION, "cadence": "DAILY",
         "required_for_portfolio_reassessment": True,
         "authoritative_owner": "api.multi_horizon_engine", "reason": "current",
         "expected_through_date": SESSION},
    ]}


def _contract(*, sc=None, hoc=None):
    return PRS.build_input_contract(
        portfolio_state=_pstate(), scoring=sc or _scoring(),
        hoc_assessment=hoc if hoc is not None else _hoc(),
        freshness=_freshness(), recent_change_history=[])


def _run(**kw):
    return PRS.run_reassessment(input_contract=_contract(**kw))


def _rederived(run):
    """The SECOND lane's output: the identical conclusion from identical
    evidence, differing ONLY inside ``provenance`` — so the document-wide
    ``reassessment_hash`` moves while the economic state, the assessment
    evidence and the decision fingerprint do not. This is the production shape
    that made 2026-09-10 differ from 09-04, 09-08 and 09-09."""
    res = json.loads(json.dumps(run["reassessment"]))
    prov = dict(res.get("provenance") or {})
    prov["produced_by"] = "api.daily_research_cycle"
    prov["drc_run_id"] = RUN_ID
    res["provenance"] = prov
    res["reassessment_hash"] = K.stable_hash(res)
    return {"reassessment": res, "input_contract": run["input_contract"]}


def _persist(tmp_path, run, *, now=None):
    return PRS.persist_reassessment(result=run["reassessment"],
                                    input_contract=run["input_contract"],
                                    reassessment_dir=str(tmp_path), now=now)


def _seed_reused(tmp_path):
    """Persist lane 1, then re-run lane 2. Returns (first, reuse, stored, redone)."""
    first_run = _run()
    first = _persist(tmp_path, first_run)
    second_run = _rederived(first_run)
    again = _persist(tmp_path, second_run)
    return first, again, first_run, second_run


# =========================================================================== #
# THE ROOT CAUSE — 1-9. The persistence owner itself.
# =========================================================================== #
class TestReassessmentReuseIdentity:

    def test_01_the_two_lanes_really_are_the_same_assessment(self, tmp_path):
        """Guard the fixture: same conclusion, same evidence, DIFFERENT document
        hash. Without this the rest of the suite could pass on the hash branch
        and never exercise the defect."""
        first_run = _run()
        second_run = _rederived(first_run)
        a, b = first_run["reassessment"], second_run["reassessment"]
        assert a["reassessment_hash"] != b["reassessment_hash"]
        assert PRS.decision_fingerprint(a) == PRS.decision_fingerprint(b)
        assert (PRS.artifact_identity(input_contract=first_run["input_contract"],
                                      result=a)["assessment_evidence_hash"]
                == PRS.artifact_identity(input_contract=second_run["input_contract"],
                                         result=b)["assessment_evidence_hash"])

    def test_02_reuse_returns_the_identity_of_the_artifact_actually_held(
            self, tmp_path):
        """The defect itself. Reuse means the caller's document was NOT written."""
        first, again, first_run, second_run = _seed_reused(tmp_path)
        assert first["status"] == PRS.PERSIST_CREATED
        assert again["status"] == PRS.PERSIST_REUSED
        assert again["reused"] is True and again["conflict"] is False
        assert again["artifact_id"] == first["artifact_id"]
        stored_hash = first_run["reassessment"]["reassessment_hash"]
        redone_hash = second_run["reassessment"]["reassessment_hash"]
        assert again["identity"]["reassessment_hash"] == stored_hash
        assert again["recomputed_reassessment_hash"] == redone_hash
        assert again["reused_recomputed_document"] is True

    def test_03_the_reported_id_and_hash_describe_ONE_object_on_disk(
            self, tmp_path):
        """The whole point: the pair must be resolvable in the store."""
        _, again, _, _ = _seed_reused(tmp_path)
        doc = json.loads((Path(tmp_path) / "artifacts"
                          / ("%s.json" % again["artifact_id"])).read_text("utf-8"))
        assert doc["identity"]["reassessment_hash"] == \
            again["identity"]["reassessment_hash"]
        assert doc["reassessment"]["reassessment_hash"] == \
            again["identity"]["reassessment_hash"]
        assert PRS.artifact_id_for(again["identity"]) == again["artifact_id"]

    def test_04_reuse_never_writes_a_second_artifact_or_history_row(self, tmp_path):
        first, again, _, _ = _seed_reused(tmp_path)
        files = sorted(p.name for p in (Path(tmp_path) / "artifacts").iterdir())
        assert files == ["%s.json" % first["artifact_id"]]
        assert again["history_appended"] is False
        assert len(PRS.load_history(reassessment_dir=str(tmp_path))) == 1
        idx = json.loads((Path(tmp_path) / "index.json").read_text("utf-8"))
        assert len(idx) == 1

    def test_05_reuse_leaves_the_stored_artifact_byte_identical(self, tmp_path):
        first_run = _run()
        first = _persist(tmp_path, first_run)
        path = (Path(tmp_path) / "artifacts" / ("%s.json" % first["artifact_id"]))
        before = path.read_bytes()
        _persist(tmp_path, _rederived(first_run))
        assert path.read_bytes() == before

    def test_06_repeated_execution_stays_idempotent(self, tmp_path):
        """Five more lanes, all the same assessment. Still ONE artifact."""
        first_run = _run()
        first = _persist(tmp_path, first_run)
        for _ in range(5):
            out = _persist(tmp_path, _rederived(first_run))
            assert out["status"] == PRS.PERSIST_REUSED
            assert out["artifact_id"] == first["artifact_id"]
            assert out["identity"]["reassessment_hash"] == \
                first["identity"]["reassessment_hash"]
        assert len(list((Path(tmp_path) / "artifacts").glob("*.json"))) == 1
        assert len(PRS.load_history(reassessment_dir=str(tmp_path))) == 1

    def test_07_an_identical_rerun_is_still_a_plain_reuse(self, tmp_path):
        """The hash branch — unchanged by this fix, and no re-derivation named."""
        run = _run()
        first = _persist(tmp_path, run)
        again = _persist(tmp_path, _run())
        assert again["status"] == PRS.PERSIST_REUSED
        assert again["artifact_id"] == first["artifact_id"]
        assert again["reused_recomputed_document"] is False
        assert PRS.recomputed_reassessment_hash(again,
                                                run["reassessment"]) is None

    def test_08_a_genuinely_different_conclusion_is_still_a_conflict(
            self, tmp_path):
        """Fail-closed is not relaxed: same evidence, a different answer, refused."""
        run = _run()
        _persist(tmp_path, run)
        changed = json.loads(json.dumps(run["reassessment"]))
        changed["recommendation_counts"] = {"HOLD": 24, "EXIT": 1}
        changed["reassessment_hash"] = K.stable_hash(changed)
        out = PRS.persist_reassessment(result=changed,
                                       input_contract=run["input_contract"],
                                       reassessment_dir=str(tmp_path))
        assert out["status"] == PRS.PERSIST_CONFLICT
        assert out["persisted"] is False and out["reused"] is False

    def test_09_materially_new_evidence_still_creates_a_version(self, tmp_path):
        first = _persist(tmp_path, _run())
        second = _persist(tmp_path, _run(sc=_scoring(output_hash="US_B",
                                                     input_contract_hash="UIC_B")))
        assert second["status"] == PRS.PERSIST_ASSESSMENT_VERSION
        assert second["persisted"] is True and second["reused"] is False
        assert second["superseded_artifact_id"] == first["artifact_id"]
        assert second["identity"]["reassessment_hash"] == \
            second["identity"]["reassessment_hash"]


# =========================================================================== #
# THE ONE SPELLING — 10-13. bound vs recomputed.
# =========================================================================== #
class TestBoundHashOwner:

    def test_10_the_bound_hash_is_the_stored_one_after_a_reuse(self, tmp_path):
        _, again, first_run, second_run = _seed_reused(tmp_path)
        stored = first_run["reassessment"]["reassessment_hash"]
        redone = second_run["reassessment"]["reassessment_hash"]
        assert PRS.bound_reassessment_hash(again,
                                           second_run["reassessment"]) == stored
        assert PRS.recomputed_reassessment_hash(
            again, second_run["reassessment"]) == redone

    def test_11_a_refused_write_keeps_its_OWN_hash_and_says_so(self, tmp_path):
        """A refused write stays visible AS a refused write; nothing is repaired."""
        run = _run()
        refused = {"status": PRS.PERSIST_CONFLICT, "artifact_id": None,
                   "persisted": False}
        assert PRS.persistence_succeeded(refused) is False
        assert PRS.bound_reassessment_hash(refused, run["reassessment"]) == \
            run["reassessment"]["reassessment_hash"]

    def test_12_no_persistence_outcome_falls_back_to_the_runs_own_hash(self):
        run = _run()
        assert PRS.bound_reassessment_hash(None, run["reassessment"]) == \
            run["reassessment"]["reassessment_hash"]
        assert PRS.bound_reassessment_hash(None, None) is None

    def test_13_the_owner_of_the_answer_is_the_store_that_decides_it(self):
        assert PRS.BOUND_HASH_OWNER == PRS.COMPOSITION_OWNER
        assert PRS.PERSIST_REUSED in PRS.PERSIST_SUCCESS_STATUSES
        assert PRS.PERSIST_CONFLICT not in PRS.PERSIST_SUCCESS_STATUSES
        assert PRS.PERSIST_INCONSISTENT not in PRS.PERSIST_SUCCESS_STATUSES


# =========================================================================== #
# THE PRODUCER SEAM — 14-17. What the daily manifest publishes.
# =========================================================================== #
class TestDailyManifestBindsWhatItCanRetrieve:

    def test_14_the_manifest_records_the_bound_hash_not_the_transient_one(
            self, tmp_path):
        first, again, first_run, second_run = _seed_reused(tmp_path)
        built = {"reassessment": second_run["reassessment"],
                 "input_contract": second_run["input_contract"],
                 "persistence": again}
        out = drc._extract_reassessment(built, SESSION)
        assert out["reassessment_hash"] == \
            first_run["reassessment"]["reassessment_hash"]
        assert out["computed_reassessment_hash"] == \
            second_run["reassessment"]["reassessment_hash"]
        assert out["recomputed_reassessment_hash"] == \
            second_run["reassessment"]["reassessment_hash"]
        assert out["reassessment_id"] == first["artifact_id"]
        assert out["reused_recomputed_document"] is True
        assert out["persisted"] is True

    def test_15_the_manifest_pair_resolves_in_the_store(self, tmp_path):
        """id + hash must name one retrievable artifact — the gate's question."""
        _, again, _, second_run = _seed_reused(tmp_path)
        out = drc._extract_reassessment(
            {"reassessment": second_run["reassessment"], "persistence": again},
            SESSION)
        doc = json.loads((Path(tmp_path) / "artifacts"
                          / ("%s.json" % out["reassessment_id"])).read_text("utf-8"))
        assert doc["identity"]["reassessment_hash"] == out["reassessment_hash"]

    def test_16_a_created_artifact_is_unchanged_by_this_fix(self, tmp_path):
        """The normal path: nothing moves when nothing was reused."""
        run = _run()
        created = _persist(tmp_path, run)
        out = drc._extract_reassessment(
            {"reassessment": run["reassessment"], "persistence": created}, SESSION)
        assert out["reassessment_hash"] == run["reassessment"]["reassessment_hash"]
        assert out["computed_reassessment_hash"] == out["reassessment_hash"]
        assert out["recomputed_reassessment_hash"] is None
        assert out["reused_recomputed_document"] in (None, False)

    def test_17_an_unpersisted_reassessment_keeps_its_own_hash(self, tmp_path):
        run = _run()
        refused = {"status": PRS.PERSIST_CONFLICT, "artifact_id": None,
                   "persisted": False}
        out = drc._extract_reassessment(
            {"reassessment": run["reassessment"], "persistence": refused}, SESSION)
        assert out["reassessment_hash"] == run["reassessment"]["reassessment_hash"]
        assert out["persisted"] is False
        assert out["reassessment_id"] is None


# =========================================================================== #
# THE GATE — 18-23. The 2026-09-10 failure, and its absence after the fix.
# =========================================================================== #
def _ps():
    return {"active_book": {"book_id": BOOK},
            "dates": {"eligible_market_date": SESSION},
            "state_hash": "PSH_DOC", "economic_state_hash": "ESH1"}


def _man(ra_hash, ra_id, **kw):
    d = {"run_id": RUN_ID, "state": "COMPLETE",
         "active_book_id": BOOK, "eligible_market_date": SESSION,
         "completed_at": STAMP,
         "session_contract_hash": "SCH1", "input_contract_hash": "ICH1",
         "portfolio_reassessment_id": ra_id,
         "portfolio_reassessment_hash": ra_hash,
         "portfolio_reassessment_state": "CURRENT_NO_CHANGE",
         "reallocation_proposal_id": "", "reallocation_proposal_hash": "",
         "reallocation_proposal_state": "NOT_REQUIRED",
         "opportunity_cost_artifact_id": "hoc_a",
         "opportunity_cost_assessment_hash": "HOC_STORED",
         "governed_decision_delegation": {
             "owner": "api.portfolio_decision",
             "contract": drc.GOVERNED_DELEGATION_CONTRACT,
             "delegates_terminal_decision": True,
             "decision_is_not_recorded_here": True}}
    d.update(kw)
    return d


def _reas(ra_hash, ra_id):
    """The read-view shape ``load_portfolio_reassessment`` returns — built from
    the STORED artifact, which is why the candidate side was always right."""
    return {"state": "CURRENT_NO_CHANGE", "eligible_market_date": SESSION,
            "active_book": {"book_id": BOOK},
            "reassessment_id": ra_id, "reassessment_hash": ra_hash,
            "artifact": {"reassessment_id": ra_id, "generated_at": STAMP,
                         "identity": {"economic_state_hash": "ESH1"}},
            "proposal_binding": {"reassessment_id": ra_id,
                                 "reassessment_hash": ra_hash,
                                 "hoc_assessment_hash": "HOC_STORED",
                                 "hoc_artifact_id": "hoc_a",
                                 "hoc_assessment_evidence_hash": "HOCEV1",
                                 "hoc_persisted": True,
                                 "universe_scoring_hash": "US1",
                                 "universe_input_contract_hash": "UIC1",
                                 "portfolio_state_hash": "PSH1",
                                 "corporate_actions_hash": "CA1",
                                 "eligible_market_date": SESSION,
                                 "active_book_id": BOOK}}


def _summ():
    return {"reallocation_proposal_available": False,
            "reallocation_proposal_stale": False,
            "reallocation_proposal_withheld": False,
            "reallocation_corporate_actions_hash": "CA1",
            "reallocation_proposal_hash": None, "reallocation_proposal_id": None,
            "reallocation_outcome": None, "reallocation_data_gaps": []}


def _con():
    return {"outcome": None, "feasible_target_exists": False,
            "calculation_owner": cr.CALCULATION_OWNER,
            "switching_economics": {"switching_hurdle": 0.02,
                                    "clears_switching_hurdle": False,
                                    "score_improvement_net_of_cost": 0.0,
                                    "one_way_turnover": 0.0,
                                    "estimated_transaction_cost": 0.0},
            "ideal_target": {"zero_base_owner": "api.zero_base_target"},
            "multi_asset": {"current_holdings_privileged": False},
            "best_feasible_target": {"allocations": []}}


def _hocb():
    return {"hoc_artifact_id": "hoc_a", "hoc_assessment_hash": "HOC_STORED",
            "hoc_persisted": True, "hoc_persistence_status": "REUSED_EXISTING",
            "hoc_assessment_evidence_hash": "HOCEV1",
            "hoc_artifact_retrievable": True,
            "hoc_artifact_identity_matches": True,
            "hoc_binding_detail": "artifact opened by id",
            "hoc_binding_resolved_by":
                "api.holding_opportunity_cost.resolve_binding"}


def _gate_for(manifest_hash, candidate_hash, ra_id="prs_a"):
    man = _man(manifest_hash, ra_id)
    reas = _reas(candidate_hash, ra_id)
    cand = pdec.build_daily_cycle_candidate(
        portfolio_state=_ps(), drc_manifest=man, reassessment=reas,
        proposal_summary=_summ(), constrained=_con(),
        scoring_identity={"ranking_date": SESSION, "input_contract_hash": "UIC1"},
        hoc_binding=_hocb())
    gate = pdec.evaluate_daily_cycle_governance(
        candidate=cand, drc_manifest=man, portfolio_state=_ps(),
        reassessment=reas, proposal_summary=_summ(), constrained=_con(),
        current_governed=None)
    return cand, gate


def _reassessment_check(gate):
    for c in gate.get("checks") or []:
        if c.get("check") == "REASSESSMENT_MATCHES_MANIFEST":
            return c
    raise AssertionError("REASSESSMENT_MATCHES_MANIFEST check not found")


class TestTheDecisionGateAcceptsTheReusedIdentity:

    def test_18_the_pre_fix_2026_09_10_shape_is_refused(self, tmp_path):
        """Reproduce the production failure exactly: the manifest carries the
        RECOMPUTED hash, the candidate the STORED one."""
        _, again, first_run, second_run = _seed_reused(tmp_path)
        _, gate = _gate_for(second_run["reassessment"]["reassessment_hash"],
                            first_run["reassessment"]["reassessment_hash"],
                            ra_id=again["artifact_id"])
        assert gate["eligible"] is False
        assert pdec.WR_REASSESSMENT_IDENTITY in gate["withheld_reason_codes"]
        assert _reassessment_check(gate)["passed"] is False

    def test_19_the_post_fix_manifest_is_accepted(self, tmp_path):
        """The same reuse, published through the fixed seam."""
        _, again, first_run, second_run = _seed_reused(tmp_path)
        published = drc._extract_reassessment(
            {"reassessment": second_run["reassessment"], "persistence": again},
            SESSION)
        _, gate = _gate_for(published["reassessment_hash"],
                            first_run["reassessment"]["reassessment_hash"],
                            ra_id=published["reassessment_id"])
        assert _reassessment_check(gate)["passed"] is True
        assert pdec.WR_REASSESSMENT_IDENTITY not in gate["withheld_reason_codes"]
        assert gate["eligible"] is True, gate.get("withheld_reason_codes")

    def test_20_the_accepted_decision_writes_exactly_ONE_ledger_row(
            self, tmp_path):
        _, again, first_run, second_run = _seed_reused(tmp_path / "prs")
        published = drc._extract_reassessment(
            {"reassessment": second_run["reassessment"], "persistence": again},
            SESSION)
        cand, gate = _gate_for(published["reassessment_hash"],
                               first_run["reassessment"]["reassessment_hash"],
                               ra_id=published["reassessment_id"])
        dd = tmp_path / "decisions"
        out = pdec.record_governed_decision(
            candidate=cand, gate=gate, provenance=pdec.PROV_GOVERNED_DAILY_CYCLE,
            confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN, decision_dir=str(dd))
        assert out["recorded"] is True and out["status"] == "CREATED"
        rows = json.loads((dd / "governed_decisions.json").read_text("utf-8"))
        assert len(rows) == 1

    def test_21_a_repeat_of_the_whole_cycle_creates_no_second_record(
            self, tmp_path):
        """Idempotency end to end: reuse the artifact, republish, re-record."""
        prs_dir = tmp_path / "prs"
        dd = tmp_path / "decisions"
        first_run = _run()
        first = _persist(prs_dir, first_run)
        record_ids = []
        for _ in range(3):
            again = _persist(prs_dir, _rederived(first_run))
            published = drc._extract_reassessment(
                {"reassessment": _rederived(first_run)["reassessment"],
                 "persistence": again}, SESSION)
            cand, gate = _gate_for(published["reassessment_hash"],
                                   first_run["reassessment"]["reassessment_hash"],
                                   ra_id=published["reassessment_id"])
            assert gate["eligible"] is True, gate.get("withheld_reason_codes")
            out = pdec.record_governed_decision(
                candidate=cand, gate=gate,
                provenance=pdec.PROV_GOVERNED_DAILY_CYCLE,
                confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN,
                decision_dir=str(dd))
            record_ids.append(out["record"]["record_id"])
        assert len(set(record_ids)) == 1
        rows = json.loads((dd / "governed_decisions.json").read_text("utf-8"))
        assert len(rows) == 1
        assert len(list((prs_dir / "artifacts").glob("*.json"))) == 1
        assert len(PRS.load_history(reassessment_dir=str(prs_dir))) == 1
        assert first["artifact_id"] == published["reassessment_id"]

    def test_22_a_genuinely_wrong_manifest_still_fails_closed(self, tmp_path):
        """The fix must not turn the gate into a rubber stamp."""
        _, _, first_run, _ = _seed_reused(tmp_path)
        _, gate = _gate_for("A_HASH_FROM_ANOTHER_SESSION",
                            first_run["reassessment"]["reassessment_hash"])
        assert gate["eligible"] is False
        assert pdec.WR_REASSESSMENT_IDENTITY in gate["withheld_reason_codes"]

    def test_23_a_withheld_candidate_writes_nothing(self, tmp_path):
        _, _, first_run, second_run = _seed_reused(tmp_path / "prs")
        cand, gate = _gate_for(second_run["reassessment"]["reassessment_hash"],
                               first_run["reassessment"]["reassessment_hash"])
        dd = tmp_path / "decisions"
        out = pdec.record_governed_decision(
            candidate=cand, gate=gate, provenance=pdec.PROV_GOVERNED_DAILY_CYCLE,
            confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN, decision_dir=str(dd))
        assert out["recorded"] is False
        assert not (dd / "governed_decisions.json").exists()


# =========================================================================== #
# BOUNDARIES — 24-27.
# =========================================================================== #
class TestBoundariesAndSafety:

    def test_24_the_owner_reports_the_stored_identity_not_the_recomputation(self):
        """Source guard: the reuse branch must not hand back its own identity."""
        assert "return _reuse_outcome(existing, identity, reassessment_dir)" in PRS_SRC
        assert "def _stored_artifact_identity(" in PRS_SRC
        assert '"identity": stored,' in PRS_SRC

    def test_25_the_producer_asks_the_owner_which_hash_is_the_dependency(self):
        assert "bound_reassessment_hash(persistence, res)" in DRC_SRC
        assert '"reassessment_hash": bound_hash,' in DRC_SRC

    def test_26_nothing_here_backfills_or_rewrites_history(self, tmp_path):
        """Sep-10's missing governed row stays missing. Reuse APPENDS nothing,
        REWRITES nothing, and the fix adds no repair path that could invent a
        historical artifact or a historical governed decision."""
        first_run = _run()
        first = _persist(tmp_path, first_run)
        before = {p.name: p.read_bytes()
                  for p in (Path(tmp_path) / "artifacts").iterdir()}
        hist_before = (Path(tmp_path) / "history.json")
        hist_bytes = hist_before.read_bytes() if hist_before.exists() else None
        again = _persist(tmp_path, _rederived(first_run))
        assert again["history_appended"] is False
        after = {p.name: p.read_bytes()
                 for p in (Path(tmp_path) / "artifacts").iterdir()}
        assert after == before
        if hist_bytes is not None:
            assert hist_before.read_bytes() == hist_bytes
        assert len(PRS.load_history(reassessment_dir=str(tmp_path))) == 1
        assert again["artifact_id"] == first["artifact_id"]

    def test_27_no_order_fill_or_approval_is_reachable_from_this_path(
            self, tmp_path):
        _, again, first_run, second_run = _seed_reused(tmp_path / "prs")
        published = drc._extract_reassessment(
            {"reassessment": second_run["reassessment"], "persistence": again},
            SESSION)
        cand, gate = _gate_for(published["reassessment_hash"],
                               first_run["reassessment"]["reassessment_hash"],
                               ra_id=published["reassessment_id"])
        dd = tmp_path / "decisions"
        out = pdec.record_governed_decision(
            candidate=cand, gate=gate, provenance=pdec.PROV_GOVERNED_DAILY_CYCLE,
            confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN, decision_dir=str(dd))
        safety = out["record"]["safety"]
        assert safety["created_orders"] is False
        assert safety.get("created_fills", False) is False
        assert safety.get("approved_proposal", False) is False
