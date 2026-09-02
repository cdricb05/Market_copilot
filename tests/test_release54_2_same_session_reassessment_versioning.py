"""R54.2 — SAME-SESSION REASSESSMENT VERSIONING.

What these tests prove:

  * the portfolio being ECONOMICALLY unchanged does not mean the investment
    ASSESSMENT is unchanged: when the bound evidence moves (ranking, holding
    opportunity cost, input freshness) a NEW immutable version is APPENDED for
    the same (book, session), and the earlier artifact is never rewritten;
  * a re-run from identical evidence stays idempotent, and identical evidence
    that yields a DIFFERENT conclusion is still refused as a conflict;
  * an artifact whose own parts disagree about the session or the book is
    refused outright — versioning relaxes no point-in-time rule;
  * there is ONE reassessment history, ONE store and ONE writer: the Daily
    Research Cycle and the live event cycle append to the same chain;
  * multiple same-session versions never double-count — the churn control, the
    forward attribution and the Stage-21 outcome observations all read the
    session's AUTHORITATIVE assessment, and a version can never protect a name
    against its own successor;
  * downstream owners bind the EXACT version, an older id keeps resolving to the
    exact artifact it named, and an obsolete assessment can never silently reach
    approval or execution;
  * the R54.1 governance gate stays strict — it is TIGHTENED, not relaxed — and
    a valid same-session version reaches 38/38 hermetically, as HOLD or CHANGE;
  * nothing here advances the operational close mark, mutates the portfolio,
    creates an order, a fill, a broker call, a model promotion, an approval or a
    rebalance.

Every write path is hermetic (``tmp_path``); no production store, no provider,
no live backend and no scheduler is touched.
"""
from __future__ import annotations

import io
import json
import re
import tokenize
from datetime import datetime, timezone
from pathlib import Path

import pytest

from paper_trader.api import active_manager_state as ams
from paper_trader.api import event_signal_refresh as esr
from paper_trader.api import portfolio_decision as pdec
from paper_trader.api import portfolio_reassessment as PRS
from paper_trader.api import reassessment_outcomes as ro
from paper_trader.engine import constrained_reallocation as cr
from paper_trader.engine import portfolio_reassessment as K

REPO = Path(__file__).resolve().parents[1]
PRS_SRC = (REPO / "api" / "portfolio_reassessment.py").read_text(
    encoding="utf-8", errors="replace")

BOOK = "alpha_paper_book_1"
SESSION = "2026-08-31"
PREV = "2026-08-28"
HELD = ["T%02d" % i for i in range(25)]


def _code_only(rel_path: str) -> str:
    """Source with comments and string literals stripped — a guard must assert on
    what a module DOES, never on what its documentation says it does not do."""
    src = (REPO / rel_path).read_text(encoding="utf-8")
    out = []
    for tok in tokenize.generate_tokens(io.StringIO(src).readline):
        if tokenize.tok_name.get(tok.type, "") in (
                "COMMENT", "STRING", "FSTRING_MIDDLE", "NL"):
            continue
        out.append(tok.string)
    return " ".join(out)


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


def _hoc(reviews=None, *, eligible=SESSION, assessment_hash="HOC_A",
         ps_hash="PSH_A", econ_hash="ECON_A", ca_hash=None, nav=100000.0,
         cash=0.0):
    reviews = reviews if reviews is not None else [
        _review("T%02d" % i, current_weight=0.04, current_rank=i + 1)
        for i in range(25)]
    cands = [{"ticker": "NEW1", "rank": 3, "score": 0.95, "combined_score": 0.95,
              "sector": "Health", "recommendation": "ADD"},
             {"ticker": "NEW2", "rank": 5, "score": 0.93, "combined_score": 0.93,
              "sector": "Fin", "recommendation": "ADD"}]
    invested = sum((r.get("market_value") or 0.0) for r in reviews)
    return {
        "schema_version": "holding_opportunity_cost.v1",
        "eligible_market_date": eligible, "active_book_id": BOOK,
        "assessment_state": "READY", "assessment_hash": assessment_hash,
        "policy": {"policy_version": "hoc_decision_policy.v1"},
        "portfolio_summary": {
            "nav": nav, "cash": cash, "invested_value": invested,
            "holdings_count": len(reviews),
            "max_name_weight": max([r["current_weight"] for r in reviews] or [0]),
            "max_name_ticker": reviews[0]["ticker"] if reviews else None,
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
                       "corporate_actions_hash": ca_hash,
                       "universe_scoring_hash": "US_A",
                       "hoc_assessment_hash": assessment_hash},
    }


def _pstate(*, ps_hash="PSH_A", econ_hash="ECON_A", nav=100000.0, cash=0.0,
            eligible=SESSION, ca_fp=None):
    return {
        "dates": {"eligible_market_date": eligible, "valuation_date": eligible},
        "active_book": {"book_id": BOOK, "book_label": "Alpha Paper Book #1",
                        "status": "ACTIVE", "initialized": True,
                        "holdings_count": 25},
        "capital": {"nav": nav, "cash": cash},
        "state_hash": ps_hash, "economic_state_hash": econ_hash,
        "corporate_actions": ({"registry_fingerprint": ca_fp, "actions": []}
                              if ca_fp is not None else {}),
    }


def _scoring(output_hash="US_A", input_contract_hash="UIC_A"):
    return {"output_hash": output_hash, "input_contract_hash": input_contract_hash,
            "strategy_id": "fundamental_momentum_50_50_v1", "strategy_version": "v1",
            "primary_model_id": "fundamental_momentum_50_50_v1",
            "champion_model_id": "composite_sn", "model_registry_version": "29",
            "universe_id": "phase8v_combined_eodhd_price_fundamentals_universe"}


def _freshness(*, eligible=SESSION, prices_status="FRESH"):
    return {"eligible_market_date": eligible, "source_freshness": [
        {"source_id": "owned_daily_prices", "status": prices_status,
         "as_of_date": eligible, "cadence": "DAILY",
         "required_for_portfolio_reassessment": True,
         "authoritative_owner": "api.operational_book", "reason": "current",
         "expected_through_date": eligible},
        {"source_id": "price_score_refresh", "status": "FRESH",
         "as_of_date": eligible, "cadence": "DAILY",
         "required_for_portfolio_reassessment": True,
         "authoritative_owner": "api.multi_horizon_engine", "reason": "current",
         "expected_through_date": eligible},
    ]}


def _contract(*, ps=None, sc=None, hoc=None, fr=None, hist=None):
    return PRS.build_input_contract(
        portfolio_state=ps or _pstate(), scoring=sc or _scoring(),
        hoc_assessment=hoc if hoc is not None else _hoc(),
        freshness=fr if fr is not None else _freshness(),
        recent_change_history=hist or [])


def _run(**kw):
    return PRS.run_reassessment(input_contract=_contract(**kw))


def _persist(tmp_path, run, *, now=None):
    return PRS.persist_reassessment(result=run["reassessment"],
                                    input_contract=run["input_contract"],
                                    reassessment_dir=tmp_path, now=now)


def _artifact_text(tmp_path, artifact_id) -> str:
    return (Path(tmp_path) / "artifacts" / ("%s.json" % artifact_id)).read_text(
        encoding="utf-8")


# =========================================================================== #
# 1-13. THE VERSIONING CONTRACT
# =========================================================================== #
class TestSameSessionVersioning:
    def test_01_exact_duplicate_of_the_same_evidence_is_idempotent(self, tmp_path):
        first = _persist(tmp_path, _run())
        assert first["status"] == PRS.PERSIST_CREATED
        again = _persist(tmp_path, _run())
        assert again["status"] == PRS.PERSIST_REUSED
        assert again["reused"] is True and again["conflict"] is False
        assert again["artifact_id"] == first["artifact_id"]
        assert again["history_appended"] is False
        assert len(list((Path(tmp_path) / "artifacts").glob("*.json"))) == 1
        assert len(PRS.load_history(reassessment_dir=tmp_path)) == 1

    def test_02_same_economic_state_new_ranking_evidence_appends_a_version(
            self, tmp_path):
        first = _persist(tmp_path, _run())
        second = _persist(tmp_path, _run(sc=_scoring(output_hash="US_B",
                                                     input_contract_hash="UIC_B")))
        assert second["status"] == PRS.PERSIST_ASSESSMENT_VERSION
        assert second["persisted"] is True and second["conflict"] is False
        assert second["assessment_evidence_changed"] is True
        assert second["economic_state_changed"] is False
        assert second["superseded_artifact_id"] == first["artifact_id"]
        assert second["version_index"] == 2

    def test_03_same_economic_state_new_hoc_evidence_appends_a_version(self, tmp_path):
        first = _persist(tmp_path, _run())
        second = _persist(tmp_path, _run(hoc=_hoc(assessment_hash="HOC_B")))
        assert second["status"] == PRS.PERSIST_ASSESSMENT_VERSION
        assert second["assessment_evidence_changed"] is True
        assert second["artifact_id"] != first["artifact_id"]

    def test_04_materially_new_input_freshness_appends_a_version(self, tmp_path):
        """Freshness is bound evidence: a required source moving FRESH -> STALE
        changes what the assessment is entitled to conclude."""
        _persist(tmp_path, _run())
        second = _persist(tmp_path, _run(fr=_freshness(prices_status="STALE")))
        assert second["status"] == PRS.PERSIST_ASSESSMENT_VERSION
        assert second["assessment_evidence_changed"] is True

    def test_05_the_older_version_stays_byte_for_byte_immutable(self, tmp_path):
        first = _persist(tmp_path, _run())
        before = _artifact_text(tmp_path, first["artifact_id"])
        _persist(tmp_path, _run(hoc=_hoc(assessment_hash="HOC_B")))
        _persist(tmp_path, _run(hoc=_hoc(assessment_hash="HOC_C")))
        assert _artifact_text(tmp_path, first["artifact_id"]) == before

    def test_06_an_explicit_id_read_returns_that_exact_version(self, tmp_path):
        first = _persist(tmp_path, _run())
        second = _persist(tmp_path, _run(hoc=_hoc(assessment_hash="HOC_B")))
        older = PRS.load_artifact_by_id(reassessment_id=first["artifact_id"],
                                        active_book_id=BOOK,
                                        eligible_market_date=SESSION,
                                        reassessment_dir=tmp_path)
        assert older["reassessment_id"] == first["artifact_id"]
        assert older["identity"]["hoc_assessment_hash"] == "HOC_A"
        newer = PRS.load_artifact_by_id(reassessment_id=second["artifact_id"],
                                        reassessment_dir=tmp_path)
        assert newer["identity"]["hoc_assessment_hash"] == "HOC_B"

    def test_07_the_latest_read_returns_the_newest_valid_version(self, tmp_path):
        _persist(tmp_path, _run())
        second = _persist(tmp_path, _run(hoc=_hoc(assessment_hash="HOC_B")))
        latest = PRS.load_latest_artifact(active_book_id=BOOK,
                                          eligible_market_date=SESSION,
                                          reassessment_dir=tmp_path)
        assert latest["reassessment_id"] == second["artifact_id"]
        summary = PRS.load_reassessment_summary(active_book_id=BOOK,
                                                eligible_market_date=SESSION,
                                                reassessment_dir=tmp_path)
        assert summary["reassessment_id"] == second["artifact_id"]

    def test_08_a_changed_economic_state_keeps_the_stage21_behaviour(self, tmp_path):
        _persist(tmp_path, _run())
        moved = _persist(tmp_path, _run(
            ps=_pstate(econ_hash="ECON_B", cash=5000.0),
            hoc=_hoc(econ_hash="ECON_B", cash=5000.0)))
        assert moved["status"] == PRS.PERSIST_ECONOMIC_VERSION
        assert moved["economic_state_changed"] is True
        # ...and the economic hint still resolves the CURRENT-state assessment.
        art = PRS.load_latest_artifact(active_book_id=BOOK,
                                       eligible_market_date=SESSION,
                                       reassessment_dir=tmp_path,
                                       economic_state_hash="ECON_A")
        assert art["identity"]["economic_state_hash"] == "ECON_A"

    def test_09_an_internally_inconsistent_identity_fails_closed(self, tmp_path):
        run = _run()
        broken = json.loads(json.dumps(run["reassessment"]))
        broken["eligible_market_date"] = "2026-09-15"
        out = PRS.persist_reassessment(result=broken,
                                       input_contract=run["input_contract"],
                                       reassessment_dir=tmp_path)
        assert out["status"] == PRS.PERSIST_INCONSISTENT
        assert out["persisted"] is False and out["identity_conflicts"]
        assert not list((Path(tmp_path) / "artifacts").glob("*.json"))

    def test_09b_a_book_that_disagrees_with_itself_fails_closed(self, tmp_path):
        run = _run()
        broken = json.loads(json.dumps(run["reassessment"]))
        broken["active_book_id"] = "some_other_book"
        out = PRS.persist_reassessment(result=broken,
                                       input_contract=run["input_contract"],
                                       reassessment_dir=tmp_path)
        assert out["status"] == PRS.PERSIST_INCONSISTENT
        assert out["persisted"] is False

    def test_10_a_duplicate_trigger_creates_no_reassessment_at_all(
            self, tmp_path, monkeypatch):
        calls = []
        out = _cycle_run(tmp_path, monkeypatch, duplicate=True,
                         reassessment_fn=lambda **kw: calls.append(kw))
        assert calls == []
        assert out["state"] == esr.ST_DUPLICATE_TRIGGER_SUPPRESSED
        assert out["reassessment_ran"] is False

    def test_11_non_material_information_creates_no_version(
            self, tmp_path, monkeypatch):
        calls = []
        out = _cycle_run(tmp_path, monkeypatch, material=False,
                         reassessment_fn=lambda **kw: calls.append(kw))
        assert calls == []
        assert out["reassessment_ran"] is False
        assert out["state"] in (esr.ST_INFORMATION_NOT_MATERIAL,
                                esr.ST_NO_NEW_INFORMATION)

    def test_12_identical_refreshed_evidence_creates_no_new_version(self, tmp_path):
        """A poll that re-derives the SAME evidence is not a new assessment, even
        when the whole cycle ran again."""
        _persist(tmp_path, _run())
        for _ in range(4):
            out = _persist(tmp_path, _run())
            assert out["status"] == PRS.PERSIST_REUSED
        assert len(list((Path(tmp_path) / "artifacts").glob("*.json"))) == 1
        assert len(PRS.load_history(reassessment_dir=tmp_path)) == 1

    def test_12b_only_the_provenance_state_hash_moving_is_not_new_evidence(
            self, tmp_path):
        """The document-wide ``portfolio_state_hash`` embeds this owner's OWN
        output, so a downstream research write moves it on every cycle. That is
        provenance drift, not evidence, and it must not manufacture a version."""
        first = _persist(tmp_path, _run())
        drifted = _persist(tmp_path, _run(
            ps=_pstate(ps_hash="PSH_DRIFTED"), hoc=_hoc(ps_hash="PSH_DRIFTED")))
        assert drifted["status"] == PRS.PERSIST_REUSED
        assert drifted["artifact_id"] == first["artifact_id"]
        assert len(list((Path(tmp_path) / "artifacts").glob("*.json"))) == 1

    def test_13_multiple_same_session_versions_order_deterministically(self, tmp_path):
        ids = []
        for h in ("HOC_A", "HOC_B", "HOC_C"):
            ids.append(_persist(tmp_path, _run(hoc=_hoc(assessment_hash=h)))
                       ["artifact_id"])
        chain = PRS.load_artifact_versions(active_book_id=BOOK,
                                           eligible_market_date=SESSION,
                                           reassessment_dir=tmp_path)
        assert [v["artifact_id"] for v in chain] == ids
        assert chain[-1]["artifact_id"] == PRS.load_latest_artifact(
            active_book_id=BOOK, eligible_market_date=SESSION,
            reassessment_dir=tmp_path)["reassessment_id"]
        # Every link names what it superseded, and nothing was rewritten.
        assert chain[1]["supersedes_artifact_id"] == ids[0]
        assert chain[2]["supersedes_artifact_id"] == ids[1]

    def test_13b_the_phase_h_intraday_scenario(self, tmp_path):
        """09:45 / 11:10 / 13:40 — the economic state never moves, the evidence
        does three times, and each conclusion becomes its own immutable version."""
        a1 = _persist(tmp_path, _run(hoc=_hoc(assessment_hash="HOC_0945")))
        a2 = _persist(tmp_path, _run(hoc=_hoc(assessment_hash="HOC_1110"),
                                     sc=_scoring(output_hash="US_1110")))
        a3 = _persist(tmp_path, _run(hoc=_hoc(assessment_hash="HOC_1340"),
                                     sc=_scoring(output_hash="US_1340")))
        assert a1["status"] == PRS.PERSIST_CREATED
        assert a2["status"] == a3["status"] == PRS.PERSIST_ASSESSMENT_VERSION
        assert all(p["economic_state_changed"] is False for p in (a2, a3))
        ids = [a1["artifact_id"], a2["artifact_id"], a3["artifact_id"]]
        assert len(set(ids)) == 3
        for aid in ids:
            assert PRS.load_artifact_by_id(reassessment_id=aid,
                                           reassessment_dir=tmp_path) is not None
        assert PRS.load_latest_artifact(
            active_book_id=BOOK, eligible_market_date=SESSION,
            reassessment_dir=tmp_path)["reassessment_id"] == ids[-1]
        # No portfolio movement was needed for the assessment to advance.
        econ = {PRS.load_artifact_by_id(reassessment_id=i,
                                        reassessment_dir=tmp_path)["identity"]
                ["economic_state_hash"] for i in ids}
        assert econ == {"ECON_A"}


# =========================================================================== #
# 14-17. ONE HISTORY, ONE STORE, ONE WRITER
# =========================================================================== #
class TestOneHistoryOneOwner:
    def test_14_the_daily_cycle_and_the_event_cycle_share_one_history(self, tmp_path):
        """Both canonical producers reach the store through the SAME entry point,
        so their assessments land in one append-only chain."""
        drc = PRS.run_and_persist(portfolio_state=_pstate(), scoring=_scoring(),
                                  hoc_assessment=_hoc(), freshness=_freshness(),
                                  recent_change_history=[],
                                  reassessment_dir=tmp_path)
        live = PRS.run_and_persist(portfolio_state=_pstate(), scoring=_scoring(),
                                   hoc_assessment=_hoc(assessment_hash="HOC_LIVE"),
                                   freshness=_freshness(),
                                   recent_change_history=[],
                                   reassessment_dir=tmp_path)
        assert drc["persistence"]["status"] == PRS.PERSIST_CREATED
        assert live["persistence"]["status"] == PRS.PERSIST_ASSESSMENT_VERSION
        chain = PRS.load_artifact_versions(active_book_id=BOOK,
                                           eligible_market_date=SESSION,
                                           reassessment_dir=tmp_path)
        assert [v["artifact_id"] for v in chain] == [
            drc["persistence"]["artifact_id"], live["persistence"]["artifact_id"]]
        assert len(list(Path(tmp_path).glob("*.json"))) == 2   # index + history

    def test_15_both_producers_delegate_to_the_one_persistence_owner(self):
        for module in ("api/daily_research_cycle.py", "api/event_signal_refresh.py"):
            src = _code_only(module)
            assert "run_and_persist" in src
            assert "def persist_reassessment" not in src

    def test_16_there_is_exactly_one_reassessment_store(self):
        writers = []
        for path in sorted((REPO / "api").glob("*.py")) + \
                sorted((REPO / "engine").glob("*.py")):
            src = path.read_text(encoding="utf-8", errors="replace")
            if "_atomic_write_json(_index_path(reassessment_dir)" in src:
                writers.append(path.name)
        assert writers == ["portfolio_reassessment.py"]

    def test_17_there_is_exactly_one_persistence_writer(self):
        defs = []
        for path in sorted((REPO / "api").glob("*.py")) + \
                sorted((REPO / "engine").glob("*.py")):
            src = path.read_text(encoding="utf-8", errors="replace")
            defs.extend([path.name] * len(re.findall(
                r"^def persist_reassessment\(", src, re.M)))
        assert defs == ["portfolio_reassessment.py"]
        for name in ("assessment_evidence_identity", "assessment_evidence_hash",
                     "decision_fingerprint", "authoritative_history_rows"):
            assert len(re.findall(r"^def %s\(" % name, PRS_SRC, re.M)) == 1

    def test_17b_the_owner_never_deletes_or_rewrites_an_artifact(self):
        src = _code_only("api/portfolio_reassessment.py")
        for forbidden in ("rmtree", "os . remove", "shutil"):
            assert forbidden not in src
        # The ONLY deletion is the atomic-write temp file; no artifact path is
        # ever removed, truncated or replaced.
        assert src.count("unlink") == 1 and "os . unlink ( tmp )" in src
        # the version chain is APPENDED, never replaced
        assert "prior_versions + [ entry ]" in src


# =========================================================================== #
# 18-21. DOWNSTREAM BINDING
# =========================================================================== #
class TestDownstreamBinding:
    def test_18_a_proposal_binds_the_exact_reassessment_version(self, tmp_path):
        run_v2 = _run(hoc=_hoc(assessment_hash="HOC_B"))
        _persist(tmp_path, _run())
        p2 = _persist(tmp_path, run_v2)
        art = PRS.load_artifact_by_id(reassessment_id=p2["artifact_id"],
                                      reassessment_dir=tmp_path)
        binding = PRS.proposal_binding(reassessment=run_v2["reassessment"],
                                       artifact=art,
                                       input_contract=run_v2["input_contract"])
        assert binding["reassessment_id"] == p2["artifact_id"]
        assert binding["reassessment_hash"] == \
            run_v2["reassessment"]["reassessment_hash"]
        assert binding["hoc_assessment_hash"] == "HOC_B"

    def test_19_a_proposal_built_for_an_older_version_is_not_current(self, tmp_path):
        run_v1 = _run()
        _persist(tmp_path, run_v1)
        run_v2 = _run(hoc=_hoc(assessment_hash="HOC_B"))
        _persist(tmp_path, run_v2)
        stale_proposal = {"identity": {
            "hoc_assessment_hash": "HOC_A",
            "portfolio_state_hash": run_v1["input_contract"]["portfolio_state_hash"],
            "universe_scoring_hash": "US_A",
            "corporate_actions_hash": None,
            "eligible_market_date": SESSION}}
        verdict = PRS.proposal_is_current_for(
            reassessment=run_v2["reassessment"], proposal_artifact=stale_proposal)
        assert verdict["reusable"] is False

    def test_20_an_obsolete_assessment_cannot_silently_become_authoritative(
            self, tmp_path):
        """Version 2 is governed at 19:05; a candidate still carrying version 1
        from 17:42 is refused rather than quietly replacing it."""
        _, _, first = _record(tmp_path, reas=_gov_reas(ra_hash="RA2"),
                              cycle=_gov_cycle(reassessment_hash="RA2"), now=T2)
        assert first["recorded"] is True
        cand, gate = _gate(reas=_gov_reas(ra_hash="RA1", rid="ra_v1"),
                           cycle=_gov_cycle(reassessment_hash="RA1",
                                            reassessment_id="ra_v1"),
                           current_governed=first["record"], now=T1)
        assert gate["verdict"] == pdec.GATE_WITHHELD
        assert pdec.WR_SUPERSEDED in set(gate["withheld_reason_codes"])
        out = pdec.record_governed_decision(
            candidate=cand, gate=gate,
            confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN,
            decision_dir=tmp_path, now=T1)
        assert out["recorded"] is False
        rows = json.loads((tmp_path / "governed_decisions.json").read_text("utf-8"))
        assert len(rows) == 1
        assert rows[0]["identity"]["reassessment_id"] == "ra_2026-08-31_2"

    def test_21_an_in_flight_execution_still_outranks_a_new_version(self, tmp_path):
        reas = _gov_reas()
        reas["execution_precedence"] = {"execution_active": True,
                                        "reason": "order plan awaiting settlement"}
        _, gate = _gate(reas=reas)
        assert gate["verdict"] == pdec.GATE_WITHHELD
        assert pdec.WR_EXECUTION_PRECEDENCE in set(gate["withheld_reason_codes"])


# =========================================================================== #
# 22-29. POINT-IN-TIME AND SAFETY
# =========================================================================== #
class TestPointInTimeAndSafety:
    def test_22_persisting_a_version_never_advances_the_operational_mark(
            self, tmp_path):
        src = _code_only("api/portfolio_reassessment.py")
        for forbidden in ("daily_close", "advance_mark", "operational_book",
                          "desk_mark_date ="):
            assert forbidden not in src
        _persist(tmp_path, _run())
        _persist(tmp_path, _run(hoc=_hoc(assessment_hash="HOC_B")))
        # Only the reassessment store was touched.
        assert sorted(p.name for p in Path(tmp_path).iterdir()) == [
            "artifacts", "index.json", "recommendation_history.json"]

    def test_23_to_29_the_owner_creates_no_capital_action(self):
        src = _code_only("api/portfolio_reassessment.py")
        for forbidden in ("create_order", "place_order", "submit_order",
                          "create_fill", "record_fill", "confirm_order_plan",
                          "promote_model", "approve_proposal", "approve_decision",
                          "activate_sleeve", "recalibrate"):
            assert forbidden not in src, forbidden
        # The ONLY execution touchpoint is a READ of the rebalance state, which
        # is how an in-flight execution keeps operator precedence.
        assert "rb . load_rebalance_state ( )" in src

    def test_29b_the_owner_declares_its_safety_posture(self, tmp_path):
        out = _persist(tmp_path, _run())
        art = PRS.load_artifact_by_id(reassessment_id=out["artifact_id"],
                                      reassessment_dir=tmp_path)
        safety = (art.get("reassessment") or {}).get("safety") or {}
        assert safety.get("created_orders") is False
        assert safety.get("created_fills") is False
        assert safety.get("created_order_plan") is False
        assert safety.get("approved_proposal") is False
        assert safety.get("broker_execution") is False
        assert safety.get("manual_review") is True

    def test_29c_intraday_evidence_never_impersonates_an_official_close(self):
        """The declared inputs carry their OWN as-of dates; nothing is back-dated
        to the eligible session to make an assessment look complete."""
        ic = _contract(fr=_freshness(prices_status="STALE"))
        rows = {r["source_id"]: r for r in ic["inputs"]}
        assert rows["owned_daily_prices"]["state"] == K.STALE_BUT_VALID
        assert rows["owned_daily_prices"]["usage"] == K.USAGE_STALE
        assert ic["inputs_as_of_eligible_date"] == SESSION
        fp_fresh = PRS.declared_inputs_fingerprint(_contract())
        assert PRS.declared_inputs_fingerprint(ic) != fp_fresh


# =========================================================================== #
# 30-38. R54.1 GOVERNANCE INTEGRATION
# =========================================================================== #
T1 = datetime(2026, 9, 1, 17, 42, 0, tzinfo=timezone.utc)
T2 = datetime(2026, 9, 1, 19, 5, 0, tzinfo=timezone.utc)


def _gov_ps(**kw) -> dict:
    d = {"state": "PORTFOLIO_STATE_READY",
         "active_book": {"book_id": BOOK, "book_label": "Alpha Paper Book #1"},
         "dates": {"eligible_market_date": SESSION, "desk_mark_date": SESSION,
                   "valuation_date": SESSION, "latest_daily_close_date": SESSION},
         "capital": {"nav": 99113.22, "cash": 1200.5},
         "positions": [{"ticker": t} for t in HELD],
         "state_hash": "PSH1", "economic_state_hash": "ESH1"}
    d.update(kw)
    return d


def _gov_cycle(**kw) -> dict:
    d = {"run_id": "evt_bbbb2222", "state": esr.ST_PROPOSAL_AVAILABLE,
         "generated_at": "2026-09-01T17:40:00+00:00",
         "completed_at": "2026-09-01T17:42:07+00:00",
         "reassessment_ran": True, "proposal_built": True,
         "materiality_change_level": "MATERIAL_SIGNAL_CHANGED",
         "active_book_id": BOOK, "eligible_market_date": SESSION,
         "portfolio_state_hash": "ESH1", "holdings": list(HELD),
         "hoc_assessment_hash": "HOC1", "hoc_holdings_reviewed": 25,
         "reassessment_hash": "RA2", "proposal_hash": "PR1",
         # R54.2 — the immutable artifact this run's conclusion became.
         "reassessment_id": "ra_2026-08-31_2",
         "reassessment_persistence_status": PRS.PERSIST_ASSESSMENT_VERSION,
         "reassessment_persisted": True,
         "assessment_evidence_changed": True,
         "supersedes_reassessment_id": "ra_2026-08-31_1",
         "materiality_trigger_fingerprint": "FP2",
         "duplicate_of_prior_trigger": False, "blocker_codes": [],
         "reassessment_state": "PROPOSAL_READY", "proposal_state": "READY",
         "stage_timestamps": {
             "signal_refresh_completed_at": "2026-09-01T17:40:30+00:00",
             "scoring_completed_at": "2026-09-01T17:40:45+00:00",
             "hoc_completed_at": "2026-09-01T17:41:10+00:00",
             "reassessment_completed_at": "2026-09-01T17:41:40+00:00",
             "target_completed_at": "2026-09-01T17:42:05+00:00"},
         "cycle_duration_seconds": 7.3,
         "oldest_event_to_reassessment_seconds": 240.0}
    d.update(kw)
    return d


def _gov_reas(*, ra_hash="RA2", rid="ra_2026-08-31_2", **kw) -> dict:
    d = {"state": "PROPOSAL_READY", "eligible_market_date": SESSION,
         "active_book": {"book_id": BOOK},
         "reassessment_id": rid, "reassessment_hash": ra_hash,
         "artifact": {"reassessment_id": rid,
                      "generated_at": "2026-09-01T17:41:40+00:00",
                      "identity": {"economic_state_hash": "ESH1"}},
         "proposal_binding": {
             "reassessment_id": rid, "reassessment_hash": ra_hash,
             "hoc_assessment_hash": "HOC1", "universe_scoring_hash": "US1",
             "universe_input_contract_hash": "UIC1",
             "portfolio_state_hash": "PSH1", "corporate_actions_hash": "CA1",
             "eligible_market_date": SESSION, "active_book_id": BOOK},
         "execution_precedence": {"execution_active": False,
                                  "reason": "no rebalance in flight"},
         "decision": {"holdings_evaluated": 25}}
    d.update(kw)
    return d


def _gov_summ(**kw) -> dict:
    d = {"reallocation_proposal_available": True,
         "reallocation_proposal_stale": False,
         "reallocation_proposal_stale_reason": None,
         "reallocation_corporate_actions_hash": "CA1",
         "reallocation_proposal_hash": "PR1", "reallocation_proposal_id": "prop_1",
         "reallocation_proposal_withheld": False,
         "reallocation_withheld_reasons": [],
         "reallocation_outcome": cr.OUTCOME_HOLD_CURRENT_BOOK,
         "reallocation_feasible_target_exists": True,
         "reallocation_data_gaps": [],
         "reallocation_bound_hoc_assessment_hash": "HOC1",
         "reallocation_bound_eligible_market_date": SESSION,
         "reallocation_bound_active_book_id": BOOK}
    d.update(kw)
    return d


def _gov_con(outcome: str = cr.OUTCOME_HOLD_CURRENT_BOOK, **kw) -> dict:
    d = {"outcome": outcome, "outcome_vocabulary": list(cr.OUTCOME_VOCAB),
         "feasible_target_exists": True, "calculation_owner": cr.CALCULATION_OWNER,
         "switching_economics": {
             "switching_hurdle": 0.02,
             "clears_switching_hurdle": (outcome == cr.OUTCOME_PROPOSAL_READY),
             "score_improvement_net_of_cost": 0.004, "one_way_turnover": 0.11,
             "estimated_transaction_cost": 214.5,
             "concentration_before": 0.0412, "concentration_after": 0.0430,
             "portfolio_volatility_before": 0.1731,
             "portfolio_volatility_after": 0.1755},
         "ideal_target": {"zero_base_owner": "api.zero_base_target"},
         "constraint_inventory": {"constraints": [{"code": "TURNOVER"}]},
         "multi_asset": {"current_holdings_privileged": False},
         "best_feasible_target": {"allocations": [
             {"ticker": "T00", "action": "REDUCE", "current_weight": 0.06,
              "proposed_weight": 0.03, "delta_weight": -0.03,
              "capital_change": -2900.0},
             {"ticker": "NEW1", "action": "ADD", "current_weight": 0.0,
              "proposed_weight": 0.03, "delta_weight": 0.03,
              "capital_change": 2900.0}]},
         "approval": {"portfolio_decision_state": "HOLD_CURRENT_BOOK",
                      "requires_manual_review": False}}
    d.update(kw)
    return d


def _gov_wf(**kw) -> dict:
    d = {"overall_state": "WAITING_FOR_OWNED_DATA",
         "operational_state": {
             "active_book_id": BOOK, "eligible_market_date": SESSION,
             "desk_mark_date": SESSION, "valuation_date": SESSION,
             "latest_completed_close_date": SESSION,
             "latest_close_status": "COMPLETE", "operational_close_valid": True,
             "eligible_session_already_processed": True, "pending_orders": 0},
         "research_cycle_state": {
             "opportunity_cost_artifact_class": "GOVERNED_DRC_TERMINAL",
             "opportunity_cost_producer_owner": "api.daily_research_cycle",
             "governed_research_evidence_current": True,
             "governed_manifest_run_id": "drc_2026_08_31"},
         "portfolio_decision_state": {"proposal_hash": "PR1"},
         "blockers": [{"code": "OWNED_DATA_NOT_CONFIRMED",
                       "detail": "expected session 2026-09-01 unconfirmed"}]}
    d.update(kw)
    return d


def _gov_sc(**kw) -> dict:
    d = {"ranking_date": SESSION, "input_contract_hash": "UIC1",
         "status": "UNIVERSE_SCORING_READY"}
    d.update(kw)
    return d


def _gate(*, ps=None, cycle=None, reas=None, summ=None, con=None, wf=None,
          sc=None, current_governed=None, now=T1):
    ps = ps if ps is not None else _gov_ps()
    cycle = cycle if cycle is not None else _gov_cycle()
    reas = reas if reas is not None else _gov_reas()
    summ = summ if summ is not None else _gov_summ()
    con = con if con is not None else _gov_con()
    wf = wf if wf is not None else _gov_wf()
    sc = sc if sc is not None else _gov_sc()
    cand = pdec.build_intraday_candidate(
        portfolio_state=ps, event_cycle=cycle, reassessment=reas,
        proposal_summary=summ, constrained=con, workflow=wf,
        scoring_identity=sc, now=now)
    return cand, pdec.evaluate_intraday_governance(
        candidate=cand, portfolio_state=ps, event_cycle=cycle, reassessment=reas,
        proposal_summary=summ, constrained=con, workflow=wf, scoring_identity=sc,
        current_governed=current_governed)


def _record(tmp_path, *, now=T1, **kw):
    cand, gate = _gate(now=now, **kw)
    return cand, gate, pdec.record_governed_decision(
        candidate=cand, gate=gate,
        confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN,
        decision_dir=tmp_path, now=now)


class TestGovernanceIntegration:
    def test_30_the_gate_is_tightened_not_relaxed(self):
        """R54.2 adds a persistence requirement INSIDE the existing check; it
        removes no comparison and adds no exemption."""
        _, gate = _gate(cycle=_gov_cycle(reassessment_persisted=False,
                                         reassessment_persistence_status=
                                         PRS.PERSIST_CONFLICT,
                                         reassessment_id=None))
        assert gate["verdict"] == pdec.GATE_WITHHELD
        assert pdec.WR_REASSESSMENT_IDENTITY in set(gate["withheld_reason_codes"])
        assert "CYCLE_REASSESSMENT_IS_THE_CANDIDATE" in gate["failing_checks"]
        # ...and the hash comparison it already made is still enforced.
        _, mismatch = _gate(cycle=_gov_cycle(reassessment_hash="RA_OTHER"))
        assert "CYCLE_REASSESSMENT_IS_THE_CANDIDATE" in mismatch["failing_checks"]

    def test_30b_an_inconsistent_identity_refusal_also_withholds(self):
        _, gate = _gate(cycle=_gov_cycle(
            reassessment_persisted=False,
            reassessment_persistence_status=PRS.PERSIST_INCONSISTENT,
            reassessment_id=None))
        assert gate["verdict"] == pdec.GATE_WITHHELD

    def test_31_a_valid_same_session_version_is_the_candidate(self):
        cand, gate = _gate()
        passed = {c["check"]: c for c in gate["checks"]}
        chk = passed["CYCLE_REASSESSMENT_IS_THE_CANDIDATE"]
        assert chk["passed"] is True
        assert PRS.PERSIST_ASSESSMENT_VERSION in chk["detail"]
        assert cand["identity"]["reassessment_hash"] == "RA2"

    def test_32_a_hermetic_valid_candidate_reaches_38_of_38(self):
        _, gate = _gate()
        assert gate["checks_total"] == 38
        assert gate["checks_passed"] == 38, gate["failing_checks"]
        assert gate["verdict"] == pdec.GATE_ELIGIBLE
        assert gate["withheld_reason_codes"] == []

    def test_32b_the_real_persisted_version_flows_end_to_end_into_the_gate(
            self, tmp_path):
        """From the REAL persistence owner to 38/38. Version 1 is the daily
        cycle's; version 2 is the intraday one; every identity the gate checks
        comes from the owner's own ``proposal_binding`` for that exact artifact
        — nothing about the chain is asserted from a literal."""
        ps = _pstate(ca_fp="CA1")
        v1 = _persist(tmp_path, _run(ps=ps, hoc=_hoc(ca_hash="CA1")))
        run2 = _run(ps=ps, hoc=_hoc(ca_hash="CA1", assessment_hash="HOC_B"))
        v2 = _persist(tmp_path, run2)
        assert v1["status"] == PRS.PERSIST_CREATED
        assert v2["status"] == PRS.PERSIST_ASSESSMENT_VERSION

        art = PRS.load_artifact_by_id(reassessment_id=v2["artifact_id"],
                                      reassessment_dir=tmp_path)
        binding = PRS.proposal_binding(reassessment=run2["reassessment"],
                                       artifact=art,
                                       input_contract=run2["input_contract"])
        assert binding["reassessment_id"] == v2["artifact_id"]

        reas = _gov_reas(ra_hash=binding["reassessment_hash"],
                         rid=v2["artifact_id"])
        reas["proposal_binding"] = binding
        cycle = _gov_cycle(
            reassessment_hash=binding["reassessment_hash"],
            reassessment_id=v2["artifact_id"],
            supersedes_reassessment_id=v1["artifact_id"],
            hoc_assessment_hash=binding["hoc_assessment_hash"])
        summ = _gov_summ(
            reallocation_bound_hoc_assessment_hash=binding["hoc_assessment_hash"],
            reallocation_corporate_actions_hash=binding["corporate_actions_hash"])
        sc = _gov_sc(
            input_contract_hash=binding["universe_input_contract_hash"])

        cand, gate = _gate(reas=reas, cycle=cycle, summ=summ, sc=sc)
        assert gate["checks_passed"] == gate["checks_total"] == 38, \
            gate["failing_checks"]
        assert gate["verdict"] == pdec.GATE_ELIGIBLE
        assert cand["identity"]["reassessment_id"] == v2["artifact_id"]
        # ...and version 1 is still exactly where it was.
        assert PRS.load_artifact_by_id(reassessment_id=v1["artifact_id"],
                                       reassessment_dir=tmp_path) is not None

    def test_33_a_governed_hold_may_result(self, tmp_path):
        cand, gate, out = _record(tmp_path)
        assert cand["decision"] == pdec.GD_HOLD_CURRENT_BOOK
        assert out["recorded"] is True
        assert out["record"]["provenance"] == pdec.PROV_GOVERNED_INTRADAY
        assert out["record"]["identity"]["reassessment_id"] == "ra_2026-08-31_2"
        # A governed HOLD proposes nothing, so it asks for no manual review —
        # and it still approves and executes nothing.
        assert out["record"]["manual_review_required"] is False
        assert out["record"]["position_recommendations"] == []
        assert out["record"]["safety"]["approved_anything"] is False

    def test_34_a_governed_change_may_result(self, tmp_path):
        cand, gate, out = _record(
            tmp_path, con=_gov_con(cr.OUTCOME_PROPOSAL_READY),
            summ=_gov_summ(reallocation_outcome=cr.OUTCOME_PROPOSAL_READY))
        assert gate["verdict"] == pdec.GATE_ELIGIBLE, gate["withheld_reasons"]
        assert cand["decision"] == pdec.GD_CHANGE_RECOMMENDED
        assert out["recorded"] is True
        rec = out["record"]
        assert rec["position_recommendations"]
        # ...and it approves and executes NOTHING.
        assert rec["manual_review_required"] is True
        assert rec["approval_required_token"] == pdec.CONFIRM_TOKEN
        assert rec["safety"]["created_orders"] is False
        assert rec["safety"]["approved_anything"] is False
        assert rec["safety"]["automatic_approval_allowed"] is False

    def test_35_an_older_governed_decision_stays_immutable(self, tmp_path):
        _, _, first = _record(tmp_path)
        before = (tmp_path / "governed_decisions.json").read_text("utf-8")
        _, _, second = _record(
            tmp_path, cycle=_gov_cycle(reassessment_hash="RA3",
                                       reassessment_id="ra_2026-08-31_3",
                                       materiality_trigger_fingerprint="FP3"),
            reas=_gov_reas(ra_hash="RA3", rid="ra_2026-08-31_3"),
            current_governed=first["record"], now=T2)
        assert second["recorded"] is True
        rows = json.loads((tmp_path / "governed_decisions.json").read_text("utf-8"))
        assert len(rows) == 2
        assert json.dumps(rows[0], sort_keys=True) in before or \
            rows[0]["record_id"] == first["record"]["record_id"]
        assert rows[0] == json.loads(before)[0]

    def test_36_a_newer_version_supersedes_correctly(self, tmp_path):
        _, _, first = _record(tmp_path)
        _, _, second = _record(
            tmp_path, cycle=_gov_cycle(reassessment_hash="RA3",
                                       reassessment_id="ra_2026-08-31_3",
                                       materiality_trigger_fingerprint="FP3"),
            reas=_gov_reas(ra_hash="RA3", rid="ra_2026-08-31_3"),
            current_governed=first["record"], now=T2)
        assert second["record"]["supersedes_decision_id"] == \
            first["record"]["record_id"]
        assert pdec.governed_decision_ordering_key(second["record"]) > \
            pdec.governed_decision_ordering_key(first["record"])
        latest = pdec.load_governed_decision_record(active_book_id=BOOK,
                                                    decision_dir=tmp_path)
        assert latest["record_id"] == second["record"]["record_id"]

    def test_37_owned_data_not_confirmed_remains_protected(self):
        """The BOOK's own session must be owned-confirmed; the NEXT expected
        session's blocker is recorded, never consumed and never cleared."""
        cand, gate = _gate()
        ev = cand["evidence"]
        assert ev["expected_session_owned_data_confirmed"] is False
        assert gate["verdict"] == pdec.GATE_ELIGIBLE
        wf = _gov_wf()
        wf["operational_state"] = dict(wf["operational_state"],
                                       latest_completed_close_date=PREV,
                                       desk_mark_date=PREV)
        _, blocked = _gate(wf=wf)
        assert blocked["verdict"] == pdec.GATE_WITHHELD
        assert pdec.WR_OWNED_DATA_NOT_CONFIRMED in set(
            blocked["withheld_reason_codes"])

    def test_38_a_governed_version_never_advances_the_close(self, tmp_path):
        _, _, out = _record(tmp_path)
        rec = out["record"]
        assert rec["evidence_provenance"]["operational_mark_source"] == \
            "api.workflow_state.operational_state"
        assert rec["safety"]["advances_operational_mark"] is False
        assert rec["safety"]["ran_daily_close"] is False
        assert sorted(p.name for p in tmp_path.iterdir()) == [
            "governed_decisions.json", "governed_index.json"]


# =========================================================================== #
# Versioning must not create evidence noise or self-blocking churn.
# =========================================================================== #
class TestNoEvidenceNoise:
    def test_39_a_session_votes_once_in_the_churn_history(self, tmp_path):
        rows = [
            {"reassessment_id": "v1", "active_book_id": BOOK,
             "eligible_market_date": PREV, "recorded_at": "2026-08-28T20:00:00Z",
             "recommendations": [{"ticker": "AAA",
                                  "recommendation": K.REC_EXIT}]},
            {"reassessment_id": "v2", "active_book_id": BOOK,
             "eligible_market_date": PREV, "recorded_at": "2026-08-28T21:00:00Z",
             "recommendations": [{"ticker": "AAA",
                                  "recommendation": K.REC_HOLD}]},
        ]
        auth = PRS.authoritative_history_rows(rows)
        assert [r["reassessment_id"] for r in auth] == ["v2"]

    def test_40_a_version_cannot_churn_protect_a_name_against_its_successor(
            self, tmp_path):
        """A reassessment has never seen its own recommendation. Letting version 2
        read version 1's row would make the cooldown self-blocking."""
        (Path(tmp_path) / "artifacts").mkdir(parents=True, exist_ok=True)
        (Path(tmp_path) / "recommendation_history.json").write_text(json.dumps([
            {"reassessment_id": "prior", "active_book_id": BOOK,
             "eligible_market_date": PREV, "recorded_at": "2026-08-28T20:00:00Z",
             "recommendations": [{"ticker": "BBB", "recommendation": K.REC_EXIT}]},
            {"reassessment_id": "today_v1", "active_book_id": BOOK,
             "eligible_market_date": SESSION,
             "recorded_at": "2026-08-31T14:00:00Z",
             "recommendations": [{"ticker": "AAA", "recommendation": K.REC_EXIT}]},
        ]), encoding="utf-8")
        pol = PRS.resolve_policy()
        rows = PRS.recent_change_rows(reassessment_dir=tmp_path,
                                      active_book_id=BOOK, policy=pol,
                                      exclude_eligible_market_date=SESSION)
        assert {r["ticker"] for r in rows} == {"BBB"}
        # ...while a genuinely prior session still protects its name.
        all_rows = PRS.recent_change_rows(reassessment_dir=tmp_path,
                                          active_book_id=BOOK, policy=pol)
        assert {r["ticker"] for r in all_rows} == {"AAA", "BBB"}

    def test_41_attribution_counts_a_session_once(self):
        rows = [
            {"reassessment_id": "v1", "active_book_id": BOOK,
             "eligible_market_date": SESSION, "recorded_at": "2026-08-31T14:00:00Z",
             "decision": K.STATE_PROPOSAL_READY,
             "recommendations": [{"ticker": "AAA", "recommendation": K.REC_EXIT,
                                  "current_weight": 0.04}]},
            {"reassessment_id": "v2", "active_book_id": BOOK,
             "eligible_market_date": SESSION, "recorded_at": "2026-08-31T18:00:00Z",
             "decision": K.STATE_PROPOSAL_READY,
             "recommendations": [{"ticker": "AAA", "recommendation": K.REC_EXIT,
                                  "current_weight": 0.04}]},
        ]
        att = PRS.build_attribution(history=rows, price_panel={"series": {}},
                                    as_of="2026-09-01")
        assert len(att["rows"]) == 1
        assert att["rows"][0]["reassessment_id"] == "v2"

    def test_42_outcome_observations_count_a_session_once(self):
        rows = [
            {"reassessment_id": "v1", "active_book_id": BOOK,
             "eligible_market_date": SESSION, "recorded_at": "2026-08-31T14:00:00Z",
             "recommendations": [{"ticker": "AAA", "recommendation": K.REC_EXIT}]},
            {"reassessment_id": "v2", "active_book_id": BOOK,
             "eligible_market_date": SESSION, "recorded_at": "2026-08-31T18:00:00Z",
             "recommendations": [{"ticker": "AAA", "recommendation": K.REC_EXIT}]},
        ]
        assert len(PRS.authoritative_history_rows(rows)) == 1
        src = _code_only("api/reassessment_outcomes.py")
        assert "authoritative_history_rows" in src

    def test_43_the_history_read_stays_the_full_append_only_record(self, tmp_path):
        _persist(tmp_path, _run())
        _persist(tmp_path, _run(hoc=_hoc(assessment_hash="HOC_B")))
        read = PRS.load_reassessment_history(active_book_id=BOOK,
                                             reassessment_dir=tmp_path)
        assert read["row_count"] == 2
        assert read["authoritative_row_count"] == 1
        assert read["superseded_row_count"] == 1
        assert read["append_only"] is True and read["backfilled"] is False
        assert len(read["authoritative_reassessment_ids"]) == 1


# =========================================================================== #
# The evidence identity itself.
# =========================================================================== #
class TestAssessmentEvidenceIdentity:
    def test_44_the_identity_excludes_the_contaminated_hashes(self):
        for forbidden in ("portfolio_state_hash", "economic_state_hash",
                          "reassessment_hash"):
            assert forbidden not in PRS.ASSESSMENT_EVIDENCE_COMPONENTS

    def test_45_the_identity_is_deterministic_and_evidence_only(self):
        a = _contract()
        b = _contract(ps=_pstate(ps_hash="DRIFT"), hoc=_hoc(ps_hash="DRIFT"))
        ia = PRS.assessment_evidence_identity(input_contract=a)
        ib = PRS.assessment_evidence_identity(input_contract=b)
        assert ia == ib
        assert PRS.assessment_evidence_hash(ia) == PRS.assessment_evidence_hash(ib)
        c = _contract(hoc=_hoc(assessment_hash="HOC_B"))
        assert PRS.assessment_evidence_hash(
            PRS.assessment_evidence_identity(input_contract=c)) != \
            PRS.assessment_evidence_hash(ia)

    def test_46_a_historical_artifact_stays_comparable_without_rewriting(
            self, tmp_path):
        """An index entry written before R54.2 carries neither new field; both are
        RECOMPUTED from what the artifact already persisted."""
        first = _persist(tmp_path, _run())
        index = json.loads((tmp_path / "index.json").read_text(encoding="utf-8"))
        key = "%s|%s" % (BOOK, SESSION)
        legacy = {k: v for k, v in index[key].items()
                  if k not in ("assessment_evidence_hash", "decision_fingerprint",
                               "versions", "supersedes_artifact_id")}
        index[key] = legacy
        (tmp_path / "index.json").write_text(json.dumps(index), encoding="utf-8")
        ev, fp = PRS._existing_assessment_identity(legacy, tmp_path)
        assert ev and fp
        # a re-run of the SAME evidence is still idempotent against it...
        assert _persist(tmp_path, _run())["status"] == PRS.PERSIST_REUSED
        # ...and new evidence still appends.
        out = _persist(tmp_path, _run(hoc=_hoc(assessment_hash="HOC_B")))
        assert out["status"] == PRS.PERSIST_ASSESSMENT_VERSION
        assert out["superseded_artifact_id"] == first["artifact_id"]

    def test_47_the_decision_fingerprint_ignores_provenance_only(self):
        run = _run()
        res = json.loads(json.dumps(run["reassessment"]))
        res["provenance"] = dict(res.get("provenance") or {},
                                 portfolio_state_hash="SOMETHING_ELSE")
        assert PRS.decision_fingerprint(res) == \
            PRS.decision_fingerprint(run["reassessment"])
        res2 = json.loads(json.dumps(run["reassessment"]))
        res2["decision"] = dict(res2["decision"], expected_net_improvement=0.99)
        assert PRS.decision_fingerprint(res2) != \
            PRS.decision_fingerprint(run["reassessment"])

    def test_48_a_colliding_artifact_id_never_overwrites_a_prior_version(
            self, tmp_path):
        first = _persist(tmp_path, _run())
        before = _artifact_text(tmp_path, first["artifact_id"])
        forced = _run(hoc=_hoc(assessment_hash="HOC_B"))
        # Force the id to collide while the identity genuinely differs.
        forced["reassessment"]["reassessment_hash"] = \
            _run()["reassessment"]["reassessment_hash"]
        out = _persist(tmp_path, forced)
        assert out["persisted"] is True
        assert out["artifact_id"] != first["artifact_id"]
        assert _artifact_text(tmp_path, first["artifact_id"]) == before


# =========================================================================== #
# Active Manager State keeps reporting the live lane truthfully.
# =========================================================================== #
class TestActiveManagerSurface:
    def test_49_the_live_lane_reports_the_persisted_artifact(self):
        d = ams.build_active_manager_state(
            workflow=_gov_wf(),
            event_refresh={"last_run_summary": _gov_cycle()},
            reassessment=_gov_reas(), constrained=_gov_con(),
            portfolio_state=_gov_ps())
        live = d["latest_live_intraday_assessment"]
        assert live["reassessment_hash"] == "RA2"
        assert live["reassessment_id"] == "ra_2026-08-31_2"
        assert live["reassessment_persisted"] is True
        assert live["reassessment_persistence_status"] == \
            PRS.PERSIST_ASSESSMENT_VERSION
        # ...and it is STILL not the authoritative decision.
        assert live["is_authoritative_decision"] is False
        assert live["advances_governed_decision"] is False
        assert d["owner"] == ams.OWNER

    def test_50_the_cycle_summary_publishes_the_persistence_outcome(self):
        summary = esr.build_last_run_summary({
            "run_id": "evt_x", "state": esr.ST_PROPOSAL_AVAILABLE,
            "active_book_id": BOOK, "eligible_market_date": SESSION,
            "reassessment_ran": True,
            "portfolio_reassessment": {
                "reassessment_hash": "RA2", "reassessment_state": "PROPOSAL_READY",
                "reassessment_id": "ra_2026-08-31_2",
                "persistence_status": PRS.PERSIST_ASSESSMENT_VERSION,
                "persisted": True, "assessment_evidence_changed": True,
                "supersedes_reassessment_id": "ra_2026-08-31_1"},
        })
        assert summary["reassessment_id"] == "ra_2026-08-31_2"
        assert summary["reassessment_persisted"] is True
        assert summary["reassessment_persistence_status"] == \
            PRS.PERSIST_ASSESSMENT_VERSION
        assert summary["supersedes_reassessment_id"] == "ra_2026-08-31_1"

    def test_51_a_refused_write_is_reported_as_refused(self):
        summary = esr.build_last_run_summary({
            "run_id": "evt_y", "reassessment_ran": True,
            "portfolio_reassessment": {
                "reassessment_hash": "RA_LIVE",
                "persistence_status": PRS.PERSIST_CONFLICT,
                "persisted": False, "reassessment_id": None},
        })
        assert summary["reassessment_persisted"] is False
        assert summary["reassessment_id"] is None


# =========================================================================== #
# The build itself refuses a second store, a second writer or an overwrite.
# =========================================================================== #
class TestArchitectureAudit:
    @pytest.fixture(scope="class")
    def audit(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "audit_architecture", REPO / "scripts" / "audit_architecture.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def test_52_the_r542_invariants_are_declared_and_blocking(self, audit):
        declared = {(g, k) for g, k, _ in audit.BLOCKING_INVARIANTS}
        group = "release54_2_same_session_reassessment_versioning"
        for key in ("single_index_writer", "duplicate_versioning_owners",
                    "version_chain_is_appended", "owner_deletes_an_artifact",
                    "forbidden_evidence_components",
                    "parallel_reassessment_stores",
                    "inconsistent_identity_guard_present",
                    "gate_requires_persisted_reassessment"):
            assert (group, key) in declared, key

    def test_53_the_repository_satisfies_them(self, audit):
        rep = audit.run_audit()
        res = rep["release54_2_same_session_reassessment_versioning"]
        assert res["single_index_writer"] is True
        assert res["index_writers"] == ["api/portfolio_reassessment.py"]
        assert res["duplicate_versioning_owners"] == []
        assert res["parallel_reassessment_stores"] == []
        assert res["forbidden_evidence_components"] == []
        assert res["owner_deletes_an_artifact"] is False
        assert res["persist_outcomes_missing"] == []
        assert audit._blocking_invariant_failures(rep) == []


# =========================================================================== #
# Hermetic event-cycle harness (materiality injected through its OWN owner).
# =========================================================================== #
def _cycle_run(tmp_path, monkeypatch, *, reassessment_fn, material=True,
               duplicate=False):
    verdict = {
        "change_level": "MATERIAL_SIGNAL_CHANGED" if material else "NONE",
        "reassessment_required": bool(material) and not duplicate,
        "reassessment_reason": "test injection",
        "duplicate_of_prior_trigger": bool(duplicate),
        "data_changed": bool(material),
        "trigger_count": 1 if material else 0,
        "trigger_fingerprint": "FP1", "affected_entities": [],
    }
    monkeypatch.setattr(esr.emat, "assess_materiality", lambda **kw: dict(verdict))
    return esr.run_event_signal_refresh(
        confirm=esr.EXECUTE_CONFIRM_TOKEN,
        fabric_dir=tmp_path / "fabric",
        portfolio_state=_gov_ps(), scoring={"rankings": []}, price_panel=None,
        corpus_events=[],
        hoc_fn=lambda **kw: {"assessment": {"assessment_hash": "HOC1",
                                            "assessment_state": "READY",
                                            "holding_reviews": []}},
        reassessment_fn=reassessment_fn,
        proposal_fn=lambda **kw: {"proposal": {"proposal_hash": "PR1",
                                               "proposal_state": "READY"}},
        proposal_gate_fn=lambda r: {"build_proposal": False},
        governance_fn=lambda **kw: None,
        prior_ranking=None,
        decision_dir=tmp_path / "decisions")
