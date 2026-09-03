"""R54.3 — SAME-SESSION HOC EVIDENCE VERSIONING & RETRIEVABLE GOVERNANCE BINDING.

What these tests prove:

  * the Holding Opportunity-Cost store separates THREE identities — the ECONOMIC
    portfolio, the ASSESSMENT EVIDENCE about it, and the CONCLUSION — so more than
    one legitimate assessment of an unchanged portfolio can coexist immutably in
    one session, exactly as R54.2 already allows for the portfolio reassessment;
  * every same-session version is APPENDED, never overwritten: v1 stays byte-
    identical, stays exactly retrievable by its own id, and the version chain is
    the audit surface;
  * the evidence identity is derived ONLY from inputs the kernel demonstrably
    consumes, and is provably free of wall clock, run id, event-cycle id and
    persistence timestamp;
  * a genuine determinism failure — identical evidence producing a different
    conclusion — still fails closed, and an artifact whose own parts disagree
    about its session or book is never written at all;
  * a reassessment, a proposal and a governed decision each bind the EXACT
    persisted opportunity-cost version, and a dependency that exists only in
    memory is visible AS transient rather than silently inherited;
  * the governed intraday gate WITHHOLDS with an explicit reason when the exact
    artifact is missing, unretrievable, or not what it claims to be — and both an
    intraday HOLD and an intraday CHANGE can be governed when it is all provable;
  * same-session versions never double-count churn, never fabricate turnover
    events, and an idempotent replay reuses the exact evidence version;
  * nothing here creates an order, a fill, a broker call, an approval, a model
    promotion or a TRUE_FORWARD row, and automation stays off.

Every write path is hermetic (``tmp_path``); no production store, no provider, no
live backend and no scheduler is touched.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from paper_trader.api import event_signal_refresh as esr
from paper_trader.api import holding_opportunity_cost as hoc
from paper_trader.api import portfolio_decision as pdec
from paper_trader.api import portfolio_reassessment as prs
from paper_trader.engine import constrained_reallocation as cr
from paper_trader.engine import holding_opportunity_cost as k

REPO = Path(__file__).resolve().parents[1]
HOC_SRC = (REPO / "api" / "holding_opportunity_cost.py").read_text(
    encoding="utf-8", errors="replace")
PRS_SRC = (REPO / "api" / "portfolio_reassessment.py").read_text(
    encoding="utf-8", errors="replace")
PDEC_SRC = (REPO / "api" / "portfolio_decision.py").read_text(
    encoding="utf-8", errors="replace")
ESR_SRC = (REPO / "api" / "event_signal_refresh.py").read_text(
    encoding="utf-8", errors="replace")

BOOK = "alpha_paper_book_1"
SESSION = "2026-08-05"


# --------------------------------------------------------------------------- #
# Hermetic HOC input contracts — the exact shape the Slice-6 owner builds.
# --------------------------------------------------------------------------- #
def _pos(t, sec, w):
    return {"ticker": t, "sector": sec, "quantity": 100, "current_weight": w,
            "market_value": w * 100000.0, "price": 40.0, "target_weight": w}


def _urow(t, rank, score, sec):
    return {"ticker": t, "rank": rank, "score": score, "sector": sec,
            "adv_dollar": 5e7, "eligible": True}


def _ic(**over):
    """A complete, valid opportunity-cost input contract."""
    ic = {
        "schema_version": k.INPUT_SCHEMA_VERSION,
        "eligible_market_date": SESSION,
        "active_book_id": BOOK,
        "active_book_label": "Alpha Paper Book #1",
        "valuation_date": SESSION,
        "portfolio_state_hash": "PSHASH",
        "economic_state_hash": "ECON1",
        "economic_identity_version": "economic_identity.v1",
        "universe_scoring_hash": "USHASH",
        "universe_input_contract_hash": "USIN",
        "scoring_ranking_date": SESSION,
        "corporate_actions_hash": "CA1",
        "nav": 100000.0, "cash": 8000.0,
        "inputs_as_of_eligible_date": True,
        "positions": [_pos("AAA", "Tech", 0.04), _pos("BBB", "Energy", 0.04)],
        "universe_rows": [_urow("AAA", 10, 0.50, "Tech"),
                          _urow("BBB", 12, 0.45, "Energy"),
                          _urow("CCC", 1, 0.99, "Health")],
        "previous_ranking": {"AAA": 9, "BBB": 11, "CCC": 2},
        "previous_ranking_state": "AVAILABLE",
        "median_dollar_volume": {"AAA": 5e7, "BBB": 5e7},
        "trailing_prices": {},
        "aligned_returns": {"dates": [], "series": {}},
    }
    ic.update(over)
    return ic


def _run(ic=None):
    return k.build_assessment(input_contract=ic or _ic(), policy=hoc.resolve_policy())


def _persist(d, ic=None, result=None, **kw):
    ic = ic if ic is not None else _ic()
    res = result if result is not None else _run(ic)
    return hoc.persist_assessment(result=res, input_contract=ic, hoc_dir=str(d), **kw)


def _versions(d):
    return hoc.load_artifact_versions(active_book_id=BOOK,
                                      eligible_market_date=SESSION, hoc_dir=str(d))


# =========================================================================== #
# 1-12. The persistence contract: five outcomes over three identity axes.
# =========================================================================== #
def test_01_first_assessment_persists(tmp_path):
    p = _persist(tmp_path)
    assert p["status"] == hoc.PERSIST_CREATED
    assert p["persisted"] is True
    assert (tmp_path / "artifacts" / ("%s.json" % p["artifact_id"])).exists()
    assert (tmp_path / "index.json").exists()


def test_02_same_state_same_evidence_same_conclusion_is_reused(tmp_path):
    first = _persist(tmp_path)
    again = _persist(tmp_path)
    assert again["status"] == hoc.PERSIST_REUSED
    assert again["reused"] is True
    assert again["artifact_id"] == first["artifact_id"]
    # Idempotent: no second artifact file.
    assert len(list((tmp_path / "artifacts").glob("hoc_*.json"))) == 1


def test_03_same_state_different_evidence_creates_an_assessment_version(tmp_path):
    _persist(tmp_path)
    v2 = _persist(tmp_path, _ic(universe_scoring_hash="USHASH_V2"))
    assert v2["status"] == hoc.PERSIST_ASSESSMENT_VERSION
    assert v2["assessment_evidence_changed"] is True
    assert v2["economic_state_changed"] is False
    assert v2["persisted"] is True


def test_04_v1_is_immutable_after_v2(tmp_path):
    v1 = _persist(tmp_path)
    path = Path(v1["path"])
    before = path.read_bytes()
    _persist(tmp_path, _ic(universe_scoring_hash="USHASH_V2"))
    assert path.read_bytes() == before, "v1 was rewritten"


def test_05_v1_remains_exactly_retrievable(tmp_path):
    v1 = _persist(tmp_path)
    _persist(tmp_path, _ic(universe_scoring_hash="USHASH_V2"))
    got = hoc.load_artifact_by_id(artifact_id=v1["artifact_id"], hoc_dir=str(tmp_path))
    assert got is not None
    assert got["artifact_id"] == v1["artifact_id"]
    assert got["identity"]["assessment_hash"] == v1["identity"]["assessment_hash"]


def test_06_v2_is_exactly_retrievable(tmp_path):
    _persist(tmp_path)
    v2 = _persist(tmp_path, _ic(universe_scoring_hash="USHASH_V2"))
    got = hoc.load_artifact_by_id(artifact_id=v2["artifact_id"], hoc_dir=str(tmp_path))
    assert got["identity"]["assessment_hash"] == v2["identity"]["assessment_hash"]


def test_07_latest_selector_returns_v2(tmp_path):
    _persist(tmp_path)
    v2 = _persist(tmp_path, _ic(universe_scoring_hash="USHASH_V2"))
    latest = hoc.load_latest_artifact(active_book_id=BOOK,
                                      eligible_market_date=SESSION,
                                      hoc_dir=str(tmp_path))
    assert latest["artifact_id"] == v2["artifact_id"]


def test_08_version_history_returns_both(tmp_path):
    v1 = _persist(tmp_path)
    v2 = _persist(tmp_path, _ic(universe_scoring_hash="USHASH_V2"))
    chain = _versions(tmp_path)
    assert [v["artifact_id"] for v in chain] == [v1["artifact_id"], v2["artifact_id"]]
    assert chain[-1]["supersedes_artifact_id"] == v1["artifact_id"]


def test_09_same_evidence_b_same_conclusion_reuses_v2_exactly(tmp_path):
    _persist(tmp_path)
    v2 = _persist(tmp_path, _ic(universe_scoring_hash="USHASH_V2"))
    again = _persist(tmp_path, _ic(universe_scoring_hash="USHASH_V2"))
    assert again["status"] == hoc.PERSIST_REUSED
    assert again["artifact_id"] == v2["artifact_id"]
    assert len(_versions(tmp_path)) == 2


def test_10_same_evidence_altered_conclusion_is_conflict_rejected(tmp_path):
    ic = _ic()
    first = _persist(tmp_path, ic)
    tampered = dict(_run(ic))
    tampered["recommendation_counts"] = {"EXIT": 99}
    tampered["assessment_hash"] = "A_DIFFERENT_ANSWER"
    p = hoc.persist_assessment(result=tampered, input_contract=ic, hoc_dir=str(tmp_path))
    assert p["status"] == hoc.PERSIST_CONFLICT
    assert p["persisted"] is False and p["conflict"] is True
    assert hoc.load_latest_artifact(
        active_book_id=BOOK, eligible_market_date=SESSION,
        hoc_dir=str(tmp_path))["artifact_id"] == first["artifact_id"]


@pytest.mark.parametrize("field,value", [
    ("eligible_market_date", "2026-08-06"),
    ("active_book_id", "some_other_book"),
])
def test_11_inconsistent_book_or_session_identity_is_rejected(tmp_path, field, value):
    ic = _ic()
    res = dict(_run(ic))
    res[field] = value
    p = hoc.persist_assessment(result=res, input_contract=ic, hoc_dir=str(tmp_path))
    assert p["status"] == hoc.PERSIST_INCONSISTENT
    assert p["persisted"] is False
    assert p["identity_conflicts"]
    assert not list((tmp_path / "artifacts").glob("*.json"))


def test_12_new_economic_state_creates_an_economic_version(tmp_path):
    _persist(tmp_path)
    v2 = _persist(tmp_path, _ic(economic_state_hash="ECON2", cash=9000.0,
                                positions=[_pos("AAA", "Tech", 0.05),
                                           _pos("BBB", "Energy", 0.04)]))
    assert v2["status"] == hoc.PERSIST_ECONOMIC_VERSION
    assert v2["economic_state_changed"] is True


# =========================================================================== #
# 13-19. Evidence identity: what it excludes, what it tracks, determinism.
# =========================================================================== #
def _ev_hash(ic=None, result=None):
    ic = ic if ic is not None else _ic()
    return hoc.assessment_evidence_hash(hoc.assessment_evidence_identity(
        input_contract=ic, result=result if result is not None else _run(ic)))


def test_13_evidence_identity_excludes_wall_clock(tmp_path):
    base = _ev_hash()
    for stamp in ("2026-08-05T09:00:00+00:00", "2026-08-05T23:59:59+00:00"):
        assert _ev_hash(_ic(generated_at=stamp, persisted_at=stamp)) == base


def test_14_evidence_identity_excludes_request_and_run_id():
    base = _ev_hash()
    assert _ev_hash(_ic(run_id="evt_1", request_id="req_1", drc_run_id="drc_1")) == base


def test_15_evidence_identity_excludes_event_cycle_id():
    base = _ev_hash()
    assert _ev_hash(_ic(event_cycle_id="evt_deadbeef",
                        scheduler_invocation_id="sched_9",
                        materiality_trigger_fingerprint="FP_XYZ")) == base


def test_16_evidence_identity_excludes_persistence_timestamp(tmp_path):
    """Two artifacts written at different wall clocks from identical evidence carry
    the identical evidence hash — the timestamp lives on the wrapper, never in it."""
    a = _persist(tmp_path, now=datetime(2026, 8, 5, 9, tzinfo=timezone.utc))
    art = hoc.load_artifact_by_id(artifact_id=a["artifact_id"], hoc_dir=str(tmp_path))
    assert art["generated_at"].startswith("2026-08-05T09")
    assert art["identity"]["assessment_evidence_hash"] == _ev_hash()
    # And the documented exclusion list is honoured by the identity function itself.
    ident = hoc.assessment_evidence_identity(input_contract=_ic(), result=_run())
    for banned in hoc.EVIDENCE_EXCLUDED_PROVENANCE:
        assert banned not in ident, banned


@pytest.mark.parametrize("field,value", [
    ("universe_scoring_hash", "OTHER_RANKING"),
    ("universe_input_contract_hash", "OTHER_MODEL_INPUTS"),
    ("corporate_actions_hash", "CA2"),
    ("scoring_ranking_date", "2026-08-04"),
    ("inputs_as_of_eligible_date", False),
])
def test_17_evidence_identity_changes_when_a_true_input_changes(field, value):
    assert _ev_hash(_ic(**{field: value})) != _ev_hash()


def test_17b_evidence_tracks_market_window_and_prior_rank():
    """The owned price window and the PIT prior-rank snapshot are real kernel
    inputs (volatility, drawdown, liquidity, rank change), so both are evidence."""
    assert _ev_hash(_ic(median_dollar_volume={"AAA": 1.0, "BBB": 1.0})) != _ev_hash()
    assert _ev_hash(_ic(previous_ranking={"AAA": 1, "BBB": 2})) != _ev_hash()
    assert _ev_hash(_ic(previous_ranking_state="UNAVAILABLE",
                        previous_ranking=None)) != _ev_hash()
    # ... and holdings, which is the safety net when no economic hash is published.
    assert _ev_hash(_ic(positions=[_pos("AAA", "Tech", 0.09)])) != _ev_hash()


def test_18_artifact_identity_is_deterministic(tmp_path):
    a = hoc.artifact_identity(input_contract=_ic(), result=_run())
    b = hoc.artifact_identity(input_contract=_ic(), result=_run())
    assert a == b
    assert hoc.artifact_id_for(a) == hoc.artifact_id_for(b)
    # No wall clock, no uuid: the id is a pure function of bound evidence.
    assert "uuid" not in HOC_SRC.split("def artifact_id_for")[1][:400]


def test_19_version_artifact_ids_cannot_overwrite_each_other(tmp_path):
    v1 = _persist(tmp_path)
    v2 = _persist(tmp_path, _ic(universe_scoring_hash="USHASH_V2"))
    assert v1["artifact_id"] != v2["artifact_id"]
    assert Path(v1["path"]).exists() and Path(v2["path"]).exists()
    # Even a forced id collision refuses to land on the existing file.
    ident = hoc.artifact_identity(input_contract=_ic(universe_scoring_hash="V3"),
                                  result=_run(_ic(universe_scoring_hash="V3")))
    forced = hoc._unique_artifact_id(v1["artifact_id"], ident, str(tmp_path))
    assert forced != v1["artifact_id"]


# =========================================================================== #
# 20-21. Legacy compatibility. No historical artifact is ever rewritten.
# =========================================================================== #
def _write_legacy(tmp_path):
    """A pre-R54.3 artifact + index entry: no evidence hash, no fingerprints."""
    ic, res = _ic(), _run()
    aid = "hoc_%s_%s_legacy" % (SESSION, BOOK)
    art = {
        "artifact_id": aid, "schema_version": hoc.SCHEMA_VERSION,
        "composition_owner": hoc.COMPOSITION_OWNER,
        "generated_at": "2026-08-05T12:00:00+00:00",
        "identity": {"eligible_market_date": SESSION, "active_book_id": BOOK,
                     "portfolio_state_hash": "PSHASH",
                     "corporate_actions_hash": "CA1",
                     "universe_scoring_hash": "USHASH",
                     "decision_policy_version": hoc.DECISION_POLICY_VERSION,
                     "assessment_hash": res["assessment_hash"]},
        "input_contract": {"schema_version": k.INPUT_SCHEMA_VERSION,
                           "eligible_market_date": SESSION, "active_book_id": BOOK,
                           "universe_scoring_hash": "USHASH",
                           "universe_input_contract_hash": "USIN"},
        "assessment": res,
    }
    (tmp_path / "artifacts").mkdir(parents=True, exist_ok=True)
    p = tmp_path / "artifacts" / ("%s.json" % aid)
    p.write_text(json.dumps(art, indent=2), encoding="utf-8")
    (tmp_path / "index.json").write_text(json.dumps({
        "%s|%s" % (BOOK, SESSION): {
            "artifact_id": aid, "path": str(p),
            "assessment_hash": res["assessment_hash"],
            "portfolio_state_hash": "PSHASH", "universe_scoring_hash": "USHASH",
            "decision_policy_version": hoc.DECISION_POLICY_VERSION,
            "eligible_market_date": SESSION, "active_book_id": BOOK,
            "generated_at": "2026-08-05T12:00:00+00:00"}}, indent=2),
        encoding="utf-8")
    return aid, p, res


def test_20_legacy_artifact_remains_readable(tmp_path):
    aid, path, res = _write_legacy(tmp_path)
    art = hoc.load_latest_artifact(active_book_id=BOOK, eligible_market_date=SESSION,
                                   hoc_dir=str(tmp_path))
    assert art["artifact_id"] == aid
    assert hoc.load_artifact_by_id(artifact_id=aid,
                                   hoc_dir=str(tmp_path))["artifact_id"] == aid
    assert len(_versions(tmp_path)) == 1
    # A legacy entry still yields comparable identities — recomputed, not invented.
    ev, fp = hoc._existing_assessment_identity(
        json.loads((tmp_path / "index.json").read_text(encoding="utf-8"))[
            "%s|%s" % (BOOK, SESSION)], str(tmp_path))
    assert ev and fp
    # An identical rerun against a legacy artifact is still idempotent.
    assert _persist(tmp_path, _ic(), res)["status"] == hoc.PERSIST_REUSED


def test_21_no_historical_artifact_rewrite(tmp_path):
    aid, path, _ = _write_legacy(tmp_path)
    before = path.read_bytes()
    out = _persist(tmp_path, _ic(universe_scoring_hash="USHASH_V2"))
    assert out["persisted"] is True
    assert path.read_bytes() == before, "the legacy artifact was rewritten"
    assert out["artifact_id"] != aid
    # Legacy evidence is never fabricated: what it did not record stays absent.
    legacy = hoc.load_artifact_by_id(artifact_id=aid, hoc_dir=str(tmp_path))
    assert "assessment_evidence_hash" not in legacy["identity"]


# =========================================================================== #
# 22-27. Downstream EXACT binding + event-cycle ordering.
# =========================================================================== #
def _prs_ps(**kw):
    d = {"active_book": {"book_id": BOOK, "book_label": "Alpha Paper Book #1"},
         "dates": {"eligible_market_date": SESSION, "valuation_date": SESSION},
         "capital": {"nav": 100000.0, "cash": 8000.0},
         "state_hash": "PSHASH", "economic_state_hash": "ECON1",
         "positions": [{"ticker": "AAA"}, {"ticker": "BBB"}]}
    d.update(kw)
    return d


def test_22_reassessment_binds_the_exact_persisted_hoc_artifact(tmp_path):
    p = _persist(tmp_path)
    art = hoc.load_artifact_by_id(artifact_id=p["artifact_id"], hoc_dir=str(tmp_path))
    binding = hoc.artifact_binding(p)
    ic = prs.build_input_contract(
        portfolio_state=_prs_ps(), scoring={"output_hash": "USHASH"},
        hoc_assessment=art["assessment"], hoc_binding=binding)
    assert ic["hoc_artifact_id"] == p["artifact_id"]
    assert ic["hoc_persisted"] is True
    assert ic["hoc_assessment_evidence_hash"] == \
        p["identity"]["assessment_evidence_hash"]
    ident = prs.artifact_identity(input_contract=ic, result={"reassessment_hash": "R"})
    assert ident["hoc_artifact_id"] == p["artifact_id"]


def test_23_reassessment_cannot_claim_a_transient_hoc_dependency(tmp_path):
    """THE R54.3 DEFECT, pinned. An assessment whose write was refused is reported
    as unpersisted — the reassessment records the truth instead of a hash that
    names nothing retrievable."""
    _persist(tmp_path)                       # the session's persisted artifact
    transient = _run(_ic(universe_scoring_hash="NEVER_PERSISTED"))
    binding = prs.resolve_hoc_binding(hoc_assessment=transient, active_book_id=BOOK,
                                      eligible_market_date=SESSION,
                                      hoc_dir=str(tmp_path))
    assert binding["hoc_persisted"] is False
    assert binding["hoc_artifact_retrievable"] is False
    assert binding["hoc_artifact_id"] is None
    assert "transiently" in binding["hoc_binding_detail"]
    ic = prs.build_input_contract(portfolio_state=_prs_ps(),
                                  scoring={"output_hash": "USHASH"},
                                  hoc_assessment=transient, hoc_binding=binding)
    assert ic["hoc_persisted"] is False and ic["hoc_artifact_id"] is None


def test_23b_a_persisted_dependency_resolves_clean(tmp_path):
    p = _persist(tmp_path)
    art = hoc.load_artifact_by_id(artifact_id=p["artifact_id"], hoc_dir=str(tmp_path))
    b = prs.resolve_hoc_binding(hoc_assessment=art["assessment"], active_book_id=BOOK,
                                eligible_market_date=SESSION, hoc_dir=str(tmp_path))
    assert b["hoc_persisted"] is True
    assert b["hoc_artifact_retrievable"] is True
    assert b["hoc_artifact_identity_matches"] is True
    assert b["hoc_artifact_id"] == p["artifact_id"]


def test_24_proposal_binds_exact_hoc_lineage_through_the_reassessment(tmp_path):
    p = _persist(tmp_path)
    art = hoc.load_artifact_by_id(artifact_id=p["artifact_id"], hoc_dir=str(tmp_path))
    ic = prs.build_input_contract(portfolio_state=_prs_ps(),
                                  scoring={"output_hash": "USHASH"},
                                  hoc_assessment=art["assessment"],
                                  hoc_binding=hoc.artifact_binding(p))
    pb = prs.proposal_binding(
        reassessment={"reassessment_hash": "RA1", "eligible_market_date": SESSION,
                      "active_book_id": BOOK},
        artifact={"reassessment_id": "prs_1", "identity": {}}, input_contract=ic)
    assert pb["hoc_artifact_id"] == p["artifact_id"]
    assert pb["hoc_assessment_evidence_hash"] == \
        p["identity"]["assessment_evidence_hash"]
    assert pb["hoc_persisted"] is True


def test_25_governed_decision_binds_exact_hoc_lineage(tmp_path):
    p = _persist(tmp_path)
    cand = pdec.build_intraday_candidate(
        portfolio_state=_prs_ps(), event_cycle={"hoc_artifact_id": p["artifact_id"],
                                                "hoc_persisted": True},
        reassessment={}, proposal_summary={}, constrained={},
        hoc_binding=hoc.resolve_binding(binding=hoc.artifact_binding(p),
                                        active_book_id=BOOK,
                                        eligible_market_date=SESSION,
                                        hoc_dir=str(tmp_path)))
    assert cand["identity"]["hoc_artifact_id"] == p["artifact_id"]
    assert cand["identity"]["hoc_assessment_evidence_hash"] == \
        p["identity"]["assessment_evidence_hash"]
    assert cand["evidence"]["hoc_artifact_retrievable"] is True


def test_26_event_refresh_persists_hoc_before_reassessment():
    """Ordering is structural: the HOLDING_OPPORTUNITY_COST step (which persists)
    closes before PORTFOLIO_REASSESSMENT opens, and the binding it produced is
    handed forward rather than re-derived."""
    body = ESR_SRC.split('with _step("HOLDING_OPPORTUNITY_COST"')[1]
    hoc_then_prs = body.split('with _step("PORTFOLIO_REASSESSMENT"')
    assert len(hoc_then_prs) == 2, "reassessment step must follow the HOC step"
    assert "hoc_binding = dict((hoc_result or {}).get(\"binding\") or {})" \
        in hoc_then_prs[0]
    assert "hoc_binding=(hoc_binding or None)" in hoc_then_prs[1][:600]


def test_27_event_cycle_output_exposes_hoc_persistence_status(tmp_path, monkeypatch):
    calls = {}

    def _hoc_fn(**kw):
        res = _run()
        p = hoc.persist_assessment(result=res, input_contract=_ic(),
                                   hoc_dir=str(tmp_path / "hoc"))
        return {"assessment": res, "persistence": p,
                "binding": hoc.artifact_binding(p)}

    def _reas_fn(**kw):
        calls.update(kw)
        return {"reassessment": {"reassessment_hash": "RA1",
                                 "reassessment_state": "CURRENT_NO_CHANGE"},
                "persistence": {"artifact_id": "prs_1", "status": "CREATED",
                                "persisted": True}}

    monkeypatch.setattr(esr.emat, "assess_materiality", lambda **kw: {
        "change_level": "MATERIAL_SIGNAL_CHANGED", "reassessment_required": True,
        "reassessment_reason": "test", "duplicate_of_prior_trigger": False,
        "data_changed": True, "trigger_count": 1, "trigger_fingerprint": "FP1",
        "affected_entities": []})
    out = esr.run_event_signal_refresh(
        confirm=esr.EXECUTE_CONFIRM_TOKEN, fabric_dir=tmp_path / "fabric",
        portfolio_state=_prs_ps(), scoring={"rankings": []}, price_panel=None,
        corpus_events=[], hoc_fn=_hoc_fn, reassessment_fn=_reas_fn,
        proposal_gate_fn=lambda r: {"build_proposal": False},
        governance_fn=lambda **kw: None, prior_ranking=None,
        decision_dir=tmp_path / "decisions")
    summ = esr.build_last_run_summary(out)
    assert summ["hoc_persisted"] is True
    assert summ["hoc_persistence_status"] == hoc.PERSIST_CREATED
    assert summ["hoc_artifact_id"]
    assert out["holding_opportunity_cost"]["persisted"] is True
    # The binding actually reached the reassessment owner.
    assert calls.get("hoc_binding", {}).get("hoc_persisted") is True


# =========================================================================== #
# 28-32. GOVERNANCE: the dependency must be producible as evidence.
# =========================================================================== #
BOOK_G, SESSION_G = "alpha_paper_book_1", "2026-08-31"
HELD_G = ["T%02d" % i for i in range(25)]
HOC_AID = "hoc_2026-08-31_alpha_paper_book_1_HOC1"
T1 = datetime(2026, 9, 1, 17, 42, 0, tzinfo=timezone.utc)


def _gps(**kw):
    d = {"state": "PORTFOLIO_STATE_READY",
         "active_book": {"book_id": BOOK_G, "book_label": "Alpha Paper Book #1"},
         "dates": {"eligible_market_date": SESSION_G, "desk_mark_date": SESSION_G,
                   "valuation_date": SESSION_G, "latest_daily_close_date": SESSION_G},
         "capital": {"nav": 99113.22, "cash": 1200.5},
         "positions": [{"ticker": t} for t in HELD_G],
         "state_hash": "PSH1", "economic_state_hash": "ESH1"}
    d.update(kw)
    return d


def _gcycle(**kw):
    d = {"run_id": "evt_aaaa1111", "state": esr.ST_PROPOSAL_AVAILABLE,
         "generated_at": "2026-09-01T17:40:00+00:00",
         "completed_at": "2026-09-01T17:42:07+00:00",
         "reassessment_ran": True, "proposal_built": True,
         "materiality_change_level": "MATERIAL_SIGNAL_CHANGED",
         "active_book_id": BOOK_G, "eligible_market_date": SESSION_G,
         "portfolio_state_hash": "ESH1", "holdings": list(HELD_G),
         "hoc_assessment_hash": "HOC1", "hoc_holdings_reviewed": 25,
         "hoc_artifact_id": HOC_AID, "hoc_persisted": True,
         "hoc_persistence_status": "CREATED",
         "hoc_assessment_evidence_hash": "HOCEV1",
         "reassessment_hash": "RA1", "reassessment_id": "ra_1",
         "reassessment_persisted": True,
         "reassessment_persistence_status": "CREATED",
         "proposal_hash": "PR1", "materiality_trigger_fingerprint": "FP1",
         "duplicate_of_prior_trigger": False, "blocker_codes": [],
         "reassessment_state": "PROPOSAL_READY", "proposal_state": "READY",
         "stage_timestamps": {"hoc_completed_at": "2026-09-01T17:41:10+00:00"},
         "cycle_duration_seconds": 7.3}
    d.update(kw)
    return d


def _greas(**kw):
    d = {"state": "PROPOSAL_READY", "eligible_market_date": SESSION_G,
         "active_book": {"book_id": BOOK_G}, "reassessment_id": "ra_1",
         "reassessment_hash": "RA1",
         "artifact": {"reassessment_id": "ra_1",
                      "identity": {"economic_state_hash": "ESH1"}},
         "proposal_binding": {"reassessment_id": "ra_1", "reassessment_hash": "RA1",
                              "hoc_assessment_hash": "HOC1",
                              "hoc_artifact_id": HOC_AID,
                              "hoc_assessment_evidence_hash": "HOCEV1",
                              "hoc_persisted": True,
                              "universe_scoring_hash": "US1",
                              "universe_input_contract_hash": "UIC1",
                              "portfolio_state_hash": "PSH1",
                              "corporate_actions_hash": "CA1",
                              "eligible_market_date": SESSION_G,
                              "active_book_id": BOOK_G},
         "execution_precedence": {"execution_active": False, "reason": "none"},
         "decision": {"holdings_evaluated": 25}}
    d.update(kw)
    return d


def _gsumm(**kw):
    d = {"reallocation_proposal_available": True, "reallocation_proposal_stale": False,
         "reallocation_corporate_actions_hash": "CA1",
         "reallocation_proposal_hash": "PR1", "reallocation_proposal_id": "prop_1",
         "reallocation_proposal_withheld": False, "reallocation_withheld_reasons": [],
         "reallocation_outcome": cr.OUTCOME_HOLD_CURRENT_BOOK,
         "reallocation_feasible_target_exists": True, "reallocation_data_gaps": [],
         "reallocation_bound_hoc_assessment_hash": "HOC1",
         "reallocation_bound_eligible_market_date": SESSION_G,
         "reallocation_bound_active_book_id": BOOK_G}
    d.update(kw)
    return d


def _gcon(outcome=cr.OUTCOME_HOLD_CURRENT_BOOK, **kw):
    econ = {"switching_hurdle": 0.02,
            "clears_switching_hurdle": outcome == cr.OUTCOME_PROPOSAL_READY,
            "score_improvement_net_of_cost": 0.004, "one_way_turnover": 0.11,
            "estimated_transaction_cost": 214.5, "concentration_before": 0.0412,
            "concentration_after": 0.0430, "portfolio_volatility_before": 0.1731,
            "portfolio_volatility_after": 0.1755}
    d = {"outcome": outcome, "feasible_target_exists": True,
         "calculation_owner": cr.CALCULATION_OWNER, "switching_economics": econ,
         "ideal_target": {"zero_base_owner": "api.zero_base_target"},
         "constraint_inventory": {"constraints": [{"code": "TURNOVER"}]},
         "multi_asset": {"current_holdings_privileged": False},
         "best_feasible_target": {"allocations": [
             {"ticker": "T00", "action": "REDUCE", "current_weight": 0.06,
              "proposed_weight": 0.03, "delta_weight": -0.03,
              "capital_change": -2900.0}]}}
    d.update(kw)
    return d


def _gwf(**kw):
    d = {"overall_state": "WAITING_FOR_OWNED_DATA",
         "operational_state": {"active_book_id": BOOK_G,
                               "eligible_market_date": SESSION_G,
                               "desk_mark_date": SESSION_G,
                               "valuation_date": SESSION_G,
                               "latest_completed_close_date": SESSION_G,
                               "operational_close_valid": True,
                               "eligible_session_already_processed": True,
                               "pending_orders": 0},
         "research_cycle_state": {
             "opportunity_cost_artifact_class": "GOVERNED_DRC_TERMINAL",
             "opportunity_cost_producer_owner": "api.daily_research_cycle",
             "governed_research_evidence_current": True},
         "portfolio_decision_state": {"proposal_hash": "PR1"}}
    d.update(kw)
    return d


def _ghocb(**kw):
    d = {"hoc_owner": "api.holding_opportunity_cost", "hoc_artifact_id": HOC_AID,
         "hoc_assessment_hash": "HOC1", "hoc_assessment_evidence_hash": "HOCEV1",
         "hoc_eligible_market_date": SESSION_G, "hoc_active_book_id": BOOK_G,
         "hoc_persistence_status": "CREATED", "hoc_persisted": True,
         "hoc_artifact_retrievable": True, "hoc_artifact_identity_matches": True,
         "hoc_binding_detail": "artifact retrieved; assessment hash matches"}
    d.update(kw)
    return d


def _ggate(*, outcome=cr.OUTCOME_HOLD_CURRENT_BOOK, cycle=None, reas=None,
           hocb=None, current_governed=None, summ=None):
    ps, wf = _gps(), _gwf()
    cycle = cycle if cycle is not None else _gcycle()
    reas = reas if reas is not None else _greas()
    summ = summ if summ is not None else _gsumm(reallocation_outcome=outcome)
    con = _gcon(outcome)
    sc = {"ranking_date": SESSION_G, "input_contract_hash": "UIC1"}
    cand = pdec.build_intraday_candidate(
        portfolio_state=ps, event_cycle=cycle, reassessment=reas,
        proposal_summary=summ, constrained=con, workflow=wf, scoring_identity=sc,
        hoc_binding=hocb if hocb is not None else _ghocb(), now=T1)
    gate = pdec.evaluate_intraday_governance(
        candidate=cand, portfolio_state=ps, event_cycle=cycle, reassessment=reas,
        proposal_summary=summ, constrained=con, workflow=wf, scoring_identity=sc,
        current_governed=current_governed)
    return cand, gate


def test_28_exact_persisted_hoc_passes_the_governance_check():
    cand, gate = _ggate()
    hoc_checks = [c for c in gate["checks"] if c["group"] == "HOC_IDENTITY"]
    assert all(c["passed"] for c in hoc_checks), [c for c in hoc_checks
                                                  if not c["passed"]]
    assert gate["verdict"] == pdec.GATE_ELIGIBLE
    names = {c["check"] for c in hoc_checks}
    assert {"HOC_ARTIFACT_ID_BOUND", "HOC_ASSESSMENT_WAS_PERSISTED",
            "HOC_ARTIFACT_RETRIEVABLE", "HOC_ARTIFACT_IDENTITY_MATCHES",
            "REASSESSMENT_BOUND_TO_THE_SAME_HOC_ARTIFACT",
            "REASSESSMENT_DEPENDENCY_IS_NOT_TRANSIENT"} <= names


def test_29_missing_exact_hoc_artifact_withholds():
    _, gate = _ggate(cycle=_gcycle(hoc_artifact_id=None, hoc_persisted=False,
                                   hoc_persistence_status="CONFLICT_REJECTED"),
                     hocb=_ghocb(hoc_artifact_id=None, hoc_persisted=False,
                                 hoc_artifact_retrievable=False,
                                 hoc_artifact_identity_matches=False))
    assert gate["verdict"] == pdec.GATE_WITHHELD
    assert pdec.WR_HOC_NOT_PERSISTED in gate["withheld_reason_codes"]
    assert "HOC_ASSESSMENT_WAS_PERSISTED" in gate["failing_checks"]


def test_30_hoc_artifact_identity_mismatch_withholds():
    _, gate = _ggate(hocb=_ghocb(hoc_artifact_identity_matches=False,
                                 hoc_binding_detail="assessment hash MISMATCH"))
    assert gate["verdict"] == pdec.GATE_WITHHELD
    assert pdec.WR_HOC_ARTIFACT_MISMATCH in gate["withheld_reason_codes"]


def test_31_hoc_evidence_mismatch_withholds():
    """The reassessment claiming a DIFFERENT artifact than the candidate stands on."""
    reas = _greas()
    reas["proposal_binding"] = {**reas["proposal_binding"],
                                "hoc_artifact_id": "hoc_some_other_version"}
    _, gate = _ggate(reas=reas)
    assert gate["verdict"] == pdec.GATE_WITHHELD
    assert pdec.WR_HOC_ARTIFACT_MISMATCH in gate["withheld_reason_codes"]
    assert "REASSESSMENT_BOUND_TO_THE_SAME_HOC_ARTIFACT" in gate["failing_checks"]
    # And a reassessment that recorded a transient dependency is refused outright.
    reas2 = _greas()
    reas2["proposal_binding"] = {**reas2["proposal_binding"], "hoc_persisted": False}
    _, gate2 = _ggate(reas=reas2)
    assert pdec.WR_HOC_NOT_PERSISTED in gate2["withheld_reason_codes"]


def test_32_wrong_book_or_session_withholds(tmp_path):
    """Retrievability is proven by the artifact's OWNER, and a stored artifact from
    another book/session does not satisfy the claim."""
    p = _persist(tmp_path)
    resolved = hoc.resolve_binding(
        binding={**hoc.artifact_binding(p), "hoc_active_book_id": "a_different_book"},
        active_book_id="a_different_book", eligible_market_date=SESSION,
        hoc_dir=str(tmp_path))
    assert resolved["hoc_artifact_identity_matches"] is False
    _, gate = _ggate(hocb=_ghocb(hoc_artifact_identity_matches=False))
    assert gate["verdict"] == pdec.GATE_WITHHELD
    assert pdec.WR_HOC_ARTIFACT_MISMATCH in gate["withheld_reason_codes"]


# =========================================================================== #
# 33-38. Authority: HOLD and CHANGE, and who may supersede what.
# =========================================================================== #
def test_33_intraday_hold_can_become_governed():
    cand, gate = _ggate(outcome=cr.OUTCOME_HOLD_CURRENT_BOOK)
    assert cand["decision"] == pdec.GD_HOLD_CURRENT_BOOK
    assert gate["eligible"] is True
    assert cand["manual_review_required"] is False
    assert cand["position_recommendations"] == []


def test_34_intraday_change_can_become_governed():
    cand, gate = _ggate(outcome=cr.OUTCOME_PROPOSAL_READY)
    assert cand["decision"] == pdec.GD_CHANGE_RECOMMENDED
    assert gate["eligible"] is True
    assert cand["manual_review_required"] is True


def test_35_governed_hold_respects_supersession(tmp_path):
    cand, gate = _ggate(outcome=cr.OUTCOME_HOLD_CURRENT_BOOK)
    rec = pdec.record_governed_decision(
        candidate=cand, gate=gate, confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN,
        decision_dir=tmp_path, now=T1)
    assert rec["recorded"] is True
    # The identical evidence cannot be promoted twice.
    _, gate2 = _ggate(outcome=cr.OUTCOME_HOLD_CURRENT_BOOK,
                      current_governed=rec["record"])
    assert gate2["verdict"] == pdec.GATE_WITHHELD
    assert pdec.WR_DUPLICATE in gate2["withheld_reason_codes"]


def test_36_governed_change_respects_supersession(tmp_path):
    cand, gate = _ggate(outcome=cr.OUTCOME_PROPOSAL_READY)
    rec = pdec.record_governed_decision(
        candidate=cand, gate=gate, confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN,
        decision_dir=tmp_path, now=T1)
    assert rec["recorded"] is True
    assert rec["record"]["decision"] == pdec.GD_CHANGE_RECOMMENDED
    # A governed CHANGE approves nothing; manual review still stands.
    assert rec["record"]["manual_review_required"] is True
    # A GENUINELY newer governed decision: a later session, different evidence and
    # a different candidate identity, so this is supersession and not a duplicate.
    newer = {**rec["record"], "record_id": "later",
             "decided_at": "2026-09-02T10:00:00+00:00",
             "eligible_market_session": "2026-09-02",
             "candidate_identity_hash": "a_different_candidate_identity",
             "identity": {**rec["record"].get("identity", {}),
                          "eligible_market_session": "2026-09-02",
                          "reassessment_hash": "RA_NEWER",
                          "proposal_hash": "PR_NEWER"}}
    _, gate2 = _ggate(outcome=cr.OUTCOME_PROPOSAL_READY, current_governed=newer)
    assert gate2["verdict"] == pdec.GATE_WITHHELD
    assert pdec.WR_SUPERSEDED in gate2["withheld_reason_codes"]


def test_37_research_only_result_cannot_supersede(tmp_path):
    """A cycle whose HOC never became an artifact is research, not governance —
    and a withheld gate records nothing."""
    _, gate = _ggate(cycle=_gcycle(hoc_persisted=False,
                                   hoc_persistence_status="CONFLICT_REJECTED"),
                     hocb=_ghocb(hoc_persisted=False,
                                 hoc_artifact_retrievable=False))
    assert gate["eligible"] is False
    cand, _ = _ggate()
    out = pdec.record_governed_decision(
        candidate=cand, gate=gate, confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN,
        decision_dir=tmp_path, now=T1)
    assert out.get("recorded") is not True
    assert not list(Path(tmp_path).glob("governed_decisions.json"))


def test_38_governance_withheld_result_cannot_supersede(tmp_path):
    cand, gate = _ggate(outcome=cr.OUTCOME_HOLD_CURRENT_BOOK)
    standing = pdec.record_governed_decision(
        candidate=cand, gate=gate, confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN,
        decision_dir=tmp_path, now=T1)["record"]
    _, withheld = _ggate(hocb=_ghocb(hoc_artifact_retrievable=False,
                                     hoc_persisted=False))
    assert withheld["eligible"] is False
    out = pdec.record_governed_decision(
        candidate=cand, gate=withheld,
        confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN, decision_dir=tmp_path, now=T1)
    assert out.get("recorded") is not True
    still = pdec.load_governed_decision_record(active_book_id=BOOK_G,
                                               decision_dir=tmp_path)
    assert still["record_id"] == standing["record_id"]


# =========================================================================== #
# 39-43. Churn / cooldown / idempotency across same-session versions.
# =========================================================================== #
def test_39_same_session_hoc_versions_do_not_double_count_churn(tmp_path):
    """The HOC store holds no change history at all — turnover and cooldown are
    counted from the reassessment owner's authoritative one-row-per-session
    history, which R54.2 already collapses by economic session."""
    for h in ("USHASH", "USHASH_V2", "USHASH_V3"):
        _persist(tmp_path, _ic(universe_scoring_hash=h))
    assert len(_versions(tmp_path)) == 3
    files = sorted(p.name for p in tmp_path.rglob("*.json"))
    assert files == sorted(["index.json"] + [
        "%s.json" % v["artifact_id"] for v in _versions(tmp_path)])
    # No history/ledger/turnover file is created by the opportunity-cost owner.
    for token in ("_append_history", "change_history", "turnover_event"):
        assert token not in HOC_SRC


def test_40_same_session_versions_create_no_turnover_events(tmp_path):
    """Every version is an ASSESSMENT, and an assessment moves no capital."""
    for h in ("USHASH", "USHASH_V2"):
        p = _persist(tmp_path, _ic(universe_scoring_hash=h))
        art = hoc.load_artifact_by_id(artifact_id=p["artifact_id"],
                                      hoc_dir=str(tmp_path))
        safety = art["assessment"].get("safety") or {}
        assert safety.get("creates_orders") is not True
        assert safety.get("changed_holdings") is not True
    assert prs.authoritative_history_rows is not None  # the ONE history collapser


def test_41_no_duplicate_proposal_from_a_persistence_retry(tmp_path):
    """A retry that re-persists identical evidence reuses the SAME artifact id, so
    nothing downstream sees a new dependency to build against."""
    a = _persist(tmp_path)
    b = _persist(tmp_path)
    c = _persist(tmp_path)
    assert a["artifact_id"] == b["artifact_id"] == c["artifact_id"]
    assert hoc.artifact_binding(b)["hoc_artifact_id"] == \
        hoc.artifact_binding(c)["hoc_artifact_id"]
    assert len(_versions(tmp_path)) == 1


def test_42_no_duplicate_governed_decision_from_a_retry(tmp_path):
    cand, gate = _ggate()
    first = pdec.record_governed_decision(
        candidate=cand, gate=gate, confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN,
        decision_dir=tmp_path, now=T1)
    assert first["recorded"] is True
    _, gate2 = _ggate(current_governed=first["record"])
    assert pdec.WR_DUPLICATE in gate2["withheld_reason_codes"]


def test_43_idempotent_event_replay_reuses_the_exact_evidence_version(tmp_path):
    """Replaying the same cycle re-derives the same identity and lands on the same
    immutable artifact — the version chain does not grow on replay."""
    ic = _ic(universe_scoring_hash="USHASH_V2")
    _persist(tmp_path)
    v2 = _persist(tmp_path, ic)
    for _ in range(3):
        again = _persist(tmp_path, ic)
        assert again["status"] == hoc.PERSIST_REUSED
        assert again["artifact_id"] == v2["artifact_id"]
    assert len(_versions(tmp_path)) == 2


# =========================================================================== #
# 44-50. SAFETY. Structural properties of the code, not runtime preferences.
# =========================================================================== #
def test_44_no_order_creation(tmp_path):
    """The owner has no order path at all, and every artifact it writes SAYS so.

    (The persisted safety block legitimately contains the words ``created_orders``
    and ``created_order_plan`` — as explicit ``false`` declarations. The invariant
    is the VALUE, so it is asserted as a value, never as a banned substring.)
    """
    p = _persist(tmp_path)
    assert "create_order" not in HOC_SRC and "place_order" not in HOC_SRC
    art = hoc.load_artifact_by_id(artifact_id=p["artifact_id"], hoc_dir=str(tmp_path))
    safety = art["assessment"].get("safety") or {}
    for flag in ("created_orders", "created_order_plan", "created_target"):
        assert safety.get(flag) is False, flag


def test_45_no_fill_creation(tmp_path):
    p = _persist(tmp_path)
    assert "create_fill" not in HOC_SRC and "record_fill" not in HOC_SRC
    art = hoc.load_artifact_by_id(artifact_id=p["artifact_id"], hoc_dir=str(tmp_path))
    assert (art["assessment"].get("safety") or {}).get("created_fills") is not True
    for f in tmp_path.rglob("*.json"):
        blob = json.loads(f.read_text(encoding="utf-8"))
        assert "fills" not in blob and "orders" not in blob


def test_46_no_broker_call():
    for token in ("broker", "alpaca", "ibkr", "execute_trade"):
        assert token not in HOC_SRC.lower().replace("no broker", "")


def test_47_automation_remains_off(tmp_path):
    p = _persist(tmp_path)
    art = hoc.load_artifact_by_id(artifact_id=p["artifact_id"], hoc_dir=str(tmp_path))
    safety = art["assessment"].get("safety") or {}
    assert safety.get("automation_enabled") is not True
    _, gate = _ggate()
    assert gate["safety"]["automation_enabled"] is False
    assert gate["safety"]["broker_enabled"] is False


def test_48_no_automatic_proposal_approval():
    cand, gate = _ggate(outcome=cr.OUTCOME_PROPOSAL_READY)
    assert cand["safety"]["approved_anything"] is False
    assert cand["safety"]["automatic_approval_allowed"] is False
    assert cand["manual_review_required"] is True
    assert gate["safety"]["manual_review_required_for_change"] is True


def test_49_no_automatic_model_promotion():
    cand, _ = _ggate()
    assert cand["safety"]["promoted_model"] is False
    assert cand["safety"]["automatic_model_promotion_allowed"] is False
    assert cand["safety"]["activated_sleeve"] is False
    assert "promote_model" not in HOC_SRC


def test_50_no_true_forward_fabrication(tmp_path):
    _persist(tmp_path, _ic(universe_scoring_hash="USHASH_V2"))
    assert "TRUE_FORWARD" not in HOC_SRC
    for f in tmp_path.rglob("*.json"):
        assert "TRUE_FORWARD" not in f.read_text(encoding="utf-8")


# =========================================================================== #
# Structural: ONE owner, one store, one governance gate.
# =========================================================================== #
def test_51_one_hoc_writer_and_one_store():
    """No second module may WRITE the opportunity-cost store.

    Reading the root env var is legitimate — ``api.daily_research_cycle`` resolves
    it to pass ``hoc_dir`` down to the one owner. What must stay unique is the
    WRITE: the identity builder, the artifact/index write and the persist entry
    point all live here and nowhere else.
    """
    assert HOC_SRC.count("def persist_assessment(") == 1
    for path in sorted((REPO / "api").glob("*.py")):
        if path.name == "holding_opportunity_cost.py":
            continue
        src = path.read_text(encoding="utf-8", errors="replace")
        # The HOC artifact-id format is this store's signature. Other domains own
        # their own artifact roots and may even reuse a generic function name
        # (api.research_agent persists ITS assessments); what must never be
        # duplicated is THIS store's id scheme or a second write into it.
        assert '"hoc_%s_%s_%s"' not in src, "%s mints HOC ids" % path.name
        assert "hoc._atomic_write_json" not in src, path.name
        assert "holding_opportunity_cost.persist_assessment" not in src, path.name
        assert "hoc.persist_assessment" not in src, path.name


def test_52_the_gate_opens_no_store_of_its_own():
    """``evaluate_intraday_governance`` stays PURE: retrievability is resolved by
    the artifact's owner and handed in as a fact."""
    body = PDEC_SRC.split("def evaluate_intraday_governance")[1].split(
        "\ndef governed_decision_ordering_key")[0]
    for banned in ("load_artifact_by_id", "load_latest_artifact", "open(",
                   "read_text", "json.load"):
        assert banned not in body, banned


def test_53_persistence_status_vocabulary_is_frozen():
    assert hoc.PERSIST_STATUS_VOCAB == (
        "CREATED", "REUSED_EXISTING", "CREATED_NEW_VERSION",
        "CREATED_ASSESSMENT_VERSION", "CONFLICT_REJECTED",
        "REJECTED_INCONSISTENT_IDENTITY", "NOT_PERSISTED")
    # The same words the reassessment owner already uses — one vocabulary.
    for name in ("PERSIST_CREATED", "PERSIST_REUSED", "PERSIST_ECONOMIC_VERSION",
                 "PERSIST_ASSESSMENT_VERSION", "PERSIST_CONFLICT",
                 "PERSIST_INCONSISTENT"):
        assert getattr(hoc, name) == getattr(prs, name)


def test_54_withheld_reason_codes_are_registered():
    assert pdec.WR_HOC_NOT_PERSISTED == "HOC_ARTIFACT_NOT_PERSISTED"
    assert pdec.WR_HOC_ARTIFACT_MISMATCH == "HOC_ARTIFACT_IDENTITY_MISMATCH"
    for code in (pdec.WR_HOC_NOT_PERSISTED, pdec.WR_HOC_ARTIFACT_MISMATCH):
        assert code in pdec.WITHHELD_REASON_VOCAB
