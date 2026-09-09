"""R61 — DECISION & RESEARCH CONTINUITY.

What these tests prove:

DECISION CONTINUITY (Workstream A)
  * the opportunity-cost owner is the ONE speller of "which assessment hash does
    this consumer depend on", and every consumer publishes the STORED artifact's
    hash rather than the document it happened to re-derive;
  * a reassessment binds the EXACT immutable version it consumed, resolved by
    content across the whole session chain — so a session that legitimately holds
    several assessments (R54.3) can no longer strand a consumer on "not persisted";
  * a LATER artifact can never validate an earlier binding, an unheld hash still
    fails closed, and no historical artifact is rebound;
  * the governance gate resolves its standing authority through the SAME rule the
    canonical read applies, so a legacy projection built from the candidate's own
    reassessment can no longer make every new candidate a DUPLICATE_CANDIDATE —
    while a genuine duplicate still is one.

RESEARCH CONTINUITY (Workstreams B, C, D, E)
  * every blocked research job resolves to a canonical, asset-agnostic reason with
    a declared clearance class, and an unrecognised blocker is never folded into a
    neighbour;
  * the runtime tells the truth about waiting: a busy worker says it is executing,
    a blocked one names the wake condition, and an exhausted frontier says so;
  * a probe whose substrate has not moved is DEFERRED rather than re-executed, and
    a generative search seed ADVANCES rather than sitting at a fixpoint;
  * a freeze resolves to exactly one lifecycle state from persisted history; a
    WITHDRAWN or INVALIDATED freeze can never be adopted, by anyone, ever;
  * freeze -> forward registration is one governed, idempotent, crash-recoverable
    operation, and an adoption may only ever look forward;
  * the adoption identity is asset-agnostic and proven for equity, commodity,
    rates, FX and cross-asset challengers.

CROSS-SURFACE CONSISTENCY (Workstream I)
  * ONE exchange calendar decides every session date, so a full-day holiday can
    never be the provider's expected market date (Sep-7 Labor Day, Sep-8 pre-close);
  * a latency endpoint the producing lane never had is NOT_APPLICABLE, not MISSING,
    and no timestamp is reconstructed anywhere.

SAFETY (Workstream G)
  * nothing here promotes a model, activates a sleeve, mutates a holding, creates
    an order or approves anything, and every adoption path says so structurally.

Every write path is hermetic (``tmp_path``); no production store, no provider, no
live backend, no scheduler and no live research state is touched.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from paper_trader.alpha_agent.r59 import blockers as BLK
from paper_trader.alpha_agent.r59 import runtime as R59RT
from paper_trader.api import active_manager_state as ams
from paper_trader.api import daily_close as dc
from paper_trader.api import event_signal_refresh as esr
from paper_trader.api import holding_opportunity_cost as hoc
from paper_trader.api import portfolio_decision as pdec
from paper_trader.api import portfolio_reassessment as prs
from paper_trader.api import prospective_adoption as PA
from paper_trader.engine import constrained_reallocation as cr
from paper_trader.engine import holding_opportunity_cost as k

REPO = Path(__file__).resolve().parents[1]
_ET = ZoneInfo("America/New_York")

BOOK = "alpha_paper_book_1"
SESSION = "2026-08-05"


# =========================================================================== #
# Hermetic opportunity-cost fixtures (the exact shape the Slice-6 owner builds)
# =========================================================================== #
def _pos(t, sec, w):
    return {"ticker": t, "sector": sec, "quantity": 100, "current_weight": w,
            "market_value": w * 100000.0, "price": 40.0, "target_weight": w}


def _urow(t, rank, score, sec):
    return {"ticker": t, "rank": rank, "score": score, "sector": sec,
            "adv_dollar": 5e7, "eligible": True}


def _ic(**over):
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


def _persist(d, ic=None):
    ic = ic if ic is not None else _ic()
    res = k.build_assessment(input_contract=ic, policy=hoc.resolve_policy())
    return hoc.persist_assessment(result=res, input_contract=ic, hoc_dir=str(d)), res


# =========================================================================== #
# A1. ONE SPELLING OF THE BOUND ASSESSMENT HASH
# =========================================================================== #
def test_01_bound_hash_is_the_stored_one_not_the_recomputed_one():
    """The live defect, in one assertion.

    A REUSE outcome leaves the caller's freshly derived document unwritten. The
    dependency hash a consumer publishes must therefore be the STORE's.
    """
    binding = {"hoc_assessment_hash": "STORED",
               "hoc_recomputed_assessment_hash": "DERIVED"}
    assert hoc.bound_assessment_hash(binding, {"assessment_hash": "DERIVED"}) \
        == "STORED"
    assert hoc.recomputed_assessment_hash(binding, {}) == "DERIVED"


def test_02_no_binding_falls_back_to_the_assessment_and_admits_nothing_else():
    """A run that never reached the persistence owner records its own hash and
    reports NO re-derivation — absence is not a mismatch."""
    assert hoc.bound_assessment_hash(None, {"assessment_hash": "OWN"}) == "OWN"
    assert hoc.recomputed_assessment_hash(None, {"assessment_hash": "OWN"}) is None
    assert hoc.bound_assessment_hash(None, None) is None


def test_03_identical_hashes_report_no_recomputation():
    b = {"hoc_assessment_hash": "H", "hoc_recomputed_assessment_hash": "H"}
    assert hoc.recomputed_assessment_hash(b, {}) is None


def test_04_event_cycle_publishes_the_stored_hash(tmp_path):
    """The exact live failure path: the cycle's HOC summary must not publish the
    transient document's hash beside the stored artifact's id."""
    p1, _ = _persist(tmp_path)
    result = {"assessment": {"assessment_hash": "TRANSIENT_REDERIVATION",
                             "assessment_state": hoc.STATE_READY},
              "persistence": {"status": hoc.PERSIST_REUSED},
              "binding": {"hoc_artifact_id": p1["artifact_id"],
                          "hoc_assessment_hash": p1["identity"]["assessment_hash"],
                          "hoc_persisted": True,
                          "hoc_reused_recomputed_document": True,
                          "hoc_recomputed_assessment_hash":
                              "TRANSIENT_REDERIVATION"}}
    summary = esr._summarize_hoc(result)
    assert summary["assessment_hash"] == p1["identity"]["assessment_hash"]
    assert summary["artifact_id"] == p1["artifact_id"]
    # The re-derivation stays VISIBLE — nothing about the reuse is hidden.
    assert summary["recomputed_assessment_hash"] == "TRANSIENT_REDERIVATION"
    assert summary["derived_assessment_hash"] == "TRANSIENT_REDERIVATION"
    assert summary["reused_recomputed_document"] is True


def test_05_reassessment_input_contract_publishes_the_stored_hash():
    ic = prs.build_input_contract(
        portfolio_state={"active_book": {"book_id": BOOK},
                         "dates": {"eligible_market_date": SESSION}},
        scoring={}, hoc_assessment={"assessment_hash": "DERIVED"},
        hoc_binding={"hoc_artifact_id": "hoc_x",
                     "hoc_assessment_hash": "STORED",
                     "hoc_recomputed_assessment_hash": "DERIVED",
                     "hoc_persisted": True})
    assert ic["hoc_assessment_hash"] == "STORED"
    assert ic["hoc_artifact_id"] == "hoc_x"
    assert ic["hoc_recomputed_assessment_hash"] == "DERIVED"


def test_06_a_governance_resolution_matches_after_the_fix(tmp_path):
    """End to end: persist, reuse with a different derived hash, and prove the
    published claim resolves against the immutable store."""
    p1, _ = _persist(tmp_path)
    claimed = {"hoc_artifact_id": p1["artifact_id"],
               "hoc_assessment_hash": hoc.bound_assessment_hash(
                   {"hoc_assessment_hash": p1["identity"]["assessment_hash"],
                    "hoc_recomputed_assessment_hash": "DERIVED"}, None)}
    res = hoc.resolve_binding(binding=claimed, active_book_id=BOOK,
                              eligible_market_date=SESSION, hoc_dir=str(tmp_path))
    assert res["hoc_artifact_retrievable"] is True
    assert res["hoc_artifact_identity_matches"] is True


def test_06b_all_three_consumers_publish_the_same_dependency_hash():
    """The identity has THREE consumers, and a fix that repaired two of them
    would simply move the refusal.

    The event cycle, the reassessment and the reallocation proposal each publish
    ``hoc_assessment_hash``, and the governed gate cross-checks the proposal's
    against the candidate's (``TARGET_BOUND_TO_SAME_HOC``). Repairing only the
    first two would have traded HOC_ARTIFACT_IDENTITY_MISMATCH for
    HOC_IDENTITY_MISMATCH — a different withheld reason for the same
    non-problem. One owner, one spelling, three consumers.
    """
    from paper_trader.api import reallocation_proposal as rpm

    binding = {"hoc_artifact_id": "hoc_x", "hoc_assessment_hash": "STORED",
               "hoc_recomputed_assessment_hash": "DERIVED",
               "hoc_reused_recomputed_document": True, "hoc_persisted": True}
    assessment = {"assessment_hash": "DERIVED", "assessment_state": hoc.STATE_READY}

    cycle = esr._summarize_hoc({"assessment": assessment, "persistence": {},
                               "binding": binding})["assessment_hash"]
    reassessment = prs.build_input_contract(
        portfolio_state={"active_book": {"book_id": BOOK},
                         "dates": {"eligible_market_date": SESSION}},
        scoring={}, hoc_assessment=assessment,
        hoc_binding=binding)["hoc_assessment_hash"]
    proposal = rpm.build_input_contract(
        portfolio_state={"active_book": {"book_id": BOOK},
                         "dates": {"eligible_market_date": SESSION}},
        scoring={}, hoc_assessment=assessment,
        hoc_binding=binding)["hoc_assessment_hash"]

    assert cycle == reassessment == proposal == "STORED"
    # And the gate's cross-check between the proposal and the candidate passes.
    assert pdec._eq_when_known(proposal, cycle) is True


def test_06c_the_proposal_records_the_re_derivation_as_audit_only():
    from paper_trader.api import reallocation_proposal as rpm
    ic = rpm.build_input_contract(
        portfolio_state={"active_book": {"book_id": BOOK},
                         "dates": {"eligible_market_date": SESSION}},
        scoring={}, hoc_assessment={"assessment_hash": "DERIVED"},
        hoc_binding={"hoc_assessment_hash": "STORED",
                     "hoc_recomputed_assessment_hash": "DERIVED"})
    assert ic["hoc_assessment_hash"] == "STORED"
    assert ic["hoc_recomputed_assessment_hash"] == "DERIVED"


def test_06d_a_proposal_with_no_binding_still_records_its_own_hash():
    """A producer that never reached the persistence owner records the honest
    answer, exactly as before R61 — nothing is defaulted into existence."""
    from paper_trader.api import reallocation_proposal as rpm
    ic = rpm.build_input_contract(
        portfolio_state={"active_book": {"book_id": BOOK},
                         "dates": {"eligible_market_date": SESSION}},
        scoring={}, hoc_assessment={"assessment_hash": "OWN"})
    assert ic["hoc_assessment_hash"] == "OWN"
    assert ic["hoc_recomputed_assessment_hash"] is None


def test_06e_the_replay_records_the_same_dependency_the_live_cycle_does():
    """Replay must not keep reproducing the defect the live cycle no longer has.

    ``api.event_replay`` wires the REAL owners to a synthetic world, so its
    seams are production code, not test doubles. R54.3 already threads the
    opportunity-cost binding into the reassessment seam for this reason. If the
    proposal seam did not accept it too, a replayed cycle would go on
    re-deriving ``hoc_assessment_hash`` from the transient document while the
    live cycle published the stored one — and the harness used to prove the
    orchestrator's behaviour would be proving the wrong behaviour.
    """
    import inspect
    from paper_trader.api import event_replay as erp

    seams = erp._real_owner_seams(
        {"portfolio_state": {}, "price_panel": None},
        {"hoc": "h", "reassess": "r", "realloc": "p", "fabric": "f"})

    for name in ("reassessment_fn", "proposal_fn"):
        params = inspect.signature(seams[name]).parameters
        assert "hoc_binding" in params, name
        # Injected, never positional — the seam stays keyword-only.
        assert params["hoc_binding"].kind is inspect.Parameter.KEYWORD_ONLY, name

    # And the live intraday producer hands it to BOTH seams, unconditionally.
    # Asserted STRUCTURALLY: a source-text scan for the spelling would be
    # defeated by a line break, and this property is about the argument
    # actually being passed, not about how it is written.
    import ast
    tree = ast.parse((REPO / "api/event_signal_refresh.py").read_text(
        encoding="utf-8"))
    passed_by_seam: dict[str, set] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            passed_by_seam.setdefault(node.func.id, set()).update(
                k.arg for k in node.keywords)
    for seam in ("reassess_call", "proposal_call"):
        assert seam in passed_by_seam, sorted(passed_by_seam)
        assert "hoc_binding" in passed_by_seam[seam], seam


# =========================================================================== #
# A2. THE EXACT ARTIFACT, RESOLVED BY CONTENT ACROSS THE WHOLE CHAIN
# =========================================================================== #
def test_07_exact_by_hash_resolves_an_earlier_version(tmp_path):
    v1, res1 = _persist(tmp_path)
    v2, _ = _persist(tmp_path, _ic(universe_scoring_hash="USHASH_V2"))
    assert v2["artifact_id"] != v1["artifact_id"]
    got = hoc.load_artifact_by_assessment_hash(
        assessment_hash=v1["identity"]["assessment_hash"],
        active_book_id=BOOK, eligible_market_date=SESSION, hoc_dir=str(tmp_path))
    assert got["artifact_id"] == v1["artifact_id"]


def test_08_a_hash_no_version_holds_resolves_to_nothing(tmp_path):
    """FAIL CLOSED: an unheld hash never falls back to the latest artifact."""
    _persist(tmp_path)
    assert hoc.load_artifact_by_assessment_hash(
        assessment_hash="NEVER_WRITTEN", active_book_id=BOOK,
        eligible_market_date=SESSION, hoc_dir=str(tmp_path)) is None
    assert hoc.load_artifact_by_assessment_hash(
        assessment_hash="", active_book_id=BOOK,
        eligible_market_date=SESSION, hoc_dir=str(tmp_path)) is None


def test_09_reassessment_binds_the_earlier_version_it_consumed(tmp_path):
    """R54.3's designed multi-version session must not strand a consumer.

    Before R61 ``resolve_hoc_binding`` compared the consumed hash against the
    session's LATEST artifact only, so a reassessment that consumed v1 reported
    "no artifact is persisted" while v1 sat retrievable on disk.
    """
    v1, res1 = _persist(tmp_path)
    _persist(tmp_path, _ic(universe_scoring_hash="USHASH_V2"))
    binding = prs.resolve_hoc_binding(
        hoc_assessment=res1, active_book_id=BOOK,
        eligible_market_date=SESSION, hoc_dir=str(tmp_path))
    assert binding["hoc_artifact_id"] == v1["artifact_id"]
    assert binding["hoc_persisted"] is True
    assert binding["hoc_artifact_retrievable"] is True
    assert binding["hoc_artifact_identity_matches"] is True


def test_10_latest_hoc_cannot_substitute_for_a_bound_hoc(tmp_path):
    """A LATER artifact must never validate a binding it does not carry."""
    v1, _ = _persist(tmp_path)
    v2, _ = _persist(tmp_path, _ic(universe_scoring_hash="USHASH_V2"))
    # Claim v1's id with v2's hash: two real artifacts, one impossible pairing.
    res = hoc.resolve_binding(
        binding={"hoc_artifact_id": v1["artifact_id"],
                 "hoc_assessment_hash": v2["identity"]["assessment_hash"]},
        active_book_id=BOOK, eligible_market_date=SESSION, hoc_dir=str(tmp_path))
    assert res["hoc_artifact_retrievable"] is True
    assert res["hoc_artifact_identity_matches"] is False
    assert "MISMATCH" in res["hoc_binding_detail"]


def test_11_a_transient_assessment_still_fails_closed(tmp_path):
    """An assessment that was never written stays visible AS transient."""
    _persist(tmp_path)
    binding = prs.resolve_hoc_binding(
        hoc_assessment={"assessment_hash": "ONLY_EVER_IN_MEMORY"},
        active_book_id=BOOK, eligible_market_date=SESSION, hoc_dir=str(tmp_path))
    assert binding["hoc_persisted"] is False
    assert binding["hoc_artifact_retrievable"] is False
    assert binding["hoc_artifact_identity_matches"] is False
    assert "not held by ANY version" in binding["hoc_binding_detail"]


def test_12_no_historical_artifact_is_rebound_or_rewritten(tmp_path):
    """Every R61 read is a read. v1's bytes are identical afterwards."""
    v1, res1 = _persist(tmp_path)
    before = Path(v1["path"]).read_bytes()
    index_before = (tmp_path / "index.json").read_bytes()
    _persist(tmp_path, _ic(universe_scoring_hash="USHASH_V2"))
    after_v2 = Path(v1["path"]).read_bytes()
    prs.resolve_hoc_binding(hoc_assessment=res1, active_book_id=BOOK,
                            eligible_market_date=SESSION, hoc_dir=str(tmp_path))
    hoc.load_artifact_by_assessment_hash(
        assessment_hash=v1["identity"]["assessment_hash"], active_book_id=BOOK,
        eligible_market_date=SESSION, hoc_dir=str(tmp_path))
    assert Path(v1["path"]).read_bytes() == before == after_v2
    assert index_before != (tmp_path / "index.json").read_bytes(), \
        "the v2 write should have appended to the index"


def test_13_retrying_the_same_logical_cycle_is_idempotent(tmp_path):
    """An idempotent retry must not produce conflicting candidate lineage."""
    first, res = _persist(tmp_path)
    again, _ = _persist(tmp_path)
    assert again["status"] == hoc.PERSIST_REUSED
    assert again["artifact_id"] == first["artifact_id"]
    b1 = prs.resolve_hoc_binding(hoc_assessment=res, active_book_id=BOOK,
                                 eligible_market_date=SESSION,
                                 hoc_dir=str(tmp_path))
    b2 = prs.resolve_hoc_binding(hoc_assessment=res, active_book_id=BOOK,
                                 eligible_market_date=SESSION,
                                 hoc_dir=str(tmp_path))
    assert b1["hoc_artifact_id"] == b2["hoc_artifact_id"] == first["artifact_id"]
    assert b1["hoc_assessment_hash"] == b2["hoc_assessment_hash"]


# =========================================================================== #
# A3. DUPLICATE-CANDIDATE DETECTION
# =========================================================================== #
BOOK_G, SESSION_G = "alpha_paper_book_1", "2026-08-31"
HELD_G = ["T%02d" % i for i in range(25)]
HOC_AID = "hoc_2026-08-31_alpha_paper_book_1_HOC1"
T1 = datetime(2026, 9, 1, 17, 42, 0, tzinfo=timezone.utc)


def _persisted_daily(**kw):
    d = {"record_id": "gdec_daily_1", "decision": pdec.GD_CHANGE_RECOMMENDED,
         "provenance": pdec.PROV_GOVERNED_DAILY_CYCLE,
         "decided_at": "2026-09-01T10:00:00+00:00",
         "eligible_market_session": SESSION_G, "active_book_id": BOOK_G,
         "candidate_identity_hash": "OLDHASH",
         "identity": {"active_book_id": BOOK_G,
                      "eligible_market_session": SESSION_G,
                      "reassessment_hash": "RA_OLD", "proposal_hash": "PR_OLD",
                      "target_outcome": cr.OUTCOME_PROPOSAL_READY}}
    d.update(kw)
    return d


def _projection(**kw):
    """The read-time shim, built from the CANDIDATE's own reassessment."""
    d = {"record_id": "drc_governed_run", "decision": pdec.GD_CHANGE_RECOMMENDED,
         "provenance": pdec.PROV_GOVERNED_DAILY_CYCLE,
         "decided_at": "2026-09-02T20:22:00+00:00",
         "eligible_market_session": SESSION_G, "active_book_id": BOOK_G,
         "projected": True, "legacy_compatibility_projection": True,
         "candidate_identity_hash": "NEWHASH",
         "identity": {"active_book_id": BOOK_G,
                      "eligible_market_session": SESSION_G,
                      "reassessment_hash": "RA_NEW", "proposal_hash": "PR_NEW",
                      "target_outcome": cr.OUTCOME_PROPOSAL_READY}}
    d.update(kw)
    return d


def _ledger(tmp_path, rows):
    (tmp_path / "governed_decisions.json").write_text(
        json.dumps(rows), encoding="utf-8")
    return tmp_path


def test_14_a_ledger_row_retires_the_projection_in_the_gate(tmp_path):
    """The live DUPLICATE_CANDIDATE defect.

    The gate compared its candidate against a projection derived from the very
    reassessment the candidate was built from — a self-comparison no new
    evidence could beat. The R54.4 retirement rule the READ has always applied
    now applies here too, through ONE shared resolver.
    """
    _ledger(tmp_path, [_persisted_daily()])
    res = pdec.resolve_standing_governed_decision(
        persisted=_persisted_daily(), projected=_projection(),
        decision_dir=str(tmp_path))
    assert res["legacy_daily_projection_suppressed"] is True
    assert res["suppression_reason"] == pdec.PROJECTION_RETIRED_BY_LEDGER_ROW
    assert res["standing"]["record_id"] == "gdec_daily_1"


def test_15_the_projection_still_stands_when_no_ledger_row_exists(tmp_path):
    """The compatibility shim is not removed — only retired by a real row."""
    _ledger(tmp_path, [])
    res = pdec.resolve_standing_governed_decision(
        persisted=None, projected=_projection(), decision_dir=str(tmp_path))
    assert res["legacy_daily_projection_suppressed"] is False
    assert res["standing"]["record_id"] == "drc_governed_run"


def test_16_a_genuinely_new_candidate_adds_new_evidence(tmp_path):
    """Different reassessment + proposal => NOT a duplicate."""
    _ledger(tmp_path, [_persisted_daily()])
    standing = pdec.resolve_standing_governed_decision(
        persisted=_persisted_daily(), projected=_projection(),
        decision_dir=str(tmp_path))["standing"]
    candidate_core = (BOOK_G, SESSION_G, "RA_NEW", "PR_NEW",
                      cr.OUTCOME_PROPOSAL_READY)
    assert candidate_core != pdec._core_evidence(standing["identity"])


def test_17_a_true_duplicate_is_still_a_duplicate(tmp_path):
    """FAIL CLOSED IN THE OTHER DIRECTION: identical evidence still collapses."""
    same = _persisted_daily(identity={
        "active_book_id": BOOK_G, "eligible_market_session": SESSION_G,
        "reassessment_hash": "RA_NEW", "proposal_hash": "PR_NEW",
        "target_outcome": cr.OUTCOME_PROPOSAL_READY})
    _ledger(tmp_path, [same])
    standing = pdec.resolve_standing_governed_decision(
        persisted=same, projected=_projection(),
        decision_dir=str(tmp_path))["standing"]
    candidate_core = (BOOK_G, SESSION_G, "RA_NEW", "PR_NEW",
                      cr.OUTCOME_PROPOSAL_READY)
    assert candidate_core == pdec._core_evidence(standing["identity"])


def test_18_the_gate_and_the_read_share_one_resolver():
    """Not two spellings of one rule — one function, used by both."""
    src = (REPO / "api" / "portfolio_decision.py").read_text(
        encoding="utf-8", errors="replace")
    assert src.count("def resolve_standing_governed_decision") == 1
    assert src.count("resolve_standing_governed_decision(") >= 3
    # The gate no longer computes its own max() over the two descriptions.
    assert "standing_rows = [r for r in (persisted_standing, projected_standing)" \
        not in src


# =========================================================================== #
# B1. THE CANONICAL BLOCKED-REASON TAXONOMY (asset-agnostic)
# =========================================================================== #
@pytest.mark.parametrize("asset_class", list(PA.PROVEN_ASSET_CLASSES))
def test_19_generative_exhaustion_classifies_identically_for_every_scope(
        asset_class):
    """The taxonomy is asset-agnostic BY CONSTRUCTION: the asset class travels
    as data and never as a branch."""
    out = BLK.classify(
        "generator produced no non-degenerate candidate for %s/SYMBOLIC"
        % asset_class, payload={"asset_class": asset_class})
    assert out["reason_code"] == BLK.FAMILY_EXHAUSTED
    assert out["asset_class"] == asset_class
    assert out["clears_on"] == BLK.CLEARS_ON_INFORMATION


def test_20_every_live_shaped_blocker_has_a_canonical_reason():
    cases = {
        "engine returned NO_MEMBERS": BLK.DEPENDENCY_BLOCKED,
        "no equity feature declared for family X": BLK.DEPENDENCY_BLOCKED,
        "blocker unchanged (DATA_INCOMPLETE); no owned reader closes it and no "
        "purchase is permitted": BLK.WAITING_FOR_EXTERNAL_ENTITLEMENT,
        "the market session has not completed": BLK.WAITING_FOR_MARKET_SESSION,
        "awaiting forward evidence": BLK.WAITING_FOR_FORWARD_EVIDENCE,
        "effective sample below the floor": BLK.WAITING_FOR_SAMPLE,
        "compute budget exhausted": BLK.COMPUTE_GATE,
        "the premise was invalidated": BLK.INVALIDATED,
        "superseded by a later artifact": BLK.SUPERSEDED,
    }
    for text, expected in cases.items():
        assert BLK.classify(text)["reason_code"] == expected, text


def test_21_an_unknown_blocker_is_never_folded_into_a_neighbour():
    out = BLK.classify("a sentence nobody has ever written before")
    assert out["reason_code"] == BLK.UNCLASSIFIED_BLOCKER
    assert out["recorded_reason"] == "a sentence nobody has ever written before"
    assert out["matched_on"] is None


def test_22_summarise_groups_by_reason_clearance_and_asset_class():
    rows = [BLK.classify("no non-degenerate candidate",
                         payload={"asset_class": "RATES_FUTURES"}),
            BLK.classify("engine returned NO_MEMBERS",
                         payload={"asset_class": "CROSS_ASSET"})]
    s = BLK.summarise(rows)
    assert s["blocked_total"] == 2
    assert s["by_reason"] == {BLK.FAMILY_EXHAUSTED: 1, BLK.DEPENDENCY_BLOCKED: 1}
    assert s["by_asset_class"] == {"RATES_FUTURES": 1, "CROSS_ASSET": 1}
    assert s["every_blocker_is_classified"] is True
    assert s["unclassified_count"] == 0


def test_23_classify_job_reads_a_queue_row_shape():
    row = {"job_id": "job_1", "category": "EXPERIMENT",
           "lane": "r59.cross_asset.cross_asset", "state": "BLOCKED_SPECIFIC",
           "blocked_reason": "engine returned NO_MEMBERS",
           "payload_json": json.dumps({"asset_class": "CROSS_ASSET",
                                       "mandate_id": "M59_x"})}
    out = BLK.classify_job(row)
    assert out["reason_code"] == BLK.DEPENDENCY_BLOCKED
    assert out["job_id"] == "job_1"
    assert out["asset_class"] == "CROSS_ASSET"
    assert out["mandate_id"] == "M59_x"


def _executable_source(rel: str) -> str:
    """The module's CODE, with docstrings and comments removed.

    A prose sentence that says "this never reads a ticker" must not fail a scan
    looking for the word "ticker" in the logic.
    """
    import ast
    import io as _io
    import tokenize
    text = (REPO / rel).read_text(encoding="utf-8", errors="replace")
    tree = ast.parse(text)
    doc_lines: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef,
                                 ast.AsyncFunctionDef)):
            continue
        body = getattr(node, "body", None) or []
        first = body[0] if body else None
        if (isinstance(first, ast.Expr)
                and isinstance(first.value, ast.Constant)
                and isinstance(first.value.value, str)):
            doc_lines.update(range(first.lineno, (first.end_lineno or
                                                  first.lineno) + 1))
    out = []
    for i, line in enumerate(text.splitlines(), start=1):
        if i in doc_lines:
            continue
        out.append(line)
    stripped = "\n".join(out)
    # Drop comments too, without disturbing string literals.
    kept = []
    try:
        for tok in tokenize.generate_tokens(_io.StringIO(stripped).readline):
            if tok.type == tokenize.COMMENT:
                continue
            kept.append(tok.string)
    except (tokenize.TokenError, IndentationError):
        return stripped
    return " ".join(kept)


def test_24_the_taxonomy_contains_no_equity_specific_concept():
    """Asset-agnostic in the LOGIC, not merely in the documentation."""
    code = _executable_source("alpha_agent/r59/blockers.py").lower()
    for banned in ("ticker", "symbol", "cusip", "sedol", "equity_only"):
        assert banned not in code, banned


# =========================================================================== #
# B2. TRUTHFUL WAIT / WAKE SEMANTICS
# =========================================================================== #
def test_25_a_worker_with_work_says_it_is_researching_and_waits_for_nothing():
    plan = R59RT.plan_sleep(ready_work=3, conditions=[])
    assert plan["state"] == R59RT.W_RESEARCHING
    assert plan["sleep_seconds"] == 0.0
    assert plan["wake_condition"] is None


def test_26_a_blocked_frontier_names_its_state_and_its_wake_condition():
    plan = R59RT.plan_sleep(
        ready_work=0, conditions=[],
        blocked_summary={"by_reason": {BLK.FAMILY_EXHAUSTED: 14,
                                       BLK.DEPENDENCY_BLOCKED: 3},
                         "time_will_clear": 0, "blocked_total": 17})
    assert plan["state"] == R59RT.W_FRONTIER_EXHAUSTED
    assert plan["blocker_reason"] == BLK.FAMILY_EXHAUSTED
    assert plan["blocker_reasons"] == {BLK.FAMILY_EXHAUSTED: 14,
                                       BLK.DEPENDENCY_BLOCKED: 3}
    assert plan["wake_condition"] == "NEW_INFORMATION_OR_AN_OPERATOR_DECISION"
    # No busy polling on a blocker time cannot clear.
    assert plan["sleep_seconds"] == R59RT.MAX_SLEEP_SECONDS


def test_27_a_session_blocker_waits_for_the_session_not_for_information():
    plan = R59RT.plan_sleep(
        ready_work=0, conditions=[],
        blocked_summary={"by_reason": {BLK.WAITING_FOR_MARKET_SESSION: 2},
                         "time_will_clear": 2, "blocked_total": 2})
    assert plan["state"] == R59RT.W_WAITING_MARKET
    assert plan["wake_condition"] == "AN_ELAPSED_MARKET_SESSION"


def test_28_an_empty_frontier_is_never_reported_as_generic_sleeping():
    plan = R59RT.plan_sleep(ready_work=0, conditions=[])
    assert plan["state"] == R59RT.W_FRONTIER_EXHAUSTED
    assert plan["state"] in R59RT.WAITING_STATES
    assert plan["wake_condition"]


def test_29_every_waiting_state_is_in_the_worker_vocabulary():
    for s in R59RT.WAITING_STATES:
        assert s in R59RT.WORKER_STATES
    for reason in BLK.BLOCKER_REASONS:
        assert R59RT.waiting_state_for(reason) in R59RT.WORKER_STATES


def test_30_a_busy_cycle_publishes_a_live_reason_not_the_boot_placeholder():
    """The runtime published ``STARTING`` for the whole life of a worker whose
    first cycle never ended. A research cycle now publishes its own reason."""
    src = (REPO / "alpha_agent" / "r59" / "runtime.py").read_text(
        encoding="utf-8", errors="replace")
    assert "def _researching_plan" in src
    assert "sleep_plan = _researching_plan(iteration)" in src
    assert 'RESEARCH_IN_PROGRESS = "EXECUTING_RESEARCH"' in src
    # The status body carries what would end the wait.
    assert '"wake_condition": sleep_plan.get("wake_condition")' in src


# =========================================================================== #
# B3. NO BUSY LOOP
# =========================================================================== #
def test_31_a_probe_whose_substrate_is_unchanged_is_not_informative():
    from paper_trader.alpha_agent.r59 import opportunities as OPP

    class _Mem:
        def __init__(self):
            self._meta = {}

        def opportunities(self):
            return [{"opportunity_id": "OWNED_NEWS_EVENT", "state":
                     "ALREADY_OWNED_UNUSED", "pit_integrity": "DATA_INCOMPLETE",
                     "detail": {"r58_classification": "DATA_INCOMPLETE"},
                     "effective_sample": "n/a"}]

        def get_meta(self, key, default=None):
            return self._meta.get(key, default)

        def set_meta(self, key, value):
            self._meta[key] = value

    mem = _Mem()
    # First look: no probe recorded, so the work is issuable.
    first = OPP.probe_is_informative(mem, opportunity_id="OWNED_NEWS_EVENT")
    assert first["informative"] is True
    # Record a probe, then ask again with nothing moved.
    OPP.record_probe(mem, opportunity_id="OWNED_NEWS_EVENT", resolved=False)
    second = OPP.probe_is_informative(mem, opportunity_id="OWNED_NEWS_EVENT")
    assert second["informative"] is False
    assert "unchanged" in second["reason"]
    # And it becomes issuable again the instant the substrate moves.
    mem.opportunities = lambda: [
        {"opportunity_id": "OWNED_NEWS_EVENT", "state": "ALREADY_OWNED_UNUSED",
         "pit_integrity": "DATA_NOW_READABLE",
         "detail": {"r58_classification": "DATA_NOW_READABLE"},
         "effective_sample": "n/a"}]
    assert OPP.probe_is_informative(
        mem, opportunity_id="OWNED_NEWS_EVENT")["informative"] is True


def test_32_an_unknown_opportunity_is_never_suppressed():
    from paper_trader.alpha_agent.r59 import opportunities as OPP

    class _Mem:
        def opportunities(self):
            return []

        def get_meta(self, key, default=None):
            return {"watermark": "anything"}

        def set_meta(self, key, value):
            pass

    out = OPP.probe_is_informative(_Mem(), opportunity_id="NOT_A_REAL_ONE")
    assert out["informative"] is True


def test_33_the_generative_seed_advances_with_the_search():
    """The symbolic fixpoint: a duplicate does not count to burden, so a seed
    derived from burden alone re-derived the same expression forever."""
    from paper_trader.alpha_agent.r59 import governor as G

    class _Mem:
        def __init__(self, drawn):
            self._drawn = drawn

        def generator_yield(self, asset_class=None):
            return [{"asset_class": "RATES_FUTURES", "kind": "SYMBOLIC_TREE",
                     "generated": self._drawn, "novel": 0,
                     "duplicates": self._drawn}]

    a = G._generative_draws(_Mem(120), "RATES_FUTURES", "SYMBOLIC")
    b = G._generative_draws(_Mem(240), "RATES_FUTURES", "SYMBOLIC")
    assert a == 120 and b == 240
    from paper_trader.alpha_agent import r59 as r59mod
    seed_a = 5900 + int(r59mod.short_hash(
        ["RATES_FUTURES", "SYMBOLIC", 7, a], 8), 16) % 90000
    seed_b = 5900 + int(r59mod.short_hash(
        ["RATES_FUTURES", "SYMBOLIC", 7, b], 8), 16) % 90000
    assert seed_a != seed_b, "the search must move even when burden does not"
    # Deterministic and replayable: the same persisted state gives the same seed.
    assert seed_a == 5900 + int(r59mod.short_hash(
        ["RATES_FUTURES", "SYMBOLIC", 7, 120], 8), 16) % 90000


def test_34_a_missing_yield_ledger_reproduces_the_previous_seed():
    from paper_trader.alpha_agent.r59 import governor as G

    class _Mem:
        def generator_yield(self, asset_class=None):
            return []

    assert G._generative_draws(_Mem(), "FX_FUTURES", "AUTO") == 0


# =========================================================================== #
# C. FREEZE LIFECYCLE
# =========================================================================== #
WITHDRAWN_REASON = (
    "WITHDRAWN AT INCEPTION: the historical candidate this challenger was "
    "frozen from no longer qualifies. Two gate defects were found and fixed "
    "after the freeze. ZERO forward observations had accrued.")


def _freeze(**kw):
    d = {"hypothesis_id": "H_abc_1", "release": "R58", "asset_class": "US_EQUITY",
         "economic_family": "R58_SHORT_VOLUME_PRESSURE_V1", "horizon_sessions": 21,
         "model_family": "RANK_TOPN", "input_data_identity": "r58_challenger_freeze",
         "outcome": "FORWARD_FROZEN", "evidence_maturity": "PROSPECTIVE_INCEPTION",
         "invalidated_reason": None,
         "spec_json": json.dumps({"substrate": "r58", "instrument_scope": ["AAA"]}),
         "forward_challenger": json.dumps(
             {"challenger_id": "R58_SHORT_VOLUME_PRESSURE_V1",
              "inception": "2026-09-03", "record_hash": "RH1",
              "forward_observations_at_freeze": 0})}
    d.update(kw)
    return d


def test_35_an_untouched_freeze_is_active():
    lc = PA.classify_lifecycle(_freeze())
    assert lc["lifecycle_state"] == PA.LC_ACTIVE
    assert lc["adoptable"] is True
    assert lc["never_resurrectable"] is False


def test_36_a_withdrawn_freeze_is_withdrawn_and_never_resurrectable():
    lc = PA.classify_lifecycle(_freeze(invalidated_reason=WITHDRAWN_REASON))
    assert lc["lifecycle_state"] == PA.LC_WITHDRAWN
    assert lc["adoptable"] is False
    assert lc["never_resurrectable"] is True


def test_37_an_invalidated_freeze_is_invalidated():
    lc = PA.classify_lifecycle(
        _freeze(invalidated_reason="the input feature was meaningless"))
    assert lc["lifecycle_state"] == PA.LC_INVALIDATED
    assert lc["adoptable"] is False
    assert lc["never_resurrectable"] is True


def test_38_a_later_freeze_of_the_same_id_supersedes():
    lc = PA.classify_lifecycle(_freeze(), later_freezes_with_same_id=1)
    assert lc["lifecycle_state"] == PA.LC_SUPERSEDED
    assert lc["adoptable"] is False


def test_39_a_forward_verdict_ends_the_lifecycle():
    assert PA.classify_lifecycle(
        _freeze(), forward_verdict="KILLED")["lifecycle_state"] == PA.LC_FAILED
    assert PA.classify_lifecycle(
        _freeze(), forward_verdict="MATURED")["lifecycle_state"] == PA.LC_MATURED


def test_40_withdrawal_outranks_every_other_signal():
    """A withdrawn freeze that is also superseded is still WITHDRAWN — the most
    terminal fact wins, so nothing can launder it back into an adoptable state."""
    lc = PA.classify_lifecycle(_freeze(invalidated_reason=WITHDRAWN_REASON),
                               later_freezes_with_same_id=3,
                               forward_verdict="MATURED")
    assert lc["lifecycle_state"] == PA.LC_WITHDRAWN


def test_41_only_active_is_adoptable():
    assert PA.ADOPTABLE_STATES == (PA.LC_ACTIVE,)
    assert set(PA.ADOPTABLE_STATES) < set(PA.LIFECYCLE_STATES)


# =========================================================================== #
# D. ATOMIC (RECOVERABLE) FREEZE -> FORWARD REGISTRATION
# =========================================================================== #
def _ok_registrar(calls):
    def _fn(*, identity, observation_clock_starts, challenger_class):
        calls.append({"identity": identity,
                      "observation_clock_starts": observation_clock_starts,
                      "challenger_class": challenger_class})
        return {"registered_with": "test.registrar",
                "challenger_id": identity["challenger_id"]}
    return _fn


def test_42_adoption_requires_an_explicit_confirmation():
    out = PA.adopt_prospective_freeze(
        freeze_row=_freeze(), observation_clock_starts="2026-09-08")
    assert out["outcome"] == PA.REFUSED_CONFIRMATION
    assert out["adopted"] is False


def test_43_a_withdrawn_freeze_can_never_be_adopted(tmp_path):
    calls = []
    out = PA.adopt_prospective_freeze(
        freeze_row=_freeze(invalidated_reason=WITHDRAWN_REASON),
        observation_clock_starts="2026-09-08",
        confirm=PA.ADOPT_CONFIRM_TOKEN, registrar=_ok_registrar(calls),
        adoption_dir_override=str(tmp_path))
    assert out["outcome"] == PA.REFUSED_LIFECYCLE
    assert out["adopted"] is False
    assert out["never_resurrectable"] is True
    assert calls == [], "the registrar must never be reached"
    assert list(tmp_path.rglob("*.json")) == [], "nothing may be written"


def test_44_an_invalidated_freeze_can_never_be_registered(tmp_path):
    calls = []
    out = PA.adopt_prospective_freeze(
        freeze_row=_freeze(invalidated_reason="the input was meaningless"),
        observation_clock_starts="2026-09-08",
        confirm=PA.ADOPT_CONFIRM_TOKEN, registrar=_ok_registrar(calls),
        adoption_dir_override=str(tmp_path))
    assert out["outcome"] == PA.REFUSED_LIFECYCLE
    assert calls == []


def test_45_an_active_freeze_may_be_adopted_only_prospectively(tmp_path):
    calls = []
    out = PA.adopt_prospective_freeze(
        freeze_row=_freeze(), observation_clock_starts="2026-09-01",
        confirm=PA.ADOPT_CONFIRM_TOKEN, registrar=_ok_registrar(calls),
        adoption_dir_override=str(tmp_path))
    assert out["outcome"] == PA.REFUSED_BACKDATED
    assert out["adopted"] is False
    assert calls == []


def test_46_the_observation_clock_starts_today_not_at_inception(tmp_path):
    calls = []
    out = PA.adopt_prospective_freeze(
        freeze_row=_freeze(), observation_clock_starts="2026-09-08",
        confirm=PA.ADOPT_CONFIRM_TOKEN, registrar=_ok_registrar(calls),
        adoption_dir_override=str(tmp_path))
    assert out["outcome"] == PA.ADOPTED
    assert calls[0]["observation_clock_starts"] == "2026-09-08"
    # NOT the inception, and no session between them is synthesised.
    assert calls[0]["identity"]["inception"] == "2026-09-03"
    assert out["safety"]["backfilled_forward_evidence"] is False


def test_47_no_forward_evidence_is_written_by_the_adoption_owner(tmp_path):
    PA.adopt_prospective_freeze(
        freeze_row=_freeze(), observation_clock_starts="2026-09-08",
        confirm=PA.ADOPT_CONFIRM_TOKEN, registrar=_ok_registrar([]),
        adoption_dir_override=str(tmp_path))
    written = sorted(p.name for p in tmp_path.rglob("*.json"))
    assert len(written) == 1, written
    body = json.loads((tmp_path / "intents" / written[0]).read_text(
        encoding="utf-8"))
    # An INTENT, not an observation, a prediction or an outcome.
    assert body["phase"] == PA.INTENT_COMMITTED
    assert "observation" not in json.dumps(body).replace(
        "observation_clock_starts", "")


def test_48_adoption_is_idempotent(tmp_path):
    calls = []
    first = PA.adopt_prospective_freeze(
        freeze_row=_freeze(), observation_clock_starts="2026-09-08",
        confirm=PA.ADOPT_CONFIRM_TOKEN, registrar=_ok_registrar(calls),
        adoption_dir_override=str(tmp_path))
    again = PA.adopt_prospective_freeze(
        freeze_row=_freeze(), observation_clock_starts="2026-09-09",
        confirm=PA.ADOPT_CONFIRM_TOKEN, registrar=_ok_registrar(calls),
        adoption_dir_override=str(tmp_path))
    assert first["outcome"] == PA.ADOPTED
    assert again["outcome"] == PA.ALREADY_ADOPTED
    assert again["idempotent"] is True
    assert len(calls) == 1, "the registrar must be called exactly once"
    assert len(list((tmp_path / "intents").glob("*.json"))) == 1


def test_49_a_crash_between_the_halves_leaves_a_resumable_intent(tmp_path):
    """True cross-store atomicity does not exist here and is not claimed. What
    IS guaranteed is that no state a crash can leave is silent."""
    def _explodes(**_kw):
        raise RuntimeError("the registrar died mid-write")

    out = PA.adopt_prospective_freeze(
        freeze_row=_freeze(), observation_clock_starts="2026-09-08",
        confirm=PA.ADOPT_CONFIRM_TOKEN, registrar=_explodes,
        adoption_dir_override=str(tmp_path))
    assert out["outcome"] == PA.REGISTRAR_FAILED
    assert out["adopted"] is False
    assert out["intent_is_resumable"] is True
    open_ = PA.open_intents(str(tmp_path))
    assert len(open_) == 1
    assert open_[0]["phase"] == PA.INTENT_OPEN


def test_50_retrying_after_a_crash_does_not_duplicate_the_registration(tmp_path):
    def _explodes(**_kw):
        raise RuntimeError("boom")

    PA.adopt_prospective_freeze(
        freeze_row=_freeze(), observation_clock_starts="2026-09-08",
        confirm=PA.ADOPT_CONFIRM_TOKEN, registrar=_explodes,
        adoption_dir_override=str(tmp_path))
    calls = []
    retry = PA.adopt_prospective_freeze(
        freeze_row=_freeze(), observation_clock_starts="2026-09-08",
        confirm=PA.ADOPT_CONFIRM_TOKEN, registrar=_ok_registrar(calls),
        adoption_dir_override=str(tmp_path))
    assert retry["outcome"] == PA.ADOPTED
    assert len(calls) == 1
    assert PA.open_intents(str(tmp_path)) == []
    assert len(list((tmp_path / "intents").glob("*.json"))) == 1


def test_51_an_unregistrable_class_records_a_named_resumable_gap(
        tmp_path, monkeypatch):
    """FAIL CLOSED: no canonical owner means no registration, and the gap is a
    durable record rather than an invisible orphan.

    Release 62.1 closed the gap this test was written against - an R58 freeze now
    routes to the canonical registrar - but the MACHINERY must stay reachable,
    because recording a named resumable gap instead of inventing an owner is what
    kept five orphans from being invisible. The class is unmapped here
    deliberately, which is the only condition that can still produce it.
    """
    monkeypatch.setattr(PA, "classify_challenger_class",
                        lambda _row: PA.CLASS_SIGNAL_UNREGISTERED)
    out = PA.adopt_prospective_freeze(
        freeze_row=_freeze(), observation_clock_starts="2026-09-08",
        confirm=PA.ADOPT_CONFIRM_TOKEN, adoption_dir_override=str(tmp_path))
    assert out["outcome"] == PA.NO_CANONICAL_REGISTRAR
    assert out["adopted"] is False
    assert out["challenger_class"] == PA.CLASS_SIGNAL_UNREGISTERED
    assert out["canonical_registrar"] is None
    open_ = PA.open_intents(str(tmp_path))
    assert len(open_) == 1
    assert open_[0]["blocked_reason"] == PA.NO_CANONICAL_REGISTRAR


def test_52_a_freeze_that_names_nothing_is_refused(tmp_path):
    out = PA.adopt_prospective_freeze(
        freeze_row={"release": "R58"}, observation_clock_starts="2026-09-08",
        confirm=PA.ADOPT_CONFIRM_TOKEN, registrar=_ok_registrar([]),
        adoption_dir_override=str(tmp_path))
    assert out["outcome"] == PA.REFUSED_IDENTITY
    assert set(out["missing_identity_fields"]) >= {"challenger_id", "freeze_id"}


def test_53_the_r59_freeze_path_delegates_to_the_adoption_owner():
    """Freeze and forward registration are ONE governed operation.

    The adoption owner is INJECTED, not imported: the research package may not
    depend on the application layer (an R59 invariant), so the seam follows the
    same composition-root pattern as the revision reader.
    """
    src = (REPO / "alpha_agent" / "r59" / "handlers.py").read_text(
        encoding="utf-8", errors="replace")
    assert "_register_forward_evidence" in src
    assert '"forward_evidence_started"' in src
    assert "adopt_forward" in src
    # The research package imports NO application module.
    for banned in ("from paper_trader.api import", "from api import",
                   "from api."):
        assert banned not in src, banned
    runner = (REPO / "scripts" / "run_research_runtime.py").read_text(
        encoding="utf-8", errors="replace")
    assert "prospective_adoption" in runner
    assert "adopt_forward=adopter" in runner


def test_53b_an_uninjected_worker_says_the_forward_half_did_not_start():
    """FAIL LOUD, NOT SILENT: the exact failure mode that produced five orphans
    is now a named state rather than nothing at all."""
    from paper_trader.alpha_agent.r59 import handlers as H

    class _Mem:
        def get(self, _hid):
            return _freeze()

    out = H._register_forward_evidence(_Mem(), "H_abc_1", None)
    assert out["adopted"] is False
    assert out["outcome"] == H.FORWARD_ADOPTION_NOT_WIRED
    assert "has NOT started" in out["detail"]


def test_53c_an_injected_adopter_is_called_with_a_prospective_clock():
    from paper_trader.alpha_agent.r59 import handlers as H

    class _Mem:
        def get(self, _hid):
            return _freeze()

    seen = {}

    def _adopter(*, freeze_row, observation_clock_starts):
        seen["row"] = freeze_row
        seen["clock"] = observation_clock_starts
        return {"adopted": True, "outcome": PA.ADOPTED}

    out = H._register_forward_evidence(_Mem(), "H_abc_1", _adopter)
    assert out["adopted"] is True
    assert seen["row"]["hypothesis_id"] == "H_abc_1"
    # A DATE, taken now — never the freeze's own inception.
    assert len(seen["clock"]) == 10 and seen["clock"] != "2026-09-03"


def test_53d_an_adopter_that_raises_never_destroys_the_freeze():
    from paper_trader.alpha_agent.r59 import handlers as H

    class _Mem:
        def get(self, _hid):
            return _freeze()

    def _boom(**_kw):
        raise RuntimeError("owner unavailable")

    out = H._register_forward_evidence(_Mem(), "H_abc_1", _boom)
    assert out["adopted"] is False
    assert out["outcome"] == "ADOPTION_OWNER_UNAVAILABLE"


def test_54_the_adoption_owner_is_not_a_second_forward_registry():
    src = (REPO / "api" / "prospective_adoption.py").read_text(
        encoding="utf-8", errors="replace")
    lowered = src.lower()
    for banned in ("def append_outcomes", "def append_prediction",
                   "forward_predictions", "def mature("):
        assert banned not in lowered, banned
    assert "REGISTRARS" in src


# =========================================================================== #
# E. THE MULTI-ASSET FORWARD-EVIDENCE CONTRACT
# =========================================================================== #
MULTI_ASSET_CASES = [
    ("US_EQUITY", ["AAPL", "MSFT"], "XNAS"),
    ("COMMODITY_FUTURES", ["CL", "NG"], "NYMEX"),
    ("RATES_FUTURES", ["ZN", "ZB"], "CBOT"),
    ("FX_FUTURES", ["6E", "6J"], "CME"),
    ("EQUITY_INDEX_FUTURES", ["ES"], "CME"),
    ("VOLATILITY", ["VX"], "CFE"),
    ("CROSS_ASSET", ["ES", "ZN", "CL", "6E"], None),
]


@pytest.mark.parametrize("asset_class,scope,venue", MULTI_ASSET_CASES)
def test_55_the_adoption_identity_is_asset_agnostic(asset_class, scope, venue):
    row = _freeze(asset_class=asset_class, release="R58",
                  spec_json=json.dumps({"substrate": "r38",
                                        "cost_model": {"bps_per_side": 2.0}}))
    ident = PA.build_adoption_identity(row, instrument_scope=scope, venue=venue,
                                       sleeve="TERM_STRUCTURE")
    assert ident["asset_class"] == asset_class
    assert ident["instrument_scope"] == [str(x) for x in scope]
    assert ident["venue"] == venue
    assert ident["horizon_sessions"] == 21
    assert ident["cost_model"] == {"bps_per_side": 2.0}
    assert ident["price_mark_owner"] == PA.DEFAULT_MARK_OWNERS[asset_class]
    assert ident["identity_hash"]


@pytest.mark.parametrize("asset_class,scope,venue", MULTI_ASSET_CASES)
def test_56_a_non_equity_challenger_registers_on_the_same_contract(
        asset_class, scope, venue, tmp_path):
    calls = []
    out = PA.adopt_prospective_freeze(
        freeze_row=_freeze(asset_class=asset_class,
                           hypothesis_id="H_%s" % asset_class),
        observation_clock_starts="2026-09-08",
        confirm=PA.ADOPT_CONFIRM_TOKEN, instrument_scope=scope, venue=venue,
        registrar=_ok_registrar(calls), adoption_dir_override=str(tmp_path))
    assert out["outcome"] == PA.ADOPTED, asset_class
    assert calls[0]["identity"]["asset_class"] == asset_class
    assert calls[0]["identity"]["instrument_scope"] == [str(x) for x in scope]


def test_57_two_asset_classes_never_collide_on_one_identity(tmp_path):
    a = PA.build_adoption_identity(_freeze(asset_class="RATES_FUTURES"),
                                   instrument_scope=["ZN"])
    b = PA.build_adoption_identity(_freeze(asset_class="FX_FUTURES"),
                                   instrument_scope=["ZN"])
    assert a["identity_hash"] != b["identity_hash"]


def test_58_the_identity_contract_carries_no_equity_only_field():
    assert "ticker" not in PA.IDENTITY_FIELDS
    assert "instrument_scope" in PA.IDENTITY_FIELDS
    src = (REPO / "api" / "prospective_adoption.py").read_text(
        encoding="utf-8", errors="replace")
    assert '"ticker"' not in src


def test_59_the_forward_evidence_owner_stays_canonical():
    """This module delegates; it never becomes the owner.

    Release 62.1 added the THIRD canonical owner and routed every freeze that is
    not a member of the two frozen cohorts to it. The delegation rule is
    unchanged: this module still names an owner and calls it, and still owns no
    forward evidence of its own.
    """
    assert set(PA.REGISTRARS.values()) == {"api.shadow_portfolio_evidence",
                                           "alpha_agent.r46.registry",
                                           "api.forward_challenger_registry"}
    assert PA.classify_challenger_class({"release": "R56"}) == \
        PA.CLASS_PAPER_PORTFOLIO
    assert PA.classify_challenger_class({"release": "R46"}) == PA.CLASS_SIGNAL_R46
    assert PA.classify_challenger_class({"release": "R58"}) == \
        PA.CLASS_SIGNAL_CANONICAL


# =========================================================================== #
# I.A. ONE EXCHANGE CALENDAR
# =========================================================================== #
def test_60_labor_day_is_never_a_provider_expected_market_date():
    """The live Sep-8 contradiction: session recovery said 2026-09-04 while the
    provider clock said 2026-09-07 — Labor Day, a full-day closure."""
    md, cutoff, within = dc._expected_session(
        datetime(2026, 9, 8, 10, 0, tzinfo=_ET))
    assert md.isoformat() == "2026-09-04"
    assert within is True, "Sep-8 IS a trading session still forming"
    assert cutoff is False


def test_61_a_holiday_is_never_within_a_trading_day():
    md, cutoff, within = dc._expected_session(
        datetime(2026, 9, 7, 18, 0, tzinfo=_ET))
    assert md.isoformat() == "2026-09-04"
    assert within is False, "the market never opened on Labor Day"
    assert cutoff is False, "a cutoff cannot pass on a session that never ran"


def test_62_the_session_completes_normally_after_the_cutoff():
    md, cutoff, within = dc._expected_session(
        datetime(2026, 9, 8, 18, 0, tzinfo=_ET))
    assert md.isoformat() == "2026-09-08"
    assert cutoff is True and within is False


def test_63_the_offline_injected_path_honours_the_same_calendar():
    clock = dc._resolve_clock(today="2026-09-08")
    assert clock["expected_market_date"] == "2026-09-04"


def test_64_the_provider_clock_agrees_with_the_session_recovery_owner():
    """ONE calendar: the two owners cannot disagree about a session date."""
    from paper_trader.engine import exchange_calendar as xcal
    from paper_trader.engine import market_session as ms
    non = xcal.non_sessions_between("2026-08-10", "2026-09-15")
    assert "2026-09-07" in non
    assert ms.previous_trading_day(
        datetime(2026, 9, 8).date(), non).isoformat() == "2026-09-04"
    assert dc._resolve_clock(
        now=datetime(2026, 9, 8, 10, 0, tzinfo=_ET))["expected_market_date"] \
        == "2026-09-04"


def test_65_daily_close_derives_no_holiday_of_its_own():
    src = (REPO / "api" / "daily_close.py").read_text(
        encoding="utf-8", errors="replace")
    assert "_authoritative_non_sessions" in src
    assert "exchange_calendar" in src
    # It asks the calendar owner; it does not carry a holiday list.
    assert "Thanksgiving" not in src and "Labor Day" not in src.replace(
        "Labor\nDay", "")


def test_66_the_mislabelled_session_field_is_deprecated_not_contradictory():
    """``current_open_or_next_session`` was a verbatim alias of the latest
    EXPECTED COMPLETED session and never carried the session it named."""
    src = (REPO / "api" / "workflow_state.py").read_text(
        encoding="utf-8", errors="replace")
    assert '"current_open_or_next_session_deprecated": True' in src
    assert '"latest_expected_completed_session"' in src
    assert '"replaced_by": "latest_expected_completed_session"' in src


# =========================================================================== #
# I.B. THE LATENCY CATEGORY ERROR - NAMED, NEVER BACKFILLED
# =========================================================================== #
def test_67_the_daily_lane_declares_its_intraday_endpoints_not_required():
    # R62.1.1 — the R61 declaration, COMPLETED. Every endpoint the daily lane
    # excuses is an EVENT CYCLE concept, and R61 named only two of the five, so
    # the record it produced said `missing_measurements: []` while three of its
    # own interval dispositions said MISSING. The intraday pair is still in the
    # set; three event-cycle stage endpoints join it.
    declared = pdec._not_required_latency_stages(
        {"intraday_latency_applicable": False})
    assert declared == list(pdec.DAILY_LANE_ABSENT_LATENCY_STAGES)
    for stage in pdec.INTRADAY_ONLY_LATENCY_STAGES:
        assert stage in declared


def test_68_a_producer_that_declares_nothing_excuses_nothing():
    assert pdec._not_required_latency_stages({}) == []
    assert pdec._not_required_latency_stages(
        {"stage_timestamps": {"a": 1}}) == []
    assert pdec._not_required_latency_stages(None) == []


def test_69_an_excused_endpoint_is_not_required_rather_than_missing():
    lat = esr.measure_decision_latency(
        stage_timestamps={}, event_cycle_started_at=None,
        observation_received_at=None,
        governance_gate_completed_at="2026-09-06T22:57:00+00:00",
        governed_decision_persisted_at="2026-09-06T22:57:01+00:00",
        not_required_stages=list(pdec.DAILY_LANE_ABSENT_LATENCY_STAGES))
    assert lat["missing_measurements"] == []
    assert lat["latency_measurement_complete"] is True
    for stage in pdec.INTRADAY_ONLY_LATENCY_STAGES:
        assert lat["stage_dispositions"][stage] == esr.LAT_NOT_REQUIRED
    # R62.1.1 — and the census now covers EVERY endpoint an interval needs, so
    # "complete" and the interval dispositions describe the same set.
    for name in esr.LATENCY_INTERVAL_ENDPOINTS:
        assert lat["interval_dispositions"][name] in (
            esr.LAT_MEASURED, esr.LAT_NOT_REQUIRED)


def test_69b_a_partial_declaration_excuses_only_what_it_names():
    """Nothing is auto-excused: a producer that names two endpoints excuses two,
    and every other unstamped endpoint is still MISSING and still incomplete."""
    lat = esr.measure_decision_latency(
        stage_timestamps={}, event_cycle_started_at=None,
        observation_received_at=None,
        governance_gate_completed_at="2026-09-06T22:57:00+00:00",
        governed_decision_persisted_at="2026-09-06T22:57:01+00:00",
        not_required_stages=list(pdec.INTRADAY_ONLY_LATENCY_STAGES))
    for stage in pdec.INTRADAY_ONLY_LATENCY_STAGES:
        assert lat["stage_dispositions"][stage] == esr.LAT_NOT_REQUIRED
    for stage in pdec.EVENT_CYCLE_ONLY_LATENCY_STAGES:
        if stage in lat["stage_dispositions"]:
            assert lat["stage_dispositions"][stage] == esr.LAT_MISSING
    assert "reassessment_completed_at" in lat["missing_measurements"]
    assert lat["latency_measurement_complete"] is False


def test_70_a_stamped_endpoint_is_measured_whatever_the_caller_claimed():
    """No backfill in the other direction either: an excuse cannot erase a stamp."""
    lat = esr.measure_decision_latency(
        stage_timestamps={}, event_cycle_started_at="2026-09-06T22:56:00+00:00",
        observation_received_at="2026-09-06T22:55:00+00:00",
        governance_gate_completed_at="2026-09-06T22:57:00+00:00",
        governed_decision_persisted_at="2026-09-06T22:57:01+00:00",
        not_required_stages=list(pdec.INTRADAY_ONLY_LATENCY_STAGES))
    for stage in pdec.INTRADAY_ONLY_LATENCY_STAGES:
        assert lat["stage_dispositions"][stage] == esr.LAT_MEASURED


def test_71_an_intraday_gap_is_still_a_real_gap():
    scope = ams._latency_lane_scope(
        {"provenance": pdec.PROV_GOVERNED_INTRADAY},
        {"missing_measurements": ["observation_received_at"]})
    assert scope["structurally_absent_measurements"] == []
    assert scope["measurements_missing_and_expected"] == [
        "observation_received_at"]


def test_72_a_daily_lane_gap_is_scoped_and_never_reconstructed():
    scope = ams._latency_lane_scope(
        {"provenance": pdec.PROV_GOVERNED_DAILY_CYCLE},
        {"missing_measurements": ["observation_received_at",
                                  "event_cycle_started_at"]})
    assert sorted(scope["structurally_absent_measurements"]) == sorted(
        pdec.INTRADAY_ONLY_LATENCY_STAGES)
    assert scope["measurements_missing_and_expected"] == []
    assert scope["backfilled"] is False


def test_73_not_applicable_is_a_distinct_acceptance_status():
    assert ams.ACCEPTANCE_NOT_APPLICABLE not in (ams.ACCEPTANCE_PRESENT,
                                                 ams.ACCEPTANCE_MISSING)


# =========================================================================== #
# G. SAFETY / GOVERNANCE
# =========================================================================== #
def test_74_the_adoption_owner_promotes_nothing_and_trades_nothing(tmp_path):
    out = PA.adopt_prospective_freeze(
        freeze_row=_freeze(), observation_clock_starts="2026-09-08",
        confirm=PA.ADOPT_CONFIRM_TOKEN, registrar=_ok_registrar([]),
        adoption_dir_override=str(tmp_path))
    s = out["safety"]
    for flag in ("promoted_model", "automatic_model_promotion_allowed",
                 "activated_sleeve", "changed_holdings", "changed_cash",
                 "changed_nav", "created_orders", "created_order_plan",
                 "created_fills", "approved_anything", "automation_enabled",
                 "broker_enabled", "backfilled_forward_evidence",
                 "rewrote_history"):
        assert s[flag] is False, flag
    assert s["research_only"] is True and s["paper_only"] is True


def test_75_there_is_no_path_from_a_candidate_to_a_promotion_or_a_position():
    """Structural: the R61 modules CALL no promotion, order or holding writer.

    The scan is over executable source and looks for a CALL — ``broker_enabled:
    False`` is a safety declaration, and refusing to declare it would be the
    opposite of the property under test.
    """
    for rel in ("api/prospective_adoption.py", "alpha_agent/r59/blockers.py"):
        code = _executable_source(rel).lower()
        for banned in ("promote_champion", "promote_model", "create_order",
                       "place_order", "submit_order", "append_fill",
                       "set_holdings", "update_holdings", "approve_proposal",
                       "rebalance_execution", "paper_trading_desk"):
            assert "%s (" % banned not in code and "%s(" % banned not in code, \
                "%s called in %s" % (banned, rel)
        # Every broker mention is a declaration that it is OFF.
        for line in (REPO / rel).read_text(
                encoding="utf-8", errors="replace").splitlines():
            if "broker" in line.lower() and not line.lstrip().startswith("#"):
                assert "False" in line or '"""' in line or "broker_enabled" in line, line


def test_76_the_governed_decision_lane_keeps_its_structural_safety():
    safety = pdec._governed_safety()
    assert safety["manual_review_required_for_change"] is True
    assert safety["automation_enabled"] is False
    assert safety["created_orders"] is False
    assert safety["promoted_model"] is False
    assert safety["automatic_model_promotion_allowed"] is False
    assert safety["activated_sleeve"] is False
    assert safety["advances_operational_mark"] is False


def test_77_adoption_cannot_be_triggered_by_an_approval_token():
    """The adoption token is distinct from every approval / promotion token, so
    approving one thing can never adopt another."""
    assert PA.ADOPT_CONFIRM_TOKEN != pdec.CONFIRM_TOKEN
    assert PA.ADOPT_CONFIRM_TOKEN != pdec.GOVERNED_DECISION_CONFIRM_TOKEN
    out = PA.adopt_prospective_freeze(
        freeze_row=_freeze(), observation_clock_starts="2026-09-08",
        confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN)
    assert out["outcome"] == PA.REFUSED_CONFIRMATION


# =========================================================================== #
# F. THE RESEARCH READ MODEL READS AUTHORITATIVE STATE
# =========================================================================== #
def test_78_the_read_model_delegates_lifecycle_and_blockers():
    """No business logic in the read model, and none in JavaScript."""
    src = (REPO / "api" / "alphaagent_outcomes.py").read_text(
        encoding="utf-8", errors="replace")
    assert "prospective_adoption" in src
    assert "blockers as BLK" in src
    assert "BLK.classify_job" in src
    assert "PA.classify_lifecycle" in src
    # The read model owns no taxonomy and no lifecycle rule of its own.
    assert "def classify_lifecycle" not in src
    assert 'FAMILY_EXHAUSTED = "' not in src
    assert src.count('"owns_no_research_state": True') >= 1


def test_79_the_read_model_separates_process_state_from_evidence_state():
    src = (REPO / "api" / "alphaagent_outcomes.py").read_text(
        encoding="utf-8", errors="replace")
    assert '"process_state_is_not_evidence_state": True' in src
    assert '"process_health_is_not_research_success": True' in src
    assert "def _blocked_block" in src


def test_80_the_ui_surface_gains_no_javascript_business_logic():
    ui = REPO / "api" / "ui"
    if not ui.exists():
        pytest.skip("no ui directory in this checkout")
    for js in list(ui.rglob("*.js")) + list(ui.rglob("*.html")):
        text = js.read_text(encoding="utf-8", errors="replace")
        assert "FRONTIER_EXHAUSTED_UNTIL_NEW_INFORMATION =" not in text
        assert "function classifyBlocker" not in text
        assert "function classifyLifecycle" not in text


def test_81_the_existing_research_surface_gains_the_r61_blocks():
    """Workstream F: the EXISTING AlphaAgent Outcomes surface, extended - not a
    second dashboard, no new route, and every value rendered verbatim."""
    idx = (REPO / "api" / "ui" / "index.html").read_text(
        encoding="utf-8", errors="replace")
    # One panel, one loader, one route - unchanged.
    assert idx.count("id=\"aaout-panel\"") == 1
    assert idx.count("_mhzGet('/v1/research/alphaagent-outcomes')") == 1
    assert idx.count("function loadAlphaAgentOutcomes") == 1
    # The new read-only blocks.
    assert "id=\"aao-blocked\"" in idx
    assert "d.blocked_research" in idx
    assert "rt.wake_condition" in idx
    assert "rt.blocker_reason" in idx
    assert "orphan_freezes_adoptable_count" in idx
    assert "open_adoption_intents" in idx
    assert "never resurrectable" in idx


def test_82_the_new_ui_blocks_add_no_dialog_and_no_action_control():
    """Safety: no alert(), no confirm(), and no control that could write."""
    idx = (REPO / "api" / "ui" / "index.html").read_text(
        encoding="utf-8", errors="replace")
    start = idx.index("// ---- R61: blocked work")
    end = idx.index("_setHtml('aao-blocked', bhtml);") + 40
    block = idx[start:end]
    for banned in ("alert(", "confirm(", "prompt(", "fetch(", "POST",
                   "createOrder", "approve"):
        assert banned not in block, banned
