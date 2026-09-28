r"""R80 - three surfaces that each reported health over a fact their own owner
had already refused, contradicted or been unable to spell.

THE THREE DEFECTS THIS LOCKS OUT
--------------------------------
1. THE PRE-BOUNDARY MONITOR CALLED AN UNPRICEABLE REGISTRATION READY.
   ``REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1`` carried accrual state
   ``INTEGRITY_BLOCKED`` with blocker
   ``DECLARED_ENTRY_MARK_IS_NOT_SERVED_BY_THE_DECLARED_VALUATION_PATH`` - the
   accrual owner's statement that the instant the strategy says it enters cannot
   be priced by the path it declared. Every OTHER readiness check passed: inputs
   present, producer live, producer fresh, misses not yet chronic. So it was
   counted ``READY_FOR_NEXT_BOUNDARY``, the module's own headline said "Every one
   has a declared, executable prediction path", and ``n_preboundary_defects`` was
   0 - about a registration that had emitted nothing across two boundaries and was
   two calendar days from a third. The one instrument whose entire purpose is to
   speak BEFORE a boundary is lost reported OK.

2. THE AUTONOMY SURFACE PUBLISHED ``proposal_id: null`` FOR A NAMED PROPOSAL.
   ``canonical_portfolio_decision`` carries ``proposal_hash`` at its root and has
   no ``proposal_id`` key at all; the canonical id lives one level down in
   ``proposal_supersession``, which is where the proposal-decision-review route
   serves it from. The top-level-only read published a real hash beside a null id,
   so a reader reconciling the autonomy surface against the review saw the review
   name a proposal and the autonomy surface deny it. This is the same defect class
   R79.4 fixed twice for spellings - this time it was a DEPTH, not a spelling.

3. THE DIRECTOR BRIEF WAS REFUSED BY ITS OWN HANDOFF CONTRACT.
   R79 joined the durable ruling store into the brief's ``queued_hypotheses`` so a
   refused cell could not be re-commissioned, which works. But the added
   ``refused_by`` prose took the LIVE brief to 504 words against
   ``MAX_HANDOFF_PROSE_WORDS`` = 500, so ``brief_problems()`` refused it and
   ``agents_v2_brief.py --role quant-research-director`` printed BRIEF_REFUSED and
   wrote no file: the fix for the re-commissioning defect had made the director
   unbriefable. It passed in test because the fixture store holds no rulings, so
   the join added nothing. Meanwhile the brief's FACTS still opened with 189 words
   of FROZEN census prose recommending two cells the estate had since REFUSED.

NOTHING HERE TOUCHES A LIVE STORE. The forward assertions run on injected
fixtures; the brief assertions build their own in-memory research memory.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from paper_trader.api import autonomous_operating_status as AOS
from paper_trader.api import forward_producer_health as H


NOW = datetime(2026, 9, 27, 16, 0, tzinfo=timezone.utc)

#: The registration whose entry mark the accrual owner refuses. Using a
#: challenger the producer declaration knows matters: an unknown id is
#: short-circuited to R_NO_PRODUCER before the entry-mark test is ever reached.
SPY_NEXT_OPEN = "REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1"
FX_CARRY = "ALPHA_RECOVERY_FX_CARRY_CADENCE_H1_F9B1ACA7"


def _beat(**kw):
    base = {
        "ran": True,
        "last_run_id": "r52run_20260927T160542Z",
        "last_run_started_utc": (NOW - timedelta(minutes=15)).isoformat(),
        "last_stage_state": "NOT_DUE",
        "last_advance_state": "NEXT_OPEN_NOT_A_DECISION_BOUNDARY",
        "last_detail": None,
        "blocked_on": None,
        "next_boundaries": ["2026-09-29"],
        "missed_boundaries": ["2026-09-15", "2026-09-22"],
        "publication": "PUBLISHED",
        "entry_session": "2026-09-28",
        "information_session": "2026-09-25",
        "entry_state": None,
        "forward_panel_last_session": "2026-09-25",
        "declared_grid_owner": "alpha_agent.alpha_recovery.next_open_runtime",
    }
    base.update(kw)
    return base


def _accrual(challenger_id, **kw):
    base = {
        "challenger_id": challenger_id,
        "asset_class": "US_ETF",
        "cadence_sessions": 5,
        "horizon_sessions": 5,
        "predictions_emitted": 0,
        "matured_observations": 0,
        "pending_observations": 0,
        "forfeitures": 0,
        "last_emission_session": None,
        "next_eligible_observation_session": None,
        "current_accrual_state": "NOT_DUE",
        "latest_blocker": None,
        "registration_session": "2026-09-12",
    }
    base.update(kw)
    return base


def _blocked_accrual(**kw):
    """The SPY next-open registration exactly as the accrual owner projects it."""
    return _accrual(
        SPY_NEXT_OPEN,
        current_accrual_state=H.ACCRUAL_INTEGRITY_BLOCKED,
        latest_blocker=H.UNPRICEABLE_ENTRY_MARK_BLOCKER,
        next_eligible_observation_session="2026-09-29", **kw)


# --------------------------------------------------------------------------- #
# 1. THE PRE-BOUNDARY MONITOR
# --------------------------------------------------------------------------- #
def test_a_refused_entry_mark_is_a_defect_not_a_ready_registration():
    out = H.preboundary_readiness(_blocked_accrual(), _beat(), now=NOW)
    assert out["readiness"] == H.R_ENTRY_MARK_INFEASIBLE
    assert out["severity"] == H.SEV_DEFECT
    assert out["is_actionable_now"] is True, (
        "an operator cannot act on a boundary nobody told them was unreachable")


def test_the_verdict_names_the_accrual_owner_and_carries_its_own_inputs():
    """A reader handed a verdict must be able to check it, not take it."""
    out = H.preboundary_readiness(_blocked_accrual(), _beat(), now=NOW)
    assert out["entry_mark_unpriceable"] is True
    assert out["accrual_blocker"] == H.UNPRICEABLE_ENTRY_MARK_BLOCKER
    assert out["accrual_state"] == H.ACCRUAL_INTEGRITY_BLOCKED
    assert out["entry_mark_refusal_owner"] == H.ACCRUAL_OWNER
    # The boundary and the distance to it, because the point is the deadline.
    assert out["next_boundary"] == "2026-09-29"
    assert out["calendar_days_until_boundary"] == 2


def test_the_refusal_outranks_every_check_that_would_report_health():
    """Inputs present, producer live and fresh, misses not chronic - and it is
    STILL a defect. The refusal is structural; those four are about lateness."""
    out = H.preboundary_readiness(_blocked_accrual(), _beat(), now=NOW)
    assert out["input_readiness"]["input_state"] == "INPUTS_PRESENT"
    assert out["producer_is_stale"] is False
    assert out["chronic_miss"]["is_chronic"] is False
    assert out["readiness"] == H.R_ENTRY_MARK_INFEASIBLE, (
        "the pre-R80 ladder fell through all four of these to READY")


def test_a_healthy_registration_is_still_ready():
    """The new state must not swallow the ordinary case."""
    out = H.preboundary_readiness(_accrual(SPY_NEXT_OPEN), _beat(), now=NOW)
    assert out["readiness"] == H.R_READY
    assert out["entry_mark_unpriceable"] is False
    assert out["entry_mark_refusal_owner"] is None


def test_an_unrelated_blocker_is_not_read_as_an_entry_mark_refusal():
    """Only the accrual owner's OWN unpriceable code counts. A DATA_BLOCKED
    registration is waiting for a publication, which is a different fact."""
    acc = _accrual(SPY_NEXT_OPEN, current_accrual_state="DATA_BLOCKED",
                   latest_blocker="PRICE_PANEL_HAS_NOT_PUBLISHED_S_MINUS_1")
    out = H.preboundary_readiness(acc, _beat(), now=NOW)
    assert out["entry_mark_unpriceable"] is False
    assert out["readiness"] != H.R_ENTRY_MARK_INFEASIBLE


def test_the_state_is_registered_in_the_vocabulary_and_severity_tables():
    assert H.R_ENTRY_MARK_INFEASIBLE in H.READINESS_STATES
    assert H.R_ENTRY_MARK_INFEASIBLE in H.ACTIONABLE_READINESS_STATES
    assert H.READINESS_SEVERITY[H.R_ENTRY_MARK_INFEASIBLE] == H.SEV_DEFECT


def test_the_blocker_code_is_imported_from_the_accrual_owner_not_retyped():
    """A rename in the accrual owner must not silently stop matching."""
    from paper_trader.api import canonical_forward_accrual as ACC
    assert H.UNPRICEABLE_ENTRY_MARK_BLOCKER == \
        ACC.INTEGRITY_ENTRY_MARK_UNPRICEABLE
    assert H.ACCRUAL_INTEGRITY_BLOCKED == ACC.ACC_INTEGRITY_BLOCKED


# --------------------------------------------------------------------------- #
# 2. THE AGGREGATE LEDGER - the headline must not reassure over the refusal
# --------------------------------------------------------------------------- #
def _coverage_fixture():
    proj = {
        "h_spy_next": _blocked_accrual(),
        "h_fx": _accrual(FX_CARRY, asset_class="FX_FUTURES",
                         predictions_emitted=1,
                         last_emission_session="2026-09-22"),
    }
    runs = {"runs": [{
        "run_id": "r1", "started_utc": (NOW - timedelta(minutes=15)).isoformat(),
        "stages": [
            {"stage": "next_open_prospective_decision", "state": "NOT_DUE",
             "advance_state": "NEXT_OPEN_NOT_A_DECISION_BOUNDARY",
             "publication": "PUBLISHED", "entry_session": "2026-09-28",
             "information_session": "2026-09-25",
             "next_boundaries": ["2026-09-29"],
             "missed_boundaries": ["2026-09-15", "2026-09-22"]},
            {"stage": "fx_carry_cadence_prospective_decision", "state": "NOT_DUE",
             "advance_state": "FX_CADENCE_OUTSIDE_A_DECISION_WINDOW",
             "next_boundaries": ["2026-09-29"],
             "missed_boundaries": ["2026-09-15"]},
        ]}]}
    return proj, runs


def test_the_headline_stops_claiming_every_path_is_executable():
    proj, runs = _coverage_fixture()
    cov = H.producer_coverage(accrual_by_identity=proj, runs=runs, now=NOW)
    assert "Every one has a declared, executable prediction path" \
        not in cov["headline"], "it was not true of the SPY next-open sibling"
    assert SPY_NEXT_OPEN in cov["headline"]
    assert "unpriceable" in cov["headline"]


def test_the_refused_registration_reaches_the_defect_count_and_the_ledger():
    proj, runs = _coverage_fixture()
    cov = H.producer_coverage(accrual_by_identity=proj, runs=runs, now=NOW)
    assert cov["by_readiness_state"].get(H.R_ENTRY_MARK_INFEASIBLE) == 1
    assert cov["n_preboundary_defects"] >= 1
    assert cov["every_next_boundary_is_reachable"] is False
    named = [w["challenger_id"] for w in cov["preboundary_warnings"]]
    assert SPY_NEXT_OPEN in named


def test_the_aggregate_ledger_can_be_asked_at_a_frozen_instant():
    """Without a threaded clock the aggregate read the wall clock while its
    fixtures pinned absolute dates, so a passing test decayed into a failing one
    as the real date advanced. The verdict must depend on ``now`` alone."""
    proj, runs = _coverage_fixture()
    early = H.producer_coverage(accrual_by_identity=proj, runs=runs,
                                now=NOW - timedelta(days=20))
    late = H.producer_coverage(accrual_by_identity=proj, runs=runs, now=NOW)
    rd = {r["challenger_id"]: r["preboundary_readiness"] for r in late["registrations"]}
    assert rd[SPY_NEXT_OPEN]["calendar_days_until_boundary"] == 2
    early_rd = {r["challenger_id"]: r["preboundary_readiness"]
                for r in early["registrations"]}
    assert early_rd[SPY_NEXT_OPEN]["calendar_days_until_boundary"] == 22
    # The refusal is not a function of the clock: it is true at both instants.
    assert early_rd[SPY_NEXT_OPEN]["readiness"] == H.R_ENTRY_MARK_INFEASIBLE


# --------------------------------------------------------------------------- #
# 3. THE AUTONOMY SURFACE - proposal identity travels one level down
# --------------------------------------------------------------------------- #
def _workflow(**decision):
    base = {
        "decision_state": "PROPOSAL_REVIEW_REQUIRED",
        "proposal_hash": "eaee484fa4a01a85d1cceb71c6a5616fd67cdeee5",
        "proposal_state": "REALLOCATION_PROPOSAL_READY",
        "proposal_supersession": {
            "owner": "api.portfolio_decision",
            "superseded": False,
            "proposal_id": "reap_2026-09-25_alpha_paper_book_1_eaee484fa4a0",
            "proposal_hash": "eaee484fa4a01a85d1cceb71c6a5616fd67cdeee5",
        },
    }
    base.update(decision)
    return {"canonical_portfolio_decision": base}


def test_the_proposal_id_is_found_in_the_block_that_carries_it():
    out = AOS._portfolio_proposal_state(_workflow())
    assert out["proposal_id"] == \
        "reap_2026-09-25_alpha_paper_book_1_eaee484fa4a0"
    assert out["projected_under"] == "canonical_portfolio_decision"


def test_a_root_level_id_still_wins_over_the_nested_one():
    """The nested read is a FALLBACK. If the decision owner ever publishes the id
    at its root, that is the more authoritative spelling and must be preferred."""
    out = AOS._portfolio_proposal_state(_workflow(proposal_id="root_wins"))
    assert out["proposal_id"] == "root_wins"


def test_a_missing_id_is_still_none_and_is_never_synthesised():
    wf = _workflow()
    wf["canonical_portfolio_decision"]["proposal_supersession"] = {}
    out = AOS._portfolio_proposal_state(wf)
    assert out["proposal_id"] is None, "absence is reported, not invented"
    assert out["proposal_hash"] == "eaee484fa4a01a85d1cceb71c6a5616fd67cdeee5"


def test_the_id_and_the_hash_are_never_published_inconsistently():
    """The defect's signature was a real hash beside a null id. Whenever the
    supersession block names a proposal, both must surface."""
    out = AOS._portfolio_proposal_state(_workflow())
    assert out["proposal_id"] and out["proposal_hash"]


# --------------------------------------------------------------------------- #
# 3b. THE AUTONOMY SURFACE - the permission it publishes must be the one that
#     governs the next write, not the one taken at startup
# --------------------------------------------------------------------------- #
def _worker(maturation, maturation_now=None, commit="5e98c80be19e"):
    body = {"source_identity": {"commit": commit, "dirty": False},
            "maturation": maturation}
    if maturation_now is not None:
        body["maturation_now"] = maturation_now
    return body


STARTUP_OK = {"allowed": True, "reason": "COMMITTED_CLEAN_SOURCE",
              "detail": None}
AT_USE_DIRTY = {"allowed": False, "reason": "SOURCE_HAS_UNCOMMITTED_CHANGES",
                "rechecked": True, "startup_commit": "5e98c80be19e",
                "current_commit": "b0f7cb3a6e2a", "current_dirty": True}


def test_a_startup_permission_is_never_published_as_the_live_one():
    """The exact live state on 2026-09-27: a persistent worker that started
    clean at 5e98c80, a checkout that has moved to b0f7cb3 and gone dirty, and
    an operator two days from two real decision boundaries asking whether the
    estate can still record forward evidence. The startup verdict says yes. The
    worker's own at-use verdict says no. The surface must publish the no."""
    out = AOS._runtime_source_identity(
        {"loaded_commit": "5e98c80be19e"},
        _worker(STARTUP_OK, AT_USE_DIRTY))
    assert out["may_write_prospective_evidence_now"] is False
    assert out["prospective_write_blocker"] == "SOURCE_HAS_UNCOMMITTED_CHANGES"
    assert out["startup_and_at_use_verdicts_agree"] is False
    assert out["maturation_gate_is_the_startup_verdict"] is True


def test_the_startup_verdict_keeps_its_name_so_no_reader_loses_a_field():
    out = AOS._runtime_source_identity(
        {"loaded_commit": "5e98c80be19e"},
        _worker(STARTUP_OK, AT_USE_DIRTY))
    assert out["maturation_gate"] == STARTUP_OK
    assert out["maturation_gate_at_use"] == AT_USE_DIRTY


def test_two_processes_agreeing_does_not_mean_they_match_the_checkout():
    """``processes_agree`` compared the backend and the worker and nothing else,
    so it read True while BOTH were behind the checkout - which is exactly the
    state that makes a restart necessary and exactly what the field hid."""
    out = AOS._runtime_source_identity(
        {"loaded_commit": "5e98c80be19e"},
        _worker(STARTUP_OK, AT_USE_DIRTY))
    assert out["processes_agree"] is True
    assert out["processes_agree_with_checkout"] is False
    assert out["checkout_commit"] == "b0f7cb3a6e2a"


def test_an_agreeing_estate_reports_agreement():
    at_use = {"allowed": True, "reason": "COMMITTED_CLEAN_SOURCE",
              "rechecked": True, "current_commit": "5e98c80be19e",
              "current_dirty": False}
    out = AOS._runtime_source_identity(
        {"loaded_commit": "5e98c80be19e"}, _worker(STARTUP_OK, at_use))
    assert out["may_write_prospective_evidence_now"] is True
    assert out["prospective_write_blocker"] is None
    assert out["startup_and_at_use_verdicts_agree"] is True
    assert out["processes_agree_with_checkout"] is True


def test_a_worker_that_recorded_no_at_use_verdict_yields_none_not_yes():
    """Fails CLOSED in the reader's vocabulary: an older worker, or one that has
    not yet reached a prospective write, must not have a permission inferred for
    it from the startup verdict that IS present."""
    out = AOS._runtime_source_identity(
        {"loaded_commit": "5e98c80be19e"}, _worker(STARTUP_OK))
    assert out["may_write_prospective_evidence_now"] is None
    assert out["prospective_write_blocker"] is None
    assert out["startup_and_at_use_verdicts_agree"] is None
    assert out["processes_agree_with_checkout"] is None
    assert out["maturation_gate"] == STARTUP_OK


# --------------------------------------------------------------------------- #
# 4. THE DIRECTOR BRIEF - the ruling join must fit inside its own contract
# --------------------------------------------------------------------------- #
def _memory_with_rulings(tmp_path, monkeypatch):
    """A store that holds rulings, which is what the R79 regression lacked.

    The fixture the R79 join was tested against had NO director rulings, so the
    join contributed nothing to the word count and the 500-word budget was never
    approached. Against the live store - twelve rulings, thirteen queued proposals
    - the same brief came to 504 words and was refused. A budget regression whose
    fixture cannot reach the budget is not a regression.
    """
    from paper_trader.alpha_agent import r59 as _r59
    from paper_trader.alpha_agent.agents_v2 import pipeline as _P
    from paper_trader.alpha_agent.r59 import memory as _M
    monkeypatch.setenv(_r59.RESEARCH_ROOT_ENV, str(tmp_path / "r59_root"))
    mem = _M.ResearchMemory(tmp_path / "research_memory.sqlite")
    families = [
        ("US_EQUITY", "SHORT_TERM_REVERSAL",
         "NEW_ORTHOGONAL_NON_PRICE_INFORMATION_THAT_IS_NOT_FINRA_SHORT_VOLUME"),
        ("US_EQUITY", "LIQUIDITY_PREMIUM",
         "NEW_ORTHOGONAL_NON_PRICE_INFORMATION_ABOUT_LIQUIDITY_PROVISION"),
        ("US_EQUITY", "RESIDUAL_MOMENTUM", "NEW_ORTHOGONAL_NON_PRICE_INFORMATION"),
        ("US_EQUITY", "PROFITABILITY", "NEW_ORTHOGONAL_INFORMATION"),
        ("COMMODITY_FUTURES", "CARRY", "NEW_ORTHOGONAL_INFORMATION"),
        ("COMMODITY_FUTURES", "POSITIONING", "NEW_ORTHOGONAL_INFORMATION"),
        ("CROSS_ASSET", "CARRY", "NEW_ORTHOGONAL_INFORMATION"),
        ("CROSS_ASSET", "CROSS_ASSET_LEAD_LAG",
         "OWNED_PIT_NON_PRICE_INFORMATION_ON_AT_LEAST_ONE_LEG"),
        ("CROSS_ASSET", "CROSS_ASSET_RELATIVE_VALUE",
         "OWNED_PIT_NON_PRICE_TERMS_OF_TRADE_VINTAGES"),
        ("CROSS_ASSET", "CROSS_ASSET_REGIME_CONDITIONING",
         "A_MECHANISM_THAT_IS_NOT_THE_TIMING_A_PREMIUM_SHAPE"),
        ("VOLATILITY", "VOLATILITY_RISK_PREMIUM", "NEW_ORTHOGONAL_INFORMATION"),
        ("RATES_FUTURES", "TREND", "NEW_ORTHOGONAL_INFORMATION"),
    ]
    for ac, fam, reopen in families:
        mem.record_director_ruling(
            asset_class=ac, economic_family=fam,
            verdict="REFUSED_AS_ALREADY_MEASURED",
            blocker_reason="FAMILY_EXHAUSTED",
            rationale=("a rationale long enough to matter if it were ever copied "
                       "into the brief, which is exactly what the R79 author "
                       "declined to do and this test defends"),
            reopen_condition=reopen, campaign_id="R80_FIXTURE",
            decided_by="quant-research-director")
    return _P.AgentPipeline(mem, artifact_root=tmp_path / "agents_v2")


def test_the_director_brief_fits_its_budget_with_a_store_full_of_rulings(
        tmp_path, monkeypatch):
    from paper_trader.alpha_agent.agents_v2 import briefs as BR
    pipe = _memory_with_rulings(tmp_path, monkeypatch)
    brief = BR.director_brief(pipe, run_id="R80", campaign_id="R80_FIXTURE")
    assert BR.brief_problems(brief) == [], (
        "the brief the orchestrator cannot hand over is not a brief")
    assert BR.prose_words(brief) <= BR.MAX_HANDOFF_PROSE_WORDS


def test_the_brief_no_longer_copies_the_frozen_census_narrative(
        tmp_path, monkeypatch):
    """The narrative recommended two cells the ruling store had refused, and it
    was the first thing in FACTS a director read."""
    from paper_trader.alpha_agent.agents_v2 import briefs as BR
    pipe = _memory_with_rulings(tmp_path, monkeypatch)
    facts = BR.director_brief(pipe, run_id="R80",
                              campaign_id="R80_FIXTURE")["FACTS"]
    oif = facts["open_information_families"]
    # The key survives - established callers and test_16 read it.
    assert isinstance(oif, dict) and oif
    for stale in ("best_h1_h5_domain", "best_cross_asset_domain",
                  "best_unexplored_equity_domain",
                  "best_unexplored_futures_domain"):
        assert stale not in oif, "the frozen prose is a pointer now, not a copy"
    assert oif["narrative_pointer"].startswith(BR.CENSUS_FILE)
    assert oif["narrative_is_frozen_prose_not_live_state"] is True


def test_the_live_ruled_families_replace_the_narrative(tmp_path, monkeypatch):
    from paper_trader.alpha_agent.agents_v2 import briefs as BR
    pipe = _memory_with_rulings(tmp_path, monkeypatch)
    oif = BR.director_brief(pipe, run_id="R80", campaign_id="R80_FIXTURE"
                            )["FACTS"]["open_information_families"]
    assert oif["n_ruled_economic_families"] == 12
    joined = "\n".join(oif["ruled_economic_families"])
    # The reopen condition that names and EXCLUDES the information a director
    # would otherwise be told is the best unexplored H1-H5 opportunity.
    assert "NOT_FINRA_SHORT_VOLUME" in joined
    assert "US_EQUITY|SHORT_TERM_REVERSAL" in joined
    # Codes, not prose: one whitespace-free token per family keeps the refusals
    # affordable inside the handoff budget.
    for row in oif["ruled_economic_families"]:
        assert " " not in row, "a ruling row costs one word, not several"


def test_the_availability_claim_comes_from_the_certification_store(
        tmp_path, monkeypatch):
    """The sibling of the narrative defect. ``available_pit_datasets`` is a
    frozen census snapshot whose rows the estate's own later certifications have
    falsified - comment letters and dividend declarations both closed DATA_HOLD,
    Nasdaq halts failed on historical depth - and it was copied into the brief as
    FACTS. The live classification must travel with it."""
    from paper_trader.alpha_agent.agents_v2 import briefs as BR
    pipe = _memory_with_rulings(tmp_path, monkeypatch)
    pipe.mem.set_provider_usage(
        "FREE_PUBLIC_SOURCES", "FINRA_SHORT_VOLUME",
        coverage={"classification": "PROSPECTIVE_ONLY"})
    census = {"available_pit_datasets": [
        {"dataset": "Nasdaq trading halts", "asset_class": "US_EQUITY",
         "state": "FREE_AVAILABLE, 0 hypotheses"}]}
    out = BR._dataset_digest(pipe, census)
    assert out["certified_data_classes"] == [
        "FREE_PUBLIC_SOURCES|FINRA_SHORT_VOLUME|PROSPECTIVE_ONLY"]
    assert out["census_rows_are_a_frozen_claim_not_a_certification"] is True
    # the census row survives, under a name that cannot be read as a verdict
    assert out["census_claims"][0]["claimed_state"] == \
        "FREE_AVAILABLE, 0 hypotheses"
    assert "state" not in out["census_claims"][0]


def test_an_uncertified_store_still_lists_the_census_claims(
        tmp_path, monkeypatch):
    """No certification is not a reason to publish nothing: the director still
    needs the census rows, and their absence from the store is itself the fact."""
    from paper_trader.alpha_agent.agents_v2 import briefs as BR
    pipe = _memory_with_rulings(tmp_path, monkeypatch)
    census = {"available_pit_datasets": [
        {"dataset": "ds", "asset_class": "US_EQUITY", "state": "FREE"}]}
    out = BR._dataset_digest(pipe, census)
    assert out["certified_data_classes"] == []
    assert out["n_certified_data_classes"] == 0
    assert len(out["census_claims"]) == 1


def test_the_de_frozen_brief_still_fits_its_budget(tmp_path, monkeypatch):
    """Both de-freezings land in the same envelope the R79 join overflowed."""
    from paper_trader.alpha_agent.agents_v2 import briefs as BR
    pipe = _memory_with_rulings(tmp_path, monkeypatch)
    brief = BR.director_brief(pipe, run_id="r80", campaign_id="r80")
    assert BR.brief_problems(brief) == []
    assert BR.prose_words(brief) <= BR.MAX_HANDOFF_PROSE_WORDS


def test_a_store_with_no_rulings_still_produces_a_usable_brief(
        tmp_path, monkeypatch):
    """The empty case must not regress into an empty or absent key."""
    from paper_trader.alpha_agent import r59 as _r59
    from paper_trader.alpha_agent.agents_v2 import briefs as BR
    from paper_trader.alpha_agent.agents_v2 import pipeline as _P
    from paper_trader.alpha_agent.r59 import memory as _M
    monkeypatch.setenv(_r59.RESEARCH_ROOT_ENV, str(tmp_path / "r59_root"))
    pipe = _P.AgentPipeline(_M.ResearchMemory(tmp_path / "m.sqlite"),
                            artifact_root=tmp_path / "a")
    oif = BR.director_brief(pipe, run_id="R80", campaign_id="R80_FIXTURE"
                            )["FACTS"]["open_information_families"]
    assert oif["ruled_economic_families"] == []
    assert oif["n_ruled_economic_families"] == 0
