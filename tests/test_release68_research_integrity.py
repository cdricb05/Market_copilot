r"""Release 68 - RESEARCH INTEGRITY: a check that is performed, not claimed.

Two defects, both of the same shape: a structurally-valid claim carrying a false
value, accepted because nothing could evaluate it.

    THE LEAKAGE CHECK.   R67 published a feature set with leakage_check="PASS"
                         whose formulas read session t against a layer declaring
                         SIGNAL_LAG_SESSIONS = 1. Each feature's "lag" label and
                         its own "formula" contradicted each other inside one
                         JSON object. publish_features enforced the SHAPE of the
                         claim and could not evaluate its TRUTH.

    THE CENSUS.          NEXT_CAMPAIGN_CENSUS.json summarised a mechanism from a
                         PARTIAL key, so a measured, failed mechanism appeared
                         in queued_hypotheses as untested - and was acted on.

Both are now answerable by arithmetic against primary evidence. These tests use
the REAL artifacts from those incidents, verbatim, because a synthetic example
would prove only that the checker rejects synthetic examples.

Nothing here runs an experiment, reads an evaluation sample, pre-registers,
settles, promotes a model, allocates capital or creates an order.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]

from paper_trader.alpha_agent.agents_v2 import leakage as LK          # noqa: E402
from paper_trader.alpha_agent.r59 import memory as M                  # noqa: E402

#: The R67 feature, verbatim from
#: research/agents/campaign_r67_multi_strategy/FEATURE_SET_LEAKAGE_CORRECTION.json
R67_LEAKY_FORMULA = ("residual_6A_t = ret_6A_t - beta_6A_t * "
                     "(equal_weighted_sum(ret_i_t for i in ['GC', 'HG', 'SI']))")
R67_FEATURE = {"name": "fs_r67_terms_of_trade_residual_6A",
               "lag": "1 session", "source": "r38_native_contract_layer",
               "formula": R67_LEAKY_FORMULA}
#: The R38 layer's own declaration (data_r38.py:116-121).
R38_TIMING = {"signal_lag_sessions": 1, "oi_latest_usable_offset": 2}


# =========================================================================== #
# 1. THE LEAKAGE CHECK
# =========================================================================== #
def test_the_exact_r67_feature_is_refused_on_its_own_text():
    """The incident, replayed. This is the assertion that would have caught it."""
    r = LK.check_feature(R67_FEATURE, R38_TIMING)
    assert r["passes"] is False
    assert r["verdict"] == LK.REFUSED_LAG_VIOLATION
    assert r["effective_lag"] == 0, (
        "only beta was lagged; every return term reads session t")
    assert r["required_lag"] == 1


def test_the_corrected_form_of_the_same_feature_passes():
    """The checker must not simply refuse everything."""
    fixed = dict(R67_FEATURE,
                 formula=("residual_6A_{t-1} = ret_6A_{t-1} - beta_6A_{t-1} * "
                          "equal_weighted_sum(ret_i_{t-1})"))
    r = LK.check_feature(fixed, R38_TIMING)
    assert r["passes"] is True, r
    assert r["effective_lag"] == 1


def test_a_label_that_disagrees_with_its_own_formula_is_refused():
    """The R67 defect's other half: two fields of one object contradicting."""
    r = LK.check_feature(
        {"name": "x", "lag": "1 session", "source": "s",
         "formula": "score_{t-5} = ret_{t-5}"}, R38_TIMING)
    assert r["verdict"] == LK.REFUSED_LABEL_DISAGREES
    assert r["passes"] is False


def test_open_interest_is_held_to_the_two_session_offset():
    """OI_LATEST_USABLE_OFFSET = 2, because the exchange publishes s on s+1."""
    ok = LK.check_feature(
        {"name": "oi_growth", "lag": "2 sessions", "source": "r38",
         "formula": "open_interest_{t-2} / open_interest_{t-23} - 1"}, R38_TIMING)
    assert ok["passes"] is True, ok
    assert ok["required_lag"] == 2
    bad = LK.check_feature(
        {"name": "oi_growth", "lag": "1 session", "source": "r38",
         "formula": "open_interest_{t-1} / open_interest_{t-22} - 1"}, R38_TIMING)
    assert bad["passes"] is False
    assert bad["required_lag"] == 2


def test_an_unparseable_formula_is_refused_not_passed():
    """'I could not check this' and 'this is fine' are different answers."""
    r = LK.check_feature(
        {"name": "x", "lag": "1 session", "source": "s",
         "formula": "some prose about the feature"}, R38_TIMING)
    assert r["verdict"] == LK.REFUSED_UNPARSEABLE
    assert r["passes"] is False


def test_a_missing_formula_is_recorded_as_unverifiable_not_silently_passed():
    """The GRADED answer, and why it is graded rather than a refusal.

    ``formula`` has never been a required lineage field, so refusing every
    feature without one would reject essentially every feature set the estate
    has ever published - a change far larger than closing the R67 defect, and
    one that would block research rather than protect it. So it publishes, and
    publishes WITH THE FACT ATTACHED: not verified, named, and copied into the
    frozen spec of every experiment built on it. The gap is now measurable
    instead of invisible, which is the precondition for closing it.
    """
    r = LK.check_feature({"name": "x", "lag": "1 session", "source": "s"},
                         R38_TIMING)
    assert r["verdict"] == LK.NOT_MACHINE_VERIFIABLE
    assert r["machine_verifiable"] is False

    out = LK.check_feature_set(features=[{"name": "x", "lag": "1 session",
                                          "source": "s"}],
                               timing_rule=R38_TIMING, asserted="PASS")
    assert out["publishable"] is True, "an old-style feature set still publishes"
    assert out["verified"] is False, "and is NOT verified - the two are different"
    assert out["not_machine_verifiable"] == ["x"]


def test_a_leaky_feature_is_refused_even_beside_unverifiable_ones():
    """The escape hatch must not launder a real violation."""
    out = LK.check_feature_set(
        features=[{"name": "plain", "lag": "1 session", "source": "s"},
                  R67_FEATURE],
        timing_rule=R38_TIMING, asserted="PASS")
    assert out["publishable"] is False
    assert out["verdict"] == LK.REFUSED_LAG_VIOLATION


def test_the_tightening_path_is_one_declared_constant():
    """The remaining gap is named in code, not left as an accident."""
    assert LK.REQUIRE_MACHINE_VERIFIABLE_FORMULA is False
    src = (REPO / "alpha_agent/agents_v2/leakage.py").read_text(encoding="utf-8")
    assert "Flipping this constant to True makes a formula" in src


def test_every_experiment_carries_whether_its_features_were_verified():
    """An unverifiable feature set cannot be mistaken for a verified one later."""
    src = (REPO / "alpha_agent/agents_v2/pipeline.py").read_text(encoding="utf-8")
    assert 'spec["leakage_machine_verified"]' in src
    assert 'spec["leakage_verification_verdict"]' in src


def test_an_undeclared_timing_rule_gets_the_STRICTEST_default():
    """Defaulting to 0 would make an undeclared dataset the easiest place to leak."""
    assert LK.DEFAULT_SIGNAL_LAG_SESSIONS == 1
    r = LK.check_feature(R67_FEATURE, None)
    assert r["passes"] is False
    assert r["required_lag"] == 1


def test_the_authors_own_PASS_is_reported_and_never_consulted():
    out = LK.check_feature_set(features=[R67_FEATURE], timing_rule=R38_TIMING,
                               dataset_id="r38", asserted="PASS")
    assert out["verified"] is False
    assert out["asserted_by_the_author"] == "PASS"
    assert out["author_assertion_contradicted"] is True


def test_the_governed_pipeline_refuses_a_false_PASS():
    """The gate itself, not just the checker. A refusal here is the whole point."""
    src = (REPO / "alpha_agent/agents_v2/pipeline.py").read_text(encoding="utf-8")
    assert "LEAKAGE_CHECK_FAILED_VERIFICATION" in src
    assert "_leakage.check_feature_set(" in src
    # and the verification travels WITH the published event, so a later reader
    # can see what was verified rather than that something was.
    assert "leakage_verification" in src
    assert "effective_lag_by_feature" in src


def test_certify_data_records_a_machine_readable_timing_rule():
    """A sentence cannot be checked against a formula. A dict can."""
    src = (REPO / "alpha_agent/agents_v2/pipeline.py").read_text(encoding="utf-8")
    assert "timing_rule: dict = None" in src
    assert 'detail["timing_rule"] = dict(timing_rule or {})' in src


def test_the_r67_correction_artifact_still_names_the_defect():
    """The append-only correction is never deleted, and this proves it is there."""
    p = (REPO / "research/agents/campaign_r67_multi_strategy"
         / "FEATURE_SET_LEAKAGE_CORRECTION.json")
    doc = json.loads(p.read_text(encoding="utf-8"))
    assert "LEAKY" in doc["VERDICT"]
    assert doc["what_was_contaminated"]["experiments_preregistered_against_it"] == 0


# =========================================================================== #
# 2. THE CENSUS AND MECHANISM-LEVEL NOVELTY
# =========================================================================== #
@pytest.fixture(scope="module")
def mem():
    return M.ResearchMemory()


def test_mechanism_state_sees_what_is_novel_could_not(mem):
    """The measured defect: novel=True for a mechanism that carries a settled row.

    is_novel hashes (family, spec). A sibling that tests the same mechanism with
    a different parameter hashes differently and is invisible to it. That is not
    a bug in is_novel - it is the wrong question to ask alone.
    """
    keys = dict(asset_class="US_EQUITY",
                economic_family="FUNDAMENTAL_MOMENTUM",
                information_family="DIVIDEND_DECLARATION_EVENTS")
    st = mem.mechanism_state(**keys)
    assert st["mechanism_is_settled"] is True
    assert st["n_settled"] >= 1
    n = mem.is_novel(family="FUNDAMENTAL_MOMENTUM",
                     spec={"a_parameter_nobody_has_used": 1}, **keys)
    assert n["novel"] is True, "a fresh spec hash is, correctly, novel"
    assert n["novel_but_the_mechanism_is_settled"] is True
    assert "mechanism_warning" in n


def test_the_open_interest_mechanism_r67_was_told_was_untested_is_settled(mem):
    """The original sighting, asserted against primary evidence."""
    st = mem.mechanism_state(asset_class="COMMODITY_FUTURES",
                             economic_family="POSITIONING",
                             information_family="EXCHANGE_OPEN_INTEREST")
    assert st["n_settled"] == 4
    assert set(st["outcomes"]) <= {"NO_ALPHA_EVIDENCE", "REJECTED"}
    assert st["mechanism_is_settled"] is True


def test_is_novel_is_unchanged_for_a_caller_that_asks_nothing_extra(mem):
    """Three live callers depend on this contract; R68 must not move it."""
    out = mem.is_novel(family="A_FAMILY", spec={"x": 1})
    assert set(out) == {"novel", "hypothesis_id", "prior"}
    assert out["novel"] is True
    assert "mechanism" not in out


def test_the_census_command_emits_the_full_mechanism_key():
    src = (REPO / "scripts/alpha_agents_v2.py").read_text(encoding="utf-8")
    assert "settled_mechanism_keys" in src
    assert "mechanism_key_rule" in src
    assert "check-mechanism" in src
    assert "_check_mechanism(" in src


def test_the_census_document_carries_its_correction():
    """Append-only: the false claims stay visible beside what memory holds."""
    doc = json.loads((REPO / "research/agents/NEXT_CAMPAIGN_CENSUS.json")
                     .read_text(encoding="utf-8"))
    corr = doc["R68_CENSUS_CORRECTION"]
    assert corr["n_queued_proposals_checked"] >= 5
    breakdown = corr["verdict_breakdown"]
    assert "FUT_OPEN_INTEREST_GROWTH" in breakdown["outright_false_novelty_claim"]
    # And the correction does NOT overstate itself: the proposal checked on a
    # coarser key is reported as such rather than as a settled mechanism.
    assert breakdown["economic_family_settled_but_mechanism_not_established"]
    # The original claims are still there, unedited.
    queued = {r["proposal"]: r for r in doc["queued_hypotheses"]}
    oi = queued["FUT_OPEN_INTEREST_GROWTH"]
    assert "not_a_repeat_because" in oi
    assert oi["R68_MECHANISM_CHECK"]["the_novelty_claim_above_is_false"] is True


def test_the_invalid_r67_feature_set_cannot_be_used_in_research():
    """It is refused by arithmetic wherever it is offered, not by a blocklist."""
    out = LK.check_feature_set(features=[R67_FEATURE], timing_rule=R38_TIMING,
                               dataset_id="r38_native_contract_layer",
                               asserted="PASS")
    assert out["verified"] is False
    assert out["publishable"] is False, "it cannot be published at all"
    assert out["verdict"] == LK.REFUSED_LAG_VIOLATION
    assert out["n_failed"] == 1
