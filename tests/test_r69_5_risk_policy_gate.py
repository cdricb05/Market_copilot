r"""R69.5 - a target that clears a cap the cap itself moved.

R69.1 found it and raised it for manual review. R69.2 made it VISIBLE: both
limits, both risk bases, and a name whose breach closed while its weight did not
move. Neither release could answer the operator's actual question, and the
detector R69.2 shipped answers a narrower one than it appears to:

  * it asks "did this name's weight move?", so a token trim removes a name from
    the list entirely. On 2026-09-23 the full target trimmed AMD by 0.36 points of
    NAV - enough to drop off that list, nowhere near enough to reach the 12% limit
    the current book was judged against;
  * it can say nothing at all about a name that was never in breach before. The
    same target raised ALAB from 1.84% to 3.21% of NAV and added SNDK at 2.88%,
    and both now carry more portfolio risk than the current book's own limit
    allows - two of the four offending names are ones the target CREATED, which no
    denominator argument reaches.

This suite proves the question is now asked and answered: hold the cap still, and
say whether the target complies. When it does not, approval is WITHHELD at a named
state bound to the exact frozen book - not refused, not granted, and with no
threshold moved anywhere.

Every world here is hermetic. No live endpoint, store, ledger, holding, cash or
NAV is touched, and the declared 3/N policy is never edited by a test.
"""
from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from paper_trader.api import paper_trading_desk as desk
from paper_trader.api import portfolio_decision as pdec
from paper_trader.api import proposal_decision_review as apdr
from paper_trader.engine import holding_opportunity_cost as hoc
from paper_trader.engine import proposal_decision_review as kernel
from paper_trader.engine import selected_target as stgt

from tests.test_r69_2_selected_target_lifecycle import (
    BOOK, COST, POLICY, SESSION, _alloc, _buy_fill, _marks,
)

BAND = {"material_weight_delta": 1.0e-4}

#: The covariance policy the R69.2 world uses, plus the sleeve and asset-class caps
#: a real proposal declares for a real sleeve. Without them an undeclared sleeve
#: falls back to the non-equity default of 25% of NAV and this world's book breaches
#: a CONCENTRATION rule that has nothing to do with what is under test here.
POLICY6 = dict(POLICY, sleeve_weight_caps={"hermetic_v1": 1.0, "cash_usd": 1.0},
               asset_class_weight_caps={"US_EQUITY": 1.0, "CASH": 1.0})


# =========================================================================== #
# 1. THE REFERENCE COMPARISON - hold the cap still and ask again
# =========================================================================== #
def _cmp(before: dict, after: dict) -> dict:
    return stgt.risk_contribution_comparison(before_state=before, after_state=after,
                                             policy=BAND)


def _state(weights, contributions, limit, n, breaches=(), **kw):
    st = {"weights": dict(weights), "risk_contributions": dict(contributions),
          "risk_contribution_limit": {"limit": limit, "n_covariance_names": n,
                                      "excess_multiple": 3.0,
                                      "basis": hoc.RISK_CONTRIBUTION_LIMIT_BASIS},
          "risk_contribution_breaches": [{"ticker": t} for t in breaches]}
    st.update(kw)
    return st


def test_01_a_real_reduction_needs_no_policy_ruling():
    """The counter-case first. A breach repaired by genuinely cutting the position
    complies with the ORIGINAL limit too, so nothing is escalated."""
    before = _state({"AAA": 0.20, "BBB": 0.10}, {"AAA": 0.30, "BBB": 0.08},
                    0.12, 25, breaches=["AAA"])
    after = _state({"AAA": 0.06, "BBB": 0.10}, {"AAA": 0.09, "BBB": 0.08},
                   0.15, 20)
    c = _cmp(before, after)
    ref = c["reference_compliance"]
    assert ref["reference_limit"] == 0.12
    assert ref["complies_with_reference"] is True
    assert ref["breached_instruments"] == []
    assert c["discharge_attribution"][0]["code"] == stgt.CLOSED_BY_EXPOSURE
    assert c["discharge_attribution"][0]["compliant_at_before_limit"] is True
    assert c["policy_review"]["required"] is False
    assert c["policy_review"]["state"] == stgt.POLICY_REVIEW_NOT_REQUIRED


def test_02_the_2026_09_23_full_target_shape_is_escalated():
    """The live case, in miniature. AMD is trimmed but not to the reference; DDOG
    is untouched and still above it; ALAB and SNDK are concentrations the target
    CREATED. Four names, one ruling."""
    before = _state(
        {"ALAB": 0.018396, "AMD": 0.050186, "DDOG": 0.041071},
        {"ALAB": 0.074297, "AMD": 0.153497, "DDOG": 0.141492},
        0.12, 25, breaches=["AMD", "DDOG"])
    after = _state(
        {"ALAB": 0.032050, "AMD": 0.046621, "DDOG": 0.041071, "SNDK": 0.028751},
        {"ALAB": 0.143419, "AMD": 0.141519, "DDOG": 0.121337, "SNDK": 0.139548},
        0.15, 20)
    c = _cmp(before, after)
    ref = c["reference_compliance"]

    # The governed verdict is unchanged and still says zero.
    assert c["after"]["breach_count"] == 0
    assert c["limit_relaxed"] is True
    # Held still, the same book breaches on four names.
    assert ref["complies_with_reference"] is False
    assert ref["breached_instruments"] == ["ALAB", "AMD", "DDOG", "SNDK"]
    # Two of them are the target's own doing, and no denominator argument reaches
    # them: neither was in breach against this limit before.
    assert ref["opened_against_reference"] == ["ALAB", "SNDK"]
    assert ref["risk_share_of_reference_breaches"] == pytest.approx(0.545823, abs=1e-6)
    assert ref["nav_share_of_reference_breaches"] == pytest.approx(0.148493, abs=1e-6)

    by = {a["ticker"]: a for a in c["discharge_attribution"]}
    assert by["AMD"]["code"] == stgt.CLOSED_BY_LIMIT_RELAXATION
    assert by["DDOG"]["code"] == stgt.CLOSED_BY_LIMIT_RELAXATION
    assert c["policy_review"]["required"] is True
    assert c["policy_review"]["instruments"] == ["ALAB", "AMD", "DDOG", "SNDK"]
    assert c["policy_review"]["reference_limit"] == 0.12
    assert c["policy_review"]["governed_limit"] == 0.15


def test_03_a_token_trim_does_not_buy_compliance():
    """The blind spot R69.2 left. AMD's weight DID move, so the old detector drops
    it; the reference says the move closed only part of the gap."""
    before = _state({"AMD": 0.050186}, {"AMD": 0.153497}, 0.12, 25, breaches=["AMD"])
    after = _state({"AMD": 0.046621}, {"AMD": 0.141519}, 0.15, 20)
    c = _cmp(before, after)
    # The old sight line sees nothing: the weight moved.
    assert c["discharged_without_reduction"] == []
    # The new one sees the whole of it.
    a = c["discharge_attribution"][0]
    assert a["code"] == stgt.CLOSED_BY_LIMIT_RELAXATION
    assert a["exposure_effect"] == pytest.approx(0.011978, abs=1e-6)
    assert a["limit_effect"] == pytest.approx(0.03, abs=1e-9)
    # The position change did about 28% of the work the reference demanded.
    assert a["exposure_share_of_closure"] == pytest.approx(0.28534, abs=1e-4)
    assert a["compliant_at_before_limit"] is False
    assert a["compliant_at_governed_limit"] is True
    assert c["policy_review"]["required"] is True


def test_04_a_name_the_target_created_is_named_as_such():
    before = _state({"AAA": 0.04}, {"AAA": 0.05}, 0.12, 25)
    after = _state({"AAA": 0.04, "NEW": 0.03}, {"AAA": 0.05, "NEW": 0.14}, 0.15, 20)
    ref = _cmp(before, after)["reference_compliance"]
    assert ref["breached_instruments"] == ["NEW"]
    assert ref["opened_against_reference"] == ["NEW"]
    row = ref["breaches"][0]
    assert row["code"] == stgt.REFERENCE_BREACH_OPENED
    assert row["weight_before"] == 0.0
    assert row["compliant_on_governed_limit"] is True
    # Indicative only, and it says so rather than pretending to be solved.
    assert row["solved"] is False
    assert row["indicative_weight_at_reference"] == pytest.approx(0.03 * 0.12 / 0.14,
                                                                 abs=1e-8)


def test_05_an_unmoved_limit_is_an_ordinary_repair_not_a_policy_question():
    """No relaxation, no escalation. A breach against a limit that did not move is
    a mandatory repair, and the existing gates already own it."""
    before = _state({"AAA": 0.20}, {"AAA": 0.30}, 0.12, 25, breaches=["AAA"])
    after = _state({"AAA": 0.18}, {"AAA": 0.28}, 0.12, 25,
                   breaches=["AAA"])
    c = _cmp(before, after)
    assert c["limit_relaxed"] is False
    assert c["reference_compliance"]["complies_with_reference"] is False
    assert c["policy_review"]["required"] is False
    assert c["discharge_attribution"][0]["code"] == stgt.STILL_IN_BREACH


def test_06_the_two_effects_are_additive_and_cover_the_excess():
    before = _state({"AAA": 0.05}, {"AAA": 0.1535}, 0.12, 25, breaches=["AAA"])
    after = _state({"AAA": 0.05}, {"AAA": 0.2073}, 0.21428571, 14)
    a = _cmp(before, after)["discharge_attribution"][0]
    excess = a["excess_over_before_limit"]
    assert excess == pytest.approx(0.0335, abs=1e-6)
    assert a["exposure_effect"] + a["limit_effect"] >= excess
    # Risk share ROSE: no share of the closure can be attributed to exposure.
    assert a["exposure_effect"] < 0
    assert a["exposure_share_of_closure"] is None
    assert a["code"] == stgt.CLOSED_BY_LIMIT_RELAXATION


def test_07_a_missing_limit_is_reported_not_fabricated():
    before = _state({"AAA": 0.05}, {"AAA": 0.30}, None, 0)
    after = _state({"AAA": 0.05}, {"AAA": 0.30}, 0.15, 20)
    c = _cmp(before, after)
    ref = c["reference_compliance"]
    assert ref["state"] == stgt.REFERENCE_UNAVAILABLE
    assert ref["complies_with_reference"] is None
    assert ref["breaches"] == []
    assert c["policy_review"]["required"] is False


def test_08_the_excess_multiple_reaches_the_block():
    """It was read under the POLICY key's name and was always None, so the one
    number that says WHY the limit is what it is never reached a screen."""
    limit = hoc.risk_contribution_limit(n_covariance_names=25)
    before = _state({"AAA": 0.05}, {"AAA": 0.30}, limit["limit"], 25)
    before["risk_contribution_limit"] = limit
    after = _state({"AAA": 0.05}, {"AAA": 0.30}, 0.15, 20)
    c = _cmp(before, after)
    assert c["before"]["excess_multiple"] == 3.0
    assert c["after"]["excess_multiple"] == 3.0


def test_09_the_comparison_changes_no_threshold():
    before = _state({"AAA": 0.05}, {"AAA": 0.30}, 0.12, 25, breaches=["AAA"])
    after = _state({"AAA": 0.05}, {"AAA": 0.40}, 0.5, 6)
    c = _cmp(before, after)
    assert c["policy_changed_by_this_release"] is False
    assert c["thresholds_changed_by_this_release"] is False
    assert c["measured_here"] is False
    assert c["reference_compliance"]["reference_is_a_new_threshold"] is False
    assert c["reference_compliance"]["measured_here"] is False
    assert c["reference_compliance"]["limit_owner"] == "engine.holding_opportunity_cost"
    pol = c["policy_review"]
    assert pol["policy_changed_here"] is False
    assert pol["exception_granted_here"] is False
    assert pol["target_approved_here"] is False
    assert pol["declared_policy"]["unchanged_by_this_release"] is True
    assert pol["declared_policy"]["basis"] == hoc.RISK_CONTRIBUTION_LIMIT_BASIS


def test_10_the_reference_uses_the_canonical_breach_function():
    """Not a second rule: the same callable the governed verdict comes from."""
    contributions = {"AAA": 0.30, "BBB": 0.05}
    assert [b["ticker"] for b in
            hoc.risk_contribution_breaches(contributions=contributions, limit=0.12)] \
        == ["AAA"]
    before = _state({"AAA": 0.05, "BBB": 0.05}, contributions, 0.12, 25,
                    breaches=["AAA"])
    after = _state({"AAA": 0.05, "BBB": 0.05}, contributions, 0.5, 6)
    ref = _cmp(before, after)["reference_compliance"]
    assert ref["breached_instruments"] == ["AAA"]


# =========================================================================== #
# 2. THE APPROVAL GATE - withheld, bound, and fail-closed
# =========================================================================== #
#: A six-name book whose risk is dominated by ONE holding. Exiting the three the
#: governed retention rule no longer admits shrinks the covariance universe from
#: six names to three and lifts the per-name cap from 0.50 to 1.00 - the same
#: mechanism as the live book, sized so the effect is unambiguous and the test
#: never depends on a fragile third decimal.
HELD6 = {tk: (150, 100.0) for tk in ("H1", "H2", "H3", "H4", "H5", "H6")}
MARKS6 = {tk: px for tk, (_, px) in HELD6.items()}
MARKS6["SPY"] = 400.0
MARKS6["NEW"] = 100.0
BROKEN6 = ("H4", "H5", "H6")


def _returns6(n=70):
    """H1 dominates the covariance WITHOUT swamping it.

    The amplitudes are chosen so H1's share of portfolio risk lands ABOVE the
    six-name cap (3/6 = 0.50) and BELOW the four-name cap (3/4 = 0.75). That is the
    whole mechanism under test: H1 never moves, and the exits alone carry it from
    breach to compliance. Distinct incommensurate frequencies keep the series close
    to orthogonal, so the covariance kernel measures something stable and no test
    here depends on an RNG seed.
    """
    dates = ["2025-%02d-%02d" % (10 + (i // 28), 1 + (i % 28)) for i in range(n)]
    amp = {"H1": 0.024, "H2": 0.010, "H3": 0.010, "H4": 0.010, "H5": 0.010,
           "H6": 0.010, "NEW": 0.010}
    freq = {"H1": 0.37, "H2": 0.71, "H3": 1.13, "H4": 0.23, "H5": 1.61,
            "H6": 0.53, "NEW": 0.89}
    phase = {"H1": 0.0, "H2": 1.1, "H3": 2.2, "H4": 3.3, "H5": 0.7, "H6": 1.9,
             "NEW": 2.8}
    return {"dates": dates,
            "series": {tk: [amp[tk] * math.sin(i * freq[tk] + phase[tk])
                            for i in range(n)] for tk in amp}}


def _measure6(weights):
    """The CANONICAL risk owner, on this world's own return panel.

    The artifact's before/after risk blocks are produced by this kernel in
    production, so the fixture produces them the same way rather than typing in a
    plausible number. Nothing about the answer is chosen here.
    """
    risk = hoc.compute_risk_contributions(weights=weights,
                                          aligned_returns=_returns6(), policy=POLICY6)
    contributions = {k: v for k, v in (risk["contributions"] or {}).items()
                     if v is not None}
    limit = hoc.risk_contribution_limit(
        n_covariance_names=len(risk.get("included_tickers") or []))
    return contributions, limit, hoc.risk_contribution_breaches(
        contributions=contributions, limit=limit["limit"])


def _proposal6(nav):
    cur = {tk: round(q * MARKS6[tk] / nav, 8) for tk, (q, _) in HELD6.items()}
    # The full target exits the three broken names and adds one.
    tgt = {"H1": cur["H1"], "H2": cur["H2"], "H3": cur["H3"], "NEW": 0.04}
    rc_before, limit_before, breaches_before = _measure6(cur)
    rc_after, limit_after, breaches_after = _measure6(tgt)
    allocs = [_alloc("H1", "RETAIN", cur["H1"], tgt["H1"], "Tech", nav),
              _alloc("H2", "RETAIN", cur["H2"], tgt["H2"], "Health", nav),
              _alloc("H3", "RETAIN", cur["H3"], tgt["H3"], "Staples", nav),
              _alloc("NEW", "ADD", 0.0, tgt["NEW"], "Energy", nav)]
    allocs += [_alloc(tk, "EXIT", cur[tk], 0.0, "Utilities", nav,
                      source_hoc_recommendation="EXIT", reason_codes=["HOC_EXIT"])
               for tk in BROKEN6]
    one_way = 0.5 * sum(abs((tgt.get(tk) or 0.0) - (cur.get(tk) or 0.0))
                        for tk in set(cur) | set(tgt))
    return {
        "proposal_hash": "HASH_R695", "proposal_state": "READY",
        "schema_version": "reallocation_proposal.v1",
        "policy_version": "reallocation_allocation_policy.v1",
        "policy": dict(POLICY6), "allocations": allocs,
        "portfolio": {
            "nav": nav,
            "current_cash_weight": round(1.0 - sum(cur.values()), 8),
            "proposed_cash_weight": round(1.0 - sum(tgt.values()), 8),
            "current_holding_count": len(cur), "proposed_holding_count": len(tgt),
            "current_allocation_by_asset_class": {"US_EQUITY": sum(cur.values())},
            "proposed_allocation_by_asset_class": {"US_EQUITY": sum(tgt.values())},
            "current_allocation_by_sleeve": {"hermetic_v1": sum(cur.values())},
            "proposed_allocation_by_sleeve": {"hermetic_v1": sum(tgt.values())}},
        "turnover": {"one_way_turnover": round(one_way, 6),
                     "two_way_turnover": round(2 * one_way, 6),
                     "estimated_transaction_cost": round(2 * one_way * nav * COST, 2)},
        "switching_economics": {"score_before": 0.80, "score_after": 0.90,
                                "score_improvement": 0.10, "score_cost_hurdle": 0.01,
                                "score_improvement_net_of_cost": 0.09,
                                "switching_hurdle": 0.02,
                                "clears_switching_hurdle": True},
        "risk": {"portfolio_volatility_before": 0.15,
                 "volatility_before_state": "AVAILABLE",
                 "portfolio_volatility_after": 0.14,
                 "volatility_after_state": "AVAILABLE",
                 "concentration_before": 0.30, "concentration_after": 0.28,
                 "largest_position_before": cur["H1"],
                 "largest_position_after": tgt["H1"],
                 "sector_concentration_before": cur["H1"],
                 "sector_concentration_after": tgt["H1"],
                 "risk_contributions_before": rc_before,
                 "risk_contributions_after": rc_after},
        "risk_contribution_policy": {
            "limit_held_book": limit_before, "limit_after_target": limit_after,
            "held_book_breaches": breaches_before,
            "after_target_breaches": breaches_after},
        "constraints": {"all_ok": True, "violations": []},
        "complete_target_limits": {"all_ok": True, "breaches": [],
                                   "owner": "engine.reallocation_proposal"},
        "constraint_reoptimization": {"applied": False, "constraints_that_reshaped": [],
                                      "breached_limits": []},
        "mandatory_repair": {"owner": "engine.holding_opportunity_cost",
                             "contract_version": "mandatory_repair_contract.v1",
                             "obligations_resolved": True,
                             "obligations_open_against_target": [],
                             "obligation_count": 4},
        "full_target_reviewable": True,
        "action_counts": {"EXIT": 3, "ADD": 1, "RETAIN": 3},
        "outcome": "TARGET_READY",
    }


def _hoc6():
    rows = [{"ticker": tk, "deterioration_state": "INTACT", "reason_codes": []}
            for tk in ("H1", "H2", "H3")]
    rows += [{"ticker": tk, "deterioration_state": "BROKEN",
              "reason_codes": ["FELL_BELOW_EXIT_BUFFER"],
              "deterioration_reason_codes": ["FELL_BELOW_EXIT_BUFFER"],
              "recommendation": "EXIT", "current_rank": 91} for tk in BROKEN6]
    return {"assessment_hash": "HOC_R695", "holding_reviews": rows}


def _world6(tmp: Path):
    sdir = tmp / "desk"
    sdir.mkdir(parents=True, exist_ok=True)
    book = {"book_id": BOOK, "book_number": 1, "display_name": "Paper Book #1",
            "initial_capital": 100000.0, "execution_model": "NEXT_CLOSE",
            "currency": "USD_PAPER", "benchmark": "SPY", "status": "OPEN",
            "model_id": "hermetic_v1"}
    desk._append_ledger(sdir, desk.BOOKS_FILE, [{"event": "BOOK_CREATED", "book": book}])
    desk._append_ledger(sdir, desk.FILLS_FILE, [
        _buy_fill(tk, q, px, "2026-01-05", "f_%s" % tk) for tk, (q, px) in HELD6.items()])
    _marks(sdir, {tk: [["2026-01-05", px * 0.98], [SESSION, px]]
                  for tk, px in MARKS6.items()}, SESSION)
    nav = desk.book_nav(book, desk._fills(sdir), desk.read_marks(sdir))["nav"]
    prop = _proposal6(nav)
    artifact = {
        "proposal_id": "reap_%s_%s_R695" % (SESSION, BOOK),
        "schema_version": "reallocation_proposal.v1",
        "generated_at": "2026-01-10T21:00:00+00:00",
        "identity": {"active_book_id": BOOK, "eligible_market_date": SESSION,
                     "proposal_hash": "HASH_R695", "portfolio_state_hash": "PSH",
                     "hoc_assessment_hash": "HOC_R695", "universe_scoring_hash": "USH",
                     "allocation_policy_version": "reallocation_allocation_policy.v1"},
        "proposal": prop}
    ddir = tmp / "decisions"
    ddir.mkdir(parents=True, exist_ok=True)
    return sdir, artifact, ddir


def _env6(artifact, ddir=None):
    ident = artifact["identity"]
    payload = {"state": "READY", "proposal_state": "READY", "approvable": True,
               "eligible_market_date": SESSION,
               "active_book": {"book_id": BOOK, "id": BOOK},
               "artifact": {"proposal_id": artifact["proposal_id"], "identity": ident,
                            "generated_at": artifact["generated_at"]}}
    return apdr.load_proposal_decision_review(
        proposal_payload=payload, proposal=artifact["proposal"],
        hoc_assessment=_hoc6(), outcome_evidence={}, aligned_returns=_returns6(),
        decision_dir=(str(ddir) if ddir is not None else None),
        latest_session=SESSION)


def _select6(target, ddir, artifact, **over):
    kw = dict(target=target, confirm=pdec.SELECTION_CONFIRM_TOKEN,
              review_envelope=_env6(artifact, ddir), decision_dir=ddir,
              latest_session=SESSION)
    kw.update(over)
    return pdec.record_target_selection(**kw)


SUMMARY6 = {"reallocation_action_counts": {"EXIT": 3, "ADD": 1},
            "reallocation_one_way_turnover": 0.30}


def _approve6(ddir, artifact, **over):
    kw = dict(decision=pdec.DECISION_APPROVE, confirm=pdec.CONFIRM_TOKEN,
              expected_proposal_hash="HASH_R695", artifact=artifact,
              proposal_summary=SUMMARY6, decision_dir=ddir, latest_session=SESSION,
              actor="test-operator")
    kw.update(over)
    return pdec.record_decision(**kw)


def _ruling(selection, **over):
    review = pdec.selection_policy_review(selection)
    ack = {"token": pdec.RISK_POLICY_ACK_TOKEN,
           "selected_target_implementation_hash": review["implementation_hash"],
           "reference_limit": review["reference_limit"],
           "instruments": list(review["instruments"]),
           "ruling": "ACCEPT_AS_IS", "ruled_by": "test-operator"}
    ack.update(over)
    return ack


def _records(ddir: Path) -> list:
    p = Path(ddir) / "decisions.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else []


def test_20_the_hermetic_world_reproduces_the_denominator_effect(tmp_path):
    """Precondition for everything below, asserted rather than assumed."""
    _, art, _ = _world6(tmp_path)
    blk = _env6(art)["selected_targets"]["FULL_TARGET"]
    rc = blk["risk_contribution"]
    assert rc["before"]["limit"] == pytest.approx(0.5)
    assert rc["after"]["limit"] > rc["before"]["limit"]
    assert rc["limit_relaxed"] is True
    assert rc["after"]["breach_count"] == 0, "compliant on its OWN cap"
    assert rc["reference_compliance"]["complies_with_reference"] is False
    assert "H1" in rc["reference_compliance"]["breached_instruments"]
    assert rc["policy_review"]["required"] is True


def test_21_approval_is_withheld_and_nothing_is_written(tmp_path):
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)
    assert sel["recorded"] is True
    before = _records(ddir)
    got = _approve6(ddir, art, expected_selection_id=sel["record"]["selection_id"])
    assert got["status"] == pdec.PDS_RISK_POLICY_REVIEW_REQUIRED
    assert got["recorded"] is False
    assert got["created_orders"] is False and got["changed_holdings"] is False
    assert got["declared_policy_changed"] is False
    assert got["exception_granted"] is False
    assert got["next_required_action"] == "RECORD_RISK_POLICY_RULING"
    assert got["risk_policy_review"]["required"] is True
    assert got["risk_policy_acknowledgement"]["reason"] == pdec.ACK_MISSING
    assert _records(ddir) == before, "the refusal wrote nothing"


def test_22_reject_and_hold_stay_available(tmp_path):
    """A gate that took away every way to record a judgement would be worse than
    the defect it closes."""
    _, art, ddir = _world6(tmp_path)
    _select6("FULL_TARGET", ddir, art)
    for decision in (pdec.DECISION_REJECT, pdec.DECISION_HOLD):
        got = _approve6(ddir, art, decision=decision)
        assert got["recorded"] is True, decision
        assert got["record"]["decision"] == decision


@pytest.mark.parametrize("over,reason", [
    ({"token": "NOPE"}, pdec.ACK_BAD_TOKEN),
    ({"selected_target_implementation_hash": "0" * 64}, pdec.ACK_WRONG_BOOK),
    ({"reference_limit": 0.9}, pdec.ACK_WRONG_LIMIT),
    ({"instruments": []}, pdec.ACK_WRONG_INSTRUMENTS),
    ({"instruments": ["H1", "ZZZ"]}, pdec.ACK_WRONG_INSTRUMENTS),
])
def test_23_a_ruling_about_something_else_is_refused(tmp_path, over, reason):
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    got = _approve6(ddir, art, risk_policy_acknowledgement=_ruling(sel, **over))
    assert got["status"] == pdec.PDS_RISK_POLICY_REVIEW_REQUIRED
    assert got["recorded"] is False
    assert got["risk_policy_acknowledgement"]["reason"] == reason
    assert _records(ddir) == []


def test_24_a_bound_ruling_lets_the_approval_through_and_is_recorded(tmp_path):
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    got = _approve6(ddir, art, expected_selection_id=sel["selection_id"],
                    risk_policy_acknowledgement=_ruling(sel))
    assert got["recorded"] is True, got.get("message")
    rec = got["record"]
    assert rec["decision"] == pdec.DECISION_APPROVE
    assert rec["selected_target"] == "FULL_TARGET"
    assert rec["risk_policy_review"]["required"] is True
    assert rec["risk_policy_acknowledgement"]["token"] == pdec.RISK_POLICY_ACK_TOKEN
    assert rec["risk_policy_acknowledgement"]["ruling"] == "ACCEPT_AS_IS"
    assert rec["declared_risk_policy_changed"] is False
    assert rec["standing_risk_policy_exception_granted"] is False
    assert rec["risk_policy_contract"].startswith("R69.5")


def test_25_a_selection_recorded_before_this_release_fails_closed(tmp_path):
    """A frozen book that never published the verdict is not asserted compliant.
    Inferring 'fine' from silence is the defect R69.1 documented, one layer down."""
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    legacy = json.loads(json.dumps(sel))
    legacy["selected_target_implementation"]["risk_contribution"].pop("policy_review")
    review = pdec.selection_policy_review(legacy)
    assert review["published"] is False
    assert review["required"] is True
    assert review["reason"] == pdec.ACK_NOT_PUBLISHED
    verdict = pdec.validate_risk_policy_acknowledgement(
        policy_review=review, acknowledgement=_ruling(sel))
    assert verdict["accepted"] is False


def test_26_a_ruling_does_not_survive_a_reselected_book(tmp_path):
    """The ruling names a book. Change the book and the ruling is about something
    that is no longer on the table."""
    _, art, ddir = _world6(tmp_path)
    full = _select6("FULL_TARGET", ddir, art)["record"]
    ruling = _ruling(full)
    repair = _select6("MINIMUM_REPAIR", ddir, art)["record"]
    assert repair["selected_target_implementation_hash"] \
        != full["selected_target_implementation_hash"]
    got = _approve6(ddir, art, risk_policy_acknowledgement=ruling)
    assert got["status"] == pdec.PDS_RISK_POLICY_REVIEW_REQUIRED
    assert got["risk_policy_acknowledgement"]["reason"] == pdec.ACK_WRONG_BOOK
    assert _records(ddir) == []


def test_27_the_gate_does_not_move_the_selected_target_identity(tmp_path):
    """R69.5 publishes a verdict ABOUT the frozen book; it is not part of it. The
    identity hash covers the weights, the rows and the economics, so a selection
    made before this release and re-made after it is the same book."""
    _, art, ddir = _world6(tmp_path)
    blocks = _env6(art)["selected_targets"]
    for target in ("MINIMUM_REPAIR", "FULL_TARGET"):
        blk = blocks[target]
        stripped = json.loads(json.dumps(blk))
        stripped["risk_contribution"].pop("policy_review")
        stripped["risk_contribution"].pop("reference_compliance")
        stripped["risk_contribution"].pop("discharge_attribution")
        assert stgt.selected_target_hash(stripped) == \
            blk["selected_target_implementation_hash"], target


def test_28_the_state_is_in_the_declared_vocabulary(tmp_path):
    assert pdec.PDS_RISK_POLICY_REVIEW_REQUIRED in pdec.DECISION_STATE_VOCAB
    assert pdec.PDS_RISK_POLICY_REVIEW_REQUIRED not in pdec.APPROVABLE_DECISION_STATES
    assert pdec.RISK_POLICY_ACK_TOKEN == stgt.POLICY_REVIEW_ACK_TOKEN
    for reason in (pdec.ACK_MISSING, pdec.ACK_BAD_TOKEN, pdec.ACK_WRONG_BOOK,
                   pdec.ACK_WRONG_LIMIT, pdec.ACK_WRONG_INSTRUMENTS,
                   pdec.ACK_NOT_PUBLISHED):
        assert reason in pdec.ACK_REASON_VOCAB


def test_29_a_compliant_target_needs_no_ruling_at_all(tmp_path):
    """The gate must be invisible when it has nothing to say. The MINIMUM REPAIR
    in the R69.2 world breaches nothing at either limit."""
    from tests.test_r69_2_selected_target_lifecycle import (
        _approve, _select, _world)
    _, _, art, ddir, _ = _world(tmp_path)
    sel = _select("MINIMUM_REPAIR", ddir, art)["record"]
    review = pdec.selection_policy_review(sel)
    assert review["published"] is True
    assert review["required"] is False
    got = _approve(ddir, art, expected_selection_id=sel["selection_id"])
    assert got["recorded"] is True
    assert got["record"]["risk_policy_review"]["required"] is False
    assert got["record"]["risk_policy_acknowledgement"] is None


# =========================================================================== #
# 3. WHAT THE REVIEW SAYS ABOUT ITS OWN SCOPE AND ITS OWN EVIDENCE
# =========================================================================== #
def test_30_the_review_states_the_capital_it_ranged_over(tmp_path):
    _, art, _ = _world6(tmp_path)
    scope = _env6(art)["review"]["capital_scope"]
    assert scope["risk_asset_classes_present"] == ["US_EQUITY"]
    assert scope["single_risk_asset_class"] is True
    assert scope["code"] == "SINGLE_RISK_ASSET_CLASS_PLUS_CASH"
    assert scope["frontier_optimised"] is False
    assert scope["declared_here"] is False
    assert "cross-asset opportunity frontier was searched" in scope["detail"]


def test_31_a_multi_class_book_is_described_as_one():
    """Derived, not declared: the block follows the data without being edited."""
    states = {s: {"allocation_by_asset_class": {"CASH": 0.1, "US_EQUITY": 0.6,
                                                "FX": 0.3},
                  "allocation_by_sleeve": {"cash_usd": 0.1, "eq_v1": 0.6,
                                           "fx_carry_v1": 0.3}}
              for s in kernel.STATE_ORDER}
    scope = kernel.capital_scope(states)
    assert scope["risk_asset_classes_present"] == ["FX", "US_EQUITY"]
    assert scope["single_risk_asset_class"] is False
    assert scope["code"] == "MULTIPLE_RISK_ASSET_CLASSES_PRESENT"
    assert scope["frontier_optimised"] is False


def test_32_cash_alone_is_not_a_second_asset_class():
    states = {s: {"allocation_by_asset_class": {"CASH": 0.9, "US_EQUITY": 0.1}}
              for s in kernel.STATE_ORDER}
    assert kernel.capital_scope(states)["risk_asset_class_count"] == 1


def test_33_the_scope_reaches_the_operator_paragraph(tmp_path):
    _, art, _ = _world6(tmp_path)
    text = _env6(art)["review"]["explanation"]["text"]
    assert "Scope of this comparison" in text
    assert "better within it" in text


def test_34_every_option_carries_what_is_known_about_its_score(tmp_path):
    """The full explanation always said a score is a percentile and that
    replacements have lost every matured comparison. The CHOOSER did not, and the
    chooser is what a surface renders."""
    _, art, _ = _world6(tmp_path)
    sel = _env6(art)["review"]["target_selection"]
    assert sel["score_converted_to_dollars"] is False
    for opt in sel["options"]:
        assert opt["score_is_a_percentile_not_a_return"] is True
        assert opt["score_converted_to_dollars"] is False
        assert opt["expected_return_state"] == "NOT_CALIBRATED"
        assert opt["expected_return"] is None
        assert "invested_weight" in opt


def test_35_adverse_matured_evidence_travels_onto_the_change_options():
    states = {s: {"constraint_status": {}, "positions": 1} for s in kernel.STATE_ORDER}
    evidence = {"cautions": [
        {"code": "REPLACEMENT_EVIDENCE_ADVERSE",
         "detail": "Replacements have underperformed in 6 of 6 matured comparisons."}]}
    sel = kernel.target_selection_options(
        states=states, verdict={"verdict": kernel.VERDICT_FULL_TARGET_REVIEWABLE},
        margins={}, repair={}, read_state="READY", evidence=evidence)
    by = {o["target"]: o for o in sel["options"]}
    assert by["FULL_TARGET"]["evidence_caution_codes"] == ["REPLACEMENT_EVIDENCE_ADVERSE"]
    assert by["MINIMUM_REPAIR"]["evidence_caution_codes"] == [
        "REPLACEMENT_EVIDENCE_ADVERSE"]
    # Doing nothing is not a replacement, so replacement evidence is not hung on it.
    assert by["CURRENT"]["evidence_caution_codes"] == []
    assert sel["evidence_caution_codes"] == ["REPLACEMENT_EVIDENCE_ADVERSE"]


# =========================================================================== #
# 4. SAFETY
# =========================================================================== #
def test_40_the_withheld_path_touches_no_ledger(tmp_path):
    sdir, art, ddir = _world6(tmp_path)
    n_orders = len(desk._read_ledger(sdir, desk.ORDERS_FILE))
    n_fills = len(desk._read_ledger(sdir, desk.FILLS_FILE))
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    got = _approve6(ddir, art, expected_selection_id=sel["selection_id"])
    assert got["status"] == pdec.PDS_RISK_POLICY_REVIEW_REQUIRED
    assert len(desk._read_ledger(sdir, desk.ORDERS_FILE)) == n_orders
    assert len(desk._read_ledger(sdir, desk.FILLS_FILE)) == n_fills
    assert got["paper_only"] is True and got["automation_off"] is True
    assert got["manual_review"] is True


def test_41_the_review_still_approves_nothing(tmp_path):
    _, art, _ = _world6(tmp_path)
    env = _env6(art)
    assert env["writes_nothing"] is True
    assert env["manual_approval_required"] is True
    blk = env["selected_targets"]["FULL_TARGET"]
    for flag in ("is_an_approval", "creates_order_plan", "creates_orders",
                 "deploys_capital", "is_an_optimiser", "second_risk_engine"):
        assert blk[flag] is False, flag
    assert blk["risk_contribution"]["policy_review"]["target_approved_here"] is False
