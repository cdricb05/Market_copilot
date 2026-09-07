"""R59 continuation - governed reallocation coherence.

Two defects reached an operator review of session 2026-09-04, both introduced
by R54.2.4 and both proven read-only before this patch:

1. THE NARRATIVE ASSERTED A VERDICT THE GATE DID NOT REACH. The mandatory-exit
   branch of ``engine.portfolio_reassessment.explain_portfolio`` said "the
   expected net improvement of +0.081 score points does not clear the 0.050
   economic hurdle" with no comparison behind it, while its own reason codes
   carried ``PORTFOLIO_NET_IMPROVEMENT_CLEARS_HURDLE``. The sibling
   held-name-breach branch has always guarded the identical sentence; the guard
   was simply never applied to its twin. These tests are that missing twin.

2. REPLACEMENT LABELS DID NOT FOLLOW THE REPAIRED WEIGHTS. The turnover budget
   deferred the HST and DVN exits while keeping the EXPD and SNDK buys, and the
   proposal went on publishing "EXPD replaces HST" beside "HST RETAIN" - two
   REPLACE_IN rows against zero REPLACE_OUT. The weights were feasible and
   correct throughout; only the labels were false, which is worse than a wrong
   number because it reads as a swap the plan will not perform.

WORKTREE IMPORT. The venv's editable finder maps ``paper_trader`` to the LIVE
C: checkout, so a test importing ``paper_trader.engine`` here would exercise
the deployed tree and prove nothing about this worktree's fix. Both modules
under test are therefore loaded as top-level ``engine.*`` from THIS repository
and the resolution is asserted, exactly as the R59 modules do. Their unchanged
dependencies (``holding_opportunity_cost``, ``constrained_reallocation``) are
byte-identical across the two trees.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import engine.portfolio_reassessment as ERA  # noqa: E402
import engine.reallocation_proposal as ERP  # noqa: E402


def test_the_modules_under_test_are_this_worktrees_not_the_live_checkouts():
    assert Path(ERA.__file__).resolve().parent == _ROOT / "engine"
    assert Path(ERP.__file__).resolve().parent == _ROOT / "engine"


# --------------------------------------------------------------------------- #
# Defect 1 - the narrative must be selected by the comparison
# --------------------------------------------------------------------------- #
def _mandatory_exit_decision(net):
    return {"actionable_holding_count": 12,
            "expected_net_improvement": net,
            "expected_one_way_turnover": 0.457125,
            "expected_transaction_cost_usd": 112.24,
            "mandatory_exit_tickers": ["AIZ", "CAT", "CVS"],
            "held_name_constraint_breaches": [],
            "reason_codes": [ERA.GATE_MANDATORY_EXIT,
                             "PORTFOLIO_NET_IMPROVEMENT_CLEARS_HURDLE"],
            "blockers": []}


def _explain(net, hurdle=0.05):
    return ERA.explain_portfolio(
        {"reassessment_state": ERA.STATE_PROPOSAL_READY,
         "decision": _mandatory_exit_decision(net)},
        {"min_portfolio_net_improvement": hurdle})


def test_mandatory_exit_with_net_above_hurdle_says_it_clears():
    """The exact 2026-09-04 case: +0.081 against a 0.050 hurdle."""
    s = _explain(0.081451)
    assert "also clears the 0.050 economic hurdle" in s
    assert "does not clear" not in s
    # The trigger is still stated: the retention breach is why a COMPLETE
    # target is requested, and that does not change with the economics.
    assert "no longer meet the HOC retention rule" in s
    assert "constraint breach, not an alpha bet" in s


def test_mandatory_exit_with_net_below_hurdle_says_it_does_not_clear():
    s = _explain(0.012)
    assert "does not clear the 0.050 economic hurdle on its own" in s
    assert "also clears" not in s


def test_the_boundary_is_treated_as_clearing():
    """Exactly at the hurdle clears, matching the gate's own tolerance."""
    assert ERA._clears_hurdle(0.05, 0.05) is True
    assert ERA._clears_hurdle(0.05 - 1e-13, 0.05) is True
    assert ERA._clears_hurdle(0.049, 0.05) is False
    assert ERA._clears_hurdle(None, 0.05) is False
    assert "also clears" in _explain(0.05)


def test_the_narrative_labels_its_figures_as_a_non_binding_estimate():
    """A number without its scope is how 45.7% and 35.0% looked like a defect."""
    for net in (0.081451, 0.012):
        s = _explain(net)
        assert ERA.NARRATIVE_ESTIMATE_SCOPE in s
        assert ERA.NARRATIVE_ESTIMATE_BINDING in s
        assert "engine.reallocation_proposal" in s
    assert ERA.NARRATIVE_ESTIMATE_SCOPE == "PRE_PROPOSAL_RELEASE_SET_ESTIMATE"
    assert ERA.NARRATIVE_ESTIMATE_BINDING == "NON_BINDING"


def test_every_proposal_ready_sentence_carries_the_scope():
    """All three PROPOSAL_READY branches quote the same non-binding figures."""
    breach = {"actionable_holding_count": 0, "expected_net_improvement": -0.0009,
              "expected_one_way_turnover": 0.1219,
              "expected_transaction_cost_usd": 30.21,
              "mandatory_exit_tickers": [],
              "held_name_constraint_breaches": ["AMD:RISK_CONTRIBUTION_BREACH"],
              "reason_codes": [ERA.GATE_HELD_NAME_BREACH_REQUIRES_TARGET],
              "blockers": []}
    generic = dict(breach, expected_net_improvement=0.09,
                   held_name_constraint_breaches=[], reason_codes=[])
    pol = {"min_portfolio_net_improvement": 0.05}
    for d in (breach, generic):
        s = ERA.explain_portfolio(
            {"reassessment_state": ERA.STATE_PROPOSAL_READY, "decision": d}, pol)
        assert ERA.NARRATIVE_ESTIMATE_SCOPE in s


def test_the_breach_branch_keeps_its_own_guard_and_wording():
    """The sibling that was already correct must not regress."""
    d = {"actionable_holding_count": 0, "expected_net_improvement": -0.000894,
         "expected_one_way_turnover": 0.121918,
         "expected_transaction_cost_usd": 30.21,
         "mandatory_exit_tickers": [],
         "held_name_constraint_breaches": ["AMD:RISK_CONTRIBUTION_BREACH"],
         "reason_codes": [ERA.GATE_HELD_NAME_BREACH_REQUIRES_TARGET,
                          "NO_ACTIONABLE_HOLDING"],
         "blockers": []}
    s = ERA.explain_portfolio(
        {"reassessment_state": ERA.STATE_PROPOSAL_READY, "decision": d},
        {"min_portfolio_net_improvement": 0.05})
    assert "constraint fact, not an economic verdict" in s
    assert "economically justified" not in s


def test_the_gate_itself_is_untouched_by_the_narrative_fix():
    """Only the sentence changed. No reason code, state or threshold moved."""
    src = (_ROOT / "engine" / "portfolio_reassessment.py").read_text(
        encoding="utf-8")
    assert ERA.GATE_MANDATORY_EXIT == "MANDATORY_EXIT_INELIGIBLE_HOLDING"
    assert ERA.STATE_PROPOSAL_READY == "PROPOSAL_READY"
    # The comparison helper is used by the narrative only; the gate keeps
    # deciding with its own logic.
    assert src.count("def _clears_hurdle") == 1
    assert "turnover_budget_binding_here" in src


# --------------------------------------------------------------------------- #
# Defect 2 - labels follow the repaired weights
# --------------------------------------------------------------------------- #
_POL = {"material_weight_delta": 0.002}


def _reopt(action, *, proposed, held, delta=0.04, counterparty=None,
           codes=None):
    return ERP._reoptimised_action(
        ticker="X", action=action, reason_codes=list(codes or []),
        delta=delta, proposed=proposed, held=held, policy=_POL,
        counterparty_proposed=counterparty)


def test_replace_in_survives_when_its_counterparty_actually_exits():
    action, codes = _reopt(ERP.ACT_REPLACE_IN, proposed=0.04, held=False,
                           counterparty=0.0,
                           codes=["FUNDS_REPLACEMENT_OF_HST"])
    assert action == ERP.ACT_REPLACE_IN
    assert "FUNDS_REPLACEMENT_OF_HST" in codes


def test_replace_in_becomes_add_when_its_counterparty_is_retained():
    """The EXPD/HST case: the buy survived, the exit was deferred."""
    action, codes = _reopt(ERP.ACT_REPLACE_IN, proposed=0.04, held=False,
                           counterparty=0.039289,
                           codes=["FUNDS_REPLACEMENT_OF_HST"])
    assert action == ERP.ACT_ADD
    assert not [c for c in codes if c.startswith("FUNDS_REPLACEMENT_OF_")]
    assert ERP.CODE_REPLACEMENT_COUNTERPARTY_RETAINED in codes


def test_an_unprovable_counterparty_fails_closed_to_add():
    """ADD is true of every funded new position; REPLACE_IN must be earned."""
    action, codes = _reopt(ERP.ACT_REPLACE_IN, proposed=0.04, held=False,
                           counterparty=None,
                           codes=["FUNDS_REPLACEMENT_OF_HST"])
    assert action == ERP.ACT_ADD
    assert ERP.CODE_REPLACEMENT_COUNTERPARTY_RETAINED in codes


def test_a_replace_out_keeps_its_semantics_when_its_counterparty_enters():
    action, codes = _reopt(ERP.ACT_REPLACE_OUT, proposed=0.0, held=True,
                           delta=-0.04, counterparty=0.04,
                           codes=["REPLACED_BY_EXPD"])
    assert action == ERP.ACT_REPLACE_OUT
    assert "REPLACED_BY_EXPD" in codes


def test_a_replace_out_becomes_a_plain_exit_when_its_counterparty_is_deferred():
    """The mirror orphan: the incumbent still leaves, the buy never happened."""
    action, codes = _reopt(ERP.ACT_REPLACE_OUT, proposed=0.0, held=True,
                           delta=-0.04, counterparty=0.0,
                           codes=["REPLACED_BY_EXPD"])
    assert action == ERP.ACT_EXIT
    assert not [c for c in codes if c.startswith("REPLACED_BY_")]
    assert ERP.CODE_REPLACEMENT_COUNTERPARTY_DEFERRED in codes


def test_an_unprovable_counterparty_fails_closed_to_exit():
    action, _ = _reopt(ERP.ACT_REPLACE_OUT, proposed=0.0, held=True,
                       delta=-0.04, counterparty=None,
                       codes=["REPLACED_BY_EXPD"])
    assert action == ERP.ACT_EXIT


def test_an_orphan_replace_out_is_a_measured_violation():
    allocations = [
        _row("HST", "REPLACE_OUT", 0.0, held=True,
             rel={"role": "REPLACE_OUT", "counterparty": "EXPD"}),
        _row("BBB", "RETAIN", 0.04, held=True),
    ]
    constraints, violations = _validate(allocations)
    codes = {v["code"] for v in violations}
    assert constraints["replacement_pairing_ok"] is False
    assert "REPLACE_OUT_COUNTERPARTY_ABSENT" in codes


def test_a_deferred_replace_out_is_reclassified_from_its_weights():
    """HST: repaired back to its current weight, so it is simply retained."""
    action, _ = _reopt(ERP.ACT_REPLACE_OUT, proposed=0.039289, held=True,
                       delta=0.0, codes=["REPLACED_BY_EXPD"])
    assert action == ERP.ACT_RETAIN


def test_the_claim_stripper_keeps_provenance_and_drops_the_claim():
    """HOC_REPLACE records what HOC asked for and stays true; the pairing does not."""
    codes = ERP._strip_replacement_claim(
        ["HOC_REPLACE", "REPLACED_BY_EXPD", "CONSTRAINT_REOPTIMIZED"],
        added=ERP.CODE_REPLACE_DEFERRED_BY_REPAIR)
    assert "HOC_REPLACE" in codes
    assert "CONSTRAINT_REOPTIMIZED" in codes
    assert "REPLACED_BY_EXPD" not in codes
    assert ERP.CODE_REPLACE_DEFERRED_BY_REPAIR in codes


# --------------------------------------------------------------------------- #
# The invariant, measured on the built proposal
# --------------------------------------------------------------------------- #
def _validate(allocations, proposed_weight=None):
    weights = proposed_weight or {r["ticker"]: r.get("proposed_weight", 0.0)
                                  for r in allocations}
    return ERP._validate_constraints(
        allocations=allocations, proposed_weight=weights,
        sector_of={r["ticker"]: "Tech" for r in allocations},
        held_set={r["ticker"] for r in allocations if r.get("held")},
        nav=100000.0, cash=0.0, N=25,
        policy=dict(ERP.default_policy(), sector_cap_fraction=1.0,
                    max_name_weight=0.5))


def _row(tk, action, pw, *, held, rel=None):
    return {"ticker": tk, "action": action, "proposed_weight": pw,
            "held": held, "replacement_relationship": rel}


def test_an_orphan_replace_in_is_a_measured_violation():
    """The exact 2026-09-04 shape: REPLACE_IN 2, REPLACE_OUT 0."""
    allocations = [
        _row("EXPD", "REPLACE_IN", 0.04, held=False,
             rel={"role": "REPLACE_IN", "counterparty": "HST"}),
        _row("HST", "RETAIN", 0.039289, held=True),
    ]
    constraints, violations = _validate(allocations)
    codes = {v["code"] for v in violations}
    assert constraints["replacement_pairing_ok"] is False
    assert constraints["all_ok"] is False
    assert "REPLACE_IN_COUNTERPARTY_RETAINED" in codes
    assert "ORPHAN_REPLACEMENT_LEGS" in codes


def test_a_retain_row_may_never_carry_a_replacement_relationship():
    allocations = [
        _row("HST", "RETAIN", 0.039289, held=True,
             rel={"role": "REPLACE_OUT", "counterparty": "EXPD"}),
    ]
    constraints, violations = _validate(allocations)
    assert constraints["replacement_pairing_ok"] is False
    assert "REPLACEMENT_RELATIONSHIP_ON_NON_REPLACE_ROW" in {
        v["code"] for v in violations}


def test_a_properly_paired_replacement_passes():
    allocations = [
        _row("EXPD", "REPLACE_IN", 0.04, held=False,
             rel={"role": "REPLACE_IN", "counterparty": "HST"}),
        _row("HST", "REPLACE_OUT", 0.0, held=True,
             rel={"role": "REPLACE_OUT", "counterparty": "EXPD"}),
    ]
    constraints, violations = _validate(allocations)
    assert constraints["replacement_pairing_ok"] is True
    assert not [v for v in violations
                if "REPLACE" in v["code"] or "ORPHAN" in v["code"]]


def test_a_replace_leg_without_a_counterparty_is_a_violation():
    allocations = [_row("EXPD", "REPLACE_IN", 0.04, held=False, rel=None)]
    constraints, violations = _validate(allocations)
    assert constraints["replacement_pairing_ok"] is False
    assert "REPLACE_WITHOUT_COUNTERPARTY" in {v["code"] for v in violations}


def test_a_plain_add_and_retain_book_is_unaffected():
    """The fix must not invent violations in the ordinary case."""
    allocations = [_row("AAA", "ADD", 0.04, held=False),
                   _row("BBB", "RETAIN", 0.04, held=True),
                   _row("CCC", "EXIT", 0.0, held=True)]
    constraints, violations = _validate(allocations)
    assert constraints["replacement_pairing_ok"] is True
    assert constraints["all_ok"] is True
    assert violations == []


# --------------------------------------------------------------------------- #
# End to end: a deferred exit must not leave a replacement behind
# --------------------------------------------------------------------------- #
def _rets(n, seed):
    out, x = [], float(seed)
    for i in range(n):
        x = (x * 1103515245 + 12345) % 2147483648
        out.append(((x / 2147483648.0) - 0.5) * 0.04)
    return out


def _aligned(tickers, n=80):
    return {"dates": ["d%03d" % i for i in range(n)],
            "series": {tk: _rets(n, seed=i + 1) for i, tk in enumerate(tickers)}}


def _position(tk, sector, w, mv):
    return {"ticker": tk, "sector": sector, "current_weight": w,
            "market_value": mv, "quantity": 100, "price": mv / 100.0}


def _urow(tk, rank, pct, sector="Tech", adv=5e8, eligible=True):
    return {"ticker": tk, "rank": rank, "percentile": pct, "combined_score": pct,
            "sector": sector, "adv_dollar": adv, "eligible": eligible}


def _review(tk, rec, rank, pct, sector="Tech", repl=None):
    return {"ticker": tk, "recommendation": rec, "current_rank": rank,
            "current_score": pct, "signal_strength": pct,
            "strongest_replacement_ticker": repl, "drawdown_60d": -0.1,
            "liquidity_state": "LIQUID", "switching_cost_usd": 10.0,
            "net_improvement": 0.5, "risk_contribution": 0.1}


def _ic(**over):
    held = ["AAA", "BBB", "CCC", "DDD"]
    cands = ["EEE", "FFF", "GGG", "HHH"]
    ic = {
        "schema_version": ERP.INPUT_SCHEMA_VERSION,
        "eligible_market_date": "2026-09-04",
        "active_book_id": "alpha_paper_book_1",
        "nav": 100000.0, "cash": 0.0,
        "portfolio_state_hash": "PSH", "universe_scoring_hash": "USH",
        "universe_input_contract_hash": "UIC",
        "hoc_assessment_hash": "HOC1", "hoc_assessment_state": "READY",
        "hoc_available": True, "hoc_data_gaps": [],
        "positions": [_position("AAA", "Tech", 0.25, 25000.0),
                      _position("BBB", "Tech", 0.25, 25000.0),
                      _position("CCC", "Fin", 0.25, 25000.0),
                      _position("DDD", "Fin", 0.25, 25000.0)],
        # Every held name is a REPLACE: the ideal target pairs each with a
        # candidate, and a tight turnover budget then defers most of the exits.
        "hoc_reviews": [_review("AAA", "REPLACE", 90, 0.05, repl="EEE"),
                        _review("BBB", "REPLACE", 91, 0.04, repl="FFF"),
                        _review("CCC", "REPLACE", 92, 0.03, sector="Fin",
                                repl="GGG"),
                        _review("DDD", "REPLACE", 93, 0.02, sector="Fin",
                                repl="HHH")],
        "universe_rows": [_urow("AAA", 90, 0.05), _urow("BBB", 91, 0.04),
                          _urow("CCC", 92, 0.03, sector="Fin"),
                          _urow("DDD", 93, 0.02, sector="Fin"),
                          _urow("EEE", 1, 0.99, sector="Health"),
                          _urow("FFF", 2, 0.98, sector="Energy"),
                          _urow("GGG", 3, 0.97, sector="Fin"),
                          _urow("HHH", 4, 0.96, sector="Tech")],
        "aligned_returns": _aligned(held + cands),
    }
    ic.update(over)
    return ic


def _pol(**over):
    p = dict(ERP.default_policy())
    p.update({"target_position_count": 5, "max_name_weight": 0.25,
              "sector_cap_fraction": 1.0, "min_covariance_obs": 20,
              "min_volatility_coverage": 0.5, "candidate_rank_max": 50,
              "min_position_weight": 0.01})
    p.update(over)
    return p


def _built(budget):
    return ERP.build_proposal(input_contract=_ic(),
                              policy=_pol(max_one_way_turnover=budget))


@pytest.mark.parametrize("budget", [0.10, 0.20, 0.35, 1.0])
def test_no_built_proposal_ever_publishes_an_orphan_replacement(budget):
    """Across budgets that bind hard, loosely and not at all."""
    res = _built(budget)
    allocations = res.get("allocations") or []
    actions = [r["action"] for r in allocations]
    n_in = actions.count("REPLACE_IN")
    n_out = actions.count("REPLACE_OUT")
    assert n_in == n_out, (budget, n_in, n_out)
    for r in allocations:
        rel = r.get("replacement_relationship")
        if r["action"] not in ("REPLACE_IN", "REPLACE_OUT"):
            assert rel is None, (budget, r["ticker"], r["action"], rel)
            assert not [c for c in (r.get("reason_codes") or [])
                        if c.startswith(("FUNDS_REPLACEMENT_OF_",
                                         "REPLACED_BY_"))], r["ticker"]
        else:
            assert rel and rel.get("counterparty"), r["ticker"]
    assert (res.get("constraints") or {}).get("replacement_pairing_ok") is True


@pytest.mark.parametrize("budget", [0.10, 0.20, 0.35, 1.0])
def test_a_surviving_replace_in_always_has_a_counterparty_at_zero(budget):
    res = _built(budget)
    allocations = res.get("allocations") or []
    weights = {r["ticker"]: r.get("proposed_weight") or 0.0 for r in allocations}
    band = _pol()["material_weight_delta"]
    for r in allocations:
        if r["action"] == "REPLACE_IN":
            cp = r["replacement_relationship"]["counterparty"]
            assert weights.get(cp, 0.0) <= band, (budget, r["ticker"], cp)


def test_the_label_fix_changed_no_weight_turnover_or_cost():
    """Measured against the pre-fix code, which lives in the deployed tree.

    This is the guard section C asked for. The first version of this patch did
    change the solution: the new pairing violation fired on a legitimately
    repaired target, the repair was abandoned, and the turnover budget silently
    stopped binding (0.35 -> 0.90). Only checking the economics caught it.
    """
    import subprocess
    script = (
        "import sys, json\n"
        "sys.path.insert(0, r'C:\\\\Users\\\\binis\\\\paper_trader')\n"
        "sys.path.insert(0, r'%s')\n"
        "import engine.reallocation_proposal as ERP\n"
        "import test_release59_reallocation_coherence as T\n"
        "T.ERP = ERP\n"
        "out = {}\n"
        "for b in (0.10, 0.20, 0.35, 1.0):\n"
        "    r = ERP.build_proposal(input_contract=T._ic(),"
        " policy=T._pol(max_one_way_turnover=b))\n"
        "    out[str(b)] = {'t': r['turnover']['one_way_turnover'],"
        " 'c': r['turnover']['estimated_transaction_cost'],"
        " 'w': {a['ticker']: a.get('proposed_weight') for a in r['allocations']}}\n"
        "print(json.dumps(out, sort_keys=True))\n" % (_ROOT / "tests"))
    proc = subprocess.run([sys.executable, "-c", script], capture_output=True,
                          text=True, timeout=600)
    if proc.returncode != 0:
        pytest.skip("the deployed reference tree is not readable here")
    before = json.loads(proc.stdout.strip().splitlines()[-1])
    for b in ("0.1", "0.2", "0.35", "1.0"):
        res = _built(float(b))
        assert res["turnover"]["one_way_turnover"] == before[b]["t"], b
        assert res["turnover"]["estimated_transaction_cost"] == before[b]["c"], b
        after_w = {a["ticker"]: a.get("proposed_weight")
                   for a in res["allocations"]}
        assert after_w == before[b]["w"], b


def test_the_turnover_budget_still_binds_and_reshapes():
    """The label fix must not touch the economics that produced the target."""
    tight = _built(0.10)
    loose = _built(1.0)
    assert tight["turnover"]["one_way_turnover"] <= 0.10 + 1e-9
    assert loose["turnover"]["one_way_turnover"] >= tight["turnover"][
        "one_way_turnover"]
    reopt = tight.get("constraint_reoptimization") or {}
    assert reopt.get("applied") is True
    assert "TURNOVER_BUDGET" in (reopt.get("constraints_that_reshaped") or [])


def test_binding_switching_economics_come_from_the_complete_target():
    """The proposal, not the reassessment, owns the binding numbers."""
    res = _built(0.35)
    se = res.get("switching_economics") or {}
    assert se.get("delegated_inputs", {}).get("one_way_turnover") is True
    assert se.get("delegated_inputs", {}).get("transaction_cost") is True
    assert se.get("one_way_turnover") == res["turnover"]["one_way_turnover"]
    assert se.get("estimated_transaction_cost") == res["turnover"][
        "estimated_transaction_cost"]
    # And the reassessment's own figures stay declared non-binding.
    src = (_ROOT / "engine" / "portfolio_reassessment.py").read_text(
        encoding="utf-8")
    assert '"turnover_budget_binding_here": False' in src


def test_the_proposal_still_verifies_and_creates_nothing():
    res = _built(0.35)
    ver = (res.get("constraint_reoptimization") or {}).get("verification") or {}
    assert ver.get("valid") is True
    assert ver.get("violations") == []
    assert (res.get("constraints") or {}).get("all_ok") is True
    safety = res.get("safety") or {}
    for flag in ("created_orders", "created_fills", "changed_holdings",
                 "changed_cash", "changed_nav"):
        assert safety.get(flag) is False, flag
