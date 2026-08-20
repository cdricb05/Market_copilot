"""Release 31 - Mathematical Alpha Frontier regressions.

These tests are the campaign's guard rails. They do not check that the research
found alpha - "no superior model exists in the bounded search" is a legitimate
result. They check that the campaign CANNOT cheat: that its budgets bite, that
its lockbox is single-use, that its contract cannot be edited after a result
exists, that its evidence layers do not overlap, that the fundamental sample's
survivorship limitation is declared rather than assumed away, and that nothing
in the research lane can reach the operational model, a proposal, a decision or
an order.

Every test here runs against a TEMPORARY research root, so no test can read,
write or invalidate the real campaign's artifacts.
"""
from __future__ import annotations

import ast
import importlib
import json
import math
import os
from pathlib import Path

import pytest

np = pytest.importorskip("numpy")

from paper_trader.alpha_agent import r31
from paper_trader.alpha_agent.r31 import (calibration as _calibration,
                                          contract as _contract,
                                          covcache as _covcache,
                                          judge as _judge,
                                          learners as _learners,
                                          lockbox as _lockbox,
                                          methods as _methods,
                                          multiple_testing as _mt,
                                          novel as _novel,
                                          partition as _partition,
                                          registry as _registry)
from paper_trader.api import mathematical_alpha_frontier as _read_model

REPO = Path(__file__).resolve().parents[1]
UI = REPO / "api" / "ui" / "index.html"


@pytest.fixture()
def temp_root(tmp_path, monkeypatch):
    monkeypatch.setenv(r31.RESEARCH_ROOT_ENV, str(tmp_path))
    monkeypatch.setenv(_read_model.RESEARCH_ROOT_ENV, str(tmp_path))
    return tmp_path


def _row(**kw):
    base = {"candidate_id": "c", "spec_hash": "h", "phase": _registry.PHASE_KNOWN,
            "family": "ridge", "role": _registry.ROLE_CANDIDATE, "params": {},
            "sample": _contract.PRIMARY_SAMPLE, "horizon_sessions": 20,
            "seed": 31, "state": _registry.STATE_OK}
    base.update(kw)
    return base


#: The v3 contract BINDS the identity of every evidence semantic a candidate will
#: be measured under. These stand-ins let the contract tests exercise hashing and
#: immutability without building a real universe, benchmark set or covariance
#: cache; the real values are asserted end-to-end by the campaign itself.
def _contract_kw(**kw):
    base = {"created_at": "2026-08-19T00:00:00+00:00",
            "data_sources": {}, "feature_spec": {},
            "universe_hash": "u1", "benchmark_hash": "b1", "judge_hash": "j1",
            "calibration_owner": "alpha_agent.r31.calibration",
            "allocation_owner": "alpha_agent.r31.allocation",
            "covariance_cache_key": "cc1", "executed_grid": {"known": 34}}
    base.update(kw)
    return base


def _spec_kw(**kw):
    base = dict(phase="KNOWN_METHOD", family="ridge", params={"alpha": 10.0},
                sample=_contract.PRIMARY_SAMPLE, horizon=20, feats=("a", "b"),
                seed=31, training_universe=_contract.TRAIN_BROAD_PIT,
                universe_hash="u1", benchmark_hash="b1")
    base.update(kw)
    return base


# --------------------------------------------------------------------------- #
# Campaign contract
# --------------------------------------------------------------------------- #
def test_contract_hash_binds_every_material_term(temp_root):
    con = _contract.build(**_contract_kw(data_sources={"a": {"path": "x"}},
                                         feature_spec={"order": ["mom_6_1"]}))
    assert _contract.verify(con)["intact"]
    tampered = dict(con)
    tampered["budgets"] = dict(con["budgets"])
    tampered["budgets"]["novel_candidates_total"] = 99999
    assert not _contract.verify(tampered)["intact"], (
        "widening a budget must break the contract hash")


@pytest.mark.parametrize("field", ["universe_hash", "benchmark_hash",
                                   "judge_hash", "covariance_cache_key"])
def test_contract_hash_binds_the_evidence_semantics(temp_root, field):
    """Every identity a score depends on must be inside the contract hash.

    Campaign v2's contract named its universe and benchmark in prose. Prose does
    not break when the code underneath it changes, which is how a campaign ends up
    reporting one evaluation universe and measuring another.
    """
    base = _contract.build(**_contract_kw())
    other = _contract.build(**_contract_kw(**{field: "CHANGED"}))
    assert base["contract_hash"] != other["contract_hash"], (
        "changing %s must change the contract hash" % field)


def test_contract_is_immutable_once_frozen(temp_root):
    con = _contract.build(**_contract_kw())
    _contract.freeze(con)
    _contract.freeze(con)                      # byte-identical rewrite is fine
    changed = dict(con)
    changed["created_at"] = "2026-01-01T00:00:00+00:00"
    with pytest.raises(r31.ArtifactImmutable):
        _contract.freeze(changed)


def test_contract_drift_after_a_result_is_refused(temp_root):
    con = _contract.build(**_contract_kw())
    _contract.freeze(con)
    reg = _registry.Registry(con["campaign_id"])
    reg.record(_row(spec_hash="aaa"))
    broken = json.loads(_contract.path_for(con["campaign_id"]).read_text("utf-8"))
    broken["budgets"]["lockbox_candidates"] = 500
    _contract.path_for(con["campaign_id"]).write_text(
        json.dumps(broken, indent=2, sort_keys=True), encoding="utf-8")
    with pytest.raises(_registry.ContractDrift):
        _registry.assert_contract_stable(con["campaign_id"])


# --------------------------------------------------------------------------- #
# Registry budgets and idempotency
# --------------------------------------------------------------------------- #
def test_a_specification_hash_may_not_execute_twice(temp_root):
    reg = _registry.Registry("c1")
    reg.record(_row(spec_hash="dup"))
    with pytest.raises(_registry.DuplicateCandidate):
        reg.record(_row(spec_hash="dup"))


def test_resumed_registry_reloads_and_still_refuses_the_duplicate(temp_root):
    _registry.Registry("c1").record(_row(spec_hash="dup"))
    reg = _registry.Registry("c1")
    assert reg.has("dup")
    with pytest.raises(_registry.DuplicateCandidate):
        reg.record(_row(spec_hash="dup"))


def test_known_method_family_budget_bites(temp_root):
    reg = _registry.Registry("c1")
    for i in range(_contract.MAX_KNOWN_METHOD_FAMILIES):
        reg.record(_row(spec_hash="f%d" % i, family="fam%d" % i))
    with pytest.raises(_registry.BudgetExceeded):
        reg.record(_row(spec_hash="over", family="one_family_too_many"))


def test_known_method_per_family_budget_bites(temp_root):
    reg = _registry.Registry("c1")
    for i in range(_contract.MAX_CONFIGS_PER_KNOWN_FAMILY):
        reg.record(_row(spec_hash="g%d" % i, family="ridge"))
    with pytest.raises(_registry.BudgetExceeded):
        reg.record(_row(spec_hash="gover", family="ridge"))


def test_novel_total_and_campaign_budgets_bite(temp_root):
    reg = _registry.Registry("c1")
    for i in range(_contract.MAX_NOVEL_CANDIDATES_PER_CAMPAIGN):
        reg.record(_row(spec_hash="n%d" % i, phase=_registry.PHASE_NOVEL,
                        family=_novel.FAM_SYMBOLIC, novel_campaign=1))
    with pytest.raises(_registry.BudgetExceeded):
        reg.record(_row(spec_hash="nover", phase=_registry.PHASE_NOVEL,
                        family=_novel.FAM_SYMBOLIC, novel_campaign=1))


def test_a_third_novel_campaign_is_refused(temp_root):
    reg = _registry.Registry("c1")
    with pytest.raises(_registry.BudgetExceeded):
        reg.record(_row(spec_hash="n3", phase=_registry.PHASE_NOVEL,
                        family=_novel.FAM_SYMBOLIC, novel_campaign=3))


def test_novel_refinement_depth_is_capped(temp_root):
    reg = _registry.Registry("c1")
    with pytest.raises(_registry.BudgetExceeded):
        reg.record(_row(spec_hash="deep", phase=_registry.PHASE_NOVEL,
                        family=_novel.FAM_SYMBOLIC, novel_campaign=1,
                        refinement_depth=_contract.MAX_NOVEL_REFINEMENT_DEPTH + 1))


def test_benchmarks_do_not_consume_the_candidate_family_budget(temp_root):
    reg = _registry.Registry("c1")
    for i in range(_contract.MAX_KNOWN_METHOD_FAMILIES + 6):
        reg.record(_row(spec_hash="b%d" % i, family="bench%d" % i,
                        role=_registry.ROLE_BENCHMARK))
    assert len(reg.entries) == _contract.MAX_KNOWN_METHOD_FAMILIES + 6


def test_rejected_candidates_stay_in_the_multiple_testing_denominator(temp_root):
    reg = _registry.Registry("c1")
    reg.record(_row(spec_hash="ok1"))
    reg.record(_row(spec_hash="bad1", state=_registry.STATE_FAILED))
    reg.record(_row(spec_hash="bad2", state=_registry.STATE_RESOURCE_INFEASIBLE))
    s = reg.summary()
    assert s["multiple_testing_denominator"] == 3
    assert s["denominator_includes_rejected_candidates"] is True


# --------------------------------------------------------------------------- #
# Evidence partition
# --------------------------------------------------------------------------- #
def test_layers_are_disjoint_and_chronological():
    p = _partition.partition_for(304, 60)
    assert p["sufficient"]
    d, v, l = set(p["discovery"]), set(p["validation"]), set(p["lockbox"])
    assert not (d & v) and not (v & l) and not (d & l)
    assert max(d) < min(v) < max(v) < min(l)
    assert max(l) == 303, "the lockbox must be the LATEST contiguous block"


def test_embargo_separates_every_adjacent_layer():
    for h in _contract.HORIZONS:
        p = _partition.partition_for(304, h)
        emb = _partition.embargo_dates(h)
        assert emb == math.ceil(h / _contract.STEP_SESSIONS)
        assert min(p["validation"]) - max(p["discovery"]) == emb + 1
        assert min(p["lockbox"]) - max(p["validation"]) == emb + 1
        embargoed = set(p["embargoed"])
        assert embargoed and not (embargoed & set(p["discovery"]))
        assert not (embargoed & set(p["validation"]))
        assert not (embargoed & set(p["lockbox"]))


def test_an_insufficient_sample_is_reported_blocked_not_manufactured():
    p = _partition.partition_for(20, 20)
    assert not p["sufficient"]
    assert p["state"] == _partition.BLOCKED


def test_training_cap_means_a_model_never_trains_on_a_lockbox_row():
    """The walk-forward bucket is capped at the last validation index."""
    p = _partition.partition_for(304, 20)
    cap = p["validation"][-1]
    for li in p["lockbox"]:
        bucket = _novel._bucket(li, p["embargo_dates"], cap)
        assert bucket <= cap
        assert bucket not in set(p["lockbox"])


# --------------------------------------------------------------------------- #
# Lockbox
# --------------------------------------------------------------------------- #
def _finalists(n, family="ridge"):
    return [{"candidate_id": "c%d" % i, "spec_hash": "h%d" % i,
             "family": "%s%d" % (family, i // _contract.MAX_LOCKBOX_PER_FAMILY),
             "sample": _contract.PRIMARY_SAMPLE, "horizon_sessions": 20}
            for i in range(n)]


def test_lockbox_refuses_more_than_twelve_finalists(temp_root):
    with pytest.raises(_lockbox.LockboxViolation):
        _lockbox.freeze_finalists(
            _finalists(_contract.MAX_LOCKBOX_CANDIDATES + 1), campaign_id="c1",
            selected_at="t", selection_basis="V")


def test_lockbox_refuses_three_finalists_from_one_family(temp_root):
    bad = [{"candidate_id": "c%d" % i, "spec_hash": "h%d" % i, "family": "ridge",
            "sample": _contract.PRIMARY_SAMPLE, "horizon_sessions": 20}
           for i in range(_contract.MAX_LOCKBOX_PER_FAMILY + 1)]
    with pytest.raises(_lockbox.LockboxViolation):
        _lockbox.freeze_finalists(bad, campaign_id="c1", selected_at="t",
                                  selection_basis="V")


def test_a_finalist_gets_exactly_one_lockbox_execution(temp_root):
    f = _finalists(4)
    _lockbox.freeze_finalists(f, campaign_id="c1", selected_at="t",
                              selection_basis="V")
    _lockbox.authorise("h0", campaign_id="c1", family=f[0]["family"],
                       candidate_id="c0", at="t")
    with pytest.raises(_lockbox.LockboxViolation):
        _lockbox.authorise("h0", campaign_id="c1", family=f[0]["family"],
                           candidate_id="c0", at="t")


def test_a_revised_candidate_cannot_be_resubmitted_to_the_same_lockbox(temp_root):
    f = _finalists(2)
    _lockbox.freeze_finalists(f, campaign_id="c1", selected_at="t",
                              selection_basis="V")
    with pytest.raises(_lockbox.LockboxViolation):
        _lockbox.authorise("a_new_hash_after_a_tweak", campaign_id="c1",
                           family=f[0]["family"], candidate_id="c0", at="t")


def test_the_lockbox_cannot_be_opened_before_the_finalists_are_frozen(temp_root):
    with pytest.raises(_lockbox.LockboxViolation):
        _lockbox.authorise("h0", campaign_id="c1", family="ridge",
                           candidate_id="c0", at="t")


def test_the_frozen_finalist_set_cannot_be_widened(temp_root):
    _lockbox.freeze_finalists(_finalists(2), campaign_id="c1", selected_at="t",
                              selection_basis="V")
    with pytest.raises(_lockbox.LockboxViolation):
        _lockbox.freeze_finalists(_finalists(4), campaign_id="c1",
                                  selected_at="t", selection_basis="V")


# --------------------------------------------------------------------------- #
# Judge
# --------------------------------------------------------------------------- #
def test_the_judge_reads_the_canonical_cost_and_constraint_owner():
    from paper_trader.engine import zero_base_allocator as zb
    pol = zb.default_policy()
    econ = _judge.economics_declaration()
    assert econ["cost_rate_per_side"] == pol["cost_rate_per_side"]
    assert econ["max_name_weight"] == pol["max_name_weight"]
    assert econ["min_adv_dollar"] == pol["min_adv_dollar"]
    assert econ["judge_owns_no_cost_or_risk_calculation"] is True


def test_the_judge_declares_sector_unmeasurable_rather_than_guessing():
    econ = _judge.economics_declaration()
    assert econ["sector_cap_state"] == _judge.SECTOR_STATE == "UNMEASURABLE_PIT"
    assert "inadmissible" in econ["sector_cap_reason"]


def test_the_judge_source_contains_no_literal_cost_or_cap_number():
    src = (REPO / "alpha_agent" / "r31" / "judge.py").read_text("utf-8")
    for forbidden in ("0.00125", "12.5", "0.10 ", "max_name_weight = "):
        assert forbidden not in src, (
            "the judge must READ %r from the canonical policy, never restate it"
            % forbidden)


def test_the_secondary_diagnostic_book_respects_cap_and_liquidity():
    pol = _judge.policy()
    rng = np.random.default_rng(0)
    pred = rng.normal(size=200)
    adv = np.full(200, pol["min_adv_dollar"] * 10)
    adv[:50] = pol["min_adv_dollar"] / 100.0      # illiquid
    w = _judge.top_n_book(pred, adv, book_n=25, pol=pol)
    assert abs(float(w.sum()) - 1.0) < 1e-9
    assert float(w.max()) <= pol["max_name_weight"] + 1e-9
    assert float(w[:50].sum()) == 0.0, "an illiquid name must never be bought"


def test_the_secondary_diagnostic_book_is_a_pure_function_of_its_inputs():
    pol = _judge.policy()
    pred = np.array([1.0, 1.0, 1.0, 0.5, 0.2])
    adv = np.full(5, pol["min_adv_dollar"] * 10)
    a = _judge.top_n_book(pred, adv, book_n=3, pol=pol)
    b = _judge.top_n_book(pred, adv, book_n=3, pol=pol)
    assert np.array_equal(a, b), "ties must resolve deterministically"


def test_top_n_may_never_carry_the_primary_verdict():
    """CORRECTION 2. v2's economic verdict came from a top-N equal-weight book.

    It survives as a diagnostic and is barred, in the contract and in the judge's
    frozen artifact, from being the thing candidates are selected on.
    """
    assert _contract.TOP_N_MAY_CARRY_PRIMARY_VERDICT is False
    jc = _judge.build_contract()
    assert jc["secondary_diagnostic"]["may_carry_primary_verdict"] is False
    assert jc["primary_construction"] == \
        "CANONICAL_ZERO_BASE_ALLOCATION_STOCKS_PLUS_CASH"
    assert "TOP_N_BOOK_ECONOMICS" in jc["not_selected_by"]
    assert "zero_base" in jc["selection_statistic"] or \
        _contract.BENCH_EQUAL_WEIGHT in jc["selection_statistic"]


def test_the_reported_cost_drag_equals_the_cost_actually_charged():
    """The drag a candidate REPORTS must be the drag its net return PAID.

    These were once computed with different factors - the net return charged the
    per-side rate on one-way turnover while the reported drag doubled it - so a
    candidate's headline cost and its actual cost disagreed by a factor of two.
    """
    pol = _judge.policy()
    rate = float(pol["cost_rate_per_side"])
    traded = np.array([1.0, 0.8, 0.6])
    gross = np.array([0.02, 0.01, 0.03])
    net = gross - traded * rate
    m = _judge._book_metrics(gross, net, np.zeros(3), np.full(3, 0.01), traded,
                             np.full(3, 0.1), np.full(3, 25.0), np.full(3, 0.04),
                             np.zeros(3), [True] * 3, {"AAA": 0.01}, rate, 0, 1.0)
    expected = float(traded.mean()) * rate * _judge.PERIODS_PER_YEAR
    assert m["cost_drag_annualised"] == pytest.approx(expected)
    # and the conventional one-way statistic is half the traded notional
    assert m["turnover_mean_one_way"] == pytest.approx(float(traded.mean()) / 2.0)


def test_the_risk_frontier_varies_risk_appetite_not_concentration():
    """CORRECTION 2. The frontier is gamma, pre-registered and frozen.

    Book size varies CONCENTRATION, which is not risk appetite and cannot express
    a cash decision at all - a 15-name book and a 40-name book are both fully
    invested by construction.
    """
    jc = _judge.build_contract()
    assert jc["risk_frontier_gamma_multipliers"] == \
        list(_contract.RISK_FRONTIER_GAMMA_MULTIPLIERS)
    assert jc["primary_gamma_multiplier"] == _contract.PRIMARY_GAMMA_MULTIPLIER
    assert jc["frontier_frozen_before_results"] is True
    assert not hasattr(_contract, "RISK_FRONTIER_BOOK_SIZES"), (
        "the v2 book-size frontier must be gone, not merely unused")
    assert "MSE" in jc["not_selected_by"]
    assert "IC_ALONE" in jc["not_selected_by"]


def test_only_risk_aversion_moves_along_the_gamma_frontier():
    """A frontier point must be a different risk appetite, not different rules."""
    base = _judge.policy()
    for gm in _contract.RISK_FRONTIER_GAMMA_MULTIPLIERS:
        p = _judge.gamma_policy(gm)
        assert p["risk_aversion_gamma"] == pytest.approx(
            base["risk_aversion_gamma"] * gm)
        for k in ("cost_rate_per_side", "max_name_weight", "min_adv_dollar",
                  "min_position_weight", "covariance_lookback",
                  "sector_cap_fraction", "max_adv_participation"):
            assert p[k] == base[k], (
                "%s moved along the risk frontier; a frontier point would then "
                "be a different set of rules rather than a different appetite" % k)


# --------------------------------------------------------------------------- #
# Learners: determinism
# --------------------------------------------------------------------------- #
def _toy(n=600, f=8, seed=0):
    rng = np.random.default_rng(seed)
    X = rng.normal(size=(n, f))
    y = X @ np.linspace(0.4, -0.2, f) + rng.normal(scale=2.0, size=n)
    return X, y


@pytest.mark.parametrize("fit", [
    lambda X, y: _learners.fit_ridge(X, y, alpha=10.0),
    lambda X, y: _learners.fit_elastic_net(X, y, alpha=1e-3, l1_ratio=0.5),
    lambda X, y: _learners.fit_huber(X, y, delta=1.5, alpha=10.0),
    lambda X, y: _learners.fit_dimension_reduction(X, y, n_components=3, variant="PCR"),
    lambda X, y: _learners.fit_dimension_reduction(X, y, n_components=3, variant="PLS"),
    lambda X, y: _learners.fit_random_forest(X, y, n_trees=6, max_depth=3,
                                             min_leaf=40, seed=31),
    lambda X, y: _learners.fit_neural_net(X, y, hidden=(6,), epochs=4, seed=31),
    lambda X, y: _learners.fit_quantile(X, y, tau=[0.5], epochs=20, seed=31),
])
def test_every_learner_is_deterministic(fit):
    X, y = _toy()
    a = _learners.predict(fit(X, y), X)
    b = _learners.predict(fit(X, y), X)
    assert np.allclose(a, b), "same data + params + seed must give the same model"
    assert np.isfinite(a).all()


def test_fama_macbeth_averages_period_slopes_not_pooled_rows():
    X, y = _toy(n=300, f=4, seed=1)
    blocks = [(X[:150], y[:150]), (X[150:], y[150:])]
    fm = _learners.fit_fama_macbeth(blocks)
    assert fm["n_periods"] == 2
    per = []
    for Xt, yt in blocks:
        per.append(np.linalg.solve(Xt.T @ Xt + 1e-8 * np.eye(4), Xt.T @ yt))
    assert np.allclose(fm["coef"], np.mean(per, axis=0))


def test_predictions_are_finite_even_for_a_degenerate_block():
    X = np.zeros((80, 6))
    y = np.zeros(80)
    out = _learners.predict(_learners.fit_ridge(X, y, alpha=1.0), X)
    assert np.isfinite(out).all()


# --------------------------------------------------------------------------- #
# Novel discovery
# --------------------------------------------------------------------------- #
def test_the_novel_grammar_is_frozen_and_bounded():
    g = _novel.grammar_contract()
    assert g["frozen_before_execution"] is True
    assert len(g["families"]) <= _contract.MAX_NOVEL_FAMILIES
    assert g["candidates_total"] == _contract.MAX_NOVEL_CANDIDATES_TOTAL == 300
    assert g["campaign_budget"] == _contract.MAX_NOVEL_CAMPAIGNS == 2
    assert g["refinement_depth"] == _contract.MAX_NOVEL_REFINEMENT_DEPTH == 3
    assert g["generation_never_reads_an_evaluation_result"] is True


def test_novel_generation_is_reproducible_across_processes():
    feats = ("mom_6_1", "vol_63", "trend_200", "log_adv_20", "beta_252")
    a = _novel.generate(1, feats)
    b = _novel.generate(1, feats)
    assert [c["candidate_id"] for c in a] == [c["candidate_id"] for c in b]
    assert json.dumps(a, sort_keys=True, default=str) == \
        json.dumps(b, sort_keys=True, default=str)


def test_novel_generation_does_not_use_the_salted_builtin_hash():
    src = (REPO / "alpha_agent" / "r31" / "novel.py").read_text("utf-8")
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            assert node.func.id != "hash", (
                "builtin hash() of a str is salted per process and would break "
                "the campaign's determinism")


def test_each_novel_campaign_respects_its_candidate_budget():
    feats = tuple("f%d" % i for i in range(14))
    for cno in (1, 2):
        assert len(_novel.generate(cno, feats)) <= \
            _contract.MAX_NOVEL_CANDIDATES_PER_CAMPAIGN


def test_symbolic_expressions_stay_within_the_declared_complexity():
    feats = ("a", "b", "c", "d")
    for e in _novel.enumerate_expressions(feats, 200, seed=1):
        assert _novel.expression_complexity(e) <= 2 * _novel.MAX_BASE_FEATURES
        assert e["op"] in _novel.PRIMITIVES


def test_regime_labels_read_only_the_past():
    mkt = np.arange(40, dtype=np.float64) / 100.0
    a = _novel.regime_at(mkt, 20, kind="TREND")
    poisoned = mkt.copy()
    poisoned[20:] = -99.0                      # the future turns catastrophic
    b = _novel.regime_at(poisoned, 20, kind="TREND")
    assert a == b, "a regime label must not change when the FUTURE changes"


def test_peer_groups_never_use_sector():
    g = _novel.grammar_contract()
    assert "SECTOR" in g["peer_group_source"]
    assert "excluded" in g["peer_group_source"]
    src = (REPO / "alpha_agent" / "r31" / "novel.py").read_text("utf-8")
    assert "pit_sector" not in src and "gics" not in src.lower()


# --------------------------------------------------------------------------- #
# Multiple testing
# --------------------------------------------------------------------------- #
def test_benjamini_hochberg_matches_the_step_up_definition():
    out = _mt.benjamini_hochberg([0.001, 0.008, 0.04, 0.6, 0.9], q=0.10)
    assert out["m"] == 5
    assert out["n_rejected"] == 3
    assert out["rejected"] == [0, 1, 2]


def test_bootstrap_is_reproducible_and_one_sided():
    rng = np.random.default_rng(7)
    d = rng.normal(loc=0.0, scale=0.02, size=60)
    a = _mt.paired_block_bootstrap(d, resamples=200)
    b = _mt.paired_block_bootstrap(d, resamples=200)
    assert a["p_value"] == b["p_value"]
    assert 0.0 < a["p_value"] <= 1.0


def test_spa_penalises_searching_many_candidates():
    rng = np.random.default_rng(11)
    pure_noise = {"c%d" % i: rng.normal(scale=0.02, size=60) for i in range(60)}
    out = _mt.superior_predictive_ability(pure_noise, resamples=300)
    assert out["state"] == "OK"
    assert out["p_value"] > 0.10, (
        "the best of 60 noise candidates must not look significant")


def test_spa_is_restricted_to_one_common_date_axis():
    """SPA over series from different samples is not a comparison.

    The campaign runs SPA on the primary sample at the primary horizon only,
    because that is the one cell where every candidate's excess series sits on
    the identical date axis. Truncating a longer axis to match a shorter one
    would compare different periods and report a p-value for neither.
    """
    src = (REPO / "alpha_agent" / "r31" / "campaign.py").read_text("utf-8")
    assert "spa_scope" in src
    assert "common_date_axis" in src
    i = src.index("spa_scope = [")
    window = src[i:i + 400]
    assert "_contract.PRIMARY_SAMPLE" in window
    assert "PRIMARY_HORIZON" in window


def test_a_genuinely_strong_series_still_rejects():
    rng = np.random.default_rng(12)
    series = {"good": rng.normal(loc=0.03, scale=0.01, size=60)}
    out = _mt.superior_predictive_ability(series, resamples=300)
    assert out["p_value"] < 0.10


# --------------------------------------------------------------------------- #
# Known-method registry
# --------------------------------------------------------------------------- #
def test_the_implemented_family_count_is_inside_the_frozen_budget():
    assert len(_methods.FAMILY_SPECS) <= _contract.MAX_KNOWN_METHOD_FAMILIES


def test_no_family_grid_exceeds_its_per_family_budget():
    for fam, grid in _methods.FAMILY_SPECS.items():
        assert grid["n_configs"] <= _contract.MAX_CONFIGS_PER_KNOWN_FAMILY, fam


def test_literature_records_why_an_excluded_method_was_excluded():
    lit = _methods.literature_registry()
    assert lit["papers_screened"] <= _contract.MAX_PAPERS_SCREENED
    assert lit["methods_deeply_extracted"] <= _contract.MAX_METHODS_EXTRACTED
    excluded = lit["excluded_methods"]
    assert excluded, "the registry must record the methods it did NOT implement"
    for e in excluded:
        assert e["reason"], "an exclusion without a reason is not a record"
    assert lit["stopping_rule"]["consecutive_dry_expansions"] >= \
        _contract.LITERATURE_DRY_EXPANSIONS


def test_a_spec_hash_binds_the_snapshot_and_the_partition():
    a = _methods.spec_hash(**_spec_kw(snapshot_hash="s1", partition_hash="p1"))
    assert a != _methods.spec_hash(**_spec_kw(snapshot_hash="s2",
                                              partition_hash="p1"))
    assert a != _methods.spec_hash(**_spec_kw(snapshot_hash="s1",
                                              partition_hash="p2"))
    assert a == _methods.spec_hash(**_spec_kw(snapshot_hash="s1",
                                              partition_hash="p1"))


@pytest.mark.parametrize("field,other", [
    ("training_universe", _contract.TRAIN_INVESTMENT_ONLY),
    ("universe_hash", "u2"),
    ("benchmark_hash", "b2"),
])
def test_a_spec_hash_binds_the_v3_evidence_semantics(field, other):
    """CORRECTIONS 1 and 4, as candidate IDENTITY.

    "Train broad, invest narrow" must occupy its own slot in the multiple-testing
    denominator rather than hiding inside another candidate's hash, and a change
    of benchmark set must invalidate cached results rather than re-labelling them.
    """
    kw = _spec_kw(snapshot_hash="s1", partition_hash="p1")
    before = _methods.spec_hash(**kw)
    kw[field] = other
    assert _methods.spec_hash(**kw) != before, (
        "%s must be part of candidate identity" % field)


def test_a_spec_hash_binds_the_judge_behaviour(monkeypatch):
    """Changing what a score MEANS must invalidate cached candidates.

    Binding only the judge's schema NAME would let a corrected cost model reuse
    results measured under the old one, and the leaderboard would silently mix
    two judges.
    """
    kw = _spec_kw(snapshot_hash="s1", partition_hash="p1")
    before = _methods.spec_hash(**kw)
    monkeypatch.setattr(_judge, "HOLD_SESSIONS", _judge.HOLD_SESSIONS + 1)
    assert _methods.spec_hash(**kw) != before

    monkeypatch.undo()
    monkeypatch.setattr(_judge, "EVALUATION_UNIVERSE", "SOMETHING_ELSE")
    assert _methods.spec_hash(**kw) != before

    monkeypatch.undo()
    monkeypatch.setattr(_calibration, "MIN_SLOPE_T", 9.9)
    assert _methods.spec_hash(**kw) != before, (
        "the Track-A calibration floor decides WHICH models become capital "
        "allocators, so it is part of what a score means")


def test_superseded_campaign_results_cannot_enter_v3():
    """A v2 result must be unable to collide with a v3 specification hash."""
    assert _contract.CAMPAIGN_ID.endswith("_v3")
    for cid, blk in _contract.SUPERSEDED_CAMPAIGNS.items():
        assert blk["state"] == _contract.SUPERSEDED_EXPERIMENTAL_DESIGN
        assert blk["defects"], "a supersession without a reason is not a record"
        assert blk["produced_a_verdict"] is False
    rules = _contract.SUPERSEDED_EVIDENCE_RULES
    for k in ("may_select_v3_hyperparameters", "may_select_v3_finalists",
              "may_influence_the_lockbox",
              "may_contribute_to_a_superiority_verdict",
              "may_reduce_the_multiple_testing_denominator",
              "may_be_reused_as_v3_validation_evidence"):
        assert rules[k] is False
    assert rules["is_preserved_on_disk"] is True
    # v2 hashed no universe and no benchmark, so its keys are structurally
    # unreachable from the v3 keyspace: the v3 hash always mixes both in.
    kw = _spec_kw(snapshot_hash="s1", partition_hash="p1")
    assert _methods.spec_hash(**kw) != _methods.spec_hash(
        **dict(kw, universe_hash="", benchmark_hash=""))


# --------------------------------------------------------------------------- #
# Safety: the research lane cannot reach production
# --------------------------------------------------------------------------- #
R31_SOURCES = sorted((REPO / "alpha_agent" / "r31").glob("*.py"))


def test_the_research_package_never_imports_the_api_package():
    for path in R31_SOURCES:
        tree = ast.parse(path.read_text("utf-8"))
        for node in ast.walk(tree):
            mods = []
            if isinstance(node, ast.Import):
                mods = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                mods = [node.module]
            for m in mods:
                assert not m.startswith("paper_trader.api"), (
                    "%s imports %s; the research lane must not reach the API "
                    "package" % (path.name, m))
                assert ".api." not in m


def test_the_research_package_only_reads_pure_stdlib_engine_owners():
    """It may read the canonical economics owners, and nothing DB-bound.

    Two owners are admissible, and both for the same reason: the campaign must
    judge candidates on the economics the operator actually faces, so it READS
    the canonical cost/constraint owner and the canonical covariance owner rather
    than restating either. Release 31 Campaign v3 added the second when the
    primary judge began allocating capital through the zero-base kernel, which
    needs a covariance matrix; building one inside the research package would
    have created a SECOND risk owner, and two risk owners disagree the first time
    a lookback changes.

    The guard is not a name allowlist. Each admitted module is re-parsed and
    proven to import nothing outside the standard library, so "pure" is verified
    rather than asserted - a module that later grew a store or API dependency
    would fail here even though its name is still on the list.
    """
    allowed = {"zero_base_allocator", "holding_opportunity_cost"}
    engine_dir = REPO / "engine"
    stdlib_ok = {"hashlib", "json", "math", "decimal", "typing", "os", "sys",
                 "datetime", "collections", "itertools", "functools", "re",
                 "pathlib", "statistics", "dataclasses", "enum", "copy",
                 "__future__", "warnings", "abc", "time"}

    for path in R31_SOURCES:
        src = path.read_text("utf-8")
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module and "engine" in node.module:
                for a in node.names:
                    assert a.name in allowed, (
                        "%s imports engine.%s" % (path.name, a.name))

    # Every admitted owner must genuinely be free of DB/API dependencies.
    for name in sorted(allowed):
        owner = engine_dir / ("%s.py" % name)
        assert owner.exists(), "admitted engine owner %s is missing" % (name,)
        otree = ast.parse(owner.read_text("utf-8"))
        for node in ast.walk(otree):
            mods = []
            if isinstance(node, ast.Import):
                mods = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                mods = [node.module]
            for m in mods:
                root = m.split(".")[0]
                assert root in stdlib_ok, (
                    "engine.%s imports %s and is therefore not a pure owner the "
                    "research lane may read" % (name, m))


def test_no_promotion_activation_or_order_vocabulary_in_the_research_lane():
    forbidden = ("create_order", "submit_order", "place_order", "broker",
                 "promote_model", "activate_model", "set_champion",
                 "record_decision", "persist_proposal")
    for path in R31_SOURCES:
        src = path.read_text("utf-8").lower()
        for token in forbidden:
            assert token not in src, "%s contains %r" % (path.name, token)


def test_automatic_promotion_is_declared_false_and_used():
    assert r31.AUTOMATIC_PROMOTION_ALLOWED is False
    assert r31.safety_block()["automatic_promotion_allowed"] is False
    for key in ("creates_order", "creates_proposal", "creates_decision",
                "writes_operational_store", "creates_signal_authority"):
        assert r31.safety_block()[key] is False


def test_every_campaign_artifact_lands_under_the_research_root(temp_root):
    cid = "c1"
    assert str(r31.campaign_dir(cid)).startswith(str(temp_root))
    for fn in (_contract.path_for, _partition.path_for, _judge.path_for,
               _mt.path_for, _lockbox.results_path, _lockbox.finalists_path,
               _lockbox.access_path, _registry.log_path,
               _registry.artifact_path):
        assert str(fn(cid)).startswith(str(temp_root)), fn


def test_the_research_root_is_not_an_operational_store():
    root = str(r31.DEFAULT_RESEARCH_ROOT).lower()
    for operational in ("operational_book", "portfolio_decisions",
                        "rebalance_order_plans", "reallocation_proposals",
                        "event_fabric", "information_collection"):
        assert operational not in root


# --------------------------------------------------------------------------- #
# Read model and UI
# --------------------------------------------------------------------------- #
def test_the_read_model_reports_not_started_without_a_contract(temp_root):
    out = _read_model.load_frontier("nothing_here")
    assert out["state"] == _read_model.STATE_NOT_STARTED
    assert out["blocker"] == "NO_CAMPAIGN_CONTRACT"
    assert out["creates_orders"] is False
    assert out["allows_model_activation"] is False


def test_the_read_model_computes_no_research_mathematics():
    """It may PASS THROUGH a published metric; it may not derive one.

    Reading a field called ``net_excess_annualised`` is exactly the behaviour we
    want, so this test checks for the machinery of computation - a numeric
    library, a statistical function - rather than for the names of the metrics
    being reported.
    """
    src = (REPO / "api" / "mathematical_alpha_frontier.py").read_text("utf-8")
    tree = ast.parse(src)
    for node in ast.walk(tree):
        mods = []
        if isinstance(node, ast.Import):
            mods = [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module:
            mods = [node.module]
        for m in mods:
            assert m.split(".")[0] not in ("numpy", "pandas", "statistics",
                                           "math"), (
                "the read model imported %s; it must report published numbers, "
                "not recompute them" % m)
            assert not m.startswith("paper_trader.alpha_agent"), (
                "the read model must read finished artifacts, never import the "
                "research package into the API process")
    for token in ("sqrt", "cumprod", "quantile", "std(", "mean(", "bootstrap",
                  "newey", "ddof"):
        assert token not in src.lower(), (
            "the read model contains statistical machinery: %r" % token)


def test_the_read_model_never_writes(temp_root):
    src = (REPO / "api" / "mathematical_alpha_frontier.py").read_text("utf-8")
    for token in ("write_text", "open(", "mkdir", "savez", "w+"):
        assert token not in src, "the read model wrote something: %r" % token


def test_the_ui_region_exists_with_visible_safety_badges():
    html = UI.read_text("utf-8")
    assert 'id="r31-frontier"' in html
    assert "loadMathematicalAlphaFrontier" in html
    # Scoped to the region: a badge present SOMEWHERE in a 34,000-line document
    # is not a badge visible on this card.
    region = _r31_region(html)
    for badge in ("RESEARCH ONLY", "READ ONLY", CANONICAL_ORDER_BADGE,
                  "AUTOMATION OFF", "MANUAL REVIEW"):
        assert badge in region


def test_the_ui_region_has_no_execute_approve_or_activate_control():
    html = UI.read_text("utf-8")
    start = html.index('id="r31-frontier"')
    end = html.index('RELEASE 29 UX2: OPERATING DIAGNOSTICS', start)
    region = html[start:end]
    for token in ("<button", "onclick=", "Approve", "Activate", "Promote",
                  "Execute", "Create Order"):
        assert token not in region, "the region contains %r" % token


def test_the_frontier_loader_uses_no_alert_or_confirm():
    html = UI.read_text("utf-8")
    start = html.index("function loadMathematicalAlphaFrontier")
    region = html[start:start + 8000]
    assert "alert(" not in region
    assert "confirm(" not in region


def test_the_route_is_declared_get_only_and_authenticated():
    app = (REPO / "api" / "app.py").read_text("utf-8")
    route = "/v1/research/mathematical-alpha-frontier"
    assert app.count('"%s"' % route) == 1, "the route must be declared once"
    idx = app.index('"%s"' % route)
    decorator = app[max(0, idx - 200): idx + 300]
    assert "@app.get(" in decorator
    assert "_verify_api_key" in decorator
    for verb in ("@app.post(", "@app.put(", "@app.delete("):
        assert route not in app[app.index(verb):app.index(verb) + 200] \
            if verb in app else True


# --------------------------------------------------------------------------- #
# Inadmissible information
# --------------------------------------------------------------------------- #
def test_news_and_external_links_are_declared_inadmissible():
    bad = _contract.INADMISSIBLE_INFORMATION
    assert "gdelt_news_text" in bad
    assert "external_reference_links" in bad
    assert "current_analyst_snapshots" in bad
    assert "entity_sic_snapshot_sector" in bad


def test_no_news_or_event_feature_appears_in_the_feature_set():
    from paper_trader.alpha_agent.r31 import snapshot as _snap
    for name in _snap.ALL_FEATURES:
        low = name.lower()
        for token in ("news", "gdelt", "article", "sentiment", "headline",
                      "event", "analyst", "revision", "sector", "gics"):
            assert token not in low, "%s looks like an inadmissible family" % name


def test_the_fundamental_sample_may_not_carry_a_verdict_alone():
    """The survivorship-limited sample is measured, but never decisive."""
    assert _contract.PRIMARY_SAMPLE == _contract.SAMPLE_PRICE_FULL
    src = (REPO / "alpha_agent" / "r31" / "snapshot.py").read_text("utf-8")
    assert '"may_carry_verdict": False' in src
    assert "FUNDAMENTAL_SAMPLE_SURVIVORSHIP_LIMITED" in src


# --------------------------------------------------------------------------- #
# The architecture audit's OWN invariants.
#
# Everything above probes the r31 package. These probe the blocking build guard
# that is supposed to stop that package from regressing - and a guard nobody has
# ever watched fail is indistinguishable from a guard with a typo in its token
# list, because both are permanently green. Release 31's forbidden-call list
# already had to be rewritten once after it matched the campaign's own
# architecture label instead of a real call, which is exactly the failure these
# probes exist to expose.
#
# Each test breaks EXACTLY ONE thing in a source the audit reads, and asserts
# the audit notices. Nothing on disk is modified: `_read` is the audit's single
# file-access seam, so monkeypatching it presents mutated text to the check
# while the working tree stays untouched.
# --------------------------------------------------------------------------- #

def _audit_with(monkeypatch, overrides, files=()):
    """Run the Release 31 audit check with specific owner sources replaced.

    ``overrides`` maps a repo-relative path suffix to a callable that receives
    the real source text and returns the text the audit should see instead.
    """
    aud = importlib.import_module("scripts.audit_architecture")
    original = aud._read

    def fake(path, *args, **kwargs):
        rel = str(path).replace("\\", "/")
        for target, mutate in overrides.items():
            if rel.endswith(target):
                return mutate(original(path, *args, **kwargs) or "")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(aud, "_read", fake)
    return aud.check_release31_mathematical_alpha_frontier(list(files))


def _must_replace(old, new):
    """A mutator that REFUSES to be a no-op.

    Every negative probe below works by breaking the tree and asserting the audit
    notices. If the search text drifts out of the source - a reformat, a trailing
    comma, a line break moved - ``str.replace`` silently changes nothing, the tree
    stays healthy, the audit correctly reports health, and the probe fails in a way
    that looks like a broken guard rather than a broken probe. This one asserts the
    mutation actually landed, so the two failure modes stay distinguishable.
    """
    def mutate(text):
        out = text.replace(old, new)
        assert out != text, (
            "negative probe is INERT: the pattern it mutates is no longer present "
            "in the source, so this probe has stopped testing anything.\n"
            "missing pattern: %r" % (old,))
        return out
    return mutate


#: The Release 31 UI region, delimited exactly as the audit delimits it.
R31_REGION_START = 'id="r31-frontier"'
R31_REGION_END = "RELEASE 29 UX2: OPERATING DIAGNOSTICS"

#: The ONE route the Release 31 read model serves.
R31_ROUTE = "/v1/research/mathematical-alpha-frontier"

#: The canonical safety wording. 27B.6 settled this: paper orders are REAL and
#: live brokerage orders are structurally disabled, so a badge may not say
#: "no orders" without saying which kind.
CANONICAL_ORDER_BADGE = "NO LIVE BROKER ORDERS"
AMBIGUOUS_ORDER_BADGES = (">NO LIVE ORDERS</span>", ">ORDERS DISABLED<")


def _r31_region(html: str | None = None) -> str:
    """The Release 31 card only, never the whole document."""
    text = UI.read_text(encoding="utf-8") if html is None else html
    start = text.index(R31_REGION_START)
    end = text.find(R31_REGION_END, start)
    return text[start: end if end > start else start + 4000]


def _strip_in_r31_region(token: str):
    """Delete ``token`` from the Release 31 region and nowhere else."""
    def mutate(text: str) -> str:
        start = text.index(R31_REGION_START)
        end = text.index(R31_REGION_END, start)
        region = text[start:end]
        assert token in region, (
            "negative probe is INERT: %r is no longer in the Release 31 region, "
            "so this probe has stopped testing anything." % (token,))
        return text[:start] + region.replace(token, "", 1) + text[end:]
    return mutate


def _route_ownership() -> list[dict]:
    inv = json.loads(
        (REPO / "docs" / "architecture" / "system_inventory.json")
        .read_text(encoding="utf-8"))
    return inv["route_ownership"]


def _segs(path: str) -> list[str]:
    return [s for s in path.split("/") if s]


def _owner_for(path: str, ownership: list[dict]):
    """Longest-prefix owner resolution, as tests/test_architecture_contracts.py
    resolves it. Duplicated deliberately: importing the other test's private
    helper would couple two independent contracts to one implementation."""
    segs = _segs(path)
    best, best_len = None, -1
    for entry in ownership:
        if entry["prefix"] == "/":
            continue
        psegs = _segs(entry["prefix"])
        if psegs and psegs == segs[:len(psegs)] and len(psegs) > best_len:
            best, best_len = entry, len(psegs)
    return best


#: The Campaign v3 correction invariants, as (audit key, one broken source).
#: Each entry is a defect that SHIPPED in Campaign v2.
V3_INVARIANT_BLOCKS = ("universe_separation", "zero_base_primary",
                       "calibration_guard", "track_b_symbol_alignment",
                       "benchmark_duality", "covariance_cache", "supersession",
                       "point_in_time_training")


def test_the_audit_is_green_on_the_shipped_tree(monkeypatch):
    """The baseline. Without it, a probe passing proves nothing."""
    out = _audit_with(monkeypatch, {})
    assert out["modules_missing"] == []
    assert out["forbidden_calls_in_research_lane"] == []
    assert out["forbidden_operational_owner_refs"] == []
    assert out["research_lane_imports_api"] == []
    assert out["forbidden_engine_imports"] == []
    assert out["impure_engine_owner_imports"] == []
    assert out["budgets_not_encoded"] == []
    assert out["read_model_write_tokens"] == []
    assert out["ui_execute_controls"] == []
    assert out["ui_missing_safety_badges"] == []
    assert all(out["budgets_enforced"].values())
    assert all(out["canonical_owner_reuse"].values())
    assert all(out["lockbox_guard"].values())
    for block in V3_INVARIANT_BLOCKS:
        failing = sorted(k for k, v in out[block].items() if not v)
        assert not failing, "%s: %s" % (block, failing)


# --------------------------------------------------------------------------- #
# Negative probes for the Campaign v3 invariants.
#
# A guard that has never been proven to fail is not a proven guard. Each probe
# reintroduces ONE of the four v2 defects (or the look-ahead fallback found while
# building v3) and asserts the audit blocks the tree.
# --------------------------------------------------------------------------- #
def test_audit_catches_the_v2_universe_conflation(monkeypatch):
    """CORRECTION 1: evaluating on the training universe."""
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/contract.py": _must_replace(
            'EVALUATION_UNIVERSE = "EVALUATE_S_AND_P_500_PIT_MEMBERS_ONLY"',
            'EVALUATION_UNIVERSE = "EVALUATE_WHATEVER_THE_PANEL_CONTAINS"')})
    assert out["universe_separation"]["evaluation_universe_declared"] is False


def test_audit_catches_hindsight_membership(monkeypatch):
    """A membership source that is not point-in-time."""
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/universe.py": _must_replace(
            "index_constituent_timeseries", "watchlist_symbols_today")})
    assert out["universe_separation"]["membership_is_point_in_time"] is False


def test_audit_catches_top_n_restored_as_the_primary_verdict(monkeypatch):
    """CORRECTION 2: the top-N book carrying the economic verdict again."""
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/contract.py": _must_replace(
            "TOP_N_MAY_CARRY_PRIMARY_VERDICT = False",
            "TOP_N_MAY_CARRY_PRIMARY_VERDICT = True")})
    assert out["zero_base_primary"]["top_n_barred_from_primary_verdict"] is False


def test_audit_catches_a_second_portfolio_optimiser(monkeypatch):
    """The allocation seam must DELEGATE, never solve."""
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/allocation.py": _must_replace(
            "res = _zb.optimise(", "res = _local_solve(")})
    assert out["zero_base_primary"]["allocation_delegates_to_canonical_optimiser"] \
        is False


def test_audit_catches_a_revived_book_size_frontier(monkeypatch):
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/contract.py": _must_replace(
            "RISK_FRONTIER_GAMMA_MULTIPLIERS = (0.5, 1.0, 2.0)",
            "RISK_FRONTIER_BOOK_SIZES = (15, 25, 40)")})
    assert out["zero_base_primary"]["book_size_frontier_removed"] is False
    assert out["zero_base_primary"]["gamma_frontier_declared"] is False


def test_audit_catches_a_calibration_that_may_invert_a_model(monkeypatch):
    """The Release 30.1 defect: a negative slope reversing the model."""
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/calibration.py": _must_replace(
            "    if slope < 0.0:", "    if False:")})
    assert out["calibration_guard"]["negative_slope_raises"] is False


def test_audit_catches_a_calibration_allowed_to_read_the_lockbox(monkeypatch):
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/calibration.py": _must_replace(
            '"lockbox_used": False', '"lockbox_used": True')})
    assert out["calibration_guard"]["lockbox_invisible_to_calibration"] is False


def test_audit_catches_positional_turnover_in_track_b(monkeypatch):
    """CORRECTION 3: the v2 defect, reintroduced exactly as it was written."""
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/learners.py": _must_replace(
            "def _align_previous(", "def _align_by_position(")})
    assert out["track_b_symbol_alignment"]["aligns_by_symbol_union"] is False


def test_audit_catches_a_reinstated_positional_shape_test(monkeypatch):
    """The exact v2 line: comparing books whenever their LENGTHS match."""
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/learners.py": lambda t: t + (
            "\ndef _leak(prev_w, X, prev_shape):\n"
            "    if prev_w is not None and prev_shape == X.shape[0]:\n"
            "        return True\n")})
    assert out["track_b_symbol_alignment"]["no_positional_shape_comparison"] is False


def test_audit_catches_a_track_b_that_cannot_hold_cash(monkeypatch):
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/learners.py": _must_replace(
            "def _softmax_with_cash(", "def _softmax_fully_invested(")})
    assert out["track_b_symbol_alignment"]["track_b_can_hold_cash"] is False


def test_audit_catches_a_novel_decision_family_trading_for_free(monkeypatch):
    """The nonlinear Track-B family once discarded its cost rate outright."""
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/novel.py": _must_replace(
            "            prev_w = {str(s): float(x) for s, x in zip(syms, w) if x > 0.0}",
            "            _ = cost_rate")})
    assert out["track_b_symbol_alignment"]["novel_decision_family_prices_cost"] \
        is False


def test_audit_catches_the_spy_benchmark_being_substituted(monkeypatch):
    """CORRECTION 4: the investable comparison silently replaced."""
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/contract.py": _must_replace(
            "BENCHMARK_SUBSTITUTION_PERMITTED = False",
            "BENCHMARK_SUBSTITUTION_PERMITTED = True")})
    assert out["benchmark_duality"]["substitution_forbidden"] is False


def test_audit_catches_a_price_only_index_becoming_admissible(monkeypatch):
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/benchmarks.py": _must_replace(
            "PRICE_ONLY_INADMISSIBLE", "PRICE_ONLY_FINE_ACTUALLY")})
    assert out["benchmark_duality"]["price_only_index_inadmissible"] is False


def test_audit_catches_a_forked_covariance_calculation(monkeypatch):
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/covcache.py": _must_replace(
            "built = _hoc.build_covariance(", "built = _my_covariance(")})
    assert out["covariance_cache"]["delegates_to_canonical_builder"] is False


def test_audit_catches_a_covariance_cache_that_cannot_detect_staleness(monkeypatch):
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/covcache.py": _must_replace(
            "class CacheKeyMismatch(RuntimeError):", "class _Unused(RuntimeError):")})
    assert out["covariance_cache"]["key_mismatch_raises"] is False


def test_audit_catches_a_campaign_that_forgot_it_superseded_v2(monkeypatch):
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/contract.py": _must_replace(
            'CAMPAIGN_ID = "r31_mathematical_alpha_frontier_v3"',
            'CAMPAIGN_ID = "r31_mathematical_alpha_frontier_v2"')})
    assert out["supersession"]["campaign_is_v3"] is False


def test_audit_catches_the_look_ahead_training_fallback(monkeypatch):
    """The leak found while building v3: a warm-up window containing the future."""
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/methods.py": _must_replace(
            "            if len(train) < MIN_TRAIN_SECTIONS:",
            "            train = warmup[:max(12, len(warmup) // 4)]\n"
            "            if False:")})
    assert out["point_in_time_training"]["methods_have_no_warmup_fallback"] is False


def test_audit_catches_the_novel_look_ahead_fallback(monkeypatch):
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/novel.py": _must_replace(
            "    tr = [i for i in warm if i <= bucket]\n"
            "    return tr if len(tr) >= _methods.MIN_TRAIN_SECTIONS else None",
            "    return [i for i in warm if i <= bucket] or warm[:12]")})
    assert out["point_in_time_training"]["novel_has_no_warmup_fallback"] is False


def test_audit_catches_an_impure_engine_owner(monkeypatch):
    """Admission to the engine allowlist is by NAME; purity must be proven.

    Without this, a future edit could pull a database dependency into the
    research lane behind a module name the allowlist already trusts.
    """
    out = _audit_with(monkeypatch, {
        "engine/holding_opportunity_cost.py": lambda t: "import sqlalchemy\n" + t})
    assert "holding_opportunity_cost:sqlalchemy" in out["impure_engine_owner_imports"]


def test_audit_catches_an_order_call_in_the_research_lane(monkeypatch):
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/judge.py": lambda t: t + "\ndef _x():\n    create_order()\n"})
    assert "create_order(" in out["forbidden_calls_in_research_lane"]


def test_audit_catches_a_reference_to_an_operational_owner(monkeypatch):
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/campaign.py":
            lambda t: t + "\nfrom paper_trader.api.portfolio_decision import x\n"})
    assert "api.portfolio_decision" in out["forbidden_operational_owner_refs"]


def test_audit_catches_the_research_lane_importing_the_api(monkeypatch):
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/novel.py":
            lambda t: t + "\nfrom paper_trader.api import operational_book\n"})
    assert "novel" in out["research_lane_imports_api"]


def test_audit_catches_a_forked_engine_owner(monkeypatch):
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/judge.py":
            lambda t: t + "\nfrom ...engine import reallocation_proposal\n"})
    assert "reallocation_proposal" in out["forbidden_engine_imports"]


def test_audit_catches_a_budget_that_stopped_being_a_number(monkeypatch):
    """A budget demoted to prose is a suggestion, not a limit."""
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/contract.py":
            lambda t: t.replace("MAX_LOCKBOX_CANDIDATES = ",
                                "MAX_LOCKBOX_CANDIDATES_DOC = ")})
    assert "MAX_LOCKBOX_CANDIDATES" in out["budgets_not_encoded"]


def test_audit_catches_a_budget_that_stopped_being_enforced(monkeypatch):
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/registry.py":
            lambda t: t.replace("raise BudgetExceeded", "pass  # BudgetExceeded")})
    assert out["budgets_enforced"]["registry_raises_on_budget"] is False


def test_audit_catches_a_lockbox_that_stopped_being_single_use(monkeypatch):
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/lockbox.py":
            lambda t: t.replace("has already used its single lockbox execution",
                                "may be re-run")})
    assert out["lockbox_guard"]["single_execution_enforced"] is False


def test_audit_catches_the_judge_forking_the_canonical_policy(monkeypatch):
    """The policy chain is judge -> allocation -> canonical owner.

    Breaking any link must be caught. The v3 judge reaches the policy through the
    shared construction seam rather than importing the allocator itself, so the
    probe cuts the seam rather than a name in the judge's own source.
    """
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/judge.py":
            _must_replace("_alloc.policy()", "_my_own_policy()")})
    assert out["canonical_owner_reuse"]["judge_reads_canonical_policy"] is False

    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/allocation.py":
            _must_replace("_zb.default_policy()", "{'cost_rate_per_side': 0.0}")})
    assert out["canonical_owner_reuse"]["judge_reads_canonical_policy"] is False


def test_audit_catches_a_judge_that_declares_its_own_policy(monkeypatch):
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/judge.py":
            lambda t: t + "\ndef default_policy():\n    return {}\n"})
    assert out["canonical_owner_reuse"]["judge_defines_no_policy_of_its_own"] is False


def test_audit_catches_a_second_cost_literal(monkeypatch):
    """The cost rate has one owner. A literal anywhere else is a fork."""
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/judge.py": lambda t: t + "\ncost_bps_per_side = 5\n"})
    assert "judge" in out["duplicate_cost_literal_modules"]


def test_audit_catches_a_second_owner_of_the_judge_contract(monkeypatch):
    """One owner per concern - a copied schema token is the drift."""
    second = REPO / "api" / "_r31_probe_second_judge.py"
    out = _audit_with(
        monkeypatch,
        {"api/_r31_probe_second_judge.py":
            lambda t: 'JUDGE_SCHEMA = "r31_research_judge_contract.v1"\n'},
        files=[second])
    assert "api/_r31_probe_second_judge.py" in out["second_owner_modules"]["research_judge"]


def test_audit_catches_a_write_creeping_into_the_read_model(monkeypatch):
    out = _audit_with(monkeypatch, {
        "api/mathematical_alpha_frontier.py":
            lambda t: t + "\ndef _save(p):\n    p.write_text('x')\n"})
    assert "write_text" in out["read_model_write_tokens"]


def test_audit_catches_automatic_promotion_being_switched_on(monkeypatch):
    out = _audit_with(monkeypatch, {
        "alpha_agent/r31/__init__.py":
            lambda t: t.replace("AUTOMATIC_PROMOTION_ALLOWED = False",
                                "AUTOMATIC_PROMOTION_ALLOWED = True")})
    assert out["automatic_promotion"]["declared_false"] is False


def test_audit_catches_an_unauthenticated_read_route(monkeypatch):
    # The route declaration spans several lines and ends with a trailing comma,
    # which the original pattern did not match - so this probe removed nothing and
    # proved nothing. ``_must_replace`` now fails loudly if that recurs.
    out = _audit_with(monkeypatch, {
        "api/app.py": _must_replace(
            "    dependencies=[Depends(_verify_api_key)],\n"
            ")\ndef get_mathematical_alpha_frontier",
            ")\ndef get_mathematical_alpha_frontier")})
    assert out["read_surface"]["route_authenticated"] is False


def test_audit_catches_an_execute_control_added_to_the_ui_region(monkeypatch):
    out = _audit_with(monkeypatch, {
        "api/ui/index.html": lambda t: t.replace(
            'id="r31-frontier"', 'id="r31-frontier"><button>Promote', 1)})
    assert "<button" in out["ui_execute_controls"]
    assert "Promote" in out["ui_execute_controls"]


def test_audit_catches_a_safety_badge_removed_from_the_ui_region(monkeypatch):
    # Region-scoped, because the canonical badge text also appears on seventeen
    # OPERATIONAL surfaces elsewhere in the document. A whole-file
    # ``replace(..., 1)`` would delete the first of those instead, leave the
    # Release 31 region intact, and quietly stop testing this guard.
    out = _audit_with(monkeypatch, {
        "api/ui/index.html": _strip_in_r31_region("NO LIVE BROKER ORDERS")})
    assert "NO LIVE BROKER ORDERS" in out["ui_missing_safety_badges"]


def test_audit_catches_the_ambiguous_order_badge_returning(monkeypatch):
    """The exact wording defect that Release 31 shipped, as a negative probe.

    The region originally carried ``>NO LIVE ORDERS</span>``, which reads as
    "this system places no orders". Paper Trader DOES create paper orders under
    a governed manual workflow; only LIVE BROKER orders are structurally
    disabled. Three long-standing contracts refuse the ambiguous form, and the
    Release 31 gate had no opinion about it at all, so the defect reached a
    broad regression instead of this file.
    """
    out = _audit_with(monkeypatch, {
        "api/ui/index.html": _must_replace(
            '<span class="cc-badge safe">RESEARCH ONLY</span>',
            '<span class="cc-badge safe">RESEARCH ONLY</span>'
            '<span class="cc-badge safe">NO LIVE ORDERS</span>')})
    assert ">NO LIVE ORDERS</span>" in out["ui_ambiguous_safety_badges"]


def test_audit_catches_orders_disabled_returning_to_the_ui_region(monkeypatch):
    out = _audit_with(monkeypatch, {
        "api/ui/index.html": _must_replace(
            '<span class="cc-badge safe">READ ONLY</span>',
            '<span class="cc-badge safe">READ ONLY</span>'
            '<span class="cc-badge safe">ORDERS DISABLED</span>')})
    assert ">ORDERS DISABLED<" in out["ui_ambiguous_safety_badges"]


# =========================================================================== #
# The wording and route-ownership contracts that Release 31 broke.
#
# Both defects were invisible to this file and to the bounded Release 31 gate,
# and both were caught only by a ~28-minute broad repository regression. They
# are guarded HERE now, on the Release 31 surface itself, so the bounded gate
# refuses them in seconds.
# =========================================================================== #


def test_the_release31_region_says_which_kind_of_order_is_impossible():
    region = _r31_region()
    assert CANONICAL_ORDER_BADGE in region
    for ambiguous in AMBIGUOUS_ORDER_BADGES:
        assert ambiguous not in region, (
            "the Release 31 region carries %r, which reads as 'this system "
            "places no orders'. Paper Trader DOES create paper orders under a "
            "governed manual workflow; only live brokerage orders are "
            "structurally disabled." % (ambiguous,))


def test_the_release31_region_states_that_paper_orders_are_real():
    """The distinction must be READABLE, not merely encoded in a badge token."""
    region = _r31_region()
    assert "Paper orders are real" in region
    assert "Live brokerage orders are structurally disabled" in region
    assert "creates none" in region


def test_the_whole_ui_carries_no_ambiguous_order_badge():
    """The three legacy contracts Release 31 broke, enforced from inside the
    Release 31 gate.

    tests/test_alpha_agent_stage12.py, tests/test_phase27b7_operator_hard_cutover.py
    and tests/test_phase27b8_operational_portfolio.py each assert this over the
    whole document. Release 31 added ONE badge and failed all three at once.
    """
    html = UI.read_text(encoding="utf-8")
    assert html.count(">NO LIVE ORDERS</span>") == 0
    assert html.count(">ORDERS DISABLED<") == 0
    assert CANONICAL_ORDER_BADGE in html


def test_the_release31_route_is_declared_and_has_exactly_one_canonical_owner():
    """Tests the REAL route surface, not a string in a document."""
    aud = importlib.import_module("scripts.audit_architecture")
    declared = {r["path"] for r in aud.check_routes()["routes"]}
    assert R31_ROUTE in declared, (
        "the Release 31 read route is no longer declared by the application")

    ownership = _route_ownership()
    exact = [e for e in ownership if e["prefix"] == R31_ROUTE]
    assert len(exact) == 1, (
        "the Release 31 route must have exactly ONE canonical owner entry, "
        "found %d" % len(exact))
    assert exact[0]["owner"] == "api/mathematical_alpha_frontier.py"

    resolved = _owner_for(R31_ROUTE, ownership)
    assert resolved is not None and resolved["owner"] == exact[0]["owner"]


def test_every_route_the_release31_read_model_serves_has_an_owner():
    aud = importlib.import_module("scripts.audit_architecture")
    ownership = _route_ownership()
    r31_routes = [r["path"] for r in aud.check_routes()["routes"]
                  if r["path"].startswith(R31_ROUTE)]
    assert r31_routes
    unmapped = sorted(p for p in r31_routes if _owner_for(p, ownership) is None)
    assert not unmapped, "Release 31 routes with no owner: %s" % (unmapped,)


def test_a_research_route_without_an_owner_is_refused():
    """Negative probe: prove the ownership guard can actually fail.

    A guard that has never been shown to fail is not proven. Release 31's route
    reached a broad regression precisely because nothing bounded ever asked
    this question.
    """
    ownership = _route_ownership()

    # 1. Remove Release 31's own entry: its route must become unowned.
    without_r31 = [e for e in ownership if e["prefix"] != R31_ROUTE]
    assert len(without_r31) == len(ownership) - 1
    assert _owner_for(R31_ROUTE, without_r31) is None, (
        "removing the Release 31 ownership entry left the route owned, so some "
        "broader prefix is silently absorbing it and the guard proves nothing")

    # 2. A brand-new research GET route nobody has registered is unowned too,
    #    which is what makes the guard bite for the NEXT release rather than
    #    only for this one.
    assert _owner_for("/v1/research/an-unregistered-surface", ownership) is None


def test_audit_catches_a_news_shaped_feature_entering_the_frozen_set(monkeypatch):
    out = _audit_with(monkeypatch, {
        "alpha_agent/release30_panel.py": lambda t: t.replace(
            "FUNDAMENTAL_FEATURES =",
            'NEWS_SENTIMENT_FEATURE = "sentiment"\nFUNDAMENTAL_FEATURES =', 1)})
    assert "sentiment" in out["news_shaped_features"]
