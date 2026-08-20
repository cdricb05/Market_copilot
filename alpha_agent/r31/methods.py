"""alpha_agent.r31.methods - the Release 31 known-method registry and tournament.

Phase 5 asks whether published human methods already solve more of this problem
than our architecture does. It is answered by REPRODUCTION, not by summary: every
method below is either implemented against the owned snapshot, or recorded as
excluded with the reason it could not be reproduced here.

Literature exhaustion
---------------------
Screening stopped under the contract's rule - TWO consecutive query/reference
expansions that yielded no new materially distinct ADMISSIBLE method family.
Expansions 1-3 added instrumented factor models, SDF shrinkage, adversarial deep
learning and quantile machine learning; expansions 4-5 returned only methods
already registered here. Excluded methods are listed with their reason, because
"we did not run it" and "it cannot be run on this data" are different facts.

Training discipline shared by every family
------------------------------------------
A candidate is fitted on an EXPANDING window that ends an embargo before the date
being scored, and the training pool is capped at the last VALIDATION date. No
model in this campaign ever sees a lockbox row - not during selection, and not
during the lockbox evaluation itself, where the model scored is the single final
model fitted on discovery plus validation. That is the deployment analogue: on
the day it would be used, the model knows everything up to then and nothing
after.
"""
from __future__ import annotations

import time
from typing import Callable, Optional

import numpy as np

from .. import r31
from . import contract as _contract
from . import judge as _judge
from . import learners as _L
from . import partition as _partition
from . import registry as _registry
from . import snapshot as _snapshot

CALCULATION_OWNER = "alpha_agent.r31.methods"
LITERATURE_SCHEMA = "r31_literature_method_registry/1"
KNOWN_SCHEMA = "r31_known_method_registry/1"
RESULTS_SCHEMA = "r31_known_method_results/1"

LITERATURE_ARTIFACT = "literature_method_registry.json"
KNOWN_ARTIFACT = "known_method_registry.json"
RESULTS_ARTIFACT = "known_method_results.json"

#: Refit cadence, in decision dates. An expanding-window refit every ~year is the
#: standard walk-forward compromise: refitting at every date multiplies cost
#: without changing conclusions on monthly data, and refitting once makes a
#: 25-year backtest a claim about a single fit.
REFIT_EVERY = 12

#: Minimum decision dates that must precede a date before any model may predict
#: on it - about two years of monthly cross-sections.
#:
#: This bound is LOAD-BEARING, and it replaced a genuine look-ahead leak. The
#: predictor previously carried a fallback: if the legitimate expanding window
#: held fewer than twelve dates it trained on ``warmup[:60]`` instead, which is
#: the first sixty dates of the layer regardless of where the scored date sits.
#: On the validation layer that branch is unreachable, so Campaign v2 never
#: executed it. Campaign v3 fits its Track-A calibration by running the predictor
#: across DISCOVERY, where the earliest dates DO reach it - and there the fallback
#: trains on dates AFTER the one being scored. A calibration fitted on those
#: scores would inherit the leak and then price capital with it.
#:
#: There is no legitimate fallback for "not enough history yet". The honest answer
#: is that no model exists at that date, so the predictor returns NaN and the
#: calibration skips the date.
MIN_TRAIN_SECTIONS = 24


# --------------------------------------------------------------------------- #
# Literature registry
# --------------------------------------------------------------------------- #
def _paper(*, key, citation, year, problem, target, loss, structure, validation,
           portfolio, cost, risk, hyperparameters, reproducible, not_reproducible,
           family: Optional[str], status: str, url: str) -> dict:
    return {"key": key, "citation": citation, "publication_year": year,
            "problem_formulation": problem, "target_variable": target,
            "loss_objective": loss, "model_structure": structure,
            "validation_structure": validation, "portfolio_construction": portfolio,
            "cost_treatment": cost, "risk_treatment": risk,
            "hyperparameters": hyperparameters,
            "reproducible_with_owned_data": reproducible,
            "not_reproducible_and_why": not_reproducible,
            "implemented_as_family": family, "status": status, "source_url": url}


LITERATURE = [
    _paper(key="gu_kelly_xiu_2020",
           citation="Gu, Kelly & Xiu, 'Empirical Asset Pricing via Machine Learning', Review of Financial Studies 33(5)",
           year=2020,
           problem="comparative analysis of ML methods for the cross-section of expected returns",
           target="forward individual stock excess return",
           loss="squared error, and a Huber variant for fat tails",
           structure="OLS/ridge, PCR, PLS, elastic net, gradient boosted trees, random forest, shallow neural networks NN1-NN5",
           validation="strict chronological train/validate/test with expanding window",
           portfolio="decile sorts, long-short and long-only value/equal weight",
           cost="reported separately; forecasts are cost-agnostic",
           risk="Sharpe ratio of the sorted portfolios",
           hyperparameters="penalty strength, tree depth/count, network width",
           reproducible="ridge, elastic net, PCR, PLS, GBRT, random forest, extra trees, shallow NN, Huber loss - all implemented here in numpy",
           not_reproducible="NN4/NN5 depth is not supported by this sample size or by an unavailable framework",
           family="ridge|elastic_net|dim_reduction|gbrt|random_forest|extra_trees|neural_net|huber",
           status="REPRODUCED",
           url="https://academic.oup.com/rfs/article/33/5/2223/5758276"),
    _paper(key="jensen_kelly_malamud_pedersen_2026",
           citation="Jensen, Kelly, Malamud & Pedersen, 'Machine Learning and the Implementable Efficient Frontier', Review of Financial Studies",
           year=2026,
           problem="portfolios should be judged on net-of-cost return per unit of risk; learn WEIGHTS under an economic objective rather than forecast returns first",
           target="portfolio weights",
           loss="net-of-trading-cost mean-variance utility",
           structure="trading-cost-aware portfolio optimisation integrated with ML; weights learned directly",
           validation="chronological out-of-sample",
           portfolio="the model output IS the portfolio",
           cost="transitory and persistent transaction costs inside the objective",
           risk="mean-variance with explicit risk aversion",
           hyperparameters="risk aversion, cost scale, signal set",
           reproducible="the decision-focused objective - learn a score whose induced book maximises net utility including its own turnover - is implemented as the direct_portfolio family",
           not_reproducible="the continuous-time persistent-cost formulation and the authors' proprietary cost model; the campaign uses the canonical flat per-side cost the operator actually faces",
           family="direct_portfolio", status="REPRODUCED_IN_SPIRIT",
           url="https://academic.oup.com/rfs/advance-article/doi/10.1093/rfs/hhag022/8524346"),
    _paper(key="rapach_strauss_zhou_2010",
           citation="Rapach, Strauss & Zhou, 'Out-of-Sample Equity Premium Prediction: Combination Forecasts', Review of Financial Studies 23(2)",
           year=2010,
           problem="model uncertainty and instability impair single-model forecasts; combining is a strong shrinkage estimator",
           target="forward return",
           loss="squared error of the combined forecast",
           structure="simple and weighted combinations of individual model forecasts",
           validation="expanding-window out-of-sample",
           portfolio="mean-variance timing",
           cost="not treated",
           risk="utility gain at fixed risk aversion",
           hyperparameters="combination weights, weighting scheme",
           reproducible="equal-weight and validation-weighted combinations of the campaign's fitted members",
           not_reproducible=None, family="combination", status="REPRODUCED",
           url="https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1257858"),
    _paper(key="fama_macbeth_1973",
           citation="Fama & MacBeth, 'Risk, Return, and Equilibrium: Empirical Tests', Journal of Political Economy 81(3)",
           year=1973,
           problem="estimate cross-sectional risk premia period by period and average",
           target="forward cross-sectional return",
           loss="per-period OLS, coefficients averaged over periods",
           structure="two-pass cross-sectional regression",
           validation="time-series average of per-period slopes; Newey-West for serial correlation",
           portfolio="characteristic-weighted",
           cost="not treated", risk="t-statistics on averaged slopes",
           hyperparameters="optional t-shrinkage of averaged slopes",
           reproducible="fully; implemented as the fama_macbeth family",
           not_reproducible=None, family="fama_macbeth", status="REPRODUCED",
           url="https://en.wikipedia.org/wiki/Fama%E2%80%93MacBeth_regression"),
    _paper(key="koenker_bassett_1978",
           citation="Koenker & Bassett, 'Regression Quantiles', Econometrica 46(1); with Quantile ML for the cross-section",
           year=1978,
           problem="model conditional QUANTILES rather than the conditional mean",
           target="conditional quantiles of forward return",
           loss="pinball / check loss",
           structure="linear quantile regression at several taus",
           validation="chronological out-of-sample",
           portfolio="rank by a chosen quantile rather than the mean",
           cost="not treated", risk="the lower quantile IS the risk statement",
           hyperparameters="tau set, ridge penalty",
           reproducible="fully; implemented as the quantile family, which is the campaign's Track-C distributional entry",
           not_reproducible=None, family="quantile", status="REPRODUCED",
           url="https://arxiv.org/pdf/2408.07497"),
    _paper(key="zou_hastie_2005",
           citation="Zou & Hastie, 'Regularization and Variable Selection via the Elastic Net', JRSS-B 67(2)",
           year=2005,
           problem="sparse selection among correlated predictors",
           target="forward return", loss="squared error with L1+L2 penalty",
           structure="coordinate descent", validation="chronological",
           portfolio="n/a", cost="n/a", risk="n/a",
           hyperparameters="alpha, l1_ratio",
           reproducible="fully", not_reproducible=None,
           family="elastic_net", status="REPRODUCED",
           url="https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2945663"),
    _paper(key="breiman_2001",
           citation="Breiman, 'Random Forests', Machine Learning 45(1); Geurts et al., 'Extremely Randomized Trees'",
           year=2001,
           problem="variance reduction by bootstrap aggregation of decision trees",
           target="forward return", loss="squared error",
           structure="bagged trees with optimised splits; extra-trees draw split thresholds",
           validation="chronological", portfolio="sorted",
           cost="n/a", risk="n/a",
           hyperparameters="tree count, depth, leaf size, feature fraction",
           reproducible="fully", not_reproducible=None,
           family="random_forest|extra_trees", status="REPRODUCED",
           url="https://link.springer.com/article/10.1007/s00291-022-00693-w"),
    _paper(key="kozak_nagel_santosh_2020",
           citation="Kozak, Nagel & Santosh, 'Shrinking the Cross-Section', Journal of Financial Economics 135(2)",
           year=2020,
           problem="a dense SDF over many characteristics, shrunk toward high-variance principal components",
           target="SDF coefficients over characteristic portfolios",
           loss="penalised GMM/ridge objective with an economic prior",
           structure="ridge plus PC shrinkage on characteristic-portfolio returns",
           validation="chronological out-of-sample",
           portfolio="SDF-implied tangency weights",
           cost="not treated", risk="mean-variance",
           hyperparameters="two penalties",
           reproducible="the shrinkage-toward-leading-PCs idea is captured by the ridge and dim_reduction families on the same characteristic set",
           not_reproducible="the full SDF/tangency formulation targets a covariance-scaled objective on characteristic PORTFOLIOS rather than a per-name score; it would need a second portfolio optimiser, which the campaign forbids",
           family="ridge|dim_reduction", status="PARTIALLY_REPRODUCED",
           url="https://www.nber.org/papers/w24070"),
    _paper(key="freyberger_neuhierl_weber_2020",
           citation="Freyberger, Neuhierl & Weber, 'Dissecting Characteristics Nonparametrically', Review of Financial Studies 33(5)",
           year=2020,
           problem="which characteristics carry INCREMENTAL information, with flexible functional form",
           target="forward return",
           loss="squared error with an adaptive group-lasso penalty over spline bases",
           structure="nonparametric additive model, group lasso selection",
           validation="chronological", portfolio="sorted",
           cost="not treated", risk="Sharpe ratio",
           hyperparameters="spline knots, group penalty",
           reproducible="the nonlinearity it demonstrates is captured by the tree families; the campaign's features are already per-date ranks, which is the paper's own monotone transform",
           not_reproducible="the adaptive group-lasso spline basis is a 13th family and would breach the frozen family budget; recorded as excluded rather than run",
           family=None, status="EXTRACTED_NOT_IMPLEMENTED",
           url="https://www.nber.org/papers/w23227"),
    _paper(key="kelly_pruitt_su_2019",
           citation="Kelly, Pruitt & Su, 'Characteristics Are Covariances: A Unified Model of Risk and Return' (IPCA), Journal of Financial Economics 134(3)",
           year=2019,
           problem="latent factors with characteristic-instrumented time-varying loadings",
           target="returns decomposed into factor exposure and anomaly intercept",
           loss="alternating least squares on the panel",
           structure="instrumented principal components",
           validation="chronological", portfolio="factor-mimicking",
           cost="not treated", risk="latent factor covariance",
           hyperparameters="number of latent factors",
           reproducible="not attempted",
           not_reproducible="IPCA answers a RISK-DECOMPOSITION question and yields a factor model, not a per-name allocation score; consuming it would require a second risk owner beside the canonical covariance owner, which the campaign forbids. Recorded as excluded",
           family=None, status="EXTRACTED_NOT_IMPLEMENTED",
           url="https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2983919"),
    _paper(key="chen_pelger_zhu_2023",
           citation="Chen, Pelger & Zhu, 'Deep Learning in Asset Pricing', Management Science",
           year=2023,
           problem="estimate the SDF with a no-arbitrage criterion and adversarially constructed test assets",
           target="SDF weights",
           loss="no-arbitrage moment conditions, adversarial (GAN) objective",
           structure="feed-forward and recurrent networks plus an adversarial network",
           validation="chronological", portfolio="SDF-implied",
           cost="not treated", risk="pricing errors and Sharpe ratio",
           hyperparameters="network depth/width, adversarial schedule, macro state dimension",
           reproducible="not attempted",
           not_reproducible="RESOURCE_INFEASIBLE: adversarial training of two networks over a macro state panel needs a framework this environment deliberately does not install, and the owned macro history is far shorter than the equity panel. Recorded as excluded",
           family=None, status="EXTRACTED_NOT_IMPLEMENTED",
           url="https://pubsonline.informs.org/doi/10.1287/mnsc.2023.4695"),
    _paper(key="demiguel_2020",
           citation="DeMiguel, Martin-Utrera, Nogales & Uppal, 'A Transaction-Cost Perspective on the Multitude of Firm Characteristics', Review of Financial Studies 33(5)",
           year=2020,
           problem="how transaction costs change WHICH characteristics matter jointly",
           target="optimal portfolio weights net of cost",
           loss="net-of-cost mean-variance utility",
           structure="parametric portfolio policy over characteristics",
           validation="chronological",
           portfolio="characteristic-parameterised weights",
           cost="quadratic and proportional costs, trades netted across characteristics",
           risk="mean-variance",
           hyperparameters="risk aversion, cost model",
           reproducible="the netting insight motivates the direct_portfolio family's turnover term, which prices the trade the score implies rather than the score alone",
           not_reproducible="their proprietary per-stock cost estimates; the campaign uses the canonical flat per-side rate",
           family="direct_portfolio", status="REPRODUCED_IN_SPIRIT",
           url="https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2912819"),
    _paper(key="hansen_2005",
           citation="Hansen, 'A Test for Superior Predictive Ability', Journal of Business & Economic Statistics 23(4); White, 'A Reality Check for Data Snooping', Econometrica 68(5)",
           year=2005,
           problem="is the BEST of many searched strategies genuinely better than a benchmark?",
           target="the maximum standardised outperformance statistic",
           loss="n/a - an inference procedure",
           structure="studentised statistic with a sample-dependent recentred null, stationary bootstrap",
           validation="n/a", portfolio="n/a", cost="n/a", risk="n/a",
           hyperparameters="bootstrap block length, resample count",
           reproducible="fully; implemented in alpha_agent.r31.multiple_testing and applied to EVERY executed candidate",
           not_reproducible=None, family=None, status="REPRODUCED_AS_INFERENCE",
           url="https://papers.ssrn.com/sol3/papers.cfm?abstract_id=264569"),
    _paper(key="bryzgalova_2025",
           citation="Bryzgalova, Pelger & Zhu, 'Forest through the Trees: Building Cross-Sections of Stock Returns', Journal of Finance",
           year=2025,
           problem="construct test assets by recursive characteristic sorting",
           target="test-asset portfolios",
           loss="pruned tree objective on portfolio returns",
           structure="decision trees over characteristic sorts",
           validation="chronological", portfolio="tree-sorted basis assets",
           cost="not treated", risk="SDF pricing errors",
           hyperparameters="tree depth, pruning",
           reproducible="not attempted",
           not_reproducible="it builds TEST ASSETS for pricing-model evaluation rather than a per-name allocation decision function; it does not answer the campaign's question",
           family=None, status="EXTRACTED_NOT_IMPLEMENTED",
           url="https://onlinelibrary.wiley.com/doi/10.1111/jofi.13477"),
]

#: Primary sources SCREENED but not deeply extracted, recorded for the count.
SCREENED_NOT_EXTRACTED = [
    "Stock & Watson (2002) diffusion-index forecasting - PCR ancestor, superseded here by gu_kelly_xiu_2020",
    "Bates & Granger (1969) combination of forecasts - ancestor of rapach_strauss_zhou_2010",
    "Friedman (2001) greedy function approximation - GBRT ancestor, implemented via release30_models",
    "Geurts, Ernst & Wehenkel (2006) extremely randomised trees - implemented via release30_models",
    "Politis & Romano (1994) the stationary bootstrap - implemented as inference machinery",
    "Newey & West (1987) HAC covariance - implemented as inference machinery",
    "Novy-Marx (2013) the other side of value: gross profitability - already an owned feature",
    "Fama & French (2015) five-factor model - its characteristics are already owned features",
    "Engle & Manganelli (2004) CAViaR - a time-series VaR model, not a cross-sectional decision function",
    "Wold (1975) partial least squares - implemented as the PLS variant",
    "Hoerl & Kennard (1970) ridge regression - implemented via release30_models",
    "Huber (1964) robust estimation - implemented as the huber family",
]

LITERATURE_STOPPING_RULE = {
    "rule": "stop when TWO consecutive query/reference expansions yield no new "
            "materially distinct ADMISSIBLE method family",
    "expansions_run": 5,
    "expansions_yielding_new_admissible_family": 3,
    "consecutive_dry_expansions": 2,
    "state": "LITERATURE_EXHAUSTED_UNDER_CONTRACT",
}


def literature_registry(*, campaign_id: str = _contract.CAMPAIGN_ID) -> dict:
    extracted = [p for p in LITERATURE]
    body = {
        "contract": LITERATURE_SCHEMA,
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "papers_screened": len(LITERATURE) + len(SCREENED_NOT_EXTRACTED),
        "papers_screened_budget": _contract.MAX_PAPERS_SCREENED,
        "methods_deeply_extracted": len(extracted),
        "methods_extracted_budget": _contract.MAX_METHODS_EXTRACTED,
        "methods": extracted,
        "screened_not_extracted": list(SCREENED_NOT_EXTRACTED),
        "implemented_families": sorted(FAMILY_SPECS),
        "implemented_family_count": len(FAMILY_SPECS),
        "family_budget": _contract.MAX_KNOWN_METHOD_FAMILIES,
        "excluded_methods": [
            {"key": p["key"], "reason": p["not_reproducible_and_why"]}
            for p in LITERATURE if p["status"] == "EXTRACTED_NOT_IMPLEMENTED"],
        "stopping_rule": dict(LITERATURE_STOPPING_RULE),
    }
    body["literature_hash"] = r31.sha(body)
    body.update(r31.safety_block())
    return body


# --------------------------------------------------------------------------- #
# The twelve implemented families and their bounded grids
# --------------------------------------------------------------------------- #
def _grid(name: str, params: list) -> dict:
    return {"family": name, "params": params, "n_configs": len(params)}


#: Twelve materially distinct families, each with a DELIBERATELY SPARSE grid.
#:
#: The v3 judge allocates capital through the canonical zero-base optimiser at
#: every decision date, which costs 4-12 seconds per date against roughly a
#: millisecond for v2's top-N book. The contract's ceilings (240 known
#: configurations, 40 per family) are unchanged; the EXECUTED grid is far below
#: them, because the contract's own rule is that configurations are executed for
#: what they test, never to consume a budget.
#:
#: Points were removed only where a grid was dense in a direction the family is
#: known to be insensitive to - three neighbouring ridge penalties test one
#: hypothesis, not three - and never where two configurations express materially
#: different structure. Penalties stay LOG-SPACED so the retained points span the
#: same range the dense grid did. A sparser pre-registered search also carries a
#: smaller multiple-testing denominator, so each survivor carries more evidence.
FAMILY_SPECS = {
    "ridge": _grid("ridge", [{"alpha": a} for a in (3.0, 30.0, 300.0)]),
    "elastic_net": _grid("elastic_net", [
        {"alpha": 1e-4, "l1_ratio": r} for r in (0.1, 0.5, 0.9)]),
    "dim_reduction": _grid("dim_reduction", [
        {"variant": "PCR", "n_components": 3},
        {"variant": "PCR", "n_components": 8},
        {"variant": "PLS", "n_components": 2},
        {"variant": "PLS", "n_components": 5}]),
    "fama_macbeth": _grid("fama_macbeth", [{"shrink": s} for s in (0.0, 2.0)]),
    "huber": _grid("huber", [{"delta": 1.0, "alpha": 30.0},
                             {"delta": 1.5, "alpha": 30.0}]),
    "gbrt": _grid("gbrt", [
        {"n_trees": 120, "max_depth": 2, "learning_rate": 0.03},
        {"n_trees": 240, "max_depth": 3, "learning_rate": 0.01},
        {"n_trees": 180, "max_depth": 4, "learning_rate": 0.02}]),
    "random_forest": _grid("random_forest", [
        {"n_trees": 100, "max_depth": 4, "min_leaf": 300},
        {"n_trees": 200, "max_depth": 6, "min_leaf": 600}]),
    "extra_trees": _grid("extra_trees", [
        {"n_trees": 100, "max_depth": 4, "min_leaf": 400},
        {"n_trees": 200, "max_depth": 6, "min_leaf": 400}]),
    "neural_net": _grid("neural_net", [
        {"hidden": (8,), "weight_decay": 1e-4, "epochs": 40},
        {"hidden": (16, 8), "weight_decay": 1e-4, "epochs": 40},
        {"hidden": (32, 16), "weight_decay": 1e-3, "epochs": 40}]),
    "quantile": _grid("quantile", [
        {"tau": [0.5], "alpha": 1.0},
        {"tau": [0.5], "alpha": 10.0},
        {"tau": [0.25, 0.5, 0.75], "alpha": 1.0}]),
    "direct_portfolio": _grid("direct_portfolio", [
        {"gamma": g, "temperature": t, "epochs": 150}
        for g in (0.5, 3.0) for t in (3.0, 8.0)]),
    "combination": _grid("combination", [
        {"scheme": "EQUAL_WEIGHT_ALL_FAMILIES"},
        {"scheme": "VALIDATION_IC_WEIGHTED"},
        {"scheme": "TOP3_VALIDATION_EQUAL_WEIGHT"}]),
}

#: Which architecture each family belongs to. Track B emits WEIGHTS and needs no
#: return forecast at all - that is the point of the architecture - so it never
#: passes through the Track-A calibration.
FAMILY_TRACK = {f: _contract.TRACK_A for f in FAMILY_SPECS}
FAMILY_TRACK["direct_portfolio"] = _contract.TRACK_B

#: Benchmarks. Declared separately and exempt from the CANDIDATE family budget -
#: they are the incumbent and the transparent references, not part of the search.
BENCHMARKS = {
    "incumbent_momentum_leg": {
        "weights": {"mom_6_1": 1.0},
        "note": "the approved operational model's momentum leg, unchanged. On "
                "the survivorship-free price sample this is the closest "
                "point-in-time reconstruction of the incumbent that the owned "
                "data supports, because the composite_sn fundamental leg has no "
                "survivorship-free history.",
        "samples": [_contract.SAMPLE_PRICE_FULL, _contract.SAMPLE_FUND_MATCHED]},
    "incumbent_blend_pit": {
        "weights": {"mom_6_1": 0.5, "fcf_to_assets": 0.25,
                    "operating_accruals": -0.25},
        "note": "point-in-time STRUCTURAL reconstruction of "
                "fundamental_momentum_50_50_v1: half momentum, half the "
                "composite_sn cash-flow/accrual pair. Sector neutralisation is "
                "deliberately absent - the canonical PIT sector owner declares "
                "its snapshot inadmissible for historical construction.",
        "samples": [_contract.SAMPLE_FUND_MATCHED]},
    "s25_operating_profitability": {
        "weights": {"s25_operating_profitability": 1.0},
        "note": "the Stage-25 frozen research challenger standing alone, carried "
                "under its own name so its contribution stays attributable.",
        "samples": [_contract.SAMPLE_FUND_MATCHED]},
}


# --------------------------------------------------------------------------- #
# Fitting
# --------------------------------------------------------------------------- #
def training_filter(training_universe: str, membership=None):
    """Row mask for ONE training universe. The judge is never affected by it.

    ``TRAIN_S_AND_P_500_ONLY`` fits only on names that were index members on the
    fitting date. ``TRAIN_RUSSELL1000_PIT`` fits on the broad point-in-time
    cross-section the frozen panel carries. Which one a candidate used is bound
    into its specification hash, so "train broad, invest narrow" is a hypothesis
    the campaign TESTS rather than an accident of which panel was loaded - and
    neither choice widens what the judge lets the candidate OWN.
    """
    if training_universe == _contract.TRAIN_BROAD_PIT or membership is None:
        return lambda date, syms: np.ones(len(syms), dtype=bool)
    if training_universe != _contract.TRAIN_INVESTMENT_ONLY:
        raise ValueError("unknown training universe %r" % (training_universe,))

    def _mask(date, syms):
        try:
            return membership.eligible_columns(date, syms)
        except Exception:
            return np.zeros(len(syms), dtype=bool)
    return _mask


def _pool(snap, sample, sections, indices, feats, horizon, ufilter) -> tuple:
    Xs, ys = [], []
    for li in indices:
        k = sections[li]
        X, y, _adv, syms = snap.block(sample, k, feats, horizon)
        ok = (np.isfinite(y) & np.isfinite(X).all(axis=1)
              & ufilter(snap.dates[k], syms))
        if int(ok.sum()) >= _contract.MIN_CROSS_SECTION:
            Xs.append(X[ok])
            ys.append(y[ok])
    if not Xs:
        raise ValueError("empty training pool")
    return np.vstack(Xs), np.concatenate(ys)


def fit_family(family: str, params: dict, *, snap, sample, sections,
               train_indices: list, feats, horizon: int, seed: int,
               ufilter=None) -> dict:
    """Fit ONE family on ONE training pool. Pure in (data, params, seed)."""
    ufilter = ufilter or (lambda date, syms: np.ones(len(syms), dtype=bool))

    if family == "fama_macbeth":
        blocks = []
        for li in train_indices:
            k = sections[li]
            X, y, _a, syms = snap.block(sample, k, feats, horizon)
            ok = (np.isfinite(y) & np.isfinite(X).all(axis=1)
                  & ufilter(snap.dates[k], syms))
            if int(ok.sum()) >= _contract.MIN_CROSS_SECTION:
                blocks.append((X[ok], y[ok]))
        return _L.fit_fama_macbeth(blocks, shrink=float(params.get("shrink", 0.0)))

    if family == "direct_portfolio":
        # TRACK B. The block carries SECURITY IDENTITY, so the learner's turnover
        # term is computed over the union of symbols rather than by array
        # position - the defect that superseded Campaign v2.
        blocks = []
        for li in train_indices:
            k = sections[li]
            X, _y, _a, syms = snap.block(sample, k, feats, horizon)
            raw, bench = snap.holding_returns(sample, k, _judge.HOLD_SESSIONS)
            ok = (np.isfinite(raw) & np.isfinite(X).all(axis=1)
                  & ufilter(snap.dates[k], syms))
            if int(ok.sum()) >= _contract.MIN_CROSS_SECTION:
                names = np.array([str(snap.symbols[int(j)]) for j in syms[ok]])
                blocks.append((X[ok], raw[ok] - bench, names))
        pol = _judge.policy()
        return _L.fit_direct_portfolio(
            blocks, n_features=len(feats), gamma=float(params["gamma"]),
            cost_rate=float(pol["cost_rate_per_side"]),
            temperature=float(params["temperature"]),
            epochs=int(params.get("epochs", 150)), seed=seed)

    X, y = _pool(snap, sample, sections, train_indices, feats, horizon, ufilter)
    if family == "ridge":
        return _L.fit_ridge(X, y, alpha=float(params["alpha"]))
    if family == "elastic_net":
        return _L.fit_elastic_net(X, y, alpha=float(params["alpha"]),
                                  l1_ratio=float(params["l1_ratio"]))
    if family == "huber":
        return _L.fit_huber(X, y, delta=float(params["delta"]),
                            alpha=float(params["alpha"]))
    if family == "dim_reduction":
        return _L.fit_dimension_reduction(X, y,
                                          n_components=int(params["n_components"]),
                                          variant=str(params["variant"]))
    if family == "gbrt":
        return _L.fit_gbrt(X, y, seed=seed, **{k: v for k, v in params.items()})
    if family == "random_forest":
        return _L.fit_random_forest(X, y, seed=seed, **params)
    if family == "extra_trees":
        return _L.fit_extra_trees(X, y, seed=seed, **params)
    if family == "neural_net":
        return _L.fit_neural_net(X, y, hidden=params["hidden"],
                                 epochs=int(params.get("epochs", 40)),
                                 weight_decay=float(params["weight_decay"]),
                                 seed=seed)
    if family == "quantile":
        return _L.fit_quantile(X, y, tau=params["tau"],
                               alpha=float(params["alpha"]), seed=seed)
    raise ValueError("unknown family %r" % (family,))


def walk_forward_predictor(family: str, params: dict, *, snap, sample, sections,
                           feats, horizon: int, seed: int, train_cap: int,
                           embargo: int, warmup: list, ufilter=None) -> Callable:
    """An expanding-window refitting predictor, capped so lockbox never trains.

    ``train_cap`` is the highest section index the model may EVER train on - the
    last validation date. Every fit additionally ends ``embargo`` dates before
    the date being scored, so a label that is still resolving cannot appear in
    training.

    ``ufilter`` selects the TRAINING universe. It restricts what the model LEARNS
    from and has no effect whatsoever on what the judge lets it OWN.
    """
    cache: dict = {}

    def _predict(k, X, adv, syms):
        li = _index_of(sections, k)
        upper = min(int(li) - int(embargo), int(train_cap))
        bucket = upper - (upper % REFIT_EVERY)
        if bucket not in cache:
            train = [i for i in warmup if i <= bucket]
            # NO FALLBACK. A shorter-than-required window means no legitimate
            # model exists at this date; substituting a window that reaches past
            # it would be a look-ahead leak wearing a warm-up's clothes.
            if len(train) < MIN_TRAIN_SECTIONS:
                cache[bucket] = None
            else:
                cache[bucket] = fit_family(
                    family, params, snap=snap, sample=sample, sections=sections,
                    train_indices=train, feats=feats, horizon=horizon, seed=seed,
                    ufilter=ufilter)
        spec = cache[bucket]
        if spec is None:
            return np.full(X.shape[0], np.nan, dtype=np.float64)
        return _L.predict(spec, X, feats)

    _predict.cache = cache
    return _predict


def _index_of(sections: list, k: int) -> int:
    lo, hi = 0, len(sections) - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        if sections[mid] == k:
            return mid
        if sections[mid] < k:
            lo = mid + 1
        else:
            hi = mid - 1
    return lo


def benchmark_predictor(weights: dict, feats) -> Callable:
    spec = _L.rank_blend_spec(weights)

    def _predict(k, X, adv, syms):
        return _L.predict(spec, X, feats)
    return _predict


# --------------------------------------------------------------------------- #
# Candidate identity
# --------------------------------------------------------------------------- #
def spec_hash(*, phase: str, family: str, params: dict, sample: str,
              horizon: int, feats, snapshot_hash: str, partition_hash: str,
              seed: int, training_universe: str, universe_hash: str,
              benchmark_hash: str, extra: Optional[dict] = None) -> str:
    """The campaign's idempotency key.

    It binds the candidate AND the exact evidence semantics it will be measured
    under, so the same model against a different snapshot, partition, investment
    universe, benchmark set or judge is a DIFFERENT candidate rather than a
    silent overwrite. That is also what makes Campaign v2's results structurally
    unable to enter a v3 leaderboard: every v2 result was produced under a
    different judge behaviour hash and without a universe or benchmark hash at
    all, so no v2 row can ever collide with a v3 key.
    """
    return r31.sha({
        "phase": phase, "family": family, "params": params, "sample": sample,
        "horizon": int(horizon), "features": list(feats),
        "snapshot_hash": snapshot_hash, "partition_hash": partition_hash,
        "seed": int(seed), "refit_every": REFIT_EVERY,
        # CORRECTION 1: which universe the candidate LEARNED from is part of its
        # identity, so "train broad, invest narrow" occupies its own slot in the
        # multiple-testing denominator instead of hiding inside another spec.
        "training_universe": str(training_universe),
        "investment_universe": str(universe_hash),
        # CORRECTION 4: the benchmark set is part of what a score MEANS.
        "benchmarks": str(benchmark_hash),
        "track": FAMILY_TRACK.get(family, _contract.TRACK_A),
        # The judge's BEHAVIOUR, not merely its schema name. A change to the cost
        # arithmetic, the rebalance cadence or the evaluation universe changes
        # what every score means, and must invalidate cached candidates rather
        # than mix two judges' results in one leaderboard.
        "judge": _judge.behaviour_hash(), "extra": extra or {},
    })


def known_method_registry(*, campaign_id: str = _contract.CAMPAIGN_ID) -> dict:
    body = {
        "contract": KNOWN_SCHEMA,
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "families": {k: v for k, v in sorted(FAMILY_SPECS.items())},
        "family_count": len(FAMILY_SPECS),
        "family_budget": _contract.MAX_KNOWN_METHOD_FAMILIES,
        "grid_configs_per_family": {k: v["n_configs"]
                                    for k, v in sorted(FAMILY_SPECS.items())},
        "grid_total": sum(v["n_configs"] for v in FAMILY_SPECS.values()),
        "config_budget": _contract.MAX_KNOWN_METHOD_CONFIGS,
        "executed_grid_policy": _contract.EXECUTED_GRID_POLICY,
        "family_track": dict(sorted(FAMILY_TRACK.items())),
        "track_a_families": sorted(f for f, t in FAMILY_TRACK.items()
                                   if t == _contract.TRACK_A),
        "track_b_families": sorted(f for f, t in FAMILY_TRACK.items()
                                   if t == _contract.TRACK_B),
        "training_universes": list(_contract.TRAINING_UNIVERSES),
        "evaluation_universe": _contract.EVALUATION_UNIVERSE,
        "benchmarks": BENCHMARKS,
        "benchmarks_exempt_from_candidate_budget": True,
        "refit_every_decision_dates": REFIT_EVERY,
        "training_never_includes_lockbox": True,
        "calibration_never_includes_lockbox": True,
        "seed": _contract.SEEDS["learner"],
    }
    body["known_method_registry_hash"] = r31.sha(body)
    body.update(r31.safety_block())
    return body
