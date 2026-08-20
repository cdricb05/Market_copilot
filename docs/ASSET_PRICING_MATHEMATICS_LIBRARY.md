# Asset-Pricing Mathematics Library

> The Release 31 method registry: which published methods were screened, which
> were reproduced against the owned point-in-time data, and — equally important —
> which were **not**, and why.
>
> Owner: `alpha_agent/r31/methods.py` (registry + families) and
> `alpha_agent/r31/learners.py` (the mathematics). Artifact:
> `literature_method_registry.json`.
>
> Read with [MATHEMATICAL_ALPHA_FRONTIER.md](MATHEMATICAL_ALPHA_FRONTIER.md) ·
> [RESEARCH_CAMPAIGN_CONTRACT.md](RESEARCH_CAMPAIGN_CONTRACT.md).

## The distinction this document exists to make permanent

**KNOWN-METHOD REPRODUCTION** and **BOUNDED NOVEL DISCOVERY** are different
activities with different failure modes, and a campaign that blurs them will
report the second as if it had the credibility of the first.

| | Known-method reproduction | Bounded novel discovery |
|---|---|---|
| question | does a published method already solve more of this than we do? | is there structure the published methods miss? |
| provenance | a citation, a stated objective, a published validation design | our own frozen grammar |
| prior | someone else's out-of-sample evidence exists | none |
| budget ceiling | 12 families, 240 configurations | 6 families, 2 campaigns, 300 candidates, depth 3 |
| how it ends | the grid is exhausted | two consecutive null campaigns → **exhaustion** |
| credibility | inherits the literature's | earns it or does not |

Both face the **same frozen judge**, the same investment universe, the same
canonical cost, covariance and constraint owners, and the same evidence
partition. That is what makes the comparison a comparison of methods rather than
of evaluation conventions.

### Ceilings and the executed grid

The budgets above are **ceilings**, and Campaign v3 executes far fewer: 31 known
configurations across all twelve families, one training-universe variant per
family, and 30 novel candidates per campaign. The v3 judge allocates capital
through the canonical optimiser at every decision date, so a candidate costs
minutes to judge rather than milliseconds, and the contract's own rule already
forbids executing configurations to consume a budget.

Sparsity was taken in the *density* of each grid, never in its *coverage*: all
twelve known families and all six novel families are retained, penalties stay
log-spaced across the same range, and no family lost a configuration that
expressed materially different structure. Three neighbouring ridge penalties test
one hypothesis, not three. A smaller pre-registered search also carries a smaller
multiple-testing denominator, so each survivor carries more evidence.

## Why the mathematics is implemented in numpy

The project declares **no modelling dependency**. scipy, scikit-learn,
statsmodels, LightGBM, XGBoost, PyTorch and cvxpy are all absent from the venv,
and the architecture audit already treats `scipy` as kernel impurity. The whole
research lane from Stage 23 through Release 30 is numpy/pandas only.

Three reasons this is the right constraint rather than an inconvenience:

1. **Environment stability.** Installing a framework to reproduce a paper changes
   the environment the operational system runs beside.
2. **Determinism.** Every learner is a pure function of `(X, y, params, seed)`.
   The same triple produces byte-identical coefficients, which is what makes a
   specification hash a real idempotency key. A library that changes its RNG or
   its tie-breaking between minor versions would silently break that.
3. **Auditability.** A reviewer can read the objective being optimised. For a
   campaign whose entire claim is that its measurements are defensible, that
   matters more than convenience.

Absent libraries are **recorded in the contract**, because "no gradient-boosting
library was installed" is a material fact about which methods were reproducible.

## The twelve implemented families

| Family | Primary source | What it contributes |
|---|---|---|
| `ridge` | Hoerl & Kennard 1970; Gu, Kelly & Xiu 2020 | dense shrinkage over correlated characteristics |
| `elastic_net` | Zou & Hastie 2005 | sparse selection among correlated predictors |
| `dim_reduction` | Stock & Watson (PCR); Wold (PLS) | collapse the characteristic space before fitting |
| `fama_macbeth` | Fama & MacBeth 1973 | per-period cross-sectional slopes, averaged |
| `huber` | Huber 1964; Gu, Kelly & Xiu 2020 | bounded influence against fat return tails |
| `gbrt` | Friedman 2001 | sequential bias reduction; nonlinear interactions |
| `random_forest` | Breiman 2001 | **bagging** as the regulariser, optimised splits |
| `extra_trees` | Geurts et al. 2006 | **randomised thresholds** as the regulariser |
| `neural_net` | Gu, Kelly & Xiu 2020 (NN1–NN3) | shallow nonlinearity at a scale the sample supports |
| `quantile` | Koenker & Bassett 1978 | **distributional** — conditional quantiles, not a mean |
| `combination` | Bates & Granger 1969; Rapach, Strauss & Zhou 2010 | forecast combination as strong shrinkage |
| `direct_portfolio` | Jensen, Kelly, Malamud & Pedersen 2026; DeMiguel et al. 2020 | **Track B** — learn the WEIGHTS under a net-of-cost objective |

### What a Track-A family must clear before it allocates capital

Eleven of the twelve families emit a **score**, not an expected return. The
canonical allocator prices return against variance and cost, so a score reaches
it only through the pre-registered monotonic calibration in
`alpha_agent/r31/calibration.py`, fitted on DISCOVERY evidence only. A family
whose score has no defensible monotone relationship to forward returns is
**rejected as a capital allocator** — recorded, kept in the multiple-testing
denominator, and never handed a manufactured μ. Good rank IC without a defensible
economic calibration is predictive evidence, not an allocation rule.

### Both Track-B families allocate cash and price their own turnover

`direct_portfolio` (linear) and the novel `novel_direct_decision` (one hidden
layer) both allocate across the names **and a cash unit** — so neither is forced
to be fully invested — and both compute turnover over the **union of security
identities**. Their training blocks are `(X_t, r_t, symbols_t)`; a two-element
block raises. Two Track-B families trained under different economics would not be
comparable with each other, let alone with Track A.

`random_forest` and `extra_trees` are kept as separate families deliberately:
they differ in *what does the regularising*, not in a parameter, and the campaign
should be able to say which mechanism worked here.

## Methods extracted but NOT implemented, with reasons

Recording these matters: *"we did not run it"* and *"it cannot be run on this
data"* are different facts, and only the second is a limitation of the evidence.

| Method | Reason |
|---|---|
| **Freyberger, Neuhierl & Weber 2020** — adaptive group-lasso splines | a 13th family would breach the frozen family budget. Its central finding — nonlinearity matters — is already carried by the tree families, and the campaign's features are per-date ranks, which is the paper's own monotone transform. |
| **Kelly, Pruitt & Su 2019** — Instrumented PCA | answers a **risk-decomposition** question and yields a factor model, not a per-name allocation score. Consuming it would require a second risk owner beside the canonical covariance owner. |
| **Chen, Pelger & Zhu 2023** — no-arbitrage GAN/SDF | `RESOURCE_INFEASIBLE`. Adversarial training of two networks over a macro state panel needs a framework this environment deliberately does not install, and the owned macro history is far shorter than the equity panel. |
| **Kozak, Nagel & Santosh 2020** — SDF shrinkage | *partially* reproduced. The shrink-toward-leading-PCs idea is carried by `ridge` and `dim_reduction`; the full tangency formulation targets characteristic **portfolios** rather than a per-name score and would need a second optimiser, which the campaign forbids. |
| **Bryzgalova, Pelger & Zhu 2025** — Forest through the Trees | builds **test assets** for pricing-model evaluation. It does not answer the campaign's question. |

## The literature exhaustion contract

> Stop when **two consecutive** query/reference expansions yield no new
> materially distinct **admissible** method family.

Five expansions were run. Expansions 1–3 added instrumented factor models, SDF
shrinkage, adversarial deep learning and quantile machine learning. Expansions
4–5 returned only methods already registered. State:
`LITERATURE_EXHAUSTED_UNDER_CONTRACT`.

Without a rule like this, "read the literature" has no terminating condition, and
the campaign's cost becomes unbounded for a shrinking return.

## Inference machinery (not a candidate family)

| Method | Source | Where |
|---|---|---|
| Benjamini–Hochberg FDR | Benjamini & Hochberg 1995 | `multiple_testing.benjamini_hochberg` |
| Superior Predictive Ability | Hansen 2005; White 2000 | `multiple_testing.superior_predictive_ability` |
| Stationary bootstrap | Politis & Romano 1994 | `multiple_testing.stationary_bootstrap_indices` |
| Newey–West HAC | Newey & West 1987 | `judge.newey_west_t` |

The normal CDF is `math.erf`; the bootstrap is `numpy.random.default_rng` seeded
from the campaign contract. Every p-value in the campaign is reproducible.

## Extending the library

A new family is added to `FAMILY_SPECS` with a bounded grid **and** a
`LITERATURE` entry recording its citation, objective, validation design, and what
could and could not be reproduced. Adding a 13th family requires a new campaign
id, because the family budget is bound into the contract hash.
