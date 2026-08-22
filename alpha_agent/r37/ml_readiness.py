"""alpha_agent.r37.ml_readiness - Track C: model-family readiness and the ML data contract.

SECONDARY to Tracks A and B, and deliberately bounded: this module declares what
each model family NEEDS and computes whether this machine can run it. It trains
nothing. ``contract.ML_TRAINING_CAMPAIGN_IN_SCOPE`` is False and there is no code
path here that fits a parameter.

Two products:

* **ADVANCED_ML_READINESS_MATRIX** - one row per model family, with its input
  requirements, its sample-size floor, its hardware demand, its point-in-time and
  survivorship failure modes, the Paper Trader questions it is actually suited
  to, and - the column that connects Track C to Track A - **which of the
  recommended datasets makes it worth more**.
* **ADVANCED_ML_DATA_CONTRACT** - the canonical research input/output shape a
  future model consumes. It COMPOSES existing owners rather than declaring a
  second market-data authority, and its output vocabulary deliberately extends
  past a point return, because every release since 33 has shown that a point
  forecast is the least useful thing a model can produce.

The judgement this module refuses to make is "newer is better". Release 34
measured a genuine forecast at t = 3.39 that did not convert, and Release 36
measured one at t = 7.97 that paid +1.4 %/yr over passive. Neither failure was a
model-capacity failure, so no row here claims that a larger model would have
fixed it.
"""
from __future__ import annotations

from typing import Optional

from .. import r37
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r37.ml_readiness"
MATRIX_SCHEMA = "r37_advanced_ml_readiness_matrix/1"
MATRIX_ARTIFACT = "advanced_ml_readiness_matrix.json"
DATA_CONTRACT_SCHEMA = "r37_advanced_ml_data_contract/1"
DATA_CONTRACT_ARTIFACT = "advanced_ml_data_contract.json"

C = _contract

TRAINS_A_MODEL = False
SELECTS_A_MODEL = False
NEWER_IMPLIES_BETTER = False

FAMILY_TABULAR = "CLASSICAL_HIGH_END_TABULAR"
FAMILY_TABULAR_FOUNDATION = "TABULAR_FOUNDATION"
FAMILY_TS_FOUNDATION = "TIME_SERIES_FOUNDATION"
FAMILY_DEEP_SEQUENCE = "DEEP_SEQUENCE"
FAMILY_REGIME_MOE = "REGIME_AND_MIXTURE_OF_EXPERTS"
FAMILY_PROBABILISTIC = "PROBABILISTIC_DISTRIBUTIONAL"
FAMILY_CROSS_MARKET = "CROSS_MARKET_RELATIONAL"
FAMILIES = (FAMILY_TABULAR, FAMILY_TABULAR_FOUNDATION, FAMILY_TS_FOUNDATION,
            FAMILY_DEEP_SEQUENCE, FAMILY_REGIME_MOE, FAMILY_PROBABILISTIC,
            FAMILY_CROSS_MARKET)

COST_TRIVIAL = "MINUTES_ON_CPU"
COST_LOW = "TENS_OF_MINUTES_ON_CPU"
COST_MEDIUM = "HOURS_ON_CPU_OR_MINUTES_ON_A_SMALL_GPU"
COST_HIGH = "HOURS_TO_DAYS_ON_A_LARGE_GPU"
COST_CLASSES = (COST_TRIVIAL, COST_LOW, COST_MEDIUM, COST_HIGH)

# --------------------------------------------------------------------------- #
# Readiness classes - hardware capability is NOT the same as being able to run
# --------------------------------------------------------------------------- #
#: The distinction this vocabulary exists to enforce: a family whose hardware
#: minima this machine meets is not a family that "runs here today" if the
#: library it needs is not installed. Release 37 reported eight of thirteen as
#: locally feasible, which was true of the HARDWARE and false as a statement
#: about what could be executed - the environment holds numpy and pandas and
#: nothing else. The two facts are now carried in separate fields.
READY_INSTALLED = "CURRENTLY_INSTALLED_AND_RUNNABLE"
READY_AFTER_INSTALL = "HARDWARE_FEASIBLE_AFTER_SOFTWARE_INSTALL"
READY_IMPRACTICAL = "LOCALLY_POSSIBLE_BUT_IMPRACTICAL"
READY_EXTERNAL_GPU = "EXTERNAL_GPU_RECOMMENDED"
READY_NOT_FEASIBLE = "NOT_CURRENTLY_FEASIBLE"
READINESS_CLASSES = (READY_INSTALLED, READY_AFTER_INSTALL, READY_IMPRACTICAL,
                     READY_EXTERNAL_GPU, READY_NOT_FEASIBLE)

READINESS_MEANING = {
    READY_INSTALLED:
        "the hardware minima are met AND every required library is already "
        "installed; this family can be executed today with no install",
    READY_AFTER_INSTALL:
        "the hardware minima are met but a required library is absent; the "
        "install is free and this release does not perform it",
    READY_IMPRACTICAL:
        "the declared minima are technically met, but with no meaningful "
        "headroom on a 4 GB card without tensor cores, so a real experiment "
        "would be slow enough to distort what gets tried",
    READY_EXTERNAL_GPU:
        "a GPU is required with more VRAM than this machine has; rented "
        "capacity would close it, and this release neither buys nor recommends "
        "buying any",
    READY_NOT_FEASIBLE:
        "a hardware minimum cannot be met on this machine at all",
}

#: VRAM headroom below which a GPU family is IMPRACTICAL rather than runnable.
#: A card whose entire memory equals the model's minimum has nothing left for
#: the display, the batch or the optimiser state.
MIN_VRAM_HEADROOM_GB = 1.0


def _m(**kw) -> dict:
    return dict(kw)


#: One row per model family. Every field is a REQUIREMENT of the method; the
#: feasibility verdict is computed against the measured machine, never typed.
MODELS = (
    _m(model="REGULARISED_LINEAR",
       family=FAMILY_TABULAR,
       description="ridge, elastic net and rank-based linear baselines",
       input_shape="CROSS_SECTIONAL_PANEL",
       min_useful_observations=500,
       min_cross_section=10,
       needs_gpu=False, min_vram_gb=0.0, min_ram_gb=2.0,
       training_cost_class=COST_TRIVIAL,
       required_libraries=("numpy",),
       preferred_libraries=("scikit-learn", "statsmodels"),
       zero_shot=False, fine_tunable=False,
       pit_risks=("a full-sample standardisation leaks the future into every "
                  "coefficient; Release 36 solved this with trailing "
                  "expanding-window normalisation and the same rule applies"),
       survivorship_risks=("a panel assembled from current members inherits "
                           "the survivor's return"),
       use_cases=("the control every other row is measured against; a "
                  "cross-sectional rank model over the futures curve"),
       improved_by=("norgate_futures_package",),
       why_improved=("a 100-market curve panel turns a 47-fund cross-section "
                     "into one with real carry, roll and term-structure "
                     "features")),
    _m(model="GRADIENT_BOOSTED_TREES",
       family=FAMILY_TABULAR,
       description="XGBoost, LightGBM and CatBoost on a tabular panel",
       input_shape="CROSS_SECTIONAL_PANEL",
       min_useful_observations=5000,
       min_cross_section=20,
       needs_gpu=False, min_vram_gb=0.0, min_ram_gb=8.0,
       training_cost_class=COST_LOW,
       required_libraries=("xgboost",),
       preferred_libraries=("xgboost", "lightgbm", "catboost"),
       zero_shot=False, fine_tunable=False,
       pit_risks=("target encoding and early stopping on a validation fold "
                  "that overlaps the training window both leak; the embargo in "
                  "the data contract is what prevents it"),
       survivorship_risks=("trees exploit a survivor-only universe more "
                           "efficiently than a linear model does, so the bias "
                           "shows up as a BETTER backtest"),
       use_cases=("non-linear interaction between carry, trend and "
                  "positioning across a futures cross-section"),
       improved_by=("norgate_futures_package", "zacks_consensus_history"),
       why_improved=("interaction effects need width; 100 markets x 7 curve "
                     "families is the first genuinely wide panel this estate "
                     "would own")),
    _m(model="EXTRA_TREES_AND_RANDOM_FORESTS",
       family=FAMILY_TABULAR,
       description="bagged tree ensembles as a variance-reduction benchmark",
       input_shape="CROSS_SECTIONAL_PANEL",
       min_useful_observations=5000,
       min_cross_section=20,
       needs_gpu=False, min_vram_gb=0.0, min_ram_gb=8.0,
       training_cost_class=COST_LOW,
       required_libraries=("scikit-learn",),
       preferred_libraries=("scikit-learn",),
       zero_shot=False, fine_tunable=False,
       pit_risks=("out-of-bag error is not an out-of-sample estimate on a time "
                  "series and must never be reported as one"),
       survivorship_risks=("as for boosted trees",),
       use_cases=("ensemble disagreement as an uncertainty estimate, which the "
                  "output contract can actually represent"),
       improved_by=("norgate_futures_package",),
       why_improved="wider panels make ensemble disagreement informative"),
    _m(model="TABPFN_CLASS_TABULAR_FOUNDATION",
       family=FAMILY_TABULAR_FOUNDATION,
       description="pre-trained transformers that classify or regress a small "
                   "tabular task in a single forward pass",
       input_shape="SMALL_TABULAR_TASK",
       min_useful_observations=100,
       min_cross_section=5,
       needs_gpu=True, min_vram_gb=4.0, min_ram_gb=16.0,
       training_cost_class=COST_MEDIUM,
       required_libraries=("tabpfn", "torch"),
       preferred_libraries=("tabpfn", "torch"),
       zero_shot=True, fine_tunable=True,
       pit_risks=("the model was pre-trained on synthetic priors, so it "
                  "carries no market look-ahead - but the CONTEXT rows handed "
                  "to it at inference are a training set and must respect the "
                  "same embargo"),
       survivorship_risks=("a context window drawn from survivors is the same "
                           "bias in a new place"),
       use_cases=("a regime classifier over a few hundred monthly "
                  "observations, which is exactly the sample size where "
                  "gradient boosting overfits"),
       improved_by=("norgate_futures_package",),
       why_improved=("small-sample strength is most valuable per market, and "
                     "the purchase multiplies the number of markets by 20")),
    _m(model="CHRONOS_CLASS_TIME_SERIES_FOUNDATION",
       family=FAMILY_TS_FOUNDATION,
       description="tokenised univariate series forecasters usable zero-shot",
       input_shape="UNIVARIATE_SEQUENCE",
       min_useful_observations=200,
       min_cross_section=1,
       needs_gpu=True, min_vram_gb=8.0, min_ram_gb=16.0,
       training_cost_class=COST_MEDIUM,
       required_libraries=("chronos-forecasting", "torch"),
       preferred_libraries=("chronos-forecasting", "torch", "transformers"),
       zero_shot=True, fine_tunable=True,
       pit_risks=("PUBLICATION LEAKAGE IS THE REAL RISK: a foundation model "
                  "pre-trained on public data may have seen the very series "
                  "being forecast, so a zero-shot result on a public price "
                  "history is NOT out-of-sample and may not be reported as "
                  "such"),
       survivorship_risks=("univariate forecasting hides survivorship rather "
                           "than removing it"),
       use_cases=("volatility and turnover forecasting, where the target is "
                  "genuinely persistent, rather than return forecasting where "
                  "five releases have found no convertible edge"),
       improved_by=("norgate_futures_package",),
       why_improved=("45 years of settlements per market is the depth these "
                     "models need to be fine-tuned rather than only prompted")),
    _m(model="TIMESFM_AND_MOIRAI_CLASS",
       family=FAMILY_TS_FOUNDATION,
       description="decoder-only and masked-encoder foundation forecasters "
                   "with native multivariate and covariate support",
       input_shape="MULTIVARIATE_SEQUENCE_WITH_COVARIATES",
       min_useful_observations=500,
       min_cross_section=2,
       needs_gpu=True, min_vram_gb=12.0, min_ram_gb=32.0,
       training_cost_class=COST_HIGH,
       required_libraries=("torch", "transformers"),
       preferred_libraries=("torch", "transformers", "gluonts"),
       zero_shot=True, fine_tunable=True,
       pit_risks=("the same pre-training contamination, plus a covariate "
                  "alignment bug is invisible in a zero-shot API"),
       survivorship_risks=("a fixed instrument list across the whole window "
                           "silently drops entries and exits"),
       use_cases=("joint forecasting of a whole futures curve, where the "
                  "covariates are the other contracts on the same day"),
       improved_by=("norgate_futures_package",),
       why_improved=("a curve is inherently multivariate and the estate owns "
                     "exactly one curve today")),
    _m(model="TEMPORAL_FUSION_TRANSFORMER",
       family=FAMILY_DEEP_SEQUENCE,
       description="attention over known-future, observed and static "
                   "covariates with built-in quantile output",
       input_shape="MULTIVARIATE_SEQUENCE_WITH_COVARIATES",
       min_useful_observations=20000,
       min_cross_section=20,
       needs_gpu=True, min_vram_gb=8.0, min_ram_gb=32.0,
       training_cost_class=COST_HIGH,
       required_libraries=("torch",),
       preferred_libraries=("torch", "darts"),
       zero_shot=False, fine_tunable=True,
       pit_risks=("the KNOWN-FUTURE covariate channel is a leakage trap by "
                  "design: only a calendar fact - an expiry date, a notice "
                  "day, a scheduled release - may enter it, never a price"),
       survivorship_risks=("static covariates encoded from today's metadata "
                           "leak the present into the past"),
       use_cases=("horizon-aware curve forecasting where expiry and first "
                  "notice day are genuinely known in advance"),
       improved_by=("norgate_futures_package",),
       why_improved=("first notice and last trading dates are real "
                     "known-future covariates, and the purchase is the only "
                     "candidate that supplies them as fields")),
    _m(model="TEMPORAL_CONVOLUTION_AND_LSTM_BENCHMARK",
       family=FAMILY_DEEP_SEQUENCE,
       description="dilated causal convolutions, and an LSTM kept only as a "
                   "reference point",
       input_shape="UNIVARIATE_OR_MULTIVARIATE_SEQUENCE",
       min_useful_observations=10000,
       min_cross_section=1,
       needs_gpu=True, min_vram_gb=4.0, min_ram_gb=16.0,
       training_cost_class=COST_MEDIUM,
       required_libraries=("torch",),
       preferred_libraries=("torch",),
       zero_shot=False, fine_tunable=True,
       pit_risks=("a non-causal padding or a bidirectional layer reads the "
                  "future; the causality of every convolution must be a test, "
                  "not a comment"),
       survivorship_risks=("sequence models need contiguous history and the "
                           "temptation is to drop instruments that end"),
       use_cases=("a benchmark that says whether attention is buying anything"),
       improved_by=("norgate_futures_package",),
       why_improved="more instruments, more sequences, same architecture"),
    _m(model="STATE_SPACE_SEQUENCE_MODELS",
       family=FAMILY_DEEP_SEQUENCE,
       description="selective state-space and Mamba-style long-context models",
       input_shape="LONG_SEQUENCE",
       min_useful_observations=50000,
       min_cross_section=1,
       needs_gpu=True, min_vram_gb=16.0, min_ram_gb=32.0,
       training_cost_class=COST_HIGH,
       required_libraries=("torch",),
       preferred_libraries=("torch",),
       zero_shot=False, fine_tunable=True,
       pit_risks=("long context makes an embargo violation span years rather "
                  "than months"),
       survivorship_risks=("long context rewards the longest-lived "
                           "instruments, which are the survivors"),
       use_cases=("daily sequences of 45-year commodity curves, the only "
                  "place in the estate where the context is genuinely long"),
       improved_by=("norgate_futures_package", "portara_cqg_futures"),
       why_improved=("length is the whole point of this family and the estate "
                     "currently has one long native series")),
    _m(model="MIXTURE_OF_EXPERTS_AND_REGIME_GATING",
       family=FAMILY_REGIME_MOE,
       description="a gate that routes to specialised experts by regime, and "
                   "aggregation over their outputs",
       input_shape="CROSS_SECTIONAL_PANEL_PLUS_REGIME_STATE",
       min_useful_observations=10000,
       min_cross_section=20,
       needs_gpu=False, min_vram_gb=0.0, min_ram_gb=16.0,
       training_cost_class=COST_MEDIUM,
       required_libraries=("scikit-learn",),
       preferred_libraries=("scikit-learn", "numpy"),
       zero_shot=False, fine_tunable=True,
       pit_risks=("a regime label assigned with hindsight is the single most "
                  "common way this family manufactures a result; Phase 10-O "
                  "already returned REJECT_REGIME_OVERFIT on owned data"),
       survivorship_risks=("regimes are estimated on the instruments that "
                           "survived them"),
       use_cases=("routing between a carry expert and a trend expert, which "
                  "Release 36 tested by hand and could not separate"),
       improved_by=("norgate_futures_package",),
       why_improved=("regime research needs many markets to avoid fitting one "
                     "market's regimes; five energy curves is not enough")),
    _m(model="QUANTILE_AND_DISTRIBUTIONAL_REGRESSION",
       family=FAMILY_PROBABILISTIC,
       description="pinball-loss quantile models and full predictive "
                   "distributions",
       input_shape="CROSS_SECTIONAL_PANEL",
       min_useful_observations=5000,
       min_cross_section=20,
       needs_gpu=False, min_vram_gb=0.0, min_ram_gb=8.0,
       training_cost_class=COST_LOW,
       required_libraries=("numpy", "scipy"),
       preferred_libraries=("scikit-learn", "statsmodels", "lightgbm"),
       zero_shot=False, fine_tunable=False,
       pit_risks=("calibration measured in-sample is not calibration"),
       survivorship_risks=("the left tail is exactly what survivorship "
                           "removes, so a survivor panel produces a "
                           "confidently wrong downside quantile"),
       use_cases=("THE most valuable family for this estate: Release 30.1 "
                  "recorded CALIBRATION_BLOCKED and Release 29H recorded "
                  "expected_return NOT_CALIBRATED. An allocator needs a "
                  "distribution, and none of the five failed releases ever "
                  "produced one"),
       improved_by=("norgate_futures_package", "orats_options_history"),
       why_improved=("tail research needs instruments with real tails, and an "
                     "option surface would supply an implied distribution to "
                     "calibrate against")),
    _m(model="ENSEMBLE_DISAGREEMENT_UNCERTAINTY",
       family=FAMILY_PROBABILISTIC,
       description="disagreement across seeds, folds and model families as an "
                   "uncertainty signal in its own right",
       input_shape="ANY",
       min_useful_observations=5000,
       min_cross_section=10,
       needs_gpu=False, min_vram_gb=0.0, min_ram_gb=8.0,
       training_cost_class=COST_MEDIUM,
       required_libraries=("numpy", "scikit-learn"),
       preferred_libraries=("scikit-learn",),
       zero_shot=False, fine_tunable=False,
       pit_risks=("disagreement measured across folds that share data is not "
                  "disagreement"),
       survivorship_risks=("low",),
       use_cases=("sizing: a position scaled by model agreement is the "
                  "cheapest thing this estate has never tried"),
       improved_by=("norgate_futures_package",),
       why_improved="more markets, more independent disagreement estimates"),
    _m(model="MULTIVARIATE_AND_GRAPH_CROSS_MARKET",
       family=FAMILY_CROSS_MARKET,
       description="relational and graph models over an instrument network, "
                   "and explicit lead-lag representation",
       input_shape="INSTRUMENT_GRAPH_PLUS_SEQUENCE",
       min_useful_observations=50000,
       min_cross_section=30,
       needs_gpu=True, min_vram_gb=16.0, min_ram_gb=32.0,
       training_cost_class=COST_HIGH,
       required_libraries=("torch",),
       preferred_libraries=("torch",),
       zero_shot=False, fine_tunable=True,
       pit_risks=("an adjacency matrix estimated on the full sample is a "
                  "full-sample statistic and leaks; it must be trailing"),
       survivorship_risks=("a graph built from today's instrument list cannot "
                           "contain a node that delisted"),
       use_cases=("cross-market lead-lag between the curve, the rates curve "
                  "and the volatility term structure - the question Release 36 "
                  "could only ask through proxies"),
       improved_by=("norgate_futures_package",),
       why_improved=("a graph needs nodes; the estate has 5 native commodity "
                     "nodes today and would have ~100 after the purchase")),
)


def feasibility(model: dict, constraints: dict,
                libraries: Optional[dict] = None) -> dict:
    """Can THIS machine run THIS family, and can it run it TODAY? Both computed.

    Two questions that Release 37 answered as one. ``hardware_feasible`` says the
    measured machine meets the declared minima. ``readiness`` says what would
    actually happen if someone tried to run it now, which additionally depends on
    whether the required library is installed - and in this environment, almost
    none is. A family is only ever reported as runnable today when both hold.
    """
    vram = constraints.get("max_vram_gb") or 0.0
    ram = constraints.get("total_ram_gb") or 0.0
    gpu_present = bool(constraints.get("gpu_present"))
    needs_gpu = bool(model["needs_gpu"])
    vram_ok = (not needs_gpu) or (gpu_present and vram >= model["min_vram_gb"])
    ram_ok = ram >= model["min_ram_gb"] if ram else None
    if needs_gpu and not gpu_present:
        verdict = "NOT_FEASIBLE_NO_GPU"
    elif not vram_ok:
        verdict = "NOT_FEASIBLE_INSUFFICIENT_VRAM"
    elif ram_ok is False:
        verdict = "NOT_FEASIBLE_INSUFFICIENT_RAM"
    elif needs_gpu:
        verdict = "FEASIBLE_ON_THE_LOCAL_GPU"
    else:
        verdict = "FEASIBLE_ON_CPU"
    hardware_feasible = verdict.startswith("FEASIBLE")

    present = set((libraries or {}).get("present") or {})
    required = tuple(model.get("required_libraries") or ())
    missing = sorted(lib for lib in required if lib not in present)
    installed = sorted(lib for lib in required if lib in present)
    libraries_measured = libraries is not None
    headroom = (round(vram - model["min_vram_gb"], 2)
                if needs_gpu and vram else None)

    if not hardware_feasible:
        readiness = (READY_EXTERNAL_GPU
                     if verdict in ("NOT_FEASIBLE_NO_GPU",
                                    "NOT_FEASIBLE_INSUFFICIENT_VRAM")
                     else READY_NOT_FEASIBLE)
    elif needs_gpu and headroom is not None and headroom < MIN_VRAM_HEADROOM_GB:
        readiness = READY_IMPRACTICAL
    elif not libraries_measured or missing:
        readiness = READY_AFTER_INSTALL
    else:
        readiness = READY_INSTALLED

    return {
        "verdict": verdict,
        # Preserved field name and meaning: this has always been the HARDWARE
        # answer, and renaming it would have silently changed what callers read.
        "feasible_locally": hardware_feasible,
        "hardware_feasible": hardware_feasible,
        "readiness": readiness,
        "readiness_meaning": READINESS_MEANING[readiness],
        "runnable_today": readiness == READY_INSTALLED,
        "requires_gpu": needs_gpu,
        "min_vram_gb": model["min_vram_gb"],
        "measured_vram_gb": vram or None,
        "vram_headroom_gb": headroom,
        "min_ram_gb": model["min_ram_gb"],
        "measured_ram_gb": ram or None,
        "training_cost_class": model["training_cost_class"],
        "required_libraries": list(required),
        "required_libraries_present": installed,
        "required_libraries_missing": missing,
        "libraries_measured": libraries_measured,
        "remedy": (
            None if readiness == READY_INSTALLED else
            "install %s (free; this release installs nothing)" % ", ".join(missing)
            if readiness == READY_AFTER_INSTALL and missing else
            "a GPU with at least %.0f GB of VRAM, rented or owned; this "
            "release neither buys nor recommends buying one"
            % max(model["min_vram_gb"], MIN_VRAM_HEADROOM_GB
                  + model["min_vram_gb"])
            if readiness in (READY_EXTERNAL_GPU, READY_IMPRACTICAL) else
            "a machine that meets the declared minima"),
    }


def matrix(constraints: dict, libraries: Optional[dict] = None) -> list:
    """The readiness matrix: one row per family, feasibility computed."""
    rows = []
    for model in MODELS:
        row = dict(model)
        row["pit_risks"] = list(model["pit_risks"])
        row["survivorship_risks"] = list(model["survivorship_risks"])
        row["improved_by"] = list(model["improved_by"])
        row["required_libraries"] = list(model.get("required_libraries") or ())
        row["preferred_libraries"] = list(model.get("preferred_libraries") or ())
        row["feasibility"] = feasibility(model, constraints, libraries)
        rows.append(row)
    return rows


def summarise(rows: list) -> dict:
    """Counts per readiness class, with the hardware answer kept separate.

    ``feasible_count`` keeps its original meaning - how many families the
    HARDWARE supports - and ``currently_runnable_count`` is the number that
    could actually be executed today. Reporting only the first is what let
    Release 37 say "8 of 13 run here today" about a machine holding numpy and
    pandas.
    """
    feasible = [r["model"] for r in rows if r["feasibility"]["hardware_feasible"]]
    blocked = [r["model"] for r in rows
               if not r["feasibility"]["hardware_feasible"]]
    runnable = [r["model"] for r in rows if r["feasibility"]["runnable_today"]]
    by_readiness = {}
    for row in rows:
        by_readiness.setdefault(row["feasibility"]["readiness"], []).append(
            row["model"])
    by_family = {}
    for row in rows:
        by_family.setdefault(row["family"], []).append(row["model"])
    missing = sorted({lib for row in rows
                      for lib in row["feasibility"]["required_libraries_missing"]})
    return {
        "families": list(FAMILIES),
        "models_total": len(rows),
        "feasible_locally": sorted(feasible),
        "not_feasible_locally": sorted(blocked),
        "feasible_count": len(feasible),
        "hardware_feasible_count": len(feasible),
        "currently_runnable": sorted(runnable),
        "currently_runnable_count": len(runnable),
        "readiness_classes": list(READINESS_CLASSES),
        "by_readiness": {k: sorted(v) for k, v in sorted(by_readiness.items())},
        "readiness_counts": {k: len(by_readiness.get(k, []))
                             for k in READINESS_CLASSES},
        "missing_libraries": missing,
        "install_would_unlock": sorted(
            r["model"] for r in rows
            if r["feasibility"]["readiness"] == READY_AFTER_INSTALL),
        "external_gpu_would_unlock": sorted(
            r["model"] for r in rows
            if r["feasibility"]["readiness"] == READY_EXTERNAL_GPU),
        "by_family": {k: sorted(v) for k, v in sorted(by_family.items())},
        "improved_by_recommended_purchase": sorted(
            r["model"] for r in rows
            if "norgate_futures_package" in r["improved_by"]),
        "newer_implies_better": NEWER_IMPLIES_BETTER,
        "trains_a_model": TRAINS_A_MODEL,
        "installs_anything": False,
    }


# --------------------------------------------------------------------------- #
# The canonical research input / output contract
# --------------------------------------------------------------------------- #
#: The INPUT record a future advanced model consumes. Every field names the
#: owner that would supply it, so this contract composes existing authorities
#: instead of becoming a second market-data owner.
INPUT_FIELDS = {
    "instrument_id": "stable instrument key; alpha_agent.r36.native_markets "
                     "for native lanes, alpha_agent.r34.universe for funds",
    "instrument_kind": "SPOT | FUND | FORWARD | DATED_FUTURE | INDEX | RATE",
    "asset_class": "alpha_agent.r36.coverage.MARKETS",
    "market_key": "the Release-36 market this instrument implements",
    "contract_month": "dated instruments only; None otherwise",
    "expiry_date": "dated instruments only",
    "first_notice_date": "dated instruments only",
    "decision_timestamp": "the moment the position could be struck",
    "feature_observation_timestamp": "when each feature was OBSERVABLE, which "
                                     "is not when it was dated",
    "as_of_rule": "alpha_agent.r35.information.as_of_align - the ONE alignment "
                  "owner; a model may not implement its own",
    "price_sequence": "settlement or total-return close, trailing only",
    "return_sequence": "the realised return of holding the instrument",
    "curve_structure": "the other contracts on the same market on the same "
                       "day; None where no curve exists",
    "volume": "traded contracts or shares",
    "open_interest": "dated instruments only",
    "carry_features": "the observable cost or benefit of holding",
    "macro_features": "alpha_agent.r35.information families, publication-lagged",
    "event_features": "scheduled events only; a surprise is not known in "
                      "advance and may not enter a known-future channel",
    "missingness_mask": "explicit per field; a model may not infer missingness "
                        "from a zero",
    "cross_sectional_id": "the group a rank is taken within",
    "partition": "TRAIN | VALIDATION | TEST, assigned by time and never by row",
    "embargo_sessions": "the gap between partitions; must exceed the target "
                        "horizon or the test set is contaminated",
    "transaction_cost_bps": "alpha_agent.r36.contract.COST_BPS_PER_SIDE, "
                            "charged on TRADED NOTIONAL",
    "passive_control_return": "the volatility-matched passive hold of what is "
                              "traded - alpha_agent.r34.economics owns it",
    "survivorship_state": "LIVE | DELISTED | EXPIRED | ADMINISTERED, as known "
                          "at the decision timestamp",
    "point_in_time_state": "OBSERVED | REVISED | UNKNOWN for every field",
}

#: The OUTPUT a model may produce. Deliberately wider than a point return: five
#: releases have now shown that a point forecast is the least decision-relevant
#: quantity a model can emit, and that the allocator's binding gap is a
#: calibrated distribution rather than a better mean.
OUTPUT_FIELDS = {
    "expected_return": "point forecast; retained as a baseline, not a product",
    "expected_excess_return": "over the passive control, which is the only "
                              "quantity any release has ever been able to act "
                              "on",
    "probability_of_positive_excess": "a calibrated probability, not a "
                                      "normalised score",
    "quantiles": "at least 0.05, 0.25, 0.50, 0.75, 0.95",
    "tail_probability": "P(excess < -x) for a declared x",
    "expected_volatility": "the forecast this estate has never produced and "
                           "the allocator most needs",
    "cross_sectional_rank": "the quantity Releases 34 and 36 actually measured",
    "uncertainty": "the model's own dispersion",
    "model_disagreement": "dispersion ACROSS models, which is different and "
                          "more honest",
    "abstain": "an explicit refusal to forecast, which a NULL result makes "
               "legitimate",
}

#: Rules the contract enforces on any consumer.
CONTRACT_RULES = (
    "a feature enters at its OBSERVATION timestamp, never at its event date",
    "every statistic used to normalise a feature is trailing and shifted",
    "partitions are assigned by time with an embargo larger than the horizon",
    "an instrument with no observable return on a date carries no weight",
    "cost is charged on traded notional, not on gross exposure",
    "a forecast is judged against the passive hold of what it trades",
    "a model output is not a portfolio target and creates no authority",
    "a zero-shot result from a model pre-trained on public data is NOT "
    "out-of-sample and may not be reported as such",
)

COMPOSES_EXISTING_OWNERS = True
CREATES_A_SECOND_MARKET_DATA_OWNER = False


def data_contract() -> dict:
    return {
        "input_fields": dict(INPUT_FIELDS),
        "output_fields": dict(OUTPUT_FIELDS),
        "rules": list(CONTRACT_RULES),
        "composes_existing_owners": COMPOSES_EXISTING_OWNERS,
        "creates_a_second_market_data_owner":
            CREATES_A_SECOND_MARKET_DATA_OWNER,
        "output_is_more_than_a_point_return": True,
        "abstention_is_a_legitimate_output": True,
    }


def matrix_artifact(rows: list, *, campaign_id: str, created_at: str) -> dict:
    payload = {
        "campaign_id": campaign_id,
        "created_at": created_at,
        "calculation_owner": CALCULATION_OWNER,
        "families": list(FAMILIES),
        "cost_classes": list(COST_CLASSES),
        "readiness_classes": list(READINESS_CLASSES),
        "readiness_meaning": dict(READINESS_MEANING),
        "hardware_feasible_is_not_runnable_today": True,
        "min_vram_headroom_gb": MIN_VRAM_HEADROOM_GB,
        "rows": rows,
        "summary": summarise(rows),
        "ml_training_campaign_in_scope": C.ML_TRAINING_CAMPAIGN_IN_SCOPE,
        "trains_a_model": TRAINS_A_MODEL,
        "selects_a_model": SELECTS_A_MODEL,
        "newer_implies_better": NEWER_IMPLIES_BETTER,
        "installed_anything": False,
    }
    return r37.artifact_body(MATRIX_SCHEMA, payload)


def data_contract_artifact(*, campaign_id: str, created_at: str) -> dict:
    payload = {
        "campaign_id": campaign_id,
        "created_at": created_at,
        "calculation_owner": CALCULATION_OWNER,
        "contract": data_contract(),
    }
    return r37.artifact_body(DATA_CONTRACT_SCHEMA, payload)


def matrix_path_for(campaign_id: str = C.CAMPAIGN_ID):
    return r37.campaign_dir(campaign_id) / MATRIX_ARTIFACT


def data_contract_path_for(campaign_id: str = C.CAMPAIGN_ID):
    return r37.campaign_dir(campaign_id) / DATA_CONTRACT_ARTIFACT


def freeze_matrix(body: dict):
    return r37.write_json(matrix_path_for(body["campaign_id"]), body)


def freeze_data_contract(body: dict):
    return r37.write_json(data_contract_path_for(body["campaign_id"]), body)


def load_matrix(campaign_id: str = C.CAMPAIGN_ID) -> Optional[dict]:
    return r37.read_json(matrix_path_for(campaign_id))


__all__ = ["CALCULATION_OWNER", "FAMILIES", "COST_CLASSES", "MODELS",
           "READINESS_CLASSES", "READINESS_MEANING", "READY_INSTALLED",
           "READY_AFTER_INSTALL", "READY_IMPRACTICAL", "READY_EXTERNAL_GPU",
           "READY_NOT_FEASIBLE", "MIN_VRAM_HEADROOM_GB",
           "TRAINS_A_MODEL", "SELECTS_A_MODEL", "NEWER_IMPLIES_BETTER",
           "feasibility", "matrix", "summarise", "INPUT_FIELDS",
           "OUTPUT_FIELDS", "CONTRACT_RULES", "data_contract",
           "matrix_artifact", "data_contract_artifact", "freeze_matrix",
           "freeze_data_contract", "load_matrix", "matrix_path_for",
           "data_contract_path_for"]
