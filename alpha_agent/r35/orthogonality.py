"""alpha_agent.r35.orthogonality - is the new information actually new?

Release 35 measures this BEFORE it measures prediction, and treats the answer as
a gate, because the alternative order is how a release talks itself into a
finding: run the predictive test first, discover a positive number, and then go
looking for a reason the source was novel.

The wrong test - and the easy one to pass - is raw correlation. A feature can
correlate 0.2 with every base feature individually and still be an exact linear
combination of them. The measured quantity here is therefore the RESIDUAL SHARE:

    residual_share = Var(feature - OLS(feature ~ all 28 base features))
                     / Var(feature)

fitted on TRAINING rows only, and reported alongside the largest single
correlation so a reader can see both. A residual share of 0.03 means 97 % of the
feature is a repackaging of what the model already had; the label is REDUNDANT
and the family cannot claim novelty from it whatever its own t-statistic says.

The regression itself is not implemented here. ``alpha_agent.orthogonality`` -
the Stage 9.4 owner - already owns ``residualize`` and ``partial_rank_ic``, and
this module calls them. What is new in Release 35 is the information, not the
arithmetic.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .. import orthogonality as _orth
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r35.orthogonality"
ORTHOGONALITY_SCHEMA = "r35_orthogonality_report/1"
ARTIFACT_NAME = "orthogonality_report.json"

#: The released residualiser is exact but O(k^3) in a dense solve per feature
#: and takes Python lists; rows are subsampled to this many when a training
#: block is larger, deterministically and without a random seed - every
#: ``stride``-th row - so the measurement is reproducible byte for byte.
MAX_ROWS_FOR_RESIDUALISATION = 20000


#: Two controls this correlated carry the same column space; keeping both makes
#: the normal equations singular without adding information.
DUPLICATE_CONTROL_CORRELATION = 0.999


def _subsample(n: int) -> np.ndarray:
    if n <= MAX_ROWS_FOR_RESIDUALISATION:
        return np.arange(n)
    stride = int(np.ceil(n / MAX_ROWS_FOR_RESIDUALISATION))
    return np.arange(0, n, stride)


def _condition_controls(controls: np.ndarray, names) -> dict:
    """Standardise the base features before the released residualiser sees them.

    The released ``alpha_agent.orthogonality`` solver is exact Gaussian
    elimination on the raw normal equations and refuses a pivot below 1e-12.
    The 28 base features span several orders of magnitude - a one-year trend
    near 0.1 sits beside a kurtosis near 5 and a carry slope that is
    structurally zero for forty of the forty-seven instruments - so X'X is badly
    enough conditioned that the solver correctly declines it.

    Standardising each column is a numerically neutral change of basis: the
    space the controls span, and therefore the residual, is identical. What it
    changes is only whether the arithmetic can be done. Constant columns and
    exact duplicates are dropped for the same reason, and the count of each is
    reported so a reader can see the conditioning rather than infer it.
    """
    matrix = np.asarray(controls, dtype=float)
    standardised, names_kept = [], []
    dropped_constant, dropped_duplicate = [], []
    for j, name in enumerate(names):
        column = matrix[:, j]
        sd = float(np.std(column))
        if not np.isfinite(sd) or sd <= 1e-12:
            dropped_constant.append(name)
            continue
        z = (column - float(np.mean(column))) / sd
        duplicate_of = None
        for existing_name, existing in zip(names_kept, standardised):
            corr = float(np.corrcoef(z, existing)[0, 1])
            if np.isfinite(corr) and abs(corr) >= DUPLICATE_CONTROL_CORRELATION:
                duplicate_of = existing_name
                break
        if duplicate_of is not None:
            dropped_duplicate.append("%s~%s" % (name, duplicate_of))
            continue
        standardised.append(z)
        names_kept.append(name)
    return {"matrix": (np.column_stack(standardised) if standardised
                       else np.zeros((matrix.shape[0], 0))),
            "names": names_kept,
            "dropped_constant": dropped_constant,
            "dropped_duplicate": dropped_duplicate}


def redundancy_label(residual_share) -> str:
    if residual_share is None or not np.isfinite(residual_share):
        return _contract.REDUNDANT
    if residual_share < _contract.REDUNDANT_RESIDUAL_SHARE_MAX:
        return _contract.REDUNDANT
    if residual_share < _contract.PARTIAL_RESIDUAL_SHARE_MAX:
        return _contract.PARTIALLY_REDUNDANT
    return _contract.DISTINCT


def measure_feature(values: np.ndarray, base: np.ndarray, base_names,
                    *, present: np.ndarray = None, y: np.ndarray = None,
                    dates=None) -> dict:
    """Distinctness of ONE candidate feature against the whole base set.

    ``present`` restricts the measurement to rows where the feature is a REAL
    value rather than neutral fill. Measuring a column that is 80 % zeros would
    report a large residual share for the wrong reason: the zeros are not
    explained by momentum because they are not data.
    """
    v = np.asarray(values, dtype=float)
    ok = np.isfinite(v)
    if present is not None:
        ok &= np.asarray(present, dtype=bool)
    ok &= np.isfinite(base).all(axis=1)
    n_rows = int(ok.sum())
    out = {"rows_measured": n_rows}
    if n_rows < 200:
        out.update({"residual_share": None, "redundancy": _contract.REDUNDANT,
                    "state": "TOO_FEW_REAL_ROWS"})
        return out

    idx = np.flatnonzero(ok)
    idx = idx[_subsample(idx.size)]
    target = v[idx]
    conditioned = _condition_controls(base[idx], list(base_names))
    controls = conditioned["matrix"]
    control_names = conditioned["names"]
    out.update({"controls_used": len(control_names),
                "controls_dropped_constant": conditioned["dropped_constant"],
                "controls_dropped_duplicate": conditioned["dropped_duplicate"]})

    variance = float(np.var(target, ddof=1))
    if not np.isfinite(variance) or variance <= 0:
        out.update({"residual_share": None, "redundancy": _contract.REDUNDANT,
                    "state": "NO_VARIANCE"})
        return out
    if controls.size == 0:
        out.update({"residual_share": None, "redundancy": _contract.REDUNDANT,
                    "state": "NO_USABLE_CONTROLS"})
        return out

    residual = _orth.residualize(
        list(target), [list(controls[:, j]) for j in range(controls.shape[1])])
    if residual is None:
        out.update({"residual_share": None, "redundancy": _contract.REDUNDANT,
                    "state": "RESIDUALISATION_FAILED"})
        return out
    residual_variance = float(np.var(np.asarray(residual, dtype=float), ddof=1))
    residual_share = residual_variance / variance

    correlations = {}
    for j, name in enumerate(control_names):
        column = controls[:, j]
        if float(np.std(column)) <= 0:
            continue
        corr = _orth.rank_correlation(list(target), list(column))
        if corr is not None:
            correlations[name] = float(corr)
    strongest = max(correlations.items(), key=lambda kv: abs(kv[1])) \
        if correlations else (None, None)

    out.update({
        "residual_share": float(residual_share),
        "explained_share": float(max(0.0, 1.0 - residual_share)),
        "redundancy": redundancy_label(residual_share),
        "max_abs_rank_correlation": (abs(strongest[1])
                                     if strongest[1] is not None else None),
        "most_correlated_base_feature": strongest[0],
        "mean_abs_rank_correlation": (
            float(np.mean([abs(c) for c in correlations.values()]))
            if correlations else None),
        "state": "OK",
    })
    if y is not None:
        partial = _orth.partial_rank_ic(
            list(target), list(np.asarray(y, dtype=float)[idx]),
            [list(controls[:, j]) for j in range(controls.shape[1])])
        out["partial_rank_ic_vs_base"] = (float(partial)
                                          if partial is not None else None)
    return out


def measure_family(ctx: dict, frames: dict, presence: pd.DataFrame, *,
                   family: str, training_rows: np.ndarray,
                   base_feature_names, y=None) -> dict:
    """Every feature of one family, measured on TRAINING rows only."""
    from . import design as _design

    design_rows = ctx["design"]
    base_index = [design_rows["feature_names"].index(n)
                  for n in base_feature_names]
    base = design_rows["X"][:, base_index]
    di, sp = design_rows["decision_index"], design_rows["symbol_position"]

    rows = np.asarray(training_rows, dtype=bool)
    per_feature = {}
    for name in _contract.features_of(family):
        frame = frames.get(name)
        if frame is None:
            per_feature[name] = {"state": "FEATURE_NOT_BUILT",
                                 "redundancy": _contract.REDUNDANT,
                                 "residual_share": None}
            continue
        values = _design._lookup(frame, di, sp)
        real = np.isfinite(values)
        if presence is not None and not presence.empty:
            real &= _design._lookup(presence.astype(float), di, sp) > 0.5
        per_feature[name] = measure_feature(
            np.where(rows, values, np.nan), base, base_feature_names,
            present=real & rows,
            y=y, dates=None)
    labels = [spec.get("redundancy") for spec in per_feature.values()]
    admitted = any(label != _contract.REDUNDANT for label in labels)
    shares = [spec.get("residual_share") for spec in per_feature.values()
              if spec.get("residual_share") is not None]
    return {
        "family": family,
        "features": per_feature,
        "distinctness_claim": _contract.DISTINCTNESS_CLAIM.get(family),
        "max_residual_share": float(max(shares)) if shares else None,
        "median_residual_share": float(np.median(shares)) if shares else None,
        "redundancy_labels": {n: s.get("redundancy")
                              for n, s in per_feature.items()},
        "admitted_to_predictive_stage": bool(admitted),
        "admission_rule": (
            "a family is admitted when at least one of its features is not "
            "REDUNDANT against the full base information set"),
    }


def report_artifact(*, campaign_id: str, created_at: str, families: dict,
                    coverage: dict, base_feature_names) -> dict:
    from .. import r35
    payload = {
        "campaign_id": campaign_id,
        "created_at": created_at,
        "calculation_owner": CALCULATION_OWNER,
        "residual_share_owner": _contract.RESIDUAL_SHARE_OWNER,
        "measured_before_prediction":
            _contract.ORTHOGONALITY_MEASURED_BEFORE_PREDICTION,
        "is_a_gate": _contract.ORTHOGONALITY_IS_A_GATE,
        "distinctness_is_raw_correlation_only":
            _contract.DISTINCTNESS_IS_RAW_CORRELATION_ONLY,
        "thresholds": {
            "redundant_residual_share_max":
                _contract.REDUNDANT_RESIDUAL_SHARE_MAX,
            "partial_residual_share_max":
                _contract.PARTIAL_RESIDUAL_SHARE_MAX,
        },
        "base_feature_names": list(base_feature_names),
        "base_feature_count": len(list(base_feature_names)),
        "families": families,
        "coverage": coverage,
        "admitted_families": sorted(
            name for name, spec in families.items()
            if spec.get("admitted_to_predictive_stage")),
        "redundant_families": sorted(
            name for name, spec in families.items()
            if not spec.get("admitted_to_predictive_stage")),
        "measured_on": "TRAINING_ROWS_ONLY",
    }
    return r35.artifact_body(ORTHOGONALITY_SCHEMA, payload)


def path_for(campaign_id: str = _contract.CAMPAIGN_ID):
    from .. import r35
    return r35.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict):
    from .. import r35
    return r35.write_json(path_for(body.get("campaign_id",
                                            _contract.CAMPAIGN_ID)), body)


__all__ = ["CALCULATION_OWNER", "redundancy_label", "measure_feature",
           "measure_family", "report_artifact", "freeze", "path_for",
           "MAX_ROWS_FOR_RESIDUALISATION"]
