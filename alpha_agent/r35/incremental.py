"""alpha_agent.r35.incremental - the paired predictive increment.

One statistic decides this release, and it is a DIFFERENCE:

    for each evaluation date d
        delta(d) = rankIC( BASE + NEW , d )  -  rankIC( BASE , d )

on the same date, the same instruments, the same realised returns and the same
model configuration. The campaign reports mean(delta), its Newey-West
t-statistic, the fraction of dates it is positive, and the two levels it came
from, so nobody has to take the difference on trust.

Why a paired difference rather than two independent scores. R34's base arm
already scores rank IC 0.065 at t = 3.39. An augmented arm scoring 0.068 at
t = 3.5 is NOT evidence of an increment: the two numbers share almost all their
sampling error, and comparing their t-statistics compares the wrong thing. The
per-date difference cancels the common component, which is the whole reason it
is the declared primary.

Two arms, one model. ``contract.MODEL_HELD_FIXED_ACROSS_ARMS`` is True: the
model configuration is chosen on the BASE arm's inner-validation block, inside
training, and the augmented arm is forced to use it. Letting each arm pick its
own would confound "the information helped" with "the augmented arm drew a
luckier architecture". The free-selection version is computed too and is
explicitly SECONDARY.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from ..r33 import predictive as _r33_predictive
from ..r34 import campaign as _r34_campaign
from ..r34 import forecast as _forecast
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r35.incremental"
INCREMENT_SCHEMA = "r35_predictive_increment/1"
ARTIFACT_NAME = "predictive_increment.json"


def evaluation_rows(ctx: dict, forecasts: dict, key) -> np.ndarray:
    """Rows with an out-of-sample score for one model configuration."""
    scores = forecasts[key]["score_eval"]
    return np.isfinite(scores)


def score_arm(ctx: dict, forecasts: dict, key, *, row_mask=None) -> dict:
    """Rank IC of one arm on the rows it is allowed to be scored on."""
    y = ctx["targets"]["y_excess"]
    dates = ctx["row_dates"]
    scores = forecasts[key]["score_eval"]
    ok = np.isfinite(scores) & np.isfinite(y)
    if row_mask is not None:
        ok &= np.asarray(row_mask, dtype=bool)
    if ok.sum() < 50:
        return {"state": "TOO_FEW_ROWS", "rank_ic": None,
                "rows": int(ok.sum())}
    result = _forecast.score_forecast(scores[ok], y[ok], dates[ok])
    per_date = _r33_predictive.per_date_rank_ic(scores[ok], y[ok], dates[ok])
    return {"state": "OK", "rows": int(ok.sum()),
            "rank_ic": result.get("value"),
            "rank_ic_t": result.get("t_stat"),
            "rank_ic_dates": result.get("n"),
            "ic_positive_fraction": result.get("ic_positive_fraction"),
            "oos_r2_vs_realised_mean": result.get("oos_r2_vs_realised_mean"),
            "_per_date": per_date}


def paired_increment(base_scored: dict, candidate_scored: dict) -> dict:
    """The per-date difference between two arms, and its inference.

    Dates present in only one arm are DROPPED rather than treated as a zero
    difference. A date the candidate could not score is not a date on which the
    candidate added nothing; it is a date with no comparison, and counting it as
    a tie would bias every increment toward zero by construction.
    """
    a = base_scored.get("_per_date")
    b = candidate_scored.get("_per_date")
    if a is None or b is None or a.empty or b.empty:
        return {"state": "NO_PAIRED_DATES", "increment": None, "n": 0}
    common = a.index.intersection(b.index)
    if len(common) < 8:
        return {"state": "TOO_FEW_PAIRED_DATES", "increment": None,
                "n": int(len(common))}
    delta = (b.loc[common] - a.loc[common]).astype(float)
    values = delta.to_numpy()
    t_stat = _r33_predictive.newey_west_t(values)
    mean = float(np.mean(values))
    sd = float(np.std(values, ddof=1))
    return {
        "state": "OK",
        "increment": mean,
        "increment_t": float(t_stat) if np.isfinite(t_stat) else None,
        "increment_p": (_r33_predictive.two_sided_p(t_stat)
                        if np.isfinite(t_stat) else None),
        "increment_std": sd,
        "increment_positive_fraction": float((values > 0).mean()),
        "n": int(len(common)),
        "base_rank_ic": base_scored.get("rank_ic"),
        "candidate_rank_ic": candidate_scored.get("rank_ic"),
        "first_date": str(common.min())[:10],
        "last_date": str(common.max())[:10],
        "_per_date_delta": delta,
    }


def arm_responded(base_forecasts: dict, arm_forecasts: dict, model_key, *,
                  row_mask=None) -> dict:
    """Whether the augmented arm's forecast actually MOVED.

    Release 34 v1 was superseded for a guard that could not fail: three
    finalists differing in two lanes reported identical economics to seven
    significant figures, because the object they varied could not move. The same
    trap is live here. An elastic net at a small alpha can shrink every new
    coefficient to exactly zero, and the augmented arm then reproduces the base
    arm's scores bit for bit. That is a real finding - the model declined the
    information - but it is NOT a tested null, and reporting it as "no
    increment" would count a comparison that could not move as evidence that
    nothing moved.
    """
    base = np.asarray(base_forecasts[model_key]["score_eval"], dtype=float)
    arm = np.asarray(arm_forecasts[model_key]["score_eval"], dtype=float)
    ok = np.isfinite(base) & np.isfinite(arm)
    if row_mask is not None:
        ok &= np.asarray(row_mask, dtype=bool)
    if not ok.any():
        return {"responded": False, "reason": "NO_COMPARABLE_ROWS",
                "max_abs_score_change": None}
    delta = np.abs(arm[ok] - base[ok])
    largest = float(delta.max())
    scale = float(np.std(base[ok], ddof=1)) or 1.0
    return {"responded": bool(largest > 0.0),
            "max_abs_score_change": largest,
            "relative_score_change": float(largest / scale),
            "reason": (None if largest > 0.0 else
                       "the fitted model assigned every added feature a zero "
                       "coefficient, so the augmented arm reproduced the base "
                       "arm exactly")}


def per_fold_increment(increment: dict, folds) -> list:
    """The increment one walk-forward evaluation block at a time.

    An increment that averages positive by being large in one regime and
    negative elsewhere is a regime observation, not an information finding, and
    only a per-block table can tell the two apart.
    """
    delta = increment.get("_per_date_delta")
    if delta is None or delta.empty:
        return []
    out = []
    for fold in folds:
        if not fold.get("usable"):
            continue
        start = pd.Timestamp(fold["evaluation_start"])
        end = pd.Timestamp(fold["evaluation_end"])
        block = delta[(delta.index >= start) & (delta.index <= end)]
        if block.size < 4:
            out.append({"evaluation_start": fold["evaluation_start"],
                        "evaluation_end": fold["evaluation_end"],
                        "n": int(block.size), "increment": None})
            continue
        t_stat = _r33_predictive.newey_west_t(block.to_numpy())
        out.append({"evaluation_start": fold["evaluation_start"],
                    "evaluation_end": fold["evaluation_end"],
                    "n": int(block.size),
                    "increment": float(block.mean()),
                    "t_stat": float(t_stat) if np.isfinite(t_stat) else None})
    return out


def minimum_detectable_increment(increment: dict) -> dict:
    """The smallest increment this comparison could have called significant.

    A failed significance test is only informative alongside the size of effect
    it could have found. The Newey-West standard error is recoverable from the
    two numbers already reported - ``increment / t`` - so the smallest increment
    that would clear the frozen t-threshold is
    ``MIN_INCREMENT_T_STAT x standard error``. Reporting it turns "not
    significant" into "not significant, and an increment above X would have
    been", which is a different and much more useful sentence.
    """
    value, t_stat = increment.get("increment"), increment.get("increment_t")
    if value is None or t_stat is None or not np.isfinite(t_stat) \
            or abs(t_stat) < 1e-9:
        return {"standard_error": None, "minimum_detectable": None}
    standard_error = abs(float(value) / float(t_stat))
    return {"standard_error": standard_error,
            "minimum_detectable": float(_contract.MIN_INCREMENT_T_STAT
                                        * standard_error),
            "observed": float(value),
            "meaning": ("the smallest mean per-date rank-IC increment this "
                        "comparison could have declared significant at the "
                        "frozen threshold")}


def gate(increment: dict, *, responded: dict = None) -> dict:
    """Whether one increment clears every frozen predictive condition."""
    conditions = {
        "arm_could_respond": (True if responded is None
                              else bool(responded.get("responded"))),
        "has_paired_dates": increment.get("state") == "OK",
        "enough_dates": (increment.get("n") or 0)
        >= _contract.MIN_INCREMENT_EVALUATION_DATES,
        "positive_sign": (
            (increment.get("increment") or 0.0) > 0
            if _contract.INCREMENT_SIGN_MUST_BE_POSITIVE else True),
        "large_enough": abs(increment.get("increment") or 0.0)
        >= _contract.MIN_INCREMENT_RANK_IC,
        "significant": abs(increment.get("increment_t") or 0.0)
        >= _contract.MIN_INCREMENT_T_STAT,
    }
    return {"conditions": conditions,
            "passed": all(conditions.values()),
            "failed_conditions": sorted(k for k, v in conditions.items()
                                        if not v)}


def run_arm(base_ctx: dict, arm_ctx: dict, *, model_key=None,
            verbose: bool = False) -> dict:
    """Fit one information set through the frozen walk-forward and score it.

    ``model_key`` forces the architecture. When it is None the arm selects its
    own on inner validation, which is the SECONDARY reading.
    """
    forecasts = _r34_campaign.run_forecasts(arm_ctx, verbose=verbose)
    selection = _r34_campaign.select_model_by_inner_validation(arm_ctx,
                                                               forecasts)
    chosen = model_key
    if chosen is None:
        selected = selection.get("selected")
        chosen = selected["key"] if selected else None
    return {"forecasts": forecasts, "selection": selection,
            "model_key": chosen,
            "model_label": (_r34_campaign.key_label(chosen) if chosen
                            else None),
            "free_selection_key": (selection["selected"]["key"]
                                   if selection.get("selected") else None),
            "free_selection_label": (
                _r34_campaign.key_label(selection["selected"]["key"])
                if selection.get("selected") else None)}


def artifact(*, campaign_id: str, created_at: str, horizon_results: dict,
             standalone: dict, executed_configs: list) -> dict:
    from .. import r35
    payload = {
        "campaign_id": campaign_id,
        "created_at": created_at,
        "calculation_owner": CALCULATION_OWNER,
        "primary_statistic": _contract.PRIMARY_INCREMENT_STATISTIC,
        "statistic_meaning": _contract.INCREMENT_STATISTIC_MEANING,
        "model_held_fixed_across_arms":
            _contract.MODEL_HELD_FIXED_ACROSS_ARMS,
        "free_model_selection_is_secondary":
            _contract.FREE_MODEL_SELECTION_IS_SECONDARY,
        "gates": {
            "min_increment_rank_ic": _contract.MIN_INCREMENT_RANK_IC,
            "min_increment_t_stat": _contract.MIN_INCREMENT_T_STAT,
            "min_increment_evaluation_dates":
                _contract.MIN_INCREMENT_EVALUATION_DATES,
            "increment_sign_must_be_positive":
                _contract.INCREMENT_SIGN_MUST_BE_POSITIVE,
        },
        "by_horizon": horizon_results,
        "standalone_diagnostic": standalone,
        "standalone_meaning": (
            "the new information ALONE, with no base features. A family can "
            "predict on its own and add nothing conditional on the base set; "
            "only the second is the release question"),
        "executed_configurations": executed_configs,
        "executed_configuration_count": len(executed_configs),
    }
    return r35.artifact_body(INCREMENT_SCHEMA, payload)


def path_for(campaign_id: str = _contract.CAMPAIGN_ID):
    from .. import r35
    return r35.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict):
    from .. import r35
    return r35.write_json(path_for(body.get("campaign_id",
                                            _contract.CAMPAIGN_ID)), body)


__all__ = ["CALCULATION_OWNER", "score_arm", "paired_increment", "gate",
           "run_arm", "artifact", "freeze", "path_for", "evaluation_rows",
           "arm_responded", "per_fold_increment",
           "minimum_detectable_increment"]
