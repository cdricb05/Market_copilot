"""alpha_agent.r35.campaign - orchestration and verdict for Release 35.

The shape of the run, and why it is in this order:

1. **Freeze the contract.** Before anything is measured, so no threshold can be
   chosen after a result.
2. **Acquire.** Five free public sources and one already-owned store, checksummed
   into a manifest. A source that fails is recorded and the rest continue.
3. **Normalise and measure coverage.** Every series gets a publication stamp.
4. **Build the new features.** Nineteen, declared, no search.
5. **Run Lane A.** The analyst-expectation lane, measured and gated, in parallel
   with everything else rather than as a blocker.
6. **Measure orthogonality on TRAINING rows.** Before prediction, as a gate.
7. **Measure the predictive increment.** Paired per-date rank IC, base arm
   against each candidate information set, same model, same rows, same dates.
8. **Convert only what survives.** The economic stage runs the base arm always
   and each predictive survivor once, through R34's FROZEN conversion.
9. **Correct for multiple testing**, counting every executed configuration.
10. **Verdict**, with three separate results and a structurally unreachable
    ``ALPHA_RESULT = PASS``.

The one thing this module must never do is let a good number change the rules
that judged it. Every threshold it reads comes from :mod:`contract`, every
statistic it computes comes from an owner imported from an earlier release, and
the qualified verdict is gated on
``contract.genuinely_independent_evidence_exists()``, which is False.
"""
from __future__ import annotations

import datetime as _dt
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from .. import r35
from ..r31 import multiple_testing as _mt
from ..r33 import features as _r33_features
from ..r34 import campaign as _r34_campaign
from ..r34 import concentration as _concentration
from ..r34 import panel as _r34_panel
from ..r34 import portfolio as _portfolio
from ..r34 import universe as _r34_universe
from . import acquisition as _acq
from . import analyst_lane as _analyst_lane
from . import contract as _contract
from . import design as _design
from . import features as _features
from . import incremental as _incremental
from . import information as _info
from . import orthogonality as _orthogonality

CALCULATION_OWNER = "alpha_agent.r35.campaign"
VERDICT_SCHEMA = "r35_final_verdict/1"
ECONOMICS_SCHEMA = "r35_economic_increment/1"
MULTIPLE_TESTING_SCHEMA = "r35_multiple_testing/1"

ARTIFACTS = {
    "contract": "research_contract.json",
    "acquisition": "acquisition_manifest.json",
    "coverage": "information_coverage.json",
    "features": "new_feature_registry.json",
    "analyst_lane": "analyst_expectation_lane.json",
    "orthogonality": "orthogonality_report.json",
    "increment": "predictive_increment.json",
    "economics": "economic_increment.json",
    "multiple_testing": "multiple_testing.json",
    "verdict": "final_verdict.json",
}

#: The R34 universe enumeration cache. Reading it rather than re-enumerating
#: 8,139 products keeps the universe BYTE-identical to the one R34 judged, which
#: is the whole basis for calling the base arm a reproduction of R34.
R34_ENUMERATION_CACHE = Path(
    r"D:\Stock_Prediction_app_data\prediction_to_pnl_r34\cache"
    r"\etp_enumeration.json")


def _now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


def _say(verbose: bool, message: str) -> None:
    if verbose:
        print(message, flush=True)


def _clean(obj):
    """JSON-safe: numpy scalars, pandas objects and private keys removed."""
    if isinstance(obj, dict):
        return {k: _clean(v) for k, v in obj.items()
                if not str(k).startswith("_")}
    if isinstance(obj, (list, tuple)):
        return [_clean(v) for v in obj]
    if isinstance(obj, (np.floating, np.integer)):
        value = obj.item()
        return None if isinstance(value, float) and not np.isfinite(value) \
            else value
    if isinstance(obj, float):
        return None if not np.isfinite(obj) else obj
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if isinstance(obj, (pd.Timestamp, _dt.date, _dt.datetime)):
        return str(obj)[:19]
    if isinstance(obj, (pd.Series, pd.DatetimeIndex, pd.Index)):
        return [str(v) for v in list(obj)[:400]]
    if isinstance(obj, np.ndarray):
        return _clean(obj.tolist())
    return obj


def _write(campaign_id: str, key: str, payload: dict) -> Path:
    return r35.write_json(
        r35.campaign_dir(campaign_id) / ARTIFACTS[key], _clean(payload))


# --------------------------------------------------------------------------- #
# Inputs
# --------------------------------------------------------------------------- #
def load_information(*, results: dict, verbose: bool = False) -> dict:
    """Every acquired source, normalised, with its own failure recorded."""
    out = {}
    cot = _info.load_cot(results[_acq.SRC_CFTC].get("files") or {})
    out["cot"] = cot
    _say(verbose, "    COT %s rows=%s %s..%s"
         % (cot.get("ok"), cot.get("rows"), cot.get("first"), cot.get("last")))

    fred = _info.load_fred(results[_acq.SRC_FRED].get("files") or {})
    out["fred"] = fred
    _say(verbose, "    FRED %s series=%d" % (fred.get("ok"),
                                             len(fred.get("series") or {})))

    cboe = _info.load_cboe(results[_acq.SRC_CBOE].get("files") or {})
    out["cboe"] = cboe
    _say(verbose, "    CBOE %s" % cboe.get("ok"))

    eia_files = results[_acq.SRC_EIA].get("files") or {}
    curve = (_info.load_eia_curve(eia_files["PET"]) if "PET" in eia_files
             else {"ok": False, "reason": "EIA_PAYLOAD_ABSENT", "frame": None})
    out["curve"] = curve
    _say(verbose, "    EIA curve %s %s..%s" % (curve.get("ok"),
                                               curve.get("first"),
                                               curve.get("last")))

    sector_series = _info.build_pit_sector_series(
        results[_acq.SRC_OWNED_FSDS].get("files") or {})
    filings = _info.load_insider_filings(
        results[_acq.SRC_SEC_INSIDER].get("files") or {})
    insider = {"ok": False, "frame": None, "reason": "NOT_BUILT"}
    if filings.get("ok") and sector_series.get("ok"):
        insider = _info.insider_sector_daily(filings["frame"],
                                             sector_series["series"])
        insider["filings"] = {k: v for k, v in filings.items()
                              if k not in ("frame",)}
        insider["pit_sector"] = {k: v for k, v in sector_series.items()
                                 if k != "series"}
    out["insider"] = insider
    _say(verbose, "    SEC insider %s classified_share=%s"
         % (insider.get("ok"), insider.get("classified_share")))
    return out


def source_summary(results: dict, loaded: dict) -> dict:
    """What each source delivered, for the coverage artifact."""
    cot, fred = loaded["cot"], loaded["fred"]
    curve, insider = loaded["curve"], loaded["insider"]
    return {
        _acq.SRC_CFTC: {
            "ok": bool(cot.get("ok")), "rows": cot.get("rows"),
            "first_report_date": cot.get("first"),
            "last_report_date": cot.get("last"),
            "years_read": cot.get("years_read"),
            "contract_codes_present": cot.get("codes_present"),
            "publication_lag_days": _contract.COT_PUBLICATION_LAG_DAYS,
            "licence": _acq.SOURCE_LICENCE[_acq.SRC_CFTC]},
        _acq.SRC_FRED: {
            "ok": bool(fred.get("ok")), "series": fred.get("meta"),
            "licence": _acq.SOURCE_LICENCE[_acq.SRC_FRED]},
        _acq.SRC_CBOE: {
            "ok": bool(loaded["cboe"].get("ok")),
            "series": loaded["cboe"].get("meta"),
            "licence": _acq.SOURCE_LICENCE[_acq.SRC_CBOE]},
        _acq.SRC_EIA: {
            "ok": bool(curve.get("ok")), "first": curve.get("first"),
            "last": curve.get("last"),
            "series_found": curve.get("series_found"),
            "source_discontinued_at": curve.get("source_discontinued_at"),
            "discontinuation_note": (
                "EIA stopped republishing NYMEX settlement prices; the series "
                "ends where the publisher ended it, and nothing extrapolates "
                "past that date"),
            "licence": _acq.SOURCE_LICENCE[_acq.SRC_EIA]},
        _acq.SRC_SEC_INSIDER: {
            "ok": bool(insider.get("ok")),
            "classified_share": insider.get("classified_share"),
            "sectors": insider.get("sectors"),
            "filings": insider.get("filings"),
            "value_weighting_allowed":
                _contract.INSIDER_VALUE_WEIGHTING_ALLOWED,
            "value_weighting_rejection_reason":
                _info.INSIDER_VALUE_REJECTION_REASON,
            "licence": _acq.SOURCE_LICENCE[_acq.SRC_SEC_INSIDER]},
        _acq.SRC_OWNED_FSDS: {
            "ok": bool((insider.get("pit_sector") or {}).get("ok")),
            "pit_sector": insider.get("pit_sector"),
            "downloaded": False,
            "licence": _acq.SOURCE_LICENCE[_acq.SRC_OWNED_FSDS]},
    }


# --------------------------------------------------------------------------- #
# Orthogonality
# --------------------------------------------------------------------------- #
def training_row_mask(ctx: dict) -> np.ndarray:
    """Rows in ANY fold's training partition, at the primary horizon.

    Every distinctness measurement is confined to these rows. Measuring a
    feature's redundancy on evaluation rows would put evaluation data into a
    decision that gates evaluation, which is the same defect as fitting a scaler
    on the test set and is just as invisible in the output.
    """
    positions = set()
    for fold in ctx["folds"]:
        if not fold["usable"]:
            continue
        positions.update(int(p) for p in fold["train"])
    rows = np.zeros(len(ctx["row_dates"]), dtype=bool)
    if not positions:
        return rows
    wanted = np.zeros(len(ctx["udates"]), dtype=bool)
    wanted[sorted(positions)] = True
    rows[wanted[ctx["row_date_pos"]]] = True
    return rows


def run_orthogonality(ctx: dict, built: dict, *, verbose: bool = False) -> dict:
    """Distinctness of every acquired family against the 28 base features."""
    frames = _features.frames(built)
    presence = _features.presence(built)
    rows = training_row_mask(ctx)
    y = ctx["targets"]["y_excess"]
    out = {}
    for family in sorted(built):
        out[family] = _orthogonality.measure_family(
            ctx, frames, presence.get(family), family=family,
            training_rows=rows,
            base_feature_names=list(_r33_features.FEATURE_NAMES), y=y)
        _say(verbose, "    %-38s admitted=%s median_residual_share=%s"
             % (family, out[family]["admitted_to_predictive_stage"],
                None if out[family]["median_residual_share"] is None
                else round(out[family]["median_residual_share"], 3)))
    return out


# --------------------------------------------------------------------------- #
# Predictive increment
# --------------------------------------------------------------------------- #
def run_increments(base_ctxs: dict, built: dict, *, verbose: bool = False
                   ) -> dict:
    """Every candidate information set, at every horizon, paired against BASE."""
    frames = _features.frames(built)
    presence = _features.presence(built)
    sets = _design.information_sets(built)
    candidates = [name for name in sets if name != "BASE"]

    by_horizon, executed = {}, []
    base_cache = {}
    for horizon in _contract.HORIZONS:
        ctx = base_ctxs[horizon]
        base_forecasts = _r34_campaign.run_forecasts(ctx)
        selection = _r34_campaign.select_model_by_inner_validation(
            ctx, base_forecasts)
        selected = selection.get("selected")
        model_key = (selected["key"] if selected
                     else list(base_forecasts.keys())[0])
        base_cache[horizon] = {"forecasts": base_forecasts,
                               "model_key": model_key}
        _say(verbose, "  h=%d base model on inner validation: %s"
             % (horizon, _r34_campaign.key_label(model_key)))

        rows_for_set = {}
        results = {}
        for name in candidates:
            feature_names = sets[name]
            if name == "ALL_NEW_COMBINED":
                mask = None
                for family in built:
                    part = _design.available_dates(ctx, presence.get(family))
                    mask = set(part) if mask is None else (mask & set(part))
                dates = pd.DatetimeIndex(sorted(mask or []))
            else:
                dates = _design.available_dates(ctx, presence.get(name))
            row_mask = _design.row_mask_for_dates(ctx, dates)
            rows_for_set[name] = row_mask

            arm_ctx = _design.augment_context(ctx, frames=frames,
                                              feature_names=feature_names)
            arm = _incremental.run_arm(ctx, arm_ctx, model_key=model_key)
            base_scored = _incremental.score_arm(ctx, base_forecasts,
                                                 model_key, row_mask=row_mask)
            candidate_scored = _incremental.score_arm(
                arm_ctx, arm["forecasts"], model_key, row_mask=row_mask)
            increment = _incremental.paired_increment(base_scored,
                                                      candidate_scored)
            responded = _incremental.arm_responded(
                base_forecasts, arm["forecasts"], model_key,
                row_mask=row_mask)
            free_key = arm.get("free_selection_key")
            free_increment = None
            if free_key is not None and free_key != model_key:
                free_scored = _incremental.score_arm(
                    arm_ctx, arm["forecasts"], free_key, row_mask=row_mask)
                free_increment = _incremental.paired_increment(base_scored,
                                                              free_scored)
            gate = _incremental.gate(increment, responded=responded)
            record = {
                "information_set": name,
                "horizon": horizon,
                "features_added": list(feature_names),
                "feature_count": len(feature_names),
                "available_dates": int(len(dates)),
                "available_first": str(dates.min())[:10] if len(dates) else None,
                "available_last": str(dates.max())[:10] if len(dates) else None,
                "model": _r34_campaign.key_label(model_key),
                "model_held_fixed": _contract.MODEL_HELD_FIXED_ACROSS_ARMS,
                "base_arm": {k: v for k, v in base_scored.items()
                             if not k.startswith("_")},
                "candidate_arm": {k: v for k, v in candidate_scored.items()
                                  if not k.startswith("_")},
                "increment": {k: v for k, v in increment.items()
                              if not k.startswith("_")},
                "arm_responded": responded,
                "minimum_detectable_increment":
                    _incremental.minimum_detectable_increment(increment),
                "per_fold_increment": _incremental.per_fold_increment(
                    increment, ctx["folds"]),
                "gate": gate,
                "secondary_free_selection": {
                    "model": arm.get("free_selection_label"),
                    "increment": ({k: v for k, v in free_increment.items()
                                   if not k.startswith("_")}
                                  if free_increment else None),
                    "is_decisive": False,
                },
            }
            results[name] = record
            executed.append({
                "configuration_id": "PREDICTIVE::%s::h%d" % (name, horizon),
                "family": "PREDICTIVE_INCREMENT",
                "information_set": name, "horizon": horizon,
                "increment": increment.get("increment"),
                "t_stat": increment.get("increment_t"),
                "p_value": increment.get("increment_p"),
                "n": increment.get("n"),
                "arm_responded": responded.get("responded"),
                "passed_gate": gate["passed"]})
            _say(verbose, "    h=%-2d %-36s delta=%s t=%s n=%s %s"
                 % (horizon, name,
                    None if increment.get("increment") is None
                    else round(increment["increment"], 5),
                    None if increment.get("increment_t") is None
                    else round(increment["increment_t"], 2),
                    increment.get("n"),
                    "PASS" if gate["passed"]
                    else ("NO_EFFECT" if not responded.get("responded")
                          else "fail")))
        by_horizon[horizon] = {"per_set": results,
                               "base_selection": {
                                   "model": _r34_campaign.key_label(model_key),
                                   "per_model": selection.get("per_model")},
                               "row_masks_built": sorted(rows_for_set)}
    return {"by_horizon": by_horizon, "executed": executed,
            "base_cache": base_cache, "sets": sets}


def run_publication_lag_sensitivity(base_ctxs: dict, panel: dict,
                                    loaded: dict, increments: dict, *,
                                    verbose: bool = False) -> dict:
    """Re-measure the positioning increment under a much longer publication lag.

    Every other acquired series carries a publication date that is READ - a
    settlement session, an index close, an SEC filing stamp. Positioning is the
    exception: the Commitments of Traders report is stamped with a Tuesday and
    the release date is INFERRED from the CFTC's ordinary Friday schedule. That
    schedule held for almost the whole sample and did not during the 2013 and
    2018-19 shutdowns, when reports appeared weeks late and a six-day rule would
    claim knowledge nobody had.

    So the family is re-run at ``COT_PUBLICATION_LAG_STRESS_DAYS``, long enough
    to cover those catch-ups. This is a robustness measurement of a frozen
    configuration, not a second configuration competing to be the answer: the
    base lag is declared in the contract and the stressed number cannot replace
    it, only qualify it.
    """
    horizon = _contract.PRIMARY_HORIZON
    ctx = base_ctxs[horizon]
    base_cache = (increments.get("base_cache") or {}).get(horizon)
    cot_frame = loaded["cot"].get("frame")
    if base_cache is None or cot_frame is None:
        return {"state": "NOT_APPLICABLE"}

    stressed = _features.build_positioning(
        cot_frame, panel["calendar"], panel["symbols"],
        lag_days=_contract.COT_PUBLICATION_LAG_STRESS_DAYS)
    frames = stressed["features"]
    names = list(_contract.features_of(_contract.FAM_POSITIONING))
    dates = _design.available_dates(ctx, stressed["present"])
    row_mask = _design.row_mask_for_dates(ctx, dates)
    model_key = base_cache["model_key"]
    arm_ctx = _design.augment_context(ctx, frames=frames, feature_names=names)
    arm = _incremental.run_arm(ctx, arm_ctx, model_key=model_key)
    base_scored = _incremental.score_arm(ctx, base_cache["forecasts"],
                                         model_key, row_mask=row_mask)
    candidate_scored = _incremental.score_arm(arm_ctx, arm["forecasts"],
                                              model_key, row_mask=row_mask)
    increment = _incremental.paired_increment(base_scored, candidate_scored)
    declared = ((increments.get("by_horizon") or {}).get(horizon) or {}) \
        .get("per_set", {}).get(_contract.FAM_POSITIONING, {}) \
        .get("increment", {})
    _say(verbose, "    positioning at %d-day lag: delta=%s t=%s (declared "
                  "%d-day: delta=%s)"
         % (_contract.COT_PUBLICATION_LAG_STRESS_DAYS,
            None if increment.get("increment") is None
            else round(increment["increment"], 5),
            None if increment.get("increment_t") is None
            else round(increment["increment_t"], 2),
            _contract.COT_PUBLICATION_LAG_DAYS, declared.get("increment")))
    return {
        "state": "OK",
        "family": _contract.FAM_POSITIONING,
        "horizon": horizon,
        "declared_lag_days": _contract.COT_PUBLICATION_LAG_DAYS,
        "stress_lag_days": _contract.COT_PUBLICATION_LAG_STRESS_DAYS,
        "declared_increment": declared.get("increment"),
        "declared_increment_t": declared.get("increment_t"),
        "stressed_increment": increment.get("increment"),
        "stressed_increment_t": increment.get("increment_t"),
        "stressed_n": increment.get("n"),
        "conclusion_depends_on_the_lag": bool(
            ((declared.get("increment") or 0.0) > 0.0)
            != ((increment.get("increment") or 0.0) > 0.0)),
        "neither_lag_clears_the_gate": bool(
            abs(declared.get("increment_t") or 0.0)
            < _contract.MIN_INCREMENT_T_STAT
            and abs(increment.get("increment_t") or 0.0)
            < _contract.MIN_INCREMENT_T_STAT),
        "why_only_this_family": (
            "positioning is the only acquired family whose publication date is "
            "inferred rather than read"),
        "is_a_robustness_measurement_not_a_competing_configuration": True,
    }


def run_standalone(base_ctxs: dict, built: dict, *, verbose: bool = False
                   ) -> dict:
    """Each family ALONE, at the primary horizon - a diagnostic, not a claim."""
    frames = _features.frames(built)
    presence = _features.presence(built)
    ctx = base_ctxs[_contract.PRIMARY_HORIZON]
    out, executed = {}, []
    for family in sorted(built):
        names = list(_contract.features_of(family))
        arm_ctx = _design.only_new_context(ctx, frames=frames,
                                           feature_names=names)
        dates = _design.available_dates(ctx, presence.get(family))
        # Covered rows only: see design.row_mask_for_presence for why an
        # all-zero feature block on the instruments a family does not cover
        # manufactures a rank IC out of the coverage pattern alone.
        row_mask = (_design.row_mask_for_dates(ctx, dates)
                    & _design.row_mask_for_presence(ctx, presence.get(family)))
        arm = _incremental.run_arm(ctx, arm_ctx, model_key=None)
        key = arm.get("model_key")
        scored = ({"state": "NO_MODEL", "rank_ic": None}
                  if key is None else
                  _incremental.score_arm(arm_ctx, arm["forecasts"], key,
                                         row_mask=row_mask))
        out[family] = {"model": arm.get("model_label"),
                       "rank_ic": scored.get("rank_ic"),
                       "rank_ic_t": scored.get("rank_ic_t"),
                       "rank_ic_dates": scored.get("rank_ic_dates"),
                       "rows": scored.get("rows"),
                       "scored_on": "COVERED_ROWS_ONLY",
                       "instruments_covered": int(
                           (presence.get(family).sum(axis=0) > 0).sum())
                       if presence.get(family) is not None else None,
                       "state": scored.get("state"),
                       "note": (
                           "a cross-sectional rank IC needs at least five "
                           "covered instruments on a date; a family covering "
                           "fewer has no standalone cross-section to rank and "
                           "reports None rather than a number computed on "
                           "three names"
                           if scored.get("rank_ic") is None else None)}
        executed.append({
            "configuration_id": "STANDALONE::%s::h%d"
                                % (family, _contract.PRIMARY_HORIZON),
            "family": "STANDALONE_DIAGNOSTIC",
            "information_set": family,
            "horizon": _contract.PRIMARY_HORIZON,
            "rank_ic": scored.get("rank_ic"),
            "t_stat": scored.get("rank_ic_t"),
            "p_value": None, "n": scored.get("rank_ic_dates"),
            "passed_gate": None})
        _say(verbose, "    standalone %-36s rankIC=%s t=%s"
             % (family,
                None if scored.get("rank_ic") is None
                else round(scored["rank_ic"], 5),
                None if scored.get("rank_ic_t") is None
                else round(scored["rank_ic_t"], 2)))
    return {"per_family": out, "executed": executed}


# --------------------------------------------------------------------------- #
# Economic increment
# --------------------------------------------------------------------------- #
def _conversion_frames(ctx_by_h: dict, forecasts_by_h: dict, model_by_h: dict):
    """R34's FROZEN conversion, applied identically to every arm."""
    per_h = {}
    for horizon in _contract.FROZEN_CONVERSION["horizons"]:
        ctx = ctx_by_h.get(horizon)
        if ctx is None:
            continue
        per_h[horizon] = _r34_campaign.conviction_frame(
            ctx, forecasts_by_h[horizon], model_key=model_by_h[horizon],
            calibration=_contract.FROZEN_CONVERSION["calibration"],
            sizing_rule=_contract.FROZEN_CONVERSION["sizing"])
    weights = {h: 1.0 / max(len(per_h), 1) for h in per_h}
    return per_h, weights


def run_economic_arm(*, label: str, ctx_by_h: dict, forecasts_by_h: dict,
                     model_by_h: dict, cov_by_date: dict) -> dict:
    """One information set through R34's winning conversion configuration."""
    primary = _contract.FROZEN_CONVERSION["primary_horizon"]
    per_h, weights = _conversion_frames(ctx_by_h, forecasts_by_h, model_by_h)
    if not per_h:
        return {"record": {"candidate_id": label, "state": "NO_FRAMES"},
                "result": {"state": "NO_FRAMES"}}
    combined = _r34_campaign.combine_frames(ctx_by_h[primary], per_h, weights)
    return _r34_campaign.run_candidate(
        ctx_by_h[primary], forecasts_by_h[primary],
        family="ECONOMIC", label=label, model_key=model_by_h[primary],
        calibration=_contract.FROZEN_CONVERSION["calibration"],
        sizing_rule=_contract.FROZEN_CONVERSION["sizing"],
        mapping=_contract.FROZEN_CONVERSION["portfolio"],
        turnover_rule=_contract.FROZEN_CONVERSION["turnover"],
        turnover_param=_contract.FROZEN_CONVERSION["turnover_param"],
        cov_by_date=cov_by_date, conviction_override=combined,
        config=dict(_contract.FROZEN_CONVERSION,
                    horizons=list(_contract.FROZEN_CONVERSION["horizons"])))


def paired_economic_increment(base_result: dict, candidate_result: dict,
                              *, horizon: int) -> dict:
    """The candidate's after-cost excess minus the base arm's, paired by date.

    Both arms are judged against their own volatility-matched control, so this
    difference is a difference of EXCESSES. That is the quantity the release
    asks about: not whether the augmented book made money, but whether the new
    information bought anything the base information did not already buy.
    """
    from ..r34 import economics as _economics
    if (base_result.get("state") != "OK"
            or candidate_result.get("state") != "OK"):
        return {"state": "AN_ARM_HAS_NO_BOOK", "increment": None}
    base_excess = pd.Series(np.asarray(base_result["path"]["net"], float)
                            - np.asarray(base_result["control"], float),
                            index=pd.DatetimeIndex(base_result["dates"]))
    cand_excess = pd.Series(np.asarray(candidate_result["path"]["net"], float)
                            - np.asarray(candidate_result["control"], float),
                            index=pd.DatetimeIndex(candidate_result["dates"]))
    common = base_excess.index.intersection(cand_excess.index)
    if len(common) < 8:
        return {"state": "TOO_FEW_PAIRED_DATES", "increment": None,
                "n": int(len(common))}
    delta = (cand_excess.loc[common] - base_excess.loc[common]).to_numpy()
    significance = _economics.excess_significance(
        cand_excess.loc[common].to_numpy(), base_excess.loc[common].to_numpy(),
        horizon=horizon)
    return {"state": "OK",
            "increment_annualised": significance.get("annualised_excess"),
            "increment_t": significance.get("t_stat"),
            "increment_positive_fraction": float((delta > 0).mean()),
            "n": int(len(common)),
            "base_after_cost_excess_annualised":
                base_result["stats"].get("after_cost_excess_annualised"),
            "candidate_after_cost_excess_annualised":
                candidate_result["stats"].get("after_cost_excess_annualised")}


def economic_gate(increment: dict, record: dict, concentration: dict) -> dict:
    """Every frozen economic condition, applied to one candidate.

    The concentration conditions read the SHARE gates rather than the
    leave-one-out sign reversal. R34 measured why: when a book's excess over its
    control is statistically indistinguishable from zero, removing almost any
    instrument reverses its sign, and the reversal then measures proximity to
    zero rather than dependence. The owner publishes
    ``sign_reversal_test_is_informative`` for exactly this reason, so the
    reversal is applied as a condition only when it carries information - a
    decision taken here in advance rather than after reading a result.
    """
    gates = (concentration or {}).get("gates") or {}
    informative = bool((concentration or {}).get(
        "sign_reversal_test_is_informative"))
    conditions = {
        "candidate_has_a_book": record.get("state") == "OK",
        "paired_increment_measured": increment.get("state") == "OK",
        "increment_positive": (increment.get("increment_annualised") or 0.0)
        > 0,
        "increment_large_enough":
            abs(increment.get("increment_annualised") or 0.0)
            >= _contract.MIN_ECONOMIC_INCREMENT_ANNUALISED,
        "increment_significant": abs(increment.get("increment_t") or 0.0)
        >= _contract.MIN_ECONOMIC_INCREMENT_T_STAT,
        "beats_its_own_risk_matched_control":
            (record.get("after_cost_excess_annualised") or 0.0) > 0,
        "survives_stressed_cost": bool(record.get("survives_stressed_cost")),
        "not_one_instrument": bool(
            gates.get("single_instrument_pnl_share_within_limit", False)),
        "not_one_asset_class": bool(
            gates.get("single_asset_class_pnl_share_within_limit", False)),
        "enough_effective_instruments": bool(
            gates.get("enough_effective_instruments", False)),
    }
    if informative:
        conditions["no_sign_reversal_on_leave_one_instrument_out"] = bool(
            gates.get("no_sign_reversal_on_leave_one_instrument_out", False))
        conditions["no_sign_reversal_on_leave_one_asset_class_out"] = bool(
            gates.get("no_sign_reversal_on_leave_one_asset_class_out", False))
    return {"conditions": conditions, "passed": all(conditions.values()),
            "failed_conditions": sorted(k for k, v in conditions.items()
                                        if not v),
            "sign_reversal_test_is_informative": informative,
            "sign_reversal_reading": (concentration or {}).get(
                "sign_reversal_reading")}


# --------------------------------------------------------------------------- #
# Verdict
# --------------------------------------------------------------------------- #
def _best_increment(increments: dict, family: str) -> dict:
    """The largest increment this family produced at any horizon, and its t.

    A horizon where the fitted model gave every added feature a zero
    coefficient is SKIPPED. Its increment is exactly 0.0, which would win this
    comparison against a genuinely negative one and report "the best this family
    managed was nothing" when the truth is "at that horizon the model declined
    to look".
    """
    best = None
    for horizon, block in (increments.get("by_horizon") or {}).items():
        record = (block.get("per_set") or {}).get(family)
        if record is None:
            continue
        if not (record.get("arm_responded") or {}).get("responded", True):
            continue
        value = (record.get("increment") or {}).get("increment")
        if value is None:
            continue
        if best is None or value > best["increment"]:
            best = {"horizon": horizon, "increment": value,
                    "t_stat": (record.get("increment") or {}).get(
                        "increment_t"),
                    "n": (record.get("increment") or {}).get("n"),
                    "minimum_detectable_increment":
                        record.get("minimum_detectable_increment"),
                    "passed_gate": (record.get("gate") or {}).get("passed")}
    return best


def build_verdict(*, campaign_id: str, created_at: str, contract_body: dict,
                  acquisition: dict, coverage: dict, orthogonality: dict,
                  increments: dict, standalone: dict, economics: dict,
                  multiple_testing: dict, analyst: dict,
                  executed_configs: list) -> dict:
    """The terminal verdict, and three separate results.

    ``ALPHA_RESULT = PASS`` requires ``VERDICT_QUALIFIED`` AND genuinely
    independent evidence. The second is False by contract, declared before the
    campaign ran, so the first cannot on its own produce Alpha here.
    """
    admitted = sorted(name for name, spec in orthogonality.items()
                      if spec.get("admitted_to_predictive_stage"))
    redundant = sorted(name for name, spec in orthogonality.items()
                       if not spec.get("admitted_to_predictive_stage"))

    predictive_survivors = sorted({
        record["information_set"]
        for horizon in increments["by_horizon"].values()
        for record in horizon["per_set"].values()
        if record["gate"]["passed"]})
    economic_survivors = sorted(name for name, spec in economics.items()
                                if name != "BASE"
                                and (spec.get("gate") or {}).get("passed"))
    bh_positive = (multiple_testing.get("benjamini_hochberg", {})
                   .get("rejected_beating_the_base") or [])
    qualified = sorted(set(economic_survivors)
                       & {row.split("::")[1] for row in bh_positive})

    families_measured = sorted(coverage.get("families") or {})
    nothing_reached_the_stage = not admitted
    integrity_blocked = bool(coverage.get("integrity_violations"))

    if integrity_blocked:
        verdict = _contract.VERDICT_INTEGRITY_BLOCKED
    elif nothing_reached_the_stage or not families_measured:
        verdict = _contract.VERDICT_ACQUISITION_BLOCKED
    elif qualified:
        verdict = _contract.VERDICT_QUALIFIED
    else:
        verdict = _contract.VERDICT_NO_EDGE

    independent = _contract.genuinely_independent_evidence_exists()
    research_candidate_pass = bool(qualified)
    alpha_pass = bool(verdict == _contract.ALPHA_PASS_REQUIRES and independent)

    blocking = []
    if not qualified:
        blocking.append("no acquired information family produced an "
                        "incremental after-cost excess over the base arm that "
                        "survives its frozen gates")
    if not independent:
        blocking.append(_contract.FRESH_UNSEEN_EVIDENCE_REASON)

    payload = {
        "campaign_id": campaign_id,
        "created_at": created_at,
        "calculation_owner": CALCULATION_OWNER,
        "contract_hash": contract_body.get("contract_hash"),
        "primary_verdict": verdict,
        "verdict_meaning": _contract.VERDICT_MEANING[verdict],
        "SYSTEM_RESULT": "PASS",
        "RESEARCH_CANDIDATE_RESULT": "PASS" if research_candidate_pass
        else "FAIL",
        "ALPHA_RESULT": "PASS" if alpha_pass else "FAIL",
        "system_and_alpha_results_are_separate":
            _contract.SYSTEM_AND_ALPHA_RESULTS_ARE_SEPARATE,
        "result_meaning": {
            "SYSTEM_RESULT": (
                "the campaign acquired what it said it would acquire, "
                "measured what it said it would measure, and wrote every "
                "artifact"),
            "RESEARCH_CANDIDATE_RESULT": (
                "at least one acquired information family added incremental "
                "prediction AND incremental after-cost economics over the "
                "base arm, on HISTORICAL evidence"),
            "ALPHA_RESULT": (
                "the same, on evidence no earlier release has consumed. "
                "Structurally unreachable in Release 35 and declared so "
                "before the run"),
        },
        "alpha_pass_requires": _contract.ALPHA_PASS_REQUIRES,
        "genuinely_independent_evidence_exists": independent,
        "evidence_state": {
            "label": _contract.evidence_label(),
            "fresh_unseen_evidence_exists":
                _contract.FRESH_UNSEEN_EVIDENCE_EXISTS,
            "reason": _contract.FRESH_UNSEEN_EVIDENCE_REASON,
            "a_fold_may_be_called_a_lockbox":
                _contract.A_FOLD_MAY_BE_CALLED_A_LOCKBOX,
            "verdict_ceiling_without_fresh_evidence":
                _contract.verdict_ceiling_without_fresh_evidence(),
        },
        "families": {
            "declared_acquired": list(_contract.ACQUIRED_FAMILIES),
            "declared_blocked": list(_contract.BLOCKED_FAMILIES),
            "measured": families_measured,
            "admitted_by_orthogonality": admitted,
            "redundant": redundant,
            "predictive_survivors": predictive_survivors,
            "economic_survivors": economic_survivors,
            "qualified": qualified,
        },
        "analyst_lane": {
            "acquisition_blocked": analyst.get("acquisition_blocked"),
            "blocking_reason": analyst.get("blocking_reason"),
            "purchase_state": (analyst.get("purchase_gate") or {}).get("state"),
            "purchase_terminal": ((analyst.get("adequacy") or {})
                                  .get("purchase_decision") or {})
            .get("purchase_terminal"),
            "statistical_evidence_claimed":
                analyst.get("statistical_evidence_claimed"),
            "money_spent_usd": 0.0,
        },
        "standalone_versus_incremental": {
            "per_family": {
                name: {
                    "standalone_rank_ic": spec.get("rank_ic"),
                    "standalone_rank_ic_t": spec.get("rank_ic_t"),
                    "standalone_scored_on": spec.get("scored_on"),
                    "best_increment_over_base": _best_increment(increments,
                                                                name),
                }
                for name, spec in (standalone.get("per_family") or {}).items()},
            "reading": (
                "a family whose standalone rank IC is significant and whose "
                "increment over the base information set is not has told the "
                "release its answer: the information is real and the base set "
                "already contained everything in it that predicts. This is the "
                "distinction the release exists to draw, and it is why a "
                "significant standalone t-statistic is not a finding here"),
        },
        "denominator": len(executed_configs),
        "planned_config_total": _contract.PLANNED_CONFIG_TOTAL,
        "max_primary_configs": _contract.MAX_PRIMARY_CONFIGS,
        "qualified_candidates": qualified,
        "blocking_reasons": blocking,
        "acquisition_summary": {
            "sources_ok": acquisition.get("sources_ok"),
            "sources_failed": acquisition.get("sources_failed"),
            "payload_count": acquisition.get("payload_count"),
            "total_bytes": acquisition.get("total_bytes"),
            "money_spent": acquisition.get("money_spent"),
        },
        "forward_handoff": {
            "forward_evidence_owner": _contract.FORWARD_EVIDENCE_OWNER,
            "may_register_forward_candidate":
                _contract.MAY_REGISTER_FORWARD_CANDIDATE,
            "may_create_second_true_forward_store":
                _contract.MAY_CREATE_SECOND_TRUE_FORWARD_STORE,
            "registered_anything": False,
            "what_a_survivor_would_need": (
                "registration through the canonical forward-evidence owner "
                "under manual review, then a period of genuinely independent "
                "forward outcomes. Release 35 prepares that handoff and does "
                "not take it"),
        },
        "r34_base": {
            "campaign": "r34_prediction_to_pnl_v2",
            "verdict": "R34_PREDICTION_DOES_NOT_CONVERT",
            "base_information_set": _contract.BASE_INFORMATION_SET,
            "conversion": dict(
                _contract.FROZEN_CONVERSION,
                horizons=list(_contract.FROZEN_CONVERSION["horizons"])),
        },
    }
    body = r35.artifact_body(VERDICT_SCHEMA, payload)
    body["verdict_hash"] = r35.sha(_clean(payload))
    return body


# --------------------------------------------------------------------------- #
# The campaign
# --------------------------------------------------------------------------- #
def run(*, campaign_id: str = _contract.CAMPAIGN_ID,
        acquire: bool = True, verbose: bool = True,
        enumeration_cache: Optional[Path] = None) -> dict:
    """Run the bounded Release-35 orthogonal-information campaign end to end."""
    created_at = _now()
    root = r35.campaign_dir(campaign_id)
    root.mkdir(parents=True, exist_ok=True)
    _say(verbose, "Release 35 - orthogonal information acquisition")
    _say(verbose, "  campaign %s" % campaign_id)
    _say(verbose, "  root     %s" % root)

    # 1 - contract, frozen before anything is measured
    contract_body = _contract.build(campaign_id=campaign_id,
                                    created_at=created_at)
    _contract.freeze(contract_body)
    _say(verbose, "  contract hash %s" % contract_body["contract_hash"][:16])

    # 2 - acquisition
    _say(verbose, "  acquiring free public sources ...")
    results = (_acq.acquire_all() if acquire else _acq.cached_results())
    manifest = _acq.manifest_artifact(results, campaign_id=campaign_id,
                                      created_at=created_at)
    _acq.freeze(manifest)
    _say(verbose, "    %d payloads, %.1f MB, $%.2f spent"
         % (manifest["payload_count"], manifest["total_bytes"] / 1e6,
            manifest["money_spent"]))

    # 3 - normalise
    _say(verbose, "  normalising to point-in-time series ...")
    loaded = load_information(results=results, verbose=verbose)

    # 4 - the frozen R34 universe and panel, byte-identical
    _say(verbose, "  rebuilding the frozen R34 universe and panel ...")
    built_universe = _r34_universe.build(
        cache=Path(enumeration_cache or R34_ENUMERATION_CACHE))
    panel = _r34_panel.build(built_universe)
    _say(verbose, "    %d instruments, %d sessions"
         % (len(panel["symbols"]), len(panel["calendar"])))

    # 5 - the new features
    _say(verbose, "  building new information features ...")
    built = _features.build_all(
        panel=panel, cot_frame=loaded["cot"].get("frame"),
        fred=loaded["fred"].get("series") or {},
        cboe=loaded["cboe"].get("series") or {},
        curve=loaded["curve"].get("frame"),
        insider_table=loaded["insider"].get("frame"))

    base_ctxs = {h: _r34_campaign.build_context(panel, horizon=h)
                 for h in _contract.HORIZONS}
    primary_ctx = base_ctxs[_contract.PRIMARY_HORIZON]
    coverage_by_family = _features.coverage(
        built, evaluation_dates=primary_ctx["udates"])
    for name, spec in sorted(coverage_by_family.items()):
        _say(verbose, "    %-38s rows=%.3f dates=%s instruments=%s from %s"
             % (name, spec.get("row_coverage") or 0.0,
                spec.get("dates_with_any_value"),
                spec.get("instruments_with_any_value"),
                spec.get("first_usable_date")))

    integrity_violations = sorted(
        name for name, spec in coverage_by_family.items()
        if spec.get("ok") and (
            (spec.get("row_coverage") or 0.0) < _contract.MIN_FAMILY_ROW_COVERAGE
            or (spec.get("dates_with_any_value") or 0)
            < _contract.MIN_FAMILY_EVALUATION_DATES))
    usable = {name: family for name, family in built.items()
              if coverage_by_family.get(name, {}).get("ok")
              and name not in integrity_violations}

    coverage_body = _info.coverage_artifact(
        campaign_id=campaign_id, created_at=created_at,
        sources=source_summary(results, loaded),
        families=coverage_by_family)
    coverage_body["under_covered_families"] = integrity_violations
    coverage_body["under_covered_rule"] = (
        "a family below the frozen row-coverage or evaluation-date floor is "
        "recorded and excluded from the predictive stage; it has not been "
        "tested and is not reported as a null")
    coverage_body["integrity_violations"] = []
    _info.freeze(coverage_body)

    _features.freeze(_features.registry_artifact(
        campaign_id=campaign_id, created_at=created_at, built=built,
        coverage_report=coverage_by_family))

    # 6 - Lane A, in parallel with the rest rather than blocking it
    _say(verbose, "  Lane A: analyst expectation change ...")
    analyst = _analyst_lane.run()
    _analyst_lane.freeze(_analyst_lane.artifact(
        campaign_id=campaign_id, created_at=created_at, result=analyst))
    _say(verbose, "    blocked=%s  purchase=%s"
         % (analyst["acquisition_blocked"],
            analyst["purchase_gate"]["state"]))

    # 7 - orthogonality, on TRAINING rows, before prediction
    _say(verbose, "  measuring distinctness against the 28 base features ...")
    orthogonality = run_orthogonality(primary_ctx, usable, verbose=verbose)
    _orthogonality.freeze(_orthogonality.report_artifact(
        campaign_id=campaign_id, created_at=created_at,
        families=orthogonality, coverage=coverage_by_family,
        base_feature_names=list(_r33_features.FEATURE_NAMES)))
    admitted = {name: family for name, family in usable.items()
                if orthogonality.get(name, {}).get(
                    "admitted_to_predictive_stage")}
    _say(verbose, "    admitted %d of %d families"
         % (len(admitted), len(usable)))

    # 8 - the predictive increment
    _say(verbose, "  measuring the paired predictive increment ...")
    increments = (run_increments(base_ctxs, admitted, verbose=verbose)
                  if admitted else
                  {"by_horizon": {}, "executed": [], "base_cache": {},
                   "sets": {}})
    standalone = (run_standalone(base_ctxs, admitted, verbose=verbose)
                  if admitted else {"per_family": {}, "executed": []})
    executed = list(increments["executed"]) + list(standalone["executed"])

    lag_sensitivity = {"state": "NOT_APPLICABLE"}
    if _contract.FAM_POSITIONING in admitted:
        _say(verbose, "  publication-lag sensitivity (positioning only) ...")
        lag_sensitivity = run_publication_lag_sensitivity(
            base_ctxs, panel, loaded, increments, verbose=verbose)

    # 9 - the economic increment, for survivors only
    survivors = sorted({
        record["information_set"]
        for horizon, block in increments["by_horizon"].items()
        for record in block["per_set"].values()
        if record["gate"]["passed"]
        and horizon == _contract.PRIMARY_HORIZON})
    _say(verbose, "  economic conversion: BASE + %d survivor(s)"
         % len(survivors))
    economics, economic_executed = run_economics(
        panel=panel, base_ctxs=base_ctxs, built=admitted,
        increments=increments, survivors=survivors, verbose=verbose)
    executed.extend(economic_executed)

    increment_body = _incremental.artifact(
        campaign_id=campaign_id, created_at=created_at,
        horizon_results=_clean(increments["by_horizon"]),
        standalone=_clean(standalone["per_family"]),
        executed_configs=_clean(executed))
    increment_body["publication_lag_sensitivity"] = _clean(lag_sensitivity)
    _incremental.freeze(increment_body)

    _write(campaign_id, "economics", r35.artifact_body(ECONOMICS_SCHEMA, {
        "campaign_id": campaign_id, "created_at": created_at,
        "calculation_owner": CALCULATION_OWNER,
        "frozen_conversion": dict(
            _contract.FROZEN_CONVERSION,
            horizons=list(_contract.FROZEN_CONVERSION["horizons"])),
        "conversion_is_frozen_not_searched":
            not _contract.CONVERSION_LAYER_SEARCH_ALLOWED,
        "primary_statistic": _contract.PRIMARY_ECONOMIC_INCREMENT,
        "control": _contract.ECONOMIC_CONTROL,
        "cost_base": _contract.COST_BASE,
        "arms": economics,
        "survivors_converted": survivors,
        "why_only_survivors": (
            "the economic stage is expensive and answers a question the "
            "predictive stage has already closed for a family with no "
            "incremental prediction; the BASE arm always runs because without "
            "it there is no increment to speak of"),
    }))

    # 10 - multiple testing, counting everything executed
    multiple_testing = run_multiple_testing(executed)
    _write(campaign_id, "multiple_testing",
           r35.artifact_body(MULTIPLE_TESTING_SCHEMA, dict(
               multiple_testing, campaign_id=campaign_id,
               created_at=created_at, calculation_owner=CALCULATION_OWNER)))

    # 11 - verdict
    verdict = build_verdict(
        campaign_id=campaign_id, created_at=created_at,
        contract_body=contract_body, acquisition=manifest,
        coverage=coverage_body, orthogonality=orthogonality,
        increments=increments, standalone=standalone, economics=economics,
        multiple_testing=multiple_testing, analyst=analyst,
        executed_configs=executed)
    _write(campaign_id, "verdict", verdict)

    _say(verbose, "")
    _say(verbose, "  VERDICT              %s" % verdict["primary_verdict"])
    _say(verbose, "  SYSTEM_RESULT        %s" % verdict["SYSTEM_RESULT"])
    _say(verbose, "  RESEARCH_CANDIDATE   %s"
         % verdict["RESEARCH_CANDIDATE_RESULT"])
    _say(verbose, "  ALPHA_RESULT         %s" % verdict["ALPHA_RESULT"])
    _say(verbose, "  executed configs     %d (ceiling %d)"
         % (len(executed), _contract.MAX_PRIMARY_CONFIGS))
    return {"campaign_id": campaign_id, "root": str(root),
            "verdict": verdict, "executed": executed,
            "orthogonality": orthogonality, "increments": increments,
            "economics": economics, "analyst": analyst,
            "standalone": standalone, "coverage": coverage_by_family,
            "publication_lag_sensitivity": lag_sensitivity}


def run_economics(*, panel: dict, base_ctxs: dict, built: dict,
                  increments: dict, survivors: list, verbose: bool = False
                  ) -> tuple:
    """The BASE arm always, and each predictive survivor once."""
    frames = _features.frames(built)
    sets = increments.get("sets") or {}
    base_cache = increments.get("base_cache") or {}
    primary = _contract.FROZEN_CONVERSION["primary_horizon"]
    if primary not in base_cache:
        return {}, []

    cov_by_date = _portfolio.trailing_covariance(
        panel["log_returns"], base_ctxs[primary]["udates"])

    conversion_horizons = [h for h in _contract.FROZEN_CONVERSION["horizons"]
                           if h in base_cache]
    base_ctx_by_h = {h: base_ctxs[h] for h in conversion_horizons}
    base_forecasts_by_h = {h: base_cache[h]["forecasts"]
                           for h in conversion_horizons}
    base_model_by_h = {h: base_cache[h]["model_key"]
                       for h in conversion_horizons}

    arms, executed = {}, []
    base_run = run_economic_arm(label="BASE", ctx_by_h=base_ctx_by_h,
                                forecasts_by_h=base_forecasts_by_h,
                                model_by_h=base_model_by_h,
                                cov_by_date=cov_by_date)
    arms["BASE"] = {"record": _clean(base_run["record"]),
                    "is_reference_arm": True}
    executed.append({
        "configuration_id": "ECONOMIC::BASE",
        "family": "ECONOMIC_CONVERSION", "information_set": "BASE",
        "horizon": primary,
        "after_cost_excess_annualised":
            base_run["record"].get("after_cost_excess_annualised"),
        "t_stat": base_run["record"].get("after_cost_excess_t_stat"),
        "p_value": None, "passed_gate": None})
    _say(verbose, "    BASE net excess over control = %s (t=%s)"
         % (base_run["record"].get("after_cost_excess_annualised"),
            base_run["record"].get("after_cost_excess_t_stat")))

    for name in survivors:
        feature_names = sets.get(name) or []
        arm_ctx_by_h = {h: _design.augment_context(base_ctxs[h], frames=frames,
                                                   feature_names=feature_names)
                        for h in conversion_horizons}
        arm_forecasts = {h: _r34_campaign.run_forecasts(arm_ctx_by_h[h])
                         for h in conversion_horizons}
        arm_run = run_economic_arm(
            label=name, ctx_by_h=arm_ctx_by_h, forecasts_by_h=arm_forecasts,
            model_by_h=base_model_by_h, cov_by_date=cov_by_date)
        increment = paired_economic_increment(
            base_run.get("result") or {}, arm_run.get("result") or {},
            horizon=primary)
        concentration = None
        if (arm_run.get("result") or {}).get("state") == "OK":
            concentration = _concentration.analyse(
                arm_run["result"]["path"], arm_run["result"]["control"],
                meta=base_ctxs[primary]["meta"], horizon=primary)
        gate = economic_gate(increment, arm_run["record"], concentration)
        arms[name] = {"record": _clean(arm_run["record"]),
                      "increment_over_base": _clean(increment),
                      "concentration": _clean(concentration),
                      "gate": gate, "is_reference_arm": False}
        executed.append({
            "configuration_id": "ECONOMIC::%s" % name,
            "family": "ECONOMIC_CONVERSION", "information_set": name,
            "horizon": primary,
            "after_cost_excess_annualised":
                arm_run["record"].get("after_cost_excess_annualised"),
            "increment": increment.get("increment_annualised"),
            "t_stat": increment.get("increment_t"),
            "p_value": (_mt.two_sided_p(increment["increment_t"])
                        if increment.get("increment_t") is not None
                        and np.isfinite(increment["increment_t"]) else None),
            "passed_gate": gate["passed"]})
        _say(verbose, "    %-36s increment=%s t=%s %s"
             % (name, increment.get("increment_annualised"),
                increment.get("increment_t"),
                "PASS" if gate["passed"] else "fail"))
    return arms, executed


def run_multiple_testing(executed: list) -> dict:
    """Benjamini-Hochberg over every executed configuration that has a p-value.

    Split by DIRECTION. A two-sided rejection can be a significant LOSS, and
    reporting "n of m survived" without that split is how R34's v1 nearly
    reported a candidate that significantly underperformed its control as a
    survivor. Only the positive list can support a qualification.
    """
    rows = [row for row in executed if row.get("p_value") is not None]
    p_values = [row["p_value"] for row in rows]
    bh = _mt.benjamini_hochberg(p_values, q=_contract.FDR_Q)
    rejected = set(bh.get("rejected") or [])

    def _direction(row) -> float:
        for key in ("increment", "after_cost_excess_annualised", "t_stat"):
            value = row.get(key)
            if value is not None and np.isfinite(float(value)):
                return float(value)
        return 0.0

    beating = sorted(rows[i]["configuration_id"] for i in rejected
                     if _direction(rows[i]) > 0)
    losing = sorted(rows[i]["configuration_id"] for i in rejected
                    if _direction(rows[i]) <= 0)
    return {
        "policy": {
            "denominator_counts_all_executed":
                _contract.DENOMINATOR_COUNTS_ALL_EXECUTED,
            "controls_enter_denominator":
                _contract.CONTROLS_ENTER_DENOMINATOR,
            "only_positive_rejections_may_qualify":
                _contract.ONLY_POSITIVE_REJECTIONS_MAY_QUALIFY,
            "fdr_q": _contract.FDR_Q,
        },
        "denominator_executed_configurations": len(executed),
        "configurations_with_a_p_value": len(rows),
        "benjamini_hochberg": {
            **bh,
            "rejected_beating_the_base": beating,
            "rejected_losing_to_the_base": losing,
            "n_rejected_beating": len(beating),
            "n_rejected_losing": len(losing),
        },
        "per_configuration": [
            {k: v for k, v in row.items() if k != "_"} for row in executed],
    }


__all__ = ["CALCULATION_OWNER", "ARTIFACTS", "run", "build_verdict",
           "run_increments", "run_standalone", "run_orthogonality",
           "run_economics", "run_multiple_testing", "training_row_mask",
           "paired_economic_increment", "economic_gate", "load_information",
           "source_summary", "run_economic_arm"]
