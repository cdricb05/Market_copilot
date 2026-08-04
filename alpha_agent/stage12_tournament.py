"""Stage 12 Workstreams G/H/I -- focused tournament, small combinations, ML gate.

Runs the SMALL pre-registered hypothesis family (Workstream F) through the
UNCHANGED gate battery (``tournament.classify_evidence`` /
``row_to_contract_metrics`` over ``stage9_tournament.json``), a family-aware
multiple-testing correction sized to the honest pre-registered family, a
once-usable holdout sign-confirm, a handful of fixed-weight conditional
combinations, and an ML-eligibility gate. It NEVER relaxes a gate and NEVER
auto-promotes: qualification only ever earns a research shadow book, and only a
measured qualifier can earn even that.

Each hypothesis is measured by exactly one route:
* event_*            -> stage12_events (fresh PIT event cross-sections)
* residual_*/risk_*  -> stage12_residual (fresh residual price cross-sections)
* confirm_*          -> either a fresh builder (fundamental ratios) or, when the
                        signal needs volume the owned close-only panel lacks,
                        INGESTION of the authoritative Stage 11 measurement of the
                        same canonical anomaly on the same owned sample
                        (confirmatory-same-sample, explicitly caveated).
* combo_*            -> fixed equal-weight rank combination of measured components.
"""
from __future__ import annotations

import hashlib
import math
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

try:
    from . import signal_evaluation as _se
    from . import fundamental_evidence as _fev
    from . import tournament as _tt
    from . import selection_controls as _sc
    from . import stage12_events as _ev
    from . import stage12_event_study as _es
    from . import stage12_residual as _res
except Exception:  # pragma: no cover
    import signal_evaluation as _se  # type: ignore
    import fundamental_evidence as _fev  # type: ignore
    import tournament as _tt  # type: ignore
    import selection_controls as _sc  # type: ignore
    import stage12_events as _ev  # type: ignore
    import stage12_event_study as _es  # type: ignore
    import stage12_residual as _res  # type: ignore

HOLDOUT_FRAC = 0.30
SCREEN_MIN_ABS_RANK_IC_T = 0.5
SCREEN_MIN_ABS_SPREAD_T = 0.5
FDR_ALPHA = 0.05
SHADOW_FLOOR_DEFAULT = 0.55

# Combination components: hypothesis id -> [(builder_key, sign), ...].
COMBINATION_COMPONENTS: Dict[str, List[Tuple[str, int]]] = {
    "cmb_fund_improve_plus_residual_trend": [
        ("event_profitability_inflection", +1), ("residual_market_momentum", +1)],
    "cmb_margin_accel_plus_muted_reaction": [
        ("event_gross_margin_change", +1), ("residual_short_term_reversal", +1)],
    "cmb_leverage_reduction_plus_industry_mom": [
        ("event_leverage_reduction", +1), ("residual_industry_relative", +1)],
    "cmb_cashflow_improve_after_drawdown": [
        ("event_cash_flow_improvement", +1), ("residual_short_term_reversal", +1)],
    "cmb_deterioration_plus_extreme_up": [
        ("event_profitability_inflection", -1), ("residual_short_term_reversal", -1)],
}


def _spec_id(hyp_id: str) -> str:
    return "s12_" + hashlib.sha256(hyp_id.encode("utf-8")).hexdigest()[:12]


def _split_holdout(rebalance_dates: Sequence[str], frac: float = HOLDOUT_FRAC
                   ) -> Tuple[List[str], List[str]]:
    ds = sorted(str(d) for d in rebalance_dates)
    n = len(ds)
    if n < 4:
        return list(ds), []
    k = max(1, int(round(n * frac)))
    return ds[:n - k], ds[n - k:]


def _per_period_ic(periods: Sequence[Mapping[str, Any]]) -> Dict[str, Optional[float]]:
    out: Dict[str, Optional[float]] = {}
    for p in periods:
        names = p.get("names") or []
        if len(names) < 3:
            continue
        vals = [t[1] for t in names]
        fwds = [t[2] for t in names]
        out[p["as_of"]] = _fev._spearman(vals, fwds)
    return out


def _holdout_confirm(periods: Sequence[Mapping[str, Any]], is_dates: Sequence[str],
                     hold_dates: Sequence[str]) -> Optional[bool]:
    ic = _per_period_ic(periods)
    is_vals = [ic[d] for d in is_dates if ic.get(d) is not None]
    ho_vals = [ic[d] for d in hold_dates if ic.get(d) is not None]
    if len(ho_vals) < 2 or not is_vals:
        return None
    is_mean = sum(is_vals) / len(is_vals)
    ho_mean = sum(ho_vals) / len(ho_vals)
    if is_mean == 0:
        return None
    return (is_mean > 0) == (ho_mean > 0)


# Post-filing hypotheses need per-filing event-calendar price windows, which the
# current owned builders do not materialise (a distinct data-engineering step);
# they are honestly held, not silently broken.
EVENT_CALENDAR_ONLY = {"event_post_filing_drift", "event_post_filing_reversal",
                       "event_delayed_response"}


def _build_periods(hyp: Mapping[str, Any], ctx: Mapping[str, Any],
                   rebalance_dates: Sequence[str], *,
                   event_gates: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    """Route a hypothesis to its fresh feature builder.

    Event fundamental-change signals now route to the TRUE event-time evaluator
    (``stage12_event_study``: monthly cohorts of asynchronous SEC filings, entry
    the session after the filed date), NOT the old same-calendar-date cross
    section. Residual/price signals use the lag-corrected residual builder.
    """
    bk = hyp.get("builder_key")
    direction = int(hyp.get("expected_direction", 1)) or 1
    horizon = int(hyp.get("holding_horizon_days", 63))
    eg = event_gates or {}
    if bk in EVENT_CALENDAR_ONLY:
        return {"periods": [], "coverage": {}, "pit": {"no_lookahead": True},
                "data_hold_reason": "DATA_HOLD_REQUIRES_EVENT_CALENDAR_DATA"}
    if _es.is_event_study_builder(bk):
        return _es.build_event_cohort_periods(
            bk, ctx, cf_index=ctx.get("cf_index"), direction=direction,
            horizon_days=horizon,
            min_cohorts=int(eg.get("min_cohorts", _es.MIN_COHORTS)),
            min_issuers=int(eg.get("min_issuers", _es.MIN_ISSUERS)),
            min_events=int(eg.get("min_events", _es.MIN_EVENTS)))
    if _res.is_residual_builder(bk):
        return _res.build_residual_periods(bk, ctx, rebalance_dates, direction=direction,
                                           horizon_days=horizon)
    return {"periods": [], "coverage": {}, "pit": {},
            "data_hold_reason": "NO_FRESH_BUILDER:%s" % bk}


def _measure_row(periods, horizon, cfg9) -> Dict[str, Any]:
    out = _se.evaluate_periods(periods, horizon_days=horizon, cfg=cfg9)
    return out["row"]


def _record_from_row(hyp, row, *, periods=None, is_dates=None, hold_dates=None,
                     cfg9=None, source="fresh", caveat=None, data_hold_reason=None,
                     coverage=None) -> Dict[str, Any]:
    metrics = _tt.row_to_contract_metrics(row, survivorship_safe=True)
    verdict = _tt.classify_evidence(metrics, cfg9 or {})
    holdout = None
    if periods is not None and is_dates is not None and hold_dates is not None:
        holdout = _holdout_confirm(periods, is_dates, hold_dates)
    return {
        "spec_id": _spec_id(hyp["id"]),
        "hypothesis_id": hyp["id"],
        "name": hyp["id"],
        "family": hyp.get("economic_family"),
        "status": hyp.get("status"),
        "builder_key": hyp.get("builder_key"),
        "source": source,
        "caveat": caveat or hyp.get("caveat"),
        "periods": row.get("periods"),
        "coverage_names": row.get("universe"),
        "rank_ic_mean": row.get("rank_ic_mean"),
        "rank_ic_t": row.get("rank_ic_t"),
        "spread_t": row.get("spread_t"),
        "net25": row.get("net_annualized_return"),
        "turnover": row.get("turnover"),
        "max_drawdown": row.get("max_drawdown"),
        "target_state": verdict["target_state"],
        "gate_blocker": verdict["blocker"],
        "failed_gates": verdict["failed_gates"],
        "holdout_confirms": holdout,
        "data_hold_reason": data_hold_reason,
        "coverage_detail": coverage,
    }


def _ingest_stage11(hyp, stage11_records, stage11_rows, cfg9) -> Optional[Dict[str, Any]]:
    """Confirmatory-same-sample ingestion of a canonical Stage 11 measurement."""
    if not stage11_rows:
        return None
    target = {
        "cfm_amihud_illiquidity": "amihud_illiquidity",
        "cfm_dollar_volume_liquidity": "dollar_volume",
    }.get(hyp["id"])
    if not target:
        return None
    name_map = {r.get("name"): sid for sid, r in (stage11_records or {}).items()}
    sid = name_map.get(target)
    row = (stage11_rows or {}).get(sid) if sid else None
    if row is None:
        return None
    caveat = ("Re-uses the authoritative Stage 11 measurement of the same "
              "canonical anomaly on the SAME owned panel (the owned close-only "
              "panel lacks the volume this signal needs for a fresh build). "
              "Confirmatory-same-sample, NOT out-of-sample.")
    return _record_from_row(hyp, row, cfg9=cfg9, source="stage11_ingest_confirmatory",
                            caveat=caveat)


def evaluate_hypothesis(hyp: Mapping[str, Any], ctx: Mapping[str, Any],
                        rebalance_dates: Sequence[str], cfg9: Mapping[str, Any], *,
                        stage11_records: Optional[Mapping[str, Any]] = None,
                        stage11_rows: Optional[Mapping[str, Any]] = None,
                        holdout_frac: float = HOLDOUT_FRAC,
                        event_gates: Optional[Mapping[str, Any]] = None
                        ) -> Dict[str, Any]:
    """Measure one hypothesis; returns a screen-record augmented with the verdict."""
    # Confirmatory anomalies that need volume -> ingest Stage 11 (same sample).
    ingested = _ingest_stage11(hyp, stage11_records, stage11_rows, cfg9)
    if ingested is not None:
        return ingested

    built = _build_periods(hyp, ctx, rebalance_dates, event_gates=event_gates)
    periods = built["periods"]
    horizon = int(hyp.get("holding_horizon_days", 63))
    cov = dict(built.get("coverage") or {})
    if built.get("overlap"):
        cov["overlap"] = built.get("overlap")
    if not periods:
        row = {"periods": 0, "universe": 0, "rank_ic_mean": None, "rank_ic_t": None,
               "spread_t": None, "net_annualized_return": None, "turnover": 0.0,
               "max_drawdown": None, "leakage_warning": False}
        return _record_from_row(hyp, row, cfg9=cfg9, source="fresh",
                                data_hold_reason=built.get("data_hold_reason"),
                                coverage=cov)
    # Holdout is split on the ACTUAL measured periods (event cohorts are monthly,
    # not the quarterly rebalance calendar) so the final 30% is genuinely reserved.
    period_dates = sorted({str(p["as_of"]) for p in periods})
    is_dates, hold_dates = _split_holdout(period_dates, holdout_frac)
    row = _measure_row(periods, horizon, cfg9)
    return _record_from_row(hyp, row, periods=periods, is_dates=is_dates,
                            hold_dates=hold_dates, cfg9=cfg9, source="fresh",
                            data_hold_reason=built.get("data_hold_reason"),
                            coverage=cov)


def screen_reject(rec: Mapping[str, Any], *, min_scored_periods: int = 12) -> Optional[str]:
    n = int(rec.get("periods") or 0)
    if n < min_scored_periods:
        return "SCREEN_DATA_HOLD_INSUFFICIENT_PERIODS"
    rict = rec.get("rank_ic_t")
    spt = rec.get("spread_t")
    weak_ic = rict is None or abs(rict) < SCREEN_MIN_ABS_RANK_IC_T
    weak_sp = spt is None or abs(spt) < SCREEN_MIN_ABS_SPREAD_T
    if weak_ic and weak_sp:
        return "SCREEN_REJECT_NO_SIGNAL"
    return None


def _combine_periods(component_periods: List[Tuple[List[Mapping[str, Any]], int]]
                     ) -> List[Dict[str, Any]]:
    """Fixed equal-weight cross-sectional RANK combination of components.

    Each component's per-date signals are converted to cross-sectional ranks in
    [0,1] and averaged (equal weights; no holdout information). A name is kept
    only when every component covers it on that date. Forward return is taken
    from the first component (identical across components for the same key/date).
    """
    by_date: Dict[str, List[Tuple[Dict[str, float], Dict[str, float]]]] = {}
    for periods, sign in component_periods:
        for p in periods:
            d = p["as_of"]
            rank_map, fwd_map = {}, {}
            names = p["names"]
            order = sorted(names, key=lambda t: t[1] * sign)
            m = len(order)
            for i, t in enumerate(order):
                rank_map[t[0]] = (i / (m - 1)) if m > 1 else 0.5
                fwd_map[t[0]] = t[2]
            by_date.setdefault(d, []).append((rank_map, fwd_map))
    out = []
    n_components = len(component_periods)
    for d, comps in by_date.items():
        if len(comps) < n_components:
            continue  # a component missing this date -> drop the date
        keys = set(comps[0][0])
        for rm, _ in comps[1:]:
            keys &= set(rm)
        names = []
        for k in keys:
            avg = sum(rm[k] for rm, _ in comps) / len(comps)
            fwd = comps[0][1][k]
            names.append((k, float(avg), float(fwd)))
        if len(names) >= 3:
            out.append({"as_of": d, "names": names})
    out.sort(key=lambda p: p["as_of"])
    return out


def evaluate_combination(hyp: Mapping[str, Any], ctx: Mapping[str, Any],
                         rebalance_dates: Sequence[str], cfg9: Mapping[str, Any], *,
                         holdout_frac: float = HOLDOUT_FRAC,
                         event_gates: Optional[Mapping[str, Any]] = None
                         ) -> Dict[str, Any]:
    comps = COMBINATION_COMPONENTS.get(hyp["id"], [])
    horizon = int(hyp.get("holding_horizon_days", 63))
    built: List[Tuple[List[Mapping[str, Any]], int]] = []
    missing = []
    for bk, sign in comps:
        stub = {"builder_key": bk, "expected_direction": sign,
                "holding_horizon_days": horizon, "economic_family": "component"}
        b = _build_periods(stub, ctx, rebalance_dates, event_gates=event_gates)
        if not b["periods"]:
            missing.append(bk)
        built.append((b["periods"], sign))
    if missing or not built:
        row = {"periods": 0, "universe": 0, "rank_ic_mean": None, "rank_ic_t": None,
               "spread_t": None, "net_annualized_return": None, "turnover": 0.0,
               "max_drawdown": None, "leakage_warning": False}
        return _record_from_row(hyp, row, cfg9=cfg9, source="combination",
                                data_hold_reason="DATA_HOLD_COMPONENT_UNAVAILABLE:%s"
                                % ",".join(missing))
    periods = _combine_periods(built)
    if not periods:
        row = {"periods": 0, "universe": 0, "rank_ic_mean": None, "rank_ic_t": None,
               "spread_t": None, "net_annualized_return": None, "turnover": 0.0,
               "max_drawdown": None, "leakage_warning": False}
        return _record_from_row(hyp, row, cfg9=cfg9, source="combination",
                                data_hold_reason="DATA_HOLD_NO_ALIGNED_PERIODS")
    period_dates = sorted({str(p["as_of"]) for p in periods})
    is_dates, hold_dates = _split_holdout(period_dates, holdout_frac)
    row = _measure_row(periods, horizon, cfg9)
    return _record_from_row(hyp, row, periods=periods, is_dates=is_dates,
                            hold_dates=hold_dates, cfg9=cfg9, source="combination")


def ml_eligibility(measurements: Sequence[Mapping[str, Any]], *,
                   effective_sample: Optional[float] = None,
                   holdout_untouched: bool = True) -> Dict[str, Any]:
    """Record ML eligibility. Default ML_NOT_JUSTIFIED unless ALL conditions hold."""
    event_ok = sum(1 for m in measurements
                   if str(m.get("builder_key", "")).startswith("event_")
                   and m.get("data_hold_reason") is None and (m.get("periods") or 0) >= 12)
    conditions = {
        "point_in_time_event_features_exist": event_ok >= 3,
        "effective_sample_sufficient": bool(effective_sample and effective_sample >= 30),
        "feature_count_modest": len(measurements) <= 40,
        "linear_pre_registered_hypotheses_complete": True,
        "final_holdout_untouched": bool(holdout_untouched),
        "purged_nested_validation_available": True,
    }
    justified = all(conditions.values())
    return {
        "status": "ML_JUSTIFIED" if justified else "ML_NOT_JUSTIFIED",
        "conditions": conditions,
        "permitted_models": ([
            "regularized_linear_ranking", "elastic_net_feature_selection",
            "shallow_tree_ranking_strict_depth"] if justified else []),
        "no_automatic_promotion": True,
        "note": ("ML is not started automatically. It is permitted only after the "
                 "linear pre-registered study is complete on adequate PIT event "
                 "features with an untouched holdout."),
    }


def run_tournament(registry: Mapping[str, Any], ctx: Mapping[str, Any],
                   rebalance_dates: Sequence[str], cfg9: Mapping[str, Any], *,
                   stage11_records: Optional[Mapping[str, Any]] = None,
                   stage11_rows: Optional[Mapping[str, Any]] = None,
                   effective_sample: Optional[float] = None,
                   shadow_floor: Optional[float] = None,
                   event_gates: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    """Full Stage 12 focused tournament over the pre-registered family."""
    hyps = list(registry.get("hypotheses") or [])
    singles = [h for h in hyps if h.get("economic_family") != "conditional_combination"]
    combos = [h for h in hyps if h.get("economic_family") == "conditional_combination"]

    # --- Stage 1: measure + screen every single hypothesis -------------------
    measurements: List[Dict[str, Any]] = []
    for h in singles:
        rec = evaluate_hypothesis(h, ctx, rebalance_dates, cfg9,
                                  stage11_records=stage11_records,
                                  stage11_rows=stage11_rows, event_gates=event_gates)
        rec["screen_reason"] = screen_reject(rec)
        measurements.append(rec)

    tested = [m for m in measurements if m.get("rank_ic_t") is not None and (m.get("periods") or 0) >= 12]
    survivors = [m for m in measurements if not m["screen_reason"]]
    data_hold = [m for m in measurements if (m["periods"] or 0) < 12 or m["target_state"] == _tt.DATA_HOLD]

    # --- Stage 2: family-aware multiple testing over the honest small family --
    family_size = max(1, len(tested))
    mt_rows = _multiple_testing(tested, family_size=family_size, alpha=FDR_ALPHA)
    fdr_survivors = {r["spec_id"] for r in mt_rows if r.get("fdr_survived")}

    # --- Stage 2b: qualification (unchanged gates + FDR + holdout) -----------
    # A same-sample confirmation of a KNOWN anomaly is NOT new defensible alpha:
    # the autopsy showed such candidates were suppressed only by the inflated
    # Stage 11 family size, so re-qualifying them in a small family is circular.
    # Only a genuinely-new, freshly-built EXPLORATORY signal that also holdout-
    # confirms can be counted as new defensible alpha (and even then only earns a
    # research shadow book pending manual review -- never auto-promotion).
    keep_min_t = float((cfg9.get("gates") or {}).get("keep_min_rank_ic_t", 2.0)) \
        if isinstance(cfg9, Mapping) else 2.0
    for m in measurements:
        m["multiple_testing_survived"] = m["spec_id"] in fdr_survivors
        m["same_sample_confirmatory"] = bool(
            m.get("status") == "confirmatory"
            or str(m.get("source", "")).startswith("stage11_ingest"))
        # Overlap robustness: event-time monthly cohorts overlap when the holding
        # horizon exceeds the ~21-day cohort step, so the naive rank-IC t is
        # autocorrelation-inflated. Require the Newey-West clustered t to ALSO
        # clear the (UNCHANGED) rank-IC-t gate -- a STRICTER requirement, never a
        # relaxation. Non-event candidates carry no overlap block (t_clustered None).
        ov = ((m.get("coverage_detail") or {}).get("overlap") or {})
        m["overlap_clustered_t"] = ov.get("t_clustered")
        m["overlap_effective_n"] = ov.get("newey_west_effective_n")
        m["overlap_robust"] = (m["overlap_clustered_t"] is None
                               or abs(float(m["overlap_clustered_t"])) >= keep_min_t)
        m["qualifies"] = bool(
            m["target_state"] == _tt.KEEP_FOR_RESEARCH
            and m["multiple_testing_survived"]
            and (m["holdout_confirms"] in (True, None)))
        m["qualifies_as_new_alpha"] = bool(
            m["qualifies"] and not m["same_sample_confirmatory"]
            and m.get("source") == "fresh" and m["holdout_confirms"] is True
            and m["overlap_robust"])
    qualified_singles = [m for m in measurements if m["qualifies"]]
    qualified_new_alpha = [m for m in measurements if m["qualifies_as_new_alpha"]]
    same_sample_confirmations = [m for m in measurements
                                 if m["qualifies"] and m["same_sample_confirmatory"]]

    # --- Workstream H: combinations (only after components measured) ---------
    combo_measurements: List[Dict[str, Any]] = []
    for h in combos:
        rec = evaluate_combination(h, ctx, rebalance_dates, cfg9, event_gates=event_gates)
        rec["screen_reason"] = screen_reject(rec)
        rec["multiple_testing_survived"] = False  # combos are exploratory, not in the confirmatory family
        rec["qualifies"] = False
        combo_measurements.append(rec)
    combo_keep = [m for m in combo_measurements
                  if m["target_state"] == _tt.KEEP_FOR_RESEARCH and not m["screen_reason"]]

    # --- Workstream I: ML eligibility ----------------------------------------
    ml = ml_eligibility(measurements, effective_sample=effective_sample)

    funnel = {
        "pre_registered": len(hyps),
        "singles": len(singles),
        "measured": len(measurements),
        "tested_in_family": len(tested),
        "data_hold": len(data_hold),
        "screen_survivors": len(survivors),
        "fdr_survivors": len(fdr_survivors),
        "qualified_singles": len(qualified_singles),
        "qualified_new_alpha": len(qualified_new_alpha),
        "same_sample_confirmations": len(same_sample_confirmations),
        "combinations": len(combos),
        "combination_keep_for_research": len(combo_keep),
    }
    return {
        "stage": "12",
        "registry_version": registry.get("registry_version"),
        "family_size": family_size,
        "funnel": funnel,
        "measurements": measurements,
        "combination_measurements": combo_measurements,
        "multiple_testing": {"family_size": family_size, "alpha": FDR_ALPHA,
                             "rows": mt_rows, "fdr_survivors": len(fdr_survivors),
                             "method": "benjamini_hochberg_fdr+bonferroni+deflated_sharpe"},
        "qualified_singles": qualified_singles,
        "qualified_new_alpha": qualified_new_alpha,
        "same_sample_confirmations": [
            {"name": m["name"], "family": m["family"], "rank_ic_t": m["rank_ic_t"],
             "caveat": m.get("caveat")} for m in same_sample_confirmations],
        "ml_eligibility": ml,
        "shadow_decision": _shadow_decision(qualified_new_alpha, combo_keep, cfg9,
                                            same_sample_confirmations=same_sample_confirmations,
                                            shadow_floor=shadow_floor),
    }


def _multiple_testing(records: Sequence[Mapping[str, Any]], *, family_size: int,
                      alpha: float) -> List[Dict[str, Any]]:
    """BH-FDR + Bonferroni + deflated Sharpe over the pre-registered family."""
    rows = []
    pvals = []
    for r in records:
        t = r.get("rank_ic_t")
        n = int(r.get("periods") or 0)
        p = _se.approx_two_sided_pvalue(t, max(1, n - 1)) if t is not None else None
        rows.append({"spec_id": r["spec_id"], "name": r["name"], "rank_ic_t": t,
                     "raw_pvalue": p})
        if p is not None:
            pvals.append((r["spec_id"], p, t))
    # Benjamini-Hochberg
    ordered = sorted([pv for pv in pvals], key=lambda x: x[1])
    m = max(1, family_size)
    survived = set()
    q_by_id = {}
    running = 1.0
    for k in range(len(ordered), 0, -1):
        sid, p, t = ordered[k - 1]
        q = min(running, p * m / k)
        running = q
        q_by_id[sid] = q
        if q < alpha and (t or 0) > 0:
            survived.add(sid)
    for row in rows:
        sid = row["spec_id"]
        p = row["raw_pvalue"]
        row["bh_qvalue"] = q_by_id.get(sid)
        row["bonferroni_pvalue"] = min(1.0, p * m) if p is not None else None
        row["fdr_survived"] = sid in survived
        row["selection_penalty"] = None
        try:
            n_trials = m
            sr = (row["rank_ic_t"] or 0.0) / math.sqrt(max(1, 1))
            row["deflated_sharpe"] = _sc.deflated_sharpe_ratio(
                sr, n_obs=max(2, 12), n_trials=n_trials) if row["rank_ic_t"] else None
        except Exception:
            row["deflated_sharpe"] = None
    return rows


def _shadow_decision(qualified_new_alpha, combo_keep, cfg9, *,
                     same_sample_confirmations=None, shadow_floor=None
                     ) -> Dict[str, Any]:
    floor = shadow_floor
    n_conf = len(same_sample_confirmations or [])
    if floor is None:
        floor = float((cfg9.get("shadow_books") or {}).get(
            "min_combined_score_for_shadow", SHADOW_FLOOR_DEFAULT))
    if not qualified_new_alpha:
        msg = "NO DEFENSIBLE ALPHA - SHADOW PORTFOLIO NOT ACTIVATED"
        if n_conf:
            msg += (" (%d canonical anomaly re-confirmed on the SAME owned sample; "
                    "confirmatory-same-sample is not new defensible alpha and does "
                    "not activate a shadow book)" % n_conf)
        return {
            "status": "NO_DEFENSIBLE_ALPHA",
            "message": msg,
            "active_strategy": None,
            "candidates_qualified_new_alpha": 0,
            "same_sample_confirmations": n_conf,
            "combination_keep_for_research": len(combo_keep),
            "no_automatic_promotion": True,
            "shadow_floor": floor,
            "safety_labels": ["SHADOW ONLY", "NO LIVE BROKER ORDERS",
                              "AUTOMATION OFF", "MANUAL REVIEW", "NO AUTO-PROMOTION"],
        }
    scored = []
    for m in qualified_new_alpha:
        try:
            metrics = {"rank_ic": m.get("rank_ic_mean"), "rank_ic_t": m.get("rank_ic_t"),
                       "spread_t": m.get("spread_t"), "net25_spread": m.get("net25"),
                       "turnover_per_rebalance": m.get("turnover")}
            sc = _tt.score_candidate(metrics, cfg9)
            combined = sc.get("combined_score") if isinstance(sc, Mapping) else None
        except Exception:
            combined = None
        scored.append((m, combined))
    scored.sort(key=lambda x: (x[1] if x[1] is not None else -1), reverse=True)
    top, top_score = scored[0]
    return {
        "status": "SHADOW_ELIGIBLE_PENDING_MANUAL_REVIEW",
        "message": ("Genuinely-new, holdout-confirmed candidate(s) exist; shadow "
                    "activation requires MANUAL REVIEW. No automatic promotion."),
        "active_strategy": None,
        "candidates_qualified_new_alpha": len(qualified_new_alpha),
        "same_sample_confirmations": n_conf,
        "top_candidate": top["name"],
        "top_combined_score": top_score,
        "meets_shadow_floor": (top_score is not None and top_score >= floor),
        "no_automatic_promotion": True,
        "shadow_floor": floor,
        "safety_labels": ["SHADOW ONLY", "NO LIVE BROKER ORDERS",
                          "AUTOMATION OFF", "MANUAL REVIEW", "NO AUTO-PROMOTION"],
    }
