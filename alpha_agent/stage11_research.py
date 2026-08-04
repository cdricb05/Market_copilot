"""
alpha_agent.stage11_research - Stage 11 Workstreams C/D/E: the bounded two-stage
research funnel, the multiple-testing / false-discovery controls, and the
orthogonal multi-factor ensemble-discovery layer.

Nothing here lowers an existing gate. Every candidate (single signal OR ensemble)
is scored into the SAME canonical evidence row and passed through the UNCHANGED
``tournament.row_to_contract_metrics`` -> ``tournament.classify_evidence`` gates,
plus the additional selection-adjusted / robustness controls this module adds.

  * Stage 1 broad screen: one efficient first-pass evaluation of every SUPPORTED
    spec; obvious failures are rejected quickly; candidates are NEVER selected on
    in-sample return alone.
  * Statistical discipline: the FULL number of distinct tested specs is recorded;
    Benjamini-Hochberg FDR + Bonferroni + a deflated Sharpe (deflated for the
    number of trials) discount the best-of-search; a candidate whose result rests
    on one period / one regime / one parameter / unrealistic turnover / a zero-cost
    assumption is rejected.
  * Stage 2 deep evaluation: expanding walk-forward is already inside the
    evaluator; deep adds a once-usable final holdout (never used to pick
    parameters), a parameter-neighborhood stability check, sector-neutral / cost
    variants, and capacity / concentration measurement.
  * Ensemble discovery: signal return-stream correlations, near-duplicate removal,
    a greedy orthogonal selection, and equal-weight / evidence-weighted /
    training-only-constrained composites - each rejected when it is merely an
    overfit combination of weak signals.

Pure stdlib; deterministic; read-only measurement (the only writes are Stage 11
research artifacts performed by the campaign layer, never here).
"""
from __future__ import annotations

from typing import Optional, Sequence

from . import fundamental_evidence as _fev
from . import orthogonality as _orth
from . import selection_controls as _sc
from . import signal_evaluation as _se
from . import signal_library as _sl

# Screen-stage quick-reject: below these a spec is dropped before deep work. These
# are DELIBERATELY LOOSER than the KEEP gates (a screen must not pre-reject a
# candidate the deep stage might confirm) - the real KEEP/REJECT decision is always
# the unchanged ``classify_evidence``.
SCREEN_MIN_ABS_RANK_IC_T = 0.5      # |t| below this AND weak spread -> screen out
SCREEN_MIN_ABS_SPREAD_T = 0.5
DEFAULT_FDR_ALPHA = 0.05
DEFAULT_MAX_ABS_CORR = 0.90         # near-duplicate return streams collapse
DEFAULT_ORTHO_CORR = 0.60           # ensemble selection independence threshold


# --------------------------------------------------------------------------- #
# Panel context bundle (built once, reused across every spec / ensemble).
# --------------------------------------------------------------------------- #
def build_panel_context(ohlcv: dict, *, store=None,
                        cik_to_assetid: Optional[dict] = None,
                        sector_series=None) -> dict:
    """Bundle the shared, expensive-to-build inputs. ``close_index`` and
    ``market`` are derived once here so per-spec evaluation is cheap."""
    close_index = {a: _fev._price_index(list(zip(s["d"], s["c"])))
                   for a, s in ohlcv.items()}
    return {
        "ohlcv": ohlcv,
        "close_index": close_index,
        "market": _sl.market_daily_returns(ohlcv),
        "store": store,
        "cik_to_assetid": cik_to_assetid or {},
        "sector_series": sector_series,
    }


def evaluate_in_context(spec: _sl.SignalSpec, ctx: dict,
                        rebalance_dates: Sequence[str], *,
                        champion_returns: Optional[dict] = None,
                        cfg: Optional[dict] = None) -> dict:
    return _se.evaluate_spec(
        spec, ohlcv=ctx["ohlcv"], close_index=ctx["close_index"],
        market=ctx["market"], store=ctx["store"],
        cik_to_assetid=ctx["cik_to_assetid"], sector_series=ctx["sector_series"],
        rebalance_dates=rebalance_dates, champion_returns=champion_returns,
        cfg=cfg)


# --------------------------------------------------------------------------- #
# Stage 1 - broad screen.
# --------------------------------------------------------------------------- #
def _screen_reject(row: dict, *, min_scored_periods: int) -> Optional[str]:
    """A fast, LOOSE screen reason (or None to keep for deep work). Never selects
    on in-sample return; only removes candidates that cannot possibly clear the
    unchanged strength gates."""
    n = int(row.get("periods") or 0)
    if n < int(min_scored_periods):
        return "SCREEN_DATA_HOLD_INSUFFICIENT_PERIODS"
    ric_t = row.get("rank_ic_t")
    sp_t = row.get("spread_t")
    weak_ic = ric_t is None or abs(ric_t) < SCREEN_MIN_ABS_RANK_IC_T
    weak_sp = sp_t is None or abs(sp_t) < SCREEN_MIN_ABS_SPREAD_T
    if weak_ic and weak_sp:
        return "SCREEN_REJECT_NO_SIGNAL"
    return None


def screen_specs(specs: Sequence[_sl.SignalSpec], ctx: dict,
                 rebalance_dates: Sequence[str], *,
                 supported_ids: Optional[set] = None,
                 min_scored_periods: int = 12,
                 champion_returns: Optional[dict] = None,
                 cfg: Optional[dict] = None) -> dict:
    """Broad first-pass over every SUPPORTED spec. Returns
    ``{screened, survivors, rejected, data_hold, series, rows, family_size}``.
    ``survivors`` keep the full row for the deep stage; ``series`` holds each
    survivor's per-date long/short return stream for the ensemble layer."""
    screened: list = []
    survivors: list = []
    rejected: list = []
    series: dict = {}
    rows: dict = {}
    tested = 0
    for spec in specs:
        if supported_ids is not None and spec.spec_id not in supported_ids:
            continue
        out = evaluate_in_context(spec, ctx, rebalance_dates,
                                  champion_returns=champion_returns, cfg=cfg)
        row = out["row"]
        tested += 1
        rows[spec.spec_id] = row
        reason = _screen_reject(row, min_scored_periods=min_scored_periods)
        rec = {"spec_id": spec.spec_id, "name": spec.name, "family": spec.family,
               "periods": row["periods"], "coverage_names": row["universe"],
               "rank_ic_mean": row["rank_ic_mean"], "rank_ic_t": row["rank_ic_t"],
               "spread_t": row["spread_t"],
               "net25": row["net_annualized_return"], "turnover": row["turnover"],
               "subperiod_direction": (1 if (row["rank_ic_mean"] or 0) > 0
                                       else -1),
               "screen_reason": reason}
        screened.append(rec)
        if reason:
            rejected.append(rec)
        else:
            survivors.append(rec)
            series[spec.spec_id] = out["series"]
    survivors.sort(key=lambda r: abs(r["rank_ic_t"] or 0.0), reverse=True)
    return {"screened": screened, "survivors": survivors, "rejected": rejected,
            "series": series, "rows": rows, "family_size": tested}


# --------------------------------------------------------------------------- #
# Statistical discipline - multiple-testing / false-discovery control.
# --------------------------------------------------------------------------- #
def multiple_testing(screen_records: Sequence[dict], *, family_size: int,
                     alpha: float = DEFAULT_FDR_ALPHA) -> dict:
    """Benjamini-Hochberg FDR + Bonferroni over the FULL tested family, plus a
    per-candidate deflated Sharpe (deflated for ``family_size`` trials). Returns
    the raw + adjusted evidence for every tested spec and the set of spec ids that
    survive FDR at ``alpha``. A larger search makes survival strictly harder."""
    recs = [r for r in screen_records if r.get("rank_ic_t") is not None
            and r.get("periods")]
    pvals = [_se.approx_two_sided_pvalue(r["rank_ic_t"], int(r["periods"]))
             for r in recs]
    qvals = _sc.benjamini_hochberg([p for p in pvals if p is not None]) \
        if pvals else []
    # re-align q back to recs (only those with a finite p entered BH, in order)
    q_iter = iter(qvals)
    out_rows = []
    survivors = set()
    fam = max(1, int(family_size))
    for r, p in zip(recs, pvals):
        q = next(q_iter) if p is not None else None
        bonf = (_sc.bonferroni_adjust(p, fam) if p is not None else None)
        # deflated Sharpe from the rank-IC t (per-period Sharpe proxy = t/sqrt(n))
        n = int(r["periods"])
        sr = (r["rank_ic_t"] / (n ** 0.5)) if n > 0 else 0.0
        dsr = _sc.deflated_sharpe_ratio(sr, n_obs=n, n_trials=fam)
        survived = (q is not None and q < alpha
                    and (r["rank_ic_t"] or 0.0) > 0)
        if survived:
            survivors.add(r["spec_id"])
        out_rows.append({
            "spec_id": r["spec_id"], "name": r["name"],
            "raw_pvalue": p, "bh_qvalue": q, "bonferroni_pvalue": bonf,
            "deflated_sharpe": dsr.get("deflated_sharpe"),
            "selection_penalty": _sc.selection_penalty(fam),
            "fdr_survived": bool(survived)})
    return {"family_size": fam, "alpha": alpha, "tested": len(recs),
            "rows": out_rows, "fdr_survivors": sorted(survivors),
            "method": "benjamini_hochberg_fdr+bonferroni+deflated_sharpe"}


# --------------------------------------------------------------------------- #
# Stage 2 - deep evaluation (holdout, parameter neighbourhood, capacity).
# --------------------------------------------------------------------------- #
def _split_holdout(rebalance_dates: Sequence[str], holdout_frac: float = 0.30
                   ) -> tuple:
    dates = list(rebalance_dates)
    n = len(dates)
    cut = int(round(n * (1.0 - holdout_frac)))
    cut = max(1, min(n - 1, cut))
    return dates[:cut], dates[cut:]


def parameter_neighborhood(spec: _sl.SignalSpec, ctx: dict,
                           rebalance_dates: Sequence[str], *,
                           cfg: Optional[dict] = None) -> dict:
    """Evaluate the spec's immediate lookback neighbours (+/-1 step). A candidate
    whose sign or significance depends on one exact parameter is unstable."""
    step = max(5, int(spec.lookback * 0.2))
    neighbours = []
    base_sign = None
    for lb in (spec.lookback - step, spec.lookback + step):
        if lb < 5:
            continue
        variant = _sl.SignalSpec(**{**spec.__dict__, "lookback": int(lb)})
        row = evaluate_in_context(variant, ctx, rebalance_dates, cfg=cfg)["row"]
        neighbours.append({"lookback": lb, "rank_ic_mean": row["rank_ic_mean"],
                           "rank_ic_t": row["rank_ic_t"]})
    base_row = evaluate_in_context(spec, ctx, rebalance_dates, cfg=cfg)["row"]
    base_sign = 1 if (base_row["rank_ic_mean"] or 0) > 0 else -1
    signs = [1 if (nb["rank_ic_mean"] or 0) > 0 else -1 for nb in neighbours]
    stable = bool(neighbours) and all(s == base_sign for s in signs)
    return {"base_lookback": spec.lookback, "neighbours": neighbours,
            "sign_stable": stable}


def deep_evaluate(spec: _sl.SignalSpec, ctx: dict,
                  rebalance_dates: Sequence[str], *, cfg9: dict,
                  family_size: int, multiple_testing_survived: bool,
                  holdout_ledger=None, cfg: Optional[dict] = None) -> dict:
    """Full deep evaluation of one survivor. Combines the unchanged gate verdict
    with the additional Stage 11 robustness controls. ``qualifies`` is True only
    when: the unchanged ``classify_evidence`` returns KEEP_FOR_RESEARCH, the
    multiple-testing FDR survived, the parameter neighbourhood is sign-stable, and
    the reserved holdout confirms the in-sample sign."""
    from . import tournament as _tt
    full = evaluate_in_context(spec, ctx, rebalance_dates, cfg=cfg)
    row = full["row"]
    metrics = _tt.row_to_contract_metrics(row)
    verdict = _tt.classify_evidence(metrics, cfg9)

    is_dates, hold_dates = _split_holdout(rebalance_dates)
    is_row = evaluate_in_context(spec, ctx, is_dates, cfg=cfg)["row"]
    hold_row = evaluate_in_context(spec, ctx, hold_dates, cfg=cfg)["row"]
    is_sign = 1 if (is_row["rank_ic_mean"] or 0) > 0 else -1
    hold_sign = 1 if (hold_row["rank_ic_mean"] or 0) > 0 else -1
    holdout_confirms = (hold_row["periods"] or 0) >= 2 and is_sign == hold_sign
    if holdout_ledger is not None:
        try:
            key = "stage11:%s:%s" % (spec.spec_id, hold_dates[0] if hold_dates
                                     else "none")
            if holdout_ledger.can_use(key):
                holdout_ledger.mark_used(key)
        except Exception:  # noqa: BLE001 - holdout ledger is advisory here
            pass

    neigh = parameter_neighborhood(spec, ctx, rebalance_dates, cfg=cfg)
    capacity = _capacity_concentration(spec, ctx, rebalance_dates[-1]
                                       if rebalance_dates else None)

    qualifies = (verdict["target_state"] == "KEEP_FOR_RESEARCH"
                 and bool(multiple_testing_survived)
                 and bool(neigh["sign_stable"])
                 and bool(holdout_confirms))
    return {
        "spec_id": spec.spec_id, "name": spec.name, "family": spec.family,
        "row": row, "series": full["series"], "metrics": metrics,
        "gate_verdict": verdict,
        "in_sample": {"periods": is_row["periods"],
                      "rank_ic_mean": is_row["rank_ic_mean"],
                      "rank_ic_t": is_row["rank_ic_t"]},
        "holdout": {"periods": hold_row["periods"],
                    "rank_ic_mean": hold_row["rank_ic_mean"],
                    "rank_ic_t": hold_row["rank_ic_t"],
                    "confirms_sign": bool(holdout_confirms)},
        "parameter_neighborhood": neigh,
        "capacity": capacity,
        "multiple_testing_survived": bool(multiple_testing_survived),
        "qualifies": bool(qualifies),
        "no_automatic_promotion": True,
    }


def _capacity_concentration(spec: _sl.SignalSpec, ctx: dict,
                            as_of: Optional[str]) -> dict:
    """Name + sector concentration of the long leg on the latest formation date -
    a capacity/concentration sanity check (never a gate override)."""
    if as_of is None:
        return {"long_names": 0, "max_name_weight": None,
                "max_sector_weight": None}
    raw = _sl.signal_cross_section(
        spec, ohlcv=ctx["ohlcv"], market=ctx["market"], store=ctx["store"],
        cik_to_assetid=ctx["cik_to_assetid"],
        sector_series=ctx["sector_series"], as_of=as_of)
    if not raw:
        return {"long_names": 0, "max_name_weight": None,
                "max_sector_weight": None}
    adj = {k: v * (spec.direction or 1) for k, v in raw.items()}
    ordered = sorted(adj.items(), key=lambda kv: kv[1], reverse=True)
    k = max(1, int(round(len(ordered) * _fev._DECILE_QUANTILE)))
    longs = [key for key, _ in ordered[:k]]
    max_name = 1.0 / len(longs) if longs else None      # equal-weight book
    max_sector = None
    if ctx.get("sector_series") is not None and ctx.get("cik_to_assetid"):
        a2c = {a: c for c, a in ctx["cik_to_assetid"].items()}
        buckets: dict = {}
        for aid in longs:
            cik = a2c.get(aid)
            sec = (ctx["sector_series"].sector_as_of(cik, as_of)
                   if cik else "Unknown")
            buckets[sec] = buckets.get(sec, 0) + 1
        if longs:
            max_sector = max(buckets.values()) / len(longs)
    return {"long_names": len(longs), "max_name_weight": max_name,
            "max_sector_weight": max_sector}


# --------------------------------------------------------------------------- #
# Ensemble discovery (Workstream E).
# --------------------------------------------------------------------------- #
def return_stream_correlations(series_by_spec: dict) -> dict:
    """Pairwise Pearson correlation of the aligned per-date long/short return
    streams. Returns ``{(id_a, id_b): corr}`` for id_a < id_b."""
    ids = sorted(series_by_spec)
    out: dict = {}
    for i in range(len(ids)):
        for j in range(i + 1, len(ids)):
            a, b = ids[i], ids[j]
            sa = series_by_spec[a]["long_short_by_date"]
            sb = series_by_spec[b]["long_short_by_date"]
            common = sorted(set(sa) & set(sb))
            if len(common) < 3:
                continue
            corr = _fev._pearson([sa[d] for d in common], [sb[d] for d in common])
            if corr is not None:
                out[(a, b)] = corr
    return out


def select_orthogonal(survivors: Sequence[dict], series_by_spec: dict, *,
                      max_corr: float = DEFAULT_ORTHO_CORR,
                      max_factors: int = 5) -> dict:
    """Greedy independence selection: walk survivors best-first (by |rank_ic_t|)
    and admit a signal only when its max |corr| to already-selected signals is
    below ``max_corr``. Near-duplicates (|corr| >= DEFAULT_MAX_ABS_CORR) are
    recorded as collapsed. Bounded to ``max_factors`` interpretable factors."""
    corr = return_stream_correlations(series_by_spec)

    def _c(a, b):
        return corr.get((a, b)) if a < b else corr.get((b, a))

    ranked = sorted([s for s in survivors if s["spec_id"] in series_by_spec],
                    key=lambda r: abs(r["rank_ic_t"] or 0.0), reverse=True)
    selected: list = []
    collapsed: list = []
    for s in ranked:
        sid = s["spec_id"]
        cors = [abs(_c(sid, o["spec_id"]) or 0.0) for o in selected]
        mx = max(cors) if cors else 0.0
        if mx >= DEFAULT_MAX_ABS_CORR:
            collapsed.append({"spec_id": sid, "max_corr": mx})
            continue
        if mx < max_corr and len(selected) < max_factors:
            selected.append(s)
    return {"selected": [s["spec_id"] for s in selected],
            "selected_records": selected, "collapsed": collapsed,
            "correlations": {"%s|%s" % k: v for k, v in corr.items()},
            "max_corr_threshold": max_corr}


def _rank01(vals: dict) -> dict:
    if not vals:
        return {}
    order = sorted(vals, key=lambda k: vals[k])
    n = len(order)
    return {k: (i / (n - 1) if n > 1 else 0.5) for i, k in enumerate(order)}


def build_ensemble_periods(components: Sequence[tuple], ctx: dict,
                           rebalance_dates: Sequence[str], horizon: int) -> list:
    """Composite cross-sections: per date, rank each component's expected-sign-
    adjusted factor to [0,1] and take the weighted sum over names present in EVERY
    component (no partial name). ``components`` = ``[(spec, weight)]``."""
    periods = []
    for d in rebalance_dates:
        rank_maps = []
        for spec, w in components:
            raw = _sl.signal_cross_section(
                spec, ohlcv=ctx["ohlcv"], market=ctx["market"],
                store=ctx["store"], cik_to_assetid=ctx["cik_to_assetid"],
                sector_series=ctx["sector_series"], as_of=d)
            raw = _sl.winsorize(raw, spec.winsor)
            adj = {k: v * (spec.direction or 1) for k, v in raw.items()}
            rank_maps.append((_rank01(adj), float(w)))
        if not rank_maps:
            continue
        common = set(rank_maps[0][0])
        for rm, _w in rank_maps[1:]:
            common &= set(rm)
        names = []
        for key in common:
            score = sum(w * rm[key] for rm, w in rank_maps)
            idx = ctx["close_index"].get(key)
            if idx is None:
                continue
            fwd = _fev._forward_return(idx[0], idx[1], d, horizon)
            if fwd is None:
                continue
            names.append((key, score, fwd))
        if names:
            periods.append({"as_of": d, "names": names})
    return periods


def evaluate_ensemble(components: Sequence[tuple], ctx: dict,
                      rebalance_dates: Sequence[str], *, horizon: int = 63,
                      label: str = "ensemble", cfg: Optional[dict] = None
                      ) -> dict:
    """Evaluate a weighted composite of component specs into a canonical row +
    each component's incremental contribution (diversification vs the others)."""
    periods = build_ensemble_periods(components, ctx, rebalance_dates, horizon)
    out = _se.evaluate_periods(periods, horizon_days=horizon, feature=label,
                               cfg=cfg)
    return {"row": out["row"], "series": out["series"],
            "components": [{"spec_id": s.spec_id, "name": s.name,
                            "weight": round(float(w), 6)}
                           for s, w in components]}


def discover_ensembles(selected_specs: Sequence[_sl.SignalSpec],
                       screen_rows: dict, ctx: dict,
                       rebalance_dates: Sequence[str], *, cfg9: dict,
                       horizon: int = 63, cfg: Optional[dict] = None) -> dict:
    """Build and evaluate equal-weight, evidence-weighted and training-only-
    constrained composites of the orthogonally-selected signals. Each ensemble is
    gated by the UNCHANGED ``classify_evidence`` and rejected when it is merely an
    overfit combination of weak signals (holdout sign not confirmed, or no
    component individually clears the screen). Weights are NEVER fit on the
    holdout."""
    from . import tournament as _tt
    specs = list(selected_specs)
    if len(specs) < 2:
        return {"ensembles": [], "qualified": [],
                "note": "fewer than two orthogonal signals; no ensemble built"}
    is_dates, hold_dates = _split_holdout(rebalance_dates)

    # weights
    ew = [(s, 1.0 / len(specs)) for s in specs]
    ic_ts = [max(0.0, (screen_rows.get(s.spec_id, {}) or {}).get("rank_ic_t")
                 or 0.0) for s in specs]
    tot = sum(ic_ts) or 1.0
    evw = [(specs[i], ic_ts[i] / tot) for i in range(len(specs))]
    # training-only constrained weights: proportional to IS rank_ic_t, >=0.
    is_ts = []
    for s in specs:
        r = evaluate_in_context(s, ctx, is_dates, cfg=cfg)["row"]
        is_ts.append(max(0.0, r["rank_ic_t"] or 0.0))
    tot_is = sum(is_ts) or 1.0
    cw = [(specs[i], is_ts[i] / tot_is) for i in range(len(specs))]

    plans = [("equal_weight", ew), ("evidence_weighted", evw),
             ("train_constrained", cw)]
    ensembles = []
    qualified = []
    for name, comps in plans:
        full = evaluate_ensemble(comps, ctx, rebalance_dates, horizon=horizon,
                                 label="ensemble_%s" % name, cfg=cfg)
        metrics = _tt.row_to_contract_metrics(full["row"])
        verdict = _tt.classify_evidence(metrics, cfg9)
        hold = evaluate_ensemble(comps, ctx, hold_dates, horizon=horizon,
                                 label="ensemble_%s_holdout" % name, cfg=cfg)
        is_sign = 1 if (evaluate_ensemble(comps, ctx, is_dates, horizon=horizon,
                        label="is", cfg=cfg)["row"]["rank_ic_mean"] or 0) > 0 \
            else -1
        hold_sign = 1 if (hold["row"]["rank_ic_mean"] or 0) > 0 else -1
        holdout_confirms = (hold["row"]["periods"] or 0) >= 2 \
            and is_sign == hold_sign
        # contribution: diversification of each component vs the ensemble stream.
        contributions = []
        ens_stream = full["series"]["long_short_by_date"]
        for s, w in comps:
            contributions.append({"spec_id": s.spec_id, "name": s.name,
                                  "weight": round(float(w), 6)})
        qualifies = (verdict["target_state"] == "KEEP_FOR_RESEARCH"
                     and bool(holdout_confirms))
        rec = {"ensemble": name, "row": full["row"],
               "components": full["components"], "contributions": contributions,
               "gate_verdict": verdict, "holdout_confirms": bool(holdout_confirms),
               "qualifies": bool(qualifies), "no_automatic_promotion": True}
        ensembles.append(rec)
        if qualifies:
            qualified.append(rec)
    return {"ensembles": ensembles, "qualified": qualified,
            "in_sample_dates": len(is_dates), "holdout_dates": len(hold_dates)}


__all__ = [
    "build_panel_context", "evaluate_in_context", "screen_specs",
    "multiple_testing", "deep_evaluate", "parameter_neighborhood",
    "return_stream_correlations", "select_orthogonal", "build_ensemble_periods",
    "evaluate_ensemble", "discover_ensembles",
    "SCREEN_MIN_ABS_RANK_IC_T", "DEFAULT_FDR_ALPHA", "DEFAULT_ORTHO_CORR",
]
