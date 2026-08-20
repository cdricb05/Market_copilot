"""alpha_agent.r31.campaign - Release 31 orchestration and terminal verdict.

Runs the campaign in the order the contract requires and in no other:

    1  freeze the investment universe          (PIT S&P 500 membership)
    2  freeze the dual benchmark set           (equal weight AND total return)
    3  freeze the data snapshot manifest
    4  freeze the evidence partition
    5  freeze the covariance cache             (shared by every candidate)
    6  freeze the research judge and the campaign contract
    7  known-method tournament                 (DISCOVERY fit, VALIDATION select)
    8  bounded novel discovery                 (two campaigns, then exhaustion)
    9  lockbox                                 (frozen finalists, one shot each)
   10  campaign-wide multiple testing
   11  terminal verdict

Every stage is resumable. A stage that has already produced its immutable
artifact re-reads it; a candidate whose specification hash is already in the
registry is not refitted. That matters for more than convenience - it is what
keeps the multiple-testing denominator equal to the number of candidates the
campaign actually executed.

Nothing here promotes a model, writes an operational store, creates a signal, a
target, a proposal, a decision or an order.
"""
from __future__ import annotations

import re
import time
from typing import Optional

import numpy as np

from .. import r31
from . import allocation as _alloc
from . import benchmarks as _benchmarks
from . import calibration as _calib
from . import contract as _contract
from . import covcache as _covcache
from . import judge as _judge
from . import lockbox as _lockbox
from . import methods as _methods
from . import multiple_testing as _mt
from . import novel as _novel
from . import partition as _partition
from . import registry as _registry
from . import snapshot as _snapshot
from . import universe as _universe

CALCULATION_OWNER = "alpha_agent.r31.campaign"

# A superiority check whose comparator does not exist. It is NOT a pass: a
# check that cannot fail proves nothing, and the campaign refuses to let one
# count toward a superiority claim.
UNAVAILABLE_NO_INCUMBENT = "UNAVAILABLE_NO_INCUMBENT"
VERDICT_SCHEMA = "r31_final_verdict/2"
FRONTIER_SCHEMA = "r31_economic_frontier_results/2"
VERDICT_ARTIFACT = "final_verdict.json"
FRONTIER_ARTIFACT = "economic_frontier_results.json"
KNOWN_RESULTS_ARTIFACT = _methods.RESULTS_ARTIFACT

#: The horizon the operational policy declares (zero_base policy_horizon_sessions).
PRIMARY_HORIZON = 20

#: Decision dates used to fit a Track-A calibration: EVERY DISCOVERY date.
#:
#: Discovery only, never validation and never lockbox. Validation selects between
#: candidates, so fitting the units conversion there too would let a candidate be
#: chosen partly on evidence its own calibration had already seen; the lockbox is
#: invisible to everything.
#:
#: ALL of discovery rather than a trailing window, for power. The calibration
#: owner tests the mean of the per-date slopes, whose t-statistic grows with
#: sqrt(number of fitting dates): a genuine modest factor produces t ~ 2.3 over 60
#: dates and t ~ 3.9 over the full 167. Fitting on a short trailing window would
#: make the gate reject real alpha for want of evidence and would additionally
#: make every calibration a claim about one regime - momentum's measured slope
#: over 2009-2014 is negative, and a window that happened to land there would
#: refuse a factor that a longer window prices normally. Taking the whole
#: entitled layer also removes a free parameter nobody would be able to defend.
CALIBRATION_DATES = None


# --------------------------------------------------------------------------- #
# The shared evaluation context
# --------------------------------------------------------------------------- #
class Context:
    """Everything a candidate needs to be fitted and judged, loaded once.

    Held as one object because a worker process must rebuild it identically, and
    because a context assembled twice from different pieces is exactly how two
    candidates end up measured against two different risk models.
    """

    def __init__(self, campaign_id: str):
        self.campaign_id = campaign_id
        self.snap = _snapshot.load(campaign_id)
        self.membership = _universe.load(campaign_id=campaign_id)
        self.benchmarks = _benchmarks.load(campaign_id=campaign_id)
        self.partition = _partition.load(campaign_id)
        self.universe_manifest = _universe.load_manifest(campaign_id)
        self.benchmark_manifest = r31.read_json(
            _benchmarks.path_for(campaign_id))
        self.cov = _covcache.load(campaign_id=campaign_id)
        self.mkt = None

    @property
    def universe_hash(self) -> str:
        return str((self.universe_manifest or {}).get("universe_hash"))

    @property
    def benchmark_hash(self) -> str:
        return str((self.benchmark_manifest or {}).get("benchmark_hash"))

    def market_history(self, sample, sections):
        if self.mkt is None:
            self.mkt = _novel.market_history(self.snap, sample, sections)
        return self.mkt


_WORKER: Optional[Context] = None


def _worker_init(campaign_id: str) -> None:
    global _WORKER
    _WORKER = Context(campaign_id)


# --------------------------------------------------------------------------- #
# Stages 1-6: freeze the contracts
# --------------------------------------------------------------------------- #
def freeze_contracts(*, campaign_id: str = _contract.CAMPAIGN_ID,
                     created_at: str, force_rebuild: bool = False,
                     log=print) -> Context:
    """Build every frozen input, in dependency order, then the contract itself."""
    from .. import release30_panel as _rp

    panel = _rp.load_price_panel()

    log("  [1] point-in-time S&P 500 investment universe ...")
    _universe.build_cache(campaign_id=campaign_id, panel=panel,
                          force=force_rebuild)
    umani = _universe.load_manifest(campaign_id)
    if umani is None:
        umani = _universe.build_manifest(campaign_id=campaign_id, panel=panel)
        _universe.freeze(umani)
    membership = _universe.load(campaign_id=campaign_id)

    log("  [2] dual benchmark set ...")
    _benchmarks.build_cache(campaign_id=campaign_id, panel_dates=panel.dates,
                            force=force_rebuild)
    bmani = r31.read_json(_benchmarks.path_for(campaign_id))
    if bmani is None:
        bmani = _benchmarks.build_manifest(campaign_id=campaign_id)
        _benchmarks.freeze(bmani)
    if bmani["investable"]["state"] != _benchmarks.SPY_AVAILABLE:
        log("  WARNING %s" % _benchmarks.SPY_BLOCKED)

    log("  [3] data snapshot ...")
    _snapshot.build_cache(campaign_id=campaign_id, force=force_rebuild)
    snap = _snapshot.load(campaign_id)
    surv = _snapshot.survivorship_report()
    manifest = _snapshot.build_manifest(snap, campaign_id=campaign_id,
                                        survivorship=surv)
    _snapshot.freeze(manifest)

    log("  [4] evidence partition ...")
    part = _partition.load(campaign_id) or _partition.build(snap,
                                                            campaign_id=campaign_id)
    _partition.freeze(part)

    log("  [5] covariance cache (shared by every candidate) ...")
    sections = part["samples"][_contract.PRIMARY_SAMPLE]["section_indices"]
    _covcache.build_cache(campaign_id=campaign_id, snap=snap,
                          membership=membership,
                          snapshot_hash=snap.content_hash,
                          universe_hash=umani["universe_hash"],
                          panel=panel, sections=sections,
                          force=force_rebuild, log=log)
    cov = _covcache.load(campaign_id=campaign_id)
    cmani = r31.read_json(
        r31.campaign_dir(campaign_id) / _covcache.MANIFEST_NAME)
    if cmani is None:
        cmani = _covcache.build_manifest(campaign_id=campaign_id, cache=cov)
        _covcache.freeze(cmani)

    log("  [6] judge and campaign contract ...")
    jc = _judge.build_contract(campaign_id=campaign_id)
    _judge.freeze(jc)
    lit = _methods.literature_registry(campaign_id=campaign_id)
    r31.write_json(r31.campaign_dir(campaign_id) / _methods.LITERATURE_ARTIFACT, lit)
    kmr = _methods.known_method_registry(campaign_id=campaign_id)
    r31.write_json(r31.campaign_dir(campaign_id) / _methods.KNOWN_ARTIFACT, kmr)

    existing = _contract.load(campaign_id)
    if existing is None:
        con = _contract.build(
            campaign_id=campaign_id, created_at=created_at,
            data_sources={f["family"]: f.get("file") or {"path": f["source"]}
                          for f in manifest["data_families"]},
            feature_spec={"order": list(snap.feature_names),
                          "price": list(_snapshot.PRICE_FEATURES),
                          "fundamental": list(_snapshot.FUNDAMENTAL_FEATURES),
                          "transform": "PER_DATE_CROSS_SECTIONAL_RANK_NORMALISE"},
            universe_hash=umani["universe_hash"],
            benchmark_hash=bmani["benchmark_hash"],
            judge_hash=jc["judge_hash"],
            calibration_owner=_calib.CALCULATION_OWNER,
            allocation_owner=_alloc.CALCULATION_OWNER,
            covariance_cache_key=cov.key,
            executed_grid={
                "known_configs_planned": sum(v["n_configs"]
                                             for v in _methods.FAMILY_SPECS.values()),
                "known_families": len(_methods.FAMILY_SPECS),
                "training_universe_variants_per_family": 1,
                "novel_per_campaign": _novel.PER_CAMPAIGN,
                "calibration_dates": CALIBRATION_DATES,
            })
        _contract.freeze(con)

    return Context(campaign_id)


# --------------------------------------------------------------------------- #
# Track-A calibration
# --------------------------------------------------------------------------- #
def build_calibration(*, ctx: Context, sample: str, sections: list, part: dict,
                      predict, horizon: int) -> tuple:
    """Fit the pre-registered monotonic score -> expected-return map.

    Reads the LAST ``CALIBRATION_DATES`` DISCOVERY dates and nothing else. Both
    the score and the realised return are cross-sectionally demeaned over the
    INVESTMENT universe on each date, because ``mu`` prices an allocation among
    index members and an excess return measured over a wider set is not the
    excess return the allocator will act on.

    Returns ``(calibration, sigma_scalar)``. Raises ``CalibrationRefused`` - which
    is a legitimate, recorded outcome, not an error to be worked around.
    """
    feats = _judge._sample_features(sample)
    disc = (part["discovery"] if CALIBRATION_DATES is None
            else part["discovery"][-int(CALIBRATION_DATES):])
    scores, realised, dates = [], [], []
    for li in disc:
        k = sections[li]
        X, _y, adv, syms = ctx.snap.block(sample, k, feats, horizon)
        try:
            elig = ctx.membership.eligible_columns(ctx.snap.dates[k], syms)
        except Exception:
            continue
        if int(elig.sum()) < _judge.MIN_ELIGIBLE:
            continue
        raw, _ = ctx.snap.holding_returns(sample, k, _judge.HOLD_SESSIONS)
        s = np.asarray(predict(k, X, adv, syms), dtype=np.float64)
        m = elig & np.isfinite(s) & np.isfinite(raw)
        if int(m.sum()) < _judge.MIN_ELIGIBLE:
            continue
        sv, rv = s[m], raw[m]
        scores.append(sv - sv.mean())
        realised.append(rv - rv.mean())
        dates.append(np.full(int(m.sum()), ctx.snap.dates[k]))

    if not scores:
        raise _calib.CalibrationRefused(
            _calib.NOT_CALIBRATABLE,
            "no discovery cross-section produced a usable score/return pair")

    s_all = np.concatenate(scores)
    r_all = np.concatenate(realised)
    d_all = np.concatenate(dates)
    cal = _calib.fit(s_all, r_all, dates=d_all)
    resid = r_all - cal.apply(s_all)
    sigma = float(np.std(resid[np.isfinite(resid)]))
    return cal, (sigma if sigma > 0 else 0.08)


# --------------------------------------------------------------------------- #
# One candidate
# --------------------------------------------------------------------------- #
def _build_predictor(task: dict, *, ctx: Context, sections: list, p: dict,
                     feats):
    """Rebuild the exact fitted decision function a task describes."""
    family, params = task["family"], task["params"]
    ufilter = _methods.training_filter(task["training_universe"], ctx.membership)
    if task["role"] == _registry.ROLE_BENCHMARK:
        return _methods.benchmark_predictor(params["weights"], feats)
    if task["phase"] == _registry.PHASE_NOVEL:
        factory = _novel.predictor_factory(
            family, params, snap=ctx.snap, sample=task["sample"],
            horizon=task["horizon"], seed=int(task["seed"]),
            mkt=ctx.market_history(task["sample"], sections))
        return factory(sections=sections, part=p, feats=feats)
    return _methods.walk_forward_predictor(
        family, params, snap=ctx.snap, sample=task["sample"], sections=sections,
        feats=feats, horizon=task["horizon"], seed=int(task["seed"]),
        train_cap=p["validation"][-1] if p["validation"] else 0,
        embargo=p["embargo_dates"],
        warmup=p["discovery"] + p["validation"], ufilter=ufilter)


def execute_task(task: dict, *, ctx: Optional[Context] = None) -> dict:
    """Fit, calibrate and score ONE candidate. Pure with respect to the registry.

    Returns the row to be recorded. Never writes: the parent process owns the
    append-only log, so a parallel run cannot interleave two writers into it.
    """
    ctx = ctx if ctx is not None else _WORKER
    sample = task["sample"]
    part_block = ctx.partition["samples"][sample]
    sections = part_block["section_indices"]
    p = part_block["horizons"][str(int(task["horizon"]))]
    feats = _judge._sample_features(sample)
    layer = p[task.get("layer", "validation")]

    t0 = time.time()
    state = _registry.STATE_OK
    failure = None
    score = None
    calib_state = None
    calib_measured = None
    try:
        predict = _build_predictor(task, ctx=ctx, sections=sections, p=p,
                                   feats=feats)
        track = task["track"]
        cal, sigma = (None, 0.08)
        if track == _contract.TRACK_A:
            cal, sigma = build_calibration(
                ctx=ctx, sample=sample, sections=sections, part=p,
                predict=predict, horizon=task["horizon"])
            calib_state = _calib.CALIBRATION_OK
        score = _judge.score_candidate(
            snap=ctx.snap, cov=ctx.cov, membership=ctx.membership,
            benchmarks=ctx.benchmarks, sample=sample,
            section_indices=sections, layer_indices=layer, predict=predict,
            horizon=task["horizon"], track=track, calib=cal, sigma_scalar=sigma,
            gamma_multipliers=task.get(
                "gamma_multipliers", (_contract.PRIMARY_GAMMA_MULTIPLIER,)))
    except _calib.CalibrationRefused as exc:
        # A candidate whose score cannot be defensibly mapped into return units
        # is not a capital allocator. It is REJECTED, recorded, and stays in the
        # multiple-testing denominator - manufacturing a mu would be the one
        # response the campaign forbids.
        state, failure = _registry.STATE_CALIBRATION_REFUSED, exc.detail
        calib_state = exc.state
        calib_measured = exc.diagnostics
    except MemoryError:
        state, failure = _registry.STATE_RESOURCE_INFEASIBLE, "MEMORY"
    except Exception as exc:                      # noqa: BLE001 - recorded, not hidden
        state, failure = _registry.STATE_FAILED, "%s: %s" % (type(exc).__name__, exc)

    return {
        "candidate_id": task["candidate_id"], "spec_hash": task["spec_hash"],
        "phase": task["phase"], "family": task["family"], "role": task["role"],
        "params": task["params"], "sample": sample,
        "horizon_sessions": int(task["horizon"]), "seed": int(task["seed"]),
        "track": task["track"],
        "training_universe": task["training_universe"],
        "evaluation_universe": _contract.EVALUATION_UNIVERSE,
        "features": list(feats), "layer_scored": task.get("layer", "validation").upper(),
        "novel_campaign": task.get("novel_campaign"),
        "refinement_depth": int(task.get("refinement_depth") or 0),
        "snapshot_hash": ctx.snap.content_hash,
        "partition_hash": ctx.partition["partition_hash"],
        "universe_hash": ctx.universe_hash,
        "benchmark_hash": ctx.benchmark_hash,
        "covariance_cache_key": ctx.cov.key,
        "training_dates": {
            "first": ctx.snap.dates[sections[p["discovery"][0]]] if p["discovery"] else None,
            "last": ctx.snap.dates[sections[p["validation"][-1]]] if p["validation"] else None,
            "cap": "LAST_VALIDATION_DATE_LOCKBOX_NEVER_TRAINED"},
        "validation_dates": {
            "first": ctx.snap.dates[sections[p["validation"][0]]] if p["validation"] else None,
            "last": ctx.snap.dates[sections[p["validation"][-1]]] if p["validation"] else None,
            "n": len(p["validation"])},
        "calibration_state": calib_state,
        # The measured slope, per-date t and sign stability behind a refusal.
        # Without these a campaign that refuses every Track-A candidate cannot
        # show whether the refusals came from the evidence or from the gate.
        "calibration_measured": calib_measured,
        "state": state, "failure": failure,
        "runtime_seconds": round(time.time() - t0, 3),
        "validation": score,
    }


def make_task(*, ctx: Context, phase: str, family: str, params: dict,
              sample: str, horizon: int, candidate_id: str,
              role: str = _registry.ROLE_CANDIDATE,
              training_universe: str = _contract.TRAIN_BROAD_PIT,
              track: Optional[str] = None, seed: Optional[int] = None,
              novel_campaign: Optional[int] = None, refinement_depth: int = 0,
              layer: str = "validation", gamma_multipliers=None,
              extra: Optional[dict] = None) -> dict:
    seed = int(seed if seed is not None else _contract.SEEDS["learner"])
    feats = _judge._sample_features(sample)
    track = track or _methods.FAMILY_TRACK.get(family, _contract.TRACK_A)
    h = _methods.spec_hash(
        phase=phase, family=family, params=params, sample=sample,
        horizon=horizon, feats=feats, snapshot_hash=ctx.snap.content_hash,
        partition_hash=ctx.partition["partition_hash"], seed=seed,
        training_universe=training_universe,
        universe_hash=ctx.universe_hash, benchmark_hash=ctx.benchmark_hash,
        extra=extra)
    return {"candidate_id": candidate_id, "spec_hash": h, "phase": phase,
            "family": family, "params": params, "sample": sample,
            "horizon": int(horizon), "role": role, "seed": seed,
            "track": track, "training_universe": training_universe,
            "novel_campaign": novel_campaign,
            "refinement_depth": int(refinement_depth), "layer": layer,
            "gamma_multipliers": tuple(
                gamma_multipliers or (_contract.PRIMARY_GAMMA_MULTIPLIER,))}


# --------------------------------------------------------------------------- #
# Bounded parallel execution
# --------------------------------------------------------------------------- #
def run_tasks(tasks: list, *, ctx: Context, reg, workers: int = 1,
              log=print) -> list:
    """Execute tasks, skipping any specification already in the registry.

    The parent process is the ONLY writer to the append-only log, whatever the
    worker count, so parallelism can never corrupt the denominator.
    """
    todo = []
    for t in tasks:
        if reg.has(t["spec_hash"]):
            continue
        try:
            reg.check_budget(phase=t["phase"], family=t["family"], role=t["role"],
                             novel_campaign=t.get("novel_campaign"),
                             refinement_depth=t.get("refinement_depth") or 0)
        except _registry.BudgetExceeded as exc:
            log("  BUDGET STOP: %s" % exc)
            break
        todo.append(t)
    if not todo:
        return []

    out = []
    n = len(todo)
    if workers <= 1:
        for i, t in enumerate(todo, 1):
            row = execute_task(t, ctx=ctx)
            _record(reg, row, log=log, i=i, n=n)
            out.append(row)
        return out

    import multiprocessing as mp
    ctxmp = mp.get_context("spawn")
    with ctxmp.Pool(processes=int(workers), initializer=_worker_init,
                    initargs=(ctx.campaign_id,)) as pool:
        for i, row in enumerate(pool.imap_unordered(execute_task, todo), 1):
            _record(reg, row, log=log, i=i, n=n)
            out.append(row)
    return out


def _record(reg, row, *, log, i, n) -> None:
    try:
        reg.record(row)
    except _registry.DuplicateCandidate:
        return
    reg.persist_summary()
    p = ((row.get("validation") or {}).get("primary") or {})
    log("  [%3d/%3d] %-40s %-22s net_excess=%s cash=%s %.0fs"
        % (i, n, row["candidate_id"][:40], row["state"],
           _fmt(p.get("net_excess_annualised")), _fmt(p.get("cash_weight_mean")),
           row.get("runtime_seconds") or 0.0))


def _fmt(x) -> str:
    return "  n/a " if x is None else "%+.4f" % float(x)


# --------------------------------------------------------------------------- #
# Stage 7: known-method tournament
# --------------------------------------------------------------------------- #
def run_known_methods(ctx: Context, *, campaign_id: str = _contract.CAMPAIGN_ID,
                      workers: int = 1, log=print) -> dict:
    reg = _registry.Registry(campaign_id)
    _registry.assert_contract_stable(campaign_id)
    primary = _contract.PRIMARY_SAMPLE

    tasks: list = []
    for bid, b in sorted(_methods.BENCHMARKS.items()):
        if primary not in b["samples"]:
            continue
        tasks.append(make_task(
            ctx=ctx, phase=_registry.PHASE_KNOWN, family=bid,
            params={"weights": b["weights"]}, sample=primary,
            horizon=PRIMARY_HORIZON, role=_registry.ROLE_BENCHMARK,
            track=_contract.TRACK_A,
            candidate_id="bench:%s:h%d" % (bid, PRIMARY_HORIZON)))

    for fam, grid in sorted(_methods.FAMILY_SPECS.items()):
        if fam == "combination":
            continue
        for i, params in enumerate(grid["params"]):
            tasks.append(make_task(
                ctx=ctx, phase=_registry.PHASE_KNOWN, family=fam, params=params,
                sample=primary, horizon=PRIMARY_HORIZON,
                candidate_id="km:%s:%02d:px:h%d" % (fam, i, PRIMARY_HORIZON)))

    log("  known-method grid: %d tasks" % len(tasks))
    run_tasks(tasks, ctx=ctx, reg=reg, workers=workers, log=log)

    # CORRECTION 1 as a TESTED hypothesis: re-run the best configuration of each
    # family fitted on the INVESTMENT universe alone. Same judge, same evaluation
    # universe; only what the model learned from differs.
    best = _best_per_family(reg, sample=primary, horizon=PRIMARY_HORIZON)
    narrow = []
    for fam, row in sorted(best.items()):
        if row is None or fam == "combination":
            continue
        narrow.append(make_task(
            ctx=ctx, phase=_registry.PHASE_KNOWN, family=fam,
            params=row["params"], sample=primary, horizon=PRIMARY_HORIZON,
            training_universe=_contract.TRAIN_INVESTMENT_ONLY,
            candidate_id="km:%s:best:trainsp500:h%d" % (fam, PRIMARY_HORIZON)))
    log("  training-universe hypothesis: %d tasks" % len(narrow))
    run_tasks(narrow, ctx=ctx, reg=reg, workers=workers, log=log)

    _run_combinations(ctx, reg, primary, PRIMARY_HORIZON, workers=workers, log=log)
    reg.persist_summary()

    summary = reg.summary()
    body = {
        "contract": _methods.RESULTS_SCHEMA,
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "executed_configs": len(reg.executed(phase=_registry.PHASE_KNOWN)),
        "config_budget": _contract.MAX_KNOWN_METHOD_CONFIGS,
        "families_executed": sorted(reg.families(_registry.PHASE_KNOWN)),
        "family_budget": _contract.MAX_KNOWN_METHOD_FAMILIES,
        "per_family": summary["known_method"]["per_family"],
        "selection_layer": "VALIDATION",
        "selection_statistic": "net_excess_annualised_vs_%s"
                               % _contract.BENCH_EQUAL_WEIGHT,
        "evaluation_universe": _contract.EVALUATION_UNIVERSE,
        "lockbox_invisible": True,
        "training_universe_comparison": _training_universe_comparison(reg),
        "calibration_rejections": _calibration_rejections(reg),
        "leaderboard": _leaderboard(reg, phase=_registry.PHASE_KNOWN),
        "benchmarks": _leaderboard(reg, phase=_registry.PHASE_KNOWN,
                                   role=_registry.ROLE_BENCHMARK),
    }
    body["known_method_results_hash"] = r31.sha(body)
    body.update(r31.safety_block())
    r31.write_json(r31.campaign_dir(campaign_id) / KNOWN_RESULTS_ARTIFACT, body)
    return body


def _run_combinations(ctx, reg, sample, horizon, *, workers, log=print) -> None:
    """Forecast combination over the members that already ran on this layer."""
    members = [e for e in reg.executed(phase=_registry.PHASE_KNOWN)
               if e["sample"] == sample and e["horizon_sessions"] == horizon
               and e["state"] == _registry.STATE_OK
               and e["family"] not in ("combination",)
               and e.get("track") == _contract.TRACK_A]
    if len(members) < 3:
        return
    ranked = sorted(members, key=_score_key, reverse=True)
    schemes = {
        "EQUAL_WEIGHT_ALL_FAMILIES": _one_per_family(ranked),
        "TOP3_VALIDATION_EQUAL_WEIGHT": ranked[:3],
        "VALIDATION_IC_WEIGHTED": _one_per_family(ranked),
    }
    tasks = []
    for i, spec in enumerate(_methods.FAMILY_SPECS["combination"]["params"]):
        scheme = spec["scheme"]
        chosen = schemes.get(scheme) or []
        if len(chosen) < 2:
            continue
        weights = None
        if scheme == "VALIDATION_IC_WEIGHTED":
            ics = np.array([max(0.0, float((m["validation"]["predictive"]
                                            .get("rank_ic_mean") or 0.0)))
                            for m in chosen])
            weights = (ics / ics.sum()).tolist() if ics.sum() > 0 else None
        if weights is None:
            weights = [1.0 / len(chosen)] * len(chosen)
        tasks.append(make_task(
            ctx=ctx, phase=_registry.PHASE_KNOWN, family="combination",
            params={"scheme": scheme,
                    "members": [{"family": m["family"], "params": m["params"],
                                 "seed": int(m["seed"]),
                                 "training_universe": m.get("training_universe")}
                                for m in chosen],
                    "weights": [round(float(w), 6) for w in weights]},
            sample=sample, horizon=horizon,
            candidate_id="km:combination:%02d:px:h%d" % (i, horizon)))
    log("  combination: %d tasks" % len(tasks))
    run_tasks(tasks, ctx=ctx, reg=reg, workers=workers, log=log)


def _one_per_family(ranked: list) -> list:
    seen, out = set(), []
    for m in ranked:
        if m["family"] in seen:
            continue
        seen.add(m["family"])
        out.append(m)
    return out


# --------------------------------------------------------------------------- #
# Ranking helpers
# --------------------------------------------------------------------------- #
def _score_key(entry: dict) -> float:
    """The campaign's ONE ranking statistic.

    Net excess annualised against the point-in-time S&P 500 equal-weight
    benchmark, produced by the canonical zero-base allocation at the canonical
    risk appetite. Not IC, not gross return, not a top-N book, and never the
    convenient point of the gamma frontier.
    """
    v = entry.get("validation") or {}
    p = v.get("primary") or {}
    x = p.get("net_excess_annualised")
    return float(x) if x is not None else float("-inf")


def _best_per_family(reg, *, sample: str, horizon: int) -> dict:
    out: dict = {}
    for e in reg.executed(phase=_registry.PHASE_KNOWN):
        if e["sample"] != sample or e["horizon_sessions"] != horizon:
            continue
        if e["state"] != _registry.STATE_OK:
            continue
        if e.get("training_universe") != _contract.TRAIN_BROAD_PIT:
            continue
        cur = out.get(e["family"])
        if cur is None or _score_key(e) > _score_key(cur):
            out[e["family"]] = e
    return out


def _leaderboard(reg, *, phase: str, role: str = _registry.ROLE_CANDIDATE,
                 limit: int = 40) -> list:
    rows = [e for e in reg.executed(phase=phase, role=role)
            if e["state"] == _registry.STATE_OK]
    rows.sort(key=_score_key, reverse=True)
    return [_row_summary(e) for e in rows[:limit]]


def _row_summary(e: dict) -> dict:
    v = e.get("validation") or {}
    p = v.get("primary") or {}
    pr = v.get("predictive") or {}
    return {
        "candidate_id": e["candidate_id"], "spec_hash": e["spec_hash"],
        "family": e["family"], "role": e["role"], "track": e.get("track"),
        "training_universe": e.get("training_universe"),
        "sample": e["sample"], "horizon_sessions": e["horizon_sessions"],
        "params": e["params"],
        "net_excess_annualised": p.get("net_excess_annualised"),
        "net_return_annualised": p.get("net_return_annualised"),
        "equal_weight_benchmark_annualised": p.get("equal_weight_benchmark_annualised"),
        "spy_benchmark_annualised": p.get("spy_benchmark_annualised"),
        "net_excess_vs_spy_annualised": p.get("net_excess_vs_spy_annualised"),
        "volatility_annualised": p.get("volatility_annualised"),
        "sharpe_net": p.get("sharpe_net"),
        "information_ratio_vs_equal_weight": p.get("information_ratio_vs_equal_weight"),
        "max_drawdown_net": p.get("max_drawdown_net"),
        "cvar_05_net": p.get("cvar_05_net"),
        "turnover_annualised": p.get("turnover_annualised"),
        "cost_drag_annualised": p.get("cost_drag_annualised"),
        "cash_weight_mean": p.get("cash_weight_mean"),
        "names_held_mean": p.get("names_held_mean"),
        "net_excess_t_newey_west": p.get("net_excess_t_newey_west"),
        "rank_ic_mean": pr.get("rank_ic_mean"),
        "rank_ic_t": pr.get("rank_ic_t_newey_west"),
        "subperiod_win_fraction": (p.get("robustness") or {}).get("subperiod_win_fraction"),
        "runtime_seconds": e.get("runtime_seconds"),
    }


def _training_universe_comparison(reg) -> dict:
    """Did fitting on the broad universe help or hurt, judged identically?"""
    out = {}
    for e in reg.executed(phase=_registry.PHASE_KNOWN):
        if e["state"] != _registry.STATE_OK:
            continue
        fam = e["family"]
        tu = e.get("training_universe")
        cur = out.setdefault(fam, {})
        if tu not in cur or _score_key(e) > (cur[tu] or float("-inf")):
            cur[tu] = _score_key(e)
    rows = []
    for fam, d in sorted(out.items()):
        broad = d.get(_contract.TRAIN_BROAD_PIT)
        narrow = d.get(_contract.TRAIN_INVESTMENT_ONLY)
        if broad is None or narrow is None:
            continue
        rows.append({"family": fam,
                     "train_broad_net_excess": None if broad == float("-inf") else broad,
                     "train_sp500_only_net_excess": None if narrow == float("-inf") else narrow,
                     "broad_training_helped": bool(broad > narrow)})
    helped = [r for r in rows if r["broad_training_helped"]]
    return {
        "hypothesis": "TRAIN_BROAD_INVEST_NARROW",
        "families_compared": len(rows),
        "families_where_broad_training_helped": len(helped),
        "evaluation_universe_identical_in_both_arms": True,
        "rows": rows,
    }


#: Refusal messages carry their own measurements. These recover them for rows
#: recorded before the structured field existed, so one campaign never reports a
#: mixture of "measured" and "unknown" for the same fact.
_DETAIL_PATTERNS = (
    ("per_date_slope_t", re.compile(r"per-date slope t=(-?\d+\.?\d*)")),
    ("fitting_dates", re.compile(r"over (\d+) (?:fitting )?dates")),
    ("sign_stability", re.compile(r"direction holds on only (-?\d+\.?\d*)")),
    ("fitting_dates", re.compile(r"of (\d+) fitting dates")),
    ("pooled_slope", re.compile(r"fitted slope (-?\d+\.?\d*(?:e-?\d+)?)")),
)


def _measured_from_detail(detail) -> dict:
    out: dict = {"source": "RECOVERED_FROM_REFUSAL_DETAIL"}
    text = str(detail or "")
    for key, pat in _DETAIL_PATTERNS:
        if key in out:
            continue
        m = pat.search(text)
        if m:
            try:
                out[key] = float(m.group(1))
            except ValueError:
                pass
    return out


def _calibration_rejections(reg) -> dict:
    """Refusal ATTRIBUTION: was it the evidence, or was it the gate?

    A campaign in which every Track-A candidate is refused owes the reader that
    question. The answer is the distribution of the measured statistics: if the
    best candidate anywhere in the search reaches a per-date t of 1.3 against a
    conventional floor of 2.0, the binding constraint is the information, and
    lowering the floor would only admit noise.
    """
    rows = [e for e in reg.entries
            if e.get("state") == _registry.STATE_CALIBRATION_REFUSED]
    measured = [(e.get("calibration_measured")
                 or _measured_from_detail(e.get("failure")))
                for e in rows]
    for e, m in zip(rows, measured):
        e.setdefault("calibration_measured", m)
    ts = [float(m["per_date_slope_t"]) for m in measured
          if m.get("per_date_slope_t") is not None]
    stabs = [float(m["sign_stability"]) for m in measured
             if m.get("sign_stability") is not None]
    by_state: dict = {}
    for e in rows:
        k = str(e.get("calibration_state"))
        by_state[k] = by_state.get(k, 0) + 1
    return {
        "count": len(rows),
        "by_state": dict(sorted(by_state.items())),
        "meaning": ("a model whose score could not be defensibly mapped into "
                    "economic return units is not a capital allocator; it is "
                    "rejected as a Track-A candidate and REMAINS in the "
                    "multiple-testing denominator"),
        "attribution": {
            "min_slope_t_required": _calib.MIN_SLOPE_T,
            "min_sign_stability_required": _calib.MIN_SIGN_STABILITY,
            "best_per_date_slope_t_observed": (max(ts) if ts else None),
            "median_per_date_slope_t_observed": (
                float(np.median(ts)) if ts else None),
            "best_sign_stability_observed": (max(stabs) if stabs else None),
            "candidates_with_a_measured_slope": len(ts),
            "reading": (
                "if the BEST measured per-date t across the whole search sits "
                "well below the conventional 2.0 floor, the binding constraint "
                "is the information rather than the gate, and relaxing the gate "
                "would admit noise rather than reveal alpha"),
        },
        "rows": [{"candidate_id": e["candidate_id"], "family": e["family"],
                  "track": e.get("track"),
                  "training_universe": e.get("training_universe"),
                  "calibration_state": e.get("calibration_state"),
                  "measured": e.get("calibration_measured"),
                  "detail": e.get("failure")} for e in rows],
    }


# --------------------------------------------------------------------------- #
# Stage 8: bounded novel discovery
# --------------------------------------------------------------------------- #
def run_novel(ctx: Context, *, campaign_id: str = _contract.CAMPAIGN_ID,
              workers: int = 1, log=print) -> dict:
    """Run novel campaign N1, and N2 only if N1 produced no improvement."""
    reg = _registry.Registry(campaign_id)
    _registry.assert_contract_stable(campaign_id)

    grammar = _novel.grammar_contract(campaign_id=campaign_id)
    r31.write_json(r31.campaign_dir(campaign_id) / _novel.GRAMMAR_ARTIFACT, grammar)

    sample = _contract.PRIMARY_SAMPLE
    feats = _judge._sample_features(sample)
    incumbent = _incumbent_row(reg, sample=sample, horizon=PRIMARY_HORIZON)
    bar = _score_key(incumbent) if incumbent else 0.0

    campaigns: dict = {}
    for cno in (1, 2):
        cands = _novel.generate(cno, feats)
        tasks = [make_task(ctx=ctx, phase=_registry.PHASE_NOVEL,
                           family=c["family"], params=c["params"],
                           sample=sample, horizon=PRIMARY_HORIZON,
                           candidate_id=c["candidate_id"], novel_campaign=cno,
                           track=_contract.TRACK_B if "direct" in c["family"]
                           else _contract.TRACK_A)
                 for c in cands]
        log("  novel campaign N%d: %d candidates" % (cno, len(tasks)))
        run_tasks(tasks, ctx=ctx, reg=reg, workers=workers, log=log)
        reg.persist_summary()

        rows = [e for e in reg.executed(phase=_registry.PHASE_NOVEL)
                if int(e.get("novel_campaign") or 0) == cno
                and e["state"] == _registry.STATE_OK]
        best = max(rows, key=_score_key) if rows else None
        improved = bool(best is not None and _score_key(best) > bar)
        campaigns["N%d" % cno] = {
            "executed": len(rows),
            "budget": _contract.MAX_NOVEL_CANDIDATES_PER_CAMPAIGN,
            "best_candidate": _row_summary(best) if best else None,
            "incumbent_bar_net_excess": bar,
            "improved_on_incumbent": improved,
            "result": "IMPROVEMENT_FOUND" if improved else "NULL_CAMPAIGN",
        }
        log("  N%d -> %s" % (cno, campaigns["N%d" % cno]["result"]))
        if improved:
            break

    nulls = sum(1 for v in campaigns.values() if v["result"] == "NULL_CAMPAIGN")
    body = {
        "contract": _novel.RESULTS_SCHEMA,
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "grammar_hash": grammar["novel_grammar_hash"],
        "families_executed": sorted(reg.families(_registry.PHASE_NOVEL)),
        "family_budget": _contract.MAX_NOVEL_FAMILIES,
        "candidates_executed": len(reg.executed(phase=_registry.PHASE_NOVEL)),
        "candidate_budget": _contract.MAX_NOVEL_CANDIDATES_TOTAL,
        "campaigns": campaigns,
        "consecutive_null_campaigns": nulls,
        "exhaustion_triggered": bool(nulls >= _contract.MAX_NOVEL_CAMPAIGNS),
        "no_third_campaign_permitted": True,
        "leaderboard": _leaderboard(reg, phase=_registry.PHASE_NOVEL),
    }
    body["novel_results_hash"] = r31.sha(body)
    body.update(r31.safety_block())
    r31.write_json(r31.campaign_dir(campaign_id) / _novel.RESULTS_ARTIFACT, body)
    return body


def _incumbent_row(reg, *, sample: str, horizon: int) -> Optional[dict]:
    for e in reg.entries:
        if (e.get("role") == _registry.ROLE_BENCHMARK
                and e.get("family") == "incumbent_momentum_leg"
                and e.get("sample") == sample
                and e.get("horizon_sessions") == horizon
                and e.get("state") == _registry.STATE_OK):
            return e
    return None


# --------------------------------------------------------------------------- #
# Stage 9: lockbox
# --------------------------------------------------------------------------- #
def select_finalists(reg) -> list:
    """Choose the lockbox finalists on DISCOVERY and VALIDATION evidence only.

    At most two per family, at most twelve in total, ranked by the campaign's one
    ranking statistic. The incumbent benchmark is always carried so the lockbox
    comparison has a reference measured on the very same dates.
    """
    rows = [e for e in reg.entries
            if e["state"] == _registry.STATE_OK
            and e["sample"] == _contract.PRIMARY_SAMPLE
            and e["horizon_sessions"] == PRIMARY_HORIZON
            and e["role"] == _registry.ROLE_CANDIDATE]
    rows.sort(key=_score_key, reverse=True)
    per_family: dict = {}
    out: list = []
    for e in rows:
        if len(out) >= _contract.MAX_LOCKBOX_CANDIDATES - 1:
            break
        # A combination candidate is a blend of OTHER candidates' fitted members.
        # Its members already compete on their own, and promoting the blend would
        # let one family occupy several finalist slots indirectly.
        if e["family"] == "combination":
            continue
        n = per_family.get(e["family"], 0)
        if n >= _contract.MAX_LOCKBOX_PER_FAMILY:
            continue
        per_family[e["family"]] = n + 1
        out.append(e)
    inc = _incumbent_row(reg, sample=_contract.PRIMARY_SAMPLE,
                         horizon=PRIMARY_HORIZON)
    if inc is not None:
        out.append(inc)
    return out


def run_lockbox(ctx: Context, *, campaign_id: str = _contract.CAMPAIGN_ID,
                at: str, workers: int = 1, log=print) -> dict:
    reg = _registry.Registry(campaign_id)
    _registry.assert_contract_stable(campaign_id)

    chosen = select_finalists(reg)
    frozen = _lockbox.freeze_finalists(
        [{"candidate_id": e["candidate_id"], "spec_hash": e["spec_hash"],
          "family": e["family"], "sample": e["sample"],
          "horizon_sessions": e["horizon_sessions"], "role": e["role"]}
         for e in chosen],
        campaign_id=campaign_id, selected_at=at,
        selection_basis="DISCOVERY_AND_VALIDATION_ONLY")
    log("  finalists frozen: %d" % frozen["count"])

    by_hash = {e["spec_hash"]: e for e in chosen}
    tasks, refused = [], []
    for f in frozen["finalists"]:
        entry = by_hash.get(f["spec_hash"])
        if entry is None:
            continue
        try:
            _lockbox.authorise(f["spec_hash"], campaign_id=campaign_id,
                               family=f["family"],
                               candidate_id=f["candidate_id"], at=at)
        except _lockbox.LockboxViolation as exc:
            refused.append({"candidate_id": f["candidate_id"],
                            "spec_hash": f["spec_hash"],
                            "state": "REFUSED", "reason": str(exc)})
            continue
        t = make_task(
            ctx=ctx, phase=entry["phase"], family=entry["family"],
            params=entry["params"], sample=entry["sample"],
            horizon=entry["horizon_sessions"], role=entry["role"],
            training_universe=entry.get("training_universe",
                                        _contract.TRAIN_BROAD_PIT),
            track=entry.get("track"), seed=entry["seed"],
            novel_campaign=entry.get("novel_campaign"),
            candidate_id=entry["candidate_id"], layer="lockbox",
            gamma_multipliers=_contract.RISK_FRONTIER_GAMMA_MULTIPLIERS)
        t["validation_row"] = _row_summary(entry)
        tasks.append(t)

    log("  lockbox: %d finalists, gamma frontier %s"
        % (len(tasks), list(_contract.RISK_FRONTIER_GAMMA_MULTIPLIERS)))
    results = list(refused)
    if tasks:
        if workers <= 1:
            rows = [execute_task(t, ctx=ctx) for t in tasks]
        else:
            import multiprocessing as mp
            ctxmp = mp.get_context("spawn")
            with ctxmp.Pool(processes=int(workers), initializer=_worker_init,
                            initargs=(ctx.campaign_id,)) as pool:
                rows = list(pool.imap_unordered(execute_task, tasks))
        vrow = {t["spec_hash"]: t["validation_row"] for t in tasks}
        for row in rows:
            results.append({
                "candidate_id": row["candidate_id"],
                "spec_hash": row["spec_hash"], "family": row["family"],
                "role": row["role"], "track": row.get("track"),
                "params": row["params"],
                "state": "EXECUTED" if row["state"] == _registry.STATE_OK
                         else row["state"],
                "validation": vrow.get(row["spec_hash"]),
                "lockbox": row["validation"],
            })
            p = ((row.get("validation") or {}).get("primary") or {})
            log("  lockbox %-38s net_excess=%s vs_spy=%s"
                % (row["candidate_id"][:38],
                   _fmt(p.get("net_excess_annualised")),
                   _fmt(p.get("net_excess_vs_spy_annualised"))))

    _lockbox.persist_results(results, campaign_id=campaign_id,
                             finalist_set_hash=frozen["finalist_set_hash"])
    return {"finalists": frozen, "results": results,
            "access_count": _lockbox.access_count(campaign_id)}


# --------------------------------------------------------------------------- #
# Stage 10: campaign-wide multiple testing
# --------------------------------------------------------------------------- #
def run_multiple_testing(*, campaign_id: str = _contract.CAMPAIGN_ID,
                         lockbox_out: dict) -> dict:
    """Inference over EVERY executed candidate, plus the lockbox finalists.

    The denominator comes from the registry, so a candidate cannot improve the
    campaign's statistics by being left out of a shortlist. Campaign v1 and v2
    results are structurally absent: they live under different campaign ids and
    their specification hashes were computed under a different judge.
    """
    reg = _registry.Registry(campaign_id)
    candidates = reg.executed(phase=None, role=_registry.ROLE_CANDIDATE)
    executed = [e for e in candidates if e["state"] == _registry.STATE_OK]

    per_candidate = []
    p_values = []
    for e in executed:
        p = (e.get("validation") or {}).get("primary") or {}
        t = p.get("net_excess_t_newey_west")
        pv = _mt.two_sided_p(t) if t is not None else None
        per_candidate.append({
            "candidate_id": e["candidate_id"], "spec_hash": e["spec_hash"],
            "family": e["family"], "phase": e["phase"], "track": e.get("track"),
            "net_excess_annualised": p.get("net_excess_annualised"),
            "t": t, "p_value": pv, "layer": "VALIDATION"})
        p_values.append(pv)
    bh = _mt.benjamini_hochberg(p_values)

    # SPA compares candidates against a benchmark ON THE SAME DATES, so it runs
    # over the PRIMARY sample at the PRIMARY horizon only - the one cell where
    # every candidate's series shares an identical validation date axis.
    spa_scope = [e for e in executed
                 if e["sample"] == _contract.PRIMARY_SAMPLE
                 and e["horizon_sessions"] == PRIMARY_HORIZON]
    series = {}
    for e in spa_scope:
        p = (e.get("validation") or {}).get("primary") or {}
        s = p.get("excess_series")
        if s:
            series[e["candidate_id"]] = np.asarray(s, dtype=np.float64)
    lengths = {len(v) for v in series.values()}
    if len(lengths) > 1:
        # Different candidates can skip different dates when the eligible index
        # cross-section is too thin. Truncating to a common length would compare
        # different periods, so SPA runs on the modal axis and says so.
        modal = max(lengths, key=lambda L: sum(1 for v in series.values()
                                               if len(v) == L))
        series = {k: v for k, v in series.items() if len(v) == modal}
    spa = _mt.superior_predictive_ability(series)
    spa["scope"] = {
        "sample": _contract.PRIMARY_SAMPLE,
        "horizon_sessions": PRIMARY_HORIZON,
        "candidates_in_scope": len(series),
        "candidates_executed_total": len(executed),
        "series_lengths": sorted(lengths),
        "common_date_axis": len(lengths) <= 1,
        "reason": "SPA requires a common date axis; candidates on other samples "
                  "or horizons are covered by the Benjamini-Hochberg control, "
                  "which does not",
    }

    paired = {"state": "NO_LOCKBOX_RESULT"}
    lb = [r for r in (lockbox_out or {}).get("results", [])
          if r.get("state") == "EXECUTED"]
    inc = next((r for r in lb if r["family"] == "incumbent_momentum_leg"), None)
    cands = [r for r in lb if r["family"] != "incumbent_momentum_leg"]
    if inc and cands:
        inc_s = np.asarray((inc["lockbox"]["primary"] or {}).get("excess_series") or [],
                           dtype=np.float64)
        best = max(cands, key=lambda r: (r["lockbox"]["primary"] or {})
                   .get("net_excess_annualised") or float("-inf"))
        b_s = np.asarray((best["lockbox"]["primary"] or {}).get("excess_series") or [],
                         dtype=np.float64)
        n = min(inc_s.size, b_s.size)
        if n >= 8:
            paired = _mt.paired_block_bootstrap(b_s[:n] - inc_s[:n])
            paired["candidate_id"] = best["candidate_id"]
            paired["incumbent_id"] = inc["candidate_id"]
            paired["layer"] = "LOCKBOX"
            paired["null"] = "CANDIDATE_DOES_NOT_BEAT_THE_INCUMBENT"

    body = _mt.build(campaign_id=campaign_id, denominator=len(candidates),
                     per_candidate=per_candidate, spa=spa, paired=paired, bh=bh)
    body["superseded_campaigns_excluded_from_denominator"] = list(
        _contract.SUPERSEDED_CAMPAIGNS)
    _mt.freeze(body)
    return body


# --------------------------------------------------------------------------- #
# Stage 11: economic frontier and terminal verdict
# --------------------------------------------------------------------------- #
def build_frontier(*, campaign_id: str = _contract.CAMPAIGN_ID,
                   lockbox_out: dict) -> dict:
    rows = []
    for r in (lockbox_out or {}).get("results", []):
        if r.get("state") != "EXECUTED":
            continue
        for gk, m in sorted((r["lockbox"].get("risk_frontier_gamma") or {}).items()):
            rows.append({
                "candidate_id": r["candidate_id"], "family": r["family"],
                "role": r["role"], "track": r.get("track"),
                "gamma_point": gk, "gamma_multiplier": m.get("gamma_multiplier"),
                "net_return_annualised": m.get("net_return_annualised"),
                "volatility_annualised": m.get("volatility_annualised"),
                "net_excess_annualised": m.get("net_excess_annualised"),
                "net_excess_vs_spy_annualised": m.get("net_excess_vs_spy_annualised"),
                "equal_weight_benchmark_annualised": m.get("equal_weight_benchmark_annualised"),
                "spy_benchmark_annualised": m.get("spy_benchmark_annualised"),
                "sharpe_net": m.get("sharpe_net"),
                "sortino_net": m.get("sortino_net"),
                "max_drawdown_net": m.get("max_drawdown_net"),
                "cvar_05_net": m.get("cvar_05_net"),
                "turnover_annualised": m.get("turnover_annualised"),
                "cost_drag_annualised": m.get("cost_drag_annualised"),
                "cash_weight_mean": m.get("cash_weight_mean"),
                "names_held_mean": m.get("names_held_mean"),
                "concentration_hhi_mean": m.get("concentration_hhi_mean"),
                "layer": "LOCKBOX"})
    body = {
        "contract": FRONTIER_SCHEMA, "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "risk_frontier_gamma_multipliers": list(_contract.RISK_FRONTIER_GAMMA_MULTIPLIERS),
        "primary_gamma_multiplier": _contract.PRIMARY_GAMMA_MULTIPLIER,
        "frozen_before_results": True,
        "frontier_varies_risk_appetite_not_concentration": True,
        "comparison_principle": "NET ECONOMICS AT COMPARABLE RISK",
        "benchmarks": list(_contract.BENCHMARKS_REPORTED),
        "rows": rows,
    }
    body["frontier_hash"] = r31.sha(body)
    body.update(r31.safety_block())
    r31.write_json(r31.campaign_dir(campaign_id) / FRONTIER_ARTIFACT, body)
    return body


def superiority_verdict(*, lockbox_out: dict, mt_out: dict) -> dict:
    """Apply the frozen superiority contract to the lockbox evidence.

    The contract's bar is stated against THE INCUMBENT. The incumbent may not be
    constructible as a zero-base capital allocator: the approved operational
    model's momentum leg is a rank SCORE, and reaching the allocator means passing
    the Track-A calibration, which refuses a score whose fitted slope is negative.
    That is not a failure of the campaign - it is the campaign measuring something
    true about the incumbent on entitled evidence.

    When that happens the comparison does not silently disappear. The bar is
    applied against the two references that need no calibration and are already
    computed for every candidate: the point-in-time S&P 500 equal-weight return
    and the S&P 500 total-return series. A candidate must clear the same numeric
    hurdle against the universe-neutral benchmark AND must not lose to the
    investable one - which is, if anything, a harder test than beating a model the
    evidence says does not work.
    """
    lb = [r for r in (lockbox_out or {}).get("results", [])
          if r.get("state") == "EXECUTED"]
    inc = next((r for r in lb if r["family"] == "incumbent_momentum_leg"), None)
    cands = [r for r in lb if r["family"] != "incumbent_momentum_leg"]
    if not cands:
        return {"winner": None, "reason": "NO_LOCKBOX_CANDIDATE_EVIDENCE",
                "checks": {}}

    best = max(cands, key=lambda r: (r["lockbox"]["primary"] or {})
               .get("net_excess_annualised") or float("-inf"))
    bp = best["lockbox"]["primary"] or {}

    def _f(x, d=0.0):
        return float(x) if x is not None else d

    if inc is not None:
        ip = inc["lockbox"]["primary"] or {}
        reference = "INCUMBENT_MOMENTUM_LEG"
        incumbent_state = "RECONSTRUCTED"
        incumbent_id = inc["candidate_id"]
    else:
        # The incumbent could not be priced. The equal-weight benchmark IS the
        # reference the excess statistic is already measured against, so a zero
        # incumbent excess is the correct, explicit substitution.
        #
        # Drawdown and turnover have NO such substitution. Copying the
        # candidate's own values into the reference would compare the candidate
        # against itself, and two blocking checks would then pass by
        # construction on every input. They are reported UNAVAILABLE instead,
        # and an unproven check does not pass.
        ip = {"net_excess_annualised": 0.0,
              "max_drawdown_net": None,
              "turnover_annualised": None}
        reference = _contract.BENCH_EQUAL_WEIGHT
        incumbent_state = "INCUMBENT_NOT_ECONOMICALLY_CALIBRATABLE"
        incumbent_id = None

    gain = _f(bp.get("net_excess_annualised")) - _f(ip.get("net_excess_annualised"))
    dd_det = (None if ip.get("max_drawdown_net") is None
              else _f(ip.get("max_drawdown_net")) - _f(bp.get("max_drawdown_net")))
    turn_ratio = (None if ip.get("turnover_annualised") is None
                  else _f(bp.get("turnover_annualised"))
                  / max(_f(ip.get("turnover_annualised"), 1e-9), 1e-9))
    win = _f((bp.get("robustness") or {}).get("subperiod_win_fraction"))
    spa_p = mt_out.get("superior_predictive_ability", {}).get("p_value")
    paired_p = mt_out.get("paired_vs_incumbent", {}).get("p_value")
    S = _contract.SUPERIORITY

    checks = {
        "net_gain_vs_incumbent": {
            "value": round(gain, 6),
            "required": S["min_net_annualised_excess_vs_incumbent"],
            "pass": gain >= S["min_net_annualised_excess_vs_incumbent"]},
        "drawdown_not_materially_worse": {
            "value": None if dd_det is None else round(dd_det, 6),
            "required_max": S["max_drawdown_deterioration"],
            "state": UNAVAILABLE_NO_INCUMBENT if dd_det is None else None,
            "pass": None if dd_det is None
                    else dd_det <= S["max_drawdown_deterioration"]},
        "turnover_ratio": {
            "value": None if turn_ratio is None else round(turn_ratio, 4),
            "required_max": S["max_turnover_ratio_vs_incumbent"],
            "state": UNAVAILABLE_NO_INCUMBENT if turn_ratio is None else None,
            "pass": None if turn_ratio is None
                    else turn_ratio <= S["max_turnover_ratio_vs_incumbent"]},
        "subperiod_stability": {
            "value": win,
            "required_min": S["min_subperiod_win_fraction"],
            "pass": win >= S["min_subperiod_win_fraction"]},
        "survives_spa": {
            "value": spa_p,
            "required_max": S["min_spa_p_value_reject"],
            "pass": spa_p is not None and float(spa_p) <= S["min_spa_p_value_reject"]},
        "beats_incumbent_paired_bootstrap": {
            "value": paired_p,
            "required_max": S["min_spa_p_value_reject"],
            "pass": paired_p is not None and float(paired_p) <= S["min_spa_p_value_reject"]},
        "evaluated_on_investment_universe_only": {
            "value": best["lockbox"].get("evaluation_universe"),
            "required": _contract.EVALUATION_UNIVERSE,
            "pass": best["lockbox"].get("evaluation_universe")
                    == _contract.EVALUATION_UNIVERSE},
        # CORRECTION 4 with teeth: a candidate that beats an equal-weight basket
        # of the same names but loses to the ETF anyone could have bought has not
        # earned a paper review, however good its selection skill looks.
        "does_not_lose_to_investable_benchmark": {
            "value": bp.get("net_excess_vs_spy_annualised"),
            "required_min": 0.0,
            "pass": (bp.get("net_excess_vs_spy_annualised") is not None
                     and float(bp["net_excess_vs_spy_annualised"]) >= 0.0)},
    }
    # ``is True`` on purpose: an UNAVAILABLE check carries ``None`` and must not
    # be counted as satisfied.
    allpass = all(c["pass"] is True for c in checks.values())
    unavailable = sorted(k for k, c in checks.items() if c.get("pass") is None)
    return {"winner": best["candidate_id"] if allpass else None,
            "checks_unavailable": unavailable,
            "best_lockbox_candidate": best["candidate_id"],
            "best_family": best["family"], "best_track": best.get("track"),
            "comparison_reference": reference,
            "incumbent_state": incumbent_state,
            "incumbent_id": incumbent_id,
            "best_vs_spy_annualised": bp.get("net_excess_vs_spy_annualised"),
            "checks": checks, "all_passed": allpass}


def final_verdict(*, campaign_id: str = _contract.CAMPAIGN_ID, ctx: Context,
                  known: dict, novel_out: dict, lockbox_out: dict,
                  mt_out: dict, frontier: dict, at: str) -> dict:
    """The ONE terminal verdict. Exactly one primary research state."""
    reg = _registry.Registry(campaign_id)
    sup = superiority_verdict(lockbox_out=lockbox_out, mt_out=mt_out)
    manifest = r31.read_json(_snapshot.path_for(campaign_id))
    part = ctx.partition
    umani = ctx.universe_manifest
    bmani = ctx.benchmark_manifest

    budget_spent = {
        "known_method_configs": len(reg.executed(phase=_registry.PHASE_KNOWN)),
        "known_method_budget": _contract.MAX_KNOWN_METHOD_CONFIGS,
        "novel_candidates": len(reg.executed(phase=_registry.PHASE_NOVEL)),
        "novel_budget": _contract.MAX_NOVEL_CANDIDATES_TOTAL,
        "lockbox_accesses": lockbox_out.get("access_count"),
        "lockbox_budget": _contract.MAX_LOCKBOX_CANDIDATES,
    }

    if sup.get("winner"):
        best_family = sup["best_family"]
        state = ("R31_NOVEL_ALPHA_SUPERIOR_MODEL_FOUND"
                 if str(best_family).startswith("novel_")
                 else "R31_KNOWN_METHOD_SUPERIOR_MODEL_FOUND")
        secondary = "MODEL_READY_FOR_MANUAL_PAPER_REVIEW"
        gap = None
    else:
        state = "R31_CURRENT_INFORMATION_MODEL_FRONTIER_EXHAUSTED"
        secondary = None
        gap = _information_gap(manifest, umani, known, novel_out)

    body = {
        "contract": VERDICT_SCHEMA,
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "generated_at": at,
        "git_head": _contract.git_head(),
        "primary_verdict": state,
        "secondary_verdict": secondary,
        "superiority": sup,
        "superiority_contract": dict(_contract.SUPERIORITY),
        "superseded_campaigns": dict(_contract.SUPERSEDED_CAMPAIGNS),
        "superseded_evidence_rules": dict(_contract.SUPERSEDED_EVIDENCE_RULES),
        "investment_universe": {
            "name": _contract.EVALUATION_UNIVERSE,
            "universe_hash": (umani or {}).get("universe_hash"),
            "median_members_per_session": ((umani or {}).get("investment_universe") or {})
                .get("median_members_per_session"),
            "survivorship": (umani or {}).get("survivorship"),
        },
        "training_universes": {
            "declared": list(_contract.TRAINING_UNIVERSES),
            "comparison": known.get("training_universe_comparison"),
        },
        "benchmarks": {
            "benchmark_hash": (bmani or {}).get("benchmark_hash"),
            "equal_weight": (bmani or {}).get("equal_weight"),
            "investable": (bmani or {}).get("investable"),
            "substitution_permitted": False,
        },
        "snapshot": {
            "hash": ctx.snap.content_hash,
            "cross_sections": manifest["cross_sections_total"],
            "samples": {k: {"cross_sections": v["cross_sections"],
                            "first_date": v["first_date"],
                            "last_date": v["last_date"],
                            "survivorship": v["survivorship"],
                            "may_carry_verdict": v["may_carry_verdict"]}
                        for k, v in manifest["samples"].items()},
            "survivorship_measurement": manifest["survivorship_measurement"],
        },
        "covariance_cache": {
            "key": ctx.cov.key,
            "sections_cached": int(ctx.cov.sections.size),
            "owner": _contract.CANONICAL_COVARIANCE_OWNER,
            "reused_by_every_candidate": True,
        },
        "evidence_partition": {
            sample: {"state": blk["state"],
                     "horizons": {h: {"counts": p["counts"], "dates": p["dates"]}
                                  for h, p in blk["horizons"].items()}}
            for sample, blk in part["samples"].items()},
        "budgets_spent": budget_spent,
        "known_method": {
            "families_executed": known.get("families_executed"),
            "configs_executed": known.get("executed_configs"),
            "best": (known.get("leaderboard") or [None])[0],
            "benchmarks": known.get("benchmarks"),
            "calibration_rejections": known.get("calibration_rejections"),
        },
        "novel": {
            "families_executed": novel_out.get("families_executed"),
            "candidates_executed": novel_out.get("candidates_executed"),
            "campaigns": novel_out.get("campaigns"),
            "exhaustion_triggered": novel_out.get("exhaustion_triggered"),
            "best": (novel_out.get("leaderboard") or [None])[0],
        },
        "lockbox": {
            "access_count": lockbox_out.get("access_count"),
            "budget": _contract.MAX_LOCKBOX_CANDIDATES,
            "finalists": [f["candidate_id"]
                          for f in lockbox_out.get("finalists", {}).get("finalists", [])],
            "results": [{"candidate_id": r["candidate_id"],
                         "family": r["family"], "track": r.get("track"),
                         "state": r["state"],
                         "net_excess_annualised": ((r.get("lockbox") or {}).get("primary") or {})
                         .get("net_excess_annualised"),
                         "net_excess_vs_spy_annualised": ((r.get("lockbox") or {}).get("primary") or {})
                         .get("net_excess_vs_spy_annualised"),
                         "sharpe_net": ((r.get("lockbox") or {}).get("primary") or {})
                         .get("sharpe_net"),
                         "cash_weight_mean": ((r.get("lockbox") or {}).get("primary") or {})
                         .get("cash_weight_mean"),
                         "max_drawdown_net": ((r.get("lockbox") or {}).get("primary") or {})
                         .get("max_drawdown_net")}
                        for r in lockbox_out.get("results", [])],
        },
        "multiple_testing": {
            "denominator": mt_out.get("denominator_executed_candidates"),
            "bh": mt_out.get("benjamini_hochberg"),
            "spa": mt_out.get("superior_predictive_ability"),
            "paired_vs_incumbent": mt_out.get("paired_vs_incumbent"),
        },
        "economic_frontier_hash": frontier.get("frontier_hash"),
        "information_gap": gap,
        "intrinio_extension_readiness": _intrinio_readiness(),
        "event_news_decision": {
            "gdelt_and_news_text": "EXCLUDED",
            "reason": _contract.INADMISSIBLE_INFORMATION["gdelt_news_text"],
            "external_reference_links": "EXCLUDED",
            "campaign_contains_no_news_derived_feature": True,
        },
        "architecture_comparison": _architecture_comparison(known, novel_out),
        "operational_model_comparison": _operational_comparison(known, lockbox_out),
        "production_state": "READ_ONLY_THROUGHOUT_RELEASE_31",
    }
    body["final_verdict_hash"] = r31.sha(body)
    body.update(r31.safety_block())
    r31.write_json(r31.campaign_dir(campaign_id) / VERDICT_ARTIFACT, body)
    return body


def _information_gap(manifest: dict, umani: dict, known: dict,
                     novel_out: dict) -> dict:
    surv = manifest["survivorship_measurement"]
    usurv = (umani or {}).get("survivorship") or {}
    return {
        "dominant_constraint": "INFORMATION_NOT_METHOD",
        "statement": (
            "the bounded search over the CURRENT information set found no "
            "decision function that beats the incumbent's net implementable "
            "economics at comparable risk, on evidence that played no part in "
            "selecting it, over the point-in-time S&P 500 investment universe; "
            "further search over the SAME information is more likely to increase "
            "data-mining risk than useful knowledge"),
        "measured_limits": {
            "fundamental_coverage_ratio_alive_over_delisted":
                surv.get("coverage_ratio_alive_over_delisted"),
            "fundamental_covered_ciks": surv.get("covered_ciks"),
            "fundamental_sample_may_carry_verdict":
                manifest["samples"][_contract.SAMPLE_FUND_MATCHED]["may_carry_verdict"],
            "price_sample_cross_sections":
                manifest["samples"][_contract.SAMPLE_PRICE_FULL]["cross_sections"],
            "investment_universe_missing_member_day_fraction":
                usurv.get("missing_fraction"),
            "investment_universe_verdict": usurv.get("verdict"),
            "historical_sector_constraint": _contract.HISTORICAL_SECTOR_CONSTRAINT,
        },
        "next_action": (
            "genuinely new ORTHOGONAL information - the pending Intrinio "
            "historical analyst-revision sample, or a survivorship-complete "
            "point-in-time fundamental history - evaluated in THIS framework"),
    }


def _intrinio_readiness() -> dict:
    return {
        "state": "READY_FOR_EXTENSION_NOT_ACQUIRED",
        "no_fabricated_history": True,
        "extension_contract": (
            "a new data family is added to alpha_agent.r31.snapshot as one more "
            "entry in the manifest's data_families list, with its own PIT "
            "semantics, publication-date semantics, delisting handling and "
            "measured survivorship coverage; its features join the frozen "
            "feature order under a new campaign id, and the SAME judge, "
            "investment universe, benchmarks, partition and budgets apply "
            "unchanged"),
        "requires_new_campaign_id": True,
        "reason": "adding a data family changes the snapshot hash, which is "
                  "bound into the campaign contract",
    }


def _architecture_comparison(known: dict, novel_out: dict) -> dict:
    def _best_of(rows, track):
        cands = [r for r in (rows or []) if r.get("track") == track]
        return max(cands, key=lambda r: r.get("net_excess_annualised")
                   if r.get("net_excess_annualised") is not None else float("-inf"),
                   default=None)

    lb = (known.get("leaderboard") or []) + (novel_out.get("leaderboard") or [])
    return {
        _contract.TRACK_A: _best_of(lb, _contract.TRACK_A),
        _contract.TRACK_B: _best_of(lb, _contract.TRACK_B),
        "note": ("both architectures were run under the same judge, the same "
                 "investment universe, the same canonical cost, covariance and "
                 "constraint owners and the same evidence partition, so the "
                 "comparison is of architectures rather than of evaluation "
                 "conventions"),
    }


def _operational_comparison(known: dict, lockbox_out: dict) -> dict:
    bench = {b["family"]: b for b in (known.get("benchmarks") or [])}
    return {
        "operational_model_id": "fundamental_momentum_50_50_v1",
        "operational_model_unchanged": True,
        "point_in_time_reconstruction_on_price_sample": bench.get("incumbent_momentum_leg"),
        "s25_operating_profitability_standalone": bench.get("s25_operating_profitability"),
        "release30_1_negative_slope_calibration_revived": False,
        "rank_identity_contract_respected": True,
        "no_model_activated": True,
        "no_champion_promoted": True,
    }
