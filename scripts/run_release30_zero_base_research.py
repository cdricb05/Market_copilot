"""scripts/run_release30_zero_base_research.py - Release 30 research runner.

RESEARCH ONLY. Runs the Release 30 walk-forward forecasting tournament over the
owned point-in-time data and writes every artifact under an ISOLATED Release 30
research root. It never touches an operational store, never promotes a model,
never creates a proposal, an order or a decision, and never restarts a service.

    py -3 scripts/run_release30_zero_base_research.py --stage tournament

Stages:

    panel        build (and cache) the point-in-time datasets only
    tournament   the full walk-forward tournament + frozen model artifact
    emit         emit the CURRENT-date forecast input for the operational lane

Output root (override with ``PAPER_TRADER_R30_ROOT``):

    D:\\Stock_Prediction_app_data\\release30_zero_base_adaptive_allocator
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

_REPO_PARENT = str(Path(__file__).resolve().parents[2])
if _REPO_PARENT not in sys.path:
    sys.path.insert(0, _REPO_PARENT)

import numpy as np  # noqa: E402

from paper_trader.alpha_agent import release30_forecast_research as rf  # noqa: E402
from paper_trader.alpha_agent import release30_models as rm  # noqa: E402
from paper_trader.alpha_agent import release30_panel as rp  # noqa: E402
from paper_trader.alpha_agent import stage24_pit_fundamental as s24  # noqa: E402
from paper_trader.alpha_agent import stage25_alpha_discovery as s25  # noqa: E402

ROOT_ENV = "PAPER_TRADER_R30_ROOT"
DEFAULT_ROOT = Path(
    r"D:\Stock_Prediction_app_data\release30_zero_base_adaptive_allocator")

SAFETY = ["RESEARCH ONLY", "READ ONLY", "NO ORDERS", "NO LIVE PROMOTION",
          "PREVIEW ONLY", "MANUAL REVIEW", "AUTOMATION OFF"]


def root() -> Path:
    p = Path(os.environ.get(ROOT_ENV) or DEFAULT_ROOT)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _write(name: str, payload) -> Path:
    p = root() / name
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=1, default=str), encoding="utf-8")
    tmp.replace(p)
    return p


# --------------------------------------------------------------------------- #
# Dataset caching
# --------------------------------------------------------------------------- #
def _cache_path(tag: str) -> Path:
    return root() / "_cache" / ("dataset_%s.npz" % tag)


def _save_dataset(ds: rf.Dataset, tag: str) -> None:
    p = _cache_path(tag)
    p.parent.mkdir(parents=True, exist_ok=True)
    blob = {"feature_names": list(ds.feature_names),
            "diagnostics": ds.diagnostics, "panel_source": ds.panel_source,
            "dates": [s.date for s in ds.sections],
            "t": [s.t for s in ds.sections],
            "horizons": sorted({int(h) for s in ds.sections for h in s.labels})}
    arrays = {"meta": np.frombuffer(json.dumps(blob).encode("utf-8"),
                                    dtype=np.uint8)}
    for i, s in enumerate(ds.sections):
        arrays["cols_%d" % i] = s.cols
        arrays["X_%d" % i] = s.X
        arrays["adv_%d" % i] = s.adv
        arrays["hf_%d" % i] = s.has_fundamentals
        for n in ds.feature_names:
            arrays["raw_%d_%s" % (i, n)] = s.raw[n]
        for h in s.labels:
            arrays["y_%d_%d" % (i, h)] = s.labels[h]
            arrays["tr_%d_%d" % (i, h)] = s.truncated[h]
    np.savez_compressed(p, **arrays)


def _load_dataset(tag: str) -> "rf.Dataset | None":
    p = _cache_path(tag)
    if not p.exists():
        return None
    z = np.load(p, allow_pickle=False)
    blob = json.loads(bytes(z["meta"]).decode("utf-8"))
    ds = rf.Dataset(feature_names=tuple(blob["feature_names"]),
                    panel_source=blob["panel_source"])
    ds.diagnostics = blob["diagnostics"]
    for i, date in enumerate(blob["dates"]):
        ds.sections.append(rf.CrossSection(
            t=int(blob["t"][i]), date=date, cols=z["cols_%d" % i],
            X=z["X_%d" % i],
            raw={n: z["raw_%d_%s" % (i, n)] for n in blob["feature_names"]},
            labels={int(h): z["y_%d_%d" % (i, h)] for h in blob["horizons"]},
            truncated={int(h): z["tr_%d_%d" % (i, h)] for h in blob["horizons"]},
            adv=z["adv_%d" % i], has_fundamentals=z["hf_%d" % i]))
    return ds


def _pit_sources():
    bridge = s24.IdentityBridge()
    bridge.load()
    store = s24.Stage24PitStore(db_path=s25.DEFAULT_CF_INDEX)
    return bridge, store


def build_datasets(force: bool = False) -> dict:
    """Universe A (price-only, survivorship-free) and Universe B (fundamental
    augmented, restricted to the coverage-matched sub-sample)."""
    t0 = time.time()
    panel = rp.load_price_panel()
    out = {}
    ds_a = None if force else _load_dataset("price_only")
    if ds_a is None:
        ds_a = rf.build_dataset(panel)
        _save_dataset(ds_a, "price_only")
    out["price_only"] = ds_a

    ds_b = None if force else _load_dataset("fundamental_matched")
    if ds_b is None:
        bridge, store = _pit_sources()
        ciks = sorted({c for c in (bridge.cik_for(str(s)) for s in panel.symbols)
                       if c})
        store.load(ciks=ciks)
        full = rf.build_dataset(panel, with_fundamentals=True, store=store,
                                bridge=bridge)
        ds_b = rf.restrict_to_fundamental_coverage(full)
        ds_b.diagnostics["unrestricted"] = full.diagnostics
        _save_dataset(ds_b, "fundamental_matched")
    out["fundamental_matched"] = ds_b
    out["_seconds"] = round(time.time() - t0, 1)
    return out


# --------------------------------------------------------------------------- #
# Coverage / point-in-time integrity report
# --------------------------------------------------------------------------- #
def integrity_report(panel: rp.PricePanel, datasets: dict) -> dict:
    bridge = s24.IdentityBridge()
    bridge.load()
    syms = [str(s) for s in panel.symbols]
    # A symbol that is a member on the LAST panel day is still listed; one that
    # was a member earlier and is not now has left the index or the market. The
    # split matters because it is exactly the axis survivorship runs along.
    ever = panel.member.any(axis=0)
    now = panel.member[-1]
    gone = ever & ~now
    resolved = np.array([bridge.cik_for(s) is not None for s in syms])
    def _rate(mask):
        n = int(mask.sum())
        return {"symbols": n,
                "cik_resolved": int((mask & resolved).sum()),
                "rate": round(float((mask & resolved).sum() / n), 4) if n else None}
    still, left = _rate(now), _rate(gone)
    skew = ((still["rate"] / left["rate"]) if (still["rate"] and left["rate"])
            else None)
    ds_a, ds_b = datasets["price_only"], datasets["fundamental_matched"]
    return {
        "price_family": {
            "source": panel.source,
            "universe": "Norgate Russell 1000 Current & Past (delisted retained)",
            "membership": "PIT per-session mask carried by the owned panel",
            "survivorship_safe": True,
            "decision_dates": len(ds_a.sections),
            "rows": ds_a.diagnostics.get("rows"),
            "label_truncated_rows": ds_a.diagnostics.get("label_truncated_rows"),
            "label_policy": ("a name that stops trading inside the forward window "
                             "is measured to its LAST OWNED CLOSE and retained"),
        },
        "fundamental_family": {
            "source": str(s25.DEFAULT_CF_INDEX),
            "pit_contract": "SEC filed date, reporting lag %d days"
                            % s24.REPORTING_LAG_DAYS,
            "identity_bridge": "alpha_agent.stage24_pit_fundamental.IdentityBridge",
            "still_member_symbols": still,
            "left_universe_symbols": left,
            "resolution_survivorship_skew": (round(skew, 3) if skew else None),
            "survivorship_safe": False,
            "survivorship_verdict": "COVERAGE_SURVIVORSHIP_SKEWED",
            "why": ("issuer resolution succeeds far more often for names still in "
                    "the universe than for names that left it, so the rows on "
                    "which any fundamental factor is DEFINED are a survivor-"
                    "skewed sub-sample of the point-in-time universe"),
            "control": ("every fundamental comparison is run on the coverage-"
                        "MATCHED sub-sample, so both sides see identical rows and "
                        "the comparison isolates the forecast rather than the "
                        "sample"),
            "decision_dates": len(ds_b.sections),
            "rows": ds_b.diagnostics.get("rows"),
        },
        "sector": {
            "used_as_historical_feature": False,
            "reason": ("the canonical PIT sector owner classifies the owned "
                       "entity-level SIC snapshot as ENTITY_SIC_SNAPSHOT_CONTROL, "
                       "inadmissible for signal construction"),
            "used_for": "CURRENT-date portfolio construction sector cap only",
        },
        "controls": [
            "no random train/test split - blocks are contiguous and ordered",
            "no future normalisation - every transform is per-decision-date",
            "no future membership - the panel's own PIT mask is used",
            "no future fundamentals - visibility is the SEC filed date",
            "no future labels - an embargo of ceil(horizon/step) decision dates "
            "separates train, validation and test",
        ],
    }


# --------------------------------------------------------------------------- #
# Frozen artifact
# --------------------------------------------------------------------------- #
def freeze_model(ds: rf.Dataset, tourney: dict, *, universe_tag: str,
                 seed: int = 30) -> dict:
    """Fit the DEPLOYMENT model on all labelled history, using only
    hyper-parameters and ensemble weights that the walk-forward evidence already
    chose. Nothing here reads a TEST statistic."""
    grid = {e["model_id"]: e for e in rf.model_grid(ds.feature_names)}
    horizons: dict = {}
    n = len(ds.sections)
    for h in tourney["horizons"]:
        blk = tourney["by_horizon"][str(h)]
        if not blk.get("folds") or not (blk.get("ensemble") or {}).get("weights"):
            continue
        ens = blk["ensemble"]
        members = []
        for mid, w in sorted(ens["weights"].items()):
            if w <= 0:
                continue
            entry = grid[mid]
            if entry["kind"] == "fixed":
                spec = entry["spec"]
            else:
                picks = [p for p in blk["selected_hyperparameters"][mid] if p]
                if not picks:
                    continue
                # The LAST fold's validation-chosen hyper-parameters are the most
                # recent evidence available without reading any test block.
                cutoff = n - rf.embargo_dates(h)
                X, y = rf.stack_block(ds, 0, max(0, cutoff), h)
                spec = rf.fit_learner(entry, X, y, picks[-1], seed)
            members.append({"model_id": mid, "weight": float(w), "spec": spec})
        if not members:
            continue
        ensemble = {"kind": rm.KIND_ENSEMBLE, "members": members}
        horizons[str(h)] = {
            "horizon_sessions": int(h),
            "model": ensemble,
            "member_ids": [m["model_id"] for m in members],
            "weights": {m["model_id"]: m["weight"] for m in members},
            "weighting_method": ens["method"],
            "calibration": _calibrate(ensemble, ds, h),
            "risk_prices": rf.calibrate_risk_prices(ensemble, ds, h),
            "training_cutoff": ds.sections[max(0, n - rf.embargo_dates(h)) - 1].date,
        }
    artifact = {
        "contract": rm.MODEL_CONTRACT,
        "research_version": rf.RESEARCH_VERSION,
        "panel_version": rp.PANEL_VERSION,
        "universe_tag": universe_tag,
        "feature_names": list(ds.feature_names),
        "feature_transform": "PER_DATE_RANK_TO_MINUS_HALF_PLUS_HALF_MISSING_ZERO",
        "target": "FORWARD_EXCESS_RETURN_VS_CROSS_SECTIONAL_MEAN",
        "horizons": horizons,
        "seed": int(seed),
        "safety": SAFETY,
        "automatic_promotion_allowed": False,
        "activation": "MANUAL_PAPER_APPROVAL_REQUIRED",
    }
    artifact["model_spec_hash"] = rp.sha(
        {k: v for k, v in artifact.items() if k != "model_spec_hash"})
    return artifact


def _calibrate(spec: dict, ds: rf.Dataset, horizon: int) -> dict:
    """Map a standardised model score onto an EXPECTED EXCESS RETURN, and measure
    the dispersion around that map.

    Both numbers come from the same walk-forward validation blocks that chose the
    weights; neither is a chosen constant. ``expected_excess_return = slope *
    standardised_score`` and ``forecast_uncertainty`` starts at
    ``residual_sigma``.
    """
    fl = rf.folds(len(ds.sections), horizon)
    P, Y = [], []
    for f in fl:
        for s in ds.sections[f["valid"][0]:f["valid"][1]]:
            p = rm.standardise(rm.predict(spec, s.X, ds.feature_names))
            y = rf.excess_target(s.labels[horizon])
            ok = np.isfinite(p) & np.isfinite(y)
            P.append(p[ok])
            Y.append(y[ok])
    if not P:
        return {"state": "UNCALIBRATED"}
    p = np.concatenate(P)
    y = np.concatenate(Y)
    denom = float((p * p).sum())
    slope = float((p * y).sum() / denom) if denom > 0 else 0.0
    resid = y - slope * p
    lo = float(np.quantile(resid, 0.05))
    return {
        "state": "CALIBRATED",
        "basis": "WALK_FORWARD_VALIDATION_BLOCKS",
        "n_rows": int(p.size),
        "slope": slope,
        "residual_sigma": float(resid.std(ddof=1)),
        "residual_q05": lo,
        "expected_excess_return_formula": "slope * standardised_score",
        "downside_quantile": 0.05,
    }


# --------------------------------------------------------------------------- #
# Verdict
# --------------------------------------------------------------------------- #
#: A candidate must beat the frozen operational benchmark on EVERY one of these,
#: on the strictly-untouched test blocks, before it may be called superior. They
#: are separate because a model can buy IC with turnover, or buy return with
#: drawdown, and either trade is a different decision from "the forecast is
#: better".
GO_CRITERIA = (
    ("rank_ic_mean", "candidate rank IC exceeds the benchmark's"),
    ("rank_ic_t", "candidate rank IC is itself statistically distinguishable "
                  "from zero (Newey-West t >= 2)"),
    ("net_return", "candidate long-only book earns more AFTER transaction costs"),
    ("information_ratio", "candidate book's risk-adjusted return is higher"),
    ("max_drawdown", "candidate book's worst drawdown is no worse"),
    ("paired_t", "the per-period net return DIFFERENCE is distinguishable from "
                 "zero (paired Newey-West t >= 2)"),
)
MIN_T = 2.0


def verdict(tourney: dict, *, benchmark_id: str, candidate_id: str) -> dict:
    per_h = {}
    for h in tourney["horizons"]:
        blk = tourney["by_horizon"][str(h)]
        models = blk.get("models") or {}
        b, c = models.get(benchmark_id), models.get(candidate_id)
        if not b or not c:
            per_h[str(h)] = {"state": "NOT_EVALUABLE"}
            continue
        bn = np.array(b["book"]["net_series"])
        cn = np.array(c["book"]["net_series"])
        m = min(bn.size, cn.size)
        diff = cn[:m] - bn[:m]
        paired_t = rf.newey_west_t(diff, max(0, rf.embargo_dates(h) - 1))
        checks = {
            "rank_ic_mean": c["test"]["rank_ic_mean"] > b["test"]["rank_ic_mean"],
            "rank_ic_t": (c["test"]["rank_ic_t"] or 0) >= MIN_T,
            "net_return": (c["book"]["annualised_net_return"]
                           > b["book"]["annualised_net_return"]),
            "information_ratio": (c["book"]["information_ratio"]
                                  > b["book"]["information_ratio"]),
            "max_drawdown": c["book"]["max_drawdown"] >= b["book"]["max_drawdown"],
            "paired_t": (paired_t or 0) >= MIN_T,
        }
        per_h[str(h)] = {
            "state": "PASS" if all(checks.values()) else "FAIL",
            "checks": checks,
            "failed": sorted([k for k, v in checks.items() if not v]),
            "benchmark": {"rank_ic_mean": b["test"]["rank_ic_mean"],
                          "rank_ic_t": b["test"]["rank_ic_t"],
                          "annualised_net_return": b["book"]["annualised_net_return"],
                          "information_ratio": b["book"]["information_ratio"],
                          "max_drawdown": b["book"]["max_drawdown"],
                          "mean_one_way_turnover": b["book"]["mean_one_way_turnover"]},
            "candidate": {"rank_ic_mean": c["test"]["rank_ic_mean"],
                          "rank_ic_t": c["test"]["rank_ic_t"],
                          "annualised_net_return": c["book"]["annualised_net_return"],
                          "information_ratio": c["book"]["information_ratio"],
                          "max_drawdown": c["book"]["max_drawdown"],
                          "mean_one_way_turnover": c["book"]["mean_one_way_turnover"]},
            "net_return_difference_annualised": (
                c["book"]["annualised_net_return"] - b["book"]["annualised_net_return"]),
            "paired_net_return_t": paired_t,
        }
    passed = [h for h, v in per_h.items() if v.get("state") == "PASS"]
    return {
        "benchmark_model_id": benchmark_id,
        "candidate_model_id": candidate_id,
        "criteria": [{"key": k, "meaning": m} for k, m in GO_CRITERIA],
        "min_t": MIN_T,
        "by_horizon": per_h,
        "horizons_passed": sorted(passed),
        "forecast_model_verdict": (
            "R30_ADAPTIVE_MODEL_READY_FOR_MANUAL_PAPER_APPROVAL" if passed
            else "R30_ADAPTIVE_MODEL_NO_GO"),
        "promotion": "MANUAL ONLY - no automatic promotion is implemented",
    }


# --------------------------------------------------------------------------- #
# Stages
# --------------------------------------------------------------------------- #
def stage_panel(args) -> int:
    ds = build_datasets(force=args.force)
    panel = rp.load_price_panel()
    rep = integrity_report(panel, ds)
    _write("point_in_time_integrity.json", rep)
    print("R30_PANEL_OK price_only_dates=%d fundamental_dates=%d %.1fs"
          % (len(ds["price_only"].sections), len(ds["fundamental_matched"].sections),
             ds["_seconds"]))
    return 0


def stage_tournament(args) -> int:
    t0 = time.time()
    datasets = build_datasets(force=args.force)
    panel = rp.load_price_panel()
    _write("point_in_time_integrity.json", integrity_report(panel, datasets))
    out = {}
    for tag in ("price_only", "fundamental_matched"):
        ds = datasets[tag]
        print("[%s] tournament over %d decision dates ..." % (tag, len(ds.sections)))
        tourney = rf.run_tournament(ds, seed=args.seed)
        art = freeze_model(ds, tourney, universe_tag=tag, seed=args.seed)
        bench = ("baseline_operational_blend_pit"
                 if tag == "fundamental_matched" else "baseline_momentum_leg")
        vd = verdict(tourney, benchmark_id=bench, candidate_id=rf.ADAPTIVE_ID)
        _write("tournament_%s.json" % tag, tourney)
        _write("model_artifact_%s.json" % tag, art)
        _write("verdict_%s.json" % tag, vd)
        out[tag] = {"verdict": vd["forecast_model_verdict"],
                    "horizons_passed": vd["horizons_passed"],
                    "model_spec_hash": art["model_spec_hash"]}
        print("[%s] %s passed=%s" % (tag, vd["forecast_model_verdict"],
                                     vd["horizons_passed"]))
    _write("tournament_summary.json",
           {"generated_seconds": round(time.time() - t0, 1), "results": out,
            "safety": SAFETY})
    print("R30_TOURNAMENT_COMPLETE %.1fs" % (time.time() - t0))
    return 0


#: The single feature name under which the CURRENT operational model's own
#: cross-sectional score is carried, so it can be forecast through exactly the
#: same contract as the candidate.
OPERATIONAL_FEATURE = "operational_combined_score"


def operational_artifact(*, datasets: dict, seed: int = 30) -> dict:
    """A frozen artifact for the CURRENT OPERATIONAL model.

    The operational champion ``fundamental_momentum_50_50_v1`` is a fixed
    cross-sectional score, so its "model" is the identity on that score. What it
    needs, and what this builds, is the CALIBRATION that turns that score into an
    expected excess return on the same scale as the candidate's - taken from the
    walk-forward validation of the point-in-time RECONSTRUCTION of the same model
    (``baseline_operational_blend_pit``).

    That reconstruction, not the live champion, is what history can calibrate:
    the live ``composite_sn`` panel is survivor-biased before 2016 and cannot be
    replayed survivorship-safe. The approximation is declared in the artifact
    rather than hidden, because the alternative - leaving the operational model
    uncalibrated - would make the A/B comparison meaningless.
    """
    ds = datasets["fundamental_matched"]
    grid = {e["model_id"]: e for e in rf.model_grid(ds.feature_names)}
    spec = grid["baseline_operational_blend_pit"]["spec"]
    horizons = {}
    for h in rp.HORIZONS:
        if not rf.folds(len(ds.sections), h):
            continue
        horizons[str(h)] = {
            "horizon_sessions": int(h),
            "model": rm.rank_blend_spec({OPERATIONAL_FEATURE: 1.0}),
            "member_ids": ["operational_combined_score"],
            "weights": {"operational_combined_score": 1.0},
            "weighting_method": "FROZEN_OPERATIONAL_CHAMPION_NO_FITTING",
            "calibration": _calibrate(spec, ds, h),
            "risk_prices": rf.calibrate_risk_prices(spec, ds, h),
            "training_cutoff": ds.sections[-1].date,
        }
    art = {
        "contract": rm.MODEL_CONTRACT,
        "research_version": rf.RESEARCH_VERSION,
        "panel_version": rp.PANEL_VERSION,
        "universe_tag": "operational",
        "feature_names": [OPERATIONAL_FEATURE],
        "feature_transform": "PER_DATE_RANK_TO_MINUS_HALF_PLUS_HALF_MISSING_ZERO",
        "target": "FORWARD_EXCESS_RETURN_VS_CROSS_SECTIONAL_MEAN",
        "horizons": horizons,
        "seed": int(seed),
        "safety": SAFETY,
        "automatic_promotion_allowed": False,
        "activation": "CURRENT_OPERATIONAL_MODEL",
        "calibration_source": "baseline_operational_blend_pit walk-forward validation",
        "calibration_caveat": (
            "the live composite_sn panel cannot be replayed survivorship-safe "
            "before 2016, so the point-in-time structural reconstruction of the "
            "same 50/50 model supplies the slope and dispersion"),
    }
    art["model_spec_hash"] = rp.sha(
        {k: v for k, v in art.items() if k != "model_spec_hash"})
    return art


def stage_acceptance(args) -> int:
    """The Aug-18 zero-base acceptance test: model A vs model B, one allocator.

    Both models see the SAME universe, date, constraints, risk, liquidity, cost
    assumptions and capital, so any difference between the two targets is a
    difference between FORECASTS - not between two portfolio-construction
    pipelines. Research evidence only: nothing is written to an operational
    store, no proposal is created and no history is mutated.
    """
    from paper_trader.api import portfolio_state as ps_owner
    from paper_trader.api import price_panel as pp
    from paper_trader.api import universe_scoring as us
    from paper_trader.api import zero_base_target as zbt
    from paper_trader.engine import return_forecast as fk
    from paper_trader.engine import zero_base_allocator as zk

    datasets = build_datasets()
    scoring = us.load_universe_scoring()
    ps = ps_owner.load_portfolio_state()
    panel = pp.load_operational_price_panel()

    art_b = json.loads((root() / "model_artifact_price_only.json")
                       .read_text(encoding="utf-8"))
    ic_b = json.loads((root() / "forecast_input_price_only.json")
                      .read_text(encoding="utf-8"))
    art_a = operational_artifact(datasets=datasets, seed=args.seed)
    _write("model_artifact_operational.json", art_a)
    ic_a = json.loads((root() / "forecast_input_operational.json")
                      .read_text(encoding="utf-8"))

    fc_a = fk.build_forecast(cross_section=ic_a, artifact=art_a)
    fc_b = fk.build_forecast(cross_section=ic_b, artifact=art_b)

    # ONE policy object for BOTH runs. Built from the CANDIDATE's calibration so
    # neither model is judged under risk prices derived from its own book.
    pol = zbt.resolve_policy(artifact=art_b)
    out = {}
    for label, fc, art in (("A_current_operational_model", fc_a, art_a),
                           ("B_adaptive_ensemble_candidate", fc_b, art_b)):
        ic = zbt.build_input_contract(portfolio_state=ps, scoring=scoring,
                                      forecast=fc, price_panel=panel, policy=pol)
        res = zk.build_allocation(input_contract=ic, policy=pol)
        res["forecast_state"] = fc.get("state")
        res["model_spec_hash"] = art.get("model_spec_hash")
        out[label] = res
        _write("aug18_zero_base_%s.json" % label, res)

    def _e(r, key):
        return ((r.get(key) or {}).get("economics") or {})

    a, b = out["A_current_operational_model"], out["B_adaptive_ensemble_candidate"]
    comparison = {
        "decision_date": scoring.get("eligible_market_date"),
        "capital_nav": a.get("nav"),
        "identical_inputs": {
            "universe": True, "date": True, "constraints": True, "risk": True,
            "liquidity": True, "transaction_costs": True, "capital": True,
            "policy_hash": rp.sha(pol),
        },
        "feature_as_of_date": ic_b.get("as_of_date"),
        "feature_panel_behind_eligible_session": ic_b.get(
            "feature_panel_behind_eligible_session"),
        "models": {
            "A_current_operational_model": {
                "model_id": scoring.get("primary_model_id"),
                "model_spec_hash": art_a.get("model_spec_hash"),
                "zero_base": _e(a, "zero_base_target"),
                "implementable": _e(a, "implementable_target"),
                "transition": (a.get("transition") or {}).get(
                    "current_to_implementable"),
                "overlap": (a.get("comparison") or {}).get("zero_base"),
            },
            "B_adaptive_ensemble_candidate": {
                "model_id": "adaptive_ensemble",
                "model_spec_hash": art_b.get("model_spec_hash"),
                "zero_base": _e(b, "zero_base_target"),
                "implementable": _e(b, "implementable_target"),
                "transition": (b.get("transition") or {}).get(
                    "current_to_implementable"),
                "overlap": (b.get("comparison") or {}).get("zero_base"),
            },
        },
        "current_portfolio": _e(a, "current_portfolio"),
        "attribution_doc": (
            "Both columns are produced by the SAME allocator under the SAME "
            "policy, so the difference between them is attributable to the "
            "FORECAST alone. The difference between either column and the "
            "current portfolio is attributable to the construction logic."),
        "safety": SAFETY,
        "mutates_nothing": True,
    }
    _write("aug18_zero_base_acceptance.json", comparison)
    print("R30_AUG18_ACCEPTANCE_OK date=%s A_positions=%s B_positions=%s"
          % (comparison["decision_date"],
             _e(a, "zero_base_target").get("position_count"),
             _e(b, "zero_base_target").get("position_count")))
    return 0


def stage_finalize(args) -> int:
    """Rebuild the frozen artifacts and verdicts from a COMPLETED tournament.

    Separate from ``tournament`` so the artifact contract can evolve without
    paying for the walk-forward again - and so nothing is ever tempted to
    shortcut the tournament itself.
    """
    datasets = build_datasets()
    out = {}
    for tag in ("price_only", "fundamental_matched"):
        tourney = json.loads((root() / ("tournament_%s.json" % tag))
                             .read_text(encoding="utf-8"))
        ds = datasets[tag]
        art = freeze_model(ds, tourney, universe_tag=tag, seed=args.seed)
        bench = ("baseline_operational_blend_pit"
                 if tag == "fundamental_matched" else "baseline_momentum_leg")
        vd = verdict(tourney, benchmark_id=bench, candidate_id=rf.ADAPTIVE_ID)
        _write("model_artifact_%s.json" % tag, art)
        _write("verdict_%s.json" % tag, vd)
        out[tag] = {"verdict": vd["forecast_model_verdict"],
                    "horizons_passed": vd["horizons_passed"],
                    "model_spec_hash": art["model_spec_hash"]}
        print("[%s] %s passed=%s" % (tag, vd["forecast_model_verdict"],
                                     vd["horizons_passed"]))
    _write("tournament_summary.json", {"results": out, "safety": SAFETY})
    print("R30_FINALIZE_COMPLETE")
    return 0


def stage_emit(args) -> int:
    """Emit the CURRENT forecast-input cross-section for the operational lane."""
    from paper_trader.alpha_agent import release30_forecast_emitter as em
    from paper_trader.api import universe_scoring as us

    scoring = us.load_universe_scoring()
    eligible = args.as_of or scoring.get("eligible_market_date")
    rows = [r for r in (scoring.get("rankings") or []) if r.get("ticker")]
    tickers = [r["ticker"] for r in rows]
    sectors = {r["ticker"]: (r.get("sector") or "Unknown") for r in rows}
    panel = rp.load_price_panel()
    payload = em.emit_cross_section(as_of_date=eligible, tickers=tickers,
                                    sectors=sectors, panel=panel)
    payload["operational_universe"] = {
        "owner": "api.universe_scoring",
        "universe_id": scoring.get("universe_id"),
        "eligible_market_date": scoring.get("eligible_market_date"),
        "universe_scoring_hash": scoring.get("output_hash"),
        "requested_tickers": len(tickers),
    }
    for tag in ("price_only", "fundamental_matched"):
        _write("forecast_input_%s.json" % tag, payload)

    # The CURRENT OPERATIONAL model's cross-section, carrying its own live score as
    # the single feature. Emitting it here - through the same input contract, over
    # the same rows, at the same session - is what makes the A/B comparison a
    # comparison of FORECASTS rather than of two different pipelines.
    combined = {r["ticker"]: r.get("combined_score") for r in rows}
    op = json.loads(json.dumps(payload))
    op["feature_names"] = [OPERATIONAL_FEATURE]
    op_rows = []
    for r in op["rows"]:
        v = combined.get(r["ticker"])
        if v is None:
            continue
        op_rows.append({"ticker": r["ticker"], "adv_dollar": r["adv_dollar"],
                        "sector": r["sector"],
                        "features": {OPERATIONAL_FEATURE: float(v)}})
    op["rows"] = op_rows
    op["row_count"] = len(op_rows)
    op["provenance"] = dict(op.get("provenance") or {},
                            operational_score_owner="api.universe_scoring",
                            operational_model_id=scoring.get("primary_model_id"))
    _write("forecast_input_operational.json", op)
    print("R30_EMIT_OK as_of=%s rows=%d requested=%d behind_session=%s"
          % (payload.get("as_of_date"), payload.get("row_count"), len(tickers),
             payload.get("feature_panel_behind_eligible_session")))
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Release 30 research runner")
    ap.add_argument("--stage", default="tournament",
                    choices=("panel", "tournament", "finalize", "emit",
                             "acceptance"))
    ap.add_argument("--force", action="store_true",
                    help="rebuild cached datasets")
    ap.add_argument("--as-of", dest="as_of", default=None)
    ap.add_argument("--seed", type=int, default=30)
    args = ap.parse_args(argv)
    return {"panel": stage_panel, "tournament": stage_tournament,
            "finalize": stage_finalize, "emit": stage_emit,
            "acceptance": stage_acceptance}[args.stage](args)


if __name__ == "__main__":
    raise SystemExit(main())
