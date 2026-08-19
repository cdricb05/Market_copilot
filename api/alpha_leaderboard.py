"""api/alpha_leaderboard.py - the model leaderboard read model.

Release 30. Research could show what the agent was doing but not what the alpha
competition actually looked like. This module answers, on one screen:

* which model is OPERATIONAL, which is the new ADAPTIVE CANDIDATE, and which
  component alphas feed it;
* what each one's strict out-of-sample evidence says - rank IC, net-of-cost book
  return, information ratio, drawdown, turnover, calibration;
* how much TRUE_FORWARD evidence each has accumulated;
* its lifecycle status.

It is a READ MODEL and owns nothing. Champion identity comes from
``api.universe_scoring``; walk-forward evidence comes from the Release 30
research artifacts; forward evidence comes from the existing evidence owners;
challenger lifecycle comes from the research agent and the tournament sync.

**It never claims superiority the evidence does not support.** Every row carries
its own verdict from the research lane's own gate, and a component that has not
beaten the benchmark is labelled as such - including
``s25_operating_profitability``, which this release deliberately surfaces rather
than leaves buried.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

SCHEMA_VERSION = "alpha_leaderboard.v1"
COMPOSITION_OWNER = "api.alpha_leaderboard"
PHASE = "R30"

STATE_READY = "READY"
STATE_NO_RESEARCH = "NO_WALK_FORWARD_EVIDENCE"
STATE_UNAVAILABLE = "UNAVAILABLE"
STATE_VOCAB = (STATE_READY, STATE_NO_RESEARCH, STATE_UNAVAILABLE)

#: Lifecycle vocabulary. A model is in exactly one of these.
LC_OPERATIONAL = "OPERATIONAL_CHAMPION"
LC_CANDIDATE = "ADAPTIVE_CANDIDATE"
LC_COMPONENT = "COMPONENT_ALPHA"
LC_BENCHMARK = "FROZEN_BENCHMARK"
LIFECYCLE_VOCAB = (LC_OPERATIONAL, LC_CANDIDATE, LC_COMPONENT, LC_BENCHMARK)

_ROLE_TO_LIFECYCLE = {
    "BENCHMARK": LC_BENCHMARK,
    "CANDIDATE": LC_COMPONENT,
    "COMPONENT_ALPHA": LC_COMPONENT,
    "ADAPTIVE_CANDIDATE": LC_CANDIDATE,
}

R30_ROOT_ENV = "PAPER_TRADER_R30_ROOT"
_DEFAULT_R30_ROOT = Path(
    r"D:\Stock_Prediction_app_data\release30_zero_base_adaptive_allocator")

SAFETY_BADGES = ["READ ONLY", "PREVIEW ONLY", "NO LIVE PROMOTION",
                 "MANUAL REVIEW", "AUTOMATION OFF"]

AUTOMATIC_PROMOTION_ALLOWED = False


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _root(root=None) -> Path:
    return Path(root or os.environ.get(R30_ROOT_ENV) or _DEFAULT_R30_ROOT)


def _load(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _r(x: Any, nd: int = 6) -> Optional[float]:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return round(v, nd) if v == v and abs(v) != float("inf") else None


def build(*, tournaments: Optional[dict] = None,
          verdicts: Optional[dict] = None,
          artifacts: Optional[dict] = None,
          scoring: Optional[dict] = None,
          forward_evidence: Optional[dict] = None,
          horizon: Optional[int] = None, root=None) -> dict:
    """Assemble the leaderboard. PURE READ."""
    tags = ("price_only", "fundamental_matched")
    tourn = tournaments if tournaments is not None else {
        t: _load(_root(root) / ("tournament_%s.json" % t)) for t in tags}
    verd = verdicts if verdicts is not None else {
        t: _load(_root(root) / ("verdict_%s.json" % t)) for t in tags}
    arts = artifacts if artifacts is not None else {
        t: _load(_root(root) / ("model_artifact_%s.json" % t)) for t in tags}
    integrity = _load(_root(root) / "point_in_time_integrity.json") or {}

    sc = scoring or {}
    champion = {
        "model_id": sc.get("primary_model_id"),
        "model_version": sc.get("strategy_version"),
        "book_id": sc.get("primary_book_id"),
        "input_contract_hash": sc.get("input_contract_hash"),
        "construction": (sc.get("construction") or {}).get("primary_weights"),
        "eligible_market_date": sc.get("eligible_market_date"),
        "universe_size": sc.get("combined_eligible_count"),
    }

    universes = []
    for tag in tags:
        t = tourn.get(tag)
        if not t:
            continue
        h = str(int(horizon)) if horizon else (
            str(t["horizons"][0]) if t.get("horizons") else None)
        blk = (t.get("by_horizon") or {}).get(h) or {}
        v = verd.get(tag) or {}
        vh = (v.get("by_horizon") or {}).get(h) or {}
        art = arts.get(tag) or {}
        rows = []
        for mid, m in sorted((blk.get("models") or {}).items()):
            test = m.get("test") or {}
            book = m.get("book") or {}
            rows.append({
                "model_id": mid,
                "lifecycle": _ROLE_TO_LIFECYCLE.get(m.get("role"), LC_COMPONENT),
                "role": m.get("role"),
                "note": m.get("note"),
                "features": list(t.get("feature_names") or []),
                "walk_forward": {
                    "folds": blk.get("folds"),
                    "decision_dates": t.get("n_decision_dates"),
                    "first_date": (t.get("dates") or [None])[0],
                    "last_date": (t.get("dates") or [None])[-1],
                    "test_dates": test.get("n_dates"),
                    "test_rows": test.get("n_rows"),
                },
                "oos_rank_ic": _r(test.get("rank_ic_mean")),
                "oos_rank_ic_t": _r(test.get("rank_ic_t"), 3),
                "oos_rank_ic_positive_fraction": _r(
                    test.get("rank_ic_positive_fraction"), 3),
                "directional_accuracy": _r(test.get("directional_accuracy"), 4),
                "calibration_slope": _r(test.get("calibration_slope")),
                "oos_net_return_annualised": _r(book.get("annualised_net_return")),
                "information_ratio": _r(book.get("information_ratio"), 3),
                "max_drawdown": _r(book.get("max_drawdown"), 4),
                "one_way_turnover": _r(book.get("mean_one_way_turnover"), 4),
                "cost_per_period": _r(book.get("cost_mean_period")),
                "ensemble_weight": _r(
                    ((blk.get("ensemble") or {}).get("weights") or {}).get(mid), 4),
                "selected_hyperparameters": m.get("selected_hyperparameters"),
            })
        rows.sort(key=lambda r: (-(r["oos_rank_ic"] or -9), r["model_id"]))
        universes.append({
            "universe_tag": tag,
            "horizon_sessions": int(h) if h else None,
            "survivorship_safe": bool(
                (integrity.get("price_family") or {}).get("survivorship_safe")
                if tag == "price_only"
                else (integrity.get("fundamental_family") or {}).get("survivorship_safe")),
            "coverage_caveat": (
                None if tag == "price_only"
                else (integrity.get("fundamental_family") or {}).get("survivorship_verdict")),
            "coverage_caveat_detail": (
                None if tag == "price_only"
                else (integrity.get("fundamental_family") or {}).get("why")),
            "benchmark_model_id": v.get("benchmark_model_id"),
            "candidate_model_id": v.get("candidate_model_id"),
            "forecast_model_verdict": v.get("forecast_model_verdict"),
            "criteria": v.get("criteria") or [],
            "criteria_result": vh.get("checks") or {},
            "criteria_failed": vh.get("failed") or [],
            "net_return_difference_annualised": _r(
                vh.get("net_return_difference_annualised")),
            "paired_net_return_t": _r(vh.get("paired_net_return_t"), 3),
            "model_spec_hash": art.get("model_spec_hash"),
            "ensemble_weights": (blk.get("ensemble") or {}).get("weights") or {},
            "ensemble_method": (blk.get("ensemble") or {}).get("method"),
            "ensemble_components": (blk.get("ensemble") or {}).get("components") or {},
            "models": rows,
        })

    fe = forward_evidence or {}
    return {
        "schema_version": SCHEMA_VERSION,
        "composition_owner": COMPOSITION_OWNER,
        "phase": PHASE,
        "generated_at": _now_iso(),
        "state": STATE_READY if universes else STATE_NO_RESEARCH,
        "state_vocabulary": list(STATE_VOCAB),
        "lifecycle_vocabulary": list(LIFECYCLE_VOCAB),
        "operational_champion": champion,
        "universes": universes,
        "forward_evidence": {
            "observations": fe.get("observations") or fe.get("observation_count"),
            "books": fe.get("books"),
            "owner": "api.forward_prediction_skill / api.forward_evidence",
            "note": ("Historical walk-forward evidence and TRUE_FORWARD evidence "
                     "are different things and are never merged. Release 30 "
                     "qualifies a candidate on walk-forward evidence; forward "
                     "evidence accumulates after a manual activation and is what "
                     "later degradation checks read."),
        },
        "point_in_time_integrity": {
            "price_family_survivorship_safe": (
                integrity.get("price_family") or {}).get("survivorship_safe"),
            "fundamental_family_verdict": (
                integrity.get("fundamental_family") or {}).get("survivorship_verdict"),
            "resolution_survivorship_skew": (
                integrity.get("fundamental_family") or {}).get(
                    "resolution_survivorship_skew"),
            "controls": integrity.get("controls") or [],
        },
        "automatic_promotion_allowed": AUTOMATIC_PROMOTION_ALLOWED,
        "promotion_doc": ("No model is promoted by any code path. A champion "
                          "change requires a human, and the read surface can only "
                          "ever report evidence."),
        "owns_no_calculation": True,
        "safety": {"badges": list(SAFETY_BADGES), "creates_orders": False,
                   "creates_decisions": False, "promotes_models": False},
    }


def load_alpha_leaderboard(*, horizon: Optional[int] = None, root=None) -> dict:
    try:
        from paper_trader.api import universe_scoring as us
        sc = us.load_universe_scoring()
    except Exception:                                          # noqa: BLE001
        sc = {}
    fe = {}
    try:
        from paper_trader.api import forward_prediction_skill as fps
        fe = {"books": list(getattr(fps, "SUPPORTED_BOOKS", []) or [])}
    except Exception:                                          # noqa: BLE001
        fe = {}
    try:
        return build(scoring=sc, forward_evidence=fe, horizon=horizon, root=root)
    except Exception as exc:                                   # noqa: BLE001
        return {
            "schema_version": SCHEMA_VERSION,
            "composition_owner": COMPOSITION_OWNER, "phase": PHASE,
            "generated_at": _now_iso(), "state": STATE_UNAVAILABLE,
            "state_vocabulary": list(STATE_VOCAB), "universes": [],
            "blockers": [{"code": "ALPHA_LEADERBOARD_UNAVAILABLE",
                          "detail": type(exc).__name__}],
            "owns_no_calculation": True,
            "automatic_promotion_allowed": AUTOMATIC_PROMOTION_ALLOWED,
            "safety": {"badges": list(SAFETY_BADGES), "creates_orders": False,
                       "creates_decisions": False, "promotes_models": False},
        }
