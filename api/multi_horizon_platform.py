"""api/multi_horizon_platform.py - Phase 25 API-facing aggregators for the multi-horizon platform (A10).

Thin, read-only composition layer over the registry / engine / ledger / history modules.  Each ``load_*``
returns a JSON-serialisable dict with the Phase 25 safety block attached (paper_only / orders_enabled=false
/ automation_enabled=false / champion_replaced=false / validated_fast_alpha_available).  Preview/confirm
delegate to the append-only ledger.

Strictly read-only except the ledger's explicit confirmed append.  No PostgreSQL, no orders/fills/trade
decisions/live signals, no broker, no automation, no prediction service, no champion replacement.  A
missing owned input degrades to an *_UNAVAILABLE status with HTTP 200, never a stack trace.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from paper_trader.api import multi_horizon_engine as eng
from paper_trader.api import multi_horizon_ledger as ledger
from paper_trader.api import multi_horizon_registry as mreg
from paper_trader.api import multi_horizon_history as hist

# Fast-spec artifact written by Track B; if it declares a qualified fast alpha, the fast sleeve activates.
FAST_SPEC_ENV = "PAPER_TRADER_MHZ_FAST_SPEC"
DEFAULT_FAST_SPEC = Path(r"D:\Stock_Prediction_app_data\phase25_fast_ohlc\frozen_fast_specs.json")


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _validated_fast_available(fast_spec_path=None) -> tuple[bool, dict]:
    p = Path(fast_spec_path) if fast_spec_path else (
        Path(os.environ[FAST_SPEC_ENV]) if os.environ.get(FAST_SPEC_ENV) else DEFAULT_FAST_SPEC)
    try:
        with open(p, "r", encoding="utf-8") as fh:
            obj = json.load(fh)
    except (OSError, ValueError):
        return False, {"fast_spec_present": False, "fast_status": mreg.NO_VALIDATED_FAST_ALPHA}
    qualified = bool(obj.get("qualified")) and bool(obj.get("frozen_specs"))
    return qualified, {"fast_spec_present": True, "qualified": qualified,
                       "fast_status": "VALIDATED" if qualified else obj.get("terminal", mreg.NO_VALIDATED_FAST_ALPHA),
                       "terminal": obj.get("terminal")}


def _safety(fast_spec_path=None) -> dict:
    ok, _info = _validated_fast_available(fast_spec_path)
    return mreg.safety_block(validated_fast_alpha_available=ok)


# --------------------------------------------------------------------------- #
# in-process history cache (warm history < 5s)
# --------------------------------------------------------------------------- #
_HIST_CACHE: dict = {}


def _history(panel_path=None, inputs_dir=None):
    cur = eng.build_current(panel_path=panel_path, inputs_dir=inputs_dir)
    key = None
    if cur.get("status") == eng.STATUS_READY:
        fps = cur.get("inputs", {}).get("fingerprints", {})
        key = (fps.get("fundamental_panel"), fps.get("current_momentum_scores"))
    if key is not None and key in _HIST_CACHE:
        return _HIST_CACHE[key]
    h = hist.build_history(panel_path=panel_path, inputs_dir=inputs_dir)
    if key is not None:
        _HIST_CACHE[key] = h
    return h


def clear_caches():
    _HIST_CACHE.clear()
    eng.clear_cache()


# --------------------------------------------------------------------------- #
# GET /v1/research/alpha-models
# --------------------------------------------------------------------------- #
def load_models(fast_spec_path=None) -> dict:
    models = mreg.model_registry()
    ok, fast_info = _validated_fast_available(fast_spec_path)
    return {"phase": mreg.PHASE, "status": "MHZ_MODELS_READY",
            "models": models, "counts": mreg.registry_counts(models),
            "recommendation_eligible_model_ids": mreg.recommendation_eligible_model_ids(),
            "fast_info": fast_info, "loaded_at": _iso_now(), **_safety(fast_spec_path)}


# --------------------------------------------------------------------------- #
# GET /v1/research/alpha-sleeves
# --------------------------------------------------------------------------- #
def load_sleeves(*, panel_path=None, inputs_dir=None, ledger_dir=None, fast_spec_path=None) -> dict:
    cur = eng.build_current(panel_path=panel_path, inputs_dir=inputs_dir)
    ok, fast_info = _validated_fast_available(fast_spec_path)
    prior = ledger.latest_confirmed_by_sleeve(ledger_dir)
    if cur.get("status") != eng.STATUS_READY:
        state = {"operating_state": eng.STATE_DATA_BLOCKED, "sleeves": []}
    else:
        state = eng.compute_operating_state(cur, prior, validated_fast_alpha_available=ok)
    # merge static sleeve contract with dynamic state
    dyn = {s["sleeve_id"]: s for s in state.get("sleeves", [])}
    sleeves = []
    for s in mreg.sleeve_registry():
        d = dyn.get(s["sleeve_id"], {})
        sleeves.append({**s, "state": d,
                        "last_review": d.get("last_confirmed_snapshot"),
                        "next_review": d.get("next_manual_review_date"),
                        "review_due": d.get("review_due", False),
                        "current_actionability": d.get("current_actionability")})
    return {"phase": mreg.PHASE, "status": "MHZ_SLEEVES_READY" if cur.get("status") == eng.STATUS_READY
            else eng.STATUS_INPUTS_UNAVAILABLE,
            "market_as_of_date": cur.get("market_as_of_date"),
            "sleeves": sleeves, "fast_info": fast_info,
            "warnings": cur.get("warnings", []), "loaded_at": _iso_now(), **_safety(fast_spec_path)}


# --------------------------------------------------------------------------- #
# GET /v1/research/alpha-operating-state
# --------------------------------------------------------------------------- #
def load_operating_state(*, panel_path=None, inputs_dir=None, ledger_dir=None, fast_spec_path=None) -> dict:
    cur = eng.build_current(panel_path=panel_path, inputs_dir=inputs_dir)
    ok, fast_info = _validated_fast_available(fast_spec_path)
    prior = ledger.latest_confirmed_by_sleeve(ledger_dir)
    state = eng.compute_operating_state(cur, prior, validated_fast_alpha_available=ok)
    risk = cur.get("inputs", {}).get("validations", {}) if cur.get("status") == eng.STATUS_READY else {}
    return {"phase": mreg.PHASE, "status": "MHZ_OPERATING_STATE_READY" if cur.get("status") == eng.STATUS_READY
            else eng.STATUS_INPUTS_UNAVAILABLE,
            **state, "fast_info": fast_info,
            "daily_monitoring": {"risk_names": risk.get("risk_names"),
                                 "momentum_eligible": risk.get("momentum_eligible"),
                                 "sector_coverage": risk.get("fundamental_sector_coverage")},
            "warnings": cur.get("warnings", []), "loaded_at": _iso_now(), **_safety(fast_spec_path)}


# --------------------------------------------------------------------------- #
# GET /v1/research/current-alpha-scores
# --------------------------------------------------------------------------- #
def load_current_scores(*, panel_path=None, inputs_dir=None, fast_spec_path=None, top=60) -> dict:
    cur = eng.build_current(panel_path=panel_path, inputs_dir=inputs_dir)
    if cur.get("status") != eng.STATUS_READY:
        return {"phase": mreg.PHASE, "status": eng.STATUS_INPUTS_UNAVAILABLE,
                "warnings": cur.get("warnings", []), "loaded_at": _iso_now(), **_safety(fast_spec_path)}
    scores = cur["scores"]
    combined = cur["combined"]["combined"]

    def _top(smap, key, n):
        rows = [sc for sc in smap.values() if sc.get("eligible")]
        rows.sort(key=lambda r: (r.get("rank") if r.get("rank") is not None else 1e9))
        return rows[:n]

    comb_rows = sorted(combined.values(), key=lambda c: (c.get("rank") or 1e9))
    return {"phase": mreg.PHASE, "status": "MHZ_SCORES_READY",
            "market_as_of_date": cur["market_as_of_date"],
            "fundamental_as_of_date": cur["fundamental_as_of_date"],
            "fundamental_month": cur["fundamental_month"], "momentum_month": cur["momentum_month"],
            "counts": scores["counts"], "n_common_universe": cur["combined"]["n_common"],
            "combined_universe": comb_rows,
            "composite_sn_top": _top(scores["composite_sn"], "raw_signal", top),
            "mom_6_1_top": _top(scores["mom_6_1"], "raw_signal", top),
            "blocked_models": eng.blocked_model_notice(),
            "validations": cur["inputs"]["validations"], "fingerprints": cur["inputs"]["fingerprints"],
            "warnings": cur.get("warnings", []), "loaded_at": _iso_now(), **_safety(fast_spec_path)}


# --------------------------------------------------------------------------- #
# GET /v1/research/current-alpha-books
# --------------------------------------------------------------------------- #
def load_current_books(*, panel_path=None, inputs_dir=None, fast_spec_path=None) -> dict:
    cur = eng.build_current(panel_path=panel_path, inputs_dir=inputs_dir)
    if cur.get("status") != eng.STATUS_READY:
        return {"phase": mreg.PHASE, "status": eng.STATUS_INPUTS_UNAVAILABLE,
                "warnings": cur.get("warnings", []), "loaded_at": _iso_now(), **_safety(fast_spec_path)}
    return {"phase": mreg.PHASE, "status": "MHZ_BOOKS_READY",
            "market_as_of_date": cur["market_as_of_date"],
            "primary_book_id": cur["books"]["primary_book_id"],
            "books": cur["books"]["books"], "overlaps": cur["books"]["overlaps"],
            "construction": {"sector_cap_fraction": eng.SECTOR_CAP_FRACTION,
                             "max_individual_weight": eng.MAX_INDIVIDUAL_WEIGHT,
                             "min_adv_dollar": eng.MIN_ADV_DOLLAR,
                             "weights": eng.PRIMARY_WEIGHTS, "long_only": True, "manual_rebalance_only": True},
            "warnings": cur.get("warnings", []), "loaded_at": _iso_now(), **_safety(fast_spec_path)}


# --------------------------------------------------------------------------- #
# GET /v1/research/current-alpha-recommendations
# --------------------------------------------------------------------------- #
def load_current_recommendations(*, panel_path=None, inputs_dir=None, ledger_dir=None,
                                 fast_spec_path=None, size=25) -> dict:
    cur = eng.build_current(panel_path=panel_path, inputs_dir=inputs_dir)
    ok, _fast = _validated_fast_available(fast_spec_path)
    if cur.get("status") != eng.STATUS_READY:
        return {"phase": mreg.PHASE, "status": eng.STATUS_INPUTS_UNAVAILABLE,
                "warnings": cur.get("warnings", []), "loaded_at": _iso_now(), **_safety(fast_spec_path)}
    prior = ledger.latest_confirmed_by_sleeve(ledger_dir)
    state = eng.compute_operating_state(cur, prior, validated_fast_alpha_available=ok)
    due_by = {s["sleeve_id"]: s["review_due"] for s in state["sleeves"]}
    sleeves = {}
    for sid in (mreg.SLEEVE_COMBINED, mreg.SLEEVE_FUNDAMENTAL, mreg.SLEEVE_MOMENTUM):
        sleeves[sid] = eng.compute_recommendations(cur, prior, sid, size=size,
                                                   review_due=due_by.get(sid))
    return {"phase": mreg.PHASE, "status": "MHZ_RECOMMENDATIONS_READY",
            "market_as_of_date": cur["market_as_of_date"],
            "fundamental_as_of_date": cur["fundamental_as_of_date"],
            "primary_sleeve": mreg.SLEEVE_COMBINED, "size": size,
            "operating_state": state["operating_state"],
            "sleeves": sleeves, "blocked_models": eng.blocked_model_notice(),
            "warnings": cur.get("warnings", []), "loaded_at": _iso_now(), **_safety(fast_spec_path)}


# --------------------------------------------------------------------------- #
# GET /v1/research/alpha-book-comparison
# --------------------------------------------------------------------------- #
def load_book_comparison(*, panel_path=None, inputs_dir=None, fast_spec_path=None) -> dict:
    cur = eng.build_current(panel_path=panel_path, inputs_dir=inputs_dir)
    if cur.get("status") != eng.STATUS_READY:
        return {"phase": mreg.PHASE, "status": eng.STATUS_INPUTS_UNAVAILABLE,
                "warnings": cur.get("warnings", []), "loaded_at": _iso_now(), **_safety(fast_spec_path)}
    h = _history(panel_path=panel_path, inputs_dir=inputs_dir)
    rows = []
    hbooks = h.get("books", {}) if h.get("status") == "MHZ_HISTORY_READY" else {}
    for bid, bk in cur["books"]["books"].items():
        hm = (hbooks.get(bid) or {}).get("metrics", {})
        cadence = ((hbooks.get(bid) or {}).get("cadence")
                   or ("quarterly" if bk["model_id"] == "composite_sn" else "monthly"))
        rows.append({"book_id": bid, "model_id": bk["model_id"], "size": bk["size_actual"],
                     "cadence": cadence,
                     "n_sectors": len(bk["sector_exposure"]), "equal_weight": bk["equal_weight"],
                     "hist_net_cumulative": hm.get("net_cumulative_return"),
                     "hist_annualized_net": hm.get("annualized_net_return"),
                     "hist_sharpe": hm.get("sharpe"), "hist_max_drawdown": hm.get("max_drawdown"),
                     "hist_hit_rate": hm.get("hit_rate"),
                     "hist_mean_steady_state_turnover": hm.get("mean_steady_state_turnover"),
                     "hist_n_periods": hm.get("n_periods"),
                     "hist_sufficient_history": hm.get("sufficient_history")})
    return {"phase": mreg.PHASE, "status": "MHZ_BOOK_COMPARISON_READY",
            "market_as_of_date": cur["market_as_of_date"],
            "primary_book_id": cur["books"]["primary_book_id"],
            "current_overlaps": cur["books"]["overlaps"],
            "historical_overlaps": h.get("overlaps"), "combined_lift": h.get("combined_lift"),
            "rows": rows, "warnings": cur.get("warnings", []), "loaded_at": _iso_now(),
            **_safety(fast_spec_path)}


# --------------------------------------------------------------------------- #
# GET /v1/research/alpha-paper-history
# --------------------------------------------------------------------------- #
def load_paper_history(*, panel_path=None, inputs_dir=None, fast_spec_path=None) -> dict:
    h = _history(panel_path=panel_path, inputs_dir=inputs_dir)
    if h.get("status") != "MHZ_HISTORY_READY":
        return {"phase": mreg.PHASE, "status": eng.STATUS_INPUTS_UNAVAILABLE,
                "warnings": h.get("warnings", []), "loaded_at": _iso_now(), **_safety(fast_spec_path)}
    # trim heavy per-period constituents; keep metrics + equity curves + reconciliation + lift
    books = {}
    for bid, bk in h["books"].items():
        m = dict(bk["metrics"])
        books[bid] = {"cadence": bk["cadence"], "metrics": m}
    return {"phase": mreg.PHASE, "status": "MHZ_HISTORY_READY", "months": h["months"],
            "books": books, "combined_lift": h["combined_lift"], "overlaps": h["overlaps"],
            "reconciliation": h["reconciliation"], "methodology": h["methodology"],
            "cost_assumption_bps": h["cost_assumption_bps"],
            "loaded_at": _iso_now(), **_safety(fast_spec_path)}


# --------------------------------------------------------------------------- #
# GET /v1/research/alpha-paper-snapshots (+ /{id}) and preview/confirm
# --------------------------------------------------------------------------- #
def load_paper_snapshots(*, ledger_dir=None, fast_spec_path=None) -> dict:
    out = ledger.list_snapshots(ledger_dir)
    return {"phase": mreg.PHASE, "status": "MHZ_SNAPSHOTS_READY", **out,
            "confirm_required_token": ledger.CONFIRM_TOKEN,
            "loaded_at": _iso_now(), **_safety(fast_spec_path)}


def get_paper_snapshot(snapshot_id: str, *, ledger_dir=None, fast_spec_path=None) -> dict:
    snap = ledger.get_snapshot(snapshot_id, ledger_dir)
    if snap is None:
        return {"phase": mreg.PHASE, "status": "MHZ_SNAPSHOT_NOT_FOUND", "snapshot_id": snapshot_id,
                "loaded_at": _iso_now(), **_safety(fast_spec_path)}
    return {"phase": mreg.PHASE, "status": "MHZ_SNAPSHOT_READY", "snapshot": snap,
            "loaded_at": _iso_now(), **_safety(fast_spec_path)}


def preview_snapshot(*, panel_path=None, inputs_dir=None, ledger_dir=None, fast_spec_path=None) -> dict:
    out = ledger.preview_snapshot(ledger_dir=ledger_dir, panel_path=panel_path, inputs_dir=inputs_dir)
    out.update({"phase": mreg.PHASE, "loaded_at": _iso_now()})
    ok, _ = _validated_fast_available(fast_spec_path)
    out["validated_fast_alpha_available"] = ok
    return out


def confirm_snapshot(*, confirm=None, panel_path=None, inputs_dir=None, ledger_dir=None,
                     fast_spec_path=None) -> dict:
    out = ledger.confirm_snapshot(confirm=confirm, ledger_dir=ledger_dir, panel_path=panel_path,
                                  inputs_dir=inputs_dir)
    out.update({"phase": mreg.PHASE, "loaded_at": _iso_now()})
    ok, _ = _validated_fast_available(fast_spec_path)
    out["validated_fast_alpha_available"] = ok
    return out


__all__ = [
    "load_models", "load_sleeves", "load_operating_state", "load_current_scores",
    "load_current_books", "load_current_recommendations", "load_book_comparison",
    "load_paper_history", "load_paper_snapshots", "get_paper_snapshot",
    "preview_snapshot", "confirm_snapshot", "clear_caches",
    "FAST_SPEC_ENV", "DEFAULT_FAST_SPEC",
]
