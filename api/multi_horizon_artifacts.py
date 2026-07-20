"""api/multi_horizon_artifacts.py - Phase 25 operational artifact builder (Track A deliverables).

Persists the multi-horizon platform's operational snapshot artifacts to the dedicated LOCAL store
``D:\\Stock_Prediction_app_data\\phase25_multi_horizon_alpha`` and measures ACTUAL cold/warm request
runtimes (runtime_benchmark.json) so the performance targets are evidenced, not claimed.

This is a manual, explicit builder (invoked by the operator / CI - never scheduled, never automatic).
It writes ONLY the local artifact store: never PostgreSQL, never an order / fill / trade decision /
live signal, never the trading workflow.  All content comes from the read-only platform aggregators.
"""
from __future__ import annotations

import csv
import json
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from paper_trader.api import multi_horizon_engine as eng
from paper_trader.api import multi_horizon_history as hist
from paper_trader.api import multi_horizon_platform as plat
from paper_trader.api import multi_horizon_registry as mreg

STORE_ENV = "PAPER_TRADER_MHZ_ARTIFACT_DIR"
DEFAULT_STORE = Path(r"D:\Stock_Prediction_app_data\phase25_multi_horizon_alpha")


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _atomic_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(obj, fh, indent=2, default=str)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def _atomic_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for r in rows:
        for k in r:
            if k not in fields:
                fields.append(k)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=fields)
            w.writeheader()
            w.writerows(rows)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def _bench(fn, n_warm=3):
    t0 = time.perf_counter()
    fn()
    cold = time.perf_counter() - t0
    warms = []
    for _ in range(n_warm):
        t1 = time.perf_counter()
        fn()
        warms.append(time.perf_counter() - t1)
    return round(cold, 4), round(sum(warms) / len(warms), 4)


def build_artifacts(store_dir=None) -> dict:
    sdir = Path(store_dir) if store_dir else (
        Path(os.environ[STORE_ENV]) if os.environ.get(STORE_ENV) else DEFAULT_STORE)
    sdir.mkdir(parents=True, exist_ok=True)
    written: list[str] = []

    # ---- runtime benchmark FIRST (measures true cold then warm) --------------------------
    eng.clear_cache()
    plat.clear_caches()
    bench = {}
    for name, fn in [
        ("current_scores", lambda: plat.load_current_scores()),
        ("current_books", lambda: plat.load_current_books()),
        ("current_recommendations", lambda: plat.load_current_recommendations()),
        ("operating_state", lambda: plat.load_operating_state()),
        ("snapshot_preview", lambda: plat.preview_snapshot()),
        ("paper_history", lambda: plat.load_paper_history()),
    ]:
        cold, warm = _bench(fn)
        bench[name] = {"cold_seconds": cold, "warm_seconds": warm}
    targets = {"current_scores": 3.0, "current_books": 3.0, "current_recommendations": 3.0,
               "operating_state": 2.0, "snapshot_preview": 5.0, "paper_history": 5.0}
    bench_report = {n: {**v, "warm_target_seconds": targets[n],
                        "warm_target_met": v["warm_seconds"] <= targets[n]}
                    for n, v in bench.items()}
    _atomic_json(sdir / "runtime_benchmark.json", {"measured_at": _iso_now(), "requests": bench_report})
    written.append("runtime_benchmark.json")

    # ---- registries ----------------------------------------------------------------------
    models = plat.load_models()
    _atomic_json(sdir / "model_registry.json", models)
    written.append("model_registry.json")
    sleeves = plat.load_sleeves()
    _atomic_json(sdir / "sleeve_registry.json", sleeves)
    written.append("sleeve_registry.json")

    # ---- current inputs / scores / books / recommendations / state -----------------------
    scores = plat.load_current_scores(top=250)
    _atomic_json(sdir / "current_input_manifest.json", {
        "market_as_of_date": scores.get("market_as_of_date"),
        "fundamental_as_of_date": scores.get("fundamental_as_of_date"),
        "validations": scores.get("validations"), "fingerprints": scores.get("fingerprints"),
        "written_at": _iso_now()})
    written.append("current_input_manifest.json")
    _atomic_json(sdir / "current_model_scores.json", scores)
    written.append("current_model_scores.json")
    score_rows = []
    for c in (scores.get("combined_universe") or []):
        score_rows.append({"ticker": c["ticker"], "model_id": c["model_id"],
                           "combined_score": c["combined_score"], "rank": c["rank"],
                           "percentile": c["percentile"], "fund_rank": c["fund_rank"],
                           "mom_rank": c["mom_rank"], "sector": c["sector"],
                           "market_as_of_date": c["market_as_of_date"]})
    _atomic_csv(sdir / "current_model_scores.csv", score_rows or [{"note": "inputs unavailable"}])
    written.append("current_model_scores.csv")

    books = plat.load_current_books()
    _atomic_json(sdir / "current_books.json", books)
    written.append("current_books.json")

    recs = plat.load_current_recommendations()
    _atomic_json(sdir / "current_recommendations.json", recs)
    written.append("current_recommendations.json")
    rec_rows = []
    for sid, blk in (recs.get("sleeves") or {}).items():
        for r in blk.get("recommendations", []):
            rec_rows.append({"sleeve": sid, "ticker": r["ticker"],
                             "recommendation": r["recommendation"],
                             "target_weight": r["target_weight"],
                             "current_weight": r["current_theoretical_weight"],
                             "combined_score": r["combined_score"],
                             "sector": r["sector"],
                             "reason_codes": ";".join(r["reason_codes"]),
                             "risk_flags": ";".join(r["risk_flags"])})
    _atomic_csv(sdir / "current_recommendations.csv", rec_rows or [{"note": "inputs unavailable"}])
    written.append("current_recommendations.csv")

    state = plat.load_operating_state()
    _atomic_json(sdir / "current_operating_state.json", state)
    written.append("current_operating_state.json")

    # ---- historical reconstruction -------------------------------------------------------
    h = hist.build_history()
    ret_rows, met_rows, attr_rows = [], [], []
    if h.get("status") == "MHZ_HISTORY_READY":
        for bid, bk in h["books"].items():
            m = bk["metrics"]
            met_rows.append({"book_id": bid, "cadence": bk["cadence"], **{k: m.get(k) for k in (
                "n_periods", "gross_cumulative_return", "net_cumulative_return",
                "annualized_net_return", "annualized_vol", "sharpe", "max_drawdown", "hit_rate",
                "mean_turnover", "mean_steady_state_turnover", "sufficient_history",
                "first_month", "last_month")}})
            for pperiod in bk["periods"]:
                ret_rows.append({"book_id": bid, "month": pperiod["month"],
                                 "gross": round(pperiod["gross"], 6), "net": round(pperiod["net"], 6),
                                 "turnover": round(pperiod["turnover"], 4), "n": pperiod["n"]})
        L = h["combined_lift"]
        attr_rows = [{"component": "combined", "net_cumulative": L["combined_net_cumulative"],
                      "sharpe": L["combined_sharpe"], "max_drawdown": L["combined_max_drawdown"]},
                     {"component": "fundamental", "net_cumulative": L["fundamental_net_cumulative"],
                      "sharpe": L["fundamental_sharpe"], "max_drawdown": None},
                     {"component": "momentum", "net_cumulative": L["momentum_net_cumulative"],
                      "sharpe": L["momentum_sharpe"], "max_drawdown": None}]
    _atomic_csv(sdir / "historical_book_returns.csv", ret_rows or [{"note": "history unavailable"}])
    written.append("historical_book_returns.csv")
    _atomic_csv(sdir / "historical_book_metrics.csv", met_rows or [{"note": "history unavailable"}])
    written.append("historical_book_metrics.csv")
    _atomic_csv(sdir / "historical_attribution.csv", attr_rows or [{"note": "history unavailable"}])
    written.append("historical_attribution.csv")
    _atomic_json(sdir / "historical_reconciliation.json", {
        "reconciliation": h.get("reconciliation"), "combined_lift": h.get("combined_lift"),
        "methodology": h.get("methodology"), "months": h.get("months")})
    written.append("historical_reconciliation.json")

    # ---- risk + turnover + comparison ----------------------------------------------------
    cur = eng.build_current()
    risk_rows = []
    if cur.get("status") == eng.STATUS_READY:
        risk = cur["inputs"].get("risk", {})
        primary = cur["books"]["books"]["fundamental_momentum_50_50_top25"]["constituents"]
        for c in primary:
            r = risk.get(c["ticker"], {})
            risk_rows.append({"ticker": c["ticker"], "weight": c["weight"], "sector": c["sector"],
                              "realized_vol_63d": r.get("realized_vol_63d"),
                              "beta_universe": r.get("beta_universe"),
                              "adv_dollar_20d": r.get("adv_dollar_20d"),
                              "max_drawdown_252d": r.get("max_drawdown_252d")})
    _atomic_json(sdir / "risk_report.json", {"primary_book": risk_rows, "written_at": _iso_now()})
    written.append("risk_report.json")
    turn_rows = []
    for sid, blk in (recs.get("sleeves") or {}).items():
        turn_rows.append({"sleeve": sid, "estimated_turnover": blk.get("estimated_turnover"),
                          "estimated_transaction_cost": blk.get("estimated_transaction_cost"),
                          "review_due": blk.get("review_due")})
    _atomic_csv(sdir / "turnover_report.csv", turn_rows or [{"note": "unavailable"}])
    _atomic_json(sdir / "turnover_report.json", {"sleeves": turn_rows, "written_at": _iso_now()})
    written += ["turnover_report.csv", "turnover_report.json"]

    comparison = plat.load_book_comparison()
    _atomic_json(sdir / "book_comparison.json", comparison)
    written.append("book_comparison.json")

    # ---- reproducibility -----------------------------------------------------------------
    _atomic_json(sdir / "reproducibility_manifest.json", {
        "phase": "25", "written_at": _iso_now(),
        "code": ["api/multi_horizon_registry.py", "api/multi_horizon_engine.py",
                 "api/multi_horizon_history.py", "api/multi_horizon_ledger.py",
                 "api/multi_horizon_platform.py", "api/multi_horizon_artifacts.py"],
        "inputs": scores.get("fingerprints"),
        "input_sources": {
            "fundamental": "frozen Phase 10-L sector-neutral scored panel (owned)",
            "momentum": "research/phase25_multi_horizon_inputs.py over the owned Phase 24 NPZ",
            "sector_map": "Phase 10-F repaired owned GICS mapping"},
        "reproduce": "python -c \"from paper_trader.api.multi_horizon_artifacts import build_artifacts; build_artifacts()\"",
        "safety": mreg.safety_block()})
    written.append("reproducibility_manifest.json")

    return {"store_dir": str(sdir), "files_written": written, "benchmark": bench_report,
            "built_at": _iso_now()}


if __name__ == "__main__":
    out = build_artifacts()
    print(json.dumps(out, indent=2, default=str))
