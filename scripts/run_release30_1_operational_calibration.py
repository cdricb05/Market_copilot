"""scripts/run_release30_1_operational_calibration.py - Release 30.1 research runner.

Reconstructs the CURRENT APPROVED operational model from owned data, calibrates
its score into a forward excess-return representation under strict walk-forward
discipline, and writes the frozen artifact plus its evidence to the Release-30.1
research root.

RESEARCH ONLY. It writes to a research directory, never to an operational store;
it creates no signal, target, proposal, decision or order, and it cannot promote
or activate anything.

    python scripts/run_release30_1_operational_calibration.py --stage calibrate
    python scripts/run_release30_1_operational_calibration.py --stage verify
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from paper_trader.alpha_agent import release30_1_operational_calibration as oc  # noqa: E402

ROOT_ENV = "PAPER_TRADER_R30_1_ROOT"
DEFAULT_ROOT = Path(
    r"D:\Stock_Prediction_app_data\release30_1_zero_base_operational_cutover")

ARTIFACT_FILE = "model_artifact_operational_v2.json"
EVIDENCE_FILE = "operational_calibration_evidence.json"
VERDICT_FILE = "operational_calibration_verdict.json"


def root() -> Path:
    return Path(os.environ.get(ROOT_ENV) or DEFAULT_ROOT)


def _write(name: str, payload) -> Path:
    d = root()
    d.mkdir(parents=True, exist_ok=True)
    p = d / name
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=1, default=str), encoding="utf-8")
    tmp.replace(p)
    return p


def stage_calibrate(args) -> int:
    t0 = time.time()
    print("R30_1_CALIBRATION_START model=%s" % oc.OPERATIONAL_MODEL_ID)
    hist = oc.reconstruct_history()
    print("  reconstructed %d decision dates %s .. %s"
          % (hist["n_sections"], hist["first_date"], hist["last_date"]))
    if not hist["n_sections"]:
        _write(VERDICT_FILE, {"verdict": "R30_1_CALIBRATION_BLOCKED",
                              "reason": "NO_OWNED_DECISION_DATES"})
        print("R30_1_CALIBRATION_BLOCKED - no owned decision dates")
        return 1

    art = oc.build_artifact(hist)
    _write(ARTIFACT_FILE, art)

    evidence = {
        "research_version": oc.RESEARCH_VERSION,
        "operational_model_id": oc.OPERATIONAL_MODEL_ID,
        "decision_dates": hist["n_sections"],
        "first_date": hist["first_date"], "last_date": hist["last_date"],
        "mean_cross_section": round(
            sum(s["n_names"] for s in hist["sections"]) / hist["n_sections"], 2),
        "sources": hist["sources"],
        "contract": {
            "rank_identity_min_slope": oc.RANK_IDENTITY_MIN_SLOPE,
            "reliability_min_t": oc.RELIABILITY_MIN_T,
            "fold_geometries": [list(g) for g in oc.FOLD_GEOMETRIES],
            "doc": ("a horizon supplies an expected return only if the "
                    "walk-forward slope is positive, keeps that sign under every "
                    "declared fold geometry, and is distinguishable from zero"),
        },
        "by_horizon": {k: v["calibration"] for k, v in art["horizons"].items()},
    }
    _write(EVIDENCE_FILE, evidence)

    print()
    print("  %-8s %-16s %-24s %-11s %-8s %s"
          % ("HORIZON", "STATE", "RANK IDENTITY", "SLOPE", "NW t", "SIGN STABLE"))
    for h in sorted(art["horizons"], key=int):
        c = art["horizons"][h]["calibration"]
        print("  %-8s %-16s %-24s %-11s %-8s %s"
              % (h + "d", c.get("state"), c.get("rank_identity") or "-",
                 ("%+.6f" % c["measured_slope"]) if c.get("measured_slope") is not None else "-",
                 ("%+.2f" % c["newey_west_t"]) if c.get("newey_west_t") is not None else "-",
                 c.get("slope_sign_stable_across_geometries")))
        for r in c.get("reasons") or []:
            print("           reason: %s" % r)

    ok = bool(art["calibrated_horizons"])
    verdict = {
        "verdict": ("R30_1_CURRENT_MODEL_CALIBRATION_READY" if ok
                    else "R30_1_CALIBRATION_BLOCKED"),
        "operational_model_id": oc.OPERATIONAL_MODEL_ID,
        "model_spec_hash": art["model_spec_hash"],
        "calibrated_horizons": art["calibrated_horizons"],
        "calibration_state": art["calibration_state"],
        "decision_dates": hist["n_sections"],
        "generated_seconds": round(time.time() - t0, 1),
        "consequence": (
            "the operational zero-base path may consume this calibration"
            if ok else
            "no horizon may supply an expected return; the operational zero-base "
            "target stays DATA_BLOCKED and no authoritative-target cutover is "
            "admissible, because the alternative is a fabricated expected return"),
        "safety": list(oc.SAFETY),
    }
    _write(VERDICT_FILE, verdict)
    print()
    print("%s calibrated_horizons=%s %.1fs"
          % (verdict["verdict"], art["calibrated_horizons"],
             verdict["generated_seconds"]))
    return 0


def stage_verify(args) -> int:
    """Prove the reconstruction reproduces the LIVE owner's cross-section."""
    from paper_trader.api import universe_scoring as us
    scoring = us.load_universe_scoring()
    live = {r["ticker"] for r in (scoring.get("rankings") or [])
            if r.get("ticker") and r.get("eligible", True)}
    fund = oc.load_fundamental_months()
    mom, _ = oc.load_momentum_months()
    months = sorted(set(fund) & set(mom))
    latest = months[-1] if months else None
    recon = (set(fund.get(latest) or {}) & set(mom.get(latest) or {})) if latest else set()
    payload = {
        "live_owner": "api.universe_scoring",
        "live_model_id": scoring.get("primary_model_id"),
        "declared_model_id": oc.OPERATIONAL_MODEL_ID,
        "model_identity_matches": scoring.get("primary_model_id") == oc.OPERATIONAL_MODEL_ID,
        "live_eligible_market_date": scoring.get("eligible_market_date"),
        "live_fundamental_as_of_date": scoring.get("fundamental_as_of_date"),
        "live_universe_size": len(live),
        "reconstruction_latest_month": latest,
        "reconstruction_universe_size": len(recon),
        "live_names_covered_by_reconstruction": len(live & recon),
        "live_names_missing_from_reconstruction": sorted(live - recon)[:25],
    }
    _write("reconstruction_verification.json", payload)
    print(json.dumps(payload, indent=1))
    covered = payload["live_names_covered_by_reconstruction"]
    print("R30_1_RECONSTRUCTION_%s %d/%d"
          % ("VERIFIED" if covered == len(live) else "PARTIAL", covered, len(live)))
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Release 30.1 operational calibration")
    ap.add_argument("--stage", default="calibrate", choices=("calibrate", "verify"))
    args = ap.parse_args(argv)
    return {"calibrate": stage_calibrate, "verify": stage_verify}[args.stage](args)


if __name__ == "__main__":
    raise SystemExit(main())
