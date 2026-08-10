#!/usr/bin/env python
r"""Intrinio TRIAL entitlement probe (read-only, credential-safe).

Runs the smallest safe Intrinio entitlement audit through the EXISTING collector
machinery (``alpha_agent.collectors.intrinio.IntrinioCollector`` over
``BaseCollector.fetch``) and classifies US-Fundamentals and Zacks entitlement as
ACTIVE / NOT_PROVISIONED / AUTH_FAILURE / ENDPOINT_NOT_ENTITLED / NETWORK_FAILURE /
RATE_LIMITED / UNKNOWN. It ALSO runs the canonical pure Data-Expansion gate
(``engine.data_expansion_gate.evaluate_dataset``) honestly over each Intrinio
dataset (no measured evidence yet -> INSUFFICIENT_EVIDENCE) and consults the
Stage-13A analyst-revision purchase gate for the historical-Zacks question.

Safety: NEVER prints or persists the API key. When the trial credential is absent
the collector short-circuits to NOT_PROVISIONED and NO network call is made. This
script purchases nothing, activates no provider, changes no billing, writes no
operational ledger, mutates no portfolio, promotes no model and enables no cadence.
It only WRITES a secret-free provenance JSON under the trial research root on D:
(and, with --out, a copy into the handoff dir).

Usage (Windows PowerShell):
  .\.venv-win\Scripts\python.exe scripts\intrinio_entitlement_probe.py --json
  .\.venv-win\Scripts\python.exe scripts\intrinio_entitlement_probe.py --out D:\Temp\paper_trader_intrinio_trial_handoff
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO.parent))

from paper_trader.alpha_agent import source_contracts as sc                # noqa: E402
from paper_trader.alpha_agent.collectors.base import (                     # noqa: E402
    CollectorContext, RawArchive, default_transport)
from paper_trader.alpha_agent.collectors.intrinio import (                 # noqa: E402
    IntrinioCollector, CLS_NOT_PROVISIONED)
from paper_trader.engine import data_expansion_gate as gate               # noqa: E402


def _now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def _load_cfg() -> dict:
    p = _REPO / "configs" / "alpha_agent" / "intrinio_trial.json"
    return json.loads(p.read_text(encoding="utf-8"))


def _build_collector(cfg: dict) -> IntrinioCollector:
    import os
    run_cfg = cfg.get("run") or {}
    source_cfg = dict(cfg.get("source") or {})
    research_root = Path(cfg.get("research_root")
                         or r"D:\Stock_Prediction_app_data\provider_trials\intrinio")
    research_root.mkdir(parents=True, exist_ok=True)
    max_bytes = int((run_cfg.get("limits") or {}).get("raw_object_max_bytes", 33554432))
    archive = RawArchive(raw_root=research_root / "raw", output_root=research_root,
                         known_ids=set(), max_bytes=max_bytes)
    ctx = CollectorContext(
        config=run_cfg, source_cfg=source_cfg, archive=archive,
        transport=default_transport, now_iso=_now_iso, clock=time.monotonic,
        sleep=time.sleep, secrets=[], user_agent=run_cfg.get("user_agent"),
        env=dict(os.environ), identity=sc.IdentityResolver())
    return IntrinioCollector(ctx)


def _honest_gate(dataset_id: str, data_category: str, *, current_only: bool) -> dict:
    """Run the canonical pure gate with NO measured evidence -> the honest
    INSUFFICIENT_EVIDENCE (never fabricated)."""
    ds_meta = {}
    if current_only:
        ds_meta = {"pit_guarantee": "CURRENT_ONLY", "estimates_are_current_consensus": True}
    input_contract = {
        "schema_version": gate.INPUT_SCHEMA_VERSION,
        "dataset_id": dataset_id,
        "provider": "Intrinio",
        "data_category": data_category,
        "dataset_name": "Intrinio %s (TRIAL)" % data_category,
        "dataset": ds_meta,
        # Evaluate the CURRENT-data trial for current-data use (do not yet assert
        # the historical-revision research requirement — that is the Stage-13A
        # question and stays DATA_HOLD until real historical PIT data exists).
        "research_requirements": {"requires_point_in_time": not current_only},
        "evidence": {"available": False},           # no measured evidence yet
        "existing_ownership": {"entitlement_state": "NOT_ENTITLED"},
    }
    res = gate.evaluate_dataset(input_contract=input_contract)
    return {"recommendation_state": res.get("recommendation_state"),
            "reason_codes": res.get("state_reason_codes"),
            "evaluation_hash": res.get("evaluation_hash")}


def _historical_zacks_stage13a() -> dict:
    """The Stage-13A analyst-revision purchase gate for the HISTORICAL-Zacks
    question. A current-only trial keeps this at PT_TRIAL_NOT_STARTED (DATA_HOLD
    equivalent) — historical revision alpha is never claimed without real
    historical PIT revision data."""
    try:
        from paper_trader.alpha_agent import analyst_revisions as ar
        dec = ar.purchase_decision(
            trial_started=False, adequacy={}, power={}, integrity={},
            cost={}, value_assumptions={})
        return {"terminal": dec.get("purchase_terminal") if isinstance(dec, dict) else str(dec),
                "reason": dec.get("reason") if isinstance(dec, dict) else None,
                "manual_approval_required": (dec.get("manual_approval_required")
                                             if isinstance(dec, dict) else None)}
    except Exception as exc:  # noqa: BLE001 — never fail the probe on this
        return {"terminal": "PT_TRIAL_NOT_STARTED",
                "note": "Stage-13A not invoked: %s" % type(exc).__name__}


def main() -> int:
    ap = argparse.ArgumentParser(description="Intrinio TRIAL entitlement probe (read-only).")
    ap.add_argument("--json", action="store_true", help="emit the full provenance JSON to stdout")
    ap.add_argument("--out", default=None, help="also copy the provenance JSON into this dir")
    args = ap.parse_args()

    cfg = _load_cfg()
    collector = _build_collector(cfg)
    result = collector.audit()

    entitlements = result.get("entitlements") or []
    classification = (result.get("inventory") or {}).get("entitlement_classification") or {}
    trial_provisioned = bool((result.get("inventory") or {}).get("trial_provisioned"))
    health = result.get("health") or {}
    credential_present = health.get("credential_present")

    # Honest canonical gate for each Intrinio dataset (no evidence yet).
    datasets = (cfg.get("source") or {}).get("datasets") or []
    gate_results = {}
    for ds in datasets:
        gate_results[ds.get("dataset_key")] = _honest_gate(
            ds.get("data_expansion_dataset_id") or ds.get("dataset_key"),
            ds.get("data_category") or "unknown",
            current_only=bool(ds.get("current_data_only")))

    provenance = {
        "probe": "intrinio_entitlement_probe",
        "provider": "Intrinio",
        "license_state": "TRIAL",
        "operational_use_allowed": False,
        "research_use_only": True,
        "retrieved_at": _now_iso(),
        "base_url": (cfg.get("source") or {}).get("base_url"),
        "credential_present": credential_present,
        "trial_provisioned": trial_provisioned,
        "overall_source_health": health.get("overall_state"),
        "entitlement_classification": classification,
        "entitlements": [
            {k: e.get(k) for k in ("dataset_key", "data_category", "classification",
                                   "state", "http_status", "current_data_only")}
            for e in entitlements],
        "data_expansion_gate": gate_results,
        "historical_zacks_stage13a": _historical_zacks_stage13a(),
        "safety": {
            "purchased_data": False, "activated_provider": False,
            "changed_billing": False, "wrote_operational_ledger": False,
            "mutated_portfolio": False, "promoted_model": False,
            "enabled_cadence": False, "printed_secret": False,
            "network_call_made": bool(trial_provisioned),
        },
        "note": ("NOT_PROVISIONED means the trial credential is absent and NO "
                 "network call was made; run scripts\\configure_alpha_agent_intrinio.ps1 "
                 "(or set INTRINIO_API_KEY) once Intrinio activates the trial, then "
                 "re-run this probe."),
    }

    # Persist the secret-free provenance under the trial research root.
    research_root = Path(cfg.get("research_root")
                         or r"D:\Stock_Prediction_app_data\provider_trials\intrinio")
    ent_dir = research_root / "entitlement"
    ent_dir.mkdir(parents=True, exist_ok=True)
    stamp = _now_iso().replace(":", "").replace("-", "")[:15]
    out_path = ent_dir / ("intrinio_entitlement_%s.json" % stamp)
    out_path.write_text(json.dumps(provenance, indent=2, sort_keys=True), encoding="utf-8")
    written = [str(out_path)]
    if args.out:
        cp = Path(args.out)
        cp.mkdir(parents=True, exist_ok=True)
        cp2 = cp / "intrinio_entitlement_probe.json"
        cp2.write_text(json.dumps(provenance, indent=2, sort_keys=True), encoding="utf-8")
        written.append(str(cp2))

    all_not_provisioned = (bool(classification)
                           and all(c == CLS_NOT_PROVISIONED for c in classification.values()))
    if args.json:
        print(json.dumps(provenance, indent=2, sort_keys=True))
    else:
        print("Intrinio TRIAL entitlement probe")
        print("  credential_present   :", credential_present)
        print("  trial_provisioned    :", trial_provisioned)
        print("  overall_source_health:", health.get("overall_state"))
        for k, c in sorted(classification.items()):
            print("  entitlement[%s] = %s" % (k, c))
        for k, g in sorted(gate_results.items()):
            print("  data_expansion_gate[%s] = %s" % (k, g.get("recommendation_state")))
        print("  historical_zacks(stage13a) =",
              provenance["historical_zacks_stage13a"].get("terminal"))
        for w in written:
            print("  wrote:", w)
    # Terminal token (never a secret).
    if all_not_provisioned:
        print("INTRINIO_ENTITLEMENT = NOT_PROVISIONED")
    else:
        print("INTRINIO_ENTITLEMENT_PROBE_DONE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
