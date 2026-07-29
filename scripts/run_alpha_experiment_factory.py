"""
scripts/run_alpha_experiment_factory.py — Alpha Agent Stage 5 CLI.

Deterministic driver for the autonomous experiment & evidence engine. Reads
verified Stage 1-3.5 packages, converts grounded Stage 3 hypotheses into
allowlisted deterministic historical experiments, applies the evidence gates and
writes an immutable experiment package — never touching operational trading
state, a model, a portfolio, PostgreSQL, the prediction service or the LLM, and
never executing LLM-authored code.

Modes:
    --mode run          discover + experiment + gate + write immutable package
    --mode verify       read-only verification of the latest package (no writes)
    --mode report-only  print the deterministic report model of the latest
                        package (no discovery, no experiment, no writes)

Terminal lines (exactly one):
    ALPHA_AGENT_STAGE5_READY
    ALPHA_AGENT_STAGE5_VERIFIED
    NO_EXPERIMENTABLE_HYPOTHESES
    ALPHA_AGENT_STAGE5_DATA_HOLD
    ALPHA_AGENT_STAGE5_PARTIAL
    ALPHA_AGENT_STAGE5_BLOCKED — <reason>

Example (Windows PowerShell):
    C:\\Users\\binis\\paper_trader\\.venv-win\\Scripts\\python.exe `
      scripts\\run_alpha_experiment_factory.py `
      --config configs\\alpha_agent\\stage5_experiment_factory.json `
      --mode run --json
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_PKG_PARENT = str(_REPO.parent)
if _PKG_PARENT not in sys.path:
    sys.path.insert(0, _PKG_PARENT)

from paper_trader.alpha_agent import experiment_contracts as ec  # noqa: E402
from paper_trader.alpha_agent import experiment_factory as ef  # noqa: E402
from paper_trader.alpha_agent import runtime as rt  # noqa: E402


def _ledger_fingerprint(cfg: dict):
    return lambda: rt.fingerprint_ledgers(cfg)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Alpha Agent Stage 5 experiment & evidence engine.")
    ap.add_argument("--config", required=True, help="Stage 5 config.")
    ap.add_argument("--mode", required=True,
                    choices=["run", "verify", "report-only"])
    ap.add_argument("--output-root", dest="output_root", default=None)
    ap.add_argument("--as-of", dest="as_of", default="latest")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    try:
        cfg = ec.load_config(args.config)
    except ec.ConfigError as exc:
        print("%s — %s" % (ec.BLOCKED, exc))
        return 1

    try:
        if args.mode == "run":
            result = ef.run_stage5_cycle(
                cfg, output_root=args.output_root, as_of=args.as_of,
                ledger_fingerprint=_ledger_fingerprint(cfg))
            payload = _run_payload(result)
        elif args.mode == "verify":
            result = ef.verify_cycle(
                cfg, output_root=args.output_root,
                ledger_fingerprint=_ledger_fingerprint(cfg))
            payload = dict(result)
        else:  # report-only
            result = _report_only(cfg, args.output_root)
            payload = result
    except Exception as exc:  # noqa: BLE001 — one clean BLOCKED line
        print("%s — %s: %s" % (ec.BLOCKED, type(exc).__name__,
                               ec.redact(str(exc))))
        return 1

    if args.json:
        print(json.dumps(payload, indent=1, sort_keys=True, default=str))

    terminal = result.get("terminal")
    reason = result.get("status") or ""
    if terminal in (ec.BLOCKED, ec.DATA_HOLD, ec.PARTIAL):
        print("%s — %s" % (terminal, reason))
    else:
        print(terminal)
    return 1 if terminal == ec.BLOCKED else 0


def _run_payload(result: dict) -> dict:
    return {
        "run_id": result.get("run_id"),
        "terminal": result.get("terminal"),
        "status": result.get("status"),
        "run_dir": result.get("run_dir"),
        "counts": result.get("counts"),
        "champion_model": result.get("champion_model"),
        "ledgers_unchanged": result.get("ledgers_unchanged"),
        "report_model": ef.experiment_report_model(result),
    }


def _report_only(cfg: dict, output_root):
    from paper_trader.alpha_agent import experiment_factory as _ef
    root = Path(output_root or cfg["experiments_root"])
    latest = _ef._read_json(root / "latest.json") or {}
    rid = latest.get("run_id")
    if not rid:
        return {"terminal": ec.BLOCKED, "status": "NO_LATEST_PACKAGE"}
    manifest = _ef._read_json(root / "runs" / rid / "run_manifest.json") or {}
    result = _ef._load_result(root, rid, manifest)
    result["terminal"] = latest.get("terminal") or manifest.get("terminal")
    result["status"] = latest.get("status") or manifest.get("status")
    result["champion_model"] = latest.get("champion_model")
    return {**result, "report_model": _ef.experiment_report_model(result)}


if __name__ == "__main__":
    raise SystemExit(main())
