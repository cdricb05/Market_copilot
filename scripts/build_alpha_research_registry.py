"""
scripts/build_alpha_research_registry.py — Alpha Agent Stage 1 CLI.

Deterministic, read-only driver that builds / incrementally updates / verifies the
Unified Alpha Research Registry. Reads owned local research artifacts and operational
ledgers only (no network, no model API, no PostgreSQL connection), writes exclusively
under --output-root, and never mutates or overwrites an immutable prior registry
version.

Modes:
    --mode bootstrap    build a full immutable registry version, publish latest.json
    --mode incremental  diff against latest; new version, or NO_RESEARCH_CHANGES
    --mode verify       validate the existing latest version; write nothing

Terminal lines (exactly one):
    ALPHA_AGENT_STAGE1_READY
    ALPHA_AGENT_STAGE1_BLOCKED — <reason>
    NO_RESEARCH_CHANGES
    ALPHA_AGENT_STAGE1_VERIFIED

Example (Windows PowerShell):
    C:\\Users\\binis\\paper_trader\\.venv-win\\Scripts\\python.exe `
      scripts\\build_alpha_research_registry.py `
      --config configs\\alpha_agent\\stage1_registry.json `
      --output-root D:\\Stock_Prediction_app_data\\alpha_agent\\registry `
      --mode bootstrap `
      --json
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

# Import root = parent of the repo dir (repo is the package `paper_trader`).
_REPO = Path(__file__).resolve().parents[1]
_PKG_PARENT = str(_REPO.parent)
if _PKG_PARENT not in sys.path:
    sys.path.insert(0, _PKG_PARENT)

from paper_trader.alpha_agent import research_registry as reg  # noqa: E402


def _git_commit(repo: Path) -> str:
    try:
        out = subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD"],
                             capture_output=True, text=True, timeout=15)
        if out.returncode == 0:
            return out.stdout.strip() or "UNKNOWN"
    except Exception:  # noqa: BLE001
        pass
    return "UNKNOWN"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Alpha Agent Stage 1 registry builder (read-only).")
    ap.add_argument("--config", required=True, help="Path to the Stage 1 config JSON.")
    ap.add_argument("--output-root", required=True,
                    help="Registry output root (immutable run dirs + latest.json).")
    ap.add_argument("--mode", default="bootstrap", choices=["bootstrap", "incremental", "verify"])
    ap.add_argument("--json", action="store_true", help="Emit a JSON result summary to stdout.")
    args = ap.parse_args(argv)

    cfg_path = Path(args.config)
    try:
        config = json.loads(cfg_path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError) as exc:
        print("%s — cannot read config %s: %s" % (reg.BLOCKED, cfg_path, exc))
        return 2

    try:
        result = reg.build_registry(config=config, output_root=args.output_root, mode=args.mode,
                                    git_commit=_git_commit(_REPO), config_path=str(cfg_path))
    except Exception as exc:  # noqa: BLE001 — surface a clean BLOCKED line
        print("%s — %s: %s" % (reg.BLOCKED, type(exc).__name__, exc))
        return 1

    if args.json:
        summary = {k: result.get(k) for k in
                   ("status", "run_id", "run_dir", "reason", "counts", "already_existed",
                    "ledger_unchanged", "decision", "incremental_diff")}
        print(json.dumps(summary, indent=1, default=str))

    status = result.get("status")
    if status == reg.READY:
        print(reg.READY)
        return 0
    if status == reg.VERIFIED:
        print(reg.VERIFIED)
        return 0
    if status == reg.NO_CHANGES:
        print(reg.NO_CHANGES)
        return 0
    print("%s — %s" % (reg.BLOCKED, result.get("reason", "unknown")))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
