"""
scripts/run_alpha_data_ingestion.py — Alpha Agent Stage 2 CLI.

Deterministic driver for the data-acquisition foundation. Reads the Stage 1
registry, audits/collects the configured real sources, archives raw objects
immutably, normalizes records, maintains resumable checkpoints and writes an
immutable evidence run under --output-root. Never calls a model API, never
connects to PostgreSQL, never mutates Paper Trader data, never prints secrets.

Modes:
    --mode audit        minimum-network source availability + entitlement audit
    --mode collect      bounded real collection; publishes latest.json on success
    --mode incremental  resume from checkpoints; NO_NEW_SOURCE_DATA when nothing new
    --mode verify       validate the latest run; no network, writes nothing

Terminal lines (exactly one):
    ALPHA_AGENT_STAGE2_READY
    ALPHA_AGENT_STAGE2_PARTIAL — <concise blocker summary>
    NO_NEW_SOURCE_DATA
    ALPHA_AGENT_STAGE2_VERIFIED
    ALPHA_AGENT_STAGE2_BLOCKED — <exact reason>

Example (Windows PowerShell):
    C:\\Users\\binis\\paper_trader\\.venv-win\\Scripts\\python.exe `
      scripts\\run_alpha_data_ingestion.py `
      --config configs\\alpha_agent\\stage2_ingestion.json `
      --output-root D:\\Stock_Prediction_app_data\\alpha_agent\\ingestion `
      --mode collect `
      --as-of latest `
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

from paper_trader.alpha_agent import ingestion as ing  # noqa: E402


def _git(args: list[str]) -> str:
    try:
        out = subprocess.run(["git", "-C", str(_REPO)] + args,
                             capture_output=True, text=True, timeout=15)
        if out.returncode == 0:
            return out.stdout.strip()
    except Exception:  # noqa: BLE001
        pass
    return ""


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Alpha Agent Stage 2 data-acquisition foundation.")
    ap.add_argument("--config", required=True, help="Stage 2 config JSON path.")
    ap.add_argument("--output-root", required=True,
                    help="Ingestion output root (state/raw/normalized/runs/latest.json).")
    ap.add_argument("--mode", default="collect",
                    choices=["audit", "collect", "incremental", "verify"])
    ap.add_argument("--as-of", default="latest", dest="as_of",
                    help="Business date YYYY-MM-DD or 'latest'.")
    ap.add_argument("--json", action="store_true",
                    help="Emit a JSON result summary to stdout.")
    args = ap.parse_args(argv)

    cfg_path = Path(args.config)
    try:
        config = json.loads(cfg_path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError) as exc:
        print("%s — cannot read config %s: %s" % (ing.BLOCKED, cfg_path, exc))
        return 2

    git_commit = _git(["rev-parse", "HEAD"]) or "UNKNOWN"
    # Contact for the SEC-compliant User-Agent; resolved from the EXISTING safe
    # configuration (git config user.email). Never printed by this CLI.
    contact_email = _git(["config", "user.email"]) or None

    try:
        result = ing.run_ingestion(
            config=config, output_root=args.output_root, mode=args.mode,
            as_of=args.as_of, git_commit=git_commit,
            contact_email=contact_email, config_path=str(cfg_path))
    except Exception as exc:  # noqa: BLE001 — surface one clean BLOCKED line
        print("%s — %s: %s" % (ing.BLOCKED, type(exc).__name__, exc))
        return 1

    if args.json:
        summary = {k: result.get(k) for k in
                   ("status", "terminal", "run_id", "run_dir", "as_of", "reason",
                    "counts", "already_existed", "blocked_sources",
                    "ledger_unchanged", "dq_failures", "source_states", "checked")}
        print(json.dumps(summary, indent=1, default=str))

    status = result.get("status")
    if status == ing.READY:
        print(ing.READY)
        return 0
    if status == ing.PARTIAL:
        print(result.get("terminal") or
              "%s — %s" % (ing.PARTIAL, "; ".join(result.get("blocked_sources", []))))
        return 0
    if status == ing.NO_NEW:
        print(ing.NO_NEW)
        return 0
    if status == ing.VERIFIED:
        print(ing.VERIFIED)
        return 0
    print("%s — %s" % (ing.BLOCKED, result.get("reason", "unknown")))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
