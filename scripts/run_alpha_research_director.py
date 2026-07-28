"""
scripts/run_alpha_research_director.py — Stage 3 CLI.

Modes: audit | analyze | incremental | verify.
Prints exactly ONE terminal token line (plus optional --json detail).

Never prints secrets; the git contact/config is read only for run identity.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO.parent) not in sys.path:
    sys.path.insert(0, str(_REPO.parent))

from paper_trader.alpha_agent import research_director as rd  # noqa: E402


def _git(args: list[str]) -> str:
    try:
        out = subprocess.run(["git", *args], cwd=str(_REPO),
                             capture_output=True, text=True, timeout=15,
                             shell=False)
        return (out.stdout or "").strip()
    except Exception:  # noqa: BLE001
        return ""


def main() -> int:
    ap = argparse.ArgumentParser(description="Alpha Agent Stage 3 research "
                                             "director")
    ap.add_argument("--config", required=True)
    ap.add_argument("--output-root", required=True)
    ap.add_argument("--mode", required=True,
                    choices=["audit", "analyze", "incremental", "verify"])
    ap.add_argument("--as-of", default="latest")
    ap.add_argument("--json", action="store_true")
    ns = ap.parse_args()

    config = json.loads(Path(ns.config).read_text(encoding="utf-8-sig"))
    git_commit = _git(["rev-parse", "HEAD"]) or "UNKNOWN"

    result = rd.run_director(config, ns.output_root, ns.mode, ns.as_of,
                             git_commit=git_commit)
    print(result["terminal"])
    if ns.json:
        detail = {k: v for k, v in result.items()
                  if k in ("token", "run_id", "run_dir", "already_existed",
                           "metrics", "considered", "problems",
                           "receipts_count")}
        print(json.dumps(detail, indent=1, sort_keys=True, default=str))
    return 0 if result["token"] != rd.BLOCKED else 1


if __name__ == "__main__":
    sys.exit(main())
