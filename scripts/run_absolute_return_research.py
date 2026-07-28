"""
scripts/run_absolute_return_research.py — Phase 31B CLI.

Deterministic, read-only driver for the absolute-return diagnosis and
risk-controlled shadow-challenger study. Reads owned local Paper Trader data
only (no database connection, no network call), selects the latest completed
market date when --as-of latest is used, creates one immutable run directory
under --output-root, and writes the full 17-file evidence package plus a run
manifest.

Ends with exactly one of:
    PHASE31B_ANALYSIS_READY
    PHASE31B_ANALYSIS_BLOCKED — <exact reason>

Example (Windows PowerShell):
    C:\\Users\\binis\\paper_trader\\.venv-win\\Scripts\\python.exe `
      scripts\\run_absolute_return_research.py `
      --config configs\\research\\phase31b_absolute_return_shadow.json `
      --output-root D:\\Stock_Prediction_app_data\\paper_trader_research `
      --as-of latest `
      --json
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

# Import root = parent of the repo dir (mirrors tests/conftest.py parents[2]).
_REPO = Path(__file__).resolve().parents[1]
_PKG_PARENT = str(_REPO.parent)
if _PKG_PARENT not in sys.path:
    sys.path.insert(0, _PKG_PARENT)

from paper_trader.engine import absolute_return_research as eng  # noqa: E402


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
    ap = argparse.ArgumentParser(description="Phase 31B absolute-return research (read-only).")
    ap.add_argument("--config", required=True, help="Path to the pre-registered JSON config.")
    ap.add_argument("--output-root", required=True,
                    help="Directory under which the immutable run dir is created.")
    ap.add_argument("--as-of", default="latest",
                    help="'latest' (default) selects the latest completed market date, or a YYYY-MM-DD date.")
    ap.add_argument("--desk-dir", default=None, help="Override the desk store dir (default: env / ~/.paper_trader).")
    ap.add_argument("--json", action="store_true", help="Emit a JSON result summary to stdout.")
    ap.add_argument("--dry-run", action="store_true", help="Compute everything but write nothing.")
    args = ap.parse_args(argv)

    cfg_path = Path(args.config)
    try:
        config = json.loads(cfg_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        print("PHASE31B_ANALYSIS_BLOCKED — cannot read config %s: %s" % (cfg_path, exc))
        return 2

    try:
        result = eng.run_research(
            output_root=args.output_root, as_of=args.as_of, config=config,
            config_path=str(cfg_path), desk_dir=args.desk_dir,
            git_commit=_git_commit(_REPO), write=not args.dry_run,
        )
    except Exception as exc:  # noqa: BLE001 — surface a clean BLOCKED line
        print("PHASE31B_ANALYSIS_BLOCKED — %s: %s" % (type(exc).__name__, exc))
        return 1

    if args.json:
        summary = {k: result.get(k) for k in
                   ("status", "run_id", "run_dir", "as_of", "decision", "adequacy",
                    "reason", "files_written")}
        summary["row_counts"] = (result.get("manifest") or {}).get("row_counts")
        print(json.dumps(summary, indent=1, default=str))

    if result.get("status") == eng.READY:
        print(eng.READY)
        return 0
    print("%s — %s" % (eng.BLOCKED, result.get("reason", "unknown")))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
