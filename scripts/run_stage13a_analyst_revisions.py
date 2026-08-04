"""Stage 13A -- analyst-revision purchase-evaluation framework driver.

RESEARCH-ONLY, non-networking, deterministic. This is NOT a data-collection or
campaign runner: no provider is contacted, no credential is read, no trial data is
fetched, no strategy is built, nothing is promoted, no operational ledger is
touched. It only:

  1. freezes the immutable 6-hypothesis pre-registered revision registry
     (first-write-wins; re-freezing different content raises);
  2. runs the deterministic ten-fixture evaluator self-test;
  3. builds the read-only command center (TRIAL_NOT_STARTED until a real local
     trial extract is supplied by hand) and writes the JSON snapshots under the
     Stage 13A research state dir.

Usage (Windows PowerShell):
  C:\\Users\\binis\\paper_trader\\.venv-win\\Scripts\\python.exe scripts\\run_stage13a_analyst_revisions.py \
      --config configs\\alpha_agent\\stage13a_analyst_revisions.json --json
  # self-test only (no snapshot write):
  ... scripts\\run_stage13a_analyst_revisions.py --self-test-only
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
if str(_REPO.parent) not in sys.path:
    sys.path.insert(0, str(_REPO.parent))


def _module():
    try:
        from alpha_agent import analyst_revisions as ar
    except Exception:  # pragma: no cover
        from paper_trader.alpha_agent import analyst_revisions as ar  # type: ignore
    return ar


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Stage 13A analyst-revision purchase-evaluation framework "
                    "(research-only; no network, no purchase, no promotion)")
    ap.add_argument("--config", default="configs/alpha_agent/stage13a_analyst_revisions.json",
                    help="Stage 13A framework config")
    ap.add_argument("--self-test-only", action="store_true",
                    help="run the deterministic evaluator self-test only (no writes)")
    ap.add_argument("--json", action="store_true", help="emit the report as JSON")
    args = ap.parse_args()
    ar = _module()

    if args.self_test_only:
        report = ar.run_fixture_self_test()
        ok = bool(report.get("all_expected"))
    else:
        report = ar.write_snapshot(args.config)
        ok = bool(report.get("self_test_all_expected"))

    if args.json:
        print(json.dumps(report, indent=1, default=str))
    else:
        print("Stage 13A framework:")
        for k in ("registry_version", "self_test_all_expected", "all_expected",
                  "purchase_terminal", "fdr_family_size", "state_dir"):
            if k in report:
                print("  %s: %s" % (k, report[k]))
    return 0 if ok else 3


if __name__ == "__main__":
    raise SystemExit(main())
