r"""scripts/r61_identity_audit.py - audit the R60 extension identity bridge.

RESEARCH DATA-QUALITY ONLY. NOT AN ALPHA EXPERIMENT. Read-only over owned
stores; registers no hypothesis, scores no candidate, charges no burden.

PowerShell:

    & C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe `
        C:\Users\binis\paper_trader\scripts\r61_identity_audit.py

Writes the FROZEN pre-registration before measuring anything, then the
measurements, the versioned content-hashed identity map, and one of
IDENTITY_BRIDGE_PASS / IDENTITY_BRIDGE_FAIL / IDENTITY_BRIDGE_INCONCLUSIVE.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from alpha_agent import r61                                   # noqa: E402
from alpha_agent.r61 import identity_audit as IA              # noqa: E402
from alpha_agent.r61 import workers as W                      # noqa: E402

WORKER_ID = "r61_identity_audit"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=str(r61.IDENTITY_ROOT))
    ap.add_argument("--log", default=None)
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    with W.worker(WORKER_ID, command=[sys.executable] + sys.argv,
                  log_path=args.log, kind=W.KIND_HEAVY) as row:
        print("WORKER pid=%s parent=%s started=%s"
              % (row["pid"], row["parent_pid"], row["started_at"]), flush=True)
        prereg = IA.pre_registration()
        r61.write_artifact(out / "PRE_REGISTRATION.json", prereg)
        print("PRE_REGISTRATION frozen: %s"
              % prereg["pre_registration_hash"][:16], flush=True)

        t0 = time.time()
        result = IA.run(verbose=not args.quiet)
        result["pre_registration_hash"] = prereg["pre_registration_hash"]
        result["elapsed_seconds"] = round(time.time() - t0, 1)
        r61.write_artifact(out / "IDENTITY_BRIDGE_AUDIT.json", result)

    print(json.dumps({
        "verdict": result["verdict"],
        "identity_map_hash": result["identity_map_hash"][:16],
        "coverage": result["coverage"],
        "failed_checks": result["failed_checks"],
        "unmeasurable_checks": result["unmeasurable_checks"],
    }, indent=1))
    print(result["verdict"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
