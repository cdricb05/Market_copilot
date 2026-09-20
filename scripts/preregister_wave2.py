r"""scripts/preregister_wave2.py - apply the R57 ALPHA WAVE 2 frozen agenda.

Runs the foundation chain (certify -> define -> publish) and the ten
pre-registrations from ``research/agents/campaign_r57_wave2/
campaign_agenda.json``, then writes the campaign spec the local runner
executes.

    & .\.venv-win\Scripts\python.exe scripts\preregister_wave2.py
    & .\.venv-win\Scripts\python.exe scripts\preregister_wave2.py --dry-run

IDEMPOTENT: a repeated pre-registration returns the existing experiment id.
NOTHING IS MEASURED HERE. RESEARCH ONLY - no order, no fill, no promotion, no
adoption, no forward clock.

TERMINAL TOKENS (exactly one, on the last line)
    PREREG_OK <new>/<total>
    PREREG_REFUSED <code>       (exit 3)
    PREREG_FAILED <reason>      (exit 1)
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
for _p in (str(_REPO), str(_REPO / "research" / "agents")):
    if _p not in sys.path:
        sys.path.insert(0, _p)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--memory", default="")
    ap.add_argument("--agenda", default="")
    ap.add_argument("--spec-out", default="")
    ap.add_argument("--dry-run", action="store_true",
                    help="read and validate the agenda; write nothing")
    args = ap.parse_args(argv)

    try:
        from alpha_agent import r59                                # noqa
        from campaign_r57_wave2 import preregistration as PR       # noqa
        r59.assert_worktree_import()

        if args.dry_run:
            agenda = PR.load_agenda(Path(args.agenda) if args.agenda else None)
            by_ex = {r["executor"]: r for r in agenda["experiments"]}
            missing = [e for e in agenda["review_order"] if e not in by_ex]
            print(json.dumps({
                "campaign_id": agenda["campaign_id"], "dry_run": True,
                "datasets": len(agenda["datasets"]),
                "universes": len(agenda["universes"]),
                "feature_sets": len(agenda["feature_sets"]),
                "experiments": len(agenda["experiments"]),
                "review_order": agenda["review_order"],
                "review_order_missing": missing,
                "asset_classes": sorted({r["asset_class"]
                                         for r in agenda["experiments"]}),
            }, indent=1))
            if missing:
                print("PREREG_REFUSED REVIEW_ORDER_NAMES_UNKNOWN_EXECUTOR")
                return 3
            print("PREREG_OK 0/%d" % len(agenda["experiments"]))
            return 0

        out = PR.main(memory_path=args.memory or None,
                      agenda_path=args.agenda or None,
                      spec_out=args.spec_out or None)
        print(json.dumps(out, indent=1, default=str))
        print("PREREG_OK %d/%d" % (out["newly_registered"],
                                   len(out["experiments"])))
        return 0
    except Exception as exc:                                       # noqa: BLE001
        name = type(exc).__name__
        if name == "PipelineRefusal":
            print(json.dumps({"refused": getattr(exc, "code", name),
                              "detail": str(exc)[:600]}, indent=1))
            print("PREREG_REFUSED %s" % getattr(exc, "code", name))
            return 3
        print(json.dumps({"failed": name, "detail": str(exc)[:600]}, indent=1))
        print("PREREG_FAILED %s" % name)
        return 1


if __name__ == "__main__":
    sys.exit(main())
