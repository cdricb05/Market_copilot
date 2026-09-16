r"""scripts/rearm_s25_prospective_collection.py - the governed ACTIVATION of
prospective evidence collection for the EXISTING frozen S25 identity.

WHAT THIS DOES, AND ONLY THIS
-----------------------------
Writes ONE governance record that stamps the PROSPECTIVE EPOCH FLOOR for
``c9_qualityprofi_e490533606``, first-write-wins. After it has run, the
``stage26_prospective_mark`` stage inside the ONE canonical research runtime
will collect marks for eligible sessions STRICTLY AFTER that floor.

It writes no mark, creates no book, no candidate and no registration, starts no
task, restarts nothing, and spends nothing. It is the deliberate human act that
separates "the repair is deployed" from "the clock is running", so a deployment
can be reviewed before any evidence begins to accrue.

WHY THE FLOOR IS DERIVED AND NOT TYPED
--------------------------------------
The floor is ``max(2026-09-15, the latest completed session in the owned
panel)``. The first term is the last permanently forfeited session, so a stale
panel can never lower the floor into the 21-session gap; the second means an
already-closed session cannot be left collectable. Both terms are recorded.

USAGE (Windows PowerShell)

    & C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe `
        C:\Users\binis\paper_trader\scripts\rearm_s25_prospective_collection.py `
        --confirm REARM_S25_PROSPECTIVE_COLLECTION

    # inspect without writing anything:
    ... rearm_s25_prospective_collection.py --status

Terminal tokens (exactly one):
    S25_REARM_ACTIVATED - <floor>
    S25_REARM_ALREADY_ACTIVE - <floor>
    S25_REARM_REFUSED - <reason>            (exit 3)
    S25_REARM_FAILED - <reason>             (exit 1)
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
for _p in (str(_REPO.parent), str(_REPO)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

ACTIVATED = "S25_REARM_ACTIVATED"
ALREADY = "S25_REARM_ALREADY_ACTIVE"
REFUSED = "S25_REARM_REFUSED"
FAILED = "S25_REARM_FAILED"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Authorise PROSPECTIVE-ONLY evidence collection for the "
                    "existing frozen S25 identity. Never backfills.")
    ap.add_argument("--confirm", default=None,
                    help="the explicit confirmation token; without it nothing "
                         "is written")
    ap.add_argument("--status", action="store_true",
                    help="read-only: print the stream's current state and "
                         "return without writing")
    ap.add_argument("--store-root", default=None,
                    help="override the Stage-8 store root (tests / drills)")
    args = ap.parse_args(argv)

    from alpha_agent import stage26_forward_runtime as S26F   # noqa: PLC0415

    if args.status:
        print(json.dumps(S26F.status(store_root=args.store_root), indent=1,
                         default=str))
        return 0

    # The latest COMPLETED session comes from the owned panel's benchmark axis -
    # the same axis the accrual stage itself uses, so the floor and the
    # collection rule can never disagree about what "now" means.
    try:
        from paper_trader.api import price_panel as PP        # noqa: PLC0415
        panel = PP.load_owned_current_panel()
        dates = (((panel or {}).get("series") or {})
                 .get(S26F.BENCHMARK) or {}).get("dates") or []
        latest = str(dates[-1])[:10] if dates else ""
    except Exception as exc:                                  # noqa: BLE001
        print("%s - the owned panel could not be read (%s: %s); the epoch "
              "floor must be derived from it, so nothing was written"
              % (FAILED, type(exc).__name__, str(exc)[:160]))
        return 1

    res = S26F.activate(latest_completed_session=latest,
                        confirm=args.confirm, store_root=args.store_root)
    state = str(res.get("state"))
    gov = res.get("governance") or {}
    floor = gov.get("prospective_epoch_floor_session")

    if state == "REFUSED":
        print("%s - %s (expected --confirm %s). Nothing was written."
              % (REFUSED, res.get("reason"), res.get("expected")))
        return 3
    if state == "ALREADY_ACTIVE":
        print("%s - %s (first-write-wins; the floor was NOT moved)"
              % (ALREADY, floor))
        return 0
    if state == "ACTIVATED":
        print(json.dumps(gov, indent=1, default=str))
        print("%s - %s (latest completed session at activation: %s)"
              % (ACTIVATED, floor,
                 gov.get("latest_completed_session_at_activation")))
        return 0
    print("%s - unexpected activation state %s" % (FAILED, state))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
