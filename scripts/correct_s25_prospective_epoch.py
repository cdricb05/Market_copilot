r"""scripts/correct_s25_prospective_epoch.py - the APPEND-ONLY governance
correction to the S25 prospective epoch boundary.

WHAT THIS CORRECTS
------------------
``scripts/rearm_s25_prospective_collection.py`` stamped the prospective epoch
floor as ``max(the last forfeited session, THE OWNED PANEL'S newest session)``.
A panel lags the exchange. At the real activation instant -
``2026-09-16T20:15:01Z``, which is **16:15:01 ET** - the 2026-09-16 NYSE session
had closed 15 minutes earlier while the panel's newest bar was still
2026-09-15. The floor was therefore stamped one session too low, leaving an
ALREADY-COMPLETED session collectable the moment the panel caught up.

Data ARRIVAL may decide whether a legitimate post-floor session can be priced.
It may NOT decide whether a session had already HAPPENED. That half of the
question belongs to ``engine.market_session`` and ``engine.exchange_calendar``,
and is answered against the EXCHANGE CLOSE rather than the owned-data cutoff.

WHAT THIS DOES, AND ONLY THIS
-----------------------------
Appends ONE correction entry to ``s25_prospective_epoch_correction.json``,
beside the activation record in the same governance store.

It does NOT rewrite ``s25_prospective_rearm.json``. That document is an
immutable record of what was actually done, wrong floor included, and rewriting
it would destroy the only evidence that the defect ever existed.

It writes no mark, deletes no evidence row, restates no evidence row, creates no
candidate, changes no inception, changes no membership, starts no task and
spends nothing. The corrected floor is DERIVED from the canonical session and
calendar owners against the activation record's OWN timestamp - no date is
accepted from the operator, and there is no override.

USAGE (Windows PowerShell)

    # read-only: what the boundary is and what it would become
    & C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe `
        C:\Users\binis\paper_trader\scripts\correct_s25_prospective_epoch.py --status

    & C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe `
        C:\Users\binis\paper_trader\scripts\correct_s25_prospective_epoch.py `
        --confirm CORRECT_S25_PROSPECTIVE_EPOCH

Terminal tokens (exactly one):
    S25_EPOCH_CORRECTED - <effective floor>
    S25_EPOCH_ALREADY_CORRECTED - <effective floor>
    S25_EPOCH_CORRECTION_NOT_REQUIRED - <effective floor>
    S25_EPOCH_CORRECTION_REFUSED - <reason>          (exit 3)
    S25_EPOCH_CORRECTION_FAILED - <reason>           (exit 1)
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

CORRECTED = "S25_EPOCH_CORRECTED"
ALREADY = "S25_EPOCH_ALREADY_CORRECTED"
NOT_REQUIRED = "S25_EPOCH_CORRECTION_NOT_REQUIRED"
REFUSED = "S25_EPOCH_CORRECTION_REFUSED"
FAILED = "S25_EPOCH_CORRECTION_FAILED"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Append the governance correction that raises the S25 "
                    "prospective epoch floor to the session that had actually "
                    "completed at activation. Never rewrites the activation "
                    "record; never backfills.")
    ap.add_argument("--confirm", default=None,
                    help="the explicit confirmation token; without it nothing "
                         "is written")
    ap.add_argument("--status", action="store_true",
                    help="read-only: print the governed epoch and the raw vs "
                         "valid mark split, and return without writing")
    ap.add_argument("--store-root", default=None,
                    help="override the Stage-8 store root (tests / drills)")
    args = ap.parse_args(argv)

    from alpha_agent import stage26_forward_runtime as S26F   # noqa: PLC0415

    if args.status:
        gov = S26F.load_governance(args.store_root)
        epoch = S26F.governed_epoch(args.store_root)
        out = {
            "activation_record_present": bool(gov),
            "activation_timestamp": (gov or {}).get("activated_at"),
            "governed_epoch": epoch,
            "would_be_canonical_completed_session_at_activation": (
                S26F.latest_completed_eligible_session(
                    (gov or {}).get("activated_at")) if gov else None),
            "stream": S26F.status(store_root=args.store_root),
        }
        print(json.dumps(out, indent=1, default=str))
        return 0

    try:
        res = S26F.record_epoch_correction(confirm=args.confirm,
                                           store_root=args.store_root)
    except Exception as exc:                                  # noqa: BLE001
        print("%s - %s: %s. Nothing was written."
              % (FAILED, type(exc).__name__, str(exc)[:200]))
        return 1

    state = str(res.get("state"))
    floor = res.get("effective_prospective_epoch_floor_session")

    if state == "REFUSED":
        print("%s - %s (expected --confirm %s). Nothing was written."
              % (REFUSED, res.get("reason"), res.get("expected")))
        return 3
    if state == "ALREADY_RECORDED":
        print("%s - %s (append-only; correction %s was already recorded and "
              "was NOT rewritten)" % (ALREADY, floor, res.get("correction_id")))
        return 0
    if state == "NOT_REQUIRED":
        print("%s - %s (the stored floor already governs the session that had "
              "completed at activation)" % (NOT_REQUIRED, floor))
        return 0
    if state == "CORRECTED":
        print(json.dumps(res.get("correction"), indent=1, default=str))
        print("%s - %s (was %s; canonical completed session at activation %s; "
              "the activation record was NOT mutated)"
              % (CORRECTED, floor,
                 res.get("original_prospective_epoch_floor_session"),
                 res.get("canonical_completed_session_at_activation")))
        return 0
    print("%s - unexpected correction state %s" % (FAILED, state))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
