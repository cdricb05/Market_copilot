"""READ-ONLY proof of the NEXT LEGAL COLLECTION STATE against the live store.

Runs the re-arm stage exactly as the canonical runtime would, against the REAL
Stage-8 store and the REAL owned panel, and proves three things:

  1. it reports AWAITING_ACTIVATION - the fail-closed default, because the
     governance record has deliberately NOT been written in this run;
  2. it writes NOTHING: no governance record, no mark, and the book's marks and
     the registry's mtime are unchanged;
  3. what the stage WOULD do once activated, computed purely, without writing -
     including the fact that at this instant every collectable session in the
     panel is a forfeited one, so the correct first answer is NOT_DUE.

No mark is written. No activation is performed. Nothing is deployed.
"""
import json
import sys
from pathlib import Path

# This checkout's own alpha_agent, ahead of anything else on the path: the
# probe must grade the tree it lives in, not whatever the venv's editable
# finder maps elsewhere.
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from alpha_agent import stage26_forward_runtime as S26F
from alpha_agent.r59 import stage25_owner as S25O

LIVE = Path(r"D:\Stock_Prediction_app_data\alpha_agent\stage8")
TODAY = "2026-09-16"


def _fingerprint():
    book = S25O.book_path(LIVE, S26F.SHADOW_BOOK_ID)
    reg = LIVE / S25O.REGISTRY_DB
    return {
        "marks": len(S25O.read_book(LIVE, S26F.SHADOW_BOOK_ID).get("marks") or []),
        "book_mtime": book.stat().st_mtime if book.exists() else None,
        "book_size": book.stat().st_size if book.exists() else None,
        "registry_mtime": reg.stat().st_mtime if reg.exists() else None,
        "registry_size": reg.stat().st_size if reg.exists() else None,
        "governance_exists": S26F.governance_path(LIVE).exists(),
    }


print("=" * 70)
print("1. THE LIVE STREAM, READ-ONLY")
print("=" * 70)
print(json.dumps(S26F.status(store_root=LIVE, today=TODAY), indent=1,
                 default=str))

before = _fingerprint()
print()
print("=" * 70)
print("2. THE STAGE, RUN EXACTLY AS THE RUNTIME WOULD")
print("=" * 70)
res = S26F.advance(store_root=LIVE)
for k in ("state", "reason", "detail", "marks_written_this_run",
          "challenger_id", "backfill"):
    print("%-24s = %s" % (k, res.get(k)))

after = _fingerprint()
print()
print("=" * 70)
print("3. NOTHING WAS WRITTEN")
print("=" * 70)
for k in sorted(before):
    same = before[k] == after[k]
    print("%-20s %-8s before=%s after=%s"
          % (k, "SAME" if same else "CHANGED", before[k], after[k]))
print()
print("UNCHANGED_ALL =", before == after)
print("MARKS_STILL_ZERO =", after["marks"] == 0)
print("GOVERNANCE_NOT_WRITTEN =", after["governance_exists"] is False)

print()
print("=" * 70)
print("4. WHAT WOULD HAPPEN ONCE ACTIVATED (computed, never written)")
print("=" * 70)
from paper_trader.api import price_panel as PP        # noqa: E402

panel = PP.load_owned_current_panel()
bench = ((panel or {}).get("series") or {}).get(S26F.BENCHMARK) or {}
sessions = bench.get("dates") or []
latest = str(sessions[-1])[:10] if sessions else None
floor = S26F.epoch_floor_for(latest)
print("panel benchmark sessions   =", len(sessions))
print("panel latest session       =", latest)
print("epoch floor WOULD BE       =", floor)
print("  (max of the last forfeited session %s and the panel's latest)"
      % S26F.FORFEITED_LAST)

resolved = S26F.resolve_evidence_session(
    panel_sessions=sessions, last_recorded=S26F.ORIGINAL_INCEPTION, floor=floor)
print("threshold                  =", resolved["threshold"])
print("session that WOULD be marked =", resolved["session"])
print("reason                     =", resolved["reason"])
print("forfeited sessions excluded =", resolved["n_forfeited_excluded"])
print("next legal collection state =",
      "NOT_DUE - the first eligible completed session strictly after %s"
      % resolved["threshold"] if resolved["session"] is None
      else "DUE for %s" % resolved["session"])
print()
print("NO_FORFEITED_SESSION_IS_COLLECTABLE =",
      not any(S26F.is_forfeited_session(s)
              for s in ([resolved["session"]] if resolved["session"] else [])))
