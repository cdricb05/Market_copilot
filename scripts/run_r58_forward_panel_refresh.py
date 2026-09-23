r"""scripts/run_r58_forward_panel_refresh.py - rebuild the R58 FORWARD panel copy.

The child process :mod:`alpha_agent.r68.r58_cadence_runtime` spawns when a
cadence boundary is within reach. It exists as a separate process for two
reasons the estate has already paid for once: a vendor call that hangs must not
wedge the long-lived research worker or the runtime lock it holds, and walking
~1,900 symbols is not work a worker cycle should carry.

It calls the CANONICAL builder, ``alpha_agent.r57.panel.build_panel``. It does
not contain a second panel definition. The only things that differ from the
frozen research build are the STORE - ``PAPER_TRADER_R57_RESEARCH_ROOT`` is set
by the parent to the forward directory, so the frozen panel is never opened for
writing - and the window END, which is live rather than 2026-09-03.

TERMINAL TOKENS (exactly one, on the last line)
    R58_FORWARD_PANEL_OK <last session>
    R58_FORWARD_PANEL_FAILED <reason>

RESEARCH ONLY. It reads owned vendor data and writes one panel cache. No order,
no fill, no promotion, no capital, no operational-store write.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

ROOT_ENV = "PAPER_TRADER_R57_RESEARCH_ROOT"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--end", required=True,
                    help="last session to request from the vendor (YYYY-MM-DD)")
    ap.add_argument("--progress-every", type=int, default=400)
    args = ap.parse_args(argv)

    root = os.environ.get(ROOT_ENV)
    if not root:
        print("R58_FORWARD_PANEL_FAILED %s is not set; this script refuses to "
              "rebuild the frozen research panel in place" % ROOT_ENV)
        return 2

    from alpha_agent.r57 import panel as R57P                # noqa: E402
    from alpha_agent import r57 as R57                       # noqa: E402

    if Path(R57.research_root()) == Path(R57.DEFAULT_RESEARCH_ROOT):
        print("R58_FORWARD_PANEL_FAILED the resolved research root is the "
              "FROZEN default (%s); the forward copy is never built there"
              % R57.DEFAULT_RESEARCH_ROOT)
        return 2

    try:
        meta = R57P.build_panel(progress_every=args.progress_every,
                                end=args.end, force=True)
    except Exception as exc:                                 # noqa: BLE001
        print("R58_FORWARD_PANEL_FAILED %s: %s"
              % (type(exc).__name__, str(exc)[:200]))
        return 1

    dates = meta.get("dates") or []
    last = str(dates[-1]) if dates else "NONE"
    print(json.dumps({"panel_dir": str(R57P.panel_dir()),
                      "requested_end": args.end,
                      "n_dates": meta.get("n_dates"),
                      "n_symbols": meta.get("n_symbols"),
                      "date_end": meta.get("date_end"),
                      "manifest_hash": meta.get("manifest_hash"),
                      "is_frozen_research_window":
                          meta.get("is_frozen_research_window")},
                     indent=1, default=str))
    if last == "NONE":
        print("R58_FORWARD_PANEL_FAILED the rebuilt panel holds no session")
        return 1
    print("R58_FORWARD_PANEL_OK %s" % last)
    return 0


if __name__ == "__main__":                                   # pragma: no cover
    raise SystemExit(main())
