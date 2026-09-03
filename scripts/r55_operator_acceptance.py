r"""Release 55 — the ACTIVE MANAGER OPERATOR ACCEPTANCE view (read-only).

WHAT THIS IS
------------
One command that renders the Active Manager acceptance contract for a human:
one row per stage of the chain, each quoting the owner that decided it, and the
three operator answers the Today page leads with.

It is a REPORTER, not an owner. It performs a single authenticated GET, applies
the ONE acceptance function that already exists
(``api.active_manager_state.build_acceptance_contract``) and prints the result.
It computes no economics, resolves no authority, orders no action and
manufactures no timestamp.

WHY IT STILL WORKS AGAINST AN OLDER RUNTIME
-------------------------------------------
A backend serving a pre-R55 payload publishes no ``acceptance`` block. Rather
than fail, this script applies the SAME owner function locally to the fetched
payload — one owner, two ways to reach it — and says which path it used. Rows
whose facts the older payload genuinely lacks are reported MISSING, which is the
truthful answer.

SAFETY
------
GET only. No write, no order, no approval, no restart, no store access. It never
starts, stops or probes a process; the canonical restart owner remains
``scripts\restart_paper_trader_backend.ps1``.

USAGE
-----
    & C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe ^
        C:\Users\binis\paper_trader\scripts\r55_operator_acceptance.py

    --base-url   default http://127.0.0.1:8001
    --json       print the acceptance contract as JSON and nothing else
    --file PATH  read a previously captured payload instead of calling the route

Exit codes: 0 when every acceptance row is PRESENT, 2 when any row is MISSING
(a truthful gap is a non-zero exit, never a silent pass), 1 on a transport or
payload error.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, os.path.dirname(REPO_ROOT))

ROUTE = "/v1/operations/active-manager-state"
DEFAULT_BASE = "http://127.0.0.1:8001"
API_KEY_ENV = "PAPER_TRADER_SERVICE_API_KEY"


def _fetch(base_url: str, timeout: float = 30.0) -> dict:
    url = base_url.rstrip("/") + ROUTE
    req = urllib.request.Request(url, method="GET")
    key = os.environ.get(API_KEY_ENV)
    if key:
        req.add_header("X-API-Key", key)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _acceptance(state: dict) -> tuple:
    """The acceptance contract + how it was obtained. ONE owner either way."""
    served = state.get("acceptance")
    if isinstance(served, dict) and served.get("rows"):
        return served, "SERVED_BY_THE_BACKEND"
    from paper_trader.api import active_manager_state as ams
    return ams.build_acceptance_contract(state), "COMPOSED_LOCALLY_FROM_THE_PAYLOAD"


def _fmt(value) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, (list, tuple)):
        return ", ".join(str(v) for v in value) if value else "-"
    return str(value)


def _rule(char: str = "=") -> str:
    return char * 78


def _print_answers(state: dict) -> None:
    ans = state.get("operator_answer")
    if not isinstance(ans, dict) or not ans.get("current_decision"):
        print(_rule())
        print("THE THREE OPERATOR ANSWERS")
        print(_rule())
        print("  Not published by this runtime (pre-R55 payload). Nothing is")
        print("  substituted in their place.")
        print()
        return
    dec = ans.get("current_decision") or {}
    chg = ans.get("what_changed_since") or {}
    act = ans.get("what_to_do_now") or {}
    print(_rule())
    print("THE THREE OPERATOR ANSWERS")
    print(_rule())
    print("  1  CURRENT DECISION")
    print("       %s" % _fmt(dec.get("headline")))
    print("       session %s | decided %s | produced by %s"
          % (_fmt(dec.get("session_display") or dec.get("session")),
             _fmt(dec.get("decided_at_display")), _fmt(dec.get("provenance"))))
    print("       persisted in the governed ledger: %s"
          % _fmt(dec.get("persisted")))
    print()
    print("  2  WHAT CHANGED SINCE")
    print("       %s material event(s) evaluated | %s directly affect a holding"
          % (_fmt(chg.get("material_events_evaluated")),
             _fmt(chg.get("affected_current_holdings_count"))))
    print("       latest reassessment %s -> %s"
          % (_fmt(chg.get("latest_reassessment_display")),
             _fmt(chg.get("latest_reassessment_conclusion"))))
    print("       research lane | supersedes standing decision: %s"
          % _fmt(chg.get("supersedes_standing_decision")))
    print()
    print("  3  WHAT YOU NEED TO DO")
    print("       %s" % _fmt(act.get("action_label")))
    print("       %s" % _fmt(act.get("action_detail")))
    print("       requires operator work: %s | executes: %s"
          % (_fmt(act.get("requires_operator_work")), _fmt(act.get("executes"))))
    print()


def _print_acceptance(acc: dict, source: str) -> None:
    print(_rule())
    print("ACTIVE MANAGER ACCEPTANCE  (%s)" % source)
    print(_rule())
    for row in acc.get("rows") or []:
        print("  %-18s %-8s %s" % (row.get("row"), row.get("status"),
                                   row.get("owner")))
        for key, value in row.items():
            if key in ("row", "status", "owner") or value in (None, [], {}):
                continue
            print("      %-34s %s" % (key, _fmt(value)))
    print()
    print("  present %s/%s | missing: %s"
          % (acc.get("present_count"), len(acc.get("rows") or []),
             _fmt(acc.get("missing_rows"))))
    print()


def _print_advisories(state: dict) -> None:
    rows = state.get("advisory_components")
    if not rows:
        return
    print(_rule())
    print("AUDIT ADVISORIES  (true, retained, NOT an operator problem)")
    print(_rule())
    for row in rows:
        print("  %s (%s)" % (row.get("component"), row.get("owner_state")))
        print("      %s" % _fmt(row.get("display_label") or row.get("detail")))
        if row.get("advisory_reason"):
            print("      why not an operator problem:")
            print("        %s" % _fmt(row.get("advisory_reason")))
    print()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="R55 Active Manager operator acceptance view (read-only).")
    ap.add_argument("--base-url", default=DEFAULT_BASE,
                    help="Backend base URL (default %s)." % DEFAULT_BASE)
    ap.add_argument("--file", default=None,
                    help="Read a captured payload instead of calling the route.")
    ap.add_argument("--json", action="store_true",
                    help="Print the acceptance contract as JSON and nothing else.")
    args = ap.parse_args(argv)

    try:
        if args.file:
            with open(args.file, "r", encoding="utf-8-sig") as fh:
                state = json.load(fh)
        else:
            state = _fetch(args.base_url)
    except (urllib.error.URLError, OSError, ValueError) as exc:
        print("R55_ACCEPTANCE_ERROR - %s" % exc, file=sys.stderr)
        return 1

    acc, source = _acceptance(state)

    if args.json:
        print(json.dumps(acc, indent=2, sort_keys=True, ensure_ascii=False))
        return 0 if acc.get("complete") else 2

    print()
    print("Active Manager operational acceptance  (READ-ONLY)")
    print("  route     %s" % ROUTE)
    print("  generated %s" % _fmt(state.get("generated_at")))
    print("  owner     %s" % _fmt(state.get("owner")))
    print()
    _print_answers(state)
    _print_acceptance(acc, source)
    _print_advisories(state)

    if acc.get("complete"):
        print("R55_ACCEPTANCE_COMPLETE")
        return 0
    print("R55_ACCEPTANCE_INCOMPLETE - missing: %s"
          % ", ".join(acc.get("missing_rows") or []))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
