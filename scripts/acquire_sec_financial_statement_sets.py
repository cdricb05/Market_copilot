"""scripts/acquire_sec_financial_statement_sets.py — Stage 26 acquisition CLI.

Fetches the ``sub.txt`` member of every published quarterly SEC Financial
Statement Data Set into the owned SEC bulk hierarchy, then writes the acquisition
manifest the stage consumes.

Free, first-party, no vendor and no quota. Reads only public SEC data; writes
only under the configured cache root and the Stage-26 research root. It never
touches an operational store, a model or the backend.

    .venv-win\\Scripts\\python.exe scripts\\acquire_sec_financial_statement_sets.py \\
        --contact <email> [--through 2026q2] [--cache-root <dir>] [--manifest <file>]
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from alpha_agent import sec_financial_statement_sets as fsds  # noqa: E402

DEFAULT_CACHE_ROOT = (r"D:\Stock_Prediction_app_data\alpha_agent\identity"
                      r"\sec_bulk\financial_statement_data_sets")
DEFAULT_MANIFEST = (r"D:\Stock_Prediction_app_data\stage26_alpha_challenger_expansion"
                    r"\_acquisition\pit_sic_acquisition_manifest.json")
UA_PRODUCT = "paper-trader-alpha-agent/2.0"


def _git_contact() -> str:
    try:
        out = subprocess.run(["git", "config", "user.email"], cwd=str(_REPO),
                             capture_output=False, stdout=subprocess.PIPE,
                             stderr=subprocess.DEVNULL, text=True, timeout=15)
        return (out.stdout or "").strip()
    except Exception:  # noqa: BLE001
        return ""


def _parse_quarter(text: str) -> "tuple[int, int]":
    s = text.strip().lower().replace("-", "").replace("_", "")
    if "q" not in s:
        raise argparse.ArgumentTypeError("expected YYYYqQ, e.g. 2026q2")
    y, q = s.split("q", 1)
    return int(y), int(q)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--contact", default="",
                    help="contact email for the SEC User-Agent (defaults to "
                         "git config user.email)")
    ap.add_argument("--through", type=_parse_quarter, default="2026q2",
                    help="last quarter to attempt (default 2026q2)")
    ap.add_argument("--cache-root", default=DEFAULT_CACHE_ROOT)
    ap.add_argument("--manifest", default=DEFAULT_MANIFEST)
    ap.add_argument("--delay", type=float, default=0.35,
                    help="seconds between HTTP requests (SEC fair access)")
    ap.add_argument("--force", action="store_true",
                    help="re-download even when a verified cached member exists")
    args = ap.parse_args(argv)

    contact = args.contact or _git_contact()
    if not contact or "@" not in contact:
        print("BLOCKED_MISSING_USER_AGENT_CONTACT: pass --contact <email>")
        return 2
    ua = "%s (%s)" % (UA_PRODUCT, contact)

    through = args.through
    if isinstance(through, str):
        through = _parse_quarter(through)
    quarters = fsds.quarters_through(through[0], through[1])
    print("acquiring %d quarters -> %s" % (len(quarters), args.cache_root))

    acq = fsds.FinancialStatementDataSetsAcquirer(
        cache_root=args.cache_root, user_agent=ua,
        request_delay_seconds=args.delay)

    results = []
    for (y, q) in quarters:
        res = acq.acquire_quarter(y, q, force=args.force)
        results.append(res)
        print("  %dq%d  %-22s member=%s bytes  net=%s bytes" % (
            y, q, res.get("disposition"),
            res.get("member_bytes", "-"),
            res.get("bytes_fetched_over_network", 0)), flush=True)
        if res.get("disposition") == fsds.D_NOT_PUBLISHED:
            print("  (quarter not yet published; stopping sweep)")
            break

    ok = [r for r in results
          if r.get("disposition") in (fsds.D_COMPLETE, fsds.D_CACHED)]
    manifest = {
        "contract_version": fsds.CONTRACT_VERSION,
        "source_host": fsds.SOURCE_HOST,
        "member": fsds.MEMBER_NAME,
        "user_agent_product": UA_PRODUCT,
        "cache_root": str(args.cache_root),
        "quarters_requested": len(results),
        "quarters_acquired": len(ok),
        "quarters_downloaded": len([r for r in results
                                    if r.get("disposition") == fsds.D_COMPLETE]),
        "quarters_from_cache": len([r for r in results
                                    if r.get("disposition") == fsds.D_CACHED]),
        "total_member_bytes": sum(int(r.get("member_bytes") or 0) for r in ok),
        "total_network_bytes": sum(
            int(r.get("bytes_fetched_over_network") or 0) for r in results),
        "first_quarter": "%dq%d" % quarters[0] if quarters else None,
        "last_quarter_acquired": (
            "%dq%d" % (ok[-1]["year"], ok[-1]["quarter"]) if ok else None),
        "failures": [r for r in results
                     if r.get("disposition") not in
                     (fsds.D_COMPLETE, fsds.D_CACHED, fsds.D_NOT_PUBLISHED)],
        "quarters": [{k: v for k, v in r.items() if k != "results"}
                     for r in results],
    }
    mp = Path(args.manifest)
    mp.parent.mkdir(parents=True, exist_ok=True)
    mp.write_text(json.dumps(manifest, indent=1, sort_keys=True), encoding="utf-8")
    print("manifest: %s" % mp)
    print("ACQUIRED %d/%d quarters, %.1f MB members, %.1f MB over the network"
          % (len(ok), len(results), manifest["total_member_bytes"] / 1e6,
             manifest["total_network_bytes"] / 1e6))
    return 0 if not manifest["failures"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
