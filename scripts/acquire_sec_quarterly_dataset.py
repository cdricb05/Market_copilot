"""scripts/acquire_sec_quarterly_dataset.py — Release-27 free acquisition of any
registered quarterly SEC structured-data archive.

Release 27 needed two information families the owned surface could not answer:
who is trading the stock, and what the company files OUTSIDE its periodic
reports. Neither needs a paid vendor. The SEC publishes both as quarterly,
tab-or-pipe-separated archives on the same terms as the Financial Statement Data
Sets Stage 26 already uses — free, first-party, no key, no quota — and the
released range reader transfers only the members a family actually reads.

    .venv-win\\Scripts\\python.exe scripts\\acquire_sec_quarterly_dataset.py \\
        --dataset insider_transactions_data_sets [--contact <email>]
        [--from-quarter 2009q1] [--through 2026q2] [--cache-root <dir>]

Registered data sets are declared in
``alpha_agent.sec_financial_statement_sets.QUARTERLY_DATASETS``; there is one
acquirer and one CLI rather than one per family.

Research-only. Writes under the owned SEC bulk cache and the campaign
acquisition manifest, and nowhere else: it never touches an operational store, a
model, the tournament, the shadow book or the backend.
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

SEC_BULK_ROOT = (r"D:\Stock_Prediction_app_data\alpha_agent\identity\sec_bulk")
MANIFEST_ROOT = (r"D:\Stock_Prediction_app_data\alpha_exhaustion_campaign"
                 r"\_acquisition")
UA_PRODUCT = "paper-trader-alpha-agent/2.0"


def _git_contact() -> str:
    try:
        out = subprocess.run(["git", "config", "user.email"], cwd=str(_REPO),
                             stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                             text=True, timeout=15)
        return (out.stdout or "").strip()
    except Exception:  # noqa: BLE001
        return ""


def _parse_quarter(text: str) -> "tuple[int, int]":
    s = str(text).strip().lower().replace("-", "").replace("_", "")
    if "q" not in s:
        raise argparse.ArgumentTypeError("expected YYYYqQ, e.g. 2026q2")
    y, q = s.split("q", 1)
    return int(y), int(q)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dataset", required=True,
                    choices=sorted(fsds.QUARTERLY_DATASETS))
    ap.add_argument("--contact", default="")
    ap.add_argument("--through", default="2026q2")
    ap.add_argument("--from-quarter", default="",
                    help="first quarter to acquire; defaults to the data set's "
                         "own first published quarter")
    ap.add_argument("--cache-root", default="")
    ap.add_argument("--manifest", default="")
    ap.add_argument("--delay", type=float, default=0.35)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args(argv)

    contact = args.contact or _git_contact()
    if not contact or "@" not in contact:
        print("BLOCKED_MISSING_USER_AGENT_CONTACT: pass --contact <email>")
        return 2
    ua = "%s (%s)" % (UA_PRODUCT, contact)

    cache_root = args.cache_root or str(Path(SEC_BULK_ROOT) / args.dataset)
    manifest = args.manifest or str(
        Path(MANIFEST_ROOT) / ("%s_manifest.json" % args.dataset))

    acq = fsds.QuarterlyDataSetAcquirer(
        args.dataset, cache_root=cache_root, user_agent=ua,
        request_delay_seconds=args.delay)
    through = _parse_quarter(args.through)
    if args.from_quarter:
        first = _parse_quarter(args.from_quarter)
        quarters = fsds.quarters_through(through[0], through[1],
                                         start_year=first[0],
                                         start_quarter=first[1])
    else:
        quarters = acq.quarters(through[0], through[1])

    print("dataset   : %s" % args.dataset, flush=True)
    print("members   : %s" % ", ".join(acq.members), flush=True)
    print("quarters  : %d -> %s" % (len(quarters), cache_root), flush=True)

    def _progress(res):
        print("  %sq%s %-22s net=%s" % (
            res.get("year"), res.get("quarter"), res.get("disposition"),
            res.get("bytes_fetched_over_network")), flush=True)

    summary = acq.acquire(quarters, force=args.force, on_quarter=_progress)

    mpath = Path(manifest)
    mpath.parent.mkdir(parents=True, exist_ok=True)
    mpath.write_text(json.dumps(summary, indent=1, sort_keys=True, default=str),
                     encoding="utf-8")
    print("quarters acquired : %d / %d"
          % (summary["quarters_acquired"], summary["quarters_requested"]))
    print("downloaded        : %d   from cache: %d"
          % (summary["quarters_downloaded"], summary["quarters_from_cache"]))
    print("member bytes      : %d" % summary["total_member_bytes"])
    print("network bytes     : %d" % summary["total_network_bytes"])
    print("manifest          : %s" % mpath)
    if summary["failures"]:
        for f in summary["failures"][:10]:
            print("FAILURE: %s" % json.dumps(f, default=str))
        print("SEC_ACQUISITION_INCOMPLETE")
        return 3
    print("SEC_ACQUISITION_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
