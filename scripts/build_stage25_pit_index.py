"""Build the Stage-25 EXTENDED SEC companyfacts index, OFFLINE.

Streams the already-owned ``companyfacts.zip`` (no network, no provider call, no
quota) through the RELEASED indexer
(:class:`alpha_agent.sec_companyfacts_index.SecCompanyFactsIndex`) with the
Stage-25 concept allowlist - the Phase-9.3 released map, extended by Stage 24 and
extended again by Stage 25 - into a SEPARATE Stage-25 database.

The Phase-9.5 index and the Stage-24 index are opened by nothing here and are
never written; Stage 25 does not mutate a prior stage's store. The CIK allowlist
is every ACTIVE, RESOLVED CIK in the owned historical identity layer - current
AND delisted - so the resulting facts are as survivorship-inclusive as the owned
identity bridge allows.

Restart-safe: the released indexer keeps a durable member cursor, so re-running
resumes. Idempotent: fact rows are UNIQUE per (cik, tag, unit, period, accession)
so amendments are preserved as distinct rows and a re-stream adds none.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alpha_agent import stage24_pit_fundamental as s24  # noqa: E402
from alpha_agent import stage25_alpha_discovery as s25  # noqa: E402
from alpha_agent.sec_companyfacts_index import SecCompanyFactsIndex  # noqa: E402


def resolved_ciks(identity_db: Path) -> list:
    conn = sqlite3.connect("file:%s?mode=ro" % identity_db, uri=True)
    try:
        rows = conn.execute(
            "select distinct cik from cik_map "
            "where active = 1 and status = 'RESOLVED' and cik is not null"
        ).fetchall()
    finally:
        conn.close()
    return sorted({str(r[0]).zfill(10) for r in rows if r[0]})


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--zip", default=str(s24.DEFAULT_COMPANYFACTS_ZIP))
    ap.add_argument("--identity-db", default=str(s24.DEFAULT_IDENTITY_DB))
    ap.add_argument("--out", default=str(s25.DEFAULT_CF_INDEX))
    ap.add_argument("--time-budget", type=float, default=5400.0)
    ap.add_argument("--member-step", type=int, default=4000)
    args = ap.parse_args(argv)

    zip_path = Path(args.zip)
    if not zip_path.exists():
        print("BLOCKED: companyfacts archive absent: %s" % zip_path)
        return 2

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    allow = resolved_ciks(Path(args.identity_db))
    tags = sorted(s25.target_tags())
    print("allowlist CIKs      : %d" % len(allow))
    print("target us-gaap tags : %d" % len(tags))
    print("mapping hash        : %s" % s25.mapping_version_hash())
    print("output              : %s" % out)

    idx = SecCompanyFactsIndex(out)
    started = time.monotonic()
    manifest = zip_path.parent / "companyfacts.manifest.json"
    ahash = None
    if manifest.exists():
        try:
            ahash = (json.loads(manifest.read_text(encoding="utf-8"))
                     or {}).get("sha256")
        except Exception:  # noqa: BLE001
            ahash = None

    res = None
    while True:
        res = idx.index_companyfacts_archive(
            zip_path, archive_hash=ahash, allowlist_ciks=allow,
            target_tags=tags, member_step=args.member_step,
            time_budget_seconds=min(900.0, float(args.time_budget)))
        if not res.get("ok"):
            print("BLOCKED: %s" % res.get("reason"))
            return 2
        print("  members %d/%d  materialized=%d  facts=%d  elapsed=%.1fs"
              % (res["members_done"], res["total_members"],
                 res["ciks_materialized"], res["facts_indexed"],
                 time.monotonic() - started), flush=True)
        if res.get("complete"):
            break
        if (time.monotonic() - started) > float(args.time_budget):
            print("TIME BUDGET REACHED at member %d - re-run to resume"
                  % res["members_done"])
            return 3

    print("STAGE25_PIT_INDEX_COMPLETE facts=%d ciks=%d elapsed=%.1fs"
          % (res["facts_indexed"], res["ciks_materialized"],
             time.monotonic() - started))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
