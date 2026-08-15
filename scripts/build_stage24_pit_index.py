"""Build the Stage-24 EXTENDED SEC companyfacts index, OFFLINE.

Streams the already-owned ``companyfacts.zip`` (no network, no provider call, no
quota) through the RELEASED indexer
(:class:`alpha_agent.sec_companyfacts_index.SecCompanyFactsIndex`) with the
Stage-24 extended concept allowlist, into a SEPARATE Stage-24 database.

The Phase-9.5 index at ``identity\\sec_companyfacts_index.sqlite`` is opened by
nothing here and is never written; Stage 24 does not mutate a prior stage's
store. The allowlist is every ACTIVE, RESOLVED CIK in the owned historical
identity layer - current AND delisted - so the resulting facts are as
survivorship-inclusive as the owned identity bridge allows.

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
    ap.add_argument("--out", default=str(s24.DEFAULT_CF_INDEX))
    ap.add_argument("--time-budget", type=float, default=5400.0)
    ap.add_argument("--member-step", type=int, default=2000)
    args = ap.parse_args(argv)

    zip_path = Path(args.zip)
    if not zip_path.exists():
        print("BLOCKED: companyfacts archive absent: %s" % zip_path)
        return 2

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    allow = resolved_ciks(Path(args.identity_db))
    tags = sorted(s24.target_tags())
    print("allowlist CIKs      : %d" % len(allow))
    print("target us-gaap tags : %d" % len(tags))
    print("mapping hash        : %s" % s24.mapping_version_hash())
    print("output              : %s" % out)

    idx = SecCompanyFactsIndex(out)
    started = time.monotonic()
    # The archive hash pins provenance; reuse the owned manifest when present.
    manifest = zip_path.with_suffix("").with_suffix("")
    manifest = zip_path.parent / "companyfacts.manifest.json"
    ahash = None
    if manifest.exists():
        try:
            ahash = (json.loads(manifest.read_text(encoding="utf-8"))
                     or {}).get("sha256")
        except Exception:  # noqa: BLE001
            ahash = None

    total_done = 0
    while True:
        res = idx.index_companyfacts_archive(
            zip_path, archive_hash=ahash, allowlist_ciks=allow,
            target_tags=tags, member_step=args.member_step,
            time_budget_seconds=min(600.0, float(args.time_budget)))
        if not res.get("ok"):
            print("BLOCKED: %s" % res.get("reason"))
            return 2
        print("  members %d/%d  materialized=%d  facts=%d  elapsed=%.1fs"
              % (res["members_done"], res["total_members"],
                 res["ciks_materialized"], res["facts_indexed"],
                 time.monotonic() - started))
        total_done = res["members_done"]
        if res.get("complete"):
            break
        if (time.monotonic() - started) > float(args.time_budget):
            print("TIME BUDGET REACHED at member %d - re-run to resume"
                  % total_done)
            return 3

    print("STAGE24_PIT_INDEX_COMPLETE facts=%d ciks=%d elapsed=%.1fs"
          % (res["facts_indexed"], res["ciks_materialized"],
             time.monotonic() - started))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
