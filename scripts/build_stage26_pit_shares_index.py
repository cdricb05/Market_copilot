"""scripts/build_stage26_pit_shares_index.py — Stage 26 PIT SHARE-COUNT index.

Streams the already-owned ``companyfacts.zip`` (no network, no provider call, no
quota) through the RELEASED indexer with the share-count concepts and the
explicit ``extra_units={'shares'}`` / ``extra_taxonomies={'dei'}`` widening, into
a SEPARATE Stage-26 database.

This closes the first of the two gaps Stage 25 identified as blocking
point-in-time market cap: the released monetary-USD unit filter silently dropped
every share count. The widening is opt-in per call, so the Phase-9.5, Stage-24
and Stage-25 indexes keep byte-identical semantics and are never opened or
written here.

    .venv-win\\Scripts\\python.exe scripts\\build_stage26_pit_shares_index.py
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alpha_agent import pit_market_equity as pme  # noqa: E402
from alpha_agent import stage24_pit_fundamental as s24  # noqa: E402
from alpha_agent.sec_companyfacts_index import (  # noqa: E402
    DEI_TAXONOMY, SHARE_UNITS, SecCompanyFactsIndex,
)

DEFAULT_OUT = (r"D:\Stock_Prediction_app_data\stage26_alpha_challenger_expansion"
               r"\_index\sec_companyfacts_shares_stage26.sqlite")


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
    ap.add_argument("--out", default=DEFAULT_OUT)
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
    tags = sorted(pme.SHARE_TAGS)
    print("allowlist CIKs : %d" % len(allow))
    print("share concepts : %s" % ", ".join(tags))
    print("extra units    : %s" % ", ".join(sorted(SHARE_UNITS)))
    print("extra taxonomy : %s" % DEI_TAXONOMY)
    print("output         : %s" % out)

    manifest = zip_path.parent / "companyfacts.manifest.json"
    ahash = None
    if manifest.exists():
        try:
            ahash = (json.loads(manifest.read_text(encoding="utf-8"))
                     or {}).get("sha256")
        except Exception:  # noqa: BLE001
            ahash = None

    idx = SecCompanyFactsIndex(out)
    started = time.monotonic()
    while True:
        res = idx.index_companyfacts_archive(
            zip_path, archive_hash=ahash, allowlist_ciks=allow,
            target_tags=tags, member_step=args.member_step,
            extra_units=SHARE_UNITS, extra_taxonomies=(DEI_TAXONOMY,),
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

    conn = sqlite3.connect("file:%s?mode=ro" % out, uri=True)
    try:
        by_concept = dict(conn.execute(
            "select concept_tag, count(*) from cf_fact group by concept_tag"))
        span = conn.execute(
            "select min(filed), max(filed), count(distinct cik) from cf_fact"
        ).fetchone()
    finally:
        conn.close()
    print("facts by concept: %s" % json.dumps(by_concept, sort_keys=True))
    print("filed span: %s -> %s across %d CIKs" % span)
    print("STAGE26_SHARES_INDEX_COMPLETE facts=%d elapsed=%.1fs"
          % (res["facts_indexed"], time.monotonic() - started))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
