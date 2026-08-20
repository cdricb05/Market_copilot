"""scripts/run_release31_campaign.py - drive the Release 31 campaign (v3).

RESEARCH ONLY. Writes exclusively under the Release-31 research root and touches
no operational store, no proposal, no decision and no order.

    $py = 'C:\\Users\\binis\\paper_trader\\.venv-win\\Scripts\\python.exe'
    & $py scripts\\run_release31_campaign.py --stage contracts
    & $py scripts\\run_release31_campaign.py --stage known   --workers 4
    & $py scripts\\run_release31_campaign.py --stage novel   --workers 4
    & $py scripts\\run_release31_campaign.py --stage lockbox --workers 4
    & $py scripts\\run_release31_campaign.py --stage verdict
    & $py scripts\\run_release31_campaign.py --stage all     --workers 4

Every stage is resumable: an immutable artifact that already exists is re-read,
and a candidate whose specification hash is already in the registry is not
refitted. ``--workers`` fans candidates across processes; the parent process
remains the only writer to the append-only candidate log, so parallelism can
never corrupt the multiple-testing denominator.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from paper_trader.alpha_agent import r31                       # noqa: E402
from paper_trader.alpha_agent.r31 import campaign as _campaign  # noqa: E402
from paper_trader.alpha_agent.r31 import contract as _contract  # noqa: E402
from paper_trader.alpha_agent.r31 import lockbox as _lockbox    # noqa: E402

STAGES = ("contracts", "known", "novel", "lockbox", "verdict", "all")


def _now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


def _created_at(campaign_id: str) -> str:
    """The contract's creation timestamp is stable once frozen.

    Re-deriving it from the clock on a resumed run would change the contract
    hash, which is exactly the drift the registry refuses.
    """
    existing = _contract.load(campaign_id)
    if existing and existing.get("created_at"):
        return str(existing["created_at"])
    return _now()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Release 31 Mathematical Alpha Frontier")
    ap.add_argument("--stage", choices=STAGES, default="all")
    ap.add_argument("--campaign-id", default=_contract.CAMPAIGN_ID)
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--force-rebuild-snapshot", action="store_true")
    args = ap.parse_args(argv)

    cid = args.campaign_id
    w = max(1, int(args.workers))
    t0 = time.time()
    print("Release 31 - Mathematical Alpha Frontier - CAMPAIGN V3", flush=True)
    print("campaign:", cid, flush=True)
    print("root    :", r31.campaign_dir(cid), flush=True)
    print("workers :", w, flush=True)
    print(flush=True)

    print("[1-6] freezing contracts ...", flush=True)
    ctx = _campaign.freeze_contracts(
        campaign_id=cid, created_at=_created_at(cid),
        force_rebuild=args.force_rebuild_snapshot, log=_log)

    umani = ctx.universe_manifest or {}
    inv = umani.get("investment_universe") or {}
    surv = umani.get("survivorship") or {}
    bmani = ctx.benchmark_manifest or {}
    manifest = r31.read_json(
        r31.campaign_dir(cid) / "data_snapshot_manifest.json") or {}

    print("  snapshot     %s  %d cross-sections"
          % (ctx.snap.content_hash[:12], manifest.get("cross_sections_total", 0)))
    print("  universe     %s  median %s members/session  missing %.3f%%  %s"
          % (str(umani.get("universe_hash"))[:12],
             inv.get("median_members_per_session"),
             100.0 * float(surv.get("missing_fraction") or 0.0),
             surv.get("verdict")))
    print("  benchmarks   %s  investable=%s (%s)"
          % (str(bmani.get("benchmark_hash"))[:12],
             (bmani.get("investable") or {}).get("source_symbol"),
             (bmani.get("investable") or {}).get("state")))
    print("  covariance   %s  %d sections cached"
          % (ctx.cov.key[:12], int(ctx.cov.sections.size)))
    for s, blk in sorted(ctx.partition["samples"].items()):
        p = blk["horizons"][str(_campaign.PRIMARY_HORIZON)]
        c = p["counts"]
        print("  partition    %-46s disc=%3d val=%3d lock=%3d  %s"
              % (s, c["discovery"], c["validation"], c["lockbox"], p["state"]))
    con = _contract.load(cid) or {}
    print("  contract     %s" % str(con.get("contract_hash"))[:12])

    known = novel_out = lockbox_out = None
    if args.stage in ("known", "all"):
        print("\n[7] known-method tournament ...", flush=True)
        known = _campaign.run_known_methods(ctx, campaign_id=cid, workers=w,
                                            log=_log)
        print("  executed %d configs across %d families"
              % (known["executed_configs"], len(known["families_executed"])))
        _print_board(known.get("leaderboard"), "known-method leaderboard")
        _print_board(known.get("benchmarks"), "benchmarks")

    if args.stage in ("novel", "all"):
        print("\n[8] bounded novel discovery ...", flush=True)
        known = known or _read(cid, _campaign.KNOWN_RESULTS_ARTIFACT)
        novel_out = _campaign.run_novel(ctx, campaign_id=cid, workers=w,
                                        log=_log)
        print("  executed %d candidates across %d families"
              % (novel_out["candidates_executed"],
                 len(novel_out["families_executed"])))
        _print_board(novel_out.get("leaderboard"), "novel leaderboard")

    if args.stage in ("lockbox", "all"):
        print("\n[9] lockbox ...", flush=True)
        known = known or _read(cid, _campaign.KNOWN_RESULTS_ARTIFACT)
        novel_out = novel_out or _read(cid, "novel_discovery_results.json")
        lockbox_out = _campaign.run_lockbox(ctx, campaign_id=cid, at=_now(),
                                            workers=w, log=_log)
        print("  lockbox accesses: %d / %d"
              % (lockbox_out["access_count"], _contract.MAX_LOCKBOX_CANDIDATES))

    if args.stage in ("verdict", "all"):
        print("\n[10] campaign-wide multiple testing ...", flush=True)
        known = known or _read(cid, _campaign.KNOWN_RESULTS_ARTIFACT)
        novel_out = novel_out or _read(cid, "novel_discovery_results.json")
        lockbox_out = lockbox_out or _rehydrate_lockbox(cid)
        mt_out = _campaign.run_multiple_testing(campaign_id=cid,
                                                lockbox_out=lockbox_out)
        print("  denominator %d  BH rejected %d/%d  SPA p=%s"
              % (mt_out["denominator_executed_candidates"],
                 mt_out["benjamini_hochberg"]["n_rejected"],
                 mt_out["benjamini_hochberg"]["m"],
                 mt_out["superior_predictive_ability"].get("p_value")))
        frontier = _campaign.build_frontier(campaign_id=cid,
                                            lockbox_out=lockbox_out)
        print("\n[11] terminal verdict ...", flush=True)
        verdict = _campaign.final_verdict(
            campaign_id=cid, ctx=ctx, known=known, novel_out=novel_out,
            lockbox_out=lockbox_out, mt_out=mt_out, frontier=frontier,
            at=_now())
        print()
        for k, c in sorted(verdict["superiority"].get("checks", {}).items()):
            print("  %-38s value=%-14s pass=%s"
                  % (k, _fmt(c.get("value")), c["pass"]))
        print()
        print("  PRIMARY VERDICT :", verdict["primary_verdict"])
        if verdict.get("secondary_verdict"):
            print("  SECONDARY       :", verdict["secondary_verdict"])

    print("\ndone in %.1fs" % (time.time() - t0), flush=True)
    return 0


def _log(msg) -> None:
    print(msg, flush=True)


def _rehydrate_lockbox(cid: str) -> dict:
    res = r31.read_json(_lockbox.results_path(cid)) or {}
    return {"finalists": r31.read_json(_lockbox.finalists_path(cid)) or {},
            "results": res.get("results", []),
            "access_count": _lockbox.access_count(cid)}


def _read(cid: str, name: str) -> dict:
    return r31.read_json(r31.campaign_dir(cid) / name) or {}


def _fmt(v) -> str:
    if v is None:
        return "None"
    try:
        return "%.5f" % float(v)
    except (TypeError, ValueError):
        return str(v)


def _print_board(rows, title: str, n: int = 15) -> None:
    if not rows:
        return
    print("\n  %s" % title)
    print("  %-34s %-20s %9s %9s %8s %7s %7s"
          % ("candidate", "family", "net_exc", "vs_spy", "sharpe", "cash", "names"))
    for r in rows[:n]:
        print("  %-34s %-20s %9s %9s %8s %7s %7s"
              % (r["candidate_id"][:34], str(r["family"])[:20],
                 _fmt(r.get("net_excess_annualised")),
                 _fmt(r.get("net_excess_vs_spy_annualised")),
                 _fmt(r.get("sharpe_net")),
                 _fmt(r.get("cash_weight_mean")),
                 _fmt(r.get("names_held_mean"))))


if __name__ == "__main__":
    raise SystemExit(main())
