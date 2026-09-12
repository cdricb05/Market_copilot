r"""scripts/run_reversed_skew_state_catchup.py - bring the frozen challenger's
TRAILING OPTION-SURFACE STATE up to the last session the venue serves.

WHY THIS IS STATE INITIALISATION AND NOT EVIDENCE
    ``REVERSED_SPY_PUT_CALL_SKEW_H5`` is scored on a z-score of PUT_CALL_SKEW
    against the 60 STRICTLY PRIOR observations. The moneyness-anchored surface
    the campaign owns ends 2026-08-20, so a decision taken on 2026-09-14 would
    compare today's skew against a window that ended sixteen sessions earlier.
    That is not the frozen construction; it is a different, staler rule wearing
    the same name.

    So the missing sessions are acquired. They are NOT a new test, NOT
    qualification evidence, NOT a holdout and NOT permission to tune: no gate is
    evaluated here, no arm is scored, no parameter is chosen and no artifact
    this script writes can classify the challenger. It fills in the trailing
    state the frozen rule was always defined to read.

THE SPENDING CONTRACT IS THE EXISTING ONE
    Every request is priced with ``metadata.get_cost`` BEFORE anything is
    downloaded, by ``alpha_agent.alpha_recovery.databento_acquisition``, which
    refuses any signature its own plan did not price. This script adds no
    provider, no endpoint and no bypass - it only names the window.

    ``--execute`` is required to spend anything. Without it the whole catch-up
    is priced and printed and NOTHING is downloaded.

RESEARCH ONLY. No order, no fill, no promotion, no registration, no capital.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alpha_agent.alpha_recovery import assert_research_root_is_not_live  # noqa: E402
from alpha_agent.alpha_recovery import databento_acquisition as DA  # noqa: E402
from alpha_agent.alpha_recovery import options_acquisition as OA  # noqa: E402
from alpha_agent.alpha_recovery import options_surface as OS  # noqa: E402

#: The first session the owned surface is missing, and the expiries that must be
#: live for those sessions to yield a row. Both are DERIVED from the frozen
#: construction (``LOOKBACK_DAYS`` before each third-Friday expiry), never chosen.
GAP_START = "2026-08-21"
#: Expiries are in scope up to here; each is BOUGHT only as far as the venue
#: serves. Three monthlies bracket the gap, which is what the term slope needs.
EXPIRY_SCOPE_END = "2026-11-20"

#: The ONE hourly spot series the corrected surface is dated and marked against.
#:
#: It is re-acquired COMPLETE into its own directory rather than appended to the
#: confirmation window's copy. ``acquire_spot`` reuses any file that exists
#: without checking what it covers, so appending is not something it can do -
#: and the existing copy stops at 2026-09-09, which would silently drop the last
#: two sessions of the catch-up. Writing a complete series beside it leaves the
#: file the confirmation result was computed on exactly where it is.
SPOT_TAG = OA.SPOT_CORRECTED_TAG
SPOT_DATASET = OA.SPOT_DATASET_PRE_2024
SPOT_SCHEMA = OA.SPOT_SCHEMA_INTRADAY
SPOT_START = "2022-09-09"

#: The surface the frozen rule actually reads.
OUT_TAG = OA.SPOT_CORRECTED_TAG


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--budget", type=float, default=0.25,
                    help="free-credit ceiling the operator authorises (USD)")
    ap.add_argument("--execute", action="store_true",
                    help="actually download; without it nothing is spent")
    ap.add_argument("--rebuild", action="store_true",
                    help="rebuild the surface from the bands on disk")
    args = ap.parse_args()

    assert_research_root_is_not_live()
    client = DA.Client()

    # Databento's ``end`` is EXCLUSIVE. ``_available_end`` additionally steps a
    # day back for safety, so using it as a request end loses TWO sessions - and
    # that is exactly why the owned surface stops at 2026-08-20 although its
    # last band was requested to 2026-08-21. The request end is therefore the
    # venue's own exclusive upper bound, and the session it actually covers is
    # the day before it.
    rng = client.dataset_range(OA.DATASET)
    request_end = str(((rng.get("schema") or {}).get(OA.SCHEMA) or rng)
                      .get("end") or rng.get("end"))[:10]
    avail_end = request_end          # exclusive upper bound used for requests
    covered_through = DA._available_end(rng, OA.SCHEMA)
    print("venue exclusive end       : %s" % request_end)
    print("last COVERED session      : %s" % covered_through)
    print("owned surface ends        : %s" % _surface_end())
    print("gap to fill               : %s .. %s" % (GAP_START, covered_through))

    # ---- price the option bands (free) ----------------------------------- #
    plan = OA.plan(client, budget_usd=args.budget,
                   window=(GAP_START, EXPIRY_SCOPE_END),
                   available_end=avail_end)
    # BUY ONLY WHAT THE FROZEN FEATURE READS. ``skew`` - the only field this
    # challenger scores - is computed on the NEAR expiry alone, and an expiry
    # qualifies as near only at ``MIN_T_YEARS_NEAR`` or more to expiry. So the
    # expiries that must be owned are exactly those that serve as the near leg
    # on some gap session; a later one would only ever be the FAR leg, which
    # this challenger never reads. The set is DERIVED from the frozen constant,
    # not chosen, and narrowing a priced plan can only reduce spend.
    needed = _near_leg_expiries()
    dropped = [r for r in plan["requests"] if r["expiry"] not in needed]
    plan["requests"] = [r for r in plan["requests"] if r["expiry"] in needed]
    band_usd = round(sum(r["cost_usd"] for r in plan["requests"]), 6)
    plan["selection"]["estimated_spend_usd"] = band_usd
    plan["selection"]["fits_in_free_credit"] = (
        band_usd <= args.budget * (1.0 - DA.BUDGET_SAFETY_MARGIN))
    print()
    print("OPTION BANDS (near-leg expiries only)")
    for r in plan["requests"]:
        print("   %-16s %s .. %s  %3d symbols  $%.6f"
              % (r["label"], r["start"], r["end"], r["n_symbols"],
                 r["cost_usd"]))
    for r in dropped:
        print("   %-16s NOT BOUGHT ($%.6f) - far leg only; the frozen feature "
              "reads the near expiry" % (r["label"], r["cost_usd"]))
    print("   band subtotal          : $%.6f" % band_usd)

    # ---- price the spot leg (free) --------------------------------------- #
    spot = OA.acquire_spot(client, SPOT_START, request_end, execute=False,
                           tag=SPOT_TAG, dataset=SPOT_DATASET,
                           schema=SPOT_SCHEMA)
    spot_usd = 0.0 if spot.get("state") == "REUSED" else float(
        spot.get("cost_usd") or 0.0)
    print()
    print("SPOT  %s / %s  %s .. %s  state=%s  $%.6f"
          % (SPOT_DATASET, SPOT_SCHEMA, SPOT_START, avail_end,
             spot.get("state"), float(spot.get("cost_usd") or 0.0)))
    if spot.get("state") == "REUSED":
        # A reused file is only usable if it actually REACHES the catch-up. The
        # reuse check is existence, not coverage, so the coverage is checked
        # here rather than discovered as missing rows in the built surface. A
        # short cache is a provider cache, not evidence, so it is replaced.
        have = OA.session_closes(tag=SPOT_TAG, dataset=SPOT_DATASET,
                                 schema=SPOT_SCHEMA)
        last = max(have) if have else None
        print("      reused file covers through %s (need %s)"
              % (last, covered_through))
        if last is None or last < covered_through:
            stale = OA.spot_path(SPOT_TAG, SPOT_DATASET, SPOT_SCHEMA)
            if not args.execute:
                print("      the cached spot series is short; --execute would "
                      "replace it")
            else:
                stale.unlink()
                spot = OA.acquire_spot(client, SPOT_START, request_end,
                                       execute=False, tag=SPOT_TAG,
                                       dataset=SPOT_DATASET,
                                       schema=SPOT_SCHEMA)
                spot_usd = float(spot.get("cost_usd") or 0.0)
                print("      replacing it: $%.6f" % spot_usd)

    total = round(band_usd + spot_usd, 6)
    cap = args.budget * (1.0 - DA.BUDGET_SAFETY_MARGIN)
    print()
    print("TOTAL PRICED              : $%.6f" % total)
    print("authorised ceiling        : $%.4f (effective cap $%.4f)"
          % (args.budget, cap))
    print("fits                      : %s" % (total <= cap))
    print("paid dollars              : 0.00  (free credit only)")

    if total > cap:
        print()
        print("REFUSED: the catch-up does not fit inside the authorised free "
              "credit. Nothing was downloaded.")
        return 2
    if not args.execute:
        print()
        print("PRICED_NOT_DOWNLOADED - re-run with --execute to acquire.")
        return 0

    # ---- spend ------------------------------------------------------------ #
    got = DA.download(client, plan, out_root=OA.data_root(None), dry_run=False,
                      schema=OA.SCHEMA, dataset=OA.DATASET)
    print()
    print("bands: %s" % got.get("state"))
    for w in got.get("written", []):
        print("   %-16s reused=%-5s %s" % (w.get("symbol"), w.get("reused"),
                                           w.get("path")))
    if got.get("failed"):
        print("   FAILED: %s" % got["failed"])

    if spot.get("state") != "REUSED":
        s = OA.acquire_spot(client, SPOT_START, request_end, execute=True,
                            tag=SPOT_TAG, dataset=SPOT_DATASET,
                            schema=SPOT_SCHEMA)
        print("spot: %s -> %s" % (s.get("state"), s.get("path")))
        have = OA.session_closes(tag=SPOT_TAG, dataset=SPOT_DATASET,
                                 schema=SPOT_SCHEMA)
        print("      covers through %s" % (max(have) if have else None))

    print()
    print("SPENT (estimate)          : $%.6f" % (
        float(got.get("spent_estimate_usd") or 0.0) + spot_usd))

    if args.rebuild:
        print()
        print("building the CATCH-UP surface (gap dates only) ...")
        out = OA.build_surface(client, window=(GAP_START, EXPIRY_SCOPE_END),
                               available_end=avail_end, tag=None,
                               spot_tag=SPOT_TAG, spot_dataset=SPOT_DATASET,
                               spot_schema=SPOT_SCHEMA, out_tag=CATCHUP_TAG)
        print("   %s" % {k: v for k, v in out.items()
                         if k in ("path", "state", "skipped")})
        if out.get("state") == "NO_ROWS":
            return 4
        print()
        print("appending it to the frozen surface ...")
        print("   %s" % extend_surface())
    return 0


#: The gap-only build. It is kept as its own file because the DISCOVERY surface
#: may never be rebuilt: adding the September expiry's band to a re-run over the
#: whole history would put a new near/far expiry into rows that already exist
#: and silently change the sample the sign was discovered on. So the frozen
#: rows are never recomputed - only appended to.
CATCHUP_TAG = "catchup_20260911"


def extend_surface() -> dict:
    """Append the catch-up rows to the frozen surface. Never rewrites a row.

    The existing file is archived first, under a name that says what it covers,
    because it is the artifact the independent historical confirmation was
    computed on. On a key collision the ORIGINAL row wins, so this operation
    cannot alter a single date the discovery sample already held.
    """
    import pandas as pd

    frozen_p = OA.surface_path(OUT_TAG)
    catch_p = OA.surface_path(CATCHUP_TAG)
    frozen = pd.read_csv(frozen_p)
    catch = pd.read_csv(catch_p)
    before = sorted(frozen["date"].astype(str).unique())
    new_dates = sorted(set(catch["date"].astype(str)) - set(before))

    archive = frozen_p.with_name(
        frozen_p.name.replace(".csv.gz", "_through_%s.csv.gz"
                              % before[-1].replace("-", "")))
    if not archive.exists():
        archive.write_bytes(frozen_p.read_bytes())

    key = ["date", "expiration", "type", "strike"]
    combined = (pd.concat([frozen, catch], ignore_index=True)
                  .drop_duplicates(subset=key, keep="first")
                  .sort_values(key).reset_index(drop=True))
    combined.to_csv(frozen_p, index=False, compression="gzip")
    after = sorted(combined["date"].astype(str).unique())
    return {"archived_original": str(archive),
            "frozen_dates_before": len(before),
            "dates_after": len(after),
            "new_dates_appended": len(new_dates),
            "first": after[0], "last": after[-1],
            "discovery_rows_unchanged": len(frozen) == int(
                (combined["date"].astype(str) <= before[-1]).sum())}


def _near_leg_expiries() -> set:
    """The expiries that serve as the NEAR leg on some session in the gap.

    Derived from ``options_surface.MIN_T_YEARS_NEAR`` - the frozen floor an
    expiry must clear to be treated as near-dated - applied to the sessions the
    owned surface actually holds from the gap onward. No expiry list is typed
    in and no threshold is invented here.
    """
    from datetime import date as _date

    f = OS.features(rebuild=True)
    sessions = [str(x)[:10] for x in f["date"] if str(x)[:10] >= GAP_START]
    cands = sorted({OA.third_friday(y, m)
                    for y in (2026, 2027) for m in range(1, 13)})
    out = set()
    for s in sessions:
        d = _date.fromisoformat(s)
        near = next((e for e in cands
                     if (e - d).days / 365.25 >= OS.MIN_T_YEARS_NEAR), None)
        if near is not None:
            out.add(near.isoformat())
    return out


def _surface_end():
    p = OS.surface_path()
    if not p.exists():
        return None
    f = OS.features(surface=p)
    return str(f["date"].iloc[-1])[:10] if len(f) else None


if __name__ == "__main__":
    raise SystemExit(main())
