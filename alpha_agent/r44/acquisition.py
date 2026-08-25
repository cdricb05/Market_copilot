"""alpha_agent.r44.acquisition - Engine 1C, 1D and 1E.

Three lanes that Release 43 closed with external blockers. Release 44
re-verifies each live - a blocker that is never re-probed becomes folklore -
and then does the one piece of new work each lane actually admits.

**1C, analyst expectations.** R43's verdict was CURRENT_CONSENSUS_ONLY. That
verdict is correct about what the endpoints SERVE, and it missed something
sitting inside the payload: EODHD's ``estimate_trend`` carries a backward
strip - ``epsTrendCurrent``, ``epsTrend7daysAgo``, ``30``, ``60``,
``90daysAgo``. That is a vendor's account of what the consensus USED to be,
which is exactly the object the estate has been unable to buy.

It is not usable on the vendor's word: a backward strip inside a current
snapshot is a reconstruction, and reconstructions get restated. But the
estate has been capturing its own PROSPECTIVE snapshots since 2026-07-31,
and two of them are exactly seven days apart. So the strip can be
RECONCILED: the consensus this estate recorded on the earlier date, against
what the vendor later claims the consensus was on that date. If they agree,
a blocked information family becomes testable at $0. If they disagree, the
family is closed for a reason stronger than "we could not reach it".

**1D, native credit.** Re-probed, and the owned OAS window re-measured -
R43 caught FRED narrowing the ICE BofA family to three years.

**1E, microstructure.** R43 could not model a maker fill without fabricating
one, and that is still true. So this lane asks a question that needs no fill
at all: the observed SPREAD is a first-class observable in the owned minute
archive, and what liquidity does around a scheduled release is measurable
directly.
"""
from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path

import numpy as np
import pandas as pd

from ..r43 import acquisition as R43AQ
from ..r43 import panels as P
from . import artifact_body, contract as C, data_dir, sha, write_artifact

CALCULATION_OWNER = "alpha_agent.r44.acquisition"

VINTAGE_ROOT = Path(
    r"D:\Stock_Prediction_app_data\alpha_agent\ingestion\vintages"
    r"\eodhd_analyst")

#: The vendor's backward strip and the lag each field claims, in days.
BACKWARD_STRIP = {"epsTrend7daysAgo": 7, "epsTrend30daysAgo": 30,
                  "epsTrend60daysAgo": 60, "epsTrend90daysAgo": 90}
RECONCILE_TOLERANCE = 0.005          # half a cent on an EPS estimate


# --------------------------------------------------------------------------- #
# 1C - analyst expectations
# --------------------------------------------------------------------------- #
def _snapshot_dates() -> list:
    if not VINTAGE_ROOT.is_dir():
        return []
    out = []
    for p in sorted(VINTAGE_ROOT.iterdir()):
        if p.is_dir():
            try:
                out.append(_dt.date.fromisoformat(p.name))
            except ValueError:
                continue
    return out


def _load_snapshot(day: _dt.date) -> dict:
    d = VINTAGE_ROOT / day.isoformat()
    if not d.is_dir():
        return {}
    out = {}
    for f in sorted(d.glob("*.json")):
        try:
            out[f.stem] = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
    return out


def vintage_ledger_state() -> dict:
    """How long the estate's own PIT revision history actually is."""
    days = _snapshot_dates()
    if not days:
        return {"state": "NOT_FOUND", "root": str(VINTAGE_ROOT)}
    per = {}
    for d in days:
        snap = _load_snapshot(d)
        per[d.isoformat()] = sorted(snap)
    tickers = sorted({t for v in per.values() for t in v})
    span = (days[-1] - days[0]).days
    boundary = VINTAGE_ROOT / "_prospective_boundary.json"
    bnd = json.loads(boundary.read_text(encoding="utf-8")) \
        if boundary.exists() else {}
    return {
        "state": "PRESENT",
        "root": str(VINTAGE_ROOT),
        "first_snapshot": days[0].isoformat(),
        "last_snapshot": days[-1].isoformat(),
        "n_snapshot_dates": len(days),
        "span_days": span,
        "tickers": tickers,
        "n_tickers": len(tickers),
        "tickers_per_snapshot": {k: len(v) for k, v in per.items()},
        "hard_pit_floor": bnd.get("hard_pit_floor"),
        "backfill_before_floor_allowed": bnd.get(
            "backfill_before_floor_allowed"),
        "is_a_reconstruction": False,
        "why_it_matters": "this is the only revision history in the estate "
                          "that was captured PROSPECTIVELY and therefore "
                          "cannot have been restated",
    }


def _trend_rows(snap: dict) -> dict:
    """(ticker, fiscal period date) -> the estimate_trend row."""
    out = {}
    for tk, body in snap.items():
        for row in (body.get("estimate_trend") or []):
            key = (tk, str(row.get("date")))
            out[key] = row
    return out


def _f(x):
    try:
        v = float(x)
        return v if np.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def reconcile_backward_strip() -> dict:
    """Does the vendor's backward strip match what we recorded at the time?

    For every pair of our own snapshots separated by one of the strip's
    declared lags, compare the LATER snapshot's ``epsTrend<lag>daysAgo``
    against the EARLIER snapshot's ``epsTrendCurrent`` for the same ticker
    and fiscal period. A faithful strip reproduces the earlier value.
    """
    days = _snapshot_dates()
    if len(days) < 2:
        return {"state": "INSUFFICIENT_SNAPSHOTS", "n": len(days)}
    snaps = {d: _trend_rows(_load_snapshot(d)) for d in days}

    comparisons, pairs_used = [], []
    for i, later in enumerate(days):
        for earlier in days[:i]:
            gap = (later - earlier).days
            for field, lag in BACKWARD_STRIP.items():
                # Allow one day of slack: the vendor stamps a business-day
                # lag and our capture time is not the vendor's.
                if abs(gap - lag) > 1:
                    continue
                pairs_used.append({"earlier": earlier.isoformat(),
                                   "later": later.isoformat(),
                                   "gap_days": gap, "field": field})
                for key, row_l in snaps[later].items():
                    row_e = snaps[earlier].get(key)
                    if row_e is None:
                        continue
                    claimed = _f(row_l.get(field))
                    recorded = _f(row_e.get("epsTrendCurrent"))
                    if claimed is None or recorded is None:
                        continue
                    comparisons.append({
                        "ticker": key[0], "period": key[1], "field": field,
                        "gap_days": gap,
                        "vendor_claims_was": claimed,
                        "we_recorded": recorded,
                        "abs_diff": abs(claimed - recorded),
                        "matches": abs(claimed - recorded)
                        <= RECONCILE_TOLERANCE})
    if not comparisons:
        return {"state": "NO_ALIGNED_PAIRS",
                "n_snapshots": len(days),
                "pairs_considered": pairs_used,
                "why": "no two prospective snapshots are separated by one of "
                       "the strip's declared lags (7/30/60/90 days)",
                "unblocks_when": "FUTURE_TIME_REQUIRED - the collector needs "
                                 "to run until a pair lands on a declared lag"}
    ok = [c for c in comparisons if c["matches"]]
    diffs = [c["abs_diff"] for c in comparisons]
    by_field = {}
    for f in {c["field"] for c in comparisons}:
        sub = [c for c in comparisons if c["field"] == f]
        by_field[f] = {"n": len(sub),
                       "match_rate": sum(c["matches"] for c in sub) / len(sub),
                       "median_abs_diff": float(np.median(
                           [c["abs_diff"] for c in sub]))}
    big = [c for c in comparisons if c["abs_diff"] > 0.05]
    return {
        "n_comparisons_by_field": by_field,
        "n_differences_above_5_cents": len(big),
        "competing_explanation":
            "part of any mismatch could be capture-time convention rather "
            "than restatement - our snapshot is stamped when the collector "
            "ran, the vendor's 'N days ago' is stamped on its own clock. "
            "That explains cent-level noise. It does not explain %d "
            "differences above five cents, the largest of which is %.4f EPS."
            % (len(big), float(np.max(diffs))),
        "state": "MEASURED",
        "calculation_owner": CALCULATION_OWNER,
        "n_comparisons": len(comparisons),
        "n_matching": len(ok),
        "match_rate": len(ok) / len(comparisons),
        "tolerance": RECONCILE_TOLERANCE,
        "median_abs_diff": float(np.median(diffs)),
        "p90_abs_diff": float(np.percentile(diffs, 90)),
        "max_abs_diff": float(np.max(diffs)),
        "pairs_used": pairs_used,
        "worst_ten": sorted(comparisons, key=lambda c: -c["abs_diff"])[:10],
        "verdict": ("VENDOR_BACKWARD_STRIP_IS_FAITHFUL"
                    if len(ok) / len(comparisons) >= 0.95
                    else "VENDOR_BACKWARD_STRIP_IS_RESTATED"),
        "what_a_faithful_strip_would_unlock":
            "90 days of consensus history per snapshot, verified against "
            "prospectively captured values - the first PIT-defensible "
            "revision history this estate has ever had",
        "what_a_restated_strip_means":
            "the family stays closed, and closed for a measured reason "
            "rather than for lack of access",
    }


def observed_revisions() -> dict:
    """Do the estimates we captured actually MOVE between our snapshots?"""
    days = _snapshot_dates()
    if len(days) < 2:
        return {"state": "INSUFFICIENT_SNAPSHOTS"}
    first, last = _trend_rows(_load_snapshot(days[0])), \
        _trend_rows(_load_snapshot(days[-1]))
    moved, same, rows = 0, 0, []
    for key, row_l in last.items():
        row_f = first.get(key)
        if row_f is None:
            continue
        a, b = _f(row_f.get("epsTrendCurrent")), _f(row_l.get("epsTrendCurrent"))
        if a is None or b is None:
            continue
        d = abs(b - a)
        if d > RECONCILE_TOLERANCE:
            moved += 1
            rows.append({"ticker": key[0], "period": key[1],
                         "first": a, "last": b, "change": b - a})
        else:
            same += 1
    n = moved + same
    return {
        "state": "MEASURED",
        "window": [days[0].isoformat(), days[-1].isoformat()],
        "span_days": (days[-1] - days[0]).days,
        "n_series": n,
        "n_revised": moved,
        "revision_rate": (moved / n) if n else None,
        "largest_moves": sorted(rows, key=lambda r: -abs(r["change"]))[:10],
        "interpretation": "if nothing moves over the window, the ledger is "
                          "not yet capturing revisions and the binding "
                          "constraint is TIME, not access",
    }


def operator_sample_request(*, write: bool = True) -> dict:
    """The vendor sample request, WRITTEN TO DISK AND NOT SENT.

    The contract sets MAY_SEND_VENDOR_EMAIL = False. This produces the exact
    text an operator can send, and stops.
    """
    universe = ["AAPL", "MON", "META", "HTZ", "CALM"]
    body = (
        "Subject: Historical analyst estimate vintage sample request "
        "(research evaluation)\n\n"
        "We are evaluating historical analyst expectation data for a "
        "quantitative research programme and would like to assess a sample "
        "before discussing licensing.\n\n"
        "Sample universe (chosen deliberately to include an acquired name, "
        "a bankruptcy and a small cap, so that inactive-security handling "
        "is visible):\n"
        + "".join("  - %s\n" % s for s in universe) +
        "\nFor each security we need, per estimate vintage:\n"
        "  - the vintage/as-of timestamp of the consensus itself\n"
        "  - the fiscal period the estimate refers to (and its end date)\n"
        "  - mean EPS estimate, high, low, standard deviation\n"
        "  - mean revenue estimate\n"
        "  - number of contributing analysts\n"
        "  - the actual reported value and its report date\n"
        "  - explicit handling of delisted / acquired / inactive securities\n"
        "  - a statement of whether vintages are ever restated\n\n"
        "History depth requested: as long as available; a minimum of ten "
        "years is needed for the study to be meaningful.\n\n"
        "We are not requesting a trial that requires payment details at "
        "this stage.\n")
    out = {
        "state": "PREPARED_NOT_SENT",
        "may_send_vendor_email": C.MAY_SEND_VENDOR_EMAIL,
        "vendors": ["Steele (Barcomb)", "Intrinio", "Zacks", "LSEG I/B/E/S"],
        "universe": universe,
        "body": body,
        "sha256": sha(body),
    }
    if write:
        p = data_dir("analyst") / "OPERATOR_SAMPLE_REQUEST.txt"
        p.write_text(body, encoding="utf-8")
        out["path"] = str(p)
    return out


def analyst_lane(lane: str = "E1C_ANALYST_REVISIONS") -> dict:
    live = R43AQ.probe_analyst_revisions()
    ledger = vintage_ledger_state()
    recon = reconcile_backward_strip()
    moved = observed_revisions()
    req = operator_sample_request()

    if recon.get("state") == "MEASURED" and \
            recon.get("verdict") == "VENDOR_BACKWARD_STRIP_IS_FAITHFUL":
        state, blocker = "EXECUTED", None
    elif recon.get("state") == "MEASURED":
        state, blocker = "EXECUTED", "PIT_INTEGRITY_FAILURE"
    else:
        state, blocker = "FUTURE_TIME_REQUIRED", "FUTURE_TIME_REQUIRED"
    return {
        "lane": lane, "state": state, "blocker": blocker,
        "calculation_owner": CALCULATION_OWNER,
        "question": C.LANES["E1C_ANALYST_REVISIONS"],
        "live_probe": live,
        "prospective_ledger": ledger,
        "vendor_backward_strip_reconciliation": recon,
        "observed_revisions": moved,
        "operator_sample_request": {k: v for k, v in req.items()
                                    if k != "body"},
        "no_current_snapshot_as_historical_vintage":
            C.NO_CURRENT_SNAPSHOT_AS_HISTORICAL_VINTAGE,
    }


# --------------------------------------------------------------------------- #
# 1D - native credit
# --------------------------------------------------------------------------- #
def credit_lane(lane: str = "E1D_NATIVE_CREDIT") -> dict:
    live = R43AQ.probe_native_credit()
    fred = P.fred_panel()
    rows = []
    for s in list(P.CREDIT_OAS) + list(P.CREDIT_TERM):
        if fred is None or s not in getattr(fred, "columns", []):
            rows.append({"series": s, "state": "ABSENT"})
            continue
        v = pd.to_numeric(fred[s], errors="coerce").dropna()
        if v.empty:
            rows.append({"series": s, "state": "EMPTY"})
            continue
        idx = pd.DatetimeIndex(v.index)
        rows.append({"series": s, "state": "PRESENT", "n": int(len(v)),
                     "first": str(idx[0])[:10], "last": str(idx[-1])[:10],
                     "years": round((idx[-1] - idx[0]).days / 365.25, 2)})
    present = [r for r in rows if r.get("state") == "PRESENT"]
    return {
        "lane": lane, "state": "LICENCE_REQUIRED",
        "blocker": "LICENCE_REQUIRED",
        "calculation_owner": CALCULATION_OWNER,
        "question": C.LANES["E1D_NATIVE_CREDIT"],
        "live_probe": live,
        "owned_oas_family": rows,
        "median_years_of_owned_oas": (
            float(np.median([r["years"] for r in present]))
            if present else None),
        "shortest_owned_series": (
            min(present, key=lambda r: r["years"]) if present else None),
        "native_instruments_still_licensed": ["CDX", "iTraxx", "single-name "
                                              "CDS", "corporate bond OAS at "
                                              "issue level"],
        "etf_proxy_status": "PROXY_ONLY - HYG/LQD is not the credit market, "
                            "and R43's label is inherited unchanged",
        "why_not_inferred_from_etfs":
            C.NO_CFD_PROXY_FOR_A_FUTURES_HYPOTHESIS,
    }


# --------------------------------------------------------------------------- #
# 1E - microstructure without a fabricated fill
# --------------------------------------------------------------------------- #
def microstructure_lane(lane: str = "E1E_MICROSTRUCTURE") -> dict:
    """What liquidity does around a scheduled release, measured directly.

    No fill is modelled and none is needed. The spread is quoted, observed
    and in the archive; its behaviour around a known timestamp is a fact
    about the market, not an assumption about our execution.
    """
    from . import intraday as ID
    live = R43AQ.probe_microstructure()
    stamps = ID.release_stamps()
    if stamps is None:
        return {"lane": lane, "state": "HISTORICAL_DATA_UNAVAILABLE",
                "live_probe": live}
    out = []
    for sym in ID.available_instruments():
        df = ID.load_bars(sym)
        if df is None or df.empty:
            continue
        px = pd.to_numeric(df["close"], errors="coerce")
        half = (pd.to_numeric(df["spread"], errors="coerce") / 2.0) / px * 1e4
        prof = {}
        for off in (-30, -10, -5, -1, 0, 1, 2, 5, 10, 30, 60, 120):
            vals = []
            for ts in stamps["stamp_utc"]:
                t = ID._bar_at(df, ts + pd.Timedelta(minutes=off))
                if t is not None:
                    v = half.get(t)
                    if v is not None and np.isfinite(v):
                        vals.append(float(v))
            if vals:
                prof[str(off)] = {"n": len(vals),
                                  "median_half_spread_bps":
                                      float(np.median(vals))}
        base = prof.get("-30", {}).get("median_half_spread_bps")
        peak = max((v["median_half_spread_bps"] for v in prof.values()),
                   default=None)
        at0 = prof.get("0", {}).get("median_half_spread_bps")
        out.append({
            "symbol": sym,
            "half_spread_profile_bps": prof,
            "baseline_t_minus_30": base,
            "at_release": at0,
            "peak": peak,
            "widening_ratio_at_release": (at0 / base) if base and at0 else None,
            "peak_widening_ratio": (peak / base) if base and peak else None,
        })
    return {
        "lane": lane, "state": "EXECUTED",
        "calculation_owner": CALCULATION_OWNER,
        "question": C.LANES["E1E_MICROSTRUCTURE"],
        "live_probe": live,
        "what_is_measured": "the OBSERVED quoted half-spread around a "
                            "scheduled macro release, minute by minute",
        "no_fill_is_modelled": True,
        "maker_execution_still_blocked": True,
        "maker_blocker": "HISTORICAL_DATA_UNAVAILABLE - queue position, fill "
                         "probability and adverse selection need full-depth "
                         "data the free archives do not carry; R43's finding "
                         "is re-verified, not re-opened",
        "instruments": out,
    }


# --------------------------------------------------------------------------- #
# Runner
# --------------------------------------------------------------------------- #
def run() -> dict:
    body = artifact_body("r44_orthogonal_data_frontier/1", {
        "calculation_owner": CALCULATION_OWNER,
        "generated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "analyst": analyst_lane(),
        "credit": credit_lane(),
        "microstructure": microstructure_lane(),
    })
    body["frontier_hash"] = sha({k: v for k, v in body.items()
                                 if k != "safety_block"})
    write_artifact("R44_ORTHOGONAL_DATA_FRONTIER.json", body, overwrite=True)
    return body
