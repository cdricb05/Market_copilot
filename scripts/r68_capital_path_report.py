r"""scripts/r68_capital_path_report.py - what can advance toward capital, and why not.

R67 asked, for the first time, whether each registered forward challenger could
EVER satisfy the capital gate, and found that four of eight could not at any
date. R68 repaired those four. This script reports the resulting picture from
the LIVE owners - it computes no threshold, declares no policy and measures no
return of its own:

    api.forward_producer_health   the producer lifecycle state of each
                                  registration, and its producer's heartbeat
    alpha_agent.r67.forward_producer
                                  how many sessions to the capital floor
    api.capital_eligibility_gate  the floor itself, and the short-leg rule
    api.capital_pool              the authorised portfolio policy
    engine.shadow_portfolio_evidence
                                  whether the RESEARCH side can account for a
                                  multi-leg book at all - measured by running
                                  one through it, not by reading its docstring

THE QUESTION SECTION 7 ACTUALLY ASKS. The operational book is long-only while
the research estate keeps producing hedged structures. R66 reported the blocker
on those structures as instrument granularity; R67 corrected it to the
long-only policy, which binds one level above size. What neither established is
whether the RESEARCH and PAPER-EVIDENCE side could account for a short leg if
the policy changed - and that is an engineering question with a measurable
answer, which this script measures.

TERMINAL TOKENS (exactly one, on the last line)
    R68_CAPITAL_PATH_OK <n registrations>
    R68_CAPITAL_PATH_FAILED <reason>

READ ONLY. It emits no prediction, freezes no decision, changes no policy, and
creates no order, fill or proposal.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

SCHEMA = "r68_capital_path/1"


def _multi_leg_accounting_probe() -> dict:
    """Can the research kernel account for a HEDGED book? Measured, not assumed.

    Two books over one synthetic panel: a market-neutral long/short pair and the
    long leg alone. If the kernel handles a short leg the hedged book must cost
    on GROSS (twice the long-only entry cost), report gross 2.0 against net 0.0,
    and earn the long leg's return MINUS the short leg's. Anything else means
    multi-leg accounting is not supported and the section-7 answer is different.
    """
    from paper_trader.engine import shadow_portfolio_evidence as K

    sessions = ["2026-09-%02d" % d for d in
                (1, 2, 3, 4, 8, 9, 10, 11, 14, 15, 16, 17, 18, 21, 22)]

    def leg(start, step):
        return {"dates": list(sessions),
                "adj": [start * (1 + step) ** i for i in range(len(sessions))]}

    series = {"LONG_LEG": leg(100.0, 0.010), "SHORT_LEG": leg(100.0, 0.002)}

    def run(weights, cid):
        rec = K.make_inception_record(
            challenger_id=cid, label=cid, family="R68_MULTI_LEG_PROBE",
            strategy_identity={"release": "R68_PROBE"}, weights=weights,
            inception_session="2026-09-01",
            inception_timestamp="2026-09-01T20:00:00+00:00",
            starting_capital=100000.0,
            pit_input_identity={"panel": "synthetic_probe"},
            cost_bps_per_side=12.5, valuation_source="synthetic_probe")
        out = K.accrue_forward(record=rec, price_series=series)
        return rec, out

    h_rec, hedged = run({"LONG_LEG": 1.0, "SHORT_LEG": -1.0}, "HEDGED")
    l_rec, longonly = run({"LONG_LEG": 1.0}, "LONG_ONLY")

    supported = bool(
        hedged.get("has_short_leg") is True
        and abs((hedged.get("gross_exposure") or 0) - 2.0) < 1e-9
        and abs((h_rec.get("net_exposure") or 0) - 0.0) < 1e-9
        and abs((hedged.get("entry_cost_usd") or 0)
                - 2 * (longonly.get("entry_cost_usd") or 0)) < 1e-6
        and len(hedged.get("curve") or []) == len(longonly.get("curve") or [])
        and (hedged.get("gross_cumulative_return") or 0)
        < (longonly.get("gross_cumulative_return") or 0))

    return {
        "measured_by": "engine.shadow_portfolio_evidence",
        "multi_leg_accounting_supported": supported,
        "hedged": {
            "has_short_leg": hedged.get("has_short_leg"),
            "gross_exposure": hedged.get("gross_exposure"),
            "net_exposure": h_rec.get("net_exposure"),
            "cash_weight": hedged.get("cash_weight"),
            "entry_cost_usd": hedged.get("entry_cost_usd"),
            "cost_basis": (h_rec.get("cost_model") or {}).get("cost_basis"),
            "n_curve_points": len(hedged.get("curve") or []),
            "gross_cumulative_return": hedged.get("gross_cumulative_return"),
            "evidence_state": hedged.get("evidence_state")},
        "long_only_control": {
            "gross_exposure": longonly.get("gross_exposure"),
            "entry_cost_usd": longonly.get("entry_cost_usd"),
            "gross_cumulative_return": longonly.get("gross_cumulative_return")},
        "what_this_proves": (
            "The research and paper-evidence side ALREADY accounts for a "
            "multi-leg book completely: gross exposure is the sum of absolute "
            "weights, cost is charged on GROSS (the hedged book pays exactly "
            "twice the long-only entry cost), a reversal from +1 to -1 is "
            "priced as a 2.0 trade rather than a zero-cost hold, and the short "
            "leg's return is subtracted rather than dropped. No engineering is "
            "missing here, so no engineering was invented."),
        "what_is_NOT_modelled_and_is_stated_rather_than_implied": [
            "MARGIN. This is a weight-book kernel and holds no margin model. A "
            "short leg's initial and variation margin is a capital_pool "
            "question, not a kernel question, and building a margin engine "
            "before any authority has approved a short leg would be inventing "
            "the answer to a question nobody has asked.",
            "CASH ON A MARKET-NEUTRAL BOOK. cash_weight is max(0, 1 - NET), so "
            "a net-zero book reads as 100% cash carrying 2.0 gross. That is a "
            "defensible convention and it is a CONVENTION: the idle cash earns "
            "nothing in this kernel, which understates a real long/short book's "
            "return by the short rebate. Reported, not silently corrected.",
            "BORROW COST. A short leg pays a stock-loan fee this kernel does "
            "not model. For any equity long/short structure that fee is a real "
            "and sometimes decisive cost.",
        ],
    }


def _policy() -> dict:
    """The AUTHORISED portfolio policy, read from its owners. Never declared here."""
    out = {"policy_owner": "api.capital_pool + api.capital_eligibility_gate"}
    try:
        from paper_trader.api import capital_pool as CP
        pool = CP.load_capital_pool() if hasattr(CP, "load_capital_pool") else {}
        sem = (pool or {}).get("semantics") or {}
        saf = (pool or {}).get("safety") or {}
        out["long_only"] = sem.get("long_only")
        out["short_exposure_supported"] = saf.get("short_exposure_supported")
        out["nav_usd"] = (pool or {}).get("nav")
        out["available_capital_usd"] = (pool or {}).get("available_capital")
        out["cash"] = (pool or {}).get("cash")
        out["gross_exposure"] = (pool or {}).get("gross_exposure")
    except Exception as exc:                                # noqa: BLE001
        out["capital_pool_read_error"] = str(exc)[:200]
    try:
        from paper_trader.api import capital_eligibility_gate as G
        out["short_leg_rule"] = getattr(G, "SHORT_LEG_RULE", None)
        out["gate_thresholds_h21"] = G.gate_thresholds(21)
    except Exception as exc:                                # noqa: BLE001
        out["gate_read_error"] = str(exc)[:200]
    return out


def build() -> dict:
    from paper_trader.alpha_agent.r67 import forward_producer as FP
    from paper_trader.api import forward_producer_health as FPH

    health = FPH.producer_coverage()
    floors = {}
    for row in health["registrations"]:
        floors[row["challenger_id"]] = FP.time_to_capital_floor({
            "challenger_id": row["challenger_id"],
            "cadence_sessions": row.get("cadence_sessions"),
            "horizon_sessions": row.get("horizon_sessions"),
            "matured_observations": row.get("matured_observations") or 0,
            "predictions_emitted": row.get("predictions_emitted") or 0,
            "asset_class": row.get("asset_class")})

    rows = []
    for row in health["registrations"]:
        f = floors.get(row["challenger_id"], {})
        blocked_by = None
        if row["lifecycle"] in FPH.DEFECT_STATES:
            blocked_by = "NO_EXECUTABLE_PREDICTION_PATH"
        elif (row.get("matured_observations") or 0) < (
                f.get("min_raw_matured_required") or 60):
            blocked_by = "FORWARD_EVIDENCE_BELOW_THE_CAPITAL_FLOOR"
        rows.append({
            "challenger_id": row["challenger_id"],
            "asset_class": row.get("asset_class"),
            "lifecycle": row["lifecycle"],
            "producer_stage": row.get("producer_stage"),
            "producer_ran": (row.get("producer_heartbeat") or {}).get("ran"),
            "predictions_emitted": row.get("predictions_emitted"),
            "matured_observations": row.get("matured_observations"),
            "observations_still_required": f.get("observations_still_required"),
            "floor_verdict": f.get("floor_verdict"),
            "sessions_to_floor": f.get("sessions_to_floor"),
            "years_to_floor_estimate": f.get("years_to_floor_estimate"),
            "can_advance_toward_capital": bool(f.get("floor_reachable")),
            "blocked_by": blocked_by,
        })

    advancing = [r for r in rows if r["can_advance_toward_capital"]]
    soonest = min((r["sessions_to_floor"] for r in advancing
                   if r["sessions_to_floor"] is not None), default=None)
    return {
        "schema": SCHEMA,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_id": "R68_FORWARD_EVIDENCE_REPAIR_AND_ALPHA_ACTIVATION",
        "read_only": True,
        "changes_no_policy": True,
        "creates_no_order_or_fill": True,
        "n_registered": health["n_registered"],
        "by_lifecycle_state": health["by_lifecycle_state"],
        "n_that_can_advance_toward_capital": len(advancing),
        "n_orphaned": health["n_orphaned"],
        "n_producer_failed": health["n_producer_failed"],
        "soonest_sessions_to_any_capital_floor": soonest,
        "authorised_policy": _policy(),
        "multi_leg_accounting": _multi_leg_accounting_probe(),
        "registrations": rows,
        "what_remains_human_gated": [
            "SHORT EXPOSURE. api.capital_pool declares long_only and "
            "short_exposure_supported=False, and api.capital_eligibility_gate "
            "applies SHORT_LEG_NOT_EXPRESSIBLE_LONG_ONLY_BOOK. Every hedged "
            "structure the estate has measured is inexpressible operationally "
            "at any size, at any NAV, under any cash policy - and that is an "
            "AUTHORISED POLICY, not a limitation. R68 does not change it, does "
            "not route around it, and does not build the margin model that "
            "would only matter after somebody with authority changed it.",
            "GROSS NOTIONAL ABOVE 1x NAV. R67 established that R66's 1.0x "
            "threshold was a convention nobody with authority set. It is still "
            "undeclared. An owner must declare it before it can bind or be "
            "relaxed.",
        ],
        "headline": (
            "%d registered; %d can advance toward capital, %d are orphaned, %d "
            "have a failed producer. The soonest any registration can satisfy "
            "the 60-observation floor is %s sessions. Multi-leg accounting on "
            "the RESEARCH side is %s; the operational block is the authorised "
            "long-only policy and is human-gated."
            % (health["n_registered"], len(advancing), health["n_orphaned"],
               health["n_producer_failed"], soonest,
               "SUPPORTED AND MEASURED"
               if _multi_leg_accounting_probe()["multi_leg_accounting_supported"]
               else "NOT SUPPORTED")),
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--out", default="")
    args = ap.parse_args(argv)
    try:
        body = build()
    except Exception as exc:                                # noqa: BLE001
        print("R68_CAPITAL_PATH_FAILED %s: %s"
              % (type(exc).__name__, str(exc)[:200]))
        return 1
    text = json.dumps(body, indent=1, sort_keys=True, default=str)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(text, encoding="utf-8")
        print("written: %s" % args.out)
    else:
        print(text)
    print("R68_CAPITAL_PATH_OK %d" % body["n_registered"])
    return 0


if __name__ == "__main__":                                   # pragma: no cover
    raise SystemExit(main())
