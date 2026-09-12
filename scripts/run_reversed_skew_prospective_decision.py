r"""scripts/run_reversed_skew_prospective_decision.py - the operator entrypoint
that DECLARES the challenger's emission boundary and FREEZES one prospective
decision inside it.

TWO ACTS, DELIBERATELY SEPARATE
    --declare-policy   Written ONCE, before any decision exists. It states the
                       boundary (information cutoff 15:45 ET, entry mark 16:00
                       ET), the cadence, the horizon, the cost policy and the
                       REGISTERED identity the decisions will bind to. Declaring
                       it in advance is what makes the cutoff a constraint: a
                       policy written alongside a decision could be written to
                       fit it.

    --freeze           Computes the frozen rule ONCE for one eligible session
                       and hands the resulting weight book to the research
                       owner, which freezes it or refuses and says why. This is
                       the ONLY place the signal is computed, and the live
                       accrual owner cannot reach it.

WHY THE LIVE SIDE CANNOT DO THIS
    ``api.canonical_forward_accrual`` READS the artifact this script writes. It
    has no import through which an option chain, an implied volatility, a
    z-score or a position choice is reachable. That separation is the safety
    property: an accrual owner that could compute the signal could compute it
    twice and keep the better answer.

RESEARCH ONLY. No order, no fill, no promotion, no capital, no registration.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alpha_agent.alpha_recovery import assert_research_root_is_not_live  # noqa: E402
from alpha_agent.alpha_recovery import options_acquisition as OA  # noqa: E402
from alpha_agent.alpha_recovery import options_surface as OS  # noqa: E402
from alpha_agent.alpha_recovery import prospective_decision as PD  # noqa: E402
from alpha_agent.alpha_recovery import reversed_skew as RS  # noqa: E402
from alpha_agent.alpha_recovery import stable_hash  # noqa: E402

#: The entry mark the frozen specification names: the 16:00 ET close on the
#: signal session, fifteen minutes after the 15:45 ET snapshot. Both instants
#: come FROM the frozen construction; neither is chosen here.
ENTRY_MARK_ET = (16, 0)


def _identity() -> dict:
    """The REGISTERED identity, read from the canonical registrar.

    A script may read it; the research package may not, which is why the
    binding is injected here rather than looked up inside the producer.
    """
    from paper_trader.api import forward_challenger_registry as FCR

    for r in FCR.load_registrations():
        if r.get("challenger_id") == RS.CHALLENGER_ID:
            ident = r.get("identity") or {}
            return {
                "challenger_id": r.get("challenger_id"),
                "freeze_id": r.get("freeze_id"),
                "freeze_record_hash": r.get("freeze_record_hash"),
                "model_spec_hash": r.get("model_spec_hash"),
                "feature_snapshot_hash": r.get("feature_snapshot_hash"),
                "identity_hash": ident.get("identity_hash"),
                "asset_class": r.get("asset_class"),
                "release": ident.get("release"),
                "registration_session": r.get("registration_session"),
            }
    return {}


def declare(args) -> int:
    ident = _identity()
    if not ident.get("identity_hash"):
        print("REFUSED: %s is not registered with the canonical registrar, so "
              "there is no identity for its decisions to bind to."
              % RS.CHALLENGER_ID)
        return 2
    out = PD.declare_policy(
        challenger_id=RS.CHALLENGER_ID,
        information_cutoff_et=OA.SNAPSHOT_ET,
        entry_mark_et=ENTRY_MARK_ET,
        rebalance_cadence_sessions=RS.FROZEN_HORIZON,
        evaluation_horizon_sessions=RS.FROZEN_HORIZON,
        cost_policy={"bps_per_side": OS.COST_PRIMARY_BPS,
                     "ladder_bps_per_side": list(OS.COST_LADDER_BPS),
                     "applied": "both legs of every decision"},
        instrument_scope=[OA.UNDERLYING],
        identity=ident,
        emission_rule=RS.frozen_specification()["emission_rule"])
    print("outcome : %s" % out.get("outcome"))
    print("path    : %s" % out.get("path"))
    pol = out.get("policy") or {}
    print("cutoff  : %s ET   entry mark: %s ET"
          % (pol.get("declared_information_cutoff_et"),
             pol.get("entry_mark_et")))
    print("cadence : %s   horizon: %s"
          % (pol.get("rebalance_cadence_sessions"),
             pol.get("evaluation_horizon_sessions")))
    print("identity: %s" % (pol.get("identity") or {}).get("identity_hash"))
    return 0


def freeze(args) -> int:
    """Compute the frozen rule ONCE for ``--session`` and freeze the result."""
    import numpy as np

    session = args.session
    sp = RS.spec()
    surface = OS.surface_path()
    f = OS.features(rebuild=True, surface=surface)
    dates = [str(x)[:10] for x in f["date"]]

    if session not in dates:
        print("REFUSED: the owned surface holds no observation for %s." % session)
        print("         last owned observation: %s" % (dates[-1] if dates else None))
        print("         The 15:45 ET snapshot for %s must be acquired before the "
              "frozen rule can be evaluated on it." % session)
        return 3

    i = dates.index(session)
    z = OS._z(f[sp["field"]])
    zi = z[i]
    if not np.isfinite(zi):
        print("REFUSED: the z-score is not finite at %s (the strictly-prior "
              "%d-observation window is incomplete)."
              % (session, OS.ZSCORE_LOOKBACK))
        return 4

    position = float(sp["sign"]) * float(np.sign(zi))
    if position == 0.0:
        print("REFUSED: the frozen rule implies a zero position at %s." % session)
        return 5
    weights = {OA.UNDERLYING: position}

    # WHAT the decision was computed from, hashed so a later reader can prove
    # the inputs were the owned ones and had not moved.
    src = Path(surface)
    source_data_hash = stable_hash({
        "surface": str(src), "bytes": src.stat().st_size,
        "observations": len(dates), "last_observation": dates[-1]})
    feature_state_hash = stable_hash({
        "field": sp["field"], "session": session,
        "value": float(f[sp["field"]].to_numpy()[i]),
        "z": float(zi), "lookback": OS.ZSCORE_LOOKBACK,
        "prior_window": dates[max(0, i - OS.ZSCORE_LOOKBACK):i]})

    print("session            : %s" % session)
    print("feature (%s)     : %.8f" % (sp["field"],
                                       f[sp["field"]].to_numpy()[i]))
    print("z (strictly prior %d): %+.6f" % (OS.ZSCORE_LOOKBACK, zi))
    print("frozen sign        : %+d" % sp["sign"])
    print("position           : %s" % weights)

    out = PD.freeze_decision(
        challenger_id=RS.CHALLENGER_ID, eligible_session=session,
        weights=weights, source_data_hash=source_data_hash,
        feature_state_hash=feature_state_hash,
        feature_observed_at=args.observed_at, now=args.now)
    print()
    print("outcome : %s" % out.get("outcome"))
    print("frozen  : %s" % out.get("frozen"))
    if out.get("detail"):
        print("detail  : %s" % out["detail"])
    if out.get("path"):
        print("path    : %s" % out["path"])
    rec = out.get("decision") or {}
    if rec:
        print("weights_hash        : %s" % rec.get("weights_hash"))
        print("record_hash         : %s" % rec.get("record_hash"))
        print("declared cutoff     : %s" % rec.get("declared_information_cutoff"))
        print("window closes       : %s" % rec.get("emission_window_closes_at"))
    return 0 if out.get("frozen") else 6


def show(args) -> int:
    st = PD.state(RS.CHALLENGER_ID, session=args.session, now=args.now)
    print("state              : %s" % st.get("state"))
    print("policy declared    : %s" % st.get("policy_declared"))
    print("decisions frozen   : %s %s" % (st.get("decisions_frozen"),
                                          st.get("frozen_sessions")))
    win = st.get("emission_window") or {}
    if win:
        print("window for %s: %s" % (args.session, win.get("state")))
        print("   opens  : %s" % win.get("opens_at"))
        print("   closes : %s" % win.get("closes_at"))
        print("   now    : %s" % win.get("now"))
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--declare-policy", action="store_true")
    ap.add_argument("--freeze", action="store_true")
    ap.add_argument("--show", action="store_true")
    ap.add_argument("--session", default=None,
                    help="the eligible session to decide for, e.g. 2026-09-14")
    ap.add_argument("--now", default=None,
                    help="ISO instant to judge the window at (testing)")
    ap.add_argument("--observed-at", default=None,
                    help="when the inputs were observed (defaults to the "
                         "declared cutoff for the session)")
    args = ap.parse_args()
    assert_research_root_is_not_live()

    if args.declare_policy:
        return declare(args)
    if args.freeze:
        if not args.session:
            print("--freeze requires --session")
            return 2
        if args.observed_at is None:
            pol = PD.load_policy(RS.CHALLENGER_ID) or {}
            from paper_trader.engine import forward_emission_window as EW
            args.observed_at = EW.information_cutoff_at(
                pol.get("emission_window"), args.session)
        return freeze(args)
    if args.show:
        return show(args)
    ap.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
