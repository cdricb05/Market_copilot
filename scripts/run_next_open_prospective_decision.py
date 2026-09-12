r"""scripts/run_next_open_prospective_decision.py - the operator entrypoint that
DECLARES the next-open challenger's emission boundary, MEASURES when its source
data actually publishes, and FREEZES one prospective decision inside the window.

FOUR ACTS, DELIBERATELY SEPARATE
    --declare-policy      Written ONCE, before any decision exists. States the
                          boundary (the window opens at midnight exchange-local
                          on the ENTRY session and shuts at its 09:30 ET open),
                          the execution contract that keeps the information
                          session and the entry session apart, the cadence, the
                          horizon, the cost policy and the REGISTERED identity.

    --probe-publication   ONE free metadata poll asking whether the historical
                          vendor has published a session yet. Downloads nothing,
                          buys nothing, and records an append-only poll so a
                          latency can be measured instead of assumed.

    --freeze              Computes the frozen rule ONCE from the INFORMATION
                          session's snapshot and freezes the weight book for the
                          ENTRY session. Refuses - loudly and with a reason - if
                          the source is not published, if the input did not come
                          from the information session, if the window has not
                          opened, or if it has already shut.

    --show                Where the challenger stands, for one entry session.

THE THREE SESSIONS THIS SCRIPT REFUSES TO CONFLATE
    information session   t   - its 15:45 ET snapshot forms the decision
    entry session         t+1 - its 09:30 ET open is the mark
    maturity session      t+1 plus five eligible sessions

    ``--session`` always names the ENTRY session, because that is the session the
    decision is ABOUT and the session its emission window sits on. The
    information session is derived from the exchange calendar, never passed in.

RESEARCH ONLY. No order, no fill, no promotion, no capital, no registration, no
purchase, no backfill.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO))
if str(_REPO.parent) not in sys.path:
    sys.path.insert(0, str(_REPO.parent))

# ---------------------------------------------------------------------------
# WORKTREE IMPORT INTEGRITY
#
# The venv carries an editable install whose finder hard-maps ``paper_trader``
# to C:\Users\binis\paper_trader and sits in ``sys.meta_path`` AHEAD of every
# ``sys.path`` entry. Run from a worktree, this entrypoint would otherwise
# freeze decisions using the LIVE tree's emission-window code while reading the
# worktree's challenger - the exact trap ``tests/conftest.py`` closes for the
# suite. In the canonical checkout the directory IS named ``paper_trader``, the
# mapping already points here, and this block is a no-op.
# ---------------------------------------------------------------------------
if _REPO.name != "paper_trader" and (_REPO / "__init__.py").exists():
    from importlib.util import spec_from_file_location

    class _WorktreePaperTraderFinder:
        """Resolve ``paper_trader`` to the checkout this script lives in."""

        ROOT = _REPO

        @classmethod
        def find_spec(cls, fullname, path=None, target=None):  # noqa: ANN001
            if fullname != "paper_trader":
                return None
            return spec_from_file_location(
                fullname, str(cls.ROOT / "__init__.py"),
                submodule_search_locations=[str(cls.ROOT)])

    if "paper_trader" in sys.modules:                    # pragma: no cover
        raise RuntimeError(
            "paper_trader was imported before the worktree finder was "
            "installed; this entrypoint would run against %s"
            % getattr(sys.modules["paper_trader"], "__file__", "?"))
    sys.meta_path.insert(0, _WorktreePaperTraderFinder)

from alpha_agent.alpha_recovery import assert_research_root_is_not_live  # noqa: E402
from alpha_agent.alpha_recovery import next_open_challenger as NOC  # noqa: E402
from alpha_agent.alpha_recovery import opra_publication_probe as PP  # noqa: E402
from alpha_agent.alpha_recovery import options_acquisition as OA  # noqa: E402
from alpha_agent.alpha_recovery import options_surface as OS  # noqa: E402
from alpha_agent.alpha_recovery import prospective_decision as PD  # noqa: E402
from alpha_agent.alpha_recovery import reversed_skew as RS  # noqa: E402
from alpha_agent.alpha_recovery import stable_hash  # noqa: E402


def _identity() -> dict:
    """The REGISTERED identity, read from the canonical registrar.

    A script may read it; the research package may not, which is why the binding
    is injected here rather than looked up inside the producer.
    """
    from paper_trader.api import forward_challenger_registry as FCR

    for r in FCR.load_registrations():
        if r.get("challenger_id") == NOC.CHALLENGER_ID:
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
              % NOC.CHALLENGER_ID)
        print("         run %s first." % NOC.REGISTRATION_OWNER)
        return 2
    out = PD.declare_policy(**NOC.policy_declaration(ident))
    pol = out.get("policy") or {}
    print("outcome  : %s" % out.get("outcome"))
    print("path     : %s" % out.get("path"))
    print("window   : opens %s ET, shuts %s ET, ON THE ENTRY SESSION"
          % (pol.get("declared_information_cutoff_et"), pol.get("entry_mark_et")))
    ec = pol.get("execution_contract") or {}
    print("boundary : %s" % ec.get("execution_boundary"))
    print("info cut : %s ET on the session BEFORE the entry session"
          % ec.get("information_cutoff_et_on_the_information_session"))
    print("cadence  : %s   horizon: %s counted from %s"
          % (pol.get("rebalance_cadence_sessions"),
             pol.get("evaluation_horizon_sessions"), ec.get("horizon_counts_from")))
    print("identity : %s" % (pol.get("identity") or {}).get("identity_hash"))
    return 0


def probe(args) -> int:
    """ONE free metadata poll for the INFORMATION session. Downloads nothing."""
    session = args.session
    if not session:
        print("--probe-publication requires --session (the INFORMATION session)")
        return 2
    res = PP.poll(session, now=args.now)
    print("session        : %s" % res.get("session"))
    print("queried at     : %s" % res.get("queried_at"))
    print("outcome        : %s" % res.get("outcome"))
    print("available      : %s" % res.get("available"))
    print("dataset end    : %s (raw %s)" % (res.get("dataset_available_end"),
                                            res.get("dataset_range_end_raw")))
    if "billable_bytes" in res:
        print("billable bytes : %s" % res.get("billable_bytes"))
    print("downloaded     : %s bytes, paid $%.2f"
          % (res.get("downloaded_bytes"), res.get("paid_usd")))
    if res.get("detail"):
        print("detail         : %s" % res["detail"])
    if args.no_record:
        print("(not recorded: --no-record)")
        return 0
    node = PP.record(res)
    rep = PP.report(session)
    print()
    for k in ("SESSION", "FIRST_QUERY_TIME", "FIRST_AVAILABLE_TIME",
              "PUBLICATION_DELAY", "AVAILABLE_BEFORE_NEXT_OPEN"):
        print("%-27s = %s" % (k, rep.get(k)))
    print("polls recorded              = %s" % node.get("polls_recorded"))
    print("next open (the deadline)    = %s" % rep.get("next_open_at"))
    return 0


def freeze(args) -> int:
    """Compute the frozen rule ONCE from the information session and freeze it
    for the ENTRY session."""
    import numpy as np

    entry = args.session
    ctx = NOC.decision_context(entry)
    info = ctx.get("information_session")

    print("entry session      : %s" % ctx.get("entry_session"))
    print("information session: %s" % info)
    print("entry boundary     : %s at %s" % (ctx.get("entry_boundary"),
                                             ctx.get("entry_mark_at")))
    print("maturity session   : %s (%d eligible sessions from ENTRY)"
          % (ctx.get("maturity_session"), ctx.get("evaluation_horizon_sessions")))
    print()

    if not NOC.is_eligible_session(entry):
        print("REFUSED: %s is not an eligible exchange session." % entry)
        return 3
    if not info:
        print("REFUSED: no information session resolves before %s." % entry)
        return 3

    src = NOC.source_available(info)
    print("source owned       : %s (last owned %s)"
          % (src.get("owned"), src.get("owned_last_session")))
    print("source published   : %s (%s)" % (src.get("published"),
                                            src.get("publication_state")))
    if not src.get("usable_for_a_freeze"):
        print()
        print("REFUSED: %s" % NOC.AWAITING_SOURCE_PUBLICATION)
        print("         the owned surface holds no 15:45 ET snapshot for %s, so "
              "the frozen rule cannot be evaluated on it." % info)
        print("         This is a legitimate MISS if the window shuts first; it "
              "is never backfilled.")
        return 4

    sp = RS.spec()
    surface = OS.surface_path()
    f = OS.features(rebuild=True, surface=surface)
    dates = [str(x)[:10] for x in f["date"]]
    if info not in dates:
        print("REFUSED: the owned surface holds no observation for %s." % info)
        return 4
    i = dates.index(info)
    z = OS._z(f[sp["field"]])
    zi = z[i]
    if not np.isfinite(zi):
        print("REFUSED: the z-score is not finite at %s (the strictly-prior "
              "%d-observation window is incomplete)." % (info, OS.ZSCORE_LOOKBACK))
        return 5
    position = float(sp["sign"]) * float(np.sign(zi))
    if position == 0.0:
        print("REFUSED: the frozen rule implies a zero position at %s." % info)
        return 5
    weights = {OA.UNDERLYING: position}

    observed_at = args.observed_at or ctx.get("information_cutoff_at")
    chk = NOC.check_information_instant(entry, observed_at)
    if chk.get("violates"):
        print("REFUSED: %s" % NOC.REFUSED_STALE_INPUT)
        print("         %s" % chk.get("reason"))
        return 6

    src_path = Path(surface)
    source_data_hash = stable_hash({
        "surface": str(src_path), "bytes": src_path.stat().st_size,
        "observations": len(dates), "last_observation": dates[-1]})
    feature_state_hash = stable_hash({
        "field": sp["field"], "session": info,
        "value": float(f[sp["field"]].to_numpy()[i]),
        "z": float(zi), "lookback": OS.ZSCORE_LOOKBACK,
        "prior_window": dates[max(0, i - OS.ZSCORE_LOOKBACK):i]})

    print()
    print("feature (%s)     : %.8f" % (sp["field"], f[sp["field"]].to_numpy()[i]))
    print("z (strictly prior %d): %+.6f" % (OS.ZSCORE_LOOKBACK, zi))
    print("frozen sign        : %+d" % sp["sign"])
    print("position           : %s" % weights)
    print("observed at        : %s (cutoff %s)"
          % (observed_at, chk.get("declared_cutoff_at")))

    out = PD.freeze_decision(
        challenger_id=NOC.CHALLENGER_ID, eligible_session=entry,
        weights=weights, source_data_hash=source_data_hash,
        feature_state_hash=feature_state_hash,
        feature_observed_at=observed_at, now=args.now, context=ctx)
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
        print("window opens/closes : %s -> %s"
              % (rec.get("emission_window_opens_at"),
                 rec.get("emission_window_closes_at")))
        dc = rec.get("decision_context") or {}
        print("context             : info %s -> entry %s -> maturity %s"
              % (dc.get("information_session"), dc.get("entry_session"),
                 dc.get("maturity_session")))
    return 0 if out.get("frozen") else 7


#: Exactly one of these is printed on the last line of a --cycle run, so a
#: scheduler (or a human reading a log) never has to parse prose.
CYCLE_FROZEN = "NEXT_OPEN_CYCLE_FROZEN"
CYCLE_ALREADY_FROZEN = "NEXT_OPEN_CYCLE_ALREADY_FROZEN"
CYCLE_AWAITING_SOURCE = "NEXT_OPEN_CYCLE_AWAITING_SOURCE"
CYCLE_MISSED = "NEXT_OPEN_CYCLE_MISSED"
CYCLE_BLOCKED = "NEXT_OPEN_CYCLE_BLOCKED"
CYCLE_TOKENS = (CYCLE_FROZEN, CYCLE_ALREADY_FROZEN, CYCLE_AWAITING_SOURCE,
                CYCLE_MISSED, CYCLE_BLOCKED)


def cycle(args) -> int:
    """ONE idempotent command for one information session: probe, then freeze.

    Safe to run as often as a scheduler likes. Every step is idempotent - the
    poll is append-only, the freeze is first-write-wins - so a trigger that
    fires while there is nothing to do is a no-op, and a trigger that fires
    twice produces one decision. That is what lets the 9.5-hour window be
    covered by a schedule instead of by a person.

    ``--session`` here is the INFORMATION session (the one that just traded).
    The entry session is derived, because deriving it is the whole point.
    """
    info = args.session
    entry = NOC.entry_session_for(info)
    print("information session : %s" % info)
    print("entry session       : %s" % entry)
    if not NOC.is_eligible_session(info):
        print("BLOCKED: %s is not an eligible exchange session." % info)
        print(CYCLE_BLOCKED)
        return 2
    if not entry:
        print("BLOCKED: no eligible session follows %s." % info)
        print(CYCLE_BLOCKED)
        return 2

    # 1. Measure publication. Free, bounded, append-only, never assumed.
    res = PP.poll(info, now=args.now)
    PP.record(res)
    rep = PP.report(info)
    print("publication         : %s (available=%s)"
          % (res.get("outcome"), res.get("available")))
    print("first available     : %s" % rep.get("FIRST_AVAILABLE_TIME"))
    print("delay               : %s" % rep.get("PUBLICATION_DELAY"))
    print("before next open    : %s (deadline %s)"
          % (rep.get("AVAILABLE_BEFORE_NEXT_OPEN"), rep.get("next_open_at")))

    # 2. Where does the entry session stand, on the facts?
    st = NOC.entry_state(entry, now=args.now)
    print("entry state         : %s" % st.get("entry_state"))
    src = st.get("source") or {}
    print("surface holds %s : %s (last owned %s)"
          % (info, src.get("owned"), src.get("owned_last_session")))

    if st["entry_state"] in (NOC.READY_FOR_ENTRY, NOC.AWAITING_MATURITY):
        print("a decision for %s is already frozen; nothing to do." % entry)
        print(CYCLE_ALREADY_FROZEN)
        return 0
    if st["entry_state"] == NOC.MISSED:
        print("the %s open has passed with no decision. This session is MISSED "
              "permanently and is never backfilled." % entry)
        print(CYCLE_MISSED)
        return 0
    if st["entry_state"] in (NOC.AWAITING_SOURCE_PUBLICATION,
                             NOC.AWAITING_INFORMATION_SESSION):
        print("blocked on          : %s" % st.get("blocked_on", st["entry_state"]))
        if not src.get("owned"):
            print()
            print("the 15:45 ET snapshot for %s is not in the owned surface yet." % info)
            print("its owner is the acquisition stage, which appends and never "
                  "rebuilds:")
            print("    python scripts/run_alpha_recovery_offensive.py options")
        print(CYCLE_AWAITING_SOURCE)
        return 0

    # 3. The window is open and the data is in hand.
    args.session = entry
    rc = freeze(args)
    print(CYCLE_FROZEN if rc == 0 else CYCLE_BLOCKED)
    return rc


def show(args) -> int:
    st = NOC.state(args.session, now=args.now)
    if args.json:
        print(json.dumps(st, indent=1, sort_keys=True, default=str))
        return 0
    print("challenger         : %s" % st.get("challenger_id"))
    print("state              : %s" % st.get("state"))
    print("registered         : %s" % st.get("registered"))
    print("policy declared    : %s" % st.get("policy_declared"))
    print("decisions frozen   : %s %s" % (st.get("decisions_frozen"),
                                          st.get("frozen_sessions")))
    print("forward observations: %s" % st.get("true_forward_observations"))
    print("capital eligible   : %s" % st.get("capital_eligible"))
    ctx = st.get("decision_context") or {}
    if ctx:
        print("information session: %s (cutoff %s)"
              % (ctx.get("information_session"), ctx.get("information_cutoff_at")))
        print("entry session      : %s (mark %s)"
              % (ctx.get("entry_session"), ctx.get("entry_mark_at")))
        print("maturity session   : %s" % ctx.get("maturity_session"))
    src = st.get("source") or {}
    if src:
        print("source owned       : %s" % src.get("owned"))
        print("source published   : %s (%s)" % (src.get("published"),
                                                src.get("publication_state")))
    if st.get("blocked_on"):
        print("blocked on         : %s" % st["blocked_on"])
    if st.get("next_action"):
        print("next action        : %s" % st["next_action"])
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--declare-policy", action="store_true")
    ap.add_argument("--probe-publication", action="store_true",
                    help="one free metadata poll for --session; downloads nothing")
    ap.add_argument("--freeze", action="store_true")
    ap.add_argument("--cycle", action="store_true",
                    help="one idempotent daily run for --session (the "
                         "INFORMATION session): probe publication, then freeze "
                         "the next session's decision if it is due")
    ap.add_argument("--show", action="store_true")
    ap.add_argument("--session", default=None,
                    help="the ENTRY session (the information session is derived)")
    ap.add_argument("--now", default=None,
                    help="ISO instant to judge the window at (testing)")
    ap.add_argument("--observed-at", default=None,
                    help="when the inputs were observed (defaults to the 15:45 "
                         "ET snapshot instant on the information session)")
    ap.add_argument("--no-record", action="store_true",
                    help="probe only: do not append the poll to the record")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    assert_research_root_is_not_live()

    if args.declare_policy:
        return declare(args)
    if args.probe_publication:
        return probe(args)
    if args.cycle:
        if not args.session:
            print("--cycle requires --session (the INFORMATION session)")
            return 2
        return cycle(args)
    if args.freeze:
        if not args.session:
            print("--freeze requires --session (the ENTRY session)")
            return 2
        return freeze(args)
    if args.show:
        return show(args)
    ap.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
