r"""scripts/run_fx_carry_cadence_forward_cycle.py - the operator entrypoint for the
per-session prospective decisions of ALPHA_RECOVERY_FX_CARRY_CADENCE_H1_F9B1ACA7.

ACTS (each separate; every rule lives in the owner
``alpha_agent.alpha_recovery.fx_carry_cadence_runtime``)

    --declare-policy [--execute]  the ONE emission policy, written once before any
                                  decision; bound to the REGISTERED identity read
                                  from the canonical registrar
    --advance [--execute]         exactly what alpha_agent.r52.runtime calls; a dry
                                  run by default; --execute acts on the MACHINE
                                  clock only (--now is refused with --execute, so
                                  no instant can be named into the past)
    --preview                     readiness proof: refresh the forward store and
                                  score the newest legitimate information context
                                  WITHOUT freezing anything
    --reproduce                   re-measure the frozen cadence cell on the FROZEN
                                  store through the same code path
    --show                        policy, frozen decisions and the next boundaries

    --child-refresh / --child-score / --child-reproduce are the owner's own child
    processes; they are never run by hand.

RESEARCH ONLY. No order, no fill, no promotion, no capital, no registration, no
purchase, no backfill. Exactly one terminal token is printed on the last line.
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

# WORKTREE IMPORT INTEGRITY - the same guard as the other alpha-recovery
# entrypoints: the venv's editable finder would otherwise run the LIVE tree's
# decision and window owners from a worktree. A no-op in the canonical checkout.
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
        raise RuntimeError("paper_trader was imported before the worktree finder was installed")
    sys.meta_path.insert(0, _WorktreePaperTraderFinder)

from alpha_agent.alpha_recovery import assert_research_root_is_not_live  # noqa: E402
from alpha_agent.alpha_recovery import fx_carry_cadence_challenger as FXC  # noqa: E402
from alpha_agent.alpha_recovery import fx_carry_cadence_runtime as FXR  # noqa: E402
from alpha_agent.alpha_recovery import prospective_decision as PD  # noqa: E402

TOKEN_POLICY_OK = "FX_CADENCE_POLICY_DECLARED"
TOKEN_POLICY_DRY = "FX_CADENCE_POLICY_DRY_RUN"
TOKEN_PREVIEW_OK = "FX_CADENCE_PREVIEW_OK"
TOKEN_REPRODUCE_OK = "FX_CADENCE_FROZEN_CELL_REPRODUCED"
TOKEN_REFUSED = "FX_CADENCE_REFUSED"


def _identity() -> dict:
    """The REGISTERED identity, read from the canonical registrar (never typed)."""
    from paper_trader.api import forward_challenger_registry as FCR

    rows = [r for r in FCR.load_registrations() if r.get("challenger_id") == FXC.CHALLENGER_ID]
    if len(rows) != 1:
        return {}
    r = rows[0]
    ident = r.get("identity") or {}
    return {"challenger_id": r.get("challenger_id"), "freeze_id": r.get("freeze_id"),
            "freeze_record_hash": r.get("freeze_record_hash"),
            "model_spec_hash": ident.get("model_spec_hash") or r.get("model_spec_hash"),
            "feature_snapshot_hash": ident.get("feature_snapshot_hash"),
            "identity_hash": ident.get("identity_hash"), "asset_class": r.get("asset_class"),
            "release": ident.get("release"), "registration_session": r.get("registration_session"),
            "instrument_scope": list(ident.get("instrument_scope") or r.get("instrument_scope") or [])}


def _scope() -> list:
    pol = PD.load_policy(FXC.CHALLENGER_ID) or {}
    if pol.get("instrument_scope"):
        return list(pol["instrument_scope"])
    uni = FXC.resolve_universe()
    return list(uni.get("instruments") or [])


def declare(args) -> str:
    ident = _identity()
    if not ident.get("identity_hash") or not ident.get("registration_session"):
        print("REFUSED: exactly one canonical registration of %s is required" % FXC.CHALLENGER_ID)
        return TOKEN_REFUSED
    resolved = FXC.resolve()
    if not resolved.get("ok"):
        print("REFUSED: the frozen record does not verify: %s" % resolved.get("problems"))
        return TOKEN_REFUSED
    scope = list(resolved["universe"]["instruments"])
    if ident.get("instrument_scope") and list(ident["instrument_scope"]) != scope:
        print("REFUSED: the registered scope differs from the record's universe")
        return TOKEN_REFUSED
    decl = FXR.policy_declaration(ident, scope=scope)
    if not args.execute:
        print(json.dumps(decl, indent=1, sort_keys=True, default=str))
        return TOKEN_POLICY_DRY
    out = PD.declare_policy(**decl)
    pol = out.get("policy") or {}
    print("outcome   : %s" % out.get("outcome"))
    print("path      : %s" % out.get("path"))
    print("window    : %s -> %s ET on the ENTRY session" % (pol.get("declared_information_cutoff_et"),
                                                          pol.get("entry_mark_et")))
    print("cadence   : %s  holding: %s" % (pol.get("rebalance_cadence_sessions"),
                                           pol.get("evaluation_horizon_sessions")))
    print("identity  : %s" % (pol.get("identity") or {}).get("identity_hash"))
    return TOKEN_POLICY_OK if out.get("outcome") in (PD.FROZEN, PD.ALREADY_FROZEN) else TOKEN_REFUSED


def advance(args) -> str:
    if args.execute and args.now:
        print("REFUSED: --now may not be combined with --execute; a decision is frozen on the "
              "machine clock or not at all")
        return TOKEN_REFUSED
    res = FXR.advance(now=args.now, execute=bool(args.execute))
    if args.json:
        print(json.dumps(res, indent=1, sort_keys=True, default=str))
    else:
        for k in ("challenger_id", "now", "newest_published_session", "entry_session", "detail"):
            print("%-26s: %s" % (k, res.get(k)))
        plan = res.get("plan") or {}
        for k in ("information_session", "rebalance_index", "previous_boundary_session",
                  "maturity_session_expected"):
            if k in plan:
                print("%-26s: %s" % (k, plan.get(k)))
        if res.get("freeze"):
            print("%-26s: %s" % ("freeze", res["freeze"]))
    return str(res.get("state"))


def preview(args) -> str:
    """Refresh + score the newest legitimate context. Freezes NOTHING."""
    scope = _scope()
    if not args.skip_refresh:
        ref = FXR.run_refresh("all")
        print("refresh   : ok=%s %s" % (ref.get("ok"), ref.get("refresh")))
        if not ref.get("ok"):
            print(ref.get("stderr_tail"))
            return TOKEN_REFUSED
    rows = {m: FXR.daily_rows(m) for m in scope}
    pub = FXR.published_sessions(scope, rows_by_market=rows)
    t, u = pub[-1], pub[-2]
    from alpha_agent.alpha_recovery import next_open_challenger as NOC
    s = NOC.next_eligible_session(t)
    fin = FXR.information_is_final(scope, u, rows)
    print("newest published t : %s   information u : %s (final=%s)   entry s : %s" % (t, u, fin["final"], s))
    plan = {"entry_session": s, "information_session": u, "newest_session_required": t}
    stamp = "preview_%s" % s
    out_path = FXR.runs_dir() / ("%s.json" % stamp)
    prev_path = FXR.runs_dir() / ("%s_prev.json" % stamp)
    FXR._write_json(prev_path, {"weights": {}})
    res = FXR._run_child(["--child-score", "--entry", s, "--info", u, "--newest", t,
                          "--prev-json", str(prev_path), "--out", str(out_path)],
                         env_extra={FXR.CURVES_DIR_ENV: FXR.curves_dir()}, timeout=FXR.TIMEOUT_SCORE)
    body = FXR._read_json(out_path) or {}
    if not body:
        print(res.get("stderr_tail"))
        return TOKEN_REFUSED
    print(json.dumps({k: body.get(k) for k in ("ok", "reason", "weights", "leverage", "leverage_state",
                                                "gross_unlevered", "predictions", "checks")},
                     indent=1, sort_keys=True, default=str))
    print("artifact  : %s  (a PREVIEW: nothing was frozen)" % out_path)
    return TOKEN_PREVIEW_OK if body.get("ok") else TOKEN_REFUSED


def reproduce(args) -> str:
    out_path = FXR.runs_dir() / "reproduce_frozen_cell.json"
    res = FXR._run_child(["--child-reproduce", "--out", str(out_path)],
                         env_extra={FXR.CURVES_DIR_ENV: FXR.FROZEN_CURVES_DIR},
                         timeout=FXR.TIMEOUT_SCORE)
    body = FXR._read_json(out_path) or {}
    print(json.dumps(body, indent=1, sort_keys=True, default=str))
    if not body:
        print(res.get("stderr_tail"))
    return TOKEN_REPRODUCE_OK if body.get("ok") else TOKEN_REFUSED


def show(args) -> str:
    pol = PD.load_policy(FXC.CHALLENGER_ID)
    held = PD.list_decisions(FXC.CHALLENGER_ID)
    scope = _scope()
    pub = FXR.published_sessions(scope)
    ident = (pol or {}).get("identity") or _identity()
    print("policy declared      : %s" % bool(pol))
    print("decisions frozen     : %d %s" % (len(held), [r.get("eligible_session") for r in held]))
    print("newest marks session : %s" % (pub[-1] if pub else None))
    reg = ident.get("registration_session")
    if reg:
        for row in FXR.schedule_preview(registration_session=reg, published=pub)[:4]:
            print("next boundary        : entry %(entry_session)s (newest published "
                  "%(newest_published_session)s, index %(rebalance_index)s)" % row)
    return "FX_CADENCE_SHOWN"


def _child_refresh(args) -> int:
    body = FXR.refresh_store_in_child(args.child_refresh, _scope())
    print(json.dumps({k: body.get(k) for k in ("ok", "reason", "n_markets", "n_failed", "failed")}))
    return 0 if body.get("ok") else 3


def _child_score(args) -> int:
    prev = (FXR._read_json(Path(args.prev_json)) or {}).get("weights") if args.prev_json else {}
    body = FXR.score_in_child(entry_session=args.entry, information_session=args.info,
                              newest_session=args.newest, prev_weights=prev or {}, scope=_scope())
    FXR._write_json(Path(args.out), body)
    return 0 if body.get("ok") else 3


def _child_reproduce(args) -> int:
    body = FXR.reproduce_frozen_cell_in_child()
    FXR._write_json(Path(args.out), body)
    return 0 if body.get("ok") else 3


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--declare-policy", action="store_true")
    ap.add_argument("--advance", action="store_true")
    ap.add_argument("--preview", action="store_true")
    ap.add_argument("--reproduce", action="store_true")
    ap.add_argument("--show", action="store_true")
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--skip-refresh", action="store_true",
                    help="--preview only: score the forward store as it stands")
    ap.add_argument("--now", default=None)
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--child-refresh", choices=("scope", "all"), default=None)
    ap.add_argument("--child-score", action="store_true")
    ap.add_argument("--child-reproduce", action="store_true")
    ap.add_argument("--entry", default=None)
    ap.add_argument("--info", default=None)
    ap.add_argument("--newest", default=None)
    ap.add_argument("--prev-json", default=None)
    ap.add_argument("--out", default=None)
    args = ap.parse_args(argv)
    assert_research_root_is_not_live()

    if args.child_refresh:
        return _child_refresh(args)
    if args.child_score:
        return _child_score(args)
    if args.child_reproduce:
        return _child_reproduce(args)

    if args.declare_policy:
        token = declare(args)
    elif args.advance:
        token = advance(args)
    elif args.preview:
        token = preview(args)
    elif args.reproduce:
        token = reproduce(args)
    elif args.show:
        token = show(args)
    else:
        ap.print_help()
        return 0
    print(token)
    return 1 if token == TOKEN_REFUSED or token == FXR.ST_BLOCKED else 0


if __name__ == "__main__":                                       # pragma: no cover
    raise SystemExit(main())
