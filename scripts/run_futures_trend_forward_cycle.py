r"""scripts/run_futures_trend_forward_cycle.py - the operator entrypoint for the
managed-futures time-series trend challenger ALPHA_RECOVERY_FUTURES_TS_TREND_H21_V1
(MULTI_ASSET_CAPITAL_ACTIVATION_R55_V1).

ACTS (each separate; every rule lives in the owners
``alpha_agent.alpha_recovery.futures_trend_challenger`` / ``futures_trend_runtime``)

    --validate                    historical point-in-time validation on the FROZEN
                                  R41 store (research; writes nothing)
    --freeze-record [--execute]   write the ONE immutable frozen record (first write
                                  wins) under the campaign research root
    --declare-policy [--execute]  the ONE emission policy, written once before any
                                  decision; bound to the REGISTERED identity read
                                  from the canonical registrar
    --seed-store                  copy the frozen store's scope series into this
                                  owner's forward store when it is empty (READ-only
                                  on the frozen store); then --refresh extends it
    --refresh                     refresh the forward store from owned Norgate
                                  settlements in a CHILD process
    --advance [--execute]         exactly what alpha_agent.r52.runtime calls; a dry
                                  run by default; --execute acts on the MACHINE clock
    --show                        record, policy, frozen decisions, next boundaries

    --child-refresh is the owner's own child process; never run by hand.

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

# WORKTREE IMPORT INTEGRITY - the same guard as the other alpha-recovery entrypoints.
if _REPO.name != "paper_trader" and (_REPO / "__init__.py").exists():
    from importlib.util import spec_from_file_location

    class _WorktreePaperTraderFinder:
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
from alpha_agent.alpha_recovery import futures_trend_challenger as FTC  # noqa: E402
from alpha_agent.alpha_recovery import futures_trend_runtime as FTR  # noqa: E402
from alpha_agent.alpha_recovery import prospective_decision as PD  # noqa: E402

TOKEN_VALIDATED = "FUTURES_TREND_VALIDATED"
TOKEN_RECORD_OK = "FUTURES_TREND_RECORD_FROZEN"
TOKEN_RECORD_DRY = "FUTURES_TREND_RECORD_DRY_RUN"
TOKEN_POLICY_OK = "FUTURES_TREND_POLICY_DECLARED"
TOKEN_POLICY_DRY = "FUTURES_TREND_POLICY_DRY_RUN"
TOKEN_STORE_OK = "FUTURES_TREND_STORE_OK"
TOKEN_REFUSED = "FUTURES_TREND_REFUSED"


def _identity() -> dict:
    """The REGISTERED identity, read from the canonical registrar (never typed)."""
    from paper_trader.api import forward_challenger_registry as FCR

    rows = [r for r in FCR.load_registrations() if r.get("challenger_id") == FTC.CHALLENGER_ID]
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
            "horizon_sessions": r.get("horizon_sessions"),
            "instrument_scope": list(ident.get("instrument_scope") or r.get("instrument_scope") or [])}


def _scope() -> list:
    pol = PD.load_policy(FTC.CHALLENGER_ID) or {}
    if pol.get("instrument_scope"):
        return list(pol["instrument_scope"])
    res = FTC.resolve()
    if res.get("ok"):
        return list((res["record"].get("forward_specification") or {}).get("instruments") or [])
    return []


def _brief(v: dict) -> dict:
    keys = ("periods", "ann_net", "ann_gross", "sharpe", "t_net", "max_dd", "ann_vol",
            "mean_oneway_turnover", "ann_cost_drag", "mean_gross_leverage", "max_class_share",
            "degenerate_under_controls")
    return {part: {k: v.get(part, {}).get(k) for k in keys} for part in ("full", "selection", "lockbox")}


def validate(args) -> str:
    v = FTC.validate()
    if not v.get("ok"):
        print("REFUSED: %s" % v.get("reason"))
        return TOKEN_REFUSED
    print(json.dumps({k: v.get(k) for k in ("cell_id", "decisions", "first_decision", "last_decision",
                                            "classification", "why", "lockbox_sign_agrees",
                                            "store_last_session", "rows_scored_by_class")},
                     indent=1, default=str))
    print(json.dumps(_brief(v), indent=1, default=str))
    print("instruments: %d" % len(v.get("instruments") or []))
    return TOKEN_VALIDATED


def freeze_record(args) -> str:
    existing = FTC.resolve()
    if existing.get("ok"):
        print("record already frozen: %s (record_hash %s)" % (existing["record_path"],
                                                               existing["record"]["record_hash"]))
        return TOKEN_RECORD_OK
    v = FTC.validate()
    if not v.get("ok"):
        print("REFUSED: %s" % v.get("reason"))
        return TOKEN_REFUSED
    rec = FTC.build_record(v)
    print("classification : %s" % rec["classification"])
    print("why            : %s" % rec["why"])
    print("instruments    : %d" % len(rec["forward_specification"]["instruments"]))
    print("record_hash    : %s" % rec["record_hash"])
    print("freeze_record  : %s" % rec["freeze_record_hash"])
    if not args.execute:
        print("dry run: nothing written (add --execute to freeze the record once)")
        return TOKEN_RECORD_DRY
    out = FTC.write_record(rec)
    print("outcome        : %s" % out["outcome"])
    print("path           : %s" % out["path"])
    return TOKEN_RECORD_OK if out["outcome"] in ("FROZEN", "ALREADY_FROZEN") else TOKEN_REFUSED


def declare(args) -> str:
    ident = _identity()
    if not ident.get("identity_hash") or not ident.get("registration_session"):
        print("REFUSED: exactly one canonical registration of %s is required" % FTC.CHALLENGER_ID)
        return TOKEN_REFUSED
    resolved = FTC.resolve()
    if not resolved.get("ok"):
        print("REFUSED: the frozen record does not verify: %s" % resolved.get("problems"))
        return TOKEN_REFUSED
    scope = list((resolved["record"].get("forward_specification") or {}).get("instruments") or [])
    if ident.get("instrument_scope") and list(ident["instrument_scope"]) != scope:
        print("REFUSED: the registered scope differs from the record's instruments")
        return TOKEN_REFUSED
    decl = FTR.policy_declaration(ident, scope=scope)
    if not args.execute:
        print(json.dumps({k: decl[k] for k in ("challenger_id", "rebalance_cadence_sessions",
                                               "evaluation_horizon_sessions", "instrument_scope")},
                         indent=1, default=str))
        print("cost policy: %s bps/side (max across scope)" % decl["cost_policy"]["bps_per_side"])
        return TOKEN_POLICY_DRY
    out = PD.declare_policy(**decl)
    pol = out.get("policy") or {}
    print("outcome   : %s" % out.get("outcome"))
    print("path      : %s" % out.get("path"))
    print("cadence   : %s  holding: %s" % (pol.get("rebalance_cadence_sessions"),
                                           pol.get("evaluation_horizon_sessions")))
    print("identity  : %s" % (pol.get("identity") or {}).get("identity_hash"))
    return TOKEN_POLICY_OK if out.get("outcome") in (PD.FROZEN, PD.ALREADY_FROZEN) else TOKEN_REFUSED


def seed_store(args) -> str:
    scope = _scope()
    if not scope:
        print("REFUSED: no scope (freeze the record first)")
        return TOKEN_REFUSED
    out = FTR.seed_store_from_frozen(scope)
    print(json.dumps({k: (len(v) if isinstance(v, list) else v) for k, v in out.items()}, indent=1))
    return TOKEN_STORE_OK if not out["missing_in_frozen_store"] else TOKEN_REFUSED


def refresh(args) -> str:
    scope = _scope()
    if not scope:
        print("REFUSED: no scope (freeze the record first)")
        return TOKEN_REFUSED
    ref = FTR.run_refresh("scope")
    print("refresh   : ok=%s %s" % (ref.get("ok"), ref.get("refresh")))
    if not ref.get("ok"):
        print(ref.get("stderr_tail"))
        return TOKEN_REFUSED
    pub = FTR.published_sessions(scope)
    print("newest published session: %s" % (pub[-1] if pub else None))
    return TOKEN_STORE_OK


def advance(args) -> str:
    if args.execute and args.now:
        print("REFUSED: --now may not be combined with --execute; a decision is frozen on the "
              "machine clock or not at all")
        return TOKEN_REFUSED
    res = FTR.advance(now=args.now, execute=bool(args.execute))
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


def show(args) -> str:
    res = FTC.resolve()
    print("record frozen        : %s (%s)" % (res.get("ok"), res.get("record_path")))
    if res.get("ok"):
        rec = res["record"]
        print("classification       : %s" % rec.get("classification"))
        print("instruments          : %d" % len(rec["forward_specification"].get("instruments") or []))
    pol = PD.load_policy(FTC.CHALLENGER_ID)
    held = PD.list_decisions(FTC.CHALLENGER_ID)
    scope = _scope()
    pub = FTR.published_sessions(scope) if scope else []
    ident = (pol or {}).get("identity") or _identity()
    print("policy declared      : %s" % bool(pol))
    print("decisions frozen     : %d %s" % (len(held), [r.get("eligible_session") for r in held]))
    print("newest marks session : %s" % (pub[-1] if pub else None))
    reg = ident.get("registration_session")
    if reg and pub:
        for row in FTR.schedule_preview(registration_session=reg, published=pub)[:3]:
            print("next boundary        : entry %(entry_session)s (newest published "
                  "%(newest_published_session)s, index %(rebalance_index)s)" % row)
    return "FUTURES_TREND_SHOWN"


def _child_refresh(args) -> int:
    body = FTR.refresh_store_in_child(args.child_refresh, _scope())
    print(json.dumps({k: body.get(k) for k in ("ok", "reason", "n_markets", "n_failed", "failed")}))
    return 0 if body.get("ok") else 3


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--validate", action="store_true")
    ap.add_argument("--freeze-record", action="store_true")
    ap.add_argument("--declare-policy", action="store_true")
    ap.add_argument("--seed-store", action="store_true")
    ap.add_argument("--refresh", action="store_true")
    ap.add_argument("--advance", action="store_true")
    ap.add_argument("--show", action="store_true")
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--now", default=None)
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--child-refresh", choices=("scope",), default=None)
    args = ap.parse_args(argv)
    assert_research_root_is_not_live()

    if args.child_refresh:
        return _child_refresh(args)
    if args.validate:
        token = validate(args)
    elif args.freeze_record:
        token = freeze_record(args)
    elif args.declare_policy:
        token = declare(args)
    elif args.seed_store:
        token = seed_store(args)
    elif args.refresh:
        token = refresh(args)
    elif args.advance:
        token = advance(args)
    elif args.show:
        token = show(args)
    else:
        ap.print_help()
        return 0
    print(token)
    return 1 if token == TOKEN_REFUSED or token == FTR.ST_BLOCKED else 0


if __name__ == "__main__":                                       # pragma: no cover
    raise SystemExit(main())
