r"""scripts/register_fx_carry_cadence_challenger.py - the operator entrypoint that
starts TRUE_FORWARD evidence for ALPHA_RECOVERY_FX_CARRY_CADENCE_H1_F9B1ACA7.

WHY A SEPARATE DOOR
-------------------
``scripts/adopt_prospective_freeze.py`` resolves freezes held in ResearchMemory.
This record never was one: ``alpha_agent.alpha_recovery.forward_package`` wrote it
as an immutable file named by its record hash. Like the next-open challenger, it
therefore gets its own named door, and that door owns no rule:

    alpha_agent.alpha_recovery.fx_carry_cadence_challenger   reads + verifies the record
    api.prospective_adoption (the ONE governed owner)        lifecycle, identity, no backdating
        -> api.forward_challenger_registry                   clock, first-write-wins record

AUTHORISED SCOPE
----------------
A human authorised prospective TRUE_FORWARD evidence collection for this exact
identity on 2026-09-14. Not model promotion, not capital eligibility, not
allocation, not a portfolio change, not an order and not an execution instruction.

NO DUPLICATE, NO BACKFILL
-------------------------
If ANY canonical registration already carries this challenger id, nothing is
called: the same identity reports ALREADY_REGISTERED, and a different identity
under the same id is refused rather than registered twice. The prospective boundary
is derived by the owner (today); there is no argument through which an earlier one
could be named.

SAFETY
------
Default is DRY RUN. A live write requires BOTH ``--confirm
CONFIRM_PROSPECTIVE_FORWARD_ADOPTION`` AND ``--execute``.

TERMINAL TOKENS (exactly one is printed, on the last line)
----------------------------------------------------------
    FX_CARRY_REGISTRATION_DRY_RUN_OK
    FX_CARRY_REGISTRATION_OK
    FX_CARRY_REGISTRATION_REFUSED
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_PKG_PARENT = str(_REPO.parent)
if _PKG_PARENT not in sys.path:
    sys.path.insert(0, _PKG_PARENT)
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

# ---------------------------------------------------------------------------
# WORKTREE IMPORT INTEGRITY - the same guard as the next-open entrypoint: the
# venv's editable finder would otherwise run the LIVE tree's adoption code.
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

    _already = sys.modules.get("paper_trader")
    if _already is None:
        sys.meta_path.insert(0, _WorktreePaperTraderFinder)
    elif Path(getattr(_already, "__file__", "") or "").resolve().parent != _REPO.resolve():
        raise RuntimeError(                              # pragma: no cover
            "paper_trader was imported from %s before the worktree finder was "
            "installed; this entrypoint would run against a different checkout"
            % getattr(_already, "__file__", "?"))

from alpha_agent.alpha_recovery import fx_carry_cadence_challenger as FXC  # noqa: E402
from alpha_agent.alpha_recovery import write_artifact                     # noqa: E402
from paper_trader.api import forward_challenger_registry as FCR  # noqa: E402
from paper_trader.api import prospective_adoption as PA          # noqa: E402

ENTRYPOINT = "scripts/register_fx_carry_cadence_challenger.py"
TOKEN_OK = "FX_CARRY_REGISTRATION_OK"
TOKEN_DRY_RUN_OK = "FX_CARRY_REGISTRATION_DRY_RUN_OK"
TOKEN_REFUSED = "FX_CARRY_REGISTRATION_REFUSED"
TERMINAL_TOKENS = (TOKEN_OK, TOKEN_DRY_RUN_OK, TOKEN_REFUSED)

D_WOULD_REGISTER = "WOULD_REGISTER"
D_ALREADY_REGISTERED = "ALREADY_REGISTERED"
D_REFUSED_DUPLICATE = "REFUSED_A_DIFFERENT_IDENTITY_ALREADY_CARRIES_THIS_CHALLENGER_ID"
D_REFUSED_RECORD = "REFUSED_RECORD_NOT_VERIFIED"


def _registrations(registry_dir_override=None) -> list:
    """Every canonical registration carrying this challenger id, read verbatim."""
    return [r for r in FCR.load_registrations(registry_dir_override)
            if str(r.get("challenger_id")) == FXC.CHALLENGER_ID]


def _identity_args(row: dict, resolved: dict) -> dict:
    return {"instrument_scope": list(resolved["universe"]["instruments"]), "venue": FXC.VENUE,
            "sleeve": row["spec_json"]["sleeve"], "cost_model": row["spec_json"]["cost_model"]}


def plan(registry_dir_override=None) -> dict:
    """DRY RUN: resolve, verify, classify, preview. Writes nothing."""
    resolved = FXC.resolve()
    base = {"entrypoint": ENTRYPOINT, "challenger_id": FXC.CHALLENGER_ID,
            "record_path": resolved.get("record_path"), "record_problems": resolved.get("problems"),
            "authorisation": FXC.AUTHORISATION, "evidence_at_inception": FXC.EVIDENCE_AT_INCEPTION,
            "forward_emission_requirement": FXC.FORWARD_EMISSION_REQUIREMENT,
            "registry_dir": str(FCR.registry_dir(registry_dir_override)),
            "confirm_token_required": PA.ADOPT_CONFIRM_TOKEN}
    existing = _registrations(registry_dir_override)
    if not resolved.get("ok"):
        return {**base, "disposition": D_REFUSED_RECORD, "existing_registrations": len(existing)}
    row = FXC.freeze_row(resolved)
    boundary = PA.current_prospective_boundary()
    lifecycle = PA.classify_lifecycle(row)
    klass = PA.classify_challenger_class(row)
    identity = PA.build_adoption_identity(row, **_identity_args(row, resolved))
    clock = FCR.resolve_observation_clock(asset_class=row["asset_class"], effective_from=boundary,
                                          horizon_sessions=row["horizon_sessions"])
    same = [r for r in existing if (r.get("identity") or {}).get("identity_hash")
            == identity["identity_hash"]]
    disposition = (D_ALREADY_REGISTERED if same else
                   D_REFUSED_DUPLICATE if existing else D_WOULD_REGISTER)
    return {**base, "disposition": disposition, "freeze_id": row["hypothesis_id"],
            "freeze_record_hash": FXC.FREEZE_RECORD_HASH, "record_hash": FXC.RECORD_HASH,
            "instrument_scope": identity["instrument_scope"], "asset_class": row["asset_class"],
            "release": row["release"], "model_family": row["model_family"],
            "prospective_boundary": boundary, "lifecycle": lifecycle,
            "adoptable": bool(lifecycle.get("adoptable")), "challenger_class": klass,
            "canonical_registrar": PA.REGISTRARS.get(klass), "lifecycle_owner": PA.COMPOSITION_OWNER,
            "identity_hash": identity["identity_hash"], "observation_clock_preview": clock,
            "existing_registrations": len(existing),
            "existing_identity_hashes": [(r.get("identity") or {}).get("identity_hash")
                                         for r in existing],
            "_row": row, "_resolved": resolved}


def register(*, confirm, registry_dir_override=None, adoption_dir_override=None) -> dict:
    """Hand the verified freeze to the ONE governed owner, at most once."""
    p = plan(registry_dir_override)
    if p["disposition"] != D_WOULD_REGISTER:
        out = {k: v for k, v in p.items() if not k.startswith("_")}
        out["adopted"] = p["disposition"] == D_ALREADY_REGISTERED
        out["outcome"] = p["disposition"]
        out["owner_called"] = False
        rows = _registrations(registry_dir_override)
        out["registration"] = FCR.registration_row(rows[0]) if rows else {}
        return out
    row, resolved = p["_row"], p["_resolved"]
    res = PA.adopt_prospective_freeze(
        freeze_row=row, observation_clock_starts=PA.current_prospective_boundary(),
        confirm=confirm, lifecycle=PA.classify_lifecycle(row),
        adoption_dir_override=adoption_dir_override,
        registry_dir_override=registry_dir_override, **_identity_args(row, resolved))
    out = {k: v for k, v in plan(registry_dir_override).items() if not k.startswith("_")}
    out["outcome"] = res.get("outcome")
    out["adopted"] = bool(res.get("adopted"))
    out["owner_called"] = True
    out["adoption_owner_result"] = {k: v for k, v in res.items()
                                    if k not in ("safety", "outcome_vocabulary", "lifecycle_vocabulary")}
    rows = _registrations(registry_dir_override)
    out["registration"] = FCR.registration_row(rows[0]) if rows else {}
    return out


def proof(body: dict, registry_dir_override=None) -> dict:
    """The facts the authorisation asked to see, each read from the registrar."""
    rows = _registrations(registry_dir_override)
    rec = rows[0] if rows else {}
    return {
        "FX_TRUE_FORWARD_REGISTERED": "YES" if rows else "NO",
        "FX_DUPLICATE_REGISTRATION": "YES" if len(rows) > 1 else "NO",
        "FX_BACKFILL": int(bool(rec.get("backfilled"))) if rows else 0,
        "FX_FORWARD_OBSERVATIONS_AT_INCEPTION": int(rec.get("forward_observations_at_registration") or 0),
        "FX_PORTFOLIO_EFFECT": "NONE" if not any(
            (rec.get("safety") or {}).get(k) for k in ("allocated_capital", "changed_holdings",
                                                       "changed_cash", "changed_nav", "created_orders",
                                                       "created_fills")) else "PRESENT",
        "FX_AUTO_PROMOTION": "YES" if rec.get("automatic_promotion_allowed") else "NO",
        "registrations_carrying_this_id": len(rows),
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--confirm", default=None,
                    help="must be %s for a live write" % PA.ADOPT_CONFIRM_TOKEN)
    ap.add_argument("--execute", action="store_true",
                    help="perform the registration; without it this is a dry run")
    ap.add_argument("--registry-dir", default=None, help="override the registry directory (tests only)")
    ap.add_argument("--adoption-dir", default=None, help="override the adoption intent directory (tests only)")
    ap.add_argument("--json", action="store_true", help="print the report as JSON")
    args = ap.parse_args(argv)

    if not args.execute:
        body = {k: v for k, v in plan(args.registry_dir).items() if not k.startswith("_")}
        body["state"] = "DRY_RUN"
        token = TOKEN_DRY_RUN_OK if body["disposition"] != D_REFUSED_RECORD else TOKEN_REFUSED
    elif args.confirm != PA.ADOPT_CONFIRM_TOKEN:
        body = {k: v for k, v in plan(args.registry_dir).items() if not k.startswith("_")}
        body["state"] = "REFUSED_CONFIRMATION"
        body["reason"] = "a live registration requires --confirm %s AND --execute" % PA.ADOPT_CONFIRM_TOKEN
        token = TOKEN_REFUSED
    else:
        body = register(confirm=args.confirm, registry_dir_override=args.registry_dir,
                        adoption_dir_override=args.adoption_dir)
        body["state"] = body.get("outcome")
        token = TOKEN_OK if body.get("adopted") else TOKEN_REFUSED

    body["proof"] = proof(body, args.registry_dir)
    body["promotion_performed"] = False
    body["capital_allocated"] = False
    body["holdings_changed"] = False
    body["orders_created"] = False
    body["historical_qualification_inherited"] = False
    if args.registry_dir is None and args.adoption_dir is None and args.execute:
        write_artifact(FXC.REGISTRATION_ARTIFACT, body)

    if args.json:
        print(json.dumps(body, indent=1, sort_keys=True, default=str))
    else:
        for key in ("challenger_id", "disposition", "state", "freeze_id", "record_hash",
                    "freeze_record_hash", "identity_hash", "asset_class", "instrument_scope",
                    "canonical_registrar", "prospective_boundary"):
            print("%-24s: %s" % (key, body.get(key)))
        clk = body.get("observation_clock_preview") or {}
        print("%-24s: %s" % ("observation clock", clk.get("state")))
        print("%-24s: %s" % ("first eligible session", clk.get("first_eligible_observation_session")))
        print("%-24s: %s" % ("first maturity session", clk.get("next_expected_maturity_session")))
        reg = body.get("registration") or {}
        if reg:
            print("%-24s: %s" % ("registered at", reg.get("registration_timestamp")))
            print("%-24s: %s" % ("evidence status", reg.get("evidence_status")))
        if body.get("record_problems"):
            print("%-24s: %s" % ("record problems", body["record_problems"]))
        for k, v in body["proof"].items():
            print("%s = %s" % (k, v))
    print(token)
    return 0 if token in (TOKEN_OK, TOKEN_DRY_RUN_OK) else 1


if __name__ == "__main__":                                       # pragma: no cover
    raise SystemExit(main())
