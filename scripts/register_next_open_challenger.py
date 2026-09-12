r"""scripts/register_next_open_challenger.py - THE operator entrypoint that
starts TRUE_FORWARD evidence for REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1.

WHY A SECOND SCRIPT AND NOT A FLAG ON THE FIRST
-----------------------------------------------
``scripts/register_reversed_skew_challenger.py`` is the entrypoint for the
SAME-SESSION challenger, and that challenger is frozen. Teaching it a second
identity would mean editing the operator surface of a frozen registration, and
the thing most worth protecting here is that the two challengers cannot be
confused with each other. They are separate scripts for the same reason they are
separate registrations.

WHAT THIS CHALLENGER INHERITS FROM ITS SIBLING: THE SIGNAL, AND NOTHING ELSE
---------------------------------------------------------------------------
Same feature, same sign, same z-score, same lookback, same option construction,
same horizon - all read out of the sibling's frozen specification rather than
retyped. It inherits NO evidence: historical qualification NONE, forward
observations ZERO, capital eligible NO. The +26.65 %/yr next-open figure was
measured on the sample the sign was discovered on; it sized this hypothesis and
cannot test it.

WHAT REGISTRATION IS
--------------------
It starts a MEASUREMENT, through the SAME governed owner the sibling was
registered through:

    api.prospective_adoption.adopt_prospective_freeze   <- the ONE owner
        -> re-checks the lifecycle
        -> builds the identity and its hash
        -> refuses any backdated observation clock
        -> delegates to api.forward_challenger_registry

This script owns no lifecycle, identity, calendar or registration rule of its
own, and there is deliberately no ``--effective-from``, ``--date`` or
``--backfill`` argument: the boundary is derived internally and is always today.

SAFETY
------
Default is DRY RUN. A live write requires BOTH ``--confirm
CONFIRM_PROSPECTIVE_FORWARD_ADOPTION`` AND ``--execute``. Retry is idempotent:
the identity hash is the key. No promotion, no capital, no order, no fill.

TERMINAL TOKENS (exactly one is printed, on the last line)
----------------------------------------------------------
    NEXT_OPEN_REGISTRATION_DRY_RUN_OK
    NEXT_OPEN_REGISTRATION_OK
    NEXT_OPEN_REGISTRATION_REFUSED
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
# WORKTREE IMPORT INTEGRITY
#
# The venv's editable finder hard-maps ``paper_trader`` to
# C:\Users\binis\paper_trader and sits in ``sys.meta_path`` AHEAD of
# ``sys.path``. Run from a worktree, this entrypoint would otherwise register a
# freeze built here through the LIVE tree's adoption code. In the canonical
# checkout the directory IS named ``paper_trader`` and this block is a no-op.
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

from alpha_agent.alpha_recovery import next_open_challenger as NOC  # noqa: E402
from alpha_agent.alpha_recovery import options_acquisition as OA    # noqa: E402
from alpha_agent.alpha_recovery import options_surface as OS        # noqa: E402
from alpha_agent.alpha_recovery import write_artifact               # noqa: E402
from paper_trader.api import forward_challenger_registry as FCR  # noqa: E402
from paper_trader.api import prospective_adoption as PA          # noqa: E402

ENTRYPOINT = "scripts/register_next_open_challenger.py"
TOKEN_OK = "NEXT_OPEN_REGISTRATION_OK"
TOKEN_DRY_RUN_OK = "NEXT_OPEN_REGISTRATION_DRY_RUN_OK"
TOKEN_REFUSED = "NEXT_OPEN_REGISTRATION_REFUSED"
TERMINAL_TOKENS = (TOKEN_OK, TOKEN_DRY_RUN_OK, TOKEN_REFUSED)


def _registration_row(registry_dir_override=None) -> dict:
    """What the CANONICAL registrar holds, read verbatim from the registrar."""
    for rec in FCR.load_registrations(registry_dir_override):
        if str((rec.get("identity") or {}).get("challenger_id")) == NOC.CHALLENGER_ID:
            return FCR.registration_row(rec)
    return {}


def _sibling_row(registry_dir_override=None) -> dict:
    """The sibling's registration, read ONLY to prove it was not touched."""
    for rec in FCR.load_registrations(registry_dir_override):
        if str((rec.get("identity") or {}).get("challenger_id")) == NOC.SIBLING_CHALLENGER_ID:
            return FCR.registration_row(rec)
    return {}


def plan(registry_dir_override=None) -> dict:
    """DRY RUN: resolve, classify, preview the clock. Writes nothing."""
    row = NOC.freeze_row()
    boundary = PA.current_prospective_boundary()
    lifecycle = PA.classify_lifecycle(row)
    klass = PA.classify_challenger_class(row)
    identity = PA.build_adoption_identity(
        row, instrument_scope=[OA.UNDERLYING], venue=NOC.VENUE, sleeve=OS.FAMILY,
        cost_model=row["spec_json"]["cost_model"], mark_owner=NOC.MARK_OWNER)
    clock = FCR.resolve_observation_clock(
        asset_class=NOC.ASSET_CLASS, effective_from=boundary,
        horizon_sessions=row["horizon_sessions"])
    first_info = clock.get("first_eligible_observation_session")
    first_entry = NOC.entry_session_for(first_info) if first_info else None
    return {
        "entrypoint": ENTRYPOINT,
        "challenger_id": NOC.CHALLENGER_ID,
        "sibling_challenger_id": NOC.SIBLING_CHALLENGER_ID,
        "freeze_id": row["hypothesis_id"],
        "specification_hash": NOC.specification_hash(),
        "scope": NOC.REGISTRATION_SCOPE,
        "prospective_boundary": boundary,
        "lifecycle": lifecycle,
        "adoptable": bool(lifecycle.get("adoptable")),
        "challenger_class": klass,
        "canonical_registrar": PA.REGISTRARS.get(klass),
        "lifecycle_owner": PA.COMPOSITION_OWNER,
        "identity_hash": identity.get("identity_hash"),
        "observation_clock_preview": clock,
        # The registrar's clock is an exchange-session FLOOR. This challenger
        # enters one session LATER than that floor by construction, and saying
        # so here is what stops a reader treating the registrar's first eligible
        # session as this challenger's first entry.
        "challenger_entry_clock": {
            "registrar_first_eligible_session": first_info,
            "first_information_session": first_info,
            "first_entry_session": first_entry,
            "first_maturity_session": (NOC.maturity_session_for(first_entry)
                                       if first_entry else None),
            "entry_boundary": NOC.EXECUTION_BOUNDARY,
            "why_later_than_the_registrar_floor": (
                "the registrar resolves the first session on which anything may "
                "be observed; this challenger's first ENTRY is the eligible "
                "session after its first INFORMATION session, which is strictly "
                "later and therefore never earlier than the floor"),
        },
        "evidence_at_inception": NOC.EVIDENCE_AT_INCEPTION,
        "historical_qualification_inherited": False,
        "new_subscription_cost_usd": NOC.NEW_SUBSCRIPTION_COST_USD,
        "registry_dir": str(FCR.registry_dir(registry_dir_override)),
        "already_registered": bool(_registration_row(registry_dir_override)),
        "sibling_registration_before": _sibling_row(registry_dir_override),
        "confirm_token_required": PA.ADOPT_CONFIRM_TOKEN,
    }


def register(*, confirm: str | None, registry_dir_override=None,
             adoption_dir_override=None) -> dict:
    """Hand the freeze to the ONE governed owner, exactly once."""
    row = NOC.freeze_row()
    before = _sibling_row(registry_dir_override)
    boundary = PA.current_prospective_boundary()
    lifecycle = PA.classify_lifecycle(row)
    res = PA.adopt_prospective_freeze(
        freeze_row=row, observation_clock_starts=boundary, confirm=confirm,
        lifecycle=lifecycle, instrument_scope=[OA.UNDERLYING], venue=NOC.VENUE,
        sleeve=OS.FAMILY, cost_model=row["spec_json"]["cost_model"],
        mark_owner=NOC.MARK_OWNER, adoption_dir_override=adoption_dir_override,
        registry_dir_override=registry_dir_override)
    out = dict(plan(registry_dir_override))
    out["outcome"] = res.get("outcome")
    out["adopted"] = bool(res.get("adopted"))
    out["adoption_owner_result"] = {k: v for k, v in res.items() if k != "safety"}
    out["registration"] = _registration_row(registry_dir_override)
    after = _sibling_row(registry_dir_override)
    out["sibling_registration_after"] = after
    out["sibling_unchanged"] = (before == after)
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--confirm", default=None,
                    help="must be %s for a live write" % PA.ADOPT_CONFIRM_TOKEN)
    ap.add_argument("--execute", action="store_true",
                    help="perform the registration; without it this is a dry run")
    ap.add_argument("--registry-dir", default=None,
                    help="override the registry directory (tests only)")
    ap.add_argument("--adoption-dir", default=None,
                    help="override the adoption intent directory (tests only)")
    ap.add_argument("--json", action="store_true", help="print the report as JSON")
    args = ap.parse_args(argv)

    if not args.execute:
        body = plan(args.registry_dir)
        body["state"] = "DRY_RUN"
        token = TOKEN_DRY_RUN_OK
    elif args.confirm != PA.ADOPT_CONFIRM_TOKEN:
        body = plan(args.registry_dir)
        body["state"] = "REFUSED_CONFIRMATION"
        body["reason"] = ("a live registration requires --confirm %s AND --execute"
                          % PA.ADOPT_CONFIRM_TOKEN)
        token = TOKEN_REFUSED
    else:
        body = register(confirm=args.confirm, registry_dir_override=args.registry_dir,
                        adoption_dir_override=args.adoption_dir)
        body["state"] = body.get("outcome")
        token = TOKEN_OK if body.get("adopted") else TOKEN_REFUSED

    body["promotion_performed"] = False
    body["capital_allocated"] = False
    body["holdings_changed"] = False
    body["orders_created"] = False
    body["historical_qualification_inherited"] = False
    body["true_forward_observations"] = 0
    if args.registry_dir is None and args.adoption_dir is None and args.execute:
        write_artifact(NOC.REGISTRATION_ARTIFACT, body)

    if args.json:
        print(json.dumps(body, indent=1, sort_keys=True, default=str))
    else:
        print("challenger        : %s" % body["challenger_id"])
        print("sibling (untouched): %s" % body["sibling_challenger_id"])
        print("freeze id         : %s" % body["freeze_id"])
        print("spec hash         : %s" % body["specification_hash"])
        print("lifecycle         : %s" % (body["lifecycle"] or {}).get("lifecycle_state"))
        print("registrar         : %s" % body["canonical_registrar"])
        print("identity hash     : %s" % body["identity_hash"])
        clk = body["challenger_entry_clock"]
        print("first information : %s" % clk.get("first_information_session"))
        print("first ENTRY       : %s  (at the %02d:%02d ET open)"
              % ((clk.get("first_entry_session"),) + NOC.ENTRY_MARK_ET))
        print("first maturity    : %s" % clk.get("first_maturity_session"))
        print("hist qualification: NONE (nothing inherited from the sibling)")
        print("forward obs       : 0")
        print("new subscription  : $%.2f" % body["new_subscription_cost_usd"])
        print("state             : %s" % body.get("state"))
        reg = body.get("registration") or {}
        if reg:
            print("evidence status   : %s" % reg.get("evidence_status"))
            print("matured           : %s" % reg.get("matured_observations"))
            print("promotion ready   : %s" % reg.get("promotion_ready"))
        if "sibling_unchanged" in body:
            print("sibling unchanged : %s" % body["sibling_unchanged"])
    print(token)
    return 0 if token in (TOKEN_OK, TOKEN_DRY_RUN_OK) else 1


if __name__ == "__main__":                                       # pragma: no cover
    raise SystemExit(main())
