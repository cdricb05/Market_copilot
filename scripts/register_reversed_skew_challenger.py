r"""scripts/register_reversed_skew_challenger.py - THE operator entrypoint that
starts TRUE_FORWARD evidence for the ONE frozen reversed-skew challenger.

WHY A SCRIPT AND NOT A FUNCTION IN THE CAMPAIGN PACKAGE
-------------------------------------------------------
``alpha_agent/alpha_recovery`` may never call a registrar or an adopter and may
never import ``paper_trader.api``. The campaign contract pins that and
``test_package_never_calls_a_registrar_adopter_or_portfolio_owner`` enforces it
file by file. The separation is the safety property, not bureaucracy: research
produces an immutable freeze record and an exact command, and a human runs the
entrypoint that performs the governed act. Research that could register itself
would be one loop away from research that promotes itself.

So the research package owns :func:`alpha_agent.alpha_recovery.reversed_skew.
freeze_row` and this script owns the call.

WHY NOT ``scripts/adopt_prospective_freeze.py``
------------------------------------------------
That entrypoint resolves challengers out of the canonical R59 ResearchMemory,
and this freeze is not in ResearchMemory - it is a campaign freeze record. It
also refuses outright to run from a worktree, because it would otherwise execute
the DEPLOYED checkout's adoption code against whatever stores the caller's
environment points at. Neither refusal is worked around here: this script hands
the freeze row to the SAME governed owner that entrypoint hands its rows to,
with no lifecycle, identity, calendar or registration rule of its own.

    api.prospective_adoption.adopt_prospective_freeze   <- the ONE owner
        -> re-checks the lifecycle
        -> builds the identity and its hash
        -> refuses any backdated observation clock
        -> delegates to api.forward_challenger_registry

WHAT REGISTRATION IS, AND IS NOT
--------------------------------
It starts a MEASUREMENT. The registrar itself records that it writes no forward
prediction, no forward outcome and no PnL, promotes no model, activates no
sleeve, allocates no capital, changes no holding, cash or NAV, creates no order
or fill, approves nothing and enables no automation. Adoption of the model
remains human-gated in every case.

NO BACKFILL, AND NO WAY TO ASK FOR ONE
--------------------------------------
There is deliberately no ``--effective-from``, ``--date`` or ``--backfill``
argument and there never may be. The prospective boundary is derived internally
by ``api.prospective_adoption.current_prospective_boundary`` and is always today;
the first legitimate observation is the first eligible session STRICTLY AFTER
registration, resolved on the authoritative exchange calendar. A session that
completed before registration can never become a prediction.

SAFETY
------
Default is DRY RUN. A live write requires BOTH ``--confirm
CONFIRM_PROSPECTIVE_FORWARD_ADOPTION`` AND ``--execute``. Retry is idempotent:
the identity hash is the key, so a second run resolves the SAME registration
rather than a second one.

TERMINAL TOKENS (exactly one is printed, on the last line)
----------------------------------------------------------
    REVERSED_SKEW_REGISTRATION_DRY_RUN_OK
    REVERSED_SKEW_REGISTRATION_OK
    REVERSED_SKEW_REGISTRATION_REFUSED
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

from alpha_agent.alpha_recovery import options_acquisition as OA  # noqa: E402
from alpha_agent.alpha_recovery import options_surface as OS      # noqa: E402
from alpha_agent.alpha_recovery import reversed_skew as RS        # noqa: E402
from alpha_agent.alpha_recovery import write_artifact             # noqa: E402
from paper_trader.api import forward_challenger_registry as FCR  # noqa: E402
from paper_trader.api import prospective_adoption as PA          # noqa: E402

ENTRYPOINT = "scripts/register_reversed_skew_challenger.py"
TOKEN_OK = "REVERSED_SKEW_REGISTRATION_OK"
TOKEN_DRY_RUN_OK = "REVERSED_SKEW_REGISTRATION_DRY_RUN_OK"
TOKEN_REFUSED = "REVERSED_SKEW_REGISTRATION_REFUSED"
TERMINAL_TOKENS = (TOKEN_OK, TOKEN_DRY_RUN_OK, TOKEN_REFUSED)


def _registration_row(registry_dir_override=None) -> dict:
    """What the CANONICAL registrar holds, read verbatim from the registrar.

    This script computes no maturity, no session and no evidence word of its own.
    """
    for rec in FCR.load_registrations(registry_dir_override):
        if str((rec.get("identity") or {}).get("challenger_id")) == RS.CHALLENGER_ID:
            return FCR.registration_row(rec)
    return {}


def plan(registry_dir_override=None) -> dict:
    """DRY RUN: resolve, classify, preview the clock. Writes nothing."""
    row = RS.freeze_row()
    boundary = PA.current_prospective_boundary()
    lifecycle = PA.classify_lifecycle(row)
    klass = PA.classify_challenger_class(row)
    identity = PA.build_adoption_identity(
        row, instrument_scope=[OA.UNDERLYING], venue=RS.VENUE, sleeve=OS.FAMILY,
        cost_model=row["spec_json"]["cost_model"], mark_owner=RS.MARK_OWNER)
    return {
        "entrypoint": ENTRYPOINT,
        "challenger_id": RS.CHALLENGER_ID,
        "freeze_id": row["hypothesis_id"],
        "scope": RS.REGISTRATION_SCOPE,
        "prospective_boundary": boundary,
        "lifecycle": lifecycle,
        "adoptable": bool(lifecycle.get("adoptable")),
        "challenger_class": klass,
        "canonical_registrar": PA.REGISTRARS.get(klass),
        "lifecycle_owner": PA.COMPOSITION_OWNER,
        "identity_hash": identity.get("identity_hash"),
        "observation_clock_preview": FCR.resolve_observation_clock(
            asset_class=RS.ASSET_CLASS, effective_from=boundary,
            horizon_sessions=RS.FROZEN_HORIZON),
        "registry_dir": str(FCR.registry_dir(registry_dir_override)),
        "already_registered": bool(_registration_row(registry_dir_override)),
        "confirm_token_required": PA.ADOPT_CONFIRM_TOKEN,
    }


def register(*, confirm: str | None, registry_dir_override=None,
             adoption_dir_override=None) -> dict:
    """Hand the freeze to the ONE governed owner, exactly once."""
    row = RS.freeze_row()
    boundary = PA.current_prospective_boundary()
    lifecycle = PA.classify_lifecycle(row)
    res = PA.adopt_prospective_freeze(
        freeze_row=row, observation_clock_starts=boundary, confirm=confirm,
        lifecycle=lifecycle, instrument_scope=[OA.UNDERLYING], venue=RS.VENUE,
        sleeve=OS.FAMILY, cost_model=row["spec_json"]["cost_model"],
        mark_owner=RS.MARK_OWNER, adoption_dir_override=adoption_dir_override,
        registry_dir_override=registry_dir_override)
    out = dict(plan(registry_dir_override))
    out["outcome"] = res.get("outcome")
    out["adopted"] = bool(res.get("adopted"))
    out["adoption_owner_result"] = {k: v for k, v in res.items() if k != "safety"}
    out["registration"] = _registration_row(registry_dir_override)
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
    ap.add_argument("--json", action="store_true", help="print the full report as JSON")
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
    if args.registry_dir is None and args.adoption_dir is None and args.execute:
        write_artifact(RS.REGISTRATION_ARTIFACT, body)

    if args.json:
        print(json.dumps(body, indent=1, sort_keys=True, default=str))
    else:
        print("challenger      : %s" % body["challenger_id"])
        print("freeze id       : %s" % body["freeze_id"])
        print("lifecycle       : %s" % (body["lifecycle"] or {}).get("lifecycle_state"))
        print("registrar       : %s" % body["canonical_registrar"])
        print("identity hash   : %s" % body["identity_hash"])
        clock = body["observation_clock_preview"]
        print("first observation : %s" % clock.get("first_eligible_observation_session"))
        print("first maturity    : %s" % clock.get("next_expected_maturity_session"))
        print("backfilled        : %s" % clock.get("backfilled"))
        print("state           : %s" % body.get("state"))
        reg = body.get("registration") or {}
        if reg:
            print("evidence status : %s" % reg.get("evidence_status"))
            print("matured         : %s" % reg.get("matured_observations"))
            print("promotion ready : %s" % reg.get("promotion_ready"))
    print(token)
    return 0 if token in (TOKEN_OK, TOKEN_DRY_RUN_OK) else 1


if __name__ == "__main__":                                       # pragma: no cover
    raise SystemExit(main())
