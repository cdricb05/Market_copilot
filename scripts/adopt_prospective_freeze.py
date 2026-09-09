r"""scripts/adopt_prospective_freeze.py - THE operator entrypoint for adopting a
prospective freeze that ALREADY EXISTS (Release 62.1.1).

THE GAP THIS CLOSES
-------------------
R61 made "qualify a challenger" and "start its forward evidence" ONE governed
operation (:mod:`paper_trader.api.prospective_adoption`), and R62.1 gave that
operation a canonical registrar for signal challengers
(:mod:`paper_trader.api.forward_challenger_registry`). Both act at the moment a
freeze is CREATED. Neither can reach a freeze created before they existed:

    alpha_agent.r59.handlers.freeze_qualified()
        -> the challenger row is already in research memory
        -> returns ALREADY_FROZEN, BEFORE the adoption owner is called

so re-running the research worker could never start the forward clock of a
freeze that was already there. A live read-only preflight on 2026-09-08 proved
the consequence exactly:

    canonical_forward_registration_count      = 0
    orphan_freezes_adoptable_count            = 4
    orphan_freezes_closed_by_lifecycle_count  = 1

Four ACTIVE R58 freezes, adoptable, with an immutable identity and no owner
accruing a single observation for them - and no governed way for an operator to
say "adopt these four".

WHAT THIS SCRIPT IS
-------------------
The ONE operator entrypoint for that act, and nothing else. It owns:

    * no lifecycle rule           (api.prospective_adoption.classify_lifecycle)
    * no identity calculation     (api.prospective_adoption.build_adoption_identity)
    * no observation calendar     (api.forward_challenger_registry)
    * no registration rule        (api.forward_challenger_registry)
    * no evidence rule            (the accrual owner the registrar names)
    * no promotion rule           (a human, through the existing governance)
    * no portfolio rule           (nothing here can reach one)

It resolves operator-named challenger ids against the canonical ResearchMemory
through its READ-ONLY handle, and hands each resolved freeze to
``api.prospective_adoption.adopt_prospective_freeze`` EXACTLY ONCE. That owner
re-checks the lifecycle, computes the identity, writes a durable intent and
delegates the registration itself to the canonical registrar, which re-checks
the lifecycle a second time and resolves the observation clock from the asset's
own calendar.

NO BACKFILL, AND NO WAY TO ASK FOR ONE
--------------------------------------
There is deliberately no ``--effective-from``, ``--date``, ``--backfill`` or
``--inception-override`` argument, and there never may be: the prospective
boundary is derived internally by
``api.prospective_adoption.current_prospective_boundary`` and is always today.
The first legitimate observation is then the first eligible session STRICTLY
AFTER the registration session, resolved by the registrar on the authoritative
exchange calendar. A session that completed before registration can never become
a prediction, and no argument to this script can make it one.

NO ``--all``
------------
Adoption is named, one challenger at a time. ``--challenger-id`` is repeatable
and REQUIRED; there is no mode that sweeps the estate. An operator who cannot
name what they are adopting is not ready to adopt it.

SAFETY
------
Default is DRY RUN. A live write requires BOTH ``--confirm
ADOPT_PROSPECTIVE_FORWARD_CLOCKS`` AND ``--execute``. Adoption starts a
MEASUREMENT: it promotes no model, activates no sleeve, allocates no capital,
changes no holding, cash or NAV, creates no order or fill, approves nothing and
enables no automation. Retry is idempotent - the identity hash is the key, so a
second run resolves the SAME registration rather than a second one.

TERMINAL TOKENS (exactly one is printed, on the last line)
----------------------------------------------------------
    FORWARD_ADOPTION_DRY_RUN_OK   a dry run completed; nothing was written
    FORWARD_ADOPTION_OK           every requested adoption is registered
    FORWARD_ADOPTION_REFUSED      a governed refusal (lifecycle / unknown id /
                                  missing confirmation / missing write flag)
    FORWARD_ADOPTION_FAILED       an owner failed; intents remain resumable
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

from paper_trader.alpha_agent import r59  # noqa: E402
from paper_trader.alpha_agent.r59 import memory as M  # noqa: E402
from paper_trader.api import forward_challenger_registry as FCR  # noqa: E402
from paper_trader.api import prospective_adoption as PA  # noqa: E402

ENTRYPOINT = "scripts/adopt_prospective_freeze.py"

TOKEN_OK = "FORWARD_ADOPTION_OK"
TOKEN_DRY_RUN_OK = "FORWARD_ADOPTION_DRY_RUN_OK"
TOKEN_REFUSED = "FORWARD_ADOPTION_REFUSED"
TOKEN_FAILED = "FORWARD_ADOPTION_FAILED"
TERMINAL_TOKENS = (TOKEN_OK, TOKEN_DRY_RUN_OK, TOKEN_REFUSED, TOKEN_FAILED)

#: Per-challenger dispositions this entrypoint reports. Every one of them is a
#: RESTATEMENT of an owner's own outcome; none is decided here.
D_WOULD_ADOPT = "WOULD_ADOPT"
D_ALREADY_REGISTERED = "ALREADY_REGISTERED"
D_ADOPTED = "ADOPTED"
D_REFUSED = "REFUSED"
D_FAILED = "FAILED"


def assert_import_integrity() -> str:
    """Fail closed if ``paper_trader`` resolved to a DIFFERENT checkout.

    The venv carries an editable install whose finder hard-maps ``paper_trader``
    to the deployed checkout and sits in ``sys.meta_path`` ahead of every
    ``sys.path`` entry. In the deployed checkout that is exactly right and this
    is a no-op. Run from anywhere else - a git worktree, a copy - the finder
    silently captures the import, and this entrypoint would run the DEPLOYED
    tree's adoption code while reading whatever stores the caller's environment
    points at. That is not a risk worth taking for an operation that writes:
    ``alpha_agent.r59.assert_worktree_import`` exists for the same hazard on the
    research side, and this is the operator-facing half of it.
    """
    here = _REPO.resolve()
    got = Path(PA.__file__).resolve().parents[1]
    if got != here:
        raise RuntimeError(
            "import integrity: %s lives in %s but paper_trader resolved to %s "
            "- the editable-install finder captured the import, so this "
            "entrypoint would run a DIFFERENT checkout's adoption code. Run it "
            "from the checkout the package resolves to." % (ENTRYPOINT, here, got))
    return str(here)


def _mem_rows() -> list:
    """Every FORWARD_FROZEN row in the canonical memory, READ-ONLY.

    The read-only handle creates no directory, runs no schema script and
    refuses every mutation, so merely asking what is adoptable can never write
    to the store the question is about.
    """
    mem = M.open_memory_readonly()
    return mem.list_hypotheses(outcome=r59.HO_FORWARD_FROZEN, limit=5000)


def _registration_for(challenger_id) -> dict:
    """What the CANONICAL registrar already holds for this challenger.

    Read verbatim from the registrar. This script computes no maturity, no
    session and no evidence word of its own.
    """
    for rec in FCR.load_registrations():
        if str(rec.get("challenger_id")) == str(challenger_id):
            return FCR.registration_row(rec)
    return {}


def _clock_preview(freeze_row: dict, boundary: str) -> dict:
    """The prospective clock the registrar WOULD open. Pure; writes nothing."""
    return FCR.resolve_observation_clock(
        asset_class=(freeze_row or {}).get("asset_class"),
        effective_from=boundary,
        horizon_sessions=(freeze_row or {}).get("horizon_sessions"))


def _row_common(cid, resolution: dict) -> dict:
    freeze = resolution.get("freeze_row") or {}
    lifecycle = PA.classify_lifecycle(freeze) if freeze else {}
    klass = PA.classify_challenger_class(freeze) if freeze else None
    return {
        "challenger_id": cid,
        "freeze_id": resolution.get("freeze_id"),
        "hypothesis_id": resolution.get("freeze_id"),
        "resolution": resolution.get("outcome"),
        "lifecycle": lifecycle.get("lifecycle_state"),
        "lifecycle_evidence": lifecycle.get("evidence"),
        "lifecycle_detail": lifecycle.get("detail"),
        "adoptable": bool(lifecycle.get("adoptable")),
        "never_resurrectable": bool(lifecycle.get("never_resurrectable")),
        "challenger_class": klass,
        "canonical_registrar": PA.REGISTRARS.get(klass) if klass else None,
        "lifecycle_owner": PA.COMPOSITION_OWNER,
        "_freeze_row": freeze,
        "_lifecycle": lifecycle,
    }


def _from_registration(row: dict, reg: dict) -> dict:
    """Project the registrar's own record onto the operator report."""
    return dict(row, **{
        "already_registered": bool(reg),
        "registration_timestamp": reg.get("registration_timestamp"),
        "first_eligible_observation_session": reg.get(
            "first_eligible_observation_session"),
        "next_legitimate_maturity_session": reg.get(
            "next_legitimate_maturity_session"),
        "observation_calendar_owner": reg.get("observation_calendar_owner"),
        "observation_clock_state": reg.get("observation_clock_state"),
        "backfilled": bool(reg.get("backfilled")),
        "predictions_emitted": reg.get("predictions_emitted"),
        "matured_observations": reg.get("matured_observations"),
        "effective_independent_observations": reg.get(
            "effective_independent_observations"),
        "evidence_status": reg.get("evidence_status"),
        "evidence_accrual_owner": reg.get("evidence_accrual_owner"),
    })


def _plan_one(cid: str, rows: list, boundary: str) -> dict:
    """DRY RUN for one challenger. Resolves, classifies, previews - writes nothing."""
    resolution = PA.resolve_freeze_by_challenger_id(cid, rows)
    row = _row_common(cid, resolution)
    if not resolution.get("resolved"):
        return dict(row, disposition=D_REFUSED, refusal=resolution.get("outcome"),
                    reason=resolution.get("reason"))
    reg = _registration_for(cid)
    if reg:
        return dict(_from_registration(row, reg),
                    disposition=D_ALREADY_REGISTERED,
                    reason=("this exact challenger is already registered with "
                            "the canonical registrar; adopting again resolves "
                            "to the same registration"))
    if not row["adoptable"]:
        return dict(row, disposition=D_REFUSED,
                    refusal=PA.REFUSED_LIFECYCLE,
                    reason=("lifecycle state %s is not adoptable; only %s may "
                            "begin accruing forward evidence"
                            % (row["lifecycle"], ", ".join(PA.ADOPTABLE_STATES))))
    clock = _clock_preview(row["_freeze_row"], boundary)
    return dict(row, disposition=D_WOULD_ADOPT, already_registered=False,
                registration_timestamp=None,
                prospective_observation_boundary=boundary,
                first_eligible_observation_session=clock.get(
                    "first_eligible_observation_session"),
                next_legitimate_maturity_session=clock.get(
                    "next_expected_maturity_session"),
                observation_calendar_owner=clock.get("calendar_owner"),
                observation_clock_state=clock.get("state"),
                backfilled=False, predictions_emitted=0,
                matured_observations=0,
                effective_independent_observations=0,
                evidence_status=None,
                reason=("an ACTIVE freeze with no canonical registration; "
                        "--execute would register it prospectively from %s"
                        % boundary))


def _adopt_one(plan: dict, boundary: str) -> dict:
    """EXECUTE for one challenger. Calls the governed owner exactly once."""
    if plan["disposition"] in (D_REFUSED, D_ALREADY_REGISTERED):
        return plan
    out = PA.adopt_prospective_freeze(
        freeze_row=plan["_freeze_row"],
        observation_clock_starts=boundary,
        # The governed operation's OWN in-process token. The operator token is
        # a separate authorisation and is verified before this call; neither can
        # satisfy the other.
        confirm=PA.ADOPT_CONFIRM_TOKEN,
        lifecycle=plan["_lifecycle"])
    outcome = out.get("outcome")
    if outcome in (PA.ADOPTED, PA.ALREADY_ADOPTED):
        reg = _registration_for(plan["challenger_id"])
        return dict(_from_registration(plan, reg),
                    disposition=(D_ADOPTED if outcome == PA.ADOPTED
                                 else D_ALREADY_REGISTERED),
                    adoption_outcome=outcome,
                    prospective_observation_boundary=boundary,
                    reason=out.get("reason") or "registered prospectively")
    if outcome in (PA.REFUSED_LIFECYCLE, PA.REFUSED_IDENTITY,
                   PA.REFUSED_BACKDATED, PA.REFUSED_CONFIRMATION):
        return dict(plan, disposition=D_REFUSED, refusal=outcome,
                    adoption_outcome=outcome, reason=out.get("reason"))
    return dict(plan, disposition=D_FAILED, adoption_outcome=outcome,
                intent_is_resumable=bool(out.get("intent_is_resumable")),
                reason=out.get("reason") or outcome)


_COLUMNS = (
    ("challenger_id", "Challenger"),
    ("freeze_id", "Freeze / hypothesis"),
    ("resolution", "Resolution"),
    ("lifecycle", "Lifecycle"),
    ("canonical_registrar", "Canonical registrar"),
    ("disposition", "Outcome"),
    ("refusal", "Refused because"),
    ("registration_timestamp", "Registered at"),
    ("first_eligible_observation_session", "First eligible observation"),
    ("next_legitimate_maturity_session", "Next legitimate maturity"),
    ("backfilled", "Backfilled"),
    ("predictions_emitted", "Predictions emitted"),
    ("matured_observations", "Matured observations"),
    ("effective_independent_observations", "Effective independent obs"),
)


def _print_row(row: dict) -> None:
    print("")
    for key, label in _COLUMNS:
        value = row.get(key)
        print("  %-28s %s" % (label + ":",
                              "-" if value is None else value))
    if row.get("reason"):
        print("  %-28s %s" % ("Why:", row["reason"]))


def _public(row: dict) -> dict:
    return {k: v for k, v in row.items() if not k.startswith("_")}


def main(argv=None) -> int:
    try:
        assert_import_integrity()
    except RuntimeError as exc:
        print("=" * 78)
        print("PROSPECTIVE FORWARD ADOPTION - REFUSED BEFORE PARSING")
        print("=" * 78)
        print("  %s" % exc)
        print("  nothing was read and nothing was written.")
        print("\n%s" % TOKEN_FAILED)
        return 1
    ap = argparse.ArgumentParser(
        prog=ENTRYPOINT,
        description=("Adopt an ALREADY-EXISTING prospective freeze into "
                     "canonical forward evidence. Dry run by default; a live "
                     "write requires --confirm %s AND --execute. There is no "
                     "adopt-all mode and no way to name a historical "
                     "observation boundary."
                     % PA.OPERATOR_ADOPT_CONFIRM_TOKEN))
    ap.add_argument(
        "--challenger-id", dest="challenger_ids", action="append",
        required=True, metavar="ID",
        help=("the EXACT challenger id to adopt. Repeatable and required; "
              "there is deliberately no --all."))
    ap.add_argument(
        "--confirm", default=None, metavar="TOKEN",
        help="must be %s for a live write" % PA.OPERATOR_ADOPT_CONFIRM_TOKEN)
    ap.add_argument(
        "--execute", action="store_true",
        help=("perform the governed write. Without it (and without --confirm) "
              "this is a DRY RUN that writes nothing."))
    ap.add_argument("--json", action="store_true",
                    help="also emit the full report as one JSON object")
    args = ap.parse_args(argv)

    requested: list = []
    for cid in args.challenger_ids:
        cid = str(cid).strip()
        if cid and cid not in requested:
            requested.append(cid)

    write_requested = bool(args.execute or args.confirm)
    confirmed = (args.confirm == PA.OPERATOR_ADOPT_CONFIRM_TOKEN)
    boundary = PA.current_prospective_boundary()

    print("=" * 78)
    print("PROSPECTIVE FORWARD ADOPTION - existing frozen challengers")
    print("=" * 78)
    print("  entrypoint                   : %s" % ENTRYPOINT)
    print("  governed adoption owner      : %s" % PA.COMPOSITION_OWNER)
    print("  canonical registrar          : %s"
          % PA.REGISTRARS[PA.CLASS_SIGNAL_CANONICAL])
    print("  challengers requested        : %d (%s)"
          % (len(requested), ", ".join(requested)))
    print("  prospective boundary         : %s (derived; no operator input)"
          % boundary)
    print("  mode                         : %s"
          % ("EXECUTE" if (args.execute and confirmed) else "DRY RUN"))
    print("  backfill                     : IMPOSSIBLE - this entrypoint has "
          "no effective-from, date, backfill or inception argument")

    try:
        rows = _mem_rows()
    except Exception as exc:                              # noqa: BLE001
        print("\n  the canonical research memory could not be read: %s"
              % str(exc)[:200])
        print("\n%s" % TOKEN_FAILED)
        return 1

    plans = [_plan_one(cid, rows, boundary) for cid in requested]

    # --- Refusals are decided BEFORE anything is written, for EVERY id. ----- #
    if write_requested and not confirmed:
        for plan in plans:
            _print_row(_public(plan))
        print("\n  a live adoption requires --confirm %s"
              % PA.OPERATOR_ADOPT_CONFIRM_TOKEN)
        print("  nothing was written.")
        print("\n%s" % TOKEN_REFUSED)
        return 2
    if confirmed and not args.execute:
        for plan in plans:
            _print_row(_public(plan))
        print("\n  a live adoption requires --execute in addition to --confirm."
              "\n  nothing was written.")
        print("\n%s" % TOKEN_REFUSED)
        return 2

    if not (args.execute and confirmed):
        for plan in plans:
            _print_row(_public(plan))
        print("\n  DRY RUN. No intent, registration, prediction, observation "
              "or artifact was written.")
        report = {"entrypoint": ENTRYPOINT, "mode": "DRY_RUN",
                  "prospective_observation_boundary": boundary,
                  "challengers": [_public(p) for p in plans],
                  "wrote_anything": False}
        if args.json:
            print(json.dumps(report, indent=1, default=str))
        print("\n%s" % TOKEN_DRY_RUN_OK)
        return 0

    results = [_adopt_one(plan, boundary) for plan in plans]
    for row in results:
        _print_row(_public(row))

    failed = [r for r in results if r["disposition"] == D_FAILED]
    refused = [r for r in results if r["disposition"] == D_REFUSED]
    adopted = [r for r in results if r["disposition"] == D_ADOPTED]
    already = [r for r in results if r["disposition"] == D_ALREADY_REGISTERED]
    print("\n  adopted %d - already registered %d - refused %d - failed %d"
          % (len(adopted), len(already), len(refused), len(failed)))
    print("  no model was promoted, no sleeve activated, no capital allocated, "
          "no holding, cash or NAV changed and no order created.")
    report = {"entrypoint": ENTRYPOINT, "mode": "EXECUTE",
              "prospective_observation_boundary": boundary,
              "challengers": [_public(r) for r in results],
              "adopted": len(adopted), "already_registered": len(already),
              "refused": len(refused), "failed": len(failed)}
    if args.json:
        print(json.dumps(report, indent=1, default=str))

    if failed:
        print("\n  the adoption intents above remain OPEN and RESUMABLE; "
              "re-running this command resolves them and never duplicates one.")
        print("\n%s" % TOKEN_FAILED)
        return 1
    if refused:
        print("\n%s" % TOKEN_REFUSED)
        return 2
    print("\n%s" % TOKEN_OK)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
