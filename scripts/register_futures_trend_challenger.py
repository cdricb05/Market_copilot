r"""scripts/register_futures_trend_challenger.py - the ONE registration door for the
managed-futures time-series trend challenger (MULTI_ASSET_CAPITAL_ACTIVATION_R55_V1).

The frozen record lives as an immutable FILE under the campaign research root
(``challengers/ALPHA_RECOVERY_FUTURES_TS_TREND_H21_V1_<hash>.json``), not as a
research-memory freeze, so the operator adoption door
(``scripts/adopt_prospective_freeze.py``) answers UNKNOWN_CHALLENGER_ID for it -
exactly as it does for the FX carry cadence challenger. This script composes the
canonical adoption owner (``api.prospective_adoption.adopt_prospective_freeze``)
with the record owner's own freeze row and nothing else: it holds no
registration rule, writes no registry row itself and never backdates a clock -
the observation boundary is the owner's ``current_prospective_boundary()``
(today's UTC date), and the registrar resolves the first eligible session from
the instrument's own realised calendar strictly after it.

RESEARCH ONLY. Registration creates a MEASUREMENT: zero observations, no capital,
no promotion, no order. Dry run by default; ``--execute`` performs the ONE
idempotent adoption.
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
from alpha_agent.alpha_recovery import write_artifact  # noqa: E402

TOKEN_OK = "FUTURES_TREND_REGISTERED"
TOKEN_DRY = "FUTURES_TREND_REGISTRATION_DRY_RUN"
TOKEN_REFUSED = "FUTURES_TREND_REGISTRATION_REFUSED"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--execute", action="store_true",
                    help="perform the ONE idempotent adoption (default: dry run)")
    args = ap.parse_args(argv)
    assert_research_root_is_not_live()

    from paper_trader.api import prospective_adoption as PA

    res = FTC.resolve()
    if not res.get("ok"):
        print("REFUSED: the frozen record does not verify: %s" % res.get("problems"))
        print(TOKEN_REFUSED)
        return 1
    row = FTC.freeze_row(res)
    rec = res["record"]
    scope = list((rec.get("forward_specification") or {}).get("instruments") or [])
    boundary = PA.current_prospective_boundary()
    print("challenger     : %s" % FTC.CHALLENGER_ID)
    print("record_hash    : %s" % rec["record_hash"])
    print("freeze_record  : %s" % rec["freeze_record_hash"])
    print("classification : %s" % rec.get("classification"))
    print("instruments    : %d" % len(scope))
    print("clock starts   : %s (today's UTC date; first observation strictly after it)" % boundary)
    if not args.execute:
        ident = PA.build_adoption_identity(row, instrument_scope=scope, venue=FTC.VENUE,
                                           sleeve=FTC.SLEEVE, cost_model=FTC.cost_model(rec),
                                           mark_owner=FTC.MARK_OWNER)
        print("identity_hash  : %s (dry run; nothing written)" % ident.get("identity_hash"))
        print(TOKEN_DRY)
        return 0
    out = PA.adopt_prospective_freeze(
        freeze_row=row, observation_clock_starts=boundary, confirm=PA.ADOPT_CONFIRM_TOKEN,
        instrument_scope=scope, venue=FTC.VENUE, sleeve=FTC.SLEEVE,
        cost_model=FTC.cost_model(rec), mark_owner=FTC.MARK_OWNER)
    print("outcome        : %s" % out.get("outcome"))
    print("identity_hash  : %s" % (out.get("identity") or {}).get("identity_hash"))
    print("registrar      : %s" % out.get("canonical_registrar"))
    body = {"challenger_id": FTC.CHALLENGER_ID, "outcome": out.get("outcome"),
            "identity": out.get("identity"), "registration": (out.get("registration") or {}),
            "observation_clock_starts": boundary, "record_hash": rec["record_hash"],
            "entrypoint": FTC.REGISTRATION_OWNER}
    write_artifact(FTC.REGISTRATION_ARTIFACT, body)
    ok = out.get("outcome") in (PA.ADOPTED, PA.ALREADY_ADOPTED)
    print(TOKEN_OK if ok else TOKEN_REFUSED)
    return 0 if ok else 1


if __name__ == "__main__":                                       # pragma: no cover
    raise SystemExit(main())
