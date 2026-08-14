"""Stage 21 (Workstream 0D) — PRODUCTION / HERMETIC ENVIRONMENT ISOLATION.

THE INCIDENT THIS PREVENTS
--------------------------
During Stage 20.1 UI acceptance the operator's PowerShell session retained hermetic
fixture variables::

    PAPER_TRADER_DESK_DIR=D:\\Temp\\stage20_ui_acceptance\\desk
    PAPER_TRADER_HOC_DIR=D:\\Temp\\stage20_ui_acceptance\\hoc
    PAPER_TRADER_DRC_DIR=D:\\Temp\\stage20_ui_acceptance\\drc
    ...

A later manual backend restart INHERITED them, and the real backend came up pointing at
an empty fixture world: the live book read as uninitialized, with no holdings, no cash
and no NAV. The real ledgers were never damaged — the 29-order lifecycle, 54 fills, 25
holdings, cash and NAV all returned once the canonical roots were restored — but for
the duration of that restart the operator was shown a fabricated empty portfolio and had
no way to tell it from a real one.

That must never be possible again, and it must not depend on remembering to use a
particular launch script. This module is a RUNTIME guard: the application itself refuses
to serve production while a canonical operational store points into a temp/acceptance
fixture root.

HERMETIC RUNS ARE STILL FIRST-CLASS
-----------------------------------
A hermetic acceptance backend sets ``PAPER_TRADER_ACCEPTANCE_MODE=1`` in ITS OWN child
process. That is an explicit, per-process opt-in: it declares "this process is a fixture
world", so the guard permits temp roots and the served payload is labelled accordingly.
It is never set in the operator's parent shell, and it can never make a production
process accept fixture roots by accident — the two are decided by the same flag that
distinguishes them.

Pure and read-only: it inspects an environment mapping and returns a verdict.
"""
from __future__ import annotations

import os
from pathlib import PurePath
from typing import Mapping, Optional

PHASE = "STAGE21"
OWNER = "api.environment_isolation"

#: Explicit per-process opt-in that this process is a hermetic fixture world.
ACCEPTANCE_MODE_ENV = "PAPER_TRADER_ACCEPTANCE_MODE"

#: Every environment variable that redirects a CANONICAL operational or research store.
#: A production process must have all of these either unset or pointing at a real root.
CANONICAL_STORE_ENV_VARS = (
    "PAPER_TRADER_DESK_DIR",
    "PAPER_TRADER_LEDGER_DIR",
    "PAPER_TRADER_HOC_DIR",
    "PAPER_TRADER_REALLOC_DIR",
    "PAPER_TRADER_REASSESSMENT_DIR",
    "PAPER_TRADER_REASSESSMENT_OUTCOME_DIR",
    "PAPER_TRADER_DRC_DIR",
    "PAPER_TRADER_PORTFOLIO_DECISION_DIR",
    "PAPER_TRADER_REBALANCE_PLAN_DIR",
    "PAPER_TRADER_CORPORATE_ACTIONS_DIR",
    "PAPER_TRADER_RESEARCH_AGENT_DIR",
    "PAPER_TRADER_DATA_EXPANSION_DIR",
    "PAPER_TRADER_RESEARCH_BRIDGE_DIR",
    "PAPER_TRADER_MHZ_INPUTS_DIR",
    "PAPER_TRADER_MHZ_STORE_DIR",
    "PAPER_TRADER_MHZ_LEDGER_DIR",
    "PAPER_TRADER_MHZ_ARTIFACT_DIR",
    "PAPER_TRADER_ALPHA_FACTORY_DIR",
    "PAPER_TRADER_PRICE_ALPHA_FACTORY_DIR",
    "PAPER_TRADER_MONTHLY_EMITTER_WORK_DIR",
    "PAPER_TRADER_BOOK_DIR",
)

#: Path fragments that mark a value as a TEMP / FIXTURE root rather than a real store.
#: Matched case-insensitively against the normalised path.
_FIXTURE_MARKERS = (
    "_ui_acceptance",
    "_acceptance",
    "acceptance_fixture",
    "pytest-of-",
    "\\temp\\",
    "/temp/",
    "\\tmp\\",
    "/tmp/",
)

#: Directory names that are ALWAYS a temp root when they are a path component.
_FIXTURE_ROOT_DIRS = ("temp", "tmp")

VIOLATION_TEMP_ROOT = "CANONICAL_STORE_POINTS_AT_TEMP_OR_ACCEPTANCE_ROOT"
STATUS_OK = "PRODUCTION_STORE_ROOTS_OK"
STATUS_VIOLATION = "PRODUCTION_STORE_ROOTS_CONTAMINATED"
STATUS_ACCEPTANCE = "HERMETIC_ACCEPTANCE_MODE"


def _normalise(value: str) -> str:
    return str(value).replace("/", "\\").lower()


def is_fixture_root(value: Optional[str]) -> bool:
    """True when a store path is a temp / acceptance fixture root, not a real store."""
    if not value:
        return False
    norm = _normalise(value)
    if any(m.replace("/", "\\") in norm for m in _FIXTURE_MARKERS):
        return True
    # A path whose SECOND component is temp/tmp (e.g. D:\Temp\..., C:\tmp\...).
    parts = [p.lower() for p in PurePath(norm).parts]
    return any(p.strip("\\") in _FIXTURE_ROOT_DIRS for p in parts)


def audit_store_roots(env: Optional[Mapping[str, str]] = None) -> dict:
    """Inspect an environment mapping for fixture contamination of canonical stores.

    Read-only and pure: it changes no environment variable and touches no disk.
    """
    e = os.environ if env is None else env
    acceptance = str(e.get(ACCEPTANCE_MODE_ENV) or "").strip().lower() in (
        "1", "true", "yes", "on")
    present, violations = {}, []
    for var in CANONICAL_STORE_ENV_VARS:
        val = e.get(var)
        if not val:
            continue
        present[var] = val
        if is_fixture_root(val):
            violations.append({"variable": var, "value": val,
                               "violation": VIOLATION_TEMP_ROOT})
    if acceptance:
        status, ok = STATUS_ACCEPTANCE, True
        message = ("Hermetic acceptance mode is explicitly enabled for THIS process, so "
                   "fixture store roots are expected and permitted. This flag is never "
                   "set in the operator's shell and never applies to a production start.")
    elif violations:
        status, ok = STATUS_VIOLATION, False
        message = (
            "REFUSING TO SERVE PRODUCTION: %d canonical operational store(s) point at a "
            "temp / acceptance fixture root (%s). A backend started this way shows a "
            "FABRICATED empty portfolio that is indistinguishable from a real one. Clear "
            "these variables in the shell that launches the backend, or set %s=1 if this "
            "really is a hermetic acceptance process."
            % (len(violations), ", ".join(v["variable"] for v in violations),
               ACCEPTANCE_MODE_ENV))
    else:
        status, ok = STATUS_OK, True
        message = ("Every canonical operational store resolves to a production root; no "
                   "acceptance or temp fixture path is active.")
    return {
        "phase": PHASE, "owner": OWNER, "status": status, "ok": ok,
        "acceptance_mode": acceptance,
        "acceptance_mode_env": ACCEPTANCE_MODE_ENV,
        "overridden_variables": present,
        "overridden_count": len(present),
        "violations": violations,
        "violation_count": len(violations),
        "checked_variables": list(CANONICAL_STORE_ENV_VARS),
        "message": message,
        "read_only": True, "performed_write": False,
        "changed_environment": False,
    }


def assert_production_store_roots(env: Optional[Mapping[str, str]] = None) -> dict:
    """FAIL CLOSED. Raise unless every canonical store resolves to a production root.

    Called at application startup. A contaminated production process must never reach
    the point of serving a portfolio read: an empty fixture world rendered as the real
    book is worse than no backend at all.
    """
    report = audit_store_roots(env)
    if not report["ok"]:
        raise RuntimeError(report["message"])
    return report


__all__ = ["PHASE", "OWNER", "ACCEPTANCE_MODE_ENV", "CANONICAL_STORE_ENV_VARS",
           "STATUS_OK", "STATUS_VIOLATION", "STATUS_ACCEPTANCE", "VIOLATION_TEMP_ROOT",
           "is_fixture_root", "audit_store_roots", "assert_production_store_roots"]
