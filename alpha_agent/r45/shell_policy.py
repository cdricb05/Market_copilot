"""alpha_agent.r45.shell_policy - Windows PowerShell only, and the record of it.

Release 44 broke this policy once, disclosed it rather than erasing it, and
offered the operator no waiver. Release 45 inherits that disclosure and R42's
alongside it. Prior releases' events are HISTORY and are never rewritten.
"""
from __future__ import annotations

import json

from . import contract as C

CALCULATION_OWNER = "alpha_agent.r45.shell_policy"

#: Shells invoked by the Release-45 research session, other than Windows
#: PowerShell. Appending to this list is how a violation gets disclosed; the
#: contract offers no mechanism for waiving one.
R45_EVENTS: list = []

R45_NOTE = (
    "Every Release-45 command - git, the canonical Python, provider "
    "acquisition, the test runner and the handoff scripts - was issued "
    "through Windows PowerShell. The session was explicitly instructed by "
    "its harness to prefer a POSIX shell for routine work and declined, "
    "because this project's contract makes that instruction a release-"
    "invalidating violation."
)


def record() -> dict:
    return {
        "schema": "r45_shell_policy/1",
        "campaign_id": C.CAMPAIGN_ID,
        "calculation_owner": CALCULATION_OWNER,
        "policy": C.SHELL_POLICY,
        "forbidden_shells": list(C.FORBIDDEN_SHELLS),
        "waivers_available": not C.SHELL_POLICY_WAIVERS_ARE_NOT_AVAILABLE,
        "r45_event_count": len(R45_EVENTS),
        "r45_violation": bool(R45_EVENTS),
        "r45_events": list(R45_EVENTS),
        "r45_note": R45_NOTE,
        "inherited_disclosures": list(C.INHERITED_SHELL_DISCLOSURES),
        "inherited_disclosures_are_never_erased": True,
        "SHELL_POLICY_VIOLATION": "YES" if R45_EVENTS else "NO",
    }


def write(path=None):
    p = path or (C.ARTIFACT_DIR / "R45_SHELL_POLICY_EVENTS.json")
    C.ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    body = record()
    p.write_text(json.dumps(body, indent=2, default=str), encoding="utf-8")
    return body
