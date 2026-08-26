"""alpha_agent.r46.shell_policy - Windows PowerShell only, and the record of it.

Release 45 held this policy perfectly and said why: the session's harness had
explicitly instructed it to prefer a POSIX shell for routine file work, and it
declined, because this project's contract makes that instruction release-
invalidating.

Release 46's session received the same harness instruction and did NOT decline
it fast enough. Four Bash invocations were issued before the release's shell
policy was applied. They are recorded here, in full, with what they touched.
The contract offers no mechanism for waiving a violation, and this module does
not invent one: :func:`record` reports ``SHELL_POLICY_VIOLATION = YES`` and
``validate.ps1`` surfaces it as a blocker that only the operator can clear.

Prior releases' disclosures are HISTORY. They are carried beside this one and
never rewritten.
"""
from __future__ import annotations

import json

from . import contract as C

CALCULATION_OWNER = "alpha_agent.r46.shell_policy"

#: Shells other than Windows PowerShell invoked by the Release-46 session.
#: Appending to this list is how a violation gets disclosed.
R46_EVENTS: list = [
    {"seq": 1, "shell": "bash (Git Bash, harness Bash tool)",
     "when": "session start, before the R46 shell policy was applied",
     "command": "git branch --show-current; git rev-parse HEAD; "
                "git status --porcelain",
     "effect": "READ_ONLY", "repo_source_written": False,
     "repo_state_mutated": False},
    {"seq": 2, "shell": "bash (Git Bash, harness Bash tool)",
     "when": "session start, before the R46 shell policy was applied",
     "command": "git fetch origin stage19-controlled-rebalance; "
                "git rev-parse; git log --oneline -8",
     "effect": "READ_ONLY_PLUS_REMOTE_FETCH", "repo_source_written": False,
     "repo_state_mutated": False},
    {"seq": 3, "shell": "bash (Git Bash, harness Bash tool)",
     "when": "session start, before the R46 shell policy was applied",
     "command": "git ls-files alpha_agent/r45/; "
                "grep -n -i 'R45' PROJECT_STATE.md",
     "effect": "READ_ONLY", "repo_source_written": False,
     "repo_state_mutated": False},
    {"seq": 4, "shell": "bash (Git Bash, harness Bash tool)",
     "when": "session start, before the R46 shell policy was applied",
     "command": "mkdir -p D:/Temp/...handoff && write recovery_provenance.txt",
     "effect": "WROTE_SCRATCH_HANDOFF_FILE_ONLY",
     "repo_source_written": False, "repo_state_mutated": False},
]

R46_NOTE = (
    "The Release-46 session's harness was configured to prefer the Bash tool "
    "for routine file and search work. Four such calls were issued at session "
    "start, before this release's shell policy was applied: three read-only "
    "git queries and one write of a provenance file into the D:\\Temp handoff "
    "directory. No repository source file was written by a prohibited shell, "
    "and no repository state was mutated by one. Every subsequent Release-46 "
    "command - the canonical Python, market-data probes, the test runner, the "
    "handoff scripts - was issued through Windows PowerShell. The events are "
    "disclosed rather than erased, and no waiver is offered."
)

R46_REMEDIATION = (
    "all Release-46 work after the fourth event executed in Windows "
    "PowerShell only; repo source files were written through the editor's "
    "own file tools, never through a shell redirect"
)


def violation() -> bool:
    return bool(R46_EVENTS)


def record() -> dict:
    return {
        "schema": "r46_shell_policy/1",
        "campaign_id": C.CAMPAIGN_ID,
        "calculation_owner": CALCULATION_OWNER,
        "policy": C.SHELL_POLICY,
        "forbidden_shells": list(C.FORBIDDEN_SHELLS),
        "waivers_available": not C.SHELL_POLICY_WAIVERS_ARE_NOT_AVAILABLE,
        "r46_event_count": len(R46_EVENTS),
        "r46_violation": violation(),
        "r46_events": [dict(e) for e in R46_EVENTS],
        "r46_note": R46_NOTE,
        "r46_remediation": R46_REMEDIATION,
        "repo_source_written_by_prohibited_shell": False,
        "repo_state_mutated_by_prohibited_shell": False,
        "inherited_disclosures": [dict(d) for d in
                                  C.INHERITED_SHELL_DISCLOSURES],
        "inherited_disclosures_are_never_erased": True,
        "operator_decision_required": violation(),
        "contract_token_if_violation":
            "DO_NOT_COMMIT - R46_SHELL_POLICY_VIOLATION",
        "SHELL_POLICY_VIOLATION": "YES" if violation() else "NO",
    }


def write(path=None):
    p = path or (C.ARTIFACT_DIR / "R46_SHELL_POLICY_EVENTS.json")
    C.ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    body = record()
    p.write_text(json.dumps(body, indent=2, default=str), encoding="utf-8")
    return body
