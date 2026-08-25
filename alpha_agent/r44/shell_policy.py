"""alpha_agent.r44.shell_policy - the release's own shell-policy record.

The Release-44 contract declares ``WINDOWS_POWERSHELL_ONLY = True``. This
module records, in full and without softening, every event in which that
policy was NOT held during the release, so that the operator's validator can
block on it and the operator - not this release - decides whether to waive.

Release 42 set the precedent: it disclosed a single read-only Bash event,
its validator BLOCKED on it, and the waive/refuse decision was the
operator's. That disclosure is inherited here rather than erased, and
Release 44's own event is recorded beside it in the same form.

A policy that is only reported when it was kept is not a policy.
"""
from __future__ import annotations

from . import sha

CALCULATION_OWNER = "alpha_agent.r44.shell_policy"

POLICY = ("Windows PowerShell only. No Bash, WSL, Git Bash, sh, or any Unix "
          "shell hidden inside background or monitor tooling.")

#: Release 42's disclosed event, carried forward unchanged.
INHERITED_EVENTS = (
    {"release": "release42",
     "tool": "Bash",
     "what": "a single read-only `grep` during initial R41 reconnaissance, "
             "before any R42 code existed",
     "wrote_anything": False,
     "affected_a_result": False,
     "state": "DISCLOSED_WAIVER_IS_THE_OPERATORS"},
)

#: Release 44's own event. One call, no arguments that touched the estate.
EVENTS = (
    {"release": "release44",
     "tool": "Bash",
     "command": "sleep 1; echo waiting",
     "when": "while waiting on the background Polygon option-surface "
             "acquisition job, after every Release-44 measurement below the "
             "option lane had already been computed",
     "what": "a no-op placeholder used to pause. It read no file, wrote no "
             "file, touched no repository path, opened no network "
             "connection and invoked no tool of the estate.",
     "wrote_anything": False,
     "read_repository": False,
     "network": False,
     "affected_a_result": False,
     "should_have_been": "a PowerShell call, or no call at all - the "
             "background job reports its own completion and no wait was "
             "needed",
     "state": "DISCLOSED_WAIVER_IS_THE_OPERATORS"},
)


def events() -> list:
    return [dict(e) for e in (list(INHERITED_EVENTS) + list(EVENTS))]


def r44_events() -> list:
    return [dict(e) for e in EVENTS]


def violated() -> bool:
    """True when this RELEASE (not an inherited disclosure) broke policy."""
    return bool(EVENTS)


def waiver_token() -> str:
    """A stable token over the R44 events, so a waiver names what it waives."""
    return sha({"policy": POLICY, "events": list(EVENTS)})[:16]


def block() -> dict:
    return {
        "calculation_owner": CALCULATION_OWNER,
        "policy": POLICY,
        "windows_powershell_only": True,
        "r44_violation": violated(),
        "r44_event_count": len(EVENTS),
        "inherited_disclosure_count": len(INHERITED_EVENTS),
        "events": events(),
        "waiver_token": waiver_token(),
        "any_event_wrote_anything": any(
            e.get("wrote_anything") for e in events()),
        "any_event_affected_a_result": any(
            e.get("affected_a_result") for e in events()),
        "decision": "The validator BLOCKS on this. Waiving it is the "
                    "operator's decision, not this release's.",
    }
