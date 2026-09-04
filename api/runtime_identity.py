r"""api.runtime_identity — RELEASE 55.2. WHICH RELEASE DID THIS PROCESS LOAD?

THE INCIDENT THIS MODULE EXISTS TO MAKE IMPOSSIBLE
--------------------------------------------------
The continuous information-collection worker was started at 2026-09-01 14:12:09.
The R54.1 intraday governance gate landed in commit ``0cff378`` at
2026-09-01 23:46:16 — nine hours and thirty-four minutes LATER. A Python process
resolves its imports once, at start, and holds that module graph for life, so the
worker went on executing a pre-R54.1 snapshot of ``api.event_signal_refresh``
while the repository, the backend and every test reported the newer code. Every
intraday cycle it persisted therefore lacked the governance step entirely, and
the system had no way to say so: the heartbeat was fresh, progress was
advancing, and the service reported RUNNING. It looked like a governance defect
for an entire release.

THE DISTINCTION THIS MODULE OWNS
--------------------------------
    SOURCE / REPOSITORY IDENTITY   what revision exists on disk RIGHT NOW.
    LOADED RUNTIME IDENTITY        what revision a process actually loaded when
                                   it started — captured ONCE, then frozen.
    RUNTIME ALIGNMENT              whether the cooperating runtimes are operating
                                   the same release.

The one rule that makes this useful: **a later source-tree change must never
change the reported loaded identity of an already-running process.**
``capture_loaded_identity`` therefore memoises per process, and every subsequent
call returns the SAME frozen mapping. Calling ``git rev-parse HEAD`` on each
status request and calling the answer "the loaded runtime identity" would
reproduce the exact bug this module exists to detect.

FAIL CLOSED
-----------
``ALIGNED`` requires two PROVEN commit identities that are equal. A fresh
heartbeat, a live pid, an advancing iteration and a healthy service state prove
that a process is running; none of them prove WHICH CODE it is running, and none
of them may ever produce ALIGNED. Anything unprovable is ``UNKNOWN``.

WHAT IT NEVER DOES
------------------
Starts, stops, restarts, signals or kills a process; writes any file; touches
the scheduler; decides an operator action; changes an allocation. It reads
``.git`` for a commit and (optionally, tolerantly) asks git whether tracked
files are modified. The canonical restart owners are unchanged:
``scripts\restart_paper_trader_backend.ps1`` for the backend and
``scripts\manage_information_collection.ps1`` for the collection worker.
"""
from __future__ import annotations

import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

OWNER = "api.runtime_identity"
SCHEMA_VERSION = "runtime_release_identity.v1"
CONTRACT_ID = "paper_trader.runtime_release_identity/1"

#: The repository this package was loaded from (``api/`` -> repo root).
REPO_ROOT = Path(__file__).resolve().parents[1]

#: How a commit identity was resolved. The git DIRECTORY read is authoritative
#: and needs no subprocess; the subprocess is only ever a fallback.
RESOLVED_FROM_GIT_DIR = "GIT_DIRECTORY_READ"
RESOLVED_FROM_GIT_COMMAND = "GIT_COMMAND"
RESOLVED_UNRESOLVED = "UNRESOLVED"

# --------------------------------------------------------------------------- #
# The alignment vocabulary. Four verdicts, and only one of them means "current".
# --------------------------------------------------------------------------- #
#: The runtime loaded the same commit that is on disk now.
ALIGNMENT_ALIGNED = "ALIGNED"
#: The runtime is alive and healthy and loaded a DIFFERENT commit. This is the
#: incident state: nothing is broken, nothing is failing, and the code being
#: executed is not the code that was deployed.
ALIGNMENT_STALE = "STALE_RUNTIME"
#: Identity is required for this runtime and cannot be proven. Never ALIGNED.
ALIGNMENT_UNKNOWN = "UNKNOWN"
#: The runtime does not need a persistent release identity — a scheduled task
#: that starts, runs one bounded invocation and exits cannot be stale, because
#: it re-imports the whole module graph on its next invocation.
ALIGNMENT_NOT_APPLICABLE = "NOT_APPLICABLE"
ALIGNMENT_VERDICTS = (ALIGNMENT_ALIGNED, ALIGNMENT_STALE, ALIGNMENT_UNKNOWN,
                      ALIGNMENT_NOT_APPLICABLE)

#: Worst first. The composed verdict is the worst verdict any REQUIRED runtime
#: holds: a proven stale runtime outranks an unprovable one, and neither is ever
#: softened by a runtime that happens to be aligned.
_ALIGNMENT_SEVERITY = {ALIGNMENT_STALE: 3, ALIGNMENT_UNKNOWN: 2,
                       ALIGNMENT_ALIGNED: 1, ALIGNMENT_NOT_APPLICABLE: 0}

#: One named reason per verdict. A verdict never arrives without its cause.
REASON_SAME_COMMIT = "LOADED_COMMIT_MATCHES_SOURCE_COMMIT"
REASON_DIFFERENT_COMMIT = "LOADED_COMMIT_DIFFERS_FROM_SOURCE_COMMIT"
REASON_LOADED_UNKNOWN = "LOADED_COMMIT_NOT_RECORDED_BY_THIS_RUNTIME"
REASON_SOURCE_UNKNOWN = "SOURCE_COMMIT_COULD_NOT_BE_RESOLVED"
REASON_NOT_REQUIRED = "RUNTIME_EXITS_BETWEEN_INVOCATIONS"
ALIGNMENT_REASONS = (REASON_SAME_COMMIT, REASON_DIFFERENT_COMMIT,
                     REASON_LOADED_UNKNOWN, REASON_SOURCE_UNKNOWN,
                     REASON_NOT_REQUIRED)

# --------------------------------------------------------------------------- #
# The runtimes. Named once, here, so no surface invents its own list.
# --------------------------------------------------------------------------- #
RUNTIME_BACKEND = "backend"
RUNTIME_COLLECTION = "information_collection_worker"
RUNTIME_RESEARCH = "research_runtime"
RUNTIME_INTRADAY_EMISSION = "intraday_emission"

#: label, whether a persistent loaded identity is REQUIRED, and — for a runtime
#: that can go stale — the ONE canonical command that brings it back current.
RUNTIME_CONTRACT = {
    RUNTIME_BACKEND: {
        "label": "Backend / API runtime",
        "identity_required": True,
        "lifecycle": "LONG_LIVED",
        "startup_owner": "scripts/restart_paper_trader_backend.ps1",
        "remediation": ("Restart the backend with "
                        "scripts\\restart_paper_trader_backend.ps1 -Force "
                        "-Port 8001."),
    },
    RUNTIME_COLLECTION: {
        "label": "Information-collection worker",
        "identity_required": True,
        "lifecycle": "LONG_LIVED",
        "startup_owner": "scripts/manage_information_collection.ps1",
        "remediation": ("Restart the canonical information-collection service "
                        "with scripts\\manage_information_collection.ps1 "
                        "-Action Restart -Execute."),
    },
    RUNTIME_RESEARCH: {
        "label": "Prospective research runtime (scheduled)",
        "identity_required": False,
        "lifecycle": "SCHEDULED_INVOCATION",
        "startup_owner": "PaperTrader-ResearchRuntime scheduled task",
        "remediation": None,
    },
    RUNTIME_INTRADAY_EMISSION: {
        "label": "Intraday prospective emission (scheduled)",
        "identity_required": False,
        "lifecycle": "SCHEDULED_INVOCATION",
        "startup_owner": "PaperTrader-IntradayEmission scheduled task",
        "remediation": None,
    },
}


# --------------------------------------------------------------------------- #
# Clock + git plumbing
# --------------------------------------------------------------------------- #
def _utc_iso(now: Optional[datetime] = None) -> str:
    return (now or datetime.now(timezone.utc)).isoformat()


def _short(commit: Any) -> Optional[str]:
    text = str(commit or "").strip()
    return text[:12] if text else None


def _resolve_git_dir(repo_root: Path) -> Optional[Path]:
    """The real ``.git`` directory, following the ``gitdir:`` pointer a worktree
    or a submodule leaves behind. Returns None when there is no git metadata."""
    dot = repo_root / ".git"
    try:
        if dot.is_dir():
            return dot
        if dot.is_file():
            text = dot.read_text(encoding="utf-8", errors="replace").strip()
            if text.startswith("gitdir:"):
                target = Path(text.split(":", 1)[1].strip())
                if not target.is_absolute():
                    target = (repo_root / target).resolve()
                return target if target.exists() else None
    except OSError:
        return None
    return None


def _commit_from_git_dir(git_dir: Path) -> tuple:
    """``(commit, branch)`` read STRAIGHT out of git's own files.

    No subprocess, so a process can capture its identity at start without paying
    for — or depending on — an external ``git`` executable. Handles the three
    shapes git actually writes: a symbolic HEAD, a detached HEAD, and a ref that
    lives only in ``packed-refs``.
    """
    try:
        head = (git_dir / "HEAD").read_text(encoding="utf-8",
                                            errors="replace").strip()
    except OSError:
        return None, None
    if not head.startswith("ref:"):
        # Detached HEAD: the file IS the commit.
        return (head or None), None
    ref = head.split(":", 1)[1].strip()
    branch = ref.rsplit("/", 1)[-1] if ref else None
    try:
        loose = git_dir / Path(ref)
        if loose.is_file():
            return (loose.read_text(encoding="utf-8",
                                    errors="replace").strip() or None), branch
    except OSError:
        pass
    try:
        for line in (git_dir / "packed-refs").read_text(
                encoding="utf-8", errors="replace").splitlines():
            if line.startswith("#") or not line.strip():
                continue
            parts = line.split(None, 1)
            if len(parts) == 2 and parts[1].strip() == ref:
                return parts[0].strip(), branch
    except OSError:
        pass
    return None, branch


def _run_git(args: list, *, repo_root: Path,
             runner: Optional[Callable] = None) -> Optional[str]:
    """A bounded, read-only git query. Any failure degrades to None — an
    unavailable git makes a fact UNKNOWN, never wrong."""
    if runner is not None:
        try:
            return runner(args)
        except Exception:  # noqa: BLE001 - an injected runner never breaks a read
            return None
    try:
        out = subprocess.run(["git", "-C", str(repo_root)] + list(args),
                             capture_output=True, text=True, timeout=20)
    except (OSError, subprocess.SubprocessError):
        return None
    if out.returncode != 0:
        return None
    return (out.stdout or "").strip()


def _dirty_flag(repo_root: Path, runner: Optional[Callable]) -> Optional[bool]:
    """Are TRACKED files modified relative to the commit?

    Untracked files are deliberately excluded: logs, scratch output and editor
    backups litter a working repository and say nothing about which application
    code a process loaded. ``None`` means git could not answer — reported as
    unknown, never guessed as clean.
    """
    out = _run_git(["status", "--porcelain", "--untracked-files=no"],
                   repo_root=repo_root, runner=runner)
    if out is None:
        return None
    return bool(out.strip())


# --------------------------------------------------------------------------- #
# SOURCE identity — what is on disk NOW. Deliberately dynamic.
# --------------------------------------------------------------------------- #
def read_source_identity(*, repo_root=None, runner: Optional[Callable] = None,
                         now: Optional[datetime] = None) -> dict:
    """The revision that exists on disk at the moment of the call.

    This is the ONLY function in the application that answers "what is the
    current source revision" for operational purposes, and it is deliberately
    NOT the loaded identity of any process. It is re-read on every call because
    that is what it means.
    """
    root = Path(repo_root) if repo_root is not None else REPO_ROOT
    git_dir = _resolve_git_dir(root)
    commit, branch = (_commit_from_git_dir(git_dir) if git_dir
                      else (None, None))
    resolved_from = RESOLVED_FROM_GIT_DIR if commit else RESOLVED_UNRESOLVED
    if not commit:
        commit = _run_git(["rev-parse", "HEAD"], repo_root=root, runner=runner)
        if commit:
            resolved_from = RESOLVED_FROM_GIT_COMMAND
    return {
        "identity_kind": "SOURCE_REPOSITORY_IDENTITY",
        "owner": OWNER,
        "schema_version": SCHEMA_VERSION,
        "repo_root": str(root),
        "commit": commit or None,
        "commit_short": _short(commit),
        "branch": branch,
        "dirty": _dirty_flag(root, runner),
        "resolved_from": resolved_from,
        "read_at": _utc_iso(now),
        "is_a_loaded_runtime_identity": False,
        "note": ("The revision on disk right now. It is NOT proof of what any "
                 "running process loaded."),
    }


# --------------------------------------------------------------------------- #
# LOADED identity — captured ONCE per process, then frozen for its life.
# --------------------------------------------------------------------------- #
#: The per-process capture. Written exactly once; never invalidated by a later
#: source change, which is the entire point.
_LOADED: Optional[dict] = None


def capture_loaded_identity(*, repo_root=None, runner: Optional[Callable] = None,
                            now: Optional[datetime] = None,
                            pid: Optional[int] = None) -> dict:
    """Capture — ONCE — the release this process loaded, and freeze it.

    Called by a runtime's startup owner as early as it can. Every later call in
    the same process returns the SAME mapping, whatever the source tree has done
    in the meantime: that immutability is the guarantee that makes a stale
    runtime detectable at all. Idempotent, so a runtime that also reaches it
    through :func:`loaded_identity` gets the identical answer.
    """
    global _LOADED
    if _LOADED is not None:
        return _LOADED
    src = read_source_identity(repo_root=repo_root, runner=runner, now=now)
    _LOADED = {
        "identity_kind": "LOADED_RUNTIME_IDENTITY",
        "owner": OWNER,
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "repo_root": src["repo_root"],
        "commit": src["commit"],
        "commit_short": src["commit_short"],
        "branch": src["branch"],
        "dirty_at_capture": src["dirty"],
        "resolved_from": src["resolved_from"],
        "captured_at": src["read_at"],
        "pid": int(pid) if pid is not None else os.getpid(),
        "captured_once_per_process": True,
        "changes_when_source_changes": False,
        "note": ("Captured when this process started. A later edit or commit "
                 "does not change it, because the process already resolved its "
                 "imports."),
    }
    return _LOADED


def loaded_identity() -> dict:
    """This process's frozen loaded identity, capturing it if nothing has yet."""
    return capture_loaded_identity()


def reset_loaded_identity_for_tests() -> None:
    """Clear the per-process capture. TESTS ONLY.

    A test proving that capture is immutable needs a way back to an uncaptured
    process; nothing in the application calls this, and the audit forbids any
    non-test caller.
    """
    global _LOADED
    _LOADED = None


# --------------------------------------------------------------------------- #
# ALIGNMENT — the comparison, and nothing but the comparison.
# --------------------------------------------------------------------------- #
def classify_alignment(*, loaded: Optional[dict], source: Optional[dict],
                       identity_required: bool = True) -> dict:
    """Is this runtime operating the release that is deployed?

    Pure. It reads no clock, no store, no process table and no heartbeat, and it
    NEVER infers alignment from liveness: a fresh heartbeat proves a process is
    running, not which code it is running. ALIGNED requires two proven commits
    that are equal; everything else is STALE_RUNTIME (proven different) or
    UNKNOWN (not provable).
    """
    loaded_commit = (loaded or {}).get("commit") or None
    source_commit = (source or {}).get("commit") or None
    if not identity_required:
        verdict, reason = ALIGNMENT_NOT_APPLICABLE, REASON_NOT_REQUIRED
    elif not loaded_commit:
        verdict, reason = ALIGNMENT_UNKNOWN, REASON_LOADED_UNKNOWN
    elif not source_commit:
        verdict, reason = ALIGNMENT_UNKNOWN, REASON_SOURCE_UNKNOWN
    elif str(loaded_commit) == str(source_commit):
        verdict, reason = ALIGNMENT_ALIGNED, REASON_SAME_COMMIT
    else:
        verdict, reason = ALIGNMENT_STALE, REASON_DIFFERENT_COMMIT
    return {
        "owner": OWNER,
        "verdict": verdict,
        "verdict_vocabulary": list(ALIGNMENT_VERDICTS),
        "reason": reason,
        "identity_required": bool(identity_required),
        "loaded_commit": loaded_commit,
        "loaded_commit_short": _short(loaded_commit),
        "source_commit": source_commit,
        "source_commit_short": _short(source_commit),
        "loaded_captured_at": (loaded or {}).get("captured_at"),
        "source_read_at": (source or {}).get("read_at"),
        "source_dirty": (source or {}).get("dirty"),
        "dirty_at_capture": (loaded or {}).get("dirty_at_capture"),
        # Commit identity is what the verdict is decided on. A dirty working
        # tree is the NORMAL state during implementation and cannot prove that a
        # running process loaded different code, so it is reported as a caveat
        # and never manufactures a STALE verdict of its own.
        "verdict_decided_on": "COMMIT_IDENTITY",
        "working_tree_caveat": (
            "The working tree has uncommitted changes to tracked files, so the "
            "commit is the strongest available proof of what is on disk."
            if (source or {}).get("dirty") else None),
        "inferred_from_process_health": False,
        "writes_nothing": True,
        "restarts_nothing": True,
    }


def _statement(runtime: str, row: dict) -> str:
    """The operator sentence for one runtime, composed HERE so no surface has to
    word an alignment verdict for itself."""
    label = row.get("label") or runtime
    verdict = row.get("verdict")
    if verdict == ALIGNMENT_ALIGNED:
        return "%s is running the deployed release (%s)." % (
            label, row.get("loaded_commit_short") or "unknown commit")
    if verdict == ALIGNMENT_STALE:
        return ("%s is running an OLDER application release: it loaded %s at "
                "start and the deployed source is %s. It is alive and healthy; "
                "it is not current."
                % (label, row.get("loaded_commit_short") or "an unknown commit",
                   row.get("source_commit_short") or "an unknown commit"))
    if verdict == ALIGNMENT_NOT_APPLICABLE:
        return ("%s starts, runs one bounded invocation and exits, so it "
                "re-loads the application every time and cannot hold a stale "
                "release." % label)
    return ("%s cannot prove which application release it loaded, so it is not "
            "reported as current." % label)


def build_runtime_alignment(*, runtimes: Optional[list] = None,
                            source: Optional[dict] = None,
                            now: Optional[datetime] = None) -> dict:
    """The composed, READ-ONLY runtime-alignment contract.

    ``runtimes`` is a list of ``{"runtime": <name>, "loaded": <identity|None>,
    "process": {...}}`` supplied by whoever already read those facts — this
    module opens no store and inspects no process table. Every field it returns
    is either copied or derived from :func:`classify_alignment`.
    """
    src = source if source is not None else read_source_identity(now=now)
    rows = []
    for entry in (runtimes or []):
        name = str((entry or {}).get("runtime") or "")
        contract = RUNTIME_CONTRACT.get(name, {})
        required = bool(entry.get("identity_required",
                                  contract.get("identity_required", True)))
        row = classify_alignment(loaded=entry.get("loaded"), source=src,
                                 identity_required=required)
        row.update({
            "runtime": name,
            "label": contract.get("label") or name,
            "lifecycle": contract.get("lifecycle"),
            "startup_owner": contract.get("startup_owner"),
            "process": entry.get("process") or {},
            "loaded_identity": entry.get("loaded"),
        })
        row["statement"] = _statement(name, row)
        row["remediation"] = (contract.get("remediation")
                              if row["verdict"] in (ALIGNMENT_STALE,
                                                    ALIGNMENT_UNKNOWN)
                              else None)
        rows.append(row)

    considered = [r for r in rows if r["identity_required"]]
    verdict = ALIGNMENT_NOT_APPLICABLE
    for row in considered:
        if _ALIGNMENT_SEVERITY[row["verdict"]] > _ALIGNMENT_SEVERITY[verdict]:
            verdict = row["verdict"]
    if considered and verdict == ALIGNMENT_NOT_APPLICABLE:
        verdict = ALIGNMENT_ALIGNED
    stale = [r["runtime"] for r in rows if r["verdict"] == ALIGNMENT_STALE]
    unknown = [r["runtime"] for r in rows if r["verdict"] == ALIGNMENT_UNKNOWN]
    remediation = [r["remediation"] for r in rows if r.get("remediation")]

    if verdict == ALIGNMENT_ALIGNED:
        headline = "All persistent runtimes are running the deployed release."
    elif verdict == ALIGNMENT_STALE:
        headline = ("A running service is executing an OLDER application "
                    "release than the one deployed on disk.")
    elif verdict == ALIGNMENT_UNKNOWN:
        headline = ("A running service cannot prove which application release "
                    "it loaded.")
    else:
        headline = "No runtime requires a persistent release identity."
    return {
        "contract_id": CONTRACT_ID,
        "schema_version": SCHEMA_VERSION,
        "owner": OWNER,
        "generated_at": _utc_iso(now),
        "verdict": verdict,
        "verdict_vocabulary": list(ALIGNMENT_VERDICTS),
        "reason_vocabulary": list(ALIGNMENT_REASONS),
        "aligned": verdict == ALIGNMENT_ALIGNED,
        # PROVEN is the fail-closed flag: it is true only when every runtime
        # that needs an identity actually produced one. Silence never counts.
        "proven": bool(considered) and not unknown,
        "headline": headline,
        "source": src,
        "runtimes": rows,
        "stale_runtimes": stale,
        "unknown_runtimes": unknown,
        "remediation": remediation,
        "read_only": True,
        "writes_nothing": True,
        "restarts_nothing": True,
        "restarts_no_process_automatically": True,
        "infers_alignment_from_process_health": False,
        # A stale RESEARCH/COLLECTION runtime degrades near-real-time research.
        # It does not invalidate a completed operational close or the standing
        # governed portfolio decision, which the backend produced.
        "invalidates_operational_close": False,
        "invalidates_governed_decision": False,
        "alters_primary_operator_action": False,
        "note": ("Alignment is decided on captured LOADED commit identity "
                 "versus the source revision on disk. A heartbeat is never "
                 "evidence of alignment, and nothing here restarts a process."),
    }


__all__ = [
    "OWNER", "SCHEMA_VERSION", "CONTRACT_ID", "REPO_ROOT",
    "ALIGNMENT_ALIGNED", "ALIGNMENT_STALE", "ALIGNMENT_UNKNOWN",
    "ALIGNMENT_NOT_APPLICABLE", "ALIGNMENT_VERDICTS", "ALIGNMENT_REASONS",
    "REASON_SAME_COMMIT", "REASON_DIFFERENT_COMMIT", "REASON_LOADED_UNKNOWN",
    "REASON_SOURCE_UNKNOWN", "REASON_NOT_REQUIRED",
    "RUNTIME_BACKEND", "RUNTIME_COLLECTION", "RUNTIME_RESEARCH",
    "RUNTIME_INTRADAY_EMISSION", "RUNTIME_CONTRACT",
    "read_source_identity", "capture_loaded_identity", "loaded_identity",
    "reset_loaded_identity_for_tests", "classify_alignment",
    "build_runtime_alignment",
]
