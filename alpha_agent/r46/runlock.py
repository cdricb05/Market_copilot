"""alpha_agent.r46.runlock - one writer at a time for the tournament step.

Release 52 gives the tournament a second caller: the canonical Daily Research
Cycle keeps calling :func:`alpha_agent.r46.advance.advance` when the operator
runs a cycle, and the scheduled research runtime now calls the SAME function
on its own triggers. The ledgers append through read-modify-write, so two
concurrent advances could lose one caller's rows silently (the R46.5
lost-update class). This module makes the collision impossible instead of
unlikely.

A plain create-exclusive lock file in the campaign directory, holding the
owner's identity, with bounded stale recovery:

* a lock whose PID is dead is reclaimed immediately;
* a lock older than ``stale_after_s`` is reclaimed even if its PID cannot be
  checked (a hung process must not deadlock research forever);
* a live younger lock makes the second caller WAIT up to ``wait_s`` and then
  raise :class:`AdvanceLockBusy` - reported, never silent.

No science, no schedule, no write outside the campaign directory.
"""
from __future__ import annotations

import ctypes
import datetime as _dt
import json
import os
import time
from pathlib import Path

from . import CAMPAIGN_ID, campaign_dir

CALCULATION_OWNER = "alpha_agent.r46.runlock"

LOCK_NAME = "r46_advance.lock"

#: A lock this old is stale even when PID liveness cannot be decided. The
#: slowest observed full advance (fresh panel rebuild ~361s plus lanes) is
#: minutes; an hour is comfortably past any honest run.
DEFAULT_STALE_AFTER_S = 3600

DEFAULT_WAIT_S = 900
_POLL_S = 2.0

_STILL_ACTIVE = 259
_PROCESS_QUERY_LIMITED_INFORMATION = 0x1000


class AdvanceLockBusy(RuntimeError):
    """Another advance holds the lock and did not release it in time."""


def lock_path(campaign_id: str = CAMPAIGN_ID) -> Path:
    return campaign_dir(campaign_id) / LOCK_NAME


def pid_alive(pid: int):
    """True/False when decidable, None when the platform will not say."""
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return None
    if pid <= 0:
        return None
    if os.name != "nt":                       # pragma: no cover - Windows estate
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False
        except OSError:
            return None
    try:
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.OpenProcess(_PROCESS_QUERY_LIMITED_INFORMATION,
                                      False, pid)
        if not handle:
            return False
        try:
            code = ctypes.c_ulong()
            if not kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
                return None
            return code.value == _STILL_ACTIVE
        finally:
            kernel32.CloseHandle(handle)
    except Exception:                         # noqa: BLE001 - undecidable
        return None


def _read(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _age_seconds(path: Path) -> float:
    try:
        return max(0.0, time.time() - path.stat().st_mtime)
    except OSError:
        return 0.0


def _try_create(path: Path, holder: str) -> bool:
    body = json.dumps({
        "holder": holder,
        "pid": os.getpid(),
        "acquired_at_utc": _dt.datetime.now(_dt.timezone.utc)
        .strftime("%Y-%m-%dT%H:%M:%SZ"),
        "owner": CALCULATION_OWNER,
    })
    try:
        fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return False
    try:
        os.write(fd, body.encode("utf-8"))
    finally:
        os.close(fd)
    return True


def _reclaim_if_stale(path: Path, stale_after_s: float) -> dict:
    """Remove a dead or ancient lock. Returns what was reclaimed, if anything."""
    info = _read(path)
    alive = pid_alive(info.get("pid"))
    age = _age_seconds(path)
    if alive is False or age > float(stale_after_s):
        try:
            path.unlink()
        except OSError:
            return {}
        return {"reclaimed": True, "previous_holder": info.get("holder"),
                "previous_pid": info.get("pid"),
                "previous_age_seconds": round(age, 1),
                "pid_alive": alive}
    return {}


def acquire_path(path: Path, holder: str, *,
                 wait_s: float = DEFAULT_WAIT_S,
                 stale_after_s: float = DEFAULT_STALE_AFTER_S) -> dict:
    """Path-addressed acquire, for callers with their own lock scope (R52)."""
    path = Path(path)
    deadline = time.monotonic() + max(0.0, float(wait_s))
    reclaimed = {}
    while True:
        if _try_create(path, holder):
            return {"acquired": True, "holder": holder, "path": str(path),
                    **({"reclaimed_stale": reclaimed} if reclaimed else {})}
        rec = _reclaim_if_stale(path, stale_after_s)
        if rec:
            reclaimed = rec
            continue
        if time.monotonic() >= deadline:
            info = _read(path)
            raise AdvanceLockBusy(
                "the lock at %s is held by %r (pid %s, age %.0fs) and was "
                "not released within %.0fs" % (
                    path, info.get("holder"), info.get("pid"),
                    _age_seconds(path), float(wait_s)))
        time.sleep(_POLL_S)


def release_path(path: Path, holder: str) -> bool:
    path = Path(path)
    info = _read(path)
    if info.get("holder") != holder or int(info.get("pid") or -1) != os.getpid():
        return False
    try:
        path.unlink()
        return True
    except OSError:
        return False


def state_path(path: Path) -> dict:
    path = Path(path)
    if not path.exists():
        return {"held": False, "path": str(path)}
    info = _read(path)
    return {"held": True, "path": str(path), "holder": info.get("holder"),
            "pid": info.get("pid"), "pid_alive": pid_alive(info.get("pid")),
            "age_seconds": round(_age_seconds(path), 1),
            "acquired_at_utc": info.get("acquired_at_utc")}


def acquire(holder: str, campaign_id: str = CAMPAIGN_ID, *,
            wait_s: float = DEFAULT_WAIT_S,
            stale_after_s: float = DEFAULT_STALE_AFTER_S) -> dict:
    """Block (bounded) until the campaign lock is held. Raises AdvanceLockBusy."""
    return acquire_path(lock_path(campaign_id), holder,
                        wait_s=wait_s, stale_after_s=stale_after_s)


def release(holder: str, campaign_id: str = CAMPAIGN_ID) -> bool:
    """Release only a lock this holder owns. Never someone else's."""
    return release_path(lock_path(campaign_id), holder)


class hold:
    """Context manager: with runlock.hold('drc'): advance(...)"""

    def __init__(self, holder: str, campaign_id: str = CAMPAIGN_ID, *,
                 wait_s: float = DEFAULT_WAIT_S,
                 stale_after_s: float = DEFAULT_STALE_AFTER_S):
        self.holder = holder
        self.campaign_id = campaign_id
        self.wait_s = wait_s
        self.stale_after_s = stale_after_s
        self.receipt = None

    def __enter__(self):
        self.receipt = acquire(self.holder, self.campaign_id,
                               wait_s=self.wait_s,
                               stale_after_s=self.stale_after_s)
        return self.receipt

    def __exit__(self, exc_type, exc, tb):
        release(self.holder, self.campaign_id)
        return False


def state(campaign_id: str = CAMPAIGN_ID) -> dict:
    """Read-only view for health reporting."""
    path = lock_path(campaign_id)
    if not path.exists():
        return {"held": False, "path": str(path)}
    info = _read(path)
    return {"held": True, "path": str(path), "holder": info.get("holder"),
            "pid": info.get("pid"), "pid_alive": pid_alive(info.get("pid")),
            "age_seconds": round(_age_seconds(path), 1),
            "acquired_at_utc": info.get("acquired_at_utc")}


__all__ = ["CALCULATION_OWNER", "LOCK_NAME", "DEFAULT_STALE_AFTER_S",
           "DEFAULT_WAIT_S", "AdvanceLockBusy", "lock_path", "pid_alive",
           "acquire", "release", "hold", "state",
           "acquire_path", "release_path", "state_path"]
