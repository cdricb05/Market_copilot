"""tests/_production_write_guard.py - R84: the pytest process may not write production.

The test suite runs inside the DEPLOYED checkout, so every module default that
resolves to a production store (``D:\\Stock_Prediction_app_data\\...``,
``~\\.paper_trader\\...``, the operator's Postgres database) is one forgotten
fixture away from a test writing the operator's live governance, execution or
research state. Before R84 the broad suite did exactly that to the live R59
research memory (a schema migration applied by a test run).

This guard makes the boundary MECHANICAL instead of conventional. It is
installed by ``tests/conftest.py`` at import time - before any test module is
collected - and, for the lifetime of the pytest process:

* file system: a Python audit hook refuses (``PermissionError``) every write-mode
  ``open``, ``os.open``, rename / replace, remove, rmdir, new mkdir, truncate,
  chmod / utime and ``shutil.rmtree`` whose target lies under a protected root.
  Reads are untouched.
* SQLite: every connection to a database under a protected root gets an
  authorizer that denies INSERT / UPDATE / DELETE / DDL / writing PRAGMAs. Reads,
  including a read handle's ``query_only`` fallback, are untouched.
* Postgres: SQLAlchemy engines bound to the PRODUCTION database (the one
  ``PAPER_TRADER_DATABASE_URL`` names, unless it is also the test database)
  refuse every data- or schema-changing statement.

Every refusal is recorded, and ``pytest_sessionfinish`` in conftest turns a
non-empty record into a failed session even when the code under test swallowed
the exception: a swallowed refusal is still a code path that would have written
production. Child processes a test launches are outside this process; the
release ceremony fingerprints the stores before and after to cover them.

Scope is the pytest process only. The operator's shell, the backend and every
worker are unaffected. ``PAPER_TRADER_TEST_PRODUCTION_WRITE_GUARD=0`` disables
it for a deliberate, supervised one-off; nothing in the repository sets that.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import sys
from pathlib import Path
from typing import Any, Optional
from urllib.parse import unquote, urlparse

#: The production store roots, as the modules that own them declare them.
PROTECTED_ROOTS = (
    Path(r"D:\Stock_Prediction_app_data"),
    Path.home() / ".paper_trader",
)

_ROOTS_NC = tuple(os.path.normcase(os.path.abspath(str(r))) for r in PROTECTED_ROOTS)
_WRITE_FLAGS = (os.O_WRONLY | os.O_RDWR | os.O_APPEND | os.O_CREAT | os.O_TRUNC)
_MUTATING_EVENTS = {
    "os.rename": (0, 1), "os.remove": (0,), "os.rmdir": (0,), "os.truncate": (0,),
    "os.chmod": (0,), "os.utime": (0,), "os.link": (0, 1), "os.symlink": (0, 1),
    "shutil.rmtree": (0,), "shutil.move": (0, 1), "shutil.copyfile": (1,),
    "shutil.copytree": (1,),
}

VIOLATIONS: list[dict] = []
_state = {"active": False, "installed": False}


class ProductionWriteRefused(PermissionError):
    """A test tried to write a production store."""


def is_protected(path: Any) -> bool:
    if path is None or isinstance(path, int):
        return False
    try:
        raw = os.fsdecode(os.fspath(path))
    except TypeError:
        return False
    if not raw or raw == ":memory:":
        return False
    p = os.path.normcase(os.path.abspath(raw))
    return any(p == r or p.startswith(r + os.sep) for r in _ROOTS_NC)


def _current_test() -> Optional[str]:
    return os.environ.get("PYTEST_CURRENT_TEST")


def _refuse(kind: str, target: Any, detail: str = "") -> None:
    rec = {"kind": kind, "target": str(target), "detail": detail,
           "test": _current_test()}
    VIOLATIONS.append(rec)
    raise ProductionWriteRefused(
        "R84 production write guard: the pytest process may not %s %s (%s). "
        "Point the owner at a tmp_path / fixture root instead." % (kind, target, detail))


def _open_is_write(mode: Any, flags: Any) -> bool:
    if isinstance(mode, str):
        return any(c in mode for c in "wax+")
    if isinstance(flags, int):
        return bool(flags & _WRITE_FLAGS)
    return False


def _audit(event: str, args: tuple) -> None:
    if not _state["active"]:
        return
    if event == "open":
        path, mode, flags = (tuple(args) + (None, None, None))[:3]
        if isinstance(path, int) or path is None:
            return
        if _open_is_write(mode, flags) and is_protected(path):
            _refuse("open-for-write", path, "mode=%r flags=%r" % (mode, flags))
        return
    if event == "os.mkdir":
        path = args[0] if args else None
        if is_protected(path) and not os.path.exists(os.fsdecode(os.fspath(path))):
            _refuse("mkdir", path)
        return
    idx = _MUTATING_EVENTS.get(event)
    if idx is None:
        return
    for i in idx:
        if i < len(args) and is_protected(args[i]):
            _refuse(event, args[i])


# --------------------------------------------------------------------------- #
# SQLite
# --------------------------------------------------------------------------- #
def _sqlite_target(db: Any) -> Optional[str]:
    """The file a sqlite3.connect() argument names, or None when read-only by URI."""
    if db is None:
        return None
    try:
        s = os.fsdecode(os.fspath(db))
    except TypeError:
        return None
    if s.startswith("file:"):
        u = urlparse(s)
        if "mode=ro" in (u.query or "") or "immutable=1" in (u.query or ""):
            return None
        path = unquote(u.path or s[5:].split("?", 1)[0])
        if re.match(r"^/[A-Za-z]:", path):
            path = path[1:]
        return path
    return s


_DENY_CODES = {getattr(sqlite3, n) for n in (
    "SQLITE_INSERT", "SQLITE_UPDATE", "SQLITE_DELETE", "SQLITE_ALTER_TABLE",
    "SQLITE_CREATE_INDEX", "SQLITE_CREATE_TABLE", "SQLITE_CREATE_TEMP_INDEX",
    "SQLITE_CREATE_TEMP_TABLE", "SQLITE_CREATE_TEMP_TRIGGER",
    "SQLITE_CREATE_TEMP_VIEW", "SQLITE_CREATE_TRIGGER", "SQLITE_CREATE_VIEW",
    "SQLITE_DROP_INDEX", "SQLITE_DROP_TABLE", "SQLITE_DROP_TEMP_INDEX",
    "SQLITE_DROP_TEMP_TABLE", "SQLITE_DROP_TEMP_TRIGGER", "SQLITE_DROP_TEMP_VIEW",
    "SQLITE_DROP_TRIGGER", "SQLITE_DROP_VIEW", "SQLITE_REINDEX", "SQLITE_ANALYZE",
    "SQLITE_CREATE_VTABLE", "SQLITE_DROP_VTABLE") if hasattr(sqlite3, n)}
#: Pragmas a READ handle legitimately runs with an argument.
_SAFE_PRAGMAS = {"journal_mode", "busy_timeout", "foreign_keys", "query_only",
                 "synchronous", "cache_size", "temp_store", "mmap_size",
                 "wal_autocheckpoint", "read_uncommitted", "cache_spill",
                 # introspection pragmas whose argument names a thing to READ
                 "table_info", "table_xinfo", "table_list", "index_list",
                 "index_info", "index_xinfo", "foreign_key_list",
                 "foreign_key_check", "integrity_check", "quick_check"}


_ORIGINAL_SQLITE_CONNECT = sqlite3.connect


def _guarded_sqlite_connect(database, *args, **kwargs):  # noqa: ANN001
    """``sqlite3.connect`` that arms the write authorizer on a protected database.

    (An audit hook cannot do this: ``sqlite3.connect/handle`` fires before the
    connection is initialised, and ``set_authorizer`` then refuses.)
    """
    conn = _ORIGINAL_SQLITE_CONNECT(database, *args, **kwargs)
    if _state["active"]:
        target = _sqlite_target(database)
        if target is not None and is_protected(target):
            _install_sqlite_authorizer(conn, target)
    return conn


def _install_sqlite_authorizer(conn: Any, target: str) -> None:
    def _auth(action, arg1, arg2, dbname, source):  # noqa: ANN001
        if dbname == "temp" or arg1 in ("sqlite_temp_master",):
            return sqlite3.SQLITE_OK
        if action in _DENY_CODES:
            VIOLATIONS.append({"kind": "sqlite-write", "target": target,
                               "detail": "action=%s arg=%s" % (action, arg1),
                               "test": _current_test()})
            return sqlite3.SQLITE_DENY
        if action == sqlite3.SQLITE_PRAGMA and arg2 is not None \
                and str(arg1).lower() not in _SAFE_PRAGMAS:
            VIOLATIONS.append({"kind": "sqlite-pragma-write", "target": target,
                               "detail": "%s=%s" % (arg1, arg2),
                               "test": _current_test()})
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK
    try:
        conn.set_authorizer(_auth)
    except Exception:  # noqa: BLE001 - never mask the caller's own error
        pass


# --------------------------------------------------------------------------- #
# Postgres (SQLAlchemy)
# --------------------------------------------------------------------------- #
_PG_WRITE = re.compile(
    r"^\s*(insert|update|delete|create|alter|drop|truncate|merge|copy|grant|revoke|"
    r"comment|vacuum|reindex|cluster|lock|refresh)\b"
    r"|\binsert\s+into\b|\bdelete\s+from\b|\bupdate\s+\S+\s+set\b",
    re.IGNORECASE)


def _db_name(url: Optional[str]) -> Optional[tuple]:
    if not url:
        return None
    try:
        u = urlparse(url.replace("postgresql+psycopg2://", "postgresql://"))
        return ((u.hostname or "").lower(), u.port or 5432, (u.path or "").lstrip("/"))
    except Exception:  # noqa: BLE001
        return None


def production_database() -> Optional[tuple]:
    prod = _db_name(os.environ.get("PAPER_TRADER_DATABASE_URL"))
    test = _db_name(os.environ.get("PAPER_TRADER_TEST_DATABASE_URL"))
    if prod is None or prod == test:
        return None
    return prod


def _install_sqlalchemy_guard() -> None:
    try:
        from sqlalchemy import event
        from sqlalchemy.engine import Engine
    except Exception:  # noqa: BLE001
        return
    prod = production_database()
    if prod is None:
        return

    @event.listens_for(Engine, "before_cursor_execute")
    def _pg_guard(conn, cursor, statement, parameters, context, executemany):  # noqa: ANN001
        if not _state["active"]:
            return
        u = conn.engine.url
        key = ((u.host or "").lower(), u.port or 5432, u.database or "")
        if key == prod and _PG_WRITE.search(statement or ""):
            _refuse("postgres-write", "%s/%s" % (u.host, u.database),
                    (statement or "").strip().split("\n", 1)[0][:120])


# --------------------------------------------------------------------------- #
def install() -> bool:
    if os.environ.get("PAPER_TRADER_TEST_PRODUCTION_WRITE_GUARD", "1") == "0":
        return False
    if not _state["installed"]:
        sys.addaudithook(_audit)
        sqlite3.connect = _guarded_sqlite_connect
        _install_sqlalchemy_guard()
        _state["installed"] = True
    _state["active"] = True
    return True


def suspend() -> None:
    """For the guard's own fixture bookkeeping only (copying a snapshot)."""
    _state["active"] = False


def resume() -> None:
    if _state["installed"]:
        _state["active"] = True


def report(path: Optional[str] = None) -> dict:
    body = {"violation_count": len(VIOLATIONS), "violations": VIOLATIONS[:500],
            "protected_roots": [str(r) for r in PROTECTED_ROOTS],
            "postgres_production_database": list(production_database() or [])[2:] or None}
    if path:
        try:
            _state["active"] = False
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            Path(path).write_text(json.dumps(body, indent=2), encoding="utf-8")
        except Exception:  # noqa: BLE001
            pass
    return body
