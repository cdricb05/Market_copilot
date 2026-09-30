"""R85 - the ONE fail-closed gate between a configured test database and any
destructive PostgreSQL test fixture (``create_all`` / ``drop_all`` / TRUNCATE).

WHY
---
Ten test modules and ``conftest.db_engine`` build an engine straight from
``PAPER_TRADER_TEST_DATABASE_URL`` and create, truncate and drop every table.
Nothing proved that URL was disposable: pointed at the operational database
(or at the same database under another spelling - ``localhost`` vs
``127.0.0.1``, a different user, a query string) it would have dropped it, and
the R84 write guard disarmed itself exactly when the two URLs were equal.

THE RULE
--------
A test database is admitted only when ALL of these are PROVEN:

1. ``PAPER_TRADER_TEST_DATABASE_URL`` is set and is a PostgreSQL URL;
2. it does not name the production target (resolved host, port, database);
3. the server identity of BOTH databases is read from the servers themselves -
   cluster ``system_identifier`` + database OID - and they differ;
4. the test database carries the designation comment
   ``COMMENT ON DATABASE <db> IS 'PAPER_TRADER_DISPOSABLE_TEST_DATABASE'``;
5. the production database does NOT carry it.

A name containing "test" proves nothing and is never consulted. Any probe that
fails, or any identity that cannot be read, is a refusal - never a pass. When
refused, the variable is REMOVED from this process's environment before any
test module imports, so every DB fixture skips instead of running DDL.

Nothing here ever prints a URL, a user or a password.
"""
from __future__ import annotations

import os
import socket
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import urlparse

TEST_URL_ENV = "PAPER_TRADER_TEST_DATABASE_URL"
PROD_URL_ENV = "PAPER_TRADER_DATABASE_URL"
DESIGNATION = "PAPER_TRADER_DISPOSABLE_TEST_DATABASE"

ADMITTED = "ADMITTED"
NOT_CONFIGURED = "NOT_CONFIGURED"
REFUSED = "REFUSED"

_REPO = Path(__file__).resolve().parents[1]
_state: dict = {"verdict": None}


# --------------------------------------------------------------------------- #
def production_url(env: Optional[dict] = None, env_file: Optional[Path] = None) -> Optional[str]:
    """The operational DSN from the environment, else from the repo ``.env`` -
    the same two sources ``config.Settings`` reads - so the gate stays effective
    when production is configured only in ``.env``."""
    env = os.environ if env is None else env
    if env.get(PROD_URL_ENV):
        return env[PROD_URL_ENV]
    path = _REPO / ".env" if env_file is None else env_file
    try:
        lines = path.read_text(encoding="utf-8-sig").splitlines()
    except OSError:
        return None
    for line in lines:
        s = line.strip()
        if s.startswith("#") or "=" not in s:
            continue
        key, _, val = s.partition("=")
        if key.strip() == PROD_URL_ENV:
            return val.strip().strip('"').strip("'") or None
    return None


def _hosts(host: str) -> frozenset:
    """Every address a host name resolves to (``localhost`` == ``127.0.0.1``)."""
    h = (host or "").strip().lower() or "localhost"
    out = {h}
    try:
        for info in socket.getaddrinfo(h, None):
            out.add(info[4][0])
    except OSError:
        pass
    if out & {"localhost", "127.0.0.1", "::1"}:
        out |= {"localhost", "127.0.0.1", "::1"}
    return frozenset(out)


def target(url: Optional[str]) -> Optional[dict]:
    """(hosts, port, database) of a PostgreSQL URL, or None when it is not one."""
    if not url:
        return None
    try:
        u = urlparse(str(url))
    except ValueError:
        return None
    if not u.scheme.lower().startswith("postgres"):
        return None
    db = (u.path or "").lstrip("/")
    if not db:
        return None
    try:
        port = u.port or 5432
    except ValueError:
        return None
    return {"hosts": _hosts(u.hostname or ""), "port": port, "database": db}


def same_target(a: Optional[dict], b: Optional[dict]) -> bool:
    return bool(a and b and a["port"] == b["port"] and a["database"] == b["database"]
                and a["hosts"] & b["hosts"])


def probe_identity(url: str) -> dict:
    """Read the database's identity FROM THE SERVER. Read-only; raises on failure."""
    from sqlalchemy import create_engine, text
    from sqlalchemy.pool import NullPool

    engine = create_engine(url, poolclass=NullPool, connect_args={"connect_timeout": 5})
    try:
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT (SELECT system_identifier FROM pg_control_system()), "
                "d.oid, d.datname, shobj_description(d.oid, 'pg_database') "
                "FROM pg_database d WHERE d.datname = current_database()")).one()
    finally:
        engine.dispose()
    if row[0] is None or row[1] is None:
        raise RuntimeError("server did not report a cluster/database identity")
    return {"cluster": str(row[0]), "oid": int(row[1]), "database": str(row[2]),
            "designation": row[3]}


# --------------------------------------------------------------------------- #
def validate(test_url: Optional[str], prod_url: Optional[str], *,
             probe: Callable[[str], dict] = probe_identity) -> dict:
    """The verdict. ``probe`` is injectable so the refusals are testable hermetically."""
    def refuse(reason: str, **kw) -> dict:
        return dict({"state": REFUSED, "reason": reason}, **kw)

    if not test_url:
        return {"state": NOT_CONFIGURED,
                "reason": "%s is not set; every PostgreSQL test skips." % TEST_URL_ENV}
    t = target(test_url)
    if t is None:
        return refuse("TEST_URL_INVALID: not a PostgreSQL URL naming a database")
    p = target(prod_url) if prod_url else None
    if prod_url and p is None:
        return refuse("PRODUCTION_URL_UNPARSEABLE: cannot prove the test target differs")
    if test_url == prod_url or same_target(t, p):
        return refuse("TEST_IS_PRODUCTION: the test URL resolves to the production "
                      "host, port and database")
    try:
        tid = probe(test_url)
    except Exception as exc:  # noqa: BLE001 - any failure is a refusal
        return refuse("TEST_IDENTITY_UNAVAILABLE: %s" % type(exc).__name__)
    if prod_url:
        try:
            pid = probe(prod_url)
        except Exception as exc:  # noqa: BLE001
            return refuse("PRODUCTION_IDENTITY_UNAVAILABLE: %s; the two databases "
                          "cannot be proven distinct" % type(exc).__name__)
        if (tid["cluster"], tid["oid"]) == (pid["cluster"], pid["oid"]):
            return refuse("TEST_IS_PRODUCTION: same cluster system_identifier and "
                          "database OID under a different URL spelling")
        if pid.get("designation") == DESIGNATION:
            return refuse("PRODUCTION_CARRIES_TEST_DESIGNATION: refusing an "
                          "ambiguous designation")
    if tid.get("designation") != DESIGNATION:
        return refuse("TEST_NOT_DESIGNATED: the database is not marked with "
                      "COMMENT ON DATABASE ... IS '%s'" % DESIGNATION)
    return {"state": ADMITTED, "reason": "identity proven distinct and designated",
            "database": tid["database"], "production_checked": bool(prod_url)}


def enforce(*, env: Optional[dict] = None,
            probe: Callable[[str], dict] = probe_identity) -> dict:
    """Run once at conftest import. A refused URL is removed from ``env``."""
    env = os.environ if env is None else env
    verdict = validate(env.get(TEST_URL_ENV), production_url(env), probe=probe)
    if verdict["state"] == REFUSED:
        env.pop(TEST_URL_ENV, None)
    _state["verdict"] = verdict
    _state["admitted_url"] = (env.get(TEST_URL_ENV) if verdict["state"] == ADMITTED
                              else None)
    return verdict


def verdict() -> Optional[dict]:
    return _state["verdict"]


def is_admitted(url: Optional[str]) -> bool:
    """True only for the exact URL proven at import. Checked at use, because test
    modules legitimately point ``PAPER_TRADER_DATABASE_URL`` at the test database
    for the app under test, so production can no longer be re-read from the
    environment after collection - and a URL swapped in after the gate ran was
    never proven at all."""
    return bool(url) and url == _state.get("admitted_url")


__all__ = ["DESIGNATION", "ADMITTED", "NOT_CONFIGURED", "REFUSED", "production_url",
           "target", "same_target", "probe_identity", "validate", "enforce", "verdict"]
