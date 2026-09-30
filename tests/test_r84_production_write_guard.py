"""R84 - the pytest process may not write a production store (the guard itself).

Proves the guard installed by ``tests/conftest.py`` refuses writes under the
protected roots and leaves reads alone. Every deliberate refusal here is
removed from the guard's record afterwards, so the session verdict only ever
reflects an UNINTENDED production write.

Nothing here writes production: each refusal happens before the write.
"""
from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

import pytest

guard = sys.modules["_paper_trader_production_write_guard"]
ROOT = guard.PROTECTED_ROOTS[0]
PROBE = ROOT / "__r84_guard_probe_must_never_exist__"


@pytest.fixture(autouse=True)
def _forget_deliberate_refusals():
    before = len(guard.VIOLATIONS)
    yield
    del guard.VIOLATIONS[before:]


def test_the_guard_is_active_in_this_process():
    assert guard._state["installed"] and guard._state["active"]
    assert guard.is_protected(ROOT / "portfolio_decisions" / "decisions.json")
    assert guard.is_protected(Path.home() / ".paper_trader" / "paper_trading_desk")
    assert not guard.is_protected(Path(__file__))


def test_a_write_mode_open_is_refused(tmp_path):
    with pytest.raises(PermissionError):
        open(PROBE, "w")
    with pytest.raises(PermissionError):
        os.open(str(PROBE), os.O_WRONLY | os.O_CREAT)
    with pytest.raises(PermissionError):
        PROBE.write_text("x")
    assert not PROBE.exists()
    (tmp_path / "ok.txt").write_text("fine")          # a temp root is untouched


def test_rename_remove_and_new_mkdir_are_refused(tmp_path):
    src = tmp_path / "a.txt"
    src.write_text("x")
    with pytest.raises(PermissionError):
        os.replace(src, PROBE)
    with pytest.raises(PermissionError):
        os.remove(PROBE)
    with pytest.raises(PermissionError):
        PROBE.mkdir()
    assert not PROBE.exists()
    # an EXISTING protected directory may be "ensured" (a no-op, not a write)
    ROOT.mkdir(parents=True, exist_ok=True) if ROOT.exists() else None


@pytest.mark.skipif(not ROOT.exists(), reason="no production data root on this host")
def test_reads_of_production_are_allowed():
    entries = list(ROOT.iterdir())
    assert entries is not None


def test_sqlite_writes_are_denied_and_reads_allowed(tmp_path, monkeypatch):
    # A protected-path database is simulated by widening the guard to tmp_path.
    monkeypatch.setattr(guard, "_ROOTS_NC", guard._ROOTS_NC + (
        os.path.normcase(os.path.abspath(str(tmp_path / "prot"))),))
    db = tmp_path / "prot" / "store.sqlite"
    guard.suspend()
    try:
        db.parent.mkdir()
        c = sqlite3.connect(str(db))
        c.execute("CREATE TABLE t (x INTEGER)")
        c.execute("INSERT INTO t VALUES (1)")
        c.commit()
        c.close()
    finally:
        guard.resume()
    conn = sqlite3.connect(str(db))
    try:
        assert conn.execute("SELECT x FROM t").fetchall() == [(1,)]
        conn.execute("PRAGMA busy_timeout=1000")          # a read handle's pragma
        assert conn.execute("PRAGMA table_info(t)").fetchall()   # introspection
        with pytest.raises(sqlite3.DatabaseError):
            conn.execute("INSERT INTO t VALUES (2)")
        with pytest.raises(sqlite3.DatabaseError):
            conn.execute("CREATE TABLE IF NOT EXISTS t (x INTEGER)")
        with pytest.raises(sqlite3.DatabaseError):
            conn.execute("PRAGMA user_version=7")
        assert conn.execute("SELECT count(*) FROM t").fetchone() == (1,)
    finally:
        conn.close()
    ro = sqlite3.connect("file:%s?mode=ro" % db.as_posix(), uri=True)
    assert ro.execute("SELECT count(*) FROM t").fetchone() == (1,)
    ro.close()


def test_a_swallowed_refusal_is_still_recorded():
    before = len(guard.VIOLATIONS)
    try:
        open(PROBE, "a")
    except PermissionError:
        pass
    assert len(guard.VIOLATIONS) == before + 1
    assert guard.VIOLATIONS[-1]["kind"] == "open-for-write"
