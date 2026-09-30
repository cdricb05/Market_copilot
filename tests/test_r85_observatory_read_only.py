"""R85 - the read-only Alpha Agent observatory must never migrate a research store.

Before R85 every observatory read constructed a WRITER (tournament
CandidateRegistry, TelegramStore, ResearchQueue): it switched journal mode,
ran CREATE TABLE IF NOT EXISTS against the live store, and - when the R84
production write guard denied that - leaked the half-built sqlite handle.
"""
from __future__ import annotations

import gc
import hashlib
import json
import sqlite3
import warnings
from pathlib import Path

import pytest

from paper_trader.alpha_agent import autonomous_research as ar
from paper_trader.alpha_agent import evidence_observatory as eo
from paper_trader.alpha_agent import telegram_control as tc
from paper_trader.alpha_agent import tournament as tt


def _digest(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def _deny_writes(monkeypatch: pytest.MonkeyPatch, targets: set[str]) -> list:
    """Arm a write-denying authorizer on every handle opened on ``targets``,
    the same mechanism the production write guard uses."""
    refused: list = []
    deny = {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE,
            sqlite3.SQLITE_CREATE_TABLE, sqlite3.SQLITE_ALTER_TABLE,
            sqlite3.SQLITE_CREATE_INDEX, sqlite3.SQLITE_DROP_TABLE}
    real = sqlite3.connect

    def _connect(database, *a, **kw):  # noqa: ANN001
        conn = real(database, *a, **kw)
        name = str(database)
        if any(t in name for t in targets):
            def _auth(action, arg1, arg2, dbname, source):  # noqa: ANN001
                if dbname != "temp" and action in deny:
                    refused.append((name, action, arg1))
                    return sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_PRAGMA and arg2 is not None \
                        and str(arg1).lower() == "journal_mode":
                    refused.append((name, "pragma", arg1))
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK
            conn.set_authorizer(_auth)
        return conn

    monkeypatch.setattr(sqlite3, "connect", _connect)
    return refused


@pytest.fixture()
def stores(tmp_path: Path) -> dict:
    root = tmp_path / "stage8"
    reg = tt.CandidateRegistry(root / "tournament.sqlite")
    reg.close()
    tc.TelegramStore(root / "telegram_state.sqlite")
    ar.ResearchQueue(root / "autonomy.sqlite")
    cfg = {"autonomy": {"queue_db": str(root / "autonomy.sqlite")},
           "telegram": {"state_db": str(root / "telegram_state.sqlite")}}
    return {"root": root, "cfg": cfg,
            "files": sorted(root.glob("*.sqlite"))}


def test_observatory_reads_perform_zero_schema_writes(stores, monkeypatch) -> None:
    before = {p.name: _digest(p) for p in stores["files"]}
    refused = _deny_writes(monkeypatch, {"tournament.sqlite",
                                         "telegram_state.sqlite",
                                         "autonomy.sqlite"})
    with warnings.catch_warnings():
        warnings.simplefilter("error", ResourceWarning)
        snap = eo.autonomy_snapshot(stores["cfg"])
        tour = tt.tournament_snapshot(
            db_path=stores["root"] / "tournament.sqlite")
        gc.collect()
    assert refused == []
    assert snap["queue"].get("status") != "UNAVAILABLE", snap["queue"]
    assert "status" not in snap["telegram"], snap["telegram"]
    assert tour["status"] == "OK"
    assert {p.name: _digest(p) for p in stores["files"]} == before


def test_incompatible_tournament_store_is_unavailable_not_migrated(
        tmp_path: Path) -> None:
    db = tmp_path / "tournament.sqlite"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE unrelated (x INTEGER)")
    conn.commit()
    conn.close()
    before = _digest(db)
    with warnings.catch_warnings():
        warnings.simplefilter("error", ResourceWarning)
        out = tt.load_tournament(db_path=db)
        gc.collect()
    assert out["status"] == "UNAVAILABLE" and out["read_only"] is True
    assert _digest(db) == before
    conn = sqlite3.connect(str(db))
    try:
        names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master")}
    finally:
        conn.close()
    assert names == {"unrelated"}


def test_missing_stores_are_not_created(tmp_path: Path) -> None:
    root = tmp_path / "absent"
    cfg = {"autonomy": {"queue_db": str(root / "autonomy.sqlite")},
           "telegram": {"state_db": str(root / "telegram_state.sqlite")}}
    snap = eo.autonomy_snapshot(cfg)
    tour = tt.tournament_snapshot(db_path=root / "tournament.sqlite")
    assert snap["queue"]["status"] == "NOT_INITIALIZED"
    assert tour["status"] == "UNAVAILABLE"
    assert not root.exists()


def test_read_only_handles_refuse_writes(stores) -> None:
    reg = tt.CandidateRegistry(stores["root"] / "tournament.sqlite",
                               read_only=True)
    try:
        with pytest.raises(sqlite3.OperationalError):
            reg._conn.execute("CREATE TABLE forbidden (x INTEGER)")
    finally:
        reg.close()
    store = tc.TelegramStore(stores["root"] / "telegram_state.sqlite",
                             read_only=True)
    with pytest.raises(sqlite3.OperationalError):
        store.set_offset(7)
    with pytest.raises(FileNotFoundError):
        tt.CandidateRegistry(stores["root"] / "nope.sqlite", read_only=True)
    with pytest.raises(FileNotFoundError):
        tc.TelegramStore(stores["root"] / "nope.sqlite", read_only=True)


def test_writer_constructor_closes_handle_when_schema_is_refused(
        tmp_path: Path, monkeypatch) -> None:
    db = tmp_path / "tournament.sqlite"
    tt.CandidateRegistry(db).close()
    _deny_writes(monkeypatch, {"tournament.sqlite"})
    with warnings.catch_warnings():
        warnings.simplefilter("error", ResourceWarning)
        with pytest.raises(sqlite3.DatabaseError):
            tt.CandidateRegistry(db)
        gc.collect()


def test_observatory_payload_is_json_safe(stores) -> None:
    json.dumps(eo.autonomy_snapshot(stores["cfg"]))
