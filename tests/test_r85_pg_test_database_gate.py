"""R85 - the fail-closed PostgreSQL test-database gate refuses every dangerous
configuration BEFORE a destructive fixture can run.

HERMETIC: the server identity probe is injected; no database is contacted.
"""
from __future__ import annotations

import sys

import pytest

gate = sys.modules["_paper_trader_pg_test_database_gate"]
guard = sys.modules["_paper_trader_production_write_guard"]

PROD = "postgresql+psycopg2://u:p@localhost:5432/paper_trader"
TEST = "postgresql+psycopg2://u:p@localhost:5432/pt_disposable"

PROD_ID = {"cluster": "7001", "oid": 16384, "database": "paper_trader", "designation": None}
TEST_ID = {"cluster": "7001", "oid": 24576, "database": "pt_disposable",
           "designation": gate.DESIGNATION}


def _probe(table):
    def probe(url):
        for key, ident in table.items():
            if url.endswith("/" + key):
                if isinstance(ident, Exception):
                    raise ident
                return ident
        raise ConnectionError("unreachable")
    return probe


GOOD = _probe({"paper_trader": PROD_ID, "pt_disposable": TEST_ID})


def test_a_designated_distinct_database_is_admitted():
    assert gate.validate(TEST, PROD, probe=GOOD)["state"] == gate.ADMITTED


def test_missing_test_url_is_not_configured_and_admits_nothing():
    assert gate.validate(None, PROD, probe=GOOD)["state"] == gate.NOT_CONFIGURED


@pytest.mark.parametrize("bad", ["sqlite:///x.db", "postgresql://u:p@localhost:5432/",
                                 "not a url", "postgresql://u:p@localhost:notaport/db"])
def test_an_invalid_test_identity_is_refused(bad):
    v = gate.validate(bad, PROD, probe=GOOD)
    assert v["state"] == gate.REFUSED and v["reason"].startswith("TEST_URL_INVALID")


def test_the_production_url_itself_is_refused():
    v = gate.validate(PROD, PROD, probe=GOOD)
    assert v["state"] == gate.REFUSED and v["reason"].startswith("TEST_IS_PRODUCTION")


@pytest.mark.parametrize("alias", [
    "postgresql://other:secret@127.0.0.1:5432/paper_trader",
    "postgresql+psycopg2://u:p@localhost/paper_trader?sslmode=disable",
    "postgresql://u:p@[::1]:5432/paper_trader"])
def test_production_under_another_spelling_is_refused_syntactically(alias):
    v = gate.validate(alias, PROD, probe=GOOD)
    assert v["state"] == gate.REFUSED and v["reason"].startswith("TEST_IS_PRODUCTION")


def test_production_behind_a_different_host_name_is_refused_by_server_identity():
    # e.g. a DNS alias or port-forward the URL parser cannot see through
    probe = _probe({"paper_trader": PROD_ID, "aliasdb": dict(PROD_ID)})
    v = gate.validate("postgresql://u:p@db-alias:6543/aliasdb", PROD, probe=probe)
    assert v["state"] == gate.REFUSED
    assert "same cluster system_identifier and database OID" in v["reason"]


def test_a_name_containing_test_is_not_a_designation():
    probe = _probe({"paper_trader": PROD_ID,
                    "paper_trader_test": dict(TEST_ID, designation=None)})
    v = gate.validate("postgresql://u:p@localhost:5432/paper_trader_test", PROD, probe=probe)
    assert v["state"] == gate.REFUSED and v["reason"].startswith("TEST_NOT_DESIGNATED")


def test_an_unreachable_test_database_is_refused():
    probe = _probe({"paper_trader": PROD_ID, "pt_disposable": ConnectionError()})
    assert gate.validate(TEST, PROD, probe=probe)["reason"].startswith(
        "TEST_IDENTITY_UNAVAILABLE")


def test_an_unverifiable_production_identity_is_refused():
    probe = _probe({"paper_trader": PermissionError(), "pt_disposable": TEST_ID})
    assert gate.validate(TEST, PROD, probe=probe)["reason"].startswith(
        "PRODUCTION_IDENTITY_UNAVAILABLE")


def test_a_production_database_carrying_the_designation_is_refused():
    probe = _probe({"paper_trader": dict(PROD_ID, designation=gate.DESIGNATION),
                    "pt_disposable": TEST_ID})
    assert gate.validate(TEST, PROD, probe=probe)["reason"].startswith(
        "PRODUCTION_CARRIES_TEST_DESIGNATION")


def test_production_configured_only_in_dotenv_is_still_found(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("# c\nPAPER_TRADER_DATABASE_URL=%s\n" % PROD, encoding="utf-8")
    assert gate.production_url({}, env_file) == PROD
    v = gate.validate(PROD, gate.production_url({}, env_file), probe=GOOD)
    assert v["state"] == gate.REFUSED


def test_enforce_removes_a_refused_url_so_every_destructive_fixture_skips(monkeypatch):
    # the session's real admission must survive this test
    monkeypatch.setattr(gate, "_state", {"verdict": None})
    env = {gate.TEST_URL_ENV: PROD, gate.PROD_URL_ENV: PROD}
    v = gate.enforce(env=env, probe=GOOD)
    assert v["state"] == gate.REFUSED and gate.TEST_URL_ENV not in env
    env = {gate.TEST_URL_ENV: TEST, gate.PROD_URL_ENV: PROD}
    assert gate.enforce(env=env, probe=GOOD)["state"] == gate.ADMITTED
    assert env[gate.TEST_URL_ENV] == TEST


def test_the_verdict_never_contains_a_credential():
    for url in (PROD, TEST, "postgresql://u:p@db-alias:6543/aliasdb"):
        v = gate.validate(url, PROD, probe=GOOD)
        assert "u:p" not in repr(v) and ":p@" not in repr(v)


def test_the_write_guard_stays_armed_when_test_equals_production(monkeypatch):
    monkeypatch.setenv("PAPER_TRADER_DATABASE_URL", PROD)
    monkeypatch.setenv("PAPER_TRADER_TEST_DATABASE_URL", PROD)
    assert guard.production_database() == ("localhost", 5432, "paper_trader")
    assert guard._same_host("127.0.0.1", "localhost")


def test_the_guard_opt_out_is_refused_while_a_test_database_is_configured(monkeypatch):
    monkeypatch.setenv("PAPER_TRADER_TEST_PRODUCTION_WRITE_GUARD", "0")
    monkeypatch.setenv("PAPER_TRADER_TEST_DATABASE_URL", TEST)
    assert guard.install() is True
