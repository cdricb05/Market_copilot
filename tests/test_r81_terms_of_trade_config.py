"""R81 - the terms-of-trade vintage backfill is a BOUNDED SIBLING, not a
production mode switch.

THE DEFECT THIS GUARDS
----------------------
``alpha_agent/collectors/fred_alfred.py`` treats ``historical_backfill`` as an
EXCLUSIVE mode: ``if hb: return self._collect_historical(...)``. Adding that key
to the production Stage 2 config would therefore stop the 12-series rolling
macro collection (DGS2, DGS10, T10Y2Y, SOFR, NFCI, VIXCLS ...) that the estate
runs every session, and nothing would say so - the run would still print
ALPHA_AGENT_STAGE2_READY.

R81 needed the backfill path to certify the terms-of-trade family, so it added a
bounded one-time sibling config instead of switching production. These tests pin
that arrangement: production stays rolling, the sibling stays backfill-only, and
the two never collect the same series into the same root.

Hermetic: reads two JSON files. No network, no collection, no research memory.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]
PROD = _REPO / "configs" / "alpha_agent" / "stage2_ingestion.json"
SIBLING = (_REPO / "configs" / "alpha_agent"
           / "stage2_terms_of_trade_vintage_backfill.json")

#: The rolling macro series the production config must keep collecting.
_PRODUCTION_SERIES = {"DGS2", "DGS10", "T10Y2Y", "DFF", "SOFR", "BAMLH0A0HYM2",
                      "BAMLC0A0CM", "CPIAUCSL", "UNRATE", "ICSA", "NFCI",
                      "VIXCLS"}

#: The 14 'All Industries' import-price-by-origin series plus the two end-use
#: aggregates. This IS the certified family; a silent change to it invalidates
#: DATA_R81_D1_TERMS_OF_TRADE_VINTAGE_CERTIFICATION.json.
_TOT_SERIES = {"ASEANTOT", "ASNRETOT", "CANTOT", "CHNTOT", "EECTOT", "FRNTOT",
               "GERTOT", "INDUSTOT", "JPNTOT", "LATTOT", "MEXTOT", "OASTOT",
               "PRIMTOT", "UKTOT", "IQ", "IR"}


def _load(p: Path) -> dict:
    return json.loads(p.read_text(encoding="utf-8"))


def _fred(cfg: dict) -> dict:
    return cfg["sources"]["fred_alfred"]


def _series(cfg: dict) -> set:
    return {s["series_id"] for s in _fred(cfg)["series_allowlist"]}


@pytest.fixture(scope="module")
def prod() -> dict:
    return _load(PROD)


@pytest.fixture(scope="module")
def sibling() -> dict:
    return _load(SIBLING)


# --------------------------------------------------------------------------- #
# Production must stay ROLLING
# --------------------------------------------------------------------------- #
def test_production_fred_has_no_historical_backfill_key(prod):
    """The key is an EXCLUSIVE mode switch, so its presence here would end the
    rolling collection silently."""
    assert "historical_backfill" not in _fred(prod), (
        "configs/alpha_agent/stage2_ingestion.json must NOT carry "
        "historical_backfill: alpha_agent/collectors/fred_alfred.py returns "
        "_collect_historical() whenever it is present, which would stop the "
        "45-day rolling macro collection for every production session")


def test_production_keeps_its_rolling_window_and_series(prod):
    f = _fred(prod)
    assert int(f["observation_window_days"]) == 45
    assert f["use_alfred_vintages"] is True
    assert _series(prod) == _PRODUCTION_SERIES


def test_production_fred_stays_enabled(prod):
    assert _fred(prod)["enabled"] is True


# --------------------------------------------------------------------------- #
# The sibling must stay a BOUNDED ONE-TIME BACKFILL
# --------------------------------------------------------------------------- #
def test_sibling_enables_only_fred_alfred(sibling):
    """A fred-only run must not be able to publish a latest.json that speaks
    for sources it never collected."""
    enabled = [n for n, b in sibling["sources"].items() if b.get("enabled")]
    assert enabled == ["fred_alfred"], enabled


def test_sibling_is_in_backfill_mode_and_not_rolling(sibling):
    f = _fred(sibling)
    hb = f.get("historical_backfill")
    assert hb, "the sibling exists to run the backfill path"
    assert "observation_window_days" not in f, (
        "a rolling window key here is dead configuration and invites the "
        "reader to think both modes run")
    # The first realtime chunk must START at observation_start so the backfill's
    # ``is_first`` branch keeps the 2010-03-16 archive baseline instead of
    # dropping it as a clamped carry-in.
    assert hb["observation_start"] <= "2010-03-16"
    assert int(hb["realtime_chunk_years"]) >= 1
    assert int(hb["min_chunk_years"]) >= 1


def test_sibling_carries_the_certified_terms_of_trade_family(sibling):
    assert _series(sibling) == _TOT_SERIES


def test_every_sibling_series_is_labelled_terms_of_trade(sibling):
    fams = {s.get("macro_family") for s in _fred(sibling)["series_allowlist"]}
    assert fams == {"terms_of_trade"}, fams


# --------------------------------------------------------------------------- #
# The two configs must not collide
# --------------------------------------------------------------------------- #
def test_the_two_allowlists_are_disjoint(prod, sibling):
    """Overlap would make one root's cursor answer for the other's mode."""
    assert not (_series(prod) & _series(sibling))


def test_sibling_declares_its_derivation_and_carries_no_credential(sibling):
    """Env var NAMES only. ``api_key`` DOES appear in the inherited
    ``secret_redaction.redacted_query_params`` - that is the redaction list
    doing its job - so the invariant is about VALUES, not the word."""
    assert sibling.get("derived_from") == "configs/alpha_agent/stage2_ingestion.json"
    assert _fred(sibling)["allowed_env_vars"] == ["FRED_API_KEY",
                                                  "PAPER_TRADER_FRED_API_KEY"]
    assert "api_key" in sibling["secret_redaction"]["redacted_query_params"]
    # No 32-hex-character token (a FRED key's shape) is persisted anywhere.
    import re
    assert not re.search(r"\b[0-9a-f]{32}\b", json.dumps(sibling)), (
        "a credential-shaped token must never be written into a config")


def test_sibling_inherits_the_production_safety_block(prod, sibling):
    assert sibling["safety"] == prod["safety"]
    assert sibling["output_contract"] == prod["output_contract"]
    assert sibling["limits"] == prod["limits"]


def test_the_bounded_run_stays_inside_the_declared_limits(sibling):
    """16 series x 4 five-year chunks = 64 requests, and the measured run
    normalised 8,703 records. Both must remain inside the inherited caps."""
    lim = sibling["limits"]
    n_series = len(_fred(sibling)["series_allowlist"])
    hb = _fred(sibling)["historical_backfill"]
    worst_requests = n_series * int(hb["max_requests_per_series"])
    assert n_series * 5 <= int(lim["max_raw_objects_per_source"]), (
        "a 5-chunk-per-series backfill must fit the raw-object cap")
    assert worst_requests >= 64
    assert 8703 < int(lim["max_normalized_records_per_source"])
