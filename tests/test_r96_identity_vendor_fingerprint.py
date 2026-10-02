"""R96 B3 — the vendor price-fingerprint identity method resolves only with every
corroboration present, and never through a name or ticker alone. Hermetic."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from paper_trader.alpha_agent import historical_identity as HI  # noqa: E402

SEC = {"security_id": "ngid:1", "security_start_date": "2001-01-02",
       "security_end_date": "2016-01-15"}
GOOD = {"vendor": "EODHD", "symbol": "ABC.US", "cik": "320193",
        "fingerprint_dates": 40, "median_abs_rel_diff": 0.0}
ISSUER = {"name": "ABC CORP", "first_filing": "1996-01-01", "last_filing": "2016-03-01"}


def test_resolves_with_fingerprint_and_sec_overlap():
    r = HI.vendor_fingerprint_mapping(SEC, GOOD, ISSUER)
    assert r is not None and r.status == HI.STATUS_RESOLVED
    assert r.cik == "0000320193"
    assert r.method == HI.METHOD_VENDOR_PRICE_FINGERPRINT
    assert r.evidence["corroboration"] == "price_fingerprint+sec_filing_date_overlap"


def test_price_mismatch_never_resolves():
    bad = dict(GOOD, median_abs_rel_diff=0.2)
    assert HI.vendor_fingerprint_mapping(SEC, bad, ISSUER) is None


def test_too_few_fingerprint_dates_never_resolves():
    assert HI.vendor_fingerprint_mapping(SEC, dict(GOOD, fingerprint_dates=9), ISSUER) is None


def test_reused_ticker_whose_cik_filed_only_after_delisting_is_refused():
    later = {"name": "NEW ABC INC", "first_filing": "2019-01-01", "last_filing": "2026-01-01"}
    assert HI.vendor_fingerprint_mapping(SEC, GOOD, later) is None


def test_no_sec_issuer_or_no_cik_is_no_evidence():
    assert HI.vendor_fingerprint_mapping(SEC, GOOD, None) is None
    assert HI.vendor_fingerprint_mapping(SEC, dict(GOOD, cik=None), ISSUER) is None


def test_store_records_append_only_and_supersedes(tmp_path):
    store = HI.IdentityStore(tmp_path / "id.sqlite")
    prior = HI.MappingResult("ngid:1", None, HI.METHOD_UNRESOLVED, HI.TIER_UNRESOLVED_BACKLOG,
                             0.0, HI.STATUS_UNRESOLVED, None, None, {"tier": 6})
    store.record_mapping(prior)
    new = HI.vendor_fingerprint_mapping(SEC, GOOD, ISSUER)
    out = store.record_mapping(new)
    assert out["inserted"] and out["superseded_prior"]
    again = store.record_mapping(new)
    assert not again["inserted"]                      # idempotent
    hist = store.mapping_history("ngid:1")
    assert len(hist) == 2
