"""
tests/test_daily_operating_run.py — Phase 15-A canonical daily-run unit tests.

Fully offline and DB-free: exercises the PURE logic of
``api/daily_operating_run.py`` — the latest-completed-market-date resolver, the
market-date alignment contract (aligned / stale / partial-coverage / blocked /
no-completed-date / unsupported-excluded), the ticker de-dup, the deterministic
price stub, the final-outcome copy, the read-only safety/provenance surface and
the status/stage enums. The DB-backed write path (idempotency, before/after
counts, cross-endpoint alignment) is covered in
test_daily_operating_run_endpoint.py.
"""
from __future__ import annotations

from datetime import datetime, timezone

from paper_trader.api import daily_operating_run as dor


# --------------------------------------------------------------------------- #
# latest_completed_market_date
# --------------------------------------------------------------------------- #

def test_weekday_after_close_is_today():
    # 21:00 UTC = 17:00 EDT on Wed 2026-07-15 (after the 16:00 close).
    now = datetime(2026, 7, 15, 21, 0, tzinfo=timezone.utc)
    assert dor.latest_completed_market_date(now).isoformat() == "2026-07-15"


def test_weekday_before_close_is_prior_weekday():
    # 18:00 UTC = 14:00 EDT on Wed 2026-07-15 (before the close) -> Tue 07-14.
    now = datetime(2026, 7, 15, 18, 0, tzinfo=timezone.utc)
    assert dor.latest_completed_market_date(now).isoformat() == "2026-07-14"


def test_saturday_walks_back_to_friday():
    now = datetime(2026, 7, 18, 12, 0, tzinfo=timezone.utc)  # Sat
    assert dor.latest_completed_market_date(now).isoformat() == "2026-07-17"


def test_sunday_walks_back_to_friday():
    now = datetime(2026, 7, 19, 23, 0, tzinfo=timezone.utc)  # Sun
    assert dor.latest_completed_market_date(now).isoformat() == "2026-07-17"


def test_monday_before_close_is_prior_friday():
    # 13:00 UTC = 09:00 EDT Mon 2026-07-20 (before close) -> Fri 07-17.
    now = datetime(2026, 7, 20, 13, 0, tzinfo=timezone.utc)
    assert dor.latest_completed_market_date(now).isoformat() == "2026-07-17"


# --------------------------------------------------------------------------- #
# compute_market_date_alignment
# --------------------------------------------------------------------------- #

def _all(required):
    return dict(
        required_market_date=required,
        price_snapshot_market_date=required,
        portfolio_snapshot_market_date=required,
        alpha_top25_market_date=required,
        alpha_top50_market_date=required,
        spy_market_date=required,
        command_center_market_date=required,
        portfolio_terminal_market_date=required,
    )


def test_alignment_all_equal_is_aligned():
    a = dor.compute_market_date_alignment(**_all("2026-07-16"))
    assert a["status"] == dor.ST_ALIGNED
    assert a["aligned"] is True
    assert a["mismatches"] == []
    assert a["blocking_mismatches"] == []


def test_alignment_mixed_dates_is_stale_with_mismatches():
    a = dor.compute_market_date_alignment(
        required_market_date="2026-07-16",
        price_snapshot_market_date="2026-06-15",
        portfolio_snapshot_market_date="2026-06-15",
        alpha_top25_market_date="2026-07-15",
        alpha_top50_market_date="2026-07-15",
        spy_market_date="2026-07-15",
    )
    assert a["status"] == dor.ST_STALE
    assert a["aligned"] is False
    datasets = {m["dataset"] for m in a["mismatches"]}
    assert "price_snapshot_market_date" in datasets
    assert "alpha_top25_market_date" in datasets


def test_alignment_no_required_date():
    a = dor.compute_market_date_alignment(required_market_date=None,
                                          price_snapshot_market_date="2026-07-16")
    assert a["status"] == dor.ST_NO_COMPLETED_MARKET_DATE
    assert a["aligned"] is False


def test_alignment_partial_coverage_blocks():
    a = dor.compute_market_date_alignment(
        **_all("2026-07-16"), coverage_complete=False)
    assert a["status"] == dor.ST_PARTIAL_COVERAGE
    assert any(b["dataset"] == "price_snapshot_market_date"
               for b in a["blocking_mismatches"])


def test_alignment_alpha_blocked():
    a = dor.compute_market_date_alignment(**_all("2026-07-16"), alpha_blocked=True)
    assert a["status"] == dor.ST_BLOCKED
    assert any(b["dataset"] == "alpha_mark" for b in a["blocking_mismatches"])


def test_alignment_unsupported_datasets_excluded():
    # Only the portfolio present and equal to required; alpha/spy empty (None).
    a = dor.compute_market_date_alignment(
        required_market_date="2026-07-16",
        price_snapshot_market_date="2026-07-16",
        portfolio_snapshot_market_date="2026-07-16",
        command_center_market_date="2026-07-16",
        portfolio_terminal_market_date="2026-07-16",
    )
    assert a["status"] == dor.ST_ALIGNED
    assert a["aligned"] is True
    assert a["alpha_top25_market_date"] is None


# --------------------------------------------------------------------------- #
# Small pure helpers
# --------------------------------------------------------------------------- #

def test_dedupe_preserves_first_seen_order():
    assert dor._dedupe(["AAA", "BBB", "AAA", "SPY", "BBB"]) == ["AAA", "BBB", "SPY"]


def test_stub_fetch_is_deterministic():
    ok, fail = dor._stub_fetch(["aaa", "bbb"])
    assert fail == []
    assert {r["ticker"] for r in ok} == {"AAA", "BBB"}
    assert all(r["price"] == dor._STUB_PRICE for r in ok)


def test_to_price_rejects_nonpositive():
    assert dor._to_price("110.5") is not None
    assert dor._to_price("0") is None
    assert dor._to_price("-3") is None
    assert dor._to_price("abc") is None


def test_final_outcome_aligned_and_partial():
    aligned = dor._final_outcome(
        status=dor.ST_ALIGNED,
        alignment={"required_market_date": "2026-07-16"},
        prices={}, snapshot={}, alpha={})
    assert "ALL OPERATING DATA ALIGNED TO 2026-07-16" in aligned
    partial = dor._final_outcome(
        status=dor.ST_PARTIAL_COVERAGE,
        alignment={"required_market_date": "2026-07-16"},
        prices={"successful_ticker_count": 48, "requested_ticker_count": 50},
        snapshot={"stage_status": dor.STG_CREATED},
        alpha={"stage_status": dor.STG_BLOCKED})
    assert "DAILY RUN PARTIAL" in partial
    assert "48/50 PRICES LOADED" in partial


# --------------------------------------------------------------------------- #
# Safety / provenance / constants
# --------------------------------------------------------------------------- #

def test_execute_confirmation_token():
    assert dor.EXECUTE_CONFIRMATION == "RUN_MANUAL_DAILY_OPERATING_SESSION"


def test_status_constants_distinct():
    vals = {dor.ST_ALIGNED, dor.ST_PREVIEW_WOULD_ALIGN, dor.ST_STALE,
            dor.ST_PARTIAL_COVERAGE, dor.ST_BLOCKED, dor.ST_NO_COMPLETED_MARKET_DATE,
            dor.ST_ALREADY_COMPLETE}
    assert len(vals) == 7


def test_safety_block_paper_only_no_automation():
    s = dor._safety()
    assert s["creates_orders"] is False
    assert s["no_broker_execution"] is True
    assert s["automation_off"] is True
    assert s["is_scheduled"] is False
    assert s["prediction_checked"] is False
    assert "NO ORDERS" in s["safety_badges"]


def test_provenance_read_only_vs_write():
    ro = dor._provenance(wrote=False)
    assert ro["read_only"] is True and ro["wrote_to_database"] is False
    wr = dor._provenance(wrote=True)
    assert wr["read_only"] is False and wr["wrote_to_database"] is True
    for prov in (ro, wr):
        assert prov["created_orders"] is False
        assert prov["created_signals"] is False
        assert prov["created_trade_decisions"] is False
        assert prov["modified_positions"] is False
        assert prov["modified_cash"] is False
        assert prov["called_prediction_service"] is False
        assert prov["made_loopback_http_calls"] is False
