"""
tests/test_portfolio_valuation.py — Phase 14-C canonical valuation unit tests.

Fully offline and DB-free: exercises the PURE logic of
``api/portfolio_valuation.py`` — the shared position-status rules, freshness
classification, price-source reconciliation, the second-pass weight/finalise
step, the empty current-mark shape, and the read-only provenance. The DB-backed
endpoint contract (current mark, snapshot separation, reconciliation invariant,
missing-price coverage, cross-endpoint consistency) is covered separately in
test_portfolio_valuation_endpoint.py.
"""
from __future__ import annotations

from datetime import date

from paper_trader.api import portfolio_valuation as pv


# --------------------------------------------------------------------------- #
# Position status — mirrors the analytics monitor rules exactly
# --------------------------------------------------------------------------- #

def test_status_price_unavailable():
    status, reason = pv._position_status(None, None)
    assert status == pv.POS_PRICE_UNAVAILABLE
    assert "unavailable" in reason.lower()


def test_status_review_for_exit_on_deep_loss():
    status, reason = pv._position_status(-6.0, 10.0)
    assert status == pv.POS_REVIEW_FOR_EXIT
    assert "stop-loss" in reason.lower()


def test_status_watch_moderate_loss_and_concentration():
    assert pv._position_status(-3.0, 10.0)[0] == pv.POS_WATCH
    assert pv._position_status(2.0, 30.0)[0] == pv.POS_WATCH


def test_status_hold_when_healthy():
    assert pv._position_status(1.5, 10.0)[0] == pv.POS_HOLD


def test_status_loss_threshold_boundaries():
    assert pv._position_status(-5.0, 5.0)[0] == pv.POS_REVIEW_FOR_EXIT
    assert pv._position_status(-2.0, 5.0)[0] == pv.POS_WATCH


# --------------------------------------------------------------------------- #
# Freshness
# --------------------------------------------------------------------------- #

def test_freshness_none_is_no_price():
    status, age = pv._freshness(None, date(2026, 7, 16))
    assert status == pv.NO_PRICE and age is None


def test_freshness_fresh_within_window():
    status, age = pv._freshness(date(2026, 7, 15), date(2026, 7, 16))
    assert status == pv.FRESH and age == 1


def test_freshness_stale_beyond_window():
    status, age = pv._freshness(date(2026, 6, 15), date(2026, 7, 16))
    assert status == pv.STALE and age == 31


# --------------------------------------------------------------------------- #
# Price source reconciliation
# --------------------------------------------------------------------------- #

def test_price_source_single():
    assert pv._price_source(["yahoo_finance", "yahoo_finance"]) == "yahoo_finance"


def test_price_source_mixed():
    assert pv._price_source(["yahoo_finance", "eodhd"]) == "MIXED"


def test_price_source_none_becomes_unknown():
    assert pv._price_source([None]) == "unknown"


def test_price_source_empty():
    assert pv._price_source([]) is None


# --------------------------------------------------------------------------- #
# Finalise positions (weight against canonical total + classify)
# --------------------------------------------------------------------------- #

def test_finalise_positions_weights_and_status():
    from decimal import Decimal
    rows = [
        {"ticker": "AAA", "market_value": "5000.00", "weight_pct": None,
         "status": pv.POS_PRICE_UNAVAILABLE, "reason": "", "_upnl_pct_f": 1.0},
        {"ticker": "BBB", "market_value": "1000.00", "weight_pct": None,
         "status": pv.POS_PRICE_UNAVAILABLE, "reason": "", "_upnl_pct_f": -6.0},
    ]
    out = pv._finalise_positions(rows, total_value=Decimal("10000"))
    by = {r["ticker"]: r for r in out}
    # AAA: healthy P&L but 50% weight -> WATCH (concentration)
    assert by["AAA"]["weight_pct"] == "50.00"
    assert by["AAA"]["status"] == pv.POS_WATCH
    assert "concentration" in by["AAA"]["reason"].lower()
    # BBB: -6% -> REVIEW_FOR_EXIT
    assert by["BBB"]["status"] == pv.POS_REVIEW_FOR_EXIT
    # sorted by weight desc
    assert [r["ticker"] for r in out] == ["AAA", "BBB"]
    # the private sort key is consumed
    assert "_upnl_pct_f" not in out[0]


# --------------------------------------------------------------------------- #
# Shapes + provenance
# --------------------------------------------------------------------------- #

def test_empty_current_mark_is_incomplete():
    cm = pv._empty_current_mark()
    assert cm["valuation_type"] == pv.VALUATION_CURRENT
    assert cm["valuation_complete"] is False
    assert cm["current_total_value"] is None


def test_provenance_is_read_only():
    p = pv._provenance()
    assert p["read_only"] is True
    for k in ("wrote_to_database", "created_orders", "created_signals",
              "created_trade_decisions", "invoked_daily_refresh",
              "called_prediction_service", "called_external_provider",
              "made_loopback_http_calls"):
        assert p[k] is False


def test_valuation_type_constants_distinct():
    assert pv.VALUATION_CURRENT == "CURRENT_MARKED_EOD"
    assert pv.VALUATION_SNAPSHOT == "OFFICIAL_PORTFOLIO_SNAPSHOT"
    assert pv.VALUATION_CURRENT != pv.VALUATION_SNAPSHOT
