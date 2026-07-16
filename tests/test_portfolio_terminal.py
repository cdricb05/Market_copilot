"""
tests/test_portfolio_terminal.py — Phase 14-B portfolio-terminal unit tests.

Fully offline and DB-free: exercises the PURE logic of
``api/portfolio_terminal.py`` — the per-position status recommendation (the same
HOLD / WATCH / REVIEW_FOR_EXIT / PRICE_UNAVAILABLE rules as /v1/portfolio/analytics),
the alert builder, the empty summary, and the read-only provenance. The DB-backed
endpoint wiring is covered separately in test_portfolio_terminal_endpoint.py.
"""
from __future__ import annotations

from paper_trader.api import portfolio_terminal as pt


# --------------------------------------------------------------------------- #
# Position status — mirrors the analytics monitor rules exactly
# --------------------------------------------------------------------------- #

def test_status_price_unavailable():
    status, reason = pt._position_status(None, None)
    assert status == pt.POS_PRICE_UNAVAILABLE
    assert "unavailable" in reason.lower()


def test_status_review_for_exit_on_deep_loss():
    status, reason = pt._position_status(-6.0, 10.0)
    assert status == pt.POS_REVIEW_FOR_EXIT
    assert "stop-loss" in reason.lower()


def test_status_watch_on_moderate_loss():
    status, _ = pt._position_status(-3.0, 10.0)
    assert status == pt.POS_WATCH


def test_status_watch_on_high_concentration():
    status, reason = pt._position_status(2.0, 30.0)
    assert status == pt.POS_WATCH
    assert "concentration" in reason.lower()


def test_status_hold_when_healthy():
    status, _ = pt._position_status(1.5, 10.0)
    assert status == pt.POS_HOLD


def test_status_loss_threshold_boundaries():
    # -5.0 exactly -> review-for-exit (<= -5.0)
    assert pt._position_status(-5.0, 5.0)[0] == pt.POS_REVIEW_FOR_EXIT
    # -2.0 exactly -> watch (<= -2.0)
    assert pt._position_status(-2.0, 5.0)[0] == pt.POS_WATCH


# --------------------------------------------------------------------------- #
# Alerts
# --------------------------------------------------------------------------- #

def test_alerts_flag_review_for_exit_and_missing_price():
    positions = {
        "rows": [
            {"ticker": "AAA", "status": pt.POS_REVIEW_FOR_EXIT, "reason": "deep loss"},
            {"ticker": "BBB", "status": pt.POS_PRICE_UNAVAILABLE, "reason": "no price"},
            {"ticker": "CCC", "status": pt.POS_HOLD, "reason": "ok"},
        ],
        "concentration_warning": False,
        "largest_position_ticker": None,
        "largest_position_weight_pct": None,
    }
    alerts = pt._build_alerts(positions, capacity_state=pt.cc.CAP_AVAILABLE)
    codes = {a["code"] for a in alerts}
    assert "REVIEW_FOR_EXIT" in codes
    assert "PRICE_UNAVAILABLE" in codes


def test_alerts_flag_concentration_and_capacity():
    positions = {
        "rows": [],
        "concentration_warning": True,
        "largest_position_ticker": "AAA",
        "largest_position_weight_pct": "40.00",
    }
    alerts = pt._build_alerts(positions, capacity_state=pt.cc.CAP_FULL)
    codes = {a["code"] for a in alerts}
    assert "HIGH_CONCENTRATION" in codes
    assert "MAX_POSITIONS_REACHED" in codes
    cap_alert = next(a for a in alerts if a["code"] == "MAX_POSITIONS_REACHED")
    assert "live" not in cap_alert["message"].lower()


# --------------------------------------------------------------------------- #
# Empty summary + provenance
# --------------------------------------------------------------------------- #

def test_empty_summary_is_not_seeded():
    s = pt._empty_summary(seeded=False, max_positions=5)
    assert s["seeded"] is False
    assert s["total_value"] is None
    assert s["max_positions"] == 5


def test_provenance_is_read_only():
    p = pt._provenance()
    assert p["read_only"] is True
    for k in ("wrote_to_database", "created_orders", "created_signals",
              "created_trade_decisions", "invoked_daily_refresh",
              "called_prediction_service", "called_external_provider",
              "made_loopback_http_calls"):
        assert p[k] is False
