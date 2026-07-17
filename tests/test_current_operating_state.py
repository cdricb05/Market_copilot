"""Phase 15-B — canonical current operating state (api/current_operating_state.py).

Pure unit tests with injected loader outputs (no DB / no live providers). They
prove the three state families are separated and never interchangeable, that the
CURRENT operating mark uses the latest daily mark (never the reconstructed
history), that the historical evidence dates stay distinct, that the legacy
reconciler cache is clearly labelled non-current, and that the aggregation is
strictly read-only.
"""
from __future__ import annotations

from paper_trader.api.current_operating_state import (
    CAT_CURRENT,
    CAT_HISTORICAL,
    CAT_LEGACY,
    LEGACY_CACHE_LABEL,
    load_current_operating_state,
)


# --------------------------------------------------------------------------- #
# Deterministic fixtures — mirror the live 2026-07-16 aligned state.
# --------------------------------------------------------------------------- #

def _valuation() -> dict:
    return {
        "current_mark": {
            "current_total_value": "10011.99",
            "current_positions_value": "2637.18",
            "current_cash": "7374.81",
            "current_total_return_pct": 0.1199,
            "current_unrealized_pnl": "13.99",
            "initial_capital": "10000.00",
            "as_of_market_date": "2026-07-16",
            "price_source": "yahoo_finance",
            "freshness_status": "FRESH",
            "valuation_complete": True,
            "covered_position_count": 2,
            "total_position_count": 2,
        },
        "latest_snapshot": {"market_date": "2026-07-16"},
        "reconciliation": {
            "vs_cached_total_value": {"cached_total_value": "9966.56", "delta": "45.43"},
        },
        "warnings": [],
    }


def _run_status() -> dict:
    return {
        "required_market_date": "2026-07-16",
        "status": "ALIGNED",
        "freshness_status": "FRESH",
        "alignment": {"aligned": True},
    }


def _daily_status() -> dict:
    return {
        "latest_valid_mark_available": True,
        "latest_valid_mark_date": "2026-07-16",
        "mark_freshness_status": "FRESH_MARK",
        "mark_age_calendar_days": 1,
        "top25": {
            "book_id": "composite_sn__2026-05-22__top25", "book_size": 25,
            "mark_date": "2026-07-16", "average_return_pct": 3.3593,
            "excess_return_vs_spy_pct_points": 2.4187, "benchmark_return_pct": 0.9406,
            "coverage_pct": 100.0, "previous_mark_date": "2026-07-15",
            "previous_average_return_pct": 2.6087,
        },
        "top50": {
            "book_id": "composite_sn__2026-05-22__top50", "book_size": 50,
            "mark_date": "2026-07-16", "average_return_pct": 4.6841,
            "excess_return_vs_spy_pct_points": 3.7435, "benchmark_return_pct": 0.9406,
            "coverage_pct": 100.0, "previous_mark_date": "2026-07-15",
            "previous_average_return_pct": 3.6946,
        },
        "spy_benchmark": {
            "reference_date": "2026-05-22", "reference_price": 743.7245,
            "latest_completed_eod_date": "2026-07-16", "return_since_signal_pct": 0.9406,
            "latest_adjusted_close": 750.72,
        },
    }


def _gate() -> dict:
    return {
        "decision": "PROVISIONAL_TOP50_PRIMARY",
        "decision_label": "Provisional Top50 Primary",
        "primary_paper_book": {"book": "TOP50"},
        "challenger_paper_book": {"label": "TOP25"},
        "top25": {"current_return_pct": 2.6087, "current_excess_return_pct_points": 1.1182,
                  "current_drawdown_pct": -3.1871, "max_drawdown_pct": -7.854,
                  "spy_cumulative_return_pct": 1.4905},
        "top50": {"current_return_pct": 3.6946, "current_excess_return_pct_points": 2.2041,
                  "current_drawdown_pct": -0.6485, "max_drawdown_pct": -4.9046,
                  "spy_cumulative_return_pct": 1.4905},
        "quarterly_rebalance_readiness": {"readiness_status": "MONITORING_MID_CYCLE"},
        "risk_review": {},
        "stability_comparison": {"more_stable": "TOP50"},
    }


def _performance() -> dict:
    return {
        "status": "PERFORMANCE_READY",
        "backfill_start_date": "2026-05-22",
        "latest_mark_date": "2026-07-15",
        "observation_count": 36,
        "benchmark": {"latest_return_since_signal_pct": 1.4905},
        "top25_analytics": {}, "top50_analytics": {},
        "reconciliation": {"status": "RECONCILED"},
    }


def _state() -> dict:
    return load_current_operating_state(
        valuation=_valuation(), run_status=_run_status(),
        daily_status=_daily_status(), gate=_gate(), performance=_performance(),
    )


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #

class TestCurrentOperatingStateStructure:
    def test_three_categories_present_and_tagged(self) -> None:
        s = _state()
        assert s["current_operating_mark"]["category"] == CAT_CURRENT
        assert s["historical_evidence_window"]["category"] == CAT_HISTORICAL
        assert s["legacy_archived_state"]["category"] == CAT_LEGACY
        assert s["categories_are_interchangeable"] is False

    def test_current_mark_uses_latest_daily_mark(self) -> None:
        c = _state()["current_operating_mark"]
        assert c["latest_completed_market_date"] == "2026-07-16"
        assert c["aligned"] is True
        # Top25 / Top50 / SPY are the CURRENT daily mark, not the 2.6087 / 3.6946 history.
        assert c["top25"]["return_pct"] == 3.3593
        assert c["top50"]["return_pct"] == 4.6841
        assert c["top50"]["excess_pct_points"] == 3.7435
        assert c["spy"]["return_since_signal_pct"] == 0.9406
        assert c["top25"]["mark_date"] == "2026-07-16"

    def test_portfolio_uses_canonical_current_not_legacy_cache(self) -> None:
        c = _state()["current_operating_mark"]["portfolio"]
        assert c["current_total_value"] == "10011.99"
        assert c["current_positions_value"] == "2637.18"
        # The legacy cached 9966.56 / 2591.75 must NOT be the current portfolio value.
        assert c["current_total_value"] != "9966.56"
        assert c["current_positions_value"] != "2591.75"


class TestHistoricalEvidenceSeparate:
    def test_history_dates_are_distinct_from_current(self) -> None:
        s = _state()
        h = s["historical_evidence_window"]
        assert h["reconstruction_start_date"] == "2026-05-22"
        assert h["reconstruction_end_date"] == "2026-07-15"
        assert h["observation_count"] == 36
        # The history end date differs from the current mark date.
        assert h["reconstruction_end_date"] != s["current_mark_date"]
        assert s["current_is_newer_than_history"] is True

    def test_history_keeps_reconstructed_spy_and_decision(self) -> None:
        h = _state()["historical_evidence_window"]
        # The reconstructed SPY cumulative (1.4905) is the history metric, kept apart
        # from the current since-signal SPY (0.9406).
        assert h["spy_cumulative_return_pct"] == 1.4905
        assert h["decision"] == "PROVISIONAL_TOP50_PRIMARY"


class TestLegacyArchivedLabelled:
    def test_legacy_cache_present_and_labelled_non_current(self) -> None:
        l = _state()["legacy_archived_state"]
        assert l["label"] == LEGACY_CACHE_LABEL
        assert "NOT CURRENT EOD VALUE" in l["label"]
        cache = l["cached_reconciler"]
        assert cache["cached_total_value"] == "9966.56"
        # Derived cached invested = cached total - current cash = 9966.56 - 7374.81.
        assert cache["cached_positions_value"] == "2591.75"
        assert cache["label"] == LEGACY_CACHE_LABEL


class TestReadOnlySafety:
    def test_read_only_and_no_writes(self) -> None:
        s = _state()
        prov = s["provenance"]
        assert prov["read_only"] is True
        assert prov["wrote_to_database"] is False
        assert prov["created_orders"] is False
        assert prov["created_signals"] is False
        assert prov["created_trade_decisions"] is False
        assert prov["called_prediction_service"] is False
        assert prov["made_loopback_http_calls"] is False
        assert prov["categories_are_interchangeable"] is False

    def test_safety_badges(self) -> None:
        s = _state()
        badges = s["safety"]["safety_badges"]
        for b in ("PAPER ONLY", "MANUAL REVIEW", "NO BROKER EXECUTION",
                  "AUTOMATION OFF", "NO LIVE ORDERS"):
            assert b in badges
        assert s["safety"]["promotes_to_live"] is False

    def test_degrades_without_raising_when_inputs_empty(self) -> None:
        # Empty injected inputs must not raise; the three categories still exist.
        s = load_current_operating_state(
            valuation={}, run_status={}, daily_status={}, gate={}, performance={})
        assert s["current_operating_mark"]["category"] == CAT_CURRENT
        assert s["historical_evidence_window"]["category"] == CAT_HISTORICAL
        assert s["legacy_archived_state"]["category"] == CAT_LEGACY
        assert s["current_is_newer_than_history"] is False
