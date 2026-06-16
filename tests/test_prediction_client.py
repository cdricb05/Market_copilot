"""
tests/test_prediction_client.py — Unit tests for prediction_client module.

Tests focus on:
    - Normalization of external API responses to internal contract.
    - Per-ticker fetch error handling.
    - Field mapping and transformation (confidence scaling, recommendation uppercase, etc.).
    - Model consensus derivation from per_model_summary.
    - Market context mapping based on recommendation.
    - Concurrent fetch: result ordering, failure tolerance, concurrency limit.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from paper_trader.engine.prediction_client import (
    build_prediction_run_values,
    fetch_predictions_for_tickers,
    normalize_prediction_response,
)


class TestNormalizePredictionResponse:
    """normalize_prediction_response() transforms GCP responses to Paper Trader contract."""

    def test_normalize_valid_buy_response(self):
        """Valid BUY prediction normalizes correctly with all fields."""
        raw = {
            "ticker": "AAPL",
            "current_price": "150.50",
            "ensemble_day5": "155.75",
            "d5_change_pct": "3.48",
            "confidence": "87.5",
            "recommendation": "Buy",
            "rationale": [
                "Models show",
                "strong agreement",
            ],
            "per_model_summary": {
                "prophet": {"direction": "Up"},
                "arima": {"direction": "Up"},
                "xgboost": {"direction": "Flat"},
                "lstm": {"direction": "Up"},
            },
        }

        normalized = normalize_prediction_response(raw)

        assert normalized is not None
        assert normalized["ticker"] == "AAPL"
        assert normalized["current_price"] == "150.50"
        assert normalized["forecast_price_5d"] == "155.75"
        assert normalized["expected_return_pct"] == "3.48"
        assert normalized["confidence"] == "0.875"
        assert normalized["recommendation"] == "BUY"
        assert normalized["reason"] == "Models show strong agreement"
        assert normalized["market_context"] == "bullish"

    def test_normalize_confidence_scaling(self):
        """Confidence is scaled from 0-100 to 0-1."""
        raw = {
            "ticker": "MSFT",
            "current_price": "400",
            "ensemble_day5": "420",
            "d5_change_pct": "5",
            "confidence": "97.7",  # 97.7% → 0.977
            "recommendation": "BUY",
            "per_model_summary": {},
        }

        normalized = normalize_prediction_response(raw)

        assert normalized is not None
        assert Decimal(normalized["confidence"]) == Decimal("0.977")

    def test_normalize_confidence_zero(self):
        """Confidence of 0 is valid."""
        raw = {
            "ticker": "TSLA",
            "current_price": "250",
            "ensemble_day5": "250",
            "d5_change_pct": "0",
            "confidence": "0",
            "recommendation": "HOLD",
            "per_model_summary": {},
        }

        normalized = normalize_prediction_response(raw)

        assert normalized is not None
        assert normalized["confidence"] == "0"

    def test_normalize_confidence_100(self):
        """Confidence of 100 is valid."""
        raw = {
            "ticker": "SPY",
            "current_price": "450",
            "ensemble_day5": "460",
            "d5_change_pct": "2.22",
            "confidence": "100",
            "recommendation": "BUY",
            "per_model_summary": {},
        }

        normalized = normalize_prediction_response(raw)

        assert normalized is not None
        assert normalized["confidence"] == "1"

    def test_normalize_confidence_out_of_range(self):
        """Confidence >100 or <0 returns None."""
        raw_over = {
            "ticker": "XYZ",
            "current_price": "100",
            "ensemble_day5": "110",
            "d5_change_pct": "10",
            "confidence": "150",
            "recommendation": "BUY",
            "per_model_summary": {},
        }

        assert normalize_prediction_response(raw_over) is None

    def test_normalize_recommendation_uppercase(self):
        """Recommendation is uppercased."""
        for rec_in, rec_out in [("buy", "BUY"), ("Buy", "BUY"), ("SELL", "SELL"), ("hold", "HOLD")]:
            raw = {
                "ticker": "AAPL",
                "current_price": "100",
                "ensemble_day5": "105",
                "d5_change_pct": "5",
                "confidence": "75",
                "recommendation": rec_in,
                "per_model_summary": {},
            }

            normalized = normalize_prediction_response(raw)
            assert normalized is not None
            assert normalized["recommendation"] == rec_out

    def test_normalize_invalid_recommendation(self):
        """Invalid recommendation returns None."""
        raw = {
            "ticker": "AAPL",
            "current_price": "100",
            "ensemble_day5": "105",
            "d5_change_pct": "5",
            "confidence": "75",
            "recommendation": "INVALID",
            "per_model_summary": {},
        }

        assert normalize_prediction_response(raw) is None

    def test_normalize_market_context_mapping(self):
        """Market context is derived from recommendation."""
        test_cases = [
            ("BUY", "bullish"),
            ("SELL", "bearish"),
            ("HOLD", "neutral"),
        ]

        for recommendation, expected_context in test_cases:
            raw = {
                "ticker": "TEST",
                "current_price": "100",
                "ensemble_day5": "105",
                "d5_change_pct": "5",
                "confidence": "75",
                "recommendation": recommendation,
                "per_model_summary": {},
            }

            normalized = normalize_prediction_response(raw)
            assert normalized is not None
            assert normalized["market_context"] == expected_context

    def test_normalize_model_consensus_derivation(self):
        """Model consensus votes are derived from per_model_summary directions."""
        raw = {
            "ticker": "AAPL",
            "current_price": "100",
            "ensemble_day5": "105",
            "d5_change_pct": "5",
            "confidence": "75",
            "recommendation": "BUY",
            "per_model_summary": {
                "prophet": {"direction": "Up"},
                "arima": {"direction": "Down"},
                "xgboost": {"direction": "Up"},
                "lstm": {"direction": "Flat"},
            },
        }

        normalized = normalize_prediction_response(raw)

        assert normalized is not None
        assert normalized["model_consensus"] == {
            "prophet": "BUY",
            "arima": "SELL",
            "xgboost": "BUY",
            "lstm": "HOLD",
        }

    def test_normalize_model_consensus_list_shape(self):
        """Model consensus works with real GCP list shape."""
        raw = {
            "ticker": "AAPL",
            "current_price": "150.50",
            "ensemble_day5": "155.75",
            "d5_change_pct": "3.48",
            "confidence": "87.5",
            "recommendation": "BUY",
            "per_model_summary": [
                {"model": "Drift", "direction": "Up"},
                {"model": "LinearTrend", "direction": "Down"},
                {"model": "XGBoost", "direction": "Down"},
                {"model": "Naive", "direction": "Flat"},
                {"model": "SMA", "direction": "Up"},
            ],
        }

        normalized = normalize_prediction_response(raw)

        assert normalized is not None
        assert normalized["model_consensus"] == {
            "Drift": "BUY",
            "LinearTrend": "SELL",
            "XGBoost": "SELL",
            "Naive": "HOLD",
            "SMA": "BUY",
        }

    def test_normalize_rationale_list_join(self):
        """Rationale list is joined into a reason string."""
        raw = {
            "ticker": "AAPL",
            "current_price": "100",
            "ensemble_day5": "105",
            "d5_change_pct": "5",
            "confidence": "75",
            "recommendation": "BUY",
            "rationale": [
                "Strong uptrend",
                "High confidence",
                "Models agree",
            ],
            "per_model_summary": {},
        }

        normalized = normalize_prediction_response(raw)

        assert normalized is not None
        assert normalized["reason"] == "Strong uptrend High confidence Models agree"

    def test_normalize_rationale_empty_list(self):
        """Empty rationale list results in empty reason string."""
        raw = {
            "ticker": "AAPL",
            "current_price": "100",
            "ensemble_day5": "105",
            "d5_change_pct": "5",
            "confidence": "75",
            "recommendation": "BUY",
            "rationale": [],
            "per_model_summary": {},
        }

        normalized = normalize_prediction_response(raw)

        assert normalized is not None
        assert normalized["reason"] == ""

    def test_normalize_rationale_string(self):
        """Rationale as string (not list) is preserved as-is."""
        raw = {
            "ticker": "AAPL",
            "current_price": "100",
            "ensemble_day5": "105",
            "d5_change_pct": "5",
            "confidence": "75",
            "recommendation": "BUY",
            "rationale": "Simple string reason",
            "per_model_summary": {},
        }

        normalized = normalize_prediction_response(raw)

        assert normalized is not None
        assert normalized["reason"] == "Simple string reason"

    def test_normalize_missing_ticker(self):
        """Missing ticker returns None."""
        raw = {
            "current_price": "100",
            "ensemble_day5": "105",
            "d5_change_pct": "5",
            "confidence": "75",
            "recommendation": "BUY",
        }

        assert normalize_prediction_response(raw) is None

    def test_normalize_missing_confidence(self):
        """Missing confidence returns None."""
        raw = {
            "ticker": "AAPL",
            "current_price": "100",
            "ensemble_day5": "105",
            "d5_change_pct": "5",
            "recommendation": "BUY",
        }

        assert normalize_prediction_response(raw) is None

    def test_normalize_missing_recommendation(self):
        """Missing recommendation returns None."""
        raw = {
            "ticker": "AAPL",
            "current_price": "100",
            "ensemble_day5": "105",
            "d5_change_pct": "5",
            "confidence": "75",
        }

        assert normalize_prediction_response(raw) is None

    def test_normalize_invalid_confidence_non_numeric(self):
        """Non-numeric confidence returns None."""
        raw = {
            "ticker": "AAPL",
            "current_price": "100",
            "ensemble_day5": "105",
            "d5_change_pct": "5",
            "confidence": "not_a_number",
            "recommendation": "BUY",
        }

        assert normalize_prediction_response(raw) is None

    def test_normalize_invalid_price_non_numeric(self):
        """Non-numeric price returns None."""
        raw = {
            "ticker": "AAPL",
            "current_price": "not_a_number",
            "ensemble_day5": "105",
            "d5_change_pct": "5",
            "confidence": "75",
            "recommendation": "BUY",
        }

        assert normalize_prediction_response(raw) is None

    def test_normalize_none_input(self):
        """None input returns None."""
        assert normalize_prediction_response(None) is None

    def test_normalize_non_dict_input(self):
        """Non-dict input returns None."""
        assert normalize_prediction_response("not a dict") is None
        assert normalize_prediction_response([1, 2, 3]) is None

    def test_normalize_empty_dict(self):
        """Empty dict returns None."""
        assert normalize_prediction_response({}) is None

    def test_normalize_minimal_valid_response(self):
        """Minimal valid response with only required fields normalizes correctly."""
        raw = {
            "ticker": "AAPL",
            "current_price": "",
            "ensemble_day5": "",
            "d5_change_pct": "",
            "confidence": "50",
            "recommendation": "HOLD",
        }

        normalized = normalize_prediction_response(raw)

        assert normalized is not None
        assert normalized["ticker"] == "AAPL"
        assert normalized["current_price"] == ""
        assert normalized["forecast_price_5d"] == ""
        assert normalized["expected_return_pct"] == ""
        assert normalized["confidence"] == "0.5"
        assert normalized["recommendation"] == "HOLD"
        assert normalized["reason"] == ""
        assert normalized["market_context"] == "neutral"

    def test_normalize_hold_with_missing_confidence_defaults_to_050(self):
        """HOLD recommendation with missing confidence defaults to 0.50."""
        raw = {
            "ticker": "MSFT",
            "current_price": "416.03",
            "ensemble_day5": "415.62",
            "d5_change_pct": "-0.1",
            "confidence": None,
            "recommendation": "HOLD",
            "per_model_summary": [],
        }

        normalized = normalize_prediction_response(raw)

        assert normalized is not None
        assert normalized["recommendation"] == "HOLD"
        assert normalized["confidence"] == "0.50"
        assert normalized["market_context"] == "neutral"

    def test_normalize_hold_with_null_confidence_string(self):
        """HOLD with empty string confidence defaults to 0.50."""
        raw = {
            "ticker": "TSLA",
            "current_price": "433.59",
            "ensemble_day5": "441.37",
            "d5_change_pct": "1.79",
            "confidence": "",
            "recommendation": "Hold",
            "per_model_summary": [
                {"model": "ModelA", "direction": "Flat"},
            ],
        }

        normalized = normalize_prediction_response(raw)

        assert normalized is not None
        assert normalized["recommendation"] == "HOLD"
        assert normalized["confidence"] == "0.50"

    def test_normalize_buy_with_missing_confidence_fails(self):
        """BUY recommendation with missing confidence fails normalization."""
        raw = {
            "ticker": "GOOG",
            "current_price": "140.00",
            "ensemble_day5": "145.00",
            "d5_change_pct": "3.57",
            "confidence": None,
            "recommendation": "BUY",
            "per_model_summary": {},
        }

        normalized = normalize_prediction_response(raw)
        assert normalized is None

    def test_normalize_sell_with_missing_confidence_fails(self):
        """SELL recommendation with missing confidence fails normalization."""
        raw = {
            "ticker": "NVDA",
            "current_price": "950.00",
            "ensemble_day5": "920.00",
            "d5_change_pct": "-3.16",
            "confidence": None,
            "recommendation": "SELL",
            "per_model_summary": {},
        }

        normalized = normalize_prediction_response(raw)
        assert normalized is None

    def test_normalize_real_msft_hold_shape(self):
        """Real MSFT Hold response with missing confidence normalizes correctly."""
        raw = {
            "ticker": "MSFT",
            "current_price": "416.03",
            "ensemble_day5": "415.62",
            "d5_change_pct": "-0.1",
            "confidence": None,
            "recommendation": "Hold",
            "per_model_summary": [
                {"model": "Drift", "direction": "Flat"},
                {"model": "LinearTrend", "direction": "Down"},
                {"model": "XGBoost", "direction": "Up"},
                {"model": "Naive", "direction": "Flat"},
                {"model": "SMA", "direction": "Down"},
            ],
        }

        normalized = normalize_prediction_response(raw)

        assert normalized is not None
        assert normalized["ticker"] == "MSFT"
        assert normalized["recommendation"] == "HOLD"
        assert normalized["confidence"] == "0.50"
        assert normalized["current_price"] == "416.03"
        assert normalized["forecast_price_5d"] == "415.62"
        assert normalized["expected_return_pct"] == "-0.1"
        assert normalized["market_context"] == "neutral"
        assert normalized["model_consensus"] == {
            "Drift": "HOLD",
            "LinearTrend": "SELL",
            "XGBoost": "BUY",
            "Naive": "HOLD",
            "SMA": "SELL",
        }

    def test_normalize_real_tsla_hold_shape(self):
        """Real TSLA Hold response with missing confidence normalizes correctly."""
        raw = {
            "ticker": "TSLA",
            "current_price": "433.59",
            "ensemble_day5": "441.37",
            "d5_change_pct": "1.79",
            "confidence": None,
            "recommendation": "Hold",
            "per_model_summary": [
                {"model": "Drift", "direction": "Up"},
                {"model": "LinearTrend", "direction": "Up"},
                {"model": "XGBoost", "direction": "Flat"},
                {"model": "Naive", "direction": "Up"},
                {"model": "SMA", "direction": "Up"},
            ],
        }

        normalized = normalize_prediction_response(raw)

        assert normalized is not None
        assert normalized["ticker"] == "TSLA"
        assert normalized["recommendation"] == "HOLD"
        assert normalized["confidence"] == "0.50"
        assert normalized["current_price"] == "433.59"
        assert normalized["forecast_price_5d"] == "441.37"
        assert normalized["expected_return_pct"] == "1.79"
        assert normalized["market_context"] == "neutral"
        assert normalized["model_consensus"] == {
            "Drift": "BUY",
            "LinearTrend": "BUY",
            "XGBoost": "HOLD",
            "Naive": "BUY",
            "SMA": "BUY",
        }

    def test_normalize_strong_buy_uppercase(self):
        """STRONG BUY normalizes to BUY."""
        raw = {
            "ticker": "QCOM",
            "current_price": "150",
            "ensemble_day5": "165",
            "d5_change_pct": "10",
            "confidence": "95",
            "recommendation": "STRONG BUY",
            "per_model_summary": {},
        }

        normalized = normalize_prediction_response(raw)

        assert normalized is not None
        assert normalized["recommendation"] == "BUY"
        assert normalized["market_context"] == "bullish"

    def test_normalize_strong_buy_underscore(self):
        """STRONG_BUY normalizes to BUY."""
        raw = {
            "ticker": "AMD",
            "current_price": "160",
            "ensemble_day5": "175",
            "d5_change_pct": "9.375",
            "confidence": "92",
            "recommendation": "STRONG_BUY",
            "per_model_summary": {},
        }

        normalized = normalize_prediction_response(raw)

        assert normalized is not None
        assert normalized["recommendation"] == "BUY"

    def test_normalize_strong_buy_mixed_case(self):
        """Strong Buy (mixed case) normalizes to BUY."""
        raw = {
            "ticker": "NVDA",
            "current_price": "875",
            "ensemble_day5": "950",
            "d5_change_pct": "8.57",
            "confidence": "88",
            "recommendation": "Strong Buy",
            "per_model_summary": {},
        }

        normalized = normalize_prediction_response(raw)

        assert normalized is not None
        assert normalized["recommendation"] == "BUY"

    def test_normalize_strong_buy_lowercase(self):
        """strong buy (lowercase) normalizes to BUY."""
        raw = {
            "ticker": "CSCO",
            "current_price": "50",
            "ensemble_day5": "55",
            "d5_change_pct": "10",
            "confidence": "85",
            "recommendation": "strong buy",
            "per_model_summary": {},
        }

        normalized = normalize_prediction_response(raw)

        assert normalized is not None
        assert normalized["recommendation"] == "BUY"

    def test_normalize_strong_sell_uppercase(self):
        """STRONG SELL normalizes to SELL."""
        raw = {
            "ticker": "TXN",
            "current_price": "165",
            "ensemble_day5": "145",
            "d5_change_pct": "-12.12",
            "confidence": "91",
            "recommendation": "STRONG SELL",
            "per_model_summary": {},
        }

        normalized = normalize_prediction_response(raw)

        assert normalized is not None
        assert normalized["recommendation"] == "SELL"
        assert normalized["market_context"] == "bearish"

    def test_normalize_strong_sell_underscore(self):
        """STRONG_SELL normalizes to SELL."""
        raw = {
            "ticker": "AMAT",
            "current_price": "140",
            "ensemble_day5": "120",
            "d5_change_pct": "-14.29",
            "confidence": "89",
            "recommendation": "STRONG_SELL",
            "per_model_summary": {},
        }

        normalized = normalize_prediction_response(raw)

        assert normalized is not None
        assert normalized["recommendation"] == "SELL"

    def test_normalize_strong_sell_mixed_case(self):
        """Strong Sell (mixed case) normalizes to SELL."""
        raw = {
            "ticker": "MU",
            "current_price": "85",
            "ensemble_day5": "75",
            "d5_change_pct": "-11.76",
            "confidence": "87",
            "recommendation": "Strong Sell",
            "per_model_summary": {},
        }

        normalized = normalize_prediction_response(raw)

        assert normalized is not None
        assert normalized["recommendation"] == "SELL"

    def test_normalize_strong_sell_lowercase(self):
        """strong sell (lowercase) normalizes to SELL."""
        raw = {
            "ticker": "ASML",
            "current_price": "600",
            "ensemble_day5": "525",
            "d5_change_pct": "-12.5",
            "confidence": "90",
            "recommendation": "strong sell",
            "per_model_summary": {},
        }

        normalized = normalize_prediction_response(raw)

        assert normalized is not None
        assert normalized["recommendation"] == "SELL"

    def test_normalize_strong_recommendation_preserves_other_fields(self):
        """STRONG BUY normalization preserves all other fields correctly."""
        raw = {
            "ticker": "INTC",
            "current_price": "45.50",
            "ensemble_day5": "52.75",
            "d5_change_pct": "15.93",
            "confidence": "93.5",
            "recommendation": "STRONG BUY",
            "rationale": ["Strong signals", "High conviction"],
            "per_model_summary": {
                "prophet": {"direction": "Up"},
                "arima": {"direction": "Up"},
            },
        }

        normalized = normalize_prediction_response(raw)

        assert normalized is not None
        assert normalized["ticker"] == "INTC"
        assert normalized["current_price"] == "45.50"
        assert normalized["forecast_price_5d"] == "52.75"
        assert normalized["expected_return_pct"] == "15.93"
        assert normalized["confidence"] == "0.935"
        assert normalized["recommendation"] == "BUY"
        assert normalized["reason"] == "Strong signals High conviction"
        assert normalized["market_context"] == "bullish"
        assert normalized["model_consensus"] == {
            "prophet": "BUY",
            "arima": "BUY",
        }


# ---------------------------------------------------------------------------
# Concurrent fetch tests
# ---------------------------------------------------------------------------

def _make_mock_response(ticker: str) -> MagicMock:
    """Build a mock httpx response for a given ticker."""
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json = MagicMock(return_value={
        "ticker": ticker,
        "current_price": "100",
        "ensemble_day5": "105",
        "d5_change_pct": "5",
        "confidence": "80",
        "recommendation": "BUY",
        "per_model_summary": {},
    })
    return resp


class TestConcurrentFetch:
    """fetch_predictions_for_tickers() concurrent behaviour."""

    def test_concurrent_fetch_preserves_one_result_per_ticker(self):
        """Each requested ticker appears exactly once in successful results."""
        tickers = ["AAPL", "MSFT", "NVDA"]

        async def _mock_post(url, json=None, **kwargs):
            return _make_mock_response(json["ticker"])

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=_mock_post)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("paper_trader.engine.prediction_client.httpx") as mock_httpx:
            mock_httpx.AsyncClient.return_value = mock_client
            mock_httpx.TimeoutException = Exception
            mock_httpx.HTTPStatusError = Exception
            mock_httpx.RequestError = Exception

            successful, failures = asyncio.run(
                fetch_predictions_for_tickers(tickers, "http://localhost:9000")
            )

        assert failures == []
        assert len(successful) == 3
        returned_tickers = [r["ticker"] for r in successful]
        assert set(returned_tickers) == {"AAPL", "MSFT", "NVDA"}

    def test_concurrent_fetch_tolerates_one_failed_ticker(self):
        """A single ticker failure does not prevent results for other tickers."""
        tickers = ["AAPL", "FAIL", "MSFT"]

        import httpx as real_httpx

        async def _mock_post(url, json=None, **kwargs):
            if json["ticker"] == "FAIL":
                err = real_httpx.TimeoutException("timeout")
                raise err
            return _make_mock_response(json["ticker"])

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=_mock_post)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("paper_trader.engine.prediction_client.httpx") as mock_httpx:
            mock_httpx.AsyncClient.return_value = mock_client
            mock_httpx.TimeoutException = real_httpx.TimeoutException
            mock_httpx.HTTPStatusError = real_httpx.HTTPStatusError
            mock_httpx.RequestError = real_httpx.RequestError

            successful, failures = asyncio.run(
                fetch_predictions_for_tickers(tickers, "http://localhost:9000")
            )

        assert len(successful) == 2
        assert {r["ticker"] for r in successful} == {"AAPL", "MSFT"}
        assert len(failures) == 1
        assert failures[0]["ticker"] == "FAIL"
        assert "timeout" in failures[0]["reason"].lower()

    def test_concurrent_fetch_respects_max_concurrency(self):
        """Semaphore limits active in-flight requests to max_concurrency."""
        tickers = ["T1", "T2", "T3", "T4", "T5"]
        max_concurrent = 2
        peak_concurrent = 0
        current_concurrent = 0

        async def _mock_post(url, json=None, **kwargs):
            nonlocal peak_concurrent, current_concurrent
            current_concurrent += 1
            peak_concurrent = max(peak_concurrent, current_concurrent)
            await asyncio.sleep(0)  # yield to allow other coroutines to run
            current_concurrent -= 1
            return _make_mock_response(json["ticker"])

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=_mock_post)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("paper_trader.engine.prediction_client.httpx") as mock_httpx:
            mock_httpx.AsyncClient.return_value = mock_client
            mock_httpx.TimeoutException = Exception
            mock_httpx.HTTPStatusError = Exception
            mock_httpx.RequestError = Exception

            successful, failures = asyncio.run(
                fetch_predictions_for_tickers(
                    tickers, "http://localhost:9000", max_concurrency=max_concurrent
                )
            )

        assert len(successful) == 5
        assert failures == []
        # With asyncio.sleep(0) yielding between acquire and release, peak should
        # not exceed max_concurrent+1 (scheduling granularity allows slight overrun)
        assert peak_concurrent <= max_concurrent + 1


# ---------------------------------------------------------------------------
# Prediction-run capture tests (local evidence store instrumentation)
# ---------------------------------------------------------------------------

def _make_capture_response(ticker: str, *, extra: dict | None = None) -> MagicMock:
    """Mock httpx response with status_code, for capture tests."""
    resp = MagicMock()
    resp.status_code = 200
    resp.raise_for_status = MagicMock()
    payload = {
        "ticker": ticker,
        "current_price": "150.50",
        "ensemble_day5": "155.75",
        "d5_change_pct": "3.48",
        "confidence": "87.5",
        "recommendation": "Buy",
        "rationale": ["Models agree"],
        "per_model_summary": [{"model": "Drift", "direction": "Up"}],
        "ran_models": ["Drift", "LinearTrend"],
        "skipped_models": [],
        "model_errors": {},
    }
    if extra:
        payload.update(extra)
    resp.json = MagicMock(return_value=payload)
    return resp


class TestPredictionRunCaptureInstrumentation:
    """fetch_predictions_for_tickers(capture=...) records observational evidence."""

    def test_capture_records_each_successful_call(self):
        tickers = ["AAPL", "MSFT"]

        async def _mock_post(url, json=None, **kwargs):
            return _make_capture_response(json["ticker"])

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=_mock_post)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        capture: list[dict] = []
        with patch("paper_trader.engine.prediction_client.httpx") as mock_httpx:
            mock_httpx.AsyncClient.return_value = mock_client
            mock_httpx.TimeoutException = Exception
            mock_httpx.HTTPStatusError = Exception
            mock_httpx.RequestError = Exception

            successful, failures = asyncio.run(
                fetch_predictions_for_tickers(
                    tickers, "http://localhost:9000", capture=capture
                )
            )

        assert failures == []
        assert len(successful) == 2
        assert len(capture) == 2
        by_ticker = {c["ticker"]: c for c in capture}
        assert set(by_ticker) == {"AAPL", "MSFT"}
        for c in capture:
            assert c["error"] is False
            assert c["error_message"] is None
            assert c["raw_response"] is not None
            assert c["request_payload"] == {"ticker": c["ticker"]}
            assert c["endpoint_url"].endswith("/predict_all_models/")
            assert c["latency_ms"] is not None and c["latency_ms"] >= 0
            assert c["request_ts"] is not None

    def test_capture_records_fetch_failure(self):
        import httpx as real_httpx

        async def _mock_post(url, json=None, **kwargs):
            raise real_httpx.TimeoutException("timeout")

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=_mock_post)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        capture: list[dict] = []
        with patch("paper_trader.engine.prediction_client.httpx") as mock_httpx:
            mock_httpx.AsyncClient.return_value = mock_client
            mock_httpx.TimeoutException = real_httpx.TimeoutException
            mock_httpx.HTTPStatusError = real_httpx.HTTPStatusError
            mock_httpx.RequestError = real_httpx.RequestError

            successful, failures = asyncio.run(
                fetch_predictions_for_tickers(
                    ["FAIL"], "http://localhost:9000", capture=capture
                )
            )

        assert successful == []
        assert len(failures) == 1
        assert len(capture) == 1
        c = capture[0]
        assert c["ticker"] == "FAIL"
        assert c["error"] is True
        assert c["raw_response"] is None
        assert "timeout" in c["error_message"].lower()

    def test_capture_records_service_error_response(self):
        """HTTP 200 with an 'error' key is recorded as an error, raw kept."""

        async def _mock_post(url, json=None, **kwargs):
            return _make_capture_response(
                json["ticker"], extra={"error": "no data for ticker"}
            )

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=_mock_post)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        capture: list[dict] = []
        with patch("paper_trader.engine.prediction_client.httpx") as mock_httpx:
            mock_httpx.AsyncClient.return_value = mock_client
            mock_httpx.TimeoutException = Exception
            mock_httpx.HTTPStatusError = Exception
            mock_httpx.RequestError = Exception

            asyncio.run(
                fetch_predictions_for_tickers(
                    ["AAPL"], "http://localhost:9000", capture=capture
                )
            )

        assert len(capture) == 1
        c = capture[0]
        assert c["error"] is True
        assert c["raw_response"]["error"] == "no data for ticker"
        assert c["error_message"] == "no data for ticker"

    def test_capture_none_is_no_op(self):
        """Default capture=None changes nothing and does not raise."""

        async def _mock_post(url, json=None, **kwargs):
            return _make_capture_response(json["ticker"])

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=_mock_post)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with patch("paper_trader.engine.prediction_client.httpx") as mock_httpx:
            mock_httpx.AsyncClient.return_value = mock_client
            mock_httpx.TimeoutException = Exception
            mock_httpx.HTTPStatusError = Exception
            mock_httpx.RequestError = Exception

            successful, failures = asyncio.run(
                fetch_predictions_for_tickers(["AAPL"], "http://localhost:9000")
            )

        assert len(successful) == 1
        assert failures == []


class TestBuildPredictionRunValues:
    """build_prediction_run_values() maps a capture record to column values."""

    def _success_capture(self, ticker: str = "AAPL", extra: dict | None = None) -> dict:
        now = datetime(2026, 6, 15, tzinfo=timezone.utc)
        raw = {
            "ticker": ticker,
            "current_price": "150.50",
            "ensemble_day5": "155.75",
            "d5_change_pct": "3.48",
            "confidence": "87.5",
            "recommendation": "Buy",
            "rationale": ["Models agree"],
            "per_model_summary": [{"model": "Drift", "direction": "Up"}],
            "ran_models": ["Drift", "LinearTrend"],
            "skipped_models": ["Prophet"],
            "model_errors": {"ETS": "timeout"},
        }
        if extra:
            raw.update(extra)
        return {
            "ticker": ticker,
            "endpoint_url": "http://127.0.0.1:9000/predict_all_models/",
            "request_payload": {"ticker": ticker},
            "request_ts": now,
            "response_ts": now,
            "latency_ms": 42,
            "http_status": 200,
            "raw_response": raw,
            "error": False,
            "error_message": None,
        }

    def test_success_record_normalizes_and_extracts_diagnostics(self):
        values = build_prediction_run_values(self._success_capture("AAPL"))
        assert values["ticker"] == "AAPL"
        assert values["error"] is False
        assert values["normalized_recommendation"] == "BUY"
        assert values["normalized_confidence"] == "0.875"
        assert values["normalized_expected_return_pct"] == "3.48"
        assert values["normalized_forecast_price_5d"] == "155.75"
        assert values["model_consensus"] == {"Drift": "BUY"}
        assert values["ran_models"] == ["Drift", "LinearTrend"]
        assert values["skipped_models"] == ["Prophet"]
        assert values["model_errors"] == {"ETS": "timeout"}
        assert values["latency_ms"] == 42
        assert values["request_payload"] == {"ticker": "AAPL"}
        # The GCP service exposes no version in its response today.
        assert values["service_version"] is None

    def test_failed_record_has_no_normalized_fields(self):
        now = datetime(2026, 6, 15, tzinfo=timezone.utc)
        cap = {
            "ticker": "ZZZ",
            "endpoint_url": "http://127.0.0.1:9000/predict_all_models/",
            "request_payload": {"ticker": "ZZZ"},
            "request_ts": now,
            "response_ts": now,
            "latency_ms": 11,
            "http_status": None,
            "raw_response": None,
            "error": True,
            "error_message": "Request timeout (>30s)",
        }
        values = build_prediction_run_values(cap)
        assert values["error"] is True
        assert values["error_message"] == "Request timeout (>30s)"
        assert values["raw_response"] is None
        assert values["normalized_recommendation"] is None
        assert values["normalized_confidence"] is None
        assert values["model_consensus"] is None
        assert values["ran_models"] is None

    def test_service_error_keeps_raw_without_normalizing(self):
        cap = self._success_capture("AAPL", extra={"error": "no data"})
        cap["error"] = True
        cap["error_message"] = "no data"
        values = build_prediction_run_values(cap)
        assert values["error"] is True
        assert values["raw_response"]["error"] == "no data"
        # diagnostics still extracted even on a service error
        assert values["ran_models"] == ["Drift", "LinearTrend"]
        # but no normalized recommendation is derived from an error response
        assert values["normalized_recommendation"] is None

    def test_service_version_extracted_when_present(self):
        values = build_prediction_run_values(
            self._success_capture("AAPL", extra={"service_version": "8751e35"})
        )
        assert values["service_version"] == "8751e35"
