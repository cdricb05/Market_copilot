"""
tests/test_prediction_client.py — Unit tests for prediction_client module.

Tests focus on:
    - Normalization of external API responses to internal contract.
    - Per-ticker fetch error handling.
    - Field mapping and transformation (confidence scaling, recommendation uppercase, etc.).
    - Model consensus derivation from per_model_summary.
    - Market context mapping based on recommendation.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from paper_trader.engine.prediction_client import (
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
