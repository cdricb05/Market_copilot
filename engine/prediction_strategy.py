"""
engine/prediction_strategy.py — Convert prediction objects to trading signals.

This module accepts structured predictions from ML models (consensus forecasts,
market context, confidence scores) and converts them into standard signal dicts
that feed into the existing decision/risk pipeline.

Converter does NOT directly create orders or decisions. It normalizes predictions
to the Signal interface and delegates direction/sizing logic to the existing
risk engine.

Design principles:
    - Validate all inputs; skip invalid predictions with clear reasons.
    - Preserve prediction metadata in raw_payload for audit/explainability.
    - Use prediction confidence directly (no recalibration).
    - Normalize tickers to uppercase.
    - Return signals ready for run_decision_workflow().
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from paper_trader.constants import SignalDirection


def generate_prediction_signals(
    predictions: list[dict | Any],
    source_run: str,
    now: datetime,
) -> tuple[list[dict], dict[str, str]]:
    """
    Convert prediction objects to trading signals.

    Args:
        predictions: List of prediction dicts with keys:
            ticker: str (required)
            current_price: str (required)
            forecast_price_5d: str (required)
            expected_return_pct: str (required)
            confidence: str|float (required, 0-1)
            recommendation: str (required: BUY|SELL|HOLD)
            reason: str (optional)
            model_consensus: dict (optional)
            market_context: str (optional)
        source_run: Idempotency key for this batch.
        now: Current timestamp.

    Returns:
        (signals, skipped)
        signals: list of dicts {ticker, direction, confidence, signal_ts, source_run, raw_payload}
        skipped: dict {ticker: reason} for invalid predictions

    Behavior:
        - Normalizes tickers to uppercase.
        - Validates numeric fields and confidence.
        - Skips predictions with missing/invalid required fields.
        - Invalid recommendations → skipped.
        - Invalid confidence (non-numeric, <0, >1) → skipped.
        - Preserves full prediction in raw_payload for traceability.
    """
    signals = []
    skipped = {}

    for pred in predictions:
        if pred is None:
            continue

        # Convert dict-like or dataclass to dict
        if not isinstance(pred, dict):
            try:
                pred_dict = vars(pred) if hasattr(pred, "__dict__") else dict(pred)
            except Exception:
                continue
        else:
            pred_dict = pred

        # Extract and normalize ticker
        ticker = pred_dict.get("ticker")
        if not ticker or not isinstance(ticker, str) or not ticker.strip():
            skipped["_missing_ticker"] = "Missing or blank ticker"
            continue
        ticker = ticker.strip().upper()

        # Extract recommendation
        recommendation = pred_dict.get("recommendation")
        if not recommendation or not isinstance(recommendation, str):
            skipped[ticker] = "Missing or invalid recommendation"
            continue
        recommendation = recommendation.upper().strip()

        # Map recommendation to direction
        direction_map = {
            "BUY": SignalDirection.BUY,
            "SELL": SignalDirection.SELL,
            "HOLD": SignalDirection.HOLD,
        }
        direction = direction_map.get(recommendation)
        if direction is None:
            skipped[ticker] = f"Invalid recommendation: {recommendation}"
            continue

        # Extract and validate confidence
        confidence_raw = pred_dict.get("confidence")
        if confidence_raw is None:
            skipped[ticker] = "Missing confidence"
            continue

        try:
            confidence = Decimal(str(confidence_raw))
            if not (Decimal("0") <= confidence <= Decimal("1")):
                skipped[ticker] = f"Confidence {confidence} out of range [0, 1]"
                continue
        except Exception:
            skipped[ticker] = f"Invalid confidence: {confidence_raw}"
            continue

        # Validate numeric fields (current_price, forecast_price_5d, expected_return_pct)
        numeric_fields = {
            "current_price": pred_dict.get("current_price"),
            "forecast_price_5d": pred_dict.get("forecast_price_5d"),
            "expected_return_pct": pred_dict.get("expected_return_pct"),
        }

        for field_name, field_value in numeric_fields.items():
            if field_value is not None:
                try:
                    Decimal(str(field_value))
                except Exception:
                    skipped[ticker] = f"Invalid {field_name}: {field_value}"
                    break
        else:
            # All numeric validations passed
            signal = {
                "ticker": ticker,
                "direction": direction,
                "confidence": confidence,
                "signal_ts": now,
                "source_run": source_run,
                "raw_payload": {
                    "strategy_name": "prediction_v2",
                    "prediction": {
                        "ticker": ticker,
                        "current_price": str(pred_dict.get("current_price", "")),
                        "forecast_price_5d": str(pred_dict.get("forecast_price_5d", "")),
                        "expected_return_pct": str(pred_dict.get("expected_return_pct", "")),
                        "confidence": str(confidence),
                        "recommendation": recommendation,
                        "reason": pred_dict.get("reason", ""),
                    },
                    "model_consensus": pred_dict.get("model_consensus", {}),
                    "market_context": pred_dict.get("market_context", ""),
                },
            }
            signals.append(signal)

    return signals, skipped
