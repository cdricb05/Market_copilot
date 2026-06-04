"""
engine/prediction_client.py — Fetch predictions from external stock prediction API.

This module fetches predictions from the GCP stock prediction service and normalizes
them into Paper Trader's prediction contract for use with generate_prediction_signals().

Design principles:
    - Graceful per-ticker failure: one bad ticker doesn't break the batch.
    - No exceptions raised to callers; failures are returned as dicts with reasons.
    - Normalization is separate from fetching for testability.
    - Uses httpx for async-capable HTTP client.
    - Bounded concurrency via asyncio.Semaphore (max_concurrency, default 4).
    - Results are returned in original input-ticker order.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any

try:
    import httpx
except ImportError:
    httpx = None


async def fetch_predictions_for_tickers(
    tickers: list[str],
    api_url: str,
    timeout_seconds: int = 30,
    max_concurrency: int = 4,
) -> tuple[list[dict], list[dict]]:
    """
    Fetch predictions from external stock prediction API for a batch of tickers.

    Args:
        tickers: List of stock tickers (case-insensitive).
        api_url: Base URL of the prediction service (e.g., http://127.0.0.1:9000).
        timeout_seconds: HTTP request timeout in seconds.
        max_concurrency: Maximum number of in-flight requests at once (default 4).

    Returns:
        (successful_predictions, failures)
        successful_predictions: list of raw API response dicts, in input ticker order.
        failures: list of dicts {ticker, reason} for tickers that couldn't be fetched.

    Behavior:
        - Normalizes tickers to uppercase.
        - One failed ticker doesn't block others.
        - Requests run concurrently up to max_concurrency via asyncio.Semaphore.
        - Returns raw responses (not normalized) — caller must call normalize_prediction_response().
        - Network errors, timeouts, and service errors are caught and returned as failures.
    """
    if not tickers:
        return [], []

    if httpx is None:
        return [], [{"ticker": t.upper(), "reason": "httpx not installed"} for t in tickers]

    if not api_url:
        return [], [{"ticker": t.upper(), "reason": "STOCK_PREDICTION_API_URL not configured"} for t in tickers]

    normalized_tickers = [t.upper() for t in tickers]
    endpoint = f"{api_url.rstrip('/')}/predict_all_models/"
    semaphore = asyncio.Semaphore(max(1, max_concurrency))

    successful_by_ticker: dict[str, dict] = {}
    failed: dict[str, str] = {}

    async def _fetch_one(ticker: str, client: "httpx.AsyncClient") -> None:
        async with semaphore:
            try:
                response = await client.post(endpoint, json={"ticker": ticker})
                response.raise_for_status()
                data = response.json()
                if "ticker" not in data:
                    data["ticker"] = ticker
                successful_by_ticker[ticker] = data
            except httpx.TimeoutException:
                failed[ticker] = f"Request timeout (>{timeout_seconds}s)"
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    failed[ticker] = "Ticker not found (404)"
                elif e.response.status_code == 503:
                    failed[ticker] = "Prediction service unavailable (503)"
                else:
                    failed[ticker] = f"HTTP {e.response.status_code}"
            except httpx.RequestError as e:
                failed[ticker] = f"Connection error: {str(e)[:50]}"
            except Exception as e:
                failed[ticker] = f"Failed to fetch: {str(e)[:50]}"

    async with httpx.AsyncClient(timeout=timeout_seconds) as client:
        await asyncio.gather(*[_fetch_one(t, client) for t in normalized_tickers])

    # Preserve input order
    successful = [successful_by_ticker[t] for t in normalized_tickers if t in successful_by_ticker]
    failures = [{"ticker": t, "reason": r} for t, r in failed.items()]
    return successful, failures


def normalize_prediction_response(raw: dict[str, Any] | None) -> dict[str, Any] | None:
    """
    Transform external API response to Paper Trader prediction contract.

    Wrapper around normalize_prediction_response_with_error() for backwards compatibility.
    Returns the normalized dict or None; error reason is discarded.

    Args:
        raw: Raw API response dict or None.

    Returns:
        Normalized dict ready for generate_prediction_signals(), or None if invalid.
    """
    normalized, _ = normalize_prediction_response_with_error(raw)
    return normalized


def _normalize_recommendation(raw_rec: str) -> tuple[str | None, str | None]:
    """
    Normalize recommendation from various API formats to BUY/SELL/HOLD.

    Handles:
        - STRONG BUY, STRONG_BUY, Strong Buy, strong buy → BUY
        - STRONG SELL, STRONG_SELL, Strong Sell, strong sell → SELL
        - BUY, SELL, HOLD (case-insensitive)

    Args:
        raw_rec: Raw recommendation string from API.

    Returns:
        (normalized_recommendation, error_reason)
        - On success: (BUY|SELL|HOLD, None)
        - On error: (None, error_reason_string)
    """
    if not raw_rec or not isinstance(raw_rec, str):
        return None, "Invalid recommendation type"

    # Normalize: uppercase, strip whitespace, replace underscores with spaces
    normalized = raw_rec.upper().strip().replace("_", " ")

    # Map strong recommendations to base recommendations
    recommendation_map = {
        "STRONG BUY": "BUY",
        "STRONG SELL": "SELL",
        "BUY": "BUY",
        "SELL": "SELL",
        "HOLD": "HOLD",
    }

    if normalized in recommendation_map:
        return recommendation_map[normalized], None

    return None, f"Invalid recommendation: {raw_rec}"


def normalize_prediction_response_with_error(
    raw: dict[str, Any] | None,
) -> tuple[dict[str, Any] | None, str | None]:
    """
    Transform external API response to Paper Trader prediction contract with error reason.

    GCP API response fields map to Paper Trader contract as follows:
        - ticker → ticker
        - current_price → current_price
        - ensemble_day5 → forecast_price_5d
        - d5_change_pct → expected_return_pct
        - confidence / 100 → confidence (0-1 decimal)
        - recommendation (uppercase) → recommendation
        - per_model_summary → model_consensus (derive votes)
        - rationale (list) → reason (joined string)
        - recommendation → market_context (BUY→bullish, SELL→bearish, HOLD→neutral)

    Special behavior for HOLD:
        - HOLD with missing/null confidence defaults to "0.50".
        - BUY and SELL require valid confidence; missing confidence is an error.

    Args:
        raw: Raw API response dict or None.

    Returns:
        (normalized_dict, error_reason)
        - On success: (dict, None)
        - On error: (None, error_reason_string)

    Behavior:
        - Skips invalid responses (None, missing required fields).
        - Uses Decimal for numeric fields (confidence, prices).
        - Normalizes recommendation: accepts STRONG BUY/SELL variants.
        - Derives market_context and model_consensus from response.
    """
    if not raw or not isinstance(raw, dict):
        return None, "Invalid response: not a dict"

    # Extract ticker
    ticker = raw.get("ticker")
    if not ticker or not isinstance(ticker, str):
        return None, "Missing or invalid ticker"

    # Extract and validate required fields
    try:
        current_price = str(raw.get("current_price", ""))
        forecast_price_5d = str(raw.get("ensemble_day5", ""))
        expected_return_pct = str(raw.get("d5_change_pct", ""))

        # Validate that we can parse these as Decimal
        if current_price:
            Decimal(current_price)
        if forecast_price_5d:
            Decimal(forecast_price_5d)
        if expected_return_pct:
            Decimal(expected_return_pct)
    except Exception:
        return None, "Invalid numeric fields"

    # Extract and validate recommendation first
    recommendation = raw.get("recommendation")
    if not recommendation or not isinstance(recommendation, str):
        return None, "Missing or invalid recommendation"

    recommendation, rec_error = _normalize_recommendation(recommendation)
    if recommendation is None:
        return None, rec_error

    # Extract and normalize confidence (0-100 → 0-1)
    confidence_raw = raw.get("confidence")

    # Special handling for HOLD: allow missing confidence, default to 0.50
    if recommendation == "HOLD":
        if confidence_raw is None or confidence_raw == "":
            # Default confidence for HOLD
            confidence = Decimal("0.50")
        else:
            try:
                confidence_pct = Decimal(str(confidence_raw))
                confidence = confidence_pct / Decimal("100")
                if not (Decimal("0") <= confidence <= Decimal("1")):
                    return None, "Invalid confidence (out of range)"
            except Exception:
                return None, "Invalid confidence"
    else:
        # BUY and SELL require confidence
        if confidence_raw is None:
            return None, f"Missing confidence for {recommendation}"

        try:
            confidence_pct = Decimal(str(confidence_raw))
            confidence = confidence_pct / Decimal("100")
            if not (Decimal("0") <= confidence <= Decimal("1")):
                return None, "Invalid confidence (out of range)"
        except Exception:
            return None, "Invalid confidence"

    # Derive market_context from recommendation
    market_context_map = {
        "BUY": "bullish",
        "SELL": "bearish",
        "HOLD": "neutral",
    }
    market_context = market_context_map.get(recommendation, "neutral")

    # Join rationale into reason
    rationale = raw.get("rationale")
    if isinstance(rationale, list):
        reason = " ".join(str(r).strip() for r in rationale if r)
    else:
        reason = str(rationale) if rationale else ""

    # Derive model_consensus from per_model_summary
    model_consensus = _derive_model_consensus(raw.get("per_model_summary", {}))

    # Build normalized prediction
    normalized = {
        "ticker": ticker,
        "current_price": current_price,
        "forecast_price_5d": forecast_price_5d,
        "expected_return_pct": expected_return_pct,
        "confidence": str(confidence),
        "recommendation": recommendation,
        "reason": reason,
        "model_consensus": model_consensus,
        "market_context": market_context,
    }

    return normalized, None


def _derive_model_consensus(per_model_summary: dict[str, Any] | list[dict[str, Any]]) -> dict[str, str]:
    """
    Derive model consensus votes from per_model_summary.

    Supports two GCP response shapes:

    A. Real GCP list shape (newer API):
       per_model_summary = [
         {"model": "Drift", "direction": "Up"},
         {"model": "LinearTrend", "direction": "Down"},
         ...
       ]

    B. Dict shape (test/legacy):
       per_model_summary = {
         "prophet": {"direction": "Up"},
         "arima": {"direction": "Down"},
         ...
       }

    Direction mapping: "Up"→"BUY", "Down"→"SELL", "Flat"→"HOLD"
    Also handles: "bullish"→"BUY", "bearish"→"SELL", "neutral"→"HOLD"

    Args:
        per_model_summary: List or dict of model summaries.

    Returns:
        Dict mapping model names to vote (BUY/SELL/HOLD).
    """
    consensus = {}
    direction_map = {
        "Up": "BUY",
        "Down": "SELL",
        "Flat": "HOLD",
        "bullish": "BUY",
        "bearish": "SELL",
        "neutral": "HOLD",
        "BUY": "BUY",
        "SELL": "SELL",
        "HOLD": "HOLD",
    }

    if isinstance(per_model_summary, list):
        for item in per_model_summary:
            if isinstance(item, dict):
                model_name = item.get("model")
                direction = item.get("direction")
                if model_name and direction:
                    vote = direction_map.get(direction, direction.upper() if isinstance(direction, str) else None)
                    if vote:
                        consensus[model_name] = vote
    elif isinstance(per_model_summary, dict):
        for model_name, model_data in per_model_summary.items():
            if isinstance(model_data, dict):
                direction = model_data.get("direction")
                if direction:
                    vote = direction_map.get(direction, direction.upper() if isinstance(direction, str) else None)
                    if vote:
                        consensus[model_name] = vote

    return consensus
