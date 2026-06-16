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
import time
from datetime import datetime, timezone
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
    capture: list[dict] | None = None,
) -> tuple[list[dict], list[dict]]:
    """
    Fetch predictions from external stock prediction API for a batch of tickers.

    Args:
        tickers: List of stock tickers (case-insensitive).
        api_url: Base URL of the prediction service (e.g., http://127.0.0.1:9000).
        timeout_seconds: HTTP request timeout in seconds.
        max_concurrency: Maximum number of in-flight requests at once (default 4).
        capture: Optional mutable list. When provided, one capture record dict is
            appended per ticker (request/response timing, URL, payload, raw
            response or error). Used by the local prediction-run capture store.
            When None (default) no capture work is done and behavior is unchanged.

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
        - Capture is best-effort and purely observational; it never alters fetch
          results and never raises.
    """
    if not tickers:
        return [], []

    if httpx is None:
        reason = "httpx not installed"
        for t in tickers:
            _append_capture(capture, t.upper(), endpoint=None, error=True, reason=reason)
        return [], [{"ticker": t.upper(), "reason": reason} for t in tickers]

    if not api_url:
        reason = "STOCK_PREDICTION_API_URL not configured"
        for t in tickers:
            _append_capture(capture, t.upper(), endpoint=None, error=True, reason=reason)
        return [], [{"ticker": t.upper(), "reason": reason} for t in tickers]

    normalized_tickers = [t.upper() for t in tickers]
    endpoint = f"{api_url.rstrip('/')}/predict_all_models/"
    semaphore = asyncio.Semaphore(max(1, max_concurrency))

    successful_by_ticker: dict[str, dict] = {}
    failed: dict[str, str] = {}

    async def _fetch_one(ticker: str, client: "httpx.AsyncClient") -> None:
        async with semaphore:
            request_ts = datetime.now(timezone.utc)
            _t0 = time.perf_counter()
            http_status: int | None = None
            try:
                response = await client.post(endpoint, json={"ticker": ticker})
                http_status = getattr(response, "status_code", None)
                response.raise_for_status()
                data = response.json()
                if "ticker" not in data:
                    data["ticker"] = ticker
                successful_by_ticker[ticker] = data
                # The remote service signals model/data failures with HTTP 200
                # plus an "error" key, not a non-2xx status. Surface that.
                service_error = data.get("error") if isinstance(data, dict) else None
                _append_capture(
                    capture, ticker, endpoint=endpoint, started=_t0,
                    request_ts=request_ts, http_status=http_status,
                    raw_response=data,
                    error=bool(service_error),
                    reason=str(service_error) if service_error else None,
                )
            except httpx.TimeoutException:
                reason = f"Request timeout (>{timeout_seconds}s)"
                failed[ticker] = reason
                _append_capture(capture, ticker, endpoint=endpoint, started=_t0,
                                request_ts=request_ts, http_status=http_status,
                                error=True, reason=reason)
            except httpx.HTTPStatusError as e:
                status_code = getattr(getattr(e, "response", None), "status_code", None)
                if status_code == 404:
                    reason = "Ticker not found (404)"
                elif status_code == 503:
                    reason = "Prediction service unavailable (503)"
                else:
                    reason = f"HTTP {status_code}"
                failed[ticker] = reason
                _append_capture(capture, ticker, endpoint=endpoint, started=_t0,
                                request_ts=request_ts, http_status=status_code,
                                error=True, reason=reason)
            except httpx.RequestError as e:
                reason = f"Connection error: {str(e)[:50]}"
                failed[ticker] = reason
                _append_capture(capture, ticker, endpoint=endpoint, started=_t0,
                                request_ts=request_ts, http_status=http_status,
                                error=True, reason=reason)
            except Exception as e:
                reason = f"Failed to fetch: {str(e)[:50]}"
                failed[ticker] = reason
                _append_capture(capture, ticker, endpoint=endpoint, started=_t0,
                                request_ts=request_ts, http_status=http_status,
                                error=True, reason=reason)

    async with httpx.AsyncClient(timeout=timeout_seconds) as client:
        await asyncio.gather(*[_fetch_one(t, client) for t in normalized_tickers])

    # Preserve input order
    successful = [successful_by_ticker[t] for t in normalized_tickers if t in successful_by_ticker]
    failures = [{"ticker": t, "reason": r} for t, r in failed.items()]
    return successful, failures


def _append_capture(
    capture: list[dict] | None,
    ticker: str,
    *,
    endpoint: str | None,
    started: float | None = None,
    request_ts: "datetime | None" = None,
    http_status: int | None = None,
    raw_response: dict | None = None,
    error: bool = False,
    reason: str | None = None,
) -> None:
    """
    Append a single capture record to the capture list, if one was provided.

    Best-effort and silent: capture is an observational side-channel for the
    local prediction-run store. It must never raise into the fetch path.
    """
    if capture is None:
        return
    try:
        response_ts = datetime.now(timezone.utc)
        latency_ms = int((time.perf_counter() - started) * 1000) if started is not None else None
        capture.append(
            {
                "ticker": ticker,
                "endpoint_url": endpoint,
                "request_payload": {"ticker": ticker},
                "request_ts": request_ts or response_ts,
                "response_ts": response_ts,
                "latency_ms": latency_ms,
                "http_status": http_status,
                "raw_response": raw_response if isinstance(raw_response, dict) else None,
                "error": bool(error),
                "error_message": reason,
            }
        )
    except Exception:
        # Never let capture bookkeeping break a fetch.
        return


def build_prediction_run_values(capture: dict) -> dict[str, Any]:
    """
    Convert one fetch capture record into a flat dict of prediction_runs column
    values (normalized fields resolved, diagnostics extracted).

    Pure / no DB, no I/O: the caller turns this dict into a PredictionRun ORM row
    and persists it. Keeps the DB layer out of the engine module and makes the
    mapping unit-testable.

    Normalization:
        - Successful, non-error raw responses are run through
          normalize_prediction_response_with_error(); the resulting
          recommendation/confidence/expected_return/forecast/model_consensus are
          stored. If normalization fails, those fields are left null.
    Diagnostics (ran_models / skipped_models / model_errors / version) are copied
    straight from the raw response when present, else null. The current GCP
    service does not expose a service/model version in its response, so
    service_version is typically null and that absence is recorded honestly.
    """
    raw = capture.get("raw_response")
    raw = raw if isinstance(raw, dict) else None

    values: dict[str, Any] = {
        "ticker": (capture.get("ticker") or "").upper() or None,
        # Daily Review session linkage (stamped by the persistence layer when the
        # dispatch belongs to a session/context). Observational only.
        "daily_session_id": capture.get("daily_session_id"),
        "source": capture.get("source"),
        "request_ts": capture.get("request_ts"),
        "response_ts": capture.get("response_ts"),
        "latency_ms": capture.get("latency_ms"),
        "prediction_service_url": capture.get("endpoint_url"),
        "request_payload": capture.get("request_payload"),
        "http_status": capture.get("http_status"),
        "raw_response": raw,
        "normalized_recommendation": None,
        "normalized_confidence": None,
        "normalized_expected_return_pct": None,
        "normalized_forecast_price_5d": None,
        "model_consensus": None,
        "ran_models": None,
        "skipped_models": None,
        "model_errors": None,
        "service_version": None,
        "error": bool(capture.get("error")),
        "error_message": capture.get("error_message"),
    }

    if raw is not None:
        # Diagnostics may be present even on a service-reported error response.
        values["ran_models"] = raw.get("ran_models")
        values["skipped_models"] = raw.get("skipped_models")
        values["model_errors"] = raw.get("model_errors")
        version = (
            raw.get("service_version")
            or raw.get("model_version")
            or raw.get("version")
            or raw.get("commit")
        )
        values["service_version"] = str(version) if version else None

        # Only attempt normalization on a non-error response.
        if not raw.get("error"):
            normalized, _err = normalize_prediction_response_with_error(raw)
            if normalized:
                values["normalized_recommendation"] = normalized.get("recommendation")
                values["normalized_confidence"] = normalized.get("confidence")
                values["normalized_expected_return_pct"] = normalized.get("expected_return_pct")
                values["normalized_forecast_price_5d"] = normalized.get("forecast_price_5d")
                values["model_consensus"] = normalized.get("model_consensus")

    return values


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
