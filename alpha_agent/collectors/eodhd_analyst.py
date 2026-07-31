"""
alpha_agent/collectors/eodhd_analyst.py — Stage 8 EODHD ANALYST-VINTAGE collector.

The live EODHD entitlement probe proved that ``/fundamentals`` exposes the full
analyst surface — earnings estimates (avg/high/low), analyst counts, 7-day and
30-day up/down revisions, estimate trends, revenue estimates, ratings + rating
counts, price targets, historical EPS actual/estimate + surprise, and filing
dates. Those values are ENTITLED, but the vendor serves them as a CURRENT
SNAPSHOT (no per-day vintages), so using them in a backtest is point-in-time-
lookahead-unsafe.

This collector is the leakage-safe production lane: it persists ONE IMMUTABLE
DAILY VINTAGE per security and snapshot timestamp. The availability of every
emitted record is the snapshot capture date (never earlier — the values are only
known as of the day they were captured), so a vintage built forward from the
first snapshot accumulates a genuine point-in-time analyst history. It NEVER uses
the current snapshot to backfill an earlier date.

Idempotency + immutability: a vintage file for ``(symbol, snapshot_date)`` is
written first-write-wins (``os.replace`` of a temp file, never overwriting an
existing vintage). A second execution on the same day sees the vintage already on
disk and skips the fetch entirely — no duplicate vintage, no duplicate record, no
network call.

Credential convention: the EXISTING ``EODHD_API_KEY`` environment variable
(presence-checked only, read into memory solely to build the request, redacted
from every persisted fingerprint, never printed/stored).
"""
from __future__ import annotations

import json
import os
import urllib.parse
from pathlib import Path
from typing import Any, Optional

from ..source_contracts import (
    ENT_AUTH_FAILED, ENT_ENTITLED, ENT_NOT_ENTITLED, ENT_RATE_LIMITED,
    ENT_UNKNOWN, EM_MATCHED_EXACT, RT_FUNDAMENTAL_FACT, SH_BLOCKED_CREDENTIAL,
    SH_FAILED, SH_HEALTHY, build_normalized_record,
)
from .base import BaseCollector

# Analyst estimate-trend fields carried through into each vintage record (the
# precise fields the live entitlement probe confirmed present).
_TREND_FIELDS = (
    "period", "date", "growth", "earningsEstimateAvg", "earningsEstimateLow",
    "earningsEstimateHigh", "earningsEstimateYearAgoEps",
    "earningsEstimateNumberOfAnalysts", "earningsEstimateGrowth",
    "revenueEstimateAvg", "revenueEstimateLow", "revenueEstimateHigh",
    "revenueEstimateYearAgoEps", "revenueEstimateNumberOfAnalysts",
    "revenueEstimateGrowth", "epsTrendCurrent", "epsTrend7daysAgo",
    "epsTrend30daysAgo", "epsTrend60daysAgo", "epsTrend90daysAgo",
    "epsRevisionsUpLast7days", "epsRevisionsUpLast30days",
    "epsRevisionsDownLast7days", "epsRevisionsDownLast30days")
_RATING_FIELDS = ("Rating", "TargetPrice", "StrongBuy", "Buy", "Hold", "Sell",
                  "StrongSell")


def _base_ticker(symbol: str) -> str:
    return symbol.split(".")[0].upper()


class EodhdAnalystCollector(BaseCollector):
    source_id = "eodhd_analyst"
    requires_credential = True

    # ------------------------------------------------------------------ #
    def _resolve_key(self) -> Optional[str]:
        for name in self.ctx.source_cfg.get("allowed_env_vars", ["EODHD_API_KEY"]):
            value = self.ctx.env.get(name)
            if value:
                return value
        return None

    def _url(self, path: str, key: str, **params: Any) -> str:
        base = self.ctx.source_cfg.get("base_url", "https://eodhd.com/api").rstrip("/")
        query = {"api_token": key, "fmt": "json"}
        query.update({k: v for k, v in params.items() if v is not None})
        return "%s/%s?%s" % (base, path.lstrip("/"), urllib.parse.urlencode(query))

    @staticmethod
    def _parse_json(body: Optional[bytes]) -> tuple[bool, Any]:
        if not body:
            return False, None
        try:
            return True, json.loads(body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return False, None

    @staticmethod
    def _entitlement_from_status(status: Optional[int], parse_ok: bool,
                                 has_analyst: bool) -> str:
        if status == 200 and parse_ok:
            return ENT_ENTITLED if has_analyst else ENT_UNKNOWN
        if status == 401:
            return ENT_AUTH_FAILED
        if status in (402, 403):
            return ENT_NOT_ENTITLED
        if status == 429:
            return ENT_RATE_LIMITED
        return ENT_UNKNOWN

    def _fundamentals_filter(self) -> str:
        return self.ctx.source_cfg.get(
            "analyst_filter", "General,Highlights,AnalystRatings,Earnings")

    def _vintage_root(self) -> Path:
        sub = self.ctx.source_cfg.get("vintage_subdir", "vintages/eodhd_analyst")
        return Path(self.ctx.archive.output_root) / sub

    # ------------------------------------------------------------------ #
    def _probe(self, key: str) -> dict:
        sym = (self.ctx.source_cfg.get("sample_symbols") or ["AAPL.US"])[0]
        res = self.fetch(self._url("fundamentals/%s" % sym, key,
                                   filter=self._fundamentals_filter()),
                         expect="text", archive=False, extension="json")
        parse_ok, obj = self._parse_json(res["body"]) if res["ok"] else (False, None)
        has_analyst = bool(parse_ok and isinstance(obj, dict) and (
            obj.get("AnalystRatings")
            or ((obj.get("Earnings") or {}).get("Trend"))))
        state = self._entitlement_from_status(res["status"], parse_ok, has_analyst)
        probe = {"family": "analyst_fundamentals", "state": state,
                 "detail": "probe fundamentals(%s) -> %s" % (sym, state),
                 "http_status": res["status"]}
        self.entitlements.append(probe)
        return probe

    # ------------------------------------------------------------------ #
    def audit(self) -> dict:
        key = self._resolve_key()
        if key is None:
            return self.blocked_result(
                SH_BLOCKED_CREDENTIAL,
                "no EODHD credential present (checked env var NAMES %s); set the "
                "existing convention EODHD_API_KEY to enable the analyst-vintage "
                "lane" % self.ctx.source_cfg.get("allowed_env_vars"),
                credential_present=False)
        probe = self._probe(key)
        state = SH_HEALTHY if probe["state"] == ENT_ENTITLED else SH_FAILED
        return self.result(overall_state=state, credential_present=True,
                           entitlement_summary="analyst_fundamentals: %s"
                                               % probe["state"])

    # ------------------------------------------------------------------ #
    def collect(self, as_of: str) -> dict:
        key = self._resolve_key()
        if key is None:
            return self.blocked_result(
                SH_BLOCKED_CREDENTIAL,
                "no EODHD credential present (checked env var NAMES %s); set the "
                "existing convention EODHD_API_KEY to enable the analyst-vintage "
                "lane" % self.ctx.source_cfg.get("allowed_env_vars"),
                credential_present=False)
        snapshot_date = as_of  # the vintage timestamp == the collection date
        probe = self._probe(key)
        retrieved = self.ctx.now_iso()
        vintage_dir = self._vintage_root() / snapshot_date
        written, skipped = 0, 0

        # Record (first-write-wins) the hard PIT floor for the prospective family.
        self._record_boundary(snapshot_date)

        for sym in self.ctx.source_cfg.get("sample_symbols", []):
            ticker = _base_ticker(sym)
            vintage_path = vintage_dir / ("%s.json" % ticker)
            if vintage_path.exists():
                # Immutable first-write-wins: a same-day vintage already exists.
                # Skip the fetch entirely -> idempotent (no dup vintage/record,
                # no network call).
                skipped += 1
                self.inventory.setdefault("idempotent_symbols", []).append(ticker)
                continue
            if probe["state"] != ENT_ENTITLED:
                continue
            res = self.fetch(self._url("fundamentals/%s" % sym, key,
                                       filter=self._fundamentals_filter()),
                             expect="text", extension="json",
                             business_date=snapshot_date,
                             native_id="analyst_fundamentals|%s|%s" % (sym,
                                                                       snapshot_date),
                             content_type="application/json")
            if not res["ok"]:
                continue
            ok, obj = self._parse_json(res["body"])
            if not ok or not isinstance(obj, dict):
                self.record_error("PARSE_ERROR",
                                  "analyst fundamentals not a JSON object for %s" % sym)
                continue
            vintage = self._build_vintage(sym, ticker, snapshot_date, retrieved,
                                          obj, res["raw"]["raw_object_id"])
            self._write_vintage(vintage_path, vintage)
            written += 1
            self._emit_records(sym, ticker, snapshot_date, retrieved, obj,
                               res["raw"]["raw_object_id"])

        self.inventory["vintages_written"] = written
        self.inventory["vintages_idempotent_skipped"] = skipped
        self.inventory["snapshot_date"] = snapshot_date
        self.inventory["vintage_dir"] = str(vintage_dir)
        self.inventory["first_snapshot_boundary"] = self._first_boundary()
        self.cursor = {"last_snapshot_date": snapshot_date,
                       "vintages_written": written,
                       "vintages_idempotent_skipped": skipped,
                       "first_snapshot_boundary": self._first_boundary(),
                       "entitlement": probe["state"]}
        if probe["state"] == ENT_ENTITLED and (written or skipped):
            state = SH_HEALTHY
        elif probe["state"] == ENT_AUTH_FAILED:
            state = SH_BLOCKED_CREDENTIAL
        else:
            state = SH_FAILED
        return self.result(overall_state=state, credential_present=True,
                           entitlement_summary="analyst_fundamentals: %s; "
                                               "vintages written=%d skipped(idempotent)=%d"
                                               % (probe["state"], written, skipped))

    # ------------------------------------------------------------------ #
    def _build_vintage(self, sym: str, ticker: str, snapshot_date: str,
                       retrieved: str, obj: dict, raw_id: str) -> dict:
        general = obj.get("General") if isinstance(obj.get("General"), dict) else {}
        highlights = obj.get("Highlights") if isinstance(obj.get("Highlights"), dict) else {}
        ratings = obj.get("AnalystRatings") if isinstance(obj.get("AnalystRatings"), dict) else {}
        trend = (obj.get("Earnings") or {}).get("Trend") \
            if isinstance(obj.get("Earnings"), dict) else {}
        trend = trend if isinstance(trend, dict) else {}
        cap = int(self.ctx.source_cfg.get("estimate_periods_cap", 8))
        estimate_trend = []
        for _period_key, row in sorted(trend.items())[-cap:]:
            if isinstance(row, dict):
                estimate_trend.append({k: row.get(k) for k in _TREND_FIELDS
                                       if k in row})
        return {
            "provider": "EODHD",
            "endpoint": "fundamentals/%s?filter=%s" % (sym,
                                                       self._fundamentals_filter()),
            "symbol": sym, "ticker": ticker,
            "cik": general.get("CIK"), "name": general.get("Name"),
            "snapshot_date": snapshot_date, "retrieved_at": retrieved,
            "raw_object_id": raw_id,
            "entitlement_evidence": "ENTITLED via live /fundamentals probe (HTTP 200"
                                    " with AnalystRatings/Earnings.Trend present)",
            "price_target": {
                "wall_street_target_price": highlights.get("WallStreetTargetPrice"),
                "analyst_target_price": ratings.get("TargetPrice")},
            "analyst_ratings": {k: ratings.get(k) for k in _RATING_FIELDS
                                if k in ratings},
            "estimate_trend": estimate_trend,
            "point_in_time": {
                "availability": snapshot_date,
                "hard_pit_floor": snapshot_date,
                "note": "immutable daily vintage; availability = snapshot capture "
                        "date; NEVER backfilled before the first snapshot"}}

    def _write_vintage(self, path: Path, vintage: dict) -> None:
        """Write ONE immutable daily vintage. Never overwrite an existing file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():  # defence in depth (caller already checked)
            return
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(vintage, indent=1, sort_keys=True,
                                  default=str), encoding="utf-8")
        os.replace(tmp, path)

    def _record_boundary(self, snapshot_date: str) -> None:
        """First-write-wins record of the hard PIT floor for the analyst family."""
        root = self._vintage_root()
        root.mkdir(parents=True, exist_ok=True)
        bpath = root / "_prospective_boundary.json"
        if bpath.exists():
            return
        tmp = bpath.with_suffix(".json.tmp")
        tmp.write_text(json.dumps({
            "fields": ["analyst_estimate_revisions", "price_targets",
                       "estimate_counts"],
            "first_snapshot_date": snapshot_date,
            "hard_pit_floor": snapshot_date,
            "backfill_before_floor_allowed": False}, indent=1, sort_keys=True),
            encoding="utf-8")
        os.replace(tmp, bpath)

    def _first_boundary(self) -> Optional[str]:
        bpath = self._vintage_root() / "_prospective_boundary.json"
        if not bpath.exists():
            return None
        try:
            return json.loads(bpath.read_text(encoding="utf-8")).get(
                "first_snapshot_date")
        except (OSError, ValueError):
            return None

    # ------------------------------------------------------------------ #
    def _emit_records(self, sym: str, ticker: str, snapshot_date: str,
                      retrieved: str, obj: dict, raw_id: str) -> None:
        general = obj.get("General") if isinstance(obj.get("General"), dict) else {}
        highlights = obj.get("Highlights") if isinstance(obj.get("Highlights"), dict) else {}
        ratings = obj.get("AnalystRatings") if isinstance(obj.get("AnalystRatings"), dict) else {}
        trend = (obj.get("Earnings") or {}).get("Trend") \
            if isinstance(obj.get("Earnings"), dict) else {}
        trend = trend if isinstance(trend, dict) else {}
        cik = str(general.get("CIK")) if general.get("CIK") else None
        # Availability of a prospective snapshot IS the capture date — the values
        # are only known as of that day; safe for any date >= snapshot_date.
        pit_warn = ["PROSPECTIVE_SNAPSHOT: values captured as a current vendor "
                    "snapshot; available_at = snapshot capture date (leakage-safe "
                    "forward only); NEVER valid before the first snapshot"]

        def _emit(event_type: str, native_suffix: str, payload: dict) -> None:
            payload = dict(payload)
            payload["revision_vintage_date"] = snapshot_date
            payload["snapshot_date"] = snapshot_date
            self.records.append(build_normalized_record(
                record_type=RT_FUNDAMENTAL_FACT, source_id=self.source_id,
                source_native_id="%s|%s|%s" % (native_suffix, ticker, snapshot_date),
                raw_object_id=raw_id, retrieved_at=retrieved,
                observed_at=snapshot_date, effective_at=snapshot_date,
                available_at=snapshot_date, ticker=ticker, company_id=cik,
                event_type=event_type, payload=payload,
                entity_mapping_confidence=EM_MATCHED_EXACT,
                provenance="EODHD /fundamentals analyst snapshot (immutable daily "
                           "vintage)", quality_warnings=list(pit_warn)))
            self.note_event_time(snapshot_date)

        # (1) price target vintage
        if highlights.get("WallStreetTargetPrice") is not None \
                or ratings.get("TargetPrice") is not None:
            _emit("ANALYST_PRICE_TARGET_VINTAGE", "analyst_pt", {
                "wall_street_target_price": highlights.get("WallStreetTargetPrice"),
                "analyst_target_price": ratings.get("TargetPrice")})
        # (2) rating-counts vintage
        if ratings:
            _emit("ANALYST_RATING_VINTAGE", "analyst_rating",
                  {k: ratings.get(k) for k in _RATING_FIELDS if k in ratings})
        # (3) per-estimate-period estimate/revision vintage
        cap = int(self.ctx.source_cfg.get("estimate_periods_cap", 8))
        for period_key, row in sorted(trend.items())[-cap:]:
            if not isinstance(row, dict):
                continue
            payload = {k: row.get(k) for k in _TREND_FIELDS if k in row}
            payload["estimate_period"] = row.get("date") or period_key
            self.records.append(build_normalized_record(
                record_type=RT_FUNDAMENTAL_FACT, source_id=self.source_id,
                source_native_id="analyst_est|%s|%s|%s" % (
                    ticker, snapshot_date, row.get("date") or period_key),
                raw_object_id=raw_id, retrieved_at=retrieved,
                observed_at=snapshot_date, effective_at=snapshot_date,
                available_at=snapshot_date, ticker=ticker, company_id=cik,
                event_type="ANALYST_ESTIMATE_VINTAGE",
                payload=dict(payload, revision_vintage_date=snapshot_date,
                             snapshot_date=snapshot_date),
                entity_mapping_confidence=EM_MATCHED_EXACT,
                provenance="EODHD /fundamentals Earnings.Trend snapshot (immutable "
                           "daily vintage)", quality_warnings=list(pit_warn)))
            self.note_event_time(snapshot_date)
