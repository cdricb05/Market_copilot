"""
alpha_agent/collectors/finra.py — SOURCE 4: FINRA daily short-sale volume.

Official public Reg SHO daily files only (cdn.finra.org). Collects the latest
available file plus a bounded recent business-day history for the configured
markets (default CNMS = consolidated NMS).

Contract discipline:
  * every file's encoded business date is validated against the filename date —
    mismatched rows are rejected and surfaced, never silently kept;
  * this is daily short-sale VOLUME and is explicitly labelled as such — it is
    NEVER labelled short interest;
  * records normalize by (symbol, market date, reporting venue);
  * the file's HTTP Last-Modified header (when present) is the observed
    publication timestamp; nothing is fabricated;
  * missing sample symbols and coverage counts are recorded.
"""
from __future__ import annotations

import datetime as _dt
from typing import Optional

from ..source_contracts import (
    EM_MATCHED_EXACT, EM_UNMATCHED, RT_SHORT_VOLUME, SH_DEGRADED, SH_FAILED,
    SH_HEALTHY, build_normalized_record,
)
from .base import BaseCollector


def _weekdays_back(as_of: str, count: int) -> list[str]:
    out: list[str] = []
    day = _dt.date.fromisoformat(as_of)
    while len(out) < count:
        if day.weekday() < 5:
            out.append(day.isoformat())
        day -= _dt.timedelta(days=1)
    return out


class FinraCollector(BaseCollector):
    source_id = "finra"
    requires_credential = False

    def _file_url(self, market: str, day: str) -> str:
        base = self.ctx.source_cfg.get(
            "base_url", "https://cdn.finra.org/equity/regsho/daily").rstrip("/")
        return "%s/%sshvol%s.txt" % (base, market, day.replace("-", ""))

    def _parse_file(self, body: bytes, expected_date: str, market: str,
                    raw_id: str, retrieved: str,
                    published_at: Optional[str]) -> tuple[int, int]:
        """Parse one pipe-delimited daily file. Returns (rows_ok, rows_rejected)."""
        try:
            text = body.decode("utf-8", errors="replace")
        except Exception:  # noqa: BLE001
            self.record_error("PARSE_ERROR", "undecodable FINRA file %s %s"
                              % (market, expected_date))
            return 0, 0
        expected_compact = expected_date.replace("-", "")
        header: Optional[list[str]] = None
        rows_ok = 0
        rows_rejected = 0
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            parts = line.split("|")
            if header is None:
                header = [p.strip() for p in parts]
                continue
            if len(parts) < 5:
                continue
            row = dict(zip(header, (p.strip() for p in parts)))
            row_date = row.get("Date", "")
            if row_date != expected_compact:
                rows_rejected += 1
                continue
            symbol = row.get("Symbol", "")
            if not symbol:
                rows_rejected += 1
                continue
            try:
                # FINRA files carry fractional share volumes — parse as float.
                short_vol = float(row.get("ShortVolume", "") or 0)
                short_exempt = float(row.get("ShortExemptVolume", "") or 0)
                total_vol = float(row.get("TotalVolume", "") or 0)
            except ValueError:
                rows_rejected += 1
                continue
            ratio = round(short_vol / total_vol, 6) if total_vol > 0 else None
            resolved = self.ctx.identity.resolve(symbol)
            payload = {
                "market_date": expected_date,
                "reporting_venue_file": market,
                "market_venues": row.get("Market"),
                "short_volume": short_vol,
                "short_exempt_volume": short_exempt,
                "total_volume": total_vol,
                "short_volume_ratio": ratio,
                "short_volume_ratio_derivation": "computed short_volume/total_volume",
                "measure_note": "daily short-sale VOLUME (Reg SHO); NOT short interest",
            }
            self.records.append(build_normalized_record(
                record_type=RT_SHORT_VOLUME, source_id=self.source_id,
                source_native_id="%s|%s|%s" % (expected_date, symbol, market),
                raw_object_id=raw_id, retrieved_at=retrieved,
                observed_at=expected_date, effective_at=expected_date,
                available_at=published_at, ticker=symbol,
                company_id=resolved["cik"],
                event_type="DAILY_SHORT_SALE_VOLUME", payload=payload,
                entity_mapping_confidence=(EM_MATCHED_EXACT if resolved["cik"]
                                           else (resolved["state"]
                                                 if resolved["state"] != "UNMATCHED"
                                                 else EM_UNMATCHED)),
                provenance="FINRA Reg SHO daily file %sshvol (official public)" % market,
                quality_warnings=(
                    [] if published_at else
                    ["AVAILABLE_AT_FROM_HEADER_MISSING: no Last-Modified header; "
                     "availability left null (not fabricated)"])))
            rows_ok += 1
        if rows_rejected:
            self.record_error(
                "BUSINESS_DATE_MISMATCH",
                "%d rows in %s %s failed business-date/shape validation and were rejected"
                % (rows_rejected, market, expected_date))
        return rows_ok, rows_rejected

    # ------------------------------------------------------------------ #
    def audit(self) -> dict:
        as_of = _dt.date.today().isoformat()
        market = (self.ctx.source_cfg.get("markets") or ["CNMS"])[0]
        found = None
        for day in _weekdays_back(as_of, 4):
            res = self.fetch(self._file_url(market, day), expect="text",
                             archive=False, allow_404=True)
            if res["ok"]:
                found = day
                break
        self.inventory["latest_available_file_date"] = found
        state = SH_HEALTHY if found else SH_FAILED
        return self.result(overall_state=state,
                           entitlement_summary="official public files, no credential required")

    def collect(self, as_of: str) -> dict:
        cfg = self.ctx.source_cfg
        retrieved = self.ctx.now_iso()
        window = int(cfg.get("window_business_days", 3))
        markets = list(cfg.get("markets") or ["CNMS"])
        days = _weekdays_back(as_of, window + 2)  # extra days cover holidays
        files_ok = 0
        files_missing: list[str] = []
        total_rows = 0
        symbols_seen: set[str] = set()
        for market in markets:
            collected_days = 0
            for day in days:
                if collected_days >= window:
                    break
                res = self.fetch(self._file_url(market, day), expect="text",
                                 extension="txt", business_date=day,
                                 native_id="%sshvol|%s" % (market, day),
                                 allow_404=True)
                if res["not_found"]:
                    files_missing.append("%s:%s" % (market, day))
                    continue
                if not res["ok"]:
                    continue
                collected_days += 1
                files_ok += 1
                rows_ok, _rej = self._parse_file(
                    res["body"], day, market, res["raw"]["raw_object_id"],
                    retrieved, res["published_at"])
                total_rows += rows_ok
                self.note_event_time(day)
        for rec in self.records:
            if rec["ticker"]:
                symbols_seen.add(rec["ticker"])
        sample = {s.split(".")[0].upper() for s in
                  self.ctx.config.get("sources", {}).get("norgate_local", {})
                  .get("sample_symbols", [])}
        self.inventory.update({
            "files_collected": files_ok,
            "files_unavailable_dates": files_missing,
            "distinct_symbols": len(symbols_seen),
            "sample_symbols_missing": sorted(sample - symbols_seen),
            "coverage_note": "daily short-sale VOLUME files; consolidated market coverage",
        })
        self.cursor = {"last_collected_as_of": as_of,
                       "newest_file_date": self.newest_source_event_time,
                       "markets": markets}
        if files_ok >= 1 and total_rows > 0 and not self.http_errors:
            state = SH_HEALTHY
        elif total_rows > 0:
            state = SH_DEGRADED
        else:
            state = SH_FAILED
        return self.result(overall_state=state,
                           entitlement_summary="official public files, no credential required")
