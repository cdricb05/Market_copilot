"""
alpha_agent/collectors/nasdaq_trader.py — SOURCE 5: Nasdaq Trader PUBLIC data.

Official public files only (this is nasdaqtrader.com public data, NOT the paid
Nasdaq Data Link product):

  * symbol directories (nasdaqlisted.txt / otherlisted.txt) — symbol + exchange
    metadata, ETF flags, test-issue flags, file-creation publication timestamp;
  * the trading-halts RSS feed — halt publication, reason codes and resumption
    timestamps preserved distinctly.

Ticker/exchange mappings are registered into the shared identity resolver.
"""
from __future__ import annotations

import datetime as _dt
import xml.etree.ElementTree as ET
from typing import Optional

from ..source_contracts import (
    EM_MATCHED_EXACT, RT_SECURITY_IDENTITY, RT_TRADING_HALT, SH_DEGRADED,
    SH_FAILED, SH_HEALTHY, build_normalized_record,
)
from .base import BaseCollector, _parse_http_datetime

_OTHER_EXCHANGE_CODES = {
    "A": "NYSE American", "N": "NYSE", "P": "NYSE Arca", "Z": "Cboe BZX",
    "V": "IEX",
}


def _parse_file_creation_time(line: str) -> Optional[str]:
    """Footer format: 'File Creation Time: MMDDYYYYHH:MM'. Returns ISO or None."""
    try:
        stamp = line.split(":", 1)[1].strip()
        compact = stamp.replace(":", "")
        if len(compact) < 12:
            return None
        month, day, year = int(compact[0:2]), int(compact[2:4]), int(compact[4:8])
        hour, minute = int(compact[8:10]), int(compact[10:12])
        return _dt.datetime(year, month, day, hour, minute).isoformat()
    except Exception:  # noqa: BLE001 — never fabricate; absent on failure
        return None


def _strip_ns(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


class NasdaqTraderCollector(BaseCollector):
    source_id = "nasdaq_trader"
    requires_credential = False

    # ------------------------------------------------------------------ #
    def _parse_symbol_directory(self, name: str, body: bytes, raw_id: str,
                                retrieved: str, as_of: str) -> int:
        try:
            text = body.decode("utf-8", errors="replace")
        except Exception:  # noqa: BLE001
            self.record_error("PARSE_ERROR", "undecodable symbol directory %s" % name)
            return 0
        lines = [ln for ln in text.splitlines() if ln.strip()]
        if not lines:
            return 0
        creation_iso: Optional[str] = None
        if lines and lines[-1].startswith("File Creation Time"):
            creation_iso = _parse_file_creation_time(lines[-1])
            lines = lines[:-1]
        header = [h.strip() for h in lines[0].split("|")]
        emitted = 0
        test_issues = 0
        for line in lines[1:]:
            parts = line.split("|")
            if len(parts) != len(header):
                continue
            row = dict(zip(header, (p.strip() for p in parts)))
            symbol = row.get("Symbol") or row.get("ACT Symbol") or row.get("NASDAQ Symbol")
            if not symbol:
                continue
            if (row.get("Test Issue") or "").upper() == "Y":
                test_issues += 1
                continue
            if name == "nasdaqlisted":
                exchange = "NASDAQ"
            else:
                code = row.get("Exchange", "")
                exchange = _OTHER_EXCHANGE_CODES.get(code, "OTHER(%s)" % code)
            self.ctx.identity.register_exchange(symbol, exchange, "nasdaq_trader")
            payload = dict(row)
            payload["directory_file"] = name
            payload["exchange_resolved"] = exchange
            payload["file_creation_time"] = creation_iso
            self.records.append(build_normalized_record(
                record_type=RT_SECURITY_IDENTITY, source_id=self.source_id,
                source_native_id="%s|%s" % (name, symbol),
                raw_object_id=raw_id, retrieved_at=retrieved,
                observed_at=creation_iso, effective_at=(creation_iso or "")[:10] or as_of,
                available_at=creation_iso, ticker=symbol, exchange=exchange,
                event_type="SYMBOL_DIRECTORY_ENTRY", payload=payload,
                entity_mapping_confidence=EM_MATCHED_EXACT,
                provenance="Nasdaq Trader public symbol directory (%s)" % name,
                quality_warnings=(
                    [] if creation_iso else
                    ["FILE_CREATION_TIME_UNPARSED: publication timestamp left null"])))
            emitted += 1
        self.inventory.setdefault("symbol_directories", {})[name] = {
            "rows_emitted": emitted, "test_issues_excluded": test_issues,
            "file_creation_time": creation_iso}
        if creation_iso:
            self.note_event_time(creation_iso)
        return emitted

    def _parse_halts_rss(self, body: bytes, raw_id: str, retrieved: str) -> int:
        try:
            root = ET.fromstring(body.decode("utf-8", errors="replace"))
        except ET.ParseError as exc:
            self.record_error("PARSE_ERROR", "halts RSS not parseable XML: %s" % exc)
            return 0
        emitted = 0
        for item in root.iter():
            if _strip_ns(item.tag) != "item":
                continue
            fields: dict[str, str] = {}
            for child in item:
                fields[_strip_ns(child.tag)] = (child.text or "").strip()
            symbol = fields.get("IssueSymbol") or fields.get("issueSymbol")
            if not symbol:
                continue
            pub_iso = _parse_http_datetime(fields.get("pubDate"))
            halt_date = fields.get("HaltDate") or ""
            halt_time = fields.get("HaltTime") or ""
            effective = None
            try:
                m, d, y = halt_date.split("/")
                effective = "%s-%s-%s" % (y, m.zfill(2), d.zfill(2))
            except Exception:  # noqa: BLE001
                effective = (pub_iso or "")[:10] or None
            resolved = self.ctx.identity.resolve(symbol)
            payload = {
                "issue_symbol": symbol,
                "issue_name": fields.get("IssueName"),
                "market": fields.get("Market"),
                "reason_code": fields.get("ReasonCode"),
                "halt_date": halt_date, "halt_time": halt_time,
                "resumption_date": fields.get("ResumptionDate"),
                "resumption_quote_time": fields.get("ResumptionQuoteTime"),
                "resumption_trade_time": fields.get("ResumptionTradeTime"),
                "publication_time": pub_iso,
                "event_time": "%s %s" % (halt_date, halt_time) if halt_date else None,
            }
            self.records.append(build_normalized_record(
                record_type=RT_TRADING_HALT, source_id=self.source_id,
                source_native_id="halt|%s|%s|%s" % (symbol, halt_date, halt_time),
                raw_object_id=raw_id, retrieved_at=retrieved,
                observed_at=pub_iso, effective_at=effective,
                available_at=pub_iso, ticker=symbol,
                exchange=fields.get("Market"),
                company_id=resolved["cik"],
                event_type="TRADING_HALT:%s" % (fields.get("ReasonCode") or "UNKNOWN"),
                payload=payload,
                entity_mapping_confidence=(resolved["state"]
                                           if resolved["cik"] else EM_MATCHED_EXACT),
                provenance="Nasdaq Trader public trading-halts RSS feed"))
            if pub_iso:
                self.note_event_time(pub_iso)
            emitted += 1
        self.inventory["halt_items"] = emitted
        return emitted

    # ------------------------------------------------------------------ #
    def audit(self) -> dict:
        urls = self.ctx.source_cfg.get("symbol_directory_urls", {})
        url = urls.get("nasdaqlisted")
        ok = False
        if url:
            res = self.fetch(url, expect="text", archive=False)
            ok = res["ok"]
        state = SH_HEALTHY if ok else SH_FAILED
        return self.result(overall_state=state,
                           entitlement_summary="official public files, no credential required")

    def collect(self, as_of: str) -> dict:
        cfg = self.ctx.source_cfg
        retrieved = self.ctx.now_iso()
        directories_ok = 0
        for name, url in sorted(cfg.get("symbol_directory_urls", {}).items()):
            res = self.fetch(url, expect="text", extension="txt",
                             business_date=as_of, native_id="symdir|%s" % name)
            if not res["ok"]:
                continue
            if self._parse_symbol_directory(name, res["body"],
                                            res["raw"]["raw_object_id"],
                                            retrieved, as_of) > 0:
                directories_ok += 1
        halts_ok = False
        halts_url = cfg.get("halts_feed_url")
        if halts_url:
            res = self.fetch(halts_url, expect="xml", extension="xml",
                             business_date=as_of, native_id="tradehalts_rss",
                             content_type="application/rss+xml")
            if res["ok"]:
                self._parse_halts_rss(res["body"], res["raw"]["raw_object_id"],
                                      retrieved)
                halts_ok = True
        self.cursor = {"last_collected_as_of": as_of,
                       "newest_event_time": self.newest_source_event_time}
        if directories_ok == 2 and halts_ok:
            state = SH_HEALTHY
        elif directories_ok or halts_ok:
            state = SH_DEGRADED
        else:
            state = SH_FAILED
        return self.result(overall_state=state,
                           entitlement_summary="official public files, no credential required")
