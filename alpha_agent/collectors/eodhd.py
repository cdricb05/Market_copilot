"""
alpha_agent/collectors/eodhd.py — SOURCE 2: EODHD (entitlement-audited).

Uses the EXISTING repository credential convention (environment variable
``EODHD_API_KEY`` — discovered from api/current_alpha_daily_refresh.py; no new
secret convention invented). The credential is:

  * checked for PRESENCE only before use;
  * read into memory solely to build approved API requests;
  * never printed, never logged, never stored — every persisted request
    fingerprint has the api_token query parameter redacted.

Entitlement is AUDITED per data family (never assumed — the plan may not
include news or fundamentals): ENTITLED / NOT_ENTITLED / UNKNOWN /
AUTHENTICATION_FAILED / RATE_LIMITED. Collection is a bounded production slice
of the entitled families only. No full-universe historical backfill.

The ``/user`` account-metadata probe is parsed in memory and NEVER archived:
only non-PII whitelisted fields (subscription type, request counters, limits)
are recorded.
"""
from __future__ import annotations

import datetime as _dt
import json
import urllib.parse
from typing import Any, Optional

from ..source_contracts import (
    EM_MATCHED_ALIAS, EM_MATCHED_EXACT, ENT_AUTH_FAILED, ENT_ENTITLED,
    ENT_NOT_ENTITLED, ENT_RATE_LIMITED, ENT_UNKNOWN, RT_CORPORATE_ACTION,
    RT_EARNINGS_EVENT, RT_FUNDAMENTAL_FACT, RT_INSIDER_FILING, RT_MARKET_BAR,
    RT_NEWS_EVENT, RT_SECURITY_IDENTITY, SH_BLOCKED_CREDENTIAL, SH_DEGRADED,
    SH_FAILED, SH_HEALTHY, build_normalized_record,
)
from .base import BaseCollector

_USER_PROBE_WHITELIST = ("subscriptionType", "subscriptionMode", "apiRequests",
                         "apiRequestsDate", "dailyRateLimit", "extraLimit")


def _days_before(as_of: str, days: int) -> str:
    return (_dt.date.fromisoformat(as_of) - _dt.timedelta(days=days)).isoformat()


def _base_ticker(symbol: str) -> str:
    return symbol.split(".")[0].upper()


class EodhdCollector(BaseCollector):
    source_id = "eodhd"
    requires_credential = True

    # ------------------------------------------------------------------ #
    def _resolve_key(self) -> Optional[str]:
        for name in self.ctx.source_cfg.get("allowed_env_vars", []):
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
    def _entitlement_from_status(status: Optional[int], parse_ok: bool) -> str:
        if status == 200 and parse_ok:
            return ENT_ENTITLED
        if status == 401:
            return ENT_AUTH_FAILED
        if status in (402, 403):
            return ENT_NOT_ENTITLED
        if status == 429:
            return ENT_RATE_LIMITED
        return ENT_UNKNOWN

    def _parse_json(self, body: Optional[bytes]) -> tuple[bool, Any]:
        if not body:
            return False, None
        try:
            return True, json.loads(body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return False, None

    # ------------------------------------------------------------------ #
    def _probe_user_metadata(self, key: str) -> None:
        """Account/entitlement metadata. Parsed in memory; NEVER archived (may
        contain account PII); only whitelisted non-PII fields recorded."""
        res = self.fetch(self._url("user", key), expect="text", archive=False,
                         extension="json")
        if res["ok"]:
            ok, obj = self._parse_json(res["body"])
            if ok and isinstance(obj, dict):
                self.inventory["account_metadata"] = {
                    k: obj.get(k) for k in _USER_PROBE_WHITELIST if k in obj}

    def _probe_family(self, family: str, key: str, as_of: str) -> dict:
        sym = (self.ctx.source_cfg.get("sample_symbols") or ["AAPL.US"])[0]
        urls = {
            "eod": self._url("eod/%s" % sym, key, period="d",
                             **{"from": _days_before(as_of, 10)}),
            "dividends": self._url("div/%s" % sym, key,
                                   **{"from": _days_before(as_of, 365)}),
            "splits": self._url("splits/%s" % sym, key,
                                **{"from": _days_before(as_of, 365)}),
            "earnings": self._url("calendar/earnings", key, symbols=sym,
                                  **{"from": _days_before(as_of, 7), "to": as_of}),
            "fundamentals": self._url("fundamentals/%s" % sym, key, filter="General"),
            "insider": self._url("insider-transactions", key, limit=10),
            "news": self._url("news", key, s=sym, limit=5),
        }
        res = self.fetch(urls[family], expect="text", archive=False, extension="json")
        parse_ok, _ = self._parse_json(res["body"]) if res["ok"] else (False, None)
        state = self._entitlement_from_status(res["status"], parse_ok)
        detail = res.get("rejected_reason") or res.get("error") or ("HTTP %s" % res["status"])
        return {"family": family, "state": state,
                "detail": "probe %s -> %s" % (family, detail),
                "http_status": res["status"]}

    def _run_entitlement_audit(self, key: str, as_of: str) -> dict[str, str]:
        self._probe_user_metadata(key)
        states: dict[str, str] = {}
        for family in self.ctx.source_cfg.get("entitlement_probe_families", []):
            probe = self._probe_family(family, key, as_of)
            states[family] = probe["state"]
            self.entitlements.append(probe)
        return states

    # ------------------------------------------------------------------ #
    def audit(self) -> dict:
        key = self._resolve_key()
        if key is None:
            return self.blocked_result(
                SH_BLOCKED_CREDENTIAL,
                "no EODHD credential present (checked env var NAMES %s); set the "
                "existing convention EODHD_API_KEY to enable"
                % self.ctx.source_cfg.get("allowed_env_vars"),
                credential_present=False)
        as_of = _dt.date.today().isoformat()
        states = self._run_entitlement_audit(key, as_of)
        entitled = [f for f, s in states.items() if s == ENT_ENTITLED]
        state = SH_HEALTHY if entitled else SH_FAILED
        return self.result(overall_state=state, credential_present=True,
                           entitlement_summary="entitled: %s" % ",".join(entitled))

    # ------------------------------------------------------------------ #
    def collect(self, as_of: str) -> dict:
        key = self._resolve_key()
        if key is None:
            return self.blocked_result(
                SH_BLOCKED_CREDENTIAL,
                "no EODHD credential present (checked env var NAMES %s); set the "
                "existing convention EODHD_API_KEY to enable"
                % self.ctx.source_cfg.get("allowed_env_vars"),
                credential_present=False)
        cfg = self.ctx.source_cfg
        states = self._run_entitlement_audit(key, as_of)
        retrieved = self.ctx.now_iso()

        if states.get("eod") == ENT_ENTITLED:
            self._collect_bars(key, as_of, retrieved)
        if states.get("dividends") == ENT_ENTITLED:
            self._collect_dividends(key, as_of, retrieved)
        if states.get("splits") == ENT_ENTITLED:
            self._collect_splits(key, as_of, retrieved)
        if states.get("earnings") == ENT_ENTITLED:
            self._collect_earnings(key, as_of, retrieved)
        if states.get("insider") == ENT_ENTITLED:
            self._collect_insider(key, as_of, retrieved)
        if states.get("news") == ENT_ENTITLED:
            self._collect_news(key, as_of, retrieved)
        if states.get("fundamentals") == ENT_ENTITLED:
            self._collect_fundamentals(key, as_of, retrieved)

        entitled = sorted(f for f, s in states.items() if s == ENT_ENTITLED)
        blocked = sorted(f for f, s in states.items() if s != ENT_ENTITLED)
        self.cursor = {"last_collected_as_of": as_of,
                       "eod_window_start": _days_before(as_of, int(cfg.get("recent_bar_days", 10))),
                       "entitled_families": entitled,
                       "newest_event_time": self.newest_source_event_time}
        if not entitled:
            state = SH_FAILED if ENT_AUTH_FAILED not in states.values() else SH_BLOCKED_CREDENTIAL
        elif blocked or (self.http_errors and self.records):
            state = SH_DEGRADED if self.records else SH_FAILED
        else:
            state = SH_HEALTHY
        return self.result(overall_state=state, credential_present=True,
                           entitlement_summary="entitled: %s; not-entitled/unknown: %s"
                                               % (",".join(entitled), ",".join(blocked)))

    # ------------------------------------------------------------------ #
    def _collect_bars(self, key: str, as_of: str, retrieved: str) -> None:
        frm = _days_before(as_of, int(self.ctx.source_cfg.get("recent_bar_days", 10)))
        for sym in self.ctx.source_cfg.get("sample_symbols", []):
            res = self.fetch(self._url("eod/%s" % sym, key, period="d", **{"from": frm}),
                             expect="text", extension="json", business_date=as_of,
                             native_id="eod|%s|%s" % (sym, frm),
                             content_type="application/json")
            if not res["ok"]:
                continue
            ok, rows = self._parse_json(res["body"])
            if not ok or not isinstance(rows, list):
                self.record_error("PARSE_ERROR", "eod response not a JSON list for %s" % sym)
                continue
            ticker = _base_ticker(sym)
            for row in rows:
                if not isinstance(row, dict) or not row.get("date"):
                    continue
                bar_date = str(row["date"])[:10]
                self.records.append(build_normalized_record(
                    record_type=RT_MARKET_BAR, source_id=self.source_id,
                    source_native_id="%s|%s" % (sym, bar_date),
                    raw_object_id=res["raw"]["raw_object_id"],
                    retrieved_at=retrieved, observed_at=bar_date,
                    effective_at=bar_date, available_at=None, ticker=ticker,
                    exchange=sym.split(".")[-1] if "." in sym else None,
                    event_type="EOD_BAR", payload=dict(row),
                    entity_mapping_confidence=EM_MATCHED_EXACT,
                    provenance="EODHD /eod (subscriber-entitled)"))
                self.note_event_time(bar_date)

    def _collect_dividends(self, key: str, as_of: str, retrieved: str) -> None:
        frm = _days_before(as_of, int(self.ctx.source_cfg.get("corporate_actions_window_days", 30)))
        for sym in self.ctx.source_cfg.get("sample_symbols", []):
            res = self.fetch(self._url("div/%s" % sym, key, **{"from": frm}),
                             expect="text", extension="json", business_date=as_of,
                             native_id="div|%s|%s" % (sym, frm),
                             content_type="application/json")
            if not res["ok"]:
                continue
            ok, rows = self._parse_json(res["body"])
            if not ok or not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict) or not row.get("date"):
                    continue
                ex_date = str(row["date"])[:10]
                declaration = row.get("declarationDate")
                payload = dict(row)
                payload["event_time"] = ex_date
                payload["publication_time"] = declaration
                warnings = [] if declaration else [
                    "PUBLICATION_TIME_UNKNOWN: no declarationDate given; availability left null"]
                self.records.append(build_normalized_record(
                    record_type=RT_CORPORATE_ACTION, source_id=self.source_id,
                    source_native_id="div|%s|%s" % (sym, ex_date),
                    raw_object_id=res["raw"]["raw_object_id"],
                    retrieved_at=retrieved, observed_at=ex_date,
                    effective_at=ex_date,
                    available_at=str(declaration)[:10] if declaration else None,
                    ticker=_base_ticker(sym), event_type="DIVIDEND",
                    payload=payload, entity_mapping_confidence=EM_MATCHED_EXACT,
                    provenance="EODHD /div", quality_warnings=warnings))
                self.note_event_time(ex_date)

    def _collect_splits(self, key: str, as_of: str, retrieved: str) -> None:
        frm = _days_before(as_of, int(self.ctx.source_cfg.get("corporate_actions_window_days", 30)))
        for sym in self.ctx.source_cfg.get("sample_symbols", []):
            res = self.fetch(self._url("splits/%s" % sym, key, **{"from": frm}),
                             expect="text", extension="json", business_date=as_of,
                             native_id="splits|%s|%s" % (sym, frm),
                             content_type="application/json")
            if not res["ok"]:
                continue
            ok, rows = self._parse_json(res["body"])
            if not ok or not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict) or not row.get("date"):
                    continue
                s_date = str(row["date"])[:10]
                self.records.append(build_normalized_record(
                    record_type=RT_CORPORATE_ACTION, source_id=self.source_id,
                    source_native_id="split|%s|%s" % (sym, s_date),
                    raw_object_id=res["raw"]["raw_object_id"],
                    retrieved_at=retrieved, observed_at=s_date,
                    effective_at=s_date, available_at=None,
                    ticker=_base_ticker(sym), event_type="SPLIT",
                    payload=dict(row), entity_mapping_confidence=EM_MATCHED_EXACT,
                    provenance="EODHD /splits"))
                self.note_event_time(s_date)

    def _collect_earnings(self, key: str, as_of: str, retrieved: str) -> None:
        frm = _days_before(as_of, int(self.ctx.source_cfg.get("earnings_window_days", 7)))
        symbols = ",".join(self.ctx.source_cfg.get("sample_symbols", []))
        res = self.fetch(self._url("calendar/earnings", key, symbols=symbols,
                                   **{"from": frm, "to": as_of}),
                         expect="text", extension="json", business_date=as_of,
                         native_id="earnings|%s|%s" % (frm, as_of),
                         content_type="application/json")
        if not res["ok"]:
            return
        ok, obj = self._parse_json(res["body"])
        rows = obj.get("earnings") if ok and isinstance(obj, dict) else (
            obj if ok and isinstance(obj, list) else None)
        if rows is None:
            self.record_error("PARSE_ERROR", "earnings calendar payload not parseable")
            return
        for row in rows:
            if not isinstance(row, dict) or not row.get("code"):
                continue
            report_date = str(row.get("report_date") or "")[:10] or None
            period_end = str(row.get("date") or "")[:10] or None
            payload = dict(row)
            payload["period_end"] = period_end
            payload["event_time"] = report_date
            payload["publication_time"] = report_date
            payload["publication_time_precision"] = "date (before/after market flag: %s)" % row.get("before_after_market")
            self.records.append(build_normalized_record(
                record_type=RT_EARNINGS_EVENT, source_id=self.source_id,
                source_native_id="earnings|%s|%s" % (row["code"], report_date or period_end),
                raw_object_id=res["raw"]["raw_object_id"], retrieved_at=retrieved,
                observed_at=report_date, effective_at=report_date,
                available_at=None, ticker=_base_ticker(str(row["code"])),
                event_type="EARNINGS_REPORT", payload=payload,
                entity_mapping_confidence=EM_MATCHED_EXACT,
                provenance="EODHD /calendar/earnings",
                quality_warnings=["PUBLICATION_TIME_OF_DAY_UNKNOWN: report_date is "
                                  "date-precision; availability left null (period_end "
                                  "NEVER substituted for publication)"]))
            if report_date:
                self.note_event_time(report_date)

    def _collect_insider(self, key: str, as_of: str, retrieved: str) -> None:
        res = self.fetch(self._url("insider-transactions", key,
                                   limit=int(self.ctx.source_cfg.get("insider_limit", 100))),
                         expect="text", extension="json", business_date=as_of,
                         native_id="insider|%s" % as_of,
                         content_type="application/json")
        if not res["ok"]:
            return
        ok, rows = self._parse_json(res["body"])
        if not ok or not isinstance(rows, list):
            return
        for row in rows:
            if not isinstance(row, dict) or not row.get("code"):
                continue
            filing_date = str(row.get("date") or "")[:10] or None
            txn_date = str(row.get("transactionDate") or "")[:10] or None
            payload = dict(row)
            payload["event_time"] = txn_date
            payload["publication_time"] = filing_date
            self.records.append(build_normalized_record(
                record_type=RT_INSIDER_FILING, source_id=self.source_id,
                source_native_id="insider|%s|%s|%s" % (row["code"], filing_date,
                                                       row.get("ownerName", "")),
                raw_object_id=res["raw"]["raw_object_id"], retrieved_at=retrieved,
                observed_at=txn_date, effective_at=txn_date or filing_date,
                available_at=filing_date, ticker=_base_ticker(str(row["code"])),
                event_type=str(row.get("transactionCode") or "INSIDER_TXN"),
                payload=payload, entity_mapping_confidence=EM_MATCHED_ALIAS,
                provenance="EODHD /insider-transactions",
                quality_warnings=["AVAILABILITY_DATE_PRECISION: filing date is "
                                  "date-precision (no acceptance time)"]))
            if filing_date:
                self.note_event_time(filing_date)

    def _collect_news(self, key: str, as_of: str, retrieved: str) -> None:
        frm = _days_before(as_of, int(self.ctx.source_cfg.get("news_window_days", 3)))
        limit = int(self.ctx.source_cfg.get("news_limit_per_symbol", 10))
        for sym in self.ctx.source_cfg.get("sample_symbols", []):
            res = self.fetch(self._url("news", key, s=sym, limit=limit,
                                       **{"from": frm, "to": as_of}),
                             expect="text", extension="json", business_date=as_of,
                             native_id="news|%s|%s" % (sym, as_of),
                             content_type="application/json")
            if not res["ok"]:
                continue
            ok, rows = self._parse_json(res["body"])
            if not ok or not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict) or not row.get("date"):
                    continue
                pub_ts = str(row["date"])
                content = str(row.get("content") or "")
                payload = {
                    "title": str(row.get("title") or "")[:500],
                    "link": row.get("link"),
                    "symbols": row.get("symbols"),
                    "tags": row.get("tags"),
                    "sentiment": row.get("sentiment"),
                    "publication_time": pub_ts,
                    "content_snippet": content[:500],
                    "content_truncated": len(content) > 500,
                }
                self.records.append(build_normalized_record(
                    record_type=RT_NEWS_EVENT, source_id=self.source_id,
                    source_native_id="news|%s" % (row.get("link") or pub_ts),
                    raw_object_id=res["raw"]["raw_object_id"],
                    retrieved_at=retrieved, observed_at=pub_ts,
                    effective_at=pub_ts[:10], available_at=pub_ts,
                    ticker=_base_ticker(sym), event_type="NEWS",
                    payload=payload, entity_mapping_confidence=EM_MATCHED_ALIAS,
                    provenance="EODHD /news (symbol-tagged feed)"))
                self.note_event_time(pub_ts)

    def _collect_fundamentals(self, key: str, as_of: str, retrieved: str) -> None:
        for sym in self.ctx.source_cfg.get("fundamentals_sample_symbols", []):
            res = self.fetch(self._url("fundamentals/%s" % sym, key,
                                       filter="General,Highlights"),
                             expect="text", extension="json", business_date=as_of,
                             native_id="fundamentals|%s" % sym,
                             content_type="application/json")
            if not res["ok"]:
                continue
            ok, obj = self._parse_json(res["body"])
            if not ok or not isinstance(obj, dict):
                continue
            general = obj.get("General") if isinstance(obj.get("General"), dict) else {}
            highlights = obj.get("Highlights") if isinstance(obj.get("Highlights"), dict) else {}
            ticker = _base_ticker(sym)
            cik = general.get("CIK")
            if cik:
                self.ctx.identity.register_cik(ticker, str(cik), "eodhd_fundamentals")
            if general:
                self.records.append(build_normalized_record(
                    record_type=RT_SECURITY_IDENTITY, source_id=self.source_id,
                    source_native_id="identity|%s" % sym,
                    raw_object_id=res["raw"]["raw_object_id"],
                    retrieved_at=retrieved, observed_at=retrieved,
                    effective_at=as_of, available_at=retrieved, ticker=ticker,
                    company_id=str(cik) if cik else None,
                    exchange=general.get("Exchange"),
                    event_type="IDENTITY_SNAPSHOT",
                    payload={k: general.get(k) for k in
                             ("Code", "Name", "Exchange", "CurrencyCode", "CIK",
                              "ISIN", "CUSIP", "GicSector", "GicIndustry",
                              "Sector", "Industry", "IPODate") if k in general},
                    entity_mapping_confidence=EM_MATCHED_EXACT,
                    provenance="EODHD /fundamentals General"))
            if highlights:
                period_end = highlights.get("MostRecentQuarter")
                self.records.append(build_normalized_record(
                    record_type=RT_FUNDAMENTAL_FACT, source_id=self.source_id,
                    source_native_id="highlights|%s|%s" % (sym, period_end),
                    raw_object_id=res["raw"]["raw_object_id"],
                    retrieved_at=retrieved, observed_at=retrieved,
                    effective_at=str(period_end)[:10] if period_end else None,
                    available_at=None, ticker=ticker,
                    company_id=str(cik) if cik else None,
                    event_type="FUNDAMENTAL_HIGHLIGHTS",
                    payload=dict(highlights, period_end=period_end),
                    entity_mapping_confidence=EM_MATCHED_EXACT,
                    provenance="EODHD /fundamentals Highlights",
                    quality_warnings=["AVAILABILITY_UNKNOWN: vendor snapshot values; "
                                      "original publication time not stated; period_end "
                                      "NOT used as availability"]))
