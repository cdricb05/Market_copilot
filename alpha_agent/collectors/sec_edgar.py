"""
alpha_agent/collectors/sec_edgar.py — SOURCE 3: SEC EDGAR (official public interfaces).

* Official endpoints only (www.sec.gov files + daily index).
* Compliant User-Agent: "<product> <contact-email>", with the contact resolved
  from git config user.email by the CLI and passed in via config runtime — the
  full email is NEVER printed in logs/health (masked form only).
* If no contact email can be resolved the source is marked
  BLOCKED_MISSING_USER_AGENT_CONTACT and Stage 2 continues.
* Conservative client-side rate limit (min_interval_seconds, well below SEC's
  10 req/s guidance) and a tiny bounded request count.
* Collects: the ticker/CIK identity map + a bounded recent daily-index filing
  window (metadata + official links; filing text NOT stored by default).
* The daily index provides the FILING DATE only. Acceptance and publication
  timestamps are NOT in the index and are left null with explicit quality
  warnings — never fabricated, and period-end is never substituted.
"""
from __future__ import annotations

import datetime as _dt
import json
from typing import Optional

from ..source_contracts import (
    EM_AMBIGUOUS, EM_MATCHED_EXACT, EM_UNMATCHED, RT_FILING_EVENT,
    RT_INSIDER_FILING, RT_SECURITY_IDENTITY, SH_BLOCKED_CONFIGURATION,
    SH_DEGRADED, SH_FAILED, SH_HEALTHY, build_normalized_record,
)
from .base import BaseCollector

BLOCKED_MISSING_USER_AGENT_CONTACT = "BLOCKED_MISSING_USER_AGENT_CONTACT"


def _mask_email(email: str) -> str:
    if "@" not in email:
        return "***"
    local, _, domain = email.partition("@")
    return "%s***@%s***" % (local[:1], domain[:1])


def _business_days_back(as_of: str, count: int) -> list[str]:
    """The last `count` weekdays ending at as_of (inclusive when a weekday)."""
    out: list[str] = []
    day = _dt.date.fromisoformat(as_of)
    while len(out) < count:
        if day.weekday() < 5:
            out.append(day.isoformat())
        day -= _dt.timedelta(days=1)
    return out


class SecEdgarCollector(BaseCollector):
    source_id = "sec_edgar"
    requires_credential = False

    def _contact(self) -> Optional[str]:
        return (self.ctx.config.get("_runtime", {}) or {}).get("contact_email")

    def _ua(self, contact: str) -> str:
        product = self.ctx.config.get("user_agent", {}).get(
            "product", "paper-trader-alpha-agent/2.0")
        return "%s %s" % (product, contact)

    def _blocked_no_contact(self) -> dict:
        return self.blocked_result(
            SH_BLOCKED_CONFIGURATION,
            "%s: no contact email resolvable from git config user.email; SEC "
            "requires a User-Agent with contact information"
            % BLOCKED_MISSING_USER_AGENT_CONTACT)

    # ------------------------------------------------------------------ #
    def _fetch_ticker_map(self, headers: dict, as_of: str) -> Optional[dict]:
        url = self.ctx.source_cfg.get("base_url_www", "https://www.sec.gov").rstrip("/") \
            + self.ctx.source_cfg.get("ticker_map_path", "/files/company_tickers.json")
        res = self.fetch(url, headers=headers, expect="text", extension="json",
                         business_date=as_of, native_id="company_tickers",
                         content_type="application/json")
        if not res["ok"]:
            return None
        try:
            obj = json.loads(res["body"].decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            self.record_error("PARSE_ERROR", "company_tickers.json not parseable JSON")
            return None
        count = 0
        cik_to_tickers: dict[str, list[str]] = {}
        for row in (obj.values() if isinstance(obj, dict) else []):
            if not isinstance(row, dict):
                continue
            ticker = str(row.get("ticker") or "").upper()
            cik = str(row.get("cik_str") or "")
            if not ticker or not cik:
                continue
            self.ctx.identity.register_cik(ticker, cik, "sec_company_tickers")
            cik_to_tickers.setdefault(cik.lstrip("0") or "0", []).append(ticker)
            count += 1
        self.inventory["ticker_map_entries"] = count
        return {"raw": res["raw"], "cik_to_tickers": cik_to_tickers}

    def _parse_master_idx(self, body: bytes) -> list[dict]:
        rows: list[dict] = []
        try:
            text = body.decode("latin-1")
        except Exception:  # noqa: BLE001
            return rows
        in_table = False
        for line in text.splitlines():
            if not in_table:
                if line.upper().startswith("CIK|"):
                    in_table = True
                continue
            if set(line.strip()) <= {"-"}:
                continue
            parts = line.split("|")
            if len(parts) != 5:
                continue
            cik, company, form, date_filed, filename = (p.strip() for p in parts)
            rows.append({"cik": cik, "company_name": company, "form_type": form,
                         "date_filed": date_filed, "filename": filename})
        return rows

    @staticmethod
    def _accession_from_filename(filename: str) -> str:
        tail = filename.rsplit("/", 1)[-1]
        return tail[:-4] if tail.lower().endswith(".txt") else tail

    @staticmethod
    def _iso_date(value: str) -> str:
        """Normalize the index's date form (compact YYYYMMDD or dashed) to
        dashed ISO. Unrecognized values pass through unchanged (never guessed)."""
        text = (value or "").strip()
        if len(text) == 8 and text.isdigit():
            return "%s-%s-%s" % (text[:4], text[4:6], text[6:8])
        return text

    # ------------------------------------------------------------------ #
    def audit(self) -> dict:
        contact = self._contact()
        if not contact:
            return self._blocked_no_contact()
        headers = {"User-Agent": self._ua(contact),
                   "Accept-Encoding": "identity"}
        as_of = _dt.date.today().isoformat()
        result = self._fetch_ticker_map(headers, as_of)
        self.inventory["user_agent_contact_masked"] = _mask_email(contact)
        state = SH_HEALTHY if result else SH_FAILED
        return self.result(overall_state=state,
                           entitlement_summary="official public data; UA contact present (masked %s)"
                                               % _mask_email(contact))

    def collect(self, as_of: str) -> dict:
        contact = self._contact()
        if not contact:
            return self._blocked_no_contact()
        cfg = self.ctx.source_cfg
        headers = {"User-Agent": self._ua(contact), "Accept-Encoding": "identity"}
        self.inventory["user_agent_contact_masked"] = _mask_email(contact)
        retrieved = self.ctx.now_iso()

        map_result = self._fetch_ticker_map(headers, as_of)
        cik_to_tickers: dict[str, list[str]] = (map_result or {}).get("cik_to_tickers", {})

        # Bounded identity records for the pre-registered sample only (the full
        # map lives in the archived raw object + identity resolver).
        sample_tickers = {s.split(".")[0].upper()
                          for s in self.ctx.config.get("sources", {})
                          .get("norgate_local", {}).get("sample_symbols", [])}
        sample_tickers |= {s.split(".")[0].upper()
                           for s in self.ctx.config.get("sources", {})
                           .get("eodhd", {}).get("sample_symbols", [])}
        if map_result:
            for ticker in sorted(sample_tickers):
                resolved = self.ctx.identity.resolve(ticker)
                if resolved["cik"] is None and not resolved["all_ciks"]:
                    continue
                self.records.append(build_normalized_record(
                    record_type=RT_SECURITY_IDENTITY, source_id=self.source_id,
                    source_native_id="cikmap|%s" % ticker,
                    raw_object_id=map_result["raw"]["raw_object_id"],
                    retrieved_at=retrieved, observed_at=retrieved,
                    effective_at=as_of, available_at=retrieved, ticker=ticker,
                    company_id=resolved["cik"],
                    event_type="TICKER_CIK_MAP",
                    payload={"all_ciks": resolved["all_ciks"]},
                    entity_mapping_confidence=resolved["state"],
                    provenance="SEC company_tickers.json (official)"))

        window = int(cfg.get("filing_window_business_days", 3))
        forms = set(cfg.get("forms_of_interest", []))
        cap = int(cfg.get("max_filings_per_day", 6000))
        base = cfg.get("base_url_www", "https://www.sec.gov").rstrip("/")
        days_hit = 0
        for day in _business_days_back(as_of, window):
            d = _dt.date.fromisoformat(day)
            quarter = (d.month - 1) // 3 + 1
            url = "%s/Archives/edgar/daily-index/%d/QTR%d/master.%s.idx" % (
                base, d.year, quarter, day.replace("-", ""))
            res = self.fetch(url, headers=headers, expect="text", extension="idx",
                             business_date=day, native_id="master_idx|%s" % day,
                             content_type="text/plain", allow_404=True)
            if res["not_found"]:
                self.inventory.setdefault("index_days_unavailable", []).append(day)
                continue
            if not res["ok"]:
                continue
            days_hit += 1
            rows = self._parse_master_idx(res["body"])
            self.inventory.setdefault("index_rows_by_day", {})[day] = len(rows)
            emitted = 0
            for row in rows:
                if row["form_type"] not in forms:
                    continue
                if emitted >= cap:
                    self.inventory.setdefault("index_days_capped", []).append(day)
                    break
                accession = self._accession_from_filename(row["filename"])
                date_filed = self._iso_date(row["date_filed"])
                cik_key = row["cik"].lstrip("0") or "0"
                tickers = cik_to_tickers.get(cik_key, [])
                if len(tickers) == 1:
                    ticker, mapping = tickers[0], EM_MATCHED_EXACT
                elif len(tickers) > 1:
                    ticker, mapping = None, EM_AMBIGUOUS
                else:
                    ticker, mapping = None, EM_UNMATCHED
                record_type = RT_INSIDER_FILING if row["form_type"].startswith("4") \
                    else RT_FILING_EVENT
                payload = {
                    "cik": cik_key, "company_name": row["company_name"],
                    "form_type": row["form_type"], "date_filed": date_filed,
                    "date_filed_raw": row["date_filed"],
                    "accession_number": accession,
                    "official_link": "%s/Archives/%s" % (base, row["filename"]),
                    "event_time": date_filed,
                    "publication_time": None, "acceptance_time": None,
                    "period_end": None,
                    "all_tickers_for_cik": tickers,
                }
                self.records.append(build_normalized_record(
                    record_type=record_type, source_id=self.source_id,
                    source_native_id=accession,
                    raw_object_id=res["raw"]["raw_object_id"],
                    retrieved_at=retrieved, observed_at=date_filed,
                    effective_at=date_filed, available_at=None,
                    ticker=ticker, company_id=cik_key,
                    event_type=row["form_type"], payload=payload,
                    entity_mapping_confidence=mapping,
                    provenance="SEC EDGAR daily index (official)",
                    quality_warnings=[
                        "ACCEPTANCE_TIME_UNKNOWN: daily index provides filing date "
                        "only; acceptance timestamp left null (not fabricated)",
                        "PUBLICATION_TIME_UNKNOWN: left null; filing date is "
                        "date-precision"]))
                emitted += 1
                self.note_event_time(date_filed)

        self.cursor = {"last_collected_as_of": as_of,
                       "index_window_business_days": window,
                       "newest_filing_date": self.newest_source_event_time}
        if map_result and days_hit > 0 and not self.http_errors:
            state = SH_HEALTHY
        elif self.records:
            state = SH_DEGRADED
        else:
            state = SH_FAILED
        return self.result(overall_state=state,
                           entitlement_summary="official public data; UA contact present (masked %s)"
                                               % _mask_email(contact))
