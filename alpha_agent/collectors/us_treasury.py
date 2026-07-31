"""
alpha_agent/collectors/us_treasury.py — SOURCE: U.S. Treasury Fiscal Data.

Official public U.S. Treasury "Fiscal Data" API (api.fiscaldata.treasury.gov).
NO credential required. Collects a bounded, pre-registered allowlist of series
(e.g. average interest rates on marketable Treasury securities) as macro
observations.

Point-in-time discipline: the API exposes ``record_date`` (the data period), not
an official release timestamp. ``observed_at``/``effective_at`` are set to
``record_date``; ``available_at`` is left null with an explicit RELEASE_LAG
warning (the value is published a short, series-specific lag AFTER record_date —
never fabricated, never back-dated to look available before it was). Deterministic
and bounded: at most ``max_pages`` pages per endpoint, newest first.
"""
from __future__ import annotations

import json
import urllib.parse
from typing import Optional

from ..source_contracts import (
    EM_UNMATCHED, RT_MACRO_OBSERVATION, SH_DEGRADED, SH_FAILED, SH_HEALTHY,
    build_normalized_record,
)
from .base import BaseCollector

_DEFAULT_BASE = ("https://api.fiscaldata.treasury.gov/services/api/"
                 "fiscal_service")


class UsTreasuryCollector(BaseCollector):
    source_id = "us_treasury"
    requires_credential = False

    def _endpoints(self) -> list:
        return self.ctx.source_cfg.get("endpoints") or [{
            "path": "/v2/accounting/od/avg_interest_rates",
            "macro_family": "treasury_rates",
            "date_field": "record_date",
            "value_field": "avg_interest_rate_amt",
            "series_fields": ["security_type_desc", "security_desc"],
        }]

    def _url(self, path: str, page_size: int, sort_field: str,
             page: int = 1) -> str:
        base = self.ctx.source_cfg.get("base_url", _DEFAULT_BASE).rstrip("/")
        params = {"sort": "-%s" % sort_field, "page[size]": str(page_size),
                  "page[number]": str(page)}
        return "%s%s?%s" % (base, path, urllib.parse.urlencode(params, safe="[]"))

    # ------------------------------------------------------------------ #
    def audit(self) -> dict:
        ep = self._endpoints()[0]
        res = self.fetch(self._url(ep["path"], 1, ep.get("date_field",
                                                         "record_date")),
                         expect="text", archive=False, extension="json",
                         content_type="application/json")
        state = SH_HEALTHY if res["ok"] else SH_FAILED
        return self.result(overall_state=state, credential_present=None,
                           entitlement_summary="official public Treasury API "
                                               "probe %s" % ("ok" if res["ok"]
                                                             else "failed"))

    def collect(self, as_of: str) -> dict:
        cfg = self.ctx.source_cfg
        retrieved = self.ctx.now_iso()
        page_size = int(cfg.get("page_size", 100))
        max_pages = int(cfg.get("max_pages", 1))
        endpoints_ok = 0
        endpoints = self._endpoints()
        for ep in endpoints:
            date_field = ep.get("date_field", "record_date")
            value_field = ep.get("value_field", "avg_interest_rate_amt")
            series_fields = ep.get("series_fields") or []
            fam = ep.get("macro_family", "treasury")
            got_any = False
            for page in range(1, max_pages + 1):
                res = self.fetch(
                    self._url(ep["path"], page_size, date_field, page),
                    expect="text", extension="json", business_date=as_of,
                    native_id="treasury|%s|p%d" % (ep["path"], page),
                    content_type="application/json")
                if not res["ok"]:
                    break
                try:
                    obj = json.loads(res["body"].decode("utf-8"))
                except (ValueError, UnicodeDecodeError):
                    self.record_error("PARSE_ERROR",
                                      "treasury %s not JSON" % ep["path"])
                    break
                rows = obj.get("data") if isinstance(obj, dict) else None
                if not isinstance(rows, list) or not rows:
                    break
                got_any = True
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    obs_date = str(row.get(date_field) or "")[:10]
                    if not obs_date:
                        continue
                    series_key = "|".join(str(row.get(f) or "")
                                          for f in series_fields) or ep["path"]
                    raw_val = row.get(value_field)
                    payload = {
                        "endpoint": ep["path"], "macro_family": fam,
                        "series_key": series_key,
                        "observation_date": obs_date, "value": raw_val,
                        "value_field": value_field,
                        "series_labels": {f: row.get(f) for f in series_fields},
                    }
                    self.records.append(build_normalized_record(
                        record_type=RT_MACRO_OBSERVATION,
                        source_id=self.source_id,
                        source_native_id="%s|%s|%s" % (ep["path"], series_key,
                                                       obs_date),
                        raw_object_id=res["raw"]["raw_object_id"],
                        retrieved_at=retrieved, observed_at=obs_date,
                        effective_at=obs_date, available_at=None,
                        event_type="MACRO_OBSERVATION", payload=payload,
                        entity_mapping_confidence=EM_UNMATCHED,
                        provenance="U.S. Treasury Fiscal Data API (official, "
                                   "public)",
                        quality_warnings=[
                            "RELEASE_LAG_UNKNOWN: record_date is the data period; "
                            "the official release occurs a short series-specific "
                            "lag AFTER it. available_at left null (never "
                            "back-dated before actual publication)"]))
                    self.note_event_time(obs_date)
                if len(rows) < page_size:
                    break
            if got_any:
                endpoints_ok += 1
        self.cursor = {"last_collected_as_of": as_of,
                       "endpoints_collected": endpoints_ok,
                       "newest_observation_date": self.newest_source_event_time}
        total = len(endpoints)
        if endpoints_ok == total and total:
            state = SH_HEALTHY
        elif endpoints_ok:
            state = SH_DEGRADED
        else:
            state = SH_FAILED
        return self.result(overall_state=state, credential_present=None,
                           entitlement_summary="official public Treasury API; "
                                               "%d/%d endpoints" % (endpoints_ok,
                                                                    total))
