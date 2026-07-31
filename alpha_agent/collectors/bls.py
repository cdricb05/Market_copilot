"""
alpha_agent/collectors/bls.py — SOURCE: U.S. Bureau of Labor Statistics.

Official public BLS Public Data API v2 (api.bls.gov). The v2 single-series GET
form is used (one bounded GET per pre-registered series) so it fits the shared
GET-only transport. A registration key is OPTIONAL: if BLS_API_KEY (or
PAPER_TRADER_BLS_API_KEY) is present it is added as ``registrationkey`` (raising
the daily/series quota) and redacted from every stored fingerprint; without a key
the public tier is used. No unofficial scraping.

Point-in-time discipline: BLS data points carry the reference period
(year + Mnn month, or M13 annual) but NOT the release timestamp, so
``observed_at``/``effective_at`` are the period start and ``available_at`` is left
null with an explicit RELEASE_LAG warning (BLS releases land a known lag after the
reference period — never fabricated).
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

_DEFAULT_BASE = "https://api.bls.gov/publicAPI/v2"
_MONTH = {"M%02d" % i: i for i in range(1, 13)}


class BlsCollector(BaseCollector):
    source_id = "bls"
    requires_credential = False

    def _resolve_key(self) -> Optional[str]:
        for name in self.ctx.source_cfg.get("allowed_env_vars",
                                            ["BLS_API_KEY",
                                             "PAPER_TRADER_BLS_API_KEY"]):
            value = self.ctx.env.get(name)
            if value:
                return value
        return None

    def _url(self, series_id: str, start: str, end: str,
             key: Optional[str]) -> str:
        base = self.ctx.source_cfg.get("base_url", _DEFAULT_BASE).rstrip("/")
        params = {"startyear": start, "endyear": end}
        if key:
            params["registrationkey"] = key
        return "%s/timeseries/data/%s?%s" % (base, series_id,
                                             urllib.parse.urlencode(params))

    @staticmethod
    def _obs_date(year: str, period: str) -> Optional[str]:
        if period in _MONTH:
            return "%s-%02d-01" % (year, _MONTH[period])
        if period == "M13" or period == "A01":       # annual average
            return "%s-01-01" % year
        if period.startswith("Q"):                    # quarterly
            q = {"Q01": 1, "Q02": 4, "Q03": 7, "Q04": 10}.get(period)
            return "%s-%02d-01" % (year, q) if q else None
        return None

    # ------------------------------------------------------------------ #
    def audit(self) -> dict:
        series = (self.ctx.source_cfg.get("series_allowlist") or [{}])[0]
        sid = series.get("series_id", "CUUR0000SA0")
        end = str(self.ctx.source_cfg.get("end_year", 2026))
        res = self.fetch(self._url(sid, end, end, self._resolve_key()),
                         expect="text", archive=False, extension="json",
                         content_type="application/json")
        ok = res["ok"]
        if ok:
            try:
                ok = json.loads(res["body"].decode("utf-8")).get("status") \
                    == "REQUEST_SUCCEEDED"
            except (ValueError, UnicodeDecodeError):
                ok = False
        return self.result(overall_state=SH_HEALTHY if ok else SH_FAILED,
                           credential_present=self._resolve_key() is not None,
                           entitlement_summary="official public BLS API probe %s"
                                               % ("ok" if ok else "failed"))

    def collect(self, as_of: str) -> dict:
        cfg = self.ctx.source_cfg
        retrieved = self.ctx.now_iso()
        key = self._resolve_key()
        end = str(cfg.get("end_year", int(as_of[:4])))
        start = str(cfg.get("start_year", int(end) - 3))
        series_ok = 0
        allow = cfg.get("series_allowlist", [])
        for series in allow:
            sid = series.get("series_id")
            if not sid:
                continue
            res = self.fetch(self._url(sid, start, end, key), expect="text",
                             extension="json", business_date=as_of,
                             native_id="bls|%s|%s-%s" % (sid, start, end),
                             content_type="application/json")
            if not res["ok"]:
                continue
            try:
                obj = json.loads(res["body"].decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                self.record_error("PARSE_ERROR", "BLS %s not JSON" % sid)
                continue
            if obj.get("status") != "REQUEST_SUCCEEDED":
                self.record_error("BLS_STATUS",
                                  "BLS %s status=%s" % (sid, obj.get("status")))
                continue
            found = False
            for s in (obj.get("Results") or {}).get("series") or []:
                if s.get("seriesID") != sid:
                    continue
                for pt in s.get("data") or []:
                    obs_date = self._obs_date(str(pt.get("year")),
                                              str(pt.get("period")))
                    if not obs_date:
                        continue
                    found = True
                    payload = {
                        "series_id": sid,
                        "macro_family": series.get("macro_family"),
                        "title": series.get("title"),
                        "observation_date": obs_date,
                        "period": pt.get("period"),
                        "period_name": pt.get("periodName"),
                        "value": pt.get("value"),
                        "footnotes": [f for f in (pt.get("footnotes") or [])
                                      if f],
                    }
                    self.records.append(build_normalized_record(
                        record_type=RT_MACRO_OBSERVATION,
                        source_id=self.source_id,
                        source_native_id="%s|%s" % (sid, obs_date),
                        raw_object_id=res["raw"]["raw_object_id"],
                        retrieved_at=retrieved, observed_at=obs_date,
                        effective_at=obs_date, available_at=None,
                        event_type="MACRO_OBSERVATION", payload=payload,
                        entity_mapping_confidence=EM_UNMATCHED,
                        provenance="U.S. Bureau of Labor Statistics API v2 "
                                   "(official, public)",
                        quality_warnings=[
                            "RELEASE_LAG_UNKNOWN: BLS reference period is not the "
                            "release date; available_at left null (releases land "
                            "a known lag after the period; never back-dated)"]))
                    self.note_event_time(obs_date)
            if found:
                series_ok += 1
        self.cursor = {"last_collected_as_of": as_of,
                       "series_collected": series_ok,
                       "window": "%s-%s" % (start, end),
                       "newest_observation_date": self.newest_source_event_time}
        total = len(allow)
        if series_ok == total and total:
            state = SH_HEALTHY
        elif series_ok:
            state = SH_DEGRADED
        else:
            state = SH_FAILED
        return self.result(overall_state=state,
                           credential_present=key is not None,
                           entitlement_summary="official public BLS API; %d/%d "
                                               "series" % (series_ok, total))
