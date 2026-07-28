"""
alpha_agent/collectors/fred_alfred.py — SOURCE 6: FRED / ALFRED macro series.

Uses the EXISTING repository FRED conventions (api.stlouisfed.org observations
endpoint, credential via FRED_API_KEY / PAPER_TRADER_FRED_API_KEY — both
conventions already exist in this repository; no new secret convention
invented). The credential is checked for PRESENCE, read into memory only to
build the approved API request, and never printed or persisted — the api_key
query parameter is redacted from every stored fingerprint.

Collects ONLY the pre-registered macro series allowlist (rates, yield curve,
credit, inflation, labor, liquidity/financial conditions). With
``use_alfred_vintages`` the full realtime range is requested so every
observation carries its true ALFRED vintage (realtime_start = availability
date); observation date, release/vintage date and retrieval time are kept
strictly separate. Series where vintages are unavailable are marked
revised-only. No scraping of unofficial sites — if no credential is present the
source is BLOCKED_CREDENTIAL and Stage 2 continues (this repository's existing
official FRED path is the keyed API itself).
"""
from __future__ import annotations

import datetime as _dt
import json
import urllib.parse
from typing import Optional

from ..source_contracts import (
    EM_UNMATCHED, RT_MACRO_OBSERVATION, SH_BLOCKED_CREDENTIAL, SH_DEGRADED,
    SH_FAILED, SH_HEALTHY, build_normalized_record,
)
from .base import BaseCollector


class FredAlfredCollector(BaseCollector):
    source_id = "fred_alfred"
    requires_credential = True

    def _resolve_key(self) -> Optional[str]:
        for name in self.ctx.source_cfg.get("allowed_env_vars", []):
            value = self.ctx.env.get(name)
            if value:
                return value
        return None

    def _observations_url(self, series_id: str, key: str, start: str,
                          vintages: bool) -> str:
        base = self.ctx.source_cfg.get(
            "base_url", "https://api.stlouisfed.org/fred").rstrip("/")
        params = {
            "series_id": series_id, "api_key": key, "file_type": "json",
            "observation_start": start,
        }
        if vintages:
            # ALFRED caps a request at 2000 vintage dates, so the realtime
            # window is bounded to the observation window instead of the full
            # 1776..9999 range. Vintages inside the window remain exact; an
            # observation whose first release pre-dates the window is CLAMPED
            # by ALFRED to the window start and flagged per record.
            params["realtime_start"] = start
            params["realtime_end"] = "9999-12-31"
        return "%s/series/observations?%s" % (base, urllib.parse.urlencode(params))

    def _blocked(self) -> dict:
        return self.blocked_result(
            SH_BLOCKED_CREDENTIAL,
            "no FRED credential present (checked env var NAMES %s); set FRED_API_KEY "
            "to enable; unofficial scraping is prohibited"
            % self.ctx.source_cfg.get("allowed_env_vars"),
            credential_present=False)

    # ------------------------------------------------------------------ #
    def audit(self) -> dict:
        key = self._resolve_key()
        if key is None:
            return self._blocked()
        series = (self.ctx.source_cfg.get("series_allowlist") or [{}])[0]
        sid = series.get("series_id", "DGS10")
        start = (_dt.date.today() - _dt.timedelta(days=14)).isoformat()
        res = self.fetch(self._observations_url(sid, key, start, False),
                         expect="text", archive=False, extension="json")
        state = SH_HEALTHY if res["ok"] else SH_FAILED
        return self.result(overall_state=state, credential_present=True,
                           entitlement_summary="keyed official API probe %s"
                                               % ("ok" if res["ok"] else "failed"))

    def collect(self, as_of: str) -> dict:
        key = self._resolve_key()
        if key is None:
            return self._blocked()
        cfg = self.ctx.source_cfg
        retrieved = self.ctx.now_iso()
        window = int(cfg.get("observation_window_days", 45))
        start = (_dt.date.fromisoformat(as_of) - _dt.timedelta(days=window)).isoformat()
        vintages = bool(cfg.get("use_alfred_vintages", True))
        series_ok = 0
        for series in cfg.get("series_allowlist", []):
            sid = series.get("series_id")
            if not sid:
                continue
            res = self.fetch(self._observations_url(sid, key, start, vintages),
                             expect="text", extension="json", business_date=as_of,
                             native_id="observations|%s|%s" % (sid, start),
                             content_type="application/json")
            if not res["ok"]:
                continue
            try:
                obj = json.loads(res["body"].decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                self.record_error("PARSE_ERROR", "FRED %s response not JSON" % sid)
                continue
            observations = obj.get("observations")
            if not isinstance(observations, list):
                self.record_error("PARSE_ERROR", "FRED %s missing observations" % sid)
                continue
            series_ok += 1
            vintage_dates_by_obs: dict[str, int] = {}
            for obs in observations:
                if isinstance(obs, dict) and obs.get("date"):
                    vintage_dates_by_obs[obs["date"]] = \
                        vintage_dates_by_obs.get(obs["date"], 0) + 1
            for obs in observations:
                if not isinstance(obs, dict) or not obs.get("date"):
                    continue
                obs_date = str(obs["date"])[:10]
                realtime_start = obs.get("realtime_start")
                realtime_end = obs.get("realtime_end")
                raw_value = obs.get("value")
                missing = (raw_value is None or raw_value == ".")
                value = None if missing else raw_value
                point_in_time = bool(vintages and realtime_start)
                warnings = []
                if missing:
                    warnings.append("VALUE_MISSING: FRED '.' marker; value left null")
                if not point_in_time:
                    warnings.append("REVISED_ONLY_SERIES: no vintage information "
                                    "requested/available; availability left null")
                clamped = bool(point_in_time and str(realtime_start) == start)
                if clamped:
                    warnings.append("AVAILABILITY_CLAMPED_TO_REQUEST_WINDOW: this "
                                    "vintage may pre-date the requested realtime "
                                    "window start; treat available_at as an upper "
                                    "bound, not the first release")
                payload = {
                    "series_id": sid,
                    "macro_family": series.get("macro_family"),
                    "title": series.get("title"),
                    "observation_date": obs_date,
                    "value": value,
                    "realtime_start": realtime_start,
                    "realtime_end": realtime_end,
                    "realtime_request_start": start if vintages else None,
                    "availability_clamped_to_window": clamped,
                    "revision_vintage_date": realtime_start if point_in_time else None,
                    "vintage_count_for_observation": vintage_dates_by_obs.get(obs_date, 1),
                    "point_in_time_vintage": point_in_time,
                }
                self.records.append(build_normalized_record(
                    record_type=RT_MACRO_OBSERVATION, source_id=self.source_id,
                    source_native_id="%s|%s|%s" % (sid, obs_date,
                                                   realtime_start or "latest"),
                    raw_object_id=res["raw"]["raw_object_id"],
                    retrieved_at=retrieved, observed_at=obs_date,
                    effective_at=obs_date,
                    available_at=(str(realtime_start)[:10]
                                  if point_in_time and realtime_start else None),
                    event_type="MACRO_OBSERVATION", payload=payload,
                    entity_mapping_confidence=EM_UNMATCHED,
                    provenance="FRED/ALFRED series/observations (official, keyed)",
                    quality_warnings=warnings))
                self.note_event_time(obs_date)
        total = len(cfg.get("series_allowlist", []))
        self.cursor = {"last_collected_as_of": as_of,
                       "observation_window_start": start,
                       "series_collected": series_ok,
                       "newest_observation_date": self.newest_source_event_time}
        if series_ok == total and total:
            state = SH_HEALTHY
        elif series_ok:
            state = SH_DEGRADED
        else:
            state = SH_FAILED
        return self.result(overall_state=state, credential_present=True,
                           entitlement_summary="keyed official API; %d/%d allowlisted series"
                                               % (series_ok, total))
