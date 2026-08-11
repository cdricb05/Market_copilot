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
        hb = cfg.get("historical_backfill")
        if hb:
            # Opt-in ONE-TIME historical vintage backfill. Absent this config the
            # collector behaves EXACTLY as before (single rolling-window request).
            return self._collect_historical(as_of, key, cfg, hb, retrieved)
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

    # ------------------------------------------------------------------ #
    # Historical vintage backfill (opt-in; config key ``historical_backfill``)
    #
    # ALFRED caps a series/observations request at 2000 vintage dates. A daily-
    # updated series (e.g. DGS10) has ~250 vintage dates/year, so a single
    # 1999..today realtime request (as the rolling path issues) is rejected with
    # HTTP 400. This mode walks the realtime axis in bounded, contiguous chunks
    # (each safely under the cap), requesting ``output_type`` 1 (observations by
    # real-time period) so every row carries its true ``realtime_start``
    # (availability). Within a chunk [A, B], observations whose value was already
    # current before A are CLAMPED by ALFRED to realtime_start == A; those carry-
    # ins are dropped (kept only in the earlier chunk that actually contains their
    # release), so concatenating chunks reconstructs the genuine, un-clamped
    # vintage history exactly once per (observation, release). Chunk boundaries sit
    # on Jan-1 (a market holiday: no release lands on it) so dropping realtime_start
    # == chunk-start never discards a genuine event. Nothing here is fabricated: a
    # missing vintage stays missing, availability is ALFRED's realtime_start, and
    # today's revised values are never substituted for a historical vintage.
    # ------------------------------------------------------------------ #
    def _hist_url(self, series_id: str, key: str, obs_start: str,
                  rt_start: str, rt_end: str) -> str:
        base = self.ctx.source_cfg.get(
            "base_url", "https://api.stlouisfed.org/fred").rstrip("/")
        params = {
            "series_id": series_id, "api_key": key, "file_type": "json",
            "observation_start": obs_start,
            "realtime_start": rt_start, "realtime_end": rt_end,
        }  # output_type defaults to 1 -> realtime_start populated per row
        return "%s/series/observations?%s" % (base, urllib.parse.urlencode(params))

    @staticmethod
    def _year_chunks(obs_start: str, as_of: str, chunk_years: int) -> list:
        """Contiguous, disjoint realtime windows [A, B] (B inclusive). The first
        starts exactly at obs_start; the rest start on Jan-1 of obs_year+k*chunk."""
        y0 = int(obs_start[:4])
        starts = [obs_start]
        cur = y0 + max(1, chunk_years)
        end_year = int(as_of[:4])
        while cur <= end_year:
            starts.append("%04d-01-01" % cur)
            cur += max(1, chunk_years)
        chunks = []
        for i, s in enumerate(starts):
            if i + 1 < len(starts):
                nxt_year = int(starts[i + 1][:4])
                e = "%04d-12-31" % (nxt_year - 1)
            else:
                e = as_of
            chunks.append((s, e))
        return chunks

    @staticmethod
    def _split_chunk(a: str, b: str, min_years: int) -> Optional[list]:
        """Bisect a realtime window [a, b] on a Jan-1 boundary; None if it is
        already at the minimum granularity."""
        ya, yb = int(a[:4]), int(b[:4])
        if yb - ya < max(1, min_years):
            return None
        mid = ya + (yb - ya) // 2 + 1
        return [(a, "%04d-12-31" % (mid - 1)), ("%04d-01-01" % mid, b)]

    def _ingest_hist_observations(self, sid: str, series: dict, observations: list,
                                  chunk_start: str, is_first: bool, retrieved: str,
                                  raw_id: Optional[str]) -> int:
        n_added = 0
        for obs in observations:
            if not isinstance(obs, dict) or not obs.get("date"):
                continue
            rt_start = obs.get("realtime_start")
            if not rt_start:
                continue
            rts = str(rt_start)[:10]
            # Drop ALFRED carry-ins clamped to the chunk start (their true release
            # lives in an earlier chunk). The first chunk has no earlier data, so
            # its boundary values are genuine and kept.
            if not is_first and rts <= chunk_start:
                continue
            obs_date = str(obs["date"])[:10]
            raw_value = obs.get("value")
            missing = (raw_value is None or raw_value == ".")
            value = None if missing else raw_value
            warnings = []
            if missing:
                warnings.append("VALUE_MISSING: FRED '.' marker; value left null")
            clamped = bool(rts == chunk_start)
            if clamped:
                warnings.append("AVAILABILITY_CLAMPED_TO_WINDOW: realtime_start equals "
                                "the realtime window start; treat available_at as an "
                                "upper bound (true first release may pre-date the "
                                "owned ALFRED vintage archive)")
            payload = {
                "series_id": sid,
                "macro_family": series.get("macro_family"),
                "title": series.get("title"),
                "observation_date": obs_date,
                "value": value,
                "realtime_start": rts,
                "realtime_end": obs.get("realtime_end"),
                "realtime_request_start": chunk_start,
                "availability_clamped_to_window": clamped,
                "revision_vintage_date": rts,
                "vintage_count_for_observation": None,
                "point_in_time_vintage": True,
                "historical_backfill": True,
            }
            self.records.append(build_normalized_record(
                record_type=RT_MACRO_OBSERVATION, source_id=self.source_id,
                source_native_id="%s|%s|%s" % (sid, obs_date, rts),
                raw_object_id=raw_id, retrieved_at=retrieved, observed_at=obs_date,
                effective_at=obs_date, available_at=rts,
                event_type="MACRO_OBSERVATION", payload=payload,
                entity_mapping_confidence=EM_UNMATCHED,
                provenance="FRED/ALFRED series/observations historical vintage backfill (official, keyed)",
                quality_warnings=warnings))
            self.note_event_time(obs_date)
            n_added += 1
        return n_added

    def _collect_historical(self, as_of: str, key: str, cfg: dict, hb: dict,
                            retrieved: str) -> dict:
        obs_start = str(hb.get("observation_start", "1999-01-01"))[:10]
        chunk_years = int(hb.get("realtime_chunk_years", 5))
        min_years = int(hb.get("min_chunk_years", 1))
        max_requests = int(hb.get("max_requests_per_series", 60))
        prior_completed = set(self.cursor.get("historical_completed_chunks", []))
        completed = set(prior_completed)
        allowlist = cfg.get("series_allowlist", [])
        total = len(allowlist)
        series_ok = 0
        for series in allowlist:
            sid = series.get("series_id")
            if not sid:
                continue
            stack = list(reversed(self._year_chunks(obs_start, as_of, chunk_years)))
            got_any = False
            reqs = 0
            while stack:
                a, b = stack.pop()
                ckey = "%s|%s|%s" % (sid, a, b)
                if ckey in prior_completed:
                    got_any = True
                    continue
                if reqs >= max_requests:
                    self.record_error("HIST_MAX_REQUESTS",
                                      "%s: reached max_requests_per_series=%d" % (sid, max_requests))
                    break
                reqs += 1
                res = self.fetch(self._hist_url(sid, key, obs_start, a, b),
                                 expect="text", extension="json", business_date=as_of,
                                 native_id="hist|%s|%s|%s" % (sid, a, b),
                                 content_type="application/json", allow_400=True)
                if res.get("bad_request"):
                    try:
                        msg = json.loads((res.get("body") or b"").decode("utf-8")).get(
                            "error_message", "")
                    except (ValueError, UnicodeDecodeError, AttributeError):
                        msg = ""
                    if "exceeds the maximum number of vintage dates" in msg:
                        halves = self._split_chunk(a, b, min_years)
                        if halves:
                            for h in reversed(halves):
                                stack.append(h)
                        else:
                            self.record_error("HIST_CHUNK_TOO_DENSE",
                                              "%s [%s..%s] over ALFRED vintage cap, "
                                              "un-splittable below %dy" % (sid, a, b, min_years))
                        continue
                    if "does not exist in ALFRED" in msg:
                        # No vintages archived in this realtime window (pre-archive).
                        completed.add(ckey)
                        continue
                    self.record_error("HIST_BAD_REQUEST", "%s [%s..%s]: %s" % (sid, a, b, msg[:200]))
                    continue
                if not res["ok"]:
                    continue  # network/http error already recorded by fetch()
                try:
                    obj = json.loads(res["body"].decode("utf-8"))
                except (ValueError, UnicodeDecodeError):
                    self.record_error("PARSE_ERROR", "FRED %s hist chunk not JSON" % sid)
                    continue
                observations = obj.get("observations")
                if not isinstance(observations, list):
                    self.record_error("PARSE_ERROR", "FRED %s hist chunk missing observations" % sid)
                    continue
                raw_id = (res.get("raw") or {}).get("raw_object_id")
                n_added = self._ingest_hist_observations(
                    sid, series, observations, a, a == obs_start, retrieved, raw_id)
                got_any = got_any or n_added > 0
                completed.add(ckey)
            if got_any:
                series_ok += 1
        self.cursor = {
            "last_collected_as_of": as_of,
            "mode": "historical_backfill",
            "observation_start": obs_start,
            "series_collected": series_ok,
            "historical_completed_chunks": sorted(completed),
            "newest_observation_date": self.newest_source_event_time,
        }
        if series_ok == total and total:
            state = SH_HEALTHY
        elif series_ok:
            state = SH_DEGRADED
        else:
            state = SH_FAILED
        return self.result(overall_state=state, credential_present=True,
                           entitlement_summary="ALFRED historical vintage backfill; "
                                               "%d/%d series" % (series_ok, total))
