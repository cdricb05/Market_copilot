"""
alpha_agent/collectors/intrinio.py — SOURCE: Intrinio (TRIAL, research-only).

Credential-gated Stage-2 collector for the operator-approved Intrinio trial
(US Fundamentals + Zacks current-data). It reuses the EXISTING collector
machinery (``BaseCollector.fetch`` — rate limit, bounded retries, circuit
breaker, response hygiene, content-addressed immutable RawArchive, redacted
fingerprints) and the existing credential conventions:

  * PRIMARY: an environment-variable NAME from ``allowed_env_vars``
    (``INTRINIO_API_KEY`` / ``PAPER_TRADER_INTRINIO_API_KEY``) — the same
    presence-only convention EODHD uses, so the ingestion redaction layer scrubs
    it automatically.
  * FALLBACK: the DPAPI-protected credential file written by
    ``scripts/configure_alpha_agent_intrinio.ps1`` (reusing ``bea.dpapi_unprotect``
    — no new secret mechanism). The decrypted key lives only in memory and is
    registered as a redaction secret so it can never reach a persisted
    fingerprint or error message.

The key is authenticated via an ``Authorization: Bearer`` HEADER, so it is NEVER
placed in a URL / query string and therefore never appears in any request
fingerprint or archived metadata.

Governance: this collector is TRIAL / RESEARCH-ONLY. It is NOT wired into
``configs/alpha_agent/stage2_ingestion.json`` and therefore is never run by the
scheduled Collect/ingestion cadence — it is reachable ONLY through the explicit
operator probe/acquire entrypoint ``scripts/intrinio_entitlement_probe.py``. It
writes ONLY to its content-addressed RawArchive under the trial research root on
``D:`` — never to an operational ledger, the portfolio, a model champion, or any
order path. When the trial credential is absent the collector returns a
controlled BLOCKED (``NOT_PROVISIONED``) result and makes NO network call at all.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Optional

from ..source_contracts import (
    EM_MATCHED_EXACT, EM_UNMATCHED, ENT_AUTH_FAILED, ENT_ENTITLED,
    ENT_NOT_ENTITLED, ENT_RATE_LIMITED, ENT_UNKNOWN, RT_EARNINGS_EVENT,
    RT_FUNDAMENTAL_FACT, RT_SECURITY_IDENTITY, SH_BLOCKED_CONFIGURATION,
    SH_BLOCKED_LICENSE, SH_DEGRADED, SH_FAILED, SH_HEALTHY,
    build_normalized_record,
)
from .base import BaseCollector
from .bea import dpapi_unprotect  # reuse the ONE DPAPI decrypt (no duplicate)

# --- Phase-1 entitlement classification vocabulary (single source of truth) --- #
CLS_ACTIVE = "ACTIVE"
CLS_NOT_PROVISIONED = "NOT_PROVISIONED"
CLS_AUTH_FAILURE = "AUTH_FAILURE"
CLS_ENDPOINT_NOT_ENTITLED = "ENDPOINT_NOT_ENTITLED"
CLS_NETWORK_FAILURE = "NETWORK_FAILURE"
CLS_RATE_LIMITED = "RATE_LIMITED"
CLS_UNKNOWN = "UNKNOWN"
ENTITLEMENT_CLASSIFICATIONS = (
    CLS_ACTIVE, CLS_NOT_PROVISIONED, CLS_AUTH_FAILURE, CLS_ENDPOINT_NOT_ENTITLED,
    CLS_NETWORK_FAILURE, CLS_RATE_LIMITED, CLS_UNKNOWN,
)

_DEFAULT_DPAPI_DIR = os.path.join(os.path.expanduser("~"), ".paper_trader",
                                  "alpha_agent_intrinio")
_DEFAULT_DPAPI_FILE = "intrinio_api_key.dpapi"


def classify_probe(status: Optional[int], error: Optional[str],
                   parse_ok: bool) -> str:
    """Map one HTTP probe outcome to the Phase-1 entitlement classification.

    A network/transport error (status 0/None with an error string) is a
    NETWORK_FAILURE — NEVER silently treated as empty data or as not-entitled.
    """
    if error:
        return CLS_NETWORK_FAILURE
    if status == 200 and parse_ok:
        return CLS_ACTIVE
    if status == 200 and not parse_ok:
        return CLS_UNKNOWN
    if status == 401:
        return CLS_AUTH_FAILURE
    if status in (402, 403):
        return CLS_ENDPOINT_NOT_ENTITLED
    if status == 429:
        return CLS_RATE_LIMITED
    if status in (0, None):
        return CLS_NETWORK_FAILURE
    return CLS_UNKNOWN


def entitlement_state_from_classification(cls: str) -> str:
    """Map the Phase-1 classification to the canonical source-contract
    entitlement state vocabulary (ENT_*)."""
    return {
        CLS_ACTIVE: ENT_ENTITLED,
        CLS_AUTH_FAILURE: ENT_AUTH_FAILED,
        CLS_ENDPOINT_NOT_ENTITLED: ENT_NOT_ENTITLED,
        CLS_RATE_LIMITED: ENT_RATE_LIMITED,
    }.get(cls, ENT_UNKNOWN)


class IntrinioCollector(BaseCollector):
    source_id = "intrinio"
    requires_credential = True
    # Trial governance (config-level; NOT new source_contracts vocabulary).
    research_use_only = True

    # ------------------------------------------------------------------ #
    # Credential resolution — env var first (redaction-integrated), DPAPI fallback
    # ------------------------------------------------------------------ #
    def _dpapi_path(self) -> str:
        explicit = self.ctx.source_cfg.get("dpapi_credential_path")
        if explicit:
            return explicit
        d = self.ctx.source_cfg.get("dpapi_credential_dir_name") or "alpha_agent_intrinio"
        f = self.ctx.source_cfg.get("dpapi_credential_file") or _DEFAULT_DPAPI_FILE
        if self.ctx.source_cfg.get("dpapi_credential_dir_name"):
            return os.path.join(os.path.expanduser("~"), ".paper_trader", d, f)
        return os.path.join(_DEFAULT_DPAPI_DIR, f)

    def _resolve_dpapi_key(self) -> Optional[str]:
        path = Path(self._dpapi_path())
        try:
            if not path.is_file():
                return None
            blob = path.read_text(encoding="ascii")
        except OSError:
            return None
        key = dpapi_unprotect(blob)
        if key and key not in self.ctx.secrets:
            self.ctx.secrets.append(key)  # redact from every outbound fingerprint/error
        return key

    def _resolve_key(self) -> Optional[str]:
        for name in self.ctx.source_cfg.get(
                "allowed_env_vars",
                ["INTRINIO_API_KEY", "PAPER_TRADER_INTRINIO_API_KEY"]):
            value = self.ctx.env.get(name)
            if value:
                # Register as a redaction secret so it can never reach a persisted
                # fingerprint/error even when this collector runs outside the
                # ingestion redaction layer (e.g. the explicit operator probe).
                if value not in self.ctx.secrets:
                    self.ctx.secrets.append(value)
                return value
        return self._resolve_dpapi_key()

    # ------------------------------------------------------------------ #
    def _headers(self, key: str) -> dict:
        # Bearer header keeps the key OUT of the URL entirely (fingerprint-safe).
        return {"Authorization": "Bearer %s" % key}

    def _url(self, path: str) -> str:
        base = self.ctx.source_cfg.get("base_url", "https://api-v2.intrinio.com").rstrip("/")
        return "%s/%s" % (base, str(path).lstrip("/"))

    def _parse_json(self, body: Optional[bytes]) -> tuple[bool, Any]:
        if not body:
            return False, None
        try:
            return True, json.loads(body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return False, None

    def _datasets(self) -> list[dict]:
        return list(self.ctx.source_cfg.get("datasets") or [])

    def _no_credential_reason(self) -> str:
        return ("no Intrinio TRIAL credential present (checked env var NAMES %s and "
                "the DPAPI credential file); the operator must run "
                "scripts\\configure_alpha_agent_intrinio.ps1 (or set INTRINIO_API_KEY) "
                "once Intrinio activates the 14-day trial — the trial is NOT_PROVISIONED"
                % self.ctx.source_cfg.get(
                    "allowed_env_vars",
                    ["INTRINIO_API_KEY", "PAPER_TRADER_INTRINIO_API_KEY"]))

    def _blocked_not_provisioned(self) -> dict:
        """Controlled NOT_PROVISIONED result: no credential, NO network call."""
        for ds in self._datasets():
            self.entitlements.append({
                "dataset_key": ds.get("dataset_key"),
                "data_category": ds.get("data_category"),
                "family": ds.get("dataset_key"),
                "state": ENT_UNKNOWN,
                "classification": CLS_NOT_PROVISIONED,
                "http_status": None,
                "detail": "credential absent; no probe issued",
                "license_state": "TRIAL",
                "research_use_only": True,
                "operational_use_allowed": False,
            })
        self.inventory["entitlement_classification"] = {
            ds.get("dataset_key"): CLS_NOT_PROVISIONED for ds in self._datasets()}
        self.inventory["trial_provisioned"] = False
        return self.blocked_result(
            SH_BLOCKED_CONFIGURATION, self._no_credential_reason(),
            credential_present=False)

    def _enforce_research_only(self) -> Optional[dict]:
        """Refuse to run if the config asserts operational use — this collector is
        TRIAL / research-only by construction."""
        if self.ctx.source_cfg.get("operational_use_allowed") is True:
            return self.blocked_result(
                SH_BLOCKED_LICENSE,
                "Intrinio data is TRIAL / research-only; operational_use_allowed "
                "must be false — refusing to run.", credential_present=None)
        return None

    # ------------------------------------------------------------------ #
    def _probe_dataset(self, ds: dict, key: str) -> dict:
        """Smallest safe entitlement probe for one dataset. Parsed in memory;
        NOT archived (an entitlement probe is not research data and may carry
        account metadata)."""
        path = ds.get("entitlement_probe_path") or "/companies?page_size=1"
        res = self.fetch(self._url(path), headers=self._headers(key),
                         expect="text", archive=False, extension="json",
                         content_type="application/json")
        parse_ok, _obj = self._parse_json(res["body"]) if res["ok"] else (False, None)
        cls = classify_probe(res.get("status"), res.get("error"), parse_ok)
        state = entitlement_state_from_classification(cls)
        detail = (res.get("rejected_reason") or res.get("error")
                  or ("HTTP %s" % res.get("status")))
        probe = {
            "dataset_key": ds.get("dataset_key"),
            "data_category": ds.get("data_category"),
            "family": ds.get("dataset_key"),
            "state": state,
            "classification": cls,
            "http_status": res.get("status"),
            "detail": "probe %s -> %s" % (ds.get("dataset_key"), detail),
            "license_state": "TRIAL",
            "research_use_only": True,
            "operational_use_allowed": False,
            "current_data_only": bool(ds.get("current_data_only")),
        }
        return probe

    def _run_entitlement_audit(self, key: str) -> dict[str, str]:
        classifications: dict[str, str] = {}
        for ds in self._datasets():
            probe = self._probe_dataset(ds, key)
            classifications[ds.get("dataset_key")] = probe["classification"]
            self.entitlements.append(probe)
        self.inventory["entitlement_classification"] = dict(classifications)
        self.inventory["trial_provisioned"] = True
        return classifications

    # ------------------------------------------------------------------ #
    def audit(self) -> dict:
        blocked = self._enforce_research_only()
        if blocked is not None:
            return blocked
        key = self._resolve_key()
        if key is None:
            return self._blocked_not_provisioned()
        cls = self._run_entitlement_audit(key)
        active = sorted(k for k, c in cls.items() if c == CLS_ACTIVE)
        if active:
            state = SH_HEALTHY
        elif any(c == CLS_AUTH_FAILURE for c in cls.values()):
            state = SH_FAILED
        else:
            state = SH_FAILED
        return self.result(
            overall_state=state, credential_present=True,
            entitlement_summary="Intrinio TRIAL entitlement: %s" % (
                "; ".join("%s=%s" % (k, c) for k, c in sorted(cls.items())) or "none"))

    # ------------------------------------------------------------------ #
    def collect(self, as_of: str) -> dict:
        """Bounded, research-only acquisition of the ACTIVE trial datasets. Only
        exercised live by the operator once the trial is provisioned; every raw
        object is content-addressed under the trial research root. NEVER writes an
        operational ledger. With no credential this returns NOT_PROVISIONED and
        makes no network call."""
        blocked = self._enforce_research_only()
        if blocked is not None:
            return blocked
        key = self._resolve_key()
        if key is None:
            return self._blocked_not_provisioned()
        cls = self._run_entitlement_audit(key)
        retrieved = self.ctx.now_iso()
        collected = 0
        for ds in self._datasets():
            if cls.get(ds.get("dataset_key")) != CLS_ACTIVE:
                continue
            cat = ds.get("data_category")
            if cat == "fundamentals":
                collected += self._collect_fundamentals(ds, key, as_of, retrieved)
            elif cat == "analyst_revisions":
                collected += self._collect_zacks_current(ds, key, as_of, retrieved)
        active = sorted(k for k, c in cls.items() if c == CLS_ACTIVE)
        self.cursor = {"last_collected_as_of": as_of, "active_datasets": active,
                       "newest_event_time": self.newest_source_event_time}
        if not active:
            state = SH_FAILED
        elif self.http_errors and self.records:
            state = SH_DEGRADED
        elif not collected:
            state = SH_DEGRADED if self.records else SH_FAILED
        else:
            state = SH_HEALTHY
        return self.result(
            overall_state=state, credential_present=True,
            entitlement_summary="Intrinio TRIAL collect; active=%s; records=%d"
                                % (",".join(active), len(self.records)))

    # ------------------------------------------------------------------ #
    def _collect_fundamentals(self, ds: dict, key: str, as_of: str,
                              retrieved: str) -> int:
        """Bounded US-Fundamentals slice for the configured sample companies.
        Standardized-financials PIT discipline: filing/publish date is the
        availability; fiscal period_end is NEVER substituted for availability."""
        n = 0
        for sym in (ds.get("sample_symbols") or self.ctx.source_cfg.get("sample_symbols") or []):
            path = (ds.get("fundamentals_path_template")
                    or "/companies/%s/fundamentals?page_size=4") % sym
            res = self.fetch(self._url(path), headers=self._headers(key),
                             expect="text", extension="json", business_date=as_of,
                             native_id="intrinio|fundamentals|%s" % sym,
                             content_type="application/json",
                             license_note=self.ctx.source_cfg.get("license_note"))
            if not res["ok"]:
                continue
            ok, obj = self._parse_json(res["body"])
            if not ok or not isinstance(obj, dict):
                continue
            rows = obj.get("fundamentals") if isinstance(obj.get("fundamentals"), list) else []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                period_end = str(row.get("end_date") or row.get("fiscal_period_end") or "")[:10] or None
                filed = str(row.get("filing_date") or row.get("filed_date") or "")[:10] or None
                payload = dict(row)
                payload["period_end"] = period_end
                payload["publication_time"] = filed
                warnings = [] if filed else [
                    "AVAILABILITY_UNKNOWN: no filing_date on the fundamental; "
                    "available_at left null (period_end NEVER used as availability)"]
                self.records.append(build_normalized_record(
                    record_type=RT_FUNDAMENTAL_FACT, source_id=self.source_id,
                    source_native_id="intrinio|fund|%s|%s" % (sym, period_end),
                    raw_object_id=res["raw"]["raw_object_id"], retrieved_at=retrieved,
                    observed_at=filed, effective_at=period_end, available_at=filed,
                    ticker=str(sym).upper(), event_type="FUNDAMENTAL_STANDARDIZED",
                    payload=payload, entity_mapping_confidence=EM_UNMATCHED,
                    provenance="Intrinio TRIAL /companies/{id}/fundamentals "
                               "(research-only)", quality_warnings=warnings))
                n += 1
                if filed:
                    self.note_event_time(filed)
        return n

    def _collect_zacks_current(self, ds: dict, key: str, as_of: str,
                               retrieved: str) -> int:
        """Bounded Zacks CURRENT-consensus snapshot for the configured symbols.

        CURRENT DATA ONLY: each snapshot is a genuine forward-time observation
        captured at ``retrieved`` (its real availability). It is explicitly marked
        as a current-consensus snapshot and is NOT a historical point-in-time
        revision — it must NEVER be used as historical-revision alpha evidence.
        """
        n = 0
        for sym in (ds.get("sample_symbols") or self.ctx.source_cfg.get("sample_symbols") or []):
            path = (ds.get("estimates_path_template")
                    or "/zacks/eps_estimates?identifier=%s&page_size=8") % sym
            res = self.fetch(self._url(path), headers=self._headers(key),
                             expect="text", extension="json", business_date=as_of,
                             native_id="intrinio|zacks_eps|%s" % sym,
                             content_type="application/json",
                             license_note=self.ctx.source_cfg.get("license_note"))
            if not res["ok"]:
                continue
            ok, obj = self._parse_json(res["body"])
            if not ok or not isinstance(obj, dict):
                continue
            rows = obj.get("estimates") if isinstance(obj.get("estimates"), list) else []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                period_end = str(row.get("fiscal_period_end") or row.get("calendar_period") or "")[:10] or None
                payload = dict(row)
                payload["period_end"] = period_end
                # The snapshot's availability is genuinely the retrieval time.
                payload["publication_time"] = retrieved
                self.records.append(build_normalized_record(
                    record_type=RT_EARNINGS_EVENT, source_id=self.source_id,
                    source_native_id="intrinio|zacks_eps|%s|%s|%s" % (
                        sym, period_end, retrieved[:10]),
                    raw_object_id=res["raw"]["raw_object_id"], retrieved_at=retrieved,
                    observed_at=retrieved, effective_at=period_end,
                    available_at=retrieved, ticker=str(sym).upper(),
                    event_type="ANALYST_ESTIMATE_CURRENT_CONSENSUS", payload=payload,
                    entity_mapping_confidence=EM_UNMATCHED,
                    provenance="Intrinio TRIAL /zacks/eps_estimates (CURRENT "
                               "consensus snapshot, research-only)",
                    quality_warnings=[
                        "CURRENT_CONSENSUS_SNAPSHOT: forward-collected current "
                        "consensus, NOT a historical point-in-time revision; INVALID "
                        "as historical-revision alpha evidence (stays DATA_HOLD)"]))
                n += 1
                self.note_event_time(retrieved)
        return n
