"""
alpha_agent/collectors/bea.py — SOURCE: U.S. Bureau of Economic Analysis.

Official public BEA API (apps.bea.gov/api). BEA requires a free UserID (obtained
by registration at bea.gov/API/signup); it is read from BEA_API_KEY /
PAPER_TRADER_BEA_API_KEY, checked for PRESENCE only, and redacted from every
stored fingerprint. When NO UserID is present the source is BLOCKED_CREDENTIAL and
Stage 2 continues — this is an honest, resolvable hard blocker (register a free
key), never a fabricated completion. No unofficial scraping.

Collects a bounded allowlist of NIPA table series as macro observations. Point-in-
time discipline mirrors the other macro collectors: the reference ``TimePeriod``
is observed/effective; ``available_at`` is left null with a RELEASE_LAG warning.
"""
from __future__ import annotations

import json
import os
import urllib.parse
from pathlib import Path
from typing import Optional

from ..source_contracts import (
    EM_UNMATCHED, RT_MACRO_OBSERVATION, SH_BLOCKED_CONFIGURATION, SH_DEGRADED,
    SH_FAILED, SH_HEALTHY, build_normalized_record,
)
from .base import BaseCollector

_DEFAULT_BASE = "https://apps.bea.gov/api/data"
_DEFAULT_DPAPI_PATH = os.path.join(os.path.expanduser("~"), ".paper_trader",
                                   "alpha_agent_bea", "bea_userid.dpapi")


def dpapi_unprotect(blob_hex: str) -> Optional[str]:
    """Decrypt a Windows DPAPI (CurrentUser) blob produced by PowerShell's
    ``ConvertFrom-SecureString`` (a hex-encoded protected blob of UTF-16LE text).
    Uses only ctypes + crypt32 (stdlib; Windows-only). Returns None on any
    failure — never raises, never logs the plaintext. The plaintext is returned
    to the caller in memory only."""
    try:
        import ctypes
        from ctypes import wintypes
    except Exception:  # noqa: BLE001 — non-Windows / no ctypes
        return None
    try:
        blob = bytes.fromhex((blob_hex or "").strip())
    except ValueError:
        return None
    if not blob:
        return None

    class _BLOB(ctypes.Structure):
        _fields_ = [("cbData", wintypes.DWORD),
                    ("pbData", ctypes.POINTER(ctypes.c_char))]

    try:
        buf = ctypes.create_string_buffer(blob, len(blob))
        blob_in = _BLOB(len(blob), ctypes.cast(buf, ctypes.POINTER(ctypes.c_char)))
        blob_out = _BLOB()
        ok = ctypes.windll.crypt32.CryptUnprotectData(
            ctypes.byref(blob_in), None, None, None, None, 0,
            ctypes.byref(blob_out))
        if not ok:
            return None
        try:
            data = ctypes.string_at(blob_out.pbData, blob_out.cbData)
        finally:
            ctypes.windll.kernel32.LocalFree(blob_out.pbData)
    except Exception:  # noqa: BLE001 — decryption must never raise upward
        return None
    for enc in ("utf-16-le", "utf-8"):
        try:
            text = data.decode(enc).strip()
            if text:
                return text
        except UnicodeDecodeError:
            continue
    return None


class BeaCollector(BaseCollector):
    source_id = "bea"
    requires_credential = True

    def _dpapi_path(self) -> str:
        return self.ctx.source_cfg.get("dpapi_credential_path") \
            or _DEFAULT_DPAPI_PATH

    def _resolve_dpapi_key(self) -> Optional[str]:
        """Resolve the BEA UserID from the DPAPI-protected credential file at
        RUNTIME (the secure lane written by scripts/configure_alpha_agent_bea.ps1).
        The plaintext lives only in memory and is registered as a redaction secret
        so it can never appear in a persisted fingerprint or error message."""
        path = Path(self._dpapi_path())
        try:
            if not path.is_file():
                return None
            blob = path.read_text(encoding="ascii")
        except OSError:
            return None
        key = dpapi_unprotect(blob)
        if key and key not in self.ctx.secrets:
            self.ctx.secrets.append(key)  # redact from every outbound fingerprint
        return key

    def _resolve_key(self) -> Optional[str]:
        for name in self.ctx.source_cfg.get("allowed_env_vars",
                                            ["BEA_API_KEY",
                                             "PAPER_TRADER_BEA_API_KEY"]):
            value = self.ctx.env.get(name)
            if value:
                return value
        # No env var: fall back to the secure DPAPI credential resolved at runtime.
        return self._resolve_dpapi_key()

    def _blocked(self) -> dict:
        # The credential is ABSENT (not rejected): a FREE registered UserID is
        # simply not provisioned here. That is a resolvable CONFIGURATION gap
        # (register + set env), classified ACCESSIBLE_AFTER_REPAIR — NOT
        # INVALID_CREDENTIAL (which would wrongly imply a bad key).
        return self.blocked_result(
            SH_BLOCKED_CONFIGURATION,
            "no BEA UserID present (checked env var NAMES %s and the DPAPI "
            "credential file); register a FREE UserID at bea.gov/API/signup then "
            "run scripts\\configure_alpha_agent_bea.ps1 (or set BEA_API_KEY); "
            "unofficial scraping is prohibited"
            % self.ctx.source_cfg.get("allowed_env_vars",
                                      ["BEA_API_KEY"]),
            credential_present=False)

    def _url(self, key: str, table: dict) -> str:
        base = self.ctx.source_cfg.get("base_url", _DEFAULT_BASE).rstrip("/")
        params = {
            "UserID": key, "method": "GetData",
            "datasetname": table.get("dataset", "NIPA"),
            "TableName": table.get("table_name"),
            "Frequency": table.get("frequency", "Q"),
            "Year": table.get("year", "ALL"),
            "ResultFormat": "JSON",
        }
        return "%s?%s" % (base, urllib.parse.urlencode(params))

    # ------------------------------------------------------------------ #
    def audit(self) -> dict:
        key = self._resolve_key()
        if key is None:
            return self._blocked()
        table = (self.ctx.source_cfg.get("tables") or [{}])[0]
        res = self.fetch(self._url(key, table), expect="text", archive=False,
                         extension="json", content_type="application/json")
        return self.result(overall_state=SH_HEALTHY if res["ok"] else SH_FAILED,
                           credential_present=True,
                           entitlement_summary="keyed official BEA API probe %s"
                                               % ("ok" if res["ok"] else "failed"))

    def collect(self, as_of: str) -> dict:
        key = self._resolve_key()
        if key is None:
            return self._blocked()
        retrieved = self.ctx.now_iso()
        tables = self.ctx.source_cfg.get("tables", [])
        tables_ok = 0
        for table in tables:
            res = self.fetch(self._url(key, table), expect="text",
                             extension="json", business_date=as_of,
                             native_id="bea|%s|%s" % (table.get("dataset"),
                                                      table.get("table_name")),
                             content_type="application/json")
            if not res["ok"]:
                continue
            try:
                obj = json.loads(res["body"].decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                self.record_error("PARSE_ERROR", "BEA response not JSON")
                continue
            results = ((obj.get("BEAAPI") or {}).get("Results") or {})
            if isinstance(results, dict) and results.get("Error"):
                self.record_error("BEA_ERROR", str(results.get("Error"))[:200])
                continue
            data = results.get("Data") if isinstance(results, dict) else None
            if not isinstance(data, list) or not data:
                continue
            tables_ok += 1
            for row in data:
                if not isinstance(row, dict):
                    continue
                tp = str(row.get("TimePeriod") or "")
                obs_date = _bea_period_to_date(tp)
                if not obs_date:
                    continue
                payload = {
                    "dataset": table.get("dataset"),
                    "table_name": table.get("table_name"),
                    "macro_family": table.get("macro_family",
                                              "national_accounts"),
                    "line_description": row.get("LineDescription"),
                    "series_code": row.get("SeriesCode"),
                    "time_period": tp, "observation_date": obs_date,
                    "value": row.get("DataValue"),
                    "unit": row.get("CL_UNIT") or row.get("UNIT_MULT"),
                }
                self.records.append(build_normalized_record(
                    record_type=RT_MACRO_OBSERVATION, source_id=self.source_id,
                    source_native_id="%s|%s|%s|%s" % (
                        table.get("table_name"), row.get("SeriesCode"), tp,
                        row.get("LineNumber", "")),
                    raw_object_id=res["raw"]["raw_object_id"],
                    retrieved_at=retrieved, observed_at=obs_date,
                    effective_at=obs_date, available_at=None,
                    event_type="MACRO_OBSERVATION", payload=payload,
                    entity_mapping_confidence=EM_UNMATCHED,
                    provenance="U.S. Bureau of Economic Analysis API (official, "
                               "keyed)",
                    quality_warnings=[
                        "RELEASE_LAG_UNKNOWN: BEA TimePeriod is the reference "
                        "period, not the release date; available_at left null"]))
                self.note_event_time(obs_date)
        total = len(tables)
        if tables_ok == total and total:
            state = SH_HEALTHY
        elif tables_ok:
            state = SH_DEGRADED
        else:
            state = SH_FAILED
        return self.result(overall_state=state, credential_present=True,
                           entitlement_summary="keyed official BEA API; %d/%d "
                                               "tables" % (tables_ok, total))


def _bea_period_to_date(tp: str) -> Optional[str]:
    """Map a BEA TimePeriod ('2025', '2025Q3', '2025M06') to an ISO date."""
    tp = (tp or "").strip()
    if len(tp) == 4 and tp.isdigit():
        return "%s-01-01" % tp
    if "Q" in tp:
        y, _, q = tp.partition("Q")
        mo = {"1": 1, "2": 4, "3": 7, "4": 10}.get(q.strip())
        return "%s-%02d-01" % (y, mo) if (y.isdigit() and mo) else None
    if "M" in tp:
        y, _, m = tp.partition("M")
        return "%s-%02d-01" % (y, int(m)) if (y.isdigit() and m.isdigit()) \
            else None
    return None
