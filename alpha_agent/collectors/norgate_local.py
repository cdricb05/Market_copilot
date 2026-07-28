"""
alpha_agent/collectors/norgate_local.py — SOURCE 1: Norgate local data.

Local historical price/volume foundation, survivorship-aware universe history,
identifiers and index membership — read through the EXISTING installed
``norgatedata`` integration (Norgate Data Updater). This collector:

  * never installs or upgrades Norgate software;
  * never changes the Norgate installation;
  * never initiates a large historical re-export — it indexes a bounded recent
    window for a pre-registered sample plus a deterministic asset/entitlement
    inventory;
  * never claims a field exists unless it was actually observed (every vendor
    call is individually guarded and its observed fields recorded);
  * makes NO network calls of its own (NDU serves data locally).

If the ``norgatedata`` package is unavailable the collector returns a
controlled NOT_CONFIGURED result and Stage 2 continues.
"""
from __future__ import annotations

import datetime as _dt
import json
from typing import Any, Optional

from ..source_contracts import (
    EM_MATCHED_EXACT, EM_UNMATCHED, RT_MARKET_BAR, RT_SECURITY_IDENTITY,
    RT_UNIVERSE_MEMBERSHIP, SH_BLOCKED_CONFIGURATION, SH_DEGRADED, SH_FAILED,
    SH_HEALTHY, SH_NOT_CONFIGURED, build_normalized_record,
)
from .base import BaseCollector

_CANDIDATE_INVENTORY_FUNCS = (
    "databases", "watchlists", "status", "last_database_update_time",
)
_CANDIDATE_IDENTITY_FUNCS = (
    "assetid", "security_name", "exchange_name", "exchange_name_full",
    "base_type", "subtype1", "subtype2", "subtype3", "currency", "domicile",
    "first_quoted_date", "second_last_quoted_date", "last_quoted_date",
    "classification", "sharesoutstanding", "sharesfloat",
)


def _iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _recarray_rows(arr: Any) -> tuple[list[str], list[dict]]:
    """Convert a numpy recarray to (observed field names, list of plain rows)."""
    names = list(getattr(arr, "dtype", None).names or [])
    rows: list[dict] = []
    for item in arr:
        row: dict[str, Any] = {}
        for name in names:
            v = item[name]
            if hasattr(v, "item"):
                v = v.item()
            if hasattr(v, "isoformat"):
                v = v.isoformat()
            row[name] = v
        rows.append(row)
    return names, rows


class NorgateLocalCollector(BaseCollector):
    source_id = "norgate_local"
    requires_credential = False

    def _import_norgatedata(self):
        try:
            import norgatedata  # noqa: PLC0415 — lazy vendor import by design
            return norgatedata
        except Exception as exc:  # noqa: BLE001
            self.record_error("VENDOR_IMPORT_UNAVAILABLE",
                              "norgatedata import failed: %s: %s"
                              % (type(exc).__name__, exc))
            return None

    @staticmethod
    def _call(nd: Any, func_name: str, *args: Any) -> tuple[bool, Any]:
        """Guarded vendor call: (observed, value). Never raises."""
        fn = getattr(nd, func_name, None)
        if fn is None or not callable(fn):
            return False, None
        try:
            return True, fn(*args)
        except Exception:  # noqa: BLE001 — record-only; never fail the run
            return False, None

    def _price_timeseries(self, nd: Any, symbol: str, start_date: str) -> Optional[Any]:
        variants = []
        try:
            variants.append(dict(
                stock_price_adjustment_setting=nd.StockPriceAdjustmentType.TOTALRETURN,
                padding_setting=nd.PaddingType.NONE,
                start_date=start_date, timeseriesformat="numpy-recarray"))
        except Exception:  # noqa: BLE001
            pass
        variants.append(dict(start_date=start_date, timeseriesformat="numpy-recarray"))
        variants.append(dict(start_date=start_date))
        for kwargs in variants:
            try:
                return nd.price_timeseries(symbol, **kwargs)
            except TypeError:
                continue
            except Exception as exc:  # noqa: BLE001
                self.record_error("VENDOR_QUERY_ERROR",
                                  "price_timeseries(%s): %s: %s"
                                  % (symbol, type(exc).__name__, exc))
                return None
        return None

    def _index_membership(self, nd: Any, symbol: str, index_name: str,
                          start_date: str) -> Optional[Any]:
        for kwargs in (dict(padding_setting=nd.PaddingType.NONE, start_date=start_date,
                            timeseriesformat="numpy-recarray"),
                       dict(start_date=start_date, timeseriesformat="numpy-recarray"),
                       dict(start_date=start_date)):
            try:
                return nd.index_constituent_timeseries(symbol, index_name, **kwargs)
            except TypeError:
                continue
            except Exception as exc:  # noqa: BLE001
                self.record_error("VENDOR_QUERY_ERROR",
                                  "index_constituent_timeseries(%s,%s): %s: %s"
                                  % (symbol, index_name, type(exc).__name__, exc))
                return None
        return None

    # ------------------------------------------------------------------ #
    def _build_inventory(self, nd: Any) -> dict:
        inv: dict[str, Any] = {
            "package_version": str(getattr(nd, "__version__", "unknown")),
            "integration": "installed norgatedata package + running Norgate Data Updater (no install, no upgrade, no re-export)",
            "functions_observed": [],
            "functions_unobserved": [],
        }
        for fname in _CANDIDATE_INVENTORY_FUNCS + _CANDIDATE_IDENTITY_FUNCS:
            if callable(getattr(nd, fname, None)):
                inv["functions_observed"].append(fname)
            else:
                inv["functions_unobserved"].append(fname)
        observed, dbs = self._call(nd, "databases")
        if observed and dbs:
            inv["databases"] = [str(d) for d in dbs]
        observed, wls = self._call(nd, "watchlists")
        if observed and wls is not None:
            max_wl = int(self.ctx.source_cfg.get("max_watchlists", 100))
            names = [str(w) for w in list(wls)[:max_wl]]
            inv["watchlist_count"] = len(list(wls))
            inv["watchlists"] = names
        observed, upd = self._call(nd, "last_database_update_time")
        if observed and upd is not None:
            inv["last_database_update_time"] = _iso(upd)
        inv["point_in_time_semantics"] = (
            "price_timeseries served with explicit adjustment enum (TOTALRETURN "
            "requested, observed per query); delisted/inactive securities retained by "
            "the vendor database; index membership via index_constituent_timeseries. "
            "Per-bar publication timestamps are NOT provided by the vendor API and are "
            "therefore left null, never fabricated.")
        return inv

    def _identity_snapshot(self, nd: Any, symbol: str) -> dict:
        ident: dict[str, Any] = {}
        for fname in _CANDIDATE_IDENTITY_FUNCS:
            observed, value = self._call(nd, fname, symbol)
            if observed and value is not None:
                ident[fname] = _iso(value) if hasattr(value, "isoformat") else (
                    value if isinstance(value, (str, int, float, bool)) else str(value))
        return ident

    # ------------------------------------------------------------------ #
    def audit(self) -> dict:
        nd = self._import_norgatedata()
        if nd is None:
            return self.blocked_result(
                SH_NOT_CONFIGURED,
                "norgatedata package not importable in this environment; Norgate "
                "collection unavailable (no install attempted).")
        observed, running = self._call(nd, "status")
        self.inventory = self._build_inventory(nd)
        self.inventory["ndu_running"] = bool(running) if observed else None
        if not (observed and running):
            return self.blocked_result(
                SH_BLOCKED_CONFIGURATION,
                "Norgate Data Updater is not running; local vendor data not servable.")
        self.last_attempt_at = self.ctx.now_iso()
        self._register_success()
        return self.result(overall_state=SH_HEALTHY, credential_present=None,
                           entitlement_summary="local vendor data reachable (NDU running)")

    def collect(self, as_of: str) -> dict:
        nd = self._import_norgatedata()
        if nd is None:
            return self.blocked_result(
                SH_NOT_CONFIGURED,
                "norgatedata package not importable in this environment; Norgate "
                "collection unavailable (no install attempted).")
        observed, running = self._call(nd, "status")
        self.inventory = self._build_inventory(nd)
        self.inventory["ndu_running"] = bool(running) if observed else None
        if not (observed and running):
            return self.blocked_result(
                SH_BLOCKED_CONFIGURATION,
                "Norgate Data Updater is not running; local vendor data not servable.")

        cfg = self.ctx.source_cfg
        sample = list(cfg.get("sample_symbols", []))
        recent_days = int(cfg.get("recent_bar_days", 30))
        start_date = (_dt.date.fromisoformat(as_of)
                      - _dt.timedelta(days=recent_days)).isoformat()
        retrieved = self.ctx.now_iso()
        license_note = cfg.get("license_note", "")
        symbols_ok = 0
        fields_observed: dict[str, list[str]] = {}

        for symbol in sample:
            self.last_attempt_at = self.ctx.now_iso()
            ident = self._identity_snapshot(nd, symbol)
            arr = self._price_timeseries(nd, symbol, start_date)
            bar_names: list[str] = []
            bar_rows: list[dict] = []
            if arr is not None:
                try:
                    bar_names, bar_rows = _recarray_rows(arr)
                except Exception as exc:  # noqa: BLE001
                    self.record_error("VENDOR_PARSE_ERROR",
                                      "recarray conversion failed for %s: %s: %s"
                                      % (symbol, type(exc).__name__, exc))
            snapshot = {"symbol": symbol, "query_start_date": start_date,
                        "identity": ident, "bar_fields": bar_names,
                        "bars": bar_rows}
            content = json.dumps(snapshot, sort_keys=True, default=str).encode("utf-8")
            raw = self.ctx.archive.store(
                source_id=self.source_id, content=content, extension="json",
                retrieved_at=retrieved, business_date=as_of,
                source_native_id="norgate|%s|%s" % (symbol, start_date),
                request_fp="LOCAL norgatedata price_timeseries(%s, start=%s)"
                           % (symbol, start_date),
                content_type="application/json", http_status=None, retry_count=0,
                published_at=self.inventory.get("last_database_update_time"),
                license_note=license_note)
            if not raw["duplicate"]:
                self.raw_objects.append(raw)

            security_id = str(ident["assetid"]) if "assetid" in ident else None
            exchange = ident.get("exchange_name")
            if security_id is not None:
                self.ctx.identity.register_security_id(symbol, security_id,
                                                       "norgate_local")
            if exchange:
                self.ctx.identity.register_exchange(symbol, str(exchange),
                                                    "norgate_local")
            mapping = EM_MATCHED_EXACT if security_id is not None else EM_UNMATCHED
            if ident:
                self.records.append(build_normalized_record(
                    record_type=RT_SECURITY_IDENTITY, source_id=self.source_id,
                    source_native_id="identity|%s" % symbol,
                    raw_object_id=raw["raw_object_id"], retrieved_at=retrieved,
                    observed_at=retrieved, effective_at=as_of,
                    available_at=retrieved, ticker=symbol,
                    security_id=security_id, exchange=exchange,
                    event_type="IDENTITY_SNAPSHOT", payload=ident,
                    entity_mapping_confidence=mapping,
                    provenance="norgatedata identity functions (observed-only fields)",
                    quality_warnings=["IDENTITY_AS_OBSERVED_ON_RETRIEVAL: identity "
                                      "fields are current vendor values, not a "
                                      "historical identity timeline"]))
            if bar_rows:
                symbols_ok += 1
                fields_observed[symbol] = bar_names
                for row in bar_rows:
                    bar_date = str(row.get("Date", ""))[:10]
                    if not bar_date:
                        continue
                    payload = {k: v for k, v in row.items()}
                    payload["adjustment_requested"] = "TOTALRETURN"
                    payload["fields_observed"] = bar_names
                    self.records.append(build_normalized_record(
                        record_type=RT_MARKET_BAR, source_id=self.source_id,
                        source_native_id="%s|%s" % (symbol, bar_date),
                        raw_object_id=raw["raw_object_id"], retrieved_at=retrieved,
                        observed_at=bar_date, effective_at=bar_date,
                        available_at=None, ticker=symbol,
                        security_id=security_id, exchange=exchange,
                        event_type="EOD_BAR", payload=payload,
                        entity_mapping_confidence=mapping,
                        provenance="norgatedata.price_timeseries (survivorship-aware local database)"))
                    self.note_event_time(bar_date)
            elif arr is None:
                self.record_error("VENDOR_NO_DATA",
                                  "no price series returned for %s" % symbol)

        # Bounded index-membership window for the pre-registered subset.
        index_name = cfg.get("index_name", "S&P 500")
        for symbol in cfg.get("index_partition_symbols", []):
            arr = self._index_membership(nd, symbol, index_name, start_date)
            if arr is None:
                continue
            try:
                names, rows = _recarray_rows(arr)
            except Exception:  # noqa: BLE001
                continue
            flag_field = next((n for n in names if "constituent" in n.lower()), None)
            content = json.dumps({"symbol": symbol, "index": index_name,
                                  "fields": names, "rows": rows},
                                 sort_keys=True, default=str).encode("utf-8")
            raw = self.ctx.archive.store(
                source_id=self.source_id, content=content, extension="json",
                retrieved_at=retrieved, business_date=as_of,
                source_native_id="membership|%s|%s" % (symbol, index_name),
                request_fp="LOCAL norgatedata index_constituent_timeseries(%s, %s)"
                           % (symbol, index_name),
                content_type="application/json", http_status=None, retry_count=0,
                published_at=self.inventory.get("last_database_update_time"),
                license_note=license_note)
            if not raw["duplicate"]:
                self.raw_objects.append(raw)
            for row in rows:
                m_date = str(row.get("Date", ""))[:10]
                if not m_date or flag_field is None:
                    continue
                self.records.append(build_normalized_record(
                    record_type=RT_UNIVERSE_MEMBERSHIP, source_id=self.source_id,
                    source_native_id="%s|%s|%s" % (symbol, index_name, m_date),
                    raw_object_id=raw["raw_object_id"], retrieved_at=retrieved,
                    observed_at=m_date, effective_at=m_date, available_at=None,
                    ticker=symbol, event_type="INDEX_MEMBERSHIP",
                    payload={"index_name": index_name,
                             "is_member": bool(row.get(flag_field)),
                             "fields_observed": names},
                    entity_mapping_confidence=EM_MATCHED_EXACT,
                    provenance="norgatedata.index_constituent_timeseries"))

        if symbols_ok > 0:
            self._register_success()
        self.inventory["bar_fields_observed"] = fields_observed
        self.inventory["sample_symbols_with_bars"] = symbols_ok
        self.cursor = {"last_collected_as_of": as_of,
                       "bar_window_start": start_date,
                       "newest_bar_date": self.newest_source_event_time}
        if symbols_ok == len(sample) and sample:
            state = SH_HEALTHY
        elif symbols_ok > 0:
            state = SH_DEGRADED
        else:
            state = SH_FAILED
        return self.result(overall_state=state, credential_present=None,
                           entitlement_summary="local vendor database (NDU running)")
