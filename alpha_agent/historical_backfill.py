"""
alpha_agent.historical_backfill — Alpha Agent Stage 6 historical-backfill engine.

A deterministic, resumable, one-time/incremental historical acquisition process
that turns the shallow Stage 2 normalized store into point-in-time-safe history
deep enough to run the first REAL Stage 5 experiments (instead of only data
holds), then a deterministic Stage 1 reopening lane + Stage 5 activation.

Priority 1 (required): Norgate local, survivorship-aware — full daily bars
(adjusted total-return close + unadjusted close + OHLCV + Turnover + Dividend),
point-in-time index-membership spans, security identity/classification and
dividend corporate actions, for the survivorship-free S&P 500 (current & past)
universe over the configured window. Reads through the EXISTING installed
``norgatedata`` integration only — never installs, upgrades, re-exports or
mutates the Norgate installation, and makes no network call of its own.

Priority 2 (entitlement-aware): existing EODHD / SEC / FINRA / FRED access is
audited for entitlement and honestly reported; families whose owned entitlement
cannot yield lookahead-free point-in-time history are recorded as structured
data gaps — never approximated and never lookahead-contaminated.

Hard safety invariants (structural):
  * ACQUISITION + READINESS only. No order/fill/signal/trade-decision/Daily
    Close/model promotion/portfolio or Alpha-Paper-Book change; no PostgreSQL;
    no prediction-service call; no LLM; no code generation or dynamic import.
  * Writes ONLY under the ingestion root (raw + normalized) and the Stage 6
    backfill root. Never inside the repository.
  * Operational ledgers are fingerprinted before and after and must be unchanged.
  * ``available_at`` is preserved as observed and NEVER fabricated; a fiscal
    period-end is NEVER substituted for a public-availability date.
  * Completed batches are idempotent; a COMPLETE run replays without re-writing.

Pure Python standard library + the local norgatedata vendor integration only.
"""
from __future__ import annotations

import csv
import datetime as _dt
import io
import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Callable, Optional

from . import backfill_contracts as bc
from . import runtime_contracts as rc
from .collectors.base import RawArchive
from .source_contracts import (EM_MATCHED_EXACT, EM_UNMATCHED,
                               RT_CORPORATE_ACTION, RT_MARKET_BAR,
                               RT_SECURITY_IDENTITY, RT_UNIVERSE_MEMBERSHIP,
                               build_normalized_record, sha256_hex)

_NORGATE = "norgate_local"
_PROVENANCE_BARS = ("norgatedata.price_timeseries TOTALRETURN "
                    "(survivorship-aware local database)")


# --------------------------------------------------------------------------- #
# Deterministic IO helpers.
# --------------------------------------------------------------------------- #
def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return None


def _read_jsonl(path: Path) -> list:
    out: list = []
    try:
        text = path.read_text(encoding="utf-8-sig")
    except OSError:
        return out
    for line in text.splitlines():
        line = line.strip()
        if line:
            try:
                out.append(json.loads(line))
            except ValueError:
                continue
    return out


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8", newline="\n")
    os.replace(tmp, path)


def _write_json_atomic(path: Path, obj: Any) -> None:
    _write_text_atomic(path, json.dumps(obj, indent=1, sort_keys=True,
                                        default=str))


def _write_jsonl(path: Path, rows: list) -> None:
    _write_text_atomic(path, "".join(rc.canonical_json(r) + "\n" for r in rows))


def _write_csv(path: Path, fieldnames: list, rows: list) -> None:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, lineterminator="\n",
                            extrasaction="ignore")
    writer.writeheader()
    for r in rows:
        writer.writerow({k: _csv_cell(r.get(k)) for k in fieldnames})
    _write_text_atomic(path, buf.getvalue())


def _csv_cell(v: Any) -> Any:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float):
        return "%.10g" % v
    if isinstance(v, (list, tuple, dict)):
        return rc.canonical_json(v)
    return v


def _sha256_file(path: Path) -> str:
    import hashlib
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


# --------------------------------------------------------------------------- #
# Persistent Stage 6 state database.
# --------------------------------------------------------------------------- #
def state_db_path(root: Path) -> Path:
    return root / "state" / "backfill_state.sqlite"


def open_state_db(root: Path, *, create: bool = True) -> sqlite3.Connection:
    path = state_db_path(root)
    if create:
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(path))
    else:
        if not path.exists():
            raise FileNotFoundError("stage6 state db missing: %s" % path)
        conn = sqlite3.connect("file:%s?immutable=1" % path.as_posix(), uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    if create:
        try:
            conn.execute("PRAGMA journal_mode = WAL")
        except sqlite3.DatabaseError:
            pass
        conn.executescript(bc.SCHEMA_SQL)
        conn.execute("INSERT OR IGNORE INTO stage6_meta (key, value) VALUES (?,?)",
                     ("schema_version", bc.STAGE6_SCHEMA_VERSION))
        conn.commit()
    return conn


# --------------------------------------------------------------------------- #
# Coverage statistics (per logical family), built either by scanning the
# existing normalized tree or accumulated as the backfill writes.
# --------------------------------------------------------------------------- #
class FamilyStats:
    """Mutable point-in-time coverage accumulator for one family."""

    __slots__ = ("tickers", "securities", "dates", "months", "quarters",
                 "record_count", "null_available_at", "avail_min", "avail_max",
                 "has_avail", "truncated")

    def __init__(self) -> None:
        self.tickers: set = set()
        self.securities: set = set()
        self.dates: set = set()
        self.months: set = set()
        self.quarters: set = set()
        self.record_count = 0
        self.null_available_at = 0
        self.avail_min: Optional[str] = None
        self.avail_max: Optional[str] = None
        self.has_avail = False
        self.truncated = False

    def add(self, *, ticker, security_id, effective_at, available_at) -> None:
        self.record_count += 1
        if ticker:
            self.tickers.add(str(ticker))
        if security_id:
            self.securities.add(str(security_id))
        eff = str(effective_at or "")[:10]
        if len(eff) == 10:
            self.dates.add(eff)
            self.months.add(eff[:7])
            self.quarters.add("%s-Q%d" % (eff[:4], (int(eff[5:7]) - 1) // 3 + 1))
        if available_at:
            self.has_avail = True
            av = str(available_at)[:10]
            self.avail_min = av if self.avail_min is None else min(self.avail_min, av)
            self.avail_max = av if self.avail_max is None else max(self.avail_max, av)
        else:
            self.null_available_at += 1

    def merge(self, other: "FamilyStats") -> None:
        self.tickers |= other.tickers
        self.securities |= other.securities
        self.dates |= other.dates
        self.months |= other.months
        self.quarters |= other.quarters
        self.record_count += other.record_count
        self.null_available_at += other.null_available_at
        self.has_avail = self.has_avail or other.has_avail
        self.truncated = self.truncated or other.truncated
        for av in (other.avail_min, other.avail_max):
            if av:
                self.avail_min = av if self.avail_min is None else min(self.avail_min, av)
                self.avail_max = av if self.avail_max is None else max(self.avail_max, av)

    def row(self, *, family: str, provider: str, survivorship_safe: bool,
            templates_unlocked: list, blocker: str) -> dict:
        dates = sorted(self.dates)
        pit_usable = bool(self.has_avail or family in (
            "prices", "universe_membership", "security_identity"))
        return {
            "family": family,
            "record_type": bc.FAMILY_RECORD_TYPE.get(family, family),
            "provider": provider,
            "min_effective_date": dates[0] if dates else None,
            "max_effective_date": dates[-1] if dates else None,
            "min_available_at": self.avail_min,
            "max_available_at": self.avail_max,
            "unique_tickers": len(self.tickers),
            "unique_securities": len(self.securities),
            "unique_dates": len(self.dates),
            "unique_months": len(self.months),
            "unique_quarters": len(self.quarters),
            "record_count": self.record_count,
            "null_available_at_count": self.null_available_at,
            "null_available_at_pct": (round(100.0 * self.null_available_at
                                            / self.record_count, 2)
                                      if self.record_count else 0.0),
            "duplicate_prevention_count": 0,
            "has_availability_timestamps": self.has_avail,
            "point_in_time_usable": pit_usable,
            "survivorship_safe": survivorship_safe,
            "templates_unlocked": templates_unlocked,
            "remaining_blocker": blocker,
        }


def scan_family(ingestion_root: Path, family: str, *,
                max_lines: int = 400000) -> FamilyStats:
    """Read-only scan of the existing normalized tree for one family. Dates come
    from the partition path; ticker/availability stats from bounded line reads."""
    rt = bc.FAMILY_RECORD_TYPE.get(family, family)
    stats = FamilyStats()
    base = ingestion_root / "normalized" / rt
    if not base.exists():
        return stats
    read = 0
    for f in sorted(base.rglob("*.jsonl")):
        if read >= max_lines:
            stats.truncated = True
            break
        try:
            text = f.read_text(encoding="utf-8-sig")
        except OSError:
            continue
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            if read >= max_lines:
                stats.truncated = True
                break
            read += 1
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            stats.add(ticker=rec.get("ticker"),
                      security_id=rec.get("security_id"),
                      effective_at=rec.get("effective_at"),
                      available_at=rec.get("available_at"))
    return stats


# --------------------------------------------------------------------------- #
# Normalized partition writer (Stage 2 layout, appended by effective date).
# --------------------------------------------------------------------------- #
class PartitionWriter:
    """Writes normalized records into <ingestion_root>/normalized/<RT>/Y/M/D/
    <run_id>.jsonl, matching the Stage 2 layout the Stage 5 store reads. Dedups
    record_ids within a run; a completed batch is never rewritten."""

    def __init__(self, ingestion_root: Path, run_id: str):
        self.root = ingestion_root / "normalized"
        self.run_id = run_id
        self._seen: set = set()
        self.written = 0
        self.duplicate_prevented = 0

    def write(self, records: list[dict]) -> int:
        buckets: dict[Path, list[str]] = {}
        n = 0
        for rec in records:
            rid = rec.get("record_id")
            if rid in self._seen:
                self.duplicate_prevented += 1
                continue
            self._seen.add(rid)
            eff = str(rec.get("effective_at") or rec.get("retrieved_at")
                      or "")[:10] or "0000-00-00"
            yyyy, mm, dd = (eff.split("-") + ["00", "00"])[:3]
            rel = self.root / rec["record_type"] / yyyy / mm / dd / (
                "%s.jsonl" % self.run_id)
            buckets.setdefault(rel, []).append(rc.canonical_json(rec))
            n += 1
        for path, lines in sorted(buckets.items(), key=lambda kv: str(kv[0])):
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as fh:
                for line in lines:
                    fh.write(line + "\n")
        self.written += n
        return n


# --------------------------------------------------------------------------- #
# Norgate historical adapter (Priority 1). Guarded, local, no network of its own.
# --------------------------------------------------------------------------- #
class NorgateHistoricalAdapter:
    def __init__(self, provider_cfg: dict, *, license_note: str = ""):
        self.cfg = provider_cfg
        self.license_note = license_note or provider_cfg.get("license_note", "")
        self._nd = None
        self.errors: list[dict] = []

    def _import(self):
        if self._nd is not None:
            return self._nd
        try:
            import norgatedata  # noqa: PLC0415 — lazy local vendor import
            self._nd = norgatedata
        except Exception as exc:  # noqa: BLE001
            self.errors.append({"error_type": "VENDOR_IMPORT_UNAVAILABLE",
                                "message": rc.redact("%s: %s" % (
                                    type(exc).__name__, exc))})
            self._nd = None
        return self._nd

    def available(self) -> tuple[bool, str]:
        nd = self._import()
        if nd is None:
            return False, "norgatedata not importable (no install attempted)"
        try:
            running = bool(nd.status())
        except Exception as exc:  # noqa: BLE001
            return False, "norgatedata.status() failed: %s" % type(exc).__name__
        if not running:
            return False, "Norgate Data Updater not running"
        return True, "NDU running (v%s)" % str(getattr(nd, "__version__", "?"))

    def resolve_universe(self, watchlist: str, cap: int,
                         current_watchlist: Optional[str] = None
                         ) -> tuple[list[str], int, int]:
        """Deterministic survivorship-aware universe. Returns
        (symbols, full_universe_size, dropped). All CURRENT members are kept, a
        deterministic (sorted) sample of past/delisted members fills up to cap —
        current members are NEVER substituted for historical membership; the
        drop is reported, never silent."""
        nd = self._import()
        if nd is None:
            return [], 0, 0
        try:
            full = sorted({str(s) for s in nd.watchlist_symbols(watchlist)})
        except Exception as exc:  # noqa: BLE001
            self.errors.append({"error_type": "VENDOR_UNIVERSE_ERROR",
                                "message": rc.redact(str(exc))})
            return [], 0, 0
        current: set = set()
        if current_watchlist:
            try:
                current = {str(s) for s in nd.watchlist_symbols(current_watchlist)}
            except Exception:  # noqa: BLE001
                current = set()
        full_size = len(full)
        if full_size <= cap:
            return full, full_size, 0
        keep = sorted(s for s in full if s in current)
        past = sorted(s for s in full if s not in current)
        room = max(0, cap - len(keep))
        selected = sorted(set(keep) | set(past[:room]))
        return selected, full_size, full_size - len(selected)

    @staticmethod
    def _iso(value: Any) -> Optional[str]:
        if value is None:
            return None
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return str(value)

    def _recarray_rows(self, arr: Any) -> tuple[list[str], list[dict]]:
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

    def _price_series(self, symbol: str, start: str, end: Optional[str]):
        nd = self._nd
        kwargs = dict(start_date=start, timeseriesformat="numpy-recarray")
        if end:
            kwargs["end_date"] = end
        try:
            kwargs2 = dict(kwargs,
                           stock_price_adjustment_setting=(
                               nd.StockPriceAdjustmentType.TOTALRETURN),
                           padding_setting=nd.PaddingType.NONE)
            return nd.price_timeseries(symbol, **kwargs2)
        except TypeError:
            pass
        except Exception as exc:  # noqa: BLE001
            self.errors.append({"error_type": "VENDOR_QUERY_ERROR",
                                "message": rc.redact("price %s: %s" % (
                                    symbol, exc))})
            return None
        try:
            return nd.price_timeseries(symbol, **kwargs)
        except Exception as exc:  # noqa: BLE001
            self.errors.append({"error_type": "VENDOR_QUERY_ERROR",
                                "message": rc.redact("price %s: %s" % (
                                    symbol, exc))})
            return None

    def _identity(self, symbol: str) -> dict:
        nd = self._nd
        out: dict[str, Any] = {}
        for fn in ("assetid", "security_name", "exchange_name", "domicile",
                   "currency", "classification", "subtype1", "base_type",
                   "first_quoted_date", "last_quoted_date"):
            f = getattr(nd, fn, None)
            if not callable(f):
                continue
            try:
                v = f(symbol)
            except Exception:  # noqa: BLE001
                continue
            if v is not None:
                out[fn] = self._iso(v) if hasattr(v, "isoformat") else (
                    v if isinstance(v, (str, int, float, bool)) else str(v))
        # GICS sector classification (best-effort; observed-only).
        for fn, key in (("classification", "gics_sector"),):
            f = getattr(nd, "classification", None)
            if callable(f):
                try:
                    sec = nd.classification(symbol, "GICS", "Name",
                                            classificationlevel=1)
                    if sec:
                        out["gics_sector"] = str(sec)
                except Exception:  # noqa: BLE001
                    pass
        return out

    def _membership_spans(self, symbol: str, index_name: str, start: str
                          ) -> list[dict]:
        nd = self._nd
        try:
            arr = nd.index_constituent_timeseries(
                symbol, index_name, start_date=start,
                timeseriesformat="numpy-recarray")
        except Exception:  # noqa: BLE001
            return []
        try:
            names, rows = self._recarray_rows(arr)
        except Exception:  # noqa: BLE001
            return []
        flag = next((n for n in names if "constituent" in n.lower()), None)
        if not flag:
            return []
        spans: list[dict] = []
        cur_start: Optional[str] = None
        prev_date: Optional[str] = None
        for row in rows:
            d = str(row.get("Date", ""))[:10]
            member = bool(row.get(flag))
            if member and cur_start is None:
                cur_start = d
            if not member and cur_start is not None:
                spans.append({"from": cur_start, "to": prev_date})
                cur_start = None
            prev_date = d
        if cur_start is not None:
            spans.append({"from": cur_start, "to": prev_date})
        return spans

    def fetch_and_normalize(self, symbol: str, *, start: str,
                            end: Optional[str], as_of: str, retrieved: str,
                            index_name: str, archive: RawArchive,
                            families: list[str]) -> dict:
        """Fetch one symbol's full history and normalize to records. Returns
        {"records", "families"} or {"error"}."""
        arr = self._price_series(symbol, start, end)
        if arr is None:
            return {"records": [], "families": {}, "no_data": True}
        try:
            names, rows = self._recarray_rows(arr)
        except Exception as exc:  # noqa: BLE001
            self.errors.append({"error_type": "VENDOR_PARSE_ERROR",
                                "message": rc.redact("%s: %s" % (symbol, exc))})
            return {"records": [], "families": {}, "no_data": True}
        ident = self._identity(symbol)
        security_id = str(ident["assetid"]) if "assetid" in ident else None
        exchange = ident.get("exchange_name")
        mapping = EM_MATCHED_EXACT if security_id else EM_UNMATCHED

        # Compact raw lineage descriptor (content-addressed) — the local vendor
        # DB is the durable raw source; we archive a deterministic fingerprint of
        # exactly what was served, not gigabytes of re-derivable arrays.
        descriptor = {
            "symbol": symbol, "start_date": start, "end_date": end,
            "bar_fields": names, "bar_rows": len(rows),
            "first_bar": str(rows[0].get("Date"))[:10] if rows else None,
            "last_bar": str(rows[-1].get("Date"))[:10] if rows else None,
            "identity": ident,
            "content_hash": sha256_hex(json.dumps(
                {"n": names, "r": rows}, sort_keys=True,
                default=str).encode("utf-8")),
        }
        content = json.dumps(descriptor, sort_keys=True,
                             default=str).encode("utf-8")
        raw = archive.store(
            source_id=_NORGATE, content=content, extension="json",
            retrieved_at=retrieved, business_date=as_of,
            source_native_id="norgate6|%s|%s|%s" % (symbol, start, end or ""),
            request_fp="LOCAL norgatedata price_timeseries(%s, %s..%s) "
                       "TOTALRETURN" % (symbol, start, end or "latest"),
            content_type="application/json", http_status=None, retry_count=0,
            published_at=None, license_note=self.license_note)
        raw_id = raw["raw_object_id"]

        records: list[dict] = []
        # MARKET_BAR — adjusted TR close + unadjusted + OHLCV + Turnover +
        # Dividend, per Priority 1. available_at left null (vendor states no
        # per-bar publication timestamp; NEVER fabricated).
        if "prices" in families:
            for row in rows:
                bar_date = str(row.get("Date", ""))[:10]
                if len(bar_date) != 10:
                    continue
                payload = {k: row.get(k) for k in names}
                payload["adjustment_requested"] = "TOTALRETURN"
                payload["fields_observed"] = names
                records.append(build_normalized_record(
                    record_type=RT_MARKET_BAR, source_id=_NORGATE,
                    source_native_id="%s|%s" % (symbol, bar_date),
                    raw_object_id=raw_id, retrieved_at=retrieved,
                    observed_at=bar_date, effective_at=bar_date,
                    available_at=None, ticker=symbol, security_id=security_id,
                    exchange=exchange, event_type="EOD_BAR", payload=payload,
                    entity_mapping_confidence=mapping,
                    provenance=_PROVENANCE_BARS))
            # CORPORATE_ACTION — dividends where a Dividend field is present.
            if "corporate_actions" in families and "Dividend" in names:
                for row in rows:
                    div = row.get("Dividend")
                    bar_date = str(row.get("Date", ""))[:10]
                    try:
                        divf = float(div)
                    except (TypeError, ValueError):
                        divf = 0.0
                    if divf and len(bar_date) == 10:
                        records.append(build_normalized_record(
                            record_type=RT_CORPORATE_ACTION, source_id=_NORGATE,
                            source_native_id="div|%s|%s" % (symbol, bar_date),
                            raw_object_id=raw_id, retrieved_at=retrieved,
                            observed_at=bar_date, effective_at=bar_date,
                            available_at=None, ticker=symbol,
                            security_id=security_id, event_type="DIVIDEND",
                            payload={"ex_date": bar_date, "dividend": divf,
                                     "period_end": None},
                            entity_mapping_confidence=mapping,
                            provenance="norgatedata.price_timeseries Dividend "
                                       "field (survivorship-aware)"))
        # SECURITY_IDENTITY — one classification snapshot (sector_map source).
        if "security_identity" in families and ident:
            records.append(build_normalized_record(
                record_type=RT_SECURITY_IDENTITY, source_id=_NORGATE,
                source_native_id="identity|%s" % symbol, raw_object_id=raw_id,
                retrieved_at=retrieved, observed_at=retrieved, effective_at=as_of,
                available_at=retrieved, ticker=symbol, security_id=security_id,
                exchange=exchange, event_type="IDENTITY_SNAPSHOT", payload=ident,
                entity_mapping_confidence=mapping,
                provenance="norgatedata identity/classification (observed-only)",
                quality_warnings=["IDENTITY_AS_OBSERVED_ON_RETRIEVAL: current "
                                  "vendor identity, not a historical timeline"]))
        # UNIVERSE_MEMBERSHIP — compact PIT membership spans (survivorship-safe).
        if "universe_membership" in families:
            for span in self._membership_spans(symbol, index_name, start):
                if not span.get("from"):
                    continue
                records.append(build_normalized_record(
                    record_type=RT_UNIVERSE_MEMBERSHIP, source_id=_NORGATE,
                    source_native_id="member|%s|%s|%s" % (
                        symbol, index_name, span["from"]),
                    raw_object_id=raw_id, retrieved_at=retrieved,
                    observed_at=span["from"], effective_at=span["from"],
                    available_at=None, ticker=symbol, security_id=security_id,
                    event_type="INDEX_MEMBERSHIP_SPAN",
                    payload={"index_name": index_name,
                             "member_from": span["from"],
                             "member_to": span.get("to")},
                    entity_mapping_confidence=EM_MATCHED_EXACT,
                    provenance="norgatedata.index_constituent_timeseries "
                               "(point-in-time membership span)"))
        fam_counts: dict[str, int] = {}
        for rec in records:
            fam = _RT_TO_FAMILY.get(rec["record_type"], rec["record_type"])
            fam_counts[fam] = fam_counts.get(fam, 0) + 1
        return {"records": records, "families": fam_counts, "no_data": False,
                "raw_new": 0 if raw["duplicate"] else 1}


_RT_TO_FAMILY = {rt: fam for fam, rt in bc.FAMILY_RECORD_TYPE.items()}


# --------------------------------------------------------------------------- #
# Stage 1 reopening lane.
# --------------------------------------------------------------------------- #
def _read_coverage_map(registry_root: Path) -> list[dict]:
    latest = _read_json(registry_root / "latest.json") or {}
    run_dir = latest.get("run_dir")
    if not run_dir:
        return []
    rp = Path(run_dir) if Path(run_dir).is_absolute() else (registry_root / run_dir)
    path = rp / "research_coverage_map.csv"
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as fh:
            return list(csv.DictReader(fh))
    except OSError:
        return []


def compute_reopen_candidates(coverage_rows: list[dict], *,
                              unlocked_templates: set, data_version: str,
                              prior_data_versions: set, max_candidates: int
                              ) -> list[dict]:
    """Deterministic Stage 1 reopening lane. Returns disposition rows for the
    canonical family threads; only families whose template is genuinely unlocked
    and whose prior verdict was a DATA limitation become NEW_DATA_AVAILABLE."""
    scored: list[dict] = []
    for row in coverage_rows:
        fam = (row.get("information_family") or "").strip()
        template = bc.reopen_information_family_for(fam)
        classification = (row.get("evidence_classification") or "").strip()
        missing = (row.get("required_missing_data") or "").strip()
        try:
            n_exp = int(row.get("unique_experiments") or 0)
            n_hyp = int(row.get("hypotheses") or 0)
        except (TypeError, ValueError):
            n_exp = n_hyp = 0
        established = (n_exp > 0) or (n_hyp > 0)
        data_limited = (classification in bc.REOPEN_ELIGIBLE_CLASSIFICATIONS
                        or bool(missing))
        conclusive = classification in bc.REOPEN_CONCLUSIVE_CLASSIFICATIONS

        if not established:
            continue  # nothing canonical to reopen
        if template is None:
            disp = bc.REOPEN_REJECT_UNSUPPORTED
        elif conclusive and not (classification
                                 in bc.REOPEN_ELIGIBLE_CLASSIFICATIONS):
            # Conclusively resolved/rejected — never reopened for more price rows.
            continue
        elif template not in unlocked_templates:
            disp = bc.REOPEN_STILL_HELD
        elif data_version in prior_data_versions:
            disp = bc.REOPEN_REJECT_DUPLICATE
        elif data_limited:
            disp = bc.REOPEN_NEW_DATA
        else:
            continue  # sufficiently tested + not data-limited: leave closed
        scored.append({
            "hypothesis_id": "hyp_reopen_%s" % fam,
            "information_family": fam,
            "template": template,
            "disposition": disp,
            "prior_classification": classification,
            "required_missing_data": missing,
            "unique_experiments": n_exp,
            "hypotheses": n_hyp,
            "data_version": data_version,
            "statement": bc.CANONICAL_FAMILY_STATEMENT.get(fam, ""),
            "detail": "family thread reopened from Stage 1 coverage map",
        })
    # Priority order: brief order first (price momentum, sector, fundamentals,
    # earnings), NEW_DATA_AVAILABLE ahead of holds, then family name.
    prio = {"price_momentum": 0, "sector_exposure": 1, "short_term_reversal": 2,
            "long_term_reversal": 3, "quality": 4, "profitability": 5,
            "value": 6, "growth": 7, "earnings": 8, "earnings_surprise": 9}
    disp_rank = {bc.REOPEN_NEW_DATA: 0, bc.REOPEN_RESUME_INCOMPLETE: 1,
                 bc.REOPEN_STILL_HELD: 2, bc.REOPEN_REJECT_DUPLICATE: 3,
                 bc.REOPEN_REJECT_UNSUPPORTED: 4}
    scored.sort(key=lambda r: (disp_rank.get(r["disposition"], 9),
                               prio.get(r["information_family"], 50),
                               r["information_family"]))
    # Cap the NEW_DATA_AVAILABLE reopens; keep the rest as recorded dispositions.
    kept: list[dict] = []
    new_data = 0
    for r in scored:
        if r["disposition"] == bc.REOPEN_NEW_DATA:
            if new_data >= max_candidates:
                r = dict(r, disposition=bc.REOPEN_STILL_HELD,
                         detail="deferred: max reopened candidates reached")
            else:
                new_data += 1
        kept.append(r)
    return kept


def reopened_intake_dicts(candidates: list[dict]) -> list[dict]:
    """Turn NEW_DATA_AVAILABLE reopen candidates into Stage 5 intake dicts (same
    shape as a grounded Stage 3 proposal). Statements are the canonical family
    research questions so Stage 5's deterministic template mapper selects the
    intended template — never a fabricated new idea."""
    out: list[dict] = []
    for c in candidates:
        if c.get("disposition") != bc.REOPEN_NEW_DATA:
            continue
        out.append({
            "hypothesis_id": c["hypothesis_id"],
            "title": "Reopened Stage 1 thread: %s" % c["information_family"],
            "text": c.get("statement") or c["information_family"],
            "information_family": c["information_family"],
            "expected_direction": "",
            "required_fields": [],
            "feature_definition": "",
            "universe": "s&p_500_survivorship_free",
            "grounding": "GROUNDED",
            "queue_status": "READY_FOR_DETERMINISTIC_DESIGN_REVIEW",
            "reopened_from": "stage1_coverage_map",
            "prior_classification": c.get("prior_classification"),
        })
    return out


# --------------------------------------------------------------------------- #
# The engine.
# --------------------------------------------------------------------------- #
def run_backfill(cfg: dict, *, mode: str = "backfill", as_of: str = "latest",
                 now_iso: Optional[str] = None,
                 ledger_fingerprint: Optional[Callable[[], dict]] = None,
                 adapter: Optional[Any] = None,
                 universe_override: Optional[list[str]] = None) -> dict:
    """Run one Stage 6 historical-backfill cycle. Injectable clock / adapter /
    universe / ledger fingerprint make the engine deterministic under test."""
    if mode not in bc.MODES:
        return {"terminal": bc.BLOCKED, "status": "UNKNOWN_MODE",
                "problems": ["unknown mode %r" % mode]}
    now = now_iso or bc.now_utc_iso()
    root = Path(cfg["backfill_root"])
    ingestion_root = Path(cfg["ingestion_root"])
    root.mkdir(parents=True, exist_ok=True)
    (root / "runs").mkdir(parents=True, exist_ok=True)
    (root / "state").mkdir(parents=True, exist_ok=True)

    ledgers_before = ledger_fingerprint() if ledger_fingerprint else None
    resolved_as_of = (str(_dt.date.today()) if as_of in (None, "", "latest")
                      else str(as_of))

    dr = cfg.get("date_range") or {}
    date_start = str(dr.get("start"))
    date_end = None if dr.get("end") in (None, "", "latest") else str(dr.get("end"))
    batch_cfg = cfg.get("batch") or {}
    cap = int(batch_cfg.get("max_universe_symbols", 800))
    batch_size = int(batch_cfg.get("ticker_batch_size", 50))
    ucfg = cfg.get("universe") or {}
    watchlist = ucfg.get("survivorship_watchlist", "S&P 500 Current & Past")
    current_wl = ucfg.get("current_watchlist", "S&P 500")
    index_name = ucfg.get("index_name", "S&P 500")
    norgate_families = list((cfg.get("providers", {}).get("norgate_local")
                             or {}).get("families")
                            or bc.PROVIDER_FAMILIES["norgate_local"]["families"])

    # Resolve universe.
    provider_cfg = (cfg.get("providers", {}).get("norgate_local") or {})
    if adapter is None:
        adapter = NorgateHistoricalAdapter(
            provider_cfg, license_note=provider_cfg.get("license_note", ""))
    ng_available, ng_detail = adapter.available()
    if universe_override is not None:
        symbols, full_size, dropped = list(universe_override), \
            len(universe_override), 0
    elif ng_available:
        symbols, full_size, dropped = adapter.resolve_universe(
            watchlist, cap, current_wl)
    else:
        symbols, full_size, dropped = [], 0, 0

    universe_fp = bc.universe_fingerprint(symbols)
    cfg_hash = bc.config_hash(cfg)
    run_id = bc.stage6_run_id(resolved_as_of, cfg_hash, universe_fp)
    run_dir = root / "runs" / run_id

    conn = open_state_db(root)
    try:
        # Idempotent completed run.
        existing = conn.execute("SELECT status FROM backfill_runs WHERE run_id=?",
                                (run_id,)).fetchone()
        if mode == "backfill" and existing and existing["status"] == "COMPLETE" \
                and (run_dir / "run_manifest.json").exists():
            cached = _read_json(run_dir / "run_manifest.json") or {}
            return _load_run_result(root, run_id, cached, idempotent=True)

        # Coverage BEFORE (existing tree; cheap).
        all_families = sorted(set(bc.FAMILY_RECORD_TYPE) | {"sector_map"})
        before_stats = {fam: scan_family(ingestion_root, _cov_family(fam))
                        for fam in all_families}
        coverage_before = _coverage_rows(before_stats, cfg, unlocked=set())

        source_plan = _build_source_plan(cfg, symbols, full_size, dropped,
                                         date_start, date_end, norgate_families,
                                         ng_available, ng_detail)

        if mode == "dry-run":
            return _finish_dry_run(conn, root, run_id, run_dir, now,
                                   resolved_as_of, cfg, cfg_hash, universe_fp,
                                   date_start, date_end, source_plan,
                                   coverage_before, ledgers_before,
                                   ledger_fingerprint)
        if mode == "coverage-report":
            return _finish_coverage_report(root, run_id, coverage_before,
                                           ng_available)

        # ---- backfill mode ----
        conn.execute(
            "INSERT OR REPLACE INTO backfill_runs (run_id, as_of, mode,"
            " config_hash, universe_fingerprint, date_start, date_end, status,"
            " started_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (run_id, resolved_as_of, mode, cfg_hash, universe_fp, date_start,
             date_end, "RUNNING", now))
        conn.commit()

        writer = PartitionWriter(ingestion_root, run_id)
        archive = RawArchive(ingestion_root / "raw", ingestion_root, set(),
                             int(cfg.get("limits", {}).get(
                                 "raw_object_max_bytes", 33554432)))
        backfill_stats = {fam: FamilyStats() for fam in bc.FAMILY_RECORD_TYPE}
        provider_errors: list[dict] = []
        batch_results: list[dict] = []
        entitlement_rows: list[dict] = []
        no_data_symbols = 0

        if ng_available and symbols:
            batches = [symbols[i:i + batch_size]
                       for i in range(0, len(symbols), batch_size)]
            for bi, batch in enumerate(batches):
                tb_id = bc.ticker_batch_id(run_id, _NORGATE, batch, date_start,
                                           date_end or "latest")
                prior = conn.execute(
                    "SELECT status FROM ticker_batches WHERE batch_id=?",
                    (tb_id,)).fetchone()
                if prior and prior["status"] == "COMPLETE":
                    conn.execute(
                        "INSERT INTO resumed_batches (run_id, batch_id,"
                        " resumed_at) VALUES (?,?,?)", (run_id, tb_id, now))
                    conn.commit()
                    continue
                # Accumulate the whole ticker-batch, then write partitions once
                # (each daily date-file is opened once per batch, not once per
                # symbol — avoids millions of file opens on a deep backfill).
                batch_records: list[dict] = []
                for symbol in batch:
                    res = adapter.fetch_and_normalize(
                        symbol, start=date_start, end=date_end,
                        as_of=resolved_as_of, retrieved=now,
                        index_name=index_name, archive=archive,
                        families=norgate_families)
                    if res.get("no_data"):
                        no_data_symbols += 1
                        continue
                    for rec in res["records"]:
                        batch_records.append(rec)
                        fam = _RT_TO_FAMILY.get(rec["record_type"])
                        if fam and fam in backfill_stats:
                            backfill_stats[fam].add(
                                ticker=rec.get("ticker"),
                                security_id=rec.get("security_id"),
                                effective_at=rec.get("effective_at"),
                                available_at=rec.get("available_at"))
                batch_written = writer.write(batch_records)
                conn.execute(
                    "INSERT OR REPLACE INTO ticker_batches (batch_id, run_id,"
                    " provider, ticker_count, tickers_json, date_start,"
                    " date_end, status, records_written, completed_at)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (tb_id, run_id, _NORGATE, len(batch),
                     rc.canonical_json(sorted(batch)), date_start,
                     date_end or "latest", "COMPLETE", batch_written, now))
                conn.commit()
                batch_results.append({"batch_id": tb_id, "provider": _NORGATE,
                                      "ticker_count": len(batch),
                                      "records_written": batch_written,
                                      "status": "COMPLETE"})
            for err in adapter.errors:
                provider_errors.append(dict(err, provider=_NORGATE))
        elif not ng_available:
            provider_errors.append({"provider": _NORGATE,
                                    "error_type": "PROVIDER_UNAVAILABLE",
                                    "message": ng_detail})

        # Priority-2 providers: entitlement-aware honest handling.
        gaps: list[dict] = _priority2_gaps(cfg, resolved_as_of,
                                           entitlement_rows, provider_errors)

        # Persist provider batches / counts / errors / entitlements.
        _persist_backfill_state(conn, run_id, now, backfill_stats,
                                provider_errors, entitlement_rows, writer)

        # Coverage AFTER = before + backfill accumulators.
        after_stats: dict = {}
        for fam in all_families:
            cov_fam = _cov_family(fam)
            merged = FamilyStats()
            merged.merge(before_stats[fam])
            if cov_fam in backfill_stats:
                merged.merge(backfill_stats[cov_fam])
            after_stats[fam] = merged

        # Readiness → templates unlocked.
        readiness = _evaluate_all_readiness(cfg, after_stats)
        unlocked = set()
        for r in readiness.values():
            unlocked |= set(r.get("unlocks", []))
        coverage_after = _coverage_rows(after_stats, cfg, unlocked=unlocked,
                                        readiness=readiness)
        coverage_delta = _coverage_delta(coverage_before, coverage_after)
        pit_audit = _pit_audit_rows(after_stats)

        # Reopen lane.
        registry_root = Path(cfg["stage1_registry_root"])
        cov_map = _read_coverage_map(registry_root)
        dv = bc.data_version_id(universe_fp, date_start,
                                date_end or resolved_as_of, writer.written)
        max_reopen = int((cfg.get("reopen") or {}).get("max_candidates", 3))
        prior_dv = {row["data_version"] for row in conn.execute(
            "SELECT DISTINCT data_version FROM reopened_hypotheses"
            " WHERE data_version IS NOT NULL")}
        reopen = compute_reopen_candidates(
            cov_map, unlocked_templates=unlocked, data_version=dv,
            prior_data_versions=prior_dv, max_candidates=max_reopen)
        for r in reopen:
            conn.execute(
                "INSERT INTO reopened_hypotheses (run_id, hypothesis_id,"
                " information_family, template, disposition, prior_classification,"
                " data_version, detail, recorded_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (run_id, r["hypothesis_id"], r["information_family"],
                 r["template"], r["disposition"], r.get("prior_classification"),
                 r["data_version"], str(r.get("detail"))[:400], now))
        conn.commit()

        reopened_intake = reopened_intake_dicts(reopen)
        activation = {
            "run_id": run_id, "data_version": dv,
            "templates_unlocked": sorted(unlocked),
            "reopened_new_data": [r["hypothesis_id"] for r in reopen
                                  if r["disposition"] == bc.REOPEN_NEW_DATA],
            "reopened_intake_count": len(reopened_intake),
            "supplier_hypothesis_status": "DATA_HOLD (unchanged; no owned "
            "point-in-time supplier relationship map / consensus estimates)",
            "stage5_should_run": bool(reopened_intake),
        }

        # Ledger immutability.
        ledgers_after = ledger_fingerprint() if ledger_fingerprint else None
        ledgers_ok = (ledgers_before is None) or (ledgers_before == ledgers_after)

        terminal, status = _terminal(writer.written, unlocked, gaps,
                                     ng_available, ledgers_ok)

        result = {
            "stage": bc.STAGE6_STAGE,
            "schema_version": bc.STAGE6_SCHEMA_VERSION,
            "engine_version": bc.STAGE6_ENGINE_VERSION,
            "run_id": run_id, "as_of": resolved_as_of, "mode": mode,
            "generated_at": now, "config_hash": cfg_hash,
            "universe_fingerprint": universe_fp,
            "universe_size": len(symbols), "universe_full_size": full_size,
            "universe_dropped": dropped, "date_start": date_start,
            "date_end": date_end or resolved_as_of,
            "records_written": writer.written,
            "duplicate_prevented": writer.duplicate_prevented,
            "no_data_symbols": no_data_symbols,
            "terminal": terminal, "status": status,
            "ledgers_unchanged": ledgers_ok,
            "norgate_available": ng_available, "norgate_detail": ng_detail,
            "source_plan": source_plan,
            "coverage_before": coverage_before,
            "coverage_after": coverage_after,
            "coverage_delta": coverage_delta,
            "pit_audit": pit_audit,
            "readiness": readiness,
            "templates_unlocked": sorted(unlocked),
            "data_gaps": gaps,
            "entitlements": entitlement_rows,
            "provider_errors": provider_errors,
            "batch_results": batch_results,
            "reopened": reopen,
            "reopened_intake": reopened_intake,
            "experiment_activation": activation,
            "data_version": dv,
        }
        _write_run_package(root, run_id, cfg, result)
        result["run_dir"] = str(run_dir)

        # Persist stable reopened-hypotheses handoff for Stage 5 (read by
        # experiment_factory.read_reopened_hypotheses via cfg path).
        handoff = cfg.get("reopened_hypotheses_output")
        if handoff:
            _write_jsonl(Path(handoff), reopened_intake)

        _snapshot_coverage(conn, run_id, "before", coverage_before, now)
        _snapshot_coverage(conn, run_id, "after", coverage_after, now)
        conn.execute(
            "UPDATE backfill_runs SET status=?, terminal=?, records_written=?,"
            " finished_at=? WHERE run_id=?",
            ("COMPLETE", terminal, writer.written, now, run_id))
        conn.commit()

        _write_json_atomic(root / "latest.json", {
            "run_id": run_id, "as_of": resolved_as_of, "mode": mode,
            "generated_at": now, "terminal": terminal, "status": status,
            "run_dir": "runs/%s" % run_id,
            "records_written": writer.written,
            "templates_unlocked": sorted(unlocked),
            "data_version": dv,
            "reopened_new_data": activation["reopened_new_data"]})
        return result
    finally:
        conn.close()


def _cov_family(fam: str) -> str:
    """sector_map is a logical view over SECURITY_IDENTITY."""
    return "security_identity" if fam == "sector_map" else fam


def _build_source_plan(cfg, symbols, full_size, dropped, date_start, date_end,
                       norgate_families, ng_available, ng_detail) -> dict:
    plan = {"norgate_local": {
        "priority": 1, "available": ng_available, "detail": ng_detail,
        "families": norgate_families,
        "universe_selected": len(symbols),
        "universe_full_survivorship_free": full_size,
        "universe_dropped_bounded": dropped,
        "date_start": date_start, "date_end": date_end or "latest",
        "survivorship_safe": True,
        "note": "Norgate S&P 500 Current & Past (includes delisted); current "
                "members never substituted for historical membership; bounded "
                "subset drop is reported, not silent."}}
    for name, pcfg in (cfg.get("providers") or {}).items():
        if name == "norgate_local":
            continue
        plan[name] = {
            "priority": bc.PROVIDER_FAMILIES.get(name, {}).get("priority", 2),
            "enabled": bool((pcfg or {}).get("enabled")),
            "families": list((pcfg or {}).get("families")
                             or bc.PROVIDER_FAMILIES.get(name, {}).get(
                                 "families", [])),
            "acquisition": "entitlement_audit_only",
            "note": "Priority-2 historical acquisition is entitlement-gated and "
                    "point-in-time-guarded; families without lookahead-free "
                    "availability timestamps are reported as data gaps, never "
                    "approximated."}
    return plan


def _priority2_gaps(cfg, as_of, entitlement_rows, provider_errors) -> list[dict]:
    """Honest data gaps for Priority-2 families not acquired this run. Never
    fabricates records or lookahead timestamps."""
    gaps: list[dict] = []
    for name, pcfg in (cfg.get("providers") or {}).items():
        if name == "norgate_local" or not (pcfg or {}).get("enabled"):
            continue
        for fam in ((pcfg or {}).get("families")
                    or bc.PROVIDER_FAMILIES.get(name, {}).get("families", [])):
            if fam in ("corporate_actions",):
                continue
            detail = ((pcfg or {}).get("gap_detail")
                      or "owned entitlement does not yield lookahead-free "
                         "point-in-time history in this bounded run")
            gaps.append({"provider": name, "family": fam,
                         "gap_reason": "PRIORITY2_ENTITLEMENT_OR_PIT_LIMITED",
                         "detail": detail})
            entitlement_rows.append({"provider": name, "family": fam,
                                     "state": "NOT_ACQUIRED_THIS_RUN",
                                     "detail": detail})
    return gaps


def _evaluate_all_readiness(cfg, after_stats) -> dict:
    gates = bc.resolved_readiness(cfg)
    out: dict = {}
    for name, gate in gates.items():
        fam_cov: dict = {}
        for fam in gate.get("families", []):
            stats = after_stats.get(fam if fam != "sector_map" else "sector_map")
            if stats is None and fam == "sector_map":
                stats = after_stats.get("sector_map")
            if stats is None:
                stats = after_stats.get(fam)
            fam_cov[fam] = stats.row(family=fam, provider="",
                                     survivorship_safe=(fam == "prices"),
                                     templates_unlocked=[], blocker="") \
                if stats else {}
        out[name] = bc.evaluate_readiness(gate, fam_cov)
    return out


def _coverage_rows(stats: dict, cfg: dict, *, unlocked: set,
                   readiness: Optional[dict] = None) -> list[dict]:
    prov_by_fam = {}
    for prov, meta in bc.PROVIDER_FAMILIES.items():
        for fam in meta["families"]:
            prov_by_fam.setdefault(fam, prov)
    # Which readiness family (and its blocker) references each logical family.
    blocker_by_fam: dict = {}
    unlocked_by_fam: dict = {}
    if readiness:
        gates = bc.resolved_readiness(cfg)
        for rname, res in readiness.items():
            for fam in gates.get(rname, {}).get("families", []):
                if not res.get("usable") and fam not in blocker_by_fam:
                    blocker_by_fam[fam] = res.get("blocker")
                if res.get("usable"):
                    unlocked_by_fam.setdefault(fam, []).extend(
                        res.get("unlocks", []))
    rows: list[dict] = []
    for fam in sorted(stats):
        st = stats[fam]
        survivorship = fam in ("prices", "universe_membership",
                               "security_identity", "sector_map",
                               "corporate_actions")
        tmpl = sorted(set(unlocked_by_fam.get(fam, [])))
        rows.append(st.row(
            family=fam, provider=prov_by_fam.get(_cov_family(fam), ""),
            survivorship_safe=survivorship, templates_unlocked=tmpl,
            blocker=blocker_by_fam.get(fam, "")))
    return rows


def _coverage_delta(before: list[dict], after: list[dict]) -> list[dict]:
    bmap = {r["family"]: r for r in before}
    out: list[dict] = []
    for a in after:
        b = bmap.get(a["family"], {})
        out.append({
            "family": a["family"], "record_type": a["record_type"],
            "record_count_before": b.get("record_count", 0),
            "record_count_after": a.get("record_count", 0),
            "record_count_added": a.get("record_count", 0)
            - b.get("record_count", 0),
            "unique_dates_before": b.get("unique_dates", 0),
            "unique_dates_after": a.get("unique_dates", 0),
            "date_min_after": a.get("min_effective_date"),
            "date_max_after": a.get("max_effective_date"),
            "templates_unlocked": a.get("templates_unlocked"),
        })
    return out


def _pit_audit_rows(stats: dict) -> list[dict]:
    out: list[dict] = []
    for fam in sorted(stats):
        st = stats[fam]
        if st.record_count == 0:
            continue
        expected_null = fam in ("prices", "universe_membership",
                                "corporate_actions")
        out.append({
            "family": fam,
            "record_type": bc.FAMILY_RECORD_TYPE.get(fam, fam),
            "record_count": st.record_count,
            "null_available_at_count": st.null_available_at,
            "null_available_at_pct": (round(100.0 * st.null_available_at
                                            / st.record_count, 2)
                                      if st.record_count else 0.0),
            "has_availability_timestamps": st.has_avail,
            "availability_expected_null": expected_null,
            "period_end_substituted_for_availability": False,
            "verdict": ("OK_NULL_BY_DESIGN" if expected_null and not st.has_avail
                        else ("OK_WITH_AVAILABILITY" if st.has_avail
                              else "AVAILABILITY_ABSENT_NOT_FABRICATED")),
        })
    return out


def _terminal(records_written, unlocked, gaps, ng_available, ledgers_ok):
    if not ledgers_ok:
        return bc.BLOCKED, "LEDGER_MUTATION_DETECTED"
    if not ng_available and records_written == 0:
        return bc.BLOCKED, "NORGATE_UNAVAILABLE"
    if records_written == 0:
        return bc.NO_NEW_DATA, "NO_RECORDS_WRITTEN"
    if unlocked and gaps:
        return bc.PARTIAL, "TEMPLATES_UNLOCKED_WITH_REMAINING_GAPS"
    if unlocked:
        return bc.READY, "TEMPLATES_UNLOCKED"
    return bc.DATA_HOLD, "NO_TEMPLATE_UNLOCKED"


def _persist_backfill_state(conn, run_id, now, backfill_stats, provider_errors,
                            entitlement_rows, writer) -> None:
    for fam, st in backfill_stats.items():
        if st.record_count == 0:
            continue
        prov = None
        for p, meta in bc.PROVIDER_FAMILIES.items():
            if fam in meta["families"]:
                prov = p
                break
        conn.execute(
            "INSERT INTO normalized_counts (run_id, family, record_type,"
            " provider, record_count, duplicate_prevention_count)"
            " VALUES (?,?,?,?,?,?)",
            (run_id, fam, bc.FAMILY_RECORD_TYPE.get(fam, fam), prov,
             st.record_count, writer.duplicate_prevented))
        dates = sorted(st.dates)
        conn.execute(
            "INSERT INTO date_ranges (run_id, family, record_type,"
            " min_effective_date, max_effective_date) VALUES (?,?,?,?,?)",
            (run_id, fam, bc.FAMILY_RECORD_TYPE.get(fam, fam),
             dates[0] if dates else None, dates[-1] if dates else None))
        conn.execute(
            "INSERT OR REPLACE INTO provider_batches (batch_id, run_id, provider,"
            " family, status, records_written, started_at, finished_at)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (bc.provider_batch_id(run_id, prov or "", fam), run_id, prov or "",
             fam, "COMPLETE", st.record_count, now, now))
    for err in provider_errors:
        conn.execute(
            "INSERT INTO provider_errors (run_id, provider, error_type, message,"
            " recorded_at) VALUES (?,?,?,?,?)",
            (run_id, err.get("provider", "?"), err.get("error_type", "ERROR"),
             str(err.get("message"))[:500], now))
    for ent in entitlement_rows:
        conn.execute(
            "INSERT INTO entitlement_blocks (run_id, provider, family, state,"
            " detail, recorded_at) VALUES (?,?,?,?,?,?)",
            (run_id, ent.get("provider"), ent.get("family"), ent.get("state"),
             str(ent.get("detail"))[:400], now))
    conn.commit()


def _snapshot_coverage(conn, run_id, phase, rows, now) -> None:
    for r in rows:
        conn.execute(
            "INSERT INTO coverage_snapshots (run_id, phase, family, record_type,"
            " record_count, min_effective_date, max_effective_date,"
            " point_in_time_usable, survivorship_safe) VALUES (?,?,?,?,?,?,?,?,?)",
            (run_id, phase, r["family"], r["record_type"], r["record_count"],
             r.get("min_effective_date"), r.get("max_effective_date"),
             1 if r.get("point_in_time_usable") else 0,
             1 if r.get("survivorship_safe") else 0))
    conn.commit()


# --------------------------------------------------------------------------- #
# Run-package writer.
# --------------------------------------------------------------------------- #
def _write_run_package(root: Path, run_id: str, cfg: dict, result: dict) -> Path:
    run_dir = root / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    _write_json_atomic(run_dir / "stage6_input.json", {
        "run_id": run_id, "as_of": result.get("as_of"),
        "generated_at": result.get("generated_at"),
        "config_hash": result.get("config_hash"),
        "config": bc.redact_config(cfg),
        "universe_fingerprint": result.get("universe_fingerprint"),
        "date_start": result.get("date_start"),
        "date_end": result.get("date_end")})
    _write_json_atomic(run_dir / "provider_entitlements.json", {
        "norgate_available": result.get("norgate_available"),
        "norgate_detail": result.get("norgate_detail"),
        "entitlements": result.get("entitlements"),
        "provider_errors": result.get("provider_errors")})
    _write_json_atomic(run_dir / "source_plan.json", result.get("source_plan"))
    _write_jsonl(run_dir / "batch_results.jsonl", result.get("batch_results"))
    _write_csv(run_dir / "normalized_record_counts.csv",
               ["family", "record_type", "record_count", "unique_tickers",
                "unique_dates", "min_effective_date", "max_effective_date"],
               [{"family": r["family"], "record_type": r["record_type"],
                 "record_count": r["record_count"],
                 "unique_tickers": r["unique_tickers"],
                 "unique_dates": r["unique_dates"],
                 "min_effective_date": r["min_effective_date"],
                 "max_effective_date": r["max_effective_date"]}
                for r in result.get("coverage_after", [])])
    _write_csv(run_dir / "coverage_before.csv", bc.COVERAGE_COLUMNS,
               result.get("coverage_before"))
    _write_csv(run_dir / "coverage_after.csv", bc.COVERAGE_COLUMNS,
               result.get("coverage_after"))
    _write_csv(run_dir / "coverage_delta.csv",
               ["family", "record_type", "record_count_before",
                "record_count_after", "record_count_added",
                "unique_dates_before", "unique_dates_after", "date_min_after",
                "date_max_after", "templates_unlocked"],
               result.get("coverage_delta"))
    _write_csv(run_dir / "point_in_time_audit.csv",
               ["family", "record_type", "record_count",
                "null_available_at_count", "null_available_at_pct",
                "has_availability_timestamps", "availability_expected_null",
                "period_end_substituted_for_availability", "verdict"],
               result.get("pit_audit"))
    _write_jsonl(run_dir / "unresolved_data_gaps.jsonl", result.get("data_gaps"))
    _write_jsonl(run_dir / "reopened_hypotheses.jsonl", result.get("reopened"))
    _write_json_atomic(run_dir / "experiment_activation.json",
                       result.get("experiment_activation"))
    _write_text_atomic(run_dir / "stage6_backfill_report.md",
                       _render_report_md(result))

    file_hashes = {}
    for name in bc.REQUIRED_RUN_FILES:
        if name == "run_manifest.json":
            continue
        p = run_dir / name
        if p.exists():
            file_hashes[name] = _sha256_file(p)
    manifest = {
        "stage": bc.STAGE6_STAGE, "schema_version": bc.STAGE6_SCHEMA_VERSION,
        "engine_version": bc.STAGE6_ENGINE_VERSION,
        "contract_version": bc.BACKFILL_CONTRACT_VERSION,
        "run_id": run_id, "generated_at": result.get("generated_at"),
        "terminal": result.get("terminal"), "status": result.get("status"),
        "records_written": result.get("records_written"),
        "templates_unlocked": result.get("templates_unlocked"),
        "data_version": result.get("data_version"),
        "universe_fingerprint": result.get("universe_fingerprint"),
        "required_files": list(bc.REQUIRED_RUN_FILES),
        "file_hashes": file_hashes,
    }
    _write_json_atomic(run_dir / "run_manifest.json", manifest)
    return run_dir


def _render_report_md(result: dict) -> str:
    lines = ["# Alpha Agent Stage 6 historical backfill & readiness report", "",
             "- Run ID: %s" % result.get("run_id"),
             "- Terminal: %s (%s)" % (result.get("terminal"),
                                      result.get("status")),
             "- Generated: %s" % result.get("generated_at"),
             "- Window: %s .. %s" % (result.get("date_start"),
                                     result.get("date_end")),
             "- Universe: %s selected of %s survivorship-free (dropped %s, "
             "bounded)" % (result.get("universe_size"),
                           result.get("universe_full_size"),
                           result.get("universe_dropped")),
             "- Records written: %s" % result.get("records_written"),
             "- Templates unlocked: %s" %
             (", ".join(result.get("templates_unlocked") or []) or "none"),
             "- Operational ledgers unchanged: %s" %
             result.get("ledgers_unchanged"), "",
             "## Coverage after (by family)", ""]
    for r in result.get("coverage_after", []):
        if r.get("record_count"):
            lines.append("- %s [%s]: %s records, %s..%s, %s tickers, PIT=%s, "
                         "survivorship_safe=%s%s" % (
                             r["family"], r["record_type"], r["record_count"],
                             r.get("min_effective_date"),
                             r.get("max_effective_date"), r["unique_tickers"],
                             r["point_in_time_usable"], r["survivorship_safe"],
                             (" — blocker: %s" % r["remaining_blocker"])
                             if r.get("remaining_blocker") else ""))
    lines += ["", "## Readiness gates", ""]
    for name, res in (result.get("readiness") or {}).items():
        lines.append("- %s: %s%s" % (
            name, "USABLE" if res.get("usable") else "NOT READY",
            "" if res.get("usable") else " — %s" % res.get("blocker")))
    lines += ["", "## Stage 1 reopening lane", ""]
    for r in result.get("reopened", []):
        lines.append("- `%s` [%s] → **%s** (prior %s)" % (
            r.get("hypothesis_id"), r.get("template"), r.get("disposition"),
            r.get("prior_classification")))
    lines += ["", "## Data gaps (unresolved)", ""]
    for g in result.get("data_gaps", []):
        lines.append("- %s/%s → %s: %s" % (g.get("provider"), g.get("family"),
                                           g.get("gap_reason"),
                                           str(g.get("detail"))[:160]))
    lines += ["", "## Supplier hypothesis", "",
              "- hyp_supplier-surprise-readthrough remains DATA_HOLD: no owned "
              "point-in-time supplier→customer relationship map or supplier "
              "consensus estimates; not approximated, not inferred.", "",
              "## Safety", "",
              "- Acquisition + readiness only: no order/fill/signal/decision/"
              "model/portfolio change; no DB; no prediction service; no LLM.",
              "- available_at preserved as observed, never fabricated; period-"
              "end never substituted for availability.",
              "- Operational ledgers unchanged: %s." %
              result.get("ledgers_unchanged"), ""]
    return "\n".join(lines)


def _finish_dry_run(conn, root, run_id, run_dir, now, as_of, cfg, cfg_hash,
                    universe_fp, date_start, date_end, source_plan,
                    coverage_before, ledgers_before, ledger_fingerprint) -> dict:
    """Dry-run: plan + current coverage only. Writes a plan package under the
    backfill root; writes NOTHING to the ingestion tree."""
    conn.execute(
        "INSERT OR REPLACE INTO backfill_runs (run_id, as_of, mode, config_hash,"
        " universe_fingerprint, date_start, date_end, status, terminal,"
        " started_at, finished_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (run_id, as_of, "dry-run", cfg_hash, universe_fp, date_start,
         date_end or "latest", "DRY_RUN", bc.DRY_RUN, now, now))
    conn.commit()
    run_dir.mkdir(parents=True, exist_ok=True)
    _write_json_atomic(run_dir / "source_plan.json", source_plan)
    _write_json_atomic(run_dir / "stage6_input.json", {
        "run_id": run_id, "as_of": as_of, "mode": "dry-run",
        "config": bc.redact_config(cfg), "date_start": date_start,
        "date_end": date_end or "latest",
        "universe_fingerprint": universe_fp})
    _write_csv(run_dir / "coverage_before.csv", bc.COVERAGE_COLUMNS,
               coverage_before)
    ledgers_after = ledger_fingerprint() if ledger_fingerprint else None
    return {"terminal": bc.DRY_RUN, "status": "PLAN_ONLY", "run_id": run_id,
            "run_dir": str(run_dir), "mode": "dry-run", "source_plan": source_plan,
            "coverage_before": coverage_before,
            "ledgers_unchanged": (ledgers_before is None) or
            (ledgers_before == ledgers_after)}


def _finish_coverage_report(root, run_id, coverage_before, ng_available) -> dict:
    return {"terminal": bc.VERIFIED if ng_available else bc.PARTIAL,
            "status": "COVERAGE_REPORT", "run_id": run_id,
            "coverage": coverage_before}


def _load_run_result(root: Path, run_id: str, manifest: dict,
                     idempotent: bool = False) -> dict:
    run_dir = root / "runs" / run_id
    return {
        "run_id": run_id, "run_dir": str(run_dir),
        "terminal": manifest.get("terminal"), "status": manifest.get("status"),
        "records_written": manifest.get("records_written"),
        "templates_unlocked": manifest.get("templates_unlocked") or [],
        "data_version": manifest.get("data_version"),
        "generated_at": manifest.get("generated_at"),
        "idempotent_replay": idempotent,
        "coverage_after": [],
        "reopened": _read_jsonl(run_dir / "reopened_hypotheses.jsonl"),
        "reopened_intake": [],
        "experiment_activation": _read_json(
            run_dir / "experiment_activation.json") or {},
    }


# --------------------------------------------------------------------------- #
# Verify (read-only; writes nothing, no network).
# --------------------------------------------------------------------------- #
def verify_backfill(cfg: dict, *, run_id: Optional[str] = None,
                    ledger_fingerprint: Optional[Callable[[], dict]] = None
                    ) -> dict:
    root = Path(cfg["backfill_root"])
    problems: list[str] = []
    checks: dict[str, Any] = {}
    latest = _read_json(root / "latest.json") or {}
    rid = run_id or latest.get("run_id")
    if not rid:
        return {"terminal": bc.BLOCKED, "status": "NO_LATEST_PACKAGE",
                "problems": ["no latest.json / run_id"]}
    run_dir = root / "runs" / rid
    manifest = _read_json(run_dir / "run_manifest.json") or {}
    if not manifest:
        problems.append("run_manifest.json missing for %s" % rid)
    for name in bc.REQUIRED_RUN_FILES:
        if not (run_dir / name).exists():
            problems.append("missing required file: %s" % name)
    for name, want in (manifest.get("file_hashes") or {}).items():
        p = run_dir / name
        if p.exists() and _sha256_file(p) != want:
            problems.append("hash mismatch: %s" % name)
    try:
        conn = open_state_db(root, create=False)
        ic = conn.execute("PRAGMA integrity_check").fetchone()
        checks["integrity_check"] = ic[0] if ic else "unknown"
        if checks["integrity_check"] != "ok":
            problems.append("state db integrity: %s" % checks["integrity_check"])
        have = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        for t in bc.REQUIRED_TABLES:
            if t not in have:
                problems.append("missing table: %s" % t)
        for r in conn.execute("SELECT disposition FROM reopened_hypotheses"
                              " WHERE run_id=?", (rid,)):
            if r[0] not in bc.REOPEN_DISPOSITIONS:
                problems.append("invalid reopen disposition: %s" % r[0])
        conn.close()
    except (FileNotFoundError, sqlite3.DatabaseError) as exc:
        problems.append("state db unreadable: %s" % exc)
    # Point-in-time honesty guard: audit rows must never claim a substitution.
    for row in _read_pit_audit(run_dir):
        if str(row.get("period_end_substituted_for_availability")).lower() in (
                "true", "1"):
            problems.append("point-in-time audit reports a period-end "
                            "substitution (must never happen)")
    if ledger_fingerprint is not None:
        checks["ledger_files"] = len(ledger_fingerprint())
    checks["problems"] = problems
    checks["run_id"] = rid
    if problems:
        return {"terminal": bc.BLOCKED, "status": "VERIFY_FAILED", "run_id": rid,
                "problems": problems, "checks": checks}
    return {"terminal": bc.VERIFIED, "status": "VERIFY_OK", "run_id": rid,
            "checks": checks}


def _read_pit_audit(run_dir: Path) -> list[dict]:
    path = run_dir / "point_in_time_audit.csv"
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as fh:
            return list(csv.DictReader(fh))
    except OSError:
        return []


# --------------------------------------------------------------------------- #
# Report model for the Stage 4 "Historical Data & Experiment Readiness" section.
# --------------------------------------------------------------------------- #
def backfill_report_model(result: dict) -> dict:
    """Deterministic model for the Stage 4 report renderer. All numbers from
    Python; no LLM."""
    cov_after = result.get("coverage_after") or []
    families = [{"family": r["family"], "record_count": r["record_count"],
                 "date_min": r.get("min_effective_date"),
                 "date_max": r.get("max_effective_date"),
                 "tickers": r.get("unique_tickers"),
                 "pit_usable": r.get("point_in_time_usable"),
                 "survivorship_safe": r.get("survivorship_safe")}
                for r in cov_after if r.get("record_count")]
    reopened = result.get("reopened") or []
    return {
        "run_id": result.get("run_id"),
        "terminal": result.get("terminal"),
        "status": result.get("status"),
        "date_start": result.get("date_start"),
        "date_end": result.get("date_end"),
        "records_written": result.get("records_written"),
        "universe_size": result.get("universe_size"),
        "universe_full_size": result.get("universe_full_size"),
        "universe_dropped": result.get("universe_dropped"),
        "templates_unlocked": result.get("templates_unlocked") or [],
        "families": families,
        "readiness": {k: v.get("usable") for k, v in
                      (result.get("readiness") or {}).items()},
        "readiness_blockers": {k: v.get("blocker") for k, v in
                               (result.get("readiness") or {}).items()
                               if not v.get("usable")},
        "data_gaps": [{"provider": g.get("provider"), "family": g.get("family"),
                       "detail": g.get("detail")}
                      for g in (result.get("data_gaps") or [])],
        "reopened": [{"hypothesis_id": r.get("hypothesis_id"),
                      "template": r.get("template"),
                      "disposition": r.get("disposition")} for r in reopened],
        "reopened_new_data": [r.get("hypothesis_id") for r in reopened
                              if r.get("disposition") == bc.REOPEN_NEW_DATA],
        "supplier_hypothesis_status": (result.get("experiment_activation") or {})
        .get("supplier_hypothesis_status",
             "DATA_HOLD (no owned point-in-time supplier data)"),
        "norgate_available": result.get("norgate_available"),
        "data_version": result.get("data_version"),
        "evidence_paths": [{"label": "Stage 6 run dir",
                            "path": result.get("run_dir")}],
    }


def latest_report_model(cfg: dict, fresh_result: Optional[dict] = None
                        ) -> Optional[dict]:
    if fresh_result:
        return backfill_report_model(fresh_result)
    root = cfg.get("stage6_backfill_root") or cfg.get("backfill_root")
    if not root:
        return None
    latest = _read_json(Path(root) / "latest.json") or {}
    rid = latest.get("run_id")
    if not rid:
        return None
    run_dir = Path(root) / "runs" / rid
    manifest = _read_json(run_dir / "run_manifest.json") or {}
    result = {
        "run_id": rid, "run_dir": str(run_dir),
        "terminal": latest.get("terminal") or manifest.get("terminal"),
        "status": latest.get("status") or manifest.get("status"),
        "date_start": manifest.get("date_start"),
        "records_written": latest.get("records_written")
        or manifest.get("records_written"),
        "templates_unlocked": latest.get("templates_unlocked")
        or manifest.get("templates_unlocked") or [],
        "data_version": latest.get("data_version"),
        "coverage_after": _read_coverage_csv(run_dir / "coverage_after.csv"),
        "reopened": _read_jsonl(run_dir / "reopened_hypotheses.jsonl"),
        "data_gaps": _read_jsonl(run_dir / "unresolved_data_gaps.jsonl"),
        "experiment_activation": _read_json(
            run_dir / "experiment_activation.json") or {},
        "readiness": {},
    }
    return backfill_report_model(result)


def _read_coverage_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as fh:
            rows = list(csv.DictReader(fh))
    except OSError:
        return []
    for r in rows:
        for k in ("record_count", "unique_tickers", "unique_dates"):
            try:
                r[k] = int(r.get(k) or 0)
            except (TypeError, ValueError):
                r[k] = 0
        for k in ("point_in_time_usable", "survivorship_safe"):
            r[k] = str(r.get(k)).lower() in ("true", "1")
    return rows
