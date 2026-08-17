r"""Release 28 — the canonical EVENT FABRIC owner (composition + immutable store).

WHAT THIS OWNS
--------------
ONE append-only, point-in-time, idempotent store of normalized events, and the ONE
per-source freshness/watermark model that says how current each lane is.

It composes and never re-derives:

* ``engine.event_fabric``   — the event contract, authority table and novelty rules.
* ``api.source_capability`` — which sources exist and what they are allowed to decide.
* ``api.data_freshness``    — the cadence-aware freshness classifier (a slow source is
                              not "broken" because it did not update this minute).

TWO INGESTION LANES, ONE CONTRACT
---------------------------------
``RESEARCH_CORPUS`` reads the Stage-2 / Stage-3.5 normalized record trees that the
bounded collectors already produce. ``LIVE_ADAPTER`` fetches the two near-real-time
sources those collectors do not run at operational speed: delayed market quotes
(``engine.market_data``) and GDELT news metadata. Both produce the SAME normalized
event; there is exactly one event store and one orchestration path over it.

IMMUTABILITY
------------
Events are appended to date-partitioned JSONL and never rewritten. A correction or a
material update is a NEW event that SUPERSEDES the earlier one. Re-ingesting the same
raw event is a no-op decided by content-addressed idempotency key, not by timestamps.

SAFETY
------
It creates no order, confirms no target, approves no proposal, promotes no model and
mutates no operational store. Provider access is read-only, bounded and only for
sources already authorized; it purchases nothing and upgrades no plan.
"""
from __future__ import annotations

import json
import os
import re
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from paper_trader.api import data_freshness as dfresh
from paper_trader.api import source_capability as scap
from paper_trader.engine import event_fabric as ek

PHASE = "RELEASE28"
COMPOSITION_OWNER = "api.event_fabric"
SCHEMA_VERSION = "1.0.0"

FABRIC_DIR_ENV = "PAPER_TRADER_EVENT_FABRIC_DIR"
_DEFAULT_FABRIC_DIR = Path(r"D:\Stock_Prediction_app_data\event_fabric")

INGESTION_ROOT_ENV = scap.INGESTION_ROOT_ENV
NEWS_ROOT_ENV = scap.NEWS_ROOT_ENV

#: Bounds. The fabric is an OPERATIONAL lane, not a history rebuild: it reads a recent
#: window and stops. Nothing here scans the full 589k-record research corpus.
DEFAULT_LOOKBACK_DAYS = 14
MAX_PARTITION_FILES = 600
MAX_EVENTS_PER_RECORD_TYPE = 4000
MAX_INDEX_KEYS = 250_000
GDELT_MAX_ARTICLES = 25
GDELT_SNIPPET_MAX_CHARS = 280
#: GDELT's free endpoint rate-limits bursts (measured: HTTP 429 on back-to-back
#: queries). These are politeness bounds, not a workaround: the adapter waits between
#: queries and retries a throttled query exactly once.
GDELT_MIN_INTERVAL_SECONDS = 5.0
GDELT_RETRY_BACKOFF_SECONDS = 8.0
HTTP_TIMEOUT_SECONDS = 25
USER_AGENT = "paper-trader-event-fabric/1.0 (+local analytical use)"

#: Record types whose volume makes an unscoped read pointless — only held/candidate
#: names can change the target book, so only those are turned into events.
ENTITY_SCOPED_RECORD_TYPES = frozenset({
    "MARKET_BAR", "SHORT_VOLUME", "SECURITY_IDENTITY", "UNIVERSE_MEMBERSHIP",
    "FUNDAMENTAL_FACT"})

#: (root env, default root, record types) for the corpus lane.
_CORPUS_TREES = (
    ("stage2", scap.INGESTION_ROOT_ENV,
     Path(r"D:\Stock_Prediction_app_data\alpha_agent\ingestion"),
     ("FILING_EVENT", "INSIDER_FILING", "EARNINGS_EVENT", "NEWS_EVENT",
      "CORPORATE_ACTION", "TRADING_HALT", "MACRO_OBSERVATION", "MARKET_BAR",
      "FUNDAMENTAL_FACT", "SHORT_VOLUME", "UNIVERSE_MEMBERSHIP", "SECURITY_IDENTITY")),
    ("news_rss", scap.NEWS_ROOT_ENV,
     Path(r"D:\Stock_Prediction_app_data\alpha_agent\news_rss"),
     ("REGULATORY_EVENT", "PRESS_RELEASE", "NEWS_EVENT")),
)

_SOURCE_QUALITY_BY_KIND = {
    "public_official": "OFFICIAL",
    "entitled_provider": "VENDOR_ENTITLED",
    "local_vendor": "VENDOR_LICENSED",
    "public_api_keyed": "OFFICIAL",
    "public_discovery": "AGGREGATOR",
    "public_delayed": "PUBLIC_DELAYED",
    "internal": "INTERNAL",
    "internal_model": "INTERNAL_MODEL",
    "paid_provider": "UNAVAILABLE",
}

# Deterministic clock seam (tests / explicit callers).
NOW_ENV = "PAPER_TRADER_EVENT_FABRIC_NOW"


def _now() -> datetime:
    raw = os.environ.get(NOW_ENV)
    if raw:
        try:
            parsed = datetime.fromisoformat(raw)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return datetime.now(tz=timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


def _iso_date(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value)[:10]
    try:
        date.fromisoformat(s)
    except ValueError:
        return None
    return s


# --------------------------------------------------------------------------- #
# Store layout
# --------------------------------------------------------------------------- #
def _fabric_dir(fabric_dir=None) -> Path:
    if fabric_dir:
        return Path(fabric_dir)
    raw = os.environ.get(FABRIC_DIR_ENV)
    return Path(raw) if raw else _DEFAULT_FABRIC_DIR


def _events_dir(fabric_dir=None) -> Path:
    return _fabric_dir(fabric_dir) / "events"


def _state_path(fabric_dir=None) -> Path:
    return _fabric_dir(fabric_dir) / "state" / "event_index.json"


def _watermark_path(fabric_dir=None) -> Path:
    return _fabric_dir(fabric_dir) / "state" / "watermarks.json"


def _runs_dir(fabric_dir=None) -> Path:
    return _fabric_dir(fabric_dir) / "runs"


def fabric_root(fabric_dir=None) -> Path:
    """The resolved event-fabric store root (public: the orchestrator resolves the
    same root and must not carry a second copy of this policy)."""
    return _fabric_dir(fabric_dir)


def runs_root(fabric_dir=None) -> Path:
    return _runs_dir(fabric_dir)


def state_root(fabric_dir=None) -> Path:
    return _fabric_dir(fabric_dir) / "state"


def _read_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return None


def read_json_artifact(path) -> Optional[dict]:
    """Read a JSON artifact from the fabric store (None when absent/unparseable)."""
    return _read_json(Path(path))


def save_json_artifact(path, payload: Any) -> None:
    """Atomically write a JSON artifact into the fabric store."""
    _atomic_write_json(Path(path), payload)


def read_latest_run(*, fabric_dir=None) -> Optional[dict]:
    """The pointer to the most recent persisted event-cycle run."""
    return _read_json(_fabric_dir(fabric_dir) / "latest.json")


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=1, sort_keys=True, default=str)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(text)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


# --------------------------------------------------------------------------- #
# The dedup / novelty index (append-only semantics, bounded)
# --------------------------------------------------------------------------- #
def load_index(*, fabric_dir=None) -> dict:
    idx = _read_json(_state_path(fabric_dir)) or {}
    return {
        "idempotency_keys": dict(idx.get("idempotency_keys") or {}),
        "shingles": dict(idx.get("shingles") or {}),
        "native_ids": dict(idx.get("native_ids") or {}),
        "links": dict(idx.get("links") or {}),
        "event_count": int(idx.get("event_count") or 0),
        "updated_at": idx.get("updated_at"),
    }


def _trim(mapping: dict, cap: int) -> dict:
    if len(mapping) <= cap:
        return mapping
    # Keep the most recently inserted keys (dicts preserve insertion order).
    items = list(mapping.items())[-cap:]
    return dict(items)


def save_index(index: dict, *, fabric_dir=None) -> None:
    payload = {
        "idempotency_keys": _trim(index.get("idempotency_keys") or {}, MAX_INDEX_KEYS),
        "shingles": _trim(index.get("shingles") or {}, MAX_INDEX_KEYS),
        "native_ids": _trim(index.get("native_ids") or {}, MAX_INDEX_KEYS),
        "links": _trim(index.get("links") or {}, MAX_INDEX_KEYS),
        "event_count": int(index.get("event_count") or 0),
        "updated_at": _now_iso(),
        "schema_version": SCHEMA_VERSION,
    }
    _atomic_write_json(_state_path(fabric_dir), payload)


def load_watermarks(*, fabric_dir=None) -> dict:
    return dict((_read_json(_watermark_path(fabric_dir)) or {}).get("sources") or {})


def save_watermarks(watermarks: dict, *, fabric_dir=None) -> None:
    _atomic_write_json(_watermark_path(fabric_dir),
                       {"schema_version": SCHEMA_VERSION, "updated_at": _now_iso(),
                        "sources": watermarks})


def append_events(events: Iterable[dict], *, fabric_dir=None,
                  index: Optional[dict] = None, run_id: Optional[str] = None) -> dict:
    """Append NEW events to the immutable log; return what was admitted and what was not.

    IDEMPOTENT. An event whose idempotency key is already in the index is not written
    again and does not advance any downstream calculation. Nothing already written is
    ever modified.
    """
    idx = index if index is not None else load_index(fabric_dir=fabric_dir)
    keys = idx["idempotency_keys"]
    shingles = idx["shingles"]
    natives = idx["native_ids"]
    links = idx["links"]

    admitted: list[dict] = []
    duplicates: list[dict] = []
    by_partition: dict[str, list[dict]] = {}

    for raw in (events or []):
        verdict = ek.classify_novelty(
            event=raw, seen_idempotency_keys=set(keys.keys()),
            seen_shingles=shingles, seen_source_event_ids=natives,
            seen_links=links)
        ev = ek.apply_novelty(raw, verdict)
        if ev["novelty"] == ek.NOV_DUPLICATE:
            duplicates.append(ev)
            continue
        key = ev["idempotency_key"]
        keys[key] = ev["event_id"]
        title = str((ev.get("materiality_inputs") or {}).get("title") or "")
        sh = ek.story_shingle(title)
        if sh and sh not in shingles:
            shingles[sh] = ev["event_id"]
        native_key = ek.supersession_key(ev)
        if ev.get("source_event_id") and native_key not in natives:
            natives[native_key] = ev["event_id"]
        ref = str(ev.get("payload_reference") or "").strip()
        if ref and ref not in links:
            links[ref] = ev["event_id"]
        part = (_iso_date(ev.get("effective_at"))
                or _iso_date(ev.get("published_at"))
                or _iso_date(ev.get("ingested_at"))
                or _now().date().isoformat())
        by_partition.setdefault(part, []).append(ev)
        admitted.append(ev)

    written = 0
    batch = str(run_id or ("batch_" + ek.sha256_text(_now_iso())[:12]))
    for part, rows in sorted(by_partition.items()):
        y, m, d = part[:4], part[5:7], part[8:10]
        out_dir = _events_dir(fabric_dir) / y / m / d
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / ("%s.jsonl" % batch)
        with out_path.open("a", encoding="utf-8", newline="\n") as fh:
            for ev in rows:
                fh.write(json.dumps(ev, sort_keys=True, default=str) + "\n")
                written += 1

    idx["event_count"] = int(idx.get("event_count") or 0) + written
    save_index(idx, fabric_dir=fabric_dir)
    return {
        "admitted": admitted, "admitted_count": len(admitted),
        "duplicates": duplicates, "duplicates_suppressed": len(duplicates),
        "written": written, "batch_id": batch,
        "partitions": sorted(by_partition.keys()),
        "immutable": True, "rewrote_history": False,
    }


def read_events(*, fabric_dir=None, since: Optional[str] = None,
                limit: int = 2000) -> list[dict]:
    """Read persisted events (newest partitions first), bounded."""
    root = _events_dir(fabric_dir)
    if not root.exists():
        return []
    files = sorted(root.rglob("*.jsonl"))
    if since:
        keep = []
        for f in files:
            parts = f.parts
            if len(parts) >= 4:
                part = "%s-%s-%s" % (parts[-4], parts[-3], parts[-2])
                if part >= since:
                    keep.append(f)
        files = keep
    out: list[dict] = []
    for f in reversed(files):
        try:
            for line in f.read_text(encoding="utf-8-sig").splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except ValueError:
                    continue
                if len(out) >= limit:
                    return out
        except OSError:
            continue
    return out


# --------------------------------------------------------------------------- #
# Entity resolution — deterministic, no fuzzy matching, no model call
# --------------------------------------------------------------------------- #
_NAME_NOISE = re.compile(r"[^a-z0-9 ]+")
_CORP_SUFFIXES = (" incorporated", " inc", " corporation", " corp", " company", " co",
                  " limited", " ltd", " plc", " holdings", " holding", " group",
                  " common", " class a", " class b", " the", " sa", " nv", " ag")


def normalize_company_name(name: Any) -> str:
    s = _NAME_NOISE.sub(" ", str(name or "").lower())
    s = " ".join(s.split())
    changed = True
    while changed:
        changed = False
        for suf in _CORP_SUFFIXES:
            if s.endswith(suf):
                s = s[: -len(suf)].strip()
                changed = True
    return s


def build_entity_index(tickers: Iterable[str], *, ingestion_root=None,
                       max_files: int = 200) -> dict:
    """ticker -> normalized company name, from OWNED identity records.

    Deterministic and bounded. Conflicting names for one ticker are kept as a list and
    a headline must match one of them EXACTLY (as a whole-word substring) — there is no
    fuzzy scoring anywhere in this path.
    """
    wanted = {str(t).strip().upper() for t in (tickers or []) if str(t or "").strip()}
    root = scap.resolve_root(
        scap.INGESTION_ROOT_ENV,
        Path(r"D:\Stock_Prediction_app_data\alpha_agent\ingestion"), ingestion_root)
    idx: dict[str, list[str]] = {}
    tree = root / "normalized" / "SECURITY_IDENTITY"
    if not tree.exists():
        return {"by_ticker": {}, "by_name": {}, "source": str(tree), "present": False}
    files = sorted(tree.rglob("*.jsonl"))[-max_files:]
    for f in files:
        try:
            text = f.read_text(encoding="utf-8-sig")
        except OSError:
            continue
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            tkr = str(rec.get("ticker") or "").strip().upper()
            if not tkr or (wanted and tkr not in wanted):
                continue
            name = normalize_company_name(
                (rec.get("normalized_payload") or {}).get("security_name"))
            if name and name not in idx.setdefault(tkr, []):
                idx[tkr].append(name)
    by_name: dict[str, str] = {}
    for tkr, names in idx.items():
        for n in names:
            by_name.setdefault(n, tkr)
    return {"by_ticker": idx, "by_name": by_name, "source": str(tree), "present": True,
            "tickers_resolved": len(idx)}


def resolve_entities(*, text: Any, entity_index: Optional[dict] = None,
                     declared: Iterable[str] = ()) -> tuple[list[str], str]:
    """Resolve a headline to tickers. Declared vendor tickers win; otherwise an EXACT
    whole-phrase company-name match. Returns (tickers, identity_confidence)."""
    dec = [str(t).strip().upper() for t in (declared or []) if str(t or "").strip()]
    dec = [t.split(".")[0] for t in dec]
    if dec:
        return sorted(set(dec)), "MATCHED_EXACT"
    by_name = (entity_index or {}).get("by_name") or {}
    if not by_name:
        return [], "UNMATCHED"
    hay = " " + normalize_company_name(text) + " "
    hits = {tkr for name, tkr in by_name.items() if name and (" " + name + " ") in hay}
    if not hits:
        return [], "UNMATCHED"
    return sorted(hits), "MATCHED_ALIAS"


# --------------------------------------------------------------------------- #
# RESEARCH_CORPUS lane — turn Stage-2 / Stage-3.5 normalized records into events
# --------------------------------------------------------------------------- #
def _partition_dates(tree: Path) -> list[str]:
    if not tree.exists():
        return []
    out: set[str] = set()
    for y in tree.iterdir():
        if not (y.is_dir() and y.name.isdigit()):
            continue
        for m in y.iterdir():
            if not (m.is_dir() and m.name.isdigit()):
                continue
            for d in m.iterdir():
                if d.is_dir() and d.name.isdigit():
                    out.add("%s-%s-%s" % (y.name, m.name, d.name))
    return sorted(out)


def _title_of(record_type: str, payload: dict) -> Optional[str]:
    for key in ("title", "headline", "issue_name", "security_name"):
        if payload.get(key):
            return str(payload[key])
    if record_type in ("FILING_EVENT", "INSIDER_FILING"):
        return "%s %s" % (payload.get("form_type") or "", payload.get("accession_number") or "")
    return None


def _declared_tickers(record_type: str, rec: dict, payload: dict) -> list[str]:
    out = []
    if rec.get("ticker"):
        out.append(str(rec["ticker"]))
    for key in ("mapped_tickers", "symbols"):
        vals = payload.get(key)
        if isinstance(vals, (list, tuple)):
            out.extend(str(v) for v in vals if v)
        elif isinstance(vals, str) and vals.strip():
            out.extend(v for v in re.split(r"[,\s]+", vals) if v)
    return out


def _event_quality(rec: dict, published: Any) -> str:
    conf = str(rec.get("entity_mapping_confidence") or "")
    warns = rec.get("quality_warnings") or []
    if conf in ("MATCHED_EXACT",) and published:
        return "HIGH"
    if conf in ("MATCHED_EXACT", "MATCHED_HISTORICAL", "MATCHED_ALIAS"):
        return "MEDIUM" if published else "LOW"
    return "LOW" if warns else "MEDIUM"


def record_to_event(rec: dict, *, lane: str, entity_index: Optional[dict] = None,
                    now_iso: Optional[str] = None) -> Optional[dict]:
    """Map ONE Stage-2/3.5 normalized record onto the canonical event contract."""
    rt = str(rec.get("record_type") or "").upper()
    if not rt:
        return None
    payload = rec.get("normalized_payload") or {}
    published = (payload.get("publication_time") or rec.get("available_at"))
    accepted = (payload.get("acceptance_datetime") or payload.get("acceptance_time"))
    title = _title_of(rt, payload)
    declared = _declared_tickers(rt, rec, payload)
    entities, confidence = resolve_entities(
        text=title, entity_index=entity_index, declared=declared)
    if not entities and rec.get("ticker"):
        entities, confidence = [str(rec["ticker"]).upper()], str(
            rec.get("entity_mapping_confidence") or "UNMATCHED")
    elif declared:
        confidence = str(rec.get("entity_mapping_confidence") or confidence)

    collector = str(rec.get("source_id") or "")
    source_id = scap.canonical_source_id(collector)
    src = scap.SOURCE_BY_ID.get(source_id)
    src_quality = _SOURCE_QUALITY_BY_KIND.get(
        (src or {}).get("kind") or "", "UNKNOWN")

    materiality: dict[str, Any] = {}
    if title:
        materiality["title"] = title
    for key in ("publisher", "author", "official_source", "trust_level", "form_type",
                "actual", "estimate", "percent", "difference", "value", "period",
                "reason_code", "series_id", "macro_family", "index_name",
                "before_after_market", "item_202_note", "sentiment", "categories"):
        if payload.get(key) not in (None, ""):
            materiality[key] = payload[key]
    link = payload.get("link") or payload.get("canonical_link") or payload.get("official_link")

    return ek.build_event(
        source_id=source_id,
        collector_id=collector,
        record_type=rt,
        source_event_id=(rec.get("source_native_id") or rec.get("record_id")),
        payload=payload,
        event_type=rec.get("event_type"),
        source_family=lane,
        source_timestamp=payload.get("event_time"),
        published_at=published,
        accepted_at=accepted,
        effective_at=rec.get("effective_at"),
        first_observed_at=rec.get("observed_at"),
        ingested_at=(now_iso or _now_iso()),
        entities=entities,
        primary_ticker=(entities[0] if entities else None),
        identity_confidence=confidence,
        source_quality=src_quality,
        event_quality=_event_quality(rec, published),
        materiality_inputs=materiality,
        payload_reference=link,
        quality_warnings=list(rec.get("quality_warnings") or []))


def ingest_corpus_lane(*, tickers: Iterable[str] = (), lookback_days: int = DEFAULT_LOOKBACK_DAYS,
                       since: Optional[str] = None, ingestion_root=None,
                       news_root=None, entity_index: Optional[dict] = None,
                       record_types: Optional[Iterable[str]] = None,
                       max_events_per_type: int = MAX_EVENTS_PER_RECORD_TYPE
                       ) -> dict:
    """Read the bounded recent window of every corpus tree and build canonical events."""
    scope = {str(t).strip().upper() for t in (tickers or []) if str(t or "").strip()}
    wanted_types = ({str(t).upper() for t in record_types} if record_types else None)
    now_iso = _now_iso()
    events: list[dict] = []
    per_source: dict[str, dict] = {}
    scanned_files = 0

    overrides = {"stage2": ingestion_root, "news_rss": news_root}
    for lane, env_name, default_root, types in _CORPUS_TREES:
        root = scap.resolve_root(env_name, default_root, overrides.get(lane))
        for rt in types:
            if wanted_types is not None and rt not in wanted_types:
                continue
            tree = root / "normalized" / rt
            dates = _partition_dates(tree)
            if not dates:
                continue
            newest = dates[-1]
            floor = since or (
                date.fromisoformat(newest) - timedelta(days=int(lookback_days))
            ).isoformat()
            take = [d for d in dates if d >= floor]
            files: list[Path] = []
            for d in take:
                files.extend(sorted(
                    (tree / d[:4] / d[5:7] / d[8:10]).glob("*.jsonl")))
            files = files[-MAX_PARTITION_FILES:]
            produced = 0
            for f in files:
                scanned_files += 1
                try:
                    text = f.read_text(encoding="utf-8-sig")
                except OSError:
                    continue
                for line in text.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except ValueError:
                        continue
                    if rt in ENTITY_SCOPED_RECORD_TYPES and scope:
                        tkr = str(rec.get("ticker") or "").upper()
                        if tkr not in scope:
                            continue
                    ev = record_to_event(rec, lane=lane, entity_index=entity_index,
                                         now_iso=now_iso)
                    if ev is None:
                        continue
                    events.append(ev)
                    sid = ev["source_id"] or lane
                    row = per_source.setdefault(
                        sid, {"source_id": sid, "lane": lane, "events": 0,
                              "newest_effective_at": None})
                    row["events"] += 1
                    eff = _iso_date(ev.get("effective_at"))
                    if eff and (row["newest_effective_at"] is None
                                or eff > row["newest_effective_at"]):
                        row["newest_effective_at"] = eff
                    produced += 1
                    if produced >= int(max_events_per_type):
                        break
                if produced >= int(max_events_per_type):
                    break
    return {"lane": scap.LANE_RESEARCH_CORPUS, "events": events,
            "event_count": len(events), "per_source": per_source,
            "scanned_files": scanned_files,
            "bounded_by": {"lookback_days": lookback_days,
                           "max_partition_files": MAX_PARTITION_FILES,
                           "max_events_per_record_type": max_events_per_type}}


# --------------------------------------------------------------------------- #
# LIVE_ADAPTER lane — the near-real-time sources the corpus collectors do not run
# --------------------------------------------------------------------------- #
def _http_get_json(url: str, *, timeout: int = HTTP_TIMEOUT_SECONDS) -> dict:
    """Bounded read-only GET. Returns a measured result; never raises to the caller."""
    started = time.time()
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                               "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
            body = resp.read().decode("utf-8", errors="replace")
            elapsed = time.time() - started
            try:
                return {"ok": True, "status": resp.status, "json": json.loads(body),
                        "elapsed_seconds": round(elapsed, 3)}
            except ValueError:
                return {"ok": False, "status": resp.status, "json": None,
                        "detail": "response was not JSON",
                        "elapsed_seconds": round(elapsed, 3)}
    except urllib.error.HTTPError as exc:
        return {"ok": False, "status": exc.code, "json": None,
                "detail": "HTTP %s" % exc.code,
                "elapsed_seconds": round(time.time() - started, 3)}
    except (urllib.error.URLError, OSError, ValueError) as exc:
        return {"ok": False, "status": None, "json": None,
                "detail": str(exc)[:200],
                "elapsed_seconds": round(time.time() - started, 3)}


def capture_market_quotes(tickers: Iterable[str], *, fetcher: Optional[Callable] = None,
                          now_iso: Optional[str] = None) -> dict:
    """MARKET_QUOTE events at the fastest cadence currently available.

    Delegates the fetch to ``engine.market_data`` — the canonical market-data owner —
    and converts its result into events. RISK AUTHORITY ONLY: no released signal
    contract is formed intraday, so these events may never move a score.

    IDENTITY IS THE DAY'S MARK, NOT THE MINUTE'S READ. ``source_event_id`` is keyed
    on (ticker, market date), so re-reading an UNCHANGED quote is an exact duplicate
    and a no-op, while a CHANGED price is one immutable MATERIAL_UPDATE that
    supersedes the prior mark for that day. Keying on the minute instead would make
    every poll of a still market manufacture a "new" event for every holding — the
    continuous collection service polls this lane every 15 minutes, which is the
    difference between an idempotent lane and 25 fabricated events an hour.

    ``now_iso`` is the CALLER'S clock. The adapter keeps no ambient time of its own:
    event identity must come from the cycle that asked for the quote, or a replay
    driven by a simulated clock is stamped with the real one and its verdict depends
    on which real-world minute the run happened to straddle.
    """
    tk = [str(t).strip().upper() for t in (tickers or []) if str(t or "").strip()]
    if not tk:
        return {"events": [], "ok": True, "detail": "no tickers requested",
                "fetched": 0, "failures": []}
    fetch = fetcher
    if fetch is None:
        from paper_trader.engine import market_data as md
        fetch = md.fetch_latest_prices
    stamp = now_iso or _now_iso()
    started = time.time()
    try:
        prices, failures = fetch(tk)
    except Exception as exc:  # noqa: BLE001 - an adapter must not crash the cycle
        return {"events": [], "ok": False, "detail": str(exc)[:200], "fetched": 0,
                "failures": [{"ticker": t, "reason": "adapter error"} for t in tk]}
    events = []
    for row in (prices or []):
        ticker = str(row.get("ticker") or "").upper()
        price = row.get("price")
        if not ticker or price is None:
            continue
        payload = {"ticker": ticker, "price": str(price), "quote_kind": "DELAYED",
                   "provider": "yahoo_finance_delayed",
                   "delay_note": ("free delayed quote; the provider states no exact "
                                  "quote timestamp, so publication time is left null")}
        events.append(ek.build_event(
            source_id="yahoo_delayed_quote", record_type="MARKET_QUOTE",
            source_event_id="quote|%s|%s" % (ticker, stamp[:10]),
            payload=payload, event_type="DELAYED_QUOTE",
            source_family=scap.LANE_LIVE_ADAPTER,
            effective_at=stamp[:10], first_observed_at=stamp, ingested_at=stamp,
            entities=[ticker], primary_ticker=ticker,
            identity_confidence="MATCHED_EXACT", source_quality="PUBLIC_DELAYED",
            event_quality="MEDIUM",
            materiality_inputs={"price": str(price), "quote_kind": "DELAYED"}))
    return {"events": events, "ok": True, "fetched": len(events),
            "failures": list(failures or []),
            "elapsed_seconds": round(time.time() - started, 3),
            "authority_note": ("MARKET_QUOTE carries OPERATIONAL_RISK authority only: "
                               "it updates valuation and risk, never a score.")}


GDELT_ENDPOINT = "https://api.gdeltproject.org/api/v2/doc/doc"


def capture_gdelt_news(tickers: Iterable[str], *, entity_index: Optional[dict] = None,
                       fetcher: Optional[Callable] = None, max_tickers: int = 8,
                       max_articles: int = GDELT_MAX_ARTICLES,
                       timespan: str = "2d", now_iso: Optional[str] = None) -> dict:
    """Bounded, METADATA-ONLY GDELT discovery events (trigger-only authority).

    Stores headline, publisher, canonical URL, timestamps and a bounded snippet. It
    never archives unrestricted copyrighted article text. ``fetcher`` is an injected
    seam so replay and tests are hermetic.
    """
    names = (entity_index or {}).get("by_ticker") or {}
    tk = [str(t).strip().upper() for t in (tickers or []) if str(t or "").strip()]
    tk = tk[: int(max_tickers)]
    stamp = now_iso or _now_iso()
    fetch = fetcher or (lambda url: _http_get_json(url))

    events: list[dict] = []
    probes: list[dict] = []
    first = True
    for ticker in tk:
        query_name = (names.get(ticker) or [None])[0]
        if not query_name:
            probes.append({"ticker": ticker, "ok": False,
                           "detail": "no owned company name resolved; not queried"})
            continue
        params = {"query": '"%s"' % query_name, "mode": "ArtList", "format": "json",
                  "maxrecords": str(int(max_articles)), "timespan": timespan,
                  "sort": "DateDesc"}
        url = GDELT_ENDPOINT + "?" + urllib.parse.urlencode(params)
        if not first and fetcher is None:
            time.sleep(GDELT_MIN_INTERVAL_SECONDS)
        first = False
        res = fetch(url) or {}
        # The free endpoint throttles bursts. One polite retry — never a tight loop.
        if (res or {}).get("status") == 429 and fetcher is None:
            time.sleep(GDELT_RETRY_BACKOFF_SECONDS)
            res = fetch(url) or {}
            res["retried_after_rate_limit"] = True
        probes.append({"ticker": ticker, "ok": bool(res.get("ok")),
                       "status": res.get("status"), "detail": res.get("detail"),
                       "retried_after_rate_limit": res.get("retried_after_rate_limit",
                                                           False),
                       "elapsed_seconds": res.get("elapsed_seconds")})
        if not res.get("ok"):
            continue
        for art in ((res.get("json") or {}).get("articles") or [])[: int(max_articles)]:
            title = str(art.get("title") or "").strip()
            url_a = str(art.get("url") or "").strip()
            if not title or not url_a:
                continue
            seendate = str(art.get("seendate") or "")
            published = None
            if len(seendate) >= 15 and seendate[:8].isdigit():
                published = "%s-%s-%sT%s:%s:%sZ" % (
                    seendate[0:4], seendate[4:6], seendate[6:8],
                    seendate[9:11], seendate[11:13], seendate[13:15])
            payload = {
                "title": title, "canonical_link": url_a,
                "publisher": art.get("domain"), "language": art.get("language"),
                "source_country": art.get("sourcecountry"),
                "publication_time": published,
                "bounded_summary": title[:GDELT_SNIPPET_MAX_CHARS],
                "allowed_storage": "METADATA_AND_BOUNDED_SNIPPET_ONLY",
                "licensing_note": ("GDELT open metadata; unrestricted copyrighted "
                                   "article text is never archived."),
                "query_company_name": query_name,
            }
            events.append(ek.build_event(
                source_id="gdelt", record_type="NEWS_EVENT",
                source_event_id="gdelt|%s" % url_a, payload=payload,
                event_type="NEWS", source_family=scap.LANE_LIVE_ADAPTER,
                published_at=published,
                effective_at=(published[:10] if published else stamp[:10]),
                first_observed_at=stamp, ingested_at=stamp,
                entities=[ticker], primary_ticker=ticker,
                identity_confidence="MATCHED_ALIAS", source_quality="AGGREGATOR",
                event_quality=("MEDIUM" if published else "LOW"),
                materiality_inputs={"title": title, "publisher": art.get("domain")},
                payload_reference=url_a))
    ok_any = any(p.get("ok") for p in probes)
    return {"events": events, "ok": ok_any, "probes": probes,
            "queried_tickers": tk, "event_count": len(events),
            "detail": (None if ok_any else
                       "; ".join(sorted({str(p.get("detail")) for p in probes
                                         if p.get("detail")}))[:300]),
            "authority_note": ("GDELT is EVENT_TRIGGER_ONLY: it can direct attention to "
                               "a holding and can never contribute expected return.")}


# --------------------------------------------------------------------------- #
# Source freshness / watermarks — cadence aware, delegated classifier
# --------------------------------------------------------------------------- #
#: The event-fabric cadence of a source, expressed in the CANONICAL freshness vocabulary
#: owned by ``api.data_freshness``. A quarterly source is never "stale" for being older
#: than a daily one.
_CADENCE_MAP = {
    "norgate_local": dfresh.DAILY,
    "eodhd": dfresh.DAILY,
    "eodhd_analyst": dfresh.DAILY,
    "sec_edgar": dfresh.EVENT_DRIVEN,
    "news_rss": dfresh.EVENT_DRIVEN,
    "nasdaq_trader": dfresh.EVENT_DRIVEN,
    "finra": dfresh.DAILY,
    "fred_alfred": dfresh.DAILY,
    "us_treasury": dfresh.MONTHLY,
    "bls": dfresh.MONTHLY,
    "bea": dfresh.QUARTERLY,
    "yahoo_delayed_quote": dfresh.DAILY,
    "gdelt": dfresh.EVENT_DRIVEN,
    "corporate_actions_registry": dfresh.EVENT_DRIVEN,
    "analyst_revision_vendor": dfresh.STATIC,
    "options_iv": dfresh.STATIC,
    "prediction_service": dfresh.STATIC,
}
#: Sources whose cadence makes them slower than the eligible session BY DESIGN. They are
#: reported NOT_DUE rather than STALE when they are merely between releases.
_STALE_TOLERANCE_DAYS = {
    "sec_edgar": 4, "news_rss": 4, "nasdaq_trader": 4, "gdelt": 2,
    "corporate_actions_registry": 3650,
}


def build_source_freshness(*, capability: Optional[dict] = None,
                           watermarks: Optional[dict] = None,
                           anchor: Any = None, fabric_dir=None,
                           ingestion_root=None, news_root=None) -> dict:
    """Per-source freshness: watermark, expected cadence, status, lag, coverage.

    The STATUS itself is classified by ``api.data_freshness.classify_source`` — the
    canonical cadence-aware classifier. This owner adds the watermark and the event
    counters; it does not fork the freshness vocabulary.
    """
    cap = capability if capability is not None else scap.build_capability_matrix(
        ingestion_root=ingestion_root, news_root=news_root)
    marks = watermarks if watermarks is not None else load_watermarks(fabric_dir=fabric_dir)
    anchor_date = _iso_date(anchor)

    rows = []
    degraded = []
    for src in (cap.get("sources") or []):
        sid = src["source_id"]
        cadence = _CADENCE_MAP.get(sid, dfresh.EVENT_DRIVEN)
        wm = marks.get(sid) or {}
        as_of = (wm.get("source_watermark") or src.get("source_watermark")
                 or wm.get("last_event_effective_at"))
        cls = dfresh.classify_source(cadence=cadence, as_of=as_of, anchor=anchor_date)
        status = cls["status"]
        tol = _STALE_TOLERANCE_DAYS.get(sid)
        if (status == dfresh.STALE and tol is not None
                and (cls.get("lag_sessions") or 0) <= tol):
            status = dfresh.NOT_DUE
            cls = dict(cls, status=status,
                       reason=("Within this source's own %d-session publication "
                               "tolerance; a publisher-driven feed is not stale merely "
                               "because it published nothing today." % tol))
        integrated = src["terminal_state"] in ek.INTEGRATED_TERMINAL_STATES
        if not integrated:
            status = dfresh.NOT_APPLICABLE
            cls = dict(cls, status=status,
                       reason="Source is terminally %s; no freshness is expected."
                              % src["terminal_state"])
        row = {
            "source_id": sid, "label": src["label"], "lane": src["lane"],
            "cadence": cadence, "expected_cadence_text": src["available_frequency"],
            "status": status, "reason": cls.get("reason"),
            "expected_through_date": cls.get("expected_through_date"),
            "lag_sessions": cls.get("lag_sessions"),
            "lag_calendar_days": cls.get("lag_calendar_days"),
            "source_watermark": as_of,
            "last_attempted_at": wm.get("last_attempt_at"),
            "last_success_at": (wm.get("last_success_at") or src.get("last_success_at")),
            "events_total": int(wm.get("events_total") or 0),
            "duplicates_total": int(wm.get("duplicates_total") or 0),
            "last_error": wm.get("last_error"),
            "circuit_state": src.get("circuit_state"),
            "connection_status": src["connection_status"],
            "terminal_state": src["terminal_state"],
            "decision_authorities": src["decision_authorities"],
            "integrated": integrated,
            "processing_lag_seconds": wm.get("processing_lag_seconds"),
        }
        rows.append(row)
        if integrated and status in (dfresh.STALE, dfresh.MISSING, dfresh.UNKNOWN):
            degraded.append({"source_id": sid, "status": status,
                             "reason": cls.get("reason")})
    return {
        "contract_id": "paper_trader.source_freshness_state/1",
        "composition_owner": COMPOSITION_OWNER,
        "classifier_owner": "api.data_freshness.classify_source",
        "freshness_vocabulary": list(dfresh.FRESHNESS_VOCAB),
        "anchor_date": anchor_date,
        "sources": rows,
        "degraded_sources": degraded,
        "degraded_count": len(degraded),
        "fresh_count": sum(1 for r in rows if r["status"] == dfresh.FRESH),
        "note": ("Freshness is judged under each source's OWN cadence. A quarterly "
                 "filing lane is not broken because it published nothing today, and a "
                 "degraded source is reported degraded rather than back-filled with a "
                 "fabricated current value."),
    }


def advance_watermarks(*, watermarks: dict, per_source: dict, admitted: list,
                       duplicates: int, now_iso: Optional[str] = None,
                       errors: Optional[dict] = None) -> dict:
    """Advance each source's watermark from what was ACTUALLY admitted."""
    stamp = now_iso or _now_iso()
    out = dict(watermarks or {})
    by_source: dict[str, list] = {}
    for ev in (admitted or []):
        by_source.setdefault(str(ev.get("source_id") or ""), []).append(ev)
    seen = set(per_source or {}) | set(by_source)
    for sid in sorted(seen):
        row = dict(out.get(sid) or {})
        evs = by_source.get(sid) or []
        newest = None
        for ev in evs:
            eff = _iso_date(ev.get("effective_at")) or _iso_date(ev.get("published_at"))
            if eff and (newest is None or eff > newest):
                newest = eff
        prior = row.get("source_watermark")
        if newest and (prior is None or newest > str(prior)):
            row["source_watermark"] = newest
        elif prior is None:
            row["source_watermark"] = (per_source.get(sid) or {}).get(
                "newest_effective_at")
        row["last_attempt_at"] = stamp
        err = (errors or {}).get(sid)
        row["last_error"] = err
        if not err:
            row["last_success_at"] = stamp
        row["events_total"] = int(row.get("events_total") or 0) + len(evs)
        out[sid] = row
    if duplicates:
        for sid in sorted(seen):
            out[sid]["duplicates_total"] = int(out[sid].get("duplicates_total") or 0)
    return out


# --------------------------------------------------------------------------- #
# Read contract
# --------------------------------------------------------------------------- #
def load_event_fabric(*, fabric_dir=None, since: Optional[str] = None,
                      limit: int = 300, anchor: Any = None,
                      ingestion_root=None, news_root=None) -> dict:
    """READ-ONLY view of the fabric: recent events, freshness, contract, graph."""
    events = read_events(fabric_dir=fabric_dir, since=since, limit=limit)
    cap = scap.build_capability_matrix(ingestion_root=ingestion_root,
                                       news_root=news_root)
    freshness = build_source_freshness(capability=cap, fabric_dir=fabric_dir,
                                       anchor=anchor)
    by_family: dict[str, int] = {}
    by_authority: dict[str, int] = {}
    for ev in events:
        by_family[str(ev.get("family"))] = by_family.get(str(ev.get("family")), 0) + 1
        a = str(ev.get("decision_authority"))
        by_authority[a] = by_authority.get(a, 0) + 1
    return {
        "schema_version": SCHEMA_VERSION,
        "phase": PHASE,
        "composition_owner": COMPOSITION_OWNER,
        "generated_at": _now_iso(),
        "event_contract": ek.event_contract(),
        "dependency_graph": ek.build_dependency_graph(),
        "capability_matrix": cap,
        "terminal_audit": scap.terminal_audit(cap, events=events),
        "source_freshness": freshness,
        "events": events,
        "event_count": len(events),
        "events_by_family": by_family,
        "events_by_authority": by_authority,
        "unclassified_signal_authority": ek.unclassified_authority_count(events),
        "store_root": str(_fabric_dir(fabric_dir)),
        "store_root_env": FABRIC_DIR_ENV,
        "immutable": True,
        "safety": {"read_only": True, "performed_write": False, "creates_orders": False,
                   "mutates_operational_state": False, "promotes_models": False},
    }


__all__ = [
    "PHASE", "COMPOSITION_OWNER", "SCHEMA_VERSION", "FABRIC_DIR_ENV",
    "DEFAULT_LOOKBACK_DAYS", "GDELT_ENDPOINT", "GDELT_MAX_ARTICLES",
    "ENTITY_SCOPED_RECORD_TYPES",
    "load_index", "save_index", "load_watermarks", "save_watermarks",
    "append_events", "read_events", "record_to_event", "ingest_corpus_lane",
    "capture_market_quotes", "capture_gdelt_news", "build_entity_index",
    "resolve_entities", "normalize_company_name", "build_source_freshness",
    "advance_watermarks", "load_event_fabric",
]
