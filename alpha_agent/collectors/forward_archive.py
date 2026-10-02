"""
alpha_agent/collectors/forward_archive.py — R96 FORWARD-ONLY archive lanes.

Some information has no usable historical archive: a provider serves only its
CURRENT news window, its CURRENT economic-calendar estimates, the CURRENT
shortable-shares file. Every day that is not captured is lost for good. These
two Stage-2 sources capture it, under the canonical collection service
(``api.information_collection`` dispatches them through ``alpha_agent.ingestion``
exactly like every other Stage-2 source — there is no second collector):

  ``eodhd_forward_archive``  (subscriber-entitled, ``EODHD_API_KEY``)
      * NEWS     — symbol-tagged articles for the configured universe file
                   (S&P 500 Current & Past + extension, currently listed). One
                   row per article id, first-ingest-wins; a symbol is visited at
                   most once per day; a page that comes back full is recorded as
                   TRUNCATED, never silently treated as complete.
      * ECONOMIC EVENTS — the provider's calendar window, snapshotted as
                   observed (actual / estimate / previous / change), one
                   immutable snapshot per day. Re-observing the same event on
                   later days is what MEASURES whether the provider overwrites
                   an estimate — it is never assumed.

  ``public_forward_archive``  (public, no credential)
      * IBKR SHORTABLE SHARES — the public ``usa.txt`` borrow file (public FTP
                   login ``shortstock``), one compressed daily snapshot.
      * SPDR ETF NAV HISTORY — the issuer's NAV-history workbook (date, NAV,
                   shares outstanding, net assets) per fund: a full copy on the
                   first capture, then a daily vintage of its most recent rows
                   plus the whole-file hash, so a later restatement is visible.

Discipline shared with the analyst-vintage lane (``eodhd_analyst``):
  * IMMUTABLE, first-write-wins (``os.replace`` of a temp file; an existing
    snapshot is never overwritten) and SQLite ``INSERT OR IGNORE`` keyed on a
    content identity — a re-run on the same day writes nothing new;
  * timestamps preserved: the provider's own publication / event time is kept
    verbatim beside ``ingested_at`` (our capture instant) — availability is
    never back-dated to the provider time;
  * every payload carries the sha256 of the provider bytes (``source_hash``);
  * NO normalized records are emitted: this is research corpus, it can never
    reach the event fabric, a reassessment or the operational target;
  * the credential is read into memory only to build requests; persisted
    request fingerprints are redacted by ``BaseCollector``.
"""
from __future__ import annotations

import datetime as _dt
import ftplib
import gzip
import hashlib
import io
import json
import os
import re
import sqlite3
import urllib.parse
from pathlib import Path
from typing import Any, Callable, Optional

from ..source_contracts import (
    CB_OPEN, ENT_AUTH_FAILED, ENT_ENTITLED, ENT_NOT_ENTITLED, ENT_RATE_LIMITED,
    ENT_UNKNOWN, SH_BLOCKED_CREDENTIAL, SH_DEGRADED, SH_FAILED, SH_HEALTHY, SH_NOT_RUN,
)
from .base import BaseCollector

FAMILY_NEWS = "eodhd_news"
FAMILY_ECON = "eodhd_economic_events"
FAMILY_IBKR = "ibkr_shortable_shares"
FAMILY_SPDR = "spdr_etf_nav_history"

NOT_AVAILABLE = "NOT_AVAILABLE"
_SNIPPET_CHARS = 500


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _days_before(as_of: str, days: int) -> str:
    return (_dt.date.fromisoformat(as_of) - _dt.timedelta(days=days)).isoformat()


def _days_after(as_of: str, days: int) -> str:
    return (_dt.date.fromisoformat(as_of) + _dt.timedelta(days=days)).isoformat()


def _write_once(path: Path, data: bytes) -> bool:
    """Immutable first-write-wins. Returns True only when this call wrote it."""
    if path.exists():
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(data)
    try:
        if path.exists():           # another writer won the race
            tmp.unlink()
            return False
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()
    return True


def _json_bytes(obj: Any) -> bytes:
    return json.dumps(obj, indent=1, sort_keys=True, default=str).encode("utf-8")


def load_universe(path: Any) -> list[str]:
    """Symbols from a universe file (``{"symbols": [...]}``) — config, not a guess."""
    if not path:
        return []
    p = Path(path)
    if not p.is_absolute():
        p = Path(__file__).resolve().parents[2] / p      # relative to the repo root
    try:
        doc = json.loads(p.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return []
    out, seen = [], set()
    for s in (doc.get("symbols") or []):
        s = str(s).strip().upper()
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def news_article_id(row: dict) -> str:
    """One identity per article: the link when present, else date|title."""
    link = str(row.get("link") or "").strip()
    basis = link if link else "%s|%s" % (row.get("date"), row.get("title"))
    return _sha256(basis.encode("utf-8"))[:32]


def econ_event_key(row: dict) -> str:
    """One identity per calendar event, stable across daily snapshots."""
    basis = "|".join(str(row.get(k) or "") for k in
                     ("type", "country", "date", "period", "comparison"))
    return _sha256(basis.encode("utf-8"))[:32]


_ECON_VALUE_FIELDS = ("actual", "estimate", "previous", "change", "change_percentage")


# =========================================================================== #
# The archive stores (SQLite, under the source's vintage root)
# =========================================================================== #
_NEWS_SCHEMA = """
CREATE TABLE IF NOT EXISTS articles (
    article_id          TEXT PRIMARY KEY,
    publication_ts      TEXT,
    title               TEXT,
    link                TEXT,
    source_domain       TEXT,
    symbols_json        TEXT,
    tags_json           TEXT,
    sentiment_json      TEXT,
    content_sha256      TEXT,
    content_chars       INTEGER,
    snippet             TEXT,
    first_query_symbol  TEXT,
    first_ingested_at   TEXT NOT NULL,
    first_capture_date  TEXT NOT NULL,
    source_payload_sha256 TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS article_symbols (
    article_id   TEXT NOT NULL,
    query_symbol TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    PRIMARY KEY (article_id, query_symbol)
);
CREATE TABLE IF NOT EXISTS symbol_days (
    query_symbol  TEXT NOT NULL,
    capture_date  TEXT NOT NULL,
    window_from   TEXT NOT NULL,
    window_to     TEXT NOT NULL,
    pages         INTEGER NOT NULL,
    articles_seen INTEGER NOT NULL,
    articles_new  INTEGER NOT NULL,
    truncated     INTEGER NOT NULL,
    status        TEXT NOT NULL,
    ingested_at   TEXT NOT NULL,
    PRIMARY KEY (query_symbol, capture_date)
);
"""

_ECON_SCHEMA = """
CREATE TABLE IF NOT EXISTS observations (
    event_key         TEXT NOT NULL,
    snapshot_date     TEXT NOT NULL,
    type              TEXT,
    country           TEXT,
    event_time        TEXT,
    period            TEXT,
    comparison        TEXT,
    actual            REAL,
    estimate          REAL,
    previous          REAL,
    change            REAL,
    change_percentage REAL,
    currency          TEXT,
    row_sha256        TEXT NOT NULL,
    ingested_at       TEXT NOT NULL,
    PRIMARY KEY (event_key, snapshot_date)
);
CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_date   TEXT PRIMARY KEY,
    window_from     TEXT NOT NULL,
    window_to       TEXT NOT NULL,
    rows            INTEGER NOT NULL,
    pages           INTEGER NOT NULL,
    truncated       INTEGER NOT NULL,
    payload_sha256  TEXT NOT NULL,
    probe_sha256    TEXT,
    ingested_at     TEXT NOT NULL
);
"""


def _open_db(path: Path, schema: str) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=60)
    conn.executescript(schema)
    return conn


def econ_estimate_revisions(db_path: Any) -> dict:
    """MEASURED overwrite evidence: the same event observed on two snapshot days
    with a different estimate (or actual). Read-only. Empty until two snapshots."""
    p = Path(db_path)
    if not p.exists():
        return {"snapshots": 0, "events_observed_twice": 0, "estimate_changed": 0,
                "estimate_changed_after_event_time": 0, "actual_changed": 0,
                "verdict": "UNDETERMINED_NO_SNAPSHOT"}
    conn = sqlite3.connect("file:%s?mode=ro" % p.as_posix(), uri=True)
    try:
        n_snap = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        rows = conn.execute(
            "SELECT event_key, snapshot_date, event_time, estimate, actual FROM observations "
            "ORDER BY event_key, snapshot_date").fetchall()
    finally:
        conn.close()
    by: dict[str, list] = {}
    for r in rows:
        by.setdefault(r[0], []).append(r)
    twice = est_chg = est_after = act_chg = 0
    for obs in by.values():          # obs sorted by snapshot_date
        if len(obs) < 2:
            continue
        twice += 1
        if len({o[3] for o in obs if o[3] is not None}) > 1:
            est_chg += 1
        # An estimate that moves BETWEEN two snapshots both taken after the event
        # day — or between the last pre-event snapshot and a post-event one — is
        # the provider rewriting history, which is what a PIT study must know.
        ev_day = str(obs[0][2] or "")[:10]
        pre = [o[3] for o in obs if o[1] <= ev_day]
        post = [o[3] for o in obs if o[1] > ev_day]
        moved_after = len(set(post)) > 1
        moved_across = bool(pre and post and post[0] != pre[-1])
        if moved_after or moved_across:
            est_after += 1
        if len({o[4] for o in obs if o[4] is not None}) > 1:
            act_chg += 1
    if n_snap < 2:
        verdict = "UNDETERMINED_NEEDS_TWO_SNAPSHOT_DAYS"
    elif est_after:
        verdict = "PROVIDER_OVERWRITES_ESTIMATES_AFTER_RELEASE"
    elif est_chg:
        verdict = "ESTIMATES_CHANGE_BEFORE_RELEASE_ONLY"
    else:
        verdict = "NO_ESTIMATE_CHANGE_OBSERVED_YET"
    return {"snapshots": n_snap, "events_observed_twice": twice,
            "estimate_changed": est_chg, "estimate_changed_after_event_time": est_after,
            "actual_changed": act_chg, "verdict": verdict}


# =========================================================================== #
# EODHD forward archive
# =========================================================================== #
class EodhdForwardArchiveCollector(BaseCollector):
    source_id = "eodhd_forward_archive"
    requires_credential = True

    def _resolve_key(self) -> Optional[str]:
        for name in self.ctx.source_cfg.get("allowed_env_vars", ["EODHD_API_KEY"]):
            value = self.ctx.env.get(name)
            if value:
                return value
        return None

    def _url(self, path: str, key: str, **params: Any) -> str:
        base = self.ctx.source_cfg.get("base_url", "https://eodhd.com/api").rstrip("/")
        query = {"api_token": key, "fmt": "json"}
        query.update({k: v for k, v in params.items() if v is not None})
        return "%s/%s?%s" % (base, path.lstrip("/"), urllib.parse.urlencode(query))

    def _root(self) -> Path:
        sub = self.ctx.source_cfg.get("vintage_subdir", "vintages/eodhd_forward_archive")
        return Path(self.ctx.archive.output_root) / sub

    def _get_json(self, url: str, *, native_id: str, as_of: str) -> tuple:
        res = self.fetch(url, expect="text", archive=False, extension="json",
                         business_date=as_of, native_id=native_id,
                         content_type="application/json")
        if not res["ok"]:
            return res, None, None
        body = res["body"]
        try:
            return res, json.loads(body.decode("utf-8")), _sha256(body)
        except (ValueError, UnicodeDecodeError):
            self.record_error("PARSE_ERROR", "not JSON: %s" % native_id)
            return res, None, _sha256(body)

    @staticmethod
    def _entitlement(status: Optional[int], parsed: bool) -> str:
        if status == 200 and parsed:
            return ENT_ENTITLED
        if status == 401:
            return ENT_AUTH_FAILED
        if status in (402, 403):
            return ENT_NOT_ENTITLED
        if status == 429:
            return ENT_RATE_LIMITED
        return ENT_UNKNOWN

    # ------------------------------------------------------------------ #
    def audit(self) -> dict:
        key = self._resolve_key()
        if key is None:
            return self.blocked_result(
                SH_BLOCKED_CREDENTIAL,
                "no EODHD credential present (checked env var NAMES %s)"
                % self.ctx.source_cfg.get("allowed_env_vars"), credential_present=False)
        as_of = _dt.date.today().isoformat()
        res, obj, _ = self._get_json(self._url("economic-events", key, limit=1,
                                               **{"from": as_of, "to": as_of}),
                                     native_id="econ_probe", as_of=as_of)
        state = self._entitlement(res.get("status"), obj is not None)
        self.entitlements.append({"family": FAMILY_ECON, "state": state,
                                  "detail": "probe economic-events -> %s" % state,
                                  "http_status": res.get("status")})
        return self.result(overall_state=SH_HEALTHY if state == ENT_ENTITLED else SH_FAILED,
                           credential_present=True,
                           entitlement_summary="%s: %s" % (FAMILY_ECON, state))

    # ------------------------------------------------------------------ #
    def collect(self, as_of: str) -> dict:
        key = self._resolve_key()
        if key is None:
            return self.blocked_result(
                SH_BLOCKED_CREDENTIAL,
                "no EODHD credential present (checked env var NAMES %s)"
                % self.ctx.source_cfg.get("allowed_env_vars"), credential_present=False)
        cfg = self.ctx.source_cfg
        families = set(cfg.get("families") or [FAMILY_ECON, FAMILY_NEWS])
        retrieved = self.ctx.now_iso()
        summary: dict[str, Any] = {}
        if FAMILY_ECON in families:
            summary[FAMILY_ECON] = self._collect_econ(key, as_of, retrieved)
        if FAMILY_NEWS in families:
            summary[FAMILY_NEWS] = self._collect_news(key, as_of, retrieved)
        self.inventory["forward_archive"] = summary
        self.inventory["archive_root"] = str(self._root())
        self.cursor = {"last_collected_as_of": as_of,
                       "families": {f: {k: v for k, v in s.items()
                                        if k in ("state", "rows", "articles_new",
                                                 "symbols_done_today", "universe_size",
                                                 "snapshot_written")}
                                    for f, s in summary.items()}}
        states = [s.get("state") for s in summary.values()]
        if states and all(s == SH_HEALTHY for s in states):
            state = SH_HEALTHY
        elif any(s == SH_HEALTHY for s in states):
            state = SH_DEGRADED
        elif any(s == SH_BLOCKED_CREDENTIAL for s in states):
            state = SH_BLOCKED_CREDENTIAL
        else:
            state = SH_FAILED
        return self.result(overall_state=state, credential_present=True,
                           entitlement_summary="; ".join(
                               "%s=%s" % (f, s.get("state")) for f, s in summary.items()))

    # ---- economic events ---------------------------------------------- #
    def _collect_econ(self, key: str, as_of: str, retrieved: str) -> dict:
        cfg = self.ctx.source_cfg
        root = self._root() / "economic_events"
        snap_path = root / "snapshots" / ("%s.json.gz" % as_of)
        db_path = root / "economic_events.sqlite"
        if snap_path.exists():
            return {"state": SH_HEALTHY, "snapshot_written": False,
                    "note": "today's snapshot already archived (immutable; idempotent no-op)",
                    "overwrite_evidence": econ_estimate_revisions(db_path)}
        frm = _days_before(as_of, int(cfg.get("econ_lookback_days", 7)))
        to = _days_after(as_of, int(cfg.get("econ_lookahead_days", 21)))
        page_size = int(cfg.get("econ_page_size", 1000))
        max_pages = int(cfg.get("econ_max_pages", 2))
        chunk_days = max(1, int(cfg.get("econ_chunk_days", 3)))
        rows: list[dict] = []
        page_hashes: list[str] = []
        status = None
        truncated = False
        # The provider refuses deep offsets (HTTP 422 at offset 2000, measured
        # 2026-10-02), so the window is read in short date chunks that each fit
        # in one or two pages. A chunk whose last allowed page is still full, or
        # whose follow-up page fails, is TRUNCATED — never silently complete.
        c0 = _dt.date.fromisoformat(frm)
        end = _dt.date.fromisoformat(to)
        while c0 <= end:
            c1 = min(end, c0 + _dt.timedelta(days=chunk_days - 1))
            for page in range(max_pages):
                res, obj, sha = self._get_json(
                    self._url("economic-events", key, limit=page_size,
                              offset=page * page_size,
                              **{"from": c0.isoformat(), "to": c1.isoformat()}),
                    native_id="econ|%s|%s|%d" % (c0, c1, page), as_of=as_of)
                status = res.get("status")
                if obj is None or not isinstance(obj, list):
                    if page > 0:
                        truncated = True
                    break
                page_hashes.append(sha)
                rows.extend(r for r in obj if isinstance(r, dict))
                if len(obj) < page_size:
                    break
            else:
                truncated = True
            c0 = c1 + _dt.timedelta(days=1)
        ent = self._entitlement(status, bool(page_hashes))
        self.entitlements.append({"family": FAMILY_ECON, "state": ent,
                                  "detail": "economic-events -> %s" % ent,
                                  "http_status": status})
        if not page_hashes:
            return {"state": SH_BLOCKED_CREDENTIAL if ent == ENT_AUTH_FAILED else SH_FAILED,
                    "snapshot_written": False, "http_status": status}
        payload_sha = _sha256("".join(page_hashes).encode())
        # A fixed historical window re-fetched every day: if the provider rewrites
        # old estimates, this hash moves. Measured, never assumed.
        probe_sha = None
        pw = cfg.get("econ_history_probe_window") or ["2021-01-04", "2021-01-15"]
        pres, pobj, psha = self._get_json(
            self._url("economic-events", key, limit=1000, country="US",
                      **{"from": pw[0], "to": pw[1]}),
            native_id="econ_history_probe|%s|%s" % tuple(pw), as_of=as_of)
        if pobj is not None:
            probe_sha = psha
        snapshot = {"schema": "paper_trader.eodhd_economic_events_snapshot/1",
                    "provider": "EODHD", "endpoint": "economic-events",
                    "snapshot_date": as_of, "ingested_at": retrieved,
                    "window": {"from": frm, "to": to}, "rows": rows,
                    "pages": len(page_hashes), "truncated": truncated,
                    "source_hash": payload_sha,
                    "history_probe": {"window": pw, "source_hash": probe_sha,
                                      "rows": pobj if isinstance(pobj, list) else None},
                    "point_in_time": {"availability": "ingested_at (capture instant)",
                                      "note": "values exactly as served at capture; an "
                                              "estimate is NEVER back-dated before capture"}}
        conn = _open_db(db_path, _ECON_SCHEMA)
        try:
            with conn:
                for r in rows:
                    vals = []
                    for f in _ECON_VALUE_FIELDS:
                        v = r.get(f)
                        try:
                            vals.append(None if v is None else float(v))
                        except (TypeError, ValueError):
                            vals.append(None)
                    conn.execute(
                        "INSERT OR IGNORE INTO observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (econ_event_key(r), as_of, r.get("type"), r.get("country"),
                         r.get("date"), r.get("period"), r.get("comparison"), *vals,
                         r.get("currency"), _sha256(_json_bytes(r)), retrieved))
                wrote = _write_once(snap_path, gzip.compress(_json_bytes(snapshot)))
                if wrote:
                    conn.execute("INSERT OR IGNORE INTO snapshots VALUES (?,?,?,?,?,?,?,?,?)",
                                 (as_of, frm, to, len(rows), len(page_hashes), int(truncated),
                                  payload_sha, probe_sha, retrieved))
        finally:
            conn.close()
        self.note_event_time(as_of)
        return {"state": SH_HEALTHY if not truncated else SH_DEGRADED,
                "snapshot_written": wrote, "rows": len(rows), "pages": len(page_hashes),
                "truncated": truncated, "rows_with_estimate": sum(
                    1 for r in rows if r.get("estimate") is not None),
                "source_hash": payload_sha, "history_probe_hash": probe_sha,
                "overwrite_evidence": econ_estimate_revisions(db_path)}

    # ---- news ---------------------------------------------------------- #
    def _collect_news(self, key: str, as_of: str, retrieved: str) -> dict:
        cfg = self.ctx.source_cfg
        universe = list(cfg.get("sample_symbols") or [])
        for s in load_universe(cfg.get("universe_file")):
            if s not in universe:
                universe.append(s)
        if not universe:
            return {"state": SH_NOT_RUN, "universe_size": 0,
                    "note": "no universe configured"}
        db_path = self._root() / "news" / "news_archive.sqlite"
        conn = _open_db(db_path, _NEWS_SCHEMA)
        try:
            done = {r[0] for r in conn.execute(
                "SELECT query_symbol FROM symbol_days WHERE capture_date=?", (as_of,))}
            todo = [s for s in universe if s not in done]
            cap = int(cfg.get("news_max_symbols_per_run", 80))
            batch = todo[:cap]
            frm = _days_before(as_of, int(cfg.get("news_lookback_days", 4)))
            limit = int(cfg.get("news_page_size", 50))
            max_pages = int(cfg.get("news_max_pages_per_symbol", 4))
            new_total = seen_total = truncated_n = failed = 0
            for sym in batch:
                pages = seen = new = 0
                truncated = page_failed = False
                for page in range(max_pages):
                    res, obj, sha = self._get_json(
                        self._url("news", key, s=sym, limit=limit, offset=page * limit,
                                  **{"from": frm, "to": as_of}),
                        native_id="news|%s|%s|%d" % (sym, as_of, page), as_of=as_of)
                    if obj is None or not isinstance(obj, list):
                        page_failed = True
                        break
                    pages += 1
                    with conn:
                        for row in obj:
                            if not isinstance(row, dict) or not row.get("date"):
                                continue
                            seen += 1
                            aid = news_article_id(row)
                            content = str(row.get("content") or "")
                            link = str(row.get("link") or "")
                            dom = urllib.parse.urlparse(link).netloc.lower() if link else None
                            cur = conn.execute(
                                "INSERT OR IGNORE INTO articles VALUES "
                                "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                (aid, str(row.get("date")), str(row.get("title") or "")[:1000],
                                 link or None, dom,
                                 json.dumps(row.get("symbols") or []),
                                 json.dumps(row.get("tags") or []),
                                 json.dumps(row.get("sentiment")) if row.get("sentiment")
                                 is not None else NOT_AVAILABLE,
                                 _sha256(content.encode("utf-8")), len(content),
                                 content[:_SNIPPET_CHARS], sym, retrieved, as_of, sha))
                            new += cur.rowcount
                            conn.execute("INSERT OR IGNORE INTO article_symbols VALUES (?,?,?)",
                                         (aid, sym, retrieved))
                    if len(obj) < limit:
                        break
                else:
                    truncated = True
                if page_failed and pages == 0:
                    # Nothing landed: no symbol-day row, so the next pass retries it.
                    failed += 1
                    if self.circuit_state == CB_OPEN:
                        break
                    continue
                status = ("TRUNCATED" if truncated else
                          "PARTIAL_LATER_PAGE_FAILED" if page_failed else "COMPLETE")
                with conn:
                    conn.execute("INSERT OR IGNORE INTO symbol_days VALUES (?,?,?,?,?,?,?,?,?,?)",
                                 (sym, as_of, frm, as_of, pages, seen, new, int(truncated),
                                  status, retrieved))
                new_total += new
                seen_total += seen
                truncated_n += int(truncated)
            done_after = conn.execute(
                "SELECT COUNT(*) FROM symbol_days WHERE capture_date=?", (as_of,)).fetchone()[0]
            total_articles = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
        finally:
            conn.close()
        if batch:
            self.note_event_time(as_of)
        state = SH_HEALTHY if (failed == 0 or failed < len(batch)) else SH_FAILED
        if failed and state == SH_HEALTHY:
            state = SH_DEGRADED
        return {"state": state if batch or done_after else SH_FAILED,
                "universe_size": len(universe), "symbols_this_run": len(batch),
                "symbols_done_today": done_after, "symbols_failed": failed,
                "symbols_truncated": truncated_n, "articles_seen": seen_total,
                "articles_new": new_total, "articles_total": total_articles}


# =========================================================================== #
# Public forward archive (no credential)
# =========================================================================== #
def default_ftp_fetch(host: str, user: str, filename: str, timeout: float) -> bytes:
    """Real FTP transport. Tests inject a fake."""
    ftp = ftplib.FTP(host, timeout=timeout)
    try:
        ftp.login(user, "")
        buf = io.BytesIO()
        ftp.retrbinary("RETR %s" % filename, buf.write)
        return buf.getvalue()
    finally:
        try:
            ftp.quit()
        except Exception:  # noqa: BLE001
            pass


class PublicForwardArchiveCollector(BaseCollector):
    source_id = "public_forward_archive"
    requires_credential = False
    ftp_fetch: Callable[[str, str, str, float], bytes] = staticmethod(default_ftp_fetch)

    def _root(self) -> Path:
        sub = self.ctx.source_cfg.get("vintage_subdir", "vintages/public_forward_archive")
        return Path(self.ctx.archive.output_root) / sub

    def audit(self) -> dict:
        return self.result(overall_state=SH_HEALTHY, credential_present=None,
                           entitlement_summary="public sources; no credential")

    def collect(self, as_of: str) -> dict:
        families = set(self.ctx.source_cfg.get("families") or [FAMILY_IBKR, FAMILY_SPDR])
        retrieved = self.ctx.now_iso()
        summary: dict[str, Any] = {}
        if FAMILY_IBKR in families:
            summary[FAMILY_IBKR] = self._collect_ibkr(as_of, retrieved)
        if FAMILY_SPDR in families:
            summary[FAMILY_SPDR] = self._collect_spdr(as_of, retrieved)
        self.inventory["forward_archive"] = summary
        self.inventory["archive_root"] = str(self._root())
        self.cursor = {"last_collected_as_of": as_of,
                       "families": {f: s.get("state") for f, s in summary.items()}}
        states = [s.get("state") for s in summary.values()]
        if states and all(s == SH_HEALTHY for s in states):
            state = SH_HEALTHY
        elif any(s == SH_HEALTHY for s in states):
            state = SH_DEGRADED
        else:
            state = SH_FAILED
        return self.result(overall_state=state, credential_present=None,
                           entitlement_summary="; ".join(
                               "%s=%s" % (f, s.get("state")) for f, s in summary.items()))

    # ---- IBKR shortable shares ------------------------------------------ #
    def _collect_ibkr(self, as_of: str, retrieved: str) -> dict:
        cfg = self.ctx.source_cfg
        files = list(cfg.get("ibkr_files") or ["usa.txt"])
        out_dir = self._root() / "ibkr_shortable" / as_of
        hosts = list(cfg.get("ibkr_hosts") or ["ftp2.interactivebrokers.com",
                                               "ftp3.interactivebrokers.com"])
        written, skipped, errors = 0, 0, []
        meta: dict[str, Any] = {}
        for fname in files:
            path = out_dir / (fname + ".gz")
            if path.exists():
                skipped += 1
                continue
            data = None
            for host in hosts:
                try:
                    data = type(self).ftp_fetch(host, cfg.get("ibkr_user", "shortstock"),
                                                fname, float(cfg.get("ftp_timeout_seconds", 45)))
                    meta[fname] = {"host": host}
                    break
                except Exception as exc:  # noqa: BLE001 - try the next host
                    errors.append("%s@%s: %s" % (fname, host, type(exc).__name__))
            if not data:
                continue
            text = data.decode("latin-1", "replace")
            stamp = next((ln for ln in text.splitlines()[:3] if ln.startswith("#BOF")), None)
            lines = [ln for ln in text.splitlines() if ln and not ln.startswith("#")]
            meta[fname].update({"bytes": len(data), "source_hash": _sha256(data),
                                # every non-'#' line is a data row (the header is '#SYM|...')
                                "data_rows": len(lines),
                                "provider_file_stamp": stamp, "ingested_at": retrieved})
            if _write_once(path, gzip.compress(data)):
                written += 1
                _write_once(out_dir / (fname + ".meta.json"), _json_bytes(meta[fname]))
        for e in errors:
            self.record_error("FTP_ERROR", e)
        if written or skipped:
            self.note_event_time(as_of)
        state = (SH_HEALTHY if (written + skipped) == len(files)
                 else (SH_DEGRADED if (written + skipped) else SH_FAILED))
        return {"state": state, "files_written": written, "files_already_archived": skipped,
                "errors": errors[:6], "files": meta,
                "blocked_external_access": bool(errors) and not (written or skipped)}

    # ---- SPDR ETF NAV history ---------------------------------------------- #
    def _collect_spdr(self, as_of: str, retrieved: str) -> dict:
        cfg = self.ctx.source_cfg
        funds = [str(f).lower() for f in (cfg.get("spdr_funds") or [])]
        tmpl = cfg.get("spdr_url_template") or (
            "https://www.ssga.com/us/en/intermediary/library-content/products/fund-data/"
            "etfs/us/navhist-us-en-%s.xlsx")
        tail_rows = int(cfg.get("spdr_tail_rows", 30))
        root = self._root() / "spdr_nav_history"
        written = skipped = failed = 0
        for fund in funds:
            vpath = root / "vintages" / as_of / ("%s.json" % fund)
            if vpath.exists():
                skipped += 1
                continue
            res = self.fetch(tmpl % fund, expect="binary", archive=False, extension="xlsx",
                             business_date=as_of, native_id="spdr_navhist|%s|%s" % (fund, as_of),
                             content_type="application/vnd.openxmlformats-officedocument."
                                          "spreadsheetml.sheet")
            if not res["ok"]:
                failed += 1
                continue
            body = res["body"]
            try:
                rows = _read_xlsx_rows(body)
            except Exception as exc:  # noqa: BLE001
                self.record_error("PARSE_ERROR", "SPDR workbook %s: %s" % (fund, type(exc).__name__))
                failed += 1
                continue
            hdr_i = next((i for i, r in enumerate(rows)
                          if r and str(r[0] or "").strip().lower() == "date"), None)
            if hdr_i is None:
                self.record_error("PARSE_ERROR", "SPDR workbook %s has no Date header" % fund)
                failed += 1
                continue
            header = [str(x or "").strip() for x in rows[hdr_i]]
            data = [r[:len(header)] for r in rows[hdr_i + 1:] if r and r[0]]
            sha = _sha256(body)
            full = root / "full" / ("%s.json.gz" % fund)
            if not full.exists():
                _write_once(full, gzip.compress(_json_bytes({
                    "fund": fund.upper(), "captured": as_of, "ingested_at": retrieved,
                    "header": header, "rows": data, "source_hash": sha,
                    "note": "first full capture; history as published by the issuer on this date"})))
            vintage = {"schema": "paper_trader.spdr_nav_history_vintage/1",
                       "fund": fund.upper(), "snapshot_date": as_of, "ingested_at": retrieved,
                       "header": header, "recent_rows": data[:tail_rows],
                       "n_rows_in_file": len(data), "source_hash": sha,
                       "provider_last_modified": res.get("published_at")}
            if _write_once(vpath, _json_bytes(vintage)):
                written += 1
        if written or skipped:
            self.note_event_time(as_of)
        state = (SH_HEALTHY if not failed and (written or skipped)
                 else (SH_DEGRADED if (written or skipped) else SH_FAILED))
        return {"state": state, "funds": len(funds), "vintages_written": written,
                "vintages_already_archived": skipped, "failed": failed}


def _read_xlsx_rows(data: bytes) -> list[list]:
    """First worksheet of an .xlsx as rows (stdlib only: zip of XML parts)."""
    import zipfile
    import xml.etree.ElementTree as ET
    ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    z = zipfile.ZipFile(io.BytesIO(data))
    shared: list[str] = []
    if "xl/sharedStrings.xml" in z.namelist():
        for si in ET.fromstring(z.read("xl/sharedStrings.xml")).findall(ns + "si"):
            shared.append("".join(t.text or "" for t in si.iter(ns + "t")))
    sheets = sorted(n for n in z.namelist() if re.match(r"xl/worksheets/sheet\d+\.xml$", n))
    out = []
    for row in ET.fromstring(z.read(sheets[0])).iter(ns + "row"):
        cells: dict[int, Any] = {}
        for c in row.findall(ns + "c"):
            ref = re.match(r"[A-Z]+", c.get("r") or "A")
            col = 0
            for ch in ref.group(0):
                col = col * 26 + (ord(ch) - 64)
            v = c.find(ns + "v")
            t = c.get("t")
            if t == "inlineStr":
                node = c.find(ns + "is")
                val = "".join(x.text or "" for x in node.iter(ns + "t")) if node is not None else None
            elif v is None:
                val = None
            elif t == "s":
                val = shared[int(v.text)]
            elif t in ("str", "b", "e"):
                val = v.text
            else:
                try:
                    val = float(v.text)
                except (TypeError, ValueError):
                    val = v.text
            cells[col - 1] = val
        if cells:
            out.append([cells.get(i) for i in range(max(cells) + 1)])
    return out
