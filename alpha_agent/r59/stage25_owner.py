"""alpha_agent.r59.stage25_owner - the READ-ONLY seam onto the Stage-25/26
candidate registry and its shadow books.

WHY THIS MODULE EXISTS
    The Stage-8 tournament registry is a sqlite file and its shadow books are
    JSON documents. Every consumer that needs to know what that owner holds -
    the owner-reconciling frontier, and the prospective re-arm stage in
    :mod:`alpha_agent.stage26_forward_runtime` - needs to READ it without
    holding a database handle of its own. This is that seam, and nothing else.

WHAT IT GUARANTEES
    * READ-ONLY. The registry is opened through SQLite's ``mode=ro`` URI, so the
      handle itself cannot write, and the directory is never created. A missing,
      locked or unreadable store is reported as absent - never as an empty
      result that would silently drop a candidate.
    * NO MARK IS EVER PRODUCED. A shadow book's marks are counted, never
      written, never inferred and never back-filled. Zero marks is reported as
      zero. The only writer of a mark remains
      ``alpha_agent.tournament.advance_shadow_books``.
    * NO SECOND REGISTRY. This holds no state, creates no table and registers
      no identity. It returns what the Stage-8 owner already says.
"""
from __future__ import annotations

import datetime as _dt
import sqlite3
from pathlib import Path
from typing import Optional

from .. import r59

REGISTRY_DB = "tournament.sqlite"
SHADOW_SUBDIR = "shadow_books"
SHADOW_FILE = "shadow_book.json"

#: A shadow book that is ACTIVE, holds zero marks and was opened more than this
#: many calendar days ago is not "young"; its mark producer is not running.
STALL_GRACE_DAYS = 5

#: Stream states this seam reports. RETIRED means a blocked forward stream, so a
#: stalled book reads as blocked rather than as one quietly accruing. Reporting
#: a dead clock as merely pending is what let 31 days pass unnoticed.
ST_CLOSED = "CLOSED"
ST_ACCRUING = "ACCRUING"
ST_RETIRED = "RETIRED"
ST_AWAITING = "AWAITING_FIRST_MARK"

_QUERY = ("SELECT b.shadow_book_id, b.candidate_id, b.inception_date, b.status, "
          "       c.name, c.family, c.lifecycle_state, c.evidence_status "
          "FROM shadow_books b "
          "LEFT JOIN candidates c ON c.candidate_id = b.candidate_id "
          "ORDER BY b.shadow_book_id")


def days_since(inception: Optional[str], today: Optional[str] = None) -> Optional[int]:
    """Calendar days from a shadow book's inception to ``today``. ``None`` when
    the stored inception date is unreadable. Pure when ``today`` is given."""
    try:
        i = _dt.date.fromisoformat(str(inception)[:10])
    except (TypeError, ValueError):
        return None
    t = _dt.date.fromisoformat(today[:10]) if today else _dt.date.today()
    return (t - i).days


def stream_state(*, status: Optional[str], marks: int,
                 days: Optional[int]) -> str:
    """The forward stream's state, derived from the book - never declared."""
    if str(status or "").upper() != "ACTIVE":
        return ST_CLOSED
    if marks:
        return ST_ACCRUING
    if days is not None and days > STALL_GRACE_DAYS:
        return ST_RETIRED
    return ST_AWAITING


def book_path(root, shadow_book_id: str) -> Path:
    """The one on-disk location of a shadow book document."""
    return Path(root) / SHADOW_SUBDIR / str(shadow_book_id) / SHADOW_FILE


def read_book(root, shadow_book_id: str) -> dict:
    """The stored shadow-book document, or ``{}``. Never writes, never creates."""
    return r59.read_json(book_path(root, shadow_book_id)) or {}


def records(root: Path, *, today: Optional[str] = None) -> Optional[list]:
    """Every shadow-booked Stage-25/26 challenger the legacy registry holds.

    Returns ``None`` when the registry is absent or unreadable, so the caller
    can report the owner as not present instead of as holding nothing.
    """
    db = Path(root) / REGISTRY_DB
    if not db.exists():
        return None
    try:
        con = sqlite3.connect("file:%s?mode=ro" % Path(db).as_posix(), uri=True)
    except sqlite3.Error:
        return None
    try:
        con.row_factory = sqlite3.Row
        rows = con.execute(_QUERY).fetchall()
    except sqlite3.Error:
        return None
    finally:
        con.close()

    out: list = []
    for r in rows:
        book = read_book(root, str(r["shadow_book_id"]))
        inc = book.get("inception") or {}
        spec = inc.get("spec") or {}
        marks = len(book.get("marks") or [])
        inception = inc.get("date") or r["inception_date"]
        days = days_since(inception, today)
        out.append({"challenger_id": str(r["candidate_id"]),
                    "name": r["name"],
                    "family": r["family"],
                    "asset_class": "US_EQUITY",
                    "shadow_book_id": str(r["shadow_book_id"]),
                    "registry_state": r["lifecycle_state"],
                    "evidence_status": r["evidence_status"],
                    "status": r["status"],
                    "inception": inception,
                    "spec_hash": spec.get("spec_hash"),
                    "horizon_days": spec.get("horizon_days"),
                    "membership": len(inc.get("membership") or []),
                    "benchmark": inc.get("benchmark"),
                    "marks": marks,
                    "last_mark": ((book.get("marks") or [])[-1] or {}).get("date")
                    if marks else None,
                    "days_since_inception": days,
                    "stream_state": stream_state(status=r["status"], marks=marks,
                                                 days=days)})
    return out


__all__ = ["REGISTRY_DB", "SHADOW_SUBDIR", "SHADOW_FILE", "STALL_GRACE_DAYS",
           "ST_CLOSED", "ST_ACCRUING", "ST_RETIRED", "ST_AWAITING",
           "days_since", "stream_state", "book_path", "read_book", "records"]
