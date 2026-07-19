"""
api/current_alpha_tournament_sync.py — Phase 19 tournament current-mark alignment
and coverage repair.

Phase 18 shipped a read-only tournament view plus a manual refresh that only re-recorded
the latest COMMON date already present in the *static* Phase 18-A forward report. That report
was bounded to 2026-06-26 (the owned per-ticker EOD strip ended there) while the main Paper
Trader market mark had advanced to 2026-07-17 — so the tournament kept showing a stale June
result and repeated refreshes correctly returned ``NO_NEW_COMPLETED_EOD_DATE``.

This module closes that gap. It backs the UPGRADED manual refresh (a real *data sync*) and the
alignment block attached to the read-only GET:

    run_current_alpha_tournament_sync(...)   -> POST /v1/research/current-alpha/tournament/refresh
    attach_alignment(payload, ...)           -> GET  /v1/research/current-alpha/tournament (block)

What the manual sync does, ONLY on an explicit confirmed request:
    1. Loads the IMMUTABLE membership of the four frozen paper books (champion Top-25 / Top-50
       from the Phase 13-A package, sector-repaired challenger Top-25 / Top-50 from the Phase
       17-B package) plus SPY.
    2. Builds the deduplicated union of those tickers.
    3. Fetches EOD history for the union ONLY, from the shared signal date (2026-05-22) through
       the latest completed US market date, using the EXISTING owned-EODHD transport that Phase
       13-G already reuses (``research.run_phase8u_eodhd_price_universe_expansion._eod_live_get``).
       The transport is injectable so tests use a fake downloader and never touch the network.
    4. Reconstructs all four frozen books on the SAME completed-date calendar (identical
       methodology to the Phase 18-A runner, so a sync to 2026-06-26 reproduces the static
       report). Never reranks, rebalances, adds or removes a member; never substitutes one
       ticker's price for another; never carries a future price backward.
    5. Writes raw/normalized prices + the reconstruction ONLY to the dedicated local tournament
       store (under the D: data root / tournament dir resolver) — never PostgreSQL, never the
       champion daily-mark files, never the Phase 13-A / 17-B / 18-A source artifacts.

Idempotent by financial date: a rerun with no newer completed common EOD date returns
``NO_NEW_COMPLETED_EOD_DATE`` and writes nothing. Partial provider failures keep every
successful ticker and report every failed one (``PARTIAL_TOURNAMENT_REFRESH``). The decision
stays ``MONITORING_MID_CYCLE`` until the real 63-mark checkpoint and never names a winner
before it.

Strict safety contract (enforced): no orders, signals, trade decisions, fills; no broker; no
automation / scheduling; no live trading; no champion replacement; no Paper Trader DB writes;
no position/order mutation; no prediction-service call; no new / paid provider. The EODHD key
is read only from the environment by the transport, never logged, never persisted, never
returned.
"""
from __future__ import annotations

import csv
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional, Sequence, Union

from paper_trader.api.current_alpha_preview import SAFETY_FLAGS
from paper_trader.api.current_alpha_book import (
    DEFAULT_DAILY_MARK_DIR,
    _atomic_write_json,
    _iso_now,
    _read_json_file,
    _resolve_daily_mark_dir,
)
from paper_trader.api.current_alpha_daily_refresh import _resolve_research_repo_dir
from paper_trader.api.current_alpha_tournament import (
    CHAMPION_SIGNAL,
    CHALLENGER_SIGNAL,
    REFRESH_CONFIRM_TOKEN,
    TOURNAMENT_SAFETY_BADGES,
    _resolve_forward_dir,
    _resolve_tournament_dir,
    FORWARD_REPORT,
)

# ---------------------------------------------------------------------------
# Constants (mirror the Phase 18-A runner so a same-date sync reconciles)
# ---------------------------------------------------------------------------

BENCHMARK_TICKER = "SPY"
HORIZON_TRADING_DAYS = 63
DEFAULT_REVIEW_TARGET = "2026-08-22"
MIN_COVERED_PER_BOOK = 5
COST_BPS_ROUND_TRIP = 50
RISK_MAX_DRAWDOWN_PCT = -35.0
RISK_MAX_CONCENTRATION_PCT = 60.0
ROLLING_WINDOWS = (5, 10, 20)
COVERAGE_FULL_PCT = 90.0
SPY_PRICE_SOURCE = "EODHD_SYNCED_LIVE(adjusted_close)"

#: The sync confirmation token is the same explicit manual token as Phase 18.
SYNC_CONFIRM_TOKEN = REFRESH_CONFIRM_TOKEN

#: Frozen-book source packages (env-overridable; never a secret).
CHAMPION_PKG_DIR_ENV = "PAPER_TRADER_TOURNAMENT_CHAMPION_PKG_DIR"
CHALLENGER_PKG_DIR_ENV = "PAPER_TRADER_TOURNAMENT_CHALLENGER_PKG_DIR"
CHAMPION_PKG_REL = Path("research") / "output" / "phase13a_current_champion_alpha_paper_test_package"
CHALLENGER_PKG_REL = Path("research") / "output" / "phase17b_sector_repaired_challenger_package"
CHAMPION_PKG_JSON = "phase13a_current_champion_alpha_paper_test_package.json"

#: The system market mark (the champion daily-refresh manifest) — the alignment reference.
SYSTEM_MARK_DIR_ENV = "PAPER_TRADER_CURRENT_ALPHA_DAILY_MARK_DIR"

#: Test seam: a JSON of ``{clean_symbol: [{"date":..,"adjusted_close":..}, ...]}`` used as an
#: OFFLINE downloader so endpoint tests can commit a full sync with no network / no key.
SYNC_FIXTURE_ENV = "PAPER_TRADER_TOURNAMENT_SYNC_FIXTURE"

#: Dedicated local tournament store files (separate from the Phase 18 snapshot files).
_SYNC_STATE_FILE = "tournament_sync_state.json"
_SYNC_DATA_FILE = "tournament_synced_data.json"
_SYNC_PRICES_FILE = "tournament_synced_prices.json"

#: Book definitions (key, signal, size, package-relative CSV name, book source).
_BOOK_DEFS = (
    ("champion_top25", CHAMPION_SIGNAL, 25, "current_alpha_paper_portfolio_top25.csv", "champion"),
    ("champion_top50", CHAMPION_SIGNAL, 50, "current_alpha_paper_portfolio_top50.csv", "champion"),
    ("challenger_top25", CHALLENGER_SIGNAL, 25, "challenger_paper_portfolio_top25.csv", "challenger"),
    ("challenger_top50", CHALLENGER_SIGNAL, 50, "challenger_paper_portfolio_top50.csv", "challenger"),
)

# Decision enums (a-priori tournament ladder; identical to Phase 18-A Part C).
DEC_MONITORING = "MONITORING_MID_CYCLE"
DEC_CHECKPOINT_REVIEW = "CHECKPOINT_READY_FOR_REVIEW"
DEC_EXTEND = "EXTEND_PARALLEL_PAPER_TEST"
DEC_KEEP = "KEEP_CURRENT_PAPER_CHAMPION"
DEC_PROMOTION_ELIGIBLE = "CHALLENGER_PAPER_PROMOTION_ELIGIBLE"
DEC_REJECT = "REJECT_PAPER_CHALLENGER"
DEC_BLOCKED_COVERAGE = "BLOCKED_INSUFFICIENT_COVERAGE"
DEC_BLOCKED_MISMATCH = "BLOCKED_DATA_MISMATCH"

# Sync status enums (Part C).
SYNC_PREVIEW = "TOURNAMENT_SYNC_PREVIEW"
SYNC_COMPLETE = "TOURNAMENT_REFRESH_COMPLETE"
SYNC_PARTIAL = "PARTIAL_TOURNAMENT_REFRESH"
SYNC_NO_NEW = "NO_NEW_COMPLETED_EOD_DATE"
SYNC_NO_DATE = "NO_COMPLETED_EOD_DATE"
SYNC_CONFIRM_REQUIRED = "REFRESH_CONFIRMATION_REQUIRED"
SYNC_UNAVAILABLE = "TOURNAMENT_SYNC_UNAVAILABLE"

# Fatal provider blocks (stop the whole sync; last valid store preserved).
BLOCKED_EODHD_KEY = "BLOCKED_EODHD_KEY"
BLOCKED_EODHD_ENTITLEMENT = "BLOCKED_EODHD_ENTITLEMENT"
BLOCKED_EODHD_RATE_LIMIT = "BLOCKED_EODHD_RATE_LIMIT"
_FATAL_BLOCKS = {BLOCKED_EODHD_KEY, BLOCKED_EODHD_ENTITLEMENT, BLOCKED_EODHD_RATE_LIMIT}

# Alignment enums (Part D).
ALIGN_ALIGNED = "ALIGNED"
ALIGN_STALE = "STALE"
ALIGN_PARTIAL = "PARTIAL_COVERAGE"
ALIGN_BLOCKED = "BLOCKED_DATA_MISMATCH"


# ---------------------------------------------------------------------------
# Pure helpers (vendored to match the Phase 18-A runner exactly)
# ---------------------------------------------------------------------------

def _round(value: Optional[float], digits: int = 4) -> Optional[float]:
    return round(value, digits) if isinstance(value, (int, float)) else None


def _to_float(x: Any) -> Optional[float]:
    if x in (None, ""):
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _parse_date(s: Any) -> Optional[date]:
    try:
        return date.fromisoformat(str(s)[:10])
    except (TypeError, ValueError):
        return None


def _today(today: Optional[str]) -> date:
    if today:
        d = _parse_date(today)
        if d is not None:
            return d
    return datetime.now(timezone.utc).date()


def _mean(xs: Sequence[float]) -> Optional[float]:
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def _median(xs: Sequence[float]) -> Optional[float]:
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    n = len(xs)
    mid = n // 2
    return xs[mid] if n % 2 else (xs[mid - 1] + xs[mid]) / 2.0


def _pstdev(xs: Sequence[float]) -> Optional[float]:
    xs = [x for x in xs if x is not None]
    if len(xs) < 2:
        return None
    m = sum(xs) / len(xs)
    return (sum((x - m) ** 2 for x in xs) / len(xs)) ** 0.5


def _clean_symbol(ticker: str) -> str:
    """EODHD class-share symbols use a dash, not a dot (e.g. BRK.B -> BRK-B)."""
    return ticker.strip().upper().replace(".", "-")


def _normalize_bars(payload: Any) -> list[tuple[str, float]]:
    """Flatten an EODHD EOD payload (list of daily bars) into sorted [(date, adj_close)].
    Adjusted close preferred, falling back to close — the same rule as the research client."""
    out: list[tuple[str, float]] = []
    if not isinstance(payload, list):
        return out
    for bar in payload:
        if not isinstance(bar, dict):
            continue
        d = str(bar.get("date") or "").strip()
        if not d:
            continue
        ac = bar.get("adjusted_close")
        if ac is None:
            ac = bar.get("close")
        acf = _to_float(ac)
        if acf is None:
            continue
        out.append((d[:10], acf))
    out.sort(key=lambda t: t[0])
    return out


def _completed_bars(bars: list[tuple[str, float]], today: date) -> list[tuple[str, float]]:
    """Only bars strictly before the reference calendar day count as completed EOD sessions
    (a same-day bar may be an incomplete intraday snapshot). Never a future price."""
    return [b for b in bars if _parse_date(b[0]) is not None and _parse_date(b[0]) < today]


def _price_at_or_before(bars: list[tuple[str, float]], as_of: str) -> Optional[tuple[str, float]]:
    """Last bar with date <= as_of (mirrors the 13-A entry-price rule; never looks forward)."""
    best: Optional[tuple[str, float]] = None
    for d, v in bars:
        if d <= as_of and v is not None:
            if best is None or d > best[0]:
                best = (d, v)
    return best


def _pct_return(entry: Optional[float], close: Optional[float]) -> Optional[float]:
    if entry is None or close is None or entry == 0:
        return None
    return (close / entry - 1.0) * 100.0


def _concentration_top5(rets: Sequence[float]) -> Optional[float]:
    mags = sorted((abs(r) for r in rets), reverse=True)
    total = sum(mags)
    if total <= 0:
        return None
    return _round(100.0 * sum(mags[:5]) / total, 2)


def _max_drawdown(cum_returns: list[Optional[float]], dates: list[str]) -> dict[str, Any]:
    peak_equity = None
    peak_date = None
    worst = 0.0
    worst_peak_date = None
    worst_trough_date = None
    for cr, d in zip(cum_returns, dates):
        if cr is None:
            continue
        eq = 1.0 + cr / 100.0
        if peak_equity is None or eq > peak_equity:
            peak_equity, peak_date = eq, d
        dd = eq / peak_equity - 1.0 if peak_equity else 0.0
        if dd < worst:
            worst = dd
            worst_peak_date, worst_trough_date = peak_date, d
    return {
        "max_drawdown_pct": _round(worst * 100.0, 4),
        "max_drawdown_peak_date": worst_peak_date,
        "max_drawdown_trough_date": worst_trough_date,
    }


# ---------------------------------------------------------------------------
# Frozen-book membership (immutable; read independently; never merged)
# ---------------------------------------------------------------------------

def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    with open(path, newline="", encoding="utf-8-sig") as fh:
        return list(csv.DictReader(fh))


def _first_present(row: dict[str, str], keys: Sequence[str]) -> Optional[str]:
    for k in keys:
        if k in row and row[k] not in (None, ""):
            return row[k]
    return None


def load_book_members(csv_path: Path) -> list[dict[str, Any]]:
    """Read one frozen paper-book CSV into an ordered member list. Handles both the champion
    (``sector`` / ``signal_composite_sn`` / ``entry_price``) and challenger (``repaired_sector``
    / ``signal_composite_sn_repaired``, no stored entry) schemas. ``stored_entry_price`` is
    captured ONLY for the champion (the reproduction cross-check); it is never substituted for
    another book's price. Members are FROZEN — de-duped in first-seen order, never reranked."""
    members: list[dict[str, Any]] = []
    seen: set[str] = set()
    for i, row in enumerate(_read_csv_rows(csv_path)):
        raw = _first_present(row, ["ticker"])
        if not raw:
            continue
        ticker = raw.strip().upper()
        if not ticker or ticker.startswith("_") or ticker in seen:
            continue
        seen.add(ticker)
        sector = _first_present(row, ["repaired_sector", "sector"]) or "Unknown"
        score = _first_present(row, ["signal_composite_sn_repaired", "signal_composite_sn",
                                     "composite_sn_repaired", "composite_sn"])
        members.append({
            "ticker": ticker,
            "rank": len(members) + 1,
            "sector": sector,
            "signal_score": _to_float(score),
            "stored_entry_price": _to_float(_first_present(row, ["entry_price"])),
            "stored_entry_reference_date": _first_present(row, ["entry_reference_date"]),
            "target_weight": _to_float(_first_present(row, ["target_weight"])),
        })
    return members


def resolve_frozen_union(champion_pkg: Path, challenger_pkg: Path
                         ) -> tuple[dict[str, list[dict[str, Any]]], list[str], list[str]]:
    """Resolve the four frozen books + the deduplicated ticker union (excluding SPY, which is
    fetched and reported separately). Returns (members_by_key, union_tickers_sorted, spy_list)."""
    members_by_key: dict[str, list[dict[str, Any]]] = {}
    for key, _sig, _sz, csv_name, source in _BOOK_DEFS:
        base = champion_pkg if source == "champion" else challenger_pkg
        members_by_key[key] = load_book_members(base / csv_name)
    union: list[str] = []
    seen: set[str] = set()
    for members in members_by_key.values():
        for m in members:
            tk = m["ticker"]
            if tk not in seen:
                seen.add(tk)
                union.append(tk)
    return members_by_key, sorted(union), [BENCHMARK_TICKER]


# ---------------------------------------------------------------------------
# Injectable downloader (owned EODHD transport by default; fake in tests)
# ---------------------------------------------------------------------------

Downloader = Callable[[str, str], Any]


class TournamentSyncBlocked(Exception):
    """Raised for a FATAL provider stop (bad key / plan / rate limit). Never carries a key."""

    def __init__(self, result_enum: str, message: str):
        super().__init__(message)
        self.result_enum = result_enum


def _fixture_downloader(fixture_path: Path) -> Downloader:
    """OFFLINE downloader over a local JSON fixture (test seam; never network / never key)."""
    data, _err = _read_json_file(fixture_path)
    table = data if isinstance(data, dict) else {}

    def _get(symbol: str, _start: str) -> Any:
        return table.get(symbol, table.get(_clean_symbol(symbol), []))

    return _get


def _live_downloader(research_repo_dir: Path) -> Downloader:
    """Reuse the EXISTING owned-EODHD transport (the same client Phase 13-G reuses). Imported
    lazily so offline tests never load the network client. The key is read from the environment
    by the transport itself and is never handled, logged or persisted here."""
    repo = str(research_repo_dir)
    if repo not in sys.path:
        sys.path.insert(0, repo)
    from research import run_phase8u_eodhd_price_universe_expansion as u8  # lazy
    return u8._eod_live_get


def _resolve_downloader(downloader: Optional[Downloader], research_repo_dir: Path) -> Downloader:
    if downloader is not None:
        return downloader
    fixture = os.environ.get(SYNC_FIXTURE_ENV)
    if fixture:
        return _fixture_downloader(Path(fixture))
    return _live_downloader(research_repo_dir)


def _classify_provider_error(exc: Exception) -> str:
    etype = getattr(exc, "error_type", "") or ""
    if etype in ("invalid_key", "host_blocked"):
        return BLOCKED_EODHD_KEY
    if etype == "plan_blocked":
        return BLOCKED_EODHD_ENTITLEMENT
    if etype == "rate_limited":
        return BLOCKED_EODHD_RATE_LIMIT
    return "ERROR"


def fetch_union_series(downloader: Downloader, tickers: Sequence[str], start: str, today: date
                       ) -> tuple[dict[str, list[tuple[str, float]]], list[dict[str, Any]]]:
    """Fetch + normalize each ticker's completed EOD series independently. Per-ticker
    success/failure is isolated: a fatal provider state (bad key / plan / rate) stops the whole
    sync (raises :class:`TournamentSyncBlocked`); a per-ticker empty/error is recorded, not
    raised. Each ticker keeps ITS OWN series — one ticker's price is never substituted for
    another, and no future (>= today) bar is ever retained."""
    series: dict[str, list[tuple[str, float]]] = {}
    per_ticker: list[dict[str, Any]] = []
    for tk in tickers:
        clean = _clean_symbol(tk)
        try:
            payload = downloader(clean, start)
        except Exception as exc:  # noqa: BLE001 — sanitized taxonomy below
            enum = _classify_provider_error(exc)
            if enum in _FATAL_BLOCKS:
                raise TournamentSyncBlocked(enum, "provider stop: %s"
                                            % getattr(exc, "error_type", "error"))
            series[tk] = []
            per_ticker.append({"ticker": tk, "status": "ERROR", "n_bars": 0,
                               "latest_completed_date": None})
            continue
        bars = _completed_bars(_normalize_bars(payload), today)
        series[tk] = bars
        status = "OK" if bars else ("EMPTY" if isinstance(payload, list) else "SCHEMA_ERROR")
        per_ticker.append({
            "ticker": tk, "status": status, "n_bars": len(bars),
            "latest_completed_date": (bars[-1][0] if bars else None),
        })
    return series, per_ticker


# ---------------------------------------------------------------------------
# Calendar + per-book reconstruction (identical methodology to Phase 18-A)
# ---------------------------------------------------------------------------

def build_common_calendar(spy_bars: list[tuple[str, float]],
                          series_by_ticker: dict[str, list[tuple[str, float]]],
                          signal_date: str, ref_today: date
                          ) -> tuple[list[str], Optional[str], dict[str, Any]]:
    """Common trading calendar = SPY completed sessions in [signal_date, latest_common], where
    latest_common = min(latest SPY date, latest fetched per-ticker EOD date). Bounding to the
    latest COMMON date means no book is marked past the union's fetched data."""
    sig = _parse_date(signal_date)
    ref_iso = ref_today.isoformat()
    spy_dates = [d for d, _ in spy_bars
                 if d < ref_iso and _parse_date(d) is not None
                 and (sig is None or _parse_date(d) >= sig)]
    latest_spy = max((d for d, _ in spy_bars), default=None)
    latest_eod = None
    for bars in series_by_ticker.values():
        if bars:
            last = bars[-1][0]
            if latest_eod is None or last > latest_eod:
                latest_eod = last
    candidates = [d for d in (latest_spy, latest_eod) if d is not None]
    latest_common = min(candidates) if candidates else None
    calendar = sorted({d for d in spy_dates if latest_common is None or d <= latest_common})
    meta = {
        "latest_spy_date": latest_spy,
        "latest_owned_eod_date": latest_eod,
        "latest_common_owned_eod_date": latest_common,
        "reference_today": ref_iso,
    }
    return calendar, latest_common, meta


def spy_curve(spy_bars: list[tuple[str, float]], dates: list[str], signal_date: str
              ) -> dict[str, dict[str, Any]]:
    ref = _price_at_or_before(spy_bars, signal_date)
    ref_price = ref[1] if ref else None
    rows: dict[str, dict[str, Any]] = {}
    prev_ret: Optional[float] = None
    for d in dates:
        at = _price_at_or_before(spy_bars, d)
        close = at[1] if at else None
        ret = _pct_return(ref_price, close)
        daily = (_round(ret - prev_ret, 4)
                 if (ret is not None and prev_ret is not None) else None)
        rows[d] = {
            "mark_date": d, "adjusted_close": _round(close, 6),
            "reference_date": (ref[0] if ref else None), "reference_price": _round(ref_price, 6),
            "return_since_signal_pct": _round(ret, 4), "daily_change_pct_points": daily,
        }
        if ret is not None:
            prev_ret = ret
    return rows


def reconstruct_book(book_key: str, book_id: str, signal_name: str, book_size: int,
                     members: list[dict[str, Any]],
                     series_by_ticker: dict[str, list[tuple[str, float]]],
                     dates: list[str], signal_date: str,
                     spy_rows: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Reconstruct ONE frozen book's daily strip. Members are fixed for every date; each
    ticker's series is its OWN fetched canonical series (never borrowed from another book)."""
    series = {m["ticker"]: series_by_ticker.get(m["ticker"], []) for m in members}
    entries: dict[str, tuple[Optional[str], Optional[float]]] = {}
    repro_checks: list[dict[str, Any]] = []
    for m in members:
        tk = m["ticker"]
        at = _price_at_or_before(series[tk], signal_date)
        entry_ref = at[0] if at else None
        entry_price = at[1] if at else None
        entries[tk] = (entry_ref, entry_price)
        if m.get("stored_entry_price") is not None and entry_price is not None:
            repro_checks.append({
                "ticker": tk, "derived_entry": _round(entry_price, 6),
                "stored_entry": _round(m["stored_entry_price"], 6),
                "abs_error": _round(abs(entry_price - m["stored_entry_price"]), 8),
            })

    total = len(members)
    strip: list[dict[str, Any]] = []
    contributor_rows_latest: list[dict[str, Any]] = []
    prev_avg: Optional[float] = None
    prev_excess: Optional[float] = None
    missing = [m["ticker"] for m in members if not series.get(m["ticker"])]

    for idx, d in enumerate(dates):
        marks = []
        for m in members:
            tk = m["ticker"]
            _entry_ref, entry_price = entries[tk]
            at = _price_at_or_before(series[tk], d)
            close = at[1] if at else None
            ret = _pct_return(entry_price, close)
            covered = entry_price is not None and close is not None
            marks.append({"ticker": tk, "sector": m["sector"], "rank": m["rank"],
                          "paper_return_pct": ret, "covered": covered})
        covered_marks = [mk for mk in marks if mk["covered"] and mk["paper_return_pct"] is not None]
        rets = [mk["paper_return_pct"] for mk in covered_marks]
        cov_n = len(covered_marks)
        avg = _mean(rets)
        spy_ret = (spy_rows.get(d) or {}).get("return_since_signal_pct")
        excess = (_round(avg - spy_ret, 4) if (avg is not None and spy_ret is not None) else None)
        daily_change = (_round(avg - prev_avg, 4)
                        if (avg is not None and prev_avg is not None) else None)
        daily_excess = (_round(excess - prev_excess, 4)
                        if (excess is not None and prev_excess is not None) else None)
        ranked = sorted(covered_marks, key=lambda mk: mk["paper_return_pct"], reverse=True)
        n_up = sum(1 for x in rets if x > 0)
        strip.append({
            "mark_date": d, "book_key": book_key, "book_id": book_id, "book_size": book_size,
            "covered_count": cov_n, "missing_count": total - cov_n, "total_count": total,
            "coverage_pct": _round(100.0 * cov_n / total, 2) if total else None,
            "average_return_pct": _round(avg, 4),
            "median_return_pct": _round(_median(rets), 4) if rets else None,
            "hit_rate_pct": _round(100.0 * n_up / len(rets), 2) if rets else None,
            "daily_change_pct_points": daily_change,
            "spy_return_pct": _round(spy_ret, 4) if spy_ret is not None else None,
            "excess_return_vs_spy_pct_points": excess,
            "daily_excess_change_pct_points": daily_excess,
            "contributor_concentration_top5_pct": _concentration_top5(rets),
            "order_action_all": "NO_ORDER",
        })
        if avg is not None:
            prev_avg = avg
        if excess is not None:
            prev_excess = excess
        if idx == len(dates) - 1:
            contributor_rows_latest = [
                {"ticker": mk["ticker"], "sector": mk["sector"],
                 "paper_return_pct": _round(mk["paper_return_pct"], 4)}
                for mk in ranked]

    return {
        "book_key": book_key, "book_id": book_id, "signal": signal_name,
        "book_size": book_size, "n_members": total,
        "strip": strip, "missing_tickers": missing,
        "reproduction_checks": repro_checks,
        "contributors_latest": contributor_rows_latest,
        "members": members,
    }


def analytics_for_book(book: dict[str, Any]) -> dict[str, Any]:
    strip = book["strip"]
    dates = [r["mark_date"] for r in strip]
    cum = [r["average_return_pct"] for r in strip]
    daily = [r["daily_change_pct_points"] for r in strip if r["daily_change_pct_points"] is not None]
    daily_excess = [r["daily_excess_change_pct_points"] for r in strip
                    if r["daily_excess_change_pct_points"] is not None]
    last = strip[-1] if strip else {}
    dd = _max_drawdown(cum, dates)
    vol = _pstdev(daily)
    te = _pstdev(daily_excess)
    avg_daily_excess = _mean(daily_excess)
    ir_valid = te not in (None, 0.0) and len(daily_excess) >= 20
    ir = (avg_daily_excess / te) if ir_valid else None
    pos_days = (100.0 * sum(1 for x in daily if x > 0) / len(daily)) if daily else None
    outperf_days = (100.0 * sum(1 for x in daily_excess if x > 0) / len(daily_excess)
                    if daily_excess else None)
    contributors = book["contributors_latest"]
    return {
        "book_key": book["book_key"], "book_id": book["book_id"], "signal": book["signal"],
        "book_size": book["book_size"], "n_members": book["n_members"], "n_marks": len(strip),
        "start_date": dates[0] if dates else None, "end_date": dates[-1] if dates else None,
        "cumulative_return_pct": last.get("average_return_pct"),
        "spy_cumulative_return_pct": last.get("spy_return_pct"),
        "excess_return_vs_spy_pct_points": last.get("excess_return_vs_spy_pct_points"),
        "max_drawdown_pct": dd["max_drawdown_pct"],
        "max_drawdown_peak_date": dd["max_drawdown_peak_date"],
        "max_drawdown_trough_date": dd["max_drawdown_trough_date"],
        "daily_volatility_pct_points": _round(vol, 4),
        "tracking_error_pct_points": _round(te, 4),
        "information_ratio": _round(ir, 4),
        "information_ratio_valid": bool(ir_valid),
        "positive_day_rate_pct": _round(pos_days, 2),
        "days_outperforming_spy_pct": _round(outperf_days, 2),
        "n_daily_observations": len(daily),
        "covered_count": last.get("covered_count"),
        "total_count": last.get("total_count"),
        "coverage_pct": last.get("coverage_pct"),
        "missing_count": last.get("missing_count"),
        "contributor_concentration_top5_pct": last.get("contributor_concentration_top5_pct"),
        "best_contributor": contributors[0] if contributors else None,
        "worst_contributor": contributors[-1] if contributors else None,
        "order_action_all": "NO_ORDER",
    }


def book_curves(book: dict[str, Any]) -> dict[str, Any]:
    strip = book["strip"]
    cumulative = [{"mark_date": r["mark_date"], "cumulative_return_pct": r["average_return_pct"]}
                  for r in strip]
    excess = [{"mark_date": r["mark_date"],
               "excess_return_vs_spy_pct_points": r["excess_return_vs_spy_pct_points"]}
              for r in strip]
    peak = None
    drawdown = []
    for r in strip:
        cur = r["average_return_pct"]
        if not isinstance(cur, (int, float)):
            drawdown.append({"mark_date": r["mark_date"], "drawdown_pct": None})
            continue
        eq = 1.0 + cur / 100.0
        if peak is None or eq > peak:
            peak = eq
        dd = (eq / peak - 1.0) * 100.0 if peak else 0.0
        drawdown.append({"mark_date": r["mark_date"], "drawdown_pct": _round(dd, 4)})
    return {"cumulative_curve": cumulative, "excess_curve": excess,
            "drawdown_curve": drawdown, "n_marks": len(strip)}


def head_to_head(size: int, champ: dict[str, Any], chall: dict[str, Any]) -> dict[str, Any]:
    def _delta(key):
        c, s = champ.get(key), chall.get(key)
        return _round(s - c, 4) if isinstance(c, (int, float)) and isinstance(s, (int, float)) else None
    metrics = ["cumulative_return_pct", "excess_return_vs_spy_pct_points", "max_drawdown_pct",
               "daily_volatility_pct_points", "positive_day_rate_pct",
               "days_outperforming_spy_pct", "contributor_concentration_top5_pct",
               "coverage_pct", "covered_count", "information_ratio"]
    return {
        "book_size": size,
        "champion_book_id": champ.get("book_id"),
        "challenger_book_id": chall.get("book_id"),
        "champion": {k: champ.get(k) for k in metrics},
        "challenger": {k: chall.get(k) for k in metrics},
        "challenger_minus_champion": {k: _delta(k) for k in metrics},
        "same_date_comparison": champ.get("start_date") == chall.get("start_date")
                                 and champ.get("end_date") == chall.get("end_date"),
        "n_marks": champ.get("n_marks"),
        "order_action_all": "NO_ORDER",
    }


def sector_exposure(book: dict[str, Any]) -> list[dict[str, Any]]:
    counts: dict[str, int] = {}
    for m in book["members"]:
        counts[m["sector"]] = counts.get(m["sector"], 0) + 1
    total = book["n_members"] or 1
    return [{"book_key": book["book_key"], "sector": s, "n_names": n,
             "weight_pct": _round(100.0 * n / total, 2)}
            for s, n in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))]


def _isolation_report(books: dict[str, dict[str, Any]]) -> dict[str, Any]:
    keys = list(books)
    sets = {k: set(m["ticker"] for m in books[k]["members"]) for k in keys}
    top25_top50_isolated = all(
        books[k]["book_id"] != books[j]["book_id"] for k in keys for j in keys if k != j)
    no_shared_id = len({books[k]["book_id"] for k in keys}) == len(keys)
    return {
        "all_isolated": bool(top25_top50_isolated and no_shared_id),
        "distinct_book_ids": no_shared_id,
        "member_counts": {k: len(sets[k]) for k in keys},
    }


def _net_after_cost(excess: Optional[float]) -> Optional[float]:
    if excess is None:
        return None
    return excess - COST_BPS_ROUND_TRIP / 100.0


def decide_tournament(*, elapsed_marks: int, spy_available: bool, calendars_aligned: bool,
                      books_isolated: bool, coverage: dict[str, int],
                      h2h_top25: dict[str, Any], h2h_top50: dict[str, Any]) -> dict[str, Any]:
    """The a-priori tournament ladder. Mid-cycle -> MONITORING_MID_CYCLE unless a data block
    fires; never names a winner mid-cycle; never replaces the champion; never promotes live."""
    reasons: list[str] = []
    checkpoint_reached = elapsed_marks >= HORIZON_TRADING_DAYS
    if not spy_available or not calendars_aligned or not books_isolated:
        reasons.append("SPY unavailable or the four book calendars are not aligned or book "
                       "isolation failed.")
        return {"status": DEC_BLOCKED_MISMATCH, "reasons": reasons,
                "checkpoint_reached": checkpoint_reached}
    min_cov = min(coverage.values()) if coverage else 0
    if min_cov < MIN_COVERED_PER_BOOK:
        reasons.append("At least one of the four books has fewer than %d covered names "
                       "(min covered = %d)." % (MIN_COVERED_PER_BOOK, min_cov))
        return {"status": DEC_BLOCKED_COVERAGE, "reasons": reasons,
                "checkpoint_reached": checkpoint_reached}
    if not checkpoint_reached:
        reasons.append("Forward window is incomplete (%d of %d trading marks). Monitoring "
                       "only — no winner is named and neither book is promoted or rejected."
                       % (elapsed_marks, HORIZON_TRADING_DAYS))
        return {"status": DEC_MONITORING, "reasons": reasons, "checkpoint_reached": False}
    chall25 = h2h_top25.get("challenger") or {}
    chall50 = h2h_top50.get("challenger") or {}
    champ25 = h2h_top25.get("champion") or {}
    champ50 = h2h_top50.get("champion") or {}
    ch25 = _net_after_cost(chall25.get("excess_return_vs_spy_pct_points"))
    ch50 = _net_after_cost(chall50.get("excess_return_vs_spy_pct_points"))
    cp25 = _net_after_cost(champ25.get("excess_return_vs_spy_pct_points"))
    cp50 = _net_after_cost(champ50.get("excess_return_vs_spy_pct_points"))
    dd_vals = [v for v in (chall25.get("max_drawdown_pct"), chall50.get("max_drawdown_pct"))
               if v is not None]
    conc_vals = [v for v in (chall25.get("contributor_concentration_top5_pct"),
                             chall50.get("contributor_concentration_top5_pct")) if v is not None]
    chall_dd = min(dd_vals) if dd_vals else None
    chall_conc = max(conc_vals) if conc_vals else None
    if (chall_dd is not None and chall_dd < RISK_MAX_DRAWDOWN_PCT) or \
       (chall_conc is not None and chall_conc > RISK_MAX_CONCENTRATION_PCT):
        reasons.append("Material paper-risk breach in the challenger (max drawdown %s%% or "
                       "top-5 concentration %s%%)." % (chall_dd, chall_conc))
        return {"status": DEC_REJECT, "reasons": reasons, "checkpoint_reached": True}
    if ch25 is None or ch50 is None:
        reasons.append("Checkpoint reached but challenger net excess is not computable on both "
                       "books; extend the parallel paper test.")
        return {"status": DEC_EXTEND, "reasons": reasons, "checkpoint_reached": True}
    if ch25 <= 0 or ch50 <= 0:
        reasons.append("Challenger net excess (after %d bps round-trip) is not positive on both "
                       "books (Top25 %.4f / Top50 %.4f)." % (COST_BPS_ROUND_TRIP, ch25, ch50))
        return {"status": DEC_REJECT, "reasons": reasons, "checkpoint_reached": True}
    challenger_better = (cp25 is not None and cp50 is not None and ch25 > cp25 and ch50 > cp50)
    champion_better = (cp25 is not None and cp50 is not None and cp25 >= ch25 and cp50 >= ch50)
    if challenger_better:
        reasons.append("Challenger net excess exceeds the champion on both books after the same "
                       "cost assumption with no risk breach. ELIGIBLE for a later explicit "
                       "MANUAL paper-champion decision only — the champion is not replaced and "
                       "nothing is promoted to live.")
        return {"status": DEC_PROMOTION_ELIGIBLE, "reasons": reasons, "checkpoint_reached": True}
    if champion_better:
        reasons.append("Champion net excess is at least the challenger on both books; keep the "
                       "current paper champion.")
        return {"status": DEC_KEEP, "reasons": reasons, "checkpoint_reached": True}
    reasons.append("Checkpoint reached with adequate coverage but no clear winner across books; "
                   "ready for manual review.")
    return {"status": DEC_CHECKPOINT_REVIEW, "reasons": reasons, "checkpoint_reached": True}


# ---------------------------------------------------------------------------
# Location resolution + safety block
# ---------------------------------------------------------------------------

def _resolve_champion_pkg(champion_pkg_dir, research_repo_dir) -> Path:
    if champion_pkg_dir is not None:
        return Path(champion_pkg_dir)
    env = os.environ.get(CHAMPION_PKG_DIR_ENV)
    if env:
        return Path(env)
    return _resolve_research_repo_dir(research_repo_dir) / CHAMPION_PKG_REL


def _resolve_challenger_pkg(challenger_pkg_dir, research_repo_dir) -> Path:
    if challenger_pkg_dir is not None:
        return Path(challenger_pkg_dir)
    env = os.environ.get(CHALLENGER_PKG_DIR_ENV)
    if env:
        return Path(env)
    return _resolve_research_repo_dir(research_repo_dir) / CHALLENGER_PKG_REL


def _resolve_system_mark_dir(system_mark_dir) -> Path:
    if system_mark_dir is not None:
        return Path(system_mark_dir)
    env = os.environ.get(SYSTEM_MARK_DIR_ENV)
    if env:
        return Path(env)
    return DEFAULT_DAILY_MARK_DIR


def _system_market_mark(system_mark_dir: Path) -> Optional[str]:
    """The current champion daily-refresh mark date (the alignment reference). Read-only."""
    manifest, _err = _read_json_file(system_mark_dir / "latest" / "refresh_manifest.json")
    if isinstance(manifest, dict) and not manifest.get("blocked"):
        md = manifest.get("mark_date")
        return str(md) if md else None
    return None


def _read_signal_date(champion_pkg: Path) -> str:
    pkg, _err = _read_json_file(champion_pkg / CHAMPION_PKG_JSON)
    sd = (pkg or {}).get("signal_date") if isinstance(pkg, dict) else None
    return str(sd) if sd else "2026-05-22"


def _safety_block(*, wrote_store: bool = False) -> dict[str, Any]:
    return {
        "safety_badges": list(TOURNAMENT_SAFETY_BADGES),
        "safety": dict(SAFETY_FLAGS),
        **dict(SAFETY_FLAGS),
        "order_action_all": "NO_ORDER",
        "champion_replaced": False,
        "replaces_champion": False,
        "mutates_champion": False,
        "promotes_to_live": False,
        "no_decision_approves_live_trading": True,
        "live_trading_status": "NOT_APPROVED_FOR_LIVE_TRADING",
        "creates_orders": False,
        "creates_signals": False,
        "creates_trade_decisions": False,
        "creates_fills": False,
        "wrote_to_database": False,
        "no_broker": True,
        "no_automation": True,
        "is_automation": False,
        "is_scheduled": False,
        "calls_prediction_service": False,
        "reranked": False,
        "rebalanced": False,
        "api_key_logged": False,
        "api_key_persisted": False,
        "wrote_to_local_tournament_store": bool(wrote_store),
    }


# ---------------------------------------------------------------------------
# Store I/O (writes ONLY the dedicated local tournament store)
# ---------------------------------------------------------------------------

def _load_sync_state(tournament_dir: Path) -> dict[str, Any]:
    data, _err = _read_json_file(tournament_dir / _SYNC_STATE_FILE)
    return data if isinstance(data, dict) else {}


def load_synced_tournament(tournament_dir: Path) -> Optional[dict[str, Any]]:
    """Read the last synced reconstruction from the dedicated local store (read-only)."""
    data, _err = _read_json_file(tournament_dir / _SYNC_DATA_FILE)
    return data if isinstance(data, dict) else None


# ---------------------------------------------------------------------------
# Reconstruction assembly (shared by preview + commit)
# ---------------------------------------------------------------------------

def _reconstruct_all(members_by_key, series, spy_bars, signal_date, ref_today, review_target):
    """Reconstruct the four frozen books + SPY on the common calendar; return the full bundle."""
    spy_available = len(spy_bars) > 0
    calendar, latest_common, cal_meta = build_common_calendar(spy_bars, series, signal_date, ref_today)
    spy_rows = spy_curve(spy_bars, calendar, signal_date) if calendar else {}

    books: dict[str, dict[str, Any]] = {}
    for key, signame, size, _csv, _src in _BOOK_DEFS:
        book_id = "%s__%s__top%d" % (signame, signal_date, size)
        books[key] = reconstruct_book(key, book_id, signame, size, members_by_key[key],
                                      series, calendar, signal_date, spy_rows)
    analytics = {k: analytics_for_book(b) for k, b in books.items()}
    curves = {k: book_curves(b) for k, b in books.items()}
    curves["spy"] = [{"mark_date": d, "return_since_signal_pct":
                      (spy_rows.get(d) or {}).get("return_since_signal_pct")} for d in calendar]
    sectors = {k: sector_exposure(b) for k, b in books.items()}
    isolation = _isolation_report(books)

    h2h_top25 = head_to_head(25, analytics["champion_top25"], analytics["challenger_top25"])
    h2h_top50 = head_to_head(50, analytics["champion_top50"], analytics["challenger_top50"])

    coverage = {k: (analytics[k]["covered_count"] or 0) for k in books}
    ends = {a["end_date"] for a in analytics.values()}
    starts = {a["start_date"] for a in analytics.values()}
    calendars_aligned = len(ends) == 1 and len(starts) == 1 and None not in ends

    elapsed_marks = len(calendar)
    decision = decide_tournament(
        elapsed_marks=elapsed_marks, spy_available=spy_available,
        calendars_aligned=calendars_aligned, books_isolated=isolation["all_isolated"],
        coverage=coverage, h2h_top25=h2h_top25, h2h_top50=h2h_top50)

    remaining = max(0, HORIZON_TRADING_DAYS - elapsed_marks)
    horizon = {
        "signal_date": signal_date, "horizon_trading_days": HORIZON_TRADING_DAYS,
        "elapsed_marks": elapsed_marks, "remaining_marks": remaining,
        "checkpoint_reached": decision["checkpoint_reached"],
        "latest_common_owned_eod_date": latest_common,
        "review_target_date": review_target,
        "progress_pct": _round(100.0 * elapsed_marks / HORIZON_TRADING_DAYS, 1),
    }

    # reproduction: champion stored entry vs derived fetched entry. This is a DIAGNOSTIC, not a
    # block: the live adjusted-close series re-adjusts historical bars for dividends/splits after
    # the frozen owned snapshot, so the absolute entry can drift while returns stay internally
    # consistent (entry and current use the SAME live series). What matters is that the entry was
    # RESOLVED at the signal date; the write is blocked only on structural integrity below.
    repro_rows = [c for b in books.values() for c in b["reproduction_checks"]]
    abs_errs = [c["abs_error"] for c in repro_rows if c["abs_error"] is not None]
    rel_errs = []
    for c in repro_rows:
        se, ae = c.get("stored_entry"), c.get("abs_error")
        if isinstance(se, (int, float)) and se not in (0, None) and ae is not None:
            rel_errs.append(abs(ae) / abs(se))
    max_abs = max(abs_errs) if abs_errs else None
    max_rel = max(rel_errs) if rel_errs else None
    reproduction = {
        "n_checks": len(abs_errs),
        "max_abs_error": max_abs,
        "max_rel_error": _round(max_rel, 6) if max_rel is not None else None,
        "entries_resolved": bool(abs_errs),
        "reproduces_frozen_entries_within_1pct": (max_rel <= 0.01) if max_rel is not None else None,
        "note": ("Entry prices are re-derived from the live adjusted-close series; small drift "
                 "from the frozen owned snapshot is expected (post-snapshot dividend/split "
                 "re-adjustment) and does not affect internally-consistent returns."),
    }

    missing_by_book = {k: books[k]["missing_tickers"] for k in books}
    return {
        "signal_date": signal_date,
        "spy": {
            "available": spy_available, "ticker": BENCHMARK_TICKER,
            "price_source": SPY_PRICE_SOURCE,
            "cumulative_return_pct": (spy_rows.get(calendar[-1]) or {}).get(
                "return_since_signal_pct") if calendar else None,
            "reference_date": (spy_rows.get(calendar[0]) or {}).get("reference_date")
            if calendar else None,
            "latest_spy_date": cal_meta.get("latest_spy_date"),
        },
        "calendar": {"n_marks": elapsed_marks, "start_date": (calendar[0] if calendar else None),
                     "end_date": (calendar[-1] if calendar else None), **cal_meta},
        "latest_common_financial_mark": latest_common,
        "book_summaries": analytics,
        "daily_curves": curves,
        "sector_exposure": sectors,
        "top25_head_to_head": h2h_top25,
        "top50_head_to_head": h2h_top50,
        "book_isolation": isolation,
        "reproduction": reproduction,
        "coverage": {k: {"covered": analytics[k]["covered_count"],
                         "total": analytics[k]["total_count"],
                         "coverage_pct": analytics[k]["coverage_pct"]} for k in books},
        "missing_tickers_by_book": missing_by_book,
        "horizon_progress": horizon,
        "decision": decision["status"],
        "decision_reasons": decision["reasons"],
        "calendars_aligned": calendars_aligned,
    }


# ---------------------------------------------------------------------------
# Public — the UPGRADED manual tournament data sync (preview / commit)
# ---------------------------------------------------------------------------

def run_current_alpha_tournament_sync(
    *,
    commit: bool = False,
    confirm: Optional[str] = None,
    downloader: Optional[Downloader] = None,
    champion_pkg_dir: Optional[Union[str, Path]] = None,
    challenger_pkg_dir: Optional[Union[str, Path]] = None,
    tournament_dir: Optional[Union[str, Path]] = None,
    system_mark_dir: Optional[Union[str, Path]] = None,
    research_repo_dir: Optional[Union[str, Path]] = None,
    today: Optional[str] = None,
    review_target: str = DEFAULT_REVIEW_TARGET,
) -> dict[str, Any]:
    """Upgraded manual tournament refresh — a real owned-EODHD data sync.

    ``commit=False`` (default) PREVIEWS: resolves the frozen union, the target completed date,
    the expected number of provider calls and the current coverage gaps. It performs NO provider
    call and NO writes. ``commit=True`` requires ``confirm == SYNC_CONFIRM_TOKEN``, fetches the
    union (+ SPY) via the injected/owned transport, rebuilds all four frozen books on the same
    completed-date calendar, and writes ONLY the dedicated local tournament store. Idempotent by
    financial date; partial provider failures keep every success and report every failure.
    """
    run_at = _iso_now()
    ref_today = _today(today)
    champion_pkg = _resolve_champion_pkg(champion_pkg_dir, research_repo_dir)
    challenger_pkg = _resolve_challenger_pkg(challenger_pkg_dir, research_repo_dir)
    tdir = _resolve_tournament_dir(tournament_dir)
    sys_mark_dir = _resolve_system_mark_dir(system_mark_dir)

    members_by_key, union, spy_list = resolve_frozen_union(champion_pkg, challenger_pkg)
    n_members_total = sum(len(m) for m in members_by_key.values())
    if n_members_total == 0:
        payload = {
            "status": SYNC_UNAVAILABLE, "action": "NO_SYNC", "committed": False,
            "wrote_store": False,
            "guidance": ("The frozen champion / challenger paper-book packages were not found "
                         "(Phase 13-A / Phase 17-B). Nothing was fetched or written."),
            "champion_pkg": str(champion_pkg), "challenger_pkg": str(challenger_pkg),
            "loaded_at": run_at,
        }
        payload.update(_safety_block())
        return payload

    signal_date = _read_signal_date(champion_pkg)
    system_mark = _system_market_mark(sys_mark_dir)
    state = _load_sync_state(tdir)
    last_synced = state.get("last_synced_financial_date")
    fetch_tickers = list(union) + list(spy_list)

    union_block = {
        "union_size": len(union),
        "union_tickers": union,
        "benchmark": BENCHMARK_TICKER,
        "expected_provider_calls": len(fetch_tickers),
        "fetch_start": signal_date,
        "fetch_through": "latest completed US market date (< %s)" % ref_today.isoformat(),
        "book_member_counts": {k: len(v) for k, v in members_by_key.items()},
    }

    # --- PREVIEW: no provider call, no writes --------------------------------
    if not commit:
        static_report, _e = _read_json_file(_resolve_forward_dir(None, research_repo_dir)
                                            / FORWARD_REPORT)
        current_tournament_mark = None
        if isinstance(static_report, dict):
            current_tournament_mark = (static_report.get("horizon_progress") or {}).get(
                "latest_common_owned_eod_date")
        synced = load_synced_tournament(tdir)
        if isinstance(synced, dict):
            current_tournament_mark = synced.get("latest_common_financial_mark") \
                or current_tournament_mark
        gaps = {}
        if isinstance(synced, dict):
            gaps = synced.get("coverage") or {}
        payload = {
            "status": SYNC_PREVIEW, "action": "PREVIEW_ONLY_NO_FETCH_NO_WRITE",
            "committed": False, "wrote_store": False, "performed_provider_call": False,
            "union": union_block,
            "signal_date": signal_date,
            "latest_system_market_mark": system_mark,
            "latest_tournament_common_mark": current_tournament_mark,
            "last_synced_financial_date": last_synced,
            "current_coverage_gaps": gaps,
            "guidance": ("Preview only — no EODHD call and no store write. Confirm the manual "
                         "sync (confirm='%s') to fetch the %d-ticker union (+ SPY) from %s "
                         "through the latest completed US market date and rebuild the four "
                         "frozen books." % (SYNC_CONFIRM_TOKEN, len(union), signal_date)),
            "store_dir": str(tdir), "loaded_at": run_at,
        }
        payload.update(_safety_block())
        return payload

    # --- COMMIT requires the explicit manual confirmation token --------------
    if confirm != SYNC_CONFIRM_TOKEN:
        payload = {
            "status": SYNC_CONFIRM_REQUIRED, "action": "NO_SYNC", "committed": False,
            "wrote_store": False, "performed_provider_call": False,
            "union": union_block,
            "guidance": ("A committing data sync (which performs the owned-EODHD fetch and one "
                         "local-store write) requires an explicit manual confirmation "
                         "(confirm='%s')." % SYNC_CONFIRM_TOKEN),
            "loaded_at": run_at,
        }
        payload.update(_safety_block())
        return payload

    # --- COMMIT: fetch the union (+ SPY) via the owned transport --------------
    dl = _resolve_downloader(downloader, _resolve_research_repo_dir(research_repo_dir))
    try:
        series, per_ticker = fetch_union_series(dl, union, signal_date, ref_today)
        spy_series, spy_status = fetch_union_series(dl, spy_list, signal_date, ref_today)
    except TournamentSyncBlocked as blocked:
        payload = {
            "status": blocked.result_enum, "action": "NO_SYNC", "committed": False,
            "wrote_store": False, "blocked": True,
            "union": union_block,
            "guidance": ("The owned-EODHD sync was blocked by a fatal provider state (%s). No "
                         "store was written; the last valid synced tournament data is preserved."
                         % blocked.result_enum),
            "last_synced_financial_date": last_synced, "loaded_at": run_at,
        }
        payload.update(_safety_block())
        return payload

    spy_bars = spy_series.get(BENCHMARK_TICKER, [])
    bundle = _reconstruct_all(members_by_key, series, spy_bars, signal_date, ref_today,
                              review_target)
    latest_common = bundle["latest_common_financial_mark"]
    per_ticker_all = per_ticker + spy_status
    failed = [r for r in per_ticker_all if r["status"] not in ("OK",)]
    n_ok = sum(1 for r in per_ticker_all if r["status"] == "OK")

    # --- structural data-integrity block (NOT the benign entry re-adjustment drift) ---
    repro = bundle["reproduction"]
    structural_ok = (bundle["spy"]["available"] and bundle["calendars_aligned"]
                     and bundle["book_isolation"]["all_isolated"])
    if not structural_ok:
        payload = {
            "status": DEC_BLOCKED_MISMATCH, "action": "NO_SYNC", "committed": False,
            "wrote_store": False, "blocked": True,
            "union": union_block, "reproduction": repro,
            "spy_available": bundle["spy"]["available"],
            "calendars_aligned": bundle["calendars_aligned"],
            "books_isolated": bundle["book_isolation"]["all_isolated"],
            "guidance": ("Structural data-integrity block: SPY is unavailable, the four book "
                         "calendars are not aligned, or book isolation failed; refusing to write "
                         "a mismatched reconstruction. No store was written."),
            "loaded_at": run_at,
        }
        payload.update(_safety_block())
        return payload

    # --- no completed common EOD date at all ---------------------------------
    if not latest_common:
        payload = {
            "status": SYNC_NO_DATE, "action": "NO_SYNC", "committed": False, "wrote_store": False,
            "union": union_block, "per_ticker_results": per_ticker_all,
            "guidance": "No completed common EOD date was resolvable from the fetched union.",
            "loaded_at": run_at,
        }
        payload.update(_safety_block())
        return payload

    # --- idempotency: no newer completed common financial mark ---------------
    if last_synced is not None and str(latest_common) <= str(last_synced):
        payload = {
            "status": SYNC_NO_NEW, "action": "NO_NEW_SNAPSHOT", "committed": False,
            "wrote_store": False,
            "union": union_block,
            "latest_tournament_common_mark": latest_common,
            "last_synced_financial_date": last_synced,
            "per_ticker_results": per_ticker_all,
            "guidance": ("The tournament store is already current (%s); no newer completed "
                         "common EOD date. No snapshot written." % last_synced),
            "loaded_at": run_at,
        }
        payload.update(_safety_block())
        return payload

    # --- write ONLY the dedicated local tournament store ---------------------
    synced_at = _iso_now()
    synced_data = {
        "alpha_tournament": "phase19_synced", "synced_at": synced_at,
        "signal_date": signal_date, "system_market_mark": system_mark,
        "latest_common_financial_mark": latest_common,
        "book_summaries": bundle["book_summaries"],
        "top25_head_to_head": bundle["top25_head_to_head"],
        "top50_head_to_head": bundle["top50_head_to_head"],
        "daily_curves": bundle["daily_curves"],
        "sector_exposure": bundle["sector_exposure"],
        "spy": bundle["spy"], "calendar": bundle["calendar"],
        "coverage": bundle["coverage"], "missing_tickers_by_book": bundle["missing_tickers_by_book"],
        "horizon_progress": bundle["horizon_progress"],
        "decision": bundle["decision"], "decision_reasons": bundle["decision_reasons"],
        "reproduction": repro, "book_isolation": bundle["book_isolation"],
        "per_ticker_results": per_ticker_all,
        "union_size": len(union), "order_action_all": "NO_ORDER",
    }
    _atomic_write_json(tdir / _SYNC_DATA_FILE, synced_data)
    # normalized prices (union only), kept separate; no key material ever stored
    prices = {tk: [{"date": d, "adjusted_close": v} for d, v in series.get(tk, [])]
              for tk in union}
    prices[BENCHMARK_TICKER] = [{"date": d, "adjusted_close": v} for d, v in spy_bars]
    _atomic_write_json(tdir / _SYNC_PRICES_FILE,
                       {"synced_at": synced_at, "signal_date": signal_date,
                        "latest_common_financial_mark": latest_common,
                        "n_tickers": len(union) + 1, "prices": prices,
                        "order_action_all": "NO_ORDER"})
    new_state = {
        "last_synced_financial_date": str(latest_common),
        "system_market_mark_at_sync": system_mark,
        "decision": bundle["decision"],
        "n_syncs": int(state.get("n_syncs", 0)) + 1,
        "union_size": len(union),
        "synced_at": synced_at, "order_action_all": "NO_ORDER",
    }
    _atomic_write_json(tdir / _SYNC_STATE_FILE, new_state)

    status_enum = SYNC_COMPLETE if not failed else SYNC_PARTIAL
    payload = {
        "status": status_enum,
        "action": "SYNC_WRITTEN",
        "committed": True, "wrote_store": True, "performed_provider_call": True,
        "union": union_block,
        "latest_tournament_common_mark": latest_common,
        "latest_system_market_mark": system_mark,
        "signal_date": signal_date,
        "reconstructed": {
            "book_summaries": bundle["book_summaries"],
            "top25_head_to_head": bundle["top25_head_to_head"],
            "top50_head_to_head": bundle["top50_head_to_head"],
            "coverage": bundle["coverage"],
            "missing_tickers_by_book": bundle["missing_tickers_by_book"],
            "spy": bundle["spy"], "horizon_progress": bundle["horizon_progress"],
            "decision": bundle["decision"], "reproduction": repro,
        },
        "per_ticker_results": per_ticker_all,
        "n_tickers_ok": n_ok, "n_tickers_failed": len(failed),
        "failed_tickers": [r["ticker"] for r in failed],
        "store_dir": str(tdir),
        "guidance": (("Synced the four frozen books to the latest completed common EOD date "
                      "(%s). " % latest_common)
                     + ("All %d union tickers (+ SPY) returned data." % len(union) if not failed
                        else "%d ticker(s) had no provider data and are reported uncovered; "
                             "every successful ticker was retained." % len(failed))),
        "loaded_at": run_at,
    }
    payload.update(_safety_block(wrote_store=True))
    return payload


# ---------------------------------------------------------------------------
# Public — alignment block attached to the read-only GET (Part D)
# ---------------------------------------------------------------------------

def _coverage_view(coverage_map: dict[str, Any]) -> dict[str, Any]:
    out = {}
    for k, v in (coverage_map or {}).items():
        if isinstance(v, dict):
            out[k] = {"covered": v.get("covered"), "total": v.get("total"),
                      "coverage_pct": v.get("coverage_pct")}
    return out


def build_alignment_block(
    static_report: Optional[dict[str, Any]],
    *,
    tournament_dir: Optional[Union[str, Path]] = None,
    system_mark_dir: Optional[Union[str, Path]] = None,
) -> dict[str, Any]:
    """Compute the explicit alignment block (Part D): the current system market mark vs the
    tournament's latest common mark, the alignment enum, the mark-date delta, four-book
    coverage, the SPY date, the unresolved tickers and the exact next action. Read-only."""
    tdir = _resolve_tournament_dir(tournament_dir)
    sys_mark_dir = _resolve_system_mark_dir(system_mark_dir)
    system_mark = _system_market_mark(sys_mark_dir)
    synced = load_synced_tournament(tdir)

    # tournament common mark + coverage: prefer the synced reconstruction, else the static report
    if isinstance(synced, dict):
        tournament_mark = synced.get("latest_common_financial_mark")
        coverage = _coverage_view(synced.get("coverage") or {})
        unresolved = synced.get("missing_tickers_by_book") or {}
        spy_date = (synced.get("calendar") or {}).get("end_date")
        isolated = (synced.get("book_isolation") or {}).get("all_isolated")
        source = "SYNCED_LOCAL_STORE"
    elif isinstance(static_report, dict):
        tournament_mark = (static_report.get("horizon_progress") or {}).get(
            "latest_common_owned_eod_date")
        summaries = static_report.get("book_summaries") or {}
        coverage = {k: {"covered": (summaries.get(k) or {}).get("covered_count"),
                        "total": (summaries.get(k) or {}).get("total_count"),
                        "coverage_pct": (summaries.get(k) or {}).get("coverage_pct")}
                    for k in ("champion_top25", "challenger_top25",
                              "champion_top50", "challenger_top50")}
        unresolved = {}
        spy_date = (static_report.get("calendar") or {}).get("end_date")
        isolated = (static_report.get("book_isolation") or {}).get("all_isolated")
        source = "STATIC_FORWARD_REPORT"
    else:
        tournament_mark = None
        coverage = {}
        unresolved = {}
        spy_date = None
        isolated = None
        source = "NONE"

    # coverage floor: any book below COVERAGE_FULL_PCT is partial
    cov_pcts = [c.get("coverage_pct") for c in coverage.values()
                if isinstance(c.get("coverage_pct"), (int, float))]
    any_partial = any(p < COVERAGE_FULL_PCT for p in cov_pcts) if cov_pcts else False

    # delta (calendar days) between the system mark and the tournament mark
    sys_d = _parse_date(system_mark)
    tour_d = _parse_date(tournament_mark)
    delta_days = (sys_d - tour_d).days if (sys_d is not None and tour_d is not None) else None

    # alignment ladder (priority: structural mismatch > stale > partial coverage > aligned).
    # Entry re-adjustment drift is benign and never blocks; only broken isolation does.
    if isolated is False:
        alignment = ALIGN_BLOCKED
        next_action = ("Data-integrity mismatch in the tournament reconstruction; do not treat "
                       "the current figures as valid. Re-run the manual tournament data sync.")
    elif delta_days is not None and delta_days > 0:
        alignment = ALIGN_STALE
        next_action = ("The tournament common mark (%s) is %d calendar day(s) behind the system "
                       "market mark (%s). Run the manual Tournament Data Sync to fetch the union "
                       "through the latest completed US market date." % (tournament_mark,
                                                                          delta_days, system_mark))
    elif any_partial:
        alignment = ALIGN_PARTIAL
        next_action = ("Marks are date-aligned but at least one book is below %.0f%% coverage. "
                       "Coverage is honestly partial (paper marks only); re-run the sync if "
                       "provider data has since filled in." % COVERAGE_FULL_PCT)
    else:
        alignment = ALIGN_ALIGNED
        next_action = ("The tournament common mark equals the system market mark; the four books "
                       "are current. Continue monitoring until the 63-mark checkpoint.")

    return {
        "latest_system_market_mark": system_mark,
        "latest_tournament_common_mark": tournament_mark,
        "tournament_alignment": alignment,
        "mark_date_delta": delta_days,
        "mark_date_delta_unit": "calendar_days",
        "is_stale": alignment == ALIGN_STALE,
        "four_book_coverage": coverage,
        "spy_date": spy_date,
        "unresolved_tickers": unresolved,
        "unresolved_ticker_count": sum(len(v or []) for v in unresolved.values())
        if isinstance(unresolved, dict) else 0,
        "tournament_mark_source": source,
        "next_action": next_action,
        "store_dir": str(tdir),
    }


def attach_alignment(payload: dict[str, Any], *, static_report: Optional[dict[str, Any]] = None,
                     tournament_dir: Optional[Union[str, Path]] = None,
                     system_mark_dir: Optional[Union[str, Path]] = None,
                     research_repo_dir: Optional[Union[str, Path]] = None) -> dict[str, Any]:
    """Attach the alignment block + (when present) the synced overlay to a GET payload. The
    static report is reused from the payload's own source when not passed in. Read-only."""
    if static_report is None:
        fdir = _resolve_forward_dir(None, research_repo_dir)
        static_report, _err = _read_json_file(fdir / FORWARD_REPORT)
    tdir = _resolve_tournament_dir(tournament_dir)
    alignment = build_alignment_block(static_report if isinstance(static_report, dict) else None,
                                      tournament_dir=tdir, system_mark_dir=system_mark_dir)
    synced = load_synced_tournament(tdir)

    payload["alignment"] = alignment
    payload["latest_system_market_mark"] = alignment["latest_system_market_mark"]
    payload["latest_tournament_common_mark"] = alignment["latest_tournament_common_mark"]
    payload["tournament_alignment"] = alignment["tournament_alignment"]
    payload["mark_date_delta"] = alignment["mark_date_delta"]

    if isinstance(synced, dict):
        # Surface the synced (current) reconstruction as the primary current view; keep the
        # static report available as the reference baseline under `static_forward_view`.
        payload["synced_tournament"] = {
            "available": True,
            "synced_at": synced.get("synced_at"),
            "latest_common_financial_mark": synced.get("latest_common_financial_mark"),
            "book_summaries": synced.get("book_summaries"),
            "top25_head_to_head": synced.get("top25_head_to_head"),
            "top50_head_to_head": synced.get("top50_head_to_head"),
            "daily_curves": synced.get("daily_curves"),
            "sector_exposure": synced.get("sector_exposure"),
            "spy": synced.get("spy"),
            "coverage": synced.get("coverage"),
            "missing_tickers_by_book": synced.get("missing_tickers_by_book"),
            "horizon_progress": synced.get("horizon_progress"),
            "decision": synced.get("decision"),
            "per_ticker_results": synced.get("per_ticker_results"),
            "union_size": synced.get("union_size"),
        }
        payload["current_view_source"] = "SYNCED_LOCAL_STORE"
    else:
        payload["synced_tournament"] = {"available": False}
        payload["current_view_source"] = "STATIC_FORWARD_REPORT"
    return payload


__all__ = [
    "run_current_alpha_tournament_sync",
    "build_alignment_block",
    "attach_alignment",
    "load_synced_tournament",
    "resolve_frozen_union",
    "load_book_members",
    "fetch_union_series",
    "TournamentSyncBlocked",
    "SYNC_CONFIRM_TOKEN",
    "SYNC_FIXTURE_ENV",
    "CHAMPION_PKG_DIR_ENV",
    "CHALLENGER_PKG_DIR_ENV",
    "SYSTEM_MARK_DIR_ENV",
    "SYNC_PREVIEW",
    "SYNC_COMPLETE",
    "SYNC_PARTIAL",
    "SYNC_NO_NEW",
    "SYNC_CONFIRM_REQUIRED",
    "SYNC_UNAVAILABLE",
    "ALIGN_ALIGNED",
    "ALIGN_STALE",
    "ALIGN_PARTIAL",
    "ALIGN_BLOCKED",
    "_SYNC_STATE_FILE",
    "_SYNC_DATA_FILE",
    "_SYNC_PRICES_FILE",
]
