r"""campaign_r56_v2.data_eq_ext - the small/mid-cap point-in-time equity EXTENSION panel (DF2).

Dataset id : ``eq_ext_smallmid_pit_panel_v1``  (PAPER_TRADER_MULTI_AGENT_ALPHA_CAMPAIGN_R56_V2)
Owner      : data-foundation-agent.  RESEARCH ONLY - no order, no fill, no broker, no promotion.

It MIRRORS ``alpha_agent.r57.panel`` (the builder of ``sp500_pit_panel_v1``) array for array, from the
same licensed, locally installed Norgate database, and adds what the campaign needs:

    npz key     shape                 meaning
    ---------   -------------------   ----------------------------------------------------------------
    tr          (n_symbols, n_dates)  TOTAL-RETURN adjusted CLOSE (float32)            [as r57]
    un          (n_symbols, n_dates)  UNADJUSTED close (price floor, dollar volume)    [as r57]
    vol         (n_symbols, n_dates)  UNADJUSTED share volume                          [as r57]
    mem         (n_symbols, n_dates)  PIT Russell 2000 membership 0/1 (uint8)          [as r57, new index]
    spy_tr      (n_dates,)            SPY total-return close, the session calendar     [as r57]
    op_tr       (n_symbols, n_dates)  TOTAL-RETURN adjusted OPEN (float32)             [NEW - P8 execution]
    sp500_mem   (n_symbols, n_dates)  PIT S&P 500 membership 0/1 (uint8)               [NEW - U5 exclusion]
    dv          (n_symbols, n_dates)  Norgate ``Turnover`` = traded dollar value       [NEW - side-car only]

``dates``, ``symbols``, ``sectors`` and the per-symbol identity columns live in the meta json, exactly as
for ``sp500_pit_panel_v1``. The frozen campaign features (``alpha_agent.r57.families.amihud``,
``alpha_agent.r57.engine.eligibility``) use ``un * vol``; ``dv`` is a cross-check, never a tuning choice.

Index-set choice is MECHANICAL (director, foundation_requests.json DF2b): Russell 2000 first.

Traps this module handles and downstream agents must respect
-------------------------------------------------------------
1. DELISTED FLAG / FORWARD FILL. ``index_constituent_timeseries`` ends on the security's last quoted
   session and its last flag can stay 1. Every series is read with ``PaddingType.NONE`` and written ONLY
   onto the sessions it actually has a row for; nothing is reindexed or forward-filled, so a dead name is
   0 after its last quote. NEVER ``ffill`` ``mem`` / ``sp500_mem``.
2. SYMBOL SPELLING. A delisted security is spelled ``<LAST TICKER>-YYYYMM``; a live security is spelled by
   its CURRENT ticker. A ticker is therefore NOT a point-in-time identifier and base tickers are recycled
   (``base_ticker`` repeats). Join anything external on identity resolved AS OF DATE, never on the raw
   ticker. The only stable vendor identifier the local API offers is ``assetid`` (no CIK / CUSIP / FIGI).
3. ``tr`` / ``op_tr`` LEVELS are back-adjusted with today's dividend history: use RATIOS only (returns).
   ``un`` is the point-in-time price level.
4. ``sectors`` is the CURRENT GICS sector (not point-in-time) - the same declared limitation as r57.
5. Membership only gates the universe. Symbols with no Russell 2000 member-session inside the panel window
   are not carried (they can never be eligible); every symbol with at least one is carried for the WHOLE
   window, dead or alive.

Usage (PowerShell):
    & C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe `
        C:\Users\binis\paper_trader\research\agents\campaign_r56_v2\data_eq_ext.py build
    ... data_eq_ext.py verify      # hash + coverage recomputed from the file on disk

Import (``alpha_agent`` is NOT installed into .venv-win: a script outside the repository root must put the
repository root on sys.path itself before it imports ``alpha_agent.r57.engine``):
    import sys
    sys.path.insert(0, r"C:\Users\binis\paper_trader")
    sys.path.insert(0, r"C:\Users\binis\paper_trader\research\agents\campaign_r56_v2")
    from data_eq_ext import load_eq_ext_panel, as_r57_panel
    panel = load_eq_ext_panel()                 # ~4 s, verifies the npz sha256 against the meta
    view = as_r57_panel(panel)                  # mem AND NOT sp500_mem; consumed UNCHANGED by r57.engine
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

PANEL_NAME = "eq_ext_smallmid_pit_panel_v1"
DATA_DIR_ENV = "PAPER_TRADER_R56V2_FOUNDATION_DATA"
DEFAULT_DATA_DIR = Path(r"D:\Stock_Prediction_app_data\r59_autonomous_alpha\agents_v2"
                        r"\campaign_R56_V2\foundation\data")

# Frozen build parameters (director: foundation_requests.json DF2c "Dates 2009-01-01 -> latest").
PANEL_START = "2009-01-01"
PANEL_END = "2026-09-18"            # last completed session in the local database at build time
WATCHLIST = "Russell 2000 Current & Past"
INDEX_NAME = "Russell 2000"
SP500_INDEX_NAME = "S&P 500"
CALENDAR_SYMBOL = "SPY"
DELISTED_DATABASE = "US Equities Delisted"

FLOAT_KEYS = ("tr", "un", "vol", "op_tr", "dv")
MASK_KEYS = ("mem", "sp500_mem")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_hash(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 22), b""):
            h.update(chunk)
    return h.hexdigest()


def data_dir() -> Path:
    return Path(os.environ.get(DATA_DIR_ENV) or DEFAULT_DATA_DIR)


def _ng():
    logging.disable(logging.WARNING)
    import norgatedata
    return norgatedata


def is_delisted_style(symbol: str) -> bool:
    """``<TICKER>-YYYYMM`` - the vendor spelling of a security that has left the database's live set."""
    if "-" not in symbol:
        return False
    tail = symbol.rsplit("-", 1)[-1]
    return len(tail) == 6 and tail.isdigit()


def base_ticker(symbol: str) -> str:
    return symbol.rsplit("-", 1)[0] if is_delisted_style(symbol) else symbol


def _rows(ng, fn, *args, **kw):
    """One vendor time series as a numpy recarray, PaddingType.NONE, inside the panel window."""
    return fn(*args, padding_setting=ng.PaddingType.NONE, start_date=PANEL_START, end_date=PANEL_END,
              timeseriesformat="numpy-recarray", **kw)


def _place(cal: np.ndarray, d: np.ndarray):
    """Column index of each row date on the session calendar; rows off the calendar are dropped."""
    d = np.asarray(d).astype("datetime64[D]")
    if len(d) == 0:
        return np.zeros(0, dtype=np.int64), np.zeros(0, dtype=bool)
    ii = np.clip(np.searchsorted(cal, d), 0, len(cal) - 1)
    return ii, cal[ii] == d


def _meta_str(fn, sym):
    try:
        v = fn(sym)
    except Exception:                                          # noqa: BLE001
        return None
    if v is None:
        return None
    return v.isoformat() if hasattr(v, "isoformat") else str(v)


# --------------------------------------------------------------------------- #
# Build
# --------------------------------------------------------------------------- #
def build_panel(out_dir: Path | None = None, progress_every: int = 1000) -> dict:
    """Build and cache the panel. Idempotent and immutable: an existing cache is returned, never rewritten."""
    out = Path(out_dir) if out_dir else data_dir()
    out.mkdir(parents=True, exist_ok=True)
    npz_path = out / (PANEL_NAME + ".npz")
    meta_path = out / (PANEL_NAME + ".meta.json")
    if npz_path.exists() and meta_path.exists():
        return json.loads(meta_path.read_text(encoding="utf-8"))

    ng = _ng()
    if not ng.status():
        raise RuntimeError("Norgate Data Updater is not running; this builder never starts it (DATA_HOLD)")
    TR, NONE = ng.StockPriceAdjustmentType.TOTALRETURN, ng.StockPriceAdjustmentType.NONE

    spy = _rows(ng, ng.price_timeseries, CALENDAR_SYMBOL, stock_price_adjustment_setting=TR)
    cal = spy["Date"].astype("datetime64[D]")
    n_dates = len(cal)

    watch = sorted(set(str(s) for s in ng.watchlist_symbols(WATCHLIST)))
    non_security = [s for s in watch if s.startswith("$")]          # index symbols listed by the vendor
    candidates = [s for s in watch if not s.startswith("$")]
    delisted_db = set(str(s) for s in ng.database_symbols(DELISTED_DATABASE))

    # ---- pass 1: PIT membership of EVERY watchlist security (no price read) ------------------------ #
    flags, failed_membership, off_calendar_rows = {}, [], 0
    died_as_member = 0                      # the forward-fill trap, measured: last row == 1 and the name is dead
    for k, sym in enumerate(candidates):
        if progress_every and k % progress_every == 0:
            print("pass1 membership %d/%d %s" % (k, len(candidates), sym), flush=True)
        try:
            a = _rows(ng, ng.index_constituent_timeseries, sym, INDEX_NAME)
        except Exception as exc:                               # noqa: BLE001
            failed_membership.append({"symbol": sym, "error": "%s: %s" % (type(exc).__name__, exc)})
            continue
        if a is None or len(a) == 0:
            continue
        ii, ok = _place(cal, a["Date"])
        off_calendar_rows += int((~ok).sum())
        f = np.asarray(a["Index Constituent"]).astype(np.uint8)
        if not (f[ok] == 1).any():
            continue
        flags[sym] = (ii[ok], f[ok])
        if int(f[-1]) == 1 and np.asarray(a["Date"]).astype("datetime64[D]")[-1] < cal[-1]:
            died_as_member += 1

    symbols = sorted(flags)
    n = len(symbols)
    tr = np.full((n, n_dates), np.nan, dtype=np.float32)
    op = np.full((n, n_dates), np.nan, dtype=np.float32)
    un = np.full((n, n_dates), np.nan, dtype=np.float32)
    vol = np.full((n, n_dates), np.nan, dtype=np.float32)
    dv = np.full((n, n_dates), np.nan, dtype=np.float32)
    mem = np.zeros((n, n_dates), dtype=np.uint8)
    spm = np.zeros((n, n_dates), dtype=np.uint8)
    sectors, identity, skipped = [], [], []
    nonpositive_open = 0

    # ---- pass 2: prices, S&P 500 membership and identity of the carried securities ----------------- #
    for k, sym in enumerate(symbols):
        if progress_every and k % progress_every == 0:
            print("pass2 prices %d/%d %s" % (k, n, sym), flush=True)
        ii, f = flags[sym]
        mem[k, ii] = f
        ident = {"symbol": sym, "base_ticker": base_ticker(sym), "delisted_style": is_delisted_style(sym),
                 "in_delisted_database": sym in delisted_db,
                 "assetid": _meta_str(ng.assetid, sym), "security_name": _meta_str(ng.security_name, sym),
                 "exchange": _meta_str(ng.exchange_name, sym), "subtype1": _meta_str(ng.subtype1, sym),
                 "subtype2": _meta_str(ng.subtype2, sym), "domicile": _meta_str(ng.domicile, sym),
                 "first_quoted_date": _meta_str(ng.first_quoted_date, sym),
                 "last_quoted_date": _meta_str(ng.last_quoted_date, sym)}
        identity.append(ident)
        try:
            sectors.append(ng.classification_at_level(sym, "GICS", "Name", level=1))
        except Exception:                                      # noqa: BLE001
            sectors.append(None)
        try:
            p = _rows(ng, ng.price_timeseries, sym, stock_price_adjustment_setting=TR)
        except Exception as exc:                               # noqa: BLE001
            skipped.append({"symbol": sym, "error": "%s: %s" % (type(exc).__name__, exc)})
            continue
        if p is None or len(p) == 0:
            skipped.append({"symbol": sym, "error": "no price rows in window"})
            continue
        jj, ok = _place(cal, p["Date"])
        tr[k, jj[ok]] = np.asarray(p["Close"], dtype=np.float32)[ok]
        o = np.asarray(p["Open"], dtype=np.float32)[ok].copy()
        bad = ~(np.isfinite(o) & (o > 0))
        nonpositive_open += int(bad.sum())
        o[bad] = np.nan                                         # an absent open is absent, never a zero fill
        op[k, jj[ok]] = o
        try:
            q = _rows(ng, ng.price_timeseries, sym, stock_price_adjustment_setting=NONE)
            ju, oku = _place(cal, q["Date"])
            un[k, ju[oku]] = np.asarray(q["Close"], dtype=np.float32)[oku]
            vol[k, ju[oku]] = np.asarray(q["Volume"], dtype=np.float32)[oku]
            dv[k, ju[oku]] = np.asarray(q["Turnover"], dtype=np.float32)[oku]
        except Exception as exc:                               # noqa: BLE001
            skipped.append({"symbol": sym, "error": "unadjusted: %s: %s" % (type(exc).__name__, exc)})
        try:
            m = _rows(ng, ng.index_constituent_timeseries, sym, SP500_INDEX_NAME)
            if m is not None and len(m):
                jm, okm = _place(cal, m["Date"])
                spm[k, jm[okm]] = np.asarray(m["Index Constituent"]).astype(np.uint8)[okm]
        except Exception:                                      # noqa: BLE001
            pass

    spy_tr = np.asarray(spy["Close"], dtype=np.float64)
    arrays = {"tr": tr, "un": un, "vol": vol, "mem": mem, "spy_tr": spy_tr,
              "op_tr": op, "sp500_mem": spm, "dv": dv}
    tmp = out / (PANEL_NAME + ".building.npz")
    np.savez_compressed(tmp, **arrays)
    tmp.replace(npz_path)

    dates = [str(d) for d in cal]
    meta = {
        "panel": PANEL_NAME, "built_at": now_iso(), "builder": "research/agents/campaign_r56_v2/data_eq_ext.py",
        "source": "Norgate Data (local database via norgatedata %s); databases US Equities + %s"
                  % (getattr(ng, "__version__", "unknown"), DELISTED_DATABASE),
        "source_last_update": {db: str(ng.last_database_update_time(db))
                               for db in ("US Equities", DELISTED_DATABASE)},
        "watchlist": WATCHLIST, "index": INDEX_NAME, "sp500_index": SP500_INDEX_NAME,
        "calendar_symbol": CALENDAR_SYMBOL, "panel_start": PANEL_START, "panel_end": PANEL_END,
        "n_symbols": n, "n_dates": n_dates, "date_start": dates[0], "date_end": dates[-1],
        "symbols": symbols, "sectors": sectors, "dates": dates, "identity": identity,
        "npz_keys": sorted(arrays), "skipped_symbols": skipped,
        "watchlist_size": len(watch), "watchlist_non_security_symbols": non_security,
        "watchlist_securities_without_member_session_in_window": len(candidates) - n,
        "failed_membership_reads": failed_membership, "off_calendar_membership_rows": off_calendar_rows,
        "nonpositive_or_missing_open_rows_set_nan": nonpositive_open,
        "ffill_trap_names_dead_with_last_flag_1": died_as_member,
        "n_member_days": int(mem.sum()),
        "price_basis": "TOTALRETURN for returns (close tr, open op_tr); UNADJUSTED for floors/volume (un, vol, dv)",
        "pit_membership": "norgatedata.index_constituent_timeseries per security, PaddingType.NONE, "
                          "written only on sessions with a vendor row - never forward-filled",
        "array_sha256": {k: hashlib.sha256(np.ascontiguousarray(v).tobytes()).hexdigest()
                         for k, v in arrays.items()},
        "npz_sha256": file_sha256(npz_path), "npz_bytes": npz_path.stat().st_size,
        "manifest_hash": stable_hash({"symbols": symbols, "d0": dates[0], "d1": dates[-1],
                                      "member_days": int(mem.sum())}),
    }
    tmpm = meta_path.with_suffix(".json.tmp")
    tmpm.write_text(json.dumps(meta, indent=1), encoding="utf-8")
    tmpm.replace(meta_path)
    return meta


# --------------------------------------------------------------------------- #
# Load  (pure read - the keys of alpha_agent.r59.engines.load_equity_panel, plus the new ones)
# --------------------------------------------------------------------------- #
_CACHE: dict = {}


def load_eq_ext_panel(directory: Path | None = None, verify_hash: bool = True, dtype=np.float64) -> dict:
    """The extension panel. ``verify_hash`` recomputes the npz sha256 against the meta and refuses a mismatch."""
    d = Path(directory) if directory else data_dir()
    key = (str(d), np.dtype(dtype).name)
    if key in _CACHE:
        return _CACHE[key]
    npz_path, meta_path = d / (PANEL_NAME + ".npz"), d / (PANEL_NAME + ".meta.json")
    if not npz_path.exists() or not meta_path.exists():
        raise FileNotFoundError("extension panel not present: %s" % npz_path)
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    if verify_hash:
        got = file_sha256(npz_path)
        if got != meta["npz_sha256"]:
            raise ValueError("extension panel hash mismatch: meta %s file %s" % (meta["npz_sha256"], got))
    z = np.load(npz_path)
    ident = meta["identity"]
    panel = {
        "meta": meta,
        "dates": np.asarray(meta["dates"]), "symbols": np.asarray(meta["symbols"]),
        "sectors": np.array([s or "Unknown" for s in meta["sectors"]]),
        "tr": z["tr"].astype(dtype), "un": z["un"].astype(dtype), "vol": z["vol"].astype(dtype),
        "mem": z["mem"], "spy_tr": z["spy_tr"],
        "op_tr": z["op_tr"].astype(dtype), "sp500_mem": z["sp500_mem"], "dv": z["dv"].astype(dtype),
        "assetid": np.array([i["assetid"] or "" for i in ident]),
        "base_ticker": np.array([i["base_ticker"] for i in ident]),
        "security_name": np.array([i["security_name"] or "" for i in ident]),
        "delisted": np.array([i["last_quoted_date"] is not None for i in ident]),
        "first_quoted_date": np.array([i["first_quoted_date"] or "" for i in ident]),
        "last_quoted_date": np.array([i["last_quoted_date"] or "" for i in ident]),
    }
    _CACHE[key] = panel
    return panel


def as_r57_panel(panel: dict, exclude_sp500: bool = True) -> dict:
    """A shallow view that ``alpha_agent.r57.engine`` / ``families`` consume UNCHANGED.

    With ``exclude_sp500`` the membership mask is ``mem AND NOT sp500_mem`` (director rule U5). No price,
    volume or return array is touched."""
    view = {k: panel[k] for k in ("dates", "symbols", "sectors", "tr", "un", "vol", "spy_tr")}
    m = panel["mem"] > 0
    if exclude_sp500:
        m = m & ~(panel["sp500_mem"] > 0)
    view["mem"] = m.astype(np.uint8)
    return view


# --------------------------------------------------------------------------- #
# Coverage  (counts, dates and shares ONLY - no return is ever related to anything)
# --------------------------------------------------------------------------- #
def coverage_report(panel: dict) -> dict:
    dates, mem, spm = panel["dates"], panel["mem"] > 0, panel["sp500_mem"] > 0
    tr, op, un, vol, dv = panel["tr"], panel["op_tr"], panel["un"], panel["vol"], panel["dv"]
    per_day = mem.sum(axis=0)
    years = np.array([d[:4] for d in dates])
    by_year = {}
    for y in sorted(set(years)):
        c = per_day[years == y]
        ever = mem[:, years == y].any(axis=1)
        by_year[y] = {"sessions": int(len(c)), "members_min": int(c.min()), "members_median": float(np.median(c)),
                      "members_max": int(c.max()), "distinct_members": int(ever.sum()),
                      "distinct_members_now_delisted": int((ever & panel["delisted"]).sum())}
    md = int(mem.sum())
    fin_tr = np.isfinite(tr) & (tr > 0)
    fin_op = np.isfinite(op) & (op > 0)
    uv = un * vol
    both = mem & np.isfinite(uv) & (uv > 0) & np.isfinite(dv) & (dv > 0)
    ratio = (dv[both] / uv[both]) if both.any() else np.array([np.nan])
    base = panel["base_ticker"]
    uniq, cnt = np.unique(base, return_counts=True)
    recycled = uniq[cnt > 1]
    overlap = 0
    for b in recycled:                                          # two securities, one ticker, same session
        rows = np.flatnonzero(base == b)
        if (mem[rows].sum(axis=0) > 1).any():
            overlap += 1
    # The forward-fill trap, measured: dead names that were still flagged 1 on their LAST priced session.
    # A naive reindex/ffill of the vendor flag would count every one of them as a member for ever.
    trap_last = 0
    for k in np.flatnonzero(panel["delisted"]):
        priced = np.flatnonzero(np.isfinite(tr[k]))
        if len(priced) and mem[k, priced[-1]]:
            trap_last += 1
    low = [str(dates[j]) for j in np.flatnonzero(per_day < 1800)]
    thin = [str(dates[j]) for j in range(len(dates))
            if per_day[j] and (mem[:, j] & fin_tr[:, j]).sum() < 0.98 * per_day[j]]
    return {
        "date_start": str(dates[0]), "date_end": str(dates[-1]), "n_dates": int(len(dates)),
        "n_symbols": int(mem.shape[0]), "n_symbols_delisted_last_quoted_date": int(panel["delisted"].sum()),
        "n_symbols_delisted_style_suffix": int(sum(is_delisted_style(s) for s in panel["symbols"])),
        "n_symbols_in_delisted_database": int(sum(i["in_delisted_database"] for i in panel["meta"]["identity"])),
        "n_symbols_without_any_price": int((~np.isfinite(tr).any(axis=1)).sum()),
        "members_first_date": int(per_day[0]), "members_last_date": int(per_day[-1]),
        "members_per_date_min": int(per_day.min()), "members_per_date_median": float(np.median(per_day)),
        "members_per_date_max": int(per_day.max()), "members_by_year": by_year,
        "member_days": md,
        "share_member_days_finite_tr": float((mem & fin_tr).sum() / md),
        "share_member_days_finite_op_tr": float((mem & fin_op).sum() / md),
        "share_member_days_finite_un": float((mem & np.isfinite(un)).sum() / md),
        "share_member_days_positive_volume": float((mem & np.isfinite(vol) & (vol > 0)).sum() / md),
        "share_member_days_positive_dv": float((mem & np.isfinite(dv) & (dv > 0)).sum() / md),
        "member_days_also_sp500": int((mem & spm).sum()),
        "share_member_days_also_sp500": float((mem & spm).sum() / md),
        "symbols_ever_sp500_in_window": int(spm.any(axis=1).sum()),
        "dv_over_un_x_vol_median": float(np.nanmedian(ratio)),
        "dv_over_un_x_vol_p05_p95": [float(np.nanpercentile(ratio, 5)), float(np.nanpercentile(ratio, 95))],
        "recycled_base_tickers": int(len(recycled)), "recycled_base_tickers_overlapping_membership": int(overlap),
        "recycled_examples": [str(b) for b in recycled[:12]],
        "delisted_names_member_on_their_last_priced_session": trap_last,
        "sessions_with_fewer_than_1800_members": low[:50], "n_sessions_with_fewer_than_1800_members": len(low),
        "sessions_where_under_98pct_of_members_have_tr": thin[:50], "n_sessions_under_98pct": len(thin),
    }


def main(argv) -> int:
    cmd = argv[1] if len(argv) > 1 else "verify"
    if cmd == "build":
        meta = build_panel()
        print(json.dumps({k: meta[k] for k in ("panel", "built_at", "n_symbols", "n_dates", "date_start",
                                                "date_end", "n_member_days", "npz_sha256", "npz_bytes",
                                                "manifest_hash")}, indent=1))
        return 0
    panel = load_eq_ext_panel(verify_hash=True)
    print(json.dumps(coverage_report(panel), indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
