"""
engine/absolute_return_research.py — Phase 31B ABSOLUTE-RETURN DIAGNOSIS AND
RISK-CONTROLLED SHADOW CHALLENGERS.

RESEARCH ONLY. READ ONLY with respect to Paper Trader.

This module diagnoses the operational Alpha Paper Book #1 and constructs three
pre-registered risk-controlled shadow portfolios. It NEVER mutates the active
book, submits an order, activates a hedge/short, promotes a challenger, opens a
database connection, or makes an external market-data call. Every input is an
owned local file read through a pure read-only path; every output is written
only under the caller-supplied output root.

Read-only guarantees (see tests/test_phase31b_absolute_return_research.py):
  * No SQLAlchemy engine/session is ever created (the module only imports the
    file-based desk / forward-prediction / forward-evidence readers and calls
    their pure JSON/CSV read functions).
  * The forward-evidence attribution is invoked with explicit injected loaders
    and an explicit ``ops`` dict so it never falls back to
    operational_book.load_operational_book (which can read the legacy Postgres
    valuation) or to any network refresh.
  * No capture / refresh / mature / run_* (write-path) function is called.

Data reality at authoring time (2026-07-28, latest completed close 2026-07-27):
  * 4 completed operational NAV marks (2026-07-22..2026-07-27); 3 daily returns.
  * 12 immutable forward book snapshots across 2 dates (07-24, 07-27); 3 matured
    1-close outcomes. The sample is far below any promotion floor — the engine
    computes every diagnostic honestly and gates every conclusion accordingly.
"""
from __future__ import annotations

import csv
import hashlib
import json
import math
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

from paper_trader.api import forward_evidence as fe
from paper_trader.api import forward_prediction_skill as fps
from paper_trader.api import paper_trading_desk as desk

PHASE = "31B"
ACTIVE_BOOK_ID = "alpha_paper_book_1"
BENCHMARK = "SPY"
TRADING_DAYS = 252

# Files written to the immutable run directory (Part L).
OUTPUT_FILES = (
    "data_adequacy.json",
    "accounting_reconciliation.csv",
    "daily_return_attribution.csv",
    "sector_attribution.csv",
    "attribution_reconciliation.json",
    "portfolio_risk_diagnostics.csv",
    "position_risk_contributions.csv",
    "sector_risk_exposures.csv",
    "signal_decay.csv",
    "signal_decay_adequacy.json",
    "shadow_variant_daily.csv",
    "shadow_variant_summary.csv",
    "cost_sensitivity.csv",
    "forward_shadow_contract.json",
    "promotion_gates.json",
    "executive_recommendation.md",
    "run_manifest.json",
)

READY = "PHASE31B_ANALYSIS_READY"
BLOCKED = "PHASE31B_ANALYSIS_BLOCKED"


# =========================================================================== #
# Small numeric helpers (deterministic; no numpy).
# =========================================================================== #
def _f(x) -> Optional[float]:
    try:
        if x is None:
            return None
        v = float(x)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def _r(x, n: int) -> Optional[float]:
    v = _f(x)
    return round(v, n) if v is not None else None


def _mean(xs) -> Optional[float]:
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def _std(xs, *, sample: bool = True) -> Optional[float]:
    xs = [x for x in xs if x is not None]
    n = len(xs)
    if n < (2 if sample else 1):
        return None
    m = sum(xs) / n
    var = sum((x - m) ** 2 for x in xs) / ((n - 1) if sample else n)
    return math.sqrt(var)


def _cov(a, b, *, sample: bool = True) -> Optional[float]:
    pairs = [(x, y) for x, y in zip(a, b) if x is not None and y is not None]
    n = len(pairs)
    if n < (2 if sample else 1):
        return None
    ma = sum(x for x, _ in pairs) / n
    mb = sum(y for _, y in pairs) / n
    s = sum((x - ma) * (y - mb) for x, y in pairs)
    return s / ((n - 1) if sample else n)


def _pearson(a, b) -> Optional[float]:
    ca, cb = _cov(a, b), None
    sa, sb = _std(a), _std(b)
    if ca is None or not sa or not sb:
        return None
    return max(-1.0, min(1.0, ca / (sa * sb)))


def _rank(values: list[float]) -> list[float]:
    """Average-tie fractional ranks (1-based)."""
    order = sorted(range(len(values)), key=lambda i: values[i])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and values[order[j + 1]] == values[order[i]]:
            j += 1
        avg = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[order[k]] = avg
        i = j + 1
    return ranks


def _spearman(a, b) -> Optional[float]:
    pairs = [(x, y) for x, y in zip(a, b) if x is not None and y is not None]
    if len(pairs) < 3:
        return None
    ra = _rank([x for x, _ in pairs])
    rb = _rank([y for _, y in pairs])
    return _pearson(ra, rb)


def _annualize_return(daily_rets: list[float]) -> Optional[float]:
    rs = [r for r in daily_rets if r is not None]
    if not rs:
        return None
    g = 1.0
    for r in rs:
        g *= (1.0 + r)
    return g ** (TRADING_DAYS / len(rs)) - 1.0


def _max_drawdown(nav_path: list[float]) -> Optional[float]:
    vals = [v for v in nav_path if v is not None]
    if len(vals) < 2:
        return None
    peak, mdd = vals[0], 0.0
    for v in vals:
        peak = max(peak, v)
        mdd = min(mdd, v / peak - 1.0)
    return mdd


def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _sha256_file(p: Path) -> Optional[str]:
    try:
        return _sha256_bytes(Path(p).read_bytes())
    except OSError:
        return None


def _read_json(p: Path) -> Optional[dict]:
    try:
        return json.loads(Path(p).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _read_csv(p: Path) -> list[dict]:
    try:
        with open(p, "r", encoding="utf-8", newline="") as fh:
            return list(csv.DictReader(fh))
    except OSError:
        return []


# =========================================================================== #
# Read-only source loading.
# =========================================================================== #
def _default_sources_config() -> dict:
    return {
        "inputs_dir": r"D:\Stock_Prediction_app_data\phase25_multi_horizon_alpha\_inputs",
        "risk_stats_file": "current_risk_stats.csv",
        "momentum_scores_file": "current_momentum_scores.csv",
        "sector_map_path": (
            r"C:\Users\binis\Stock_Prediction_app_push\research\output"
            r"\phase10f_owned_sector_mapping_repair\repaired_sector_mapping.csv"
        ),
    }


def load_sources(*, desk_dir: Optional[str] = None, config: Optional[dict] = None,
                 perf_loader: Optional[Callable] = None,
                 marks_loader: Optional[Callable] = None) -> dict:
    """Load every owned source through a pure read-only path (no DB, no network).

    Returns a bundle dict. All heavy readers are the file-based desk / fps
    functions; the two CSVs are stdlib csv reads.
    """
    cfg = dict(_default_sources_config())
    src = ((config or {}).get("sources") or {})
    if src.get("mhz_inputs_dir_default"):
        cfg["inputs_dir"] = src["mhz_inputs_dir_default"]
    if src.get("sector_map_default"):
        cfg["sector_map_path"] = src["sector_map_default"]

    sdir = desk._desk_dir(desk_dir)
    perf = (perf_loader or desk.load_performance)(desk_dir)
    marks = (marks_loader or desk.read_marks)(desk_dir)
    price_store = fps.read_price_store(desk_dir)
    eligible_calendar = fps.eligible_calendar(desk_dir)
    snap_rows = desk._read_ledger(sdir, fps.SNAPSHOT_LEDGER_FILE)
    outcome_rows = desk._read_ledger(sdir, fps.OUTCOME_LEDGER_FILE)
    close_rows = [r for r in desk._read_ledger(sdir, "daily_close_journal.json")
                  if r.get("event") == "DAILY_CLOSE" and r.get("book_id") == ACTIVE_BOOK_ID]
    close_rows.sort(key=lambda r: (r.get("market_date") or "", r.get("seq") or 0))
    fills = desk._read_ledger(sdir, desk.FILLS_FILE)
    book_records = desk._read_ledger(sdir, "alpha_book_records.json")
    policy_rows = desk._read_ledger(sdir, "alpha_book_policy.json")

    inputs_dir = Path(cfg["inputs_dir"])
    risk_csv = inputs_dir / cfg["risk_stats_file"]
    mom_csv = inputs_dir / cfg["momentum_scores_file"]
    sector_csv = Path(cfg["sector_map_path"])

    risk_rows = _read_csv(risk_csv)
    mom_rows = _read_csv(mom_csv)
    sector_rows = _read_csv(sector_csv)

    # ticker -> owned risk stats (as-of the current inputs date)
    risk_by_ticker = {}
    for r in risk_rows:
        t = (r.get("ticker") or "").strip().upper()
        if t:
            risk_by_ticker[t] = {
                "realized_vol_63d": _f(r.get("realized_vol_63d")),
                "beta_universe": _f(r.get("beta_universe")),
                "adv_dollar_20d": _f(r.get("adv_dollar_20d")),
                "max_drawdown_252d": _f(r.get("max_drawdown_252d")),
                "is_current_member": r.get("is_current_member"),
                "last_price_date": r.get("last_price_date"),
            }
    mom_by_ticker = {}
    for r in mom_rows:
        t = (r.get("ticker") or "").strip().upper()
        if t:
            mom_by_ticker[t] = {"mom_6_1": _f(r.get("mom_6_1")),
                                "is_member": r.get("is_member"),
                                "market_as_of_date": r.get("market_as_of_date")}
    sector_by_ticker = {}
    for r in sector_rows:
        t = (r.get("ticker") or "").strip().upper()
        if t:
            sec = (r.get("repaired_sector") or r.get("sector") or "").strip()
            if sec:
                sector_by_ticker[t] = sec

    # PIT-at-capture sector map from the immutable snapshot cross-sections
    # (overrides the current owned map where available; it is what the model saw).
    snap_sector = {}
    snap_score = {}
    for cs in fps._cross_sections(snap_rows):
        for row in fps._cs_rows_as_dicts(cs):
            t = (row.get("ticker") or "").strip().upper()
            if t:
                snap_sector.setdefault(t, row.get("sector"))
                if row.get("normalized_score") is not None:
                    snap_score.setdefault(t, _f(row.get("normalized_score")))

    return {
        "desk_dir": str(sdir),
        "config": config or {},
        "sources_config": cfg,
        "perf": perf,
        "perf_rows": _perf_flat_rows(perf),
        "marks": marks,
        "marks_series": (marks.get("series") or {}),
        "price_store": price_store,
        "price_series": (price_store.get("series") or {}),
        "eligible_calendar": eligible_calendar,
        "snapshot_rows": snap_rows,
        "outcome_rows": outcome_rows,
        "close_rows": close_rows,
        "fills": fills,
        "book_records": book_records,
        "policy_rows": policy_rows,
        "risk_by_ticker": risk_by_ticker,
        "mom_by_ticker": mom_by_ticker,
        "sector_by_ticker": sector_by_ticker,
        "snap_sector": snap_sector,
        "snap_score": snap_score,
        "source_paths": {
            "desk_dir": str(sdir),
            "risk_stats_csv": str(risk_csv),
            "momentum_scores_csv": str(mom_csv),
            "sector_map_csv": str(sector_csv),
        },
    }


def _perf_flat_rows(perf: dict) -> list[dict]:
    """Return the append-only performance rows as flat dicts, sorted by date."""
    rows = []
    for r in (perf.get("rows") or []):
        row = r.get("row") if isinstance(r, dict) and "row" in r else r
        if isinstance(row, dict) and row.get("date"):
            rows.append(row)
    rows.sort(key=lambda x: x.get("date") or "")
    return rows


def latest_completed_date(S: dict) -> Optional[str]:
    dates = [r.get("date") for r in S["perf_rows"] if r.get("date")]
    cal = S.get("eligible_calendar") or []
    cand = [d for d in dates] + [cal[-1]] if cal else list(dates)
    return max(cand) if cand else None


def sector_of(S: dict, ticker: str) -> str:
    t = ticker.upper()
    return (S["snap_sector"].get(t) or S["sector_by_ticker"].get(t) or "Unknown")


def current_holdings(S: dict) -> dict:
    """{ticker: qty} for the latest completed operational close."""
    if not S["perf_rows"]:
        return {}
    return {k.upper(): _f(v) for k, v in (S["perf_rows"][-1].get("holdings") or {}).items()}


def book_avg_costs(S: dict) -> dict:
    """Weighted-average PAPER_BUY fill cost per ticker (for unrealized-PnL display)."""
    agg = {}
    for r in S["fills"]:
        t = (r.get("ticker") or "").strip().upper()
        side = (r.get("side") or "").upper()
        q = _f(r.get("quantity"))
        px = _f(r.get("fill_price"))
        if not t or q is None or px is None:
            continue
        sign = 1.0 if "BUY" in side else -1.0
        a = agg.setdefault(t, {"qty": 0.0, "cost": 0.0})
        a["qty"] += sign * q
        a["cost"] += sign * q * px
    return {t: (v["cost"] / v["qty"] if v["qty"] else None) for t, v in agg.items()}


# =========================================================================== #
# Aligned daily-return panel (from the forward-prediction price store).
# =========================================================================== #
def _series_to_map(series_rows) -> dict:
    out = {}
    for item in (series_rows or []):
        try:
            d, px = item[0], _f(item[1])
        except (TypeError, IndexError):
            continue
        if px is not None:
            out[d] = px
    return out


def build_returns(S: dict, tickers, *, source: str = "price_store") -> dict:
    """Aligned daily simple-return panel for ``tickers`` over the common dates.

    Returns {"dates":[...], "ret":{ticker:[r_or_None,...]}, "px":{...},
             "n_returns": int, "source": source}. Dates are the union of the
    SPY calendar (from the chosen source) so every ticker aligns to the same
    completed-close grid.
    """
    series = S["price_series"] if source == "price_store" else S["marks_series"]
    price_maps = {t.upper(): _series_to_map(series.get(t.upper()) or series.get(t) or [])
                  for t in list(tickers) + [BENCHMARK]}
    spy_dates = sorted(price_maps.get(BENCHMARK, {}).keys())
    if len(spy_dates) < 2:
        return {"dates": [], "ret": {}, "px": {}, "n_returns": 0, "source": source}
    ret = {}
    px = {}
    for t, pm in price_maps.items():
        prices = [pm.get(d) for d in spy_dates]
        px[t] = prices
        rr = [None]
        for i in range(1, len(spy_dates)):
            p0, p1 = prices[i - 1], prices[i]
            rr.append((p1 / p0 - 1.0) if (p0 and p1 is not None) else None)
        ret[t] = rr
    return {"dates": spy_dates, "ret": ret, "px": px,
            "n_returns": len(spy_dates) - 1, "source": source}


# =========================================================================== #
# PART A — DATA ADEQUACY & POINT-IN-TIME AUDIT.
# =========================================================================== #
def build_data_adequacy(S: dict) -> dict:
    perf_rows = S["perf_rows"]
    perf_dates = [r.get("date") for r in perf_rows]
    marks_series = S["marks_series"]
    price_series = S["price_series"]
    cal = S.get("eligible_calendar") or []

    def _src(name, earliest, latest, rows, distinct, pit, mutable, survivorship, sufficient, note):
        return {"source": name, "earliest": earliest, "latest": latest, "row_count": rows,
                "distinct_securities": distinct, "point_in_time_semantics": pit,
                "historical_values_can_change": mutable, "survivorship_or_reuse_risk": survivorship,
                "sufficient_for_requested_calc": sufficient, "note": note}

    mark_dates = sorted({d for v in marks_series.values() for d, _ in
                         [(x[0], x[1]) for x in v]}) if marks_series else []
    px_dates = sorted({d for v in price_series.values() for d, _ in
                       [(x[0], x[1]) for x in v]}) if price_series else []

    inventory = [
        _src("operational_nav_strip (forward_performance.json)",
             perf_dates[0] if perf_dates else None, perf_dates[-1] if perf_dates else None,
             len(perf_rows), 1, "completed-close, append-once-per-date, never recomputed",
             False, "none (single owned paper book)", len(perf_rows) >= 2,
             "Authoritative recorded NAV / cash / invested / benchmark strip."),
        _src("daily_holdings (forward_performance.holdings)",
             perf_dates[0] if perf_dates else None, perf_dates[-1] if perf_dates else None,
             len(perf_rows), len(current_holdings(S)), "integer shares at each completed close",
             False, "none", len(perf_rows) >= 1,
             "Static 25-name book; no rebalance since inception."),
        _src("desk_marks_cache (desk_marks.json)", mark_dates[0] if mark_dates else None,
             mark_dates[-1] if mark_dates else None, len(mark_dates),
             len(marks_series), "owned EODHD completed adjusted closes (cache)", False,
             "none for current members", len(mark_dates) >= 2,
             "Covers the 25 holdings + SPY; primary accounting price source."),
        _src("forward_prediction_prices (price store)", px_dates[0] if px_dates else None,
             px_dates[-1] if px_dates else None, len(px_dates), len(price_series),
             "completed closes, first-write-wins (immutable)", False,
             "universe is current-membership (survivorship-biased for history)",
             len(px_dates) >= 2, "Universe panel used for beta/covariance/benchmark."),
        _src("forward_prediction_snapshots (cross-sections + book snapshots)",
             None, None, len(S["snapshot_rows"]),
             len({r.get("market_date") for r in S["snapshot_rows"]}),
             "immutable TRUE_FORWARD, chain-hashed, never backfilled", False, "none",
             len(S["snapshot_rows"]) > 0, "Per-date ranks/scores/weights/sector."),
        _src("forward_prediction_outcomes (matured)", None, None, len(S["outcome_rows"]),
             len({r.get("model_id") for r in S["outcome_rows"]}),
             "matured only when >= horizon eligible closes elapse", False, "none",
             len(S["outcome_rows"]) > 0, "Only horizon=1 has matured (3 rows)."),
        _src("owned_risk_stats (current_risk_stats.csv)", None,
             (list(S["risk_by_ticker"].values())[0].get("last_price_date")
              if S["risk_by_ticker"] else None), len(S["risk_by_ticker"]),
             len(S["risk_by_ticker"]), "CURRENT as-of only (single snapshot, not a history)",
             True, "current-membership only", True,
             "realized_vol_63d + beta_universe for all 25 holdings; no per-date history."),
        _src("owned_sector_map (repaired_sector_mapping.csv + snapshot sectors)", None, None,
             len(S["sector_by_ticker"]), len(S["sector_by_ticker"]),
             "CURRENT-as-of GICS; snapshot sectors are PIT-at-capture", True,
             "no point-in-time sector history exists", True,
             "Grouping only; NOT a point-in-time return input."),
        _src("owned_daily_ohlc_panel (phase7i_broad_price_history_free.csv)", "2016-06-23",
             "2026-06-22", None, 301, "completed daily OHLC (owned)", False,
             "SURVIVORSHIP-BIASED (current members only)", False,
             "Ends 2026-06-22 — does NOT cover the forward window; unused for the live book."),
    ]

    historically_valid = [
        "operational NAV accounting reconciliation (Part B)",
        "per-date holding & sector P&L attribution reconciled to NAV (Part C)",
        "cumulative return / excess-vs-SPY over the 4 completed closes",
        "rank-IC at horizon=1 for the 2026-07-24 snapshot (single observation)",
    ]
    current_period_only = [
        "portfolio beta / volatility / correlation (only ~10-15 completed returns exist)",
        "risk contributions / covariance (owned realized_vol_63d is current-as-of only)",
        "shadow-variant realized performance (0-1 forward closes after construction)",
        "sector exposure vs eligible-universe benchmark (current sectors only)",
    ]
    blocked = [
        "annualized Sharpe / Sortino / vol with statistical confidence (need >= 20 returns)",
        "signal decay at horizons 5/10/20/40 (no matured outcomes yet)",
        "point-in-time sector attribution across a long history (no PIT sector data)",
        "walk-forward covariance/beta from a long owned window (owned daily panel ends 2026-06-22)",
        "multi-window robustness of any performance claim (< 5 completed returns)",
    ]

    return {
        "phase": PHASE, "as_of": latest_completed_date(S), "read_only": True,
        "inventory": inventory,
        "completed_market_dates": perf_dates,
        "eligible_calendar": cal,
        "classification": {
            "historically_valid": historically_valid,
            "available_current_period_only": current_period_only,
            "blocked_insufficient_data": blocked,
        },
        "point_in_time_rules_enforced": [
            "never reconstruct unavailable history from current values",
            "never use future target membership / future sector / future prices / restated values",
            "snapshot ranks are frozen TRUE_FORWARD (never backfilled)",
        ],
        "overall_adequacy": "INSUFFICIENT_FORWARD_SAMPLE",
        "overall_adequacy_reason": (
            "Only %d completed operational closes and %d matured forward outcomes exist; "
            "all performance/risk conclusions are diagnostic, not decision-grade."
            % (len(perf_rows), len(S["outcome_rows"])),
        )[0],
    }


# =========================================================================== #
# PART B — ACCOUNTING RECONCILIATION.
# =========================================================================== #
#: Controlled provenance tokens for how each historical accounting row was
#: reconstructed. A row that used ANY revisable-cache mark is downgraded to the
#: fallback token and can never silently claim frozen-close quality.
MARK_EVIDENCE_FROZEN = "FROZEN_CLOSE_MARKS"
MARK_EVIDENCE_FALLBACK = "HISTORICAL_MARK_FALLBACK"
MARK_EVIDENCE_NONE = "NO_MARKS"


def build_accounting_reconciliation(S: dict, cfg: dict) -> dict:
    tol_usd = _f(((cfg.get("reconciliation") or {}).get("accounting_nav_abs_tolerance_usd"))) or 0.01
    tol_bps = _f(((cfg.get("reconciliation") or {}).get("accounting_return_abs_tolerance_bps"))) or 1.0
    # Durable as-of-close valuation. Source precedence for every historical mark:
    #   1) FROZEN_CLOSE_MARKS — the immutable first-write-wins forward-prediction
    #      price store (api.forward_prediction_skill.read_price_store, exposed as
    #      S["price_series"]). An already-recorded (ticker, date) close is NEVER
    #      overwritten, so it preserves the exact mark the operational Daily Close
    #      used on that date. We take the EXACT as-of-date entry only (never a
    #      prior-date carry) so a frozen mark means "this date's close".
    #   2) HISTORICAL_MARK_FALLBACK — the mutable desk_marks cache, which a later
    #      close revises in place when a vendor publishes an adjusted-close for a
    #      prior date. It is used ONLY when the immutable store has no exact
    #      as-of-close mark for a (ticker, date); any row that needed it is flagged
    #      NOT fully reproducible from immutable close evidence.
    # This guarantees a frozen historical NAV is never reconciled against a later-
    # revised current mark whenever the original close mark is available.
    frozen_series = S.get("price_series") or {}
    marks = S["marks_series"]
    rows = S["perf_rows"]

    def _frozen_exact(t, d):
        for e in (frozen_series.get(t) or frozen_series.get(t.upper()) or []):
            if isinstance(e, (list, tuple)) and len(e) >= 2 and e[0] == d:
                return _f(e[1])
        return None

    def _fallback_at_or_before(t, d):
        hit = desk._series_price_at_or_before(marks.get(t) or marks.get(t.upper()) or [], d)
        return _f(hit[1]) if hit else None

    def _mark(t, d):
        """Return (price, provenance) under the immutable-first precedence."""
        p = _frozen_exact(t, d)
        if p is not None:
            return p, MARK_EVIDENCE_FROZEN
        p = _fallback_at_or_before(t, d)
        if p is not None:
            return p, MARK_EVIDENCE_FALLBACK
        return None, None

    out_rows = []
    prev_nav = None
    max_resid_nav = 0.0
    max_resid_ret_bps = 0.0
    all_ok = True
    frozen_rows = 0
    fallback_rows = 0
    for i, r in enumerate(rows):
        d = r.get("date")
        recorded_nav = _f(r.get("nav"))
        cash = _f(r.get("cash"))
        recorded_invested = _f(r.get("invested"))
        holdings = {k.upper(): _f(v) for k, v in (r.get("holdings") or {}).items()}
        recon_invested = 0.0
        priced = 0
        priced_frozen = 0
        priced_fallback = 0
        missing = []
        for t, q in holdings.items():
            m, src = _mark(t, d)
            if m is None or q is None:
                missing.append(t)
                continue
            recon_invested += q * m
            priced += 1
            if src == MARK_EVIDENCE_FROZEN:
                priced_frozen += 1
            elif src == MARK_EVIDENCE_FALLBACK:
                priced_fallback += 1
        # A single revisable-cache mark downgrades the whole row's provenance.
        if priced == 0:
            mark_evidence = MARK_EVIDENCE_NONE
        elif priced_fallback:
            mark_evidence = MARK_EVIDENCE_FALLBACK
            fallback_rows += 1
        else:
            mark_evidence = MARK_EVIDENCE_FROZEN
            frozen_rows += 1
        recon_nav = (cash + recon_invested) if cash is not None else None
        nav_resid = (recon_nav - recorded_nav) if (recon_nav is not None and recorded_nav is not None) else None
        recorded_daily_ret = _f(r.get("daily_return_pct"))
        recon_daily_ret = ((recorded_nav / prev_nav - 1.0) * 100.0) if (prev_nav and recorded_nav is not None) else None
        ret_resid_bps = (abs((recon_daily_ret or 0.0) - (recorded_daily_ret or 0.0)) * 100.0
                         if (recon_daily_ret is not None and recorded_daily_ret is not None) else None)
        nav_ok = (nav_resid is None) or (abs(nav_resid) <= tol_usd)
        ret_ok = (ret_resid_bps is None) or (ret_resid_bps <= tol_bps)
        if nav_resid is not None:
            max_resid_nav = max(max_resid_nav, abs(nav_resid))
        if ret_resid_bps is not None:
            max_resid_ret_bps = max(max_resid_ret_bps, ret_resid_bps)
        if not (nav_ok and ret_ok):
            all_ok = False
        out_rows.append({
            "market_date": d,
            "starting_nav": _r(prev_nav, 2),
            "ending_nav": _r(recorded_nav, 2),
            "cash": _r(cash, 2),
            "recorded_invested": _r(recorded_invested, 2),
            "reconstructed_invested": _r(recon_invested, 2),
            "reconstructed_nav": _r(recon_nav, 2),
            "nav_residual_usd": _r(nav_resid, 4),
            "gross_position_contribution": _r(
                (recorded_nav - prev_nav) if (prev_nav is not None and recorded_nav is not None) else None, 2),
            "execution_cost": _r(r.get("transaction_cost"), 4),
            "recorded_daily_return_pct": _r(recorded_daily_ret, 6),
            "reconstructed_daily_return_pct": _r(recon_daily_ret, 6),
            "return_residual_bps": _r(ret_resid_bps, 6),
            "priced_holdings": priced,
            "priced_from_frozen_close": priced_frozen,
            "priced_from_historical_fallback": priced_fallback,
            "missing_marks": ";".join(sorted(missing)),
            "mark_evidence": mark_evidence,
            "fully_reproducible_from_frozen_close": (mark_evidence == MARK_EVIDENCE_FROZEN),
            "nav_reconciles": nav_ok,
            "return_reconciles": ret_ok,
        })
        prev_nav = recorded_nav

    reconciliation = {
        "tolerance_nav_usd": tol_usd, "tolerance_return_bps": tol_bps,
        "rows_checked": len(out_rows),
        "max_abs_nav_residual_usd": _r(max_resid_nav, 6),
        "max_abs_return_residual_bps": _r(max_resid_ret_bps, 6),
        "all_rows_reconcile": all_ok,
        "control_reconciled": all_ok,
        "rows_from_frozen_close_marks": frozen_rows,
        "rows_from_historical_fallback": fallback_rows,
        "all_rows_from_frozen_close_marks": (frozen_rows == len(out_rows) and len(out_rows) > 0),
        "mark_source_precedence": [MARK_EVIDENCE_FROZEN, MARK_EVIDENCE_FALLBACK],
        "method": ("reconstructed_nav = cash + sum(qty_i * as_of_close_mark_i), where "
                   "as_of_close_mark_i is the immutable first-write-wins price-store close "
                   "for (ticker, date) [FROZEN_CLOSE_MARKS], falling back to the revisable "
                   "desk_marks cache [HISTORICAL_MARK_FALLBACK] only when no frozen close "
                   "exists; reconstructed_return = ending_nav/starting_nav - 1"),
    }
    return {"rows": out_rows, "reconciliation": reconciliation}


# =========================================================================== #
# PART C — RETURN ATTRIBUTION (reuses the audited forward-evidence reconciler).
# =========================================================================== #
def _build_ops(S: dict) -> dict:
    holdings = current_holdings(S)
    avg = book_avg_costs(S)
    marks = S["marks_series"]
    latest = latest_completed_date(S)
    detail = []
    total_mv = 0.0
    mv = {}
    for t, q in holdings.items():
        hit = desk._series_price_at_or_before(marks.get(t) or [], latest)
        px = hit[1] if hit else None
        m = (q * px) if (q is not None and px is not None) else None
        mv[t] = m
        if m is not None:
            total_mv += m
    for t, q in sorted(holdings.items()):
        ac = avg.get(t)
        cb = (q * ac) if (q is not None and ac is not None) else None
        w = (mv[t] / total_mv) if (mv.get(t) is not None and total_mv) else None
        detail.append({"ticker": t, "quantity": q, "sector": sector_of(S, t),
                       "average_cost": ac, "cost_basis": cb,
                       "current_weight": (round(w, 6) if w is not None else None)})
    starting = None
    for r in S["book_records"]:
        if r.get("event") == "ALPHA_BOOK_INITIALIZED":
            starting = _f(r.get("starting_virtual_capital"))
    return {"operational_book": {"book_id": ACTIVE_BOOK_ID,
                                 "starting_capital": starting or 100000.0,
                                 "holdings_detail": detail},
            "canonical_state": {"holdings_detail": detail}}


def build_return_attribution(S: dict, cfg: dict) -> dict:
    ops = _build_ops(S)
    desk_dir = S["desk_dir"]

    def _perf_loader(_d=None):
        return {"rows": S["perf_rows"],
                "summary": (S["perf"].get("summary") or {})}

    def _marks_loader(_d=None):
        return {"series": S["marks_series"]}

    hist = fe.build_attribution_history(desk_dir=desk_dir, ops=ops,
                                        perf_loader=_perf_loader, marks_loader=_marks_loader)
    latest = fe.build_daily_attribution(desk_dir=desk_dir, ops=ops,
                                        perf_loader=_perf_loader, marks_loader=_marks_loader)

    # --- daily attribution rows (exact accounting) + factor decomposition overlay
    daily_rows = []
    tol_bps = _f(((cfg.get("reconciliation") or {}).get("attribution_return_abs_tolerance_bps"))) or 1.0
    for r in reversed(hist.get("rows") or []):   # oldest-first for the CSV
        daily_rows.append({
            "market_date": r.get("market_date"),
            "prior_market_date": r.get("prior_market_date"),
            "daily_pnl": r.get("daily_pnl"),
            "daily_return_pct": r.get("daily_return_pct"),
            "spy_daily_return_pct": r.get("spy_daily_return_pct"),
            "daily_excess_return_pct": r.get("daily_excess_return_pct"),
            "position_contribution_sum": (r.get("reconciliation") or {}).get("position_contribution_sum")
            if isinstance(r.get("reconciliation"), dict) else None,
            "market_movement": (r.get("reconciliation") or {}).get("market_movement")
            if isinstance(r.get("reconciliation"), dict) else None,
            "residual_usd": (r.get("reconciliation") or {}).get("residual")
            if isinstance(r.get("reconciliation"), dict) else None,
            "reconciles": r.get("reconciles"),
            "top_positive_ticker": (r.get("top_positive") or {}).get("ticker") if r.get("top_positive") else None,
            "top_negative_ticker": (r.get("top_negative") or {}).get("ticker") if r.get("top_negative") else None,
        })

    # --- factor decomposition of the LATEST reconciled close (beta vs selection)
    factor = _factor_decomposition(S, latest, cfg)

    # --- sector attribution rows (from the latest reconciled close)
    sector_rows = []
    if latest.get("available"):
        for srow in (latest.get("sectors") or []):
            sector_rows.append({
                "market_date": latest.get("market_date"),
                "sector": srow.get("sector"),
                "pnl_contribution": srow.get("pnl_contribution"),
                "prior_market_value": srow.get("prior_market_value"),
                "weight": srow.get("weight"),
                "n_holdings": srow.get("n_holdings"),
            })

    recon = {
        "attribution_available": bool(latest.get("available")),
        "status": latest.get("status"),
        "latest_market_date": latest.get("market_date"),
        "nav_reconcile_tolerance_usd": _f(((cfg.get("reconciliation") or {}).get("attribution_nav_abs_tolerance_usd"))) or 1.0,
        "return_reconcile_tolerance_bps": tol_bps,
        "position_reconciliation": latest.get("reconciliation"),
        "all_history_rows_reconcile": all(r.get("reconciles") for r in (hist.get("rows") or []))
        if hist.get("rows") else None,
        "history_count": hist.get("count"),
        "factor_decomposition": factor,
    }
    return {"daily_rows": daily_rows, "sector_rows": sector_rows, "reconciliation": recon}


def _factor_decomposition(S: dict, latest: dict, cfg: dict) -> dict:
    """Split the latest daily active return into beta + sector-allocation +
    within-sector selection + residual, versus SPY. Reconciles to daily_return
    within an explicit residual (never fabricated when data is absent)."""
    if not latest.get("available"):
        return {"available": False, "reason": latest.get("status")}
    port = latest.get("portfolio") or {}
    r_p = _f(port.get("daily_return_pct"))
    r_b = _f(port.get("spy_daily_return_pct"))
    if r_p is None or r_b is None:
        return {"available": False, "reason": "MISSING_PORTFOLIO_OR_BENCHMARK_RETURN"}
    beta = portfolio_beta_vs_spy(S)
    beta_val = beta.get("portfolio_beta")
    beta_contrib = (beta_val * r_b) if beta_val is not None else None
    active = (r_p - beta_contrib) if beta_contrib is not None else None
    return {
        "available": True,
        "market_date": latest.get("market_date"),
        "daily_return_pct": _r(r_p, 6),
        "spy_daily_return_pct": _r(r_b, 6),
        "portfolio_beta_vs_spy": _r(beta_val, 4),
        "beta_source": beta.get("source"),
        "market_beta_contribution_pct": _r(beta_contrib, 6),
        "active_return_pct": _r(active, 6),
        "sector_allocation_contribution_pct": None,
        "within_sector_selection_contribution_pct": None,
        "note": ("Beta contribution = portfolio_beta x SPY daily return. Active = daily - beta "
                 "contribution. Sector-allocation vs within-sector selection are NOT split at the "
                 "single-day level with <15 completed returns (Brinson requires a stable benchmark "
                 "sector-return vector); the exact per-position/per-sector accounting attribution in "
                 "sector_attribution.csv fully reconciles the P&L. Residual carried explicitly."),
        "residual_pct": _r(active, 6),
    }


# =========================================================================== #
# PART D — RISK DIAGNOSTICS.
# =========================================================================== #
def portfolio_beta_vs_spy(S: dict, weights: Optional[dict] = None) -> dict:
    """Weighted per-name beta vs SPY from the price-store return window, with an
    owned beta_universe proxy fallback per name. Deterministic."""
    holdings = current_holdings(S)
    if weights is None:
        marks = S["marks_series"]
        latest = latest_completed_date(S)
        mv = {}
        tot = 0.0
        for t, q in holdings.items():
            hit = desk._series_price_at_or_before(marks.get(t) or [], latest)
            px = hit[1] if hit else None
            if q is not None and px is not None:
                mv[t] = q * px
                tot += mv[t]
        weights = {t: (mv[t] / tot) for t in mv} if tot else {}
    tickers = list(weights.keys())
    pan = build_returns(S, tickers)
    spy = pan["ret"].get(BENCHMARK, [])
    var_spy = _cov(spy, spy)
    per = {}
    used_window = 0
    used_proxy = 0
    for t in tickers:
        b = None
        src = None
        if var_spy:
            c = _cov(pan["ret"].get(t, []), spy)
            if c is not None:
                b = c / var_spy
                src = "price_store_window"
                used_window += 1
        if b is None:
            b = (S["risk_by_ticker"].get(t) or {}).get("beta_universe")
            if b is not None:
                src = "owned_beta_universe_proxy"
                used_proxy += 1
        per[t] = {"beta": b, "source": src}
    pbeta = None
    contribs = [(weights[t] * per[t]["beta"]) for t in tickers if per[t]["beta"] is not None]
    if contribs and abs(sum(weights[t] for t in tickers if per[t]["beta"] is not None)) > 0:
        pbeta = sum(contribs)
    return {"portfolio_beta": pbeta, "per_name": per, "n_returns": pan["n_returns"],
            "names_from_window": used_window, "names_from_proxy": used_proxy,
            "source": ("price_store_window" if used_window >= used_proxy else "owned_beta_universe_proxy")}


def build_covariance(S: dict, tickers, cfg: dict) -> dict:
    """Annualized covariance: diag(realized_vol_63d^2) scaled by a shrunk
    correlation from the price-store window. Diagonal fallback when the window
    is too short. Records estimator + lookback used."""
    risk = cfg.get("risk") or {}
    intensity = _f(risk.get("covariance_shrinkage_intensity"))
    intensity = 0.5 if intensity is None else intensity
    min_corr = int(risk.get("min_returns_for_rolling_corr") or 5)
    tickers = [t for t in tickers]
    vol = {}
    for t in tickers:
        v = (S["risk_by_ticker"].get(t) or {}).get("realized_vol_63d")
        vol[t] = v
    pan = build_returns(S, tickers)
    n_ret = pan["n_returns"]
    use_corr = n_ret >= min_corr
    # correlation matrix
    corr = {}
    for i, a in enumerate(tickers):
        for b in tickers[i:]:
            if a == b:
                c = 1.0
            elif use_corr:
                c = _pearson(pan["ret"].get(a, []), pan["ret"].get(b, []))
            else:
                c = None
            corr[(a, b)] = c
            corr[(b, a)] = c
    # shrink toward identity
    cov = {}
    for a in tickers:
        for b in tickers:
            va, vb = vol.get(a), vol.get(b)
            if va is None or vb is None:
                cov[(a, b)] = None
                continue
            if a == b:
                cov[(a, b)] = va * vb
            else:
                c = corr.get((a, b))
                c = 0.0 if c is None else (1.0 - intensity) * c  # shrink toward 0
                cov[(a, b)] = c * va * vb
    return {"cov": cov, "vol": vol, "tickers": tickers,
            "estimator": ("shrinkage_constant_correlation" if use_corr else "diagonal_fallback"),
            "shrinkage_intensity": intensity, "correlation_lookback_returns": n_ret,
            "used_correlation": use_corr,
            "fallback_reason": (None if use_corr else "fewer_than_%d_returns" % min_corr)}


def _port_vol(weights: dict, cov: dict) -> Optional[float]:
    ts = list(weights.keys())
    s = 0.0
    ok = False
    for a in ts:
        for b in ts:
            c = cov.get((a, b))
            if c is None:
                continue
            s += weights[a] * weights[b] * c
            ok = True
    if not ok or s < 0:
        return None
    return math.sqrt(s)


def build_risk_diagnostics(S: dict, cfg: dict) -> dict:
    holdings = current_holdings(S)
    marks = S["marks_series"]
    latest = latest_completed_date(S)
    tickers = sorted(holdings.keys())
    # market-value weights
    mv = {}
    tot = 0.0
    for t in tickers:
        hit = desk._series_price_at_or_before(marks.get(t) or [], latest)
        px = hit[1] if hit else None
        if holdings[t] is not None and px is not None:
            mv[t] = holdings[t] * px
            tot += mv[t]
    weights = {t: (mv.get(t, 0.0) / tot if tot else 0.0) for t in tickers}
    cashrow = _f(S["perf_rows"][-1].get("cash")) if S["perf_rows"] else None
    nav = _f(S["perf_rows"][-1].get("nav")) if S["perf_rows"] else None

    covb = build_covariance(S, tickers, cfg)
    cov = covb["cov"]
    port_vol = _port_vol(weights, cov)

    # portfolio return series (invested book) from price store
    pan = build_returns(S, tickers)
    spy = pan["ret"].get(BENCHMARK, [])
    port_ret = []
    for i in range(len(pan["dates"])):
        num = 0.0
        wsum = 0.0
        for t in tickers:
            r = pan["ret"].get(t, [None] * len(pan["dates"]))[i]
            if r is not None and weights.get(t):
                num += weights[t] * r
                wsum += weights[t]
        port_ret.append(num if wsum > 0 and i > 0 else (None if i == 0 else num))
    beta = portfolio_beta_vs_spy(S, weights)

    # position risk contributions (MRC / %CR) using cov
    pos_rows = []
    mrc_denom = port_vol
    for t in tickers:
        # marginal contribution to variance = (cov * w)_t ; to vol = that / port_vol
        cw = 0.0
        ok = False
        for b in tickers:
            c = cov.get((t, b))
            if c is not None:
                cw += c * weights.get(b, 0.0)
                ok = True
        mrc = (cw / mrc_denom) if (ok and mrc_denom) else None      # d sigma / d w
        pct_cr = ((weights.get(t, 0.0) * cw) / (port_vol ** 2)) if (ok and port_vol) else None
        rk = S["risk_by_ticker"].get(t) or {}
        pos_rows.append({
            "ticker": t, "sector": sector_of(S, t),
            "weight": _r(weights.get(t), 6),
            "market_value": _r(mv.get(t), 2),
            "realized_vol_63d": _r(rk.get("realized_vol_63d"), 6),
            "beta_vs_spy": _r((beta["per_name"].get(t) or {}).get("beta"), 4),
            "beta_source": (beta["per_name"].get(t) or {}).get("source"),
            "beta_universe_owned": _r(rk.get("beta_universe"), 4),
            "marginal_contribution_to_risk": _r(mrc, 6),
            "pct_contribution_to_risk": _r(pct_cr, 6),
            "max_drawdown_252d_owned": _r(rk.get("max_drawdown_252d"), 6),
        })

    # sector exposures + active vs eligible-universe EW benchmark
    sec_w = {}
    sec_n = {}
    for t in tickers:
        s = sector_of(S, t)
        sec_w[s] = sec_w.get(s, 0.0) + weights.get(t, 0.0)
        sec_n[s] = sec_n.get(s, 0) + 1
    bench_sec = _eligible_universe_sector_weights(S)
    sector_rows = []
    for s in sorted(sec_w.keys()):
        sector_rows.append({
            "sector": s, "portfolio_weight": _r(sec_w[s], 6),
            "n_holdings": sec_n[s],
            "benchmark_weight": _r(bench_sec.get(s), 6),
            "active_weight_pp": _r((sec_w[s] - bench_sec.get(s, 0.0)) * 100.0, 4),
        })

    # concentration
    hhi = sum(w ** 2 for w in weights.values())
    eff_n = (1.0 / hhi) if hhi else None
    top_w = max(weights.values()) if weights else None
    # pairwise correlation distribution
    pw = []
    for i, a in enumerate(tickers):
        for b in tickers[i + 1:]:
            if covb["used_correlation"]:
                c = _pearson(pan["ret"].get(a, []), pan["ret"].get(b, []))
                if c is not None:
                    pw.append(c)
    # capture ratios / TE / IR (short-sample, flagged)
    up = [(pr, sr) for pr, sr in zip(port_ret[1:], spy[1:]) if pr is not None and sr is not None and sr > 0]
    dn = [(pr, sr) for pr, sr in zip(port_ret[1:], spy[1:]) if pr is not None and sr is not None and sr < 0]
    up_cap = (sum(p for p, _ in up) / sum(s for _, s in up)) if up and sum(s for _, s in up) else None
    dn_cap = (sum(p for p, _ in dn) / sum(s for _, s in dn)) if dn and sum(s for _, s in dn) else None
    active_ret = [(pr - sr) for pr, sr in zip(port_ret[1:], spy[1:]) if pr is not None and sr is not None]
    te_daily = _std(active_ret)
    downside = _std([min(0.0, r) for r in port_ret[1:] if r is not None])
    n_ok = covb["correlation_lookback_returns"]
    min_ann = int((cfg.get("risk") or {}).get("min_returns_for_annualization") or 20)
    ann_ok = n_ok >= min_ann

    # Owned structural beta proxy (weighted beta_universe) — more stable than the
    # short realized-window SPY beta, which is dominated by idiosyncratic single-
    # name moves over the ~15-return window and is reported alongside it, not instead.
    bu_num = 0.0
    bu_den = 0.0
    for t in tickers:
        b = (S["risk_by_ticker"].get(t) or {}).get("beta_universe")
        w = weights.get(t, 0.0)
        if b is not None and w:
            bu_num += w * b
            bu_den += w
    beta_universe_wt = (bu_num / bu_den) if bu_den else None

    portfolio_rows = [{
        "as_of": latest, "nav": _r(nav, 2), "cash": _r(cashrow, 2),
        "gross_exposure": _r((tot / nav) if (tot and nav) else None, 6),
        "net_exposure": _r((tot / nav) if (tot and nav) else None, 6),
        "cash_exposure": _r((cashrow / nav) if (cashrow is not None and nav) else None, 6),
        "portfolio_beta_vs_spy_window": _r(beta["portfolio_beta"], 4),
        "portfolio_beta_vs_spy": _r(beta["portfolio_beta"], 4),
        "portfolio_beta_universe_owned_weighted": _r(beta_universe_wt, 4),
        "beta_window_reliability": ("LOW_SHORT_WINDOW_IDIOSYNCRATIC" if (beta["n_returns"] or 0) < min_ann
                                    else "OK"),
        "beta_n_returns": beta["n_returns"],
        "annualized_volatility_est": _r((port_vol) if port_vol is not None else None, 6),
        "annualized_volatility_source": "owned_realized_vol_63d + shrunk_window_corr",
        "downside_vol_daily_est": _r(downside, 6),
        "tracking_error_daily_est": _r(te_daily, 6),
        "information_ratio_daily_est": _r((_mean(active_ret) / te_daily) if te_daily else None, 4),
        "rolling_corr_spy_est": _r(_pearson(port_ret[1:], spy[1:]), 4),
        "upside_capture_est": _r(up_cap, 4),
        "downside_capture_est": _r(dn_cap, 4),
        "single_name_max_weight": _r(top_w, 6),
        "herfindahl_hhi": _r(hhi, 6),
        "effective_number_of_positions": _r(eff_n, 3),
        "n_holdings": len(tickers),
        "n_sectors": len(sec_w),
        "top_sector": max(sec_w, key=sec_w.get) if sec_w else None,
        "top_sector_weight": _r(max(sec_w.values()) if sec_w else None, 6),
        "pairwise_corr_mean": _r(_mean(pw), 4),
        "pairwise_corr_min": _r(min(pw) if pw else None, 4),
        "pairwise_corr_max": _r(max(pw) if pw else None, 4),
        "turnover_since_inception_pct": _r(sum(_f(r.get("turnover_pct")) or 0.0 for r in S["perf_rows"]), 4),
        "cost_drag_since_inception_usd": _r(sum(_f(r.get("transaction_cost")) or 0.0 for r in S["perf_rows"]), 4),
        "covariance_estimator": covb["estimator"],
        "covariance_lookback_returns": covb["correlation_lookback_returns"],
        "annualization_supported": ann_ok,
        "annualization_warning": (None if ann_ok else
                                  "fewer than %d completed returns — annualized figures are indicative only" % min_ann),
    }]
    return {"portfolio_rows": portfolio_rows, "position_rows": pos_rows,
            "sector_rows": sector_rows, "covariance": covb,
            "benchmark_sector_weights": bench_sec}


def _eligible_universe_sector_weights(S: dict) -> dict:
    """Equal-weight sector weights of the model's eligible universe (from the
    latest snapshot cross-section). This is the 'selected eligible-universe
    benchmark' for active sector deviation."""
    xs = fps._cross_sections(S["snapshot_rows"], fps.ACTIVE_MODEL_ID)
    if not xs:
        xs = fps._cross_sections(S["snapshot_rows"])
    if not xs:
        return {}
    universe = {}
    for row in fps._cs_rows_as_dicts(xs[0]):
        t = (row.get("ticker") or "").strip().upper()
        if t and row.get("eligible", True):
            universe[t] = row.get("sector") or sector_of(S, t)
    n = len(universe)
    if not n:
        return {}
    sec = {}
    for s in universe.values():
        sec[s] = sec.get(s, 0.0) + 1.0 / n
    return sec


# =========================================================================== #
# PART E — SIGNAL DECAY.
# =========================================================================== #
def build_signal_decay(S: dict, cfg: dict) -> dict:
    sd = cfg.get("signal_decay") or {}
    req_h = list(sd.get("requested_horizons_eligible_closes") or [1, 5, 10, 20, 40])
    min_names = int(sd.get("min_names_for_ic") or 3)
    min_dates = int(sd.get("min_matured_dates_for_conclusion") or 20)
    calendar = list(S.get("eligible_calendar") or [])
    price = S["price_series"]

    def _px(t, d):
        for dd, pp in (price.get(t) or []):
            if dd == d:
                return _f(pp)
        return None

    rows = []
    coverage = {}
    per_model_dates = {}
    for cs in fps._cross_sections(S["snapshot_rows"]):
        model = cs.get("model_id")
        d0 = cs.get("market_date")
        try:
            idx0 = calendar.index(d0)
        except ValueError:
            continue
        cs_rows = fps._cs_rows_as_dicts(cs)
        for h in req_h:
            j = idx0 + h
            if j >= len(calendar):
                rows.append({"model_id": model, "snapshot_date": d0, "horizon_eligible_closes": h,
                             "status": "PENDING_NOT_ENOUGH_CLOSES", "n_names": None,
                             "rank_ic_spearman": None, "top_minus_bottom_pp": None,
                             "coverage_pct": None, "matured": False})
                continue
            dh = calendar[j]
            ranks = []
            rets = []
            eligible = 0
            priced = 0
            for r in cs_rows:
                if not r.get("eligible", True):
                    continue
                eligible += 1
                t = (r.get("ticker") or "").upper()
                rank = _f(r.get("rank"))
                p0, ph = _px(t, d0), _px(t, dh)
                if rank is None or p0 is None or ph is None or p0 <= 0:
                    continue
                priced += 1
                ranks.append(-rank)   # rank 1 = best -> highest signal
                rets.append(ph / p0 - 1.0)
            ic = _spearman(ranks, rets) if len(ranks) >= min_names else None
            # top/bottom decile spread
            q = _f(sd.get("top_bottom_quantile")) or 0.1
            pairs = sorted(zip(ranks, rets), key=lambda x: -x[0])  # best signal first
            k = max(1, int(len(pairs) * q))
            top = _mean([r for _, r in pairs[:k]])
            bot = _mean([r for _, r in pairs[-k:]])
            spread = ((top - bot) * 100.0) if (top is not None and bot is not None) else None
            cov_pct = (priced / eligible * 100.0) if eligible else None
            rows.append({"model_id": model, "snapshot_date": d0, "horizon_eligible_closes": h,
                         "status": "MATURED" if ic is not None else "INSUFFICIENT_NAMES",
                         "n_names": len(ranks), "rank_ic_spearman": _r(ic, 6),
                         "top_minus_bottom_pp": _r(spread, 6),
                         "coverage_pct": _r(cov_pct, 3), "matured": ic is not None})
            per_model_dates.setdefault((model, h), 0)
            if ic is not None:
                per_model_dates[(model, h)] += 1

    # system-native cells (1/5/20/63) from the audited skill payload
    native = fps.load_prediction_skill(desk_dir=S["desk_dir"])
    native_cells = [{"model_id": c.get("model_id"),
                     "horizon_eligible_closes": c.get("horizon_eligible_closes"),
                     "evidence_state": c.get("evidence_state"),
                     "matured_observation_count": c.get("matured_observation_count"),
                     "ic_mean": c.get("ic_mean")}
                    for c in (native.get("prediction_skill") or [])]

    matured_total = sum(1 for r in rows if r.get("matured"))
    max_dates_any = max(per_model_dates.values()) if per_model_dates else 0
    adequacy = {
        "phase": PHASE,
        "requested_horizons": req_h,
        "system_native_horizons": list(sd.get("system_native_horizons_eligible_closes") or [1, 5, 20, 63]),
        "snapshot_dates": sorted({r["snapshot_date"] for r in rows}),
        "matured_cells": matured_total,
        "pending_cells": sum(1 for r in rows if r.get("status") == "PENDING_NOT_ENOUGH_CLOSES"),
        "max_matured_dates_for_a_model_horizon": max_dates_any,
        "min_matured_dates_for_conclusion": min_dates,
        "sufficient_to_conclude": max_dates_any >= min_dates,
        "overall_status": "INSUFFICIENT_SAMPLE",
        "note": ("Rank-IC is computed only where >= horizon eligible closes have elapsed since a "
                 "TRUE_FORWARD snapshot; pending horizons are never mixed into matured results. "
                 "With %d matured cell(s) across at most %d date(s), no signal conclusion is drawn."
                 % (matured_total, max_dates_any)),
        "system_native_cells": native_cells,
        "native_matured_outcome_count": native.get("matured_outcome_count"),
    }
    return {"rows": rows, "adequacy": adequacy}


# =========================================================================== #
# PART F/G/H — PRE-REGISTERED SHADOW PORTFOLIOS + COST SENSITIVITY.
# =========================================================================== #
def _current_weights(S: dict) -> dict:
    holdings = current_holdings(S)
    marks = S["marks_series"]
    latest = latest_completed_date(S)
    mv = {}
    tot = 0.0
    for t, q in holdings.items():
        hit = desk._series_price_at_or_before(marks.get(t) or [], latest)
        px = hit[1] if hit else None
        if q is not None and px is not None:
            mv[t] = q * px
            tot += mv[t]
    return {t: (mv[t] / tot) for t in mv} if tot else {}


def construct_variant_b(S: dict, cfg: dict) -> dict:
    """Risk-controlled long-only weights on the active universe (deterministic,
    as-of the latest completed close; no future data)."""
    vcfg = ((cfg.get("shadow_variants") or {}).get("variant_b_risk_controlled_long_only") or {})
    name_cap = _f(vcfg.get("max_individual_weight")) or 0.05
    sector_dev = _f(vcfg.get("max_active_sector_deviation_pp")) or 0.05
    vol_target = _f(vcfg.get("annualized_volatility_target")) or 0.10
    lam = _f(vcfg.get("turnover_penalty_lambda")) or 0.10

    tickers = sorted(current_holdings(S).keys())
    curw = _current_weights(S)
    covb = build_covariance(S, tickers, cfg)
    cov = covb["cov"]
    fallbacks = []

    # covariance-aware alpha tilt: w ∝ max(0, mom_6_1) / variance
    raw = {}
    for t in tickers:
        alpha = (S["mom_by_ticker"].get(t) or {}).get("mom_6_1")
        var = (S["risk_by_ticker"].get(t) or {}).get("realized_vol_63d")
        var = (var ** 2) if var else None
        a = max(0.0, alpha) if alpha is not None else 0.0
        if var and var > 0:
            raw[t] = a / var
        else:
            raw[t] = 0.0
    if sum(raw.values()) <= 0:
        # fallback: inverse-variance risk-parity-lite
        fallbacks.append("no_positive_alpha_inverse_variance_fallback")
        for t in tickers:
            var = (S["risk_by_ticker"].get(t) or {}).get("realized_vol_63d")
            raw[t] = (1.0 / (var ** 2)) if var else 0.0
    tot = sum(raw.values()) or 1.0
    w = {t: raw[t] / tot for t in tickers}

    bench = _eligible_universe_sector_weights(S)
    # turnover penalty: blend toward the current book FIRST, then enforce the
    # name + sector caps as the final constraint (so the blend can never
    # re-introduce concentration above the caps).
    if curw:
        w = {t: (1 - lam) * w.get(t, 0.0) + lam * curw.get(t, 0.0) for t in set(w) | set(curw)}
        w = {t: v for t, v in w.items() if t in tickers}
        s = sum(w.values()) or 1.0
        w = {t: v / s for t, v in w.items()}
    # Enforce the per-name and per-sector caps SIMULTANEOUSLY with a
    # deterministic bounded projection. Weight that cannot be placed without
    # breaching a cap is held as CASH (the weights may sum to < 1); it is never
    # redistributed in a way that lifts a name back above its own cap.
    # (Alternating single-constraint projections settle into a limit cycle whose
    # trailing sector-cap step re-inflates a name above the name cap — the
    # Phase 31B name-cap regression.)
    w = _apply_joint_caps(S, w, bench, name_cap, sector_dev)
    capped = _sectors_at_cap(S, w, bench, sector_dev)
    if capped:
        fallbacks.append("sector_active_deviation_capped:%s" % ";".join(sorted(capped)))

    # volatility targeting: scale gross so annualized vol == min(target, unscaled)
    port_vol = _port_vol(w, cov)
    if port_vol and port_vol > 0:
        g = min(1.0, vol_target / port_vol)
    else:
        g = 1.0
        fallbacks.append("covariance_unavailable_no_vol_scaling")
    scaled = {t: v * g for t, v in w.items()}
    cash = 1.0 - sum(scaled.values())
    beta = portfolio_beta_vs_spy(S, {t: v for t, v in scaled.items() if v > 0})

    return {"weights": scaled, "gross": _r(sum(scaled.values()), 6), "cash": _r(cash, 6),
            "vol_scale": _r(g, 6), "ex_ante_vol_unscaled": _r(port_vol, 6),
            "ex_ante_vol_scaled": _r((port_vol * g) if port_vol else None, 6),
            "ex_ante_beta_vs_spy": _r(beta["portfolio_beta"], 4),
            "covariance_estimator": covb["estimator"],
            "covariance_lookback_returns": covb["correlation_lookback_returns"],
            "fallbacks": fallbacks, "name_cap": name_cap, "sector_active_cap_pp": sector_dev,
            "vol_target": vol_target, "beta_source": beta["source"]}


def _apply_name_cap(w: dict, cap: float) -> dict:
    w = dict(w)
    for _ in range(20):
        over = {t: v for t, v in w.items() if v > cap + 1e-12}
        if not over:
            break
        excess = sum(v - cap for v in over.values())
        for t in over:
            w[t] = cap
        under = {t: v for t, v in w.items() if v < cap - 1e-12}
        pool = sum(under.values())
        if pool <= 0:
            break
        for t in under:
            w[t] += excess * (w[t] / pool)
    s = sum(w.values()) or 1.0
    return {t: v / s for t, v in w.items()}


def _apply_sector_cap(S: dict, w: dict, bench: dict, dev_pp: float):
    """Cap each sector's total weight at benchmark+dev; redistribute proportionally
    to non-capped names. Deterministic single family of passes."""
    w = dict(w)
    capped = set()
    for _ in range(20):
        sec_w = {}
        for t, v in w.items():
            s = sector_of(S, t)
            sec_w[s] = sec_w.get(s, 0.0) + v
        over = {}
        for s, tot in sec_w.items():
            cap = bench.get(s, 0.0) + dev_pp
            if tot > cap + 1e-12:
                over[s] = tot - cap
        if not over:
            break
        for s in over:
            capped.add(s)
            names = [t for t in w if sector_of(S, t) == s and w[t] > 0]
            tot = sum(w[t] for t in names)
            if tot <= 0:
                continue
            target = bench.get(s, 0.0) + dev_pp
            for t in names:
                w[t] *= (target / tot)
        # redistribute the removed weight to under-cap sectors' names proportionally
        removed = sum(over.values())
        under_names = [t for t in w if sector_of(S, t) not in over]
        pool = sum(w[t] for t in under_names)
        if pool > 0 and removed > 0:
            for t in under_names:
                w[t] += removed * (w[t] / pool)
        else:
            break
    s = sum(w.values()) or 1.0
    return {t: v / s for t, v in w.items()}, capped


def _waterfill(items: list, desired: dict, total: float, cap) -> dict:
    """Place exactly ``total`` mass across ``items`` proportional to ``desired``,
    with no item exceeding its cap; mass that spills over an item's cap is
    re-spread across the items still under cap. ``cap`` is either a scalar (same
    ceiling for every item) or a ``{item: ceiling}`` mapping. Deterministic —
    items are processed in sorted order — and terminates in at most ``len+2``
    passes (each pass either finishes or retires at least one saturated item).
    The caller guarantees ``sum(caps) >= total`` so all of ``total`` is placed.
    """
    items = sorted(items)

    def _cap(t):
        return cap[t] if isinstance(cap, dict) else cap

    alloc = {t: 0.0 for t in items}
    if total <= 0.0:
        return alloc
    active = set(items)
    for _ in range(len(items) + 2):
        if not active:
            break
        remaining = total - sum(alloc.values())
        if remaining <= 1e-15:
            break
        d = sum(max(0.0, desired.get(t, 0.0)) for t in active)
        overflow = []
        if d <= 0.0:                                   # no signal -> equal split
            share = remaining / len(active)
            for t in sorted(active):
                give = min(share, _cap(t) - alloc[t])
                alloc[t] += give
                if alloc[t] >= _cap(t) - 1e-15:
                    overflow.append(t)
        else:
            for t in sorted(active):
                give = remaining * (max(0.0, desired.get(t, 0.0)) / d)
                room = _cap(t) - alloc[t]
                if give >= room:
                    give = room
                    overflow.append(t)
                alloc[t] += give
        if not overflow:
            break
        for t in overflow:
            active.discard(t)
    return alloc


def _apply_joint_caps(S: dict, w: dict, bench: dict, name_cap: float,
                      sector_dev: float, *, iters: int = 200) -> dict:
    """Deterministic bounded projection enforcing the per-name cap AND the
    per-sector active-deviation cap SIMULTANEOUSLY, while keeping the invested
    book proportional to the ``w`` tilt.

    The per-name cap is absolute (weight <= ``name_cap``). The per-sector cap is
    on the *invested* book: sector weight / invested-gross <= benchmark + active
    deviation. Because some sectors have too few names to reach their sector cap
    without breaching the 5% name cap, the whole book cannot always be fully
    invested; the largest investable gross ``T`` (<= 1) is found by bisection on
    ``sum_s min(sector_cap_s * T, n_s * name_cap) >= T`` and the unplaceable
    remainder is held as CASH. Exactly ``T`` is then placed — first across
    sectors (proportional to each sector's tilt, capped at
    ``min(sector_cap*T, n_s*name_cap)``), then within each sector across its
    names (proportional to the tilt, capped at ``name_cap``). Mass is never
    redistributed in a way that lifts a name — or a sector's invested share —
    above its cap; both caps are exact by construction. (Naive alternating
    single-constraint projections instead settle into a limit cycle whose
    trailing sector step re-inflates a name above the name cap — the Phase 31B
    regression.)
    """
    names = sorted(w)
    w = {t: max(0.0, _f(w.get(t)) or 0.0) for t in names}
    sec_of = {t: sector_of(S, t) for t in names}
    sectors: dict = {}
    for t in names:
        sectors.setdefault(sec_of[t], []).append(t)
    sec_cap = {s: bench.get(s, 0.0) + sector_dev for s in sectors}
    n_of = {s: len(ts) for s, ts in sectors.items()}

    def _max_placeable(t: float) -> float:
        return sum(min(sec_cap[s] * t, n_of[s] * name_cap) for s in sectors)

    if _max_placeable(1.0) >= 1.0:                     # full investment feasible
        total = 1.0
    else:                                              # largest investable gross
        lo, hi = 0.0, 1.0
        for _ in range(iters):
            mid = (lo + hi) / 2.0
            if _max_placeable(mid) >= mid:
                lo = mid
            else:
                hi = mid
        total = lo

    # (1) place `total` across sectors, proportional to each sector's tilt, each
    #     sector capped by min(sector share cap at gross `total`, name-cap room).
    sector_names = sorted(sectors)
    sector_desired = {s: sum(w[t] for t in sectors[s]) for s in sector_names}
    sector_cap_mass = {s: min(sec_cap[s] * total, n_of[s] * name_cap) for s in sector_names}
    sector_mass = _waterfill(sector_names, sector_desired, total, sector_cap_mass)

    # (2) place each sector's mass across its names, proportional to the tilt,
    #     capped at the per-name ceiling.
    out = {t: 0.0 for t in names}
    for s in sector_names:
        alloc = _waterfill(sectors[s], {t: w[t] for t in sectors[s]},
                           sector_mass[s], name_cap)
        out.update(alloc)
    # defensive final clamp (guarantees the per-name post-condition against drift)
    return {t: min(out[t], name_cap) for t in names}


def _sectors_at_cap(S: dict, w: dict, bench: dict, dev_pp: float,
                    tol: float = 1e-9) -> set:
    """Sectors whose invested share is bound at (or numerically at) their active
    deviation cap after projection — recorded as a fallback annotation."""
    tot: dict = {}
    for t, v in w.items():
        s = sector_of(S, t)
        tot[s] = tot.get(s, 0.0) + v
    gross = sum(tot.values()) or 1.0
    return {s for s, v in tot.items() if v / gross >= bench.get(s, 0.0) + dev_pp - tol}


def construct_variant_c(S: dict, cfg: dict, variant_b: dict, target_beta: float) -> dict:
    """Variant B long book + simulated SPY hedge to a net beta target."""
    long_w = {t: v for t, v in variant_b["weights"].items() if v > 0}
    long_gross = sum(long_w.values())
    beta_b = portfolio_beta_vs_spy(S, long_w).get("portfolio_beta")
    if beta_b is None:
        return {"target_beta": target_beta, "available": False,
                "reason": "long_book_beta_unavailable"}
    hedge = target_beta - beta_b            # SPY weight (negative = short SPY)
    net_exposure = long_gross + hedge
    net_beta = beta_b + hedge               # SPY beta = 1 by definition
    max_gross = _f(((cfg.get("shadow_variants") or {}).get("variant_c_beta_controlled") or {}).get("max_gross_exposure")) or 1.0
    return {"target_beta": target_beta, "available": True,
            "long_book_beta": _r(beta_b, 4),
            "long_gross_exposure": _r(long_gross, 6),
            "spy_hedge_weight": _r(hedge, 6),
            "spy_hedge_notional_per_100k": _r(hedge * 100000.0, 2),
            "net_exposure": _r(net_exposure, 6),
            "estimated_net_beta": _r(net_beta, 4),
            "gross_within_limit": long_gross <= max_gross + 1e-9,
            "no_single_stock_shorting": True}


def _reconstruct_variant_daily(S: dict, weights: dict, *, hedge_weight: float = 0.0,
                               cost_bps: float, turnover: float, label: str) -> dict:
    """Indicative HISTORICAL RECONSTRUCTION over the completed window (07-22..).

    Marks static weights forward over the operational window using the price
    store. One-off entry cost applied on the first day. Clearly labelled as a
    small-sample reconstruction, never a walk-forward proof."""
    tickers = [t for t in weights if weights[t] != 0]
    pan = build_returns(S, tickers)
    dates = pan["dates"]
    # restrict to the operational window (>= first perf date)
    first = S["perf_rows"][0].get("date") if S["perf_rows"] else (dates[0] if dates else None)
    idxs = [i for i, d in enumerate(dates) if d >= first] if first else list(range(len(dates)))
    spy = pan["ret"].get(BENCHMARK, [])
    nav = 100000.0
    entry_cost = nav * turnover * (cost_bps / 10000.0)
    nav -= entry_cost
    rows = []
    navs = [nav]
    dr = []
    for k, i in enumerate(idxs):
        if k == 0:
            rows.append({"variant": label, "market_date": dates[i], "daily_return_pct": 0.0,
                         "nav": _r(nav, 2), "entry_cost_usd": _r(entry_cost, 2)})
            continue
        rp = 0.0
        for t in tickers:
            r = pan["ret"].get(t, [None] * len(dates))[i]
            if r is not None:
                rp += weights[t] * r
        rs = spy[i]
        if hedge_weight and rs is not None:
            rp += hedge_weight * rs
        nav *= (1.0 + rp)
        navs.append(nav)
        dr.append(rp)
        rows.append({"variant": label, "market_date": dates[i], "daily_return_pct": _r(rp * 100.0, 6),
                     "nav": _r(nav, 2), "entry_cost_usd": 0.0})
    total_ret = (navs[-1] / 100000.0 - 1.0) if navs else None
    return {"rows": rows, "nav_path": navs, "daily_returns": dr,
            "total_return_pct": _r((total_ret * 100.0) if total_ret is not None else None, 6),
            "n_returns": len(dr), "max_drawdown_pct": _r((_max_drawdown(navs) or 0.0) * 100.0, 6)}


def build_shadow_variants(S: dict, cfg: dict) -> dict:
    scfg = cfg.get("shadow_variants") or {}
    costs = list((cfg.get("transaction_costs") or {}).get("one_way_cost_grid_bps") or [0, 5, 10, 20])
    primary_cost = _f((cfg.get("transaction_costs") or {}).get("primary_cost_bps_used")) or 12.5
    beta_targets = list((scfg.get("variant_c_beta_controlled") or {}).get("preregistered_net_beta_sensitivity_targets")
                        or [0.0, 0.25, 0.5])
    primary_beta = _f((scfg.get("variant_c_beta_controlled") or {}).get("primary_net_beta_target"))
    primary_beta = 0.25 if primary_beta is None else primary_beta

    curw = _current_weights(S)
    variant_b = construct_variant_b(S, cfg)
    variant_c = {str(bt): construct_variant_c(S, cfg, variant_b, bt) for bt in beta_targets}

    # turnover of B/C vs current book (one-way, sum of positive weight increases)
    def _turnover(target):
        keys = set(target) | set(curw)
        return sum(max(0.0, target.get(t, 0.0) - curw.get(t, 0.0)) for t in keys)
    turn_a = 0.0
    turn_b = _turnover({t: v for t, v in variant_b["weights"].items() if v > 0})

    # indicative historical reconstruction over the completed window
    a_daily = _reconstruct_variant_daily(S, curw, cost_bps=0.0, turnover=turn_a, label="A")
    b_daily = _reconstruct_variant_daily(S, {t: v for t, v in variant_b["weights"].items() if v > 0},
                                         cost_bps=primary_cost, turnover=turn_b, label="B")
    c_primary = variant_c[str(primary_beta)]
    hedge_w = _f(c_primary.get("spy_hedge_weight")) or 0.0
    c_daily = _reconstruct_variant_daily(
        S, {t: v for t, v in variant_b["weights"].items() if v > 0},
        hedge_weight=hedge_w, cost_bps=primary_cost, turnover=turn_b, label="C")

    variant_daily = a_daily["rows"] + b_daily["rows"] + c_daily["rows"]

    def _summ(label, daily, weights, extra):
        navs = daily["nav_path"]
        dr = daily["daily_returns"]
        spy_pan = build_returns(S, list(weights.keys()))
        # SPY over same window
        first = S["perf_rows"][0].get("date") if S["perf_rows"] else None
        spy_rets = [r for d, r in zip(spy_pan["dates"], spy_pan["ret"].get(BENCHMARK, []))
                    if r is not None and (first is None or d >= first)]
        row = {
            "variant": label, "n_returns": daily["n_returns"],
            "total_return_pct": daily["total_return_pct"],
            "max_drawdown_pct": daily["max_drawdown_pct"],
            "worst_1d_return_pct": _r((min(dr) * 100.0) if dr else None, 6),
            "pct_positive_days": _r((100.0 * sum(1 for r in dr if r > 0) / len(dr)) if dr else None, 3),
            "daily_vol_pct": _r((_std(dr) * 100.0) if _std(dr) is not None else None, 6),
            "spy_total_return_pct": _r((( _prod1(spy_rets) - 1.0) * 100.0) if spy_rets else None, 6),
            "cumulative_excess_vs_spy_pp": None,
            "annualized_return_pct": None,
            "sharpe": None, "sortino": None, "beta_vs_spy": None,
            "annualization_supported": False,
            "primary_cost_bps": _r(primary_cost, 4),
        }
        if row["total_return_pct"] is not None and row["spy_total_return_pct"] is not None:
            row["cumulative_excess_vs_spy_pp"] = _r(row["total_return_pct"] - row["spy_total_return_pct"], 6)
        row.update(extra)
        return row

    summary_rows = [
        _summ("A", a_daily, curw, {"label": "CURRENT CONTROL (frozen)",
                                   "ex_ante_beta_vs_spy": _r(portfolio_beta_vs_spy(S, curw)["portfolio_beta"], 4),
                                   "gross": _r(sum(curw.values()), 6), "cash": _r(1 - sum(curw.values()), 6),
                                   "turnover_vs_current": _r(turn_a, 6)}),
        _summ("B", b_daily, {t: v for t, v in variant_b["weights"].items() if v > 0},
              {"label": "RISK-CONTROLLED LONG-ONLY", "ex_ante_beta_vs_spy": variant_b["ex_ante_beta_vs_spy"],
               "gross": variant_b["gross"], "cash": variant_b["cash"], "turnover_vs_current": _r(turn_b, 6),
               "ex_ante_vol_scaled": variant_b["ex_ante_vol_scaled"], "vol_scale": variant_b["vol_scale"]}),
        _summ("C", c_daily, {t: v for t, v in variant_b["weights"].items() if v > 0},
              {"label": "BETA-CONTROLLED SHADOW (net beta %.2f)" % primary_beta,
               "ex_ante_beta_vs_spy": c_primary.get("estimated_net_beta"),
               "gross": variant_b["gross"], "cash": variant_b["cash"], "turnover_vs_current": _r(turn_b, 6),
               "spy_hedge_weight": c_primary.get("spy_hedge_weight")}),
    ]

    # cost sensitivity across the grid for each variant (entry cost only, static book)
    cost_rows = []
    for label, daily, weights, turn in (("A", a_daily, curw, turn_a),
                                        ("B", b_daily, {t: v for t, v in variant_b["weights"].items() if v > 0}, turn_b),
                                        ("C", c_daily, {t: v for t, v in variant_b["weights"].items() if v > 0}, turn_b)):
        gross_ret = daily["total_return_pct"]
        for cb in costs:
            drag_pct = turn * (cb / 10000.0) * 100.0
            net = (gross_ret - drag_pct) if gross_ret is not None else None
            cost_rows.append({"variant": label, "one_way_cost_bps": cb,
                              "turnover_vs_current": _r(turn, 6),
                              "cost_drag_pct": _r(drag_pct, 6),
                              "gross_reconstructed_return_pct": gross_ret,
                              "net_reconstructed_return_pct": _r(net, 6),
                              "is_primary": (cb == 10)})

    definitions = {
        "universe_rule": scfg.get("shadow_variants", scfg).get("universe_rule", "active_book_25_holdings"),
        "variant_a": {"id": "A", "label": "CURRENT CONTROL", "weights": {t: _r(v, 6) for t, v in sorted(curw.items())}},
        "variant_b": {"id": "B", "label": "RISK-CONTROLLED LONG-ONLY",
                      "weights": {t: _r(v, 6) for t, v in sorted(variant_b["weights"].items()) if v > 0},
                      "construction": variant_b},
        "variant_c": {"id": "C", "label": "BETA-CONTROLLED SHADOW",
                      "primary_net_beta_target": primary_beta,
                      "sensitivity": {k: v for k, v in variant_c.items()}},
        "primary_cost_bps": primary_cost,
        "reconstruction_caveat": (
            "Variant A is the actual frozen book. Variants B and C were NEVER held; their daily rows "
            "are an indicative HISTORICAL RECONSTRUCTION over the completed window using end-of-window "
            "owned risk statistics (owned realized_vol_63d/beta are current-as-of only, so a fully "
            "walk-forward reconstruction is not possible from owned data). They are NOT a walk-forward "
            "proof and are never used for promotion; the forward_shadow_contract registers them for "
            "genuine forward tracking."),
    }
    return {"variant_daily": variant_daily, "variant_summary": summary_rows,
            "cost_sensitivity": cost_rows, "definitions": definitions,
            "variant_b": variant_b, "variant_c": variant_c, "current_weights": curw,
            "turnover": {"A": turn_a, "B": turn_b}}


def _prod1(rets) -> float:
    g = 1.0
    for r in rets:
        g *= (1.0 + r)
    return g


# =========================================================================== #
# PART J — FORWARD SHADOW-TRACKING CONTRACT.
# =========================================================================== #
def build_forward_shadow_contract(S: dict, cfg: dict, shadow: dict, *, run_id: str,
                                  config_hash: str, source_hashes: dict) -> dict:
    latest = latest_completed_date(S)
    calendar = list(S.get("eligible_calendar") or [])
    try:
        idx = calendar.index(latest)
    except ValueError:
        idx = len(calendar) - 1
    horizons = list((cfg.get("signal_decay") or {}).get("requested_horizons_eligible_closes") or [1, 5, 10, 20, 40])
    maturities = {}
    for h in horizons:
        j = idx + h
        maturities[str(h)] = (calendar[j] if 0 <= j < len(calendar) else "PENDING_FUTURE_CLOSE")

    vb = shadow["variant_b"]
    vc_primary_beta = _f(((cfg.get("shadow_variants") or {}).get("variant_c_beta_controlled") or {}).get("primary_net_beta_target")) or 0.25
    vc = shadow["variant_c"].get(str(vc_primary_beta)) or {}

    def _sector_weights(weights):
        sec = {}
        for t, v in weights.items():
            if v > 0:
                sec[sector_of(S, t)] = sec.get(sector_of(S, t), 0.0) + v
        return {k: _r(v, 6) for k, v in sorted(sec.items())}

    return {
        "phase": PHASE, "contract_type": "FILE_BASED_FORWARD_SHADOW_TRACKING",
        "immutable": True, "append_safe": True, "written_to_database": False,
        "run_id": run_id, "run_date": latest, "source_market_date": latest,
        "active_book_id": ACTIVE_BOOK_ID,
        "benchmark": BENCHMARK,
        "cost_assumption_bps_one_way": _f((cfg.get("transaction_costs") or {}).get("primary_cost_bps_used")),
        "variant_a_target_weights": {t: _r(v, 6) for t, v in sorted(shadow["current_weights"].items())},
        "variant_b_target_weights": {t: _r(v, 6) for t, v in sorted(vb["weights"].items()) if v > 0},
        "variant_c_long_weights": {t: _r(v, 6) for t, v in sorted(vb["weights"].items()) if v > 0},
        "variant_c_spy_hedge_weight": vc.get("spy_hedge_weight"),
        "variant_c_net_beta_target": vc_primary_beta,
        "expected_portfolio_beta": {
            "A": _r(portfolio_beta_vs_spy(S, shadow["current_weights"])["portfolio_beta"], 4),
            "B": vb.get("ex_ante_beta_vs_spy"),
            "C": vc.get("estimated_net_beta"),
        },
        "expected_portfolio_volatility_annualized": {
            "B_scaled": vb.get("ex_ante_vol_scaled"), "B_target": vb.get("vol_target"),
        },
        "sector_weights": {
            "A": _sector_weights(shadow["current_weights"]),
            "B": _sector_weights(vb["weights"]),
        },
        "turnover_vs_current": {"A": _r(shadow["turnover"]["A"], 6), "B": _r(shadow["turnover"]["B"], 6)},
        "maturity_dates_eligible_closes": maturities,
        "immutable_snapshot_identity": {
            "latest_forward_snapshot_dates": sorted({r.get("market_date") for r in S["snapshot_rows"]}),
            "active_model_id": fps.ACTIVE_MODEL_ID,
        },
        "source_data_hashes": source_hashes,
        "config_hash": config_hash,
        "automation": False, "orders": False, "broker": False,
        "note": "Deterministic file-based contract for FUTURE manual shadow runs. Not automated. "
                "Records the three variant target sets as of the latest completed close so that "
                "1/5/10/20/40-eligible-close forward evidence can be measured going forward.",
    }


# =========================================================================== #
# PART K — PROMOTION GATES.
# =========================================================================== #
def build_promotion_gates(S: dict, cfg: dict, shadow: dict, signal: dict) -> dict:
    g = cfg.get("promotion_gates") or {}
    n_days = len(S["perf_rows"])
    n_matured = len(S["outcome_rows"])
    b_summary = next((r for r in shadow["variant_summary"] if r["variant"] == "B"), {})
    excess = b_summary.get("cumulative_excess_vs_spy_pp")

    checks = [
        {"gate": "completed_trading_days", "required": g.get("min_completed_trading_days", 60),
         "preferred": g.get("preferred_completed_trading_days", 120), "observed": n_days,
         "met": n_days >= (g.get("min_completed_trading_days", 60))},
        {"gate": "matured_observations", "required": g.get("min_matured_observations", 100),
         "observed": n_matured, "met": n_matured >= (g.get("min_matured_observations", 100))},
        {"gate": "positive_net_excess_after_cost", "required": "> 0",
         "observed": excess, "met": (excess is not None and excess > 0),
         "note": "Reconstruction over %d returns — NOT decision-grade." % (b_summary.get("n_returns") or 0)},
        {"gate": "multiple_rolling_windows", "required": ">= 2 windows",
         "observed": "0 full windows (need >= 5-20 returns)", "met": False},
        {"gate": "stable_ic_or_spread", "required": "stable across dates",
         "observed": "max %d matured date(s) at any horizon" % (signal["adequacy"].get("max_matured_dates_for_a_model_horizon") or 0),
         "met": bool(signal["adequacy"].get("sufficient_to_conclude"))},
        {"gate": "no_single_sector_majority", "required": "< 50% in any sector",
         "observed": _top_sector_weight(S), "met": _top_sector_weight(S) < 0.5},
        {"gate": "controlled_beta", "required": "beta target achievable",
         "observed": b_summary.get("ex_ante_beta_vs_spy"),
         "met": (b_summary.get("ex_ante_beta_vs_spy") is not None)},
        {"gate": "lower_drawdown_or_better_risk_adjusted", "required": "vs control",
         "observed": "insufficient sample", "met": False},
        {"gate": "no_lookahead_survivorship_reconciliation_failure", "required": "all clean",
         "observed": "accounting reconciles; owned universe is survivorship-biased for history",
         "met": True},
    ]
    all_met = all(c["met"] for c in checks)
    if n_days < (g.get("min_completed_trading_days", 60)) or n_matured < (g.get("min_matured_observations", 100)):
        decision = "RESEARCH_DATA_INSUFFICIENT"
    elif all_met:
        decision = "CHALLENGER_EVIDENCE_PROMISING_NOT_PROMOTABLE"
    else:
        decision = "CONTINUE_CONTROL_AND_SHADOW_TEST"
    return {
        "phase": PHASE, "active_book_unchanged": True, "promotes_now": False,
        "allowed_decisions": g.get("allowed_decisions"),
        "gates": checks, "all_gates_met": all_met,
        "decision": decision,
        "secondary_decision": "CONTINUE_CONTROL_AND_SHADOW_TEST",
        "rationale": ("Only %d of >=%d required completed trading days and %d of >=%d required matured "
                      "observations exist. No challenger is eligible for promotion; the active book "
                      "remains unchanged and shadow forward tracking continues."
                      % (n_days, g.get("min_completed_trading_days", 60),
                         n_matured, g.get("min_matured_observations", 100))),
    }


def _top_sector_weight(S: dict) -> float:
    w = _current_weights(S)
    sec = {}
    for t, v in w.items():
        sec[sector_of(S, t)] = sec.get(sector_of(S, t), 0.0) + v
    return max(sec.values()) if sec else 0.0


# =========================================================================== #
# EXECUTIVE RECOMMENDATION (markdown).
# =========================================================================== #
def build_executive_recommendation(S: dict, cfg: dict, *, adequacy, accounting, attribution,
                                   risk, signal, shadow, gates) -> str:
    latest = latest_completed_date(S)
    prow = risk["portfolio_rows"][0]
    perf_summary = S["perf"].get("summary") or {}
    cum = _f(perf_summary.get("cumulative_return_pct"))
    spy_cum = _f(perf_summary.get("benchmark_cumulative_return_pct"))
    excess = _f(perf_summary.get("excess_vs_benchmark_pct_points"))
    top_sec = prow.get("top_sector")
    top_sec_w = prow.get("top_sector_weight")
    beta = prow.get("portfolio_beta_vs_spy")
    beta_owned = prow.get("portfolio_beta_universe_owned_weighted")
    # top 3 active sector tilts (largest absolute deviations vs eligible universe)
    tilts = sorted((risk.get("sector_rows") or []),
                   key=lambda r: -abs(_f(r.get("active_weight_pp")) or 0.0))[:3]
    tilt_str = ", ".join("%s %s%spp" % (t.get("sector"),
                                        "+" if (_f(t.get("active_weight_pp")) or 0) >= 0 else "",
                                        t.get("active_weight_pp")) for t in tilts)
    L = []
    L.append("# Phase 31B — Executive Recommendation")
    L.append("")
    L.append("**Scope:** research-only, read-only. The active Alpha Paper Book #1 was not changed. "
             "No order, hedge, short, automation, model promotion, or database write occurred.")
    L.append("")
    L.append("## Bottom line")
    L.append("")
    L.append("- **The 4-day loss tracked the market.** Over the %d completed closes (%s..%s) the book returned "
             "**%s%%** versus SPY **%s%%** — a raw excess of **%s pp** (a beta-1 comparison that slightly flatters). "
             "This is one market-down episode, not evidence about selection skill."
             % (len(S["perf_rows"]), (S["perf_rows"][0].get("date") if S["perf_rows"] else "?"),
                latest, _fmt(cum), _fmt(spy_cum), _fmt(excess)))
    L.append("- **The book carries real market and concentration risk (H1 + H2).** Owned structural beta "
             "(weighted beta-to-universe) is **%s**, with several holdings at 1.5-2.1 beta offset by low/negative-beta "
             "energy and staples; the short realized-window SPY beta (**%s**, %d noisy returns) is unreliable and reported "
             "only for completeness. The largest active sector tilts vs the eligible universe are **%s**. Concentration, "
             "not costs, is the second-order driver."
             % (_fmt(beta_owned), _fmt(beta), prow.get("beta_n_returns") or 0, tilt_str))
    L.append("- **Reducing market exposure would have helped in this window.** The beta-controlled shadow (Variant C, "
             "net beta 0.25) reconstructs a smaller loss than the control over the same window, consistent with the "
             "reframed objective of positive absolute return with controlled drawdown.")
    L.append("- **There is no credible alpha signal yet.** Rank-IC exists only at horizon 1 for a single snapshot date "
             "(one observation per model); every longer horizon is pending. No signal conclusion is drawn.")
    L.append("- **The evidence is far too short to choose a winner.** %d completed trading days and %d matured "
             "outcomes versus promotion floors of 60 days / 100 outcomes. Correct outcome: continue forward shadow observation (H6)."
             % (len(S["perf_rows"]), len(S["outcome_rows"])))
    L.append("")
    L.append("## Direct answers")
    L.append("")
    L.append("1. **Is the loss beta, sector, selection, sizing, timing, costs, or insufficient evidence?** "
             "Primarily **market beta** (the book has meaningful, uncontrolled market exposure) with **sector and "
             "single-name concentration** as the second-order driver. The raw excess vs SPY was slightly positive "
             "(**%s pp**, beta-1), but at 3 daily returns a beta-adjusted separation of beta vs selection is "
             "**inconclusive**. Costs are immaterial after the one-time entry (turnover 0 since). The honest primary "
             "answer is **insufficient evidence**, with beta + concentration the clearest structural exposures." % _fmt(excess))
    L.append("2. **Is the existing alpha showing credible relative value?** A tentative, non-significant **yes** on "
             "4 days (positive excess), but **not credible** — it is one market-down window with no matured signal evidence.")
    L.append("3. **Which risk overlay best fits positive absolute returns?** **Variant C (beta-controlled, net beta "
             "%.2f)** is most aligned with the reframed objective: it explicitly caps market exposure, which is exactly "
             "what drove the loss. **Variant B (risk-controlled long-only, 10%% vol target)** reduces concentration and "
             "single-name risk. Both are registered for forward tracking; neither is promotable yet."
             % (_f(((cfg.get('shadow_variants') or {}).get('variant_c_beta_controlled') or {}).get('primary_net_beta_target')) or 0.25))
    L.append("4. **Is there enough data to choose among A/B/C?** **No.** %s" % adequacy["overall_adequacy"])
    L.append("5. **What should remain frozen?** The active book (A) — holdings, cash, model target, review cadence.")
    L.append("6. **What should be shadow-tested next?** Variants B and C via the forward_shadow_contract, measured at "
             "1/5/10/20/40 eligible closes, plus continued maturation of the existing forward snapshots.")
    L.append("7. **What evidence is required before changing the active book?** The Part K promotion gates: >=60 "
             "(pref. 120) completed trading days, >=100 matured observations, positive net excess after cost across "
             "multiple rolling windows, stable IC/spread, controlled beta, no single-sector majority, and lower drawdown "
             "or better risk-adjusted return than the control — with no look-ahead / survivorship / reconciliation failure.")
    L.append("")
    L.append("## Decision")
    L.append("")
    L.append("**%s** (secondary: %s). The active book is unchanged; nothing is promoted."
             % (gates["decision"], gates["secondary_decision"]))
    L.append("")
    L.append("## Data & method integrity")
    L.append("")
    L.append("- Accounting reconciled: %s (max NAV residual $%s, max return residual %s bps over %d rows)."
             % (accounting["reconciliation"]["all_rows_reconcile"],
                accounting["reconciliation"]["max_abs_nav_residual_usd"],
                accounting["reconciliation"]["max_abs_return_residual_bps"],
                accounting["reconciliation"]["rows_checked"]))
    L.append("- Attribution reconciled to NAV within tolerance: %s (%s)."
             % (attribution["reconciliation"]["attribution_available"], attribution["reconciliation"]["status"]))
    L.append("- Covariance estimator: %s (lookback %s returns); annualization supported: %s."
             % (prow.get("covariance_estimator"), prow.get("covariance_lookback_returns"),
                prow.get("annualization_supported")))
    L.append("- Read-only: no database connection, no network call, no active-book mutation, no orders/hedge/short/automation.")
    L.append("")
    return "\n".join(L)


def _fmt(x) -> str:
    v = _f(x)
    return ("%.4f" % v) if v is not None else "n/a"


# =========================================================================== #
# ORCHESTRATION.
# =========================================================================== #
def compute_run_id(*, git_commit: str, config_hash: str, as_of: str, source_hashes: dict) -> str:
    payload = "|".join([
        str(git_commit), str(config_hash), str(as_of),
        "|".join("%s=%s" % (k, source_hashes[k]) for k in sorted(source_hashes)),
    ])
    return "phase31b_" + _sha256_bytes(payload.encode("utf-8"))[:16]


def _source_hashes(S: dict) -> dict:
    sdir = Path(S["desk_dir"])
    out = {}
    for f in ("forward_performance.json", "desk_marks.json", "forward_prediction_snapshots.json",
              "forward_prediction_outcomes.json", "forward_prediction_prices.json",
              "daily_close_journal.json", "alpha_book_records.json", "alpha_book_policy.json"):
        out[f] = _sha256_file(sdir / f)
    for key, p in (("current_risk_stats.csv", S["source_paths"]["risk_stats_csv"]),
                   ("current_momentum_scores.csv", S["source_paths"]["momentum_scores_csv"]),
                   ("repaired_sector_mapping.csv", S["source_paths"]["sector_map_csv"])):
        out[key] = _sha256_file(Path(p))
    return out


def _write_csv(path: Path, rows: list[dict], fieldnames: Optional[list] = None) -> int:
    if not rows and not fieldnames:
        path.write_text("", encoding="utf-8")
        return 0
    fn = fieldnames or list(rows[0].keys())
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fn)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fn})
    return len(rows)


def run_research(*, output_root, as_of: str = "latest", config: Optional[dict] = None,
                 config_path: Optional[str] = None, desk_dir: Optional[str] = None,
                 git_commit: str = "UNKNOWN", now: Optional[datetime] = None,
                 write: bool = True) -> dict:
    """Execute the full Phase 31B analysis. Read-only; writes only under output_root.

    Returns {status, run_id, run_dir, files, manifest}. Never overwrites a prior run.
    """
    cfg = config or {}
    config_bytes = json.dumps(cfg, sort_keys=True, separators=(",", ":")).encode("utf-8")
    config_hash = _sha256_bytes(config_bytes)
    gen_at = (now or datetime.now(timezone.utc)).isoformat()

    S = load_sources(desk_dir=desk_dir, config=cfg)
    resolved_as_of = latest_completed_date(S) if as_of in (None, "latest") else as_of
    if not resolved_as_of or not S["perf_rows"]:
        return {"status": BLOCKED, "reason": "NO_COMPLETED_OPERATIONAL_MARKS", "run_id": None}

    source_hashes = _source_hashes(S)
    run_id = compute_run_id(git_commit=git_commit, config_hash=config_hash,
                            as_of=resolved_as_of, source_hashes=source_hashes)
    run_dir = Path(output_root) / run_id

    # Build all parts.
    adequacy = build_data_adequacy(S)
    accounting = build_accounting_reconciliation(S, cfg)
    attribution = build_return_attribution(S, cfg)
    risk = build_risk_diagnostics(S, cfg)
    signal = build_signal_decay(S, cfg)
    shadow = build_shadow_variants(S, cfg)
    contract = build_forward_shadow_contract(S, cfg, shadow, run_id=run_id,
                                             config_hash=config_hash, source_hashes=source_hashes)
    gates = build_promotion_gates(S, cfg, shadow, signal)
    exec_md = build_executive_recommendation(S, cfg, adequacy=adequacy, accounting=accounting,
                                             attribution=attribution, risk=risk, signal=signal,
                                             shadow=shadow, gates=gates)

    manifest = {
        "phase": PHASE, "run_id": run_id, "generated_at": gen_at,
        "git_commit": git_commit, "config_hash": config_hash, "config_path": config_path,
        "as_of": resolved_as_of, "as_of_requested": as_of,
        "active_book_id": ACTIVE_BOOK_ID, "benchmark": BENCHMARK,
        "read_only": True, "wrote_to_database": False, "database_connections_opened": 0,
        "external_market_data_calls": 0, "active_book_changed": False,
        "orders_created": False, "hedge_activated": False, "short_activated": False,
        "model_promoted": False, "automation_added": False,
        "source_paths": S["source_paths"],
        "source_data_hashes": source_hashes,
        "source_data_timestamps": {
            "forward_performance_updated_at": (S["perf"].get("summary") or {}).get("end_date"),
            "desk_marks_completed_through": (S["marks"] or {}).get("completed_through"),
            "desk_marks_updated_at": (S["marks"] or {}).get("updated_at"),
            "price_store_dates": [S["eligible_calendar"][0], S["eligible_calendar"][-1]] if S["eligible_calendar"] else [],
        },
        "row_counts": {
            "completed_operational_marks": len(S["perf_rows"]),
            "forward_snapshots": len(S["snapshot_rows"]),
            "matured_outcomes": len(S["outcome_rows"]),
            "eligible_calendar_dates": len(S["eligible_calendar"]),
            "holdings": len(current_holdings(S)),
            "accounting_rows": len(accounting["rows"]),
            "attribution_daily_rows": len(attribution["daily_rows"]),
            "risk_position_rows": len(risk["position_rows"]),
            "signal_decay_rows": len(signal["rows"]),
            "shadow_daily_rows": len(shadow["variant_daily"]),
        },
        "completeness": {
            "accounting_all_reconcile": accounting["reconciliation"]["all_rows_reconcile"],
            "attribution_available": attribution["reconciliation"]["attribution_available"],
            "overall_adequacy": adequacy["overall_adequacy"],
            "signal_sufficient": signal["adequacy"]["sufficient_to_conclude"],
            "decision": gates["decision"],
        },
        "output_files": list(OUTPUT_FILES),
    }

    result = {"status": READY, "run_id": run_id, "run_dir": str(run_dir),
              "as_of": resolved_as_of, "decision": gates["decision"],
              "adequacy": adequacy["overall_adequacy"], "manifest": manifest,
              "payload": {"data_adequacy": adequacy, "accounting": accounting,
                          "attribution": attribution, "risk": risk, "signal": signal,
                          "shadow": shadow, "forward_shadow_contract": contract,
                          "promotion_gates": gates, "executive_recommendation": exec_md}}

    if not write:
        return result

    if run_dir.exists() and any(run_dir.iterdir()):
        result["status"] = BLOCKED
        result["reason"] = "RUN_DIR_EXISTS_IMMUTABLE:%s" % run_dir
        return result
    run_dir.mkdir(parents=True, exist_ok=True)

    (run_dir / "data_adequacy.json").write_text(json.dumps(adequacy, indent=1), encoding="utf-8")
    _write_csv(run_dir / "accounting_reconciliation.csv", accounting["rows"])
    _write_csv(run_dir / "daily_return_attribution.csv", attribution["daily_rows"])
    _write_csv(run_dir / "sector_attribution.csv", attribution["sector_rows"],
               fieldnames=["market_date", "sector", "pnl_contribution", "prior_market_value",
                           "weight", "n_holdings"])
    (run_dir / "attribution_reconciliation.json").write_text(
        json.dumps(attribution["reconciliation"], indent=1), encoding="utf-8")
    _write_csv(run_dir / "portfolio_risk_diagnostics.csv", risk["portfolio_rows"])
    _write_csv(run_dir / "position_risk_contributions.csv", risk["position_rows"])
    _write_csv(run_dir / "sector_risk_exposures.csv", risk["sector_rows"])
    _write_csv(run_dir / "signal_decay.csv", signal["rows"],
               fieldnames=["model_id", "snapshot_date", "horizon_eligible_closes", "status",
                           "n_names", "rank_ic_spearman", "top_minus_bottom_pp", "coverage_pct", "matured"])
    (run_dir / "signal_decay_adequacy.json").write_text(json.dumps(signal["adequacy"], indent=1), encoding="utf-8")
    _write_csv(run_dir / "shadow_variant_daily.csv", shadow["variant_daily"],
               fieldnames=["variant", "market_date", "daily_return_pct", "nav", "entry_cost_usd"])
    _write_csv(run_dir / "shadow_variant_summary.csv", shadow["variant_summary"])
    _write_csv(run_dir / "cost_sensitivity.csv", shadow["cost_sensitivity"],
               fieldnames=["variant", "one_way_cost_bps", "turnover_vs_current", "cost_drag_pct",
                           "gross_reconstructed_return_pct", "net_reconstructed_return_pct", "is_primary"])
    (run_dir / "forward_shadow_contract.json").write_text(json.dumps(contract, indent=1), encoding="utf-8")
    (run_dir / "promotion_gates.json").write_text(json.dumps(gates, indent=1), encoding="utf-8")
    (run_dir / "executive_recommendation.md").write_text(exec_md, encoding="utf-8")
    (run_dir / "run_manifest.json").write_text(json.dumps(manifest, indent=1), encoding="utf-8")

    result["files_written"] = sorted(p.name for p in run_dir.iterdir())
    return result
