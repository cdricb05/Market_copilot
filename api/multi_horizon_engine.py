"""api/multi_horizon_engine.py - Phase 25 current input pipeline + scores + books + state + recs.

Pure stdlib compute layer for the multi-horizon paper platform (Track A A2-A7).  It reads owned local
CSVs only and never touches the network, PostgreSQL, the prediction service, or any order/trade workflow.

Inputs (all owned, local):
  * Frozen Phase 10-L sector-neutral scored panel CSV  -> composite_sn current cross-section (fundamental leg)
  * current_momentum_scores.csv  (emitted by research/phase25_multi_horizon_inputs)  -> mom_6_1 leg
  * current_risk_stats.csv       (same emitter)                                       -> risk diagnostics
  * momentum_monthly_panel.csv   (same emitter)                                       -> used by history module

Outputs (in-memory dicts; the platform layer serves them and the builder persists the operational cache):
  A2 input manifest (validation + fingerprints), A3 per-security model scores, A4 fixed 50/50 combined
  model (+ 30/70 and 70/30 sensitivity views), A5 the six long-only books (composite/mom/combined
  Top-25/Top-50) with sector caps / liquidity / equal weight, and helpers the platform uses for the A6
  operating state and A7 recommendation engine.

Every score is point-in-time: composite_sn is the fundamental as-of the panel; mom_6_1 uses only closes
through the prior month.  Nothing here is optimized on forward returns (the 50/50 weights are FIXED).
"""
from __future__ import annotations

import csv
import hashlib
import math
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

from paper_trader.api import multi_horizon_registry as mreg

# --------------------------------------------------------------------------- #
# Paths (env-overridable for tests)
# --------------------------------------------------------------------------- #
PANEL_ENV = "PAPER_TRADER_MHZ_FUND_PANEL"
INPUTS_ENV = "PAPER_TRADER_MHZ_INPUTS_DIR"
STORE_ENV = "PAPER_TRADER_MHZ_STORE_DIR"
SECTOR_MAP_ENV = "PAPER_TRADER_MHZ_SECTOR_MAP"

DEFAULT_PANEL = Path(
    r"C:\Users\binis\Stock_Prediction_app_push\research\output"
    r"\phase10l_historical_sector_neutral_scored_panel_reconstruction"
    r"\historical_sector_neutral_scored_panel.csv")
DEFAULT_INPUTS = Path(r"D:\Stock_Prediction_app_data\phase25_multi_horizon_alpha\_inputs")
DEFAULT_STORE = Path(r"D:\Stock_Prediction_app_data\phase25_multi_horizon_alpha")
# Owned GICS sector map (Phase 10-F repaired: ticker -> canonical 11-bucket GICS, current-as-of, used
# only as a grouping for the concentration cap + exposure report - NOT a point-in-time return input).
DEFAULT_SECTOR_MAP = Path(
    r"C:\Users\binis\Stock_Prediction_app_push\research\output"
    r"\phase10f_owned_sector_mapping_repair\repaired_sector_mapping.csv")

CUR_MOM_FILE = "current_momentum_scores.csv"
MONTHLY_PANEL_FILE = "momentum_monthly_panel.csv"
RISK_FILE = "current_risk_stats.csv"

# --------------------------------------------------------------------------- #
# Construction constants (fixed, not optimized)
# --------------------------------------------------------------------------- #
BOOK_SIZES = (25, 50)
SECTOR_CAP_FRACTION = 0.25       # max fraction of a book in any single (known) sector
MAX_INDIVIDUAL_WEIGHT = 0.10     # equal weight (<=1/25=0.04) always respects this
MIN_ADV_DOLLAR = 1.0e7           # $10M/day minimum dollar-liquidity
ENTRY_BUFFER = 0                 # enter at rank <= N
EXIT_BUFFER_FRACTION = 0.20      # hold until rank > N*(1+this); then EXIT_CANDIDATE
COST_BPS = 0.0025                # 25 bps round-trip for the estimated-cost display

# Combined model fixed weights + sensitivity views.
PRIMARY_WEIGHTS = {"composite_sn": 0.5, "mom_6_1": 0.5}
SENSITIVITY_VIEWS = {
    "fund30_mom70": {"composite_sn": 0.3, "mom_6_1": 0.7},
    "fund70_mom30": {"composite_sn": 0.7, "mom_6_1": 0.3},
}

STATUS_READY = "MHZ_READY"
STATUS_INPUTS_UNAVAILABLE = "MHZ_INPUTS_UNAVAILABLE"

# Frozen panel columns (subset used here).
C_REB = "rebalance_date"
C_TICKER = "ticker"
C_SECTOR = "sector"
C_LIQ = "liquidity_proxy"
C_COMPOSITE_SN = "composite_sn"
C_ASOF = "as_of_date"


# --------------------------------------------------------------------------- #
# small helpers
# --------------------------------------------------------------------------- #
def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve(explicit, env_var, default) -> Path:
    if explicit is not None:
        return Path(explicit)
    env = os.environ.get(env_var)
    return Path(env) if env else Path(default)


def _to_float(x) -> Optional[float]:
    if x is None or x == "":
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if math.isnan(v) or math.isinf(v):
        return None
    return v


def _round(x, nd):
    return None if x is None else round(float(x), nd)


def _mean(vals):
    return sum(vals) / len(vals) if vals else None


def _std(vals):
    n = len(vals)
    if n < 2:
        return None
    m = _mean(vals)
    return math.sqrt(sum((v - m) ** 2 for v in vals) / (n - 1))


def _fingerprint_file(path: Path) -> Optional[str]:
    try:
        h = hashlib.sha1()
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()[:16]
    except OSError:
        return None


def _percentiles(values: dict[str, float]) -> tuple[dict[str, float], dict[str, int], dict[str, float]]:
    """Return (percentile[0..1, best=1], rank[1=best], zscore) over a name->value dict.

    Ties are broken deterministically by ticker (ascending) so ranks are reproducible.
    """
    items = sorted(values.items(), key=lambda kv: (-kv[1], kv[0]))  # desc value, asc ticker
    n = len(items)
    ranks = {tk: i + 1 for i, (tk, _v) in enumerate(items)}
    pct = {tk: (1.0 if n == 1 else (n - ranks[tk]) / (n - 1)) for tk, _v in items}
    vals = list(values.values())
    mu = _mean(vals)
    sd = _std(vals)
    z = {tk: ((v - mu) / sd if sd else 0.0) for tk, v in values.items()}
    return pct, ranks, z


# --------------------------------------------------------------------------- #
# A2 - input pipeline (read owned CSVs, validate, fingerprint)
# --------------------------------------------------------------------------- #
def _load_fundamental_cross_section(panel_path: Path) -> dict:
    """Latest-month representative composite_sn cross-section from the frozen 10-L panel."""
    try:
        with open(panel_path, "r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            best: dict[str, dict[str, tuple[str, dict]]] = {}
            max_full = None
            as_of = None
            for r in reader:
                full = (r.get(C_REB) or "").strip()
                if len(full) < 7:
                    continue
                comp = _to_float(r.get(C_COMPOSITE_SN))
                if comp is None:
                    continue
                tk = (r.get(C_TICKER) or "").strip().upper()
                if not tk:
                    continue
                as_of = as_of or (r.get(C_ASOF) or "").strip()
                if max_full is None or full > max_full:
                    max_full = full
                mth = best.setdefault(full[:7], {})
                cur = mth.get(tk)
                if cur is None or full > cur[0]:
                    mth[tk] = (full, {"ticker": tk, "composite_sn": comp,
                                      "sector": (r.get(C_SECTOR) or "").strip() or "Unknown",
                                      "liquidity_proxy": _to_float(r.get(C_LIQ)),
                                      "rebalance_date": full})
    except OSError:
        return {"available": False}
    if not best:
        return {"available": False}
    latest_month = max(best)
    rows = {tk: payload for tk, (_f, payload) in best[latest_month].items()}
    fund_as_of = max((p["rebalance_date"] for p in rows.values()), default=latest_month)
    return {"available": True, "latest_month": latest_month, "as_of_date": fund_as_of,
            "panel_as_of": as_of, "rows": rows, "n_names": len(rows), "n_months": len(best)}


def _load_csv_rows(path: Path) -> Optional[list[dict]]:
    try:
        with open(path, "r", encoding="utf-8", newline="") as fh:
            return list(csv.DictReader(fh))
    except OSError:
        return None


def _load_sector_map(path: Path) -> dict[str, str]:
    """Owned ticker -> canonical GICS sector map (Phase 10-F repaired). Best-effort; empty if absent."""
    rows = _load_csv_rows(path)
    if not rows:
        return {}
    out: dict[str, str] = {}
    for r in rows:
        tk = (r.get("ticker") or "").strip().upper()
        sec = (r.get("repaired_sector") or r.get("sector") or "").strip()
        if tk and sec and sec != "Unknown":
            out[tk] = sec
    return out


def _apply_sector_map(sector: str, ticker: str, smap: dict[str, str]) -> str:
    if sector and sector != "Unknown":
        return sector
    return smap.get(ticker, sector or "Unknown")


def load_inputs(*, panel_path=None, inputs_dir=None) -> dict:
    """A2: load + validate all owned inputs. Never raises; returns availability + a validation report."""
    ppath = _resolve(panel_path, PANEL_ENV, DEFAULT_PANEL)
    idir = _resolve(inputs_dir, INPUTS_ENV, DEFAULT_INPUTS)
    smpath = _resolve(None, SECTOR_MAP_ENV, DEFAULT_SECTOR_MAP)
    mom_path = idir / CUR_MOM_FILE
    risk_path = idir / RISK_FILE

    fund = _load_fundamental_cross_section(ppath)
    mom_rows = _load_csv_rows(mom_path)
    risk_rows = _load_csv_rows(risk_path)
    smap = _load_sector_map(smpath)
    # Fill Unknown sectors on the fundamental cross-section from the owned GICS map.
    if fund.get("available"):
        for tk, row in fund["rows"].items():
            row["sector"] = _apply_sector_map(row.get("sector", "Unknown"), tk, smap)

    warnings: list[str] = []
    validations: dict[str, Any] = {}

    # fundamental checks
    if not fund.get("available"):
        warnings.append("Frozen Phase 10-L fundamental panel missing/empty at %s" % ppath)
    # momentum checks
    mom: dict[str, dict] = {}
    dup_mom = 0
    market_as_of = None
    mom_month = None
    if mom_rows is None:
        warnings.append("current_momentum_scores.csv missing at %s" % mom_path)
    else:
        for r in mom_rows:
            tk = (r.get("ticker") or "").strip().upper()
            if not tk:
                continue
            if tk in mom:
                dup_mom += 1
                continue
            market_as_of = market_as_of or (r.get("market_as_of_date") or "").strip()
            mom_month = mom_month or (r.get("month_label") or "").strip()
            _sec = (r.get("sector") or "").strip() or "Unknown"
            mom[tk] = {
                "ticker": tk, "mom_6_1": _to_float(r.get("mom_6_1")),
                "is_member": (r.get("is_member") == "1"),
                "adv_dollar": _to_float(r.get("adv_dollar")),
                "realized_vol_63d": _to_float(r.get("realized_vol_63d")),
                "trailing_obs_126": _to_float(r.get("trailing_obs_126")),
                "eligible_history": (r.get("eligible_history") == "1"),
                "extreme_flag": (r.get("extreme_flag") == "1"),
                "sector": _apply_sector_map(_sec, tk, smap),
            }
    # risk checks
    risk: dict[str, dict] = {}
    if risk_rows is None:
        warnings.append("current_risk_stats.csv missing at %s" % risk_path)
    else:
        for r in risk_rows:
            tk = (r.get("ticker") or "").strip().upper()
            if not tk or tk in risk:
                continue
            risk[tk] = {"realized_vol_63d": _to_float(r.get("realized_vol_63d")),
                        "beta_universe": _to_float(r.get("beta_universe")),
                        "adv_dollar_20d": _to_float(r.get("adv_dollar_20d")),
                        "max_drawdown_252d": _to_float(r.get("max_drawdown_252d")),
                        "is_current_member": (r.get("is_current_member") == "1"),
                        "sector": (r.get("sector") or "").strip() or "Unknown"}

    n_extreme = sum(1 for v in mom.values() if v["extreme_flag"])
    n_mom_elig = sum(1 for v in mom.values() if v["eligible_history"] and not v["extreme_flag"])
    fund_rows_d = fund.get("rows", {}) if fund.get("available") else {}
    fund_sec_known = sum(1 for r in fund_rows_d.values() if r.get("sector", "Unknown") != "Unknown")
    validations = {
        "fundamental_available": bool(fund.get("available")),
        "fundamental_latest_month": fund.get("latest_month"),
        "fundamental_as_of_date": fund.get("as_of_date"),
        "fundamental_names": fund.get("n_names"),
        "momentum_available": mom_rows is not None,
        "momentum_month": mom_month,
        "market_as_of_date": market_as_of,
        "momentum_names": len(mom),
        "momentum_duplicates_dropped": dup_mom,
        "momentum_extreme_flagged": n_extreme,
        "momentum_eligible": n_mom_elig,
        "risk_available": risk_rows is not None,
        "risk_names": len(risk),
        "min_adv_dollar": MIN_ADV_DOLLAR,
        "sector_map_available": bool(smap),
        "sector_map_names": len(smap),
        "fundamental_sector_known": fund_sec_known,
        "fundamental_sector_coverage": (round(fund_sec_known / len(fund_rows_d), 4) if fund_rows_d else 0.0),
    }
    available = bool(fund.get("available")) and mom_rows is not None
    fingerprints = {
        "fundamental_panel": _fingerprint_file(ppath),
        "current_momentum_scores": _fingerprint_file(mom_path),
        "current_risk_stats": _fingerprint_file(risk_path),
        "sector_map": _fingerprint_file(smpath),
    }
    return {
        "available": available,
        "fund": fund, "mom": mom, "risk": risk,
        "market_as_of_date": market_as_of, "momentum_month": mom_month,
        "fundamental_as_of_date": fund.get("as_of_date"), "fundamental_month": fund.get("latest_month"),
        "validations": validations, "warnings": warnings, "fingerprints": fingerprints,
        "paths": {"fundamental_panel": str(ppath), "inputs_dir": str(idir)},
    }


# --------------------------------------------------------------------------- #
# A3 - current model scores per security
# --------------------------------------------------------------------------- #
def _fund_eligibility(row: dict) -> tuple[bool, Optional[str], list[str]]:
    flags = []
    if row.get("sector", "Unknown") == "Unknown":
        flags.append("MISSING_SECTOR")
    liq = row.get("liquidity_proxy")
    if row.get("composite_sn") is None:
        return False, "MISSING_COMPOSITE_SN", flags
    return True, None, flags


def _mom_eligibility(row: dict) -> tuple[bool, Optional[str], list[str]]:
    flags = []
    if row.get("mom_6_1") is None:
        return False, "MISSING_MOMENTUM", flags
    if not row.get("is_member"):
        return False, "NOT_CURRENT_MEMBER", flags
    if row.get("extreme_flag"):
        flags.append("EXTREME_MOMENTUM")
        return False, "DATA_QUALITY_BLOCK", flags
    if not row.get("eligible_history"):
        return False, "MOMENTUM_HISTORY_INSUFFICIENT", flags
    adv = row.get("adv_dollar")
    if adv is not None and adv < MIN_ADV_DOLLAR:
        flags.append("LOW_LIQUIDITY")
        return False, "LIQUIDITY_FILTER_FAILED", flags
    if row.get("sector", "Unknown") == "Unknown":
        flags.append("MISSING_SECTOR")
    return True, None, flags


def compute_scores(inp: dict) -> dict:
    """A3: per-security model legs (raw/oriented/normalized/percentile/rank/eligible + flags)."""
    fund_rows = inp["fund"].get("rows", {}) if inp.get("fund", {}).get("available") else {}
    mom_rows = inp.get("mom", {})
    market_as_of = inp.get("market_as_of_date")
    fund_as_of = inp.get("fundamental_as_of_date")

    # fundamental leg
    fund_elig_vals: dict[str, float] = {}
    fund_scores: dict[str, dict] = {}
    for tk, row in fund_rows.items():
        ok, reason, flags = _fund_eligibility(row)
        fund_scores[tk] = {"ticker": tk, "model_id": "composite_sn", "model_version": "v1",
                           "raw_signal": row.get("composite_sn"), "oriented_signal": row.get("composite_sn"),
                           "sector": row.get("sector"), "eligible": ok, "exclusion_reason": reason,
                           "data_quality_flags": flags, "market_as_of_date": market_as_of,
                           "fundamental_as_of_date": fund_as_of, "liquidity_proxy": row.get("liquidity_proxy")}
        if ok:
            fund_elig_vals[tk] = row["composite_sn"]
    fpct, frank, fz = _percentiles(fund_elig_vals) if fund_elig_vals else ({}, {}, {})
    for tk, sc in fund_scores.items():
        sc["percentile"] = _round(fpct.get(tk), 6)
        sc["rank"] = frank.get(tk)
        sc["normalized_score"] = _round(fz.get(tk), 6)

    # momentum leg
    mom_elig_vals: dict[str, float] = {}
    mom_scores: dict[str, dict] = {}
    for tk, row in mom_rows.items():
        ok, reason, flags = _mom_eligibility(row)
        mom_scores[tk] = {"ticker": tk, "model_id": "mom_6_1", "model_version": "v1",
                          "raw_signal": row.get("mom_6_1"), "oriented_signal": row.get("mom_6_1"),
                          "sector": row.get("sector"), "eligible": ok, "exclusion_reason": reason,
                          "data_quality_flags": flags, "market_as_of_date": market_as_of,
                          "fundamental_as_of_date": None, "adv_dollar": row.get("adv_dollar"),
                          "realized_vol_63d": row.get("realized_vol_63d")}
        if ok:
            mom_elig_vals[tk] = row["mom_6_1"]
    mpct, mrank, mz = _percentiles(mom_elig_vals) if mom_elig_vals else ({}, {}, {})
    for tk, sc in mom_scores.items():
        sc["percentile"] = _round(mpct.get(tk), 6)
        sc["rank"] = mrank.get(tk)
        sc["normalized_score"] = _round(mz.get(tk), 6)

    return {
        "composite_sn": fund_scores, "mom_6_1": mom_scores,
        "fund_eligible": list(fund_elig_vals), "mom_eligible": list(mom_elig_vals),
        "counts": {"composite_sn_eligible": len(fund_elig_vals),
                   "mom_6_1_eligible": len(mom_elig_vals)},
    }


# --------------------------------------------------------------------------- #
# A4 - fixed 50/50 combined (+ sensitivity views) over the common eligible universe
# --------------------------------------------------------------------------- #
def compute_combined(inp: dict, scores: dict) -> dict:
    """A4: 50/50 rank blend over the common eligible universe. Fixed weights; sensitivities are separate."""
    fund_scores = scores["composite_sn"]
    mom_scores = scores["mom_6_1"]
    fund_elig = set(scores["fund_eligible"])
    mom_elig = set(scores["mom_eligible"])
    common = sorted(fund_elig & mom_elig)

    # re-rank each leg WITHIN the common universe
    fvals = {tk: fund_scores[tk]["raw_signal"] for tk in common}
    mvals = {tk: mom_scores[tk]["raw_signal"] for tk in common}
    fpct, _fr, _fz = _percentiles(fvals) if fvals else ({}, {}, {})
    mpct, _mr, _mz = _percentiles(mvals) if mvals else ({}, {}, {})

    def blend(weights):
        out = {}
        for tk in common:
            fp = fpct.get(tk, 0.0)
            mp = mpct.get(tk, 0.0)
            out[tk] = weights["composite_sn"] * fp + weights["mom_6_1"] * mp
        return out

    primary = blend(PRIMARY_WEIGHTS)
    ppct, prank, _pz = _percentiles(primary) if primary else ({}, {}, {})
    sens = {name: blend(w) for name, w in SENSITIVITY_VIEWS.items()}
    sens_rank = {name: _percentiles(v)[1] if v else {} for name, v in sens.items()}

    combined = {}
    for tk in common:
        sector = fund_scores[tk]["sector"] if fund_scores[tk]["sector"] != "Unknown" else mom_scores[tk]["sector"]
        combined[tk] = {
            "ticker": tk, "model_id": "fundamental_momentum_50_50_v1", "model_version": "v1",
            "combined_score": _round(primary[tk], 6),
            "percentile": _round(ppct.get(tk), 6), "rank": prank.get(tk),
            "sector": sector,
            "fund_percentile": _round(fpct.get(tk), 6),
            "mom_percentile": _round(mpct.get(tk), 6),
            "component_contributions": {
                "composite_sn": _round(PRIMARY_WEIGHTS["composite_sn"] * fpct.get(tk, 0.0), 6),
                "mom_6_1": _round(PRIMARY_WEIGHTS["mom_6_1"] * mpct.get(tk, 0.0), 6)},
            "sensitivity_ranks": {name: sens_rank[name].get(tk) for name in SENSITIVITY_VIEWS},
            "adv_dollar": mom_scores[tk].get("adv_dollar"),
            "fund_rank": fund_scores[tk]["rank"], "mom_rank": mom_scores[tk]["rank"],
            "market_as_of_date": inp.get("market_as_of_date"),
        }
    return {"combined": combined, "common_universe": common, "n_common": len(common),
            "weights": PRIMARY_WEIGHTS, "sensitivity_views": SENSITIVITY_VIEWS}


# --------------------------------------------------------------------------- #
# A5 - the six long-only books (equal weight, sector cap, liquidity, buffers)
# --------------------------------------------------------------------------- #
def _select_book(ranked: list[dict], size: int) -> dict:
    """Greedy top-N with a per-sector cap on KNOWN sectors and equal weight. Deterministic."""
    max_per_sector = max(1, int(SECTOR_CAP_FRACTION * size))
    sector_count: dict[str, int] = {}
    picked: list[dict] = []
    skipped_sector: list[str] = []
    for row in ranked:  # ranked is already score-desc, ticker-asc
        if len(picked) >= size:
            break
        sec = row.get("sector", "Unknown")
        if sec != "Unknown" and sector_count.get(sec, 0) >= max_per_sector:
            skipped_sector.append(row["ticker"])
            continue
        picked.append(row)
        sector_count[sec] = sector_count.get(sec, 0) + 1
    # equal weight, hard-capped at MAX_INDIVIDUAL_WEIGHT: a thin (under-filled) book holds the
    # remainder as implied cash rather than over-concentrating single names.
    raw_weight = (1.0 / len(picked)) if picked else 0.0
    weight = round(min(raw_weight, MAX_INDIVIDUAL_WEIGHT), 6)
    unallocated = round(max(0.0, 1.0 - weight * len(picked)), 6)
    sector_exposure: dict[str, float] = {}
    for row in picked:
        sec = row.get("sector", "Unknown")
        sector_exposure[sec] = round(sector_exposure.get(sec, 0.0) + weight, 6)
    constituents = [{"ticker": r["ticker"], "weight": weight, "rank": i + 1,
                     "score": r.get("score"), "sector": r.get("sector"),
                     "adv_dollar": r.get("adv_dollar")} for i, r in enumerate(picked)]
    return {"size_target": size, "size_actual": len(picked), "equal_weight": weight,
            "unallocated_weight": unallocated,
            "max_individual_weight_cap": MAX_INDIVIDUAL_WEIGHT,
            "sector_cap_fraction": SECTOR_CAP_FRACTION, "max_per_sector": max_per_sector,
            "sector_exposure": sector_exposure, "constituents": constituents,
            "sector_capped_out": skipped_sector}


def _ranked_from_scores(score_map: dict, value_key: str) -> list[dict]:
    rows = []
    for tk, sc in score_map.items():
        if not sc.get("eligible", True):
            continue
        v = sc.get(value_key)
        if v is None:
            continue
        rows.append({"ticker": tk, "score": v, "sector": sc.get("sector", "Unknown"),
                     "adv_dollar": sc.get("adv_dollar")})
    rows.sort(key=lambda r: (-r["score"], r["ticker"]))
    return rows


def build_books(inp: dict, scores: dict, combined: dict) -> dict:
    """A5: the six long-only equal-weight books with deterministic construction."""
    fund_ranked = _ranked_from_scores(scores["composite_sn"], "raw_signal")
    mom_ranked = _ranked_from_scores(scores["mom_6_1"], "raw_signal")
    comb_map = {tk: {"eligible": True, "raw_signal": c["combined_score"], "sector": c["sector"],
                     "adv_dollar": c.get("adv_dollar")} for tk, c in combined["combined"].items()}
    comb_ranked = _ranked_from_scores(comb_map, "raw_signal")

    books = {}
    for size in BOOK_SIZES:
        books[f"composite_sn_top{size}"] = {"model_id": "composite_sn", **_select_book(fund_ranked, size)}
        books[f"mom_6_1_top{size}"] = {"model_id": "mom_6_1", **_select_book(mom_ranked, size)}
        books[f"fundamental_momentum_50_50_top{size}"] = {
            "model_id": "fundamental_momentum_50_50_v1", **_select_book(comb_ranked, size)}

    primary_book_id = "fundamental_momentum_50_50_top25"
    # overlaps between the three Top-25 books
    def _set(bid):
        return {c["ticker"] for c in books[bid]["constituents"]}
    overlaps = {
        "fund_vs_mom_top25": len(_set("composite_sn_top25") & _set("mom_6_1_top25")),
        "combined_vs_fund_top25": len(_set("fundamental_momentum_50_50_top25") & _set("composite_sn_top25")),
        "combined_vs_mom_top25": len(_set("fundamental_momentum_50_50_top25") & _set("mom_6_1_top25")),
    }
    return {"books": books, "primary_book_id": primary_book_id, "overlaps": overlaps}


# --------------------------------------------------------------------------- #
# top-level current-state build (A2-A5) with mtime cache
# --------------------------------------------------------------------------- #
_CACHE: dict = {}


def clear_cache() -> None:
    _CACHE.clear()


def _cache_key(panel_path: Path, inputs_dir: Path) -> Optional[tuple]:
    try:
        pm = panel_path.stat().st_mtime
    except OSError:
        pm = 0.0
    parts = [("panel", pm)]
    for fn in (CUR_MOM_FILE, RISK_FILE):
        try:
            parts.append((fn, (inputs_dir / fn).stat().st_mtime))
        except OSError:
            parts.append((fn, 0.0))
    return tuple(parts)


def build_current(*, panel_path=None, inputs_dir=None, use_cache=True) -> dict:
    """Build A2-A5 for the current cross-section. Cached by input mtimes; never raises."""
    ppath = _resolve(panel_path, PANEL_ENV, DEFAULT_PANEL)
    idir = _resolve(inputs_dir, INPUTS_ENV, DEFAULT_INPUTS)
    key = _cache_key(ppath, idir) if use_cache else None
    if key is not None and key in _CACHE:
        return _CACHE[key]

    inp = load_inputs(panel_path=ppath, inputs_dir=idir)
    if not inp["available"]:
        result = {"status": STATUS_INPUTS_UNAVAILABLE, "inputs": inp,
                  "warnings": inp["warnings"], "built_at": _iso_now()}
        if key is not None:
            _CACHE[key] = result
        return result

    scores = compute_scores(inp)
    combined = compute_combined(inp, scores)
    books = build_books(inp, scores, combined)
    result = {
        "status": STATUS_READY,
        "market_as_of_date": inp["market_as_of_date"],
        "fundamental_as_of_date": inp["fundamental_as_of_date"],
        "fundamental_month": inp["fundamental_month"],
        "momentum_month": inp["momentum_month"],
        "inputs": inp, "scores": scores, "combined": combined, "books": books,
        "warnings": inp["warnings"], "built_at": _iso_now(),
    }
    if key is not None:
        _CACHE[key] = result
    return result


# --------------------------------------------------------------------------- #
# A6 - daily operating state (compares current cross-section to prior confirmed snapshots)
# --------------------------------------------------------------------------- #
STATE_NO_REVIEW_DUE = "NO_REVIEW_DUE"
STATE_RISK_REFRESH_ONLY = "RISK_REFRESH_ONLY"
STATE_FUNDAMENTAL_REVIEW_DUE = "FUNDAMENTAL_REVIEW_DUE"
STATE_MOMENTUM_REVIEW_DUE = "MOMENTUM_REVIEW_DUE"
STATE_COMBINED_REVIEW_DUE = "COMBINED_REVIEW_DUE"
STATE_FAST_REVIEW_DUE = "FAST_REVIEW_DUE"
STATE_DATA_BLOCKED = "DATA_BLOCKED"
STATE_MANUAL_CONFIRMATION_REQUIRED = "MANUAL_CONFIRMATION_REQUIRED"


def _parse_iso_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return date.fromisoformat(s[:10])
    except (ValueError, TypeError):
        return None


def _month_key(d: Optional[date]) -> Optional[str]:
    return f"{d.year:04d}-{d.month:02d}" if d else None


def _next_month_first(d: date) -> date:
    return date(d.year + 1, 1, 1) if d.month == 12 else date(d.year, d.month + 1, 1)


def _next_quarter_first(d: date) -> date:
    q_end_month = ((d.month - 1) // 3 + 1) * 3  # 3,6,9,12
    return date(d.year + 1, 1, 1) if q_end_month == 12 else date(d.year, q_end_month + 1, 1)


def next_review_date(cadence: str, market_as_of: Optional[str]) -> Optional[str]:
    d = _parse_iso_date(market_as_of)
    if d is None:
        return None
    if cadence == mreg.CADENCE_MONTHLY:
        return _next_month_first(d).isoformat()
    if cadence == mreg.CADENCE_QUARTERLY:
        return _next_quarter_first(d).isoformat()
    if cadence in (mreg.CADENCE_DAILY, mreg.CADENCE_WEEKLY):
        return market_as_of  # risk refresh / daily observation - reviewed continuously
    return None


def _sleeve_current_period(sleeve_id: str, current: dict) -> Optional[str]:
    """The period key a sleeve rebalances on: fundamental->fund month, others->momentum month."""
    if sleeve_id == mreg.SLEEVE_FUNDAMENTAL:
        return current.get("fundamental_month")
    return current.get("momentum_month")


def compute_operating_state(current: dict, prior: Optional[dict] = None,
                            validated_fast_alpha_available: bool = False) -> dict:
    """A6: per-sleeve review-due state + the single daily operating state.

    ``prior`` maps sleeve_id -> last-confirmed snapshot dict (from the ledger), or is empty/None.
    """
    prior = prior or {}
    market_as_of = current.get("market_as_of_date")
    if current.get("status") != STATUS_READY:
        return {"operating_state": STATE_DATA_BLOCKED, "market_as_of_date": market_as_of,
                "model_scores_recalculated": False, "risk_data_refreshed": False,
                "portfolio_review_due": False, "recommendation_changed": False,
                "no_change_required": False, "sleeves": [],
                "reason": "Owned inputs unavailable; no scores computed."}

    risk_available = bool(current.get("inputs", {}).get("validations", {}).get("risk_available"))
    sleeves_state = []
    any_review_due = False
    any_unconfirmed_due = False
    due_priority = None  # (priority, state)

    for s in mreg.sleeve_registry():
        sid = s["sleeve_id"]
        cad = s["cadence"]
        cur_period = _sleeve_current_period(sid, current)
        p = prior.get(sid) or {}
        last_confirmed_period = p.get("period")
        last_confirmed_at = p.get("confirmed_at")

        if sid == mreg.SLEEVE_DEFENSIVE:
            state = {"sleeve_id": sid, "display_name": s["display_name"], "active_model": s["active_model"],
                     "cadence": cad, "review_due": False, "rebalance_due_status": "RISK_REFRESH_ONLY",
                     "risk_only_refresh": True, "action_generation_enabled": False,
                     "last_calculation_date": market_as_of, "last_confirmed_snapshot": last_confirmed_at,
                     "next_manual_review_date": next_review_date(cad, market_as_of),
                     "current_actionability": mreg.ACT_MONITOR_ONLY, "paper_only": True}
            sleeves_state.append(state)
            continue

        if sid == mreg.SLEEVE_FAST:
            fast_due = bool(validated_fast_alpha_available)
            state = {"sleeve_id": sid, "display_name": s["display_name"], "active_model": s["active_model"],
                     "cadence": cad, "review_due": fast_due,
                     "rebalance_due_status": (mreg.NO_VALIDATED_FAST_ALPHA if not fast_due else "FAST_REVIEW_DUE"),
                     "risk_only_refresh": False, "action_generation_enabled": fast_due,
                     "last_calculation_date": market_as_of, "last_confirmed_snapshot": last_confirmed_at,
                     "next_manual_review_date": None,
                     "current_actionability": mreg.ACT_INACTIVE, "paper_only": True,
                     "fast_status": mreg.NO_VALIDATED_FAST_ALPHA if not fast_due else "VALIDATED"}
            sleeves_state.append(state)
            if fast_due:
                any_review_due = True
                due_priority = due_priority or (0, STATE_FAST_REVIEW_DUE)
            continue

        review_due = (last_confirmed_period is None) or (cur_period != last_confirmed_period)
        unconfirmed_due = review_due  # a due sleeve with a target book but no matching confirmed snapshot
        if review_due:
            any_review_due = True
            if unconfirmed_due:
                any_unconfirmed_due = True
        actionability = mreg.ACT_REVIEW_DUE if review_due else mreg.ACT_HOLD
        state = {"sleeve_id": sid, "display_name": s["display_name"], "active_model": s["active_model"],
                 "cadence": cad, "review_due": review_due,
                 "rebalance_due_status": ("REBALANCE_DUE" if review_due else "UP_TO_DATE"),
                 "risk_only_refresh": False, "action_generation_enabled": s["action_generation_enabled"],
                 "last_calculation_date": market_as_of, "last_confirmed_snapshot": last_confirmed_at,
                 "last_confirmed_period": last_confirmed_period, "current_period": cur_period,
                 "next_manual_review_date": next_review_date(cad, market_as_of),
                 "current_actionability": actionability, "paper_only": True}
        sleeves_state.append(state)
        # priority for the single daily state: combined > momentum > fundamental
        pr = {mreg.SLEEVE_COMBINED: (1, STATE_COMBINED_REVIEW_DUE),
              mreg.SLEEVE_MOMENTUM: (2, STATE_MOMENTUM_REVIEW_DUE),
              mreg.SLEEVE_FUNDAMENTAL: (3, STATE_FUNDAMENTAL_REVIEW_DUE)}.get(sid)
        if review_due and pr is not None:
            if due_priority is None or pr[0] < due_priority[0]:
                due_priority = pr

    if any_unconfirmed_due:
        operating_state = STATE_MANUAL_CONFIRMATION_REQUIRED
    elif due_priority is not None:
        operating_state = due_priority[1]
    elif risk_available:
        operating_state = STATE_RISK_REFRESH_ONLY
    else:
        operating_state = STATE_NO_REVIEW_DUE

    return {
        "operating_state": operating_state,
        "market_as_of_date": market_as_of,
        "fundamental_month": current.get("fundamental_month"),
        "momentum_month": current.get("momentum_month"),
        "model_scores_recalculated": True,
        "risk_data_refreshed": risk_available,
        "portfolio_review_due": any_review_due,
        "recommendation_changed": any_unconfirmed_due,
        "no_change_required": (not any_review_due),
        "sleeves": sleeves_state,
        "validated_fast_alpha_available": bool(validated_fast_alpha_available),
        "reason": _operating_state_reason(operating_state, any_review_due),
    }


def _operating_state_reason(state: str, any_due: bool) -> str:
    return {
        STATE_MANUAL_CONFIRMATION_REQUIRED: "A review is due and a target book is ready; manual confirmation "
                                            "is required to append a paper snapshot. No orders are created.",
        STATE_COMBINED_REVIEW_DUE: "The combined (primary) sleeve is due for its monthly manual review.",
        STATE_MOMENTUM_REVIEW_DUE: "The momentum sleeve is due for its monthly manual review.",
        STATE_FUNDAMENTAL_REVIEW_DUE: "The fundamental sleeve is due for its quarterly manual review.",
        STATE_FAST_REVIEW_DUE: "A validated fast model is available for review.",
        STATE_RISK_REFRESH_ONLY: "Only risk/market data refreshed today; no model review is due. HOLD/WAIT.",
        STATE_NO_REVIEW_DUE: "No sleeve review is due today; scores recalculated only. HOLD/WAIT.",
        STATE_DATA_BLOCKED: "Owned inputs unavailable; scores could not be computed.",
    }.get(state, "")


# --------------------------------------------------------------------------- #
# A7 - recommendation engine (compares target book to prior confirmed book)
# --------------------------------------------------------------------------- #
REC_BUY = "BUY_CANDIDATE"
REC_HOLD = "HOLD"
REC_REDUCE = "REDUCE_CANDIDATE"
REC_EXIT = "EXIT_CANDIDATE"
REC_WAIT = "WAIT"

_SLEEVE_MODEL = {
    mreg.SLEEVE_FUNDAMENTAL: "composite_sn",
    mreg.SLEEVE_MOMENTUM: "mom_6_1",
    mreg.SLEEVE_COMBINED: "fundamental_momentum_50_50_v1",
}


def _model_ranked(current: dict, model_id: str) -> list[dict]:
    """Full eligible ranked list (score desc, ticker asc) for a model, with sector + component pcts."""
    if model_id == "fundamental_momentum_50_50_v1":
        comb = current["combined"]["combined"]
        rows = [{"ticker": tk, "score": c["combined_score"], "sector": c["sector"],
                 "fund_percentile": c["fund_percentile"], "mom_percentile": c["mom_percentile"],
                 "fund_rank": c["fund_rank"], "mom_rank": c["mom_rank"],
                 "adv_dollar": c.get("adv_dollar")} for tk, c in comb.items()]
    else:
        smap = current["scores"][model_id]
        rows = [{"ticker": tk, "score": sc["raw_signal"], "sector": sc.get("sector", "Unknown"),
                 "fund_percentile": sc.get("percentile") if model_id == "composite_sn" else None,
                 "mom_percentile": sc.get("percentile") if model_id == "mom_6_1" else None,
                 "adv_dollar": sc.get("adv_dollar")}
                for tk, sc in smap.items() if sc.get("eligible") and sc.get("raw_signal") is not None]
    rows.sort(key=lambda r: (-r["score"], r["ticker"]))
    for i, r in enumerate(rows):
        r["current_rank"] = i + 1
    return rows


def _component_reason_codes(row: dict) -> list[str]:
    codes = []
    fp = row.get("fund_percentile")
    mp = row.get("mom_percentile")
    if fp is not None and mp is not None:
        if fp > 0.5 and mp > 0.5:
            codes.append("BOTH_ALPHA_LEGS_POSITIVE")
        elif fp >= 0.6 and mp < 0.4:
            codes.append("FUNDAMENTAL_STRONG_MOMENTUM_WEAK")
        elif mp >= 0.6 and fp < 0.4:
            codes.append("MOMENTUM_STRONG_FUNDAMENTAL_WEAK")
    return codes


def compute_recommendations(current: dict, prior: Optional[dict], sleeve_id: str,
                            size: int = 25, review_due: Optional[bool] = None) -> dict:
    """A7: recommendations for one sleeve/size vs the prior confirmed book. Deterministic reason codes."""
    model_id = _SLEEVE_MODEL.get(sleeve_id)
    if model_id is None:
        return {"sleeve_id": sleeve_id, "recommendations": [], "note": "Sleeve does not generate recommendations.",
                "counts": {}}
    book_id = {"composite_sn": f"composite_sn_top{size}",
               "mom_6_1": f"mom_6_1_top{size}",
               "fundamental_momentum_50_50_v1": f"fundamental_momentum_50_50_top{size}"}[model_id]
    exit_buffer_rank = math.ceil(size * (1.0 + EXIT_BUFFER_FRACTION))

    ranked = _model_ranked(current, model_id)
    book = current["books"]["books"].get(book_id)
    if book is None:
        # non-standard size: construct the book on the fly with the same deterministic rules
        book = _select_book([{"ticker": r["ticker"], "score": r["score"],
                              "sector": r.get("sector", "Unknown"),
                              "adv_dollar": r.get("adv_dollar")} for r in ranked], size)
    target = {c["ticker"]: c for c in book["constituents"]}
    weight = book["equal_weight"]
    rank_by_tk = {r["ticker"]: r for r in ranked}

    prior_snap = (prior or {}).get(sleeve_id) or {}
    prior_book = set(prior_snap.get(f"constituents_top{size}") or [])
    p = prior_snap.get("period")
    cur_period = _sleeve_current_period(sleeve_id, current)
    if review_due is None:
        review_due = (p is None) or (cur_period != p)

    market_as_of = current.get("market_as_of_date")
    fund_as_of = current.get("fundamental_as_of_date")
    fund_month = current.get("fundamental_month")
    stale_fundamental = _is_fundamental_stale(fund_month, market_as_of)

    recs = []
    universe = set(target) | prior_book
    for tk in sorted(universe):
        r = rank_by_tk.get(tk)
        in_target = tk in target
        in_prior = tk in prior_book
        cur_rank = r["current_rank"] if r else None
        reason_codes: list[str] = []
        risk_flags: list[str] = []

        # base decision
        if not review_due:
            rec = REC_HOLD if in_prior else REC_WAIT
            reason_codes.append("REVIEW_NOT_DUE")
        elif in_target and in_prior:
            rec = REC_HOLD
            reason_codes.append(f"REMAINS_TOP_{size}")
        elif in_target and not in_prior:
            rec = REC_BUY
            reason_codes.append(f"ENTERED_TOP_{size}")
        elif in_prior and not in_target:
            if r is None:
                rec = REC_EXIT
                # became ineligible - attach the exclusion reason if available
                sc = current["scores"].get(model_id, {}).get(tk) if model_id != "fundamental_momentum_50_50_v1" else None
                excl = (sc or {}).get("exclusion_reason")
                reason_codes.append(excl or "DATA_QUALITY_BLOCK")
                if excl:
                    risk_flags.append(excl)
            elif cur_rank is not None and cur_rank <= exit_buffer_rank:
                rec = REC_HOLD
                reason_codes.append(f"WITHIN_EXIT_BUFFER_TOP_{exit_buffer_rank}")
            else:
                rec = REC_EXIT
                reason_codes.append("FELL_BELOW_EXIT_BUFFER")
        else:
            rec = REC_WAIT

        reason_codes.extend(_component_reason_codes(r or {}))
        if stale_fundamental and model_id in ("composite_sn", "fundamental_momentum_50_50_v1"):
            reason_codes.append("FUNDAMENTAL_DATA_STALE")
            risk_flags.append("FUNDAMENTAL_DATA_STALE")
        if tk in book.get("sector_capped_out", []):
            reason_codes.append("SECTOR_LIMIT_REACHED")

        recs.append({
            "ticker": tk, "sleeve": sleeve_id, "recommendation": rec,
            "current_theoretical_weight": (weight if in_prior else 0.0),
            "target_weight": (weight if in_target else 0.0),
            "model_ranks": {"combined": (r or {}).get("current_rank") if model_id.startswith("fundamental_momentum") else None,
                            "fundamental": (r or {}).get("fund_rank"),
                            "momentum": (r or {}).get("mom_rank"),
                            "current": cur_rank},
            "combined_score": (r or {}).get("score"),
            "component_contributions": {"fund_percentile": (r or {}).get("fund_percentile"),
                                        "mom_percentile": (r or {}).get("mom_percentile")},
            "sector": (r or {}).get("sector") or (target.get(tk, {}) or {}).get("sector"),
            "reason_codes": reason_codes,
            "review_cadence": mreg.sleeve_by_id(sleeve_id)["cadence"],
            "actionability": (mreg.ACT_REVIEW_DUE if review_due else mreg.ACT_HOLD),
            "source_dates": {"market_as_of_date": market_as_of, "fundamental_as_of_date": fund_as_of},
            "risk_flags": risk_flags,
        })

    # order: BUY, EXIT, REDUCE, HOLD, WAIT
    order = {REC_BUY: 0, REC_EXIT: 1, REC_REDUCE: 2, REC_HOLD: 3, REC_WAIT: 4}
    recs.sort(key=lambda x: (order.get(x["recommendation"], 9), -(x["combined_score"] or -1e9), x["ticker"]))
    counts = {}
    for x in recs:
        counts[x["recommendation"]] = counts.get(x["recommendation"], 0) + 1
    est_turnover = _estimate_turnover(target, prior_book, size)
    return {"sleeve_id": sleeve_id, "model_id": model_id, "book_id": book_id, "size": size,
            "review_due": review_due, "recommendations": recs, "counts": counts,
            "estimated_turnover": est_turnover,
            "estimated_transaction_cost": round(2.0 * COST_BPS * est_turnover, 6),
            "exit_buffer_rank": exit_buffer_rank}


def _estimate_turnover(target: dict, prior_book: set, size: int) -> float:
    if not prior_book:
        return 1.0  # first establishment of the book
    tset = set(target)
    churn = len(tset - prior_book) + len(prior_book - tset)
    denom = (len(tset) + len(prior_book)) or 1
    return round(churn / denom, 6)


def _is_fundamental_stale(fund_month: Optional[str], market_as_of: Optional[str]) -> bool:
    """Fundamental is 'stale' when the panel month lags the market month by more than a quarter."""
    fm = _parse_iso_date((fund_month or "") + "-01") if fund_month else None
    md = _parse_iso_date(market_as_of)
    if fm is None or md is None:
        return False
    months_lag = (md.year - fm.year) * 12 + (md.month - fm.month)
    return months_lag > 3


def blocked_model_notice() -> dict:
    """Reversal + fast: explicitly report that no recommendations are generated (and why)."""
    return {
        "short_reversal_close_to_close": {
            "deployment_status": mreg.STATUS_INFO_ONLY,
            "recommendations_generated": False,
            "reason_codes": ["FAST_MODEL_NOT_VALIDATED", "INFORMATION_ONLY_SIGNAL"],
            "note": "Cost-killed at 25 bps (Phase 24). Blocked from recommendation generation."},
    }


__all__ = [
    "PANEL_ENV", "INPUTS_ENV", "STORE_ENV", "SECTOR_MAP_ENV",
    "DEFAULT_PANEL", "DEFAULT_INPUTS", "DEFAULT_STORE", "DEFAULT_SECTOR_MAP",
    "CUR_MOM_FILE", "MONTHLY_PANEL_FILE", "RISK_FILE",
    "BOOK_SIZES", "SECTOR_CAP_FRACTION", "MAX_INDIVIDUAL_WEIGHT", "MIN_ADV_DOLLAR",
    "ENTRY_BUFFER", "EXIT_BUFFER_FRACTION", "COST_BPS",
    "PRIMARY_WEIGHTS", "SENSITIVITY_VIEWS", "STATUS_READY", "STATUS_INPUTS_UNAVAILABLE",
    "load_inputs", "compute_scores", "compute_combined", "build_books", "build_current",
    "clear_cache",
    "STATE_NO_REVIEW_DUE", "STATE_RISK_REFRESH_ONLY", "STATE_FUNDAMENTAL_REVIEW_DUE",
    "STATE_MOMENTUM_REVIEW_DUE", "STATE_COMBINED_REVIEW_DUE", "STATE_FAST_REVIEW_DUE",
    "STATE_DATA_BLOCKED", "STATE_MANUAL_CONFIRMATION_REQUIRED",
    "REC_BUY", "REC_HOLD", "REC_REDUCE", "REC_EXIT", "REC_WAIT",
    "compute_operating_state", "compute_recommendations", "next_review_date", "blocked_model_notice",
]
