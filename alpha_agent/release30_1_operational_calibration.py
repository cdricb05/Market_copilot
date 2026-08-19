"""alpha_agent/release30_1_operational_calibration.py - Release 30.1 walk-forward
calibration of the CURRENT APPROVED operational model.

RESEARCH ONLY. This module answers exactly one question:

    "What historically realised forward excess-return distribution corresponds to
     the CURRENT APPROVED MODEL's own cross-sectional score?"

It introduces **no predictor family**. The score it calibrates is the approved
model ``fundamental_momentum_50_50_v1`` itself, reconstructed period by period
from the SAME owned inputs and the SAME construction the live owner uses
(``api.multi_horizon_engine.compute_scores`` -> ``compute_combined``): the fixed
50/50 blend of the within-common-universe percentile ranks of ``composite_sn``
and ``mom_6_1``. No weight is fitted, no feature is added, no component of the
Release-30 adaptive candidate is admitted, and ``s25_operating_profitability``
appears nowhere.

Only ONE number per horizon is estimated here - the slope that converts the
approved model's standardised score into an expected forward excess return -
together with the dispersion around it.

Why this module exists beside ``release30_forecast_research``
------------------------------------------------------------
Release 30 calibrated the operational lane on ``baseline_operational_blend_pit``,
a *structural reconstruction* of the approved model built from
``fcf_to_assets`` / ``operating_accruals`` / ``mom_6_1``, because it was solving
a different problem: putting a candidate and an incumbent on one comparable
scale. That proxy is not the approved model's score, and its measured 20-session
slope is NEGATIVE - which, applied through the forecast kernel, inverts the
approved model's ranking. Release 30.1 therefore calibrates the approved model's
ACTUAL score, and treats rank identity as a hard contract rather than an outcome.

Measurement discipline
----------------------
* **Common decision timestamp.** Every name in a cross-section is measured over
  the SAME forward window, starting at the decision session. An operational
  allocator commits capital at one timestamp; a calibration whose reliability
  depends on each name having its own private window start cannot be transported
  to that timestamp, and its "cross-sectional excess" would be taken over
  windows that do not overlap.
* **Owned survivorship-free prices.** Forward returns come from the owned
  Phase-24 daily panel (Norgate Russell 1000 Current & Past, delisted retained).
  A name that stops trading inside a forward window is measured to its LAST
  OWNED CLOSE and retained.
* **Strict walk-forward.** Blocks are contiguous and ordered, an embargo of
  ``ceil(horizon / step)`` decision dates separates the reserve from the
  validation block, and the slope is estimated on VALIDATION rows only. No
  random split, no hindsight, no current snapshot substituted into history.
* **Sign stability.** The slope is re-estimated under several declared fold
  geometries. A sign that depends on the analyst's arbitrary choice of geometry
  is not a calibration, and is reported as such.

Nothing here promotes a model, writes to an operational store, creates a signal,
target, proposal, decision or order, or imports ``paper_trader.api``.
"""
from __future__ import annotations

import collections
import csv
import hashlib
import json
import math
from pathlib import Path
from typing import Optional

import numpy as np

RELEASE = "release30_1"
RESEARCH_VERSION = "release30_1_operational_calibration.v1"
CALCULATION_OWNER = "alpha_agent.release30_1_operational_calibration"

#: The approved operational model this module calibrates - and may never change.
#: Declared as a literal so the research lane never imports the operational API
#: package; ``tests/test_release30_1_operational_calibration.py`` asserts it
#: still equals ``api.universe_scoring.PRIMARY_MODEL_ID``.
OPERATIONAL_MODEL_ID = "fundamental_momentum_50_50_v1"
OPERATIONAL_COMPONENTS = ("composite_sn", "mom_6_1")
OPERATIONAL_WEIGHTS = {"composite_sn": 0.5, "mom_6_1": 0.5}

#: The single feature name the forecast contract carries the approved model's own
#: score under. Identical to the Release-30 operational lane so the frozen
#: artifact stays loadable by the unchanged kernel.
OPERATIONAL_FEATURE = "operational_combined_score"

#: Owned input locations. Literals for the same reason as above; each mirrors a
#: canonical constant owned by ``api.multi_horizon_engine`` / the Phase-24 cache.
FUND_PANEL = Path(
    r"C:\Users\binis\Stock_Prediction_app_push\research\output"
    r"\phase10l_historical_sector_neutral_scored_panel_reconstruction"
    r"\historical_sector_neutral_scored_panel.csv")
MOM_PANEL = Path(
    r"D:\Stock_Prediction_app_data\phase25_multi_horizon_alpha\_inputs"
    r"\momentum_monthly_panel.csv")
PRICE_PANEL = Path(
    r"D:\Stock_Prediction_app_data\phase24_cache\daily_panel"
    r"\russell1000_cp_daily.npz")

#: Mirrors ``api.multi_horizon_engine.MIN_ADV_DOLLAR``.
MIN_ADV_DOLLAR = 1.0e7
#: A date with fewer eligible names is not a cross-section.
MIN_CROSS_SECTION = 50

#: Horizons, in trading sessions. Identical to ``engine.return_forecast.HORIZONS``.
HORIZONS = (5, 20, 60)
#: Decision dates are monthly, so one step is about 21 sessions.
STEP_SESSIONS = 21

#: The declared fold geometries, as (initial reserve dates, validation block,
#: step). The slope must keep ONE sign across all of them.
FOLD_GEOMETRIES = ((24, 6, 6), (30, 6, 6), (36, 12, 12))

#: The calibration contract. All three must hold or the horizon supplies no
#: expected return at all.
RANK_IDENTITY_MIN_SLOPE = 0.0
RELIABILITY_MIN_T = 2.0

CAL_CALIBRATED = "CALIBRATED"
CAL_NOT_CALIBRATED = "NOT_CALIBRATED"
RANK_IDENTITY_PRESERVED = "RANK_IDENTITY_PRESERVED"
RANK_IDENTITY_VIOLATED = "RANK_IDENTITY_VIOLATED"

REASON_SLOPE_NOT_POSITIVE = "SLOPE_NOT_POSITIVE_RANK_ORDER_WOULD_INVERT"
REASON_SIGN_UNSTABLE = "SLOPE_SIGN_NOT_STABLE_ACROSS_FOLD_GEOMETRIES"
REASON_NOT_RELIABLE = "SLOPE_NOT_DISTINGUISHABLE_FROM_ZERO"
REASON_NO_FOLDS = "INSUFFICIENT_DECISION_DATES_FOR_WALK_FORWARD"

SAFETY = ["RESEARCH ONLY", "READ ONLY", "NO ORDERS", "NO LIVE PROMOTION",
          "MANUAL REVIEW", "AUTOMATION OFF", "NO OPERATIONAL WRITE"]


# --------------------------------------------------------------------------- #
# small helpers
# --------------------------------------------------------------------------- #
def _f(x) -> Optional[float]:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _truthy(x) -> bool:
    return str(x).strip().lower() in ("1", "true", "yes")


def sha(obj) -> str:
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def file_fingerprint(path) -> dict:
    try:
        st = Path(path).stat()
        return {"path": str(path), "exists": True, "size_bytes": st.st_size,
                "mtime": int(st.st_mtime)}
    except OSError:
        return {"path": str(path), "exists": False}


def percentiles(values: dict) -> dict:
    """Cross-sectional percentile in [0, 1], best = 1.0.

    Reproduces ``api.multi_horizon_engine._percentiles`` EXACTLY, down to its tie
    handling: descending by value then ascending by ticker, strict ranks (ties
    are broken by ticker, never averaged), and ``pct = (n - rank) / (n - 1)``.
    The approved model's score is a blend of two of these, so any difference here
    would calibrate a model the operator does not run - which is the whole defect
    this release exists to close. ``tests/test_release30_1_operational_cutover``
    asserts the two functions agree, ties included.
    """
    items = sorted(values.items(), key=lambda kv: (-kv[1], kv[0]))
    n = len(items)
    if n == 0:
        return {}
    return {tk: (1.0 if n == 1 else (n - (i + 1)) / (n - 1))
            for i, (tk, _v) in enumerate(items)}


def rank_normalise(values: list) -> list:
    """Identical to ``engine.return_forecast.rank_normalise``."""
    n = len(values)
    out = [0.0] * n
    present = [(v, i) for i, v in enumerate(values) if _f(v) is not None]
    m = len(present)
    if m < 2:
        return out
    present.sort(key=lambda p: (float(p[0]), p[1]))
    i = 0
    while i < m:
        j = i
        while j + 1 < m and float(present[j + 1][0]) == float(present[i][0]):
            j += 1
        avg = (i + j) / 2.0
        for k in range(i, j + 1):
            out[present[k][1]] = (avg + 0.5) / m - 0.5
        i = j + 1
    return out


def standardise(scores: list) -> list:
    """Identical to ``engine.return_forecast.standardise``."""
    vals = [v for v in scores if _f(v) is not None]
    n = len(vals)
    if n < 2:
        return [0.0] * len(scores)
    mean = sum(vals) / n
    var = sum((v - mean) ** 2 for v in vals) / (n - 1)
    sd = math.sqrt(var) if var > 0 else 0.0
    if sd <= 0:
        return [0.0] * len(scores)
    return [(float(v) - mean) / sd for v in scores]


def newey_west_t(x, lags: int) -> float:
    a = np.asarray(list(x), dtype=np.float64)
    n = a.size
    if n < 3:
        return float("nan")
    d = a - a.mean()
    s = float((d * d).sum() / n)
    for lag in range(1, min(int(lags), n - 1) + 1):
        s += 2.0 * (1.0 - lag / (lags + 1.0)) * float(
            (d[lag:] * d[:-lag]).sum() / n)
    return float(a.mean() / math.sqrt(s / n)) if s > 0 else float("nan")


# --------------------------------------------------------------------------- #
# Owned input readers
# --------------------------------------------------------------------------- #
def load_fundamental_months(path=FUND_PANEL) -> dict:
    """month -> {ticker: composite_sn}, taking each ticker's LATEST row in the month.

    This is exactly ``api.multi_horizon_engine._load_fundamental_cross_section``
    generalised from "the latest month" to "every month", so a historical
    cross-section is built the same way the live one is.
    """
    out: dict = collections.defaultdict(dict)
    with open(path, "r", encoding="utf-8", newline="") as fh:
        for r in csv.DictReader(fh):
            d = (r.get("rebalance_date") or "").strip()
            if len(d) < 7:
                continue
            comp = _f(r.get("composite_sn"))
            if comp is None:
                continue
            tk = (r.get("ticker") or "").strip().upper()
            if not tk:
                continue
            month = out[d[:7]]
            cur = month.get(tk)
            if cur is None or d > cur[0]:
                month[tk] = (d, comp)
    return {k: {tk: v[1] for tk, v in rows.items()} for k, rows in out.items()}


def load_momentum_months(path=MOM_PANEL) -> tuple:
    """month -> {ticker: mom_6_1} plus month -> market_date, with the canonical
    momentum eligibility already applied (``_mom_eligibility``)."""
    rows: dict = collections.defaultdict(dict)
    mdate: dict = {}
    with open(path, "r", encoding="utf-8", newline="") as fh:
        for r in csv.DictReader(fh):
            month = (r.get("month") or "").strip()
            tk = (r.get("ticker") or "").strip().upper()
            if not month or not tk:
                continue
            mdate.setdefault(month, (r.get("market_date") or "").strip())
            mom = _f(r.get("mom_6_1"))
            if mom is None or not _truthy(r.get("is_member")):
                continue
            if not _truthy(r.get("eligible_history")):
                continue
            adv = _f(r.get("adv_dollar"))
            if adv is not None and adv < MIN_ADV_DOLLAR:
                continue
            rows[month][tk] = mom
    return dict(rows), mdate


class DailyPanel:
    """The owned survivorship-free daily panel."""

    def __init__(self, path=PRICE_PANEL):
        z = np.load(str(path), allow_pickle=True)
        self.dates = z["dates"]
        self.symbols = z["symbols"]
        self.close = z["close"].astype(np.float64)
        self.col = {str(s): i for i, s in enumerate(self.symbols)}
        self.source = file_fingerprint(path)

    def index_at_or_before(self, iso: str) -> Optional[int]:
        if not iso:
            return None
        i = int(np.searchsorted(self.dates, np.datetime64(str(iso)[:10], "D"),
                                side="right")) - 1
        return i if i >= 0 else None

    def forward_return(self, t: int, ticker: str, horizon: int) -> Optional[float]:
        """Forward total return over ``horizon`` sessions from ``t``.

        A name that stops trading inside the window is measured to its LAST OWNED
        CLOSE and retained - never dropped as if it had never existed, and never
        carried forward at a stale price beyond the window.
        """
        c = self.col.get(ticker)
        if c is None:
            return None
        p0 = self.close[t, c]
        if not np.isfinite(p0) or p0 <= 0:
            return None
        hi = min(t + int(horizon), self.close.shape[0] - 1)
        seg = self.close[t + 1:hi + 1, c]
        fin = seg[np.isfinite(seg)]
        return float(fin[-1] / p0 - 1.0) if fin.size else None


# --------------------------------------------------------------------------- #
# Historical reconstruction of the APPROVED model
# --------------------------------------------------------------------------- #
def reconstruct_history(*, panel=None, horizons=HORIZONS) -> dict:
    """Rebuild ``fundamental_momentum_50_50_v1`` at every owned decision date.

    Returns the ordered decision-date sections, each carrying the approved
    model's own standardised score and the forward EXCESS return - the forward
    total return minus the equal-weight mean of the same eligible cross-section -
    at every horizon.
    """
    panel = panel or DailyPanel()
    fund = load_fundamental_months()
    mom, mdate = load_momentum_months()
    months = sorted(set(fund) & set(mom))

    sections: list = []
    skipped: dict = collections.Counter()
    for month in months:
        fvals, mvals = fund[month], mom[month]
        common = sorted(set(fvals) & set(mvals))
        if len(common) < MIN_CROSS_SECTION:
            skipped["CROSS_SECTION_TOO_SMALL"] += 1
            continue
        fpct = percentiles({tk: fvals[tk] for tk in common})
        mpct = percentiles({tk: mvals[tk] for tk in common})
        combined = {tk: (OPERATIONAL_WEIGHTS["composite_sn"] * fpct[tk]
                         + OPERATIONAL_WEIGHTS["mom_6_1"] * mpct[tk])
                    for tk in common}
        t = panel.index_at_or_before(mdate.get(month) or "")
        if t is None:
            skipped["NO_PANEL_SESSION"] += 1
            continue
        score = dict(zip(common, standardise(
            rank_normalise([combined[tk] for tk in common]))))
        forward: dict = {}
        for h in horizons:
            row = {}
            for tk in common:
                r = panel.forward_return(t, tk, h)
                if r is not None:
                    row[tk] = r
            forward[int(h)] = row
        sections.append({"month": month, "decision_date": str(panel.dates[t]),
                         "session_index": int(t), "n_names": len(common),
                         "combined": combined, "score": score,
                         "forward": forward})
    return {"sections": sections, "n_sections": len(sections),
            "skipped": dict(skipped),
            "first_date": sections[0]["decision_date"] if sections else None,
            "last_date": sections[-1]["decision_date"] if sections else None,
            "sources": {"fundamental_panel": file_fingerprint(FUND_PANEL),
                        "momentum_panel": file_fingerprint(MOM_PANEL),
                        "price_panel": dict(panel.source)}}


# --------------------------------------------------------------------------- #
# Walk-forward calibration
# --------------------------------------------------------------------------- #
def validation_dates(n: int, horizon: int, geometry) -> list:
    """Contiguous, ordered validation blocks with an embargo. Never a random split."""
    initial, valid, step = geometry
    embargo = int(math.ceil(float(horizon) / STEP_SESSIONS))
    out: list = []
    lo = int(initial)
    while lo + embargo + valid <= n:
        out.extend(range(lo + embargo, lo + embargo + valid))
        lo += int(step)
    return sorted(set(out))


def _fit_on(sections: list, idx, horizon: int) -> dict:
    """Pooled slope, per-date slope series and residual dispersion on given dates."""
    pooled_p, pooled_y, per_date = [], [], []
    for k in idx:
        s = sections[k]
        d = s["forward"].get(int(horizon)) or {}
        tks = [t for t in sorted(s["combined"]) if t in d]
        if len(tks) < MIN_CROSS_SECTION:
            continue
        mu = sum(d[t] for t in tks) / len(tks)
        p = np.array([s["score"][t] for t in tks], dtype=np.float64)
        y = np.array([d[t] - mu for t in tks], dtype=np.float64)
        pooled_p.append(p)
        pooled_y.append(y)
        den = float((p * p).sum())
        if den > 0:
            per_date.append(float((p * y).sum() / den))
    if not pooled_p:
        return {}
    pc, yc = np.concatenate(pooled_p), np.concatenate(pooled_y)
    den = float((pc * pc).sum())
    slope = float((pc * yc).sum() / den) if den > 0 else 0.0
    resid = yc - slope * pc
    return {"slope": slope, "n_rows": int(pc.size), "n_dates": len(per_date),
            "per_date_slope": per_date,
            "residual_sigma": float(resid.std(ddof=1)),
            "residual_q05": float(np.quantile(resid, 0.05))}


def calibrate_horizon(history: dict, horizon: int) -> dict:
    """The Release-30.1 calibration contract for ONE horizon.

    A horizon is CALIBRATED only if the walk-forward slope is positive (rank
    identity), keeps that sign under every declared fold geometry, and is
    distinguishable from zero. Otherwise it supplies no expected return at all -
    the honest alternative to a mapping that would silently re-rank the approved
    model, or dress noise as an economic forecast.
    """
    sections = history["sections"]
    n = len(sections)
    primary = FOLD_GEOMETRIES[0]
    geometries: dict = {}
    for g in FOLD_GEOMETRIES:
        vdates = validation_dates(n, horizon, g)
        fit = _fit_on(sections, vdates, horizon) if vdates else {}
        geometries["%d_%d_%d" % g] = {
            "geometry": {"initial_reserve_dates": g[0],
                         "validation_block": g[1], "step": g[2]},
            "embargo_dates": int(math.ceil(float(horizon) / STEP_SESSIONS)),
            "validation_dates": len(vdates),
            "slope": fit.get("slope"), "n_rows": fit.get("n_rows"),
        }
    base = _fit_on(sections, validation_dates(n, horizon, primary), horizon)
    out = {
        "horizon_sessions": int(horizon),
        "basis": "WALK_FORWARD_VALIDATION_BLOCKS_COMMON_DECISION_TIMESTAMP",
        "expected_excess_return_formula": "slope * standardised_score",
        "expected_return_units": "DECIMAL_FRACTION_OF_CAPITAL_OVER_THE_HORIZON",
        "downside_quantile": 0.05,
        "primary_geometry": "%d_%d_%d" % primary,
        "fold_geometries": geometries,
        "rank_identity_contract": (
            "the approved model's ranking is the investment thesis; a mapping "
            "that is not non-decreasing in its score is refused, never flipped"),
    }
    if not base:
        out.update({"state": CAL_NOT_CALIBRATED, "rank_identity": None,
                    "slope": None, "reasons": [REASON_NO_FOLDS]})
        return out

    slopes = [g["slope"] for g in geometries.values() if g["slope"] is not None]
    sign_stable = bool(slopes) and (all(s > 0 for s in slopes)
                                    or all(s < 0 for s in slopes))
    lags = max(1, int(math.ceil(float(horizon) / STEP_SESSIONS)))
    t_stat = newey_west_t(base["per_date_slope"], lags)
    positive_dates = sum(1 for s in base["per_date_slope"] if s > 0)

    reasons: list = []
    if not base["slope"] > RANK_IDENTITY_MIN_SLOPE:
        reasons.append(REASON_SLOPE_NOT_POSITIVE)
    if not sign_stable:
        reasons.append(REASON_SIGN_UNSTABLE)
    if not (math.isfinite(t_stat) and t_stat >= RELIABILITY_MIN_T):
        reasons.append(REASON_NOT_RELIABLE)

    out.update({
        "state": CAL_CALIBRATED if not reasons else CAL_NOT_CALIBRATED,
        "rank_identity": (RANK_IDENTITY_PRESERVED
                          if base["slope"] > RANK_IDENTITY_MIN_SLOPE
                          else RANK_IDENTITY_VIOLATED),
        "reasons": reasons,
        "measured_slope": base["slope"],
        "slope": base["slope"] if not reasons else None,
        "n_rows": base["n_rows"],
        "validation_dates": base["n_dates"],
        "positive_validation_dates": positive_dates,
        "per_date_slope_mean": (float(np.mean(base["per_date_slope"]))
                                if base["per_date_slope"] else None),
        "newey_west_t": None if not math.isfinite(t_stat) else float(t_stat),
        "newey_west_lags": lags,
        "reliability_min_t": RELIABILITY_MIN_T,
        "slope_sign_stable_across_geometries": sign_stable,
        "residual_sigma": base["residual_sigma"],
        "residual_q05": base["residual_q05"],
    })
    return out


# --------------------------------------------------------------------------- #
# Frozen artifact
# --------------------------------------------------------------------------- #
def build_artifact(history: Optional[dict] = None, *, horizons=HORIZONS) -> dict:
    """The frozen Release-30.1 operational forecast artifact.

    Shape-compatible with ``engine.return_forecast``: one feature, a rank-blend
    of weight 1.0 on the approved model's own score, so the kernel applies the
    identity on the approved ranking and the slope is the ONLY thing that could
    change it.
    """
    hist = history if history is not None else reconstruct_history(horizons=horizons)
    blocks: dict = {}
    for h in horizons:
        cal = calibrate_horizon(hist, int(h))
        blocks[str(int(h))] = {
            "horizon_sessions": int(h),
            "model": {"kind": "rank_blend", "weights": {OPERATIONAL_FEATURE: 1.0}},
            "member_ids": [OPERATIONAL_FEATURE],
            "weights": {OPERATIONAL_FEATURE: 1.0},
            "weighting_method": "FROZEN_OPERATIONAL_CHAMPION_NO_FITTING",
            "calibration": cal,
            "training_cutoff": hist["last_date"],
        }
    calibrated = sorted(int(k) for k, v in blocks.items()
                        if v["calibration"].get("state") == CAL_CALIBRATED)
    art = {
        "contract": "release30_forecast_model/1",
        "research_version": RESEARCH_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "universe_tag": "operational_v2",
        "operational_model_id": OPERATIONAL_MODEL_ID,
        "operational_model_components": list(OPERATIONAL_COMPONENTS),
        "operational_model_weights": dict(OPERATIONAL_WEIGHTS),
        "feature_names": [OPERATIONAL_FEATURE],
        "feature_transform": "PER_DATE_RANK_TO_MINUS_HALF_PLUS_HALF_MISSING_ZERO",
        "target": "FORWARD_EXCESS_RETURN_VS_CROSS_SECTIONAL_MEAN",
        "activation": "CURRENT_OPERATIONAL_MODEL",
        "model_identity_contract": "APPROVED_MODEL_RANKING_IS_PRESERVED",
        "horizons": blocks,
        "calibrated_horizons": calibrated,
        "calibration_state": CAL_CALIBRATED if calibrated else CAL_NOT_CALIBRATED,
        "history": {"decision_dates": hist["n_sections"],
                    "first_date": hist["first_date"],
                    "last_date": hist["last_date"],
                    "skipped": hist["skipped"],
                    "sources": hist["sources"]},
        "point_in_time_controls": [
            "the approved model is reconstructed from the SAME owned inputs and "
            "the SAME construction the live owner uses; no weight is fitted",
            "every cross-section reads only rows visible at its own decision month",
            "forward returns start at ONE common decision session for the whole "
            "cross-section, never at a per-name window start",
            "forward returns come from the owned survivorship-free daily panel; "
            "a delisted name is measured to its last owned close and retained",
            "validation blocks are contiguous and ordered with an embargo; there "
            "is no random split and no current snapshot placed into history",
        ],
        "no_new_predictor_family": True,
        "adaptive_candidate_components_admitted": [],
        "automatic_promotion_allowed": False,
        "safety": list(SAFETY),
    }
    art["model_spec_hash"] = sha({k: v for k, v in art.items()
                                  if k not in ("model_spec_hash", "history")})
    return art
