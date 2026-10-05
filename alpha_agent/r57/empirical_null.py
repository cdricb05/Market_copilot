r"""alpha_agent.r57.empirical_null - empirical-null calibration of a mass discovery screen.

RESEARCH ONLY. Pure numpy/pandas statistics; reads no store, writes no file.

WHY (R93 finding, R94 owner)
----------------------------
R93 screened ~53,000 relationships with the analytic Newey-West t of a book's
net return on OVERLAPPING stride-1 decisions of h-session forward returns and
Benjamini-Hochberg on the analytic p. With the targets deliberately misaligned
by >= 1 year the screen produced the SAME exceedance rates (|t| >= 2: 19.4%
null vs 18.8% real; BH q <= 0.20: 595 null vs 573 real). The analytic t is
therefore not a valid selection statistic in this estate, and the director
retired it: no mass screen may select a candidate until the statistic is
compared with an EMPIRICAL null produced by the identical pipeline.

This module is that calibration layer. It owns three frozen objects:

    NULL      shift_targets()      the forward-return panel is circularly shifted
                                   INSIDE the decision span by ONE offset common to
                                   every market and every horizon (>= min_offset
                                   sessions). Every marginal distribution, every
                                   autocorrelation, every cross-sectional
                                   correlation, the overlap, the missing-data
                                   pattern, the universe, the costs and the
                                   decision cadence are kept; only the alignment
                                   of predictor and target is destroyed. The
                                   caller runs the SAME screen code on the
                                   shifted targets; weights and costs are then
                                   byte-identical between real and null
                                   (cost depends on weights only), which the
                                   caller must assert.
    P-VALUE   empirical_p()        within a stratum (same engine, panel, book,
                                   horizon), p = (1 + #null >= s) / (1 + n_null):
                                   the share of the pooled null statistics at or
                                   above the row's own statistic, floored at
                                   1 / (n_null + 1).
    FDR       empirical_fdr_q()    the count-based q-value (Tusher/Storey style):
                                   FDR(c) = max(1, N_null(c)) / n_seeds / R(c)
                                   where N_null(c) counts pooled null rows >= c,
                                   R(c) counts real rows >= c, and the max(1, .)
                                   floor refuses to call a lone real row that
                                   merely exceeds the null maximum a discovery
                                   (its q is then 1 / n_seeds). q(row) is the
                                   running minimum of FDR over every threshold
                                   at or below the row's statistic.

A promotion rule built on these (R94: q <= 0.05 for a frozen validation
candidate; empirical p <= 0.01 for the shortlist) must be written and hashed
BEFORE the real screen is read (freeze_thresholds -> canonical_hash), and the
null itself must be built from discovery data only. This module never looks at
validation or lockbox data: it receives arrays, nothing else.

NOT A SECOND FRAMEWORK: the D/V/L verdict of a frozen candidate stays with the
canonical gate (alpha_agent.r59 + agents_v2.runner on non-overlapping books).
This layer only decides which discovery rows may ASK for that verdict.
"""
from __future__ import annotations

import hashlib
import json

import numpy as np
import pandas as pd

__all__ = ["shift_targets", "empirical_p", "empirical_fdr_q", "stratum_null_summary",
           "calibrate_frame", "freeze_thresholds", "canonical_hash",
           "dedupe_identical_statistics", "calibrate_two_nulls",
           "NULL_MODE_COMMON", "NULL_MODE_PER_MARKET", "SIDED_TWO", "SIDED_ONE"]

NULL_MODE_COMMON = "COMMON_OFFSET"
NULL_MODE_PER_MARKET = "PER_MARKET_OFFSET"
SIDED_TWO = "TWO_SIDED_ABS"
SIDED_ONE = "ONE_SIDED_POSITIVE"


# --------------------------------------------------------------------------- #
# The null
# --------------------------------------------------------------------------- #
def shift_targets(targets: dict, lo: int, hi: int, seed: int, *, min_offset: int = 260,
                  mode: str = NULL_MODE_COMMON) -> tuple:
    """Return (shifted targets, meta).

    ``targets`` maps horizon -> float array [T, N] (NaN where undefined). Rows
    ``lo .. hi-h`` of horizon h (the span where a target is defined inside the
    decision window) are circularly rolled by an offset drawn ONCE per seed
    from ``[min_offset, span - min_offset]`` where span uses the shortest
    valid block (largest horizon), so every horizon and every market is
    misaligned by the same amount (COMMON_OFFSET). PER_MARKET_OFFSET draws one
    offset per market column (R93's construction; it breaks the
    cross-sectional structure of the target panel and is kept only for
    comparison). Nothing outside the block is touched; no NaN is created.
    """
    if mode not in (NULL_MODE_COMMON, NULL_MODE_PER_MARKET):
        raise ValueError("unknown null mode %r" % mode)
    hs = sorted(int(h) for h in targets)
    if not hs:
        raise ValueError("no targets")
    span_min = int(hi - max(hs) - lo)
    if span_min < 2 * min_offset + 1:
        raise ValueError("decision span %d too short for min_offset %d" % (span_min, min_offset))
    rng = np.random.default_rng(int(seed))
    n_cols = int(next(iter(targets.values())).shape[1])
    if mode == NULL_MODE_COMMON:
        offsets = np.full(n_cols, int(rng.integers(min_offset, span_min - min_offset + 1)))
    else:
        offsets = rng.integers(min_offset, span_min - min_offset + 1, size=n_cols)
    out = {}
    for h in hs:
        f = np.array(targets[h], dtype=np.float64, copy=True)
        a, b = int(lo), int(hi - h)
        blk = f[a:b]
        for j in range(n_cols):
            blk[:, j] = np.roll(blk[:, j], int(offsets[j]))
        f[a:b] = blk
        out[h] = f
    meta = {"mode": mode, "seed": int(seed), "min_offset": int(min_offset), "block_lo": int(lo),
            "block_hi_exclusive_per_h": {int(h): int(hi - h) for h in hs}, "span_min": span_min,
            "offset_common": int(offsets[0]) if mode == NULL_MODE_COMMON else None,
            "offset_per_market_min_max": [int(offsets.min()), int(offsets.max())]}
    return out, meta


# --------------------------------------------------------------------------- #
# Statistics
# --------------------------------------------------------------------------- #
def _stat(t: np.ndarray, sided: str) -> np.ndarray:
    t = np.asarray(t, dtype=np.float64)
    if sided == SIDED_TWO:
        return np.abs(t)
    if sided == SIDED_ONE:
        return t
    raise ValueError("unknown sidedness %r" % sided)


def empirical_p(stat: np.ndarray, null_stat: np.ndarray) -> np.ndarray:
    """(1 + #null >= s) / (1 + n_null) per real row; NaN where s is NaN."""
    s = np.asarray(stat, dtype=np.float64)
    ns = np.sort(np.asarray(null_stat, dtype=np.float64)[np.isfinite(null_stat)])
    n = len(ns)
    out = np.full(len(s), np.nan)
    fin = np.isfinite(s)
    if n == 0:
        return out
    ge = n - np.searchsorted(ns, s[fin], side="left")
    out[fin] = (1.0 + ge) / (1.0 + n)
    return out


def empirical_fdr_q(stat: np.ndarray, null_stat: np.ndarray, n_seeds: int) -> np.ndarray:
    """Count-based q-value per real row (see module docstring). NaN rows get NaN."""
    s = np.asarray(stat, dtype=np.float64)
    ns = np.sort(np.asarray(null_stat, dtype=np.float64)[np.isfinite(null_stat)])
    out = np.full(len(s), np.nan)
    fin = np.where(np.isfinite(s))[0]
    if not len(fin) or not len(ns) or int(n_seeds) <= 0:
        return out
    sv = s[fin]
    order = np.argsort(-sv, kind="mergesort")          # descending statistic
    sd = sv[order]
    # R(c): every real row with statistic >= c, ties included (-sd is ascending)
    real_ge = np.searchsorted(-sd, -sd, side="right").astype(np.float64)
    real_ge = np.where(real_ge <= 0, 1.0, real_ge)
    null_ge = (len(ns) - np.searchsorted(ns, sd, side="left")).astype(np.float64)
    fdr = np.maximum(null_ge, 1.0) / float(n_seeds) / real_ge
    q = np.minimum.accumulate(fdr[::-1])[::-1]         # min over thresholds at or below the row
    q = np.clip(q, 0.0, 1.0)
    res = np.empty(len(sd))
    res[order] = q
    out[fin] = res
    return out


def stratum_null_summary(null_stat: np.ndarray, seeds: np.ndarray) -> dict:
    s = np.asarray(null_stat, dtype=np.float64)
    seeds = np.asarray(seeds)
    fin = np.isfinite(s)
    s, seeds = s[fin], seeds[fin]
    if not len(s):
        return {"null_rows": 0}
    per_seed_max = {str(k): float(s[seeds == k].max()) for k in np.unique(seeds)}
    return {"null_rows": int(len(s)), "null_seeds": sorted(per_seed_max), "n_seeds": int(len(per_seed_max)),
            "null_mean": float(s.mean()), "null_q90": float(np.quantile(s, 0.90)), "null_q95": float(np.quantile(s, 0.95)),
            "null_q99": float(np.quantile(s, 0.99)), "null_q999": float(np.quantile(s, 0.999)), "null_max": float(s.max()),
            "null_per_seed_max": per_seed_max, "null_min_of_seed_max": float(min(per_seed_max.values())),
            "null_rate_ge_2": float((s >= 2).mean()), "null_rate_ge_2_5": float((s >= 2.5).mean()), "null_rate_ge_3": float((s >= 3).mean())}


# --------------------------------------------------------------------------- #
# Frozen thresholds and the calibrated frame
# --------------------------------------------------------------------------- #
def canonical_hash(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()


def _key(df: pd.DataFrame, strata: list) -> pd.Series:
    parts = [df[c].astype(str) for c in strata]
    k = parts[0]
    for p in parts[1:]:
        k = k + "|" + p
    return k


def freeze_thresholds(null: pd.DataFrame, *, strata: list, stat_col: str, seed_col: str, sided: str,
                      fdr_promote: float, p_shortlist: float, rule_text: str) -> dict:
    """The null summary per stratum plus the selection RULE, hashed. Reads ONLY null rows.
    Must be written to disk before any real row is read by calibrate_frame."""
    null = null.copy()
    null["_s"] = _stat(null[stat_col].values, sided)
    null["_k"] = _key(null, strata)
    per = {}
    for k, g in null.groupby("_k"):
        per[k] = stratum_null_summary(g["_s"].values, g[seed_col].values)
    body = {"strata": list(strata), "stat_col": stat_col, "sided": sided, "seed_col": seed_col,
            "rule": {"promote_if_empirical_fdr_q_le": float(fdr_promote), "shortlist_if_empirical_p_le": float(p_shortlist),
                     "fdr_estimator": "max(1, N_null(c)) / n_seeds / R(c); q = running min over thresholds at or below the row",
                     "p_estimator": "(1 + #null >= s) / (1 + n_null) within stratum", "text": rule_text},
            "null_rows_total": int(len(null)), "seeds": sorted(str(x) for x in null[seed_col].unique()),
            "per_stratum": per}
    body["sha256"] = canonical_hash({k: v for k, v in body.items() if k != "sha256"})
    return body


def calibrate_frame(real: pd.DataFrame, null: pd.DataFrame, thresholds: dict) -> pd.DataFrame:
    """Attach empirical_p / empirical_fdr_q / above_null_max / shortlist_null / promote_null to
    every real row using the FROZEN thresholds record (its hash is verified against the null)."""
    strata, stat_col, seed_col, sided = thresholds["strata"], thresholds["stat_col"], thresholds["seed_col"], thresholds["sided"]
    chk = dict(thresholds)
    chk.pop("sha256", None)
    if canonical_hash(chk) != thresholds.get("sha256"):
        raise ValueError("thresholds record does not match its own hash")
    null = null.copy()
    null["_s"] = _stat(null[stat_col].values, sided)
    null["_k"] = _key(null, strata)
    n_seeds_total = len(thresholds["seeds"])
    out = real.copy()
    out["_s"] = _stat(out[stat_col].values, sided)
    out["_k"] = _key(out, strata)
    for c in ("empirical_p", "empirical_fdr_q", "null_q99", "null_max", "null_min_of_seed_max"):
        out[c] = np.nan
    out["null_rows"] = 0
    out["null_seeds"] = 0
    out["above_null_max_all_seeds"] = False
    for k, g in out.groupby("_k"):
        ng = null[null["_k"] == k]
        per = thresholds["per_stratum"].get(k)
        if per is None or not len(ng):
            continue
        ns = ng["_s"].values
        n_seeds = int(per.get("n_seeds", ng[seed_col].nunique()))
        out.loc[g.index, "empirical_p"] = empirical_p(g["_s"].values, ns)
        out.loc[g.index, "empirical_fdr_q"] = empirical_fdr_q(g["_s"].values, ns, n_seeds)
        out.loc[g.index, "null_q99"] = per["null_q99"]
        out.loc[g.index, "null_max"] = per["null_max"]
        out.loc[g.index, "null_min_of_seed_max"] = per["null_min_of_seed_max"]
        out.loc[g.index, "null_rows"] = int(per["null_rows"])
        out.loc[g.index, "null_seeds"] = n_seeds
        out.loc[g.index, "above_null_max_all_seeds"] = g["_s"].values > float(per["null_max"])
    rule = thresholds["rule"]
    out["null_calibrated"] = out["null_rows"] > 0
    out["shortlist_null"] = out["null_calibrated"] & (out["empirical_p"] <= float(rule["shortlist_if_empirical_p_le"]))
    out["promote_null"] = out["null_calibrated"] & (out["empirical_fdr_q"] <= float(rule["promote_if_empirical_fdr_q_le"]))
    out["null_thresholds_sha256"] = thresholds["sha256"]
    out["null_stratum"] = out["_k"]
    out["calibration_stat"] = out["_s"]
    return out.drop(columns=["_s", "_k"])


# --------------------------------------------------------------------------- #
# Amended-rule PROTOTYPE (R94 director defects D2 and D5). NOT applied to any R94
# selection: the R94 rule was frozen before its screens ran. Offered so that the
# human decision on the amended rule can be taken on working, tested mechanics.
# --------------------------------------------------------------------------- #
def dedupe_identical_statistics(df: pd.DataFrame, *, strata: list, stat_col: str, extra_cols: tuple = ("net_ann", "cost_ann", "n_decisions"),
                                decimals: int = 10) -> pd.DataFrame:
    """D2: rows whose statistic AND bookkeeping columns are byte-identical inside a stratum are one
    book under several names (rank-identical features). Keep the first, mark the rest
    ``duplicate_of`` so R(c) and N_null(c) count each book once. Applied to real and null alike."""
    out = df.copy()
    cols = [c for c in extra_cols if c in out.columns]
    key = _key(out, strata)
    for c in [stat_col] + cols:
        key = key + "|" + out[c].astype(float).round(decimals).astype(str)
    out["_dkey"] = key
    first = out.groupby("_dkey", sort=False).cumcount() == 0
    out["duplicate_of"] = np.where(first, "", out.groupby("_dkey")["_dkey"].transform("first"))
    out["is_duplicate_book"] = ~first
    return out.drop(columns=["_dkey"])


def calibrate_two_nulls(real: pd.DataFrame, null_a: pd.DataFrame, thr_a: dict, null_b: pd.DataFrame, thr_b: dict,
                        *, dedupe: bool = True) -> pd.DataFrame:
    """D5: a row is promoted only if it passes the frozen rule under BOTH null constructions (for example
    COMMON_OFFSET and PER_MARKET_OFFSET); its reported q is the larger of the two. With ``dedupe`` the
    identical-statistic books are collapsed first (D2) in the real frame and in both null frames."""
    strata, stat_col = thr_a["strata"], thr_a["stat_col"]
    if dedupe:
        real = dedupe_identical_statistics(real, strata=strata, stat_col=stat_col)
        real = real[~real["is_duplicate_book"]]
        null_a = dedupe_identical_statistics(null_a, strata=strata + [thr_a["seed_col"]], stat_col=stat_col)
        null_a = null_a[~null_a["is_duplicate_book"]]
        null_b = dedupe_identical_statistics(null_b, strata=strata + [thr_b["seed_col"]], stat_col=stat_col)
        null_b = null_b[~null_b["is_duplicate_book"]]
        thr_a = freeze_thresholds(null_a, strata=strata, stat_col=stat_col, seed_col=thr_a["seed_col"], sided=thr_a["sided"],
                                  fdr_promote=thr_a["rule"]["promote_if_empirical_fdr_q_le"], p_shortlist=thr_a["rule"]["shortlist_if_empirical_p_le"], rule_text=thr_a["rule"]["text"] + " [dedup]")
        thr_b = freeze_thresholds(null_b, strata=strata, stat_col=stat_col, seed_col=thr_b["seed_col"], sided=thr_b["sided"],
                                  fdr_promote=thr_b["rule"]["promote_if_empirical_fdr_q_le"], p_shortlist=thr_b["rule"]["shortlist_if_empirical_p_le"], rule_text=thr_b["rule"]["text"] + " [dedup]")
    a = calibrate_frame(real, null_a, thr_a)
    b = calibrate_frame(real, null_b, thr_b)
    out = a.copy()
    out["empirical_fdr_q_null_a"] = a["empirical_fdr_q"].values
    out["empirical_fdr_q_null_b"] = b["empirical_fdr_q"].values
    out["empirical_fdr_q"] = np.fmax(a["empirical_fdr_q"].values, b["empirical_fdr_q"].values)
    out["empirical_p"] = np.fmax(a["empirical_p"].values, b["empirical_p"].values)
    out["promote_null"] = a["promote_null"].values & b["promote_null"].values
    out["shortlist_null"] = a["shortlist_null"].values & b["shortlist_null"].values
    out["null_thresholds_sha256"] = a["null_thresholds_sha256"].astype(str) + "+" + b["null_thresholds_sha256"].astype(str)
    out["two_null_rule"] = True
    return out
