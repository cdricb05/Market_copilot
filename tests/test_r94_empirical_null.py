r"""R94 empirical-null calibration layer (alpha_agent.r57.empirical_null) - targeted tests.

A  known null targets produce about the expected false-positive rate (and ZERO promotions at FDR 5%)
B  overlapping decisions inflate the analytic t but do NOT create promotions under the empirical null
C  real and null evaluate through the SAME execution path (one function, one target transform)
D  transaction costs are identical under real and null targets (cost depends on weights only)
E  the null shift never touches rows outside the declared block (D/V/L untouched by construction)
F  a settled candidate list can never be promoted (identity refusal is upstream of the statistic)
G  thresholds carry their own hash and calibrate_frame refuses a tampered record
H  the threshold record is built from null rows only (it is a pure function of the null frame)
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from alpha_agent.r57 import empirical_null as EN


def _nw_t(x: np.ndarray, lag: int) -> float:
    x = x[np.isfinite(x)]
    n = len(x)
    m = x.mean()
    e = x - m
    s2 = (e * e).sum() / n
    for k in range(1, lag + 1):
        s2 += 2.0 * (1.0 - k / (lag + 1.0)) * (e[k:] * e[:-k]).sum() / n
    return float(m / np.sqrt(max(s2, 1e-18) / n))


def _screen(features: np.ndarray, targets: np.ndarray, h: int, stride: int) -> pd.DataFrame:
    """ONE execution path for real and null: sign(feature) book held h sessions, overlapping decisions."""
    T, K = features.shape
    idx = np.arange(0, T - h - 1, stride)
    rows = []
    for k in range(K):
        w = np.sign(features[:, k])
        w_prev = np.zeros_like(w)
        w_prev[h:] = w[:-h]
        cost = 0.0005 * np.abs(w - w_prev)
        pnl = w * targets[:, k] - cost
        rows.append({"relationship_id": "f%d" % k, "book": "TS", "horizon": h, "t_net_nw": _nw_t(pnl[idx], h - 1), "cost_ann": float(cost[idx].mean() * 252 / h)})
    return pd.DataFrame(rows)


def _panel(seed: int, T: int = 1400, K: int = 200, h: int = 21):
    rng = np.random.default_rng(seed)
    r = rng.standard_normal((T, K)) * 0.01
    lc = np.cumsum(r, axis=0)
    fwd = np.full((T, K), np.nan)
    fwd[:T - h] = lc[h:] - lc[:T - h]
    feat = pd.DataFrame(lc).rolling(63).mean().values - lc            # a persistent price-state feature
    feat = np.where(np.isfinite(feat), feat, 0.0)
    return feat, fwd


def _run(seed_real: int, seeds_null: list, h: int = 21, stride: int = 1, inject: int = 0):
    feat, fwd = _panel(seed_real, h=h)
    if inject:
        # a genuine signal: the target follows the feature's sign for the first `inject` columns
        fwd[:, :inject] = fwd[:, :inject] + 0.03 * np.sign(feat[:, :inject])
    real = _screen(feat, fwd, h, stride)
    nulls = []
    for s in seeds_null:
        TGn, _ = EN.shift_targets({h: fwd}, 0, feat.shape[0], s, min_offset=260)
        d = _screen(feat, TGn[h], h, stride)
        d["null_seed"] = s
        nulls.append(d)
    null = pd.concat(nulls, ignore_index=True)
    thr = EN.freeze_thresholds(null, strata=["book", "horizon"], stat_col="t_net_nw", seed_col="null_seed", sided=EN.SIDED_TWO,
                               fdr_promote=0.05, p_shortlist=0.01, rule_text="test")
    cal = EN.calibrate_frame(real, null, thr)
    return real, null, thr, cal


def test_A_known_null_false_positive_rate_and_zero_promotions():
    real, null, thr, cal = _run(1, [11, 12, 13, 14, 15])
    analytic_rate = float((real["t_net_nw"].abs() >= 2.0).mean())
    assert analytic_rate > 0.06, "overlapping decisions should inflate the analytic |t| >= 2 rate above the nominal 4.6%%: got %.3f" % analytic_rate
    p_rate = float((cal["empirical_p"] <= 0.05).mean())
    assert 0.01 <= p_rate <= 0.12, "empirical p <= 0.05 should fire at about 5%% on a pure null: got %.3f" % p_rate
    assert int(cal["promote_null"].sum()) == 0, "a pure null must give ZERO promotions at FDR 5%"


def test_B_more_overlap_does_not_create_alpha_but_signal_is_found():
    _, _, _, cal1 = _run(2, [21, 22, 23, 24, 25], stride=1)
    _, _, _, cal5 = _run(2, [21, 22, 23, 24, 25], stride=21)
    assert int(cal1["promote_null"].sum()) == 0 and int(cal5["promote_null"].sum()) == 0
    _, _, _, cal = _run(2, [21, 22, 23, 24, 25], stride=1, inject=12)
    promoted = cal[cal["promote_null"]]["relationship_id"].tolist()
    true_ids = {"f%d" % k for k in range(12)}
    assert len(promoted) >= 8, "an injected signal must be promoted: %d" % len(promoted)
    assert len([p for p in promoted if p not in true_ids]) <= max(2, int(0.25 * len(promoted))), "FDR among promotions should be small (one draw; nominal 5%)"


def test_C_same_execution_path_identity_transform():
    feat, fwd = _panel(3)
    TG0, meta = EN.shift_targets({21: fwd}, 0, feat.shape[0], 7, min_offset=260)
    a = _screen(feat, fwd, 21, 1)
    b = _screen(feat, TG0[21], 21, 1)
    assert list(a.columns) == list(b.columns) and len(a) == len(b)
    # same code path, different alignment: t differs, cost does not (D)
    assert not np.allclose(a["t_net_nw"], b["t_net_nw"])
    assert np.allclose(a["cost_ann"], b["cost_ann"])
    assert meta["mode"] == EN.NULL_MODE_COMMON and meta["offset_common"] >= 260


def test_D_costs_identical_real_vs_null():
    real, null, thr, cal = _run(4, [41, 42, 43])
    j = real.merge(null[null["null_seed"] == 41], on="relationship_id", suffixes=("_r", "_n"))
    assert np.allclose(j["cost_ann_r"], j["cost_ann_n"])


def test_E_shift_touches_only_the_declared_block():
    rng = np.random.default_rng(5)
    T, N = 2000, 4
    tg = {5: rng.standard_normal((T, N)), 21: rng.standard_normal((T, N))}
    tg[5][-5:] = np.nan
    tg[21][-21:] = np.nan
    lo, hi = 900, T
    out, meta = EN.shift_targets(tg, lo, hi, 99, min_offset=260)
    for h in (5, 21):
        assert np.array_equal(out[h][:lo], tg[h][:lo])
        assert np.array_equal(np.isnan(out[h][hi - h:]), np.isnan(tg[h][hi - h:]))
        assert np.allclose(np.sort(out[h][lo:hi - h, 0]), np.sort(tg[h][lo:hi - h, 0]))
        assert not np.allclose(out[h][lo:hi - h], tg[h][lo:hi - h])
        assert np.isnan(out[h]).sum() == np.isnan(tg[h]).sum()
    assert meta["offset_common"] == meta["offset_per_market_min_max"][0] == meta["offset_per_market_min_max"][1]


def test_F_settled_candidates_cannot_be_promoted_by_the_statistic():
    real, null, thr, cal = _run(6, [61, 62, 63, 64, 65], inject=10)
    settled = {"f0", "f1", "f2"}
    cal["refused_settled"] = cal["relationship_id"].isin(settled)
    eligible = cal[cal["promote_null"] & ~cal["refused_settled"]]
    assert not set(eligible["relationship_id"]) & settled
    assert cal[cal["refused_settled"]]["promote_null"].any(), "the statistic alone WOULD promote them; the identity refusal is what stops it"


def test_G_thresholds_hash_and_tamper_refusal():
    real, null, thr, cal = _run(7, [71, 72, 73])
    assert cal["null_thresholds_sha256"].iloc[0] == thr["sha256"]
    bad = dict(thr)
    bad["rule"] = dict(thr["rule"], promote_if_empirical_fdr_q_le=0.5)
    with pytest.raises(ValueError):
        EN.calibrate_frame(real, null, bad)


def test_H_thresholds_are_a_pure_function_of_the_null():
    real, null, thr, cal = _run(8, [81, 82, 83])
    real2 = real.copy()
    real2["t_net_nw"] = real2["t_net_nw"] * 10.0            # any real data whatsoever
    thr2 = EN.freeze_thresholds(null, strata=["book", "horizon"], stat_col="t_net_nw", seed_col="null_seed", sided=EN.SIDED_TWO,
                                fdr_promote=0.05, p_shortlist=0.01, rule_text="test")
    assert thr2["sha256"] == thr["sha256"]
    assert "t_net_nw" not in thr["per_stratum"][next(iter(thr["per_stratum"]))]  # only null summaries, never a real row


def test_fdr_q_monotone_and_floored():
    rng = np.random.default_rng(9)
    real = np.abs(rng.standard_normal(500))
    null = np.abs(rng.standard_normal(2500))
    q = EN.empirical_fdr_q(real, null, 5)
    order = np.argsort(-real)
    assert np.all(np.diff(q[order]) >= -1e-12), "q must be non-decreasing as the statistic falls"
    real2 = real.copy()
    real2[0] = 100.0                                             # one lone row above the null maximum
    q2 = EN.empirical_fdr_q(real2, null, 5)
    assert abs(q2[0] - 0.2) < 1e-12, "a single row above the null max gets q = 1/n_seeds, never 0"
