"""Executor for IMPLIED_CORRELATION_PREMIUM_INDEX_TIMING_V1 - synthetic data only.

Protects the strictly prior realised correlation and z-score, the next-close entry, the frozen
sign, the timing-increment gate that separates a correlation-premium signal from simply holding
the index, the unread confirmation after a qualification failure, the cache DATA_HOLD, and the
Alpha Agent executor contract.
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from alpha_agent import alpha_recovery as AR  # noqa: E402
from alpha_agent.alpha_recovery import implied_correlation_premium as IC  # noqa: E402
from alpha_agent.r59 import mechanisms as MX  # noqa: E402

SESSIONS = pd.bdate_range("2004-06-01", "2026-09-11")
PANEL_END = pd.Timestamp("2026-09-03")
OK_CACHE = {"passes": True, "file_sha256": "synthetic"}


def _signal_inputs(*, n_names: int = 120, trend: bool = False, seed: int = 11):
    rng = np.random.default_rng(seed)
    dates = SESSIONS[SESSIONS <= PANEL_END]
    m = len(dates)
    f = 0.01 * rng.choice([-1.0, 1.0], m)
    load = np.full(m, 0.8) if trend else 0.6 + 0.4 * np.sin(np.arange(m) / 400.0)
    r = load[None, :] * f[None, :] + rng.normal(0.0, 0.012, (n_names, m))
    tr = np.cumprod(1.0 + r, axis=1).astype(np.float32)
    mem = np.ones((n_names, m), dtype=np.uint8)
    mem[:10, :1500] = 0                       # ten names join the index later
    tr[-10:, 3000:3100] = np.nan              # ten names with a price gap
    pit = {"tr": tr, "mem": mem, "dates": dates, "identity": "SYNTHETIC_PIT"}
    n = len(SESSIONS)
    if trend:
        level = 5.0 + 90.0 * np.arange(n) / n + rng.normal(0.0, 0.3, n)
    else:
        level = 45.0 + 12.0 * np.sin(np.arange(n) / 260.0) + rng.normal(0.0, 4.0, n)
    cor3m = pd.Series(level, index=SESSIONS)
    return cor3m[cor3m.index >= pd.Timestamp("2006-01-03")], pit


def _spy(grid: pd.DataFrame, *, effect: float = 0.0, conf_sign: float = 1.0, drift: float = 0.0003,
         seed: int = 5) -> pd.Series:
    rng = np.random.default_rng(seed)
    base = rng.normal(drift, 0.009, len(SESSIONS))
    if effect:
        d0 = IC.decisions(grid, pd.Series(np.cumprod(1.0 + base), index=SESSIONS))
        for _, row in d0.iterrows():
            if not np.isfinite(row["pos"]):
                continue
            e, x = SESSIONS.get_loc(row["entry_date"]), SESSIONS.get_loc(row["exit_date"])
            sign = conf_sign if row["entry_date"] >= pd.Timestamp(IC.CONFIRMATION[0]) else 1.0
            base[e + 1:x + 1] += sign * effect * row["pos"]
    return pd.Series(np.cumprod(1.0 + base), index=SESSIONS)


def test_realised_correlation_is_the_member_average_over_complete_strictly_prior_windows():
    cor3m, pit = _signal_inputs()
    g = IC.build_grid(cor3m, SESSIONS, pit)
    assert g["decision_date"].iloc[0] == pd.Timestamp("2006-01-03")
    seen = set()
    for k in range(0, len(g), 7):
        i = SESSIONS.get_loc(g["decision_date"].iloc[k])
        p = pit["dates"].get_loc(SESSIONS[i - 1])
        if p < IC.RC_LOOKBACK:
            continue
        closes = pit["tr"][:, p - IC.RC_LOOKBACK:p + 1].astype(float)
        ok = pit["mem"][:, p].astype(bool) & np.isfinite(closes).all(axis=1)
        c = np.corrcoef(closes[ok, 1:] / closes[ok, :-1] - 1.0)
        n = c.shape[0]
        assert g["realised_corr"].iloc[k] == pytest.approx((c.sum() - n) / (n * (n - 1)), abs=1e-9)
        assert g["names"].iloc[k] == n
        seen.add(n)
    assert {110, 120} <= seen


def test_realised_correlation_premium_and_z_read_only_the_past():
    cor3m, pit = _signal_inputs()
    g1 = IC.build_grid(cor3m, SESSIONS, pit)
    k = 150
    cut = g1["decision_date"].iloc[k]
    p = pit["dates"].get_loc(cut)
    tr2 = pit["tr"].copy()
    tr2[:, p:] *= np.random.default_rng(3).uniform(0.5, 1.5, tr2[:, p:].shape).astype(np.float32)
    cor2 = cor3m.where(cor3m.index <= cut, 99.0)
    g2 = IC.build_grid(cor2, SESSIONS, dict(pit, tr=tr2))
    for col in ("realised_corr", "premium", "z"):
        assert np.allclose(g1[col].iloc[:k + 1].to_numpy(dtype=float),
                           g2[col].iloc[:k + 1].to_numpy(dtype=float), equal_nan=True)
    assert not np.allclose(g1["realised_corr"].iloc[k + 1:].to_numpy(dtype=float),
                           g2["realised_corr"].iloc[k + 1:].to_numpy(dtype=float))
    prem = g1["premium"].to_numpy(dtype=float)
    prior = prem[k - IC.Z_WINDOW:k]
    assert g1["z"].iloc[k] == pytest.approx((prem[k] - prior.mean()) / prior.std(ddof=1))
    assert np.isnan(g1["z"].iloc[IC.Z_MIN_OBS - 1]) and np.isfinite(g1["z"].iloc[IC.Z_MIN_OBS])


def test_entry_is_the_close_after_the_signal_close_and_periods_do_not_overlap():
    cor3m, pit = _signal_inputs()
    g = IC.build_grid(cor3m, SESSIONS, pit)
    spy = _spy(g)
    d = IC.decisions(g, spy)
    assert d["decision_date"].iloc[0] == g["decision_date"].iloc[IC.Z_MIN_OBS]
    row = d.iloc[10]
    i = SESSIONS.get_loc(row["decision_date"])
    e = i + IC.ENTRY_DELAY
    assert row["entry_date"] == SESSIONS[e] and row["exit_date"] == SESSIONS[e + IC.HORIZON]
    assert (d["entry_date"].iloc[1:].to_numpy() == d["exit_date"].iloc[:-1].to_numpy()).all()
    assert row["pos"] == np.sign(row["z"]) * IC.FROZEN_SIGN
    base = np.array(spy.pct_change().fillna(0.0).to_numpy(dtype=float), copy=True)
    base[: e + 1] += 0.05                    # the signal-close-to-entry return is never earned
    d2 = IC.decisions(g, pd.Series(np.cumprod(1.0 + base), index=SESSIONS))
    assert d2["spy_r"].iloc[10] == pytest.approx(row["spy_r"])


def test_a_wrong_sign_closes_the_mechanism_and_leaves_confirmation_unread():
    cor3m, pit = _signal_inputs()
    g = IC.build_grid(cor3m, SESSIONS, pit)
    res = IC.evaluate(g, _spy(g, effect=-0.0015), integrity_check=OK_CACHE)
    assert res["verdict"] == "KILLED_WRONG_SIGN", res["why"]
    assert res["confirmation"]["state"] == "UNREAD"


def test_a_book_that_only_tracks_the_passive_long_is_killed_at_the_timing_increment():
    cor3m, pit = _signal_inputs(trend=True)
    g = IC.build_grid(cor3m, SESSIONS, pit)
    spy = _spy(g, drift=0.0008)
    q = IC._finite(IC._window(IC.decisions(g, spy), *IC.QUALIFICATION))
    assert (q["pos"] > 0).mean() > 0.9               # a rising premium keeps the book long
    res = IC.evaluate(g, spy, integrity_check=OK_CACHE)
    assert res["verdict"] == "KILLED_NONINCREMENTAL" and res["gate"] == "TIMING_INCREMENT", res["why"]
    assert res["confirmation"]["state"] == "UNREAD"


def test_a_planted_effect_that_does_not_reproduce_is_killed_at_confirmation():
    cor3m, pit = _signal_inputs()
    g = IC.build_grid(cor3m, SESSIONS, pit)
    res = IC.evaluate(g, _spy(g, effect=0.0015, conf_sign=-1.0), integrity_check=OK_CACHE)
    assert res["confirmation"]["state"] == "READ"
    assert res["verdict"] == "KILLED_UNSTABLE" and res["gate"] == "UNTOUCHED_CONFIRMATION", res["why"]


def test_a_reproduced_planted_effect_qualifies_and_is_never_capital_eligible(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    cor3m, pit = _signal_inputs()
    spy = _spy(IC.build_grid(cor3m, SESSIONS, pit), effect=0.0015)
    body = IC.run(verbose=False, write=True, cor3m=cor3m, pit=pit, spy_tr=spy,
                  integrity_check=OK_CACHE, incumbent_daily=None)
    res = body["result"]
    assert res["verdict"] == "QUALIFIED", res["why"]
    assert body["capital_eligible"] is False and Path(body["artifact_path"]).exists()
    assert res["multiplicity"]["m"] == 8 == 1 + len(IC.INHERITED_NULLS)
    assert set(res["confirmation"]["by_publication_period"]) == set(IC.CONFIRMATION_ROWS)
    assert res["incumbent_equal_risk"]["state"] == "DATA_HOLD"


def test_too_few_names_or_a_manifest_mismatch_is_a_data_hold(tmp_path):
    cor3m, pit = _signal_inputs(n_names=95)
    g = IC.build_grid(cor3m, SESSIONS, pit)
    res = IC.evaluate(g, _spy(g), integrity_check=OK_CACHE)
    assert res["verdict"] == "DATA_HOLD" and res["gate"] == "DATA"
    cache = tmp_path / IC.CACHE_SUBDIR
    cache.mkdir()
    payload = b"DATE,OPEN,HIGH,LOW,CLOSE\n01/03/2006,31.34,31.34,31.34,31.34\n"
    (cache / IC.COR3M_FILE).write_bytes(payload)
    (cache / IC.MANIFEST_FILE).write_text(json.dumps({"COR3M": {"sha256": "0" * 64}}), encoding="utf-8")
    bad = IC.cache_integrity(cache)
    assert bad["passes"] is False
    cor3m, pit = _signal_inputs()
    g = IC.build_grid(cor3m, SESSIONS, pit)
    res = IC.evaluate(g, _spy(g, effect=0.0015), integrity_check=bad)
    assert res["verdict"] == "DATA_HOLD" and res["confirmation"]["state"] == "UNREAD"
    (cache / IC.MANIFEST_FILE).write_text(
        json.dumps({"COR3M": {"sha256": hashlib.sha256(payload).hexdigest()}}), encoding="utf-8")
    assert IC.cache_integrity(cache)["passes"] is True


def test_a_missing_cor3m_cache_is_a_data_hold_not_a_download(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "empty_research_root"))
    out = IC.run_mechanism(mechanism={"mechanism_id": IC.MECHANISM_ID})
    assert out["verdict"] == "DATA_HOLD" and out["kill_rule_fired"] == "DATA"
    assert out["capital_eligible"] is False and MX.validate_result(out) == []


def test_the_executor_contract_is_what_the_agent_validates(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    cor3m, pit = _signal_inputs()
    spy = _spy(IC.build_grid(cor3m, SESSIONS, pit), effect=-0.0015)
    monkeypatch.setattr(IC, "load_cor3m", lambda path=None: cor3m)
    monkeypatch.setattr(IC, "cache_integrity", lambda cache=None: OK_CACHE)
    monkeypatch.setattr(IC, "load_pit_panel", lambda *a, **k: pit)
    monkeypatch.setattr(IC, "load_spy_total_return", lambda: spy)
    import alpha_agent.alpha_recovery.intraday_alpha as IA
    monkeypatch.setattr(IA, "incumbent_daily_path", lambda: None)
    out = IC.run_mechanism(mechanism={"mechanism_id": IC.MECHANISM_ID})
    assert MX.validate_result(out) == [] and out["capital_eligible"] is False
    assert out["verdict"] == "KILLED_WRONG_SIGN" and out["multiplicity"]["m"] == 8
    with pytest.raises(ValueError):
        IC.run_mechanism(mechanism={"mechanism_id": "OTHER"})


def test_the_timing_increment_binds_on_the_worse_of_volatility_and_regression_matching(monkeypatch):
    rng = np.random.default_rng(21)
    p = rng.normal(0.01, 0.04, 200)
    y = -p + 0.01 + rng.normal(0.0, 0.004, 200)       # short beta plus a constant: regression alpha only
    f = pd.DataFrame({"pos": np.ones(200), "spy_r": p})
    monkeypatch.setattr(IC, "net", lambda d, cost_bps: y)
    inc = IC.timing_increment(f, IC.COST_PRIMARY_BPS)
    rm, vm = inc["regression_matched"], inc["volatility_matched"]
    assert rm["t"] > 2.0 and rm["ann"] > AR.MATERIALITY_ANN_NET
    assert vm["t"] < 0.0
    assert inc["t"] == min(rm["t"], vm["t"]) and inc["ann"] == min(rm["ann"], vm["ann"])
