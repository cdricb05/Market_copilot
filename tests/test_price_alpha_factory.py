"""
tests/test_price_alpha_factory.py — Phase 21 Price-Alpha Factory engine (offline, deterministic).

Exercises the generate -> multi-horizon evaluate -> gate -> correlate -> combine -> leaderboard ->
artifact pipeline against synthetic owned-style fundamental + price panels (never the network, never
the database, never the prediction service). Asserts the five price families are all data-ready, that
every candidate carries its four-horizon metric bundle, that the automatic overfit-defense gates and
the param-neighbour check behave, that a preview writes nothing, that a committing build requires the
token and writes ONLY the local store, and that no status ever approves live trading or replaces the
champion.
"""
from __future__ import annotations

import math
from datetime import date, timedelta
from pathlib import Path

import pytest

from paper_trader.api import price_alpha_factory as paf
from paper_trader.api import alpha_registry as reg
from tests.test_alpha_factory import _write_panel, _write_reval_report

_PX_HEADER = "date,ticker,adjusted_close,volume,benchmark_close,daily_return"


def _weekdays(start: date, end: date) -> list[str]:
    out, d = [], start
    while d <= end:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _write_price_csv(path: Path, n_tickers: int = 30) -> None:
    """Synthetic daily price panel aligned to the synthetic fundamental panel (tickers T00..T29,
    months 2019-06..2020-06). A monotone cross-sectional drift makes momentum a genuinely positive
    signal, and a per-ticker oscillation gives realized volatility cross-sectional spread."""
    dates = _weekdays(date(2018, 1, 1), date(2020, 9, 30))
    lines = [_PX_HEADER]
    for i, d in enumerate(dates):
        bench = round(300.0 * math.exp(0.0002 * i), 6)
        for t in range(n_tickers):
            drift = (t - (n_tickers - 1) / 2.0) * 0.0004          # cross-sectional momentum
            amp = 0.006 + (t % 5) * 0.002                          # cross-sectional vol spread
            freq = 0.25 + (t % 7) * 0.03
            logp = drift * i + amp * math.sin(freq * i)
            px = round(100.0 * math.exp(logp), 6)
            lines.append("%s,T%02d,%s,%d,%s," % (d, t, px, 1000000, bench))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


@pytest.fixture(autouse=True)
def _clear_cache():
    paf.clear_cache()
    yield
    paf.clear_cache()


@pytest.fixture
def panels(tmp_path) -> dict:
    fp = tmp_path / "fund.csv"
    px = tmp_path / "px.csv"
    rp = tmp_path / "reval.json"
    _write_panel(fp)
    _write_price_csv(px)
    _write_reval_report(rp)
    return {"fund": fp, "px": px, "report": rp, "store": tmp_path / "store"}


def _build(panels: dict) -> dict:
    return paf.build_price_alpha_factory(panel_path=panels["fund"], price_path=panels["px"],
                                         reval_report_path=panels["report"], use_cache=False)


# --------------------------------------------------------------------------- #
# Structure + family activation
# --------------------------------------------------------------------------- #
def test_build_ready_and_five_families_active(panels):
    b = _build(panels)
    assert b["status"] == paf.STATUS_READY
    fams = b["families"]
    assert len(fams) == 5
    assert {f["family"] for f in fams} == set(reg.PRICE_GATED_FAMILIES)
    assert all(f["data_ready"] and f["status"] == "DATA_READY" for f in fams)
    assert all(f["n_candidates"] > 0 for f in fams)


def test_generated_candidate_count_and_registry_total(panels):
    b = _build(panels)
    assert b["diagnostics"]["generated_candidates"] == 29
    # 29 candidates + champion + challenger
    assert b["registry"]["counts"]["total"] == 31
    assert b["champion"]["name"] == "composite_sn"
    assert b["challenger"]["name"] == "composite_sn_repaired"


def test_every_candidate_has_four_horizons(panels):
    b = _build(panels)
    cands = [a for a in b["registry"]["alphas"] if a["role"] == "GENERATED PRICE CANDIDATE"]
    assert len(cands) == 29
    for a in cands:
        assert set(a["horizons"].keys()) == {"5", "10", "21", "63"}
        assert a["primary_horizon_trading_days"] in (5, 21, 63)
        assert a["rebalance_cadence"] == "monthly"
        assert a["data_source"] == paf.pp.SOURCE_NAME
        assert "pit_caveats" in a and a["horizon"] is not None


def test_momentum_is_positive_ic_by_construction(panels):
    b = _build(panels)
    # cross-sectional drift is monotone in ticker index, so mid-horizon momentum has positive IC
    mom = {a["name"]: a for a in b["registry"]["alphas"] if a["family"] == "MOMENTUM"}
    assert mom["mom_63"]["ic"] is not None and mom["mom_63"]["ic"] > 0


def test_horizon_summary_and_best_horizon_present(panels):
    b = _build(panels)
    hz = b["horizon_summary"]
    assert len(hz) == 29 * 4
    assert any(r["is_primary"] for r in hz)
    fbh = b["diagnostics"]["family_best_horizon"]
    assert set(fbh.keys()) == set(reg.PRICE_GATED_FAMILIES)


# --------------------------------------------------------------------------- #
# Gates
# --------------------------------------------------------------------------- #
def test_hard_gate_reason_unit():
    good = {"coverage_pct": 90.0, "n_ic_months": 60, "mean_ic": 0.02, "ic_t_stat": 2.5,
            "net25_spread": 0.01, "positive_ic_month_rate": 0.6, "cumulative_spread": 0.4,
            "max_drawdown": -0.1, "mean_turnover": 0.5,
            "subperiod": {"pre2020": {"mean_ic": 0.02, "mean_spread": 0.01},
                          "post2020": {"mean_ic": 0.02, "mean_spread": 0.01}}}
    assert paf._hard_gate_reason(good, False) is None
    assert paf._hard_gate_reason({**good, "coverage_pct": 10.0}, False) == reg.REJECT_LOW_COVERAGE
    assert paf._hard_gate_reason({**good, "n_ic_months": 5}, False) == reg.REJECT_INSUFFICIENT_PERIODS
    assert paf._hard_gate_reason({**good, "mean_ic": -0.01}, False) == reg.REJECT_NEGATIVE_IC
    assert paf._hard_gate_reason({**good, "ic_t_stat": 0.5}, False) == reg.REJECT_STATISTICALLY_WEAK
    assert paf._hard_gate_reason({**good, "net25_spread": -0.001}, False) == reg.REJECT_COST_KILLED
    rev = {**good, "subperiod": {"pre2020": {"mean_ic": 0.02, "mean_spread": 0.01},
                                 "post2020": {"mean_ic": -0.02, "mean_spread": 0.01}}}
    assert paf._hard_gate_reason(rev, False) == reg.REJECT_UNSTABLE
    conc = {**good, "subperiod": {"pre2020": {"mean_ic": 0.02, "mean_spread": -0.01},
                                  "post2020": {"mean_ic": 0.02, "mean_spread": 0.03}}}
    assert paf._hard_gate_reason(conc, False) == reg.REJECT_CONCENTRATED
    assert paf._hard_gate_reason(good, True) == reg.REJECT_PARAM_UNSTABLE
    dd = {**good, "cumulative_spread": 0.1, "max_drawdown": -0.5}
    assert paf._hard_gate_reason(dd, False) == reg.REJECT_SEVERE_DRAWDOWN
    turn = {**good, "mean_turnover": 0.995, "net25_spread": 0.0005}
    assert paf._hard_gate_reason(turn, False) == reg.REJECT_EXCESSIVE_TURNOVER


def test_some_candidates_rejected_with_reasons(panels):
    b = _build(panels)
    rej = b["rejected"]
    assert len(rej) > 0
    for r in rej:
        assert r["reject_reason"] in reg.REJECT_REASON_TEXT
        assert r["reject_reason_text"]


def test_no_champion_and_no_survivor_is_a_valid_outcome(panels):
    b = _build(panels)
    # every candidate is one of the allowed lifecycle statuses (never CHAMPION)
    for a in b["registry"]["alphas"]:
        if a["role"] == "GENERATED PRICE CANDIDATE":
            assert a["status"] in (reg.STATUS_ACTIVE, reg.STATUS_RESEARCH, reg.STATUS_REJECTED)


# --------------------------------------------------------------------------- #
# Correlation + combinations + cross-check
# --------------------------------------------------------------------------- #
def test_correlation_matrix_square_symmetric_diag1(panels):
    b = _build(panels)
    corr = b["correlation"]
    assert corr["signals"][0] == "composite_sn"
    n = len(corr["signals"])
    mat = corr["matrix"]
    assert len(mat) == n and all(len(r) == n for r in mat)
    for i in range(n):
        assert mat[i][i] == 1.0
        for j in range(n):
            assert mat[i][j] == mat[j][i]


def test_combinations_never_promote_champion(panels):
    b = _build(panels)
    cb = b["combinations"]
    assert "combinations" in cb and "champion_baseline_on_intersection" in cb
    for r in cb["combinations"]:
        assert r["status"] == reg.STATUS_RESEARCH
        assert r["eligibility"] in (paf.ELIG_CHALLENGER, paf.ELIG_RESEARCH)
    assert cb["n_challenger_eligible"] <= len(cb["combinations"])


def test_no_lookahead_cross_check_positive(panels):
    b = _build(panels)
    cc = b["diagnostics"]["no_lookahead_cross_check"]
    if cc.get("available"):
        assert cc["spearman_price63_vs_fundamental63"] is not None


# --------------------------------------------------------------------------- #
# Determinism
# --------------------------------------------------------------------------- #
def test_deterministic(panels):
    b1 = _build(panels)
    b2 = _build(panels)
    assert b1["registry"]["counts"] == b2["registry"]["counts"]
    assert b1["leaderboard"] == b2["leaderboard"]
    assert b1["correlation"]["matrix"] == b2["correlation"]["matrix"]


# --------------------------------------------------------------------------- #
# Preview / commit / store isolation
# --------------------------------------------------------------------------- #
def _run(panels, **kw):
    return paf.run_price_alpha_factory(panel_path=panels["fund"], price_path=panels["px"],
                                       reval_report_path=panels["report"], store_dir=panels["store"], **kw)


def test_preview_writes_nothing(panels):
    out = _run(panels, commit=False)
    assert out["status"] == paf.STATUS_BUILD_PREVIEW
    assert out["wrote_store"] is False and out["performed_write"] is False
    assert len(out["would_write_files"]) == 13
    assert not panels["store"].exists()


def test_commit_requires_token(panels):
    out = _run(panels, commit=True)
    assert out["status"] == paf.STATUS_CONFIRM_REQUIRED
    assert out["wrote_store"] is False
    assert not (panels["store"] / paf._RUN_STATE_FILE).exists()


def test_commit_writes_only_local_store(panels):
    out = _run(panels, commit=True, confirm=paf.BUILD_CONFIRM_TOKEN)
    assert out["status"] == paf.STATUS_BUILD_COMPLETE
    assert out["wrote_store"] is True and out["wrote_to_database"] is False
    assert out["replaces_champion"] is False and out["promotes_to_live"] is False
    written = sorted(p.name for p in panels["store"].iterdir())
    for fn in paf._ALL_ARTIFACTS:
        assert fn in written
    # the final report explicitly records the safety posture
    import json
    fr = json.loads((panels["store"] / paf._FINAL_REPORT_JSON).read_text(encoding="utf-8"))
    assert fr["champion_unchanged"] is True and fr["promotes_to_live"] is False


def test_load_aggregate_carries_safety_block(panels):
    p = paf.load_price_alpha_factory(panel_path=panels["fund"], price_path=panels["px"],
                                     reval_report_path=panels["report"], store_dir=panels["store"])
    assert p["live_trading_status"] == "NOT_APPROVED_FOR_LIVE_TRADING"
    assert p["no_decision_approves_live_trading"] is True
    assert p["replaces_champion"] is False and p["wrote_to_database"] is False
    assert p["safety_badges"][0] == "RESEARCH ONLY"


def test_price_panel_unavailable_degrades_gracefully(panels, tmp_path):
    out = paf.build_price_alpha_factory(panel_path=panels["fund"], price_path=tmp_path / "missing.csv",
                                        reval_report_path=panels["report"], use_cache=False)
    assert out["status"] == paf.STATUS_PANEL_UNAVAILABLE
    assert "price" in out["missing"].lower()
    load = paf.load_price_alpha_factory(panel_path=panels["fund"], price_path=tmp_path / "missing.csv",
                                        reval_report_path=panels["report"], store_dir=panels["store"])
    assert load["status"] == paf.STATUS_PANEL_UNAVAILABLE
    assert load["no_decision_approves_live_trading"] is True
