"""
tests/test_alpha_factory.py — Phase 20 Alpha Factory engine (offline, deterministic).

Exercises the generation → evaluation → gating → leaderboard → correlation → artifact pipeline
against a synthetic owned-style panel fixture (never the network, never the database, never the
prediction service). Also asserts, when the real committed artifacts are present, that recomputing
the champion (composite_sn) from the frozen panel reproduces the committed Phase 17-A champion
battery exactly.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from paper_trader.api import alpha_factory as af
from paper_trader.api import alpha_registry as reg

_COLUMNS = [
    "as_of_date", "rebalance_date", "ticker", "sector", "cohort", "is_new_cohort", "liquidity_proxy",
    "fcf_to_assets", "operating_accruals", "fcf_to_assets_raw", "operating_accruals_raw",
    "fcf_to_assets_sector_neutral_z", "operating_accruals_sector_neutral_z",
    "operating_accruals_oriented_sector_neutral_z", "composite_sn", "composite_raw",
    "forward_63d_return", "forward_63d_return_start_date", "forward_63d_return_end_date",
    "has_forward_return", "source_phase", "data_quality_flag",
]

_MONTHS = (["2019-%02d" % k for k in range(6, 13)] + ["2020-%02d" % k for k in range(1, 7)])  # 7 pre + 6 post
_N_TICKERS = 30


def _write_panel(path: Path, last_month_no_fwd: bool = False) -> None:
    """Deterministic synthetic panel where composite_sn == fcf_sn + acc_sn EXACTLY and the forward
    return is monotone in composite_sn (so the champion has a strongly positive IC)."""
    lines = [",".join(_COLUMNS)]
    for mi, m in enumerate(_MONTHS):
        for t in range(_N_TICKERS):
            fcf_sn = ((t * 13 + mi * 7) % 21 - 10) / 5.0
            acc_sn = ((t * 29 + mi * 3) % 17 - 8) / 5.0
            comp_sn = fcf_sn + acc_sn
            fcf_raw = 0.5 + fcf_sn * 0.1
            acc_raw = 0.5 + acc_sn * 0.1
            comp_raw = fcf_raw + acc_raw
            fcf_level = fcf_raw * 0.2
            acc_level = -acc_raw * 0.2
            fwd = round(0.02 * comp_sn + (((t * 5 + mi) % 7) - 3) * 0.0005, 6)
            has_fwd = "True"
            if last_month_no_fwd and mi == len(_MONTHS) - 1:
                has_fwd = "False"
                fwd = ""
            is_new = "True" if t % 4 == 0 else "False"
            row = [
                "2020-06-26", "%s-15" % m, "T%02d" % t, ["SEC_A", "SEC_B", "SEC_C"][t % 3],
                "new" if is_new == "True" else "established", is_new, str(1000000.0 * (t + 1)),
                str(fcf_level), str(acc_level), str(fcf_raw), str(acc_raw),
                str(fcf_sn), str(acc_sn), str(acc_sn), str(comp_sn), str(comp_raw),
                str(fwd), "%s-15" % m, "", has_fwd, "test", "OK",
            ]
            lines.append(",".join(row))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_reval_report(path: Path) -> None:
    path.write_text(json.dumps({
        "decision": "PAPER_CHALLENGER_ELIGIBLE",
        "full_panel": {
            "rank_spearman_champion_vs_repaired": 0.8915,
            "champion": {"mean_ic": 0.036541, "ic_t_stat": 3.2645, "mean_gross_spread": 0.01366,
                         "mean_turnover": 0.9946, "net25_spread": 0.011174, "net50_spread": 0.008687,
                         "max_drawdown": -0.250924, "positive_ic_month_rate": 0.6186},
            "repaired_candidate": {"mean_ic": 0.02926, "ic_t_stat": 2.9285, "mean_gross_spread": 0.012698,
                                   "mean_turnover": 0.9948, "net25_spread": 0.010211, "net50_spread": 0.007724,
                                   "max_drawdown": -0.557426, "positive_ic_month_rate": 0.5932},
        },
    }), encoding="utf-8")


@pytest.fixture(autouse=True)
def _clear_cache():
    af.clear_cache()
    yield
    af.clear_cache()


@pytest.fixture
def panel(tmp_path) -> Path:
    p = tmp_path / "panel.csv"
    _write_panel(p)
    return p


@pytest.fixture
def report(tmp_path) -> Path:
    p = tmp_path / "reval.json"
    _write_reval_report(p)
    return p


# --------------------------------------------------------------------------- #
# pure maths primitives
# --------------------------------------------------------------------------- #
def test_spearman_perfect_monotone():
    assert af._spearman([1, 2, 3, 4], [10, 20, 30, 40]) == pytest.approx(1.0)
    assert af._spearman([1, 2, 3, 4], [40, 30, 20, 10]) == pytest.approx(-1.0)


def test_max_drawdown_and_tstat():
    assert af._max_drawdown([0, 1, 2, 1, 3]) == pytest.approx(-1.0)
    assert af._t_stat([1.0, 1.0, 1.0, 1.0]) is None  # zero variance
    assert af._t_stat([0.1, 0.2, 0.15, 0.18]) > 0


def test_evaluate_battery_shapes(panel):
    p = af.load_panel(panel)
    vals = {i: af._to_float(p["rows"][i].get("composite_sn")) for i in range(len(p["rows"]))}
    monthly, cov = af._build_monthly(p["rows"], p["rep_index"], vals)
    m = af.evaluate_battery(monthly)
    assert m["n_months_scored"] == len(_MONTHS)
    assert m["mean_ic"] is not None and m["mean_ic"] > 0  # champion is monotone in fwd
    assert m["subperiod"]["pre2020"]["n_months"] == 7
    assert m["subperiod"]["post2020"]["n_months"] == 6
    assert cov["coverage_pct"] == pytest.approx(100.0)


# --------------------------------------------------------------------------- #
# build: families, registry, reproduction
# --------------------------------------------------------------------------- #
def test_build_status_and_universe(panel, report):
    b = af.build_alpha_factory(panel_path=panel, reval_report_path=report, use_cache=False)
    assert b["status"] == af.STATUS_READY
    assert b["universe"]["n_names"] == _N_TICKERS
    assert b["signal_date"] == "2020-06-15"


def test_all_ten_families_five_gated(panel, report):
    b = af.build_alpha_factory(panel_path=panel, reval_report_path=report, use_cache=False)
    fams = b["families"]
    assert len(fams) == 10
    gated = [f for f in fams if not f["data_ready"]]
    assert {f["family"] for f in gated} == set(reg.PRICE_GATED_FAMILIES)
    for f in gated:
        assert f["n_candidates"] == 0
        assert f["status"] == "DATA_GATED"


def test_registry_has_champion_challenger_and_candidates(panel, report):
    b = af.build_alpha_factory(panel_path=panel, reval_report_path=report, use_cache=False)
    alphas = b["registry"]["alphas"]
    by_status = {}
    for a in alphas:
        by_status.setdefault(a["status"], []).append(a["name"])
    assert by_status[reg.STATUS_CHAMPION] == [af.CHAMPION_SIGNAL]
    assert by_status[reg.STATUS_CHALLENGER] == [af.CHALLENGER_SIGNAL]
    # 16 generated candidates across the data-ready families
    n_candidates = sum(1 for a in alphas if a["role"] == "GENERATED CANDIDATE")
    assert n_candidates == 16
    # every record carries the fixed metadata schema
    for a in alphas:
        for field in reg.REGISTRY_METADATA_FIELDS:
            assert field in a


def test_champion_reproduction_additive_identity(panel, report):
    b = af.build_alpha_factory(panel_path=panel, reval_report_path=report, use_cache=False)
    repro = b["reproduction"]
    assert repro["champion_reproduced"] is True
    assert repro["max_abs_error"] == pytest.approx(0.0, abs=1e-12)


def test_challenger_metrics_from_report(panel, report):
    b = af.build_alpha_factory(panel_path=panel, reval_report_path=report, use_cache=False)
    cl = b["challenger"]
    assert cl["status"] == reg.STATUS_CHALLENGER
    assert cl["ic_t"] == pytest.approx(2.9285, abs=1e-4)
    assert cl["corr_vs_champion"] == pytest.approx(0.8915, abs=1e-4)


def test_generation_is_deterministic(panel, report):
    b1 = af.build_alpha_factory(panel_path=panel, reval_report_path=report, use_cache=False)
    b2 = af.build_alpha_factory(panel_path=panel, reval_report_path=report, use_cache=False)
    names1 = [(a["name"], a["status"], a["ic"], a["net25"]) for a in b1["registry"]["alphas"]]
    names2 = [(a["name"], a["status"], a["ic"], a["net25"]) for a in b2["registry"]["alphas"]]
    assert names1 == names2


# --------------------------------------------------------------------------- #
# gates
# --------------------------------------------------------------------------- #
def test_hard_gate_reasons_unit():
    ok = {"mean_ic": 0.03, "ic_t_stat": 3.0, "net25_spread": 0.01, "positive_ic_month_rate": 0.6,
          "subperiod": {"pre2020": {"mean_ic": 0.03}, "post2020": {"mean_ic": 0.02}}}
    cov_ok = {"coverage_pct": 100.0}
    assert af._hard_gate_reason(ok, cov_ok) is None
    assert af._hard_gate_reason(ok, {"coverage_pct": 10.0}) == reg.REJECT_LOW_COVERAGE
    assert af._hard_gate_reason({**ok, "mean_ic": -0.01}, cov_ok) == reg.REJECT_NEGATIVE_IC
    assert af._hard_gate_reason({**ok, "ic_t_stat": 0.5}, cov_ok) == reg.REJECT_STATISTICALLY_WEAK
    assert af._hard_gate_reason({**ok, "net25_spread": -0.001}, cov_ok) == reg.REJECT_COST_KILLED
    reversal = {**ok, "subperiod": {"pre2020": {"mean_ic": 0.03}, "post2020": {"mean_ic": -0.02}}}
    assert af._hard_gate_reason(reversal, cov_ok) == reg.REJECT_UNSTABLE
    lowhit = {**ok, "positive_ic_month_rate": 0.3}
    assert af._hard_gate_reason(lowhit, cov_ok) == reg.REJECT_UNSTABLE


def test_weak_candidates_are_rejected_with_reasons(panel, report):
    b = af.build_alpha_factory(panel_path=panel, reval_report_path=report, use_cache=False)
    rejected = b["rejected"]
    # the quality level factors are rank-identical to the fundamental legs -> redundant
    reasons = {r["name"]: r["reject_reason"] for r in rejected}
    assert any(v == reg.REJECT_REDUNDANT for v in reasons.values())
    for r in rejected:
        assert r["reject_reason"] in reg.REJECT_REASON_TEXT


# --------------------------------------------------------------------------- #
# leaderboard + correlation
# --------------------------------------------------------------------------- #
def test_leaderboard_champion_first_then_sorted(panel, report):
    b = af.build_alpha_factory(panel_path=panel, reval_report_path=report, use_cache=False)
    lb = b["leaderboard"]
    assert lb[0]["is_champion"] is True
    survivor_net = [r["net25"] for r in lb[1:] if r["net25"] is not None]
    assert survivor_net == sorted(survivor_net, reverse=True)


def test_correlation_matrix_square_symmetric_diag1(panel, report):
    b = af.build_alpha_factory(panel_path=panel, reval_report_path=report, use_cache=False)
    corr = b["correlation"]
    sig = corr["signals"]
    mat = corr["matrix"]
    assert sig[0] == af.CHAMPION_SIGNAL
    n = len(sig)
    assert all(len(row) == n for row in mat)
    for i in range(n):
        assert mat[i][i] == pytest.approx(1.0)
        for j in range(n):
            if mat[i][j] is not None and mat[j][i] is not None:
                assert mat[i][j] == pytest.approx(mat[j][i])


# --------------------------------------------------------------------------- #
# preview / commit / artifacts / load
# --------------------------------------------------------------------------- #
def test_preview_writes_nothing(panel, report, tmp_path):
    store = tmp_path / "store"
    out = af.run_alpha_factory(commit=False, panel_path=panel, reval_report_path=report, store_dir=store)
    assert out["status"] == af.STATUS_BUILD_PREVIEW
    assert out["wrote_store"] is False and out["performed_write"] is False
    assert not store.exists() or not any(store.iterdir())


def test_commit_requires_token(panel, report, tmp_path):
    store = tmp_path / "store"
    out = af.run_alpha_factory(commit=True, confirm="WRONG", panel_path=panel,
                               reval_report_path=report, store_dir=store)
    assert out["status"] == af.STATUS_CONFIRM_REQUIRED
    assert out["wrote_store"] is False
    assert not store.exists() or not any(store.iterdir())


def test_commit_writes_only_local_store(panel, report, tmp_path):
    store = tmp_path / "store"
    out = af.run_alpha_factory(commit=True, confirm=af.BUILD_CONFIRM_TOKEN, panel_path=panel,
                               reval_report_path=report, store_dir=store)
    assert out["status"] == af.STATUS_BUILD_COMPLETE
    assert out["wrote_store"] is True and out["wrote_to_database"] is False
    assert out["replaces_champion"] is False and out["calls_prediction_service"] is False
    written = set(out["files_written"])
    for fn in (af._REGISTRY_FILE, af._LEADERBOARD_FILE, af._CORRELATION_FILE,
               af._DIAGNOSTICS_FILE, af._CANDIDATE_REPORTS_FILE, af._RUN_STATE_FILE):
        assert fn in written
        assert (store / fn).exists()
    # the registry artifact round-trips
    disk = json.loads((store / af._REGISTRY_FILE).read_text(encoding="utf-8"))
    assert len(disk["alphas"]) == 18
    assert len(disk["schema"]) == len(reg.REGISTRY_METADATA_FIELDS)


def test_load_aggregate_safety_block(panel, report, tmp_path):
    p = af.load_alpha_factory(panel_path=panel, reval_report_path=report, store_dir=tmp_path / "s")
    assert p["status"] == af.STATUS_READY
    assert p["no_orders"] is True and p["no_broker"] is True and p["no_automation"] is True
    assert p["replaces_champion"] is False and p["wrote_to_database"] is False
    assert p["live_trading_status"] == "NOT_APPROVED_FOR_LIVE_TRADING"
    assert p["persisted"]["has_artifacts"] is False


def test_panel_unavailable_degrades(tmp_path):
    missing = tmp_path / "nope.csv"
    p = af.load_alpha_factory(panel_path=missing, store_dir=tmp_path / "s")
    assert p["status"] == af.STATUS_PANEL_UNAVAILABLE
    assert p["no_orders"] is True and p["replaces_champion"] is False
    assert len(p["families"]) == 10
    run = af.run_alpha_factory(commit=True, confirm=af.BUILD_CONFIRM_TOKEN, panel_path=missing,
                               store_dir=tmp_path / "s")
    assert run["status"] == af.STATUS_PANEL_UNAVAILABLE and run["wrote_store"] is False


def test_last_month_without_forward_return_is_excluded(tmp_path, report):
    p = tmp_path / "panel2.csv"
    _write_panel(p, last_month_no_fwd=True)
    af.clear_cache()
    b = af.build_alpha_factory(panel_path=p, reval_report_path=report, use_cache=False)
    # champion battery scores only months with a realized forward return (12, not 13)
    assert b["champion"]["n_months_scored"] == len(_MONTHS) - 1


# --------------------------------------------------------------------------- #
# integration: real owned panel reproduces the committed Phase 17-A champion
# --------------------------------------------------------------------------- #
def test_real_panel_reproduces_committed_champion():
    if not af.DEFAULT_PANEL.exists() or not af.DEFAULT_REVAL_REPORT.exists():
        pytest.skip("owned frozen panel / committed Phase 17-A report not present")
    af.clear_cache()
    b = af.build_alpha_factory(use_cache=False)
    cc = b["diagnostics"]["battery_cross_check_vs_committed_phase17a_champion"]
    assert cc["available"] is True
    assert cc["reproduces_committed_champion"] is True
    assert cc["max_relative_error"] == pytest.approx(0.0, abs=1e-6)
    assert b["champion"]["ic_t"] == pytest.approx(3.2645, abs=1e-3)
    assert b["reproduction"]["max_abs_error"] == pytest.approx(0.0, abs=1e-9)
