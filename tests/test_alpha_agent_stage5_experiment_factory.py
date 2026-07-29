"""
tests/test_alpha_agent_stage5_experiment_factory.py — Alpha Agent Stage 5.

Deterministic coverage of the autonomous experiment & evidence engine: config
validation, deterministic identifiers, source-package verification, grounded
hypothesis intake, zero-hypothesis behaviour, duplicate prevention, the
unsupported-template data hold, every supported template mapping, leakage +
point-in-time coverage controls, the pure metric math (rank IC / decile spread /
turnover / cost / drawdown / benchmark / champion / regime / subperiod / cost
sensitivity), the full evidence-gate decision surface, the bounded hypothesis /
spec / experiment counts, resumability + idempotency + determinism, immutable
package output, read-only verify mode, the Stage 4 report model + rendering, and
the hard safety invariants (no LLM / PostgreSQL / prediction / order-signal-fill-
decision / model or ledger mutation). Every store, clock and source package is a
FAKE — no network, LLM, database or operational ledger is ever touched.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO.parent) not in sys.path:
    sys.path.insert(0, str(_REPO.parent))

from paper_trader.alpha_agent import experiment_contracts as ec  # noqa: E402
from paper_trader.alpha_agent import experiment_factory as ef  # noqa: E402
from paper_trader.alpha_agent import experiment_runner as er  # noqa: E402
from paper_trader.alpha_agent import report_renderer as rr  # noqa: E402

_NOW = "2026-07-29T00:00:00+00:00"


# --------------------------------------------------------------------------- #
# Fixtures + fakes.
# --------------------------------------------------------------------------- #
def _mom_panel(symbols=60, n_days=252 * 6):
    panel = {}
    for s in range(symbols):
        base = 10.0 + s
        mu = 0.0002 + 0.00004 * s
        price = base
        series = []
        for i in range(n_days):
            eps = 0.00006 * (((s * 7 + i * 13) % 11) - 5)
            price *= (1.0 + mu + eps)
            y = 2020 + i // 252
            series.append(("%04d-%03d" % (y, i % 252), price))
        panel["SYM%02d" % s] = series
    return panel


class FakeStore:
    def __init__(self, panel=None, families_present=None, days=252 * 6,
                 months=72):
        self.panel = panel if panel is not None else _mom_panel()
        self.families_present = families_present  # None => all present
        self.days = days
        self.months = months

    def coverage(self, record_types):
        present = (self.families_present is None
                   or any(rt in self.families_present for rt in record_types))
        if not present:
            return {"files": 0, "date_start": None, "date_end": None,
                    "days": 0, "months": 0}
        return {"files": 10, "date_start": "2020-01-01",
                "date_end": "2025-12-31", "days": self.days,
                "months": self.months}

    def price_panel(self, date_start=None, date_end=None):
        return {k: list(v) for k, v in self.panel.items()}


def _base_cfg(tmp_path, **over):
    cfg = {
        "stage": "5",
        "experiments_root": str(tmp_path / "experiments"),
        "stage1_registry_root": str(tmp_path / "registry"),
        "stage2_ingestion_root": str(tmp_path / "ingestion"),
        "stage3_director_root": str(tmp_path / "director"),
        "stage3_5_news_rss_root": str(tmp_path / "news_rss"),
        "operational_ledger_roots": [str(tmp_path / "ledger")],
        "bounds": {"max_new_hypotheses": 3, "max_specs_per_hypothesis": 2,
                   "max_experiments": 6, "max_runtime_seconds": 1800,
                   "max_workers": 2, "max_symbols": 800},
        "gates": dict(ec.DEFAULT_GATES),
        "templates_enabled": list(ec.SUPPORTED_TEMPLATES),
        "benchmark": {"name": "equal_weight_universe"},
        "cost_bps": [10, 25, 50],
        "control": {"enable_champion_health_check": True},
    }
    cfg.update(over)
    return cfg


def _pkgs(tmp_path):
    for name in ("registry", "ingestion", "director"):
        (tmp_path / name / "runs" / ("%s_run" % name)).mkdir(parents=True,
                                                             exist_ok=True)
    return {
        "stage1": {"run_id": "registry_run", "run_dir": "runs/registry_run",
                   "champion_model": "fundamental_momentum_50_50_v1"},
        "stage2": {"run_id": "ingestion_run", "run_dir": "runs/ingestion_run"},
        "stage3": {"run_id": "director_run", "run_dir": "runs/director_run"},
        "stage3_5": {"run_id": "news_run"},
        "champion_model": "fundamental_momentum_50_50_v1",
        "source_fingerprint": "fp_test",
    }


def _hyp(hid="hyp_mom", family="price_momentum", text="price momentum trend",
         **over):
    h = {"hypothesis_id": hid, "title": text, "text": text,
         "information_family": family, "expected_direction": "LONG_HIGH",
         "rebalance_cadence": "MONTHLY", "horizon": "21 days",
         "universe": "test_universe", "required_fields": [],
         "feature_definition": "", "source_record_ids": [],
         "priority": 1, "queue_status": "READY_FOR_DETERMINISTIC_DESIGN_REVIEW",
         "grounding": "GROUNDED"}
    h.update(over)
    return h


def _run(cfg, tmp_path, hyps, *, store=None, now=_NOW, output_root=None):
    return ef.run_stage5_cycle(
        cfg, output_root=output_root, as_of="2026-07-29", now_iso=now,
        hypotheses=hyps, source_packages=_pkgs(tmp_path),
        store=store or FakeStore(), ledger_fingerprint=lambda: {})


# --------------------------------------------------------------------------- #
# Config validation.
# --------------------------------------------------------------------------- #
def test_real_config_loads_and_is_secret_free():
    cfg = ec.load_config(_REPO / "configs" / "alpha_agent" /
                         "stage5_experiment_factory.json")
    assert cfg["stage"] == "5"
    assert ec.scan_for_secrets(cfg) == []
    assert len(ec.enabled_templates(cfg)) == 8


def _write(p, obj):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj), encoding="utf-8")


def test_config_rejects_bad_stage(tmp_path):
    p = tmp_path / "c.json"
    cfg = _base_cfg(tmp_path)
    cfg["stage"] = "4"
    _write(p, cfg)
    with pytest.raises(ec.ConfigError):
        ec.load_config(p)


def test_config_rejects_embedded_secret(tmp_path):
    p = tmp_path / "c.json"
    cfg = _base_cfg(tmp_path)
    cfg["api_key"] = "sk-ant-abcdefghijklmnop"
    _write(p, cfg)
    with pytest.raises(ec.ConfigError):
        ec.load_config(p)


def test_config_rejects_bad_bounds(tmp_path):
    p = tmp_path / "c.json"
    cfg = _base_cfg(tmp_path)
    cfg["bounds"]["max_experiments"] = 0
    _write(p, cfg)
    with pytest.raises(ec.ConfigError):
        ec.load_config(p)


def test_config_rejects_unknown_template(tmp_path):
    p = tmp_path / "c.json"
    cfg = _base_cfg(tmp_path)
    cfg["templates_enabled"] = ["not_a_template"]
    _write(p, cfg)
    with pytest.raises(ec.ConfigError):
        ec.load_config(p)


# --------------------------------------------------------------------------- #
# Deterministic identifiers.
# --------------------------------------------------------------------------- #
def test_deterministic_ids():
    spec = {"template": "t", "template_version": "1", "feature": "f",
            "universe": "u", "date_start": "a", "date_end": "b",
            "rebalance": "monthly", "horizon_days": 21, "benchmark": "bm",
            "transaction_cost_bps": 25, "required_datasets": ["prices"],
            "leakage_control": "x"}
    fp1 = ec.spec_fingerprint(spec)
    fp2 = ec.spec_fingerprint(dict(spec))
    assert fp1 == fp2
    eid = ec.experiment_id("hyp", "t", fp1, "dv")
    assert eid == ec.experiment_id("hyp", "t", fp1, "dv")
    assert eid.startswith("exp_")
    rid = ec.stage5_run_id("2026-07-29", "cfgh", "srcfp")
    assert rid == ec.stage5_run_id("2026-07-29", "cfgh", "srcfp")
    assert rid.startswith("stage5_")
    # Changing any component changes the id.
    assert eid != ec.experiment_id("hyp", "t", fp1, "dv2")


# --------------------------------------------------------------------------- #
# Source-package verification.
# --------------------------------------------------------------------------- #
def test_verify_source_packages_ok(tmp_path):
    ok, problems = ef.verify_source_packages(_base_cfg(tmp_path),
                                             _pkgs(tmp_path))
    assert ok and not problems


def test_verify_source_packages_missing(tmp_path):
    cfg = _base_cfg(tmp_path)
    pkgs = {"stage1": {}, "stage2": {}, "stage3": {}, "stage3_5": {}}
    ok, problems = ef.verify_source_packages(cfg, pkgs)
    assert not ok and problems


def test_blocked_when_source_missing(tmp_path):
    cfg = _base_cfg(tmp_path)
    res = ef.run_stage5_cycle(cfg, now_iso=_NOW, hypotheses=[_hyp()],
                              source_packages={"stage1": {}, "stage2": {},
                                               "stage3": {}, "stage3_5": {}},
                              store=FakeStore(), ledger_fingerprint=lambda: {})
    assert res["terminal"] == ec.BLOCKED


# --------------------------------------------------------------------------- #
# Hypothesis intake.
# --------------------------------------------------------------------------- #
def test_read_grounded_hypotheses_filters(tmp_path):
    cfg = _base_cfg(tmp_path)
    root = tmp_path / "director"
    rundir = root / "runs" / "s3"
    rundir.mkdir(parents=True)
    _write(root / "latest.json", {"run_id": "s3", "run_dir": "runs/s3"})
    _write(rundir / "hypothesis_proposals.json", {"proposals": [
        {"grounding_validation": {"grounding": "GROUNDED"},
         "proposal": {"hypothesis_id": "h1", "title": "A",
                      "economic_rationale": "price momentum",
                      "information_family": "momentum"}},
        {"grounding_validation": {"grounding": "UNGROUNDED"},
         "proposal": {"hypothesis_id": "h2", "title": "B"}},
        {"grounding_validation": {"grounding": "GROUNDED"},
         "proposal": {"hypothesis_id": "h3", "title": "C"}}]})
    _write(rundir / "research_queue.json", {"queue": [
        {"hypothesis_id": "h1",
         "status": "READY_FOR_DETERMINISTIC_DESIGN_REVIEW", "priority": 1},
        {"hypothesis_id": "h3", "status": "HELD_FOR_DATA", "priority": 2}]})
    hyps = ef.read_grounded_hypotheses(cfg)
    ids = {h["hypothesis_id"] for h in hyps}
    assert ids == {"h1"}  # h2 ungrounded, h3 not accepted in queue


def test_zero_hypotheses_valid_noop(tmp_path):
    cfg = _base_cfg(tmp_path)
    res = _run(cfg, tmp_path, [])
    assert res["terminal"] == ec.NO_EXPERIMENTABLE_HYPOTHESES
    assert res["counts"]["experiments_completed"] == 0
    # Still writes a valid immutable package.
    run_dir = Path(res["run_dir"])
    for name in ec.REQUIRED_RUN_FILES:
        assert (run_dir / name).exists()


# --------------------------------------------------------------------------- #
# Duplicate prevention + unsupported template.
# --------------------------------------------------------------------------- #
def test_duplicate_prevention(tmp_path):
    cfg = _base_cfg(tmp_path)
    store = FakeStore()
    r1 = _run(cfg, tmp_path, [_hyp()], store=store, output_root=None)
    assert r1["counts"]["specs_generated"] >= 1
    # Second cycle, same hypothesis, but force a NEW run by bumping as_of so it
    # is not an idempotent replay — the hypothesis/template is now seen.
    r2 = ef.run_stage5_cycle(cfg, as_of="2026-07-30", now_iso=_NOW,
                             hypotheses=[_hyp()],
                             source_packages=_pkgs(tmp_path), store=store,
                             ledger_fingerprint=lambda: {})
    assert r2["counts"]["duplicates_rejected"] >= 1
    assert r2["counts"]["specs_generated"] == 0


def test_unsupported_template_data_hold(tmp_path):
    cfg = _base_cfg(tmp_path)
    hyp = _hyp(hid="hyp_sup", family="earnings_surprise",
               text="supply-chain supplier read-through with consensus",
               feature_definition="supplier relationship map",
               required_fields=["supplier-customer relationship mapping"])
    res = _run(cfg, tmp_path, [hyp])
    assert res["terminal"] == ec.DATA_HOLD
    gaps = res["data_gaps"]
    assert gaps and gaps[0]["gap_reason"] == ec.DATA_HOLD_UNSUPPORTED_TEMPLATE


@pytest.mark.parametrize("template,family,text", [
    (ec.TPL_PRICE_MOMENTUM, "price_momentum", "price momentum trend"),
    (ec.TPL_FUNDAMENTAL_MOMENTUM, "fundamental_momentum",
     "earnings momentum profitability"),
    (ec.TPL_EVENT_WINDOW, "events", "announcement surprise reaction window"),
    (ec.TPL_EARNINGS_DRIFT, "earnings", "post-earnings drift pead sue"),
    (ec.TPL_INSIDER_EVENT, "insider", "insider buying form 4 activity"),
    (ec.TPL_NEWS_EVENT, "news", "regulatory news sentiment headline"),
    (ec.TPL_SECTOR_RELATIVE, "sector", "sector-relative intra-sector ranking"),
    (ec.TPL_COMBINED_FACTOR, "combined", "combined multi-factor blend"),
])
def test_each_template_maps(template, family, text):
    hyp = _hyp(family=family, text=text, feature_definition=text)
    cand = ef.map_to_templates(hyp, list(ec.SUPPORTED_TEMPLATES))
    assert template in cand


# --------------------------------------------------------------------------- #
# Pure metric math.
# --------------------------------------------------------------------------- #
def test_rank_ic_calc():
    assert ec.spearman([1, 2, 3, 4, 5], [1, 2, 3, 4, 5]) == pytest.approx(1.0)
    assert ec.spearman([1, 2, 3, 4, 5], [5, 4, 3, 2, 1]) == pytest.approx(-1.0)
    assert ec.spearman([1, 2], [1, 2]) is None  # < 3


def test_decile_spread_calc():
    fac = list(range(20))
    fwd = list(range(20))
    sp = ec._decile_spread_one(fac, fwd, 10)
    # top decile (18,19)=18.5 mean, bottom (0,1)=0.5 mean => 18.0
    assert sp == pytest.approx(18.0)


def test_turnover_calc():
    assert ec.turnover({}, {"A": 1.0}) == pytest.approx(0.5)
    assert ec.turnover({"A": 1.0}, {"A": 1.0}) == pytest.approx(0.0)
    assert ec.turnover({"A": 0.5, "B": 0.5},
                       {"B": 0.5, "C": 0.5}) == pytest.approx(0.5)


def test_transaction_cost_calc():
    net = ec.apply_costs([0.01, 0.01], [1.0, 0.0], 25)
    assert net[0] == pytest.approx(0.01 - 25 / 10000.0)
    assert net[1] == pytest.approx(0.01)


def test_drawdown_calc():
    assert ec.max_drawdown([0.1, -0.5, 0.1]) == pytest.approx(-0.5, abs=1e-9)
    assert ec.max_drawdown([0.1, 0.1, 0.1]) == pytest.approx(0.0)


def test_sharpe_and_annualized():
    r = [0.01] * 12
    assert ec.annualized_return(r, periods_per_year=12) == pytest.approx(
        (1.01 ** 12) - 1.0)
    assert ec.sharpe(r, periods_per_year=12) is None  # zero variance
    assert ec.annualized_vol([0.01, -0.01, 0.02],
                             periods_per_year=12) is not None


def test_subperiod_consistency():
    assert ec.subperiod_consistency([0.01] * 8, parts=2) == pytest.approx(1.0)
    assert ec.subperiod_consistency(
        [0.01, 0.01, -0.02, -0.02], parts=2) == pytest.approx(0.5)


# --------------------------------------------------------------------------- #
# Evidence-gate decision surface.
# --------------------------------------------------------------------------- #
def _strong_metrics(**over):
    m = {"observations": 1440, "periods": 24, "universe": 60,
         "missing_data_rate": 0.0, "leakage_warning": False,
         "turnover": 0.2, "gross_annualized_return": 0.28,
         "net_annualized_return": 0.26, "cost_flips_sign": False,
         "cost_erosion_ratio": 0.1, "subperiod_consistency": 1.0,
         "regime_consistency": 1.0, "max_drawdown": -0.15,
         "rank_ic_t": 4.0, "rank_ic_positive_ratio": 0.8, "spread_t": 3.0,
         "oos_ic_mean": 0.02, "benchmark_excess_annualized": 0.05,
         "champion_complementarity": 0.6}
    m.update(over)
    return m


def test_gate_keep():
    d = ec.evaluate_evidence(_strong_metrics(), ec.DEFAULT_GATES)
    assert d["decision"] == ec.KEEP_FOR_RESEARCH


def test_gate_sample_size():
    d = ec.evaluate_evidence(_strong_metrics(observations=50),
                             ec.DEFAULT_GATES)
    assert d["decision"] == ec.NEED_MORE_DATA


def test_gate_leakage():
    d = ec.evaluate_evidence(
        _strong_metrics(leakage_warning=True, leakage_detail="lookahead"),
        ec.DEFAULT_GATES)
    assert d["decision"] == ec.REJECT_LEAKAGE_RISK


def test_gate_cost_sensitivity():
    d = ec.evaluate_evidence(
        _strong_metrics(cost_flips_sign=True, net_annualized_return=-0.01),
        ec.DEFAULT_GATES)
    assert d["decision"] == ec.REJECT_COST_SENSITIVITY


def test_gate_instability():
    d = ec.evaluate_evidence(_strong_metrics(subperiod_consistency=0.5),
                             ec.DEFAULT_GATES)
    assert d["decision"] == ec.REJECT_INSTABILITY


def test_gate_weak_evidence():
    d = ec.evaluate_evidence(_strong_metrics(rank_ic_t=0.5, spread_t=0.3),
                             ec.DEFAULT_GATES)
    assert d["decision"] == ec.REJECT_WEAK_EVIDENCE


def test_gate_experiment_failed():
    d = ec.evaluate_evidence({"experiment_failed": True,
                              "failure_reason": "boom"}, ec.DEFAULT_GATES)
    assert d["decision"] == ec.EXPERIMENT_FAILED


def test_gate_data_hold():
    d = ec.evaluate_evidence({"data_gap": ec.DATA_HOLD_MISSING_DATASET},
                             ec.DEFAULT_GATES)
    assert d["decision"] == ec.DECISION_DATA_HOLD


# --------------------------------------------------------------------------- #
# Coverage / point-in-time / leakage controls.
# --------------------------------------------------------------------------- #
def test_coverage_missing_dataset(tmp_path):
    runner = er.ExperimentRunner(_base_cfg(tmp_path),
                                 store=FakeStore(families_present={"MARKET_BAR"}))
    cov = runner.check_coverage(_hyp(family="fundamental_momentum"),
                                ec.TPL_FUNDAMENTAL_MOMENTUM)
    assert cov["data_gap"] == ec.DATA_HOLD_MISSING_DATASET


def test_coverage_insufficient_history(tmp_path):
    runner = er.ExperimentRunner(_base_cfg(tmp_path),
                                 store=FakeStore(days=100))
    cov = runner.check_coverage(_hyp(), ec.TPL_PRICE_MOMENTUM)
    assert cov["data_gap"] == ec.DATA_HOLD_INSUFFICIENT_HISTORY


def test_coverage_ok(tmp_path):
    runner = er.ExperimentRunner(_base_cfg(tmp_path), store=FakeStore())
    cov = runner.check_coverage(_hyp(), ec.TPL_PRICE_MOMENTUM)
    assert cov["data_gap"] is None and cov["date_start"]


def test_leakage_control_declared():
    # Every template declares a leakage-control rule; factor uses only data
    # through the formation index (return window strictly after).
    for name, tpl in ec.TEMPLATES.items():
        assert tpl["leakage_control"]
    closes = [10.0 * (1.02 ** i) for i in range(300)]
    # Factor at idx=260 must not depend on closes after 260.
    v1 = er._factor_value("mom_12_1", closes, 260)
    closes2 = list(closes)
    closes2[275] = 999.0  # perturb a FUTURE bar
    v2 = er._factor_value("mom_12_1", closes2, 260)
    assert v1 == v2


# --------------------------------------------------------------------------- #
# Scorer end-to-end (benchmark / champion / regime / cost sensitivity + KEEP).
# --------------------------------------------------------------------------- #
def _built_strong(periods=24, universe=60):
    cross = []
    port = []
    turns = []
    bench = []
    for p in range(periods):
        fac = [float(i) for i in range(universe)]
        # Forward return strongly (but imperfectly) monotonic in the factor: a
        # per-period scale varies the decile spread and a varying number of
        # adjacent swaps varies the per-period rank IC — both finite, positive
        # and non-constant so ic_t / spread_t are large and defined.
        scale = 1.0 + (p % 4) * 0.10
        fwd = [float(i) * scale for i in range(universe)]
        for k in range(1 + (p % 5)):
            a = (p * 3 + k * 4) % (universe - 1)
            fwd[a], fwd[a + 1] = fwd[a + 1], fwd[a]
        cross.append((fac, fwd))
        port.append(0.02 + (p % 3) * 0.001)   # long-only top decile
        bench.append(0.01)                     # equal-weight benchmark
        turns.append(0.1)
    return {"cross_sections": cross, "portfolio_returns": port,
            "turnovers": turns, "benchmark_returns": bench,
            "observations": periods * universe, "periods": periods,
            "universe": universe}


def _spec():
    return {"template": ec.TPL_PRICE_MOMENTUM, "feature": "mom_12_1",
            "transaction_cost_bps": 25, "benchmark": "equal_weight_universe",
            "rebalance": "monthly", "horizon_days": 21, "min_universe": 30,
            "min_periods": 12}


def test_score_experiment_keep():
    m = er.score_experiment(_spec(), _built_strong(), gates=ec.DEFAULT_GATES,
                            cost_grid=[10, 25, 50], periods_per_year=12,
                            champion_returns=None)
    assert m["rank_ic_mean"] > 0
    assert m["rank_ic_t"] is not None and m["rank_ic_t"] > 2
    assert m["benchmark_excess_annualized"] > 0
    d = ec.evaluate_evidence(m, ec.DEFAULT_GATES)
    assert d["decision"] == ec.KEEP_FOR_RESEARCH


def test_benchmark_and_champion_comparison():
    champ = [0.005 + (i % 3) * 0.001 for i in range(24)]  # non-constant
    m = er.score_experiment(_spec(), _built_strong(), gates=ec.DEFAULT_GATES,
                            cost_grid=[10, 25, 50], periods_per_year=12,
                            champion_returns=champ)
    assert m["benchmark_name"] == "equal_weight_universe"
    assert m["champion_complementarity"] is not None


def test_regime_consistency_metric():
    m = er.score_experiment(_spec(), _built_strong(), gates=ec.DEFAULT_GATES,
                            cost_grid=[25], periods_per_year=12)
    assert m["regime_consistency"] is not None
    assert m["subperiod_consistency"] is not None


def test_cost_sensitivity_grid():
    m = er.score_experiment(_spec(), _built_strong(), gates=ec.DEFAULT_GATES,
                            cost_grid=[10, 25, 50], periods_per_year=12)
    grid = m["cost_sensitivity"]["grid"]
    assert [g["cost_bps"] for g in grid] == [10, 25, 50]
    # Higher cost never increases net return.
    nets = [g["net_annualized_return"] for g in grid]
    assert nets[0] >= nets[-1]


def test_run_experiment_end_to_end(tmp_path):
    runner = er.ExperimentRunner(_base_cfg(tmp_path), store=FakeStore())
    cov = runner.check_coverage(_hyp(), ec.TPL_PRICE_MOMENTUM)
    spec = ef.build_spec(_hyp(), ec.TPL_PRICE_MOMENTUM, _pkgs(tmp_path),
                         coverage=cov, cfg=_base_cfg(tmp_path))
    m = runner.run_experiment(spec, gates=ec.DEFAULT_GATES,
                              champion="fundamental_momentum_50_50_v1")
    assert m["periods"] >= 12
    assert m["rank_ic_mean"] is not None and m["rank_ic_mean"] > 0


def test_missing_data_gate():
    d = ec.evaluate_evidence(_strong_metrics(missing_data_rate=0.9),
                             ec.DEFAULT_GATES)
    assert d["decision"] == ec.NEED_MORE_DATA


# --------------------------------------------------------------------------- #
# Bounded execution.
# --------------------------------------------------------------------------- #
def test_bounded_hypotheses(tmp_path):
    cfg = _base_cfg(tmp_path)
    hyps = [_hyp(hid="h%d" % i) for i in range(10)]
    res = _run(cfg, tmp_path, hyps)
    assert res["counts"]["hypotheses_considered"] == 3  # max_new_hypotheses


def test_bounded_specs_per_hypothesis(tmp_path):
    cfg = _base_cfg(tmp_path)
    # A hypothesis matching many templates is capped at max_specs_per_hypothesis.
    hyp = _hyp(family="momentum",
               text="combined multi-factor momentum blend price momentum")
    res = _run(cfg, tmp_path, [hyp])
    per_hyp = [s for s in res["specs"] if s["hypothesis_id"] == hyp[
        "hypothesis_id"]]
    assert len(per_hyp) <= 2


def test_bounded_experiments(tmp_path):
    cfg = _base_cfg(tmp_path)
    cfg["bounds"]["max_experiments"] = 2
    cfg["bounds"]["max_new_hypotheses"] = 5
    hyps = [_hyp(hid="h%d" % i, text="price momentum trend %d" % i)
            for i in range(5)]
    res = _run(cfg, tmp_path, hyps)
    assert res["counts"]["experiments_completed"] <= 2


def test_runtime_bound_present():
    assert ec.bound(_base_cfg(Path(".")), "max_runtime_seconds", 0) == 1800
    assert ec.bound(_base_cfg(Path(".")), "max_workers", 0) == 2


# --------------------------------------------------------------------------- #
# Idempotency / resumability / determinism / immutability.
# --------------------------------------------------------------------------- #
def test_idempotent_replay(tmp_path):
    cfg = _base_cfg(tmp_path)
    r1 = _run(cfg, tmp_path, [_hyp()])
    r2 = _run(cfg, tmp_path, [_hyp()])
    assert r2.get("idempotent_replay") is True
    assert r1["run_id"] == r2["run_id"]


def test_resumable_after_interruption(tmp_path):
    cfg = _base_cfg(tmp_path)
    r1 = _run(cfg, tmp_path, [_hyp()])
    # Simulate an interrupted cycle: drop the manifest so replay recomputes.
    (Path(r1["run_dir"]) / "run_manifest.json").unlink()
    r2 = _run(cfg, tmp_path, [_hyp()])
    assert r2["run_id"] == r1["run_id"]
    assert (Path(r2["run_dir"]) / "run_manifest.json").exists()


def test_deterministic_results(tmp_path):
    cfg = _base_cfg(tmp_path)
    a = _run(cfg, tmp_path, [_hyp()], output_root=str(tmp_path / "A"))
    b = _run(cfg, tmp_path, [_hyp()], output_root=str(tmp_path / "B"))
    assert a["run_id"] == b["run_id"]
    ma = json.loads((Path(a["run_dir"]) / "run_manifest.json").read_text())
    mb = json.loads((Path(b["run_dir"]) / "run_manifest.json").read_text())
    assert ma["file_hashes"] == mb["file_hashes"]


def test_immutable_package_hashes_reconcile(tmp_path):
    cfg = _base_cfg(tmp_path)
    res = _run(cfg, tmp_path, [_hyp()])
    run_dir = Path(res["run_dir"])
    manifest = json.loads((run_dir / "run_manifest.json").read_text())
    import hashlib
    for name, want in manifest["file_hashes"].items():
        got = hashlib.sha256((run_dir / name).read_bytes()).hexdigest()
        assert got == want, name


# --------------------------------------------------------------------------- #
# Verify mode (read-only).
# --------------------------------------------------------------------------- #
def test_verify_ok(tmp_path):
    cfg = _base_cfg(tmp_path)
    _run(cfg, tmp_path, [_hyp()])
    v = ef.verify_cycle(cfg, ledger_fingerprint=lambda: {})
    assert v["terminal"] == ec.VERIFIED


def test_verify_writes_nothing(tmp_path):
    cfg = _base_cfg(tmp_path)
    _run(cfg, tmp_path, [_hyp()])
    root = Path(cfg["experiments_root"])
    before = {str(p): p.read_bytes() for p in sorted(root.rglob("*"))
              if p.is_file()}
    ef.verify_cycle(cfg, ledger_fingerprint=lambda: {})
    after = {str(p): p.read_bytes() for p in sorted(root.rglob("*"))
             if p.is_file()}
    assert before == after


def test_verify_detects_tamper(tmp_path):
    cfg = _base_cfg(tmp_path)
    res = _run(cfg, tmp_path, [_hyp()])
    # Tamper with a package file → hash mismatch is detected.
    (Path(res["run_dir"]) / "evidence_metrics.csv").write_text("tampered\n",
                                                               encoding="utf-8")
    v = ef.verify_cycle(cfg, ledger_fingerprint=lambda: {})
    assert v["terminal"] == ec.BLOCKED


# --------------------------------------------------------------------------- #
# Report model + rendering (Stage 4 integration surface).
# --------------------------------------------------------------------------- #
def test_experiment_report_model(tmp_path):
    cfg = _base_cfg(tmp_path)
    res = _run(cfg, tmp_path, [_hyp()])
    model = ef.experiment_report_model(res)
    assert model["run_id"] == res["run_id"]
    assert "next_action" in model
    assert model["champion_model"] == "fundamental_momentum_50_50_v1"


def test_report_section_rendered():
    model = {"cycle_label": "morning", "cycle_date": "2026-07-29",
             "experiment": {"run_id": "stage5_x", "terminal": ec.READY,
                            "status": "EXPERIMENTS_COMPLETE",
                            "hypotheses_considered": 1,
                            "experiments_started": 1,
                            "experiments_completed": 1, "experiments_failed": 0,
                            "data_gaps": 0, "duplicates_rejected": 0,
                            "keep_for_research": 1,
                            "decision_counts": {"KEEP_FOR_RESEARCH": 1},
                            "strongest": {"hypothesis_id": "h1",
                                          "template": "price_momentum_rank",
                                          "rank_ic_t": 3.2},
                            "benchmark_comparison": {"excess_annualized": 0.05},
                            "cost_sensitivity": {"flips_sign": False},
                            "champion_model": "fundamental_momentum_50_50_v1",
                            "next_action": "Human review."}}
    html = rr.render_html(model)
    text = rr.render_text(model)
    assert "Experiment &amp; Evidence" in html
    assert "stage5_x" in html
    assert "EXPERIMENT & EVIDENCE" in text
    assert "KEEP_FOR_RESEARCH is not model promotion" in html


def test_report_section_when_not_run():
    model = {"cycle_label": "morning", "cycle_date": "2026-07-29",
             "experiment": None}
    html = rr.render_html(model)
    assert "Stage 5 experiment engine not run" in html


# --------------------------------------------------------------------------- #
# Hard safety invariants.
# --------------------------------------------------------------------------- #
_STAGE5_SOURCES = ("experiment_contracts.py", "experiment_factory.py",
                   "experiment_runner.py")


def test_no_llm_or_network_in_sources():
    for mod in _STAGE5_SOURCES:
        low = (_REPO / "alpha_agent" / mod).read_text(encoding="utf-8").lower()
        for bad in ("llm_providers", "anthropic", "requests.", "urllib",
                    "socket", "http.client", "smtplib"):
            assert bad not in low, "%s in %s" % (bad, mod)


def test_no_postgres_or_prediction_in_sources():
    # Scan for concrete DB/prediction USAGE (not the safety prose that mentions
    # PostgreSQL / the prediction service to declare they are never used).
    for mod in _STAGE5_SOURCES:
        low = (_REPO / "alpha_agent" / mod).read_text(encoding="utf-8").lower()
        for bad in ("psycopg", "postgresql://", "import api", "from api",
                    "127.0.0.1:9000", ":9000/"):
            assert bad not in low, "%s in %s" % (bad, mod)


def test_no_order_signal_fill_decision_creation():
    for mod in _STAGE5_SOURCES:
        low = (_REPO / "alpha_agent" / mod).read_text(encoding="utf-8").lower()
        for bad in ("create_order", "submit_order", "place_order",
                    "create_signal", "create_fill", "trade_decision",
                    "daily_close", "promote_model"):
            assert bad not in low, "%s in %s" % (bad, mod)


def test_operational_ledger_immutability(tmp_path):
    cfg = _base_cfg(tmp_path)
    ledger = tmp_path / "ledger"
    ledger.mkdir()
    (ledger / "book.json").write_text('{"nav": 100000}', encoding="utf-8")
    import hashlib

    def fp():
        return {str(p): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in sorted(ledger.rglob("*")) if p.is_file()}

    before = fp()
    ef.run_stage5_cycle(cfg, now_iso=_NOW, hypotheses=[_hyp()],
                        source_packages=_pkgs(tmp_path), store=FakeStore(),
                        ledger_fingerprint=fp)
    assert fp() == before


def test_ledger_mutation_blocks(tmp_path):
    cfg = _base_cfg(tmp_path)
    state = {"n": 0}

    def fp():
        state["n"] += 1
        return {"file": "hash_%d" % state["n"]}  # changes between before/after

    res = ef.run_stage5_cycle(cfg, now_iso=_NOW, hypotheses=[_hyp()],
                              source_packages=_pkgs(tmp_path),
                              store=FakeStore(), ledger_fingerprint=fp)
    assert res["terminal"] == ec.BLOCKED
    assert res["status"] == "LEDGER_MUTATION_DETECTED"


def test_state_db_schema_has_required_tables(tmp_path):
    cfg = _base_cfg(tmp_path)
    _run(cfg, tmp_path, [_hyp()])
    conn = sqlite3.connect(str(ef.state_db_path(Path(cfg["experiments_root"]))))
    have = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    conn.close()
    for t in ec.REQUIRED_TABLES:
        assert t in have
