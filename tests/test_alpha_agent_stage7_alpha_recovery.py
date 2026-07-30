"""
Focused tests for Alpha Agent Stage 7 — alpha recovery (evidence observatory,
champion autopsy, risk-overlay tournament, bounded alpha campaign, promotion
gate). Deterministic synthetic fixtures + injected fakes only: no network,
Norgate, PostgreSQL, prediction service, LLM, email or scheduled task.
"""
from __future__ import annotations

import datetime
import json
from pathlib import Path

import pytest

from paper_trader.alpha_agent import champion_forensics as cf
from paper_trader.alpha_agent import evidence_observatory as eo
from paper_trader.alpha_agent import experiment_contracts as ec
from paper_trader.alpha_agent import experiment_runner as er
from paper_trader.alpha_agent import risk_overlay_research as ro


# --------------------------------------------------------------------------- #
# Deterministic synthetic survivorship-free panel.
# --------------------------------------------------------------------------- #
def _dates(n_days: int) -> list:
    out = []
    dt = datetime.date(2015, 1, 2)
    while len(out) < n_days:
        if dt.weekday() < 5:
            out.append(dt.isoformat())
        dt = dt + datetime.timedelta(days=1)
    return out


def _panel(n_tickers: int = 30, n_days: int = 1000, seed: int = 7) -> dict:
    dates = _dates(n_days)
    panel = {}
    for k in range(n_tickers):
        tkr = "T%02d" % k
        drift = 0.0002 * ((k % 7) - 3)
        px = 100.0 * (1 + 0.1 * k / n_tickers)
        state = seed * 131 + k * 17
        series = []
        for dd in dates:
            state = (1103515245 * state + 12345) & 0x7FFFFFFF
            u = state / 0x7FFFFFFF - 0.5
            px *= (1.0 + drift + 0.01 * u)
            series.append((dd, round(px, 4)))
        panel[tkr] = series
    return panel


def _spy(n_days: int = 1000, seed: int = 99) -> list:
    dates = _dates(n_days)
    px = 200.0
    state = seed
    out = []
    for dd in dates:
        state = (1103515245 * state + 12345) & 0x7FFFFFFF
        u = state / 0x7FFFFFFF - 0.48
        px *= (1.0 + 0.0003 + 0.008 * u)
        out.append((dd, round(px, 4)))
    return out


_POLICY = {"strategy": "fundamental_momentum_50_50_v1",
           "target_book": "fundamental_momentum_50_50_top25",
           "target_position_count": 25, "target_weight_per_name_pct": 4.0,
           "maximum_position_weight_pct": 5.0, "maximum_sector_weight_pct": 25.0}
_SECTORS = {"T%02d" % k: ["Tech", "Health", "Fin", "Energy", "Cons"][k % 5]
            for k in range(30)}


@pytest.fixture(scope="module")
def built():
    panel = _panel()
    spy = _spy()
    recon = cf.reconstruct_champion(panel, policy=_POLICY, sector_map=_SECTORS,
                                    spy_series=spy, benchmark_source="SPY",
                                    top_n=25)
    holdings = [{"ticker": t, "weight": 0.953 / 25}
                for t in recon["reconstructed_top_names"][:25]]
    return_panel = ro.build_return_panel(panel)
    spy_ret = ro.series_to_returns(spy)
    tour = ro.run_overlay_tournament(holdings, return_panel, spy_returns=spy_ret,
                                     spy_levels=spy, sector_map=_SECTORS,
                                     specs=ro.default_overlay_specs(), cost_bps=10)
    champ_returns = [p["book_return"] for p in recon["per_period"]]
    gates = dict(ec.DEFAULT_GATES, min_observations=50, min_periods=12)
    camp = er.run_price_factor_campaign(panel, gates=gates,
                                        champion_returns=champ_returns,
                                        spy_series=spy,
                                        bounds={"max_new_hypotheses": 6,
                                                "max_specs_per_hypothesis": 2,
                                                "max_experiments": 12})
    return {"panel": panel, "spy": spy, "recon": recon, "holdings": holdings,
            "return_panel": return_panel, "tour": tour, "camp": camp}


# --------------------------------------------------------------------------- #
# Contracts.
# --------------------------------------------------------------------------- #
def test_terminal_tokens_unique_and_stage7():
    toks = eo.all_terminal_tokens()
    assert len(toks) == len(set(toks))
    assert all(t.startswith("ALPHA_AGENT_STAGE7") for t in toks)


def test_recovery_dispositions_exact():
    assert eo.RECOVERY_DISPOSITIONS == frozenset({
        "CHAMPION_CONFIRMED_RISK_OVERLAY_REQUIRED",
        "CHAMPION_CONFIRMED_NO_CHANGE", "CHAMPION_REJECTED",
        "NEED_MORE_EVIDENCE"})


def test_required_run_files_complete():
    for f in ("stage7_input.json", "evidence_inventory.json",
              "champion_reconstruction.json", "overlay_results.csv",
              "recovery_disposition.json", "manual_risk_preview.json",
              "remaining_data_gaps.jsonl", "stage7_recovery_report.md",
              "run_manifest.json"):
        assert f in eo.REQUIRED_RUN_FILES


def test_config_rejects_embedded_secret(tmp_path):
    p = tmp_path / "cfg.json"
    p.write_text(json.dumps({"stage": "7", "recovery_root": "x",
                             "stage_roots": {}, "operational_ledger_roots": [],
                             "champion": {}, "bounds": {}, "overlays": {},
                             "promotion_gate": {},
                             "api_key": "sk-ant-abcdefghijklmnop"}),
                 encoding="utf-8")
    with pytest.raises(eo.ConfigError):
        eo.load_config(p)


def test_config_requires_stage7(tmp_path):
    p = tmp_path / "cfg.json"
    p.write_text(json.dumps({"stage": "6", "recovery_root": "x",
                             "stage_roots": {}, "operational_ledger_roots": [],
                             "champion": {}, "bounds": {}, "overlays": {},
                             "promotion_gate": {}}), encoding="utf-8")
    with pytest.raises(eo.ConfigError):
        eo.load_config(p)


def test_shipped_config_loads_and_scans_clean():
    repo = Path(__file__).resolve().parents[1]
    cfg = eo.load_config(repo / "configs" / "alpha_agent"
                         / "stage7_alpha_recovery.json")
    assert cfg["stage"] == "7"
    assert cfg["champion"]["model_id"] == "fundamental_momentum_50_50_v1"


# --------------------------------------------------------------------------- #
# Champion autopsy.
# --------------------------------------------------------------------------- #
def test_champion_classification_exact_partial_unverifiable(built):
    classes = {c["component"]: c["class"]
               for c in built["recon"]["classification"]}
    assert classes["portfolio_construction"] == eo.RECON_EXACT
    assert classes["selection_signal_price_leg"] == eo.RECON_PARTIAL
    assert classes["fundamental_leg_point_in_time"] == eo.RECON_UNVERIFIABLE


def test_no_fundamental_lookahead_leg_never_computed(built):
    # The fundamental leg is UNVERIFIABLE and is never turned into a return
    # series: the reconstruction basis is explicitly the price leg only.
    assert "price-momentum" in built["recon"]["reconstruction_basis"]
    detail = next(c["detail"] for c in built["recon"]["classification"]
                  if c["component"] == "fundamental_leg_point_in_time")
    assert "NOT reconstructed" in detail or "not reconstructed" in detail


def test_champion_forensics_full_battery(built):
    f = built["recon"]["forensics"]
    for k in ("periods", "gross_annualized_return", "annualized_vol",
              "max_drawdown", "rank_ic_t", "decile_spread_mean", "turnover_mean",
              "market_beta", "top5_contribution_share", "name_concentration_hhi",
              "sector_concentration_hhi", "rolling_oos"):
        assert k in f
    assert f["periods"] > 0
    assert len(f["rolling_oos"]) == 3


def test_construction_matches_operational_top25(built):
    cm = built["recon"]["construction_match"]
    assert cm["operational_top25_matches_research"] is True
    assert cm["reconstructed_position_count"] == 25
    assert cm["name_cap_respected"] is True


def test_equal_dollar_unequal_risk_flag_present(built):
    assert "equal_dollar_implies_unequal_risk" in built["recon"]["forensics"]


def test_champion_insufficient_history_is_honest():
    tiny = {t: s[:100] for t, s in _panel(n_tickers=30, n_days=200).items()}
    recon = cf.reconstruct_champion(tiny, policy=_POLICY)
    assert recon["forensics"].get("periods") == 0
    # classification still names all three components honestly.
    assert len(recon["classification"]) == 3


# --------------------------------------------------------------------------- #
# Historical vs forward separation.
# --------------------------------------------------------------------------- #
def test_historical_and_forward_evidence_separated(built):
    # Champion forensics is HISTORICAL reconstruction; the forward sample is a
    # separate input to the disposition and is never mixed into the forensics.
    disp = ro.synthesize_disposition(
        champion_recon=built["recon"], overlay=built["tour"],
        campaign=built["camp"], forward_context={"observations": 5},
        gates={"min_ic_t": 2.0, "min_forward_observations": 20})
    assert disp["inputs"]["forward_observations"] == 5
    assert disp["inputs"]["forward_sufficient"] is False
    assert "forensics" in built["recon"]


# --------------------------------------------------------------------------- #
# Risk-overlay tournament.
# --------------------------------------------------------------------------- #
def test_all_fixed_overlay_variants_present(built):
    labels = {r["overlay"] for r in built["tour"]["overlays"]}
    for expected in ("CURRENT_CONTROL", "INVERSE_VOL_NAME_CAPPED",
                     "PORTFOLIO_VOL_TARGET_15", "PORTFOLIO_VOL_TARGET_20",
                     "PORTFOLIO_VOL_TARGET_25", "RISK_CONTRIBUTION_CAPPED",
                     "SECTOR_RISK_CAPPED", "EXTREME_VOLATILITY_SLEEVE_CAP",
                     "MARKET_REGIME_CASH_OVERLAY"):
        assert expected in labels


def test_no_leverage_gross_le_one(built):
    for r in built["tour"]["overlays"]:
        # avg cash weight ≥ 0 ⇒ gross ≤ 1 (residual is cash, never leverage).
        assert (r.get("avg_cash_weight") or 0.0) >= -1e-9
        if r.get("max_name_weight") is not None:
            assert r["max_name_weight"] <= 1.0 + 1e-9


def test_residual_is_cash_never_negative(built):
    for r in built["tour"]["overlays"]:
        assert (r.get("avg_cash_weight") or 0.0) >= -1e-9


def test_risk_contribution_reconciles_to_one(built):
    for lbl in ("CURRENT_CONTROL", "INVERSE_VOL_NAME_CAPPED"):
        rc = [x for x in built["tour"]["risk_contributions"]
              if x["overlay"] == lbl]
        assert rc, "risk contributions present for %s" % lbl
        total = sum(x["pct_risk_contribution"] for x in rc)
        assert abs(total - 1.0) < 1e-6


def test_transaction_costs_applied(built):
    cs = built["tour"]["cost_sensitivity"]
    assert cs
    # higher cost ⇒ lower (or equal) net return for a given overlay
    ctrl = sorted([r for r in cs if r["overlay"] == "CURRENT_CONTROL"],
                  key=lambda r: r["cost_bps"])
    nets = [r["net_annualized_return"] for r in ctrl
            if r["net_annualized_return"] is not None]
    assert nets == sorted(nets, reverse=True) or len(set(nets)) <= 1


def test_subperiods_and_regimes_present(built):
    assert built["tour"]["subperiods"]
    assert built["tour"]["regimes"]
    # each overlay has 2 subperiods
    per = {}
    for s in built["tour"]["subperiods"]:
        per[s["overlay"]] = per.get(s["overlay"], 0) + 1
    assert all(v == 2 for v in per.values())


def test_overlay_insufficient_history_is_controlled():
    tiny_panel = _panel(n_tickers=5, n_days=70)
    holdings = [{"ticker": t, "weight": 0.2} for t in tiny_panel]
    rp = ro.build_return_panel(tiny_panel)
    tour = ro.run_overlay_tournament(holdings, rp, sector_map=_SECTORS)
    assert tour["status"] == "INSUFFICIENT_HISTORY"
    assert tour["overlays"] == []


# --------------------------------------------------------------------------- #
# Bounded alpha campaign.
# --------------------------------------------------------------------------- #
def test_campaign_respects_limits(built):
    plan = built["camp"]["plan"]
    assert len(plan["hypotheses"]) <= 6
    assert plan["experiments_planned"] <= 12
    # each hypothesis ≤ 2 specs
    per = {}
    for r in built["camp"]["results"]:
        per[r["hypothesis_id"]] = per.get(r["hypothesis_id"], 0) + 1
    assert all(v <= 2 for v in per.values())


def test_campaign_decisions_in_vocabulary(built):
    for d in built["camp"]["decisions"]:
        assert d["decision"] in (ec.EVIDENCE_DECISIONS | {"DATA_HOLD"})


def test_campaign_holds_non_pit_and_missing_factors(built):
    held = {h["feature"] for h in built["camp"]["held"]}
    assert "sector_neutral_momentum" in held
    assert "insider_event" in held and "news_event" in held


def test_campaign_null_control_is_weak():
    # A pure null (deterministic pseudo-random) factor must NOT be a strong
    # signal — guards against a harness that manufactures alpha.
    panel = _panel()
    null_built = er._build_campaign_cross_sections(
        panel, feature="null_control", horizon_days=21, rebalance="monthly")
    ic_t = ec.tstat(ec.rank_ic_series(null_built["cross_sections"]))
    assert ic_t is None or abs(ic_t) < 2.0


def test_campaign_does_not_force_winner():
    # A structureless (flat) panel yields no strong evidence — no forced KEEP.
    dates = _dates(1000)
    flat = {"T%02d" % k: [(d, 100.0 + 0.0 * i) for i, d in enumerate(dates)]
            for k in range(30)}
    camp = er.run_price_factor_campaign(
        flat, gates=dict(ec.DEFAULT_GATES, min_observations=50, min_periods=12),
        bounds={"max_new_hypotheses": 6, "max_specs_per_hypothesis": 2,
                "max_experiments": 12})
    assert camp["keep_for_research"] == 0


# --------------------------------------------------------------------------- #
# Disposition + manual preview + promotion gate.
# --------------------------------------------------------------------------- #
def test_disposition_need_more_evidence_when_fundamental_unverifiable(built):
    disp = ro.synthesize_disposition(
        champion_recon=built["recon"], overlay=built["tour"],
        campaign=built["camp"], forward_context={"observations": 5},
        gates={"min_ic_t": 2.0, "min_forward_observations": 20})
    assert disp["disposition"] == eo.DISP_NEED_MORE_EVIDENCE
    assert disp["no_operational_change"] is True


def test_manual_preview_withheld_without_robust_evidence(built):
    disp = ro.synthesize_disposition(
        champion_recon=built["recon"], overlay=built["tour"],
        campaign=built["camp"], forward_context={"observations": 5})
    prev = ro.build_manual_risk_preview(overlay=built["tour"],
                                        disposition=disp,
                                        holdings=built["holdings"])
    assert prev["status"] == "WITHHELD_NO_ROBUST_EVIDENCE"
    assert prev["is_order"] is False and prev["is_target"] is False


def test_promotion_gate_closed_and_shadow_gated():
    gate = eo.promotion_checklist({})
    assert gate["promotion_allowed_now"] is False
    assert gate["shadow_challenger_allowed_now"] is False
    assert len(gate["requirements"]) == len(eo.PROMOTION_REQUIREMENTS)


def test_promotion_gate_all_met_allows_shadow_only():
    ev = {k: True for k, _ in eo.PROMOTION_REQUIREMENTS}
    gate = eo.promotion_checklist(ev)
    assert gate["shadow_challenger_allowed_now"] is True
    assert gate["promotion_allowed_now"] is False


# --------------------------------------------------------------------------- #
# Evidence inventory (missing / stale / exact paths).
# --------------------------------------------------------------------------- #
def _inv_cfg(tmp_path: Path) -> dict:
    roots = {}
    for k in eo.STAGE_KEYS:
        roots[k] = str(tmp_path / k)
    return {"stage": "7", "recovery_root": str(tmp_path / "stage7"),
            "stage_roots": roots, "operational_ledger_roots": [],
            "champion": {"model_id": "fundamental_momentum_50_50_v1"},
            "bounds": {}, "overlays": {}, "promotion_gate": {},
            "freshness": {"stale_after_days": 7}}


def test_inventory_handles_missing_runs(tmp_path):
    cfg = _inv_cfg(tmp_path)
    inv = eo.build_evidence_inventory(cfg, today="2026-07-30")
    assert set(inv["stages"].keys()) == set(eo.STAGE_KEYS)
    assert inv["stages"]["stage5"]["status"] == "UNAVAILABLE"


def test_inventory_reads_run_and_marks_stale(tmp_path):
    cfg = _inv_cfg(tmp_path)
    s5 = tmp_path / "stage5"
    run = s5 / "runs" / "stage5_abc"
    run.mkdir(parents=True)
    (s5 / "latest.json").write_text(json.dumps({"run_id": "stage5_abc"}),
                                    encoding="utf-8")
    (run / "evidence_metrics.csv").write_text(
        "experiment_id,decision\ne1,REJECT_WEAK_EVIDENCE\n", encoding="utf-8")
    (run / "evidence_decisions.jsonl").write_text(
        json.dumps({"decision": "REJECT_WEAK_EVIDENCE"}) + "\n",
        encoding="utf-8")
    # Force the package mtime well into the past so the freshness clock trips.
    import os
    old = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc).timestamp()
    for f in (s5 / "latest.json", run):
        os.utime(f, (old, old))
    inv = eo.build_evidence_inventory(cfg, today="2026-07-30")
    s5b = inv["stages"]["stage5"]
    assert s5b["run_id"] == "stage5_abc"
    assert s5b["experiments_completed"] == 1
    assert s5b["stale"] is True  # mtime 2025-01-01, today 2026-07-30


def test_inventory_exact_evidence_paths(tmp_path):
    cfg = _inv_cfg(tmp_path)
    inv = eo.build_evidence_inventory(cfg, today="2026-07-30")
    paths = {p["stage"]: p["path"] for p in inv["evidence_paths"]}
    assert len(paths) == len(eo.STAGE_KEYS)
    # every path is a concrete string under the configured roots
    assert all(isinstance(v, str) and v for v in paths.values())


def test_source_freshness_rows(tmp_path):
    cfg = _inv_cfg(tmp_path)
    inv = eo.build_evidence_inventory(cfg, today="2026-07-30")
    rows = eo.source_freshness_rows(inv, today="2026-07-30")
    assert len(rows) == len(eo.STAGE_KEYS)
    assert all("status" in r and "path" in r for r in rows)


def test_observatory_payload_missing_config_is_controlled():
    out = eo.observatory_payload(config_path="does_not_exist.json")
    assert out["status"] == "UNAVAILABLE"
    assert "PAPER ONLY" in out["safety_badges"]


# --------------------------------------------------------------------------- #
# Package assembly + verify + tamper + ledger immutability.
# --------------------------------------------------------------------------- #
def _assemble(tmp_path, built):
    cfg = _inv_cfg(tmp_path)
    cfg["recovery_root"] = str(tmp_path / "stage7")
    recon, tour, camp = built["recon"], built["tour"], built["camp"]
    disp = ro.synthesize_disposition(
        champion_recon=recon, overlay=tour, campaign=camp,
        forward_context={"observations": 5})
    manual = ro.build_manual_risk_preview(overlay=tour, disposition=disp,
                                          holdings=built["holdings"])
    promo = eo.promotion_checklist(
        ro.promotion_evidence(campaign=camp, champion_recon=recon, overlay=tour))
    inv = eo.build_evidence_inventory(cfg, today="2026-07-30")
    gaps = [{"gap": "POINT_IN_TIME_FUNDAMENTALS", "detail": "held"}]
    run_id = "stage7_test0001"
    campaign_out = dict(camp, overlay_specs=ro.default_overlay_specs(cfg))
    pkg = ro.assemble_and_write(
        cfg, run_id=run_id, as_of="2026-07-30",
        stage7_input={"terminal": eo.READY, "as_of": "2026-07-30"},
        inventory=inv, champion_recon=recon, overlay=tour,
        campaign=campaign_out, disposition=disp, manual_preview=manual,
        gaps=gaps, promotion=promo, report_md="# report\n", write=True)
    return cfg, run_id, pkg


def test_package_written_with_all_required_files(tmp_path, built):
    cfg, run_id, pkg = _assemble(tmp_path, built)
    run_dir = Path(pkg["run_dir"])
    for f in eo.REQUIRED_RUN_FILES:
        assert (run_dir / f).exists(), "missing %s" % f
    latest = json.loads((Path(cfg["recovery_root"]) / "latest.json")
                        .read_text(encoding="utf-8"))
    assert latest["run_id"] == run_id


def test_verify_passes_on_clean_package(tmp_path, built):
    cfg, run_id, _ = _assemble(tmp_path, built)
    res = ro.verify_recovery(cfg, run_id=run_id)
    assert res["terminal"] == eo.VERIFIED
    assert res["disposition_valid"] is True


def test_verify_detects_tamper(tmp_path, built):
    cfg, run_id, pkg = _assemble(tmp_path, built)
    tampered = Path(pkg["run_dir"]) / "recovery_disposition.json"
    tampered.write_text(tampered.read_text(encoding="utf-8") + " ",
                        encoding="utf-8")
    res = ro.verify_recovery(cfg, run_id=run_id)
    assert res["terminal"] == eo.BLOCKED
    assert "recovery_disposition.json" in res["hash_mismatch"]


def test_no_operational_ledger_mutation(tmp_path, built):
    # Simulate an operational ledger; the recovery write must not touch it.
    ledger_root = tmp_path / "ledgers"
    ledger_root.mkdir()
    lf = ledger_root / "book.json"
    lf.write_text('{"chain":"immutable"}', encoding="utf-8")
    import hashlib
    before = hashlib.sha256(lf.read_bytes()).hexdigest()
    cfg = _inv_cfg(tmp_path)
    cfg["recovery_root"] = str(tmp_path / "stage7")
    cfg["operational_ledger_roots"] = [str(ledger_root)]
    inv = eo.build_evidence_inventory(cfg, today="2026-07-30")
    ro.assemble_and_write(
        cfg, run_id="stage7_x", as_of="2026-07-30",
        stage7_input={"terminal": eo.READY}, inventory=inv,
        champion_recon=built["recon"], overlay=built["tour"],
        campaign=dict(built["camp"]),
        disposition={"disposition": eo.DISP_NEED_MORE_EVIDENCE},
        manual_preview={"status": "WITHHELD_NO_ROBUST_EVIDENCE"},
        gaps=[], promotion=eo.promotion_checklist({}), report_md="# r\n",
        write=True)
    after = hashlib.sha256(lf.read_bytes()).hexdigest()
    assert after == before


def test_recovery_root_is_only_write_target(tmp_path, built):
    cfg, run_id, pkg = _assemble(tmp_path, built)
    # Only the recovery_root tree contains new files; stage roots stay empty.
    for k in ("stage1", "stage2", "stage3", "stage5", "stage6"):
        root = Path(cfg["stage_roots"][k])
        assert not root.exists() or not any(root.rglob("*"))


# --------------------------------------------------------------------------- #
# Stage 7.1 — forward risk shadows, upstream fingerprint, cadence decision.
# --------------------------------------------------------------------------- #
def test_forward_shadow_reference_exactly_three(built):
    ref = built["tour"].get("forward_shadow_reference")
    assert ref is not None and len(ref) == 3
    names = {r["overlay"] for r in ref}
    assert names == {"CURRENT_CONTROL", "MARKET_REGIME_CASH_OVERLAY",
                     "PORTFOLIO_VOL_TARGET_20"}
    by = {r["overlay"]: r for r in ref}
    # Control scale is exactly 1; every scale in [0, 1] (no leverage) and cash
    # is the invested-gross residual.
    assert abs(by["CURRENT_CONTROL"]["forward_scale"] - 1.0) < 1e-9
    for r in ref:
        assert 0.0 <= r["forward_scale"] <= 1.0 + 1e-9
        assert abs((r["invested_gross"] + r["cash"]) - 1.0) < 1e-6 \
            or r["cash"] >= 0.0


def _write_forward_pkg(tmp_path, forward_shadows):
    root = tmp_path / "recovery"
    rid = "stage7_fwd"
    rd = root / "runs" / rid
    rd.mkdir(parents=True)
    (root / "latest.json").write_text(json.dumps({
        "run_id": rid, "as_of": "2026-07-29",
        "holdings_source": "RECONSTRUCTED_CHAMPION_PROXY"}), encoding="utf-8")
    (rd / "run_manifest.json").write_text(json.dumps({
        "forward_shadows": forward_shadows}), encoding="utf-8")
    (rd / "overlay_results.csv").write_text(
        "overlay,annualized_vol,turnover_annualized,cost_drag_annualized\n"
        "CURRENT_CONTROL,0.28,0.5,0.005\n"
        "MARKET_REGIME_CASH_OVERLAY,0.24,0.6,0.006\n"
        "PORTFOLIO_VOL_TARGET_20,0.17,0.4,0.004\n", encoding="utf-8")
    return {"recovery_root": str(root)}


def test_build_forward_shadows_three_and_scaled(tmp_path):
    fwd = [{"overlay": "CURRENT_CONTROL", "invested_gross": 0.95,
            "cash": 0.05, "forward_scale": 1.0},
           {"overlay": "MARKET_REGIME_CASH_OVERLAY", "invested_gross": 0.475,
            "cash": 0.525, "forward_scale": 0.5},
           {"overlay": "PORTFOLIO_VOL_TARGET_20", "invested_gross": 0.6,
            "cash": 0.4, "forward_scale": 0.6}]
    cfg = _write_forward_pkg(tmp_path, fwd)
    series = [{"date": "2026-07-28", "book_return": -0.01, "spy_return": -0.008},
              {"date": "2026-07-29", "book_return": 0.02, "spy_return": 0.015}]
    out = ro.build_forward_shadows(cfg, forward_series=series,
                                   baseline_date="2026-07-27")
    assert out["status"] == "OK"
    shadows = out["shadows"]
    assert len(shadows) == 3
    by = {s["overlay"]: s for s in shadows}
    # The control tracks the book 1:1; the regime overlay is scaled by 0.5.
    ctrl = by["CURRENT_CONTROL"]
    reg = by["MARKET_REGIME_CASH_OVERLAY"]
    assert ctrl["observations"] == 2 and reg["observations"] == 2
    # Regime daily return == 0.5 x control daily return (each aligned day).
    assert abs(reg["daily_return"] - 0.5 * ctrl["daily_return"]) < 1e-9
    assert reg["cash"] == 0.525
    # Vol is withheld below the minimum observation count.
    assert ctrl["realized_vol"] is None


def test_build_forward_shadows_no_observations(tmp_path):
    cfg = _write_forward_pkg(tmp_path, [])
    out = ro.build_forward_shadows(cfg, forward_series=[])
    assert out["status"] == "NO_FORWARD_OBSERVATIONS"
    # Still exactly three tracked overlays, each with zero observations.
    assert len(out["shadows"]) == 3
    assert all(s["observations"] == 0 for s in out["shadows"])


def test_upstream_fingerprint_changes_with_evidence(tmp_path):
    def _mk(stage3_run, s6_dv):
        r = tmp_path / stage3_run
        (r / "s3").mkdir(parents=True, exist_ok=True)
        (r / "s3" / "latest.json").write_text(
            json.dumps({"run_id": stage3_run}), encoding="utf-8")
        (r / "s6").mkdir(parents=True, exist_ok=True)
        (r / "s6" / "latest.json").write_text(
            json.dumps({"data_version": s6_dv}), encoding="utf-8")
        return {"stage_roots": {"stage3": str(r / "s3"),
                                "stage6": str(r / "s6")}}
    a = ro.upstream_evidence_fingerprint(_mk("run_a", "dv1"))
    b = ro.upstream_evidence_fingerprint(_mk("run_b", "dv1"))
    c = ro.upstream_evidence_fingerprint(_mk("run_a", "dv2"))
    assert a["fingerprint"] != b["fingerprint"]  # stage 3 run changed
    assert a["fingerprint"] != c["fingerprint"]  # stage 6 data version changed


def test_stage7_cadence_actions_are_deterministic():
    friday = ro.stage7_cadence_decision(
        label="post_close", weekday=4, current_fingerprint="x",
        latest_manifest={"upstream_fingerprint": "x"})
    assert friday["action"] == ro.CAD_RUN_FULL
    delta = ro.stage7_cadence_decision(
        label="morning", weekday=2, current_fingerprint="new",
        latest_manifest={"upstream_fingerprint": "old"})
    assert delta["action"] == ro.CAD_RUN_DELTA
    reuse = ro.stage7_cadence_decision(
        label="morning", weekday=2, current_fingerprint="same",
        latest_manifest={"upstream_fingerprint": "same"})
    assert reuse["action"] == ro.CAD_REUSE
    assert reuse["action"] in ro.STAGE7_CADENCE_ACTIONS


def test_forward_shadow_reference_persisted_in_manifest(tmp_path, built):
    cfg, run_id, pkg = _assemble(tmp_path, built)
    manifest = json.loads((Path(pkg["run_dir"]) / "run_manifest.json")
                          .read_text(encoding="utf-8"))
    fs = manifest.get("forward_shadows")
    assert fs and len(fs) == 3
    assert manifest.get("upstream_fingerprint")  # deterministic marker present


# --------------------------------------------------------------------------- #
# Stage 7.1 correction — immutable-evidence: run identity + no-overwrite guard.
# --------------------------------------------------------------------------- #
def _awrite_kwargs(cfg, built, run_id, as_of="2026-07-30"):
    """Full assemble_and_write kwargs for an identity-stable rerun."""
    recon, tour, camp = built["recon"], built["tour"], built["camp"]
    disp = ro.synthesize_disposition(champion_recon=recon, overlay=tour,
                                     campaign=camp,
                                     forward_context={"observations": 5})
    manual = ro.build_manual_risk_preview(overlay=tour, disposition=disp,
                                          holdings=built["holdings"])
    promo = eo.promotion_checklist(
        ro.promotion_evidence(campaign=camp, champion_recon=recon, overlay=tour))
    inv = eo.build_evidence_inventory(cfg, today="2026-07-30")
    return dict(run_id=run_id, as_of=as_of,
                stage7_input={"terminal": eo.READY, "as_of": as_of},
                inventory=inv, champion_recon=recon, overlay=tour,
                campaign=dict(camp, overlay_specs=ro.default_overlay_specs(cfg)),
                disposition=disp, manual_preview=manual,
                gaps=[{"gap": "POINT_IN_TIME_FUNDAMENTALS", "detail": "held"}],
                promotion=promo, report_md="# report\n", write=True)


def _hash_tree(root: Path) -> dict:
    import hashlib
    out = {}
    for f in sorted(root.rglob("*")):
        if f.is_file():
            out[str(f.relative_to(root))] = hashlib.sha256(f.read_bytes()).hexdigest()
    return out


def test_stage7_run_id_binds_schema_and_engine():
    base = eo.stage7_run_id("2026-07-30", "cfgh", "evfp")
    # A schema-version change yields a NEW identity (new directory).
    assert eo.stage7_run_id("2026-07-30", "cfgh", "evfp",
                            schema_version="9.9.9") != base
    # An engine-version change also yields a NEW identity.
    assert eo.stage7_run_id("2026-07-30", "cfgh", "evfp",
                            engine_version="9.9.9") != base
    # The default identity binds the CURRENT schema + engine version.
    assert base == eo.stage7_run_id(
        "2026-07-30", "cfgh", "evfp",
        schema_version=eo.STAGE7_SCHEMA_VERSION,
        engine_version=eo.STAGE7_ENGINE_VERSION)


def test_run_identity_embeds_schema_engine_and_evidence(tmp_path):
    cfg = _inv_cfg(tmp_path)
    ident = ro._run_identity(cfg, "2026-07-30", {"terminal": eo.READY})
    assert ident["schema_version"] == eo.STAGE7_SCHEMA_VERSION
    assert ident["engine_version"] == eo.STAGE7_ENGINE_VERSION
    assert ident["config_hash"] == eo.config_hash(cfg)
    assert ident["as_of"] == "2026-07-30"
    assert "upstream_fingerprint" in ident


def test_rerun_identical_inputs_is_idempotent_and_prior_untouched(tmp_path, built):
    cfg = _inv_cfg(tmp_path)
    cfg["recovery_root"] = str(tmp_path / "stage7")
    kwargs = _awrite_kwargs(cfg, built, run_id="stage7_idem")
    first = ro.assemble_and_write(cfg, **kwargs)
    run_dir = Path(first["run_dir"])
    assert (run_dir / "run_manifest.json").exists()
    before = _hash_tree(run_dir)
    # Re-run with identical identity inputs: idempotent no-op, bytes untouched.
    second = ro.assemble_and_write(cfg, **kwargs)
    assert second.get("idempotent") is True
    assert second["files_written"] == []
    assert _hash_tree(run_dir) == before          # prior directory NOT modified


def test_non_identical_content_cannot_overwrite_existing_run(tmp_path, built):
    cfg = _inv_cfg(tmp_path)
    cfg["recovery_root"] = str(tmp_path / "stage7")
    run_id = "stage7_guard"
    run_dir = Path(cfg["recovery_root"]) / "runs" / run_id
    run_dir.mkdir(parents=True)
    # A legacy package with a DIFFERENT (here: absent) identity already exists.
    (run_dir / "run_manifest.json").write_text(
        json.dumps({"run_id": run_id, "schema_version": "0.0.1"}),
        encoding="utf-8")
    (run_dir / "recovery_disposition.json").write_text(
        json.dumps({"disposition": "LEGACY"}), encoding="utf-8")
    before = _hash_tree(run_dir)
    with pytest.raises(ro.ImmutableRunError):
        ro.assemble_and_write(cfg, **_awrite_kwargs(cfg, built, run_id=run_id))
    # The prior immutable directory is left exactly as it was.
    assert _hash_tree(run_dir) == before
