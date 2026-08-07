"""tests/test_slice3_2_monthly_emitter_bridge.py — Phase 29D.2 production monthly-
momentum emitter bridge.

Deterministic, OFFLINE tests for the production monthly-momentum emitter bridge
(``api.monthly_momentum_emitter``) and its integration with the canonical adapter
(``api.monthly_momentum_input``) and the Daily Research Cycle
(``api.daily_research_cycle``).

Every provider / subprocess boundary is an INJECTED seam and every path is a
per-test ``tmp_path`` fixture root: NO real subprocess runs, NO live provider /
Norgate / prediction call occurs, NO Daily Close runs, and NO real research
artifact, operational ledger, database row, order / signal / decision / fill,
holding, cash or NAV is touched. The real August-5 research artifacts are never
read or written — the emitter runs against a fake repo/python/panel under tmp_path.
"""
from __future__ import annotations

import copy
import csv
import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from paper_trader.api import monthly_momentum_emitter as mme
from paper_trader.api import monthly_momentum_input as mmi
from paper_trader.api import daily_research_cycle as drc
from paper_trader.api import data_freshness as df
from paper_trader.api import multi_horizon_engine as eng

ROOT = Path(__file__).resolve().parent.parent
UI = ROOT / "api" / "ui" / "index.html"

D = "2026-08-05"
D1 = "2026-08-04"
NOW_AFTER_CUTOFF_D = datetime(2026, 8, 5, 21, 45, tzinfo=timezone.utc)  # 17:45 EDT

MOM_COLS = ["ticker", "mom_6_1", "is_member", "adv_dollar", "realized_vol_63d",
            "trailing_obs_126", "eligible_history", "extreme_flag", "sector",
            "market_as_of_date", "month_label"]


# --------------------------------------------------------------------------- #
# Fixture environment + injected subprocess runner (never a real subprocess).
# --------------------------------------------------------------------------- #
def _fixture_cfg(tmp_path, *, panel_last=D, timeout=60, missing=None, work=None):
    """A fake AVAILABLE emitter environment under tmp_path (unless a piece is omitted)."""
    missing = set(missing or ())
    repo = tmp_path / "repo"
    (repo / "research").mkdir(parents=True, exist_ok=True)
    if "phase24" not in missing:
        (repo / "research" / "phase24_daily_panel.py").write_text("# fake", encoding="utf-8")
    if "phase25" not in missing:
        (repo / "research" / "phase25_multi_horizon_inputs.py").write_text("# fake", encoding="utf-8")
    python = tmp_path / "python.exe"
    if "python" not in missing:
        python.write_text("", encoding="utf-8")
    data = tmp_path / "data"
    data.mkdir(exist_ok=True)
    panel = data / "russell1000_cp_daily.npz"
    if "panel" not in missing:
        panel.write_bytes(b"NPZFAKEBYTES")
    manifest = data / "manifest.json"
    if "manifest" not in missing:
        manifest.write_text(json.dumps({"first_date": "2000-01-03",
                                        "last_date": panel_last}), encoding="utf-8")
    return mme.resolve_config({
        "repo": (str(tmp_path / "no_repo") if "repo" in missing else str(repo)),
        "python": str(python), "panel_npz": str(panel),
        "panel_manifest": str(manifest),
        "work_dir": str(work or (tmp_path / "work")), "timeout_seconds": timeout})


def _make_runner(*, calls=None, month="2026-08", asof=D, rows=None, cols=None,
                 returncode=0, timed_out=False, manifest_extra=None,
                 npz_fp="fp16"):
    calls = calls if calls is not None else {"n": 0}

    def runner(cmd, *, timeout, cwd=None):
        calls["n"] += 1
        calls["last_cmd"] = list(cmd)
        out_dir = Path(cmd[5])  # [python, "-c", DRIVER, repo, npz, out_dir]
        if timed_out:
            return {"argv": list(cmd), "returncode": None, "stdout": "partial-run",
                    "stderr": "", "duration_seconds": float(timeout),
                    "timed_out": True}
        if returncode != 0:
            return {"argv": list(cmd), "returncode": returncode, "stdout": "",
                    "stderr": "Traceback: phase25 boom\n", "duration_seconds": 0.4,
                    "timed_out": False}
        out_dir.mkdir(parents=True, exist_ok=True)
        _cols = cols if cols is not None else MOM_COLS
        _rows = rows if rows is not None else [
            {"ticker": tk, "mom_6_1": "0.12", "is_member": "1", "adv_dollar": "1e8",
             "realized_vol_63d": "0.2", "trailing_obs_126": "126",
             "eligible_history": "1", "extreme_flag": "0", "sector": "Tech",
             "market_as_of_date": asof, "month_label": month}
            for tk in ("AAA", "BBB", "CCC")]
        with open(out_dir / "current_momentum_scores.csv", "w", encoding="utf-8",
                  newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=_cols, extrasaction="ignore")
            w.writeheader()
            for r in _rows:
                w.writerow(r)
        man = {"market_as_of_date": asof, "current_month_label": month,
               "source_npz_fingerprint": npz_fp,
               "counts": {"current_names": len(_rows)},
               "outputs": ["current_momentum_scores.csv", "inputs_manifest.json"]}
        if manifest_extra:
            man.update(manifest_extra)
        (out_dir / "inputs_manifest.json").write_text(json.dumps(man), encoding="utf-8")
        stdout = (mme._RESULT_MARKER + json.dumps(
            {"market_as_of_date": asof, "current_month_label": month,
             "source_npz_fingerprint": npz_fp, "counts": man["counts"],
             "out_dir": str(out_dir)}) + "\n")
        return {"argv": list(cmd), "returncode": 0, "stdout": stdout, "stderr": "",
                "duration_seconds": 1.0, "timed_out": False}

    return runner


def _seed_mom(inputs_dir, month, asof):
    p = Path(inputs_dir)
    p.mkdir(parents=True, exist_ok=True)
    with open(p / "current_momentum_scores.csv", "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(mmi.REQUIRED_COLUMNS))
        w.writeheader()
        for tk in ("AAA", "BBB", "CCC"):
            w.writerow({"ticker": tk, "mom_6_1": "0.01", "is_member": "1",
                        "sector": "Tech", "market_as_of_date": asof, "month_label": month})
    return p / "current_momentum_scores.csv"


# =========================================================================== #
# CONFIG / AVAILABILITY (1–7)
# =========================================================================== #
def test_01_config_resolution_defaults_and_overrides(monkeypatch):
    monkeypatch.delenv(mme.REPO_ENV, raising=False)
    c = mme.resolve_config()
    assert c.repo == mme.DEFAULT_REPO and c.python == mme.DEFAULT_PYTHON
    assert c.phase25_module.endswith("phase25_multi_horizon_inputs.py")
    c2 = mme.resolve_config({"repo": r"X:\repo", "timeout_seconds": 42})
    assert c2.repo == r"X:\repo" and c2.timeout_seconds == 42


def test_02_python_resolution_from_env(monkeypatch):
    monkeypatch.setenv(mme.PYTHON_ENV, r"X:\py\python.exe")
    assert mme.resolve_config().python == r"X:\py\python.exe"


def test_03_missing_repository_unavailable(tmp_path):
    cfg = _fixture_cfg(tmp_path, missing={"repo"})
    a = mme.check_availability(cfg)
    assert a["available"] is False and "REPO_MISSING" in a["reasons"]
    assert mme.resolve_production_emitter(cfg) is None


def test_04_missing_python_unavailable(tmp_path):
    cfg = _fixture_cfg(tmp_path, missing={"python"})
    a = mme.check_availability(cfg)
    assert a["available"] is False and "PYTHON_MISSING" in a["reasons"]


def test_05_missing_phase24_module_unavailable(tmp_path):
    cfg = _fixture_cfg(tmp_path, missing={"phase24"})
    assert "PHASE24_MODULE_MISSING" in mme.check_availability(cfg)["reasons"]


def test_06_missing_phase25_module_unavailable(tmp_path):
    cfg = _fixture_cfg(tmp_path, missing={"phase25"})
    assert "PHASE25_MODULE_MISSING" in mme.check_availability(cfg)["reasons"]


def test_07_safe_argument_array_no_shell(tmp_path):
    cfg = _fixture_cfg(tmp_path)
    cmd = mme.build_run_command(cfg, out_dir=str(tmp_path / "out"))
    assert isinstance(cmd, list) and cmd[0] == cfg.python and cmd[1] == "-c"
    assert cmd[3] == cfg.repo and cmd[4] == cfg.panel_npz
    assert not any("&&" in str(x) or "|" in str(x) for x in cmd)  # not a shell string


# =========================================================================== #
# SUBPROCESS BEHAVIOUR (8–11)
# =========================================================================== #
def test_08_timeout_is_a_recoverable_hold(tmp_path):
    cfg = _fixture_cfg(tmp_path)
    with pytest.raises(mme.MonthlyEmitterHold) as ei:
        mme.production_emitter(month="2026-08", eligible=D, config=cfg,
                               runner=_make_runner(timed_out=True))
    assert ei.value.emitter_status == "MONTHLY_EMITTER_TIMEOUT"
    assert ei.value.retry_classification == "TRANSIENT"
    assert getattr(ei.value, "monthly_data_hold", False) is True


def test_09_nonzero_exit_is_a_hard_error(tmp_path):
    cfg = _fixture_cfg(tmp_path)
    with pytest.raises(mme.MonthlyEmitterError) as ei:
        mme.production_emitter(month="2026-08", eligible=D, config=cfg,
                               runner=_make_runner(returncode=3))
    assert ei.value.emitter_status == "MONTHLY_EMITTER_SUBPROCESS_FAILED"
    assert getattr(ei.value, "monthly_data_hold", False) is False


def test_10_stdout_stderr_captured_in_diagnostic(tmp_path):
    cfg = _fixture_cfg(tmp_path)
    with pytest.raises(mme.MonthlyEmitterError):
        mme.production_emitter(month="2026-08", eligible=D, config=cfg,
                               runner=_make_runner(returncode=3))
    diags = list((Path(cfg.work_dir) / "_diagnostics").glob("*.json"))
    assert diags, "a diagnostic must be retained on failure"
    rec = json.loads(diags[0].read_text(encoding="utf-8"))
    assert "boom" in rec["stderr_tail"] and rec["returncode"] == 3


def test_11_current_panel_skips_provider(tmp_path):
    cfg = _fixture_cfg(tmp_path, panel_last=D)
    panel = mme.inspect_source_panel(cfg, eligible=D)
    assert panel["action"] == "USE_EXISTING" and panel["covered"] is True
    assert panel["refresh_required"] is False and panel["refresh_supported"] is False


# =========================================================================== #
# SOURCE-PANEL POLICY (12–13)
# =========================================================================== #
def test_12_stale_panel_requires_refresh_but_is_unsupported(tmp_path):
    cfg = _fixture_cfg(tmp_path, panel_last="2026-07-31")
    panel = mme.inspect_source_panel(cfg, eligible=D)
    assert panel["action"] == "BLOCK" and panel["status"] == "MONTHLY_PANEL_BEHIND_ELIGIBLE"
    assert panel["refresh_required"] is True and panel["incremental_supported"] is False


def test_13_unsupported_incremental_blocks_emission(tmp_path):
    cfg = _fixture_cfg(tmp_path, panel_last="2026-07-31")
    calls = {"n": 0}
    with pytest.raises(mme.MonthlyEmitterHold) as ei:
        mme.production_emitter(month="2026-08", eligible=D, config=cfg,
                               runner=_make_runner(calls=calls))
    assert ei.value.emitter_status == "MONTHLY_PANEL_BEHIND_ELIGIBLE"
    assert calls["n"] == 0  # never ran Phase 25 / a full rebuild


def test_13b_future_panel_blocks(tmp_path):
    cfg = _fixture_cfg(tmp_path, panel_last="2026-08-31")
    panel = mme.inspect_source_panel(cfg, eligible=D)
    assert panel["status"] == "MONTHLY_PANEL_FUTURE_DATED" and panel["action"] == "BLOCK"


def test_13c_unverifiable_panel_blocks(tmp_path):
    cfg = _fixture_cfg(tmp_path, missing={"manifest"})
    # availability already fails (manifest missing) -> resolver None; and inspect blocks.
    panel = mme.inspect_source_panel(cfg, eligible=D)
    assert panel["action"] == "BLOCK" and "UNVERIFIABLE" in panel["status"]


# =========================================================================== #
# EMISSION + OUTPUT VALIDATION (14–22)
# =========================================================================== #
def test_14_successful_emission_returns_validated_artifact(tmp_path):
    cfg = _fixture_cfg(tmp_path)
    calls = {"n": 0}
    art = mme.production_emitter(month="2026-08", eligible=D, config=cfg,
                                runner=_make_runner(calls=calls))
    assert calls["n"] == 1
    assert art["month_label"] == "2026-08" and art["market_as_of_date"] == D
    assert art["source"] == mme.EMITTER_SOURCE and art["approximated"] is False
    assert len(art["rows"]) == 3 and art["validated"] is True
    assert art["content_hash"] and art["panel_fingerprint"]


def test_15_isolated_temp_deleted_on_success_manifest_written(tmp_path):
    cfg = _fixture_cfg(tmp_path)
    mme.production_emitter(month="2026-08", eligible=D, config=cfg,
                           runner=_make_runner())
    tmp_root = Path(cfg.work_dir) / "_tmp"
    assert not list(tmp_root.glob("mme_*")), "temp dir must be deleted on success"
    ems = list((Path(cfg.work_dir) / "_emissions").glob("*.json"))
    assert ems and json.loads(ems[0].read_text(encoding="utf-8"))["month_label"] == "2026-08"


def test_16_wrong_month_rejected(tmp_path):
    cfg = _fixture_cfg(tmp_path)
    with pytest.raises(mme.MonthlyEmitterError) as ei:
        mme.production_emitter(month="2026-08", eligible=D, config=cfg,
                               runner=_make_runner(month="2026-07"))
    assert "PERIOD_MISMATCH" in str(ei.value)


def test_17_wrong_date_rejected(tmp_path):
    cfg = _fixture_cfg(tmp_path)
    with pytest.raises(mme.MonthlyEmitterError) as ei:
        mme.production_emitter(month="2026-08", eligible=D, config=cfg,
                               runner=_make_runner(asof="2026-08-04"))
    assert "PROVENANCE_MISMATCH" in str(ei.value)


def test_18_future_data_rejected(tmp_path):
    cfg = _fixture_cfg(tmp_path)
    with pytest.raises(mme.MonthlyEmitterError) as ei:
        mme.production_emitter(month="2026-08", eligible=D, config=cfg,
                               runner=_make_runner(asof="2026-08-20", month="2026-08"))
    assert "FUTURE_PROVENANCE" in str(ei.value)


def test_19_duplicate_ticker_rejected(tmp_path):
    cfg = _fixture_cfg(tmp_path)
    dup_rows = [{"ticker": "AAA", "mom_6_1": "0.1", "is_member": "1", "adv_dollar": "1",
                 "realized_vol_63d": "0.2", "trailing_obs_126": "126",
                 "eligible_history": "1", "extreme_flag": "0", "sector": "Tech",
                 "market_as_of_date": D, "month_label": "2026-08"}] * 2
    with pytest.raises(mme.MonthlyEmitterError) as ei:
        mme.production_emitter(month="2026-08", eligible=D, config=cfg,
                               runner=_make_runner(rows=dup_rows))
    assert "DUPLICATE_TICKERS" in str(ei.value)


def test_20_schema_rejected(tmp_path):
    cfg = _fixture_cfg(tmp_path)
    cols = [c for c in MOM_COLS if c != "sector"]  # drop a required column
    with pytest.raises(mme.MonthlyEmitterError) as ei:
        mme.production_emitter(month="2026-08", eligible=D, config=cfg,
                               runner=_make_runner(cols=cols))
    assert "SCHEMA_MISSING_COLUMNS" in str(ei.value)


def test_21_null_required_score_rejected(tmp_path):
    cfg = _fixture_cfg(tmp_path)
    rows = [{"ticker": "AAA", "mom_6_1": "", "is_member": "1", "adv_dollar": "1",
             "realized_vol_63d": "0.2", "trailing_obs_126": "126",
             "eligible_history": "1", "extreme_flag": "0", "sector": "Tech",
             "market_as_of_date": D, "month_label": "2026-08"}]
    with pytest.raises(mme.MonthlyEmitterError) as ei:
        mme.production_emitter(month="2026-08", eligible=D, config=cfg,
                               runner=_make_runner(rows=rows))
    assert "NULL_REQUIRED_SCORE" in str(ei.value)


def test_22_provenance_and_source_panel_fingerprint_recorded(tmp_path):
    cfg = _fixture_cfg(tmp_path)
    art = mme.production_emitter(month="2026-08", eligible=D, config=cfg,
                                runner=_make_runner())
    prov = art["provenance"]
    assert prov["math_owner"] == mme.EMITTER_SOURCE
    assert prov["panel_owner"] == mme.PANEL_SOURCE
    assert art["source_panel"]["npz_fingerprint"] is not None


def test_22b_missing_output_file_rejected(tmp_path):
    cfg = _fixture_cfg(tmp_path)

    def empty_runner(cmd, *, timeout, cwd=None):
        return {"argv": list(cmd), "returncode": 0, "stdout": mme._RESULT_MARKER + "{}",
                "stderr": "", "duration_seconds": 1.0, "timed_out": False}
    with pytest.raises(mme.MonthlyEmitterError) as ei:
        mme.production_emitter(month="2026-08", eligible=D, config=cfg, runner=empty_runner)
    assert ei.value.emitter_status == "MONTHLY_OUTPUT_MISSING"


# =========================================================================== #
# ADAPTER INTEGRATION (23–28) — atomic promotion / reuse / conflict / cache.
# =========================================================================== #
def _bound(cfg, runner):
    def _emit(*, month, eligible, inputs_dir):
        return mme.production_emitter(month=month, eligible=eligible,
                                      inputs_dir=inputs_dir, config=cfg, runner=runner)
    return _emit


def test_23_atomic_promotion_through_adapter(tmp_path):
    inputs = tmp_path / "inputs"
    _seed_mom(inputs, "2026-07", "2026-07-31")
    cfg = _fixture_cfg(tmp_path)
    r = mmi.emit_if_due(eligible=D, inputs_dir=str(inputs),
                        emitter_fn=_bound(cfg, _make_runner()))
    assert r["status"] == mmi.S_EMITTED and r["performed_write"] is True
    assert r["promotion"]["old_artifact_hash"] and r["promotion"]["new_artifact_hash"]
    assert r["promotion"]["cache_cleared"] is True
    rows, _ = mmi._read_csv_rows(inputs / "current_momentum_scores.csv")
    assert rows and rows[0]["month_label"] == "2026-08"
    assert not list(inputs.glob("*.tmp"))  # atomic replace leaves no temp


def test_24_rollback_leaves_prior_input_on_failure(tmp_path):
    inputs = tmp_path / "inputs"
    _seed_mom(inputs, "2026-07", "2026-07-31")
    cfg = _fixture_cfg(tmp_path)
    r = mmi.emit_if_due(eligible=D, inputs_dir=str(inputs),
                        emitter_fn=_bound(cfg, _make_runner(month="2026-07")))
    assert r["status"] == mmi.S_INVALID and r["performed_write"] is False
    rows, _ = mmi._read_csv_rows(inputs / "current_momentum_scores.csv")
    assert rows[0]["month_label"] == "2026-07"  # unchanged prior input


def test_25_identical_artifact_reused(tmp_path):
    inputs = tmp_path / "inputs"
    _seed_mom(inputs, "2026-07", "2026-07-31")
    cfg = _fixture_cfg(tmp_path)
    calls = {"n": 0}
    r1 = mmi.emit_if_due(eligible=D, inputs_dir=str(inputs),
                         emitter_fn=_bound(cfg, _make_runner(calls=calls)))
    r2 = mmi.emit_if_due(eligible=D, inputs_dir=str(inputs),
                         emitter_fn=_bound(cfg, _make_runner(calls=calls)))
    assert r1["status"] == mmi.S_EMITTED and r2["status"] == mmi.S_CURRENT
    assert r2["performed_write"] is False and calls["n"] == 1  # emitter not re-run


def test_26_conflicting_artifact_rejected(tmp_path):
    inputs = tmp_path / "inputs"
    _seed_mom(inputs, "2026-09", "2026-09-01")  # persisted month AHEAD of eligible
    cfg = _fixture_cfg(tmp_path)
    r = mmi.emit_if_due(eligible=D, inputs_dir=str(inputs),
                        emitter_fn=_bound(cfg, _make_runner()))
    assert r["status"] == mmi.S_CONFLICT and r["performed_write"] is False


def test_27_cache_cleared_after_success(tmp_path, monkeypatch):
    inputs = tmp_path / "inputs"
    _seed_mom(inputs, "2026-07", "2026-07-31")
    cfg = _fixture_cfg(tmp_path)
    n = {"clear": 0}
    monkeypatch.setattr(eng, "clear_cache", lambda: n.__setitem__("clear", n["clear"] + 1))
    r = mmi.emit_if_due(eligible=D, inputs_dir=str(inputs),
                        emitter_fn=_bound(cfg, _make_runner()))
    assert r["status"] == mmi.S_EMITTED and n["clear"] == 1


def test_28_no_cache_clear_after_failure(tmp_path, monkeypatch):
    inputs = tmp_path / "inputs"
    _seed_mom(inputs, "2026-07", "2026-07-31")
    cfg = _fixture_cfg(tmp_path)
    n = {"clear": 0}
    monkeypatch.setattr(eng, "clear_cache", lambda: n.__setitem__("clear", n["clear"] + 1))
    r = mmi.emit_if_due(eligible=D, inputs_dir=str(inputs),
                        emitter_fn=_bound(cfg, _make_runner(returncode=1)))
    assert r["status"] == mmi.S_INVALID and n["clear"] == 0


def test_28b_panel_behind_maps_to_blocked_data_hold(tmp_path):
    inputs = tmp_path / "inputs"
    _seed_mom(inputs, "2026-07", "2026-07-31")
    cfg = _fixture_cfg(tmp_path, panel_last="2026-07-31")
    r = mmi.emit_if_due(eligible=D, inputs_dir=str(inputs),
                        emitter_fn=_bound(cfg, _make_runner()))
    assert r["status"] == mmi.S_UNAVAILABLE  # honest BLOCKED, not FAILED
    assert mmi.classify_result(r) == "BLOCKED"


# =========================================================================== #
# RESOLVER + WIRING (29–33)
# =========================================================================== #
def test_29_resolver_none_when_unavailable_callable_when_available(tmp_path):
    assert mme.resolve_production_emitter(_fixture_cfg(tmp_path, missing={"panel"})) is None
    fn = mme.resolve_production_emitter(_fixture_cfg(tmp_path))
    assert callable(fn)


def test_30_adapter_activation_wires_resolver(tmp_path):
    cfg = _fixture_cfg(tmp_path)
    try:
        mmi.activate_production_emitter(lambda: mme.resolve_production_emitter(cfg))
        assert mmi.emitter_is_wired() is True
    finally:
        mmi.activate_production_emitter(None)
    assert mmi.emitter_is_wired() is False  # cleared -> hermetic default


def test_31_idempotent_rerun_resume_safe(tmp_path):
    inputs = tmp_path / "inputs"
    _seed_mom(inputs, "2026-07", "2026-07-31")
    cfg = _fixture_cfg(tmp_path)
    calls = {"n": 0}
    mmi.emit_if_due(eligible=D, inputs_dir=str(inputs),
                    emitter_fn=_bound(cfg, _make_runner(calls=calls)))
    # Simulate a crash-then-resume: the input already advanced, so no re-emission.
    r = mmi.emit_if_due(eligible=D, inputs_dir=str(inputs),
                        emitter_fn=_bound(cfg, _make_runner(calls=calls)))
    assert r["status"] == mmi.S_CURRENT and calls["n"] == 1


def test_32_disabled_env_makes_unavailable(tmp_path, monkeypatch):
    monkeypatch.setenv(mme.DISABLE_ENV, "1")
    cfg = _fixture_cfg(tmp_path)
    assert mme.check_availability(cfg)["available"] is False
    assert mme.resolve_production_emitter(cfg) is None


def test_33_concurrent_identical_drc_request_reports_in_progress(tmp_path):
    s = _drc_status(tmp_path, inputs=_inputs(price=D, month="2026-07"))
    drc._atomic_write_json(drc._lock_path(str(tmp_path)), {
        "idempotency_key": s["idempotency_key"], "eligible_date": s["eligible_market_date"],
        "run_id": "drc_other", "input_contract_hash": s["input_contract_hash"],
        "started_at": datetime.now(tz=timezone.utc).isoformat(), "pid": 999})
    r, _ = _run_drc(tmp_path, inputs=_inputs(price=D, month="2026-07"))
    assert r["state"] == drc.RUN_IN_PROGRESS


# =========================================================================== #
# DAILY RESEARCH CYCLE INTEGRATION (34–39) — Workstream K live regression.
# =========================================================================== #
def _op(desk=D, nav=D, target=D):
    return {"operational_book": {"book_id": "alpha_paper_book_1",
            "book_label": "Alpha Paper Book #1", "current_status": "FORWARD_TRACKING_ACTIVE",
            "initialized": True, "nav_as_of_date": nav, "desk_mark_date": desk,
            "latest_desk_mark_date": desk, "nav": 102000.0, "cash": 1500.0,
            "holdings_count": 25, "pending_order_count": 0,
            "current_target": {"alpha_market_date": target, "latest_completed_market_date": desk}}}


def _desk(spy=((D1, 771.3), (D, 773.0))):
    return {"series": {"SPY": [list(x) for x in spy]}, "latest_completed_date": spy[-1][0]}


def _inputs(price=D, month="2026-07", fundamental="2026-05-22"):
    return {"market_as_of_date": price, "momentum_month": month,
            "fundamental_as_of_date": fundamental}


_CLOSE = {"market_date": D1, "done": True,
          "final_close_status": "DAILY_CLOSE_COMPLETE_HOLD", "status": "CLOSE_FINISHED"}
_FWD = {"latest_snapshot_date": D1, "snapshot_count": 6, "evidence_state": "X"}
_DAILY = {"status": "DAILY_STATUS_READY", "latest_valid_mark_date": D}
_REG = [("m", "b", 1, "ACTIVE")]


def _drc_status(tmp, *, inputs=None):
    return drc.load_daily_research_cycle_status(
        drc_dir=str(tmp), now=NOW_AFTER_CUTOFF_D, operational=_op(),
        inputs=(inputs if inputs is not None else _inputs()), daily_status=dict(_DAILY),
        desk_marks=_desk(), close_progress=dict(_CLOSE), forward_status=copy.deepcopy(_FWD))


def _fakes():
    def score():
        return {"status": "MHZ_READY", "market_as_of_date": D, "momentum_month": "2026-08",
                "fundamental_as_of_date": "2026-05-22",
                "scores": {"composite_sn": {t: {"eligible": True} for t in ("A", "B")},
                           "counts": {}}, "combined": {"n_common": 2},
                "books": {"primary_book_id": "fundamental_momentum_50_50_top25", "books": {
                    "fundamental_momentum_50_50_top25": {"constituents": [
                        {"ticker": "A", "rank": 1, "weight": 0.5, "sector": "Tech"},
                        {"ticker": "B", "rank": 2, "weight": 0.5, "sector": "Fin"}]},
                    "fundamental_momentum_50_50_top50": {"constituents": []}}}}

    def target():
        return {"state": "READY_TO_CONFIRM", "dates": {"alpha_market_date": D},
                "required_next_action": "PREVIEW_CONFIRM"}

    def capture(*, market_date, current, ops, downloader):
        return {"snapshots_expected": 1, "snapshots_created": 1,
                "snapshots_already_present": 0, "mandatory_active_snapshot_persisted": True,
                "evidence_status": "FORWARD_EVIDENCE_COMPLETE",
                "artifact_bundle_id": "fca_%s" % market_date, "artifact_hash": "h",
                "performed_write": True}

    def refresh(*, confirm, downloader, completed_through):
        return {"status": "ALPHA_TARGET_ALREADY_FRESH", "performed_write": False}

    def assess(*, today):
        return {"latest_completed_market_date": today, "outcome": "NO_ACTION_TODAY",
                "target_state": "CURRENT_ALIGNED", "headline": "No change."}
    return score, target, capture, refresh, assess


def _hoc_stub(*, scoring=None, hoc_dir=None):   # Slice 6: hermetic stub (no I/O)
    return {"assessment": {"assessment_state": "READY", "assessment_hash": "hoc_stub",
                           "eligible_market_date": D, "holding_reviews": [],
                           "recommendation_counts": {"HOLD": 0, "REDUCE": 0, "EXIT": 0,
                                                     "REPLACE": 0, "ADD": 0},
                           "data_quality": {"data_gaps": []}},
            "persistence": {"status": "CREATED", "artifact_id": "hoc_stub", "persisted": True}}


def _realloc_stub(*, scoring=None, hoc_assessment=None, reallocation_dir=None, hoc_dir=None):
    return {"proposal": {"proposal_state": "READY", "proposal_hash": "realloc_stub",
                         "eligible_market_date": D, "action_counts": {}, "data_gaps": [],
                         "portfolio": {}, "signal": {}, "turnover": {}},
            "persistence": {"status": "CREATED", "proposal_id": "realloc_stub",
                            "persisted": True, "superseded_proposal_id": None}}


def _ra_stub(*, scoring=None, reallocation=None, research_agent_dir=None, hoc_dir=None,
             reallocation_dir=None, desk_dir=None):
    # Slice 8: hermetic stub of the Persistent Alpha Research Agent seam (no I/O).
    return {"assessment": {"research_state": "INSUFFICIENT_EVIDENCE",
                           "assessment_hash": "ra_stub", "eligible_market_date": D,
                           "evidence_quality": {"state": "INSUFFICIENT"},
                           "degradation": {"categories": []},
                           "recalibration": {"state": "INSUFFICIENT_EVIDENCE",
                                             "recommended": False},
                           "challenger": {"state": "NOT_EVALUATED"},
                           "research_opportunities": [], "data_gaps": []},
            "persistence": {"status": "CREATED", "assessment_id": "ra_stub",
                            "persisted": True, "superseded_assessment_id": None}}


def _run_drc(tmp, *, inputs=None, monthly_emitter_fn=None):
    score, target, capture, refresh, assess = _fakes()
    return drc.run_daily_research_cycle(
        confirm=drc.EXECUTE_CONFIRMATION, drc_dir=str(tmp), now=NOW_AFTER_CUTOFF_D,
        holding_opp_cost_fn=_hoc_stub, reallocation_proposal_fn=_realloc_stub,
        research_agent_fn=_ra_stub,
        operational=_op(), inputs=(inputs if inputs is not None else _inputs()),
        daily_status=dict(_DAILY), desk_marks=_desk(), close_progress=dict(_CLOSE),
        forward_status=copy.deepcopy(_FWD), daily_refresh_fn=refresh, scoring_fn=score,
        target_loader=target, evidence_capture_fn=capture, evidence_registry=list(_REG),
        assessment_loader=assess, refresh_confirm_token="CONFIRM_ALPHA_TARGET_REFRESH",
        monthly_emitter_fn=monthly_emitter_fn), None


def test_34_workstream_k_due_month_emitted_in_one_cycle(tmp_path):
    inputs_dir = tmp_path / "inputs"
    _seed_mom(inputs_dir, "2026-07", "2026-07-31")   # canonical input still 2026-07
    cfg = _fixture_cfg(tmp_path, panel_last=D)         # owned panel current to eligible
    calls = {"n": 0}

    def monthly_fn(*, eligible):
        return mmi.emit_if_due(eligible=eligible, inputs_dir=str(inputs_dir),
                               emitter_fn=_bound(cfg, _make_runner(calls=calls)))

    r, _ = _run_drc(tmp_path, inputs=_inputs(price=D, month="2026-07"),
                    monthly_emitter_fn=monthly_fn)
    assert r["state"] == drc.COMPLETE
    assert r["completed_steps"] == list(drc.STEP_SEQUENCE)
    assert calls["n"] == 1  # the emitter ran exactly once
    # The canonical monthly input advanced to the eligible month; cache was cleared.
    rows, _f = mmi._read_csv_rows(inputs_dir / "current_momentum_scores.csv")
    assert rows[0]["month_label"] == "2026-08" and rows[0]["market_as_of_date"] == D


def test_35_cycle_blocked_when_emitter_holds(tmp_path):
    inputs_dir = tmp_path / "inputs"
    _seed_mom(inputs_dir, "2026-07", "2026-07-31")
    cfg = _fixture_cfg(tmp_path, panel_last="2026-07-31")  # panel behind -> DATA_HOLD

    def monthly_fn(*, eligible):
        return mmi.emit_if_due(eligible=eligible, inputs_dir=str(inputs_dir),
                               emitter_fn=_bound(cfg, _make_runner()))
    r, _ = _run_drc(tmp_path, inputs=_inputs(price=D, month="2026-07"),
                    monthly_emitter_fn=monthly_fn)
    assert r["state"] == drc.BLOCKED and r["failed_step"] == drc.STEP_REFRESH_INPUTS
    assert any(b.get("missing_implementation") == drc.MONTHLY_EMITTER_ACTION
               for b in r["blockers"])


def test_36_status_exposes_monthly_owner_block(tmp_path):
    s = _drc_status(tmp_path, inputs=_inputs(price=D, month="2026-07"))
    mo = s["monthly_owner"]
    assert mo["owner"] == "api.monthly_momentum_input"
    assert mo["producer"] == "api.monthly_momentum_emitter"
    assert mo["current_month"] == "2026-07" and mo["required_month"] == "2026-08"
    assert mo["due"] is True and set(mo) >= {
        "refresh_required", "refresh_selected", "emitter_status", "last_error",
        "retry_classification", "source_panel_date"}


def test_37_status_monthly_owner_hermetic_when_unavailable(tmp_path):
    # Default (no activation, no injected availability): available=False, no real read.
    s = _drc_status(tmp_path, inputs=_inputs(price=D, month="2026-07"))
    assert s["monthly_owner"]["available"] is False
    assert s["monthly_owner"]["source_panel_date"] is None


def test_38_no_live_provider_prediction_or_close_in_emitter():
    src = (ROOT / "api" / "monthly_momentum_emitter.py").read_text(encoding="utf-8")
    for tok in ("requests.get(", "requests.post(", "httpx.", ":9000", "yfinance",
                "predict(", "prediction_client", "run_daily_close(", "place_order(",
                "submit_order(", "Signal(", "TradeDecision(", "shell=True",
                "import numpy", "import pandas"):
        assert tok not in src, tok


def test_39_no_operational_ledger_reference_in_emitter():
    src = (ROOT / "api" / "monthly_momentum_emitter.py").read_text(encoding="utf-8")
    assert ".paper_trader" not in src  # never the operational ledger root


# =========================================================================== #
# ARCHITECTURE / UI / SCOPE (40–44)
# =========================================================================== #
def _audit():
    spec = importlib.util.spec_from_file_location(
        "audit_arch", ROOT / "scripts" / "audit_architecture.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_40_architecture_bridge_guard_and_zero_drift():
    mod = _audit()
    files = mod._iter_source_files()
    me = mod.check_monthly_emitter_bridge_ownership(files)
    assert me["owner_present"] and me["bridge_pure_stdlib"]
    assert me["bridge_no_shell_string"] and me["bridge_uses_argv_array"]
    assert me["bridge_delegates_phase25_math"]
    assert me["second_monthly_formula_modules"] == []
    assert me["adapter_wires_production_resolver"] and me["app_wires_production_resolver"]
    assert me["drc_status_exposes_monthly_owner"]
    assert me["no_separate_monthly_ui_button"] and me["separate_monthly_endpoints"] == []
    d = mod.check_inventory_drift(files)
    assert d["on_disk_not_in_inventory"] == [] and d["in_inventory_not_on_disk"] == []


def test_41_ui_shows_monthly_step_inside_drc_no_separate_button():
    html = UI.read_text(encoding="utf-8")
    assert 'id="drc-monthly"' in html and "d.monthly_owner" in html
    assert "runMonthlyInputEmitter" not in html and "month-boundary-btn" not in html
    # exactly ONE canonical daily research primary action.
    assert html.count("function runDailyResearchCycle") == 1


def test_42_no_separate_monthly_execution_endpoint():
    mod = _audit()
    routes = mod.check_routes()["routes"]
    monthly_routes = [r["path"] for r in routes if "monthly" in (r["path"] or "").lower()]
    assert monthly_routes == []  # the monthly step lives inside the DRC run endpoint
    src = (ROOT / "api" / "app.py").read_text(encoding="utf-8")
    assert "activate_production_emitter" in src  # app wires the resolver, adds no route


def test_43_slice5_6_7_8_landed():
    # Slice 5 (Phase 29F), Slice 6 (Phase 29G), Slice 7 (Reallocation Proposal, Phase 29H)
    # and Slice 8 (Persistent Alpha Research Agent, Phase 29I) have LANDED; the NEXT slice
    # (Slice 9, Paid-data integration) is NOT started.
    roadmap = (ROOT / "docs" / "CONSOLIDATION_ROADMAP.md").read_text(encoding="utf-8")
    s5 = roadmap.index("## Slice 5")
    s6 = roadmap.index("## Slice 6")
    s7 = roadmap.index("## Slice 7")
    s8 = roadmap.index("## Slice 8")
    s9 = roadmap.index("## Slice 9")
    s10 = roadmap.index("## Slice 10")
    assert "LANDED (Phase 29F)" in roadmap[s5:s6]
    assert "LANDED (Phase 29G)" in roadmap[s6:s7]
    assert "LANDED (Phase 29H)" in roadmap[s7:s8]
    assert "LANDED (Phase 29I)" in roadmap[s8:s9]
    assert "LANDED" not in roadmap[s9:s10]
    assert (ROOT / "engine" / "reallocation_proposal.py").exists()
    assert (ROOT / "engine" / "research_agent.py").exists()
    assert not (ROOT / "api" / "portfolio_proposal.py").exists()
    assert not (ROOT / "api" / "model_registry.py").exists()


def test_44_no_new_scheduler_or_cadence_enabled():
    src = (ROOT / "api" / "monthly_momentum_emitter.py").read_text(encoding="utf-8")
    assert "schtasks" not in src and "ScheduledTask" not in src
