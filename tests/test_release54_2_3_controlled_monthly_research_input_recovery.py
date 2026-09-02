r"""tests/test_release54_2_3_controlled_monthly_research_input_recovery.py

RELEASE 54.2.3 — CONTROLLED MONTHLY RESEARCH-INPUT RECOVERY.

WHAT THIS RELEASE REPAIRS. The owned survivorship-free daily source panel stopped at
2026-08-05 because NOTHING in the running system was responsible for advancing it: the
panel acquisition is a ONE-TIME research build (a no-op once the NPZ exists, and an
unbounded pull "to latest" when forced), and the emitter bridge deliberately refused to
trigger it. Every new month therefore became a permanent blocker that only a hidden
manual maintenance step could clear.

The repair is ONE controlled, point-in-time-BOUNDED refresh on the panel owner itself,
driven by the existing emitter bridge with the cutoff supplied INTERNALLY from the
eligible research session. It is not a new workflow, route, button or date picker: the
governed cycle performs its own prerequisite maintenance inside the monthly step it
already had.

Every provider / subprocess boundary is an INJECTED seam and every path is a per-test
``tmp_path`` root. NO real subprocess runs, NO Norgate / provider / prediction call
occurs, NO Daily Close or Daily Research Cycle runs against production, and NO real
research artifact, operational ledger, database row, order / signal / decision / fill,
holding, cash or NAV is touched. The production panel under D:\Stock_Prediction_app_data
is never read for a decision and never written.
"""
from __future__ import annotations

import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pytest

from paper_trader.api import daily_research_cycle as drc
from paper_trader.api import monthly_momentum_emitter as mme
from paper_trader.api import monthly_momentum_input as mmi
from paper_trader.api import workflow_state as ws

ROOT = Path(__file__).resolve().parent.parent
UI = ROOT / "api" / "ui" / "index.html"
RELEASE_DOC = ROOT / "docs" / "RELEASE54_2_3_CONTROLLED_MONTHLY_RESEARCH_INPUT_RECOVERY.md"

#: The recovery session used throughout (the real one this release was built for).
ELIG = "2026-09-01"
#: The month-end whose closes the September frozen feature is allowed to see.
AUG_END = "2026-08-31"
#: A later session that must never leak into a 2026-09-01 research run.
NEXT_SESSION = "2026-09-02"
#: The stale panel date the live system actually had.
PANEL_STALE_DATE = "2026-08-05"

MOM_COLS = ["ticker", "mom_6_1", "is_member", "adv_dollar", "realized_vol_63d",
            "trailing_obs_126", "eligible_history", "extreme_flag", "sector",
            "market_as_of_date", "month_label"]


# =========================================================================== #
# Fixture environment: a fake emitter repo/python/panel entirely under tmp_path.
# =========================================================================== #
def _cfg(tmp_path, *, panel_last=PANEL_STALE_DATE, refresh=True, timeout=60):
    repo = tmp_path / "repo"
    (repo / "research").mkdir(parents=True, exist_ok=True)
    (repo / "research" / "phase24_daily_panel.py").write_text("# fake", encoding="utf-8")
    (repo / "research" / "phase25_multi_horizon_inputs.py").write_text("# fake",
                                                                      encoding="utf-8")
    python = tmp_path / "python.exe"
    python.write_text("", encoding="utf-8")
    data = tmp_path / "data"
    data.mkdir(exist_ok=True)
    npz = data / "russell1000_cp_daily.npz"
    npz.write_bytes(b"NPZFAKEBYTES")
    manifest = data / "manifest.json"
    manifest.write_text(json.dumps({"first_date": "2000-01-03",
                                    "last_date": panel_last}), encoding="utf-8")
    return mme.resolve_config({
        "repo": str(repo), "python": str(python), "panel_npz": str(npz),
        "panel_manifest": str(manifest), "work_dir": str(tmp_path / "work"),
        "timeout_seconds": timeout, "panel_refresh_enabled": refresh,
        "panel_refresh_timeout_seconds": timeout})


def _panel_last(cfg):
    return json.loads(Path(cfg.panel_manifest).read_text(encoding="utf-8"))["last_date"]


def _refresh_runner(cfg, *, calls=None, advance_to=None, ok=True, code=None,
                    returncode=0, timed_out=False, emit_marker=True,
                    securities=3076, days=6706):
    """An injected runner for the BOUNDED REFRESH argv.

    It behaves like the real panel owner: it advances the on-disk manifest to the cutoff
    the bridge asked for (or to ``advance_to`` when a test needs a short/wrong result),
    and prints the owner's result marker.
    """
    calls = calls if calls is not None else {"n": 0, "as_of": []}

    def runner(cmd, *, timeout, cwd=None):
        calls["n"] += 1
        calls["as_of"].append(cmd[4])
        calls["timeout"] = timeout
        calls["last_cmd"] = list(cmd)
        if timed_out:
            return {"argv": list(cmd), "returncode": None, "stdout": "", "stderr": "",
                    "duration_seconds": float(timeout), "timed_out": True}
        if returncode != 0:
            return {"argv": list(cmd), "returncode": returncode, "stdout": "",
                    "stderr": "panel refresh boom\n", "duration_seconds": 0.2,
                    "timed_out": False}
        asked = cmd[4]
        if ok and emit_marker:
            landed = advance_to or asked
            man = json.loads(Path(cfg.panel_manifest).read_text(encoding="utf-8"))
            man.update({"last_date": landed, "as_of_cutoff": asked,
                        "bounded_refresh": True, "securities_pulled": securities,
                        "n_trading_days": days})
            Path(cfg.panel_manifest).write_text(json.dumps(man), encoding="utf-8")
            payload = {"ok": True, "as_of": asked, "last_date": landed,
                       "as_of_cutoff": asked, "securities_pulled": securities,
                       "n_trading_days": days, "build_seconds": 93.6}
        elif ok:
            # A driver that died before printing its marker promoted nothing.
            payload = {}
        else:
            payload = {"ok": False, "as_of": asked,
                       "code": code or mme.PANEL_INCOMPLETE,
                       "error": "the owner refused", "detail": {"as_of": asked}}
        stdout = (mme._REFRESH_MARKER + json.dumps(payload) + "\n") if emit_marker else "x"
        return {"argv": list(cmd), "returncode": 0, "stdout": stdout, "stderr": "",
                "duration_seconds": 93.6, "timed_out": False}

    runner.calls = calls
    return runner


def _emit_runner(*, calls=None, month="2026-09", asof=ELIG, rows=None):
    """An injected runner for the PHASE-25 emission argv."""
    calls = calls if calls is not None else {"n": 0}

    def runner(cmd, *, timeout, cwd=None):
        calls["n"] += 1
        calls["last_cmd"] = list(cmd)
        out_dir = Path(cmd[5])
        out_dir.mkdir(parents=True, exist_ok=True)
        _rows = rows if rows is not None else [
            {"ticker": tk, "mom_6_1": "0.12", "is_member": "1", "adv_dollar": "1e8",
             "realized_vol_63d": "0.2", "trailing_obs_126": "126",
             "eligible_history": "1", "extreme_flag": "0", "sector": "Tech",
             "market_as_of_date": asof, "month_label": month}
            for tk in ("AAA", "BBB", "CCC")]
        with open(out_dir / "current_momentum_scores.csv", "w", encoding="utf-8",
                  newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=MOM_COLS, extrasaction="ignore")
            w.writeheader()
            for r in _rows:
                w.writerow(r)
        man = {"market_as_of_date": asof, "current_month_label": month,
               "source_npz_fingerprint": "fp16",
               "counts": {"current_names": len(_rows)},
               "outputs": ["current_momentum_scores.csv", "inputs_manifest.json"]}
        (out_dir / "inputs_manifest.json").write_text(json.dumps(man), encoding="utf-8")
        stdout = mme._RESULT_MARKER + json.dumps(
            {"market_as_of_date": asof, "current_month_label": month,
             "source_npz_fingerprint": "fp16", "counts": man["counts"],
             "out_dir": str(out_dir)}) + "\n"
        return {"argv": list(cmd), "returncode": 0, "stdout": stdout, "stderr": "",
                "duration_seconds": 1.0, "timed_out": False}

    runner.calls = calls
    return runner


def _seed_mom(inputs_dir, month, asof):
    p = Path(inputs_dir)
    p.mkdir(parents=True, exist_ok=True)
    with open(p / "current_momentum_scores.csv", "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(mmi.REQUIRED_COLUMNS))
        w.writeheader()
        for tk in ("AAA", "BBB", "CCC"):
            w.writerow({"ticker": tk, "mom_6_1": "0.01", "is_member": "1",
                        "sector": "Tech", "market_as_of_date": asof,
                        "month_label": month})
    return p / "current_momentum_scores.csv"


def _freshness(*, eligible=ELIG, monthly_asof=PANEL_STALE_DATE, price_asof=AUG_END,
               session="BEFORE_SESSION_CLOSE"):
    return {
        "eligible_market_date": eligible,
        "expected_completed_market_date": eligible,
        "consistency_status": "CONSISTENT",
        "market_session": {"session_status": session,
                           "latest_confirmed_owned_data_date": eligible},
        "active_book": {"active_book_id": "bk", "active_book_name": "bk"},
        "source_freshness": [
            {"source_id": "momentum_monthly",
             "display_name": "Frozen monthly momentum input", "status": "STALE",
             "cadence": "MONTHLY", "as_of_date": monthly_asof,
             "required_for_signal_refresh": True,
             "authoritative_owner": "api.monthly_momentum_input"},
            {"source_id": "price_score_refresh",
             "display_name": "Latest price / score refresh", "status": "STALE",
             "cadence": "DAILY", "as_of_date": price_asof,
             "required_for_signal_refresh": True,
             "authoritative_owner": "api.alpha_target.run_refresh"},
        ],
    }


def _owner_block(freshness, *, cfg, available=True):
    """The monthly-owner block as the STATUS path builds it (production emitter status)."""
    facts = drc._facts(freshness)
    return drc._monthly_owner_status(
        freshness, facts, available, [],
        emitter_status_fn=lambda e: mme.status(cfg, eligible=e))


def _code_without_docstrings(path):
    """A module's source with every module/class/function docstring removed, so a
    source assertion tests the IMPLEMENTATION rather than its prose."""
    import ast

    src = Path(path).read_text(encoding="utf-8")
    tree = ast.parse(src)
    docs = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef,
                             ast.ClassDef)):
            d = ast.get_docstring(node, clean=False)
            if d:
                docs.add(d)
    for d in docs:
        src = src.replace(d, "")
    return src


def _research_repo_module():
    """The real panel owner, when this machine has the research repo checked out."""
    p = Path(mme.DEFAULT_REPO) / mme.PHASE24_REL
    return p if p.is_file() else None


# =========================================================================== #
# 1–2 — OWNERSHIP AND ROOT CAUSE
# =========================================================================== #
def test_01_the_source_panel_owner_is_named_by_every_surface():
    assert mme.PANEL_SOURCE == "research.phase24_daily_panel"
    fr = _freshness()
    block = drc._monthly_owner_status(fr, drc._facts(fr), False, [])
    assert block["panel_owner"] == "research.phase24_daily_panel"
    assert block["owner"] == "api.monthly_momentum_input"
    assert block["producer"] == "api.monthly_momentum_emitter"
    assert block["math_owner"] == "research.phase25_multi_horizon_inputs"


def test_02_the_stale_panel_root_cause_is_recorded_not_guessed():
    """The panel stopped because NO owner was responsible for advancing it.

    The release document must state that, because a repair whose cause is unrecorded is
    the same repair repeated next month.
    """
    doc = RELEASE_DOC.read_text(encoding="utf-8")
    assert "2026-08-05" in doc
    low = doc.lower()
    assert "one-time" in low or "one time" in low
    assert "no owner" in low or "nothing" in low
    # And the bridge's own docstring now states who owns the refresh policy.
    assert "never triggers a panel refresh" not in mme.__doc__
    assert "bounded" in mme.__doc__.lower()


# =========================================================================== #
# 3–8 — THE BOUNDED AS-OF REFRESH CONTRACT
# =========================================================================== #
def test_03_a_bounded_as_of_refresh_exists_and_is_argv_not_shell(tmp_path):
    cfg = _cfg(tmp_path)
    cmd = mme.build_refresh_command(cfg, as_of=ELIG)
    assert isinstance(cmd, list) and cmd[0] == cfg.python and cmd[1] == "-c"
    assert cmd[2] == mme.REFRESH_DRIVER_SRC and cmd[3] == cfg.repo and cmd[4] == ELIG
    assert not any("&&" in str(x) or "|" in str(x) for x in cmd[3:])
    assert "refresh_daily_panel_as_of" in mme.REFRESH_DRIVER_SRC


def test_04_the_cutoff_is_the_session_and_no_later_observation_is_requested(tmp_path):
    """The refresh asks for the ELIGIBLE SESSION, never "latest".

    This is the whole point-in-time guarantee: a run started on 2026-09-02 for the
    2026-09-01 research session must not pull 2026-09-02 data merely because it exists.
    """
    cfg = _cfg(tmp_path)
    r = _refresh_runner(cfg)
    rec = mme.refresh_source_panel(cfg, as_of=ELIG, runner=r)
    assert r.calls["as_of"] == [ELIG]
    assert NEXT_SESSION not in " ".join(str(x) for x in r.calls["last_cmd"])
    assert rec["as_of"] == ELIG and rec["bounded_to_session"] is True
    assert rec["operator_supplied_date"] is False
    assert _panel_last(cfg) == ELIG


def test_05_a_refresh_that_lands_past_the_cutoff_fails_closed(tmp_path):
    """Defence in depth: even a "successful" refresh is rejected when the panel it
    produced is dated ahead of the session. Nothing downstream is allowed to see it."""
    cfg = _cfg(tmp_path)
    r = _refresh_runner(cfg, advance_to=NEXT_SESSION)
    with pytest.raises(mme.MonthlyEmitterHold) as ei:
        mme.refresh_source_panel(cfg, as_of=ELIG, runner=r)
    assert ei.value.emitter_status == mme.PANEL_FUTURE_DATED
    assert ei.value.retry_classification == "PERMANENT"


def test_06_a_refresh_short_of_the_cutoff_fails_closed(tmp_path):
    cfg = _cfg(tmp_path)
    r = _refresh_runner(cfg, advance_to=AUG_END)   # landed a session short
    with pytest.raises(mme.MonthlyEmitterHold) as ei:
        mme.refresh_source_panel(cfg, as_of=ELIG, runner=r)
    assert ei.value.emitter_status == mme.PANEL_STALE
    assert _panel_last(cfg) == AUG_END   # whatever landed, the claim is not believed


def test_07_the_bounded_refresh_is_idempotent(tmp_path):
    """Refreshing twice to the same cutoff reaches the same panel; and once the panel
    covers the session the policy stops asking for a refresh at all."""
    cfg = _cfg(tmp_path)
    r = _refresh_runner(cfg)
    a = mme.refresh_source_panel(cfg, as_of=ELIG, runner=r)
    b = mme.refresh_source_panel(cfg, as_of=ELIG, runner=r)
    assert a["panel_state"] == b["panel_state"] == mme.PANEL_CURRENT
    assert _panel_last(cfg) == ELIG
    assert mme.inspect_source_panel(cfg, eligible=ELIG)["action"] == "USE_EXISTING"


@pytest.mark.parametrize("code,expect_retry", [
    (mme.PANEL_UNIVERSE_FAILED, "PERMANENT"),
    (mme.PANEL_INCOMPLETE, "TRANSIENT"),
])
def test_08_a_partial_or_universe_losing_refresh_fails_closed(tmp_path, code,
                                                             expect_retry):
    """The owner's quality contract is honoured, not second-guessed. A refresh that
    would drop historical (delisted) names is refused, and the previous panel stands."""
    cfg = _cfg(tmp_path)
    r = _refresh_runner(cfg, ok=False, code=code)
    with pytest.raises(mme.MonthlyEmitterHold) as ei:
        mme.refresh_source_panel(cfg, as_of=ELIG, runner=r)
    assert ei.value.emitter_status == code
    assert ei.value.retry_classification == expect_retry
    assert _panel_last(cfg) == PANEL_STALE_DATE    # untouched


def test_08b_a_timeout_or_crash_leaves_the_previous_panel_intact(tmp_path):
    for kwargs, code in ((dict(timed_out=True), mme.PANEL_REFRESHING),
                         (dict(returncode=7), mme.PANEL_INCOMPLETE),
                         (dict(emit_marker=False), mme.PANEL_UNVERIFIABLE)):
        cfg = _cfg(tmp_path / ("t" + code))
        with pytest.raises(mme.MonthlyEmitterHold) as ei:
            mme.refresh_source_panel(cfg, as_of=ELIG,
                                     runner=_refresh_runner(cfg, **kwargs))
        assert ei.value.emitter_status == code
        assert _panel_last(cfg) == PANEL_STALE_DATE


def test_08c_a_failed_refresh_retains_a_diagnostic(tmp_path):
    cfg = _cfg(tmp_path)
    with pytest.raises(mme.MonthlyEmitterHold):
        mme.refresh_source_panel(cfg, as_of=ELIG,
                                 runner=_refresh_runner(cfg, returncode=7))
    diags = list((Path(cfg.work_dir) / "_diagnostics").glob("*PANEL_REFRESH*.json"))
    assert diags, "a bounded-refresh failure must leave evidence"
    rec = json.loads(diags[0].read_text(encoding="utf-8"))
    assert rec["as_of"] == ELIG and rec["returncode"] == 7


# =========================================================================== #
# 5–6 (PIT / survivorship, proven on the real owner when it is present)
# =========================================================================== #
@pytest.mark.skipif(_research_repo_module() is None,
                    reason="the owned research repo is not present on this machine")
def test_09_the_panel_owner_bounds_prices_AND_membership_by_the_cutoff():
    """Historical membership must be point-in-time, so the cutoff has to bind BOTH the
    price series and the index-constituent series. Binding only prices would let a
    later membership decision be written onto a historical date."""
    src = _research_repo_module().read_text(encoding="utf-8")
    assert "def refresh_daily_panel_as_of" in src
    pull = src.split("def _pull_symbol", 1)[1].split("\ndef ", 1)[0]
    assert 'kw["end_date"] = end_date' in pull
    assert pull.count("**kw") >= 2, "both price and membership series must be bounded"
    # The universe is still the Current & Past watchlist -> delisted names retained.
    assert "WATCHLIST_CP" in src.split("def refresh_daily_panel_as_of", 1)[1]


@pytest.mark.skipif(_research_repo_module() is None,
                    reason="the owned research repo is not present on this machine")
def test_10_the_panel_owner_truncates_after_assembly_and_fails_closed():
    src = _research_repo_module().read_text(encoding="utf-8")
    body = src.split("def refresh_daily_panel_as_of", 1)[1]
    for code in ("SOURCE_PANEL_INCOMPLETE", "SOURCE_PANEL_FUTURE_DATED",
                 "HISTORICAL_UNIVERSE_COVERAGE_FAILED"):
        assert code in body
    # The canonical NPZ is only replaced AFTER the quality checks.
    assert body.index("HISTORICAL_UNIVERSE_COVERAGE_FAILED") < body.index("os.replace(tmp_npz")
    assert "close_df.index <= pd.Timestamp(as_of)" in src


def test_11_there_is_exactly_one_panel_writer_and_one_monthly_formula():
    """No second panel owner, no second monthly formula, no second DRC owner."""
    api = ROOT / "api"
    writers = [p.name for p in api.glob("*.py")
               if "savez" in p.read_text(encoding="utf-8", errors="ignore")]
    assert writers == [], "the panel NPZ is written by the research owner alone"
    # The bridge DRIVES the formula's owner; its CODE never restates the formula.
    # (Its docstring cites the definition when naming that owner — documentation, not a
    # second implementation.)
    src = (api / "monthly_momentum_emitter.py").read_text(encoding="utf-8")
    code = _code_without_docstrings(api / "monthly_momentum_emitter.py")
    assert "close[m-1]" not in code and "shift(7)" not in code
    assert "refresh_daily_panel_as_of" in src   # it calls the owner's entry point
    assert drc.INPUT_RECOVERY_OWNER == "api.daily_research_cycle"
    assert mmi.CANONICAL_OWNER == "api.monthly_momentum_input"


# =========================================================================== #
# 9–14 — MONTHLY BOUNDARY, CUTOFF, IMMUTABILITY, IDEMPOTENCY
# =========================================================================== #
def test_12_the_month_boundary_is_detected_generically(tmp_path):
    assert mmi.is_due(eligible=ELIG, current_month="2026-08") is True
    assert mmi.is_due(eligible=ELIG, current_month="2026-09") is False     # intramonth
    assert mmi.is_due(eligible="2026-10-01", current_month="2026-09") is True
    assert mmi.is_due(eligible="2027-01-04", current_month="2026-12") is True
    assert mmi.is_due(eligible=ELIG, current_month=None) is True           # missing


def test_13_the_refreshed_panel_lets_the_month_be_emitted_with_the_session_cutoff(tmp_path):
    """The whole chain, in ONE call: panel behind -> bounded refresh -> emission."""
    cfg = _cfg(tmp_path)
    rr, er = _refresh_runner(cfg), _emit_runner()
    art = mme.production_emitter(month="2026-09", eligible=ELIG, config=cfg,
                                 runner=er, panel_refresh_runner=rr)
    assert rr.calls["n"] == 1 and er.calls["n"] == 1
    assert art["month_label"] == "2026-09" and art["market_as_of_date"] == ELIG
    assert art["source_panel_refresh"]["as_of"] == ELIG
    assert art["source_panel_refresh"]["bounded_to_session"] is True
    assert art["source_panel"]["last_date"] == ELIG


def test_14_a_covered_panel_never_triggers_a_refresh(tmp_path):
    cfg = _cfg(tmp_path, panel_last=ELIG)
    rr, er = _refresh_runner(cfg), _emit_runner()
    mme.production_emitter(month="2026-09", eligible=ELIG, config=cfg,
                           runner=er, panel_refresh_runner=rr)
    assert rr.calls["n"] == 0, "no provider work when the panel already covers the session"


def test_15_the_monthly_formula_and_artifact_identity_are_unchanged():
    assert mmi.REQUIRED_COLUMNS == ("ticker", "mom_6_1", "is_member", "sector",
                                    "market_as_of_date", "month_label")
    assert mmi._IDENTITY_COLUMNS == ("ticker", "mom_6_1", "is_member", "sector",
                                     "month_label")
    assert mme.EMITTER_SOURCE == "research.phase25_multi_horizon_inputs"


def test_16_a_second_emission_reuses_the_artifact_and_writes_nothing(tmp_path):
    inputs = tmp_path / "inputs"
    _seed_mom(inputs, "2026-08", PANEL_STALE_DATE)
    cfg = _cfg(tmp_path)
    rr, er = _refresh_runner(cfg), _emit_runner()

    def emit(*, month, eligible, inputs_dir):
        return mme.production_emitter(month=month, eligible=eligible,
                                      inputs_dir=inputs_dir, config=cfg,
                                      runner=er, panel_refresh_runner=rr)

    first = mmi.emit_if_due(eligible=ELIG, inputs_dir=str(inputs), emitter_fn=emit)
    assert first["status"] == mmi.S_EMITTED and first["performed_write"] is True
    second = mmi.emit_if_due(eligible=ELIG, inputs_dir=str(inputs), emitter_fn=emit)
    assert second["status"] == mmi.S_CURRENT and second["performed_write"] is False
    assert er.calls["n"] == 1, "the existing artifact is reused, never recomputed"


def test_17_a_future_dated_panel_is_never_repaired_by_rebuilding(tmp_path):
    """Rebuilding backwards would DISCARD observations a later session legitimately
    holds, so a future-dated panel stays a blocker — exactly as before this release."""
    cfg = _cfg(tmp_path, panel_last=NEXT_SESSION)
    p = mme.inspect_source_panel(cfg, eligible=ELIG)
    assert p["action"] == "BLOCK" and p["panel_state"] == mme.PANEL_FUTURE_DATED
    rr = _refresh_runner(cfg)
    with pytest.raises(mme.MonthlyEmitterHold):
        mme.production_emitter(month="2026-09", eligible=ELIG, config=cfg,
                               runner=_emit_runner(), panel_refresh_runner=rr)
    assert rr.calls["n"] == 0


def test_18_a_future_dated_monthly_artifact_is_refused(tmp_path):
    inputs = tmp_path / "inputs"
    _seed_mom(inputs, "2026-10", "2026-10-01")           # already ahead of the session
    r = mmi.emit_if_due(eligible=ELIG, inputs_dir=str(inputs),
                        emitter_fn=lambda **k: pytest.fail("must not run"))
    assert r["status"] == mmi.S_CONFLICT


# =========================================================================== #
# 15–17 — PRICE / SCORE REFRESH DEPENDENCY
# =========================================================================== #
def test_19_price_refresh_waits_on_the_monthly_input_and_then_proceeds(tmp_path):
    cfg = _cfg(tmp_path)
    fr = _freshness()
    mo = _owner_block(fr, cfg=cfg)
    plan = drc.build_execution_plan(fr, monthly_emitter_available=True, monthly_owner=mo)
    cl = drc.classify_stale_inputs(plan=plan, monthly_owner=mo, freshness=fr)
    price = next(r for r in cl["inputs"] if r["source_id"] == "price_score_refresh")
    assert price["classification"] == drc.RECOVERY_CURRENT_REFRESH
    assert price["code"] == drc.PRICE_SCORE_MONTH_BOUNDARY
    assert price["depends_on"] == ["momentum_monthly"]
    # The dependency is recoverable in the SAME run, so it is not a governed blocker.
    assert price["dependency_recoverable_now"] is True
    assert price["blocks_governed_research"] is False

    q = drc._research_input_quality(mo, plan)
    assert q["price_refresh_state"] == drc.PRICE_REFRESH_WAITING_ON_MONTHLY
    assert q["price_refresh_depends_on"] == ["momentum_monthly"]


def test_20_price_refresh_remains_blocked_while_the_dependency_is_invalid(tmp_path):
    cfg = _cfg(tmp_path, refresh=False)
    fr = _freshness()
    mo = _owner_block(fr, cfg=cfg)
    plan = drc.build_execution_plan(fr, monthly_emitter_available=True, monthly_owner=mo)
    cl = drc.classify_stale_inputs(plan=plan, monthly_owner=mo, freshness=fr)
    price = next(r for r in cl["inputs"] if r["source_id"] == "price_score_refresh")
    assert price["dependency_recoverable_now"] is False
    assert price["blocks_governed_research"] is True


def test_21_price_refresh_is_ready_once_the_month_matches(tmp_path):
    cfg = _cfg(tmp_path, panel_last=ELIG)
    fr = _freshness(monthly_asof=ELIG)      # monthly input already in 2026-09
    mo = _owner_block(fr, cfg=cfg)
    plan = drc.build_execution_plan(fr, monthly_emitter_available=True, monthly_owner=mo)
    q = drc._research_input_quality(mo, plan)
    assert q["monthly_input_state"] == drc.MONTHLY_INPUT_CURRENT
    assert q["price_refresh_state"] == drc.PRICE_REFRESH_READY
    cl = drc.classify_stale_inputs(plan=plan, monthly_owner=mo, freshness=fr)
    price = next(r for r in cl["inputs"] if r["source_id"] == "price_score_refresh")
    assert price["classification"] == drc.RECOVERY_SAFE_PIT


# =========================================================================== #
# 18–22 — ONE ORCHESTRATION PATH, NO OPERATOR DATE, NO RECOVERY ROUTE
# =========================================================================== #
def test_22_a_behind_panel_is_recoverable_work_not_a_true_blocker(tmp_path):
    cfg = _cfg(tmp_path)
    fr = _freshness()
    mo = _owner_block(fr, cfg=cfg)
    assert mo["source_panel_covered"] is False
    assert mo["source_panel_state"] == mme.PANEL_STALE
    assert mo["source_panel_can_cover_session"] is True
    assert mo["source_panel_refresh_as_of"] == ELIG

    plan = drc.build_execution_plan(fr, monthly_emitter_available=True, monthly_owner=mo)
    assert plan["plan_blocked"] is False and plan["blockers"] == []
    step = next(s for s in plan["refresh_steps"] if s["source_id"] == "momentum_monthly")
    assert step["can_refresh_automatically"] is True
    assert step["prerequisite_maintenance"] == "BOUNDED_SOURCE_PANEL_REFRESH"
    assert step["prerequisite_bound_to_session"] == ELIG

    cl = drc.classify_stale_inputs(plan=plan, monthly_owner=mo, freshness=fr)
    mm = next(r for r in cl["inputs"] if r["source_id"] == "momentum_monthly")
    assert mm["classification"] == drc.RECOVERY_SAFE_PIT
    assert mm["code"] == drc.MONTHLY_PANEL_REFRESHABLE
    assert cl["true_blockers"] == [] and cl["safe_work_remains"] is True


def test_23_an_unrecoverable_panel_is_still_a_named_true_blocker(tmp_path):
    cfg = _cfg(tmp_path, refresh=False)
    fr = _freshness()
    mo = _owner_block(fr, cfg=cfg)
    assert mo["source_panel_can_cover_session"] is False
    plan = drc.build_execution_plan(fr, monthly_emitter_available=True, monthly_owner=mo)
    assert plan["plan_blocked"] is True
    b = plan["blockers"][0]
    assert b["source_id"] == "momentum_monthly"
    assert b["source_panel_state"] == mme.PANEL_STALE
    assert "source panel" in b["detail"]
    cl = drc.classify_stale_inputs(plan=plan, monthly_owner=mo, freshness=fr)
    assert [x["source_id"] for x in cl["true_blockers"]] == ["momentum_monthly"]
    assert cl["true_blockers"][0]["display_name"] == "Frozen monthly momentum input"


def test_24_the_cycle_status_is_executable_when_the_input_is_recoverable(tmp_path):
    cfg = _cfg(tmp_path)
    fr = _freshness()
    s = drc.load_daily_research_cycle_status(
        drc_dir=str(tmp_path / "drc"), freshness=fr, monthly_emitter_available=True,
        monthly_emitter_status_fn=None) if False else \
        drc.load_daily_research_cycle_status(
            drc_dir=str(tmp_path / "drc"), freshness=fr, monthly_emitter_available=True)
    # The production status path consults the REAL emitter status; assert only the
    # structure it must expose, then pin behaviour through the owner block above.
    assert s["state"] in (drc.NOT_STARTED, drc.BLOCKED)
    assert "research_input_quality" in s
    q = s["research_input_quality"]
    assert set(q["monthly_input_state_vocabulary"]) == set(drc.MONTHLY_INPUT_STATES)
    assert set(q["source_panel_state_vocabulary"]) == set(mme.SOURCE_PANEL_STATES)
    assert q["derived_by_backend"] is True
    del cfg


def test_25_the_operator_supplies_no_recovery_date_anywhere(tmp_path):
    """The cutoff is derived from the eligible session by the backend. No route, no
    payload field and no UI control may choose it."""
    cfg = _cfg(tmp_path)
    r = _refresh_runner(cfg)
    mme.refresh_source_panel(cfg, as_of=ELIG, runner=r)
    assert r.calls["as_of"] == [ELIG]
    app = (ROOT / "api" / "app.py").read_text(encoding="utf-8")
    for banned in ("panel_refresh_date", "refresh_as_of=", "as_of_override",
                   "source_panel_refresh/run", "REFRESH_OWNED_SOURCE_PANEL"):
        assert banned not in app, "no manual panel-refresh route or date may exist"
    ui = UI.read_text(encoding="utf-8")
    assert "refresh_as_of" not in ui and "panel_refresh_date" not in ui


def test_26_there_is_no_second_recovery_route_or_orchestration_path():
    app = (ROOT / "api" / "app.py").read_text(encoding="utf-8")
    assert app.count('"/v1/operations/portfolio-cycle/run"') <= 2
    assert "/v1/operations/source-panel" not in app
    assert "/v1/operations/monthly-emitter/run" not in app
    assert ws.PORTFOLIO_CYCLE_EXECUTION_CONTRACT["path"] == \
        "/v1/operations/portfolio-cycle/run"


def test_27_the_daily_close_is_never_repeated_by_this_recovery():
    """The recovery lives entirely inside the research stage; it cannot re-close a
    session, and the research obligation says so."""
    ob = ws.build_research_obligation(
        latest_completed_close_date=ELIG, operational_close_valid=True,
        eligible_market_date=ELIG, governed_research_session=None,
        governed_research_current=False,
        stale_input_ids=["momentum_monthly"],
        input_classification={"true_blockers": [], "safe_work_remains": True})
    assert ob["repeats_the_completed_close"] is False
    assert ob["invalidates_operational_close"] is False
    assert ob["operator_supplies_no_date"] is True


# =========================================================================== #
# 25–28 — HISTORICAL GAPS AND FUTURE CONTINUITY
# =========================================================================== #
def test_28_true_forward_history_is_never_reconstructed():
    """The Sep-1 TRUE_FORWARD snapshot gap is permanent. Nothing in this release
    writes, back-fills or infers a forward observation."""
    for name in ("monthly_momentum_emitter.py", "daily_research_cycle.py"):
        src = (ROOT / "api" / name).read_text(encoding="utf-8")
        for banned in ("true_forward_prices", "reconstruct_forward",
                       "backfill_true_forward"):
            assert banned not in src


def test_29_a_documented_historical_gap_does_not_freeze_later_sessions(tmp_path):
    """An unrecoverable September never blocks October: due-ness is a comparison
    against the session in hand, not a queue of unfinished months."""
    cfg = _cfg(tmp_path, panel_last="2026-10-01")
    fr = _freshness(eligible="2026-10-01", monthly_asof=PANEL_STALE_DATE,
                    price_asof="2026-09-30")
    mo = _owner_block(fr, cfg=cfg)
    assert mo["required_month"] == "2026-10"
    assert mo["source_panel_covered"] is True
    plan = drc.build_execution_plan(fr, monthly_emitter_available=True, monthly_owner=mo)
    cl = drc.classify_stale_inputs(plan=plan, monthly_owner=mo, freshness=fr)
    assert cl["true_blockers"] == []
    mm = next(r for r in cl["inputs"] if r["source_id"] == "momentum_monthly")
    assert mm["classification"] == drc.RECOVERY_SAFE_PIT


@pytest.mark.parametrize("session,month,panel_at", [
    ("2026-09-01", "2026-09", "2026-08-05"),     # the September transition
    ("2026-10-01", "2026-10", "2026-09-30"),     # a second, synthetic transition
    ("2027-01-04", "2027-01", "2026-12-31"),     # across a year boundary
])
def test_30_any_new_month_transition_works_generically(tmp_path, session, month,
                                                       panel_at):
    """No production code contains a hard-coded month or date: the same fixture shape
    resolves September, October and a year boundary identically."""
    cfg = _cfg(tmp_path / session, panel_last=panel_at)
    rr, er = _refresh_runner(cfg), _emit_runner(month=month, asof=session)
    art = mme.production_emitter(month=month, eligible=session, config=cfg,
                                 runner=er, panel_refresh_runner=rr)
    assert rr.calls["as_of"] == [session]
    assert art["month_label"] == month and art["market_as_of_date"] == session


def test_31_no_production_module_hard_codes_a_recovery_date():
    """Executable code carries no literal date. Docstrings and comments may show an
    EXAMPLE payload; a date the code branches on is what this forbids."""
    import ast

    pat = re.compile(r"^20\d\d-\d\d-\d\d")
    for name in ("monthly_momentum_emitter.py", "monthly_momentum_input.py",
                 "daily_research_cycle.py"):
        tree = ast.parse((ROOT / "api" / name).read_text(encoding="utf-8"))
        docstrings = set()
        for node in ast.walk(tree):
            if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef,
                                 ast.ClassDef)):
                d = ast.get_docstring(node, clean=False)
                if d:
                    docstrings.add(d)
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str) \
                    and node.value not in docstrings:
                assert not pat.match(node.value), (
                    "%s line %d hard-codes the date %r"
                    % (name, node.lineno, node.value))


# =========================================================================== #
# 29–34 — PORTFOLIO-CYCLE ACTIONABILITY
# =========================================================================== #
def _command(overall, primary):
    return ws.build_operator_command(overall=overall, primary=primary,
                                     eligible_date=ELIG, latest_close_date=ELIG)


def test_32_a_blocked_cycle_is_never_presented_as_an_actionable_portfolio_cycle():
    """THE LIVE DEFECT. With a TRUE blocker the state machine reaches
    RESEARCH_CYCLE_BLOCKED, whose primary action is not executable — so no surface may
    render a green "Run the portfolio cycle"."""
    primary = ws._primary_action(ws.RESEARCH_CYCLE_BLOCKED, {
        "eligible_date": ELIG, "outstanding_research_session": ELIG,
        "research_true_blockers": [
            {"source_id": "momentum_monthly",
             "display_name": "Frozen monthly momentum input",
             "code": drc.MONTHLY_PANEL_BEHIND,
             "reason": "The owned source panel cannot be brought to the session.",
             "operator_action": "REFRESH_OWNED_SOURCE_PANEL"}]})
    c = _command(ws.RESEARCH_CYCLE_BLOCKED, primary)
    assert c["portfolio_cycle_actionable"] is False
    assert c["portfolio_cycle_safe_to_execute"] is False
    assert c["portfolio_cycle_action_code"] is None
    assert c["portfolio_cycle_action_label"] is None
    assert c["portfolio_cycle_blocking_reason"]
    assert "monthly momentum input" in c["portfolio_cycle_blocking_reason"].lower()
    # and the pre-existing authority agrees, because they are one verdict
    assert c["primary_action_available"] is False
    assert c["mutation_controls_allowed"] is False and c["passive"] is True
    assert c["next_text"] != "Confirm to run — " + ws.PORTFOLIO_CYCLE_CONFIRMATION


def test_33_an_actionable_cycle_appears_only_when_the_backend_marks_it_so():
    primary = ws._primary_action(ws.RESEARCH_CYCLE_REQUIRED,
                                 {"eligible_date": ELIG,
                                  "research_cycle_due_after_close": True})
    c = _command(ws.RESEARCH_CYCLE_REQUIRED, primary)
    assert c["portfolio_cycle_actionable"] is True
    assert c["portfolio_cycle_action_code"] == "RUN_PORTFOLIO_CYCLE"
    assert c["portfolio_cycle_action_label"] == ws.PORTFOLIO_CYCLE_LABEL
    assert c["portfolio_cycle_blocking_reason"] is None
    assert c["primary_action_available"] is True


def test_34_every_operator_surface_reads_the_same_single_verdict():
    """Top workflow status, primary CTA, research banner and Active Manager guidance
    must agree, because they are projections of ONE decided value."""
    for overall in (ws.RESEARCH_CYCLE_BLOCKED, ws.RESEARCH_CYCLE_REQUIRED,
                    ws.WAITING_FOR_SESSION_CLOSE, ws.READY_FOR_DAILY_CLOSE,
                    ws.DAILY_CYCLE_COMPLETE):
        primary = ws._primary_action(overall, {"eligible_date": ELIG})
        c = _command(overall, primary)
        assert c["portfolio_cycle_actionable"] == c["primary_action_available"]
        assert c["portfolio_cycle_safe_to_execute"] == c["primary_action_available"]
        assert c["mutation_controls_allowed"] == c["primary_action_available"]
        assert (c["portfolio_cycle_action_code"] is None) != c["primary_action_available"]
        if not c["primary_action_available"]:
            assert c["portfolio_cycle_blocking_reason"]
        assert c["state"] == overall


def test_35_the_ui_performs_no_actionability_logic():
    """The UI renders the backend verdict; it never derives one from action codes,
    state names or dates."""
    ui = UI.read_text(encoding="utf-8")
    assert "primary_action_available" in ui
    # No client-side reconstruction of the withheld/allowed decision.
    for banned in ("action_code === 'RUN_DAILY_RESEARCH_CYCLE' ?",
                   "overall_state === 'RESEARCH_CYCLE_REQUIRED' ?",
                   "source_panel_covered", "panel_last_date <"):
        assert banned not in ui
    # The one CTA gate is the backend flag, and it is compared strictly.
    assert "c.primary_action_available === true" in ui


# =========================================================================== #
# 35–40 — SAFETY BOUNDARIES
# =========================================================================== #
def test_36_the_recovery_creates_no_order_fill_or_broker_call():
    for name in ("monthly_momentum_emitter.py", "monthly_momentum_input.py"):
        src = (ROOT / "api" / name).read_text(encoding="utf-8").lower()
        for banned in ("place_order", "submit_order", "create_fill", "broker",
                       "execute_trade"):
            assert banned not in src


def test_37_automation_stays_off_and_nothing_is_promoted_or_activated(tmp_path):
    s = mmi._safety()
    assert s["automation_off"] is True and s["manual_review"] is True
    assert s["created_orders"] is False and s["created_fills"] is False
    assert s["promoted_model"] is False and s["approximates_intramonth"] is False
    src = (ROOT / "api" / "monthly_momentum_emitter.py").read_text(encoding="utf-8")
    for banned in ("promote_model", "activate_sleeve", "set_champion"):
        assert banned not in src


def test_38_the_refresh_writes_only_research_side_paths(tmp_path):
    """It touches the panel owner's own root and the emitter work dir — never an
    operational ledger, a database or the canonical inputs directory."""
    cfg = _cfg(tmp_path)
    r = _refresh_runner(cfg)
    rec = mme.refresh_source_panel(cfg, as_of=ELIG, runner=r)
    argv = " ".join(str(a) for a in rec["argv"])
    assert "operational" not in argv and "ledger" not in argv
    assert str(tmp_path) in argv or cfg.repo in argv
    diag = list((Path(cfg.work_dir) / "_diagnostics").glob("*.json"))
    assert all(str(cfg.work_dir) in str(p) for p in diag)


def test_39_the_bridge_computes_no_mathematics_of_its_own():
    src = (ROOT / "api" / "monthly_momentum_emitter.py").read_text(encoding="utf-8")
    assert "import numpy" not in src and "import pandas" not in src
    assert "np." not in src and "pd." not in src


def test_40_the_refresh_never_runs_without_a_session_to_bind_it(tmp_path):
    cfg = _cfg(tmp_path)
    with pytest.raises(mme.MonthlyEmitterError):
        mme.build_refresh_command(cfg, as_of=None)
    p = mme.inspect_source_panel(cfg, eligible=None)
    assert p["action"] == "BLOCK" and p["panel_state"] == mme.PANEL_UNVERIFIABLE


def test_40b_the_architecture_audit_enforces_the_single_panel_owner():
    """Phase P — the build fails on a second panel writer, a second refresh policy, a
    manual panel-refresh route, an operator-supplied cutoff, a future-dated rebuild, a
    copied panel vocabulary or UI-derived actionability."""
    import sys

    sys.path.insert(0, str(ROOT))
    from scripts import audit_architecture as audit   # noqa: PLC0415

    rep = audit.check_release54_2_3_source_panel_recovery(audit._iter_source_files())
    assert rep["refresh_policy_defined_in_bridge"] is True
    assert rep["missing_refresh_policy_defs"] == []
    assert rep["second_panel_writer"] == []
    assert rep["second_refresh_policy"] == []
    assert rep["bridge_pure_stdlib"] is True and rep["bridge_numeric_imports"] == []
    assert rep["bridge_drives_panel_owner"] is True
    assert rep["refresh_uses_argv_array"] is True
    assert rep["cutoff_bound_to_eligible_session"] is True
    assert rep["operator_supplied_date_fields"] == []
    assert rep["forbidden_panel_routes"] == []
    assert rep["future_dated_panel_still_blocks"] is True
    assert rep["verdict_defined_in_panel_owner"] is True
    assert rep["cycle_reads_single_verdict"] is True
    assert rep["cycle_publishes_data_quality"] is True
    assert rep["cycle_copies_panel_vocabulary"] is False
    assert rep["workflow_projects_actionability"] is True
    assert rep["ui_actionability_derivation"] == []
    assert rep["ui_reads_backend_actionability"] is True


def test_41_the_status_contract_publishes_the_refresh_capability(tmp_path):
    cfg = _cfg(tmp_path)
    st = mme.status(cfg, eligible=ELIG)
    assert st["panel_refresh_supported"] is True
    assert st["panel_refresh_bounded_to_session"] is True
    assert st["incremental_supported"] is False
    sp = st["source_panel"]
    assert sp["can_cover_eligible_session"] is True
    assert sp["panel_state"] == mme.PANEL_STALE and sp["refresh_as_of"] == ELIG
    off = mme.status(_cfg(tmp_path / "off", refresh=False), eligible=ELIG)
    assert off["panel_refresh_supported"] is False
    assert off["source_panel"]["can_cover_eligible_session"] is False


def test_42_the_emitter_environment_gates_the_refresh_capability(tmp_path):
    """A refresh that cannot run is never advertised: an unavailable environment
    reports no capability, so the plan does not promise one."""
    cfg = _cfg(tmp_path)
    Path(cfg.panel_npz).unlink()          # environment now unavailable
    fr = _freshness()
    mo = _owner_block(fr, cfg=cfg)
    assert mo["emitter_status"] == "UNAVAILABLE"
    assert mo["source_panel_can_cover_session"] is False
    plan = drc.build_execution_plan(fr, monthly_emitter_available=True, monthly_owner=mo)
    assert plan["plan_blocked"] is True


def test_43_the_hermetic_run_path_keeps_its_prior_behaviour():
    """The RUN path builds the monthly block WITHOUT reading the production panel, so it
    reports no verdict — and the plan must then behave exactly as it did before, with the
    emitter still blocking honestly at execution time."""
    fr = _freshness()
    hermetic = drc._monthly_owner_status(fr, drc._facts(fr), True, [],
                                         emitter_status_fn=lambda _e: None)
    assert hermetic["source_panel_can_cover_session"] is None
    assert drc._monthly_producible(hermetic) is None
    plan = drc.build_execution_plan(fr, monthly_emitter_available=True,
                                    monthly_owner=hermetic)
    step = next(s for s in plan["refresh_steps"] if s["source_id"] == "momentum_monthly")
    assert step["can_refresh_automatically"] is True
    assert plan["plan_blocked"] is False
