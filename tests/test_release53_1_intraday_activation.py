"""Release 53.1 - intraday activation & alpha-to-capital conversion.

What must stay true:

* **The collector's durable task definition has ONE owner** whose decisions
  (INSTALL / UNCHANGED / BLOCKED / MIGRATE) are proven hermetically against
  snapshot JSON with the real PowerShell binder - and an Interactive
  principal NEVER validates (the 2026-08-28 defect).
* **A delayed feed never masquerades as real-time** - latency classes come
  from measured delay, forming bars are not observations, and the mark
  function only serves prints observable at or after the requested instant.
* **The frozen R53 specs were not retuned** - every spec hash matches the
  registration-time record.
* **Prospective discipline survives the adapter** - emission through the
  real signal path appends TRUE_FORWARD rows only, refuses staleness,
  dedupes, and never backfills.
* **The risk-budget shadow can see diversification** (a score-only hurdle
  cannot) while conserving NAV, respecting whole units, collateral and
  budgets - and mutating nothing in production.
* **Production stays long-only, unpromoted, unordered, unchanged.**
"""
from __future__ import annotations

import datetime as _dt
import json
import math
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO.parent))

from alpha_agent import r53_1 as R                                  # noqa: E402
from alpha_agent.r53 import intraday_factory as IF                  # noqa: E402
from alpha_agent.r53_1 import intraday_feed as FEED                 # noqa: E402
from alpha_agent.r53_1 import intraday_signals as SIG               # noqa: E402
from alpha_agent.r53_1 import risk_budget as RB                     # noqa: E402
from alpha_agent.r53_1 import executable_universe as EU             # noqa: E402
from alpha_agent.r53_1 import short_capability as SC                # noqa: E402
from alpha_agent.r53_1 import collection_runtime as CR              # noqa: E402
from alpha_agent.r46 import intraday as LANE                        # noqa: E402

PS = ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File"]
INSTALLER = REPO / "scripts" / "install_information_collection_task.ps1"
VALIDATOR = REPO / "scripts" / "validate_information_collection_task.ps1"
EM_INSTALLER = REPO / "scripts" / "install_intraday_emission_task.ps1"


def _utc(*a) -> _dt.datetime:
    return _dt.datetime(*a, tzinfo=_dt.timezone.utc)


def _run_ps(script: Path, *args: str) -> str:
    out = subprocess.run(PS + [str(script), *args], capture_output=True,
                         text=True, timeout=180, cwd=str(REPO))
    return (out.stdout or "") + (out.stderr or "")


def _user() -> str:
    return "%s\\%s" % (os.environ.get("USERDOMAIN", "HOST"),
                       os.environ.get("USERNAME", "user"))


def _collection_snapshot(**over) -> dict:
    """A snapshot exactly matching the installer's desired definition."""
    snap = {
        "TaskName": "PaperTrader-InformationCollection",
        "State": "Ready", "Enabled": True,
        "Action": {
            "Execute": r"C:\Users\binis\paper_trader\.venv-win\Scripts"
                       r"\python.exe",
            "Arguments": '"C:\\Users\\binis\\paper_trader\\scripts'
                         '\\run_information_collection_service.py" '
                         "--interval-seconds 60",
            "WorkingDirectory": r"C:\Users\binis\paper_trader"},
        "Triggers": [
            {"Type": "MSFT_TaskBootTrigger", "StartBoundary": None,
             "Enabled": True, "RepetitionInterval": None},
            {"Type": "MSFT_TaskDailyTrigger",
             "StartBoundary": "2026-09-01T00:05:00", "Enabled": True,
             "DaysInterval": 1,
             "RepetitionInterval": "PT30M", "RepetitionDuration": "P1D"}],
        "Principal": {"UserId": _user(), "LogonType": "S4U",
                      "RunLevel": "Limited"},
        "Settings": {"StartWhenAvailable": True,
                     "MultipleInstances": "IgnoreNew",
                     "ExecutionTimeLimit": "PT0S",
                     "RestartCount": 3, "RestartInterval": "PT5M"},
    }
    snap.update(over)
    return snap


# =========================================================================== #
# 1. Collection task: ONE definition owner, hermetic decisions
# =========================================================================== #
class TestCollectionTaskDefinition:
    def test_fresh_install_decision(self):
        out = _run_ps(INSTALLER, "-DecisionProbe", "ABSENT")
        flat = "".join(out.split())
        assert '"decision":"INSTALL"' in flat
        assert '"requested_logon_type":"S4U"' in flat

    def test_identical_definition_is_unchanged(self, tmp_path):
        p = tmp_path / "snap.json"
        p.write_text(json.dumps(_collection_snapshot()), encoding="utf-8")
        out = _run_ps(INSTALLER, "-DecisionProbe", str(p))
        assert "UNCHANGED" in out

    def test_interactive_principal_blocks_without_force(self, tmp_path):
        snap = _collection_snapshot()
        snap["Principal"]["LogonType"] = "Interactive"
        p = tmp_path / "snap.json"
        p.write_text(json.dumps(snap), encoding="utf-8")
        out = _run_ps(INSTALLER, "-DecisionProbe", str(p))
        assert "BLOCKED_PRINCIPAL" in out

    def test_interactive_principal_migrates_with_force(self, tmp_path):
        snap = _collection_snapshot()
        snap["Principal"]["LogonType"] = "Interactive"
        p = tmp_path / "snap.json"
        p.write_text(json.dumps(snap), encoding="utf-8")
        out = _run_ps(INSTALLER, "-DecisionProbe", str(p), "-Force")
        assert "MIGRATE" in out

    def test_missing_recovery_trigger_blocks(self, tmp_path):
        snap = _collection_snapshot()
        snap["Triggers"] = [snap["Triggers"][0]]        # boot only
        p = tmp_path / "snap.json"
        p.write_text(json.dumps(snap), encoding="utf-8")
        out = _run_ps(INSTALLER, "-DecisionProbe", str(p))
        assert "BLOCKED_DEFINITION" in out and "repetition" in out

    # ---- R53.1 hotfix: the recovery trigger must be WINDOWS-VALID -------- #
    # Task Scheduler rejected the serialized TimeSpan.MaxValue duration
    # (P99999999DT23H59M59S) at the first operator migration.

    def test_installers_never_use_timespan_maxvalue(self):
        for script in (INSTALLER, EM_INSTALLER):
            src = script.read_text(encoding="utf-8", errors="replace")
            assert "::MaxValue" not in src, script.name

    def test_recovery_trigger_serializes_to_a_supported_form(self):
        out = _run_ps(INSTALLER, "-TriggerProbe")
        probe = json.loads(out[out.index("{"):out.rindex("}") + 1])
        assert "Boot" in probe["boot_class"]
        assert probe["boot_delay"] == "PT2M"
        assert "Daily" in probe["recovery_class"]
        assert probe["days_interval"] == 1
        assert probe["repetition_interval"] == "PT30M"
        assert probe["stop_at_duration_end"] is False
        assert probe["coverage"] == "CONTINUOUS"
        dur = probe["repetition_duration"]
        assert "99999999" not in dur
        m = re.fullmatch(
            r"P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?", dur)
        assert m, "duration %r is not a plain ISO-8601 duration" % dur
        seconds = (int(m.group(1) or 0) * 86400 + int(m.group(2) or 0) * 3600
                   + int(m.group(3) or 0) * 60 + int(m.group(4) or 0))
        assert seconds == 86400, "duration %r must cover exactly 1 day" % dur

    def test_legacy_indefinite_repetition_is_still_unchanged(self, tmp_path):
        # An already-registered indefinite repetition is ALSO continuous
        # coverage; the comparator must not force churn onto it.
        snap = _collection_snapshot()
        snap["Triggers"][1] = {
            "Type": "MSFT_TaskTimeTrigger",
            "StartBoundary": "2026-09-01T00:05:00", "Enabled": True,
            "RepetitionInterval": "PT30M"}
        p = tmp_path / "snap.json"
        p.write_text(json.dumps(snap), encoding="utf-8")
        out = _run_ps(INSTALLER, "-DecisionProbe", str(p))
        assert "UNCHANGED" in out

    def test_finite_repetition_without_daily_recurrence_blocks(self, tmp_path):
        # PT30M for one day on a ONCE trigger stops forever after a day.
        snap = _collection_snapshot()
        snap["Triggers"][1] = {
            "Type": "MSFT_TaskTimeTrigger",
            "StartBoundary": "2026-09-01T00:05:00", "Enabled": True,
            "RepetitionInterval": "PT30M", "RepetitionDuration": "P1D"}
        p = tmp_path / "snap.json"
        p.write_text(json.dumps(snap), encoding="utf-8")
        out = _run_ps(INSTALLER, "-DecisionProbe", str(p))
        assert "BLOCKED_DEFINITION" in out
        assert "repetition" in out

    XML_ERR = ("The task XML contains a value which is incorrectly formatted "
               "or out of range. (12,42):Duration:P99999999DT23H59M59S")

    def test_xml_rejection_is_not_mislabeled_as_elevation(self):
        out = _run_ps(INSTALLER, "-ClassifyProbe", self.XML_ERR,
                      "-ClassifyShell", "Elevated")
        assert "DEFINITION_REJECTED_BY_SCHEDULER" in out
        assert "ELEVATION_REQUIRED" not in out
        assert "not an elevation problem" in out

    def test_access_denied_without_elevation_is_elevation_required(self):
        out = _run_ps(INSTALLER, "-ClassifyProbe", "Access is denied.",
                      "-ClassifyShell", "NotElevated")
        assert "ELEVATION_REQUIRED" in out

    def test_access_denied_while_elevated_is_not_blamed_on_elevation(self):
        out = _run_ps(INSTALLER, "-ClassifyProbe", "Access is denied.",
                      "-ClassifyShell", "Elevated")
        assert "ACCESS_DENIED_WHILE_ELEVATED" in out
        assert "ELEVATION_REQUIRED" not in out

    def test_unknown_registration_error_surfaces_verbatim(self):
        out = _run_ps(INSTALLER, "-ClassifyProbe",
                      "The network path was not found XYZ123",
                      "-ClassifyShell", "Elevated")
        assert "REGISTRATION_ERROR" in out
        assert "XYZ123" in out

    def test_validator_accepts_corrected_trigger_design(self, tmp_path):
        good = {"Triggers": [
            {"Type": "MSFT_TaskBootTrigger", "Enabled": True},
            {"Type": "MSFT_TaskDailyTrigger", "Enabled": True,
             "DaysInterval": 1, "RepetitionInterval": "PT30M",
             "RepetitionDuration": "P1D"}]}
        p = tmp_path / "good.json"
        p.write_text(json.dumps(good), encoding="utf-8")
        out = _run_ps(VALIDATOR, "-TriggerProbe", str(p))
        assert "R53_1_TRIGGER_CONTRACT_OK" in out

        bad = {"Triggers": [
            {"Type": "MSFT_TaskBootTrigger", "Enabled": True},
            {"Type": "MSFT_TaskTimeTrigger", "Enabled": True,
             "RepetitionInterval": "PT30M", "RepetitionDuration": "P1D"}]}
        p2 = tmp_path / "bad.json"
        p2.write_text(json.dumps(bad), encoding="utf-8")
        out2 = _run_ps(VALIDATOR, "-TriggerProbe", str(p2))
        assert "R53_1_TRIGGER_CONTRACT_PROBLEMS" in out2
        assert "not continuous" in out2

    def test_validator_rejects_worker_killing_stop_at_duration_end(self, tmp_path):
        kill = {"Triggers": [
            {"Type": "MSFT_TaskBootTrigger", "Enabled": True},
            {"Type": "MSFT_TaskDailyTrigger", "Enabled": True,
             "DaysInterval": 1, "RepetitionInterval": "PT30M",
             "RepetitionDuration": "P1D", "StopAtDurationEnd": True}]}
        p = tmp_path / "kill.json"
        p.write_text(json.dumps(kill), encoding="utf-8")
        out = _run_ps(VALIDATOR, "-TriggerProbe", str(p))
        assert "R53_1_TRIGGER_CONTRACT_PROBLEMS" in out
        assert "kill the running worker" in out

    def test_validator_rejects_interactive_and_accepts_s4u(self):
        rej = _run_ps(VALIDATOR, "-PrincipalProbe", "Interactive")
        acc = _run_ps(VALIDATOR, "-PrincipalProbe", "S4U")
        assert "R53_1_PRINCIPAL_REJECTED" in rej
        assert "R53_1_PRINCIPAL_ACCEPTED" in acc

    def test_emission_task_fresh_install_and_unchanged(self, tmp_path):
        out = _run_ps(EM_INSTALLER, "-DecisionProbe", "ABSENT")
        assert "INSTALL" in out
        snap = {
            "TaskName": "PaperTrader-IntradayEmission",
            "Action": {
                "Execute": r"C:\Users\binis\paper_trader\.venv-win\Scripts"
                           r"\python.exe",
                "Arguments": '"C:\\Users\\binis\\paper_trader\\scripts'
                             '\\run_intraday_emission.py"',
                "WorkingDirectory": r"C:\Users\binis\paper_trader"},
            "Triggers": [
                {"Type": "MSFT_TaskDailyTrigger",
                 "StartBoundary": "2026-09-01T%s:00" % t, "Enabled": True}
                for t in ("10:00", "12:00", "14:00", "16:20")],
            "Principal": {"UserId": _user(), "LogonType": "S4U"},
            "Settings": {"StartWhenAvailable": False,
                         "MultipleInstances": "IgnoreNew",
                         "ExecutionTimeLimit": "PT30M"},
        }
        p = tmp_path / "em.json"
        p.write_text(json.dumps(snap), encoding="utf-8")
        out2 = _run_ps(EM_INSTALLER, "-DecisionProbe", str(p))
        assert "UNCHANGED" in out2

    def test_manager_delegates_to_the_one_definition_owner(self):
        src = (REPO / "scripts" / "manage_information_collection.ps1"
               ).read_text(encoding="utf-8", errors="replace")
        assert "install_information_collection_task.ps1" in src
        assert "New-ScheduledTaskTrigger -AtLogOn" not in src

    def test_research_code_touches_no_scheduler(self):
        for f in sorted((REPO / "alpha_agent" / "r53_1").glob("*.py")):
            src = f.read_text(encoding="utf-8", errors="replace")
            for token in ("Register-ScheduledTask", "Set-ScheduledTask",
                          "schtasks", "Unregister-ScheduledTask"):
                assert token not in src, "%s mentions %s" % (f.name, token)
        assert R.safety_block()["changes_scheduler"] is False

    def test_single_flight_and_dead_holder_recovery_unchanged(self):
        from paper_trader.api import information_collection as ic
        assert callable(ic.acquire_service_lock)
        assert callable(ic.acquire_service_lock_with_wait)
        assert ic.LOCK_TAKEOVER_SECONDS >= 300


# =========================================================================== #
# 2. Feed adapter honesty
# =========================================================================== #
def _fake_fetchers(received_iso: str, bars_by_sym: dict, daily: dict):
    def fetch_bars(syms):
        return (bars_by_sym, [],
                {"received_at_utc": received_iso, "provider": "test",
                 "interval_minutes": 5, "lookback_days": 1,
                 "timestamp_semantics": "test"})
    def fetch_daily(syms):
        return daily
    return fetch_bars, fetch_daily


def _bar(ts: str, end: str, o=100.0, c=100.0, v=1000):
    return {"ts_utc": ts, "bar_end_utc": end, "open": o, "high": max(o, c),
            "low": min(o, c), "close": c, "volume": v}


class TestFeedAdapterHonesty:
    def test_forming_bar_is_not_an_observation(self):
        received = "2026-09-01T16:02:00Z"
        bars = {"SPY": [
            _bar("2026-09-01T15:55:00Z", "2026-09-01T16:00:00Z"),
            _bar("2026-09-01T16:00:00Z", "2026-09-01T16:05:00Z")]}  # forming
        fb, fd = _fake_fetchers(received, bars, {"SPY": []})
        snap = FEED.build_snapshot(now_utc=_utc(2026, 9, 1, 16, 2),
                                   fetch_bars=fb, fetch_daily=fd)
        assert len(snap["bars"]["SPY"]) == 1
        assert snap["data_timestamp_utc"]["SPY"] == "2026-09-01T16:00:00Z"
        assert snap["freshness_seconds"]["SPY"] == pytest.approx(120.0)

    def test_latency_classification_is_honest(self):
        assert LANE._classify_delay(0.0) == LANE.LAT_REAL_TIME
        assert LANE._classify_delay(30.0) == LANE.LAT_NEAR_REAL_TIME
        assert LANE._classify_delay(900.0) == LANE.LAT_DELAYED_INTRADAY
        assert LANE._classify_delay(None) == LANE.LAT_NOT_ENTITLED
        assert set(LANE.LATENCY_CLASSES) >= {
            "REAL_TIME", "NEAR_REAL_TIME", "DELAYED_INTRADAY",
            "DAILY_ONLY", "NOT_ENTITLED"}

    def test_mark_fn_serves_only_future_prints(self):
        received = "2026-09-01T18:00:00Z"
        bars = {"SPY": [
            _bar("2026-09-01T16:00:00Z", "2026-09-01T16:05:00Z", o=101.0),
            _bar("2026-09-01T16:05:00Z", "2026-09-01T16:10:00Z", o=102.0)]}
        fb, fd = _fake_fetchers(received, bars, {"SPY": []})
        snap = FEED.build_snapshot(now_utc=_utc(2026, 9, 1, 18, 0),
                                   fetch_bars=fb, fetch_daily=fd)
        mark = FEED.make_mark_fn(snap)
        # first print at or after 16:01 is the 16:05 bar's open
        assert mark("SPY", "2026-09-01T16:01:00Z") == 102.0
        assert mark("SPY", "2026-09-01T16:00:00Z") == 101.0
        # session close (20:00Z) has not happened at received=18:00 -> None
        assert mark("SPY", "2026-09-01T20:00:00Z") is None

    def test_mark_fn_serves_session_close_after_the_session(self):
        received = "2026-09-01T21:00:00Z"
        bars = {"SPY": [
            _bar("2026-09-01T19:55:00Z", "2026-09-01T20:00:00Z", c=111.5)]}
        fb, fd = _fake_fetchers(received, bars, {"SPY": []})
        snap = FEED.build_snapshot(now_utc=_utc(2026, 9, 1, 21, 0),
                                   fetch_bars=fb, fetch_daily=fd)
        assert FEED.make_mark_fn(snap)("SPY", "2026-09-01T20:00:00Z") == 111.5


# =========================================================================== #
# 3. The frozen specs: implementations obey them, hashes unchanged
# =========================================================================== #
def _synthetic_snapshot(gap_sigma: float) -> dict:
    """A session whose open gaps `gap_sigma` daily sigmas from prior close,
    with 1% daily vol so the arithmetic is transparent."""
    daily = [{"date": "2026-08-%02d" % d, "close": 100.0 * (1.01 ** (d % 2))}
             for d in range(1, 30)]
    prior_close = daily[-1]["close"]
    sig = SIG._std if False else None  # noqa: F841 - clarity only
    # measured sigma of that alternating series:
    closes = [r["close"] for r in daily][-21:]
    rets = [closes[i] / closes[i - 1] - 1 for i in range(1, len(closes))]
    m = sum(rets) / len(rets)
    sigma = math.sqrt(sum((x - m) ** 2 for x in rets) / (len(rets) - 1))
    open_px = prior_close * (1.0 + gap_sigma * sigma)
    bars = [_bar("2026-09-01T13:30:00Z", "2026-09-01T13:35:00Z",
                 o=open_px, c=open_px)]
    return {"received_at_utc": "2026-09-01T13:40:00Z",
            "session_date_et": "2026-09-01",
            "session_close_utc": "2026-09-01T20:00:00Z",
            "bars": {"SPY": bars, "QQQ": bars, "IWM": bars},
            "bars_today": {"SPY": bars, "QQQ": bars, "IWM": bars},
            "daily_closes": {"SPY": daily, "QQQ": daily, "IWM": daily},
            "freshness_seconds": {s: 300.0 for s in ("SPY", "QQQ", "IWM")},
            "data_timestamp_utc": {s: "2026-09-01T13:35:00Z"
                                   for s in ("SPY", "QQQ", "IWM")}}


class TestFrozenSpecObedience:
    def test_gap_cells_partition_the_domain(self):
        cont = IF.spec_by_id("r53i_open_gap_continuation")
        rev = IF.spec_by_id("r53i_open_gap_reversal")
        slot = {"slot_utc": "X"}
        big = _synthetic_snapshot(gap_sigma=1.2)
        small = _synthetic_snapshot(gap_sigma=0.2)
        assert SIG.gap_continuation(cont, slot, big) and \
            not SIG.gap_reversal(rev, slot, big)
        assert SIG.gap_reversal(rev, slot, small) and \
            not SIG.gap_continuation(cont, slot, small)
        # direction: continuation follows the gap, reversal opposes it
        assert SIG.gap_continuation(cont, slot, big)[0]["direction"] == 1
        assert SIG.gap_reversal(rev, slot, small)[0]["direction"] == -1

    def test_signal_timestamps_never_exceed_the_information_set(self):
        snap = _synthetic_snapshot(1.0)
        for row in SIG.gap_continuation(
                IF.spec_by_id("r53i_open_gap_continuation"),
                {"slot_utc": "X"}, snap):
            assert row["data_timestamp_utc"] <= snap["received_at_utc"]

    def test_spec_hashes_match_the_frozen_record(self):
        art = R.read_json(
            Path(r"D:\Stock_Prediction_app_data"
                 r"\active_risk_intraday_alpha_r53"
                 r"\r53_active_risk_intraday_alpha_v1"
                 r"\R53_INTRADAY_FACTORY.json"), default=None)
        if not art:
            pytest.skip("frozen factory artifact not on this machine")
        recorded = {s["challenger_id"]: s["spec_hash"] for s in art["specs"]}
        for spec in IF.INTRADAY_SPECS:
            assert IF.spec_hash(spec) == recorded[spec["challenger_id"]], \
                "spec %s was retuned after freezing" % spec["challenger_id"]

    def test_owner_map_covers_every_frozen_spec(self):
        for spec in IF.INTRADAY_SPECS:
            assert spec["signal_owner"] in SIG._OWNERS


# =========================================================================== #
# 4. Prospective discipline through the REAL signal path
# =========================================================================== #
@pytest.fixture()
def tmp_ledger(tmp_path, monkeypatch):
    import alpha_agent.r53 as r53pkg
    monkeypatch.setattr(r53pkg, "RESEARCH_ROOT", tmp_path)
    import alpha_agent.r53.intraday_factory as fac
    monkeypatch.setattr(fac, "research_dir",
                        lambda: (tmp_path / "c").mkdir(parents=True,
                                                       exist_ok=True)
                        or (tmp_path / "c"))
    return tmp_path


class TestProspectiveDisciplineThroughAdapter:
    def test_emission_appends_true_forward_and_dedupes(self, tmp_ledger):
        snap = _synthetic_snapshot(1.2)
        # 14:05 UTC = 10:05 ET -> inside the 10:00 slot's grace
        now = _utc(2026, 9, 1, 14, 5)
        snap["data_timestamp_utc"] = {s: "2026-09-01T14:00:00Z"
                                      for s in ("SPY", "QQQ", "IWM")}
        lane = {"state": IF.LANE_AVAILABLE}
        out = IF.emit_due(now_utc=now, lane=lane,
                          signal_fn=SIG.make_signal_fn(snap),
                          session_close_utc=snap["session_close_utc"])
        assert out["state"] == IF.EMIT_OK and out["n_appended"] > 0
        for row in IF.predictions():
            assert row["forward_evidence_type"] == "TRUE_FORWARD"
            assert row["emitted_at_utc"] < row["outcome_window_start_utc"]
            assert row["data_timestamp_utc"] <= row["emitted_at_utc"]
        again = IF.emit_due(now_utc=now, lane=lane,
                            signal_fn=SIG.make_signal_fn(snap),
                            session_close_utc=snap["session_close_utc"])
        assert again["n_appended"] == 0
        assert again["n_duplicates_skipped"] > 0

    def test_stale_information_is_refused(self, tmp_ledger):
        snap = _synthetic_snapshot(1.2)
        snap["data_timestamp_utc"] = {s: "2026-09-01T13:35:00Z"
                                      for s in ("SPY", "QQQ", "IWM")}
        now = _utc(2026, 9, 1, 14, 5)     # 30 minutes after the data
        with pytest.raises(IF.LedgerRefusal):
            IF.emit_due(now_utc=now, lane={"state": IF.LANE_AVAILABLE},
                        signal_fn=SIG.make_signal_fn(snap),
                        session_close_utc=snap["session_close_utc"])

    def test_no_emission_outside_a_slot(self, tmp_ledger):
        snap = _synthetic_snapshot(1.2)
        out = IF.emit_due(now_utc=_utc(2026, 9, 1, 15, 0),
                          lane={"state": IF.LANE_AVAILABLE},
                          signal_fn=SIG.make_signal_fn(snap),
                          session_close_utc=snap["session_close_utc"])
        assert out["state"] == IF.EMIT_NOT_A_SLOT


# =========================================================================== #
# 5. Risk budget: sees diversification, conserves NAV, mutates nothing
# =========================================================================== #
def _aligned(rho_target: float, n: int = 120) -> dict:
    import random
    rnd = random.Random(7)
    book = [rnd.gauss(0, 0.01) for _ in range(n)]
    other = [rho_target * b + math.sqrt(1 - rho_target ** 2)
             * rnd.gauss(0, 0.01) for b in book]
    return {"dates": ["d%d" % i for i in range(n)],
            "series": {"&ES": book, "&X": other}}


class TestRiskBudgetShadow:
    def _eval(self, monkeypatch, rho, unit=9000.0, sigma_scale=1.0):
        aligned = _aligned(rho)
        if sigma_scale != 1.0:
            aligned["series"]["&X"] = [x * sigma_scale
                                       for x in aligned["series"]["&X"]]
        monkeypatch.setattr(RB, "probe_contract", lambda s: {
            "symbol": s, "owned": True, "unit_notional_usd": unit,
            "initial_margin_per_unit": 1000.0, "median_volume_21d": 1e5})
        return RB.evaluate_candidate(
            sleeve_id="sleeve_test", symbol="&X", nav=99000.0,
            aligned=aligned, strength={"state": "OK", "percentile": 0.8})

    def test_diversification_changes_the_shadow_answer(self, monkeypatch):
        low = self._eval(monkeypatch, rho=0.0)
        high = self._eval(monkeypatch, rho=0.95)
        pol = "MODERATE_ACTIVE_POLICY"
        assert low["policies"][pol]["delta_sigma"] < \
            high["policies"][pol]["delta_sigma"]
        assert low["policies"][pol]["diversification_benefit_sigma"] > \
            high["policies"][pol]["diversification_benefit_sigma"]

    def test_whole_units_and_weights_conserve_nav(self, monkeypatch):
        row = self._eval(monkeypatch, rho=0.3, unit=9000.0)
        for pol_name, pol in RB.SHADOW_BUDGET_POLICIES.items():
            v = row["policies"][pol_name]
            assert v["whole_units"] == int(
                (pol["unit_weight_cap"] * 99000.0) // 9000.0)
            assert v["achievable_weight"] == pytest.approx(
                v["whole_units"] * 9000.0 / 99000.0, abs=1e-4)
            assert v["achievable_weight"] <= pol["unit_weight_cap"] + 1e-9

    def test_granularity_blocks_an_oversized_unit(self, monkeypatch):
        row = self._eval(monkeypatch, rho=0.0, unit=50000.0)
        v = row["policies"]["CURRENT_CONSERVATIVE_POLICY"]
        assert v["verdict"] == "BLOCKED_BY_UNIT_GRANULARITY"
        assert v["min_nav_for_one_unit"] == pytest.approx(500000.0)

    def test_vol_budget_can_block_what_the_cap_allows(self, monkeypatch):
        row = self._eval(monkeypatch, rho=0.9, unit=9000.0, sigma_scale=8.0)
        v = row["policies"]["CURRENT_CONSERVATIVE_POLICY"]
        assert v["verdict"] == "BLOCKED_BY_VOL_BUDGET"

    def test_collateral_is_respected(self, monkeypatch):
        aligned = _aligned(0.2)
        monkeypatch.setattr(RB, "probe_contract", lambda s: {
            "symbol": s, "owned": True, "unit_notional_usd": 9000.0,
            "initial_margin_per_unit": 200000.0})
        row = RB.evaluate_candidate(
            sleeve_id="sleeve_test", symbol="&X", nav=99000.0,
            aligned=aligned, strength={"state": "OK", "percentile": 0.8})
        v = row["policies"]["CURRENT_CONSERVATIVE_POLICY"]
        assert v["budget_checks"]["collateral"] is False

    def test_production_policy_is_untouched(self):
        from engine import constrained_reallocation as CRE
        pol = CRE.default_policy()
        assert pol["min_switching_net_improvement"] == pytest.approx(0.05)
        assert pol["max_name_weight"] == pytest.approx(0.10)
        assert pol["max_one_way_turnover"] == pytest.approx(0.35)
        assert pol["max_gross_exposure"] == pytest.approx(1.0)
        assert pol["max_net_exposure"] == pytest.approx(1.0)


# =========================================================================== #
# 6. Executable universe: sizing arithmetic and classification honesty
# =========================================================================== #
class TestExecutableUniverse:
    def test_feasibility_arithmetic(self):
        contract = {"symbol": "&T", "unit_notional_usd": 14800.0,
                    "initial_margin_per_unit": 900.0}
        row = EU.feasibility_row(contract, nav=99000.0)
        assert row["min_nav_under_cap_10"] == pytest.approx(148000.0)
        assert row["executable_under_cap_10"] is False
        assert row["executable_under_cap_15"] is True
        assert row["margin_executable"] is True
        assert row["state"] == "EXECUTABLE_ONLY_UNDER_WIDER_CAP"

    def test_contract_multiplier_is_read_not_guessed(self):
        c = {"symbol": "&T", "unit_notional_usd": None}
        assert EU.feasibility_row(c, 99000.0)["state"] == \
            "NOT_PRICEABLE_FROM_OWNED_DATA"

    def test_met_is_micro_ether_not_an_equity_index(self):
        from api import market_reference_data as mrd
        from engine import instrument_contract as ic
        assert mrd.FUTURES_ASSET_CLASS_BY_ROOT["MET"] == ic.AC_CRYPTO_FUTURES
        assert mrd.FUTURES_ASSET_CLASS_BY_ROOT["MBT"] == ic.AC_CRYPTO_FUTURES

    def test_proxy_vocabulary_is_closed(self):
        allowed = {"SAME_THESIS_SAME_MARKET", "PROXY_WITH_BASIS_RISK",
                   "NOT_EQUIVALENT"}
        for p in EU.ETF_PROXIES:
            assert p["classification"] in allowed


# =========================================================================== #
# 7. Short capability: assessment only, nothing activated
# =========================================================================== #
class TestShortCapability:
    def test_assessment_activates_nothing(self):
        a = SC.assessment()
        assert a["activation_state"].startswith("NOT_ACTIVATED")
        assert a["futures_vs_equity_short"]["never_conflate"] is True
        assert "engine.cross_asset_risk" in str(
            a["already_supports_signed_exposure"])

    def test_long_only_wall_is_where_the_assessment_says(self):
        from engine import constrained_reallocation as CRE
        assert CRE.C_LONG_ONLY == "LONG_ONLY"
        src = (REPO / "engine" / "constrained_reallocation.py").read_text(
            encoding="utf-8", errors="replace")
        assert "Negative weight is clipped to zero" in src


# =========================================================================== #
# 8. Release-wide safety and canonical-chain integrity
# =========================================================================== #
class TestReleaseSafety:
    def test_safety_block_flags(self):
        sb = R.safety_block()
        for flag in ("mutates_production_policy", "creates_order",
                     "creates_fill", "promotes_model", "activates_sleeve",
                     "changes_scheduler", "may_spend_money",
                     "backfills_predictions", "writes_operational_store",
                     "second_allocator_created",
                     "second_forward_evidence_system_created",
                     "activates_short_exposure"):
            assert sb[flag] is False, flag

    def test_canonical_chain_is_importable_with_no_second_manager(self):
        v = CR.verify_chain()
        assert v["all_importable"] is True
        assert v["second_portfolio_manager"] is False

    def test_r46_ledgers_remain_intact(self):
        from alpha_agent.r46 import ledger as R46L
        rep = R46L.verify()
        assert all(r.get("intact") for r in rep.get("ledgers", [])) or \
            rep.get("all_intact", False)

    def test_intraday_ledgers_are_chain_intact(self):
        rep = IF.verify()
        assert rep["all_intact"] is True

    def test_market_data_owner_reports_never_asserts_freshness(self):
        src = (REPO / "engine" / "market_data.py").read_text(
            encoding="utf-8", errors="replace")
        assert "fetch_current_session_bars" in src
        assert "this function reports, it never" in " ".join(src.split())
        assert "asserts freshness" in " ".join(src.split())
