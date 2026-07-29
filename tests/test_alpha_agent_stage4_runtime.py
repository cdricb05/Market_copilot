"""
tests/test_alpha_agent_stage4_runtime.py — Alpha Agent Stage 4 runtime.

Deterministic coverage of the persistent Windows research runtime: lock
lifecycle + stale recovery + active refusal, the collect/research/report-only/
watchdog/verify modes, no-new-input and provider-unavailable degraded reports,
the two-cycle/day cap, email idempotency + retry + redaction + outbox
transitions, deterministic HTML/plain-text + escaping + KPI reconciliation,
read-only paper-book context + operational-ledger immutability, Task Scheduler
command generation, verify-mode read-only guarantee, required output files,
deterministic run ids, heartbeat, retention safety and the hard safety
invariants (no PostgreSQL / order / fill / signal / decision / model promotion /
Daily Close). Every stage runner, email sender, clock and recovery launcher is a
FAKE — no real network, LLM, email or scheduled task is ever touched.
"""
from __future__ import annotations

import base64
import contextlib
import importlib.util
import io
import json
import sqlite3
import sys
import urllib.error
from datetime import datetime
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO.parent) not in sys.path:
    sys.path.insert(0, str(_REPO.parent))

from paper_trader.alpha_agent import report_renderer as rr  # noqa: E402
from paper_trader.alpha_agent import runtime as rt  # noqa: E402
from paper_trader.alpha_agent import runtime_contracts as rc  # noqa: E402


# --------------------------------------------------------------------------- #
# Fixtures + fakes.
# --------------------------------------------------------------------------- #
def _w(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj), encoding="utf-8")


def _make_ledger(root: Path) -> None:
    _w(root / "forward_performance.json", {"rows": [
        {"row": {"date": "2026-07-27", "nav": 98960.36, "cash": 4630.31,
                 "daily_return_pct": -0.3995, "cumulative_return_pct": -1.0396,
                 "drawdown_pct": -1.0396, "benchmark_close": 739.09,
                 "benchmark_cumulative_return_pct": -1.1132}},
        {"row": {"date": "2026-07-28", "nav": 98568.68, "cash": 4630.31,
                 "daily_return_pct": -0.3958, "cumulative_return_pct": -1.4313,
                 "drawdown_pct": -1.4313, "benchmark_close": 740.86,
                 "benchmark_cumulative_return_pct": -0.8764}}]})
    _w(root / "daily_close_journal.json", {"rows": [
        {"gate_outcome": "NO_ACTION_TODAY", "daily_pnl": -391.68,
         "cumulative_pnl": -1431.32,
         "checks_summary_line": "13 checks completed · 0 triggered · 0 unavailable"}]})
    _w(root / "daily_close_progress.json",
       {"final_close_status": "DAILY_CLOSE_COMPLETE_HOLD", "running": False,
        "done": True, "evaluation_date": "2026-07-28"})
    _w(root / "forward_prediction_outcomes.json", {"rows": [
        {"status": "MATURED"}, {"status": "MATURED"}, {"status": "PENDING"}]})


def _make_stage_root(base: Path, name: str, latest: dict) -> Path:
    d = base / name
    run_dir = d / "runs" / ("%s_run" % name)
    run_dir.mkdir(parents=True, exist_ok=True)
    _w(run_dir / "run_manifest.json", {"run_id": "%s_run" % name})
    payload = {"run_id": "%s_run" % name, "run_dir": "runs/%s_run" % name}
    payload.update(latest)
    _w(d / "latest.json", payload)
    return d


@pytest.fixture
def env(tmp_path):
    runtime_root = tmp_path / "runtime"
    ledger = tmp_path / "ledger"
    ledger.mkdir()
    _make_ledger(ledger)
    _make_stage_root(tmp_path, "registry", {"counts": {}})
    _make_stage_root(tmp_path, "ingestion", {"counts": {"raw_objects_new": 1}})
    _make_stage_root(tmp_path, "director",
                     {"counts": {"analyses": 6, "hypotheses": 0},
                      "provider": "claude_code",
                      "provider_recorded_as": "CLAUDE_CODE_DEVELOPMENT_ONLY"})
    _make_stage_root(tmp_path, "news_rss",
                     {"counts": {"normalized_records_new": 3,
                                 "normalized_records_total": 103,
                                 "clusters": 769},
                      "enabled_feeds": 11, "healthy_feeds": 7, "as_of":
                      "2026-07-29", "status": "ALPHA_AGENT_STAGE3_5_PARTIAL",
                      "terminal_token": "ALPHA_AGENT_STAGE3_5_PARTIAL — 7/11 "
                      "feeds healthy; unavailable: cisa_advisories(HTTP_403); "
                      "doj_opa_news(HTTP_404)"})
    cfg = {
        "stage": "4",
        "runtime_root": str(runtime_root),
        "recipient_email": "binisti@gmail.com",
        "stage1_registry_root": str(tmp_path / "registry"),
        "stage2_ingestion_root": str(tmp_path / "ingestion"),
        "stage3_director_root": str(tmp_path / "director"),
        "stage3_5_news_rss_root": str(tmp_path / "news_rss"),
        "operational_ledger_roots": [str(ledger)],
        "cadence": {"max_research_cycles_per_day": 2, "stale_lock_seconds": 900,
                    "heartbeat_stale_seconds": 7200},
        "provider_order": ["claude_code", "anthropic_http"],
        "allowed_task_names": list(rc.ALPHA_AGENT_TASK_NAMES),
        "email": {"credential_dir": str(tmp_path / "no_creds"),
                  "refresh_token_file": "gmail_oauth_refresh_token.dpapi",
                  "delivery_provider": "gmail_api_oauth"},
    }
    return {"cfg": cfg, "root": runtime_root, "ledger": ledger,
            "base": tmp_path}


def _norm(component, *, status="OK", ok=True, verified=False, no_new=False,
          run_id=None, run_dir=None, counts=None, metrics=None, raw=None):
    return {"component": component, "status": status, "terminal": status,
            "ok": ok, "verified": verified, "no_new": no_new, "run_id": run_id,
            "run_dir": run_dir, "counts": counts or {}, "metrics": metrics or {},
            "raw": raw or {}}


class FakeDrivers(rt.StageDrivers):
    def __init__(self, research=None):
        self.calls = []
        self.research_calls = 0
        self._research = research

    def verify_stage1(self):
        self.calls.append("verify_stage1")
        return _norm(rc.COMPONENT_STAGE1, status="ALPHA_AGENT_STAGE1_VERIFIED",
                     verified=True)

    def collect_stage2(self, mode):
        self.calls.append(("collect_stage2", mode))
        return _norm(rc.COMPONENT_STAGE2, status="ALPHA_AGENT_STAGE2_READY",
                     counts={"raw_objects_new": 1}, run_id="stage2_run")

    def verify_stage2(self):
        self.calls.append("verify_stage2")
        return _norm(rc.COMPONENT_STAGE2, status="ALPHA_AGENT_STAGE2_VERIFIED",
                     verified=True)

    def collect_stage35(self, mode):
        self.calls.append(("collect_stage35", mode))
        return _norm(rc.COMPONENT_STAGE35,
                     status="ALPHA_AGENT_STAGE3_5_PARTIAL",
                     counts={"normalized_records_new": 3}, run_id="stage35_run")

    def verify_stage35(self):
        self.calls.append("verify_stage35")
        return _norm(rc.COMPONENT_STAGE35,
                     status="ALPHA_AGENT_STAGE3_5_VERIFIED", verified=True)

    def research_stage3(self, mode):
        self.calls.append(("research_stage3", mode))
        self.research_calls += 1
        if self._research is not None:
            return dict(self._research)
        return _norm(rc.COMPONENT_STAGE3, status="ALPHA_AGENT_STAGE3_DEV_READY",
                     counts={"analyses": 6, "hypotheses": 2, "llm_calls": 2,
                             "records_selected": 6, "queue_entries": 1},
                     run_id="stage3_run",
                     raw={"provider": "claude_code",
                          "provider_recorded_as":
                          "CLAUDE_CODE_DEVELOPMENT_ONLY",
                          "stage1_run_id": "stage1_run",
                          "stage2_run_id": "stage2_run"})

    def verify_stage3(self):
        self.calls.append("verify_stage3")
        return _norm(rc.COMPONENT_STAGE3, status="ALPHA_AGENT_STAGE3_VERIFIED",
                     verified=True)


class FakeSender:
    def __init__(self, status=rc.EMAIL_SENT, error=None, message_id=None,
                 diagnostic=None):
        self.status = status
        self.error = error
        self.message_id = message_id
        self.diagnostic = diagnostic
        self.calls = []

    def __call__(self, job):
        self.calls.append(dict(job))
        return {"status": self.status, "error": self.error,
                "message_id": self.message_id, "diagnostic": self.diagnostic}


def _clock(hour=18, minute=30, second=0, day=29):
    return rt.FixedClock(datetime(2026, 7, day, hour, minute, second))


def _runtime(env, *, drivers=None, sender=None, clock=None,
             recovery_launcher=None):
    return rt.Runtime(env["cfg"], drivers=drivers or FakeDrivers(),
                      email_sender=sender if sender is not None
                      else FakeSender(),
                      clock=clock or _clock(),
                      recovery_launcher=recovery_launcher)


def _fingerprint(root: Path) -> dict:
    return {str(p): p.read_bytes()
            for p in sorted(root.rglob("*")) if p.is_file()}


# --------------------------------------------------------------------------- #
# Contracts + config.
# --------------------------------------------------------------------------- #
def test_real_config_loads_and_is_secret_free():
    cfg = rc.load_config(_REPO / "configs" / "alpha_agent" /
                         "stage4_runtime.json")
    assert cfg["stage"] == "4"
    assert sorted(cfg["allowed_task_names"]) == sorted(rc.ALPHA_AGENT_TASK_NAMES)
    assert rc.scan_for_secrets(cfg) == []


def test_config_rejects_embedded_secret(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"stage": "4", "api_key":
                               "sk-ant-abcdef0123456789ABCDEF"}),
                   encoding="utf-8")
    with pytest.raises(rc.ConfigError):
        rc.load_config(bad)


def test_config_requires_exact_task_names(tmp_path):
    p = tmp_path / "c.json"
    base = {k: "x" for k in ("stage", "runtime_root", "recipient_email",
                             "stage1_registry_root", "stage2_ingestion_root",
                             "stage3_director_root", "stage3_5_news_rss_root")}
    base["stage"] = "4"
    base["operational_ledger_roots"] = []
    base["cadence"] = {}
    base["provider_order"] = []
    base["allowed_task_names"] = ["AlphaAgent-Collect"]
    p.write_text(json.dumps(base), encoding="utf-8")
    with pytest.raises(rc.ConfigError):
        rc.load_config(p)


def test_deterministic_ids():
    assert rc.report_cycle_id("morning", "2026-07-29") == \
        rc.report_cycle_id("morning", "2026-07-29")
    assert rc.report_cycle_id("morning", "2026-07-29") != \
        rc.report_cycle_id("post_close", "2026-07-29")
    a = rc.runtime_run_id("cyc", "research", "fp")
    assert a == rc.runtime_run_id("cyc", "research", "fp")


# --------------------------------------------------------------------------- #
# Locks.
# --------------------------------------------------------------------------- #
def test_lock_acquire_and_release(env):
    rt.ensure_layout(env["root"])
    conn = rt.open_state_db(env["root"])
    clock = _clock()
    h = rt.acquire_lock(env["root"], "collect", "run1", clock=clock, conn=conn)
    assert h.path.exists()
    rt.release_lock(h, clock=clock, conn=conn)
    assert not h.path.exists()
    conn.close()


def test_active_lock_refused(env):
    rt.ensure_layout(env["root"])
    conn = rt.open_state_db(env["root"])
    clock = _clock()
    rt.acquire_lock(env["root"], "collect", "run1", clock=clock, conn=conn)
    with pytest.raises(rt.LockHeld):
        rt.acquire_lock(env["root"], "collect", "run2", clock=clock, conn=conn)
    conn.close()


def test_stale_lock_recovered(env):
    rt.ensure_layout(env["root"])
    conn = rt.open_state_db(env["root"])
    old = rt.FixedClock(datetime(2026, 7, 29, 10, 0, 0))
    rt.acquire_lock(env["root"], "collect", "old_run", clock=old, conn=conn,
                    stale_seconds=900)
    later = rt.FixedClock(datetime(2026, 7, 29, 18, 0, 0))
    h = rt.acquire_lock(env["root"], "collect", "new_run", clock=later,
                        conn=conn, stale_seconds=900)
    assert h.stale_cleared is True
    n = conn.execute("SELECT COUNT(*) FROM recovery_actions WHERE"
                     " action='CLEAR_STALE_LOCK'").fetchone()[0]
    assert n >= 1
    conn.close()


def test_collect_refused_when_locked(env):
    rt.ensure_layout(env["root"])
    conn = rt.open_state_db(env["root"])
    rt.acquire_lock(env["root"], "collect", "holder", clock=_clock(), conn=conn)
    conn.close()
    res = _runtime(env).run_collect()
    assert res.terminal == rc.BLOCKED
    assert res.status == "COLLECT_REFUSED"


# --------------------------------------------------------------------------- #
# Collect.
# --------------------------------------------------------------------------- #
def test_collect_sequence_and_success(env):
    d = FakeDrivers()
    res = _runtime(env, drivers=d).run_collect()
    assert res.terminal == rc.READY
    assert res.status == "COLLECT_OK"
    # Ordered: verify1, collect2, verify2, collect35, verify35.
    assert d.calls == ["verify_stage1", ("collect_stage2", "incremental"),
                       "verify_stage2", ("collect_stage35", "incremental"),
                       "verify_stage35"]
    # No LLM/email in collect.
    assert res.email_status is None


def test_collect_writes_required_run_files(env):
    res = _runtime(env).run_collect()
    run_dir = Path(res.run_dir)
    for name in rt.REQUIRED_RUN_FILES:
        assert (run_dir / name).exists(), name


def test_collect_heartbeat_updated(env):
    _runtime(env).run_collect()
    hb = rt.read_heartbeat(env["root"])
    assert hb["mode"] == "collect"
    assert hb["status"] in ("COLLECT_OK", "COLLECT_DEGRADED")


# --------------------------------------------------------------------------- #
# Research + report.
# --------------------------------------------------------------------------- #
def test_research_sequence_and_report(env):
    d = FakeDrivers()
    res = _runtime(env, drivers=d).run_research(label="post_close")
    assert res.terminal == rc.READY
    assert ("research_stage3", "incremental") in d.calls
    assert Path(res.detail["report_html"]).exists()
    assert Path(res.detail["report_text"]).exists()


def test_no_new_input_report(env):
    d = FakeDrivers(research=_norm(rc.COMPONENT_STAGE3, no_new=True,
                                   status="NO_NEW_DIRECTOR_INPUT"))
    res = _runtime(env, drivers=d).run_research(label="morning")
    assert res.terminal == rc.NO_NEW_RESEARCH_INPUT
    man = json.loads(Path(res.detail["report_html"]).with_name(
        "report_manifest.json").read_text(encoding="utf-8"))
    assert man["email_llm_tokens"] == 0


def test_provider_unavailable_degraded(env):
    d = FakeDrivers(research=_norm(rc.COMPONENT_STAGE3, ok=False,
                                   status="ALPHA_AGENT_STAGE3_BLOCKED"))
    res = _runtime(env, drivers=d).run_research(label="post_close")
    assert res.terminal == rc.DEGRADED
    html = Path(res.detail["report_html"]).read_text(encoding="utf-8")
    assert "ACTION REQUIRED" in html
    assert "LLM SKIPPED" in html


def test_max_two_research_cycles_per_day(env):
    d = FakeDrivers()
    r = _runtime(env, drivers=d)
    r.run_research(label="morning")
    r.run_research(label="post_close")
    third = r.run_research(label="manual")
    assert d.research_calls == 2  # third cycle skipped the LLM
    assert "cap reached" in (third.detail.get("llm_skipped_reason") or "")


def test_idempotent_no_duplicate_email(env):
    sender = FakeSender(rc.EMAIL_SENT)
    r = _runtime(env, sender=sender)
    r.run_research(label="morning")
    r.run_research(label="morning")
    assert len(sender.calls) == 1  # second cycle deduped
    conn = rt.open_state_db(env["root"])
    sent = conn.execute("SELECT COUNT(*) FROM email_deliveries WHERE"
                        " status='EMAIL_SENT'").fetchone()[0]
    conn.close()
    assert sent == 1


def test_email_credential_absent(env):
    # Real sender, no credential dir -> credential-required, no subprocess.
    r = rt.Runtime(env["cfg"], drivers=FakeDrivers(), email_sender=None,
                   clock=_clock())
    res = r.run_research(label="post_close")
    assert res.terminal == rc.EMAIL_CREDENTIAL_REQUIRED
    assert res.email_status == rc.EMAIL_CREDENTIAL_REQUIRED_STATUS


def test_email_outbox_transitions_sent(env):
    r = _runtime(env, sender=FakeSender(rc.EMAIL_SENT))
    res = r.run_research(label="morning")
    cyc = res.cycle_id
    assert (env["root"] / "outbox" / "sent" / ("%s.json" % cyc)).exists()
    assert not (env["root"] / "outbox" / "pending" / ("%s.json" % cyc)).exists()


def test_email_outbox_transitions_failed(env):
    r = _runtime(env, sender=FakeSender(rc.EMAIL_RETRYABLE_FAILURE))
    res = r.run_research(label="morning")
    cyc = res.cycle_id
    assert (env["root"] / "outbox" / "failed" / ("%s.json" % cyc)).exists()


def test_email_error_redacted(env):
    secret_err = "auth failed sk-ant-abcdef0123456789ABCDEF please"
    r = _runtime(env, sender=FakeSender(rc.EMAIL_RETRYABLE_FAILURE,
                                        error=secret_err))
    res = r.run_research(label="morning")
    conn = rt.open_state_db(env["root"])
    err = conn.execute("SELECT last_error FROM email_deliveries WHERE"
                       " cycle_id=?", (res.cycle_id,)).fetchone()[0]
    conn.close()
    assert "sk-ant-" not in err
    assert "REDACTED" in err


def test_email_retry_then_success(env):
    r1 = _runtime(env, sender=FakeSender(rc.EMAIL_RETRYABLE_FAILURE),
                  clock=_clock(minute=30))
    r1.run_research(label="morning")
    r2 = _runtime(env, sender=FakeSender(rc.EMAIL_SENT), clock=_clock(minute=31))
    res2 = r2.run_research(label="morning")
    assert res2.email_status == rc.EMAIL_SENT
    conn = rt.open_state_db(env["root"])
    rows = conn.execute("SELECT status FROM email_deliveries WHERE cycle_id=?"
                        " ORDER BY id", (res2.cycle_id,)).fetchall()
    conn.close()
    assert rows[0][0] == rc.EMAIL_RETRYABLE_FAILURE
    assert any(r[0] == rc.EMAIL_SENT for r in rows)


# --------------------------------------------------------------------------- #
# Deterministic rendering + reconciliation.
# --------------------------------------------------------------------------- #
def test_deterministic_html_and_text(env):
    d = FakeDrivers()
    r1 = _runtime(env, drivers=d).run_research(label="morning")
    h1 = Path(r1.detail["report_html"]).read_text(encoding="utf-8")
    t1 = Path(r1.detail["report_text"]).read_text(encoding="utf-8")
    # Re-render the identical model deterministically.
    r2 = _runtime(env, drivers=FakeDrivers()).run_research(label="morning")
    h2 = Path(r2.detail["report_html"]).read_text(encoding="utf-8")
    t2 = Path(r2.detail["report_text"]).read_text(encoding="utf-8")
    assert h1 == h2
    assert t1 == t2


def test_html_escaping():
    model = {"cycle_label": "morning", "cycle_date": "2026-07-29",
             "subject": "S", "generated_at": "t", "badges": [],
             "executive_summary": "x", "kpis": {},
             "material_events": [{"summary": "<script>alert(1)</script>",
                                  "entity": "A&B", "materiality": "high",
                                  "source_ids": ["r<1"]}],
             "research": {"no_new_action": True, "notes": ""},
             "paper_book": {}, "news_rss": {"failed_feeds": []},
             "llm": {}, "operating_action": "", "evidence": {}}
    html = rr.render_html(model)
    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;" in html
    assert "A&amp;B" in html


def test_bounded_event_summaries():
    # Real Stage 3 schema nests fields under an "analysis" wrapper.
    run = {"events": [{"analysis": {
        "factual_summary": "z" * 5000, "affected_tickers": ["AAPL", "MSFT"],
        "materiality": "IMMATERIAL", "economic_mechanism": "m" * 5000,
        "source_record_ids": ["rec_1", "rec_2"]}}],
        "hypotheses": [{"proposal": {"economic_rationale": "e" * 5000}}]}
    evs = rt._material_events_from_run(run)
    assert len(evs[0]["summary"]) <= 300
    assert len(evs[0]["mechanism"]) <= 240
    assert evs[0]["entity"] == "AAPL, MSFT"
    assert evs[0]["materiality"] == "IMMATERIAL"
    assert evs[0]["source_ids"] == ["rec_1", "rec_2"]
    hyps = rt._hypotheses_text(run)
    assert hyps and len(hyps[0]) <= 280
    assert hyps[0].startswith("e")


def test_kpi_reconciliation(env):
    d = FakeDrivers()
    res = _runtime(env, drivers=d).run_research(label="post_close")
    man = json.loads(Path(res.detail["report_html"]).with_name(
        "report_manifest.json").read_text(encoding="utf-8"))
    k = man["kpis"]
    assert k["accepted_analyses"] == 6
    assert k["hypotheses"] == 2
    assert "7/11" in k["feed_health"]


def test_paper_book_context_read_only(env):
    before = _fingerprint(env["ledger"])
    res = _runtime(env).run_research(label="morning")
    after = _fingerprint(env["ledger"])
    assert before == after  # ledgers never mutated
    pc = json.loads((Path(res.run_dir) / "portfolio_context.json").read_text(
        encoding="utf-8"))
    assert pc["nav"] == 98568.68
    assert pc["daily_pnl"] == -391.68
    assert pc["matured_evidence"] == 2
    assert pc["pending_evidence"] == 1
    # SPY daily return computed from consecutive benchmark closes.
    assert pc["spy_daily_pct"] == pytest.approx(
        (740.86 / 739.09 - 1) * 100, rel=1e-3)


def test_operational_ledger_immutability_collect(env):
    before = _fingerprint(env["ledger"])
    _runtime(env).run_collect()
    assert _fingerprint(env["ledger"]) == before


# --------------------------------------------------------------------------- #
# Watchdog.
# --------------------------------------------------------------------------- #
def test_watchdog_detects_missed_reports(env):
    res = _runtime(env, clock=_clock(hour=19)).run_watchdog()
    assert set(res.detail["checks"]["missed_reports"]) == {"morning",
                                                           "post_close"}
    assert res.terminal == rc.DEGRADED
    conn = rt.open_state_db(env["root"])
    n = conn.execute("SELECT COUNT(*) FROM missed_cycles").fetchone()[0]
    conn.close()
    assert n == 2


def test_watchdog_no_duplicate_when_already_sent(env):
    # Morning already reported+sent earlier today.
    r = _runtime(env, sender=FakeSender(rc.EMAIL_SENT),
                 clock=_clock(hour=9))
    r.run_research(label="morning")
    calls = []
    wr = _runtime(env, clock=_clock(hour=19),
                  recovery_launcher=lambda lb: calls.append(lb))
    # enable launching so we can prove morning is NOT relaunched
    wr.cfg = dict(env["cfg"])
    wr.cfg["cadence"] = dict(env["cfg"]["cadence"])
    wr.cfg["cadence"]["watchdog_launch_missed_reports"] = True
    res = wr.run_watchdog()
    assert "morning" not in res.detail["checks"]["missed_reports"]
    assert "morning" not in calls
    assert "post_close" in calls  # only the genuinely-missed one launched


def test_watchdog_clears_stale_lock(env):
    rt.ensure_layout(env["root"])
    conn = rt.open_state_db(env["root"])
    old = rt.FixedClock(datetime(2026, 7, 29, 8, 0, 0))
    rt.acquire_lock(env["root"], "research", "stuck", clock=old, conn=conn,
                    stale_seconds=900)
    conn.close()
    res = _runtime(env, clock=_clock(hour=18)).run_watchdog()
    assert not (env["root"] / "locks" / "research.lock").exists()
    assert any(r["action"] == "CLEAR_STALE_LOCK"
               for r in res.detail["recoveries"])


def test_watchdog_retries_failed_email(env):
    r1 = _runtime(env, sender=FakeSender(rc.EMAIL_RETRYABLE_FAILURE),
                  clock=_clock(hour=9))
    r1.run_research(label="morning")
    wr = _runtime(env, sender=FakeSender(rc.EMAIL_SENT), clock=_clock(hour=10))
    wr.run_watchdog()
    conn = rt.open_state_db(env["root"])
    sent = conn.execute("SELECT COUNT(*) FROM email_deliveries WHERE"
                        " status='EMAIL_SENT'").fetchone()[0]
    conn.close()
    assert sent == 1  # watchdog retried the stored report successfully


# --------------------------------------------------------------------------- #
# Verify mode (no writes, no network).
# --------------------------------------------------------------------------- #
def test_verify_ok(env):
    _runtime(env).run_collect()  # create state db + layout
    res = _runtime(env).run_verify()
    assert res.terminal == rc.VERIFIED


def test_verify_writes_nothing(env):
    _runtime(env).run_collect()
    before = _fingerprint(env["root"])
    _runtime(env).run_verify()
    assert _fingerprint(env["root"]) == before


def test_verify_blocked_when_state_missing(env):
    res = _runtime(env).run_verify()
    assert res.terminal == rc.BLOCKED


# --------------------------------------------------------------------------- #
# Task Scheduler command generation.
# --------------------------------------------------------------------------- #
def test_task_definitions_exact_four_names(env):
    defs = rt.build_task_definitions(env["cfg"], repo_root=str(_REPO),
                                     python_exe="py.exe",
                                     config_path="c.json")
    names = [d["task_name"] for d in defs]
    assert names == list(rc.ALPHA_AGENT_TASK_NAMES)


def test_task_definitions_no_password_and_limited(env):
    defs = rt.build_task_definitions(env["cfg"], repo_root=str(_REPO),
                                     python_exe="py.exe", config_path="c.json")
    for d in defs:
        assert d["stores_windows_password"] is False
        assert d["run_level"] == "Limited"
        assert d["multiple_instances"] == "IgnoreNew"
        assert d["logon_type"] == "Interactive"
        assert "run_alpha_agent.py" in d["action_args"]


def test_task_triggers_match_cadence(env):
    defs = {d["task_name"]: d for d in rt.build_task_definitions(
        env["cfg"], repo_root=str(_REPO), python_exe="py.exe",
        config_path="c.json")}
    assert defs["AlphaAgent-Collect"]["trigger"]["interval_minutes"] == 30
    assert defs["AlphaAgent-Watchdog"]["trigger"]["interval_minutes"] == 60
    assert defs["AlphaAgent-Morning-Report"]["trigger"]["type"] == "daily"
    assert defs["AlphaAgent-PostClose-Report"]["trigger"]["type"] == "weekly"


# --------------------------------------------------------------------------- #
# Report-only.
# --------------------------------------------------------------------------- #
def test_report_only_no_email_by_default(env):
    sender = FakeSender()
    res = _runtime(env, sender=sender).run_report_only(label="manual")
    assert res.terminal == rc.READY
    assert sender.calls == []  # report-only does not email by default
    assert Path(res.detail["report_html"]).exists()


# --------------------------------------------------------------------------- #
# Safety invariants (structural).
# --------------------------------------------------------------------------- #
def test_no_postgres_or_api_imports_in_runtime_sources():
    for mod in ("runtime.py", "runtime_contracts.py", "report_renderer.py"):
        src = (_REPO / "alpha_agent" / mod).read_text(encoding="utf-8")
        low = src.lower()
        assert "psycopg" not in low
        assert "import api" not in low
        assert "from api" not in low
        assert "import psycopg2" not in low


def test_renderer_has_no_network_or_subprocess():
    src = (_REPO / "alpha_agent" / "report_renderer.py").read_text(
        encoding="utf-8")
    for bad in ("subprocess", "socket", "requests", "urllib", "http.client",
                "smtplib"):
        assert bad not in src


def test_research_creates_no_orders_signals_fills_decisions(env):
    """A full research cycle appends NOTHING to any operational ledger file."""
    before = _fingerprint(env["ledger"])
    r = _runtime(env)
    r.run_research(label="morning")
    r.run_collect()
    r.run_watchdog()
    assert _fingerprint(env["ledger"]) == before


def test_retention_config_present():
    cfg = rc.load_config(_REPO / "configs" / "alpha_agent" /
                         "stage4_runtime.json")
    ret = cfg["retention"]
    assert ret["reports_days"] == 365
    assert ret["runtime_logs_days"] == 90
    assert ret["credentials_in_retention"] == "never"


def test_deterministic_run_ids_reproduce(env):
    # Two runtimes with the same fixed clock produce the same collect run id.
    r1 = _runtime(env, clock=_clock(second=5)).run_collect()
    # fresh runtime root to avoid lock/idempotency interference
    import shutil
    shutil.rmtree(env["root"])
    r2 = _runtime(env, clock=_clock(second=5)).run_collect()
    assert r1.run_id == r2.run_id


# --------------------------------------------------------------------------- #
# Stage 4 Gmail-API OAuth email delivery.
#
# The standalone authorization + sender scripts are imported in-process;
# urllib.request.urlopen is monkeypatched so NO real network, browser or Gmail is
# ever touched. The PowerShell wrappers are checked by static source assertions
# (no python -c, no embedded Python, no SMTP, refresh token only on stdin).
# Runtime status handling is checked with injected FakeSender objects.
# --------------------------------------------------------------------------- #
_SCRIPTS = _REPO / "scripts"
_SENDER_PY = _SCRIPTS / "send_alpha_agent_email.py"
_SENDER_PS1 = _SCRIPTS / "send_alpha_agent_email.ps1"
_AUTHORIZE_PY = _SCRIPTS / "authorize_alpha_agent_gmail.py"
_CONFIGURE_PS1 = _SCRIPTS / "configure_alpha_agent_email.ps1"

# Deterministic, obviously-fake secrets. Never real; never transmitted.
_REFRESH_TOKEN = "1//0refresh-token-FAKE-not-a-real-value"
_ACCESS_TOKEN = "ya29.FAKE-access-token-not-real"
_MESSAGE_ID = "18fabc0000000test"


def _load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _load_sender_module():
    return _load_module(_SENDER_PY, "aa_send_email_test")


def _load_authorize_module():
    return _load_module(_AUTHORIZE_PY, "aa_authorize_gmail_test")


class _FakeResp:
    def __init__(self, body):
        self._body = body.encode("utf-8") if isinstance(body, str) else body

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _http_error(code, body_obj=None):
    body = json.dumps(body_obj or {}).encode("utf-8")
    return urllib.error.HTTPError("https://api.test/x", code, "err", {},
                                  io.BytesIO(body))


def _fake_urlopen(*, token_body=None, token_error=None, send_body=None,
                  send_error=None):
    """A urlopen replacement that routes by URL (token endpoint vs Gmail API)."""
    def _open(req, timeout=None):
        url = req.full_url if hasattr(req, "full_url") else str(req)
        if "oauth2" in url or url.rstrip("/").endswith("token"):
            if token_error is not None:
                raise token_error
            return _FakeResp(token_body if token_body is not None
                             else json.dumps({"access_token": _ACCESS_TOKEN}))
        if send_error is not None:
            raise send_error
        return _FakeResp(send_body if send_body is not None
                         else json.dumps({"id": _MESSAGE_ID}))
    return _open


def _client_json(tmp_path):
    p = tmp_path / "google_oauth_client.json"
    p.write_text(json.dumps({"installed": {
        "client_id": "test-client.apps.googleusercontent.com",
        "client_secret": "test-secret-not-real",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "redirect_uris": ["http://localhost"]}}), encoding="utf-8")
    return p


def _valid_job(tmp_path):
    (tmp_path / "report.html").write_text("<h1>hi</h1>", encoding="utf-8")
    (tmp_path / "report.txt").write_text("hi", encoding="utf-8")
    job = {"recipient": "binisti@gmail.com", "subject": "Alpha Agent Report",
           "html_path": str(tmp_path / "report.html"),
           "text_path": str(tmp_path / "report.txt"),
           "attach_markdown": [str(tmp_path / "report.txt")]}
    job_path = tmp_path / "job.json"
    job_path.write_text(json.dumps(job), encoding="utf-8")
    return job_path


def _run_sender(mod, monkeypatch, job_path, client_path, *, urlopen=None,
                refresh_token=_REFRESH_TOKEN, account="binisti@gmail.com"):
    if urlopen is not None:
        monkeypatch.setattr(mod.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(sys, "stdin", io.StringIO(refresh_token + "\n"))
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        code = mod.main(["--job-path", str(job_path),
                         "--oauth-client-path", str(client_path),
                         "--account", account,
                         "--gmail-endpoint", "https://gmail.example.test/send",
                         "--token-endpoint", "https://oauth2.example.test/token",
                         "--timeout-seconds", "5"])
    out = buf.getvalue()
    lines = [ln.strip() for ln in out.splitlines()
             if ln.strip().startswith("{")]
    result = json.loads(lines[-1]) if lines else {}
    return code, result, out


# 1. Authorization script compiles.
def test_01_authorize_script_compiles():
    src = _AUTHORIZE_PY.read_text(encoding="utf-8")
    compile(src, str(_AUTHORIZE_PY), "exec")
    assert "import os" not in src        # cannot read the env for a secret
    assert "webbrowser" in src


# 2. Sender script compiles + never touches env/argv for the refresh token.
def test_02_sender_script_compiles_and_is_stdin_only():
    src = _SENDER_PY.read_text(encoding="utf-8")
    compile(src, str(_SENDER_PY), "exec")
    assert "sys.stdin.readline" in src
    assert "import os" not in src
    assert "getpass" not in src
    assert "--refresh-token" not in src   # never a CLI flag


# 3. No `python -c` in either PowerShell wrapper.
@pytest.mark.parametrize("ps1", [_SENDER_PS1, _CONFIGURE_PS1])
def test_03_ps_wrappers_have_no_python_dash_c(ps1):
    src = ps1.read_text(encoding="utf-8")
    low = src.lower()
    assert " -c " not in src
    assert "-c $" not in src
    assert "python -c" not in low
    assert ".exe -c" not in low


# 4. No embedded Python here-string / source in either PowerShell wrapper.
@pytest.mark.parametrize("ps1", [_SENDER_PS1, _CONFIGURE_PS1])
def test_04_ps_wrappers_have_no_embedded_python(ps1):
    src = ps1.read_text(encoding="utf-8")
    for marker in ('@"', '"@', "@'", "'@"):
        assert marker not in src, "here-string marker %r present" % marker
    for py in ("import smtplib", "import urllib", "EmailMessage",
               "urlopen", "base64.urlsafe", "def main("):
        assert py not in src, "embedded Python token %r present" % py


# 5. No smtplib anywhere in the active implementation.
def test_05_no_smtplib_in_active_implementation():
    for p in (_SENDER_PY, _SENDER_PS1, _CONFIGURE_PS1, _AUTHORIZE_PY,
              _REPO / "alpha_agent" / "runtime.py"):
        assert "smtplib" not in p.read_text(encoding="utf-8"), p.name


# 6. No smtp.gmail.com anywhere in the active implementation or config.
def test_06_no_smtp_gmail_host():
    for p in (_SENDER_PY, _SENDER_PS1, _CONFIGURE_PS1, _AUTHORIZE_PY,
              _REPO / "alpha_agent" / "runtime.py",
              _REPO / "configs" / "alpha_agent" / "stage4_runtime.json"):
        assert "smtp.gmail.com" not in p.read_text(encoding="utf-8"), p.name


# 7. No App Password references in the active implementation.
def test_07_no_app_password_references():
    for p in (_SENDER_PY, _SENDER_PS1, _CONFIGURE_PS1,
              _REPO / "configs" / "alpha_agent" / "stage4_runtime.json"):
        low = p.read_text(encoding="utf-8").lower()
        assert "app password" not in low, p.name
        assert "app_password" not in low, p.name
        assert "apppassword" not in low, p.name


# 8. PKCE S256 generation (verifier in range; challenge = b64url(sha256)).
def test_08_pkce_s256_generation():
    import hashlib
    am = _load_authorize_module()
    verifier, challenge = am._pkce_pair()
    assert 43 <= len(verifier) <= 128
    assert all(c.isalnum() or c in "-_" for c in verifier)
    assert "=" not in challenge and "+" not in challenge and "/" not in challenge
    expected = base64.urlsafe_b64encode(
        hashlib.sha256(verifier.encode("ascii")).digest()).rstrip(b"=") \
        .decode("ascii")
    assert challenge == expected


# 8b. The authorization URL carries offline/consent/PKCE/login_hint parameters.
def test_08b_auth_url_parameters():
    am = _load_authorize_module()
    client = {"client_id": "cid", "client_secret": "sec",
              "auth_uri": am._DEFAULT_AUTH_URI, "token_uri": am._DEFAULT_TOKEN_URI}
    url = am._build_auth_url(client, redirect_uri="http://127.0.0.1:5555/",
                             scope=am._GMAIL_SEND_SCOPE, state="STATE123",
                             challenge="CHAL", account="binisti@gmail.com")
    for token in ("access_type=offline", "prompt=consent",
                  "code_challenge_method=S256", "code_challenge=CHAL",
                  "state=STATE123", "login_hint=binisti",
                  "gmail.send"):
        assert token in url


# 9. Random state generation + exact state validation.
def test_09_random_state_and_validation():
    am = _load_authorize_module()
    assert am._random_state() != am._random_state()
    assert len(am._random_state()) >= 16
    # Mismatched state is rejected.
    status, code, _ = am._classify_redirect(
        {"state": "attacker", "code": "abc"}, "expected")
    assert status == am.AUTHORIZATION_STATE_MISMATCH
    assert code is None
    # Matching state with a code proceeds.
    status2, code2, _ = am._classify_redirect(
        {"state": "expected", "code": "abc"}, "expected")
    assert status2 is None and code2 == "abc"


# 10. Loopback listener binds ONLY to 127.0.0.1.
def test_10_loopback_only():
    am = _load_authorize_module()
    assert am._LOOPBACK == "127.0.0.1"
    src = _AUTHORIZE_PY.read_text(encoding="utf-8")
    assert "0.0.0.0" not in src
    assert 'HTTPServer((_LOOPBACK, 0)' in src


# 11. OAuth error responses are handled (denied / no code).
def test_11_oauth_error_handling():
    am = _load_authorize_module()
    status, code, _ = am._classify_redirect(
        {"error": "access_denied", "state": "s"}, "s")
    assert status == am.AUTHORIZATION_DENIED and code is None
    status2, _, _ = am._classify_redirect({"state": "s"}, "s")
    assert status2 == am.AUTHORIZATION_NO_CODE


# 12. A token response missing the refresh_token is rejected.
def test_12_missing_refresh_token_rejected():
    am = _load_authorize_module()
    status, refresh, _, _ = am._finalize_token(
        {"access_token": "a", "scope": am._GMAIL_SEND_SCOPE},
        required_scope=am._GMAIL_SEND_SCOPE)
    assert status == am.AUTHORIZATION_NO_REFRESH_TOKEN and refresh is None
    # Insufficient scope is also rejected.
    status2, _, _, _ = am._finalize_token(
        {"refresh_token": "r", "scope": "https://mail.google.com/"},
        required_scope=am._GMAIL_SEND_SCOPE)
    assert status2 == am.AUTHORIZATION_SCOPE_INSUFFICIENT


# 13. The refresh token is captured but never printed except in the one result.
def test_13_refresh_token_only_in_success_json():
    src = _AUTHORIZE_PY.read_text(encoding="utf-8")
    assert '"refresh_token": None' in src              # failures carry no token
    assert src.count('"refresh_token": refresh') == 1  # emitted exactly once
    assert "print(" not in src                          # never printed at all
    am = _load_authorize_module()
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc_ = am._fail(am.AUTHORIZATION_TIMEOUT, "safe")
    obj = json.loads(buf.getvalue().strip())
    assert obj["refresh_token"] is None and rc_ == 1


# 14. DPAPI encrypted storage contract in the configure wrapper.
def test_14_configure_dpapi_storage_contract():
    src = _CONFIGURE_PS1.read_text(encoding="utf-8")
    assert "ConvertFrom-SecureString" in src        # DPAPI encrypt
    assert "gmail_oauth_refresh_token.dpapi" in src  # stored token file
    assert "gmail_oauth_account.txt" in src          # stored account file
    assert "ConvertTo-SecureString" in src           # round-trip verify
    assert "ZeroFreeBSTR" in src                     # zero the BSTR
    assert "icacls" in src                           # current-user ACL
    assert "GMAIL_OAUTH_CONFIGURED" in src


# 15. Refresh token passed by redirected stdin only (send wrapper).
def test_15_refresh_token_via_redirected_stdin():
    src = _SENDER_PS1.read_text(encoding="utf-8")
    assert "RedirectStandardInput = $true" in src
    assert "StandardInput.WriteLine($Plain)" in src
    assert "StandardInput.Close()" in src


# 16. Refresh token does not appear in process arguments.
def test_16_refresh_token_not_in_arguments():
    src = _SENDER_PS1.read_text(encoding="utf-8")
    for line in src.splitlines():
        if "Arguments" in line or "Quote-Arg" in line:
            assert "$Plain" not in line


# 17. Refresh token does not appear in any environment variable.
def test_17_refresh_token_not_in_environment():
    src = _SENDER_PS1.read_text(encoding="utf-8")
    for line in src.splitlines():
        if "$env:" in line:
            assert "$Plain" not in line


# 18. A successful token refresh + send yields EMAIL_SENT.
def test_18_successful_token_refresh(monkeypatch, tmp_path):
    mod = _load_sender_module()
    code, result, out = _run_sender(mod, monkeypatch, _valid_job(tmp_path),
                                    _client_json(tmp_path),
                                    urlopen=_fake_urlopen())
    assert result["status"] == mod.EMAIL_SENT
    assert code == 0
    assert _ACCESS_TOKEN not in out


# 19. Token endpoint invalid_grant maps to OAUTH_REAUTHORIZATION_REQUIRED.
def test_19_invalid_grant_mapping(monkeypatch, tmp_path):
    mod = _load_sender_module()
    fake = _fake_urlopen(token_error=_http_error(400, {"error": "invalid_grant"}))
    _, result, out = _run_sender(mod, monkeypatch, _valid_job(tmp_path),
                                 _client_json(tmp_path), urlopen=fake)
    assert result["status"] == mod.OAUTH_REAUTHORIZATION_REQUIRED
    assert _REFRESH_TOKEN not in out


# 19b. invalid_client maps to OAUTH_CLIENT_INVALID.
def test_19b_invalid_client_mapping(monkeypatch, tmp_path):
    mod = _load_sender_module()
    fake = _fake_urlopen(token_error=_http_error(401, {"error": "invalid_client"}))
    _, result, _ = _run_sender(mod, monkeypatch, _valid_job(tmp_path),
                               _client_json(tmp_path), urlopen=fake)
    assert result["status"] == mod.OAUTH_CLIENT_INVALID


# 20. Gmail-API MIME base64url encoding (URL-safe, unpadded, round-trips).
def test_20_mime_base64url_encoding():
    mod = _load_sender_module()
    msg = mod.EmailMessage()
    msg["From"] = "a@b.com"
    msg["To"] = "c@d.com"
    msg["Subject"] = "unit-subject"
    msg.set_content("body text")
    raw = mod._encode_mime(msg)
    assert "=" not in raw and "+" not in raw and "/" not in raw
    decoded = base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4))
    assert b"Subject: unit-subject" in decoded


# 21. Gmail-API send success returns the message id.
def test_21_send_success_message_id(monkeypatch, tmp_path):
    mod = _load_sender_module()
    code, result, out = _run_sender(mod, monkeypatch, _valid_job(tmp_path),
                                    _client_json(tmp_path),
                                    urlopen=_fake_urlopen())
    assert result["status"] == mod.EMAIL_SENT
    assert result["message_id"] == _MESSAGE_ID
    assert code == 0
    assert _REFRESH_TOKEN not in out and _ACCESS_TOKEN not in out


# 22. HTTP 401 on send -> OAUTH_REAUTHORIZATION_REQUIRED.
def test_22_http_401_mapping(monkeypatch, tmp_path):
    mod = _load_sender_module()
    assert mod._map_send_status(401) == mod.OAUTH_REAUTHORIZATION_REQUIRED
    fake = _fake_urlopen(send_error=_http_error(401))
    _, result, out = _run_sender(mod, monkeypatch, _valid_job(tmp_path),
                                 _client_json(tmp_path), urlopen=fake)
    assert result["status"] == mod.OAUTH_REAUTHORIZATION_REQUIRED
    assert _ACCESS_TOKEN not in out


# 23. HTTP 403 on send -> GMAIL_API_PERMISSION_DENIED.
def test_23_http_403_mapping(monkeypatch, tmp_path):
    mod = _load_sender_module()
    assert mod._map_send_status(403) == mod.GMAIL_API_PERMISSION_DENIED
    fake = _fake_urlopen(send_error=_http_error(403))
    _, result, _ = _run_sender(mod, monkeypatch, _valid_job(tmp_path),
                               _client_json(tmp_path), urlopen=fake)
    assert result["status"] == mod.GMAIL_API_PERMISSION_DENIED


# 24. HTTP 429 on send -> GMAIL_API_RATE_LIMITED.
def test_24_http_429_mapping(monkeypatch, tmp_path):
    mod = _load_sender_module()
    assert mod._map_send_status(429) == mod.GMAIL_API_RATE_LIMITED
    fake = _fake_urlopen(send_error=_http_error(429))
    _, result, _ = _run_sender(mod, monkeypatch, _valid_job(tmp_path),
                               _client_json(tmp_path), urlopen=fake)
    assert result["status"] == mod.GMAIL_API_RATE_LIMITED


# 25. HTTP 5xx on send -> GMAIL_API_RETRYABLE_FAILURE.
def test_25_http_5xx_mapping(monkeypatch, tmp_path):
    mod = _load_sender_module()
    for code in (500, 502, 503, 504):
        assert mod._map_send_status(code) == mod.GMAIL_API_RETRYABLE_FAILURE
    fake = _fake_urlopen(send_error=_http_error(503))
    _, result, _ = _run_sender(mod, monkeypatch, _valid_job(tmp_path),
                               _client_json(tmp_path), urlopen=fake)
    assert result["status"] == mod.GMAIL_API_RETRYABLE_FAILURE


# 25b. A network error (no HTTP status) is retryable.
def test_25b_network_error_retryable(monkeypatch, tmp_path):
    mod = _load_sender_module()
    fake = _fake_urlopen(send_error=urllib.error.URLError("no route"))
    _, result, _ = _run_sender(mod, monkeypatch, _valid_job(tmp_path),
                               _client_json(tmp_path), urlopen=fake)
    assert result["status"] == mod.GMAIL_API_RETRYABLE_FAILURE


# 26. An invalid job maps to EMAIL_JOB_INVALID (never reaches the network).
def test_26_invalid_job(monkeypatch, tmp_path):
    mod = _load_sender_module()
    bad = tmp_path / "bad.json"
    bad.write_text("{ this is not json", encoding="utf-8")
    _, result, out = _run_sender(mod, monkeypatch, bad, _client_json(tmp_path),
                                 urlopen=_fake_urlopen())
    assert result["status"] == mod.EMAIL_JOB_INVALID
    assert _REFRESH_TOKEN not in out


# 27. Attachment validation (oversize / non-string) -> EMAIL_ATTACHMENT_INVALID.
def test_27_attachment_validation(monkeypatch, tmp_path):
    mod = _load_sender_module()
    (tmp_path / "report.html").write_text("<h1>hi</h1>", encoding="utf-8")
    (tmp_path / "report.txt").write_text("hi", encoding="utf-8")
    big = tmp_path / "big.bin"
    big.write_bytes(b"x" * (mod._MAX_ATTACHMENT_BYTES + 1))
    job = {"recipient": "binisti@gmail.com", "subject": "s",
           "html_path": str(tmp_path / "report.html"),
           "text_path": str(tmp_path / "report.txt"),
           "attach_markdown": [str(big)]}
    jp = tmp_path / "job.json"
    jp.write_text(json.dumps(job), encoding="utf-8")
    _, result, _ = _run_sender(mod, monkeypatch, jp, _client_json(tmp_path),
                               urlopen=_fake_urlopen())
    assert result["status"] == mod.EMAIL_ATTACHMENT_INVALID
    # A non-string attachment entry is likewise rejected.
    job["attach_markdown"] = [123]
    jp.write_text(json.dumps(job), encoding="utf-8")
    _, result2, _ = _run_sender(mod, monkeypatch, jp, _client_json(tmp_path),
                                urlopen=_fake_urlopen())
    assert result2["status"] == mod.EMAIL_ATTACHMENT_INVALID


# 27b. A bad OAuth client file maps to OAUTH_CLIENT_INVALID.
def test_27b_bad_client_file(monkeypatch, tmp_path):
    mod = _load_sender_module()
    bad_client = tmp_path / "client.json"
    bad_client.write_text("{}", encoding="utf-8")
    _, result, _ = _run_sender(mod, monkeypatch, _valid_job(tmp_path),
                               bad_client, urlopen=_fake_urlopen())
    assert result["status"] == mod.OAUTH_CLIENT_INVALID


# --- Runtime-level status preservation ------------------------------------- #
# The runtime parses the sender/wrapper JSON, preserving status + message id.
def test_runtime_parses_success_json():
    line = ('{"status":"EMAIL_SENT","message_id":"18fabc",'
            '"diagnostic":"Report delivered through Gmail API."}\n')
    status, message_id, diag = rt._parse_email_result(line)
    assert status == rc.EMAIL_SENT
    assert message_id == "18fabc"
    assert diag == "Report delivered through Gmail API."


# The runtime falls back to a token scan (and safe [gmail] diag) with no JSON.
def test_runtime_parses_token_fallback():
    text = "OAUTH_REAUTHORIZATION_REQUIRED\n[gmail] re-auth required\n"
    status, message_id, diag = rt._parse_email_result(text)
    assert status == rt.OAUTH_REAUTHORIZATION_REQUIRED
    assert message_id is None
    assert diag == "re-auth required"


# 28. A previously TRANSIENT-failed cycle is retried by the watchdog.
def test_28_transient_failure_retried_by_watchdog(env):
    r1 = _runtime(env, sender=FakeSender(rt.GMAIL_API_RETRYABLE_FAILURE),
                  clock=_clock(hour=8, minute=0))
    r1.run_research(label="morning")
    wr = _runtime(env, sender=FakeSender(rc.EMAIL_SENT), clock=_clock(hour=9))
    wr.run_watchdog()
    conn = rt.open_state_db(env["root"])
    sent = conn.execute("SELECT COUNT(*) FROM email_deliveries WHERE"
                        " status='EMAIL_SENT'").fetchone()[0]
    conn.close()
    assert sent == 1


# 28b. A rate-limited cycle is also retried by the watchdog.
def test_28b_rate_limited_retried_by_watchdog(env):
    r1 = _runtime(env, sender=FakeSender(rt.GMAIL_API_RATE_LIMITED),
                  clock=_clock(hour=8, minute=0))
    r1.run_research(label="morning")
    wr = _runtime(env, sender=FakeSender(rc.EMAIL_SENT), clock=_clock(hour=9))
    wr.run_watchdog()
    conn = rt.open_state_db(env["root"])
    sent = conn.execute("SELECT COUNT(*) FROM email_deliveries WHERE"
                        " status='EMAIL_SENT'").fetchone()[0]
    conn.close()
    assert sent == 1


# 29. Duplicate successful delivery is prevented.
def test_29_duplicate_success_prevented(env):
    sender = FakeSender(rc.EMAIL_SENT, message_id=_MESSAGE_ID)
    r = _runtime(env, sender=sender)
    r.run_research(label="morning")
    r.run_research(label="morning")
    assert len(sender.calls) == 1
    conn = rt.open_state_db(env["root"])
    sent = conn.execute("SELECT COUNT(*) FROM email_deliveries WHERE"
                        " status='EMAIL_SENT'").fetchone()[0]
    conn.close()
    assert sent == 1


# 30. The existing report is reused on retry — no second LLM/research cycle.
def test_30_retry_reuses_report_no_new_research(env):
    drivers = FakeDrivers()
    r1 = rt.Runtime(env["cfg"], drivers=drivers,
                    email_sender=FakeSender(rt.GMAIL_API_RETRYABLE_FAILURE),
                    clock=_clock(hour=8, minute=0))
    r1.run_research(label="morning")
    assert drivers.research_calls == 1
    wr = rt.Runtime(env["cfg"], drivers=drivers,
                    email_sender=FakeSender(rc.EMAIL_SENT),
                    clock=_clock(hour=9))
    wr.run_watchdog()
    assert drivers.research_calls == 1


# 31. Watchdog does NOT repeatedly retry a reauthorization-required failure,
#     but still flags it as an outstanding failure needing attention.
def test_31_reauth_not_autoretried(env):
    r1 = _runtime(env, sender=FakeSender(rt.OAUTH_REAUTHORIZATION_REQUIRED),
                  clock=_clock(hour=8, minute=0))
    r1.run_research(label="morning")
    retry_sender = FakeSender(rc.EMAIL_SENT)
    wr = _runtime(env, sender=retry_sender, clock=_clock(hour=9))
    wres = wr.run_watchdog()
    assert retry_sender.calls == []                       # not auto-retried
    assert wres.detail["checks"]["email_failures"] >= 1   # still flagged
    assert wres.terminal == rc.DEGRADED


# 31b. Each distinct failure status flows through without collapsing.
@pytest.mark.parametrize("status", [
    rt.OAUTH_REAUTHORIZATION_REQUIRED, rt.OAUTH_TOKEN_REFRESH_REJECTED,
    rt.OAUTH_CLIENT_INVALID, rt.GMAIL_API_PERMISSION_DENIED,
    rt.GMAIL_API_RATE_LIMITED, rt.GMAIL_API_RETRYABLE_FAILURE,
    rt.EMAIL_JOB_INVALID, rc.EMAIL_PERMANENT_FAILURE])
def test_31b_distinct_failure_statuses_not_collapsed(env, status):
    r = _runtime(env, sender=FakeSender(status))
    res = r.run_research(label="morning")
    assert res.email_status == status
    assert res.terminal == rc.DEGRADED
    assert res.as_dict()["email_status"] == status


# 31c. Outbox transitions: sent on success (message id retained), failed on error.
def test_31c_outbox_transition_sent(env):
    r = _runtime(env, sender=FakeSender(rc.EMAIL_SENT, message_id=_MESSAGE_ID))
    res = r.run_research(label="morning")
    cyc = res.cycle_id
    assert (env["root"] / "outbox" / "sent" / ("%s.json" % cyc)).exists()
    assert res.email_status == rc.EMAIL_SENT
    assert res.email_message_id == _MESSAGE_ID
    assert res.as_dict()["email_message_id"] == _MESSAGE_ID


def test_31d_outbox_transition_failed_preserves_status(env):
    r = _runtime(env, sender=FakeSender(rt.GMAIL_API_PERMISSION_DENIED))
    res = r.run_research(label="morning")
    cyc = res.cycle_id
    assert (env["root"] / "outbox" / "failed" / ("%s.json" % cyc)).exists()
    conn = rt.open_state_db(env["root"])
    st = conn.execute("SELECT status FROM email_deliveries WHERE cycle_id=?",
                      (cyc,)).fetchone()[0]
    conn.close()
    assert st == rt.GMAIL_API_PERMISSION_DENIED   # NOT collapsed


# 32. Operational ledgers remain byte-identical across failure + retry.
def test_32_ledgers_unchanged_across_failure_and_retry(env):
    before = _fingerprint(env["ledger"])
    r1 = _runtime(env, sender=FakeSender(rt.GMAIL_API_RETRYABLE_FAILURE),
                  clock=_clock(hour=8, minute=0))
    r1.run_research(label="morning")
    wr = _runtime(env, sender=FakeSender(rc.EMAIL_SENT), clock=_clock(hour=9))
    wr.run_watchdog()
    assert _fingerprint(env["ledger"]) == before


# 33. No order/fill/signal/decision/model file is ever created by delivery.
def test_33_delivery_creates_no_operational_artifacts(env):
    before = _fingerprint(env["ledger"])
    for st in (rt.OAUTH_REAUTHORIZATION_REQUIRED, rt.GMAIL_API_RATE_LIMITED,
               rc.EMAIL_SENT):
        _runtime(env, sender=FakeSender(st)).run_research(label="morning")
    assert _fingerprint(env["ledger"]) == before
    for banned in ("orders", "fills", "signals", "decisions", "promotion"):
        assert not any(banned in p.name.lower()
                       for p in (env["root"]).rglob("*") if p.is_file())


# 34. No credential or token leakage: the refresh/access tokens never appear in
#     the sender's stdout, and the shipped config embeds no secret.
def test_34_no_credential_or_token_leakage(monkeypatch, tmp_path):
    mod = _load_sender_module()
    _, _result, out = _run_sender(mod, monkeypatch, _valid_job(tmp_path),
                                  _client_json(tmp_path),
                                  urlopen=_fake_urlopen())
    assert _REFRESH_TOKEN not in out
    assert _ACCESS_TOKEN not in out
    cfg = rc.load_config(_REPO / "configs" / "alpha_agent" /
                         "stage4_runtime.json")
    assert rc.scan_for_secrets(cfg) == []
    assert cfg["email"]["delivery_provider"] == "gmail_api_oauth"


# --------------------------------------------------------------------------- #
# Stage 5 integration (optional experiment & evidence engine).
# --------------------------------------------------------------------------- #
_S5_RESULT = {
    "run_id": "stage5_fake123", "terminal": rt.ec.READY,
    "status": "EXPERIMENTS_COMPLETE", "run_dir": "d",
    "champion_model": "fundamental_momentum_50_50_v1",
    "counts": {"grounded_hypotheses": 1, "hypotheses_considered": 1,
               "specs_generated": 1, "experiments_completed": 1,
               "experiments_failed": 0, "data_gaps": 0,
               "duplicates_rejected": 0, "keep_for_research": 1,
               "deferred_to_next_cycle": 0},
    "results": [{"experiment_id": "exp_1", "hypothesis_id": "h1",
                 "template": "price_momentum_rank", "rank_ic_t": 3.1,
                 "rank_ic_mean": 0.05, "net_annualized_return": 0.2,
                 "benchmark_name": "equal_weight_universe",
                 "benchmark_excess_annualized": 0.05,
                 "champion_complementarity": 0.6, "cost_flips_sign": False}],
    "decisions": [{"experiment_id": "exp_1", "hypothesis_id": "h1",
                   "template": "price_momentum_rank",
                   "decision": "KEEP_FOR_RESEARCH", "reasons": ["ok"]}],
    "data_gaps": [], "duplicates": [],
}


class _S5Drivers(FakeDrivers):
    def run_stage5(self, mode):
        return {"component": rt.COMPONENT_STAGE5, "status": rt.ec.READY,
                "terminal": rt.ec.READY, "ok": True, "no_new": False,
                "verified": False, "run_id": "stage5_fake123", "run_dir": "d",
                "counts": _S5_RESULT["counts"], "metrics": {},
                "result": _S5_RESULT}


def test_stage5_integration_in_research(env):
    cfg = dict(env["cfg"])
    cfg["stage5_enabled"] = True
    cfg["stage5_experiments_root"] = str(env["base"] / "experiments")
    r = rt.Runtime(cfg, drivers=_S5Drivers(), email_sender=FakeSender(),
                   clock=_clock())
    res = r.run_research(label="morning")
    assert rt.COMPONENT_STAGE5 in [c.get("component") for c in res.components]
    html = Path(res.detail["report_html"]).read_text(encoding="utf-8")
    assert "Experiment &amp; Evidence" in html
    assert "stage5_fake123" in html


def test_stage5_disabled_by_default(env):
    """Without stage5_enabled the runtime never calls run_stage5 (Stage 4
    behaviour is unchanged) and the report shows 'not run'."""
    r = _runtime(env, drivers=_S5Drivers())
    res = r.run_research(label="morning")
    assert rt.COMPONENT_STAGE5 not in [c.get("component")
                                       for c in res.components]
    html = Path(res.detail["report_html"]).read_text(encoding="utf-8")
    assert "Stage 5 experiment engine not run" in html


def test_stage5_report_only_reads_latest(env):
    exp_root = env["base"] / "experiments"
    rid = "stage5_diskabc"
    run_dir = exp_root / "runs" / rid
    run_dir.mkdir(parents=True)
    _w(exp_root / "latest.json",
       {"run_id": rid, "terminal": rt.ec.DATA_HOLD,
        "status": "ALL_HYPOTHESES_DATA_HELD",
        "champion_model": "fundamental_momentum_50_50_v1"})
    _w(run_dir / "run_manifest.json",
       {"terminal": rt.ec.DATA_HOLD, "status": "ALL_HYPOTHESES_DATA_HELD",
        "counts": {"grounded_hypotheses": 1, "hypotheses_considered": 1,
                   "specs_generated": 0, "experiments_completed": 0,
                   "experiments_failed": 0, "data_gaps": 1,
                   "duplicates_rejected": 0, "keep_for_research": 0}})
    for f in ("hypothesis_intake.jsonl", "experiment_specs.jsonl",
              "experiment_results.jsonl", "evidence_decisions.jsonl",
              "data_gaps.jsonl", "rejected_duplicates.jsonl"):
        (run_dir / f).write_text("", encoding="utf-8")
    cfg = dict(env["cfg"])
    cfg["stage5_experiments_root"] = str(exp_root)
    r = rt.Runtime(cfg, drivers=FakeDrivers(), email_sender=FakeSender(),
                   clock=_clock())
    res = r.run_report_only(label="morning")
    html = Path(res.detail["report_html"]).read_text(encoding="utf-8")
    assert "Experiment &amp; Evidence" in html
    assert rid in html
