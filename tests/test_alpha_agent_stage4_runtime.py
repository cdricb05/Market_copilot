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
                  "app_credential_file": "gmail_smtp_app_password.dpapi",
                  "smtp_host": "smtp.gmail.com", "smtp_port": 587,
                  "transport": "gmail_smtp",
                  "delivery_provider": "gmail_smtp"},
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
    # A degraded runtime surfaces as the single DATA / AGENT ATTENTION action.
    assert rr.ACTION_DATA_ATTENTION in html


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
    # Real SMTP sender, no App Password -> credential-missing, no subprocess.
    r = rt.Runtime(env["cfg"], drivers=FakeDrivers(), email_sender=None,
                   clock=_clock())
    res = r.run_research(label="post_close")
    assert res.terminal == rc.EMAIL_CREDENTIAL_REQUIRED
    assert res.email_status == rt.EMAIL_SMTP_CREDENTIAL_MISSING
    assert res.email_transport == rt.EMAIL_TRANSPORT_SMTP


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
    # Dynamic values (here, failed-feed names surfaced in the Data & System
    # Issues section) are HTML-escaped so untrusted content can never inject.
    model = {"cycle_label": "morning", "cycle_date": "2026-07-29",
             "subject": "S", "generated_at": "t",
             "schedule_status": "Off", "status_flags": list(rr.STATUS_FLAGS),
             "paper_book": {}, "recovery_readiness": {},
             "news_rss": {"failed_feeds": ["<script>alert(1)</script>", "A&B"]}}
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
# Stage 7.2 — read-only Gmail credential diagnostic (token-exchange probe).
_DIAG_PY = _SCRIPTS / "diagnose_alpha_agent_gmail.py"
_DIAG_PS1 = _SCRIPTS / "diagnose_alpha_agent_gmail.ps1"
# Gmail SMTP (App Password) — the PRIMARY, active transport (replaces OAuth).
_SMTP_SENDER_PY = _SCRIPTS / "send_alpha_agent_smtp.py"
_SMTP_SENDER_PS1 = _SCRIPTS / "send_alpha_agent_smtp.ps1"
_SMTP_DIAG_PY = _SCRIPTS / "diagnose_alpha_agent_smtp.py"
_SMTP_DIAG_PS1 = _SCRIPTS / "diagnose_alpha_agent_smtp.ps1"
_SMTP_CONFIGURE_PS1 = _SCRIPTS / "configure_alpha_agent_smtp.ps1"

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


# 6. smtp.gmail.com must NOT appear in the LEGACY Gmail-API OAuth transport
#    files (kept unmuddled). Gmail SMTP is now the primary transport, so the host
#    legitimately lives in the new SMTP scripts, runtime and config.
def test_06_no_smtp_gmail_host_in_legacy_oauth_files():
    for p in (_SENDER_PY, _SENDER_PS1, _CONFIGURE_PS1, _AUTHORIZE_PY):
        assert "smtp.gmail.com" not in p.read_text(encoding="utf-8"), p.name


# 7. App Password references must NOT appear in the LEGACY OAuth transport files;
#    they must never carry the SMTP credential. They legitimately live in the new
#    SMTP scripts + config.
def test_07_no_app_password_in_legacy_oauth_files():
    for p in (_SENDER_PY, _SENDER_PS1, _CONFIGURE_PS1, _AUTHORIZE_PY):
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


# ---------------------------------------------------------------------------- #
# Stage 7.2 — read-only Gmail credential DIAGNOSTIC (token-exchange probe).
# It classifies a delivery-blocking credential state without sending an email,
# authorizing, writing a file, or exposing any secret.
# ---------------------------------------------------------------------------- #
def _load_diag_module():
    return _load_module(_DIAG_PY, "aa_gmail_diagnostic_test")


def test_diag_probe_compiles_and_is_stdin_only():
    src = _DIAG_PY.read_text(encoding="utf-8")
    compile(src, str(_DIAG_PY), "exec")
    assert "sys.stdin.readline" in src        # refresh token only from stdin
    assert "--refresh-token" not in src        # never a CLI flag
    assert "import smtplib" not in src         # Gmail/OAuth over HTTPS, no SMTP
    assert '"w"' not in src and "'w'" not in src   # read-only: never writes


def test_diag_classifier_mappings():
    dm = _load_diag_module()
    assert dm.classify_token_error("invalid_grant", 400) == \
        dm.TOKEN_EXCHANGE_INVALID_GRANT
    assert dm.classify_token_error("invalid_client", 401) == \
        dm.TOKEN_EXCHANGE_CLIENT_MISMATCH
    assert dm.classify_token_error("access_denied", 403) == \
        dm.TOKEN_EXCHANGE_POLICY_REJECTION
    assert dm.classify_token_error("", 400) == \
        dm.TOKEN_EXCHANGE_POLICY_REJECTION
    assert dm.classify_token_error("", 0) == dm.TOKEN_EXCHANGE_UNREACHABLE


def test_diag_probe_emits_safe_invalid_grant_json(monkeypatch, tmp_path):
    dm = _load_diag_module()
    client = _client_json(tmp_path)
    monkeypatch.setattr(dm.urllib.request, "urlopen", _fake_urlopen(
        token_error=_http_error(400, {"error": "invalid_grant",
                                      "error_description": "Bad Request"})))
    monkeypatch.setattr(sys, "stdin", io.StringIO(_REFRESH_TOKEN + "\n"))
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        code = dm.main(["--oauth-client-path", str(client),
                        "--expected-account", "binisti@gmail.com",
                        "--token-endpoint", "https://oauth2.example.test/token",
                        "--timeout-seconds", "5"])
    out = buf.getvalue()
    result = json.loads([ln for ln in out.splitlines()
                         if ln.strip().startswith("{")][-1])
    assert code == 1
    assert result["classification"] == dm.TOKEN_EXCHANGE_INVALID_GRANT
    assert result["google_error"] == "invalid_grant"
    assert _REFRESH_TOKEN not in out           # refresh token never printed
    assert "test-secret-not-real" not in out   # client secret never printed


def test_diag_probe_empty_stdin_is_token_file_not_found(monkeypatch, tmp_path):
    dm = _load_diag_module()
    client = _client_json(tmp_path)
    monkeypatch.setattr(sys, "stdin", io.StringIO("\n"))
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        code = dm.main(["--oauth-client-path", str(client)])
    result = json.loads([ln for ln in buf.getvalue().splitlines()
                         if ln.strip().startswith("{")][-1])
    assert result["classification"] == dm.TOKEN_FILE_NOT_FOUND
    assert code == 1


@pytest.mark.parametrize("ps1", [_DIAG_PS1])
def test_diag_ps1_is_readonly_stdin_only_no_python_dash_c(ps1):
    src = ps1.read_text(encoding="utf-8")
    low = src.lower()
    assert "python -c" not in low and " -c " not in src
    for marker in ('@"', '"@', "@'", "'@"):
        assert marker not in src               # no embedded Python here-string
    assert "StandardInput" in src              # refresh token only via stdin
    assert "Move-Item" not in src              # moves nothing
    assert "Set-Content" not in src            # writes nothing
    assert "Out-File" not in src


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
    # Gmail SMTP is now the primary, active transport (OAuth retired).
    assert cfg["email"]["transport"] == "gmail_smtp"
    assert cfg["email"]["delivery_provider"] == "gmail_smtp"


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
    # Executive layout: research progress in the main body; raw Stage 5 run ids
    # no longer leak into the email (they live in the API/UI observatory).
    assert "4. Research progress" in html
    assert "stage5_fake123" not in html


def test_stage5_disabled_by_default(env):
    """Without stage5_enabled the runtime never calls run_stage5 (Stage 4
    behaviour is unchanged) and the report shows 'not run'."""
    r = _runtime(env, drivers=_S5Drivers())
    res = r.run_research(label="morning")
    assert rt.COMPONENT_STAGE5 not in [c.get("component")
                                       for c in res.components]
    html = Path(res.detail["report_html"]).read_text(encoding="utf-8")
    # No Stage 5 result and no recovery package → research progress still states
    # the verdict once and reports that nothing was evaluated / promoted.
    assert "4. Research progress" in html
    assert rr.VERDICT_SENTENCE in html
    assert "no model was promoted" in html.lower()


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
    assert "4. Research progress" in html
    # A Stage 5 data-held study is reported SEPARATELY (never merged into the
    # Stage 7 recovery evaluated count), and the raw run id stays out of the
    # email.
    assert "could not run" in html.lower()
    assert rid not in html


def test_stage7_recovery_report_only_reads_latest(env):
    rec_root = env["base"] / "recovery"
    rid = "stage7_diskrecov"
    run_dir = rec_root / "runs" / rid
    run_dir.mkdir(parents=True)
    _w(rec_root / "latest.json",
       {"run_id": rid, "disposition": "NEED_MORE_EVIDENCE",
        "terminal": "ALPHA_AGENT_STAGE7_READY"})
    _w(run_dir / "recovery_disposition.json",
       {"disposition": "NEED_MORE_EVIDENCE",
        "rationale": "fundamental leg unverifiable; forward sample too small"})
    _w(run_dir / "champion_reconstruction.json",
       {"champion_model": "fundamental_momentum_50_50_v1",
        "classification": [
            {"component": "portfolio_construction",
             "class": "EXACT_RECONSTRUCTION"},
            {"component": "fundamental_leg_point_in_time",
             "class": "UNVERIFIABLE_COMPONENT"}],
        "forensics": {"rank_ic_t": 0.24, "max_drawdown": -0.22,
                      "annualized_vol": 0.21, "top5_contribution_share": 0.3,
                      "equal_dollar_implies_unequal_risk": True}})
    _w(run_dir / "manual_risk_preview.json",
       {"status": "WITHHELD_NO_ROBUST_EVIDENCE"})
    _w(run_dir / "run_manifest.json", {"safety": {}})
    (run_dir / "overlay_results.csv").write_text(
        "overlay,net_annualized_return,annualized_vol,max_drawdown,worst_20d,"
        "spy_excess_annualized,avg_cash_weight\n"
        "CURRENT_CONTROL,0.05,0.21,-0.22,-0.08,0.005,0.047\n", encoding="utf-8")
    (run_dir / "alpha_evidence_decisions.jsonl").write_text(
        json.dumps({"experiment_id": "e1", "decision": "REJECT_WEAK_EVIDENCE"})
        + "\n", encoding="utf-8")
    (run_dir / "remaining_data_gaps.jsonl").write_text(
        json.dumps({"gap": "POINT_IN_TIME_FUNDAMENTALS"}) + "\n",
        encoding="utf-8")
    cfg = dict(env["cfg"])
    cfg["stage7_recovery_root"] = str(rec_root)
    r = rt.Runtime(cfg, drivers=FakeDrivers(), email_sender=FakeSender(),
                   clock=_clock())
    res = r.run_report_only(label="morning")
    html = Path(res.detail["report_html"]).read_text(encoding="utf-8")
    body, _, appendix = html.partition(rr.BODY_APPENDIX_SEPARATOR)
    # Plain-English verdict in the body; the raw run id is a single reference in
    # the compact appendix; no machine tokens leak into the body.
    assert rr.VERDICT_SENTENCE in body
    assert rid in appendix and rid not in body
    assert "NEED_MORE_EVIDENCE" not in body
    assert "UNVERIFIABLE_COMPONENT" not in html
    manifest = json.loads(Path(res.detail["report_html"]).with_name(
        "report_manifest.json").read_text(encoding="utf-8"))
    assert manifest["recovery_readiness"]["disposition"] == "NEED_MORE_EVIDENCE"


def test_stage7_recovery_report_absent_is_controlled(env):
    # With no recovery root configured, the report renders a controlled empty
    # state — never an error, never a fabricated disposition.
    r = rt.Runtime(dict(env["cfg"]), drivers=FakeDrivers(),
                   email_sender=FakeSender(), clock=_clock())
    res = r.run_report_only(label="morning")
    html = Path(res.detail["report_html"]).read_text(encoding="utf-8")
    assert rr.VERDICT_SENTENCE in html
    assert "No new recovery ideas were evaluated this cycle" in html


# --------------------------------------------------------------------------- #
# Stage 7.1 — executive email, signed metrics, translation, cadence, shadows.
# --------------------------------------------------------------------------- #
def _make_recovery_pkg(base: Path, *, as_of="2026-07-29",
                       disposition="NEED_MORE_EVIDENCE", forward_shadows=None,
                       upstream_fp="fp_current", decisions=None) -> str:
    rec_root = base / "recovery"
    rid = "stage7_t_%s" % as_of.replace("-", "")
    rd = rec_root / "runs" / rid
    rd.mkdir(parents=True, exist_ok=True)
    _w(rec_root / "latest.json",
       {"run_id": rid, "as_of": as_of, "disposition": disposition,
        "terminal": "ALPHA_AGENT_STAGE7_READY",
        "holdings_source": "RECONSTRUCTED_CHAMPION_PROXY"})
    _w(rd / "recovery_disposition.json",
       {"disposition": disposition,
        "rationale": "fundamental leg unverifiable; forward sample too small"})
    _w(rd / "champion_reconstruction.json",
       {"champion_model": "fundamental_momentum_50_50_v1",
        "classification": [
            {"component": "portfolio_construction",
             "class": "EXACT_RECONSTRUCTION"},
            {"component": "fundamental_leg_point_in_time",
             "class": "UNVERIFIABLE_COMPONENT"}],
        "forensics": {"rank_ic_t": 0.23, "max_drawdown": -0.25,
                      "annualized_vol": 0.27, "top5_contribution_share": 0.16,
                      "equal_dollar_implies_unequal_risk": True}})
    _w(rd / "manual_risk_preview.json",
       {"status": "WITHHELD_NO_ROBUST_EVIDENCE"})
    _w(rd / "run_manifest.json",
       {"safety": {}, "upstream_fingerprint": upstream_fp,
        "forward_shadows": forward_shadows or []})
    (rd / "overlay_results.csv").write_text(
        "overlay,net_annualized_return,annualized_vol,max_drawdown,worst_20d,"
        "spy_excess_annualized,avg_cash_weight,turnover_annualized,"
        "cost_drag_annualized\n"
        "CURRENT_CONTROL,0.05,0.28,-0.37,-0.31,0.19,0.047,0.5,0.005\n"
        "MARKET_REGIME_CASH_OVERLAY,0.04,0.24,-0.29,-0.28,0.17,0.121,0.6,0.006\n"
        "PORTFOLIO_VOL_TARGET_20,0.03,0.17,-0.23,-0.23,0.05,0.354,0.4,0.004\n",
        encoding="utf-8")
    (rd / "alpha_evidence_decisions.jsonl").write_text(
        "".join(json.dumps(d) + "\n" for d in (decisions or [
            {"experiment_id": "e1", "decision": "REJECT_WEAK_EVIDENCE"},
            {"experiment_id": "e2", "decision": "REJECT_INSTABILITY"}])),
        encoding="utf-8")
    (rd / "remaining_data_gaps.jsonl").write_text(
        json.dumps({"gap": "POINT_IN_TIME_FUNDAMENTALS"}) + "\n",
        encoding="utf-8")
    return str(rec_root)


def test_signed_money_pct_pp_formatting():
    assert rr.fmt_signed_money(123.45) == "+$123.45"
    assert rr.fmt_signed_money(-123.45) == "-$123.45"
    assert rr.fmt_signed_money(0) == "+$0.00"
    assert rr.fmt_pct(0.5) == "+0.50%"
    assert rr.fmt_pct(-0.5) == "-0.50%"
    assert rr.fmt_pp(0.53) == "+0.53 pp"
    assert rr.fmt_pp(-0.53) == "-0.53 pp"
    assert rr.fmt_money(1234.5) == "$1,234.50"
    assert rr.fmt_money(None) == "Not available"


def test_negative_values_cannot_lose_their_sign():
    assert rr.fmt_signed_money(-0.01).startswith("-")
    assert rr.fmt_pct(-0.01).startswith("-")
    assert rr.fmt_pp(-0.01).startswith("-")
    assert rr._fmt_ret(-0.01).startswith("-")
    # A positive value is explicitly '+', never bare.
    assert rr.fmt_signed_money(0.01).startswith("+")
    assert rr.fmt_pct(0.01).startswith("+")


def test_no_color_only_negative_indicator(env):
    # The env ledger has a negative daily P&L; the sign must be in the TEXT.
    res = _runtime(env).run_research(label="morning")
    html = Path(res.detail["report_html"]).read_text(encoding="utf-8")
    text = Path(res.detail["report_text"]).read_text(encoding="utf-8")
    assert "-$391.68" in html
    assert "-$391.68" in text  # plain text carries the sign with no colour


def test_exactly_one_action_today(env):
    res = _runtime(env).run_research(label="morning")
    html = Path(res.detail["report_html"]).read_text(encoding="utf-8")
    present = [s for s in rr.ACTION_STATES if s in html]
    assert len(present) == 1
    # Healthy env (no degrade, feeds partial-not-zero, gate 0 triggered).
    assert present[0] == rr.ACTION_NO_TRADE


def test_hold_is_not_called_proof_of_acceptable_risk(env):
    res = _runtime(env).run_research(label="morning")
    html = Path(res.detail["report_html"]).read_text(encoding="utf-8")
    assert "not a statement that the portfolio&#x27;s absolute risk is " \
           "acceptable" in html or "absolute risk is acceptable" in html


def test_action_data_attention_when_degraded(env):
    d = FakeDrivers(research=_norm(rc.COMPONENT_STAGE3, ok=False,
                                   status="ALPHA_AGENT_STAGE3_BLOCKED"))
    res = _runtime(env, drivers=d).run_research(label="post_close")
    html = Path(res.detail["report_html"]).read_text(encoding="utf-8")
    assert rr.ACTION_DATA_ATTENTION in html
    present = [s for s in rr.ACTION_STATES if s in html]
    assert len(present) == 1


def test_machine_status_translation():
    assert "too weak" in rr.translate("REJECT_WEAK_EVIDENCE").lower()
    assert "consistently" in rr.translate("REJECT_INSTABILITY").lower()
    assert "neither confirmed nor rejected" in \
        rr.translate("NEED_MORE_EVIDENCE").lower()
    assert "cannot yet be validated" in \
        rr.translate("UNVERIFIABLE_COMPONENT").lower()
    assert "not yet available" in \
        rr.translate("ALPHA_AGENT_STAGE5_DATA_HOLD").lower()
    assert rr.translate("SOME_UNKNOWN_TOKEN") == "SOME_UNKNOWN_TOKEN"


def test_no_internal_tokens_in_main_sections(env):
    cfg = dict(env["cfg"])
    cfg["stage7_recovery_root"] = _make_recovery_pkg(env["base"])
    r = rt.Runtime(cfg, drivers=FakeDrivers(), email_sender=FakeSender(),
                   clock=_clock())
    res = r.run_report_only(label="morning")
    html = Path(res.detail["report_html"]).read_text(encoding="utf-8")
    body, _, appendix = html.partition(rr.BODY_APPENDIX_SEPARATOR)
    assert appendix  # the compact audit appendix exists after the separator
    # No raw machine tokens appear in the plain-English executive body at all.
    for token in ("NEED_MORE_EVIDENCE", "UNVERIFIABLE_COMPONENT",
                  "REJECT_WEAK_EVIDENCE", "KEEP_FOR_RESEARCH"):
        assert token not in body, token
    # The plain-English verdict appears in the body, exactly once.
    assert rr.VERDICT_SENTENCE in body
    assert html.count(rr.VERDICT_SENTENCE) == 1


def test_appendix_is_compact_with_no_paths(env):
    res = _runtime(env).run_research(label="post_close")
    html = Path(res.detail["report_html"]).read_text(encoding="utf-8")
    body, _, appendix = html.partition(rr.BODY_APPENDIX_SEPARATOR)
    assert appendix
    # The compact appendix carries exactly the five allowed audit lines and one
    # research-run reference — no per-stage run-id list, no evidence paths.
    for label in ("Generated", "Market data through", "Latest research run",
                  "Data quality", "Safety"):
        assert label in appendix, label
    # No local file path anywhere in the email (body or appendix).
    for bad in ("C:\\", "D:\\", "/runs/", "\\runs\\", ".paper_trader"):
        assert bad not in html, bad
    # The per-stage evidence-path list of the old appendix is gone.
    assert "Stage 3.5 news/RSS root" not in html


def test_schedule_state_not_reported_on_when_disabled(env):
    # The safety band shows the RESOLVED schedule state, never a hardcoded ON.
    res = _runtime(env).run_research(label="morning")
    html = Path(res.detail["report_html"]).read_text(encoding="utf-8")
    assert "RESEARCH SCHEDULE: ON" not in html
    for flag in ("AUTOMATIC SCHEDULE:", "TRADING AUTOMATION: OFF",
                 "BROKER EXECUTION: OFF", "PAPER ONLY"):
        assert flag in html, flag
    assert ">AUTOMATION OFF<" not in html


def test_email_html_and_text_have_executive_sections(env):
    res = _runtime(env).run_research(label="morning")
    html = Path(res.detail["report_html"]).read_text(encoding="utf-8")
    text = Path(res.detail["report_text"]).read_text(encoding="utf-8")
    for h in ("1. Bottom line", "2. Your action today", "3. Portfolio today",
              "4. Research progress", "5. Risk experiments",
              "6. Data and system issues"):
        assert h in html, h
    # No seventh numbered section in the primary body.
    assert "7. " not in html.partition(rr.BODY_APPENDIX_SEPARATOR)[0]
    for h in ("1. BOTTOM LINE", "2. YOUR ACTION TODAY", "3. PORTFOLIO TODAY",
              "4. RESEARCH PROGRESS", "5. RISK EXPERIMENTS",
              "6. DATA AND SYSTEM ISSUES"):
        assert h in text, h


def test_no_dialogs_in_email_html(env):
    res = _runtime(env).run_research(label="morning")
    html = Path(res.detail["report_html"]).read_text(encoding="utf-8")
    for bad in ("alert(", "confirm(", "prompt("):
        assert bad not in html


def test_stage6_window_and_universe_corrected(env):
    base = env["base"]
    bf_root = base / "backfill"
    rid = "stage6_t1"
    rd = bf_root / "runs" / rid
    rd.mkdir(parents=True)
    _w(bf_root / "latest.json",
       {"run_id": rid, "terminal": "ALPHA_AGENT_STAGE6_PARTIAL",
        "status": "TEMPLATES_UNLOCKED_WITH_REMAINING_GAPS",
        "records_written": 1531490, "data_version": "dv6_test"})
    _w(rd / "run_manifest.json", {"terminal": "ALPHA_AGENT_STAGE6_PARTIAL"})
    _w(rd / "stage6_input.json",
       {"date_start": "2015-01-01", "date_end": "2026-07-29"})
    (rd / "coverage_after.csv").write_text(
        "family,record_type,provider,min_effective_date,max_effective_date,"
        "unique_tickers,record_count,point_in_time_usable,survivorship_safe\n"
        "prices,MARKET_BAR,norgate_local,2015-01-02,2026-07-29,572,1530376,"
        "false,true\n"
        "universe_membership,UNIVERSE_MEMBERSHIP,norgate_local,2015-01-02,"
        "2026-07-29,548,640,false,true\n", encoding="utf-8")
    (rd / "reopened_hypotheses.jsonl").write_text("", encoding="utf-8")
    (rd / "unresolved_data_gaps.jsonl").write_text("", encoding="utf-8")
    cfg = dict(env["cfg"])
    cfg["stage6_backfill_root"] = str(bf_root)
    r = rt.Runtime(cfg, drivers=FakeDrivers(), email_sender=FakeSender(),
                   clock=_clock())
    res = r.run_report_only(label="morning")
    # The Stage 6 window/universe repair is verified on the report MODEL (the
    # compact email no longer renders Stage 6 internals; they live in the
    # observatory). The invalid "? .. ?" / 0-of-0 must never appear.
    man = json.loads(Path(res.detail["report_html"]).with_name(
        "report_manifest.json").read_text(encoding="utf-8"))
    hr = man["historical_readiness"]
    assert hr["date_start"] == "2015-01-01" and hr["date_end"] == "2026-07-29"
    assert hr["universe_full_size"] == 572 and hr["universe_size"] == 548


def test_latest_stage7_results_and_stale_age(env):
    cfg = dict(env["cfg"])
    # as_of far in the past → stale age shown.
    cfg["stage7_recovery_root"] = _make_recovery_pkg(env["base"],
                                                     as_of="2026-06-01")
    r = rt.Runtime(cfg, drivers=FakeDrivers(), email_sender=FakeSender(),
                   clock=_clock())  # clock date is 2026-07-29
    res = r.run_report_only(label="morning")
    html = Path(res.detail["report_html"]).read_text(encoding="utf-8")
    # The latest-research-run reference in the compact appendix shows the age and
    # the STALE flag when the verdict is old.
    assert "day(s) old" in html and "STALE" in html


def test_cadence_reuse_delta_and_friday():
    ro = rt.ro_recovery
    friday = ro.stage7_cadence_decision(
        label="post_close", weekday=4, current_fingerprint="x",
        latest_manifest={"upstream_fingerprint": "x"})
    assert friday["action"] == ro.CAD_RUN_FULL and friday["weekly"] is True
    delta = ro.stage7_cadence_decision(
        label="morning", weekday=1, current_fingerprint="y",
        latest_manifest={"upstream_fingerprint": "x"})
    assert delta["action"] == ro.CAD_RUN_DELTA
    reuse = ro.stage7_cadence_decision(
        label="morning", weekday=1, current_fingerprint="x",
        latest_manifest={"upstream_fingerprint": "x"})
    assert reuse["action"] == ro.CAD_REUSE
    assert "No new research evidence" in reuse["reason"]
    none = ro.stage7_cadence_decision(
        label="morning", weekday=1, current_fingerprint="x",
        latest_manifest=None)
    assert none["action"] == ro.CAD_NONE


def test_no_new_evidence_phrase_in_what_changed():
    # When nothing material changed, the change note collapses to one plain
    # sentence — no wall of $0.00 / 0.00% / 0.00 pp lines (Stage 7.2 defect #6).
    model = {"scorecard": rr.scorecard({"nav": 100.0}),
             "prior": {"scorecard": {"nav": 100.0}}, "no_new_evidence": True}
    bullets = rr.what_changed(model)
    assert bullets == ["No meaningful portfolio or research change occurred "
                       "since the previous report."]


def test_stage7_launch_is_idempotent_and_gated(env):
    launches = []
    cfg = dict(env["cfg"])
    cfg["stage7_recovery_root"] = _make_recovery_pkg(env["base"],
                                                     upstream_fp="OLD")
    cfg["cadence"] = dict(cfg["cadence"])
    cfg["cadence"]["stage7_launch_in_cycle"] = True
    r = rt.Runtime(cfg, drivers=FakeDrivers(), email_sender=FakeSender(),
                   clock=_clock(),
                   stage7_launcher=lambda dec: launches.append(dec))
    # Upstream fingerprint differs from the package's 'OLD' → a refresh is due.
    r.run_research(label="morning")
    assert len(launches) == 1  # launched once
    r.run_research(label="morning")  # same cycle → idempotent, no relaunch
    assert len(launches) == 1


def test_stage7_not_launched_when_disabled(env):
    launches = []
    cfg = dict(env["cfg"])
    cfg["stage7_recovery_root"] = _make_recovery_pkg(env["base"],
                                                     upstream_fp="OLD")
    # stage7_launch_in_cycle defaults to False.
    r = rt.Runtime(cfg, drivers=FakeDrivers(), email_sender=FakeSender(),
                   clock=_clock(),
                   stage7_launcher=lambda dec: launches.append(dec))
    r.run_research(label="morning")
    assert launches == []


def test_forward_shadows_exactly_three_and_read_only(env):
    fwd = [{"overlay": "CURRENT_CONTROL", "invested_gross": 0.95, "cash": 0.05,
            "forward_scale": 1.0},
           {"overlay": "MARKET_REGIME_CASH_OVERLAY", "invested_gross": 0.475,
            "cash": 0.525, "forward_scale": 0.5},
           {"overlay": "PORTFOLIO_VOL_TARGET_20", "invested_gross": 0.6,
            "cash": 0.4, "forward_scale": 0.63}]
    cfg = dict(env["cfg"])
    cfg["stage7_recovery_root"] = _make_recovery_pkg(env["base"],
                                                     forward_shadows=fwd)
    before = _fingerprint(env["ledger"])
    r = rt.Runtime(cfg, drivers=FakeDrivers(), email_sender=FakeSender(),
                   clock=_clock())
    res = r.run_report_only(label="morning")
    after = _fingerprint(env["ledger"])
    assert before == after  # forward shadows never mutate any ledger
    man = json.loads(Path(res.detail["report_html"]).with_name(
        "report_manifest.json").read_text(encoding="utf-8"))
    shadows = man["forward_shadows"]
    assert shadows is not None and len(shadows) == 3
    names = {s["overlay"] for s in shadows}
    assert names == {"CURRENT_CONTROL", "MARKET_REGIME_CASH_OVERLAY",
                     "PORTFOLIO_VOL_TARGET_20"}
    # The control scale is 1; the vol-target is scaled below 1 (de-risked).
    by = {s["overlay"]: s for s in shadows}
    assert by["CURRENT_CONTROL"]["scale"] == 1.0
    assert by["PORTFOLIO_VOL_TARGET_20"]["scale"] < 1.0
    html = Path(res.detail["report_html"]).read_text(encoding="utf-8")
    assert "No shadow portfolio changes the active paper portfolio." in html


def test_executive_test_email_subject_and_single_send(env):
    sender = FakeSender(rc.EMAIL_SENT, message_id="gmail_msgid_123")
    r = rt.Runtime(dict(env["cfg"]), drivers=FakeDrivers(),
                   email_sender=sender, clock=_clock())
    res = r.run_report_only(label="morning", exec_test=True)
    assert res.detail["subject"] == \
        "TEST — Alpha Agent Executive Research Brief — 2026-07-29"
    assert res.email_status == rc.EMAIL_SENT
    assert res.email_message_id == "gmail_msgid_123"
    assert len(sender.calls) == 1
    # A second executive test on the same date does not send again.
    res2 = r.run_report_only(label="morning", exec_test=True)
    assert len(sender.calls) == 1
    assert res2.email_status == rc.EMAIL_ALREADY_SENT


def test_exec_test_creates_no_orders_or_ledger_change(env):
    before = _fingerprint(env["ledger"])
    r = rt.Runtime(dict(env["cfg"]), drivers=FakeDrivers(),
                   email_sender=FakeSender(rc.EMAIL_SENT), clock=_clock())
    r.run_report_only(label="morning", exec_test=True)
    after = _fingerprint(env["ledger"])
    assert before == after


# --------------------------------------------------------------------------- #
# Stage 7.2 — executive-brief quality (deterministic renderer-level tests).
# --------------------------------------------------------------------------- #
def _v2_model(**over):
    """A live-shaped executive model for the Stage 7.2 quality tests."""
    m = {
        "report_title": "Alpha Agent Manual Executive Research Brief",
        "cycle_label": "manual", "cycle_date": "2026-07-30",
        "generated_at": "2026-07-30T18:40:00+00:00",
        "subject": "TEST v2", "schedule_status": "Off",
        "market_data_through": "2026-07-29",
        "status_flags": list(rr.STATUS_FLAGS),
        "scorecard": rr.scorecard({
            "nav": 98125.23, "daily_pnl": -443.45, "daily_return_pct": -0.45,
            "cumulative_pnl": -1874.77, "cumulative_return_pct": -1.87,
            "drawdown_pct": -1.87, "spy_cumulative_pct": -2.40,
            "cumulative_excess_pp": 0.53}),
        "paper_book": {"nav": 98125.23},
        "prior": {"scorecard": {"nav": 98125.23, "cumulative_return_pct": -1.87,
                                "cumulative_excess_pp": 0.53},
                  "disposition": "NEED_MORE_EVIDENCE"},
        "recovery_readiness": {
            "run_id": "stage7_f2d4dfc3895a3667",
            "disposition": "NEED_MORE_EVIDENCE",
            "champion_model": "fundamental_momentum_50_50_v1",
            "campaign_experiments": 7, "campaign_keep_for_research": 0,
            "campaign_decision_counts": {"REJECT_WEAK_EVIDENCE": 5,
                                         "REJECT_INSTABILITY": 2}},
        "stage5_could_not_run": 3,
        "risk_and_shadow": {"shadows": [
            {"overlay": "CURRENT_CONTROL", "cumulative_return": -0.0187,
             "drawdown": -0.0187, "realized_vol": None, "cash": 0.047,
             "spy_excess": 0.0053, "observations": 6},
            {"overlay": "MARKET_REGIME_CASH_OVERLAY", "cumulative_return": -0.0187,
             "drawdown": -0.0187, "realized_vol": None, "cash": 0.047,
             "spy_excess": 0.0053, "observations": 6},
            {"overlay": "PORTFOLIO_VOL_TARGET_20", "cumulative_return": -0.0078,
             "drawdown": -0.0078, "realized_vol": None, "cash": 0.603,
             "spy_excess": 0.0162, "observations": 6}]},
        "historical_readiness_summary": [
            {"label": "Price history", "status": "READY", "note": "x"},
            {"label": "Point-in-time fundamentals", "status": "NOT READY",
             "note": "x"},
            {"label": "Earnings history", "status": "NOT READY", "note": "x"},
            {"label": "Historical sector classifications", "status": "NOT READY",
             "note": "x"}],
        "news_rss": {"healthy": 7, "enabled": 11, "failed_feeds": []},
        "source_agent_health": {"provider_ok": True, "ledgers_unchanged": True,
                                "stage7_age_note": "1 day(s) old"},
        "evidence": {"run_id": "runtime_x"},
    }
    m.update(over)
    return m


def _v2_body(model):
    return rr.render_html(model).partition(rr.BODY_APPENDIX_SEPARATOR)[0]


def _v2_text_body(model):
    return rr.render_text(model).partition(rr.BODY_APPENDIX_SEPARATOR)[0]


def test_v2_periods_not_mixed_in_one_metric_statement():
    text = rr.render_text(_v2_model())
    # Each dollar is paired with the percent from the SAME period.
    assert "$443.45 (-0.45%) today" in text
    assert "$1,874.77 (-1.87%) since launch" in text
    # A today dollar never pairs with a since-inception percent (or vice versa).
    assert "$443.45 (-1.87%)" not in text
    assert "$1,874.77 (-0.45%)" not in text


def test_v2_schedule_off_when_all_tasks_disabled():
    assert rr.schedule_status({"a": "Disabled", "b": "Disabled",
                               "c": "Disabled", "d": "Disabled"}) == "Off"
    assert rr.schedule_status({"a": "Disabled", "b": "Enabled"}) == "On"
    assert rr.schedule_status({"a": None}) == "Not verified"
    assert rr.schedule_status({}) == "Not verified"
    html = rr.render_html(_v2_model(schedule_status="Off"))
    text = rr.render_text(_v2_model(schedule_status="Off"))
    assert "AUTOMATIC SCHEDULE: OFF" in html
    assert "RESEARCH SCHEDULE: ON" not in html
    assert "Automatic research schedule: Off" in text


def test_v2_body_has_no_machine_status_tokens():
    body = _v2_body(_v2_model()) + _v2_text_body(_v2_model())
    for tok in ("stage7_", "stage5_", "stage6_", "REJECT_", "KEEP_FOR_RESEARCH",
                "NEED_MORE_EVIDENCE", "DATA_HOLD", "ALPHA_AGENT_STAGE",
                "UNVERIFIABLE_COMPONENT"):
        assert tok not in body, tok


def test_v2_email_has_no_local_file_paths():
    m = _v2_model()
    html = rr.render_html(m)
    text = rr.render_text(m)
    for bad in ("C:\\", "D:\\", "/runs/", "\\runs\\", ".paper_trader",
                "Stock_Prediction_app_data"):
        assert bad not in html and bad not in text, bad


def test_v2_research_counts_reconcile_exactly():
    recon = rr.research_decision_reconciliation(
        {"campaign_experiments": 7,
         "campaign_decision_counts": {"REJECT_WEAK_EVIDENCE": 5,
                                      "REJECT_INSTABILITY": 2}})
    assert recon["evaluated"] == 7
    assert (recon["rejected"] + recon["retained"] + recon["could_not_run"]
            + recon["other"] == recon["accounted"] == recon["evaluated"])
    assert recon["reconciles"] is True
    assert recon["promoted"] == 0
    # A 'could not run' campaign token is described in plain English, never leaked
    # as a raw token, and is counted in its own bucket.
    r2 = rr.research_decision_reconciliation(
        {"campaign_experiments": 3,
         "campaign_decision_counts": {"REJECT_WEAK_EVIDENCE": 1,
                                      "DATA_HOLD": 2}})
    assert r2["rejected"] == 1 and r2["could_not_run"] == 2
    assert all("DATA_HOLD" not in c["phrase"] for c in r2["categories"])


def test_v2_stage5_and_stage7_counts_not_conflated():
    recon = rr.research_decision_reconciliation(
        {"campaign_experiments": 7,
         "campaign_decision_counts": {"REJECT_WEAK_EVIDENCE": 7}},
        stage5_could_not_run=3)
    assert recon["evaluated"] == 7            # Stage 7 recovery ideas evaluated
    assert recon["stage5_could_not_run"] == 3  # Stage 5 data-holds, kept apart
    body = _v2_body(_v2_model())
    assert "7 recovery idea(s) were evaluated" in body
    assert "3 study(ies) that could not run" in body
    # The Stage 5 count is never added into the evaluated count.
    assert "10 recovery" not in body


def test_v2_verdict_appears_exactly_once():
    html = rr.render_html(_v2_model())
    text = rr.render_text(_v2_model())
    assert html.count(rr.VERDICT_SENTENCE) == 1
    assert text.count(rr.VERDICT_SENTENCE) == 1


def test_v2_zero_changes_are_suppressed():
    body = _v2_body(_v2_model())  # prior == current -> nothing material moved
    assert ("No meaningful portfolio or research change occurred since the "
            "previous report.") in body
    assert "$0.00" not in body
    assert "0.00%" not in body
    assert "0.00 pp" not in body


def test_v2_benchmark_differences_use_percentage_points():
    m = _v2_model()
    body = _v2_body(m)
    assert "percentage points" in body                 # bottom-line prose
    assert "+0.53 pp" in rr.render_html(m)              # shadow vs-SPY column
    assert m["scorecard"]["formatted"]["cumulative_excess_pp"].endswith(" pp")


def test_v2_cash_percentages_have_no_plus_sign():
    text = rr.render_text(_v2_model())
    assert "60.3%" in text
    assert "+60.3%" not in text and "+4.7%" not in text
    assert rr.fmt_cash_pct(0.603) == "60.3%"
    assert not rr.fmt_cash_pct(0.603).startswith("+")


def test_v2_realized_vol_becomes_observation_explanation():
    m = _v2_model()
    text = rr.render_text(m)
    assert "realized volatility cannot be measured yet" in text
    assert "Only 6 trading day(s) are available" in text
    assert "Realized vol" not in text  # no bare 'Not available' vol column


def test_v2_body_passes_forbidden_jargon_scan():
    m = _v2_model()
    body = (_v2_body(m) + _v2_text_body(m)).lower()
    hits = [j for j in rr.FORBIDDEN_JARGON if j in body]
    assert hits == [], hits


def test_v2_valid_html_and_complete_text_alternative():
    m = _v2_model()
    html = rr.render_html(m)
    text = rr.render_text(m)
    assert html.startswith("<!doctype html>")
    assert html.rstrip().endswith("</html>")
    assert html.count("<body") == 1 and html.count("</body>") == 1
    for h in ("1. BOTTOM LINE", "2. YOUR ACTION TODAY", "3. PORTFOLIO TODAY",
              "4. RESEARCH PROGRESS", "5. RISK EXPERIMENTS",
              "6. DATA AND SYSTEM ISSUES", rr.BODY_APPENDIX_SEPARATOR):
        assert h in text, h
    assert len(text.strip()) > 200


def test_v2_no_browser_dialogs_in_email():
    html = rr.render_html(_v2_model())
    for d in ("alert(", "confirm(", "prompt("):
        assert d not in html


def test_v2_render_is_pure_no_operational_mutation(env):
    before = _fingerprint(env["ledger"])
    m = _v2_model()
    rr.render_html(m)
    rr.render_text(m)
    rr.report_manifest(m, "h", "t")
    assert _fingerprint(env["ledger"]) == before


# --------------------------------------------------------------------------- #
# Gmail SMTP (App Password) transport — WS1-WS5.
#
# Gmail SMTP is the PRIMARY, active email transport (the Gmail-API OAuth
# transport is retired, disabled by config, and never attempted when SMTP is
# selected). Delivery uses smtplib over STARTTLS with a dedicated Google App
# Password stored ONLY as a Windows DPAPI blob. Every SMTP client here is a FAKE
# — no real network, TLS handshake, authentication or email is ever performed,
# and no real App Password is ever used.
# --------------------------------------------------------------------------- #
import smtplib as _smtplib_real   # noqa: E402
import socket as _socket_real     # noqa: E402
import ssl as _ssl_real           # noqa: E402

# Deterministic, obviously-fake 16-char App Password. Never real; never sent to a
# real server (all SMTP clients in these tests are fakes).
_APP_PASSWORD = "ZZ16charFAKE0000"


def _load_smtp_sender_module():
    return _load_module(_SMTP_SENDER_PY, "aa_send_smtp_test")


def _load_smtp_diag_module():
    return _load_module(_SMTP_DIAG_PY, "aa_diag_smtp_test")


def _smtp_job(tmp_path, *, subject="Alpha Agent SMTP Report",
              text="plain body", html="<h1>html body</h1>", attach=False):
    (tmp_path / "report.html").write_text(html, encoding="utf-8")
    (tmp_path / "report.txt").write_text(text, encoding="utf-8")
    job = {"recipient": "binisti@gmail.com", "subject": subject,
           "html_path": str(tmp_path / "report.html"),
           "text_path": str(tmp_path / "report.txt")}
    if attach:
        job["attach_markdown"] = [str(tmp_path / "report.txt")]
    return job


def _smtp_recorder(*, on_connect=None, on_starttls=None, on_login=None,
                   on_send=None):
    """Return (record, factory). The factory is a fake smtplib.SMTP that records
    the call order and never touches the network."""
    record = {"calls": [], "debuglevel": None, "constructed": 0}

    class _FakeSMTP:
        def __init__(self, host, port, timeout=None):
            record["host"] = host
            record["port"] = port
            record["timeout"] = timeout
            record["constructed"] += 1
            if on_connect is not None:
                raise on_connect

        def set_debuglevel(self, level):
            record["debuglevel"] = level

        def ehlo(self, *a, **k):
            record["calls"].append("ehlo")

        def starttls(self, context=None):
            record["calls"].append("starttls")
            record["tls_context"] = context
            if on_starttls is not None:
                raise on_starttls

        def login(self, user, password):
            record["calls"].append("login")
            record["login_user"] = user
            record["login_password"] = password
            if on_login is not None:
                raise on_login

        def send_message(self, message):
            record["calls"].append("send")
            record["sent_message"] = message
            if on_send is not None:
                raise on_send

        def noop(self):
            record["calls"].append("noop")

        def quit(self):
            record["calls"].append("quit")

    return record, _FakeSMTP


# WS5.6 — SMTP sender uses port 587 (module + wrapper + runtime defaults).
def test_smtp_default_port_is_587():
    mod = _load_smtp_sender_module()
    assert mod._DEFAULT_SMTP_PORT == 587
    assert mod._DEFAULT_SMTP_HOST == "smtp.gmail.com"
    ps1 = _SMTP_SENDER_PS1.read_text(encoding="utf-8")
    assert "$SmtpPort = 587" in ps1
    cfg = rc.load_config(_REPO / "configs" / "alpha_agent" /
                         "stage4_runtime.json")
    assert cfg["email"]["smtp_port"] == 587
    assert cfg["email"]["smtp_host"] == "smtp.gmail.com"
    assert cfg["email"]["smtp_security"] == "starttls"


# WS5.8 — builds a valid plain-text + HTML multipart/alternative message.
def test_smtp_builds_multipart_alternative(tmp_path):
    mod = _load_smtp_sender_module()
    msg, mid, err = mod.build_message(
        _smtp_job(tmp_path, text="plain here", html="<p>html here</p>"),
        "binisti@gmail.com")
    assert err is None
    assert msg.get_content_type() == "multipart/alternative"
    plain = msg.get_body(preferencelist=("plain",)).get_content()
    html = msg.get_body(preferencelist=("html",)).get_content()
    assert "plain here" in plain and plain.strip()
    assert "html here" in html and html.strip()


# WS5.9 — UTF-8 subject and body survive intact.
def test_smtp_utf8_subject_and_body(tmp_path):
    mod = _load_smtp_sender_module()
    subject = "Café — Über résumé ✓"
    msg, mid, err = mod.build_message(
        _smtp_job(tmp_path, subject=subject, text="café ✓", html="<p>über</p>"),
        "binisti@gmail.com")
    assert err is None
    assert str(msg["Subject"]) == subject
    assert isinstance(msg.as_bytes(), bytes)   # serialises without error
    plain = msg.get_body(preferencelist=("plain",)).get_content()
    assert "café ✓" in plain


# WS5.10 — an RFC 5322 Message-ID is generated and returned.
def test_smtp_generates_and_returns_message_id(tmp_path):
    mod = _load_smtp_sender_module()
    msg, mid, err = mod.build_message(_smtp_job(tmp_path), "binisti@gmail.com")
    assert err is None
    assert mid and mid.startswith("<") and mid.endswith(">")
    assert "@gmail.com>" in mid
    assert msg["Message-ID"] == mid
    assert msg["Date"] and msg["From"] == "binisti@gmail.com"


# WS5.7 — STARTTLS happens BEFORE authentication (and before any send).
def test_smtp_starttls_before_auth_and_send(tmp_path):
    mod = _load_smtp_sender_module()
    msg, mid, _ = mod.build_message(_smtp_job(tmp_path), "binisti@gmail.com")
    record, factory = _smtp_recorder()
    status, _diag = mod.deliver(
        msg, account="binisti@gmail.com", app_password=_APP_PASSWORD,
        host="smtp.gmail.com", port=587, timeout=5, smtp_factory=factory,
        ssl_context=_ssl_real.create_default_context())
    assert status == mod.EMAIL_SENT
    calls = record["calls"]
    assert "starttls" in calls and "login" in calls and "send" in calls
    assert calls.index("starttls") < calls.index("login")
    assert calls.index("starttls") < calls.index("send")
    assert record["port"] == 587
    assert record["debuglevel"] is None   # SMTP debug output never enabled


# WS5.14 — a successful send maps to EMAIL_SENT (+ returned Message-ID).
def test_smtp_success_maps_to_email_sent(monkeypatch, tmp_path):
    mod = _load_smtp_sender_module()
    record, factory = _smtp_recorder()
    monkeypatch.setattr(mod.smtplib, "SMTP", factory)
    monkeypatch.setattr(mod.sys, "stdin",
                        io.StringIO(_APP_PASSWORD + "\n"))
    job_path = tmp_path / "job.json"
    job_path.write_text(json.dumps(_smtp_job(tmp_path)), encoding="utf-8")
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        code = mod.main(["--job-path", str(job_path),
                         "--account", "binisti@gmail.com",
                         "--smtp-host", "smtp.gmail.com",
                         "--smtp-port", "587", "--timeout-seconds", "5"])
    out = buf.getvalue()
    result = json.loads([ln for ln in out.splitlines()
                         if ln.strip().startswith("{")][-1])
    assert code == 0
    assert result["status"] == "EMAIL_SENT"
    assert result["transport"] == "gmail_smtp"
    assert result["message_id"] and result["message_id"].startswith("<")


# WS5.11 — an authentication rejection (SMTP 535) maps correctly.
def test_smtp_auth_rejection_maps(tmp_path):
    mod = _load_smtp_sender_module()
    msg, _mid, _ = mod.build_message(_smtp_job(tmp_path), "binisti@gmail.com")
    err = _smtplib_real.SMTPAuthenticationError(
        535, b"5.7.8 Username and Password not accepted")
    record, factory = _smtp_recorder(on_login=err)
    status, _diag = mod.deliver(
        msg, account="binisti@gmail.com", app_password=_APP_PASSWORD,
        host="smtp.gmail.com", port=587, timeout=5, smtp_factory=factory,
        ssl_context=_ssl_real.create_default_context())
    assert status == mod.EMAIL_SMTP_AUTHENTICATION_REJECTED
    assert "send" not in record["calls"]   # never sends after auth failure


# WS5.12 — a STARTTLS failure maps correctly.
def test_smtp_tls_failure_maps(tmp_path):
    mod = _load_smtp_sender_module()
    msg, _mid, _ = mod.build_message(_smtp_job(tmp_path), "binisti@gmail.com")
    record, factory = _smtp_recorder(on_starttls=_ssl_real.SSLError("tls"))
    status, _diag = mod.deliver(
        msg, account="binisti@gmail.com", app_password=_APP_PASSWORD,
        host="smtp.gmail.com", port=587, timeout=5, smtp_factory=factory,
        ssl_context=_ssl_real.create_default_context())
    assert status == mod.EMAIL_SMTP_TLS_FAILED
    assert "login" not in record["calls"]   # never authenticates without TLS


# WS5.13 — a connection / DNS failure maps correctly.
def test_smtp_connection_failure_maps(tmp_path):
    mod = _load_smtp_sender_module()
    msg, _mid, _ = mod.build_message(_smtp_job(tmp_path), "binisti@gmail.com")
    _record, factory = _smtp_recorder(
        on_connect=_socket_real.gaierror("name resolution failed"))
    status, _diag = mod.deliver(
        msg, account="binisti@gmail.com", app_password=_APP_PASSWORD,
        host="smtp.gmail.com", port=587, timeout=5, smtp_factory=factory,
        ssl_context=_ssl_real.create_default_context())
    assert status == mod.EMAIL_SMTP_CONNECTION_FAILED


# A generic send failure maps to EMAIL_SEND_FAILED.
def test_smtp_send_failure_maps(tmp_path):
    mod = _load_smtp_sender_module()
    msg, _mid, _ = mod.build_message(_smtp_job(tmp_path), "binisti@gmail.com")
    record, factory = _smtp_recorder(
        on_send=_smtplib_real.SMTPServerDisconnected("dropped"))
    status, _diag = mod.deliver(
        msg, account="binisti@gmail.com", app_password=_APP_PASSWORD,
        host="smtp.gmail.com", port=587, timeout=5, smtp_factory=factory,
        ssl_context=_ssl_real.create_default_context())
    assert status == mod.EMAIL_SEND_FAILED
    assert "login" in record["calls"]   # reached the send phase


# Missing App Password on stdin maps to EMAIL_SMTP_CREDENTIAL_MISSING.
def test_smtp_missing_credential_maps(monkeypatch, tmp_path):
    mod = _load_smtp_sender_module()
    monkeypatch.setattr(mod.sys, "stdin", io.StringIO("\n"))
    job_path = tmp_path / "job.json"
    job_path.write_text(json.dumps(_smtp_job(tmp_path)), encoding="utf-8")
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        code = mod.main(["--job-path", str(job_path),
                         "--account", "binisti@gmail.com"])
    result = json.loads([ln for ln in buf.getvalue().splitlines()
                         if ln.strip().startswith("{")][-1])
    assert code == 1
    assert result["status"] == "EMAIL_SMTP_CREDENTIAL_MISSING"


# WS5.1/5.2 — the App Password is never a CLI flag and never read from the env.
def test_smtp_password_never_cli_or_env():
    src = _SMTP_SENDER_PY.read_text(encoding="utf-8")
    compile(src, str(_SMTP_SENDER_PY), "exec")
    assert "sys.stdin.readline" in src         # stdin only
    assert "--app-password" not in src.lower()
    assert "--password" not in src.lower()
    assert "import os" not in src              # cannot read the environment
    assert "os.environ" not in src
    assert "getpass" not in src
    assert "set_debuglevel" not in src         # AUTH exchange never printed
    for ps1 in (_SMTP_SENDER_PS1, _SMTP_DIAG_PS1):
        text = ps1.read_text(encoding="utf-8")
        low = text.lower()
        # the plaintext only ever crosses the process boundary via stdin
        assert "standardinput.writeline" in low
        assert "-apppassword" not in low       # never an argument
        # no environment variable ever carries the plaintext
        for envline in [ln for ln in text.splitlines() if "$env:" in ln.lower()]:
            low_env = envline.lower()
            assert "plain" not in low_env
            assert "password" not in low_env


# WS5.4 — the App Password never appears in emitted output on auth failure.
def test_smtp_password_not_leaked_in_output(monkeypatch, tmp_path):
    mod = _load_smtp_sender_module()
    err = _smtplib_real.SMTPAuthenticationError(535, b"rejected")
    _record, factory = _smtp_recorder(on_login=err)
    monkeypatch.setattr(mod.smtplib, "SMTP", factory)
    monkeypatch.setattr(mod.sys, "stdin", io.StringIO(_APP_PASSWORD + "\n"))
    job_path = tmp_path / "job.json"
    job_path.write_text(json.dumps(_smtp_job(tmp_path)), encoding="utf-8")
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        mod.main(["--job-path", str(job_path), "--account", "binisti@gmail.com"])
    out = buf.getvalue()
    assert _APP_PASSWORD not in out
    assert "EMAIL_SMTP_AUTHENTICATION_REJECTED" in out


# WS5.3/5.5 — configure script: DPAPI-only storage, 16-char validation, secure
# prompt, and it never writes/echoes the plaintext App Password.
def test_smtp_configure_script_is_secure_and_validates():
    src = _SMTP_CONFIGURE_PS1.read_text(encoding="utf-8")
    assert "Read-Host" in src and "-AsSecureString" in src   # secure prompt
    assert "ConvertFrom-SecureString" in src                 # DPAPI encrypt
    assert "-ne 16" in src                                    # length check
    assert "[A-Za-z0-9]{16}" in src                          # format check
    assert "GMAIL_SMTP_CONFIGURED" in src
    # Only the DPAPI-encrypted blob + the (non-secret) account are written.
    for line in src.splitlines():
        s = line.strip()
        if s.startswith("Set-Content") or s.startswith("Out-File") \
                or "Add-Content" in s:
            assert "$Normalized" not in s, s
            assert "$Raw" not in s, s
            assert "$SecureInput" not in s, s
    # The plaintext is never sent to the console, and no email is ever sent.
    low = src.lower()
    assert "write-host $normalized" not in low
    assert "write-host $raw" not in low
    assert "send_message" not in low
    assert "smtplib" not in low


# WS4 — the SMTP diagnostic authenticates without sending, and classifies.
def test_smtp_diagnostic_probe_read_only(tmp_path):
    mod = _load_smtp_diag_module()
    record, factory = _smtp_recorder()
    result = mod.probe(account="binisti@gmail.com", app_password=_APP_PASSWORD,
                       host="smtp.gmail.com", port=587, timeout=5,
                       smtp_factory=factory,
                       ssl_context=_ssl_real.create_default_context())
    assert result == mod.SMTP_AUTHENTICATION_OK
    # A read-only probe never issues MAIL/RCPT/DATA (no send).
    assert "send" not in record["calls"]
    assert "noop" in record["calls"]


def test_smtp_diagnostic_classifies_failures(tmp_path):
    mod = _load_smtp_diag_module()
    assert mod.probe(account="a@b.com", app_password="", host="h", port=587,
                     timeout=5, smtp_factory=_smtp_recorder()[1]) \
        == mod.SMTP_CREDENTIAL_MISSING
    _r, f_auth = _smtp_recorder(
        on_login=_smtplib_real.SMTPAuthenticationError(535, b"no"))
    assert mod.probe(account="a@b.com", app_password=_APP_PASSWORD, host="h",
                     port=587, timeout=5, smtp_factory=f_auth,
                     ssl_context=_ssl_real.create_default_context()) \
        == mod.SMTP_AUTHENTICATION_REJECTED
    _r2, f_conn = _smtp_recorder(on_connect=_socket_real.gaierror("dns"))
    assert mod.probe(account="a@b.com", app_password=_APP_PASSWORD, host="h",
                     port=587, timeout=5, smtp_factory=f_conn) \
        == mod.SMTP_CONNECTION_FAILED


# WS5.15/5.16 — the runtime uses ONLY the SMTP transport when SMTP is selected;
# OAuth is not invoked and both transports are never attempted in one cycle.
def test_runtime_uses_smtp_only_not_oauth(monkeypatch, tmp_path):
    cred = tmp_path / "creds"
    cred.mkdir()
    (cred / "gmail_smtp_app_password.dpapi").write_text("ENC", encoding="utf-8")
    (cred / "gmail_smtp_account.txt").write_text("binisti@gmail.com",
                                                 encoding="utf-8")
    cfg = {"email": {"transport": "gmail_smtp", "credential_dir": str(cred),
                     "app_credential_file": "gmail_smtp_app_password.dpapi",
                     "smtp_host": "smtp.gmail.com", "smtp_port": 587}}
    calls = []

    class _Proc:
        stdout = ('{"status":"EMAIL_SENT","message_id":"<x@gmail.com>",'
                  '"transport":"gmail_smtp","diagnostic":"Gmail SMTP accepted '
                  'the message."}')
        stderr = ""

    def _fake_run(cmd, **kwargs):
        calls.append(cmd)
        return _Proc()

    monkeypatch.setattr(rt.subprocess, "run", _fake_run)
    sender = rt.make_real_email_sender(cfg, repo_root=_REPO)
    result = sender({"job_path": str(tmp_path / "job.json")})
    assert result["status"] == "EMAIL_SENT"
    assert result["transport"] == "gmail_smtp"
    assert result["message_id"] == "<x@gmail.com>"
    assert len(calls) == 1                              # exactly one transport
    joined = " ".join(calls[0])
    assert "send_alpha_agent_smtp.ps1" in joined        # SMTP wrapper
    assert "send_alpha_agent_email.ps1" not in joined   # never the OAuth wrapper
    assert rt.resolve_email_transport(cfg) == rt.EMAIL_TRANSPORT_SMTP


# WS5.18 — a failed OAuth entry can never be auto-retried by the watchdog.
def test_failed_oauth_and_smtp_auth_never_auto_retried():
    assert rt.OAUTH_REAUTHORIZATION_REQUIRED not in rt.EMAIL_TRANSIENT_STATUSES
    assert rt.OAUTH_REAUTHORIZATION_REQUIRED in rt.EMAIL_NONRETRYABLE_STATUSES
    assert rt.EMAIL_SMTP_AUTHENTICATION_REJECTED in rt.EMAIL_NONRETRYABLE_STATUSES
    assert rt.EMAIL_SMTP_TLS_FAILED in rt.EMAIL_NONRETRYABLE_STATUSES
    # Only genuinely transient SMTP failures are eligible for a retry.
    assert rt.EMAIL_SMTP_CONNECTION_FAILED in rt.EMAIL_TRANSIENT_STATUSES
    assert rt.EMAIL_SEND_FAILED in rt.EMAIL_TRANSIENT_STATUSES


# WS5.17 — the SMTP acceptance send uses a NEW idempotency identity so it never
# collides with the failed OAuth v2 cycle, and it sends exactly once.
def test_smtp_acceptance_uses_new_idempotency_identity(env):
    d = "2026-07-29"
    assert rc.report_cycle_id("exec_test_v2", d) != rc.report_cycle_id(
        "exec_test", d)
    sender = FakeSender(rc.EMAIL_SENT)
    r = _runtime(env, sender=sender)
    res = r.run_report_only(
        exec_test=True, exec_test_key="exec_test_v2",
        subject_override="TEST — Alpha Agent Executive Brief v2 — 2026-07-30")
    assert len(sender.calls) == 1                       # exactly one delivery
    assert res.cycle_id == rc.report_cycle_id("exec_test_v2", d)
    assert res.email_status == rc.EMAIL_SENT


# WS5.19 — building/using the SMTP transport mutates no operational ledger.
def test_smtp_transport_no_operational_mutation(env):
    before = _fingerprint(env["ledger"])
    sender = FakeSender(rc.EMAIL_SENT)
    _runtime(env, sender=sender).run_report_only(
        exec_test=True, exec_test_key="exec_test_v2")
    _ = rt.make_real_email_sender(env["cfg"], repo_root=_REPO)
    assert _fingerprint(env["ledger"]) == before


# WS5.20 — SMTP introduces no new scheduled task (still exactly the four).
def test_smtp_adds_no_scheduled_task():
    cfg = rc.load_config(_REPO / "configs" / "alpha_agent" /
                         "stage4_runtime.json")
    assert sorted(cfg["allowed_task_names"]) == sorted(rc.ALPHA_AGENT_TASK_NAMES)
    assert len(cfg["allowed_task_names"]) == 4


# Regression: a UTF-8 BOM that Windows stdin pipes can prepend must be stripped,
# or the App Password is corrupted (previously raised UnicodeEncodeError at AUTH).
def test_smtp_stdin_strips_utf8_bom(monkeypatch):
    for mod in (_load_smtp_sender_module(), _load_smtp_diag_module()):
        monkeypatch.setattr(mod.sys, "stdin",
                            io.StringIO("\ufeff" + _APP_PASSWORD + "\n"))
        assert mod._read_app_password_from_stdin() == _APP_PASSWORD


# --------------------------------------------------------------------------- #
# Stage 8 compatibility: the autonomy additions are purely additive and never
# alter any Stage 4 mode; SMTP delivery remains intact.
# --------------------------------------------------------------------------- #
def test_stage8_additions_do_not_break_stage4_config():
    cfg = rc.load_config(_REPO / "configs" / "alpha_agent"
                         / "stage4_runtime.json")
    # Stage 8 flags were added additively.
    assert cfg.get("stage8_enabled") is True
    # The strict Stage-4 contract is untouched: SMTP transport + exactly the
    # four cadence task names (the Telegram control task is NOT one of them).
    assert cfg["email"]["transport"] == "gmail_smtp"
    assert sorted(cfg["allowed_task_names"]) == sorted(
        list(rc.ALPHA_AGENT_TASK_NAMES))


def test_stage8_runtime_entrypoints_are_additive_and_research_only(tmp_path):
    # New Stage 8 entry points exist and run a bounded, never-idle cycle without
    # touching any Stage 4 mode or any operational ledger.
    assert hasattr(rt, "run_autonomy_cycle") and hasattr(rt, "stage8_enabled")
    summ = rt.run_autonomy_cycle(
        {"stage8_autonomy_root": str(tmp_path / "s8"),
         "autonomy": {"max_jobs_per_cycle": 3}})
    assert summ["processed"] >= 1 and summ["depth"] >= 1
