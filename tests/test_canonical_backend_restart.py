"""ONE canonical backend restart / smoke workflow.

THE DEFECT THIS PREVENTS
------------------------
Stage after stage regenerated a stage-specific ``restart_smoke.ps1`` in a throwaway
handoff directory, and stage after stage reintroduced the SAME defect: polling
``http://127.0.0.1:8001/health`` when the canonical readiness routes are ``/v1/health``
and ``/v1/ready``. Stage 12 shipped it. Stage 21 shipped it again. The operator's own
``paper_trader_8001.stdout.log`` records 39 consecutive ``GET /health -> 404`` lines
before a human probed the right path by hand.

This file is deliberately NOT named after a stage: it outlives them.

The tests are in two layers.

*Contract* tests read the canonical script and prove structural properties - including
the one that actually kills the recurring defect: **every path the restart workflow
probes must be a route the application really declares as GET**, checked against the
parsed route table rather than against a convention.

*Runtime* tests actually execute the canonical script three times - contaminated store
roots, a startup failure, and a successful start against a hermetic stub backend on a
throwaway port - and assert what it really did. They never touch port 8001, never import
or launch the real application, and never read a live operational store.
"""
from __future__ import annotations

import importlib.util
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
OWNER_REL = "scripts/restart_paper_trader_backend.ps1"
OWNER = REPO / "scripts" / "restart_paper_trader_backend.ps1"
AUDIT_PY = REPO / "scripts" / "audit_architecture.py"

CANONICAL_HEALTH = "/v1/health"
CANONICAL_READY = "/v1/ready"
NONCANONICAL_PROBES = ("/health", "/ready", "/healthz", "/readyz", "/livez")

IS_WINDOWS = sys.platform == "win32"
POWERSHELL = shutil.which("powershell") or shutil.which("powershell.exe")
PYTHON = REPO / ".venv-win" / "Scripts" / "python.exe"

requires_powershell = pytest.mark.skipif(
    not (IS_WINDOWS and POWERSHELL and PYTHON.exists()),
    reason="the canonical restart workflow is Windows PowerShell only")


@pytest.fixture(scope="module")
def audit():
    spec = importlib.util.spec_from_file_location("audit_architecture_restart", AUDIT_PY)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def owner_src() -> str:
    return OWNER.read_text(encoding="utf-8")


# =========================================================================== #
# CONTRACT - one owner, canonical routes, real routes, no duplication
# =========================================================================== #
def test_01_the_canonical_owner_exists_and_declares_itself(audit, owner_src):
    assert OWNER.exists(), (
        "the canonical restart/smoke owner is missing; a handoff directory is a "
        "throwaway and must never be where this workflow lives")
    assert audit.RESTART_OWNER == OWNER_REL
    assert audit.RESTART_OWNER_DECLARATION in owner_src


def test_02_the_canonical_readiness_routes_are_v1_health_and_v1_ready(audit):
    assert audit.CANONICAL_READINESS_ROUTES == (CANONICAL_HEALTH, CANONICAL_READY)


def test_03_the_owner_polls_the_canonical_v1_health_path(owner_src):
    assert '"%s"' % CANONICAL_HEALTH in owner_src


def test_04_the_owner_polls_the_canonical_v1_ready_path(owner_src):
    assert '"%s"' % CANONICAL_READY in owner_src


def test_05_no_workflow_probes_a_noncanonical_health_route(audit):
    rep = audit.check_backend_restart_ownership()
    assert rep["noncanonical_health_probes"] == []


def test_06_the_canonical_health_and_ready_paths_are_really_declared_get_routes(audit):
    """The paths are not merely spelled 'v1' - the application declares them."""
    get_routes = {r["path"] for r in audit.check_routes()["routes"]
                  if r["method"] == "GET"}
    assert CANONICAL_HEALTH in get_routes
    assert CANONICAL_READY in get_routes
    for bad in NONCANONICAL_PROBES:
        assert bad not in get_routes, (
            "%s is not served by this application, which is exactly why polling it "
            "returned 404 for two stages" % bad)


def test_07_every_path_the_restart_workflow_probes_is_a_declared_get_route(audit):
    rep = audit.check_backend_restart_ownership()
    assert rep["probed_routes_not_declared"] == []


def test_08_the_owner_implements_every_responsibility_it_claims(audit):
    rep = audit.check_backend_restart_ownership()
    assert rep["owner_missing_contract"] == []
    assert rep["owner_missing_canonical_routes"] == []


def test_09_the_owner_prints_the_full_startup_diagnostic_set(audit):
    rep = audit.check_backend_restart_ownership()
    assert rep["owner_missing_diagnostics"] == []


def test_10_no_other_powershell_script_reimplements_the_launch(audit):
    rep = audit.check_backend_restart_ownership()
    assert rep["reimplementing_scripts"] == []


def test_11_no_restart_workflow_uses_a_mutating_http_verb(audit, owner_src):
    rep = audit.check_backend_restart_ownership()
    assert rep["mutating_http_calls"] == []
    for verb in ("Post", "Put", "Patch", "Delete"):
        assert ("-Method %s" % verb) not in owner_src


def test_12_live_smoke_ok_is_emitted_by_exactly_one_script_exactly_once(audit):
    rep = audit.check_backend_restart_ownership()
    assert rep["live_smoke_emitting_scripts"] == [OWNER_REL]
    assert rep["owner_live_smoke_emissions"] == 1


def test_13_the_owner_never_creates_orders_calls_a_broker_or_enables_automation(owner_src):
    # CODE only: a comment that promises never to call a broker must not read as one.
    code = "\n".join(re.sub(r"#.*$", "", ln) for ln in owner_src.splitlines()).lower()
    for forbidden in ("place_order", "submit_order", "create_order", "broker",
                      "automation_enabled", "enable_automation", "daily_close",
                      "invoke-restmethod -method post"):
        assert forbidden not in code


def test_14_the_blocking_invariants_cover_the_restart_owner(audit):
    fields = {field for key, field, _ in audit.BLOCKING_INVARIANTS
              if key == "backend_restart_ownership"}
    for required in ("owner_present", "owner_declares_ownership",
                     "noncanonical_health_probes", "probed_routes_not_declared",
                     "reimplementing_scripts", "owner_missing_contract",
                     "owner_missing_diagnostics", "mutating_http_calls",
                     "live_smoke_emitting_scripts", "owner_live_smoke_emissions"):
        assert required in fields


def test_15_a_strict_audit_of_this_tree_reports_no_restart_violation(audit):
    rep = audit.run_audit()
    assert rep["backend_restart_ownership"]["owner_present"] is True
    failures = [f for f in audit._blocking_invariant_failures(rep)
                if f.startswith("backend_restart_ownership.")]
    assert failures == []


# =========================================================================== #
# THE GUARD ITSELF - it must actually fail on each forbidden shape
# =========================================================================== #
def _write_ps1(directory: Path, name: str, body: str) -> Path:
    p = directory / name
    p.write_text(body, encoding="utf-8")
    return p


def test_16_the_guard_fails_on_the_exact_historical_defect(audit, tmp_path):
    """The literal regression: a handoff script polling 8001/health."""
    _write_ps1(tmp_path, "restart_smoke.ps1", "\n".join([
        "param([switch]$Force)",
        '$null = Invoke-RestMethod -Uri "http://127.0.0.1:8001/health" -TimeoutSec 5',
    ]) + "\n")
    rep = audit.check_backend_restart_ownership((str(tmp_path),))
    assert any("/health" in x for x in rep["noncanonical_health_probes"])


@pytest.mark.parametrize("path", NONCANONICAL_PROBES)
def test_17_the_guard_fails_on_any_noncanonical_health_route(audit, tmp_path, path):
    _write_ps1(tmp_path, "restart_smoke.ps1",
               '$null = Invoke-RestMethod -Uri "http://127.0.0.1:8001%s"\n' % path)
    rep = audit.check_backend_restart_ownership((str(tmp_path),))
    assert rep["noncanonical_health_probes"] != []


def test_18_the_guard_fails_when_a_second_script_launches_the_application(audit, tmp_path):
    _write_ps1(tmp_path, "restart_smoke.ps1", "\n".join([
        '$args = @("-m", "uvicorn", "paper_trader.api.app:app", "--port", "8001")',
        "Start-Process -FilePath $Py -ArgumentList $args",
    ]) + "\n")
    rep = audit.check_backend_restart_ownership((str(tmp_path),))
    assert rep["reimplementing_scripts"] != []


def test_18b_the_guard_fails_when_a_second_script_manages_the_live_port(audit, tmp_path):
    """Port handling is the owner's. A handoff delegates with -Port; it never binds,
    stops or lists listeners on the live port itself."""
    _write_ps1(tmp_path, "restart_smoke.ps1",
               "$c = Get-NetTCPConnection -LocalPort 8001 -State Listen\n"
               "foreach ($x in $c) { Stop-Process -Id $x.OwningProcess -Force }\n")
    rep = audit.check_backend_restart_ownership((str(tmp_path),))
    assert rep["reimplementing_scripts"] != []


def test_19_the_guard_fails_when_a_script_probes_an_undeclared_route(audit, tmp_path):
    _write_ps1(tmp_path, "restart_smoke.ps1",
               '$null = Invoke-RestMethod -Uri "http://127.0.0.1:8001/v1/not-a-real-route"\n')
    rep = audit.check_backend_restart_ownership((str(tmp_path),))
    assert any("/v1/not-a-real-route" in x for x in rep["probed_routes_not_declared"])


def test_20_the_guard_fails_when_a_second_script_prints_live_smoke_ok(audit, tmp_path):
    _write_ps1(tmp_path, "restart_smoke.ps1", 'Write-Host "LIVE_SMOKE_OK"\n')
    rep = audit.check_backend_restart_ownership((str(tmp_path),))
    assert rep["live_smoke_emitting_scripts"] != [OWNER_REL]


def test_21_the_guard_fails_on_a_mutating_http_verb(audit, tmp_path):
    _write_ps1(tmp_path, "restart_smoke.ps1",
               '$null = Invoke-RestMethod -Uri "http://127.0.0.1:8001/v1/health" -Method Post\n')
    rep = audit.check_backend_restart_ownership((str(tmp_path),))
    assert rep["mutating_http_calls"] != []


def test_22_a_delegating_handoff_script_passes_the_guard(audit, tmp_path):
    """The shape every future handoff must take: delegate, add GET assertions."""
    _write_ps1(tmp_path, "restart_smoke.ps1", "\n".join([
        "param([switch]$Force)",
        '$Canonical = Join-Path $Repo "scripts\\restart_paper_trader_backend.ps1"',
        '$Extra = @("/v1/operations/daily-close/progress")',
        "& $Canonical -Force -Port 8001 -SmokePath $Extra",
        'if ($LASTEXITCODE -ne 0) { throw "delegated restart failed" }',
    ]) + "\n")
    rep = audit.check_backend_restart_ownership((str(tmp_path),))
    assert rep["noncanonical_health_probes"] == []
    assert rep["reimplementing_scripts"] == []
    assert rep["probed_routes_not_declared"] == []
    assert rep["live_smoke_emitting_scripts"] == [OWNER_REL]


# =========================================================================== #
# RUNTIME - the script is actually executed. Never port 8001, never the real app.
# =========================================================================== #
STUB_BACKEND = '''\
"""A hermetic stub backend. It is NOT the Paper Trader application: it serves the
canonical routes, records every request, and 404s everything else, so the restart
workflow's real HTTP behaviour can be asserted without launching production."""
import json
import os

JOURNAL = os.environ["RESTART_STUB_JOURNAL"]
KEY = os.environ.get("PAPER_TRADER_SERVICE_API_KEY", "")
# Reproduce the historical defect: a backend that serves nothing the workflow probes.
ALL_404 = os.environ.get("RESTART_STUB_404_ALL", "") == "1"

OPEN_ROUTES = {"/v1/health": {"status": "ok", "service": "stub"},
               "/v1/ready": {"ready": True}}
AUTHED_ROUTES = {
    "/v1/operations/portfolio-state": {
        "positions": [{"ticker": "MNST", "shares": 100}],
        "capital": {"nav": 100000.0, "cash": 1000.0}},
    "/v1/operations/daily-close/progress": {"status": "CLOSE_COMPLETE_HOLD"},
}


async def _json(send, status, payload):
    body = json.dumps(payload).encode("utf-8")
    await send({"type": "http.response.start", "status": status,
                "headers": [[b"content-type", b"application/json"],
                            [b"content-length", str(len(body)).encode("ascii")]]})
    await send({"type": "http.response.body", "body": body})


async def app(scope, receive, send):
    if scope["type"] == "lifespan":
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
                return
    if scope["type"] != "http":
        return
    path = scope["path"]
    method = scope["method"]
    headers = {k.decode("latin-1").lower(): v.decode("latin-1")
               for k, v in scope["headers"]}
    with open(JOURNAL, "a", encoding="utf-8") as fh:
        fh.write(json.dumps({"method": method, "path": path,
                             "api_key": headers.get("x-api-key", "")}) + "\\n")
    if method != "GET":
        await _json(send, 405, {"detail": "this stub accepts GET only"})
        return
    if ALL_404:
        await _json(send, 404, {"detail": "not found"})
        return
    if path in OPEN_ROUTES:
        await _json(send, 200, OPEN_ROUTES[path])
        return
    if path in AUTHED_ROUTES:
        if headers.get("x-api-key", "") != KEY:
            await _json(send, 401, {"detail": "missing or invalid X-API-Key"})
            return
        await _json(send, 200, AUTHED_ROUTES[path])
        return
    await _json(send, 404, {"detail": "not found"})
'''

STUB_KEY = "restart-regression-stub-key"


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _child_env(**extra) -> dict:
    """A clean child environment. Every canonical store override is removed so the
    workflow's own production-root validation is exercised honestly."""
    spec = importlib.util.spec_from_file_location(
        "environment_isolation_restart", REPO / "api" / "environment_isolation.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    env = dict(os.environ)
    for var in tuple(mod.CANONICAL_STORE_ENV_VARS) + (mod.ACCEPTANCE_MODE_ENV,):
        env.pop(var, None)
    env["PAPER_TRADER_SERVICE_API_KEY"] = STUB_KEY
    env.update(extra)
    return env


def _run_owner(args, env, tmp_path, timeout=180):
    """Run the canonical workflow and return ``(returncode, combined_output)``.

    Output goes to FILES, never to pipes. A successful restart deliberately leaves the
    backend running, and that surviving grandchild inherits the parent's handles: with
    ``capture_output=True`` the pipe never closes and the test hangs until its timeout
    even though the script exited cleanly seconds earlier.
    """
    out_file = Path(tmp_path) / "owner_stdout.txt"
    err_file = Path(tmp_path) / "owner_stderr.txt"
    cmd = [POWERSHELL, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
           "-File", str(OWNER)] + [str(a) for a in args]
    with open(out_file, "wb") as so, open(err_file, "wb") as se:
        proc = subprocess.run(cmd, stdout=so, stderr=se, stdin=subprocess.DEVNULL,
                              timeout=timeout, env=env, cwd=str(REPO))
    text = (out_file.read_text(encoding="utf-8", errors="replace")
            + err_file.read_text(encoding="utf-8", errors="replace"))
    return proc.returncode, text


def _kill_launched(output: str) -> None:
    for m in re.finditer(r"launched pid (\d+)", output):
        subprocess.run(["taskkill", "/F", "/T", "/PID", m.group(1)],
                       capture_output=True, text=True)


@requires_powershell
def test_30_it_refuses_to_launch_when_a_store_root_is_contaminated(tmp_path):
    env = _child_env(PAPER_TRADER_DESK_DIR=r"D:\Temp\stage20_ui_acceptance\desk")
    code, out = _run_owner(["-Force", "-Port", _free_port(), "-LogDir", tmp_path],
                           env, tmp_path, timeout=90)
    _kill_launched(out)
    assert code != 0, out
    assert "PRODUCTION_STORE_ROOTS_CONTAMINATED" in out
    assert "PAPER_TRADER_DESK_DIR" in out
    assert "LIVE_SMOKE_OK" not in out
    assert "launched pid" not in out, "nothing may be launched from a contaminated shell"


@requires_powershell
def test_31_a_startup_failure_prints_full_diagnostics_and_returns_nonzero(tmp_path):
    env = _child_env(RESTART_STUB_JOURNAL=str(tmp_path / "journal.jsonl"))
    code, out = _run_owner(["-Force", "-Port", _free_port(), "-LogDir", tmp_path,
                            "-AppDir", tmp_path, "-AppModule", "no_such_stub_module:app",
                            "-ReadyTimeoutSec", 30], env, tmp_path, timeout=180)
    _kill_launched(out)
    assert code != 0, out
    assert "BACKEND STARTUP DIAGNOSTICS" in out
    for token in ("launched pid", "pid still alive", "process exit code",
                  "listener", "STDERR", "STDOUT"):
        assert token in out, "missing startup diagnostic: %s\n%s" % (token, out)
    assert re.search(r"pid still alive\s*:\s*False", out), out
    assert "LIVE_SMOKE_OK" not in out


@requires_powershell
def test_32_a_successful_restart_probes_only_canonical_routes_and_smokes_live(tmp_path):
    journal = tmp_path / "journal.jsonl"
    (tmp_path / "restart_stub_backend.py").write_text(STUB_BACKEND, encoding="utf-8")
    env = _child_env(RESTART_STUB_JOURNAL=str(journal))
    port = _free_port()
    code, out = _run_owner(["-Force", "-Port", port, "-LogDir", tmp_path,
                            "-AppDir", tmp_path, "-AppModule", "restart_stub_backend:app",
                            "-SmokePath", "/v1/operations/daily-close/progress",
                            "-ReadyTimeoutSec", 60], env, tmp_path, timeout=180)
    try:
        assert code == 0, out

        # (7) LIVE_SMOKE_OK exactly once, and only at the end.
        assert out.count("LIVE_SMOKE_OK") == 1, out
        assert out.rstrip().endswith("LIVE_SMOKE_OK"), out

        # process start + port listener + exactly one listener
        assert re.search(r"launched pid \d+", out), out
        assert "exactly one backend listener" in out, out

        # the request journal is the ground truth for what was actually probed
        rows = [json.loads(x) for x in
                journal.read_text(encoding="utf-8").splitlines() if x.strip()]
        paths = [row["path"] for row in rows]
        assert CANONICAL_HEALTH in paths, paths
        assert CANONICAL_READY in paths, paths
        for bad in NONCANONICAL_PROBES:
            assert bad not in paths, "the workflow probed %s" % bad

        # (9) no POST - not anywhere, not once
        assert {row["method"] for row in rows} == {"GET"}, rows

        # authenticated live read
        authed = [row for row in rows
                  if row["path"] == "/v1/operations/portfolio-state"]
        assert authed, paths
        assert all(row["api_key"] == STUB_KEY for row in authed), authed

        # stage-specific GET assertions are additive, not a replacement
        assert "/v1/operations/daily-close/progress" in paths, paths
    finally:
        _kill_launched(out)


@requires_powershell
def test_33b_a_health_route_that_404s_fails_fast_and_by_name(tmp_path):
    """THE recurring defect, at runtime.

    A backend that is up but does not serve the probed health path must be reported
    immediately and by name. The historical failure mode was the opposite: 39 silent
    retries of a 404 that no amount of waiting could ever fix. ``-ReadyTimeoutSec 120``
    is deliberately generous - if the workflow retried, this test would take two minutes.
    """
    journal = tmp_path / "journal.jsonl"
    (tmp_path / "restart_stub_backend.py").write_text(STUB_BACKEND, encoding="utf-8")
    env = _child_env(RESTART_STUB_JOURNAL=str(journal), RESTART_STUB_404_ALL="1")
    started = time.monotonic()
    code, out = _run_owner(["-Force", "-Port", _free_port(), "-LogDir", tmp_path,
                            "-AppDir", tmp_path, "-AppModule", "restart_stub_backend:app",
                            "-ReadyTimeoutSec", 120], env, tmp_path, timeout=200)
    elapsed = time.monotonic() - started
    try:
        assert code != 0, out
        assert "NONCANONICAL HEALTH ROUTE" in out, out
        assert CANONICAL_HEALTH in out and CANONICAL_READY in out, out
        assert "BACKEND STARTUP DIAGNOSTICS" in out, out
        assert "LIVE_SMOKE_OK" not in out
        assert elapsed < 90, (
            "the workflow retried a 404 instead of failing fast (%.0fs)" % elapsed)
        rows = [json.loads(x) for x in
                journal.read_text(encoding="utf-8").splitlines() if x.strip()]
        probes = [r for r in rows if r["path"] == CANONICAL_HEALTH]
        assert len(probes) == 1, "a 404 must be probed once, not retried: %r" % rows
    finally:
        _kill_launched(out)


@requires_powershell
def test_33_without_force_it_validates_and_starts_nothing(tmp_path):
    env = _child_env()
    code, out = _run_owner(["-Port", _free_port(), "-LogDir", tmp_path],
                           env, tmp_path, timeout=90)
    _kill_launched(out)
    assert code == 0, out
    assert "PRODUCTION_STORE_ROOTS_OK" in out
    assert "RESTART_PREFLIGHT_OK" in out
    assert "LIVE_SMOKE_OK" not in out
    assert "launched pid" not in out
