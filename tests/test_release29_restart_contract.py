"""Release 29 UX2 — PERMANENT restart / smoke invocation contract.

Two REAL PowerShell defects motivated this file:

  1. A release wrapper forwarded a ``String[]`` of smoke paths across
     ``powershell.exe -File``. ``-File`` has no PowerShell parser on the far side: the
     array flattened into bare tokens, the binder took the first as ``-SmokePath`` and
     bound the NEXT URL positionally to ``-ReadyTimeoutSec:Int32``. The run died naming a
     timeout, not a path.

  2. The repair attempt used ``powershell.exe -Command`` with a DOUBLE-QUOTED here-string
     containing continuation backticks. The outer shell ate the backticks, so ``-Force``,
     ``-Port`` and ``-SmokePath`` became three separate commands.

Neither is a defect inside the restart owner — both are defects in how it was INVOKED. So
the contract proven here is the INVOCATION contract:

  A. the owner contains no process-terminating statement, so calling it directly can never
     end an operator's shell;
  B. a DIRECT invocation binds ``switch`` / ``Int32`` / ``String[]`` / ``Int32``, and five
     smoke paths stay ONE ``String[]`` of five elements — proven by the real PowerShell
     parameter binder, not by a regex;
  C. the static guard actually REJECTS both historical shapes (and two neighbours), proven
     against synthetic violating workflows;
  D. every capability the owner is required to keep is still in it.

Hermetic: the only PowerShell run is ``-ContractProbe``, which returns before any store
root is read and before any process or socket is touched.
"""
from __future__ import annotations

import importlib
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
OWNER = ROOT / "scripts" / "restart_paper_trader_backend.ps1"
PROBE = ROOT / "tests" / "support" / "restart_contract_probe.ps1"
OWNER_SRC = OWNER.read_text(encoding="utf-8")

#: The canonical caller-supplied smoke paths of the production restart command.
CANONICAL_SMOKE_PATHS = [
    "/v1/operations/workflow-state",
    "/v1/operations/information-collection",
    "/v1/operations/daily-close",
    "/v1/operational-book",
    "/v1/operations/portfolio-reassessment",
]

# Violating shapes are assembled from PARTS so that no single line of this test file is
# itself a violating invocation — the guard scans .py files too, and a test that trips its
# own guard would be a false alarm forever.
_SHELL = "powershell.exe"
_OWNER_NAME = "scripts\\restart_paper_trader_backend.ps1"


def _powershell() -> str:
    exe = shutil.which("powershell.exe") or shutil.which("powershell")
    if not exe:
        pytest.skip("Windows PowerShell is required for the restart contract probe")
    return exe


def _audit():
    return importlib.import_module("scripts.audit_architecture")


# =========================================================================== #
# A. NO PROCESS-TERMINATING STATEMENT — the owner is safe to call directly
# =========================================================================== #
def test_a1_owner_contains_no_exit_statement():
    """`exit` in a directly-invoked script can end the operator's own session."""
    offenders = []
    lit = re.compile(r""""(?:[^"`]|`.)*"|'(?:[^']|'')*'""")
    for i, line in enumerate(OWNER_SRC.splitlines(), start=1):
        code = lit.sub(" ", line).split("#", 1)[0]
        if re.search(r"(?im)(?:^|[\s;{(&|])exit\b", code):
            offenders.append((i, line.strip()))
        if re.search(r"(?i)\$Host\.SetShouldExit|\[Environment\]::Exit", code):
            offenders.append((i, line.strip()))
    assert offenders == [], f"the canonical restart owner must contain no exit: {offenders}"


def test_a2_owner_reports_its_outcome_without_exiting():
    """No exit means the outcome contract is a printed token + $LASTEXITCODE + a global."""
    assert "$global:LASTEXITCODE = $script:ResultCode" in OWNER_SRC
    assert "$global:PaperTraderRestartResult = $script:ResultToken" in OWNER_SRC
    assert 'Write-Host "LIVE_SMOKE_OK"' in OWNER_SRC
    assert 'Write-Host "RESTART_PREFLIGHT_OK"' in OWNER_SRC
    assert '$RESTART_FAILURE_PREFIX = "RESTART_SMOKE_FAILED - "' in OWNER_SRC
    # a failure is a throw caught by the ONE outer handler, never a process exit
    assert re.search(r"function Fail\(\[string\]\$Message\)\s*\{\s*throw\s*\(\$RESTART_FAILURE_PREFIX",
                     OWNER_SRC), "Fail must throw, never terminate the process"


def test_a3_owner_documents_the_direct_invocation_verbatim():
    assert "& C:\\Users\\binis\\paper_trader\\scripts\\restart_paper_trader_backend.ps1" in OWNER_SRC
    assert "-SmokePath $SmokePaths" in OWNER_SRC
    for p in CANONICAL_SMOKE_PATHS:
        assert p in OWNER_SRC, f"the documented canonical command must show {p}"


# =========================================================================== #
# B. THE BINDER PROVES THE PARAMETER CONTRACT
# =========================================================================== #
@pytest.fixture(scope="module")
def probe_payload() -> dict:
    exe = _powershell()
    proc = subprocess.run(
        [exe, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
         "-File", str(PROBE)],
        capture_output=True, text=True, cwd=str(ROOT), timeout=180)
    out = (proc.stdout or "") + (proc.stderr or "")
    assert "CONTRACT_PROBE_JSON_BEGIN" in out, out[-4000:]
    body = out.split("CONTRACT_PROBE_JSON_BEGIN", 1)[1].split("CONTRACT_PROBE_JSON_END", 1)[0]
    payload = json.loads(body.strip())
    payload["_raw"] = out
    return payload


def test_b1_force_binds_as_a_switch(probe_payload):
    assert probe_payload["Force_type"] == "System.Management.Automation.SwitchParameter"
    assert probe_payload["Force_value"] is True


def test_b2_port_binds_as_int32(probe_payload):
    assert probe_payload["Port_type"] == "System.Int32"
    assert probe_payload["Port_value"] == 8098


def test_b3_smokepath_binds_as_one_string_array(probe_payload):
    assert probe_payload["SmokePath_type"] == "System.String[]"


def test_b4_five_smoke_paths_stay_five_elements_of_one_array(probe_payload):
    """The historical defect: five paths became one path plus four stray tokens, one of
    which was bound to -ReadyTimeoutSec:Int32."""
    assert probe_payload["SmokePath_count"] == 5
    assert list(probe_payload["SmokePath_values"]) == CANONICAL_SMOKE_PATHS


def test_b5_readytimeoutsec_is_int32_and_was_never_touched_by_a_path(probe_payload):
    assert probe_payload["ReadyTimeoutSec_type"] == "System.Int32"
    assert probe_payload["ReadyTimeoutSec_value"] == 90


def test_b6_the_direct_caller_survives_and_reports_success(probe_payload):
    raw = probe_payload["_raw"]
    assert "RESTART_CONTRACT_PROBE_OK" in raw
    assert "PROBE_LASTEXITCODE=0" in raw
    assert "PROBE_CALLER_SURVIVED" in raw


def test_b7_a_flattened_path_is_rejected_by_name():
    """A non-rooted smoke path is the flattening signature and must fail loudly."""
    exe = _powershell()
    proc = subprocess.run(
        [exe, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
         "-Command",
         "& '%s' -ContractProbe -SmokePath @('90')" % str(OWNER).replace("'", "''")],
        capture_output=True, text=True, cwd=str(ROOT), timeout=180)
    out = (proc.stdout or "") + (proc.stderr or "")
    assert "RESTART_SMOKE_FAILED" in out, out[-3000:]
    assert "flattening signature" in out


# =========================================================================== #
# C. THE STATIC GUARD REJECTS THE HISTORICAL SHAPES
# =========================================================================== #
def _hygiene_on(tmp_path: Path, filename: str, body: str) -> dict:
    (tmp_path / filename).write_text(body, encoding="utf-8")
    return _audit().check_restart_invocation_hygiene((str(tmp_path),))


def test_c1_repository_is_clean_today():
    rep = _audit().check_restart_invocation_hygiene()
    assert rep["owner_is_exit_free"] is True
    assert rep["file_switch_smokepath_forwarding"] == []
    assert rep["command_switch_lifecycle_construction"] == []
    assert rep["fragile_array_forwarding"] == []
    assert rep["duplicate_restart_implementations"] == []
    assert rep["owner_declares_direct_invocation"] is True
    assert rep["owner_exposes_contract_probe"] is True
    assert rep["owner_reports_last_exit_code"] is True
    assert rep["owner_asserts_smokepath_contract"] is True


def test_c2_guard_rejects_file_switch_smokepath_forwarding(tmp_path):
    """Defect #1: String[] forwarded across a child shell with -File."""
    bad = (
        "$paths = @('/v1/operational-book','/v1/operations/daily-close')\n"
        + _SHELL + " -File .\\" + _OWNER_NAME + " -Force -Port 8001 -SmokePath $paths\n"
    )
    rep = _hygiene_on(tmp_path, "release_wrapper.ps1", bad)
    assert rep["file_switch_smokepath_forwarding"], \
        "the -File + -SmokePath forwarding defect must be rejected"


def test_c3_guard_rejects_file_switch_forwarding_across_backtick_continuations(tmp_path):
    """The same defect, spread over continuation lines, is the same defect."""
    bad = (
        _SHELL + " -File `\n"
        "    .\\" + _OWNER_NAME + " `\n"
        "    -Force -Port 8001 `\n"
        "    -SmokePath $paths\n"
    )
    rep = _hygiene_on(tmp_path, "release_wrapper_multiline.ps1", bad)
    assert rep["file_switch_smokepath_forwarding"], \
        "continuation lines must be judged as one logical command"


def test_c4_guard_rejects_dynamic_command_lifecycle_construction(tmp_path):
    """Defect #2: a lifecycle command built through a child shell's -Command."""
    bad = _SHELL + " -Command \"& .\\" + _OWNER_NAME + " -Force -Port 8001\"\n"
    rep = _hygiene_on(tmp_path, "repair_attempt.ps1", bad)
    assert rep["command_switch_lifecycle_construction"], \
        "dynamic -Command lifecycle construction must be rejected"


def test_c5_guard_rejects_a_collapsed_string_array(tmp_path):
    """Five paths joined into one string is five checks silently becoming zero."""
    comma_joined = (
        "-SmokePath "
        "\"/v1/operational-book,/v1/operations/daily-close\"\n"
    )
    rep = _hygiene_on(tmp_path, "joined.ps1", "& .\\owner.ps1 -Force " + comma_joined)
    assert rep["fragile_array_forwarding"], "a comma-joined String[] must be rejected"

    join_operator = (
        "-SmokePath "
        "($paths -join "
        "',')\n"
    )
    rep2 = _hygiene_on(tmp_path, "joined2.ps1", "& .\\owner.ps1 -Force " + join_operator)
    assert rep2["fragile_array_forwarding"], "a -join'ed String[] must be rejected"


def test_c6_guard_rejects_a_duplicate_restart_implementation(tmp_path):
    dup = (
        "Start-Process python -ArgumentList '-m','uvicorn','app:app'\n"
        "Invoke-WebRequest http://127.0.0.1:8001/v1/health\n"
    )
    rep = _hygiene_on(tmp_path, "restart_smoke.ps1", dup)
    assert rep["duplicate_restart_implementations"], \
        "a second restart implementation must be rejected"


def test_c7_guard_is_wired_into_the_strict_gate():
    aud = _audit()
    keys = {k for k, _f, _v in aud.BLOCKING_INVARIANTS}
    assert "restart_invocation_hygiene" in keys
    fields = {f for k, f, _v in aud.BLOCKING_INVARIANTS if k == "restart_invocation_hygiene"}
    for required in ("owner_is_exit_free", "file_switch_smokepath_forwarding",
                     "command_switch_lifecycle_construction", "fragile_array_forwarding",
                     "duplicate_restart_implementations"):
        assert required in fields, f"{required} must make --strict fail"


# =========================================================================== #
# D. NOTHING THE OWNER IS REQUIRED TO DO WAS LOST IN THE REFACTOR
# =========================================================================== #
@pytest.mark.parametrize("token,why", [
    ("environment_isolation", "production-store-root gate"),
    ("$script:LaunchedPid", "PID tracking"),
    ("Get-BackendListeners", "stop only the intended listener"),
    ("Stop-Process", "process stop"),
    ("Start-Process", "process start"),
    ('"/v1/health"', "canonical health gate"),
    ('"/v1/ready"', "canonical readiness gate"),
    ("Show-StartupDiagnostics", "stdout/stderr failure diagnostics"),
    ("exactly one backend must own it", "exactly-one-listener assertion"),
    ("X-API-Key", "authenticated live read"),
    ("EMPTY portfolio", "the store-root contamination assertion"),
])
def test_d1_owner_keeps_every_required_capability(token, why):
    assert token in OWNER_SRC, f"the refactor must preserve: {why} ({token})"


def test_d2_live_smoke_ok_is_emitted_once_and_only_after_every_check():
    emits = [ln for ln in OWNER_SRC.splitlines()
             if "LIVE_SMOKE_OK" in ln and re.search(r"Write-Host|Write-Output|\becho\b", ln)]
    assert len(emits) == 1, f"LIVE_SMOKE_OK must be emitted exactly once, got {emits}"
    emit_at = OWNER_SRC.find('Write-Host "LIVE_SMOKE_OK"')
    for earlier in ('Fail ("the backend served an EMPTY portfolio',
                    'Write-Section "AUTHENTICATED LIVE READ"',
                    'Write-Section "PORT LISTENER"',
                    'Write-Section "READINESS"'):
        assert 0 < OWNER_SRC.find(earlier) < emit_at, \
            f"LIVE_SMOKE_OK must come after {earlier!r}"


def test_d3_the_workflow_stays_get_only():
    assert not re.search(r"""-Method\s+["']?(?:Post|Put|Patch|Delete)\b""",
                         OWNER_SRC, re.IGNORECASE)


def test_d4_strict_architecture_audit_exit_zero():
    assert _audit().main(["--strict"]) == 0
