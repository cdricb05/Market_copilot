r"""scripts/validate_multi_asset_capital_activation.py - the operator validation gate
for MULTI_ASSET_CAPITAL_ACTIVATION_R55_V1.

Runs, from the checkout it lives in:

    1. the strict architecture audit (in-process; exit code 0 required);
    2. the release's own regression plus the adjacent owners it touched
       (a bounded pytest set, in a child process on THIS checkout);
    3. the static safety invariants of the release (no order / fill / promotion
       path in the new owners; the gate owner cannot promote without a
       pre-declared approval; the registry still imports no research package);

and prints EXACTLY ONE terminal token on its last line:

    COMMIT_OK
    DO_NOT_COMMIT
    <exact blocker>

It writes nothing outside pytest's own temp roots and touches no operational or
research store. It is not the restart owner and it does not start the backend.
"""
from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]

RELEASE_TESTS = [
    "tests/test_multi_asset_capital_activation_r55_v1.py",
    "tests/test_release47_constrained_reallocation.py",
    "tests/test_slice7_reallocation_proposal.py",
    "tests/test_slice6_holding_opportunity_cost.py",
    "tests/test_release29_3_decision_integrity.py",
    "tests/test_stage21_outcome_intelligence.py",
    "tests/test_release50_multi_asset_operational_manager.py",
    "tests/test_release55_2_runtime_release_identity.py",
    "tests/test_release54_active_manager_state.py",
    "tests/test_release62_1_1_forward_activation_integrity.py",
    "tests/test_fx_carry_cadence_forward_runtime.py",
    "tests/test_release52_research_runtime.py",
    "tests/test_canonical_backend_restart.py",
    "tests/test_release29_restart_contract.py",
]

NEW_OWNERS = [
    "api/capital_eligibility_gate.py",
    "alpha_agent/alpha_recovery/futures_trend_challenger.py",
    "alpha_agent/alpha_recovery/futures_trend_runtime.py",
    "scripts/audit_multi_asset_capital_pipeline.py",
]
BANNED_IN_NEW_OWNERS = ("place_order", "submit_order", "create_order(", "apply_fill(",
                        "promote_model(", "activate_sleeve(", "approve_proposal(",
                        "register_forward_challenger(", "import requests", "import httpx")


def _read(rel: str) -> str:
    return (_REPO / rel).read_text(encoding="utf-8", errors="replace")


def _strict_audit() -> tuple:
    spec = importlib.util.spec_from_file_location("audit_architecture_gate",
                                                  _REPO / "scripts" / "audit_architecture.py")
    A = importlib.util.module_from_spec(spec)
    sys.modules["audit_architecture_gate"] = A
    spec.loader.exec_module(A)
    rc = A.main(["--strict", "--json-only"])
    return rc, A


def _pytest(paths: list) -> tuple:
    env = dict(os.environ)
    env["PYTHONPATH"] = str(_REPO)
    env.setdefault("PYTHONUTF8", "1")
    cmd = [sys.executable, "-P", "-m", "pytest", "-q", "-p", "no:cacheprovider"] + paths
    proc = subprocess.run(cmd, cwd=str(_REPO), capture_output=True, text=True, env=env)
    tail = (proc.stdout or "")[-3000:]
    return proc.returncode, tail


def main(argv=None) -> int:
    blockers = []
    print("MULTI_ASSET_CAPITAL_ACTIVATION_R55_V1 - operator validation gate")
    print("checkout: %s" % _REPO)

    # (3) static safety invariants first - cheap, and a failure here ends everything.
    for rel in NEW_OWNERS:
        if not (_REPO / rel).exists():
            blockers.append("MISSING_OWNER %s" % rel)
            continue
        src = _read(rel)
        for token in BANNED_IN_NEW_OWNERS:
            if token in src:
                blockers.append("FORBIDDEN_TOKEN %s in %s" % (token, rel))
    gate_src = _read("api/capital_eligibility_gate.py") if (_REPO / "api/capital_eligibility_gate.py").exists() else ""
    if "REQ_APPROVAL" not in gate_src or 'if not appr:' not in gate_src:
        blockers.append("GATE_OWNER_DOES_NOT_REQUIRE_A_PRE_DECLARED_APPROVAL")
    if "alpha_agent" in _read("api/investability_registry.py"):
        blockers.append("REGISTRY_IMPORTS_A_RESEARCH_PACKAGE")
    print("static safety invariants: %s" % ("OK" if not blockers else "; ".join(blockers)))

    # (1) the strict audit
    try:
        rc, _ = _strict_audit()
    except Exception as exc:  # noqa: BLE001
        rc = 99
        blockers.append("STRICT_AUDIT_CRASHED %s" % str(exc)[:160])
    print("strict architecture audit exit: %s" % rc)
    if rc != 0 and not any(b.startswith("STRICT_AUDIT") for b in blockers):
        blockers.append("STRICT_AUDIT_EXIT_%s" % rc)

    # (2) the bounded regression
    rc2, tail = _pytest(RELEASE_TESTS)
    summary = [ln for ln in tail.splitlines() if "passed" in ln or "failed" in ln or "error" in ln.lower()]
    print("pytest exit: %s  %s" % (rc2, (summary[-1] if summary else tail[-300:])))
    if rc2 != 0:
        failed = [ln for ln in tail.splitlines() if ln.startswith("FAILED") or ln.startswith("ERROR")]
        blockers.append("PYTEST_EXIT_%s %s" % (rc2, "; ".join(failed[:5]) or "see output"))

    if blockers:
        print("DO_NOT_COMMIT")
        print(blockers[0] if len(blockers) == 1 else json.dumps(blockers))
        return 1
    print("COMMIT_OK")
    return 0


if __name__ == "__main__":                                       # pragma: no cover
    raise SystemExit(main())
