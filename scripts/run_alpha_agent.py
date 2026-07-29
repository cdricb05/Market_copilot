"""
scripts/run_alpha_agent.py — Alpha Agent Stage 4 persistent-runtime CLI.

Deterministic driver for the persistent Windows research runtime. Collects
source + RSS data, runs bounded grounded LLM research cycles, renders a
deterministic friendly report and hands it to the deterministic email sender —
never touching the operational portfolio, never creating trading activity,
never running the prediction service or PostgreSQL, never printing a secret.

Modes:
    --mode collect        deterministic Stage 2 + Stage 3.5 incremental
                          collection with verification; no LLM, no email
    --mode research       fresh collection + one bounded Stage 3 cycle +
                          deterministic report + single email delivery
    --mode report-only    render a report from the latest verified packages;
                          no collection, no LLM; email only with --send-email
    --mode watchdog       deterministic health + missed-cycle recovery
    --mode verify         structural verification; no network, writes nothing

Terminal lines (exactly one):
    ALPHA_AGENT_STAGE4_READY
    ALPHA_AGENT_STAGE4_DEGRADED — <reason>
    ALPHA_AGENT_STAGE4_EMAIL_CREDENTIAL_REQUIRED
    NO_NEW_RESEARCH_INPUT
    ALPHA_AGENT_STAGE4_VERIFIED
    ALPHA_AGENT_STAGE4_BLOCKED — <reason>

Example (Windows PowerShell):
    C:\\Users\\binis\\paper_trader\\.venv-win\\Scripts\\python.exe `
      scripts\\run_alpha_agent.py `
      --config configs\\alpha_agent\\stage4_runtime.json `
      --mode research --label post_close --json
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_PKG_PARENT = str(_REPO.parent)
if _PKG_PARENT not in sys.path:
    sys.path.insert(0, _PKG_PARENT)

from paper_trader.alpha_agent import runtime as rt  # noqa: E402
from paper_trader.alpha_agent import runtime_contracts as rc  # noqa: E402


def _git(args: list[str]) -> str:
    try:
        out = subprocess.run(["git", "-C", str(_REPO)] + args,
                             capture_output=True, text=True, timeout=15)
        if out.returncode == 0:
            return out.stdout.strip()
    except Exception:  # noqa: BLE001
        pass
    return ""


def _build_runtime(cfg: dict) -> rt.Runtime:
    git_commit = _git(["rev-parse", "HEAD"]) or "UNKNOWN"
    contact = _git(["config", "user.email"]) or None
    drivers = rt.RealStageDrivers(cfg, repo_root=_REPO, git_commit=git_commit,
                                  contact_email=contact)
    return rt.Runtime(cfg, drivers=drivers)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Alpha Agent Stage 4 persistent research runtime.")
    ap.add_argument("--config", required=True, help="Stage 4 runtime config.")
    ap.add_argument("--mode", required=True, choices=list(rc.MODES))
    ap.add_argument("--label", default=rc.LABEL_MANUAL,
                    choices=list(rc.REPORT_LABELS),
                    help="Report cycle label (research / report-only).")
    ap.add_argument("--as-of", default="latest", dest="as_of")
    ap.add_argument("--send-email", dest="send_email", action="store_true",
                    default=None, help="Force email delivery.")
    ap.add_argument("--no-send-email", dest="send_email",
                    action="store_false", help="Suppress email delivery.")
    ap.add_argument("--test-report", action="store_true",
                    help="Use the one-off TEST email subject.")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    try:
        cfg = rc.load_config(args.config)
    except rc.ConfigError as exc:
        print("%s — %s" % (rc.BLOCKED, exc))
        return 1

    runtime = _build_runtime(cfg)
    try:
        if args.mode == rc.MODE_COLLECT:
            result = runtime.run_collect()
        elif args.mode == rc.MODE_RESEARCH:
            send = True if args.send_email is None else args.send_email
            result = runtime.run_research(label=args.label, send_email=send,
                                          test_report=args.test_report)
        elif args.mode == rc.MODE_REPORT_ONLY:
            send = False if args.send_email is None else args.send_email
            result = runtime.run_report_only(label=args.label, send_email=send)
        elif args.mode == rc.MODE_WATCHDOG:
            result = runtime.run_watchdog()
        else:
            result = runtime.run_verify()
    except Exception as exc:  # noqa: BLE001 — one clean BLOCKED line
        print("%s — %s: %s" % (rc.BLOCKED, type(exc).__name__,
                               rc.redact(str(exc))))
        return 1

    if args.json:
        detail = dict(result.as_dict())
        # Never emit raw component payloads (may be large); keep summaries.
        detail["components"] = [
            {k: c.get(k) for k in ("component", "status", "ok", "run_id")}
            for c in (result.components or [])]
        print(json.dumps(detail, indent=1, sort_keys=True, default=str))

    terminal = result.terminal
    if terminal in (rc.DEGRADED, rc.BLOCKED):
        reason = result.detail.get("reason") or result.status
        print("%s — %s" % (terminal, reason))
    else:
        print(terminal)
    return 0 if terminal != rc.BLOCKED else 1


if __name__ == "__main__":
    raise SystemExit(main())
