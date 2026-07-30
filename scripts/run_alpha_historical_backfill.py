"""
scripts/run_alpha_historical_backfill.py — Alpha Agent Stage 6 CLI.

Deterministic, resumable historical-backfill driver. Acquires point-in-time-safe
history through EXISTING owned providers (Priority 1: Norgate local, survivorship
aware), generates a deterministic coverage matrix + readiness verdict, runs the
Stage 1 reopening lane, and — only when reopened hypotheses become data-ready —
activates one bounded Stage 5 experiment cycle. It NEVER touches operational
trading state, a model, a portfolio, PostgreSQL, the prediction service or the
LLM, never installs/upgrades a package, and never prints a credential.

Modes:
    --mode dry-run          plan + current coverage only; writes nothing to the
                            ingestion tree
    --mode backfill         real acquisition + coverage + readiness + reopen lane
    --mode verify           read-only verification of the latest package
    --mode coverage-report  recompute + print the coverage matrix (read-only)

Terminal lines (exactly one):
    ALPHA_AGENT_STAGE6_READY
    ALPHA_AGENT_STAGE6_PARTIAL
    ALPHA_AGENT_STAGE6_DATA_HOLD
    ALPHA_AGENT_STAGE6_DRY_RUN
    ALPHA_AGENT_STAGE6_VERIFIED
    NO_NEW_HISTORICAL_DATA
    ALPHA_AGENT_STAGE6_BLOCKED — <reason>

Example (Windows PowerShell):
    C:\\Users\\binis\\paper_trader\\.venv-win\\Scripts\\python.exe `
      scripts\\run_alpha_historical_backfill.py `
      --config configs\\alpha_agent\\stage6_historical_backfill.json `
      --mode backfill --activate-stage5 --json
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_PKG_PARENT = str(_REPO.parent)
if _PKG_PARENT not in sys.path:
    sys.path.insert(0, _PKG_PARENT)

from paper_trader.alpha_agent import backfill_contracts as bc  # noqa: E402
from paper_trader.alpha_agent import historical_backfill as hb  # noqa: E402
from paper_trader.alpha_agent import runtime as rt  # noqa: E402


def _ledger_fp(cfg: dict):
    return lambda: rt.fingerprint_ledgers(cfg)


def _activate_stage5(cfg6: dict, result: dict) -> dict:
    """Run one bounded Stage 5 cycle over grounded + reopened hypotheses. The
    fresh data version forces a new run id; reopened statements map to the newly
    unlocked deterministic templates. Research-only."""
    from paper_trader.alpha_agent import experiment_contracts as ec
    from paper_trader.alpha_agent import experiment_factory as ef
    s5_ref = cfg6.get("stage5_config")
    if not s5_ref:
        return {"terminal": "STAGE5_NOT_CONFIGURED"}
    s5_path = Path(s5_ref)
    if not s5_path.is_absolute():
        s5_path = _REPO / s5_ref
    cfg5 = ec.load_config(s5_path)
    grounded = ef.read_grounded_hypotheses(cfg5)
    reopened = result.get("reopened_intake") or []
    if not reopened:
        # fall back to the stable handoff file the backfill wrote
        reopened = ef.read_reopened_hypotheses(cfg5)
    hyps = grounded + reopened
    # A stage6-derived as_of forces a fresh Stage 5 run id reflecting new data.
    as_of = "s6-%s" % (result.get("data_version") or result.get("run_id"))
    s5 = ef.run_stage5_cycle(
        cfg5, as_of=as_of, hypotheses=hyps,
        ledger_fingerprint=lambda: rt.fingerprint_ledgers(cfg5))
    return {"terminal": s5.get("terminal"), "status": s5.get("status"),
            "run_id": s5.get("run_id"), "counts": s5.get("counts"),
            "reopened_used": len(reopened), "grounded_used": len(grounded)}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Alpha Agent Stage 6 historical-backfill engine.")
    ap.add_argument("--config", required=True, help="Stage 6 config.")
    ap.add_argument("--mode", required=True, choices=list(bc.MODES))
    ap.add_argument("--output-root", dest="output_root", default=None)
    ap.add_argument("--as-of", dest="as_of", default="latest")
    ap.add_argument("--activate-stage5", dest="activate", action="store_true",
                    help="After a real backfill, run one bounded Stage 5 cycle "
                         "over grounded + reopened hypotheses.")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    try:
        cfg = bc.load_config(args.config)
    except bc.ConfigError as exc:
        print("%s — %s" % (bc.BLOCKED, exc))
        return 1
    if args.output_root:
        cfg = dict(cfg, backfill_root=args.output_root)

    stage5 = None
    try:
        if args.mode == "verify":
            result = hb.verify_backfill(cfg, ledger_fingerprint=_ledger_fp(cfg))
        else:
            result = hb.run_backfill(cfg, mode=args.mode, as_of=args.as_of,
                                     ledger_fingerprint=_ledger_fp(cfg))
            if args.activate and args.mode == "backfill" \
                    and result.get("terminal") in (bc.READY, bc.PARTIAL):
                stage5 = _activate_stage5(cfg, result)
    except Exception as exc:  # noqa: BLE001 — one clean BLOCKED line
        print("%s — %s: %s" % (bc.BLOCKED, type(exc).__name__,
                               bc.redact(str(exc))))
        return 1

    if args.json:
        payload = {"run_id": result.get("run_id"),
                   "terminal": result.get("terminal"),
                   "status": result.get("status"),
                   "run_dir": result.get("run_dir"),
                   "records_written": result.get("records_written"),
                   "templates_unlocked": result.get("templates_unlocked"),
                   "reopened": result.get("reopened"),
                   "data_gaps": result.get("data_gaps"),
                   "ledgers_unchanged": result.get("ledgers_unchanged"),
                   "stage5_activation": stage5}
        print(json.dumps(payload, indent=1, sort_keys=True, default=str))

    if stage5:
        print("STAGE5_ACTIVATION %s (%s) run=%s" % (
            stage5.get("terminal"), stage5.get("status"),
            stage5.get("run_id")))

    terminal = result.get("terminal")
    reason = result.get("status") or ""
    if terminal in (bc.BLOCKED, bc.DATA_HOLD, bc.PARTIAL):
        print("%s — %s" % (terminal, reason))
    else:
        print(terminal)
    return 1 if terminal == bc.BLOCKED else 0


if __name__ == "__main__":
    raise SystemExit(main())
