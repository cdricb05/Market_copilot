"""alpha_agent.r52 - the persistent prospective research runtime (Release 52).

Release 51 measured the estate's real bottleneck: forward evidence accrues
only when the daily cycle runs, a skipped session forfeits its emissions
permanently, and the only scheduled task that could have run anything
(``PaperTrader-InformationCollection``) has a LogonTrigger and never runs the
research advance at all. The 2026-08-25 VX decision was lost exactly this way
and R46.6.2 wrote the refusal down by name.

Release 52 makes prospective evidence capture durable, session-aware,
idempotent and fail-closed - and makes the loss of an opportunity a
first-class, append-only fact instead of a silence:

* :mod:`alpha_agent.r52.timing_contract` - ONE derived timing contract for
  every prospective lane. The scheduler consumes it; it never becomes a
  second timing authority (every rule is read from the canonical owners:
  :mod:`alpha_agent.r46.clock`, :mod:`alpha_agent.r46.lanes`,
  :mod:`alpha_agent.r46.adopted_forward`).
* :mod:`alpha_agent.r52.forfeiture` - THE canonical research-only forfeiture
  ledger. Append-only, chain-hashed with the canonical desk primitives,
  idempotent on (lane_id, decision_date, challenger_scope). A forfeited
  prediction is deliberately absent TRUE_FORWARD evidence, never data to
  reconstruct later: ``backfill_refused`` is ``true`` on every row.
* :mod:`alpha_agent.r52.runtime` - ONE orchestration owner,
  ``research_runtime_cycle()``. It coordinates the canonical owners
  (score -> boards -> emit through ``alpha_agent.r46.advance.advance``, the
  forfeiture sweep, the R51 promotion-frontier refresh, the health read
  model) and calculates no signal, no outcome, no calendar and no promotion
  rule of its own.
* :mod:`alpha_agent.r52.frontier_refresh` - assembles the INJECTED inputs of
  the pure :mod:`alpha_agent.r51.promotion_frontier` from the canonical
  artifacts and persists the refreshed frontier. It can mark a sleeve
  PROMOTION_READY for a human; it can approve nothing.
* :mod:`alpha_agent.r52.velocity_ops` - evidence velocity made operational:
  the split between SCIENTIFICALLY_SLOW (the calendar) and
  OPERATIONALLY_MISSED (the runtime), measured per week.
* :mod:`alpha_agent.r52.health` - one authoritative runtime health read
  model, served by ``GET /v1/research/runtime-health``.

RESEARCH ONLY. This package never calls the portfolio cycle, never runs a
daily close, never mutates a holding, an order, a cash balance, a NAV or an
approval, never promotes a model, never activates a sleeve, and never spends
money. Track B (the live portfolio cycle) is a different owner and a manual
one.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

RELEASE = "R52"

#: The R52 runtime's own state root. The R46 campaign artifacts stay under the
#: R46 research root and are written only by their R46 owners; this root holds
#: what R52 itself owns: the timing contract, the forfeiture ledger, the
#: runtime run journal, the runtime health read model and the refreshed
#: promotion frontier.
RUNTIME_ROOT = Path(r"D:\Stock_Prediction_app_data\research_runtime_r52")

#: The instant this runtime's accountability begins. Opportunities whose legal
#: windows closed before R52 existed are historical facts recorded by their
#: own releases (R46.6.2 wrote the 2026-08-25 VX refusal down); the sweep
#: mirrors recorded refusals verbatim and manufactures nothing earlier than
#: this date on its own authority.
ACCOUNTABILITY_START_DATE = "2026-08-31"

#: Mirrors the R46/R51 convention: declarations the audit and tests bind to.
PROMOTION_ALLOWED = False
AUTOMATIC_PROMOTION_ALLOWED = False
AUTOMATIC_SLEEVE_ACTIVATION_ALLOWED = False
MAY_SPEND_MONEY = False
MAY_MUTATE_PRODUCTION = False
CREATES_ORDERS = False
CALLS_PORTFOLIO_CYCLE = False
RUNS_DAILY_CLOSE = False
BACKFILLS_FORWARD_ROWS = False

SAFETY_BLOCK = {
    "safety": [
        "RESEARCH ONLY",
        "PREVIEW ONLY",
        "NO LIVE BROKER ORDERS",
        "AUTOMATION OFF FOR THE PORTFOLIO",
        "MANUAL REVIEW",
        "NO OPERATIONAL WRITE",
        "NO MODEL PROMOTION",
        "NO SLEEVE ACTIVATION",
        "NO PORTFOLIO CYCLE CALL",
        "NO DAILY CLOSE",
        "NO PURCHASE",
        "NO BACKDATED FORWARD ROW",
        "FORFEITURE IS RECORDED, NEVER REPAIRED",
    ],
    "calls_portfolio_cycle": False,
    "runs_daily_close": False,
    "creates_order": False,
    "creates_proposal": False,
    "approves_proposal": False,
    "mutates_holdings": False,
    "mutates_cash": False,
    "mutates_nav": False,
    "promotes_model": False,
    "activates_sleeve": False,
    "may_spend_money": False,
    "purchases_data": False,
    "backfills_forward_rows": False,
    "writes_operational_store": False,
}


def runtime_dir() -> Path:
    RUNTIME_ROOT.mkdir(parents=True, exist_ok=True)
    return RUNTIME_ROOT


def sha(obj) -> str:
    blob = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def read_json(path, default=None):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


def write_json(path, body) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(body, indent=2, default=str), encoding="utf-8")
    tmp.replace(p)
    return p


def artifact_body(schema: str, owner: str, **extra) -> dict:
    body = {
        "schema": schema,
        "release": RELEASE,
        "calculation_owner": owner,
        "safety_block": dict(SAFETY_BLOCK),
    }
    body.update(extra)
    return body


__all__ = [
    "RELEASE", "RUNTIME_ROOT", "ACCOUNTABILITY_START_DATE",
    "PROMOTION_ALLOWED", "AUTOMATIC_PROMOTION_ALLOWED",
    "AUTOMATIC_SLEEVE_ACTIVATION_ALLOWED", "MAY_SPEND_MONEY",
    "MAY_MUTATE_PRODUCTION", "CREATES_ORDERS", "CALLS_PORTFOLIO_CYCLE",
    "RUNS_DAILY_CLOSE", "BACKFILLS_FORWARD_ROWS", "SAFETY_BLOCK",
    "runtime_dir", "sha", "read_json", "write_json", "artifact_body",
    "timing_contract", "forfeiture", "runtime", "frontier_refresh",
    "velocity_ops", "health",
]
