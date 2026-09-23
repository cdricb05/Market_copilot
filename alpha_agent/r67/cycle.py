r"""alpha_agent.r67.cycle - the repeatable research-to-capital pass.

WHAT PROBLEM THIS SOLVES
------------------------
Every release since R56 has answered "where should capital go, and what is the
research estate worth?" by hand, in a campaign, once, and then thrown the
answer away. The next release re-derived it. R67's own history is the proof:
establishing the state at the top of this release took a dozen separate reads
across eight owners, and half of what it found - that four registered
challengers can never reach the capital gate, that every hedged structure R66
measured is refused by an authorised policy R66 never checked - had been true
for weeks with nobody able to see it.

This module is the standing answer. It runs the three R67 projections in ONE
pass, over the canonical owners, and emits ONE artifact.

    forward_producer      can anything we have registered ever be funded?
    strategy_inventory    what mechanisms do we own, and what is each worth?
    capital_feasibility   what can this book actually hold, and whose rule says so?

WHAT IT DELIBERATELY IS NOT
---------------------------
It is not a scheduler. It starts nothing, wakes nothing and owns no clock. The
estate already has exactly one research worker
(``PaperTrader-ResearchRuntime`` driving ``alpha_agent.r52.runtime``) and one
operator cycle (``POST /v1/operations/daily-research-cycle/run``); adding a
second would be the defect this codebase has spent several releases removing.
This is a PURE FUNCTION that either of them - or an operator, or a test - can
call, and it is cheap enough to call often because every input is a persisted
read model rather than a recomputation.

It is also not a decision. It produces no proposal, approves nothing, promotes
nothing and moves no capital. It tells an operator what is true; the governed
owners decide what to do about it.

THE THREE CYCLES, AND WHICH ONE THIS SERVES
-------------------------------------------
The charter names three operating cycles: frequent SIGNAL REFRESH, frequent
PORTFOLIO REASSESSMENT, and controlled MODEL RECALIBRATION. This pass serves
the second. It does not refresh a signal (the r52 worker owns that) and it
never recalibrates a model - recalibration is evidence-gated and is not
triggered by a research cycle having occurred, which is precisely the failure
mode the charter warns about.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

CALCULATION_OWNER = "alpha_agent.r67.cycle"
ARTIFACT_NAME = "R67_RESEARCH_TO_CAPITAL.json"

#: Where the artifact lands. Under the research data root, never in the repo and
#: never in an operational store.
DEFAULT_ROOT = Path(r"D:\Stock_Prediction_app_data\research_to_capital_r67")

#: Terminal verdicts for the pass.
C_READY = "RESEARCH_TO_CAPITAL_READY"
C_DEGRADED = "RESEARCH_TO_CAPITAL_DEGRADED"
CYCLE_STATES = (C_READY, C_DEGRADED)


def _safe(fn, label: str, problems: list):
    """Run one projection; a failure degrades the pass, never crashes it.

    A pass that dies because one owner is unavailable tells the operator
    nothing. A pass that completes and names the owner that failed tells them
    exactly where to look, which is the whole purpose.
    """
    try:
        return fn()
    except Exception as exc:                                 # noqa: BLE001
        problems.append({"projection": label,
                         "error": "%s: %s" % (type(exc).__name__, exc)})
        return None


def run(*, root: Optional[Path] = None, execute: bool = False,
        inventory_limit: Optional[int] = 60) -> dict:
    """One research-to-capital pass.

    ``execute=False`` (the default) computes and returns without writing, so a
    caller can look before it commits anything to disk. ``execute=True`` also
    persists the artifact. Nothing else changes between the two, and neither
    touches an operational store in any case.
    """
    from paper_trader.alpha_agent.r67 import (capital_feasibility as CF,
                                              forward_producer as FP,
                                              strategy_inventory as SI)
    from paper_trader.alpha_agent import r67

    problems: list = []
    fwd = _safe(FP.reconcile, "forward_producer", problems)
    inv = _safe(lambda: SI.build(limit=inventory_limit),
                "strategy_inventory", problems)
    pol = _safe(CF.authorised_policy, "capital_feasibility", problems)

    body = {
        "schema_version": "r67_research_to_capital.v1",
        "calculation_owner": CALCULATION_OWNER,
        "release": r67.RELEASE,
        "run_id": r67.RUN_ID,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "state": C_DEGRADED if problems else C_READY,
        "state_vocabulary": list(CYCLE_STATES),
        "problems": problems,
        "serves_operating_cycle": "PORTFOLIO_REASSESSMENT",
        "does_not_serve": ["SIGNAL_REFRESH", "MODEL_RECALIBRATION"],
        "owns_no_clock": True,
        "starts_no_worker": True,
        "forward_producer": fwd,
        "strategy_inventory": inv,
        "authorised_policy": pol,
        "safety": dict(r67.SAFETY),
    }
    body["headline"] = _headline(body)

    if execute:
        out = Path(root or DEFAULT_ROOT)
        out.mkdir(parents=True, exist_ok=True)
        p = out / ARTIFACT_NAME
        p.write_text(json.dumps(body, indent=1, default=str), encoding="utf-8")
        body["artifact_path"] = str(p)
    return body


def _headline(body: dict) -> str:
    """The three sentences an operator needs before reading anything else."""
    fwd, inv, pol = (body.get("forward_producer"),
                     body.get("strategy_inventory"),
                     body.get("authorised_policy"))
    lines = []
    if pol:
        lines.append(
            "Book NAV $%s, free cash $%s, valued %s; the book is LONG ONLY "
            "(short_exposure_supported=%s)."
            % (pol.get("nav_usd"), pol.get("free_cash_usd"),
               pol.get("valuation_date"), pol.get("short_exposure_supported")))
    if inv:
        e = inv.get("estate") or {}
        lines.append(
            "Research estate: %s hypotheses settled across %s economic "
            "mechanisms, %s qualified survivors, %s flagged as regime "
            "artifacts."
            % (e.get("hypotheses_settled"), inv.get("n_mechanisms"),
               e.get("qualified_survivors"),
               inv.get("n_mechanisms_flagged_regime_artifact")))
    if fwd:
        lines.append(
            "Forward book: %s registered, %s with a live cadence producer, %s "
            "ORPHANED, %s matured observations. Soonest any sleeve can clear "
            "the capital gate: %s years."
            % (fwd.get("n_registered"), fwd.get("n_with_live_producer"),
               fwd.get("n_orphaned_defect"),
               fwd.get("matured_observations_total"),
               fwd.get("soonest_years_to_any_capital_floor_estimate")))
    return " ".join(lines) or "no projection completed"
