r"""scripts/stage20_acceptance_server.py — Stage 20.1 HERMETIC acceptance backend.

Serves the REAL Paper Trader app (real routes, real auth dependency, real ``/ui/`` assets)
with EVERY canonical read seam bound to ONE composed scenario from
``scripts/stage20_ui_fixtures.py``.

Why this exists
---------------
Stage 20's acceptance backend started the real app against an EMPTY store root and seeded
only the reassessment artifact, so every other canonical surface read its own empty store
and rendered an unrelated default world. Seeding the stores alone cannot close that gap:
the eligible market date, the research-input freshness and the market session are resolved
from the wall clock and from research roots the acceptance harness does not own, so the
rendered page could never be made deterministic OR coherent.

Binding the composed scenario at the READ SEAM fixes both at once. The page under test is
the real UI driving the real routes; what those routes return is ONE synthetic world.

Safety
------
* It refuses to bind the live backend port (8001).
* Every persistent store is redirected to a throwaway root before the app is imported.
* Every bound seam is a pure read that returns a precomputed dict — no store is read at
  request time, so no live ledger, order, fill, holding, cash or NAV can be reached, and
  the 29 SUBMITTED NEXT_CLOSE paper orders of the real rebalance are structurally
  unreachable from this process.
* No provider, no prediction service, no write path.

Usage:
    python scripts/stage20_acceptance_server.py --scenario scenario_5_execution_pending
    python scripts/stage20_acceptance_server.py --scenario ... --port 8021 --print-only
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

LIVE_BACKEND_PORT = 8001
DEFAULT_PORT = 8021

#: Every persistent-store env var the app resolves. ALL of them are redirected into the
#: throwaway acceptance root BEFORE the app is imported, so an unbound seam can still only
#: ever reach an empty temp directory — never a live store.
_STORE_ENV_VARS = (
    "PAPER_TRADER_DESK_DIR", "PAPER_TRADER_HOC_DIR", "PAPER_TRADER_REALLOC_DIR",
    "PAPER_TRADER_REASSESSMENT_DIR", "PAPER_TRADER_DRC_DIR",
    "PAPER_TRADER_PORTFOLIO_DECISION_DIR", "PAPER_TRADER_REBALANCE_PLAN_DIR",
    "PAPER_TRADER_CORPORATE_ACTIONS_DIR", "PAPER_TRADER_RESEARCH_AGENT_DIR",
    "PAPER_TRADER_DATA_EXPANSION_DIR", "PAPER_TRADER_RESEARCH_BRIDGE_DIR",
    "PAPER_TRADER_MHZ_INPUTS_DIR", "PAPER_TRADER_MHZ_STORE_DIR",
    "PAPER_TRADER_MHZ_LEDGER_DIR", "PAPER_TRADER_MHZ_ARTIFACT_DIR",
    "PAPER_TRADER_ALPHA_FACTORY_DIR", "PAPER_TRADER_PRICE_ALPHA_FACTORY_DIR",
    "PAPER_TRADER_MONTHLY_EMITTER_WORK_DIR",
)


def redirect_stores(root: Path) -> dict:
    """Point every persistent store at the throwaway acceptance root."""
    out = {}
    for i, var in enumerate(_STORE_ENV_VARS):
        path = root / ("store_%02d" % i)
        os.environ[var] = str(path)
        out[var] = str(path)
    return out


def bind_scenario(scenario: str, root: Path) -> dict:
    """Compose ONE world and bind every canonical read seam of the real app to it.

    Returns the composed scenario so the caller can report/verify it.
    """
    import stage20_ui_fixtures as fx

    composed = fx.compose(scenario, root=root / "compose")
    panels = composed["panels"]

    from paper_trader.api import app as _app
    from paper_trader.api import daily_action_gate as _dag
    from paper_trader.api import daily_close as _dclose
    from paper_trader.api import holding_opportunity_cost as _hoc
    from paper_trader.api import operational_book as _opbook
    from paper_trader.api import portfolio_reassessment as _reassess
    from paper_trader.api import portfolio_state as _pstate
    from paper_trader.api import reallocation_proposal as _realloc
    from paper_trader.api import rebalance_execution as _rebalance

    def _const(payload):
        def _fn(*_a, **_k):
            return payload
        return _fn

    # The routes delegate through MODULE attributes, so rebinding the attribute rebinds
    # the route. `load_workflow_state` is imported into the app namespace by name, so it
    # is bound there instead.
    _app.load_workflow_state = _const(panels["workflow_state"])
    _opbook.load_operational_book = _const(panels["operational_book"])
    _pstate.load_portfolio_state = _const(panels["portfolio_state"])
    _rebalance.load_rebalance_state = _const(panels["rebalance"])
    _hoc.load_holding_opportunity_cost = _const(panels["holding_opportunity_cost"])
    _realloc.load_reallocation_proposal = _const(panels["reallocation_proposal"])
    _reassess.load_portfolio_reassessment = _const(panels["portfolio_reassessment"])
    _dag.load_daily_action_gate = _const(panels["daily_action_gate"])
    _dclose.load_close_progress = _const(fx._close_progress(   # noqa: SLF001
        fx.scenarios()[scenario]))
    return composed


BOUND_SEAMS = (
    "api.app.load_workflow_state",
    "api.operational_book.load_operational_book",
    "api.portfolio_state.load_portfolio_state",
    "api.rebalance_execution.load_rebalance_state",
    "api.holding_opportunity_cost.load_holding_opportunity_cost",
    "api.reallocation_proposal.load_reallocation_proposal",
    "api.portfolio_reassessment.load_portfolio_reassessment",
    "api.daily_action_gate.load_daily_action_gate",
    "api.daily_close.load_close_progress",
)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Stage 20.1 hermetic acceptance backend.")
    ap.add_argument("--scenario", required=True)
    ap.add_argument("--port", type=int, default=DEFAULT_PORT)
    ap.add_argument("--root", default=None)
    ap.add_argument("--api-key", default="stage20-acceptance-key")
    ap.add_argument("--print-only", action="store_true",
                    help="Compose and report the scenario; do not bind a port.")
    args = ap.parse_args(argv)

    if args.port == LIVE_BACKEND_PORT:
        print("REFUSED: the acceptance backend must never bind the live backend port %d"
              % LIVE_BACKEND_PORT, file=sys.stderr)
        return 2

    repo = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(repo))
    sys.path.insert(0, str(repo / "scripts"))
    import stage20_ui_fixtures as fx
    if args.scenario not in fx.SCENARIO_KEYS:
        print("REFUSED: unknown scenario '%s'; expected one of: %s"
              % (args.scenario, ", ".join(fx.SCENARIO_KEYS)), file=sys.stderr)
        return 2

    root = Path(args.root) if args.root else Path(
        tempfile.mkdtemp(prefix="stage20_1_acceptance_"))
    root.mkdir(parents=True, exist_ok=True)
    redirect_stores(root)
    os.environ["PAPER_TRADER_SERVICE_API_KEY"] = args.api_key

    composed = bind_scenario(args.scenario, root)
    cons = composed["consistency"]
    print(json.dumps({
        "scenario": args.scenario, "title": composed["title"],
        "acceptance_root": str(root), "port": args.port, "api_key": args.api_key,
        "bound_seams": list(BOUND_SEAMS),
        "cross_panel_consistent": cons["consistent"],
        "violations": cons["violations"],
        "mutation_action_count": cons["mutation_action_count"],
        "mutation_actions": cons["mutation_actions"],
        "current_rebalance": cons["current_rebalance"],
        "historical_implementation_fill_count":
            cons["historical_implementation_fill_count"],
        "superseded_order_count": cons["superseded_order_count"],
    }, indent=2, sort_keys=True, default=str))

    if not cons["consistent"]:
        print("REFUSED: scenario '%s' is not cross-panel consistent; not serving it."
              % args.scenario, file=sys.stderr)
        return 1
    if args.print_only:
        return 0

    import uvicorn
    from paper_trader.api.app import app
    print("SERVING http://127.0.0.1:%d/ui/#portfolio-manager" % args.port, flush=True)
    uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="warning")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
