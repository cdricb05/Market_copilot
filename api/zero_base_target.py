"""api/zero_base_target.py - composition, persistence and read owner for the
Release 30 zero-base allocation.

It owns no mathematics. Every number in the payload comes from an existing
canonical owner:

* eligible universe, sector, liquidity, ranks - ``api.universe_scoring``
* NAV, cash, holdings, current weights        - ``api.portfolio_state``
* expected return / uncertainty / downside    - ``api.return_forecast``
* trailing return series                      - ``api.price_panel``
* covariance                                  - ``engine.holding_opportunity_cost``
* construction caps                           - ``api.multi_horizon_engine``
* transaction cost rate                       - ``api.paper_trading_desk``
* the objective and the optimiser             - ``engine.zero_base_allocator``

What it is NOT: it is not a proposal engine, not a decision owner, and not an
execution path. ``engine.reallocation_proposal`` remains the ONE portfolio
proposal owner and ``api.portfolio_decision`` remains the ONE canonical decision
owner; this module produces an intrinsic target and the economics of reaching it,
which a human reads. It cannot approve anything, and by construction it has no
way to emit an order, a signal or a decision.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from paper_trader.api import multi_horizon_engine as eng
from paper_trader.api import paper_trading_desk as desk
from paper_trader.api import return_forecast as rfc
from paper_trader.engine import zero_base_allocator as kernel

SCHEMA_VERSION = kernel.SCHEMA_VERSION
COMPOSITION_OWNER = "api.zero_base_target"
PHASE = "R30"

STATE_READY = kernel.STATE_READY
STATE_DEGRADED = kernel.STATE_DEGRADED
STATE_BLOCKED = kernel.STATE_BLOCKED
STATE_NO_ACTIVE_BOOK = kernel.STATE_NO_ACTIVE_BOOK
STATE_UNAVAILABLE = "UNAVAILABLE"
READ_STATE_VOCAB = (STATE_READY, STATE_DEGRADED, STATE_BLOCKED,
                    STATE_NO_ACTIVE_BOOK, STATE_UNAVAILABLE)

ZB_DIR_ENV = "PAPER_TRADER_ZERO_BASE_DIR"
_DEFAULT_ZB_DIR = Path(r"D:\Stock_Prediction_app_data\zero_base_targets")

#: How many trailing sessions of returns feed the covariance builder. Wider than
#: the covariance lookback so the builder can discard partial series and still
#: have its minimum observation count.
_RETURN_LOOKBACK = 120

SAFETY_BADGES = list(kernel.SAFETY_BADGES)

# --------------------------------------------------------------------------- #
# Authority - Release 30.1
# --------------------------------------------------------------------------- #
#: A target computed from a model the operator does not run is RESEARCH. A target
#: computed from the CURRENT APPROVED model, on current inputs, with a
#: rank-preserving calibration, is GOVERNED. The two are never shown as equally
#: authoritative, and only the governed one could ever reach a proposal.
LANE_RESEARCH_PREVIEW = "RESEARCH_PREVIEW"
LANE_GOVERNED_OPERATIONAL = "GOVERNED_OPERATIONAL_TARGET"
LANE_VOCAB = (LANE_RESEARCH_PREVIEW, LANE_GOVERNED_OPERATIONAL)

AUTHORITY_DOC = {
    LANE_RESEARCH_PREVIEW: (
        "Computed from a forecasting model that is NOT the approved operational "
        "model and is NOT activated. It answers a research question and carries "
        "no capital authority: it can never become a proposal or a decision."),
    LANE_GOVERNED_OPERATIONAL: (
        "Computed from the CURRENT APPROVED operational model on the current "
        "eligible session, through a calibration that preserves that model's "
        "ranking. This is the only lane a portfolio proposal could ever be "
        "derived from, and only after the existing reassessment gate, the "
        "existing proposal owner and a human."),
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _zb_dir(zero_base_dir=None) -> Path:
    return Path(zero_base_dir or os.environ.get(ZB_DIR_ENV) or _DEFAULT_ZB_DIR)


def _f(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v


# --------------------------------------------------------------------------- #
# Live policy overrides - the canonical constants, never a private fork
# --------------------------------------------------------------------------- #
def _live_policy_overrides() -> dict:
    """Bind the kernel policy to the LIVE canonical constants.

    Every one of these is owned elsewhere. Binding them here at run time is what
    makes the kernel's declared defaults documentation rather than a second
    source of truth: if the desk changes its cost rate, this target changes with
    it and no one has to remember to edit two files.
    """
    return {
        "max_name_weight": float(eng.MAX_INDIVIDUAL_WEIGHT),
        "sector_cap_fraction": float(eng.SECTOR_CAP_FRACTION),
        "min_adv_dollar": float(eng.MIN_ADV_DOLLAR),
        "cost_rate_per_side": float(desk.COST_RATE_PER_SIDE),
        "cost_bps_per_side": float(desk.COST_BPS_PER_SIDE),
    }


def _calibrated_risk_prices(artifact: Optional[dict], horizon: int) -> dict:
    """Risk prices from the frozen artifact's walk-forward calibration.

    Absent a calibration the kernel's NEUTRAL defaults stand and the payload says
    so through ``risk_price_source``. Nothing here invents a value.
    """
    blk = ((artifact or {}).get("horizons") or {}).get(str(int(horizon))) or {}
    rp = blk.get("risk_prices") or {}
    out: dict = {}
    for key, name in (("risk_aversion_gamma", "risk_aversion_gamma"),
                      ("uncertainty_aversion_phi", "uncertainty_aversion_phi"),
                      ("downside_aversion_delta", "downside_aversion_delta"),
                      ("downside_tail_factor", "downside_tail_factor")):
        v = _f(rp.get(key))
        if v is not None:
            out[name] = v
    if out:
        out["risk_price_source"] = "WALK_FORWARD_CALIBRATED"
    return out


def resolve_policy(*, artifact: Optional[dict] = None,
                   policy_overrides: Optional[dict] = None) -> dict:
    pol = dict(kernel.default_policy())
    pol.update(_live_policy_overrides())
    pol.update(_calibrated_risk_prices(artifact,
                                       int(pol["policy_horizon_sessions"])))
    if policy_overrides:
        pol.update(policy_overrides)
    return pol


# --------------------------------------------------------------------------- #
# Input contract
# --------------------------------------------------------------------------- #
def build_input_contract(*, portfolio_state: dict, scoring: dict,
                         forecast: dict, price_panel: Optional[dict] = None,
                         policy: Optional[dict] = None) -> dict:
    """Assemble the immutable zero-base input contract.

    The candidate set is the ELIGIBLE universe from the scoring owner - the whole
    of it, not the part we happen to hold. That is the zero-base property, and it
    is established here at the input boundary rather than left to the optimiser
    to respect.
    """
    from paper_trader.api import price_panel as pp
    pol = policy or resolve_policy()
    ps = portfolio_state or {}
    dates = ps.get("dates") or {}
    capital = ps.get("capital") or {}
    book = ps.get("active_book") or {}
    eligible = (dates.get("eligible_market_date")
                or (scoring or {}).get("eligible_market_date"))

    candidates = []
    for r in (scoring or {}).get("rankings") or []:
        tk = r.get("ticker")
        if not tk or not r.get("eligible", True):
            continue
        adv = _f(r.get("adv_dollar"))
        if adv is not None and adv < float(pol["min_adv_dollar"]):
            continue
        candidates.append({"ticker": tk, "sector": r.get("sector") or "Unknown",
                           "adv_dollar": adv, "rank": r.get("rank"),
                           "percentile": _f(r.get("percentile"))})

    positions = ps.get("positions") or []
    nav = _f(capital.get("nav"))
    current = {}
    for p in positions:
        tk = p.get("ticker")
        w = _f(p.get("portfolio_weight"))
        if tk and w is not None:
            current[tk] = w

    horizon = int(pol["policy_horizon_sessions"])
    from paper_trader.engine import return_forecast as fk
    mu = fk.expected_returns(forecast or {}, horizon)
    sigma = fk.uncertainties(forecast or {}, horizon)
    down = fk.downside(forecast or {}, horizon)
    per_h = {str(h): fk.expected_returns(forecast or {}, h)
             for h in (forecast or {}).get("horizons") or []}

    universe = sorted({c["ticker"] for c in candidates} | set(current))
    panel = price_panel if price_panel is not None else pp.load_operational_price_panel()
    ar = pp.aligned_returns(price_panel=panel, tickers=universe,
                            as_of=eligible or "", lookback=_RETURN_LOOKBACK)

    return {
        "input_schema_version": kernel.INPUT_SCHEMA_VERSION,
        "eligible_market_date": eligible,
        "active_book_id": book.get("book_id"),
        "nav": nav,
        "cash": _f(capital.get("cash")),
        "candidates": candidates,
        "current_weights": current,
        "mu": mu,
        "sigma_forecast": sigma,
        "downside": down,
        "expected_returns_by_horizon": per_h,
        "aligned_returns": ar,
        "forecast_model_spec_hash": (forecast or {}).get("model_spec_hash"),
        "feature_snapshot_hash": (forecast or {}).get("feature_snapshot_hash"),
        "portfolio_state_hash": ps.get("state_hash"),
        "universe_scoring_hash": (scoring or {}).get("output_hash"),
        "sources": {
            "universe": "api.universe_scoring",
            "portfolio": "api.portfolio_state",
            "forecast": "api.return_forecast",
            "returns": "api.price_panel.aligned_returns",
            "covariance": "engine.holding_opportunity_cost.build_covariance",
            "cost": "api.paper_trading_desk",
            "caps": "api.multi_horizon_engine",
        },
    }


# --------------------------------------------------------------------------- #
# Run / read
# --------------------------------------------------------------------------- #
def run_allocation(*, portfolio_state: Optional[dict] = None,
                   scoring: Optional[dict] = None,
                   forecast: Optional[dict] = None,
                   price_panel: Optional[dict] = None,
                   artifact: Optional[dict] = None,
                   policy_overrides: Optional[dict] = None) -> dict:
    """Compute the zero-base allocation. PURE - no write, no mutation."""
    from paper_trader.api import portfolio_state as ps_owner
    from paper_trader.api import universe_scoring as us

    ps = portfolio_state if portfolio_state is not None else ps_owner.load_portfolio_state()
    sc = scoring if scoring is not None else us.load_universe_scoring()
    art = artifact if artifact is not None else rfc.load_model_artifact()
    fc = forecast if forecast is not None else rfc.build(artifact=art)
    pol = resolve_policy(artifact=art, policy_overrides=policy_overrides)
    ic = build_input_contract(portfolio_state=ps, scoring=sc, forecast=fc,
                              price_panel=price_panel, policy=pol)
    result = kernel.build_allocation(input_contract=ic, policy=pol)
    result["composition_owner"] = COMPOSITION_OWNER
    result["generated_at"] = _now_iso()
    # Release 30.1: this lane is RESEARCH. It is built from a forecasting model
    # the operator does not run, and saying so in the payload - not only in a
    # footnote - is what stops it being read as a portfolio instruction.
    result["authority"] = {
        "lane": LANE_RESEARCH_PREVIEW,
        "vocabulary": list(LANE_VOCAB),
        "doc": AUTHORITY_DOC[LANE_RESEARCH_PREVIEW],
        "forecast_state": fc.get("state"),
        "forecast_operational_use": fc.get("operational_use"),
        "activation_state": (fc.get("activation") or {}).get("state"),
        "can_become_a_proposal": False,
    }
    result["forecast"] = rfc.summary(fc)
    result["input_contract_summary"] = {
        "eligible_candidates": len(ic["candidates"]),
        "current_positions": len(ic["current_weights"]),
        "return_series_names": len((ic["aligned_returns"] or {}).get("series") or {}),
        "return_series_dates": len((ic["aligned_returns"] or {}).get("dates") or []),
        "portfolio_state_hash": ic.get("portfolio_state_hash"),
        "universe_scoring_hash": ic.get("universe_scoring_hash"),
        "sources": ic["sources"],
    }
    return result


def run_operational_allocation(*, portfolio_state: Optional[dict] = None,
                               scoring: Optional[dict] = None,
                               forecast: Optional[dict] = None,
                               price_panel: Optional[dict] = None,
                               artifact: Optional[dict] = None,
                               policy_overrides: Optional[dict] = None) -> dict:
    """The GOVERNED operational zero-base allocation. PURE - no write, no mutation.

    Everything is the same owner as the research lane except the forecast, which
    here is the CURRENT APPROVED model's own calibrated representation. When that
    forecast is blocked this returns a BLOCKED allocation carrying the forecast's
    reasons; it NEVER falls back to the research forecast, because a target the
    operator would read as governed must not be able to come from a model the
    operator does not run.
    """
    from paper_trader.api import portfolio_state as ps_owner
    from paper_trader.api import universe_scoring as us

    ps = portfolio_state if portfolio_state is not None else ps_owner.load_portfolio_state()
    sc = scoring if scoring is not None else us.load_universe_scoring()
    art = artifact if artifact is not None else rfc.load_operational_artifact()
    fc = (forecast if forecast is not None
          else rfc.build_operational(scoring=sc, artifact=art))
    pol = resolve_policy(artifact=art, policy_overrides=policy_overrides)

    authority = {
        "lane": LANE_GOVERNED_OPERATIONAL,
        "vocabulary": list(LANE_VOCAB),
        "doc": AUTHORITY_DOC[LANE_GOVERNED_OPERATIONAL],
        "operational_model_id": sc.get("primary_model_id"),
        "score_owner": rfc.OPERATIONAL_SCORE_OWNER,
        "forecast_lane": fc.get("lane"),
        "model_identity_contract": fc.get("model_identity_contract"),
        "applied_horizons": fc.get("horizons") or [],
        "suppressed_horizons": fc.get("suppressed_horizons") or [],
    }

    horizon = int(pol["policy_horizon_sessions"])
    if horizon not in (fc.get("horizons") or []):
        blockers = list(fc.get("blockers") or [])
        blockers.append({
            "code": "POLICY_HORIZON_NOT_CALIBRATED",
            "policy_horizon_sessions": horizon,
            "applied_horizons": fc.get("horizons") or [],
            "detail": ("the approved model supplies no rank-preserving, "
                       "reliable expected return at the policy horizon, so no "
                       "governed target can be computed without fabricating "
                       "one"),
        })
        out = kernel.build_allocation(input_contract={
            "input_schema_version": kernel.INPUT_SCHEMA_VERSION,
            "eligible_market_date": (ps.get("dates") or {}).get("eligible_market_date"),
            "active_book_id": (ps.get("active_book") or {}).get("book_id"),
        }, policy=pol)
        out["state"] = STATE_BLOCKED
        out["blockers"] = blockers
        out["composition_owner"] = COMPOSITION_OWNER
        out["generated_at"] = _now_iso()
        out["authority"] = authority
        out["forecast"] = {"state": fc.get("state"),
                           "lane": fc.get("lane"),
                           "operational_use": fc.get("operational_use"),
                           "eligible_market_date": fc.get("eligible_market_date"),
                           "input_freshness": fc.get("input_freshness"),
                           "horizons": fc.get("horizons") or [],
                           "suppressed_horizons": fc.get("suppressed_horizons") or [],
                           "model_spec_hash": fc.get("model_spec_hash")}
        return out

    ic = build_input_contract(portfolio_state=ps, scoring=sc, forecast=fc,
                              price_panel=price_panel, policy=pol)
    result = kernel.build_allocation(input_contract=ic, policy=pol)
    result["composition_owner"] = COMPOSITION_OWNER
    result["generated_at"] = _now_iso()
    result["authority"] = authority
    result["forecast"] = rfc.summary(fc)
    result["input_contract_summary"] = {
        "eligible_candidates": len(ic["candidates"]),
        "current_positions": len(ic["current_weights"]),
        "return_series_names": len((ic["aligned_returns"] or {}).get("series") or {}),
        "return_series_dates": len((ic["aligned_returns"] or {}).get("dates") or []),
        "portfolio_state_hash": ic.get("portfolio_state_hash"),
        "universe_scoring_hash": ic.get("universe_scoring_hash"),
        "sources": ic["sources"],
    }
    return result


def load_operational_zero_base_target(**kwargs) -> dict:
    """Read surface for the GOVERNED operational lane. Degrades, never raises."""
    try:
        return run_operational_allocation(**kwargs)
    except Exception as exc:                                       # noqa: BLE001
        return {
            "schema_version": SCHEMA_VERSION,
            "composition_owner": COMPOSITION_OWNER, "phase": "R30.1",
            "state": STATE_UNAVAILABLE,
            "state_vocabulary": list(READ_STATE_VOCAB),
            "generated_at": _now_iso(),
            "authority": {"lane": LANE_GOVERNED_OPERATIONAL,
                          "vocabulary": list(LANE_VOCAB),
                          "doc": AUTHORITY_DOC[LANE_GOVERNED_OPERATIONAL]},
            "zero_base_target": {"rows": [], "economics": {}},
            "implementable_target": {"rows": [], "economics": {}},
            "current_portfolio": {"weights": {}, "economics": {}},
            "comparison": {}, "transition": {},
            "blockers": [{"code": "OPERATIONAL_ZERO_BASE_TARGET_UNAVAILABLE",
                          "detail": type(exc).__name__}],
            "warnings": [],
            "safety": {"badges": list(SAFETY_BADGES), "creates_orders": False,
                       "creates_decisions": False, "mutates_holdings": False},
        }


def load_zero_base_target(**kwargs) -> dict:
    """The read surface. Degrades to an explicit UNAVAILABLE payload instead of
    raising, so a missing research artifact can never take down the operator
    surface."""
    try:
        return run_allocation(**kwargs)
    except Exception as exc:                                  # noqa: BLE001
        return {
            "schema_version": SCHEMA_VERSION,
            "composition_owner": COMPOSITION_OWNER, "phase": PHASE,
            "state": STATE_UNAVAILABLE,
            "state_vocabulary": list(READ_STATE_VOCAB),
            "generated_at": _now_iso(),
            "zero_base_target": {"rows": [], "economics": {}},
            "implementable_target": {"rows": [], "economics": {}},
            "current_portfolio": {"weights": {}, "economics": {}},
            "comparison": {}, "transition": {},
            "blockers": [{"code": "ZERO_BASE_TARGET_UNAVAILABLE",
                          "detail": type(exc).__name__}],
            "warnings": [],
            "safety": {"badges": list(SAFETY_BADGES), "creates_orders": False,
                       "creates_decisions": False, "mutates_holdings": False},
        }


def summary(payload: Optional[dict] = None, **kwargs) -> dict:
    """Compact block for Today / Portfolio composition."""
    p = payload if payload is not None else load_zero_base_target(**kwargs)
    zb = (p.get("zero_base_target") or {}).get("economics") or {}
    impl = (p.get("implementable_target") or {}).get("economics") or {}
    cur = (p.get("current_portfolio") or {}).get("economics") or {}
    cmp_ = p.get("comparison") or {}
    tr = (p.get("transition") or {}).get("current_to_implementable") or {}
    return {
        "state": p.get("state"),
        "eligible_market_date": p.get("eligible_market_date"),
        "policy_horizon_sessions": p.get("policy_horizon_sessions"),
        "current": {"positions": cur.get("position_count"),
                    "cash_weight": cur.get("cash_weight"),
                    "expected_excess_return": cur.get("expected_excess_return"),
                    "expected_net_utility": cur.get("expected_net_utility")},
        "zero_base": {"positions": zb.get("position_count"),
                      "cash_weight": zb.get("cash_weight"),
                      "expected_excess_return": zb.get("expected_excess_return"),
                      "expected_net_utility": zb.get("expected_net_utility")},
        "implementable": {"positions": impl.get("position_count"),
                          "cash_weight": impl.get("cash_weight"),
                          "expected_excess_return": impl.get("expected_excess_return"),
                          "expected_net_utility": impl.get("expected_net_utility")},
        "overlap": {"retained": (cmp_.get("zero_base") or {}).get("retained_count"),
                    "removed": (cmp_.get("zero_base") or {}).get("removed_count"),
                    "new": (cmp_.get("zero_base") or {}).get("new_count")},
        "transition": {"one_way_turnover": tr.get("one_way_turnover"),
                       "transaction_cost_dollars": tr.get("transaction_cost_dollars")},
        "forecast": p.get("forecast") or {},
        "allocation_hash": p.get("allocation_hash"),
    }


def persist_allocation(*, result: dict, zero_base_dir=None) -> dict:
    """Persist one computed allocation as research evidence.

    First-write-wins per (book, eligible date, allocation hash): a target that
    already exists is never rewritten. Deliberately not called by any GET.
    """
    d = _zb_dir(zero_base_dir) / "artifacts"
    key = "%s_%s_%s" % (result.get("active_book_id") or "no_book",
                        result.get("eligible_market_date") or "no_date",
                        str(result.get("allocation_hash") or "no_hash")[:16])
    path = d / (key + ".json")
    if path.exists():
        return {"state": "ALREADY_PERSISTED", "path": str(path)}
    d.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(result, indent=1, default=str), encoding="utf-8")
    tmp.replace(path)
    return {"state": "PERSISTED", "path": str(path)}
