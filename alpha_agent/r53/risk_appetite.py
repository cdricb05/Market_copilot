r"""alpha_agent.r53.risk_appetite - Track A: is the production risk policy
protection or dilution? Answered empirically, without touching production.

THREE INSTRUMENTS, ONE CANONICAL KERNEL
---------------------------------------
1. :func:`current_policy_inventory` - the production policy read VERBATIM from
   its two canonical owners (``engine.constrained_reallocation.default_policy``
   and ``engine.reallocation_proposal.default_policy``). Nothing here declares
   a threshold of its own.

2. :func:`live_binding_analysis` - a census over the REAL governed artifacts
   (the reallocation proposal, the DRC run manifests, the recorded decisions):
   which constraints actually reshaped, how far the real decisions sat from
   each limit, and which limits have never once bound.

3. :func:`run_walkforward` - a walk-forward policy-REGION study on the owned
   survivorship-free Russell-1000 panel (``alpha_agent.release30_panel``, PIT
   membership, 2000-2026). Every portfolio construction step calls the
   CANONICAL constraint kernel (``solve_feasible_target`` ->
   ``switching_economics`` -> ``decide_outcome``) with a VARIANT policy dict -
   the same injection seam production uses - so no second allocator exists.

WHAT KEEPS THIS HONEST
----------------------
* The signal is a PROXY and says so: 12-1 price momentum percentile, the
  momentum leg of the production sleeve. The fundamental leg cannot be
  reconstructed point-in-time without vintage fundamentals, and fabricating
  it is forbidden; every artifact carries ``signal_is_proxy: true``.
* No parameter is fitted to the evaluation outcomes. The three shadow
  policies are declared a priori from economically round values, BEFORE any
  zone is scored; the axis sweeps report REGIONS (a metric per grid point in
  both zones), never a champion combination.
* Development and validation are separated in time and reported separately.
  A conclusion that holds only in one zone is labelled exactly that.
* The sector-cap axis is NOT swept historically - the frozen panel carries no
  point-in-time sector classification and inventing one would be silent
  look-ahead. It is measured on the live cross-sections instead.
* Gross exposure is not swept - the canonical kernel is long-only with gross
  <= 100% by architecture; a shadow gross > 1 would need an allocator this
  estate does not have, and building one is forbidden by principle 1.

SHADOW ONLY. The production policy dict is never modified; every variant
lives in a local copy handed to pure kernel functions.
"""
from __future__ import annotations

import math
from pathlib import Path
from typing import Any, Optional

import numpy as np

from . import (CAMPAIGN_ID, EVIDENCE_ROOT, RELEASE, artifact_body, read_json,
               research_dir, safety_block, sha, write_json)

CALCULATION_OWNER = "alpha_agent.r53.risk_appetite"

ARTIFACT_INVENTORY = "R53_POLICY_INVENTORY.json"
ARTIFACT_LIVE_BINDING = "R53_LIVE_BINDING_ANALYSIS.json"
ARTIFACT_WALKFORWARD = "R53_RISK_APPETITE_WALKFORWARD.json"
ARTIFACT_SHADOW_POLICIES = "R53_SHADOW_POLICIES.json"

#: Decision cadence in sessions. Two calendar weeks: frequent enough to see
#: hysteresis and turnover budgets bind, coarse enough that 25 years of
#: decisions stay computable. Reassessment cadence, not trading cadence - the
#: hurdle decides whether a decision trades.
DECISION_STEP_SESSIONS = 10
#: Formation window for the proxy signal (the production momentum convention).
MOMENTUM_FORMATION = 252
MOMENTUM_SKIP = 21
MIN_HISTORY_SESSIONS = 260
MIN_CROSS_SECTION = 80
#: The zone boundary: decisions strictly before it are DEVELOPMENT, at or
#: after it VALIDATION. Declared once, before any zone was scored.
VALIDATION_START = "2018-01-01"

TRADING_DAYS = 252.0

SIGNAL_PROXY_NOTE = (
    "12-1 price-momentum percentile - the momentum leg of the production "
    "sleeve. The production signal is 50/50 fundamental+momentum; the "
    "fundamental leg cannot be rebuilt point-in-time and is NOT fabricated. "
    "Policy conclusions are therefore about the CONTROLS, conditional on a "
    "signal of comparable character, not a re-estimate of production alpha.")


# --------------------------------------------------------------------------- #
# 1. The production policy, verbatim
# --------------------------------------------------------------------------- #
def current_policy_inventory() -> dict:
    from paper_trader.engine import constrained_reallocation as CR
    from paper_trader.engine import reallocation_proposal as RP
    cr, rp = CR.default_policy(), RP.default_policy()
    body = artifact_body(
        "r53_policy_inventory/1", CALCULATION_OWNER,
        release=RELEASE, campaign_id=CAMPAIGN_ID,
        constraint_owner="engine.constrained_reallocation.default_policy",
        allocation_owner="engine.reallocation_proposal.default_policy",
        constraint_policy=cr,
        allocation_policy=rp,
        constraint_inventory=CR.constraint_inventory(cr),
        shared_values_agree={
            k: (cr.get(k) == rp.get(k))
            for k in ("target_position_count", "max_name_weight",
                      "sector_cap_fraction", "min_adv_dollar",
                      "max_one_way_turnover", "cost_rate_per_side")},
        **safety_block(),
    )
    write_json(research_dir() / ARTIFACT_INVENTORY, body)
    return body


# --------------------------------------------------------------------------- #
# 2. Binding census over the real governed artifacts
# --------------------------------------------------------------------------- #
_DRC_DIR = Path(r"D:\Stock_Prediction_app_data\daily_research_cycle\runs")
_PROPOSAL_DIR = Path(r"D:\Stock_Prediction_app_data\reallocation_proposals")
_REASSESS_DIR = Path(r"D:\Stock_Prediction_app_data\portfolio_reassessments")
_DECISIONS = Path(r"D:\Stock_Prediction_app_data\portfolio_decisions\decisions.json")


def _hurdle_census(manifests: list[dict]) -> dict:
    rows = []
    for m in manifests:
        imp = m.get("reallocation_score_improvement")
        net = m.get("reallocation_score_improvement_net_of_cost")
        if imp is None and net is None:
            continue
        rows.append({
            "run_id": m.get("run_id"),
            "market_date": m.get("eligible_market_date"),
            "score_improvement": imp,
            "net_of_cost": net,
            "one_way_turnover": m.get("reallocation_one_way_turnover"),
            "estimated_cost": m.get("reallocation_estimated_transaction_cost"),
            "decision": m.get("portfolio_reassessment_decision"),
        })
    nets = [r["net_of_cost"] for r in rows if r["net_of_cost"] is not None]
    return {
        "n_observations": len(rows),
        "rows": rows,
        "net_improvement_max": max(nets) if nets else None,
        "net_improvement_min": min(nets) if nets else None,
        "hurdle": 0.05,
        "hurdle_cleared_count": sum(1 for v in nets if v >= 0.05),
        "closest_approach_to_hurdle": (max(nets) if nets else None),
        "finding": ("the switching hurdle has bound on EVERY governed "
                    "observation to date" if nets and max(nets) < 0.05
                    else "the hurdle has been cleared at least once"),
    }


def live_binding_analysis() -> dict:
    manifests = []
    if _DRC_DIR.exists():
        for p in sorted(_DRC_DIR.glob("drc_*.json")):
            m = read_json(p, default=None)
            if m:
                manifests.append(m)
    proposals = []
    if _PROPOSAL_DIR.exists():
        for p in sorted(_PROPOSAL_DIR.glob("*.json")):
            pr = read_json(p, default=None)
            if pr:
                proposals.append({"file": p.name, "body": pr})

    # Which constraints REALLY reshaped the real proposals.
    reshaped: dict[str, int] = {}
    turnover_rows = []
    for entry in proposals:
        pr = entry["body"]
        for holder in (pr, pr.get("constrained_reallocation") or {},
                       pr.get("solution") or {}):
            for code in holder.get("constraints_that_reshaped") or []:
                reshaped[code] = reshaped.get(code, 0) + 1
        t = pr.get("turnover") or {}
        if t:
            turnover_rows.append({"file": entry["file"],
                                  "one_way": t.get("one_way_turnover"),
                                  "budget": t.get("max_one_way_turnover")})

    decisions = read_json(_DECISIONS, default=None)
    if isinstance(decisions, list):
        decisions = {"decisions": decisions}
    decisions = decisions or {}
    body = artifact_body(
        "r53_live_binding_analysis/1", CALCULATION_OWNER,
        release=RELEASE, campaign_id=CAMPAIGN_ID,
        n_drc_manifests=len(manifests),
        n_proposals=len(proposals),
        hurdle_census=_hurdle_census(manifests),
        constraints_that_reshaped_live=reshaped,
        turnover_observations=turnover_rows,
        recorded_decisions_summary={
            "n": len(decisions.get("decisions") or []) if isinstance(
                decisions.get("decisions"), list) else None},
        evidence_scope_note=(
            "the governed history is %d research-cycle manifests and %d "
            "proposal artifacts - weeks, not years. The live census states "
            "what HAS bound; the walk-forward study states what WOULD bind. "
            "Neither is asked to answer the other's question."
            % (len(manifests), len(proposals))),
        **safety_block(),
    )
    write_json(research_dir() / ARTIFACT_LIVE_BINDING, body)
    return body


# --------------------------------------------------------------------------- #
# 3. Walk-forward policy-region study
# --------------------------------------------------------------------------- #
def _harness_policy(**over) -> dict:
    """A VARIANT policy dict for the canonical kernel: the production policy
    with the harness's declared deviations, then the axis override. The
    production dict itself is never modified."""
    from paper_trader.engine import constrained_reallocation as CR
    pol = dict(CR.default_policy())
    pol.update({
        # No PIT sector classification exists in the frozen panel; a sweep
        # against an invented one would be look-ahead. Disabled HERE ONLY.
        "sector_cap_fraction": 1.0,
        # Harness bookkeeping (not kernel inputs).
        "entry_rank": int(pol["target_position_count"]),
        "exit_buffer_rank": 30,
        "cooldown_sessions": 0,
    })
    pol.update(over)
    pol.setdefault("candidate_rank_max", 2 * int(pol["entry_rank"]))
    return pol


def base_config() -> dict:
    return {"config_id": "BASE_PRODUCTION_MIRROR", "overrides": {}}


def axis_sweep_configs() -> list[dict]:
    """One-at-a-time deviations from the production mirror. Regions, not a
    grid search: every config is reported in both zones; none is crowned."""
    cfgs: list[dict] = [base_config()]
    for n in (8, 10, 12, 15, 20, 30):
        cfgs.append({"config_id": "COUNT_%d" % n,
                     "axis": "position_count",
                     "overrides": {"target_position_count": n,
                                   "entry_rank": n,
                                   "exit_buffer_rank": int(math.ceil(n * 1.2)),
                                   "candidate_rank_max": 2 * n}})
    for cap in (0.04, 0.06, 0.08, 0.12, 0.15):
        cfgs.append({"config_id": "NAMECAP_%d" % int(cap * 100),
                     "axis": "max_name_weight",
                     "overrides": {"max_name_weight": cap}})
    for h in (0.0, 0.02, 0.035, 0.075, 0.10):
        cfgs.append({"config_id": "HURDLE_%s" % str(h).replace(".", "p"),
                     "axis": "switching_hurdle",
                     "overrides": {"min_switching_net_improvement": h}})
    for t in (0.10, 0.15, 0.25, 0.50, 0.75):
        cfgs.append({"config_id": "TURNOVER_%d" % int(t * 100),
                     "axis": "max_one_way_turnover",
                     "overrides": {"max_one_way_turnover": t}})
    for cd in (5, 21, 63):
        cfgs.append({"config_id": "COOLDOWN_%d" % cd,
                     "axis": "reentry_cooldown_sessions",
                     "overrides": {"cooldown_sessions": cd}})
    for mult in (1.0, 1.6, 2.0):
        cfgs.append({"config_id": "EXITBUF_%dX" % int(mult * 10),
                     "axis": "exit_buffer_hysteresis",
                     "overrides": {"exit_buffer_rank":
                                   int(math.ceil(25 * mult))}})
    return cfgs


#: The three SHADOW policies, declared a priori from economically round
#: values BEFORE any zone was scored (the freeze is this source line plus its
#: hash in the artifact). They are challengers to compare PROSPECTIVELY;
#: none of them touches production in Release 53.
SHADOW_POLICY_DEFINITIONS = {
    "CURRENT_CONSERVATIVE_POLICY": {
        "rationale": "the production policy verbatim - the incumbent",
        "overrides": {},
    },
    "MODERATE_ACTIVE_POLICY": {
        "rationale": "concentrate to 15 names (equal weight 6.7%, inside the "
                     "unchanged 10% cap), halve the switching hurdle to "
                     "0.035, widen the turnover budget to 50% and the exit "
                     "buffer to 24 - more willing to act, same per-name and "
                     "liquidity protections",
        "overrides": {"target_position_count": 15, "entry_rank": 15,
                      "exit_buffer_rank": 24, "candidate_rank_max": 30,
                      "min_switching_net_improvement": 0.035,
                      "max_one_way_turnover": 0.50},
    },
    "HIGH_ACTIVE_POLICY": {
        "rationale": "ten names at a 12% cap, hurdle 0.02, turnover budget "
                     "75%, exit buffer 20 - the aggressive end of the "
                     "defensible region; liquidity floor and long-only "
                     "unchanged",
        "overrides": {"target_position_count": 10, "entry_rank": 10,
                      "exit_buffer_rank": 20, "candidate_rank_max": 20,
                      "max_name_weight": 0.12,
                      "min_switching_net_improvement": 0.02,
                      "max_one_way_turnover": 0.75},
    },
}


def shadow_policy_configs() -> list[dict]:
    return [{"config_id": name, "axis": "shadow_policy",
             "overrides": dict(d["overrides"]), "rationale": d["rationale"]}
            for name, d in SHADOW_POLICY_DEFINITIONS.items()]


# ---- panel machinery ------------------------------------------------------- #
class _Panel:
    """The owned panel plus the derived arrays the simulation loops over."""

    def __init__(self, panel) -> None:
        self.dates = panel.dates
        self.symbols = [str(s) for s in panel.symbols]
        self.close = panel.close
        self.member = panel.member
        with np.errstate(invalid="ignore"):
            self.adv20 = np.full_like(panel.dvol, np.nan)
            csum = np.nancumsum(panel.dvol, axis=0)
            self.adv20[20:] = (csum[20:] - csum[:-20]) / 20.0
        self.source = panel.source

    def iso(self, t: int) -> str:
        return str(self.dates[t])[:10]


def load_panel() -> _Panel:
    from .. import release30_panel as RP30
    return _Panel(RP30.load_price_panel())


def eligible_and_scores(pn: _Panel, t: int, min_adv: float) -> dict:
    """Eligible tickers with 12-1 momentum percentile at decision index t.
    Uses rows <= t only."""
    if t < MIN_HISTORY_SESSIONS:
        return {}
    c_now = pn.close[t - MOMENTUM_SKIP]
    c_then = pn.close[t - MOMENTUM_FORMATION]
    adv = pn.adv20[t]
    ok = (pn.member[t] & np.isfinite(pn.close[t]) & np.isfinite(c_now)
          & np.isfinite(c_then) & (c_then > 0) & np.isfinite(adv)
          & (adv >= float(min_adv)))
    idx = np.flatnonzero(ok)
    if idx.size < MIN_CROSS_SECTION:
        return {}
    mom = c_now[idx] / c_then[idx] - 1.0
    order = np.argsort(-mom, kind="stable")
    out = {}
    n = idx.size
    for rank_pos, oi in enumerate(order):
        i = idx[oi]
        out[pn.symbols[i]] = {
            "rank": rank_pos + 1,
            "score": 1.0 - rank_pos / max(1, n - 1),
            "adv_dollar": float(adv[i]),
            "col": int(i),
        }
    return out


def _ideal_target(*, current: dict, universe: dict, policy: dict,
                  cooldown_until: dict, t: int) -> dict:
    """The hysteresis ideal: incumbents stay while ranked inside the exit
    buffer; vacant slots fill with the best-ranked eligible entrants inside
    the entry rank, skipping names still in re-entry cooldown. Equal weight.
    This mirrors the production entry/exit convention; the canonical kernel
    then repairs it under every mandatory constraint."""
    n_target = int(policy["target_position_count"])
    entry_rank = int(policy["entry_rank"])
    exit_rank = int(policy["exit_buffer_rank"])
    keep = [tk for tk in current
            if tk in universe and universe[tk]["rank"] <= exit_rank]
    keep = sorted(keep, key=lambda tk: universe[tk]["rank"])[:n_target]
    held = set(keep)
    entrants = [tk for tk, row in universe.items()
                if tk not in held and row["rank"] <= entry_rank
                and cooldown_until.get(tk, -1) < t]
    entrants = sorted(entrants, key=lambda tk: universe[tk]["rank"])
    book = keep + entrants[:max(0, n_target - len(keep))]
    if not book:
        return {}
    w = 1.0 / n_target
    return {tk: w for tk in book}


def _zone(date_iso: str) -> str:
    return "VALIDATION" if date_iso >= VALIDATION_START else "DEVELOPMENT"


def _metrics(daily: list[tuple[str, float]], turnovers: list[float],
             decisions: int, traded: int, hhis: list[float]) -> dict:
    if not daily:
        return {"n_days": 0}
    r = np.array([x[1] for x in daily], dtype=float)
    nav = np.cumprod(1.0 + r)
    peak = np.maximum.accumulate(nav)
    dd = nav / peak - 1.0
    mu = float(np.mean(r) * TRADING_DAYS)
    sd = float(np.std(r, ddof=1) * math.sqrt(TRADING_DAYS)) if len(r) > 2 else 0.0
    years = len(r) / TRADING_DAYS
    return {
        "n_days": int(len(r)),
        "total_return": round(float(nav[-1]) - 1.0, 6),
        "cagr": round(float(nav[-1]) ** (1.0 / years) - 1.0, 6) if years > 0 else None,
        "ann_return": round(mu, 6),
        "ann_volatility": round(sd, 6),
        "sharpe": round(mu / sd, 4) if sd > 0 else None,
        "max_drawdown": round(float(dd.min()), 6),
        "avg_one_way_turnover_per_decision": (
            round(float(np.mean(turnovers)), 6) if turnovers else 0.0),
        "decisions": int(decisions),
        "decisions_traded": int(traded),
        "trade_rate": round(traded / decisions, 4) if decisions else None,
        "avg_hhi": round(float(np.mean(hhis)), 6) if hhis else None,
    }


def simulate_config(pn: _Panel, config: dict, *, nav_usd: float = 1.0e5,
                    start: Optional[str] = None, end: Optional[str] = None
                    ) -> dict:
    """Walk one policy config forward through the panel via the canonical
    kernel. Pure function of the panel and the config; writes nothing."""
    from paper_trader.engine import constrained_reallocation as CR
    pol = _harness_policy(**config.get("overrides", {}))
    cost_rate = float(pol["cost_rate_per_side"])
    cooldown = int(pol.get("cooldown_sessions", 0))

    t0 = MIN_HISTORY_SESSIONS
    if start:
        t0 = max(t0, int(np.searchsorted(pn.dates,
                                         np.datetime64(start[:10]))))
    t_end = pn.close.shape[0] - 1
    if end:
        t_end = min(t_end, int(np.searchsorted(pn.dates,
                                               np.datetime64(end[:10]))))

    current: dict[str, float] = {}
    col_of = {sym: i for i, sym in enumerate(pn.symbols)}
    cooldown_until: dict[str, int] = {}
    pending_cost = 0.0
    binding: dict[str, dict[str, int]] = {}
    zone_data = {z: {"daily": [], "turnovers": [], "hhis": [],
                     "decisions": 0, "traded": 0, "held_subhurdle": 0,
                     "mandatory_exit_only": 0}
                 for z in ("DEVELOPMENT", "VALIDATION")}

    t = t0
    while t <= t_end:
        date_iso = pn.iso(t)
        z = _zone(date_iso)
        d = zone_data[z]
        universe = eligible_and_scores(pn, t, pol["min_adv_dollar"])
        if universe:
            d["decisions"] += 1
            candidates = []
            ranked = sorted(universe.items(), key=lambda kv: kv[1]["rank"])
            cand_max = int(pol.get("candidate_rank_max") or 50)
            for tk, row in ranked:
                if row["rank"] <= max(cand_max, 3 * int(
                        pol["target_position_count"])) or tk in current:
                    candidates.append({"ticker": tk, "sector": "UNCLASSIFIED",
                                       "adv_dollar": row["adv_dollar"],
                                       "score": row["score"],
                                       "rank": row["rank"]})
            ideal = _ideal_target(current=current, universe=universe,
                                  policy=pol, cooldown_until=cooldown_until,
                                  t=t)
            # GENESIS: the first decision of an empty book is the zero-base
            # deployment ("if all capital were cash NOW ..."), exactly like
            # the production book's own seeding. The turnover budget is a
            # limit on CHANGING a book, not on having one, and the switching
            # hurdle compares two books, which requires two books to exist.
            genesis = not current
            solve_pol = (dict(pol, max_one_way_turnover=1.0)
                         if genesis else pol)
            solution = CR.solve_feasible_target(
                current_weight=current, ideal_weight=ideal,
                candidates=candidates, nav=nav_usd, policy=solve_pol)
            zb = binding.setdefault(z, {})
            for code in solution.get("constraints_that_reshaped") or []:
                zb[code] = zb.get(code, 0) + 1
            economics = CR.switching_economics(
                current_weight=current,
                target_weight=solution.get("best_feasible_target") or {},
                candidates=candidates, nav=nav_usd,
                mandatory_exits=solution.get("mandatory_exits") or [],
                policy=pol)
            outcome = CR.decide_outcome(solution=solution, economics=economics)
            target = solution.get("best_feasible_target") or {}
            mand = set(solution.get("mandatory_exits") or [])
            if genesis and target:
                one_way = CR.one_way_turnover(current, target)
                pending_cost += 2.0 * one_way * cost_rate
                current = dict(target)
                d["genesis_deployments"] = d.get("genesis_deployments", 0) + 1
            elif outcome["outcome"] == "PROPOSAL_READY":
                one_way = CR.one_way_turnover(current, target)
                pending_cost += 2.0 * one_way * cost_rate
                d["turnovers"].append(one_way)
                d["traded"] += 1
                for tk in set(current) - set(target):
                    cooldown_until[tk] = t + cooldown
                current = dict(target)
            elif mand and set(current) & mand:
                # Mandatory exits execute without the hurdle: the exited
                # weight goes to cash at the decision (a constraint, not an
                # economic choice - the kernel's own doctrine).
                exited = {tk: w for tk, w in current.items() if tk in mand}
                one_way = sum(exited.values())
                pending_cost += one_way * cost_rate
                d["turnovers"].append(one_way)
                d["mandatory_exit_only"] += 1
                current = {tk: w for tk, w in current.items()
                           if tk not in mand}
                for tk in exited:
                    cooldown_until[tk] = t + cooldown
            else:
                d["held_subhurdle"] += 1
            if current:
                d["hhis"].append(sum(w * w for w in current.values()))

        # Evolve daily to the next decision index. Weights are fractions of
        # NAV; cash earns zero; each day w_i <- w_i(1+r_i)/(1+r_p).
        t_next = min(t + DECISION_STEP_SESSIONS, t_end)
        for day in range(t, t_next):
            r_day = 0.0
            new_current = {}
            for tk, w in current.items():
                ci = col_of.get(tk)
                p0 = pn.close[day, ci] if ci is not None else np.nan
                p1 = pn.close[day + 1, ci] if ci is not None else np.nan
                r_i = (p1 / p0 - 1.0) if (np.isfinite(p0) and np.isfinite(p1)
                                          and p0 > 0) else 0.0
                r_day += w * r_i
                new_current[tk] = w * (1.0 + r_i)
            if pending_cost:
                r_day -= pending_cost
                pending_cost = 0.0
            gross = 1.0 + r_day
            if gross > 0 and new_current:
                new_current = {tk: v / gross for tk, v in new_current.items()}
            current = new_current
            zone_data[_zone(pn.iso(day + 1))]["daily"].append(
                (pn.iso(day + 1), r_day))
        if t_next == t_end:
            break
        t = t_next

    out = {"config_id": config["config_id"], "axis": config.get("axis"),
           "overrides": config.get("overrides", {}),
           "policy_hash": sha({k: v for k, v in pol.items()
                               if k != "constraint_policy_version"}),
           "zones": {}}
    for z, d in zone_data.items():
        out["zones"][z] = _metrics(d["daily"], d["turnovers"], d["decisions"],
                                   d["traded"], d["hhis"])
        out["zones"][z]["held_subhurdle"] = d["held_subhurdle"]
        out["zones"][z]["mandatory_exit_only_trades"] = d["mandatory_exit_only"]
        out["zones"][z]["constraints_that_reshaped"] = binding.get(z, {})
    return out


def run_walkforward(*, configs: Optional[list] = None,
                    start: Optional[str] = None, end: Optional[str] = None,
                    panel: Optional[_Panel] = None) -> dict:
    pn = panel or load_panel()
    cfgs = configs if configs is not None else (
        axis_sweep_configs() + shadow_policy_configs())
    results = [simulate_config(pn, c, start=start, end=end) for c in cfgs]
    body = artifact_body(
        "r53_risk_appetite_walkforward/1", CALCULATION_OWNER,
        release=RELEASE, campaign_id=CAMPAIGN_ID,
        panel_source=pn.source,
        panel_span=[pn.iso(0), pn.iso(len(pn.dates) - 1)],
        decision_step_sessions=DECISION_STEP_SESSIONS,
        validation_start=VALIDATION_START,
        signal_is_proxy=True,
        signal_proxy_note=SIGNAL_PROXY_NOTE,
        kernel_owner="engine.constrained_reallocation (canonical; policy "
                     "injected, never modified in place)",
        sector_cap_axis_not_swept=(
            "no point-in-time sector classification exists in the frozen "
            "panel; the sector cap is censused on the live cross-sections "
            "in R53_LIVE_BINDING_ANALYSIS instead"),
        gross_exposure_axis_not_swept=(
            "the canonical kernel is long-only with gross <= 100% by "
            "architecture; simulating gross > 1 would require a second "
            "allocator, which principle 1 forbids"),
        n_configs=len(results),
        results=results,
        no_champion_selected=True,
        **safety_block(),
    )
    write_json(research_dir() / ARTIFACT_WALKFORWARD, body)
    return body


def write_shadow_policy_artifact() -> dict:
    from paper_trader.engine import constrained_reallocation as CR
    prod = CR.default_policy()
    rows = []
    for name, d in SHADOW_POLICY_DEFINITIONS.items():
        eff = dict(prod)
        eff.update(d["overrides"])
        rows.append({"policy_id": name, "rationale": d["rationale"],
                     "overrides": d["overrides"],
                     "effective_policy_hash": sha(eff),
                     "is_production": not d["overrides"]})
    body = artifact_body(
        "r53_shadow_policies/1", CALCULATION_OWNER,
        release=RELEASE, campaign_id=CAMPAIGN_ID,
        declared_a_priori=True,
        production_policy_hash=sha(prod),
        production_policy_unchanged=True,
        policies=rows,
        prospective_comparison_path=(
            "compare the three policies on FUTURE governed cycles: each "
            "cycle's canonical solve can be repeated in shadow with each "
            "policy dict through the same kernel seam this study used"),
        **safety_block(),
    )
    write_json(research_dir() / ARTIFACT_SHADOW_POLICIES, body)
    return body
