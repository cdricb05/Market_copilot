"""alpha_agent.r32.campaign - orchestration and the terminal verdict.

The sequence is fixed and the ordering carries meaning:

    contract -> sources -> panels -> partition -> screening -> qualification
    -> lockbox -> multiple testing -> frontier -> verdict

Screening and qualification see DISCOVERY and VALIDATION only. The LOCKBOX
segment is the most recent stretch of history and is opened exactly once, by two
finalists per sleeve, after which no configuration may be tuned. That is the
whole value of a held-out sample: it is evidence precisely because it could not
influence what was tried.

Nothing here activates a sleeve, allocates capital, or writes an operational
store. The output is a comparison and a verdict, and a NULL verdict is a
legitimate result rather than a failure to try hard enough.
"""
from __future__ import annotations

import datetime as _dt
from typing import Optional

import numpy as np

from .. import r32
from ..r31 import multiple_testing as _mt
from . import contract as _contract
from . import funnel as _funnel
from . import judge as _judge
from . import panels as _panels
from . import sleeve as _sleeve
from . import sources as _sources
from .sleeves import (
    cross_asset_trend,
    equity_beta_timing,
    equity_selection,
    event_driven,
    sector_rotation,
    volatility_risk_regime,
)

CALCULATION_OWNER = "alpha_agent.r32.campaign"
VERDICT_SCHEMA = "r32_final_verdict/1"
VERDICT_ARTIFACT = "final_verdict.json"
SLEEVE_RESULTS_SCHEMA = "r32_sleeve_results/1"
SLEEVE_RESULTS_ARTIFACT = "sleeve_results.json"

SLEEVE_MODULES = {
    _contract.SLEEVE_EQUITY_BETA_TIMING: equity_beta_timing,
    _contract.SLEEVE_SECTOR_ROTATION: sector_rotation,
    _contract.SLEEVE_CROSS_ASSET_TREND: cross_asset_trend,
    _contract.SLEEVE_VOLATILITY_RISK_REGIME: volatility_risk_regime,
    _contract.SLEEVE_EVENT_DRIVEN: event_driven,
}

#: Evidence partition proportions. Declared before any result is seen.
DISCOVERY_FRACTION = 0.55
VALIDATION_FRACTION = 0.25
#: The remainder is the lockbox. One decision date is skipped either side so a
#: hold window cannot straddle a boundary.
EMBARGO_DECISIONS = 1


class Context:
    """Loaded panels and the derived per-sleeve evaluation inputs."""

    def __init__(self, *, campaign_id: str = _contract.CAMPAIGN_ID):
        self.campaign_id = campaign_id
        self.panels: dict = {}
        self.sleeve_inputs: dict = {}
        self.errors: dict = {}
        self.common_calendar: list = []

    # ------------------------------------------------------------------ load #
    def load(self, *, nd=None) -> "Context":
        nd = nd or _panels._norgate()
        for name in _panels.PANELS:
            try:
                self.panels[name] = _panels.build_panel(name, nd=nd)
            except Exception as exc:  # noqa: BLE001
                self.panels[name] = {"ok": False,
                                     "reason": f"{type(exc).__name__}"}
                self.errors[name] = str(exc)[:300]
        self.common_calendar = self._common_calendar()
        for sleeve, mod in SLEEVE_MODULES.items():
            self.sleeve_inputs[sleeve] = self._prepare(sleeve, mod)
        return self

    def _common_calendar(self) -> list:
        """The trading sessions EVERY panel covers.

        Each panel has its own session index, so per-panel decision dates land
        on different calendars and two sleeves can share no dates at all. That
        is not a cosmetic problem: without a shared calendar there is no
        cross-sleeve correlation, no latent-risk clustering, and no honest
        ranking - only five strategies measured in five different eras.
        """
        sets = [set(p["dates"]) for p in self.panels.values() if p.get("ok")]
        return sorted(set.intersection(*sets)) if sets else []

    def _prepare(self, sleeve: str, mod) -> dict:
        panel = self.panels.get(mod.PANEL)
        if not panel or not panel.get("ok"):
            return {"ok": False, "reason": _funnel.STATE_NO_PANEL}
        dates = panel["dates"]
        idx = _panels.decision_dates(dates)
        if not idx:
            return {"ok": False, "reason": "NO_DECISION_DATES"}
        cash_col = panel["columns"].get("CASH_YIELD")
        bench_col = panel["columns"].get("BENCHMARK")
        if bench_col is None:
            bench_col = panel["columns"].get("EQUITY_US")
        hold = _contract.HOLD_SESSIONS
        cash_by_date, bench_by_date = {}, {}
        usable = []
        for i in idx:
            c = _panels.hold_cash_return(cash_col, dates, i, hold)
            b = (_panels.hold_return(bench_col, i, hold)
                 if bench_col is not None else float("nan"))
            if not np.isfinite(c):
                continue
            cash_by_date[dates[i]] = float(c)
            bench_by_date[dates[i]] = float(b)
            usable.append(i)
        if not usable:
            return {"ok": False, "reason": "NO_CASH_OBSERVATIONS"}
        try:
            returns = mod.instrument_returns(panel, usable)
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "reason": f"RETURNS_FAILED_{type(exc).__name__}",
                    "error": str(exc)[:300]}
        part = partition(usable)
        return {"ok": True, "panel": panel, "idx": usable,
                "instrument_returns": returns, "cash": cash_by_date,
                "benchmark": bench_by_date, "partition": part,
                "first": dates[usable[0]], "last": dates[usable[-1]],
                "n_decisions": len(usable)}


def partition(idx: list) -> dict:
    """Split decision indices into DISCOVERY / VALIDATION / LOCKBOX."""
    n = len(idx)
    d_end = int(n * DISCOVERY_FRACTION)
    v_end = int(n * (DISCOVERY_FRACTION + VALIDATION_FRACTION))
    discovery = idx[:d_end]
    validation = idx[d_end + EMBARGO_DECISIONS:v_end]
    lockbox = idx[v_end + EMBARGO_DECISIONS:]
    return {"DISCOVERY": discovery, "VALIDATION": validation,
            "LOCKBOX": lockbox,
            "search": discovery + validation,
            "embargo_decisions": EMBARGO_DECISIONS,
            "counts": {"DISCOVERY": len(discovery),
                       "VALIDATION": len(validation),
                       "LOCKBOX": len(lockbox)}}


# --------------------------------------------------------------------------- #
# Execution
# --------------------------------------------------------------------------- #
def execute(spec, *, ctx: Context, layer: str) -> dict:
    """Generate one configuration's opportunities and score them."""
    inp = ctx.sleeve_inputs.get(spec.sleeve)
    if not inp or not inp.get("ok"):
        return {"scored": False, "reason": (inp or {}).get("reason", "NO_INPUT")}
    idx = inp["partition"][layer]
    if not idx:
        return {"scored": False, "reason": f"EMPTY_LAYER_{layer}"}
    opportunities = spec.generate(inp["panel"], idx, spec.params)
    if not opportunities:
        return {"scored": False, "reason": "NO_OPPORTUNITIES_GENERATED"}
    out = _judge.score(opportunities, sleeve=spec.sleeve,
                       instrument_returns_by_date=inp["instrument_returns"],
                       cash_return_by_date=inp["cash"],
                       benchmark_return_by_date=inp["benchmark"])
    out["layer"] = layer
    return out


def _rank_key(row: dict) -> float:
    """Rank by after-cost excess over the VOLATILITY-MATCHED control.

    Ranking by raw return prefers whichever sleeve held the riskiest thing.
    Ranking by excess over cash prefers whichever sleeve held the most equity -
    over a long window every equity strategy beats bills, so that statistic
    cannot distinguish skill from exposure. Ranking by excess over the benchmark
    punishes a defensive sleeve for doing its job.

    The volatility-matched control carries the sleeve's own risk in a static mix
    of benchmark and cash. Beating it means beating what de-risking alone would
    have produced.
    """
    t = (row.get("vs_volatility_matched_control") or {}).get("t_stat")
    return float(t) if t is not None and np.isfinite(t) else float("-inf")


def run_sleeve(sleeve: str, *, ctx: Context, fun: _funnel.Funnel) -> dict:
    """Screen, qualify and (if it survives) lockbox one sleeve."""
    mod = SLEEVE_MODULES[sleeve]
    inp = ctx.sleeve_inputs.get(sleeve) or {}
    if not inp.get("ok"):
        return {"sleeve": sleeve, "state": _sleeve.STATE_DATA_BLOCKED,
                "reason": inp.get("reason", "NO_INPUT"), "screened": 0,
                "qualified": 0, "finalists": []}

    screened = []
    for spec in mod.screening_specs():
        row = fun.run(spec, lambda s: execute(s, ctx=ctx, layer="DISCOVERY"))
        if row:
            screened.append(row)

    if not screened:
        return {"sleeve": sleeve, "state": _sleeve.STATE_REJECTED,
                "reason": "NO_SCREENING_CANDIDATE_PRODUCED_ENOUGH_DECISIONS",
                "screened": fun.count(stage=_contract.STAGE_SCREENING,
                                      sleeve=sleeve),
                "qualified": 0, "finalists": [],
                "partition": inp["partition"]["counts"],
                "window": {"first": inp["first"], "last": inp["last"]}}

    # The families that survive screening are the ones whose BEST configuration
    # beat cash after cost. Screening never selects a configuration - only a
    # family - so a lucky parameter cannot smuggle itself into qualification.
    best_by_family: dict = {}
    for r in screened:
        k = r["family"]
        if k not in best_by_family or _rank_key(r) > _rank_key(best_by_family[k]):
            best_by_family[k] = r
    families = [f for f, r in
                sorted(best_by_family.items(), key=lambda kv: -_rank_key(kv[1]))
                if _rank_key(r) > 0.0]
    families = families[:_contract.QUALIFICATION_MAX_FAMILIES_PER_SLEEVE]

    qualified = []
    for spec in mod.qualification_specs(families):
        row = fun.run(spec, lambda s: execute(s, ctx=ctx, layer="VALIDATION"))
        if row:
            qualified.append(row)

    # Controls are executed, counted in the denominator, and reported - but a
    # control may never become a finalist. Otherwise "hold the index every
    # session" reaches the lockbox and a sleeve qualifies for rediscovering
    # buy-and-hold.
    pool = [r for r in (qualified or screened) if not r.get("is_control")]
    ranked = sorted(pool, key=_rank_key, reverse=True)
    finalists = ranked[:_contract.LOCKBOX_MAX_FINALISTS_PER_SLEEVE]

    return {"sleeve": sleeve,
            "state": _sleeve.STATE_RESEARCH_ONLY,
            "screened": fun.count(stage=_contract.STAGE_SCREENING, sleeve=sleeve),
            "qualified": fun.count(stage=_contract.STAGE_QUALIFICATION,
                                   sleeve=sleeve),
            "surviving_families": families,
            "finalists": finalists,
            "partition": inp["partition"]["counts"],
            "window": {"first": inp["first"], "last": inp["last"]},
            "screening_rows": screened,
            "qualification_rows": qualified}


def run_lockbox(sleeve_results: dict, *, ctx: Context,
                fun: _funnel.Funnel) -> dict:
    """Open the lockbox once per finalist. No tuning is possible afterwards."""
    out = {}
    for sleeve, res in sleeve_results.items():
        rows = []
        for f in res.get("finalists", []):
            mod = SLEEVE_MODULES[sleeve]
            spec = _sleeve.SleeveSpec(
                sleeve=sleeve, family=f["family"], params=dict(f["params"]),
                generate=mod.FAMILIES[f["family"]],
                stage=_contract.STAGE_LOCKBOX)
            fun.authorise_lockbox(spec.spec_hash(fun.judge_behaviour_hash))
            row = fun.run(spec, lambda s: execute(s, ctx=ctx, layer="LOCKBOX"))
            if row:
                rows.append(row)
            else:
                rows.append({"sleeve": sleeve, "family": f["family"],
                             "params": f["params"], "state":
                                 "LOCKBOX_PRODUCED_NO_SCORABLE_RESULT"})
        out[sleeve] = rows
    return out


# --------------------------------------------------------------------------- #
# Common overlap
# --------------------------------------------------------------------------- #
def common_overlap_paths(lockbox: dict, *, ctx: Context) -> dict:
    """Re-score each sleeve's best finalist on the SHARED decision calendar.

    This is REPORTING ONLY and it is important that it stays that way. It runs
    no new specification - the same finalist, on a stated sub-window - so it is
    not a new hypothesis and does not enter the denominator. It may not qualify
    anything and does not touch the funnel: qualification was settled on each
    sleeve's own lockbox, and letting a second window change that would be
    choosing the window that gives the nicer answer.

    What it produces is the only thing that makes the sleeves comparable at all:
    net return paths on identical dates.
    """
    # The decision dates must be SAMPLED FROM the shared calendar. Sampling each
    # panel independently and then filtering to shared sessions keeps
    # panel-specific dates that merely happen to be shared sessions - which is
    # why the first attempt still produced zero overlapping decisions.
    shared_decisions = ctx.common_calendar[::_contract.STEP_SESSIONS]
    out, windows = {}, {}
    for sleeve, rows in lockbox.items():
        inp = ctx.sleeve_inputs.get(sleeve) or {}
        if not inp.get("ok") or not rows:
            continue
        best = max(rows, key=_rank_key, default=None)
        if best is None or not best.get("_dates"):
            continue
        dates = inp["panel"]["dates"]
        pos = {d: i for i, d in enumerate(dates)}
        lock_idx = inp["partition"]["LOCKBOX"]
        if not lock_idx:
            windows[sleeve] = {"n": 0, "reason": "EMPTY_LOCKBOX_LAYER"}
            continue
        lo, hi = dates[lock_idx[0]], dates[lock_idx[-1]]
        idx = [pos[d] for d in shared_decisions
               if d in pos and lo <= d <= hi]
        if len(idx) < 8:
            windows[sleeve] = {"n": len(idx), "reason": "TOO_FEW_SHARED_DATES"}
            continue
        mod = SLEEVE_MODULES[sleeve]
        panel = inp["panel"]
        opportunities = mod.FAMILIES[best["family"]](
            panel, idx, dict(best["params"]))
        # The precomputed returns cover the sleeve's OWN decision dates, which
        # these are not. They are recomputed here with the same functions, on
        # the same panel - a different sampling of the same measurements, not a
        # different measurement.
        returns = mod.instrument_returns(panel, idx)
        cash_col = panel["columns"].get("CASH_YIELD")
        # ``a or b`` on numpy arrays raises; the truth value is ambiguous.
        bench_col = panel["columns"].get("BENCHMARK")
        if bench_col is None:
            bench_col = panel["columns"].get("EQUITY_US")
        hold = _contract.HOLD_SESSIONS
        cash = {dates[i]: _panels.hold_cash_return(cash_col, dates, i, hold)
                for i in idx}
        bench = {dates[i]: _panels.hold_return(bench_col, i, hold) for i in idx}
        scored = _judge.score(
            opportunities, sleeve=sleeve,
            instrument_returns_by_date=returns,
            cash_return_by_date=cash, benchmark_return_by_date=bench)
        if not scored.get("scored"):
            windows[sleeve] = {"n": 0, "reason": scored.get("reason")}
            continue
        d, p = scored["_dates"], list(scored["_net_path"])
        out[sleeve] = {dd: float(x) for dd, x in zip(d, p)}
        windows[sleeve] = {"n": len(d), "first": d[0], "last": d[-1],
                           "configuration": best.get("label"),
                           "net_annual_return":
                               (scored.get("net") or {}).get("annual_return"),
                           "net_sharpe": (scored.get("net") or {}).get("sharpe"),
                           "t_vs_volatility_matched_control":
                               (scored.get("vs_volatility_matched_control")
                                or {}).get("t_stat")}
    return {"paths": out, "windows": windows,
            "reporting_only": True,
            "may_qualify_a_sleeve": False,
            "enters_denominator": False}


def common_overlap(sleeve_results: dict, lockbox: dict) -> dict:
    """The decision calendar every scored sleeve actually shares.

    A cross-sleeve ranking that does not name this window is comparing eras. A
    sleeve whose observable did not exist before 2002 cannot be credited with
    the 1990s, and a sleeve that only looks good in one decade should be visible
    as such rather than averaged into a single number.
    """
    per_sleeve = {}
    for sleeve, rows in lockbox.items():
        dates = set()
        for r in rows:
            for d in (r.get("_dates") or []):
                dates.add(d)
        if not dates:
            win = (sleeve_results.get(sleeve) or {}).get("window") or {}
            per_sleeve[sleeve] = {"first": win.get("first"),
                                  "last": win.get("last"), "n": 0}
        else:
            per_sleeve[sleeve] = {"first": min(dates), "last": max(dates),
                                  "n": len(dates)}
    scored = [v for v in per_sleeve.values() if v["n"] > 0]
    if not scored:
        return {"ok": False, "reason": "NO_SLEEVE_PRODUCED_LOCKBOX_DECISIONS",
                "per_sleeve": per_sleeve}
    return {"ok": True,
            "common_first": max(v["first"] for v in scored),
            "common_last": min(v["last"] for v in scored),
            "per_sleeve": per_sleeve,
            "binding_sleeve": max(
                (v for v in per_sleeve.items() if v[1]["n"] > 0),
                key=lambda kv: str(kv[1]["first"]))[0]}


# --------------------------------------------------------------------------- #
# Multiple testing
# --------------------------------------------------------------------------- #
def run_multiple_testing(fun: _funnel.Funnel, lockbox: dict) -> dict:
    """BH/FDR over every lockbox result, against the FULL denominator."""
    entries = []
    for sleeve, rows in lockbox.items():
        for r in rows:
            if r.get("is_control"):
                continue
            t = (r.get("vs_volatility_matched_control") or {}).get("t_stat")
            if t is None or not np.isfinite(t):
                continue
            entries.append({"sleeve": sleeve, "label": r.get("label"),
                            "spec_hash": r.get("spec_hash"),
                            "statistic": "EXCESS_OVER_VOLATILITY_MATCHED_CONTROL",
                            "t_stat": float(t),
                            "p_value": _mt.two_sided_p(float(t))})
    denominator = fun.denominator
    if not entries:
        return {"denominator": denominator, "tested": 0, "survivors": [],
                "n_survivors": 0,
                "note": "no lockbox result produced a finite t-statistic"}
    # The BH family size must be the FULL denominator, not the handful of
    # lockbox results being reported. Screening and qualification searched the
    # same data; pretending only the finalists were tested would understate the
    # correction by an order of magnitude. Padding with p = 1.0 sets m to the
    # denominator without inventing a rejectable hypothesis.
    p_values = [e["p_value"] for e in entries]
    padding = max(0, denominator - len(p_values))
    bh = _mt.benjamini_hochberg(p_values + [1.0] * padding, q=_contract.FDR_Q)
    rejected = set(bh.get("rejected") or [])
    for i, e in enumerate(entries):
        e["survives_fdr"] = i in rejected
    return {"denominator": denominator,
            "denominator_counts_all_executed": True,
            "bh_family_size": bh.get("m"),
            "padded_with_non_reportable": padding,
            "tested": len(entries),
            "fdr_q": _contract.FDR_Q,
            "bh": bh,
            "entries": entries,
            "survivors": [e for e in entries if e["survives_fdr"]],
            "n_survivors": sum(1 for e in entries if e["survives_fdr"]),
            "note": ("the denominator is every executed hypothesis, not the "
                     "number tested here; screening and qualification searched "
                     "the same data and their cost is real")}


# --------------------------------------------------------------------------- #
# Verdict
# --------------------------------------------------------------------------- #
def qualify(row: dict, mt_entry: Optional[dict]) -> dict:
    """Does one lockbox result qualify its sleeve? All five gates must pass.

    The load-bearing gate is ``beats_volatility_matched_control``. Without it, a
    sleeve qualifies by holding equities and calling the result alpha.
    """
    vs_cash = row.get("vs_cash") or {}
    vs_bench = row.get("vs_benchmark") or {}
    vs_matched = row.get("vs_volatility_matched_control") or {}
    mpv = row.get("marginal_portfolio_value") or {}
    gates = {
        "is_not_a_control": not bool(row.get("is_control")),
        "scored_enough_decisions":
            int(row.get("n") or 0) >= _contract.MIN_SCORED_DECISIONS,
        "beats_cash_after_cost": bool((vs_cash.get("mean_excess") or 0.0) > 0.0),
        "beats_volatility_matched_control":
            bool((vs_matched.get("mean_excess") or 0.0) > 0.0),
        "survives_multiple_testing":
            bool(mt_entry and mt_entry.get("survives_fdr")),
        "beats_benchmark_or_adds_marginal_value":
            bool((vs_bench.get("mean_excess") or 0.0) > 0.0
                 or mpv.get("improves")),
    }
    return {"gates": gates, "qualifies": all(gates.values())}


def build_verdict(*, campaign_id: str, sleeve_results: dict, lockbox: dict,
                  mt: dict, overlap: dict, inherited: dict,
                  fun: _funnel.Funnel, source_registry: dict,
                  panel_manifest: dict) -> dict:
    by_hash = {e.get("spec_hash"): e for e in (mt.get("entries") or [])}
    sleeves = {}
    qualified_sleeves = []
    for sleeve in _contract.NEW_SLEEVES:
        res = sleeve_results.get(sleeve) or {}
        rows = lockbox.get(sleeve) or []
        judged = []
        for r in rows:
            q = qualify(r, by_hash.get(r.get("spec_hash")))
            judged.append({"label": r.get("label"), "family": r.get("family"),
                           "params": r.get("params"),
                           "spec_hash": r.get("spec_hash"),
                           "n": r.get("n"),
                           "net_annual_return": (r.get("net") or {}).get("annual_return"),
                           "net_sharpe": (r.get("net") or {}).get("sharpe"),
                           "max_drawdown": (r.get("net") or {}).get("max_drawdown"),
                           "mean_cash_weight": r.get("mean_cash_weight"),
                           "annual_cost_drag": r.get("annual_cost_drag"),
                           "is_control": bool(r.get("is_control")),
                           "vs_cash": r.get("vs_cash"),
                           "vs_benchmark": r.get("vs_benchmark"),
                           "vs_volatility_matched_control":
                               r.get("vs_volatility_matched_control"),
                           "volatility_matched_control":
                               r.get("volatility_matched_control"),
                           "marginal_portfolio_value":
                               r.get("marginal_portfolio_value"),
                           **q})
        any_q = any(j["qualifies"] for j in judged)
        if any_q:
            qualified_sleeves.append(sleeve)
        state = (_sleeve.STATE_QUALIFIED if any_q
                 else res.get("state", _sleeve.STATE_REJECTED))
        if state == _sleeve.STATE_RESEARCH_ONLY and not any_q:
            state = _sleeve.STATE_REJECTED
        sleeves[sleeve] = {
            "state": state,
            "owns_capital": False,
            "activated": False,
            "screened": res.get("screened", 0),
            "qualified_configs": res.get("qualified", 0),
            "surviving_families": res.get("surviving_families", []),
            "window": res.get("window"),
            "partition": res.get("partition"),
            "lockbox_results": judged,
            "rejection_reason": (None if any_q
                                 else _rejection_reason(res, judged)),
        }
    sleeves[_contract.CONTROL_SLEEVE] = {
        "state": _sleeve.STATE_REJECTED,
        "owns_capital": False, "activated": False,
        "is_control": True, "rerun_in_r32": False,
        "inherited": inherited,
        "rejection_reason": _contract.R31_VERDICT,
    }

    n_q = len(qualified_sleeves)
    if n_q >= 2:
        primary = _contract.VERDICT_MULTIPLE
    elif n_q == 1:
        primary = _contract.VERDICT_SINGLE
    else:
        primary = _contract.VERDICT_EXHAUSTED
    secondary = (_contract.SECONDARY_READY if n_q >= 1
                 else _contract.SECONDARY_SAMPLE)

    payload = {
        "calculation_owner": CALCULATION_OWNER,
        "campaign_id": campaign_id,
        "created_at": _dt.datetime.now().isoformat(timespec="seconds"),
        "question": _contract.QUESTION,
        "primary_verdict": primary,
        "secondary_verdict": secondary,
        "qualified_sleeves": qualified_sleeves,
        "n_qualified": n_q,
        "sleeves": sleeves,
        "common_overlap": overlap,
        "multiple_testing": {k: v for k, v in mt.items() if k != "entries"},
        "multiple_testing_entries": mt.get("entries", []),
        "funnel": fun.summary(),
        "source_registry_hash": source_registry.get("registry_hash"),
        "panel_manifest_hash": panel_manifest.get("manifest_hash"),
        "judge_behaviour_hash": _judge.behaviour_hash(),
        "sleeve_contract_hash":
            _sleeve.build_contract(campaign_id=campaign_id)["sleeve_contract_hash"],
        "cash_is_a_real_asset_choice": True,
        "null_result_is_valid": True,
        "nothing_activated": True,
    }
    body = r32.artifact_body(VERDICT_SCHEMA, payload)
    body["verdict_hash"] = r32.sha(payload)
    return body


def _rejection_reason(res: dict, judged: list) -> str:
    if res.get("state") == _sleeve.STATE_DATA_BLOCKED:
        return f"DATA_COVERAGE_INSUFFICIENT: {res.get('reason')}"
    if not judged:
        return "NO_LOCKBOX_RESULT"
    fails = {}
    for j in judged:
        for gate, ok in (j.get("gates") or {}).items():
            if not ok:
                fails[gate] = fails.get(gate, 0) + 1
    if not fails:
        return "UNKNOWN"
    worst = sorted(fails.items(), key=lambda kv: -kv[1])[0][0]
    return f"FAILED_GATE:{worst}"


def path_for(campaign_id: str = _contract.CAMPAIGN_ID):
    return r32.campaign_dir(campaign_id) / VERDICT_ARTIFACT


def freeze(body: dict):
    return r32.write_json(path_for(body["campaign_id"]), body)
