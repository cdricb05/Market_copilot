"""alpha_agent.r31.judge - the ONE Release 31 research judge (Campaign v3).

Built and frozen BEFORE the candidate search begins. Every candidate - the
incumbent, a reproduced published method, a novel discovery - is scored by this
module and by nothing else, on the same dates, over the same investment universe,
with the same cost owner, the same risk prices, the same cash policy and the same
constraints. Two judges drift, and the more generous one wins wherever it happens
to be called.

What changed from Campaign v2, and why
--------------------------------------
v2 turned predictions into a portfolio by holding the top N names at roughly
equal weight with cash pinned to zero, over whatever cross-section the Russell
1000 panel happened to contain. Three consequences, all fatal to the business
question:

* a model that found nothing worth owning was still made to own 25 names, so the
  judge could not tell "no opportunity" from "twenty-five good names";
* the book was evaluated on a universe we do not manage;
* concentration (N) stood in for risk appetite, which it is not.

The v3 judge routes BOTH architectures through real capital allocation:

    TRACK A   score -> economic return units -> engine.zero_base_allocator
    TRACK B   proposed weights -> the SAME canonical feasibility seam

and cash is whatever the allocation does not invest - free to be 100%.

What the judge does NOT own
---------------------------
No cost model, no risk aversion, no name cap, no liquidity floor, no covariance
and no optimiser. It READS them from the canonical owners through
:mod:`alpha_agent.r31.allocation`. Release 31 compares candidates on the
economics the operator would actually face, so a research-only cost assumption
would make every number here unusable downstream.

Two benchmarks, never one
-------------------------
Every result reports the point-in-time S&P 500 equal-weight return (universe
neutral: it isolates SELECTION skill) AND the S&P 500 total-return series (the
investable alternative: it isolates the decision to run the strategy at all).
Neither may stand in for the other.

A declared limitation
---------------------
Historical SECTOR exposure is NOT measured. The canonical point-in-time sector
owner classifies the owned entity-level SIC snapshot as inadmissible for
historical signal construction, and using it to compute a historical sector cap
would be the same violation wearing a different hat. The judge reports sector as
UNMEASURABLE_PIT rather than reporting a number it cannot defend.
"""
from __future__ import annotations

import math
from typing import Optional

import numpy as np

from .. import r31
from . import allocation as _alloc
from . import benchmarks as _bench
from . import calibration as _calib
from . import contract as _contract
from . import universe as _universe

CALCULATION_OWNER = "alpha_agent.r31.judge"
JUDGE_SCHEMA = "r31_research_judge_contract/2"
ARTIFACT_NAME = "research_judge_contract.json"

#: Rebalance cadence, in sessions. The book turns over at the decision cadence
#: whatever horizon the model forecasts, and it equals STEP_SESSIONS so decision
#: date ``k+1`` IS the end of the book struck at ``k``.
HOLD_SESSIONS = _contract.STEP_SESSIONS

#: Periods per year at that cadence.
PERIODS_PER_YEAR = 252.0 / float(HOLD_SESSIONS)

SECTOR_STATE = _contract.HISTORICAL_SECTOR_CONSTRAINT
EVALUATION_UNIVERSE = _universe.EVALUATION_UNIVERSE

#: A cross-section with fewer eligible index members than this is not scored.
MIN_ELIGIBLE = 20

#: Candidate rejection states the judge reports rather than scoring around.
REJECT_NO_CALIBRATION = "FORECAST_NOT_ECONOMICALLY_CALIBRATABLE"
REJECT_RANK_IDENTITY = "FORECAST_RANK_IDENTITY_VIOLATION"


def policy() -> dict:
    return _alloc.policy()


def gamma_policy(multiplier: float) -> dict:
    """The canonical policy with ONE pre-registered risk-appetite multiplier.

    Only ``risk_aversion_gamma`` moves. Every other term - cost, caps, liquidity,
    lookback, minimum position - is the canonical value, so a frontier point is a
    different risk appetite rather than a different set of rules.
    """
    p = dict(policy())
    p["risk_aversion_gamma"] = float(policy()["risk_aversion_gamma"]) * float(multiplier)
    p["risk_frontier_gamma_multiplier"] = float(multiplier)
    return p


def economics_declaration() -> dict:
    p = policy()
    return {
        "policy_owner": _contract.CANONICAL_POLICY_OWNER,
        "allocator_owner": _contract.CANONICAL_ALLOCATOR_OWNER,
        "covariance_owner": _contract.CANONICAL_COVARIANCE_OWNER,
        "cost_bps_per_side": p["cost_bps_per_side"],
        "cost_rate_per_side": p["cost_rate_per_side"],
        "cost_base": _contract.COST_BASE,
        "turnover_alignment": _contract.TURNOVER_ALIGNMENT,
        "max_name_weight": p["max_name_weight"],
        "min_adv_dollar": p["min_adv_dollar"],
        "min_position_weight": p["min_position_weight"],
        "risk_aversion_gamma": p["risk_aversion_gamma"],
        "uncertainty_aversion_phi": p["uncertainty_aversion_phi"],
        "covariance_lookback": p["covariance_lookback"],
        "cash_return_policy": _contract.CASH_RETURN_POLICY,
        "cash_is_a_real_allocation_choice": True,
        "sector_cap_state": SECTOR_STATE,
        "sector_cap_reason": ("the canonical PIT sector owner declares the owned "
                             "entity SIC snapshot inadmissible for historical "
                             "construction; a historical sector cap would violate "
                             "the same rule it claims to enforce"),
        "evaluation_universe": EVALUATION_UNIVERSE,
        "benchmarks": list(_contract.BENCHMARKS_REPORTED),
        "benchmark_substitution_permitted": False,
        "judge_owns_no_cost_or_risk_calculation": True,
        "judge_owns_no_portfolio_optimiser": True,
    }


# --------------------------------------------------------------------------- #
# Statistics
# --------------------------------------------------------------------------- #
def _rank(v: np.ndarray) -> np.ndarray:
    order = np.argsort(np.argsort(v, kind="stable"), kind="stable")
    return order.astype(np.float64)


def rank_ic(pred: np.ndarray, y: np.ndarray) -> float:
    m = np.isfinite(pred) & np.isfinite(y)
    if int(m.sum()) < 10:
        return float("nan")
    a, b = _rank(pred[m]), _rank(y[m])
    a = a - a.mean()
    b = b - b.mean()
    den = math.sqrt(float((a * a).sum()) * float((b * b).sum()))
    return float((a * b).sum() / den) if den > 0 else float("nan")


def newey_west_t(x, lags: int) -> float:
    a = np.asarray(list(x), dtype=np.float64)
    a = a[np.isfinite(a)]
    n = a.size
    if n < 3:
        return float("nan")
    d = a - a.mean()
    s = float((d * d).sum() / n)
    for lag in range(1, min(int(lags), n - 1) + 1):
        s += 2.0 * (1.0 - lag / (lags + 1.0)) * float((d[lag:] * d[:-lag]).sum() / n)
    return float(a.mean() / math.sqrt(s / n)) if s > 0 else float("nan")


def max_drawdown(period_returns: np.ndarray) -> float:
    r = np.asarray(period_returns, dtype=np.float64)
    r = r[np.isfinite(r)]
    if r.size == 0:
        return float("nan")
    equity = np.cumprod(1.0 + r)
    peak = np.maximum.accumulate(equity)
    return float(np.min(equity / peak - 1.0))


def cvar(period_returns: np.ndarray, q: float = 0.05) -> float:
    r = np.asarray(period_returns, dtype=np.float64)
    r = r[np.isfinite(r)]
    if r.size < 5:
        return float("nan")
    cut = float(np.quantile(r, q))
    tail = r[r <= cut]
    return float(tail.mean()) if tail.size else float("nan")


def annualise(period_returns: np.ndarray) -> float:
    r = np.asarray(period_returns, dtype=np.float64)
    r = r[np.isfinite(r)]
    if r.size == 0:
        return float("nan")
    growth = float(np.prod(1.0 + r))
    if growth <= 0:
        return float("nan")
    return float(growth ** (PERIODS_PER_YEAR / r.size) - 1.0)


def volatility(period_returns: np.ndarray) -> float:
    r = np.asarray(period_returns, dtype=np.float64)
    r = r[np.isfinite(r)]
    if r.size < 3:
        return float("nan")
    return float(r.std(ddof=1) * math.sqrt(PERIODS_PER_YEAR))


def sharpe(period_returns: np.ndarray) -> float:
    r = np.asarray(period_returns, dtype=np.float64)
    r = r[np.isfinite(r)]
    if r.size < 3:
        return float("nan")
    sd = float(r.std(ddof=1))
    return float(r.mean() / sd * math.sqrt(PERIODS_PER_YEAR)) if sd > 0 else float("nan")


def sortino(period_returns: np.ndarray) -> float:
    r = np.asarray(period_returns, dtype=np.float64)
    r = r[np.isfinite(r)]
    if r.size < 3:
        return float("nan")
    down = r[r < 0.0]
    if down.size < 2:
        return float("nan")
    sd = float(down.std(ddof=1))
    return float(r.mean() / sd * math.sqrt(PERIODS_PER_YEAR)) if sd > 0 else float("nan")


def _hhi(w) -> float:
    v = np.asarray(list(w), dtype=np.float64)
    s = float(v.sum())
    if s <= 0:
        return float("nan")
    p = v / s
    return float((p * p).sum())


def _none_if_nan(x):
    try:
        f = float(x)
    except (TypeError, ValueError):
        return None
    return None if not math.isfinite(f) else f


# --------------------------------------------------------------------------- #
# Secondary diagnostic - top-N equal weight. NEVER the primary verdict.
# --------------------------------------------------------------------------- #
def top_n_book(pred: np.ndarray, adv: np.ndarray, *, book_n: int,
               pol: dict) -> np.ndarray:
    """Long-only capped top-N book. Retained from v2 and DEMOTED to a diagnostic.

    It answers "which names does this model like?", which is worth reporting and
    is not the business question. ``contract.TOP_N_MAY_CARRY_PRIMARY_VERDICT`` is
    False and the campaign asserts it.
    """
    n = pred.shape[0]
    w = np.zeros(n, dtype=np.float64)
    ok = np.isfinite(pred) & (adv >= float(pol["min_adv_dollar"]))
    idx = np.nonzero(ok)[0]
    if idx.size == 0:
        return w
    k = int(min(book_n, idx.size))
    order = idx[np.argsort(-pred[idx], kind="stable")][:k]
    cap = float(pol["max_name_weight"])
    w[order] = 1.0 / k
    for _ in range(8):
        over = w > cap
        if not over.any():
            break
        excess = float((w[over] - cap).sum())
        w[over] = cap
        room = np.zeros(n, dtype=bool)
        room[order] = True
        room &= w < cap
        if not room.any():
            break
        w[room] += excess / int(room.sum())
    s = float(w.sum())
    return w / s if s > 0 else w


# --------------------------------------------------------------------------- #
# One decision date
# --------------------------------------------------------------------------- #
def _allocate_one_date(*, snap, cov, membership, k, X, adv, syms, raw_scores,
                       track, calib, sigma_scalar, pol, prev_weights):
    """Turn ONE cross-section into ONE capital allocation over index members.

    Returns ``None`` when the date cannot be judged - too few eligible members, or
    no cached covariance. A date that cannot be judged is SKIPPED and counted,
    never silently scored as zero.
    """
    date = snap.dates[k]
    elig = membership.eligible_columns(date, syms)
    if int(elig.sum()) < MIN_ELIGIBLE or not cov.has(k):
        return None

    cov_h, cov_names = cov.horizon_scaled(k, sessions=HOLD_SESSIONS,
                                          symbols=snap.symbols)
    cov_set = set(cov_names)

    rows = np.nonzero(elig)[0]
    tickers, keep = [], []
    for i in rows:
        tk = str(snap.symbols[int(syms[i])])
        if tk in cov_set and tk not in tickers:
            tickers.append(tk)
            keep.append(int(i))
    if len(tickers) < MIN_ELIGIBLE:
        return None
    keep_arr = np.asarray(keep, dtype=np.int64)

    adv_map = {tk: float(adv[i]) for tk, i in zip(tickers, keep)}
    score = np.asarray(raw_scores, dtype=np.float64)[keep_arr]
    # A predictor returns NaN when no legitimate training window precedes this
    # date. That is a real state, not a zero: allocating on a fabricated flat
    # score would report a portfolio the model never proposed.
    if int(np.isfinite(score).sum()) < MIN_ELIGIBLE:
        return None

    if track == _contract.TRACK_B:
        proposed = {tk: float(v) for tk, v in zip(tickers, score)}
        target = _alloc.feasible_portfolio(tickers=tickers, proposed=proposed,
                                           adv=adv_map, pol=pol)
        mu_used = None
    else:
        mu_vec = calib.apply(score)
        if not _calib.verify_rank_identity(score, mu_vec):
            raise _calib.CalibrationRefused(
                _calib.RANK_IDENTITY_VIOLATION,
                "the accepted calibration reordered a live cross-section on %s"
                % (date,))
        mu = {tk: float(v) for tk, v in zip(tickers, mu_vec)}
        sigma = {tk: float(sigma_scalar) for tk in tickers}
        target = _alloc.zero_base_target(
            tickers=tickers, mu=mu, sigma=sigma, cov_h=cov_h,
            cov_included=[t for t in cov_names if t in set(tickers)],
            adv=adv_map, pol=pol, current_weight=None)
        mu_used = mu

    w = target["weights"]
    traded = _alloc.traded_notional(prev_weights, w)
    cost = traded * float(pol["cost_rate_per_side"])
    return {"tickers": tickers, "keep": keep_arr, "elig": elig, "target": target,
            "traded": traded, "cost": cost, "mu": mu_used}


# --------------------------------------------------------------------------- #
# The one scoring entry point
# --------------------------------------------------------------------------- #
def score_candidate(*, snap, cov, membership, benchmarks, sample: str,
                    section_indices: list, layer_indices: list, predict,
                    horizon: int, track: str = _contract.TRACK_A,
                    calib=None, sigma_scalar: float = 0.08,
                    gamma_multipliers=(_contract.PRIMARY_GAMMA_MULTIPLIER,),
                    pol: Optional[dict] = None) -> dict:
    """Score ONE candidate on ONE evidence layer through real capital allocation.

    ``predict(k, X, adv, sym_idx) -> np.ndarray`` is the candidate's already
    fitted decision function. The judge never fits anything; a candidate arrives
    fitted on layers it was entitled to read, and the judge only measures.

    For TRACK A the returned array is a SCORE, mapped into economic return units
    by ``calib`` and handed to the canonical zero-base allocator. For TRACK B it
    is a proposed WEIGHT per row, made feasible against the same canonical
    constraints. Both land in the same economics.
    """
    base = pol or policy()
    if track == _contract.TRACK_A and calib is None:
        raise ValueError("a TRACK_A candidate needs an accepted calibration")

    per_gamma: dict = {}
    ics: list = []
    diag_ret: list = []
    skipped = 0

    for gm in gamma_multipliers:
        gpol = gamma_policy(gm) if gm != 1.0 else dict(base)
        prev_w: Optional[dict] = None
        gross, net, ew_bench, spy_bench = [], [], [], []
        turn, cash, held_n, hhi, unreal, conv, dust = [], [], [], [], [], [], []
        contrib_by_sym: dict = {}
        spy_missing = 0
        first_gamma = (gm == gamma_multipliers[0])

        for li in layer_indices:
            k = section_indices[li]
            X, y, adv, syms = snap.block(sample, k, _sample_features(sample), horizon)
            raw_scores = np.asarray(predict(k, X, adv, syms), dtype=np.float64)
            if raw_scores.shape[0] != X.shape[0]:
                raise ValueError("candidate returned %d outputs for %d rows"
                                 % (raw_scores.shape[0], X.shape[0]))

            one = _allocate_one_date(
                snap=snap, cov=cov, membership=membership, k=k, X=X, adv=adv,
                syms=syms, raw_scores=raw_scores, track=track, calib=calib,
                sigma_scalar=sigma_scalar, pol=gpol, prev_weights=prev_w)
            if one is None:
                if first_gamma:
                    skipped += 1
                continue

            hold_raw, _ = snap.holding_returns(sample, k, HOLD_SESSIONS)
            ret_by_sym = {}
            for tk, i in zip(one["tickers"], one["keep"]):
                v = hold_raw[int(i)]
                ret_by_sym[tk] = float(v) if np.isfinite(v) else None

            w = one["target"]["weights"]
            g = _alloc.realised_return(w, ret_by_sym) or 0.0
            c = one["cost"]
            gross.append(g)
            net.append(g - c)
            turn.append(one["traded"])
            cash.append(float(one["target"]["cash_weight"]))
            held_n.append(int(one["target"]["names_held"]))
            hhi.append(_hhi(w.values()) if w else float("nan"))
            unreal.append(_alloc.unrealised_weight(w, ret_by_sym))
            conv.append(bool(one["target"].get("converged", True)))
            dust.append(float(one["target"].get("dust_weight_dropped_to_cash") or 0.0))

            ew = _bench.equal_weight_return(hold_raw, one["elig"])
            ew_bench.append(float(ew) if ew is not None else 0.0)
            spy = None
            if k + 1 < snap.n_sections:
                spy = benchmarks.hold_return(snap.dates[k], snap.dates[k + 1])
            if spy is None:
                spy_missing += 1
                spy_bench.append(float("nan"))
            else:
                spy_bench.append(float(spy))

            for tk, wv in w.items():
                contrib_by_sym[tk] = (contrib_by_sym.get(tk, 0.0)
                                      + wv * (ret_by_sym.get(tk) or 0.0))
            prev_w = w

            if first_gamma:
                ic = rank_ic(raw_scores, y)
                if math.isfinite(ic):
                    ics.append(ic)
                dw = top_n_book(raw_scores, adv,
                                book_n=_contract.SECONDARY_DIAGNOSTIC_BOOK_SIZE,
                                pol=gpol)
                held = np.isfinite(hold_raw)
                diag_ret.append(float((dw * np.where(held, hold_raw, 0.0)).sum()))

        per_gamma[_gkey(gm)] = _book_metrics(
            np.array(gross), np.array(net), np.array(ew_bench),
            np.array(spy_bench), np.array(turn), np.array(cash),
            np.array(held_n, dtype=np.float64), np.array(hhi), np.array(unreal),
            conv, contrib_by_sym, float(gpol["cost_rate_per_side"]),
            spy_missing, gm, np.array(dust))

    primary = per_gamma[_gkey(_contract.PRIMARY_GAMMA_MULTIPLIER)]
    lags = max(1, int(math.ceil(float(horizon) / HOLD_SESSIONS)))
    return {
        "sample": sample,
        "track": track,
        "evaluation_universe": EVALUATION_UNIVERSE,
        "horizon_sessions": int(horizon),
        "evaluation_dates": len(layer_indices),
        "dates_scored": primary["periods"],
        "dates_skipped_no_eligible_universe_or_covariance": skipped,
        "first_date": (snap.dates[section_indices[layer_indices[0]]]
                       if layer_indices else None),
        "last_date": (snap.dates[section_indices[layer_indices[-1]]]
                      if layer_indices else None),
        "predictive": {
            "rank_ic_mean": float(np.mean(ics)) if ics else None,
            "rank_ic_std": float(np.std(ics, ddof=1)) if len(ics) > 1 else None,
            "rank_ic_t_newey_west": (None if not ics
                                     else _none_if_nan(newey_west_t(ics, lags))),
            "rank_ic_positive_fraction": (float(np.mean(np.array(ics) > 0))
                                          if ics else None),
            "rank_ic_dates": len(ics),
            "note": "REPORTED, NEVER THE SELECTION STATISTIC",
        },
        "secondary_diagnostic": {
            "construction": "TOP_%d_EQUAL_WEIGHT_CASH_ZERO"
                            % _contract.SECONDARY_DIAGNOSTIC_BOOK_SIZE,
            "may_carry_primary_verdict": _contract.TOP_N_MAY_CARRY_PRIMARY_VERDICT,
            "gross_return_annualised": (_none_if_nan(annualise(np.array(diag_ret)))
                                        if diag_ret else None),
        },
        "risk_frontier_gamma": per_gamma,
        "primary_gamma_multiplier": _contract.PRIMARY_GAMMA_MULTIPLIER,
        "primary": primary,
        "calibration": (calib.to_dict() if calib is not None else None),
        "forecast_uncertainty_scalar": float(sigma_scalar),
        "sector_exposure": {"state": SECTOR_STATE},
        "benchmarks": {
            "equal_weight": _contract.BENCH_EQUAL_WEIGHT,
            "investable": _contract.BENCH_SPY,
            "investable_state": benchmarks.spy_state,
            "investable_source": benchmarks.source or None,
        },
    }


def _gkey(multiplier) -> str:
    return "gamma_x%.1f" % float(multiplier)


def _book_metrics(gross, net, ew, spy, turn, cash, held_n, hhi, unreal,
                  conv, contrib_by_sym, cost_rate, spy_missing, gm,
                  dust=None) -> dict:
    n = net.size
    ex_ew = net - ew
    spy_ok = np.isfinite(spy)
    ex_spy = np.where(spy_ok, net - spy, np.nan)

    thirds = [ex_ew[i * n // 3:(i + 1) * n // 3] for i in range(3)] if n >= 6 else []
    win_frac = (float(np.mean([t.mean() > 0 for t in thirds if t.size]))
                if thirds else None)

    top_sym, top_val = (None, 0.0)
    if contrib_by_sym:
        top_sym = max(contrib_by_sym, key=lambda s: abs(contrib_by_sym[s]))
        top_val = contrib_by_sym[top_sym]
    total_contrib = float(sum(contrib_by_sym.values())) if contrib_by_sym else 0.0

    ex_best = None
    if n >= 6:
        j = int(np.argmax(ex_ew))
        ex_best = float(np.mean(np.delete(ex_ew, j)))

    return {
        "gamma_multiplier": float(gm),
        "periods": int(n),
        "gross_return_annualised": _none_if_nan(annualise(gross)),
        "net_return_annualised": _none_if_nan(annualise(net)),
        "volatility_annualised": _none_if_nan(volatility(net)),
        # --- benchmark 1: universe-neutral, isolates selection skill --------- #
        "equal_weight_benchmark_annualised": _none_if_nan(annualise(ew)),
        "net_excess_annualised": _none_if_nan(annualise(net) - annualise(ew)),
        "net_excess_mean_per_period": _none_if_nan(float(np.mean(ex_ew))),
        "net_excess_t_newey_west": _none_if_nan(newey_west_t(ex_ew, 3)),
        "information_ratio_vs_equal_weight": _none_if_nan(sharpe(ex_ew)),
        "hit_rate_vs_equal_weight": _none_if_nan(float(np.mean(ex_ew > 0))),
        # --- benchmark 2: the investable alternative ------------------------- #
        "spy_benchmark_annualised": (_none_if_nan(annualise(spy[spy_ok]))
                                     if spy_ok.any() else None),
        "net_excess_vs_spy_annualised": (
            _none_if_nan(annualise(net[spy_ok]) - annualise(spy[spy_ok]))
            if spy_ok.any() else None),
        "net_excess_vs_spy_t_newey_west": (
            _none_if_nan(newey_west_t(ex_spy[spy_ok], 3)) if spy_ok.any() else None),
        "hit_rate_vs_spy": (_none_if_nan(float(np.mean(ex_spy[spy_ok] > 0)))
                            if spy_ok.any() else None),
        "spy_periods_missing": int(spy_missing),
        # --- risk ------------------------------------------------------------ #
        "sharpe_net": _none_if_nan(sharpe(net)),
        "sortino_net": _none_if_nan(sortino(net)),
        "max_drawdown_net": _none_if_nan(max_drawdown(net)),
        "cvar_05_net": _none_if_nan(cvar(net)),
        # --- what the allocation actually did --------------------------------- #
        "cash_weight_mean": _none_if_nan(float(np.mean(cash)) if cash.size else float("nan")),
        "cash_weight_min": _none_if_nan(float(np.min(cash)) if cash.size else float("nan")),
        "cash_weight_max": _none_if_nan(float(np.max(cash)) if cash.size else float("nan")),
        "periods_fully_in_cash": int(np.sum(cash >= 0.999)) if cash.size else 0,
        # Track B only, and always reported beside cash so the two are not
        # confused: weight the proposal asked for that the canonical minimum
        # position size converted into cash. Non-zero here means part of the cash
        # figure above is a proposal the book could not hold, not a preference.
        "proposal_dust_converted_to_cash_mean": (
            _none_if_nan(float(np.mean(dust)))
            if dust is not None and getattr(dust, "size", 0) else None),
        "names_held_mean": _none_if_nan(float(np.mean(held_n)) if held_n.size else float("nan")),
        "concentration_hhi_mean": _none_if_nan(float(np.nanmean(hhi)) if hhi.size else float("nan")),
        "unrealised_weight_mean": _none_if_nan(float(np.mean(unreal)) if unreal.size else float("nan")),
        "allocator_converged_fraction": (float(np.mean(conv)) if conv else None),
        # ``turn`` holds TRADED NOTIONAL (sells + buys). One-way turnover, the
        # conventional reporting statistic, is half of it; the cost drag is the
        # notional itself times the per-side rate - the same quantity actually
        # subtracted from every period's net return above.
        "turnover_mean_one_way": _none_if_nan(float(np.mean(turn)) / 2.0 if turn.size else float("nan")),
        "turnover_annualised": _none_if_nan(
            float(np.mean(turn)) / 2.0 * PERIODS_PER_YEAR if turn.size else float("nan")),
        "traded_notional_annualised": _none_if_nan(
            float(np.mean(turn)) * PERIODS_PER_YEAR if turn.size else float("nan")),
        "cost_drag_annualised": _none_if_nan(
            float(np.mean(turn)) * cost_rate * PERIODS_PER_YEAR if turn.size else float("nan")),
        # The per-period excess series is retained because campaign-wide
        # inference (SPA, paired block bootstrap) needs the SERIES, not a summary
        # statistic; a p-value derived from a mean and a count would ignore the
        # serial dependence these returns actually have.
        "excess_series": [round(float(v), 8) for v in ex_ew],
        "net_series": [round(float(v), 8) for v in net],
        "robustness": {
            "subperiod_thirds_excess": [_none_if_nan(float(t.mean())) for t in thirds],
            "subperiod_win_fraction": win_frac,
            "excess_excluding_best_period": ex_best,
            "largest_single_name_contribution": _none_if_nan(top_val),
            "largest_single_name": top_sym,
            "largest_name_share_of_total": _none_if_nan(
                abs(top_val) / abs(total_contrib) if total_contrib else float("nan")),
        },
    }


def _sample_features(sample: str) -> tuple:
    from . import snapshot as _snap
    if sample == _contract.SAMPLE_PRICE_FULL:
        return _snap.PRICE_FEATURES
    return _snap.ALL_FEATURES


# --------------------------------------------------------------------------- #
# Frozen judge contract
# --------------------------------------------------------------------------- #
def build_contract(*, campaign_id: str = _contract.CAMPAIGN_ID) -> dict:
    body = {
        "contract": JUDGE_SCHEMA,
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "frozen_before_candidate_search": True,
        "one_judge_for_every_candidate": True,
        "hold_sessions": HOLD_SESSIONS,
        "periods_per_year": PERIODS_PER_YEAR,
        "evaluation_universe": EVALUATION_UNIVERSE,
        "primary_construction": "CANONICAL_ZERO_BASE_ALLOCATION_STOCKS_PLUS_CASH",
        "risk_frontier_gamma_multipliers": list(_contract.RISK_FRONTIER_GAMMA_MULTIPLIERS),
        "primary_gamma_multiplier": _contract.PRIMARY_GAMMA_MULTIPLIER,
        "frontier_scope": _contract.FRONTIER_SCOPE,
        "frontier_frozen_before_results": True,
        "secondary_diagnostic": {
            "construction": "TOP_%d_EQUAL_WEIGHT" % _contract.SECONDARY_DIAGNOSTIC_BOOK_SIZE,
            "may_carry_primary_verdict": _contract.TOP_N_MAY_CARRY_PRIMARY_VERDICT,
        },
        "economics": economics_declaration(),
        "tracks": list(_contract.TRACKS),
        "calibration_contract": _calib.contract(),
        "predictive_metrics": ["rank_ic_mean", "rank_ic_std",
                               "rank_ic_t_newey_west",
                               "rank_ic_positive_fraction"],
        "portfolio_metrics": [
            "gross_return_annualised", "net_return_annualised",
            "volatility_annualised", "equal_weight_benchmark_annualised",
            "net_excess_annualised", "net_excess_t_newey_west",
            "spy_benchmark_annualised", "net_excess_vs_spy_annualised",
            "sharpe_net", "sortino_net", "information_ratio_vs_equal_weight",
            "max_drawdown_net", "cvar_05_net", "hit_rate_vs_equal_weight",
            "hit_rate_vs_spy", "cash_weight_mean", "cash_weight_min",
            "cash_weight_max", "periods_fully_in_cash", "names_held_mean",
            "turnover_mean_one_way", "turnover_annualised",
            "cost_drag_annualised", "concentration_hhi_mean",
            "unrealised_weight_mean", "allocator_converged_fraction"],
        "robustness_metrics": [
            "subperiod_thirds_excess", "subperiod_win_fraction",
            "excess_excluding_best_period",
            "largest_single_name_contribution", "largest_name_share_of_total"],
        "primary_selection_principle": (
            "IMPLEMENTABLE NET PORTFOLIO ECONOMICS AT COMPARABLE RISK"),
        "selection_statistic": "net_excess_annualised_vs_%s_at_gamma_x%.1f"
                               % (_contract.BENCH_EQUAL_WEIGHT,
                                  _contract.PRIMARY_GAMMA_MULTIPLIER),
        "not_selected_by": ["MSE", "IC_ALONE", "GROSS_RETURN_ALONE",
                            "SINGLE_PERIOD_SHARPE", "TOP_N_BOOK_ECONOMICS"],
        "declared_limitations": [
            "historical sector exposure is UNMEASURABLE_PIT and is not reported "
            "as a number",
            "per-name forecast uncertainty is not produced by these learners; a "
            "single measured residual scale is used for every name and reported "
            "as forecast_uncertainty_scalar",
            "long-only; the campaign models no short book because the operational "
            "boundary is a long paper portfolio",
            "the frozen panel cannot represent every historical S&P 500 member; "
            "the gap is MEASURED by alpha_agent.r31.universe.survivorship_report",
        ],
    }
    body["judge_hash"] = r31.sha(body)
    body.update(r31.safety_block())
    return body


def behaviour_hash() -> str:
    """A hash of everything that changes what a SCORE MEANS.

    Bound into every candidate's specification hash, so a change to the cost
    arithmetic, the rebalance cadence, the evaluation universe, the benchmark
    policy, the risk frontier or the canonical policy invalidates cached
    candidates instead of silently mixing results measured under two different
    judges. The campaign id is deliberately excluded - the same judge behaviour
    across two campaigns is the same behaviour.
    """
    return r31.sha({
        "schema": JUDGE_SCHEMA,
        "hold_sessions": HOLD_SESSIONS,
        "periods_per_year": PERIODS_PER_YEAR,
        "primary_construction": "CANONICAL_ZERO_BASE_ALLOCATION_STOCKS_PLUS_CASH",
        "gamma_multipliers": list(_contract.RISK_FRONTIER_GAMMA_MULTIPLIERS),
        "primary_gamma": _contract.PRIMARY_GAMMA_MULTIPLIER,
        "economics": economics_declaration(),
        "cost_base": _contract.COST_BASE,
        "turnover_alignment": _contract.TURNOVER_ALIGNMENT,
        "evaluation_universe": EVALUATION_UNIVERSE,
        "benchmarks": list(_contract.BENCHMARKS_REPORTED),
        "sector_state": SECTOR_STATE,
        "min_eligible": MIN_ELIGIBLE,
        # The Track-A units conversion is part of what a score MEANS: a different
        # calibration floor admits a different set of capital allocators.
        "calibration": _calib.contract(),
    })


def path_for(campaign_id: str = _contract.CAMPAIGN_ID):
    return r31.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict):
    return r31.write_json(path_for(body["campaign_id"]), body)
