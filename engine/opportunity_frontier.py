r"""engine/opportunity_frontier.py - Release 50: the ONE cross-asset opportunity
frontier (pure kernel).

Every CAPITAL-ELIGIBLE opportunity - the current equity holdings, the eligible
equity candidates, every eligible non-equity instrument, and cash - enters ONE
frontier with the SAME portfolio-construction inputs:

    sleeve, asset class, instrument, instrument type, currency
    operational signal -> normalised opportunity_score with its BASIS
    expected return (only when a calibrated forecast exists; otherwise NOT_CALIBRATED)
    uncertainty state
    volatility, risk contribution, diversification effect (advisory)
    transaction cost, holding cost, liquidity, capacity, unit granularity
    current capital, candidate capital
    eligibility, and the exact reason when not eligible

SCORE COMPARABILITY (the critical part, stated as data)
-------------------------------------------------------
A raw equity percentile, a futures momentum score, an FX carry, a t-statistic, a
crypto funding rate and a macro surprise are NOT the same unit and are never
compared as if they were. The frontier carries ONE normalised representation:

* ``OPERATIONAL_MODEL_COMBINED_PERCENTILE`` - the approved US-equity model's
  cross-sectional combined percentile in [0, 1] (the unit the proposal, the
  switching hurdle and the reassessment already use);
* ``OPERATIONAL_SLEEVE_NORMALISED_RANK`` - for an APPROVED non-equity sleeve, the
  sleeve's approved operational signal rank-normalised to [0, 1] within that
  sleeve's opportunity set (declared on the approval record; never derived here);
* ``CASH_DECLARED_ZERO`` - cash, at the declared zero paper return;
* ``NONE_RESEARCH_ONLY`` - an instrument whose only signal is research; it is
  listed, scored ``None`` and NOT eligible.

A research statistic never becomes an expected return here. ``expected_return``
is populated ONLY from a calibrated forecast supplied by the forecast owner and is
otherwise ``None`` with ``expected_return_state = NOT_CALIBRATED``. The
comparability limitation is declared: two rank-normalised scores say "how good is
this instrument relative to its sleeve's opportunity set" - they assume comparable
opportunity dispersion across sleeves, which is why an approved non-equity sleeve
still has to clear the SAME entry / exit and switching rules as an equity name,
and why a zero-signal instrument can never receive capital as a residual sink.

Pure stdlib; no I/O.
"""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Optional

from paper_trader.engine import instrument_contract as ic

PHASE = "R50"
CALCULATION_OWNER = "engine.opportunity_frontier"
SCHEMA_VERSION = "opportunity_frontier.v1"

SB_EQUITY_PERCENTILE = "OPERATIONAL_MODEL_COMBINED_PERCENTILE"
SB_SLEEVE_RANK = "OPERATIONAL_SLEEVE_NORMALISED_RANK"
SB_CASH = "CASH_DECLARED_ZERO"
SB_NONE = "NONE_RESEARCH_ONLY"
SCORE_BASIS_VOCAB = (SB_EQUITY_PERCENTILE, SB_SLEEVE_RANK, SB_CASH, SB_NONE)

ER_NOT_CALIBRATED = "NOT_CALIBRATED"
ER_CALIBRATED = "WALK_FORWARD_CALIBRATED"

E_NOT_ELIGIBLE_SLEEVE = "SLEEVE_NOT_CAPITAL_ELIGIBLE"
E_NO_SCORE = "NO_OPERATIONAL_SCORE"
E_UNIT_GRANULARITY = "UNIT_NOTIONAL_EXCEEDS_NAME_CAP_AT_NAV"
E_MARK = "MARK_OR_FX_UNAVAILABLE"
E_LIQUIDITY = "BELOW_LIQUIDITY_FLOOR"
E_UNIVERSE = "NOT_IN_ELIGIBLE_UNIVERSE"

COMPARABILITY_POLICY = {
    "representation": "opportunity_score in [0, 1] with an explicit score_basis",
    "bases": list(SCORE_BASIS_VOCAB),
    "transformation": {
        SB_EQUITY_PERCENTILE: "combined percentile from api.universe_scoring, unchanged",
        SB_SLEEVE_RANK: "rank percentile of the approved operational signal within the "
                        "sleeve's opportunity set, declared on the approval record",
        SB_CASH: "0.0 by the declared paper cash policy",
        SB_NONE: "no score; never eligible",
    },
    "normalisation": "rank-based within each sleeve; cross-sleeve comparison assumes "
                     "comparable opportunity dispersion (declared limitation)",
    "uncertainty": "rank scores carry no calibrated forecast error; uncertainty_state "
                   "= UNQUANTIFIED_RANK_SCORE unless a calibrated forecast supplies one",
    "expected_return": "populated only from a calibrated forecast; research statistics "
                       "never become expected return",
    "hurdle": "the frozen Release-47 switching hurdle in the same score points",
    "zero_signal_rule": "an instrument with no operational score is never a residual "
                        "capital sink; cash is",
}


def _f(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _r(x: Optional[float], nd: int) -> Optional[float]:
    return None if x is None else round(float(x), nd)


def stable_hash(obj: Any) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(",", ":"),
                                     default=str).encode("utf-8")).hexdigest()[:32]


def rank_normalise(scores: dict) -> dict:
    """Rank percentile in (0, 1] within one sleeve (ties share the mean rank)."""
    vals = [(k, _f(v)) for k, v in (scores or {}).items() if _f(v) is not None]
    if not vals:
        return {}
    n = len(vals)
    if n == 1:
        return {vals[0][0]: 1.0}
    ordered = sorted(vals, key=lambda kv: (kv[1], kv[0]))
    out = {}
    i = 0
    while i < n:
        j = i
        while j + 1 < n and ordered[j + 1][1] == ordered[i][1]:
            j += 1
        mean_rank = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            out[ordered[k][0]] = mean_rank / n
        i = j + 1
    return out


def _row(**kw) -> dict:
    base = {
        "instrument_id": None, "label": None, "sleeve_id": None, "asset_class": None,
        "asset_class_label": None, "instrument_type": None, "currency": ic.REPORTING_CURRENCY,
        "sector": None, "rank": None,
        "opportunity_score": None, "score_basis": SB_NONE,
        "expected_return": None, "expected_return_state": ER_NOT_CALIBRATED,
        "uncertainty_state": "UNQUANTIFIED_RANK_SCORE",
        "volatility_annualised": None, "risk_contribution": None,
        "diversification_effect": None,
        "transaction_cost_bps_per_side": None, "holding_cost_bps_annual": None,
        "liquidity_adv_dollar": None, "liquidity_state": None,
        "capacity_dollar": None, "unit_notional_usd": None, "capital_usage_ratio": None,
        "executable_at_nav": None,
        "current_weight": 0.0, "current_capital_usd": 0.0,
        "candidate_capital_usd": None, "held": False,
        "eligible": False, "eligibility_reason": None,
        "multiplier": 1.0, "initial_margin_per_unit": 0.0,
    }
    base.update(kw)
    return base


def build_frontier(*, eligible_market_date: Optional[str], nav: Optional[float],
                   equity_rankings: list, equity_sleeve_eligible: bool,
                   non_equity_instruments: list, positions: list,
                   risk_state: Optional[dict] = None, expected_returns: Optional[dict] = None,
                   policy: Optional[dict] = None) -> dict:
    """Build the frontier. Pure.

    ``equity_rankings`` are the scoring owner's ranking rows; ``non_equity_instruments``
    are registry descriptors (already CAPITAL-ELIGIBLE at sleeve level) carrying
    ``opportunity_score`` / ``mark`` / ``unit_notional_usd`` / ``executable_at_nav``;
    ``positions`` are position-contract rows (current book); ``risk_state`` is the
    cross-asset risk state; ``expected_returns`` is an OPTIONAL calibrated
    ``{instrument_id: mu}`` from the forecast owner.
    """
    pol = {"max_name_weight": 0.10, "min_adv_dollar": 1.0e7, "max_adv_participation": 1.0,
           "min_position_weight": 0.005}
    if policy:
        pol.update({k: v for k, v in policy.items() if k in pol})
    navv = _f(nav)
    er = dict(expected_returns or {})
    rs = risk_state or {}
    contrib = rs.get("risk_contribution") or {}
    svol = rs.get("standalone_volatility_annualised") or {}
    held = {p.get("instrument_id"): p for p in (positions or []) if isinstance(p, dict)}
    rows: list[dict] = []

    def _er(tk):
        v = _f(er.get(tk))
        return (v, ER_CALIBRATED) if v is not None else (None, ER_NOT_CALIBRATED)

    # --- equities: the approved model's own percentile ------------------------- #
    for r in equity_rankings or []:
        tk = r.get("ticker")
        if not tk:
            continue
        pct = _f(r.get("percentile"))
        adv = _f(r.get("adv_dollar"))
        elig_u = bool(r.get("eligible", True))
        p = held.get(tk) or {}
        cw = _f(p.get("exposure_weight")) or 0.0
        cap = float(pol["max_name_weight"])
        capacity = (min(cap * navv, float(pol["max_adv_participation"]) * adv)
                    if (navv and adv is not None) else (cap * navv if navv else None))
        reason = None
        eligible = True
        if not equity_sleeve_eligible:
            eligible, reason = False, E_NOT_ELIGIBLE_SLEEVE
        elif not elig_u:
            eligible, reason = False, E_UNIVERSE
        elif pct is None:
            eligible, reason = False, E_NO_SCORE
        elif adv is not None and adv < float(pol["min_adv_dollar"]):
            eligible, reason = False, E_LIQUIDITY
        mu, mu_state = _er(tk)
        rows.append(_row(
            instrument_id=tk, label=tk, sleeve_id=ic.DEFAULT_EQUITY_SLEEVE,
            asset_class=ic.AC_US_EQUITY, asset_class_label=ic.ASSET_CLASS_LABELS[ic.AC_US_EQUITY],
            instrument_type=ic.IT_CASH_EQUITY, sector=r.get("sector") or "Unknown",
            rank=r.get("rank"), opportunity_score=_r(pct, 6),
            score_basis=SB_EQUITY_PERCENTILE if pct is not None else SB_NONE,
            expected_return=_r(mu, 6), expected_return_state=mu_state,
            uncertainty_state=("CALIBRATED_FORECAST" if mu is not None else "UNQUANTIFIED_RANK_SCORE"),
            volatility_annualised=svol.get(tk), risk_contribution=contrib.get(tk),
            transaction_cost_bps_per_side=ic.COST_BPS_PER_SIDE_BY_CLASS[ic.AC_US_EQUITY],
            holding_cost_bps_annual=0.0, liquidity_adv_dollar=adv,
            liquidity_state=("LIQUID" if (adv is not None and adv >= float(pol["min_adv_dollar"]))
                             else ("ILLIQUID" if adv is not None else "UNAVAILABLE")),
            capacity_dollar=_r(capacity, 2), unit_notional_usd=_r(_f(p.get("mark")), 6),
            capital_usage_ratio=1.0, executable_at_nav=True,
            current_weight=_r(cw, 6), current_capital_usd=_r(_f(p.get("notional_usd")), 2),
            candidate_capital_usd=_r(capacity, 2), held=bool(p),
            eligible=eligible, eligibility_reason=reason))

    # --- held names outside the equity ranking (mandatory exits / unranked) ---- #
    ranked = {r.get("ticker") for r in equity_rankings or []}
    for tk, p in held.items():
        if tk in ranked or p.get("instrument_type") != ic.IT_CASH_EQUITY:
            continue
        rows.append(_row(
            instrument_id=tk, label=tk, sleeve_id=p.get("sleeve_id") or ic.DEFAULT_EQUITY_SLEEVE,
            asset_class=ic.AC_US_EQUITY, asset_class_label=ic.ASSET_CLASS_LABELS[ic.AC_US_EQUITY],
            instrument_type=ic.IT_CASH_EQUITY, sector=p.get("sector") or "Unknown",
            opportunity_score=None, score_basis=SB_NONE,
            transaction_cost_bps_per_side=ic.COST_BPS_PER_SIDE_BY_CLASS[ic.AC_US_EQUITY],
            current_weight=_r(_f(p.get("exposure_weight")), 6),
            current_capital_usd=_r(_f(p.get("notional_usd")), 2), held=True,
            eligible=False, eligibility_reason=E_UNIVERSE))

    # --- eligible non-equity instruments (approved sleeves only) --------------- #
    for d in non_equity_instruments or []:
        tk = d.get("instrument_id")
        if not tk:
            continue
        score = _f(d.get("opportunity_score"))
        p = held.get(tk) or {}
        un = _f(d.get("unit_notional_usd"))
        adv_units = _f(d.get("average_daily_volume_units"))
        adv_dollar = (adv_units * un) if (adv_units is not None and un is not None) else None
        cap = float(pol["max_name_weight"])
        capacity = None
        if navv:
            capacity = cap * navv
            if adv_dollar is not None:
                capacity = min(capacity, float(pol["max_adv_participation"]) * adv_dollar)
        eligible, reason = True, None
        if score is None:
            eligible, reason = False, E_NO_SCORE
        elif un is None:
            eligible, reason = False, E_MARK
        elif d.get("executable_at_nav") is False:
            eligible, reason = False, E_UNIT_GRANULARITY
        mu, mu_state = _er(tk)
        rows.append(_row(
            instrument_id=tk, label=d.get("label") or tk, sleeve_id=d.get("sleeve_id"),
            asset_class=d.get("asset_class"), asset_class_label=d.get("asset_class_label"),
            instrument_type=d.get("instrument_type"), currency=d.get("currency"),
            sector=d.get("sector") or d.get("asset_class_label"),
            rank=d.get("rank"), opportunity_score=_r(score, 6),
            score_basis=SB_SLEEVE_RANK if score is not None else SB_NONE,
            expected_return=_r(mu, 6), expected_return_state=mu_state,
            uncertainty_state=("CALIBRATED_FORECAST" if mu is not None else "UNQUANTIFIED_RANK_SCORE"),
            volatility_annualised=svol.get(tk), risk_contribution=contrib.get(tk),
            diversification_effect=d.get("diversification_effect"),
            transaction_cost_bps_per_side=d.get("cost_bps_per_side"),
            holding_cost_bps_annual=d.get("holding_cost_bps_annual"),
            liquidity_adv_dollar=_r(adv_dollar, 2),
            liquidity_state=("LIQUID" if adv_dollar and adv_dollar >= float(pol["min_adv_dollar"])
                             else ("ILLIQUID" if adv_dollar is not None else "UNAVAILABLE")),
            capacity_dollar=_r(capacity, 2), unit_notional_usd=_r(un, 2),
            capital_usage_ratio=_r(_f(d.get("capital_usage_ratio")), 6),
            executable_at_nav=d.get("executable_at_nav"),
            current_weight=_r(_f(p.get("exposure_weight")), 6) or 0.0,
            current_capital_usd=_r(_f(p.get("notional_usd")), 2) or 0.0,
            candidate_capital_usd=_r(capacity, 2), held=bool(p),
            eligible=eligible, eligibility_reason=reason,
            multiplier=d.get("multiplier"), initial_margin_per_unit=d.get("initial_margin_per_unit")))

    # --- held non-equity positions whose sleeve is no longer eligible ---------- #
    listed = {r["instrument_id"] for r in rows}
    for tk, p in held.items():
        if tk in listed or p.get("instrument_type") in (ic.IT_CASH_EQUITY, ic.IT_CASH):
            continue
        rows.append(_row(
            instrument_id=tk, label=p.get("label") or tk, sleeve_id=p.get("sleeve_id"),
            asset_class=p.get("asset_class"), asset_class_label=p.get("asset_class_label"),
            instrument_type=p.get("instrument_type"), currency=p.get("currency"),
            sector=p.get("sector"), opportunity_score=None, score_basis=SB_NONE,
            current_weight=_r(_f(p.get("exposure_weight")), 6) or 0.0,
            current_capital_usd=_r(_f(p.get("notional_usd")), 2) or 0.0, held=True,
            eligible=False, eligibility_reason=E_NOT_ELIGIBLE_SLEEVE,
            multiplier=p.get("multiplier")))

    # --- cash ------------------------------------------------------------------ #
    cash_w = 1.0 - sum((_f(p.get("exposure_weight")) or 0.0) for p in held.values())
    rows.append(_row(
        instrument_id=ic.CASH_INSTRUMENT_ID, label="USD cash", sleeve_id=ic.CASH_SLEEVE,
        asset_class=ic.AC_CASH, asset_class_label=ic.ASSET_CLASS_LABELS[ic.AC_CASH],
        instrument_type=ic.IT_CASH, opportunity_score=0.0, score_basis=SB_CASH,
        expected_return=ic.CASH_RETURN, expected_return_state="DECLARED_POLICY",
        uncertainty_state="RISKLESS_BY_DECLARATION", volatility_annualised=0.0,
        transaction_cost_bps_per_side=0.0, holding_cost_bps_annual=0.0,
        liquidity_state="LIQUID", capacity_dollar=_r(navv, 2), capital_usage_ratio=1.0,
        executable_at_nav=True, current_weight=_r(max(0.0, cash_w), 6),
        current_capital_usd=_r((max(0.0, cash_w) * navv) if navv else None, 2),
        candidate_capital_usd=_r(navv, 2), held=True, eligible=True, eligibility_reason=None))

    rows.sort(key=lambda r: (-(r["opportunity_score"] if r["opportunity_score"] is not None else -1.0),
                             r["instrument_id"]))
    eligible_rows = [r for r in rows if r["eligible"]]
    by_class: dict[str, dict] = {}
    for r in rows:
        b = by_class.setdefault(r["asset_class"] or "UNKNOWN", {"listed": 0, "eligible": 0, "held": 0})
        b["listed"] += 1
        b["eligible"] += int(bool(r["eligible"]))
        b["held"] += int(bool(r["held"]))
    body = {
        "schema_version": SCHEMA_VERSION, "phase": PHASE, "calculation_owner": CALCULATION_OWNER,
        "eligible_market_date": eligible_market_date, "nav": _r(navv, 2),
        "rows": rows, "row_count": len(rows),
        "eligible_instrument_count": len(eligible_rows),
        "eligible_non_cash_count": sum(1 for r in eligible_rows if r["instrument_type"] != ic.IT_CASH),
        "eligible_non_equity_count": sum(1 for r in eligible_rows
                                         if r["instrument_type"] not in (ic.IT_CASH, ic.IT_CASH_EQUITY)),
        "asset_classes_present": sorted({r["asset_class"] for r in rows if r["held"] and r["asset_class"]}),
        "asset_classes_eligible": sorted({r["asset_class"] for r in eligible_rows if r["asset_class"]}),
        "by_asset_class": by_class,
        "ineligibility_reasons": sorted({r["eligibility_reason"] for r in rows if r["eligibility_reason"]}),
        "score_basis_vocabulary": list(SCORE_BASIS_VOCAB),
        "comparability_policy": dict(COMPARABILITY_POLICY),
        "expected_return_state": (ER_CALIBRATED if er else ER_NOT_CALIBRATED),
        "policy": pol,
        "forced_diversification": False,
        "incumbency_privilege": False,
        "safety": ic.safety_block(),
    }
    body["frontier_hash"] = stable_hash({k: v for k, v in body.items() if k != "safety"})
    return body


def candidate_rows_for_proposal(frontier: dict, *, held: Optional[set] = None) -> list[dict]:
    """The ELIGIBLE non-equity frontier rows in the shape the proposal kernel's
    ``universe_rows`` already use (``ticker / sector / adv_dollar / rank / percentile /
    eligible``) plus the instrument descriptor fields the multi-asset constraints
    read. Equities are NOT re-emitted (the scoring owner's rows are authoritative)."""
    out = []
    rows = [r for r in (frontier or {}).get("rows") or []
            if r.get("eligible") and r.get("instrument_type") not in (ic.IT_CASH, ic.IT_CASH_EQUITY)]
    rows.sort(key=lambda r: (-(r["opportunity_score"] or 0.0), r["instrument_id"]))
    for i, r in enumerate(rows, start=1):
        out.append({
            "ticker": r["instrument_id"], "sector": r.get("sector") or r.get("asset_class_label"),
            "adv_dollar": r.get("liquidity_adv_dollar"), "rank": r.get("rank") or i,
            "percentile": r.get("opportunity_score"), "eligible": True,
            "asset_class": r.get("asset_class"), "sleeve_id": r.get("sleeve_id"),
            "instrument_type": r.get("instrument_type"), "currency": r.get("currency"),
            "multiplier": r.get("multiplier"), "initial_margin_per_unit": r.get("initial_margin_per_unit"),
            "unit_notional_usd": r.get("unit_notional_usd"),
            "capital_usage_ratio": r.get("capital_usage_ratio"),
            "cost_bps_per_side": r.get("transaction_cost_bps_per_side"),
            "score_basis": r.get("score_basis"), "frontier_row": True,
        })
    return out


__all__ = ["PHASE", "CALCULATION_OWNER", "SCHEMA_VERSION", "SCORE_BASIS_VOCAB",
           "SB_EQUITY_PERCENTILE", "SB_SLEEVE_RANK", "SB_CASH", "SB_NONE",
           "ER_NOT_CALIBRATED", "ER_CALIBRATED", "COMPARABILITY_POLICY",
           "rank_normalise", "build_frontier", "candidate_rows_for_proposal", "stable_hash"]
