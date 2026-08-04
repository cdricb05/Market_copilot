"""Stage 12 Workstream D -- event-driven, point-in-time-safe feature foundation.

Builds cross-sectional features around *information arrival* (SEC filings), so a
signal reflects a fundamental CHANGE dated by the day the market could first see
it (the ``filed`` date), never the fiscal period-end. Each builder returns
cross-sections in the exact ``{as_of, names:[(key, adj_signal, forward_ret)]}``
format the UNCHANGED evaluator (`signal_evaluation.evaluate_periods`) consumes,
so Stage 12 event candidates flow through the identical gate battery as Stage 11.

Point-in-time guarantees (all inherited from ``PitFundamentalsStore``):
* availability = SEC ``filed`` date; ``store.as_of`` never returns a fact filed
  after the formation date (no future restatement leaks in).
* restatement/amendment handling and (accn,tag,unit,end,fy,fp) de-duplication are
  the store's; amendments are preferred on tie.
* prior period is the strictly-comparable (FY-1)-FP key via ``prior_fiscal_key``
  (no adjacent-quarter substitution).

If owned PIT fundamentals do not cover enough names per formation date, the built
cross-sections are thin and the evaluator keeps the candidate DATA_HOLD -- an
honest "owned data cannot power this event study", never a fabricated spread.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence

try:
    from . import fundamental_evidence as _fev
    from . import signal_library as _sl
    from . import stage12_execution as _exec
except Exception:  # pragma: no cover
    import fundamental_evidence as _fev  # type: ignore
    import signal_library as _sl  # type: ignore
    import stage12_execution as _exec  # type: ignore

ANCHOR_CONCEPT = "assets"


def _obs_value(v: Any) -> Optional[float]:
    """Numeric value of a PIT lookup.

    ``PitFundamentalsStore.as_of`` returns a ``PitObservation`` (whose ``.value``
    is the number), NOT a bare float; earlier code did ``float(observation)`` and
    silently got ``None`` for every fact (the true root cause of "0 scored
    periods"). ``getattr(v, 'value', v)`` unwraps an observation and passes a raw
    number/float store (used in tests) through unchanged.
    """
    if v is None:
        return None
    val = getattr(v, "value", v)
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _ratio(store: Any, cik: str, num: str, den: str, fk: str, as_of: str) -> Optional[float]:
    a = _obs_value(store.as_of(cik, num, fk, as_of))
    b = _obs_value(store.as_of(cik, den, fk, as_of))
    if a is None or b is None or b == 0:
        return None
    return a / b


def _level(store: Any, cik: str, concept: str, fk: str, as_of: str) -> Optional[float]:
    return _obs_value(store.as_of(cik, concept, fk, as_of))


def _growth(store: Any, cik: str, concept: str, fk_now: str, fk_prior: str,
            as_of: str) -> Optional[float]:
    now = _level(store, cik, concept, fk_now, as_of)
    prior = _level(store, cik, concept, fk_prior, as_of)
    if now is None or prior is None or prior == 0:
        return None
    return now / prior - 1.0


# Each event feature: (store, cik, fk_now, fk_prior, as_of) -> Optional[float].
def _revenue_acceleration(store, cik, fk_now, fk_prior, as_of):
    return _growth(store, cik, "revenue", fk_now, fk_prior, as_of)


def _gross_margin_change(store, cik, fk_now, fk_prior, as_of):
    now = _ratio(store, cik, "gross_profit", "revenue", fk_now, as_of)
    prior = _ratio(store, cik, "gross_profit", "revenue", fk_prior, as_of)
    return None if now is None or prior is None else now - prior


def _operating_margin_change(store, cik, fk_now, fk_prior, as_of):
    now = _ratio(store, cik, "operating_income", "revenue", fk_now, as_of)
    prior = _ratio(store, cik, "operating_income", "revenue", fk_prior, as_of)
    return None if now is None or prior is None else now - prior


def _cash_flow_improvement(store, cik, fk_now, fk_prior, as_of):
    cash_now = _level(store, cik, "cash", fk_now, as_of)
    cash_prior = _level(store, cik, "cash", fk_prior, as_of)
    assets = _level(store, cik, "assets", fk_now, as_of)
    if cash_now is None or cash_prior is None or not assets:
        return None
    return (cash_now - cash_prior) / assets


def _accrual_quality_change(store, cik, fk_now, fk_prior, as_of):
    def accr(fk):
        ni = _ratio(store, cik, "net_income", "assets", fk, as_of)
        return ni
    now = accr(fk_now)
    prior = accr(fk_prior)
    if now is None or prior is None:
        return None
    # rising net_income/assets that is NOT cash-backed is penalised; we proxy the
    # accrual *change* by the change in net_income/assets net of cash change.
    dcash = _cash_flow_improvement(store, cik, fk_now, fk_prior, as_of) or 0.0
    return -((now - prior) - dcash)


def _asset_growth_inflection(store, cik, fk_now, fk_prior, as_of):
    g = _growth(store, cik, "assets", fk_now, fk_prior, as_of)
    return None if g is None else -g


def _leverage_reduction(store, cik, fk_now, fk_prior, as_of):
    now = _ratio(store, cik, "liabilities", "assets", fk_now, as_of)
    prior = _ratio(store, cik, "liabilities", "assets", fk_prior, as_of)
    return None if now is None or prior is None else -(now - prior)


def _profitability_inflection(store, cik, fk_now, fk_prior, as_of):
    now = _ratio(store, cik, "net_income", "assets", fk_now, as_of)
    prior = _ratio(store, cik, "net_income", "assets", fk_prior, as_of)
    return None if now is None or prior is None else now - prior


EVENT_BUILDERS: Dict[str, Callable] = {
    "event_revenue_acceleration": _revenue_acceleration,
    "event_gross_margin_change": _gross_margin_change,
    "event_operating_margin_change": _operating_margin_change,
    "event_cash_flow_improvement": _cash_flow_improvement,
    "event_accrual_quality_change": _accrual_quality_change,
    "event_asset_growth_inflection": _asset_growth_inflection,
    "event_leverage_reduction": _leverage_reduction,
    "event_profitability_inflection": _profitability_inflection,
    # confirmatory fundamental levels re-used as event ratios:
    "confirm_gross_profitability": lambda s, c, n, p, a: _ratio(s, c, "gross_profit", "assets", n, a),
    "confirm_gross_profit_growth": lambda s, c, n, p, a: _growth(s, c, "gross_profit", n, p, a),
}

# Concepts each builder needs (for the coverage / DATA_HOLD reason).
BUILDER_CONCEPTS: Dict[str, List[str]] = {
    "event_revenue_acceleration": ["revenue"],
    "event_gross_margin_change": ["gross_profit", "revenue"],
    "event_operating_margin_change": ["operating_income", "revenue"],
    "event_cash_flow_improvement": ["cash", "assets"],
    "event_accrual_quality_change": ["net_income", "assets", "cash"],
    "event_asset_growth_inflection": ["assets"],
    "event_leverage_reduction": ["liabilities", "assets"],
    "event_profitability_inflection": ["net_income", "assets"],
    "confirm_gross_profitability": ["gross_profit", "assets"],
    "confirm_gross_profit_growth": ["gross_profit", "assets"],
}


def build_event_periods(builder_key: str, ctx: Mapping[str, Any],
                        rebalance_dates: Sequence[str], *, direction: int = 1,
                        horizon_days: int = 63, winsor: float = 0.02,
                        min_names_for_period: int = 3) -> Dict[str, Any]:
    """Build PIT-safe event cross-sections for one builder.

    ``ctx`` is a ``stage11_research.build_panel_context`` dict (needs
    ``close_index``, ``store`` = PitFundamentalsStore, ``cik_to_assetid``).
    Returns ``{periods, coverage, pit, data_hold_reason}``. ``periods`` is empty
    (and a ``data_hold_reason`` is set) when the store or a required input is
    absent -- never fabricated.
    """
    store = ctx.get("store")
    cik_to_assetid = ctx.get("cik_to_assetid") or {}
    close_index = ctx.get("close_index") or {}
    fn = EVENT_BUILDERS.get(builder_key)
    pit = {
        "availability": "SEC filed date (store.as_of never returns filed>as_of)",
        "restatement": "amendments preferred on tie; store de-dupes (accn,tag,unit,end,fy,fp)",
        "prior_period": "strictly-comparable (FY-1)-FP via prior_fiscal_key",
        "no_lookahead": True,
        "required_concepts": BUILDER_CONCEPTS.get(builder_key, []),
    }
    if fn is None:
        return {"periods": [], "coverage": {}, "pit": pit,
                "data_hold_reason": "UNKNOWN_EVENT_BUILDER:%s" % builder_key}
    if store is None:
        return {"periods": [], "coverage": {}, "pit": pit,
                "data_hold_reason": "DATA_HOLD_NO_PIT_FUNDAMENTALS_STORE"}
    if not cik_to_assetid:
        return {"periods": [], "coverage": {}, "pit": pit,
                "data_hold_reason": "DATA_HOLD_NO_CIK_TO_ASSETID"}

    sign = int(direction) or 1
    periods: List[Dict[str, Any]] = []
    names_counts: List[int] = []
    for d in rebalance_dates:
        raw: Dict[str, float] = {}
        for cik, assetid in cik_to_assetid.items():
            if assetid not in close_index:
                continue
            try:
                fk_now = store.latest_fiscal_key(cik, d, concept=ANCHOR_CONCEPT)
            except Exception:
                fk_now = None
            if not fk_now:
                continue
            try:
                fk_prior = store.prior_fiscal_key(cik, d, fk_now, concept=ANCHOR_CONCEPT)
            except Exception:
                fk_prior = None
            try:
                val = fn(store, cik, fk_now, fk_prior, d)
            except Exception:
                val = None
            if val is None:
                continue
            raw[assetid] = float(val)
        if len(raw) < min_names_for_period:
            continue
        raw = _sl.winsorize(raw, winsor)
        names = []
        for key, val in raw.items():
            idx = close_index.get(key)
            if idx is None:
                continue
            fwd = _exec.forward_return_lagged(idx[0], idx[1], d, horizon_days)
            if fwd is None:
                continue
            names.append((key, float(val) * sign, float(fwd)))
        if len(names) >= min_names_for_period:
            periods.append({"as_of": d, "names": names})
            names_counts.append(len(names))

    coverage = {
        "n_periods": len(periods),
        "median_names": (sorted(names_counts)[len(names_counts) // 2]
                         if names_counts else 0),
        "min_names": min(names_counts) if names_counts else 0,
        "max_names": max(names_counts) if names_counts else 0,
        "rebalance_dates_requested": len(rebalance_dates),
    }
    data_hold_reason = None
    if coverage["n_periods"] < 12:
        data_hold_reason = "DATA_HOLD_INSUFFICIENT_PERIODS(%d<12)" % coverage["n_periods"]
    elif coverage["median_names"] < 20:
        data_hold_reason = "DATA_HOLD_INSUFFICIENT_UNIVERSE(median=%d<20)" % coverage["median_names"]
    return {"periods": periods, "coverage": coverage, "pit": pit,
            "data_hold_reason": data_hold_reason}


def is_event_builder(builder_key: str) -> bool:
    return builder_key in EVENT_BUILDERS
