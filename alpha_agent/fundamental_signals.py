"""
alpha_agent.fundamental_signals - Stage 9.4 Track F: the fundamental MVP signals.

Three initial, economically-motivated fundamental factors computed ONLY from
owned point-in-time SEC observations (``pit_fundamentals.PitFundamentalsStore``):

  1. gross_profitability = Gross Profit / Assets (Novy-Marx), PREFERRED, with a
     deterministic (Revenue - Cost of Revenue) / Assets fallback used ONLY when
     Gross Profit is unavailable;
  2. asset_growth       = annual percentage change in Assets (Cooper-Gulen-Schill);
  3. balance_sheet_quality = equity / assets and leverage = liabilities / assets.

Leakage-safety is inherited from the PIT store: every value is read via
``as_of(..., as_of_date)`` which returns the latest fact FILED on or before the
as-of date, so a current snapshot is never substituted historically and no fact
is used before its filing date. Missing inputs yield an EXPLICIT None (never an
imputed value). The canonical Stage 9 evidence contract is only run once a
minimum company + rebalance-period coverage passes (``coverage_ready``); until
then the fundamental candidates stay DATA_HOLD.

Pure stdlib; deterministic; no state writes, no network.
"""
from __future__ import annotations

from typing import Optional

from . import pit_fundamentals as pfd

SIGNALS = ("gross_profitability", "asset_growth", "balance_sheet_quality")

# Pre-registered expected signs (documented before evaluation).
EXPECTED_SIGN = {"gross_profitability": 1, "asset_growth": -1,
                 "balance_sheet_quality": 1}


def _val(store: pfd.PitFundamentalsStore, cik: str, concept: str,
         fiscal_key: str, as_of: str) -> Optional[float]:
    obs = store.as_of(cik, concept, fiscal_key, as_of)
    if obs is None:
        return None
    try:
        return float(obs.value)
    except (TypeError, ValueError):
        return None


def gross_profitability(store, *, cik: str, fiscal_key: str, as_of: str) -> dict:
    """Gross Profit / Assets (Novy-Marx), PREFERRED; deterministic fallback to
    (Revenue - Cost of Revenue) / Assets ONLY when Gross Profit is unavailable.

    Every input is read for the SAME fiscal period ``fiscal_key`` as-of ``as_of``,
    so the numerator and denominator never combine different fiscal periods.
    Returns ``{value, basis, missing}`` with an explicit missing list (never
    imputed); ``basis`` records which path produced the value (``gross_profit`` |
    ``revenue_minus_cost_fallback``) so it can be persisted in every observation
    and experiment artifact."""
    assets = _val(store, cik, "assets", fiscal_key, as_of)
    if assets is None or assets <= 0:
        return {"value": None, "basis": None, "missing": ["assets"]}
    # 1. Preferred: Gross Profit / Assets.
    gp = _val(store, cik, "gross_profit", fiscal_key, as_of)
    if gp is not None:
        return {"value": gp / assets, "basis": "gross_profit", "missing": []}
    # 2. Fallback ONLY when Gross Profit is unavailable.
    rev = _val(store, cik, "revenue", fiscal_key, as_of)
    cost = _val(store, cik, "cost_of_revenue", fiscal_key, as_of)
    if rev is not None and cost is not None:
        return {"value": (rev - cost) / assets,
                "basis": "revenue_minus_cost_fallback", "missing": []}
    miss = [c for c, v in (("gross_profit", gp), ("revenue", rev),
                           ("cost_of_revenue", cost)) if v is None]
    return {"value": None, "basis": None, "missing": miss}


def asset_growth(store, *, cik: str, fiscal_key: str, prior_fiscal_key: str,
                 as_of: str) -> dict:
    """Annual percentage change in Assets between two fiscal periods (both read
    as-of ``as_of`` - no forward fill)."""
    a1 = _val(store, cik, "assets", fiscal_key, as_of)
    a0 = _val(store, cik, "assets", prior_fiscal_key, as_of)
    if a1 is None or a0 is None:
        miss = [k for k, v in (("assets@t", a1), ("assets@t-1", a0)) if v is None]
        return {"value": None, "basis": None, "missing": miss}
    if a0 == 0:
        return {"value": None, "basis": None, "missing": ["assets@t-1==0"]}
    return {"value": (a1 - a0) / abs(a0), "basis": "annual_change",
            "missing": []}


def balance_sheet_quality(store, *, cik: str, fiscal_key: str, as_of: str
                          ) -> dict:
    """Leverage = liabilities/assets and equity_ratio = equity/assets. The signal
    value is the equity ratio (higher = stronger balance sheet)."""
    assets = _val(store, cik, "assets", fiscal_key, as_of)
    liab = _val(store, cik, "liabilities", fiscal_key, as_of)
    eq = _val(store, cik, "stockholders_equity", fiscal_key, as_of)
    if assets is None or assets <= 0:
        return {"value": None, "basis": None, "missing": ["assets"],
                "leverage": None, "equity_ratio": None}
    leverage = (liab / assets) if liab is not None else None
    equity_ratio = (eq / assets) if eq is not None else None
    if equity_ratio is None:
        return {"value": None, "basis": None, "missing": ["stockholders_equity"],
                "leverage": leverage, "equity_ratio": None}
    return {"value": equity_ratio, "basis": "equity_over_assets", "missing": [],
            "leverage": leverage, "equity_ratio": equity_ratio}


def compute_signal(store, signal: str, *, cik: str, fiscal_key: str, as_of: str,
                   prior_fiscal_key: Optional[str] = None) -> dict:
    if signal == "gross_profitability":
        return gross_profitability(store, cik=cik, fiscal_key=fiscal_key,
                                   as_of=as_of)
    if signal == "asset_growth":
        return asset_growth(store, cik=cik, fiscal_key=fiscal_key,
                            prior_fiscal_key=prior_fiscal_key or fiscal_key,
                            as_of=as_of)
    if signal == "balance_sheet_quality":
        return balance_sheet_quality(store, cik=cik, fiscal_key=fiscal_key,
                                     as_of=as_of)
    return {"value": None, "basis": None, "missing": ["unknown_signal"]}


def coverage_ready(store, signal: str, *, scored_periods: int,
                   min_ciks: int = 50, min_periods: int = 12) -> dict:
    """Whether owned PIT coverage is sufficient to run the canonical Stage 9
    evidence contract for ``signal``. Below the company/period minimums the
    candidate stays DATA_HOLD (honest) - the contract is never run on thin data.
    Returns ``{sufficient, ciks_with_all_concepts, min_ciks, scored_periods,
    min_periods, missing_concepts, blocker}``."""
    ciks = len(store.ciks_with_candidate(signal))
    missing = store.missing_concepts(signal)
    enough_ciks = ciks >= int(min_ciks)
    enough_periods = int(scored_periods) >= int(min_periods)
    sufficient = enough_ciks and enough_periods and not missing
    blocker = None
    if missing:
        blocker = "DATA_HOLD_MISSING_CONCEPTS"
    elif not enough_ciks:
        blocker = "DATA_HOLD_INSUFFICIENT_UNIVERSE"
    elif not enough_periods:
        blocker = "DATA_HOLD_INSUFFICIENT_OBSERVATIONS"
    return {"sufficient": bool(sufficient),
            "ciks_with_all_concepts": ciks, "min_ciks": int(min_ciks),
            "scored_periods": int(scored_periods), "min_periods": int(min_periods),
            "missing_concepts": missing, "blocker": blocker,
            "expected_sign": EXPECTED_SIGN.get(signal)}


def signal_cross_section(store, signal: str, *, rebalance_points: list) -> dict:
    """Build per-period ``{cik: signal_value}`` cross-sections from PIT
    observations. ``rebalance_points`` is a list of dicts, each
    ``{as_of, fiscal_key, prior_fiscal_key, ciks}``. Values are explicit (None
    dropped). Returns the per-period value maps + the realized scored-period
    count (periods with >= 4 named values)."""
    periods: list[dict] = []
    scored = 0
    for pt in rebalance_points:
        vals: dict[str, float] = {}
        for cik in pt.get("ciks", []):
            r = compute_signal(store, signal, cik=cik,
                               fiscal_key=pt["fiscal_key"], as_of=pt["as_of"],
                               prior_fiscal_key=pt.get("prior_fiscal_key"))
            if r.get("value") is not None:
                vals[cik] = r["value"]
        periods.append({"as_of": pt["as_of"], "values": vals, "n": len(vals)})
        if len(vals) >= 4:
            scored += 1
    return {"signal": signal, "periods": periods, "scored_periods": scored}


__all__ = ["SIGNALS", "EXPECTED_SIGN", "gross_profitability", "asset_growth",
           "balance_sheet_quality", "compute_signal", "coverage_ready",
           "signal_cross_section"]
