r"""R65 panel substrate - the certified futures layer, scoped to the PUBLISHED
R65 universes.

RESEARCH ONLY. PAPER ONLY. Nothing here creates an order, a fill, a promotion
or an adoption, and nothing here decides anything.

Why this module loads someone else's layer instead of building its own
---------------------------------------------------------------------
``campaign_r60_information_frontier.panels.futures_layer`` is already the
place where three things happen exactly once:

    the zero rule          ``open_interest == 0`` becomes NaN AT LOAD, so a
                           zero can never enter a mean or become a denominator
    the unit conversion    the R38 manifest states cost in BASIS POINTS and
                           the frozen futures book charges a RATE on traded
                           notional, so the manifest number is divided by 1e4
                           in one place. Passing bps straight through charges
                           10,000x the real cost and every cell "fails" on a
                           cost that was never real.
    the vocabulary         the manifest spells asset classes "COMMODITY" and
                           "INTERNATIONAL_EQUITY"; the estate spells them
                           "COMMODITY_FUTURES" and "EQUITY_INDEX_FUTURES". An
                           untranslated compare matches nothing and a universe
                           quietly empties.

Re-implementing any of the three here would be a second accounting of the same
data, and the estate has one rule about that. So this module IMPORTS it.

What this module ADDS is the thing R65 has and R60 did not: the universe is
the FROZEN PUBLISHED ARTIFACT, not a rule re-derived at run time. R65's cells
were pre-registered against ``r65_intl_index_oi_covered`` (14 markets) and
``r65_commodity_oi_covered`` (38 markets, AFB excluded at 60.07% coverage
against the 0.80 floor the director froze on 2026-09-22). A mask rebuilt from
``asset_class == COMMODITY`` would silently re-admit AFB and measure a
different cell from the one that was registered.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np

from alpha_agent import r59

# THE certified layer, with the zero rule and the unit conversion already
# applied. Imported, never reimplemented.
from campaign_r60_information_frontier.panels import (  # noqa: E402
    OI_LAG, PRICE_LAG, futures_layer)

CAMPAIGN_ID = "R65_NON_EQUITY_COST_HONEST_FRONTIER"
CAMPAIGN_DIR = Path(__file__).resolve().parent

__all__ = ("CAMPAIGN_ID", "OI_LAG", "PRICE_LAG", "futures_layer",
           "universe", "universe_rows", "oi_coverage", "UniverseMismatch")

_CACHE: dict = {}


class UniverseMismatch(RuntimeError):
    """The published universe names a market the certified layer does not
    carry. That is a substrate defect, never something to quietly drop: a
    silently shrinking universe measures a different cell from the registered
    one."""


def universe(universe_id: str) -> dict:
    """The PUBLISHED universe artifact, as frozen."""
    if universe_id in _CACHE:
        return _CACHE[universe_id]
    path = CAMPAIGN_DIR / ("UNIVERSE_%s.json" % universe_id)
    if not path.exists():
        raise UniverseMismatch("published universe not found: %s" % path)
    body = json.loads(path.read_text(encoding="utf-8-sig"))
    _CACHE[universe_id] = body
    return body


def universe_rows(layer: dict, universe_id: str) -> np.ndarray:
    """Boolean row mask over ``layer['symbols']`` for a published universe.

    Refuses on a name the layer does not carry rather than returning a
    shorter universe than the one that was registered.
    """
    body = universe(universe_id)
    want = [str(r["market"]) for r in body["instruments"]]
    symbols = list(layer["symbols"])
    missing = [m for m in want if m not in symbols]
    if missing:
        raise UniverseMismatch(
            "%s names %d market(s) absent from the certified layer: %s"
            % (universe_id, len(missing), ", ".join(sorted(missing))))
    declared = int(body.get("n_instruments") or 0)
    if declared and declared != len(want):
        raise UniverseMismatch(
            "%s declares n_instruments=%d but lists %d"
            % (universe_id, declared, len(want)))
    keep = set(want)
    return np.array([s in keep for s in symbols], dtype=bool)


def oi_coverage(layer: dict, rows: np.ndarray, *,
                start: str = r59.DISCOVERY_START) -> dict:
    """Usable non-zero open-interest coverage over DECISION-ELIGIBLE
    market-sessions, for the cell-level 0.80 floor.

    Decision-eligible means the market was live that session (the layer's own
    ``own`` mask, ``isfinite(ret)``) from ``start`` onward. Sessions on which a
    market does not trade are not counted against it: a market cannot be
    faulted for having no open interest on a day its exchange was shut.

    The zero rule has already turned ``open_interest == 0`` into NaN upstream,
    so "usable" here is simply "finite and positive".
    """
    dates = np.asarray(layer["dates"])
    t0 = int(np.searchsorted(dates, start))
    own = np.asarray(layer["own"], dtype=bool)[:, t0:]
    oi = np.asarray(layer["open_interest"], dtype=np.float64)[:, t0:]
    sel = np.asarray(rows, dtype=bool)
    own, oi = own[sel], oi[sel]

    usable = own & np.isfinite(oi) & (oi > 0)
    eligible = int(own.sum())
    covered = int(usable.sum())
    per_market_elig = own.sum(axis=1)
    per_market_cov = usable.sum(axis=1)
    with np.errstate(invalid="ignore", divide="ignore"):
        per_market = np.where(per_market_elig > 0,
                              per_market_cov / np.maximum(per_market_elig, 1),
                              np.nan)
    symbols = [s for s, k in zip(layer["symbols"], sel) if k]
    return {
        "coverage": (covered / eligible) if eligible else float("nan"),
        "eligible_market_sessions": eligible,
        "covered_market_sessions": covered,
        "n_markets": int(sel.sum()),
        "start": str(start),
        "per_market": {s: (None if not np.isfinite(v) else round(float(v), 6))
                       for s, v in zip(symbols, per_market)},
        "worst_market": (min(
            ((s, float(v)) for s, v in zip(symbols, per_market)
             if np.isfinite(v)), key=lambda kv: kv[1], default=None)),
    }
