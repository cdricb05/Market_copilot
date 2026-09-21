r"""alpha_agent.r61.calibration_panels - the FIVE real panels (Workstream B).

RESEARCH ONLY. Loads owned historical substrates read-only. Builds no
universe, certifies no dataset and registers no hypothesis.

These are the panels the estate actually tests on, loaded through their
existing owners and wrapped in nothing. The calibration measures the real
apparatus or it measures nothing:

    US_LARGE_CAP            sp500_pit_panel_v1, the substrate 1,858 equity
                            tests were run on. EQUITY_TOPN, 12.5 bp/side.
    EXTENSION_EQUITY        the R60 mid/small extension universe - 2,947
                            names, ~1,000 tradable per decision, delisted
                            RETAINED. EQUITY_TOPN, 25 bp/side.
    COMMODITY_FUTURES       the certified R38 dated-contract layer, commodity
                            markets. The panel basis momentum died on.
    EQUITY_INDEX_FUTURES    the same layer, equity-index markets.
    CROSS_ASSET_FUTURES     rates + FX + equity index - R60's DISJOINT
                            replication set, which shares no market with the
                            commodity panel.

Each panel carries the cost model its own campaign froze, because a detection
floor measured under a cost the estate does not charge would answer a question
nobody asked.

UNIVERSE RULES ARE RE-DERIVED, NOT IMPORTED FROM A PRIVATE. ``base_live``
below restates R60's live-market rule; ``tests/test_release61_*`` proves it
equals ``campaign_r60_information_frontier.executors._base_live`` decision for
decision on the real layer. That is stronger evidence than the import would
have been, and it keeps a private out of a production path.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Callable, Optional

import numpy as np

from .. import r59
from ..agents_v2 import books as B
from ..r57 import engine as K
from ..r59 import engines as E

PANELS_OWNER = "alpha_agent.r61.calibration_panels"

#: Panel ids, as the final table names them.
US_LARGE_CAP = "US_LARGE_CAP"
EXTENSION_EQUITY = "EXTENSION_EQUITY"
COMMODITY_FUTURES = "COMMODITY_FUTURES"
EQUITY_INDEX_FUTURES = "EQUITY_INDEX_FUTURES"
CROSS_ASSET_FUTURES = "CROSS_ASSET_FUTURES"
PANEL_IDS = (US_LARGE_CAP, EXTENSION_EQUITY, COMMODITY_FUTURES,
             EQUITY_INDEX_FUTURES, CROSS_ASSET_FUTURES)

#: R60's live-market rule, restated. A market is live, certified, and has
#: traded at least 227 of its OWN sessions in the trailing 252. Counting grid
#: slots instead of own sessions is the documented R56 trap.
LIVE_MIN_OWN = 227
LIVE_WINDOW = 252

_CACHE: dict = {}


class PanelRefusal(RuntimeError):
    """A calibration panel could not be loaded as specified."""


def _r60_panels():
    """The R60 campaign's panel owner, imported BY PATH as the runner does."""
    if "pn" in _CACHE:
        return _CACHE["pn"]
    root = Path(__file__).resolve().parents[2] / "research" / "agents"
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    from campaign_r60_information_frontier import panels as PN  # noqa: PLC0415
    _CACHE["pn"] = PN
    return PN


def base_live(layer: dict, t: int, live) -> np.ndarray:
    """Live, certified, and traded >= 227 of its own last 252 sessions."""
    m = np.asarray(live, bool) & np.asarray(layer["certified"], bool)
    own = layer["own"]
    lo, hi = max(0, int(t) - LIVE_WINDOW), max(0, int(t))
    return m & (own[:, lo:hi].sum(axis=1) >= LIVE_MIN_OWN)


# --------------------------------------------------------------------------- #
# The five specifications
# --------------------------------------------------------------------------- #
def _equity_large_cap() -> dict:
    panel = E.load_equity_panel()
    return {
        "panel_id": US_LARGE_CAP,
        "book": B.BOOK_EQUITY_TOPN,
        "panel": panel,
        "asset_class": r59.AC_US_EQUITY,
        "book_kwargs": {"elig": K.eligibility, "top_n": K.EQ_TOP_N,
                        "cost_rate": r59.EQ_COST_RATE_PER_SIDE},
        "cost_model": {"cost_model_id": "US_EQUITY_12_5BP_V1",
                       "rate_per_side": r59.EQ_COST_RATE_PER_SIDE,
                       "basis": "traded notional",
                       "owner": "alpha_agent.r59.EQ_COST_RATE_PER_SIDE"},
        "cost_per_side": float(r59.EQ_COST_RATE_PER_SIDE),
        "rebalance_interval_sessions": r59.CADENCE,
        "instrument_count": int(panel["tr"].shape[0]),
        "source": "R57 sp500_pit_panel_v1 (survivorship-safe, PIT membership)",
    }


def _equity_extension() -> dict:
    PN = _r60_panels()
    panel = PN.equity_panel()
    return {
        "panel_id": EXTENSION_EQUITY,
        "book": B.BOOK_EQUITY_TOPN,
        "panel": panel,
        "asset_class": r59.AC_US_EQUITY,
        "book_kwargs": {"elig": PN.extension_eligible, "top_n": 100,
                        "cost_rate": B.EQ_EXT_COST_RATE},
        "cost_model": {"cost_model_id": "EQ_EXT_25BP_V1",
                       "rate_per_side": float(B.EQ_EXT_COST_RATE),
                       "basis": "traded notional",
                       "owner": "EQ_EXT_COST_RATE in the frozen R60 evaluator"},
        "cost_per_side": float(B.EQ_EXT_COST_RATE),
        "rebalance_interval_sessions": r59.CADENCE,
        "instrument_count": int(panel["tr"].shape[0]),
        "source": ("R60 us_equity_extension_pit_panel_v1 "
                   "(S&P MidCap 400 + SmallCap 600, delisted retained)"),
    }


def _futures(panel_id: str, rows_fn: Callable, *, min_markets: int) -> dict:
    PN = _r60_panels()
    layer = PN.futures_layer()
    rows = np.asarray(rows_fn(layer), dtype=bool)
    if int(rows.sum()) < min_markets:
        raise PanelRefusal(
            "%s has %d markets, below its own floor of %d"
            % (panel_id, int(rows.sum()), min_markets))
    costs = np.asarray(layer["cost_per_side"], dtype=np.float64)[rows]
    finite = costs[np.isfinite(costs)]
    return {
        "panel_id": panel_id,
        "book": B.BOOK_FUTURES,
        "panel": layer,
        "asset_class": (r59.AC_COMMODITY if panel_id == COMMODITY_FUTURES
                        else r59.AC_EQUITY_INDEX
                        if panel_id == EQUITY_INDEX_FUTURES
                        else r59.AC_CROSS_ASSET),
        "rows": rows,
        "min_markets": int(min_markets),
        "book_kwargs": {"min_markets": int(min_markets)},
        "cost_model": {"cost_model_id": "FUTURES_PER_MARKET_R38_PLUS_ROLL_V1",
                       "rate_per_side": "per market, 2-15 bp",
                       "basis": "traded notional", "roll_cost": "charged",
                       "owner": "R38 native_contract_layer cost_bps_per_side"},
        # A per-market vector has no scalar rate, so the equal-weight mean over
        # the panel's own markets is reported as the effective rate. It is
        # LABELLED as derived, because the cost budget must never silently
        # invent a scalar where the model states a vector.
        "cost_per_side": (float(finite.mean()) if finite.size else None),
        "cost_per_side_basis": "EQUAL_WEIGHT_MEAN_OVER_PANEL_MARKETS",
        "rebalance_interval_sessions": r59.CADENCE,
        "instrument_count": int(rows.sum()),
        "source": "R38 certified dated-contract layer (zero OI masked at load)",
    }


def _commodity() -> dict:
    PN = _r60_panels()
    return _futures(COMMODITY_FUTURES, PN.commodity_rows, min_markets=12)


def _equity_index_rows(layer: dict) -> np.ndarray:
    return np.array([a == r59.AC_EQUITY_INDEX for a in layer["asset_class"]])


def _equity_index() -> dict:
    return _futures(EQUITY_INDEX_FUTURES, _equity_index_rows, min_markets=8)


def _cross_asset_rows(layer: dict) -> np.ndarray:
    """R60's DISJOINT set: rates, FX and equity index. Shares no market with
    the commodity panel, which is what made it a replication and not a
    re-test."""
    keep = (r59.AC_RATES, r59.AC_FX, r59.AC_EQUITY_INDEX)
    return np.array([a in keep for a in layer["asset_class"]])


def _cross_asset() -> dict:
    return _futures(CROSS_ASSET_FUTURES, _cross_asset_rows, min_markets=12)


_BUILDERS = {
    US_LARGE_CAP: _equity_large_cap,
    EXTENSION_EQUITY: _equity_extension,
    COMMODITY_FUTURES: _commodity,
    EQUITY_INDEX_FUTURES: _equity_index,
    CROSS_ASSET_FUTURES: _cross_asset,
}


def load(panel_id: str) -> dict:
    """Load ONE calibration panel. Cached; the substrates are large."""
    if panel_id not in _BUILDERS:
        raise PanelRefusal("unknown panel %r; the panels are %s"
                           % (panel_id, list(PANEL_IDS)))
    key = "panel:%s" % panel_id
    if key not in _CACHE:
        _CACHE[key] = _BUILDERS[panel_id]()
    return _CACHE[key]


def describe(panel_id: str) -> dict:
    """The panel's identity, WITHOUT the arrays. Safe to put in an artifact."""
    p = load(panel_id)
    return {k: v for k, v in p.items()
            if k not in ("panel", "rows", "book_kwargs")}


def eligible_at(spec: dict, t: int, live=None) -> np.ndarray:
    """The instruments this panel's own universe rule admits at decision t."""
    panel = spec["panel"]
    if spec["book"] == B.BOOK_FUTURES:
        if live is None:
            live = B.live_markets(panel, int(t))
        return base_live(panel, int(t), live) & spec["rows"]
    elig = spec["book_kwargs"]["elig"]
    return np.asarray(elig(panel, int(t)), dtype=bool)
