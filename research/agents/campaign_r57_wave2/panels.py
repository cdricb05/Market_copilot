r"""campaign_r57_wave2.panels - the certified substrates ALPHA WAVE 2 reads.

RESEARCH ONLY. PAPER ONLY. NO ORDERS. This module loads owned data and builds
point-in-time universe masks. It owns no book, no gate, no registry and no
forward clock.

Four substrates, all already certified by earlier releases and re-certified for
this campaign by the data-foundation-agent:

    r38_native_contract_layer_v4   68 manifest-listed, hash-verified markets on
                                   ONE union-of-15-exchange session grid, with
                                   ret / ret2 / slope / open_interest / volume
                                   / roll. Loaded through the R57-repaired
                                   ``alpha_agent.r59.native.load_layer`` (the
                                   orphan FGBL.csv can no longer enter).
    r38_aggregate_oi_v1            whole-curve open interest, reconciled to
                                   CFTC COT. MEASURED coverage: 100% of
                                   commodity own sessions, 0% elsewhere - so
                                   only the three commodity cells may use it,
                                   and the cross-asset churn cell says in its
                                   own pre-registration that it uses the FRONT
                                   contract's open interest instead.
    sp500_pit_panel_v1             point-in-time S&P 500 membership and total
                                   returns.
    r58_pit_fundamental_panel_v1   PANEL-F: SEC XBRL facts stamped by FILING
                                   AVAILABILITY, on the SAME 181-slot decision
                                   grid as the equity book (verified, not
                                   assumed: the two index arrays are equal).

TIMING, stated once and obeyed everywhere below. Index s is session s; nothing
is forward filled. A decision at t may read:

    price / return / slope     through t-1   (SIGNAL_LAG_SESSIONS = 1)
    volume / open interest     through t-2   (provisional on the newest row;
                                              OI is published one session late)
    PANEL-F                    slot j of t   (already stamped by the filing
                                              available at that decision date)

A market that did not trade on a session is NaN, never a forward fill, so every
window below counts a market's OWN sessions rather than grid slots.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Optional

import numpy as np

from alpha_agent import r59
from alpha_agent.r57 import engine as K
from alpha_agent.r59 import engines as E
from alpha_agent.r59 import native

CAMPAIGN_ID = "R57_ALPHA_WAVE2"

#: Datasets, named as the pre-registrations name them.
DS_R38 = "r38_native_contract_layer_v4"
DS_AGG_OI = "r38_aggregate_oi_v1"
DS_SP500 = "sp500_pit_panel_v1"
DS_PANEL_F = "r58_pit_fundamental_panel_v1"

#: Lags, from the R38 build's own documented publication behaviour.
PRICE_LAG = 1
OI_LAG = 2

#: Universe ids.
U_COMMODITY_OI = "w2_commodity_wholecurve_oi"
U_COMMODITY_SLOPE = "w2_commodity_slope_covered"
U_ALL68 = "w2_all_certified_traded"
U_NONFX = "w2_nonfx_dollar_exposed"
U_INTL_INDEX = "w2_intl_index_futures"
U_SP500_FUNDAMENTAL = "w2_sp500_pit_fundamental"

_FROZEN_R56 = (Path(__file__).resolve().parents[1] / "campaign_r56_v2")
_CACHE: dict = {}


def _frozen(name: str):
    """Load a FROZEN R56 data module by path. Reused, never edited, never
    copied - the aggregate-OI reader and its COT reconciliation are already
    certified and a second copy would be a second dataset."""
    key = "frozen:%s" % name
    if key in _CACHE:
        return _CACHE[key]
    p = _FROZEN_R56 / ("%s.py" % name)
    spec = importlib.util.spec_from_file_location("r56_%s" % name, p)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["r56_%s" % name] = mod
    spec.loader.exec_module(mod)
    _CACHE[key] = mod
    return mod


# --------------------------------------------------------------------------- #
# Substrate 1-2: the dated-contract layer and whole-curve open interest
# --------------------------------------------------------------------------- #
def futures_layer() -> dict:
    """The certified layer, with whole-curve OI attached where it exists.

    Added keys, all aligned to ``[market, session]``:

    ``oi_aggregate``  whole-curve open interest (NaN outside commodities)
    ``own``           bool - the market had a session that date
    ``own_cum``       cumulative own-session count, for O(1) window counts
    """
    if "layer" in _CACHE:
        return _CACHE["layer"]
    d38 = _frozen("data_r38")
    layer = d38.load_certified_layer(verify=True)
    agg = d38.load_aggregate_oi(layer, verify=True)
    layer["oi_aggregate"] = agg["oi_aggregate"]
    layer["oi_manifest"] = agg.get("manifest")
    own = d38.own_sessions(layer)
    layer["own"] = own
    cum = np.zeros((own.shape[0], own.shape[1] + 1), dtype=np.int32)
    np.cumsum(own, axis=1, out=cum[:, 1:])
    layer["own_cum"] = cum
    _CACHE["layer"] = layer
    return layer


def own_in_window(layer: dict, lo: int, hi: int) -> np.ndarray:
    """How many OWN sessions each market had in the grid slots [lo, hi)."""
    cum = layer["own_cum"]
    lo = max(0, int(lo))
    hi = max(lo, min(int(hi), cum.shape[1] - 1))
    return (cum[:, hi] - cum[:, lo]).astype(np.int64)


def _finite_share(arr: np.ndarray, own: np.ndarray, lo: int, hi: int
                  ) -> np.ndarray:
    """Share of a market's OWN sessions in [lo, hi) on which ``arr`` is finite.

    Counting grid slots instead of own sessions is the trap R56 documented: a
    complete US series fails a ">= 90% of 21 slots" test whenever three US
    holidays fall in the window.
    """
    lo, hi = max(0, int(lo)), max(0, int(hi))
    if hi <= lo:
        return np.zeros(arr.shape[0])
    o = own[:, lo:hi]
    n = o.sum(axis=1).astype(np.float64)
    f = (np.isfinite(arr[:, lo:hi]) & o).sum(axis=1).astype(np.float64)
    return np.where(n > 0, f / np.maximum(n, 1.0), 0.0)


# -- the point-in-time universe rules, one function each ------------------- #
def _base_live(layer: dict, t: int, live: np.ndarray, *,
               min_own: int = 227, window: int = 252) -> np.ndarray:
    """Certified, live at t, and traded on >= ``min_own`` of its own sessions
    in the ``window`` grid slots ending t-1. The R56 U1 rule, unchanged."""
    m = np.asarray(live, dtype=bool) & np.asarray(layer["certified"], bool)
    return m & (own_in_window(layer, t - window, t) >= int(min_own))


def universe_commodity_oi(layer: dict, t: int, live: np.ndarray) -> np.ndarray:
    """Commodities with whole-curve OI on >= 9/10 of own sessions in
    [t-274, t-2]. The R56 U2 rule, reused unchanged."""
    m = _base_live(layer, t, live)
    m &= np.array([a == r59.AC_COMMODITY for a in layer["asset_class"]])
    share = _finite_share(layer["oi_aggregate"], layer["own"],
                          t - 274, t - OI_LAG + 1)
    return m & (share >= 0.9)


def universe_commodity_slope(layer: dict, t: int,
                             live: np.ndarray) -> np.ndarray:
    """Commodities with a finite term-structure slope on >= 40 of the 63 grid
    slots ending t-2."""
    m = _base_live(layer, t, live)
    m &= np.array([a == r59.AC_COMMODITY for a in layer["asset_class"]])
    lo, hi = t - 65, t - PRICE_LAG
    cov = (np.isfinite(layer["slope"][:, max(0, lo):max(0, hi)])
           ).sum(axis=1)
    return m & (cov >= 40)


def universe_all_traded(layer: dict, t: int, live: np.ndarray) -> np.ndarray:
    """All 68 certified markets that actually traded: volume > 0 on >= 1/2 of
    own sessions in [t-253, t-2]."""
    m = _base_live(layer, t, live)
    vol = layer["volume"]
    lo, hi = max(0, t - 253), max(0, t - OI_LAG + 1)
    o = layer["own"][:, lo:hi]
    n = o.sum(axis=1).astype(np.float64)
    traded = ((vol[:, lo:hi] > 0) & np.isfinite(vol[:, lo:hi]) & o
              ).sum(axis=1).astype(np.float64)
    return m & (np.where(n > 0, traded / np.maximum(n, 1.0), 0.0) >= 0.5)


def universe_nonfx(layer: dict, t: int, live: np.ndarray) -> np.ndarray:
    """The 60 certified NON-FX markets. The 8 FX markets DEFINE the dollar
    factor and are never scored, so no market can predict itself."""
    m = _base_live(layer, t, live)
    return m & np.array([a != r59.AC_FX for a in layer["asset_class"]])


def universe_intl_index(layer: dict, t: int, live: np.ndarray) -> np.ndarray:
    """The 14 international index-futures markets, >= 200 own sessions in the
    252 slots ending t-1."""
    m = np.asarray(live, bool) & np.asarray(layer["certified"], bool)
    m &= (own_in_window(layer, t - 252, t) >= 200)
    groups = ("INTL_INDEX_FUTURES", "INTL_INDEX_FUTURES_EMERGING")
    return m & np.array([g in groups for g in layer["economic_group"]])


def fx_factor_rows(layer: dict) -> np.ndarray:
    return np.array([a == r59.AC_FX for a in layer["asset_class"]])


# --------------------------------------------------------------------------- #
# Substrate 3-4: the S&P 500 panel and PANEL-F
# --------------------------------------------------------------------------- #
def equity_panel() -> dict:
    """``sp500_pit_panel_v1`` with PANEL-F's cube attached by SESSION INDEX.

    ``pf_cube``     [n_names, n_slots, n_features]
    ``pf_slot``     {session index -> PANEL-F slot}
    ``pf_f_ix``     feature name -> cube column

    The two decision grids are VERIFIED equal here rather than assumed: a
    fundamental read at the wrong slot is a look-ahead that no later test
    would catch.
    """
    if "equity" in _CACHE:
        return _CACHE["equity"]
    from alpha_agent.r58 import panel_f as PF

    panel = dict(E.load_equity_panel())
    pf = PF.load()
    if list(pf["price"]["symbols"]) != list(panel["symbols"]) or \
            list(pf["price"]["dates"]) != list(panel["dates"]):
        raise RuntimeError("PANEL-F is not aligned to sp500_pit_panel_v1")
    book_idx = K.decision_indices(panel["dates"], r59.CADENCE,
                                  r59.DISCOVERY_START, r59.HORIZON)
    if list(np.asarray(pf["dec"]).tolist()) != list(book_idx.tolist()):
        raise RuntimeError(
            "PANEL-F decision grid differs from the book's: %d vs %d slots"
            % (len(pf["dec"]), len(book_idx)))
    panel["pf_cube"] = pf["cube"]
    panel["pf_slot"] = {int(t): j for j, t in enumerate(pf["dec"])}
    panel["pf_f_ix"] = dict(pf["f_ix"])
    panel["pf_meta"] = pf["meta"]
    _CACHE["equity"] = panel
    return panel


#: Quarterly stride and sample count of the four-year fundamental window.
FUND_STRIDE = 3
FUND_SAMPLES = 16
FUND_MIN_FINITE = 10


def fundamental_samples(panel: dict, t: int, name: str = "opinc_to_assets"
                        ) -> Optional[np.ndarray]:
    """The 16 quarterly samples ending at t's own slot, OLDEST FIRST.

    Every sample is the value PANEL-F stamped at ITS OWN decision date, so the
    series carries no restatement and no look-ahead: sample k is what was
    filed and available k quarters ago, not what the company later said about
    that quarter.
    """
    j = panel["pf_slot"].get(int(t))
    if j is None:
        return None
    lo = j - FUND_STRIDE * (FUND_SAMPLES - 1)
    if lo < 0:
        return None
    cols = np.arange(lo, j + 1, FUND_STRIDE)
    return panel["pf_cube"][:, cols, panel["pf_f_ix"][name]]


def fundamental_first_date(panel: dict) -> str:
    """The first decision date at which the FOUR-YEAR window exists at all.

    PANEL-F begins at ``DISCOVERY_START``, so the first 45 slots have no
    sixteen-quarter history and NO security can be scored on them. Running the
    book from the grid's start would not measure a weak discovery - it would
    fill 45 of the 77 discovery decisions with a book that holds nothing and
    average a real effect toward zero. The sample therefore begins where the
    hypothesis can first be asked, and that fact is declared in the
    pre-registration rather than discovered afterwards.
    """
    slots = sorted(panel["pf_slot"].items(), key=lambda kv: kv[1])
    need = FUND_STRIDE * (FUND_SAMPLES - 1)
    for t, j in slots:
        if j >= need:
            return str(panel["dates"][t])
    raise RuntimeError("PANEL-F is shorter than the four-year window")


def sp500_eligible(panel: dict, t: int) -> np.ndarray:
    """An S&P 500 member at t, under the panel's OWN point-in-time membership
    mask, with a finite total-return price at t and at t+1 (the entry)."""
    tr = panel["tr"]
    mem = panel["mem"][:, t] > 0
    ok = np.isfinite(tr[:, t]) & (tr[:, t] > 0)
    if t + 1 < tr.shape[1]:
        ok &= np.isfinite(tr[:, t + 1])
    return mem & ok
