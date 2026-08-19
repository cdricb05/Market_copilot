"""alpha_agent/release30_panel.py - Release 30 point-in-time research panel.

RESEARCH ONLY. This module builds the survivorship-free, point-in-time training
panel behind the Release 30 forward-return forecasting layer. It creates NO
signal authority, NO portfolio target, NO order and NO model promotion; it reads
OWNED data and returns arrays.

Ownership boundaries (nothing here is a second owner):

* The **price / liquidity / membership** source is the OWNED Phase-24 daily panel
  (``russell1000_cp_daily.npz``): Norgate "Russell 1000 Current & Past" with
  delisted securities retained and a per-day PIT membership mask. This module
  does not rebuild it and does not re-derive its adjustment policy.
* The **fundamental** source is the released Stage-24/25 point-in-time reader
  (``alpha_agent.stage24_pit_fundamental.Stage24PitStore``), whose PIT contract
  is the SEC ``filed`` date. This module does not re-implement that contract; it
  calls ``annual_record`` and inherits ``REPORTING_LAG_DAYS``.
* The **symbol -> issuer** map is the released
  ``stage24_pit_fundamental.IdentityBridge``.
* **Sector is deliberately absent from every historical feature.** The canonical
  owner (``alpha_agent.pit_sector``) classifies the owned entity-level SIC
  snapshot as ``ENTITY_SIC_SNAPSHOT_CONTROL`` -> *inadmissible for signal
  construction*. Release 30 obeys that rule: sector enters only the CURRENT-date
  portfolio construction sector cap, where the classification is genuinely known
  at the decision timestamp.

Point-in-time rules enforced here, and asserted by
``tests/test_release30_return_forecast.py``:

1.  Every feature at decision index ``t`` reads only rows at or before ``t``.
2.  Membership is the panel's own PIT mask at ``t`` - never current membership.
3.  Delisted names are retained; a name that stops trading inside a forward
    window is labelled to its LAST OWNED CLOSE, never dropped as if it never
    existed and never carried forward at a stale price.
4.  No cross-sectional statistic is computed over a period that includes the
    session after ``t``. Normalisation is per-date and uses that date's
    cross-section only.
5.  Fundamentals are visible only when FILED by ``t`` minus the released
    reporting lag.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

from . import stage24_pit_fundamental as _s24

RELEASE = "release30"
PANEL_VERSION = "release30_pit_panel.v1"

PRICE_PANEL_ENV = "PAPER_TRADER_R30_PRICE_PANEL"
DEFAULT_PRICE_PANEL = Path(
    r"D:\Stock_Prediction_app_data\phase24_cache\daily_panel"
    r"\russell1000_cp_daily.npz")

#: Trading-session horizons the forecasting layer targets. Declared once; every
#: downstream artifact carries them.
HORIZONS = (5, 20, 60)

#: Decision dates are struck every N trading sessions. 21 is about one month,
#: which is the cadence at which the operational monthly momentum input actually
#: changes, so the research grid never claims more independent decisions than the
#: owned inputs support.
STEP_DAYS = 21

#: Minimum trailing sessions a name needs before any feature is computed.
MIN_HISTORY = 252

#: Cross-sectional minimum. A date with fewer eligible names is not a
#: cross-section and is skipped rather than padded.
MIN_CROSS_SECTION = 50

#: Liquidity floor mirroring the canonical construction constant
#: (``api.multi_horizon_engine.MIN_ADV_DOLLAR``). Declared as a literal so the
#: research lane never imports the operational API package.
MIN_ADV_DOLLAR = 1.0e7

_TRADING_DAYS_YEAR = 252.0


def sha(obj) -> str:
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def file_fingerprint(path: Path) -> dict:
    try:
        st = Path(path).stat()
        return {"path": str(path), "exists": True, "size_bytes": st.st_size,
                "mtime": int(st.st_mtime)}
    except OSError:
        return {"path": str(path), "exists": False}


# --------------------------------------------------------------------------- #
# Price panel
# --------------------------------------------------------------------------- #
@dataclass
class PricePanel:
    """The owned survivorship-free daily panel, loaded once."""

    dates: np.ndarray            # (T,) datetime64[D]
    symbols: np.ndarray          # (N,) unicode
    close: np.ndarray            # (T, N) float64, NaN outside listing
    dvol: np.ndarray             # (T, N) float64
    member: np.ndarray           # (T, N) bool - PIT membership
    source: dict = field(default_factory=dict)

    @property
    def n_dates(self) -> int:
        return int(self.dates.shape[0])

    @property
    def n_symbols(self) -> int:
        return int(self.symbols.shape[0])

    def iso(self, t: int) -> str:
        return str(self.dates[t])[:10]


def load_price_panel(path=None) -> PricePanel:
    p = Path(path or os.environ.get(PRICE_PANEL_ENV) or DEFAULT_PRICE_PANEL)
    z = np.load(p, allow_pickle=False)
    close = z["close"].astype(np.float64)
    close[~(close > 0)] = np.nan
    return PricePanel(dates=z["dates"], symbols=z["symbols"],
                      close=close, dvol=z["dvol"].astype(np.float64),
                      member=z["member"].astype(np.int8) == 1,
                      source=file_fingerprint(p))


# --------------------------------------------------------------------------- #
# Point-in-time price / liquidity / risk features
# --------------------------------------------------------------------------- #
#: (name, direction). ``direction`` is the ECONOMIC prior sign, recorded for
#: reporting only - no model is forced to respect it.
PRICE_FEATURES = (
    ("mom_12_1", +1),
    ("mom_6_1", +1),
    ("mom_3_1", +1),
    ("rev_1m", -1),
    ("trend_200", +1),
    ("high_52w_prox", +1),
    ("vol_63", -1),
    ("vol_252", -1),
    ("downside_vol_63", -1),
    ("max_drawdown_252", +1),
    ("log_adv_20", +1),
    ("adv_trend_20_252", +1),
    ("beta_252", -1),
    ("ivol_63", -1),
)
PRICE_FEATURE_NAMES = tuple(n for n, _ in PRICE_FEATURES)


def _nan_std(a: np.ndarray, axis=0) -> np.ndarray:
    with np.errstate(invalid="ignore"):
        return np.nanstd(a, axis=axis, ddof=1)


def price_features_at(panel: PricePanel, t: int) -> dict:
    """Every price-family feature at decision index ``t``, using rows <= t only.

    The slices below are the whole point-in-time argument: each one ends at
    ``t`` inclusive and none reaches the following session.
    """
    c = panel.close
    px_t = c[t]
    out: dict = {}

    def _rat(num_idx, den_idx):
        with np.errstate(invalid="ignore", divide="ignore"):
            return c[num_idx] / c[den_idx] - 1.0

    out["mom_12_1"] = _rat(t - 21, t - 252)
    out["mom_6_1"] = _rat(t - 21, t - 126)
    out["mom_3_1"] = _rat(t - 21, t - 63)
    with np.errstate(invalid="ignore", divide="ignore"):
        out["rev_1m"] = -(px_t / c[t - 21] - 1.0)

    win200 = c[t - 199:t + 1]
    with np.errstate(invalid="ignore", divide="ignore"):
        out["trend_200"] = px_t / np.nanmean(win200, axis=0) - 1.0

    win252 = c[t - 251:t + 1]
    with np.errstate(invalid="ignore", divide="ignore"):
        out["high_52w_prox"] = px_t / np.nanmax(win252, axis=0)
        run_max = np.fmax.accumulate(
            np.where(np.isfinite(win252), win252, -np.inf), axis=0)
        dd = win252 / np.where(run_max > 0, run_max, np.nan) - 1.0
        out["max_drawdown_252"] = np.nanmin(dd, axis=0)

    with np.errstate(invalid="ignore", divide="ignore"):
        r252 = c[t - 251:t + 1] / c[t - 252:t] - 1.0
    r252[~np.isfinite(r252)] = np.nan
    r63 = r252[-63:]

    out["vol_63"] = _nan_std(r63, axis=0) * math.sqrt(_TRADING_DAYS_YEAR)
    out["vol_252"] = _nan_std(r252, axis=0) * math.sqrt(_TRADING_DAYS_YEAR)
    out["downside_vol_63"] = _nan_std(np.where(r63 < 0, r63, np.nan),
                                      axis=0) * math.sqrt(_TRADING_DAYS_YEAR)

    adv20 = np.nanmean(panel.dvol[t - 19:t + 1], axis=0)
    adv252 = np.nanmean(panel.dvol[t - 251:t + 1], axis=0)
    with np.errstate(invalid="ignore", divide="ignore"):
        out["log_adv_20"] = np.log(np.where(adv20 > 0, adv20, np.nan))
        out["adv_trend_20_252"] = np.log(
            np.where((adv20 > 0) & (adv252 > 0), adv20 / adv252, np.nan))

    # Market factor = equal-weight mean return of the PIT members at t, built
    # from the SAME trailing window, so it carries no forward information.
    mkt = np.nanmean(np.where(panel.member[t][None, :], r252, np.nan), axis=1)
    mk = mkt - np.nanmean(mkt)
    mk_var = float(np.nanmean(mk * mk))
    if mk_var > 0:
        dev = r252 - np.nanmean(r252, axis=0)
        beta = np.nanmean(dev * mk[:, None], axis=0) / mk_var
        out["beta_252"] = beta
        mk63 = mkt[-63:] - np.nanmean(mkt[-63:])
        resid = (r63 - np.nanmean(r63, axis=0)) - beta[None, :] * mk63[:, None]
        out["ivol_63"] = _nan_std(resid, axis=0) * math.sqrt(_TRADING_DAYS_YEAR)
    else:
        out["beta_252"] = np.full(panel.n_symbols, np.nan)
        out["ivol_63"] = np.full(panel.n_symbols, np.nan)

    out["_adv_dollar"] = adv20
    return out


def forward_returns_at(panel: PricePanel, t: int, horizon: int) -> tuple:
    """Forward total return over ``horizon`` sessions, delisting-safe.

    Returns ``(fwd, truncated)``. When a name stops trading inside the window the
    return is measured to its LAST OWNED CLOSE and ``truncated`` is True for that
    name. Dropping such a name instead would quietly re-introduce survivorship
    into the LABEL, which is the failure mode this function exists to prevent.
    """
    n = panel.n_symbols
    end = min(panel.n_dates - 1, t + horizon)
    win = panel.close[t + 1:end + 1]
    if win.shape[0] == 0:
        return np.full(n, np.nan), np.zeros(n, dtype=bool)
    finite = np.isfinite(win)
    idx = np.where(finite, np.arange(win.shape[0])[:, None], -1)
    last = idx.max(axis=0)
    have = last >= 0
    last_px = np.full(n, np.nan)
    cols = np.arange(n)
    last_px[have] = win[last[have], cols[have]]
    with np.errstate(invalid="ignore", divide="ignore"):
        fwd = last_px / panel.close[t] - 1.0
    return fwd, have & (last < (win.shape[0] - 1))


# --------------------------------------------------------------------------- #
# Point-in-time fundamental features
# --------------------------------------------------------------------------- #
#: The fundamental family. ``s25_operating_profitability`` is the Stage-25 frozen
#: research challenger and is carried under its OWN name so its contribution to
#: the Release 30 candidate stays attributable.
FUNDAMENTAL_FEATURES = (
    ("s25_operating_profitability", +1),
    ("gross_profitability", +1),
    ("fcf_to_assets", +1),
    ("operating_accruals", -1),
    ("asset_growth", -1),
    ("roe", +1),
    ("leverage", -1),
)
FUNDAMENTAL_FEATURE_NAMES = tuple(n for n, _ in FUNDAMENTAL_FEATURES)

ALL_FEATURE_NAMES = PRICE_FEATURE_NAMES + FUNDAMENTAL_FEATURE_NAMES

FEATURE_DIRECTION = dict(PRICE_FEATURES + FUNDAMENTAL_FEATURES)


def _ratio(num, den):
    if num is None or den is None:
        return None
    try:
        d = float(den)
        n = float(num)
    except (TypeError, ValueError):
        return None
    if d == 0 or not math.isfinite(d) or not math.isfinite(n):
        return None
    v = n / d
    return v if math.isfinite(v) else None


def fundamental_values(rec: dict) -> dict:
    """The fundamental family for one point-in-time annual record.

    ``rec`` is the released ``stage24_pit_fundamental.annual_record`` shape:
    ``{"cur": {...}, "prior": {...}, "period_end": ...}``. Every input is a fact
    FILED by the as-of date; this function adds arithmetic only.
    """
    cur = rec.get("cur") or {}
    pri = rec.get("prior") or {}
    gp = _s24.gross_profit(cur)
    assets = cur.get("assets")
    sganda = cur.get("sganda")
    cfo = cur.get("cash_flow_operations")
    ni = cur.get("net_income")
    op_surplus = None if (gp is None or sganda is None) else gp - float(sganda)
    fcf = (None if cfo is None
           else float(cfo) - float(cur.get("capital_expenditure") or 0.0))
    accr = None if (ni is None or cfo is None) else float(ni) - float(cfo)
    growth = _ratio(assets, pri.get("assets"))
    return {
        "s25_operating_profitability": _ratio(op_surplus, assets),
        "gross_profitability": _ratio(gp, assets),
        "fcf_to_assets": _ratio(fcf, assets),
        "operating_accruals": _ratio(accr, assets),
        "asset_growth": None if growth is None else growth - 1.0,
        "roe": _ratio(ni, cur.get("stockholders_equity")),
        "leverage": _ratio(cur.get("liabilities"), assets),
    }
