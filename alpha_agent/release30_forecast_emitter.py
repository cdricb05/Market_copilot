"""alpha_agent/release30_forecast_emitter.py - the Release 30 forecast-input bridge.

RESEARCH ONLY. Emits the CURRENT decision-date feature cross-section that the
pure-stdlib operational owner ``api.return_forecast`` validates and applies.

This is deliberately the same shape as the Phase-29D.2 monthly momentum bridge:
the numpy side computes, the stdlib side validates and promotes. The emitter has
no authority - it produces a candidate input document and nothing else. It never
scores, never ranks, never allocates, never promotes and never writes to an
operational store.

**Staleness is declared, never hidden.** The owned Phase-24 daily panel is a
periodic build, so its last session can sit behind the operational eligible
market date. The emitter refuses to extrapolate: it stamps the cross-section
with the panel session it actually used, records the requested eligible date
beside it, and lets the operational owner decide what a gap means. Silently
labelling stale features with today's date would be the one failure mode that
makes every downstream number untrustworthy.
"""
from __future__ import annotations

import json
import warnings
from datetime import date
from pathlib import Path
from typing import Optional

import numpy as np

from . import release30_panel as rp

INPUT_SCHEMA_VERSION = "return_forecast.input.v1"
EMITTER_OWNER = "alpha_agent.release30_forecast_emitter"
EMITTER_VERSION = "release30_forecast_emitter.v1"

PIT_OK = "POINT_IN_TIME_OK"

POINT_IN_TIME_CONTROLS = [
    "every feature reads owned panel rows at or before the stamped panel session",
    "membership is the panel's own point-in-time mask, never current membership",
    "no forward window is read - this cross-section has no label",
    "cross-sectional normalisation happens in the operational owner, per date",
    "sector is not a feature; it is carried only for the construction sector cap",
]


def _panel_index_at_or_before(panel: rp.PricePanel, as_of: str) -> Optional[int]:
    target = np.datetime64(str(as_of)[:10], "D")
    idx = int(np.searchsorted(panel.dates, target, side="right")) - 1
    return idx if idx >= rp.MIN_HISTORY else None


def emit_cross_section(*, as_of_date: str, tickers=None, sectors=None,
                       panel: Optional[rp.PricePanel] = None,
                       min_adv_dollar: float = rp.MIN_ADV_DOLLAR) -> dict:
    """Build the forecast-input cross-section for one decision date.

    ``tickers`` restricts the emission to the operational eligible universe. That
    universe is decided by ``api.universe_scoring`` - the emitter does not decide
    who is eligible, it only computes features for whoever is.
    """
    panel = panel or rp.load_price_panel()
    t = _panel_index_at_or_before(panel, as_of_date)
    if t is None:
        return {
            "input_schema_version": INPUT_SCHEMA_VERSION,
            "as_of_date": None,
            "requested_eligible_market_date": str(as_of_date),
            "rows": [],
            "feature_names": list(rp.PRICE_FEATURE_NAMES),
            "point_in_time_status": PIT_OK,
            "blockers": [{"code": "NO_PANEL_SESSION_AT_OR_BEFORE_REQUESTED_DATE"}],
            "provenance": {"emitter": EMITTER_OWNER,
                           "panel_source": dict(panel.source)},
        }

    panel_date = panel.iso(t)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        f = rp.price_features_at(panel, t)
    adv = f["_adv_dollar"]
    want = None if tickers is None else {str(x) for x in tickers}
    sector_map = dict(sectors or {})

    rows = []
    excluded: dict = {}
    for c in range(panel.n_symbols):
        sym = str(panel.symbols[c])
        if want is not None and sym not in want:
            continue
        if not panel.member[t, c]:
            excluded[sym] = "NOT_A_POINT_IN_TIME_MEMBER"
            continue
        a = adv[c]
        if not np.isfinite(a) or a < min_adv_dollar:
            excluded[sym] = "BELOW_LIQUIDITY_FLOOR"
            continue
        feats = {}
        missing = []
        for n in rp.PRICE_FEATURE_NAMES:
            v = f[n][c]
            if np.isfinite(v):
                feats[n] = float(v)
            else:
                missing.append(n)
        if missing:
            excluded[sym] = "INCOMPLETE_FEATURES"
            continue
        rows.append({"ticker": sym, "features": feats,
                     "adv_dollar": float(a),
                     "sector": sector_map.get(sym) or "Unknown"})
    rows.sort(key=lambda r: r["ticker"])

    unresolved = sorted((want or set()) - {r["ticker"] for r in rows}
                        - set(excluded)) if want is not None else []
    gap_days = None
    try:
        gap_days = (date.fromisoformat(str(as_of_date)[:10])
                    - date.fromisoformat(panel_date)).days
    except ValueError:
        pass

    return {
        "input_schema_version": INPUT_SCHEMA_VERSION,
        "emitter": EMITTER_OWNER,
        "emitter_version": EMITTER_VERSION,
        "as_of_date": panel_date,
        "requested_eligible_market_date": str(as_of_date)[:10],
        "feature_panel_behind_eligible_session": bool(
            panel_date != str(as_of_date)[:10]),
        "feature_panel_gap_calendar_days": gap_days,
        "feature_names": list(rp.PRICE_FEATURE_NAMES),
        "rows": rows,
        "row_count": len(rows),
        "excluded": dict(sorted(excluded.items())),
        "unresolved_requested_tickers": unresolved,
        "point_in_time_status": PIT_OK,
        "point_in_time_controls": list(POINT_IN_TIME_CONTROLS),
        "provenance": {
            "emitter": EMITTER_OWNER,
            "emitter_version": EMITTER_VERSION,
            "panel_version": rp.PANEL_VERSION,
            "panel_source": dict(panel.source),
            "panel_session_index": int(t),
            "min_adv_dollar": float(min_adv_dollar),
            "universe_decided_by": "api.universe_scoring",
        },
    }


def aligned_returns(*, as_of_date: str, tickers, lookback: int = 90,
                    panel: Optional[rp.PricePanel] = None) -> dict:
    """Trailing daily returns for the canonical covariance builder.

    Same shape the Slice-6 risk owner already consumes
    (``{"dates": [...], "series": {ticker: [...]}}``), so the allocator gets its
    covariance from that owner rather than from a second implementation here.
    """
    panel = panel or rp.load_price_panel()
    t = _panel_index_at_or_before(panel, as_of_date)
    if t is None:
        return {"dates": [], "series": {}}
    lo = max(1, t - int(lookback) + 1)
    dates = [panel.iso(i) for i in range(lo, t + 1)]
    want = {str(x) for x in tickers}
    series: dict = {}
    for c in range(panel.n_symbols):
        sym = str(panel.symbols[c])
        if sym not in want:
            continue
        col = panel.close[lo - 1:t + 1, c]
        vals = []
        for k in range(1, col.shape[0]):
            prev, cur = col[k - 1], col[k]
            vals.append(float(cur / prev - 1.0)
                        if (np.isfinite(prev) and np.isfinite(cur) and prev > 0)
                        else None)
        if any(v is not None for v in vals):
            series[sym] = vals
    return {"dates": dates, "series": series}


def write_emission(payload: dict, path) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=1, default=str), encoding="utf-8")
    tmp.replace(p)
    return p
