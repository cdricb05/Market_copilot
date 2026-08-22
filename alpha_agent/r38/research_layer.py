"""alpha_agent.r38.research_layer - Phase 4: ONE dated-contract research
representation.

Economic return for every native futures strategy in this release is computed
from DATED contracts under the ONE frozen, observable roll policy declared in
:mod:`alpha_agent.r38.contract`:

* exit the held contract at the EARLIER of (first notice - 2 business days)
  and (scheduled last quoted day - 5 business days) - both dates are
  exchange-published schedule metadata, observable long before they arrive;
* the next contract is the next delivery month; the switch happens after the
  exit day's settlement; the first return earned on the new contract is
  computed against ITS OWN prior settlement, so no artificial roll jump is
  ever booked as PnL;
* no hindsight roll, no roll-rule search, no vendor continuous series in any
  economic path (``&``-symbols may appear only as clearly labelled derived
  features, and this module never reads them).

The layer persists one CSV per market (daily front-roll excess return, the
held contract identity per date, the annualised front-to-second calendar
slope, front volume and open interest) plus a checksum, so every downstream
phase - experiments, coverage closure, the ML panel - reads identical bytes.
"""
from __future__ import annotations

import datetime as _dt
import hashlib as _hashlib
import json as _json
import math
from pathlib import Path
from typing import Callable, Optional

from .. import r38
from . import contract as C
from . import enumeration as EN

CALCULATION_OWNER = "alpha_agent.r38.research_layer"
LAYER_DIR = "native_contract_layer"
LAYER_SCHEMA = "r38_native_contract_layer/1"

#: A period return at the experiment cadence requires at least this share of
#: business days observed inside the window; otherwise the period is NaN.
MIN_PERIOD_COVERAGE = 0.6


def _nd():
    import norgatedata as nd
    return nd


def layer_dir(campaign_id: str = C.CAMPAIGN_ID) -> Path:
    return r38.campaign_dir(campaign_id) / LAYER_DIR


def roll_exit_date(first_notice, last_quoted):
    """The frozen observable roll-exit day for one contract, or None."""
    import pandas as pd

    if last_quoted is None:
        return None
    lq = pd.Timestamp(str(last_quoted)[:10])
    exit_day = lq - pd.tseries.offsets.BDay(C.ROLL_LAST_TRADE_BUFFER_SESSIONS)
    if first_notice is not None:
        fn = (pd.Timestamp(str(first_notice)[:10])
              - pd.tseries.offsets.BDay(C.ROLL_FIRST_NOTICE_BUFFER_SESSIONS))
        exit_day = min(exit_day, fn)
    return exit_day


def build_market_series(market: str, contract_symbols: list):
    """Daily front-roll series for one market from its dated contracts.

    Returns a pandas DataFrame indexed by date with columns
    ``ret`` (daily excess return of the held contract), ``held`` (contract
    symbol - the explicit, hindsight-free roll record), ``close``, ``slope_ann``
    (annualised log(front settle / second settle)), ``volume`` and
    ``open_interest`` - or None when fewer than two contracts are usable.
    """
    import numpy as np
    import pandas as pd

    nd = _nd()
    parsed = sorted(
        (p for p in (EN.parse_contract_symbol(s) for s in contract_symbols)
         if p["parsed"]),
        key=lambda p: (p["delivery_year"], p["delivery_month"]))

    contracts = []
    for p in parsed:
        sym = p["symbol"]
        try:
            lq = nd.last_quoted_date(sym)
        except Exception:
            lq = None
        try:
            fn = nd.first_notice_date(sym)
        except Exception:
            fn = None
        exit_day = roll_exit_date(fn, lq)
        if exit_day is None:
            continue
        contracts.append({"symbol": sym, "exit": exit_day,
                          "scheduled_last_quoted": str(lq)[:10] if lq else None})
    # A later delivery month whose exit precedes the previous contract's exit
    # would create a negative holding window; enforce monotone exits.
    monotone = []
    for row in contracts:
        if monotone and row["exit"] <= monotone[-1]["exit"]:
            continue
        monotone.append(row)
    contracts = monotone
    if len(contracts) < 2:
        return None

    closes = {}
    frames = {}
    for row in contracts:
        sym = row["symbol"]
        try:
            df = nd.price_timeseries(sym, timeseriesformat="pandas-dataframe")
        except Exception:
            df = None
        if df is None or not len(df):
            continue
        df = df[~df.index.duplicated(keep="last")].sort_index()
        closes[sym] = df["Close"].astype(float)
        frames[sym] = df
    contracts = [c for c in contracts if c["symbol"] in closes]
    if len(contracts) < 2:
        return None

    pieces = []
    prev_exit = None
    for i, row in enumerate(contracts):
        sym = row["symbol"]
        px = closes[sym]
        start = prev_exit  # exclusive
        end = row["exit"]  # inclusive
        window = px.loc[(px.index > start) if start is not None
                        else slice(None)] if start is not None else px
        window = window.loc[window.index <= end]
        if not len(window):
            prev_exit = end
            continue
        ret = px.pct_change().reindex(window.index)
        nxt = contracts[i + 1]["symbol"] if i + 1 < len(contracts) else None
        piece = pd.DataFrame({"ret": ret, "close": window, "held": sym})
        frame = frames[sym]
        piece["volume"] = frame["Volume"].reindex(window.index) \
            if "Volume" in frame.columns else np.nan
        piece["open_interest"] = (
            frame["Open Interest"].reindex(window.index)
            if "Open Interest" in frame.columns else np.nan)
        if nxt is not None and nxt in closes:
            second = closes[nxt].reindex(window.index)
            piece["ret2"] = closes[nxt].pct_change().reindex(window.index)
            lq1 = contracts[i]["scheduled_last_quoted"]
            lq2 = contracts[i + 1]["scheduled_last_quoted"]
            if lq1 and lq2:
                gap_days = max(
                    (pd.Timestamp(lq2) - pd.Timestamp(lq1)).days, 1)
                with np.errstate(all="ignore"):
                    piece["slope_ann"] = (np.log(window / second)
                                          * (365.25 / gap_days))
            else:
                piece["slope_ann"] = np.nan
        else:
            piece["slope_ann"] = np.nan
            piece["ret2"] = np.nan
        pieces.append(piece)
        prev_exit = end

    if not pieces:
        return None
    out = pd.concat(pieces)
    out = out[~out.index.duplicated(keep="first")].sort_index()
    return out


def series_path(market: str, campaign_id: str = C.CAMPAIGN_ID) -> Path:
    return layer_dir(campaign_id) / ("%s.csv" % market)


def build_layer(markets: list, *, campaign_id: str = C.CAMPAIGN_ID,
                progress: Optional[Callable[[str], None]] = None) -> dict:
    """Build and persist the layer for ``markets``; returns the manifest."""
    registry = EN.load_contract_registry(campaign_id)
    if registry is None:
        raise RuntimeError("Phase-2 contract registry not frozen yet")
    directory = layer_dir(campaign_id)
    directory.mkdir(parents=True, exist_ok=True)

    manifest = {}
    for market in markets:
        lists = registry["contract_symbols"].get(market, {})
        primary = (sorted(lists, key=lambda s: (len(s), s)) or [None])[0]
        symbols = lists.get(primary, []) if primary else []
        series = build_market_series(market, symbols)
        if series is None or not len(series):
            manifest[market] = {"state": "NO_USABLE_SERIES"}
            continue
        path = series_path(market, campaign_id)
        csv_bytes = series.to_csv().encode("utf-8")
        path.write_bytes(csv_bytes)
        manifest[market] = {
            "state": "OK",
            "rows": int(len(series)),
            "first_date": str(series.index.min().date()),
            "last_date": str(series.index.max().date()),
            "contracts_used": int(series["held"].nunique()),
            "sha256": _hashlib.sha256(csv_bytes).hexdigest(),
        }
        if progress is not None:
            progress("%s: %d rows %s..%s" % (
                market, len(series), manifest[market]["first_date"],
                manifest[market]["last_date"]))

    body = r38.artifact_body(LAYER_SCHEMA, {
        "campaign_id": campaign_id,
        "created_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "calculation_owner": CALCULATION_OWNER,
        "roll_policy": {
            "policy": C.ROLL_POLICY,
            "first_notice_buffer_sessions":
                C.ROLL_FIRST_NOTICE_BUFFER_SESSIONS,
            "last_trade_buffer_sessions": C.ROLL_LAST_TRADE_BUFFER_SESSIONS,
        },
        "returns_are_excess_of_collateral": True,
        "vendor_continuous_series_used": False,
        "min_period_coverage": MIN_PERIOD_COVERAGE,
        "markets": manifest,
    })
    manifest_path = directory / "layer_manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as fh:
        _json.dump(body, fh, indent=2, default=str)
    return body


def load_series(market: str, campaign_id: str = C.CAMPAIGN_ID):
    import pandas as pd

    path = series_path(market, campaign_id)
    if not path.exists():
        return None
    return pd.read_csv(path, index_col=0, parse_dates=True)


def load_manifest(campaign_id: str = C.CAMPAIGN_ID) -> Optional[dict]:
    path = layer_dir(campaign_id) / "layer_manifest.json"
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as fh:
        return _json.load(fh)


def period_returns(daily, decision_dates):
    """Compound daily front-roll returns into decision-period returns.

    ``decision_dates`` must be an ordered DatetimeIndex; the return labelled
    at decision date d_k is realised over (d_k, d_{k+1}] - strictly AFTER the
    decision - and is NaN when observed coverage inside the window falls
    below ``MIN_PERIOD_COVERAGE``.
    """
    import numpy as np
    import pandas as pd

    ret = daily["ret"]
    out = {}
    for k in range(len(decision_dates) - 1):
        d0, d1 = decision_dates[k], decision_dates[k + 1]
        window = ret.loc[(ret.index > d0) & (ret.index <= d1)]
        expected = len(pd.bdate_range(d0, d1)) - 1
        if expected <= 0:
            out[d0] = np.nan
            continue
        observed = window.notna().sum()
        if observed / float(expected) < MIN_PERIOD_COVERAGE:
            out[d0] = np.nan
            continue
        out[d0] = float((1.0 + window.fillna(0.0)).prod() - 1.0)
    return pd.Series(out, dtype=float)
