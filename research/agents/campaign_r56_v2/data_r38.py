"""data_r38 - point-in-time access to the two DF1 datasets of campaign R56_V2.

    r38_native_contract_layer_v4   dated-contract futures layer (frozen 2026-08-22)
    r38_aggregate_oi_v1            all-listed-months exchange open interest, DERIVED here

RESEARCH ONLY. PAPER ONLY. NO ORDERS. This module reads; the only thing it can
write is the derived open-interest dataset, and only under the campaign
foundation directory (never into the repository, never into the R38 store).

It owns NO registry, gate, evaluator or return calculation. Returns and costs
come from their owner, ``alpha_agent.r59.native`` (``load_layer`` /
``load_meta`` / ``cost_vector`` / ``run_book``); this module is a certification
OVERLAY on that reader so every array stays index-aligned with ``run_book``.

What the layer is (measured by data-foundation-agent on 68 markets, 640,656
market-sessions, against the owned dated-contract store; see
foundation/df1_report.json under the campaign_R56_V2 directory)
---------------------------------------------------------------------
* One row per market-session. ``held`` is the dated contract held that session.
* ROLL RULE (alpha_agent.r38.research_layer.roll_exit_date): leave the held
  contract after the settlement of  min(first_notice - 2 weekdays,
  last_quoted - 5 weekdays).  Both dates are exchange SCHEDULE metadata. No
  price, volume or open-interest input: the roll is a calendar, not a
  liquidity switch, and it never looks ahead. Rebuilding ``held`` from that
  metadata alone reproduces 640,582 of 640,656 market-sessions (the other 74
  are a one-to-two-session vendor revision of ``last_quoted`` in
  YAP/SNK/SSG/SXF since 2023-12; the frozen roll is earlier, never later).
* ``ret``  = held contract's settle(s) / ITS OWN settle(s-1) - 1   (640,656 of
  640,656 rows; 17,127 of 17,127 roll sessions).
  ``ret2`` = the NEXT LISTED DELIVERY MONTH's settle(s) / ITS OWN settle(s-1) - 1
  - the contract that will be held next. Next LISTED, not next LIQUID.
  On a roll session both switch contract together and neither books the
  price gap between two contracts. ``ret2`` is NaN while that next month is
  not listed yet (GC/SI serial months, HSI 14% of sessions).
* NEVER difference ``close`` across sessions: it is the raw settle of whichever
  contract is held and jumps at every roll. Use ``ret``.
* "Held" is the NEAREST LISTED month. In GC, SI, HG, PL, EUA, SNK and YAP that
  is usually a thin serial month (GC: held = 2% of all-months open interest at
  the median). The settlement is an exchange mark and the return is valid; the
  modelled cost may be optimistic. AFB is a stale market (``ret`` == 0 on 59%
  of sessions).
* ``volume`` / ``open_interest`` are the HELD CONTRACT ONLY. Their growth is
  the roll cycle. Aggregate open interest is the separate dataset below.
  BOTH are provisional on the newest vendor row (volume revised, open interest
  0.0) until the next session.
* ``close_b`` / ``_CCB`` of futures_panel_v1 is NOT here and must never be used
  as a deferred contract: it is the same front contract re-adjusted.
* Survivorship: every expired contract is retained. The MARKET list is the
  vendor's current composition - no discontinued market is delivered.

Timing every consumer must respect
----------------------------------
* A row dated s holds the settlement of exchange session s (exchange-local
  trade date), observable after the close of s (vendor distribution no later
  than the next morning).
* ``native.run_book`` compounds ``ret[t+1 .. t+horizon]``: the position is
  entered AT THE SETTLEMENT OF SESSION t. A score for decision index t must
  therefore read data through index t-1 only (campaign rule G6).
* Open interest for session s is published by the exchange on s+1. In the
  vendor database the newest row carries ``0.0`` until then. With the
  one-session signal lag the latest usable open interest is index t-2.
  ``OI_LATEST_USABLE_OFFSET`` = 2 encodes that.
* The session grid is the UNION of 15 exchanges' calendars (260-261 dates a
  year; a US market has 250-253). A market that did not trade is NaN (never
  forward filled). "252 sessions" on this grid is about 244 sessions of a US
  market and "21" is about 20: count a market's OWN sessions
  (``own_sessions``), never grid slots - a ">= 90% of 21 slots" test fails a
  complete US series whenever three US holidays fall in the window.
* ``native.decision_indices`` steps on the union grid, so a decision can land
  on a date with no US session (H21: 2011-12-26, 2016-07-04, 2018-01-15,
  2023-01-16). ``run_book`` then treats every US market as not live.
* ``run_book`` charges cost on |w_new - w_old| at rebalances ONLY. It charges
  NOTHING for rolls. ``roll_counts_in_window`` supplies what is needed to add
  the charge (campaign cost model FUTURES_PER_MARKET_R38_PLUS_ROLL_V1).
* ``cost_bps_per_side`` is MODELLED per cost group
  (alpha_agent.r38.contract.COST_MODEL_STATE = MODELLED_NOT_OBSERVED), not
  measured, whatever a docstring elsewhere says.
* FGBL.csv in the layer directory is an ORPHAN of an earlier build: absent from
  the manifest, no ``ret2`` column, no cost. ``native.load_layer`` still loads
  it. It is NOT certified: use ``certified`` to mask it out.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Iterable, Optional

import numpy as np

DATASET_ID = "r38_native_contract_layer_v4"
AGG_OI_DATASET_ID = "r38_aggregate_oi_v1"

R38_ROOT = Path(r"D:\Stock_Prediction_app_data\native_futures_r38"
                r"\r38_native_futures_information_frontier_v4")
LAYER_DIR = R38_ROOT / "native_contract_layer"
LAYER_MANIFEST = LAYER_DIR / "layer_manifest.json"
ML_PANEL = R38_ROOT / "ml_ready_native_futures_panel.csv"
ML_CONTRACT = R38_ROOT / "ml_ready_native_futures_contract.json"
CONTRACT_REGISTRY = R38_ROOT / "dated_contract_registry.json"

CAMPAIGN_FOUNDATION = Path(r"D:\Stock_Prediction_app_data\r59_autonomous_alpha"
                           r"\agents_v2\campaign_R56_V2\foundation")
AGG_OI_DIR = CAMPAIGN_FOUNDATION / "data" / AGG_OI_DATASET_ID
AGG_OI_MANIFEST = AGG_OI_DIR / "manifest.json"

#: R38 ml-panel ``asset_class`` -> alpha_agent.r59.ASSET_CLASSES vocabulary.
R38_TO_R59_ASSET_CLASS = {
    "COMMODITY": "COMMODITY_FUTURES",
    "FX": "FX_FUTURES",
    "RATES": "RATES_FUTURES",
    "INTERNATIONAL_EQUITY": "EQUITY_INDEX_FUTURES",
    "VOLATILITY": "VOLATILITY",
}

#: Scores read data through index t-1 (run_book enters at the settlement of t).
SIGNAL_LAG_SESSIONS = 1
#: Exchange open interest for session s is published on s+1.
OI_PUBLICATION_DELAY_SESSIONS = 1
#: Latest open-interest index usable for a decision at index t is t-2.
OI_LATEST_USABLE_OFFSET = SIGNAL_LAG_SESSIONS + OI_PUBLICATION_DELAY_SESSIONS

AGG_OI_START = "2010-01-01"
AGG_OI_COLUMNS = ("Date", "oi_aggregate", "n_contracts_summed",
                  "n_contracts_oi_positive", "volume_aggregate")


# --------------------------------------------------------------------------- #
# Lineage
# --------------------------------------------------------------------------- #
def _sha256(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def verify_layer() -> dict:
    """Hash every layer CSV against ``layer_manifest.json``.

    Returns ``{"ok", "certified_markets", "orphans", "mismatched", "missing",
    "ml_panel_sha_ok"}``. A CSV that the manifest does not list is an ORPHAN
    and is never certified. ``ok`` is False on any mismatch or missing file.
    """
    manifest = json.loads(LAYER_MANIFEST.read_text(encoding="utf-8"))
    listed = manifest["markets"]
    on_disk = {p.stem: p for p in sorted(LAYER_DIR.glob("*.csv"))}
    mismatched, missing, good = [], [], []
    for market, row in sorted(listed.items()):
        path = on_disk.get(market)
        if path is None or row.get("state") != "OK":
            missing.append(market)
        elif _sha256(path) != row.get("sha256"):
            mismatched.append(market)
        else:
            good.append(market)
    ml = json.loads(ML_CONTRACT.read_text(encoding="utf-8"))
    panel_ok = _sha256(ML_PANEL) == ml.get("panel_sha256")
    return {"ok": not mismatched and not missing and panel_ok,
            "certified_markets": good,
            "orphans": sorted(set(on_disk) - set(listed)),
            "mismatched": mismatched, "missing": missing,
            "ml_panel_sha_ok": bool(panel_ok),
            "manifest_created_at": manifest.get("created_at"),
            "roll_policy": manifest.get("roll_policy")}


# --------------------------------------------------------------------------- #
# The certified view of the layer
# --------------------------------------------------------------------------- #
def load_certified_layer(verify: bool = True) -> dict:
    """``alpha_agent.r59.native.load_layer()`` plus the certification overlay.

    Arrays keep the owner's shape ``[n_markets, n_sessions]`` and order, so a
    mask built here can be passed straight to ``native.run_book``. Added keys:

    ``certified``      bool[n_markets]  manifest-listed, hash-verified, costed
    ``asset_class``    list[str]        r59 vocabulary ("" if unknown)
    ``r38_asset_class``/``economic_group`` list[str]
    ``cost_per_side``  float[n_markets] fraction (owner's ``cost_vector``)
    ``cost_known``     bool[n_markets]  False = charged the panel maximum

    Nothing is forward filled and nothing is lagged here: index s is session s.
    Apply ``SIGNAL_LAG_SESSIONS`` / ``OI_LATEST_USABLE_OFFSET`` when scoring.
    """
    from alpha_agent.r59 import native

    layer = dict(native.load_layer())
    meta = native.load_meta()
    check = verify_layer() if verify else None
    if check is not None and not check["ok"]:
        raise RuntimeError("R38 layer failed hash verification: %s" % check)
    good = set(check["certified_markets"]) if check else set(meta)
    syms = list(layer["symbols"])
    layer["certified"] = np.array([s in good and s in meta for s in syms])
    layer["r38_asset_class"] = [(meta.get(s) or {}).get("asset_class", "")
                                for s in syms]
    layer["economic_group"] = [(meta.get(s) or {}).get("economic_group", "")
                               for s in syms]
    layer["asset_class"] = [R38_TO_R59_ASSET_CLASS.get(a, "")
                            for a in layer["r38_asset_class"]]
    layer["cost_per_side"] = native.cost_vector()
    layer["cost_known"] = np.array([s in meta for s in syms])
    layer["verification"] = check
    return layer


def market_mask(layer: dict, *, asset_classes: Optional[Iterable[str]] = None,
                groups: Optional[Iterable[str]] = None,
                markets: Optional[Iterable[str]] = None) -> np.ndarray:
    """Certified markets restricted by r59 asset class / R38 group / name.

    A STATIC membership mask. Point-in-time eligibility (e.g. 227 of 252
    paired sessions) is the universe agent's rule and is applied per decision.
    """
    m = layer["certified"].copy()
    if asset_classes is not None:
        keep = set(asset_classes)
        m &= np.array([a in keep for a in layer["asset_class"]])
    if groups is not None:
        keep = set(groups)
        m &= np.array([g in keep for g in layer["economic_group"]])
    if markets is not None:
        keep = set(markets)
        m &= np.array([s in keep for s in layer["symbols"]])
    return m


def own_sessions(layer: dict) -> np.ndarray:
    """bool[n_markets, n_sessions]: the market has a finite ``ret`` that date.

    The denominator for every "share of sessions" rule. The grid is a union of
    exchange calendars, so a grid slot is not a session of every market.
    """
    return np.isfinite(layer["ret"])


def roll_counts_in_window(layer: dict, t: int, horizon: int) -> dict:
    """Held-contract changes that a position entered at settle(t) lives through.

    ``layer["roll"][i, s] == 1`` means market i's held contract at session s
    differs from the one at its previous session: the switch was TRADED at the
    settlement of the previous session.

    ``interior``  rolls flagged at s in [t+2, t+horizon]: traded strictly inside
                  the holding interval -> each costs 2 x cost_i x |w_i|.
    ``at_entry``  roll flagged at s = t+1: traded at settle(t), the rebalance
                  instant itself. The new position is opened directly in the new
                  contract; the extra traded notional over run_book's
                  |w_new - w_old| is 2 x min(|w_old|, |w_new|) when both have the
                  same sign, else 0.
    Uses only the exchange schedule (known in advance), so it is not look-ahead.
    """
    roll = layer["roll"]
    n_d = roll.shape[1]
    lo, hi = min(t + 2, n_d), min(t + horizon + 1, n_d)
    interior = roll[:, lo:hi].sum(axis=1).astype(np.int64)
    at_entry = (roll[:, t + 1].astype(np.int64) if t + 1 < n_d
                else np.zeros(roll.shape[0], dtype=np.int64))
    return {"interior": interior, "at_entry": at_entry}


# --------------------------------------------------------------------------- #
# r38_aggregate_oi_v1 - derived from the OWNED dated-contract store
# --------------------------------------------------------------------------- #
def _primary_contracts(market: str, registry: dict) -> list:
    """The primary session's full contract list, exactly as research_layer
    chooses it (shortest session symbol), from the FROZEN R38 registry."""
    lists = registry["contract_symbols"].get(market, {})
    primary = (sorted(lists, key=lambda s: (len(s), s)) or [None])[0]
    return list(lists.get(primary, [])) if primary else []


def aggregate_market(market: str, registry: dict, *, start: str, end: str):
    """Sum open interest and volume over EVERY listed contract of one market.

    Reads the locally installed Norgate futures database (owned; no download).
    Row s = exchange session s. ``oi_aggregate`` for s is observable on s+1.
    Returns ``(frame, stats)``; ``frame`` has ``AGG_OI_COLUMNS``.
    """
    import norgatedata as nd
    import pandas as pd

    oi, vol = {}, {}
    unreadable = 0
    for sym in _primary_contracts(market, registry):
        try:
            df = nd.price_timeseries(sym, timeseriesformat="pandas-dataframe")
        except Exception:
            unreadable += 1
            continue
        if df is None or not len(df) or "Open Interest" not in df.columns:
            continue
        df = df[~df.index.duplicated(keep="last")].sort_index()
        df = df.loc[(df.index >= start) & (df.index <= end)]
        if not len(df):
            continue
        oi[sym] = df["Open Interest"].astype(float)
        vol[sym] = df["Volume"].astype(float)
    if not oi:
        return None, {"contracts_read": 0, "unreadable": unreadable}
    oi_m = pd.DataFrame(oi).sort_index()
    vol_m = pd.DataFrame(vol).reindex(oi_m.index)
    frame = pd.DataFrame({
        "oi_aggregate": oi_m.sum(axis=1, skipna=True, min_count=1),
        "n_contracts_summed": oi_m.notna().sum(axis=1).astype(int),
        "n_contracts_oi_positive": (oi_m > 0).sum(axis=1).astype(int),
        "volume_aggregate": vol_m.sum(axis=1, skipna=True, min_count=1),
    })
    frame.index.name = "Date"
    return frame, {"contracts_read": int(oi_m.shape[1]),
                   "unreadable": unreadable}


def build_aggregate_oi(markets: Iterable[str], *, start: str = AGG_OI_START,
                       end: Optional[str] = None,
                       out_dir: Path = AGG_OI_DIR) -> dict:
    """Build ``r38_aggregate_oi_v1`` (one CSV per market) and its manifest.

    ``end`` defaults to the layer's last session, so the newest - still
    provisional - vendor row is never written. Deterministic for a given
    vendor database vintage, which the manifest records.
    """
    import norgatedata as nd
    import pandas as pd

    registry = json.loads(CONTRACT_REGISTRY.read_text(encoding="utf-8"))
    if end is None:
        any_csv = pd.read_csv(LAYER_DIR / "CL.csv", usecols=["Date"])
        end = str(any_csv["Date"].iloc[-1])[:10]
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    rows = {}
    for market in sorted(set(markets)):
        frame, stats = aggregate_market(market, registry, start=start, end=end)
        if frame is None:
            rows[market] = {"state": "NO_DATA", **stats}
            continue
        body = frame.to_csv(float_format="%.1f").encode("utf-8")
        (out_dir / ("%s.csv" % market)).write_bytes(body)
        rows[market] = {"state": "OK", "rows": int(len(frame)),
                        "first_date": str(frame.index.min().date()),
                        "last_date": str(frame.index.max().date()),
                        "sha256": hashlib.sha256(body).hexdigest(), **stats}
    try:
        vintage = str(nd.last_database_update_time("Futures"))
    except Exception as exc:  # recorded, never hidden
        vintage = "UNKNOWN (%s: %s)" % (type(exc).__name__, exc)
    manifest = {
        "dataset_id": AGG_OI_DATASET_ID,
        "derived_from": "locally installed Norgate 'Futures' database, every "
                        "contract of the primary session listed in "
                        "dated_contract_registry.json (frozen R38 artifact)",
        "builder": "research.agents.campaign_r56_v2.data_r38.build_aggregate_oi",
        "start": start, "end": end,
        "vendor_database_vintage": vintage,
        "columns": list(AGG_OI_COLUMNS),
        "availability_rule": "exchange open interest for session s is "
                             "observable on s+1; latest usable index for a "
                             "decision at index t is t-%d"
                             % OI_LATEST_USABLE_OFFSET,
        "markets": rows,
        "safety": ["RESEARCH ONLY", "PAPER ONLY", "NO ORDERS"],
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=1),
                                           encoding="utf-8")
    return manifest


def load_aggregate_oi(layer: dict, *, verify: bool = True) -> dict:
    """Aggregate open interest aligned to the layer's ``[market, session]`` grid.

    Index s is session s, UNLAGGED. A decision at index t may read columns
    ``<= t - OI_LATEST_USABLE_OFFSET`` only. Markets without a file, and
    sessions without a row, are NaN - never filled.
    """
    import pandas as pd

    manifest = json.loads(AGG_OI_MANIFEST.read_text(encoding="utf-8"))
    dix = {d: i for i, d in enumerate(layer["dates"])}
    shape = (len(layer["symbols"]), len(layer["dates"]))
    oi = np.full(shape, np.nan)
    n_sum = np.full(shape, np.nan)
    vol = np.full(shape, np.nan)
    for i, sym in enumerate(layer["symbols"]):
        row = manifest["markets"].get(sym)
        if not row or row.get("state") != "OK":
            continue
        path = AGG_OI_DIR / ("%s.csv" % sym)
        if verify and _sha256(path) != row["sha256"]:
            raise RuntimeError("aggregate OI hash mismatch: %s" % sym)
        df = pd.read_csv(path)
        ii = np.array([dix.get(d, -1) for d in df["Date"].astype(str).str[:10]])
        ok = ii >= 0
        oi[i, ii[ok]] = df["oi_aggregate"].to_numpy(dtype=np.float64)[ok]
        n_sum[i, ii[ok]] = df["n_contracts_summed"].to_numpy(np.float64)[ok]
        vol[i, ii[ok]] = df["volume_aggregate"].to_numpy(np.float64)[ok]
    return {"oi_aggregate": oi, "n_contracts_summed": n_sum,
            "volume_aggregate": vol, "manifest": manifest,
            "latest_usable_offset": OI_LATEST_USABLE_OFFSET}
