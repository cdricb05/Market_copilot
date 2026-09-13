"""alpha_agent.alpha_recovery.ownership_identity - the free, authoritative,
effective-dated CUSIP -> owned-symbol bridge.

13F reports holdings by CUSIP. The owned equity panel is keyed by Norgate
symbol. Everything in this campaign depends on joining the two WITHOUT a
purchased identifier product and WITHOUT fuzzy issuer-name matching, and the
preregistration froze how, before any result existed:

    bridge source   SEC Fails-to-Deliver files ONLY. They publish
                    ``SETTLEMENT DATE | CUSIP | SYMBOL | ...``, so ONE publisher
                    states both identifiers in the SAME record. That is what
                    makes the join authoritative; a name join is a guess.
    placeholder     a SYMBOL matching ``(ZZZZ|XXXX)$`` or starting with a digit
                    is a clearing placeholder, rejected by a pattern fixed in
                    advance rather than by per-case judgement.
    ambiguity       a CUSIP resolving to more than one surviving SYMBOL is
                    resolved EFFECTIVE-DATED, never collapsed to "the most
                    common one"; what cannot be resolved is DROPPED and counted.
    point in time   for a decision dated D only FTD files published before D may
                    contribute, and publication is the settlement date plus the
                    DECLARED lag in :data:`ownership_data.FTD_PUBLICATION_LAG_DAYS`.

There are TWO hops, and the second one is where survivorship bias hides. The
owned panel spells a delisted security ``AAMRQ-201312`` while FTD spells the
ticker it actually traded under, ``AAMRQ``. 1238 of the panel's 1897 symbols
carry that suffix, so a naive string join would silently drop every delisted
name and quietly rebuild the survivorship bias the panel exists to prevent. Hop
two therefore resolves ticker -> panel symbol through the estate's own
effective-dated identity owner (:func:`alpha_agent.historical_identity.parse_norgate_symbol`)
and the panel's delisting dates, so a ticker that two different securities used
at two different times resolves to the one that was ALIVE on the decision date.

Deterministic; no network; no live write.
"""
from __future__ import annotations

import io
import re
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

from alpha_agent.historical_identity import parse_norgate_symbol

from . import now_iso, research_root
from . import ownership_data as OD

CALCULATION_OWNER = "alpha_agent.alpha_recovery.ownership_identity"

#: FROZEN in the preregistration, before any result existed.
PLACEHOLDER_PATTERN = r"(ZZZZ|XXXX)$|^[0-9]"
_PLACEHOLDER = re.compile(PLACEHOLDER_PATTERN)

#: The join key is the 8-character CUSIP (issuer 6 + issue 2). The ninth
#: character is only a check digit, and 13F filers truncate or mistype it often
#: enough that joining on 9 would drop real holdings for a reason that carries
#: no information.
CUSIP_KEY_LEN = 8

OBS_CACHE = "ftd_cusip_symbol_observations.csv"


def derived_dir() -> Path:
    return research_root() / "_derived"


def is_placeholder(symbol: str) -> bool:
    return bool(_PLACEHOLDER.search(str(symbol).strip().upper()))


def ticker_key(ticker: str) -> str:
    """Canonical ticker spelling. The panel writes a class share ``BRK.B`` and
    ``BF.B``; FTD writes ``BRKB`` and ``BFB``. Dropping punctuation on BOTH
    sides compares the same string - it is a spelling convention, not a fuzzy
    name match, and without it every dual-class name is silently unbridgeable."""
    return re.sub(r"[^0-9A-Z]", "", str(ticker or "").strip().upper())


def cusip_key(cusip: str) -> str | None:
    c = re.sub(r"[^0-9A-Za-z]", "", str(cusip or "")).upper()
    return c[:CUSIP_KEY_LEN] if len(c) >= CUSIP_KEY_LEN else None


# --------------------------------------------------------------------------- #
# Hop 1: FTD -> (cusip, symbol) observations with a publication instant
# --------------------------------------------------------------------------- #
def parse_ftd_zip(path: Path) -> pd.DataFrame:
    """Distinct (cusip8, symbol) pairs in one FTD file with their first and last
    SETTLEMENT dates. A file holds ~50k rows; only the pairs are kept."""
    try:
        zf = zipfile.ZipFile(path)
    except (zipfile.BadZipFile, OSError):
        return pd.DataFrame(columns=["cusip", "symbol", "first_settlement",
                                     "last_settlement"])
    member = zf.namelist()[0]
    with zf.open(member) as fh:
        df = pd.read_csv(io.TextIOWrapper(fh, encoding="utf-8", errors="replace"),
                         sep="|", usecols=[0, 1, 2], dtype=str,
                         names=["settlement", "cusip", "symbol"], header=0,
                         on_bad_lines="skip")
    df = df.dropna(subset=["cusip", "symbol", "settlement"])
    df["cusip"] = df["cusip"].map(cusip_key)
    df["symbol"] = df["symbol"].str.strip().str.upper()
    df["settlement"] = pd.to_datetime(df["settlement"].str.strip(),
                                      format="%Y%m%d", errors="coerce")
    df = df.dropna(subset=["cusip", "symbol", "settlement"])
    df = df[~df["symbol"].map(is_placeholder)]
    g = df.groupby(["cusip", "symbol"], sort=False)["settlement"]
    out = g.agg(["min", "max"]).reset_index()
    out.columns = ["cusip", "symbol", "first_settlement", "last_settlement"]
    return out


def build_observations(*, rebuild: bool = False, verbose: bool = True) -> pd.DataFrame:
    """Every (cusip8, symbol) pair the FTD archive has ever published, with the
    first and last settlement date it was observed on, and the DECLARED
    publication instant of each."""
    cache = derived_dir() / OBS_CACHE
    if cache.exists() and not rebuild:
        df = pd.read_csv(cache, parse_dates=["first_settlement", "last_settlement",
                                             "first_available", "last_available"])
        return df
    files = sorted(OD.ftd_dir().glob("cnsfails*.zip"))
    parts = []
    for k, p in enumerate(files):
        parts.append(parse_ftd_zip(p))
        if verbose and (k + 1) % 50 == 0:
            print("  FTD parsed %d/%d files" % (k + 1, len(files)), flush=True)
    if not parts:
        return pd.DataFrame(columns=["cusip", "symbol", "first_settlement",
                                     "last_settlement", "first_available",
                                     "last_available"])
    all_df = pd.concat(parts, ignore_index=True)
    g = all_df.groupby(["cusip", "symbol"], sort=True)
    df = pd.DataFrame({"first_settlement": g["first_settlement"].min(),
                       "last_settlement": g["last_settlement"].max()}).reset_index()
    lag = pd.Timedelta(days=OD.FTD_PUBLICATION_LAG_DAYS)
    df["first_available"] = df["first_settlement"] + lag
    df["last_available"] = df["last_settlement"] + lag
    derived_dir().mkdir(parents=True, exist_ok=True)
    df.to_csv(cache, index=False)
    return df


def bridge_as_of(obs: pd.DataFrame, date) -> dict:
    """Effective-dated CUSIP -> ticker using ONLY pairs published before ``date``.

    The ticker in effect is the one observed most recently before the decision.
    A CUSIP whose most recent publication carries two different tickers on the
    SAME settlement date is genuinely ambiguous and is dropped, never guessed.
    """
    d = pd.Timestamp(date)
    vis = obs[obs["first_available"] < d]
    if vis.empty:
        return {"map": {}, "n_cusips": 0, "ambiguous": 0, "dropped_ambiguous": []}
    # the most recent observation of each pair, capped at the decision
    capped = vis.copy()
    capped["eff"] = capped["last_available"].where(capped["last_available"] < d,
                                                   capped["first_available"])
    idx = capped.groupby("cusip")["eff"].transform("max") == capped["eff"]
    top = capped[idx]
    counts = top.groupby("cusip")["symbol"].nunique()
    amb = set(counts[counts > 1].index)
    resolved = top[~top["cusip"].isin(amb)]
    return {"map": dict(zip(resolved["cusip"], resolved["symbol"])),
            "n_cusips": int(vis["cusip"].nunique()),
            "ambiguous": len(amb), "dropped_ambiguous": sorted(amb)[:50]}


# --------------------------------------------------------------------------- #
# Hop 2: ticker -> owned panel symbol, effective-dated
# --------------------------------------------------------------------------- #
def panel_ticker_index(E: dict) -> dict:
    """``ticker -> [(panel_row, first_session, delisting_date)]``.

    The panel spells a delisted security ``AAMRQ-201312``; FTD spells
    ``AAMRQ``. Without this hop every delisted name is dropped and the panel's
    survivorship safety is thrown away at the join.
    """
    syms = list(E["symbols"])
    status = E.get("status") or {}
    tr = E["price"]["tr"]
    dates = np.asarray(E["dates"])
    out: dict = {}
    for i, s in enumerate(syms):
        t = ticker_key(parse_norgate_symbol(str(s))["ticker"])
        seen = np.where(np.isfinite(tr[i]))[0]
        first = str(dates[seen[0]])[:10] if len(seen) else None
        last = str(dates[seen[-1]])[:10] if len(seen) else None
        st = status.get(str(s)) or {}
        delist = st.get("delisting_date") or last
        out.setdefault(t, []).append({"row": i, "symbol": str(s),
                                      "first_session": first,
                                      "delisting_date": delist,
                                      "is_current": int(st.get("is_current") or 0)})
    return out


def resolve_ticker(tix: dict, ticker: str, date: str) -> dict:
    """The panel row a ticker denoted ON ``date``, or an explicit non-resolution.

    Two securities may use the same ticker at different times (``A`` and
    ``A-197701``); the one ALIVE on the decision date is the answer.
    """
    cands = tix.get(ticker_key(ticker))
    if not cands:
        return {"row": None, "state": "NO_PANEL_SYMBOL"}
    alive = [c for c in cands
             if (c["first_session"] or "9999") <= date
             and (c["is_current"] == 1 or (c["delisting_date"] or "0000") >= date)]
    if len(alive) == 1:
        return {"row": alive[0]["row"], "state": "RESOLVED"}
    if not alive:
        return {"row": None, "state": "NOT_LISTED_ON_DATE"}
    return {"row": None, "state": "AMBIGUOUS_TICKER",
            "candidates": [c["symbol"] for c in alive]}


def bridge_rows_as_of(obs: pd.DataFrame, tix: dict, date) -> dict:
    """Both hops at one decision date: cusip8 -> panel row, plus the census the
    preregistration requires to be reported BEFORE any alpha is measured."""
    d = str(pd.Timestamp(date))[:10]
    b = bridge_as_of(obs, date)
    rows, states = {}, {}
    changes = {}
    for cusip, ticker in b["map"].items():
        r = resolve_ticker(tix, ticker, d)
        states[r["state"]] = states.get(r["state"], 0) + 1
        if r["row"] is not None:
            rows[cusip] = r["row"]
            changes[cusip] = ticker
    return {"date": d, "cusip_to_row": rows, "cusip_to_ticker": changes,
            "n_cusips_visible": b["n_cusips"], "n_ambiguous_cusip": b["ambiguous"],
            "n_resolved": len(rows), "hop2_states": states}


# --------------------------------------------------------------------------- #
# Hop 2, CUSIP-anchored: which ROW is this security?
# --------------------------------------------------------------------------- #
def panel_cusip_index(E: dict, obs: pd.DataFrame) -> dict:
    """``cusip8 -> panel row``, built from the whole FTD archive.

    Matching a decision-date ticker to a panel symbol is the WRONG join and it
    fails in one specific, measurable way: the panel labels a still-listed
    security by its CURRENT ticker, so Booking Holdings is spelled ``BKNG`` even
    in 2013, when it traded as ``PCLN``. Roughly fifty S&P names per early
    rebalance were lost exactly this way - CB was ACE, APTV was DLPH, BBWI was
    LB - which is what drove bridged coverage down to 89 % in 2013.

    The estate's identity layer cannot repair it: ``ticker_history`` holds
    exactly one row per security (1895 rows for 1895 securities), so it records
    a lifespan, not a ticker change.

    Both sides already share the identifier that does not change: the CUSIP.
    13F reports a CUSIP and FTD publishes CUSIP and SYMBOL together, so the
    correspondence "this CUSIP is that row" is settled by the archive as a
    whole, and it is BOOKKEEPING, not information - it says which column of the
    price matrix a holding refers to, and that column's returns are the same
    either way. It cannot smuggle in survivorship, because the panel retains
    delisted securities and so a dead security links exactly as a live one does.

    The point-in-time rule still binds where information actually lives: only
    13F accessions FILED before the decision may be counted, and a CUSIP that
    the archive never associates with a panel security is never invented.
    """
    tix = panel_ticker_index(E)
    pairs = obs[["cusip", "symbol", "first_settlement", "last_settlement"]]
    cand: dict = {}
    for cusip, symbol, a, b in pairs.itertuples(index=False, name=None):
        for c in tix.get(ticker_key(symbol), ()):
            lo = max(str(a)[:10], c["first_session"] or "0000")
            hi = min(str(b)[:10], c["delisting_date"] or "9999")
            if lo <= hi:                      # the pair and the security overlap
                cand.setdefault(cusip, set()).add(c["row"])
    rows, ambiguous = {}, []
    for cusip, rs in cand.items():
        if len(rs) == 1:
            rows[cusip] = next(iter(rs))
        else:
            ambiguous.append(cusip)
    return {"cusip_to_row": rows, "ambiguous": sorted(ambiguous),
            "n_cusips": len(cand)}


def bridge_rows_cusip_anchored(obs: pd.DataFrame, cix: dict, date) -> dict:
    """The CUSIP-anchored hop 2, restricted to CUSIPs the FTD archive had
    already published by ``date`` so the visible identifier space stays
    point-in-time."""
    d = str(pd.Timestamp(date))[:10]
    b = bridge_as_of(obs, date)
    visible = set(b["map"])
    rows = {c: r for c, r in cix["cusip_to_row"].items() if c in visible}
    return {"date": d, "cusip_to_row": rows, "cusip_to_ticker": b["map"],
            "n_cusips_visible": b["n_cusips"], "n_ambiguous_cusip": b["ambiguous"],
            "n_resolved": len(rows),
            "hop2_states": {"RESOLVED": len(rows),
                            "NO_PANEL_SYMBOL": len(visible) - len(rows)}}


# --------------------------------------------------------------------------- #
# The census the preregistration demands before any alpha is tested
# --------------------------------------------------------------------------- #
BRIDGE_TICKER_AS_OF = "TICKER_AS_OF"
BRIDGE_CUSIP_ANCHORED = "CUSIP_ANCHORED"
BRIDGE_MODES = (BRIDGE_TICKER_AS_OF, BRIDGE_CUSIP_ANCHORED)


def make_resolver(E: dict, obs: pd.DataFrame, mode: str = BRIDGE_CUSIP_ANCHORED):
    """``date -> {cusip8: panel row}`` under one of the two hop-2 readings."""
    if mode == BRIDGE_CUSIP_ANCHORED:
        cix = panel_cusip_index(E, obs)
        return (lambda d: bridge_rows_cusip_anchored(obs, cix, d),
                {"mode": mode, "cusips_indexed": cix["n_cusips"],
                 "rows_ambiguous": len(cix["ambiguous"])})
    tix = panel_ticker_index(E)
    return (lambda d: bridge_rows_as_of(obs, tix, d),
            {"mode": mode, "panel_tickers": len(tix)})


def coverage_report(E: dict, elig: np.ndarray, decision_dates: list, *,
                    held_cusips_by_date: dict | None = None,
                    resolve=None, mode: str = BRIDGE_TICKER_AS_OF,
                    verbose: bool = True) -> dict:
    """Universe / matched / coverage / ambiguous / dropped / ticker-change counts.

    ``held_cusips_by_date`` restricts the census to the CUSIPs actually reported
    in 13F at each date, which is the population the experiment depends on; when
    it is absent the census covers the whole FTD-visible CUSIP space.
    """
    obs = build_observations(verbose=verbose)
    meta = {"mode": mode}
    if resolve is None:
        resolve, meta = make_resolver(E, obs, mode)
    dates = np.asarray(E["dates"])
    per_date, prev_ticker = [], {}
    ticker_changes = 0
    changed_examples = []
    for t in decision_dates:
        d = str(dates[t])[:10]
        br = resolve(d)
        universe_rows = set(np.where(elig[:, t])[0].tolist())
        if held_cusips_by_date is not None:
            held = held_cusips_by_date.get(d) or set()
            matched_rows = {br["cusip_to_row"][c] for c in held
                            if c in br["cusip_to_row"]}
            n_held = len(held)
            n_held_matched = sum(1 for c in held if c in br["cusip_to_row"])
        else:
            matched_rows = set(br["cusip_to_row"].values())
            n_held = n_held_matched = None
        covered = universe_rows & matched_rows
        for cusip, tk in br["cusip_to_ticker"].items():
            if cusip in prev_ticker and prev_ticker[cusip] != tk:
                ticker_changes += 1
                if len(changed_examples) < 25:
                    changed_examples.append({"cusip": cusip, "from": prev_ticker[cusip],
                                             "to": tk, "date": d})
            prev_ticker[cusip] = tk
        per_date.append({
            "date": d, "universe": len(universe_rows), "matched": len(covered),
            "coverage": (len(covered) / len(universe_rows)) if universe_rows else 0.0,
            "cusips_visible": br["n_cusips_visible"],
            "ambiguous_cusip": br["n_ambiguous_cusip"],
            "held_cusips": n_held, "held_cusips_matched": n_held_matched,
            "hop2": br["hop2_states"]})
    cov = [r["coverage"] for r in per_date]
    dropped = sum(r["ambiguous_cusip"] for r in per_date)
    no_panel = sum((r["hop2"].get("NO_PANEL_SYMBOL") or 0) for r in per_date)
    not_listed = sum((r["hop2"].get("NOT_LISTED_ON_DATE") or 0) for r in per_date)
    amb_ticker = sum((r["hop2"].get("AMBIGUOUS_TICKER") or 0) for r in per_date)
    return {
        "calculation_owner": CALCULATION_OWNER,
        "generated_at": now_iso(),
        "bridge_mode": meta,
        "bridge_source": "SEC Fails-to-Deliver (CUSIP and SYMBOL in one record)",
        "fuzzy_name_matching_used": False,
        "purchased_identifier_product": False,
        "placeholder_pattern": PLACEHOLDER_PATTERN,
        "cusip_join_key": "CUSIP%d" % CUSIP_KEY_LEN,
        "publication_lag_days_declared": OD.FTD_PUBLICATION_LAG_DAYS,
        "ftd_pairs_total": int(len(obs)),
        "ftd_distinct_cusips": int(obs["cusip"].nunique()) if len(obs) else 0,
        "n_decision_dates": len(per_date),
        "universe_mean": float(np.mean([r["universe"] for r in per_date])) if per_date else 0.0,
        "matched_mean": float(np.mean([r["matched"] for r in per_date])) if per_date else 0.0,
        "coverage_mean": float(np.mean(cov)) if cov else 0.0,
        "coverage_min": float(np.min(cov)) if cov else 0.0,
        "coverage_max": float(np.max(cov)) if cov else 0.0,
        "ambiguous_cusip_total": int(dropped),
        "dropped_no_panel_symbol": int(no_panel),
        "dropped_not_listed_on_date": int(not_listed),
        "dropped_ambiguous_ticker": int(amb_ticker),
        "effective_dated_ticker_changes": int(ticker_changes),
        "ticker_change_examples": changed_examples,
        "per_date": per_date,
    }
