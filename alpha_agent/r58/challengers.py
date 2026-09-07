"""alpha_agent.r58.challengers - immutable prospective forward challengers.

The project has spent months insisting that missing point-in-time history must
never be fabricated. This module is the payoff for that discipline: when an
information family is real, timestamped and orthogonal but its owned history is
weeks or months long, R58 does not back-fill it and does not abandon it. It
freezes the signal specification TODAY and starts the evidence clock.

A challenger may only be frozen if its cross-section can actually be COMPUTED
from owned data at the freeze session. A specification with no computable
ranking is a wish, not a challenger - so NEWS_EVENT (7 tickers), EARNINGS_EVENT
(15) and CORPORATE_ACTION (5) are recorded as NOT_A_CROSS_SECTION and refused,
even though a paragraph about news sentiment would have read well.

Every record states plainly that zero forward observations exist at freeze, and
records the exact inputs, hashes and git identity that produced it. Nothing here
touches an operational store, creates an order, or promotes anything.
"""
from __future__ import annotations

import json
import os
import subprocess
from collections import defaultdict
from datetime import date, timedelta

import numpy as np

from . import (CADENCE, EQ_COST_RATE_PER_SIDE, EQ_MIN_ADV, EQ_MIN_HISTORY,
               EQ_MIN_PRICE, EQ_TOP_N, HORIZON, INGEST_ROOT, REPO_ROOT,
               now_iso, protocol_hash, research_root, stable_hash, write_artifact)
from . import fundamentals as FU
from .engine import xs_rank01, xs_z
from ..r57.engine import eligibility as _r57_eligibility

ARTIFACT = "r58_forward_challengers.json"
CHALLENGER_SUBDIR = "challengers"
LOOKBACK_DAYS = 21          # event-window length for the event challengers
BASELINE_DAYS = 63


def _git_identity() -> dict:
    def _run(*args):
        try:
            return subprocess.run(["git", "-C", str(REPO_ROOT), *args],
                                  capture_output=True, text=True,
                                  timeout=30).stdout.strip()
        except Exception:                                # noqa: BLE001
            return "UNKNOWN"
    return {"commit": _run("rev-parse", "HEAD"),
            "branch": _run("rev-parse", "--abbrev-ref", "HEAD"),
            "dirty": bool(_run("status", "--porcelain"))}


# --------------------------------------------------------------------------- #
# Freeze-session universe
# --------------------------------------------------------------------------- #
def freeze_universe(price: dict, session: str) -> dict:
    """The R57-floor eligible PIT S&P 500 members at the freeze session."""
    dates = price["dates"]
    t = int(np.searchsorted(dates, session))
    if t >= len(dates) or dates[t] != session:
        t = int(np.searchsorted(dates, session)) - 1
    elig = _r57_eligibility(price, t)
    ix = np.where(elig)[0]
    return {"session": str(dates[t]), "t": t, "index": ix,
            "symbols": [str(s) for s in price["symbols"][ix]],
            "n": int(len(ix)),
            "rule": ("PIT S&P 500 member AND unadjusted close >= $%.0f AND "
                     "63-session median dollar volume >= $%.0fM AND >= %d prior "
                     "sessions" % (EQ_MIN_PRICE, EQ_MIN_ADV / 1e6, EQ_MIN_HISTORY))}


def fundamental_snapshot(symbols, asof: str) -> dict:
    """Derived fundamentals per symbol as of ``asof``, filed <= asof."""
    bridge = FU.cik_bridge()
    sym2cik = {s: bridge[s] for s in symbols if s in bridge}
    facts = FU.load_facts(set(sym2cik.values()))
    snap_by_cik = {}
    for cik, rows in facts.items():
        st = FU.CompanyState()
        used = 0
        for filed, tag, ps, pe, d, val in rows:
            if filed[:10] > asof:
                break
            st.absorb(filed, tag, ps, pe, d, val)
            used += 1
        snap_by_cik[cik] = st.snapshot() if used else None
    out = {}
    for s in symbols:
        cik = sym2cik.get(s)
        sn = snap_by_cik.get(cik) if cik else None
        if not sn:
            continue
        a, cfo, capex, ni = sn["assets"], sn["cfo"], sn["capex"], sn["ni"]
        if a is None or not a or cfo is None or capex is None:
            continue
        fcf = cfo - capex
        row = {"fcf_to_assets": fcf / a}
        if ni is not None:
            row["accruals_to_assets"] = (ni - fcf) / a
        row["obs_period_end"] = sn["obs_period_end"]
        row["last_filed"] = sn["last_filed"]
        out[s] = row
    return out


# --------------------------------------------------------------------------- #
# Event-feed readers (owned normalized records, effective_at <= freeze session)
# --------------------------------------------------------------------------- #
def _read_family(family: str, upto: str):
    root = INGEST_ROOT / family
    if not root.exists():
        return []
    rows = []
    for dirpath, _d, files in os.walk(root):
        for f in files:
            if not f.endswith(".jsonl"):
                continue
            with open(os.path.join(dirpath, f), encoding="utf-8") as fh:
                for line in fh:
                    try:
                        r = json.loads(line)
                    except Exception:                    # noqa: BLE001
                        continue
                    if str(r.get("effective_at"))[:10] <= upto:
                        rows.append(r)
    return rows


def short_volume_scores(symbols, upto: str) -> dict:
    """Change in FINRA short-volume ratio: recent mean minus baseline mean.

    Rising short-sale participation is the hypothesis's bearish leg, so the
    score is the NEGATIVE of the change (falling short pressure ranks high).
    """
    rows = _read_family("SHORT_VOLUME", upto)
    want = set(symbols)
    by = defaultdict(list)
    for r in rows:
        tk = r.get("ticker")
        if tk not in want:
            continue
        p = r.get("normalized_payload") or {}
        v = p.get("short_volume_ratio")
        if v is None:
            continue
        by[tk].append((str(r.get("effective_at"))[:10], float(v)))
    cut = (date.fromisoformat(upto) - timedelta(days=LOOKBACK_DAYS)).isoformat()
    out, meta = {}, {"records": len(rows), "tickers_with_data": 0}
    for tk, vals in by.items():
        recent = [v for d, v in vals if d > cut]
        base = [v for d, v in vals if d <= cut]
        if len(recent) >= 5 and len(base) >= 5:
            out[tk] = -(float(np.mean(recent)) - float(np.mean(base)))
    meta["tickers_with_data"] = len(out)
    meta["window"] = {"recent_after": cut, "upto": upto}
    return out, meta


def insider_scores(symbols, upto: str) -> dict:
    """Net insider acquisition intensity from Form 4 filings."""
    rows = _read_family("INSIDER_FILING", upto)
    want = set(symbols)
    acq, dis, tot = defaultdict(float), defaultdict(float), defaultdict(float)
    cut = (date.fromisoformat(upto) - timedelta(days=BASELINE_DAYS)).isoformat()
    for r in rows:
        tk = r.get("ticker")
        if tk not in want or str(r.get("effective_at"))[:10] <= cut:
            continue
        p = r.get("normalized_payload") or {}
        if not p.get("is_form4_insider"):
            continue
        tot[tk] += 1.0
        ad = str(p.get("acquired_disposed") or "").upper()
        if ad.startswith("A"):
            acq[tk] += 1.0
        elif ad.startswith("D"):
            dis[tk] += 1.0
    out = {}
    for tk in tot:
        n = acq[tk] + dis[tk]
        if n >= 2:
            out[tk] = (acq[tk] - dis[tk]) / n
    return out, {"records": len(rows), "tickers_with_data": len(out),
                 "window_days": BASELINE_DAYS,
                 "note": "net = (acquisitions - disposals) / labelled Form 4 rows"}


def disclosure_intensity_scores(symbols, upto: str) -> dict:
    """Abnormal 8-K disclosure rate: recent count versus the company's baseline.

    Unscheduled disclosure is information arriving. The sign is NOT assumed - the
    hypothesis registered here is that ABNORMALLY HIGH unscheduled disclosure
    precedes underperformance, so the score is the negative of the abnormality.
    """
    rows = _read_family("FILING_EVENT", upto)
    want = set(symbols)
    cut = (date.fromisoformat(upto) - timedelta(days=LOOKBACK_DAYS)).isoformat()
    base_cut = (date.fromisoformat(upto) - timedelta(days=BASELINE_DAYS * 3)).isoformat()
    recent, base = defaultdict(float), defaultdict(float)
    for r in rows:
        tk = r.get("ticker")
        d = str(r.get("effective_at"))[:10]
        if tk not in want or d <= base_cut:
            continue
        p = r.get("normalized_payload") or {}
        if not p.get("is_8k") and str(p.get("form_type") or "") not in ("8-K", "8-K/A"):
            continue
        if d > cut:
            recent[tk] += 1.0
        else:
            base[tk] += 1.0
    span = (date.fromisoformat(upto) - date.fromisoformat(base_cut)).days - LOOKBACK_DAYS
    out = {}
    for tk in set(recent) | set(base):
        rate = base[tk] / max(span, 1) * LOOKBACK_DAYS
        out[tk] = -(recent[tk] - rate)
    return out, {"records": len(rows), "tickers_with_data": len(out),
                 "recent_window_days": LOOKBACK_DAYS,
                 "baseline_window_days": span}


# --------------------------------------------------------------------------- #
def _book_from_scores(scores: dict, universe: dict, top_n=EQ_TOP_N) -> dict:
    """Long-only equal-weight top-N book from a symbol->score map."""
    ranked = sorted((s for s in scores if s in set(universe["symbols"])),
                    key=lambda s: -scores[s])
    held = ranked[:top_n]
    if not held:
        return {"weights": {}, "n_held": 0, "n_scored": len(ranked)}
    w = round(1.0 / len(held), 10)
    return {"weights": {s: w for s in held}, "n_held": len(held),
            "n_scored": len(ranked),
            "top_10_by_score": [(s, round(float(scores[s]), 8)) for s in held[:10]]}


def build(price: dict, session: str) -> dict:
    """Compute every candidate challenger's cross-section at the freeze session."""
    uni = freeze_universe(price, session)
    syms = uni["symbols"]
    asof = uni["session"]

    fund = fundamental_snapshot(syms, asof)
    sv, sv_meta = short_volume_scores(syms, asof)
    ins, ins_meta = insider_scores(syms, asof)
    dis, dis_meta = disclosure_intensity_scores(syms, asof)

    # fundamental composite + momentum veto (the B3 shape), computed here on the
    # freeze session rather than read from the cube, whose last slot is earlier
    tr = price["tr"]
    t = uni["t"]
    mom = {}
    for s, i in zip(syms, uni["index"]):
        a, b = t - 21, t - 21 - 126
        if b >= 0 and np.isfinite(tr[i, a]) and np.isfinite(tr[i, b]) and tr[i, b] > 0:
            mom[s] = float(tr[i, a] / tr[i, b] - 1.0)
    have = [s for s in syms if s in fund and "accruals_to_assets" in fund[s]]
    f_arr = np.array([fund[s]["fcf_to_assets"] for s in have])
    a_arr = np.array([fund[s]["accruals_to_assets"] for s in have])
    m = np.ones(len(have), dtype=bool)
    zf = xs_z(f_arr, m); za = xs_z(-a_arr, m)
    comp = {s: float((zf[k] + za[k]) / 2.0) for k, s in enumerate(have)
            if np.isfinite(zf[k]) and np.isfinite(za[k])}
    mom_have = [s for s in comp if s in mom]
    mr = xs_rank01(np.array([mom[s] for s in mom_have]),
                   np.ones(len(mom_have), dtype=bool))
    veto = {s for k, s in enumerate(mom_have)
            if np.isfinite(mr[k]) and mr[k] < 1.0 / 3.0}
    fund_veto = {s: v for s, v in comp.items() if s not in veto and s in mom}
    fcf_pure = {s: fund[s]["fcf_to_assets"] for s in have}

    # The insider hypothesis is COMPUTED and then REFUSED on the evidence: the
    # direction field it needs is populated on 195 of 28,002 owned records.
    # Freezing a challenger whose book is empty would be theatre.
    ins_meta = dict(ins_meta)
    ins_meta["refused"] = bool(len(ins) < 100)

    return {"universe": uni, "asof": asof,
            "insider_probe": {"scored": len(ins), "meta": ins_meta},
            "candidates": {
                "R58_SHORT_VOLUME_PRESSURE_V1": (sv, sv_meta),
                "R58_DISCLOSURE_INTENSITY_V1": (dis, dis_meta),
                "R58_FUND_MOMENTUM_VETO_V1": (fund_veto,
                                              {"tickers_with_data": len(fund_veto),
                                               "vetoed": len(veto)}),
                "R58_FCF_PURE_V1": (fcf_pure, {"tickers_with_data": len(fcf_pure)}),
            }}


SPECS = {
    "R58_SHORT_VOLUME_PRESSURE_V1": {
        "hypothesis": "a fall in a name's FINRA short-sale participation relative "
                      "to its own recent baseline precedes relative outperformance",
        "information_family": "SHORT_VOLUME (FINRA daily short-sale volume)",
        "why_prospective": "owned history begins 2026-07-23 (24 day-partitions). "
                           "No honest backtest is possible and none was attempted.",
        "orthogonality": "positioning/flow data, not price and not accounting; "
                         "nothing in R57's or R58's historical tournaments uses it",
        "formula": "-(mean short_volume_ratio over the last 21 calendar days "
                   "minus mean over all earlier owned days); >= 5 observations "
                   "in each window",
    },
    "R58_DISCLOSURE_INTENSITY_V1": {
        "hypothesis": "ABNORMALLY HIGH unscheduled disclosure (8-K rate above the "
                      "company's own baseline) precedes relative UNDERperformance",
        "information_family": "FILING_EVENT (SEC EDGAR submissions, 8-K)",
        "why_prospective": "owned normalized history begins 2025-11-24",
        "orthogonality": "disclosure timing and frequency; uses no accounting "
                         "number and no price",
        "formula": "-(8-K count in the last 21 calendar days minus the "
                   "company's own baseline rate scaled to 21 days)",
    },
    "R58_FUND_MOMENTUM_VETO_V1": {
        "hypothesis": "the fundamental composite ranks well among names that are "
                      "NOT in visible price distress; momentum belongs as a veto, "
                      "not as a constant 50% vote",
        "information_family": "SEC companyfacts PIT fundamentals + price momentum",
        "why_prospective": "THIS ONE IS DIFFERENT AND THE DIFFERENCE IS DISCLOSED. "
                           "It has ample history and WAS prosecuted historically as "
                           "family B3. It did NOT pass: the pre-registered coverage "
                           "gate blocked it (a veto excludes a third of the universe "
                           "by construction, which the gate cannot distinguish from "
                           "a data gap) and it was never BH-tested. It is frozen "
                           "here because it was the most sign-stable, "
                           "sector-robust shape in the campaign - a judgement made "
                           "AFTER seeing the lockbox, which is selection bias, which "
                           "is exactly why forward evidence is the only thing that "
                           "can settle it. It carries NO historical alpha claim.",
        "orthogonality": "not orthogonal - it is the incumbent's own two inputs "
                         "recombined; the claim is about COMBINATION, not new data",
        "formula": "equal-weight z of (+FCF/assets, -accruals/assets) over the "
                   "eligible universe, with any name in the bottom momentum "
                   "tercile (126-session return skipping 21) removed",
        "post_hoc_selection_disclosed": True,
    },
    "R58_FCF_PURE_V1": {
        "hypothesis": "free cash flow to assets alone - the one component of the "
                      "incumbent's fundamental leg whose sign did not flip across "
                      "R58's three historical layers - ranks stocks forward",
        "information_family": "SEC companyfacts PIT fundamentals",
        "why_prospective": "it has history and it FAILED the historical gates "
                           "(lockbox +0.83%/yr, t 0.24, below the 1.5% materiality "
                           "floor). It is frozen as the CONTROL against which the "
                           "other four are read, not as a candidate.",
        "orthogonality": "none claimed; this is the incumbent's strongest single "
                         "component and exists here as a baseline",
        "formula": "cross-sectional rank of (cfo_ttm - capex_ttm) / assets",
        "role": "CONTROL",
    },
}

REFUSED = {
    "INSIDER_FILING": {
        "reason": "FIELD_UNPOPULATED",
        "detail": "the direction the hypothesis needs (acquired vs disposed) is "
                  "populated on 195 of 28,002 owned records (0.7%), and only "
                  "3,138 rows carry is_form4_insider at all. The net-insider "
                  "challenger was written, computed, and produced an EMPTY book, "
                  "so it was refused rather than frozen. Timestamps are fine; the "
                  "transaction detail is not collected.",
        "reopen_condition": "the Stage-2 SEC collector parses Form 4 transaction "
                            "tables (acquired/disposed, shares, price) for the "
                            "eligible universe",
    },
    "NEWS_EVENT": {"reason": "NOT_A_CROSS_SECTION",
                   "detail": "5,494 owned records but only 7 distinct tickers - "
                             "the feed is symbol-limited to a small watchlist. A "
                             "ranking challenger cannot be built from 7 names and "
                             "was not invented."},
    "EARNINGS_EVENT": {"reason": "NOT_A_CROSS_SECTION",
                       "detail": "34 records over 15 tickers"},
    "CORPORATE_ACTION": {"reason": "NOT_A_CROSS_SECTION",
                         "detail": "8 records over 5 tickers"},
    "TRADING_HALT": {"reason": "UNIVERSE_MISMATCH",
                     "detail": "411 tickers but overwhelmingly micro-cap names "
                               "outside the eligible S&P 500 universe"},
    "MACRO_BEA_BLS": {"reason": "TIMESTAMP_INSUFFICIENT",
                      "detail": "available_at is null with RELEASE_LAG_UNKNOWN; a "
                                "release date was not fabricated"},
}


def freeze(price: dict, session: str) -> dict:
    """Write the immutable challenger records. Forward evidence starts AFTER this."""
    built = build(price, session)
    uni = built["universe"]
    d = research_root() / CHALLENGER_SUBDIR
    d.mkdir(parents=True, exist_ok=True)
    git = _git_identity()
    frozen = {}
    for name, (scores, meta) in built["candidates"].items():
        book = _book_from_scores(scores, uni)
        rec = {
            "challenger_id": name,
            "spec": SPECS[name],
            "status": "FORWARD_PENDING_ALPHA_CANDIDATE",
            "forward_observations_at_freeze": 0,
            "inception_rule": ("signal uses information available through the "
                               "close of %s; the position is effective at the "
                               "NEXT close; forward evidence begins strictly "
                               "after inception and is NEVER back-filled"
                               % uni["session"]),
            "eligible_session": uni["session"],
            "decision_timestamp_utc": now_iso(),
            "universe": {"rule": uni["rule"], "n_eligible": uni["n"],
                         "identity_hash": stable_hash(uni["symbols"])},
            "construction": {"type": "LONG_ONLY_EQUAL_WEIGHT_TOP_N",
                             "top_n": EQ_TOP_N, "cash_weight": 0.0,
                             "rebalance_cadence_sessions": CADENCE,
                             "evaluation_horizon_sessions": HORIZON},
            "cost_policy": {"bps_per_side": EQ_COST_RATE_PER_SIDE * 1e4,
                            "charged_to": "strategy AND benchmark symmetrically"},
            "benchmark": "equal weight of the eligible universe at inception, "
                         "same cadence, same costs",
            "input_evidence": meta,
            "n_scored": book["n_scored"], "n_held": book["n_held"],
            "weights": book["weights"],
            "top_10_by_score": book.get("top_10_by_score"),
            "spec_hash": stable_hash(SPECS[name]),
            "weights_hash": stable_hash(book["weights"]),
            "protocol_sha256": protocol_hash(),
            "git": git,
            "safety": {"research_only": True, "creates_orders": False,
                       "creates_fills": False, "promotes_model": False,
                       "activates_sleeve": False,
                       "mutates_operational_store": False},
        }
        rec["record_hash"] = stable_hash({k: v for k, v in rec.items()
                                          if k != "decision_timestamp_utc"})
        p = d / ("%s.json" % name)
        p.write_text(json.dumps(rec, indent=1, sort_keys=True), encoding="utf-8")
        frozen[name] = {"path": str(p), "n_held": rec["n_held"],
                        "n_scored": rec["n_scored"],
                        "record_hash": rec["record_hash"],
                        "role": SPECS[name].get("role", "CANDIDATE")}
    body = {
        "track": "R58_PROSPECTIVE_CHALLENGERS",
        "eligible_session": uni["session"],
        "frozen": frozen,
        "insider_probe_result": built.get("insider_probe"),
        "refused_information_families": REFUSED,
        "freeze_discipline": ("a challenger is frozen only if its cross-section "
                              "can be COMPUTED today from owned data; four "
                              "information families were refused rather than "
                              "described"),
        "forward_observations_at_freeze": 0,
        "backfill": "FORBIDDEN",
    }
    write_artifact(ARTIFACT, body)
    return body
