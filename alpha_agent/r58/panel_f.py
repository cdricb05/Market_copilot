"""alpha_agent.r58.panel_f - PANEL-F, the survivorship-safe PIT fundamental cube.

PANEL-F is the intersection of two owned assets that had never been joined:

    the R57 Norgate S&P 500 Current & Past price panel (1,897 securities, PIT
    index membership, TOTALRETURN prices, delisted names retained)

    the owned SEC EDGAR companyfacts store (1.6M facts, real ``filed`` dates,
    846 CIKs), reached through the identity layer's RESOLVED CIK bridge

885 symbols join, 237 of them delisted. That is the first time this project can
score a fundamental signal on a universe that contains the companies that died.

The cube is built ONCE and cached as npz keyed by a manifest hash; reads are
pure. Every feature at decision index t is derived only from facts whose SEC
filed date is <= the calendar date of t.
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import date
from pathlib import Path

import numpy as np

from . import (CADENCE, DISCOVERY_START, HORIZON, now_iso, research_root,
               stable_hash)
from . import fundamentals as FU
from ..r57 import panel as r57panel

PANEL_NAME = "r58_pit_fundamental_panel_v1"

# derived feature names, fixed before any experiment ran
FEATURES = (
    "fcf_to_assets",            # (cfo - capex) / assets
    "accruals_to_assets",       # (ni - (cfo - capex)) / assets   higher = worse
    "accruals_to_assets_prior",
    "opinc_to_assets",
    "opinc_to_assets_prior",
    "wc_to_revenue",            # (inventory + receivables) / revenue
    "wc_to_revenue_prior",
    "rnd_to_assets",
    "asset_growth",
    "sales_growth",
    "obs_age_days",             # decision date - fundamental period end
    "filed_ix",                 # session index of the most recent periodic filing
    "has_core",                 # 1 if assets and at least one TTM flow exist
)
F_IX = {n: i for i, n in enumerate(FEATURES)}


def panel_dir() -> Path:
    d = research_root() / "panels"
    d.mkdir(parents=True, exist_ok=True)
    return d


def decision_indices(dates: np.ndarray) -> np.ndarray:
    start = int(np.searchsorted(dates, DISCOVERY_START))
    last = len(dates) - HORIZON - 2
    return np.arange(start, last + 1, CADENCE)


def _safe_div(a, b):
    if a is None or b is None:
        return np.nan
    try:
        b = float(b)
        if not np.isfinite(b) or abs(b) < 1e-9:
            return np.nan
        v = float(a) / b
        return v if np.isfinite(v) else np.nan
    except Exception:                                    # noqa: BLE001
        return np.nan


def _derive(snap, dec_date, date_ix):
    """Turn one company snapshot into the R58 derived feature vector."""
    out = np.full(len(FEATURES), np.nan)
    a = snap.get("assets")
    ap = snap.get("assets_prior")
    cfo, capex, ni = snap.get("cfo"), snap.get("capex"), snap.get("ni")
    fcf = None if (cfo is None or capex is None) else cfo - capex
    out[F_IX["fcf_to_assets"]] = _safe_div(fcf, a)
    if ni is not None and fcf is not None:
        out[F_IX["accruals_to_assets"]] = _safe_div(ni - fcf, a)
    cfo_p, capex_p, ni_p = (snap.get("cfo_prior"), snap.get("capex_prior"),
                            snap.get("ni_prior"))
    fcf_p = None if (cfo_p is None or capex_p is None) else cfo_p - capex_p
    if ni_p is not None and fcf_p is not None:
        out[F_IX["accruals_to_assets_prior"]] = _safe_div(ni_p - fcf_p, ap)
    out[F_IX["opinc_to_assets"]] = _safe_div(snap.get("opinc"), a)
    out[F_IX["opinc_to_assets_prior"]] = _safe_div(snap.get("opinc_prior"), ap)
    inv, rec = snap.get("inventory"), snap.get("receivables")
    if inv is not None or rec is not None:
        out[F_IX["wc_to_revenue"]] = _safe_div((inv or 0.0) + (rec or 0.0),
                                               snap.get("revenue"))
    invp, recp = snap.get("inventory_prior"), snap.get("receivables_prior")
    if invp is not None or recp is not None:
        out[F_IX["wc_to_revenue_prior"]] = _safe_div((invp or 0.0) + (recp or 0.0),
                                                     snap.get("revenue_prior"))
    out[F_IX["rnd_to_assets"]] = _safe_div(snap.get("rnd"), a)
    if a is not None and ap:
        g = _safe_div(a, ap)
        out[F_IX["asset_growth"]] = (g - 1.0) if np.isfinite(g) else np.nan
    rev, rev_p = snap.get("revenue"), snap.get("revenue_prior")
    if rev is not None and rev_p:
        g = _safe_div(rev, rev_p)
        out[F_IX["sales_growth"]] = (g - 1.0) if np.isfinite(g) else np.nan
    pe = snap.get("obs_period_end")
    if pe:
        try:
            out[F_IX["obs_age_days"]] = (date.fromisoformat(dec_date)
                                         - date.fromisoformat(pe)).days
        except Exception:                                # noqa: BLE001
            pass
    lf = snap.get("last_filed")
    if lf:
        out[F_IX["filed_ix"]] = float(date_ix.get(lf[:10], -1))
    out[F_IX["has_core"]] = 1.0 if (a is not None and (cfo is not None
                                                      or ni is not None)) else 0.0
    return out


def build(progress_every: int = 100) -> dict:
    """Build and cache PANEL-F. Idempotent: an existing cache is returned."""
    npz_path = panel_dir() / (PANEL_NAME + ".npz")
    meta_path = panel_dir() / (PANEL_NAME + ".meta.json")
    if npz_path.exists() and meta_path.exists():
        return json.loads(meta_path.read_text(encoding="utf-8"))

    price = r57panel.load_panel()
    dates = price["dates"]
    symbols = list(price["symbols"])
    date_ix = {d: i for i, d in enumerate(dates)}
    dec = decision_indices(dates)
    dec_dates = [str(dates[t]) for t in dec]

    bridge = FU.cik_bridge()
    status = FU.security_status()
    sym2cik = {s: bridge[s] for s in symbols if s in bridge}
    facts = FU.load_facts(set(sym2cik.values()))

    # one company state replay per CIK, snapshotted at each distinct filed date
    snaps_by_cik = {}
    ciks = sorted(set(sym2cik.values()) & set(facts))
    for k, cik in enumerate(ciks):
        if progress_every and k % progress_every == 0:
            print("panel_f %d/%d cik=%s" % (k, len(ciks), cik), flush=True)
        st = FU.CompanyState()
        rows = facts[cik]
        snaps, i, n = [], 0, len(rows)
        while i < n:
            filed = rows[i][0]
            while i < n and rows[i][0] == filed:
                _f, tag, ps, pe, d, val = rows[i]
                st.absorb(filed, tag, ps, pe, d, val)
                i += 1
            snaps.append((filed[:10], st.snapshot()))
        snaps_by_cik[cik] = snaps

    n_sym, n_dec = len(symbols), len(dec)
    cube = np.full((n_sym, n_dec, len(FEATURES)), np.nan, dtype=np.float32)
    joined = []
    for si, sym in enumerate(symbols):
        cik = sym2cik.get(sym)
        snaps = snaps_by_cik.get(cik) if cik else None
        if not snaps:
            continue
        joined.append(sym)
        filed_dates = [s[0] for s in snaps]
        pos = np.searchsorted(np.array(filed_dates), np.array(dec_dates),
                              side="right") - 1
        for j, p in enumerate(pos):
            if p < 0:
                continue
            cube[si, j, :] = _derive(snaps[p][1], dec_dates[j], date_ix)

    np.savez_compressed(npz_path, cube=cube, dec=dec)
    meta = {
        "panel": PANEL_NAME, "built_at": now_iso(),
        "price_panel": price["meta"]["panel"],
        "price_panel_manifest_hash": price["meta"]["manifest_hash"],
        "n_symbols": n_sym, "n_decisions": n_dec,
        "features": list(FEATURES),
        "decision_dates": dec_dates,
        "cadence": CADENCE, "horizon": HORIZON,
        "joined_symbols": sorted(joined),
        "n_joined": len(joined),
        "n_joined_current": sum(1 for s in joined
                                if status.get(s, {}).get("is_current") == 1),
        "n_joined_delisted": sum(1 for s in joined
                                 if status.get(s, {}).get("is_current") != 1),
        "n_ciks": len(ciks),
        "availability_rule": "SEC filed <= decision calendar date; NEXT_CLOSE entry",
        "restatement_rule": "latest filed <= t per (cik, concept, period)",
        "ttm_rule": "annual anchor A; A + YTD_curr - YTD_prior when the same-length "
                    "prior-year YTD exists (YTD_DIFF), else A (ANNUAL)",
        "concept_ladders": {"instant": FU.INSTANT_CONCEPTS, "flow": FU.FLOW_CONCEPTS},
        "manifest_hash": stable_hash({"syms": sorted(joined), "dec": dec_dates,
                                      "feat": list(FEATURES)}),
    }
    meta_path.write_text(json.dumps(meta, indent=1), encoding="utf-8")
    return meta


def load() -> dict:
    """Load PANEL-F together with the R57 price panel it is aligned to."""
    meta = json.loads((panel_dir() / (PANEL_NAME + ".meta.json"))
                      .read_text(encoding="utf-8"))
    z = np.load(panel_dir() / (PANEL_NAME + ".npz"))
    price = r57panel.load_panel()
    return {
        "meta": meta,
        "cube": z["cube"].astype(np.float64),
        "dec": z["dec"],
        "dec_dates": np.array(meta["decision_dates"]),
        "price": price,
        "f_ix": dict(F_IX),
    }


def feature(pf: dict, name: str, j: int) -> np.ndarray:
    """Cross-section of one derived feature at decision slot j."""
    return pf["cube"][:, j, pf["f_ix"][name]]
