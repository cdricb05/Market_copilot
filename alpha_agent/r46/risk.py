"""alpha_agent.r46.risk - ONE cross-strategy risk state.

Twenty-one strategies are not twenty-one independent P&L engines, and with a
handful of matured forward rows nothing defensible can be ESTIMATED about how
they co-move. So this module does what the velocity owner did for evidence:
it declares a conservative structural prior, uses realised forward history
only when there is enough of it, and says which one it used.

**Volatility.** A ``RISK_PRIOR`` per strategy: for a single-instrument
strategy, the instrument's own trailing 252-session realised volatility read
from owned bars (HISTORICAL, point-in-time at the decision session, and
labelled as a prior); for a book, a structural table declared here for the
book's construction. Once a strategy holds at least ``MIN_REALISED_SESSIONS``
forward unit-return observations, realised volatility is used, floored at
half the prior so a quiet month cannot manufacture leverage.

**Correlation.** Structural: 1.0 inside a declared dependence cluster, 0.0
across clusters - the same accounting the velocity owner uses, and it
over-discounts on purpose. With ``MIN_REALISED_SESSIONS`` of common history
the realised correlation is shrunk half-way toward the structural matrix.

**Effective independent P&L streams.** The exponential of the entropy of the
normalised eigenvalues of the correlation matrix over the strategies that
hold shadow capital - an honest "how many bets is this really" that equals
the cluster count under the structural prior and moves only when realised
history earns it.

Historical data informs the RISK PRIOR here. It never informs alpha.
"""
from __future__ import annotations

import datetime as _dt
import math
from typing import Optional

import numpy as np

from . import CAMPAIGN_ID, artifact_body, campaign_dir, write_json
from . import challengers as CH
from . import clock as CK
from . import pnl as PN

CALCULATION_OWNER = "alpha_agent.r46.risk"

ARTIFACT = "R46_4_RISK_STATE.json"

MIN_REALISED_SESSIONS = 40
REALISED_VOL_FLOOR_SHARE_OF_PRIOR = 0.5
CORRELATION_SHRINK_TO_STRUCTURAL = 0.5
PRIOR_LOOKBACK_SESSIONS = 252

#: Structural annualised volatility priors for BOOK constructions, per unit
#: of capital at gross notional 1.0. Declared, not fitted; conservative.
BOOK_VOL_PRIOR = {
    "CROSS_SECTIONAL_LONG_SHORT|US_EQUITY": 0.12,
    "CROSS_SECTIONAL_LONG_SHORT|MULTI_ASSET_FUTURES": 0.10,
    "CROSS_SECTIONAL_LONG_SHORT|FX": 0.07,
    "CROSS_SECTIONAL_LONG_SHORT|COMMODITY": 0.15,
    "TIME_SERIES_DIRECTIONAL_BASKET|MULTI_ASSET_FUTURES": 0.10,
    "RELATIVE_VALUE_SPREAD|RATES": 0.04,
    "DEFAULT_BOOK": 0.12,
}

#: The vocabulary for where a number came from.
SOURCE_PRIOR_STRUCTURAL = "RISK_PRIOR_STRUCTURAL_TABLE"
SOURCE_PRIOR_INSTRUMENT = "RISK_PRIOR_OWNED_INSTRUMENT_HISTORY"
SOURCE_REALISED = "REALISED_FORWARD_UNIT_RETURNS"


def _single_instrument(entry: dict) -> Optional[str]:
    inst = str(entry.get("instrument") or "")
    if inst and not inst.startswith("BOOK:"):
        return inst
    return None


def volatility_prior(entry: dict, as_of: _dt.date, series_fn=None) -> dict:
    """Annualised vol prior for ONE strategy at ``as_of``. Labelled."""
    from . import marketdata as MD
    sf = series_fn or MD.closes
    sym = _single_instrument(entry)
    if sym:
        s = sf(sym)
        if s is not None and len(s):
            s = s[[ts.date() <= as_of for ts in s.index]]
        if s is not None and len(s) > 40:
            w = s.iloc[-(PRIOR_LOOKBACK_SESSIONS + 1):]
            if not bool((w <= 0).any()):
                r = np.log(w).diff().dropna()
                v = float(r.std(ddof=1) * math.sqrt(252.0))
                if math.isfinite(v) and v > 0:
                    return {"annual_vol": v, "source": SOURCE_PRIOR_INSTRUMENT,
                            "instrument": sym, "as_of": str(as_of),
                            "evidence_class": PN.EVIDENCE_RISK_PRIOR}
    key = "%s|%s" % (entry.get("prediction_type"), entry.get("asset_class"))
    v = BOOK_VOL_PRIOR.get(key, BOOK_VOL_PRIOR["DEFAULT_BOOK"])
    return {"annual_vol": float(v), "source": SOURCE_PRIOR_STRUCTURAL,
            "key": key, "as_of": str(as_of),
            "evidence_class": PN.EVIDENCE_RISK_PRIOR}


def strategy_volatility(entry: dict, unit_returns: list, as_of: _dt.date,
                        series_fn=None) -> dict:
    """Prior, or realised when earned - and which one, always stated."""
    prior = volatility_prior(entry, as_of, series_fn)
    obs = [float(v) for v in (unit_returns or ()) if v is not None
           and math.isfinite(float(v))]
    if len(obs) >= MIN_REALISED_SESSIONS:
        sd = float(np.std(obs, ddof=1)) * math.sqrt(252.0)
        floor = prior["annual_vol"] * REALISED_VOL_FLOOR_SHARE_OF_PRIOR
        return {"annual_vol": max(sd, floor), "source": SOURCE_REALISED,
                "n_sessions": len(obs), "realised_unfloored": sd,
                "floor": floor, "prior": prior}
    return dict(prior, n_sessions=len(obs), realised_sessions_needed=(
        MIN_REALISED_SESSIONS - len(obs)))


def structural_correlation(entries: list) -> np.ndarray:
    n = len(entries)
    clusters = [CH.cluster_for(e) for e in entries]
    m = np.zeros((n, n))
    for i in range(n):
        for j in range(n):
            m[i, j] = 1.0 if clusters[i] == clusters[j] else 0.0
    return m


def correlation(entries: list, streams: dict) -> dict:
    """Correlation over the given strategies, structural or shrunk-realised."""
    n = len(entries)
    struct = structural_correlation(entries)
    ids = [e["challenger_id"] for e in entries]
    # Common sessions with a unit return for EVERY strategy in the set.
    common = None
    for cid in ids:
        s = streams.get(cid) or {}
        keys = {k for k, v in s.items() if v is not None}
        common = keys if common is None else (common & keys)
    common = sorted(common or ())
    used = SOURCE_PRIOR_STRUCTURAL
    corr = struct
    if n >= 2 and len(common) >= MIN_REALISED_SESSIONS:
        x = np.array([[float(streams[cid][k]) for k in common] for cid in ids])
        sd = x.std(axis=1, ddof=1)
        if bool((sd > 0).all()):
            real = np.corrcoef(x)
            real = np.nan_to_num(real, nan=0.0)
            corr = (CORRELATION_SHRINK_TO_STRUCTURAL * struct
                    + (1.0 - CORRELATION_SHRINK_TO_STRUCTURAL) * real)
            np.fill_diagonal(corr, 1.0)
            used = SOURCE_REALISED
    return {"ids": ids, "matrix": corr, "source": used,
            "n_common_sessions": len(common),
            "structural": struct}


def effective_streams(corr: np.ndarray, weights: list = None) -> float:
    """exp(entropy of normalised eigenvalues) of the (weighted) correlation."""
    n = corr.shape[0] if corr is not None else 0
    if n == 0:
        return 0.0
    m = np.array(corr, dtype=float)
    if weights is not None:
        w = np.array([max(0.0, float(x)) for x in weights], dtype=float)
        keep = w > 0
        if not keep.any():
            return 0.0
        m = m[np.ix_(keep, keep)]
        w = w[keep]
        d = np.sqrt(w / w.sum())
        m = (d[:, None] * m) * d[None, :]
    ev = np.linalg.eigvalsh((m + m.T) / 2.0)
    ev = np.clip(ev, 1e-12, None)
    p = ev / ev.sum()
    h = float(-(p * np.log(p)).sum())
    return float(math.exp(h))


def cluster_view(entries: list, streams: dict, weights: dict = None) -> dict:
    """Collapse strategies into their dependence clusters - ONE stream each.

    A cluster is one bet by declaration, so the effective count is computed
    over CLUSTER streams: under the structural prior that is the cluster
    count exactly, and realised history can only lower it. Cluster streams
    are the weight-averaged member net returns (equal weights when nothing is
    allocated), and cluster weights are the summed member weights.
    """
    groups: dict = {}
    for e in entries:
        groups.setdefault(CH.cluster_for(e), []).append(e["challenger_id"])
    names = sorted(groups)
    cl_streams, cl_weights = {}, {}
    for name in names:
        members = groups[name]
        ws = {cid: float((weights or {}).get(cid) or 0.0) for cid in members}
        tot = sum(ws.values())
        if tot <= 0:
            ws = {cid: 1.0 for cid in members}
            tot = float(len(members))
        cl_weights[name] = float(sum(float((weights or {}).get(cid) or 0.0)
                                     for cid in members))
        common = None
        for cid in members:
            keys = {k for k, v in (streams.get(cid) or {}).items()
                    if v is not None}
            common = keys if common is None else (common & keys)
        cl_streams[name] = {
            k: sum(ws[cid] * float(streams[cid][k]) for cid in members) / tot
            for k in sorted(common or ())}
    n = len(names)
    struct = np.eye(n)
    corr, used, n_common = struct, SOURCE_PRIOR_STRUCTURAL, 0
    if n >= 2:
        common = None
        for name in names:
            keys = set(cl_streams[name])
            common = keys if common is None else (common & keys)
        common = sorted(common or ())
        n_common = len(common)
        if n_common >= MIN_REALISED_SESSIONS:
            x = np.array([[cl_streams[name][k] for k in common]
                          for name in names])
            sd = x.std(axis=1, ddof=1)
            if bool((sd > 0).all()):
                real = np.nan_to_num(np.corrcoef(x), nan=0.0)
                corr = (CORRELATION_SHRINK_TO_STRUCTURAL * struct
                        + (1.0 - CORRELATION_SHRINK_TO_STRUCTURAL) * real)
                np.fill_diagonal(corr, 1.0)
                used = SOURCE_REALISED
    return {"clusters": names, "members": groups, "matrix": corr,
            "weights": cl_weights, "source": used,
            "n_common_sessions": n_common}


def overlap(entries: list) -> list:
    """Pairwise structural overlap descriptors - what the strategies share."""
    out = []
    for i, a in enumerate(entries):
        for b in entries[i + 1:]:
            shared = []
            if CH.cluster_for(a) == CH.cluster_for(b):
                shared.append("dependence_cluster")
            if a.get("family") == b.get("family"):
                shared.append("economic_family")
            if CH.info_family_for(a) == CH.info_family_for(b):
                shared.append("information_family")
            if a.get("asset_class") == b.get("asset_class"):
                shared.append("asset_class")
            if a.get("instrument") == b.get("instrument"):
                shared.append("instrument")
            if set(a.get("horizons") or ()) & set(b.get("horizons") or ()):
                shared.append("horizon")
            if len(shared) >= 3:
                out.append({"a": a["challenger_id"], "b": b["challenger_id"],
                            "shared": shared, "n_shared": len(shared)})
    out.sort(key=lambda r: -r["n_shared"])
    return out


def build(as_of: _dt.date, entries: list, streams: dict,
          weights: dict = None, campaign_id: str = CAMPAIGN_ID,
          series_fn=None, write: bool = True) -> dict:
    """The one risk state for the field at ``as_of``."""
    vols = {}
    for e in entries:
        vols[e["challenger_id"]] = strategy_volatility(
            e, list((streams.get(e["challenger_id"]) or {}).values()),
            as_of, series_fn)
    cr = correlation(entries, streams)
    ids = cr["ids"]
    w = [float((weights or {}).get(cid) or 0.0) for cid in ids]
    n_alloc = sum(1 for x in w if x > 0)
    clusters = sorted({CH.cluster_for(e) for e in entries})
    alloc_clusters = sorted({CH.cluster_for(e) for e in entries
                             if float((weights or {}).get(
                                 e["challenger_id"]) or 0.0) > 0})

    # Effective independent P&L streams over CLUSTER streams - a cluster is
    # one bet by declaration; realised history can only lower the count.
    cv = cluster_view(entries, streams, weights)
    cl_w = [cv["weights"][c] for c in cv["clusters"]]
    eff_all = effective_streams(cv["matrix"])
    eff_alloc = (effective_streams(cv["matrix"], cl_w)
                 if n_alloc and sum(cl_w) > 0 else 0.0)

    # Marginal diversification: how much the effective count falls without
    # each allocated strategy (positive = it adds an independent stream).
    marginal = {}
    for i, cid in enumerate(ids):
        if w[i] <= 0:
            marginal[cid] = None
            continue
        w2 = dict(weights or {})
        w2[cid] = 0.0
        cv2 = cluster_view(entries, streams, w2)
        cl_w2 = [cv2["weights"][c] for c in cv2["clusters"]]
        without = (effective_streams(cv2["matrix"], cl_w2)
                   if sum(cl_w2) > 0 else 0.0)
        marginal[cid] = round(eff_alloc - without, 4)

    # Risk contribution under the correlation used and the vols stated.
    contrib = {}
    if n_alloc:
        sig = np.array([vols[c]["annual_vol"] for c in ids])
        cov = cr["matrix"] * np.outer(sig, sig)
        wv = np.array(w)
        port_var = float(wv @ cov @ wv)
        port_vol = math.sqrt(max(port_var, 0.0))
        mc = cov @ wv
        for i, cid in enumerate(ids):
            contrib[cid] = (None if port_var <= 0 or w[i] <= 0
                            else float(w[i] * mc[i] / port_var))
    else:
        port_vol = 0.0

    body = artifact_body(
        "r46_4_risk_state/1", CALCULATION_OWNER,
        as_of=str(as_of),
        built_at_utc=CK.iso(CK.now_utc()),
        n_strategies=len(entries),
        n_allocated=n_alloc,
        nominal_streams=len(entries),
        nominal_clusters=len(clusters),
        allocated_clusters=len(alloc_clusters),
        effective_independent_streams_all=round(eff_all, 3),
        effective_independent_streams_allocated=round(eff_alloc, 3),
        effective_streams_rule=("exp(entropy of normalised eigenvalues) over "
                                "CLUSTER streams; equals the cluster count "
                                "under the structural prior and can only "
                                "fall as realised history arrives"),
        cluster_correlation_source=cv["source"],
        cluster_correlation_common_sessions=cv["n_common_sessions"],
        cluster_weights=cv["weights"],
        correlation_source=cr["source"],
        correlation_common_sessions=cr["n_common_sessions"],
        correlation_rule=("structural 1.0 inside a declared dependence "
                          "cluster and 0.0 across; shrunk half-way toward "
                          "realised only after %d common forward sessions"
                          % MIN_REALISED_SESSIONS),
        portfolio_annual_vol_estimate=round(port_vol, 6),
        volatility={cid: vols[cid] for cid in ids},
        risk_contribution=contrib,
        marginal_diversification=marginal,
        overlap=overlap(entries),
        correlation_matrix={"ids": ids,
                            "rows": [[round(float(v), 4) for v in row]
                                     for row in cr["matrix"]]},
        historical_data_informs_the_prior_never_alpha=True,
        precision_is_not_fabricated=True,
    )
    if write:
        write_json(campaign_dir(campaign_id) / ARTIFACT, body)
    return body


__all__ = ["CALCULATION_OWNER", "ARTIFACT", "MIN_REALISED_SESSIONS",
           "BOOK_VOL_PRIOR", "volatility_prior", "strategy_volatility",
           "structural_correlation", "correlation", "effective_streams",
           "cluster_view", "overlap", "build"]
