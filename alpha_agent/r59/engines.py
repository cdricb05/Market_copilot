"""alpha_agent.r59.engines - the research ENGINES the queue can invoke.

Level 3 of the target architecture. AlphaAgent asks "how do we investigate this
mandate?"; an engine answers by producing concrete, executable hypotheses and
measuring them. Every engine here obeys three rules:

* ONE EVALUATION KERNEL. Statistics come from
  :func:`alpha_agent.r57.engine.nw_tstat` and
  :func:`alpha_agent.r57.engine.bh_fdr`, futures books are simulated by
  :func:`alpha_agent.r57.futures_tournament.simulate` and equity books by
  :func:`alpha_agent.r57.engine.run_topn`. A new release does not get a new
  evaluator, because a family measured by its own code is not comparable to
  the 25 families R57 and R58 already prosecuted.
* THE MACHINE NAMES THE HYPOTHESIS. The mathematical engine calls R39's own
  grammar - :func:`alpha_agent.r39.representation_factory.generate_auto_transforms`
  and :func:`~alpha_agent.r39.representation_factory.generate_symbolic` - so
  candidates exist without a human enumerating them. R59 adds no handcrafted
  replacement for that machinery.
* POINT-IN-TIME OR NOTHING. Every feature at decision date d reads observations
  dated <= d; the forward window opens at d+1. Panels carry survivorship-safe
  membership and delisted names.

Engines return a uniform result dict; they never write memory, never freeze a
challenger and never decide what runs next. Those are the queue's and the
governor's jobs.
"""
from __future__ import annotations

import warnings

import numpy as np

from .. import r59
from ..r57 import engine as K              # the ONE statistical kernel
from ..r57 import futures_tournament as FT  # the ONE futures simulator
from . import frontier as FR

CALCULATION_OWNER = "alpha_agent.r59.engines"

_CACHE: dict = {}


# --------------------------------------------------------------------------- #
# Panels
# --------------------------------------------------------------------------- #
def load_equity_panel() -> dict:
    """The owned survivorship-safe US single-name panel (R57 cache)."""
    if "eq" in _CACHE:
        return _CACHE["eq"]
    meta = r59.read_json(r59.EQUITY_PANEL_META)
    if not meta or not r59.EQUITY_PANEL.exists():
        raise FileNotFoundError("equity PIT panel not present: %s"
                                % r59.EQUITY_PANEL)
    z = np.load(r59.EQUITY_PANEL, allow_pickle=True)
    panel = {"tr": z["tr"].astype(np.float64), "un": z["un"].astype(np.float64),
             "vol": z["vol"].astype(np.float64), "mem": z["mem"],
             "spy_tr": z["spy_tr"],
             "dates": np.asarray(meta["dates"]),
             "symbols": np.asarray(meta["symbols"]),
             "sectors": meta.get("sectors")}
    _CACHE["eq"] = panel
    return panel


def load_futures_panel() -> dict:
    """The owned Norgate continuous-futures panel (R57 cache), with the
    per-market metadata the R57 simulator needs."""
    if "fut" in _CACHE:
        return _CACHE["fut"]
    meta = r59.read_json(r59.FUTURES_PANEL_META)
    if not meta or not r59.FUTURES_PANEL.exists():
        raise FileNotFoundError("futures panel not present: %s"
                                % r59.FUTURES_PANEL)
    z = np.load(r59.FUTURES_PANEL, allow_pickle=True)
    markets = meta["markets"]
    fp = {"close_a": z["close_a"].astype(np.float64),
          "close_b": z["close_b"].astype(np.float64),
          "rolls": z["rolls"],
          "dates": np.asarray(meta["dates"]),
          "point_values": np.asarray([float(m["point_value"])
                                      for m in markets]),
          "symbols": [m["symbol"] for m in markets],
          "markets": markets}
    _CACHE["fut"] = fp
    return fp


# --------------------------------------------------------------------------- #
# Base feature state (trailing windows only)
# --------------------------------------------------------------------------- #
def _rolling_std(x: np.ndarray, w: int) -> np.ndarray:
    return FT._rolling_std(x, w)  # noqa: SLF001 - the kernel's own helper


def futures_base_features(fp: dict) -> dict:
    """Base state per market, aligned to the daily grid, using data <= t.

    These are the columns the machine grammar composes over. Every one is a
    trailing statistic of the market's own back-adjusted dollar path: nothing
    reads a future bar, and no cross-sectional statistic is folded in here.
    """
    close = fp["close_a"]
    pv = np.asarray(fp["point_values"])[:, None]
    dollar = close * pv
    dpnl = np.diff(dollar, axis=1, prepend=dollar[:, :1])
    dpnl[:, 0] = 0.0
    n_m, n_d = close.shape

    def _shift(a, k):
        out = np.full_like(a, np.nan)
        if k < n_d:
            out[:, k:] = a[:, :n_d - k]
        return out

    def _ret(k):
        prev = _shift(dollar, k)
        with np.errstate(invalid="ignore", divide="ignore"):
            return (dollar - prev) / np.abs(prev)

    vol63 = _rolling_std(dpnl, 63)
    vol252 = _rolling_std(dpnl, 252)
    # NOTE: there is deliberately NO carry column here. ``close_b`` is
    # ``<SYM>_CCB`` - the SAME front contract under a second back-adjustment
    # convention, not the deferred contract (measured daily-change correlation
    # against close_a: median 0.982 across the 103 markets). The difference
    # between them is the accumulated adjustment offset from an arbitrary
    # anchor date and carries no economic information. Real carry needs two
    # DATED contracts and lives in :mod:`alpha_agent.r59.native`, on the R38
    # native-contract layer.

    # trailing 252-session high proximity. A market that has not started yet is
    # an all-NaN window - an expected state on a 22-year panel of 103 markets
    # with staggered listings, not an error - so the warning is suppressed and
    # the value stays NaN (a MASK, never a fill).
    high = np.full_like(close, np.nan)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        for j in range(n_d):
            lo = max(0, j - 251)
            high[:, j] = np.nanmax(close[:, lo:j + 1], axis=1)
    with np.errstate(invalid="ignore", divide="ignore"):
        prox = close / high

    feats = {
        "ret_21": _ret(21), "ret_63": _ret(63), "ret_126": _ret(126),
        "ret_252": _ret(252),
        "mom_252_21": _ret(252) - _ret(21),
        "vol_63": vol63, "vol_252": vol252,
        "vol_ratio": np.where(vol252 > 0, vol63 / vol252, np.nan),
        "high_252_prox": prox,
    }
    return feats


def equity_base_features(panel: dict, t: int) -> dict:
    """Base state per symbol at ONE decision index, using data <= t."""
    tr, un, vol = panel["tr"], panel["un"], panel["vol"]

    def _ret(k):
        a, b = tr[:, t], tr[:, max(0, t - k)]
        with np.errstate(invalid="ignore", divide="ignore"):
            return np.where(np.isfinite(a) & np.isfinite(b) & (b > 0),
                            a / b - 1.0, np.nan)

    lo = max(0, t - 62)
    win = tr[:, lo:t + 1]
    with np.errstate(invalid="ignore", divide="ignore"):
        rets = np.diff(win, axis=1) / win[:, :-1]
    rets = np.where(np.isfinite(rets), rets, np.nan)
    with np.errstate(invalid="ignore"):
        v63 = np.nanstd(rets, axis=1)
        adv = np.nanmedian(un[:, lo:t + 1] * vol[:, lo:t + 1], axis=1)
    lo2 = max(0, t - 251)
    with np.errstate(invalid="ignore"):
        hi = np.nanmax(tr[:, lo2:t + 1], axis=1)
    with np.errstate(invalid="ignore", divide="ignore"):
        prox = np.where(hi > 0, tr[:, t] / hi, np.nan)

    return {
        "mom_252_21": _ret(252) - _ret(21),
        "mom_126_21": _ret(126) - _ret(21),
        "rev_21": -_ret(21),
        "vol_63": v63,
        "log_adv": np.log(np.where(adv > 0, adv, np.nan)),
        "high_252_prox": prox,
        "ret_21": _ret(21),
        "ret_252": _ret(252),
    }


# --------------------------------------------------------------------------- #
# Decision grid + partition (inherited from R57/R58)
# --------------------------------------------------------------------------- #
def decision_grid(dates: np.ndarray, cadence: int = r59.CADENCE,
                  horizon: int = r59.HORIZON,
                  first_date: str = r59.DISCOVERY_START) -> np.ndarray:
    return K.decision_indices(dates, cadence, first_date, horizon)


def layers_for(dates: np.ndarray, idx: np.ndarray,
               cadence: int = r59.CADENCE,
               horizon: int = r59.HORIZON) -> np.ndarray:
    return K.layer_of(dates, idx, cadence, horizon)


# --------------------------------------------------------------------------- #
# ENGINE A/E - economic + cross-asset families on the futures panel
# --------------------------------------------------------------------------- #
def _zs(v: np.ndarray) -> np.ndarray:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        m = np.nanmean(v)
        s = np.nanstd(v)
    if not np.isfinite(s) or s <= 0:
        return np.zeros_like(v)
    return np.clip((v - m) / s, -3.0, 3.0)


def _xs_rank_signal(col: np.ndarray, mask: np.ndarray) -> np.ndarray:
    """Cross-sectional demeaned rank in [-1, 1] over the masked members."""
    out = np.zeros_like(col, dtype=np.float64)
    sel = mask & np.isfinite(col)
    n = int(sel.sum())
    if n < 3:
        return out
    vals = col[sel]
    order = np.argsort(np.argsort(vals)).astype(np.float64)
    out[sel] = 2.0 * (order / max(1, n - 1)) - 1.0
    return out


def build_signal_matrix(fp: dict, feats: dict, *, feature: str, mode: str,
                        mask: np.ndarray, cadence: int = 5) -> np.ndarray:
    """Turn a feature into a (markets x dates) position-strength matrix.

    ``mode`` TIME_SERIES uses the sign of the market's own standardised feature;
    CROSS_SECTIONAL uses the demeaned cross-sectional rank inside the scope, so
    the book is self-financing across the scope's members. Both read column j
    only, which is the decision date; the R57 simulator then enters at the NEXT
    close.
    """
    v = feats[feature]
    n_m, n_d = v.shape
    sig = np.zeros((n_m, n_d), dtype=np.float64)
    last = np.zeros(n_m, dtype=np.float64)
    for j in range(n_d):
        if j % cadence == 0:
            col = v[:, j]
            if mode == "TIME_SERIES":
                s = np.tanh(_zs(np.where(np.isfinite(col), col, np.nan)))
                s = np.where(np.isfinite(s), s, 0.0)
            else:
                s = _xs_rank_signal(col, mask)
            last = np.where(mask, s, 0.0)
        sig[:, j] = last
    return sig


ECONOMIC_FEATURE_MAP = {
    "TIME_SERIES_TREND": ("mom_252_21", "TIME_SERIES"),
    "CROSS_SECTIONAL_MOMENTUM": ("mom_252_21", "CROSS_SECTIONAL"),
    "CHANNEL_BREAKOUT": ("high_252_prox", "TIME_SERIES"),
    "RETURN_SEASONALITY": ("ret_252", "TIME_SERIES"),
    "VOLATILITY_SCALED_TREND": ("vol_ratio", "TIME_SERIES"),
    "INTER_COMMODITY_SPREAD": ("ret_63", "CROSS_SECTIONAL"),
    "CROSS_ASSET_RELATIVE_VALUE": ("ret_126", "CROSS_SECTIONAL"),
    "CROSS_ASSET_LEAD_LAG": ("ret_21", "CROSS_SECTIONAL"),
    "CROSS_ASSET_REGIME_CONDITIONING": ("vol_ratio", "CROSS_SECTIONAL"),
}


def run_futures_hypothesis(*, asset_class: str, feature: str, mode: str,
                           label: str, cadence: int = 5) -> dict:
    """Measure ONE futures hypothesis through the R57 simulator and kernel."""
    fp = load_futures_panel()
    feats = _futures_features_cached(fp)
    members = FR.futures_members(asset_class)
    if not members:
        return {"state": "NO_MEMBERS", "asset_class": asset_class}
    mask = np.zeros(len(fp["symbols"]), dtype=bool)
    for m in members:
        mask[int(m["index"])] = True
    if feature not in feats:
        return {"state": "UNKNOWN_FEATURE", "feature": feature}

    sig = build_signal_matrix(fp, feats, feature=feature, mode=mode,
                              mask=mask, cadence=cadence)
    sim = FT.simulate(fp, "r59:%s" % label, "a",
                      signal_override=sig, market_mask=mask)
    out = {"state": "MEASURED", "asset_class": asset_class,
           "feature": feature, "mode": mode, "label": label,
           "n_markets": int(mask.sum()),
           "markets": [m["symbol"] for m in members][:40],
           "evaluator": "alpha_agent.r57.futures_tournament.simulate",
           "layers": {}}
    for layer in ("D", "V", "L"):
        out["layers"][layer] = FT.stats(sim, layer)
    return out


def _futures_features_cached(fp: dict) -> dict:
    if "fut_feats" not in _CACHE:
        _CACHE["fut_feats"] = futures_base_features(fp)
    return _CACHE["fut_feats"]


# --------------------------------------------------------------------------- #
# ENGINE B - the MATHEMATICAL discovery engine (R39's own grammar)
# --------------------------------------------------------------------------- #
def _panel_frame(fp: dict, feats: dict, mask: np.ndarray, *,
                 cadence_days: int = 21):
    """A long-format decision panel the R39 grammar can operate on.

    R39's generators expect a pandas frame keyed by ``market_id`` and
    ``decision_date``; the owned R57 cache is a matrix. Building the frame here
    is what lets R59 call R39's generators UNCHANGED instead of reimplementing
    a grammar - the specific mistake this release exists to stop.
    """
    import pandas as pd

    dates = fp["dates"]
    n_d = len(dates)
    cols = list(feats)
    rows = []
    idx = list(range(0, n_d, cadence_days))
    members = np.where(mask)[0]
    for j in idx:
        for i in members:
            rec = {"market_id": fp["symbols"][int(i)],
                   "decision_date": str(dates[j]), "_j": j, "_i": int(i)}
            for c in cols:
                rec[c] = float(feats[c][i, j]) if np.isfinite(feats[c][i, j]) \
                    else np.nan
            rows.append(rec)
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# Signal identity.
#
# A machine grammar generates monotone transforms of the same column - abs(x),
# tanh(x), x + x, rankxs(x) - and a rank-based book cannot tell them apart:
# they select the identical names on the identical dates and produce a t-stat
# identical to sixteen digits. Keying novelty on the EXPRESSION therefore
# counts one hypothesis many times, inflates the search denominator with
# duplicates and - worse - lets one book clear the gate repeatedly and look
# like several independent discoveries.
#
# So identity is the REALISED SIGNAL, not the formula that produced it, and a
# feature that is a monotone function of a base column is attributed to THAT
# column's economic family, so it inherits the burden that family has already
# absorbed. Without the attribution, a machine rediscovery of the liquidity
# factor is charged nothing for the twelve price families R57 already retired.
# --------------------------------------------------------------------------- #
EQUITY_BASE_FAMILY = {
    "mom_252_21": "CROSS_SECTIONAL_MOMENTUM",
    "mom_126_21": "CROSS_SECTIONAL_MOMENTUM",
    "ret_252": "CROSS_SECTIONAL_MOMENTUM",
    "rev_21": "SHORT_HORIZON_REVERSAL",
    "ret_21": "SHORT_HORIZON_REVERSAL",
    "vol_63": "LOW_RISK_ANOMALY",
    "log_adv": "LIQUIDITY_PREMIUM",
    "high_252_prox": "52_WEEK_HIGH_PROXIMITY",
}

FUTURES_BASE_FAMILY = {
    "ret_21": "CROSS_SECTIONAL_MOMENTUM",
    "ret_63": "CROSS_SECTIONAL_MOMENTUM",
    "ret_126": "CROSS_SECTIONAL_MOMENTUM",
    "ret_252": "CROSS_SECTIONAL_MOMENTUM",
    "mom_252_21": "CROSS_SECTIONAL_MOMENTUM",
    "vol_63": "VOLATILITY_SCALED_TREND",
    "vol_252": "VOLATILITY_SCALED_TREND",
    "vol_ratio": "VOLATILITY_SCALED_TREND",
    "high_252_prox": "CHANNEL_BREAKOUT",
}

#: |Spearman| at or above this against a base column means the machine feature
#: orders the cross-section the same way that column does. It is not "highly
#: correlated with" - at this level the book is the same book.
MONOTONE_EQUIVALENT_RHO = 0.999


def _spearman_pooled(a, b) -> float:
    """|Spearman| between two pooled columns, ignoring rows either is missing."""
    import pandas as pd
    x = pd.to_numeric(a, errors="coerce")
    y = pd.to_numeric(b, errors="coerce")
    ok = x.notna() & y.notna()
    if int(ok.sum()) < 50:
        return 0.0
    rx = x[ok].rank()
    ry = y[ok].rank()
    if rx.std() == 0 or ry.std() == 0:
        return 0.0
    return abs(float(rx.corr(ry)))


def _base_equivalent(frame, feature: str, base_cols, family_map: dict) -> dict:
    """Which base column, if any, this machine feature simply re-expresses."""
    best, best_rho = None, 0.0
    for c in base_cols:
        rho = _spearman_pooled(frame[feature], frame[c])
        if rho > best_rho:
            best, best_rho = c, rho
    if best is not None and best_rho >= MONOTONE_EQUIVALENT_RHO:
        return {"base_column": best, "rho": round(best_rho, 6),
                "economic_family": family_map.get(best),
                "is_monotone_equivalent": True}
    return {"base_column": best, "rho": round(best_rho, 6),
            "economic_family": None, "is_monotone_equivalent": False}


def _rank_fingerprint(per_decision: list) -> str:
    """Hash the realised decision-by-decision ORDERING of a signal.

    Two features that select the same names in the same order on the same dates
    are one hypothesis, however different their formulas look.
    """
    parts = []
    for key, order in per_decision:
        parts.append("%s:%s" % (key, ",".join(str(int(i)) for i in order)))
    return r59.short_hash("|".join(parts), 16)


def _order_of(col: np.ndarray, mask: np.ndarray, top: int = 50) -> list:
    v = np.where(mask & np.isfinite(col), col, -np.inf)
    k = int(np.isfinite(v).sum())
    k = min(top, int((v > -np.inf).sum()))
    if k <= 0:
        return []
    idx = np.argpartition(-v, k - 1)[:k]
    return list(idx[np.argsort(-v[idx])])


def equity_decision_frame(*, cadence: int = r59.CADENCE,
                          max_names_per_date: int = 500):
    """A long-format equity decision panel the R39 grammar can operate on.

    Bounded on purpose: at each decision date only the ELIGIBLE names (R57's
    own membership / price / liquidity / history rule) are carried, and the
    most liquid ``max_names_per_date`` of those. The full 1897 x 5601 matrix
    would make a 10-million-row frame whose groupby cost buys nothing - the
    illiquid tail is not investable and the R57 top-N kernel excludes it
    anyway, so the frame matches what the evaluator will actually trade.
    """
    import pandas as pd

    panel = load_equity_panel()
    dates = panel["dates"]
    idx = decision_grid(dates)
    syms = panel["symbols"]
    rows = []
    for j, t in enumerate(idx):
        elig = K.eligibility(panel, int(t))
        if not elig.any():
            continue
        f = equity_base_features(panel, int(t))
        adv = f["log_adv"]
        cand = np.where(elig & np.isfinite(adv))[0]
        if len(cand) > max_names_per_date:
            cand = cand[np.argsort(-adv[cand])[:max_names_per_date]]
        for i in cand:
            rec = {"market_id": str(syms[int(i)]),
                   "decision_date": str(dates[int(t)]),
                   "_j": int(t), "_i": int(i), "_dec": j}
            for c, v in f.items():
                val = v[int(i)]
                rec[c] = float(val) if np.isfinite(val) else np.nan
            rows.append(rec)
    return pd.DataFrame(rows), idx


def generate_equity_machine_hypotheses(*, kind: str, k: int,
                                       seed: int) -> list:
    """Generate machine hypotheses on the EQUITY cross-section.

    Same R39 grammar, same lineage, different substrate. Without this the only
    families left open in US_EQUITY are generative ones, and blocking them
    would make the estate's deepest panel unresearchable - a scope declared
    READY that nothing can ever run.
    """
    from ..r39 import representation_factory as RF

    key = "eqframe"
    if key not in _CACHE:
        _CACHE[key] = equity_decision_frame()
    frame, idx = _CACHE[key]
    frame = frame.copy()
    base = tuple(c for c in frame.columns
                 if c not in ("market_id", "decision_date", "_j", "_i", "_dec"))
    if kind == "AUTO":
        frame, names, lineage = RF.generate_auto_transforms(
            frame, base_cols=base, k=k, seed=seed, id_col="market_id")
        method = "AUTO_TRANSFORM_GRAMMAR"
    elif kind == "SYMBOLIC":
        frame, names, lineage = RF.generate_symbolic(
            frame, base_cols=base, k=k, seed=seed)
        method = "SYMBOLIC_TREE_SEARCH"
    else:
        raise ValueError("unknown machine kind: %s" % kind)

    lin = {l["name"]: l for l in lineage}
    out = []
    for name in names:
        spec = lin.get(name, {}).get("spec")
        if spec and (str(spec).startswith("('col'")
                     or str(spec).startswith("('U', 'id'")):
            continue
        eq = _base_equivalent(frame, name, base, EQUITY_BASE_FAMILY)
        out.append({"feature_name": name, "kind": kind,
                    "generation_method": method, "spec": spec, "seed": seed,
                    "asset_class": r59.AC_US_EQUITY,
                    "base_equivalent": eq,
                    "generator": "alpha_agent.r39.representation_factory"})
    _CACHE["eqframe_%s_%d" % (kind, seed)] = (frame, idx)
    return out


def run_equity_machine_hypothesis(*, kind: str, seed: int, feature_name: str,
                                  label: str) -> dict:
    """Measure ONE machine-generated equity feature through R57's top-N kernel."""
    key = "eqframe_%s_%d" % (kind, seed)
    if key not in _CACHE:
        generate_equity_machine_hypotheses(kind=kind, k=1, seed=seed)
    if key not in _CACHE:
        return {"state": "NO_FRAME", "asset_class": r59.AC_US_EQUITY}
    frame, _idx = _CACHE[key]
    if feature_name not in frame.columns:
        return {"state": "UNKNOWN_FEATURE", "feature": feature_name}

    panel = load_equity_panel()
    n_sym = panel["tr"].shape[0]
    # Sparse lookup: decision index -> per-symbol score, built once.
    scores: dict = {}
    sub = frame[["_j", "_i", feature_name]].to_numpy()
    for j, i, v in sub:
        col = scores.get(int(j))
        if col is None:
            col = np.full(n_sym, np.nan)
            scores[int(j)] = col
        col[int(i)] = v

    def _score(p, t):
        col = scores.get(int(t))
        return col if col is not None else np.full(n_sym, np.nan)

    res = K.run_topn(panel, _score, r59.CADENCE, r59.HORIZON,
                     first_date=r59.DISCOVERY_START)

    fp = _rank_fingerprint([
        (int(t), _order_of(scores[int(t)], K.eligibility(panel, int(t))))
        for t in sorted(scores)])
    return {"state": "MEASURED", "asset_class": r59.AC_US_EQUITY,
            "feature": feature_name, "kind": kind, "seed": seed,
            "signal_fingerprint": fp,
            "evaluator": "alpha_agent.r57.engine.run_topn",
            "generator": "alpha_agent.r39.representation_factory",
            "layers": {l: K.layer_stats(res, l) for l in ("D", "V", "L")}}


def generate_machine_hypotheses(*, asset_class: str, kind: str, k: int,
                                seed: int) -> list:
    """Generate machine-named hypotheses with R39's grammar.

    ``kind`` is AUTO (the seeded transform grammar) or SYMBOLIC (seeded
    expression trees). Nothing about the resulting features was named by a
    human: the generator invents the composition, hashes it into a name and
    keeps the lineage.
    """
    from ..r39 import representation_factory as RF

    fp = load_futures_panel()
    feats = _futures_features_cached(fp)
    members = FR.futures_members(asset_class)
    if len(members) < 2:
        return []
    mask = np.zeros(len(fp["symbols"]), dtype=bool)
    for m in members:
        mask[int(m["index"])] = True

    frame = _panel_frame(fp, feats, mask)
    base = tuple(c for c in feats)
    if kind == "AUTO":
        frame, names, lineage = RF.generate_auto_transforms(
            frame, base_cols=base, k=k, seed=seed, id_col="market_id")
        method = "AUTO_TRANSFORM_GRAMMAR"
    elif kind == "SYMBOLIC":
        frame, names, lineage = RF.generate_symbolic(
            frame, base_cols=base, k=k, seed=seed)
        method = "SYMBOLIC_TREE_SEARCH"
    else:
        raise ValueError("unknown machine kind: %s" % kind)

    out = []
    lin = {l["name"]: l for l in lineage}
    for name in names:
        spec = lin.get(name, {}).get("spec")
        # R39's tree sampler terminates early with probability 0.3 per level,
        # so a "symbolic" candidate is sometimes the bare base column. That is
        # not a new hypothesis - it is the feature the base state already has,
        # and admitting it would inflate the search denominator with duplicates
        # while claiming machine novelty. Filter it here rather than changing
        # R39's generator, whose sampling is part of a frozen release.
        if spec and str(spec).startswith("('col'"):
            continue
        if spec and str(spec).startswith("('U', 'id'"):
            continue
        eq = _base_equivalent(frame, name, base, FUTURES_BASE_FAMILY)
        out.append({"feature_name": name, "kind": kind,
                    "generation_method": method,
                    "spec": spec,
                    "seed": seed, "asset_class": asset_class,
                    "degenerate_filtered": False,
                    "base_equivalent": eq,
                    "generator": "alpha_agent.r39.representation_factory"})
    _CACHE["frame_%s_%s_%d" % (asset_class, kind, seed)] = (frame, mask)
    return out


def run_machine_hypothesis(*, asset_class: str, kind: str, seed: int,
                           feature_name: str, label: str) -> dict:
    """Measure ONE machine-generated feature through the SAME kernel.

    The generated column lives on the decision frame; it is expanded back onto
    the daily grid with a forward hold between decisions (never interpolated
    backwards), then simulated by the R57 futures simulator.
    """
    key = "frame_%s_%s_%d" % (asset_class, kind, seed)
    if key not in _CACHE:
        generate_machine_hypotheses(asset_class=asset_class, kind=kind,
                                    k=1, seed=seed)
    if key not in _CACHE:
        return {"state": "NO_FRAME", "asset_class": asset_class}
    frame, mask = _CACHE[key]
    if feature_name not in frame.columns:
        return {"state": "UNKNOWN_FEATURE", "feature": feature_name}

    fp = load_futures_panel()
    n_m, n_d = fp["close_a"].shape
    grid = np.full((n_m, n_d), np.nan)
    sub = frame[["_i", "_j", feature_name]].to_numpy()
    for i, j, v in sub:
        grid[int(i), int(j)] = v

    # forward-hold the decision value to the next decision (data <= t only)
    last = np.full(n_m, np.nan)
    held = np.zeros((n_m, n_d))
    for j in range(n_d):
        col = grid[:, j]
        upd = np.isfinite(col)
        last = np.where(upd, col, last)
        held[:, j] = last

    sig = np.zeros((n_m, n_d))
    cur = np.zeros(n_m)
    for j in range(n_d):
        if j % 21 == 0:
            cur = _xs_rank_signal(held[:, j], mask)
        sig[:, j] = cur

    sim = FT.simulate(fp, "r59m:%s" % label, "a",
                      signal_override=sig, market_mask=mask)
    fingerprint = _rank_fingerprint([
        (j, _order_of(held[:, j], mask, top=int(mask.sum())))
        for j in range(0, n_d, 21)])
    return {"state": "MEASURED", "asset_class": asset_class,
            "feature": feature_name, "kind": kind, "seed": seed,
            "n_markets": int(mask.sum()),
            "signal_fingerprint": fingerprint,
            "evaluator": "alpha_agent.r57.futures_tournament.simulate",
            "generator": "alpha_agent.r39.representation_factory",
            "layers": {l: FT.stats(sim, l) for l in ("D", "V", "L")}}


# --------------------------------------------------------------------------- #
# ENGINE C - equity families through the R57 top-N kernel
# --------------------------------------------------------------------------- #
EQUITY_FEATURE_MAP = {
    "CROSS_SECTIONAL_MOMENTUM": "mom_252_21",
    "SHORT_HORIZON_REVERSAL": "rev_21",
    "LOW_RISK_ANOMALY": "vol_63",
    "LIQUIDITY_PREMIUM": "log_adv",
    "IDIOSYNCRATIC_VOLATILITY": "vol_63",
    "52_WEEK_HIGH_PROXIMITY": "high_252_prox",
    "RESIDUAL_MOMENTUM": "mom_126_21",
    "SECTOR_RELATIVE_MOMENTUM": "mom_252_21",
}


def run_equity_hypothesis(*, feature: str, sign: float, label: str) -> dict:
    """Measure ONE equity hypothesis through R57's own top-N simulator."""
    panel = load_equity_panel()

    def _score(p, t):
        f = equity_base_features(p, t)
        v = f.get(feature)
        if v is None:
            return np.full(p["tr"].shape[0], np.nan)
        return sign * v

    res = K.run_topn(panel, _score, r59.CADENCE, r59.HORIZON,
                     first_date=r59.DISCOVERY_START)
    return {"state": "MEASURED", "asset_class": r59.AC_US_EQUITY,
            "feature": feature, "sign": sign, "label": label,
            "evaluator": "alpha_agent.r57.engine.run_topn",
            "layers": {l: K.layer_stats(res, l) for l in ("D", "V", "L")}}


# --------------------------------------------------------------------------- #
# Gate - one verdict rule for every engine
# --------------------------------------------------------------------------- #
def gate(result: dict, *, prior_burden: int, family_tests: int = 1) -> dict:
    """Apply the shared qualification gate to a measured result.

    The lockbox layer decides. A candidate must show a positive, material,
    stable, statistically credible lockbox result whose sign was already there
    in validation - and it is charged for the search that produced it. The
    burden multiplier is the honest cost of having looked at 700+ prior
    hypotheses: a nominal p is divided down by the number of tests the estate
    has run in this family, which is what R58 approximated with a hand-copied
    constant.
    """
    layers = result.get("layers") or {}
    L = layers.get("L") or {}
    V = layers.get("V") or {}

    # Futures stats and equity stats report under different names because the
    # books are different objects (a dollar-PnL book has a Sharpe, a top-N
    # book has an excess return); both are read here so one gate serves both.
    l_t = L.get("t_net") if "t_net" in L else L.get("t_net_excess")
    v_t = V.get("t_net") if "t_net" in V else V.get("t_net_excess")
    l_p = L.get("p_one_sided")
    l_material = (L.get("net_sharpe") if "net_sharpe" in L
                  else L.get("ann_net_excess"))
    v_material = (V.get("net_sharpe") if "net_sharpe" in V
                  else V.get("ann_net_excess"))
    # Prefer EFFECTIVE observations when the evaluator reports them. An
    # overlapping-window book's raw period count is not its sample size, and
    # applying the floor to the raw count let a book with ~14 independent
    # observations pass a floor of 36.
    obs = (L.get("effective_observations") if "effective_observations" in L
           else (L.get("days") if "days" in L else L.get("periods")))
    raw_obs = L.get("days") if "days" in L else L.get("periods")

    floor = r59.GATE_MATERIALITY if "ann_net_excess" in L else 0.40
    # A material validation result, not merely a positive one. An effect that
    # is ~0 in validation and large in the lockbox is period-specific, and
    # "v > 0" waves through a validation return of 0.2%.
    v_floor = 0.25 * floor
    checks = {
        "has_lockbox_observations": bool(obs) and int(obs) >= (
            30 if "days" in L else r59.OBS_FLOOR),
        "lockbox_material": (l_material is not None
                             and float(l_material) >= floor),
        "validation_same_sign": (v_material is not None and l_material is not None
                                 and float(v_material) > 0
                                 and float(l_material) > 0),
        "validation_material": (v_material is not None
                                and float(v_material) >= v_floor),
        "lockbox_t_positive": l_t is not None and float(l_t) > 0,
    }
    # Burden-corrected significance: Bonferroni over the family's own tests
    # times the estate's prior search in the same family.
    denom = max(1, int(prior_burden) + int(family_tests))
    adj_p = None if l_p is None else min(1.0, float(l_p) * denom)
    checks["burden_corrected_significant"] = (adj_p is not None
                                              and adj_p <= r59.BH_Q)

    failed = [k for k, v in checks.items() if not v]
    qualified = not failed
    return {
        "qualified": qualified,
        "outcome": r59.HO_QUALIFIED if qualified else r59.HO_NO_ALPHA_EVIDENCE,
        "checks": checks,
        "failed_gates": failed,
        "lockbox_t": l_t, "lockbox_p_one_sided": l_p,
        "burden_denominator": denom,
        "burden_corrected_p": adj_p,
        "lockbox_materiality": l_material,
        "validation_materiality": v_material,
        "lockbox_observations": obs,
        "lockbox_raw_periods": raw_obs,
        "overlap_factor": L.get("overlap_factor"),
        "validation_materiality_floor": v_floor,
    }
