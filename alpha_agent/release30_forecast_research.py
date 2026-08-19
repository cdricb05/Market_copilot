"""alpha_agent/release30_forecast_research.py - Release 30 walk-forward tournament.

RESEARCH ONLY. Assembles the point-in-time dataset from
``release30_panel``, runs a bounded, reproducible model tournament under strict
walk-forward evaluation, derives ensemble weights and forecast uncertainty from
that evidence, and emits a FROZEN model artifact for the pure-stdlib operational
kernel ``engine/return_forecast.py``.

What this module may not do, by design:

* promote a model, write to any operational store, or touch a portfolio;
* let the TEST block influence any hyper-parameter, weight or threshold;
* use a random train/test split, a future normalisation, or a future label.

The evaluation contract:

    TRAIN   dates strictly before the training cut
    (embargo of ceil(horizon / STEP_DAYS) decision dates)
    VALID   the next block - hyper-parameters and ensemble weights are chosen HERE
    (embargo again)
    TEST    the next block - touched once, for reporting only

Every reported statistic is computed on TEST unless its name says VALID.
"""
from __future__ import annotations

import math
import warnings
from dataclasses import dataclass, field

import numpy as np

from . import release30_models as rm
from . import release30_panel as rp
from . import stage24_pit_fundamental as _s24

RESEARCH_VERSION = "release30_forecast_research.v1"

#: Book simulation constants. The cost rate MIRRORS the canonical desk cost owner
#: (``api.paper_trading_desk.COST_BPS_PER_SIDE`` = 12.5). It is declared as a
#: literal here so the research lane never imports the operational API package;
#: ``tests/test_release30_return_forecast.py`` asserts the two stay equal.
COST_BPS_PER_SIDE = 12.5
COST_RATE_PER_SIDE = COST_BPS_PER_SIDE / 10000.0

#: Long-only book size used for the economic comparison. Mirrors the canonical
#: primary book size (``api.multi_horizon_engine.BOOK_SIZES[0]``).
BOOK_N = 25

#: Walk-forward geometry, in decision dates.
INITIAL_TRAIN = 96
VALID_BLOCK = 12
TEST_BLOCK = 24
FOLD_STEP = 24


# --------------------------------------------------------------------------- #
# Dataset
# --------------------------------------------------------------------------- #
@dataclass
class CrossSection:
    t: int
    date: str
    cols: np.ndarray             # (n,) symbol indices into the price panel
    X: np.ndarray                # (n, F) rank-normalised features
    raw: dict                    # feature name -> raw values (n,)
    labels: dict                 # horizon -> (n,) forward total return
    truncated: dict              # horizon -> (n,) bool, delisted inside window
    adv: np.ndarray              # (n,) 20-session average dollar volume
    has_fundamentals: np.ndarray  # (n,) bool


@dataclass
class Dataset:
    feature_names: tuple
    sections: list = field(default_factory=list)
    diagnostics: dict = field(default_factory=dict)
    panel_source: dict = field(default_factory=dict)

    @property
    def dates(self) -> list:
        return [s.date for s in self.sections]


def rank_normalise(v: np.ndarray) -> np.ndarray:
    """Cross-sectional rank transform onto [-0.5, +0.5]; missing -> 0 (neutral).

    Rank rather than z-score because owned factor distributions are heavy-tailed
    and a single outlier must not be able to dominate a cross-section. The
    transform uses ONLY this date's values, so it can carry no future
    information.
    """
    out = np.zeros(v.shape[0], dtype=np.float64)
    ok = np.isfinite(v)
    n = int(ok.sum())
    if n < 2:
        return out
    order = np.argsort(np.argsort(v[ok], kind="stable"), kind="stable")
    out[ok] = (order + 0.5) / n - 0.5
    return out


def build_dataset(panel: rp.PricePanel, *, horizons=rp.HORIZONS,
                  step_days: int = rp.STEP_DAYS,
                  with_fundamentals: bool = False,
                  store=None, bridge=None,
                  first_index: int | None = None,
                  last_index: int | None = None,
                  require_forward: bool = True) -> Dataset:
    """Assemble every decision-date cross-section.

    ``require_forward`` is False only for the CURRENT decision date, where no
    forward window exists yet - that is a forecast, not a training row, and it is
    never mixed into an evaluation sample.
    """
    feat_names = (rp.PRICE_FEATURE_NAMES
                  + (rp.FUNDAMENTAL_FEATURE_NAMES if with_fundamentals else ()))
    ds = Dataset(feature_names=feat_names, panel_source=dict(panel.source))
    max_h = max(list(horizons) + [step_days])
    lo = rp.MIN_HISTORY if first_index is None else int(first_index)
    hi = (panel.n_dates - 1 - (max_h if require_forward else 0)
          if last_index is None else int(last_index))
    diag = {"dates_attempted": 0, "dates_kept": 0, "rows": 0,
            "rows_with_fundamentals": 0, "label_truncated_rows": 0,
            "cik_lookups": 0, "annual_records": 0}

    for t in range(lo, hi + 1, int(step_days)):
        diag["dates_attempted"] += 1
        as_of = panel.iso(t)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            f = rp.price_features_at(panel, t)
        adv = f["_adv_dollar"]
        elig = panel.member[t] & np.isfinite(panel.close[t]) & (adv >= rp.MIN_ADV_DOLLAR)
        for name in rp.PRICE_FEATURE_NAMES:
            elig &= np.isfinite(f[name])
        labels: dict = {}
        truncated: dict = {}
        for h in list(horizons) + [step_days]:
            fwd, tr = rp.forward_returns_at(panel, t, h)
            labels[h] = fwd
            truncated[h] = tr
            if require_forward:
                elig &= np.isfinite(fwd)
        cols = np.nonzero(elig)[0]
        if cols.shape[0] < rp.MIN_CROSS_SECTION:
            continue

        raw = {n: f[n][cols] for n in rp.PRICE_FEATURE_NAMES}
        has_fund = np.zeros(cols.shape[0], dtype=bool)
        if with_fundamentals:
            fund_cols = {n: np.full(cols.shape[0], np.nan)
                         for n in rp.FUNDAMENTAL_FEATURE_NAMES}
            fund_as_of = _s24.pit_as_of(as_of)
            for i, c in enumerate(cols):
                sym = str(panel.symbols[c])
                cik = bridge.cik_for(sym) if bridge is not None else None
                if cik is None:
                    continue
                diag["cik_lookups"] += 1
                rec = _s24.annual_record(store, cik, fund_as_of)
                if rec is None:
                    continue
                diag["annual_records"] += 1
                vals = rp.fundamental_values(rec)
                any_val = False
                for n, v in vals.items():
                    if v is not None:
                        fund_cols[n][i] = v
                        any_val = True
                has_fund[i] = any_val
            raw.update(fund_cols)

        X = np.column_stack([rank_normalise(raw[n]) for n in feat_names])
        ds.sections.append(CrossSection(
            t=t, date=as_of, cols=cols, X=X, raw=raw,
            labels={h: labels[h][cols] for h in labels},
            truncated={h: truncated[h][cols] for h in truncated},
            adv=adv[cols], has_fundamentals=has_fund))
        diag["dates_kept"] += 1
        diag["rows"] += int(cols.shape[0])
        diag["rows_with_fundamentals"] += int(has_fund.sum())
        diag["label_truncated_rows"] += int(truncated[max(horizons)][cols].sum())

    ds.diagnostics = diag
    return ds


def restrict_to_fundamental_coverage(ds: Dataset) -> Dataset:
    """The matched sub-sample on which the fundamental family is defined.

    Comparing a fundamental-augmented model with a price-only model on DIFFERENT
    rows would confound "better forecast" with "different sample". Every
    fundamental comparison in this release therefore runs on this restricted
    dataset for BOTH sides.
    """
    out = Dataset(feature_names=ds.feature_names, panel_source=dict(ds.panel_source))
    kept = 0
    for s in ds.sections:
        m = s.has_fundamentals
        if int(m.sum()) < rp.MIN_CROSS_SECTION:
            continue
        out.sections.append(CrossSection(
            t=s.t, date=s.date, cols=s.cols[m],
            X=np.column_stack([rank_normalise(s.raw[n][m])
                               for n in ds.feature_names]),
            raw={n: s.raw[n][m] for n in s.raw},
            labels={h: s.labels[h][m] for h in s.labels},
            truncated={h: s.truncated[h][m] for h in s.truncated},
            adv=s.adv[m], has_fundamentals=m[m]))
        kept += int(m.sum())
    out.diagnostics = {"rows": kept, "dates_kept": len(out.sections),
                       "restriction": "FUNDAMENTAL_COVERAGE_MATCHED_SUBSAMPLE"}
    return out


# --------------------------------------------------------------------------- #
# Targets
# --------------------------------------------------------------------------- #
def excess_target(labels: np.ndarray) -> np.ndarray:
    """Forward EXCESS return: the label minus its own cross-sectional mean.

    The cross-section cannot forecast the market's level, so the market level is
    removed from the target rather than silently attributed to the model. What
    remains is exactly the quantity a long-only selector can act on.
    """
    m = float(np.nanmean(labels)) if labels.size else 0.0
    return labels - m


def standardised_target(labels: np.ndarray) -> np.ndarray:
    y = excess_target(labels)
    s = float(np.nanstd(y, ddof=1)) if y.size > 1 else 0.0
    return y / s if (math.isfinite(s) and s > 0) else np.zeros_like(y)


# --------------------------------------------------------------------------- #
# Statistics
# --------------------------------------------------------------------------- #
def _rank(v: np.ndarray) -> np.ndarray:
    return np.argsort(np.argsort(v, kind="stable"), kind="stable").astype(np.float64)


def corr(a: np.ndarray, b: np.ndarray) -> float:
    if a.size < 3:
        return float("nan")
    a = a - a.mean()
    b = b - b.mean()
    d = float(np.sqrt((a * a).sum() * (b * b).sum()))
    return float((a * b).sum() / d) if d > 0 else float("nan")


def rank_corr(a: np.ndarray, b: np.ndarray) -> float:
    return corr(_rank(a), _rank(b))


def newey_west_t(x: np.ndarray, lags: int) -> float:
    """t-statistic of a mean with a Newey-West correction.

    Decision dates are struck every ``STEP_DAYS`` sessions while horizons run
    longer, so consecutive per-date statistics OVERLAP. A naive t-statistic would
    treat overlapping observations as independent and overstate significance;
    ``lags`` is set from the overlap the horizon actually implies.
    """
    x = np.asarray([v for v in x if math.isfinite(v)], dtype=np.float64)
    n = x.size
    if n < 4:
        return float("nan")
    d = x - x.mean()
    var = float((d * d).sum() / n)
    for l in range(1, min(int(lags), n - 1) + 1):
        cov = float((d[l:] * d[:-l]).sum() / n)
        var += 2.0 * (1.0 - l / (int(lags) + 1.0)) * cov
    if var <= 0:
        return float("nan")
    return float(x.mean() / math.sqrt(var / n))


def max_drawdown(returns: np.ndarray) -> float:
    if returns.size == 0:
        return float("nan")
    eq = np.cumprod(1.0 + returns)
    peak = np.maximum.accumulate(eq)
    return float((eq / peak - 1.0).min())


# --------------------------------------------------------------------------- #
# Walk-forward machinery
# --------------------------------------------------------------------------- #
def embargo_dates(horizon: int, step_days: int = rp.STEP_DAYS) -> int:
    """Decision dates dropped between blocks so no training label overlaps the
    block being scored. A 60-session horizon struck every 21 sessions overlaps
    three decisions, so three are dropped."""
    return int(math.ceil(horizon / float(step_days)))


def folds(n_dates: int, horizon: int, *, initial_train=INITIAL_TRAIN,
          valid=VALID_BLOCK, test=TEST_BLOCK, step=FOLD_STEP) -> list:
    e = embargo_dates(horizon)
    out = []
    train_end = int(initial_train)
    while train_end + e + valid + e + test <= n_dates:
        v0 = train_end + e
        t0 = v0 + valid + e
        out.append({"train": (0, train_end), "valid": (v0, v0 + valid),
                    "test": (t0, t0 + test), "embargo_dates": e})
        train_end += int(step)
    return out


def stack_block(ds: Dataset, lo: int, hi: int, horizon: int) -> tuple:
    Xs, ys = [], []
    for s in ds.sections[lo:hi]:
        Xs.append(s.X)
        ys.append(standardised_target(s.labels[horizon]))
    if not Xs:
        return np.zeros((0, len(ds.feature_names))), np.zeros(0)
    return np.vstack(Xs), np.concatenate(ys)


#: The tournament grid. Deliberately small: every extra candidate is another
#: chance for a noisy winner, and the selection budget is itself a source of
#: overfitting. Each entry is (model_id, builder, hyper-parameter grid).
def model_grid(feature_names: tuple) -> list:
    has_fund = "s25_operating_profitability" in feature_names
    grid = [
        {"model_id": "baseline_momentum_leg", "kind": "fixed",
         "spec": rm.rank_blend_spec({"mom_6_1": 1.0}),
         "role": "BENCHMARK",
         "note": "The operational champion's momentum leg, unchanged."},
        {"model_id": "ridge", "kind": "fit", "learner": "ridge",
         "role": "CANDIDATE",
         "params": [{"alpha": a} for a in (3.0, 10.0, 30.0, 100.0, 300.0)]},
        {"model_id": "gbrt", "kind": "fit", "learner": "gbrt",
         "role": "CANDIDATE",
         "params": [{"n_trees": 120, "max_depth": 2, "learning_rate": 0.03},
                    {"n_trees": 240, "max_depth": 2, "learning_rate": 0.02},
                    {"n_trees": 120, "max_depth": 3, "learning_rate": 0.02}]},
        {"model_id": "extra_trees", "kind": "fit", "learner": "extra_trees",
         "role": "CANDIDATE",
         "params": [{"n_trees": 100, "max_depth": 4, "min_leaf": 400},
                    {"n_trees": 200, "max_depth": 5, "min_leaf": 600}]},
    ]
    if has_fund:
        grid.insert(1, {
            "model_id": "baseline_operational_blend_pit", "kind": "fixed",
            "role": "BENCHMARK",
            "spec": rm.rank_blend_spec({
                "mom_6_1": 0.5,
                "fcf_to_assets": 0.25, "operating_accruals": -0.25}),
            "note": ("Point-in-time structural reconstruction of the operational "
                     "champion fundamental_momentum_50_50_v1: half momentum, half "
                     "the composite_sn cash-flow/accrual pair. Sector "
                     "neutralisation is deliberately absent - the canonical PIT "
                     "sector owner declares its snapshot inadmissible for signal "
                     "construction.")})
        grid.append({
            "model_id": "s25_operating_profitability", "kind": "fixed",
            "role": "COMPONENT_ALPHA",
            "spec": rm.rank_blend_spec({"s25_operating_profitability": 1.0}),
            "note": "The Stage-25 frozen research challenger, standing alone."})
    return grid


def fit_learner(entry: dict, X: np.ndarray, y: np.ndarray, params: dict,
         seed: int) -> dict:
    learner = entry["learner"]
    if learner == "ridge":
        return rm.fit_ridge(X, y, **params)
    if learner == "gbrt":
        return rm.fit_gbrt(X, y, seed=seed, **params)
    if learner == "extra_trees":
        return rm.fit_extra_trees(X, y, seed=seed, **params)
    raise ValueError("unknown learner %r" % (learner,))


def _score_block(spec: dict, ds: Dataset, lo: int, hi: int,
                 horizon: int) -> dict:
    """Per-date statistics for one model over one block of decision dates."""
    ics, rics, hits = [], [], []
    preds, ys = [], []
    for s in ds.sections[lo:hi]:
        p = rm.standardise(rm.predict(spec, s.X, ds.feature_names))
        y = excess_target(s.labels[horizon])
        ok = np.isfinite(y) & np.isfinite(p)
        if int(ok.sum()) < rp.MIN_CROSS_SECTION:
            continue
        ics.append(corr(p[ok], y[ok]))
        rics.append(rank_corr(p[ok], y[ok]))
        hits.append(float(np.mean(np.sign(p[ok]) == np.sign(y[ok]))))
        preds.append(p[ok])
        ys.append(y[ok])
    if not preds:
        return {"n_dates": 0}
    P = np.concatenate(preds)
    Y = np.concatenate(ys)
    slope = float((P * Y).sum() / (P * P).sum()) if (P * P).sum() > 0 else 0.0
    resid = Y - slope * P
    lags = max(0, embargo_dates(horizon) - 1)
    return {
        "n_dates": len(ics), "n_rows": int(P.size),
        "ic_mean": float(np.mean(ics)), "ic_t": newey_west_t(np.array(ics), lags),
        "rank_ic_mean": float(np.mean(rics)),
        "rank_ic_t": newey_west_t(np.array(rics), lags),
        "rank_ic_std": float(np.std(rics, ddof=1)) if len(rics) > 1 else float("nan"),
        "directional_accuracy": float(np.mean(hits)),
        "calibration_slope": slope,
        "residual_sigma": float(np.std(resid, ddof=1)) if resid.size > 1 else float("nan"),
        "mae": float(np.mean(np.abs(Y - slope * P))),
        "rmse": float(np.sqrt(np.mean((Y - slope * P) ** 2))),
        "ic_series": [float(v) for v in ics],
        "rank_ic_series": [float(v) for v in rics],
    }


def _bucket_returns(spec: dict, ds: Dataset, lo: int, hi: int,
                    horizon: int) -> dict:
    top_dec, top_qui, top_n, universe = [], [], [], []
    for s in ds.sections[lo:hi]:
        p = rm.predict(spec, s.X, ds.feature_names)
        y = s.labels[horizon]
        ok = np.isfinite(y) & np.isfinite(p)
        if int(ok.sum()) < rp.MIN_CROSS_SECTION:
            continue
        pi, yi = p[ok], y[ok]
        order = np.argsort(-pi, kind="stable")
        n = pi.size
        universe.append(float(yi.mean()))
        top_dec.append(float(yi[order[:max(1, n // 10)]].mean()))
        top_qui.append(float(yi[order[:max(1, n // 5)]].mean()))
        top_n.append(float(yi[order[:min(BOOK_N, n)]].mean()))
    if not universe:
        return {}
    u = np.array(universe)
    return {
        "universe_mean_return": float(u.mean()),
        "top_decile_return": float(np.mean(top_dec)),
        "top_decile_excess": float(np.mean(np.array(top_dec) - u)),
        "top_quintile_return": float(np.mean(top_qui)),
        "top_quintile_excess": float(np.mean(np.array(top_qui) - u)),
        "top_%d_return" % BOOK_N: float(np.mean(top_n)),
        "top_%d_excess" % BOOK_N: float(np.mean(np.array(top_n) - u)),
    }


def book_simulation(spec: dict, ds: Dataset, lo: int, hi: int, *,
                    book_n: int = BOOK_N,
                    step_days: int = rp.STEP_DAYS) -> dict:
    """A long-only top-N equal-weight paper book over one block of dates.

    Gross return for a period is the equal-weight mean of the held names'
    realised ``step_days`` forward returns. Cost is charged on the actual weight
    change at the canonical per-side rate, so a model that reshuffles its book
    every period pays for it and a persistent model does not. This is the
    economic comparison; the IC table is the statistical one.
    """
    prev: dict = {}
    gross, net, turn = [], [], []
    per_period = []
    for s in ds.sections[lo:hi]:
        p = rm.predict(spec, s.X, ds.feature_names)
        y = s.labels[step_days]
        ok = np.isfinite(y) & np.isfinite(p)
        if int(ok.sum()) < book_n:
            continue
        idx = np.nonzero(ok)[0]
        order = idx[np.argsort(-p[idx], kind="stable")][:book_n]
        w = 1.0 / book_n
        cur = {int(s.cols[i]): w for i in order}
        names = set(cur) | set(prev)
        traded = sum(abs(cur.get(k, 0.0) - prev.get(k, 0.0)) for k in names)
        cost = traded * COST_RATE_PER_SIDE
        g = float(y[order].mean())
        gross.append(g)
        net.append(g - cost)
        turn.append(traded / 2.0)
        per_period.append({"date": s.date, "gross": g, "cost": cost,
                           "net": g - cost, "one_way_turnover": traded / 2.0})
        prev = cur
    if not net:
        return {"periods": 0}
    ret = np.array(net)
    ppy = 252.0 / float(step_days)
    sd = float(ret.std(ddof=1)) if ret.size > 1 else float("nan")
    return {
        "periods": int(ret.size),
        "gross_mean_period": float(np.mean(gross)),
        "net_mean_period": float(ret.mean()),
        "cost_mean_period": float(np.mean(gross) - ret.mean()),
        "annualised_net_return": float((1.0 + ret.mean()) ** ppy - 1.0),
        "annualised_volatility": (float(sd * math.sqrt(ppy))
                                  if math.isfinite(sd) else float("nan")),
        "information_ratio": (float(ret.mean() / sd * math.sqrt(ppy))
                              if (math.isfinite(sd) and sd > 0) else float("nan")),
        "max_drawdown": max_drawdown(ret),
        "mean_one_way_turnover": float(np.mean(turn)),
        "net_series": [float(v) for v in ret],
        "per_period": per_period,
    }


#: The adaptive candidate's model id. It is assembled fold by fold from that
#: fold's VALIDATION evidence and then scored on that fold's untouched TEST
#: block, so the ensemble is measured the same way every other entrant is.
ADAPTIVE_ID = "adaptive_ensemble"


def run_tournament(ds: Dataset, *, horizons=rp.HORIZONS, seed: int = 30) -> dict:
    """The full walk-forward tournament for one dataset.

    The loop is FOLD-major, not model-major, because the adaptive ensemble is
    itself a competitor: its members, hyper-parameters and weights all come from
    one fold's validation block, and only then is it shown that fold's test
    block. A model-major loop would have had to build the ensemble after every
    test block had already been read.
    """
    grid = model_grid(ds.feature_names)
    roles = {e["model_id"]: e["role"] for e in grid}
    roles[ADAPTIVE_ID] = "ADAPTIVE_CANDIDATE"
    entries = {e["model_id"]: e for e in grid}
    n = len(ds.sections)
    result = {"n_decision_dates": n, "feature_names": list(ds.feature_names),
              "dates": [s.date for s in ds.sections],
              "horizons": list(horizons), "adaptive_model_id": ADAPTIVE_ID,
              "by_horizon": {}}
    for h in horizons:
        fl = folds(n, h)
        if not fl:
            result["by_horizon"][str(h)] = {"folds": 0,
                                            "reason": "INSUFFICIENT_DECISION_DATES"}
            continue
        acc: dict = {}
        fold_weights = []
        pooled: dict = {}

        def _record(mid, spec, f):
            ts = _score_block(spec, ds, f["test"][0], f["test"][1], h)
            if not ts.get("n_dates"):
                return
            a = acc.setdefault(mid, {"test": [], "valid": [], "books": [],
                                     "buckets": [], "picks": []})
            a["test"].append(ts)
            a["books"].append(book_simulation(spec, ds, f["test"][0], f["test"][1]))
            a["buckets"].append(_bucket_returns(spec, ds, f["test"][0],
                                                f["test"][1], h))

        for k, f in enumerate(fl):
            specs: dict = {}
            vstats: dict = {}
            for entry in grid:
                mid = entry["model_id"]
                a = acc.setdefault(mid, {"test": [], "valid": [], "books": [],
                                         "buckets": [], "picks": []})
                if entry["kind"] == "fixed":
                    specs[mid] = entry["spec"]
                    vstats[mid] = _score_block(entry["spec"], ds, f["valid"][0],
                                               f["valid"][1], h)
                    a["picks"].append(None)
                else:
                    Xtr, ytr = stack_block(ds, f["train"][0], f["train"][1], h)
                    best = None
                    for params in entry["params"]:
                        cand = fit_learner(entry, Xtr, ytr, params, seed + 17 * k)
                        vs = _score_block(cand, ds, f["valid"][0], f["valid"][1], h)
                        score = vs.get("rank_ic_mean", float("nan"))
                        if not math.isfinite(score):
                            continue
                        if best is None or score > best[0]:
                            best = (score, params, vs)
                    if best is None:
                        continue
                    a["picks"].append(best[1])
                    vstats[mid] = best[2]
                    # Refit on TRAIN + VALID with the VALID-chosen
                    # hyper-parameters. TEST has still not been read.
                    Xf, yf = stack_block(ds, f["train"][0], f["valid"][1], h)
                    specs[mid] = fit_learner(entry, Xf, yf, best[1],
                                             seed + 17 * k)
                a["valid"].append(vstats.get(mid) or {})
                pooled.setdefault(mid, []).extend(
                    (vstats.get(mid) or {}).get("rank_ic_series") or [])

            ew = ensemble_from_validation(pooled, roles, h)
            fold_weights.append(dict(ew, fold=k,
                                     train_end=ds.sections[f["train"][1] - 1].date,
                                     valid_end=ds.sections[f["valid"][1] - 1].date))
            members = [{"model_id": mid, "weight": float(w), "spec": specs[mid]}
                       for mid, w in sorted(ew["weights"].items())
                       if w > 0 and mid in specs]
            for mid, spec in specs.items():
                _record(mid, spec, f)
            if members:
                _record(ADAPTIVE_ID, {"kind": rm.KIND_ENSEMBLE,
                                      "members": members}, f)
                acc[ADAPTIVE_ID]["valid"].append(
                    {"rank_ic_mean": sum(float(m["weight"])
                                         * vstats[m["model_id"]]["rank_ic_mean"]
                                         for m in members),
                     "rank_ic_t": float("nan")})

        per_model = {}
        for mid, a in acc.items():
            if not a["test"]:
                continue
            entry = entries.get(mid, {"model_id": ADAPTIVE_ID,
                                      "role": "ADAPTIVE_CANDIDATE",
                                      "note": "Walk-forward weighted ensemble."})
            per_model[mid] = _aggregate(entry, a["test"], a["valid"], a["books"],
                                        a["buckets"], a["picks"], h)
        result["by_horizon"][str(h)] = {
            "folds": len(fl), "fold_geometry": fl,
            "models": per_model,
            "ensemble_weights_by_fold": fold_weights,
            "ensemble": fold_weights[-1] if fold_weights else {},
            "selected_hyperparameters": {mid: a["picks"]
                                         for mid, a in acc.items()},
        }
    return result


def _aggregate(entry, test_stats, valid_stats, books, buckets, picks,
               horizon) -> dict:
    def _m(key, rows):
        vals = [r[key] for r in rows if math.isfinite(r.get(key, float("nan")))]
        return float(np.mean(vals)) if vals else float("nan")

    ric = np.concatenate([np.array(r["rank_ic_series"]) for r in test_stats])
    ic = np.concatenate([np.array(r["ic_series"]) for r in test_stats])
    lags = max(0, embargo_dates(horizon) - 1)
    net = np.concatenate([np.array(b["net_series"]) for b in books
                          if b.get("periods")]) if books else np.zeros(0)
    ppy = 252.0 / float(rp.STEP_DAYS)
    sd = float(net.std(ddof=1)) if net.size > 1 else float("nan")
    return {
        "model_id": entry["model_id"], "role": entry["role"],
        "note": entry.get("note"),
        "test": {
            "n_dates": int(sum(r["n_dates"] for r in test_stats)),
            "n_rows": int(sum(r["n_rows"] for r in test_stats)),
            "ic_mean": float(ic.mean()), "ic_t": newey_west_t(ic, lags),
            "rank_ic_mean": float(ric.mean()),
            "rank_ic_t": newey_west_t(ric, lags),
            "rank_ic_std": float(ric.std(ddof=1)) if ric.size > 1 else float("nan"),
            "rank_ic_positive_fraction": float(np.mean(ric > 0)),
            "directional_accuracy": _m("directional_accuracy", test_stats),
            "calibration_slope": _m("calibration_slope", test_stats),
            "residual_sigma": _m("residual_sigma", test_stats),
            "mae": _m("mae", test_stats), "rmse": _m("rmse", test_stats),
        },
        "valid": {
            "rank_ic_mean": _m("rank_ic_mean", valid_stats),
            "rank_ic_t": _m("rank_ic_t", valid_stats),
            "calibration_slope": _m("calibration_slope", valid_stats),
            "residual_sigma": _m("residual_sigma", valid_stats),
        },
        "buckets": {k: float(np.mean([b[k] for b in buckets if k in b]))
                    for k in (buckets[0] if buckets else {})},
        "book": {
            "periods": int(net.size),
            "net_mean_period": float(net.mean()) if net.size else float("nan"),
            "annualised_net_return": (float((1.0 + net.mean()) ** ppy - 1.0)
                                      if net.size else float("nan")),
            "annualised_volatility": (float(sd * math.sqrt(ppy))
                                      if math.isfinite(sd) else float("nan")),
            "information_ratio": (float(net.mean() / sd * math.sqrt(ppy))
                                  if (math.isfinite(sd) and sd > 0) else float("nan")),
            "max_drawdown": max_drawdown(net),
            "mean_one_way_turnover": _m("mean_one_way_turnover", books),
            "cost_mean_period": _m("cost_mean_period", books),
            "net_series": [float(v) for v in net],
        },
        "selected_hyperparameters": [p for p in picks if p is not None],
    }


#: Normal-tail multiplier for the 5 % quantile, matching
#: ``engine.zero_base_allocator.Z05``.
Z05 = 1.6448536269514722

#: Bounds on the calibrated covariance risk price. A measured value outside this
#: range is reported AND clamped: an unbounded gamma derived from a small mean
#: over a small variance is a numerical artefact, not an appetite.
GAMMA_MIN = 0.5
GAMMA_MAX = 50.0


def calibrate_risk_prices(spec: dict, ds: Dataset, horizon: int, *,
                          book_n: int = BOOK_N) -> dict:
    """Derive the allocator's risk prices from VALIDATION evidence only.

    ``gamma`` is the risk price at which a fully-invested diversified book is
    exactly marginal: for a mean-variance investor the first-order condition is
    ``mu_p = gamma * sigma_p^2``, so ``gamma = mu_p / sigma_p^2`` measured on the
    candidate's own realised validation book. This is derived, not chosen, and it
    is why the allocator's cash weight reflects the opportunity set rather than
    somebody's taste.

    ``phi`` is set equal to ``gamma``: there is no evidence that forecast error
    should be priced differently from realised covariance risk, so it is priced
    the same. The two stay SEPARATE parameters so a future calibration can move
    one without the other.

    ``delta`` comes from the measured asymmetry of the walk-forward residuals.
    ``tail_factor`` is how much fatter the empirical 5 % left tail is than a
    normal one; ``delta = max(0, tail_factor - 1)``. A symmetric residual
    distribution therefore charges NOTHING extra, which is the only way to avoid
    double counting a tail the variance term has already priced.
    """
    fl = folds(len(ds.sections), horizon)
    excess, resid = [], []
    for f in fl:
        for s in ds.sections[f["valid"][0]:f["valid"][1]]:
            p = rm.predict(spec, s.X, ds.feature_names)
            y = s.labels[rp.STEP_DAYS]
            ok = np.isfinite(y) & np.isfinite(p)
            if int(ok.sum()) < book_n:
                continue
            idx = np.nonzero(ok)[0]
            order = idx[np.argsort(-p[idx], kind="stable")][:book_n]
            excess.append(float(y[order].mean() - y[ok].mean()))
            ps = rm.standardise(p)
            yy = excess_target(s.labels[horizon])
            m = np.isfinite(ps) & np.isfinite(yy)
            resid.append((ps[m], yy[m]))
    out = {"basis": "WALK_FORWARD_VALIDATION_BLOCKS",
           "book_n": int(book_n), "validation_periods": len(excess)}
    if len(excess) < 8:
        out.update({"state": "UNCALIBRATED",
                    "reason": "INSUFFICIENT_VALIDATION_PERIODS"})
        return out
    ex = np.array(excess, dtype=np.float64)
    mean_x, var_x = float(ex.mean()), float(ex.var(ddof=1))
    raw_gamma = (mean_x / var_x) if (var_x > 0 and mean_x > 0) else GAMMA_MAX
    gamma = float(min(GAMMA_MAX, max(GAMMA_MIN, raw_gamma)))
    P = np.concatenate([a for a, _ in resid])
    Y = np.concatenate([b for _, b in resid])
    slope = float((P * Y).sum() / (P * P).sum()) if (P * P).sum() > 0 else 0.0
    r = Y - slope * P
    sigma = float(r.std(ddof=1))
    q05 = float(np.quantile(r, 0.05))
    tail = float(q05 / (-Z05 * sigma)) if sigma > 0 else 1.0
    out.update({
        "state": "CALIBRATED",
        "validation_book_mean_excess_per_period": mean_x,
        "validation_book_variance_per_period": var_x,
        "raw_gamma": raw_gamma,
        "gamma_clamped": bool(raw_gamma != gamma),
        "gamma_bounds": [GAMMA_MIN, GAMMA_MAX],
        "risk_aversion_gamma": gamma,
        "uncertainty_aversion_phi": gamma,
        "phi_rationale": ("priced equal to gamma - no evidence supports pricing "
                          "forecast error differently from realised covariance "
                          "risk; the parameters stay separate so that can change"),
        "residual_sigma": sigma,
        "residual_q05": q05,
        "downside_tail_factor": tail,
        "downside_aversion_delta": float(max(0.0, tail - 1.0)),
        "delta_rationale": ("max(0, tail_factor - 1): a symmetric residual "
                            "distribution charges nothing extra, so the variance "
                            "term is never double counted"),
        "gamma_formula": "mean / variance of the validation book's excess return",
    })
    return out


#: The reliability bar at which a component's measured skill is reported as
#: statistically distinguishable from zero. It is a DIAGNOSTIC, not a cliff: the
#: weighting below is continuous, because a hard threshold would hand a component
#: full influence at t = 2.01 and none at t = 1.99 on the strength of noise.
RELIABILITY_REPORT_T = 2.0


def shrunk_ic(ic: float, t: float) -> float:
    """Validation rank IC shrunk by its own reliability.

    The factor is the classic reliability ratio ``t^2 / (1 + t^2)``: an estimate
    with t = 1 keeps half of its measured IC, t = 2 keeps 80 %, t = 5 keeps 96 %,
    and a component with no measurable skill keeps nothing. A component whose
    out-of-sample IC is NEGATIVE receives exactly zero - a signal that has been
    wrong out of sample must not be blended in on the strength of its magnitude.
    """
    if not (math.isfinite(ic) and math.isfinite(t)) or ic <= 0:
        return 0.0
    return float(ic * (t * t) / (1.0 + t * t))


def ensemble_from_validation(pooled: dict, roles: dict, horizon: int) -> dict:
    """Ensemble weights from ACCUMULATED validation evidence only.

    ``pooled[model_id]`` is every per-date validation rank IC observed in this
    fold and every EARLIER fold - all of which lie strictly before this fold's
    test block, so nothing here has seen the data it will be judged on. Pooling
    matters: a single twelve-date validation block cannot distinguish a real
    information coefficient from noise, and weighting on that alone would make
    the ensemble a coin toss re-thrown every fold.

    Benchmarks are excluded - the frozen operational champion is the thing being
    measured, not an ingredient. Weights are never hand-picked and never assumed
    equal: equal weights arise only if the accumulated evidence is equal.
    """
    lags = max(0, embargo_dates(horizon) - 1)
    stats: dict = {}
    for mid, series in pooled.items():
        if roles.get(mid) not in ("CANDIDATE", "COMPONENT_ALPHA"):
            continue
        arr = np.array([v for v in series if math.isfinite(v)], dtype=np.float64)
        ic = float(arr.mean()) if arr.size else float("nan")
        t = newey_west_t(arr, lags)
        stats[mid] = {"n_validation_dates": int(arr.size),
                      "rank_ic_mean": ic, "rank_ic_t": t,
                      "reliable": bool(math.isfinite(t) and t >= RELIABILITY_REPORT_T),
                      "shrunk_ic": shrunk_ic(ic, t)}
    total = sum(s["shrunk_ic"] for s in stats.values())
    weights = ({mid: s["shrunk_ic"] / total for mid, s in stats.items()}
               if total > 0 else {mid: 0.0 for mid in stats})
    return {
        "method": "SHRUNK_ACCUMULATED_VALIDATION_RANK_IC",
        "shrinkage": "reliability_ratio_t2_over_1_plus_t2",
        "reliability_report_t": RELIABILITY_REPORT_T,
        "components": stats,
        "weights": weights,
        "zeroed": sorted([mid for mid, w in weights.items() if w == 0.0]),
        "degenerate": total <= 0,
    }
