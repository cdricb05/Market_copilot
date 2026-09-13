r"""alpha_agent.alpha_recovery.implied_correlation_premium - executor for the Alpha Agent mechanism
``IMPLIED_CORRELATION_PREMIUM_INDEX_TIMING_V1``.

Contract: ``research/preregistration/IMPLIED_CORRELATION_PREMIUM_PREREGISTRATION.md``.
One frozen cell: on a monthly grid of SPY sessions the correlation risk premium - Cboe COR3M
implied correlation minus the strictly trailing 63-session average pairwise correlation of
point-in-time S&P 500 members - is z-scored against the strictly prior 36 grid observations. A
HIGH premium z-score is LONG SPY (a low one short) for 21 sessions, entered at the close AFTER the
signal close. Gates run in the preregistered order; the untouched confirmation is read only when
every qualification gate passes, and is also reported, descriptively, split at the boundary
between Cboe's methodology backfill and the published period.

Existing owners only: ``r63.sensitivity.nw_tstat`` / ``bh_fdr`` / ``_max_dd`` and
``intraday_alpha.equal_risk_daily`` / ``incumbent_daily_path``; the z-score is the
``options_surface._z`` formula on the declared grid window. RESEARCH ONLY; ``capital_eligible``
is always False.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from alpha_agent.r63 import sensitivity as S

from . import (MATERIALITY_ANN_NET, MIN_EFFECTIVE_PERIODS, SPY_PROXY_COST_BPS, research_root,
               write_artifact)

CALCULATION_OWNER = "alpha_agent.alpha_recovery.implied_correlation_premium"
MECHANISM_ID = "IMPLIED_CORRELATION_PREMIUM_INDEX_TIMING_V1"
ARTIFACT_NAME = "implied_correlation_premium.json"
PREREGISTRATION = "research/preregistration/IMPLIED_CORRELATION_PREMIUM_PREREGISTRATION.md"

CACHE_SUBDIR = "_data_implied_correlation"
COR3M_FILE = "COR3M_History.csv"
MANIFEST_FILE = "fetch_manifest.json"
MANIFEST_KEY = "COR3M"
PIT_PANEL_NPZ = Path(r"D:\Stock_Prediction_app_data\r57_alpha_discovery\panels\sp500_pit_panel_v1.npz")
PIT_PANEL_META = Path(r"D:\Stock_Prediction_app_data\r57_alpha_discovery\panels\sp500_pit_panel_v1.meta.json")

FROZEN_SIGN = 1.0                      # HIGH premium z -> LONG SPY
GRID_STEP = 21                         # every 21st SPY session from the anchor
HORIZON = 21
ENTRY_DELAY = 1                        # entry at the close AFTER the signal close
RC_LOOKBACK = 63                       # daily returns over sessions t-63..t-1
RC_MIN_NAMES = 100
Z_WINDOW = 36                          # strictly prior grid slots
Z_MIN_OBS = 24
PERIODS_PER_YEAR = 252.0 / HORIZON
COST_PRIMARY_BPS = float(SPY_PROXY_COST_BPS)       # 1 bp per side
COST_LADDER_BPS = (COST_PRIMARY_BPS, 2.0, 5.0)
COST_STRESS_BPS = 5.0

QUALIFICATION = ("2006-01-01", "2018-12-31")
PARTITIONS = {"P1_2006_2012": ("2006-01-01", "2012-12-31"),
              "P2_2013_2018": ("2013-01-01", "2018-12-31")}
CONFIRMATION = ("2019-01-01", "2026-08-31")
PUBLISHED_FROM = "2021-10-01"
CONFIRMATION_ROWS = {"BACKFILL_2019_01_2021_09": ("2019-01-01", "2021-09-30"),
                     "PUBLISHED_2021_10_2026_08": (PUBLISHED_FROM, "2026-08-31")}
MIN_FINITE_SHARE = 0.95
T_FLOOR = 2.0
BH_Q = 0.10
#: The seven index-level implied-state timing tests, inherited at p = 1 (m = 8 with this cell).
INHERITED_NULLS = ("R32|EQUITY_BETA_TIMING", "R32|VOLATILITY_RISK_REGIME",
                   "R35|MARKET_IMPLIED_RISK_PREMIA", "R36|VOL_TERM_EQUITY_TIMING",
                   "R63|VOLATILITY_EXPECTATIONS_IV|h1", "R63|VOLATILITY_EXPECTATIONS_IV|h5",
                   "R63|VOLATILITY_EXPECTATIONS_IV|h21")
SCORER = ("alpha_agent.alpha_recovery.options_surface._z formula (36/24 grid slots) + "
          "alpha_agent.r63.sensitivity.nw_tstat/bh_fdr/_max_dd + "
          "alpha_agent.alpha_recovery.intraday_alpha.equal_risk_daily")


# --------------------------------------------------------------------------- #
# Data
# --------------------------------------------------------------------------- #
def cache_dir() -> Path:
    return research_root() / CACHE_SUBDIR


def load_cor3m(path: Optional[Path] = None) -> pd.Series:
    """The cached Cboe COR3M closes. A missing cache raises FileNotFoundError (DATA_HOLD);
    nothing is ever downloaded here."""
    p = cache_dir() / COR3M_FILE if path is None else Path(path)
    df = pd.read_csv(p)
    s = pd.Series(pd.to_numeric(df["CLOSE"], errors="coerce").to_numpy(dtype=float),
                  index=pd.to_datetime(df["DATE"], format="%m/%d/%Y"))
    return s[~s.index.duplicated(keep="first")].sort_index()


def cache_integrity(cache: Optional[Path] = None) -> dict:
    """sha256 of the cached COR3M bytes against the fetch manifest."""
    d = cache_dir() if cache is None else Path(cache)
    try:
        manifest = json.loads((d / MANIFEST_FILE).read_text(encoding="utf-8"))
        want = str(((manifest or {}).get(MANIFEST_KEY) or {}).get("sha256") or "")
    except (OSError, ValueError):
        want = ""
    try:
        got = hashlib.sha256((d / COR3M_FILE).read_bytes()).hexdigest()
    except OSError:
        got = None
    return {"manifest_sha256": want or None, "file_sha256": got,
            "passes": bool(want) and got == want}


def load_pit_panel(npz: Path = PIT_PANEL_NPZ, meta_path: Path = PIT_PANEL_META) -> dict:
    """The owned R57 point-in-time S&P 500 panel: total-return closes and membership."""
    meta = json.loads(Path(meta_path).read_text(encoding="utf-8"))
    z = np.load(npz, allow_pickle=False)
    return {"tr": z["tr"], "mem": z["mem"], "dates": pd.DatetimeIndex(pd.to_datetime(meta["dates"])),
            "identity": "R57_SP500_PIT_PANEL:%s:%s..%s" % (meta.get("manifest_hash"),
                                                        meta.get("date_start"), meta.get("date_end"))}


def load_spy_total_return() -> pd.Series:
    import norgatedata as nd
    df = nd.price_timeseries("SPY", stock_price_adjustment_setting=nd.StockPriceAdjustmentType.TOTALRETURN,
                             padding_setting=nd.PaddingType.NONE, timeseriesformat="pandas-dataframe")
    return df["Close"].astype(float).sort_index()


# --------------------------------------------------------------------------- #
# Signal (reads no return after any grid session)
# --------------------------------------------------------------------------- #
def average_pairwise_correlation(closes: np.ndarray) -> tuple:
    """Equal-weight mean of the off-diagonal of ``z @ z.T / T`` for names x (T+1) closes; names
    with a non-finite return or zero variance in the window are dropped."""
    c = np.asarray(closes, dtype=np.float64)
    if c.ndim != 2 or c.shape[1] < 3:
        return np.nan, 0
    with np.errstate(divide="ignore", invalid="ignore"):
        r = c[:, 1:] / c[:, :-1] - 1.0
    r = r[np.isfinite(r).all(axis=1)]
    sd = r.std(axis=1) if len(r) else np.array([])
    r, sd = r[sd > 0], sd[sd > 0]
    n = int(len(r))
    if n < RC_MIN_NAMES:
        return np.nan, n
    zz = (r - r.mean(axis=1, keepdims=True)) / sd[:, None]
    cm = zz @ zz.T / r.shape[1]
    return float((cm.sum() - np.trace(cm)) / (n * (n - 1))), n


def realised_correlation(pit: dict, p: int) -> tuple:
    """Average pairwise correlation over the RC_LOOKBACK daily returns ending at panel position
    ``p`` (the session BEFORE the decision), across S&P 500 members on that session."""
    if p < RC_LOOKBACK:
        return np.nan, 0
    members = np.asarray(pit["mem"][:, p]).astype(bool)
    closes = np.asarray(pit["tr"][members, p - RC_LOOKBACK:p + 1], dtype=np.float64)
    return average_pairwise_correlation(closes)


def zscore_prior(x: pd.Series) -> np.ndarray:
    """The ``options_surface._z`` formula (strictly prior mean and sample sd, zero sd -> NaN)
    on the declared grid window: Z_WINDOW prior slots with at least Z_MIN_OBS finite."""
    prior = x.shift(1).rolling(Z_WINDOW, min_periods=Z_MIN_OBS)
    return ((x - prior.mean()) / prior.std().replace(0.0, np.nan)).to_numpy(dtype=float)


def build_grid(cor3m: pd.Series, sessions: pd.DatetimeIndex, pit: dict) -> pd.DataFrame:
    """The frozen monthly grid: anchored at the first SPY session with a finite COR3M close and a
    complete realised-correlation window inside the panel, then every GRID_STEP sessions while an
    exit exists on the calendar. COR3M is aligned to the SPY calendar and never forward-filled."""
    sessions = pd.DatetimeIndex(sessions)
    n = len(sessions)
    implied = cor3m.reindex(sessions).to_numpy(dtype=float)
    ppos = pd.DatetimeIndex(pit["dates"]).get_indexer(sessions)
    span = np.arange(RC_LOOKBACK + 1)

    def window_ready(i: int) -> bool:
        j = i - 1 - RC_LOOKBACK
        return j >= 0 and ppos[j] >= 0 and bool((ppos[j:i] == ppos[j] + span).all())

    start = next((i for i in range(1, n) if np.isfinite(implied[i]) and window_ready(i)), n)
    rows = []
    i = start
    while i + ENTRY_DELAY + HORIZON < n:
        rc, names = realised_correlation(pit, int(ppos[i - 1])) if window_ready(i) else (np.nan, 0)
        prem = implied[i] / 100.0 - rc if (np.isfinite(implied[i]) and np.isfinite(rc)) else np.nan
        rows.append({"decision_date": sessions[i], "cor3m": implied[i], "realised_corr": rc,
                     "names": int(names), "premium": prem})
        i += GRID_STEP
    g = pd.DataFrame(rows, columns=["decision_date", "cor3m", "realised_corr", "names", "premium"])
    g["z"] = zscore_prior(g["premium"].astype(float)) if len(g) else np.array([], dtype=float)
    return g


def decisions(grid: pd.DataFrame, spy_tr: pd.Series) -> pd.DataFrame:
    """Decisions start at the first grid slot with a finite z. The position read at t is entered
    at the close of t+1 and exited at the close of t+1+HORIZON."""
    if not len(grid):
        return pd.DataFrame()
    tr = spy_tr.dropna().sort_index()
    sessions, px = tr.index, tr.to_numpy(dtype=float)
    z = grid["z"].to_numpy(dtype=float)
    fin = np.where(np.isfinite(z))[0]
    if not len(fin):
        return pd.DataFrame()
    loc = sessions.get_indexer(pd.DatetimeIndex(grid["decision_date"]))
    rows = []
    for k in range(int(fin[0]), len(grid)):
        i = int(loc[k])
        e, x = i + ENTRY_DELAY, i + ENTRY_DELAY + HORIZON
        if i < 0 or x >= len(sessions):
            continue
        pos = FROZEN_SIGN * float(np.sign(z[k])) if np.isfinite(z[k]) else np.nan
        rows.append({"decision_date": sessions[i], "entry_date": sessions[e], "exit_date": sessions[x],
                     "premium": float(grid["premium"].iloc[k]), "z": float(z[k]), "pos": pos,
                     "spy_r": px[x] / px[e] - 1.0})
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# Statistics - existing owners only
# --------------------------------------------------------------------------- #
def _window(d: pd.DataFrame, lo: str, hi: str) -> pd.DataFrame:
    if not len(d):
        return d
    return d[(d["entry_date"] >= pd.Timestamp(lo)) & (d["entry_date"] <= pd.Timestamp(hi))]


def _finite(d: pd.DataFrame) -> pd.DataFrame:
    return d[np.isfinite(d["pos"].to_numpy(dtype=float))] if len(d) else d


def net(d: pd.DataFrame, cost_bps: float) -> np.ndarray:
    f = _finite(d)
    if not len(f):
        return np.array([])
    return (f["pos"] * f["spy_r"] - f["pos"].abs() * 2.0 * cost_bps * 1e-4).to_numpy(dtype=float)


def stats(x: np.ndarray) -> dict:
    x = np.asarray(x, dtype=float)
    x = x[np.isfinite(x)]
    if len(x) < 3:
        return {"periods": int(len(x))}
    sd = float(np.std(x, ddof=1))
    st = S.nw_tstat(x, 0)
    return {"periods": int(len(x)), "mean": float(x.mean()), "ann_net": float(x.mean() * PERIODS_PER_YEAR),
            "sharpe": float(x.mean() / sd * np.sqrt(PERIODS_PER_YEAR)) if sd > 0 else None,
            "t_net": st["t"], "p_net_one_sided": st["p_one_sided"], "max_dd": S._max_dd(x),
            "hit_rate": float((x > 0).mean())}


def _increment(y: np.ndarray, p: np.ndarray, scale: float, definition: str) -> dict:
    a = y - scale * p
    st = S.nw_tstat(a, 0)
    return {"scale": scale, "t": st["t"], "p_one_sided": st["p_one_sided"],
            "ann": float(a.mean() * PERIODS_PER_YEAR), "definition": definition}


def timing_increment(d: pd.DataFrame, cost_bps: float) -> dict:
    """Increment over the passive long on the same entry->exit spans, measured against BOTH the
    volatility-matched passive long (the catalog's control, k = sd(net) / sd(spy_r)) and the
    regression-matched one (beta_hat = cov(net, spy_r) / var(spy_r)). Gate 5 binds on the worse
    of the two: ``t`` and ``ann`` are the minima."""
    f = _finite(d)
    y = net(f, cost_bps)
    p = f["spy_r"].to_numpy(dtype=float) if len(f) else np.array([])
    if len(y) < 3 or float(np.var(p)) <= 0:
        return {"periods": int(len(y)), "t": None, "ann": None, "beta_on_passive_long": None,
                "volatility_matched": None, "regression_matched": None}
    k = float(np.std(y, ddof=1) / np.std(p, ddof=1))
    beta = float(np.cov(y, p, ddof=1)[0, 1] / np.var(p, ddof=1))
    vm = _increment(y, p, k, "a = net - k * spy_r, k = sd(net) / sd(spy_r); nw_tstat(a, 0)")
    rm = _increment(y, p, beta, "a = net - beta_hat * spy_r, beta_hat = cov(net, spy_r) / var(spy_r); "
                                "nw_tstat(a, 0)")
    ts = [x["t"] for x in (vm, rm)]
    return {"periods": int(len(y)), "volatility_matched": vm, "regression_matched": rm,
            "beta_on_passive_long": beta,
            "t": None if any(t is None for t in ts) else float(min(ts)),
            "ann": float(min(vm["ann"], rm["ann"])),
            "passive_long_ann": float(p.mean() * PERIODS_PER_YEAR),
            "binding": "the worse of the volatility-matched and regression-matched increments"}


def _incumbent(d: pd.DataFrame, incumbent_daily: Optional[pd.Series]) -> dict:
    if incumbent_daily is None or not len(incumbent_daily):
        return {"state": "DATA_HOLD", "why": "the incumbent daily path is unavailable"}
    from .intraday_alpha import equal_risk_daily
    f = _finite(d)
    s = pd.Series(net(f, COST_PRIMARY_BPS), index=pd.to_datetime(f["entry_date"]))
    daily = s.apply(lambda r: (1.0 + r) ** (1.0 / HORIZON) - 1.0)
    daily = daily.reindex(pd.bdate_range(daily.index.min(), daily.index.max()))
    daily = daily.ffill(limit=HORIZON - 1).dropna()
    return equal_risk_daily(daily, incumbent_daily, label=MECHANISM_ID)


# --------------------------------------------------------------------------- #
# The gates, in the preregistered order
# --------------------------------------------------------------------------- #
def evaluate(grid: pd.DataFrame, spy_tr: pd.Series, *, integrity_check: dict,
             incumbent_daily: Optional[pd.Series] = None) -> dict:
    d = decisions(grid, spy_tr)
    q = _window(d, *QUALIFICATION)
    out: dict = {"n_grid_slots": int(len(grid)), "n_decisions_total": int(len(d)),
                 "qualification_window": QUALIFICATION, "confirmation_window": CONFIRMATION,
                 "integrity": integrity_check,
                 "confirmation": {"state": "UNREAD", "why": "read only if every qualification gate passes"}}

    def done(verdict: str, gate: Optional[str], why: str) -> dict:
        out.update({"verdict": verdict, "gate": gate, "why": why})
        return out

    finite_share = float(np.isfinite(q["pos"].to_numpy(dtype=float)).mean()) if len(q) else 0.0
    parts = {k: _finite(_window(q, *w)) for k, w in PARTITIONS.items()}
    counts = {k: int(len(v)) for k, v in parts.items()}
    out["data"] = {"first_grid_session": str(grid["decision_date"].iloc[0].date()) if len(grid) else None,
                   "first_decision": str(d["decision_date"].iloc[0].date()) if len(d) else None,
                   "qualification_decisions": int(len(q)), "finite_share": finite_share,
                   "partition_finite_decisions": counts,
                   "median_names": float(grid["names"].median()) if len(grid) else None}
    if (not integrity_check.get("passes") or finite_share < MIN_FINITE_SHARE
            or min(counts.values()) < MIN_EFFECTIVE_PERIODS):
        return done("DATA_HOLD", "DATA", "cache integrity %s, finite share %.3f (floor %.2f), partition "
                    "decisions %s (floor %d)" % (integrity_check.get("passes"), finite_share,
                                                 MIN_FINITE_SHARE, counts, MIN_EFFECTIVE_PERIODS))

    by_cost = {"%.1f" % c: stats(net(q, c)) for c in COST_LADDER_BPS}
    prim = by_cost["%.1f" % COST_PRIMARY_BPS]
    out["qualification_by_cost_bps_per_side"] = by_cost
    out["qualification_gross"] = stats(net(q, 0.0))
    n1 = net(q, COST_PRIMARY_BPS)
    if float(np.mean(n1)) <= 0.0:
        return done("KILLED_WRONG_SIGN", "FROZEN_SIGN",
                    "mean net %.6f per period is not in the frozen HIGH-premium-LONG direction; the sign "
                    "is never reversed" % float(np.mean(n1)))
    if prim.get("t_net") is None or float(prim["t_net"]) < T_FLOOR:
        return done("NO_EDGE", "STANDALONE_T", "NW t %s below %.1f" % (prim.get("t_net"), T_FLOOR))
    if (prim.get("ann_net") or 0.0) < MATERIALITY_ANN_NET:
        return done("KILLED_BELOW_MATERIALITY", "MATERIALITY", "annualised net %.4f below %.3f"
                    % (prim.get("ann_net") or 0.0, MATERIALITY_ANN_NET))
    inc = timing_increment(q, COST_PRIMARY_BPS)
    out["timing_increment"] = inc
    if inc.get("t") is None or float(inc["t"]) < T_FLOOR or (inc.get("ann") or 0.0) < MATERIALITY_ANN_NET:
        return done("KILLED_NONINCREMENTAL", "TIMING_INCREMENT",
                    "worse of the volatility- and regression-matched increments over the passive long: "
                    "t %s ann %s" % (inc.get("t"), inc.get("ann")))
    stab = {k: stats(net(v, COST_PRIMARY_BPS)).get("ann_net") for k, v in parts.items()}
    out["partition_ann_net"] = stab
    bad = sorted(k for k, v in stab.items() if v is None or v <= 0.0)
    if bad:
        return done("KILLED_UNSTABLE", "PARTITION_SIGN", "non-positive annualised net in %s" % ", ".join(bad))
    pvals = {MECHANISM_ID: prim.get("p_net_one_sided")}
    pvals.update({k: 1.0 for k in INHERITED_NULLS})
    bh = S.bh_fdr(pvals, BH_Q)
    out["multiplicity"] = {"m": bh["m"], "q": BH_Q, "inherited_nulls": list(INHERITED_NULLS),
                           "p_one_sided": prim.get("p_net_one_sided"),
                           "passes": bool(bh["per_test"].get(MECHANISM_ID))}
    if not out["multiplicity"]["passes"]:
        return done("KILLED_MULTIPLICITY", "BH_INHERITED", "one-sided p %s fails BH q=%.2f at m=%d"
                    % (prim.get("p_net_one_sided"), BH_Q, bh["m"]))
    stress = by_cost["%.1f" % COST_STRESS_BPS]
    if (stress.get("ann_net") or 0.0) < MATERIALITY_ANN_NET:
        return done("KILLED_BELOW_MATERIALITY", "STRESS_COST", "annualised net at %.0f bp %.4f"
                    % (COST_STRESS_BPS, stress.get("ann_net") or 0.0))
    ir = _incumbent(q, incumbent_daily)
    out["incumbent_equal_risk"] = ir
    if ir.get("state") == "OK" and not ir.get("positive_incremental_utility_after_costs"):
        return done("NO_INCREMENTAL_INFORMATION_EDGE", "INCUMBENT",
                    "equal-risk increment %s t %s" % (ir.get("incremental_ann_net_return"), ir.get("t_incremental")))

    c = _window(d, *CONFIRMATION)
    c = c[c["exit_date"] <= pd.Timestamp(CONFIRMATION[1])] if len(c) else c
    cnet = net(c, COST_PRIMARY_BPS)
    cs = stats(cnet)
    out["confirmation"] = {
        "state": "READ", "stats": cs,
        "by_publication_period": {k: stats(net(_window(c, *w), COST_PRIMARY_BPS))
                                  for k, w in CONFIRMATION_ROWS.items()},
        "publication_split": "DESCRIPTIVE ONLY, by entry date at %s; the gate reads the whole window"
                             % PUBLISHED_FROM}
    ok = (cs.get("t_net") is not None and len(cnet) > 0 and float(np.mean(cnet)) > 0.0
          and cs["ann_net"] >= MATERIALITY_ANN_NET and float(cs["t_net"]) >= T_FLOOR
          and cs.get("p_net_one_sided") is not None and float(cs["p_net_one_sided"]) <= BH_Q)
    if not ok:
        return done("KILLED_UNSTABLE", "UNTOUCHED_CONFIRMATION",
                    "confirmation ann %s t %s did not reproduce" % (cs.get("ann_net"), cs.get("t_net")))
    return done("QUALIFIED", None, "every preregistered gate passed, including the untouched "
                                   "confirmation; a HUMAN gate governs prospective registration")


# --------------------------------------------------------------------------- #
# Executor contract
# --------------------------------------------------------------------------- #
def _identity(cor3m: pd.Series, integrity_check: dict, pit: dict, spy_tr: pd.Series) -> str:
    def span(s: pd.Series) -> str:
        return "%s..%s" % (str(s.index.min().date()), str(s.index.max().date())) if len(s) else "EMPTY"
    return "CBOE_COR3M:%s:%s|%s|NORGATE_SPY_TOTALRETURN:%s:%d" % (
        str(integrity_check.get("file_sha256") or "UNHASHED")[:16], span(cor3m.dropna()),
        pit.get("identity") or "PIT_PANEL:UNIDENTIFIED", span(spy_tr.dropna()), int(spy_tr.notna().sum()))


def run(*, verbose: bool = True, write: bool = True, cor3m: Optional[pd.Series] = None,
        pit: Optional[dict] = None, spy_tr: Optional[pd.Series] = None,
        integrity_check: Optional[dict] = None, incumbent_daily="LOAD") -> dict:
    cor3m = load_cor3m() if cor3m is None else cor3m           # first: a missing cache holds before any load
    integrity_check = cache_integrity() if integrity_check is None else integrity_check
    pit = load_pit_panel() if pit is None else pit
    spy_tr = load_spy_total_return() if spy_tr is None else spy_tr
    if isinstance(incumbent_daily, str):
        from .intraday_alpha import incumbent_daily_path
        incumbent_daily = incumbent_daily_path()
    grid = build_grid(cor3m, spy_tr.dropna().sort_index().index, pit)
    res = evaluate(grid, spy_tr, integrity_check=integrity_check, incumbent_daily=incumbent_daily)
    body = {"schema": "alpha_agent_mechanism_result/1", "calculation_owner": CALCULATION_OWNER,
            "mechanism_id": MECHANISM_ID, "preregistration": PREREGISTRATION,
            "frozen": {"sign": FROZEN_SIGN, "grid_step_sessions": GRID_STEP, "horizon": HORIZON,
                       "entry_delay": ENTRY_DELAY,
                       "realised_correlation": "equal-weight average pairwise correlation of daily total "
                                               "returns over the %d sessions ending t-1, S&P 500 members on "
                                               "t-1 with complete windows, >= %d names"
                                               % (RC_LOOKBACK, RC_MIN_NAMES),
                       "premium": "COR3M(t)/100 - realised_corr(t)",
                       "zscore": "options_surface._z formula, strictly prior %d grid slots, >= %d finite"
                                 % (Z_WINDOW, Z_MIN_OBS),
                       "cost_ladder_bps_per_side": list(COST_LADDER_BPS),
                       "published_from": PUBLISHED_FROM},
            "input_data_identity": _identity(cor3m, integrity_check, pit, spy_tr),
            "result": res, "capital_eligible": False}
    if write:
        body["artifact_path"] = str(write_artifact(ARTIFACT_NAME, body))
    if verbose:
        print("%s -> %s (%s)" % (MECHANISM_ID, res["verdict"], res["why"]), flush=True)
    return body


def run_mechanism(*, mechanism: dict) -> dict:
    """The Alpha Agent executor contract (``alpha_agent.r59.mechanisms.validate_result``)."""
    if (mechanism or {}).get("mechanism_id") != MECHANISM_ID:
        raise ValueError("this executor implements %s only" % MECHANISM_ID)
    try:
        body = run(verbose=True, write=True)
    except (ImportError, FileNotFoundError) as exc:
        return {"verdict": "DATA_HOLD", "why": "an input is unavailable: %s" % exc, "kill_rule_fired": "DATA",
                "statistic": {}, "economics": {}, "multiplicity": {}, "artifact": None,
                "capital_eligible": False, "scorer": SCORER, "input_data_identity": "UNAVAILABLE"}
    res = body["result"]
    prim = (res.get("qualification_by_cost_bps_per_side") or {}).get("%.1f" % COST_PRIMARY_BPS) or {}
    return {"verdict": res["verdict"], "why": res["why"], "kill_rule_fired": res.get("gate"),
            "statistic": {"lockbox_t": prim.get("t_net"),
                          "lockbox_window": "QUALIFICATION %s..%s" % QUALIFICATION,
                          "p_one_sided": prim.get("p_net_one_sided"), "periods": prim.get("periods"),
                          "timing_increment_t": (res.get("timing_increment") or {}).get("t"),
                          "confirmation": res.get("confirmation")},
            "economics": {"ann_net_1bp": prim.get("ann_net"), "sharpe": prim.get("sharpe"),
                          "max_dd": prim.get("max_dd"), "partition_ann_net": res.get("partition_ann_net"),
                          "qualification_gross": res.get("qualification_gross")},
            "multiplicity": res.get("multiplicity") or {"m": 1 + len(INHERITED_NULLS), "q": BH_Q,
                                                        "state": "NOT_REACHED"},
            "artifact": body.get("artifact_path"), "capital_eligible": False, "scorer": SCORER,
            "input_data_identity": body["input_data_identity"]}
