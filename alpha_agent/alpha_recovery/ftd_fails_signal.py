"""alpha_agent.alpha_recovery.ftd_fails_signal - fails-to-deliver relative to a
security's OWN trading volume, point-in-time by publication, and its census.

THIS MODULE COMPUTES NO RETURN. It reads the fails balances of
:mod:`ftd_fails_data`, the share VOLUME of the same half-month (a quantity already
public at every session that may use it), the eligibility mask and the calendar.
No total-return series and no forward window is touched - a test pins it.

THE ONE FROZEN MEASURE

    FAILS_TO_VOLUME[i, file] = mean daily fails balance of security i over the
                               file's settlement dates (absent = 0)
                             / mean daily share volume of i over the sessions
                               of the same half-month

    projected onto a decision session only from the file covering the LATEST
    half-month whose declared ``usable_after`` day is strictly before it, and
    cross-sectionally ranked (percentile, ties averaged) among eligible
    identified names. Direction NEGATIVE, frozen: a larger fails-to-volume
    ratio is predicted to be followed by LOWER return.

A volume denominator, not shares outstanding: the estate owns no PIT shares
outstanding, so the Reg SHO threshold test (0.5% of shares outstanding) cannot be
computed; a security's own volume is PIT-safe and scale-free.

RESEARCH ONLY. No purchase, no promotion, no capital, no proposal, no order, no
fill, no backfill, no live write.
"""
from __future__ import annotations

import re
from collections import Counter

import numpy as np
import pandas as pd

from alpha_agent.r63 import EQUITY_DISCOVERY_START, LOCKBOX_START
from alpha_agent.r63 import experiments as X
from alpha_agent.r63 import pit

from . import now_iso, write_artifact
from . import ftd_fails_data as D
from . import ownership_identity as OI

CALCULATION_OWNER = "alpha_agent.alpha_recovery.ftd_fails_signal"
CENSUS_ARTIFACT = "ftd_fails_census.json"

SIGN_NEGATIVE = -1
CELL = "FAILS_TO_VOLUME"
CELL_SPECS = {
    CELL: {
        "sign": SIGN_NEGATIVE,
        "measure": "mean daily fails balance over the latest usable half-month / mean daily "
                   "share volume over the sessions of the same half-month",
        "transform": "cross-sectional percentile rank among eligible identified names",
        "first_half_usable_after_day": D.FIRST_HALF_USABLE_AFTER_DAY,
        "second_half_usable_after_day": D.SECOND_HALF_USABLE_AFTER_DAY,
    },
}
CELLS = tuple(CELL_SPECS)

#: The minimum ranked cross-section, the canonical grid's own floor.
MIN_NAMES = 50

_DELISTED = re.compile(r"^.*-\d{6}$")


def identity(E: dict) -> dict:
    """The owned CUSIP-anchored bridge (FTD CUSIP+SYMBOL in one record)."""
    obs = OI.build_observations(verbose=False)
    return OI.panel_cusip_index(E, obs)


def fails_to_volume_by_file(E: dict, bal: dict) -> np.ndarray:
    """(n_files, n_rows). NaN where the row is unidentified or had no volume in
    the half-month; 0 where it traded and no fail was outstanding."""
    dates = pd.to_datetime(np.asarray(E["dates"]))
    vol = np.asarray(E["price"]["vol"], dtype=float)
    out = np.full(bal["mean_balance"].shape, np.nan)
    for k, f in enumerate(bal["files"]):
        if not int(bal["settlement_days"][k]):
            continue
        ix = np.where((dates >= pd.Timestamp(f["first_day"])) & (dates <= pd.Timestamp(f["last_day"])))[0]
        if not len(ix):
            continue
        v = vol[:, ix]
        good = np.isfinite(v) & (v > 0)
        cnt = good.sum(axis=1)
        mv = np.where(cnt > 0, np.where(good, v, 0.0).sum(axis=1) / np.maximum(cnt, 1), np.nan)
        with np.errstate(invalid="ignore", divide="ignore"):
            out[k] = np.where(np.isfinite(mv) & (mv > 0), bal["mean_balance"][k] / mv, np.nan)
    return out


def session_matrix(E: dict, per_file: np.ndarray, files: list) -> tuple:
    """(n_rows, n_dates) projection by publication, and the visible-file index."""
    vis = D.visible_file_index(E["dates"], files)
    M = np.full((per_file.shape[1], len(vis)), np.nan)
    ok = vis >= 0
    if ok.any():
        M[:, ok] = per_file[vis[ok]].T
    return M, vis


def rank_signal(M: np.ndarray, elig: np.ndarray, dec: np.ndarray, sign: int) -> tuple:
    """``sign`` x percentile rank of M among eligible names with a finite value,
    on the decision sessions only; NaN elsewhere (unidentified is unassessable,
    never zero)."""
    sig = np.full(M.shape, np.nan)
    per = []
    for t in np.asarray(dec, dtype=int):
        u = elig[:, t] & np.isfinite(M[:, t])
        n = int(u.sum())
        if n < MIN_NAMES:
            continue
        r = pd.Series(M[u, t]).rank(pct=True, method="average").to_numpy()
        sig[u, t] = float(sign) * r
        per.append({"t": int(t), "ranked": n, "eligible": int(elig[:, t].sum())})
    return sig, per


def build(E: dict, elig: np.ndarray, *, verbose: bool = True) -> dict:
    cix = identity(E)
    bal = D.load_or_build_balances(cix["cusip_to_row"], len(E["symbols"]), verbose=verbose)
    per_file = fails_to_volume_by_file(E, bal)
    M, vis = session_matrix(E, per_file, bal["files"])
    return {"cix": cix, "balances": bal, "per_file": per_file, "M": M, "visible": vis}


# --------------------------------------------------------------------------- #
# Census - no return
# --------------------------------------------------------------------------- #
def _q(a, qs=(0.5, 0.9, 0.99)) -> dict:
    a = np.asarray(a, dtype=float)
    a = a[np.isfinite(a)]
    return {str(q): float(np.quantile(a, q)) for q in qs} if a.size else {}


def census(E: dict, elig: np.ndarray, built: dict, *, verbose: bool = True) -> dict:
    dates = np.asarray(E["dates"])
    d64 = pd.to_datetime(dates)
    syms = [str(s) for s in np.asarray(E["symbols"])]
    delisted = np.array([bool(_DELISTED.match(s)) for s in syms])
    bal, M, vis, cix = built["balances"], built["M"], built["visible"], built["cix"]
    identified = np.asarray(bal["identified"], dtype=bool)
    files = bal["files"]

    grids = {}
    for h in (21, 63):
        cad = X.cadence_for(h)
        dec = pit.decision_indices(dates, EQUITY_DISCOVERY_START, cad, h)
        folds = pit.walk_forward(dates, dec, horizon=h)
        test_pos = sorted({int(p) for f in folds for p in f["test"]})
        cov, fin, lag_days = [], [], []
        dl_n = dl_id = 0
        for t in np.asarray(dec, dtype=int):
            e = elig[:, t]
            n = int(e.sum())
            if not n:
                continue
            cov.append(float((e & identified).sum()) / n)
            fin.append(float((e & np.isfinite(M[:, t])).sum()) / n)
            dl_n += int((e & delisted).sum())
            dl_id += int((e & delisted & identified).sum())
            if vis[t] >= 0:
                lag_days.append(int((d64[t] - pd.Timestamp(files[vis[t]]["last_day"])).days))
        cov = np.asarray(cov)
        sig, _per = rank_signal(M, elig, dec, SIGN_NEGATIVE)
        rs = []
        prev = None
        for t in np.asarray(dec, dtype=int):
            col = sig[:, t]
            if prev is not None:
                ok = np.isfinite(col) & np.isfinite(prev)
                if ok.sum() >= MIN_NAMES:
                    rs.append(float(pd.Series(col[ok]).corr(pd.Series(prev[ok]), method="spearman")))
            prev = col
        grids[str(h)] = {
            "cadence": cad, "decision_sessions": int(len(dec)),
            "oos_test_periods": len(test_pos),
            "effective_periods": int(len(test_pos) * min(1.0, cad / float(h))),
            "identified_share_mean": float(cov.mean()), "identified_share_min": float(cov.min()),
            "sessions_under_95pct": int((cov < 0.95).sum()),
            "uncovered_share": float((cov < 0.95).mean()),
            "finite_signal_share_mean": float(np.mean(fin)),
            "eligible_delisted_observations_identified_share": dl_id / max(dl_n, 1),
            "days_from_half_month_end_to_decision": _q(lag_days, (0.0, 0.5, 1.0)),
            "rank_autocorrelation_consecutive_decisions": _q(rs, (0.1, 0.5, 0.9)),
        }

    by_year: dict = {}
    for k, f in enumerate(files):
        ix = np.where((d64 >= pd.Timestamp(f["first_day"])) & (d64 <= pd.Timestamp(f["last_day"])))[0]
        if not len(ix):
            continue
        u = elig[:, ix[-1]] & identified
        if not u.any():
            continue
        y = f["period"][:4]
        z = bal["nonzero_days"][k, u]
        r = built["per_file"][k, u]
        a = by_year.setdefault(y, {"files": 0, "any_fail": [], "ratio_pos": [], "over_half_pct": []})
        a["files"] += 1
        a["any_fail"].append(float((z > 0).mean()))
        a["ratio_pos"] += list(r[np.isfinite(r) & (r > 0)])
        a["over_half_pct"].append(float((r > 0.005).mean()))
    years = {y: {"files": a["files"], "any_fail_share": float(np.mean(a["any_fail"])),
                 "ratio_positive_quantiles": _q(a["ratio_pos"]),
                 "share_over_0p5pct_of_volume": float(np.mean(a["over_half_pct"]))}
             for y, a in sorted(by_year.items())}

    first_vis = int(np.argmax(vis >= 0)) if (vis >= 0).any() else None
    return {
        "schema": "alpha_recovery_ftd_fails_census/1",
        "calculation_owner": CALCULATION_OWNER, "generated_at": now_iso(),
        "returns_computed": False, "prices_read": False, "volume_read": True, "paid_dollars": 0,
        "source": "SEC Fails-to-Deliver semi-monthly files, owned (ownership_data acquisition)",
        "files": {"count": len(files), "first": files[0]["period"] if files else None,
                  "last": files[-1]["period"] if files else None,
                  "settlement_days_total": int(np.sum(bal["settlement_days"])),
                  "from_cache": bool(bal.get("from_cache"))},
        "availability_rule": {"first_half_usable_after_day_of_next_month": D.FIRST_HALF_USABLE_AFTER_DAY,
                              "second_half_usable_after_day_of_next_month": D.SECOND_HALF_USABLE_AFTER_DAY,
                              "all_nonzero_balances_from": D.ALL_NONZERO_BALANCES_FROM,
                              "first_session_with_a_visible_file":
                                  str(dates[first_vis])[:10] if first_vis is not None else None,
                              "last_visible_file_at_panel_end":
                                  files[vis[-1]]["period"] if len(vis) and vis[-1] >= 0 else None},
        "identity": {"mode": "CUSIP_ANCHORED", "cusips_indexed": cix["n_cusips"],
                     "rows_identified": int(identified.sum()), "panel_rows": len(syms),
                     "delisted_rows": int(delisted.sum()),
                     "delisted_rows_identified": int((delisted & identified).sum()),
                     "cusips_ambiguous_dropped": len(cix["ambiguous"]),
                     "fuzzy_name_matching_used": False},
        "panel": {"first_session": str(dates[0])[:10], "last_session": str(dates[-1])[:10],
                  "discovery_start": EQUITY_DISCOVERY_START, "lockbox_start": LOCKBOX_START},
        "grids": grids, "by_year": years, "cell": CELL, "cell_spec": CELL_SPECS[CELL],
    }


def run_census(*, verbose: bool = True, write: bool = True) -> dict:
    from . import incumbent as INC
    E, elig = INC.equity_substrate()
    built = build(E, elig, verbose=verbose)
    body = census(E, elig, built, verbose=verbose)
    if write:
        write_artifact(CENSUS_ARTIFACT, body)
    return body
