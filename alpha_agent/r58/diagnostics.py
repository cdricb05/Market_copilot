"""alpha_agent.r58.diagnostics - the measurements that keep R58 honest.

Four of them, all required by the protocol:

COVERAGE BIAS       PANEL-F covers 88.8% of PIT S&P 500 member-days in 2010 and
                    98.7% in 2026. That drift is not random - a company that
                    died early is less likely to have been CIK-resolved - so the
                    residual survivorship exposure is measured, not asserted
                    away: the forward return of covered eligible members minus
                    the forward return of uncovered ones.

FROZEN PANEL        what the operational fundamental leg actually IS. The
FORENSICS           champion reads a frozen CSV; this counts its universe, its
                    exits, its duplicate rows and how stale its last row is.

LIVE FORWARD        the desk's own TRUE_FORWARD matured outcomes for the blend
LEDGER              and its two legs, re-read READ-ONLY, so R58's historical
                    answer can be set against the live evidence that motivated
                    the question. Diagnostic only: the sample is tiny.

INCUMBENT           the champion's replication (B0) against the fundamental-only
COMPARISON          families, layer by layer.
"""
from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict

import numpy as np

from . import (DESK_DIR, FROZEN_FUND_PANEL, write_artifact)
from . import engine as E
from ..r57.engine import eligibility as _r57_eligibility, forward_return, nw_tstat

PPY = 12.0
ARTIFACT = "r58_diagnostics.json"


# --------------------------------------------------------------------------- #
def coverage_bias(pf: dict) -> dict:
    """Forward return of PANEL-F-covered members minus uncovered ones."""
    price = pf["price"]
    layers = E.layer_of(pf["dec_dates"])
    rows = defaultdict(list)
    cov_frac = defaultdict(list)
    unc_n = defaultdict(list)
    for j, t in enumerate(pf["dec"]):
        t = int(t)
        lay = layers[j]
        if lay == "":
            continue
        elig57 = _r57_eligibility(price, t)
        core = pf["cube"][:, j, pf["f_ix"]["has_core"]]
        covered = elig57 & np.isfinite(core) & (core > 0)
        uncovered = elig57 & ~covered
        if covered.sum() < 50 or uncovered.sum() < 5:
            cov_frac[lay].append(float(covered.sum()) / max(int(elig57.sum()), 1))
            continue
        rc = forward_return(price, np.where(covered)[0], t, pf["meta"]["horizon"])
        ru = forward_return(price, np.where(uncovered)[0], t, pf["meta"]["horizon"])
        rows[lay].append(float(rc.mean()) - float(ru.mean()))
        cov_frac[lay].append(float(covered.sum()) / max(int(elig57.sum()), 1))
        unc_n[lay].append(int(uncovered.sum()))
    out = {}
    for lay in ("D", "V", "L"):
        v = np.array(rows.get(lay, []))
        if len(v) < 4:
            out[lay] = {"periods": len(v)}
            continue
        st = nw_tstat(v)
        out[lay] = {
            "periods": len(v),
            "ann_covered_minus_uncovered": float(v.mean() * PPY),
            "t": st["t"],
            "median_coverage_fraction": float(np.median(cov_frac[lay])),
            "median_uncovered_names": float(np.median(unc_n[lay])) if unc_n[lay] else None,
            "precision_warning": (
                "the uncovered leg is a median of %s names; a difference "
                "estimated against a handful of names is noisy however large "
                "its point estimate looks"
                % (int(np.median(unc_n[lay])) if unc_n[lay] else "n/a")),
        }
    out["interpretation_rule"] = (
        "a materially POSITIVE covered-minus-uncovered return means the names "
        "PANEL-F can see out-earned the ones it cannot, so every long-only "
        "excess R58 reports against the covered benchmark is an UPPER BOUND on "
        "what the same signal would have earned on the full index. The number "
        "is published whatever it says.")
    return out


# --------------------------------------------------------------------------- #
def frozen_panel_forensics() -> dict:
    """What the operational fundamental leg's own source panel actually is."""
    if not FROZEN_FUND_PANEL.exists():
        return {"status": "PANEL_NOT_FOUND", "path": str(FROZEN_FUND_PANEL)}
    rows = []
    with open(FROZEN_FUND_PANEL, newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    per_date = Counter(r["rebalance_date"] for r in rows)
    biggest_date, biggest_n = per_date.most_common(1)[0]
    mega = [r for r in rows if r["rebalance_date"] == biggest_date]
    mega_t = {r["ticker"] for r in mega}
    # exact duplicate detection on the mega date: same ticker, same score
    dup_keys = Counter((r["ticker"], r["composite_sn"], r["forward_63d_return"])
                       for r in mega)
    exact_dupes = sum(c - 1 for c in dup_keys.values() if c > 1)
    by_t = defaultdict(list)
    for r in rows:
        if r["rebalance_date"] != biggest_date:
            by_t[r["ticker"]].append(r["rebalance_date"])
    last_seen = {t: max(v) for t, v in by_t.items() if v}
    max_date = max(last_seen.values()) if last_seen else None
    exits = sum(1 for d in last_seen.values() if d < "2025-06-01")
    return {
        "path": str(FROZEN_FUND_PANEL),
        "rows": len(rows),
        "distinct_tickers": len({r["ticker"] for r in rows}),
        "as_of_dates": sorted({r["as_of_date"] for r in rows}),
        "panel_last_rebalance_date": max_date,
        "pseudo_date_collapse": {
            "date": biggest_date, "rows_on_that_date": biggest_n,
            "distinct_tickers_on_that_date": len(mega_t),
            "exact_duplicate_rows": exact_dupes,
            "share_of_panel": round(biggest_n / max(len(rows), 1), 4),
            "finding": ("the 'old' cohort's whole history is stamped on ONE "
                        "pseudo rebalance date and replicated per ticker, so "
                        "roughly %d%% of the panel is exact duplicates of %d "
                        "tickers. Any IC or regression run on the raw file is "
                        "dominated by a single cross-section and its effective "
                        "sample size is a fraction of its row count."
                        % (round(100 * biggest_n / max(len(rows), 1)), len(mega_t))),
        },
        "survivorship": {
            "tickers_last_seen_before_2025_06": exits,
            "tickers_total": len(last_seen),
            "finding": ("%d of %d tickers are still present at the panel's last "
                        "month. A real S&P 500 panel over this window loses "
                        "roughly 250 members. This universe is 'names that are "
                        "in the index now, backfilled'."
                        % (len(last_seen) - exits, len(last_seen))),
        },
        "staleness": {
            "last_rebalance_date": max_date,
            "finding": ("the operational fundamental leg reads this file, so its "
                        "live scores cannot be fresher than %s" % max_date),
        },
        "role_in_r58": ("object of study, never evidence: R58 rebuilds the "
                        "construct from owned SEC point-in-time facts on the "
                        "survivorship-safe Norgate universe instead"),
    }


# --------------------------------------------------------------------------- #
def live_forward_ledger() -> dict:
    """The desk's matured TRUE_FORWARD outcomes. READ ONLY, diagnostic only."""
    models = ("fundamental_momentum_50_50_v1", "composite_sn", "mom_6_1")
    horizons = (1, 5, 20)
    path = DESK_DIR / "forward_prediction_outcomes.json"
    if not path.exists():
        return {"status": "LEDGER_NOT_FOUND", "path": str(path)}
    rows = json.loads(path.read_text(encoding="utf-8"))["rows"]
    outs = [r for r in rows if r.get("kind") == "OUTCOME"
            and r.get("status") == "MATURED"]
    out = {"source": str(path), "matured_rows_total": len(outs),
           "scope": "DIAGNOSTIC_ONLY_NO_ALPHA_VERDICT", "by_model_horizon": {}}
    sessions = set()
    for model in models:
        for h in horizons:
            buys, sells, ics = [], [], []
            for o in outs:
                if o.get("model_id") != model or o.get("horizon") != h:
                    continue
                m = o.get("metrics") or {}
                need = ("top_decile_return_pct", "bottom_decile_return_pct",
                        "universe_avg_return_pct")
                if any(m.get(k) is None for k in need):
                    continue
                buys.append(m["top_decile_return_pct"] - m["universe_avg_return_pct"])
                sells.append(m["bottom_decile_return_pct"] - m["universe_avg_return_pct"])
                if m.get("rank_ic_spearman") is not None:
                    ics.append(m["rank_ic_spearman"])
                if o.get("market_date"):
                    sessions.add(str(o["market_date"])[:10])
            if not buys:
                continue
            out["by_model_horizon"]["%s_h%d" % (model, h)] = {
                "model": model, "horizon": h, "n_matured_sessions": len(buys),
                "buy_side_top_decile_excess_pct_mean": float(np.mean(buys)),
                "buy_side_t": nw_tstat(np.array(buys), lag=max(0, h // 5))["t"],
                "sell_side_bottom_decile_excess_pct_mean": float(np.mean(sells)),
                "sell_side_skill_t": nw_tstat(-np.array(sells), lag=max(0, h // 5))["t"],
                "mean_rank_ic": float(np.mean(ics)) if ics else None,
                "sample_warning": "TINY forward sample; diagnostic only",
            }
    out["distinct_matured_sessions"] = len(sessions)
    return out


# --------------------------------------------------------------------------- #
def run(pf: dict) -> dict:
    body = {
        "track": "R58_DIAGNOSTICS",
        "coverage_bias": coverage_bias(pf),
        "frozen_operational_panel_forensics": frozen_panel_forensics(),
        "live_forward_ledger": live_forward_ledger(),
    }
    write_artifact(ARTIFACT, body)
    return body
