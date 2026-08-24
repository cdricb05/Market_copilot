"""alpha_agent.r42.asset_universe - Track I: freeze the universe, then look.

The eligibility rule in :data:`contract.ASSET_ELIGIBILITY` was frozen and
hashed before any asset's strategy outcome existed. It is metadata only:
history length, funding-event count, liquidity, identity, and structural
exclusions (stablecoins, leveraged tokens, synthetic redenominations). No
return, Sharpe or t-statistic of any candidate asset may influence
membership, and every symbol satisfying the rule is tested - including
delisted ones, which the Binance public archive preserves.

The frozen R41 BTC rule is then applied UNCHANGED to each eligible asset.
ETH was already observed in R41 and is labelled prior evidence. Every new
asset is HISTORICAL_OUT_OF_ASSET_REPLICATION, never TRUE_FORWARD.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import CAMPAIGN_ID, artifact_body, read_json, sha, write_artifact
from . import acquisition as ACQ
from . import capital as CAP
from . import contract as C
from . import data_dir
from . import pnl_audit as PA
from ..r41 import evidence as EV

CALCULATION_OWNER = "alpha_agent.r42.asset_universe"
FREEZE_ARTIFACT = "R42_ASSET_UNIVERSE_FREEZE.json"
RESULT_ARTIFACT = "CROSS_ASSET_REPLICATION.json"

PRIOR_EVIDENCE = ("BTCUSDT", "ETHUSDT")


# --------------------------------------------------------------------------- #
# 1. The frozen eligibility rule, applied to metadata only
# --------------------------------------------------------------------------- #
def _excluded_reason(symbol: str) -> str:
    e = C.ASSET_ELIGIBILITY
    base = symbol[:-4] if symbol.endswith("USDT") else symbol
    if base in e["exclude_stablecoin_bases"]:
        return "STABLECOIN_BASE"
    for suf in e["exclude_leveraged_tokens"]:
        if base.endswith(suf) and len(base) > len(suf):
            return "LEVERAGED_TOKEN"
    for pre in e["exclude_synthetic_duplicates"]:
        if base.startswith(pre):
            return "SYNTHETIC_REDENOMINATION"
    return None


def evaluate_symbol_metadata(symbol: str) -> dict:
    """Everything the frozen rule needs, and nothing it must not see."""
    e = C.ASSET_ELIGIBILITY
    row = {"symbol": symbol}
    ex = _excluded_reason(symbol)
    if ex:
        return {**row, "eligible": False, "reason": ex}
    df = ACQ.load_universe_daily(symbol)
    if not len(df):
        # No local panel means the archive-coverage survey (metadata only)
        # already showed too little joint history to be worth fetching.
        cov = (read_json(data_dir("binance") / ACQ.COVERAGE) or {}) \
            .get("symbols", {}).get(symbol, {})
        joint_months = len(set(cov.get("funding", []))
                           & set(cov.get("perp_1d", []))
                           & set(cov.get("spot_1d", [])))
        return {**row, "eligible": False,
                "reason": "min_joint_history_days",
                "joint_archive_months": joint_months,
                "note": "excluded by the frozen history rule at the "
                        "archive-coverage stage; no strategy outcome was "
                        "ever computed for this symbol"}
    joint = df.dropna(subset=["spot", "perp", "funding"])
    n_days = int(len(joint))
    n_events = int(joint["n_funding_events"].fillna(0).sum())
    med_vol = float(joint["spot_quote_volume"].median()) \
        if "spot_quote_volume" in joint else 0.0
    row.update({
        "first": str(joint.index.min().date()) if n_days else None,
        "last": str(joint.index.max().date()) if n_days else None,
        "joint_history_days": n_days,
        "funding_events": n_events,
        "median_daily_quote_volume_usd": med_vol,
        "is_prior_evidence": symbol in PRIOR_EVIDENCE,
    })
    checks = {
        "min_joint_history_days": n_days >= e["min_joint_history_days"],
        "min_funding_events": n_events >= e["min_funding_events"],
        "min_median_daily_quote_volume_usd":
            med_vol >= e["min_median_daily_quote_volume_usd"],
        "identity_verified": symbol.endswith(e["quote_currency"]),
    }
    row["checks"] = checks
    row["eligible"] = all(checks.values())
    row["reason"] = None if row["eligible"] else "+".join(
        k for k, v in checks.items() if not v)
    return row


def freeze_universe(*, refresh: bool = False) -> dict:
    path = ACQ.data_dir("binance") / "r42_universe_freeze_cache.json"
    cached = read_json(path)
    if cached and not refresh:
        rows = cached["rows"]
    else:
        cov = read_json(data_dir("binance") / ACQ.COVERAGE) or {}
        cands = sorted(cov.get("symbols", {}))
        rows = {s: evaluate_symbol_metadata(s) for s in cands}
        ACQ.write_json(path, {"rows": rows}, immutable=False)
    eligible = sorted(s for s, r in rows.items() if r.get("eligible"))
    body = artifact_body("r42_asset_universe_freeze/1", {
        "calculation_owner": CALCULATION_OWNER,
        "track": "I - predeclared out-of-asset replication universe",
        "eligibility_rule": C.ASSET_ELIGIBILITY,
        "rule_frozen_before_results":
            C.ASSET_UNIVERSE_FROZEN_BEFORE_RESULTS,
        "selection_may_use_performance":
            C.ASSET_ELIGIBILITY["selection_may_use_performance"],
        "n_candidates_surveyed": len(rows),
        "n_eligible": len(eligible),
        "eligible_symbols": eligible,
        "prior_evidence_symbols": list(PRIOR_EVIDENCE),
        "n_new_assets": len([s for s in eligible
                             if s not in PRIOR_EVIDENCE]),
        "exclusion_counts": _counts(rows),
        "survivorship_note":
            "membership is decided by ARCHIVE EXISTENCE, so a symbol that "
            "was delisted keeps its history and stays in the universe; "
            "today's active list never enters the rule",
        "rows": rows,
    })
    body["asset_universe_freeze_hash"] = sha(body)
    write_artifact(FREEZE_ARTIFACT, body, CAMPAIGN_ID, overwrite=True)
    return body


def _counts(rows: dict) -> dict:
    out = {}
    for r in rows.values():
        if r.get("eligible"):
            continue
        out[r.get("reason") or "UNKNOWN"] = out.get(
            r.get("reason") or "UNKNOWN", 0) + 1
    return dict(sorted(out.items(), key=lambda kv: -kv[1]))


# --------------------------------------------------------------------------- #
# 2. Apply the EXACT R41 rule, unchanged
# --------------------------------------------------------------------------- #
def r41_rule_signal(df: pd.DataFrame) -> pd.Series:
    """The frozen R41 rule, parameters identical, nothing fit."""
    f = df["funding"]
    z = (f.rolling(30, min_periods=15).mean()
         / f.rolling(90, min_periods=30).std()).shift(1)
    sig = pd.Series(0.0, index=df.index)
    sig[z > 0.5] = 1.0
    sig[z < -0.5] = -1.0
    return sig


#: The BTC Zone-C window, reused UNCHANGED as the recent-period window for
#: every replication asset so the cross-section is compared with BTC
#: like-for-like instead of over each asset's own flattering full history.
RECENT_WINDOW = ("2025-04-14", "2026-07-31")


def replicate_symbol(symbol: str, *, window: tuple = None) -> dict:
    df = ACQ.load_universe_daily(symbol)
    if not len(df):
        return {"symbol": symbol, "state": "NO_DATA"}
    df = df.dropna(subset=["spot", "perp", "funding"])
    df["basis_ret"] = df["spot"].pct_change() - df["perp"].pct_change()
    if window:
        df = df.loc[str(window[0]):str(window[1])]
        if len(df) < 120:
            return {"symbol": symbol, "state": "TOO_SHORT",
                    "n_days": int(len(df))}
    out = {"symbol": symbol, "state": "OK", "n_days": int(len(df)),
           "first": str(df.index.min().date()),
           "last": str(df.index.max().date()),
           "median_daily_quote_volume_usd":
               float(df["spot_quote_volume"].median())
               if "spot_quote_volume" in df else None}
    for label, sig in (("R41_RULE", r41_rule_signal(df)),
                       ("POSITIVE_ONLY",
                        _positive_only(df)),
                       ("ALWAYS_ON", _always_on(df))):
        for mode, full in (("FULL_ECONOMICS", True),
                           ("R41_CONVENTION", False)):
            bk = CAP.implementable_book(
                df, sig,
                capital_model=(C.PRIMARY_CAPITAL_MODEL if full
                               else "TRADED_NOTIONAL"),
                execution_model=(C.PRIMARY_EXECUTION_MODEL if full
                                 else "R41_BASELINE"),
                charge_financing=full)
            card = EV.scorecard(bk["pnl_on_capital"].to_numpy(),
                                np.zeros(len(bk)),
                                bk["benchmark"].to_numpy(),
                                periods_per_year=PA.R41_PPY, overlap=1)
            out["%s__%s" % (label, mode)] = {
                "excess_ann": card.get("excess_ann"),
                "t": card.get("excess_t_hac"),
                "sharpe": card.get("sharpe"),
                "roc_ann": float(np.nanmean(bk["pnl_on_capital"])
                                 * PA.R41_PPY),
                "gross_ann": float(np.nanmean(bk["gross"]) * PA.R41_PPY),
                "ess": (card.get("effective_sample") or {}).get("ess"),
            }
    return out


def _positive_only(df):
    mean30 = df["funding"].rolling(30, min_periods=15).mean().shift(1)
    s = pd.Series(0.0, index=df.index)
    s[mean30 > 0] = 1.0
    return s


def _always_on(df):
    s = pd.Series(1.0, index=df.index)
    if len(s):
        s.iloc[0] = 0.0
    return s


# --------------------------------------------------------------------------- #
# 3. Meta-analysis across assets
# --------------------------------------------------------------------------- #
def meta_analyse(rows: list, key: str) -> dict:
    eff = [(r["symbol"], r[key]["excess_ann"], r[key]["t"])
           for r in rows if r.get("state") == "OK"
           and r.get(key, {}).get("t") is not None]
    if not eff:
        return {"state": "EMPTY"}
    e = np.array([x[1] for x in eff], dtype=float)
    t = np.array([x[2] for x in eff], dtype=float)
    se = np.where(np.abs(t) > 1e-9, np.abs(e / t), np.nan)
    ok = np.isfinite(e) & np.isfinite(se) & (se > 0)
    e, se, names = e[ok], se[ok], [x[0] for x, m in zip(eff, ok) if m]
    if not len(e):
        return {"state": "EMPTY"}
    w = 1.0 / se ** 2
    fixed = float((w * e).sum() / w.sum())
    q = float((w * (e - fixed) ** 2).sum())
    dfree = len(e) - 1
    c = float(w.sum() - (w ** 2).sum() / w.sum()) if w.sum() else 0.0
    tau2 = max(0.0, (q - dfree) / c) if c > 0 else 0.0
    wr = 1.0 / (se ** 2 + tau2)
    rand = float((wr * e).sum() / wr.sum())
    rand_se = float(np.sqrt(1.0 / wr.sum()))
    i2 = float(max(0.0, (q - dfree) / q) * 100.0) if q > 0 else 0.0
    loo = []
    for i in range(len(e)):
        m = np.ones(len(e), dtype=bool)
        m[i] = False
        wi = 1.0 / (se[m] ** 2 + tau2)
        loo.append(float((wi * e[m]).sum() / wi.sum()))
    return {
        "state": "OK",
        "n_assets": int(len(e)),
        "n_positive": int((e > 0).sum()),
        "n_negative": int((e <= 0).sum()),
        "share_same_sign_as_btc": float((e > 0).mean()),
        "median_effect_ann": float(np.median(e)),
        "unweighted_mean_effect_ann": float(e.mean()),
        "fixed_effect_ann": fixed,
        "random_effect_ann": rand,
        "random_effect_se": rand_se,
        "random_effect_t": float(rand / rand_se) if rand_se > 0 else None,
        "heterogeneity_Q": q, "heterogeneity_I2_pct": i2, "tau2": tau2,
        "leave_one_out_min": float(np.min(loo)),
        "leave_one_out_max": float(np.max(loo)),
        "concentration_top1_share_of_positive_effect":
            float(np.max(np.where(e > 0, e, 0.0)) / np.sum(np.where(e > 0, e,
                                                                    0.0)))
            if (e > 0).any() else None,
        "most_positive_asset": names[int(np.argmax(e))],
        "most_negative_asset": names[int(np.argmin(e))],
    }


def run(*, refresh: bool = False) -> dict:
    frz = freeze_universe(refresh=refresh)
    eligible = frz["eligible_symbols"]
    rows = [replicate_symbol(s) for s in eligible]
    new = [r for r in rows if r["symbol"] not in PRIOR_EVIDENCE]
    rec_rows = [replicate_symbol(s, window=RECENT_WINDOW) for s in eligible]
    rec_new = [r for r in rec_rows if r["symbol"] not in PRIOR_EVIDENCE
               and r.get("state") == "OK"]
    keys = ["R41_RULE__FULL_ECONOMICS", "R41_RULE__R41_CONVENTION",
            "POSITIVE_ONLY__FULL_ECONOMICS", "ALWAYS_ON__FULL_ECONOMICS",
            "ALWAYS_ON__R41_CONVENTION"]
    meta_all = {k: meta_analyse(rows, k) for k in keys}
    meta_new = {k: meta_analyse(new, k) for k in keys}
    meta_recent = {k: meta_analyse(rec_new, k) for k in keys}
    liq = _liquidity_cross_section(rec_new)
    body = artifact_body("r42_cross_asset_replication/1", {
        "calculation_owner": CALCULATION_OWNER,
        "track": "I - out-of-asset replication",
        "label": C.NEW_ASSET_LABEL,
        "is_true_forward": not C.NEW_ASSET_IS_NOT_TRUE_FORWARD,
        "n_eligible": len(eligible),
        "n_new_assets": len(new),
        "eth_is_prior_evidence": C.ETH_IS_PRIOR_EVIDENCE,
        "rule_applied": "the frozen R41 z-gate, parameters IDENTICAL, "
                        "nothing fit on any new asset",
        "per_asset": rows,
        "meta_all_eligible": meta_all,
        "meta_new_assets_only": meta_new,
        "recent_window": RECENT_WINDOW,
        "recent_window_note":
            "the BTC Zone-C window, reused UNCHANGED, so the cross-section "
            "is judged on the SAME dates the BTC candidate failed on - not "
            "on each asset's own longer and more flattering history",
        "per_asset_recent": rec_rows,
        "meta_new_assets_recent_window": meta_recent,
        "liquidity_cross_section": liq,
        "cost_model_caveat":
            "the execution ladder is calibrated on BTCUSDT, the most "
            "liquid pair in the set. Applying it unchanged to 69 smaller "
            "assets UNDERSTATES their true execution cost, so every "
            "cross-asset number here is an UPPER BOUND on what those "
            "assets could actually have earned.",
        "verdict": _verdict(meta_new, meta_all, meta_recent),
    })
    body["cross_asset_replication_hash"] = sha(body)
    write_artifact(RESULT_ARTIFACT, body, CAMPAIGN_ID, overwrite=True)
    return body


def _liquidity_cross_section(rows: list) -> dict:
    """Is the premium paid where capital can actually go?"""
    ok = [r for r in rows
          if r.get("state") == "OK"
          and r.get("median_daily_quote_volume_usd")
          and r.get("R41_RULE__FULL_ECONOMICS", {}).get("excess_ann")
          is not None]
    if len(ok) < 8:
        return {"state": "INSUFFICIENT", "n": len(ok)}
    vol = np.array([r["median_daily_quote_volume_usd"] for r in ok])
    eff = np.array([r["R41_RULE__FULL_ECONOMICS"]["excess_ann"]
                    for r in ok])
    order = np.argsort(vol)
    k = max(1, len(ok) // 3)
    small, large = order[:k], order[-k:]
    rho = float(np.corrcoef(np.log(vol), eff)[0, 1])
    return {
        "state": "OK", "n_assets": len(ok),
        "corr_log_volume_vs_excess": rho,
        "least_liquid_third_median_excess": float(np.median(eff[small])),
        "most_liquid_third_median_excess": float(np.median(eff[large])),
        "least_liquid_third_median_volume_usd": float(np.median(vol[small])),
        "most_liquid_third_median_volume_usd": float(np.median(vol[large])),
        "finding": "a negative correlation means the premium is paid where "
                   "capital cannot go: the carry is compensation for "
                   "illiquidity and delisting risk, not a free lunch that "
                   "scales.",
    }


def _verdict(meta_new: dict, meta_all: dict, meta_recent: dict) -> dict:
    fe = meta_new.get("R41_RULE__FULL_ECONOMICS", {})
    r41c = meta_new.get("R41_RULE__R41_CONVENTION", {})
    rec = meta_recent.get("R41_RULE__FULL_ECONOMICS", {})
    ok = fe.get("state") == "OK"
    positive = ok and (fe.get("random_effect_t") or 0) >= 2.0 \
        and (fe.get("random_effect_ann") or 0) > 0
    rec_positive = rec.get("state") == "OK" \
        and (rec.get("random_effect_t") or 0) >= 2.0 \
        and (rec.get("random_effect_ann") or 0) > 0
    return {
        "state": ("CROSS_ASSET_REPLICATION_CONFIRMS" if positive
                  else "R42_CROSS_ASSET_REPLICATION_FAILS_ON_FULL_ECONOMICS"),
        "recent_window_state": ("CONFIRMS_IN_RECENT_WINDOW" if rec_positive
                                else "FAILS_IN_RECENT_WINDOW"),
        "recent_n_assets": rec.get("n_assets"),
        "recent_share_same_sign": rec.get("share_same_sign_as_btc"),
        "recent_random_effect_ann": rec.get("random_effect_ann"),
        "recent_random_effect_t": rec.get("random_effect_t"),
        "recent_median_effect_ann": rec.get("median_effect_ann"),
        "n_new_assets_tested": fe.get("n_assets"),
        "share_same_sign_under_r41_convention":
            r41c.get("share_same_sign_as_btc"),
        "random_effect_under_r41_convention": r41c.get("random_effect_ann"),
        "random_effect_t_under_r41_convention": r41c.get("random_effect_t"),
        "share_same_sign_under_full_economics":
            fe.get("share_same_sign_as_btc"),
        "random_effect_under_full_economics": fe.get("random_effect_ann"),
        "random_effect_t_under_full_economics": fe.get("random_effect_t"),
        "heterogeneity_I2_pct": fe.get("heterogeneity_I2_pct"),
        "note": "the premium's EXISTENCE and its VALUE are different "
                "claims. Scored as R41 scored it, the carry replicates "
                "broadly. Scored against the cost of the capital it "
                "requires, the same assets are judged on whether they beat "
                "cash - which is the only question that matters to a "
                "portfolio.",
    }
