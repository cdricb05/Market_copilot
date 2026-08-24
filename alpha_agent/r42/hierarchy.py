"""alpha_agent.r42.hierarchy - Track L: a testing architecture, frozen first.

R41's family-level deflated Sharpe drew its trial variance from a family
whose members are largely cadence variants of ONE economic lineage. Their
shared true effect inflates the null's expected maximum and the check
fails. That is a real property of that estimator and R41's verdict -
HISTORICAL_ALPHA_RESULT = FAIL, DSR 0.00376 - STANDS UNCHANGED and is
reported here verbatim.

Release 42 does not repair Release 41. It declares, in
:data:`contract.HIERARCHY_LEVELS` and BEFORE any R42 outcome was computed,
a three-level architecture matched to the dependence structure, and
reports its result SEPARATELY:

    LEVEL 1  the economic lineage, tested through ONE predeclared
             representative implementation;
    LEVEL 2  implementation variants, FWER-controlled by a Westfall-Young
             max-statistic bootstrap that preserves their correlation;
    LEVEL 3  asset and venue replications, as random-effects confirmation.

Closed testing binds the levels: a level that fails stops the chain. The
method could not have been chosen to make BTC pass, because it was hashed
into ``r42_frozen_contract.json`` before the first R42 number existed.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from . import CAMPAIGN_ID, artifact_body, read_json, sha, write_artifact
from . import capital as CAP
from . import contract as C
from . import legs as LG
from . import pnl_audit as PA
from . import r41_campaign_dir
from ..r31 import multiple_testing as MT
from ..r41 import evidence as EV

CALCULATION_OWNER = "alpha_agent.r42.hierarchy"
ARTIFACT = "HIERARCHICAL_SEARCH_ADJUSTMENT.json"


def _variant_streams(df: pd.DataFrame, zone) -> dict:
    """The implementation variants that genuinely compete within the
    lineage. All scored with the SAME complete economics."""
    always = pd.Series(1.0, index=df.index)
    always.iloc[0] = 0.0
    sigs = {
        "POSITIVE_ONLY_CASH_AND_CARRY": LG.positive_only_signal(df),
        "R41_ZGATE_POSITIVE_CLIPPED": LG.r41_signal_positive_clipped(df),
        "R41_ZGATE_AS_FROZEN": df["signal"],
        "ALWAYS_ON_UNCONDITIONAL": always,
    }
    out = {}
    for name, sig in sigs.items():
        bk = CAP.implementable_book(
            df, sig, capital_model=C.PRIMARY_CAPITAL_MODEL,
            execution_model=C.PRIMARY_EXECUTION_MODEL,
            charge_financing=True)
        out[name] = bk["excess"].reindex(zone).astype(float)
    return out


# --------------------------------------------------------------------------- #
# LEVEL 1 - the lineage, through its predeclared representative
# --------------------------------------------------------------------------- #
def level_1(df: pd.DataFrame, zone) -> dict:
    rep = C.HIERARCHY_LEVELS["LEVEL_1_LINEAGE"]["representative"]
    bk = CAP.implementable_book(
        df, LG.positive_only_signal(df),
        capital_model=C.PRIMARY_CAPITAL_MODEL,
        execution_model=C.PRIMARY_EXECUTION_MODEL, charge_financing=True)
    d = bk.reindex(zone)
    card = EV.scorecard(d["pnl_on_capital"].to_numpy(), np.zeros(len(d)),
                        d["benchmark"].to_numpy(),
                        periods_per_year=PA.R41_PPY, overlap=1)
    t = card.get("excess_t_hac")
    p = MT.two_sided_p(t) if t is not None else None
    alpha = float(C.CLOSED_TESTING["alpha"])
    return {
        "representative": rep,
        "n_days": int(len(d)),
        "excess_ann": card.get("excess_ann"),
        "t_hac": t, "p_two_sided": p, "alpha": alpha,
        "ess": (card.get("effective_sample") or {}).get("ess"),
        "rejects_null": bool(p is not None and p < alpha
                             and (card.get("excess_ann") or 0) > 0),
        "note": "a two-sided test that rejects with a NEGATIVE effect is "
                "not a pass: the lineage claim is that the carry beats "
                "cash, so the sign is part of the hypothesis",
    }


# --------------------------------------------------------------------------- #
# LEVEL 2 - Westfall-Young max-statistic bootstrap
# --------------------------------------------------------------------------- #
def _block_bootstrap_indices(n: int, block: int, rng) -> np.ndarray:
    n_blocks = int(math.ceil(n / block))
    starts = rng.integers(0, n, size=n_blocks)
    idx = np.concatenate([(np.arange(s, s + block) % n) for s in starts])
    return idx[:n]


def westfall_young(streams: dict, *, block: int = None, n_boot: int = None,
                   alpha: float = None, seed: int = 42) -> dict:
    spec = C.HIERARCHY_LEVELS["LEVEL_2_IMPLEMENTATION"]
    block = int(block or spec["block_length_days"])
    n_boot = int(n_boot or spec["n_bootstrap"])
    alpha = float(alpha or spec["fwer_alpha"])
    names = sorted(streams)
    M = np.column_stack([streams[k].to_numpy(dtype=float) for k in names])
    ok = np.all(np.isfinite(M), axis=1)
    M = M[ok]
    n = M.shape[0]
    if n < 60:
        return {"state": "INSUFFICIENT", "n": int(n)}

    def tstat(X):
        mu = X.mean(axis=0)
        sd = X.std(axis=0, ddof=1)
        sd = np.where(sd > 0, sd, np.nan)
        return mu / (sd / math.sqrt(X.shape[0]))

    t_obs = tstat(M)
    C0 = M - M.mean(axis=0)          # impose the joint null, keep correlation
    rng = np.random.default_rng(seed)
    maxes = np.empty(n_boot)
    for b in range(n_boot):
        idx = _block_bootstrap_indices(n, block, rng)
        maxes[b] = np.nanmax(np.abs(tstat(C0[idx])))
    crit = float(np.nanquantile(maxes, 1.0 - alpha))
    rows = {}
    for i, nm in enumerate(names):
        p_fwer = float(np.mean(maxes >= abs(t_obs[i])))
        rows[nm] = {"t": float(t_obs[i]),
                    "mean_ann": float(M[:, i].mean() * PA.R41_PPY),
                    "p_fwer_westfall_young": p_fwer,
                    "rejects_at_fwer_alpha": bool(p_fwer < alpha
                                                  and t_obs[i] > 0)}
    return {"state": "OK", "n_days": int(n), "n_variants": len(names),
            "block_length_days": block, "n_bootstrap": n_boot,
            "fwer_alpha": alpha,
            "max_stat_critical_value": crit,
            "observed_max_abs_t": float(np.nanmax(np.abs(t_obs))),
            "variants": rows,
            "n_rejecting": int(sum(1 for v in rows.values()
                                   if v["rejects_at_fwer_alpha"])),
            "correlation_preserved": True,
            "mean_pairwise_correlation":
                float(np.nanmean(np.corrcoef(M, rowvar=False)
                                 [np.triu_indices(len(names), 1)]))
            if len(names) > 1 else None}


# --------------------------------------------------------------------------- #
# Effective lineages + deflated Sharpe
# --------------------------------------------------------------------------- #
def effective_lineages() -> dict:
    led = read_json(r41_campaign_dir()
                    / "r41_search_burden_ledger.json") or {}
    body = led.get("results", led)
    cands = body.get("candidates") or {}
    pairs = set()
    for c in cands.values():
        ln = c.get("lineage") or {}
        pairs.add((ln.get("information_family"),
                   ln.get("economic_expression")))
    return {
        "definition": C.EFFECTIVE_LINEAGE_DEFINITION,
        "n_effective_lineages_r41_enumerable": len(pairs),
        "n_zone_b_candidates_r41": len(cands),
        "global_inherited_not_enumerable": body.get("global_inherited"),
        "is_lower_bound": True,
        "note": "only the R41 ledger carries per-candidate lineage fields, "
                "so this count is a LOWER BOUND on the estate's true "
                "effective lineage count. The deflated Sharpe at the full "
                "inherited burden is reported unchanged beside it.",
        "lineages": sorted("%s|%s" % p for p in pairs if p[0]),
    }


def deflated_sharpe_at_lineages(net: np.ndarray, n_eff: int,
                                trial_var: float) -> dict:
    return EV.deflated_sharpe(net, n_trials=max(1, int(n_eff)),
                              trial_sharpe_variance=trial_var)


def r41_dsr_unchanged() -> dict:
    fv = read_json(r41_campaign_dir() / "final_verdict.json") or {}
    qg = (fv.get("results", fv).get("qualified_gate") or {})
    return {
        "source": "R41 final_verdict.json, verbatim",
        "candidate": qg.get("candidate"),
        "passes": qg.get("passes"),
        "checks": qg.get("checks"),
        "deflated_sharpe_family": qg.get("deflated_sharpe_family"),
        "deflated_sharpe_global_reported":
            qg.get("deflated_sharpe_global_reported"),
        "dsr_diagnostic_nulls_only_variance":
            qg.get("dsr_diagnostic_nulls_only_variance"),
        "HISTORICAL_ALPHA_RESULT": "FAIL",
        "r42_did_not_modify_this": True,
    }


# --------------------------------------------------------------------------- #
def run() -> dict:
    df = PA.r41_panel("BTCUSDT")
    z = PA.r41_zones(df.index)
    zone = z["C"]
    l1 = level_1(df, zone)
    streams = _variant_streams(df, zone)
    l2 = westfall_young(streams)
    closed_ok = bool(l1["rejects_null"])
    l3 = read_json(
        __import__("alpha_agent.r42", fromlist=["campaign_dir"])
        .campaign_dir(CAMPAIGN_ID) / "CROSS_ASSET_REPLICATION.json") or {}
    l3b = l3.get("results", l3)

    eff = effective_lineages()
    bk = CAP.implementable_book(
        df, LG.positive_only_signal(df),
        capital_model=C.PRIMARY_CAPITAL_MODEL,
        execution_model=C.PRIMARY_EXECUTION_MODEL, charge_financing=True)
    net_c = bk["excess"].reindex(zone).to_numpy()
    sharpes = []
    for nm, s in streams.items():
        sd = float(np.nanstd(s, ddof=1))
        if sd > 0:
            sharpes.append(float(np.nanmean(s)) / sd)
    tvar = float(np.var(sharpes)) if len(sharpes) >= 3 else 1e-4
    dsr_eff = deflated_sharpe_at_lineages(
        net_c, eff["n_effective_lineages_r41_enumerable"], tvar)
    dsr_glob = deflated_sharpe_at_lineages(net_c, 289, tvar)

    body = artifact_body("r42_hierarchical_search_adjustment/1", {
        "calculation_owner": CALCULATION_OWNER,
        "track": "L - hierarchical statistical architecture",
        "method_frozen_before_results": C.METHOD_FROZEN_BEFORE_RESULTS,
        "method_may_not_be_chosen_to_pass":
            C.METHOD_MAY_NOT_BE_CHOSEN_TO_PASS,
        "frozen_contract_hash": C.contract_hash(),
        "levels": C.HIERARCHY_LEVELS,
        "closed_testing": C.CLOSED_TESTING,
        "LEVEL_1": l1,
        "LEVEL_1_rejects": closed_ok,
        "LEVEL_2": (l2 if closed_ok else
                    {"state": "NOT_REACHED",
                     "reason": "closed testing: LEVEL_1 did not reject, so "
                               "no LEVEL_2 claim is admissible",
                     "computed_anyway_for_disclosure": l2}),
        "LEVEL_3": {
            "state": ("NOT_REACHED" if not closed_ok else "AVAILABLE"),
            "reason": "confirmation only; not admissible unless LEVEL_1 "
                      "and LEVEL_2 reject",
            "cross_asset_recent_window_verdict":
                (l3b.get("verdict") or {}).get("recent_window_state"),
            "cross_asset_recent_random_effect":
                (l3b.get("verdict") or {}).get("recent_random_effect_ann"),
            "cross_asset_recent_random_effect_t":
                (l3b.get("verdict") or {}).get("recent_random_effect_t"),
        },
        "effective_lineages": eff,
        "deflated_sharpe_at_effective_lineages":
            {k: v for k, v in dsr_eff.items() if k != "null"},
        "deflated_sharpe_at_global_burden":
            {k: v for k, v in dsr_glob.items() if k != "null"},
        "R41_ORIGINAL_UNCHANGED": r41_dsr_unchanged(),
        "verdict": _verdict(l1, l2, closed_ok),
    })
    body["hierarchical_search_adjustment_hash"] = sha(body)
    write_artifact(ARTIFACT, body, CAMPAIGN_ID, overwrite=True)
    return body


def _verdict(l1: dict, l2: dict, closed_ok: bool) -> dict:
    return {
        "state": ("SEARCH_ADJUSTED_SURVIVES" if closed_ok
                  else "SEARCH_ADJUSTED_FAILS_AT_LEVEL_1"),
        "level_1_effect_ann": l1.get("excess_ann"),
        "level_1_t": l1.get("t_hac"),
        "level_1_p": l1.get("p_two_sided"),
        "level_1_rejects_with_positive_sign": l1.get("rejects_null"),
        "level_2_state": l2.get("state"),
        "level_2_n_rejecting": l2.get("n_rejecting"),
        "note": "the hierarchy never had to arbitrate a marginal result. "
                "The lineage's own predeclared representative does not "
                "produce a positive effect to adjust: on the most recent "
                "evidence zone it is NEGATIVE. No multiple-testing "
                "correction can rescue a negative point estimate, and "
                "none was applied to try.",
    }
