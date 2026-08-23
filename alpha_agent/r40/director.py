"""alpha_agent.r40.director - ONE prepared research director per process.

Release 40 runs every historical experiment through the Release-39
continuation director (``Director2``): the same frozen universal state, the
same deterministic representations, the same economic judge, the same
Zone-A-fit / Zone-B-judge protocol, the same masked evaluation that cannot
touch Zone C - with the reuse ledger pointed at the R40 campaign (which the
R39 owner serves under the R40 root). Nothing here evaluates, judges, costs
or controls anything itself.

Extra surfaces the R40 tracks need are attached ONCE here: the repaired
calendar-grid latent/graph features (continuation owner), the
international-rates extension (cache hit from the continuation campaign),
the causal availability masks (R40 availability owner), and the sequence
bundle for the deep lanes.
"""
from __future__ import annotations

import time

import numpy as np
import pandas as pd

from .. import r39 as _r39
from ..r39 import continuation as CONT
from ..r39.continuation_director import (
    Director2,
    add_latent_graph_repaired,
    build_intl_rates_extension,
    register_bundles,
    register_sequence_bundle,
)
from ..r39.discovery_director import _strip
from . import CAMPAIGN_ID, campaign_dir
from . import availability as AV
from . import burden_ledger as BL

CALCULATION_OWNER = "alpha_agent.r40.director"

_SESSION: dict = {}


def log(msg: str) -> None:
    print("[r40 %s] %s" % (time.strftime("%H:%M:%S"), msg), flush=True)


def session(campaign_id: str = CAMPAIGN_ID, *, with_sequence: bool = True,
            with_intl_rates: bool = True) -> Director2:
    """The prepared director (cached per process)."""
    if _SESSION.get("director") is not None:
        return _SESSION["director"]
    campaign_dir(campaign_id)
    BL.inherit(campaign_id)
    log("loading the frozen R39 universal state")
    state = CONT.load_frozen_state()
    d2 = Director2(state, campaign_id)
    log("regenerating representations (deterministic)")
    d2.prepare_representations()
    fut = d2.state["fut"]
    log("adding repaired calendar-grid latent/graph (continuation owner)")
    fut, names, coverage = add_latent_graph_repaired(fut)
    log("adding causal availability masks (R40 owner)")
    all_feats = [c for fam in AV.WIDE_FAMILIES.values() for c in fam] + \
        [c for fam in AV.REPAIRED_FAMILIES.values() for c in fam]
    fut, mask_names = AV.add_causal_masks(fut, all_feats)
    d2.state["fut"] = fut.sort_values(["decision_date", "market_id"]) \
        .reset_index(drop=True)
    d2.bundles["FUT_LATENT2"] = [n for n in names if n.startswith("latent2")]
    d2.bundles["FUT_GRAPH2"] = ["graph2_leadlag", "ret_1m", "vol_63"]
    _SESSION["repaired_coverage"] = coverage
    _SESSION["mask_names"] = mask_names
    if with_intl_rates:
        ext = build_intl_rates_extension(d2.state,
                                         CONT.CONTINUATION_CAMPAIGN_ID)
        _SESSION["intl_rates"] = {k: v for k, v in ext.items()
                                  if k != "markets"}
        log("international rates extension: %s" % ext.get("state"))
    register_bundles(d2)
    if with_sequence:
        register_sequence_bundle(d2)
    _SESSION["director"] = d2
    return d2


def state_info() -> dict:
    d2 = _SESSION.get("director")
    if d2 is None:
        return {}
    fut = d2.state["fut"]
    return {"fut_rows": int(len(fut)),
            "fut_markets": int(fut["market_id"].nunique()),
            "fut_columns": int(fut.shape[1]),
            "zones": {k: int(v) for k, v in
                      fut["zone"].value_counts().items()},
            "repaired_coverage": _SESSION.get("repaired_coverage"),
            "intl_rates": _SESSION.get("intl_rates"),
            "n_masks": len(_SESSION.get("mask_names") or [])}


def summarise(rep: dict) -> dict:
    keep = ("state", "after_cost_excess_annualised",
            "after_cost_excess_t_stat", "sharpe", "annualised_turnover",
            "annualised_cost", "periods", "control", "n_fit_rows",
            "protocol", "hit_rate", "max_drawdown", "volatility_annualised")
    out = {k: rep.get(k) for k in keep if k in rep}
    ic = rep.get("ic") or {}
    out["mean_ic"] = ic.get("mean_ic")
    out["ic_t"] = ic.get("t_stat")
    return out


def zone_b(cand: dict, *, stage: str, d2: Director2 = None) -> dict:
    """Fit Zone A, judge Zone B, ledger-counted in the R40 campaign. Returns
    the full report (with the net stream and dates)."""
    d2 = d2 or session()
    return d2.eval_zone_b(cand, stage=stage)


def stream(rep: dict) -> pd.Series:
    if not rep or rep.get("state") != "OK":
        return pd.Series(dtype=float)
    return pd.Series(np.asarray(rep["book_net"], dtype=float),
                     index=pd.DatetimeIndex(rep["book_dates"]))


def cost_stress(cand: dict, *, d2: Director2 = None) -> dict:
    """2x modelled costs on Zone B (fit Zone A). Same candidate id, so this
    adds a reuse count, never a distinct trial."""
    d2 = d2 or session()
    rep = d2.evaluate_on_zone(cand, fit_zones=("ZONE_A",),
                              eval_zone="ZONE_B", cost_multiplier=2.0,
                              record_reuse=True)
    return summarise(rep)


def halves_same_sign(rep: dict) -> dict:
    from ..r39 import judge as J
    diff = rep.get("excess_diff_series")
    if diff is None:
        diff = []
    return J.sign_split_robustness(np.asarray(diff, dtype=float))


def strip(rep: dict) -> dict:
    return _strip(rep)


def correlation(a: pd.Series, b: pd.Series) -> float:
    """Correlation of two net streams on their shared decision dates."""
    if a.empty or b.empty:
        return float("nan")
    j = pd.concat([a.rename("a"), b.rename("b")], axis=1, join="inner") \
        .dropna()
    if len(j) < 24 or j["a"].std() == 0 or j["b"].std() == 0:
        return float("nan")
    return float(j["a"].corr(j["b"]))


def write_state_artifact(campaign_id: str = CAMPAIGN_ID) -> dict:
    from . import artifact_body
    body = artifact_body("r40_director_state/1", {
        "calculation_owner": CALCULATION_OWNER,
        **state_info(),
        "protocol": "fit ZONE_A, judge ZONE_B, Zone C structurally "
                    "unreachable through masked evaluation; every Zone-B "
                    "evaluation recorded in the R40 reuse ledger (R39 owner)",
    })
    body["director_state_hash"] = _r39.sha(body)
    _r39.write_json(campaign_dir(campaign_id) / "director_state.json", body,
                    immutable=False)
    return body
