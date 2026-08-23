"""alpha_agent.r40.availability - feature-availability integrity (Track E).

The defect this module exists to eliminate: a feature family that is
effectively ABSENT during selection (Zone B) and suddenly LIVE during
confirmation / forward operation, without the model or the search process
knowing it. Release 39 measured exactly that for the v1 LATENT / GRAPH
features inside the frozen WIDE book; this module turns the finding into a
diagnostic that runs BEFORE any model is evaluated, and into a hard rule:

    NO FEATURE FAMILY MAY BE CLAIMED AS SELECTED INFORMATION IF IT WAS
    EFFECTIVELY UNAVAILABLE THROUGH THE SELECTION PERIOD.

Diagnostics (all measured, never assumed): coverage by zone, by era
(five-year bins), by market, by asset class; coverage drift by year;
missingness-state transitions per (market, feature) and the feature's
activation date; and CAUSAL availability masks - a mask value at decision
date d depends only on whether the feature is observable at d, so a model
that receives the mask as an input knows, point-in-time, which information
it is looking at. Missing values are still imputed with TRAIN-ONLY medians
(the frozen WIDE convention) - the mask is what stops the imputation from
silently becoming a different model.

Admissibility (declared here, before measurement): a feature is admissible
for a corrected successor if it is finite in at least
``MIN_SELECTION_COVERAGE`` of BOTH Zone-A and Zone-B rows. A feature below
the floor in either zone is ``INADMISSIBLE_SELECTION_UNAVAILABLE`` and may
not enter the successor's bundle, whatever it does in Zone C.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .. import r39 as _r39
from ..r39.representation_factory import (
    CLASSICAL_FUT,
    GRAPH_FEATURES,
    LATENT_FEATURES,
    MACRO_COLS,
    MSTRUCT_FEATURES,
    SPECTRAL_FEATURES,
)
from . import CAMPAIGN_ID, artifact_body, campaign_dir

CALCULATION_OWNER = "alpha_agent.r40.availability"
ARTIFACT_NAME = "wide_availability_defect_report.json"

#: Declared BEFORE measurement.
MIN_SELECTION_COVERAGE = 0.50
SELECTION_ZONES = ("ZONE_A", "ZONE_B")
ERA_YEARS = 5

WIDE_FAMILIES = {
    "CLASSICAL": list(CLASSICAL_FUT),
    "MACRO": list(MACRO_COLS),
    "SPECTRAL": list(SPECTRAL_FEATURES),
    "LATENT": list(LATENT_FEATURES),
    "GRAPH": list(GRAPH_FEATURES),
    "MSTRUCT": list(MSTRUCT_FEATURES),
}
REPAIRED_FAMILIES = {
    "LATENT2": ["latent2_load_pc1", "latent2_load_pc2", "latent2_load_pc3",
                "latent2_resid_mom"],
    "GRAPH2": ["graph2_leadlag"],
}

MASK_SUFFIX = "__avail"


def _finite(s: pd.Series) -> np.ndarray:
    return np.isfinite(s.to_numpy(dtype=float))


def _era(dates: pd.Series) -> pd.Series:
    y = pd.to_datetime(dates).dt.year
    start = (y // ERA_YEARS) * ERA_YEARS
    return start.astype(str) + "-" + (start + ERA_YEARS - 1).astype(str)


def coverage_report(panel: pd.DataFrame, features: list) -> dict:
    """Coverage of every feature by zone / era / market / asset class, plus
    drift by year, missingness transitions and activation dates."""
    p = panel
    feats = [f for f in features if f in p.columns]
    fin = pd.DataFrame({f: _finite(p[f]) for f in feats}, index=p.index)
    zone = p["zone"].astype(str)
    era = _era(p["decision_date"])
    year = pd.to_datetime(p["decision_date"]).dt.year
    out = {}
    for f in feats:
        col = fin[f]
        by_zone = {z: round(float(col[zone == z].mean()), 4)
                   for z in sorted(zone.unique())}
        by_era = {e: round(float(col[era == e].mean()), 4)
                  for e in sorted(era.unique())}
        by_year = {int(y): round(float(col[year == y].mean()), 4)
                   for y in sorted(year.unique())}
        by_class = {c: round(float(col[p["asset_class"] == c].mean()), 4)
                    for c in sorted(p["asset_class"].astype(str).unique())}
        by_market = {m: round(float(col[p["market_id"] == m].mean()), 4)
                     for m in sorted(p["market_id"].astype(str).unique())}
        # activation date + transitions per market
        activation, transitions = {}, 0
        for m, grp in p.assign(_f=col).sort_values("decision_date") \
                .groupby("market_id"):
            v = grp["_f"].to_numpy()
            if v.any():
                activation[str(m)] = str(pd.Timestamp(
                    grp["decision_date"].to_numpy()[int(np.argmax(v))]
                ).date())
                transitions += int((v[1:] != v[:-1]).sum())
        acts = sorted(activation.values())
        a_ok = by_zone.get("ZONE_A", 0.0) >= MIN_SELECTION_COVERAGE
        b_ok = by_zone.get("ZONE_B", 0.0) >= MIN_SELECTION_COVERAGE
        out[f] = {
            "overall": round(float(col.mean()), 4),
            "by_zone": by_zone, "by_era": by_era, "by_year": by_year,
            "by_asset_class": by_class,
            "by_market": by_market,
            "market_coverage_min": min(by_market.values()) if by_market
            else None,
            "market_coverage_max": max(by_market.values()) if by_market
            else None,
            "first_activation_date": acts[0] if acts else None,
            "median_activation_date": acts[len(acts) // 2] if acts else None,
            "markets_ever_available": len(activation),
            "missingness_transitions": transitions,
            "coverage_drift_abs_year_to_year": round(float(np.nanmean(
                np.abs(np.diff(list(by_year.values()))))), 4)
            if len(by_year) > 1 else None,
            "selection_admissible": bool(a_ok and b_ok),
            "admissibility_state":
                "ADMISSIBLE" if (a_ok and b_ok) else
                "INADMISSIBLE_SELECTION_UNAVAILABLE",
            "zone_c_live_but_selection_absent": bool(
                by_zone.get("ZONE_C", 0.0) >= MIN_SELECTION_COVERAGE
                and not (a_ok and b_ok)),
        }
    return out


def family_summary(cov: dict, families: dict) -> dict:
    fam = {}
    for name, cols in families.items():
        rows = [cov[c] for c in cols if c in cov]
        if not rows:
            fam[name] = {"state": "NOT_IN_PANEL"}
            continue
        fam[name] = {
            "n_features": len(rows),
            "zone_a_mean_coverage": round(float(np.mean(
                [r["by_zone"].get("ZONE_A", 0.0) for r in rows])), 4),
            "zone_b_mean_coverage": round(float(np.mean(
                [r["by_zone"].get("ZONE_B", 0.0) for r in rows])), 4),
            "zone_c_mean_coverage": round(float(np.mean(
                [r["by_zone"].get("ZONE_C", 0.0) for r in rows])), 4),
            "admissible_features": [c for c in cols
                                    if c in cov and cov[c][
                                        "selection_admissible"]],
            "inadmissible_features": [c for c in cols
                                      if c in cov and not cov[c][
                                          "selection_admissible"]],
            "zone_c_live_but_selection_absent": [
                c for c in cols if c in cov
                and cov[c]["zone_c_live_but_selection_absent"]],
        }
        fam[name]["family_state"] = (
            "FULLY_ADMISSIBLE" if not fam[name]["inadmissible_features"]
            else "PARTIALLY_ADMISSIBLE" if fam[name]["admissible_features"]
            else "INADMISSIBLE_SELECTION_UNAVAILABLE")
    return fam


def add_causal_masks(panel: pd.DataFrame, features: list) -> tuple:
    """Append one causal availability mask per feature (1.0 if the feature
    is finite at that row's decision date, else 0.0). The mask depends only
    on observability at d - never on a future row."""
    p = panel
    names = []
    new = {}
    for f in features:
        if f not in p.columns:
            continue
        name = f + MASK_SUFFIX
        new[name] = _finite(p[f]).astype(float)
        names.append(name)
    if new:
        p = pd.concat([p, pd.DataFrame(new, index=p.index)], axis=1)
    return p, names


def write_report(panel: pd.DataFrame, *, frozen_features: list,
                 campaign_id: str = CAMPAIGN_ID) -> dict:
    """WIDE_AVAILABILITY_DEFECT_REPORT over the frozen panel."""
    all_feats = [c for fam in WIDE_FAMILIES.values() for c in fam] + \
        [c for fam in REPAIRED_FAMILIES.values() for c in fam]
    cov = coverage_report(panel, all_feats)
    fam = family_summary(cov, {**WIDE_FAMILIES, **REPAIRED_FAMILIES})
    frozen = [f for f in frozen_features if f in cov]
    defect = {
        "frozen_wide_features": frozen,
        "frozen_features_inadmissible_under_rule": [
            f for f in frozen if not cov[f]["selection_admissible"]],
        "frozen_features_zone_c_live_but_selection_absent": [
            f for f in frozen if cov[f]["zone_c_live_but_selection_absent"]],
        "frozen_features_partial_selection_coverage": {
            f: {"ZONE_A": cov[f]["by_zone"].get("ZONE_A"),
                "ZONE_B": cov[f]["by_zone"].get("ZONE_B"),
                "ZONE_C": cov[f]["by_zone"].get("ZONE_C")}
            for f in frozen
            if min(cov[f]["by_zone"].get("ZONE_A", 0.0),
                   cov[f]["by_zone"].get("ZONE_B", 0.0)) < 0.95},
    }
    body = artifact_body("r40_wide_availability_defect_report/1", {
        "calculation_owner": CALCULATION_OWNER,
        "rule": "NO FEATURE FAMILY MAY BE CLAIMED AS SELECTED INFORMATION "
                "IF IT WAS EFFECTIVELY UNAVAILABLE THROUGH THE SELECTION "
                "PERIOD",
        "min_selection_coverage": MIN_SELECTION_COVERAGE,
        "selection_zones": list(SELECTION_ZONES),
        "era_years": ERA_YEARS,
        "declared_before_measurement": True,
        "features": cov,
        "families": fam,
        "frozen_wide_defect": defect,
        "masks": "one causal availability mask per feature "
                 "(<feature>%s); imputation stays train-only median"
                 % MASK_SUFFIX,
        "original_wide_untouched": True,
    })
    body["availability_report_hash"] = _r39.sha(body)
    _r39.write_json(campaign_dir(campaign_id) / ARTIFACT_NAME, body,
                    immutable=False)
    return body
