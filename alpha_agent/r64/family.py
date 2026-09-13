"""alpha_agent.r64.family - family-aware multiple testing.

Two controls, both pre-registered:

    campaign BH   Benjamini-Hochberg at q = 0.10 across EVERY R64 conditional
                  p-value and, separately, across every R64 economic p-value
                  (``alpha_agent.r63.sensitivity.bh_fdr``, reused)
    family Holm   Holm step-down at alpha = 0.05 WITHIN the FX CARRY family:
                  the 4 horizons x 2 modes are ONE economic family and its
                  family p-value is the Holm-adjusted minimum

The R63 campaign-wide BH result (m = 978) is quoted, never re-run on a
smaller family to manufacture survival.
"""
from __future__ import annotations

import math

from alpha_agent.r63 import sensitivity as S

from . import (AC_FX, BH_Q, CARRY_FAMILY, DIM_CURVE_CARRY, FX_CARRY_FAMILY_ID,
               FX_CARRY_FAMILY_MODES, HOLM_ALPHA, HORIZONS, write_artifact)

CALCULATION_OWNER = "alpha_agent.r64.family"
ARTIFACT_NAME = "r64_fx_carry_family.json"
F_SURVIVES = "FAMILY_SURVIVES_HOLM"
F_FAILS = "FAMILY_FAILS_HOLM"
F_INCOMPLETE = "FAMILY_INCOMPLETE"


def holm(pvals: dict, alpha: float = HOLM_ALPHA) -> dict:
    """Holm step-down. ``pvals`` {key: one-sided p}. Returns adjusted p-values
    (monotone, capped at 1), rejections at ``alpha`` and the family p (the
    smallest adjusted p)."""
    items = sorted([(k, float(v)) for k, v in pvals.items()
                    if v is not None and math.isfinite(float(v))], key=lambda kv: kv[1])
    m = len(items)
    adjusted: dict = {}
    running = 0.0
    for i, (k, p) in enumerate(items):
        adj = min(1.0, (m - i) * p)
        running = max(running, adj)
        adjusted[k] = running
    rejected = {k: adjusted[k] <= alpha for k in adjusted}
    return {"method": "Holm step-down", "alpha": alpha, "m": m,
            "adjusted": adjusted, "rejected": rejected,
            "family_p": (min(adjusted.values()) if adjusted else None),
            "n_rejected": sum(1 for v in rejected.values() if v)}


def campaign_bh(cells: list, q: float = BH_Q) -> dict:
    """The two campaign-wide BH families over the R64 cells."""
    cond = {c["cell_id"]: (c.get("conditional") or {}).get("p_one_sided") for c in cells
            if c.get("conditional")}
    econ = {c["cell_id"]: (c.get("r64_economics") or {}).get("p_increment_one_sided")
            for c in cells if c.get("r64_economics")}
    return {"conditional": S.bh_fdr(cond, q), "economic": S.bh_fdr(econ, q),
            "n_conditional_raw_below_0p05": sum(1 for p in cond.values()
                                                if p is not None and p < 0.05),
            "n_economic_raw_below_0p05": sum(1 for p in econ.values()
                                             if p is not None and p < 0.05)}


def fx_carry_family_rows(cells: list) -> list:
    return sorted([c for c in cells if c.get("scope") == AC_FX
                   and c.get("dimension") == DIM_CURVE_CARRY
                   and c.get("mode") in FX_CARRY_FAMILY_MODES],
                  key=lambda c: (c.get("mode"), int(c.get("horizon") or 0)))


def fx_carry_family(cells: list, *, write: bool = True) -> dict:
    """The FX CARRY family record: per-horizon rows, Holm within the family
    for the conditional and for the R64 economic increments, and the family
    verdict."""
    rows = fx_carry_family_rows(cells)
    expected = len(HORIZONS) * len(FX_CARRY_FAMILY_MODES)
    cond_p = {c["cell_id"]: (c.get("conditional") or {}).get("p_one_sided") for c in rows}
    econ_p = {c["cell_id"]: (c.get("r64_economics") or {}).get("p_increment_one_sided")
              for c in rows}
    h_cond, h_econ = holm(cond_p), holm(econ_p)
    per = []
    for c in rows:
        co, e = c.get("conditional") or {}, c.get("r64_economics") or {}
        per.append({"cell_id": c["cell_id"], "mode": c.get("mode"), "horizon": c.get("horizon"),
                    "conditional_t": co.get("t"), "conditional_p": co.get("p_one_sided"),
                    "conditional_p_holm": h_cond["adjusted"].get(c["cell_id"]),
                    "lockbox_sign_agrees": co.get("lockbox_sign_agrees"),
                    "economic_t": e.get("t_increment"), "economic_p": e.get("p_increment_one_sided"),
                    "economic_p_holm": h_econ["adjusted"].get(c["cell_id"]),
                    "ann_net_increment": e.get("ann_net_increment"),
                    "ann_net_increment_at_2x_cost": e.get("ann_net_increment_at_2x_cost"),
                    "sharpe_increment": e.get("sharpe_increment"),
                    "r63_verdict": c.get("verdict"), "r64_verdict": c.get("r64_verdict"),
                    "reproduction_matches": (c.get("reproduction") or {}).get("matches")})
    if len(rows) < expected:
        verdict = F_INCOMPLETE
    elif (h_cond["n_rejected"] or 0) > 0 and (h_econ["n_rejected"] or 0) > 0:
        verdict = F_SURVIVES
    else:
        verdict = F_FAILS
    body = {"schema": "r64_fx_carry_family/1", "calculation_owner": CALCULATION_OWNER,
            "family_id": FX_CARRY_FAMILY_ID, "economic_family": CARRY_FAMILY,
            "rule": ("horizons %s in modes %s are ONE economic family; Holm step-down at "
                     "alpha %.2f within the family; the family p is the smallest adjusted p"
                     % (list(HORIZONS), list(FX_CARRY_FAMILY_MODES), HOLM_ALPHA)),
            "n_cells": len(rows), "n_expected": expected, "rows": per,
            "holm_conditional": h_cond, "holm_economic": h_econ,
            "family_p_conditional": h_cond["family_p"], "family_p_economic": h_econ["family_p"],
            "verdict": verdict}
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body
