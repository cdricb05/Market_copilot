r"""alpha_agent.r61.mechanism_power - mechanism-aware power CLASSIFICATION (R89).

RESEARCH INFRASTRUCTURE ONLY. NO ORDERS, NO FILLS, NO PROMOTION, NO ADOPTION.
Registers no hypothesis, charges no burden, computes no candidate return.

THE DEFECT THIS EXISTS TO REMOVE (R88)
--------------------------------------
``alpha_agent.r61.power`` measures a panel's minimum detectable effect by
injecting a synthetic information coefficient into the real books under the
real gate. R88 then used that number as a HARD PRE-MEASUREMENT WALL through a
campaign-local rule: a cell was CLOSED at G7 when the director's own assumed
ex-ante IC fell below MDE_80. Three cells died that way before any return
existed, no experiment id was minted, and nothing could ever accumulate the
forward evidence that would have made the cell measurable. Two things were
wrong with that, and neither was the statistical standard:

1. A final-certification power requirement was applied as a wall BEFORE
   measurement, so an underpowered-but-clean mechanism was permanently closed
   instead of being routed to research-only forward observation.
2. A language model's guessed effect size (0.05, 0.05, 0.015) was the sole
   basis for a permanent closure. ``MDE > LLM_ESTIMATED_IC`` is a
   prioritisation fact, never a closure rule.

WHAT THIS MODULE IS
-------------------
A THIN ADAPTER behind the ONE canonical power owner. It adds no second
Monte-Carlo harness and moves no threshold. It owns exactly three things:

    effective_sample     raw observation count -> independent decision count
                         per RESEARCH STRUCTURE (cross-sectional, time-series,
                         panel, event-driven, relative-value, ensemble), from
                         the actual cross-market correlation and the actual
                         overlap of horizon over cadence. Overlapping or
                         correlated observations are never counted as
                         independent.
    classify             an MDE_80 (MEASURED by alpha_agent.r61.power wherever
                         the substrate is a certified book; ANALYTIC only
                         where the harness cannot represent the structure, and
                         then LABELLED as such) -> ONE of five power classes.
    g7_ruling            the deterministic G7 decision: a RESEARCH PATH, never
                         a permanent closure, and never on an agent prior.

THE CLASSIFICATION DOES NOT CHANGE FINAL QUALIFICATION. ``engines.gate`` is
untouched, ``r59.BH_Q``, ``OBS_FLOOR`` and the materiality floors are read, not
written, and ``tests/test_r89_power_expanded_alpha.py`` proves the frozen
final-gate thresholds are the same before and after. What the class decides is
WHICH RESEARCH PATH a clean candidate may take:

    POWER_STRONG / POWER_FEASIBLE   historical qualification (the R59 path)
    POWER_MARGINAL                  measure historically; if clean but not
                                    qualified, research-only forward
                                    observation is admissible
    POWER_WEAK                      research-only forward observation
                                    (historical measurement cannot decide it)
    POWER_UNIDENTIFIABLE            not researchable ON THIS EXPRESSION - the
                                    gate can never pass at any effect on the
                                    frozen grid; reopen on width, decisions or
                                    cadence. Still not a permanent closure of
                                    the MECHANISM.

WHERE THE CLASS BANDS COME FROM (recorded evidence, not a new opinion)
----------------------------------------------------------------------
The bands are in cross-sectional rank-IC units and are anchored to references
the estate already recorded BEFORE R89:

* ``alpha_agent.r61.power.RHO_GRID`` rationale (frozen R61): published
  cross-sectional rank ICs for equity factors sit at 0.01-0.05 monthly;
  commodity/futures carry and momentum up to ~0.10-0.15 on small panels.
* ``R88_G7_RULE_FROZEN.json`` (frozen 2026-09-30 before any curve): published
  multi-asset macro/fundamental monthly ICs 0.03-0.10; 0.15 is "outside what
  the literature reports".
* the estate's own settled evidence: 7,662 lockbox t-statistics in research
  memory, of which the 90th percentile per asset class sits at 0.95-1.66 and
  the maximum at 3.20 - i.e. nothing this apparatus has measured on price
  state has ever carried an IC near 0.15.

So: an expression that can detect 0.03 sees the LOWER edge of what is
plausible (STRONG); 0.05 the R88 declared priors (FEASIBLE); 0.10 only the
upper edge (MARGINAL); 0.15 only the R88 cap (WEAK); above it, or never on the
grid, nothing plausible (UNIDENTIFIABLE). These are constants, declared here,
and the record hash of every assessment binds them.
"""
from __future__ import annotations

import math
from typing import Optional

from .. import r59
from . import now_iso, stable_hash

MECHANISM_POWER_OWNER = "alpha_agent.r61.mechanism_power"
MECHANISM_POWER_VERSION = "R89_MECHANISM_AWARE_POWER_V1"
#: The ONE authority for a MEASURED minimum detectable effect.
MEASURED_MDE_OWNER = "alpha_agent.r61.power"

# --------------------------------------------------------------------------- #
# Research structures the estate can actually represent
# --------------------------------------------------------------------------- #
CROSS_SECTIONAL = "CROSS_SECTIONAL"
TIME_SERIES = "TIME_SERIES"
PANEL = "PANEL"
EVENT_DRIVEN = "EVENT_DRIVEN"
RELATIVE_VALUE = "RELATIVE_VALUE"
ENSEMBLE = "ENSEMBLE"
STRUCTURES = (CROSS_SECTIONAL, TIME_SERIES, PANEL, EVENT_DRIVEN,
              RELATIVE_VALUE, ENSEMBLE)

#: Which structures the canonical Monte-Carlo harness can MEASURE today, and
#: through which book. A structure absent here can only be assessed
#: analytically, and every such assessment is labelled ANALYTIC.
MC_MEASURABLE = {
    CROSS_SECTIONAL: "FUTURES_DATED_CONTRACT | EQUITY_TOPN (rank book)",
    PANEL: "FUTURES_DATED_CONTRACT | EQUITY_TOPN (rank book over a panel "
           "signal; identical geometry to CROSS_SECTIONAL)",
    ENSEMBLE: "FUTURES_DATED_CONTRACT | EQUITY_TOPN (one combined frozen "
              "score is one rank book)",
    RELATIVE_VALUE: "FUTURES_DATED_CONTRACT (rank book restricted to one "
                    "economic group; alpha_agent.r59.native.RV_GROUPS)",
}
NOT_MC_MEASURABLE = tuple(s for s in STRUCTURES if s not in MC_MEASURABLE)

MDE_BASIS_MEASURED = "MEASURED_BY_R61_MONTE_CARLO"
MDE_BASIS_ANALYTIC = "ANALYTIC_APPROXIMATION_CALIBRATED_TO_R61_MONTE_CARLO"

# --------------------------------------------------------------------------- #
# Power classes and the research path each one admits
# --------------------------------------------------------------------------- #
POWER_STRONG = "POWER_STRONG"
POWER_FEASIBLE = "POWER_FEASIBLE"
POWER_MARGINAL = "POWER_MARGINAL"
POWER_WEAK = "POWER_WEAK"
POWER_UNIDENTIFIABLE = "POWER_UNIDENTIFIABLE"
POWER_CLASSES = (POWER_STRONG, POWER_FEASIBLE, POWER_MARGINAL, POWER_WEAK,
                 POWER_UNIDENTIFIABLE)

#: Upper edge of each class in MDE_80 rank-IC units (see module docstring).
CLASS_BANDS = ((POWER_STRONG, 0.03), (POWER_FEASIBLE, 0.05),
               (POWER_MARGINAL, 0.10), (POWER_WEAK, 0.15))
CLASS_BAND_REFERENCES = {
    "0.03": "lower edge of published multi-asset macro/fundamental ICs "
            "(R88_G7_RULE_FROZEN.why_0_15) and of the R61 RHO_GRID rationale",
    "0.05": "the ex-ante prior R88 declared for its two macro cells; the "
            "director's STOP rule named 0.05 as the level honest priors reach",
    "0.10": "upper edge of published multi-asset macro/fundamental ICs",
    "0.15": "the frozen R88 G7 cap: 'outside what the literature reports'",
}

# --------------------------------------------------------------------------- #
# R92: EVENT-DRIVEN books are classified in EVENT-RETURN units
# --------------------------------------------------------------------------- #
#: THE DEFECT THIS REMOVES (R91). The EVENT_DRIVEN branch above produced an
#: MDE in cross-sectional rank-IC units and classified it against the rank-IC
#: bands. A 1-6 leg event book has no cross-section to rank, so the number had
#: no meaning, and a book with 189 independent lockbox observations - which
#: ``engines.gate`` can evaluate - was called POWER_UNIDENTIFIABLE
#: (R91_POWER_ASSESSMENTS_CORRECTED.json: C04/C05 mde_rho 0.172 > 0.15).
#: The event object is  event -> position -> realised event return,  so the
#: detectable effect is an EVENT RETURN:
#:
#:     MDE_per_event = t_required * sigma_event / sqrt(N_eff_lockbox)
#:     MDE_annualised = MDE_per_event * independent_observations_per_year
#:
#: with N_eff the MERGED independent lockbox observations (clusters and
#: overlapping windows count once, exactly as alpha_agent.agents_v2.event_book
#: reports ``effective_observations``), reduced for lag-1 serial dependence
#: by (1 - rho1) / (1 + rho1), and t_required at the frozen one-sided
#: BH_Q / (burden x tested windows) threshold and 80% power. The answer reads
#: "what annualised net excess return can this event sample detect", which is
#: the unit the materiality floor is written in. NOTHING ELSE MOVES: OBS_FLOOR
#: (on merged lockbox observations), BH_Q, the D/V/L boundaries and the final
#: gate are read, not written; a sparse book below OBS_FLOOR stays
#: POWER_UNIDENTIFIABLE in any units.
EVENT_POWER_CALIBRATION = "R92_EVENT_RETURN_UNITS_V1"
MDE_UNITS_RANK_IC = "CROSS_SECTIONAL_RANK_IC"
MDE_UNITS_EVENT_RETURN = "ANNUALISED_NET_EXCESS_RETURN"
#: Event class bands are MULTIPLES OF THE FROZEN MATERIALITY FLOOR
#: (r59.GATE_MATERIALITY = 1.5%/yr), so no new number is introduced:
#:   STRONG    <= 1x  the book can detect the floor itself
#:   FEASIBLE  <= 2x  the R61 cost-budget ceiling (2 x materiality) and the
#:                    lower edge of the MEASURED R61 apparatus floor (3%/yr)
#:   MARGINAL  <= 4x  the upper edge of that measured floor (6.0-6.5%/yr)
#:   WEAK      <= 10x only a premium ten times the floor is detectable
#:   beyond          nothing plausible is detectable on this sample
EVENT_CLASS_BAND_MULTIPLES = ((POWER_STRONG, 1.0), (POWER_FEASIBLE, 2.0),
                              (POWER_MARGINAL, 4.0), (POWER_WEAK, 10.0))
EVENT_CLASS_BANDS = tuple((name, m * float(r59.GATE_MATERIALITY))
                          for name, m in EVENT_CLASS_BAND_MULTIPLES)
EVENT_CLASS_BAND_REFERENCES = {
    "1x": "r59.GATE_MATERIALITY: the annualised net excess a candidate must "
          "deliver to be material at all",
    "2x": "alpha_agent.r61.cost_budget.COST_BUDGET_CEILING (2 x materiality) "
          "and the lower edge of the R61-measured detection floor (3%/yr)",
    "4x": "upper edge of the R61-measured detection floor (MDE_80 3-6.5%/yr "
          "net, Release 61 apparatus calibration)",
    "10x": "ten times the floor; no lockbox in this estate has reported such "
           "a premium, so a sample that detects nothing smaller detects "
           "nothing plausible",
}
EVENT_SERIAL_DEPENDENCE_RULE = "N_eff = N x (1 - rho1) / (1 + rho1)"
EVENT_VOL_BASIS_REALISED = "REALISED_PER_OBSERVATION_NET_EXCESS_VOL"
EVENT_VOL_BASIS_EXANTE = "FROZEN_EXANTE_UNCONDITIONAL_WINDOW_VOL"
#: The forward door asks for "the feasible edge (0.05)" and "the strong edge
#: (0.03)" in rank-IC labels; in event-return units those labels are the
#: event class edges of the same name.
IC_EDGE_TO_EVENT_EDGE = {0.05: dict(EVENT_CLASS_BANDS)[POWER_FEASIBLE],
                         0.03: dict(EVENT_CLASS_BANDS)[POWER_STRONG]}

PATH_HISTORICAL = "HISTORICAL_QUALIFICATION"
PATH_HISTORICAL_THEN_INCUBATE = "HISTORICAL_MEASUREMENT_THEN_INCUBATION"
PATH_FORWARD_OBSERVATION = "FORWARD_OBSERVATION_ONLY"
PATH_NOT_ON_THIS_EXPRESSION = "NOT_RESEARCHABLE_ON_THIS_EXPRESSION"
RESEARCH_PATHS = (PATH_HISTORICAL, PATH_HISTORICAL_THEN_INCUBATE,
                  PATH_FORWARD_OBSERVATION, PATH_NOT_ON_THIS_EXPRESSION)

PATH_FOR_CLASS = {
    POWER_STRONG: PATH_HISTORICAL,
    POWER_FEASIBLE: PATH_HISTORICAL,
    POWER_MARGINAL: PATH_HISTORICAL_THEN_INCUBATE,
    POWER_WEAK: PATH_FORWARD_OBSERVATION,
    POWER_UNIDENTIFIABLE: PATH_NOT_ON_THIS_EXPRESSION,
}

#: Classes from which research-only forward observation is admissible when the
#: candidate is otherwise clean (pipeline enforces the rest of the checklist).
INCUBATION_ADMISSIBLE_CLASSES = (POWER_MARGINAL, POWER_WEAK)

#: G7 closure vocabulary. PERMANENT is deliberately absent: G7 can close an
#: EXPRESSION, never a mechanism.
CLOSURE_NONE = "NONE"
CLOSURE_EXPRESSION_ONLY = "CLOSED_FOR_THIS_EXPRESSION_ONLY"
CLOSURE_KINDS = (CLOSURE_NONE, CLOSURE_EXPRESSION_ONLY)

PRIOR_UNKNOWN = "PRIOR_UNKNOWN"
AGENT_PRIOR_USE = "PRIORITISATION_ONLY"

# --------------------------------------------------------------------------- #
# The calibration of the ANALYTIC fallback, pinned to MEASURED points
# --------------------------------------------------------------------------- #
#: The nine R88 Monte-Carlo points (alpha_agent.r61.power, 20 seeds, frozen
#: rule, cost net). ``n_eff`` is the effective instrument count computed from
#: the panel's own 21-session return correlation by the R89 breadth audit;
#: it is filled in by that audit and re-checked by its test. The analytic
#: form is  t = k * rho * sqrt((N_eff - 1) * T_eff_lockbox), solved for rho
#: at the frozen one-sided threshold. ``K_CALIBRATION`` is the median k over
#: the measured points; the audit reports every residual.
MEASURED_REFERENCE_POINTS = (
    {"panel": "ALL68_M", "n": 68, "decisions": 187, "lockbox_decisions": 44,
     "cadence": 21, "horizon": 21, "mde_80": 0.0692},
    {"panel": "ALL68_W", "n": 68, "decisions": 787, "lockbox_decisions": 188,
     "cadence": 5, "horizon": 5, "mde_80": 0.0476},
    {"panel": "COMMODITY39_M", "n": 39, "decisions": 187,
     "lockbox_decisions": 44, "cadence": 21, "horizon": 21, "mde_80": 0.0818},
    {"panel": "COMMODITY39_W", "n": 39, "decisions": 787,
     "lockbox_decisions": 188, "cadence": 5, "horizon": 5, "mde_80": 0.0667},
    {"panel": "XA28_M", "n": 28, "decisions": 187, "lockbox_decisions": 44,
     "cadence": 21, "horizon": 21, "mde_80": 0.0938},
    {"panel": "C02_PROXY23_M", "n": 23, "decisions": 187,
     "lockbox_decisions": 44, "cadence": 21, "horizon": 21, "mde_80": 0.110},
    {"panel": "C03_21_W", "n": 21, "decisions": 787, "lockbox_decisions": 188,
     "cadence": 5, "horizon": 5, "mde_80": 0.090},
    {"panel": "C01_17_M", "n": 17, "decisions": 187, "lockbox_decisions": 44,
     "cadence": 21, "horizon": 21, "mde_80": 0.1438},
    {"panel": "EQIDX14_M", "n": 14, "decisions": 187, "lockbox_decisions": 44,
     "cadence": 21, "horizon": 21, "mde_80": 0.1357},
    {"panel": "FX8_M", "n": 8, "decisions": 187, "lockbox_decisions": 44,
     "cadence": 21, "horizon": 21, "mde_80": 0.2556},
    {"panel": "RATES6_M", "n": 6, "decisions": 187, "lockbox_decisions": 44,
     "cadence": 21, "horizon": 21, "mde_80": 0.2875},
)

#: Default k when no audit-fitted calibration is supplied. Derived from the
#: ALL68 monthly point with the panel's measured mean pairwise correlation
#: (see R89_EFFECTIVE_BREADTH_AUDIT.json, which re-fits and overrides it).
DEFAULT_K = 0.90

#: The frozen final gate reads (never written here).
BH_Q = float(r59.BH_Q)
OBS_FLOOR = int(r59.OBS_FLOOR)
POWER_TARGET = 0.80


def _z(p: float) -> float:
    """Inverse standard normal CDF (Acklam), stdlib only."""
    if p <= 0.0 or p >= 1.0:
        raise ValueError("p must be in (0,1)")
    a = (-3.969683028665376e+01, 2.209460984245205e+02,
         -2.759285104469687e+02, 1.383577518672690e+02,
         -3.066479806614716e+01, 2.506628277459239e+00)
    b = (-5.447609879822406e+01, 1.615858368580409e+02,
         -1.556989798598866e+02, 6.680131188771972e+01,
         -1.328068155288572e+01)
    c = (-7.784894002430293e-03, -3.223964580411365e-01,
         -2.400758277161838e+00, -2.549732539343734e+00,
         4.374664141464968e+00, 2.938163982698783e+00)
    d = (7.784695709041462e-03, 3.224671290700398e-01,
         2.445134137142996e+00, 3.754408661907416e+00)
    plow, phigh = 0.02425, 1 - 0.02425
    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / \
               ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
    if p > phigh:
        q = math.sqrt(-2 * math.log(1 - p))
        return -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / \
            ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
    q = p - 0.5
    r = q * q
    return (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r+a[5])*q / \
           (((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1)


# --------------------------------------------------------------------------- #
# Effective sample: what the structure's geometry actually earns
# --------------------------------------------------------------------------- #
def effective_instruments(n: int, mean_pairwise_correlation: float) -> float:
    """Independent-equivalent instrument count under equicorrelation.

    ``n / (1 + (n-1) * rho)``: the classical effective-N of n equally
    correlated series. Six Treasury tenors at rho 0.85 are ~1.2 instruments,
    which is why RATES6 measured an MDE_80 of 0.29 and not 0.10.
    """
    n = int(n)
    if n <= 0:
        return 0.0
    rho = min(max(float(mean_pairwise_correlation), 0.0), 1.0)
    return n / (1.0 + (n - 1) * rho)


def overlap_factor(cadence: int, horizon: int) -> float:
    """How many consecutive decisions share a forward window."""
    return max(1.0, float(horizon) / float(max(1, int(cadence))))


def effective_sample(structure: str, *, n_instruments: int = 0,
                     n_decisions: int = 0, lockbox_decisions: int = 0,
                     cadence: int = r59.CADENCE, horizon: int = r59.HORIZON,
                     mean_pairwise_correlation: float = 0.0,
                     n_events: int = 0, n_event_clusters: int = 0,
                     n_pairs: int = 0, pair_correlation: float = 0.0,
                     component_count: int = 0,
                     n_observations: int = 0, n_lockbox_observations: int = 0,
                     n_lockbox_clusters: int = 0,
                     event_return_vol: Optional[float] = None,
                     event_return_vol_basis: str = "",
                     events_per_year: Optional[float] = None,
                     serial_correlation: float = 0.0,
                     n_tested_windows: int = 1) -> dict:
    """Raw counts -> independent decision counts, per structure.

    Nothing here is a verdict. Every count is reported next to the raw count
    it was derived from, so a reader can see exactly what dependence removed.

    R92, EVENT_DRIVEN only: ``n_lockbox_observations`` / ``n_observations``
    are the MERGED independent observations the event book reports (or a
    pre-measurement resolution computes without reading a return);
    ``event_return_vol`` is the per-observation net excess volatility
    (realised, or the frozen ex-ante unconditional window volatility);
    ``events_per_year`` is the lockbox's independent observations per year;
    ``serial_correlation`` is the lag-1 autocorrelation of observation
    returns; ``n_tested_windows`` the holding windows tested on the object.
    When the volatility is supplied the sample carries EVENT-RETURN units and
    :func:`assess` classifies it in those units; without it the R89 rank-IC
    fallback is kept and LABELLED, which is the conservative behaviour.
    """
    if structure not in STRUCTURES:
        raise ValueError("unknown research structure %r; the structures are %s"
                         % (structure, list(STRUCTURES)))
    ov = overlap_factor(cadence, horizon)
    t_eff = float(n_decisions) / ov
    l_eff = float(lockbox_decisions) / ov
    out = {"structure": structure, "cadence_sessions": int(cadence),
           "horizon_sessions": int(horizon), "overlap_factor": ov,
           "raw_decisions": int(n_decisions),
           "effective_decisions": t_eff,
           "raw_lockbox_decisions": int(lockbox_decisions),
           "effective_lockbox_decisions": l_eff,
           "mean_pairwise_correlation": float(mean_pairwise_correlation)}
    if structure in (CROSS_SECTIONAL, PANEL, ENSEMBLE):
        n_eff = effective_instruments(n_instruments, mean_pairwise_correlation)
        out.update({"raw_instruments": int(n_instruments),
                    "effective_instruments": n_eff,
                    "raw_observations": int(n_instruments) * int(n_decisions),
                    "independent_decisions": n_eff * t_eff,
                    "independent_lockbox_decisions": n_eff * l_eff,
                    "component_count": int(component_count)})
        if structure == ENSEMBLE:
            out["note"] = ("one combined frozen score is ONE rank book; the "
                           "components add to the search burden, never to N")
    elif structure == TIME_SERIES:
        # A common timing signal has NO cross-sectional ranking content: the
        # book earns one decision per date, however many legs express it.
        # Correlated legs move together, so width adds almost nothing.
        n_eff = effective_instruments(n_instruments, mean_pairwise_correlation)
        out.update({"raw_instruments": int(n_instruments),
                    "effective_instruments": n_eff,
                    "raw_observations": int(n_instruments) * int(n_decisions),
                    "independent_decisions": t_eff,
                    "independent_lockbox_decisions": l_eff,
                    "note": ("timing: one independent decision per date; "
                             "cross-sectional width does not multiply N")})
    elif structure == EVENT_DRIVEN:
        # Events that share a date (a scheduled release across countries, an
        # exchange-wide action) are ONE cluster, not n events.
        clusters = int(n_event_clusters) if n_event_clusters else int(n_events)
        # R92: the lockbox sample is the MERGED independent observation count
        # when the book (or a return-free pre-measurement resolution) reports
        # it; lockbox clusters over overlap is the next best; the R89 share
        # scaling is the last resort and is labelled.
        if n_lockbox_observations:
            l_ind = float(n_lockbox_observations)
            l_basis = "MERGED_INDEPENDENT_LOCKBOX_OBSERVATIONS"
        elif n_lockbox_clusters:
            l_ind = float(n_lockbox_clusters) / ov
            l_basis = "LOCKBOX_CLUSTERS_OVER_OVERLAP"
        else:
            l_ind = (float(clusters) / ov * (float(lockbox_decisions)
                                            / float(n_decisions))
                     if n_decisions else 0.0)
            l_basis = "CLUSTERS_SCALED_BY_LOCKBOX_SHARE_R89"
        rho1 = min(max(float(serial_correlation or 0.0), 0.0), 0.99)
        serial_factor = (1.0 - rho1) / (1.0 + rho1)
        total_ind = (float(n_observations) if n_observations
                     else float(clusters) / ov) * serial_factor
        vol = (None if event_return_vol is None
               else float(event_return_vol))
        units = (MDE_UNITS_EVENT_RETURN if vol is not None and vol > 0
                 else MDE_UNITS_RANK_IC)
        out.update({"raw_events": int(n_events),
                    "event_clusters": clusters,
                    "raw_observations": int(n_events),
                    "merged_observations": int(n_observations),
                    "merged_lockbox_observations": int(n_lockbox_observations),
                    "lockbox_clusters": int(n_lockbox_clusters),
                    "independent_decisions": total_ind,
                    "independent_lockbox_decisions": l_ind * serial_factor,
                    "lockbox_sample_basis": l_basis,
                    "serial_correlation": rho1,
                    "serial_dependence_factor": serial_factor,
                    "serial_dependence_rule": EVENT_SERIAL_DEPENDENCE_RULE,
                    "event_return_vol": vol,
                    "event_return_vol_basis": str(event_return_vol_basis or ""),
                    "events_per_year": (None if events_per_year is None
                                        else float(events_per_year)),
                    "n_tested_windows": max(1, int(n_tested_windows or 1)),
                    "mde_units": units,
                    "event_power_calibration": EVENT_POWER_CALIBRATION,
                    "note": "clustered-by-date events count once; overlapping "
                            "windows are merged; serial dependence divides"})
    elif structure == RELATIVE_VALUE:
        p_eff = effective_instruments(n_pairs, pair_correlation)
        out.update({"raw_pairs": int(n_pairs), "effective_pairs": p_eff,
                    "raw_observations": int(n_pairs) * int(n_decisions),
                    "independent_decisions": p_eff * t_eff,
                    "independent_lockbox_decisions": p_eff * l_eff})
    # THE FROZEN OBS_FLOOR IS ON LOCKBOX PERIODS, NOT ON PERIODS x WIDTH.
    # ``engines.gate`` reads the book's ``effective_observations``, which the
    # frozen books report as lockbox decisions divided by overlap (44 monthly,
    # 188 weekly on the certified layer). Width never rescues a short lockbox.
    if structure == EVENT_DRIVEN:
        out["lockbox_clears_obs_floor"] = bool(
            out["independent_lockbox_decisions"] >= OBS_FLOOR)
    else:
        out["lockbox_clears_obs_floor"] = bool(l_eff >= OBS_FLOOR)
    return out


# --------------------------------------------------------------------------- #
# The analytic fallback, and what it must confess
# --------------------------------------------------------------------------- #
def t_required(*, burden_denominator: int = 1, power: float = POWER_TARGET
               ) -> float:
    """t-statistic the FROZEN gate needs, at ``power``, one-sided.

    ``burden_corrected_significant`` requires p_one_sided * denominator <=
    BH_Q, i.e. alpha = BH_Q / denominator. Nothing here is a new threshold.
    """
    alpha = BH_Q / max(1, int(burden_denominator))
    return _z(1.0 - alpha) + _z(float(power))


def analytic_mde(eff: dict, *, k: float = DEFAULT_K,
                 burden_denominator: int = 1, power: float = POWER_TARGET
                 ) -> dict:
    """MDE_80 in rank-IC units from the effective sample, ANALYTICALLY.

    Used ONLY where the Monte-Carlo owner cannot measure the structure, and
    labelled so. ``k`` is the calibration the breadth audit fits to the
    measured points; the residuals are published next to it.
    """
    n_eff = float(eff.get("effective_instruments", 1.0) or 1.0)
    l_eff = float(eff.get("effective_lockbox_decisions", 0.0) or 0.0)
    if eff["structure"] in (TIME_SERIES, EVENT_DRIVEN):
        width = 1.0
        l_eff = float(eff.get("independent_lockbox_decisions", l_eff) or 0.0)
    elif eff["structure"] == RELATIVE_VALUE:
        width = max(float(eff.get("effective_pairs", 1.0) or 1.0), 1.0)
    else:
        width = max(n_eff - 1.0, 1.0)
    if l_eff <= 0 or width <= 0:
        return {"basis": MDE_BASIS_ANALYTIC, "mde_rho": None, "reached": False,
                "reason": "no lockbox decisions", "k": float(k)}
    t_req = t_required(burden_denominator=burden_denominator, power=power)
    rho = t_req / (float(k) * math.sqrt(width * l_eff))
    return {"basis": MDE_BASIS_ANALYTIC, "mde_rho": float(rho),
            "reached": rho <= 1.0, "k": float(k), "t_required": t_req,
            "width_term": width, "effective_lockbox_decisions": l_eff,
            "power": float(power),
            "burden_denominator": int(burden_denominator)}


def event_analytic_mde(eff: dict, *, burden_denominator: int = 1,
                       power: float = POWER_TARGET) -> dict:
    """MDE_80 of an EVENT_DRIVEN sample in EVENT-RETURN units (R92).

    ``MDE_per_event = t_required x sigma_event / sqrt(N_eff_lockbox)`` and
    ``MDE_annualised = MDE_per_event x observations_per_year``. The threshold
    is the frozen gate's (BH_Q over the burden, further divided by the number
    of windows tested on the object); the power target is unchanged. Returns
    a labelled, non-reaching record when the volatility or the frequency is
    missing - never a pass.
    """
    if eff.get("structure") != EVENT_DRIVEN:
        raise ValueError("event_analytic_mde is for EVENT_DRIVEN samples only")
    n = float(eff.get("independent_lockbox_decisions") or 0.0)
    vol = eff.get("event_return_vol")
    epy = eff.get("events_per_year")
    windows = max(1, int(eff.get("n_tested_windows") or 1))
    base = {"basis": MDE_BASIS_ANALYTIC, "units": MDE_UNITS_EVENT_RETURN,
            "calibration": EVENT_POWER_CALIBRATION, "mde_rho": None,
            "mde_per_event": None, "mde_annualised": None,
            "power": float(power), "burden_denominator": int(burden_denominator),
            "n_tested_windows": windows,
            "effective_burden_denominator": int(burden_denominator) * windows}
    if vol is None or float(vol) <= 0.0:
        return {**base, "reached": False,
                "reason": "EVENT_RETURN_VOL_NOT_SUPPLIED: an event sample "
                          "needs a realised or frozen ex-ante per-observation "
                          "volatility before an economic MDE exists"}
    if n <= 0.0:
        return {**base, "reached": False,
                "reason": "no independent lockbox observations"}
    if epy is None or float(epy) <= 0.0:
        return {**base, "reached": False,
                "reason": "EVENTS_PER_YEAR_NOT_SUPPLIED: the annualised unit "
                          "needs the lockbox's observations per year"}
    t_req = t_required(burden_denominator=int(burden_denominator) * windows,
                       power=power)
    per_event = t_req * float(vol) / math.sqrt(n)
    ann = per_event * float(epy)
    return {**base, "reached": True, "t_required": t_req,
            "mde_per_event": float(per_event), "mde_annualised": float(ann),
            "independent_lockbox_observations": n,
            "event_return_vol": float(vol),
            "event_return_vol_basis": str(eff.get("event_return_vol_basis") or ""),
            "events_per_year": float(epy),
            "form": "MDE_per_event = t_req * sigma_event / sqrt(N_eff); "
                    "MDE_annualised = MDE_per_event * observations_per_year; "
                    "t_req at BH_Q / (burden x windows), one-sided, 80% power"}


def classify_event(mde_annualised: Optional[float], *, reached: bool,
                   lockbox_clears_obs_floor: bool) -> dict:
    """ONE power class from an annualised event-return MDE. Pure; declared
    bands (multiples of the frozen materiality floor); no prior used."""
    if not lockbox_clears_obs_floor:
        return {"power_class": POWER_UNIDENTIFIABLE,
                "reason": ("merged independent lockbox observations below the "
                           "frozen OBS_FLOOR %d: engines.gate cannot pass at "
                           "ANY effect size in any units" % OBS_FLOOR)}
    if mde_annualised is None or not reached:
        return {"power_class": POWER_UNIDENTIFIABLE,
                "reason": "no economic MDE: event-return units unavailable"}
    for (name, edge), (_n, mult) in zip(EVENT_CLASS_BANDS,
                                        EVENT_CLASS_BAND_MULTIPLES):
        if float(mde_annualised) <= edge:
            return {"power_class": name,
                    "reason": "MDE_80 %.4f/yr <= %.4f/yr (%gx materiality: %s)"
                              % (float(mde_annualised), edge, mult,
                                 EVENT_CLASS_BAND_REFERENCES["%gx" % mult])}
    return {"power_class": POWER_UNIDENTIFIABLE,
            "reason": "MDE_80 %.4f/yr > %.4f/yr (10x the materiality floor): "
                      "only an implausible premium is detectable"
                      % (float(mde_annualised), EVENT_CLASS_BANDS[-1][1])}


def fit_k(points=MEASURED_REFERENCE_POINTS, *, n_eff_by_panel: dict) -> dict:
    """Fit the single calibration constant to the MEASURED points.

    ``n_eff_by_panel`` maps panel -> effective instruments measured from the
    panel's own return correlation. Returns the median k and EVERY residual,
    so the quality of the analytic fallback is a published number.
    """
    ks, rows = [], []
    for p in points:
        n_eff = n_eff_by_panel.get(p["panel"])
        if n_eff is None:
            continue
        ov = overlap_factor(p["cadence"], p["horizon"])
        l_eff = p["lockbox_decisions"] / ov
        width = max(n_eff - 1.0, 1.0)
        k = t_required() / (p["mde_80"] * math.sqrt(width * l_eff))
        ks.append(k)
        rows.append({"panel": p["panel"], "n": p["n"], "n_eff": n_eff,
                     "lockbox_eff": l_eff, "mde_80_measured": p["mde_80"],
                     "k_implied": k})
    if not ks:
        return {"k": DEFAULT_K, "fitted": False, "rows": rows}
    ks_sorted = sorted(ks)
    k_med = ks_sorted[len(ks_sorted) // 2] if len(ks_sorted) % 2 else \
        0.5 * (ks_sorted[len(ks_sorted) // 2 - 1] + ks_sorted[len(ks_sorted) // 2])
    for r in rows:
        eff = {"structure": CROSS_SECTIONAL, "effective_instruments": r["n_eff"],
               "effective_lockbox_decisions": r["lockbox_eff"]}
        r["mde_80_analytic"] = analytic_mde(eff, k=k_med)["mde_rho"]
        r["residual"] = r["mde_80_analytic"] - r["mde_80_measured"]
        r["relative_error"] = r["residual"] / r["mde_80_measured"]
    rel = [abs(r["relative_error"]) for r in rows]
    return {"k": float(k_med), "fitted": True, "n_points": len(rows),
            "median_abs_relative_error": sorted(rel)[len(rel) // 2],
            "max_abs_relative_error": max(rel), "rows": rows,
            "form": "t = k * rho * sqrt((N_eff - 1) * T_eff_lockbox); "
                    "rho solved at the frozen one-sided threshold "
                    "(BH_Q / burden) and 80% power"}


# --------------------------------------------------------------------------- #
# Classification
# --------------------------------------------------------------------------- #
def classify(mde_rho: Optional[float], *, reached: bool,
             lockbox_clears_obs_floor: bool) -> dict:
    """ONE power class from an MDE_80. Pure; declared bands; no prior used."""
    if not lockbox_clears_obs_floor:
        return {"power_class": POWER_UNIDENTIFIABLE,
                "reason": ("effective lockbox decisions below the frozen "
                           "OBS_FLOOR %d: engines.gate cannot pass at ANY "
                           "effect size" % OBS_FLOOR)}
    if mde_rho is None or not reached:
        return {"power_class": POWER_UNIDENTIFIABLE,
                "reason": "80% detection never reached on the frozen grid"}
    for name, edge in CLASS_BANDS:
        if float(mde_rho) <= edge:
            return {"power_class": name,
                    "reason": "MDE_80 rho %.4f <= %.2f (%s)" % (
                        float(mde_rho), edge,
                        CLASS_BAND_REFERENCES["%.2f" % edge])}
    return {"power_class": POWER_UNIDENTIFIABLE,
            "reason": "MDE_80 rho %.4f > 0.15, the frozen R88 cap: only an "
                      "implausible effect is detectable" % float(mde_rho)}


def minimum_forward_evidence(eff: dict, *, target_ic: float,
                             k: float = DEFAULT_K, power: float = POWER_TARGET,
                             burden_denominator: int = 1,
                             target_annual_return: Optional[float] = None
                             ) -> dict:
    """How much FORWARD evidence a research-only candidate must accrue.

    Determined here, by the power owner, from effect type, cadence,
    correlation, effective N and the pre-declared target power - never an
    arbitrary observation count. The frozen OBS_FLOOR is still a floor.

    R92: an EVENT_DRIVEN sample in event-return units is answered in those
    units. ``target_annual_return`` names the premium to detect; when it is
    absent the rank-IC edge label the caller passed (0.05 feasible, 0.03
    strong) is mapped to the event class edge of the same name.
    """
    n_eff = float(eff.get("effective_instruments", 1.0) or 1.0)
    struct = eff["structure"]
    if struct == EVENT_DRIVEN and eff.get("mde_units") == MDE_UNITS_EVENT_RETURN:
        vol = float(eff.get("event_return_vol") or 0.0)
        epy = float(eff.get("events_per_year") or 0.0)
        windows = max(1, int(eff.get("n_tested_windows") or 1))
        if target_annual_return is None:
            target_annual_return = IC_EDGE_TO_EVENT_EDGE.get(
                round(float(target_ic), 4))
        if target_annual_return is None or float(target_annual_return) <= 0:
            raise ValueError("an event sample needs target_annual_return (or "
                             "one of the edge labels %s)"
                             % sorted(IC_EDGE_TO_EVENT_EDGE))
        if vol <= 0 or epy <= 0:
            raise ValueError("event sample lacks event_return_vol or "
                             "events_per_year")
        t_req = t_required(burden_denominator=int(burden_denominator) * windows,
                           power=power)
        tgt = float(target_annual_return)
        n_needed = (t_req * vol * epy / tgt) ** 2
        n_needed = max(n_needed, float(OBS_FLOOR))
        sessions = n_needed / epy * 252.0
        return {"target_ic": float(target_ic),
                "target_annual_return": tgt,
                "units": MDE_UNITS_EVENT_RETURN,
                "target_power": float(power), "t_required": t_req,
                "burden_denominator": int(burden_denominator),
                "n_tested_windows": windows,
                "effective_decisions_required": n_needed,
                "raw_decisions_required": int(math.ceil(n_needed)),
                "obs_floor_applied": n_needed == float(OBS_FLOOR),
                "approx_sessions_required": int(math.ceil(sessions)),
                "approx_years_required": round(sessions / 252.0, 2),
                "events_per_year": epy, "event_return_vol": vol,
                "owner": MECHANISM_POWER_OWNER}
    if struct in (TIME_SERIES, EVENT_DRIVEN):
        width = 1.0
    elif struct == RELATIVE_VALUE:
        width = max(float(eff.get("effective_pairs", 1.0) or 1.0), 1.0)
    else:
        width = max(n_eff - 1.0, 1.0)
    t_req = t_required(burden_denominator=burden_denominator, power=power)
    ic = float(target_ic)
    if ic <= 0:
        raise ValueError("target_ic must be positive")
    t_eff_needed = (t_req / (float(k) * ic)) ** 2 / width
    t_eff_needed = max(t_eff_needed, float(OBS_FLOOR))
    ov = float(eff.get("overlap_factor", 1.0) or 1.0)
    raw_needed = math.ceil(t_eff_needed * ov)
    cadence = int(eff.get("cadence_sessions", r59.CADENCE))
    sessions = raw_needed * cadence
    return {"target_ic": ic, "target_power": float(power),
            "t_required": t_req, "burden_denominator": int(burden_denominator),
            "effective_decisions_required": t_eff_needed,
            "raw_decisions_required": int(raw_needed),
            "obs_floor_applied": t_eff_needed == float(OBS_FLOOR),
            "approx_sessions_required": int(sessions),
            "approx_years_required": round(sessions / 252.0, 2),
            "owner": MECHANISM_POWER_OWNER}


# --------------------------------------------------------------------------- #
# The assessment record and the deterministic G7 ruling
# --------------------------------------------------------------------------- #
def assess(*, structure: str, sample: dict, measured_mde_80: Optional[float]
           = None, measured_reached: Optional[bool] = None,
           measured_artifact: str = "", k: float = DEFAULT_K,
           burden_denominator: int = 1, agent_prior_ic: Optional[float] = None,
           agent_prior_source: str = "", expression_id: str = "") -> dict:
    """ONE power assessment. Measured MDE wins; analytic is a labelled fallback.

    ``sample`` is :func:`effective_sample`'s output. ``agent_prior_ic`` is
    METADATA: it is recorded, it may order a queue, and it never decides.
    """
    if structure not in STRUCTURES:
        raise ValueError("unknown research structure %r" % (structure,))
    if sample.get("structure") != structure:
        raise ValueError("sample was computed for %r, not %r"
                         % (sample.get("structure"), structure))
    if measured_mde_80 is not None:
        if structure not in MC_MEASURABLE:
            raise ValueError(
                "%s cannot carry a MEASURED MDE: the Monte-Carlo harness does "
                "not represent that structure (%s)"
                % (structure, list(MC_MEASURABLE)))
        if not str(measured_artifact or "").strip():
            raise ValueError("a measured MDE must name its r61.power artifact")
        mde = {"basis": MDE_BASIS_MEASURED, "owner": MEASURED_MDE_OWNER,
               "mde_rho": float(measured_mde_80),
               "reached": bool(True if measured_reached is None
                               else measured_reached),
               "artifact": str(measured_artifact)}
    elif structure == EVENT_DRIVEN and \
            sample.get("mde_units") == MDE_UNITS_EVENT_RETURN:
        # R92: event-return units. The sample carries its volatility and its
        # observation frequency; the class bands are multiples of the frozen
        # materiality floor. OBS_FLOOR is applied to MERGED observations.
        mde = event_analytic_mde(sample, burden_denominator=burden_denominator)
        mde["owner"] = MECHANISM_POWER_OWNER
    else:
        mde = analytic_mde(sample, k=k, burden_denominator=burden_denominator)
        mde["owner"] = MECHANISM_POWER_OWNER
        if structure == EVENT_DRIVEN:
            mde["units"] = MDE_UNITS_RANK_IC
            mde["units_warning"] = (
                "RANK_IC_FALLBACK: no event_return_vol was supplied, so this "
                "event sample is classified in rank-IC units, which have no "
                "economic meaning for a 1-6 leg event book (R91 defect); the "
                "classification is conservative, not calibrated")
    if mde.get("units") == MDE_UNITS_EVENT_RETURN:
        cls = classify_event(mde.get("mde_annualised"),
                             reached=bool(mde.get("reached")),
                             lockbox_clears_obs_floor=bool(
                                 sample.get("lockbox_clears_obs_floor")))
        bands = [list(b) for b in EVENT_CLASS_BANDS]
    else:
        cls = classify(mde.get("mde_rho"), reached=bool(mde.get("reached")),
                       lockbox_clears_obs_floor=bool(
                           sample.get("lockbox_clears_obs_floor")))
        bands = [list(b) for b in CLASS_BANDS]
    body = {
        "record": "MECHANISM_POWER_ASSESSMENT",
        "owner": MECHANISM_POWER_OWNER,
        "version": MECHANISM_POWER_VERSION,
        "expression_id": str(expression_id or ""),
        "structure": structure,
        "mc_measurable": structure in MC_MEASURABLE,
        "sample": sample,
        "mde": mde,
        "mde_units": mde.get("units", MDE_UNITS_RANK_IC),
        "power_class": cls["power_class"],
        "power_class_reason": cls["reason"],
        "research_path": PATH_FOR_CLASS[cls["power_class"]],
        "incubation_admissible_on_power": cls["power_class"]
        in INCUBATION_ADMISSIBLE_CLASSES,
        "class_bands": bands,
        "final_gate_read_only": {"bh_q": BH_Q, "obs_floor": OBS_FLOOR,
                                 "materiality_floors": dict(
                                     r59.GATE_MATERIALITY_FLOORS),
                                 "changed_by_this_module": False},
        "agent_prior": {"ic": (None if agent_prior_ic is None
                               else float(agent_prior_ic)),
                        "source": str(agent_prior_source or PRIOR_UNKNOWN),
                        "use": AGENT_PRIOR_USE,
                        "is_closure_basis": False},
        "timestamp": now_iso(),
    }
    body["record_hash"] = stable_hash(
        {k_: v for k_, v in body.items() if k_ not in ("timestamp",
                                                       "record_hash")})
    return body


def verify(assessment: dict) -> bool:
    """Is this record one this module produced (hash binds every field)?"""
    if not isinstance(assessment, dict):
        return False
    if assessment.get("owner") != MECHANISM_POWER_OWNER:
        return False
    if assessment.get("record") != "MECHANISM_POWER_ASSESSMENT":
        return False
    want = stable_hash({k: v for k, v in assessment.items()
                        if k not in ("timestamp", "record_hash")})
    return want == assessment.get("record_hash")


def g7_ruling(assessment: dict) -> dict:
    """The deterministic G7 decision. Never permanent; never on a prior.

    The ONLY inputs are the measured/analytic MDE and the effective sample.
    The recorded agent prior is echoed as a priority hint and nothing else.
    An UNIDENTIFIABLE expression is closed FOR THAT EXPRESSION with a reopen
    condition; the mechanism stays open to any expression that changes width,
    decision count or cadence.
    """
    if not verify(assessment):
        raise ValueError("g7_ruling needs a verified MECHANISM_POWER_ASSESSMENT")
    cls = assessment["power_class"]
    path = assessment["research_path"]
    prior = assessment.get("agent_prior") or {}
    if cls == POWER_UNIDENTIFIABLE:
        closure = CLOSURE_EXPRESSION_ONLY
        reopen = ("WIDTH_OR_DECISIONS_OR_CADENCE: a certified expression "
                  "whose effective sample lifts MDE_80 into POWER_WEAK or "
                  "better; the mechanism itself is NOT closed")
    else:
        closure = CLOSURE_NONE
        reopen = ""
    mde = assessment.get("mde") or {}
    prio = None
    # The hint divides the prior by the MDE in the SAME units the record
    # carries: rank IC for rank books, annualised return for event books.
    mde_value = mde.get("mde_rho") or mde.get("mde_annualised")
    if prior.get("ic") is not None and mde_value:
        prio = float(prior["ic"]) / float(mde_value)
    return {
        "gate": "G7_STATISTICAL_POWER",
        "owner": MECHANISM_POWER_OWNER,
        "power_class": cls,
        "research_path": path,
        "closure": closure,
        "closure_is_permanent": False,
        "closure_basis": "EFFECTIVE_SAMPLE_AND_MDE_ONLY",
        "agent_prior_used_for_closure": False,
        "priority_hint": ({"agent_prior_ic": prior.get("ic"),
                           "prior_over_mde_80": prio,
                           "use": AGENT_PRIOR_USE} if prio is not None
                          else {"agent_prior": PRIOR_UNKNOWN,
                                "use": AGENT_PRIOR_USE}),
        "reopen_condition": reopen,
        "assessment_hash": assessment["record_hash"],
    }
