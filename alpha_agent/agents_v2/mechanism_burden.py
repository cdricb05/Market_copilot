r"""alpha_agent.agents_v2.mechanism_burden - the HIERARCHICAL research burden (R91).

RESEARCH INFRASTRUCTURE ONLY. NO ORDERS, NO FILLS, NO PROMOTION, NO ADOPTION.
Registers no hypothesis, measures nothing, spends no lockbox look.

THE DEFECT THIS EXISTS TO REMOVE (R90)
--------------------------------------
The campaign-local measurement cap (``campaign_spec.BUDGET.
max_measured_per_economic_family = 2``) was keyed on ``economic_family`` and
applied by the director by hand; it was enforced nowhere in code. Two GDELT
MEDIA cells routed to FUNDAMENTAL_MOMENTUM consumed the whole cap and blocked
USDM drought, export sales, crop condition, ENSO, hurricanes and mining
earthquakes - cells that share nothing with a news-tone object except the
label that routes them to the same signal agent. The director recorded it as
OPEN_HUMAN_DECISIONS[0] in R90_DIRECTOR_FINAL_RULING.json.

The GLOBAL burden was never the problem and is NOT touched here:
``ResearchMemory.burden`` counts every settled hypothesis by its four-part
``family_key`` and the gate (``alpha_agent.r59.engines.gate``) divides the
lockbox p by that total through ``alpha_agent.r59.handlers.search_denominator``.
Every experiment pre-registered under this module still lands in that
denominator; nothing here resets, forgets or nets any prior test.

THE MODEL (three identities, three jobs)
----------------------------------------
    economic_family    routing to a signal agent (agent_contracts.family_routing;
                       unchanged)
    mechanism_family   the CAMPAIGN-LOCAL measurement-cap unit (new). Two
                       genuinely different physical mechanisms under one routing
                       family may be measured independently.
    burden_unit        the RE-EXPRESSION identity (new):
                           asset_class | mechanism_family | economic_object_id
                       where ``economic_object_id`` hashes the DECLARED economic
                       object {source, variable, mapping} and NOT the free-text
                       ``information_family`` label, NOT the sign, horizon,
                       threshold, z-window, universe or theme subset, ranking,
                       residualisation or cadence. A sign flip, a threshold
                       change, a horizon change or a cosmetic rename of a settled
                       object is therefore the SAME burden unit and is refused at
                       pre-registration (``SAME_BURDEN_UNIT_ALREADY_SETTLED``).
    family_key         the GLOBAL burden / FDR denominator (r59.memory; unchanged)

Legacy rows (every hypothesis registered before R91) carry no mechanism
fields. They are matched by their ``information_family`` name, so a candidate
that re-expresses R89 P1 or R90 Lane A under the settled name - or declares the
settled name as its economic variable - is caught; and the director's
mechanism-scoped rulings (``ResearchMemory.director_ruling``) continue to bind
on the four-part key exactly as before.
"""
from __future__ import annotations

import json
import re
from typing import Any, Optional

from .. import r59

MECHANISM_BURDEN_OWNER = "alpha_agent.agents_v2.mechanism_burden"
MECHANISM_BURDEN_VERSION = "R91_HIERARCHICAL_BURDEN_V1"

#: The canonical mechanism-family vocabulary. A name outside it is refused; a
#: campaign may not invent a family to dodge a cap.
MECHANISM_FAMILIES = (
    "PHYSICAL_SUPPLY_DISRUPTION",   # outages, strikes, conflict, disasters hitting supply
    "WEATHER_PRODUCTION",           # drought, degree days, ENSO, hurricanes on production
    "PRODUCTION_COST_VALUE",        # price relative to a physical cost of production
    "PHYSICAL_INVENTORY_FLOW",      # stocks, storage, export sales, shipments
    "SCHEDULED_ANNOUNCEMENT_RISK",  # premia around pre-scheduled information releases
    "POLICY_EVENT",                 # the CONTENT of a policy decision (rates, quotas)
    "COUNTRY_MACRO_INFORMATION",    # macro vintages, terms of trade, country state
    "NEWS_INFORMATION_DIFFUSION",   # media tone, co-occurrence, attention (GDELT)
    "POSITIONING_FLOW",             # holdings, flows, positioning reports
    "CORPORATE_DISCLOSURE",         # filings, fundamentals, insider events
    "PRICE_STATE",                  # anything derived from the leg's own price path
)

#: Spec keys that are EXPRESSION choices. Changing any of them never creates a
#: new burden unit. The list is documentation of the rule the identity enforces
#: structurally (none of these keys enters ``economic_object_id``).
EXPRESSION_PARAMETER_KEYS = frozenset({
    "expected_sign", "sign", "horizon_sessions", "horizon", "cadence_sessions",
    "cadence", "threshold", "thresholds", "z_window", "zscore_window",
    "lookback", "universe_subset", "legs", "theme_subset", "themes", "ranking",
    "ranking_method", "residualisation", "residualization", "min_markets",
    "long_short", "holding_window_sessions", "holding_windows", "entry_rule",
    "parameters", "discovery_sample", "evaluation_sample",
    "within_family_tests", "information_family",
})

ECONOMIC_OBJECT_KEYS = ("source", "variable", "mapping")

UNIT_NEW = "NEW_BURDEN_UNIT"
UNIT_OPEN = "SAME_BURDEN_UNIT_OPEN"
UNIT_SETTLED = "SAME_BURDEN_UNIT_SETTLED_DO_NOT_REPEAT"
UNIT_VERDICTS = (UNIT_NEW, UNIT_OPEN, UNIT_SETTLED)
#: Outcomes that make a burden unit DO_NOT_REPEAT.
SETTLED_NEGATIVE = (r59.HO_NO_ALPHA_EVIDENCE, r59.HO_REJECTED)

CAP_SCOPE = "CAMPAIGN_LOCAL_PER_ASSET_CLASS_AND_MECHANISM_FAMILY"
GLOBAL_BURDEN_OWNER = "alpha_agent.r59.memory.ResearchMemory.burden"


class MechanismBurdenRefusal(RuntimeError):
    """A declaration this owner cannot accept."""


# --------------------------------------------------------------------------- #
# The economic object and the burden unit
# --------------------------------------------------------------------------- #
def _norm(value: Any) -> str:
    """Case, whitespace and punctuation are not identity: 'legacy tone
    diffusion' and 'LEGACY_TONE_DIFFUSION' are one name."""
    s = str(value if value is not None else "").strip().upper()
    return re.sub(r"[^A-Z0-9]+", "_", s).strip("_")


def canonical_economic_object(obj: Any) -> dict:
    """The declared economic object, normalised. Three non-empty strings."""
    if not isinstance(obj, dict):
        raise MechanismBurdenRefusal(
            "ECONOMIC_OBJECT_NOT_A_MAPPING: expected {source, variable, mapping}")
    out = {}
    for k in ECONOMIC_OBJECT_KEYS:
        v = _norm(obj.get(k))
        if not v:
            raise MechanismBurdenRefusal(
                "ECONOMIC_OBJECT_INCOMPLETE: missing %s (need %s)"
                % (k, ", ".join(ECONOMIC_OBJECT_KEYS)))
        out[k] = v
    return out


def economic_object_id(obj: Any) -> str:
    return r59.short_hash(canonical_economic_object(obj), 12)


def burden_unit(*, asset_class: str, mechanism_family: str,
                economic_object: Any) -> str:
    if mechanism_family not in MECHANISM_FAMILIES:
        raise MechanismBurdenRefusal(
            "MECHANISM_FAMILY_UNKNOWN: %r is not one of %s"
            % (mechanism_family, ", ".join(MECHANISM_FAMILIES)))
    return "%s|%s|%s" % (asset_class, mechanism_family,
                         economic_object_id(economic_object))


def legacy_unit(asset_class: Any, information_family: Any) -> str:
    """Rows registered before R91 carry no mechanism fields; their identity
    for re-expression purposes is their information-family name."""
    return "LEGACY|%s|%s" % (asset_class, _norm(information_family))


def _spec_of(row: dict) -> dict:
    spec = row.get("spec")
    if isinstance(spec, dict):
        return spec
    raw = row.get("spec_json")
    if isinstance(raw, str) and raw:
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except ValueError:
            return {}
    return {}


def unit_of_row(row: dict) -> dict:
    spec = _spec_of(row)
    mf = spec.get("mechanism_family")
    eo = spec.get("economic_object")
    if mf in MECHANISM_FAMILIES and isinstance(eo, dict):
        try:
            return {"unit": burden_unit(asset_class=row.get("asset_class"),
                                        mechanism_family=mf, economic_object=eo),
                    "legacy": False, "mechanism_family": mf}
        except MechanismBurdenRefusal:
            pass
    return {"unit": legacy_unit(row.get("asset_class"),
                                row.get("information_family")),
            "legacy": True, "mechanism_family": None}


def is_expression_variant(spec_a: dict, spec_b: dict) -> bool:
    """Do two specs differ ONLY in expression parameters? (Documentation
    helper for tests; the identity itself never reads these keys.)"""
    a = {k: v for k, v in (spec_a or {}).items() if k not in EXPRESSION_PARAMETER_KEYS}
    b = {k: v for k, v in (spec_b or {}).items() if k not in EXPRESSION_PARAMETER_KEYS}
    return a == b


# --------------------------------------------------------------------------- #
# Re-expression check
# --------------------------------------------------------------------------- #
def _hit(row: dict, unit: dict, why: str) -> dict:
    return {"hypothesis_id": row.get("hypothesis_id"),
            "outcome": row.get("outcome"),
            "economic_family": row.get("economic_family"),
            "information_family": row.get("information_family"),
            "model_family": row.get("model_family"),
            "release": row.get("release"),
            "settled_at": row.get("settled_at"),
            "legacy_row": unit["legacy"], "matched_by": why,
            "reopen_condition": row.get("reopen_condition")}


def same_unit_rows(mem, *, asset_class: str, mechanism_family: str,
                   economic_object: Any,
                   information_family: Optional[str] = None) -> list:
    """Every hypothesis in memory that is the SAME burden unit as the candidate.

    Exact match on the unit for R91 rows; for legacy rows, a match on the
    information-family name - either the candidate's own label or the
    candidate's declared economic variable equals the settled row's label.
    """
    unit = burden_unit(asset_class=asset_class, mechanism_family=mechanism_family,
                       economic_object=economic_object)
    obj = canonical_economic_object(economic_object)
    legacy_names = {n for n in (_norm(information_family), obj["variable"]) if n}
    hits = []
    for row in mem.list_hypotheses(asset_class=asset_class):
        u = unit_of_row(row)
        if u["unit"] == unit:
            hits.append(_hit(row, u, "BURDEN_UNIT"))
        elif u["legacy"] and _norm(row.get("information_family")) in legacy_names:
            hits.append(_hit(row, u, "LEGACY_INFORMATION_FAMILY_NAME"))
    return hits


def re_expression_check(mem, *, asset_class: str, mechanism_family: str,
                        economic_object: Any,
                        information_family: Optional[str] = None,
                        economic_family: Optional[str] = None,
                        model_family: Optional[str] = None) -> dict:
    """Is this candidate a re-expression of something the estate already ran?

    ``SAME_BURDEN_UNIT_SETTLED_DO_NOT_REPEAT`` when any same-unit row settled
    NO_ALPHA_EVIDENCE / REJECTED (a sign flip, threshold, horizon, subset,
    ranking, residualisation, cadence or rename of it is refused);
    ``SAME_BURDEN_UNIT_OPEN`` when same-unit rows exist but none settled
    negative (allowed, recorded - they share one family_key denominator);
    ``NEW_BURDEN_UNIT`` otherwise. The director's mechanism-scoped ruling on
    the four-part key is read and reported alongside; it binds independently.
    """
    hits = same_unit_rows(mem, asset_class=asset_class,
                          mechanism_family=mechanism_family,
                          economic_object=economic_object,
                          information_family=information_family)
    settled_negative = [h for h in hits if h["outcome"] in SETTLED_NEGATIVE]
    ruling = None
    if economic_family:
        try:
            ruling = mem.director_ruling(asset_class=asset_class,
                                         economic_family=economic_family,
                                         information_family=information_family,
                                         model_family=model_family)
        except Exception as exc:  # noqa: BLE001 - a ruling read must never block the check
            ruling = {"error": str(exc)[:200]}
    if settled_negative:
        verdict = UNIT_SETTLED
    elif hits:
        verdict = UNIT_OPEN
    else:
        verdict = UNIT_NEW
    return {
        "owner": MECHANISM_BURDEN_OWNER, "version": MECHANISM_BURDEN_VERSION,
        "burden_unit": burden_unit(asset_class=asset_class,
                                   mechanism_family=mechanism_family,
                                   economic_object=economic_object),
        "economic_object": canonical_economic_object(economic_object),
        "economic_object_id": economic_object_id(economic_object),
        "verdict": verdict, "verdict_vocabulary": list(UNIT_VERDICTS),
        "same_unit_rows": hits, "settled_negative": settled_negative,
        "n_same_unit": len(hits), "n_settled_negative": len(settled_negative),
        "director_ruling": ruling,
        "rule": ("a change of sign, horizon, threshold, z-window, universe or "
                 "theme subset, ranking, residualisation, cadence or label on a "
                 "settled economic object is the SAME burden unit; reopen only "
                 "on the recorded reopen condition"),
    }


# --------------------------------------------------------------------------- #
# The campaign-local cap, per (asset class, mechanism family)
# --------------------------------------------------------------------------- #
def campaign_local_count(mem, *, campaign_id: str, asset_class: str,
                         mechanism_family: str,
                         exclude_hypothesis_id: Optional[str] = None) -> dict:
    """What this campaign has already spent in one (asset class, mechanism
    family) cell. A SLOT is a distinct burden unit (one economic object): the
    1/2/5-session windows of one event object, pre-declared together, are one
    slot and three burden charges, never three slots."""
    pre, measured, units = [], [], set()
    for row in mem.list_hypotheses(asset_class=asset_class):
        spec = _spec_of(row)
        if (str(spec.get("campaign_id") or "") != str(campaign_id)
                or spec.get("mechanism_family") != mechanism_family):
            continue
        hid = row.get("hypothesis_id")
        if exclude_hypothesis_id and hid == exclude_hypothesis_id:
            continue
        (measured if row.get("outcome") else pre).append(hid)
        units.add(spec.get("burden_unit") or unit_of_row(row)["unit"])
    return {"campaign_id": str(campaign_id), "asset_class": asset_class,
            "mechanism_family": mechanism_family,
            "preregistered_open": pre, "measured_or_settled": measured,
            "burden_units_used": sorted(units),
            "experiments": len(pre) + len(measured),
            "used": len(units)}


def cap_check(mem, *, campaign_id: str, asset_class: str,
              mechanism_family: str, cap: int,
              exclude_hypothesis_id: Optional[str] = None,
              burden_unit_of_candidate: Optional[str] = None) -> dict:
    """May one more economic object in this (asset class, mechanism family) be
    pre-registered under this campaign's cap? A pre-registration consumes its
    slot the moment it is minted (it is charged to the burden whatever
    happens), so open pre-registrations count as used. A candidate whose
    burden unit already holds a slot (another pre-declared window of the same
    object, or a resume) needs no new slot."""
    if mechanism_family not in MECHANISM_FAMILIES:
        raise MechanismBurdenRefusal("MECHANISM_FAMILY_UNKNOWN: %r" % (mechanism_family,))
    c = campaign_local_count(mem, campaign_id=campaign_id, asset_class=asset_class,
                             mechanism_family=mechanism_family,
                             exclude_hypothesis_id=exclude_hypothesis_id)
    cap_i = int(cap)
    same_slot = bool(burden_unit_of_candidate
                     and burden_unit_of_candidate in c["burden_units_used"])
    allowed = same_slot or c["used"] < cap_i
    return {**c, "cap": cap_i, "allowed": allowed, "scope": CAP_SCOPE,
            "candidate_holds_a_slot_already": same_slot,
            "reason": ("%d of %d %s slots (distinct economic objects) used in %s%s"
                       % (c["used"], cap_i, mechanism_family, campaign_id,
                          "; candidate's unit already holds one" if same_slot else "")),
            "global_burden_owner_unchanged": GLOBAL_BURDEN_OWNER,
            "note": ("the cap is campaign-local and per mechanism family; the "
                     "global burden and FDR denominator still count every "
                     "experiment in every family")}


def global_burden(mem) -> dict:
    """The one global denominator, read from its owner, never recomputed."""
    return mem.burden()


# --------------------------------------------------------------------------- #
# The ONE way a settled unit reopens: an owner-certified defect
# --------------------------------------------------------------------------- #
REOPEN_DEFECT_KINDS = ("DATA_DEFECT", "EXECUTOR_DEFECT")
REOPEN_REQUIRED = ("hypothesis_ids", "defect", "certified_by", "evidence")


def reopen_allowed(check: dict, reopen_defect: Optional[dict]) -> dict:
    """May a SAME_BURDEN_UNIT_SETTLED candidate be pre-registered anyway?

    Only when the director names an owner-certified defect that covers EVERY
    settled-negative row of the unit (the recorded reopen condition of every
    R89/R90 ruling: "owner-certified executor or data defect only; never a
    parameter change"). The old rows stay settled and stay in the burden; the
    new spec records what it reopens. Anything else is a rescue and refused.
    """
    settled = [h["hypothesis_id"] for h in (check or {}).get("settled_negative") or []]
    if not settled:
        return {"allowed": True, "reason": "unit is not settled negative", "reopens": []}
    rd = dict(reopen_defect or {})
    missing = [k for k in REOPEN_REQUIRED if not rd.get(k)]
    if missing:
        return {"allowed": False, "reopens": [],
                "reason": "settled unit; reopen record incomplete: missing %s" % ", ".join(missing)}
    if rd.get("defect") not in REOPEN_DEFECT_KINDS:
        return {"allowed": False, "reopens": [],
                "reason": "defect must be one of %s" % (REOPEN_DEFECT_KINDS,)}
    named = {str(x) for x in (rd.get("hypothesis_ids") or [])}
    uncovered = [h for h in settled if h not in named]
    if uncovered:
        return {"allowed": False, "reopens": [],
                "reason": "the defect record does not cover settled rows %s" % uncovered}
    return {"allowed": True, "reopens": sorted(settled),
            "reason": "owner-certified %s by %s: %s" % (rd["defect"], rd["certified_by"],
                                                        str(rd["evidence"])[:200])}


# --------------------------------------------------------------------------- #
# Read-only view for the agents' CLI
# --------------------------------------------------------------------------- #
def cli_view(mem, payload: dict) -> dict:
    """``scripts/alpha_agents_v2.py mechanism-burden --input <payload.json>``.

    Payload: asset_class, mechanism_family, economic_object{source, variable,
    mapping}; optional information_family, economic_family, model_family,
    campaign_id, cap. Runs nothing, writes nothing.
    """
    body = dict(payload or {})
    out = {"kind": "MECHANISM_BURDEN_CHECK", "read_only": True,
           "experiments_run": 0, "evaluation_samples_read": 0,
           "mechanism_families": list(MECHANISM_FAMILIES)}
    try:
        out["re_expression"] = re_expression_check(
            mem, asset_class=body.get("asset_class"),
            mechanism_family=body.get("mechanism_family"),
            economic_object=body.get("economic_object"),
            information_family=body.get("information_family"),
            economic_family=body.get("economic_family"),
            model_family=body.get("model_family"))
        out["verdict"] = out["re_expression"]["verdict"]
    except MechanismBurdenRefusal as exc:
        out["verdict"] = "INVALID_DECLARATION"
        out["invalid_declaration_detail"] = str(exc)
        return out
    if body.get("campaign_id") and body.get("cap") is not None:
        out["cap"] = cap_check(mem, campaign_id=body["campaign_id"],
                               asset_class=body.get("asset_class"),
                               mechanism_family=body.get("mechanism_family"),
                               cap=int(body["cap"]))
    gb = global_burden(mem)
    out["global_burden"] = {"total": gb.get("total"),
                            "distinct_families": gb.get("distinct_families"),
                            "owner": GLOBAL_BURDEN_OWNER}
    out["is_governed_refusal"] = out["verdict"] == UNIT_SETTLED
    return out
