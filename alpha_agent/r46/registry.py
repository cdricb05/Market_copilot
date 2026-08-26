"""alpha_agent.r46.registry - THE challenger registry, and the adoption of orphans.

Two jobs.

**Register R46's own challengers**, each frozen with its specification hash,
its freeze timestamp, its feasibility verdict and its version. A challenger's
forward clock begins at its freeze and never restarts.

**Adopt the seven orphans by reference.** Five releases each froze a
prospective shadow registry - R39 (3), R40 (+2), R41 (1), R42 (1), and R43/R45
(0 each, correctly). Between them they hold ZERO forward observations. Those
registries are opened READ-ONLY, hashed before and after, and their members
appear on the one leaderboard with their real row counts and the real reason
those counts are zero. Their ledgers, their owners and their evidence stay
exactly where they are. Adoption moves nothing; it makes a fact visible that
was previously spread across five campaign roots and therefore visible nowhere.

Versioning is the rule that stops a losing challenger from being quietly
improved into a winner. ``v1`` makes a hundred predictions; research finds a
better parameter; ``v1`` is NOT touched. ``v2`` is registered, starts its own
forward clock at zero, and ``v1``'s record stays permanently on the board.
Every material change - features, parameters, universe, model family, entry,
exit, horizon, costs, hedge, threshold - forces a new version, and
:func:`classify_change` is what decides whether a change was material.
"""
from __future__ import annotations

import hashlib
from pathlib import Path

from . import CAMPAIGN_ID, artifact_body, campaign_dir, read_json, sha
from . import challengers as CH
from . import clock as CK
from . import contract as C
from . import feasibility as FE
from . import write_json

CALCULATION_OWNER = "alpha_agent.r46.registry"

REGISTRY_NAME = "r46_challenger_registry.json"

ORIGIN_R46_SEED = "R46_SEED"
ORIGIN_ADOPTED = "ADOPTED_PRIOR_RELEASE"

#: Changes that force a new version. Anything here alters the ECONOMICS.
MATERIAL_CHANGE_FIELDS = (
    "family", "asset_class", "instrument", "prediction_type", "horizons",
    "control", "benchmark", "cost_class", "universe", "parameters",
    "signal_owner", "hedge_definition",
)


def registry_path(campaign_id: str = CAMPAIGN_ID) -> Path:
    return campaign_dir(campaign_id) / REGISTRY_NAME


def load(campaign_id: str = CAMPAIGN_ID) -> dict:
    return read_json(registry_path(campaign_id), default={}) or {}


# --------------------------------------------------------------------------- #
# Versioning
# --------------------------------------------------------------------------- #
def classify_change(old_spec: dict, new_spec: dict) -> dict:
    """MATERIAL (needs a new version) or IMPLEMENTATION (does not)."""
    changed = []
    for f in MATERIAL_CHANGE_FIELDS:
        a, b = old_spec.get(f), new_spec.get(f)
        if isinstance(a, (list, tuple)) or isinstance(b, (list, tuple)):
            a = sorted(a or [])
            b = sorted(b or [])
        if a != b:
            changed.append(f)
    material = bool(changed)
    return {
        "classification": "MATERIAL" if material else "IMPLEMENTATION",
        "changed_fields": changed,
        "requires_new_version": material,
        "may_modify_existing_version": not material,
        "rule": "a material change starts a NEW version with a NEW forward "
                "clock; the prior version's record is never edited and never "
                "removed",
    }


def next_version(existing_versions) -> str:
    n = 0
    for v in existing_versions or ():
        try:
            n = max(n, int(str(v).lstrip("vV")))
        except ValueError:
            continue
    return "v%d" % (n + 1)


# --------------------------------------------------------------------------- #
# Adoption
# --------------------------------------------------------------------------- #
def _file_sha(path) -> str:
    try:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()
    except OSError:
        return "UNREADABLE"


def adopt_prior_shadows() -> dict:
    """Read the prior prospective registries. READ-ONLY, hashed.

    Deduplicated GLOBALLY, not per source. Release 40's registry re-lists
    Release 39's three shadows by reference - they are the same frozen
    objects, with the same specification hashes, still written by the R39
    owner - so counting each registry's members would report ten prospective
    shadows where the estate has seven. The first registry to declare a
    shadow owns it here; every later re-listing is recorded as a reference.
    """
    adopted, sources = [], []
    global_seen: dict = {}
    for src in C.ADOPTED_REGISTRY_SOURCES:
        p = Path(src["path"])
        before = _file_sha(p)
        body = read_json(p, default=None)
        entry = {
            "release": src["release"], "campaign_id": src["campaign_id"],
            "path": str(p), "owner": src["owner"],
            "file_sha256_before": before,
            "present": body is not None,
        }
        shadows = []
        if isinstance(body, dict):
            raw = body.get("shadows")
            if isinstance(raw, list):
                shadows = [s for s in raw if isinstance(s, dict)]
            entry["registry_frozen_at"] = body.get("frozen_at")
            entry["registry_declared_n"] = body.get("n_shadows",
                                                    body.get("n_frozen"))
            entry["why_none"] = body.get("why_none")
        seen_ids, re_listed = set(), []
        for sh in shadows:
            sid = str(sh.get("shadow_id") or sh.get("id") or "")
            if not sid or sid in seen_ids:
                continue
            seen_ids.add(sid)
            if sid in global_seen:
                re_listed.append({"challenger_id": sid,
                                  "first_declared_by": global_seen[sid]})
                continue
            global_seen[sid] = src["release"]
            record = dict(sh)
            record["source_release"] = src["release"]
            stream = FE.adopted_stream_state(record)
            adopted.append({
                "challenger_id": sid,
                "challenger_version": "adopted",
                "origin": ORIGIN_ADOPTED,
                "source_release": src["release"],
                "source_campaign_id": src["campaign_id"],
                "source_owner": src["owner"],
                "source_registry_sha256": before,
                "frozen_at": (sh.get("frozen_at") or sh.get("frozen_at_utc")
                              or body.get("frozen_at")),
                "spec_hash": sh.get("spec_hash")
                             or sh.get("specification_hash") or sha(sh),
                "asset_class": sh.get("asset_class")
                               or sh.get("information_family") or "UNDECLARED",
                "family": sh.get("information_family")
                          or sh.get("economic_expression") or "UNDECLARED",
                "decision_cadence": sh.get("decision_cadence"),
                "instrument": sh.get("symbol") or sh.get("instrument")
                              or "UNDECLARED",
                "promotion_allowed": False,
                "research_shadow_only": True,
                "forward_rows_owned_by": src["owner"],
                "r46_writes_forward_rows_for_it": False,
                "stream_state": stream,
                "state": C.DATA_BLOCKED if not stream.get("can_accrue_today")
                         else C.FORWARD_PENDING,
            })
        entry["n_shadows_listed"] = len(seen_ids)
        entry["n_shadows_adopted"] = len(seen_ids) - len(re_listed)
        entry["n_re_listed_from_earlier_release"] = len(re_listed)
        entry["re_listed"] = re_listed
        entry["file_sha256_after"] = _file_sha(p)
        entry["unchanged_by_r46"] = (entry["file_sha256_after"] == before)
        sources.append(entry)
    return {
        "schema": "r46_adoption/1",
        "calculation_owner": CALCULATION_OWNER,
        "rules": C.ADOPTION_RULES,
        "sources": sources,
        "adopted": adopted,
        "n_adopted": len(adopted),
        "n_distinct_shadows": len(global_seen),
        "n_registry_listings": sum(s.get("n_shadows_listed", 0)
                                   for s in sources),
        "deduplicated_globally": True,
        "dedup_note": ("R40 re-lists R39's three shadows by reference; they "
                       "are counted once, under R39, which still owns their "
                       "ledgers"),
        "all_sources_unchanged": all(s.get("unchanged_by_r46", True)
                                     for s in sources),
        "finding": (
            "%d distinct prospective shadows were frozen across five "
            "releases and hold zero forward observations between them"
            % len(adopted)),
    }


# --------------------------------------------------------------------------- #
# Registration
# --------------------------------------------------------------------------- #
def register(campaign_id: str = CAMPAIGN_ID, specs=None,
             frozen_at: str = None) -> dict:
    """Freeze the R46 cohort and adopt the orphans. Idempotent.

    Re-running never changes an already-registered challenger's spec hash,
    version or freeze timestamp: a registered challenger is re-read, its spec
    hash re-derived from the frozen file, and any mismatch is reported as a
    RETUNE_DETECTED blocker rather than silently accepted.
    """
    specs = list(specs if specs is not None else CH.SEED_SPECS)
    _now_dt = CK.now_utc()
    now = frozen_at or CK.iso(_now_dt)
    # Release 46.2 - the sub-second companion, recorded ONLY for a challenger being
    # frozen for the first time. An already-registered challenger keeps whatever it
    # was frozen with (usually nothing, because it predates this field), so no
    # existing registry entry changes and no existing hash moves.
    # An explicit ``frozen_at`` drives BOTH stamps, so a caller that pins the freeze
    # instant cannot end up with a precise stamp taken from the wall clock instead.
    now_precise = CK.iso_precise(
        (CK.parse_iso(frozen_at) if frozen_at else None) or _now_dt)
    prior = load(campaign_id)
    prior_by_id = {c["challenger_id"]: c
                   for c in (prior.get("challengers") or [])
                   if c.get("origin") == ORIGIN_R46_SEED}

    reference = CK.eastern_date(CK.now_utc())
    feas = FE.probe_all(specs, reference)
    feas_by_id = {r["challenger_id"]: r for r in feas["results"]}

    challengers, retunes = [], []
    for spec in specs:
        cid = spec["challenger_id"]
        h = CH.spec_hash(spec)
        was = prior_by_id.get(cid)
        if was is not None and was.get("spec_hash") != h:
            retunes.append({
                "challenger_id": cid,
                "registered_spec_hash": was.get("spec_hash"),
                "current_spec_hash": h,
                "verdict": "RETUNE_DETECTED",
                "required_action": "register a NEW version; the existing "
                                   "version's record may not be edited",
            })
        f = feas_by_id.get(cid, {"state": FE.NOT_PROBED})
        can = f.get("state") == FE.CAN_ACCRUE
        challengers.append({
            "challenger_id": cid,
            "challenger_version": spec["challenger_version"],
            "origin": ORIGIN_R46_SEED,
            "spec_hash": h,
            "model_parameters_hash": CH.parameters_hash(spec),
            "feature_set_hash": CH.feature_set_hash(spec),
            "frozen_at": (was or {}).get("frozen_at", now),
            # Only a challenger being frozen for the FIRST time gets a precise stamp.
            # An already-registered one keeps its own, which for the R46 cohort is
            # absent - and absent is the truth. Back-stamping today's microseconds
            # onto a freeze that happened on 2026-08-25 would manufacture precision
            # the record never had, which is the opposite of what this field is for.
            "frozen_at_precise": (was.get("frozen_at_precise") if was
                                  else now_precise),
            "forward_start": (was or {}).get("forward_start", now),
            "family": spec["family"],
            "asset_class": spec["asset_class"],
            "instrument": spec["instrument"],
            "prediction_type": spec["prediction_type"],
            "horizons": list(spec["horizons"]),
            "control": spec["control"],
            "benchmark": spec["benchmark"],
            "cost_class": spec["cost_class"],
            "universe": spec["universe"],
            "thesis": spec["thesis"],
            "parameters": spec["parameters"],
            "parameters_were_searched": False,
            "historical_qualification_state": C.HISTORICAL_ONLY,
            "historical_qualification_summary": (
                "none - R46 ran no historical screen to select this "
                "challenger; its parameters are canonical constants declared "
                "in the frozen contract"),
            "point_in_time_status": C.PIT_OK,
            "feasibility": f,
            "promotion_allowed": False,
            "research_shadow_only": True,
            "economic_overlap_with": list(spec.get("economic_overlap_with")
                                          or ()),
            "overlap_note": spec.get("overlap_note"),
            "state": C.FORWARD_PENDING if can else C.DATA_BLOCKED,
            "blocked_reason": None if can else f.get("reason"),
        })

    adoption = adopt_prior_shadows()
    body = artifact_body(
        "r46_challenger_registry/1", CALCULATION_OWNER,
        frozen_at=now,
        contract_hash=C.contract_hash(),
        n_r46_challengers=len(challengers),
        n_adopted=adoption["n_adopted"],
        n_total=len(challengers) + adoption["n_adopted"],
        n_active=sum(1 for c in challengers
                     if c["state"] == C.FORWARD_PENDING),
        n_blocked=(sum(1 for c in challengers
                       if c["state"] == C.DATA_BLOCKED)
                   + sum(1 for a in adoption["adopted"]
                         if a["state"] == C.DATA_BLOCKED)),
        challengers=challengers,
        adoption=adoption,
        retunes_detected=retunes,
        retune_free=not retunes,
        versioning_rules={
            "material_change_fields": list(MATERIAL_CHANGE_FIELDS),
            "material_change_requires_new_version": True,
            "prior_versions_are_never_edited": True,
            "a_new_version_starts_a_new_forward_clock": True,
        },
        asset_classes_active=sorted({c["asset_class"] for c in challengers
                                     if c["state"] == C.FORWARD_PENDING}),
        horizons_active=sorted({h for c in challengers
                                if c["state"] == C.FORWARD_PENDING
                                for h in c["horizons"]}),
        provider_state=feas["provider_state"],
        no_hero_candidate=True,
        proven_alpha_is_not_a_state=True,
    )
    body["registry_hash"] = sha({k: v for k, v in body.items()
                                 if k != "registry_hash"})
    write_json(registry_path(campaign_id), body)
    return body


def active_specs(registry: dict, specs=None) -> list:
    """The seed specs whose registry entry is not DATA_BLOCKED."""
    specs = list(specs if specs is not None else CH.SEED_SPECS)
    ok = {c["challenger_id"] for c in (registry.get("challengers") or ())
          if c.get("state") != C.DATA_BLOCKED}
    return [s for s in specs if s["challenger_id"] in ok]


def entry_for(registry: dict, challenger_id: str):
    for c in (registry.get("challengers") or ()):
        if c.get("challenger_id") == challenger_id:
            return c
    return None
