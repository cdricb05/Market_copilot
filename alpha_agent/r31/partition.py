"""alpha_agent.r31.partition - the ONE Release 31 evidence partition owner.

Four layers, chosen by chronology and frozen before any candidate result:

    DISCOVERY  ->  candidate and model development
    VALIDATION ->  model family and hyperparameter SELECTION
    LOCKBOX    ->  finalists only, touched once
    TRUE_FORWARD -> post-deployment evidence, owned elsewhere and never
                    synthesised here

The boundaries are contiguous and ordered. LOCKBOX is the LATEST block, because a
lockbox drawn from the middle of history would let a model be selected on data
that comes after it - the most common way a "held-out" result turns out to be
in-sample.

Overlapping labels are the other trap. A 60-session label struck at decision date
``k`` still resolves during decision dates ``k+1``, ``k+2``, ``k+3`` at a 21
session cadence. Without a purge those rows appear in two layers at once and the
lockbox silently contains validation information. Every adjacent boundary
therefore carries an embargo of ``ceil(horizon / step)`` decision dates, and
those dates belong to NO layer.
"""
from __future__ import annotations

import math
from pathlib import Path
from typing import Optional

from .. import r31
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r31.partition"
PARTITION_SCHEMA = "r31_evidence_partition_contract/1"
ARTIFACT_NAME = "evidence_partition_contract.json"

LAYER_DISCOVERY = "DISCOVERY"
LAYER_VALIDATION = "VALIDATION"
LAYER_LOCKBOX = "LOCKBOX"
LAYER_TRUE_FORWARD = "TRUE_FORWARD"
LAYERS = (LAYER_DISCOVERY, LAYER_VALIDATION, LAYER_LOCKBOX, LAYER_TRUE_FORWARD)

BLOCKED = "POINT_IN_TIME_EVIDENCE_BLOCKED"


def embargo_dates(horizon: int, step: int = _contract.STEP_SESSIONS) -> int:
    """Decision dates that must separate adjacent layers for one horizon."""
    return int(math.ceil(float(horizon) / float(step)))


def partition_for(n_dates: int, horizon: int) -> dict:
    """The layer index ranges for one sample geometry and one horizon.

    Returns index LISTS into the sample's own ordered cross-section axis, plus
    the embargoed indices that belong to no layer.
    """
    emb = embargo_dates(horizon)
    n = int(n_dates)
    n_disc = int(round(n * _contract.DISCOVERY_FRACTION))
    n_val = int(round(n * _contract.VALIDATION_FRACTION))

    disc = list(range(0, n_disc))
    gap1 = list(range(n_disc, min(n, n_disc + emb)))
    v0 = n_disc + emb
    val = list(range(v0, min(n, v0 + n_val)))
    gap2 = list(range(min(n, v0 + n_val), min(n, v0 + n_val + emb)))
    l0 = v0 + n_val + emb
    lock = list(range(l0, n))

    sufficient = (len(disc) >= _contract.MIN_DISCOVERY_DATES
                  and len(val) >= _contract.MIN_VALIDATION_DATES
                  and len(lock) >= _contract.MIN_LOCKBOX_DATES)
    return {
        "horizon_sessions": int(horizon),
        "embargo_dates": emb,
        "discovery": disc, "validation": val, "lockbox": lock,
        "embargoed": gap1 + gap2,
        "counts": {"discovery": len(disc), "validation": len(val),
                   "lockbox": len(lock), "embargoed": len(gap1) + len(gap2)},
        "sufficient": bool(sufficient),
        "state": "READY" if sufficient else BLOCKED,
    }


def layer_of(part: dict, index: int) -> Optional[str]:
    if index in set(part["discovery"]):
        return LAYER_DISCOVERY
    if index in set(part["validation"]):
        return LAYER_VALIDATION
    if index in set(part["lockbox"]):
        return LAYER_LOCKBOX
    return None


def build(snap, *, campaign_id: str = _contract.CAMPAIGN_ID) -> dict:
    """The frozen partition contract across every sample and horizon."""
    samples: dict = {}
    for sample in _contract.SAMPLES:
        sections = snap.sample_sections(sample)
        per_h: dict = {}
        for h in _contract.HORIZONS:
            part = partition_for(len(sections), h)
            part["dates"] = {
                "discovery": _bounds(snap, sections, part["discovery"]),
                "validation": _bounds(snap, sections, part["validation"]),
                "lockbox": _bounds(snap, sections, part["lockbox"]),
            }
            per_h[str(int(h))] = part
        samples[sample] = {
            "cross_sections": len(sections),
            "first_date": snap.dates[sections[0]] if sections else None,
            "last_date": snap.dates[sections[-1]] if sections else None,
            "section_indices": sections,
            "horizons": per_h,
            "state": ("READY" if all(p["sufficient"] for p in per_h.values())
                      else BLOCKED),
        }

    primary = samples[_contract.PRIMARY_SAMPLE]
    body = {
        "contract": PARTITION_SCHEMA,
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "layers": list(LAYERS),
        "policy": {
            "lockbox_is_latest_contiguous_block": True,
            "validation_precedes_lockbox": True,
            "discovery_precedes_validation": True,
            "purge_embargo_rule": "ceil(horizon / step) decision dates between "
                                  "adjacent layers, belonging to no layer",
            "no_random_split": True,
            "no_model_or_hyperparameter_may_read_lockbox": True,
            "lockbox_result_may_not_redesign_the_same_candidate": True,
            "true_forward_is_owned_elsewhere_and_never_synthesised": True,
        },
        "snapshot_hash": snap.content_hash,
        "samples": samples,
        "primary_sample": _contract.PRIMARY_SAMPLE,
        "state": primary["state"],
    }
    body["partition_hash"] = r31.sha(body)
    body.update(r31.safety_block())
    return body


def _bounds(snap, sections: list, idx: list) -> dict:
    if not idx:
        return {"first": None, "last": None, "n": 0}
    return {"first": snap.dates[sections[idx[0]]],
            "last": snap.dates[sections[idx[-1]]], "n": len(idx)}


def path_for(campaign_id: str = _contract.CAMPAIGN_ID) -> Path:
    return r31.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(part: dict) -> Path:
    return r31.write_json(path_for(part["campaign_id"]), part)


def load(campaign_id: str = _contract.CAMPAIGN_ID) -> Optional[dict]:
    return r31.read_json(path_for(campaign_id))
