"""alpha_agent.r39.handoff - forward-evidence handoff and the Intrinio lane.

Any candidate that survives every historical gate is FROZEN here: an
immutable specification (model, code hash, data schema hash, features,
target, trade construction, hyperparameters, cost model, control, cadence,
sizing, allowed markets, risk constraints) prepared for TRUE_FORWARD shadow
evidence under the existing canonical forward-evidence architecture.

PREPARED, not activated: this release performs no operational write, so
activation of the shadow stream is an operator action through the canonical
forward-evidence owner. Historical research is never relabelled
TRUE_FORWARD (``contract.HISTORICAL_ALPHA_IS_NOT_TRUE_FORWARD_EVIDENCE``).

The Intrinio/Steele analyst lane continues in parallel, untouched: the
operator-ready sample request prepared by Release 38 is fingerprinted and
its status reported; nothing is sent, purchased or trialled by Claude.
"""
from __future__ import annotations

import time
from pathlib import Path

from .. import r39
from . import contract as C
from .estate import R38_V4

CALCULATION_OWNER = "alpha_agent.r39.handoff"
SCHEMA = "r39_forward_evidence_handoff/1"
ARTIFACT_NAME = "forward_evidence_handoff.json"
INTRINIO_NAME = "intrinio_parallel_status.json"

R39_PACKAGE_DIR = Path(__file__).resolve().parent


def _code_hash() -> str:
    parts = {}
    for p in sorted(R39_PACKAGE_DIR.glob("*.py")):
        parts[p.name] = r39.sha_file(p)
    return r39.sha(parts)


def freeze_forward_candidates(director, qualified: list, *,
                              state_contract_hash: str,
                              campaign_id: str = C.CAMPAIGN_ID) -> dict:
    specs = []
    for c in qualified:
        cols = list(director.bundles.get(c["bundle"], []))
        spec = {
            "candidate_id": c["candidate_id"],
            "spec_hash": director.spec_hash(c),
            "origin": c.get("origin"),
            "model": c["model"],
            "model_versions": {
                k: v for k, v in
                __import__("alpha_agent.r39.model_registry",
                           fromlist=["_versions"])._versions().items()},
            "code_hash": _code_hash(),
            "data_schema_hash": state_contract_hash,
            "feature_definition": cols,
            "target_definition": c["target"],
            "trade_construction": c["expression"],
            "hyperparameters": c["hyper"],
            "cost_model": {"base": C.COST_BASE,
                           "state": C.COST_MODEL_STATE,
                           "stress_multiplier": C.COST_STRESS_MULTIPLIER},
            "control": (director.results[c["candidate_id"]]
                        .get("zone_c", {}) or {}).get("control"),
            "decision_cadence_sessions": c["horizon"],
            "position_sizing_rule": "equal weight within expression legs; "
                                    "no leverage",
            "allowed_markets": c["scope"],
            "risk_constraints": "gross exposure <= 1; no leverage; "
                                "paper only",
            "refit_zones": ["ZONE_A", "ZONE_B"],
        }
        specs.append(spec)
    body = r39.artifact_body(SCHEMA, {
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "frozen_forward_candidates": specs,
        "n_frozen": len(specs),
        "registration_state": "PREPARED_NOT_ACTIVATED",
        "activation_owner": "the operator, through the canonical "
                            "forward-evidence architecture (Phase 28 "
                            "family); this release writes nothing "
                            "operational",
        "true_forward_begins_only_after_freeze":
            C.TRUE_FORWARD_BEGINS_ONLY_AFTER_FREEZE,
        "historical_is_not_true_forward":
            C.HISTORICAL_ALPHA_IS_NOT_TRUE_FORWARD_EVIDENCE,
        "frozen_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    })
    body["forward_handoff_hash"] = r39.sha(body)
    r39.write_json(r39.campaign_dir(campaign_id) / ARTIFACT_NAME, body)
    return body


def intrinio_status(campaign_id: str = C.CAMPAIGN_ID) -> dict:
    msg = R38_V4 / "intrinio_steele_sample_request_message.md"
    req = R38_V4 / "intrinio_steele_sample_request.json"
    body = r39.artifact_body("r39_intrinio_parallel_status/1", {
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "lane": "Intrinio / Steele Barcomb historical analyst sample",
        "state": "OPERATOR_READY_NOT_SENT",
        "frozen_sample_tickers": ["AAPL", "MON", "META", "HTZ", "CALM"],
        "purpose": "SCHEMA_AND_PIT_VALIDATION_ONLY",
        "sample_is_alpha_evidence": False,
        "message_fingerprint": r39.file_fingerprint(msg),
        "request_fingerprint": r39.file_fingerprint(req),
        "actions_by_r39": "read-only status report; nothing sent, "
                          "purchased, trialled or licensed",
        "blocking": False,
    })
    body["intrinio_status_hash"] = r39.sha(body)
    r39.write_json(r39.campaign_dir(campaign_id) / INTRINIO_NAME, body)
    return body
