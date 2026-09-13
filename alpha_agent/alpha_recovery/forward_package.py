"""alpha_agent.alpha_recovery.forward_package - immutable candidate records and the
human-gated adoption command.

Workstream 10. A candidate that clears the frozen historical gates becomes an
IMMUTABLE artifact: exact specification hash, information identity, universe
identity, portfolio / cost identity, qualification evidence and a prospective
freeze package - the same shape ``alpha_agent.r64.challenger`` writes, so the
existing human-gated path (``scripts/adopt_prospective_freeze.py`` ->
``api.prospective_adoption`` -> ``api.forward_challenger_registry``) can bind
to exactly this specification later. Nothing here promotes, registers, adopts
or changes a holding; the file name carries the record hash so a re-run
whose numbers changed writes a NEW file and never overwrites a record.
"""
from __future__ import annotations

import json

from . import (CAMPAIGN_ID, EQ_COST_RATE_PER_SIDE, EQ_TOP_N_OPERATIONAL, INCUMBENT_MODEL_ID,
               LOCKBOX_START, contract_hash, protocol_hash, research_root, stable_hash,
               write_artifact)

CALCULATION_OWNER = "alpha_agent.alpha_recovery.forward_package"
ARTIFACT_NAME = "forward_candidates.json"
RECORD_SCHEMA = "alpha_recovery_forward_candidate/1"
SUBDIR = "challengers"

READY_VERDICTS = ("MATERIALLY_BEATS_INCUMBENT", "ECONOMIC_UNDER_CONTROLS", "CALIBRATED_DIRECTIONAL_SKILL")
SURVIVOR_VERDICTS = ("BEATS_INCUMBENT_NOT_QUALIFIED", "ECONOMIC_UNDER_CONTROLS_NOT_FDR",
                     "ECONOMIC_UNDER_CONTROLS_UNSTABLE", "PROFITABLE_NOT_CALIBRATED")

CONFIRM_TOKEN = "ADOPT_PROSPECTIVE_FORWARD_CLOCKS"


def adoption_command(challenger_id: str) -> str:
    """The EXACT human-gated command. Dry run by default; a live registration
    needs BOTH the confirmation token and --execute, typed by a human."""
    return ("C:\\Users\\binis\\paper_trader\\.venv-win\\Scripts\\python.exe "
            "scripts\\adopt_prospective_freeze.py --challenger-id %s "
            "--confirm %s --execute" % (challenger_id, CONFIRM_TOKEN))


def same_domain_spec(cell: dict) -> dict:
    return {"schema": "alpha_recovery_forward_specification/1",
            "cell_id": cell.get("cell_id"), "scope": cell.get("scope"), "mode": "XS",
            "horizon_sessions": int(cell.get("horizon") or 0), "cadence_sessions": cell.get("cadence"),
            "universe": {"rule": "R63 equity eligibility AND PANEL-F core record (PIT S&P 500 members, "
                                 "unadjusted close >= 5, 63-session median dollar volume >= 10M, >= 234 "
                                 "of 260 prior sessions)", "n_instruments": cell.get("n_instruments")},
            "signal": {"baseline": cell.get("baseline"), "added_dimension": cell.get("dimension"),
                       "features": ("ear: 3-session announcement-window abnormal return vs SPY carried 63 "
                                    "sessions; sue: XBRL standardised unexpected earnings carried 63 "
                                    "sessions; sessions_since_event capped 126"),
                       "model": "ridge on training-standardised features; penalty by blocked inner CV on "
                                "the baseline arm and forced on the augmented arm; R63 yearly expanding folds "
                                "with purge and embargo"},
            "construction": {"book": "long-only equal-weight top-%d" % EQ_TOP_N_OPERATIONAL,
                             "benchmark": "equal-weight scored universe",
                             "cost_per_side": EQ_COST_RATE_PER_SIDE},
            "incumbent": INCUMBENT_MODEL_ID,
            "emission_rule": "a decision is formed at close t from data dated <= t and emitted strictly "
                             "before session t+1; NEXT_CLOSE entry; no backfill",
            "lockbox_start": LOCKBOX_START, "pit_quality": "PIT_TRUE (filed dates) / PIT_MARKET_OBSERVABLE",
            "protocol_sha256": protocol_hash(), "contract_sha256": contract_hash()}


def record(cell: dict, *, state: str, why: str, kind: str, spec: dict, evidence: dict) -> dict:
    body = {"schema": RECORD_SCHEMA, "campaign_id": CAMPAIGN_ID,
            "challenger_id": "ALPHA_RECOVERY_%s_H%d_%s" % (
                str(cell.get("family") or cell.get("dimension") or "X").upper(), int(cell.get("horizon") or 0),
                stable_hash(cell.get("cell_id"))[:8].upper()),
            "cell_id": cell.get("cell_id"), "classification": state, "why": why, "kind": kind,
            "information_identity": {"family": cell.get("family"), "dimension": cell.get("dimension"),
                                     "baseline": cell.get("baseline")},
            "asset_class": cell.get("scope"), "horizon_sessions": int(cell.get("horizon") or 0),
            "qualification_evidence": evidence, "forward_specification": spec,
            "freeze_record_hash": stable_hash(spec),
            "human_gated_adoption_command": None,
            "promotion_allowed": False, "live_registration_performed": False, "holdings_changed": False,
            "records_are_immutable": True}
    body["human_gated_adoption_command"] = adoption_command(body["challenger_id"])
    body["record_hash"] = stable_hash(body)
    return body


def build(*, tournament: dict | None, cadence: dict | None, write: bool = True) -> dict:
    ready, survivors = [], []
    for c in (tournament or {}).get("cells") or []:
        v = c.get("tournament_verdict")
        if v not in READY_VERDICTS + SURVIVOR_VERDICTS:
            continue
        op = ((c.get("head_to_head") or {}).get("top%d" % EQ_TOP_N_OPERATIONAL)) or {}
        ev = {"conditional": c.get("conditional"), "redundancy": c.get("redundancy"),
              "head_to_head_top25": {k: op.get(k) for k in ("all", "selection", "lockbox")},
              "gates": c.get("gates"), "fdr_pass_paired": c.get("fdr_pass_paired"),
              "holm_pass_family": c.get("holm_pass_family"), "stability": c.get("stability")}
        rec = record(c, state=v, why="tournament verdict %s" % v, kind="SAME_DOMAIN",
                     spec=same_domain_spec(c), evidence=ev)
        (ready if v in READY_VERDICTS else survivors).append(rec)
    for c in (cadence or {}).get("cells") or []:
        v = c.get("cadence_verdict")
        if v not in READY_VERDICTS + SURVIVOR_VERDICTS:
            continue
        e = c.get("cadence_economics") or {}
        pol = ((e.get("augmented") or {}).get("policy")) or {}
        spec = {"schema": "alpha_recovery_forward_specification/1", "cell_id": c.get("cell_id"),
                "scope": c.get("scope"), "mode": "XS", "horizon_sessions": 1,
                "trade_every_sessions": c.get("trade_every"), "no_trade_band": c.get("band"),
                "universe": {"instruments": c.get("instruments") or (c.get("inst") if isinstance(c.get("inst"), list) else None),
                             "rule": "R63 universe rule over the R41 dated-contract store"},
                "signal": {"baseline": c.get("baseline"), "added_dimension": c.get("dimension"), "kind": c.get("kind")},
                "construction": {k: pol.get(k) for k in sorted(pol)},
                "costs": "R38 per-market cost per side on one-way turnover",
                "emission_rule": "decision at close t from data <= t, effective close t+1, no backfill",
                "lockbox_start": LOCKBOX_START, "pit_quality": "PIT_MARKET_OBSERVABLE",
                "evidence_label": e.get("evidence_label"), "protocol_sha256": protocol_hash(),
                "contract_sha256": contract_hash()}
        ev = {"conditional": c.get("conditional"), "cadence_economics": {k: e.get(k) for k in
              ("ann_net_increment", "ann_net_increment_at_2x_cost", "sharpe_increment", "t_increment",
               "p_increment_one_sided", "selection", "lockbox", "lockbox_sign_agrees", "evidence_label")},
              "augmented": {k: (e.get("augmented") or {}).get(k) for k in ("ann_net", "sharpe", "max_dd",
                                                                         "mean_oneway_turnover", "ann_cost_drag")},
              "fdr_pass_economic": c.get("fdr_pass_economic"), "holm_pass_family": c.get("holm_pass_family"),
              "stability": c.get("stability")}
        rec = record(c, state=v, why="cadence verdict %s" % v, kind="CROSS_DOMAIN_SLEEVE", spec=spec, evidence=ev)
        (ready if v in READY_VERDICTS else survivors).append(rec)
    d = research_root() / SUBDIR
    d.mkdir(parents=True, exist_ok=True)
    for rec in ready + survivors:
        p = d / ("%s_%s.json" % (rec["challenger_id"], rec["record_hash"][:12]))
        rec["record_file"] = p.name
        if not p.exists():
            p.write_text(json.dumps(rec, indent=1, sort_keys=True, default=str), encoding="utf-8")
    body = {"schema": "alpha_recovery_forward_candidates/1", "calculation_owner": CALCULATION_OWNER,
            "n_ready": len(ready), "n_survivors_not_qualified": len(survivors),
            "ready": ready, "survivors_not_qualified": survivors,
            "adoption_owner": "scripts/adopt_prospective_freeze.py -> api.prospective_adoption -> "
                              "api.forward_challenger_registry",
            "adoption_requires": "a human typing --confirm %s AND --execute; dry run otherwise" % CONFIRM_TOKEN,
            "note": ("a record here is a research artifact; it becomes a forward challenger only through the "
                     "human-gated path, and only a READY record should be offered for adoption"),
            "promotion_performed": False, "live_registration_performed": False, "artifact_dir": str(d)}
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body
