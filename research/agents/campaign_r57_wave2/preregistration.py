r"""campaign_r57_wave2.preregistration - apply the director's frozen agenda.

RESEARCH ONLY. PAPER ONLY. NO ORDERS, NO FILLS, NO PROMOTION, NO ADOPTION.

``campaign_agenda.json`` is the agenda as DATA: the data-foundation-agent's
certifications, the universe-construction-agent's rules, the feature-library-
agent's lineage and the director's ten frozen pre-registrations. This module
applies them through ``AgentPipeline`` in the one order the pipeline admits
(certify -> define -> publish -> preregister) and then writes the campaign
spec the local runner executes.

It is MECHANICAL. It decides nothing: every field it writes was named by an
agent in the agenda, and every refusal the pipeline raises is reported rather
than worked around.

IDEMPOTENT. Re-running it re-applies the same declarations - the memory's
``is_novel`` check makes a repeated pre-registration return the existing
experiment id rather than mint a second one - so a partial run can be
completed without minting duplicates.

    & .\.venv-win\Scripts\python.exe -m campaign_r57_wave2.preregistration

(run from ``research/agents`` on ``sys.path``; see
``scripts/preregister_wave2.py``.)
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from alpha_agent.agents_v2 import pipeline as P
from alpha_agent.r59 import memory as M

HERE = Path(__file__).resolve().parent
AGENDA = HERE / "campaign_agenda.json"
SPEC_OUT = HERE / "campaign_spec.json"
EXECUTOR_MODULE = "research/agents/campaign_r57_wave2/executors.py"

DATA_AGENT = "data-foundation-agent"
UNIVERSE_AGENT = "universe-construction-agent"
FEATURE_AGENT = "feature-library-agent"
DIRECTOR = "quant-research-director"


def load_agenda(path: Optional[Path] = None) -> dict:
    return json.loads(Path(path or AGENDA).read_text(encoding="utf-8-sig"))


def apply_foundation(pipe: P.AgentPipeline, agenda: dict) -> dict:
    out = {"datasets": [], "universes": [], "feature_sets": []}
    for d in agenda["datasets"]:
        pipe.perform(DATA_AGENT, "certify_data", {
            "dataset_id": d["dataset_id"],
            "asset_classes": d["asset_classes"],
            "pit_status": d["pit_status"],
            "availability_rule": d["availability_rule"],
            "survivorship": d["survivorship"],
            "path": d.get("path", ""), "notes": d.get("notes", "")})
        out["datasets"].append(d["dataset_id"])
    for u in agenda["universes"]:
        pipe.perform(UNIVERSE_AGENT, "define_universe", {
            "universe_id": u["universe_id"], "dataset_id": u["dataset_id"],
            "asset_class": u["asset_class"],
            "execution_representation": u["execution_representation"],
            "short_leg_expressible": bool(u.get("short_leg_expressible")),
            "rules": u["rules"]})
        out["universes"].append(u["universe_id"])
    for f in agenda["feature_sets"]:
        pipe.perform(FEATURE_AGENT, "publish_features", {
            "feature_set_id": f["feature_set_id"],
            "universe_id": f["universe_id"], "features": f["features"],
            "leakage_check": f["leakage_check"]})
        out["feature_sets"].append(f["feature_set_id"])
    return out


def _payload(agenda: dict, row: dict) -> dict:
    """The director's pre-registration, assembled from the frozen agenda.

    ``parameters`` carries everything that makes the cell REPRODUCIBLE and is
    part of the identity the memory hashes: the label, the executor, the
    rebalance rule, the cadence and the sample-start rule. A construction
    choice that is not in here is a construction choice that could be changed
    after the fact, which is the whole thing a pre-registration exists to
    prevent.
    """
    cost = agenda["cost_models"][row["cost_model_id"]]
    params = {
        "label": row["label"], "executor": row["executor"],
        "book": row["book"], "cadence_sessions": row["cadence_sessions"],
        "rebalance_rule": row["rebalance_rule"],
        "campaign_id": agenda["campaign_id"],
        "runner": "alpha_agent.agents_v2.runner",
        "sequential_reveal": "D -> V -> L, halted cells never compute L",
    }
    for k in ("declared_variant_of", "sample_start_rule"):
        if row.get(k):
            params[k] = row[k]
    return {
        "owning_agent": row["owning_agent"], "hypothesis": row["hypothesis"],
        "asset_class": row["asset_class"], "family": row["family"],
        "feature_set_id": row["feature_set_id"],
        "horizon_sessions": row["horizon_sessions"], "parameters": params,
        "discovery_sample": {"start": row.get("sample_start", "2011-07-01"),
                             "end": "2017-12-31"},
        "evaluation_sample": {"validation": "2018-01-01",
                              "lockbox": "2023-01-01"},
        "cost_model": dict(cost, cost_model_id=row["cost_model_id"]),
        "expected_sign": int(row["expected_sign"]),
        "information_family": row["information_family"],
        "long_short": bool(row["long_short"]),
        "within_family_tests": int(row["within_family_tests"]),
        "generative_search": bool(row["generative_search"]),
        "instrument_scope": row["instrument_scope"],
        "venue": "RESEARCH", "mechanism": row["mechanism"],
    }


def preregister_all(pipe: P.AgentPipeline, agenda: dict) -> list:
    by_executor = {r["executor"]: r for r in agenda["experiments"]}
    rows = []
    for ex in agenda["review_order"]:
        row = by_executor[ex]
        reg = pipe.perform(DIRECTOR, "preregister", _payload(agenda, row))
        rows.append({"experiment_id": reg["experiment_id"],
                     "executor": ex, "book": row["book"],
                     "label": row["label"],
                     "owning_agent": row["owning_agent"],
                     "spec_hash": reg["spec_hash"],
                     "already_registered": reg["already_registered"]})
    return rows


def write_spec(agenda: dict, rows: list,
               out: Optional[Path] = None) -> Path:
    body = {"campaign_id": agenda["campaign_id"],
            "agent_system_version": agenda["agent_system_version"],
            "executor_module": EXECUTOR_MODULE,
            "review_order": agenda["review_order"],
            "review_order_rule": agenda["review_order_rule"],
            "safety": agenda["safety"],
            "experiments": [{"experiment_id": r["experiment_id"],
                             "executor": r["executor"], "book": r["book"],
                             "label": r["label"],
                             "owning_agent": r["owning_agent"]}
                            for r in rows]}
    p = Path(out or SPEC_OUT)
    p.write_text(json.dumps(body, indent=1), encoding="utf-8")
    return p


def main(memory_path: Optional[str] = None,
         agenda_path: Optional[str] = None,
         spec_out: Optional[str] = None) -> dict:
    agenda = load_agenda(Path(agenda_path) if agenda_path else None)
    mem = M.ResearchMemory(Path(memory_path) if memory_path else None)
    pipe = P.AgentPipeline(mem)
    found = apply_foundation(pipe, agenda)
    rows = preregister_all(pipe, agenda)
    spec = write_spec(agenda, rows, Path(spec_out) if spec_out else None)
    return {"campaign_id": agenda["campaign_id"], "foundation": found,
            "experiments": rows, "spec": str(spec),
            "newly_registered": sum(1 for r in rows
                                    if not r["already_registered"])}
