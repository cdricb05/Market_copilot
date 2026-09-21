r"""campaign_r60_information_frontier.preregistration - apply the frozen agenda.

RESEARCH ONLY. PAPER ONLY. NO ORDERS, NO FILLS, NO PROMOTION, NO ADOPTION.

``campaign_agenda.json`` is the agenda as DATA: the data-foundation-agent's
certifications, the universe-construction-agent's rules, the feature-library-
agent's lineage and the director's frozen pre-registrations. This module
applies them through ``AgentPipeline`` in the one order the pipeline admits
(certify -> define -> publish -> preregister) and writes the campaign spec the
canonical runner executes.

It is MECHANICAL and decides nothing. Every field it writes was named by an
agent in the agenda; every refusal the pipeline raises is reported, never
worked around. IDEMPOTENT: the memory's ``is_novel`` check makes a repeated
pre-registration return the existing experiment id rather than mint a second.

WHAT IS NEW HERE, versus the R57 module it is modelled on: the spec it writes
DECLARES its substrate.

    "requires": {"datasets": [...], "universes": [...], "feature_sets": [...]}

``briefs.campaign_state`` scopes the three foundation flags to that block, so
the spawn gate asks "is THIS campaign's substrate certified", not "has the
estate ever certified anything". Without it, a campaign whose entire premise is
uncertified information would be told its data foundation was already done -
and then refused at write time by ``preregister`` for the missing feature set.

    & .\.venv-win\Scripts\python.exe -m campaign_r60_information_frontier.preregistration

(run from ``research/agents`` on ``sys.path``.)
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
EXECUTOR_MODULE = ("research/agents/campaign_r60_information_frontier/"
                   "executors.py")

DATA_AGENT = "data-foundation-agent"
UNIVERSE_AGENT = "universe-construction-agent"
FEATURE_AGENT = "feature-library-agent"
DIRECTOR = "quant-research-director"


def load_agenda(path: Optional[Path] = None) -> dict:
    return json.loads(Path(path or AGENDA).read_text(encoding="utf-8-sig"))


#: The data-foundation-agent grades on a five-value scale; the pipeline's
#: ``certify_data`` is BINARY (PIT_SAFE / NOT_PIT_SAFE) and ``preregister``
#: refuses anything that is not PIT_SAFE. The translation is declared here, in
#: one place, rather than performed silently at each call site.
#:
#: PIT_SAFE_WITH_LIMITATIONS maps to PIT_SAFE, and ONLY it does, because the
#: limitations it carries in this campaign are about COVERAGE (per-field
#: completeness by year; a join rate measured against a proxy symbol list)
#: and not about AVAILABILITY. The point-in-time rule itself -
#: latest-filed-as-of-t against each observation's own filing date - is sound
#: and was measured. A dataset whose AVAILABILITY is in doubt is a different
#: thing entirely and must not reach a book, so DATA_HOLD, PROSPECTIVE_ONLY
#: and REJECTED all map to NOT_PIT_SAFE and the pipeline then refuses every
#: hypothesis that depends on them - which is the correct outcome, not an
#: obstacle to route around.
PIT_MAP = {
    "PIT_SAFE": P.PIT_SAFE,
    "PIT_SAFE_WITH_LIMITATIONS": P.PIT_SAFE,
    "PROSPECTIVE_ONLY": P.NOT_PIT_SAFE,
    "DATA_HOLD": P.NOT_PIT_SAFE,
    "REJECTED": P.NOT_PIT_SAFE,
}


class CertificationRefused(RuntimeError):
    """A dataset row the translation cannot honestly carry."""


def _pit_status(d: dict) -> tuple:
    """(pipeline status, notes) for one agenda dataset row."""
    raw = str(d.get("pit_status") or "")
    if raw not in PIT_MAP:
        raise CertificationRefused(
            "unknown certification %r for %s" % (raw, d.get("dataset_id")))
    notes = d.get("notes", "")
    lims = d.get("limitations") or d.get(
        "limitations_that_must_travel_with_it") or []
    if raw == "PIT_SAFE_WITH_LIMITATIONS":
        if not lims:
            raise CertificationRefused(
                "%s is PIT_SAFE_WITH_LIMITATIONS but names no limitation; a "
                "qualified certification whose qualification is empty is not "
                "a certification" % d.get("dataset_id"))
        notes = ("CERTIFIED PIT_SAFE_WITH_LIMITATIONS. The PIT rule is sound; "
                 "the limitations are coverage, not availability. %s | %s"
                 % (notes, " | ".join(str(x) for x in lims)))
    return PIT_MAP[raw], notes


def apply_foundation(pipe: P.AgentPipeline, agenda: dict) -> dict:
    """certify -> define -> publish, in the only order the pipeline admits."""
    out = {"datasets": [], "universes": [], "feature_sets": [],
           "qualified": []}
    for d in agenda.get("datasets") or ():
        status, notes = _pit_status(d)
        if status == P.PIT_SAFE and not d.get("survivorship"):
            # The pipeline requires it, and for good reason: a PIT_SAFE claim
            # with no survivorship statement is the shape of every
            # survivor-biased result this estate has ever had to withdraw.
            # Refuse loudly rather than pass an empty string.
            raise CertificationRefused(
                "%s is certified PIT_SAFE but states no survivorship "
                "treatment" % d.get("dataset_id"))
        if status != P.PIT_SAFE:
            out["qualified"].append(
                {"dataset_id": d["dataset_id"], "certification":
                 d.get("pit_status"), "effect": "NOT_PIT_SAFE - every "
                 "hypothesis depending on it will be refused"})
        elif str(d.get("pit_status")) == "PIT_SAFE_WITH_LIMITATIONS":
            out["qualified"].append(
                {"dataset_id": d["dataset_id"],
                 "certification": "PIT_SAFE_WITH_LIMITATIONS",
                 "effect": "carried as PIT_SAFE; limitations travel in notes"})
        pipe.perform(DATA_AGENT, "certify_data", {
            "dataset_id": d["dataset_id"],
            "asset_classes": d["asset_classes"],
            "pit_status": status,
            "availability_rule": d["availability_rule"],
            "survivorship": d["survivorship"],
            "path": d.get("path", ""), "notes": notes})
        out["datasets"].append(d["dataset_id"])
    for u in agenda.get("universes") or ():
        pipe.perform(UNIVERSE_AGENT, "define_universe", {
            "universe_id": u["universe_id"], "dataset_id": u["dataset_id"],
            "asset_class": u["asset_class"],
            "execution_representation": u["execution_representation"],
            "short_leg_expressible": bool(u.get("short_leg_expressible")),
            "rules": u["rules"]})
        out["universes"].append(u["universe_id"])
    for f in agenda.get("feature_sets") or ():
        pipe.perform(FEATURE_AGENT, "publish_features", {
            "feature_set_id": f["feature_set_id"],
            "universe_id": f["universe_id"], "features": f["features"],
            "leakage_check": f["leakage_check"]})
        out["feature_sets"].append(f["feature_set_id"])
    return out


def _payload(agenda: dict, row: dict) -> dict:
    """The director's pre-registration, assembled from the frozen agenda.

    ``parameters`` carries everything that makes the cell REPRODUCIBLE and is
    part of the identity the memory hashes: label, executor, book, cadence,
    rebalance rule and sample-start rule. A construction choice that is not in
    here is one that could be changed after the fact, which is the whole thing
    a pre-registration exists to prevent.
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
    for k in ("declared_variant_of", "sample_start_rule", "hold_sessions",
              "information_source"):
        if row.get(k):
            params[k] = row[k]
    ev = agenda.get("evaluation_sample") or {"validation": "2018-01-01",
                                             "lockbox": "2023-01-01"}
    return {
        "owning_agent": row["owning_agent"], "hypothesis": row["hypothesis"],
        "asset_class": row["asset_class"], "family": row["family"],
        "feature_set_id": row["feature_set_id"],
        "horizon_sessions": row["horizon_sessions"], "parameters": params,
        "discovery_sample": {"start": row.get("sample_start", "2011-07-01"),
                             "end": ev["validation"]},
        "evaluation_sample": dict(ev),
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
    """Pre-register in REVIEW ORDER, which is also the execution order: a
    halted cell settles and counts to the burden just like a reviewed one, so
    the strongest ideas must pay the smallest denominator."""
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


def write_spec(agenda: dict, rows: list, found: dict,
               out: Optional[Path] = None) -> Path:
    body = {"campaign_id": agenda["campaign_id"],
            "agent_system_version": agenda["agent_system_version"],
            "executor_module": EXECUTOR_MODULE,
            "review_order": agenda["review_order"],
            "review_order_rule": agenda["review_order_rule"],
            "safety": agenda["safety"],
            # The substrate this campaign needs, so the spawn gate asks about
            # THIS campaign rather than about the estate's history.
            "requires": {"datasets": list(found["datasets"]),
                         "universes": list(found["universes"]),
                         "feature_sets": list(found["feature_sets"])},
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
    spec = write_spec(agenda, rows, found, Path(spec_out) if spec_out else None)
    return {"campaign_id": agenda["campaign_id"], "foundation": found,
            "experiments": rows, "spec": str(spec),
            "newly_registered": sum(1 for r in rows
                                    if not r["already_registered"])}


if __name__ == "__main__":
    print(json.dumps(main(), indent=1))
