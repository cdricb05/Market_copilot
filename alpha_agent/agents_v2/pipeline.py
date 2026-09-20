"""alpha_agent.agents_v2.pipeline - the governed agent handoff pipeline.

A PROJECTION over the one research memory, never a second registry. An agent
experiment IS a ``ResearchMemory.hypotheses`` row (``release=AGENTS_V2``) and
its history IS a run of ``AGENTS_V2_*`` rows in that memory's append-only
``events`` journal. This module decides exactly one kind of thing - WHO may
hand WHAT to WHOM, and in what order:

    director            preregister  ->  mints the experiment id, freezes the
                                         spec, the sign, the cost model and the
                                         gate-schema hash BEFORE evaluation
    data / universe /   certify_data -> define_universe -> publish_features
    features                             (each refuses without its predecessor)
    signal agents       reveal_stage      D, then V, then L - a later layer
                                          is refused until the earlier one
                                          earned it, and a layer that does not
                                          advance SETTLES the experiment
    signal agents       submit_candidate  (own experiments only, frozen spec,
                                          assembled from revealed layers only)
    skeptic             skeptic_review    rejects by default; the ONLY door
    risk                risk_review       skeptic survivors only
    meta                meta_review       validated survivors only
    director            director_clear    the final tournament ruling
    publishing          publish_candidate, request_forward_registration

Everything that is a CALCULATION is delegated to its canonical owner:

    the statistical verdict     alpha_agent.r59.engines.gate
    the search denominator      alpha_agent.r59.handlers.search_denominator
    the prospective freeze      alpha_agent.r59.handlers.freeze_qualified
    forward adoption            INJECTED (api.prospective_adoption); this
                                package never imports the application layer

There is no verb here that creates an order, a fill, a proposal approval, a
promotion or a capital decision, and ``perform`` refuses any verb that is not
in the calling agent's contract. Nothing is ever deleted: a killed experiment
stays in the memory, in the ledger and in the search burden.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Optional

from .. import r59
from ..r59 import engines as E
from ..r59 import handlers as H
from ..r59 import memory as M
from . import (AGENT_SYSTEM_VERSION, CAPITAL_ELIGIBILITY_OWNER,
               FORWARD_ADOPTION_OWNER, FORWARD_EVIDENCE_OWNER,
               FORWARD_MATURATION_OWNER, FORWARD_REGISTRAR_OWNER,
               GENERATION_METHOD, OPERATOR_ADOPTION_ENTRYPOINT, ORIGIN_PREFIX,
               PROSPECTIVE_FREEZE_OWNER,
               RELEASE, SAFETY, SIGNAL_AGENTS, SIGNAL_PUBLISHING_BOUNDARY)
from .contracts import Contracts

# --------------------------------------------------------------------------- #
# Event kinds. Every one is prefixed so the journal can be read back by kind
# through the memory's own public reader.
# --------------------------------------------------------------------------- #
EV_DATA = "AGENTS_V2_DATA_CERTIFIED"
EV_UNIVERSE = "AGENTS_V2_UNIVERSE_DEFINED"
EV_FEATURES = "AGENTS_V2_FEATURES_PUBLISHED"
EV_PREREG = "AGENTS_V2_PREREGISTERED"
EV_STAGE = "AGENTS_V2_STAGE_REVEALED"
EV_CANDIDATE = "AGENTS_V2_CANDIDATE_SUBMITTED"
EV_SKEPTIC = "AGENTS_V2_SKEPTIC_REVIEW"
EV_RISK = "AGENTS_V2_RISK_REVIEW"
EV_META = "AGENTS_V2_META_REVIEW"
EV_DIRECTOR = "AGENTS_V2_DIRECTOR_DECISION"
EV_PUBLISHED = "AGENTS_V2_CANDIDATE_PUBLISHED"
EV_FORWARD = "AGENTS_V2_FORWARD_REQUEST"
EV_REFUSED = "AGENTS_V2_REFUSED"

PIT_SAFE = "PIT_SAFE"
NOT_PIT_SAFE = "NOT_PIT_SAFE"

SK_KILLED = "KILLED"
SK_SURVIVED = "SURVIVED"
RISK_ACCEPTABLE = "ACCEPTABLE"
RISK_REJECTED = "REJECTED"
META_READY = ("ENSEMBLE_READY", "STANDALONE")
META_VERDICTS = META_READY + ("REDUNDANT",)
DIRECTOR_DECISIONS = ("CLEARED", "HELD", "REJECTED")

REQUIRED_PREREG_FIELDS = (
    "owning_agent", "hypothesis", "asset_class", "family", "feature_set_id",
    "horizon_sessions", "parameters", "discovery_sample", "evaluation_sample",
    "cost_model", "expected_sign")

#: Keys a forward-registration request may NEVER carry. There is no way to ask
#: this pipeline for a backfill, because there is no argument that means one.
BACKFILL_KEYS = ("effective_from", "inception", "inception_override",
                 "backfill", "observation_clock_starts", "start_date",
                 "as_of", "backdate")

_EVENT_SCAN_LIMIT = 100000


class PipelineRefusal(RuntimeError):
    """A governed refusal. ``code`` is stable; the message is for a human."""

    def __init__(self, code: str, message: str):
        super().__init__("%s: %s" % (code, message))
        self.code = code


def _spec_hash(spec: dict) -> str:
    return r59.stable_hash(spec)


class AgentPipeline:
    """Role-checked handoffs over ONE :class:`ResearchMemory`."""

    def __init__(self, mem: M.ResearchMemory, *,
                 contracts: Optional[Contracts] = None,
                 artifact_root: Optional[Path] = None):
        self.mem = mem
        self.contracts = contracts or Contracts()
        self._artifact_root = Path(artifact_root) if artifact_root else None

    # -- plumbing ----------------------------------------------------------- #
    @property
    def artifact_root(self) -> Path:
        return self._artifact_root or (r59.research_root() / "agents_v2")

    def _require(self, agent: str, verb: str) -> None:
        if verb not in self.contracts.verbs_for(agent):
            self.mem.event(EV_REFUSED, subject=agent,
                           detail={"verb": verb,
                                   "reason": "VERB_NOT_IN_AGENT_CONTRACT"})
            raise PipelineRefusal(
                "VERB_NOT_IN_AGENT_CONTRACT",
                "%s may not perform %r under %s"
                % (agent, verb, AGENT_SYSTEM_VERSION))

    def _events(self, kind: str, subject: Optional[str] = None) -> list:
        rows = self.mem.events(kind=kind, limit=_EVENT_SCAN_LIMIT)
        if subject is not None:
            rows = [r for r in rows if r.get("subject") == subject]
        return list(reversed(rows))                      # oldest first

    def _latest(self, kind: str, subject: str) -> Optional[dict]:
        rows = self._events(kind, subject)
        return rows[-1]["detail"] if rows else None

    def _experiment(self, experiment_id: str) -> dict:
        row = self.mem.get(experiment_id)
        if row is None or row.get("release") != RELEASE:
            raise PipelineRefusal("UNKNOWN_EXPERIMENT",
                                  "%s is not a pre-registered %s experiment"
                                  % (experiment_id, RELEASE))
        return row

    def _write_artifact(self, subdir: str, name: str, body: dict) -> Path:
        d = self.artifact_root / subdir
        d.mkdir(parents=True, exist_ok=True)
        body = dict(body)
        body.setdefault("agent_system_version", AGENT_SYSTEM_VERSION)
        body.setdefault("generated_at", r59.now_iso())
        body["safety"] = dict(SAFETY)
        body["artifact_hash"] = r59.stable_hash(
            {k: v for k, v in body.items()
             if k not in ("artifact_hash", "generated_at")})
        p = d / name
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_text(json.dumps(body, indent=1, default=str),
                       encoding="utf-8")
        tmp.replace(p)
        return p

    # -- the one generic entry point ---------------------------------------- #
    def perform(self, agent: str, verb: str, payload: Optional[dict] = None,
                **injected: Any) -> dict:
        """Dispatch ``verb`` for ``agent``. An unknown verb - and every verb
        that would be operational is unknown - is refused and journalled."""
        fn = _VERBS.get(verb)
        if fn is None:
            self.mem.event(EV_REFUSED, subject=agent,
                           detail={"verb": verb, "reason": "UNKNOWN_VERB"})
            raise PipelineRefusal(
                "UNKNOWN_VERB",
                "%r is not a pipeline verb. %s" % (verb,
                                                   SIGNAL_PUBLISHING_BOUNDARY))
        body = {k: v for k, v in (payload or {}).items() if k != "agent"}
        try:
            return fn(self, agent=agent, **body, **injected)
        except TypeError as exc:
            raise PipelineRefusal("BAD_PAYLOAD", str(exc)) from exc

    # -- foundation stages --------------------------------------------------- #
    def certify_data(self, *, agent: str, dataset_id: str, asset_classes: list,
                     pit_status: str, availability_rule: str,
                     survivorship: str, path: str = "",
                     notes: str = "") -> dict:
        self._require(agent, "certify_data")
        if pit_status not in (PIT_SAFE, NOT_PIT_SAFE):
            raise PipelineRefusal("BAD_PIT_STATUS", str(pit_status))
        bad = [a for a in asset_classes if a not in r59.ASSET_CLASSES]
        if bad or not asset_classes:
            raise PipelineRefusal("UNKNOWN_ASSET_CLASS", str(bad))
        if pit_status == PIT_SAFE and not (availability_rule and survivorship):
            raise PipelineRefusal(
                "CERTIFICATION_INCOMPLETE",
                "a PIT_SAFE certification must state the availability rule "
                "and the survivorship treatment")
        detail = {"agent": agent, "dataset_id": dataset_id,
                  "asset_classes": list(asset_classes),
                  "pit_status": pit_status,
                  "availability_rule": availability_rule,
                  "survivorship": survivorship, "path": path, "notes": notes}
        self.mem.event(EV_DATA, subject=dataset_id, detail=detail)
        return {"state": "DATA_CERTIFIED", **detail}

    def define_universe(self, *, agent: str, universe_id: str, dataset_id: str,
                        asset_class: str, execution_representation: str,
                        rules: str, short_leg_expressible: bool = False
                        ) -> dict:
        self._require(agent, "define_universe")
        cert = self._latest(EV_DATA, dataset_id)
        if cert is None:
            raise PipelineRefusal("DATA_NOT_CERTIFIED", dataset_id)
        if cert["pit_status"] != PIT_SAFE:
            raise PipelineRefusal("DATA_NOT_PIT_SAFE", dataset_id)
        if asset_class not in cert["asset_classes"]:
            raise PipelineRefusal(
                "ASSET_CLASS_NOT_CERTIFIED",
                "%s is not certified for %s" % (dataset_id, asset_class))
        detail = {"agent": agent, "universe_id": universe_id,
                  "dataset_id": dataset_id, "asset_class": asset_class,
                  "execution_representation": execution_representation,
                  "short_leg_expressible": bool(short_leg_expressible),
                  "rules": rules}
        self.mem.event(EV_UNIVERSE, subject=universe_id, detail=detail)
        return {"state": "UNIVERSE_DEFINED", **detail}

    def publish_features(self, *, agent: str, feature_set_id: str,
                         universe_id: str, features: list,
                         leakage_check: str) -> dict:
        self._require(agent, "publish_features")
        uni = self._latest(EV_UNIVERSE, universe_id)
        if uni is None:
            raise PipelineRefusal("UNIVERSE_NOT_DEFINED", universe_id)
        if leakage_check != "PASS":
            raise PipelineRefusal(
                "FEATURES_NOT_LEAK_SAFE",
                "a feature set is published only with leakage_check=PASS")
        undocumented = [f for f in (features or [])
                        if not (isinstance(f, dict) and f.get("name")
                                and f.get("lag") is not None
                                and f.get("source"))]
        if undocumented or not features:
            raise PipelineRefusal(
                "FEATURE_LINEAGE_INCOMPLETE",
                "every feature needs name, lag and source")
        detail = {"agent": agent, "feature_set_id": feature_set_id,
                  "universe_id": universe_id,
                  "dataset_id": uni["dataset_id"],
                  "asset_class": uni["asset_class"],
                  "features": features, "leakage_check": leakage_check}
        self.mem.event(EV_FEATURES, subject=feature_set_id, detail=detail)
        return {"state": "FEATURES_PUBLISHED", **detail}

    # -- director: pre-registration ----------------------------------------- #
    def preregister(self, *, agent: str, **spec_in: Any) -> dict:
        self._require(agent, "preregister")
        missing = [k for k in REQUIRED_PREREG_FIELDS
                   if spec_in.get(k) in (None, "", [], {})
                   and k != "parameters"]
        if "parameters" not in spec_in:
            missing.append("parameters")
        if missing:
            raise PipelineRefusal("PREREGISTRATION_INCOMPLETE",
                                  "missing: %s" % ", ".join(sorted(missing)))
        owner = spec_in["owning_agent"]
        if owner not in SIGNAL_AGENTS:
            raise PipelineRefusal("OWNER_NOT_A_SIGNAL_AGENT", str(owner))
        family = spec_in["family"]
        if family not in self.contracts.families_for(owner):
            raise PipelineRefusal(
                "FAMILY_NOT_ROUTED_TO_AGENT",
                "%s is not routed to %s" % (family, owner))
        if spec_in["expected_sign"] not in (1, -1):
            raise PipelineRefusal("SIGN_NOT_DECLARED",
                                  "expected_sign must be +1 or -1")
        feats = self._latest(EV_FEATURES, spec_in["feature_set_id"])
        if feats is None:
            raise PipelineRefusal("FEATURES_NOT_PUBLISHED",
                                  str(spec_in["feature_set_id"]))
        ac = spec_in["asset_class"]
        if ac not in r59.ASSET_CLASSES:
            raise PipelineRefusal("UNKNOWN_ASSET_CLASS", str(ac))
        if ac != feats["asset_class"] and ac != r59.AC_CROSS_ASSET:
            raise PipelineRefusal(
                "ASSET_CLASS_FEATURE_MISMATCH",
                "experiment is %s but feature set %s is %s"
                % (ac, feats["feature_set_id"], feats["asset_class"]))
        cert = self._latest(EV_DATA, feats["dataset_id"]) or {}
        if cert.get("pit_status") != PIT_SAFE:
            raise PipelineRefusal("DATA_NOT_PIT_SAFE", feats["dataset_id"])

        spec = {k: spec_in[k] for k in REQUIRED_PREREG_FIELDS}
        spec["mechanism"] = spec_in.get("mechanism", "")
        spec["long_short"] = bool(spec_in.get("long_short", False))
        spec["within_family_tests"] = int(spec_in.get("within_family_tests", 1))
        # THE BURDEN FLAG, frozen here and never chosen at review time. A
        # candidate selected out of a generated or swept space is charged the
        # estate's prior machine-representation search in its asset class
        # (alpha_agent.r59.handlers.search_denominator). Until R57 the one
        # caller passed a hard-coded False, so that prior was unreachable for
        # every agent experiment however the candidate had been found.
        spec["generative_search"] = bool(spec_in.get("generative_search",
                                                     False))
        # What a forward registration will need to NAME this book. Carried in
        # the frozen spec because api.prospective_adoption reads the identity
        # from the freeze and defaults nothing into existence.
        spec["instrument_scope"] = list(spec_in.get("instrument_scope") or [])
        spec["venue"] = spec_in.get("venue") or ""
        spec["sleeve"] = spec_in.get("sleeve") or ""
        spec["pit_status"] = cert["pit_status"]
        spec["dataset_id"] = feats["dataset_id"]
        spec["universe_id"] = feats["universe_id"]
        spec["frozen_gate_schema_hash"] = self.contracts.gate_schema_hash
        spec["agent_system_version"] = AGENT_SYSTEM_VERSION

        model_family = "XS_LONG_SHORT" if spec["long_short"] else "LONG_ONLY"
        info_family = spec_in.get("information_family") or "PRICE_STATE"
        fam_key = M.family_key(economic_family=family,
                               information_family=info_family,
                               asset_class=ac, model_family=model_family)
        novelty = self.mem.is_novel(family=fam_key, spec=spec)
        if not novelty["novel"]:
            raise PipelineRefusal(
                "ALREADY_SETTLED_DO_NOT_REPEAT",
                "%s is %s; reopen condition: %s"
                % (novelty["hypothesis_id"], novelty.get("reason"),
                   novelty.get("reopen_condition")))
        already = novelty["prior"] is not None
        hid = self.mem.register(
            title=str(spec["hypothesis"])[:240], release=RELEASE,
            origin=ORIGIN_PREFIX + owner,
            generation_method=GENERATION_METHOD,
            information_family=info_family, economic_family=family,
            asset_class=ac, model_family=model_family,
            horizon_sessions=int(spec["horizon_sessions"]),
            input_data_identity=str(spec["feature_set_id"]), spec=spec)
        if not already:
            self.mem.event(EV_PREREG, subject=hid,
                           detail={"agent": agent, "owning_agent": owner,
                                   "spec_hash": _spec_hash(spec),
                                   "family_key": fam_key})
        return {"state": "PREREGISTERED", "experiment_id": hid,
                "owning_agent": owner, "spec_hash": _spec_hash(spec),
                "already_registered": already,
                "frozen_gate_schema_hash": spec["frozen_gate_schema_hash"]}

    # -- signal agents: SEQUENTIAL reveal ------------------------------------ #
    def _stage_events(self, experiment_id: str) -> list:
        return [r["detail"] for r in self._events(EV_STAGE, experiment_id)]

    def _owning_agent(self, agent: str, experiment_id: str) -> dict:
        row = self._experiment(experiment_id)
        spec = row.get("spec") or {}
        if spec.get("owning_agent") != agent:
            raise PipelineRefusal(
                "NOT_THE_OWNING_AGENT",
                "%s is assigned to %s" % (experiment_id,
                                          spec.get("owning_agent")))
        return row

    def reveal_stage(self, *, agent: str, experiment_id: str, stage: str,
                     spec_hash: str, stats: Optional[dict] = None,
                     evaluator: str = "") -> dict:
        """Measure and reveal ONE evaluation layer.

        The three layers are a TIME partition, so a book run over the whole
        grid produces all three at once - which is exactly how the lockbox
        came to be read before the earlier layers had earned it. Here a layer
        is revealed only when its predecessor advanced, and the runner that
        calls this verb measures no further than the layer it is revealing.

        A layer that does not advance HALTS the experiment: it is settled
        NO_ALPHA_EVIDENCE, it still counts to the search burden, and the
        lockbox is never computed at all.
        """
        self._require(agent, "reveal_stage")
        row = self._owning_agent(agent, experiment_id)
        spec = row.get("spec") or {}
        if spec_hash != _spec_hash(spec):
            raise PipelineRefusal(
                "SPEC_CHANGED_AFTER_PREREGISTRATION",
                "a layer was not measured under the frozen spec")
        if stage not in r59.STAGES:
            raise PipelineRefusal("UNKNOWN_STAGE",
                                  "%r is not one of %s" % (stage, r59.STAGES))
        if self._latest(EV_CANDIDATE, experiment_id) is not None:
            raise PipelineRefusal("CANDIDATE_ALREADY_SUBMITTED",
                                  "the result is final; a layer cannot be "
                                  "re-revealed after submission")
        revealed = self._stage_events(experiment_id)
        seen = [e["stage"] for e in revealed]
        if stage in seen:
            raise PipelineRefusal(
                "STAGE_ALREADY_REVEALED",
                "%s was revealed once; a second measurement of the same layer "
                "is a second draw and needs a second pre-registration"
                % stage)
        if any(not e.get("advance") for e in revealed):
            raise PipelineRefusal(
                "EXPERIMENT_HALTED",
                "%s stopped at layer %s; a halted experiment is settled, not "
                "continued" % (experiment_id,
                               [e["stage"] for e in revealed
                                if not e.get("advance")]))
        expected = r59.STAGES[len(seen)]
        if stage != expected:
            raise PipelineRefusal(
                "STAGE_OUT_OF_ORDER",
                "the next unrevealed layer is %s, not %s; %s is revealed "
                "before %s so the lockbox is never read first"
                % (expected, stage, " then ".join(r59.STAGES), stage))
        try:
            adv = E.stage_advance(stage, stats or {},
                                  expected_sign=int(spec["expected_sign"]))
        except E.StageRefusal as exc:
            raise PipelineRefusal("STAGE_NOT_JUDGEABLE", str(exc)) from exc
        detail = {"agent": agent, "stage": stage, "stats": stats or {},
                  "advance": bool(adv["advance"]),
                  "is_terminal_stage": bool(adv["is_terminal_stage"]),
                  "halt_reasons": adv["halt_reasons"],
                  "advance_floors": adv["advance_floors"],
                  "stats_hash": r59.stable_hash(stats or {}),
                  "evaluator": evaluator}
        self.mem.event(EV_STAGE, subject=experiment_id, detail=detail)
        halted = not adv["advance"] and not adv["is_terminal_stage"]
        if halted:
            self.mem.record_result(
                experiment_id, outcome=r59.HO_NO_ALPHA_EVIDENCE,
                evidence_maturity="HISTORICAL",
                economics={"layers": {e["stage"]: e["stats"]
                                      for e in revealed + [detail]}},
                robustness={"skeptic_verdict": "NOT_REVIEWED",
                            "sequential_reveal": "HALTED_AT_%s" % stage,
                            "stages_revealed": seen + [stage]},
                reason_rejected="HALTED_AT_%s: %s"
                                % (stage, "; ".join(adv["halt_reasons"])),
                reopen_condition="NEW_ORTHOGONAL_INFORMATION")
        return {"experiment_id": experiment_id, "stage": stage,
                "advance": bool(adv["advance"]), "halted": bool(halted),
                "next_stage": adv["next_stage"] if adv["advance"] else None,
                "halt_reasons": adv["halt_reasons"],
                "next": ("validation-skeptic-agent"
                         if adv["is_terminal_stage"] else None)}

    # -- signal agents ------------------------------------------------------- #
    def submit_candidate(self, *, agent: str, experiment_id: str,
                         spec_hash: str, signal_sign: int = 0,
                         evidence_kind: str = "HISTORICAL",
                         cost_model: Optional[dict] = None,
                         turnover: Optional[float] = None,
                         layers: Optional[dict] = None,
                         state: str = "MEASURED", reason: str = "",
                         evaluator: str = "") -> dict:
        self._require(agent, "submit_candidate")
        row = self._experiment(experiment_id)
        spec = row.get("spec") or {}
        if spec.get("owning_agent") != agent:
            raise PipelineRefusal(
                "NOT_THE_OWNING_AGENT",
                "%s is assigned to %s" % (experiment_id,
                                          spec.get("owning_agent")))
        if self._latest(EV_CANDIDATE, experiment_id) is not None:
            raise PipelineRefusal(
                "CANDIDATE_ALREADY_SUBMITTED",
                "one experiment, one result; a second draw needs a second "
                "pre-registration and is charged to the burden")
        if spec_hash != _spec_hash(spec):
            raise PipelineRefusal(
                "SPEC_CHANGED_AFTER_PREREGISTRATION",
                "the result was not measured under the frozen spec")
        if evidence_kind != "HISTORICAL":
            raise PipelineRefusal(
                "FORWARD_EVIDENCE_CANNOT_BE_SUBMITTED",
                "a candidate result is HISTORICAL; forward evidence is "
                "measured by %s" % FORWARD_EVIDENCE_OWNER)
        if state == "DATA_HOLD":
            detail = {"agent": agent, "state": "DATA_HOLD", "reason": reason}
            self.mem.event(EV_CANDIDATE, subject=experiment_id, detail=detail)
            self.mem.record_result(
                experiment_id, outcome=r59.HO_DATA_HOLD,
                robustness={"skeptic_verdict": "NOT_REVIEWED"},
                reason_rejected=reason or "DATA_HOLD",
                reopen_condition="DATA_AVAILABLE")
            return {"experiment_id": experiment_id, **detail}
        if not isinstance(layers, dict) or "L" not in layers \
                or "V" not in layers:
            raise PipelineRefusal(
                "LAYERS_MISSING",
                "a measured candidate reports the kernel's D/V/L layers")
        # SEQUENTIAL REVEAL. The submitted layers must be the layers that were
        # revealed through ``reveal_stage``, in order, byte for byte. Without
        # this, staging would be advisory: an agent could reveal D and then
        # submit a lockbox nobody ever earned.
        revealed = {e["stage"]: e for e in self._stage_events(experiment_id)}
        order = [e["stage"] for e in self._stage_events(experiment_id)]
        if order != list(r59.STAGES):
            raise PipelineRefusal(
                "LAYERS_NOT_SEQUENTIALLY_REVEALED",
                "layers revealed %s; %s must be revealed in that order "
                "through reveal_stage before a candidate exists"
                % (order or "none", " -> ".join(r59.STAGES)))
        drifted = [s for s in r59.STAGES
                   if r59.stable_hash(layers.get(s) or {})
                   != revealed[s]["stats_hash"]]
        if drifted:
            raise PipelineRefusal(
                "SUBMITTED_LAYER_DIFFERS_FROM_REVEALED",
                "layer(s) %s do not match what was revealed" % drifted)
        if turnover is None or cost_model is None:
            raise PipelineRefusal("TURNOVER_OR_COST_MISSING",
                                  "turnover and cost model are mandatory")
        detail = {"agent": agent, "state": "MEASURED",
                  "signal_sign": int(signal_sign), "cost_model": cost_model,
                  "turnover": float(turnover), "layers": layers,
                  "evaluator": evaluator}
        self.mem.event(EV_CANDIDATE, subject=experiment_id, detail=detail)
        return {"experiment_id": experiment_id, "state": "CANDIDATE_SUBMITTED",
                "next": "validation-skeptic-agent"}

    # -- skeptic: the only door --------------------------------------------- #
    def skeptic_review(self, *, agent: str, experiment_id: str,
                       checks: Optional[dict] = None,
                       kill_reason: str = "") -> dict:
        self._require(agent, "skeptic_review")
        row = self._experiment(experiment_id)
        spec = row.get("spec") or {}
        cand = self._latest(EV_CANDIDATE, experiment_id)
        if cand is None or cand.get("state") != "MEASURED":
            raise PipelineRefusal("NO_MEASURED_CANDIDATE", experiment_id)
        if self._latest(EV_SKEPTIC, experiment_id) is not None:
            raise PipelineRefusal(
                "ALREADY_REVIEWED",
                "a verdict is final; a killed candidate is never re-reviewed "
                "into a pass")
        if spec.get("frozen_gate_schema_hash") != \
                self.contracts.gate_schema_hash:
            raise PipelineRefusal(
                "THRESHOLDS_CHANGED_AFTER_PREREGISTRATION",
                "the gate schema is not the one frozen at pre-registration")

        # The generative prior is read from the FROZEN pre-registration, not
        # hard-coded here. ``search_denominator`` additionally charges it when
        # the attributed family is itself a machine-representation family.
        den = H.search_denominator(
            self.mem, family_key=row["family_key"],
            asset_class=row["asset_class"],
            machine_generated=bool(spec.get("generative_search", False)),
            campaign_method=GENERATION_METHOD,
            within_family_tests=int(spec.get("within_family_tests", 1)))
        g = E.gate({"layers": cand["layers"]}, prior_burden=den["total"],
                   family_tests=1)

        ceiling = self.contracts.gate_schema()["canonical_statistical_gate"][
            "inherited_thresholds"]["max_turnover_per_decision_one_side"]
        machine = {
            "sign_consistent": cand["signal_sign"] == spec["expected_sign"],
            "cost_model_frozen": cand["cost_model"] == spec["cost_model"],
            "turnover_within_ceiling": cand["turnover"] <= float(ceiling),
        }
        adversarial = {}
        for cid in self.contracts.required_adversarial_checks():
            c = (checks or {}).get(cid) or {}
            adversarial[cid] = bool(
                c.get("passed") is True and c.get("measured") is not None
                and str(c.get("evidence") or "").strip())
        failed = ([k for k, v in g["checks"].items() if not v]
                  + [k for k, v in machine.items() if not v]
                  + [k for k, v in adversarial.items() if not v])
        if kill_reason:
            failed.append("SKEPTIC_KILL: %s" % kill_reason)
        survived = not failed
        verdict = SK_SURVIVED if survived else SK_KILLED
        if survived:
            outcome = r59.HO_NEEDS_MORE_EVIDENCE
        elif not g["qualified"]:
            outcome = r59.HO_NO_ALPHA_EVIDENCE
        else:
            outcome = r59.HO_REJECTED
        self.mem.record_result(
            experiment_id, outcome=outcome, evidence_maturity="HISTORICAL",
            statistic={"lockbox_t": g["lockbox_t"],
                       "lockbox_p_one_sided": g["lockbox_p_one_sided"],
                       "burden_corrected_p": g["burden_corrected_p"],
                       "burden_denominator": g["burden_denominator"],
                       "lockbox_observations": g["lockbox_observations"]},
            economics={"lockbox_materiality": g["lockbox_materiality"],
                       "validation_materiality": g["validation_materiality"],
                       "layers": cand["layers"]},
            turnover_cost={"turnover": cand["turnover"],
                           "cost_model": cand["cost_model"]},
            robustness={"skeptic_verdict": verdict,
                        "statistical_gate_owner": E.CALCULATION_OWNER,
                        "statistical_checks": g["checks"],
                        "machine_checks": machine,
                        "adversarial_checks": adversarial,
                        "adversarial_evidence": checks or {},
                        "search_denominator": den},
            reason_rejected=None if survived else "; ".join(failed),
            reopen_condition="NEW_ORTHOGONAL_INFORMATION")
        detail = {"agent": agent, "verdict": verdict, "failed": failed,
                  "outcome": outcome, "search_denominator": den}
        self.mem.event(EV_SKEPTIC, subject=experiment_id, detail=detail)
        return {"experiment_id": experiment_id, **detail,
                "next": "risk-portfolio-agent" if survived else None}

    # -- risk ---------------------------------------------------------------- #
    def survivors(self) -> list:
        """Experiment ids the skeptic passed. What the risk agent may see."""
        return [e["subject"] for e in self._events(EV_SKEPTIC)
                if e["detail"].get("verdict") == SK_SURVIVED]

    def risk_review(self, *, agent: str, experiment_id: str, verdict: str,
                    metrics: Optional[dict] = None, breach: str = "") -> dict:
        self._require(agent, "risk_review")
        self._experiment(experiment_id)
        if experiment_id not in self.survivors():
            raise PipelineRefusal(
                "NOT_A_SKEPTIC_SURVIVOR",
                "risk-portfolio-agent sees skeptic survivors only")
        if verdict not in (RISK_ACCEPTABLE, RISK_REJECTED):
            raise PipelineRefusal("BAD_RISK_VERDICT", str(verdict))
        if self._latest(EV_RISK, experiment_id) is not None:
            raise PipelineRefusal("ALREADY_RISK_REVIEWED", experiment_id)
        if verdict == RISK_ACCEPTABLE and not metrics:
            raise PipelineRefusal(
                "RISK_METRICS_MISSING",
                "ACCEPTABLE needs the measured risk metrics behind it")
        detail = {"agent": agent, "verdict": verdict,
                  "metrics": metrics or {}, "breach": breach}
        self.mem.event(EV_RISK, subject=experiment_id, detail=detail)
        if verdict == RISK_REJECTED:
            self._resettle(experiment_id, r59.HO_REJECTED,
                           "risk-portfolio-agent: %s" % (breach or "rejected"),
                           {"risk_verdict": verdict})
        return {"experiment_id": experiment_id, **detail}

    def validated_survivors(self) -> list:
        """Skeptic SURVIVED **and** risk ACCEPTABLE. What meta may see."""
        ok = set(self.survivors())
        return [e["subject"] for e in self._events(EV_RISK)
                if e["detail"].get("verdict") == RISK_ACCEPTABLE
                and e["subject"] in ok]

    # -- meta ---------------------------------------------------------------- #
    def meta_review(self, *, agent: str, experiment_ids: list, verdict: str,
                    correlation: Optional[dict] = None,
                    notes: str = "") -> dict:
        self._require(agent, "meta_review")
        valid = set(self.validated_survivors())
        intruders = [x for x in experiment_ids if x not in valid]
        if intruders or not experiment_ids:
            raise PipelineRefusal(
                "NOT_VALIDATED_SURVIVORS",
                "meta-model-ensemble-agent receives validated survivors "
                "only; refused: %s" % intruders)
        if verdict not in META_VERDICTS:
            raise PipelineRefusal("BAD_META_VERDICT", str(verdict))
        if verdict == "STANDALONE" and len(experiment_ids) != 1:
            raise PipelineRefusal("STANDALONE_MEANS_ONE", str(experiment_ids))
        detail = {"agent": agent, "verdict": verdict,
                  "members": list(experiment_ids),
                  "correlation": correlation or {}, "notes": notes}
        for x in experiment_ids:
            self.mem.event(EV_META, subject=x, detail=detail)
        return {"state": "META_REVIEWED", **detail}

    # -- director: final tournament ----------------------------------------- #
    def director_clear(self, *, agent: str, experiment_id: str, decision: str,
                       rationale: str) -> dict:
        self._require(agent, "director_clear")
        self._experiment(experiment_id)
        if decision not in DIRECTOR_DECISIONS:
            raise PipelineRefusal("BAD_DIRECTOR_DECISION", str(decision))
        if not str(rationale or "").strip():
            raise PipelineRefusal("RATIONALE_REQUIRED",
                                  "a ruling without a reason is not a ruling")
        if decision == "CLEARED":
            if experiment_id not in self.validated_survivors():
                raise PipelineRefusal(
                    "NOT_A_VALIDATED_SURVIVOR",
                    "the director cannot clear what the skeptic killed, "
                    "never reviewed, or the risk agent rejected")
            meta = self._latest(EV_META, experiment_id)
            if meta is None or meta.get("verdict") not in META_READY:
                raise PipelineRefusal(
                    "META_REVIEW_REQUIRED",
                    "clearance needs a meta review of ENSEMBLE_READY or "
                    "STANDALONE")
            self._resettle(experiment_id, r59.HO_QUALIFIED, None,
                           {"director_decision": decision})
        elif decision == "REJECTED":
            self._resettle(experiment_id, r59.HO_REJECTED,
                           "director: %s" % rationale,
                           {"director_decision": decision})
        detail = {"agent": agent, "decision": decision,
                  "rationale": rationale}
        self.mem.event(EV_DIRECTOR, subject=experiment_id, detail=detail)
        return {"experiment_id": experiment_id, **detail}

    def _resettle(self, experiment_id: str, outcome: str,
                  reason: Optional[str], extra_robustness: dict) -> None:
        """Move a settled row to a later outcome, carrying its evidence.

        ``record_result`` rewrites every evidence column, so the measured
        statistics are re-passed unchanged. Only the outcome, the reason and
        the additive robustness keys move.
        """
        row = self.mem.get(experiment_id) or {}
        rob = dict(row.get("robustness") or {})
        rob.update(extra_robustness)
        self.mem.record_result(
            experiment_id, outcome=outcome, evidence_maturity="HISTORICAL",
            statistic=row.get("statistic"), economics=row.get("economics"),
            turnover_cost=row.get("turnover_cost"), robustness=rob,
            reason_rejected=reason,
            reopen_condition=row.get("reopen_condition")
            or "NEW_ORTHOGONAL_INFORMATION")

    # -- publishing: the boundary ------------------------------------------- #
    def publish_candidate(self, *, agent: str, experiment_id: str) -> dict:
        self._require(agent, "publish_candidate")
        row = self._experiment(experiment_id)
        ruling = self._latest(EV_DIRECTOR, experiment_id)
        if row.get("outcome") != r59.HO_QUALIFIED or ruling is None \
                or ruling.get("decision") != "CLEARED":
            raise PipelineRefusal(
                "NOT_DIRECTOR_CLEARED",
                "only a candidate the director cleared may be published")
        body = {
            "kind": "RESEARCH_CANDIDATE",
            "experiment_id": experiment_id,
            "specification": row.get("spec"),
            "historical_evidence": {
                "note": "HISTORICAL ONLY - never forward evidence",
                "statistic": row.get("statistic"),
                "economics": row.get("economics"),
                "turnover_cost": row.get("turnover_cost"),
                "robustness": row.get("robustness")},
            "director_ruling": ruling,
            "safety_labels": ["RESEARCH ONLY", "PREVIEW ONLY", "NO ORDERS",
                              "ORDERS DISABLED", "AUTOMATION OFF",
                              "MANUAL REVIEW"],
            "boundary": SIGNAL_PUBLISHING_BOUNDARY,
            "operational_effect": "NONE",
        }
        path = self._write_artifact("candidates",
                                    "%s.json" % experiment_id, body)
        self.mem.event(EV_PUBLISHED, subject=experiment_id,
                       detail={"agent": agent, "artifact": str(path)})
        return {"experiment_id": experiment_id, "state": "PUBLISHED",
                "artifact": str(path)}

    def request_forward_registration(
            self, *, agent: str, experiment_id: str,
            frozen_decision_producer: str = "",
            adopt_forward: Optional[Callable] = None,
            **unexpected: Any) -> dict:
        """Raise ONE governed prospective registration request.

        The freeze is performed by ``alpha_agent.r59.handlers.freeze_qualified``
        and the adoption by the INJECTED ``api.prospective_adoption`` owner,
        which derives the observation clock itself. Nothing here names a date.
        """
        self._require(agent, "request_forward_registration")
        asked = [k for k in unexpected if k in BACKFILL_KEYS]
        if asked:
            raise PipelineRefusal(
                "BACKFILL_CANNOT_BE_REQUESTED",
                "%s: the prospective boundary is derived by %s and is never "
                "an argument" % (", ".join(sorted(asked)),
                                 FORWARD_ADOPTION_OWNER))
        if unexpected:
            raise PipelineRefusal("UNKNOWN_REQUEST_FIELD",
                                  ", ".join(sorted(unexpected)))
        self._experiment(experiment_id)
        if self._latest(EV_PUBLISHED, experiment_id) is None:
            raise PipelineRefusal("NOT_PUBLISHED", experiment_id)

        freeze = H.freeze_qualified(self.mem, hypothesis_id=experiment_id,
                                    adopt_forward=adopt_forward)
        adoption = freeze.get("forward_adoption") or {}
        if adoption.get("adopted"):
            forward_state = "FORWARD_REGISTERED"
        elif adopt_forward is None:
            forward_state = "FROZEN_AWAITING_ADOPTION_OWNER"
        else:
            forward_state = "FROZEN_ADOPTION_NOT_COMPLETED"
        request = {
            "kind": "GOVERNED_PROSPECTIVE_REGISTRATION_REQUEST",
            "experiment_id": experiment_id,
            "challenger_id": freeze.get("challenger_id"),
            "freeze_state": freeze.get("state"),
            "forward_state": forward_state,
            "observation_clock": "DERIVED_BY_%s" % FORWARD_ADOPTION_OWNER,
            "forward_observations_at_request": 0,
            "frozen_decision_producer": frozen_decision_producer
            or "FORWARD_PRODUCER_REQUIRED",
            "research_shadow_pnl": {
                "requested": True, "measured_by": FORWARD_EVIDENCE_OWNER,
                "matured_by": FORWARD_MATURATION_OWNER},
            "owners": {"freeze": PROSPECTIVE_FREEZE_OWNER,
                       "adoption": FORWARD_ADOPTION_OWNER,
                       "registrar": FORWARD_REGISTRAR_OWNER,
                       "evidence": FORWARD_EVIDENCE_OWNER,
                       "capital_eligibility": CAPITAL_ELIGIBILITY_OWNER},
            "adoption_outcome": adoption.get("outcome"),
            # The agents never start a forward clock. A human does, through the
            # one operator door, naming this challenger id.
            "operator_adoption": {
                "performed_by": "the human operator",
                "entrypoint": OPERATOR_ADOPTION_ENTRYPOINT,
                "argument": "--challenger-id %s" % freeze.get("challenger_id"),
                "default": "DRY RUN; a live write needs that script's own "
                           "confirmation token and --execute"},
            "auto_promotion": False, "capital_eligible": False,
            "operational_effect": "NONE",
        }
        path = self._write_artifact("forward_requests",
                                    "%s.json" % experiment_id, request)
        self.mem.event(EV_FORWARD, subject=experiment_id,
                       detail={"agent": agent, "forward_state": forward_state,
                               "challenger_id": freeze.get("challenger_id"),
                               "artifact": str(path)})
        return {**request, "artifact": str(path)}

    # -- the ledger: eighteen fields, failures included ---------------------- #
    def ledger(self) -> list:
        rows = [h for h in self.mem.list_hypotheses(limit=_EVENT_SCAN_LIMIT)
                if h.get("release") == RELEASE
                and h.get("generation_method") == GENERATION_METHOD]
        return [self._ledger_row(h) for h in rows]

    def _ledger_row(self, h: dict) -> dict:
        hid = h["hypothesis_id"]
        spec = h.get("spec") or {}
        rob = h.get("robustness") or {}
        cand = self._latest(EV_CANDIDATE, hid)
        risk = self._latest(EV_RISK, hid)
        meta = self._latest(EV_META, hid)
        ruling = self._latest(EV_DIRECTOR, hid)
        fwd = self._latest(EV_FORWARD, hid)
        skeptic = rob.get("skeptic_verdict") or "NOT_REVIEWED"
        risk_v = (risk or {}).get("verdict") or "NOT_REVIEWED"
        stages = [e["detail"] for e in self._events(EV_STAGE, hid)]
        revealed = [s["stage"] for s in stages]
        halted = next((s["stage"] for s in stages
                       if not s.get("advance") and not s.get(
                           "is_terminal_stage")), None)

        if self._latest(EV_PUBLISHED, hid) is not None:
            state = "PUBLISHED"
        elif ruling and ruling.get("decision") == "CLEARED":
            state = "DIRECTOR_CLEARED"
        elif ruling and ruling.get("decision") == "REJECTED":
            state = "REJECTED_BY_DIRECTOR"
        elif meta is not None:
            state = "META_REVIEWED"
        elif risk_v == RISK_ACCEPTABLE:
            state = "VALIDATED_SURVIVOR"
        elif risk_v == RISK_REJECTED:
            state = "REJECTED_BY_RISK"
        elif skeptic == SK_SURVIVED:
            state = "SKEPTIC_SURVIVOR"
        elif skeptic == SK_KILLED:
            state = "KILLED_BY_SKEPTIC"
        elif cand and cand.get("state") == "DATA_HOLD":
            state = "DATA_HOLD"
        elif cand:
            state = "CANDIDATE_SUBMITTED"
        elif halted:
            # A cell that stopped at D or V is SETTLED: it has an outcome, it
            # counts to the search burden, and it never reaches the skeptic
            # because the pipeline admits only a measured candidate. Reporting
            # it as PREREGISTERED would tell a reader nine experiments were
            # still pending when in fact nine had already answered.
            state = "HALTED_AT_%s" % halted
        else:
            state = "PREREGISTERED"
        tc = h.get("turnover_cost") or {}
        return {
            "EXPERIMENT_ID": hid,
            "AGENT": str(h.get("origin") or "").replace(ORIGIN_PREFIX, ""),
            "HYPOTHESIS": spec.get("hypothesis") or h.get("title"),
            "ASSET_CLASS": h.get("asset_class"),
            "FAMILY": h.get("economic_family"),
            "FEATURE_SET": spec.get("feature_set_id"),
            "HORIZON": h.get("horizon_sessions"),
            "PARAMETERS": spec.get("parameters"),
            "DISCOVERY_SAMPLE": spec.get("discovery_sample"),
            "EVALUATION_SAMPLE": spec.get("evaluation_sample"),
            "PIT_STATUS": spec.get("pit_status"),
            "TURNOVER": tc.get("turnover", (cand or {}).get("turnover")),
            "COST_MODEL": spec.get("cost_model"),
            "RESULT": {"outcome": h.get("outcome"),
                       "statistic": h.get("statistic"),
                       "reason": h.get("reason_rejected")},
            "SKEPTIC_VERDICT": skeptic,
            "RISK_VERDICT": risk_v,
            "SURVIVOR_STATE": state,
            "FORWARD_STATE": (fwd or {}).get("forward_state") or "NONE",
            # Appended AFTER the eighteen the release brief requires, so that
            # list stays exactly where and as it was. These three make the
            # sequential reveal legible: without them a cell that stopped at
            # discovery is indistinguishable from one that never started.
            "STAGES_REVEALED": revealed,
            "HALTED_AT": halted,
            "LOCKBOX_COMPUTED": "L" in revealed,
        }


_VERBS = {
    "certify_data": AgentPipeline.certify_data,
    "define_universe": AgentPipeline.define_universe,
    "publish_features": AgentPipeline.publish_features,
    "preregister": AgentPipeline.preregister,
    "reveal_stage": AgentPipeline.reveal_stage,
    "submit_candidate": AgentPipeline.submit_candidate,
    "skeptic_review": AgentPipeline.skeptic_review,
    "risk_review": AgentPipeline.risk_review,
    "meta_review": AgentPipeline.meta_review,
    "director_clear": AgentPipeline.director_clear,
    "publish_candidate": AgentPipeline.publish_candidate,
    "request_forward_registration":
        AgentPipeline.request_forward_registration,
}
