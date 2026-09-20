"""alpha_agent.agents_v2.contracts - read and VALIDATE the V2 agent contracts.

The seven JSON contracts under ``research/agents`` and the twelve definitions
under ``.claude/agents`` are the specification; ``pipeline`` is the enforcement.
This module is the reader both of them share, plus the consistency check that
keeps the specification honest: a roster that disagrees between two files, a
handoff edge that lets a signal agent skip the skeptic, or a definition that
quietly grants the Bash tool is a defect, and it is reported here rather than
discovered by an agent at run time.

Pure stdlib. Reads files; writes nothing.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Optional

from . import (AGENT_DEFINITION_DIR, AGENT_SYSTEM_VERSION, CONTRACT_DIR,
               CONTRACT_FILES, PUBLISHING, ROSTER, SIGNAL_AGENTS, SKEPTIC)
from . import routing as R

#: Sections every Paper-Trader-native agent definition must carry.
REQUIRED_SECTIONS = (
    "## Mission", "## When to invoke", "## Context contract",
    "## Allowed inputs",
    "## Required outputs", "## Prohibited actions", "## Validation gates",
    "## Handoff contract", "## Canonical owners",
    "## Failure-reporting requirements", "## PowerShell-only rule",
    "## No-hallucination rule", "## No-hidden-tuning rule",
    "## No-production/order/automation rule",
)

#: Phrases that belong to the RETIRED Phase 8-A contract. Their presence in a
#: V2 definition means a file was copied rather than ported.
RETIRED_PHRASES = (
    "No commit, no push", "No commit/push", "Phase 8A", "Phase 8-A",
    "No Paper Trader", "/Paper Trader/GCP", "forever read-only",
    "D:\\Stock_Prediction_app_data\\research_panels",
)

#: Verbs every signal agent owns, because they measure their own experiment.
SHARED_SIGNAL_VERBS = ("reveal_stage", "submit_candidate")

#: Downstream stages no signal agent may hand to directly.
_DOWNSTREAM_OF_SKEPTIC = ("risk-portfolio-agent", "meta-model-ensemble-agent",
                          PUBLISHING, "quant-research-director")

PUBLISHING_MAY_NOT = (
    "create operational orders", "create fills",
    "approve a portfolio proposal", "promote a champion automatically",
    "make a sleeve capital eligible by itself",
)


def _normalised_bytes(path: Path) -> bytes:
    """File bytes with CRLF folded to LF, so a hash survives ``core.autocrlf``."""
    return path.read_bytes().replace(b"\r\n", b"\n")


def contract_path(name: str, contract_dir: Optional[Path] = None) -> Path:
    return Path(contract_dir or CONTRACT_DIR) / name


def load_contract(name: str, contract_dir: Optional[Path] = None) -> dict:
    return json.loads(_normalised_bytes(
        contract_path(name, contract_dir)).decode("utf-8"))


def contract_hash(name: str, contract_dir: Optional[Path] = None) -> str:
    return hashlib.sha256(_normalised_bytes(
        contract_path(name, contract_dir))).hexdigest()


def load_all(contract_dir: Optional[Path] = None) -> dict:
    return {n: load_contract(n, contract_dir) for n in CONTRACT_FILES}


class Contracts:
    """The loaded contract set, with the three lookups the pipeline needs."""

    def __init__(self, contract_dir: Optional[Path] = None):
        self.contract_dir = Path(contract_dir or CONTRACT_DIR)
        self.docs = load_all(self.contract_dir)
        self.gate_schema_hash = contract_hash("validation_gate_schema.json",
                                              self.contract_dir)
        ac = self.docs["agent_contracts.json"]
        self._agents = {a["name"]: a for a in ac["agents"]}
        self._routing = ac["family_routing"]

    def verbs_for(self, agent: str) -> tuple:
        return tuple((self._agents.get(agent) or {}).get("pipeline_verbs", ()))

    def families_for(self, agent: str) -> tuple:
        return tuple(self._routing.get(agent, ())) + \
            tuple(self._routing.get("any_signal_agent", ()))

    def gate_schema(self) -> dict:
        return self.docs["validation_gate_schema.json"]

    def registry_schema(self) -> dict:
        return self.docs["experiment_registry_schema.json"]

    def required_adversarial_checks(self) -> tuple:
        return tuple(c["id"] for c in
                     self.gate_schema()["adversarial_checks"]["required"])


def parse_agent_definition(path: Path) -> dict:
    """``{frontmatter: {...}, body: str}`` for one ``.claude/agents`` file."""
    text = _normalised_bytes(Path(path)).decode("utf-8")
    front: dict = {}
    body = text
    if text.startswith("---\n"):
        end = text.find("\n---\n", 4)
        if end != -1:
            for line in text[4:end].splitlines():
                if ":" in line:
                    k, v = line.split(":", 1)
                    front[k.strip()] = v.strip()
            body = text[end + 5:]
    tools = tuple(t.strip() for t in front.get("tools", "").split(",")
                  if t.strip())
    return {"frontmatter": front, "tools": tools, "body": body,
            "path": str(path)}


def validate(contract_dir: Optional[Path] = None,
             definition_dir: Optional[Path] = None) -> list:
    """Every inconsistency in the specification. Empty means coherent."""
    cdir = Path(contract_dir or CONTRACT_DIR)
    ddir = Path(definition_dir or AGENT_DEFINITION_DIR)
    problems: list = []

    docs = {}
    for name in CONTRACT_FILES:
        p = cdir / name
        if not p.exists():
            problems.append("missing contract: %s" % name)
            continue
        try:
            docs[name] = load_contract(name, cdir)
        except ValueError as exc:
            problems.append("unparseable contract %s: %s" % (name, exc))
            continue
        if docs[name].get("agent_system_version") != AGENT_SYSTEM_VERSION:
            problems.append("%s does not declare %s"
                            % (name, AGENT_SYSTEM_VERSION))
    if len(docs) != len(CONTRACT_FILES):
        return problems

    roster = set(ROSTER)
    manifest_names = [a["name"] for a in docs["agent_manifest.json"]["agents"]]
    contract_names = [a["name"] for a in docs["agent_contracts.json"]["agents"]]
    if manifest_names != list(ROSTER):
        problems.append("agent_manifest roster/order differs from ROSTER")
    if contract_names != list(ROSTER):
        problems.append("agent_contracts roster/order differs from ROSTER")

    verbs = set(docs["agent_contracts.json"]["pipeline_verbs"])
    seen_verbs: dict = {}
    for a in docs["agent_contracts.json"]["agents"]:
        for v in a.get("pipeline_verbs", ()):
            if v not in verbs:
                problems.append("%s uses undeclared verb %s" % (a["name"], v))
            seen_verbs.setdefault(v, []).append(a["name"])
    for v in verbs:
        if v not in seen_verbs:
            problems.append("verb %s is owned by no agent" % v)
    for v, owners in seen_verbs.items():
        # The two MEASUREMENT verbs belong to exactly the four signal agents;
        # every other verb has exactly one owner. ``reveal_stage`` is shared
        # for the same reason ``submit_candidate`` is: the agent that measures
        # a layer is the agent that owns the experiment.
        if v in SHARED_SIGNAL_VERBS:
            if set(owners) != set(SIGNAL_AGENTS):
                problems.append("%s must belong to exactly the four signal "
                                "agents" % v)
        elif len(owners) != 1:
            problems.append("verb %s has %d owners" % (v, len(owners)))

    allowed_families = set(
        docs["research_director_protocol.json"]["families_allowed"])
    routed: set = set()
    for agent, fams in docs["agent_contracts.json"]["family_routing"].items():
        if agent != "any_signal_agent" and agent not in SIGNAL_AGENTS:
            problems.append("family routed to a non-signal agent: %s" % agent)
        routed.update(fams)
    if routed != allowed_families:
        problems.append("family_routing and families_allowed disagree: %s"
                        % sorted(routed ^ allowed_families))

    gates = set(docs["handoff_contracts.json"]["gates"])
    for e in docs["handoff_contracts.json"]["edges"]:
        if e["gate"] not in gates:
            problems.append("edge uses undeclared gate: %s" % e["gate"])
        if e["from"] not in roster:
            problems.append("edge from unknown agent: %s" % e["from"])
        if e["to"] not in roster and e["gate"] != "governed_forward_request":
            problems.append("edge to unknown agent: %s" % e["to"])
        if e["from"] in SIGNAL_AGENTS and e["to"] != SKEPTIC:
            problems.append("SKEPTIC BYPASS: %s -> %s" % (e["from"], e["to"]))

    gov = docs["governance_contract.json"]
    boundary = gov.get("signal_publishing_boundary") or {}
    for item in PUBLISHING_MAY_NOT:
        if item not in (boundary.get("may_not") or ()):
            problems.append("publishing boundary lost a prohibition: %s" % item)
    if "Bash" not in (gov.get("shell_policy") or {}).get("forbidden_tools", ()):
        problems.append("governance shell_policy does not forbid Bash")
    if len(gov.get("preserved_safety_principles") or ()) < 19:
        problems.append("governance contract lost a preserved safety principle")

    for name in ROSTER:
        p = ddir / ("%s.md" % name)
        if not p.exists():
            problems.append("missing agent definition: %s" % name)
            continue
        d = parse_agent_definition(p)
        if d["frontmatter"].get("name") != name:
            problems.append("%s: frontmatter name mismatch" % name)
        if "Bash" in d["tools"]:
            problems.append("%s: grants the Bash tool" % name)
        if "PowerShell" not in d["tools"]:
            problems.append("%s: does not grant PowerShell" % name)
        # A research agent may not launch another agent: the session
        # orchestrator is the only dispatcher, and a self-dispatching agent
        # would escape the spawn discipline entirely.
        for tool in R.DELEGATION_TOOLS:
            if tool in d["tools"]:
                problems.append("%s: grants the delegation tool %s"
                                % (name, tool))
        if AGENT_SYSTEM_VERSION not in d["body"]:
            problems.append("%s: does not name %s"
                            % (name, AGENT_SYSTEM_VERSION))
        for section in REQUIRED_SECTIONS:
            if section not in d["body"]:
                problems.append("%s: missing section %r" % (name, section))
        for phrase in RETIRED_PHRASES:
            if phrase in d["body"]:
                problems.append("%s: carries retired Phase 8-A text %r"
                                % (name, phrase))
    # The committed model/effort routing must BE the routing Claude Code
    # loads. ``routing`` owns the policy; this is where a drifted definition
    # is caught.
    problems.extend(R.routing_problems(ddir))
    return problems
