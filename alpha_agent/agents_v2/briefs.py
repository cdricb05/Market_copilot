r"""alpha_agent.agents_v2.briefs - the CONTEXT CONTRACT for each of the twelve.

RESEARCH ONLY. PAPER ONLY. NO ORDERS, NO FILLS, NO PROMOTION, NO ADOPTION.

What a brief is
---------------
A brief is everything a role needs to make its decision, and nothing else. It
is a PROJECTION - a read model - over state that is already owned elsewhere:

    hypothesis identity, outcome, burden, graveyard   alpha_agent.r59.memory
    the ONE statistical verdict                       alpha_agent.r59.engines.gate
    the search-burden denominator                     alpha_agent.r59.handlers
    handoff legality                                  alpha_agent.agents_v2.pipeline
    measured layers and adversarial attacks           alpha_agent.agents_v2.runner
    the settled estate census                         research/agents/NEXT_CAMPAIGN_CENSUS.json

Nothing here is a second truth source. Every number a brief carries was
computed by the owner named beside it, and a brief that cannot find its owner
says so rather than inventing the number.

Why briefs exist
----------------
Before this module, a role discovered its own context: the director re-read
the charter, the frontier, the sleeve contract and a twenty-kilobyte census;
each signal agent received the whole twenty-six-kilobyte campaign agenda to
find the two experiments assigned to it; and the skeptic received a result
bundle that invited it to re-narrate placebo, doubled cost and subperiod tests
that ``runner.adversarial_pack`` had already measured.

A brief replaces the discovery. It is ids, numbers, booleans, reason codes and
artifact paths, with a prose budget - ``MAX_HANDOFF_PROSE_WORDS`` - that
``brief_problems`` enforces.

The handoff envelope
--------------------
Every brief carries the same envelope, so a reader never has to learn a new
shape: RUN_ID, CAMPAIGN_ID, EXPERIMENT_ID, SOURCE_AGENT, TARGET_AGENT,
DECISION_REQUIRED, FACTS, METRICS, FAILED_GATES, ARTIFACT_POINTERS,
SAFETY_STATE.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Optional

from .. import r59
from ..r59 import engines as E
from ..r59 import handlers as H
from ..r61 import drawdown as DD
from . import (AGENT_SYSTEM_VERSION, CONTRACT_DIR, DATA_FOUNDATION, DIRECTOR,
               FEATURES, GENERATION_METHOD, META, PUBLISHING, RISK, SAFETY,
               SIGNAL_AGENTS, SKEPTIC, UNIVERSE)
from . import routing as RT
from .pipeline import (EV_CANDIDATE, EV_DATA, EV_DIRECTOR, EV_FEATURES,
                       EV_META, EV_RISK, EV_SKEPTIC, EV_STAGE, EV_UNIVERSE,
                       RISK_ACCEPTABLE, SK_SURVIVED)

BRIEF_OWNER = "alpha_agent.agents_v2.briefs"
BRIEF_VERSION = "R58_AGENT_CONTEXT_CONTRACT_V1"

#: A handoff is ids, numbers and reason codes. Prose is for the one thing a
#: number cannot carry - why a mechanism should work - and it is budgeted.
MAX_HANDOFF_PROSE_WORDS = 500

#: The census is the settled-estate projection the director already owns. A
#: brief POINTS at it and lifts the few compact fields the director rules on;
#: it never copies the file into a prompt.
CENSUS_FILE = "NEXT_CAMPAIGN_CENSUS.json"

#: R79 - HOW a queued proposal is already settled, if it is. Three states,
#: because they need three different director actions: a RULED mechanism must be
#: overturned on the record, a mechanism settled BY MEASUREMENT must clear its
#: own reopen condition, and only the third is genuinely open work. The estate
#: previously published one boolean about the first and nothing about the second,
#: so a proposal settled by measurement was indistinguishable from a new one -
#: which is how R78 re-commissioned a mechanism that already carried a verdict.
SETTLED_RULED = "RULED_BY_THE_DIRECTOR"
SETTLED_BY_MEASUREMENT = "SETTLED_BY_MEASUREMENT_WITHOUT_A_RULING"
SETTLED_NOT = "NOT_SETTLED_IN_CANONICAL_MEMORY"
SETTLED_STATES = (SETTLED_RULED, SETTLED_BY_MEASUREMENT, SETTLED_NOT)


# --------------------------------------------------------------------------- #
# Envelope
# --------------------------------------------------------------------------- #
def envelope(*, run_id: str, campaign_id: str, source: str, target: str,
             decision: str, facts: Optional[dict] = None,
             metrics: Optional[dict] = None,
             failed_gates: Optional[list] = None,
             pointers: Optional[dict] = None,
             experiment_id: str = "") -> dict:
    """The one handoff shape. Terse by construction."""
    return {
        "RUN_ID": run_id,
        "CAMPAIGN_ID": campaign_id,
        "EXPERIMENT_ID": experiment_id,
        "SOURCE_AGENT": source,
        "TARGET_AGENT": target,
        "DECISION_REQUIRED": decision,
        "FACTS": facts or {},
        "METRICS": metrics or {},
        "FAILED_GATES": list(failed_gates or ()),
        "ARTIFACT_POINTERS": pointers or {},
        "SAFETY_STATE": dict(SAFETY),
        "AGENT_SYSTEM_VERSION": AGENT_SYSTEM_VERSION,
        "BRIEF_OWNER": BRIEF_OWNER,
        "BRIEF_VERSION": BRIEF_VERSION,
        # Stated in the role's OWN context so it does not recompute what a
        # deterministic owner already settled.
        "DO_NOT_RECOMPUTE": dict(RT.DETERMINISTIC_LOCAL_OWNERS),
        "MODEL_ROUTING": {"model": RT.ROUTING[target]["model"],
                          "effort": RT.ROUTING[target]["effort"],
                          "tier": RT.ROUTING[target]["tier"]},
    }


def prose_words(obj: Any) -> int:
    """Words in every string value of a brief. The budgeted quantity."""
    if isinstance(obj, str):
        return len(obj.split())
    if isinstance(obj, dict):
        return sum(prose_words(v) for v in obj.values())
    if isinstance(obj, (list, tuple)):
        return sum(prose_words(v) for v in obj)
    return 0


ENVELOPE_KEYS = ("RUN_ID", "CAMPAIGN_ID", "EXPERIMENT_ID", "SOURCE_AGENT",
                 "TARGET_AGENT", "DECISION_REQUIRED", "FACTS", "METRICS",
                 "FAILED_GATES", "ARTIFACT_POINTERS", "SAFETY_STATE")


def brief_problems(brief: dict,
                   max_words: int = MAX_HANDOFF_PROSE_WORDS) -> list:
    """Every way a brief breaks the handoff contract. Empty means compliant."""
    problems = []
    for key in ENVELOPE_KEYS:
        if key not in brief:
            problems.append("handoff is missing %s" % key)
    words = prose_words(brief)
    if words > max_words:
        problems.append("handoff prose is %d words, budget is %d"
                        % (words, max_words))
    safety = brief.get("SAFETY_STATE") or {}
    for flag in ("creates_orders", "creates_fills", "promotes_model",
                 "approves_proposal", "automation_enabled"):
        if safety.get(flag) is not False:
            problems.append("SAFETY_STATE.%s is not False" % flag)
    return problems


# --------------------------------------------------------------------------- #
# The campaign projection
# --------------------------------------------------------------------------- #
def _experiment_ids(spec: Optional[dict]) -> list:
    if not spec:
        return []
    return [r["experiment_id"] for r in spec.get("experiments") or ()]


#: What a campaign spec calls the substrate it needs, and which event answers
#: it. A campaign that names nothing keeps the estate-wide answer.
REQUIRES_KEYS = (("datasets", "data_certified"),
                 ("universes", "universe_defined"),
                 ("feature_sets", "features_published"))


def _required(spec: Optional[dict], key: str) -> list:
    req = ((spec or {}).get("requires") or {}).get(key) or ()
    return [str(x) for x in req]


def _foundation_done(pipe, spec: Optional[dict], event: str,
                     key: str) -> bool:
    """Is the substrate THIS campaign needs already in the memory?

    The other facts in ``campaign_state`` are scoped to the campaign's own
    experiment ids; these three were not, so once ANY dataset had ever been
    certified the spawn gate skipped the data-foundation agent for every
    campaign that followed - including one whose entire premise is data the
    estate has never certified. ``preregister`` still refuses without the
    feature set -> universe -> PIT_SAFE dataset chain, so the campaign would
    have been blocked at write time by the agent the gate would not spawn.

    A spec therefore DECLARES what it requires:

        "requires": {"datasets": [...], "universes": [...],
                     "feature_sets": [...]}

    and the role is skipped only when every declared id is already recorded.
    A spec that declares nothing keeps the estate-wide answer, so every
    existing campaign spec reads exactly as it did before.
    """
    need = _required(spec, key)
    if not need:
        return bool(pipe._events(event))
    return all(pipe._latest(event, i) is not None for i in need)


def campaign_state(pipe, campaign_id: str,
                   spec: Optional[dict] = None) -> dict:
    """The compact state a spawn decision and a resume are made from.

    Read-only. Every fact comes from the ONE research memory through the
    pipeline that owns it.
    """
    ids = _experiment_ids(spec)
    rows = {}
    for eid in ids:
        row = pipe.mem.get(eid)
        if row is not None:
            rows[eid] = row

    candidates = {e["subject"]: e["detail"]
                  for e in pipe._events(EV_CANDIDATE) if e["subject"] in rows}
    skeptic = {e["subject"]: e["detail"]
               for e in pipe._events(EV_SKEPTIC) if e["subject"] in rows}
    risk = {e["subject"]: e["detail"]
            for e in pipe._events(EV_RISK) if e["subject"] in rows}
    meta = {e["subject"]: e["detail"]
            for e in pipe._events(EV_META) if e["subject"] in rows}
    director = {e["subject"]: e["detail"]
                for e in pipe._events(EV_DIRECTOR) if e["subject"] in rows}

    assigned: dict = {}
    unmeasured: dict = {}
    halted = 0
    for eid, row in rows.items():
        owner = (row.get("spec") or {}).get("owning_agent") or ""
        if owner:
            assigned[owner] = assigned.get(owner, 0) + 1
        measured = eid in candidates
        if not measured and not row.get("outcome"):
            unmeasured[owner] = unmeasured.get(owner, 0) + 1
        if not measured and row.get("outcome"):
            halted += 1

    awaiting_skeptic = [e for e in candidates
                        if candidates[e].get("state") == "MEASURED"
                        and e not in skeptic]
    survivors = [e for e, d in skeptic.items()
                 if d.get("verdict") == SK_SURVIVED]
    awaiting_risk = [e for e in survivors if e not in risk]
    validated = [e for e in survivors
                 if (risk.get(e) or {}).get("verdict") == RISK_ACCEPTABLE]
    cleared = [e for e, d in director.items() if d.get("decision") == "CLEARED"]

    return {
        "campaign_id": campaign_id,
        "experiments_preregistered": len(rows),
        "experiments_in_spec": len(ids),
        "assigned_by_agent": assigned,
        "unmeasured_by_agent": unmeasured,
        "measured": sorted(candidates),
        "halted": halted,
        "awaiting_skeptic": sorted(awaiting_skeptic),
        "skeptic_survivors": sorted(survivors),
        "awaiting_risk": sorted(awaiting_risk),
        "validated_survivors": sorted(validated),
        "meta_reviewed": sorted(meta),
        "director_cleared": sorted(cleared),
        "data_certified": _foundation_done(pipe, spec, EV_DATA,
                                           "datasets"),
        "universe_defined": _foundation_done(pipe, spec, EV_UNIVERSE,
                                             "universes"),
        "features_published": _foundation_done(pipe, spec, EV_FEATURES,
                                               "feature_sets"),
        "substrate_required": {k: _required(spec, k)
                               for k, _ in REQUIRES_KEYS},
        "substrate_missing": {
            k: [i for i in _required(spec, k)
                if pipe._latest(ev, i) is None]
            for k, ev in (("datasets", EV_DATA),
                          ("universes", EV_UNIVERSE),
                          ("feature_sets", EV_FEATURES))},
    }


# --------------------------------------------------------------------------- #
# Director: the compact research state (Workstream F)
# --------------------------------------------------------------------------- #
#: How many dataset rows the director's brief inlines. The census's dataset
#: list GROWS every time a campaign certifies something - R60 alone added four
#: - so a brief that inlined all of them had an unbounded prose budget and
#: would breach the handoff contract on some future campaign for no reason
#: anyone would connect to the cause. The brief carries the most decision-
#: relevant rows and a pointer to the rest; the census is on disk and the
#: director already holds its path in ARTIFACT_POINTERS.
MAX_BRIEF_DATASETS = 12

#: A dataset the director can still DO something with sorts before one that is
#: closed, mined out or already spoken for. Matched case-insensitively against
#: the row's ``state``.
_SPENT_MARKERS = ("heavily mined", "closed", "exhausted", "prospective-only")


def _dataset_rank(row: dict) -> tuple:
    state = str(row.get("state") or "").lower()
    spent = any(m in state for m in _SPENT_MARKERS)
    unused = ("unused" in state or "never" in state or "0 hypotheses" in state
              or "free" in state)
    return (1 if spent else 0, 0 if unused else 1)


def _dataset_digest(pipe, census: dict) -> dict:
    """What the estate can actually research on, asked of the CERTIFICATION store.

    R80 - THE SECOND FROZEN CENSUS BLOCK, AND IT FAILS EXACTLY AS
    ``best_domains`` DID. ``available_pit_datasets`` is a snapshot taken on the
    census date and copied verbatim into every director brief since. Three of its
    rows are now falsified by the estate's own later certifications:

    * "SEC EDGAR full index (UPLOAD / CORRESP comment letters) - FREE, never
      used" is ``CERTIFICATION: DATA_HOLD``
      (campaign_r60_information_frontier/CERTIFICATION_DF4B_COMMENT_LETTERS).
    * "EODHD dividend declarations - PIT_SAFE_R60" closed
      ``DATA_HOLD_ON_A_PRE_DECLARED_KILL_CONDITION`` with purchase-gate state
      COVERAGE_BLOCKED (campaign_r78_dividend_announcement).
    * "Nasdaq trading halts - FREE_AVAILABLE, 0 hypotheses" failed its
      certification: the feed is live tradability, not an archive, so it has no
      historical depth to research on.

    A director handed those three as FACTS reads three free, unused lanes that do
    not exist, and this estate has already paid for that once: R76 read the frozen
    NARRATIVE, wrote a refused cell up as available, and R77 spent a campaign
    establishing it. The narrative was de-frozen; its sibling was not.

    So the availability claim is no longer taken from the census. The live answer
    is ``mem.provider_usage()`` - the certification store, which is where a
    coverage verdict is actually recorded and which carries a CLASSIFICATION per
    provider and data class. The census rows are still listed, because they name
    owned panels the certification store does not itself enumerate, but they are
    labelled as the frozen claim they are and the classification travels beside
    them. Nothing is filtered out and no verdict is synthesised: a dataset the
    certification store has never heard of says so.

    Ordering of the census rows is STABLE (a sort by rank alone, preserving census
    order within a rank), so two runs against the same census agree.
    """
    certified: list = []
    try:
        usage = list(pipe.mem.provider_usage() or ())
    except Exception:                                        # noqa: BLE001
        usage = []
    for row in usage:
        cov = row.get("coverage")
        cls = (cov or {}).get("classification") if isinstance(cov, dict) else None
        # ONE unspaced pipe token per certified data class, for the same
        # handoff-budget reason ``ruled_economic_families`` is one.
        certified.append("|".join(str(x or "?") for x in (
            row.get("provider"), row.get("data_class"), cls or "UNCLASSIFIED")))

    rows = list(census.get("available_pit_datasets") or ())
    ranked = sorted(rows, key=_dataset_rank)
    return {
        "certified_data_classes": sorted(set(certified)),
        "n_certified_data_classes": len(set(certified)),
        "certification_owner": "mem.provider_usage()",
        "census_rows_are_a_frozen_claim_not_a_certification": True,
        "census_rows_include_claims_later_certifications_falsified": True,
        "census_claims": [
            {"dataset": d.get("dataset"), "asset_class": d.get("asset_class"),
             "claimed_state": d.get("state")}
            for d in ranked[:MAX_BRIEF_DATASETS]],
        "verify_any_availability_claim_against_the_certification_store": True,
    }


#: The fixed field order inside the ``refused_by`` token. One string, not five,
#: because the handoff budget counts words in string values.
REFUSED_BY_FIELDS = "campaign_id|verdict|blocker_reason|ruling_scope|reopen"

#: A reopen condition longer than this (characters), or containing whitespace,
#: is rendered as ``<leading identifier>@<ruling-store key>`` instead of verbatim.
MAX_INLINE_REOPEN_CHARS = 96
#: The leading identifier of a compacted reopen condition is capped at this.
MAX_REOPEN_ID_CHARS = 48
#: Where the full text of a compacted reopen condition is read.
REOPEN_OWNER = "mem.director_rulings()"


def _reopen_token(ruling: dict) -> str:
    """The reopen condition as ONE whitespace-free token the budget can afford.

    R86 - TWO R81 RULINGS STORE A 279-WORD PROSE REOPEN CONDITION, and both the
    ``refused_by`` join and ``ruled_economic_families`` appended it verbatim
    after ``reopen=``. The director brief came to 902 words against the 500-word
    handoff contract and ``agents_v2_brief.py`` printed BRIEF_REFUSED: the ruling
    store made the director unbriefable, the same class as the R79/R80 overflows.

    A condition that is already a short code is returned unchanged, so no
    existing ruling loses meaning. A long one is rendered as its leading
    identifier (the text before its first sentence break) plus the primary key
    of the ruling that holds the full text - ``director_rulings[asset_class/
    economic_family/information_family/model_family]`` - which the director
    resolves through ``mem.director_rulings()``. The stored ruling is only
    read, never shortened.
    """
    text = str(ruling.get("reopen_condition") or "").strip()
    if not text:
        return "?"
    if len(text) <= MAX_INLINE_REOPEN_CHARS and not re.search(r"\s", text):
        return text
    lead = re.split(r"\.\s|:\s|;\s|\n", text, maxsplit=1)[0]
    ident = re.sub(r"[^A-Za-z0-9_\-]+", "_", lead).strip("_")
    ident = ident[:MAX_REOPEN_ID_CHARS].rstrip("_") or "REOPEN"
    key = "/".join(str(ruling.get(k) or "*") for k in (
        "asset_class", "economic_family", "information_family",
        "model_family"))
    return "%s@director_rulings[%s]" % (ident, re.sub(r"\s+", "_", key))


def _queued_with_rulings(pipe, census: dict) -> list:
    """Queued census proposals, each joined to the ruling that already covers it.

    THE DEFECT THIS ANSWERS. ``director_brief`` projected six scalar fields per
    queued hypothesis and consulted neither ``mem.director_rulings()`` nor the
    census's own ``R68_MECHANISM_CHECK`` block. The brief was therefore strictly
    LESS informative about novelty than the census file it was built from, and a
    director reading it could not see that the proposal in front of him had been
    refused by rank in an earlier campaign. R73 refused census ranks 9 and 12 by
    name; R71 refused rank 10 by name; both were re-commissioned afterwards.

    The join is by (asset_class, economic_family), which is the scope a
    family-wide ruling is recorded at. A wildcard ``information_family`` ('*')
    ruling covers every information family in that economic family, so it matches
    too. Nothing is filtered OUT: a refused proposal stays visible, carrying its
    refusal and its reopen condition, because the director alone rules and needs
    to see what he is being asked to overturn.

    R79 - THE SECOND HALF OF THE SAME DEFECT: A RULING IS NOT THE ONLY WAY A
    MECHANISM GETS SETTLED. R78 re-commissioned a mechanism this estate had
    already prosecuted, and every check passed while it did. The reason is a
    seam, not an oversight:

    * the census is a FROZEN artifact (2026-09-19) and the mechanism was settled
      on 2026-09-21, so the census still lists it as queued;
    * its rows carry ``asset_class`` and ``family`` but no ``information_family``,
      so the canonical four-part mechanism key cannot be reconstructed from them;
    * it was settled by MEASUREMENT - a recorded DATA_HOLD - and not by a
      director ruling, so the ruling join above found nothing;
    * therefore ``already_ruled`` came back False, which a director reasonably
      read as "never tested".

    ``already_ruled: false`` never meant "novel" and now cannot be mistaken for
    it. Each proposal is additionally asked of the CANONICAL mechanism-state
    owner (:meth:`alpha_agent.r59.memory.ResearchMemory.mechanism_state`, built by
    R68 for exactly this question and never wired into this brief), bound on the
    two components the census rows actually carry - a partial bind that owner
    documents as a valid question with a real answer. The derived
    ``settled_state`` is the one field a director should read, and it separates
    RULED from SETTLED_BY_MEASUREMENT from NOT_SETTLED, so a stale census can no
    longer present a prosecuted mechanism as an open one.

    No second novelty database: the ruling store and research memory are both
    existing canonical owners, read live, and neither is written here.
    """
    rulings: list = []
    try:
        rulings = list(pipe.mem.director_rulings() or ())
    except Exception:                                        # noqa: BLE001
        rulings = []
    out: list = []
    for q in (census.get("queued_hypotheses") or ()):
        ac = q.get("asset_class")
        fam = q.get("family")
        covering = [r for r in rulings
                    if r.get("asset_class") == ac
                    and r.get("economic_family") == fam]
        row = {"rank": q.get("rank"), "proposal": q.get("proposal"),
               "agent": q.get("agent"), "asset_class": ac,
               "family": fam,
               "horizon_sessions": q.get("horizon_sessions")}
        # The LIVE canonical mechanism state, not the frozen census's claim.
        mech: dict = {}
        try:
            mech = pipe.mem.mechanism_state(asset_class=ac,
                                            economic_family=fam) or {}
        except Exception:                                    # noqa: BLE001
            mech = {}
        settled_by_measurement = bool(mech.get("mechanism_is_settled"))
        row["mechanism_settled"] = settled_by_measurement
        row["mechanism_settled_rows"] = int(mech.get("n_settled") or 0)
        if settled_by_measurement:
            # ONE unspaced pipe token, for the same handoff-budget reason
            # ``refused_by`` below is one: ``prose_words`` counts string values by
            # whitespace, so the outcome mix costs one word instead of several.
            row["settled_by"] = "|".join(
                "%s=%s" % (k, v) for k, v in
                sorted((mech.get("outcomes") or {}).items()))
        # The ONE field a director should read. ``already_ruled`` is retained
        # because established callers and tests read it, but it answers a
        # narrower question than the one that matters.
        row["settled_state"] = (
            SETTLED_RULED if covering
            else SETTLED_BY_MEASUREMENT if settled_by_measurement
            else SETTLED_NOT)
        # The census's R68_MECHANISM_CHECK block is deliberately NOT copied. It is
        # a static snapshot of a check the director must run live anyway, its
        # verdict is prose where the ruling store's is a code, and the census file
        # is already an ARTIFACT_POINTER. ``already_ruled`` below is the fact that
        # actually changes a decision, and it is carried for every proposal.
        mc = q.get("R68_MECHANISM_CHECK") or {}
        if mc and not covering:
            row["census_novelty_claim_is_false"] = mc.get(
                "the_novelty_claim_above_is_false")
        if covering:
            # ONE binding ruling per proposal, codes only, never the rationale.
            # The handoff contract budgets this brief at 500 words: a rationale
            # excerpt per ruling took it to 1007, and every covering ruling took it
            # to 609. Handing a role more than it needs is the same failure the
            # budget exists to prevent. The binding ruling is the NARROWEST scope,
            # newest first - a MECHANISM-scoped refusal is more specific than the
            # family-wide one it sits under, and it is the one a director would
            # have to overturn. ``verdict``/``blocker_reason`` are the ruling
            # table's own column names; an earlier draft read a non-existent
            # ``decision`` key and every refusal rendered None.
            binding = sorted(
                covering,
                key=lambda r: (0 if r.get("ruling_scope") == "MECHANISM" else 1,
                               str(r.get("updated_at") or "")))[0]
            row["already_ruled"] = True
            # ONE pipe-delimited token, deliberately unspaced. ``prose_words``
            # budgets STRING VALUES by whitespace, so five separate code fields
            # per proposal cost five words and this costs one - and the five codes
            # are all still there, in a fixed order, machine-splittable on "|".
            # That is what makes the refusal affordable inside the 500-word
            # handoff budget the contract enforces.
            row["refused_by"] = "|".join(str(x or "?") for x in (
                binding.get("campaign_id"), binding.get("verdict"),
                binding.get("blocker_reason"), binding.get("ruling_scope"),
                "reopen=" + _reopen_token(binding)))
            row["covering_ruling_count"] = len(covering)
        else:
            row["already_ruled"] = False
        out.append(row)
    return out


def _census(contract_dir: Optional[Path] = None) -> dict:
    p = Path(contract_dir or CONTRACT_DIR) / CENSUS_FILE
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8-sig"))
    except ValueError:
        return {}


def _open_information_families(pipe, census: dict) -> dict:
    """Which economic families are still open, asked of the RULING STORE.

    R80 - THE NARRATIVE WAS THE LAST UNJOINED PROJECTION, AND IT WAS THE ONE THE
    DIRECTOR READ FIRST. ``census['best_domains']`` is four paragraphs of frozen
    prose that were copied verbatim into every director brief since R60. Two of
    the four recommend cells this estate has since REFUSED by durable ruling:
    ``best_h1_h5_domain`` recommends "H1-H5 reversal conditioned on never-used
    non-price information (FINRA short-volume share)", and ``best_cross_asset_
    domain`` recommends CROSS_ASSET_RELATIVE_VALUE "first", then
    CROSS_ASSET_LEAD_LAG. All three are ruled REFUSED, and the FINRA one is
    refused by a reopen condition that names FINRA short volume and excludes it.

    R79 joined the rulings into ``queued_hypotheses``, one row per proposal - and
    that join works. But the director reads the agenda narrative at the TOP of
    FACTS and the per-proposal refusal at row 9, 10 and 12 of a 13-row table. R76
    read the narrative, wrote rank 12 up as available, and R77 was commissioned
    on two already-refused cells and spent a whole campaign establishing it.

    So the prose is no longer copied. The same reasoning the R79 author recorded
    for the census's ``R68_MECHANISM_CHECK`` block applies to it word for word: it
    is a static snapshot of a question the director must ask live anyway, its
    answer is prose where the ruling store's is a code, and the census file is
    already an ARTIFACT_POINTER the director may read in full. What replaces it is
    the live answer - every economic family the ruling store has closed, as codes.

    This also brings the brief back INSIDE its own handoff contract. The R79 join
    added ``refused_by`` to thirteen queued rows and took the live director brief
    to 504 words against ``MAX_HANDOFF_PROSE_WORDS`` = 500, so
    ``brief_problems()`` refused it and ``agents_v2_brief.py --role
    quant-research-director`` printed BRIEF_REFUSED and wrote no file: the fix for
    the re-commissioning defect had made the director unbriefable. The fixture
    store in the regression has no rulings, so the join added nothing there and
    the test passed. Dropping 189 words of frozen prose to publish the live codes
    costs the director nothing he should have been trusting and pays for the
    refusals outright.
    """
    ruled: list = []
    try:
        rulings = list(pipe.mem.director_rulings() or ())
    except Exception:                                        # noqa: BLE001
        rulings = []
    for r in rulings:
        # ONE unspaced pipe token per closed family, for the same handoff-budget
        # reason ``refused_by`` is one: ``prose_words`` counts string values by
        # whitespace, so a family's four codes cost one word instead of four.
        ruled.append("|".join(str(x or "?") for x in (
            r.get("asset_class"), r.get("economic_family"),
            r.get("verdict"), r.get("blocker_reason"),
            "reopen=" + _reopen_token(r))))
    return {
        # A long reopen condition is an identifier@key pointer; the full text
        # is read from its owner, never copied (see ``_reopen_token``).
        "reopen_condition_owner": REOPEN_OWNER,
        # The prose stays in the census, which is an ARTIFACT_POINTER already.
        "narrative_pointer": "%s#best_domains" % CENSUS_FILE,
        "narrative_is_frozen_prose_not_live_state": True,
        "narrative_recommends_families_that_are_now_ruled": True,
        "ruled_economic_families": sorted(set(ruled)),
        "n_ruled_economic_families": len(set(ruled)),
        "read_the_ruling_store_before_believing_any_availability_claim": True,
    }


def director_brief(pipe, *, run_id: str, campaign_id: str,
                   spec: Optional[dict] = None,
                   contract_dir: Optional[Path] = None) -> dict:
    """What the director needs to set an agenda - and NOT the whole estate.

    The director used to re-read the charter, the frontier document, the
    sleeve contract and the census in full. It needs five things: what is
    closed, what is open, what it costs to try, what is already running
    forward, and what happened last time. Everything else is a POINTER.
    """
    census = _census(contract_dir)
    summary = pipe.mem.summary()
    burden = pipe.mem.burden()
    state = campaign_state(pipe, campaign_id, spec)
    must = census.get("must_not_repeat") or {}
    ledger = must.get("closed_mechanism_ledger") or []

    env = envelope(
        run_id=run_id, campaign_id=campaign_id,
        source="SESSION_ORCHESTRATOR", target=DIRECTOR,
        decision=("Set the campaign agenda: which hypotheses to pre-register, "
                  "to which signal agent, at what budget. Rule the tournament "
                  "on return."),
        facts={
            "open_information_families": _open_information_families(
                pipe, census),
            "exhausted_families": {
                "closed_mechanisms": len(ledger),
                "ledger_pointer": "%s#must_not_repeat.closed_mechanism_ledger"
                                  % CENSUS_FILE,
                "structural_rule": must.get("rule") or "",
            },
            "available_datasets": _dataset_digest(pipe, census),
            "available_datasets_pointer":
                "%s#available_pit_datasets" % CENSUS_FILE,
            "blocked_datasets": (census.get("must_not_repeat") or {}).get(
                "human_gated_not_for_the_agents") or [],
            # R77 - each queued proposal now carries the STANDING REFUSAL that
            # already covers it, if there is one. Before this the projection kept
            # rank/proposal/agent/asset_class/family/horizon and nothing else: it
            # dropped the census's own R68_MECHANISM_CHECK verdict AND never read
            # the ruling store, so a proposal a predecessor had refused BY RANK
            # reached the next director looking open. Ranks 9, 10 and 12 were
            # refused by name in R71 and R73 and were re-commissioned twice after
            # that, costing two whole campaigns. See _queued_with_rulings.
            "queued_hypotheses": _queued_with_rulings(pipe, census),
            "current_campaign": state,
        },
        metrics={
            "hypotheses_settled": summary.get("settled"),
            "qualified_survivors": summary.get("qualified"),
            "search_burden_total": (burden or {}).get("total"),
            "burden_by_asset_class": (census.get("estate_facts") or {}).get(
                "burden_by_asset_class") or {},
            "honest_prior": "0 of %s hypotheses have qualified"
                            % (summary.get("settled")),
        },
        pointers={
            "census": str(Path(contract_dir or CONTRACT_DIR) / CENSUS_FILE),
            "standing_rulings": "mem.director_rulings(asset_class,economic_family)",
            "refused_by_field_order": REFUSED_BY_FIELDS,
            "charter": "docs/PROJECT_CHARTER.md",
            "frontier": "docs/PNL_OPPORTUNITY_FRONTIER.md",
            "sleeve_contract": "docs/STRATEGY_SLEEVE_CONTRACT.md",
            "protocol": "research/agents/research_director_protocol.json",
        })
    env["FULL_MEMORY_DUMP_REQUIRED"] = False
    env["FULL_CENSUS_READ_REQUIRED"] = False
    return env


# --------------------------------------------------------------------------- #
# Foundation roles: mechanical preparation
# --------------------------------------------------------------------------- #
def foundation_brief(pipe, *, run_id: str, campaign_id: str, target: str,
                     spec: Optional[dict] = None) -> dict:
    """data / universe / feature: a contract check, not a research question."""
    state = campaign_state(pipe, campaign_id, spec)
    decisions = {
        DATA_FOUNDATION: ("Certify each named dataset PIT_SAFE or "
                          "NOT_PIT_SAFE and state the availability rule."),
        UNIVERSE: ("Declare the point-in-time universe over the certified "
                   "dataset and its execution representation."),
        FEATURES: ("Publish the versioned leak-safe feature set and the lag "
                   "and availability instant of every feature."),
    }
    agenda = spec or {}
    keys = {DATA_FOUNDATION: "datasets", UNIVERSE: "universes",
            FEATURES: "feature_sets"}
    return envelope(
        run_id=run_id, campaign_id=campaign_id, source=DIRECTOR, target=target,
        decision=decisions[target],
        facts={"items": agenda.get(keys[target]) or [],
               "already_satisfied": state.get(
                   {DATA_FOUNDATION: "data_certified",
                    UNIVERSE: "universe_defined",
                    FEATURES: "features_published"}[target])},
        pointers={"campaign_spec": agenda.get("campaign_id") or campaign_id,
                  "schema": "research/agents/agent_contracts.json"})


# --------------------------------------------------------------------------- #
# Signal agents: only THEIR experiments
# --------------------------------------------------------------------------- #
def signal_brief(pipe, *, run_id: str, campaign_id: str, target: str,
                 spec: Optional[dict] = None) -> dict:
    """One signal agent's OWN assignment. Not the whole campaign agenda."""
    if target not in SIGNAL_AGENTS:
        raise KeyError("%s is not a signal agent" % target)
    mine = []
    for eid in _experiment_ids(spec):
        row = pipe.mem.get(eid)
        if row is None:
            continue
        s = row.get("spec") or {}
        if s.get("owning_agent") != target:
            continue
        mine.append({
            "experiment_id": eid,
            "hypothesis": s.get("hypothesis") or "",
            "asset_class": row.get("asset_class"),
            "economic_family": row.get("economic_family"),
            "horizon_sessions": row.get("horizon_sessions"),
            "expected_sign": s.get("expected_sign"),
            "already_settled": bool(row.get("outcome")),
            "outcome": row.get("outcome"),
        })
    return envelope(
        run_id=run_id, campaign_id=campaign_id, source=DIRECTOR, target=target,
        decision=("Review the measured result of each experiment assigned to "
                  "you and state whether the economic mechanism is intact. "
                  "The local runner measures; you interpret."),
        facts={"assigned_experiments": mine,
               "unmeasured": sum(1 for m in mine if not m["already_settled"])},
        pointers={"runner": "scripts/run_agents_v2_campaign.py",
                  "results": "research/agents/%s/results.json" % campaign_id})


# --------------------------------------------------------------------------- #
# Skeptic: ONE compact review bundle (Workstream E)
# --------------------------------------------------------------------------- #
#: The five questions the skeptic is for. Everything else on its desk was
#: settled by a deterministic owner before it was spawned.
SKEPTIC_QUESTIONS = (
    "Is there a hidden research flaw not captured mechanically?",
    "Is the economic mechanism credible?",
    "Is performance overly dependent on one instrument, regime or group?",
    "Is there evidence of disguised duplication or a factor proxy?",
    "PASS or KILL, with a concise reason.",
)


def _attack(result: dict, name: str) -> dict:
    a = ((result.get("adversarial") or {}).get(name) or {})
    return {"passed": a.get("passed"), "measured": a.get("measured"),
            "evidence": a.get("evidence") or ""}


def skeptic_brief(pipe, *, run_id: str, campaign_id: str, experiment_id: str,
                  result: Optional[dict] = None) -> dict:
    """Everything the skeptic grades, already measured, in one bundle.

    The machine tests are REPORTED, never re-requested: placebo, doubled cost
    and subperiod stability were measured by ``runner.adversarial_pack``, and
    the statistical verdict belongs to ``r59.engines.gate``, reached with the
    burden ``r59.handlers.search_denominator`` charges. The skeptic answers
    the five questions a machine cannot.
    """
    row = pipe.mem.get(experiment_id) or {}
    spec = row.get("spec") or {}
    cand = pipe._latest(EV_CANDIDATE, experiment_id) or {}
    layers = cand.get("layers") or {}
    if result is None:
        result = {}

    den = None
    gate = None
    try:
        # THE SAME CALL THE REVIEW IS CHARGED AGAINST, campaign_method
        # included. Omitting it left ``campaign_cells`` at 0 here while
        # ``pipeline.skeptic_review`` charged the campaign's real cell count
        # (20 on R60's single reviewed candidate), so the skeptic was shown a
        # SMALLER search burden than the verdict was actually computed with -
        # always in the direction that flatters the candidate. A brief that
        # understates the denominator is not a compact brief, it is a
        # misleading one.
        den = H.search_denominator(
            pipe.mem, family_key=row.get("family_key") or "",
            asset_class=row.get("asset_class") or "",
            machine_generated=bool(spec.get("generative_search", False)),
            campaign_method=GENERATION_METHOD,
            within_family_tests=int(spec.get("within_family_tests", 1)))
        if layers:
            gate = E.gate({"layers": layers}, prior_burden=den["total"],
                          family_tests=1)
    except Exception as exc:                                   # noqa: BLE001
        gate = {"unavailable": type(exc).__name__}

    failed = []
    if gate and isinstance(gate.get("checks"), dict):
        failed = [k for k, v in gate["checks"].items() if not v]

    env = envelope(
        run_id=run_id, campaign_id=campaign_id, experiment_id=experiment_id,
        source=(spec.get("owning_agent") or "SIGNAL_AGENT"), target=SKEPTIC,
        decision="PASS or KILL this candidate. Answer only the five questions.",
        facts={
            "CANDIDATE": {"experiment_id": experiment_id,
                          "owning_agent": spec.get("owning_agent"),
                          "asset_class": row.get("asset_class"),
                          "economic_family": row.get("economic_family"),
                          "horizon_sessions": row.get("horizon_sessions")},
            "HYPOTHESIS": spec.get("hypothesis") or "",
            "ECONOMIC_MECHANISM": spec.get("economic_mechanism")
                                  or spec.get("mechanism") or "",
            "FROZEN_SPEC": {
                "expected_sign": spec.get("expected_sign"),
                "cost_model": spec.get("cost_model"),
                "frozen_gate_schema_hash": spec.get("frozen_gate_schema_hash"),
                "generative_search": spec.get("generative_search"),
                "within_family_tests": spec.get("within_family_tests")},
            "KNOWN_FAILURE_FLAGS": {
                "sign_consistent": (cand.get("signal_sign")
                                    == spec.get("expected_sign")),
                "cost_model_frozen": (cand.get("cost_model")
                                      == spec.get("cost_model")),
                "novel_identity": None if not row else row.get("novel", None)},
            "QUESTIONS": list(SKEPTIC_QUESTIONS),
        },
        metrics={
            "D_RESULT": layers.get("D") or {},
            "V_RESULT": layers.get("V") or {},
            "L_RESULT": layers.get("L") or {},
            "GATE_RESULT": gate or {},
            "PLACEBO_RESULT": _attack(result, "placebo_clean"),
            "DOUBLE_COST_RESULT": _attack(result, "cost_robust"),
            "SUBPERIOD_RESULT": _attack(result, "subperiod_stable"),
            "DEPENDENCE_DIAGNOSTICS": {
                "turnover": cand.get("turnover"),
                "halves": (layers.get("L") or {}).get("halves_ann_net_excess"),
            },
            "BURDEN": den or {},
        },
        failed_gates=failed,
        pointers={"result_artifact":
                  "research/agents/%s/artifacts/%s.json"
                  % (campaign_id, experiment_id),
                  "gate_owner": "alpha_agent.r59.engines.gate",
                  "attack_owner":
                      "alpha_agent.agents_v2.runner.adversarial_pack"})
    env["SKEPTIC_RECOMPUTES_MACHINE_TESTS"] = False
    return env


# --------------------------------------------------------------------------- #
# Risk / meta / publishing
# --------------------------------------------------------------------------- #
def risk_brief(pipe, *, run_id: str, campaign_id: str,
               experiment_id: str) -> dict:
    """Only a skeptic SURVIVOR reaches this brief."""
    row = pipe.mem.get(experiment_id) or {}
    cand = pipe._latest(EV_CANDIDATE, experiment_id) or {}
    verdict = pipe._latest(EV_SKEPTIC, experiment_id) or {}
    return envelope(
        run_id=run_id, campaign_id=campaign_id, experiment_id=experiment_id,
        source=SKEPTIC, target=RISK,
        decision=("Rule ACCEPTABLE or REJECTED on position caps, turnover, "
                  "cost on traded notional, drawdown, beta, concentration, "
                  "liquidity and short-leg expressibility."),
        facts={"skeptic_verdict": verdict.get("verdict"),
               "asset_class": row.get("asset_class"),
               "economic_family": row.get("economic_family")},
        # THE RISK AGENT RULES ON DRAWDOWN, so it is handed the CANONICAL
        # concept rather than a raw layer dict whose drawdown key means a
        # different thing in each book (R61 Workstream D). ``rulable`` is
        # false when the concept was never measured, and a false there is a
        # blocker - not a zero.
        metrics={"lockbox": (cand.get("layers") or {}).get("L") or {},
                 "drawdown": DD.risk_view((cand.get("layers") or {}).get("L")),
                 "turnover": cand.get("turnover"),
                 "cost_model": cand.get("cost_model"),
                 "cost_budget": (verdict.get("cost_budget") or {})},
        pointers={"sleeve_contract": "docs/STRATEGY_SLEEVE_CONTRACT.md",
                  "governance": "docs/DAILY_MULTI_ASSET_GOVERNANCE.md"})


def meta_brief(pipe, *, run_id: str, campaign_id: str,
               experiment_ids: list) -> dict:
    """Only VALIDATED survivors, and only when there are at least two."""
    members = []
    for eid in experiment_ids:
        row = pipe.mem.get(eid) or {}
        cand = pipe._latest(EV_CANDIDATE, eid) or {}
        members.append({"experiment_id": eid,
                        "asset_class": row.get("asset_class"),
                        "economic_family": row.get("economic_family"),
                        "lockbox": (cand.get("layers") or {}).get("L") or {}})
    return envelope(
        run_id=run_id, campaign_id=campaign_id, source=RISK, target=META,
        decision=("Rule ENSEMBLE_READY, STANDALONE or REDUNDANT over these "
                  "validated survivors."),
        facts={"members": members, "member_count": len(members),
               "minimum_for_a_real_question": RT.META_MIN_SURVIVORS},
        pointers={"contract": "research/agents/agent_contracts.json"})


def publishing_brief(pipe, *, run_id: str, campaign_id: str,
                     experiment_id: str) -> dict:
    """Only a director-CLEARED candidate reaches this brief."""
    ruling = pipe._latest(EV_DIRECTOR, experiment_id) or {}
    row = pipe.mem.get(experiment_id) or {}
    return envelope(
        run_id=run_id, campaign_id=campaign_id, experiment_id=experiment_id,
        source=DIRECTOR, target=PUBLISHING,
        decision=("Write the research candidate artifact and, if the director "
                  "asked for it, the GOVERNED prospective registration "
                  "request. Never an order, fill, approval or promotion."),
        facts={"director_decision": ruling.get("decision"),
               "outcome": row.get("outcome")},
        pointers={"adoption_door": "scripts/adopt_prospective_freeze.py",
                  "boundary": "alpha_agent.agents_v2.SIGNAL_PUBLISHING_BOUNDARY"})


#: role -> builder, for the CLI and the tests.
BUILDERS = {
    DIRECTOR: "director_brief",
    DATA_FOUNDATION: "foundation_brief",
    UNIVERSE: "foundation_brief",
    FEATURES: "foundation_brief",
    SKEPTIC: "skeptic_brief",
    RISK: "risk_brief",
    META: "meta_brief",
    PUBLISHING: "publishing_brief",
}
for _s in SIGNAL_AGENTS:
    BUILDERS[_s] = "signal_brief"


# --------------------------------------------------------------------------- #
# Resume: the transcript is NOT the system of record (Workstream H)
# --------------------------------------------------------------------------- #
RESUME_KIND = "CAMPAIGN_RESUME_STATE"
RESUME_OWNER = BRIEF_OWNER


def resume_state(pipe, *, run_id: str, campaign_id: str,
                 spec: Optional[dict] = None) -> dict:
    """Everything needed to resume a campaign after a context reset.

    A new session reads this file and knows what was pre-registered, what was
    measured, what halted where, who has ruled, which roles are still owed a
    decision and which roles must NOT be spawned. No chat transcript, no tool
    log and no prior brief is required.
    """
    state = campaign_state(pipe, campaign_id, spec)
    plan = RT.spawn_plan(state)
    experiments = []
    for eid in _experiment_ids(spec):
        row = pipe.mem.get(eid)
        if row is None:
            experiments.append({"experiment_id": eid, "preregistered": False})
            continue
        stages = [e["detail"]["stage"] for e in pipe._events(EV_STAGE, eid)]
        experiments.append({
            "experiment_id": eid,
            "preregistered": True,
            "owning_agent": (row.get("spec") or {}).get("owning_agent"),
            "asset_class": row.get("asset_class"),
            "economic_family": row.get("economic_family"),
            "stages_revealed": stages,
            "outcome": row.get("outcome"),
            "settled": bool(row.get("outcome")),
        })
    return {
        "kind": RESUME_KIND,
        "owner": RESUME_OWNER,
        "brief_version": BRIEF_VERSION,
        "agent_system_version": AGENT_SYSTEM_VERSION,
        "run_id": run_id,
        "campaign_id": campaign_id,
        "state": state,
        "experiments": experiments,
        "next_roles_to_spawn": plan["spawn"],
        "roles_not_to_spawn": [
            {"role": d["role"], "reason": d["reason"]}
            for d in plan["roles"] if d["decision"] == RT.SKIP],
        "model_routing": {k: {"model": v["model"], "effort": v["effort"]}
                          for k, v in RT.ROUTING.items()},
        "deterministic_execution_owner": "alpha_agent.agents_v2.runner",
        "full_transcript_required_to_resume": False,
        "safety": dict(SAFETY),
    }
