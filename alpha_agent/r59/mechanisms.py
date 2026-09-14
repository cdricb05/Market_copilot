"""alpha_agent.r59.mechanisms - the MECHANISM-FIRST research frontier.

THE ORCHESTRATION GAP THIS CLOSES
    The governor can only mandate what its frontier can name, and until this
    module its frontier named two things: price-state families over the owned
    Norgate panels and generative grammars over the same panels. On 2026-09-13
    every one of those scopes measured EXHAUSTED on the live memory, and the
    information-need mandates the governor still issued were RECORD-ONLY
    (``information_needs.record_mandated`` executes no research). Meanwhile the
    Alpha Recovery campaign prosecuted eleven information families - the SPY
    option surface, native futures intraday, order flow, 13F, 13D/G, 8-K,
    Form 4, fails-to-deliver - through runner stages a person chose one at a
    time, and not one of those verdicts reached ResearchMemory. The agent had
    nothing left to select, no executor to dispatch to and no record of what had
    already failed, so the choice of the next experiment lived in session notes.
    That is the whole reason each experiment needed a manual prompt.

WHAT THIS MODULE IS
    ONE adapter, in the shape of ``information_needs``. It READS a committed
    catalog of economic P&L mechanisms declared before any return exists; it
    applies the P&L WORK GATE; it refuses a mechanism that re-tests a closed
    family under a new name, that is a cosmetic variant of a live candidate, or
    that starts from a dataset instead of a mispricing; it ranks the rest by
    expected research value; and it yields mandate rows that the EXISTING
    governor, queue and handlers carry. It seeds the closed ledger into the ONE
    ResearchMemory, dispatches a mandate to its pinned, preregistered executor,
    records the verdict there, and writes the resumable agent checkpoint.

WHAT IT IS NOT
    Not a second agent, memory, governor, queue, scorer, forward ledger or
    allocator. It computes no statistic: an executor runs the canonical scorer
    (``alpha_agent.r63.sensitivity.run_cell``) and the existing multiplicity
    owners inside its own module. It purchases nothing, registers no
    challenger, promotes nothing and writes no operational store. A QUALIFIED
    verdict raises a human gate and stops there.
"""
from __future__ import annotations

import hashlib
import importlib
import json
import os
import re
from pathlib import Path
from typing import Callable, Optional

from .. import autonomous_research as AR
from .. import r59
from ..r46 import runlock as RL
from . import memory as M

CALCULATION_OWNER = "alpha_agent.r59.mechanisms"
SOURCE = "ALPHA_AGENT_MECHANISM_FRONTIER"
SCHEMA = "alpha_agent_mechanism_frontier/1"

CATALOG_PATH_ENV = "PAPER_TRADER_MECHANISM_FRONTIER_PATH"
DEFAULT_CATALOG_PATH = r59.REPO_ROOT / "research" / "alpha_agent" / "MECHANISM_FRONTIER.json"
#: Where executor module and preregistration paths are resolved (hermetic tests
#: redirect it; production resolves against this checkout).
REPO_ROOT_ENV = "PAPER_TRADER_MECHANISM_REPO_ROOT"

RELEASE = "ALPHA_AGENT"
ORIGIN = "ALPHA_AGENT_MECHANISM_FRONTIER"
GENERATION_METHOD = "MECHANISM_FRONTIER"
CLOSED_LEDGER_METHOD = "CLOSED_MECHANISM_LEDGER"
FAMILY_PREFIX = "MECHANISM:"
HYP_PREFIX = "HM_"
CLOSED_PREFIX = "HM_CLOSED_"
ORIGIN_MECHANISM_FIRST = "MECHANISM_FIRST"

CHECKPOINT_NAME = "alpha_agent_checkpoint.json"
CHECKPOINT_SUBDIR = "agent"
LEASE_SUBDIR = "leases"
#: A mechanism executor may legitimately run for hours; the lease is reclaimed
#: only when its holder is dead or this old.
EXECUTOR_LEASE_STALE_SECONDS = 12 * 3600.0

# --------------------------------------------------------------------------- #
# THE P&L WORK GATE. Every field must be answered, in words, BEFORE a return is
# looked at. A mechanism that cannot answer them is not researched.
# --------------------------------------------------------------------------- #
PNL_GATE_FIELDS = (
    "PNL_MECHANISM",
    "WHY_MISPRICING_SHOULD_EXIST",
    "WHY_IT_SHOULD_STILL_EXIST_WHEN_TRADABLE",
    "INFORMATION_ADVANTAGE_OR_STRUCTURAL_PRESSURE",
    "EXPECTED_HOLDING_HORIZON",
    "EXPECTED_COST_SURVIVABILITY",
    "WHAT_DECISION_THIS_EXPERIMENT_WILL_CHANGE",
    "SUCCESS_PATH_TO_CAPITAL",
    "KILL_RULE",
    "TIME_OR_DATA_BUDGET",
    "WHY_NOT_DUPLICATIVE",
)

#: No piece of work starts unless it satisfies at least one of these.
WORK_CONDITIONS = (
    "ADVANCES_LIVE_CANDIDATE_TOWARD_CAPITAL",
    "TESTS_DISTINCT_PNL_MECHANISM",
    "REMOVES_BLOCKER_TO_HIGH_VALUE_EXPERIMENT",
    "IMPROVES_AGENT_ABILITY",
)

#: Why a tradeable price could be wrong. A mechanism is classified by the
#: mispricing it claims, never by the dataset that measures it.
MECHANISM_CLASSES = (
    "FORCED_TRADING_FLOW",
    "HEDGER_DEMAND_PRESSURE",
    "INFORMATION_DIFFUSION_SPEED",
    "LIQUIDITY_PRESSURE",
    "DERIVATIVES_LEAD",
    "FUNDING_COLLATERAL_CONSTRAINT",
    "INDEX_FUND_DEALER_FLOW",
    "RELATIVE_VALUE_DISLOCATION",
    "RISK_TRANSFER_PREMIUM",
    "NONLINEAR_POSITIONING",
    "PUBLIC_SLOW_DISCLOSURE",
    "RISK_PREMIUM_TIMING",
    "CALENDAR_SEASONALITY",
    "PRICE_STATE_TRANSFORMATION",
)

#: A class the estate has closed AS A CLASS. A new member needs a declared
#: reopen (contract rule 13), never a new name.
CLOSED_CLASSES = ("PRICE_STATE_TRANSFORMATION",)

#: The frontier order the operating brief sets, used as a small ranking bonus;
#: measured evidence (the class multiplier below) can and does overturn it.
PRIORITY_DOMAINS = (
    "DERIVATIVES_IMPLIED_INFORMATION",
    "CROSS_MARKET_LEAD_LAG",
    "STRUCTURAL_FLOWS_FORCED_TRADING",
    "HIGH_VALUE_INFORMATION_GAP",
)
DOMAIN_BONUS = {
    "DERIVATIVES_IMPLIED_INFORMATION": 0.06,
    "CROSS_MARKET_LEAD_LAG": 0.04,
    "STRUCTURAL_FLOWS_FORCED_TRADING": 0.03,
    "HIGH_VALUE_INFORMATION_GAP": 0.0,
}

#: Mirrors contract rule 13 (``alpha_agent.alpha_recovery.program.REOPEN_REASONS``);
#: a test pins the two spellings together.
REOPEN_REASONS = ("NEW_ORTHOGONAL_INFORMATION", "PIT_HISTORY_MATERIALLY_IMPROVED",
                  "COVERAGE_MATERIALLY_IMPROVED",
                  "DISTINCT_IMPLEMENTATION_RESOLVES_NAMED_BINDING_FAILURE")

# --------------------------------------------------------------------------- #
# RESEARCH PRIORITY SCORE - expected value, not coding convenience. Every input
# is declared in the catalog before returns; the only thing that moves after
# is the class multiplier, which is MEASURED from the graveyard.
# --------------------------------------------------------------------------- #
UNIT_INPUTS = ("mechanism_strength", "unpriced_at_entry", "adjacent_evidence",
               "gross_edge", "cost_survivability", "pit_quality",
               "effective_sample", "survivorship_quality", "independence",
               "incremental_portfolio_value", "live_candidate_decision_value")
#: ``live_candidate_decision_value`` prices the first work condition: how much
#: the experiment's answer moves a LIVE candidate's odds of capital
#: eligibility. Independence prices the opposite virtue, a new return source;
#: both are declared, so the trade-off between them is visible, not implicit.
SCORE_WEIGHTS = {
    "mechanism_strength": 0.15,
    "unpriced_at_entry": 0.12,
    "adjacent_evidence": 0.07,
    "gross_edge": 0.09,
    "cost_survivability": 0.09,
    "pit_quality": 0.07,
    "effective_sample": 0.07,
    "survivorship_quality": 0.04,
    "independence": 0.08,
    "incremental_portfolio_value": 0.08,
    "live_candidate_decision_value": 0.08,
    "implementation_speed": 0.03,
    "data_cost": 0.03,
}
IMPLEMENTATION_DAYS_HORIZON = 20.0
DATA_COST_USD_HORIZON = 2000.0
#: Each closed mechanism of the SAME class multiplies that class's adjacent
#: evidence by this, down to the floor; a live candidate in the class adds the
#: bonus. Four failed public-disclosure axes are evidence about the fifth.
ADJACENT_DECAY = 0.80
ADJACENT_FLOOR = 0.25
ADJACENT_LIVE_BONUS = 0.15
EIV_BASE = 0.50
EIV_SLOPE = 0.45
EIV_CAP = 0.95
DEFAULT_MECHANISM_BATCH = 8

MIN_FIELD_CHARS = 30
_PLACEHOLDER_PREFIXES = ("tbd", "todo", "to be determined", "see results",
                         "after results", "unknown", "n/a", "later", "pending")
#: A kill rule must be decidable from a measurement. One of these must appear.
KILL_RULE_TOKENS = ("t <", "t<", "t-stat", "%", "bp", "period", "sign",
                    "multiplic", "bh", "holm", "effective", "increment",
                    "priced", "material", "partition", "ic ")
#: Closed-family semantic tokens shared before a candidate must NAME the family
#: in WHY_NOT_DUPLICATIVE (a rename cannot slip through unexamined).
TOKEN_HITS = 2
MEMORY_FAMILY_TOKEN_HITS = 3

# Statuses ------------------------------------------------------------------ #
ST_ELIGIBLE = "ELIGIBLE"
ST_HUMAN_GATE = "HUMAN_GATE_PURCHASE"
ST_REFUSED_GATE = "REFUSED_PNL_WORK_GATE"
ST_REFUSED_DUPLICATE = "REFUSED_DUPLICATIVE"
ST_SETTLED_CLOSED = "SETTLED_CLOSED"
ST_SETTLED_QUALIFIED = "SETTLED_QUALIFIED_AWAITING_HUMAN_GATE"
ST_SETTLED_HOLD = "SETTLED_HOLD"
#: An eligible mechanism whose next action loses to a higher-value action in the
#: global multi-asset top five (``alpha_agent.r59.global_frontier``). Deferred,
#: never refused: it becomes issuable again the moment the frontier changes.
ST_DEFERRED_GLOBAL = "DEFERRED_BY_GLOBAL_OPPORTUNITY_COST"
REFUSED_STATUSES = (ST_REFUSED_GATE, ST_REFUSED_DUPLICATE)
SETTLED_STATUSES = (ST_SETTLED_CLOSED, ST_SETTLED_QUALIFIED, ST_SETTLED_HOLD)

EX_READY = "READY"
EX_NOT_BUILT = "NOT_BUILT"
EX_PREREG_MISSING = "PREREGISTRATION_MISSING"
EX_PIN_MISMATCH = "PIN_MISMATCH"

# Executor verdict contract ------------------------------------------------- #
V_QUALIFIED = "QUALIFIED"
V_DATA_HOLD = "DATA_HOLD"
V_NEED_MORE = "NEED_MORE_EVIDENCE"
EXECUTOR_VERDICTS = (
    V_QUALIFIED, "NO_EDGE", "NO_INCREMENTAL_INFORMATION_EDGE",
    "KILLED_PRICED_BEFORE_ENTRY", "KILLED_INSUFFICIENT_SAMPLE",
    "KILLED_PIT_UNESTABLISHED", "KILLED_BELOW_MATERIALITY",
    "KILLED_NONINCREMENTAL", "KILLED_UNSTABLE", "KILLED_WRONG_SIGN",
    "KILLED_MULTIPLICITY", "KILLED_TIMING_CONTRADICTS_MECHANISM",
    V_DATA_HOLD, V_NEED_MORE)
VERDICT_OUTCOME = {v: r59.HO_NO_ALPHA_EVIDENCE for v in EXECUTOR_VERDICTS}
VERDICT_OUTCOME.update({V_QUALIFIED: r59.HO_QUALIFIED, V_DATA_HOLD: r59.HO_DATA_HOLD,
                        V_NEED_MORE: r59.HO_NEEDS_MORE_EVIDENCE})

HUMAN_GATE_REGISTRATION = (
    "PROSPECTIVE_REGISTRATION: contract rule 16 keeps live registration and "
    "adoption human-gated (scripts/adopt_prospective_freeze.py with its "
    "confirmation token); the agent froze nothing and registered nothing")

_STOP = frozenset(
    "the a an of and or to in on for with by from at as is are be this that it "
    "its into than then when which who what why how not no over under per via "
    "vs versus after before their there these those were was has have had can "
    "may will would should could does did been being more most less least".split())


# --------------------------------------------------------------------------- #
# Catalog
# --------------------------------------------------------------------------- #
def catalog_path() -> Path:
    return Path(os.environ.get(CATALOG_PATH_ENV) or DEFAULT_CATALOG_PATH)


def load_catalog(path: Optional[Path] = None) -> Optional[dict]:
    p = Path(path) if path else catalog_path()
    try:
        if not p.exists():
            return None
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def find(catalog: Optional[dict], mechanism_id: Optional[str]) -> Optional[dict]:
    for e in (catalog or {}).get("mechanisms") or []:
        if e.get("mechanism_id") == mechanism_id:
            return e
    return None


def normalised_sha256(path: Path) -> Optional[str]:
    """sha256 of a text file with CRLF folded to LF (the worktree is CRLF, the
    live checkout LF, and git considers them identical)."""
    try:
        data = Path(path).read_bytes()
    except OSError:
        return None
    return hashlib.sha256(data.replace(b"\r\n", b"\n")).hexdigest()


def entry_hash(entry: dict) -> str:
    return r59.short_hash(entry, 16)


def _tokens(text: str) -> set:
    return {t for t in re.findall(r"[a-z0-9]+", str(text or "").lower())
            if len(t) >= 3 and t not in _STOP}


def _objects(row: dict) -> set:
    return {str(o).strip().upper() for o in (row.get("information_objects") or [])
            if str(o).strip()}


def _clip01(x) -> float:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, min(1.0, v))


# --------------------------------------------------------------------------- #
# The gate
# --------------------------------------------------------------------------- #
def _placeholder(text: str) -> bool:
    low = str(text).strip().lower()
    return any(low.startswith(p) for p in _PLACEHOLDER_PREFIXES)


def pnl_gate(entry: dict) -> dict:
    """The P&L WORK GATE. Pure. Returns every failure, not just the first."""
    failures: list = []
    if entry.get("origin") != ORIGIN_MECHANISM_FIRST:
        failures.append(
            "DATASET_TOURISM: origin %r is not MECHANISM_FIRST - research starts "
            "from why a tradeable price could be wrong, never from an untested "
            "dataset" % entry.get("origin"))
    cls = entry.get("mechanism_class")
    if cls not in MECHANISM_CLASSES:
        failures.append("NO_ECONOMIC_MECHANISM_CLASS: %r" % cls)
    elif cls in CLOSED_CLASSES and not entry.get("reopen"):
        failures.append("CLOSED_MECHANISM_CLASS: %s reopens only under contract "
                        "rule 13 with a declared reopen" % cls)
    if entry.get("domain") not in PRIORITY_DOMAINS:
        failures.append("UNKNOWN_DOMAIN: %r" % entry.get("domain"))
    if entry.get("asset_class") not in r59.ASSET_CLASSES:
        failures.append("UNKNOWN_ASSET_CLASS: %r" % entry.get("asset_class"))
    if entry.get("declared_before_returns") is not True:
        failures.append("NOT_DECLARED_BEFORE_RETURNS")
    gate = entry.get("pnl_gate") or {}
    for f in PNL_GATE_FIELDS:
        v = gate.get(f)
        if not isinstance(v, str) or len(v.strip()) < MIN_FIELD_CHARS:
            failures.append("PNL_GATE_FIELD_MISSING: %s" % f)
        elif _placeholder(v):
            failures.append("PNL_GATE_FIELD_PLACEHOLDER: %s" % f)
    kr = str(gate.get("KILL_RULE") or "").lower()
    if kr and not any(tok in kr for tok in KILL_RULE_TOKENS):
        failures.append("KILL_RULE_NOT_MEASURABLE")
    wc = entry.get("work_conditions") or []
    if not wc or any(c not in WORK_CONDITIONS for c in wc):
        failures.append("WORK_CONDITION_UNSATISFIED: %r" % (wc,))
    si = entry.get("score_inputs") or {}
    missing = [k for k in UNIT_INPUTS + ("implementation_days", "data_cost_usd")
               if si.get(k) is None]
    if missing:
        failures.append("SCORE_INPUTS_UNDECLARED: %s" % ",".join(missing))
    if not _objects(entry):
        failures.append("NO_INFORMATION_OBJECT")
    return {"passed": not failures, "failures": failures}


def _valid_reopen(reopen: dict, family_id: str) -> bool:
    return bool(reopen) and reopen.get("closed_family") == family_id \
        and reopen.get("reason") in REOPEN_REASONS \
        and len(str(reopen.get("measured_binding_failure") or "").strip()) >= MIN_FIELD_CHARS


def closed_index(catalog: Optional[dict], mem: Optional[M.ResearchMemory] = None, *,
                 exclude: Optional[str] = None) -> list:
    """Every closed mechanism the estate knows: the committed ledger, plus every
    mechanism this frontier has itself prosecuted to a negative verdict."""
    out = []
    for fam in (catalog or {}).get("closed_mechanisms") or []:
        out.append({"family_id": fam.get("family_id"),
                    "mechanism_class": fam.get("mechanism_class"),
                    "objects": _objects(fam),
                    "tokens": {str(t).lower() for t in fam.get("semantic_tokens") or []},
                    "source": "LEDGER"})
    if mem is not None:
        by_id = {e.get("mechanism_id"): e for e in (catalog or {}).get("mechanisms") or []}
        for row in mem.list_hypotheses(generation_method=GENERATION_METHOD, limit=5000):
            if row.get("outcome") not in (r59.HO_NO_ALPHA_EVIDENCE, r59.HO_REJECTED):
                continue
            mid = str(row.get("economic_family") or "")[len(FAMILY_PREFIX):]
            if not mid or mid == exclude:
                continue
            e = by_id.get(mid) or (row.get("spec") or {})
            out.append({"family_id": mid,
                        "mechanism_class": e.get("mechanism_class"),
                        "objects": _objects(e),
                        "tokens": {str(t).lower() for t in e.get("semantic_tokens") or []},
                        "source": "EXECUTED"})
    return out


def distinctness(entry: dict, *, closed: list, live: list,
                 memory_families: Optional[dict] = None) -> dict:
    """Refuse a closed mechanism under a new name, and a cosmetic variant of a
    live candidate. An overlap is allowed only when WHY_NOT_DUPLICATIVE names
    the family it overlaps, so the author has to confront it."""
    gate = entry.get("pnl_gate") or {}
    why = str(gate.get("WHY_NOT_DUPLICATIVE") or "").upper()
    objs = _objects(entry)
    toks = _tokens(" ".join([str(entry.get("title") or ""),
                             str(gate.get("PNL_MECHANISM") or ""),
                             " ".join(objs).replace(":", " ").replace("_", " ")]))
    reopen = entry.get("reopen") or {}
    refusals, addressed = [], []
    for c in closed:
        fid = str(c.get("family_id") or "")
        if fid == entry.get("mechanism_id"):
            continue
        exact = objs & c["objects"]
        if exact:
            if _valid_reopen(reopen, fid):
                addressed.append(fid)
            else:
                refusals.append("DUPLICATE_OF_CLOSED: %s (same information object %s)"
                                % (fid, sorted(exact)))
            continue
        need = min(TOKEN_HITS, len(c["tokens"]))
        hits = c["tokens"] & toks
        if need and len(hits) >= need:
            if fid.upper() in why:
                addressed.append(fid)
            else:
                refusals.append("UNADDRESSED_OVERLAP_WITH_CLOSED: %s (shared %s)"
                                % (fid, sorted(hits)))
    for lc in live:
        cid = str(lc.get("candidate_id") or "")
        if objs & _objects(lc):
            refusals.append("COSMETIC_VARIANT_OF_LIVE_CANDIDATE: %s" % cid)
            continue
        ltoks = {str(t).lower() for t in lc.get("semantic_tokens") or []}
        need = min(TOKEN_HITS, len(ltoks))
        if need and len(ltoks & toks) >= need:
            if cid.upper() in why:
                addressed.append(cid)
            else:
                refusals.append("UNADDRESSED_OVERLAP_WITH_LIVE_CANDIDATE: %s" % cid)
    for fam in sorted(memory_families or {}):
        ftoks = _tokens(str(fam).replace("_", " ").replace(":", " "))
        if len(ftoks) < MEMORY_FAMILY_TOKEN_HITS or str(fam).startswith(FAMILY_PREFIX):
            continue
        if len(ftoks & toks) >= MEMORY_FAMILY_TOKEN_HITS:
            if str(fam).upper() in why:
                addressed.append(fam)
            else:
                refusals.append("UNADDRESSED_OVERLAP_WITH_GRAVEYARD_FAMILY: %s" % fam)
    return {"distinct": not refusals, "refusals": refusals,
            "addressed_overlaps": sorted(set(addressed))}


# --------------------------------------------------------------------------- #
# Score
# --------------------------------------------------------------------------- #
def evidenced_live_candidates(catalog: Optional[dict]) -> list:
    """Live candidates that carry HISTORICAL qualification evidence.

    A challenger that is merely forward-pending in some tournament protects its
    information object from duplication, but it is not evidence that its
    mechanism class pays; only a candidate that survived its own historical
    gates may lift the adjacent-evidence prior of its class.
    """
    return [c for c in (catalog or {}).get("live_candidates") or []
            if str(c.get("historical_evidence") or "").strip()]


def evidenced_live_classes(catalog: Optional[dict]) -> set:
    return {c.get("mechanism_class") for c in evidenced_live_candidates(catalog)}


def closed_by_class(catalog: Optional[dict], mem: Optional[M.ResearchMemory]) -> dict:
    out: dict = {}
    for c in closed_index(catalog, mem):
        k = c.get("mechanism_class")
        if k:
            out[k] = out.get(k, 0) + 1
    return out


def score(entry: dict, *, class_counts: dict, live_classes: set) -> dict:
    si = entry.get("score_inputs") or {}
    x = {k: _clip01(si.get(k)) for k in UNIT_INPUTS}
    days = si.get("implementation_days")
    usd = si.get("data_cost_usd")
    x["implementation_speed"] = 1.0 - min(1.0, max(0.0, float(days if days is not None else
                                                               IMPLEMENTATION_DAYS_HORIZON))
                                          / IMPLEMENTATION_DAYS_HORIZON)
    x["data_cost"] = 1.0 - min(1.0, max(0.0, float(usd if usd is not None else
                                                   DATA_COST_USD_HORIZON))
                               / DATA_COST_USD_HORIZON)
    cls = entry.get("mechanism_class")
    n_closed = int(class_counts.get(cls, 0))
    mult = max(ADJACENT_FLOOR, ADJACENT_DECAY ** n_closed)
    live = cls in live_classes
    x["adjacent_evidence"] = min(1.0, x["adjacent_evidence"] * mult
                                 + (ADJACENT_LIVE_BONUS if live else 0.0))
    base = sum(SCORE_WEIGHTS[k] * x[k] for k in SCORE_WEIGHTS)
    bonus = DOMAIN_BONUS.get(entry.get("domain"), 0.0)
    return {"total": round(base + bonus, 4), "base": round(base, 4),
            "domain_bonus": bonus, "class_closed_count": n_closed,
            "class_multiplier": round(mult, 4), "live_candidate_in_class": live,
            "inputs_effective": {k: round(v, 4) for k, v in x.items()},
            "weights": dict(SCORE_WEIGHTS)}


def eiv_for(total: float) -> float:
    return round(min(EIV_CAP, EIV_BASE + EIV_SLOPE * float(total or 0.0)), 4)


# --------------------------------------------------------------------------- #
# Executor state
# --------------------------------------------------------------------------- #
def executor_state(entry: dict, *, repo_root: Optional[Path] = None) -> dict:
    """Is this mechanism's executor built, preregistered and PINNED?

    An executor runs only when its module and its preregistration both exist
    and both hash to the values pinned in the catalog. The pin is the
    deliberate act that releases an experiment: implement, commit, pin, commit.
    A later edit to either file un-pins it, so code cannot change silently
    after a result exists.
    """
    root = Path(repo_root or os.environ.get(REPO_ROOT_ENV) or r59.REPO_ROOT)
    ex = entry.get("executor") or {}
    if not ex.get("callable") or not ex.get("module_path"):
        return {"state": EX_NOT_BUILT, "pin": None}
    mod = root / ex["module_path"]
    if not mod.exists():
        return {"state": EX_NOT_BUILT, "pin": None, "module_path": str(mod)}
    pre = root / str(ex.get("preregistration_path") or "")
    if not ex.get("preregistration_path") or not pre.exists():
        return {"state": EX_PREREG_MISSING, "pin": None}
    got_mod, got_pre = normalised_sha256(mod), normalised_sha256(pre)
    pin = {"module_sha256": got_mod, "preregistration_sha256": got_pre}
    if got_mod != ex.get("module_sha256") or got_pre != ex.get("preregistration_sha256"):
        return {"state": EX_PIN_MISMATCH, "pin": None, "observed": pin,
                "pinned": {"module_sha256": ex.get("module_sha256"),
                           "preregistration_sha256": ex.get("preregistration_sha256")}}
    return {"state": EX_READY, "pin": pin}


def resolve_executor(entry: dict) -> Callable:
    target = str((entry.get("executor") or {}).get("callable") or "")
    mod_name, _, fn_name = target.partition(":")
    if not mod_name.startswith("alpha_agent.") or not fn_name:
        raise ValueError("executor %r must be an alpha_agent.<module>:<function> "
                         "research callable" % target)
    return getattr(importlib.import_module(mod_name), fn_name)


# --------------------------------------------------------------------------- #
# Assessment
# --------------------------------------------------------------------------- #
def _settled_status(mem: Optional[M.ResearchMemory], entry: dict) -> Optional[dict]:
    if mem is None:
        return None
    row = mem.get(HYP_PREFIX + str(entry.get("mechanism_id")))
    if not row or row.get("outcome") is None:
        return None
    out = row.get("outcome")
    rob = row.get("robustness") or {}
    if out == r59.HO_QUALIFIED:
        return {"status": ST_SETTLED_QUALIFIED, "row": row}
    if out in (r59.HO_DATA_HOLD, r59.HO_NEEDS_MORE_EVIDENCE):
        if rob.get("data_version") != entry.get("data_version"):
            return None
        return {"status": ST_SETTLED_HOLD, "row": row}
    rerun = entry.get("rerun_for_measured_defect") or {}
    if (len(str(rerun.get("defect") or "")) >= MIN_FIELD_CHARS
            and rerun.get("defect_commit")
            and rob.get("executor_pin") != executor_state(entry).get("pin")):
        return None
    return {"status": ST_SETTLED_CLOSED, "row": row}


def assess(entry: dict, *, catalog: Optional[dict],
           mem: Optional[M.ResearchMemory] = None,
           closed: Optional[list] = None,
           class_counts: Optional[dict] = None,
           memory_families: Optional[dict] = None) -> dict:
    mid = entry.get("mechanism_id")
    live = list((catalog or {}).get("live_candidates") or [])
    gate = pnl_gate(entry)
    closed = closed if closed is not None else closed_index(catalog, mem, exclude=mid)
    dist = distinctness(entry, closed=closed, live=live,
                        memory_families=memory_families)
    counts = class_counts if class_counts is not None else closed_by_class(catalog, mem)
    sc = score(entry, class_counts=counts, live_classes=evidenced_live_classes(catalog))
    ex = executor_state(entry)
    settled = _settled_status(mem, entry)
    if not gate["passed"]:
        status, reasons = ST_REFUSED_GATE, gate["failures"]
    elif not dist["distinct"]:
        status, reasons = ST_REFUSED_DUPLICATE, dist["refusals"]
    elif settled:
        status = settled["status"]
        reasons = [str((settled["row"] or {}).get("reason_rejected")
                       or (settled["row"] or {}).get("outcome"))]
    elif entry.get("requires_purchase"):
        status, reasons = ST_HUMAN_GATE, [
            "the information is not owned and no free path exists; the purchase "
            "case is prepared and the loop stops for a human decision"]
    else:
        status, reasons = ST_ELIGIBLE, []
    return {"mechanism_id": mid, "title": entry.get("title"),
            "status": status, "reasons": reasons,
            "domain": entry.get("domain"), "mechanism_class": entry.get("mechanism_class"),
            "asset_class": entry.get("asset_class"),
            "information_objects": sorted(_objects(entry)),
            "score": sc, "eiv": eiv_for(sc["total"]),
            "executor_state": ex["state"], "executor_pin": ex.get("pin"),
            "addressed_overlaps": dist["addressed_overlaps"],
            "catalog_entry_hash": entry_hash(entry)}


def frontier(mem: Optional[M.ResearchMemory] = None, *,
             catalog: Optional[dict] = None) -> dict:
    cat = catalog if catalog is not None else load_catalog()
    if not cat:
        return {"state": "CATALOG_UNAVAILABLE", "catalog_path": str(catalog_path()),
                "ranked": [], "rows": []}
    graveyard = mem.graveyard_families() if mem is not None else {}
    counts = closed_by_class(cat, mem)
    rows = []
    for entry in cat.get("mechanisms") or []:
        closed = closed_index(cat, mem, exclude=entry.get("mechanism_id"))
        rows.append(assess(entry, catalog=cat, mem=mem, closed=closed,
                           class_counts=counts, memory_families=graveyard))
    ranked = sorted((r for r in rows if r["status"] == ST_ELIGIBLE),
                    key=lambda r: (-r["score"]["total"], str(r["mechanism_id"])))
    by_status: dict = {}
    for r in rows:
        by_status.setdefault(r["status"], []).append(r["mechanism_id"])
    return {"state": "OK", "calculation_owner": CALCULATION_OWNER,
            "catalog_path": str(catalog_path()),
            "catalog_hash": r59.short_hash(cat, 16),
            "n_mechanisms": len(rows), "by_status": by_status,
            "class_closed_counts": counts, "ranked": ranked, "rows": rows}


def candidates(mem: Optional[M.ResearchMemory] = None, *,
               catalog: Optional[dict] = None,
               limit: int = DEFAULT_MECHANISM_BATCH) -> list:
    """Governor-shaped rows for the eligible mechanisms, best first.

    The payload carries the executor state and pin and the catalog entry hash,
    so the mandate identity MOVES when an executor is built or re-pinned: a
    mechanism that was blocked on a missing executor becomes issuable again the
    moment it is released, without any clock and without retrying a refusal.

    When the catalog declares the global reconciliation, a mechanism is offered
    only if its next action beats every higher-value action the same research
    capacity could take in the GLOBAL multi-asset top five; the verdict travels
    in the payload, so a deferred mechanism re-enters the moment it flips.
    """
    cat = catalog if catalog is not None else load_catalog()
    fr = frontier(mem, catalog=cat)
    gfr = global_frontier_for(mem, cat)
    out = []
    for r in fr["ranked"]:
        if len(out) >= int(limit):
            break
        payload = {"source": SOURCE, "mechanism_id": r["mechanism_id"],
                   "domain": r["domain"], "mechanism_class": r["mechanism_class"],
                   "priority_score": r["score"]["total"],
                   "executor_state": r["executor_state"],
                   "executor_pin": r["executor_pin"],
                   "catalog_entry_hash": r["catalog_entry_hash"]}
        if gfr is not None:
            from . import global_frontier as GF
            cmp = GF.mechanism_comparison(gfr, r["mechanism_id"])
            if not cmp["admitted"]:
                continue
            payload["global_candidate_id"] = (gfr.get("member_to_candidate") or {}).get(
                r["mechanism_id"])
            payload["global_verdict"] = cmp["verdict"]
        out.append({
            "asset_class": r["asset_class"],
            "family": FAMILY_PREFIX + str(r["mechanism_id"]),
            "eiv": r["eiv"],
            "reason": ("mechanism frontier: %s / %s priority %.4f executor %s"
                       % (r["domain"], r["mechanism_class"], r["score"]["total"],
                          r["executor_state"])),
            "payload": payload})
    return out


def global_frontier_for(mem: Optional[M.ResearchMemory],
                        catalog: Optional[dict]) -> Optional[dict]:
    """The global multi-asset frontier, when the catalog declares its reconciliation.

    Imported lazily because ``global_frontier`` reads this module. A catalog
    without the section (a hermetic fixture) keeps the mechanism-only behaviour
    exactly; the committed catalog declares it, so production never skips it.
    """
    from . import global_frontier as GF
    if not GF.declared(catalog):
        return None
    return GF.current(mem, catalog=catalog)


def is_mechanism_mandate(mandate: Optional[dict]) -> bool:
    p = (mandate or {}).get("payload") or {}
    return p.get("source") == SOURCE and bool(p.get("mechanism_id"))


# --------------------------------------------------------------------------- #
# Memory: the closed ledger and executed verdicts
# --------------------------------------------------------------------------- #
def _info_root(row: dict) -> str:
    objs = sorted(_objects(row))
    return objs[0].split(":")[0] if objs else "UNDECLARED"


def seed_closed(mem: M.ResearchMemory, catalog: Optional[dict] = None) -> dict:
    """Write the committed closed-mechanism ledger into the ONE memory.

    Idempotent: a family already settled is left untouched, so re-seeding
    never rewrites a verdict's settlement instant. Each closed family counts
    once to the search burden; its cell-level multiplicity lives in the
    family's own recorded artifact.
    """
    cat = catalog if catalog is not None else load_catalog()
    added, present = [], 0
    for fam in (cat or {}).get("closed_mechanisms") or []:
        fid = str(fam.get("family_id") or "")
        if not fid:
            continue
        hid = CLOSED_PREFIX + fid
        existing = mem.get(hid)
        if existing and existing.get("outcome") is not None:
            present += 1
            continue
        ac = fam.get("asset_scope") if fam.get("asset_scope") in r59.ASSET_CLASSES \
            else r59.AC_CROSS_ASSET
        mem.register(title="Closed mechanism %s" % fid,
                     release=str(fam.get("release") or "LEDGER"),
                     origin="CLOSED_MECHANISM_LEDGER",
                     generation_method=CLOSED_LEDGER_METHOD,
                     information_family=_info_root(fam), economic_family=fid,
                     asset_class=ac, model_family="CLOSED_MECHANISM_LEDGER",
                     horizon_sessions=None,
                     input_data_identity=str(fam.get("evidence") or ""),
                     spec={"mechanism_class": fam.get("mechanism_class"),
                           "information_objects": sorted(_objects(fam)),
                           "semantic_tokens": fam.get("semantic_tokens") or [],
                           "ledger": SCHEMA},
                     hyp_id=hid)
        mem.record_result(hid, outcome=r59.HO_NO_ALPHA_EVIDENCE,
                          evidence_maturity="HISTORICAL",
                          statistic=fam.get("statistic") or None,
                          robustness={"binding_failure": fam.get("binding_failure"),
                                      "forbidden_rescues": fam.get("forbidden_rescues") or [],
                                      "horizons": fam.get("horizons")},
                          reason_rejected=str(fam.get("verdict") or "CLOSED"),
                          reopen_condition=str(fam.get("reopen_condition")
                                               or "NEW_ORTHOGONAL_INFORMATION"))
        added.append(fid)
    if added:
        mem.event("CLOSED_MECHANISMS_SEEDED", subject=CALCULATION_OWNER,
                  detail={"added": added, "already_present": present})
    return {"added": added, "already_present": present}


def validate_result(result) -> list:
    problems = []
    if not isinstance(result, dict):
        return ["result is not a mapping"]
    if result.get("verdict") not in EXECUTOR_VERDICTS:
        problems.append("verdict %r is not in the executor vocabulary" % result.get("verdict"))
    if result.get("capital_eligible") is not False:
        problems.append("capital_eligible must be False: capital eligibility is decided by "
                        "forward evidence and human governance, never by an executor")
    if not result.get("why"):
        problems.append("why is required")
    return problems


def record_execution(mem: M.ResearchMemory, entry: dict, result: dict, *,
                     pin: Optional[dict]) -> dict:
    mid = str(entry["mechanism_id"])
    verdict = result["verdict"]
    outcome = VERDICT_OUTCOME[verdict]
    hid = mem.register(
        title="Alpha Agent mechanism %s" % mid, release=RELEASE, origin=ORIGIN,
        generation_method=GENERATION_METHOD, information_family=_info_root(entry),
        economic_family=FAMILY_PREFIX + mid, asset_class=entry["asset_class"],
        model_family=str(entry.get("model_family") or "PREREGISTERED_EXECUTOR"),
        horizon_sessions=entry.get("horizon_sessions"),
        input_data_identity=str(result.get("input_data_identity") or ""),
        spec={"mechanism_id": mid, "mechanism_class": entry.get("mechanism_class"),
              "domain": entry.get("domain"),
              "information_objects": sorted(_objects(entry)),
              "semantic_tokens": entry.get("semantic_tokens") or []},
        hyp_id=HYP_PREFIX + mid)
    qualified = outcome == r59.HO_QUALIFIED
    mem.record_result(
        hid, outcome=outcome, evidence_maturity="HISTORICAL",
        statistic=result.get("statistic"), economics=result.get("economics"),
        robustness={"verdict": verdict, "kill_rule_fired": result.get("kill_rule_fired"),
                    "why": result.get("why"), "multiplicity": result.get("multiplicity"),
                    "artifact": result.get("artifact"), "scorer": result.get("scorer"),
                    "executor_pin": pin, "data_version": entry.get("data_version"),
                    "catalog_entry_hash": entry_hash(entry)},
        reason_rejected=None if qualified else "%s: %s" % (verdict, result.get("why")),
        reopen_condition=None if qualified else "NEW_ORTHOGONAL_INFORMATION")
    mem.event("MECHANISM_SETTLED", subject=mid,
              detail={"verdict": verdict, "outcome": outcome,
                      "kill_rule_fired": result.get("kill_rule_fired"),
                      "artifact": result.get("artifact")})
    if qualified:
        mem.event("MECHANISM_QUALIFIED_AWAITING_HUMAN_GATE", subject=mid,
                  detail={"gate": HUMAN_GATE_REGISTRATION,
                          "registers_forward_challenger": False,
                          "promotes_model": False})
    return {"hypothesis_id": hid, "outcome": outcome, "verdict": verdict,
            "human_gate": HUMAN_GATE_REGISTRATION if qualified else None}


# --------------------------------------------------------------------------- #
# The handler body (called from handlers.make_handlers)
# --------------------------------------------------------------------------- #
def _lease_path(mechanism_id: str) -> Path:
    d = r59.research_root() / CHECKPOINT_SUBDIR / LEASE_SUBDIR
    d.mkdir(parents=True, exist_ok=True)
    return d / ("%s.lease" % re.sub(r"[^A-Za-z0-9_.-]", "_", mechanism_id))


def retire_superseded(queue, mechanism_id: str, keep_job_id: str) -> int:
    """Reject older BLOCKED jobs for the same mechanism once a newer mandate for
    it is being executed (its executor was built, or its entry re-declared)."""
    if queue is None:
        return 0
    n = 0
    for job in queue.blocked_jobs(limit=1000):
        p = (job.payload or {}).get("payload") or {}
        if job.job_id != keep_job_id and p.get("source") == SOURCE \
                and p.get("mechanism_id") == mechanism_id:
            queue.reject(job.job_id, "superseded by a newer mandate for the same mechanism")
            n += 1
    return n


def execute_job(mem: M.ResearchMemory, job, *, queue=None,
                catalog: Optional[dict] = None) -> tuple:
    m = job.payload or {}
    p = m.get("payload") or {}
    mid = str(p.get("mechanism_id") or "")
    cat = catalog if catalog is not None else load_catalog()
    retired = retire_superseded(queue, mid, job.job_id)
    entry = find(cat, mid)
    base = {"real_work": "alpha_agent_mechanism", "mechanism_id": mid,
            "superseded_jobs_retired": retired, "hypotheses_measured": 0}
    if entry is None:
        return AR.OUTCOME_REJECTED, {**base, "reason": "mechanism is no longer declared in "
                                                      "the catalog (superseded)"}
    a = assess(entry, catalog=cat, mem=mem,
               memory_families=mem.graveyard_families())
    if a["status"] in REFUSED_STATUSES:
        mem.event("MECHANISM_REFUSED", subject=mid,
                  detail={"status": a["status"], "reasons": a["reasons"]})
        return AR.OUTCOME_REJECTED, {**base, "disposition": a["status"],
                                     "reason": "; ".join(a["reasons"])[:900]}
    if a["status"] in SETTLED_STATUSES:
        return AR.OUTCOME_COMPLETED, {**base, "disposition": "ALREADY_SETTLED:%s" % a["status"]}
    gfr = global_frontier_for(mem, cat)
    if gfr is not None:
        from . import global_frontier as GF
        cmp = GF.mechanism_comparison(gfr, mid)
        if not cmp["admitted"]:
            mem.event("MECHANISM_DEFERRED_BY_GLOBAL_OPPORTUNITY_COST", subject=mid,
                      detail={k: cmp.get(k) for k in ("verdict", "why", "frontier_state",
                                                      "beaten_by")})
            return AR.OUTCOME_BLOCKED_SPECIFIC, {
                **base, "disposition": ST_DEFERRED_GLOBAL,
                "reason": ("DEFERRED BY THE GLOBAL MULTI-ASSET FRONTIER: %s" % cmp["why"])[:900]}
    if a["status"] == ST_HUMAN_GATE:
        return AR.OUTCOME_BLOCKED_SPECIFIC, {
            **base, "disposition": ST_HUMAN_GATE,
            "reason": "HUMAN GATE - purchase required: the estate is not entitled to %s; the "
                      "agent never buys, subscribes or starts a trial"
                      % ", ".join(a["information_objects"])}
    ex = executor_state(entry)
    if ex["state"] != EX_READY:
        text = {EX_NOT_BUILT: "EXECUTOR NOT BUILT",
                EX_PREREG_MISSING: "PREREGISTRATION MISSING",
                EX_PIN_MISMATCH: "EXECUTOR PIN MISMATCH"}[ex["state"]]
        return AR.OUTCOME_BLOCKED_SPECIFIC, {
            **base, "disposition": ex["state"],
            "reason": "%s for %s - build order: %s" % (text, mid, " / ".join(build_order(entry)))}
    lease = _lease_path(mid)
    holder = "mechanism_executor:%s:%d" % (mid, os.getpid())
    try:
        RL.acquire_path(lease, holder, wait_s=0, stale_after_s=EXECUTOR_LEASE_STALE_SECONDS,
                        extra={"job_id": job.job_id})
    except RL.AdvanceLockBusy:
        return AR.OUTCOME_RETRYABLE, {**base, "reason": "another worker holds this mechanism's "
                                                        "executor lease"}
    try:
        fn = resolve_executor(entry)
        result = fn(mechanism=entry)
    finally:
        RL.release_path(lease, holder)
    problems = validate_result(result)
    if problems:
        return AR.OUTCOME_BLOCKED_SPECIFIC, {
            **base, "reason": "MALFORMED EXECUTOR RESULT for %s: %s" % (mid, "; ".join(problems))}
    rec = record_execution(mem, entry, result, pin=ex["pin"])
    return AR.OUTCOME_COMPLETED, {
        **base, "hypotheses_measured": 1, "candidate_id": rec["hypothesis_id"],
        "disposition": rec["verdict"], "outcome": rec["outcome"],
        "human_gate": rec["human_gate"], "artifact": result.get("artifact")}


def build_order(entry: dict) -> list:
    mid = str(entry.get("mechanism_id"))
    return [
        "write research/preregistration/%s_PREREGISTRATION.md from the catalog's P&L gate "
        "fields and commit it before any return exists" % mid,
        "implement an alpha_agent.alpha_recovery executor returning the executor result "
        "contract (verdict, why, statistic, economics, multiplicity, artifact, "
        "capital_eligible=False) through alpha_agent.r63.sensitivity.run_cell and the "
        "existing multiplicity owner",
        "commit, then pin executor.module_sha256 and executor.preregistration_sha256 in "
        "the catalog and commit; the agent executes it on its next iteration"]


# --------------------------------------------------------------------------- #
# Checkpoint
# --------------------------------------------------------------------------- #
def _mechanism_jobs(queue) -> dict:
    out = {"running": [], "queued": [], "blocked": []}
    if queue is None:
        return out
    for state, key in ((AR.STATE_RUNNING, "running"), (AR.STATE_QUEUED, "queued"),
                       (AR.STATE_RETRYABLE, "queued"), (AR.STATE_BLOCKED_SPECIFIC, "blocked")):
        for job in queue.list_jobs(state=state, limit=1000):
            if not str(job.lane).startswith(r59.LANE_MECHANISM_PREFIX):
                continue
            p = (job.payload or {}).get("payload") or {}
            out[key].append({"job_id": job.job_id, "mechanism_id": p.get("mechanism_id"),
                             "state": job.state, "blocked_reason": job.blocked_reason})
    return out


def checkpoint(mem: M.ResearchMemory, queue=None, *, catalog: Optional[dict] = None,
               context: Optional[dict] = None,
               global_frontier: Optional[dict] = None) -> dict:
    """The resumable state of the agent, in the fields the operating brief names."""
    cat = catalog if catalog is not None else load_catalog()
    fr = frontier(mem, catalog=cat)
    ctx = context or {}
    jobs = _mechanism_jobs(queue)
    rows = {r["mechanism_id"]: r for r in fr.get("rows") or []}
    ranked = fr.get("ranked") or []
    gfr = global_frontier if global_frontier is not None else global_frontier_for(mem, cat)
    GF = None
    deferred: list = []
    if gfr is not None:
        from . import global_frontier as GF
        admitted = []
        for r in ranked:
            cmp = GF.mechanism_comparison(gfr, r["mechanism_id"])
            (admitted if cmp["admitted"] else deferred).append(
                {**r, "global_opportunity_cost": cmp})
        ranked = admitted

    executed = mem.list_hypotheses(generation_method=GENERATION_METHOD, limit=5000)
    qualified = [{"mechanism_id": str(h["economic_family"])[len(FAMILY_PREFIX):],
                  "settled_at": h.get("settled_at"),
                  "human_gate": HUMAN_GATE_REGISTRATION}
                 for h in executed if h.get("outcome") == r59.HO_QUALIFIED]
    closed_exec = [{"family_id": str(h["economic_family"])[len(FAMILY_PREFIX):],
                    "verdict": (h.get("robustness") or {}).get("verdict"),
                    "why": (h.get("robustness") or {}).get("why"),
                    "settled_at": h.get("settled_at"), "source": "EXECUTED_BY_AGENT"}
                   for h in executed if h.get("outcome") == r59.HO_NO_ALPHA_EVIDENCE]
    ledger = [{"family_id": f.get("family_id"), "mechanism_class": f.get("mechanism_class"),
               "verdict": f.get("verdict"), "source": "LEDGER"}
              for f in (cat or {}).get("closed_mechanisms") or []]

    if jobs["running"]:
        cur = {"action": "EXECUTING", **jobs["running"][0]}
    elif ranked:
        top = ranked[0]
        cur = {"action": ("EXECUTE" if top["executor_state"] == EX_READY
                          else "BUILD_EXECUTOR"),
               "mechanism_id": top["mechanism_id"], "title": top["title"],
               "domain": top["domain"], "priority_score": top["score"]["total"],
               "executor_state": top["executor_state"],
               "build_order": (None if top["executor_state"] == EX_READY
                               else build_order(find(cat, top["mechanism_id"]) or {}))}
        if top.get("global_opportunity_cost"):
            cur["global_opportunity_cost"] = top["global_opportunity_cost"]
    elif deferred:
        cur = {"action": ST_DEFERRED_GLOBAL,
               "deferred_mechanisms": [{"mechanism_id": r["mechanism_id"],
                                        "verdict": r["global_opportunity_cost"]["verdict"],
                                        "why": r["global_opportunity_cost"]["why"]}
                                       for r in deferred],
               "next_global_action": (gfr or {}).get("next_global_action"),
               "note": "every eligible mechanism loses to a higher-value action in the global "
                       "multi-asset top five"}
    else:
        cur = {"action": "NO_ELIGIBLE_MECHANISM",
               "note": "every declared mechanism is settled, refused or human-gated; the "
                       "next act is to declare new mechanisms from the economic frontier"}

    gates = [{"gate": "PURCHASE", "mechanism_id": mid, "reasons": rows[mid]["reasons"]}
             for mid in (fr.get("by_status") or {}).get(ST_HUMAN_GATE, [])]
    if GF is not None:
        for g in gates:
            g["global_comparison"] = GF.mechanism_comparison(gfr, g["mechanism_id"])
    gates += [{"gate": "PROSPECTIVE_REGISTRATION", **q} for q in qualified]
    if GF is not None:
        by_id = {c["candidate_id"]: c for c in gfr.get("candidates") or []}
        for cid in (gfr.get("global_top_ids") or [])[:GF.TOP_N]:
            na = by_id[cid]["next_best_action"]
            if na.get("requires_human"):
                gates.append({"gate": "GLOBAL_FRONTIER_HUMAN_DECISION", "candidate_id": cid,
                              "global_rank": by_id[cid]["global_rank"],
                              "asset_class": by_id[cid]["asset_class"], "kind": na.get("kind"),
                              "description": na.get("description"),
                              "human_gate": na.get("human_gate")})

    domains: dict = {}
    for r in fr.get("rows") or []:
        d = domains.setdefault(r["domain"], {"eligible": [], "settled": [], "refused": [],
                                             "human_gated": [], "best_priority": None})
        key = ("eligible" if r["status"] == ST_ELIGIBLE else
               "human_gated" if r["status"] == ST_HUMAN_GATE else
               "refused" if r["status"] in REFUSED_STATUSES else "settled")
        d[key].append(r["mechanism_id"])
        if r["status"] == ST_ELIGIBLE:
            d["best_priority"] = max(d["best_priority"] or 0.0, r["score"]["total"])
    live = list((cat or {}).get("live_candidates") or [])
    live_domains = {c.get("domain") for c in evidenced_live_candidates(cat)}
    best = sorted(((dom, v) for dom, v in domains.items() if v["best_priority"] is not None),
                  key=lambda kv: (-(1 if kv[0] in live_domains else 0),
                                  -(kv[1]["best_priority"] or 0.0)))
    estimate = {
        "domain": best[0][0] if best else None,
        "why": (("the only domain holding a surviving historical candidate (%s), and its best "
                 "eligible mechanism scores %.4f" % (", ".join(c.get("candidate_id") for c in live
                                                             if c.get("domain") == best[0][0]),
                                                    best[0][1]["best_priority"]))
                if best and best[0][0] in live_domains else
                ("highest-priority eligible mechanism %.4f" % best[0][1]["best_priority"])
                if best else "no eligible mechanism"),
        "ranking_by_domain": [{"domain": dom, "best_priority": v["best_priority"],
                               "eligible": len(v["eligible"]), "settled": len(v["settled"])}
                              for dom, v in best],
        "class_closed_counts": fr.get("class_closed_counts")}

    return {
        "schema": "alpha_agent_checkpoint/1",
        "calculation_owner": CALCULATION_OWNER,
        "catalog_path": fr.get("catalog_path"), "catalog_hash": fr.get("catalog_hash"),
        "ACTIVE_CANDIDATES": {"historical_survivors": [
            {k: c.get(k) for k in ("candidate_id", "sibling_id", "domain", "state",
                                   "historical_evidence")} for c in live],
            "qualified_by_agent_awaiting_human_gate": qualified},
        "TRUE_FORWARD_CANDIDATES": ctx.get("forward") or [
            {"candidate_id": c.get("candidate_id"), "state": c.get("state"),
             "state_source": "catalog declaration; no forward reader was injected"}
            for c in live],
        "CLOSED_MECHANISMS": ledger + closed_exec,
        "CURRENT_RESEARCH_ACTION": cur,
        "NEXT_RANKED_OPPORTUNITIES": [
            {"rank": i + 1, "mechanism_id": r["mechanism_id"], "domain": r["domain"],
             "mechanism_class": r["mechanism_class"], "priority_score": r["score"]["total"],
             "executor_state": r["executor_state"]} for i, r in enumerate(ranked[:12])],
        "HUMAN_GATES": gates,
        "DATA_GAPS": (cat or {}).get("data_gaps") or [],
        "LATEST_RESEARCH_COMMITS": ctx.get("commits"),
        "CURRENT_INFORMATION_FRONTIER": {"by_domain": domains,
                                         "by_status": fr.get("by_status")},
        "CURRENT_BEST_ESTIMATE_OF_WHERE_ALPHA_IS_MOST_LIKELY": estimate,
        "REFUSED": [{"mechanism_id": r["mechanism_id"], "status": r["status"],
                     "reasons": r["reasons"]} for r in fr.get("rows") or []
                    if r["status"] in REFUSED_STATUSES],
        "GLOBAL_MULTI_ASSET_FRONTIER": (GF.checkpoint_block(gfr) if GF is not None
                                        else {"state": "NOT_DECLARED"}),
        "DEFERRED_BY_GLOBAL_OPPORTUNITY_COST": [
            {"mechanism_id": r["mechanism_id"], "verdict": r["global_opportunity_cost"]["verdict"],
             "why": r["global_opportunity_cost"]["why"]} for r in deferred],
        "queue": jobs,
        "resume": "the queue and ResearchMemory are SQLite and this checkpoint is rewritten "
                  "every iteration; any session resumes by running "
                  "scripts/run_r59_autonomous_engine.py --continuous",
        "human_gates_are_the_only_stops": True,
        "safety": dict(r59.SAFETY),
    }


def write_checkpoint(mem: M.ResearchMemory, queue=None, *, catalog: Optional[dict] = None,
                     context: Optional[dict] = None) -> Path:
    cat = catalog if catalog is not None else load_catalog()
    gfr = global_frontier_for(mem, cat)
    if gfr is not None:
        from . import global_frontier as GF
        GF.write(gfr)
    return r59.write_artifact(CHECKPOINT_NAME,
                              checkpoint(mem, queue, catalog=cat, context=context,
                                         global_frontier=gfr),
                              subdir=CHECKPOINT_SUBDIR)


__all__ = ["CALCULATION_OWNER", "SOURCE", "PNL_GATE_FIELDS", "WORK_CONDITIONS",
           "MECHANISM_CLASSES", "PRIORITY_DOMAINS", "EXECUTOR_VERDICTS", "load_catalog",
           "pnl_gate", "distinctness", "score", "assess", "frontier", "candidates",
           "seed_closed", "execute_job", "record_execution", "checkpoint",
           "write_checkpoint", "executor_state", "is_mechanism_mandate"]
