"""alpha_agent.r59.global_frontier - ONE global multi-asset alpha frontier.

THE ERROR THIS CLOSES
    The mechanism frontier (``alpha_agent.r59.mechanisms``) ranks the economic
    mechanisms declared in its catalog and nothing else. Its whole view of the
    rest of the estate was six hand-typed ``live_candidates``, one of which
    carried historical evidence. On 2026-09-14 the estate actually held about
    120 candidate identities across twelve owners - the R46 prospective
    tournament (45 challengers and 7 adopted shadows), the canonical forward
    registry, the R58 freezes, the R63 and R64 candidate records, the Alpha
    Recovery forward package, the frozen hypotheses in ResearchMemory - in every
    asset class. A newly declared equity mechanism could therefore rank first
    only because an FX, credit, volatility or rates candidate lived in an older
    owner the ranking never read.

WHAT THIS MODULE IS
    An aggregation and reconciliation layer over the EXISTING owners, which it
    reads by path and never writes. The catalog's ``global_reconciliation``
    section declares which owner identities make up each economic opportunity,
    the evidence citations, and the three judgement inputs no artifact
    measures. Every STATE - forward counts, accrual states, promotion distance,
    executed verdicts, owner classifications - is read from its owner on every
    build. An owner identity that no opportunity claims is UNRECONCILED and the
    frontier is not COMPLETE, so a candidate can never silently disappear.

    It ranks every open opportunity from zero with one score that is blind to
    asset class, release, declaration date and executor state, and it answers,
    for every proposed research task and every purchase, whether that action
    beats advancing the global top five.

WHAT IT IS NOT
    Not a second research memory, candidate registry, forward ledger, scorer,
    governor or allocator. It computes no return statistic, registers nothing,
    promotes nothing, buys nothing and writes exactly one artifact: this
    frontier, beside the agent checkpoint.
"""
from __future__ import annotations

import inspect
import math
import os
from pathlib import Path
from typing import Optional

from .. import r59
from ..r46 import CAMPAIGN_ID as R46_CAMPAIGN_ID
from ..r46 import RESEARCH_ROOT as R46_RESEARCH_ROOT
from ..r46 import contract as C46
from ..r51 import promotion_frontier as PF
from ..r52 import RUNTIME_ROOT as R52_RUNTIME_ROOT
from . import mechanisms as MX
from . import memory as M

CALCULATION_OWNER = "alpha_agent.r59.global_frontier"
SCHEMA = "alpha_agent_global_multi_asset_frontier/1"
RECONCILIATION_SCHEMA = "alpha_agent_global_reconciliation/1"
CATALOG_SECTION = "global_reconciliation"
ARTIFACT_NAME = "global_multi_asset_frontier.json"
ARTIFACT_SUBDIR = MX.CHECKPOINT_SUBDIR

QUESTION = ("If all research capacity and investable capital were uncommitted today, where "
            "across all asset classes is the strongest after-cost P&L opportunity?")
OPPORTUNITY_COST_QUESTION = ("WHY IS THIS A BETTER USE OF RESEARCH TIME THAN ADVANCING THE "
                             "CURRENT TOP-RANKED OPPORTUNITY IN ANOTHER ASSET CLASS?")

# --------------------------------------------------------------------------- #
# Asset classes. An asset class may never disappear from the frontier.
# --------------------------------------------------------------------------- #
REQUIRED_ASSET_CLASSES = ("US_EQUITY", "EQUITY_INDEX", "FX", "RATES", "COMMODITIES",
                          "VOLATILITY", "CREDIT", "CRYPTO", "CROSS_ASSET")

#: Every asset label an owner uses, mapped onto the nine required classes.
ASSET_CLASS_ALIASES = {
    "US_EQUITY": "US_EQUITY",
    "EQUITY_INDEX": "EQUITY_INDEX", "EQUITY_INDEX_FUTURES": "EQUITY_INDEX", "US_ETF": "EQUITY_INDEX",
    "FX": "FX", "FX_FUTURES": "FX", "FX_SPOT": "FX",
    "RATES": "RATES", "RATES_FUTURES": "RATES",
    "COMMODITIES": "COMMODITIES", "COMMODITY": "COMMODITIES", "COMMODITY_FUTURES": "COMMODITIES",
    "VOLATILITY": "VOLATILITY", "VOLATILITY_FUTURES": "VOLATILITY",
    "CREDIT": "CREDIT", "CREDIT_PROXY": "CREDIT",
    "CRYPTO": "CRYPTO", "CRYPTO_MARKET_STRUCTURE": "CRYPTO",
    "CROSS_ASSET": "CROSS_ASSET", "MULTI_ASSET_FUTURES": "CROSS_ASSET", "FUTURES": "CROSS_ASSET",
}

# --------------------------------------------------------------------------- #
# The owners reconciled. Nothing here is a store of its own.
# --------------------------------------------------------------------------- #
OW_R46 = "R46_TOURNAMENT"
OW_R46_ADOPTED = "R46_ADOPTED_CONTINUATION"
OW_REGISTRY = "FORWARD_REGISTRY"
OW_ACCRUAL = "FORWARD_ACCRUAL"
OW_R51 = "R51_PROMOTION_FRONTIER"
OW_R63 = "R63_CANDIDATES"
OW_R64 = "R64_CANDIDATES"
OW_R58 = "R58_FREEZES"
OW_AR = "ALPHA_RECOVERY_FORWARD_PACKAGE"
OW_R59 = "R59_CHALLENGERS"
OW_MEMORY = "RESEARCH_MEMORY_FORWARD_FROZEN"
OW_CATALOG = "MECHANISM_CATALOG"
OWNERS = (OW_R46, OW_R46_ADOPTED, OW_REGISTRY, OW_ACCRUAL, OW_R51, OW_R63, OW_R64, OW_R58,
          OW_AR, OW_R59, OW_MEMORY, OW_CATALOG)
#: An absent R59 challenger directory only means R59 froze nothing.
REQUIRED_OWNERS = tuple(o for o in OWNERS if o != OW_R59)
FORWARD_OWNERS = (OW_R46, OW_R46_ADOPTED, OW_REGISTRY, OW_ACCRUAL, OW_R58, OW_MEMORY, OW_R51)

#: File names inside the owners' roots. Mirrored, not imported, because this
#: module imports no api, r63 or r64 package; a test pins every mirror.
R46_LEADERBOARD = "R46_LEADERBOARD.json"
R46_CONTINUATION = "R46_6_1_ADOPTED_CONTINUATION.json"
R51_FRONTIER_ARTIFACT = "R51_PROMOTION_FRONTIER.json"
ACCRUAL_PROJECTION = "accrual_projection.json"
REGISTRATIONS_SUBDIR = "registrations"
CHALLENGERS_SUBDIR = "challengers"
R58_FORWARD = ("results", "r58_forward_challengers.json")
AR_FORWARD_PACKAGE = ("results", "forward_candidates.json")

DATA_ROOT_ENV = "PAPER_TRADER_GLOBAL_FRONTIER_DATA_ROOT"
DEFAULT_DATA_ROOT = Path(r"D:\Stock_Prediction_app_data")
REGISTRY_DIR_ENV = "PAPER_TRADER_FORWARD_CHALLENGER_REGISTRY_DIR"
ACCRUAL_DIR_ENV = "PAPER_TRADER_CANONICAL_FORWARD_ACCRUAL_DIR"
R63_ROOT_ENV = "PAPER_TRADER_R63_RESEARCH_ROOT"
R64_ROOT_ENV = "PAPER_TRADER_R64_RESEARCH_ROOT"
R58_ROOT_ENV = "PAPER_TRADER_R58_RESEARCH_ROOT"
ALPHA_RECOVERY_ROOT_ENV = "PAPER_TRADER_ALPHA_RECOVERY_RESEARCH_ROOT"

# --------------------------------------------------------------------------- #
# Declaration vocabulary
# --------------------------------------------------------------------------- #
DISP_OPEN, DISP_CLOSED, DISP_CONTROL = "OPEN", "CLOSED", "CONTROL"
DISPOSITIONS = (DISP_OPEN, DISP_CLOSED, DISP_CONTROL)

NA_ACCRUE = "ACCRUE_FORWARD_EVIDENCE"
NA_OBSERVE = "CONTINUE_OPERATIONAL_OBSERVATION"
NA_REGISTER = "HUMAN_GATED_FORWARD_REGISTRATION"
NA_HUMAN_PREREG = "HUMAN_AUTHORISED_PREREGISTRATION"
NA_PURCHASE = "DATA_PURCHASE_DECISION"
NA_BUILD = "PREREGISTER_BUILD_AND_EXECUTE"
NA_HISTORICAL = "PREREGISTERED_HISTORICAL_TEST"
NA_DATA_HOLD = "RESOLVE_DATA_HOLD"
NEXT_ACTION_KINDS = (NA_ACCRUE, NA_OBSERVE, NA_REGISTER, NA_HUMAN_PREREG, NA_PURCHASE,
                     NA_BUILD, NA_HISTORICAL, NA_DATA_HOLD)
#: Actions that consume no research capacity: the owners accrue the evidence.
PASSIVE_KINDS = (NA_ACCRUE, NA_OBSERVE)
#: Actions only a human can take.
HUMAN_KINDS = (NA_REGISTER, NA_HUMAN_PREREG, NA_PURCHASE)
#: Actions the agent itself can take.
AGENT_KINDS = (NA_BUILD, NA_HISTORICAL, NA_DATA_HOLD)

UNTOUCHED = {"CONFIRMED": 1.0, "PARTIAL": 0.6, "NONE": 0.35, "FAILED": 0.0}
MULTIPLICITY = {"PASS": 1.0, "NOT_RUN": 0.8, "FAIL": 0.6}
PIT = {"PIT_TRUE": 1.0, "PIT_MARKET_OBSERVABLE": 1.0, "PIT_BY_DECLARED_LAG": 0.8,
       "METHODOLOGY_BACKFILL": 0.6, "UNESTABLISHED": 0.2}
JUDGEMENT_KEYS = ("diversification", "liquidity_capacity", "probability_alive")
HISTORICAL_NUMBERS = ("net_after_cost_pa", "t_stat", "increment_pa", "increment_t", "max_drawdown")
MIN_TEXT = MX.MIN_FIELD_CHARS

#: State belongs to its owner. A declaration that carries any of these keys is
#: a second forward ledger in disguise and is refused.
FORBIDDEN_STATE_KEYS = frozenset((
    "predictions_emitted", "matured_observations", "pending_observations",
    "effective_independent", "effective_independent_observations",
    "forward_predictions_emitted", "forward_predictions_matured", "net_alpha_bps",
    "current_state", "forward_state", "forward_observations", "global_rank",
    "opportunity_cost_score", "accrual_state", "current_accrual_state",
    "lifecycle_state", "registered_at", "capital_eligibility"))

# Candidate states ----------------------------------------------------------- #
CS_LIVE = "LIVE_CANDIDATE"
CS_TRUE_FORWARD = "TRUE_FORWARD"
CS_FORWARD_PENDING = "FORWARD_PENDING"
CS_RESEARCH_READY = "RESEARCH_READY"
CS_HUMAN_GATE = "HUMAN_GATE"
CS_BLOCKED = "BLOCKED"
CS_CLOSED = "CLOSED"
CS_CONTROL = "CONTROL"
UNRANKED_STATES = (CS_CLOSED, CS_CONTROL)

# Asset-class states ----------------------------------------------------------- #
AS_LIVE, AS_TRUE_FORWARD, AS_FORWARD_PENDING = "LIVE_CANDIDATE", "TRUE_FORWARD", "FORWARD_PENDING"
AS_RESEARCH_READY, AS_BLOCKED, AS_EXHAUSTED = "RESEARCH_READY", "BLOCKED", "EXHAUSTED"
ASSET_CLASS_STATES = (AS_LIVE, AS_TRUE_FORWARD, AS_FORWARD_PENDING, AS_RESEARCH_READY,
                      AS_BLOCKED, AS_EXHAUSTED)

# Frontier states -------------------------------------------------------------- #
ST_COMPLETE = "COMPLETE"
ST_NOT_DECLARED = "NOT_DECLARED"
ST_INVALID = "BLOCKED_INVALID_RECONCILIATION"
ST_ESTATE = "BLOCKED_ESTATE_UNREADABLE"
ST_UNRECONCILED = "BLOCKED_UNRECONCILED"
ST_CONFLICT = "BLOCKED_UNRESOLVED_OWNER_CONFLICT"

# Evidence-quality labels ------------------------------------------------------ #
EQ_GOOD_INCOMPLETE = "GOOD_STRATEGY_INCOMPLETE_EVIDENCE"
EQ_GOOD_MATURE = "GOOD_STRATEGY_MATURE_EVIDENCE"
EQ_BAD_COMPLETE = "BAD_STRATEGY_COMPLETE_EVIDENCE"
EQ_UNPROVEN = "UNPROVEN_STRATEGY_INCOMPLETE_EVIDENCE"
EQ_CLOSED = "CLOSED_BY_OWNER_EVIDENCE"
EQ_CONTROL = "CONTROL_NOT_A_CANDIDATE"

# Comparison verdicts ---------------------------------------------------------- #
V_BEATS = "BEATS_GLOBAL_TOP5"
V_LOSES = "LOSES_TO_GLOBAL_TOP5"
V_PURCHASE_JUSTIFIED = "PURCHASE_GLOBALLY_JUSTIFIED"
V_PURCHASE_NOT_JUSTIFIED = "PURCHASE_NOT_GLOBALLY_JUSTIFIED"
V_FRONTIER_INCOMPLETE = "GLOBAL_FRONTIER_INCOMPLETE"
V_NOT_RECONCILED = "NOT_IN_GLOBAL_FRONTIER"
V_CLOSED = "CANDIDATE_CLOSED"

MANUAL_CAPITAL_GATE = ("MANUAL_PROMOTION_AND_CAPITAL_APPROVAL: the R46/R51 forward evidence "
                       "gates, R50 investability and a human decision; no automatic path exists")

# --------------------------------------------------------------------------- #
# THE SCORE. Zero-base: it takes the evidence, the judgement inputs, the forward
# projection and the next action - and nothing that names an asset class, a
# release, a declaration date or an executor.
# --------------------------------------------------------------------------- #
QUALITY_WEIGHTS = {
    "historical_return": 0.12,        # 1  historical after-cost return
    "statistical_evidence": 0.10,     # 2  statistical evidence, discounted by multiplicity
    "untouched_evidence": 0.12,       # 3  untouched / independent evidence
    "incremental_information": 0.14,  # 5  increment over the incumbent or control
    "diversification": 0.08,          # 6  portfolio diversification value
    "drawdown_risk": 0.08,            # 7  drawdown / risk
    "cost_survivability": 0.10,       # 8  turnover / transaction cost
    "liquidity_capacity": 0.06,       # 9  liquidity / capacity at the paper NAV
    "pit_integrity": 0.08,            # 10 point-in-time integrity
    "probability_alive": 0.12,        # 15 probability the mechanism is still alive
}
COMPLETENESS_WEIGHTS = {
    "forward_evidence": 0.45,         # 4  TRUE_FORWARD evidence against its floor
    "untouched_resolved": 0.20,
    "multiplicity_resolved": 0.15,
    "evidence_maturity": 0.20,        # 11 evidence maturity
}
UNKNOWN_PRIOR = {"historical_return": 0.25, "statistical_evidence": 0.25,
                 "incremental_information": 0.35, "drawdown_risk": 0.5,
                 "cost_survivability": 0.5, "pit_integrity": 0.5}
RETURN_SCALE = 0.10
T_SCALE = 4.0
INCREMENT_T_SCALE = 3.0
DRAWDOWN_SCALE = 0.60
COST_SURVIVES, COST_FAILS = 1.0, 0.3
#: Forward evidence moves the probability that a mechanism is alive only once it
#: is decisive: two observations never outrank five hundred.
DECISIVE_FORWARD_EFFECTIVE = 5.0
FORWARD_T_DECISIVE = 2.0
ALIVE_ON_BAD_FORWARD = 0.6
ALIVE_ON_GOOD_FORWARD = 1.15
NEXT_ACTION_BASE = 0.5                # 16 expected value of the next action
MATURITY_BASE = 0.85                  # 12 remaining qualification burden
MAX_COST_PENALTY = 0.25               # 13 data cost, 14 implementation cost - bounded
IMPLEMENTATION_DAYS_HORIZON = MX.IMPLEMENTATION_DAYS_HORIZON
DATA_COST_USD_HORIZON = MX.DATA_COST_USD_HORIZON
GOOD_QUALITY = 0.50
MATURE_COMPLETENESS = 0.60
TOP_N = 5
#: Inputs the score must never see.
EXCLUDED_SCORE_INPUTS = ("asset_class", "information_asset_classes", "release", "declared_at",
                         "domain", "executor", "executor_state", "members", "candidate_id")

BLOCKED_FORWARD_STATES = ("DATA_BLOCKED", "RETIRED", "CALLED_PIT_BLOCKED", "INTEGRITY_BLOCKED",
                          "RETIRED_UNTIL_DATA_AVAILABLE")

LEADERBOARD_FIELDS = ("challenger_id", "horizon", "asset_class", "family", "state",
                      "source_release", "forward_predictions_emitted",
                      "forward_predictions_matured", "effective_independent", "net_alpha_bps",
                      "t_stat", "blocked_reason", "next_evidence_gate")
R51_FIELDS = ("sleeve_id", "state", "challenger_ids", "weeks_to_evidence_floor",
              "exact_missing_gate", "forward_predictions_emitted", "forward_predictions_matured",
              "best_effective_independent", "registry_blocker")


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
def _num(v) -> Optional[float]:
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    f = float(v)
    return f if math.isfinite(f) else None


def _int(v) -> int:
    f = _num(v)
    return int(f) if f is not None else 0


def _clip01(x) -> float:
    return max(0.0, min(1.0, float(x)))


def canonical_asset_class(label) -> Optional[str]:
    return ASSET_CLASS_ALIASES.get(str(label or "").strip().upper())


def declared(catalog: Optional[dict]) -> bool:
    return isinstance((catalog or {}).get(CATALOG_SECTION), dict)


def reconciliation(catalog: Optional[dict]) -> dict:
    return dict((catalog or {}).get(CATALOG_SECTION) or {})


def _forbidden_keys(obj, found=None) -> set:
    found = set() if found is None else found
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in FORBIDDEN_STATE_KEYS:
                found.add(k)
            _forbidden_keys(v, found)
    elif isinstance(obj, list):
        for v in obj:
            _forbidden_keys(v, found)
    return found


# --------------------------------------------------------------------------- #
# The estate - read only
# --------------------------------------------------------------------------- #
def estate_paths() -> dict:
    """Where each owner keeps what this frontier reads. An owner's own root
    override wins; ``PAPER_TRADER_GLOBAL_FRONTIER_DATA_ROOT`` relocates the rest."""
    data_env = os.environ.get(DATA_ROOT_ENV)
    data = Path(data_env) if data_env else DEFAULT_DATA_ROOT

    def owned(env: str, leaf: str) -> Path:
        v = os.environ.get(env)
        return Path(v) if v else data / leaf

    return {
        "r46_campaign": (data / R46_RESEARCH_ROOT.name / R46_CAMPAIGN_ID) if data_env
        else R46_RESEARCH_ROOT / R46_CAMPAIGN_ID,
        "r52_runtime": (data / R52_RUNTIME_ROOT.name) if data_env else R52_RUNTIME_ROOT,
        "forward_registry": owned(REGISTRY_DIR_ENV, "forward_challenger_registry"),
        "forward_accrual": owned(ACCRUAL_DIR_ENV, "canonical_forward_accrual"),
        "r63_root": owned(R63_ROOT_ENV, "r63_information_sensitivity"),
        "r64_root": owned(R64_ROOT_ENV, "r64_information_directed_alpha"),
        "r58_root": owned(R58_ROOT_ENV, "r58_orthogonal_alpha"),
        "alpha_recovery_root": owned(ALPHA_RECOVERY_ROOT_ENV, "alpha_recovery_offensive"),
        "r59_root": r59.research_root(),
    }


def _dir_records(directory: Path) -> Optional[list]:
    if not directory.exists():
        return None
    out = []
    for p in sorted(directory.glob("*.json")):
        body = r59.read_json(p)
        if isinstance(body, dict):
            out.append(body)
    return out


def _candidate_record(r: dict) -> dict:
    spec = r.get("forward_specification") or {}
    return {"challenger_id": r.get("challenger_id"),
            "classification": r.get("classification"),
            "cell_id": r.get("cell_id"),
            "asset_class": r.get("asset_class") or spec.get("scope"),
            "verdict": r.get("r64_verdict") or r.get("verdict"),
            "binding_failure": r.get("binding_failure") or r.get("strongest_failure_mode"),
            "freeze_record_hash": r.get("freeze_record_hash")}


def _registration(r: dict) -> dict:
    ident = r.get("identity") or {}
    clock = r.get("observation_clock") or {}
    return {"challenger_id": r.get("challenger_id"),
            "identity_hash": ident.get("identity_hash"),
            "asset_class": r.get("asset_class"),
            "horizon_sessions": r.get("horizon_sessions"),
            "release": ident.get("release"),
            "registration_session": r.get("registration_session"),
            "first_eligible_observation_session": clock.get("first_eligible_observation_session"),
            "lifecycle_at_registration": (r.get("lifecycle_at_registration") or {}).get(
                "lifecycle_state")}


def _adopted(cont: Optional[dict], leaderboard_rows: list) -> dict:
    """Adopted prior-release shadows, as the R46-owned continuation reports them."""
    out: dict = {}
    if isinstance(cont, dict):
        lanes = cont.get("lane_results") or {}
        summary = (cont.get("summary") or {}).get("by_adopted_challenger") or {}
        for sid, lane in PF.ADOPTED_CONTINUATION_LANES.items():
            lr, s = lanes.get(lane) or {}, summary.get(sid) or {}
            out[sid] = {"lane": lane, "lifecycle": lr.get("lifecycle"),
                        "next_decision_date": lr.get("next_decision_date"),
                        "emitted": _int(s.get("emitted")), "scored": _int(s.get("scored"))}
        for sid, s in summary.items():
            out.setdefault(sid, {"lane": None, "lifecycle": None, "next_decision_date": None,
                                 "emitted": _int((s or {}).get("emitted")),
                                 "scored": _int((s or {}).get("scored"))})
    for r in leaderboard_rows:
        if r.get("source_release") not in (None, "R46") and r["challenger_id"] not in out:
            out[r["challenger_id"]] = {"lane": None, "lifecycle": r.get("state"),
                                       "next_decision_date": None,
                                       "emitted": _int(r.get("forward_predictions_emitted")),
                                       "scored": _int(r.get("forward_predictions_matured"))}
    return out


def _frozen_challenger_id(h: dict) -> str:
    fc = h.get("forward_challenger")
    if isinstance(fc, dict) and fc.get("challenger_id"):
        return str(fc["challenger_id"])
    title = str(h.get("title") or "").strip()
    return title.split()[-1] if title else str(h.get("economic_family") or h.get("hypothesis_id"))


def load_estate(mem: Optional[M.ResearchMemory] = None, *, catalog: Optional[dict] = None,
                paths: Optional[dict] = None) -> dict:
    """Read every owner this frontier reconciles. Writes nothing."""
    p = {k: Path(v) for k, v in (paths or estate_paths()).items()}
    owners: dict = {}

    lb_path = p["r46_campaign"] / R46_LEADERBOARD
    lb = r59.read_json(lb_path)
    lb_rows = [{k: r.get(k) for k in LEADERBOARD_FIELDS}
               for r in (lb or {}).get("rows") or [] if r.get("challenger_id")]
    owners[OW_R46] = {"present": isinstance(lb, dict), "path": str(lb_path),
                      "generated_at": (lb or {}).get("built_at_utc"), "rows": lb_rows}

    cont_path = p["r46_campaign"] / R46_CONTINUATION
    cont = r59.read_json(cont_path)
    owners[OW_R46_ADOPTED] = {"present": isinstance(cont, dict), "path": str(cont_path),
                              "generated_at": (cont or {}).get("built_at_utc"),
                              "by_adopted": _adopted(cont, lb_rows)}

    reg_dir = p["forward_registry"] / REGISTRATIONS_SUBDIR
    regs = _dir_records(reg_dir)
    owners[OW_REGISTRY] = {"present": regs is not None, "path": str(reg_dir),
                           "rows": [_registration(r) for r in regs or [] if r.get("challenger_id")]}

    acc_path = p["forward_accrual"] / ACCRUAL_PROJECTION
    acc = r59.read_json(acc_path)
    owners[OW_ACCRUAL] = {"present": isinstance(acc, dict) or not (regs or []),
                          "path": str(acc_path), "generated_at": (acc or {}).get("generated_at"),
                          "by_identity": dict((acc or {}).get("by_identity") or {})}

    r51_path = p["r52_runtime"] / R51_FRONTIER_ARTIFACT
    r51 = r59.read_json(r51_path)
    fr51 = (r51 or {}).get("frontier") or {}
    owners[OW_R51] = {"present": isinstance(r51, dict), "path": str(r51_path),
                      "generated_at": (r51 or {}).get("refreshed_at_utc"),
                      "promotion_ready_count": fr51.get("promotion_ready_count"),
                      "rows": [{k: row.get(k) for k in R51_FIELDS} for row in fr51.get("rows") or []]}

    for owner, root in ((OW_R63, p["r63_root"]), (OW_R64, p["r64_root"])):
        d = root / CHALLENGERS_SUBDIR
        recs = _dir_records(d)
        owners[owner] = {"present": recs is not None, "path": str(d),
                         "records": [_candidate_record(r) for r in recs or [] if r.get("challenger_id")]}

    r58_path = p["r58_root"].joinpath(*R58_FORWARD)
    r58 = r59.read_json(r58_path)
    owners[OW_R58] = {"present": isinstance(r58, dict), "path": str(r58_path),
                      "records": [{"challenger_id": cid, "role": (v or {}).get("role"),
                                   "record_hash": (v or {}).get("record_hash")}
                                  for cid, v in sorted(((r58 or {}).get("frozen") or {}).items())]}

    ar_path = p["alpha_recovery_root"].joinpath(*AR_FORWARD_PACKAGE)
    ar = r59.read_json(ar_path)
    ar_rows: dict = {}
    for r in list((ar or {}).get("ready") or []) + list((ar or {}).get("survivors_not_qualified") or []):
        if r.get("challenger_id"):
            ar_rows[r["challenger_id"]] = _candidate_record(r)
    for r in _dir_records(p["alpha_recovery_root"] / CHALLENGERS_SUBDIR) or []:
        if r.get("challenger_id") and r["challenger_id"] not in ar_rows:
            ar_rows[r["challenger_id"]] = _candidate_record(r)
    owners[OW_AR] = {"present": isinstance(ar, dict), "path": str(ar_path),
                     "records": [ar_rows[k] for k in sorted(ar_rows)]}

    r59_dir = p["r59_root"] / CHALLENGERS_SUBDIR
    r59_recs = _dir_records(r59_dir)
    owners[OW_R59] = {"present": r59_recs is not None, "path": str(r59_dir),
                      "records": [{"challenger_id": r["challenger_id"],
                                   "withdrawn": bool(r.get("withdrawn"))}
                                  for r in r59_recs or [] if r.get("challenger_id")]}

    ro, mem_error = mem, None
    if ro is None:
        try:
            ro = M.open_memory_readonly()
        except Exception as exc:                        # noqa: BLE001 - absence is reported
            ro, mem_error = None, "%s: %s" % (type(exc).__name__, str(exc)[:200])
    frozen = []
    if ro is not None:
        for h in ro.list_hypotheses(outcome=r59.HO_FORWARD_FROZEN, limit=5000):
            frozen.append({"challenger_id": _frozen_challenger_id(h),
                           "hypothesis_id": h.get("hypothesis_id"),
                           "release": h.get("release"), "asset_class": h.get("asset_class")})
    owners[OW_MEMORY] = {"present": ro is not None, "path": str(M.memory_db_path()),
                         "error": mem_error, "records": frozen}

    cat = catalog if catalog is not None else MX.load_catalog()
    fr = MX.frontier(ro, catalog=cat) if cat else {"rows": [], "ranked": []}
    owners[OW_CATALOG] = {
        "present": bool(cat), "path": str(MX.catalog_path()),
        "catalog_hash": fr.get("catalog_hash"),
        "live_candidates": sorted(str(c.get("candidate_id"))
                                  for c in (cat or {}).get("live_candidates") or []
                                  if c.get("candidate_id")),
        "mechanisms": [{"mechanism_id": r["mechanism_id"], "status": r["status"],
                        "asset_class": r.get("asset_class"), "priority": r["score"]["total"],
                        "executor_state": r.get("executor_state")} for r in fr.get("rows") or []],
        "ranked": [r["mechanism_id"] for r in fr.get("ranked") or []],
        "closed_mechanisms": [{"family_id": f.get("family_id"), "asset_scope": f.get("asset_scope"),
                               "mechanism_class": f.get("mechanism_class"),
                               "verdict": f.get("verdict")}
                              for f in (cat or {}).get("closed_mechanisms") or []]}
    return {"schema": "alpha_agent_global_frontier_estate/1",
            "paths": {k: str(v) for k, v in p.items()}, "owners": owners}


def owner_identities(estate: dict) -> dict:
    """Every candidate identity each owner holds right now."""
    o = estate.get("owners") or {}

    def ids(owner: str, key: str) -> list:
        return sorted({str(r.get("challenger_id")) for r in (o.get(owner) or {}).get(key) or []
                       if r.get("challenger_id")})

    reg_rows = (o.get(OW_REGISTRY) or {}).get("rows") or []
    known = {r.get("identity_hash") for r in reg_rows}
    cat = o.get(OW_CATALOG) or {}
    r51_ids = set()
    for row in (o.get(OW_R51) or {}).get("rows") or []:
        if row.get("state") == "ALREADY_OPERATIONAL" and row.get("sleeve_id"):
            r51_ids.add(str(row["sleeve_id"]))
        r51_ids.update(str(c) for c in row.get("challenger_ids") or [])
    return {
        OW_R46: ids(OW_R46, "rows"),
        OW_R46_ADOPTED: sorted((o.get(OW_R46_ADOPTED) or {}).get("by_adopted") or {}),
        OW_REGISTRY: ids(OW_REGISTRY, "rows"),
        OW_ACCRUAL: sorted("ACCRUAL_IDENTITY:%s" % h
                           for h in (o.get(OW_ACCRUAL) or {}).get("by_identity") or {}
                           if h not in known),
        OW_R51: sorted(r51_ids),
        OW_R63: ids(OW_R63, "records"),
        OW_R64: ids(OW_R64, "records"),
        OW_R58: ids(OW_R58, "records"),
        OW_AR: ids(OW_AR, "records"),
        OW_R59: ids(OW_R59, "records"),
        OW_MEMORY: ids(OW_MEMORY, "records"),
        OW_CATALOG: sorted(set(cat.get("live_candidates") or [])
                           | {str(m.get("mechanism_id")) for m in cat.get("mechanisms") or []}),
    }


def owner_asset_labels(estate: dict) -> dict:
    """``identity -> canonical asset classes`` as the owners label them."""
    o = estate.get("owners") or {}
    out: dict = {}

    def add(i, label):
        ac = canonical_asset_class(label)
        if i and ac:
            out.setdefault(str(i), set()).add(ac)

    for r in (o.get(OW_R46) or {}).get("rows") or []:
        add(r.get("challenger_id"), r.get("asset_class"))
    for r in (o.get(OW_REGISTRY) or {}).get("rows") or []:
        add(r.get("challenger_id"), r.get("asset_class"))
    for owner in (OW_R63, OW_R64, OW_AR, OW_MEMORY):
        for r in (o.get(owner) or {}).get("records") or []:
            add(r.get("challenger_id"), r.get("asset_class"))
    for m in (o.get(OW_CATALOG) or {}).get("mechanisms") or []:
        add(m.get("mechanism_id"), m.get("asset_class"))
    return out


def owner_classifications(estate: dict) -> dict:
    """``identity -> {owner: the owner's own word for its state}``."""
    o = estate.get("owners") or {}
    out: dict = {}

    def put(i, owner, value):
        if i:
            prev = out.setdefault(str(i), {}).get(owner)
            out[str(i)][owner] = value if prev is None else "%s; %s" % (prev, value)

    for r in (o.get(OW_R46) or {}).get("rows") or []:
        if r.get("source_release") in (None, "R46"):
            put(r.get("challenger_id"), OW_R46, "%s h%s" % (r.get("state"), r.get("horizon")))
    for sid, a in ((o.get(OW_R46_ADOPTED) or {}).get("by_adopted") or {}).items():
        put(sid, OW_R46_ADOPTED, a.get("lifecycle"))
    acc = (o.get(OW_ACCRUAL) or {}).get("by_identity") or {}
    for r in (o.get(OW_REGISTRY) or {}).get("rows") or []:
        put(r.get("challenger_id"), OW_REGISTRY,
            (acc.get(r.get("identity_hash")) or {}).get("current_accrual_state") or "REGISTERED")
    for owner in (OW_R63, OW_R64, OW_AR):
        for r in (o.get(owner) or {}).get("records") or []:
            put(r.get("challenger_id"), owner, r.get("classification") or r.get("verdict"))
    for r in (o.get(OW_R58) or {}).get("records") or []:
        put(r.get("challenger_id"), OW_R58, "FROZEN:%s" % r.get("role"))
    for r in (o.get(OW_R59) or {}).get("records") or []:
        put(r.get("challenger_id"), OW_R59, "WITHDRAWN" if r.get("withdrawn") else "FROZEN")
    for r in (o.get(OW_MEMORY) or {}).get("records") or []:
        put(r.get("challenger_id"), OW_MEMORY, r59.HO_FORWARD_FROZEN)
    cat = o.get(OW_CATALOG) or {}
    for i in cat.get("live_candidates") or []:
        put(i, OW_CATALOG, "LISTED_LIVE_CANDIDATE")
    for m in cat.get("mechanisms") or []:
        put(m.get("mechanism_id"), OW_CATALOG, m.get("status"))
    for row in (o.get(OW_R51) or {}).get("rows") or []:
        if row.get("state") == "ALREADY_OPERATIONAL":
            put(row.get("sleeve_id"), OW_R51, row.get("state"))
    return out


# --------------------------------------------------------------------------- #
# Validation of the declaration
# --------------------------------------------------------------------------- #
def validate_reconciliation(rec: dict, catalog: Optional[dict] = None) -> list:
    """Every problem with the declared reconciliation. Pure."""
    problems: list = []
    if rec.get("schema") != RECONCILIATION_SCHEMA:
        problems.append("SCHEMA: %r is not %s" % (rec.get("schema"), RECONCILIATION_SCHEMA))
    ledger = {str(f.get("family_id")) for f in (catalog or {}).get("closed_mechanisms") or []}
    mechanism_ids = {str(m.get("mechanism_id")) for m in (catalog or {}).get("mechanisms") or []}
    seen, claimed = set(), {}
    for c in rec.get("candidates") or []:
        cid = str((c or {}).get("candidate_id") or "")
        if not cid or cid in seen:
            problems.append("CANDIDATE_ID_MISSING_OR_DUPLICATED: %r" % cid)
        seen.add(cid)
        state_keys = _forbidden_keys(c)
        if state_keys:
            problems.append("STATE_DECLARED: %s declares %s; state is read from its owner"
                            % (cid, sorted(state_keys)))
        if c.get("asset_class") not in REQUIRED_ASSET_CLASSES:
            problems.append("ASSET_CLASS_UNKNOWN: %s %r" % (cid, c.get("asset_class")))
        for ac in c.get("information_asset_classes") or []:
            if ac not in REQUIRED_ASSET_CLASSES:
                problems.append("INFORMATION_ASSET_CLASS_UNKNOWN: %s %r" % (cid, ac))
        if c.get("mechanism_class") not in MX.MECHANISM_CLASSES:
            problems.append("MECHANISM_CLASS_UNKNOWN: %s %r" % (cid, c.get("mechanism_class")))
        if not str(c.get("information_object") or "").strip():
            problems.append("INFORMATION_OBJECT_MISSING: %s" % cid)
        disp = c.get("disposition")
        if disp not in DISPOSITIONS:
            problems.append("DISPOSITION_UNKNOWN: %s %r" % (cid, disp))
        members = c.get("members")
        if not isinstance(members, list):
            problems.append("MEMBERS_NOT_A_LIST: %s" % cid)
            members = []
        if not members and not (disp == DISP_OPEN and str(c.get("source_artifact") or "").strip()):
            problems.append("NO_OWNER_MEMBER: %s claims no owner identity and cites no source "
                            "artifact" % cid)
        for m in members:
            if str(m) in claimed:
                problems.append("IDENTITY_CLAIMED_TWICE: %s by %s and %s" % (m, claimed[str(m)], cid))
            claimed.setdefault(str(m), cid)
        for fam in c.get("closed_families") or []:
            if str(fam) not in ledger and str(fam) not in mechanism_ids:
                problems.append("CLOSED_FAMILY_UNKNOWN: %s cites %r" % (cid, fam))
        hist = c.get("historical") or {}
        if len(str(hist.get("summary") or "").strip()) < MIN_TEXT:
            problems.append("HISTORICAL_SUMMARY_MISSING: %s" % cid)
        if disp in (DISP_CLOSED, DISP_CONTROL):
            if len(str(c.get("closed_reason") or "").strip()) < MIN_TEXT:
                problems.append("CLOSED_REASON_MISSING: %s" % cid)
            continue
        if disp != DISP_OPEN:
            continue
        if hist.get("untouched_confirmation") not in UNTOUCHED:
            problems.append("UNTOUCHED_UNKNOWN: %s %r" % (cid, hist.get("untouched_confirmation")))
        if hist.get("multiplicity") not in MULTIPLICITY:
            problems.append("MULTIPLICITY_UNKNOWN: %s %r" % (cid, hist.get("multiplicity")))
        if hist.get("pit") not in PIT:
            problems.append("PIT_UNKNOWN: %s %r" % (cid, hist.get("pit")))
        for k in HISTORICAL_NUMBERS:
            if hist.get(k) is not None and _num(hist.get(k)) is None:
                problems.append("HISTORICAL_NUMBER_INVALID: %s.%s" % (cid, k))
        if hist.get("cost_survives_2x") not in (True, False, None):
            problems.append("COST_SURVIVES_2X_INVALID: %s" % cid)
        judgement = c.get("judgement") or {}
        for k in JUDGEMENT_KEYS:
            v = _num(judgement.get(k))
            if v is None or not 0.0 <= v <= 1.0:
                problems.append("JUDGEMENT_INVALID: %s.%s" % (cid, k))
        if len(str(judgement.get("rationale") or "").strip()) < MIN_TEXT:
            problems.append("JUDGEMENT_RATIONALE_MISSING: %s" % cid)
        na = c.get("next_action") or {}
        kind = na.get("kind")
        if kind not in NEXT_ACTION_KINDS:
            problems.append("NEXT_ACTION_UNKNOWN: %s %r" % (cid, kind))
        if len(str(na.get("description") or "").strip()) < MIN_TEXT:
            problems.append("NEXT_ACTION_DESCRIPTION_MISSING: %s" % cid)
        for k in ("research_days", "data_cost_usd"):
            v = _num(na.get(k))
            if v is None or v < 0:
                problems.append("NEXT_ACTION_COST_INVALID: %s.%s" % (cid, k))
        gain = _num(na.get("information_gain"))
        if gain is None or not 0.0 <= gain <= 1.0:
            problems.append("INFORMATION_GAIN_INVALID: %s" % cid)
        if (kind in HUMAN_KINDS) != bool(str(na.get("human_gate") or "").strip()):
            problems.append("HUMAN_GATE_MISMATCH: %s kind %s human_gate %r"
                            % (cid, kind, na.get("human_gate")))
        if len(str(c.get("data_requirement") or "").strip()) < 10:
            problems.append("DATA_REQUIREMENT_MISSING: %s" % cid)
    for ac, d in (rec.get("asset_classes") or {}).items():
        if ac not in REQUIRED_ASSET_CLASSES:
            problems.append("ASSET_CLASS_SECTION_UNKNOWN: %r" % ac)
            continue
        ex = (d or {}).get("declared_exhaustion")
        if ex:
            mech = [str(x) for x in (ex.get("mechanisms") or [])]
            if not mech or any(x not in ledger and x not in seen for x in mech):
                problems.append("EXHAUSTION_MECHANISMS_INVALID: %s must name the exact closed "
                                "mechanisms" % ac)
            if len(str(ex.get("why") or "").strip()) < MIN_TEXT:
                problems.append("EXHAUSTION_REASON_MISSING: %s" % ac)
    for fam, ac in (rec.get("closed_family_asset_classes") or {}).items():
        if fam not in ledger or ac not in REQUIRED_ASSET_CLASSES:
            problems.append("CLOSED_FAMILY_OVERRIDE_INVALID: %s -> %s" % (fam, ac))
    return problems


# --------------------------------------------------------------------------- #
# Forward evidence, projected from its owners
# --------------------------------------------------------------------------- #
def _floor(horizon) -> int:
    floors = C46.FORWARD_EVIDENCE_GATES["min_effective_independent"]
    h = _num(horizon)
    if h is None:
        return int(min(floors.values()))
    key = min((k for k in sorted(floors) if h <= k), default=max(floors))
    return int(floors[key])


def forward_projection(members: list, estate: dict) -> dict:
    """What the forward owners say about these identities. Never a count of its own."""
    o = estate.get("owners") or {}
    ids = {str(m) for m in members}
    rows: list = []
    for r in (o.get(OW_R46) or {}).get("rows") or []:
        if r.get("challenger_id") in ids and r.get("source_release") in (None, "R46"):
            rows.append({"owner": OW_R46, "id": r["challenger_id"], "horizon": r.get("horizon"),
                         "state": r.get("state"),
                         "emitted": _int(r.get("forward_predictions_emitted")),
                         "matured": _int(r.get("forward_predictions_matured")),
                         "effective": _num(r.get("effective_independent")) or 0.0,
                         "net_alpha_bps": _num(r.get("net_alpha_bps")),
                         "t_stat": _num(r.get("t_stat")),
                         "next_evidence_gate": r.get("next_evidence_gate")})
    for sid, a in ((o.get(OW_R46_ADOPTED) or {}).get("by_adopted") or {}).items():
        if sid in ids:
            rows.append({"owner": OW_R46_ADOPTED, "id": sid, "horizon": 20,
                         "state": a.get("lifecycle"), "emitted": _int(a.get("emitted")),
                         "matured": _int(a.get("scored")), "effective": float(_int(a.get("scored"))),
                         "net_alpha_bps": None, "t_stat": None,
                         "next_evidence_gate": "next continuation decision %s"
                                               % a.get("next_decision_date")})
    acc = (o.get(OW_ACCRUAL) or {}).get("by_identity") or {}
    for r in (o.get(OW_REGISTRY) or {}).get("rows") or []:
        if r.get("challenger_id") in ids:
            a = acc.get(r.get("identity_hash")) or {}
            rows.append({"owner": OW_REGISTRY, "id": r["challenger_id"],
                         "identity_hash": r.get("identity_hash"),
                         "horizon": r.get("horizon_sessions"),
                         "state": a.get("current_accrual_state") or "NO_ACCRUAL_PROJECTION",
                         "emitted": _int(a.get("predictions_emitted")),
                         "matured": _int(a.get("matured_observations")),
                         "effective": _num(a.get("effective_independent_observations")) or 0.0,
                         "net_alpha_bps": None, "t_stat": None,
                         "next_evidence_gate": "first eligible observation session %s"
                                               % r.get("first_eligible_observation_session")})
    operational = False
    sleeves = []
    for row in (o.get(OW_R51) or {}).get("rows") or []:
        in_row = ids & ({str(row.get("sleeve_id"))} | {str(c) for c in row.get("challenger_ids") or []})
        if not in_row:
            continue
        sleeves.append({"sleeve_id": row.get("sleeve_id"), "state": row.get("state"),
                        "weeks_to_evidence_floor": row.get("weeks_to_evidence_floor"),
                        "exact_missing_gate": row.get("exact_missing_gate")})
        if row.get("state") == "ALREADY_OPERATIONAL" and str(row.get("sleeve_id")) in ids:
            operational = True
            rows.append({"owner": OW_R51, "id": row.get("sleeve_id"), "horizon": 20,
                         "state": row.get("state"),
                         "emitted": _int(row.get("forward_predictions_emitted")),
                         "matured": _int(row.get("forward_predictions_matured")),
                         "effective": _num(row.get("best_effective_independent")) or 0.0,
                         "net_alpha_bps": None, "t_stat": None,
                         "next_evidence_gate": row.get("exact_missing_gate")})
    best = max(rows, key=lambda r: (r["effective"], r["matured"], r["emitted"]), default=None)
    emitted = sum(r["emitted"] for r in rows)
    matured = sum(r["matured"] for r in rows)
    registered = bool(rows)
    blocked = registered and all(str(r["state"]) in BLOCKED_FORWARD_STATES for r in rows)
    floor = _floor(best["horizon"] if best else None)
    best_eff = float(best["effective"]) if best else 0.0
    t = best.get("t_stat") if best else None
    if not registered:
        signal = "NO_FORWARD_CLOCK"
    elif best_eff < DECISIVE_FORWARD_EFFECTIVE:
        signal = "NOT_YET_DECISIVE"
    elif t is not None and t >= FORWARD_T_DECISIVE:
        signal = "DECISIVE_POSITIVE"
    elif t is not None and t <= -FORWARD_T_DECISIVE:
        signal = "DECISIVE_NEGATIVE"
    else:
        signal = "INCONCLUSIVE"
    if not registered:
        summary = "NO_FORWARD_CLOCK: no owner holds a prospective registration or freeze"
    elif blocked:
        summary = "FORWARD_BLOCKED: every forward stream is %s" % sorted({str(r["state"]) for r in rows})
    elif operational:
        summary = "OPERATIONAL: %d emitted, %d matured under manual governance" % (emitted, matured)
    elif emitted:
        summary = ("TRUE_FORWARD_ACCRUING: %d emitted, %d matured; best cell %s h%s holds %.0f of "
                   "%d effective independent observations" % (emitted, matured, best["id"],
                                                               best["horizon"], best_eff, floor))
    else:
        summary = "FORWARD_PENDING: registered or frozen, no prediction emitted yet"
    return {"rows": rows, "registered": registered, "blocked": blocked, "operational": operational,
            "emitted": emitted, "matured": matured, "best_id": best["id"] if best else None,
            "best_horizon": best["horizon"] if best else None, "best_effective": best_eff,
            "best_t_stat": t, "best_net_alpha_bps": best.get("net_alpha_bps") if best else None,
            "floor": floor, "signal": signal, "summary_state": summary, "r51_sleeves": sleeves}


# --------------------------------------------------------------------------- #
# Scoring
# --------------------------------------------------------------------------- #
def quality_components(historical: dict, judgement: dict, forward: dict) -> dict:
    h = historical or {}
    ret, t, it, dd = (_num(h.get(k)) for k in ("net_after_cost_pa", "t_stat", "increment_t",
                                                "max_drawdown"))
    mult = MULTIPLICITY.get(h.get("multiplicity"), MULTIPLICITY["NOT_RUN"])
    c2 = h.get("cost_survives_2x")
    alive = _clip01(_num((judgement or {}).get("probability_alive")) or 0.0)
    if forward.get("best_effective", 0.0) >= DECISIVE_FORWARD_EFFECTIVE:
        if forward.get("signal") == "DECISIVE_NEGATIVE":
            alive *= ALIVE_ON_BAD_FORWARD
        elif forward.get("signal") == "DECISIVE_POSITIVE":
            alive = min(1.0, alive * ALIVE_ON_GOOD_FORWARD)
    return {
        "historical_return": UNKNOWN_PRIOR["historical_return"] if ret is None
        else _clip01(ret / RETURN_SCALE),
        "statistical_evidence": (UNKNOWN_PRIOR["statistical_evidence"] if t is None
                                 else _clip01(t / T_SCALE)) * mult,
        "untouched_evidence": UNTOUCHED.get(h.get("untouched_confirmation"), UNTOUCHED["NONE"]),
        "incremental_information": UNKNOWN_PRIOR["incremental_information"] if it is None
        else _clip01(it / INCREMENT_T_SCALE),
        "diversification": _clip01(_num((judgement or {}).get("diversification")) or 0.0),
        "drawdown_risk": UNKNOWN_PRIOR["drawdown_risk"] if dd is None
        else 1.0 - _clip01(abs(dd) / DRAWDOWN_SCALE),
        "cost_survivability": UNKNOWN_PRIOR["cost_survivability"] if c2 is None
        else (COST_SURVIVES if c2 else COST_FAILS),
        "liquidity_capacity": _clip01(_num((judgement or {}).get("liquidity_capacity")) or 0.0),
        "pit_integrity": PIT.get(h.get("pit"), UNKNOWN_PRIOR["pit_integrity"]),
        "probability_alive": alive,
    }


def completeness_components(historical: dict, forward: dict) -> dict:
    h = historical or {}
    u, m = h.get("untouched_confirmation"), h.get("multiplicity")
    if forward.get("matured", 0) > 0:
        maturity = 1.0
    elif forward.get("emitted", 0) > 0:
        maturity = 0.6
    elif forward.get("registered"):
        maturity = 0.4
    elif _num(h.get("t_stat")) is not None:
        maturity = 0.25
    else:
        maturity = 0.0
    floor = float(forward.get("floor") or 1)
    return {
        "forward_evidence": _clip01(float(forward.get("best_effective") or 0.0) / floor)
        if forward.get("registered") else 0.0,
        "untouched_resolved": 1.0 if u in ("CONFIRMED", "FAILED") else 0.5 if u == "PARTIAL" else 0.0,
        "multiplicity_resolved": 1.0 if m in ("PASS", "FAIL") else 0.0,
        "evidence_maturity": maturity,
    }


def score_candidate(*, historical: dict, judgement: dict, forward: dict,
                    next_action: dict) -> dict:
    """The one opportunity-cost score. It never sees an asset class, a release, a
    declaration date or an executor state."""
    qc = quality_components(historical, judgement, forward)
    cc = completeness_components(historical, forward)
    quality = sum(QUALITY_WEIGHTS[k] * qc[k] for k in QUALITY_WEIGHTS)
    completeness = sum(COMPLETENESS_WEIGHTS[k] * cc[k] for k in COMPLETENESS_WEIGHTS)
    na = next_action or {}
    gain = _clip01(_num(na.get("information_gain")) or 0.0)
    days = max(0.0, _num(na.get("research_days")) or 0.0)
    usd = max(0.0, _num(na.get("data_cost_usd")) or 0.0)
    next_value = NEXT_ACTION_BASE + (1.0 - NEXT_ACTION_BASE) * gain
    maturity = MATURITY_BASE + (1.0 - MATURITY_BASE) * completeness
    implementation = 1.0 - MAX_COST_PENALTY * _clip01(days / IMPLEMENTATION_DAYS_HORIZON)
    data_cost = 1.0 - MAX_COST_PENALTY * _clip01(usd / DATA_COST_USD_HORIZON)
    total = quality * next_value * maturity * implementation * data_cost
    return {"opportunity_cost_score": round(total, 4), "quality": round(quality, 4),
            "completeness": round(completeness, 4),
            "remaining_qualification_burden": round(1.0 - completeness, 4),
            "next_action_value": round(next_value, 4), "maturity_factor": round(maturity, 4),
            "implementation_cost_factor": round(implementation, 4),
            "data_cost_factor": round(data_cost, 4),
            "quality_components": {k: round(v, 4) for k, v in qc.items()},
            "completeness_components": {k: round(v, 4) for k, v in cc.items()}}


def score_is_blind() -> bool:
    """The score's inputs name none of the excluded attributes."""
    params = set(inspect.signature(score_candidate).parameters)
    return params == {"historical", "judgement", "forward", "next_action"} and \
        not params & set(EXCLUDED_SCORE_INPUTS)


def evidence_quality(quality: float, completeness: float, historical: dict) -> str:
    h = historical or {}
    resolved = h.get("untouched_confirmation") in ("CONFIRMED", "FAILED") and \
        h.get("multiplicity") in ("PASS", "FAIL")
    if quality >= GOOD_QUALITY:
        return EQ_GOOD_MATURE if completeness >= MATURE_COMPLETENESS else EQ_GOOD_INCOMPLETE
    return EQ_BAD_COMPLETE if resolved else EQ_UNPROVEN


def candidate_state(disposition: str, forward: dict, next_action: dict) -> str:
    if disposition == DISP_CLOSED:
        return CS_CLOSED
    if disposition == DISP_CONTROL:
        return CS_CONTROL
    if forward.get("operational"):
        return CS_LIVE
    if forward.get("emitted", 0) > 0:
        return CS_TRUE_FORWARD
    if forward.get("registered") and not forward.get("blocked"):
        return CS_FORWARD_PENDING
    kind = (next_action or {}).get("kind")
    if kind in HUMAN_KINDS:
        return CS_HUMAN_GATE
    if kind == NA_DATA_HOLD or forward.get("blocked"):
        return CS_BLOCKED
    return CS_RESEARCH_READY


def remaining_gates(historical: dict, forward: dict, next_action: dict) -> list:
    h, na = historical or {}, next_action or {}
    gates = []
    u, m = h.get("untouched_confirmation"), h.get("multiplicity")
    if u == "FAILED":
        gates.append("OVERTURN_FAILED_UNTOUCHED_EVIDENCE: the historical record failed on evidence "
                     "that did not choose it; only TRUE_FORWARD evidence can overturn it")
    elif u == "PARTIAL":
        gates.append("UNTOUCHED_CONFIRMATION: the lockbox agrees but was viewed before this "
                     "specification was chosen (post-selection)")
    elif u != "CONFIRMED":
        gates.append("UNTOUCHED_CONFIRMATION: no independent untouched window has confirmed it")
    if m == "FAIL":
        gates.append("MULTIPLICITY: the declared multiple-testing family refused the result; more "
                     "backtests on the same history cannot buy it back")
    elif m != "PASS":
        gates.append("MULTIPLICITY_NOT_RUN: the search that produced the rule has not been charged")
    if not forward.get("operational"):
        if not forward.get("registered"):
            gates.append("NO_FORWARD_CLOCK: no prospective registration or freeze exists, so no "
                         "TRUE_FORWARD evidence can accrue")
        elif forward.get("best_effective", 0.0) < forward.get("floor", 0):
            gates.append("FORWARD_EVIDENCE_FLOOR: best cell %s h%s holds %.0f of %d effective "
                         "independent observations (alpha_agent.r46.contract."
                         "FORWARD_EVIDENCE_GATES)" % (forward.get("best_id"),
                                                      forward.get("best_horizon"),
                                                      forward.get("best_effective", 0.0),
                                                      forward.get("floor", 0)))
    if na.get("human_gate"):
        gates.append("HUMAN_GATE: %s" % na["human_gate"])
    gates.append(MANUAL_CAPITAL_GATE)
    return gates


def _next_action(na: dict) -> dict:
    kind = (na or {}).get("kind")
    return {"kind": kind, "description": (na or {}).get("description"),
            "research_days": _num((na or {}).get("research_days")) or 0.0,
            "data_cost_usd": _num((na or {}).get("data_cost_usd")) or 0.0,
            "information_gain": _num((na or {}).get("information_gain")),
            "human_gate": (na or {}).get("human_gate"),
            "consumes_research_capacity": kind not in PASSIVE_KINDS,
            "executable_by_agent": kind in AGENT_KINDS,
            "requires_human": kind in HUMAN_KINDS}


def _conflicts(cid: str, disposition: str, members: list, classification: dict,
               mechanism_status: dict, forward: dict) -> list:
    out = []
    for m in members:
        st = (mechanism_status.get(m) or {}).get("status")
        if disposition == DISP_OPEN and st == MX.ST_SETTLED_CLOSED:
            out.append({"candidate_id": cid, "id": m,
                        "conflict": "OPEN_CANDIDATE_HOLDS_A_SETTLED_CLOSED_MECHANISM"})
        if disposition == DISP_CLOSED and st in (MX.ST_ELIGIBLE, MX.ST_HUMAN_GATE):
            out.append({"candidate_id": cid, "id": m,
                        "conflict": "CLOSED_CANDIDATE_HOLDS_AN_UNSETTLED_MECHANISM"})
        if disposition == DISP_CLOSED and any(
                str(v).startswith("READY_FOR_FORWARD_QUALIFICATION")
                for v in (classification.get(m) or {}).values()):
            out.append({"candidate_id": cid, "id": m,
                        "conflict": "CLOSED_CANDIDATE_HOLDS_A_READY_OWNER_RECORD"})
    if disposition == DISP_CLOSED and forward.get("emitted", 0) > 0 and not forward.get("blocked"):
        out.append({"candidate_id": cid, "id": forward.get("best_id"),
                    "conflict": "CLOSED_CANDIDATE_HAS_A_LIVE_FORWARD_CLOCK"})
    return out


# --------------------------------------------------------------------------- #
# The opportunity-cost comparison
# --------------------------------------------------------------------------- #
def _compact(c: dict) -> dict:
    return {"candidate_id": c["candidate_id"], "asset_class": c["asset_class"],
            "global_rank": c.get("global_rank"), "opportunity_cost_score": c.get("opportunity_cost_score"),
            "next_action_kind": (c.get("next_best_action") or {}).get("kind")}


def compare_proposal(frontier: dict, candidate_id: str, *, purchase: Optional[bool] = None) -> dict:
    """Does advancing ``candidate_id`` beat advancing the global top five?

    A research proposal competes with every higher-scoring action in the top
    five that the same research capacity could take; a purchase competes with
    every research action AND every human decision there. Passive accrual
    consumes neither, so it competes with neither - but it is listed.
    """
    cands = {c["candidate_id"]: c for c in frontier.get("candidates") or []}
    me = cands.get(candidate_id)
    base = {"proposal": candidate_id, "question": OPPORTUNITY_COST_QUESTION, "top_n": TOP_N,
            "frontier_state": frontier.get("state")}
    if frontier.get("state") != ST_COMPLETE:
        return {**base, "admitted": False, "verdict": V_FRONTIER_INCOMPLETE,
                "why": "the global frontier is %s; no research task or purchase is admitted "
                       "until every owner candidate is reconciled" % frontier.get("state")}
    if me is None:
        return {**base, "admitted": False, "verdict": V_NOT_RECONCILED,
                "why": "%s is not an opportunity in the reconciled global frontier, so it cannot "
                       "be compared with the global top five" % candidate_id}
    if me["current_state"] in UNRANKED_STATES:
        return {**base, "admitted": False, "verdict": V_CLOSED,
                "why": "%s is %s: %s" % (candidate_id, me["current_state"], me.get("closed_reason"))}
    kind = (me.get("next_best_action") or {}).get("kind")
    is_purchase = (kind == NA_PURCHASE) if purchase is None else bool(purchase)
    top = [cands[i] for i in frontier.get("global_top_ids") or [] if i in cands][:TOP_N]
    rivals, human, passive = [], [], []
    for c in top:
        if c["candidate_id"] == candidate_id:
            continue
        k = (c.get("next_best_action") or {}).get("kind")
        if k in PASSIVE_KINDS:
            passive.append(c)
        elif is_purchase or k in AGENT_KINDS:
            rivals.append(c)
        else:
            human.append(c)
    beating = sorted((c for c in rivals if c["opportunity_cost_score"] > me["opportunity_cost_score"]),
                     key=lambda c: -c["opportunity_cost_score"])
    admitted = not beating
    verdict = ((V_PURCHASE_JUSTIFIED if admitted else V_PURCHASE_NOT_JUSTIFIED) if is_purchase
               else (V_BEATS if admitted else V_LOSES))
    mine = "%s (%s, rank #%s, score %.4f, next action %s)" % (
        candidate_id, me["asset_class"], me.get("global_rank"), me["opportunity_cost_score"], kind)
    if beating:
        b = beating[0]
        why = ("NO: advancing %s (%s, rank #%s, score %.4f, next action %s) is worth more than %s; "
               "it waits until that action is taken or the frontier changes"
               % (b["candidate_id"], b["asset_class"], b.get("global_rank"),
                  b["opportunity_cost_score"], b["next_best_action"]["kind"], mine))
    elif is_purchase:
        why = ("YES: no research action or human decision in the global top %d scores higher than "
               "%s; passive accrual ahead of it costs nothing (%s)"
               % (TOP_N, mine, ", ".join(c["candidate_id"] for c in passive) or "none"))
    else:
        why = ("YES: no agent-executable action in the global top %d scores higher than %s; the "
               "higher-ranked opportunities either accrue evidence passively (%s) or wait at a "
               "human gate the agent cannot pass (%s)"
               % (TOP_N, mine, ", ".join(c["candidate_id"] for c in passive
                                         if c["opportunity_cost_score"] > me["opportunity_cost_score"]) or "none",
                  ", ".join(c["candidate_id"] for c in human
                            if c["opportunity_cost_score"] > me["opportunity_cost_score"]) or "none"))
    return {**base, "admitted": admitted, "verdict": verdict, "is_purchase": is_purchase,
            "proposal_rank": me.get("global_rank"),
            "proposal_score": me["opportunity_cost_score"], "asset_class": me["asset_class"],
            "competing_top5": [_compact(c) for c in rivals],
            "human_gated_top5": [_compact(c) for c in human],
            "passive_top5": [_compact(c) for c in passive],
            "beaten_by": [_compact(c) for c in beating], "why": why}


def mechanism_comparison(frontier: dict, mechanism_id: str) -> dict:
    """The comparison for a mechanism-catalog proposal, through its reconciled opportunity."""
    cid = (frontier.get("member_to_candidate") or {}).get(str(mechanism_id))
    if cid is None:
        return {"proposal": mechanism_id, "question": OPPORTUNITY_COST_QUESTION,
                "frontier_state": frontier.get("state"), "admitted": False,
                "verdict": V_FRONTIER_INCOMPLETE if frontier.get("state") != ST_COMPLETE
                else V_NOT_RECONCILED,
                "why": "mechanism %s is not claimed by any opportunity in the global frontier"
                       % mechanism_id}
    out = compare_proposal(frontier, cid)
    out["mechanism_id"] = mechanism_id
    return out


# --------------------------------------------------------------------------- #
# Build
# --------------------------------------------------------------------------- #
def _reason_not_higher(me: Optional[dict], top: Optional[dict]) -> str:
    if me is None:
        return ("no open candidate exists in this asset class; its closed mechanisms and data gaps "
                "are listed, and the class is not exhausted unless an exhaustion is declared")
    if top is None or me["candidate_id"] == top["candidate_id"]:
        return "it is the global #1 opportunity"
    diffs = {k: QUALITY_WEIGHTS[k] * (top["score"]["quality_components"][k]
                                      - me["score"]["quality_components"][k])
             for k in QUALITY_WEIGHTS}
    worst = [k for v, k in sorted(((v, k) for k, v in diffs.items() if v > 0), reverse=True)[:3]]
    return ("ranks #%d (score %.4f) behind the global #1 %s (score %.4f): largest weighted quality "
            "shortfalls %s; quality %.3f vs %.3f; next-action value %.2f vs %.2f; evidence "
            "completeness %.2f vs %.2f"
            % (me["global_rank"], me["opportunity_cost_score"], top["candidate_id"],
               top["opportunity_cost_score"], ", ".join(worst) or "none",
               me["score"]["quality"], top["score"]["quality"],
               me["score"]["next_action_value"], top["score"]["next_action_value"],
               me["score"]["completeness"], top["score"]["completeness"]))


def _asset_classes(candidates: list, rec: dict, estate: dict, top: Optional[dict]) -> dict:
    cat = (estate.get("owners") or {}).get(OW_CATALOG) or {}
    overrides = rec.get("closed_family_asset_classes") or {}
    sections = rec.get("asset_classes") or {}
    out = {}
    for ac in REQUIRED_ASSET_CLASSES:
        mine = [c for c in candidates if c["asset_class"] == ac]
        linked = sorted(c["candidate_id"] for c in candidates
                        if c["asset_class"] != ac and ac in c["information_asset_classes"])
        open_ = [c for c in mine if c["current_state"] not in UNRANKED_STATES]
        closed = [c for c in mine if c["current_state"] == CS_CLOSED]
        ledger = [f for f in cat.get("closed_mechanisms") or []
                  if (overrides.get(f.get("family_id")) or canonical_asset_class(f.get("asset_scope"))) == ac]
        states = {c["current_state"] for c in open_}
        exhaustion = (sections.get(ac) or {}).get("declared_exhaustion")
        if CS_LIVE in states:
            state, why = AS_LIVE, "an operational book holds approved capital in this class"
        elif CS_TRUE_FORWARD in states:
            state, why = AS_TRUE_FORWARD, "at least one open candidate is emitting TRUE_FORWARD evidence"
        elif CS_FORWARD_PENDING in states:
            state, why = AS_FORWARD_PENDING, "a forward clock is registered but has not emitted yet"
        elif CS_RESEARCH_READY in states:
            state, why = AS_RESEARCH_READY, "an agent-executable research action is available"
        elif open_:
            state, why = AS_BLOCKED, "every open candidate waits on a human gate or a data hold"
        elif exhaustion and exhaustion.get("mechanisms"):
            state, why = AS_EXHAUSTED, "declared exhaustion: %s" % exhaustion.get("why")
        else:
            state, why = AS_BLOCKED, ("no open mechanism and no declared exhaustion: closed "
                                      "mechanisms are not an exhausted asset class")
        strongest = min(open_, key=lambda c: c["global_rank"]) if open_ else None
        out[ac] = {
            "asset_class": ac, "state": state, "state_reason": why,
            "strongest_candidate": strongest["candidate_id"] if strongest else None,
            "strongest_global_rank": strongest["global_rank"] if strongest else None,
            "strongest_score": strongest["opportunity_cost_score"] if strongest else None,
            "tested_mechanisms": sorted({c["candidate_id"] for c in mine}
                                        | {str(f.get("family_id")) for f in ledger}),
            "closed_mechanisms": [{"id": c["candidate_id"], "source": "GLOBAL_RECONCILIATION",
                                   "reason": c.get("closed_reason")} for c in closed]
            + [{"id": f.get("family_id"), "source": "CLOSED_MECHANISM_LEDGER",
                "reason": f.get("verdict")} for f in ledger],
            "open_mechanisms": [{"id": c["candidate_id"], "state": c["current_state"],
                                 "global_rank": c["global_rank"]}
                                for c in sorted(open_, key=lambda c: c["global_rank"])],
            "information_linked_candidates": linked,
            "data_gaps": list((sections.get(ac) or {}).get("data_gaps") or []),
            "declared_exhaustion": exhaustion,
            "next_highest_value_action": strongest["next_best_action"] if strongest else {
                "kind": "DECLARE_NEW_MECHANISM",
                "description": "no open candidate: declare a mechanism-first hypothesis or resolve a "
                               "named data gap before any research in this class"},
            "reason_not_currently_ranked_higher": _reason_not_higher(strongest, top),
        }
    return out


def _old_frontier(estate: dict, candidates: list, member_of: dict) -> dict:
    """What the mechanism frontier could see before this reconciliation."""
    cat = (estate.get("owners") or {}).get(OW_CATALOG) or {}
    by_id = {c["candidate_id"]: c for c in candidates}
    live = set(cat.get("live_candidates") or [])
    mechs = {str(m.get("mechanism_id")): m for m in cat.get("mechanisms") or []}
    ranked = list(cat.get("ranked") or [])
    listed = live | set(mechs)
    visible = {member_of[i] for i in listed if i in member_of}
    open_ = [c for c in candidates if c["current_state"] not in UNRANKED_STATES]
    omitted = sorted((c for c in open_ if c["candidate_id"] not in visible),
                     key=lambda c: c["global_rank"])
    ranked_classes = {by_id[member_of[i]]["asset_class"] for i in ranked if i in member_of}
    listed_classes = {by_id[g]["asset_class"] for g in visible
                      if by_id[g]["current_state"] not in UNRANKED_STATES}
    identities = owner_identities(estate)
    omitted_ids = sorted({i for ow, ids in identities.items() for i in ids if i not in listed})
    changes = []
    for i in sorted(listed):
        g = member_of.get(i)
        c = by_id.get(g) if g else None
        changes.append({"old_id": i, "old_rank": (ranked.index(i) + 1) if i in ranked else None,
                        "old_status": (mechs.get(i) or {}).get("status")
                        or ("LISTED_LIVE_CANDIDATE_UNRANKED" if i in live else None),
                        "global_candidate": g, "global_rank": (c or {}).get("global_rank"),
                        "global_state": (c or {}).get("current_state")})
    material = [r for r in changes if (r["old_rank"] == 1 and r["global_rank"] != 1)
                or (r["old_rank"] is None and r["global_rank"] is not None
                    and r["global_rank"] <= TOP_N)]
    return {
        "old_frontier": "alpha_agent.r59.mechanisms (catalog live_candidates + mechanisms)",
        "old_listed_identities": sorted(listed), "old_ranked": ranked,
        "candidates_previously_omitted": [
            {"candidate_id": c["candidate_id"], "asset_class": c["asset_class"],
             "global_rank": c["global_rank"], "current_state": c["current_state"],
             "members": [m["id"] for m in c["members"]]} for c in omitted],
        "owner_identities_previously_invisible": omitted_ids,
        "n_owner_identities_previously_invisible": len(omitted_ids),
        "asset_classes_previously_underrepresented": [
            {"asset_class": ac,
             "old_frontier": "LISTED_BUT_NOT_RANKED" if ac in listed_classes else "ABSENT",
             "open_candidates_now": sum(1 for c in open_ if c["asset_class"] == ac)}
            for ac in REQUIRED_ASSET_CLASSES if ac not in ranked_classes],
        "rank_changes": changes, "material_rank_changes": material,
        "rank_materially_changed": bool(material),
    }


def build(catalog: dict, estate: dict) -> dict:
    """Reconcile the owners and rank every open opportunity. Pure over its inputs."""
    rec = reconciliation(catalog)
    problems = validate_reconciliation(rec, catalog)
    owners = estate.get("owners") or {}
    missing = sorted(o for o in REQUIRED_OWNERS if not (owners.get(o) or {}).get("present"))
    identities = owner_identities(estate)
    held_by: dict = {}
    for ow in OWNERS:
        for i in identities.get(ow, []):
            held_by.setdefault(i, []).append(ow)
    entries = [c for c in rec.get("candidates") or [] if isinstance(c, dict)]
    member_of: dict = {}
    for c in entries:
        for m in c.get("members") or []:
            member_of.setdefault(str(m), str(c.get("candidate_id")))
    unreconciled = [{"owner": ow, "id": i} for ow in OWNERS for i in identities.get(ow, [])
                    if i not in member_of]
    orphans = sorted(m for m in member_of if m not in held_by)
    classification = owner_classifications(estate)
    mech_status = {m["mechanism_id"]: m for m in (owners.get(OW_CATALOG) or {}).get("mechanisms") or []}

    candidates, conflicts = [], []
    for c in entries:
        cid = str(c.get("candidate_id"))
        members = [str(m) for m in c.get("members") or []]
        fwd = forward_projection(members, estate)
        disp, hist = c.get("disposition"), c.get("historical") or {}
        na, judgement = c.get("next_action") or {}, c.get("judgement") or {}
        state = candidate_state(disp, fwd, na)
        conflicts += _conflicts(cid, disp, members, classification, mech_status, fwd)
        row = {
            "candidate_id": cid, "title": c.get("title"), "asset_class": c.get("asset_class"),
            "information_asset_classes": list(c.get("information_asset_classes") or []),
            "mechanism_class": c.get("mechanism_class"),
            "information_object": c.get("information_object"),
            "disposition": disp, "current_state": state,
            "members": [{"id": m, "owners": held_by.get(m, []),
                         "owner_states": classification.get(m, {})} for m in members],
            "source_artifact": c.get("source_artifact"),
            "historical_result": hist.get("summary"),
            "historical_evidence": list(hist.get("evidence") or []),
            "after_cost_economics": {k: hist.get(k) for k in ("net_after_cost_pa", "t_stat",
                                                              "max_drawdown", "cost_survives_2x")},
            "incremental_result": {"increment_pa": hist.get("increment_pa"),
                                   "increment_t": hist.get("increment_t"),
                                   "control": hist.get("increment_control")},
            "risk_summary": ("maximum drawdown %s; survives 2x cost %s; liquidity/capacity %s; "
                             "diversification %s" % (hist.get("max_drawdown"),
                                                     hist.get("cost_survives_2x"),
                                                     judgement.get("liquidity_capacity"),
                                                     judgement.get("diversification"))),
            "forward_state": fwd["summary_state"],
            "forward_observations": {"emitted": fwd["emitted"], "matured": fwd["matured"],
                                     "best_effective_independent": fwd["best_effective"],
                                     "floor": fwd["floor"], "signal": fwd["signal"],
                                     "best_cell": fwd["best_id"], "best_horizon": fwd["best_horizon"],
                                     "best_net_alpha_bps": fwd["best_net_alpha_bps"],
                                     "best_t_stat": fwd["best_t_stat"]},
            "forward_detail": fwd,
            "closed_families": list(c.get("closed_families") or []),
            "closed_reason": c.get("closed_reason") if disp != DISP_OPEN else None,
            "data_requirement": c.get("data_requirement"),
        }
        if disp == DISP_OPEN:
            sc = score_candidate(historical=hist, judgement=judgement, forward=fwd, next_action=na)
            gates = remaining_gates(hist, fwd, na)
            row.update({
                "score": sc, "opportunity_cost_score": sc["opportunity_cost_score"],
                "evidence_quality": evidence_quality(sc["quality"], sc["completeness"], hist),
                "judgement": dict(judgement),
                "capital_eligibility": ("OPERATIONAL_APPROVED: approved under manual governance; "
                                        "this frontier changes nothing about it")
                if fwd["operational"] else "NOT_ELIGIBLE: %s" % gates[0],
                "remaining_gate": gates[0], "remaining_gates": gates,
                "human_gate": na.get("human_gate") or MANUAL_CAPITAL_GATE,
                "next_best_action": _next_action(na)})
        else:
            row.update({"score": None, "opportunity_cost_score": None,
                        "evidence_quality": EQ_CLOSED if disp == DISP_CLOSED else EQ_CONTROL,
                        "judgement": None, "capital_eligibility": "NOT_ELIGIBLE: %s" % disp,
                        "remaining_gate": None, "remaining_gates": [],
                        "human_gate": MANUAL_CAPITAL_GATE,
                        "next_best_action": {"kind": "NONE",
                                             "description": "closed or a control; reopened only "
                                                            "under contract rule 13",
                                             "consumes_research_capacity": False,
                                             "executable_by_agent": False, "requires_human": False}})
        candidates.append(row)

    ranked = sorted((r for r in candidates if r["current_state"] not in UNRANKED_STATES),
                    key=lambda r: (-r["opportunity_cost_score"], r["candidate_id"]))
    for i, r in enumerate(ranked, 1):
        r["global_rank"] = i
    for r in candidates:
        r.setdefault("global_rank", None)

    if problems:
        state = ST_INVALID
    elif missing:
        state = ST_ESTATE
    elif unreconciled:
        state = ST_UNRECONCILED
    elif conflicts:
        state = ST_CONFLICT
    else:
        state = ST_COMPLETE

    body = {
        "schema": SCHEMA, "calculation_owner": CALCULATION_OWNER, "question": QUESTION,
        "state": state, "GLOBAL_MULTI_ASSET_FRONTIER": "COMPLETE" if state == ST_COMPLETE else "BLOCKED",
        "catalog_hash": (owners.get(OW_CATALOG) or {}).get("catalog_hash"),
        "reconciliation_declared_at": rec.get("declared_at"),
        "owner_inputs": {ow: {"present": bool((owners.get(ow) or {}).get("present")),
                              "path": (owners.get(ow) or {}).get("path"),
                              "generated_at": (owners.get(ow) or {}).get("generated_at"),
                              "n_identities": len(identities.get(ow, []))} for ow in OWNERS},
        "reconciliation": {"problems": problems, "missing_owners": missing,
                           "n_owner_identities": len(held_by), "n_claimed": len(member_of),
                           "unreconciled": unreconciled, "orphan_declared_members": orphans,
                           "owner_conflicts": conflicts},
        "member_to_candidate": member_of,
        "n_candidates": len(candidates), "n_open": len(ranked),
        "global_top_ids": [r["candidate_id"] for r in ranked],
        "candidates": candidates,
    }
    top = ranked[0] if ranked else None
    for r in ranked:
        if r["next_best_action"]["kind"] not in PASSIVE_KINDS:
            r["opportunity_cost_comparison"] = compare_proposal(body, r["candidate_id"])
    body["asset_classes"] = _asset_classes(candidates, rec, estate, top)
    body["global_top_10"] = [
        {"rank": r["global_rank"], "candidate_id": r["candidate_id"], "asset_class": r["asset_class"],
         "state": r["current_state"], "opportunity_cost_score": r["opportunity_cost_score"],
         "evidence_quality": r["evidence_quality"],
         "after_cost_evidence": r["after_cost_economics"], "forward_evidence": r["forward_state"],
         "remaining_gate": r["remaining_gate"], "next_action": r["next_best_action"]["kind"]}
        for r in ranked[:10]]
    nga = next((r for r in ranked if r["next_best_action"]["kind"] not in PASSIVE_KINDS), None)
    naa = next((r for r in ranked if r["next_best_action"]["kind"] in AGENT_KINDS
                and (r.get("opportunity_cost_comparison") or {}).get("admitted")), None)
    body["next_global_action"] = None if nga is None else {
        "candidate_id": nga["candidate_id"], "global_rank": nga["global_rank"],
        "asset_class": nga["asset_class"], **nga["next_best_action"],
        "opportunity_cost_score": nga["opportunity_cost_score"],
        "passive_opportunities_ranked_above": [r["candidate_id"] for r in ranked
                                               if r["global_rank"] < nga["global_rank"]]}
    body["next_agent_research_action"] = None if naa is None else {
        "candidate_id": naa["candidate_id"], "global_rank": naa["global_rank"],
        "asset_class": naa["asset_class"], **naa["next_best_action"],
        "opportunity_cost_score": naa["opportunity_cost_score"],
        "comparison": naa["opportunity_cost_comparison"]}
    body["purchase_comparisons"] = [r["opportunity_cost_comparison"] for r in ranked
                                    if r["next_best_action"]["kind"] == NA_PURCHASE]
    body["old_frontier_comparison"] = _old_frontier(estate, candidates, member_of)
    body["answers"] = _answers(body, ranked)
    body["scoring"] = {"quality_weights": dict(QUALITY_WEIGHTS),
                       "completeness_weights": dict(COMPLETENESS_WEIGHTS),
                       "formula": ("score = quality x (0.5 + 0.5 x information_gain) x (0.85 + 0.15 x "
                                   "completeness) x (1 - 0.25 x min(1, research_days/20)) x "
                                   "(1 - 0.25 x min(1, data_cost_usd/2000))"),
                       "excluded_inputs": list(EXCLUDED_SCORE_INPUTS),
                       "good_quality_threshold": GOOD_QUALITY,
                       "mature_completeness_threshold": MATURE_COMPLETENESS}
    body["invariants"] = _invariants(body, rec, estate, identities, member_of, unreconciled)
    body["safety"] = {**r59.SAFETY, "registers_forward_challenger": False, "purchases_data": False,
                      "writes_owner_store": False, "allocates_capital": False}
    return body


def _answers(body: dict, ranked: list) -> dict:
    by_ac = {}
    for r in ranked:
        by_ac.setdefault(r["asset_class"], r)
    options = next((r for r in ranked if str(r.get("information_object") or "").startswith(
        "OPRA_SINGLE_NAME")), None)
    fx = by_ac.get("FX")
    nga, naa = body.get("next_global_action"), body.get("next_agent_research_action")
    opt_cmp = (options or {}).get("opportunity_cost_comparison") or {}
    return {
        "IS_SINGLE_NAME_OPTIONS_STILL_THE_BEST_NEXT_RESEARCH_SPEND":
            None if options is None else ("YES" if opt_cmp.get("admitted") and nga
                                          and nga["candidate_id"] == options["candidate_id"] else "NO"),
        "single_name_options": None if options is None else {
            "global_rank": options["global_rank"], "score": options["opportunity_cost_score"],
            "verdict": opt_cmp.get("verdict"), "why": opt_cmp.get("why")},
        "IS_FX_CURRENTLY_A_TOP_GLOBAL_PRIORITY":
            None if fx is None else ("YES" if fx["global_rank"] <= TOP_N else "NO"),
        "fx": None if fx is None else {"candidate_id": fx["candidate_id"],
                                       "global_rank": fx["global_rank"],
                                       "score": fx["opportunity_cost_score"]},
        "FX_VERSUS_THE_SINGLE_NAME_OPTIONS_PURCHASE":
            None if fx is None or options is None else
            ("STRONGER" if fx["opportunity_cost_score"] > options["opportunity_cost_score"] else "WEAKER"),
        "WHAT_SHOULD_THE_AGENT_DO_NEXT": {"next_global_action": nga, "next_agent_research_action": naa},
    }


def _invariants(body: dict, rec: dict, estate: dict, identities: dict, member_of: dict,
                unreconciled: list) -> dict:
    unrec_owners = {u["owner"] for u in unreconciled}
    labels = owner_asset_labels(estate)
    cat = (estate.get("owners") or {}).get(OW_CATALOG) or {}
    classes = body.get("asset_classes") or {}
    ranked = [c for c in body["candidates"] if c["current_state"] not in UNRANKED_STATES]
    exhausted_ok = all(
        (not any(c["asset_class"] == ac and c["current_state"] not in UNRANKED_STATES
                 for c in body["candidates"]))
        and bool(((rec.get("asset_classes") or {}).get(ac) or {}).get("declared_exhaustion"))
        for ac, v in classes.items() if v["state"] == AS_EXHAUSTED)
    state_declared = any(p.startswith("STATE_DECLARED") for p in body["reconciliation"]["problems"])

    def class_ok(ac: str) -> bool:
        return all(i in member_of for i, ls in labels.items() if ac in ls)

    def v(ok: bool, evidence: str) -> dict:
        return {"value": "YES" if ok else "NO", "evidence": evidence}

    tests = "tests/test_alpha_agent_global_multi_asset_frontier.py"
    return {
        "GLOBAL_MULTI_ASSET_FRONTIER": v(body["state"] == ST_COMPLETE, "computed: state %s" % body["state"]),
        "ALL_REQUIRED_ASSET_CLASSES_PRESENT": v(set(classes) == set(REQUIRED_ASSET_CLASSES),
                                                "computed over the nine required classes"),
        "OLD_FORWARD_CANDIDATES_RECONCILED": v(not unrec_owners & set(FORWARD_OWNERS)
                                               and all(i in member_of for i in cat.get("live_candidates") or []),
                                               "computed: every forward-owner identity is claimed"),
        "R46_CANDIDATES_NOT_SILENTLY_DROPPED": v(not unrec_owners & {OW_R46, OW_R46_ADOPTED},
                                                 "computed over the R46 leaderboard and adopted continuation"),
        "R63_R64_CANDIDATES_RECONCILED": v(not unrec_owners & {OW_R63, OW_R64},
                                           "computed over the R63 and R64 challenger records"),
        "TRUE_FORWARD_CANDIDATES_RECONCILED": v(not unrec_owners & {OW_REGISTRY, OW_ACCRUAL},
                                                "computed over the canonical registry and accrual projection"),
        "FX_CANDIDATES_RECONCILED": v(class_ok("FX"), "computed over every FX-labelled owner identity"),
        "CREDIT_CANDIDATES_RECONCILED": v(class_ok("CREDIT"), "computed over every credit-labelled owner identity"),
        "VOLATILITY_CANDIDATES_RECONCILED": v(class_ok("VOLATILITY"),
                                              "computed over every volatility-labelled owner identity"),
        "EQUITY_HAS_NO_PRIORITY_BONUS": v(score_is_blind(), "signature check; perturbation proven in %s" % tests),
        "RECENT_MECHANISM_HAS_NO_PRIORITY_BONUS": v(score_is_blind(), "signature check; perturbation proven in %s" % tests),
        "EXECUTOR_AVAILABILITY_HAS_NO_PRIORITY_BONUS": v(score_is_blind(), "signature check; perturbation proven in %s" % tests),
        "GLOBAL_OPPORTUNITY_COST_REQUIRED": v(all(c.get("opportunity_cost_comparison") for c in ranked
                                                  if c["next_best_action"]["kind"] not in PASSIVE_KINDS),
                                              "computed; the mechanism frontier's gate is proven in %s" % tests),
        "PURCHASE_REQUIRES_GLOBAL_COMPARISON": v(all((c.get("opportunity_cost_comparison") or {}).get("is_purchase")
                                                     for c in ranked if c["next_best_action"]["kind"] == NA_PURCHASE),
                                                 "computed over every purchase decision"),
        "ASSET_CLASS_CANNOT_DISAPPEAR": v(all(ac in classes and classes[ac]["state"] in ASSET_CLASS_STATES
                                              for ac in REQUIRED_ASSET_CLASSES),
                                          "computed: every required class carries a state"),
        "WHOLE_ASSET_CLASS_NOT_EXHAUSTED_FROM_PARTIAL_FAILURES": v(exhausted_ok,
                                                                   "computed: EXHAUSTED only with a declared exhaustion and no open candidate"),
        "NO_SECOND_RESEARCH_MEMORY": v(True, "structural: this module holds no database and calls no memory writer; proven in %s" % tests),
        "NO_SECOND_FORWARD_LEDGER": v(not state_declared, "computed: the declaration carries no forward state; the module writes no ledger"),
        "NO_PORTFOLIO_MUTATION": v(not any(r59.SAFETY[k] for k in ("creates_orders", "creates_fills",
                                                                    "promotes_model", "mutates_operational_store",
                                                                    "automation_enabled")),
                                   "safety flags; no api/engine import, proven in %s" % tests),
    }


# --------------------------------------------------------------------------- #
# Entry points
# --------------------------------------------------------------------------- #
def current(mem: Optional[M.ResearchMemory] = None, *, catalog: Optional[dict] = None,
            paths: Optional[dict] = None) -> dict:
    """Load the owners and build the frontier for the committed catalog."""
    cat = catalog if catalog is not None else MX.load_catalog()
    if not declared(cat):
        return {"schema": SCHEMA, "calculation_owner": CALCULATION_OWNER, "state": ST_NOT_DECLARED,
                "GLOBAL_MULTI_ASSET_FRONTIER": "BLOCKED", "candidates": [], "global_top_ids": [],
                "why": "the catalog declares no %s section" % CATALOG_SECTION}
    return build(cat, load_estate(mem, catalog=cat, paths=paths))


def artifact_path() -> Path:
    return r59.research_root() / ARTIFACT_SUBDIR / ARTIFACT_NAME


def write(frontier: dict) -> Path:
    """The ONE artifact this module writes."""
    return r59.write_artifact(ARTIFACT_NAME, frontier, subdir=ARTIFACT_SUBDIR)


def checkpoint_block(frontier: dict) -> dict:
    """The global frontier as the agent checkpoint carries it."""
    old = frontier.get("old_frontier_comparison") or {}
    return {
        "state": frontier.get("state"), "calculation_owner": CALCULATION_OWNER,
        "artifact": str(artifact_path()),
        "global_top_10": frontier.get("global_top_10") or [],
        "best_by_asset_class": {ac: {"state": v["state"], "strongest_candidate": v["strongest_candidate"],
                                     "global_rank": v["strongest_global_rank"]}
                                for ac, v in (frontier.get("asset_classes") or {}).items()},
        "next_global_action": frontier.get("next_global_action"),
        "next_agent_research_action": frontier.get("next_agent_research_action"),
        "unreconciled": len((frontier.get("reconciliation") or {}).get("unreconciled") or []),
        "candidates_previously_omitted": [c["candidate_id"] for c in old.get("candidates_previously_omitted") or []],
        "purchase_comparisons": [{k: c.get(k) for k in ("proposal", "verdict", "admitted", "why")}
                                 for c in frontier.get("purchase_comparisons") or []],
        "invariants": {k: v["value"] for k, v in (frontier.get("invariants") or {}).items()},
    }


def _pct(v) -> str:
    f = _num(v)
    return "-" if f is None else "%+.2f %%/yr" % (100.0 * f)


def render_markdown(frontier: dict) -> str:
    """A human-readable rendering of the artifact. The artifact is authoritative."""
    lines = ["# GLOBAL MULTI-ASSET ALPHA FRONTIER", "",
             "**GLOBAL_MULTI_ASSET_FRONTIER = %s** (state `%s`)" % (
                 frontier.get("GLOBAL_MULTI_ASSET_FRONTIER"), frontier.get("state")), "",
             "Rendered from `%s` (%s). The artifact is authoritative; this page is a view of it." % (
                 ARTIFACT_NAME, CALCULATION_OWNER), "",
             "> %s" % QUESTION, ""]
    rec = frontier.get("reconciliation") or {}
    lines += ["Owner identities reconciled: %s claimed of %s held; unreconciled %d; owner conflicts %d; "
              "invalid declarations %d." % (rec.get("n_claimed"), rec.get("n_owner_identities"),
                                            len(rec.get("unreconciled") or []),
                                            len(rec.get("owner_conflicts") or []),
                                            len(rec.get("problems") or [])), ""]
    lines += ["## GLOBAL TOP 10", "",
              "| Rank | Candidate | Asset | State | After-cost evidence | Forward evidence | Remaining gate | Next action |",
              "|---|---|---|---|---|---|---|---|"]
    by_id = {c["candidate_id"]: c for c in frontier.get("candidates") or []}
    for row in frontier.get("global_top_10") or []:
        c = by_id[row["candidate_id"]]
        econ = c["after_cost_economics"]
        ev = "net %s, t %s" % (_pct(econ.get("net_after_cost_pa")), econ.get("t_stat"))
        fo = c["forward_observations"]
        fwd = "%d emitted / %d matured / %.0f of %d effective" % (
            fo["emitted"], fo["matured"], fo["best_effective_independent"], fo["floor"])
        lines.append("| %d | `%s` (%.4f) | %s | %s | %s | %s | %s | %s |" % (
            row["rank"], row["candidate_id"], row["opportunity_cost_score"], row["asset_class"],
            row["state"], ev, fwd, str(row["remaining_gate"]).split(":")[0], row["next_action"]))
    lines += ["", "## BEST BY ASSET CLASS", ""]
    for ac, v in (frontier.get("asset_classes") or {}).items():
        lines.append("- **%s** = %s - state %s%s" % (
            ac, "`%s` (global #%s)" % (v["strongest_candidate"], v["strongest_global_rank"])
            if v["strongest_candidate"] else "none open", v["state"],
            "" if v["strongest_candidate"] else "; data gaps: %s" % "; ".join(v["data_gaps"])))
    old = frontier.get("old_frontier_comparison") or {}
    lines += ["", "## WHAT THE OLD FRONTIER MISSED", "",
              "- Owner identities the mechanism frontier never read: %d" % old.get(
                  "n_owner_identities_previously_invisible", 0),
              "- Open candidates previously omitted: %s" % ", ".join(
                  "`%s` (%s, #%s)" % (c["candidate_id"], c["asset_class"], c["global_rank"])
                  for c in old.get("candidates_previously_omitted") or []),
              "- Asset classes previously underrepresented: %s" % ", ".join(
                  "%s (%s)" % (a["asset_class"], a["old_frontier"])
                  for a in old.get("asset_classes_previously_underrepresented") or []),
              "- Rank materially changed: %s" % ("YES" if old.get("rank_materially_changed") else "NO")]
    for r in old.get("material_rank_changes") or []:
        lines.append("  - `%s`: old rank %s -> `%s` global #%s" % (
            r["old_id"], r["old_rank"] or "unranked", r["global_candidate"], r["global_rank"]))
    ans = frontier.get("answers") or {}
    lines += ["", "## ANSWERS", "",
              "- IS SINGLE-NAME OPTIONS STILL THE BEST NEXT RESEARCH SPEND? **%s** - %s" % (
                  ans.get("IS_SINGLE_NAME_OPTIONS_STILL_THE_BEST_NEXT_RESEARCH_SPEND"),
                  (ans.get("single_name_options") or {}).get("why")),
              "- IS FX CURRENTLY A TOP GLOBAL PRIORITY? **%s** (%s)" % (
                  ans.get("IS_FX_CURRENTLY_A_TOP_GLOBAL_PRIORITY"), ans.get("fx")),
              "- FX versus the single-name options purchase: **%s**" % ans.get(
                  "FX_VERSUS_THE_SINGLE_NAME_OPTIONS_PURCHASE")]
    nga, naa = frontier.get("next_global_action"), frontier.get("next_agent_research_action")
    if nga:
        lines.append("- Highest-value next action globally: `%s` (%s) - %s" % (
            nga["candidate_id"], nga["kind"], nga["description"]))
    if naa:
        lines.append("- Highest-value action the agent itself can take: `%s` (%s) - %s" % (
            naa["candidate_id"], naa["kind"], naa["comparison"]["why"]))
    lines += ["", "## INVARIANTS", ""]
    for k, v in (frontier.get("invariants") or {}).items():
        lines.append("- %s = %s (%s)" % (k, v["value"], v["evidence"]))
    lines += ["", "Research only. No purchase, subscription, registration, promotion, order, fill, "
                  "capital allocation or backfill was performed by this frontier.", ""]
    return "\n".join(lines)


__all__ = ["CALCULATION_OWNER", "SCHEMA", "CATALOG_SECTION", "REQUIRED_ASSET_CLASSES",
           "declared", "reconciliation", "estate_paths", "load_estate", "owner_identities",
           "validate_reconciliation", "forward_projection", "score_candidate", "score_is_blind",
           "build", "compare_proposal", "mechanism_comparison", "current", "write",
           "checkpoint_block", "render_markdown"]
