"""alpha_agent.r59 - Release 59: the autonomous alpha engine REUNIFICATION.

R56-R58 were three consecutive one-shot campaigns. Each built its own families,
its own runner and its own verdict file; each ended NO_ALPHA_EVIDENCE; each
stopped and waited for a human to choose the next topic. The capability to NOT
stop was already in the tree and was simply not wired to them:

    alpha_agent.autonomous_research   a durable, crash-safe, never-idle SQLite
                                      work queue with atomic claim/settle, lane
                                      routing and replenishment (Stage 8)
    api.research_bridge               governor mandate -> THAT queue -> drain ->
                                      evidence -> alpha-strength gate
    alpha_agent.r39                   a real machine discovery engine: an auto-
                                      transform grammar, symbolic expression
                                      trees, a target factory, a trade space, a
                                      model registry and a search-budget ledger
    alpha_agent.r57.engine            ONE statistical kernel (Newey-West +
                                      Benjamini-Hochberg) shared by families
    alpha_agent.r46 / r52             the prospective forward-evidence runtime

R59 adds NO second queue, NO second governor and NO second tournament. It adds
the four things that were genuinely missing and that made every release stop:

    memory      ONE persistent research memory across releases - hypothesis
                identity, graveyard, reopen conditions and a search-burden
                ledger that is COUNTED rather than copied as a constant
                (R58 carried ``PRIOR_SEARCH_BURDEN = 302`` by hand).
    frontier    ONE multi-asset research frontier measured from owned panels,
                so a failure in US equities allocates capacity elsewhere
                instead of ending the session.
    governor    ONE discovery governor that turns evidence + frontier + burden
                into bounded mandates, with a reserved share for non-equity
                work so equities cannot monopolise the queue.
    handlers    r59.* lanes on the CANONICAL queue that execute those mandates
                through the existing engines and kernels.

RESEARCH ONLY. Nothing here can create an order or a fill, enable a broker,
promote a model, activate a sleeve, approve a proposal or write an operational
store. The live C: checkout is read-only. R39/R46/R56/R57/R58 forward evidence
is imported, never rewritten, and a historical result is never converted into
prospective evidence.
"""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

CAMPAIGN_ID = "r59_autonomous_alpha_engine_v1"
PHASE = "R59"
RELEASE = "R59"

RESEARCH_ROOT_ENV = "PAPER_TRADER_R59_RESEARCH_ROOT"
DEFAULT_RESEARCH_ROOT = Path(r"D:\Stock_Prediction_app_data\r59_autonomous_alpha")

REPO_ROOT = Path(__file__).resolve().parents[2]

SAFETY = {
    "research_only": True,
    "paper_only": True,
    "creates_orders": False,
    "creates_fills": False,
    "broker_enabled": False,
    "promotes_model": False,
    "activates_sleeve": False,
    "approves_proposal": False,
    "mutates_operational_store": False,
    "automation_enabled": False,
    "live_checkout_read_only": True,
}

# --------------------------------------------------------------------------- #
# Owned, measured research substrates (read-only). Every path here is verified
# by frontier.measure() before it is used - nothing is assumed to exist.
# --------------------------------------------------------------------------- #
R39_ROOT = Path(r"D:\Stock_Prediction_app_data\universal_alpha_r39")
R46_ROOT = Path(r"D:\Stock_Prediction_app_data\prospective_alpha_tournament_r46")
R56_ROOT = Path(r"D:\Stock_Prediction_app_data\r56_shadow_portfolios")
R57_ROOT = Path(r"D:\Stock_Prediction_app_data\r57_alpha_discovery")
R58_ROOT = Path(r"D:\Stock_Prediction_app_data\r58_orthogonal_alpha")

EQUITY_PANEL = R57_ROOT / "panels" / "sp500_pit_panel_v1.npz"
EQUITY_PANEL_META = R57_ROOT / "panels" / "sp500_pit_panel_v1.meta.json"
FUTURES_PANEL = R57_ROOT / "panels" / "futures_panel_v1.npz"
FUTURES_PANEL_META = R57_ROOT / "panels" / "futures_panel_v1.meta.json"
FUNDAMENTAL_PANEL = R58_ROOT / "panels" / "r58_pit_fundamental_panel_v1.npz"
FUNDAMENTAL_PANEL_META = R58_ROOT / "panels" / "r58_pit_fundamental_panel_v1.meta.json"
FORM4_RAW_DIR = R46_ROOT / "_data_form4"

# --------------------------------------------------------------------------- #
# Asset classes. These are RESEARCH SCOPES, not risk factors (Release-32 design
# rule): a scope says where a cross-section is taken, never that the label
# itself carries risk.
# --------------------------------------------------------------------------- #
AC_US_EQUITY = "US_EQUITY"
AC_EQUITY_INDEX = "EQUITY_INDEX_FUTURES"
AC_RATES = "RATES_FUTURES"
AC_COMMODITY = "COMMODITY_FUTURES"
AC_FX = "FX_FUTURES"
AC_VOLATILITY = "VOLATILITY"
AC_CREDIT = "CREDIT_PROXY"
AC_CROSS_ASSET = "CROSS_ASSET"

ASSET_CLASSES = (AC_US_EQUITY, AC_EQUITY_INDEX, AC_RATES, AC_COMMODITY,
                 AC_FX, AC_VOLATILITY, AC_CREDIT, AC_CROSS_ASSET)

NON_EQUITY_CLASSES = tuple(c for c in ASSET_CLASSES if c != AC_US_EQUITY)

# Norgate futures ``classification`` -> R59 asset class.
NORGATE_CLASS_MAP = {
    "Stock Index": AC_EQUITY_INDEX,
    "Interest Rate": AC_RATES,
    "Agriculture & Livestock": AC_COMMODITY,
    "Energy": AC_COMMODITY,
    "Metal": AC_COMMODITY,
    "Currency": AC_FX,
    "Other": AC_COMMODITY,
}
# &VX is a volatility market that Norgate classifies under Stock Index; it is
# reassigned by symbol because a volatility future is not an equity index.
VOLATILITY_SYMBOLS = ("&VX",)

# --------------------------------------------------------------------------- #
# Frontier states (section F of the release brief).
# --------------------------------------------------------------------------- #
FS_DATA_READY = "DATA_READY"
FS_RESEARCH_READY = "RESEARCH_READY"
FS_ACTIVE_SEARCH = "ACTIVE_SEARCH"
FS_FORWARD_SHADOW = "FORWARD_SHADOW"
FS_EXHAUSTED = "EXHAUSTED"
FS_BLOCKED = "BLOCKED"

FRONTIER_STATES = (FS_DATA_READY, FS_RESEARCH_READY, FS_ACTIVE_SEARCH,
                   FS_FORWARD_SHADOW, FS_EXHAUSTED, FS_BLOCKED)

# --------------------------------------------------------------------------- #
# Data-opportunity states (section K).
# --------------------------------------------------------------------------- #
DO_ALREADY_OWNED_UNUSED = "ALREADY_OWNED_UNUSED"
DO_FREE_AVAILABLE = "FREE_AVAILABLE"
DO_WAITING_FOR_SAMPLE = "WAITING_FOR_SAMPLE"
DO_SAMPLE_UNDER_EVALUATION = "SAMPLE_UNDER_EVALUATION"
DO_PURCHASE_CANDIDATE = "PURCHASE_CANDIDATE"
DO_REJECTED_LOW_VALUE = "REJECTED_LOW_VALUE"
DO_BLOCKED = "BLOCKED"
DO_EXHAUSTED = "EXHAUSTED"

DATA_OPPORTUNITY_STATES = (
    DO_ALREADY_OWNED_UNUSED, DO_FREE_AVAILABLE, DO_WAITING_FOR_SAMPLE,
    DO_SAMPLE_UNDER_EVALUATION, DO_PURCHASE_CANDIDATE, DO_REJECTED_LOW_VALUE,
    DO_BLOCKED, DO_EXHAUSTED)

# --------------------------------------------------------------------------- #
# Hypothesis outcomes. A rejection is a RESULT, never a stop condition.
# --------------------------------------------------------------------------- #
HO_REJECTED = "REJECTED"
HO_NO_ALPHA_EVIDENCE = "NO_ALPHA_EVIDENCE"
HO_QUALIFIED = "QUALIFIED"
HO_DATA_HOLD = "DATA_HOLD"
HO_FORWARD_FROZEN = "FORWARD_FROZEN"
HO_NEEDS_MORE_EVIDENCE = "NEEDS_MORE_EVIDENCE"

HYPOTHESIS_OUTCOMES = (HO_REJECTED, HO_NO_ALPHA_EVIDENCE, HO_QUALIFIED,
                       HO_DATA_HOLD, HO_FORWARD_FROZEN, HO_NEEDS_MORE_EVIDENCE)

TERMINAL_OUTCOMES = frozenset({HO_REJECTED, HO_NO_ALPHA_EVIDENCE, HO_QUALIFIED,
                               HO_FORWARD_FROZEN})

# --------------------------------------------------------------------------- #
# Queue lanes. Every R59 lane is prefixed so the canonical queue's lane
# allowlist can route R59 work without touching one existing job.
# --------------------------------------------------------------------------- #
LANE_PREFIX = "r59."
LANE_ECONOMIC = "r59.economic"
LANE_MATHEMATICAL = "r59.mathematical"
LANE_CROSS_ASSET = "r59.cross_asset"
LANE_DATA_OPPORTUNITY = "r59.data_opportunity"
LANE_FORM4 = "r59.form4_normalise"
LANE_PROVIDER = "r59.provider_value"
LANE_NATIVE = "r59.native"

# --------------------------------------------------------------------------- #
# Pre-registered evaluation conventions. These are INHERITED from R57/R58 on
# purpose: reusing the partition and the cost model is what makes an R59 result
# comparable to the 25 families those releases already prosecuted, and what
# stops a new release from quietly grading itself more kindly.
# --------------------------------------------------------------------------- #
DISCOVERY_START = "2011-07-01"
VALIDATION_START = "2018-01-01"
LOCKBOX_START = "2023-01-01"
CADENCE = 21
HORIZON = 21
BH_Q = 0.10
OBS_FLOOR = 36
GATE_MATERIALITY = 0.015          # annualised net excess
GATE_MAX_TURNOVER = 0.40          # per decision, one side
EQ_COST_RATE_PER_SIDE = 0.00125
FUT_COST_RATE_PER_SIDE = 0.0002

# Cross-asset fairness: the share of each generation batch reserved for the
# strongest NON-EQUITY ready mandates (section N - "prevent EQUITY MONOPOLY").
NON_EQUITY_RESERVATION = 0.40


def research_root() -> Path:
    return Path(os.environ.get(RESEARCH_ROOT_ENV) or DEFAULT_RESEARCH_ROOT)


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def stable_hash(obj) -> str:
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def short_hash(obj, n: int = 12) -> str:
    return stable_hash(obj)[:n]


def write_artifact(name: str, body: dict, subdir: str = "results") -> Path:
    """Atomically write a hashed, safety-stamped research artifact."""
    d = research_root() / subdir
    d.mkdir(parents=True, exist_ok=True)
    body = dict(body)
    body.setdefault("campaign_id", CAMPAIGN_ID)
    body.setdefault("phase", PHASE)
    body.setdefault("generated_at", now_iso())
    body.setdefault("safety", dict(SAFETY))
    body["artifact_hash"] = stable_hash(
        {k: v for k, v in body.items()
         if k not in ("artifact_hash", "generated_at")})
    p = d / name
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(body, indent=1, default=str), encoding="utf-8")
    tmp.replace(p)
    return p


def read_artifact(name: str, subdir: str = "results"):
    p = research_root() / subdir / name
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def read_json(path) -> dict | None:
    """Read a JSON artifact from ANY owned research root, tolerating absence.

    Used to import prior-release evidence. A missing or unreadable artifact is
    reported as absence, never as an empty result that could be mistaken for a
    measured zero.
    """
    p = Path(path)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def assert_worktree_import() -> str:
    """Fail closed if alpha_agent resolved to a different checkout.

    The venv carries an editable install whose finder maps ``paper_trader`` to
    ``C:\\Users\\binis\\paper_trader``. A worktree that imports through that
    finder silently runs the LIVE tree's code against the worktree's data. R59
    is imported as top-level ``alpha_agent`` for exactly this reason, and this
    assertion proves it at runtime rather than trusting it.
    """
    here = Path(__file__).resolve().parents[1]
    import alpha_agent as _aa
    got = Path(_aa.__file__).resolve().parent
    if got != here:
        raise RuntimeError(
            "R59 import integrity: alpha_agent resolved to %s but r59 lives in "
            "%s - the editable-install finder captured the import" % (got, here))
    return str(here)
