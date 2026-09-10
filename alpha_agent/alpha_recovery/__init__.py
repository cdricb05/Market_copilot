"""alpha_agent.alpha_recovery - the Alpha Recovery Offensive (project stop-loss campaign).

This package owns the PERMANENT alpha objective's instruments, as declared in
``docs/ALPHA_RECOVERY_OPERATING_CONTRACT.md``:

    checkpoint          the frozen 10-eligible-session project stop-loss, computed
                        by the canonical exchange-session owner and never moved
    scoreboard          alpha_recovery_scoreboard.json (+ human rendering): the
                        primary progress measure of the project
    forecast_contract   the ONE canonical forecast product (market / regime,
                        cross-sectional equities, multi-asset sleeves) in which
                        a score may never masquerade as an expected return
    incumbent           the real baseline of fundamental_momentum_50_50_v1:
                        historical OOS and TRUE_FORWARD, measured SEPARATELY
    program             the information-directed research program: frontier
                        consumption through the R59 governor adapter, family
                        budgets, the 75 % non-price rule, the reopening rule
    earnings_events     EARNINGS_EVENT_REACTION: announcement-window reaction
                        and XBRL earnings surprise from owned SEC data, PIT
    tournament          head-to-head against the incumbent on identical samples;
                        equal-risk cross-domain incremental utility
    cadence             signal observation frequency != trading frequency
    market_direction    calibrated broad-market directional forecasts (SPY)
    purchase_case       owned / free exhaustion and the paid-information case
    report              the final report, scoreboard first

RESEARCH ONLY. Nothing here creates an order, a fill, a proposal, a promotion,
a sleeve activation, a registration, a purchase, a subscription or an
operational-store write. The live ``C:\\Users\\binis\\paper_trader`` checkout is
read-only. Every write lands under the campaign research root (redirectable by
env var for hermetic tests) or under ``research/alpha_recovery`` in the
development worktree. The ONE scorer (``alpha_agent.r63.sensitivity.run_cell``),
the ONE PIT engine (``alpha_agent.r63.pit``), the ONE risk-controlled book
(``alpha_agent.r64.construction``), the ONE frontier (``alpha_agent.r63.gaps``)
and the ONE governor adapter (``alpha_agent.r59.information_needs``) are reused,
never re-implemented.
"""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from alpha_agent import r63 as _r63
from alpha_agent import r64 as _r64

CAMPAIGN_ID = "alpha_recovery_offensive_v1"
PHASE = "ALPHA_RECOVERY"
BRANCH = "alpha-recovery-offensive"

RESEARCH_ROOT_ENV = "PAPER_TRADER_ALPHA_RECOVERY_RESEARCH_ROOT"
DEFAULT_RESEARCH_ROOT = Path(r"D:\Stock_Prediction_app_data\alpha_recovery_offensive")

#: Where the durable, committed campaign artifacts live inside the checkout.
#: A test redirects the directory so it never touches the committed files.
REPO_DIR_ENV = "PAPER_TRADER_ALPHA_RECOVERY_REPO_DIR"
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_REPO_DIR = REPO_ROOT / "research" / "alpha_recovery"
CONTRACT_DOC = REPO_ROOT / "docs" / "ALPHA_RECOVERY_OPERATING_CONTRACT.md"
PROTOCOL_PATH = REPO_ROOT / "research" / "alpha_recovery" / "ALPHA_RECOVERY_PROTOCOL.json"
CHECKPOINT_NAME = "alpha_recovery_checkpoint.json"
SCOREBOARD_JSON_NAME = "alpha_recovery_scoreboard.json"
SCOREBOARD_MD_NAME = "ALPHA_RECOVERY_SCOREBOARD.md"
REPORT_MD_NAME = "ALPHA_RECOVERY_REPORT.md"

CONTRACT_VERSION = 1

#: The incumbent: the BENCHMARK, not the presumed correct model (contract rule 2).
INCUMBENT_MODEL_ID = "fundamental_momentum_50_50_v1"
INCUMBENT_OPERATIONAL_BOOK = "alpha_paper_book_1"
INCUMBENT_SHADOW_BOOK = "fundamental_momentum_50_50_top50"
INCUMBENT_LEGS = {"fundamental": "composite_sn", "momentum": "mom_6_1"}
INCUMBENT_BLEND = {"fundamental": 0.5, "momentum": 0.5}

#: The R64 development parent this campaign was cut from (verified at freeze).
R64_PARENT_COMMIT = "cb162a8e5871b15e28b54862fe79aea4901cb764"

STOP_LOSS_SESSIONS = 10

# Scoreboard status vocabulary (contract section 4).
ST_RESEARCHING = "RESEARCHING"
ST_HISTORICAL_SURVIVOR = "HISTORICAL_SURVIVOR"
ST_READY = "READY_FOR_FORWARD_QUALIFICATION"
ST_COMPETING = "TRUE_FORWARD_COMPETING"
ST_REJECTED = "REJECTED"
ST_EXHAUSTED = "OWNED_FREE_INFORMATION_EXHAUSTED"
STATUSES = (ST_RESEARCHING, ST_HISTORICAL_SURVIVOR, ST_READY, ST_COMPETING, ST_REJECTED,
            ST_EXHAUSTED)

# Deadline outcomes (contract section 5).
DL_CHALLENGER = "MATERIAL_CHALLENGER_IN_TRUE_FORWARD_COMPETITION"
DL_EXHAUSTED = "OWNED_FREE_INFORMATION_EXHAUSTED"
DL_BREACH = "STOP_LOSS_BREACH"
DEADLINE_OUTCOMES = (DL_CHALLENGER, DL_EXHAUSTED, DL_BREACH)

# Completion tokens (contract section 9).
TK_READY = "ALPHA_RECOVERY_MATERIAL_CHALLENGER_READY"
TK_EXHAUSTED = "ALPHA_RECOVERY_OWNED_FREE_INFORMATION_EXHAUSTED"
TK_IN_PROGRESS = "ALPHA_RECOVERY_IN_PROGRESS_WITHIN_STOP_LOSS"
TK_DO_NOT_COMMIT = "DO_NOT_COMMIT"
COMPLETION_TOKENS = (TK_READY, TK_EXHAUSTED, TK_IN_PROGRESS, TK_DO_NOT_COMMIT)

# Incumbent verdict vocabulary (workstream 1).
IV_MATERIAL = "INCUMBENT_HAS_MATERIAL_ALPHA"
IV_WEAK = "INCUMBENT_WEAK_OR_UNPROVEN"
IV_NEGATIVE = "INCUMBENT_NEGATIVE_ALPHA_EVIDENCE"
INCUMBENT_VERDICTS = (IV_MATERIAL, IV_WEAK, IV_NEGATIVE)

# Research budgets (contract section 7) and the non-price rule (rule 14).
FAMILY_PRIMARY_MAX = 6
FAMILY_RESCUE_MAX = 2
NON_PRICE_SHARE_MIN = 0.75

# Frozen gates - INHERITED from the existing owners, never re-declared weaker.
HORIZONS = _r63.HORIZONS
LOCKBOX_START = _r63.LOCKBOX_START
BH_Q = _r63.BH_Q
HOLM_ALPHA = _r64.HOLM_ALPHA
CONDITIONAL_T_FLOOR = _r63.CONDITIONAL_T_FLOOR
MIN_EFFECTIVE_PERIODS = _r63.MIN_EFFECTIVE_PERIODS
MATERIALITY_ANN_NET = _r63.MATERIALITY_ANN_NET          # 1.5 %/yr, the R57/R58/R59/R63 floor
MATERIALITY_SHARPE = _r64.MATERIALITY_SHARPE
GATE_HALF_FLOOR = -0.005                                  # R58
GATE_MAX_TURNOVER = 0.40                                  # R58, one-way per period
GATE_DD_MULTIPLE = 1.5                                    # R58
EQ_COST_RATE_PER_SIDE = _r63.EQ_COST_RATE_PER_SIDE
SPY_PROXY_COST_BPS = _r63.SPY_PROXY_COST_BPS
EQ_TOP_N_OPERATIONAL = 25
EQ_TOP_N_SHADOW = 50

SAFETY = {
    "research_only": True,
    "paper_only": True,
    "creates_orders": False,
    "creates_fills": False,
    "broker_enabled": False,
    "promotes_model": False,
    "activates_sleeve": False,
    "approves_proposal": False,
    "registers_forward_challenger": False,
    "mutates_operational_store": False,
    "mutates_live_research_store": False,
    "purchases_data": False,
    "starts_trial_or_subscription": False,
    "automation_enabled": False,
    "automatic_promotion": False,
    "automatic_portfolio_mutation": False,
    "live_checkout_read_only": True,
}

SAFETY_BADGES = ("RESEARCH ONLY", "PREVIEW ONLY", "NO ORDERS", "ORDERS DISABLED",
                 "AUTOMATION OFF", "MANUAL REVIEW", "NO PURCHASE", "NO LIVE REGISTRATION",
                 "NO PROMOTION", "HUMAN-GATED ADOPTION")

stable_hash = _r63.stable_hash
short_hash = _r63.short_hash
normalised_sha256 = _r64.normalised_sha256


def research_root() -> Path:
    return Path(os.environ.get(RESEARCH_ROOT_ENV) or DEFAULT_RESEARCH_ROOT)


def repo_dir() -> Path:
    """The committed campaign directory (checkpoint, scoreboard, report)."""
    return Path(os.environ.get(REPO_DIR_ENV) or DEFAULT_REPO_DIR)


def checkpoint_path() -> Path:
    return repo_dir() / CHECKPOINT_NAME


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def contract_hash() -> str | None:
    return normalised_sha256(CONTRACT_DOC) if CONTRACT_DOC.exists() else None


def protocol() -> dict:
    return json.loads(PROTOCOL_PATH.read_text(encoding="utf-8"))


def protocol_hash() -> str | None:
    return normalised_sha256(PROTOCOL_PATH) if PROTOCOL_PATH.exists() else None


def _stamp(body: dict) -> dict:
    body = dict(body)
    body.setdefault("campaign_id", CAMPAIGN_ID)
    body.setdefault("phase", PHASE)
    body.setdefault("generated_at", now_iso())
    body.setdefault("protocol_sha256", protocol_hash())
    body.setdefault("contract_sha256", contract_hash())
    body.setdefault("safety", dict(SAFETY))
    body["artifact_hash"] = stable_hash(
        {k: v for k, v in body.items() if k not in ("artifact_hash", "generated_at")})
    return body


def _atomic_write(p: Path, body: dict) -> Path:
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(body, indent=1, sort_keys=True, default=str), encoding="utf-8")
    tmp.replace(p)
    return p


def write_artifact(name: str, body: dict, subdir: str = "results") -> Path:
    """Atomically write a hashed, safety-stamped, deterministic artifact under
    the research root. Same contract as ``alpha_agent.r64.write_artifact``:
    sorted keys; the content hash excludes ``generated_at``."""
    caller = body
    stamped = _stamp(body)
    p = _atomic_write(research_root() / subdir / name, stamped)
    for k in ("campaign_id", "phase", "generated_at", "protocol_sha256", "contract_sha256",
              "safety", "artifact_hash"):
        caller[k] = stamped[k]
    return p


def write_repo_artifact(name: str, body: dict) -> Path:
    """The same deterministic contract, into the committed campaign directory."""
    caller = body
    stamped = _stamp(body)
    p = _atomic_write(repo_dir() / name, stamped)
    for k in ("campaign_id", "phase", "generated_at", "protocol_sha256", "contract_sha256",
              "safety", "artifact_hash"):
        caller[k] = stamped[k]
    return p


def read_artifact(name: str, subdir: str = "results"):
    p = research_root() / subdir / name
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def read_repo_artifact(name: str):
    p = repo_dir() / name
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def read_json(path) -> dict | list | None:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def assert_worktree_import() -> str:
    """Fail closed if ``alpha_agent`` resolved to a different checkout (the
    venv's editable finder maps ``paper_trader`` to the live C: checkout)."""
    here = Path(__file__).resolve().parents[1]
    import alpha_agent as _aa
    got = Path(_aa.__file__).resolve().parent
    if got != here:
        raise RuntimeError(
            "alpha_recovery import integrity: alpha_agent resolved to %s but this package "
            "lives in %s - the editable-install finder captured the import" % (got, here))
    return str(here)


def assert_research_root_is_not_live(root: Path | None = None) -> Path:
    """Refuse a research root inside the live checkout or a production store.
    Delegates to the R63 owner of the rule."""
    return _r63.assert_research_root_is_not_live(Path(root or research_root()))


def git_head(repo: Path | None = None) -> str | None:
    """The checkout HEAD, read from .git without a subprocess (a worktree's
    ``.git`` is a file pointing at the gitdir)."""
    root = Path(repo or REPO_ROOT)
    dotgit = root / ".git"
    try:
        if dotgit.is_file():
            gitdir = Path(dotgit.read_text(encoding="utf-8").split("gitdir:", 1)[1].strip())
            if not gitdir.is_absolute():
                gitdir = (root / gitdir).resolve()
        else:
            gitdir = dotgit
        head = (gitdir / "HEAD").read_text(encoding="utf-8").strip()
        if head.startswith("ref:"):
            ref = head.split(" ", 1)[1].strip()
            common = gitdir
            cd = gitdir / "commondir"
            if cd.exists():
                common = (gitdir / cd.read_text(encoding="utf-8").strip()).resolve()
            for base in (gitdir, common):
                p = base / ref
                if p.exists():
                    return p.read_text(encoding="utf-8").strip()
            packed = common / "packed-refs"
            if packed.exists():
                for line in packed.read_text(encoding="utf-8").splitlines():
                    parts = line.split()
                    if len(parts) == 2 and parts[1] == ref:
                        return parts[0]
            return None
        return head
    except (OSError, IndexError):
        return None


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
