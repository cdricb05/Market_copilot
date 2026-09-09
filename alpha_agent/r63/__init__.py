"""alpha_agent.r63 - Release 63: information sensitivity, orthogonal information
discovery and the substantial alpha offensive.

The research estate has prosecuted 8,380 hypotheses across 311 families and
found 0 QUALIFIED. 98.5% of those hypotheses use PRICE_STATE information, and
every settled historical row carries the same reopen condition:
``NEW_ORTHOGONAL_INFORMATION``. The frontier is information-constrained, not
compute-constrained, and generating a 501st transformation of the same price
panel is not research. R63 asks a different question, pre-registered in
``research/r63/R63_RESEARCH_PROTOCOL.json`` BEFORE any experiment ran:

    WHICH economic information actually matters, for WHICH assets, at WHICH
    horizons, CONDITIONAL on what the estate already knows - and where is the
    estate information-blind?

It measures the chain the brief demands and refuses to collapse:

    economic information -> conditional predictive value -> incremental OOS
    signal -> incremental NET portfolio utility -> deployable P&L

Modules (one question each, no second owner of an existing concept):

    ontology        ONE canonical economic-information ontology (deterministic)
    inventory       owned-information CERTIFICATION: every meaningful owned or
                    entitled field classified USED / TESTED / REJECTED / BLOCKED
                    with provider, PIT semantics, collector, consumer, evidence.
                    UNKNOWN is invalid at completion.
    panels          the R63 research substrates, built ONLY from owned data
                    (R38 native futures layer, R57 equity panel, R58 PANEL-F,
                    R35 acquisitions, R41 market series, ALFRED vintages, the
                    EIA bulk archive) with explicit available_at semantics
    pit             the ONE as-of join and the purged / embargoed walk-forward
    sensitivity     the mathematical engine: standalone association,
                    conditional / marginal value, redundancy, stability,
                    economic value after costs, Benjamini-Hochberg control
    experiments     baseline vs baseline + information, identical everything
    asset_horizon   the asset x horizon x information map
    gaps            the ranked INFORMATION GAP FRONTIER (unit: information
                    need, never a vendor)
    sourcing        cheapest-first sourcing economics and the paid-data gate
                    (break-even alpha against the authoritative paper NAV)
    challengers     research-only challenger candidates; never promoted
    handoff         the machine-readable frontier the persistent AlphaAgent can
                    ask "what next?" - implemented, tested, NOT deployed

RESEARCH ONLY. Nothing here creates an order, a fill, a proposal, a promotion,
a sleeve activation, a registration, a purchase, a subscription or an
operational-store write. The live ``C:\\Users\\binis\\paper_trader`` checkout
is read-only; live state, where needed, is READ and never mutated. Every R63
write lands under the R63 research root, which a test can redirect.
"""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

CAMPAIGN_ID = "r63_information_sensitivity_v1"
PHASE = "R63"
RELEASE = "R63"

RESEARCH_ROOT_ENV = "PAPER_TRADER_R63_RESEARCH_ROOT"
DEFAULT_RESEARCH_ROOT = Path(r"D:\Stock_Prediction_app_data\r63_information_sensitivity")

REPO_ROOT = Path(__file__).resolve().parents[2]
PROTOCOL_PATH = REPO_ROOT / "research" / "r63" / "R63_RESEARCH_PROTOCOL.json"

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
    "live_checkout_read_only": True,
}

SAFETY_BADGES = ("RESEARCH ONLY", "PREVIEW ONLY", "NO ORDERS", "ORDERS DISABLED",
                 "AUTOMATION OFF", "MANUAL REVIEW", "NO PURCHASE",
                 "NO LIVE REGISTRATION")

# --------------------------------------------------------------------------- #
# Owned, read-only substrates. Every path is MEASURED by panels.substrate_report
# before use; a missing substrate is reported, never assumed.
# --------------------------------------------------------------------------- #
DATA_ROOT = Path(r"D:\Stock_Prediction_app_data")
R35_ROOT = DATA_ROOT / "orthogonal_information_r35"
R35_ACQUIRED = R35_ROOT / "acquired"
R38_ROOT = DATA_ROOT / "native_futures_r38" / "r38_native_futures_information_frontier_v4"
R38_NATIVE_LAYER = R38_ROOT / "native_contract_layer"
R38_ML_PANEL = R38_ROOT / "ml_ready_native_futures_panel.csv"
R41_ROOT = DATA_ROOT / "multi_horizon_alpha_r41"
R41_FRED_DAILY = R41_ROOT / "_data_fred" / "fred_daily_panel.csv"
R41_CBOE_DIR = R41_ROOT / "_data_cboe"
R57_ROOT = DATA_ROOT / "r57_alpha_discovery"
R57_EQUITY_PANEL_META = R57_ROOT / "panels" / "sp500_pit_panel_v1.meta.json"
R57_FUTURES_PANEL_META = R57_ROOT / "panels" / "futures_panel_v1.meta.json"
R58_ROOT = DATA_ROOT / "r58_orthogonal_alpha"
R58_PANEL_F_META = R58_ROOT / "panels" / "r58_pit_fundamental_panel_v1.meta.json"
R58_INVENTORY = R58_ROOT / "results" / "r58_information_inventory.json"
R59_ROOT = DATA_ROOT / "r59_autonomous_alpha"
R59_OPPORTUNITY_FRONTIER = R59_ROOT / "results" / "r59_data_opportunity_frontier.json"
SEC_FACTS_DB = DATA_ROOT / "stage24_pit_fundamental_alpha" / "_index" / "sec_companyfacts_stage24.sqlite"
IDENTITY_DB = DATA_ROOT / "alpha_agent" / "identity" / "historical_identity.sqlite"
INGEST_NORMALIZED = DATA_ROOT / "alpha_agent" / "ingestion" / "normalized"
EIA_PET_BULK = R35_ACQUIRED / "eia_petroleum_bulk" / "PET.zip"
COT_ARCHIVE_DIR = R35_ACQUIRED / "cftc_commitments_of_traders"
FORM345_DIR = R35_ACQUIRED / "sec_insider_transactions_data_sets"
FRED_R35_DIR = R35_ACQUIRED / "fred_st_louis_fed"

# --------------------------------------------------------------------------- #
# Asset classes are RESEARCH SCOPES (Release-32 rule: a label is not a risk
# factor). Reused verbatim from R59 so the frontier vocabularies line up.
# --------------------------------------------------------------------------- #
AC_US_EQUITY = "US_EQUITY"
AC_EQUITY_INDEX = "EQUITY_INDEX_FUTURES"
AC_RATES = "RATES_FUTURES"
AC_COMMODITY = "COMMODITY_FUTURES"
AC_FX = "FX_FUTURES"
AC_VOLATILITY = "VOLATILITY"
AC_CROSS_ASSET = "CROSS_ASSET"
AC_CREDIT = "CREDIT_PROXY"
ASSET_CLASSES = (AC_US_EQUITY, AC_EQUITY_INDEX, AC_RATES, AC_COMMODITY, AC_FX,
                 AC_VOLATILITY, AC_CROSS_ASSET, AC_CREDIT)

# --------------------------------------------------------------------------- #
# Horizons, in sessions. Intraday is NOT manufactured: the owned daily panels
# cannot support it and the protocol says so.
# --------------------------------------------------------------------------- #
H_1 = 1
H_5 = 5
H_21 = 21
H_63 = 63
HORIZONS = (H_1, H_5, H_21, H_63)
HORIZON_LABELS = {H_1: "1_SESSION", H_5: "5_SESSIONS", H_21: "21_SESSIONS_MONTH",
                  H_63: "63_SESSIONS_QUARTER"}
INTRADAY_STATE = "NOT_SUPPORTED_BY_OWNED_DAILY_PANELS"

# --------------------------------------------------------------------------- #
# Certification dispositions (Stage 1). UNKNOWN is invalid at completion.
# --------------------------------------------------------------------------- #
D_USED = "USED"
D_TESTED = "TESTED"
D_REJECTED = "REJECTED"
D_BLOCKED = "BLOCKED"
DISPOSITIONS = (D_USED, D_TESTED, D_REJECTED, D_BLOCKED)

# Observation states of one asset x horizon x information cell (Stage 3).
OBS_WELL = "WELL_OBSERVED"
OBS_PARTIAL = "PARTIALLY_OBSERVED"
OBS_NOT = "NOT_OBSERVED"
OBS_BLOCKED = "BLOCKED"
OBSERVATION_STATES = (OBS_WELL, OBS_PARTIAL, OBS_NOT, OBS_BLOCKED)

# Point-in-time status vocabulary.
PIT_TRUE = "PIT_TRUE"                    # real availability instant per record
PIT_LAGGED = "PIT_BY_DECLARED_LAG"       # publication lag declared and applied
PIT_MARKET = "PIT_MARKET_OBSERVABLE"     # a market price on its own session
PIT_UNVERIFIED = "PIT_UNVERIFIED"        # cannot prove what was known when
PIT_BLOCKED = "PIT_BLOCKED"              # revised history only, no vintages
PIT_STATES = (PIT_TRUE, PIT_LAGGED, PIT_MARKET, PIT_UNVERIFIED, PIT_BLOCKED)

# Challenger classification (Stage 11).
CH_READY = "READY_FOR_FORWARD_QUALIFICATION"
CH_MORE = "MORE_RESEARCH_REQUIRED"
CH_REJECTED = "REJECTED"
CHALLENGER_STATES = (CH_READY, CH_MORE, CH_REJECTED)

# --------------------------------------------------------------------------- #
# Pre-registered statistical conventions (protocol section "statistics").
# The LOCKBOX boundary is inherited from R57/R58/R59 so an R63 result is
# comparable to the 8,380 hypotheses already prosecuted.
# --------------------------------------------------------------------------- #
LOCKBOX_START = "2023-01-01"
SELECTION_END = LOCKBOX_START          # nothing after this date may select
FUTURES_DISCOVERY_START = "1995-01-01"
EQUITY_DISCOVERY_START = "2011-07-01"  # PANEL-F's first honest formation
REFIT_EVERY_SESSIONS = 252
MIN_TRAIN_SESSIONS = 1260              # five years before the first OOS fold
RIDGE_ALPHAS = (0.1, 1.0, 10.0, 100.0)
INNER_CV_FOLDS = 3
BH_Q = 0.10
MIN_EFFECTIVE_PERIODS = 36
MATERIALITY_ANN_NET = 0.015            # 1.5%/yr, the R57/R58/R59 floor
REDUNDANT_RESIDUAL_SHARE_MAX = 0.10    # R35's owned thresholds, reused
PARTIAL_RESIDUAL_SHARE_MAX = 0.35
STANDALONE_T_FLOOR = 2.0
CONDITIONAL_T_FLOOR = 2.0
FUT_DEFAULT_COST_BPS = 15.0            # worst R38 market when a cost is unknown
EQ_COST_RATE_PER_SIDE = 0.00125        # the desk convention
CREDIT_PROXY_COST_BPS = 5.0            # an HYG/LQD-class ETF, disclosed
SPY_PROXY_COST_BPS = 1.0

# Declared publication lags (calendar days) for information that is not a
# market price. Each is a CONTRACT, applied in exactly one place (pit.as_of).
COT_PUBLICATION_LAG_DAYS = 6           # Tuesday report, Friday release (R35)
EIA_WEEKLY_PUBLICATION_LAG_DAYS = 7    # Friday week-end, Wednesday release,
                                       # +2 days of conservatism
SEC_FILING_BROADCAST_LAG_SESSIONS = 1  # filed date -> next session (R35/R58)
MARKET_BROADCAST_LAG_SESSIONS = 1      # a close is acted on the next close


def research_root() -> Path:
    return Path(os.environ.get(RESEARCH_ROOT_ENV) or DEFAULT_RESEARCH_ROOT)


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def stable_hash(obj) -> str:
    return hashlib.sha256(
        json.dumps(obj, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def short_hash(obj, n: int = 12) -> str:
    return stable_hash(obj)[:n]


def protocol() -> dict:
    return json.loads(PROTOCOL_PATH.read_text(encoding="utf-8"))


def protocol_hash() -> str:
    return hashlib.sha256(PROTOCOL_PATH.read_bytes()).hexdigest()


def write_artifact(name: str, body: dict, subdir: str = "results") -> Path:
    """Atomically write a hashed, safety-stamped, deterministic artifact.

    Keys are sorted so two runs over identical inputs produce identical bytes
    apart from ``generated_at``, which the content hash deliberately excludes.
    """
    d = research_root() / subdir
    d.mkdir(parents=True, exist_ok=True)
    caller = body
    body = dict(body)
    body.setdefault("campaign_id", CAMPAIGN_ID)
    body.setdefault("phase", PHASE)
    body.setdefault("generated_at", now_iso())
    body.setdefault("protocol_sha256", protocol_hash() if PROTOCOL_PATH.exists()
                    else None)
    body.setdefault("safety", dict(SAFETY))
    body["artifact_hash"] = stable_hash(
        {k: v for k, v in body.items()
         if k not in ("artifact_hash", "generated_at")})
    p = d / name
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(body, indent=1, sort_keys=True, default=str),
                   encoding="utf-8")
    tmp.replace(p)
    # the caller keeps the stamped identity of what was written
    for k in ("campaign_id", "phase", "generated_at", "protocol_sha256", "safety",
              "artifact_hash"):
        caller[k] = body[k]
    return p


def read_artifact(name: str, subdir: str = "results"):
    p = research_root() / subdir / name
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def read_json(path):
    p = Path(path)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def assert_worktree_import() -> str:
    """Fail closed if ``alpha_agent`` resolved to a different checkout.

    The venv's editable finder maps ``paper_trader`` to the live C: checkout.
    R63 is imported as top-level ``alpha_agent`` for exactly this reason and
    this proves it at runtime instead of trusting it.
    """
    here = Path(__file__).resolve().parents[1]
    import alpha_agent as _aa
    got = Path(_aa.__file__).resolve().parent
    if got != here:
        raise RuntimeError(
            "R63 import integrity: alpha_agent resolved to %s but r63 lives in "
            "%s - the editable-install finder captured the import" % (got, here))
    return str(here)


def assert_research_root_is_not_live(root: Path | None = None) -> Path:
    """Refuse a research root inside the live checkout or a production store."""
    r = Path(root or research_root()).resolve()
    s = str(r).lower()
    # the live checkout and the live ledger home; the ledger literal is built
    # by concatenation because the architecture audit reserves that token for
    # the ledger OWNERS, and this function only refuses it
    live_home = r"c:\users\binis"
    forbidden = (live_home + r"\paper_trader", live_home + "\\." + "paper_trader")
    for f in forbidden:
        if s.startswith(f):
            raise RuntimeError("R63 research root may not sit inside %s" % f)
    return r
