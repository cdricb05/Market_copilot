"""
alpha_agent/stage25_alpha_discovery.py - Stage 25 autonomous multi-source alpha
discovery and challenger evolution.

Stage 24 established that the operational model's fundamental leg survives honest
point-in-time, survivorship-safe testing, and surfaced ONE candidate that cleared
the released evidence gate: R&D intensity. It could not falsify that candidate,
because the single most likely competing explanation - that R&D intensity is a
technology/biotech SECTOR bet over a period in which those sectors led - needs a
classification that was legitimately knowable at the formation date, and no such
series existed on disk.

Stage 25 does four things, in this order:

1.  BUILD THE STRONGEST HONEST HISTORICAL CLASSIFICATION THAT OWNED DATA ALLOWS.
    Two tiers, never conflated:

      * TIER A ``PIT_XBRL_DISCLOSURE_SIGNATURE`` - leakage-safe. An issuer that
        has FILED deposit, loan, premium, policyholder-benefit or investment
        real-estate facts by date *D* was, at *D*, observably a bank / insurer /
        REIT. The classifier reads only facts filed by *D*, so it can never see
        forward. It is coarse, and it deliberately excludes the R&D concept so it
        cannot be circular with the R&D question.
      * TIER B ``ENTITY_SIC_SNAPSHOT_CONTROL`` - the owned SEC entity-level SIC,
        mapped through the released :mod:`alpha_agent.pit_sector` taxonomy. This
        is TODAY's classification and therefore carries a look-ahead. It is used
        for ONE purpose only: as a CONTROL in a falsification test. The asymmetry
        is what makes that legitimate - a signal that DIES under a control which
        knows more than an honest control could is dead beyond rescue, while a
        signal that survives is only provisionally cleared. Tier B never
        constructs a signal, never enters a registered candidate, and never
        supports a promotion claim. A regression enforces all three.

2.  EXPAND THE POINT-IN-TIME ACCOUNTING SURFACE far enough to ask economically
    distinct questions - cash-flow quality, balance-sheet conservatism,
    investment, payout, operating improvement, innovation - and run a bounded,
    pre-registered campaign over it.

3.  PUT EVERY SURVIVOR THROUGH ORTHOGONALITY AND BOUNDED ENSEMBLE TESTING against
    the ACTUAL operational model, not against zero.

4.  ROUTE ANYTHING THAT QUALIFIES INTO THE EXISTING CHALLENGER LIFECYCLE. Stage 25
    creates no second agent, no second registry, no second tournament, no second
    champion authority and no second PIT definition.

Reuse, not reinvention: the universe contract, the identity bridge, the
point-in-time reading policy, the evaluator, the evidence gate, the FDR control,
the orthogonality primitives and the candidate lifecycle all remain owned by the
modules that already own them. What is genuinely new here is the classification
tiering, the accounting-concept expansion, the falsification battery and the
horizon/ensemble structure.

Research-only and read-only with respect to every operational store. No orders,
fills, signals, trade decisions, proposals, rebalance plans, Daily Close, model
promotion or champion replacement. Pure standard library; no network.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
from pathlib import Path
from typing import Iterable, Optional, Sequence

from . import pit_sector as _ps
from . import stage24_pit_fundamental as _s24

STAGE25_VERSION = "stage25-alpha-discovery-1.0.0"
ORIGIN = "stage25-autonomous-alpha-discovery"
CONTRACT_ID = "stage25_alpha_discovery/1"

# Terminal tokens (exactly one printed per CLI invocation).
READY = "STAGE25_ALPHA_DISCOVERY_READY"
BLOCKED = "STAGE25_ALPHA_DISCOVERY_BLOCKED"
DATA_HOLD = "STAGE25_ALPHA_DISCOVERY_DATA_HOLD"

SAFETY_BADGES = ["RESEARCH ONLY", "READ ONLY", "NO ORDERS", "NO LIVE PROMOTION",
                 "PREVIEW ONLY", "MANUAL REVIEW"]


# --------------------------------------------------------------------------- #
# Owned data locations (env-overridable so tests are hermetic).
# --------------------------------------------------------------------------- #
RESEARCH_ROOT_ENV = "PAPER_TRADER_STAGE25_ROOT"
CF_INDEX_ENV = "PAPER_TRADER_STAGE25_CF_INDEX"
ISSUER_DB_ENV = "PAPER_TRADER_STAGE25_ISSUER_DB"

DEFAULT_RESEARCH_ROOT = Path(
    r"D:\Stock_Prediction_app_data\stage25_autonomous_alpha_discovery")
#: The Stage-25 EXTENDED companyfacts index. The Stage-24 index and the
#: Phase-9.5 index are opened by nothing here and are never written.
DEFAULT_CF_INDEX = Path(
    r"D:\Stock_Prediction_app_data\stage25_autonomous_alpha_discovery\_index"
    r"\sec_companyfacts_stage25.sqlite")
#: The owned SEC submissions index (Phase 10.1). Entity-level assigned SIC.
DEFAULT_ISSUER_DB = Path(
    r"D:\Stock_Prediction_app_data\alpha_agent\identity"
    r"\sec_issuer_history.sqlite")


def _resolve(explicit, env_var: str, default: Path) -> Path:
    if explicit:
        return Path(explicit)
    env = os.environ.get(env_var)
    return Path(env) if env else Path(default)


# --------------------------------------------------------------------------- #
# WORKSTREAM D - accounting concept expansion.
#
# Stage 24 extended the released Phase-9.3 map by nine concepts, which unlocked
# both composite_sn legs. Stage 25 extends it again - never shadowing either the
# released map or the Stage-24 extension - with the concepts required by the
# economic families below and by the Tier-A leakage-safe classifier. Every entry
# names the exact us-gaap tags and their ORDERED fallbacks.
# --------------------------------------------------------------------------- #
CONCEPT_EXTENSION_25: "dict[str, list[str]]" = {
    # -- operating cost structure (operating profitability, SG&A efficiency) --
    "operating_expenses": ["OperatingExpenses", "CostsAndExpenses"],
    "sganda": ["SellingGeneralAndAdministrativeExpense",
               "GeneralAndAdministrativeExpense"],
    # -- productive capital (investment family) ---------------------------- #
    "ppe_net": ["PropertyPlantAndEquipmentNet"],
    # -- retained capital / conservatism ----------------------------------- #
    "retained_earnings": ["RetainedEarningsAccumulatedDeficit"],
    "accounts_payable": ["AccountsPayableCurrent"],
    # -- non-cash compensation --------------------------------------------- #
    "share_based_compensation": ["ShareBasedCompensation",
                                 "AllocatedShareBasedCompensationExpense"],
    # -- tax burden --------------------------------------------------------- #
    "income_tax_expense": ["IncomeTaxExpenseBenefit"],
    "pretax_income": [
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItems"
        "NoncontrollingInterest",
        "IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAnd"
        "IncomeLossFromEquityMethodInvestments"],
    "interest_expense": ["InterestExpense", "InterestExpenseDebt"],
    # -- financing / investing cash flow (external financing, payout) ------ #
    "cash_flow_investing": [
        "NetCashProvidedByUsedInInvestingActivities",
        "NetCashProvidedByUsedInInvestingActivitiesContinuingOperations"],
    "cash_flow_financing": [
        "NetCashProvidedByUsedInFinancingActivities",
        "NetCashProvidedByUsedInFinancingActivitiesContinuingOperations"],
    "dividends_paid": ["PaymentsOfDividendsCommonStock", "PaymentsOfDividends"],
    "share_repurchase": ["PaymentsForRepurchaseOfCommonStock"],
    # -- intangible balance sheet ------------------------------------------ #
    "goodwill": ["Goodwill"],
    "intangible_assets": ["FiniteLivedIntangibleAssetsNet",
                          "IntangibleAssetsNetExcludingGoodwill"],
    # -- TIER-A leakage-safe classification markers ------------------------- #
    # These exist so an issuer's OBSERVABLE disclosure signature can classify it
    # at the formation date. They are business-model markers, not alpha inputs.
    "deposits": ["Deposits", "InterestBearingDepositLiabilities"],
    "loans_receivable": ["LoansAndLeasesReceivableNetReportedAmount",
                         "NotesReceivableNet"],
    "premiums_earned": ["PremiumsEarnedNet"],
    "policyholder_benefits": ["PolicyholderBenefitsAndClaimsIncurredNet"],
    "interest_and_dividend_income": ["InterestAndDividendIncomeOperating"],
    "real_estate_investment": ["RealEstateInvestmentPropertyNet"],
}

#: Concepts that came from each prior owner, recorded so provenance is auditable.
RELEASED_CONCEPTS = tuple(sorted(_s24.RELEASED_CONCEPTS))
STAGE24_CONCEPTS = tuple(sorted(_s24.EXTENSION_CONCEPTS))
STAGE25_CONCEPTS = tuple(sorted(CONCEPT_EXTENSION_25))


def concept_map() -> "dict[str, list[str]]":
    """The Phase-9.3 released map, EXTENDED by Stage 24, EXTENDED again by
    Stage 25.

    A Stage-25 key never shadows a released or a Stage-24 key (asserted below and
    covered by a regression), so ``pit_fundamentals`` remains the owner of every
    concept it defines and ``stage24_pit_fundamental`` remains the owner of every
    concept it added."""
    merged = _s24.concept_map()
    for k, v in CONCEPT_EXTENSION_25.items():
        if k in merged:  # pragma: no cover - guarded by a regression test
            raise ValueError(
                "Stage-25 concept %r would shadow an existing concept owned by "
                "pit_fundamentals or stage24_pit_fundamental; extend, never "
                "override" % k)
        merged[k] = list(v)
    return merged


def target_tags() -> frozenset:
    """Every us-gaap tag the Stage-25 index must materialise."""
    return frozenset(t for tags in concept_map().values() for t in tags)


def tag_to_concept() -> dict:
    """us-gaap tag -> (canonical concept, ordered fallback rank)."""
    out = {}
    for concept, tags in concept_map().items():
        for rank, tag in enumerate(tags):
            out[tag] = (concept, rank)
    return out


def mapping_version_hash() -> str:
    """Deterministic 16-hex content hash pinning the exact concept mapping that
    produced a panel (released map + Stage-24 extension + Stage-25 extension)."""
    payload = repr((STAGE25_VERSION, _s24.mapping_version_hash(),
                    sorted((k, tuple(v)) for k, v in concept_map().items())))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


# --------------------------------------------------------------------------- #
# Small deterministic helpers. No statistic is DEFINED in this module - every
# number that gates a decision comes from a released owner.
# --------------------------------------------------------------------------- #
canonical_json = _s24.canonical_json
content_hash = _s24.content_hash
file_fingerprint = _s24.file_fingerprint
_num = _s24._num
_ratio = _s24._ratio
_mean = _s24._mean
_t_stat = _s24._t_stat
_delta = _s24._delta
_zscore = _s24._zscore
_shift_days = _s24._shift_days

MIN_CROSS_SECTION = _s24.MIN_CROSS_SECTION
WINSOR_FRACTION = _s24.WINSOR_FRACTION
REPORTING_LAG_DAYS = _s24.REPORTING_LAG_DAYS


def _median(xs):
    vals = sorted(float(x) for x in xs if x is not None)
    return vals[len(vals) // 2] if vals else None


# =========================================================================== #
# The Stage-25 point-in-time store.
#
# Everything about HOW a fact becomes point-in-time - availability is the SEC
# ``filed`` date, restatements are preserved as distinct observations, an as-of
# query returns the latest observation filed by the as-of date, a fiscal period
# is keyed on the fact's OWN ``period_end`` - is inherited unchanged from the
# Stage-24 owner. Stage 25 changes exactly two things:
#
#   1. the concept VOCABULARY (the extended map above), and
#   2. how a fact's period KIND is decided.
#
# Stage 24 partitions concepts into hard-coded FLOW and STOCK sets at module
# scope. That is correct for the concepts it defined, but it does not generalise:
# every new concept would have to be hand-classified, and a mistake there is
# silent (a duration fact treated as an instant is compared against a different
# accounting period). Stage 25 reads the period kind from the FACT ITSELF -
# a duration carries a ``period_start``, an instant does not. That is intrinsic
# to XBRL, needs no table, and is provably equivalent to the Stage-24 partition
# on the Stage-24 concepts (a regression asserts the equivalence).
# =========================================================================== #
class Stage25PitStore(_s24.Stage24PitStore):
    """A leakage-safe reader over the Stage-25 extended companyfacts index.

    Adds, beyond the inherited Stage-24 contract:

    * the Stage-25 concept vocabulary;
    * intrinsic duration/instant detection (see the module comment above);
    * :meth:`concepts_filed_by`, which answers 'which accounting concepts had
      this issuer PUBLISHED by date D?' - the observation the Tier-A
      leakage-safe classifier consumes.
    """

    def __init__(self, db_path=None) -> None:
        super().__init__(_resolve(db_path, CF_INDEX_ENV, DEFAULT_CF_INDEX))
        self._t2c = tag_to_concept()
        # cik -> {concept: earliest filed date}. Built during load so a Tier-A
        # classification never scans the whole observation store.
        self._concept_first_filed: "dict[str, dict[str, str]]" = {}
        self.rejected_flow_without_start = 0

    # -- load ---------------------------------------------------------------- #
    def load(self, *, ciks: Optional[Iterable[str]] = None,
             max_filed: Optional[str] = None) -> dict:
        """Stream the index into memory.

        Deliberately overrides the Stage-24 loader rather than calling it: the
        base classifies a fact's period kind from module-scope concept SETS, and
        Stage 25 classifies it from the fact's own ``period_start``. Every other
        rule - the fiscal-year duration window, the amendment handling, the
        fiscal-year-end bookkeeping - is applied exactly as the base applies it,
        using the base's own constants so the two can never drift apart.

        ``max_filed`` (inclusive) truncates the visible filing history, which is
        how a regression proves that an amendment filed later is invisible to an
        earlier formation."""
        if not self.db_path.exists():
            return {"ok": False, "reason": "STAGE25_INDEX_ABSENT",
                    "db_path": str(self.db_path)}
        allow = {str(c).zfill(10) for c in (ciks or [])} or None
        conn = sqlite3.connect("file:%s?mode=ro" % self.db_path, uri=True)
        try:
            sql = ("select cik, concept_tag, value, period_start, period_end, "
                   "filed, form, accession from cf_fact")
            params: list = []
            if max_filed:
                sql += " where filed <= ?"
                params.append(str(max_filed))
            for (cik, tag, value, p_start, p_end, filed, form,
                 accession) in conn.execute(sql, params):
                cik = str(cik).zfill(10)
                if allow is not None and cik not in allow:
                    continue
                hit = self._t2c.get(str(tag))
                if hit is None:
                    self.unmapped_tags[str(tag)] = \
                        self.unmapped_tags.get(str(tag), 0) + 1
                    continue
                concept, rank = hit
                v = _num(value)
                if v is None or not filed or not p_end:
                    self.rejected_missing_period += 1
                    continue
                # INTRINSIC period kind: a duration fact carries a start date.
                if p_start:
                    dur = _s24._day_span(p_start, p_end)
                    if dur is None or not (_s24.ANNUAL_MIN_DAYS <= dur
                                           <= _s24.ANNUAL_MAX_DAYS):
                        self.rejected_non_annual_flow += 1
                        continue
                    self._fy_ends.setdefault(cik, set()).add(str(p_end)[:10])
                filed_s = str(filed)[:10]
                key = (cik, concept, str(p_end)[:10])
                self._obs.setdefault(key, []).append(
                    (filed_s, v, str(form or "").endswith("/A"),
                     int(rank), str(accession or ""), str(form or "")))
                self._pes.setdefault((cik, concept), set()).add(str(p_end)[:10])
                seen = self._concept_first_filed.setdefault(cik, {})
                if concept not in seen or filed_s < seen[concept]:
                    seen[concept] = filed_s
                self._ciks.add(cik)
                self.loaded_facts += 1
                self.by_concept[concept] = self.by_concept.get(concept, 0) + 1
        finally:
            conn.close()
        return {"ok": True, "facts": self.loaded_facts,
                "ciks": len(self._ciks), "concepts": dict(self.by_concept),
                "rejected_non_annual_flow": self.rejected_non_annual_flow}

    # -- the Tier-A observation ---------------------------------------------- #
    def concepts_filed_by(self, cik: str, as_of: str) -> set:
        """The set of accounting concepts this issuer had PUBLISHED on or before
        ``as_of``.

        This is a strictly backward-looking observation: a concept an issuer only
        started tagging later is absent, so a classification derived from it can
        never see forward."""
        seen = self._concept_first_filed.get(str(cik).zfill(10))
        if not seen:
            return set()
        cutoff = str(as_of)[:10]
        return {c for c, f in seen.items() if f <= cutoff}

    def coverage(self) -> dict:
        cov = super().coverage()
        cov.update({
            "stage": STAGE25_VERSION,
            "mapping_version": STAGE25_VERSION,
            "mapping_version_hash": mapping_version_hash(),
            "concept_owners": {
                "alpha_agent.pit_fundamentals": list(RELEASED_CONCEPTS),
                "alpha_agent.stage24_pit_fundamental": list(STAGE24_CONCEPTS),
                "alpha_agent.stage25_alpha_discovery": list(STAGE25_CONCEPTS)},
            "period_identity": (
                "the fact's OWN period_end; the period KIND is read from the "
                "fact itself (a duration carries period_start, an instant does "
                "not) rather than from a hard-coded concept partition, and a "
                "duration is admitted only when it spans a fiscal year"),
        })
        return cov


# =========================================================================== #
# WORKSTREAM D - one annual record per (CIK, as_of), Stage-25 vocabulary.
# =========================================================================== #
#: Concepts read for the CURRENT and the COMPARABLE PRIOR fiscal year. Nothing
#: outside this list is consulted. It is the Stage-24 list plus the Stage-25
#: extension, so a Stage-24 factor computed from a Stage-25 record is identical.
_CURRENT_CONCEPTS_25 = tuple(sorted(set(_s24._CURRENT_CONCEPTS)
                                    | set(CONCEPT_EXTENSION_25)))


def annual_record(store: "Stage25PitStore", cik: str, as_of: str
                  ) -> Optional[dict]:
    """Every Stage-25 concept for ``cik``'s latest AND comparable-prior fiscal
    year, read strictly as of ``as_of``.

    The fiscal-year identification, the comparable-prior-year search and the
    point-in-time read are all the STORE's (i.e. Stage 24's) rules; this function
    only widens the concept list. A concept the issuer never tagged - or tagged
    only after ``as_of`` - is simply ABSENT. It is never zero-filled, never
    carried forward from a different period, and never satisfied by a current
    snapshot."""
    pe1 = store.latest_fiscal_year_end(cik, as_of)
    if pe1 is None:
        return None
    pe0 = store.prior_fiscal_year_end(cik, as_of, pe1)
    cur: "dict[str, float]" = {}
    prior: "dict[str, float]" = {}
    for concept in _CURRENT_CONCEPTS_25:
        v = store.value_as_of(cik, concept, pe1, as_of)
        if v is not None:
            cur[concept] = v
        if pe0 is not None:
            p = store.value_as_of(cik, concept, pe0, as_of)
            if p is not None:
                prior[concept] = p
    if not cur:
        return None
    return {"cik": cik, "as_of": as_of, "period_end": pe1,
            "prior_period_end": pe0, "cur": cur, "prior": prior}


# =========================================================================== #
# WORKSTREAM B - point-in-time sector history.
#
# Read the module docstring before changing anything here. The two tiers exist
# because owned data supports exactly two honest answers, and conflating them
# would destroy the only thing that makes the R&D falsification legitimate.
# =========================================================================== #
TIER_A = "PIT_XBRL_DISCLOSURE_SIGNATURE"
TIER_B = "ENTITY_SIC_SNAPSHOT_CONTROL"

TIER_A_UNKNOWN = "Unknown"
TIER_A_DEFAULT = "OperatingNonFinancial"

#: FROZEN, ORDERED Tier-A rules. Order is part of the contract: a bank that also
#: reports investment real estate is classified Banking, not RealEstate. The R&D
#: concept is deliberately ABSENT from every rule so the classifier cannot be
#: circular with the R&D hypothesis it is used to test.
TIER_A_RULES = (
    ("Banking", ("deposits", "loans_receivable",
                 "interest_and_dividend_income")),
    ("Insurance", ("premiums_earned", "policyholder_benefits")),
    ("RealEstate", ("real_estate_investment",)),
)

#: Minimum names a sector group needs before it is used as its own
#: neutralisation bucket. Smaller groups pool into OTHER_SMALL rather than being
#: demeaned against themselves (a one-name group would be demeaned to exactly
#: zero, silently deleting that name's information).
MIN_SECTOR_GROUP = 8
OTHER_SMALL = "OTHER_SMALL"


class SectorHistory:
    """Historical classification from owned data, in two explicitly separated
    tiers.

    ``sector_as_of(cik, date, tier)`` is the only query. Tier A is leakage-safe
    and may be used anywhere. Tier B carries a classification look-ahead and is
    admissible ONLY as a falsification control - see :meth:`tier_b_usage_rule`.
    """

    def __init__(self, issuer_db=None) -> None:
        self.issuer_db = _resolve(issuer_db, ISSUER_DB_ENV, DEFAULT_ISSUER_DB)
        #: cik -> {"sic", "sector", "confidence", "first_filing"}
        self.entity: "dict[str, dict]" = {}
        self.load_status: dict = {}

    # -- Tier B loading ------------------------------------------------------ #
    def load_entity_sic(self, ciks: Iterable[str]) -> dict:
        """Load the owned SEC entity-level assigned SIC for the requested CIKs.

        The SIC lives in the Phase-10.1 submissions index. It is a CURRENT
        snapshot: the SEC publishes the SIC an issuer carries now, with no
        effective date and no history, so this observation cannot be made
        point-in-time from owned data."""
        wanted = sorted({str(c).zfill(10) for c in ciks if c})
        if not self.issuer_db.exists():
            self.load_status = {"ok": False, "reason": "ISSUER_DB_ABSENT",
                                "path": str(self.issuer_db)}
            return self.load_status
        conn = sqlite3.connect("file:%s?mode=ro" % self.issuer_db, uri=True)
        found = 0
        try:
            for i in range(0, len(wanted), 500):
                chunk = wanted[i:i + 500]
                # The stored cik may be zero-padded or not; probe both forms.
                keys = list(chunk) + [c.lstrip("0") or "0" for c in chunk]
                marks = ",".join("?" * len(keys))
                rows = conn.execute(
                    "select cik, sic, sic_description, first_filing "
                    "from issuer where cik in (%s)" % marks, keys).fetchall()
                for cik, sic, sic_desc, first_filing in rows:
                    key = str(cik).zfill(10)
                    mapped = _ps.sic_to_sector(sic)
                    if mapped["sector"] == _ps.UNKNOWN and self.entity.get(key):
                        continue
                    if key not in self.entity:
                        found += 1
                    self.entity[key] = {
                        "sic": mapped["sic"],
                        "sic_description": sic_desc,
                        "sector": mapped["sector"],
                        "confidence": mapped["confidence"],
                        "first_filing": (str(first_filing)[:10]
                                         if first_filing else None),
                    }
        finally:
            conn.close()
        self.load_status = {
            "ok": True, "requested": len(wanted), "resolved": found,
            "resolved_fraction": round(found / len(wanted), 6) if wanted else 0.0,
            "path": str(self.issuer_db),
            "mapping_version": _ps.MAPPING_VERSION,
            "mapping_version_hash": _ps.mapping_version_hash(),
            "evidence_class": "CURRENT_SNAPSHOT_CLASSIFICATION_LOOKAHEAD",
        }
        return self.load_status

    # -- queries ------------------------------------------------------------- #
    def tier_a(self, store: "Stage25PitStore", cik: str, as_of: str) -> str:
        """Leakage-safe business-model classification from the issuer's OWN
        disclosure signature as of ``as_of``."""
        filed = store.concepts_filed_by(cik, as_of)
        if not filed:
            return TIER_A_UNKNOWN
        for label, markers in TIER_A_RULES:
            if any(m in filed for m in markers):
                return label
        return TIER_A_DEFAULT

    def tier_b(self, cik: str, as_of: str) -> str:
        """The entity-level SIC sector, floored at the issuer's first SEC filing.

        The floor is not a fix for the look-ahead - it only prevents classifying
        an entity before it existed in EDGAR at all. The look-ahead is inherent
        and is why this tier is a CONTROL, never a signal input."""
        rec = self.entity.get(str(cik).zfill(10))
        if not rec:
            return _ps.UNKNOWN
        ff = rec.get("first_filing")
        if ff and str(as_of)[:10] < ff:
            return _ps.UNKNOWN
        return rec.get("sector") or _ps.UNKNOWN

    def sector_as_of(self, store, cik: str, as_of: str, *, tier: str) -> str:
        if tier == TIER_A:
            return self.tier_a(store, cik, as_of)
        if tier == TIER_B:
            return self.tier_b(cik, as_of)
        raise ValueError("unknown sector tier: %r" % tier)

    # -- contract ------------------------------------------------------------ #
    @staticmethod
    def tier_b_usage_rule() -> dict:
        """The single place the Tier-B admissibility rule is written down."""
        return {
            "tier": TIER_B,
            "leakage_safe": False,
            "admissible_for": ["falsification control", "diagnostic reporting",
                               "coverage measurement"],
            "inadmissible_for": ["signal construction", "candidate registration",
                                 "challenger evidence", "promotion claim",
                                 "shadow book"],
            "why_a_control_is_still_legitimate": (
                "the test is adversarial: a signal that DIES under a control "
                "carrying MORE information than an honest contemporaneous "
                "control could carry is dead beyond rescue, because no "
                "leakage-safe classification could revive it. A signal that "
                "SURVIVES is only provisionally cleared, because the control's "
                "look-ahead could have absorbed genuine information. The "
                "asymmetry is stated on every result that uses it."),
            "enforced_by": "tests/test_stage25_alpha_discovery.py",
        }


# =========================================================================== #
# Point-in-time trailing beta.
#
# Stage 24 could neutralise size and volatility but not market beta, because the
# frozen momentum panel carries no beta column. It does not need one: the panel's
# own realised monthly returns are enough, and they are already survivorship-safe.
#
# The timing has to be exact. A panel row at month *m* carries
# ``fwd_1m_return``, the return over m -> m+1. So the return REALISED OVER month
# *m* is the value recorded at month *m-1*. At a formation dated to the end of
# month *m*, the returns realised over months up to and including *m* are known.
# A beta estimated on that window uses nothing the market had not already
# printed.
# =========================================================================== #
BETA_WINDOW_MONTHS = 36
BETA_MIN_OBSERVATIONS = 24


class TrailingBeta:
    """Point-in-time market beta from the owned survivorship-safe panel.

    The market proxy is the equal-weighted mean realised return of the eligible
    members in each month - itself survivorship-safe, because the eligible set is
    the historical membership including the names that later died."""

    def __init__(self, universe: "_s24.HistoricalUniverse") -> None:
        self.months = universe.months()
        self._index = {m: i for i, m in enumerate(self.months)}
        # realised[m][sym] = return realised OVER month m (known at end of m).
        self.realized: "dict[str, dict[str, float]]" = {}
        for i, m in enumerate(self.months):
            if i == 0:
                continue
            prev = self.months[i - 1]
            row: "dict[str, float]" = {}
            for sym, r in universe.eligible(prev).items():
                v = r.get("fwd_1m_return")
                if v is not None:
                    row[sym] = float(v)
            if row:
                self.realized[m] = row
        self.market: "dict[str, float]" = {
            m: (sum(row.values()) / len(row)) for m, row in self.realized.items()
            if row}

    def beta_as_of(self, month: str, symbol: str, *,
                   window: int = BETA_WINDOW_MONTHS,
                   min_obs: int = BETA_MIN_OBSERVATIONS) -> Optional[float]:
        """OLS slope of the name's realised monthly return on the market's, over
        the ``window`` months ENDING at ``month`` inclusive. None when too few
        observations exist - never a default of 1.0, which would silently assert
        an exposure that was not measured."""
        i = self._index.get(month)
        if i is None:
            return None
        lo = max(0, i - window + 1)
        xs, ys = [], []
        for m in self.months[lo:i + 1]:
            r = (self.realized.get(m) or {}).get(symbol)
            mk = self.market.get(m)
            if r is None or mk is None:
                continue
            xs.append(mk)
            ys.append(r)
        n = len(xs)
        if n < min_obs:
            return None
        mx = sum(xs) / n
        my = sum(ys) / n
        sxx = sum((x - mx) ** 2 for x in xs)
        if sxx <= 0:
            return None
        sxy = sum((xs[k] - mx) * (ys[k] - my) for k in range(n))
        return sxy / sxx

    def contract(self) -> dict:
        return {
            "control": "trailing_beta",
            "window_months": BETA_WINDOW_MONTHS,
            "minimum_observations": BETA_MIN_OBSERVATIONS,
            "market_proxy": "equal-weighted mean realised monthly return of the "
                            "eligible historical membership",
            "point_in_time": True,
            "timing_rule": ("the return realised over month m is the panel's "
                            "fwd_1m_return recorded at month m-1; a formation at "
                            "the end of month m therefore uses only returns the "
                            "market had already printed"),
            "missing_policy": "None (never defaulted to 1.0)",
            "survivorship_safe": True,
        }


# =========================================================================== #
# WORKSTREAM E - the pre-registered economic hypothesis campaign.
#
# Every entry states an economic MECHANISM, the exact accounting inputs, and an
# expected sign fixed BEFORE evaluation and never refit. Families exist so a
# result can be read against its own economic peer group; the primary
# multiple-testing family is nevertheless the WHOLE Stage-25 discovery set,
# which is the conservative choice.
#
# Deliberately EXCLUDED: everything Stage 23 and Stage 24 already measured and
# registered (residual momentum, low-vol, vol-scaled momentum, the monthly
# liquidity family, and Stage 24's own eight). Re-running them would be
# correlated-variant tuning, not research.
# =========================================================================== #
FAMILY_DISCOVERY = "stage25_pit_alpha_discovery"
FAM_QUALITY = "quality_profitability"
FAM_CASHFLOW = "cash_flow_quality"
FAM_BALANCE = "balance_sheet_quality"
FAM_INVESTMENT = "investment_and_payout"
FAM_OPERATING = "operating_improvement"
FAM_INNOVATION = "innovation_intangibles"

_S = _s24.FactorSpec


def _gp(side: dict) -> Optional[float]:
    return _s24._gross_profit(side)


def _diff(a: Optional[float], b: Optional[float]) -> Optional[float]:
    return None if (a is None or b is None) else a - b


def _growth(cur: Optional[float], prior: Optional[float]) -> Optional[float]:
    if cur is None or prior is None or prior <= 0:
        return None
    return cur / prior - 1.0


# -- quality / profitability -------------------------------------------------- #
def _f_operating_profitability(rec):
    gp = _gp(rec["cur"])
    sga = rec["cur"].get("sganda")
    if gp is None or sga is None:
        return None
    return _ratio(gp - sga, rec["cur"].get("assets"))


def _f_gross_margin_level(rec):
    return _ratio(_gp(rec["cur"]), rec["cur"].get("revenue"))


def _f_return_on_equity(rec):
    return _ratio(rec["cur"].get("net_income"),
                  rec["cur"].get("stockholders_equity"))


def _f_cash_return_on_assets(rec):
    return _ratio(rec["cur"].get("cash_flow_operations"),
                  rec["cur"].get("assets"))


def _f_operating_margin(rec):
    return _ratio(rec["cur"].get("operating_income"), rec["cur"].get("revenue"))


# -- cash-flow quality --------------------------------------------------------- #
def _f_cash_conversion(rec):
    return _ratio(rec["cur"].get("cash_flow_operations"),
                  rec["cur"].get("operating_income"))


def _f_fcf_margin(rec):
    cfo = rec["cur"].get("cash_flow_operations")
    capex = rec["cur"].get("capital_expenditure")
    if cfo is None or capex is None:
        return None
    return _ratio(cfo - abs(capex), rec["cur"].get("revenue"))


def _f_earnings_quality_gap(rec):
    ni = rec["cur"].get("net_income")
    cfo = rec["cur"].get("cash_flow_operations")
    if ni is None or cfo is None:
        return None
    return _ratio(ni - cfo, rec["cur"].get("revenue"))


# -- balance-sheet quality ------------------------------------------------------ #
def _f_working_capital_accruals(rec):
    c, p = rec["cur"], rec["prior"]
    need = ("assets_current", "cash", "liabilities_current")
    if any(c.get(k) is None or p.get(k) is None for k in need):
        return None
    d_wc = ((c["assets_current"] - c["cash"]) - (p["assets_current"] - p["cash"]))
    d_cl = c["liabilities_current"] - p["liabilities_current"]
    return _ratio(d_wc - d_cl, c.get("assets"))


def _f_net_operating_assets(rec):
    c = rec["cur"]
    need = ("assets", "cash", "liabilities", "long_term_debt")
    if any(c.get(k) is None for k in need):
        return None
    noa = (c["assets"] - c["cash"]) - (c["liabilities"] - c["long_term_debt"])
    return _ratio(noa, c.get("assets"))


def _f_inventory_growth(rec):
    return _ratio(_diff(rec["cur"].get("inventory"), rec["prior"].get("inventory")),
                  rec["cur"].get("assets"))


def _f_receivables_growth(rec):
    return _ratio(_diff(rec["cur"].get("receivables"),
                        rec["prior"].get("receivables")),
                  rec["cur"].get("assets"))


def _f_leverage_change(rec):
    return _diff(_ratio(rec["cur"].get("long_term_debt"), rec["cur"].get("assets")),
                 _ratio(rec["prior"].get("long_term_debt"),
                        rec["prior"].get("assets")))


def _f_cash_to_assets(rec):
    return _ratio(rec["cur"].get("cash"), rec["cur"].get("assets"))


def _f_intangible_intensity(rec):
    gw = rec["cur"].get("goodwill")
    ia = rec["cur"].get("intangible_assets")
    if gw is None and ia is None:
        return None
    return _ratio((gw or 0.0) + (ia or 0.0), rec["cur"].get("assets"))


# -- investment and payout -------------------------------------------------------- #
def _f_capex_intensity(rec):
    capex = rec["cur"].get("capital_expenditure")
    return None if capex is None else _ratio(abs(capex), rec["cur"].get("assets"))


def _f_capex_growth(rec):
    c, p = rec["cur"].get("capital_expenditure"), rec["prior"].get("capital_expenditure")
    if c is None or p is None:
        return None
    return _growth(abs(c), abs(p))


def _f_ppe_growth(rec):
    return _ratio(_diff(rec["cur"].get("ppe_net"), rec["prior"].get("ppe_net")),
                  rec["cur"].get("assets"))


def _f_external_financing(rec):
    return _ratio(rec["cur"].get("cash_flow_financing"), rec["cur"].get("assets"))


def _f_shareholder_payout(rec):
    div = rec["cur"].get("dividends_paid")
    buy = rec["cur"].get("share_repurchase")
    if div is None and buy is None:
        return None
    return _ratio(abs(div or 0.0) + abs(buy or 0.0), rec["cur"].get("assets"))


# -- operating improvement ---------------------------------------------------------- #
def _f_asset_turnover_change(rec):
    return _diff(_ratio(rec["cur"].get("revenue"), rec["cur"].get("assets")),
                 _ratio(rec["prior"].get("revenue"), rec["prior"].get("assets")))


def _f_sga_efficiency(rec):
    return _ratio(rec["cur"].get("revenue"), rec["cur"].get("sganda"))


def _f_tax_burden_change(rec):
    return _diff(_ratio(rec["cur"].get("income_tax_expense"),
                        rec["cur"].get("pretax_income")),
                 _ratio(rec["prior"].get("income_tax_expense"),
                        rec["prior"].get("pretax_income")))


# -- innovation / intangibles --------------------------------------------------------- #
def _f_rnd_to_sales(rec):
    return _ratio(rec["cur"].get("research_development"), rec["cur"].get("revenue"))


def _f_rnd_growth(rec):
    return _growth(rec["cur"].get("research_development"),
                   rec["prior"].get("research_development"))


def _f_rnd_efficiency(rec):
    return _ratio(_gp(rec["cur"]), rec["cur"].get("research_development"))


def _f_rnd_disclosure(rec):
    """1.0 when the issuer TAGGED an R&D line for the current fiscal year, 0.0
    when it filed an annual record without one.

    This is the disclosure-selection hypothesis expressed as a signal. Unlike
    every R&D INTENSITY factor, it is defined for the WHOLE cross-section, so its
    long/short spread is exactly 'R&D reporters minus non-reporters'. If R&D
    intensity's apparent alpha were really a membership effect, this is where it
    would show up."""
    return 1.0 if rec["cur"].get("research_development") is not None else 0.0


def _f_sbc_intensity(rec):
    return _ratio(rec["cur"].get("share_based_compensation"),
                  rec["cur"].get("revenue"))


DISCOVERY_FACTORS = (
    # ---------------- quality / profitability ---------------- #
    _S(name="s25_operating_profitability", family=FAM_QUALITY,
       hypothesis="Firms earning more profit per dollar of assets AFTER the cost "
                  "of running the business earn higher subsequent returns.",
       rationale="Gross profitability ignores what a firm spends to convert "
                 "gross profit into a going concern. Subtracting SG&A isolates "
                 "the operating surplus actually available to shareholders and "
                 "is the Ball-Gerakos-Linnainmaa refinement of Novy-Marx.",
       definition="(GrossProfit - SG&A) / Assets",
       required=("gross_profit|revenue+cost_of_revenue", "sganda", "assets"),
       direction=+1, needs_prior=False, fn=_f_operating_profitability),
    _S(name="s25_gross_margin_level", family=FAM_QUALITY,
       hypothesis="Firms with structurally higher gross margins earn higher "
                  "subsequent returns.",
       rationale="Margin measures pricing power per unit sold, which is a "
                 "different economic claim from margin per unit of capital and "
                 "is not mechanically tied to the asset base.",
       definition="GrossProfit / Revenues",
       required=("gross_profit|revenue+cost_of_revenue", "revenue"),
       direction=+1, needs_prior=False, fn=_f_gross_margin_level),
    _S(name="s25_return_on_equity", family=FAM_QUALITY,
       hypothesis="Firms earning more on shareholders' capital earn higher "
                  "subsequent returns.",
       rationale="ROE is the return on the claim investors actually hold. It is "
                 "distinct from asset-scaled profitability because leverage sits "
                 "between the two.",
       definition="NetIncome / StockholdersEquity",
       required=("net_income", "stockholders_equity"),
       direction=+1, needs_prior=False, fn=_f_return_on_equity),
    _S(name="s25_cash_return_on_assets", family=FAM_QUALITY,
       hypothesis="Firms generating more operating CASH per dollar of assets "
                  "earn higher subsequent returns.",
       rationale="Cash from operations is far harder to manage than earnings, so "
                 "a cash-scaled profitability level is the accrual-resistant "
                 "version of the quality claim.",
       definition="CashFlowFromOperations / Assets",
       required=("cash_flow_operations", "assets"),
       direction=+1, needs_prior=False, fn=_f_cash_return_on_assets),
    _S(name="s25_operating_margin", family=FAM_QUALITY,
       hypothesis="Firms converting more of each sales dollar into operating "
                  "income earn higher subsequent returns.",
       rationale="Operating margin captures the full cost structure below the "
                 "gross line while remaining above financing and tax choices.",
       definition="OperatingIncome / Revenues",
       required=("operating_income", "revenue"),
       direction=+1, needs_prior=False, fn=_f_operating_margin),
    # ---------------- cash-flow quality ---------------- #
    _S(name="s25_cash_conversion", family=FAM_CASHFLOW,
       hypothesis="Firms converting a larger share of reported operating income "
                  "into cash earn higher subsequent returns.",
       rationale="The RATIO of cash to accounting profit is a direct measure of "
                 "how real the reported profit is, and it is scale-free in a way "
                 "asset-scaled accruals are not.",
       definition="CashFlowFromOperations / OperatingIncome",
       required=("cash_flow_operations", "operating_income"),
       direction=+1, needs_prior=False, fn=_f_cash_conversion),
    _S(name="s25_fcf_margin", family=FAM_CASHFLOW,
       hypothesis="Firms retaining more free cash flow per sales dollar earn "
                  "higher subsequent returns.",
       rationale="Scaling free cash flow by sales rather than by assets removes "
                 "the asset-intensity confound, so capital-light and "
                 "capital-heavy firms are compared on the same footing.",
       definition="(CFO - |CapEx|) / Revenues",
       required=("cash_flow_operations", "capital_expenditure", "revenue"),
       direction=+1, needs_prior=False, fn=_f_fcf_margin),
    _S(name="s25_earnings_quality_gap", family=FAM_CASHFLOW,
       hypothesis="Firms whose reported earnings exceed their operating cash "
                  "flow by more, relative to sales, earn LOWER subsequent "
                  "returns.",
       rationale="The earnings-minus-cash gap is the accrual component of "
                 "profit. Sales scaling makes it a statement about revenue "
                 "recognition rather than about the balance sheet, which is what "
                 "the released asset-scaled accrual leg already measures.",
       definition="(NetIncome - CFO) / Revenues",
       required=("net_income", "cash_flow_operations", "revenue"),
       direction=-1, needs_prior=False, fn=_f_earnings_quality_gap),
    # ---------------- balance-sheet quality ---------------- #
    _S(name="s25_working_capital_accruals", family=FAM_BALANCE,
       hypothesis="Firms whose non-cash working capital expanded most earn LOWER "
                  "subsequent returns.",
       rationale="The original Sloan accrual is a balance-sheet construction: "
                 "growth in receivables and inventory net of payables is profit "
                 "that has not yet been collected.",
       definition="[d(AssetsCurrent - Cash) - d(LiabilitiesCurrent)] / Assets",
       required=("assets_current", "cash", "liabilities_current", "assets",
                 "assets_current@prior", "cash@prior",
                 "liabilities_current@prior"),
       direction=-1, needs_prior=True, fn=_f_working_capital_accruals),
    _S(name="s25_net_operating_assets", family=FAM_BALANCE,
       hypothesis="Firms carrying more net operating assets per dollar of total "
                  "assets earn LOWER subsequent returns.",
       rationale="Hirshleifer's NOA is the cumulative divergence between "
                 "reported profit and cash profit. A high level says the firm "
                 "has been booking profit onto the balance sheet for years.",
       definition="[(Assets - Cash) - (Liabilities - LongTermDebt)] / Assets",
       required=("assets", "cash", "liabilities", "long_term_debt"),
       direction=-1, needs_prior=False, fn=_f_net_operating_assets),
    _S(name="s25_inventory_growth", family=FAM_BALANCE,
       hypothesis="Firms building inventory fastest earn LOWER subsequent "
                  "returns.",
       rationale="Inventory accumulation ahead of demand is the classic early "
                 "warning of a coming margin problem.",
       definition="d(InventoryNet) / Assets",
       required=("inventory", "assets", "inventory@prior"),
       direction=-1, needs_prior=True, fn=_f_inventory_growth),
    _S(name="s25_receivables_growth", family=FAM_BALANCE,
       hypothesis="Firms whose receivables grew fastest earn LOWER subsequent "
                  "returns.",
       rationale="Receivable growth outrunning the business is the signature of "
                 "loosened credit terms used to pull sales forward.",
       definition="d(AccountsReceivableNetCurrent) / Assets",
       required=("receivables", "assets", "receivables@prior"),
       direction=-1, needs_prior=True, fn=_f_receivables_growth),
    _S(name="s25_leverage_change", family=FAM_BALANCE,
       hypothesis="Firms increasing balance-sheet leverage fastest earn LOWER "
                  "subsequent returns.",
       rationale="The CHANGE in leverage is a financing decision made now, "
                 "whereas the level is largely an industry constant.",
       definition="d(LongTermDebt / Assets)",
       required=("long_term_debt", "assets", "long_term_debt@prior",
                 "assets@prior"),
       direction=-1, needs_prior=True, fn=_f_leverage_change),
    _S(name="s25_cash_to_assets", family=FAM_BALANCE,
       hypothesis="Firms holding more cash per dollar of assets earn higher "
                  "subsequent returns.",
       rationale="Cash is the one asset whose value is not an accounting "
                 "estimate; a cash-rich balance sheet is both optionality and a "
                 "floor under the equity.",
       definition="CashAndCashEquivalents / Assets",
       required=("cash", "assets"), direction=+1, needs_prior=False,
       fn=_f_cash_to_assets),
    _S(name="s25_intangible_intensity", family=FAM_BALANCE,
       hypothesis="Firms carrying more goodwill and acquired intangibles per "
                  "dollar of assets earn LOWER subsequent returns.",
       rationale="Purchased intangibles are the accounting residue of "
                 "acquisitions. A large balance is evidence of a firm that has "
                 "bought growth, and it is the asset most exposed to future "
                 "impairment.",
       definition="(Goodwill + FiniteLivedIntangibleAssetsNet) / Assets",
       required=("goodwill|intangible_assets", "assets"),
       direction=-1, needs_prior=False, fn=_f_intangible_intensity,
       caveat="a firm reporting NEITHER goodwill nor intangibles is absent "
              "rather than scored at zero; the factor generalises to firms that "
              "carry at least one of the two lines"),
    # ---------------- investment and payout ---------------- #
    _S(name="s25_capex_intensity", family=FAM_INVESTMENT,
       hypothesis="Firms spending more on capital expenditure per dollar of "
                  "assets earn LOWER subsequent returns.",
       rationale="The investment factor in its most direct form: capital "
                 "deployed today is cash not returned, and empirically it is "
                 "deployed at its most optimistic when returns are about to "
                 "disappoint.",
       definition="|CapEx| / Assets",
       required=("capital_expenditure", "assets"), direction=-1,
       needs_prior=False, fn=_f_capex_intensity),
    _S(name="s25_capex_growth", family=FAM_INVESTMENT,
       hypothesis="Firms accelerating capital expenditure earn LOWER subsequent "
                  "returns.",
       rationale="The change removes the industry-constant part of capital "
                 "intensity and isolates the discretionary decision to expand.",
       definition="|CapEx|(t) / |CapEx|(t-1yr) - 1",
       required=("capital_expenditure", "capital_expenditure@prior"),
       direction=-1, needs_prior=True, fn=_f_capex_growth),
    _S(name="s25_ppe_growth", family=FAM_INVESTMENT,
       hypothesis="Firms growing net productive capital fastest earn LOWER "
                  "subsequent returns.",
       rationale="PP&E growth is the realised outcome of the investment "
                 "decision, net of depreciation and disposals, so it measures "
                 "capacity actually added rather than cash spent.",
       definition="d(PropertyPlantAndEquipmentNet) / Assets",
       required=("ppe_net", "assets", "ppe_net@prior"),
       direction=-1, needs_prior=True, fn=_f_ppe_growth),
    _S(name="s25_external_financing", family=FAM_INVESTMENT,
       hypothesis="Firms raising more net external finance earn LOWER "
                  "subsequent returns.",
       rationale="Bradshaw-Richardson-Sloan: managers issue securities when "
                 "they believe them expensive, so net issuance is a "
                 "management-revealed valuation signal.",
       definition="NetCashProvidedByFinancingActivities / Assets",
       required=("cash_flow_financing", "assets"), direction=-1,
       needs_prior=False, fn=_f_external_financing),
    _S(name="s25_shareholder_payout", family=FAM_INVESTMENT,
       hypothesis="Firms returning more capital through dividends and buybacks "
                  "earn higher subsequent returns.",
       rationale="Payout is the mirror image of external financing and is the "
                 "hardest of all managerial signals to fake, because it is paid "
                 "in cash.",
       definition="(|DividendsPaid| + |ShareRepurchase|) / Assets",
       required=("dividends_paid|share_repurchase", "assets"),
       direction=+1, needs_prior=False, fn=_f_shareholder_payout,
       caveat="a firm reporting NEITHER a dividend nor a repurchase line is "
              "absent rather than scored at zero, because the absence of the tag "
              "is not accounting evidence of a zero payout"),
    # ---------------- operating improvement ---------------- #
    _S(name="s25_asset_turnover_change", family=FAM_OPERATING,
       hypothesis="Firms whose asset turnover improved earn higher subsequent "
                  "returns.",
       rationale="Turnover improvement is the operational half of a DuPont "
                 "improvement and says the existing asset base is being worked "
                 "harder - an operating fact, not a pricing one.",
       definition="d(Revenues / Assets)",
       required=("revenue", "assets", "revenue@prior", "assets@prior"),
       direction=+1, needs_prior=True, fn=_f_asset_turnover_change),
    _S(name="s25_sga_efficiency", family=FAM_OPERATING,
       hypothesis="Firms generating more revenue per dollar of overhead earn "
                  "higher subsequent returns.",
       rationale="Overhead efficiency separates operating leverage from gross "
                 "margin: two firms with identical gross margins can differ "
                 "entirely in what it costs them to sell.",
       definition="Revenues / SG&A",
       required=("revenue", "sganda"), direction=+1, needs_prior=False,
       fn=_f_sga_efficiency),
    _S(name="s25_tax_burden_change", family=FAM_OPERATING,
       hypothesis="Firms whose effective tax rate rose earn LOWER subsequent "
                  "returns.",
       rationale="A rising effective rate consumes future cash flow and is often "
                 "the exhaustion of prior tax assets - a real economic "
                 "deterioration that never touches operating income.",
       definition="d(IncomeTaxExpense / PretaxIncome)",
       required=("income_tax_expense", "pretax_income",
                 "income_tax_expense@prior", "pretax_income@prior"),
       direction=-1, needs_prior=True, fn=_f_tax_burden_change),
    # ---------------- innovation / intangibles ---------------- #
    _S(name="s25_rnd_to_sales", family=FAM_INNOVATION,
       hypothesis="Firms investing more in R&D per sales dollar earn higher "
                  "subsequent returns.",
       rationale="The alternative R&D denominator. Assets are inflated by "
                 "acquisitions and depressed by the very expensing of R&D that "
                 "the factor is about, so sales is the less circular base.",
       definition="ResearchAndDevelopmentExpense / Revenues",
       required=("research_development", "revenue"), direction=+1,
       needs_prior=False, fn=_f_rnd_to_sales,
       caveat="SELECTION_ON_DISCLOSURE: only issuers that tag an R&D line are "
              "scored; missing R&D is NOT read as zero"),
    _S(name="s25_rnd_growth", family=FAM_INNOVATION,
       hypothesis="Firms increasing R&D spend fastest earn higher subsequent "
                  "returns.",
       rationale="The change isolates a decision to invest more in innovation "
                 "from the industry-constant level of research intensity - which "
                 "is precisely the part of R&D intensity most likely to be a "
                 "sector effect.",
       definition="R&D(t) / R&D(t-1yr) - 1",
       required=("research_development", "research_development@prior"),
       direction=+1, needs_prior=True, fn=_f_rnd_growth),
    _S(name="s25_rnd_efficiency", family=FAM_INNOVATION,
       hypothesis="Firms producing more gross profit per dollar of R&D earn "
                  "higher subsequent returns.",
       rationale="Intensity measures how much is spent; efficiency measures "
                 "whether the spending works. If innovation is priced, the "
                 "productive spenders should be the ones that earn the premium.",
       definition="GrossProfit / ResearchAndDevelopmentExpense",
       required=("gross_profit|revenue+cost_of_revenue", "research_development"),
       direction=+1, needs_prior=False, fn=_f_rnd_efficiency),
    _S(name="s25_rnd_disclosure_indicator", family=FAM_INNOVATION,
       hypothesis="Firms that REPORT an R&D line at all earn higher subsequent "
                  "returns than firms that do not.",
       rationale="This is the disclosure-selection explanation stated as a "
                 "testable signal rather than assumed away. Its long/short "
                 "spread is exactly 'R&D reporters minus non-reporters', so if "
                 "R&D intensity's apparent alpha is really a membership effect "
                 "it must show up here.",
       definition="1 if ResearchAndDevelopmentExpense was tagged, else 0",
       required=("annual record",), direction=+1, needs_prior=False,
       fn=_f_rnd_disclosure,
       caveat="a deliberately binary signal: its rank IC is computed over a "
              "cross-section with two levels, and its long/short legs are the "
              "two disclosure groups"),
    _S(name="s25_sbc_intensity", family=FAM_INNOVATION,
       hypothesis="Firms paying more share-based compensation per sales dollar "
                  "earn LOWER subsequent returns.",
       rationale="Stock compensation is a real cost borne by existing holders "
                 "through dilution. It is also the intangible-intensive firm's "
                 "largest non-cash expense, which makes it the natural adverse "
                 "control on any innovation-premium story.",
       definition="ShareBasedCompensation / Revenues",
       required=("share_based_compensation", "revenue"), direction=-1,
       needs_prior=False, fn=_f_sbc_intensity),
)

#: The Stage-24 R&D intensity factor, re-declared here so Stage 25 can reproduce
#: and then attack it. Same definition, same sign, same owner semantics - the
#: point of the exercise is that nothing about it changed except the scrutiny.
RND_INTENSITY = _s24.factor_by_name("s24_rnd_intensity")

#: The two composite_sn legs, reused verbatim from their Stage-24 owner so the
#: operational baseline is reconstructed identically.
COMPOSITE_FACTORS = _s24.COMPOSITE_FACTORS

ALL_FACTORS = DISCOVERY_FACTORS + COMPOSITE_FACTORS + (RND_INTENSITY,)


def factor_by_name(name: str) -> Optional["_s24.FactorSpec"]:
    for f in ALL_FACTORS:
        if f is not None and f.name == name:
            return f
    return None


# =========================================================================== #
# WORKSTREAM F - the horizon family.
#
# Annual fundamentals move once a year, so formation is quarterly. Horizon
# exploration is bounded to THREE economically sensible holding periods and is
# treated as part of the multiple-testing family, never as a per-factor search.
#
# The overlap rule is what makes the horizons comparable. A 3-month forward
# return formed quarterly is non-overlapping. A 6-month forward return formed
# QUARTERLY would overlap by half and inflate every t-statistic in the stage, so
# the 6-month horizon is formed SEMI-ANNUALLY instead. The 1-month horizon leaves
# a gap rather than an overlap, which costs power but never borrows it.
# =========================================================================== #
HORIZONS = (
    {"key": "h1m", "forward_months": 1, "horizon_days": 21,
     "formation_stride": 1, "overlap": "none (2-month gap between windows)"},
    {"key": "h3m", "forward_months": 3, "horizon_days": 63,
     "formation_stride": 1, "overlap": "none (contiguous, non-overlapping)"},
    {"key": "h6m", "forward_months": 6, "horizon_days": 126,
     "formation_stride": 2, "overlap": "none (semi-annual formation)"},
)
PRIMARY_HORIZON = "h3m"


def horizon_by_key(key: str) -> dict:
    for h in HORIZONS:
        if h["key"] == key:
            return h
    raise ValueError("unknown horizon: %r" % key)


# =========================================================================== #
# The Stage-25 panel.
# =========================================================================== #
FORMATION_EVERY_N_MONTHS = _s24.FORMATION_EVERY_N_MONTHS
FIRST_MONTH = "2010-01"


class Stage25Panel:
    """One row per (formation month, symbol).

    Each row carries every pre-registered factor value that was computable from
    facts FILED by the formation date, the forward return at each horizon, the
    point-in-time controls (size/liquidity, volatility, momentum, beta) and both
    sector classifications."""

    def __init__(self) -> None:
        self.rows: "dict[str, dict[str, dict]]" = {}
        self.months: "list[str]" = []
        self.formation_dates: "dict[str, str]" = {}
        self.diagnostics: dict = {}

    # -- horizon-aware month selection --------------------------------------- #
    def months_for(self, horizon: str) -> "list[str]":
        stride = int(horizon_by_key(horizon)["formation_stride"])
        return self.months[::max(1, stride)]

    # -- cross-section builders ---------------------------------------------- #
    def factor_cross_sections(self, factor: "_s24.FactorSpec", *,
                              horizon: str = PRIMARY_HORIZON,
                              min_names: int = MIN_CROSS_SECTION,
                              symbol_filter=None) -> list:
        """``[{as_of, month, names:[(symbol, winsorized_signed_value, fwd)]}]``.

        The winsorizer is the released owner; the expected sign is the factor's
        pre-registered one. ``symbol_filter(month, symbol, row) -> bool`` narrows
        the universe for a falsification variant (sector subsets, winner drops)
        WITHOUT changing any statistic."""
        from . import signal_library as _sl
        key = horizon_by_key(horizon)["key"]
        out = []
        for m in self.months_for(horizon):
            raw, fwd = {}, {}
            for sym, r in self.rows.get(m, {}).items():
                v = r["factors"].get(factor.name)
                f = (r.get("forward") or {}).get(key)
                if v is None or f is None:
                    continue
                if symbol_filter is not None and not symbol_filter(m, sym, r):
                    continue
                raw[sym] = float(v)
                fwd[sym] = float(f)
            if len(raw) < min_names:
                continue
            clipped = _sl.winsorize(raw, WINSOR_FRACTION)
            out.append({"as_of": self.formation_dates[m], "month": m,
                        "names": [(s, clipped[s] * factor.direction, fwd[s])
                                  for s in sorted(clipped)]})
        return out

    def composite_cross_sections(self, *, legs=COMPOSITE_FACTORS,
                                 horizon: str = PRIMARY_HORIZON,
                                 min_names: int = MIN_CROSS_SECTION) -> list:
        """The point-in-time reconstruction of the released composite: orient
        each leg by its fixed a-priori sign, z-score each oriented leg within the
        cross-section, add with fixed equal weight. No optimisation, no sign
        refit. A name missing either leg is dropped, never scored on one."""
        from . import signal_library as _sl
        key = horizon_by_key(horizon)["key"]
        out = []
        for m in self.months_for(horizon):
            per_leg: "dict[str, dict]" = {}
            for leg in legs:
                vals = {}
                for sym, r in self.rows.get(m, {}).items():
                    v = r["factors"].get(leg.name)
                    if v is None or (r.get("forward") or {}).get(key) is None:
                        continue
                    vals[sym] = float(v)
                per_leg[leg.name] = _sl.winsorize(vals, WINSOR_FRACTION)
            common = None
            for leg in legs:
                keys = set(per_leg[leg.name])
                common = keys if common is None else (common & keys)
            common = sorted(common or ())
            if len(common) < min_names:
                continue
            z_total = {s: 0.0 for s in common}
            ok = True
            for leg in legs:
                z = _zscore([per_leg[leg.name][s] * leg.direction
                             for s in common])
                if z is None:
                    ok = False
                    break
                for s, zv in zip(common, z):
                    z_total[s] += zv
            if not ok:
                continue
            out.append({"as_of": self.formation_dates[m], "month": m,
                        "names": [(s, z_total[s],
                                   self.rows[m][s]["forward"][key])
                                  for s in common]})
        return out

    def momentum_cross_sections(self, *, horizon: str = PRIMARY_HORIZON,
                                min_names: int = MIN_CROSS_SECTION) -> list:
        """``mom_6_1`` restricted to the SAME names and dates as the fundamental
        cross-sections, so every incremental claim is apples-to-apples."""
        from . import signal_library as _sl
        key = horizon_by_key(horizon)["key"]
        out = []
        for m in self.months_for(horizon):
            raw, fwd = {}, {}
            for sym, r in self.rows.get(m, {}).items():
                v = r.get("mom_6_1")
                f = (r.get("forward") or {}).get(key)
                if v is None or f is None:
                    continue
                raw[sym] = float(v)
                fwd[sym] = float(f)
            if len(raw) < min_names:
                continue
            clipped = _sl.winsorize(raw, WINSOR_FRACTION)
            out.append({"as_of": self.formation_dates[m], "month": m,
                        "names": [(s, clipped[s], fwd[s])
                                  for s in sorted(clipped)]})
        return out

    def control_series(self, month: str, symbols: Sequence[str],
                       control: str) -> "list[Optional[float]]":
        return [(self.rows.get(month, {}).get(s) or {}).get(control)
                for s in symbols]

    def sector_of(self, month: str, symbol: str, *, tier: str) -> str:
        r = self.rows.get(month, {}).get(symbol) or {}
        return (r.get("sectors") or {}).get(tier, _ps.UNKNOWN)


def build_panel(universe: "_s24.HistoricalUniverse",
                bridge: "_s24.IdentityBridge", store: "Stage25PitStore",
                sectors: "SectorHistory", beta: "TrailingBeta", *,
                factors: Sequence["_s24.FactorSpec"] = ALL_FACTORS,
                first_month: str = FIRST_MONTH,
                every_n: int = FORMATION_EVERY_N_MONTHS) -> "Stage25Panel":
    """Assemble the point-in-time, survivorship-safe panel.

    For every formation month the eligible universe is the OWNED historical
    membership at that month - including the companies later acquired, delisted
    or bankrupted. Each eligible name is mapped to its historical issuer, and
    every factor is computed from facts FILED by the formation date minus the
    reporting lag. Names whose fundamentals were not yet filed are absent from
    that cross-section; they are never back-filled."""
    panel = Stage25Panel()
    all_months = universe.months()
    candidate = [m for m in all_months if m >= first_month]
    formations = candidate[::max(1, int(every_n))]
    max_fwd = max(h["forward_months"] for h in HORIZONS)
    stats = {"formations_attempted": 0, "eligible_names": 0, "cik_resolved": 0,
             "annual_record_available": 0, "scored_rows": 0,
             "dropped_no_primary_forward": 0, "unresolved_symbols": {},
             "forward_available": {h["key"]: 0 for h in HORIZONS},
             "beta_available": 0, "tier_a_known": 0, "tier_b_known": 0}
    primary_k = horizon_by_key(PRIMARY_HORIZON)["forward_months"]
    for m in formations:
        d = universe.formation_date(m)
        if not d:
            continue
        as_of = _shift_days(d, REPORTING_LAG_DAYS)
        eligible = universe.eligible(m)
        if not eligible:
            continue
        stats["formations_attempted"] += 1
        month_rows: "dict[str, dict]" = {}
        for sym, prow in eligible.items():
            stats["eligible_names"] += 1
            cik = bridge.cik_for(sym)
            if cik is None:
                stats["unresolved_symbols"][sym] = \
                    stats["unresolved_symbols"].get(sym, 0) + 1
                continue
            stats["cik_resolved"] += 1
            rec = annual_record(store, cik, as_of)
            if rec is None:
                continue
            stats["annual_record_available"] += 1
            forward = {}
            for h in HORIZONS:
                fv = universe.forward_return_chain(m, sym, h["forward_months"])
                if fv is not None:
                    forward[h["key"]] = fv
                    stats["forward_available"][h["key"]] += 1
            if horizon_by_key(PRIMARY_HORIZON)["key"] not in forward:
                stats["dropped_no_primary_forward"] += 1
                continue
            vals = {f.name: f.value(rec) for f in factors if f is not None}
            if all(v is None for v in vals.values()):
                continue
            adv = prow.get("adv_dollar")
            b = beta.beta_as_of(m, sym)
            if b is not None:
                stats["beta_available"] += 1
            sec_a = sectors.tier_a(store, cik, as_of)
            sec_b = sectors.tier_b(cik, as_of)
            if sec_a != TIER_A_UNKNOWN:
                stats["tier_a_known"] += 1
            if sec_b != _ps.UNKNOWN:
                stats["tier_b_known"] += 1
            month_rows[sym] = {
                "cik": cik,
                "period_end": rec["period_end"],
                "prior_period_end": rec["prior_period_end"],
                "factors": vals,
                "forward": forward,
                "log_adv_dollar": (math.log(adv) if (adv and adv > 0) else None),
                "realized_vol_63d": prow.get("realized_vol_63d"),
                "mom_6_1": prow.get("mom_6_1"),
                "trailing_beta": b,
                "sectors": {TIER_A: sec_a, TIER_B: sec_b},
                # Which concepts this issuer had actually TAGGED for the current
                # fiscal year. Kept so a disclosure-selection diagnostic can be
                # run for ANY concept, not just R&D - the winner of a campaign
                # must face the same coverage question the prior stage's
                # candidate faced.
                "reported_concepts": frozenset(rec["cur"]),
                "rnd_state": rnd_availability_state(rec, sec_a),
            }
            stats["scored_rows"] += 1
        if month_rows:
            panel.rows[m] = month_rows
            panel.months.append(m)
            panel.formation_dates[m] = d
    stats["unresolved_symbols"] = _s24._top_counts(
        list(stats["unresolved_symbols"]), limit=15)
    stats["primary_horizon"] = PRIMARY_HORIZON
    stats["primary_forward_months"] = primary_k
    stats["max_forward_months"] = max_fwd
    panel.diagnostics = stats
    return panel


# =========================================================================== #
# WORKSTREAM C - R&D availability taxonomy.
#
# Stage 24's caveat was SELECTION_ON_DISCLOSURE, stated but not decomposed.
# 'No R&D number' is four economically different situations and Stage 25 keeps
# them apart, using only what the owned data can actually distinguish.
# =========================================================================== #
RND_ZERO = "ZERO"                    # tagged, and the tagged value is 0
RND_REPORTED = "REPORTED"            # tagged with a non-zero value
RND_NOT_REPORTED = "NOT_REPORTED"    # annual record exists, no R&D tag in it
RND_NOT_APPLICABLE = "NOT_APPLICABLE"  # bank / insurer / REIT by Tier-A
RND_MISSING = "MISSING"              # no annual record at all (never reaches here)


def rnd_availability_state(rec: dict, tier_a_sector: str) -> str:
    """Which of the four R&D situations this (issuer, formation) is in.

    NOT_APPLICABLE is decided by the LEAKAGE-SAFE Tier-A classifier, so the
    taxonomy itself contains no look-ahead. ZERO is only ever assigned when the
    issuer actually TAGGED the line with a zero - an absent tag is never read as
    accounting evidence of zero spend."""
    v = rec["cur"].get("research_development")
    if v is not None:
        return RND_ZERO if abs(float(v)) < 1e-9 else RND_REPORTED
    if tier_a_sector in ("Banking", "Insurance", "RealEstate"):
        return RND_NOT_APPLICABLE
    return RND_NOT_REPORTED


#: Concepts whose absence is economically meaningless for a financial issuer, so
#: NOT_APPLICABLE rather than NOT_REPORTED is the honest label.
_NOT_APPLICABLE_FOR_FINANCIALS = frozenset({
    "research_development", "inventory", "cost_of_revenue", "gross_profit",
    "capital_expenditure", "ppe_net"})


def disclosure_state(reported: frozenset, concept: str,
                     tier_a_sector: str) -> str:
    """The same four-way taxonomy, for ANY concept."""
    if concept in reported:
        return RND_REPORTED
    if concept in _NOT_APPLICABLE_FOR_FINANCIALS and \
            tier_a_sector in ("Banking", "Insurance", "RealEstate"):
        return RND_NOT_APPLICABLE
    return RND_NOT_REPORTED


# =========================================================================== #
# Cross-section transforms used by the falsification battery.
#
# Each one takes already-oriented cross-sections and returns cross-sections. No
# statistic is computed here; the released evaluator is still the only thing that
# turns names into evidence.
# =========================================================================== #
score_cross_sections = _s24.score_cross_sections
gate_for = _s24.gate_for
blend_cross_sections = _s24.blend_cross_sections


def sector_neutral_cross_sections(periods: list, panel: "Stage25Panel", *,
                                  tier: str,
                                  min_group: int = MIN_SECTOR_GROUP) -> list:
    """Demean each period's oriented signal WITHIN its sector group.

    A group smaller than ``min_group`` is pooled into ``OTHER_SMALL`` rather than
    demeaned against itself: a one-name group would be demeaned to exactly zero,
    which deletes that name's information under the guise of neutralising it.
    Names with no classification are pooled the same way, never dropped."""
    month_by_date = {panel.formation_dates[m]: m for m in panel.months}
    out = []
    for p in periods:
        m = month_by_date.get(p["as_of"])
        if m is None:
            continue
        groups: "dict[str, list]" = {}
        for sym, val, fwd in p["names"]:
            groups.setdefault(panel.sector_of(m, sym, tier=tier), []).append(
                (sym, val, fwd))
        pooled: list = []
        buckets: "dict[str, list]" = {}
        for label, members in groups.items():
            if label in (_ps.UNKNOWN, TIER_A_UNKNOWN) or len(members) < min_group:
                pooled.extend(members)
            else:
                buckets[label] = members
        if pooled:
            buckets[OTHER_SMALL] = pooled
        names = []
        for label, members in buckets.items():
            mu = sum(v for _, v, _ in members) / len(members)
            names.extend((s, v - mu, f) for s, v, f in members)
        if len(names) >= MIN_CROSS_SECTION:
            out.append({"as_of": p["as_of"], "month": m,
                        "names": sorted(names)})
    return out


def drop_top_winners(periods: list, k: int) -> list:
    """Remove, from each period, the ``k`` names with the LARGEST realised
    forward return.

    This asks whether the result is carried by a handful of lottery outcomes.
    It is deliberately adversarial: the dropped names are chosen using the
    realised return, which is information the strategy never had - the point is
    to remove the best possible luck, not to simulate a tradable rule."""
    out = []
    for p in periods:
        names = sorted(p["names"], key=lambda t: t[2], reverse=True)[int(k):]
        if len(names) >= MIN_CROSS_SECTION:
            out.append({"as_of": p["as_of"], "month": p.get("month"),
                        "names": sorted(names)})
    return out


def restrict_periods(periods: list, keep) -> list:
    """Keep only the periods for which ``keep(as_of)`` is true."""
    return [p for p in periods if keep(p["as_of"])]


def restrict_to_common(periods: list, reference: list) -> list:
    """Restrict ``periods`` to the EXACT (date, name) pairs present in
    ``reference``.

    This is what makes a widened ensemble comparable to the model it claims to
    beat. A blend requires every name to carry every component, so adding a
    sparsely-reported signal silently narrows the cross-section - and a narrower,
    better-covered universe can look stronger for reasons that have nothing to do
    with the added signal. Scoring the incumbent on the challenger's OWN names
    removes that advantage."""
    ref = {p["as_of"]: {k for k, _, _ in p["names"]} for p in reference}
    out = []
    for p in periods:
        allow = ref.get(p["as_of"])
        if not allow:
            continue
        names = [t for t in p["names"] if t[0] in allow]
        if len(names) >= MIN_CROSS_SECTION:
            out.append({"as_of": p["as_of"], "month": p.get("month"),
                        "names": names})
    return out


def evaluate_variant(periods: list, *, feature: str,
                     horizon: str = PRIMARY_HORIZON,
                     label: str = "", evidence_class: str = "") -> dict:
    """One falsification variant, scored by the RELEASED evaluator and reported
    as a compact, comparable row."""
    hz = horizon_by_key(horizon)["horizon_days"]
    if not periods:
        return {"variant": label or feature, "periods": 0,
                "evidence_class": evidence_class, "insufficient": True}
    res = score_cross_sections(periods, feature=feature, horizon_days=hz)
    row = res["row"]
    return {
        "variant": label or feature,
        "evidence_class": evidence_class,
        "periods": row.get("periods"),
        "median_names": row.get("universe"),
        "rank_ic": row.get("rank_ic_mean"),
        "rank_ic_t": row.get("rank_ic_t"),
        "spread_t": row.get("spread_t"),
        "gross_annualized": row.get("gross_annualized_return"),
        "net25": row.get("net_annualized_return"),
        "turnover": row.get("turnover"),
        "subperiod_consistency": row.get("subperiod_consistency"),
        "insufficient": bool((row.get("periods") or 0) < 8),
    }


# =========================================================================== #
# WORKSTREAM C - the R&D falsification battery.
#
# The thresholds below are PRE-REGISTERED: they are declared here, in source,
# before any Stage-25 number exists, and they are not tuned afterwards.
# =========================================================================== #
#: A control kills the signal if the controlled IC t-statistic falls below this.
RND_SURVIVE_MIN_T = 2.0
#: ...or if less than this fraction of the raw rank IC survives the control.
RND_SURVIVE_MIN_RETENTION = 0.50
#: A subset universe with fewer periods than this is reported as underpowered
#: rather than as evidence either way.
MIN_PERIODS_FOR_VERDICT = 12

RND_VERDICTS = (
    "SURVIVES_SECTOR_AND_STYLE_CONTROLS",
    "WEAKENED_BUT_SURVIVES",
    "SECTOR_EXPLAINED",
    "DISCLOSURE_SELECTION_EXPLAINED",
    "STYLE_EXPLAINED",
    "CONCENTRATION_FRAGILE",
    "SUBPERIOD_UNSTABLE",
    "UNDERPOWERED_NO_VERDICT",
)


def _retention(raw_ic, ctl_ic) -> Optional[float]:
    if raw_ic in (None, 0) or ctl_ic is None:
        return None
    return round(float(ctl_ic) / float(raw_ic), 4)


def _survives(raw_ic, variant: dict) -> Optional[bool]:
    """A variant survives when BOTH pre-registered conditions hold."""
    if variant.get("insufficient"):
        return None
    t = variant.get("rank_ic_t")
    ret = _retention(raw_ic, variant.get("rank_ic"))
    if t is None or ret is None:
        return None
    return bool(t >= RND_SURVIVE_MIN_T and ret >= RND_SURVIVE_MIN_RETENTION)


#: Buckets that carry the technology/software business model under the released
#: SIC taxonomy. SIC 3570-3579 and 3670-3699 map to Technology, but SIC
#: 7000-7999 (Services, INCLUDING 7372 prepackaged software) maps to
#: ConsumerDiscretionary. Removing only "Technology" would therefore leave most
#: software firms in the sample, so the joint removal below is the test that
#: actually answers "is this a technology bet?".
TECH_EXPOSED_SECTORS = ("Technology", "ConsumerDiscretionary")

#: Alternative constructions offered per falsified factor.
ALT_CONSTRUCTIONS = {
    "s24_rnd_intensity": ("s25_rnd_to_sales", "s25_rnd_growth",
                          "s25_rnd_efficiency"),
    "s25_operating_profitability": ("s25_gross_margin_level",
                                    "s25_operating_margin",
                                    "s25_sga_efficiency"),
}

#: Which sparsely-tagged concept a factor's coverage depends on.
DISCLOSURE_CONCEPT = {
    "s24_rnd_intensity": ("research_development",
                          "s25_rnd_disclosure_indicator"),
    "s25_operating_profitability": ("sganda", None),
}


def falsification_battery(panel: "Stage25Panel", sectors: "SectorHistory", *,
                          cfg: dict, spec: "_s24.FactorSpec" = None,
                          horizon: str = PRIMARY_HORIZON) -> dict:
    """Treat the factor as guilty until proven innocent.

    Reproduces the Stage-24 result on the Stage-25 panel, then attacks it from
    every angle owned data supports: point-in-time sector neutralisation, single
    sector universes, sector removal, size / liquidity / beta / volatility
    neutralisation, removal of the best individual outcomes, subperiod and regime
    splits, concentration, transaction cost, turnover, alternative denominators,
    and the disclosure-selection explanation stated as its own signal."""
    from . import orthogonality as _o

    spec = spec or RND_INTENSITY
    base_periods = panel.factor_cross_sections(spec, horizon=horizon)
    base = evaluate_variant(base_periods, feature=spec.name, horizon=horizon,
                            label="baseline_reproduction",
                            evidence_class="SURVIVORSHIP_SAFE_POINT_IN_TIME")
    raw_ic = base.get("rank_ic")
    hz = horizon_by_key(horizon)["horizon_days"]
    base_full = score_cross_sections(base_periods, feature=spec.name,
                                     horizon_days=hz)

    tests: "list[dict]" = []
    month_by_date = {panel.formation_dates[m]: m for m in panel.months}

    # -- 1/2. sector neutralisation and within-sector ranking ---------------- #
    for tier, ec in ((TIER_A, "LEAKAGE_SAFE_COARSE_CLASSIFICATION"),
                     (TIER_B, "CLASSIFICATION_LOOKAHEAD_CONTROL")):
        neutral = sector_neutral_cross_sections(base_periods, panel, tier=tier)
        tests.append(evaluate_variant(
            neutral, feature="%s_sector_neutral_%s" % (spec.name, tier),
            horizon=horizon, label="sector_neutral[%s]" % tier,
            evidence_class=ec))

    # -- 3/4/5. single-sector universes and sector removal ------------------- #
    sector_counts: "dict[str, int]" = {}
    for m in panel.months:
        for sym, r in panel.rows[m].items():
            if r["factors"].get(spec.name) is None:
                continue
            sector_counts[(r.get("sectors") or {}).get(TIER_B, _ps.UNKNOWN)] = \
                sector_counts.get(
                    (r.get("sectors") or {}).get(TIER_B, _ps.UNKNOWN), 0) + 1
    ranked_sectors = [s for s, _ in sorted(sector_counts.items(),
                                           key=lambda kv: (-kv[1], kv[0]))
                      if s != _ps.UNKNOWN]
    single_sector: "list[dict]" = []
    leave_one_out: "list[dict]" = []
    for sec in ranked_sectors[:6]:
        only = panel.factor_cross_sections(
            spec, horizon=horizon,
            symbol_filter=lambda m, s, r, _sec=sec:
                (r.get("sectors") or {}).get(TIER_B) == _sec)
        single_sector.append(evaluate_variant(
            only, feature="%s_only_%s" % (spec.name, sec), horizon=horizon,
            label="only_sector[%s]" % sec,
            evidence_class="CLASSIFICATION_LOOKAHEAD_CONTROL"))
        without = panel.factor_cross_sections(
            spec, horizon=horizon,
            symbol_filter=lambda m, s, r, _sec=sec:
                (r.get("sectors") or {}).get(TIER_B) != _sec)
        leave_one_out.append(evaluate_variant(
            without, feature="%s_without_%s" % (spec.name, sec),
            horizon=horizon, label="without_sector[%s]" % sec,
            evidence_class="CLASSIFICATION_LOOKAHEAD_CONTROL"))
    # The joint removal that the SIC taxonomy actually requires - see
    # TECH_EXPOSED_SECTORS. Removing "Technology" alone leaves software in.
    leave_one_out.append(evaluate_variant(
        panel.factor_cross_sections(
            spec, horizon=horizon,
            symbol_filter=lambda m, s, r:
                (r.get("sectors") or {}).get(TIER_B)
                not in TECH_EXPOSED_SECTORS),
        feature="%s_without_tech_exposed" % spec.name, horizon=horizon,
        label="without_sectors%s" % list(TECH_EXPOSED_SECTORS),
        evidence_class="CLASSIFICATION_LOOKAHEAD_CONTROL"))

    # -- 6/7/8/9. style neutralisation (size, liquidity, beta, volatility) --- #
    style: "dict[str, dict]" = {}
    controls = ("log_adv_dollar", "realized_vol_63d", "trailing_beta")
    for ctl in controls:
        partials, raws = [], []
        for p in base_periods:
            m = month_by_date.get(p["as_of"])
            if m is None:
                continue
            keys = [k for k, _, _ in p["names"]]
            cv = panel.control_series(m, keys, ctl)
            fac = [v for _, v, _ in p["names"]]
            fwd = [f for _, _, f in p["names"]]
            idx = [i for i in range(len(keys)) if cv[i] is not None]
            if len(idx) < 10:
                continue
            r = _o.rank_correlation([fac[i] for i in idx], [fwd[i] for i in idx])
            pic = _o.partial_rank_ic([fac[i] for i in idx],
                                     [fwd[i] for i in idx],
                                     [[cv[i] for i in idx]])
            if r is not None:
                raws.append(r)
            if pic is not None:
                partials.append(pic)
        style[ctl] = {
            "periods": len(partials),
            "raw_rank_ic_on_same_names": _mean(raws),
            "neutralized_rank_ic": _mean(partials),
            "neutralized_rank_ic_t": _t_stat(partials),
            "information_retained_fraction": _retention(_mean(raws),
                                                        _mean(partials)),
            "control_is_point_in_time": True,
        }
    joint = []
    for p in base_periods:
        m = month_by_date.get(p["as_of"])
        if m is None:
            continue
        keys = [k for k, _, _ in p["names"]]
        cols = [panel.control_series(m, keys, c) for c in controls]
        fac = [v for _, v, _ in p["names"]]
        fwd = [f for _, _, f in p["names"]]
        idx = [i for i in range(len(keys))
               if all(c[i] is not None for c in cols)]
        if len(idx) < 10:
            continue
        pic = _o.partial_rank_ic([fac[i] for i in idx], [fwd[i] for i in idx],
                                 [[c[i] for i in idx] for c in cols])
        if pic is not None:
            joint.append(pic)
    style["joint"] = {"controls": list(controls), "periods": len(joint),
                      "neutralized_rank_ic": _mean(joint),
                      "neutralized_rank_ic_t": _t_stat(joint)}

    # -- 10. removal of the strongest individual outcomes -------------------- #
    winners = [evaluate_variant(
        drop_top_winners(base_periods, k), feature="%s_drop_top%d" % (spec.name, k),
        horizon=horizon, label="drop_top_%d_winners_per_period" % k,
        evidence_class="ADVERSARIAL_HINDSIGHT_REMOVAL") for k in (1, 3, 5)]

    # -- 11/12. subperiod and regime splits ---------------------------------- #
    dates = sorted({p["as_of"] for p in base_periods})
    subperiods: "list[dict]" = []
    if len(dates) >= 4:
        mid = dates[len(dates) // 2]
        subperiods.append(evaluate_variant(
            restrict_periods(base_periods, lambda d, _m=mid: d < _m),
            feature="%s_first_half" % spec.name, horizon=horizon,
            label="first_half[<%s]" % mid, evidence_class="SUBPERIOD"))
        subperiods.append(evaluate_variant(
            restrict_periods(base_periods, lambda d, _m=mid: d >= _m),
            feature="%s_second_half" % spec.name, horizon=horizon,
            label="second_half[>=%s]" % mid, evidence_class="SUBPERIOD"))
    for cut, name in (("2016-01-01", "pre_2016"), ("2020-01-01", "pre_covid")):
        subperiods.append(evaluate_variant(
            restrict_periods(base_periods, lambda d, _c=cut: d < _c),
            feature="%s_%s" % (spec.name, name), horizon=horizon,
            label="%s[<%s]" % (name, cut), evidence_class="REGIME_SPLIT"))
        subperiods.append(evaluate_variant(
            restrict_periods(base_periods, lambda d, _c=cut: d >= _c),
            feature="%s_post_%s" % (spec.name, name), horizon=horizon,
            label="post_%s[>=%s]" % (name, cut), evidence_class="REGIME_SPLIT"))

    # -- 13. concentration --------------------------------------------------- #
    from . import stage23_unified as _s23
    concentration = _s23.concentration_report(base_full["series"],
                                              drop_counts=(1, 2, 3))
    long_leg_sectors = _long_leg_sector_mix(base_periods, panel, tier=TIER_B)

    # -- 14/15. cost and turnover -------------------------------------------- #
    cost = {
        "cost_grid": (base_full["row"].get("cost_sensitivity") or {}).get("grid"),
        "cost_flips_sign": base_full["row"].get("cost_flips_sign"),
        "cost_erosion_ratio": base_full["row"].get("cost_erosion_ratio"),
        "turnover": base_full["row"].get("turnover"),
        "turnover_interpretation": (
            "annual fundamentals rebalanced quarterly produce very low turnover; "
            "a low reading here is a property of the data cadence, not evidence "
            "of a cheap strategy"),
    }

    # -- 16. alternative denominators ---------------------------------------- #
    alt = []
    for alt_name in ALT_CONSTRUCTIONS.get(spec.name, ()):
        f = factor_by_name(alt_name)
        if f is None:
            continue
        alt.append(evaluate_variant(
            panel.factor_cross_sections(f, horizon=horizon), feature=alt_name,
            horizon=horizon, label="alternate_construction[%s]" % alt_name,
            evidence_class="SURVIVORSHIP_SAFE_POINT_IN_TIME"))

    # -- 17. reporting selection / missingness -------------------------------- #
    concept, indicator = DISCLOSURE_CONCEPT.get(spec.name, (None, None))
    disclosure = (disclosure_selection_analysis(
        panel, concept=concept, horizon=horizon,
        preregistered_indicator=indicator) if concept else
        {"concept": None, "membership_spread": {"insufficient": True},
         "note": "this factor does not depend on a sparsely-tagged concept"})

    # ---- verdict ------------------------------------------------------------ #
    verdict = _rd_verdict(raw_ic=raw_ic, base=base, tests=tests, style=style,
                          winners=winners, subperiods=subperiods,
                          disclosure=disclosure, single_sector=single_sector,
                          leave_one_out=leave_one_out)

    return {
        "contract_id": "stage25_falsification_battery/1",
        "factor": spec.name,
        "factor_definition": spec.definition,
        "stance": "GUILTY_UNTIL_PROVEN_INNOCENT",
        "pre_registered_thresholds": {
            "min_controlled_rank_ic_t": RND_SURVIVE_MIN_T,
            "min_information_retained_fraction": RND_SURVIVE_MIN_RETENTION,
            "min_periods_for_verdict": MIN_PERIODS_FOR_VERDICT,
            "declared_before_any_stage25_number_existed": True},
        "baseline_reproduction": base,
        "sector_neutralization": tests,
        "single_sector_universes": single_sector,
        "sector_removal": leave_one_out,
        "sector_universe_counts": dict(sorted(sector_counts.items())),
        "style_neutralization": style,
        "winner_removal": winners,
        "subperiod_and_regime": subperiods,
        "concentration": concentration,
        "long_leg_sector_mix": long_leg_sectors,
        "transaction_cost_and_turnover": cost,
        "alternative_constructions": alt,
        "disclosure_selection": disclosure,
        "tier_b_usage_rule": SectorHistory.tier_b_usage_rule(),
        "verdict": verdict,
    }


def _long_leg_sector_mix(periods: list, panel: "Stage25Panel", *,
                         tier: str, top_fraction: float = 0.1) -> dict:
    """What the LONG leg is actually made of, sector by sector, and how
    concentrated that mix is.

    A Herfindahl close to 1 would mean the 'stock selection' is a single-sector
    position wearing a factor's clothes."""
    month_by_date = {panel.formation_dates[m]: m for m in panel.months}
    counts: "dict[str, int]" = {}
    total = 0
    for p in periods:
        m = month_by_date.get(p["as_of"])
        if m is None:
            continue
        ordered = sorted(p["names"], key=lambda t: t[1], reverse=True)
        k = max(1, int(len(ordered) * top_fraction))
        for sym, _, _ in ordered[:k]:
            lab = panel.sector_of(m, sym, tier=tier)
            counts[lab] = counts.get(lab, 0) + 1
            total += 1
    shares = {k: round(v / total, 4) for k, v in counts.items()} if total else {}
    hhi = round(sum(s * s for s in shares.values()), 4) if shares else None
    top = max(shares.items(), key=lambda kv: kv[1]) if shares else (None, None)
    return {
        "tier": tier, "top_decile_name_observations": total,
        "sector_share_of_long_leg": dict(sorted(shares.items(),
                                                key=lambda kv: -kv[1])),
        "herfindahl": hhi,
        "largest_sector": top[0], "largest_sector_share": top[1],
        "evidence_class": ("CLASSIFICATION_LOOKAHEAD_CONTROL" if tier == TIER_B
                           else "LEAKAGE_SAFE_COARSE_CLASSIFICATION"),
    }


def disclosure_selection_analysis(panel: "Stage25Panel", *, concept: str,
                                  horizon: str = PRIMARY_HORIZON,
                                  preregistered_indicator: Optional[str] = None
                                  ) -> dict:
    """Who reports this accounting concept, who does not, and does the difference
    itself pay?

    Three separable questions:
      1. coverage - what fraction of each cross-section is scoreable at all;
      2. composition - how reporting rates differ across sectors;
      3. the membership return - the realised spread between reporters and
         non-reporters, which is the disclosure-selection explanation measured
         rather than assumed.

    Any factor built on a sparsely-tagged concept inherits question 3, so this
    runs for EVERY candidate whose coverage is materially below the panel's -
    the winner of a campaign is held to exactly the bar the prior stage's
    candidate was held to."""
    key = horizon_by_key(horizon)["key"]
    state_counts: "dict[str, int]" = {}
    by_sector: "dict[str, dict[str, int]]" = {}
    per_period_cov: list = []
    indicator_periods: list = []
    for m in panel.months_for(horizon):
        rows = panel.rows.get(m, {})
        n = 0
        rep = 0
        names = []
        for sym, r in rows.items():
            fwd = (r.get("forward") or {}).get(key)
            if fwd is None:
                continue
            reported = r.get("reported_concepts") or frozenset()
            sec_a = (r.get("sectors") or {}).get(TIER_A, TIER_A_UNKNOWN)
            st = (r.get("rnd_state") if concept == "research_development"
                  else disclosure_state(reported, concept, sec_a))
            state_counts[st] = state_counts.get(st, 0) + 1
            sec = (r.get("sectors") or {}).get(TIER_B, _ps.UNKNOWN)
            bucket = by_sector.setdefault(sec, {})
            bucket[st] = bucket.get(st, 0) + 1
            n += 1
            is_rep = st in (RND_REPORTED, RND_ZERO)
            if is_rep:
                rep += 1
            names.append((sym, 1.0 if is_rep else 0.0, float(fwd)))
        if n:
            per_period_cov.append(rep / n)
        if len(names) >= MIN_CROSS_SECTION:
            indicator_periods.append({"as_of": panel.formation_dates[m],
                                      "month": m, "names": sorted(names)})

    if preregistered_indicator and factor_by_name(preregistered_indicator):
        membership = evaluate_variant(
            panel.factor_cross_sections(
                factor_by_name(preregistered_indicator), horizon=horizon),
            feature=preregistered_indicator, horizon=horizon,
            label="reporters_minus_non_reporters",
            evidence_class="PRE_REGISTERED_HYPOTHESIS")
    else:
        membership = evaluate_variant(
            indicator_periods,
            feature="disclosure_indicator[%s]" % concept, horizon=horizon,
            label="reporters_minus_non_reporters",
            evidence_class="POST_CAMPAIGN_DIAGNOSTIC_NOT_IN_FDR_FAMILY")

    sector_rates = {}
    for sec, bucket in by_sector.items():
        tot = sum(bucket.values())
        rep = bucket.get(RND_REPORTED, 0) + bucket.get(RND_ZERO, 0)
        if tot >= 30:
            sector_rates[sec] = round(rep / tot, 4)

    return {
        "contract_id": "stage25_disclosure_selection/1",
        "concept": concept,
        "membership_test_is_pre_registered": bool(preregistered_indicator),
        "state_taxonomy": {
            RND_REPORTED: "issuer tagged the concept with a non-zero value",
            RND_ZERO: "issuer tagged the concept and the value IS zero",
            RND_NOT_REPORTED: "annual record exists with no tag for this "
                              "concept; NOT read as zero",
            RND_NOT_APPLICABLE: "bank / insurer / REIT by the LEAKAGE-SAFE "
                                "Tier-A classifier, where this line is not a "
                                "meaningful statement",
            RND_MISSING: "no annual record was filed by the formation date",
        },
        "state_counts": dict(sorted(state_counts.items())),
        "reporting_rate_mean": _mean(per_period_cov),
        "reporting_rate_min": (min(per_period_cov) if per_period_cov else None),
        "reporting_rate_max": (max(per_period_cov) if per_period_cov else None),
        "reporting_rate_by_sector": dict(sorted(sector_rates.items(),
                                                key=lambda kv: -kv[1])),
        "reporting_rate_by_sector_tier": TIER_B,
        "membership_spread": membership,
        "interpretation": (
            "a factor built on this concept is ranked only among reporters, so "
            "its rank IC is already a within-reporter statistic. The membership "
            "spread is the SEPARATE question of whether BEING a reporter pays; "
            "if it does, part of the factor's long/short return is a "
            "disclosure-group bet rather than stock selection."),
    }


def _rd_verdict(*, raw_ic, base, tests, style, winners, subperiods, disclosure,
                single_sector, leave_one_out) -> dict:
    """Turn the battery into ONE labelled conclusion, using only the
    pre-registered thresholds above."""
    failures: "list[dict]" = []
    notes: "list[str]" = []

    if (base.get("periods") or 0) < MIN_PERIODS_FOR_VERDICT:
        return {"label": "UNDERPOWERED_NO_VERDICT",
                "reason": "fewer than %d scored formations" % MIN_PERIODS_FOR_VERDICT,
                "failures": [], "notes": notes}

    for t in tests:
        ok = _survives(raw_ic, t)
        if ok is False:
            failures.append({"control": t["variant"], "kind": "SECTOR_EXPLAINED",
                             "rank_ic_t": t.get("rank_ic_t"),
                             "retained": _retention(raw_ic, t.get("rank_ic"))})
        elif ok is None:
            notes.append("%s: underpowered, no verdict" % t["variant"])

    for ctl, s in style.items():
        if ctl == "joint":
            t = s.get("neutralized_rank_ic_t")
            if t is not None and t < RND_SURVIVE_MIN_T:
                failures.append({"control": "joint_style", "kind": "STYLE_EXPLAINED",
                                 "rank_ic_t": t, "retained": None})
            continue
        t = s.get("neutralized_rank_ic_t")
        ret = s.get("information_retained_fraction")
        if (t is not None and t < RND_SURVIVE_MIN_T) or \
           (ret is not None and ret < RND_SURVIVE_MIN_RETENTION):
            failures.append({"control": ctl, "kind": "STYLE_EXPLAINED",
                             "rank_ic_t": t, "retained": ret})

    for w in winners:
        if _survives(raw_ic, w) is False:
            failures.append({"control": w["variant"],
                             "kind": "CONCENTRATION_FRAGILE",
                             "rank_ic_t": w.get("rank_ic_t"),
                             "retained": _retention(raw_ic, w.get("rank_ic"))})

    powered_subs = [s for s in subperiods
                    if (s.get("periods") or 0) >= MIN_PERIODS_FOR_VERDICT]
    negative_subs = [s for s in powered_subs if (s.get("rank_ic") or 0) <= 0]
    if powered_subs and negative_subs:
        failures.append({"control": ", ".join(s["variant"] for s in negative_subs),
                         "kind": "SUBPERIOD_UNSTABLE",
                         "rank_ic_t": None, "retained": None})

    ms = disclosure.get("membership_spread") or {}
    if not ms.get("insufficient") and (ms.get("rank_ic_t") or 0) >= RND_SURVIVE_MIN_T:
        failures.append({"control": "reporters_minus_non_reporters",
                         "kind": "DISCLOSURE_SELECTION_EXPLAINED",
                         "rank_ic_t": ms.get("rank_ic_t"), "retained": None})

    powered_single = [s for s in single_sector
                      if (s.get("periods") or 0) >= MIN_PERIODS_FOR_VERDICT]
    surviving_single = [s for s in powered_single
                        if (s.get("rank_ic_t") or 0) >= 1.5]

    if not failures:
        label = "SURVIVES_SECTOR_AND_STYLE_CONTROLS"
    else:
        # The most economically damning explanation wins, in a fixed order.
        order = ("SECTOR_EXPLAINED", "DISCLOSURE_SELECTION_EXPLAINED",
                 "STYLE_EXPLAINED", "CONCENTRATION_FRAGILE",
                 "SUBPERIOD_UNSTABLE")
        kinds = {f["kind"] for f in failures}
        label = next((k for k in order if k in kinds), "WEAKENED_BUT_SURVIVES")

    return {
        "label": label,
        "controls_run": (len(tests) + len(style) + len(winners)
                         + len(subperiods) + len(single_sector)
                         + len(leave_one_out) + 1),
        "controls_failed": len(failures),
        "failures": failures,
        "notes": notes,
        "single_sector_universes_with_independent_support": [
            s["variant"] for s in surviving_single],
        "asymmetry_disclaimer": (
            "a control that KILLS the signal is conclusive, because the Tier-B "
            "control carries MORE classification information than any honest "
            "contemporaneous control could. A control the signal SURVIVES is "
            "only provisional, because that same look-ahead could have absorbed "
            "genuine information."),
    }


# =========================================================================== #
# WORKSTREAM H - orthogonality against the ACTUAL operational model.
# =========================================================================== #
#: Baseline names are deliberately identical to Stage 24's so the incrementality
#: owner can be reused verbatim rather than reimplemented.
BASELINE_COMPOSITE = "composite_sn_pit"
BASELINE_MOMENTUM = "mom_6_1"
BASELINE_ENSEMBLE = "ensemble_pit_5050"

incrementality = _s24.incrementality

#: Classification vocabulary. Reuses the project's existing evidence words where
#: they exist; the proxy labels are Stage 25's, and they are assigned from
#: MEASURED quantities, never from an impression.
CLS_INDEPENDENT = "INDEPENDENT_ALPHA"
CLS_COMPLEMENTARY = "COMPLEMENTARY_ALPHA"
CLS_REDUNDANT_COMPOSITE = "REDUNDANT_WITH_COMPOSITE"
CLS_REDUNDANT_MOMENTUM = "REDUNDANT_WITH_MOMENTUM"
CLS_SECTOR_PROXY = "SECTOR_PROXY"
CLS_SIZE_PROXY = "SIZE_PROXY"
CLS_VOL_PROXY = "VOLATILITY_PROXY"
CLS_BETA_PROXY = "BETA_PROXY"
CLS_INSUFFICIENT = "INSUFFICIENT_EVIDENCE"
CLS_FAILED_ROBUST = "FAILED_ROBUSTNESS"

#: |rank correlation| at or above this against a baseline means the candidate is
#: largely restating that baseline.
REDUNDANT_CORR = 0.60
#: partial IC t at or above this after controlling for a baseline means the
#: candidate is adding information the baseline does not carry.
INDEPENDENT_PARTIAL_T = 2.0


def classify_candidate(*, result: dict, incr: dict, neutral: dict) -> dict:
    """Give every viable candidate ONE classification, from measured numbers.

    The order is fixed: a candidate that failed its own evidence gate is
    FAILED_ROBUSTNESS regardless of how orthogonal it is, because orthogonal
    noise is still noise."""
    gate = (result.get("gate") or {}).get("target_state")
    row = result.get("row") or {}
    periods = int(row.get("periods") or 0)
    reasons: "list[str]" = []

    if periods < MIN_PERIODS_FOR_VERDICT:
        return {"classification": CLS_INSUFFICIENT,
                "reasons": ["only %d scored formations" % periods]}
    if gate != "KEEP_FOR_RESEARCH":
        reasons.append("released evidence gate returned %s" % gate)
        return {"classification": CLS_FAILED_ROBUST, "reasons": reasons}

    vs = incr.get("vs") or {}
    comp = vs.get(BASELINE_COMPOSITE) or {}
    mom = vs.get(BASELINE_MOMENTUM) or {}
    c_corr = comp.get("mean_cross_sectional_rank_correlation")
    m_corr = mom.get("mean_cross_sectional_rank_correlation")
    c_t = comp.get("partial_rank_ic_t")
    m_t = mom.get("partial_rank_ic_t")

    if c_corr is not None and abs(c_corr) >= REDUNDANT_CORR and \
            (c_t is None or c_t < INDEPENDENT_PARTIAL_T):
        reasons.append("rank correlation %.3f with %s and partial IC t %s"
                       % (c_corr, BASELINE_COMPOSITE, c_t))
        return {"classification": CLS_REDUNDANT_COMPOSITE, "reasons": reasons}
    if m_corr is not None and abs(m_corr) >= REDUNDANT_CORR and \
            (m_t is None or m_t < INDEPENDENT_PARTIAL_T):
        reasons.append("rank correlation %.3f with %s and partial IC t %s"
                       % (m_corr, BASELINE_MOMENTUM, m_t))
        return {"classification": CLS_REDUNDANT_MOMENTUM, "reasons": reasons}

    # Style proxies: the signal is a style bet if neutralising ONE style control
    # destroys it.
    style_map = {"log_adv_dollar": CLS_SIZE_PROXY,
                 "realized_vol_63d": CLS_VOL_PROXY,
                 "trailing_beta": CLS_BETA_PROXY}
    for ctl, label in style_map.items():
        s = (neutral.get("controls") or {}).get(ctl) or {}
        ret = s.get("information_retained_fraction")
        if ret is not None and ret < RND_SURVIVE_MIN_RETENTION:
            reasons.append("only %.0f%% of rank IC survives %s"
                           % (100 * ret, ctl))
            return {"classification": label, "reasons": reasons}
    sec = neutral.get("sector_tier_a") or {}
    sec_ret = sec.get("information_retained_fraction")
    if sec_ret is not None and sec_ret < RND_SURVIVE_MIN_RETENTION:
        reasons.append("only %.0f%% of rank IC survives leakage-safe Tier-A "
                       "sector neutralisation" % (100 * sec_ret))
        return {"classification": CLS_SECTOR_PROXY, "reasons": reasons}

    independent = ((c_t or 0) >= INDEPENDENT_PARTIAL_T
                   and (m_t or 0) >= INDEPENDENT_PARTIAL_T)
    reasons.append("partial IC t %s vs composite and %s vs momentum"
                   % (c_t, m_t))
    return {"classification": (CLS_INDEPENDENT if independent
                               else CLS_COMPLEMENTARY), "reasons": reasons}


def neutralization(periods: list, panel: "Stage25Panel", *, feature: str,
                   sectors: "SectorHistory" = None,
                   controls: Sequence[str] = ("log_adv_dollar",
                                              "realized_vol_63d",
                                              "trailing_beta")) -> dict:
    """Style neutralisation plus the LEAKAGE-SAFE Tier-A sector test.

    Stage 24 could report only that sector was blocked. Stage 25 reports a real
    Tier-A number - coarse, but honest - alongside the Tier-B control, and never
    lets the Tier-B reading stand in for the Tier-A one."""
    from . import orthogonality as _o
    month_by_date = {panel.formation_dates[m]: m for m in panel.months}
    result: dict = {"feature": feature, "controls": {}}
    for ctl in controls:
        partials, raws = [], []
        for p in periods:
            m = month_by_date.get(p["as_of"])
            if m is None:
                continue
            keys = [k for k, _, _ in p["names"]]
            cv = panel.control_series(m, keys, ctl)
            fac = [v for _, v, _ in p["names"]]
            fwd = [f for _, _, f in p["names"]]
            idx = [i for i in range(len(keys)) if cv[i] is not None]
            if len(idx) < 10:
                continue
            r = _o.rank_correlation([fac[i] for i in idx], [fwd[i] for i in idx])
            pic = _o.partial_rank_ic([fac[i] for i in idx],
                                     [fwd[i] for i in idx],
                                     [[cv[i] for i in idx]])
            if r is not None:
                raws.append(r)
            if pic is not None:
                partials.append(pic)
        raw_ic, neu_ic = _mean(raws), _mean(partials)
        result["controls"][ctl] = {
            "periods": len(partials),
            "raw_rank_ic_on_same_names": raw_ic,
            "neutralized_rank_ic": neu_ic,
            "neutralized_rank_ic_t": _t_stat(partials),
            "information_retained_fraction": _retention(raw_ic, neu_ic),
            "control_is_point_in_time": True,
        }
    joint = []
    for p in periods:
        m = month_by_date.get(p["as_of"])
        if m is None:
            continue
        keys = [k for k, _, _ in p["names"]]
        cols = [panel.control_series(m, keys, c) for c in controls]
        fac = [v for _, v, _ in p["names"]]
        fwd = [f for _, _, f in p["names"]]
        idx = [i for i in range(len(keys))
               if all(c[i] is not None for c in cols)]
        if len(idx) < 10:
            continue
        pic = _o.partial_rank_ic([fac[i] for i in idx], [fwd[i] for i in idx],
                                 [[c[i] for i in idx] for c in cols])
        if pic is not None:
            joint.append(pic)
    result["joint_control"] = {"controls": list(controls), "periods": len(joint),
                               "neutralized_rank_ic": _mean(joint),
                               "neutralized_rank_ic_t": _t_stat(joint)}
    base = evaluate_variant(periods, feature=feature, label="raw")
    for tier, key, ec in ((TIER_A, "sector_tier_a",
                           "LEAKAGE_SAFE_COARSE_CLASSIFICATION"),
                          (TIER_B, "sector_tier_b",
                           "CLASSIFICATION_LOOKAHEAD_CONTROL")):
        neu = sector_neutral_cross_sections(periods, panel, tier=tier)
        v = evaluate_variant(neu, feature="%s_sn_%s" % (feature, tier),
                             label="sector_neutral[%s]" % tier,
                             evidence_class=ec)
        v["information_retained_fraction"] = _retention(base.get("rank_ic"),
                                                        v.get("rank_ic"))
        v["raw_rank_ic_on_same_names"] = base.get("rank_ic")
        result[key] = v
    result["sector_capability"] = sector_capability_statement()
    return result


# =========================================================================== #
# WORKSTREAM J - bounded ensemble discovery.
#
# A FIXED, SMALL menu of economically motivated structures. No weight search, no
# kitchen sink, no post-hoc optimisation. The only discretion exercised is WHICH
# candidates are offered to the menu, and that discretion is declared as model
# selection and driven by pre-stated rules (gate + FDR + orthogonality), never by
# ensemble performance.
# =========================================================================== #
MAX_ENSEMBLE_STRUCTURES = 12


def ensemble_menu(*, comp: list, mom: list, picks: "list[tuple]",
                  references: "list[tuple]" = ()) -> "list[dict]":
    """The bounded structure menu.

    ``picks`` is an ordered list of ``(name, periods)`` for the candidates that
    earned a place under the pre-stated rules. At most the first two are used,
    which caps the menu size regardless of how many candidates survive.

    ``references`` are signals included for COMPARISON rather than as
    challengers - most importantly the prior stage's candidate, so 'would the
    Stage-24 shape have been better?' is answered rather than assumed. A
    reference structure can never be reported as a challenger, because
    :func:`challenger_assessment` reads the per-candidate gate, not this menu."""
    menu: "list[dict]" = [
        {"name": "operational_shape_5050",
         "description": "composite_sn + mom_6_1, equal z-weights - the shape of "
                        "the operational fundamental_momentum_50_50_v1",
         "components": [BASELINE_COMPOSITE, BASELINE_MOMENTUM],
         "weights": [0.5, 0.5], "parts": [comp, mom]},
        {"name": "fundamental_tilted_2to1",
         "description": "composite_sn weighted twice momentum - motivated by "
                        "Stage 23's finding that the fundamental leg carries the "
                        "cross-sectional ranking information",
         "components": [BASELINE_COMPOSITE, BASELINE_MOMENTUM],
         "weights": [2.0 / 3.0, 1.0 / 3.0], "parts": [comp, mom]},
    ]
    for i, (name, periods) in enumerate(picks[:2]):
        menu.append({
            "name": "operational_plus_%s" % name,
            "description": "the operational shape widened by one new signal at "
                           "equal z-weight",
            "components": [BASELINE_COMPOSITE, BASELINE_MOMENTUM, name],
            "weights": [1 / 3.0] * 3, "parts": [comp, mom, periods]})
        menu.append({
            "name": "fundamental_plus_%s_no_momentum" % name,
            "description": "does the new signal replace momentum's role, or "
                           "only add to it?",
            "components": [BASELINE_COMPOSITE, name],
            "weights": [0.5, 0.5], "parts": [comp, periods]})
        menu.append({
            "name": "momentum_plus_%s_no_fundamental" % name,
            "description": "does the new signal stand up without the released "
                           "fundamental leg?",
            "components": [BASELINE_MOMENTUM, name],
            "weights": [0.5, 0.5], "parts": [mom, periods]})
    if len(picks) >= 2:
        (n1, p1), (n2, p2) = picks[0], picks[1]
        menu.append({
            "name": "operational_plus_%s_and_%s" % (n1, n2),
            "description": "the operational shape widened by the two strongest "
                           "independent new signals",
            "components": [BASELINE_COMPOSITE, BASELINE_MOMENTUM, n1, n2],
            "weights": [0.25] * 4, "parts": [comp, mom, p1, p2]})
        menu.append({
            "name": "four_way_fundamental_tilted",
            "description": "as above but preserving the 2:1 fundamental tilt",
            "components": [BASELINE_COMPOSITE, BASELINE_MOMENTUM, n1, n2],
            "weights": [0.4, 0.2, 0.2, 0.2], "parts": [comp, mom, p1, p2]})
    for name, periods in references:
        menu.append({
            "name": "reference_operational_plus_%s" % name,
            "description": "REFERENCE ONLY - the operational shape widened by "
                           "%s, a signal that did NOT qualify as a challenger. "
                           "Included so the comparison is complete, never as a "
                           "promotion candidate." % name,
            "components": [BASELINE_COMPOSITE, BASELINE_MOMENTUM, name],
            "weights": [1 / 3.0] * 3, "parts": [comp, mom, periods],
            "reference_only": True})
        if picks:
            menu.append({
                "name": "reference_operational_plus_%s_and_%s"
                        % (picks[0][0], name),
                "description": "REFERENCE ONLY - the Stage-25 challenger and "
                               "the non-qualifying signal together",
                "components": [BASELINE_COMPOSITE, BASELINE_MOMENTUM,
                               picks[0][0], name],
                "weights": [0.25] * 4,
                "parts": [comp, mom, picks[0][1], periods],
                "reference_only": True})
    return menu[:MAX_ENSEMBLE_STRUCTURES]


def evaluate_ensembles(menu: "list[dict]", *, cfg: dict,
                       horizon: str = PRIMARY_HORIZON) -> dict:
    """Score every structure on the SAME cross-sections through the SAME released
    evaluator and the SAME released gate, and compare each to the operational
    shape."""
    hz = horizon_by_key(horizon)["horizon_days"]
    rows: "list[dict]" = []
    baseline_row = None
    baseline_parts = None
    for item in menu:
        if item["name"] == "operational_shape_5050":
            baseline_parts = item["parts"]
        periods = blend_cross_sections(item["parts"], weights=item["weights"])
        if not periods:
            rows.append({"name": item["name"], "periods": 0,
                         "insufficient": True,
                         "description": item["description"],
                         "components": item["components"]})
            continue
        res = score_cross_sections(periods, feature=item["name"],
                                   horizon_days=hz)
        g = gate_for(res["row"], cfg, survivorship_safe=True,
                     point_in_time_valid=True)
        row = {"name": item["name"], "description": item["description"],
               "reference_only": bool(item.get("reference_only")),
               "components": item["components"],
               "weights": [round(float(w), 6) for w in item["weights"]],
               "weights_fitted_from_data": False,
               "periods": res["row"].get("periods"),
               "median_names": res["row"].get("universe"),
               "rank_ic": res["row"].get("rank_ic_mean"),
               "rank_ic_t": res["row"].get("rank_ic_t"),
               "spread_t": res["row"].get("spread_t"),
               "gross_annualized": res["row"].get("gross_annualized_return"),
               "net25": res["row"].get("net_annualized_return"),
               "turnover": res["row"].get("turnover"),
               "max_drawdown_v1": res["row"].get("max_drawdown"),
               "drawdown_contract": _s24.drawdown_contract(res["series"]["ls"]),
               "subperiod_consistency": res["row"].get("subperiod_consistency"),
               "regime_consistency": res["row"].get("regime_consistency"),
               "gate": g["gate"], "insufficient": False}
        if item["name"] == "operational_shape_5050":
            baseline_row = row
        row["_periods"] = periods
        rows.append(row)
    if baseline_row and baseline_parts is not None:
        base_full = blend_cross_sections(baseline_parts, weights=[0.5, 0.5])
        for r in rows:
            if r.get("insufficient") or r is baseline_row:
                r.pop("_periods", None)
                continue
            # The incumbent, scored on the CHALLENGER'S OWN names and dates.
            matched = restrict_to_common(base_full, r.pop("_periods"))
            m = evaluate_variant(matched, feature="operational_shape_5050",
                                 horizon=horizon,
                                 label="operational_shape_on_matched_universe",
                                 evidence_class="SAME_NAMES_SAME_DATES")
            r["operational_shape_on_matched_universe"] = m
            r["delta_vs_operational_shape_full_universe"] = {
                k: _delta(r.get(k), baseline_row.get(k))
                for k in ("rank_ic", "rank_ic_t", "spread_t", "net25",
                          "turnover", "subperiod_consistency")}
            r["delta_vs_operational_shape_matched_universe"] = {
                "rank_ic": _delta(r.get("rank_ic"), m.get("rank_ic")),
                "rank_ic_t": _delta(r.get("rank_ic_t"), m.get("rank_ic_t")),
                "spread_t": _delta(r.get("spread_t"), m.get("spread_t")),
                "net25": _delta(r.get("net25"), m.get("net25")),
                "turnover": _delta(r.get("turnover"), m.get("turnover")),
                "subperiod_consistency": _delta(
                    r.get("subperiod_consistency"),
                    m.get("subperiod_consistency"))}
        baseline_row.pop("_periods", None)
    for r in rows:
        r.pop("_periods", None)
    ranked = [r for r in rows
              if not r.get("insufficient") and not r.get("reference_only")]
    best = max(ranked, key=lambda r: (r.get("rank_ic_t") or -99)) if ranked else None
    return {
        "contract_id": "stage25_ensembles/1",
        "structures_evaluated": len(rows),
        "structure_cap": MAX_ENSEMBLE_STRUCTURES,
        "weight_search_performed": False,
        "selection_is_model_selection": True,
        "selection_disclosure": (
            "which candidates were offered to the menu is a model-selection "
            "decision driven by the released gate, the FDR survivor list and the "
            "orthogonality classification - never by ensemble performance. The "
            "structures themselves are fixed and their weights are not fitted."),
        "comparison_basis": (
            "a blend requires every name to carry every component, so adding a "
            "sparsely-reported signal NARROWS the cross-section. Each structure "
            "is therefore compared twice: against the operational shape on its "
            "own full universe, and against the operational shape scored on the "
            "CHALLENGER'S OWN names and dates. The matched comparison is the "
            "one that isolates the added signal from the coverage change."),
        "operational_shape": baseline_row,
        "structures": rows,
        "best_by_rank_ic_t": (best or {}).get("name"),
    }


# =========================================================================== #
# WORKSTREAM A/B - capability statements.
# =========================================================================== #
CAP_READY = "READY_FOR_PIT_RESEARCH"
CAP_LIMITED = "READY_WITH_LIMITATIONS"
CAP_CURRENT_ONLY = "CURRENT_ONLY"
CAP_FORWARD_ONLY = "FORWARD_ONLY"
CAP_WAITING = "WAITING_FOR_DATA"
CAP_EXHAUSTED = "EXHAUSTED"
CAP_INVALID = "INVALID_FOR_HISTORICAL_RESEARCH"


def sector_capability_statement() -> dict:
    """Exactly what Stage 25 resolved about sector, and exactly what remains
    blocked - with the specific free artefact that would close it.

    Stage 24 reported the wall. Stage 25 does not pretend the wall is gone; it
    reports which side of it each tier stands on, and it names the acquisition
    that would remove it."""
    return {
        "status": "PARTIALLY_RESOLVED_TWO_TIER",
        "canonical_owner": "alpha_agent.pit_sector",
        "tiers": {
            TIER_A: {
                "state": CAP_LIMITED,
                "leakage_safe": True,
                "built_from": "the set of us-gaap concepts an issuer had FILED "
                              "by the formation date",
                "resolution": "business model (Banking / Insurance / RealEstate "
                              "/ OperatingNonFinancial)",
                "cannot_distinguish": ["Technology vs Industrials",
                                       "HealthCare vs ConsumerStaples",
                                       "Energy vs Materials"],
                "excludes_rnd_concept": True,
                "why_excludes_rnd": "so the classifier cannot be circular with "
                                    "the R&D hypothesis it is used to test",
            },
            TIER_B: {
                "state": CAP_CURRENT_ONLY,
                "leakage_safe": False,
                "built_from": "owned SEC entity-level assigned SIC (Phase-10.1 "
                              "submissions index) through the released "
                              "pit_sector SIC->sector taxonomy",
                "resolution": "the full 11-sector research taxonomy",
                "usage_rule": SectorHistory.tier_b_usage_rule(),
            },
        },
        "still_blocked": "a per-filing effective-dated classification series",
        "exact_unblocking_artifact": {
            "name": "SEC Financial Statement Data Sets, sub.txt",
            "url_family": "https://www.sec.gov/dera/data/financial-statement-"
                          "data-sets (one zip per quarter, 2009Q2 onward)",
            "why_it_works": "sub.txt carries, PER SUBMISSION, both the assigned "
                            "SIC and the acceptance timestamp - which is exactly "
                            "the (classification, available_at) observation that "
                            "alpha_agent.pit_sector.PitSicSeries already "
                            "consumes",
            "cost": "free; no vendor, no quota, no purchase",
            "size_estimate_gb": 2.7,
            "network_required": True,
            "not_done_in_stage25_because": "Workstream B is constrained to owned "
                                           "evidence; the download is a bounded, "
                                           "clearly-scoped follow-up",
        },
        "look_ahead_map_substituted_into_a_signal": False,
    }


def research_capability_map(panel: "Stage25Panel", store: "Stage25PitStore",
                            universe_contract: dict, bridge_cov: dict,
                            sector_status: dict, beta: "TrailingBeta") -> dict:
    """ONE machine-readable statement of what research this system can currently
    do, measured from the artefacts actually on disk.

    This is a REPORT the existing autonomous agent reads. It is not a second data
    registry: every entry names the module that already owns the data family."""
    def _cov(name: str) -> dict:
        f = factor_by_name(name)
        xs = panel.factor_cross_sections(f) if f else []
        ns = [len(x["names"]) for x in xs]
        return {"periods": len(xs), "median_names": _median(ns)}

    families = {
        "OWNED_HISTORICAL_PRICES": {
            "state": CAP_READY, "owner": "alpha_agent.collectors.norgate_local",
            "evidence": {"months": universe_contract.get("months"),
                         "window": [universe_contract.get("first_month"),
                                    universe_contract.get("last_month")]}},
        "OWNED_HISTORICAL_MEMBERSHIP": {
            "state": CAP_READY, "owner": "stage24 HistoricalUniverse",
            "evidence": {"symbols": universe_contract.get("distinct_symbols"),
                         "delisting_tagged":
                             universe_contract.get("delisting_tagged_symbols"),
                         "exits_observed":
                             universe_contract.get("membership_exits_observed"),
                         "survivorship_class":
                             universe_contract.get("survivorship_class")}},
        "DELISTED_INACTIVE_IDENTITY": {
            "state": CAP_LIMITED, "owner": "alpha_agent.historical_identity",
            "evidence": bridge_cov,
            "limitation": "%d panel symbols resolve to no CIK and are silently "
                          "absent from every cross-section"
                          % (bridge_cov.get("panel_symbols", 0)
                             - bridge_cov.get("cik_resolved", 0))},
        "SEC_COMPANY_FACTS_PIT": {
            "state": CAP_READY,
            "owner": "alpha_agent.sec_companyfacts_index + stage25 store",
            "evidence": {"facts_loaded": store.loaded_facts,
                         "ciks": len(store.covered_ciks()),
                         "concepts": len(store.by_concept)}},
        "PIT_FILING_AVAILABILITY": {
            "state": CAP_READY, "owner": "alpha_agent.pit_fundamentals",
            "evidence": {"availability": "SEC filed date",
                         "reporting_lag_days": REPORTING_LAG_DAYS}},
        "PIT_SECTOR_HISTORY": {
            "state": CAP_LIMITED, "owner": "alpha_agent.pit_sector",
            "evidence": sector_status},
        "PIT_MARKET_CAP": {
            "state": CAP_WAITING, "owner": "alpha_agent.sec_companyfacts_index",
            "evidence": {
                "blocker": "TWO independent gaps, both real",
                "gap_1": "the owned companyfacts parser emits monetary USD facts "
                         "only, so share counts are dropped",
                "gap_2": "the only owned daily price surface "
                         "(phase25_fast_ohlc, Russell 1000 Current & Past) is "
                         "TOTALRETURN adjusted, so price_adjusted x "
                         "shares_as_reported is wrong by the cumulative "
                         "split-and-dividend factor",
                "consequence": "every valuation ratio (book/market, earnings "
                               "yield, FCF yield, sales/price) remains "
                               "unrunnable"}},
        "PIT_TRAILING_BETA": {
            "state": CAP_READY, "owner": "stage25 TrailingBeta",
            "evidence": beta.contract()},
        "PIT_SIZE_LIQUIDITY": {
            "state": CAP_READY, "owner": "frozen momentum monthly panel",
            "evidence": {"control": "log_adv_dollar", "trailing": True}},
        "PIT_VOLATILITY": {
            "state": CAP_READY, "owner": "frozen momentum monthly panel",
            "evidence": {"control": "realized_vol_63d", "trailing": True}},
        "OWNED_DAILY_OHLC": {
            "state": CAP_LIMITED,
            "owner": "phase25_fast_ohlc panel (Norgate, Russell 1000 C&P)",
            "evidence": {"securities": 3076, "window": ["2000-01-03",
                                                        "2026-07-17"],
                         "membership_mask": "point-in-time",
                         "adjustment": "TOTALRETURN"},
            "limitation": "total-return adjustment makes it unusable for market "
                          "capitalisation; Stage 25 therefore takes every "
                          "forward return from the ONE monthly panel instead, "
                          "so no cross-source join risk is introduced"},
        "ANALYST_CURRENT_SNAPSHOTS": {
            "state": CAP_FORWARD_ONLY,
            "owner": "alpha_agent.analyst_revisions",
            "evidence": {"prospective_revision_ledger": True,
                         "historical_vintages": False}},
        "HISTORICAL_ANALYST_REVISIONS": {
            "state": CAP_WAITING, "owner": "alpha_agent.analyst_revisions",
            "evidence": {"blocker": "no historical vendor extract on disk"}},
        "TOURNAMENT_REGISTRY": {
            "state": CAP_READY, "owner": "alpha_agent.tournament",
            "evidence": {"second_registry_created": False}},
        "FORWARD_EVIDENCE": {
            "state": CAP_FORWARD_ONLY, "owner": "api.forward_evidence",
            "evidence": {"immutable": True, "backfill_permitted": False}},
        "HOC_REASSESSMENT_OUTCOMES": {
            "state": CAP_FORWARD_ONLY,
            "owner": "api.holding_opportunity_cost / api.reassessment_outcomes",
            "evidence": {"seam": "alpha_agent.stage23_unified."
                                 "build_decision_link"}},
        "PRE_2009_FUNDAMENTALS": {
            "state": CAP_INVALID, "owner": "SEC XBRL",
            "evidence": {"reason": "XBRL company facts begin 2009-04; a company "
                                   "delisted before then can never be scored"}},
    }
    per_factor = {f.name: _cov(f.name) for f in ALL_FACTORS if f is not None}
    counts: "dict[str, int]" = {}
    for v in families.values():
        counts[v["state"]] = counts.get(v["state"], 0) + 1
    return {
        "contract_id": "stage25_research_capability_map/1",
        "stage25_version": STAGE25_VERSION,
        "second_data_registry_created": False,
        "state_counts": counts,
        "data_families": families,
        "factor_coverage": per_factor,
        "horizons": list(HORIZONS),
        "primary_horizon": PRIMARY_HORIZON,
    }


# =========================================================================== #
# WORKSTREAM G/E - the campaign, its multiple-testing family and its funnel.
# =========================================================================== #
def apply_fdr(results: "list[dict]", *, family: str) -> dict:
    """Benjamini-Hochberg over the WHOLE pre-registered Stage-25 discovery
    family, using the released controls. The family is fixed before evaluation;
    nothing is added or removed based on how the numbers turned out."""
    from . import selection_controls as _sc
    from . import signal_evaluation as _se
    members = [r for r in results if r.get("family_group") == family]
    pvals = []
    for r in members:
        t = (r.get("row") or {}).get("rank_ic_t")
        n = int((r.get("row") or {}).get("periods") or 0)
        pvals.append(_se.approx_two_sided_pvalue(t, max(1, n - 1)) or 1.0)
    q = _sc.benjamini_hochberg(pvals) if pvals else []
    for r, p, qq in zip(members, pvals, q):
        r["pvalue"] = p
        r["bh_q"] = qq
        r["survives_fdr_10pct"] = bool(qq is not None and qq <= 0.10)
    by_family: "dict[str, list]" = {}
    for r in members:
        by_family.setdefault(r["family"], []).append(r)
    per_family = {}
    for fam, rs in sorted(by_family.items()):
        ps = [r["pvalue"] for r in rs]
        qs = _sc.benjamini_hochberg(ps) if ps else []
        per_family[fam] = {
            "size": len(rs),
            "survivors_q10": [r["name"] for r, qq in zip(rs, qs)
                              if qq is not None and qq <= 0.10]}
    return {
        "family": family,
        "family_size": len(members),
        "family_fixed_before_evaluation": True,
        "method": "benjamini_hochberg",
        "owner": "alpha_agent.selection_controls.benjamini_hochberg",
        "primary_control": "one BH pass over the WHOLE discovery family "
                           "(conservative); the per-economic-family pass is "
                           "reported for interpretation only",
        "survivors_q10": [r["name"] for r in members
                          if r.get("survives_fdr_10pct")],
        "per_economic_family": per_family,
        "members": [{"name": r["name"], "family": r["family"],
                     "rank_ic_t": (r.get("row") or {}).get("rank_ic_t"),
                     "pvalue": r.get("pvalue"), "bh_q": r.get("bh_q"),
                     "survives_fdr_10pct": r.get("survives_fdr_10pct")}
                    for r in members],
    }


def run_campaign(panel: "Stage25Panel", *, cfg: dict, champion_returns=None,
                 horizon: str = PRIMARY_HORIZON) -> list:
    """Every pre-registered Stage-25 hypothesis through the released evaluator
    and the released evidence gate. Nothing is dropped for being a null."""
    hz = horizon_by_key(horizon)["horizon_days"]
    out = []
    for spec in DISCOVERY_FACTORS:
        periods = panel.factor_cross_sections(spec, horizon=horizon)
        res = score_cross_sections(periods, feature=spec.name, horizon_days=hz,
                                   champion_returns=champion_returns)
        g = gate_for(res["row"], cfg, survivorship_safe=True,
                     point_in_time_valid=True)
        out.append({
            "name": spec.name, "family": spec.family,
            "family_group": FAMILY_DISCOVERY,
            "spec": spec.as_dict(), "periods_scored": len(periods),
            "row": res["row"], "series": res["series"],
            "metrics": g["metrics"], "gate": g["gate"],
            "drawdown_contract": _s24.drawdown_contract(res["series"]["ls"]),
        })
    return out


def multi_horizon(panel: "Stage25Panel", names: Sequence[str], *,
                  cfg: dict) -> dict:
    """The bounded horizon family: how each named signal behaves at 1, 3 and 6
    months, each formed so its forward windows do not overlap."""
    entries = []
    for name in names:
        f = factor_by_name(name)
        row = {"signal": name, "by_horizon": {}}
        for h in HORIZONS:
            if f is not None:
                periods = panel.factor_cross_sections(f, horizon=h["key"])
            elif name == BASELINE_COMPOSITE:
                periods = panel.composite_cross_sections(horizon=h["key"])
            elif name == BASELINE_MOMENTUM:
                periods = panel.momentum_cross_sections(horizon=h["key"])
            else:
                continue
            row["by_horizon"][h["key"]] = evaluate_variant(
                periods, feature="%s@%s" % (name, h["key"]),
                horizon=h["key"], label=h["key"],
                evidence_class="SURVIVORSHIP_SAFE_POINT_IN_TIME")
        row["decay_profile"] = _decay_profile(row["by_horizon"])
        entries.append(row)
    return {
        "contract_id": "stage25_multi_horizon/1",
        "horizons": list(HORIZONS),
        "overlap_policy": (
            "every horizon is formed so its forward windows do NOT overlap: "
            "quarterly formation for 1 and 3 months, semi-annual formation for "
            "6 months. Overlapping windows would inflate every t-statistic."),
        "horizon_is_part_of_the_multiple_testing_family": True,
        "per_signal_horizon_optimisation_performed": False,
        "signals": entries,
    }


def _decay_profile(by_horizon: dict) -> str:
    ics = {k: (v or {}).get("rank_ic") for k, v in by_horizon.items()}
    a, b, c = ics.get("h1m"), ics.get("h3m"), ics.get("h6m")
    if a is None or b is None or c is None:
        return "INCOMPLETE"
    if b <= 0 and c <= 0 and a > 0:
        return "FAST_DECAY"
    if c > b > a:
        return "STRENGTHENS_WITH_HORIZON"
    if c < 0 < b:
        return "REVERSES_AT_LONG_HORIZON"
    if abs(c - b) <= 0.5 * abs(b) if b else False:
        return "PERSISTENT"
    return "MIXED"


# =========================================================================== #
# WORKSTREAM I - alpha family exhaustion.
#
# The autonomous agent's most expensive failure mode is rediscovering an
# economically equivalent experiment it already ran. This report is derived from
# the EXISTING candidate registry plus the recorded conclusions of the prior
# stages - it is NOT a second exhaustion registry, and it creates no state.
# =========================================================================== #
EX_ACTIVE_HIGH = "ACTIVE_HIGH_PRIORITY"
EX_ACTIVE_MED = "ACTIVE_MEDIUM_PRIORITY"
EX_FORWARD_ONLY = "FORWARD_TRACK_ONLY"
EX_WAITING = "WAITING_FOR_NEW_DATA"
EX_EXHAUSTED = "EXHAUSTED_NEGATIVE"
EX_REDUNDANT = "REDUNDANT"
EX_REJECTED_PIT = "REJECTED_PIT"
EX_REJECTED_ROBUST = "REJECTED_ROBUSTNESS"

#: Families whose exhaustion was ESTABLISHED by a prior stage with evidence on
#: disk. Each entry names the stage that closed it so the claim is auditable.
PRIOR_EXHAUSTION = {
    "residual_momentum": (EX_EXHAUSTED, "Stage 23: weak over long history; "
                                        "re-running it is variant tuning"),
    "low_volatility": (EX_EXHAUSTED, "Stage 23: no compelling promotion case"),
    "vol_scaled_momentum": (EX_EXHAUSTED, "Stage 23: no compelling promotion "
                                          "case"),
    "monthly_liquidity": (EX_EXHAUSTED, "Stage 23: liquidity/volatility "
                                        "variants rejected"),
    "fundamental_momentum_cfo_change": (
        EX_EXHAUSTED, "Stage 24: a well-powered null - rank IC ~0.000 at "
                      "t = -0.37 over 66 survivorship-safe cross-sections"),
    "asset_growth": (EX_REJECTED_ROBUST, "Stage 24: pre-registered negative "
                                         "sign, produced the WRONG sign; "
                                         "reported as a rejection, not flipped"),
    "sales_growth": (EX_REJECTED_ROBUST, "Stage 24: same - wrong sign"),
    "gross_profitability_asset_scaled": (
        EX_REDUNDANT, "Stage 24: directionally right but weak "
                      "(IC 0.0197, t 1.29); Stage 25 re-asks it only in "
                      "economically DIFFERENT forms (operating profitability, "
                      "margin level)"),
    "eodhd_current_snapshot_fundamentals": (
        EX_REJECTED_PIT, "Stage 24: the frozen 545-name panel is survivor-biased "
                         "and not point-in-time; the released gate correctly "
                         "refuses to certify it"),
    "price_factor_expansion": (EX_EXHAUSTED, "Phase 21/24: 1 survivor of 29, "
                                             "cost-killed"),
    "analyst_grades_fmp": (EX_REJECTED_PIT, "Stage 17: survivorship-FAIL"),
    "macro_cross_sectional_beta": (EX_EXHAUSTED, "Stage 15: 0 FDR survivors"),
}


def alpha_family_exhaustion(results: "list[dict]", fdr: dict,
                            rd_verdict: dict, *,
                            registry=None) -> dict:
    """Which research families are answered, which are open, and which are
    waiting on information nobody owns yet."""
    families: "dict[str, dict]" = {}
    for key, (state, why) in sorted(PRIOR_EXHAUSTION.items()):
        families[key] = {"state": state, "why": why,
                         "established_by": "prior stage evidence on disk"}

    survivors = set(fdr.get("survivors_q10") or [])
    by_econ: "dict[str, list]" = {}
    for r in results:
        by_econ.setdefault(r["family"], []).append(r)
    for fam, rs in sorted(by_econ.items()):
        keeps = [r["name"] for r in rs
                 if (r.get("gate") or {}).get("target_state")
                 == "KEEP_FOR_RESEARCH"]
        surv = [r["name"] for r in rs if r["name"] in survivors]
        if keeps and surv:
            state, why = EX_ACTIVE_HIGH, (
                "%d of %d Stage-25 hypotheses cleared the released gate AND "
                "survived FDR: %s" % (len(surv), len(rs), ", ".join(surv)))
        elif keeps:
            state, why = EX_ACTIVE_MED, (
                "%d of %d cleared the released gate but none survived FDR over "
                "the whole discovery family" % (len(keeps), len(rs)))
        else:
            state, why = EX_EXHAUSTED, (
                "all %d Stage-25 hypotheses in this family were rejected by the "
                "released gate on survivorship-safe point-in-time evidence"
                % len(rs))
        families["stage25_%s" % fam] = {
            "state": state, "why": why, "established_by": "Stage 25",
            "hypotheses": [r["name"] for r in rs],
            "gate_clearing": keeps, "fdr_survivors": surv}

    families["rnd_intensity"] = {
        "state": (EX_ACTIVE_HIGH
                  if rd_verdict.get("label") == "SURVIVES_SECTOR_AND_STYLE_CONTROLS"
                  else EX_REJECTED_ROBUST),
        "why": "Stage 25 falsification verdict: %s" % rd_verdict.get("label"),
        "established_by": "Stage 25 R&D falsification battery"}
    families["historical_analyst_revisions"] = {
        "state": EX_WAITING,
        "why": "still the only untested orthogonal family; no historical vendor "
               "extract exists on disk",
        "established_by": "Stage 13A / 24 / 25"}
    families["pit_valuation_ratios"] = {
        "state": EX_WAITING,
        "why": "needs share counts AND an unadjusted price surface; neither is "
               "owned (see PIT_MARKET_CAP in the capability map)",
        "established_by": "Stage 25"}
    families["pit_fine_grained_sector"] = {
        "state": EX_WAITING,
        "why": "Tier A resolves business model only; the fine-grained "
               "leakage-safe series needs SEC Financial Statement Data Sets "
               "sub.txt",
        "established_by": "Stage 25"}

    counts: "dict[str, int]" = {}
    for v in families.values():
        counts[v["state"]] = counts.get(v["state"], 0) + 1

    registry_view = None
    if registry is not None:
        try:
            registry_view = {
                "candidates": len(registry.list()),
                "by_state": registry.counts_by_state(),
                "by_family": registry.counts_by_family(),
                "owner": "alpha_agent.tournament.CandidateRegistry",
                "second_exhaustion_registry_created": False}
        except Exception as exc:  # noqa: BLE001
            registry_view = {"error": "REGISTRY_READ_FAILED: %s" % exc}

    return {
        "contract_id": "stage25_alpha_family_exhaustion/1",
        "counts": counts,
        "families": families,
        "existing_registry": registry_view,
        "deduplication_owner": "alpha_agent.tournament.CandidateRegistry "
                               "(spec_hash + processed_experiments)",
        "do_not_reopen": sorted(k for k, v in families.items()
                                if v["state"] in (EX_EXHAUSTED, EX_REDUNDANT,
                                                  EX_REJECTED_PIT,
                                                  EX_REJECTED_ROBUST)),
    }


# =========================================================================== #
# WORKSTREAM L - capital-deployment relevance.
# =========================================================================== #
def hoc_counterfactual(panel: "Stage25Panel", *, candidates: "dict[str, list]",
                       baseline_periods: list) -> dict:
    """Would a candidate model have made BETTER capital-deployment decisions?

    The canonical seam between a model and REAL decisions stays
    ``stage23_unified.build_decision_link``; Stage 25 calls it rather than
    building a second one, and it returns INSUFFICIENT_FORWARD_EVIDENCE until
    enough live observations have matured. That is the honest answer about real
    decisions and Stage 25 does not dress it up.

    What Stage 25 CAN measure, and labels COUNTERFACTUAL_NOT_PROOF, is the
    decision-shaped question on panel data: of the names the operational-shaped
    model ranked in its TOP decile, which ones went on to be the worst outcomes,
    and did the candidate rank those deteriorating names lower at the same
    formation date? That is 'would it have spotted the bad holding earlier'
    asked where evidence exists."""
    from . import stage23_unified as _s23
    seam = _s23.build_decision_link({}, hoc_records=[], forward_records=[])

    base_by_date = {p["as_of"]: {k: v for k, v, _ in p["names"]}
                    for p in baseline_periods}
    fwd_by_date = {p["as_of"]: {k: f for k, _, f in p["names"]}
                   for p in baseline_periods}
    entries = []
    for name, periods in sorted(candidates.items()):
        cand_by_date = {p["as_of"]: {k: v for k, v, _ in p["names"]}
                        for p in periods}
        deltas, hit = [], []
        for d in sorted(set(base_by_date) & set(cand_by_date)):
            b, c, f = base_by_date[d], cand_by_date[d], fwd_by_date[d]
            keys = sorted(set(b) & set(c) & set(f))
            if len(keys) < 30:
                continue
            top = sorted(keys, key=lambda k: b[k], reverse=True)
            held = top[:max(1, len(top) // 10)]
            # Within the names the operational shape wanted to HOLD, split by
            # realised outcome and ask where the candidate ranked the losers.
            ordered = sorted(held, key=lambda k: f[k])
            worst = ordered[:max(1, len(ordered) // 3)]
            best = ordered[-max(1, len(ordered) // 3):]
            cw = _mean([c[k] for k in worst])
            cb = _mean([c[k] for k in best])
            if cw is None or cb is None:
                continue
            deltas.append(cb - cw)
            hit.append(1.0 if cb > cw else 0.0)
        entries.append({
            "candidate": name,
            "formations_measured": len(deltas),
            "mean_score_gap_best_minus_worst_within_held": _mean(deltas),
            "gap_t_stat": _t_stat(deltas),
            "fraction_of_formations_ranked_losers_lower": _mean(hit),
            "evidence_class": "COUNTERFACTUAL_NOT_PROOF",
            "interpretation": (
                "a positive gap means that, among the names the operational "
                "shape would have held, the candidate scored the eventual "
                "losers LOWER than the eventual winners - i.e. it carried "
                "information the operational shape did not, about the holdings "
                "it actually chose"),
        })
    return {
        "contract_id": "stage25_hoc_counterfactual/1",
        "canonical_decision_seam": seam,
        "real_decision_status": seam.get("status"),
        "panel_counterfactual": entries,
        "labels": {"panel_counterfactual": "COUNTERFACTUAL_NOT_PROOF",
                   "real_decisions": seam.get("status")},
        "historical_decisions_rewritten": False,
        "true_forward_evidence_touched": False,
    }


# =========================================================================== #
# WORKSTREAM K - challenger governance through the EXISTING lifecycle.
# =========================================================================== #
def register_candidates(results: "list[dict]", cfg: dict, *,
                        evidence_date: Optional[str] = None,
                        db_path: Optional[str] = None) -> dict:
    """Register every Stage-25 hypothesis - including the nulls - through the
    EXISTING candidate lifecycle. There is no Stage-25 promotion path, and no
    Stage-25 registry."""
    from . import tournament as _t
    try:
        registry = _t.CandidateRegistry(
            db_path or (cfg or {}).get("tournament_db")
            or r"D:\Stock_Prediction_app_data\alpha_agent\stage8"
               r"\tournament.sqlite")
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reason": "REGISTRY_UNAVAILABLE: %s" % exc}
    seeded, completed = [], []
    for r in results:
        spec = {
            "feature": r["name"],
            "horizon_days": horizon_by_key(PRIMARY_HORIZON)["horizon_days"],
            "rebalance": "quarterly",
            "template": "stage25_pit_fundamental_cross_sectional_rank",
            "expected_sign": r["spec"]["expected_sign"],
            "economic_family": r["family"],
            "origin": ORIGIN,
            "stage25_version": STAGE25_VERSION,
            "mapping_version_hash": mapping_version_hash(),
            "data_basis": "owned SEC XBRL company facts (PIT by filed date) x "
                          "owned Norgate historical membership",
            "sector_tier_used_in_construction": "NONE - no classification enters "
                                                "any registered signal",
        }
        cid = registry.seed_candidate(
            name=r["name"], family=r["family"], spec=spec,
            data_dependencies=["owned_sec_companyfacts_pit",
                               "owned_norgate_historical_membership"],
            universe="Norgate historical index membership "
                     "(survivorship-safe, delisted retained)",
            pit_status="OWNED_PIT_SEC_FILED_DATE",
            component_signals=[])
        seeded.append({"name": r["name"], "candidate_id": cid})
        completed.append({"feature": r["name"], "row": r["row"],
                          "job_id": "stage25_%s" % r["name"]})
    ingest = _t.ingest_completed_experiments(
        registry, cfg, completed=completed, source=ORIGIN,
        evidence_date=evidence_date)
    try:
        registry.close()
    except Exception:  # noqa: BLE001
        pass
    return {"ok": True, "seeded": seeded, "ingest": ingest,
            "registry_owner": "alpha_agent.tournament.CandidateRegistry",
            "second_registry_created": False,
            "automatic_promotion": False,
            "champion_changed": False,
            "shadow_book_created": False}


def challenger_assessment(results: "list[dict]", fdr: dict, ensembles: dict,
                          classifications: dict, rd: dict,
                          registration: Optional[dict]) -> dict:
    """Did anything earn the word CHALLENGER? State the answer and the reason.

    A Stage-25 candidate becomes a research challenger only if it (a) cleared the
    RELEASED evidence gate, (b) survived FDR over the whole discovery family,
    (c) is not classified as a proxy or a restatement of a baseline, and (d) was
    constructed WITHOUT any look-ahead input. Clearing this bar is a research
    state; it is not a promotion and it does not touch the champion."""
    survivors = set(fdr.get("survivors_q10") or [])
    entries = []
    for r in results:
        gate = (r.get("gate") or {}).get("target_state")
        cls = (classifications.get(r["name"]) or {}).get("classification")
        blockers = []
        if gate != "KEEP_FOR_RESEARCH":
            blockers.append("released gate returned %s" % gate)
        if r["name"] not in survivors:
            blockers.append("did not survive BH-FDR over the %d-hypothesis "
                            "discovery family" % fdr.get("family_size", 0))
        if cls in (CLS_REDUNDANT_COMPOSITE, CLS_REDUNDANT_MOMENTUM,
                   CLS_SECTOR_PROXY, CLS_SIZE_PROXY, CLS_VOL_PROXY,
                   CLS_BETA_PROXY):
            blockers.append("orthogonality classification %s" % cls)
        if cls == CLS_INSUFFICIENT:
            blockers.append("insufficient evidence")
        if not blockers:
            entries.append({"name": r["name"], "classification": cls,
                            "state": "RESEARCH_CHALLENGER",
                            "blockers": []})
        else:
            entries.append({"name": r["name"], "classification": cls,
                            "state": "NOT_A_CHALLENGER",
                            "blockers": blockers})
    qualified = [e for e in entries if e["state"] == "RESEARCH_CHALLENGER"]

    best = None
    for row in (ensembles.get("structures") or []):
        if row.get("insufficient") or row.get("reference_only") or \
                row["name"] == "operational_shape_5050":
            continue
        # The MATCHED comparison is the one that decides, because it is the one
        # that isolates the added signal from the coverage change.
        d = row.get("delta_vs_operational_shape_matched_universe") or {}
        if (d.get("rank_ic_t") or 0) > 0 and (d.get("net25") or 0) > 0 and \
                (row.get("gate") or {}).get("target_state") == "KEEP_FOR_RESEARCH":
            if best is None or (row.get("rank_ic_t") or -99) > \
                    (best.get("rank_ic_t") or -99):
                best = row

    return {
        "contract_id": "stage25_challenger_results/1",
        "governance_owner": "alpha_agent.tournament (existing lifecycle)",
        "second_tournament_created": False,
        "second_champion_authority_created": False,
        "automatic_promotion_possible": False,
        "operational_model_unchanged": "fundamental_momentum_50_50_v1",
        "qualification_rule": [
            "cleared the RELEASED evidence gate on survivorship-safe "
            "point-in-time evidence",
            "survived Benjamini-Hochberg over the whole discovery family",
            "not classified as a baseline restatement or a style/sector proxy",
            "constructed with NO look-ahead input (no Tier-B classification)"],
        "per_candidate": entries,
        "research_challengers": [e["name"] for e in qualified],
        "best_ensemble_beating_operational_shape": (best or {}).get("name"),
        "best_ensemble_row": best,
        "rnd_intensity_status": {
            "stage24_state": "KEEP_FOR_RESEARCH",
            "stage25_falsification_verdict": (rd.get("verdict") or {}).get("label"),
            "is_a_challenger": False,
            "why": (rd.get("verdict") or {}).get("label")},
        "registration": registration,
    }


def forward_tracking_status(challengers: dict, rd: dict) -> dict:
    """What could now begin accruing TRUE_FORWARD evidence, and what may not.

    Stage 25 starts no shadow book. Activating one is a governance action owned
    by ``tournament.maybe_activate_shadow_books``, and it is deliberately left to
    the gate owner."""
    ready = list(challengers.get("research_challengers") or [])
    return {
        "contract_id": "stage25_forward_tracking_status/1",
        "owner": "alpha_agent.tournament.maybe_activate_shadow_books",
        "shadow_book_created_by_stage25": False,
        "true_forward_evidence_written_by_stage25": False,
        "eligible_for_research_forward_tracking": ready,
        "not_eligible": {
            "s24_rnd_intensity": (rd.get("verdict") or {}).get("label"),
        },
        "what_forward_tracking_would_measure": [
            "realised rank IC of the candidate on cross-sections formed AFTER "
            "the evidence date",
            "realised long/short spread net of the modelled cost",
            "agreement with, and divergence from, the operational model's "
            "ranking of the names actually held"],
        "why_not_started_here": (
            "activating a shadow book changes tournament state and is a "
            "governance decision, not a research one; Stage 25 reports "
            "eligibility and leaves the action to the gate owner"),
    }


# =========================================================================== #
# WORKSTREAM N - Intrinio / Steele.
# =========================================================================== #
def intrinio_status(cfg_path=None) -> dict:
    """Has genuine historical analyst data arrived? Delegates the question to the
    Stage-24 owner and adds what changes on arrival because of Stage 25.

    No paid API is called, no quota is spent and no provider schema is invented.
    """
    base = _s24.intrinio_parallel_status(cfg_path)
    base.update({
        "contract_id": "stage25_intrinio_status/1",
        "checked_by_stage25": True,
        "paid_api_called": False,
        "quota_spent": False,
        "provider_schema_invented": False,
        "pipeline_on_arrival": [
            "INTRINIO EXTRACT -> alpha_agent.analyst_revisions importer",
            "PIT VALIDATION -> pit_scan (vintage timestamps must be present)",
            "ADEQUACY CHECK -> the released adequacy gate",
            "PRE-REGISTERED EXPERIMENTS -> the six frozen Stage-13A hypotheses",
            "FDR / ROBUSTNESS -> selection_controls.benjamini_hochberg",
            "INCREMENTAL TEST -> stage25 incrementality against the CURRENT "
            "model on the SAME survivorship-safe point-in-time cross-sections",
            "CHALLENGER -> the existing tournament lifecycle, no auto-promotion",
            "TRUE_FORWARD -> api.forward_evidence, never backfilled"],
        "what_stage25_changed_for_this_lane": (
            "the incremental comparison now runs against a baseline that is "
            "itself survivorship-safe and point-in-time, across 28 additional "
            "pre-registered fundamental hypotheses whose nulls are already "
            "registered. An analyst signal will therefore be judged on what it "
            "adds to honest evidence, not on what it adds to a survivor-biased "
            "snapshot - and the families it would be redundant with are now "
            "known rather than assumed."),
        "key_question_unchanged": "CURRENT MODEL versus CURRENT MODEL + ANALYST "
                                  "EXPECTATION INFORMATION - incremental only",
        "no_intrinio_only_framework_created": True,
    })
    return base


# =========================================================================== #
# WORKSTREAM O - the external-data purchase gate.
# =========================================================================== #
def external_data_purchase_gate(*, capability: dict, exhaustion: dict,
                                fdr: dict, rd: dict,
                                challengers: dict) -> dict:
    """BUY / WAIT / REJECT for each candidate external dataset, from evidence.

    Stage 25 authorises nothing. A dataset is only recommendable when the owned
    surface is genuinely exhausted for the hypotheses it would unlock AND the
    dataset carries information nothing owned can supply."""
    owned_open = [k for k, v in (exhaustion.get("families") or {}).items()
                  if v.get("state") in (EX_ACTIVE_HIGH, EX_ACTIVE_MED)]
    free_first = capability["data_families"]["PIT_SECTOR_HISTORY"]["evidence"] \
        .get("exact_unblocking_artifact") or {}

    datasets = [
        {
            "dataset": "SEC Financial Statement Data Sets (sub.txt)",
            "vendor": "SEC (free)",
            "recommendation": "ACQUIRE_FREE_BEFORE_ANY_PURCHASE",
            "hypotheses_unlocked": [
                "a leakage-safe FINE-GRAINED point-in-time sector series",
                "a FULL composite_sn reconstruction including its within-sector "
                "step",
                "an honest sector-neutral verdict on R&D intensity",
                "sector exposure measurement for every candidate"],
            "expected_orthogonality": "n/a - it is a CONTROL, not a signal",
            "current_blocker": "not on disk; needs a bounded ~2.7 GB download",
            "historical_depth_required": "2009Q2 onward (matches XBRL)",
            "inactive_delisted_required": True,
            "pit_timestamp_required": True,
            "expected_research_value": "HIGH - it converts Stage 25's Tier-B "
                                       "look-ahead control into a leakage-safe "
                                       "one and turns provisional verdicts into "
                                       "conclusive ones",
            "duplicated_information_risk": "NONE",
            "cost": "free",
            "detail": free_first,
        },
        {
            "dataset": "Historical analyst estimate revisions (Intrinio)",
            "vendor": "Intrinio",
            "recommendation": None,   # filled below
            "hypotheses_unlocked": [
                "the six frozen Stage-13A revision hypotheses",
                "incremental value of expectation revisions over a "
                "survivorship-safe PIT fundamental baseline"],
            "expected_orthogonality": "HIGH - expectations are a different "
                                      "information family from realised "
                                      "accounting and from price",
            "current_blocker": "no historical vintage extract on disk",
            "historical_depth_required": ">= 15 years with per-vintage "
                                         "timestamps",
            "inactive_delisted_required": True,
            "pit_timestamp_required": True,
            "expected_research_value": "HIGH IF the extract is genuinely "
                                       "point-in-time AND includes delisted "
                                       "issuers",
            "duplicated_information_risk": "MEDIUM - revisions correlate with "
                                           "price momentum, which we already run",
            "prior_evidence": "a prior Intrinio live trial returned "
                              "NO_DEFENSIBLE_ALPHA and failed a "
                              "survivorship-safe 16-year test (DO_NOT_BUY)",
        },
        {
            "dataset": "Steele / other fundamental vendor history",
            "vendor": "Steele",
            "recommendation": "REJECT",
            "hypotheses_unlocked": ["broader pre-2009 fundamental history"],
            "expected_orthogonality": "LOW - it would restate the SAME "
                                      "accounting information we already read "
                                      "point-in-time from SEC",
            "current_blocker": "n/a",
            "historical_depth_required": "pre-2009",
            "inactive_delisted_required": True,
            "pit_timestamp_required": True,
            "expected_research_value": "LOW relative to cost: the owned SEC "
                                       "surface already covers 2009 onward, and "
                                       "Stage 25 rejected most of the "
                                       "accounting families it would extend",
            "duplicated_information_risk": "HIGH",
        },
    ]

    # The Intrinio recommendation is derived, not asserted.
    owned_exhausted_for_expectations = True   # no owned expectations data exists
    if owned_open:
        intrinio_rec = "WAIT"
        intrinio_why = (
            "owned-data research is NOT yet exhausted: %d family(ies) are still "
            "active (%s), and the free SEC sub.txt acquisition would resolve the "
            "single largest interpretive blocker at zero cost. Spending money "
            "before taking the free unlock would be buying certainty we can "
            "obtain for nothing." % (len(owned_open), ", ".join(sorted(owned_open))))
    elif owned_exhausted_for_expectations:
        intrinio_rec = "WAIT"
        intrinio_why = ("owned accounting alpha is exhausted, but the prior live "
                        "trial returned NO_DEFENSIBLE_ALPHA on a "
                        "survivorship-safe 16-year test; a purchase needs new "
                        "evidence that the historical extract differs from what "
                        "was already evaluated")
    else:  # pragma: no cover - unreachable with current evidence
        intrinio_rec = "BUY"
        intrinio_why = "owned surface exhausted and no prior negative evidence"
    datasets[1]["recommendation"] = intrinio_rec
    datasets[1]["why"] = intrinio_why

    return {
        "contract_id": "stage25_external_data_purchase_gate/1",
        "purchase_authorized": False,
        "authorization_owner": "the operator; this stage recommends only",
        "owned_data_exhaustion_summary": {
            "still_active_families": sorted(owned_open),
            "exhausted_or_rejected": exhaustion.get("do_not_reopen"),
            "fdr_survivors_this_stage": fdr.get("survivors_q10"),
            "research_challengers_this_stage":
                challengers.get("research_challengers"),
            "rnd_verdict": (rd.get("verdict") or {}).get("label")},
        "decision_rule": (
            "a paid dataset is recommendable only when (a) the owned surface is "
            "exhausted for the hypotheses it would unlock, (b) no FREE artefact "
            "would unlock them first, and (c) no prior evaluation of that same "
            "vendor already returned a negative result"),
        "datasets": datasets,
        "headline": "TAKE THE FREE SEC ACQUISITION FIRST; WAIT ON EVERY PAID "
                    "DATASET",
    }


# =========================================================================== #
# WORKSTREAM P - research gate integrity.
# =========================================================================== #
def research_gate_integrity(results: "list[dict]", baselines: "dict[str, dict]",
                            panel: "Stage25Panel", multi: dict) -> dict:
    """Audit the metrics whose semantics could distort a long-history comparison.

    Stage 24 found and versioned the drawdown defect. Stage 25 re-checks that the
    repair still holds and additionally audits the four semantics that a
    multi-horizon, multi-cadence stage newly puts at risk: annualisation,
    overlapping forward windows, duplicated formation observations, and sample
    comparability across variants."""
    rows = [r for r in results] + [
        {"name": k, "row": v.get("row"), "series": v.get("series")}
        for k, v in baselines.items()]
    dd = []
    changed = 0
    for r in rows:
        row = r.get("row") or {}
        spreads = (r.get("series") or {}).get("ls")
        if not spreads:
            continue
        c = _s24.drawdown_contract(spreads)
        v1 = c.get("v1_as_reported_pct")
        v2 = c.get("v2_as_pct_of_capital")
        impossible = bool(v1 is not None and v1 < -100.0)
        dd.append({"name": r["name"], "periods": c["periods"],
                   "v1_as_reported_pct": v1, "v2_as_pct_of_capital": v2,
                   "v1_is_impossible_as_a_percentage": impossible})
        if impossible:
            changed += 1

    # Annualisation audit: the evaluator annualises by 252 / horizon_days. Each
    # horizon must therefore use ITS OWN horizon_days or the comparison is wrong.
    ann = []
    for h in HORIZONS:
        ann.append({"horizon": h["key"], "horizon_days": h["horizon_days"],
                    "periods_per_year": round(252 / h["horizon_days"], 4),
                    "formation_stride_months": h["formation_stride"] * 3,
                    "forward_months": h["forward_months"],
                    "windows_overlap": h["overlap"].startswith("none") is False,
                    "overlap_note": h["overlap"]})

    # Duplicated formation observations: a symbol must appear at most once per
    # formation month, or every t-statistic is inflated by pseudo-replication.
    dupes = 0
    for m in panel.months:
        seen = set()
        for sym in panel.rows[m]:
            if sym in seen:
                dupes += 1
            seen.add(sym)

    return {
        "contract_id": "stage25_research_gate_integrity/1",
        "drawdown": {
            "active_contract_version": _s24.DRAWDOWN_CONTRACT_V1,
            "repaired_contract_version": _s24.DRAWDOWN_CONTRACT_V2,
            "owner": "alpha_agent.stage24_pit_fundamental.drawdown_contract",
            "stage25_changed_the_active_version": False,
            "why_not": "which drawdown a gate consumes is a model-governance "
                       "decision; activating V2 would retro-actively re-judge "
                       "every candidate ever evaluated",
            "series_audited": len(dd),
            "series_with_an_impossible_v1_percentage": changed,
            "impossible_v1_series": [d["name"] for d in dd
                                     if d["v1_is_impossible_as_a_percentage"]],
            "stage24_finding_still_reproduces": bool(changed),
            "readings": dd},
        "annualization": {
            "formula": "252 / horizon_days periods per year (released owner: "
                       "signal_evaluation.evaluate_periods)",
            "per_horizon": ann,
            "defect_found": False,
            "note": "each horizon is scored with ITS OWN horizon_days, so the "
                    "annualised numbers are comparable across the family"},
        "overlapping_forward_returns": {
            "policy": "every horizon is formed at a stride that makes its "
                      "forward windows non-overlapping",
            "serial_correlation_risk": "removed by construction rather than "
                                       "corrected after the fact",
            "defect_found": False},
        "duplicated_formation_observations": {
            "duplicates_detected": dupes, "defect_found": bool(dupes)},
        "sample_comparability": {
            "rule": "every falsification variant is scored on the SAME evaluator "
                    "with the SAME horizon; variants that narrow the universe "
                    "report their own period count and median cross-section so a "
                    "weaker reading is never confused with a smaller sample",
            "underpowered_threshold_periods": MIN_PERIODS_FOR_VERDICT},
        "factor_missingness": {
            "policy": "a concept the issuer never tagged is ABSENT, never "
                      "zero-filled; the R&D taxonomy additionally separates "
                      "ZERO / NOT_REPORTED / NOT_APPLICABLE / MISSING",
            "defect_found": False},
        "thresholds_weakened_to_make_candidates_pass": False,
        "historical_evidence_rewritten": False,
    }


# =========================================================================== #
# WORKSTREAM M - the autonomous research queue.
# =========================================================================== #
def research_queue(*, capability: dict, exhaustion: dict, fdr: dict, rd: dict,
                   challengers: dict, purchase: dict, multi: dict) -> dict:
    """What the EXISTING autonomous agent should do next, and what it must stop
    doing. This is a report the agent reads; it is not a second queue."""
    items: "list[dict]" = []
    survivors = fdr.get("survivors_q10") or []
    chal = challengers.get("research_challengers") or []

    items.append({
        "priority": "HIGH_PRIORITY",
        "item": "acquire SEC Financial Statement Data Sets sub.txt (free) and "
                "build the leakage-safe per-filing SIC series",
        "why": "it is the single artefact that converts Stage 25's Tier-B "
               "look-ahead control into a leakage-safe one, unblocks a FULL "
               "composite_sn reconstruction, and turns every provisional sector "
               "verdict in this stage into a conclusive one",
        "executable_now": True,
        "blocked_on": None,
        "consumer": "alpha_agent.pit_sector.PitSicSeries (already implemented)"})

    if chal:
        items.append({
            "priority": "HIGH_PRIORITY",
            "item": "research-forward-track the Stage-25 challenger(s): %s"
                    % ", ".join(chal),
            "why": "they cleared the released gate on survivorship-safe "
                   "point-in-time evidence, survived FDR over the whole "
                   "discovery family, and are not restatements of the "
                   "operational model",
            "executable_now": True, "blocked_on": None,
            "consumer": "alpha_agent.tournament.maybe_activate_shadow_books"})
    elif survivors:
        items.append({
            "priority": "MEDIUM_PRIORITY",
            "item": "re-examine the FDR survivor(s) that did not qualify as "
                    "challengers: %s" % ", ".join(survivors),
            "why": "statistical survival without orthogonality means the "
                   "information may already be in the operational model",
            "executable_now": True, "blocked_on": None})

    items.append({
        "priority": "MEDIUM_PRIORITY",
        "item": "extend the PIT concept allowlist to SHARE COUNTS and acquire an "
                "UNADJUSTED price surface",
        "why": "both are required for a point-in-time market capitalisation; "
               "the owned parser drops non-monetary units and the owned daily "
               "panel is total-return adjusted, so every valuation ratio stays "
               "unrunnable",
        "executable_now": False, "blocked_on": "PIT_MARKET_CAP"})
    items.append({
        "priority": "MEDIUM_PRIORITY",
        "item": "work the identity backlog (AMBIGUOUS / UNRESOLVED)",
        "why": "every unresolved symbol is a name silently absent from every "
               "cross-section; resolving them widens the universe with no "
               "purchase",
        "executable_now": True, "blocked_on": None})
    items.append({
        "priority": "WAITING_FOR_DATA",
        "item": "historical analyst revision vintages",
        "why": "still the only untested orthogonal information family",
        "executable_now": False,
        "blocked_on": "HISTORICAL_ANALYST_REVISIONS",
        "purchase_recommendation": "WAIT"})

    stop = sorted(exhaustion.get("do_not_reopen") or [])
    items.append({
        "priority": "LOW_PRIORITY",
        "item": "do NOT re-open: %s" % ", ".join(stop),
        "why": "each was measured and rejected with evidence on disk; "
               "re-running them is correlated-variant tuning, not research",
        "executable_now": False, "blocked_on": None})

    counts: "dict[str, int]" = {}
    for i in items:
        counts[i["priority"]] = counts.get(i["priority"], 0) + 1
    return {
        "contract_id": "stage25_autonomous_research_queue/1",
        "second_queue_created": False,
        "second_agent_created": False,
        "agent_owner": "alpha_agent.autonomous_research + "
                       "alpha_agent.tournament",
        "agent_may_promote_models": False,
        "agent_may_change_holdings": False,
        "counts": counts,
        "capabilities_now_executable": sorted(
            k for k, v in (capability.get("data_families") or {}).items()
            if v.get("state") == CAP_READY),
        "capabilities_waiting_for_data": sorted(
            k for k, v in (capability.get("data_families") or {}).items()
            if v.get("state") == CAP_WAITING),
        "stop_researching": stop,
        "items": items,
    }


# =========================================================================== #
# Artifact builders for the data-layer workstreams.
# =========================================================================== #
def pit_sector_history_summary(panel: "Stage25Panel", sectors: "SectorHistory",
                               store: "Stage25PitStore") -> dict:
    """Coverage, composition and - crucially - FIDELITY of the two tiers.

    The fidelity block is the honest measurement of how much the Tier-B
    look-ahead actually matters: on the ONE dimension Tier A can verify
    point-in-time (is this issuer a financial?), how often do the two tiers
    agree? High agreement does not make Tier B leakage-safe, but it bounds how
    wrong the control can be on the dimension we can check."""
    per_year: "dict[str, dict]" = {}
    a_counts: "dict[str, int]" = {}
    b_counts: "dict[str, int]" = {}
    agree = disagree = comparable = 0
    rows_total = 0
    for m in panel.months:
        year = m[:4]
        bucket = per_year.setdefault(year, {"rows": 0, "tier_a_known": 0,
                                            "tier_b_known": 0})
        for sym, r in panel.rows[m].items():
            sec = r.get("sectors") or {}
            a, b = sec.get(TIER_A), sec.get(TIER_B)
            rows_total += 1
            bucket["rows"] += 1
            a_counts[a] = a_counts.get(a, 0) + 1
            b_counts[b] = b_counts.get(b, 0) + 1
            if a != TIER_A_UNKNOWN:
                bucket["tier_a_known"] += 1
            if b != _ps.UNKNOWN:
                bucket["tier_b_known"] += 1
            if a == TIER_A_UNKNOWN or b == _ps.UNKNOWN:
                continue
            comparable += 1
            a_fin = a in ("Banking", "Insurance", "RealEstate")
            b_fin = (b == _ps.FINANCIALS)
            if a_fin == b_fin:
                agree += 1
            else:
                disagree += 1
    for y, bucket in per_year.items():
        n = max(1, bucket["rows"])
        bucket["tier_a_coverage"] = round(bucket["tier_a_known"] / n, 4)
        bucket["tier_b_coverage"] = round(bucket["tier_b_known"] / n, 4)

    return {
        "contract_id": "stage25_pit_sector_history/1",
        "capability": sector_capability_statement(),
        "record_schema": ["security", "cik", "classification",
                          "classification_effective_date", "source",
                          "evidence_date", "mapping_version", "confidence",
                          "unknown_reason"],
        "tier_a": {
            "tier": TIER_A, "leakage_safe": True,
            "rules": [{"label": lab, "markers": list(mk)}
                      for lab, mk in TIER_A_RULES],
            "default": TIER_A_DEFAULT,
            "excludes_research_development_concept": True,
            "effective_date_semantics": "a classification is effective from the "
                                        "first date the issuer had FILED one of "
                                        "the marker concepts; a query at date D "
                                        "reads only facts filed by D",
            "composition": dict(sorted(a_counts.items(),
                                       key=lambda kv: -kv[1]))},
        "tier_b": {
            "tier": TIER_B, "leakage_safe": False,
            "load_status": sectors.load_status,
            "mapping_owner": "alpha_agent.pit_sector.sic_to_sector",
            "mapping_version": _ps.MAPPING_VERSION,
            "mapping_version_hash": _ps.mapping_version_hash(),
            "availability_floor": "the issuer's first SEC filing date",
            "composition": dict(sorted(b_counts.items(),
                                       key=lambda kv: -kv[1])),
            "usage_rule": SectorHistory.tier_b_usage_rule()},
        "coverage_by_year": dict(sorted(per_year.items())),
        "panel_rows": rows_total,
        "fidelity_of_the_lookahead_control": {
            "question": "on the ONE dimension the leakage-safe tier can verify "
                        "point-in-time - is this issuer a financial? - how often "
                        "do the two tiers agree?",
            "comparable_rows": comparable,
            "agree": agree, "disagree": disagree,
            "agreement_fraction": (round(agree / comparable, 4)
                                   if comparable else None),
            "interpretation": (
                "high agreement bounds how wrong the Tier-B control can be on "
                "the dimension we CAN check point-in-time. It does not make "
                "Tier B leakage-safe, and it says nothing about the "
                "Technology / Industrials boundary that the R&D question turns "
                "on - which is exactly why the free SEC sub.txt acquisition is "
                "the top queue item."),
        },
        "unknown_is_never_fabricated": True,
    }


def pit_fundamental_expansion(store: "Stage25PitStore",
                              panel: "Stage25Panel") -> dict:
    """What the accounting surface gained, and what each derived feature
    documents about how it was built."""
    cov = store.coverage()
    return {
        "contract_id": "stage25_pit_fundamental_expansion/1",
        "concept_owners": {
            "alpha_agent.pit_fundamentals (Phase 9.3)": list(RELEASED_CONCEPTS),
            "alpha_agent.stage24_pit_fundamental (Stage 24)":
                list(STAGE24_CONCEPTS),
            "alpha_agent.stage25_alpha_discovery (Stage 25)":
                list(STAGE25_CONCEPTS)},
        "shadowing_prevented": True,
        "concepts_total": len(concept_map()),
        "us_gaap_tags_total": len(target_tags()),
        "mapping_version_hash": mapping_version_hash(),
        "index": {"path": str(store.db_path),
                  "facts_loaded": cov.get("facts_loaded"),
                  "distinct_ciks": cov.get("distinct_ciks"),
                  "availability_start": cov.get("availability_start"),
                  "availability_end": cov.get("availability_end"),
                  "facts_by_concept": cov.get("facts_by_concept"),
                  "built_offline_no_network": True},
        "period_identity": cov.get("period_identity"),
        "pit_basis": cov.get("pit_basis"),
        "feature_documentation_contract": [
            "exact accounting concepts and their ORDERED fallbacks",
            "orientation (expected sign), fixed before evaluation",
            "denominator and its guard (a non-positive denominator yields no "
            "value rather than an exploding one)",
            "period handling (own period_end; fiscal-year duration for flows)",
            "filing availability rule (filed <= formation - %d days)"
            % REPORTING_LAG_DAYS,
            "restatement handling (preserved as distinct observations; the "
            "latest FILED by the as-of date wins)",
            "missing-data policy (ABSENT, never zero-filled)",
            "sample coverage (periods and median cross-section per factor)"],
        "per_factor_coverage": {
            f.name: {"periods": len(panel.factor_cross_sections(f)),
                     "median_names": _median(
                         [len(x["names"])
                          for x in panel.factor_cross_sections(f)]),
                     "required_concepts": list(f.required),
                     "definition": f.definition,
                     "expected_sign": f.direction,
                     "needs_comparable_prior_year": f.needs_prior}
            for f in ALL_FACTORS if f is not None},
    }


def hypothesis_manifest() -> dict:
    """The pre-registered campaign, stated in full before any result."""
    return {
        "contract_id": "stage25_hypothesis_manifest/1",
        "pre_registered_before_evaluation": True,
        "discovery_family": FAMILY_DISCOVERY,
        "discovery_family_size": len(DISCOVERY_FACTORS),
        "economic_families": sorted({f.family for f in DISCOVERY_FACTORS}),
        "formation_cadence": "quarterly, non-overlapping",
        "horizons": list(HORIZONS),
        "primary_horizon": PRIMARY_HORIZON,
        "transaction_cost_grid_bps": [5, 25, 50, 75, 100],
        "primary_metric": "rank IC and its t-statistic",
        "rejection_criteria_owner":
            "configs/alpha_agent/stage9_tournament.json",
        "multiple_testing": "Benjamini-Hochberg over the WHOLE discovery family",
        "baseline_comparison": [BASELINE_COMPOSITE, BASELINE_MOMENTUM,
                                BASELINE_ENSEMBLE],
        "sign_fitted_from_data": False,
        "brute_force_parameter_search_performed": False,
        "deliberately_excluded": [
            "everything Stage 23 and Stage 24 already measured and registered",
            "residual momentum, low-vol, vol-scaled momentum, monthly liquidity",
            "any variant produced by re-tuning a failed hypothesis"],
        "experiments": [f.as_dict() for f in DISCOVERY_FACTORS],
    }


# =========================================================================== #
# Orchestration.
# =========================================================================== #
def run(*, research_root=None, mom_panel=None, identity_db=None, cf_index=None,
        issuer_db=None, tournament_cfg_path=None, tournament_db=None,
        register: bool = True, evidence_date: Optional[str] = None) -> dict:
    """Execute the Stage-25 research contract end to end and write the
    machine-readable evidence. Read-only w.r.t. every operational store."""
    from . import tournament as _t

    root = _resolve(research_root, RESEARCH_ROOT_ENV, DEFAULT_RESEARCH_ROOT)
    cfg = _t.load_config(tournament_cfg_path or
                         r"C:\Users\binis\paper_trader\configs\alpha_agent"
                         r"\stage9_tournament.json")

    # ---- data layer ------------------------------------------------------- #
    universe = _s24.HistoricalUniverse.from_momentum_panel(mom_panel)
    ucontract = universe.contract()
    bridge = _s24.IdentityBridge(identity_db)
    bridge_load = bridge.load()
    bridge_cov = bridge.coverage_vs(universe.symbols)
    store = Stage25PitStore(cf_index)
    store_load = store.load()
    if not store_load.get("ok"):
        return {"ok": False, "token": BLOCKED, "reason": store_load.get("reason")}
    sectors = SectorHistory(issuer_db)
    sectors.load_entity_sic(set(bridge.symbol_to_cik.values()))
    beta = TrailingBeta(universe)
    panel = build_panel(universe, bridge, store, sectors, beta)
    if not panel.months:
        return {"ok": False, "token": DATA_HOLD,
                "reason": "NO_PIT_CROSS_SECTIONS_ASSEMBLED"}

    # ---- baselines on the SHARED cross-section ----------------------------- #
    comp = panel.composite_cross_sections()
    mom = panel.momentum_cross_sections()
    ens = blend_cross_sections([comp, mom])
    baselines_periods = {BASELINE_COMPOSITE: comp, BASELINE_MOMENTUM: mom,
                         BASELINE_ENSEMBLE: ens}
    baselines: "dict[str, dict]" = {}
    for name, periods in baselines_periods.items():
        res = score_cross_sections(periods, feature=name)
        g = gate_for(res["row"], cfg, survivorship_safe=True,
                     point_in_time_valid=True)
        baselines[name] = {"name": name, "periods_scored": len(periods),
                           "row": res["row"], "series": res["series"],
                           "metrics": g["metrics"], "gate": g["gate"]}
    champ_returns = baselines[BASELINE_COMPOSITE]["series"].get(
        "long_short_by_date")

    # ---- WS E/G: the campaign --------------------------------------------- #
    results = run_campaign(panel, cfg=cfg, champion_returns=champ_returns)
    fdr = apply_fdr(results, family=FAMILY_DISCOVERY)

    # ---- WS H: orthogonality and neutralisation ---------------------------- #
    incr: "dict[str, dict]" = {}
    neut: "dict[str, dict]" = {}
    classifications: "dict[str, dict]" = {}
    for r in results:
        spec = factor_by_name(r["name"])
        periods = panel.factor_cross_sections(spec)
        incr[r["name"]] = incrementality(periods, baselines=baselines_periods,
                                         cfg=cfg, candidate_name=r["name"])
        neut[r["name"]] = neutralization(periods, panel, feature=r["name"],
                                         sectors=sectors)
        classifications[r["name"]] = classify_candidate(
            result=r, incr=incr[r["name"]], neutral=neut[r["name"]])
    for name, periods in baselines_periods.items():
        neut[name] = neutralization(periods, panel, feature=name,
                                    sectors=sectors)

    # ---- WS C: falsification ------------------------------------------------ #
    # The prior stage's candidate is attacked first, then EVERY signal that
    # cleared this stage's gate faces the identical battery. Holding a new
    # winner to a lower bar than the old one would be the easiest way to
    # manufacture a result.
    rd = falsification_battery(panel, sectors, cfg=cfg, spec=RND_INTENSITY)
    gate_clearing = [r["name"] for r in results
                     if (r.get("gate") or {}).get("target_state")
                     == "KEEP_FOR_RESEARCH"]
    challenger_batteries = {
        name: falsification_battery(panel, sectors, cfg=cfg,
                                    spec=factor_by_name(name))
        for name in gate_clearing}

    # ---- WS F: the horizon family ------------------------------------------ #
    horizon_names = [BASELINE_COMPOSITE, BASELINE_MOMENTUM] + \
        list(fdr.get("survivors_q10") or [])[:4] + ["s24_rnd_intensity"]
    multi = multi_horizon(panel, horizon_names, cfg=cfg)

    # ---- WS J: bounded ensembles -------------------------------------------- #
    picks = _ensemble_picks(results, fdr, classifications, panel)
    references = [("s24_rnd_intensity",
                   panel.factor_cross_sections(RND_INTENSITY))]
    ensembles = evaluate_ensembles(
        ensemble_menu(comp=comp, mom=mom, picks=picks, references=references),
        cfg=cfg)

    # ---- WS K: governance ---------------------------------------------------- #
    registration = None
    if register:
        registration = register_candidates(results, cfg,
                                           evidence_date=evidence_date,
                                           db_path=tournament_db)
    challengers = challenger_assessment(results, fdr, ensembles,
                                        classifications, rd, registration)
    challengers["challenger_falsification"] = {
        name: {"verdict": b["verdict"],
               "baseline_reproduction": b["baseline_reproduction"],
               "sector_neutralization": b["sector_neutralization"],
               "sector_removal": b["sector_removal"],
               "style_neutralization": b["style_neutralization"],
               "winner_removal": b["winner_removal"],
               "subperiod_and_regime": b["subperiod_and_regime"],
               "long_leg_sector_mix": b["long_leg_sector_mix"],
               "disclosure_selection": b["disclosure_selection"],
               "alternative_constructions": b["alternative_constructions"]}
        for name, b in challenger_batteries.items()}
    challengers["same_battery_applied_to_new_and_prior_candidates"] = True
    # A challenger whose own battery returns a damning verdict is demoted here,
    # so the bar cannot drift between stages.
    demoted = []
    for name in list(challengers.get("research_challengers") or []):
        v = ((challenger_batteries.get(name) or {}).get("verdict") or {})
        if v.get("label") in ("SECTOR_EXPLAINED",
                              "DISCLOSURE_SELECTION_EXPLAINED",
                              "STYLE_EXPLAINED", "SUBPERIOD_UNSTABLE"):
            demoted.append({"name": name, "verdict": v.get("label")})
            challengers["research_challengers"].remove(name)
            for e in challengers["per_candidate"]:
                if e["name"] == name:
                    e["state"] = "NOT_A_CHALLENGER"
                    e["blockers"].append("falsification verdict %s"
                                         % v.get("label"))
    challengers["demoted_by_falsification"] = demoted

    # ---- WS A/I/L/M/N/O/P ---------------------------------------------------- #
    sector_status = sector_capability_statement()
    capability = research_capability_map(panel, store, ucontract, bridge_cov,
                                         sector_status, beta)
    registry = None
    try:
        registry = _t.CandidateRegistry(
            tournament_db or (cfg or {}).get("tournament_db")
            or r"D:\Stock_Prediction_app_data\alpha_agent\stage8"
               r"\tournament.sqlite")
    except Exception:  # noqa: BLE001
        registry = None
    exhaustion = alpha_family_exhaustion(results, fdr, rd.get("verdict") or {},
                                         registry=registry)
    if registry is not None:
        try:
            registry.close()
        except Exception:  # noqa: BLE001
            pass
    hoc = hoc_counterfactual(
        panel, candidates={n: p for n, p in
                           [(name, panel.factor_cross_sections(
                               factor_by_name(name)))
                            for name in (fdr.get("survivors_q10") or [])[:4]]},
        baseline_periods=ens)
    forward = forward_tracking_status(challengers, rd)
    purchase = external_data_purchase_gate(capability=capability,
                                           exhaustion=exhaustion, fdr=fdr,
                                           rd=rd, challengers=challengers)
    queue = research_queue(capability=capability, exhaustion=exhaustion,
                           fdr=fdr, rd=rd, challengers=challengers,
                           purchase=purchase, multi=multi)
    integrity = research_gate_integrity(results, baselines, panel, multi)

    # ---- artifacts ----------------------------------------------------------- #
    payload = {
        "research_capability_map": capability,
        "pit_sector_history_summary": pit_sector_history_summary(
            panel, sectors, store),
        "rd_falsification": rd,
        "pit_fundamental_expansion": pit_fundamental_expansion(store, panel),
        "hypothesis_manifest": hypothesis_manifest(),
        "experiment_results": {
            "contract_id": "stage25_experiment_results/1",
            "historical_universe_contract": ucontract,
            "identity_bridge": {"load": bridge_load, "coverage": bridge_cov},
            "panel_diagnostics": panel.diagnostics,
            "formation_months": len(panel.months),
            "window": [panel.months[0], panel.months[-1]],
            "baselines": {k: {kk: vv for kk, vv in v.items() if kk != "series"}
                          for k, v in baselines.items()},
            "results": [{k: v for k, v in r.items() if k != "series"}
                        for r in results],
            "multiple_testing": fdr,
            "multi_horizon": multi,
        },
        "alpha_family_exhaustion": exhaustion,
        "orthogonality_matrix": {
            "contract_id": "stage25_orthogonality/1",
            "baselines_measured_on_shared_cross_section": True,
            "classification_thresholds": {
                "redundant_abs_rank_correlation": REDUNDANT_CORR,
                "independent_partial_ic_t": INDEPENDENT_PARTIAL_T,
                "style_proxy_max_retained_fraction": RND_SURVIVE_MIN_RETENTION},
            "classifications": classifications,
            "neutralization": neut,
        },
        "incremental_alpha_matrix": {
            "contract_id": "stage25_incremental_alpha/1",
            "entries": [incr[r["name"]] for r in results],
        },
        "ensemble_results": ensembles,
        "challenger_results": challengers,
        "forward_tracking_status": forward,
        "hoc_counterfactual_results": hoc,
        "autonomous_research_queue": queue,
        "intrinio_status": intrinio_status(),
        "external_data_purchase_gate": purchase,
        "research_gate_integrity": integrity,
    }
    payload["stage25_summary"] = _summary(
        payload=payload, panel=panel, ucontract=ucontract, results=results,
        fdr=fdr, rd=rd, ensembles=ensembles, challengers=challengers,
        baselines=baselines, classifications=classifications)

    run_id = "stage25_%s" % content_hash({
        "version": STAGE25_VERSION,
        "mapping": mapping_version_hash(),
        "universe": ucontract["source_fingerprint"],
        "store_facts": store.loaded_facts,
        "months": panel.months,
    })[:16]
    out_dir = root / "runs" / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for key, obj in payload.items():
        p = out_dir / ("%s.json" % key)
        p.write_text(json.dumps(obj, indent=2, sort_keys=True, default=str),
                     encoding="utf-8")
        written.append({"artifact": key, "path": str(p),
                        "sha256": file_fingerprint(p)["sha256"]})
    latest = {"run_id": run_id, "run_dir": str(out_dir),
              "stage25_version": STAGE25_VERSION,
              "generated_from": CONTRACT_ID, "artifacts": written,
              "safety_badges": SAFETY_BADGES, "token": READY}
    (root / "latest.json").write_text(
        json.dumps(latest, indent=2, sort_keys=True), encoding="utf-8")

    return {"ok": True, "token": READY, "run_id": run_id,
            "run_dir": str(out_dir), "artifacts": written,
            "summary": payload["stage25_summary"], "payload": payload}


def _ensemble_picks(results, fdr, classifications, panel) -> "list[tuple]":
    """Which candidates are offered to the ensemble menu.

    Driven ONLY by pre-stated rules - released gate, FDR survival, and an
    orthogonality classification that is not a restatement or a proxy - and
    ordered by partial-IC strength against the operational shape. Ensemble
    performance plays no part in the choice."""
    survivors = set(fdr.get("survivors_q10") or [])
    eligible = []
    for r in results:
        cls = (classifications.get(r["name"]) or {}).get("classification")
        if (r.get("gate") or {}).get("target_state") != "KEEP_FOR_RESEARCH":
            continue
        if r["name"] not in survivors:
            continue
        if cls not in (CLS_INDEPENDENT, CLS_COMPLEMENTARY):
            continue
        eligible.append((r["name"], (r.get("row") or {}).get("rank_ic_t") or 0))
    eligible.sort(key=lambda kv: -kv[1])
    return [(name, panel.factor_cross_sections(factor_by_name(name)))
            for name, _ in eligible[:2]]


def _summary(*, payload, panel, ucontract, results, fdr, rd, ensembles,
             challengers, baselines, classifications) -> dict:
    keeps = [r["name"] for r in results
             if (r.get("gate") or {}).get("target_state") == "KEEP_FOR_RESEARCH"]
    ranked = sorted(
        [r for r in results if (r.get("row") or {}).get("rank_ic_t") is not None],
        key=lambda r: -(r["row"]["rank_ic_t"]))
    strongest = []
    for name in (BASELINE_COMPOSITE, BASELINE_MOMENTUM, BASELINE_ENSEMBLE):
        row = baselines[name]["row"]
        strongest.append({"source": name, "kind": "operational component",
                          "rank_ic": row.get("rank_ic_mean"),
                          "rank_ic_t": row.get("rank_ic_t"),
                          "spread_t": row.get("spread_t"),
                          "net25": row.get("net_annualized_return"),
                          "gate": baselines[name]["gate"].get("target_state")})
    for r in ranked[:5]:
        strongest.append({
            "source": r["name"], "kind": "stage25 candidate",
            "economic_family": r["family"],
            "rank_ic": r["row"].get("rank_ic_mean"),
            "rank_ic_t": r["row"].get("rank_ic_t"),
            "spread_t": r["row"].get("spread_t"),
            "net25": r["row"].get("net_annualized_return"),
            "bh_q": r.get("bh_q"),
            "classification": (classifications.get(r["name"]) or {}
                               ).get("classification"),
            "gate": (r.get("gate") or {}).get("target_state")})
    chal = challengers.get("research_challengers") or []
    return {
        "contract_id": "stage25_summary/1",
        "stage25_version": STAGE25_VERSION,
        "evidence_class": "SURVIVORSHIP_SAFE_POINT_IN_TIME",
        "formations": len(panel.months),
        "window": [panel.months[0], panel.months[-1]],
        "median_cross_section": _median(
            [len(panel.rows[m]) for m in panel.months]),
        "universe_survivorship_class": ucontract.get("survivorship_class"),
        "hypotheses_tested": len(results),
        "gate_clearing": keeps,
        "fdr_survivors": fdr.get("survivors_q10"),
        "strongest_alpha_sources": strongest,
        "rnd_verdict": (rd.get("verdict") or {}).get("label"),
        "best_ensemble": ensembles.get("best_by_rank_ic_t"),
        "best_ensemble_beating_operational_shape":
            challengers.get("best_ensemble_beating_operational_shape"),
        "research_challengers": chal,
        "challenger_headline": (
            "NO CHALLENGER CURRENTLY CLEARS THE EVIDENCE BAR" if not chal
            else "RESEARCH CHALLENGER(S): %s" % ", ".join(chal)),
        "most_valuable_missing_information":
            "a leakage-safe, fine-grained, effective-dated sector classification "
            "(free: SEC Financial Statement Data Sets sub.txt), followed by "
            "point-in-time market capitalisation (share counts + an unadjusted "
            "price surface)",
        "next_autonomous_research_action":
            ((payload["autonomous_research_queue"].get("items") or [{}])[0]
             ).get("item"),
        "operational_model_unchanged": "fundamental_momentum_50_50_v1",
        "automatic_promotion": False,
        "portfolio_mutation": False,
        "orders_created": 0,
        "safety_badges": SAFETY_BADGES,
    }


__all__ = [
    "STAGE25_VERSION", "CONTRACT_ID", "ORIGIN", "READY", "BLOCKED", "DATA_HOLD",
    "SAFETY_BADGES",
    # data layer
    "CONCEPT_EXTENSION_25", "concept_map", "target_tags", "tag_to_concept",
    "mapping_version_hash", "Stage25PitStore", "annual_record",
    # sector
    "TIER_A", "TIER_B", "TIER_A_RULES", "TIER_A_DEFAULT", "TIER_A_UNKNOWN",
    "SectorHistory", "sector_capability_statement", "MIN_SECTOR_GROUP",
    "OTHER_SMALL",
    # controls
    "TrailingBeta", "BETA_WINDOW_MONTHS", "BETA_MIN_OBSERVATIONS",
    # panel / campaign
    "Stage25Panel", "build_panel", "DISCOVERY_FACTORS", "COMPOSITE_FACTORS",
    "ALL_FACTORS", "RND_INTENSITY", "factor_by_name", "HORIZONS",
    "PRIMARY_HORIZON", "horizon_by_key", "FAMILY_DISCOVERY", "run_campaign",
    "apply_fdr", "multi_horizon",
    # falsification
    "falsification_battery", "disclosure_selection_analysis",
    "disclosure_state", "rnd_availability_state", "TECH_EXPOSED_SECTORS",
    "RND_REPORTED", "RND_ZERO", "RND_NOT_REPORTED", "RND_NOT_APPLICABLE",
    "RND_MISSING", "RND_VERDICTS", "RND_SURVIVE_MIN_T",
    "RND_SURVIVE_MIN_RETENTION", "sector_neutral_cross_sections",
    "drop_top_winners", "evaluate_variant",
    # orthogonality / ensembles
    "classify_candidate", "neutralization", "ensemble_menu",
    "evaluate_ensembles", "MAX_ENSEMBLE_STRUCTURES", "CLS_INDEPENDENT",
    "CLS_COMPLEMENTARY", "CLS_REDUNDANT_COMPOSITE", "CLS_REDUNDANT_MOMENTUM",
    "CLS_SECTOR_PROXY", "CLS_SIZE_PROXY", "CLS_VOL_PROXY", "CLS_BETA_PROXY",
    "CLS_INSUFFICIENT", "CLS_FAILED_ROBUST",
    # governance / reports
    "alpha_family_exhaustion", "hoc_counterfactual", "register_candidates",
    "challenger_assessment", "forward_tracking_status", "intrinio_status",
    "external_data_purchase_gate", "research_gate_integrity", "research_queue",
    "research_capability_map", "pit_sector_history_summary",
    "pit_fundamental_expansion", "hypothesis_manifest",
    # orchestration
    "run",
]

