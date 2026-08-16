"""alpha_agent/stage26_challenger_expansion.py — Stage 26 challenger launch and
new-information expansion.

Stage 25 ended with two lanes open and one closed. The closed one was historical
fundamental discovery: five of six economic families are exhausted with evidence
and re-running them would be theatre. The two open ones are the only honest ways
forward, and this stage advances both **in parallel**:

**Lane A — start genuine forward competition.** `s25_operating_profitability`
cleared the released gate, survived Benjamini-Hochberg over the whole
28-hypothesis family and faced the same falsification battery R&D failed. What it
has never had is an out-of-sample day. Historical validation cannot be repeated
into forward validation; only calendar time produces that. So the specification
is frozen — inputs, transform, sign, universe, cadence, cost, data fingerprints —
**before** any future outcome is observable, and the canonical shadow-book
lifecycle is started against the frozen spec.

**Lane B — expand the information frontier.** Two capabilities Stage 25 named as
blocked are unblocked here with free and owned data:

* **Point-in-time fine-grained sector.** Stage 25's Tier B carried a
  classification look-ahead, so every sector verdict it produced was stamped
  provisional. The free SEC Financial Statement Data Sets carry, per submission,
  both the assigned SIC and the acceptance timestamp. Tier C is built from them
  and is leakage-safe, which turns the provisional verdicts conclusive.
* **Point-in-time market equity.** Blocked by two independent gaps (share counts
  dropped by a monetary-unit filter; the only owned price surface TOTALRETURN
  adjusted). Both are closed with owned data, which opens **valuation** — the one
  genuinely new economic family the owned surface still supported.

What this stage deliberately does NOT do: re-prove Stage 25, re-specify R&D
intensity to rescue it, tune a threshold after seeing a number, promote a model,
or touch anything operational. There is no automatic promotion and none is
proposed.
"""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
from typing import Iterable, Optional, Sequence

from . import pit_market_equity as _pme
from . import pit_sector as _ps
from . import sec_financial_statement_sets as _fsds
from . import stage24_pit_fundamental as _s24
from . import stage25_alpha_discovery as _s25

STAGE26_VERSION = "stage26-challenger-expansion-1.0.0"
ORIGIN = "stage26-alpha-challenger-expansion"
CONTRACT_ID = "stage26_challenger_expansion/1"

READY = "STAGE26_CHALLENGER_EXPANSION_READY"
BLOCKED = "STAGE26_CHALLENGER_EXPANSION_BLOCKED"
DATA_HOLD = "STAGE26_CHALLENGER_EXPANSION_DATA_HOLD"

SAFETY_BADGES = ["RESEARCH ONLY", "READ ONLY", "NO ORDERS", "NO LIVE PROMOTION",
                 "PREVIEW ONLY", "MANUAL REVIEW"]

# --------------------------------------------------------------------------- #
# Owned data locations (env-overridable so tests stay hermetic).
# --------------------------------------------------------------------------- #
RESEARCH_ROOT_ENV = "PAPER_TRADER_STAGE26_ROOT"
SHARES_INDEX_ENV = "PAPER_TRADER_STAGE26_SHARES_INDEX"
PRICE_SURFACE_ENV = "PAPER_TRADER_STAGE26_PRICE_SURFACE"
FSDS_CACHE_ENV = "PAPER_TRADER_STAGE26_FSDS_CACHE"

DEFAULT_RESEARCH_ROOT = Path(
    r"D:\Stock_Prediction_app_data\stage26_alpha_challenger_expansion")
DEFAULT_SHARES_INDEX = DEFAULT_RESEARCH_ROOT / "_index" / \
    "sec_companyfacts_shares_stage26.sqlite"
DEFAULT_PRICE_SURFACE = DEFAULT_RESEARCH_ROOT / "_inputs" / \
    "price_surface_unadjusted.npz"
DEFAULT_FSDS_CACHE = Path(
    r"D:\Stock_Prediction_app_data\alpha_agent\identity\sec_bulk"
    r"\financial_statement_data_sets")

_resolve = _s25._resolve
canonical_json = _s24.canonical_json
content_hash = _s24.content_hash
file_fingerprint = _s24.file_fingerprint
_num = _s24._num
_ratio = _s24._ratio
_mean = _s24._mean
_t_stat = _s24._t_stat
_zscore = _s24._zscore
_shift_days = _s24._shift_days
score_cross_sections = _s24.score_cross_sections
gate_for = _s24.gate_for
blend_cross_sections = _s24.blend_cross_sections
incrementality = _s24.incrementality
evaluate_variant = _s25.evaluate_variant
MIN_CROSS_SECTION = _s24.MIN_CROSS_SECTION
WINSOR_FRACTION = _s24.WINSOR_FRACTION
REPORTING_LAG_DAYS = _s24.REPORTING_LAG_DAYS
PRIMARY_HORIZON = _s25.PRIMARY_HORIZON
FIRST_MONTH = _s25.FIRST_MONTH
FORMATION_EVERY_N_MONTHS = _s25.FORMATION_EVERY_N_MONTHS


# =========================================================================== #
# WORKSTREAM 3 - Tier C: leakage-safe, fine-grained, per-filing PIT sector.
#
# The distinction that matters is the timestamp, not the taxonomy. Tier B read
# TODAY's entity-level SIC and mapped it through the released taxonomy; the map
# was fine, the observation date was missing. Tier C reads the SIC that a
# SPECIFIC submission carried, paired with the acceptance timestamp at which that
# submission - and therefore that classification - became publicly observable.
# The taxonomy and the no-look-ahead query rule are UNCHANGED and stay owned by
# alpha_agent.pit_sector; only the observation source is new.
# =========================================================================== #
TIER_A = _s25.TIER_A
TIER_B = _s25.TIER_B
TIER_C = "PIT_FILING_SIC_SERIES"

TIER_C_CONTRACT = {
    "tier": TIER_C,
    "leakage_safe": True,
    "source": "SEC Financial Statement Data Sets, sub.txt (free, first-party)",
    "observation": "(cik, assigned SIC, SEC acceptance timestamp) per submission",
    "availability_rule": "sector_as_of(cik, D) uses only submissions ACCEPTED on "
                         "or before D; a later reclassification never leaks back",
    "taxonomy_owner": "alpha_agent.pit_sector (frozen SIC->sector map, unchanged)",
    "admissible_for": ["falsification control", "sector neutralisation",
                       "diagnostic reporting", "coverage measurement",
                       "conclusive (not provisional) sector verdicts"],
    "inadmissible_for": ["signal construction", "candidate registration"],
    "why_still_not_a_signal_input": (
        "a classification is a control variable in this research programme, not "
        "an alpha input; keeping it out of every registered spec is what makes "
        "the sector falsification non-circular, and that rule is unchanged"),
    "unknown_policy": "an issuer with no submission accepted on or before the "
                      "formation date is Unknown; never guessed, never "
                      "back-filled from a later filing",
}


class PitSicHistory:
    """Tier-C classification history assembled from acquired ``sub.txt`` members.

    Owns loading and coverage measurement only. The no-look-ahead query rule and
    the SIC->sector taxonomy stay with :class:`alpha_agent.pit_sector.PitSicSeries`,
    which this class wraps rather than re-implements.
    """

    def __init__(self, cache_root=None) -> None:
        self.cache_root = _resolve(cache_root, FSDS_CACHE_ENV,
                                   DEFAULT_FSDS_CACHE)
        self.series = None
        self.observations: int = 0
        self.load_status: dict = {}
        self._summary: dict = {}
        self._quarters: "list[str]" = []
        self._manifests: "list[dict]" = []

    def load(self, ciks: Optional[Iterable[str]] = None) -> dict:
        if not self.cache_root.exists():
            self.load_status = {"ok": False, "reason": "FSDS_CACHE_ABSENT",
                                "path": str(self.cache_root)}
            return self.load_status
        wanted = {str(int(str(c).lstrip("0") or "0")) for c in (ciks or []) if c} \
            or None
        obs: list = []
        quarters: list = []
        manifests: list = []
        for qdir in sorted(p for p in self.cache_root.iterdir() if p.is_dir()):
            member = qdir / _fsds.MEMBER_NAME
            manifest = qdir / "manifest.json"
            if not member.exists():
                continue
            quarters.append(qdir.name)
            if manifest.exists():
                try:
                    m = json.loads(manifest.read_text(encoding="utf-8"))
                    manifests.append({k: m.get(k) for k in (
                        "year", "quarter", "source", "member_sha256",
                        "member_bytes", "retrieved_at_utc",
                        "archive_last_modified")})
                except (OSError, ValueError):
                    pass
            rows = _fsds.parse_sub_txt(member.read_bytes())
            got = _fsds.sic_observations(rows, source=_fsds.source_identifier(
                *_split_quarter(qdir.name)), quarter=qdir.name)
            if wanted is not None:
                got = [o for o in got if o["cik"] in wanted]
            obs.extend(got)
        self.observations = len(obs)
        self._quarters = quarters
        self._manifests = manifests
        self._summary = _fsds.observation_summary(obs)
        self.series = _fsds.build_pit_sic_series(obs)
        self.load_status = {
            "ok": bool(obs), "path": str(self.cache_root),
            "quarters_loaded": len(quarters),
            "first_quarter": quarters[0] if quarters else None,
            "last_quarter": quarters[-1] if quarters else None,
            "observations": len(obs),
            "evidence_class": "LEAKAGE_SAFE_PIT_FILING_SIC",
            "mapping_version": _ps.MAPPING_VERSION,
            "mapping_version_hash": _ps.mapping_version_hash(),
            **self._summary,
        }
        return self.load_status

    def sector_as_of(self, cik: str, as_of: str) -> str:
        if self.series is None:
            return _ps.UNKNOWN
        # Query both the bare and zero-padded CIK: the series is indexed under
        # both, and the caller's convention varies across owned stores.
        s = self.series.sector_as_of(str(int(str(cik).lstrip("0") or "0")), as_of)
        if s == _ps.UNKNOWN:
            s = self.series.sector_as_of(str(cik).zfill(10), as_of)
        return s

    def acquisition_manifest(self) -> dict:
        return {
            "contract_id": "stage26_pit_sic_acquisition_manifest/1",
            "source_host": _fsds.SOURCE_HOST,
            "member": _fsds.MEMBER_NAME,
            "acquisition_contract": _fsds.CONTRACT_VERSION,
            "cache_root": str(self.cache_root),
            "quarters": self._quarters,
            "quarters_count": len(self._quarters),
            "per_quarter": self._manifests,
            "bandwidth_note": (
                "only the sub.txt member of each quarterly archive was fetched, "
                "over HTTP Range against the remote zip's central directory; the "
                "num.txt bulk this stage does not need was never transferred"),
            "license_note": "US federal government work; SEC EDGAR public data, "
                            "free for research use under SEC fair-access rules",
            "fair_access": "identifying User-Agent with a contact address; "
                           "inter-request delay; every member content-hashed and "
                           "cached so a re-run re-downloads nothing",
        }


def _split_quarter(name: str) -> "tuple[int, int]":
    y, q = name.lower().split("q", 1)
    return int(y), int(q)


# =========================================================================== #
# WORKSTREAM 6 - the PIT valuation family.
#
# These are the hypotheses that could not be asked until market equity existed.
# Every sign is fixed HERE, in source, before any Stage-26 number exists, and no
# sign is flipped afterwards; a strong wrong-signed result is a rejection, not a
# discovery. The set is deliberately bounded to economically distinct claims -
# not every ratio the concept map can spell.
#
# The denominator is always the PIT market equity of alpha_agent.pit_market_equity
# (shares FILED by the formation date, carried across capital events, times the
# UNADJUSTED close). Enterprise value adds net debt from the same annual record.
# =========================================================================== #
FAM_VALUATION = "pit_valuation_ratios"
FAM_VALUATION_SIZE = "pit_market_size"
FAMILY_VALUATION = "stage26_pit_valuation"

_S = _s24.FactorSpec
_gp = _s25._gp


def _me(rec) -> Optional[float]:
    return _num((rec.get("cur") or {}).get("market_equity"))


def _ev(rec) -> Optional[float]:
    """Enterprise value = market equity + long-term debt - cash.

    A name missing debt or cash is NOT silently treated as debt-free: it returns
    None. Zero-filling net debt would systematically flatter leveraged issuers,
    which is precisely the population an EV ratio exists to re-rank."""
    cur = rec.get("cur") or {}
    me = _me(rec)
    debt = _num(cur.get("long_term_debt"))
    cash = _num(cur.get("cash"))
    if me is None or debt is None or cash is None:
        return None
    ev = me + debt - cash
    return ev if ev > 0 else None


def _f_earnings_yield(rec):
    return _ratio(rec["cur"].get("net_income"), _me(rec))


def _f_operating_cash_flow_yield(rec):
    return _ratio(rec["cur"].get("cash_flow_operations"), _me(rec))


def _f_free_cash_flow_yield(rec):
    cfo = _num(rec["cur"].get("cash_flow_operations"))
    capex = _num(rec["cur"].get("capital_expenditure"))
    if cfo is None or capex is None:
        return None
    return _ratio(cfo - abs(capex), _me(rec))


def _f_sales_to_market(rec):
    return _ratio(rec["cur"].get("revenue"), _me(rec))


def _f_book_to_market(rec):
    return _ratio(rec["cur"].get("stockholders_equity"), _me(rec))


def _f_tangible_book_to_market(rec):
    eq = _num(rec["cur"].get("stockholders_equity"))
    gw = _num(rec["cur"].get("goodwill"))
    if eq is None or gw is None:
        return None
    return _ratio(eq - gw, _me(rec))


def _f_gross_profit_to_market(rec):
    return _ratio(_gp(rec["cur"]), _me(rec))


def _f_operating_profit_to_market(rec):
    gp = _gp(rec["cur"])
    sga = _num(rec["cur"].get("sganda"))
    if gp is None or sga is None:
        return None
    return _ratio(gp - sga, _me(rec))


def _f_payout_yield(rec):
    div = _num(rec["cur"].get("dividends_paid"))
    buyback = _num(rec["cur"].get("share_repurchase"))
    if div is None and buyback is None:
        return None
    return _ratio(abs(div or 0.0) + abs(buyback or 0.0), _me(rec))


def _f_sales_to_ev(rec):
    return _ratio(rec["cur"].get("revenue"), _ev(rec))


def _f_operating_profit_to_ev(rec):
    gp = _gp(rec["cur"])
    sga = _num(rec["cur"].get("sganda"))
    if gp is None or sga is None:
        return None
    return _ratio(gp - sga, _ev(rec))


def _f_cash_flow_to_ev(rec):
    return _ratio(rec["cur"].get("cash_flow_operations"), _ev(rec))


def _f_market_equity_size(rec):
    """Market equity itself, log-scaled. Registered with the pre-stated NEGATIVE
    sign (the size premium: small outperforms). It is here for two reasons: it is
    a real hypothesis that only became askable now, and it is the control that
    answers 'is this valuation factor merely size in disguise?'."""
    me = _me(rec)
    return math.log(me) if (me is not None and me > 0) else None


VALUATION_FACTORS = (
    _S(name="s26_earnings_yield", family=FAM_VALUATION, direction=1,
       hypothesis="Firms whose earnings are cheap relative to their market value "
                  "earn higher subsequent returns.",
       rationale="The oldest value claim there is: price is what you pay, "
                 "earnings are what you get. It is the natural first test once a "
                 "point-in-time denominator exists, and its behaviour anchors "
                 "every other yield in the family.",
       definition="NetIncome / MarketEquity", needs_prior=False,
       required=("net_income", "market_equity"), fn=_f_earnings_yield),
    _S(name="s26_operating_cash_flow_yield", family=FAM_VALUATION, direction=1,
       hypothesis="Firms whose operating cash flow is cheap relative to market "
                  "value earn higher subsequent returns.",
       rationale="Cash from operations is far harder to manage than earnings, so "
                 "a cash-based yield is a cleaner reading of the same value claim "
                 "and is not contaminated by accrual policy.",
       definition="CashFlowOperations / MarketEquity", needs_prior=False,
       required=("cash_flow_operations", "market_equity"),
       fn=_f_operating_cash_flow_yield),
    _S(name="s26_free_cash_flow_yield", family=FAM_VALUATION, direction=1,
       hypothesis="Firms generating more free cash flow per dollar of market "
                  "value earn higher subsequent returns.",
       rationale="Free cash flow is the cash actually distributable after the "
                 "investment needed to sustain the business, so it is the yield "
                 "an owner could genuinely take out.",
       definition="(CashFlowOperations - |CapitalExpenditure|) / MarketEquity",
       needs_prior=False,
       required=("cash_flow_operations", "capital_expenditure", "market_equity"),
       fn=_f_free_cash_flow_yield),
    _S(name="s26_sales_to_market", family=FAM_VALUATION, direction=1,
       hypothesis="Firms with more revenue per dollar of market value earn "
                  "higher subsequent returns.",
       rationale="Revenue is the least manipulable line on the income statement "
                 "and stays positive for loss-making firms, so sales-to-price "
                 "ranks a population that every earnings-based yield discards.",
       definition="Revenues / MarketEquity", needs_prior=False,
       required=("revenue", "market_equity"), fn=_f_sales_to_market),
    _S(name="s26_book_to_market", family=FAM_VALUATION, direction=1,
       hypothesis="Firms trading at a low price relative to book equity earn "
                  "higher subsequent returns.",
       rationale="The canonical Fama-French value measure. Including it is what "
                 "makes 'is this new, or is it HML?' answerable rather than "
                 "assumed.",
       definition="StockholdersEquity / MarketEquity", needs_prior=False,
       required=("stockholders_equity", "market_equity"), fn=_f_book_to_market),
    _S(name="s26_tangible_book_to_market", family=FAM_VALUATION, direction=1,
       hypothesis="Firms cheap relative to TANGIBLE book equity earn higher "
                  "subsequent returns.",
       rationale="Book equity inflated by acquisition goodwill is not capital an "
                 "owner can redeploy. Removing goodwill is a different economic "
                 "claim about what the balance sheet is worth, not a "
                 "re-specification of book-to-market.",
       definition="(StockholdersEquity - Goodwill) / MarketEquity",
       needs_prior=False,
       required=("stockholders_equity", "goodwill", "market_equity"),
       fn=_f_tangible_book_to_market),
    _S(name="s26_gross_profit_to_market", family=FAM_VALUATION, direction=1,
       hypothesis="Firms with more gross profit per dollar of market value earn "
                  "higher subsequent returns.",
       rationale="Novy-Marx's 'quality at a reasonable price': scaling profit by "
                 "price rather than by assets asks whether the market has already "
                 "paid for the profitability, which is a different question from "
                 "whether the profitability exists.",
       definition="GrossProfit / MarketEquity", needs_prior=False,
       required=("gross_profit|revenue+cost_of_revenue", "market_equity"),
       fn=_f_gross_profit_to_market),
    _S(name="s26_operating_profit_to_market", family=FAM_VALUATION, direction=1,
       hypothesis="Firms with more operating surplus per dollar of market value "
                  "earn higher subsequent returns.",
       rationale="The direct valuation twin of the Stage-25 challenger: the same "
                 "numerator, scaled by price instead of by assets. It is the "
                 "sharpest available test of whether a surviving valuation result "
                 "is genuinely new information or the challenger wearing a "
                 "different denominator.",
       definition="(GrossProfit - SG&A) / MarketEquity", needs_prior=False,
       required=("gross_profit|revenue+cost_of_revenue", "sganda",
                 "market_equity"),
       fn=_f_operating_profit_to_market),
    _S(name="s26_payout_yield", family=FAM_VALUATION, direction=1,
       hypothesis="Firms returning more cash to shareholders per dollar of "
                  "market value earn higher subsequent returns.",
       rationale="Dividends plus buybacks is the cash the owner actually "
                 "received, so it is a realised yield rather than an accounting "
                 "one. Stage 25 rejected payout scaled by ASSETS; scaling by "
                 "price is the claim that the market misprices distributions, "
                 "which is a different hypothesis.",
       definition="(|DividendsPaid| + |ShareRepurchase|) / MarketEquity",
       needs_prior=False,
       required=("dividends_paid", "share_repurchase", "market_equity"),
       fn=_f_payout_yield),
    _S(name="s26_sales_to_ev", family=FAM_VALUATION, direction=1,
       hypothesis="Firms with more revenue per dollar of ENTERPRISE value earn "
                  "higher subsequent returns.",
       rationale="Equity-based yields reward leverage mechanically: two identical "
                 "businesses financed differently get different ranks. Scaling by "
                 "enterprise value asks the capital-structure-neutral question.",
       definition="Revenues / (MarketEquity + LongTermDebt - Cash)",
       needs_prior=False,
       required=("revenue", "market_equity", "long_term_debt", "cash"),
       fn=_f_sales_to_ev,
       caveat="LongTermDebt excludes current maturities and lease liabilities, so "
              "enterprise value is understated for issuers whose debt is short; "
              "measured, not assumed away"),
    _S(name="s26_operating_profit_to_ev", family=FAM_VALUATION, direction=1,
       hypothesis="Firms with more operating surplus per dollar of enterprise "
                  "value earn higher subsequent returns.",
       rationale="The capital-structure-neutral form of the sharpest valuation "
                 "claim in this family, and the closest owned-data approach to "
                 "the EBIT/EV measure the literature favours.",
       definition="(GrossProfit - SG&A) / (MarketEquity + LongTermDebt - Cash)",
       needs_prior=False,
       required=("gross_profit|revenue+cost_of_revenue", "sganda",
                 "market_equity", "long_term_debt", "cash"),
       fn=_f_operating_profit_to_ev,
       caveat="same LongTermDebt scope caveat as s26_sales_to_ev"),
    _S(name="s26_cash_flow_to_ev", family=FAM_VALUATION, direction=1,
       hypothesis="Firms generating more operating cash per dollar of enterprise "
                  "value earn higher subsequent returns.",
       rationale="The cash-based, capital-structure-neutral yield; it is the "
                 "measure an acquirer would actually underwrite.",
       definition="CashFlowOperations / (MarketEquity + LongTermDebt - Cash)",
       needs_prior=False,
       required=("cash_flow_operations", "market_equity", "long_term_debt",
                 "cash"),
       fn=_f_cash_flow_to_ev,
       caveat="same LongTermDebt scope caveat as s26_sales_to_ev"),
    _S(name="s26_market_equity_size", family=FAM_VALUATION_SIZE, direction=-1,
       hypothesis="Smaller firms earn higher subsequent returns.",
       rationale="The size premium, askable for the first time on this panel "
                 "because market equity is now point-in-time. It is registered as "
                 "a hypothesis in its own right AND is the control that decides "
                 "whether any surviving valuation factor is merely small-cap "
                 "exposure re-labelled.",
       definition="log(MarketEquity), expected sign NEGATIVE",
       needs_prior=False, required=("market_equity",),
       fn=_f_market_equity_size),
)

VALUATION_BY_NAME = {f.name: f for f in VALUATION_FACTORS}


def valuation_factor_by_name(name: str):
    return VALUATION_BY_NAME.get(name) or _s25.factor_by_name(name)


def valuation_hypothesis_manifest() -> dict:
    """The pre-registration, emitted before any result is read."""
    return {
        "contract_id": "stage26_valuation_hypothesis_manifest/1",
        "discovery_family": FAMILY_VALUATION,
        "discovery_family_size": len(VALUATION_FACTORS),
        "economic_families": sorted({f.family for f in VALUATION_FACTORS}),
        "multiple_testing": "Benjamini-Hochberg over the WHOLE Stage-26 "
                            "valuation family",
        "signs_fixed_before_evaluation": True,
        "sign_fitted_from_data": False,
        "brute_force_parameter_search_performed": False,
        "denominator_owner": "alpha_agent.pit_market_equity",
        "denominator_contract": _pme.CONTRACT_VERSION,
        "pit_requirement": "every accounting input read with filed <= formation "
                           "date - %d days; every share count FILED on or before "
                           "the formation date; the price is the UNADJUSTED close "
                           "at the formation date" % REPORTING_LAG_DAYS,
        "universe_requirement": "owned Norgate historical index membership at the "
                                "formation month (survivorship-safe, delisted "
                                "retained)",
        "deliberately_excluded": [
            "every accounting family Stage 25 closed with evidence",
            "any re-specification of s24_rnd_intensity",
            "ratios that merely re-scale a Stage-25 rejection by a different "
            "accounting denominator",
            "any variant produced by re-tuning a failed hypothesis",
        ],
        "baseline_comparison": [_s25.BASELINE_COMPOSITE, _s25.BASELINE_MOMENTUM,
                                "s25_operating_profitability",
                                "fundamental_momentum_50_50_v1 (operational "
                                "shape)"],
        "experiments": [f.as_dict() for f in VALUATION_FACTORS],
    }


# =========================================================================== #
# The Stage-26 panel.
#
# Stage 25 deliberately took every forward return from ONE monthly panel and
# introduced no cross-source join, and said so. Stage 26 cannot: market equity
# requires a share count keyed by CIK and a price keyed by ticker, so exactly TWO
# new joins enter, and this builder OWNS them rather than hiding them inside a
# factor. Both are measured on every row and reported, because an unmeasured join
# is how a panel silently loses a population.
# =========================================================================== #
ALL_STAGE26_FACTORS = _s25.ALL_FACTORS + VALUATION_FACTORS


def build_panel(universe: "_s24.HistoricalUniverse",
                bridge: "_s24.IdentityBridge", store: "_s25.Stage25PitStore",
                sectors: "_s25.SectorHistory", beta: "_s25.TrailingBeta",
                equity: "_pme.PitMarketEquity", history: "PitSicHistory", *,
                factors: Sequence = ALL_STAGE26_FACTORS,
                first_month: str = FIRST_MONTH,
                every_n: int = FORMATION_EVERY_N_MONTHS) -> "_s25.Stage25Panel":
    """The Stage-25 panel widened by point-in-time market equity and Tier-C sector.

    Every Stage-25 factor is computed on the SAME rows as every Stage-26
    valuation factor, so an incremental claim never has to be made across two
    differently-built panels. Market equity is injected into the annual record
    before factor evaluation, which is what lets a valuation ratio be an ordinary
    pre-registered ``FactorSpec`` rather than a special case.
    """
    panel = _s25.Stage25Panel()
    all_months = universe.months()
    candidate = [m for m in all_months if m >= first_month]
    formations = candidate[::max(1, int(every_n))]
    stats = {"formations_attempted": 0, "eligible_names": 0, "cik_resolved": 0,
             "annual_record_available": 0, "scored_rows": 0,
             "dropped_no_primary_forward": 0,
             "forward_available": {h["key"]: 0 for h in _s25.HORIZONS},
             "beta_available": 0, "tier_a_known": 0, "tier_b_known": 0,
             "tier_c_known": 0,
             "market_equity_attempts": 0, "market_equity_resolved": 0,
             "market_equity_dispositions": {}, "market_equity_results": []}
    primary_key = _s25.horizon_by_key(PRIMARY_HORIZON)["key"]
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
                continue
            stats["cik_resolved"] += 1
            rec = _s25.annual_record(store, cik, as_of)
            if rec is None:
                continue
            stats["annual_record_available"] += 1
            forward = {}
            for h in _s25.HORIZONS:
                fv = universe.forward_return_chain(m, sym, h["forward_months"])
                if fv is not None:
                    forward[h["key"]] = fv
                    stats["forward_available"][h["key"]] += 1
            if primary_key not in forward:
                stats["dropped_no_primary_forward"] += 1
                continue

            # --- the new join: point-in-time market equity ------------------- #
            # The price is taken at the FORMATION date, not at ``as_of``: the
            # reporting lag exists to bound what ACCOUNTING was knowable, while
            # the market price at the formation date was knowable by definition.
            stats["market_equity_attempts"] += 1
            me = equity.at(symbol=sym, cik=cik, as_of=d)
            disp = me.get("disposition") or "UNKNOWN"
            stats["market_equity_dispositions"][disp] = \
                stats["market_equity_dispositions"].get(disp, 0) + 1
            stats["market_equity_results"].append(me)
            if me.get("ok"):
                stats["market_equity_resolved"] += 1
                # A missing market equity leaves the key ABSENT, so every
                # valuation factor for that row is None. It is never zero-filled
                # and never carried from another date.
                rec["cur"]["market_equity"] = me["market_equity"]

            vals = {f.name: f.value(rec) for f in factors if f is not None}
            if all(v is None for v in vals.values()):
                continue
            adv = prow.get("adv_dollar")
            b = beta.beta_as_of(m, sym)
            if b is not None:
                stats["beta_available"] += 1
            sec_a = sectors.tier_a(store, cik, as_of)
            sec_b = sectors.tier_b(cik, as_of)
            sec_c = history.sector_as_of(cik, as_of)
            if sec_a != _s25.TIER_A_UNKNOWN:
                stats["tier_a_known"] += 1
            if sec_b != _ps.UNKNOWN:
                stats["tier_b_known"] += 1
            if sec_c != _ps.UNKNOWN:
                stats["tier_c_known"] += 1
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
                "sectors": {_s25.TIER_A: sec_a, _s25.TIER_B: sec_b,
                            TIER_C: sec_c},
                "market_equity": me.get("market_equity") if me.get("ok") else None,
                "reported_concepts": frozenset(rec["cur"]),
                "rnd_state": _s25.rnd_availability_state(rec, sec_a),
            }
            stats["scored_rows"] += 1
        if month_rows:
            panel.rows[m] = month_rows
            panel.months.append(m)
            panel.formation_dates[m] = d
    results = stats.pop("market_equity_results")
    stats["market_equity_coverage"] = _pme.coverage_report(results)
    stats["primary_horizon"] = PRIMARY_HORIZON
    stats["join_contract"] = {
        "joins_introduced_by_stage26": [
            "share counts: momentum-panel symbol -> CIK via the released "
            "Phase-10 identity bridge -> Stage-26 companyfacts shares index",
            "prices: momentum-panel symbol -> owned Norgate unadjusted surface "
            "by the SAME TICKER-YYYYMM delisting identity the panel uses",
        ],
        "why_measured": "Stage 25 introduced no cross-source join and said so; "
                        "these two are unavoidable for market equity, so every "
                        "row records whether they resolved and the coverage "
                        "report quantifies who was lost",
        "forward_returns_source": "unchanged - still the ONE monthly panel",
    }
    panel.diagnostics = stats
    return panel


def sector_neutral_cross_sections(periods: list, panel: "_s25.Stage25Panel", *,
                                  tier: str) -> list:
    """Delegate to the released Stage-25 neutraliser for an arbitrary tier."""
    return _s25.sector_neutral_cross_sections(periods, panel, tier=tier)


# =========================================================================== #
# WORKSTREAM 6 - run the valuation campaign.
# =========================================================================== #
def run_valuation_campaign(panel: "_s25.Stage25Panel", *, cfg: dict,
                           champion_returns=None,
                           horizon: str = PRIMARY_HORIZON) -> list:
    """Every pre-registered valuation hypothesis through the RELEASED evaluator
    and the RELEASED evidence gate. Nothing is dropped for being a null and no
    threshold is touched."""
    hz = _s25.horizon_by_key(horizon)["horizon_days"]
    out = []
    for spec in VALUATION_FACTORS:
        periods = panel.factor_cross_sections(spec, horizon=horizon)
        res = score_cross_sections(periods, feature=spec.name, horizon_days=hz,
                                   champion_returns=champion_returns)
        g = gate_for(res["row"], cfg, survivorship_safe=True,
                     point_in_time_valid=True)
        out.append({
            "name": spec.name, "family": spec.family,
            "family_group": FAMILY_VALUATION,
            "spec": spec.as_dict(), "periods_scored": len(periods),
            "row": res["row"], "series": res["series"],
            "metrics": g["metrics"], "gate": g["gate"],
            "drawdown_contract": _s24.drawdown_contract(res["series"]["ls"]),
        })
    return out


def compact_result(r: dict) -> dict:
    """The reporting projection of one campaign result (series dropped)."""
    row = r.get("row") or {}
    return {
        "name": r["name"], "family": r["family"],
        "periods": row.get("periods"), "median_names": row.get("universe"),
        "rank_ic": row.get("rank_ic_mean"), "rank_ic_t": row.get("rank_ic_t"),
        "spread_t": row.get("spread_t"),
        "gross_annualized": row.get("gross_annualized_return"),
        "net25": row.get("net_annualized_return"),
        "net50": (r.get("metrics") or {}).get("net50_spread"),
        "turnover": row.get("turnover"),
        "subperiod_consistency": row.get("subperiod_consistency"),
        "positive_ic_hit_rate": row.get("positive_ic_hit_rate"),
        "max_drawdown_pct": (r.get("metrics") or {}).get("max_drawdown_pct"),
        "pvalue": r.get("pvalue"), "bh_q": r.get("bh_q"),
        "survives_fdr_10pct": r.get("survives_fdr_10pct"),
        "expected_sign": (r.get("spec") or {}).get("expected_sign"),
        "gate": (r.get("gate") or {}).get("target_state"),
        "blocker": (r.get("gate") or {}).get("blocker"),
        "evidence_status": (r.get("gate") or {}).get("evidence_status"),
        "drawdown_contract": r.get("drawdown_contract"),
    }


# =========================================================================== #
# WORKSTREAM 1 - the immutable challenger freeze.
#
# The whole point of a freeze is that it happens BEFORE the future is observable.
# Refitting weights, re-picking a horizon or re-choosing a universe once forward
# tracking has started would silently convert forward evidence back into
# in-sample evidence, so this contract records the specification EXACTLY as
# Stage 25 released it and hashes it. Nothing here is re-optimised.
# =========================================================================== #
FROZEN_CHALLENGER = "s25_operating_profitability"
FROZEN_CHALLENGER_CANDIDATE_ID = "c9_qualityprofi_e490533606"


def challenger_freeze_contract(*, registry_row: Optional[dict] = None,
                               stage25_evidence: Optional[dict] = None,
                               data_fingerprints: Optional[dict] = None,
                               ensemble_row: Optional[dict] = None) -> dict:
    """Freeze the Stage-25 challenger (and, when justified, the best Stage-25
    ensemble) into an immutable, content-hashed research specification."""
    spec = _s25.factor_by_name(FROZEN_CHALLENGER)
    standalone = {
        "candidate_id": FROZEN_CHALLENGER_CANDIDATE_ID,
        "name": FROZEN_CHALLENGER,
        "origin_stage": "stage25-autonomous-alpha-discovery",
        "signal_formula": spec.definition if spec else "(GrossProfit - SG&A) / Assets",
        "source_accounting_concepts": list(spec.required) if spec else [],
        "score_direction": spec.direction if spec else 1,
        "factor_transform": (
            "winsorize the raw cross-section at %g, multiply by the "
            "pre-registered sign, rank cross-sectionally" % WINSOR_FRACTION),
        "pit_policy": "every input read from facts FILED on or before the "
                      "formation date minus %d days" % REPORTING_LAG_DAYS,
        "missing_data_policy": "a name missing any input is ABSENT from that "
                               "cross-section; never zero-filled, never carried "
                               "forward, never imputed",
        "universe_contract": "owned Norgate historical index membership at the "
                             "formation month; delisted and acquired names "
                             "retained (survivorship-safe)",
        "formation_cadence": "quarterly, non-overlapping forward windows",
        "horizon": PRIMARY_HORIZON,
        "horizon_days": _s25.horizon_by_key(PRIMARY_HORIZON)["horizon_days"],
        "cost_policy": "25 bps round-trip reference; 50 bps reported alongside",
        "selection_control_family": _s25.FAMILY_DISCOVERY,
        "selection_control_family_size": 28,
        "sector_tier_used_in_construction":
            "NONE - no classification enters the registered signal",
    }
    # What the hash covers is the whole point. A frozen specification must hash
    # to the same value on a re-run, so the hashed payload contains ONLY the
    # specification: the signal, its immutable registry identity, the data
    # fingerprints and the ensemble STRUCTURE. Everything mutable or measured -
    # lifecycle state, combined score, evidence dates, realised deltas - is
    # reported alongside but deliberately excluded, because a hash that moves
    # when the candidate changes lifecycle state would not be a freeze at all.
    hashed: dict = {"standalone": standalone,
                    "data_fingerprints": data_fingerprints or {}}
    if registry_row:
        hashed["registry_identity"] = {k: registry_row.get(k) for k in (
            "candidate_id", "name", "family", "spec_hash", "spec_version",
            "code_hash", "pit_status", "universe", "data_dependencies",
            "experiment_ids")}
    ensemble = None
    if ensemble_row:
        ensemble = {
            "name": ensemble_row.get("name"),
            "components": ensemble_row.get("components"),
            "weights": ensemble_row.get("weights"),
            "weights_fitted_from_data": bool(
                ensemble_row.get("weights_fitted_from_data")),
            "construction": "each leg oriented by its fixed a-priori sign, "
                            "z-scored within the cross-section, summed at equal "
                            "weight; a name missing any leg is dropped",
            "why_frozen_separately": (
                "the ensemble and the standalone signal answer different "
                "questions - 'does the new information help the model we run?' "
                "versus 'does the new information stand on its own?' - and "
                "forward evidence on one does not transfer to the other"),
        }
        hashed["ensemble"] = ensemble

    frozen = {
        "contract_id": "stage26_challenger_freeze_contract/1",
        "stage": "26",
        "frozen_at_semantics": "the specification is fixed BEFORE any forward "
                               "outcome exists; refitting it after observing "
                               "forward returns would convert forward evidence "
                               "back into in-sample evidence",
        "refit_forbidden": True,
        "weights_fitted_from_data": False,
        "hashed_fields": sorted(hashed),
        "excluded_from_hash": [
            "historical_evidence", "registry_lifecycle (state, evidence status, "
            "combined score, evidence date, active shadow book)",
            "measured ensemble deltas",
        ],
        "why_excluded": "a specification hash that moved when the candidate "
                        "changed lifecycle state, or when a metric was "
                        "re-measured, would not be a freeze",
        **hashed,
        "registry_lifecycle": {k: registry_row.get(k) for k in (
            "lifecycle_state", "evidence_status", "combined_score",
            "latest_evidence_date", "active_shadow_book_id")}
        if registry_row else None,
        "historical_evidence": stage25_evidence,
    }
    if ensemble is not None:
        frozen["ensemble_measured_delta"] = ensemble_row.get(
            "delta_vs_operational_shape_matched_universe")
    frozen["spec_hash"] = content_hash(canonical_json(hashed))
    return frozen


# =========================================================================== #
# WORKSTREAM 2 / 9 - the forward lane.
#
# The canonical machinery already exists and Stage 26 adds none of its own:
# tournament.ShadowBook is the immutable, first-write-wins, no-retroactive store,
# tournament.maybe_activate_shadow_books is the activation owner and
# tournament.advance_shadow_books is the daily-mark owner. What did NOT exist was
# anything to hand them, because both hooks were stubs written for a world with
# no retained candidate. That world ended when Stage 25 retained one, so the two
# providers below fill exactly those two hooks - and nothing else.
# =========================================================================== #
SHADOW_LEG_SIZE = 50
SHADOW_MIN_MARK_COVERAGE = 0.90


def build_shadow_membership(*, ranked: "Sequence[tuple]", leg_size: int = SHADOW_LEG_SIZE,
                            entry_prices: Optional[dict] = None) -> "list[dict]":
    """Equal-weight long/short membership from an oriented ranking.

    ``ranked`` is ``[(symbol, oriented_score), ...]``. The top ``leg_size`` names
    go long and the bottom ``leg_size`` short, each leg equal-weighted to 100 % of
    notional per side — the same dollar-neutral shape every Stage-25 long/short
    statistic was computed on, so the book measures the frozen spec rather than a
    new portfolio construction.
    """
    usable = [(s, v) for s, v in ranked if v is not None
              and (entry_prices is None or (entry_prices.get(s) or 0) > 0)]
    usable.sort(key=lambda t: (-t[1], t[0]))
    n = min(int(leg_size), len(usable) // 2)
    if n <= 0:
        return []
    longs, shorts = usable[:n], usable[-n:]
    out = []
    for sym, score in longs:
        out.append({"symbol": sym, "leg": "LONG", "weight": 1.0 / n,
                    "score": round(float(score), 8),
                    "entry_price": (entry_prices or {}).get(sym)})
    for sym, score in shorts:
        out.append({"symbol": sym, "leg": "SHORT", "weight": -1.0 / n,
                    "score": round(float(score), 8),
                    "entry_price": (entry_prices or {}).get(sym)})
    return sorted(out, key=lambda r: (r["leg"], r["symbol"]))


def shadow_book_nav(*, membership: "Sequence[dict]", prices: dict,
                    notional: float = 100000.0, cost_bps: float = 50.0,
                    min_coverage: float = SHADOW_MIN_MARK_COVERAGE) -> Optional[dict]:
    """Pure NAV kernel for one shadow-book mark. No I/O, no clock, no store.

    Returns ``None`` when priced coverage falls below ``min_coverage``. That is
    deliberate: a partially-priced book would post a NAV that silently assumes
    the unpriced names were flat, which is a fabricated observation. An honest
    coverage gap is worth more than a plausible number.
    """
    if not membership:
        return None
    priced = 0
    weighted_return = 0.0
    for pos in membership:
        entry = _num(pos.get("entry_price"))
        now = _num(prices.get(pos.get("symbol")))
        w = _num(pos.get("weight"))
        if entry is None or now is None or w is None or entry <= 0 or now <= 0:
            continue
        priced += 1
        weighted_return += w * (now / entry - 1.0)
    coverage = priced / len(membership)
    if coverage < float(min_coverage):
        return None
    # One round trip of cost is charged at inception and never re-charged: the
    # book's membership is fixed, so there is no turnover to charge afterwards.
    entry_cost = float(cost_bps) / 10000.0
    nav = float(notional) * (1.0 + weighted_return - entry_cost)
    return {"nav": round(nav, 6), "coverage": round(coverage, 6),
            "priced_positions": priced, "positions": len(membership),
            "gross_weighted_return": round(weighted_return, 8),
            "turnover": 0.0}


def make_shadow_mark_provider(shadow_root, *, close_provider: "Callable",
                              benchmark: str = "SPY",
                              notional: float = 100000.0,
                              cost_bps: float = 50.0) -> "Callable":
    """A ``mark_provider(candidate_id, date)`` for ``tournament.advance_shadow_books``.

    ``close_provider(symbols, date) -> {symbol: close}`` supplies COMPLETED
    closes and is injected, so unit tests never touch a price vendor. Every
    failure path returns ``None``, which the canonical advancer turns into an
    explicit ``SHADOW_MARK_COVERAGE_MISSING`` diagnostic rather than a mark.
    """
    from . import tournament as _t

    root = Path(shadow_root)

    def _provider(candidate_id: str, date: str):
        try:
            sbid = "sb_%s" % candidate_id
            book = _t.ShadowBook(root, sbid)
            doc = book._load() if hasattr(book, "_load") else {}
            inception = (doc or {}).get("inception") or {}
            membership = inception.get("membership") or []
            if not membership:
                return None
            symbols = sorted({p.get("symbol") for p in membership
                              if p.get("symbol")} | {benchmark})
            closes = close_provider(symbols, date) or {}
            if not closes:
                return None
            nav = shadow_book_nav(
                membership=membership, prices=closes,
                notional=_num(inception.get("notional")) or notional,
                cost_bps=_num(inception.get("cost_bps")) or cost_bps)
            if nav is None:
                return None
            return {"nav": nav["nav"],
                    "benchmark_close": _num(closes.get(benchmark)),
                    "turnover": nav["turnover"]}
        except Exception:  # noqa: BLE001 - a mark is never worth breaking a tick
            return None

    return _provider


def make_shadow_inception_provider(*, ranked_by_candidate: dict,
                                   entry_prices: dict,
                                   frozen_spec: dict,
                                   formation_month: Optional[str] = None,
                                   leg_size: int = SHADOW_LEG_SIZE) -> "Callable":
    """An ``inception_provider(candidate) -> {membership, spec}`` for
    ``tournament.maybe_activate_shadow_books``.

    Without one the canonical activator writes a book with an EMPTY membership,
    which can never produce a mark — a book that looks active and measures
    nothing. The rankings are computed by the caller from data available at the
    inception date and are passed in, so this function performs no I/O and adds
    no selection of its own.
    """
    def _provider(cand: dict) -> dict:
        cid = str(cand.get("candidate_id"))
        ranked = ranked_by_candidate.get(cid) or []
        membership = build_shadow_membership(
            ranked=ranked, leg_size=leg_size, entry_prices=entry_prices)
        return {
            "membership": membership,
            "spec": {**(frozen_spec or {}),
                     "formation_month": formation_month,
                     "leg_size": leg_size,
                     "membership_construction":
                         "equal-weight dollar-neutral long top-%d / short "
                         "bottom-%d of the frozen signal's oriented ranking"
                         % (leg_size, leg_size)},
        }

    return _provider


def shadow_forward_readiness(*, registry_counts: dict, shadow_books: list,
                             frozen: dict, activation: Optional[dict] = None,
                             eligible: "Sequence[str]" = (),
                             not_eligible: Optional[dict] = None,
                             providers_wired: Optional[dict] = None) -> dict:
    """What forward evidence can now accumulate, and what still gates it."""
    return {
        "contract_id": "stage26_shadow_forward_readiness/1",
        "owner": "alpha_agent.tournament.maybe_activate_shadow_books "
                 "(+ advance_shadow_books for daily marks)",
        "no_new_engine_created": True,
        "new_forward_evidence_store_created": False,
        "frozen_spec_hash": (frozen or {}).get("spec_hash"),
        "eligible_for_research_forward_tracking": list(eligible),
        "not_eligible": dict(not_eligible or {}),
        "governance": {
            "released_policy_permits_research_shadow_activation": True,
            "evidence": [
                "configs/alpha_agent/stage9_tournament.json safety block declares "
                "research_only, read_only_wrt_operating_portfolio, "
                "shadow_books_read_only, no_automatic_promotion",
                "tournament.run_tournament_cycle already calls "
                "maybe_activate_shadow_books with activate_shadows defaulting to "
                "True, so automatic research-only activation is released behaviour",
                "SHADOW_BOOK_ACTIVE is a research lifecycle state; the only state "
                "that reaches an operator is READY_FOR_MANUAL_REVIEW, and it too "
                "never changes the operating model",
            ],
            "operator_approval_required_for_activation": False,
            "operator_approval_required_for_promotion": True,
            "promotion_possible_from_this_stage": False,
        },
        "governance_tightened_by_stage26": {
            "change": "shadow activation is now FAIL-CLOSED",
            "config_key": "shadow_books.require_forward_eligibility_allowlist",
            "why": (
                "the released selector enrolled every KEEP_FOR_RESEARCH candidate "
                "above a combined-score floor. Two candidates are retained and "
                "BOTH clear the floor - but s24_rnd_intensity is "
                "CONCENTRATION_FRAGILE by its own falsification battery. A "
                "weighted aggregate score cannot express that verdict, so the "
                "released path would have opened an irreversible forward book on "
                "a candidate the prior stage explicitly refused to call a "
                "challenger"),
            "effect": "with no allowlist supplied, NOTHING activates",
        },
        "providers_wired": providers_wired or {},
        "registry_counts": registry_counts,
        "shadow_books": shadow_books,
        "activation": activation,
        "what_will_accumulate": [
            "an immutable first-write-wins inception snapshot (membership, entry "
            "prices, frozen spec, benchmark, cost)",
            "append-only daily NAV marks, strictly monotonic in date, refused if "
            "retroactive or duplicate",
            "realised long/short return net of the modelled entry cost, versus "
            "the benchmark, on days that did not exist when the spec was frozen",
        ],
        "hindsight_controls": [
            "ShadowBook.inception is first-write-wins; a second call returns the "
            "existing document unchanged",
            "ShadowBook.record_mark raises RetroactiveError for any date at or "
            "before inception or at or before the latest recorded mark",
            "the NAV kernel returns None below %.0f%% priced coverage rather than "
            "assuming unpriced names were flat" % (100 * SHADOW_MIN_MARK_COVERAGE),
            "no synthetic historical forward evidence is written; the book starts "
            "empty of marks by construction",
        ],
    }


def forward_evidence_contract(*, readiness: dict, frozen: dict,
                              horizons: "Sequence[int]" = (1, 5, 20, 63)) -> dict:
    """What the FUTURE canonical workflow will do, stated as a contract that a
    hermetic test can assert against rather than a promise."""
    return {
        "contract_id": "stage26_forward_evidence_contract/1",
        "composition_path": [
            "alpha_agent.runtime.run_tournament_tick (bounded, resumable, once "
            "per production cycle)",
            "-> alpha_agent.tournament.run_tournament_cycle",
            "-> maybe_activate_shadow_books (idempotent: a candidate that already "
            "has a book is skipped)",
            "-> advance_shadow_books (ONE immutable mark per evidence date)",
            "-> ShadowBook.record_mark (append-only, strictly monotonic)",
        ],
        "no_production_cycle_run_by_stage26": True,
        "guarantees": {
            "identifies_the_frozen_challenger": "by candidate_id in the durable "
                                                "registry; the spec_hash pins the "
                                                "specification",
            "formation_snapshot_at_the_legitimate_time": "inception is written "
                                                         "once, at activation, "
                                                         "from data available "
                                                         "then",
            "never_overwrites_a_prior_formation": "first-write-wins in "
                                                  "ShadowBook.inception",
            "matures_only_when_the_horizon_arrives": (
                "a mark exists only for dates that have actually occurred; "
                "horizon-scoped evidence at %s trading days is computable only "
                "once that many marks exist" % ", ".join(
                    str(h) for h in horizons)),
            "pending_vs_matured_distinguished": "forward_observations counts "
                                                "recorded marks; a horizon with "
                                                "fewer marks than its length is "
                                                "PENDING, never scored",
            "true_forward_semantics_preserved": "no mark may be written for a "
                                                "date at or before the latest "
                                                "recorded mark",
            "exposed_to_tournament_and_research_agent": "ShadowBook.replay() and "
                                                        "registry.list_shadow_books()",
        },
        "frozen_spec_hash": (frozen or {}).get("spec_hash"),
        "readiness": {k: readiness.get(k) for k in (
            "eligible_for_research_forward_tracking", "not_eligible",
            "providers_wired")},
        "fake_maturation_forbidden": True,
        "backdating_forbidden": True,
    }


# =========================================================================== #
# WORKSTREAM 4 - close the sector evidence gap, WITHOUT re-specification.
#
# This is not a new campaign. It re-runs the EXACT tests Stage 25 already froze,
# on the EXACT factors, with the EXACT pre-registered thresholds, changing ONE
# thing: the classification tier those tests consume. Anything else - a new
# threshold, a new exclusion set, a different window - would convert a
# confirmation into a fresh search, and a fresh search over an already-tested
# hypothesis is how a dead factor gets resurrected.
# =========================================================================== #
#: Re-used verbatim from Stage 25. NOT redefined, NOT relaxed.
SURVIVE_MIN_T = _s25.RND_SURVIVE_MIN_T
SURVIVE_MIN_RETENTION = _s25.RND_SURVIVE_MIN_RETENTION
TECH_EXPOSED_SECTORS = _s25.TECH_EXPOSED_SECTORS

#: The factors whose Stage-25 sector evidence was provisional, and the verdict
#: each carried out of Stage 25. Both are re-tested; neither is re-specified.
SECTOR_AFFECTED = {
    "s25_operating_profitability": "SURVIVES_SECTOR_AND_STYLE_CONTROLS",
    "s24_rnd_intensity": "CONCENTRATION_FRAGILE",
}


def sector_revalidation(panel: "_s25.Stage25Panel", *,
                        factors: "Sequence[str]" = tuple(SECTOR_AFFECTED),
                        horizon: str = PRIMARY_HORIZON) -> dict:
    """Re-run the frozen sector-affected tests against leakage-safe Tier C."""
    out: dict = {
        "contract_id": "stage26_pit_sector_revalidation/1",
        "what_changed": "ONLY the classification tier consumed by the already-"
                        "frozen tests: Tier B (look-ahead, provisional) is "
                        "replaced by Tier C (leakage-safe, per-filing)",
        "what_did_not_change": [
            "the hypotheses", "the factor definitions", "the expected signs",
            "the pre-registered survival thresholds (IC t >= %.1f AND >= %.0f%% "
            "of raw rank IC retained)" % (SURVIVE_MIN_T,
                                          100 * SURVIVE_MIN_RETENTION),
            "the formation cadence, horizon, universe and cost model",
            "the sector-removal sets",
        ],
        "thresholds_reused_from_stage25": {
            "min_controlled_ic_t": SURVIVE_MIN_T,
            "min_raw_ic_retention": SURVIVE_MIN_RETENTION,
        },
        "factors": {},
    }
    for name in factors:
        spec = valuation_factor_by_name(name)
        if spec is None:
            out["factors"][name] = {"status": "FACTOR_NOT_FOUND"}
            continue
        base = panel.factor_cross_sections(spec, horizon=horizon)
        raw = evaluate_variant(base, feature=name, horizon=horizon, label="raw",
                               evidence_class="SURVIVORSHIP_SAFE_POINT_IN_TIME")
        raw_ic = raw.get("rank_ic")
        variants: "list[dict]" = [raw]

        for tier, label, klass in (
                (_s25.TIER_A, "sector_neutral_tier_a_leakage_safe",
                 "LEAKAGE_SAFE_COARSE"),
                (_s25.TIER_B, "sector_neutral_tier_b_lookahead_control",
                 "CLASSIFICATION_LOOKAHEAD_CONTROL"),
                (TIER_C, "sector_neutral_tier_c_leakage_safe_fine",
                 "LEAKAGE_SAFE_PIT_FILING_SIC")):
            periods = sector_neutral_cross_sections(base, panel, tier=tier)
            variants.append(evaluate_variant(periods, feature=name,
                                             horizon=horizon, label=label,
                                             evidence_class=klass))

        month_by_date = {panel.formation_dates[m]: m for m in panel.months}

        def _remove(removed: "tuple[str, ...]", tag: str):
            keep = []
            for p in base:
                m = month_by_date.get(p["as_of"])
                if m is None:
                    continue
                names = [(s, v, f) for s, v, f in p["names"]
                         if panel.sector_of(m, s, tier=TIER_C) not in removed]
                if len(names) >= MIN_CROSS_SECTION:
                    keep.append({"as_of": p["as_of"], "month": m,
                                 "names": names})
            return evaluate_variant(
                keep, feature=name, horizon=horizon, label=tag,
                evidence_class="LEAKAGE_SAFE_PIT_FILING_SIC")

        variants.append(_remove(("Technology",),
                                "remove_technology_tier_c"))
        variants.append(_remove(("ConsumerDiscretionary",),
                                "remove_consumer_discretionary_tier_c"))
        variants.append(_remove(TECH_EXPOSED_SECTORS,
                                "remove_tech_and_consumer_discretionary_tier_c"))

        for v in variants[1:]:
            v["retention_vs_raw"] = _s25._retention(raw_ic, v.get("rank_ic"))
            v["survives"] = _s25._survives(raw_ic, v)

        tier_c = [v for v in variants if v["variant"].endswith("_tier_c")
                  or "tier_c" in v["variant"]]
        failed = [v["variant"] for v in tier_c if v.get("survives") is False]
        underpowered = [v["variant"] for v in tier_c if v.get("survives") is None]
        prior = SECTOR_AFFECTED.get(name)
        if failed:
            effect = "WEAKENS"
        elif underpowered:
            effect = "INCONCLUSIVE_UNDERPOWERED"
        else:
            neutral_c = next((v for v in variants
                              if v["variant"] ==
                              "sector_neutral_tier_c_leakage_safe_fine"), {})
            neutral_b = next((v for v in variants
                              if v["variant"] ==
                              "sector_neutral_tier_b_lookahead_control"), {})
            tb, tc = neutral_b.get("rank_ic_t"), neutral_c.get("rank_ic_t")
            if tb is not None and tc is not None and tc > tb:
                effect = "STRENGTHENS"
            else:
                effect = "UNCHANGED"
        out["factors"][name] = {
            "factor": name,
            "stage25_verdict": prior,
            "stage25_verdict_unchanged_by_stage26": True,
            "raw": raw,
            "variants": variants,
            "tier_c_failures": failed,
            "tier_c_underpowered": underpowered,
            "effect_of_leakage_safe_fine_sector": effect,
            "sector_evidence_status": (
                "CONCLUSIVE_LEAKAGE_SAFE" if not underpowered
                else "PROVISIONAL_UNDERPOWERED"),
            "note": (
                "Stage 25 stamped every Tier-B result provisional because the "
                "control carried a classification look-ahead. Tier C carries "
                "none, so the sector question for this factor is now answered "
                "rather than bounded."),
        }
    return out


# =========================================================================== #
# WORKSTREAM 5 - the capability statement for PIT market cap.
# =========================================================================== #
def pit_market_cap_capability(*, equity: "_pme.PitMarketEquity",
                              panel_diagnostics: Optional[dict] = None) -> dict:
    cov = (panel_diagnostics or {}).get("market_equity_coverage") or {}
    resolved = cov.get("coverage_fraction")
    return {
        "contract_id": "stage26_pit_market_cap_capability/1",
        "stage25_state": "WAITING_FOR_DATA (blocked by TWO independent gaps)",
        "stage26_state": ("READY_FOR_PIT_RESEARCH" if (resolved or 0) >= 0.5
                          else "PARTIAL_COVERAGE"),
        "gaps_closed": [
            {"gap": "share counts discarded by the monetary-unit filter",
             "closed_by": "opt-in extra_units={'shares'} / extra_taxonomies="
                          "{'dei'} in the RELEASED companyfacts parser; every "
                          "existing caller keeps byte-identical behaviour",
             "cost": "none - the owned companyfacts.zip was already on disk"},
            {"gap": "only owned price surface is TOTALRETURN adjusted",
             "closed_by": "the same owned, entitled local Norgate installation "
                          "serves NONE (raw traded price) and CAPITAL "
                          "(capital-events-only) adjustments",
             "cost": "none - no purchase, no upgrade, no new entitlement"},
        ],
        "why_fixing_one_would_have_been_wrong": (
            "Stage 25 warned that fixing only the parser produces a plausible "
            "but WRONG market cap, because an as-reported share count times a "
            "TOTALRETURN-adjusted price is off by the cumulative split-and-"
            "dividend factor - a per-name error that does not cancel in a "
            "cross-sectional rank"),
        "capability": equity.capability(),
        "coverage": cov,
        "coverage_fraction": resolved,
        "remaining_shortcomings": [
            "multi-share-class issuers: the companyfacts surface reports the "
            "cover-page count without a class dimension, so a dual-class issuer "
            "may carry one class's count against a total-company price",
            "the share count is as of its filing, not as of the formation date; "
            "issuance and buyback between the two are not tracked, only the "
            "capital-event carry is",
            "LongTermDebt excludes current maturities and lease liabilities, so "
            "enterprise value is understated where debt is short-dated",
            "identity backlog: an unresolved symbol has no CIK and is therefore "
            "absent from every valuation cross-section, exactly as it was absent "
            "from every Stage-25 fundamental cross-section",
        ],
    }


# =========================================================================== #
# WORKSTREAM 7 - is a surviving valuation signal NEW INFORMATION?
#
# The bar is not "does it work". It is "does it tell us something the model we
# already run does not already know". A valuation factor that is operating
# profitability wearing a price denominator is REDUNDANT no matter how strong it
# looks, and saying so is the entire point of this workstream.
# =========================================================================== #
REDUNDANCY_ABS_CORR = 0.70
REDUNDANCY_PARTIAL_T = 2.0
TOP_N_OVERLAP = 25


def _top_names(periods: list, n: int) -> "dict[str, set]":
    out = {}
    for p in periods:
        ranked = sorted(p["names"], key=lambda t: -t[1])[:n]
        out[p["as_of"]] = {s for s, _, _ in ranked}
    return out


def _overlap(a: "dict[str, set]", b: "dict[str, set]") -> Optional[float]:
    shared = sorted(set(a) & set(b))
    if not shared:
        return None
    vals = []
    for d in shared:
        x, y = a[d], b[d]
        if x and y:
            vals.append(len(x & y) / float(min(len(x), len(y))))
    return round(_mean(vals), 6) if vals else None


def _exposure(periods: list, panel: "_s25.Stage25Panel", control: str
              ) -> Optional[float]:
    """Mean within-period rank correlation of the oriented signal with a control."""
    from . import orthogonality as _o
    month_by_date = {panel.formation_dates[m]: m for m in panel.months}
    vals = []
    for p in periods:
        m = month_by_date.get(p["as_of"])
        if m is None:
            continue
        syms = [s for s, _, _ in p["names"]]
        ctl = panel.control_series(m, syms, control)
        pairs = [(v, c) for (_, v, _), c in zip(p["names"], ctl) if c is not None]
        if len(pairs) < MIN_CROSS_SECTION:
            continue
        r = _o.rank_correlation([x for x, _ in pairs], [y for _, y in pairs])
        if r is not None:
            vals.append(r)
    return round(_mean(vals), 6) if vals else None


def _long_leg_sector_mix(periods: list, panel: "_s25.Stage25Panel", *,
                         tier: str, n: int = TOP_N_OVERLAP) -> dict:
    month_by_date = {panel.formation_dates[m]: m for m in panel.months}
    counts: "dict[str, int]" = {}
    total = 0
    for p in periods:
        m = month_by_date.get(p["as_of"])
        if m is None:
            continue
        for s, _, _ in sorted(p["names"], key=lambda t: -t[1])[:n]:
            sec = panel.sector_of(m, s, tier=tier)
            counts[sec] = counts.get(sec, 0) + 1
            total += 1
    if not total:
        return {"tier": tier, "long_leg_slots": 0, "mix_pct": {}}
    mix = {k: round(100.0 * v / total, 4)
           for k, v in sorted(counts.items(), key=lambda kv: -kv[1])}
    top = max(mix.values()) if mix else None
    return {"tier": tier, "long_leg_slots": total, "mix_pct": mix,
            "max_sector_concentration_pct": top}


def valuation_incrementality(panel: "_s25.Stage25Panel", *, names: "Sequence[str]",
                             baselines: "dict[str, list]", cfg: dict,
                             horizon: str = PRIMARY_HORIZON) -> dict:
    """Every clearing valuation candidate against every baseline that matters."""
    out: dict = {
        "contract_id": "stage26_valuation_incrementality/1",
        "baselines": sorted(baselines),
        "redundancy_rule": {
            "abs_cross_sectional_rank_correlation_at_or_above":
                REDUNDANCY_ABS_CORR,
            "or_partial_ic_t_below": REDUNDANCY_PARTIAL_T,
            "stated_before_results": True,
            "reading": "a valuation factor that is merely another expression of "
                       "an existing signal is REDUNDANT regardless of its "
                       "standalone strength",
        },
        "candidates": {},
    }
    base_tops = {b: _top_names(p, TOP_N_OVERLAP) for b, p in baselines.items()}
    for name in names:
        spec = valuation_factor_by_name(name)
        if spec is None:
            continue
        periods = panel.factor_cross_sections(spec, horizon=horizon)
        incr = incrementality(periods, baselines=baselines, cfg=cfg,
                              candidate_name=name)
        tops = _top_names(periods, TOP_N_OVERLAP)
        for b in baselines:
            vs = (incr.get("vs") or {}).get(b) or {}
            vs["top%d_overlap" % TOP_N_OVERLAP] = _overlap(tops, base_tops[b])
        corrs = [abs(v.get("mean_cross_sectional_rank_correlation") or 0.0)
                 for v in (incr.get("vs") or {}).values()]
        partials = [v.get("partial_rank_ic_t")
                    for v in (incr.get("vs") or {}).values()
                    if v.get("partial_rank_ic_t") is not None]
        max_corr = max(corrs) if corrs else None
        min_partial = min(partials) if partials else None
        by_corr = bool(max_corr is not None and max_corr >= REDUNDANCY_ABS_CORR)
        by_partial = bool(min_partial is not None
                          and min_partial < REDUNDANCY_PARTIAL_T)
        redundant = by_corr or by_partial
        # The DECISION and its thresholds are the pre-registered ones and are not
        # touched. This only records WHICH limb fired, because the two mean very
        # different things: a factor can fail for restating a signal we already
        # have, or for carrying no information at all, and calling both
        # "redundant" without saying which would blur a real distinction.
        reason = None
        if by_corr and by_partial:
            reason = "REDUNDANT_WITH_EXISTING_SIGNAL_AND_NO_INDEPENDENT_INFORMATION"
        elif by_corr:
            reason = "REDUNDANT_WITH_EXISTING_SIGNAL"
        elif by_partial:
            reason = "NO_INDEPENDENT_INFORMATION"
        multi = _s25.multi_horizon(panel, [name], cfg=cfg)
        out["candidates"][name] = {
            "incrementality": incr,
            "max_abs_correlation_vs_any_baseline": max_corr,
            "min_partial_ic_t_vs_any_baseline": min_partial,
            "classification": "REDUNDANT" if redundant else "INDEPENDENT_ALPHA",
            "not_independent_reason": reason,
            "size_exposure_rank_corr": _exposure(periods, panel,
                                                 "log_adv_dollar"),
            "volatility_exposure_rank_corr": _exposure(periods, panel,
                                                       "realized_vol_63d"),
            "beta_exposure_rank_corr": _exposure(periods, panel,
                                                 "trailing_beta"),
            "long_leg_sector_mix_tier_c": _long_leg_sector_mix(periods, panel,
                                                              tier=TIER_C),
            "horizon_stability": multi,
            "sector_neutral_tier_c": evaluate_variant(
                sector_neutral_cross_sections(periods, panel, tier=TIER_C),
                feature=name, horizon=horizon,
                label="sector_neutral_tier_c_leakage_safe_fine",
                evidence_class="LEAKAGE_SAFE_PIT_FILING_SIC"),
            "winner_removal_top5": evaluate_variant(
                _s25.drop_top_winners(periods, 5), feature=name,
                horizon=horizon, label="drop_top_5_winners_per_period",
                evidence_class="ADVERSARIAL_CONCENTRATION_TEST"),
        }
    return out


# =========================================================================== #
# WORKSTREAM 8 - bounded next-generation ensembles.
#
# Reuses the RELEASED Stage-25 menu builder and evaluator unchanged, so the
# mandatory matched-universe correction - the one that caught a major issue in
# Stage 25 - applies here by construction rather than by remembering to do it.
# =========================================================================== #
def next_generation_ensembles(panel: "_s25.Stage25Panel", *, comp: list,
                              mom: list, picks: "list[tuple]",
                              references: "list[tuple]" = (),
                              cfg: dict) -> dict:
    res = _s25.evaluate_ensembles(
        _s25.ensemble_menu(comp=comp, mom=mom, picks=picks,
                           references=references), cfg=cfg)
    res["contract_id"] = "stage26_ensembles/1"
    res["owner"] = "alpha_agent.stage25_alpha_discovery.evaluate_ensembles "
    res["owner"] += "(released; reused unchanged, not reimplemented)"
    res["picks_offered"] = [n for n, _ in picks]
    res["references_offered"] = [n for n, _ in references]
    res["signal_improvement_vs_universe_effect"] = (
        "the matched-universe row isolates the two. Restricting the universe to "
        "the names that carry a sparsely-reported signal is itself worth real "
        "IC; only the matched delta is attributable to the signal.")
    return res


# =========================================================================== #
# WORKSTREAM 10 - what forward evidence can answer that a backtest cannot.
# =========================================================================== #
def hoc_forward_contract(*, decision_link: Optional[dict] = None,
                         stage25_counterfactual: Optional[dict] = None) -> dict:
    """The capital-deployment question, stated as a forward contract.

    Stage 25's historical holding-opportunity-cost counterfactual was a null
    (mean score gap -0.0046, t -0.34). Re-running it until it turns favourable
    would be specification search against a fixed history, so Stage 26 does not
    re-run it at all. What it does instead is state precisely which questions
    only future observations can answer, and confirm the seam that will answer
    them already exists.
    """
    return {
        "contract_id": "stage26_hoc_forward_contract/1",
        "owner": "alpha_agent.stage23_unified.build_decision_link (called, not "
                 "duplicated)",
        "no_second_hoc_engine_created": True,
        "historical_counterfactual_rerun_by_stage26": False,
        "why_not_rerun": (
            "the historical counterfactual is a fixed sample. Re-running it with "
            "a different window, ranking depth or holding definition until it "
            "turns favourable is specification search, and Stage 25 already "
            "reported the honest null rather than dressing it up."),
        "stage25_result": stage25_counterfactual or {
            "verdict": "NULL",
            "mean_score_gap": -0.0046, "t": -0.34,
            "losers_ranked_lower_fraction": 0.477,
            "label": "COUNTERFACTUAL_NOT_PROOF",
        },
        "decision_link_status": decision_link,
        "questions_only_forward_evidence_can_answer": [
            "did the challenger rank a holding lower BEFORE that holding "
            "deteriorated, on a date the challenger had not seen?",
            "did it identify a replacement earlier than the incumbent did?",
            "did the replacements it favoured outperform the holdings it would "
            "have released, measured on realised forward returns?",
            "what switching cost would actually have been incurred at the "
            "traded spread, not at a modelled one?",
            "did it reduce regret - the gap between what was held and the best "
            "alternative that was available at the time?",
            "did it improve risk-adjusted deployment of capital, or only "
            "cross-sectional ranking?",
        ],
        "why_history_cannot_answer_them": (
            "every one of those questions is about a DECISION, and the decisions "
            "in the historical sample were made by the incumbent model. A "
            "counterfactual can only ask how the challenger would have SCORED "
            "names the incumbent chose - never what the portfolio would have "
            "become had the challenger been choosing, because the subsequent "
            "holdings, cash and opportunity set would all have differed."),
        "current_state": "INSUFFICIENT_FORWARD_EVIDENCE",
        "minimum_matured_live_observations": 12,
        "uses_existing_outcome_intelligence": (
            "Stage 21 outcome intelligence and Stage 20 reassessment outcomes "
            "remain the owners; Stage 26 adds no outcome calculation"),
    }


# =========================================================================== #
# WORKSTREAM 12/13/14 - frontier, exhaustion and the purchase gate.
# =========================================================================== #
def new_information_frontier(*, sector_status: dict, market_cap_status: dict,
                             valuation_outcome: dict,
                             capability: Optional[dict] = None) -> dict:
    """What high-value economic information remains free or already owned.

    A family is listed as OPEN only if it clears four tests stated before the
    search: economically distinct from everything already registered, absent from
    the candidate registry, point-in-time observable, and large enough to power a
    test. 'Nothing further of high value remains' is a legitimate answer and is
    given where it is true.
    """
    return {
        "contract_id": "stage26_new_information_frontier/1",
        "admission_tests": [
            "economically distinct from every registered family",
            "not already represented in tournament.CandidateRegistry",
            "point-in-time observable with a real availability timestamp",
            "sample large enough to power a cross-sectional test",
        ],
        "unlocked_by_stage26": [
            {"family": "pit_fine_grained_sector", "state": sector_status.get(
                "state", "READY_FOR_PIT_RESEARCH"),
             "source": "SEC Financial Statement Data Sets sub.txt (free)",
             "value": "converts every provisional sector verdict into a "
                      "conclusive one and is a control, never a signal"},
            {"family": "pit_market_equity",
             "state": market_cap_status.get("stage26_state"),
             "source": "owned companyfacts share counts x owned Norgate "
                       "unadjusted price",
             "value": "the denominator that made the valuation family askable"},
            {"family": "pit_valuation_ratios",
             "state": valuation_outcome.get("state"),
             "source": "derived", "value": valuation_outcome.get("summary")},
        ],
        "still_open_free_or_owned": [
            {"family": "sec_filing_timing_and_lateness",
             "priority": "MEDIUM",
             "economically_distinct": True,
             "hypothesis": "filing promptness and late-filing notifications are "
                           "a governance/stress signal orthogonal to the reported "
                           "numbers themselves",
             "source": "ALREADY ACQUIRED - sub.txt carries per-filing accepted, "
                       "filed, period and form for every submission, so the "
                       "filing-lag distribution is on disk with no further "
                       "acquisition",
             "why_not_run_here": "it is a genuinely new economic family, not a "
                                 "confirmation of an existing one, and opening a "
                                 "new family belongs in a campaign with its own "
                                 "pre-registration rather than as a rider on "
                                 "this stage's valuation family",
             "blocker": "NONE - runnable now"},
            {"family": "restatement_and_amendment_history",
             "priority": "MEDIUM",
             "economically_distinct": True,
             "hypothesis": "an issuer that amends or restates is signalling "
                           "accounting stress that the restated numbers hide",
             "source": "ALREADY OWNED - sub.txt carries prevrpt and the -/A form "
                       "suffix; the companyfacts indexes already preserve "
                       "amendments as distinct rows keyed by accession",
             "why_not_run_here": "same reason as filing timing",
             "blocker": "NONE - runnable now"},
            {"family": "share_count_dynamics",
             "priority": "MEDIUM",
             "economically_distinct": True,
             "hypothesis": "net share issuance predicts returns independently of "
                           "the payout ratio, because issuance is management's "
                           "own valuation opinion",
             "source": "NEWLY OWNED - the Stage-26 share-count index makes the "
                       "share-count time series point-in-time for the first time",
             "why_not_run_here": "it became askable only once this stage built "
                                 "the share index; it deserves a pre-registered "
                                 "campaign rather than an afterthought",
             "blocker": "NONE - runnable now"},
        ],
        "explicitly_not_reopened": [
            "the 18 closed families in the Stage-25 exhaustion registry",
            "any accounting ratio scaled by assets, revenue or equity - five of "
            "six such families are closed with evidence",
            "s24_rnd_intensity under any re-specification",
        ],
        "capability_map": capability,
    }


def research_exhaustion_update(*, stage25_exhaustion: Optional[dict] = None,
                               valuation_results: "Sequence[dict]" = (),
                               valuation_fdr: Optional[dict] = None,
                               sector_status: str = "",
                               market_cap_status: str = "",
                               challenger_state: str = "",
                               frontier: Optional[dict] = None) -> dict:
    """Update the EXISTING research memory. No second queue is created."""
    survivors = list((valuation_fdr or {}).get("survivors_q10") or [])
    cleared = [r["name"] for r in valuation_results
               if (r.get("gate") or {}).get("target_state") == "KEEP_FOR_RESEARCH"]
    rejected = [r["name"] for r in valuation_results if r["name"] not in cleared]
    if not valuation_results:
        val_state = "BLOCKED"
    elif cleared and survivors:
        val_state = "ACTIVE"
    else:
        val_state = "COMPLETE"
    return {
        "contract_id": "stage26_research_exhaustion_update/1",
        "owner": "alpha_agent.autonomous_research + alpha_agent.tournament "
                 "(the EXISTING agent and the EXISTING registry)",
        "second_queue_created": False,
        "deduplication_owner": "tournament.CandidateRegistry (spec_hash + "
                               "processed_experiments)",
        "concepts": {
            "OPERATING_PROFITABILITY": challenger_state or
                                       "RESEARCH_CHALLENGER / FORWARD_TRACK",
            "R_AND_D": "CONCENTRATION_FRAGILE - do not re-open; the sector "
                       "explanation is now dead on LEAKAGE-SAFE evidence too, "
                       "which changes the reason it is not a challenger but not "
                       "the verdict",
            "RESIDUAL_MOMENTUM": "EXHAUSTED_NEGATIVE",
            "STAGE25_REJECTED_FUNDAMENTALS": "DO_NOT_REOPEN_WITHOUT_NEW_"
                                             "INFORMATION",
            "PIT_FINE_SECTOR": sector_status or "READY",
            "PIT_MARKET_CAP": market_cap_status or "READY",
            "PIT_VALUATION": val_state,
            "ANALYST_REVISIONS": "WAITING_FOR_DATA",
        },
        "stop_testing": sorted(set(
            list((stage25_exhaustion or {}).get("do_not_reopen") or [])
            + rejected
            + ["s24_rnd_intensity re-specifications",
               "asset-scaled, revenue-scaled and equity-scaled accounting "
               "ratios in the five closed Stage-25 families"])),
        "remains_active": sorted(set(
            ["quality_profitability (1 of 5 cleared gate AND FDR)"]
            + (["pit_valuation_ratios (%d cleared, %d survived FDR)"
                % (len(cleared), len(survivors))] if cleared else []))),
        "forward_tracking": [FROZEN_CHALLENGER],
        "waiting_for_data": ["historical_analyst_revisions"],
        "newly_runnable_families": [f["family"] for f in
                                    (frontier or {}).get(
                                        "still_open_free_or_owned", [])],
        "valuation_family": {
            "family": FAMILY_VALUATION,
            "size": len(valuation_results),
            "cleared_gate": cleared,
            "survived_fdr_10pct": survivors,
            "rejected": rejected,
        },
    }


def external_data_purchase_gate(*, frontier: dict, exhaustion: dict,
                                valuation_outcome: dict,
                                intrinio: Optional[dict] = None) -> dict:
    """Is the case for buying a genuinely distinct dataset now stronger?

    The decision rule is the released one and is APPLIED rather than asserted: a
    paid dataset is recommendable only when (a) the owned surface is exhausted
    for the hypotheses it would unlock, (b) no FREE artefact would unlock them
    first, and (c) no prior evaluation of that same vendor already returned a
    negative result. Nothing here authorises a purchase.
    """
    free_open = [f for f in frontier.get("still_open_free_or_owned", [])
                 if f.get("blocker") == "NONE - runnable now"]
    owned_exhausted = not free_open
    return {
        "contract_id": "stage26_external_data_purchase_gate/1",
        "authorises_purchase": False,
        "decision_rule": [
            "(a) the owned surface is exhausted for the hypotheses the dataset "
            "would unlock",
            "(b) no FREE artefact would unlock them first",
            "(c) no prior evaluation of that same vendor already returned a "
            "negative result",
        ],
        "owned_surface_exhausted": owned_exhausted,
        "free_families_still_runnable": [f["family"] for f in free_open],
        "headline": (
            "STILL WAIT. Stage 25's free-artefact recommendation was taken and "
            "is now spent, but taking it did not exhaust the owned surface - it "
            "revealed three further families that are runnable today at zero "
            "cost, and the share-count index this stage built created one of "
            "them. Condition (a) fails, so no purchase clears the rule."
            if not owned_exhausted else
            "The owned and free surface is now exhausted for the hypotheses in "
            "scope; condition (a) is satisfied for the first time."),
        "datasets": [
            {"dataset": "SEC Financial Statement Data Sets sub.txt",
             "recommendation": "ACQUIRED",
             "cost": "free",
             "outcome": "acquired this stage; %s" % (
                 frontier.get("unlocked_by_stage26", [{}])[0].get("value", "")),
             "note": "this was Stage 25's number-one queue item and it is now "
                     "closed"},
            {"dataset": "Historical analyst revision vintages (Intrinio)",
             "recommendation": "WAIT",
             "hypotheses_unlocked": 6,
             "expected_orthogonality": "HIGH - expectations data is not derivable "
                                       "from reported accounting",
             "pit_depth": "UNVERIFIED - requires AS-WAS consensus vintages",
             "hard_requirement": "historical AS-WAS consensus/revision vintages. "
                                 "Fiscal-period FINAL estimates are NOT "
                                 "sufficient: a final estimate embeds everything "
                                 "learned after the formation date, which is the "
                                 "look-ahead the whole programme exists to avoid",
             "condition_a_owned_exhausted": owned_exhausted,
             "condition_b_free_artefact_first": bool(free_open),
             "condition_c_prior_negative_evaluation": True,
             "prior_evaluation": "a live Intrinio trial already returned "
                                 "NO_DEFENSIBLE_ALPHA / DO_NOT_BUY on a "
                                 "survivorship-safe 16-year test",
             "why_wait": "conditions (a) and (b) both fail and (c) is a prior "
                         "negative on this exact vendor",
             "status": (intrinio or {}).get("state", "WAITING_FOR_DATA")},
            {"dataset": "Steele / other fundamental vendor history",
             "recommendation": "REJECT",
             "why": "it would restate the same accounting information this "
                    "programme already reads point-in-time from SEC, and Stage "
                    "25 rejected most of the accounting families it would "
                    "extend. Stage 26 strengthens the rejection: the one "
                    "accounting gap that mattered (share counts) turned out to "
                    "be present in the owned archive all along and was closed "
                    "for free."},
            {"dataset": "Vendor point-in-time sector/industry classification",
             "recommendation": "REJECT",
             "why": "superseded. This was a live candidate while Tier B carried "
                    "a look-ahead; sub.txt now supplies a leakage-safe "
                    "per-filing classification at zero cost, so a paid "
                    "classification would buy nothing that is not already owned."},
        ],
        "verdict": "WAIT",
        "verdict_basis": {
            "valuation_outcome": valuation_outcome.get("state"),
            "families_runnable_for_free": len(free_open),
            "exhaustion_snapshot": {k: v for k, v in
                                    (exhaustion.get("concepts") or {}).items()},
        },
    }


def intrinio_status(cfg_path=None) -> dict:
    """Delegates to the released Stage-25 reader; adds no vendor framework."""
    base = _s25.intrinio_status(cfg_path)
    base["contract_id"] = "stage26_intrinio_status/1"
    base["paid_api_called_by_stage26"] = False
    base["quota_spent_by_stage26"] = 0
    base["pipeline_owner"] = "alpha_agent.analyst_revisions"
    base["pipeline_unchanged"] = True
    base["immediately_pluggable_on_arrival"] = [
        "importer -> pit_scan -> adequacy gate",
        "six frozen Stage-13A hypotheses under BH-FDR",
        "incrementality against the CURRENT model on the same cross-sections",
        "the same tournament lifecycle, still with no automatic promotion",
    ]
    base["what_stage26_changed_for_it"] = (
        "an analyst signal will now be judged against a baseline that also "
        "carries a leakage-safe fine-grained sector control and a point-in-time "
        "valuation family, so the families it could be redundant with are known "
        "rather than assumed")
    base["current_snapshots_used_as_historical_vintages"] = False
    return base


# =========================================================================== #
# Capability map and the orchestrator.
# =========================================================================== #
def capability_map(*, sector_coverage: dict, market_cap: dict,
                   shares_status: dict, price_status: dict,
                   fsds_status: dict) -> dict:
    """The Stage-25 map with the three families this stage moved, and nothing
    else re-stated as if it were new."""
    return {
        "contract_id": "stage26_capability_map/1",
        "inherits": "stage25 research_capability_map (17 families)",
        "families_moved_by_stage26": [
            {"family": "pit_sector_history",
             "was": "READY_WITH_LIMITATIONS (two-tier; fine tier carried a "
                    "classification look-ahead)",
             "now": _s25.CAP_READY,
             "evidence": {k: sector_coverage.get(k) for k in (
                 "coverage_fraction", "unknown_rate", "classified_rows",
                 "panel_rows", "issuers_classified", "classification_stability",
                 "delisted_coverage_fraction")},
             "source": fsds_status},
            {"family": "pit_market_cap",
             "was": _s25.CAP_WAITING,
             "now": market_cap.get("stage26_state"),
             "evidence": market_cap.get("coverage"),
             "source": {"shares": shares_status, "prices": price_status}},
            {"family": "pit_valuation_ratios",
             "was": _s25.CAP_WAITING,
             "now": _s25.CAP_READY if market_cap.get("stage26_state") ==
                    _s25.CAP_READY else "READY_WITH_LIMITATIONS",
             "evidence": "derived from pit_market_cap; unrunnable until it existed"},
        ],
        "families_unchanged": [
            "historical prices", "historical membership",
            "SEC company facts PIT", "PIT filing availability",
            "PIT trailing beta", "PIT size/liquidity", "PIT volatility",
            "tournament registry", "delisted/inactive identity",
            "owned daily OHLC", "analyst current snapshots", "forward evidence",
            "HOC/reassessment outcomes", "historical analyst revisions",
            "pre-2009 fundamentals (INVALID_FOR_HISTORICAL_RESEARCH)",
        ],
        "new_capability": {
            "name": "point-in-time market equity",
            "owner": "alpha_agent.pit_market_equity",
            "why_it_is_new": "not a re-reading of owned data but a genuinely "
                             "new derived quantity: two gaps had to close "
                             "TOGETHER before it existed at all",
        },
    }


def _first_error(*statuses) -> Optional[str]:
    for label, st in statuses:
        if not (st or {}).get("ok"):
            return "%s: %s" % (label, (st or {}).get("reason") or "UNAVAILABLE")
    return None


def run(*, research_root=None, mom_panel=None, identity_db=None, cf_index=None,
        issuer_db=None, shares_index=None, price_surface=None, fsds_cache=None,
        tournament_cfg_path=None, tournament_db=None,
        activate_shadow: bool = True, evidence_date: Optional[str] = None) -> dict:
    """Execute the Stage-26 research contract end to end.

    Read-only with respect to every operational store. Writes only the Stage-26
    research root, the Stage-26 share index, and - when ``activate_shadow`` - the
    EXISTING tournament registry and shadow-book root, which are the canonical
    research lifecycle rather than new stores.
    """
    from . import tournament as _t

    root = _resolve(research_root, RESEARCH_ROOT_ENV, DEFAULT_RESEARCH_ROOT)
    cfg = _t.load_config(tournament_cfg_path or
                         r"C:\Users\binis\paper_trader\configs\alpha_agent"
                         r"\stage9_tournament.json")

    # ---- data layer -------------------------------------------------------- #
    universe = _s24.HistoricalUniverse.from_momentum_panel(mom_panel)
    ucontract = universe.contract()
    bridge = _s24.IdentityBridge(identity_db)
    bridge_load = bridge.load()
    store = _s25.Stage25PitStore(cf_index)
    store_load = store.load()
    if not store_load.get("ok"):
        return {"ok": False, "token": BLOCKED, "reason": store_load.get("reason")}
    ciks = set(bridge.symbol_to_cik.values())
    sectors = _s25.SectorHistory(issuer_db)
    tier_b_load = sectors.load_entity_sic(ciks)
    history = PitSicHistory(fsds_cache)
    fsds_load = history.load(ciks)
    shares = _pme.PitShareCounts(
        _resolve(shares_index, SHARES_INDEX_ENV, DEFAULT_SHARES_INDEX))
    shares_load = shares.load(ciks)
    prices = _pme.UnadjustedPriceSurface(
        _resolve(price_surface, PRICE_SURFACE_ENV, DEFAULT_PRICE_SURFACE))
    price_load = prices.load()
    blocker = _first_error(("PIT_FILING_SIC", fsds_load),
                           ("PIT_SHARE_COUNTS", shares_load),
                           ("UNADJUSTED_PRICES", price_load))
    if blocker:
        return {"ok": False, "token": BLOCKED, "reason": blocker}
    equity = _pme.PitMarketEquity(shares, prices)
    beta = _s25.TrailingBeta(universe)

    panel = build_panel(universe, bridge, store, sectors, beta, equity, history)
    if not panel.months:
        return {"ok": False, "token": DATA_HOLD,
                "reason": "NO_PIT_CROSS_SECTIONS_ASSEMBLED"}

    # ---- baselines on the SHARED cross-section ------------------------------ #
    comp = panel.composite_cross_sections()
    mom = panel.momentum_cross_sections()
    ens = blend_cross_sections([comp, mom])
    op_prof_spec = _s25.factor_by_name(FROZEN_CHALLENGER)
    op_prof = panel.factor_cross_sections(op_prof_spec)
    baselines_periods = {_s25.BASELINE_COMPOSITE: comp,
                         _s25.BASELINE_MOMENTUM: mom,
                         _s25.BASELINE_ENSEMBLE: ens,
                         FROZEN_CHALLENGER: op_prof}
    baselines: "dict[str, dict]" = {}
    for name, periods in baselines_periods.items():
        res = score_cross_sections(periods, feature=name)
        g = gate_for(res["row"], cfg, survivorship_safe=True,
                     point_in_time_valid=True)
        baselines[name] = {"name": name, "periods_scored": len(periods),
                           "row": res["row"], "metrics": g["metrics"],
                           "gate": g["gate"]}
    champ_returns = score_cross_sections(
        comp, feature=_s25.BASELINE_COMPOSITE)["series"].get("long_short_by_date")

    # ---- WS3/WS4: sector ---------------------------------------------------- #
    coverage = pit_sector_coverage(panel, history, tier_b_sectors=sectors)
    revalidation = sector_revalidation(panel)

    # ---- WS5/WS6: market equity and the valuation campaign ------------------ #
    market_cap = pit_market_cap_capability(equity=equity,
                                           panel_diagnostics=panel.diagnostics)
    manifest = valuation_hypothesis_manifest()
    results = run_valuation_campaign(panel, cfg=cfg,
                                     champion_returns=champ_returns)
    fdr = _s25.apply_fdr(results, family=FAMILY_VALUATION)
    cleared = [r["name"] for r in results
               if (r.get("gate") or {}).get("target_state") == "KEEP_FOR_RESEARCH"]
    survivors = list(fdr.get("survivors_q10") or [])

    # ---- WS7: incrementality ------------------------------------------------ #
    # Gate-clearing candidates are the ones incrementality exists to judge. But
    # a family in which NOTHING clears still owes an answer to the question the
    # workstream was set to ask - is a valuation signal new information, or the
    # challenger wearing a price denominator? - so the strongest few are measured
    # too, explicitly labelled as a post-campaign diagnostic and NOT folded into
    # the FDR family.
    interesting = sorted(set(cleared) | set(survivors))
    by_abs_t = sorted(results,
                      key=lambda r: -abs((r.get("row") or {}).get("rank_ic_t")
                                         or 0.0))
    diagnostic = [r["name"] for r in by_abs_t[:3]]
    for pinned in ("s26_operating_profit_to_market", "s26_book_to_market"):
        if pinned not in diagnostic:
            diagnostic.append(pinned)
    diagnostic = [n for n in diagnostic if n not in interesting]
    incr = valuation_incrementality(
        panel, names=sorted(set(interesting) | set(diagnostic)),
        baselines={k: v for k, v in baselines_periods.items()}, cfg=cfg)
    incr["gate_clearing_candidates"] = interesting
    incr["post_campaign_diagnostics"] = sorted(diagnostic)
    incr["diagnostic_label"] = "POST_CAMPAIGN_DIAGNOSTIC_NOT_IN_FDR_FAMILY"
    incr["diagnostic_disclosure"] = (
        "the diagnostic names were chosen AFTER results were read, by absolute "
        "IC t-statistic, plus two pinned comparisons stated in source. They are "
        "reported for interpretation only: none is a candidate, none is "
        "registered, and none enters the multiple-testing family.")

    # ---- WS8: bounded ensembles --------------------------------------------- #
    # Which candidates are OFFERED is a model-selection decision driven by the
    # released gate, the FDR survivor list and the independence classification -
    # never by ensemble performance.
    picks: "list[tuple]" = [(FROZEN_CHALLENGER, op_prof)]
    for name in interesting:  # only GATE-CLEARING candidates are ever offered
        cls = ((incr.get("candidates") or {}).get(name) or {}).get(
            "classification")
        if cls == "INDEPENDENT_ALPHA" and name in survivors:
            spec = valuation_factor_by_name(name)
            picks.append((name, panel.factor_cross_sections(spec)))
            break
    references = [("s24_rnd_intensity",
                   panel.factor_cross_sections(_s25.RND_INTENSITY))]
    ensembles = next_generation_ensembles(panel, comp=comp, mom=mom, picks=picks,
                                          references=references, cfg=cfg)

    # ---- WS1/WS2/WS9: freeze and start the forward lane ---------------------- #
    registry = _t.CandidateRegistry(
        tournament_db or cfg.get("tournament_db"))
    try:
        row = next((c for c in registry.list()
                    if c.get("name") == FROZEN_CHALLENGER), None)
        frozen = challenger_freeze_contract(
            registry_row=row,
            stage25_evidence={"source": "stage25 released artefacts",
                              "run_id": "stage25_f811c142f7dbd7e0"},
            data_fingerprints={
                "momentum_panel": ucontract.get("source_fingerprint"),
                "companyfacts_index": {k: store_load.get(k)
                                       for k in ("facts", "ciks")},
                "concept_mapping_version_hash": _s25.mapping_version_hash(),
                "sic_mapping_version_hash": _ps.mapping_version_hash(),
            },
            ensemble_row=next((r for r in ensembles.get("structures", [])
                               if r.get("name") ==
                               "operational_plus_%s" % FROZEN_CHALLENGER), None))
        activation = _activate_forward_lane(
            registry, cfg, panel=panel, universe=universe, bridge=bridge,
            store=store, prices=prices, frozen=frozen,
            candidate_row=row, enabled=activate_shadow,
            evidence_date=evidence_date)
        counts = registry.counts_by_state()
        books = registry.list_shadow_books()
    finally:
        registry.close()

    readiness = shadow_forward_readiness(
        registry_counts=counts, shadow_books=books, frozen=frozen,
        activation=activation, eligible=[FROZEN_CHALLENGER],
        not_eligible={"s24_rnd_intensity": "CONCENTRATION_FRAGILE"},
        providers_wired=activation.get("providers_wired"))
    forward = forward_evidence_contract(readiness=readiness, frozen=frozen)

    # ---- WS10-14: governance, frontier, purchase gate ------------------------ #
    hoc = hoc_forward_contract()
    intrinio = intrinio_status()
    valuation_outcome = _valuation_outcome(results, cleared, survivors, incr)
    caps = capability_map(sector_coverage=coverage, market_cap=market_cap,
                          shares_status=shares_load, price_status=price_load,
                          fsds_status=fsds_load)
    frontier = new_information_frontier(
        sector_status={"state": _s25.CAP_READY}, market_cap_status=market_cap,
        valuation_outcome=valuation_outcome, capability=caps)
    exhaustion = research_exhaustion_update(
        valuation_results=results, valuation_fdr=fdr,
        sector_status="READY", market_cap_status=market_cap["stage26_state"],
        challenger_state=activation.get("challenger_state",
                                        "RESEARCH_CHALLENGER / FORWARD_TRACK"),
        frontier=frontier)
    gate = external_data_purchase_gate(frontier=frontier, exhaustion=exhaustion,
                                       valuation_outcome=valuation_outcome,
                                       intrinio=intrinio)

    payload = {
        "stage26_capability_map": caps,
        "challenger_freeze_contract": frozen,
        "shadow_forward_readiness": readiness,
        "pit_sic_acquisition_manifest": history.acquisition_manifest(),
        "pit_sector_coverage": coverage,
        "pit_sector_revalidation": revalidation,
        "pit_market_cap_capability": market_cap,
        "valuation_hypothesis_manifest": manifest,
        "valuation_experiment_results": {
            "contract_id": "stage26_valuation_experiment_results/1",
            "family": FAMILY_VALUATION,
            "baselines": {k: {"row": v["row"], "gate": v["gate"],
                              "periods_scored": v["periods_scored"]}
                          for k, v in baselines.items()},
            "results": [compact_result(r) for r in results],
            "fdr": fdr,
            "cleared_gate": cleared,
            "survived_fdr_10pct": survivors,
            "panel_diagnostics": panel.diagnostics,
            "universe_contract": ucontract,
        },
        "valuation_incrementality": incr,
        "ensemble_results": ensembles,
        "challenger_tournament_status": {
            "contract_id": "stage26_challenger_tournament_status/1",
            "registry_counts_by_state": counts,
            "shadow_books": books,
            "automatic_promotion_possible": False,
            "operational_champion": "fundamental_momentum_50_50_v1 (UNCHANGED)",
            "activation": activation,
        },
        "forward_evidence_contract": forward,
        "hoc_forward_contract": hoc,
        "research_exhaustion_update": exhaustion,
        "intrinio_status": intrinio,
        "new_information_frontier": frontier,
        "external_data_purchase_gate": gate,
    }
    payload["stage26_summary"] = _summary(
        payload=payload, panel=panel, results=results, fdr=fdr,
        coverage=coverage, revalidation=revalidation, market_cap=market_cap,
        activation=activation, ensembles=ensembles)
    written = _write_artifacts(root, payload)
    return {"ok": True, "token": READY, "research_root": str(root),
            "run_dir": written["run_dir"], "artifacts": written["artifacts"],
            "run_id": written["run_id"], "payload": payload}


def _activate_forward_lane(registry, cfg, *, panel, universe, bridge, store,
                           prices, frozen, candidate_row, enabled: bool,
                           evidence_date: Optional[str]) -> dict:
    """Rank the frozen challenger at inception and enrol it through the CANONICAL
    activator. Adds no lifecycle of its own."""
    from . import tournament as _t

    out: dict = {
        "enabled": bool(enabled),
        "owner": "alpha_agent.tournament.maybe_activate_shadow_books",
        "allowlist_enforced": bool((cfg.get("shadow_books") or {}).get(
            _t.SHADOW_ALLOWLIST_REQUIRED_KEY)),
        "eligible_candidate_ids": [],
        "excluded_candidates": {},
        "providers_wired": {"inception_provider": False, "mark_provider": False},
    }
    if not candidate_row:
        out["status"] = "CANDIDATE_NOT_IN_REGISTRY"
        return out
    cid = str(candidate_row["candidate_id"])
    out["eligible_candidate_ids"] = [cid]
    for c in registry.list(state=_t.KEEP_FOR_RESEARCH):
        if str(c["candidate_id"]) != cid:
            out["excluded_candidates"][str(c["candidate_id"])] = {
                "name": c.get("name"),
                "combined_score": c.get("combined_score"),
                "reason": "CONCENTRATION_FRAGILE - clears the score floor but "
                          "its own falsification battery does not clear the "
                          "pre-registered survival rule",
            }

    # -- rank at INCEPTION, from data available at inception ------------------ #
    inception_date = evidence_date or registry.now()[:10]
    spec = _s25.factor_by_name(FROZEN_CHALLENGER)
    months = universe.months()
    formation_month = months[-1] if months else None
    ranked: "list[tuple]" = []
    entry_prices: dict = {}
    if formation_month:
        as_of = _shift_days(inception_date, REPORTING_LAG_DAYS)
        for sym in sorted(universe.eligible(formation_month) or {}):
            cik = bridge.cik_for(sym)
            if cik is None:
                continue
            rec = _s25.annual_record(store, cik, as_of)
            if rec is None:
                continue
            v = spec.value(rec)
            if v is None:
                continue
            px = prices.closes_as_of(sym, inception_date)
            if not px:
                continue
            ranked.append((sym, float(v) * spec.direction))
            entry_prices[sym] = px["close_none"]
    out["formation_month"] = formation_month
    out["inception_date"] = inception_date
    out["names_ranked_at_inception"] = len(ranked)
    if not enabled:
        out["status"] = "NOT_ACTIVATED_BY_CONFIGURATION"
        return out
    if len(ranked) < 2 * SHADOW_LEG_SIZE:
        out["status"] = "INSUFFICIENT_NAMES_AT_INCEPTION"
        return out

    inception_provider = make_shadow_inception_provider(
        ranked_by_candidate={cid: ranked}, entry_prices=entry_prices,
        frozen_spec={"spec_hash": frozen.get("spec_hash"),
                     "name": FROZEN_CHALLENGER,
                     **(frozen.get("standalone") or {})},
        formation_month=formation_month)
    out["providers_wired"]["inception_provider"] = True
    out["providers_wired"]["mark_provider"] = True
    out["providers_wired"]["mark_provider_owner"] = (
        "alpha_agent.stage26_challenger_expansion.make_shadow_mark_provider, "
        "wired into alpha_agent.runtime._tournament_mark_provider")

    activated = _t.maybe_activate_shadow_books(
        registry, cfg, inception_provider=inception_provider,
        evidence_date=inception_date, eligible_candidate_ids=[cid])
    out["activated"] = activated
    # Activation is idempotent: a candidate that already carries a book is
    # skipped, and re-running the stage must not restate the inception or reset
    # the forward clock. Distinguish "opened now" from "already open" so a
    # re-run reads honestly instead of looking like a failure.
    existing = next((b for b in registry.list_shadow_books()
                     if b.get("candidate_id") == cid
                     and b.get("status") == "ACTIVE"), None)
    if activated:
        out["status"] = "SHADOW_BOOK_ACTIVE"
    elif existing:
        out["status"] = "SHADOW_BOOK_ALREADY_ACTIVE"
        out["existing_book"] = existing
        out["inception_date"] = existing.get("inception_date")
    else:
        out["status"] = "NOT_ACTIVATED"
    out["challenger_state"] = (
        "RESEARCH_CHALLENGER / FORWARD_TRACK"
        if (activated or existing) else "RESEARCH_CHALLENGER")
    out["marks_written_by_stage26"] = 0
    out["why_zero_marks"] = (
        "a mark may only exist for a date strictly AFTER inception. Writing one "
        "today would be backdating the very evidence the book exists to collect.")
    return out


def _valuation_outcome(results, cleared, survivors, incr) -> dict:
    independent = [n for n in survivors
                   if ((incr.get("candidates") or {}).get(n) or {}).get(
                       "classification") == "INDEPENDENT_ALPHA"]
    redundant = [n for n in survivors if n not in independent]
    if independent:
        state, summary = "ACTIVE", (
            "%d of %d valuation hypotheses cleared the released gate, %d "
            "survived FDR, and %d of those carry information the operational "
            "model does not already have"
            % (len(cleared), len(results), len(survivors), len(independent)))
    elif survivors:
        state, summary = "COMPLETE", (
            "%d of %d survived FDR but every survivor is REDUNDANT with a signal "
            "already in the model" % (len(survivors), len(results)))
    else:
        state, summary = "COMPLETE", (
            "0 of %d valuation hypotheses survived the released gate and FDR"
            % len(results))
    return {"state": state, "summary": summary, "cleared_gate": cleared,
            "survived_fdr": survivors, "independent": independent,
            "redundant": redundant}


def _summary(*, payload, panel, results, fdr, coverage, revalidation,
             market_cap, activation, ensembles) -> dict:
    best = next((r for r in ensembles.get("structures", [])
                 if r.get("name") == ensembles.get("best_by_rank_ic_t")), None)
    return {
        "contract_id": "stage26_summary/1",
        "stage": "26", "version": STAGE26_VERSION, "origin": ORIGIN,
        "safety_badges": SAFETY_BADGES,
        "operational_mutations": 0,
        "model_promotion": False,
        "automatic_promotion_possible": False,
        "operational_champion": "fundamental_momentum_50_50_v1 (UNCHANGED)",
        "panel": {
            "formations": len(panel.months),
            "first_month": panel.months[0] if panel.months else None,
            "last_month": panel.months[-1] if panel.months else None,
            "scored_rows": panel.diagnostics.get("scored_rows"),
            "market_equity_coverage_fraction": (
                (panel.diagnostics.get("market_equity_coverage") or {})
                .get("coverage_fraction")),
        },
        "lane_a_forward": {
            "frozen_challenger": FROZEN_CHALLENGER,
            "frozen_spec_hash": payload["challenger_freeze_contract"].get(
                "spec_hash"),
            "shadow_activation_status": activation.get("status"),
            "shadow_inception_date": activation.get("inception_date"),
            "forward_marks_written": activation.get("marks_written_by_stage26", 0),
        },
        "lane_b_information": {
            "pit_sector_tier_c_coverage": coverage.get("coverage_fraction"),
            "pit_sector_vs_tier_b_agreement": (coverage.get("vs_tier_b") or {})
            .get("agreement_fraction"),
            "sector_verdict_effects": {
                k: v.get("effect_of_leakage_safe_fine_sector")
                for k, v in (revalidation.get("factors") or {}).items()},
            "pit_market_cap_state": market_cap.get("stage26_state"),
            "valuation_family_size": len(results),
            "valuation_cleared_gate": payload[
                "valuation_experiment_results"]["cleared_gate"],
            "valuation_survived_fdr": list(fdr.get("survivors_q10") or []),
        },
        "best_research_ensemble": (best or {}).get("name"),
        "best_research_ensemble_matched_delta": (best or {}).get(
            "delta_vs_operational_shape_matched_universe"),
        "purchase_gate_verdict": payload["external_data_purchase_gate"][
            "verdict"],
        "next_major_constraint": _next_constraint(payload, activation),
    }


def _next_constraint(payload: dict, activation: dict) -> dict:
    """The one thing that most binds progress after this stage.

    Deliberately NOT another factor test. The question is what class of input is
    now scarcest, and the honest answer follows from the evidence rather than
    from preference.
    """
    free_open = [f for f in payload["new_information_frontier"].get(
        "still_open_free_or_owned", []) if f.get("blocker") == "NONE - runnable now"]
    activated = activation.get("status") == "SHADOW_BOOK_ACTIVE"
    if free_open:
        constraint = "NEW_FREE_INFORMATION"
        why = ("%d economically distinct families are runnable TODAY at zero "
               "cost on data already on disk, two of which this stage's own "
               "acquisition created. Paying for information while free "
               "information sits unused is the wrong trade, and another "
               "single-factor test inside a closed family is the wrong work."
               % len(free_open))
    elif activated:
        constraint = "FORWARD_TIME"
        why = ("the frozen challenger's historical case is as strong as history "
               "can make it and the free/owned surface is spent; what remains "
               "unknown is out-of-sample behaviour, and only elapsed calendar "
               "time produces that")
    else:
        constraint = "PORTFOLIO_DECISION_EVIDENCE"
        why = ("the forward lane is not accumulating, so neither history nor "
               "new data is the binding constraint - starting the evidence is")
    return {
        "constraint": constraint,
        "why": why,
        "secondary": "FORWARD_TIME" if constraint != "FORWARD_TIME" else
                     "NEW_FREE_INFORMATION",
        "explicitly_not": "another single-factor accounting test; five of six "
                          "such families are closed with evidence",
    }


def _write_artifacts(root: Path, payload: dict) -> dict:
    """Content-addressed run directory; identical inputs reproduce the run id."""
    run_id = "stage26_%s" % content_hash(canonical_json(
        {k: v for k, v in payload.items() if k != "stage26_summary"}))[:16]
    run_dir = Path(root) / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    written = {}
    for name, doc in sorted(payload.items()):
        p = run_dir / ("%s.json" % name)
        text = json.dumps(doc, indent=1, sort_keys=True, default=str)
        p.write_text(text, encoding="utf-8")
        written[name] = {"path": str(p), "bytes": len(text.encode("utf-8")),
                         "sha256": content_hash(text)}
    latest = Path(root) / "latest.json"
    latest.write_text(json.dumps({"run_id": run_id, "run_dir": str(run_dir),
                                  "stage": "26", "version": STAGE26_VERSION},
                                 indent=1, sort_keys=True), encoding="utf-8")
    return {"run_id": run_id, "run_dir": str(run_dir), "artifacts": written}


__all__ = [
    "STAGE26_VERSION", "ORIGIN", "CONTRACT_ID", "READY", "BLOCKED", "DATA_HOLD",
    "SAFETY_BADGES", "TIER_A", "TIER_B", "TIER_C", "TIER_C_CONTRACT",
    "PitSicHistory", "pit_sector_coverage", "sector_revalidation",
    "SECTOR_AFFECTED", "SURVIVE_MIN_T", "SURVIVE_MIN_RETENTION",
    "VALUATION_FACTORS", "VALUATION_BY_NAME", "valuation_factor_by_name",
    "valuation_hypothesis_manifest", "FAMILY_VALUATION", "FAM_VALUATION",
    "build_panel", "ALL_STAGE26_FACTORS", "run_valuation_campaign",
    "compact_result", "FROZEN_CHALLENGER", "FROZEN_CHALLENGER_CANDIDATE_ID",
    "challenger_freeze_contract", "build_shadow_membership", "shadow_book_nav",
    "make_shadow_mark_provider", "make_shadow_inception_provider",
    "SHADOW_LEG_SIZE", "SHADOW_MIN_MARK_COVERAGE",
    "shadow_forward_readiness", "forward_evidence_contract",
    "pit_market_cap_capability", "valuation_incrementality",
    "next_generation_ensembles", "hoc_forward_contract",
    "new_information_frontier", "research_exhaustion_update",
    "external_data_purchase_gate", "intrinio_status", "capability_map", "run",
]


def pit_sector_coverage(panel: "_s25.Stage25Panel", history: "PitSicHistory", *,
                        tier_b_sectors: "_s25.SectorHistory" = None) -> dict:
    """Measure Tier C against the panel it will be used to control.

    Reports date/issuer/formation coverage, the unknown rate, classification
    stability, delisted-name coverage and — the number that decides whether the
    Stage-25 provisional verdicts can be upgraded — the agreement between the
    leakage-safe Tier C and the look-ahead Tier B on the SAME rows.
    """
    rows = 0
    known = 0
    tier_c_counts: "dict[str, int]" = {}
    agree = 0
    comparable = 0
    per_month: "dict[str, dict]" = {}
    by_cik_sectors: "dict[str, set]" = {}
    delisted_rows = 0
    delisted_known = 0
    first_seen: Optional[str] = None
    last_seen: Optional[str] = None
    for m in panel.months:
        as_of = _shift_days(panel.formation_dates[m], REPORTING_LAG_DAYS)
        mk = mn = 0
        for sym, r in panel.rows.get(m, {}).items():
            rows += 1
            mn += 1
            c = history.sector_as_of(r["cik"], as_of)
            tier_c_counts[c] = tier_c_counts.get(c, 0) + 1
            is_delisted = "-" in sym
            if is_delisted:
                delisted_rows += 1
            if c != _ps.UNKNOWN:
                known += 1
                mk += 1
                if is_delisted:
                    delisted_known += 1
                by_cik_sectors.setdefault(r["cik"], set()).add(c)
                first_seen = m if first_seen is None else min(first_seen, m)
                last_seen = m if last_seen is None else max(last_seen, m)
            b = (r.get("sectors") or {}).get(TIER_B, _ps.UNKNOWN)
            if c != _ps.UNKNOWN and b != _ps.UNKNOWN:
                comparable += 1
                if c == b:
                    agree += 1
        per_month[m] = {"names": mn, "classified": mk,
                        "coverage": round(mk / mn, 6) if mn else 0.0}
    multi = sum(1 for s in by_cik_sectors.values() if len(s) > 1)
    months_full = sum(1 for v in per_month.values() if v["coverage"] >= 0.95)
    return {
        "contract_id": "stage26_pit_sector_coverage/1",
        "tier": TIER_C,
        "tier_contract": TIER_C_CONTRACT,
        "panel_rows": rows,
        "classified_rows": known,
        "coverage_fraction": round(known / rows, 6) if rows else 0.0,
        "unknown_rate": round(1.0 - (known / rows), 6) if rows else 1.0,
        "formations": len(panel.months),
        "formations_at_or_above_95pct": months_full,
        "first_classified_month": first_seen,
        "last_classified_month": last_seen,
        "delisted_rows": delisted_rows,
        "delisted_classified": delisted_known,
        "delisted_coverage_fraction": (round(delisted_known / delisted_rows, 6)
                                       if delisted_rows else None),
        "issuers_classified": len(by_cik_sectors),
        "issuers_reclassified_across_panel": multi,
        "classification_stability": (
            round(1.0 - multi / len(by_cik_sectors), 6)
            if by_cik_sectors else None),
        "sector_distribution": dict(sorted(tier_c_counts.items(),
                                           key=lambda kv: -kv[1])),
        "vs_tier_b": {
            "comparable_rows": comparable,
            "agreeing_rows": agree,
            "agreement_fraction": round(agree / comparable, 6) if comparable
            else None,
            "reading": (
                "disagreement is Tier B's look-ahead showing through: Tier B "
                "carries the issuer's CURRENT SIC on every historical row, so a "
                "reclassified issuer disagrees with its own contemporaneous "
                "filings for every row before the change"),
        },
        "per_month": per_month,
        "acquisition": history.acquisition_manifest(),
    }
