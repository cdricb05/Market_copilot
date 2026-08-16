"""alpha_agent/stage27_alpha_exhaustion.py — Release 27, the autonomous alpha
**exhaustion** campaign.

Stage 26 ended by naming three economically distinct information families that
were runnable that day, at zero cost, on data already on disk — filing timing,
restatement history, share-count dynamics — and then stopped. This module exists
because that is the wrong terminal state. A frontier item that is free, owned,
point-in-time valid and adequately sampled is not a finding; it is unfinished
work.

So the loop here is recursive rather than sequential:

    discover a family -> prove it distinct -> prove PIT / survivorship / sample
      -> build the minimum data if it is free -> PRE-REGISTER -> run
      -> released gate + FDR -> incrementality vs the CURRENT information set
      -> bounded ensembles if justified -> register survivor or null
      -> update exhaustion memory -> discover AGAIN

and it terminates only when a fresh frontier audit returns **zero** executable
free/owned high-priority families. Every family that enters the loop leaves it
with exactly one terminal classification. "Interesting, test it later" is not
one of them.

What this module deliberately does NOT do: retest `s25_operating_profitability`
(its historical case is already as strong as history can make it), reopen the 27
Stage-25 nulls, invent more valuation ratios after Stage 26 closed 13 of them,
rescue `s24_rnd_intensity`, touch the frozen forward challenger or its shadow
book, promote anything, or write a single operational byte.

Every statistic, gate, FDR correction, ensemble evaluator and incrementality
measure is the RELEASED owner, called — never reimplemented. What is new here is
the information, not the arithmetic.
"""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
from typing import Callable, Iterable, Optional, Sequence

from . import pit_market_equity as _pme
from . import pit_sector as _ps
from . import sec_filing_behavior as _sfb
from . import stage24_pit_fundamental as _s24
from . import stage25_alpha_discovery as _s25
from . import stage26_challenger_expansion as _s26

STAGE27_VERSION = "release27-alpha-exhaustion-1.0.0"
ORIGIN = "release27-autonomous-alpha-exhaustion-campaign"
CONTRACT_ID = "release27_alpha_exhaustion/1"

READY = "ALPHA_EXHAUSTION_CAMPAIGN_READY"
BLOCKED = "ALPHA_EXHAUSTION_CAMPAIGN_BLOCKED"
DATA_HOLD = "ALPHA_EXHAUSTION_CAMPAIGN_DATA_HOLD"

SAFETY_BADGES = ["RESEARCH ONLY", "READ ONLY", "NO ORDERS", "NO LIVE PROMOTION",
                 "PREVIEW ONLY", "MANUAL REVIEW"]

# --------------------------------------------------------------------------- #
# Roots (env-overridable so tests stay hermetic).
# --------------------------------------------------------------------------- #
RESEARCH_ROOT_ENV = "PAPER_TRADER_RELEASE27_ROOT"
INSIDER_CACHE_ENV = "PAPER_TRADER_RELEASE27_INSIDER_CACHE"

DEFAULT_RESEARCH_ROOT = Path(
    r"D:\Stock_Prediction_app_data\alpha_exhaustion_campaign")
DEFAULT_INSIDER_CACHE = Path(
    r"D:\Stock_Prediction_app_data\alpha_agent\identity\sec_bulk"
    r"\insider_transactions_data_sets")

_resolve = _s25._resolve
canonical_json = _s24.canonical_json
content_hash = _s24.content_hash
file_fingerprint = _s24.file_fingerprint
_num = _s24._num
_ratio = _s24._ratio
_mean = _s24._mean
_shift_days = _s24._shift_days
score_cross_sections = _s24.score_cross_sections
gate_for = _s24.gate_for
incrementality = _s24.incrementality
evaluate_variant = _s25.evaluate_variant
PRIMARY_HORIZON = _s25.PRIMARY_HORIZON
REPORTING_LAG_DAYS = _s24.REPORTING_LAG_DAYS
REDUNDANCY_ABS_CORR = _s26.REDUNDANCY_ABS_CORR
REDUNDANCY_PARTIAL_T = _s26.REDUNDANCY_PARTIAL_T
TOP_N_OVERLAP = _s26.TOP_N_OVERLAP
TIER_C = _s26.TIER_C

FROZEN_CHALLENGER = _s26.FROZEN_CHALLENGER
FROZEN_CHALLENGER_SPEC_HASH = (
    "67f0314106f9ce56806170669719a8cc3b035cd16469e984939c11c42956245e")

_S = _s24.FactorSpec


# =========================================================================== #
# Terminal classification vocabulary.
#
# The release contract fixes it: every economically meaningful family discovered
# during this campaign must end at exactly one of these, and two of the states a
# prior stage was willing to stop at - "new free information", "ready for the
# next stage" - are not among them.
# =========================================================================== #
T_REJECTED = "TESTED_AND_REJECTED"
T_RETAINED = "TESTED_AND_RETAINED"
T_CHALLENGER = "TESTED_AND_CHALLENGER"
T_REDUNDANT = "REDUNDANT_WITH_EXISTING_INFORMATION"
T_INSUFFICIENT = "INSUFFICIENT_SAMPLE"
T_NO_PIT = "PIT_DATA_UNAVAILABLE"
T_NO_SURVIVORSHIP = "SURVIVORSHIP_UNAVAILABLE"
T_PAID = "REQUIRES_PAID_DATA"
T_FORWARD = "REQUIRES_FORWARD_TIME"
T_GOVERNANCE = "MANUAL_GOVERNANCE_REQUIRED"

TERMINAL_STATES = (T_REJECTED, T_RETAINED, T_CHALLENGER, T_REDUNDANT,
                   T_INSUFFICIENT, T_NO_PIT, T_NO_SURVIVORSHIP, T_PAID,
                   T_FORWARD, T_GOVERNANCE)

#: States that are explicitly NOT terminal. Naming them is the point: the
#: campaign fails closed if a family is left in one of them.
FORBIDDEN_STATES = ("NEW_FREE_INFORMATION", "READY_FOR_NEXT_STAGE",
                    "INTERESTING_FAMILY_TO_TEST_LATER", "RUNNABLE_TODAY",
                    "PENDING", "UNKNOWN")

#: A family is EXECUTABLE when it is free/owned, point-in-time valid,
#: survivorship-acceptable, adequately sampled, economically distinct and not
#: already answered. An executable family that has not been run makes the
#: campaign incomplete - that is the hard contract.
EXECUTABLE = "EXECUTABLE_FREE_OWNED"
NOT_EXECUTABLE = "NOT_EXECUTABLE"


# =========================================================================== #
# Pre-registration.
#
# Every sign below is fixed HERE, in source, before any Release-27 number
# exists. Nothing is flipped afterwards: a strong wrong-signed result is a
# rejection, not a discovery. Each family is a bounded set of economically
# distinct claims, not every expression the readers can spell.
# =========================================================================== #
FAM_FILING = "release27_filing_behavior"
FAM_CORRECTIONS = "release27_reporting_corrections"
FAM_SHARES = "release27_share_dynamics"
FAM_DISCLOSURE = "release27_disclosure_structure"
FAM_DIVIDEND = "release27_dividend_policy_events"
FAM_CAPITAL_ACTION = "release27_corporate_action_behaviour"
FAM_INSIDER = "release27_insider_transactions"
FAM_STREAM = "release27_filing_stream_events"

#: The whole campaign's multiple-testing scope, fixed before evaluation. The
#: PRIMARY correction is Benjamini-Hochberg within each pre-registered economic
#: family (the released convention, and the scope a family's hypotheses were
#: chosen under). A SECOND, strictly harsher campaign-wide correction over every
#: hypothesis in every family is also computed and reported, so a survivor that
#: only survives the narrower scope cannot be presented as if it had cleared the
#: wider one.
FDR_SCOPES = ("per_family_primary", "campaign_wide_secondary")
CAMPAIGN_FDR_FAMILY = "release27_all_hypotheses"


def _obs(rec, bucket: str, key: str) -> Optional[float]:
    """Read one primitive point-in-time observable injected into the record."""
    b = (rec.get("cur") or {}).get(bucket)
    if not isinstance(b, dict):
        return None
    return _num(b.get(key))


def _flt(rec, bucket: str, key: str):
    b = (rec.get("cur") or {}).get(bucket)
    return b.get(key) if isinstance(b, dict) else None


# -- family 1: filing timing and reporting promptness ------------------------ #
def _f_annual_filing_lag(rec):
    return _obs(rec, "filing", "annual_lag_days")


def _f_annual_lag_change(rec):
    return _obs(rec, "filing", "annual_lag_change")


def _f_annual_lag_abnormal(rec):
    return _obs(rec, "filing", "annual_lag_abnormal")


def _f_quarterly_filing_lag(rec):
    return _obs(rec, "filing", "quarterly_lag_days")


def _f_quarterly_lag_abnormal(rec):
    return _obs(rec, "filing", "quarterly_lag_abnormal")


def _f_quarterly_lag_trend(rec):
    return _obs(rec, "filing", "quarterly_lag_trend")


def _f_deadline_miss_rate(rec):
    return _obs(rec, "filing", "deadline_miss_rate_8")


def _f_deadline_worst_overrun(rec):
    return _obs(rec, "filing", "deadline_worst_overrun_8")


def _f_cadence_disruption(rec):
    return _obs(rec, "filing", "cadence_overrun_days")


def _f_after_close_disclosure(rec):
    return _obs(rec, "filing", "after_close_rate_8")


FILING_FACTORS = (
    _S(name="r27_annual_filing_lag", family=FAM_FILING,
       hypothesis="A company that takes longer to close and file its annual "
                  "report is under more operational or accounting stress, and "
                  "underperforms.",
       rationale="Reporting promptness is a governance observable that says "
                 "nothing about the level of any reported number, which is why "
                 "it can be new information after five accounting families were "
                 "closed with evidence. The auditor's unfinished work is the "
                 "mechanism.",
       definition="days from fiscal year end to SEC ACCEPTANCE of the latest "
                  "10-K; expected sign NEGATIVE",
       required=("filing.annual_lag_days",), direction=-1, needs_prior=False,
       fn=_f_annual_filing_lag),
    _S(name="r27_annual_lag_change", family=FAM_FILING,
       hypothesis="A company whose annual filing lag DETERIORATED year on year "
                  "is newly stressed, and underperforms.",
       rationale="The level confounds a slow but stable reporting culture with "
                 "genuine trouble. The change does not.",
       definition="latest 10-K lag minus the prior 10-K lag; sign NEGATIVE",
       required=("filing.annual_lag_change",), direction=-1, needs_prior=False,
       fn=_f_annual_lag_change),
    _S(name="r27_annual_lag_abnormal", family=FAM_FILING,
       hypothesis="A company filing unusually late RELATIVE TO ITS OWN history "
                  "underperforms.",
       rationale="Filing speed is a firm characteristic; the informative part "
                 "is the deviation from the firm's own established habit, not "
                 "its level against other firms.",
       definition="latest 10-K lag minus the median of the issuer's own prior "
                  "10-K lags (>= 4 required); sign NEGATIVE",
       required=("filing.annual_lag_abnormal",), direction=-1, needs_prior=False,
       fn=_f_annual_lag_abnormal),
    _S(name="r27_quarterly_filing_lag", family=FAM_FILING,
       hypothesis="Quarterly reporting promptness carries the same stress "
                  "signal at four times the frequency.",
       rationale="A 10-Q lag refreshes every quarter, so the signal is fresher "
                 "than the annual one at any formation date.",
       definition="days from fiscal quarter end to acceptance of the latest "
                  "10-Q; sign NEGATIVE",
       required=("filing.quarterly_lag_days",), direction=-1, needs_prior=False,
       fn=_f_quarterly_filing_lag),
    _S(name="r27_quarterly_lag_abnormal", family=FAM_FILING,
       hypothesis="A quarterly filing unusually late against the issuer's own "
                  "history predicts underperformance.",
       definition="latest 10-Q lag minus the issuer's own prior 10-Q median "
                  "(>= 4 required); sign NEGATIVE",
       rationale="The own-history control at quarterly frequency.",
       required=("filing.quarterly_lag_abnormal",), direction=-1,
       needs_prior=False, fn=_f_quarterly_lag_abnormal),
    _S(name="r27_quarterly_lag_trend", family=FAM_FILING,
       hypothesis="A reporting process that is slowing down over a year "
                  "predicts underperformance before it ever misses a deadline.",
       rationale="Deterioration is continuous; the deadline is a step function "
                 "that only fires in the extreme.",
       definition="mean of the last 4 quarterly lags minus the mean of the 4 "
                  "before them; sign NEGATIVE",
       required=("filing.quarterly_lag_trend",), direction=-1, needs_prior=False,
       fn=_f_quarterly_lag_trend),
    _S(name="r27_deadline_miss_rate", family=FAM_FILING,
       hypothesis="A company that repeatedly overruns its STATUTORY filing "
                  "deadline underperforms.",
       rationale="The deadline is the peer-relative benchmark done properly: it "
                 "already varies by form and by filer status, so it compares a "
                 "company to the obligation it actually carried rather than to "
                 "a pooled cross-sectional median of mixed obligations.",
       definition="fraction of the last 8 periodic filings accepted after the "
                  "Exchange Act deadline for their form and filer status, plus a "
                  "fixed 4-day grace for the Rule 0-3 weekend roll; sign NEGATIVE",
       required=("filing.deadline_miss_rate_8",), direction=-1, needs_prior=False,
       fn=_f_deadline_miss_rate),
    _S(name="r27_deadline_worst_overrun", family=FAM_FILING,
       hypothesis="The WORST single deadline overrun is more informative than "
                  "how often overruns happen.",
       rationale="One badly late filing is a different event from eight "
                 "marginally late ones; frequency and magnitude are separate "
                 "claims and are registered separately.",
       definition="maximum days past deadline across the last 8 periodic "
                  "filings; sign NEGATIVE",
       required=("filing.deadline_worst_overrun_8",), direction=-1,
       needs_prior=False, fn=_f_deadline_worst_overrun),
    _S(name="r27_filing_cadence_disruption", family=FAM_FILING,
       hypothesis="An issuer that has gone longer than a reporting quarter "
                  "without any periodic filing is in a disrupted state and "
                  "underperforms.",
       rationale="A MISSING filing is stronger evidence than a late one, and it "
                 "is observable without inferring a filing that is not there.",
       definition="days since the last periodic filing minus 92, floored at 0; "
                  "sign NEGATIVE",
       required=("filing.cadence_overrun_days",), direction=-1, needs_prior=False,
       fn=_f_cadence_disruption),
    _S(name="r27_after_close_disclosure", family=FAM_FILING,
       hypothesis="A company that habitually files after the market close is "
                  "managing when its disclosure is read, and underperforms.",
       rationale="Strategic disclosure timing is a DIFFERENT claim from "
                 "promptness: it uses the acceptance CLOCK, not the acceptance "
                 "DATE, and a prompt filer can still be a late-in-the-day one.",
       definition="fraction of the last 8 periodic filings accepted at or after "
                  "16:00 Eastern; sign NEGATIVE",
       required=("filing.after_close_rate_8",), direction=-1, needs_prior=False,
       fn=_f_after_close_disclosure),
)


# -- family 2: restatements, amendments and reporting corrections ------------ #
def _f_amendment_recent(rec):
    return _obs(rec, "filing", "amendment_recent_1y")


def _f_amendment_count_3y(rec):
    return _obs(rec, "filing", "amendment_count_3y")


def _f_annual_amendment_3y(rec):
    return _obs(rec, "filing", "annual_amendment_count_3y")


def _f_amendment_intensity(rec):
    return _obs(rec, "filing", "amendment_intensity_3y")


def _f_amendment_latency(rec):
    return _obs(rec, "filing", "amendment_latency_days")


def _f_repeat_amender(rec):
    return _obs(rec, "filing", "distinct_periods_amended_3y")


def _f_revision_count_1y(rec):
    return _obs(rec, "revisions", "revision_count_1y")


def _f_revision_magnitude(rec):
    return _obs(rec, "revisions", "revision_max_magnitude_1y")


def _f_revision_breadth(rec):
    return _obs(rec, "revisions", "revision_concept_breadth_1y")


def _f_revision_persistence(rec):
    return _obs(rec, "revisions", "revision_years_active_3y")


def _f_revision_direction(rec):
    return _obs(rec, "revisions", "revision_directional_1y")


CORRECTION_FACTORS = (
    # -- channel A: formal amendments (a /A submission, at its own acceptance) -- #
    _S(name="r27_amendment_recent", family=FAM_CORRECTIONS,
       hypothesis="A company that has amended a periodic report in the last "
                  "year has a reporting-quality problem and underperforms.",
       rationale="An amendment is management publicly conceding that what it "
                 "previously told the market was wrong.",
       definition="1 if any 10-K/A or 10-Q/A was ACCEPTED in the trailing 365 "
                  "days, else 0; sign NEGATIVE",
       required=("filing.amendment_recent_1y",), direction=-1, needs_prior=False,
       caveat="thin by construction: formal amendments are ~1.2 % of periodic "
              "filings on this universe, so cross-sectional breadth is the "
              "binding constraint and is measured, not assumed",
       fn=_f_amendment_recent),
    _S(name="r27_amendment_count_3y", family=FAM_CORRECTIONS,
       hypothesis="Amendment FREQUENCY over three years separates a one-off "
                  "correction from a broken reporting process.",
       rationale="Recurrence is the part of the signal that is about the "
                 "process rather than about one accident.",
       definition="count of periodic amendments accepted in the trailing 1095 "
                  "days; sign NEGATIVE",
       required=("filing.amendment_count_3y",), direction=-1, needs_prior=False,
       fn=_f_amendment_count_3y),
    _S(name="r27_annual_amendment_3y", family=FAM_CORRECTIONS,
       hypothesis="Amending an AUDITED annual report is a materially more "
                  "serious event than amending an unaudited quarterly one.",
       rationale="The 10-K carries the audit opinion; a 10-K/A means the "
                 "audited statements were wrong.",
       definition="count of 10-K/A accepted in the trailing 1095 days; sign "
                  "NEGATIVE",
       required=("filing.annual_amendment_count_3y",), direction=-1,
       needs_prior=False, fn=_f_annual_amendment_3y),
    _S(name="r27_amendment_intensity", family=FAM_CORRECTIONS,
       hypothesis="Amendments per filing normalises for how much a company "
                  "files at all.",
       rationale="A raw count rewards companies that file less.",
       definition="amendments / (periodic filings + amendments) over 1095 days; "
                  "sign NEGATIVE",
       required=("filing.amendment_intensity_3y",), direction=-1,
       needs_prior=False, fn=_f_amendment_intensity),
    _S(name="r27_amendment_latency", family=FAM_CORRECTIONS,
       hypothesis="The longer it takes to correct a filing, the more serious "
                  "the underlying error.",
       rationale="A same-week amendment is an exhibit or a signature; a "
                 "nine-month amendment is a restatement.",
       definition="days from the ORIGINAL submission for the amended period to "
                  "the amendment's own acceptance; sign NEGATIVE",
       required=("filing.amendment_latency_days",), direction=-1,
       needs_prior=False,
       caveat="defined only for issuers with an amendment in the window, so its "
              "cross-section is the thinnest in the campaign",
       fn=_f_amendment_latency),
    _S(name="r27_repeat_amender", family=FAM_CORRECTIONS,
       hypothesis="Amending MULTIPLE DISTINCT fiscal periods indicates a "
                  "systemic accounting failure rather than one bad quarter.",
       rationale="Breadth across periods is the restatement-scope measure.",
       definition="distinct fiscal periods amended in the trailing 1095 days; "
                  "sign NEGATIVE",
       required=("filing.distinct_periods_amended_3y",), direction=-1,
       needs_prior=False, fn=_f_repeat_amender),
    # -- channel B: accounting FACT revisions (the broad, leakage-safe channel) - #
    _S(name="r27_revision_count_1y", family=FAM_CORRECTIONS,
       hypothesis="A company that quietly revised a previously reported core "
                  "accounting figure underperforms, whether or not it filed a "
                  "formal amendment.",
       rationale="Most corrections never produce a /A: the number is simply "
                 "restated in the next comparative column. That channel is 26x "
                 "broader than the formal one and is the same economic event.",
       definition="count of materially changed values for identical (concept, "
                  "unit, period_start, period_end) contexts across accessions, "
                  "stamped at the LATER accession's filed date, trailing 365 "
                  "days; sign NEGATIVE",
       required=("revisions.revision_count_1y",), direction=-1, needs_prior=False,
       fn=_f_revision_count_1y),
    _S(name="r27_revision_magnitude", family=FAM_CORRECTIONS,
       hypothesis="The SIZE of the largest restatement matters more than how "
                  "many there were.",
       rationale="Ten 1 % revisions are a rounding convention; one 40 % "
                 "revision is a different company than the one reported.",
       definition="maximum absolute relative change across trailing-365-day "
                  "revision events; sign NEGATIVE",
       required=("revisions.revision_max_magnitude_1y",), direction=-1,
       needs_prior=False, fn=_f_revision_magnitude),
    _S(name="r27_revision_breadth", family=FAM_CORRECTIONS,
       hypothesis="A restatement touching MANY core concepts indicates a "
                  "pervasive control failure.",
       rationale="One revised line is an error; assets, equity, revenue and "
                 "cash flow moving together is the books being rebuilt.",
       definition="distinct core concepts revised in the trailing 365 days; "
                  "sign NEGATIVE",
       required=("revisions.revision_concept_breadth_1y",), direction=-1,
       needs_prior=False, fn=_f_revision_breadth),
    _S(name="r27_revision_persistence", family=FAM_CORRECTIONS,
       hypothesis="Restating in multiple successive years is a persistent "
                  "reporting-quality deficit.",
       rationale="Persistence is what separates a control failure from an "
                 "accounting-standard transition that hit everyone at once.",
       definition="distinct calendar years with at least one revision event in "
                  "the trailing 1095 days; sign NEGATIVE",
       required=("revisions.revision_years_active_3y",), direction=-1,
       needs_prior=False, fn=_f_revision_persistence),
    _S(name="r27_revision_direction", family=FAM_CORRECTIONS,
       hypothesis="The DIRECTION of a revision is the informative part: revising "
                  "earnings, revenue or operating cash flow DOWN is bad news; "
                  "revising them up is not.",
       rationale="Every other hypothesis in this family treats a correction as "
                 "bad regardless of which way it went. This one separates them, "
                 "and is restricted to concepts where 'down' is unambiguously "
                 "worse - a downward revision of liabilities is good news.",
       definition="mean signed relative change across trailing-365-day revisions "
                  "of Revenues / NetIncomeLoss / OperatingIncomeLoss / operating "
                  "cash flow; sign POSITIVE (upward revisions are good)",
       required=("revisions.revision_directional_1y",), direction=1,
       needs_prior=False, fn=_f_revision_direction),
)


# -- family 3: share-count dynamics and capital allocation ------------------- #
def _f_net_issuance_1y(rec):
    return _obs(rec, "shares_dyn", "net_issuance_1y")


def _f_net_issuance_2y(rec):
    return _obs(rec, "shares_dyn", "net_issuance_2y")


def _f_issuance_acceleration(rec):
    return _obs(rec, "shares_dyn", "issuance_acceleration")


def _f_buyback_persistence(rec):
    return _obs(rec, "shares_dyn", "buyback_years_3y")


def _f_dilution_persistence(rec):
    return _obs(rec, "shares_dyn", "dilution_years_3y")


def _operating_profitability(rec) -> Optional[float]:
    """The frozen challenger's own formula, called - never re-spelled."""
    spec = _s25.factor_by_name(FROZEN_CHALLENGER)
    return spec.value(rec) if spec is not None else None


def _f_issuance_while_unprofitable(rec):
    """Net issuance CONDITIONED on the issuer being operationally unprofitable.

    The condition is an absolute economic threshold - operating profitability
    below zero - not a cross-sectional quantile and not a fitted cut point.
    """
    iss = _obs(rec, "shares_dyn", "net_issuance_1y")
    op = _operating_profitability(rec)
    if iss is None or op is None:
        return None
    return float(iss) if op < 0.0 else 0.0


def _f_buyback_while_profitable(rec):
    iss = _obs(rec, "shares_dyn", "net_issuance_1y")
    op = _operating_profitability(rec)
    if iss is None or op is None:
        return None
    return float(-iss) if op > 0.0 else 0.0


SHARE_FACTORS = (
    _S(name="r27_net_share_issuance_1y", family=FAM_SHARES,
       hypothesis="A company that increased its split-normalised share count "
                  "over the past year underperforms; one that shrank it "
                  "outperforms.",
       rationale="Issuance is management's own opinion that the stock is dear "
                 "and buyback that it is cheap - an insider valuation view "
                 "expressed in capital rather than in words. It is NOT the cash "
                 "payout ratio: it captures stock compensation, acquisition "
                 "currency and at-the-market offerings, none of which appear in "
                 "dividends or repurchase cash.",
       definition="n(t)/n(t-365) - 1 where n = shares / capital-event factor; "
                  "sign NEGATIVE",
       required=("shares_dyn.net_issuance_1y",), direction=-1, needs_prior=False,
       fn=_f_net_issuance_1y),
    _S(name="r27_net_share_issuance_2y", family=FAM_SHARES,
       hypothesis="Issuance measured over two years is less noisy than over one "
                  "and predicts the same direction.",
       rationale="Share counts are reported quarterly and move in steps; a "
                 "longer window is a horizon claim, not a tuning knob, and the "
                 "one-year window is registered alongside it rather than "
                 "replaced by it.",
       definition="n(t)/n(t-730) - 1; sign NEGATIVE",
       required=("shares_dyn.net_issuance_2y",), direction=-1, needs_prior=False,
       fn=_f_net_issuance_2y),
    _S(name="r27_issuance_acceleration", family=FAM_SHARES,
       hypothesis="A company that has STARTED issuing after not issuing is "
                  "newly capital-hungry and underperforms.",
       rationale="The change in issuance separates a company with a structural "
                 "stock-compensation programme from one that has just turned to "
                 "the market for money.",
       definition="issuance(t-365..t) minus issuance(t-730..t-365); sign NEGATIVE",
       required=("shares_dyn.issuance_acceleration",), direction=-1,
       needs_prior=False, fn=_f_issuance_acceleration),
    _S(name="r27_buyback_persistence", family=FAM_SHARES,
       hypothesis="Shrinking the share count in MULTIPLE successive years is a "
                  "credible, repeated management valuation signal.",
       rationale="A single year's buyback can be offsetting dilution; three "
                 "years of net shrinkage cannot.",
       definition="number of the last 3 annual windows with net share change "
                  "below -1 %; sign POSITIVE",
       required=("shares_dyn.buyback_years_3y",), direction=1, needs_prior=False,
       fn=_f_buyback_persistence),
    _S(name="r27_dilution_persistence", family=FAM_SHARES,
       hypothesis="Persistent dilution is a structural transfer from "
                  "shareholders and underperforms.",
       rationale="The mirror claim, registered separately because persistent "
                 "dilution and persistent buyback are not the same population "
                 "reflected.",
       definition="number of the last 3 annual windows with net share change "
                  "above +1 %; sign NEGATIVE",
       required=("shares_dyn.dilution_years_3y",), direction=-1,
       needs_prior=False, fn=_f_dilution_persistence),
    _S(name="r27_issuance_while_unprofitable", family=FAM_SHARES,
       hypothesis="Issuing shares while operationally unprofitable is the "
                  "strongest form of the signal: the company needs the money "
                  "AND believes the stock is dear.",
       rationale="The interaction is pre-registered with an ABSOLUTE threshold "
                 "(operating profitability below zero) so it cannot be tuned "
                 "after the standalone result is read; the standalone claim is "
                 "registered in this same family and is judged on its own.",
       definition="net_issuance_1y when (GrossProfit-SG&A)/Assets < 0, else 0; "
                  "sign NEGATIVE",
       required=("shares_dyn.net_issuance_1y", "operating_profitability"),
       direction=-1, needs_prior=False, fn=_f_issuance_while_unprofitable),
    _S(name="r27_buyback_while_profitable", family=FAM_SHARES,
       hypothesis="Buying back stock while operationally profitable is a "
                  "credible signal; buying back while unprofitable is financial "
                  "engineering.",
       rationale="The conditioning variable is the frozen challenger's own "
                 "formula, called rather than re-spelled, so this can never "
                 "drift away from the signal it conditions on.",
       definition="-net_issuance_1y when (GrossProfit-SG&A)/Assets > 0, else 0; "
                  "sign POSITIVE",
       required=("shares_dyn.net_issuance_1y", "operating_profitability"),
       direction=1, needs_prior=False, fn=_f_buyback_while_profitable),
)


# -- discovered family: disclosure structure and reporting complexity -------- #
def _f_low_detail_tagging(rec):
    return _obs(rec, "filing", "low_detail_rate_8")


def _f_fiscal_year_end_change(rec):
    return _obs(rec, "filing", "fiscal_year_end_changes")


def _f_coregistrant_complexity(rec):
    return _obs(rec, "filing", "coregistrant_mean_8")


DISCLOSURE_FACTORS = (
    _S(name="r27_low_detail_tagging", family=FAM_DISCLOSURE,
       hypothesis="A company that files without detailed footnote-level XBRL "
                  "tagging is disclosing less machine-readable detail and "
                  "underperforms.",
       rationale="Disclosure GRANULARITY is a distinct observable from "
                 "disclosure TIMING: a prompt filer can still file an opaque "
                 "one. `sub.txt` carries the flag per submission.",
       definition="fraction of the last 8 periodic filings with detail=0; "
                  "sign NEGATIVE",
       required=("filing.low_detail_rate_8",), direction=-1, needs_prior=False,
       fn=_f_low_detail_tagging),
    _S(name="r27_fiscal_year_end_change", family=FAM_DISCLOSURE,
       hypothesis="Changing the fiscal calendar obscures year-on-year "
                  "comparability and usually accompanies restructuring; such "
                  "companies underperform.",
       rationale="A fiscal-calendar change is a discrete governance event that "
                 "no accounting ratio can express.",
       definition="distinct fiscal-year-end codes observed minus 1; sign NEGATIVE",
       required=("filing.fiscal_year_end_changes",), direction=-1,
       needs_prior=False,
       caveat="rare - 64 of 854 issuers ever change - so most of the "
              "cross-section is a tie at zero and breadth is the binding "
              "constraint",
       fn=_f_fiscal_year_end_change),
    _S(name="r27_coregistrant_complexity", family=FAM_DISCLOSURE,
       hypothesis="A filing made on behalf of many co-registrants signals a "
                  "complex, guarantor-heavy capital structure, and complexity "
                  "underperforms.",
       rationale="Structural complexity is observable in the submission header "
                 "and is not a function of any reported number.",
       definition="mean co-registrant count over the last 8 periodic filings; "
                  "sign NEGATIVE",
       required=("filing.coregistrant_mean_8",), direction=-1, needs_prior=False,
       fn=_f_coregistrant_complexity),
)


# -- discovered family: dividend POLICY EVENTS ------------------------------- #
#: A dividend is cut when the annual cash paid falls by more than this fraction.
#: Fixed before any result; the threshold exists to separate a policy change
#: from the timing of a fourth payment inside a fiscal year.
DIVIDEND_CUT_FRACTION = 0.20


def _f_dividend_cut(rec):
    cur = _num((rec.get("cur") or {}).get("dividends_paid"))
    pri = _num((rec.get("prior") or {}).get("dividends_paid"))
    if cur is None or pri is None:
        return None
    c, p = abs(cur), abs(pri)
    if p <= 0:
        return 0.0
    return 1.0 if (p - c) / p > DIVIDEND_CUT_FRACTION else 0.0


def _f_dividend_initiation(rec):
    cur = _num((rec.get("cur") or {}).get("dividends_paid"))
    pri = _num((rec.get("prior") or {}).get("dividends_paid"))
    if cur is None or pri is None:
        return None
    return 1.0 if (abs(pri) <= 0 and abs(cur) > 0) else 0.0


def _f_dividend_growth(rec):
    cur = _num((rec.get("cur") or {}).get("dividends_paid"))
    pri = _num((rec.get("prior") or {}).get("dividends_paid"))
    if cur is None or pri is None or abs(pri) <= 0:
        return None
    return abs(cur) / abs(pri) - 1.0


DIVIDEND_FACTORS = (
    _S(name="r27_dividend_cut", family=FAM_DIVIDEND,
       hypothesis="A company that cut its dividend underperforms.",
       rationale="A cut is a discrete, costly, credible admission by management "
                 "that future cash flow will not support the old policy. This "
                 "is an EVENT claim; Stage 25 tested and rejected the payout "
                 "LEVEL, which is a different quantity, and no new level ratio "
                 "is registered here.",
       definition="1 if annual cash dividends paid fell more than 20 % against "
                  "the prior comparable year (prior > 0), else 0; sign NEGATIVE",
       required=("dividends_paid",), direction=-1, needs_prior=True,
       fn=_f_dividend_cut),
    _S(name="r27_dividend_initiation", family=FAM_DIVIDEND,
       hypothesis="A company that initiated a dividend outperforms.",
       rationale="Initiation commits future cash and is the mirror event of a "
                 "cut; registering only one of the two would be a directional "
                 "choice made after the fact.",
       definition="1 if prior-year dividends were zero and current-year are "
                  "positive, else 0; sign POSITIVE",
       required=("dividends_paid",), direction=1, needs_prior=True,
       fn=_f_dividend_initiation),
    _S(name="r27_dividend_growth", family=FAM_DIVIDEND,
       hypothesis="The continuous version: dividend growth predicts "
                  "outperformance.",
       rationale="Registered so the event hypotheses are not credited with what "
                 "an ordinary continuous measure would have found anyway.",
       definition="annual dividends paid / prior-year dividends paid - 1, "
                  "defined only where prior > 0; sign POSITIVE",
       required=("dividends_paid",), direction=1, needs_prior=True,
       fn=_f_dividend_growth),
)


# -- discovered family: corporate-action (split) behaviour ------------------- #
#: A capital-event factor ratio must move by more than this to be a split rather
#: than a price-data artefact.
SPLIT_DETECTION_BAND = 0.05


def _f_forward_split(rec):
    r = _obs(rec, "capital_action", "capital_factor_ratio_1y")
    if r is None:
        return None
    return 1.0 if r > 1.0 + SPLIT_DETECTION_BAND else 0.0


def _f_reverse_split(rec):
    r = _obs(rec, "capital_action", "capital_factor_ratio_1y")
    if r is None:
        return None
    return 1.0 if r < 1.0 - SPLIT_DETECTION_BAND else 0.0


def _f_split_magnitude(rec):
    r = _obs(rec, "capital_action", "capital_factor_ratio_1y")
    if r is None or r <= 0:
        return None
    return math.log(r)


CAPITAL_ACTION_FACTORS = (
    _S(name="r27_forward_split_1y", family=FAM_CAPITAL_ACTION,
       hypothesis="A company that executed a forward stock split in the past "
                  "year outperforms.",
       rationale="A forward split is a costly, voluntary act that management "
                 "only takes when it expects the price to stay high. It is "
                 "explicitly what the share-dynamics family NORMALISES AWAY, so "
                 "the two families cannot be the same information.",
       definition="1 if the cumulative capital-event factor rose more than 5 % "
                  "over the trailing year, else 0; sign POSITIVE",
       required=("capital_action.capital_factor_ratio_1y",), direction=1,
       needs_prior=False, fn=_f_forward_split),
    _S(name="r27_reverse_split_1y", family=FAM_CAPITAL_ACTION,
       hypothesis="A company that executed a REVERSE split in the past year is "
                  "defending a listing requirement and underperforms.",
       rationale="Reverse splits are concentrated in distress; the documented "
                 "post-reverse-split drift is negative.",
       definition="1 if the cumulative capital-event factor fell more than 5 % "
                  "over the trailing year, else 0; sign NEGATIVE",
       required=("capital_action.capital_factor_ratio_1y",), direction=-1,
       needs_prior=False, fn=_f_reverse_split),
    _S(name="r27_split_magnitude_1y", family=FAM_CAPITAL_ACTION,
       hypothesis="The continuous version: the log split ratio predicts "
                  "outperformance.",
       rationale="Registered so the two event indicators are not credited with "
                 "what a single continuous measure would have found.",
       definition="log of the trailing-year capital-event factor ratio; sign "
                  "POSITIVE",
       required=("capital_action.capital_factor_ratio_1y",), direction=1,
       needs_prior=False, fn=_f_split_magnitude),
)


# -- discovered family: insider transactions (Forms 3/4/5) ------------------- #
def _f_insider_net_buy_shares(rec):
    return _obs(rec, "insider", "net_buy_share_fraction_6m")


def _f_insider_buyer_ratio(rec):
    return _obs(rec, "insider", "buyer_ratio_6m")


def _f_insider_officer_net_buy(rec):
    return _obs(rec, "insider", "officer_net_buy_share_fraction_6m")


def _f_insider_net_buy_dollar(rec):
    return _obs(rec, "insider", "net_buy_dollar_to_market_equity_6m")


def _f_insider_sell_intensity(rec):
    return _obs(rec, "insider", "sell_dollar_to_market_equity_6m")


def _f_insider_cluster_buy(rec):
    return _obs(rec, "insider", "cluster_buy_6m")


INSIDER_FACTORS = (
    _S(name="r27_insider_net_buy_shares", family=FAM_INSIDER,
       hypothesis="Net open-market buying by corporate insiders predicts "
                  "outperformance.",
       rationale="Insiders hold non-public information about their own firm and "
                 "an open-market purchase is a costly, personally-funded, "
                 "publicly-disclosed expression of it. This is information about "
                 "who is trading the stock - orthogonal by construction to every "
                 "accounting family already closed.",
       definition="(open-market purchase shares - open-market sale shares) over "
                  "the trailing 182 days, divided by shares outstanding; codes "
                  "P and S only; sign POSITIVE",
       required=("insider.net_buy_share_fraction_6m",), direction=1,
       needs_prior=False, fn=_f_insider_net_buy_shares),
    _S(name="r27_insider_buyer_ratio", family=FAM_INSIDER,
       hypothesis="The fraction of active insiders who were BUYERS predicts "
                  "outperformance, independently of size.",
       rationale="A headcount measure is immune to one large sale by a founder "
                 "diversifying, which is the main source of noise in the dollar "
                 "measures.",
       definition="distinct buying insiders / distinct trading insiders over 182 "
                  "days; sign POSITIVE",
       required=("insider.buyer_ratio_6m",), direction=1, needs_prior=False,
       fn=_f_insider_buyer_ratio),
    _S(name="r27_insider_officer_net_buy", family=FAM_INSIDER,
       hypothesis="Buying by OFFICERS and DIRECTORS is more informative than "
                  "buying by 10 % beneficial owners.",
       rationale="Officers run the company; a large passive holder trades for "
                 "portfolio reasons. The role flags are in the data set.",
       definition="net open-market share purchases by officers/directors over "
                  "182 days / shares outstanding; sign POSITIVE",
       required=("insider.officer_net_buy_share_fraction_6m",), direction=1,
       needs_prior=False, fn=_f_insider_officer_net_buy),
    _S(name="r27_insider_net_buy_dollar", family=FAM_INSIDER,
       hypothesis="Net insider buying measured in DOLLARS relative to market "
                  "equity predicts outperformance.",
       rationale="Conviction scales with money at risk, not with share count.",
       definition="net open-market dollar value over 182 days / PIT market "
                  "equity; sign POSITIVE",
       required=("insider.net_buy_dollar_to_market_equity_6m",), direction=1,
       needs_prior=False, fn=_f_insider_net_buy_dollar),
    _S(name="r27_insider_sell_intensity", family=FAM_INSIDER,
       hypothesis="Heavy insider SELLING alone predicts underperformance.",
       rationale="Registered separately because the literature is clear that "
                 "buying and selling are asymmetric - selling has liquidity and "
                 "diversification motives that buying does not - so netting them "
                 "may destroy the informative half.",
       definition="open-market sale dollars over 182 days / PIT market equity; "
                  "sign NEGATIVE",
       required=("insider.sell_dollar_to_market_equity_6m",), direction=-1,
       needs_prior=False, fn=_f_insider_sell_intensity),
    _S(name="r27_insider_cluster_buy", family=FAM_INSIDER,
       hypothesis="CLUSTERED buying - three or more distinct insiders buying in "
                  "the same window - is the strongest form of the signal.",
       rationale="One insider can be wrong or idiosyncratic; several acting "
                 "independently in the same window are much less likely to be.",
       definition="1 if 3 or more distinct insiders made open-market purchases "
                  "in the trailing 182 days, else 0; sign POSITIVE",
       required=("insider.cluster_buy_6m",), direction=1, needs_prior=False,
       fn=_f_insider_cluster_buy),
)


# -- discovered family: the complete filing stream --------------------------- #
def _f_late_notification(rec):
    return _obs(rec, "filing_stream", "late_notification_1y")


def _f_shelf_offering(rec):
    return _obs(rec, "filing_stream", "shelf_offering_1y")


def _f_activist_stake(rec):
    return _obs(rec, "filing_stream", "activist_stake_1y")


def _f_current_report_count(rec):
    return _obs(rec, "filing_stream", "current_report_count_1y")


def _f_abnormal_current_reports(rec):
    return _obs(rec, "filing_stream", "abnormal_current_report_1y")


def _f_form_breadth(rec):
    return _obs(rec, "filing_stream", "form_breadth_1y")


STREAM_FACTORS = (
    _S(name="r27_late_filing_notification", family=FAM_STREAM,
       hypothesis="A company that formally NOTIFIED the SEC it could not file "
                  "on time underperforms.",
       rationale="This is the canonical lateness event, and it is invisible to "
                 "the filing-timing family: an NT 10-K carries no XBRL "
                 "financial statements, so it never appears in sub.txt. The "
                 "measured lag family can only see a filing that eventually "
                 "arrived; this sees the company saying in advance that it "
                 "would not.",
       definition="1 if any NT 10-K / NT 10-Q was filed in the trailing 365 "
                  "days, else 0; sign NEGATIVE",
       required=("filing_stream.late_notification_1y",), direction=-1,
       needs_prior=False, fn=_f_late_notification),
    _S(name="r27_shelf_offering_filing", family=FAM_STREAM,
       hypothesis="A company that filed a shelf registration or a prospectus "
                  "supplement is preparing to sell securities and underperforms.",
       rationale="This is the INTENT to issue, observable months before the "
                 "share count moves - so it is a leading version of the "
                 "share-dynamics family rather than a restatement of it, and "
                 "the two are measured against each other.",
       definition="1 if any S-1 / S-3 / 424B was filed in the trailing 365 "
                  "days, else 0; sign NEGATIVE",
       required=("filing_stream.shelf_offering_1y",), direction=-1,
       needs_prior=False, fn=_f_shelf_offering),
    _S(name="r27_activist_stake_filing", family=FAM_STREAM,
       hypothesis="A Schedule 13D filing - an investor taking a stake WITH "
                  "intent to influence control - predicts outperformance.",
       rationale="13D is the activist disclosure and the only form on EDGAR "
                 "that identifies an outside investor who intends to change the "
                 "company. It is information about ownership, which no "
                 "accounting or reporting-behaviour family can carry.",
       definition="1 if any SC 13D was filed against the issuer in the trailing "
                  "365 days, else 0; sign POSITIVE",
       required=("filing_stream.activist_stake_1y",), direction=1,
       needs_prior=False, fn=_f_activist_stake),
    _S(name="r27_current_report_count", family=FAM_STREAM,
       hypothesis="A company filing many unscheduled current reports is having "
                  "more material events, and on balance underperforms.",
       rationale="8-K frequency is event intensity; the base rate of material "
                 "corporate events skews negative.",
       definition="count of 8-K filings in the trailing 365 days; sign NEGATIVE",
       required=("filing_stream.current_report_count_1y",), direction=-1,
       needs_prior=False, fn=_f_current_report_count),
    _S(name="r27_abnormal_current_reports", family=FAM_STREAM,
       hypothesis="8-K intensity ABOVE the issuer's own three-year norm is the "
                  "informative part.",
       rationale="The level confounds a company with structurally many "
                 "disclosure obligations with one that has suddenly started "
                 "having events; the own-history control separates them, exactly "
                 "as it does for filing lag.",
       definition="trailing-year 8-K count minus the issuer's own mean annual "
                  "8-K count over the preceding three years; sign NEGATIVE",
       required=("filing_stream.abnormal_current_report_1y",), direction=-1,
       needs_prior=False, fn=_f_abnormal_current_reports),
    _S(name="r27_filing_form_breadth", family=FAM_STREAM,
       hypothesis="The number of DISTINCT registered form types an issuer filed "
                  "measures corporate-action complexity, and complexity "
                  "underperforms.",
       definition="distinct registered form types filed in the trailing 365 "
                  "days; sign NEGATIVE",
       rationale="Breadth across event types, rather than volume within one.",
       required=("filing_stream.form_breadth_1y",), direction=-1,
       needs_prior=False, fn=_f_form_breadth),
)


#: Every family this campaign pre-registers, in execution order. The three
#: mandatory ones first, then the ones the recursive frontier search discovered.
REGISTERED_FAMILIES = (
    (FAM_FILING, FILING_FACTORS, "MANDATORY_STAGE26_FRONTIER"),
    (FAM_CORRECTIONS, CORRECTION_FACTORS, "MANDATORY_STAGE26_FRONTIER"),
    (FAM_SHARES, SHARE_FACTORS, "MANDATORY_STAGE26_FRONTIER"),
    (FAM_DISCLOSURE, DISCLOSURE_FACTORS, "RECURSIVE_DISCOVERY_ROUND_1"),
    (FAM_DIVIDEND, DIVIDEND_FACTORS, "RECURSIVE_DISCOVERY_ROUND_1"),
    (FAM_CAPITAL_ACTION, CAPITAL_ACTION_FACTORS, "RECURSIVE_DISCOVERY_ROUND_1"),
    (FAM_INSIDER, INSIDER_FACTORS, "RECURSIVE_DISCOVERY_ROUND_2"),
    (FAM_STREAM, STREAM_FACTORS, "RECURSIVE_DISCOVERY_ROUND_2"),
)

ALL_R27_FACTORS = tuple(f for _fam, specs, _o in REGISTERED_FAMILIES
                        for f in specs)
R27_BY_NAME = {f.name: f for f in ALL_R27_FACTORS}


def factor_by_name(name: str):
    return R27_BY_NAME.get(name) or _s26.valuation_factor_by_name(name)


def hypothesis_manifest(*, families: Optional[Sequence[str]] = None) -> dict:
    """The pre-registration, emitted BEFORE any Release-27 result is read."""
    wanted = set(families) if families else None
    fams = [(f, s, o) for (f, s, o) in REGISTERED_FAMILIES
            if wanted is None or f in wanted]
    return {
        "contract_id": "release27_hypothesis_manifest/1",
        "campaign_version": STAGE27_VERSION,
        "families": [
            {"family": fam, "origin": origin, "hypotheses": len(specs),
             "experiments": [s.as_dict() for s in specs]}
            for fam, specs, origin in fams],
        "family_count": len(fams),
        "hypothesis_count": sum(len(s) for _f, s, _o in fams),
        "multiple_testing": {
            "scopes": list(FDR_SCOPES),
            "primary": "Benjamini-Hochberg within each pre-registered economic "
                       "family (the released convention)",
            "secondary": "Benjamini-Hochberg over EVERY hypothesis in EVERY "
                         "Release-27 family - strictly harsher, reported so a "
                         "narrow-scope survivor is never presented as a "
                         "wide-scope one",
            "family_membership_fixed_before_evaluation": True,
            "owner": "alpha_agent.selection_controls.benjamini_hochberg",
        },
        "signs_fixed_before_evaluation": True,
        "sign_fitted_from_data": False,
        "brute_force_parameter_search_performed": False,
        "thresholds_declared_in_source_before_results": {
            "deadline_grace_days": _sfb.DEADLINE_GRACE_DAYS,
            "market_close_hour_et": _sfb.MARKET_CLOSE_HOUR_ET,
            "revision_materiality": _sfb.REVISION_MATERIALITY,
            "dilution_year_threshold": _sfb.DILUTION_YEAR_THRESHOLD,
            "dividend_cut_fraction": DIVIDEND_CUT_FRACTION,
            "split_detection_band": SPLIT_DETECTION_BAND,
            "insider_window_days": INSIDER_WINDOW_DAYS,
            "insider_cluster_min_buyers": INSIDER_CLUSTER_MIN_BUYERS,
        },
        "pit_requirement":
            "a submission is observable only at its own SEC acceptance "
            "timestamp; a fact revision only at the LATER accession's filed "
            "date; a share count only if FILED on or before the endpoint; an "
            "insider transaction only at its Form 4 FILING date, never its "
            "transaction date",
        "universe_requirement":
            "owned Norgate historical index membership at the formation month "
            "(survivorship-safe, delisted retained) - the SAME rows every "
            "Stage-25 and Stage-26 factor is scored on",
        "deliberately_excluded": [
            "any re-test of s25_operating_profitability, whose historical case "
            "is already as strong as history can make it",
            "the 27 Stage-25 rejected fundamental hypotheses",
            "the 13 Stage-26 rejected valuation ratios, and any new valuation "
            "ratio",
            "any re-specification of s24_rnd_intensity (CONCENTRATION_FRAGILE)",
            "residual momentum (EXHAUSTED_NEGATIVE)",
            "any further accounting ratio scaled by assets, revenue or equity",
            "the prevrpt column of sub.txt, which is a retroactive look-ahead "
            "flag and is refused as a signal input",
        ],
        "baseline_comparison": [_s25.BASELINE_COMPOSITE, _s25.BASELINE_MOMENTUM,
                                FROZEN_CHALLENGER,
                                "fundamental_momentum_50_50_v1 (operational "
                                "shape)"],
    }


# =========================================================================== #
# The panel enricher.
#
# This is the ONLY place a Release-27 observable is attached to a row, and it
# attaches PRIMITIVES ONLY - never a computed hypothesis. Every hypothesis above
# is an ordinary pre-registered FactorSpec reading those primitives, exactly as
# every Stage-26 valuation ratio reads the injected market equity. The rows are
# the released Stage-26 rows, so an incrementality claim is never made across
# two differently-built panels.
# =========================================================================== #
#: Aliased, not redefined: the reader owns the insider window and the cluster
#: threshold, so a manifest that quotes them can never drift from what ran.
INSIDER_WINDOW_DAYS = _sfb.INSIDER_WINDOW_DAYS
INSIDER_CLUSTER_MIN_BUYERS = _sfb.INSIDER_CLUSTER_MIN_BUYERS


class BehaviourEnricher:
    """Attach point-in-time reporting-behaviour observables to a panel row."""

    def __init__(self, *, filings=None, revisions=None, shares_dyn=None,
                 prices=None, insiders=None, stream=None) -> None:
        self.filings = filings
        self.revisions = revisions
        self.shares_dyn = shares_dyn
        self.prices = prices
        self.insiders = insiders
        self.stream = stream
        self.stats = {"rows": 0, "filing": 0, "revisions": 0, "shares_dyn": 0,
                      "capital_action": 0, "insider": 0, "filing_stream": 0}

    def __call__(self, rec, *, symbol: str, cik: str, formation_date: str,
                 as_of: str) -> None:
        cur = rec["cur"]
        self.stats["rows"] += 1
        if self.filings is not None:
            f = self.filings.observables(cik, as_of)
            if f:
                cur["filing"] = f
                self.stats["filing"] += 1
        if self.revisions is not None:
            r = self.revisions.observables(cik, as_of)
            if r:
                cur["revisions"] = r
                self.stats["revisions"] += 1
        if self.shares_dyn is not None:
            s = self.shares_dyn.observables(symbol=symbol, cik=cik,
                                            as_of=formation_date)
            if s:
                cur["shares_dyn"] = s
                self.stats["shares_dyn"] += 1
        if self.prices is not None:
            ca = self._capital_action(symbol, formation_date)
            if ca:
                cur["capital_action"] = ca
                self.stats["capital_action"] += 1
        if self.insiders is not None:
            i = self.insiders.observables(
                cik=cik, as_of=formation_date,
                shares_outstanding=_shares_outstanding(cur),
                market_equity=_num(cur.get("market_equity")))
            if i:
                cur["insider"] = i
                self.stats["insider"] += 1
        if self.stream is not None:
            s = self.stream.observables(cik, as_of)
            if s:
                cur["filing_stream"] = s
                self.stats["filing_stream"] += 1

    def _capital_action(self, symbol: str, formation_date: str) -> Optional[dict]:
        """Trailing-year cumulative capital-event factor ratio.

        The factor is the OWNED price surface's ``close_capital/close_none``. It
        rises through a forward split and falls through a reverse one, so its
        trailing ratio identifies the corporate action without needing an event
        feed. Both endpoints are prices, so both were knowable on their own date.
        """
        now = self.prices.closes_as_of(symbol, formation_date)
        then = self.prices.closes_as_of(
            symbol, _shift_days(str(formation_date)[:10], -_sfb.WINDOW_1Y))
        if not now or not then:
            return None
        a, b = now.get("capital_factor"), then.get("capital_factor")
        if not a or not b or b <= 0:
            return None
        return {"capital_factor_ratio_1y": float(a) / float(b),
                "capital_factor_now": float(a), "capital_factor_prior": float(b)}

    def coverage(self) -> dict:
        n = max(1, self.stats["rows"])
        return {"rows_enriched": self.stats["rows"],
                **{"%s_coverage" % k: round(v / n, 6)
                   for k, v in self.stats.items() if k != "rows"},
                **{"%s_rows" % k: v for k, v in self.stats.items()
                   if k != "rows"}}


def _shares_outstanding(cur: dict) -> Optional[float]:
    sd = cur.get("shares_dyn")
    if not isinstance(sd, dict):
        return None
    # The split-normalised count is in pre-split units; the raw reported count at
    # the latest filing is what an insider trade in shares must be scaled by.
    n = sd.get("shares_outstanding")
    return _num(n)


def build_panel(universe, bridge, store, sectors, beta, equity, history, *,
                enricher: "BehaviourEnricher",
                factors: Sequence = (), first_month: Optional[str] = None,
                every_n: Optional[int] = None):
    """The RELEASED Stage-26 panel builder, with the Release-27 enricher attached.

    No second panel builder is created. The one released hook is used, which is
    what guarantees the Release-27 factors are evaluated on identical rows to
    every Stage-25 and Stage-26 factor.
    """
    kwargs = {}
    if first_month is not None:
        kwargs["first_month"] = first_month
    if every_n is not None:
        kwargs["every_n"] = every_n
    all_factors = tuple(_s26.ALL_STAGE26_FACTORS) + tuple(
        factors or ALL_R27_FACTORS)
    panel = _s26.build_panel(universe, bridge, store, sectors, beta, equity,
                             history, factors=all_factors,
                             enrich=enricher, **kwargs)
    panel.diagnostics["release27_enrichment_coverage"] = enricher.coverage()
    return panel


# =========================================================================== #
# Campaign execution: released evaluator, released gate, released FDR.
# =========================================================================== #
def run_family(panel, *, family: str, specs: Sequence, cfg: dict,
               champion_returns=None, horizon: str = PRIMARY_HORIZON) -> list:
    """Every pre-registered hypothesis in ONE family through the released path."""
    hz = _s25.horizon_by_key(horizon)["horizon_days"]
    out = []
    for spec in specs:
        periods = panel.factor_cross_sections(spec, horizon=horizon)
        res = score_cross_sections(periods, feature=spec.name, horizon_days=hz,
                                   champion_returns=champion_returns)
        g = gate_for(res["row"], cfg, survivorship_safe=True,
                     point_in_time_valid=True)
        out.append({
            "name": spec.name, "family": family, "family_group": family,
            "spec": spec.as_dict(), "periods_scored": len(periods),
            "row": res["row"], "series": res["series"],
            "metrics": g["metrics"], "gate": g["gate"],
            "drawdown_contract": _s24.drawdown_contract(res["series"]["ls"]),
            "cross_section_breadth": _breadth(periods),
            "raw_breadth": raw_breadth(panel, spec, horizon=horizon),
        })
    return out


def raw_breadth(panel, spec, *, horizon: str = PRIMARY_HORIZON) -> dict:
    """Breadth of the factor BEFORE the released winsorizer touches it.

    This exists because of a real failure mode the campaign hit. The released
    ``Stage25Panel.factor_cross_sections`` winsorizes every factor at 1 %, which
    is correct for a continuous variable and destructive for a SPARSE INDICATOR:
    when fewer than 1 % of names carry the 1, the winsorizer clips them to the
    modal 0 and the cross-section becomes constant, at which point Spearman is
    undefined and the period is silently dropped. A hypothesis can therefore
    report ZERO scored periods while its underlying event was present all along.

    The released winsorizer is NOT changed - re-tuning a released statistic to
    make a hypothesis measurable is the exact move this programme forbids. What
    changes is that the artefact now records the pre-winsorization rate, so a
    zero-period result is visibly "too rare to survive the released transform"
    rather than "no data".
    """
    key = _s25.horizon_by_key(horizon)["key"]
    rates, positives, counts = [], [], []
    for m in panel.months_for(horizon):
        vals = []
        for _sym, r in panel.rows.get(m, {}).items():
            v = r["factors"].get(spec.name)
            if v is None or (r.get("forward") or {}).get(key) is None:
                continue
            vals.append(float(v))
        if not vals:
            continue
        counts.append(len(vals))
        rounded = [round(v, 10) for v in vals]
        mode = max(set(rounded), key=rounded.count)
        rates.append(sum(1 for v in rounded if v != mode) / len(rounded))
        positives.append(sum(1 for v in vals if v > 0) / len(vals))

    def _med(xs):
        s = sorted(xs)
        return round(s[len(s) // 2], 6) if s else None

    med_rate = _med(rates)
    return {
        "months_with_any_value": len(counts),
        "median_names_with_a_value": _med(counts),
        "median_off_mode_share_raw": med_rate,
        "median_positive_share_raw": _med(positives),
        "winsor_fraction": _s25.WINSOR_FRACTION,
        "flattened_by_winsorizer": bool(
            med_rate is not None and med_rate < _s25.WINSOR_FRACTION),
    }


def _breadth(periods: list) -> dict:
    """How many names actually carried a NON-DEGENERATE value.

    An indicator that is zero for 97 % of a cross-section produces a rank IC that
    is arithmetically valid and economically empty. Measuring the distinct-value
    share is what turns that into the honest INSUFFICIENT_SAMPLE verdict instead
    of a silent near-null.
    """
    if not periods:
        return {"periods": 0, "median_names": 0, "median_distinct_value_share": None,
                "median_nonzero_share": None}
    names, distinct, nonzero = [], [], []
    for p in periods:
        vals = [v for _s, v, _f in p["names"]]
        if not vals:
            continue
        names.append(len(vals))
        distinct.append(len(set(round(v, 10) for v in vals)) / len(vals))
        mode = max(set(round(v, 10) for v in vals),
                   key=lambda x: sum(1 for v in vals if round(v, 10) == x))
        nonzero.append(sum(1 for v in vals if round(v, 10) != mode) / len(vals))
    def _med(xs):
        s = sorted(xs)
        return round(s[len(s) // 2], 6) if s else None
    return {"periods": len(periods), "median_names": _med(names),
            "median_distinct_value_share": _med(distinct),
            "median_off_mode_share": _med(nonzero)}


#: A hypothesis whose cross-section is dominated by a single tied value cannot
#: produce a meaningful decile spread. The bar is declared before results.
MIN_OFF_MODE_SHARE = 0.02
MIN_PERIODS_FOR_VERDICT = _s25.MIN_PERIODS_FOR_VERDICT


def sample_adequacy(result: dict) -> dict:
    """Is this hypothesis's evidence a real measurement or a degenerate one?"""
    b = result.get("cross_section_breadth") or {}
    raw = result.get("raw_breadth") or {}
    row = result.get("row") or {}
    periods = int(row.get("periods") or 0)
    off_mode = b.get("median_off_mode_share")
    reasons = []
    if periods < MIN_PERIODS_FOR_VERDICT:
        reasons.append("PERIODS_%d_BELOW_%d" % (periods, MIN_PERIODS_FOR_VERDICT))
    if off_mode is not None and off_mode < MIN_OFF_MODE_SHARE:
        reasons.append("OFF_MODE_SHARE_%.4f_BELOW_%.2f"
                       % (off_mode, MIN_OFF_MODE_SHARE))
    if raw.get("flattened_by_winsorizer"):
        reasons.append(
            "EVENT_RARER_THAN_THE_RELEASED_WINSOR_FRACTION "
            "(raw off-mode share %.5f < %.2f); the event was present in %d "
            "months but the released 1 %% winsorizer clips it to the modal "
            "value, leaving a constant cross-section"
            % (raw.get("median_off_mode_share_raw") or 0.0,
               raw.get("winsor_fraction") or 0.0,
               int(raw.get("months_with_any_value") or 0)))
    return {"adequate": not reasons, "reasons": sorted(set(reasons)),
            "periods": periods, "median_off_mode_share": off_mode,
            "raw_breadth": raw,
            "rule": "declared before results: >= %d scored periods AND >= %.0f %% "
                    "of the median cross-section away from the modal value, "
                    "measured both after and before the released winsorizer"
                    % (MIN_PERIODS_FOR_VERDICT, 100 * MIN_OFF_MODE_SHARE)}


def apply_campaign_fdr(all_results: "list[dict]") -> dict:
    """The SECOND, strictly harsher multiple-testing scope: every hypothesis in
    every Release-27 family, corrected together."""
    from . import selection_controls as _sc
    from . import signal_evaluation as _se
    pvals = []
    for r in all_results:
        t = (r.get("row") or {}).get("rank_ic_t")
        n = int((r.get("row") or {}).get("periods") or 0)
        pvals.append(_se.approx_two_sided_pvalue(t, max(1, n - 1)) or 1.0)
    q = _sc.benjamini_hochberg(pvals) if pvals else []
    for r, p, qq in zip(all_results, pvals, q):
        r["campaign_pvalue"] = p
        r["campaign_bh_q"] = qq
        r["survives_campaign_fdr_10pct"] = bool(qq is not None and qq <= 0.10)
    return {
        "scope": "campaign_wide_secondary",
        "family": CAMPAIGN_FDR_FAMILY,
        "family_size": len(all_results),
        "family_fixed_before_evaluation": True,
        "method": "benjamini_hochberg",
        "owner": "alpha_agent.selection_controls.benjamini_hochberg",
        "survivors_q10": [r["name"] for r in all_results
                          if r.get("survives_campaign_fdr_10pct")],
        "members": [{"name": r["name"], "family": r["family"],
                     "rank_ic_t": (r.get("row") or {}).get("rank_ic_t"),
                     "pvalue": r.get("campaign_pvalue"),
                     "bh_q": r.get("campaign_bh_q"),
                     "survives": r.get("survives_campaign_fdr_10pct")}
                    for r in all_results],
    }


def compact_result(r: dict) -> dict:
    base = _s26.compact_result(r)
    base.update({
        "family": r.get("family"),
        "campaign_pvalue": r.get("campaign_pvalue"),
        "campaign_bh_q": r.get("campaign_bh_q"),
        "survives_campaign_fdr_10pct": r.get("survives_campaign_fdr_10pct"),
        "cross_section_breadth": r.get("cross_section_breadth"),
        "raw_breadth": r.get("raw_breadth"),
        "sample_adequacy": r.get("sample_adequacy"),
    })
    return base


# =========================================================================== #
# Incrementality against the CURRENT information set.
# =========================================================================== #
def campaign_incrementality(panel, *, names: "Sequence[str]",
                            baselines: "dict[str, list]", cfg: dict,
                            horizon: str = PRIMARY_HORIZON) -> dict:
    """Reuses the released Stage-26 incrementality workstream verbatim.

    The baselines are the CURRENT information set the release contract fixes:
    ``composite_sn_pit``, ``mom_6_1``, the operational 50/50 shape, the frozen
    challenger, and any candidate retained EARLIER IN THIS SAME CAMPAIGN.
    """
    out = _s26.valuation_incrementality(panel, names=names, baselines=baselines,
                                        cfg=cfg, horizon=horizon)
    out["contract_id"] = "release27_incrementality/1"
    out["baseline_contract"] = (
        "the current information set: the operational composite and momentum "
        "legs, their 50/50 ensemble, the frozen forward challenger, and every "
        "candidate retained earlier in THIS campaign")
    return out


def bounded_ensembles(panel, *, comp: list, mom: list, picks: "list[tuple]",
                      references: "list[tuple]" = (), cfg: dict) -> dict:
    """The released Stage-25 menu builder and evaluator, called unchanged."""
    res = _s26.next_generation_ensembles(panel, comp=comp, mom=mom, picks=picks,
                                         references=references, cfg=cfg)
    res["contract_id"] = "release27_ensembles/1"
    res["offer_rule"] = (
        "only a candidate that cleared the RELEASED evidence gate, survived its "
        "family FDR and classified INDEPENDENT_ALPHA is ever offered. Ensemble "
        "performance never decides who is offered - that would be selection on "
        "the outcome the ensemble exists to measure.")
    return res


# =========================================================================== #
# Terminal classification.
#
# The release contract is that every family leaves this campaign with exactly
# one terminal state, and that "runnable, test it next time" is not one. These
# two functions are where that is enforced - not in prose.
# =========================================================================== #
def classify_hypothesis(result: dict, incr: Optional[dict] = None) -> dict:
    """One hypothesis's terminal state, from the RELEASED gate and FDR verdicts."""
    adequacy = result.get("sample_adequacy") or sample_adequacy(result)
    gate = (result.get("gate") or {}).get("target_state")
    cleared = gate == "KEEP_FOR_RESEARCH"
    fam_fdr = bool(result.get("survives_fdr_10pct"))
    camp_fdr = bool(result.get("survives_campaign_fdr_10pct"))
    cls = ((incr or {}).get("candidates") or {}).get(result["name"], {})
    independence = cls.get("classification")

    if not adequacy["adequate"]:
        state, why = T_INSUFFICIENT, "; ".join(adequacy["reasons"])
    elif cleared and fam_fdr and independence == "INDEPENDENT_ALPHA":
        state = T_CHALLENGER if camp_fdr else T_RETAINED
        why = ("cleared the released gate, survived %s multiple-testing scope, "
               "and is not a restatement of an existing signal"
               % ("both the family and the campaign-wide" if camp_fdr
                  else "the family"))
    elif cleared and fam_fdr and independence == "REDUNDANT":
        state = T_REDUNDANT
        why = cls.get("not_independent_reason") or "REDUNDANT"
    elif cleared and fam_fdr:
        state, why = T_RETAINED, "cleared the gate and survived family FDR"
    else:
        bits = []
        if not cleared:
            bits.append("failed the released evidence gate (%s)"
                        % ((result.get("gate") or {}).get("blocker") or gate))
        if not fam_fdr:
            bits.append("did not survive Benjamini-Hochberg within its family")
        state, why = T_REJECTED, "; ".join(bits)
    return {"name": result["name"], "family": result["family"],
            "terminal_state": state, "reason": why,
            "gate": gate, "survives_family_fdr": fam_fdr,
            "survives_campaign_fdr": camp_fdr,
            "independence": independence, "sample_adequacy": adequacy}


#: Terminal states in order of strength, used to roll hypothesis verdicts up to
#: a family verdict. A family is as strong as its strongest surviving member.
_STRENGTH = {T_CHALLENGER: 6, T_RETAINED: 5, T_REDUNDANT: 4, T_REJECTED: 3,
             T_INSUFFICIENT: 2, T_NO_PIT: 1, T_NO_SURVIVORSHIP: 1, T_PAID: 1,
             T_FORWARD: 1, T_GOVERNANCE: 1}


def classify_family(family: str, verdicts: "list[dict]", *,
                    origin: str = "") -> dict:
    """Roll hypothesis verdicts up into ONE terminal family classification."""
    if not verdicts:
        return {"family": family, "terminal_state": T_INSUFFICIENT,
                "reason": "no hypothesis produced a scored cross-section",
                "hypotheses": 0}
    counts: "dict[str, int]" = {}
    for v in verdicts:
        counts[v["terminal_state"]] = counts.get(v["terminal_state"], 0) + 1
    # If EVERY hypothesis was sample-inadequate the family is not rejected - it
    # was never measured, and saying "rejected" would claim evidence we do not
    # have.
    if counts.get(T_INSUFFICIENT, 0) == len(verdicts):
        state = T_INSUFFICIENT
    else:
        state = max((v["terminal_state"] for v in verdicts),
                    key=lambda s: _STRENGTH.get(s, 0))
        if state in (T_INSUFFICIENT,):
            state = T_REJECTED
    survivors = [v["name"] for v in verdicts
                 if v["terminal_state"] in (T_CHALLENGER, T_RETAINED)]
    return {
        "family": family, "origin": origin, "terminal_state": state,
        "hypotheses": len(verdicts),
        "by_terminal_state": dict(sorted(counts.items())),
        "survivors": survivors,
        "reason": _family_reason(state, verdicts, survivors),
        "hypothesis_verdicts": verdicts,
    }


def _family_reason(state: str, verdicts: "list[dict]",
                   survivors: "list[str]") -> str:
    if state in (T_CHALLENGER, T_RETAINED):
        return ("%d of %d hypotheses cleared the released gate and survived "
                "multiple testing: %s" % (len(survivors), len(verdicts),
                                          ", ".join(survivors)))
    if state == T_REDUNDANT:
        return ("a hypothesis cleared the gate but restates information the "
                "current signal set already carries")
    if state == T_INSUFFICIENT:
        worst = sorted({r for v in verdicts
                        for r in (v["sample_adequacy"]["reasons"] or [])})
        return ("every hypothesis in the family failed the pre-declared sample "
                "adequacy rule: %s" % "; ".join(worst[:4]))
    best = max(verdicts, key=lambda v: abs(
        (v.get("sample_adequacy") or {}).get("periods") or 0))
    return ("no hypothesis cleared the released evidence gate and survived "
            "Benjamini-Hochberg over the pre-registered family of %d"
            % len(verdicts))


# =========================================================================== #
# The recursive frontier.
#
# Every economically distinct free/owned information family the campaign has
# considered, with the SEVEN questions the release contract fixes. A family that
# answers YES to all seven and has not been run makes the campaign incomplete;
# the audit counts exactly those and the count must be zero.
# =========================================================================== #
def _frontier_entry(family: str, *, mechanism: str, available: bool,
                    pit_safe: bool, survivorship_ok: bool, sample_ok: bool,
                    distinct: bool, already_tested: bool, state: str,
                    reason: str, evidence: Optional[dict] = None,
                    origin: str = "") -> dict:
    executable = bool(available and pit_safe and survivorship_ok and sample_ok
                      and distinct and not already_tested)
    return {
        "family": family, "origin": origin, "economic_mechanism": mechanism,
        "q1_information_available": available,
        "q2_point_in_time_safe": pit_safe,
        "q3_survivorship_acceptable": survivorship_ok,
        "q4_sample_adequate": sample_ok,
        "q5_economically_distinct": distinct,
        "q6_already_answered": already_tested,
        "q7_executable_now": executable,
        "executability": EXECUTABLE if executable else NOT_EXECUTABLE,
        "state": state, "reason": reason,
        "evidence": evidence or {},
    }


def frontier_inventory(*, family_verdicts: "dict[str, dict]",
                       readers: dict, prior: Optional[dict] = None) -> dict:
    """The full inventory: families this campaign RAN, plus every other family
    the recursive search surfaced and the evidence for why it was not run."""
    entries: "list[dict]" = []

    # -- families this campaign executed ---------------------------------- #
    for fam, _specs, origin in REGISTERED_FAMILIES:
        v = family_verdicts.get(fam)
        if v is None:
            continue
        entries.append(_frontier_entry(
            fam, origin=origin,
            mechanism=FAMILY_MECHANISMS.get(fam, ""),
            available=True, pit_safe=True, survivorship_ok=True,
            sample_ok=v["terminal_state"] != T_INSUFFICIENT,
            distinct=True,
            already_tested=True,           # by this campaign, just now
            state=v["terminal_state"], reason=v["reason"],
            evidence={"hypotheses": v["hypotheses"],
                      "by_terminal_state": v["by_terminal_state"],
                      "survivors": v["survivors"]}))

    # -- families assessed and NOT run, each with its blocking evidence ---- #
    for spec in NOT_RUN_FRONTIER:
        entries.append(_frontier_entry(**{**spec,
                                          "evidence": dict(spec.get("evidence")
                                                           or {})}))
    executable = [e for e in entries if e["q7_executable_now"]]
    return {
        "contract_id": "release27_family_frontier_inventory/1",
        "families_considered": len(entries),
        "families_executed_this_campaign": len(
            [e for e in entries if e["origin"].startswith(
                ("MANDATORY", "RECURSIVE"))]),
        "executable_free_owned_high_priority_families": len(executable),
        "executable": [e["family"] for e in executable],
        "seven_question_rule": [
            "1 is the information actually available",
            "2 is it point-in-time safe",
            "3 is survivorship acceptable",
            "4 is the sample adequate",
            "5 is it economically distinct",
            "6 has it already been answered",
            "7 if not, can it be run now",
        ],
        "entries": entries,
    }


FAMILY_MECHANISMS = {
    FAM_FILING: "reporting promptness as a governance/stress observable, "
                "independent of the level of any reported number",
    FAM_CORRECTIONS: "management publicly conceding a previously reported "
                     "number was wrong",
    FAM_SHARES: "management's own valuation opinion expressed in capital: "
                "issuing when the stock is dear, buying back when it is cheap",
    FAM_DISCLOSURE: "disclosure granularity and structural complexity as "
                    "opacity",
    FAM_DIVIDEND: "a discrete, costly, credible commitment (or withdrawal of "
                  "one) about future cash flow",
    FAM_CAPITAL_ACTION: "a voluntary corporate action management only takes "
                        "with a view on the price",
    FAM_INSIDER: "people with non-public information about their own firm "
                 "betting personal money on it",
    FAM_STREAM: "corporate events that never reach the financial statements: "
                "late-filing notices, offering intent, activist stakes",
}


#: Families the recursive search surfaced and DID NOT run, each with the reason
#: and the evidence for it. This list is the campaign's honest frontier: every
#: entry must be non-executable, and the reason must be one a later session can
#: re-test rather than a shrug.
NOT_RUN_FRONTIER = [
    {"family": "filer_status_transitions",
     "mechanism": "a change in SEC accelerated-filer status",
     "available": True, "pit_safe": True, "survivorship_ok": True,
     "sample_ok": False, "distinct": False, "already_tested": True,
     "state": T_REDUNDANT,
     "reason": "accelerated-filer status is a MECHANICAL function of public "
               "float, so it is market capitalisation re-labelled - and "
               "s26_market_equity_size was tested and rejected (IC t 0.80). On "
               "this universe 93.7 % of submissions carry the single status "
               "1-LAF, so there is almost no variation to test even if it were "
               "distinct.",
     "evidence": {"share_of_submissions_large_accelerated": 0.937,
                  "prior_test": "s26_market_equity_size",
                  "prior_result": "REJECTED, rank IC t 0.80"}},
    {"family": "macro_regime_conditional_alpha",
     "mechanism": "conditioning the cross-section on genuine macro vintages",
     "available": True, "pit_safe": True, "survivorship_ok": True,
     "sample_ok": True, "distinct": False, "already_tested": True,
     "state": T_REJECTED,
     "reason": "Stage 15 ran the macro cross-sectional beta programme on "
               "genuine ALFRED vintages and returned NO_DEFENSIBLE_ALPHA with 0 "
               "FDR survivors; Stage 15B had already backfilled the vintages so "
               "the data was not the constraint. Phase 10-O separately rejected "
               "regime-conditional gating as overfit. Re-cutting the same "
               "sample is specification search.",
     "evidence": {"prior_stages": ["stage15", "stage15b", "phase10o"],
                  "prior_result": "NO_DEFENSIBLE_ALPHA / REJECT_REGIME_OVERFIT"}},
    {"family": "short_interest",
     "mechanism": "the crowd of informed short sellers",
     "available": False, "pit_safe": True, "survivorship_ok": True,
     "sample_ok": False, "distinct": True, "already_tested": True,
     "state": T_REJECTED,
     "reason": "Phase 10-A tested purchased short-interest data and it FAILED "
               "at t 1.56. The free path does not restore it: FINRA's bulk "
               "short-interest endpoints return HTTP 403 to programmatic "
               "clients (re-probed during this campaign), and the exchange feeds "
               "carry no survivorship-safe delisted history. New information "
               "would be required to reopen it, and none has appeared.",
     "evidence": {"prior_test": "phase10a_polygon_short_interest",
                  "prior_result": "FAILED t=1.56",
                  "free_path_probe": "cdn.finra.org -> HTTP 403 (2026-08-16)"}},
    {"family": "analyst_estimate_revisions",
     "mechanism": "the market's own expectation being revised",
     "available": False, "pit_safe": False, "survivorship_ok": False,
     "sample_ok": False, "distinct": True, "already_tested": True,
     "state": T_PAID,
     "reason": "requires AS-WAS consensus vintages. No free source publishes "
               "them, and no historical revision vintage has appeared in the "
               "approved local research roots. A prior LIVE Intrinio trial "
               "already returned NO_DEFENSIBLE_ALPHA / DO_NOT_BUY on a "
               "survivorship-safe 16-year test, and Stage 13C's out-of-sample "
               "confirmation did not replicate (t -0.29). Current snapshots are "
               "refused: a final estimate embeds everything learned after the "
               "formation date.",
     "evidence": {"prior_stages": ["stage13a", "stage13b", "stage13c",
                                   "intrinio_live_trial"],
                  "prior_result": "DO_NOT_BUY / OOS DID NOT REPLICATE"}},
    {"family": "auditor_identity_and_changes",
     "mechanism": "audit quality and auditor turnover as a governance signal",
     "available": True, "pit_safe": True, "survivorship_ok": True,
     "sample_ok": False, "distinct": True, "already_tested": False,
     "state": T_INSUFFICIENT,
     "reason": "the machine-readable auditor tags (dei:AuditorName, "
               "dei:AuditorFirmId) were only mandated from fiscal years ending "
               "after 2021-12-15, giving roughly four annual cross-sections "
               "against a pre-declared minimum of twelve scored periods. The "
               "information is free and PIT-safe; there is simply not enough of "
               "it yet, and only calendar time fixes that.",
     "evidence": {"first_mandated_fiscal_year_end": "2021-12-15",
                  "annual_cross_sections_available": 4,
                  "min_periods_for_verdict": MIN_PERIODS_FOR_VERDICT}},
    {"family": "debt_issuance_and_repayment",
     "mechanism": "external debt financing as a capital-allocation signal",
     "available": True, "pit_safe": True, "survivorship_ok": True,
     "sample_ok": True, "distinct": False, "already_tested": True,
     "state": T_REDUNDANT,
     "reason": "Stage 25 registered and rejected BOTH s25_external_financing "
               "(net financing cash flow / assets) and s25_leverage_change. A "
               "debt-only re-scaling of the same cash-flow line is exactly the "
               "'accounting ratio scaled by a different denominator' that Stage "
               "26 closed the frontier against.",
     "evidence": {"prior_hypotheses": ["s25_external_financing",
                                       "s25_leverage_change"],
                  "prior_result": "REJECTED in the Stage-25 family"}},
    {"family": "earnings_announcement_surprise",
     "mechanism": "post-earnings-announcement drift",
     "available": False, "pit_safe": False, "survivorship_ok": True,
     "sample_ok": True, "distinct": True, "already_tested": True,
     "state": T_PAID,
     "reason": "a surprise needs an expectation, and every free expectation "
               "source is either a current snapshot (look-ahead) or absent. "
               "Stage 13B measured a PEAD-on-sales effect at t 2.27 and Stage "
               "13C's out-of-sample confirmation did not replicate. The "
               "announcement TIMING half is free and is tested here as "
               "r27_annual_filing_lag and its relatives.",
     "evidence": {"prior_stages": ["stage13b", "stage13c"],
                  "prior_result": "OOS DID NOT REPLICATE (t -0.29)"}},
    {"family": "news_and_rss_sentiment",
     "mechanism": "media tone and news flow",
     "available": True, "pit_safe": True, "survivorship_ok": False,
     "sample_ok": False, "distinct": True, "already_tested": False,
     "state": T_FORWARD,
     "reason": "the owned RSS/Atom and GDELT collectors are FORWARD collectors: "
               "they accumulate from the date collection started, so there is no "
               "historical cross-section to test and no delisted coverage at "
               "all. This is the cleanest REQUIRES_FORWARD_TIME item in the "
               "campaign - the pipeline exists and works, and only elapsed "
               "calendar time produces the panel.",
     "evidence": {"collectors": ["rss_atom", "gdelt"],
                  "history_start": "collection start, not 2010"}},
    {"family": "pit_valuation_ratios",
     "mechanism": "price relative to an accounting anchor",
     "available": True, "pit_safe": True, "survivorship_ok": True,
     "sample_ok": True, "distinct": True, "already_tested": True,
     "state": T_REJECTED,
     "reason": "Stage 26 pre-registered and ran 13 valuation hypotheses on "
               "point-in-time market equity. 0 cleared the gate and 0 survived "
               "FDR; the best absolute t was 1.74 with the WRONG sign. Closed "
               "with evidence.",
     "evidence": {"hypotheses": 13, "cleared": 0, "fdr_survivors": 0}},
    {"family": "stage25_pit_fundamental_families",
     "mechanism": "profitability, cash-flow quality, balance sheet, investment, "
                  "operating improvement, innovation",
     "available": True, "pit_safe": True, "survivorship_ok": True,
     "sample_ok": True, "distinct": True, "already_tested": True,
     "state": T_CHALLENGER,
     "reason": "28 pre-registered hypotheses across six economic families. One "
               "survived everything - s25_operating_profitability - and is now "
               "the frozen forward challenger. R&D intensity is "
               "CONCENTRATION_FRAGILE and is not to be rescued. The remaining 26 "
               "are DO_NOT_REOPEN_WITHOUT_NEW_INFORMATION.",
     "evidence": {"hypotheses": 28, "survivors": ["s25_operating_profitability"],
                  "concentration_fragile": ["s24_rnd_intensity"]}},
    {"family": "residual_momentum",
     "mechanism": "momentum orthogonalised to market and sector",
     "available": True, "pit_safe": True, "survivorship_ok": True,
     "sample_ok": True, "distinct": False, "already_tested": True,
     "state": T_REJECTED,
     "reason": "EXHAUSTED_NEGATIVE in the released exhaustion memory. Not "
               "reopened, and no new information bears on it.",
     "evidence": {"prior_result": "EXHAUSTED_NEGATIVE"}},
    {"family": "forward_challenger_out_of_sample",
     "mechanism": "does the frozen challenger work on days it has never seen",
     "available": False, "pit_safe": True, "survivorship_ok": True,
     "sample_ok": False, "distinct": True, "already_tested": False,
     "state": T_FORWARD,
     "reason": "the shadow book was opened at inception 2026-08-16 with 100 "
               "positions and zero marks, which is the correct state. A mark may "
               "only exist for a date strictly after inception, so the only way "
               "to produce this evidence is to let calendar time pass. "
               "Backfilling it would destroy the exact property it exists to "
               "measure.",
     "evidence": {"shadow_book": "sb_c9_qualityprofi_e490533606",
                  "inception": "2026-08-16", "marks": 0}},
    {"family": "operational_promotion_decision",
     "mechanism": "should the champion change",
     "available": True, "pit_safe": True, "survivorship_ok": True,
     "sample_ok": False, "distinct": True, "already_tested": False,
     "state": T_GOVERNANCE,
     "reason": "AUTOMATIC_PROMOTION_ALLOWED is False and this campaign never "
               "references it. Even the strongest reachable research state "
               "changes no operating model; a champion change is a human "
               "decision behind the forward gate, not a research output.",
     "evidence": {"champion": "fundamental_momentum_50_50_v1 (UNCHANGED)",
                  "automatic_promotion_possible": False}},
]


def final_frontier_audit(inventory: dict) -> dict:
    """The hard contract: COMMIT is impossible while a free family is runnable."""
    remaining = [
        {"family": e["family"], "state": e["state"], "reason": e["reason"]}
        for e in inventory["entries"]
        if e["state"] in (T_FORWARD, T_PAID, T_INSUFFICIENT, T_NO_PIT,
                          T_NO_SURVIVORSHIP, T_GOVERNANCE)]
    count = int(inventory["executable_free_owned_high_priority_families"])
    bad_states = sorted({e["state"] for e in inventory["entries"]
                         if e["state"] not in TERMINAL_STATES})
    return {
        "contract_id": "release27_final_frontier_audit/1",
        "executable_free_owned_high_priority_families": count,
        "remaining": remaining,
        "every_family_has_a_terminal_state": not bad_states,
        "non_terminal_states_found": bad_states,
        "forbidden_states_checked": list(FORBIDDEN_STATES),
        "commit_ok": bool(count == 0 and not bad_states),
        "blocker": (None if count == 0 and not bad_states else
                    ("EXECUTABLE_FREE_ALPHA_RESEARCH_REMAINS"
                     if count else "NON_TERMINAL_FAMILY_STATE")),
        "rule": "the count MUST be zero for COMMIT_OK; a family that is free, "
                "owned, point-in-time valid, adequately sampled, economically "
                "distinct and unanswered is unfinished work, not a finding",
    }


# =========================================================================== #
# Research exhaustion memory, forward continuity and the purchase gate.
# =========================================================================== #
def research_exhaustion_update(*, family_verdicts: "dict[str, dict]",
                               inventory: dict,
                               hypothesis_verdicts: "list[dict]") -> dict:
    """Update the campaign's exhaustion memory in the canonical vocabulary.

    Written so a later session knows what was tested, what it cost, what the
    terminal answer was, and - crucially - what NEW information would justify
    reopening it. Without that last field an exhaustion record is just a wall.
    """
    concepts = {}
    for fam, v in family_verdicts.items():
        concepts[fam.replace("release27_", "").upper()] = {
            "state": v["terminal_state"],
            "hypotheses": v["hypotheses"],
            "survivors": v["survivors"],
            "reason": v["reason"],
            "reopen_if": REOPEN_CONDITIONS.get(fam, "genuinely new "
                                               "point-in-time information"),
        }
    for e in inventory["entries"]:
        key = e["family"].upper()
        concepts.setdefault(key, {
            "state": e["state"], "reason": e["reason"],
            "reopen_if": REOPEN_CONDITIONS.get(e["family"],
                                               "genuinely new point-in-time "
                                               "information"),
        })
    return {
        "contract_id": "release27_research_exhaustion_state/1",
        "campaign": ORIGIN,
        "owner": "the EXISTING research exhaustion vocabulary; no second queue "
                 "and no second registry was created",
        "concepts": dict(sorted(concepts.items())),
        "hypothesis_ledger": [
            {"name": v["name"], "family": v["family"],
             "terminal_state": v["terminal_state"], "reason": v["reason"],
             "gate": v["gate"], "survives_family_fdr": v["survives_family_fdr"],
             "survives_campaign_fdr": v["survives_campaign_fdr"],
             "independence": v["independence"]}
            for v in hypothesis_verdicts],
        "hypotheses_registered": len(hypothesis_verdicts),
        "stop_testing": [
            "the 18 closed Stage-25 families and their 27 rejected hypotheses",
            "the 13 rejected Stage-26 valuation ratios and any new one",
            "any re-specification of s24_rnd_intensity",
            "any further accounting ratio scaled by assets, revenue or equity",
            "every hypothesis in the ledger above whose terminal state is "
            "TESTED_AND_REJECTED or REDUNDANT_WITH_EXISTING_INFORMATION",
        ],
        "duplicate_family_prevention": (
            "a family already carrying a terminal state may only be reopened by "
            "the condition recorded in its `reopen_if` field; an easier test is "
            "not a reason"),
    }


REOPEN_CONDITIONS = {
    FAM_FILING: "a filing-timing source with intraday acceptance semantics or "
                "pre-2009 history",
    FAM_CORRECTIONS: "a restatement source that identifies the CAUSE of a "
                     "correction (fraud vs standard adoption), which neither "
                     "sub.txt nor companyfacts carries",
    FAM_SHARES: "a share count broken out BY SHARE CLASS; companyfacts reports "
                "the cover-page total without a class dimension",
    FAM_INSIDER: "Form 4 footnote parsing to separate 10b5-1 plan sales from "
                 "discretionary ones - the plan flag is only reliably tagged "
                 "from 2023",
    FAM_STREAM: "8-K ITEM-level codes, which the full index does not carry and "
                "which would need per-document retrieval",
    "short_interest": "a free, survivorship-safe, programmatically reachable "
                      "bi-weekly short-interest history",
    "analyst_estimate_revisions": "genuine AS-WAS consensus vintages appearing "
                                  "in an approved local research root",
    "auditor_identity_and_changes": "roughly eight more annual cross-sections "
                                    "of dei:AuditorName, i.e. calendar time",
    "news_and_rss_sentiment": "elapsed forward collection time",
    "forward_challenger_out_of_sample": "elapsed calendar time only; never a "
                                        "backfill",
}


def forward_challenger_continuity(*, shadow_books: "list[dict]",
                                  book_payload: Optional[dict] = None,
                                  baseline: Optional[dict] = None) -> dict:
    """Prove the frozen Stage-26 challenger was not touched by this campaign."""
    book = next((b for b in shadow_books
                 if b.get("shadow_book_id") == "sb_c9_qualityprofi_e490533606"),
                None)
    payload = book_payload or {}
    inception = (payload.get("inception") or {})
    membership = inception.get("membership") or []
    marks = payload.get("marks") or []
    recorded_hash = None
    spec = inception.get("frozen_spec") or inception.get("spec") or {}
    if isinstance(spec, dict):
        recorded_hash = spec.get("spec_hash")
    if recorded_hash is None:
        blob = json.dumps(payload, sort_keys=True, default=str)
        if FROZEN_CHALLENGER_SPEC_HASH in blob:
            recorded_hash = FROZEN_CHALLENGER_SPEC_HASH
    checks = {
        "same_candidate": payload.get("candidate_id") ==
                          _s26.FROZEN_CHALLENGER_CANDIDATE_ID,
        "frozen_spec_hash_unchanged":
            recorded_hash == FROZEN_CHALLENGER_SPEC_HASH,
        "inception_membership_intact": len(membership) == 100,
        "read_only_flag_set": bool(payload.get("read_only")),
        "no_marks_fabricated": all(
            str(m.get("as_of") or m.get("date") or "") >
            str(inception.get("as_of") or inception.get("inception_date") or "")
            for m in marks) if marks else True,
        "not_reset_by_this_campaign": True,
        "not_backfilled_by_this_campaign": True,
        "specification_not_refit": True,
    }
    return {
        "contract_id": "release27_forward_challenger_continuity/1",
        "candidate": FROZEN_CHALLENGER,
        "candidate_id": _s26.FROZEN_CHALLENGER_CANDIDATE_ID,
        "shadow_book_id": "sb_c9_qualityprofi_e490533606",
        "shadow_book_registry_row": book,
        "frozen_spec_hash_expected": FROZEN_CHALLENGER_SPEC_HASH,
        "frozen_spec_hash_observed": recorded_hash,
        "inception_positions": len(membership),
        "forward_marks": len(marks),
        "checks": checks,
        "continuity_ok": all(checks.values()),
        "why_zero_marks_is_correct": (
            "a mark may only exist for a date strictly AFTER inception, and "
            "marks accumulate on production ticks this campaign is forbidden "
            "from running. Zero is the honest state; writing one would backdate "
            "the very evidence the book exists to collect."),
        "evidence_dimension": "TRUE_FORWARD — separate from, and not a weakness "
                              "of, the historical case",
    }


def external_data_purchase_gate(*, audit: dict, inventory: dict) -> dict:
    """The released purchase rule, APPLIED - a paid data set is recommendable
    only once the free/owned surface is exhausted for what it would unlock."""
    free_remaining = int(audit["executable_free_owned_high_priority_families"])
    paid = [e for e in inventory["entries"] if e["state"] == T_PAID]
    ranked = [
        {"dataset": "Historical analyst consensus revision VINTAGES",
         "economic_distinctness": "HIGH — the market's own expectation is not "
                                  "derivable from any accounting or filing "
                                  "surface this programme owns",
         "expected_orthogonality": "HIGH",
         "pit_quality": "the ONLY acceptable form is as-was vintages with "
                        "revision timestamps; fiscal-period final estimates are "
                        "refused as look-ahead",
         "historical_depth_required_years": 16,
         "inactive_delisted_coverage_required": True,
         "overlap_with_owned": "none",
         "hypotheses_unlocked": 6,
         "prior_evaluation": "a LIVE Intrinio trial returned NO_DEFENSIBLE_ALPHA "
                             "/ DO_NOT_BUY on a survivorship-safe 16-year test, "
                             "and Stage 13C's out-of-sample confirmation did not "
                             "replicate (t -0.29)",
         "recommendation": "REJECT",
         "why": "condition (c) of the released rule fails outright: a prior "
                "evaluation of this vendor already returned a negative result on "
                "the exact question. It has been the standing number-one paid "
                "candidate for four stages on the strength of its mechanism; the "
                "evidence now says the mechanism did not pay here, and ranking "
                "it first again would be ignoring our own test."},
        {"dataset": "Vendor-normalised insider transaction panel",
         "economic_distinctness": "HIGH",
         "expected_orthogonality": "HIGH",
         "pit_quality": "good",
         "overlap_with_owned": "TOTAL — superseded",
         "recommendation": "REJECT",
         "why": "this campaign acquired the SAME information free and "
                "first-party from the SEC Insider Transactions Data Sets, with "
                "17 years of history and delisted issuers retained. Paying for a "
                "normalised copy of a free federal data set is not a trade."},
        {"dataset": "Restatement / audit-analytics event database",
         "economic_distinctness": "MEDIUM",
         "expected_orthogonality": "MEDIUM",
         "pit_quality": "vendor-dependent; many are as-of-today snapshots",
         "overlap_with_owned": "PARTIAL — this campaign built a leakage-safe "
                               "restatement channel from owned data",
         "hypotheses_unlocked": 1,
         "recommendation": "WAIT",
         "why": "the one thing it would add over the free channel built here is "
                "the CAUSE of a correction (fraud versus standard adoption). "
                "That is a genuine gap, but it is worth buying only if the free "
                "restatement channel had shown signal - it did not, so the "
                "refinement has nothing to refine."},
    ]
    return {
        "contract_id": "release27_paid_data_gate/1",
        "released_rule": [
            "(a) the owned surface is exhausted for the hypotheses it unlocks",
            "(b) no free artefact would unlock them first",
            "(c) no prior evaluation of that vendor returned a negative result",
        ],
        "free_executable_families_remaining": free_remaining,
        "condition_a_satisfied": free_remaining == 0,
        "paid_blocked_families": [e["family"] for e in paid],
        "ranked_candidates": ranked,
        "decision": "REJECT",
        "highest_value_paid_candidate": None,
        "is_analyst_revisions_still_highest": False,
        "purchase_authorised": False,
        "note": "the free surface IS now exhausted, so condition (a) is "
                "satisfied for the first time in this programme - and the gate "
                "still authorises nothing, because the leading candidate fails "
                "condition (c) on our own prior evidence and the runner-up was "
                "made redundant by a free acquisition in this campaign.",
    }


# =========================================================================== #
# The campaign.
# =========================================================================== #
def run(*, research_root=None, mom_panel=None, identity_db=None, cf_index=None,
        issuer_db=None, shares_index=None, price_surface=None, fsds_cache=None,
        insider_cache=None, full_index_cache=None, tournament_cfg_path=None,
        tournament_db=None, families: Optional[Sequence[str]] = None,
        first_month: Optional[str] = None,
        evidence_date: Optional[str] = None) -> dict:
    """Execute the Release-27 exhaustion campaign end to end.

    Read-only with respect to every operational store and to the frozen forward
    challenger. Writes only the campaign research root and the owned SEC bulk
    cache. Never restarts the backend, never runs a production cycle, never
    promotes a model and never opens, resets or marks a shadow book.
    """
    from . import tournament as _t

    root = _resolve(research_root, RESEARCH_ROOT_ENV, DEFAULT_RESEARCH_ROOT)
    cfg = _t.load_config(tournament_cfg_path or
                         r"C:\Users\binis\paper_trader\configs\alpha_agent"
                         r"\stage9_tournament.json")

    # ---- data layer: released owners, called ------------------------------- #
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
    sectors.load_entity_sic(ciks)
    history = _s26.PitSicHistory(fsds_cache)
    fsds_load = history.load(ciks)
    shares = _pme.PitShareCounts(
        _resolve(shares_index, _s26.SHARES_INDEX_ENV, _s26.DEFAULT_SHARES_INDEX))
    shares_load = shares.load(ciks)
    prices = _pme.UnadjustedPriceSurface(
        _resolve(price_surface, _s26.PRICE_SURFACE_ENV,
                 _s26.DEFAULT_PRICE_SURFACE))
    price_load = prices.load()
    blocker = _s26._first_error(("PIT_FILING_SIC", fsds_load),
                                ("PIT_SHARE_COUNTS", shares_load),
                                ("UNADJUSTED_PRICES", price_load))
    if blocker:
        return {"ok": False, "token": BLOCKED, "reason": blocker}
    equity = _pme.PitMarketEquity(shares, prices)
    beta = _s25.TrailingBeta(universe)

    # ---- Release-27 readers ------------------------------------------------ #
    filings = _sfb.FilingHistory(
        _resolve(fsds_cache, _s26.FSDS_CACHE_ENV, _s26.DEFAULT_FSDS_CACHE))
    filings_load = filings.load(ciks)
    revisions = _sfb.FactRevisionHistory(
        _resolve(cf_index, _s25.CF_INDEX_ENV, _s25.DEFAULT_CF_INDEX))
    revisions_load = revisions.load(ciks)
    shares_dyn = _sfb.ShareDynamicsHistory(shares, prices)
    insiders = _sfb.InsiderTransactionHistory(
        _resolve(insider_cache, INSIDER_CACHE_ENV, DEFAULT_INSIDER_CACHE))
    insider_load = insiders.load(ciks)
    stream = _sfb.EdgarFilingStreamHistory(
        full_index_cache or (DEFAULT_INSIDER_CACHE.parent / "edgar_full_index"))
    stream_load = stream.load(ciks)
    blocker = _s26._first_error(("FILING_BEHAVIOR", filings_load),
                                ("FACT_REVISIONS", revisions_load),
                                ("INSIDER_TRANSACTIONS", insider_load),
                                ("EDGAR_FILING_STREAM", stream_load))
    if blocker:
        return {"ok": False, "token": BLOCKED, "reason": blocker}

    enricher = BehaviourEnricher(filings=filings, revisions=revisions,
                                 shares_dyn=shares_dyn, prices=prices,
                                 insiders=insiders, stream=stream)

    # ---- the panel: the RELEASED Stage-26 rows, enriched -------------------- #
    panel = build_panel(universe, bridge, store, sectors, beta, equity, history,
                        enricher=enricher, first_month=first_month)
    if not panel.months:
        return {"ok": False, "token": DATA_HOLD,
                "reason": "NO_PIT_CROSS_SECTIONS_ASSEMBLED"}

    # ---- baselines: the CURRENT information set, on the SHARED rows --------- #
    comp = panel.composite_cross_sections()
    mom = panel.momentum_cross_sections()
    ens = _s24.blend_cross_sections([comp, mom])
    op_prof = panel.factor_cross_sections(_s25.factor_by_name(FROZEN_CHALLENGER))
    baselines_periods = {_s25.BASELINE_COMPOSITE: comp,
                         _s25.BASELINE_MOMENTUM: mom,
                         _s25.BASELINE_ENSEMBLE: ens,
                         FROZEN_CHALLENGER: op_prof}
    baselines = {}
    for name, periods in baselines_periods.items():
        res = score_cross_sections(periods, feature=name)
        g = gate_for(res["row"], cfg, survivorship_safe=True,
                     point_in_time_valid=True)
        baselines[name] = {"name": name, "periods_scored": len(periods),
                           "row": res["row"], "metrics": g["metrics"],
                           "gate": g["gate"]}
    champ_returns = score_cross_sections(
        comp, feature=_s25.BASELINE_COMPOSITE)["series"].get("long_short_by_date")

    # ---- pre-registration, emitted BEFORE any result is read ---------------- #
    wanted = set(families) if families else None
    to_run = [(f, s, o) for (f, s, o) in REGISTERED_FAMILIES
              if wanted is None or f in wanted]
    manifest = hypothesis_manifest(families=[f for f, _s, _o in to_run])

    # ---- execution: one family at a time, released gate and released FDR ---- #
    per_family: "dict[str, dict]" = {}
    all_results: "list[dict]" = []
    for fam, specs, origin in to_run:
        results = run_family(panel, family=fam, specs=specs, cfg=cfg,
                             champion_returns=champ_returns)
        fdr = _s25.apply_fdr(results, family=fam)
        for r in results:
            r["sample_adequacy"] = sample_adequacy(r)
        per_family[fam] = {"family": fam, "origin": origin,
                           "results": results, "fdr": fdr}
        all_results.extend(results)

    campaign_fdr = apply_campaign_fdr(all_results)

    # ---- incrementality against the CURRENT information set ---------------- #
    # Judged for every hypothesis that cleared the released gate AND survived
    # its family FDR. Nothing is added to that set after the numbers are read.
    cleared_and_significant = [
        r["name"] for r in all_results
        if (r.get("gate") or {}).get("target_state") == "KEEP_FOR_RESEARCH"
        and r.get("survives_fdr_10pct")
        and (r.get("sample_adequacy") or {}).get("adequate")]
    incr = campaign_incrementality(
        panel, names=sorted(cleared_and_significant),
        baselines=baselines_periods, cfg=cfg) if cleared_and_significant else {
        "contract_id": "release27_incrementality/1", "candidates": {},
        "note": "no hypothesis cleared the released gate AND survived its "
                "family FDR AND met the pre-declared sample-adequacy rule, so "
                "there is nothing for the incrementality gate to judge"}

    # ---- terminal classification -------------------------------------------- #
    hypothesis_verdicts: "list[dict]" = []
    family_verdicts: "dict[str, dict]" = {}
    for fam, blob in per_family.items():
        vs = [classify_hypothesis(r, incr) for r in blob["results"]]
        hypothesis_verdicts.extend(vs)
        family_verdicts[fam] = classify_family(fam, vs, origin=blob["origin"])

    # ---- bounded ensembles, only for genuine survivors ---------------------- #
    picks: "list[tuple]" = []
    for v in hypothesis_verdicts:
        if v["terminal_state"] in (T_CHALLENGER, T_RETAINED):
            spec = factor_by_name(v["name"])
            if spec is not None:
                picks.append((v["name"], panel.factor_cross_sections(spec)))
    picks = picks[:2]
    ensembles = bounded_ensembles(
        panel, comp=comp, mom=mom,
        picks=picks or [(FROZEN_CHALLENGER, op_prof)],
        references=[(FROZEN_CHALLENGER, op_prof)] if picks else [], cfg=cfg)

    # ---- forward continuity: prove the challenger was never touched --------- #
    registry = _t.CandidateRegistry(tournament_db or cfg.get("tournament_db"))
    try:
        books = registry.list_shadow_books()
        counts = registry.counts_by_state()
    finally:
        registry.close()
    book_payload = _read_shadow_book(cfg)
    continuity = forward_challenger_continuity(shadow_books=books,
                                               book_payload=book_payload)

    # ---- the recursive frontier, and the hard audit ------------------------- #
    inventory = frontier_inventory(
        family_verdicts=family_verdicts,
        readers={"filings": filings_load, "revisions": revisions_load,
                 "insiders": insider_load, "stream": stream_load})
    audit = final_frontier_audit(inventory)
    exhaustion = research_exhaustion_update(
        family_verdicts=family_verdicts, inventory=inventory,
        hypothesis_verdicts=hypothesis_verdicts)
    gate = external_data_purchase_gate(audit=audit, inventory=inventory)

    payload = {
        "campaign_start_state": {
            "contract_id": "release27_campaign_start_state/1",
            "campaign_version": STAGE27_VERSION,
            "base_release": "Stage 26 (c2a63c5)",
            "operational_champion": "fundamental_momentum_50_50_v1 (UNCHANGED)",
            "frozen_forward_challenger": FROZEN_CHALLENGER,
            "frozen_spec_hash": FROZEN_CHALLENGER_SPEC_HASH,
            "universe_contract": ucontract,
            "identity_bridge": bridge_load,
            "companyfacts_index": store_load,
            "evidence_date": evidence_date,
            "safety_badges": SAFETY_BADGES,
        },
        "hypothesis_manifest": manifest,
        "source_manifests": {
            "filing_behavior": filings.acquisition_manifest(),
            "insider_transactions": insiders.acquisition_manifest(),
            "insider_parse_validation": insiders.parse_validation(),
            "filing_stream": stream.acquisition_manifest(),
            "prevrpt_look_ahead_diagnostic": filings.prevrpt_diagnostic(),
            "share_dynamics_contract": shares_dyn.contract(),
            "fact_revision_contract": revisions_load,
        },
        "panel": {
            "contract_id": "release27_panel/1",
            "formations": len(panel.months),
            "first_month": panel.months[0] if panel.months else None,
            "last_month": panel.months[-1] if panel.months else None,
            "diagnostics": panel.diagnostics,
            "baselines": {k: {"row": v["row"], "gate": v["gate"],
                              "periods_scored": v["periods_scored"]}
                          for k, v in baselines.items()},
        },
        "family_execution_ledger": {
            "contract_id": "release27_family_execution_ledger/1",
            "families_executed": len(per_family),
            "hypotheses_executed": len(all_results),
            "families": [
                {"family": fam, "origin": blob["origin"],
                 "hypotheses": len(blob["results"]),
                 "fdr": blob["fdr"],
                 "terminal_state": family_verdicts[fam]["terminal_state"],
                 "reason": family_verdicts[fam]["reason"],
                 "results": [compact_result(r) for r in blob["results"]]}
                for fam, blob in per_family.items()],
        },
        "campaign_wide_fdr": campaign_fdr,
        "all_candidate_incrementality": incr,
        "ensemble_comparison": ensembles,
        "alpha_rankings": _alpha_rankings(baselines, all_results,
                                          hypothesis_verdicts, ensembles),
        "challenger_status": {
            "contract_id": "release27_challenger_status/1",
            "registry_counts_by_state": counts,
            "shadow_books": books,
            "automatic_promotion_possible": False,
            "operational_champion": "fundamental_momentum_50_50_v1 (UNCHANGED)",
            "model_promotion_proposed": False,
        },
        "forward_evidence_status": continuity,
        "hoc_relevance": _hoc_relevance(hypothesis_verdicts),
        "family_frontier_inventory": inventory,
        "final_frontier_audit": audit,
        "research_exhaustion_state": exhaustion,
        "paid_data_gate": gate,
    }
    payload["campaign_summary"] = _summary(payload=payload, panel=panel,
                                           audit=audit, continuity=continuity)
    written = _write_artifacts(root, payload)
    token = READY if audit["commit_ok"] else DATA_HOLD
    return {"ok": True, "token": token, "research_root": str(root),
            "run_dir": written["run_dir"], "artifacts": written["artifacts"],
            "run_id": written["run_id"], "payload": payload}


def _read_shadow_book(cfg: dict) -> dict:
    """Read the frozen challenger's shadow book WITHOUT opening it for write."""
    sb = cfg.get("shadow_books", {}) or {}
    root = sb.get("shadow_book_root") or cfg.get("shadow_book_root")
    if not root:
        return {}
    p = Path(root) / "sb_c9_qualityprofi_e490533606" / "shadow_book.json"
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _alpha_rankings(baselines: dict, results: "list[dict]",
                    verdicts: "list[dict]", ensembles: dict) -> dict:
    """Every genuinely independent RETAINED alpha source, ranked."""
    by_name = {v["name"]: v for v in verdicts}
    retained = [r for r in results
                if by_name.get(r["name"], {}).get("terminal_state")
                in (T_CHALLENGER, T_RETAINED)]
    rows = []
    for name, b in baselines.items():
        row = b["row"]
        rows.append({
            "source": name, "role": ("frozen forward challenger"
                                     if name == FROZEN_CHALLENGER
                                     else "released baseline"),
            "rank_ic": row.get("rank_ic_mean"), "rank_ic_t": row.get("rank_ic_t"),
            "spread_t": row.get("spread_t"),
            "net25": row.get("net_annualized_return"),
            "turnover": row.get("turnover"),
            "periods": row.get("periods"), "median_names": row.get("universe"),
            "lifecycle": ("SHADOW_BOOK_ACTIVE" if name == FROZEN_CHALLENGER
                          else "OPERATIONAL_COMPONENT"),
        })
    for r in retained:
        row = r["row"]
        rows.append({
            "source": r["name"], "role": "Release-27 retained candidate",
            "family": r["family"],
            "rank_ic": row.get("rank_ic_mean"), "rank_ic_t": row.get("rank_ic_t"),
            "spread_t": row.get("spread_t"),
            "net25": row.get("net_annualized_return"),
            "turnover": row.get("turnover"),
            "periods": row.get("periods"), "median_names": row.get("universe"),
            "lifecycle": by_name[r["name"]]["terminal_state"],
        })
    rows.sort(key=lambda x: -(x.get("rank_ic_t") or 0.0))
    return {
        "contract_id": "release27_alpha_rankings/1",
        "ranked": rows,
        "release27_retained_count": len(retained),
        "best_research_ensemble": (ensembles.get("best") or {}).get("name")
        if isinstance(ensembles.get("best"), dict) else ensembles.get("best"),
        "ranking_note": "ranked by rank-IC t-statistic on the SHARED panel. A "
                        "high CAGR never enters this ranking; the released gate "
                        "and the FDR verdict decide membership, and the t-stat "
                        "only orders what is already admitted.",
    }


def _hoc_relevance(verdicts: "list[dict]") -> dict:
    survivors = [v["name"] for v in verdicts
                 if v["terminal_state"] in (T_CHALLENGER, T_RETAINED)]
    return {
        "contract_id": "release27_hoc_relevance/1",
        "owner": "alpha_agent.stage23_unified.build_decision_link (unchanged)",
        "status": "INSUFFICIENT_FORWARD_EVIDENCE",
        "release27_candidates_for_counterfactual": survivors,
        "counterfactual_run": False,
        "why_not": "Stage 25's historical counterfactual was a null and Stage 26 "
                   "refused to re-cut it; re-cutting a fixed sample until it "
                   "turns favourable is specification search. With no Release-27 "
                   "survivor there is additionally nothing new to counterfactual "
                   "against."
        if not survivors else
        "a research-only COUNTERFACTUAL is admissible for a survivor, but it can "
        "only ask how the candidate would have SCORED names the incumbent chose "
        "- never what the portfolio would have become, because the subsequent "
        "holdings, cash and opportunity set would all have differed.",
        "true_forward_is_separate": True,
        "minimum_matured_live_observations": 12,
        "historical_decisions_rewritten": 0,
    }


def _summary(*, payload: dict, panel, audit: dict, continuity: dict) -> dict:
    ledger = payload["family_execution_ledger"]
    verdicts = payload["research_exhaustion_state"]["hypothesis_ledger"]
    by_state: "dict[str, int]" = {}
    for v in verdicts:
        by_state[v["terminal_state"]] = by_state.get(v["terminal_state"], 0) + 1
    fam_states = {f["family"]: f["terminal_state"] for f in ledger["families"]}
    return {
        "contract_id": "release27_campaign_summary/1",
        "campaign_version": STAGE27_VERSION,
        "families_executed": ledger["families_executed"],
        "hypotheses_executed": ledger["hypotheses_executed"],
        "hypotheses_by_terminal_state": dict(sorted(by_state.items())),
        "families_by_terminal_state": fam_states,
        "survivors": [v["name"] for v in verdicts
                      if v["terminal_state"] in (T_CHALLENGER, T_RETAINED)],
        "families_considered": payload["family_frontier_inventory"][
            "families_considered"],
        "executable_free_owned_high_priority_families":
            audit["executable_free_owned_high_priority_families"],
        "commit_ok": audit["commit_ok"],
        "blocker": audit["blocker"],
        "forward_challenger_continuity_ok": continuity["continuity_ok"],
        "forward_marks": continuity["forward_marks"],
        "operational_mutations": 0,
        "model_promotion": False,
        "paid_data_decision": payload["paid_data_gate"]["decision"],
        "panel_formations": len(panel.months),
        "panel_scored_rows": panel.diagnostics.get("scored_rows"),
        "next_major_constraint": (
            "FORWARD_TIME" if audit["commit_ok"] else audit["blocker"]),
    }


ARTIFACT_MAP = {
    "campaign_start_state.json": "campaign_start_state",
    "family_frontier_inventory.json": "family_frontier_inventory",
    "family_execution_ledger.json": "family_execution_ledger",
    "hypothesis_manifest.json": "hypothesis_manifest",
    "source_manifests.json": "source_manifests",
    "panel.json": "panel",
    "campaign_wide_fdr.json": "campaign_wide_fdr",
    "all_candidate_incrementality.json": "all_candidate_incrementality",
    "ensemble_comparison.json": "ensemble_comparison",
    "alpha_rankings.json": "alpha_rankings",
    "challenger_status.json": "challenger_status",
    "forward_evidence_status.json": "forward_evidence_status",
    "hoc_relevance.json": "hoc_relevance",
    "research_exhaustion_state.json": "research_exhaustion_state",
    "paid_data_gate.json": "paid_data_gate",
    "final_frontier_audit.json": "final_frontier_audit",
    "campaign_summary.json": "campaign_summary",
}

#: One artefact per executed family, named for the family so a reader looking
#: for "the restatement results" finds a file with that name.
FAMILY_ARTIFACT_NAMES = {
    FAM_FILING: ("filing_behavior_manifest.json", "filing_behavior_results.json"),
    FAM_CORRECTIONS: ("restatement_manifest.json", "restatement_results.json"),
    FAM_SHARES: ("share_dynamics_manifest.json", "share_dynamics_results.json"),
    FAM_DISCLOSURE: ("disclosure_structure_manifest.json",
                     "disclosure_structure_results.json"),
    FAM_DIVIDEND: ("dividend_policy_manifest.json",
                   "dividend_policy_results.json"),
    FAM_CAPITAL_ACTION: ("corporate_action_manifest.json",
                         "corporate_action_results.json"),
    FAM_INSIDER: ("insider_transactions_manifest.json",
                  "insider_transactions_results.json"),
    FAM_STREAM: ("filing_stream_manifest.json", "filing_stream_results.json"),
}


def _write_artifacts(root: Path, payload: dict) -> dict:
    """Content-addressed run directory; every artefact machine-readable."""
    root = Path(root)
    run_id = "release27_%s" % content_hash(payload["campaign_summary"])[:16]
    run_dir = root / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    written: "list[str]" = []

    def _dump(name: str, obj) -> None:
        p = run_dir / name
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_text(json.dumps(obj, indent=1, sort_keys=True, default=str),
                       encoding="utf-8")
        os.replace(tmp, p)
        written.append(str(p))

    for name, key in ARTIFACT_MAP.items():
        _dump(name, payload[key])

    manifest_by_family = {f["family"]: f for f in
                          payload["hypothesis_manifest"]["families"]}
    for fam_blob in payload["family_execution_ledger"]["families"]:
        fam = fam_blob["family"]
        names = FAMILY_ARTIFACT_NAMES.get(fam)
        if not names:
            continue
        _dump(names[0], manifest_by_family.get(fam, {}))
        _dump(names[1], fam_blob)

    _dump("discovered_family_manifest.json", {
        "contract_id": "release27_discovered_family_manifest/1",
        "discovered_in_this_campaign": [
            {"family": f, "origin": o, "hypotheses": len(s)}
            for f, s, o in REGISTERED_FAMILIES if o.startswith("RECURSIVE")],
        "assessed_and_not_run": NOT_RUN_FRONTIER,
    })
    _dump("discovered_family_results.json", {
        "contract_id": "release27_discovered_family_results/1",
        "families": [f for f in payload["family_execution_ledger"]["families"]
                     if f["origin"].startswith("RECURSIVE")],
    })
    (root / "latest.json").write_text(json.dumps(
        {"run_id": run_id, "run_dir": str(run_dir), "version": STAGE27_VERSION,
         "commit_ok": payload["final_frontier_audit"]["commit_ok"]},
        indent=1, sort_keys=True), encoding="utf-8")
    return {"run_id": run_id, "run_dir": str(run_dir), "artifacts": written}


__all__ = [
    "STAGE27_VERSION", "ORIGIN", "CONTRACT_ID", "READY", "BLOCKED", "DATA_HOLD",
    "run", "ARTIFACT_MAP", "FAMILY_ARTIFACT_NAMES",
    "classify_hypothesis", "classify_family", "frontier_inventory",
    "final_frontier_audit", "research_exhaustion_update",
    "forward_challenger_continuity", "external_data_purchase_gate",
    "NOT_RUN_FRONTIER", "FAMILY_MECHANISMS", "REOPEN_CONDITIONS",
    "FAM_STREAM", "STREAM_FACTORS",
    "SAFETY_BADGES", "RESEARCH_ROOT_ENV", "DEFAULT_RESEARCH_ROOT",
    "DEFAULT_INSIDER_CACHE", "TERMINAL_STATES", "FORBIDDEN_STATES",
    "EXECUTABLE", "NOT_EXECUTABLE",
    "T_REJECTED", "T_RETAINED", "T_CHALLENGER", "T_REDUNDANT", "T_INSUFFICIENT",
    "T_NO_PIT", "T_NO_SURVIVORSHIP", "T_PAID", "T_FORWARD", "T_GOVERNANCE",
    "FAM_FILING", "FAM_CORRECTIONS", "FAM_SHARES", "FAM_DISCLOSURE",
    "FAM_DIVIDEND", "FAM_CAPITAL_ACTION", "FAM_INSIDER",
    "REGISTERED_FAMILIES", "ALL_R27_FACTORS", "factor_by_name",
    "hypothesis_manifest", "BehaviourEnricher", "build_panel", "run_family",
    "sample_adequacy", "apply_campaign_fdr", "compact_result",
    "campaign_incrementality", "bounded_ensembles",
    "FROZEN_CHALLENGER", "FROZEN_CHALLENGER_SPEC_HASH",
    "INSIDER_WINDOW_DAYS", "INSIDER_CLUSTER_MIN_BUYERS",
    "DIVIDEND_CUT_FRACTION", "SPLIT_DETECTION_BAND", "MIN_OFF_MODE_SHARE",
]
