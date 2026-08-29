"""alpha_agent.r46.adopted_forward - THE adopted-shadow forward continuation.

Release 46.6 registered the seven prospective shadows five prior releases had
frozen, and proved the thing five releases had asserted: their capture owner is
not broken and their data is not gone. It was never called. From R46.6 the
canonical Daily Research Cycle calls every one of them on every run.

And then they still could not accrue, for a reason R46.6 reported by name
rather than working around: **the only ledger those owners write is the PRIOR
RELEASE's own snapshot ledger**, and
``alpha_agent.r46.contract.SAFETY_BLOCK["mutates_prior_release_artifacts"]`` is
``False``. So the live payload said, correctly and uselessly, that
``r39_vx_weekly`` would decide on 2026-08-28 and that ``append_authorised`` was
``false``. A stream that is called, has something to say, and has nowhere to
say it is not better than a stream nobody calls. It is the same defect wearing
a label.

WHAT THIS MODULE IS
-------------------
ONE R46-owned, append-only, chain-hashed continuation ledger for adopted
shadows, plus the maturity adapter that makes its rows judgeable by the R46
prospective machinery. It is the only place adopted forward evidence is ever
written, and it writes NOTHING ELSE.

WHAT IT DOES NOT DO
-------------------
It never opens a prior release's registry, ledger, model or artifact for write.
:data:`PRIOR_RELEASE_APPEND_AUTHORISED` is ``False`` and stays ``False``; the
frozen safety flag is untouched; the R39/R40 stores are read, hashed before and
after, and proved byte-identical. It creates no second capture implementation:
every signal is produced by calling the ORIGINAL release's own scoring function
on the ORIGINAL release's own panel, with the ORIGINAL frozen specification.

THE CLAUSE THIS SUPERSEDES, SAID OUT LOUD
-----------------------------------------
:data:`SUPERSEDED_ADOPTION_CLAUSE` quotes the frozen R46 adoption clause that
this module changes the scope of, states who changed it and why, and states
exactly what remains forbidden. The frozen contract file is NOT edited: its
``contract_hash`` still binds the sixty-eight predictions already on the record,
and a release that quietly re-hashed the contract those rows were emitted under
would be destroying the evidence it claims to be extending.

TRUE_FORWARD, ENFORCED RATHER THAN ASSERTED
-------------------------------------------
The adopted owners decide at a session's close and measure their horizon from
it. That entry convention is theirs and R46.6.1 does not change it - changing
the entry would change the strategy. What R46 requires, and what this ledger
REFUSES a row without, is that the OUTCOME was unknown at emission: the first
session after the decision date must not have opened yet. A decision date whose
outcome window is already open is not evidence this release is entitled to, and
it is refused by name rather than backfilled. That is why a continuation run on
a Friday emits the Friday and refuses every stale month-end since the freeze.
"""
from __future__ import annotations

import datetime as _dt
from importlib import import_module
from pathlib import Path
from typing import Optional

from . import CAMPAIGN_ID, artifact_body, campaign_dir, read_json, sha, write_json
from . import clock as CK
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r46.adopted_forward"

ARTIFACT = "R46_6_1_ADOPTED_CONTINUATION.json"

CONTINUATION_DIRNAME = "adopted_continuation"
CONTINUATION_LEDGER = "r46_adopted_continuation_predictions.json"
CONTINUATION_OUTCOME_LEDGER = "r46_adopted_continuation_outcomes.json"
LEDGERS = (CONTINUATION_LEDGER, CONTINUATION_OUTCOME_LEDGER)

# --------------------------------------------------------------------------- #
# Governance - the two append rights, never conflated again
# --------------------------------------------------------------------------- #
#: May R46 write into a PRIOR RELEASE's own store? No. Permanently.
PRIOR_RELEASE_APPEND_AUTHORISED = False

#: May R46 write into its OWN continuation ledger? Yes - that is this module.
R46_CONTINUATION_APPEND_AUTHORISED = True

CONTINUATION_OWNER = CALCULATION_OWNER

SUPERSEDED_ADOPTION_CLAUSE = {
    "clause": "alpha_agent.r46.contract.ADOPTION_RULES"
              "['r46_never_writes_a_forward_row_for_an_adopted_shadow']",
    "frozen_value": True,
    "frozen_reason": (
        "adoption exists so ONE leaderboard can show an operator that seven "
        "frozen shadows hold zero forward observations. It does not move "
        "their evidence, their ledgers or their ownership into R46."),
    "amended_by": "R46.6.1",
    "amended_by_whom": "the operator, explicitly, after R46.6 reported the "
                       "blocker by name and left the decision to a person",
    "what_changed": (
        "R46 now writes forward rows for an adopted shadow into an R46-OWNED "
        "continuation ledger. The clause's purpose - that adoption must not "
        "move a prior release's evidence, ledgers or ownership into R46 - is "
        "preserved for the HISTORY it was written about, and is no longer "
        "true of evidence produced AFTER this amendment."),
    "what_remains_forbidden": (
        "writing, editing, re-hashing or deleting any prior-release registry, "
        "ledger, model artifact or hash file; moving a prior release's "
        "existing evidence into R46; re-labelling a prior release's history "
        "as R46 evidence; backdating a continuation row; emitting a "
        "continuation row whose outcome window has already opened",
    ),
    "frozen_contract_file_edited": False,
    "contract_hash_unchanged": True,
    "safety_flag_mutates_prior_release_artifacts": False,
    "why_the_contract_file_was_not_edited": (
        "alpha_agent.r46.contract.contract_hash() is stamped into the "
        "provenance of every prediction already on the record and into the "
        "frozen challenger registry. Re-hashing it to accommodate a later "
        "decision would fork the identity of evidence that was emitted under "
        "the original. The amendment is therefore recorded HERE, in the owner "
        "that acts on it, and surfaced in every artifact this module writes."),
}

#: Vocabulary for the continuation half of an adopted lane. Reported next to -
#: never instead of - the prior-release append right, so no reader can conclude
#: that an old artifact became writable.
CONTINUATION_READY = "READY"
CONTINUATION_PIT_BLOCKED = "PIT_BLOCKED"
CONTINUATION_DATA_BLOCKED = "DATA_BLOCKED"
CONTINUATION_IDENTITY_BLOCKED = "IDENTITY_BLOCKED"
CONTINUATION_NOT_DUE = "QUIET_NOT_DUE"
CONTINUATION_RETIRED = "RETIRED"
CONTINUATION_STATES = (CONTINUATION_READY, CONTINUATION_PIT_BLOCKED,
                       CONTINUATION_DATA_BLOCKED,
                       CONTINUATION_IDENTITY_BLOCKED, CONTINUATION_NOT_DUE,
                       CONTINUATION_RETIRED)

# --------------------------------------------------------------------------- #
# Frozen strategy identity - read from the prior registries, pinned here
# --------------------------------------------------------------------------- #
#: The fields that ARE the strategy. A change in any of them is a different
#: strategy, and a continuation row for a different strategy would be a retune
#: wearing the original's name.
IDENTITY_FIELDS = ("shadow_id", "candidate_id", "lane", "scope", "model",
                   "expression", "cadence", "horizon_sessions", "control",
                   "spec_hash", "coefficient_hash", "frozen_at")

#: Additional immutable terms folded into the identity hash: the cost model and
#: the position sizing decide the economics as much as the signal does.
IDENTITY_EXTRA = ("cost_model_hash", "position_sizing", "hyper", "bundle")

#: The identity of every adopted shadow, computed from the prior releases' own
#: frozen registries at R46.6.1 and pinned as bytes. If a registry ever reads
#: differently, the continuation refuses rather than guesses.
FROZEN_STRATEGY_IDENTITY = {
    "shadow_wide_xs":
        "7df1d351304c75a60117d593a444333c2220faed3203f91defdafdd9b531c850",
    "shadow_carry_rule_xs":
        "66f6385affb23d86030e064995a50cd7852e63537bae58c60402fed1810cdab1",
    "shadow_vx_carry_ts":
        "ec992a66c074cf72bf4184e9abe434436fc9db5e3ead52ff119b91e44041e10b",
    "shadow_intl_rates_carry_rv":
        "8acf69a64e04e33bffeab49c04b0574a04ed36929751a86b4e02f9e9d0baff46",
    "shadow_slot5_c39_fad367467c79":
        "f2c91131943a79f577cf2f5995d1e6d3f352265a307f15f7e2166254c308248c",
}

#: The prior registries' own self-hashes, pinned. Read-only evidence that the
#: file this module read is the file the prior release froze.
FROZEN_REGISTRY_HASH = {
    "R39": "ae7f76daba0467b78f41ce24308fd6532a1bf422c6f30555942ee4599d4f0f4e",
    "R40": "84f217e046a5a0395e8b9f596043f02fd18dd16bd45491a3bcb660bf85fafcc8",
}
FROZEN_REGISTRY_HASH_FIELD = {"R39": "shadow_registry_hash",
                              "R40": "shadow_registry_v2_hash"}
FROZEN_REGISTRY_FROZEN_AT = {"R39": "2026-08-23T04:02:47Z",
                             "R40": "2026-08-23T17:42:31Z"}

#: A learned member is frozen as BYTES. Its ``coefficient_hash`` is recomputed
#: from the stored coefficients on every continuation run - the strongest
#: available proof that nothing was refitted. R40's freezer stamps ``hyper``
#: onto the frozen model AFTER hashing it, so that one key is excluded for R40
#: members; the exclusion is declared, not discovered at runtime.
COEFFICIENT_HASH_EXCLUSIONS = {"R39": ("coefficient_hash",),
                               "R40": ("coefficient_hash", "hyper")}

#: Which panel a lane decides on. Mirrors
#: ``alpha_agent.r40.research_cycle._panel_for`` exactly; it is a MAPPING, not
#: a calculation, and it is stated here so an R39-only continuation never has
#: to import the R40 owner. ``test_the_panel_map_matches_the_r40_owner`` pins
#: the two together.
PANEL_BY_LANE = {"VX": "vx", "FUT_INTL_RATES": "fut_intl_rates"}
PANEL_DEFAULT = "fut"

ASSET_CLASS_BY_LANE = {"VX": "VOLATILITY_FUTURES",
                       "FUT_INTL_RATES": "RATES_FUTURES",
                       "FUT": "FUTURES_MULTI_ASSET"}

ECONOMIC_FAMILY_FALLBACK = "FUTURES_CROSS_SECTION"

# --------------------------------------------------------------------------- #
# The TWO controls, and why neither may ever stand in for the other
# --------------------------------------------------------------------------- #
# "Was research capital better deployed here than in cash?" and "did this frozen
# strategy beat the benchmark it was frozen against?" are DIFFERENT questions.
# An answer to the first is not evidence for the second, and a VX timing rule
# that beats cash while LOSING to passive long VX has produced no alpha at all -
# it has produced a worse way to hold the same risk.
#
# R46.6.1 therefore computes both, keeps them apart in the record, and lets only
# the frozen scientific control decide a formal verdict.
CONTROL_RISK_MATCHED_CASH = "RISK_MATCHED_CASH"
CONTROL_VOL_MATCHED_PASSIVE = "VOL_MATCHED_PASSIVE_EW_SAME_SCOPE"

CONTROL_DESCRIPTION = {
    CONTROL_RISK_MATCHED_CASH:
        "the zero-excess line a SELF-FINANCED book is measured against: a "
        "dollar-neutral futures book ties up collateral, its forward returns "
        "are already excess of financing, and the adopted owner scores it "
        "against zero",
    CONTROL_VOL_MATCHED_PASSIVE:
        "the passive equal-weight basket of the SAME scope over the same "
        "horizon: same dates, same instruments, same cost model, none of the "
        "timing",
}

#: A: the SCIENTIFIC control. Frozen by the adopted strategy itself, computed
#: HERE by the prior release's own implementation. R46.6.1 defines no control
#: of its own and searches no parameter.
SCIENTIFIC_CONTROL_OWNER = {
    CONTROL_VOL_MATCHED_PASSIVE:
        "alpha_agent.r39.trade_space.passive_ew_control",
    CONTROL_RISK_MATCHED_CASH:
        "alpha_agent.r39.discovery_director (control_net = zeros)",
}

#: The exact line of the original implementation each control is taken from, so
#: a reader can check the claim rather than believe it.
SCIENTIFIC_CONTROL_DEFINITION = {
    CONTROL_VOL_MATCHED_PASSIVE:
        "alpha_agent.r39.discovery_director pairs TS_OUTRIGHT with "
        "T.passive_ew_control(fwd_matrix, cost, cost_multiplier=...) and takes "
        "its NET path; R46.6.1 calls that same function, on the adopted "
        "owner's own panel, over the scope's own markets, priced by the "
        "shadow's OWN FROZEN cost model",
    CONTROL_RISK_MATCHED_CASH:
        "alpha_agent.r39.discovery_director sets control_net = np.zeros(...) "
        "for every self-financed expression; the control return is 0.0 in the "
        "units the strategy's own returns are measured in, and R46.6.1 "
        "reproduces that rather than substituting a cash rate",
}

SCIENTIFIC_CONTROL_OK = "OK"
SCIENTIFIC_CONTROL_BLOCKED_UNKNOWN = \
    "BLOCKED_UNKNOWN_DECLARED_CONTROL_NOT_IMPLEMENTED_BY_THE_ADOPTED_OWNER"
SCIENTIFIC_CONTROL_BLOCKED_NO_SCOPE = \
    "BLOCKED_THE_SCOPE_IS_ABSENT_FROM_THE_ADOPTED_OWNERS_PANEL"
SCIENTIFIC_CONTROL_BLOCKED_NO_DATE = \
    "BLOCKED_THE_DECISION_DATE_IS_ABSENT_FROM_THE_CONTROL_PATH"
SCIENTIFIC_CONTROL_BLOCKED_NOT_PIT = \
    "BLOCKED_THE_CONTROL_CANNOT_BE_RECONSTRUCTED_FROM_THE_PIT_SAFE_PANEL"
SCIENTIFIC_CONTROL_BLOCKED_DRIFT = \
    "BLOCKED_THE_DECLARED_CONTROL_CHANGED_SINCE_THE_ROW_WAS_EMITTED"
SCIENTIFIC_CONTROL_BLOCKED_OWNER = \
    "BLOCKED_THE_ORIGINAL_CONTROL_OWNER_COULD_NOT_BE_CALLED"

#: B: the CAPITAL opportunity-cost control. R46's own, canonical, and the same
#: one every other row on the board is scored against.
CAPITAL_CONTROL = C.CONTROL_CASH
CAPITAL_CONTROL_DESCRIPTION = (
    "risk-free accrual on the capital the book ties up (%s)"
    % C.RISK_FREE_SERIES)

SCIENTIFIC_QUESTION = ("did this frozen strategy beat the control it was "
                       "frozen against?")
CAPITAL_QUESTION = ("was research capital better deployed here than in cash?")

SCIENTIFIC_ALPHA_FIELD = "scientific_alpha_vs_declared_control"
CAPITAL_ALPHA_FIELD = "capital_alpha_vs_cash"

#: The gate section 4 of R46.6.1 asks for, stated where the numbers are made.
FORMAL_VERDICT_USES = SCIENTIFIC_ALPHA_FIELD
CASH_SUBSTITUTION_FOR_NONCASH_CONTROL_ALLOWED = False
CAPITAL_ALPHA_IS_NOT_A_VERDICT_INPUT = (
    "a formal scientific verdict (POSITIVE_EARLY, SHADOW_SCALE_CANDIDATE, "
    "FORWARD_CONFIRMED) may never be earned from capital alpha vs cash by a "
    "strategy whose frozen control is something else; where that control "
    "cannot be computed the scientific verdict stays blocked and only the "
    "capital number is displayed")

#: A discrepancy inside R39 itself, recorded rather than resolved by fiat.
#: ``research_shadow.register()`` freezes the VX shadow's control as
#: VOL_MATCHED_PASSIVE_EW_SAME_SCOPE (TS_OUTRIGHT is not XS_LONG_SHORT), and
#: ``trade_space.EXPRESSION_CONTROLS`` and ``discovery_director`` agree. But
#: ``universal_state.build_vx_weekly`` carries ``control_fwd_5 = 0.0`` and says
#: in its own docstring that the VX lane's control is risk-matched cash because
#: the lane holds one market. The FROZEN REGISTRY is the artifact the shadow was
#: frozen under and it is what R46.6.1 follows; with one market in scope the
#: passive EW basket is a passive LONG VX holding, which is a real and demanding
#: benchmark for a VX timing rule - and emphatically not zero.
VX_CONTROL_DISCREPANCY = {
    "frozen_registry_says": CONTROL_VOL_MATCHED_PASSIVE,
    "frozen_registry_owner": "alpha_agent.r39.research_shadow.register",
    "panel_column_says": "control_fwd_5 = 0.0 (RISK_MATCHED_CASH)",
    "panel_owner": "alpha_agent.r39.universal_state.build_vx_weekly",
    "r46_6_1_follows": "THE_FROZEN_REGISTRY",
    "why": "the registry is the artifact the shadow was frozen under, the "
           "expression map and the historical director both pair TS_OUTRIGHT "
           "with the passive basket, and a one-market scope makes that basket "
           "a passive LONG VX holding - the demanding benchmark, not the "
           "trivial one",
    "prior_release_artifact_mutated": False,
}

ENTRY_CONVENTION = "ADOPTED_OWNER_DECISION_CLOSE"
ENTRY_CONVENTION_STATEMENT = (
    "the adopted owner enters at the CLOSE of its own decision date and "
    "measures its horizon in sessions of its own panel from that close. "
    "R46.6.1 does not change it - changing the entry would change the "
    "strategy, and section 4 of this release forbids that. The R46 "
    "TRUE_FORWARD requirement is met on the OUTCOME instead: the row is "
    "refused unless it is emitted before the first session AFTER the decision "
    "date has opened, so the outcome was genuinely unknown at emission.")

HORIZON_UNIT = "ELIGIBLE_SESSIONS_OF_THE_ADOPTED_OWNERS_PANEL"

# --------------------------------------------------------------------------- #
# The record
# --------------------------------------------------------------------------- #
CONTINUATION_RECORD_FIELDS = (
    "continuation_id",
    "adopted_challenger_id", "adopted_candidate_id",
    "adopted_from_release", "source_owner", "source_registry_path",
    "source_registry_hash",
    "original_spec_hash", "original_coefficient_hash", "original_frozen_at",
    "strategy_identity_hash", "spec_identity",
    "decision_date", "emitted_at_utc", "emitted_at_utc_precise",
    "outcome_window_start_utc", "entry_convention",
    "horizon", "horizon_unit",
    "economic_family", "information_family",
    "asset_class", "instrument", "instrument_identity", "venue",
    "prediction_type", "direction",
    "position_expression", "weights", "n_legs", "n_predictions",
    "gross_notional",
    "control", "control_description", "r46_control",
    "scientific_control", "capital_control", "formal_verdict_uses",
    "cost_model", "source_state_hash", "input_freshness_stale_sources",
    "evidence_class", "point_in_time_status", "status",
    "prior_release_artifact_mutated", "provenance", "calculation_owner",
)

#: The idempotency key. The same challenger, deciding on the same date, at the
#: same horizon, under the same frozen specification, may never append twice.
CONTINUATION_IDENTITY_KEY = ("adopted_challenger_id", "decision_date",
                             "horizon", "spec_identity")

OUTCOME_IDENTITY_KEY = ("continuation_id",)

#: Why a due decision date produced no row. Never silent, never a bare skip.
SKIP_OUTCOME_WINDOW_OPEN = "OUTCOME_WINDOW_ALREADY_OPEN"
SKIP_SIGNAL_UNAVAILABLE = "OWNER_PRODUCED_NO_SIGNAL"
SKIP_DUPLICATE = "ALREADY_IN_THE_CONTINUATION_LEDGER"

# --------------------------------------------------------------------------- #
# Release 46.6.2 - CAN this lane ever emit, and did anyone say which date was
# refused?
# --------------------------------------------------------------------------- #
#: R46.6.1's lane artifact reported ``n_refused_outcome_window_open = 1`` and
#: never said WHICH decision date was refused, so the 2026-08-28 production
#: event could not be adjudicated from the artifact at all - it had to be
#: reconstructed from the owner's panel. A refusal that does not name its own
#: date is the same defect as a lane nobody calls: the state is recorded and
#: the fact is not. Every refusal now carries its decision date, its first
#: outcome session, the instant its window opened and how late the attempt was.
REFUSAL_EVIDENCE_FIELDS = ("decision_date", "decision_weekday",
                           "first_outcome_session",
                           "outcome_window_start_utc", "hours_late")

#: Whether this lane's OWN decision grid can ever expose a decision date before
#: that date's outcome window opens. Measured on the panel, never assumed.
EMISSION_CAN_EMIT = "CAN_EMIT"
EMISSION_STRUCTURALLY_LATE = "STRUCTURALLY_LATE"
EMISSION_LATE_THIS_RUN = "LATE_THIS_RUN"
EMISSION_NOTHING_DUE = "NOTHING_DUE"
EMISSION_FEASIBILITY = (EMISSION_CAN_EMIT, EMISSION_STRUCTURALLY_LATE,
                        EMISSION_LATE_THIS_RUN, EMISSION_NOTHING_DUE)

#: THE measurement behind ``STRUCTURALLY_LATE``, stated exactly.
#:
#: A decision date D becomes visible to this owner only once its panel holds
#: the row for D. If the panel ALSO holds a session strictly after D, then that
#: session's own date is >= ``next_weekday(D)``, so midnight Eastern of
#: ``next_weekday(D)`` - the instant :func:`outcome_window_start` returns - has
#: already passed. The refusal is then not a scheduling accident that an
#: earlier run could have avoided: no run could ever have been early enough.
#:
#: This is why ``r39_vx_weekly`` refused on 2026-08-28 and why it will refuse
#: every time. :func:`alpha_agent.r39.universal_state.build_vx_weekly` walks
#: ``range(260, len(sessions) - 1, 5)``, so its newest decision date is ALWAYS
#: at least one session short of the panel end. Measured on the live panel that
#: day: latest VX session 2026-08-28, latest decision date 2026-08-25, first
#: outcome session 2026-08-26, window open from 2026-08-26T04:00:00Z, emission
#: attempted 2026-08-28T23:18:37Z - 67.3 hours late.
#:
#: Neither side of that is edited to make evidence appear. Widening the grid
#: would change a frozen strategy; relaxing the refusal would let R46 record an
#: outcome it already knew. The honest report is that this stream cannot
#: produce TRUE_FORWARD continuation evidence, said in those words.
STRUCTURALLY_LATE_TEST = (
    "the panel holds at least one session strictly after the newest decision "
    "date, so that decision's outcome window had already opened by the time "
    "the decision date existed to be read")

VX_CADENCE_DISCREPANCY = {
    "lane": "r39_vx_weekly",
    "lane_due_predicate_says": "WEEKLY_ON_FRIDAY "
                               "(alpha_agent.r46.lanes.due_weekly_friday)",
    "frozen_owner_grid_says": "every 5th VX session from index 260 "
                              "(alpha_agent.r39.universal_state."
                              "build_vx_weekly)",
    "they_are_not_the_same_rule": True,
    "measured_2026_08_28": {
        "latest_vx_session": "2026-08-28",
        "latest_owner_decision_date": "2026-08-25",
        "latest_owner_decision_weekday": "Tuesday",
    },
    "consequence": "R46.6 reported 'next decision 2026-08-28' for this lane. "
                   "That was the next FRIDAY, not the next date the frozen "
                   "owner decides on. The call cadence and the owner's "
                   "decision grid are now reported apart: next_call_date is "
                   "the cheap predicate's answer, next_decision_date is the "
                   "owner's, and a null owner answer is never filled in from "
                   "the calendar.",
    "prior_release_artifact_mutated": False,
}


class ContinuationRefusal(Exception):
    """Raised when a row may not enter the continuation ledger."""


# --------------------------------------------------------------------------- #
# Module loading - one root, whichever name the package was imported under
# --------------------------------------------------------------------------- #
def _owner_module(dotted: str):
    """Import a prior release's owner from the SAME package root as this one.

    ``alpha_agent`` is importable both top-level and as ``paper_trader
    .alpha_agent``; parts of the R40 lineage reach ``...engine`` and resolve
    only under the latter. The fallback is explicit rather than incidental.
    """
    root = __name__.rsplit(".", 2)[0]
    try:
        return import_module("%s.%s" % (root, dotted))
    except ImportError:
        return import_module("paper_trader.alpha_agent.%s" % dotted)


def _r39_owner():
    return _owner_module("r39.research_shadow")


def _r40_owner():
    return _owner_module("r40.shadow_registry")


def _r40_cycle():
    return _owner_module("r40.research_cycle")


def _desk():
    from paper_trader.api import paper_trading_desk as desk
    return desk


# --------------------------------------------------------------------------- #
# Ledger
# --------------------------------------------------------------------------- #
def continuation_dir(campaign_id: str = CAMPAIGN_ID) -> Path:
    d = campaign_dir(campaign_id) / CONTINUATION_DIRNAME
    d.mkdir(parents=True, exist_ok=True)
    return d


def predictions(campaign_id: str = CAMPAIGN_ID) -> list:
    return _desk()._read_ledger(continuation_dir(campaign_id),
                                CONTINUATION_LEDGER)


def outcomes(campaign_id: str = CAMPAIGN_ID) -> list:
    return _desk()._read_ledger(continuation_dir(campaign_id),
                                CONTINUATION_OUTCOME_LEDGER)


def verify(campaign_id: str = CAMPAIGN_ID) -> dict:
    desk = _desk()
    d = continuation_dir(campaign_id)
    reports = [desk.verify_ledger(d, f) for f in LEDGERS]
    return {"all_intact": all(r["intact"] for r in reports),
            "ledgers": reports,
            "primitives": "api.paper_trading_desk chain-hash ledgers "
                          "(canonical)"}


def continuation_key(row: dict) -> tuple:
    return tuple(str(row.get(k)) for k in CONTINUATION_IDENTITY_KEY)


def continuation_id(adopted_challenger_id: str, decision_date: str,
                    horizon: int, spec_identity: str) -> str:
    """Deterministic. The same decision can never receive two identities."""
    return "r46c_" + sha({"adopted_challenger_id": adopted_challenger_id,
                          "decision_date": str(decision_date),
                          "horizon": int(horizon),
                          "spec_identity": str(spec_identity)})[:20]


def existing_keys(campaign_id: str = CAMPAIGN_ID) -> set:
    return {continuation_key(r) for r in predictions(campaign_id)}


def validate(row: dict) -> None:
    missing = [f for f in CONTINUATION_RECORD_FIELDS if f not in row]
    if missing:
        raise ContinuationRefusal(
            "continuation row is missing required fields: %s"
            % ", ".join(sorted(missing)))
    if row.get("evidence_class") != C.TRUE_FORWARD:
        raise ContinuationRefusal(
            "this ledger holds TRUE_FORWARD rows only; got %r"
            % row.get("evidence_class"))
    if row.get("status") != C.STATUS_PENDING:
        raise ContinuationRefusal(
            "a continuation row enters as %s; got %r"
            % (C.STATUS_PENDING, row.get("status")))
    if row.get("prior_release_artifact_mutated") is not False:
        raise ContinuationRefusal(
            "REFUSED - a continuation row may only be written when no prior "
            "release artifact was mutated to produce it")
    emitted = CK.parse_iso(row.get("emitted_at_utc_precise")
                           or row.get("emitted_at_utc"))
    start = CK.parse_iso(row.get("outcome_window_start_utc"))
    if emitted is None or start is None:
        raise ContinuationRefusal(
            "emitted_at_utc and outcome_window_start_utc are both required to "
            "prove the ordering")
    if not emitted < start:
        raise ContinuationRefusal(
            "REFUSED - not TRUE_FORWARD: emitted at %s is not strictly before "
            "the outcome window opening at %s. The decision date's outcome "
            "was already being determined; this release does not backfill it."
            % (row.get("emitted_at_utc"),
               row.get("outcome_window_start_utc")))
    if row.get("point_in_time_status") == C.PIT_VIOLATION:
        raise ContinuationRefusal("REFUSED - row carries PIT_VIOLATION")


def append(rows: list, campaign_id: str = CAMPAIGN_ID) -> dict:
    """Append continuation rows. Duplicates are skipped, never overwritten."""
    seen = existing_keys(campaign_id)
    fresh, duplicates = [], []
    for row in rows:
        validate(row)
        k = continuation_key(row)
        if k in seen:
            duplicates.append({"key": list(k),
                               "continuation_id": row.get("continuation_id")})
            continue
        seen.add(k)
        fresh.append(row)
    appended = []
    if fresh:
        appended = _desk()._append_ledger(continuation_dir(campaign_id),
                                          CONTINUATION_LEDGER, fresh)
    return {"n_offered": len(rows), "n_appended": len(appended),
            "n_duplicates_skipped": len(duplicates), "duplicates": duplicates,
            "appended": appended, "idempotent": True,
            "prior_release_ledger_written": False}


def append_outcomes(rows: list, campaign_id: str = CAMPAIGN_ID) -> dict:
    seen = {str(r.get("continuation_id")) for r in outcomes(campaign_id)}
    fresh, duplicates = [], []
    for row in rows:
        cid = str(row.get("continuation_id"))
        if not cid or cid == "None":
            raise ContinuationRefusal("an outcome row must name its "
                                      "continuation_id")
        if row.get("forward_evidence_type") != C.TRUE_FORWARD:
            raise ContinuationRefusal("outcome rows score TRUE_FORWARD "
                                      "continuation rows only")
        if cid in seen:
            duplicates.append({"continuation_id": cid})
            continue
        seen.add(cid)
        fresh.append(row)
    appended = []
    if fresh:
        appended = _desk()._append_ledger(continuation_dir(campaign_id),
                                          CONTINUATION_OUTCOME_LEDGER, fresh)
    return {"n_offered": len(rows), "n_appended": len(appended),
            "n_duplicates_skipped": len(duplicates), "duplicates": duplicates,
            "appended": appended, "idempotent": True,
            "prior_release_ledger_written": False}


# --------------------------------------------------------------------------- #
# Frozen specification identity
# --------------------------------------------------------------------------- #
def source(release: str) -> dict:
    """The prior release's declared registry source. READ ONLY, always."""
    return next((dict(a) for a in C.ADOPTED_REGISTRY_SOURCES
                 if a["release"] == release), {})


def load_source_registry(release: str) -> Optional[dict]:
    """Read the prior release's frozen registry. Never opened for write."""
    src = source(release)
    if not src.get("path"):
        return None
    return read_json(Path(src["path"]), default=None)


def registry_shadows(release: str, registry: dict) -> list:
    """Every shadow row this release OWNS.

    R40's registry-v2 carries the three R39 members by reference; their owner
    is still R39 and they are excluded here so no shadow is ever continued
    twice under two releases' names.
    """
    rows = list((registry or {}).get("shadows") or ())
    if release == "R40":
        return [r for r in rows if r.get("origin_release") == "release40"]
    return rows


def strategy_identity(shadow: dict) -> dict:
    """The immutable terms of one shadow's strategy."""
    fm = shadow.get("frozen_model") or {}
    body = {f: shadow.get(f) for f in IDENTITY_FIELDS}
    body["coefficient_hash"] = (shadow.get("coefficient_hash")
                                or fm.get("coefficient_hash"))
    body["cost_model_hash"] = sha(shadow.get("cost_model"))
    body["position_sizing"] = shadow.get("position_sizing")
    body["hyper"] = shadow.get("hyper")
    body["bundle"] = shadow.get("bundle")
    return body


def strategy_identity_hash(shadow: dict) -> str:
    return sha(strategy_identity(shadow))


def coefficient_evidence(release: str, shadow: dict) -> dict:
    """Recompute a frozen learned model's hash from its own stored bytes."""
    fm = shadow.get("frozen_model") or {}
    declared = shadow.get("coefficient_hash") or fm.get("coefficient_hash")
    if not fm:
        return {"state": "NO_LEARNED_MODEL", "declared": declared,
                "recomputed": None, "matches": None}
    # The hash was produced by the R39 package's own hashing owner; it is the
    # only function that can reproduce it.
    r39pkg = _owner_module("r39")
    excl = COEFFICIENT_HASH_EXCLUSIONS.get(release, ("coefficient_hash",))
    recomputed = r39pkg.sha({k: v for k, v in fm.items() if k not in excl})
    return {"state": "RECOMPUTED_FROM_FROZEN_BYTES", "declared": declared,
            "recomputed": recomputed, "matches": recomputed == declared,
            "excluded_keys": list(excl)}


def verify_identity(release: str, shadow_ids: tuple,
                    registry: dict = None) -> dict:
    """Prove the strategy that is about to speak is the one that was frozen."""
    reg = registry if registry is not None else load_source_registry(release)
    src = source(release)
    if not reg:
        return {"ok": False, "blocker": "NO_SOURCE_REGISTRY",
                "release": release,
                "reason": "the prior release's frozen registry could not be "
                          "read at %s" % src.get("path"),
                "rows": []}
    field = FROZEN_REGISTRY_HASH_FIELD.get(release)
    reg_hash = reg.get(field) if field else None
    rows, ok = [], True
    by_id = {s.get("shadow_id"): s for s in registry_shadows(release, reg)}
    for sid in shadow_ids:
        sh = by_id.get(sid)
        if sh is None:
            rows.append({"shadow_id": sid, "ok": False,
                         "blocker": "SHADOW_ABSENT_FROM_SOURCE_REGISTRY"})
            ok = False
            continue
        h = strategy_identity_hash(sh)
        expected = FROZEN_STRATEGY_IDENTITY.get(sid)
        coef = coefficient_evidence(release, sh)
        row_ok = bool(expected) and h == expected and coef["matches"] is not False
        rows.append({
            "shadow_id": sid,
            "candidate_id": sh.get("candidate_id"),
            "strategy_identity_hash": h,
            "expected_strategy_identity_hash": expected,
            "identity_matches_freeze": bool(expected) and h == expected,
            "original_spec_hash": sh.get("spec_hash"),
            "original_frozen_at": sh.get("frozen_at"),
            "coefficient_evidence": coef,
            "ok": row_ok,
            "blocker": (None if row_ok else
                        "IDENTITY_DRIFT_SINCE_FREEZE"
                        if expected else "NO_FROZEN_IDENTITY_ON_RECORD"),
        })
        ok = ok and row_ok
    reg_ok = (reg_hash == FROZEN_REGISTRY_HASH.get(release))
    frozen_at_ok = (str(reg.get("frozen_at"))
                    == FROZEN_REGISTRY_FROZEN_AT.get(release))
    return {
        "ok": bool(ok and reg_ok and frozen_at_ok),
        "release": release,
        "source_owner": src.get("owner"),
        "source_registry_path": src.get("path"),
        "source_registry_hash": reg_hash,
        "expected_source_registry_hash": FROZEN_REGISTRY_HASH.get(release),
        "source_registry_hash_matches": reg_ok,
        "source_registry_frozen_at": reg.get("frozen_at"),
        "source_registry_frozen_at_matches": frozen_at_ok,
        "blocker": (None if (ok and reg_ok and frozen_at_ok)
                    else "SOURCE_REGISTRY_HASH_DRIFT" if not reg_ok
                    else "SOURCE_REGISTRY_FREEZE_TIMESTAMP_DRIFT"
                    if not frozen_at_ok else "SPECIFICATION_IDENTITY_DRIFT"),
        "rows": rows,
        "prior_release_artifact_mutated": False,
    }


# --------------------------------------------------------------------------- #
# Panels, signals and eligibility - ALWAYS the prior owner's own functions
# --------------------------------------------------------------------------- #
def panel_for(shadow: dict, state: dict):
    return (state or {}).get(
        PANEL_BY_LANE.get(shadow.get("lane"), PANEL_DEFAULT))


#: R40's freeze wrote a DISPLAY string into its registry rows' ``model`` field
#: - ``"rule:carry_slope_ann (no parameters)"`` - while its own scorer,
#: :func:`alpha_agent.r40.shadow_registry.score_at`, reads everything after the
#: colon as a COLUMN NAME. The R40 slot-4 shadow therefore could never have
#: scored through its own owner: the column ``"carry_slope_ann (no
#: parameters)"`` does not exist and the owner returns ``None``. R39's owner is
#: unaffected because it matches with ``startswith``.
#:
#: The frozen SPECIFICATION does not carry the suffix. R40's
#: ``slot4_candidate()`` declares ``model="rule:carry_slope_ann"``, and the
#: ``spec_hash`` on the registry row - ``935e3de5...`` - was computed from that
#: candidate, not from the display string. Passing the owner the model name its
#: own spec hash was taken over RESTORES the frozen specification; it does not
#: change it. The suffix stripped is exactly this one, only on ``rule:`` models,
#: and every emitted row records which name the owner was given.
MODEL_DISPLAY_SUFFIX = " (no parameters)"


def frozen_model_name(shadow: dict) -> str:
    """The model name the shadow's own spec hash was computed over."""
    model = str(shadow.get("model") or "")
    if model.startswith("rule:") and model.endswith(MODEL_DISPLAY_SUFFIX):
        return model[:-len(MODEL_DISPLAY_SUFFIX)]
    return model


def model_name_was_normalised(shadow: dict) -> bool:
    return frozen_model_name(shadow) != str(shadow.get("model") or "")


def signal(release: str, shadow: dict, panel, decision_date) -> Optional[dict]:
    """The weights the ORIGINAL owner would produce at this decision date.

    R39 members go through ``alpha_agent.r39.research_shadow._target_snapshot``
    and R40 members through ``alpha_agent.r40.shadow_registry.score_at`` - the
    same two functions the prior releases' own capture paths call. Neither
    writes anything, and neither is reimplemented here: a second copy of a
    frozen strategy is a retune waiting to happen.
    """
    import pandas as pd
    if panel is None or not len(panel):
        return None
    if release == "R39":
        snap = _r39_owner()._target_snapshot(shadow, panel,
                                             pd.Timestamp(decision_date), None)
        return None if snap is None else dict(snap.get("weights") or {})
    if release == "R40":
        rows_d = panel[pd.to_datetime(panel["decision_date"])
                       == pd.Timestamp(decision_date)]
        spec = dict(shadow, model=frozen_model_name(shadow))
        w = _r40_owner().score_at(spec, rows_d)
        return None if w is None else {k: float(v) for k, v in w.items()}
    return None


def signal_owner(release: str) -> str:
    return ("alpha_agent.r39.research_shadow._target_snapshot"
            if release == "R39"
            else "alpha_agent.r40.shadow_registry.score_at")


def due_decision_dates(shadow: dict, panel, captured: set) -> list:
    """Decision dates strictly after the shadow's own freeze, in the CURRENT
    panel, not already in the R46 continuation ledger."""
    import pandas as pd
    if panel is None or not len(panel):
        return []
    frozen_at = pd.Timestamp(str(shadow["frozen_at"]).replace("Z", ""))
    dates = sorted(pd.to_datetime(panel["decision_date"]).unique())
    return [d for d in dates
            if d > frozen_at and str(pd.Timestamp(d).date()) not in captured]


def outcome_window_start(decision_date) -> _dt.datetime:
    """The instant the decision date's outcome starts being determined.

    The adopted owner enters at the decision close; what it does NOT yet know
    is what happens in the sessions after it. So the window opens at midnight
    Eastern of the first weekday AFTER the decision date - computed by the
    canonical R46 clock, never by a second calendar.
    """
    d = _as_date(decision_date)
    return CK.outcome_window_start_utc(CK.next_weekday(d))


def _as_date(value) -> _dt.date:
    if isinstance(value, _dt.datetime):
        return value.date()
    if isinstance(value, _dt.date):
        return value
    return _dt.date.fromisoformat(str(value)[:10])


# --------------------------------------------------------------------------- #
# Release 46.6.2 - the refusal says WHICH date, and whether any run could have
# been early enough
# --------------------------------------------------------------------------- #
def refusal_evidence(decision_date, emitted_at: _dt.datetime) -> dict:
    """Everything an auditor needs to adjudicate ONE refusal, by itself.

    R46.6.1 recorded that a decision date had been refused and not which one,
    so the only way to check the gate was to rebuild the owner's panel. This
    is that check, written down at the moment the refusal happens.
    """
    d = _as_date(decision_date)
    first = CK.next_weekday(d)
    start = CK.outcome_window_start_utc(first)
    return {
        "decision_date": str(d),
        "decision_weekday": d.strftime("%A"),
        "first_outcome_session": str(first),
        "first_outcome_session_weekday": first.strftime("%A"),
        "outcome_window_start_utc": CK.iso(start),
        "emission_attempted_utc": CK.iso(emitted_at),
        "hours_late": round((emitted_at - start).total_seconds() / 3600.0, 4),
        "reason": SKIP_OUTCOME_WINDOW_OPEN,
    }


def panel_sessions(panel) -> list:
    """The decision dates this panel actually carries, ascending."""
    import pandas as pd
    if panel is None or not len(panel):
        return []
    return [pd.Timestamp(d).date()
            for d in sorted(pd.to_datetime(panel["decision_date"]).unique())]


def owner_session_axis(state: dict, shadow: dict) -> list:
    """The SESSION dates the adopted owner's OWN source layer actually printed.

    Not the decision dates. The two differ whenever the owner's grid stops
    short of its own data - which is exactly the condition
    :data:`STRUCTURALLY_LATE_TEST` measures, and it cannot be seen from the
    decision column alone. ``build_fresh_state`` carries the raw per-market
    layer beside the panels precisely so this stays observable.
    """
    import pandas as pd
    layer = (state or {}).get("layer") or {}
    if not layer:
        return []
    lane = str(shadow.get("lane") or "")
    frames = ([layer["VX"]] if lane == "VX" and "VX" in layer
              else list(layer.values()))
    seen = set()
    for f in frames:
        if f is None or not len(f) or "Date" not in getattr(f, "columns", ()):
            continue
        seen.update(pd.Timestamp(d).date()
                    for d in pd.to_datetime(f["Date"]).unique())
    return sorted(seen)


def emission_feasibility(*, due: list, panel, emitted_at: _dt.datetime,
                         sessions: list = None) -> dict:
    """Could ANY run have emitted these due decision dates in time?

    ``STRUCTURALLY_LATE`` is a statement about the owner's grid, not about this
    run's punctuality: see :data:`STRUCTURALLY_LATE_TEST`. It is measured, from
    the owner's own session axis where one is available, and it is never
    asserted from a lane name. Without a session axis the honest answer is
    ``LATE_THIS_RUN`` - the refusal is recorded, the structural claim is not
    made.
    """
    axis = list(sessions or [])
    decisions = panel_sessions(panel)
    latest_session = (axis[-1] if axis
                      else (decisions[-1] if decisions else None))
    if not due:
        return {"state": EMISSION_NOTHING_DUE,
                "owner_latest_session": (str(latest_session) if latest_session
                                         else None),
                "owner_latest_decision_date": (str(decisions[-1]) if decisions
                                               else None),
                "reason": "no decision date is due for this lane"}
    newest_due = max(_as_date(d) for d in due)
    window = outcome_window_start(newest_due)
    after = [d for d in axis if d > newest_due]
    common = {
        "owner_latest_session": (str(latest_session) if latest_session
                                 else None),
        "owner_latest_decision_date": (str(decisions[-1]) if decisions
                                       else None),
        "newest_due_decision_date": str(newest_due),
        "n_sessions_after_newest_due_decision_date": (len(after) if axis
                                                      else None),
        "outcome_window_start_utc": CK.iso(window),
        "session_axis_available": bool(axis),
    }
    if emitted_at < window:
        return dict(common, state=EMISSION_CAN_EMIT,
                    reason=("the newest due decision date's outcome window "
                            "has not opened yet"))
    if axis and after:
        return dict(
            common, state=EMISSION_STRUCTURALLY_LATE,
            test=STRUCTURALLY_LATE_TEST,
            reason=("the newest decision date this owner will produce is %s, "
                    "and its own session axis already held %d session(s) "
                    "after it, so the outcome window (from %s) had opened "
                    "before that decision date could be read at all. No "
                    "earlier run could have emitted it: the grid, not the "
                    "schedule, is what makes it late."
                    % (newest_due, len(after), CK.iso(window))))
    return dict(common, state=EMISSION_LATE_THIS_RUN,
                reason=("the newest due decision date's outcome window opened "
                        "at %s, before this run. Whether an earlier run could "
                        "have caught it is NOT claimed here - the owner's "
                        "session axis was not available to decide it."
                        % CK.iso(window)))


# --------------------------------------------------------------------------- #
# Building one continuation record
# --------------------------------------------------------------------------- #
def build_record(*, release: str, shadow: dict, identity_row: dict,
                 identity: dict, weights: dict, decision_date,
                 information_family: str, emitted_at: _dt.datetime,
                 source_state_hash: str,
                 stale_sources: list = None) -> dict:
    d = str(_as_date(decision_date))
    horizon = int(shadow.get("horizon_sessions") or 0)
    spec_identity = (shadow.get("spec_hash")
                     or identity_row.get("strategy_identity_hash"))
    legs = [{"instrument": m, "weight": float(w), "score": None,
             "side": "LONG" if float(w) > 0 else "SHORT",
             "cost_class": None}
            for m, w in sorted(weights.items())]
    gross_notional = float(sum(abs(l["weight"]) for l in legs))
    net = float(sum(l["weight"] for l in legs))
    control = shadow.get("control")
    cost_model = dict(shadow.get("cost_model") or {})
    return {
        "continuation_id": continuation_id(shadow["shadow_id"], d, horizon,
                                           spec_identity),
        "adopted_challenger_id": shadow["shadow_id"],
        "adopted_candidate_id": shadow.get("candidate_id"),
        "adopted_from_release": release,
        "source_owner": identity.get("source_owner"),
        "source_registry_path": identity.get("source_registry_path"),
        "source_registry_hash": identity.get("source_registry_hash"),

        "original_spec_hash": shadow.get("spec_hash"),
        "original_coefficient_hash":
            (identity_row.get("coefficient_evidence") or {}).get("declared"),
        "original_frozen_at": shadow.get("frozen_at"),
        "strategy_identity_hash": identity_row.get("strategy_identity_hash"),
        "spec_identity": spec_identity,

        "decision_date": d,
        "emitted_at_utc": CK.iso(emitted_at),
        "emitted_at_utc_precise": CK.iso_precise(emitted_at),
        "outcome_window_start_utc": CK.iso(outcome_window_start(d)),
        "entry_convention": ENTRY_CONVENTION,

        "horizon": horizon,
        "horizon_unit": HORIZON_UNIT,
        "economic_family": shadow.get("family") or ECONOMIC_FAMILY_FALLBACK,
        "information_family": information_family,

        "asset_class": ASSET_CLASS_BY_LANE.get(shadow.get("lane"),
                                               "FUTURES_MULTI_ASSET"),
        "instrument": "BOOK:%s" % shadow.get("scope"),
        "instrument_identity": {
            "kind": shadow.get("expression"),
            "legs": [l["instrument"] for l in legs],
            "universe": "%s markets present in the adopted owner's own panel "
                        "at the decision date" % shadow.get("scope"),
        },
        "venue": "OWNED_NORGATE_CONSOLIDATED",
        "prediction_type": shadow.get("expression"),
        "direction": ("MARKET_NEUTRAL" if abs(net) < 1e-9 else
                      "LONG_BIASED" if net > 0 else "SHORT_BIASED"),

        "position_expression": {"legs": legs},
        "weights": {m: round(float(w), 8) for m, w in sorted(weights.items())},
        "n_legs": len(legs),
        "n_predictions": len(legs),
        "gross_notional": gross_notional,

        # The strategy's OWN frozen control travels with the row from emission,
        # so maturity can never be tempted to pick a convenient one later.
        "control": control,
        "control_description": CONTROL_DESCRIPTION.get(
            control, "the adopted owner's own declared control"),
        "scientific_control": control,
        "scientific_control_owner": SCIENTIFIC_CONTROL_OWNER.get(control),
        "capital_control": CAPITAL_CONTROL,
        "controls_are_separate": True,
        "formal_verdict_uses": FORMAL_VERDICT_USES,
        "r46_control": C.CONTROL_CASH,

        "cost_model": {
            "base": cost_model.get("base"),
            "state": cost_model.get("state"),
            "source": "the adopted shadow's OWN frozen cost model; R46.6.1 "
                      "charges no rate of its own",
            "bps_per_side_hash": sha(cost_model.get("bps_per_side")),
            "n_markets_priced": len(cost_model.get("bps_per_side") or {}),
        },
        "source_state_hash": source_state_hash,
        "input_freshness_stale_sources": list(stale_sources or ()),

        "evidence_class": C.TRUE_FORWARD,
        "point_in_time_status": C.PIT_OK,
        "status": C.STATUS_PENDING,
        "prior_release_artifact_mutated": False,
        "provenance": {
            "calculation_owner": CALCULATION_OWNER,
            "signal_owner": signal_owner(release),
            "registry_model_string": shadow.get("model"),
            "model_name_passed_to_owner": frozen_model_name(shadow),
            "model_name_was_normalised": model_name_was_normalised(shadow),
            "entry_convention_statement": ENTRY_CONVENTION_STATEMENT,
            "superseded_clause": SUPERSEDED_ADOPTION_CLAUSE["clause"],
            "prior_release_append_authorised": PRIOR_RELEASE_APPEND_AUTHORISED,
            "r46_continuation_append_authorised":
                R46_CONTINUATION_APPEND_AUTHORISED,
            "contract_hash": C.contract_hash(),
        },
        "calculation_owner": CALCULATION_OWNER,
    }


def state_hash(shadow: dict, panel, decision_date) -> str:
    """A deterministic fingerprint of the input state this row was read from."""
    import pandas as pd
    d = pd.Timestamp(decision_date)
    rows_d = panel[pd.to_datetime(panel["decision_date"]) == d]
    return sha({
        "lane": shadow.get("lane"),
        "decision_date": str(_as_date(decision_date)),
        "markets": sorted(str(m) for m in rows_d["market_id"].tolist()),
        "n_markets_at_decision_date": int(len(rows_d)),
        "n_panel_rows": int(len(panel)),
        "panel_latest_decision_date":
            str(pd.to_datetime(panel["decision_date"]).max().date()),
    })


# --------------------------------------------------------------------------- #
# THE lane call
# --------------------------------------------------------------------------- #
def run_lane(*, release: str, shadow_ids: tuple, as_of: _dt.date,
             campaign_id: str = CAMPAIGN_ID,
             information_family: str = None,
             state: dict = None, now: _dt.datetime = None,
             registry: dict = None, build_state: bool = True,
             mature_matured: bool = True) -> dict:
    """Continue ONE adopted lane. Reads prior artifacts; writes only R46's."""
    emitted_at = now or CK.now_utc()
    ident = verify_identity(release, shadow_ids, registry=registry)
    if not ident["ok"]:
        return {"lifecycle": "CALLED_PIT_BLOCKED",
                "owner_state": "SPECIFICATION_IDENTITY_UNPROVEN",
                "continuation_state": CONTINUATION_IDENTITY_BLOCKED,
                "continuation_owner": CONTINUATION_OWNER,
                "prior_release_append_authorised":
                    PRIOR_RELEASE_APPEND_AUTHORISED,
                "r46_continuation_append_authorised":
                    R46_CONTINUATION_APPEND_AUTHORISED,
                "reason": ident.get("reason") or (
                    "the adopted specification could not be proved identical "
                    "to the one that was frozen (%s); R46.6.1 refuses rather "
                    "than guesses" % ident.get("blocker")),
                "identity": ident,
                "shadow_ids": list(shadow_ids),
                "prior_release_artifact_mutated": False}

    reg = registry if registry is not None else load_source_registry(release)
    shadows = [s for s in registry_shadows(release, reg)
               if s.get("shadow_id") in set(shadow_ids)]

    if state is None:
        if not build_state:
            return _blocked(shadow_ids, "NO_STATE_SUPPLIED",
                            "a current panel was not supplied and this call "
                            "was told not to build one")
        try:
            state = build_current_state(release, reg, as_of)
        except Exception as exc:                # noqa: BLE001 - reported
            return _blocked(shadow_ids, "OWNER_RAISED_BUILDING_STATE",
                            "%s: %s" % (type(exc).__name__, str(exc)[:200]))

    stale = _stale_sources(state, as_of)
    captured_by_shadow: dict = {}
    for row in predictions(campaign_id):
        captured_by_shadow.setdefault(
            row.get("adopted_challenger_id"), set()).add(
                str(row.get("decision_date")))

    ident_by_id = {r["shadow_id"]: r for r in ident["rows"]}
    fresh, skipped, per_shadow = [], [], {}
    refusals: list = []
    for sh in shadows:
        sid = sh["shadow_id"]
        panel = panel_for(sh, state)
        if panel is None or not len(panel):
            skipped.append({"shadow_id": sid, "reason": "PANEL_UNAVAILABLE"})
            per_shadow[sid] = {"due": 0, "emitted": 0,
                               "state": "PANEL_UNAVAILABLE"}
            continue
        due = due_decision_dates(sh, panel, captured_by_shadow.get(sid, set()))
        n_emitted, n_window = 0, 0
        for d in due:
            if emitted_at >= outcome_window_start(d):
                n_window += 1
                # Release 46.6.2: a refusal that does not name its own date
                # cannot be adjudicated later. This one names everything.
                ev = dict(refusal_evidence(d, emitted_at), shadow_id=sid)
                refusals.append(ev)
                skipped.append({"shadow_id": sid,
                                "decision_date": str(_as_date(d)),
                                "reason": SKIP_OUTCOME_WINDOW_OPEN,
                                "first_outcome_session":
                                    ev["first_outcome_session"],
                                "outcome_window_start_utc":
                                    ev["outcome_window_start_utc"],
                                "hours_late": ev["hours_late"]})
                continue
            w = signal(release, sh, panel, d)
            if not w:
                skipped.append({"shadow_id": sid,
                                "decision_date": str(_as_date(d)),
                                "reason": SKIP_SIGNAL_UNAVAILABLE})
                continue
            fresh.append(build_record(
                release=release, shadow=sh, identity_row=ident_by_id[sid],
                identity=ident, weights=w, decision_date=d,
                information_family=information_family, emitted_at=emitted_at,
                source_state_hash=state_hash(sh, panel, d),
                stale_sources=stale))
            n_emitted += 1
        feas = emission_feasibility(due=due, panel=panel,
                                    emitted_at=emitted_at,
                                    sessions=owner_session_axis(state, sh))
        per_shadow[sid] = {
            "due": len(due), "emitted": n_emitted,
            "refused_outcome_window_open": n_window,
            "already_in_ledger": len(captured_by_shadow.get(sid, set())),
            # Release 46.6.2 - the owner's OWN answers, never the calendar's.
            "due_decision_dates": [str(_as_date(d)) for d in due][:20],
            "emission_feasibility": feas["state"],
            "emission_feasibility_reason": feas.get("reason"),
            "owner_latest_session": feas.get("owner_latest_session"),
            "owner_latest_decision_date":
                feas.get("owner_latest_decision_date"),
            "n_sessions_after_newest_due_decision_date":
                feas.get("n_sessions_after_newest_due_decision_date"),
        }

    result = append(fresh, campaign_id)
    matured = ({"n_appended": 0, "state": "NOT_RUN"} if not mature_matured
               else mature(campaign_id, state=state, now=emitted_at))

    n_appended = int(result["n_appended"])
    n_window_refused = sum(int(v.get("refused_outcome_window_open") or 0)
                           for v in per_shadow.values())
    n_due = sum(int(v.get("due") or 0) for v in per_shadow.values())
    if n_appended:
        life, owner_state, cstate = ("CALLED_AND_EMITTED",
                                     "CONTINUATION_APPENDED",
                                     CONTINUATION_READY)
    elif result["n_duplicates_skipped"]:
        life, owner_state, cstate = ("CALLED_QUIET_NOT_DUE",
                                     "ALREADY_CAPTURED_IDEMPOTENT",
                                     CONTINUATION_READY)
    elif n_window_refused:
        life, owner_state, cstate = ("CALLED_PIT_BLOCKED",
                                     "OUTCOME_WINDOW_ALREADY_OPEN",
                                     CONTINUATION_PIT_BLOCKED)
    elif any(s.get("reason") == "PANEL_UNAVAILABLE" for s in skipped):
        life, owner_state, cstate = ("CALLED_DATA_BLOCKED", "PANEL_UNAVAILABLE",
                                     CONTINUATION_DATA_BLOCKED)
    elif n_due:
        life, owner_state, cstate = ("CALLED_DATA_BLOCKED",
                                     "OWNER_PRODUCED_NO_SIGNAL",
                                     CONTINUATION_DATA_BLOCKED)
    else:
        life, owner_state, cstate = ("CALLED_QUIET_NOT_DUE",
                                     "NO_ELIGIBLE_DECISION",
                                     CONTINUATION_READY)

    return {
        "lifecycle": life,
        "owner_state": owner_state,
        "continuation_state": cstate,
        "continuation_owner": CONTINUATION_OWNER,
        "continuation_ledger": str(continuation_dir(campaign_id)
                                   / CONTINUATION_LEDGER),
        "prior_release_append_authorised": PRIOR_RELEASE_APPEND_AUTHORISED,
        "r46_continuation_append_authorised":
            R46_CONTINUATION_APPEND_AUTHORISED,
        "prior_release_artifact_mutated": False,
        "n_appended": n_appended,
        "n_duplicates_skipped": result["n_duplicates_skipped"],
        "n_refused_outcome_window_open": n_window_refused,
        "n_due_decision_dates": n_due,
        "n_outcomes_matured": int((matured or {}).get("n_appended") or 0),
        # Release 46.6.2 - WHICH decision date was refused, and how late.
        # Without these the gate can only be audited by rebuilding the panel.
        "refused_decision_dates": refusals[:40],
        "refusal_evidence_fields": list(REFUSAL_EVIDENCE_FIELDS),
        # The OWNER's next decision date, or null. Never the call calendar's -
        # see VX_CADENCE_DISCREPANCY.
        "next_decision_date": None,
        "next_decision_date_source": "ADOPTED_OWNER_PANEL",
        "emission_feasibility": _lane_feasibility(per_shadow),
        "per_shadow": per_shadow,
        "skipped": skipped[:40],
        "input_freshness_stale_sources": stale,
        "identity": {k: ident[k] for k in
                     ("ok", "source_registry_hash",
                      "source_registry_hash_matches",
                      "source_registry_frozen_at")},
        "shadow_ids": list(shadow_ids),
        "owner_is_reachable": True,
    }


def _lane_feasibility(per_shadow: dict) -> str:
    """ONE feasibility verdict for a lane whose shadows may disagree.

    The lane can emit if ANY of its shadows can; it is structurally late only
    when every shadow that had something due was structurally late.
    """
    states = [v.get("emission_feasibility") for v in per_shadow.values()
              if v.get("emission_feasibility")]
    if not states:
        return EMISSION_NOTHING_DUE
    if EMISSION_CAN_EMIT in states:
        return EMISSION_CAN_EMIT
    blocked = [s for s in states if s != EMISSION_NOTHING_DUE]
    if not blocked:
        return EMISSION_NOTHING_DUE
    if all(s == EMISSION_STRUCTURALLY_LATE for s in blocked):
        return EMISSION_STRUCTURALLY_LATE
    return EMISSION_LATE_THIS_RUN


def _blocked(shadow_ids: tuple, owner_state: str, reason: str) -> dict:
    return {"lifecycle": "CALLED_DATA_BLOCKED", "owner_state": owner_state,
            "continuation_state": CONTINUATION_DATA_BLOCKED,
            "continuation_owner": CONTINUATION_OWNER,
            "prior_release_append_authorised": PRIOR_RELEASE_APPEND_AUTHORISED,
            "r46_continuation_append_authorised":
                R46_CONTINUATION_APPEND_AUTHORISED,
            "prior_release_artifact_mutated": False,
            "reason": reason, "shadow_ids": list(shadow_ids)}


#: Rebuilding the adopted panels from the live Norgate entitlement costs about
#: six minutes. On a month-end BOTH futures lanes are due in the same canonical
#: run and would each pay it. The state is a pure function of the provider data
#: for a given session, so one run reuses it - keyed by the session so a
#: long-lived process can never serve yesterday's panel today.
_STATE_CACHE: dict = {}


def build_current_state(release: str, registry: dict,
                        as_of: _dt.date = None, *,
                        use_cache: bool = True) -> dict:
    """A CURRENT panel from the ADOPTED release's own state builder."""
    key = (release, str(as_of) if as_of else None)
    if use_cache and key[1] and key in _STATE_CACHE:
        return _STATE_CACHE[key]
    if release == "R40":
        st = _r40_cycle().build_fresh_state(registry or {})
    else:
        st = _r39_owner().build_fresh_state()
    if use_cache and key[1]:
        # one session's panels at a time; yesterday's are dropped, never served
        for k in [k for k in _STATE_CACHE if k[1] != key[1]]:
            _STATE_CACHE.pop(k, None)
        _STATE_CACHE[key] = st
    return st


def _stale_sources(state: dict, as_of) -> list:
    try:
        import pandas as pd
        return list(_r40_cycle().input_freshness(
            state, pd.Timestamp(str(as_of))).get("stale_sources") or ())
    except Exception:                           # noqa: BLE001 - descriptive
        return []


# --------------------------------------------------------------------------- #
# Maturity - the ONE adapter, so the R46 machinery can judge these rows
# --------------------------------------------------------------------------- #
#: Why an adapter exists at all: the R46 judge prices a prediction leg by leg
#: from :mod:`alpha_agent.r46.marketdata`, and an adopted shadow's legs are
#: Norgate CONTINUOUS FUTURES market ids that live in the adopted owner's own
#: panel, not in that seam. The adapter resolves the realised forward from the
#: panel the owner already computes, charges the shadow's OWN frozen cost
#: model, and credits the SAME canonical R46 control every other outcome row is
#: scored against. It invents no rate, no benchmark and no horizon.
MATURITY_ADAPTER_REASON = (
    "adopted shadows trade Norgate continuous-futures market ids that the R46 "
    "market-data seam does not price; their realised forward lives in their "
    "own panel. One adapter resolves it, charges their own frozen cost model, "
    "and scores it TWICE: against the control the strategy FROZE, computed by "
    "the prior release's own implementation, which is the number a formal "
    "scientific verdict may use; and against the canonical R46 cash control, "
    "which answers the separate question of whether research capital was "
    "better deployed here than in cash.")

DEFAULT_COST_BPS_PER_SIDE = 10.0        # the adopted owners' own fallback


def _r39_trade_space():
    return _owner_module("r39.trade_space")


def declared_control_path(shadow: dict, panel, decision_dates: list,
                          fwd_col: str, *, cost_multiplier: float = 1.0
                          ) -> dict:
    """The FROZEN scientific control, computed by the ORIGINAL owner's logic.

    Never invents a control, never searches a parameter, never substitutes
    cash for a non-cash declaration. Where the original control cannot be
    reconstructed from the legitimate PIT-safe panel the state is BLOCKED and
    the return is ``None`` - the one thing this function may not do is guess.
    """
    import pandas as pd

    name = shadow.get("control")
    base = {"control": name,
            "owner": SCIENTIFIC_CONTROL_OWNER.get(name),
            "definition": SCIENTIFIC_CONTROL_DEFINITION.get(name),
            "cost_multiplier": float(cost_multiplier),
            "by_date": {}}
    dates = [str(_as_date(d)) for d in decision_dates]

    if name == CONTROL_RISK_MATCHED_CASH:
        # R39's own implementation: control_net = np.zeros(...). The book is
        # self-financed and its forward returns are already excess of
        # financing, so the control line is zero in the strategy's own units.
        # Cost multipliers do not move a zero line.
        return dict(base, state=SCIENTIFIC_CONTROL_OK,
                    by_date={d: 0.0 for d in dates},
                    is_zero_excess_line=True)

    if name != CONTROL_VOL_MATCHED_PASSIVE:
        return dict(base, state=SCIENTIFIC_CONTROL_BLOCKED_UNKNOWN)

    if panel is None or not len(panel) or fwd_col not in panel.columns:
        return dict(base, state=SCIENTIFIC_CONTROL_BLOCKED_NO_SCOPE)

    want = {pd.Timestamp(d) for d in dates}
    rows = panel[pd.to_datetime(panel["decision_date"]).isin(want)]
    if rows.empty:
        return dict(base, state=SCIENTIFIC_CONTROL_BLOCKED_NO_SCOPE)

    # The ORIGINAL function, on the ORIGINAL scope, over the SAME dates, at the
    # shadow's OWN frozen per-market cost. ``passive_ew_control`` walks the
    # whole date path so its turnover is the original's turnover; the book's
    # own cost in ``mature`` starts from a flat position for the same reason.
    try:
        TS = _r39_trade_space()
        fwd_m = rows.pivot_table(index="decision_date", columns="market_id",
                                 values=fwd_col, aggfunc="last")
        bps = ((shadow.get("cost_model") or {}).get("bps_per_side")) or {}
        cost = pd.Series({str(k): float(v) for k, v in bps.items()},
                         dtype=float)
        ctrl = TS.passive_ew_control(fwd_m, cost,
                                     cost_multiplier=float(cost_multiplier))
    except Exception as exc:                        # noqa: BLE001 - named
        return dict(base, state=SCIENTIFIC_CONTROL_BLOCKED_OWNER,
                    blocker_detail=str(exc)[:200])

    by = {}
    for dt, gross, net in zip(ctrl["dates"], ctrl["gross"], ctrl["net"]):
        if not _finite(net):
            continue
        by[str(pd.Timestamp(dt).date())] = float(net)
    if not by:
        return dict(base, state=SCIENTIFIC_CONTROL_BLOCKED_NOT_PIT)
    return dict(base, state=SCIENTIFIC_CONTROL_OK, by_date=by,
                n_markets_in_scope=int(fwd_m.shape[1]),
                control_expression=ctrl.get("expression"))


def _finite(x) -> bool:
    try:
        return float(x) == float(x)                 # NaN is never equal
    except (TypeError, ValueError):
        return False


def mature(campaign_id: str = CAMPAIGN_ID, *, state: dict,
           now: _dt.datetime = None, registries: dict = None) -> dict:
    """Score every continuation row whose horizon has genuinely matured."""
    import numpy as np
    import pandas as pd
    from . import marketdata as MD

    scored_at = now or CK.now_utc()
    rows = predictions(campaign_id)
    done = {str(o.get("continuation_id")) for o in outcomes(campaign_id)}
    regs = dict(registries or {})
    appended, pending = [], []
    prev_w: dict = {}
    # Every continuation decision date this shadow has on the record, in order:
    # the frozen control is a PATH, and its turnover is the original's turnover.
    dates_by_sid: dict = {}
    for r0 in rows:
        dates_by_sid.setdefault(str(r0.get("adopted_challenger_id")),
                                []).append(str(r0.get("decision_date")))
    for sid0 in dates_by_sid:
        dates_by_sid[sid0] = sorted(set(dates_by_sid[sid0]))
    ctrl_cache: dict = {}
    for row in sorted(rows, key=lambda r: (str(r.get("adopted_challenger_id")),
                                           str(r.get("decision_date")))):
        cid = str(row.get("continuation_id"))
        sid = row.get("adopted_challenger_id")
        rel = row.get("adopted_from_release")
        if rel not in regs:
            regs[rel] = load_source_registry(rel) or {}
        sh = next((s for s in registry_shadows(rel, regs[rel])
                   if s.get("shadow_id") == sid), None)
        w = pd.Series(row.get("weights") or {}, dtype=float)
        if sh is None or w.empty:
            pending.append({"continuation_id": cid,
                            "reason": "SHADOW_ABSENT_FROM_SOURCE_REGISTRY"})
            continue
        panel = panel_for(sh, state)
        horizon = int(row.get("horizon") or 0)
        fwd_col = "fwd_%d" % horizon
        if panel is None or not len(panel) or fwd_col not in panel.columns:
            pending.append({"continuation_id": cid,
                            "reason": "PANEL_OR_FORWARD_COLUMN_UNAVAILABLE"})
            continue
        rows_d = panel[pd.to_datetime(panel["decision_date"])
                       == pd.Timestamp(row["decision_date"])]
        if rows_d.empty:
            pending.append({"continuation_id": cid,
                            "reason": "DECISION_DATE_ABSENT_FROM_PANEL"})
            continue
        fwd_all = rows_d.set_index("market_id")[fwd_col]
        r = fwd_all.reindex(w.index)
        if cid in done:
            prev_w[sid] = row.get("weights") or {}
            continue
        if r.isna().all():
            pending.append({"continuation_id": cid, "reason": "NOT_MATURED"})
            # The book took this position on its decision date whether or not
            # the horizon has run, so the NEXT decision's turnover is measured
            # against it. Rows are walked in decision-date order for exactly
            # this reason.
            prev_w[sid] = row.get("weights") or {}
            continue

        gross = float((w * r.fillna(0.0)).sum())
        pw = pd.Series(prev_w.get(sid, {}), dtype=float)
        union = w.index.union(pw.index)
        dw = (w.reindex(union, fill_value=0.0)
              - pw.reindex(union, fill_value=0.0)).abs()
        bps = ((sh.get("cost_model") or {}).get("bps_per_side")) or {}
        rate = pd.Series({m: float(bps.get(m, DEFAULT_COST_BPS_PER_SIDE)) / 1e4
                          for m in dw.index})
        cost = float((dw * rate).sum())
        net = gross - cost
        net_2x = gross - 2.0 * cost

        # ---- A. the FROZEN SCIENTIFIC control ------------------------------ #
        # The control the strategy was frozen against, computed by the prior
        # release's own implementation. This is the number a formal scientific
        # verdict may use, and the only one.
        declared = sh.get("control")
        if sid not in ctrl_cache:
            ctrl_cache[sid] = {
                mult: declared_control_path(sh, panel, dates_by_sid.get(sid, []),
                                            fwd_col, cost_multiplier=mult)
                for mult in (1.0, 2.0)}
        c1, c2 = ctrl_cache[sid][1.0], ctrl_cache[sid][2.0]
        dkey = str(row["decision_date"])
        declared_return = c1["by_date"].get(dkey)
        declared_return_2x = c2["by_date"].get(dkey)
        if declared != row.get("control"):
            declared_state = SCIENTIFIC_CONTROL_BLOCKED_DRIFT
            declared_return = declared_return_2x = None
        elif c1["state"] != SCIENTIFIC_CONTROL_OK:
            declared_state = c1["state"]
            declared_return = declared_return_2x = None
        elif declared_return is None:
            declared_state = SCIENTIFIC_CONTROL_BLOCKED_NO_DATE
            declared_return_2x = None
        else:
            declared_state = SCIENTIFIC_CONTROL_OK
        sci_alpha = (None if declared_return is None
                     else net - declared_return)
        sci_alpha_2x = (None if declared_return_2x is None
                        else net_2x - declared_return_2x)

        # ---- B. the CAPITAL opportunity cost ------------------------------- #
        # A different question, kept in different fields: what the same capital
        # would have earned in cash. Never a substitute for the control above.
        rf = MD.risk_free_per_session(horizon)
        rf_state = MD.risk_free_annual().get("state")
        cap_alpha = None if rf is None else net - rf
        cap_alpha_2x = None if rf is None else net_2x - rf

        m = r.notna()
        sign_acc = (float((np.sign(w[m]) == np.sign(r[m])).mean())
                    if int(m.sum()) else None)
        ic = (float(w[m].rank().corr(r[m].rank()))
              if int(m.sum()) >= 5 else None)
        maturity = _maturity_date(panel, row["decision_date"], horizon)
        gross_notional = float(w.abs().sum())

        appended.append({
            "kind": "ADOPTED_CONTINUATION_OUTCOME",
            "continuation_id": cid,
            # keyed as the canonical R46 outcome rows are, so a downstream
            # reader joining on prediction_id needs no special case
            "prediction_id": cid,
            "challenger_id": sid,
            "adopted_challenger_id": sid,
            "adopted_from_release": rel,
            "challenger_spec_hash": row.get("spec_identity"),
            "challenger_version": "adopted",
            "asset_class": row.get("asset_class"),
            "horizon": horizon,
            "effective_as_of": row.get("decision_date"),
            "maturity_date": maturity,
            "maturity_dates": [maturity] if maturity else [],
            "entry_dates": [row.get("decision_date")],

            "realised_gross_return": gross,
            "realised_cost": cost,
            "realised_net_return": net,
            "realised_net_return_at_2x_costs": net_2x,
            # the benchmark is the strategy's OWN frozen control, not an
            # average this release invented
            "realised_benchmark_return": declared_return,
            "realised_residual_return": sci_alpha,
            "realised_residual_return_means":
                "AFTER_COST_EXCESS_VS_THE_FROZEN_SCIENTIFIC_CONTROL",

            # ---- A. the frozen SCIENTIFIC control -------------------------- #
            "scientific_control": declared,
            "scientific_control_owner": c1.get("owner"),
            "scientific_control_definition": c1.get("definition"),
            "scientific_control_description":
                CONTROL_DESCRIPTION.get(declared),
            "scientific_control_return": declared_return,
            "scientific_control_return_at_2x_costs": declared_return_2x,
            "scientific_control_state": declared_state,
            "scientific_control_computed_by_the_original_owner":
                declared_state == SCIENTIFIC_CONTROL_OK,
            "scientific_alpha_vs_declared_control": sci_alpha,
            "scientific_alpha_vs_declared_control_at_2x_costs": sci_alpha_2x,
            "scientific_question": SCIENTIFIC_QUESTION,
            # back-compatible names for the same three numbers
            "declared_control": declared,
            "declared_control_return": declared_return,
            "declared_control_state": declared_state,

            # ---- B. the CAPITAL opportunity cost --------------------------- #
            "capital_control": CAPITAL_CONTROL,
            "capital_control_description": CAPITAL_CONTROL_DESCRIPTION,
            "capital_control_return": rf,
            "capital_alpha_vs_cash": cap_alpha,
            "capital_alpha_vs_cash_at_2x_costs": cap_alpha_2x,
            "capital_question": CAPITAL_QUESTION,
            "risk_free_state": rf_state,

            # ---- the two, kept apart --------------------------------------- #
            "controls_are_separate": True,
            "formal_verdict_uses": FORMAL_VERDICT_USES,
            "cash_substitution_for_noncash_control_allowed":
                CASH_SUBSTITUTION_FOR_NONCASH_CONTROL_ALLOWED,
            "capital_alpha_is_not_a_verdict_input":
                CAPITAL_ALPHA_IS_NOT_A_VERDICT_INPUT,

            # The generic R46 names remain so the P&L, cost-efficiency and
            # evidence owners keep reading a number that means what they think
            # it means - CAPITAL alpha - and their semantics are stated here
            # rather than inferred. They cannot reach a formal verdict: the
            # verdict owner refuses one while the scientific control is blocked.
            "control": CAPITAL_CONTROL,
            "control_description": CAPITAL_CONTROL_DESCRIPTION,
            "control_return": rf,
            "net_alpha_vs_control": cap_alpha,
            "net_alpha_vs_control_at_2x_costs": cap_alpha_2x,
            "net_alpha_vs_control_means": "CAPITAL_ALPHA_VS_CASH",

            "gross_notional": gross_notional,
            "turnover": float(dw.sum()),
            "n_legs": int(len(w)),
            "n_matured_markets": int(m.sum()),
            "rank_ic": ic,
            "sign_accuracy": sign_acc,
            # a "hit" is a scientific claim, so it is measured against the
            # frozen control; where that control is blocked there is no hit to
            # report, and the capital comparison travels under its own name
            "hit": (None if sci_alpha is None else bool(sci_alpha > 0)),
            "hit_measured_against": FORMAL_VERDICT_USES,
            "capital_hit": (None if cap_alpha is None
                            else bool(cap_alpha > 0)),
            "cost_model_source": "the adopted shadow's OWN frozen cost model",
            "forward_evidence_type": C.TRUE_FORWARD,
            "status": C.STATUS_SCORED,
            "scored_at_utc": CK.iso(scored_at),
            "prior_release_artifact_mutated": False,
            "calculation_owner": CALCULATION_OWNER,
            "adapter_reason": MATURITY_ADAPTER_REASON,
        })
        prev_w[sid] = row.get("weights") or {}

    res = append_outcomes(appended, campaign_id) if appended else {
        "n_offered": 0, "n_appended": 0, "n_duplicates_skipped": 0,
        "duplicates": [], "appended": [], "idempotent": True}
    return {"n_appended": res["n_appended"],
            "n_duplicates_skipped": res["n_duplicates_skipped"],
            "n_pending": len(pending), "pending": pending[:40],
            "never_revises_an_emitted_row": True,
            "prior_release_ledger_written": False}


def verdict_inputs(campaign_id: str = CAMPAIGN_ID) -> dict:
    """Per adopted challenger: the matured record measured against its OWN
    FROZEN control, and the formal verdict the canonical verdict owner reaches
    on it.

    The capital comparison travels alongside and is never handed to the verdict
    owner. Where the frozen control could not be computed the state is passed
    through as the gate, and no formal verdict is available - a strategy that
    beat cash while its own benchmark was unmeasurable has proved nothing.
    """
    from . import verdicts as VD

    outs = outcomes(campaign_id)
    by: dict = {}
    for o in outs:
        by.setdefault(str(o.get("adopted_challenger_id")), []).append(o)
    rows = []
    for sid in sorted(by):
        rec = sorted(by[sid], key=lambda o: str(o.get("effective_as_of")))
        states = {str(o.get("scientific_control_state")) for o in rec}
        blocked = sorted(s for s in states if s != SCIENTIFIC_CONTROL_OK)
        state = blocked[0] if blocked else SCIENTIFIC_CONTROL_OK
        sci = [o.get(SCIENTIFIC_ALPHA_FIELD) for o in rec]
        sci = [float(x) for x in sci if x is not None]
        sci2 = [o.get("scientific_alpha_vs_declared_control_at_2x_costs")
                for o in rec]
        sci2 = [float(x) for x in sci2 if x is not None]
        cap = [o.get(CAPITAL_ALPHA_FIELD) for o in rec]
        cap = [float(x) for x in cap if x is not None]
        n = len(sci)
        cum = float(sum(sci))
        t = None
        if n >= 2:
            mu = cum / n
            var = sum((x - mu) ** 2 for x in sci) / (n - 1)
            sd = var ** 0.5
            t = (mu / (sd / (n ** 0.5))) if sd > 0 else None
        peak = run = dd = 0.0
        for x in sci:
            run += x
            peak = max(peak, run)
            dd = min(dd, run - peak)
        v = VD.verdict_for(
            n_closed=n, residual=cum, t_residual=t,
            net_at_2x=float(sum(sci2)), max_drawdown=dd,
            hit_rate=(float(sum(1 for x in sci if x > 0)) / n) if n else None,
            reconciliation_mismatches=0, marginal_diversification=None,
            tournament_states=set(), economic_state=None,
            scientific_control_state=state)
        rows.append({
            "adopted_challenger_id": sid,
            "adopted_from_release": rec[0].get("adopted_from_release"),
            "scientific_control": rec[0].get("scientific_control"),
            "scientific_control_state": state,
            "scientific_control_owner": rec[0].get("scientific_control_owner"),
            "n_matured": len(rec),
            "n_scored_against_the_frozen_control": n,
            "cum_scientific_alpha": cum,
            "cum_scientific_alpha_at_2x_costs": float(sum(sci2)),
            "t_scientific_alpha": t,
            "max_drawdown_scientific_alpha": dd,
            "cum_capital_alpha_vs_cash": float(sum(cap)) if cap else None,
            "formal_verdict": v["verdict"],
            "formal_verdict_blocked": bool(v.get("formal_verdict_blocked")),
            "formal_verdict_reasons": v["reasons"],
            "formal_verdict_metric": FORMAL_VERDICT_USES,
            "capital_alpha_was_not_an_input": True,
        })
    return {
        "schema": "r46_6_1_adopted_verdict_inputs/1",
        "calculation_owner": CALCULATION_OWNER,
        "verdict_owner": VD.CALCULATION_OWNER,
        "gate": VD.SCIENTIFIC_CONTROL_GATE,
        "formal_verdict_uses": FORMAL_VERDICT_USES,
        "capital_alpha_field": CAPITAL_ALPHA_FIELD,
        "cash_substitution_for_noncash_control_allowed":
            CASH_SUBSTITUTION_FOR_NONCASH_CONTROL_ALLOWED,
        "n_adopted_with_outcomes": len(rows),
        "rows": rows,
    }


def _maturity_date(panel, decision_date, horizon: int):
    import pandas as pd
    dates = sorted(pd.to_datetime(panel["decision_date"]).unique())
    sessions = [pd.Timestamp(d).date() for d in dates]
    s = CK.maturity_session(sessions, _as_date(decision_date), horizon)
    return str(s) if s else None


# --------------------------------------------------------------------------- #
# The artifact
# --------------------------------------------------------------------------- #
def summary(campaign_id: str = CAMPAIGN_ID) -> dict:
    preds = predictions(campaign_id)
    outs = outcomes(campaign_id)
    scored = {str(o.get("continuation_id")) for o in outs}
    by: dict = {}
    for p in preds:
        e = by.setdefault(p.get("adopted_challenger_id"),
                          {"emitted": 0, "scored": 0})
        e["emitted"] += 1
        if str(p.get("continuation_id")) in scored:
            e["scored"] += 1
    return {
        "schema": "r46_6_1_adopted_continuation_summary/1",
        "calculation_owner": CALCULATION_OWNER,
        "continuation_dir": str(continuation_dir(campaign_id)),
        "prediction_ledger": CONTINUATION_LEDGER,
        "outcome_ledger": CONTINUATION_OUTCOME_LEDGER,
        "n_continuation_predictions": len(preds),
        "n_continuation_outcomes": len(outs),
        "n_pending": len(preds) - len(scored),
        "by_adopted_challenger": by,
        "chain": verify(campaign_id),
        "true_forward_only": True,
        "prior_release_ledgers_are_never_written": True,
        "scientific_control_field": SCIENTIFIC_ALPHA_FIELD,
        "capital_control_field": CAPITAL_ALPHA_FIELD,
        "controls_are_separate": True,
        "formal_verdict_uses": FORMAL_VERDICT_USES,
    }


def contract_body() -> dict:
    """The continuation contract, as a declaration a reader can check."""
    return {
        "schema": "r46_6_1_adopted_continuation_contract/1",
        "calculation_owner": CALCULATION_OWNER,
        "continuation_owner": CONTINUATION_OWNER,
        "prior_release_append_authorised": PRIOR_RELEASE_APPEND_AUTHORISED,
        "r46_continuation_append_authorised":
            R46_CONTINUATION_APPEND_AUTHORISED,
        "superseded_adoption_clause": SUPERSEDED_ADOPTION_CLAUSE,
        "safety_block": dict(C.SAFETY_BLOCK),
        "record_fields": list(CONTINUATION_RECORD_FIELDS),
        "identity_key": list(CONTINUATION_IDENTITY_KEY),
        "outcome_identity_key": list(OUTCOME_IDENTITY_KEY),
        "identity_fields": list(IDENTITY_FIELDS) + list(IDENTITY_EXTRA),
        "frozen_strategy_identity": dict(FROZEN_STRATEGY_IDENTITY),
        "frozen_registry_hash": dict(FROZEN_REGISTRY_HASH),
        "frozen_registry_frozen_at": dict(FROZEN_REGISTRY_FROZEN_AT),
        "entry_convention": ENTRY_CONVENTION,
        "entry_convention_statement": ENTRY_CONVENTION_STATEMENT,
        "evidence_class": C.TRUE_FORWARD,
        "continuation_states": list(CONTINUATION_STATES),
        "maturity_adapter_reason": MATURITY_ADAPTER_REASON,
        "controls": {
            "there_are_two_and_they_answer_different_questions": True,
            "scientific": {
                "question": SCIENTIFIC_QUESTION,
                "control": "the control the adopted strategy FROZE",
                "owner": dict(SCIENTIFIC_CONTROL_OWNER),
                "definition": dict(SCIENTIFIC_CONTROL_DEFINITION),
                "field": SCIENTIFIC_ALPHA_FIELD,
                "at_2x_costs":
                    "scientific_alpha_vs_declared_control_at_2x_costs",
                "state_field": "scientific_control_state",
                "blocked_states": [SCIENTIFIC_CONTROL_BLOCKED_UNKNOWN,
                                   SCIENTIFIC_CONTROL_BLOCKED_NO_SCOPE,
                                   SCIENTIFIC_CONTROL_BLOCKED_NO_DATE,
                                   SCIENTIFIC_CONTROL_BLOCKED_NOT_PIT,
                                   SCIENTIFIC_CONTROL_BLOCKED_DRIFT,
                                   SCIENTIFIC_CONTROL_BLOCKED_OWNER],
                "r46_6_1_defines_no_control_of_its_own": True,
            },
            "capital": {
                "question": CAPITAL_QUESTION,
                "control": CAPITAL_CONTROL,
                "description": CAPITAL_CONTROL_DESCRIPTION,
                "field": CAPITAL_ALPHA_FIELD,
                "at_2x_costs": "capital_alpha_vs_cash_at_2x_costs",
                "may_not_masquerade_as_the_scientific_control": True,
            },
            "formal_verdict_uses": FORMAL_VERDICT_USES,
            "cash_substitution_for_noncash_control_allowed":
                CASH_SUBSTITUTION_FOR_NONCASH_CONTROL_ALLOWED,
            "gate": CAPITAL_ALPHA_IS_NOT_A_VERDICT_INPUT,
            "vx_control_discrepancy_inside_r39": dict(VX_CONTROL_DISCREPANCY),
        },
        "signal_owners": {"R39": signal_owner("R39"),
                          "R40": signal_owner("R40")},
        "model_display_suffix_stripped": MODEL_DISPLAY_SUFFIX,
        "why_the_display_suffix_is_stripped": (
            "R40's freeze wrote a display string into its registry rows' model "
            "field while its own scorer reads everything after the colon as a "
            "column name, so the R40 slot-4 shadow could never have scored "
            "through its own owner. The frozen spec_hash was computed over "
            "the candidate's model name, which carries no suffix; the owner is "
            "given that name and every emitted row records both."),
        "no_second_capture_implementation": True,
        "ledger_primitives": "api.paper_trading_desk chain-hash ledgers "
                             "(canonical), R46 research root",
        # ---- Release 46.6.2 --------------------------------------------- #
        "refusal_evidence_fields": list(REFUSAL_EVIDENCE_FIELDS),
        "emission_feasibility_vocabulary": list(EMISSION_FEASIBILITY),
        "structurally_late_test": STRUCTURALLY_LATE_TEST,
        "vx_cadence_discrepancy": dict(VX_CADENCE_DISCREPANCY),
        "the_gate_was_not_changed_by_r46_6_2": True,
        "why_the_gate_was_not_changed": (
            "R46.6.2 reconstructed the 2026-08-28 refusal from the owner's own "
            "panel and found it CORRECT. The refused decision date was "
            "2026-08-25, not 2026-08-28: the frozen VX grid walks every 5th "
            "session and stops one session short of its own panel end, so its "
            "newest decision date is never today's. That decision's outcome "
            "window opened at 2026-08-26T04:00:00Z and the cycle attempted to "
            "emit at 2026-08-28T23:18:37Z - 67.3 hours late. The observation "
            "is irrecoverable TRUE_FORWARD evidence and is NOT backfilled. "
            "What R46.6.2 changed is that the refusal now names its own date, "
            "and that a refusal no run could have avoided is reported as "
            "STRUCTURALLY_LATE rather than as a transient block."),
    }


def _all_refusals(lane_results: dict) -> dict:
    """Every refusal this run made, keyed by lane, each naming its own date."""
    out = {}
    for lane_id, res in (lane_results or {}).items():
        rows = (res or {}).get("refused_decision_dates") or []
        if rows:
            out[lane_id] = rows
    return out


def build(as_of: _dt.date, campaign_id: str = CAMPAIGN_ID,
          lane_results: dict = None, write: bool = True) -> dict:
    """The R46.6.1 continuation artifact. Pure read model over the ledgers."""
    body = artifact_body(
        "r46_6_1_adopted_continuation/1", CALCULATION_OWNER,
        as_of=str(as_of),
        built_at_utc=CK.iso(CK.now_utc()),
        statement="adopted shadows are CALLED by the canonical cycle and now "
                  "have somewhere to speak: ONE R46-owned append-only "
                  "continuation ledger. No prior release's artifact is ever "
                  "written, and the frozen safety flag that says so is "
                  "untouched.",
        contract=contract_body(),
        summary=summary(campaign_id),
        verdict_inputs=verdict_inputs(campaign_id),
        lane_results=lane_results or {},
        # Release 46.6.2 - the refusals this run made, each naming its own
        # decision date. An artifact that says "1 refused" and not WHICH is
        # not evidence, and R46.6.1's did exactly that.
        refused_decision_dates=_all_refusals(lane_results),
        emission_feasibility={
            k: (v or {}).get("emission_feasibility")
            for k, v in (lane_results or {}).items()},
        prior_release_artifacts_mutated=0,
        research_only=True,
    )
    if write:
        write_json(campaign_dir(campaign_id) / ARTIFACT, body)
    return body


__all__ = [
    "CALCULATION_OWNER", "CONTINUATION_OWNER", "ARTIFACT",
    "CONTINUATION_LEDGER", "CONTINUATION_OUTCOME_LEDGER",
    "CONTINUATION_IDENTITY_KEY", "CONTINUATION_RECORD_FIELDS",
    "CONTINUATION_STATES", "CONTINUATION_READY", "CONTINUATION_PIT_BLOCKED",
    "CONTINUATION_DATA_BLOCKED", "CONTINUATION_IDENTITY_BLOCKED",
    "PRIOR_RELEASE_APPEND_AUTHORISED", "R46_CONTINUATION_APPEND_AUTHORISED",
    "SUPERSEDED_ADOPTION_CLAUSE", "FROZEN_STRATEGY_IDENTITY",
    "FROZEN_REGISTRY_HASH", "ContinuationRefusal",
    "continuation_dir", "predictions", "outcomes", "verify",
    "continuation_key", "continuation_id", "validate", "append",
    "append_outcomes", "source", "load_source_registry", "registry_shadows",
    "strategy_identity", "strategy_identity_hash", "coefficient_evidence",
    "verify_identity", "panel_for", "signal", "signal_owner",
    "frozen_model_name", "model_name_was_normalised", "MODEL_DISPLAY_SUFFIX",
    "due_decision_dates", "outcome_window_start", "build_record",
    "state_hash", "run_lane", "build_current_state", "mature", "summary",
    "contract_body", "build",
    "CONTROL_RISK_MATCHED_CASH", "CONTROL_VOL_MATCHED_PASSIVE",
    "SCIENTIFIC_CONTROL_OWNER", "SCIENTIFIC_CONTROL_DEFINITION",
    "SCIENTIFIC_CONTROL_OK", "SCIENTIFIC_ALPHA_FIELD", "CAPITAL_ALPHA_FIELD",
    "CAPITAL_CONTROL", "FORMAL_VERDICT_USES",
    "CASH_SUBSTITUTION_FOR_NONCASH_CONTROL_ALLOWED", "VX_CONTROL_DISCREPANCY",
    "declared_control_path", "verdict_inputs",
    # Release 46.6.2
    "REFUSAL_EVIDENCE_FIELDS", "EMISSION_FEASIBILITY", "EMISSION_CAN_EMIT",
    "EMISSION_STRUCTURALLY_LATE", "EMISSION_LATE_THIS_RUN",
    "EMISSION_NOTHING_DUE", "STRUCTURALLY_LATE_TEST",
    "VX_CADENCE_DISCREPANCY", "refusal_evidence", "panel_sessions",
    "owner_session_axis", "emission_feasibility",
]
