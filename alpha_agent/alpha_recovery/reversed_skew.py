r"""alpha_agent.alpha_recovery.reversed_skew - ONE frozen challenger, and the
one independent historical window that is allowed to speak about it.

WHAT HAPPENED, IN THE ORDER IT HAPPENED
    1. ``options_surface`` pre-registered a sign for ``PUT_CALL_SKEW`` FROM
       THEORY, before any arm ran: ``+1``, on the reasoning that a steep
       put-over-call skew is crash fear and fear has historically been paid.
    2. The arm at h = 5 came back strongly significant in the OPPOSITE
       direction. ``options_surface.contradicted_signs`` computed exactly what
       the other sign would have been worth - about +38 %/yr at t 3.27, passing
       every gate - reported it in full, and REFUSED to adopt it, because a
       direction chosen by the same sample that scores it is not evidence.
    3. This module is step three, and it is the only honest one available: take
       the direction the data suggested, FREEZE it as a hypothesis, and submit
       it to evidence that did not choose it.

    There are exactly two such sources of evidence, and this module runs both:

        TRUE_FORWARD                 data that does not yet exist
        INDEPENDENT CONFIRMATION     data that existed but was never looked at

THE DISCOVERY SAMPLE IS DISCOVERY ONLY, PERMANENTLY
    ``2024-09-10 -> 2026-08-20`` is the entire moneyness-anchored surface, and
    the sign was read off it. For THIS challenger that sample can never be
    qualification evidence again - not its holdout, not its halves, not its
    equal-risk increment. The holdout stopped being a holdout the instant the
    sign became a choice, and re-quoting those numbers as if they tested the
    frozen rule would be the original error with an extra step. They are carried
    here only so the size of what is being tested is on the record.

WHAT IS FROZEN, AND WHAT "FROZEN" FORBIDS
    One specification. One sign. One horizon. Every other knob is inherited from
    ``options_surface`` by IMPORT rather than by copy, so there is no second
    definition of the z-score, the lookback, the snapshot, the moneyness, the
    expiry rule, the cost ladder or the gates that could drift away from the
    original. There is deliberately no grid, no sweep, no rescue and no
    alternative threshold in this module, and none may be added: a confirmation
    that can be re-run with a different parameter is not a confirmation.

RESEARCH ONLY. Registration starts a MEASUREMENT. It promotes no model,
allocates no capital, changes no holding, and creates no order.
"""
from __future__ import annotations

import json

from alpha_agent.r63 import sensitivity as S
from alpha_agent.r64 import family as FAM

from . import (BH_Q, CAMPAIGN_ID, HOLM_ALPHA, INCUMBENT_MODEL_ID, MIN_EFFECTIVE_PERIODS,
               contract_hash, protocol_hash, research_root, stable_hash, write_artifact)
from . import forward_package as FP
from . import options_acquisition as OA
from . import options_surface as OS
from . import tournament as T

CALCULATION_OWNER = "alpha_agent.alpha_recovery.reversed_skew"
ARTIFACT_NAME = "reversed_skew_challenger.json"

CHALLENGER_ID = "REVERSED_SPY_PUT_CALL_SKEW_H5"
RELEASE = "ALPHA_RECOVERY_OFFENSIVE"
ASSET_CLASS = "US_ETF"
VENUE = "ARCX"
MARK_OWNER = "api.price_panel"          # BENCHMARK_TICKER is SPY; verified, not assumed

#: The ONE signal this challenger reverses, read from the original declaration
#: rather than restated, so the two can never disagree.
SOURCE_SIGNAL = "PUT_CALL_SKEW"
SOURCE_SIGN = OS.SIGNALS[SOURCE_SIGNAL]["sign"]
FROZEN_SIGN = -SOURCE_SIGN
FROZEN_HORIZON = 5

#: The sample the sign was read off. Never qualification evidence for this rule.
DISCOVERY_SAMPLE = OA.DISCOVERY_WINDOW
#: The untouched window that precedes it. Proven disjoint in ``disjointness()``.
CONFIRMATION_WINDOW = ("2022-09-09", "2024-09-09")

INCEPTION = "2026-09-11"

#: Exactly what "the sign was discovered post hoc" costs this challenger.
DISCOVERY_DISCLOSURE = (
    "THE SIGN WAS DISCOVERED POST-HOC ON THE %s -> %s SAMPLE. That entire sample is "
    "DISCOVERY ONLY for this challenger and can never be used as qualification evidence "
    "for the newly frozen reversed rule - including its holdout, its holdout halves and "
    "its equal-risk increment, all of which were computed after the sign was visible."
    % DISCOVERY_SAMPLE)


def spec() -> dict:
    """The frozen rule, as the specification dict ``options_surface`` scores.

    Every field except ``sign`` is the original ``PUT_CALL_SKEW`` h = 5 entry of
    ``options_surface.default_grid()``, taken FROM that grid rather than retyped.
    """
    src = [g for g in OS.default_grid()
           if g["name"] == SOURCE_SIGNAL and g["horizon"] == FROZEN_HORIZON]
    if len(src) != 1:                                        # pragma: no cover - defensive
        raise RuntimeError("expected exactly one %s h%d arm, found %d"
                           % (SOURCE_SIGNAL, FROZEN_HORIZON, len(src)))
    out = dict(src[0])
    out["sign"] = FROZEN_SIGN
    out["name"] = "REVERSED_%s" % SOURCE_SIGNAL
    out["cell_id"] = "OPTIONS|SPY|REVERSED_%s|h%d" % (SOURCE_SIGNAL, FROZEN_HORIZON)
    out["tag"] = "FROZEN_CHALLENGER"
    out["economics"] = (
        "the direction the data preferred, frozen as a hypothesis rather than adopted as a "
        "result. The pre-registered theoretical sign (%+d: crash fear is paid) was "
        "contradicted at h=1 and h=5; this challenger declares the reverse (%+d: a steep "
        "put-over-call skew precedes LOWER subsequent SPY returns) and submits it to "
        "evidence that did not choose it." % (SOURCE_SIGN, FROZEN_SIGN))
    out["evidence_label"] = "FROZEN_HYPOTHESIS: " + DISCOVERY_DISCLOSURE
    return out


def frozen_specification() -> dict:
    """Everything that is nailed down, named one field at a time.

    Each value is READ from the module that owns it. A reader checking that the
    confirmation ran the same rule as the discovery does not have to trust this
    docstring - the numbers here are the ones the scorer used.
    """
    return {
        "schema": "alpha_recovery_forward_specification/1",
        "challenger_id": CHALLENGER_ID,
        "underlying": OA.UNDERLYING,
        "asset_class": ASSET_CLASS, "venue": VENUE,
        "information_source": "the moneyness-anchored SPY option surface "
                              "(%s / %s, +/-%.0f %% moneyness band)"
                              % (OA.DATASET, OA.SCHEMA, OA.MONEYNESS_BAND * 100),
        "feature": SOURCE_SIGNAL,
        "feature_definition": "mean implied volatility of near-expiry puts at moneyness "
                              "<= 0.98 minus mean implied volatility of near-expiry calls at "
                              "moneyness >= 1.02, on the SAME expiry",
        "sign": FROZEN_SIGN,
        "sign_is_reversed_relative_to": {"signal": SOURCE_SIGNAL, "original_sign": SOURCE_SIGN,
                                         "original_economics": OS.SIGNALS[SOURCE_SIGNAL]["economics"]},
        "horizon_sessions": FROZEN_HORIZON,
        "position": "long/short, gross 1.0, %+d x sign of a strictly-trailing "
                    "%d-observation z-score" % (FROZEN_SIGN, OS.ZSCORE_LOOKBACK),
        "zscore_lookback": OS.ZSCORE_LOOKBACK,
        "zscore_construction": "mean and standard deviation of STRICTLY PRIOR observations "
                               "only (shift(1) before rolling)",
        "cadence": FROZEN_HORIZON, "overlapping": False,
        "snapshot_et": "%02d:%02d" % OA.SNAPSHOT_ET,
        "moneyness_construction": "strike / put-call-parity forward, measured inside the same "
                                  "snapshot as the option quote",
        "expiry_construction": "third Friday monthly expiries, each bought over the %d days "
                               "before it; holiday shifts resolved by the venue"
                               % OA.LOOKBACK_DAYS,
        "strike_band": {"moneyness": OA.MONEYNESS_BAND, "spacing": OA.STRIKE_SPACING},
        "quote_schema": OA.SCHEMA,
        "implied_volatility": "Black-76 by bisection on the quote midpoint; forward and "
                              "discount from put-call parity C - P = D*(F - K)",
        "cost_ladder_bps_per_side": list(OS.COST_LADDER_BPS),
        "cost_primary_bps_per_side": OS.COST_PRIMARY_BPS,
        "cost_stress_bps_per_side": OS.COST_STRESS_BPS,
        "risk_sizing": "gross 1.0 per decision, unlevered, unchanged",
        "equal_risk_comparison": "alpha_agent.alpha_recovery.intraday_alpha.equal_risk_daily "
                                 "against %s" % INCUMBENT_MODEL_ID,
        "qualification_gates": "alpha_agent.alpha_recovery.options_surface.gates - unchanged",
        "incumbent": INCUMBENT_MODEL_ID,
        "emission_rule": "the decision is formed from the 15:45 ET option snapshot on session t "
                         "and held for %d sessions; no backfill" % FROZEN_HORIZON,
        "price_mark_owner": MARK_OWNER,
        "inception": INCEPTION,
        "discovery_sample": list(DISCOVERY_SAMPLE),
        "discovery_sample_is_qualification_evidence": False,
        "discovery_disclosure": DISCOVERY_DISCLOSURE,
        "not_optimised": {
            "alternate_thresholds": 0, "alternate_lookbacks": 0, "alternate_horizons": 0,
            "alternate_signs": 0, "rescue_arms": 0, "parameter_search": False,
            "note": "one specification enters and one result comes out; this module contains "
                    "no grid, and a confirmation that can be re-run with a different "
                    "parameter is not a confirmation"},
        "protocol_sha256": protocol_hash(), "contract_sha256": contract_hash(),
    }


# --------------------------------------------------------------------------- #
# The independent historical confirmation
# --------------------------------------------------------------------------- #
def disjointness() -> dict:
    """PROVE the confirmation window shares no date with the discovery sample.

    Measured from the two surface files, not asserted from the two windows: a
    window is what was requested and a surface is what arrived, and only the
    second one can overlap.
    """
    out = {"requested_confirmation_window": list(CONFIRMATION_WINDOW),
           "declared_discovery_window": list(DISCOVERY_SAMPLE)}
    # compare against the surface the axis ACTUALLY runs on, not a fixed name:
    # if a corrected build supersedes the original, the disjointness claim must
    # be about the dates that were really measured
    dp, cp = OS.surface_path(), OA.surface_path(OA.CONFIRMATION_TAG)
    out["discovery_surface"] = str(dp)
    out["confirmation_surface"] = str(cp)
    if not (dp.exists() and cp.exists()):
        out["state"] = "SURFACE_MISSING"
        return out
    d = set(str(x)[:10] for x in OS.features(surface=dp)["date"])
    c = set(str(x)[:10] for x in OS.features(surface=cp)["date"])
    shared = sorted(d & c)
    out.update({
        "state": "DISJOINT" if not shared else "OVERLAPPING",
        "discovery_dates": len(d), "confirmation_dates": len(c),
        "shared_dates": len(shared), "shared_examples": shared[:10],
        "discovery_span": [min(d), max(d)] if d else None,
        "confirmation_span": [min(c), max(c)] if c else None,
        "confirmation_ends_before_discovery_begins": bool(c and d and max(c) < min(d)),
    })
    return out


def confirmation(*, write: bool = True) -> dict:
    """Run the ONE frozen rule on the ONE untouched window. One result."""
    surf = OA.surface_path(OA.CONFIRMATION_TAG)
    body = {"schema": "alpha_recovery_reversed_skew_confirmation/1",
            "calculation_owner": CALCULATION_OWNER,
            "challenger_id": CHALLENGER_ID,
            "label": "INDEPENDENT_HISTORICAL_CONFIRMATION",
            "is_true_forward": False,
            "why_this_is_not_true_forward": (
                "these sessions had already happened when the rule was frozen. They were never "
                "inspected, which is what makes them independent, but independence is not "
                "prospectivity: only data that did not exist when the hypothesis was written "
                "can answer whether the effect survives forward."),
            "window": list(CONFIRMATION_WINDOW),
            "surface": str(surf),
            "disjointness": disjointness(),
            "frozen_specification": frozen_specification()}
    if not surf.exists():
        body["state"] = "SURFACE_NOT_ACQUIRED"
        body["classification"] = "INSUFFICIENT_EVIDENCE"
        if write:
            write_artifact(ARTIFACT_NAME.replace(".json", "_confirmation.json"), body)
        return body

    body["usability"] = OS.usability(surface=surf)
    cell = OS.measure_cell(spec(), surface=surf)

    # Multiplicity at the HONEST denominator. One hypothesis was frozen before
    # this window was touched, and exactly one test is run on it, so m = 1. That
    # denominator is only legitimate BECAUSE of the freeze - it is what a
    # pre-registration buys, and it is why no second arm may ever be added here.
    a = OS._lvl(cell, OS.COST_PRIMARY_BPS, "all")
    p_one = a.get("p_net_one_sided")
    if p_one is not None:
        bh = S.bh_fdr({cell["cell_id"]: p_one}, BH_Q)
        holm = FAM.holm({cell["cell_id"]: p_one}, HOLM_ALPHA)
        cell["fdr_pass"] = bh["per_test"].get(cell["cell_id"])
        cell["holm_pass_family"] = holm.get("rejected", {}).get(cell["cell_id"])
        cell["gates"] = OS.gates(cell, fdr_pass=cell["fdr_pass"],
                                 holm_pass=cell["holm_pass_family"])
        cell["verdict"] = OS.verdict(cell)
        body["multiple_testing"] = {
            "denominator": 1,
            "why_one": "one hypothesis, frozen in advance, tested once on a window chosen "
                       "before it was measured",
            "benjamini_hochberg": {k: v for k, v in bh.items() if k != "per_test"},
            "holm": {k: v for k, v in holm.items() if k != "adjusted"}}
    body["cell"] = T._jsonable(cell)
    body["brief"] = OS.brief(cell)
    body["independent_periods"] = a.get("periods")
    body["exposure_check"] = is_it_just_long_the_market(surf)
    body["classification"] = classify(cell)
    body["classification_rule"] = CLASSIFICATION_RULE
    body["what_a_pass_does_not_establish"] = [
        "this is HISTORICAL evidence on data that already existed; only TRUE_FORWARD can "
        "test the rule on sessions that did not exist when it was frozen",
        "the multiplicity denominator of 1 covers the SIGN, which was frozen before this "
        "window was touched. It does not cover the human choice of which feature, which "
        "horizon menu and which surface construction to build in the first place",
        "88 independent periods is under two years of non-overlapping observations",
        "an effect of this size in SPY options - among the most heavily traded and most "
        "heavily published markets that exist - is a priori more likely to be an artefact "
        "than an edge that survived everyone else looking for it, so the prior against it "
        "should stay strong until forward evidence accrues",
        "entry is the 16:00 close on the signal session, fifteen minutes after the 15:45 "
        "snapshot; no latency variant was run here, because a second arm would break the "
        "freeze",
    ]
    if write:
        write_artifact(ARTIFACT_NAME.replace(".json", "_confirmation.json"), body)
    return body


def is_it_just_long_the_market(surface) -> dict:
    """The check that could hollow out a PASS, so it is run either way.

    SPY rose hard across both windows. A rule that happened to be LONG most of
    the time would "confirm" simply by being a disguised buy-and-hold, and every
    gate in the battery would wave it through - they compare against the
    incumbent and against zero, not against the market. So the position itself
    is measured: how often it is long, how correlated the result is with SPY,
    what a fixed long position would have earned on the SAME entry dates, and
    whether the SHORT legs carry their own weight.

    This decomposes the ONE frozen arm. It adds no specification, changes no
    gate and cannot alter the classification.
    """
    import numpy as np

    sp = spec()
    f = OS.features(surface=surface)
    z = OS._z(f[sp["field"]])
    pos = float(sp["sign"]) * np.sign(z)
    px = f["underlying_close"].to_numpy()
    h = int(sp["horizon"])
    rule, market, sgn = [], [], []
    t = 0
    while t + h < len(f):
        p = pos[t]
        if np.isfinite(p):
            r = px[t + h] / px[t] - 1.0
            rule.append(p * r)
            market.append(r)
            sgn.append(p)
        t += h
    if len(rule) < 5:
        return {"state": "TOO_FEW_PERIODS"}
    rule, market, sgn = np.array(rule), np.array(market), np.array(sgn)
    ppy = OS.PPY / h
    out = {
        "state": "OK", "periods": int(len(rule)),
        "mean_position": float(sgn.mean()),
        "share_long": float((sgn > 0).mean()), "share_short": float((sgn < 0).mean()),
        "rule_ann_gross": float(rule.mean() * ppy),
        "buy_and_hold_ann_on_the_same_dates": float(market.mean() * ppy),
        "rule_minus_buy_and_hold_ann": float((rule.mean() - market.mean()) * ppy),
        "correlation_with_spy": float(np.corrcoef(rule, market)[0, 1]),
    }
    for s, name in ((1, "long"), (-1, "short")):
        m = sgn == s
        if m.sum() >= 5:
            out["%s_legs" % name] = {
                "n": int(m.sum()),
                "mean_return": float(rule[m].mean()),
                "ann": float(rule[m].mean() * ppy),
                "spy_mean_on_those_dates": float(market[m].mean())}
    out["verdict"] = (
        "NOT_A_DISGUISED_LONG" if abs(out["mean_position"]) < 0.25
        and abs(out["correlation_with_spy"]) < 0.5
        and (out.get("short_legs") or {}).get("mean_return", 0.0) > 0
        else "REVIEW_THE_EXPOSURE")
    return out


CLASSIFICATION_RULE = {
    "CONFIRMED": "every EXISTING qualification gate that this window can decide passes, "
                 "including the Benjamini-Hochberg and Holm checks at denominator 1",
    "FAILED": "the window decides the gates and at least one of them fails",
    "INSUFFICIENT_EVIDENCE": "the window cannot decide, because the existing "
                             "MIN_EFFECTIVE_PERIODS floor of %d independent periods is not "
                             "met" % MIN_EFFECTIVE_PERIODS,
    "no_new_threshold_was_created": True,
    "thresholds_are": "the frozen options_surface.gates, unchanged",
}


def classify(cell: dict) -> str:
    """CONFIRMED / FAILED / INSUFFICIENT_EVIDENCE, from the EXISTING gates only.

    No threshold is invented here. The floor that decides "insufficient" is the
    same ``MIN_EFFECTIVE_PERIODS`` every other arm in this campaign answered to,
    and the gates that decide pass or fail are ``options_surface.gates``
    verbatim.
    """
    a = OS._lvl(cell, OS.COST_PRIMARY_BPS, "all")
    if (a.get("periods") or 0) < MIN_EFFECTIVE_PERIODS:
        return "INSUFFICIENT_EVIDENCE"
    g = cell.get("gates") or {}
    decided = {k: v for k, v in g.items() if v is not None}
    if decided and all(decided.values()):
        return "CONFIRMED"
    return "FAILED"


# --------------------------------------------------------------------------- #
# The freeze record and the canonical TRUE_FORWARD registration
# --------------------------------------------------------------------------- #
def discovery_evidence() -> dict:
    """What the discovery sample said, carried as CONTEXT and labelled as such."""
    cells = OS.load_cells()
    src_id = "OPTIONS|SPY|%s|h%d" % (SOURCE_SIGNAL, FROZEN_HORIZON)
    arm = next((a for a in (OS.contradicted_signs(cells) or {}).get("arms") or []
                if a.get("cell_id") == src_id), None)
    return {
        "sample": list(DISCOVERY_SAMPLE),
        "status": "DISCOVERY_ONLY_NEVER_QUALIFICATION_EVIDENCE",
        "disclosure": DISCOVERY_DISCLOSURE,
        "as_declared_with_the_theoretical_sign": (arm or {}).get("as_declared"),
        "with_the_sign_the_data_preferred": (arm or {}).get("with_the_sign_the_data_prefers"),
        "gates_it_would_have_passed": (arm or {}).get("gates_it_would_then_pass"),
        "why_those_numbers_do_not_qualify_this_challenger": (
            "the direction was chosen by that sample, so every statistic computed on it - the "
            "holdout and the halves included - is conditioned on the choice. They size the "
            "hypothesis; they cannot test it."),
    }


def freeze_row() -> dict:
    """The immutable freeze, in the shape the canonical adoption owner reads.

    This is the campaign's own freeze record: the R62 path takes a freeze row,
    re-checks its lifecycle and derives the observation clock itself, so a
    challenger frozen here registers on exactly the same contract as one frozen
    by the research worker.
    """
    sp = frozen_specification()
    record_hash = stable_hash(sp)
    return {
        "hypothesis_id": "%s_%s" % (CHALLENGER_ID, record_hash[:12]),
        "release": RELEASE,
        "asset_class": ASSET_CLASS,
        "economic_family": OS.FAMILY,
        "model_family": "OPTION_SURFACE_SKEW_DIRECTIONAL",
        "horizon_sessions": FROZEN_HORIZON,
        "outcome": "FORWARD_FROZEN",
        "invalidated_reason": None,
        "input_data_identity": stable_hash({"surface": str(OA.surface_path()),
                                            "schema": OA.SCHEMA, "dataset": OA.DATASET,
                                            "band": OA.MONEYNESS_BAND}),
        "spec_json": {**sp, "instrument_scope": [OA.UNDERLYING], "venue": VENUE,
                      "sleeve": OS.FAMILY,
                      "cost_model": {"primary_bps_per_side": OS.COST_PRIMARY_BPS,
                                     "stress_bps_per_side": OS.COST_STRESS_BPS,
                                     "applied": "both legs of every decision"},
                      "feature_snapshot_hash": record_hash},
        "forward_challenger": {"challenger_id": CHALLENGER_ID,
                               "record_hash": record_hash,
                               "inception": INCEPTION},
    }


def freeze_record() -> dict:
    """The campaign-side immutable artifact, in ``forward_package`` shape."""
    sp = frozen_specification()
    body = {
        "schema": FP.RECORD_SCHEMA, "campaign_id": CAMPAIGN_ID,
        "challenger_id": CHALLENGER_ID,
        "cell_id": spec()["cell_id"],
        "classification": "FROZEN_PROSPECTIVE_HYPOTHESIS",
        "why": ("a pre-registered sign was contradicted by the data. The contradicted "
                "direction is not adoptable on the sample that suggested it, and is not "
                "discardable either - so it is frozen and submitted to evidence that did "
                "not choose it."),
        "kind": "REVERSED_PRE_REGISTERED_SIGN",
        "information_identity": {"family": OS.FAMILY, "dimension": OS.DIMENSION,
                                 "baseline": None, "signal": SOURCE_SIGNAL,
                                 "sign": FROZEN_SIGN},
        "asset_class": ASSET_CLASS, "horizon_sessions": FROZEN_HORIZON,
        "qualification_evidence": {
            "state": "NONE_YET",
            "discovery_sample_excluded": True,
            "discovery": discovery_evidence(),
            "independent_historical_confirmation": "see %s"
                                                   % ARTIFACT_NAME.replace(".json",
                                                                           "_confirmation.json"),
            "true_forward": "accrues from the first eligible session after registration"},
        "forward_specification": sp,
        "freeze_record_hash": stable_hash(sp),
        "human_gated_adoption_command": FP.adoption_command(CHALLENGER_ID),
        "promotion_allowed": False, "live_registration_performed": False,
        "holdings_changed": False, "records_are_immutable": True,
    }
    body["record_hash"] = stable_hash(body)
    return body


REGISTRATION_SCOPE = {
    "approved": "forward evidence registration only",
    "not_approved": ["promote the model", "allocate capital", "modify the portfolio",
                     "create orders", "merge to live", "deploy anything",
                     "replace the incumbent"],
    "backfill": "NONE - the registrar derives the boundary itself and the first legitimate "
                "observation is the first eligible session STRICTLY AFTER registration",
}

#: WHERE THE REGISTRATION LIVES, AND WHY IT IS NOT HERE.
#:
#: This package may never call a registrar or an adopter, and may never import
#: ``paper_trader.api`` - the campaign contract pins it and
#: ``test_package_never_calls_a_registrar_adopter_or_portfolio_owner`` enforces
#: it file by file. The separation is the safety property: research produces an
#: immutable freeze record and an exact command, and a human runs an operator
#: entrypoint that performs the governed act. Research that could register
#: itself would be one loop away from research that promotes itself.
#:
#: So the freeze record is built here, the registration is performed by
#: ``scripts/register_reversed_skew_challenger.py``, and this module learns the
#: outcome the only way it is allowed to: by reading the artifact that script
#: wrote.
REGISTRATION_OWNER = "scripts/register_reversed_skew_challenger.py"
REGISTRATION_ARTIFACT = "reversed_skew_registration.json"


def true_forward_state() -> dict:
    """What the canonical owner holds, read from the operator script's artifact.

    Deliberately NOT read from the live registry: that would mean importing the
    registrar into a research module, which is exactly what the contract forbids.
    """
    from . import read_artifact

    body = read_artifact(REGISTRATION_ARTIFACT)
    if not body or not body.get("registration"):
        return {"state": "NOT_REGISTERED", "challenger_id": CHALLENGER_ID,
                "registration_owner": REGISTRATION_OWNER}
    row = body["registration"]
    return {"state": "REGISTERED", "challenger_id": CHALLENGER_ID,
            "registration_owner": REGISTRATION_OWNER,
            "registration": row,
            "evidence_status": row.get("evidence_status"),
            "first_eligible_observation_session": row.get("first_eligible_observation_session"),
            "next_legitimate_maturity_session": row.get("next_legitimate_maturity_session"),
            "matured_observations": row.get("matured_observations"),
            "backfilled": bool(row.get("backfilled")),
            "capital_eligible_now": False,
            "why_not_capital_eligible": (
                "forward evidence has not matured, and adoption is human-gated in every case; "
                "registration starts a measurement and nothing else")}


def build(*, write: bool = True) -> dict:
    """The whole challenger, in one artifact: discovery, confirmation, forward."""
    rec = freeze_record()
    conf = confirmation(write=write)
    body = {
        "schema": "alpha_recovery_reversed_skew/1", "calculation_owner": CALCULATION_OWNER,
        "challenger_id": CHALLENGER_ID,
        "headline": "REVERSED SPY PUT-CALL SKEW",
        "frozen_specification": frozen_specification(),
        "freeze_record": rec,
        "discovery_sample": discovery_evidence(),
        "independent_historical_confirmation": {
            k: conf.get(k) for k in ("state", "window", "classification", "classification_rule",
                                     "usability", "disjointness", "brief", "independent_periods",
                                     "multiple_testing", "is_true_forward", "exposure_check",
                                     "why_this_is_not_true_forward",
                                     "what_a_pass_does_not_establish")},
        "true_forward": true_forward_state(),
        "capital_eligible_now": False,
        "promotion_performed": False, "holdings_changed": False,
        "registration_scope": REGISTRATION_SCOPE,
    }
    d = research_root() / FP.SUBDIR
    d.mkdir(parents=True, exist_ok=True)
    p = d / ("%s_%s.json" % (CHALLENGER_ID, rec["record_hash"][:12]))
    if not p.exists():
        p.write_text(json.dumps(rec, indent=1, sort_keys=True, default=str), encoding="utf-8")
    body["freeze_record_file"] = str(p)
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body
