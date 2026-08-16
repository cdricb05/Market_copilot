r"""Release 28 — the MATERIALITY / ANTI-CHURN gate (PURE kernel).

WHY THIS EXISTS
---------------
"React to new information" degenerates immediately into "reassess constantly" unless
something separates four different statements::

    DATA_CHANGED               a byte arrived that we had not seen
    SIGNAL_CHANGED             a business concept we actually use moved
    MATERIAL_SIGNAL_CHANGED    it moved enough to be worth looking again
    PORTFOLIO_DECISION_CHANGED the answer to "what should we hold?" is different

This kernel owns the first three. The FOURTH is emphatically not its business: whether
a change of holdings is economically justified after costs is decided by
``api.portfolio_reassessment`` and ``api.reallocation_proposal``, exactly as it is in
the daily cycle. This gate only decides whether those owners are asked the question.

DESIGN RULES THAT KEPT THIS HONEST
----------------------------------
* Thresholds are CONSERVATIVE and are set to levels that are obviously material on
  their face (a 7% single-day move in a holding; a 20% drawdown; a halt). They were
  NOT searched over outcomes — no threshold here was chosen because it produced more
  trades, and none is fitted to any backtest.
* Triggering a reassessment is not an action. Every trigger costs one read-only
  assessment; the transaction-cost hurdle downstream is untouched.
* The gate is idempotent by fingerprint: the same set of triggering facts against the
  same portfolio produces the same fingerprint, and a repeated fingerprint suppresses
  the reassessment instead of duplicating it.
* Authority is obeyed, never re-litigated: an ``EVENT_TRIGGER_ONLY`` event can put a
  holding on the review list and can never contribute a score.

PURITY
------
No IO, no clock, no store, no network, no ``api.*`` import. Creates no order.
"""
from __future__ import annotations

from typing import Any, Iterable, Optional

from . import event_fabric as ef

PHASE = "RELEASE28"
CALCULATION_OWNER = "engine.event_materiality"
MATERIALITY_POLICY_VERSION = "event_materiality.v1"

# --------------------------------------------------------------------------- #
# Change levels (the frozen vocabulary)
# --------------------------------------------------------------------------- #
LVL_NO_CHANGE = "NO_CHANGE"
LVL_DATA_CHANGED = "DATA_CHANGED"
LVL_SIGNAL_CHANGED = "SIGNAL_CHANGED"
LVL_MATERIAL_SIGNAL_CHANGED = "MATERIAL_SIGNAL_CHANGED"
CHANGE_LEVELS = (LVL_NO_CHANGE, LVL_DATA_CHANGED, LVL_SIGNAL_CHANGED,
                 LVL_MATERIAL_SIGNAL_CHANGED)

# --------------------------------------------------------------------------- #
# Trigger codes
# --------------------------------------------------------------------------- #
T_STRUCTURAL_INFORMATION = "STRUCTURAL_INFORMATION_ARRIVED"
T_MATERIAL_COMPANY_EVENT = "MATERIAL_COMPANY_EVENT"
T_HOLDING_PRICE_SHOCK = "HOLDING_PRICE_SHOCK"
T_HOLDING_DRAWDOWN = "HOLDING_DRAWDOWN_BREACH"
T_HOLDING_VOLATILITY = "HOLDING_VOLATILITY_BREACH"
T_HOLDING_LIQUIDITY = "HOLDING_LIQUIDITY_BREACH"
T_HOLDING_TRADABILITY = "HOLDING_TRADABILITY_BLOCKED"
T_RANK_DETERIORATION = "HOLDING_RANK_DETERIORATION"
T_ALTERNATIVE_IMPROVEMENT = "ALTERNATIVE_IMPROVEMENT"
T_CORPORATE_ACTION = "CORPORATE_ACTION_OUTSTANDING"
T_REGIME_TRANSITION = "MARKET_REGIME_TRANSITION"
T_UNIVERSE_ELIGIBILITY = "UNIVERSE_ELIGIBILITY_CHANGED"
TRIGGER_CODES = (
    T_STRUCTURAL_INFORMATION, T_MATERIAL_COMPANY_EVENT, T_HOLDING_PRICE_SHOCK,
    T_HOLDING_DRAWDOWN, T_HOLDING_VOLATILITY, T_HOLDING_LIQUIDITY,
    T_HOLDING_TRADABILITY, T_RANK_DETERIORATION, T_ALTERNATIVE_IMPROVEMENT,
    T_CORPORATE_ACTION, T_REGIME_TRANSITION, T_UNIVERSE_ELIGIBILITY,
)

# --------------------------------------------------------------------------- #
# Suppression codes (why an arriving event did NOT cause a reassessment)
# --------------------------------------------------------------------------- #
S_NO_NEW_INFORMATION = "NO_NEW_INFORMATION"
S_DUPLICATE_STORY = "DUPLICATE_STORY_SUPPRESSED"
S_NON_TRIGGER_AUTHORITY = "AUTHORITY_MAY_NOT_TRIGGER"
S_UNRELATED_ENTITY = "ENTITY_NOT_HELD_AND_NOT_A_CANDIDATE"
S_BELOW_THRESHOLD = "BELOW_MATERIALITY_THRESHOLD"
S_DUPLICATE_TRIGGER = "DUPLICATE_TRIGGER_FINGERPRINT"
S_UNMAPPED_ENTITY = "EVENT_NOT_MAPPED_TO_A_SECURITY"
SUPPRESSION_CODES = (S_NO_NEW_INFORMATION, S_DUPLICATE_STORY, S_NON_TRIGGER_AUTHORITY,
                     S_UNRELATED_ENTITY, S_BELOW_THRESHOLD, S_DUPLICATE_TRIGGER,
                     S_UNMAPPED_ENTITY)

# --------------------------------------------------------------------------- #
# Policy. Every threshold states WHAT it means and WHY it is where it is. None of
# these numbers is fitted; each is a level that is material on its face.
# --------------------------------------------------------------------------- #
DEFAULT_POLICY: dict[str, Any] = {
    "policy_version": MATERIALITY_POLICY_VERSION,
    "abs_return_1d": 0.07,
    "abs_return_5d": 0.15,
    "max_drawdown": 0.20,
    "volatility_ratio": 1.75,
    "liquidity_floor_usd": 2_000_000.0,
    "rank_deterioration_places": 25,
    "score_change": 0.10,
    "alternative_rank_advantage": 25,
    "candidate_depth": 100,
    "require_informative_novelty": True,
    "structural_requires_held_or_candidate": True,
    "reasons": {
        "abs_return_1d": ("A 7% single-session move in a held name is far outside the "
                          "ordinary daily range of a large-cap and is worth looking at "
                          "on its face. Not fitted to any outcome."),
        "abs_return_5d": ("A 15% move over a week is a change in the market's view of "
                          "the name, not noise."),
        "max_drawdown": ("A 20% peak-to-trough drawdown is the conventional bear "
                         "threshold and is already the level the risk surface reports."),
        "volatility_ratio": ("Realized volatility 1.75x its own prior level means the "
                             "risk the position contributes has genuinely changed, even "
                             "if the thesis has not."),
        "liquidity_floor_usd": ("Below ~$2m median daily dollar volume a 4% book weight "
                                "is no longer cleanly exitable at the modelled cost."),
        "rank_deterioration_places": ("A 25-place fall in the operational ranking is a "
                                      "real change in relative attractiveness, not "
                                      "cross-sectional jitter."),
        "score_change": ("A 0.10 move in the normalised combined score is large "
                         "relative to the cross-sectional dispersion of the released "
                         "model."),
        "alternative_rank_advantage": ("An alternative must be at least 25 places "
                                       "better than the holding it would replace "
                                       "before the switching question is even asked; "
                                       "the cost hurdle downstream is unchanged."),
        "candidate_depth": ("Structural information about a name outside the top 100 "
                            "cannot change the target book, so it is recorded without "
                            "triggering."),
    },
    "not_fitted": True,
    "fitted_to_outcomes": False,
}


def resolve_policy(overrides: Optional[dict] = None) -> dict:
    """Merge caller overrides over the default policy (tests / explicit callers)."""
    policy = dict(DEFAULT_POLICY)
    for k, v in (overrides or {}).items():
        if k in ("reasons", "policy_version"):
            continue
        policy[k] = v
    return policy


def _f(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def _norm_set(values: Any) -> set[str]:
    return {str(v).strip().upper() for v in (values or []) if str(v or "").strip()}


def _trigger(code, *, entity, why, observed=None, threshold=None, family=None,
             authority=None, event_id=None, speed=None, event_date=None) -> dict:
    return {"code": code, "entity": entity, "why": why, "observed": observed,
            "threshold": threshold, "family": family, "decision_authority": authority,
            "event_id": event_id, "event_date": event_date, "signal_speed": speed,
            "occurrences": 1, "event_ids": ([event_id] if event_id else []),
            "changed_score": False, "creates_order": False}


#: How many contributing event ids one collapsed trigger keeps. Lineage, not a corpus.
_MAX_TRIGGER_EVENT_IDS = 5


def collapse_triggers(triggers: list) -> list[dict]:
    """Collapse triggers that say the SAME THING about the same security on the same day.

    A wire story re-collected under six symbol scopes, or an SEC accession seen by two
    collector lanes, must not become six reasons to reassess one holding. The collapse
    key deliberately includes the event DATE, so tomorrow's genuinely new 8-K on the
    same name is a NEW trigger rather than a suppressed repeat of today's.
    """
    out: dict[tuple, dict] = {}
    for t in (triggers or []):
        key = (t.get("code"), t.get("entity"), t.get("family"), t.get("event_date"))
        prior = out.get(key)
        if prior is None:
            out[key] = dict(t)
            continue
        prior["occurrences"] = int(prior.get("occurrences") or 1) + 1
        ids = prior.setdefault("event_ids", [])
        if t.get("event_id") and len(ids) < _MAX_TRIGGER_EVENT_IDS:
            ids.append(t["event_id"])
    ordered = sorted(out.values(),
                     key=lambda t: (str(t.get("code")), str(t.get("entity") or ""),
                                    str(t.get("family") or ""),
                                    str(t.get("event_date") or "")))
    for t in ordered:
        n = int(t.get("occurrences") or 1)
        if n > 1:
            t["why"] = ("%s (%d re-collected observations of the same information were "
                        "collapsed into one reason)" % (t["why"], n))
    return ordered


def _suppressed(code, *, entity, why, family=None, authority=None,
                event_id=None) -> dict:
    return {"code": code, "entity": entity, "why": why, "family": family,
            "decision_authority": authority, "event_id": event_id}


# --------------------------------------------------------------------------- #
# Event-driven triggers
# --------------------------------------------------------------------------- #
def _event_triggers(*, events: Iterable[dict], held: set[str], candidates: set[str],
                    policy: dict) -> tuple[list[dict], list[dict]]:
    triggers: list[dict] = []
    suppressed: list[dict] = []
    for e in (events or []):
        fam = e.get("family")
        auth = str(e.get("decision_authority"))
        eid = e.get("event_id")
        entity = e.get("primary_ticker")
        ents = _norm_set(e.get("entities"))

        if policy.get("require_informative_novelty", True) and \
                not ef.carries_new_information(e):
            suppressed.append(_suppressed(
                S_DUPLICATE_STORY if e.get("duplicate_of") else S_NO_NEW_INFORMATION,
                entity=entity, family=fam, authority=auth, event_id=eid,
                why=("%s — %s" % (e.get("novelty"), e.get("novelty_reason") or
                                  "carries no new fact"))))
            continue

        if not ef.authority_may_trigger_reassessment(auth):
            suppressed.append(_suppressed(
                S_NON_TRIGGER_AUTHORITY, entity=entity, family=fam, authority=auth,
                event_id=eid,
                why=("%s authority may not request a reassessment. %s"
                     % (auth, e.get("why_authority") or ""))))
            continue

        relevant = ents & (held | candidates)
        if not ents:
            # A market-wide event (regime, macro) has no entity; it is judged by the
            # regime/risk path below, not here.
            if fam not in (ef.F_MACRO_REGIME,):
                suppressed.append(_suppressed(
                    S_UNMAPPED_ENTITY, entity=None, family=fam, authority=auth,
                    event_id=eid,
                    why="No security could be resolved from this event; it decides nothing."))
            continue
        if not relevant:
            suppressed.append(_suppressed(
                S_UNRELATED_ENTITY, entity=entity, family=fam, authority=auth,
                event_id=eid,
                why=("%s is neither held nor within the top %d candidates, so this event "
                     "cannot change the target book."
                     % (entity, int(policy.get("candidate_depth") or 0)))))
            continue

        speed = str(e.get("signal_speed"))
        edate = str(e.get("effective_at") or e.get("published_at") or "")[:10] or None
        for tkr in sorted(relevant):
            if fam in (ef.F_STRUCTURAL_REPORT, ef.F_FUNDAMENTAL_FACT):
                triggers.append(_trigger(
                    T_STRUCTURAL_INFORMATION, entity=tkr, family=fam, authority=auth,
                    event_id=eid, speed=speed, event_date=edate,
                    why=("New structural fundamental information became available for "
                         "%s (%s). The dependent structural inputs are refreshed through "
                         "the canonical scoring owner." % (tkr, e.get("event_type")))))
            elif fam == ef.F_CORPORATE_ACTION:
                triggers.append(_trigger(
                    T_CORPORATE_ACTION, entity=tkr, family=fam, authority=auth,
                    event_id=eid, speed=speed, event_date=edate,
                    why=("A corporate action (%s) affects %s; share counts and marks "
                         "must be reconciled before any comparison is trusted."
                         % (e.get("event_type"), tkr))))
            elif fam == ef.F_TRADING_HALT:
                triggers.append(_trigger(
                    T_HOLDING_TRADABILITY, entity=tkr, family=fam, authority=auth,
                    event_id=eid, speed=speed, event_date=edate,
                    why="%s is halted; the position is currently not exitable." % tkr))
            elif fam == ef.F_UNIVERSE_MEMBERSHIP:
                triggers.append(_trigger(
                    T_UNIVERSE_ELIGIBILITY, entity=tkr, family=fam, authority=auth,
                    event_id=eid, speed=speed, event_date=edate,
                    why="Index membership for %s changed, altering eligibility." % tkr))
            else:
                triggers.append(_trigger(
                    T_MATERIAL_COMPANY_EVENT, entity=tkr, family=fam, authority=auth,
                    event_id=eid, speed=speed, event_date=edate,
                    why=("A material company event (%s / %s) named %s. TRIGGER ONLY: it "
                         "puts the holding on the review list and contributes no "
                         "expected return." % (fam, e.get("event_type"), tkr))))
    return triggers, suppressed


# --------------------------------------------------------------------------- #
# Risk-state triggers (fast lane) and ranking triggers (slow lane)
# --------------------------------------------------------------------------- #
def _risk_triggers(*, risk_state: dict, held: set[str], policy: dict
                   ) -> tuple[list[dict], list[dict]]:
    triggers: list[dict] = []
    suppressed: list[dict] = []
    for tkr in sorted(held):
        row = (risk_state or {}).get(tkr) or {}
        hit = False
        r1 = _f(row.get("ret_1"))
        if r1 is not None and abs(r1) >= float(policy["abs_return_1d"]):
            hit = True
            triggers.append(_trigger(
                T_HOLDING_PRICE_SHOCK, entity=tkr, observed=r1,
                threshold=policy["abs_return_1d"], speed=ef.SPEED_MARKET_RISK,
                authority=ef.AUTH_OPERATIONAL_RISK,
                why=("%s moved %.2f%% in one session, beyond the %.1f%% materiality "
                     "level. The amount of capital allocated is reassessed; the thesis "
                     "score is untouched."
                     % (tkr, r1 * 100.0, float(policy["abs_return_1d"]) * 100.0))))
        r5 = _f(row.get("ret_5"))
        if r5 is not None and abs(r5) >= float(policy["abs_return_5d"]):
            hit = True
            triggers.append(_trigger(
                T_HOLDING_PRICE_SHOCK, entity=tkr, observed=r5,
                threshold=policy["abs_return_5d"], speed=ef.SPEED_MARKET_RISK,
                authority=ef.AUTH_OPERATIONAL_RISK,
                why="%s moved %.2f%% over five sessions." % (tkr, r5 * 100.0)))
        dd = _f(row.get("maxdd_252"))
        if dd is not None and abs(dd) >= float(policy["max_drawdown"]):
            hit = True
            triggers.append(_trigger(
                T_HOLDING_DRAWDOWN, entity=tkr, observed=dd,
                threshold=-abs(float(policy["max_drawdown"])),
                speed=ef.SPEED_MARKET_RISK, authority=ef.AUTH_OPERATIONAL_RISK,
                why=("%s is %.1f%% below its trailing peak, past the %.0f%% drawdown "
                     "level." % (tkr, abs(dd) * 100.0,
                                 float(policy["max_drawdown"]) * 100.0))))
        ratio = _f(row.get("volatility_ratio"))
        if ratio is not None and ratio >= float(policy["volatility_ratio"]):
            hit = True
            triggers.append(_trigger(
                T_HOLDING_VOLATILITY, entity=tkr, observed=ratio,
                threshold=policy["volatility_ratio"], speed=ef.SPEED_MARKET_RISK,
                authority=ef.AUTH_OPERATIONAL_RISK,
                why=("Realized volatility for %s is %.2fx its prior level; the risk this "
                     "position contributes has changed." % (tkr, ratio))))
        liq = _f(row.get("median_dollar_volume"))
        if liq is not None and liq < float(policy["liquidity_floor_usd"]):
            hit = True
            triggers.append(_trigger(
                T_HOLDING_LIQUIDITY, entity=tkr, observed=liq,
                threshold=policy["liquidity_floor_usd"], speed=ef.SPEED_MARKET_RISK,
                authority=ef.AUTH_OPERATIONAL_RISK,
                why=("Median owned dollar volume for %s fell below the exitability "
                     "floor." % tkr)))
        if not hit and row:
            suppressed.append(_suppressed(
                S_BELOW_THRESHOLD, entity=tkr, authority=ef.AUTH_OPERATIONAL_RISK,
                why=("Risk state for %s moved within its ordinary range; no "
                     "reassessment is warranted." % tkr)))
    return triggers, suppressed


def _ranking_triggers(*, rank_deltas: dict, held: set[str], policy: dict
                      ) -> tuple[list[dict], list[dict]]:
    triggers: list[dict] = []
    suppressed: list[dict] = []
    for tkr in sorted(held):
        row = (rank_deltas or {}).get(tkr) or {}
        before, after = row.get("rank_before"), row.get("rank_after")
        moved = False
        if before is not None and after is not None:
            drop = int(after) - int(before)
            if drop >= int(policy["rank_deterioration_places"]):
                moved = True
                triggers.append(_trigger(
                    T_RANK_DETERIORATION, entity=tkr, observed=drop,
                    threshold=policy["rank_deterioration_places"],
                    speed=ef.SPEED_STRUCTURAL, authority=ef.AUTH_OPERATIONAL_ALPHA,
                    why=("%s fell %d places in the operational ranking (%s -> %s); its "
                         "opportunity cost is reassessed against the current "
                         "alternatives." % (tkr, drop, before, after))))
        sb, sa = _f(row.get("score_before")), _f(row.get("score_after"))
        if sb is not None and sa is not None and abs(sa - sb) >= float(policy["score_change"]):
            moved = True
            triggers.append(_trigger(
                T_RANK_DETERIORATION, entity=tkr, observed=(sa - sb),
                threshold=policy["score_change"], speed=ef.SPEED_STRUCTURAL,
                authority=ef.AUTH_OPERATIONAL_ALPHA,
                why="%s combined score moved %.3f." % (tkr, sa - sb)))
        adv = row.get("best_alternative_rank_advantage")
        if adv is not None and int(adv) >= int(policy["alternative_rank_advantage"]):
            moved = True
            triggers.append(_trigger(
                T_ALTERNATIVE_IMPROVEMENT, entity=tkr, observed=int(adv),
                threshold=policy["alternative_rank_advantage"],
                speed=ef.SPEED_STRUCTURAL, authority=ef.AUTH_OPERATIONAL_ALPHA,
                why=("An eligible alternative now ranks %d places above %s. Whether "
                     "switching is worth its cost remains a decision for the "
                     "reallocation owner." % (int(adv), tkr))))
        if not moved and row:
            suppressed.append(_suppressed(
                S_BELOW_THRESHOLD, entity=tkr, authority=ef.AUTH_OPERATIONAL_ALPHA,
                why="%s ranking and score are materially unchanged." % tkr))
    return triggers, suppressed


def _regime_trigger(*, regime_before: Any, regime_after: Any) -> list[dict]:
    if regime_before is None or regime_after is None:
        return []
    if str(regime_before) == str(regime_after):
        return []
    return [_trigger(
        T_REGIME_TRANSITION, entity=None, observed=str(regime_after),
        threshold=str(regime_before), speed=ef.SPEED_MARKET_RISK,
        authority=ef.AUTH_OPERATIONAL_RISK,
        why=("The market regime state moved from %s to %s. A new macro OBSERVATION is "
             "never material on its own; a state TRANSITION is."
             % (regime_before, regime_after)))]


# --------------------------------------------------------------------------- #
# The gate
# --------------------------------------------------------------------------- #
def assess_materiality(*, events: Optional[list] = None,
                       risk_state: Optional[dict] = None,
                       rank_deltas: Optional[dict] = None,
                       holdings: Any = None, candidates: Any = None,
                       regime_before: Any = None, regime_after: Any = None,
                       portfolio_state_hash: Any = None,
                       prior_trigger_fingerprint: Any = None,
                       policy_overrides: Optional[dict] = None) -> dict:
    """Decide whether the arriving information justifies asking the portfolio question.

    Returns the four-level change verdict, every trigger with its reason and threshold,
    every suppression with its reason, and a deterministic trigger fingerprint that
    makes a repeat of the SAME facts a no-op.
    """
    policy = resolve_policy(policy_overrides)
    held = _norm_set(holdings)
    cands = _norm_set(candidates)
    evs = list(events or [])

    admitted = [e for e in evs if ef.carries_new_information(e)]
    data_changed = bool(evs)

    ev_trig, ev_supp = _event_triggers(events=evs, held=held, candidates=cands,
                                       policy=policy)
    risk_trig, risk_supp = _risk_triggers(risk_state=risk_state or {}, held=held,
                                          policy=policy)
    rank_trig, rank_supp = _ranking_triggers(rank_deltas=rank_deltas or {}, held=held,
                                             policy=policy)
    regime_trig = _regime_trigger(regime_before=regime_before, regime_after=regime_after)

    triggers = collapse_triggers(ev_trig + risk_trig + rank_trig + regime_trig)
    suppressed = ev_supp + risk_supp + rank_supp

    # SIGNAL_CHANGED is weaker than MATERIAL: a concept we use moved at all.
    signal_changed = bool(admitted or risk_state or rank_deltas)
    material = bool(triggers)

    affected = sorted({t["entity"] for t in triggers if t.get("entity")})
    fingerprint_basis = {
        "policy_version": MATERIALITY_POLICY_VERSION,
        "portfolio_state_hash": str(portfolio_state_hash or ""),
        # Keyed on WHAT was concluded about WHICH security on WHICH day — never on the
        # individual event id, so re-collecting the same information cannot manufacture
        # a "new" fingerprint and a second assessment.
        "triggers": sorted(
            "%s|%s|%s|%s" % (t["code"], t.get("entity") or "", t.get("family") or "",
                             t.get("event_date") or "")
            for t in triggers),
    }
    fingerprint = ef.content_fingerprint(fingerprint_basis)[:32]
    duplicate = bool(prior_trigger_fingerprint
                     and str(prior_trigger_fingerprint) == fingerprint and material)
    if duplicate:
        suppressed.append(_suppressed(
            S_DUPLICATE_TRIGGER, entity=None,
            why=("The identical set of triggering facts was already assessed against "
                 "this portfolio state (fingerprint %s). Re-running would duplicate the "
                 "assessment and the proposal without new information." % fingerprint)))

    reassessment_required = bool(material and not duplicate)

    if material:
        level = LVL_MATERIAL_SIGNAL_CHANGED
    elif signal_changed:
        level = LVL_SIGNAL_CHANGED
    elif data_changed:
        level = LVL_DATA_CHANGED
    else:
        level = LVL_NO_CHANGE

    if reassessment_required:
        codes = sorted({t["code"] for t in triggers})
        reason = ("%d material change(s) affecting %d security(ies): %s."
                  % (len(triggers), len(affected), ", ".join(codes)))
    elif duplicate:
        reason = ("No reassessment: the identical triggering facts were already "
                  "assessed against this portfolio state.")
    elif data_changed:
        reason = ("No reassessment: information arrived but nothing crossed a "
                  "materiality threshold, and %d item(s) were suppressed as duplicate, "
                  "unrelated or non-triggering." % len(suppressed))
    else:
        reason = "No reassessment: no new information arrived since the last watermark."

    return {
        "phase": PHASE,
        "calculation_owner": CALCULATION_OWNER,
        "policy_version": MATERIALITY_POLICY_VERSION,
        "policy": policy,
        "change_level": level,
        "change_level_vocabulary": list(CHANGE_LEVELS),
        "data_changed": data_changed,
        "signal_changed": signal_changed,
        "material_signal_changed": material,
        "reassessment_required": reassessment_required,
        "reassessment_reason": reason,
        "events_seen": len(evs),
        "events_carrying_new_information": len(admitted),
        "triggers": triggers,
        "trigger_count": len(triggers),
        "trigger_codes": sorted({t["code"] for t in triggers}),
        "suppressed": suppressed,
        "suppressed_count": len(suppressed),
        "affected_entities": affected,
        "trigger_fingerprint": fingerprint,
        "prior_trigger_fingerprint": (str(prior_trigger_fingerprint)
                                      if prior_trigger_fingerprint else None),
        "duplicate_of_prior_trigger": duplicate,
        "portfolio_state_hash": (str(portfolio_state_hash)
                                 if portfolio_state_hash else None),
        "authority_note": ("No trigger changed a score. Only OPERATIONAL_ALPHA events "
                           "reach the scoring owner; EVENT_TRIGGER_ONLY events reach the "
                           "review list and nothing else."),
        "cost_hurdle_note": ("A reassessment is a read-only question. Whether a change "
                             "of holdings survives transaction costs is decided by "
                             "api.portfolio_reassessment and api.reallocation_proposal, "
                             "unchanged by this gate."),
        "creates_orders": False,
        "automatic_execution": False,
    }


def policy_contract() -> dict:
    """The machine-readable materiality policy (thresholds + provenance)."""
    return {
        "contract_id": "paper_trader.event_materiality_policy/1",
        "policy_version": MATERIALITY_POLICY_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "change_levels": list(CHANGE_LEVELS),
        "trigger_codes": list(TRIGGER_CODES),
        "suppression_codes": list(SUPPRESSION_CODES),
        "thresholds": {k: v for k, v in DEFAULT_POLICY.items()
                       if k not in ("reasons", "policy_version")},
        "threshold_reasons": dict(DEFAULT_POLICY["reasons"]),
        "fitted_to_outcomes": False,
        "separation": ("This policy decides whether to ASK the portfolio question. It "
                       "never decides the answer, never sizes a position and never "
                       "changes a model. It is versioned separately from every alpha "
                       "model calibration."),
        "anti_churn": ("Identical triggering facts against an identical portfolio state "
                       "produce an identical fingerprint, and a repeated fingerprint "
                       "suppresses the reassessment instead of duplicating it."),
    }


__all__ = [
    "PHASE", "CALCULATION_OWNER", "MATERIALITY_POLICY_VERSION", "DEFAULT_POLICY",
    "CHANGE_LEVELS", "LVL_NO_CHANGE", "LVL_DATA_CHANGED", "LVL_SIGNAL_CHANGED",
    "LVL_MATERIAL_SIGNAL_CHANGED", "TRIGGER_CODES", "SUPPRESSION_CODES",
    "T_STRUCTURAL_INFORMATION", "T_MATERIAL_COMPANY_EVENT", "T_HOLDING_PRICE_SHOCK",
    "T_HOLDING_DRAWDOWN", "T_HOLDING_VOLATILITY", "T_HOLDING_LIQUIDITY",
    "T_HOLDING_TRADABILITY", "T_RANK_DETERIORATION", "T_ALTERNATIVE_IMPROVEMENT",
    "T_CORPORATE_ACTION", "T_REGIME_TRANSITION", "T_UNIVERSE_ELIGIBILITY",
    "S_NO_NEW_INFORMATION", "S_DUPLICATE_STORY", "S_NON_TRIGGER_AUTHORITY",
    "S_UNRELATED_ENTITY", "S_BELOW_THRESHOLD", "S_DUPLICATE_TRIGGER", "S_UNMAPPED_ENTITY",
    "resolve_policy", "assess_materiality", "policy_contract", "collapse_triggers",
]
