"""alpha_agent.r37.market_structure - Track D: the market-structure backlog.

DESIGN ONLY. ``contract.MARKET_STRUCTURE_EXPERIMENT_IN_SCOPE`` is False and
there is no code path in this module that reads a price, computes a feature or
judges a book. What it produces is a bounded, pre-registered research design so
the idea survives to the release that can afford to run it.

The lane comes from discretionary trading-floor practice: trend structure, swing
points, impulse and retracement, breakouts and retests, support and resistance,
channels, volatility contraction, volume confirmation, and multi-timeframe
agreement. Practitioners use these; this project has never tested them.

**Fibonacci is a hypothesis, not doctrine.** The canonical retracement and
extension levels are declared as a testable set, and every future test of them
MUST run the same design against placebo levels drawn from between them. Without
the placebo arm, a positive result cannot distinguish *"the 61.8 % level is
special"* from *"buying a pullback inside a trend works"*, and the second
explanation is both simpler and already partly supported by Release 36's finding
that short-horizon currency moves continue rather than revert.

**The anti-hindsight rule is the whole design.** A swing high is only a swing
high in retrospect. Any future implementation must identify a pivot through a
CONFIRMATION MECHANISM that would have fired in real time - a declared number of
subsequent bars, or a declared ATR displacement - and must stamp the pivot with
the date the confirmation completed, never with the date of the extreme itself.
A backtest that uses the extreme's own date is measuring the future.
"""
from __future__ import annotations

from typing import Optional

from .. import r37
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r37.market_structure"
SCHEMA = "r37_market_structure_visual_intelligence_backlog/1"
ARTIFACT_NAME = "market_structure_visual_intelligence_backlog.json"

C = _contract

EXECUTED_IN_THIS_RELEASE = False
READS_A_PRICE = False
COMPUTES_A_FEATURE = False
JUDGES_A_BOOK = False

# --------------------------------------------------------------------------- #
# The anti-hindsight rules. These are the reason the lane is worth designing
# carefully rather than coding quickly.
# --------------------------------------------------------------------------- #
PIVOT_CONFIRMATION_REQUIRED = True
PIVOT_TIMESTAMP_IS_THE_CONFIRMATION_DATE = True
FUTURE_KNOWN_EXTREMA_ALLOWED = False

#: A pivot is confirmed when BOTH hold, and it is stamped with the later date.
PIVOT_CONFIRMATION_RULES = (
    "N subsequent sessions have closed without exceeding the candidate "
    "extreme, where N is declared before the test and never searched",
    "price has displaced from the candidate extreme by at least K x ATR, "
    "where K is declared before the test and never searched",
)
PIVOT_CONFIRMATION_SESSIONS = 5
PIVOT_CONFIRMATION_ATR_MULTIPLE = 1.0
PIVOT_PARAMETERS_ARE_PRE_DECLARED = True
PIVOT_PARAMETER_SEARCH_ALLOWED = False

#: The lag a confirmed pivot inevitably carries, stated so no future design
#: pretends the structure was visible when the extreme printed.
PIVOT_CONFIRMATION_LAG_NOTE = (
    "a confirmed pivot is available at least PIVOT_CONFIRMATION_SESSIONS after "
    "the extreme. Every retracement measured from it therefore starts life "
    "already several sessions old, and any design whose edge disappears under "
    "that lag never had one")

# --------------------------------------------------------------------------- #
# The structural hypotheses
# --------------------------------------------------------------------------- #
def _h(**kw) -> dict:
    return dict(kw)


HYPOTHESES = (
    _h(hypothesis_id="TREND_STRUCTURE",
       statement="a sequence of confirmed higher highs and higher lows "
                 "predicts continuation beyond what a moving-average trend "
                 "signal already captures",
       structure_inputs=("confirmed swing highs", "confirmed swing lows"),
       control="the 12-1 trend signal Release 36 already froze and tested",
       why_it_might_fail="Release 36 measured trend 12-1 at t = 3.89 in FX and "
                         "found no after-cost excess; structure may be a "
                         "re-parameterisation of the same thing",
       leakage_risk="pivot identification",
       cost_sensitivity="LOW - structural signals are slow"),
    _h(hypothesis_id="IMPULSE_RETRACEMENT",
       statement="entering in the direction of a confirmed impulse during a "
                 "retracement beats entering at an arbitrary point in the "
                 "trend",
       structure_inputs=("confirmed impulse leg", "retracement depth"),
       control="the same trend signal entered without regard to retracement",
       why_it_might_fail="the improvement may be entirely a lower average "
                         "entry price, which is a cost effect rather than a "
                         "predictive one",
       leakage_risk="the impulse's end is a pivot",
       cost_sensitivity="MEDIUM - retracement entries trade more often"),
    _h(hypothesis_id="BREAKOUT_AND_RETEST",
       statement="a breakout that is retested and holds continues more "
                 "reliably than one that is not",
       structure_inputs=("confirmed level", "breakout close", "retest close"),
       control="every breakout, retested or not",
       why_it_might_fail="conditioning on the retest holding is a survivorship "
                         "rule applied to signals",
       leakage_risk="'holds' must be defined forward, never in retrospect",
       cost_sensitivity="MEDIUM"),
    _h(hypothesis_id="SUPPORT_RESISTANCE",
       statement="prices react at levels defined by prior confirmed pivots",
       structure_inputs=("confirmed pivot cluster", "touch count"),
       control="levels placed at random prices within the same range",
       why_it_might_fail="a level with many touches is a level price has spent "
                         "time near, which is nearly a tautology",
       leakage_risk="touch counting must use only pivots confirmed before t",
       cost_sensitivity="MEDIUM"),
    _h(hypothesis_id="CHANNEL_GEOMETRY",
       statement="position within a regression or pivot channel predicts the "
                 "next move",
       structure_inputs=("channel slope", "channel width", "position in "
                         "channel"),
       control="a trailing z-score of price, which is the same idea without "
               "the geometry",
       why_it_might_fail="a channel is a trailing regression with extra steps",
       leakage_risk="channel fitted on the full window",
       cost_sensitivity="LOW"),
    _h(hypothesis_id="ATR_NORMALISED_GEOMETRY",
       statement="structural distances expressed in ATR units are comparable "
                 "across instruments and horizons where raw distances are not",
       structure_inputs=("ATR", "structural distance"),
       control="the same structure in raw price units",
       why_it_might_fail="this is a normalisation claim rather than a "
                         "predictive one, and should be tested as such",
       leakage_risk="ATR must be trailing",
       cost_sensitivity="NONE"),
    _h(hypothesis_id="VOLATILITY_CONTRACTION_EXPANSION",
       statement="a contraction in realised range precedes an expansion whose "
                 "direction is predicted by the prevailing structure",
       structure_inputs=("trailing range percentile", "trend state"),
       control="the unconditional expansion, which is well documented and not "
               "an edge",
       why_it_might_fail="volatility clustering is real and free; DIRECTION is "
                         "the claim, and it is the part with no prior support",
       leakage_risk="percentile must be trailing",
       cost_sensitivity="MEDIUM"),
    _h(hypothesis_id="VOLUME_CONFIRMATION",
       statement="a structural break on elevated volume continues more "
                 "reliably than one on ordinary volume",
       structure_inputs=("volume", "open interest", "structural break"),
       control="the same break without a volume condition",
       why_it_might_fail="volume is mechanically higher on large moves",
       leakage_risk="volume must be same-session and the position next-session",
       cost_sensitivity="MEDIUM"),
    _h(hypothesis_id="MULTI_TIMEFRAME_AGREEMENT",
       statement="structure agreeing across daily, weekly and monthly views "
                 "predicts better than any single view",
       structure_inputs=("structure state per timeframe",),
       control="the daily view alone",
       why_it_might_fail="three views of one price series are not three "
                         "independent observations",
       leakage_risk="a weekly bar is only complete at the week's end",
       cost_sensitivity="LOW"),
)

# --------------------------------------------------------------------------- #
# Fibonacci as a testable hypothesis, with its placebo arm
# --------------------------------------------------------------------------- #
FIBONACCI_RETRACEMENT_LEVELS = (0.236, 0.382, 0.500, 0.618, 0.786)
FIBONACCI_EXTENSION_LEVELS = (1.272, 1.618)
FIBONACCI_LEVELS = FIBONACCI_RETRACEMENT_LEVELS + FIBONACCI_EXTENSION_LEVELS

#: Placebo levels sit BETWEEN the canonical ones and are as arbitrary as the
#: canonical ones would be if the hypothesis were false. If the canonical levels
#: do not beat these, the finding is "pullbacks in trends work", not
#: "Fibonacci works" - and those are different claims with different futures.
FIBONACCI_PLACEBO_LEVELS = (0.300, 0.440, 0.560, 0.700, 0.860, 1.400, 1.800)
PLACEBO_ARM_REQUIRED = True
FIBONACCI_IS_DOCTRINE = False

FIBONACCI_DESIGN = {
    "hypothesis": ("price reacts at canonical Fibonacci retracement and "
                   "extension levels of a CONFIRMED impulse leg more than at "
                   "arbitrary levels of the same leg"),
    "levels": list(FIBONACCI_LEVELS),
    "placebo_levels": list(FIBONACCI_PLACEBO_LEVELS),
    "placebo_arm_required": PLACEBO_ARM_REQUIRED,
    "leg_definition": ("from one confirmed pivot to the next confirmed pivot, "
                       "both stamped with their CONFIRMATION dates"),
    "reaction_definition": ("a declared forward return measured from the "
                            "session after the level is touched, so the touch "
                            "itself is observable before the position exists"),
    "primary_statistic": ("the difference in after-cost excess return between "
                          "the canonical arm and the placebo arm, judged with "
                          "the released Release-31 multiple-testing owner over "
                          "ALL levels tested, canonical and placebo together"),
    "denominator_includes_placebo_levels": True,
    "failure_reading": ("if canonical and placebo are indistinguishable, the "
                        "correct conclusion is that retracement entries in "
                        "trends work and the ratios are decoration"),
    "prohibited": (
        "choosing the leg after seeing which level worked",
        "reporting only the level that survived",
        "counting a touch that only became visible after the pivot confirmed",
        "widening the tolerance band until a level hits",
    ),
}

# --------------------------------------------------------------------------- #
# The visual lane
# --------------------------------------------------------------------------- #
VISUAL_PIPELINE = (
    "render a standardised chart from PIT-safe bars only, with a fixed window, "
    "a fixed aspect ratio, no axis labels and no future bars",
    "encode the image with a frozen vision encoder",
    "reduce the embedding with a trailing-fitted projection",
    "combine with the numeric representation in an ensemble whose weights are "
    "fitted on the training partition only",
)

#: The comparison that makes the visual lane worth running at all. Without the
#: first two arms, a chart-image result cannot be attributed to anything.
REPRESENTATION_ARMS = (
    "NUMERIC_OHLCV - the raw sequence, which every prior release has used",
    "ENGINEERED_MARKET_STRUCTURE - the hypotheses above as explicit features",
    "CHART_IMAGE - a rendered picture through a vision encoder",
)

VISUAL_LEAKAGE_RULES = (
    "the rendered window must end at the decision timestamp; a chart drawn to "
    "the right edge of a completed move contains the answer",
    "axis scaling must be computed from the visible window only, because a "
    "y-axis scaled to a future extreme encodes that extreme",
    "a colour scheme, a marker or an annotation derived from a later outcome "
    "is a label, not a feature",
    "an encoder pre-trained on financial charts may have seen the instrument; "
    "a frozen general-purpose encoder is the safer default",
)

VISUAL_EXPERIMENT_IN_SCOPE = False

# --------------------------------------------------------------------------- #
# Preconditions - when this backlog becomes worth executing
# --------------------------------------------------------------------------- #
PRECONDITIONS = (
    "a native instrument universe wide enough that a structural result is not "
    "one market's history: the recommended futures purchase takes the native "
    "count from 5 energy curves to roughly 100 markets",
    "daily settlement, volume and open interest, so VOLUME_CONFIRMATION is "
    "testable rather than aspirational",
    "the released multiple-testing owner applied over canonical AND placebo "
    "arms together, so the placebo arm cannot be quietly dropped",
    "a pre-registered grid, frozen before any result is seen, exactly as "
    "Release 36 froze its 34 configurations",
)


def backlog() -> dict:
    return {
        "executed_in_this_release": EXECUTED_IN_THIS_RELEASE,
        "reads_a_price": READS_A_PRICE,
        "computes_a_feature": COMPUTES_A_FEATURE,
        "judges_a_book": JUDGES_A_BOOK,
        "anti_hindsight": {
            "pivot_confirmation_required": PIVOT_CONFIRMATION_REQUIRED,
            "pivot_timestamp_is_the_confirmation_date":
                PIVOT_TIMESTAMP_IS_THE_CONFIRMATION_DATE,
            "future_known_extrema_allowed": FUTURE_KNOWN_EXTREMA_ALLOWED,
            "rules": list(PIVOT_CONFIRMATION_RULES),
            "confirmation_sessions": PIVOT_CONFIRMATION_SESSIONS,
            "confirmation_atr_multiple": PIVOT_CONFIRMATION_ATR_MULTIPLE,
            "parameters_are_pre_declared": PIVOT_PARAMETERS_ARE_PRE_DECLARED,
            "parameter_search_allowed": PIVOT_PARAMETER_SEARCH_ALLOWED,
            "confirmation_lag_note": PIVOT_CONFIRMATION_LAG_NOTE,
        },
        "hypotheses": [dict(h, structure_inputs=list(h["structure_inputs"]))
                       for h in HYPOTHESES],
        "n_hypotheses": len(HYPOTHESES),
        "fibonacci": dict(FIBONACCI_DESIGN,
                          prohibited=list(FIBONACCI_DESIGN["prohibited"])),
        "fibonacci_is_doctrine": FIBONACCI_IS_DOCTRINE,
        "visual": {
            "pipeline": list(VISUAL_PIPELINE),
            "representation_arms": list(REPRESENTATION_ARMS),
            "leakage_rules": list(VISUAL_LEAKAGE_RULES),
            "experiment_in_scope": VISUAL_EXPERIMENT_IN_SCOPE,
        },
        "preconditions": list(PRECONDITIONS),
    }


def artifact(*, campaign_id: str, created_at: str) -> dict:
    payload = {
        "campaign_id": campaign_id,
        "created_at": created_at,
        "calculation_owner": CALCULATION_OWNER,
        "backlog": backlog(),
        "market_structure_experiment_in_scope":
            C.MARKET_STRUCTURE_EXPERIMENT_IN_SCOPE,
    }
    return r37.artifact_body(SCHEMA, payload)


def path_for(campaign_id: str = C.CAMPAIGN_ID):
    return r37.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict):
    return r37.write_json(path_for(body["campaign_id"]), body)


def load(campaign_id: str = C.CAMPAIGN_ID) -> Optional[dict]:
    return r37.read_json(path_for(campaign_id))


__all__ = ["CALCULATION_OWNER", "EXECUTED_IN_THIS_RELEASE",
           "PIVOT_CONFIRMATION_REQUIRED",
           "PIVOT_TIMESTAMP_IS_THE_CONFIRMATION_DATE",
           "FUTURE_KNOWN_EXTREMA_ALLOWED", "PIVOT_CONFIRMATION_RULES",
           "PIVOT_CONFIRMATION_SESSIONS", "PIVOT_CONFIRMATION_ATR_MULTIPLE",
           "HYPOTHESES", "FIBONACCI_LEVELS", "FIBONACCI_RETRACEMENT_LEVELS",
           "FIBONACCI_EXTENSION_LEVELS", "FIBONACCI_PLACEBO_LEVELS",
           "PLACEBO_ARM_REQUIRED", "FIBONACCI_IS_DOCTRINE", "FIBONACCI_DESIGN",
           "VISUAL_PIPELINE", "REPRESENTATION_ARMS", "VISUAL_LEAKAGE_RULES",
           "VISUAL_EXPERIMENT_IN_SCOPE", "PRECONDITIONS", "backlog",
           "artifact", "freeze", "load", "path_for"]
