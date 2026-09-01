r"""alpha_agent.r53_1.short_capability - what stands between this estate and
SIGNED exposure, owner by owner. ASSESSMENT ONLY - nothing here activates,
enables, or prototypes shorting; production remains long-only.

Why it matters now: two of the five measured non-equity sleeves currently
signal SHORT (VX term-structure carry: short/flat; rates copper-gold lead:
short &ZN), and the R53 competition had to report them as inexpressible
rather than size them. A long-only mandate silently discards half of every
symmetric signal family.

A futures short and an equity short are DIFFERENT INSTRUMENTS and this
assessment never conflates them: a futures short is the same margin posture
as a futures long with a negative quantity (no borrow, no locate, no
recall risk, no hard-to-borrow fee); an equity short needs a borrow ledger,
a locate, a fee accrual, and unbounded-loss handling that the paper desk
has no primitives for.
"""
from __future__ import annotations

from . import (CAMPAIGN_ID, RELEASE, artifact_body, research_dir,
               safety_block, write_json)

CALCULATION_OWNER = "alpha_agent.r53_1.short_capability"
ARTIFACT = "R53_1_SHORT_CAPABILITY_ASSESSMENT.json"

#: Each row is verified against the named owner (importable; the cited
#: behaviours are read from the code, not remembered).
INVENTORY = (
    {"owner": "engine.constrained_reallocation",
     "assumes_long_only": True,
     "evidence": "C_LONG_ONLY constraint: 'Negative weight is clipped to "
                 "zero' (reshape action); default_policy max_net_exposure "
                 "== max_gross_exposure == 1.0 (net==gross is long-only by "
                 "identity)",
     "signed_exposure_needs": "separate gross and net budgets; the clip "
                              "becomes a signed cap (|w_i| <= name cap); "
                              "donor/funding arithmetic must fund from "
                              "reduced longs OR increased shorts "
                              "explicitly"},
    {"owner": "engine.opportunity_frontier / api.investability_registry",
     "assumes_long_only": True,
     "evidence": "candidate rows enter from LONG legs only; a sleeve whose "
                 "book is net short surfaces as SIGNAL_DIRECTION_SHORT_ONLY "
                 "and is excluded upstream of the allocator (measured in "
                 "the R53 competition: &VX, &ZN sleeves)",
     "signed_exposure_needs": "a signed candidate row (direction field), "
                              "and an eligibility rule for short candidates "
                              "distinct from 'score percentile high'"},
    {"owner": "engine.instrument_contract / api.operational_book (position "
              "contract)",
     "assumes_long_only": "PARTIALLY",
     "evidence": "futures descriptors carry multiplier and initial margin "
                 "per unit - quantity COULD carry a sign - but every desk "
                 "ledger row and NAV identity is written and tested with "
                 "non-negative quantity",
     "signed_exposure_needs": "signed-quantity invariants through the whole "
                              "chain: position, marks-to-NAV, realised PnL "
                              "on cover, margin call arithmetic"},
    {"owner": "engine.cross_asset_risk",
     "assumes_long_only": False,
     "evidence": "covariance/portfolio-volatility arithmetic is "
                 "sign-agnostic (a negative weight is just a number); the "
                 "measured &VX correlation of -0.71 becomes a HEDGE only "
                 "with a short position the risk model can already price",
     "signed_exposure_needs": "nothing structural; directional "
                              "concentration limits (net per asset class) "
                              "would be new POLICY, not new math"},
    {"owner": "api.paper_trading_desk (fills / cost model)",
     "assumes_long_only": True,
     "evidence": "cost model is symmetric bps per side, which is CORRECT "
                 "for futures shorts and WRONG for equity shorts (no borrow "
                 "fee, no locate, no recall)",
     "signed_exposure_needs": "futures: none beyond signed quantity. "
                              "equities: a borrow ledger with fee accrual "
                              "and availability states - a NEW subsystem"},
    {"owner": "evidence/approval gates (alpha_agent.r46.contract)",
     "assumes_long_only": False,
     "evidence": "forward evidence rows already carry direction (+1/-1); "
                 "shadow challengers accrue short-leg evidence today - the "
                 "evidence pipeline is ALREADY signed",
     "signed_exposure_needs": "nothing; the gates would evaluate a signed "
                              "sleeve exactly as a long one"},
)

MANDATORY_CONTROLS_BEFORE_ANY_SHORT = (
    "explicit gross exposure budget separate from net (long-only makes them "
    "identical; signed books break that identity)",
    "net exposure budget per asset class AND portfolio-wide (directional "
    "concentration is the new risk axis)",
    "margin/collateral ledger with daily variation margin marks for every "
    "futures position, long or short",
    "stress policy: a short's loss is unbounded in price space - stress "
    "tests must move AGAINST the position, and a stop/de-risk policy must "
    "be declared before the first unit",
    "futures-only first: equity shorting additionally needs borrow cost, "
    "locate availability and recall handling that the paper desk cannot "
    "yet represent honestly",
    "promotion gates unchanged: a short sleeve earns capital through the "
    "same forward-evidence floors as a long one",
)


def assessment() -> dict:
    importable = {}
    for row in INVENTORY:
        mod = row["owner"].split(" ")[0]
        try:
            __import__("paper_trader." + mod.replace("engine.", "engine.")
                       .replace("api.", "api."), fromlist=["_"])
            importable[mod] = True
        except Exception:  # noqa: BLE001
            importable[mod] = False
    return {
        "already_supports_signed_exposure": [
            r["owner"] for r in INVENTORY if r["assumes_long_only"] is False],
        "assumes_long_only": [
            r["owner"] for r in INVENTORY if r["assumes_long_only"] is True],
        "partial": [r["owner"] for r in INVENTORY
                    if r["assumes_long_only"] == "PARTIALLY"],
        "inventory": list(INVENTORY),
        "owners_importable": importable,
        "futures_vs_equity_short": {
            "futures_short": "margin posture identical to a long with "
                             "negative quantity: no borrow, no locate, no "
                             "fee, loss bounded only by price - variation "
                             "margin and stress policy are the controls",
            "equity_short": "requires borrow (fee, locate, recall), a "
                            "borrow ledger the desk does not have, and "
                            "dividend liability handling",
            "never_conflate": True},
        "signals_currently_inexpressible": {
            "sleeve_volatility_futures": "short/flat &VX (term-structure "
                                         "carry) - also the single best "
                                         "measured diversifier "
                                         "(rho=-0.71 to the book)",
            "sleeve_rates_futures": "short &ZN (copper-gold lead)"},
        "recommended_sequence": [
            "1. SHADOW signed books only (already the case for challengers)",
            "2. futures signed-quantity position contract + margin ledger",
            "3. signed candidate rows + gross/net budgets in the allocator",
            "4. equity borrow subsystem LAST, only with evidence it pays"],
        "activation_state": "NOT_ACTIVATED - assessment only",
    }


def write_artifact() -> dict:
    body = artifact_body(
        "r53_1_short_capability_assessment/1", CALCULATION_OWNER,
        release=RELEASE, campaign_id=CAMPAIGN_ID,
        mandatory_controls_before_any_short=list(
            MANDATORY_CONTROLS_BEFORE_ANY_SHORT),
        **assessment(), **safety_block())
    write_json(research_dir() / ARTIFACT, body)
    return body
