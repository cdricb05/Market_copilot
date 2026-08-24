"""alpha_agent.r42.contract - the Release-42 rules, FROZEN BEFORE RESULTS.

Every rule that could be bent after seeing an outcome lives here and is
hashed into ``r42_frozen_contract.json`` before any R42 lab runs:

* what the R41 candidate is and that it is IMMUTABLE;
* the complete economic PnL equation an implementable claim must satisfy;
* the CONSERVATIVE PRIMARY capital model and every reported denominator;
* the execution-cost ladder and the maker-fill admissibility rule;
* the borrow-evidence rule that decides whether the reverse leg counts;
* the DATA-ONLY asset-eligibility rule (Track I) and venue-eligibility
  rule (Tracks H/J) - both frozen before any strategy outcome is computed;
* the HIERARCHICAL multiple-testing architecture (Track L), chosen for
  the structure of the problem and NOT because it makes BTC pass;
* the R42 qualification-state vocabulary;
* the $0 / no-account / no-order safety boundary.

R41's own gates are NOT modified here. ``alpha_agent.r41.contract`` remains
the authority for the R41 verdict, and ``HISTORICAL_ALPHA_RESULT = FAIL``
stands exactly as R41 recorded it.
"""
from __future__ import annotations

from .. import r39 as _r39
from ..r41 import contract as _c41

CALCULATION_OWNER = "alpha_agent.r42.contract"

# --------------------------------------------------------------------------- #
# 0. What R41 handed over, and what may never be touched
# --------------------------------------------------------------------------- #
R41_EXPECTED = {
    "release": "release41",
    "campaign_id": "r41_multi_horizon_alpha_breakthrough_v1",
    "branch": "stage19-controlled-rebalance",
    "shadow_id": "shadow_btc_funding_carry_1d",
    "shadow_spec_hash":
        "4976215a2994ea69fafdb7f486a6fee3a7cd37fa738ba3d5a255588e89c2a62e",
    "registry_hash":
        "f5f8b2c7cede63170a30e04e284e8e2d8dbacbeb786f06a1174685db87c1f0d1",
    "global_cumulative_burden": 289,
    "global_inherited_burden": 230,
    "crypto_family_burden": 10,
    "historical_alpha_result": "FAIL",
    "zone_b_t": 10.17551468086003,
    "zone_b_excess_ann": 0.08754154652851544,
    "zone_c_t": 6.905945198485986,
    "zone_c_excess_ann": 0.03150725728982265,
    "zone_c_sharpe": 7.8222160059138135,
    "zone_c_x3_t": 3.032346267946327,
    "eth_zone_b_t": 9.468285727405593,
    "eth_zone_c_t": 4.5013033539812755,
    "dsr_family": 0.0037613087434269672,
    "n_killer_sign_flips": 0,
    "placebo_gate_t": 4.450945732518154,
}

#: The R41 shadow and every R41 artifact are EVIDENCE. R42 may read them,
#: hash them and reason about them. R42 may not rewrite them.
R41_IMMUTABLE_FIELDS = (
    "shadow_id", "rule", "symbol", "venue", "z_window_mean", "z_window_std",
    "z_threshold", "decision_cadence", "cost_model", "spec_hash",
    "frozen_at", "selection_evidence", "information_family",
    "economic_expression",
)
R41_CANDIDATE_IS_IMMUTABLE = True
R42_CORRECTIONS_GET_NEW_IDENTITIES = True

# --------------------------------------------------------------------------- #
# 1. The complete economic PnL equation (Track A)
# --------------------------------------------------------------------------- #
#: An implementable claim must account for EVERY term. A term that is not
#: measured must be declared OMITTED and its sign stated; it may never be
#: silently assumed to be zero.
PNL_TERMS = (
    "SPOT_PNL",              # mark-to-market of the spot leg
    "PERP_PNL",              # mark-to-market of the perpetual leg
    "FUNDING_CASHFLOW",      # realised funding payments, event-exact
    "SPOT_FEES",             # exchange fees on the spot leg
    "PERP_FEES",             # exchange fees on the perpetual leg
    "SPREAD_SLIPPAGE",       # half-spread + size impact on both legs
    "FINANCING",             # opportunity cost of committed capital
    "BORROW",                # spot borrow fee when the leg is short
    "COLLATERAL_DRAG",       # non-interest-bearing collateral / haircut
)
PNL_IDENTITY = ("NET = SPOT_PNL + PERP_PNL + FUNDING_CASHFLOW - SPOT_FEES "
                "- PERP_FEES - SPREAD_SLIPPAGE - FINANCING - BORROW "
                "- COLLATERAL_DRAG")
PNL_RECONCILIATION_TOLERANCE = 1e-12

# --------------------------------------------------------------------------- #
# 2. Capital (Track E) - the denominator is part of the claim
# --------------------------------------------------------------------------- #
#: A spot/perp cash-and-carry book is NOT self-financing. R41 scored the
#: stream against a ZERO control on ONE leg's notional. R42's PRIMARY
#: economics are return on CONSERVATIVE COMMITTED CAPITAL, in EXCESS of the
#: risk-free rate that capital would otherwise have earned.
CAPITAL_MODELS = {
    "TRADED_NOTIONAL": {
        "denominator": 1.00,
        "note": "R41's implicit denominator - one leg's notional. Reported "
                "for comparability ONLY; never the primary claim.",
    },
    "GROSS_EXPOSURE": {
        "denominator": 2.00,
        "note": "spot notional + perp notional.",
    },
    "FULLY_FUNDED_COMMITTED": {
        "denominator": 1.20,
        "note": "100% cash for the spot leg + 20% initial margin posted "
                "against the perpetual leg. No variation buffer.",
    },
    "CONSERVATIVE_COLLATERAL": {
        "denominator": 1.35,
        "note": "PRIMARY. 100% cash for the spot leg + 20% initial margin "
                "+ 15% variation-margin / liquidation buffer against a "
                "basis and mark-price shock. No cross-margin credit for "
                "the spot leg is assumed, because no venue admissible to "
                "the operator has been demonstrated to grant it.",
    },
}
PRIMARY_CAPITAL_MODEL = "CONSERVATIVE_COLLATERAL"

#: The control for a FULLY FUNDED book is the risk-free rate on the whole
#: committed capital: cash placed at an exchange (spot inventory + USDT
#: collateral) earns nothing, so the entire opportunity cost is forgone.
PRIMARY_CONTROL = "RISK_FREE_ON_COMMITTED_CAPITAL"
RISK_FREE_SERIES_PREFERENCE = ("SOFR", "EFFR", "CMT_3M")
RISK_FREE_SOURCE = "FRED daily panel acquired by R41 (read-only)"
CONTROL_RATIONALE = (
    "R41's DELTA_NEUTRAL_BASIS stream was judged against a ZERO control, "
    "the convention r41.contract.CONTROLS reserves for RV_SELF_FINANCED "
    "books. A cash-and-carry is the opposite of self-financing: it "
    "immobilises 100% of the spot notional plus margin in non-interest-"
    "bearing form. Scoring it against zero credits the strategy with the "
    "entire risk-free rate it actually forgoes.")

# --------------------------------------------------------------------------- #
# 3. Execution (Track F)
# --------------------------------------------------------------------------- #
#: bps per side per leg. R41 charged a flat 5.0 taker bps per side per leg
#: and nothing else.
EXECUTION_MODELS = {
    "R41_BASELINE": {"spot_bps": 5.0, "perp_bps": 5.0, "spread_bps": 0.0,
                     "note": "exactly what R41 charged - reproduced, not "
                             "endorsed"},
    "DEFAULT_TAKER": {"spot_bps": 10.0, "perp_bps": 5.0, "spread_bps": 1.0,
                      "note": "published VIP0 taker: spot 10 bps, USD-M "
                              "perp 5 bps, plus 1 bp observed half-spread "
                              "per leg"},
    "REALISTIC_MIXED": {"spot_bps": 7.5, "perp_bps": 3.5, "spread_bps": 1.0,
                        "note": "half maker / half taker with the maker "
                                "leg discounted only where a fill model "
                                "exists"},
    "MAKER_OPTIMISTIC": {"spot_bps": 5.0, "perp_bps": 2.0, "spread_bps": 0.0,
                         "note": "ADMISSIBLE ONLY with a fill-probability "
                                 "and adverse-selection model; otherwise "
                                 "reported as an upper bound, never as the "
                                 "claim"},
}
PRIMARY_EXECUTION_MODEL = "DEFAULT_TAKER"
EXECUTION_STRESS_MULTIPLIERS = (1.0, 2.0, 3.0)
MAKER_FILL_REQUIRES = ("queue_position_or_fill_probability",
                       "maker_fee_or_rebate", "adverse_selection",
                       "latency")
ASSUMED_LIMIT_FILL_IS_FORBIDDEN = True

# --------------------------------------------------------------------------- #
# 4. Borrow / reverse leg (Track D)
# --------------------------------------------------------------------------- #
#: The R41 rule holds a NEGATIVE position (long perp / SHORT spot) when the
#: funding z-score is below -0.5. That requires borrowing spot.
BORROW_EVIDENCE_REQUIRED = ("historical_borrow_availability",
                            "historical_borrow_rate", "recall_risk",
                            "short_sale_mechanics", "borrow_capacity")
#: If historical borrow cannot be PROVEN from evidence dated inside the
#: sample, the reverse leg is HISTORICALLY_NON_IMPLEMENTABLE and its PnL
#: may not be counted toward an implementable Alpha claim.
BORROW_UNPROVEN_VERDICT = "HISTORICALLY_NON_IMPLEMENTABLE"
CURRENT_SNAPSHOT_IS_NOT_HISTORY = True

# --------------------------------------------------------------------------- #
# 5. Asset universe (Track I) - DATA-ONLY, frozen before any outcome
# --------------------------------------------------------------------------- #
#: Eligibility is decided ENTIRELY by metadata. No strategy return, Sharpe,
#: t-statistic or PnL of any candidate asset may influence membership.
#: Delisted symbols are INCLUDED wherever the archive preserves history.
ASSET_ELIGIBILITY = {
    "quote_currency": "USDT",
    "requires_spot_klines": True,
    "requires_perp_funding_history": True,
    "requires_perp_klines": True,
    "min_joint_history_days": 1095,          # three years overlapping
    "min_funding_events": 3285,              # 3 per day x 1095 days
    "min_median_daily_quote_volume_usd": 5.0e6,
    "exclude_stablecoin_bases": ("USDC", "BUSD", "TUSD", "FDUSD", "DAI",
                                 "USDP", "EURI", "AEUR", "PAXG", "XUSD",
                                 "USD1", "USDE", "SUSDE"),
    "exclude_leveraged_tokens": ("UP", "DOWN", "BULL", "BEAR"),
    "exclude_synthetic_duplicates": ("1000", "1000000", "1MBABY"),
    "symbol_identity_must_be_verified": True,
    "include_delisted_if_history_exists": True,
    "selection_may_use_performance": False,
}
ASSET_UNIVERSE_FROZEN_BEFORE_RESULTS = True
ETH_IS_PRIOR_EVIDENCE = True
NEW_ASSET_LABEL = "HISTORICAL_OUT_OF_ASSET_REPLICATION"
NEW_ASSET_IS_NOT_TRUE_FORWARD = True

# --------------------------------------------------------------------------- #
# 6. Venue universe (Tracks H / J) - DATA-ONLY, frozen before any outcome
# --------------------------------------------------------------------------- #
VENUE_ELIGIBILITY = {
    "requires_public_funding_history": True,
    "requires_public_price_history": True,
    "requires_no_account": True,
    "requires_no_payment": True,
    "requires_no_licence_acceptance": True,
    "min_funding_history_days": 365,
    "selection_may_use_performance": False,
}
VENUE_CANDIDATES = ("BINANCE", "BYBIT", "OKX", "DERIBIT", "BITMEX",
                    "COINBASE_INTX", "HYPERLIQUID", "KRAKEN_FUTURES")
#: Research MAY use a venue's public data. INVESTABILITY requires a
#: separately demonstrated, legally admissible account path for the
#: operator. The two are never conflated.
DATA_ACCESS_IS_NOT_INVESTABILITY = True
INVESTABILITY_REQUIRES_ADMISSIBLE_VENUE_PATH = True

# --------------------------------------------------------------------------- #
# 7. Regulated-market replication (Track K)
# --------------------------------------------------------------------------- #
CME_REPLICATION = {
    "source": "Norgate (owned entitlement, local NDU)",
    "contracts": ("BTC", "ETH", "MBT"),
    "expression": "SPOT_VS_DATED_FUTURES_BASIS",
    "roll_policy": "R38 owned roll policy; exact dated contracts only",
    "spot_reference": "Binance USDT spot at the CME settlement minute",
    "question": "does crypto derivative demand create a broader basis/carry "
                "premium OUTSIDE Binance perpetuals?",
    "is_not_a_perp_funding_clone": True,
}

# --------------------------------------------------------------------------- #
# 8. Hierarchical multiple testing (Track L) - FROZEN BEFORE NEW RESULTS
# --------------------------------------------------------------------------- #
#: R41's family-level deflated Sharpe computed trial variance over a family
#: whose members are largely CADENCE VARIANTS OF ONE lineage; their shared
#: true effect inflates the expected-max null and the check fails. That is
#: a real property of that gate and R41's verdict STANDS UNCHANGED. R42
#: does not repair R41; it declares, in advance, a testing architecture
#: matched to the dependence structure, and reports its result SEPARATELY.
HIERARCHY_LEVELS = {
    "LEVEL_1_LINEAGE": {
        "unit": "one economic hypothesis (crypto perpetual funding/basis "
                "premium)",
        "method": "single predeclared representative implementation, tested "
                  "two-sided at alpha 0.05 with HAC inference",
        "representative": "R42_POSITIVE_ONLY_CASH_AND_CARRY_BTC",
        "rationale": "the implementability-first baseline is declared the "
                     "lineage representative BEFORE any R42 outcome is "
                     "computed, so the level-1 test cannot be chosen to "
                     "favour whichever variant wins",
    },
    "LEVEL_2_IMPLEMENTATION": {
        "unit": "implementation variants within the lineage (cadence, "
                "threshold, leg policy, venue implementation)",
        "method": "Westfall-Young max-statistic bootstrap preserving the "
                  "cross-variant correlation, stationary block bootstrap "
                  "on dates",
        "block_length_days": 21,
        "n_bootstrap": 5000,
        "fwer_alpha": 0.05,
    },
    "LEVEL_3_REPLICATION": {
        "unit": "asset and venue replications of the frozen rule",
        "method": "DerSimonian-Laird random-effects meta-analysis with "
                  "leave-one-out, heterogeneity I^2 and concentration",
        "confirmation_only": True,
    },
}
CLOSED_TESTING = {
    "rule": "LEVEL_1 must reject before any LEVEL_2 claim is admissible; "
            "LEVEL_2 must reject before LEVEL_3 is read as confirmation. A "
            "level that fails STOPS the chain.",
    "alpha": 0.05,
}
#: Deflated Sharpe at the EFFECTIVE number of independent lineages, not the
#: raw trial count. n_eff is the count of distinct economic lineages in the
#: global burden ledger, which is a property of the ledger, not of BTC.
DEFLATED_SHARPE_AT_EFFECTIVE_LINEAGES = True
EFFECTIVE_LINEAGE_DEFINITION = ("distinct (information_family, "
                                "economic_expression) pairs in the global "
                                "search-burden ledger")
R41_DSR_REPORTED_UNCHANGED = True
METHOD_FROZEN_BEFORE_RESULTS = True
METHOD_MAY_NOT_BE_CHOSEN_TO_PASS = True

# --------------------------------------------------------------------------- #
# 9. Attribution (Track M)
# --------------------------------------------------------------------------- #
ATTRIBUTION_COMPONENTS = ("UNCONDITIONAL_FUNDING_CARRY",
                          "INCREMENTAL_Z_GATE_TIMING")
#: Declared BEFORE evaluation: the implementability-first baseline.
POSITIVE_ONLY_BASELINE = {
    "candidate_id_prefix": "R42_POSITIVE_ONLY_CASH_AND_CARRY",
    "rule": "hold LONG SPOT / SHORT PERP whenever the trailing 30-day mean "
            "funding observed through t-1 is strictly positive; otherwise "
            "hold CASH. Never short spot. Daily UTC decisions.",
    "declared_before_evaluation": True,
    "why": "it is the only leg the estate can prove implementable, and a "
           "simpler strategy may be economically superior even where a "
           "timing overlay is statistically stronger",
}
R41_THRESHOLD_MAY_NOT_BE_RETUNED = True

# --------------------------------------------------------------------------- #
# 10. Margin / liquidation (Track G) and collateral (Track O)
# --------------------------------------------------------------------------- #
MARGIN_STRESS = {
    "price_shocks": (-0.20, -0.10, 0.10, 0.20),
    "basis_widening": (0.01, 0.02, 0.05),
    "spread_multiplier": 3.0,
    "one_leg_fill_delay_days": 1,
    "venue_outage": ("PERP_EXCHANGE_OUTAGE", "SPOT_EXCHANGE_OUTAGE"),
    "maintenance_margin_rate": 0.005,
    "primary_test_requires_no_leverage": True,
}
COLLATERAL_STRESS = {
    "stablecoin_depeg": (0.005, 0.01, 0.05),
    "exchange_haircut": (0.05, 0.10),
    "withdrawal_freeze_days": (7, 30),
    "counterparty_impairment": (0.10, 1.00),
}

# --------------------------------------------------------------------------- #
# 11. Capacity (Track N)
# --------------------------------------------------------------------------- #
CAPACITY_LEVELS_USD = (1.0e4, 1.0e5, 1.0e6, 1.0e7)
CAPACITY_INPUTS = ("spot_quote_volume", "perp_quote_volume",
                   "open_interest", "observed_spread", "slippage_proxy")
FUNDING_IS_NOT_EXOGENOUS_AT_SCALE = True

# --------------------------------------------------------------------------- #
# 12. R42 qualification states
# --------------------------------------------------------------------------- #
QUALIFICATION_STATES = (
    "R42_CRYPTO_BASIS_ALPHA_VALIDATED_HISTORICALLY",
    "R42_STRONG_REPLICATED_CANDIDATE_FORWARD_PENDING",
    "R42_SINGLE_VENUE_PREMIUM_ONLY",
    "R42_EXECUTION_REALITY_KILLS_EDGE",
    "R42_CAPITAL_EFFICIENCY_KILLS_EDGE",
    "R42_BORROW_REALITY_KILLS_REVERSE_LEG",
    "R42_CROSS_ASSET_REPLICATION_FAILS",
    "R42_CROSS_VENUE_REPLICATION_FAILS",
    "R42_STRUCTURAL_PREMIUM_CONFIRMED_NOT_TIMING_ALPHA",
    "R42_FORWARD_EVIDENCE_STRENGTHENED",
    "R42_FORWARD_EVIDENCE_WEAKENED",
    "R42_DATA_LIMIT_BINDING",
)
MULTIPLE_STATES_MAY_HOLD = True

#: What a strong result would have to show. Declared in advance; standards
#: may not be lowered after the data is seen.
BELIEF_STANDARD = (
    "exact cashflow reconciliation",
    "positive after FULL realistic costs",
    "positive return on conservative committed capital",
    "reverse leg proven borrowable or excluded",
    "BTC survives",
    "ETH remains positive",
    "broad new-asset replication",
    "at least one independent venue or regulated-market analogue",
    "limited venue/asset concentration",
    "positive at severe cost stress",
    "no liquidation dependence",
    "hierarchical search-adjusted evidence",
    "forward BTC evidence consistent with the hypothesis",
)
STANDARDS_MAY_NOT_BE_LOWERED_AFTER_DATA = True

# --------------------------------------------------------------------------- #
# 13. Safety / commercial boundary
# --------------------------------------------------------------------------- #
FORBIDDEN_ACTIONS = (
    "crypto purchase", "exchange account", "deposit", "withdrawal",
    "API trading key", "order", "paper order", "portfolio mutation",
    "live execution", "cloud spend", "paid provider trial", "subscription",
    "automatic model promotion", "R41 shadow mutation",
    "backdated forward row",
)
MONEY_BUDGET_USD = 0.0
FREE_SAMPLE_CONDITIONS = _c41.FREE_SAMPLE_CONDITIONS \
    if hasattr(_c41, "FREE_SAMPLE_CONDITIONS") else (
        "NO_ACCOUNT", "NO_PAYMENT", "NO_LICENCE_ACCEPTANCE",
        "PUBLIC_ENDPOINT", "PROVENANCE_URL_TIME_HASH_RECORDED")
BLOCKER_VOCAB = tuple(_c41.BLOCKER_VOCAB) + (
    "VENUE_GEO_RESTRICTED", "BORROW_HISTORY_UNAVAILABLE",
    "FUNDING_HISTORY_TOO_SHALLOW", "EXECUTION_MICROSTRUCTURE_DATA",
)
SHELL_POLICY = "WINDOWS_POWERSHELL_ONLY"
CLAUDE_MAY_COMMIT = False
CLAUDE_MAY_PUSH = False

#: Parallel data frontier - preserved, NOT acted on in R42.
QUEUED_PURCHASE_LANE = ("ORATS historical options",
                        "Alpha Vantage one-month options pilot",
                        "Databento intraday futures",
                        "Steele/Intrinio analyst revisions")
NO_OPTIONS_PURCHASE_IN_R42 = True


def freeze_artifact() -> dict:
    """Write ``r42_frozen_contract.json`` and return it.

    This MUST be called before any R42 lab computes an outcome. The
    artifact carries the contract hash, so a later edit to any rule above
    is detectable: the frozen artifact and the live hash diverge, and the
    audit fails the build.
    """
    import datetime as _dt

    from . import CAMPAIGN_ID, artifact_body, read_json, write_artifact
    from . import campaign_dir as _cd

    path = _cd(CAMPAIGN_ID) / "r42_frozen_contract.json"
    existing = read_json(path) if path.exists() else None
    if existing:
        return existing
    body = artifact_body("r42_frozen_contract/1", {
        "calculation_owner": CALCULATION_OWNER,
        "frozen_at": _dt.datetime.now(_dt.timezone.utc)
        .strftime("%Y-%m-%dT%H:%M:%SZ"),
        "contract_hash": contract_hash(),
        "method_frozen_before_results": METHOD_FROZEN_BEFORE_RESULTS,
        "asset_universe_frozen_before_results":
            ASSET_UNIVERSE_FROZEN_BEFORE_RESULTS,
        "standards_may_not_be_lowered_after_data":
            STANDARDS_MAY_NOT_BE_LOWERED_AFTER_DATA,
        "r41_candidate_is_immutable": R41_CANDIDATE_IS_IMMUTABLE,
        "rules": {
            "PNL_TERMS": list(PNL_TERMS), "PNL_IDENTITY": PNL_IDENTITY,
            "CAPITAL_MODELS": CAPITAL_MODELS,
            "PRIMARY_CAPITAL_MODEL": PRIMARY_CAPITAL_MODEL,
            "PRIMARY_CONTROL": PRIMARY_CONTROL,
            "CONTROL_RATIONALE": CONTROL_RATIONALE,
            "EXECUTION_MODELS": EXECUTION_MODELS,
            "PRIMARY_EXECUTION_MODEL": PRIMARY_EXECUTION_MODEL,
            "EXECUTION_STRESS_MULTIPLIERS":
                list(EXECUTION_STRESS_MULTIPLIERS),
            "BORROW_EVIDENCE_REQUIRED": list(BORROW_EVIDENCE_REQUIRED),
            "BORROW_UNPROVEN_VERDICT": BORROW_UNPROVEN_VERDICT,
            "ASSET_ELIGIBILITY": ASSET_ELIGIBILITY,
            "VENUE_ELIGIBILITY": VENUE_ELIGIBILITY,
            "VENUE_CANDIDATES": list(VENUE_CANDIDATES),
            "CME_REPLICATION": CME_REPLICATION,
            "HIERARCHY_LEVELS": HIERARCHY_LEVELS,
            "CLOSED_TESTING": CLOSED_TESTING,
            "EFFECTIVE_LINEAGE_DEFINITION": EFFECTIVE_LINEAGE_DEFINITION,
            "POSITIVE_ONLY_BASELINE": POSITIVE_ONLY_BASELINE,
            "MARGIN_STRESS": MARGIN_STRESS,
            "COLLATERAL_STRESS": COLLATERAL_STRESS,
            "CAPACITY_LEVELS_USD": list(CAPACITY_LEVELS_USD),
            "QUALIFICATION_STATES": list(QUALIFICATION_STATES),
            "BELIEF_STANDARD": list(BELIEF_STANDARD),
            "FORBIDDEN_ACTIONS": list(FORBIDDEN_ACTIONS),
            "MONEY_BUDGET_USD": MONEY_BUDGET_USD,
            "SHELL_POLICY": SHELL_POLICY,
        },
    })
    write_artifact("r42_frozen_contract.json", body, CAMPAIGN_ID)
    return body


def contract_hash() -> str:
    """Hash of every frozen rule above."""
    return _r39.sha({
        "R41_EXPECTED": R41_EXPECTED,
        "R41_IMMUTABLE_FIELDS": R41_IMMUTABLE_FIELDS,
        "PNL_TERMS": PNL_TERMS, "PNL_IDENTITY": PNL_IDENTITY,
        "CAPITAL_MODELS": CAPITAL_MODELS,
        "PRIMARY_CAPITAL_MODEL": PRIMARY_CAPITAL_MODEL,
        "PRIMARY_CONTROL": PRIMARY_CONTROL,
        "RISK_FREE_SERIES_PREFERENCE": RISK_FREE_SERIES_PREFERENCE,
        "EXECUTION_MODELS": EXECUTION_MODELS,
        "PRIMARY_EXECUTION_MODEL": PRIMARY_EXECUTION_MODEL,
        "EXECUTION_STRESS_MULTIPLIERS": EXECUTION_STRESS_MULTIPLIERS,
        "MAKER_FILL_REQUIRES": MAKER_FILL_REQUIRES,
        "BORROW_EVIDENCE_REQUIRED": BORROW_EVIDENCE_REQUIRED,
        "BORROW_UNPROVEN_VERDICT": BORROW_UNPROVEN_VERDICT,
        "ASSET_ELIGIBILITY": ASSET_ELIGIBILITY,
        "VENUE_ELIGIBILITY": VENUE_ELIGIBILITY,
        "VENUE_CANDIDATES": VENUE_CANDIDATES,
        "CME_REPLICATION": CME_REPLICATION,
        "HIERARCHY_LEVELS": HIERARCHY_LEVELS,
        "CLOSED_TESTING": CLOSED_TESTING,
        "EFFECTIVE_LINEAGE_DEFINITION": EFFECTIVE_LINEAGE_DEFINITION,
        "ATTRIBUTION_COMPONENTS": ATTRIBUTION_COMPONENTS,
        "POSITIVE_ONLY_BASELINE": POSITIVE_ONLY_BASELINE,
        "MARGIN_STRESS": MARGIN_STRESS,
        "COLLATERAL_STRESS": COLLATERAL_STRESS,
        "CAPACITY_LEVELS_USD": CAPACITY_LEVELS_USD,
        "QUALIFICATION_STATES": QUALIFICATION_STATES,
        "BELIEF_STANDARD": BELIEF_STANDARD,
        "FORBIDDEN_ACTIONS": FORBIDDEN_ACTIONS,
        "MONEY_BUDGET_USD": MONEY_BUDGET_USD,
        "BLOCKER_VOCAB": BLOCKER_VOCAB,
        "SHELL_POLICY": SHELL_POLICY,
    })
