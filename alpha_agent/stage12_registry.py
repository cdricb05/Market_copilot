"""Stage 12 Workstream F -- pre-registered, frozen hypothesis registry.

A small number of *economically distinct* hypotheses, pre-registered and frozen
BEFORE any confirmatory test, is the honest antidote to the Stage 11 failure the
autopsy diagnosed: a 300-member family whose ~8 effective independent tests were
crushed by the multiple-testing correction. The remedy is a small honest family,
NOT hundreds of near-duplicate variations and NEVER a relaxed gate.

This module *defines* that family as data, computes a content-addressed
``registry_version``, and enforces immutability (first-write-wins; re-freezing
different content raises). Confirmatory members are canonical anomalies with a
prior independent of this dataset (cited); exploratory members are genuinely new
event-driven / residual-price signals Stage 11 never tested. Each hypothesis
carries the full pre-registration contract the directive requires.

The registry is the SINGLE source of truth for what the Stage 12 tournament may
test. The frozen JSON artifact lives at ``configs/alpha_agent/stage12_hypotheses.json``;
a test asserts the on-disk artifact matches this module's canonical content, so
the pre-registration cannot silently drift.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

REGISTRY_SCHEMA_VERSION = "stage12.hypotheses.v2"

CONFIRMATORY = "confirmatory"
EXPLORATORY = "exploratory"

# The evaluation contract changed materially in the evidence-completion pass
# (event-driven signals now measured in TRUE event time -- monthly cohorts of
# asynchronous SEC filings, entry the session after the filed date, execution lag
# applied, overlap-aware clustered-t robustness). Per the directive we mint a NEW
# explicit registry version that includes this contract in its content hash, and
# freeze it to a NEW artifact; the prior v1 registry
# (configs/alpha_agent/stage12_hypotheses.json) is RETAINED unchanged (never
# silently rewritten). The economic hypotheses themselves are unchanged.
EVALUATION_CONTRACT = {
    "contract_version": "stage12-event-time-v2",
    "event_families_measured_in": "event_time_monthly_cohort",
    "event_entry": "first trading session STRICTLY AFTER the SEC filed date "
                   "(>=1 full trading-session lag; companyfacts is day-precision)",
    "residual_entry": "formation_index + execution_lag (>=1); never the formation close",
    "forward_horizons_days": [5, 20, 63],
    "primary_cross_sectional_unit": "monthly event cohort "
                                    "(weekly only when coverage supports)",
    "overlap_adjustment": "Newey-West effective-N + clustered-t; an event candidate "
                          "must ALSO clear the rank_ic_t gate on the autocorrelation-"
                          "robust clustered t (STRICTER, never a relaxation)",
    "clustering": "issuer/date (one observation per issuer per cohort)",
    "purge_embargo": "purged walk-forward folds with 1-period embargo/purge",
    "bootstrap": "moving-block bootstrap CI on the cohort IC series",
    "restatement_isolation": "values read as_of=filed; later amendments excluded by "
                             "the availability boundary (no future restatement leakage)",
    "holdout": "final 30% of measured periods (cohorts), sign-confirm only, once-usable",
    "execution_lag_applied": True,
    "gates_unchanged": True,
    "supersedes": "stage12.hypotheses.v1 (calendar-cross-section event builder), "
                  "frozen at configs/alpha_agent/stage12_hypotheses.json -- RETAINED, "
                  "never rewritten",
}

# Gates every hypothesis must clear (the UNCHANGED tournament battery, by name).
_DEFAULT_GATES = (
    "point_in_time_valid", "survivorship_safe", "min_scored_periods>=12",
    "min_universe_names>=20", "rank_ic_t>=2.0", "spread_t>=2.0",
    "positive_ic_hit_rate>=0.52", "net25_spread>0", "subperiod_consistency>=0.60",
    "regime_consistency>=0.50", "max_drawdown>=-35pct", "turnover<=2.0",
    "family_aware_multiple_testing_bh_fdr", "deflated_sharpe",
    "holdout_sign_confirms", "parameter_neighborhood_sign_stable",
)

_DEFAULT_DESIGN = {
    "train": "in-sample rebalance dates (first 70%)",
    "validation": "purged walk-forward folds with 1-period embargo/purge",
    "holdout": "final 30% of rebalance dates, sign-confirm only, once-usable ledger",
}


def _h(hid: str, family: str, status: str, rationale: str, formula: str,
       builder_key: str, direction: int, timing: str, data_requirement: List[str],
       *, horizon_days: int = 63, neutralization: str = "sector",
       universe: str = "owned survivorship-safe S&P500 current+past (identity-resolved, price-covered)",
       cost_bps: int = 25, turnover_expectation: str = "low", primary_metric: str = "rank_ic_t",
       citation: Optional[str] = None, caveat: Optional[str] = None) -> Dict[str, Any]:
    return {
        "id": hid,
        "economic_family": family,
        "status": status,
        "economic_rationale": rationale,
        "formula": formula,
        "builder_key": builder_key,
        "expected_direction": direction,
        "event_or_rebalance_timing": timing,
        "universe": universe,
        "data_requirement": data_requirement,
        "holding_horizon_days": horizon_days,
        "neutralization": neutralization,
        "cost_assumption_bps": cost_bps,
        "turnover_expectation": turnover_expectation,
        "primary_metric": primary_metric,
        "disqualifying_gates": list(_DEFAULT_GATES),
        "design": dict(_DEFAULT_DESIGN),
        "confirmatory_or_exploratory": status,
        "citation": citation,
        "caveat": caveat,
    }


def _hypotheses() -> List[Dict[str, Any]]:
    H: List[Dict[str, Any]] = []

    # --- Family 1: event-driven fundamental change (exploratory, PIT filed) ---
    H.append(_h("evt_revenue_acceleration", "fundamental_change_event", EXPLORATORY,
                "Under-reaction to accelerating revenue growth; the market prices "
                "the change in growth slowly across quarters.",
                "sign(dRevenueGrowth_yoy) ranked cross-sectionally at filing date",
                "event_revenue_acceleration", +1, "SEC filing (filed date)",
                ["revenue"]))
    H.append(_h("evt_gross_margin_change", "fundamental_change_event", EXPLORATORY,
                "Gross-margin expansion signals durable competitive improvement "
                "under-priced at the filing.",
                "d(gross_profit/revenue) at filing date, cross-sectional rank",
                "event_gross_margin_change", +1, "SEC filing (filed date)",
                ["gross_profit", "revenue", "cost_of_revenue"]))
    H.append(_h("evt_operating_margin_change", "fundamental_change_event", EXPLORATORY,
                "Operating-margin inflection reflects operating leverage the market "
                "adjusts to with delay.",
                "d(operating_income/revenue) at filing date, cross-sectional rank",
                "event_operating_margin_change", +1, "SEC filing (filed date)",
                ["operating_income", "revenue"]))
    H.append(_h("evt_cash_flow_improvement", "fundamental_change_event", EXPLORATORY,
                "Improving cash position (change in cash) is a quality signal "
                "under-weighted relative to accrual earnings.",
                "d(cash) scaled by assets at filing date, cross-sectional rank",
                "event_cash_flow_improvement", +1, "SEC filing (filed date)",
                ["cash", "assets"]))
    H.append(_h("evt_accrual_quality_change", "fundamental_change_event", EXPLORATORY,
                "Rising accruals (net_income far above cash) predicts lower future "
                "returns (Sloan accrual anomaly), measured as a change.",
                "-d(net_income - dCash)/assets at filing date, cross-sectional rank",
                "event_accrual_quality_change", -1, "SEC filing (filed date)",
                ["net_income", "cash", "assets"],
                citation="Sloan (1996) accrual anomaly"))
    H.append(_h("evt_asset_growth_inflection", "fundamental_change_event", EXPLORATORY,
                "High asset growth predicts low returns (investment/asset-growth "
                "anomaly); the inflection is the signal.",
                "-d(asset_growth_yoy) at filing date, cross-sectional rank",
                "event_asset_growth_inflection", -1, "SEC filing (filed date)",
                ["assets"], citation="Cooper, Gulen & Schill (2008)"))
    H.append(_h("evt_leverage_reduction", "fundamental_change_event", EXPLORATORY,
                "De-leveraging (falling liabilities/assets) reduces distress risk "
                "and is rewarded with delay.",
                "-d(liabilities/assets) at filing date, cross-sectional rank",
                "event_leverage_reduction", +1, "SEC filing (filed date)",
                ["liabilities", "assets"]))
    H.append(_h("evt_profitability_inflection", "fundamental_change_event", EXPLORATORY,
                "Turn to profitability (rising net_income/assets) is a quality "
                "improvement under-priced at the filing.",
                "d(net_income/assets) at filing date, cross-sectional rank",
                "event_profitability_inflection", +1, "SEC filing (filed date)",
                ["net_income", "assets"]))

    # --- Family 2: post-filing price reaction (exploratory) -------------------
    H.append(_h("evt_post_filing_drift", "post_filing_price_reaction", EXPLORATORY,
                "Post-earnings/filing announcement drift: abnormal return around "
                "the filing continues for weeks.",
                "market-residual return over [filing, filing+20d], cross-sectional rank",
                "event_post_filing_drift", +1, "SEC filing (filed date)",
                ["price"], horizon_days=20, turnover_expectation="high"))
    H.append(_h("evt_post_filing_reversal", "post_filing_price_reaction", EXPLORATORY,
                "Extreme immediate filing reaction partially reverses (over-reaction).",
                "-abnormal_return over [filing-1, filing+1], ranked; hold 20d",
                "event_post_filing_reversal", -1, "SEC filing (filed date)",
                ["price"], horizon_days=20, turnover_expectation="high"))
    H.append(_h("evt_delayed_response_to_change", "post_filing_price_reaction", EXPLORATORY,
                "Delayed market response to a material fundamental change: pair a "
                "large fundamental delta with a muted initial price move.",
                "rank(fundamental_delta) * I(|abnormal_return_filing| small)",
                "event_delayed_response", +1, "SEC filing (filed date)",
                ["price", "revenue"], horizon_days=63))

    # --- Family 3: residual / relative price (exploratory) --------------------
    H.append(_h("res_market_residual_momentum", "residual_price", EXPLORATORY,
                "Momentum in the market-residual (idiosyncratic) return is cleaner "
                "than raw momentum, removing beta contamination.",
                "residual 6-1 momentum after removing market beta, cross-sectional rank",
                "residual_market_momentum", +1, "quarterly rebalance",
                ["price"], horizon_days=63))
    H.append(_h("res_industry_residual_momentum", "residual_price", EXPLORATORY,
                "Momentum in the industry-residual return isolates stock-specific "
                "trend from industry co-movement.",
                "residual 6-1 momentum after removing industry mean, cross-sectional rank",
                "residual_industry_momentum", +1, "quarterly rebalance",
                ["price", "sector"], horizon_days=63))
    H.append(_h("res_industry_relative_momentum", "residual_price", EXPLORATORY,
                "Industry-relative momentum (stock minus industry return) captures "
                "within-industry winners.",
                "(stock 6-1 return - industry mean 6-1 return), cross-sectional rank",
                "residual_industry_relative", +1, "quarterly rebalance",
                ["price", "sector"], horizon_days=63))
    H.append(_h("res_short_term_reversal_extreme", "residual_price", EXPLORATORY,
                "Short-term reversal after an extreme abnormal move (liquidity "
                "provision premium).",
                "-abnormal_return_1m conditioned on |abnormal_return_1m| in top decile",
                "residual_short_term_reversal", -1, "monthly rebalance",
                ["price"], horizon_days=20, turnover_expectation="high"))

    # --- Family 4: liquidity (CONFIRMATORY, canonical anomaly) ----------------
    same_sample = ("Re-test on the SAME owned panel Stage 11 used -> confirmatory "
                   "of a pre-existing economic prior, NOT out-of-sample.")
    H.append(_h("cfm_amihud_illiquidity", "liquidity", CONFIRMATORY,
                "Illiquidity premium: less-liquid stocks earn higher returns.",
                "Amihud illiquidity = mean(|ret|/dollar_volume), cross-sectional rank",
                "confirm_amihud_illiquidity", +1, "quarterly rebalance",
                ["price", "volume"], citation="Amihud (2002)", caveat=same_sample))
    H.append(_h("cfm_dollar_volume_liquidity", "liquidity", CONFIRMATORY,
                "Low dollar-volume (small, less-liquid) names carry a liquidity "
                "premium.",
                "-log(dollar_volume), cross-sectional rank",
                "confirm_dollar_volume", +1, "quarterly rebalance",
                ["price", "volume"], citation="Amihud (2002); Datar et al. (1998)",
                caveat=same_sample))

    # --- Family 5: profitability / quality (CONFIRMATORY, canonical) ----------
    H.append(_h("cfm_gross_profitability", "profitability_quality", CONFIRMATORY,
                "Gross profitability (gross_profit/assets) predicts returns as well "
                "as book-to-market (quality anomaly).",
                "gross_profit/assets, cross-sectional rank",
                "confirm_gross_profitability", +1, "quarterly rebalance",
                ["gross_profit", "assets"], citation="Novy-Marx (2013)",
                caveat=same_sample))
    H.append(_h("cfm_gross_profit_growth", "profitability_quality", CONFIRMATORY,
                "Growth in gross profit is a quality-improvement signal.",
                "d(gross_profit)/assets, cross-sectional rank",
                "confirm_gross_profit_growth", +1, "quarterly rebalance",
                ["gross_profit", "assets"], caveat=same_sample))

    # --- Family 6: risk / defensive (exploratory) -----------------------------
    H.append(_h("risk_idiosyncratic_vol", "risk_defensive", EXPLORATORY,
                "Low idiosyncratic volatility earns higher risk-adjusted returns "
                "(low-vol anomaly).",
                "-idiosyncratic_vol (residual std), cross-sectional rank",
                "risk_idiosyncratic_vol", +1, "quarterly rebalance",
                ["price"], citation="Ang, Hodrick, Xing, Zhang (2006)"))
    H.append(_h("risk_downside_beta_defensive", "risk_defensive", EXPLORATORY,
                "Defensive names with low downside beta are under-priced (BAB).",
                "-downside_beta, cross-sectional rank",
                "risk_downside_beta", +1, "quarterly rebalance",
                ["price"], citation="Frazzini & Pedersen (2014)"))

    # --- Family 7: small conditional combinations (Workstream H, exploratory) -
    H.append(_h("cmb_fund_improve_plus_residual_trend", "conditional_combination", EXPLORATORY,
                "Fundamental improvement CONFIRMED by positive residual price trend "
                "(two independent legs agreeing).",
                "rank(profitability_inflection) + rank(residual_market_momentum)",
                "combo_fund_plus_residual", +1, "quarterly rebalance",
                ["net_income", "assets", "price"]))
    H.append(_h("cmb_margin_accel_plus_muted_reaction", "conditional_combination", EXPLORATORY,
                "Margin acceleration with a muted initial price reaction (delayed "
                "response setup).",
                "rank(gross_margin_change) gated on small |abnormal_return_filing|",
                "combo_margin_plus_muted", +1, "SEC filing (filed date)",
                ["gross_profit", "revenue", "price"]))
    H.append(_h("cmb_leverage_reduction_plus_industry_mom", "conditional_combination", EXPLORATORY,
                "De-leveraging plus industry-relative momentum (fundamental + price "
                "confirmation).",
                "rank(leverage_reduction) + rank(industry_relative_momentum)",
                "combo_leverage_plus_industry", +1, "quarterly rebalance",
                ["liabilities", "assets", "price", "sector"]))
    H.append(_h("cmb_cashflow_improve_after_drawdown", "conditional_combination", EXPLORATORY,
                "Cash-flow improvement following a price drawdown (fundamental "
                "recovery the market has not yet priced).",
                "rank(cash_flow_improvement) gated on trailing drawdown in bottom tercile",
                "combo_cashflow_after_drawdown", +1, "quarterly rebalance",
                ["cash", "assets", "price"]))
    H.append(_h("cmb_deterioration_plus_extreme_up", "conditional_combination", EXPLORATORY,
                "Fundamental deterioration combined with an extreme positive "
                "abnormal return (over-extension short).",
                "-rank(profitability_inflection) gated on top-decile abnormal_return",
                "combo_deterioration_plus_extreme", -1, "quarterly rebalance",
                ["net_income", "assets", "price"], turnover_expectation="high"))

    return H


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def build_registry() -> Dict[str, Any]:
    """Construct the pre-registered registry with a content-addressed version."""
    hyps = _hypotheses()
    ids = [h["id"] for h in hyps]
    if len(ids) != len(set(ids)):
        dupes = sorted({i for i in ids if ids.count(i) > 1})
        raise ValueError("duplicate hypothesis ids: %s" % dupes)
    families = sorted({h["economic_family"] for h in hyps})
    body = {
        "schema_version": REGISTRY_SCHEMA_VERSION,
        "evaluation_contract": dict(EVALUATION_CONTRACT),
        "n_hypotheses": len(hyps),
        "economic_families": families,
        "n_economic_families": len(families),
        "n_confirmatory": sum(1 for h in hyps if h["status"] == CONFIRMATORY),
        "n_exploratory": sum(1 for h in hyps if h["status"] == EXPLORATORY),
        "hypotheses": hyps,
    }
    # The version hashes the hypotheses AND the evaluation contract + schema, so a
    # material change to HOW the hypotheses are measured mints a new version even
    # when the economic ideas are unchanged.
    body["registry_version"] = hashlib.sha256(canonical_json({
        "schema_version": REGISTRY_SCHEMA_VERSION,
        "evaluation_contract": EVALUATION_CONTRACT,
        "hypotheses": body["hypotheses"]}).encode("utf-8")).hexdigest()[:16]
    return body


def validate_registry(reg: Mapping[str, Any]) -> List[str]:
    """Return a list of pre-registration violations (empty == valid)."""
    problems: List[str] = []
    hyps = reg.get("hypotheses") or []
    if not (20 <= len(hyps) <= 40):
        problems.append("hypothesis count %d outside [20,40]" % len(hyps))
    fams = {h.get("economic_family") for h in hyps}
    if len(fams) < 5:
        problems.append("only %d economic families (need >=5)" % len(fams))
    required = ("id", "economic_family", "status", "economic_rationale", "formula",
                "expected_direction", "event_or_rebalance_timing", "universe",
                "data_requirement", "holding_horizon_days", "neutralization",
                "cost_assumption_bps", "turnover_expectation", "primary_metric",
                "disqualifying_gates", "design", "confirmatory_or_exploratory")
    ids = set()
    for h in hyps:
        for f in required:
            if f not in h or h.get(f) in (None, "", []):
                if f == "data_requirement" and h.get(f) == []:
                    continue
                problems.append("hypothesis %s missing field %s" % (h.get("id"), f))
        if h.get("id") in ids:
            problems.append("duplicate id %s" % h.get("id"))
        ids.add(h.get("id"))
        if h.get("status") not in (CONFIRMATORY, EXPLORATORY):
            problems.append("hypothesis %s bad status %s" % (h.get("id"), h.get("status")))
    expected = build_registry()["registry_version"]
    if reg.get("registry_version") != expected:
        problems.append("registry_version mismatch (frozen=%s current=%s)"
                        % (reg.get("registry_version"), expected))
    return problems


class RegistryImmutabilityError(RuntimeError):
    pass


def freeze(path: str | Path, *, overwrite_identical: bool = True) -> Dict[str, Any]:
    """Write the frozen registry once. Re-freezing DIFFERENT content raises.

    First-write-wins: if the file exists and its content matches, it is left
    untouched; if it exists with different content, ``RegistryImmutabilityError``
    is raised (the pre-registration must never silently drift).
    """
    path = Path(path)
    reg = build_registry()
    payload = json.dumps(reg, sort_keys=True, indent=1) + "\n"
    if path.exists():
        existing = path.read_text(encoding="utf-8")
        try:
            existing_reg = json.loads(existing)
        except ValueError:
            existing_reg = None
        if existing_reg and existing_reg.get("registry_version") == reg["registry_version"]:
            return reg
        raise RegistryImmutabilityError(
            "frozen registry at %s differs (frozen=%s new=%s); pre-registration is immutable"
            % (path, (existing_reg or {}).get("registry_version"), reg["registry_version"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(payload, encoding="utf-8")
    tmp.replace(path)
    return reg


def load_registry(path: str | Path) -> Dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def default_registry_path(repo_root: Optional[str | Path] = None) -> Path:
    # v2 (current) event-time contract registry. The v1 file (prior_registry_path)
    # is retained unchanged as the historical pre-registration.
    root = Path(repo_root) if repo_root else Path(__file__).resolve().parent.parent
    return root / "configs" / "alpha_agent" / "stage12_hypotheses_v2.json"


def prior_registry_path(repo_root: Optional[str | Path] = None) -> Path:
    """The retained v1 frozen registry (calendar-cross-section contract)."""
    root = Path(repo_root) if repo_root else Path(__file__).resolve().parent.parent
    return root / "configs" / "alpha_agent" / "stage12_hypotheses.json"


if __name__ == "__main__":  # pragma: no cover
    reg = build_registry()
    print("registry_version:", reg["registry_version"], "n:", reg["n_hypotheses"],
          "families:", reg["n_economic_families"])
    print("problems:", validate_registry(reg))
