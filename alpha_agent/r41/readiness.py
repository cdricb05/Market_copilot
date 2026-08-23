"""alpha_agent.r41.readiness - Track 18: near-real-time research readiness
for every surviving strategy (and the blockers for everything else).

No live execution is wired. This artifact answers: at what cadence COULD
each candidate eventually operate, what must refresh, how stale is fatal,
and which blocker (INFORMATION / MODEL / COMPUTE / TIME) binds.
"""
from __future__ import annotations

import datetime as _dt
import time

from . import artifact_body, campaign_dir, sha, write_json

CALCULATION_OWNER = "alpha_agent.r41.readiness"
ARTIFACT_NAME = "near_real_time_readiness.json"


def _time_funding_rescore() -> float:
    from . import forward_freeze as FF
    t = time.time()
    try:
        FF._signal_series()
    except Exception:
        return float("nan")
    return round(time.time() - t, 1)


ROWS = {
    "shadow_btc_funding_carry_1d (FROZEN R41 SHADOW)": {
        "required_input_refresh": "Binance funding (8h) + daily closes",
        "feature_update_latency": "seconds after UTC close",
        "scoring_latency_seconds_measured": None,   # filled at build
        "portfolio_reassessment_trigger": "daily UTC close; funding-"
                                          "interval cadence feasible",
        "maximum_stale_age": "1 day (the signal is a 30-day mean; one "
                             "stale day degrades gracefully)",
        "data_quality_blockers": "archive publishes T+1; live REST needed "
                                 "for same-day",
        "execution_relevant_latency": "minutes (funding accrues at fixed "
                                      "times)",
        "feasible_cadence": "8h to daily",
        "binding_blocker": "TIME (forward evidence accrual)"},
    "RATES_RV lab (no surviving candidate)": {
        "feasible_cadence": "daily now; intraday after a native intraday "
                            "futures purchase",
        "binding_blocker": "INFORMATION (killed by year-block instability "
                           "and placebo insensitivity - needs genuinely "
                           "new conditioning information, e.g. flow/"
                           "positioning at higher frequency)"},
    "COMMODITY_CURVE lab": {
        "feasible_cadence": "daily",
        "binding_blocker": "INFORMATION+COST (gross curve information "
                           "real; notional costs dominate the diversified "
                           "book; concentrated/liquid expressions failed "
                           "Zone B)"},
    "VOLATILITY lab": {
        "feasible_cadence": "daily on VX",
        "binding_blocker": "INFORMATION (options surfaces unowned - the "
                           "top purchase recommendation)"},
    "MICROSTRUCTURE (signed flow)": {
        "feasible_cadence": "per 5m bar",
        "binding_blocker": "COST (taker fees; maker/queue execution is an "
                           "EXECUTION-MODEL question, not information)"},
    "FX lab": {"feasible_cadence": "daily/intraday",
               "binding_blocker": "INFORMATION (premium era-limited)"},
    "MODEL scale axis": {
        "feasible_cadence": "n/a",
        "binding_blocker": "NONE REMAINING LOCALLY - scaling the TCN 2-8x "
                           "locally DEGRADED Zone-B t (2.07 -> -0.03); "
                           "the GPU escalation case is WEAKENED by "
                           "measurement"},
}


def build() -> dict:
    rows = dict(ROWS)
    key = "shadow_btc_funding_carry_1d (FROZEN R41 SHADOW)"
    rows[key] = dict(rows[key])
    rows[key]["scoring_latency_seconds_measured"] = _time_funding_rescore()
    body = artifact_body("r41_near_real_time_readiness/1", {
        "calculation_owner": CALCULATION_OWNER,
        "built_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "rows": rows,
        "no_live_execution_wired": True})
    body["readiness_hash"] = sha(body)
    write_json(campaign_dir() / ARTIFACT_NAME, body, immutable=False)
    return body
