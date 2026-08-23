"""alpha_agent.r41.horizon_engine - Track 1/13: the multi-horizon target
contract, and the honest decisions-per-year ledger.

Cadence is a property of the CANDIDATE. Every lab family below reports the
horizons its information genuinely supports (from the measured inventory)
and the EFFECTIVE decisions per year its candidates produce - effective
sample sizes from the labs' own HAC/autocorrelation measurements, never
rows-in-a-dataframe. The objective is MORE REAL DECISIONS PER YEAR, and
this artifact is where that claim is audited.
"""
from __future__ import annotations

import datetime as _dt

from . import artifact_body, campaign_dir, sha, write_json
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r41.horizon_engine"
ARTIFACT_NAME = "multi_horizon_target_contract.json"

FAMILY_HORIZONS = {
    "RATES_RV": {
        "target_horizons": ["1s", "2s", "5s", "10s", "21s", "42s", "63s"],
        "decision_cadence": "daily (settlements); intraday only after a "
                            "native intraday futures purchase",
        "measured": "daily decisions with h-session tranching; ESS ratios "
                    "0.8-1.0 at h<=5, shrinking with overlap at h=21",
    },
    "COMMODITY_CURVE": {
        "target_horizons": ["1s", "2s", "5s", "10s", "21s", "42s", "63s",
                            "EVENT(EIA weekly)"],
        "decision_cadence": "daily + weekly event windows",
        "measured": "gross curve information real at 5-21s; net killed by "
                    "notional costs on a diversified book",
    },
    "VOLATILITY_OPTIONS": {
        "target_horizons": ["1s", "2s", "5s", "10s", "21s"],
        "decision_cadence": "daily (VX curve); richer cadence needs an "
                            "options surface purchase",
        "measured": "no advanceable daily VX signal this release",
    },
    "FX": {
        "target_horizons": ["1s", "2s", "5s", "10s", "21s"],
        "decision_cadence": "daily; 1-minute where Dukascopy history runs",
        "measured": "carry/momentum era-limited (Zone-A only), all "
                    "horizons - cadence does not rescue a dead premium",
    },
    "CRYPTO": {
        "target_horizons": ["5m", "15m", "60m", "8h_funding", "1d", "3d",
                            "7d"],
        "decision_cadence": "daily for the frozen funding shadow; 8h "
                            "funding intervals feasible",
        "measured": "funding carry: ~365 marks/yr, ESS ratio 0.07-0.15 "
                    "(the premium is persistent income); OFI: 105k "
                    "bars/yr, taker-cost-killed",
    },
    "MICROSTRUCTURE": {
        "target_horizons": ["5m", "15m", "60m"],
        "decision_cadence": "per bar (research only)",
        "measured": "signed-flow gross information real (BTC +21%/yr "
                    "gross at 5m); dies at taker costs",
    },
    "CREDIT": {
        "target_horizons": ["2s", "5s", "21s"],
        "decision_cadence": "daily (ETF proxies)",
        "measured": "no advanceable signal; deep OAS history licence-"
                    "walled",
    },
    "EQUITY_REVISIONS": {
        "target_horizons": [],
        "decision_cadence": "BLOCKED - no PIT vintage source owned",
        "measured": "Zacks sample tier is megacap current-snapshot only",
    },
}


def build() -> dict:
    body = artifact_body("r41_multi_horizon_target_contract/1", {
        "calculation_owner": CALCULATION_OWNER,
        "built_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "horizon_classes": C.HORIZON_CLASSES,
        "no_interpolated_intraday": C.NO_INTERPOLATED_INTRADAY,
        "cadence_is_a_candidate_property":
            C.DECISION_CADENCE_IS_A_CANDIDATE_PROPERTY,
        "no_upsampling": C.NO_UPSAMPLING_OF_SLOW_STRATEGIES,
        "families": FAMILY_HORIZONS,
        "system_clocks": C.SYSTEM_CLOCKS,
        "the_correction": "the system is NOT monthly; R39/R40 shadows are "
                          "monthly because THOSE candidates are monthly; "
                          "the R41 shadow is daily; intraday candidates "
                          "await native intraday data",
    })
    body["horizon_contract_hash"] = sha(body)
    write_json(campaign_dir() / ARTIFACT_NAME, body, immutable=False)
    return body
