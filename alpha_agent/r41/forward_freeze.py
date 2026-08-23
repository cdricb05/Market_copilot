"""alpha_agent.r41.forward_freeze - Track 17: the immediate forward freeze.

Any candidate that passes the declared historical RESEARCH-CANDIDATE gate
(and survives the alpha-killer) is frozen IMMEDIATELY as a NON-PROMOTABLE
research shadow, before a single prospective outcome exists. Cap: THREE
R41 shadows (the R40 family of five is separate and immutable). Ledgers:
the canonical chain-hashed desk primitives (``api.paper_trading_desk``),
path-parameterised under the R41 research root - the same owner every
release since Phase 27 has used.

Shadow 1 - shadow_btc_funding_carry_1d: the delta-neutral BTC perp
funding-carry rule (30/90 z-gate at +-0.5, short-perp/long-spot on
positive z), DAILY decisions on the Binance public archive. Frozen with
its full parameterisation hashed; capture never refits anything (there is
nothing to fit). TRUE_FORWARD rows begin strictly after the freeze.
"""
from __future__ import annotations

import datetime as _dt

import numpy as np
import pandas as pd

from . import (CAMPAIGN_ID, artifact_body, campaign_dir, read_json, sha,
               write_json)
from ..r39.research_shadow import _desk
from . import crypto_lab as CRL

CALCULATION_OWNER = "alpha_agent.r41.forward_freeze"
REGISTRY_NAME = "r41_shadow_registry.json"
SNAPSHOT_LEDGER = "r41_shadow_forward_snapshots.json"
OUTCOME_LEDGER = "r41_shadow_forward_outcomes.json"
MAX_R41_SHADOWS = 3

FUNDING_SPEC = {
    "shadow_id": "shadow_btc_funding_carry_1d",
    "rule": "z = mean(funding, 30d) / std(funding, 90d), lagged 1 day; "
            "position +1 (short perp / long spot) when z > 0.5, -1 when "
            "z < -0.5, else flat; daily UTC decisions; unit notional",
    "symbol": "BTCUSDT", "venue": "BINANCE (public archive)",
    "information_family": "CRYPTO_MARKET_STRUCTURE",
    "economic_expression": "DELTA_NEUTRAL_BASIS",
    "decision_cadence": "DAILY (UTC close)",
    "cost_model": "5 bps per side per leg on position change (2 legs)",
    "z_window_mean": 30, "z_window_std": 90, "z_threshold": 0.5,
    "selection_evidence": {
        "zone_b_t": 10.24, "zone_b_excess_ann": 0.0874,
        "zone_c_t": 6.91, "zone_c_excess_ann": 0.0315,
        "killer": "0 sign flips; survives cost x3, latency, year blocks, "
                  "threshold perturbation; placebo gate retains t 4.45 "
                  "(the unconditional premium) vs 10.2 (timing adds)"},
    "caveats": ["single-venue counterparty/execution risk not modelled",
                "premium compressed from 8.7%/yr (2022-24) to 3.2%/yr "
                "(2024-26) - the forward stream tests persistence",
                "BTC/ETH universe selection PIT-defensible but recorded"],
}


def shadow_dir():
    d = campaign_dir(CAMPAIGN_ID) / "research_shadow_forward"
    d.mkdir(parents=True, exist_ok=True)
    return d


def load_registry() -> dict:
    return read_json(campaign_dir(CAMPAIGN_ID) / REGISTRY_NAME) or {}


def freeze() -> dict:
    existing = load_registry()
    if existing:
        return existing
    frozen_at = _dt.datetime.now(_dt.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ")
    spec = dict(FUNDING_SPEC)
    spec_hash = sha(spec)
    shadows = [{**spec, "spec_hash": spec_hash, "frozen_at": frozen_at,
                "first_eligible_decision": "the first full UTC day "
                                           "strictly after frozen_at",
                "research_shadow_only": True, "promotion_allowed": False,
                "historical_qualification":
                    "SEE r41 campaign verdict (gate evaluated there)",
                "immutable": True,
                "r40_family_distinctness": "asset-disjoint from all five "
                                           "R40 shadows (no crypto member)",
                "ledger_root": str(shadow_dir())}]
    if len(shadows) > MAX_R41_SHADOWS:
        raise RuntimeError("R41 shadow cap exceeded")
    body = artifact_body("r41_shadow_registry/1", {
        "calculation_owner": CALCULATION_OWNER, "frozen_at": frozen_at,
        "family_cap": MAX_R41_SHADOWS, "n_shadows": len(shadows),
        "shadows": shadows,
        "snapshot_ledger": str(shadow_dir() / SNAPSHOT_LEDGER),
        "outcome_ledger": str(shadow_dir() / OUTCOME_LEDGER),
        "historical_observations_can_never_enter": True,
        "ledger_primitives": "api.paper_trading_desk chain-hash ledgers"})
    body["r41_shadow_registry_hash"] = sha(body)
    write_json(campaign_dir(CAMPAIGN_ID) / REGISTRY_NAME, body)
    return body


def _signal_series() -> pd.DataFrame:
    fc = CRL.funding_carry_stream("BTCUSDT")
    return pd.DataFrame({"gross": fc["gross"], "signal": fc["signal"],
                         "funding": fc["funding"]},
                        index=pd.DatetimeIndex(fc["dates"]))


def capture(*, as_of: str = None) -> dict:
    """Contiguous TRUE_FORWARD capture for the R41 shadow: every full UTC
    day strictly after the freeze and strictly before today, in order,
    never twice. Refuses historical rows by construction."""
    reg = load_registry()
    if not reg:
        return {"state": "NOT_FROZEN"}
    frozen_at = pd.Timestamp(reg["frozen_at"]).tz_localize(None)
    desk = _desk()
    sdir = shadow_dir()
    snaps = desk._read_ledger(sdir, SNAPSHOT_LEDGER)
    have = {r["decision_date"] for r in snaps}
    df = _signal_series()
    df.index = df.index.tz_localize(None)
    today = pd.Timestamp((as_of or _dt.datetime.now(_dt.timezone.utc)
                          .strftime("%Y-%m-%d")))
    eligible = [d for d in df.index
                if d > frozen_at and d < today
                and str(d.date()) not in have]
    appended = []
    for d in sorted(eligible):
        row = df.loc[d]
        rec = {"decision_date": str(d.date()),
               "shadow_id": reg["shadows"][0]["shadow_id"],
               "spec_hash": reg["shadows"][0]["spec_hash"],
               "signal": float(row["signal"]),
               "funding_day": None if not np.isfinite(row["funding"])
               else float(row["funding"]),
               "gross_day": None if not np.isfinite(row["gross"])
               else float(row["gross"]),
               "captured_at": _dt.datetime.now(_dt.timezone.utc)
               .strftime("%Y-%m-%dT%H:%M:%SZ"),
               "lateness_days": int((today - d).days) - 1,
               "true_forward": True}
        desk._append_ledger(sdir, SNAPSHOT_LEDGER, [rec])
        appended.append(rec["decision_date"])
    return {"state": "OK", "appended": appended,
            "n_rows": len(desk._read_ledger(sdir, SNAPSHOT_LEDGER)),
            "chain": desk.verify_ledger(sdir, SNAPSHOT_LEDGER)}
