"""alpha_agent.r42.forward - Tracks P and Q: prospective evidence.

Track P. The R41 shadow accrues forward evidence through ITS OWN frozen
owner, :mod:`alpha_agent.r41.forward_freeze`. R42 calls
``capture()``; it does not reimplement capture, does not backfill, does
not refit and does not revise a frozen decision. Whatever the forward
stream says - helpful or not - is reported.

R42 also audits whether that stream CAN accrue. The R41 shadow reads the
Binance monthly public archive, and the venue's own REST API answers
HTTP 451 from the operator's location. If the archive only publishes
month-end, a DAILY shadow cannot produce a daily row, and that is an
operational defect worth naming before a year of silence is mistaken for
a year of evidence.

Track Q. At most THREE new R42 shadows, and one is frozen only if its
specification was fixed BEFORE any of its prospective observations - which
in practice means its rule must already be inside the hashed frozen
contract. Every R42 shadow is RESEARCH_SHADOW_ONLY with
PROMOTION_ALLOWED = False.
"""
from __future__ import annotations

import datetime as _dt

import pandas as pd

from . import CAMPAIGN_ID, artifact_body, campaign_dir, read_json, sha
from . import contract as C
from . import write_artifact
from ..r39.research_shadow import _desk
from ..r41 import forward_freeze as FF

CALCULATION_OWNER = "alpha_agent.r42.forward"
ARTIFACT = "FORWARD_EVIDENCE.json"
R42_REGISTRY = "r42_shadow_registry.json"
R42_SNAPSHOTS = "r42_shadow_forward_snapshots.json"
MAX_R42_SHADOWS = 3

ARCHIVE_DAILY_FUNDING_PROBE = (
    "https://data.binance.vision/data/futures/um/daily/fundingRate/"
    "BTCUSDT/BTCUSDT-fundingRate-2026-07-15.zip")


# --------------------------------------------------------------------------- #
# Track P - capture through the R41 owner, and audit whether it can run
# --------------------------------------------------------------------------- #
def capture_r41(*, as_of: str = None) -> dict:
    """Delegate to the R41 owner. R42 never writes an R41 forward row."""
    try:
        res = FF.capture(as_of=as_of)
    except Exception as exc:                              # pragma: no cover
        return {"state": "CAPTURE_ERROR",
                "error": "%s: %s" % (type(exc).__name__, exc)}
    res["owner"] = "alpha_agent.r41.forward_freeze.capture"
    res["r42_wrote_no_forward_row"] = True
    return res


def stream_feasibility() -> dict:
    """Can the frozen daily shadow actually produce daily rows?"""
    from . import acquisition as ACQ
    reg = FF.load_registry()
    frozen_at = reg.get("frozen_at")
    df = FF._signal_series()
    last = pd.Timestamp(df.index.max())
    now = pd.Timestamp(_dt.datetime.now(_dt.timezone.utc))
    daily_probe = ACQ.fetch(ARCHIVE_DAILY_FUNDING_PROBE, timeout=45,
                            retries=1)
    return {
        "shadow_id": (reg.get("shadows") or [{}])[0].get("shadow_id"),
        "frozen_at": frozen_at,
        "data_source": "Binance public monthly archive (the shadow's own "
                       "declared venue)",
        "signal_series_last_date": str(last.date()),
        "utc_now": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "data_lag_days": int((now.tz_localize(None)
                              - last.tz_localize(None)).days),
        "archive_publishes_daily_funding_files": bool(daily_probe),
        "archive_daily_probe_url": ARCHIVE_DAILY_FUNDING_PROBE,
        "venue_rest_api_state": "VENUE_GEO_RESTRICTED (HTTP 451)",
        "first_eligible_decision_date":
            str((pd.Timestamp(frozen_at).tz_localize(None)
                 + pd.Timedelta(days=1)).date()) if frozen_at else None,
        "can_accrue_today": bool(
            last.tz_localize(None) > pd.Timestamp(frozen_at)
            .tz_localize(None)) if frozen_at else False,
        "blocker": (None if daily_probe
                    else "the archive publishes funding MONTHLY, so a "
                         "daily shadow reading it cannot produce a row "
                         "until the month closes and the file appears; the "
                         "venue's REST API, which would close the gap, "
                         "answers HTTP 451 from this location"),
        "consequence": "the R41 shadow's first TRUE_FORWARD rows will "
                       "appear in a monthly batch, not daily. The rows are "
                       "still genuinely prospective - capture refuses any "
                       "date at or before the freeze - but the CADENCE "
                       "claim (~365 marks/yr) is not achievable through "
                       "this data path from this location.",
    }


# --------------------------------------------------------------------------- #
# Track Q - new R42 shadows
# --------------------------------------------------------------------------- #
R42_SHADOW_CANDIDATES = {
    "R42_POSITIVE_ONLY_CASH_AND_CARRY_BTC": {
        "eligible": True,
        "rule": C.POSITIVE_ONLY_BASELINE["rule"],
        "why_eligible": "the rule is written verbatim in "
                        "contract.POSITIVE_ONLY_BASELINE and was hashed "
                        "into r42_frozen_contract.json BEFORE the first "
                        "R42 outcome was computed; no prospective "
                        "observation of it exists yet",
        "economics": "complete: real fee schedule + spread, conservative "
                     "committed capital, risk-free control",
        "prospective_question": "the historical point estimate on the most "
                                "recent evidence zone is NEGATIVE "
                                "(-0.67 %/yr, t -2.41). This shadow tests "
                                "prospectively whether the crypto carry "
                                "premium recovers ABOVE the cost of the "
                                "capital it requires. A shadow is not a "
                                "recommendation.",
    },
    "R42_BROAD_CROSS_ASSET_FUNDING_PORTFOLIO": {
        "eligible": False,
        "why_not": "the ASSET ELIGIBILITY rule was frozen in advance, but "
                   "the PORTFOLIO construction (weighting, rebalancing, "
                   "per-name caps, delisting policy) was not. Specifying "
                   "it now would be specifying it after seeing 69 assets' "
                   "results, which is exactly the failure mode this "
                   "release exists to police.",
    },
    "R42_CME_REGULATED_BASIS": {
        "eligible": False,
        "why_not": "contract.CME_REPLICATION froze the EXPRESSION and the "
                   "roll policy, but not the entry rule, the contract "
                   "selection or the capital treatment - and the fairest "
                   "capital treatment (FCM margin earning interest) was "
                   "chosen during the track, after seeing the result. It "
                   "is a strong Release-43 candidate to predeclare, not a "
                   "Release-42 freeze.",
    },
}


def freeze_r42_shadows() -> dict:
    existing = read_json(campaign_dir(CAMPAIGN_ID) / R42_REGISTRY)
    if existing:
        return existing
    frozen_at = _dt.datetime.now(_dt.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ")
    shadows = []
    for sid, spec in R42_SHADOW_CANDIDATES.items():
        if not spec.get("eligible"):
            continue
        body = {
            "shadow_id": sid,
            "rule": spec["rule"],
            "symbol": "BTCUSDT", "venue": "BINANCE (public archive)",
            "information_family": "CRYPTO_MARKET_STRUCTURE",
            "economic_expression": "DELTA_NEUTRAL_BASIS_CASH_AND_CARRY",
            "decision_cadence": "DAILY (UTC close)",
            "capital_model": C.PRIMARY_CAPITAL_MODEL,
            "execution_model": C.PRIMARY_EXECUTION_MODEL,
            "control": C.PRIMARY_CONTROL,
            "economics": spec["economics"],
            "prospective_question": spec["prospective_question"],
            "specification_predates_every_prospective_observation": True,
            "predeclared_in_contract_hash": C.contract_hash(),
            "research_shadow_only": True,
            "promotion_allowed": False,
            "historical_qualification":
                "FAIL - the predeclared representative's Zone-C excess over "
                "the risk-free rate is -0.67 %/yr (t -2.41). This shadow is "
                "frozen to test the hypothesis prospectively, NOT because "
                "it qualified.",
            "caveats": [
                "the operator has no demonstrated admissible account path "
                "at this venue (HTTP 451)",
                "the reverse (short-spot) leg is excluded as "
                "HISTORICALLY_NON_IMPLEMENTABLE",
                "collateral and counterparty tail risk are not in the "
                "daily P&L",
            ],
        }
        body["spec_hash"] = sha({k: v for k, v in body.items()
                                 if k != "spec_hash"})
        body["frozen_at"] = frozen_at
        body["first_eligible_decision"] = ("the first full UTC day strictly "
                                           "after frozen_at")
        body["ledger_root"] = str(campaign_dir(CAMPAIGN_ID)
                                  / "research_shadow_forward")
        shadows.append(body)
    if len(shadows) > MAX_R42_SHADOWS:
        raise RuntimeError("R42 shadow cap exceeded")
    reg = artifact_body("r42_shadow_registry/1", {
        "calculation_owner": CALCULATION_OWNER,
        "frozen_at": frozen_at, "family_cap": MAX_R42_SHADOWS,
        "n_shadows": len(shadows), "shadows": shadows,
        "declined": {k: v for k, v in R42_SHADOW_CANDIDATES.items()
                     if not v.get("eligible")},
        "r41_shadow_untouched": True,
        "historical_observations_can_never_enter": True,
        "ledger_primitives": "api.paper_trading_desk chain-hash ledgers",
        "snapshot_ledger": str(campaign_dir(CAMPAIGN_ID)
                               / "research_shadow_forward" / R42_SNAPSHOTS),
    })
    reg["r42_shadow_registry_hash"] = sha(reg)
    write_artifact(R42_REGISTRY, reg, CAMPAIGN_ID)
    return reg


def _shadow_dir():
    d = campaign_dir(CAMPAIGN_ID) / "research_shadow_forward"
    d.mkdir(parents=True, exist_ok=True)
    return d


def capture_r42(*, as_of: str = None) -> dict:
    """Prospective capture for R42 shadows, on the canonical primitives."""
    reg = read_json(campaign_dir(CAMPAIGN_ID) / R42_REGISTRY)
    if not reg or not reg.get("shadows"):
        return {"state": "NOT_FROZEN"}
    from . import acquisition as ACQ
    from . import capital as CAP
    from . import legs as LG
    from . import pnl_audit as PA
    del ACQ
    frozen_at = pd.Timestamp(reg["frozen_at"]).tz_localize(None)
    desk = _desk()
    sdir = _shadow_dir()
    have = {r["decision_date"] for r in desk._read_ledger(sdir, R42_SNAPSHOTS)}
    df = PA.r41_panel("BTCUSDT")
    sig = LG.positive_only_signal(df)
    bk = CAP.implementable_book(df, sig,
                                capital_model=C.PRIMARY_CAPITAL_MODEL,
                                execution_model=C.PRIMARY_EXECUTION_MODEL,
                                charge_financing=True)
    bk.index = bk.index.tz_localize(None)
    today = pd.Timestamp(as_of or _dt.datetime.now(_dt.timezone.utc)
                         .strftime("%Y-%m-%d"))
    appended = []
    for d in bk.index:
        if not (d > frozen_at and d < today):
            continue
        if str(d.date()) in have:
            continue
        r = bk.loc[d]
        desk._append_ledger(sdir, R42_SNAPSHOTS, [{
            "decision_date": str(d.date()),
            "shadow_id": reg["shadows"][0]["shadow_id"],
            "spec_hash": reg["shadows"][0]["spec_hash"],
            "held": float(r["held"]) if pd.notna(r["held"]) else None,
            "pnl_on_capital": float(r["pnl_on_capital"]),
            "benchmark": float(r["benchmark"]),
            "excess": float(r["excess"]),
            "captured_at": _dt.datetime.now(_dt.timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%SZ"),
            "true_forward": True,
        }])
        appended.append(str(d.date()))
    return {"state": "OK", "appended": appended,
            "n_rows": len(desk._read_ledger(sdir, R42_SNAPSHOTS)),
            "chain": desk.verify_ledger(sdir, R42_SNAPSHOTS)}


def run(*, as_of: str = None) -> dict:
    r41 = capture_r41(as_of=as_of)
    feas = stream_feasibility()
    reg = freeze_r42_shadows()
    r42 = capture_r42(as_of=as_of)
    body = artifact_body("r42_forward_evidence/1", {
        "calculation_owner": CALCULATION_OWNER,
        "track": "P + Q - prospective evidence and new shadows",
        "r41_capture": r41,
        "r41_stream_feasibility": feas,
        "r42_registry": {"n_shadows": reg.get("n_shadows"),
                         "frozen_at": reg.get("frozen_at"),
                         "shadow_ids": [s["shadow_id"]
                                        for s in reg.get("shadows", [])],
                         "declined": list((reg.get("declined") or {}))},
        "r42_capture": r42,
        "evidence_classes": {
            "HISTORICAL": "R41 Zone A/B/C and every R42 re-scoring of the "
                          "same dates",
            "HISTORICAL_OUT_OF_ASSET_REPLICATION": "the 69 new eligible "
                                                   "assets - NOT forward",
            "HISTORICAL_CROSS_VENUE_REPLICATION": "the 6 eligible venues - "
                                                  "NOT forward",
            "TRUE_FORWARD": "rows strictly after a freeze, captured "
                            "contiguously, never backfilled",
        },
        "never_backfilled": True,
        "never_refitted": True,
        "no_frozen_decision_revised": True,
        "true_forward_rows_r41": r41.get("n_rows", 0),
        "true_forward_rows_r42": r42.get("n_rows", 0),
        "verdict": {
            "state": ("TRUE_FORWARD_NOT_YET_TESTABLE"
                      if not (r41.get("n_rows") or r42.get("n_rows"))
                      else "TRUE_FORWARD_ACCRUING"),
            "note": "the R41 shadow froze %s and the R42 shadow froze %s; "
                    "no full UTC day has elapsed after either freeze that "
                    "the venue archive has published. Forward evidence "
                    "cannot strengthen or weaken anything yet, and no "
                    "claim is made that it does."
                    % (FF.load_registry().get("frozen_at"),
                       reg.get("frozen_at")),
        },
    })
    body["forward_evidence_hash"] = sha(body)
    write_artifact(ARTIFACT, body, CAMPAIGN_ID, overwrite=True)
    return body
