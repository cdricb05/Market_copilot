"""alpha_agent.r45.campaign - run Release 45 in the order the contract declares.

The ordering is the science. Replication first, on the events the rule's
parameters could not have been chosen on; then, and only then, the mechanism
work. Nothing downstream is allowed to change a parameter of the frozen rule,
and everything that does change one is charged.
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import json

from . import acquisition as AQ
from . import analyst as AN
from . import burden as BU
from . import causal as CA
from . import contract as C
from . import discovery as DI
from . import eventstudy as ES
from . import frontier as FR
from . import killer as KI
from . import ml as ML
from . import options as OP
from . import rv as RV
from . import shell_policy as SP
from . import surprise as SU

CALCULATION_OWNER = "alpha_agent.r45.campaign"


def _write(name: str, body: dict) -> str:
    C.ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    p = C.ARTIFACT_DIR / name
    p.write_text(json.dumps(body, indent=2, default=str), encoding="utf-8")
    return str(p)


def sha_file(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


def run(*, acquire: bool = False, deep: bool = True,
        options_acquire: bool = False, precomputed: dict = None) -> dict:
    """Run the release. ``precomputed`` may supply any lane already scored.

    The placebo battery resamples the whole calendar hundreds of times, so
    handing back a lane that has already been computed - byte for byte, from
    this same code - is the difference between one run and three. It changes
    no number; anything absent is still computed here.

    Only ``causal`` and ``discovery`` are honoured. Every other lane is
    re-run live regardless of what is offered, because a cached lane never
    fires its burden callback and an uncharged trial is laundering whether
    or not anybody intended it.
    """
    started = _dt.datetime.now(_dt.timezone.utc).isoformat()
    pre = precomputed or {}
    C.ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)

    contract_body = C.frozen_contract()
    _write("r45_frozen_contract.json", contract_body)
    _write("R45_SHELL_POLICY_EVENTS.json", SP.record())

    stamps = ES.release_stamps()
    manifest = AQ.run(listed=acquire, native=acquire) if acquire else \
        {"state": "USED_CACHED_PANELS",
         "blocked_native_routes": AQ.probe_blocked_native_routes()}

    # ---- Track B: the frozen rule, unchanged ---------------------------- #
    from . import replication as RE
    rep = RE.run(stamps)
    BU.charge_frozen_replication(
        list(C.OWNED_MINUTE_INSTRUMENTS)
        + list(C.LISTED_MINUTE_INSTRUMENTS)
        + list(C.NATIVE_FUTURES_INSTRUMENTS))

    # ---- Tracks C and D: is it the release? ----------------------------- #
    # Only lanes that charge NOTHING may be served from cache. A cached lane
    # never fires its charge callback, and a lane whose cells were explored
    # but not charged is burden laundering by accident - which the ledger has
    # no way to detect after the fact. Causal and discovery are cacheable
    # because their one charged item is re-charged explicitly below.
    causal = pre.get("causal") or (
        CA.run(stamps=stamps, charge=BU.charge) if deep
        else {"state": "SKIPPED"})
    if pre.get("causal") and causal.get("selection_premium"):
        BU.charge({"lane": "SELECTION_PREMIUM_DIAGNOSTIC",
                   "screen": "R44_60_CELL", "zones": ["A", "B", "C"]},
                  family="EVENT_FAMILY", lane="L5_CAUSAL",
                  label="the height of a maximum over sixty draws")

    # ---- Track E: who prices it first ----------------------------------- #
    # A measurement, not a candidate: it proposes no rule and charges nothing.
    disc = pre.get("discovery") or (
        DI.run(stamps) if deep else {"state": "SKIPPED"})

    # ---- Track F: relative value ---------------------------------------- #
    rv = RV.run(stamps, charge=BU.charge) if deep else {"state": "SKIPPED"}

    # ---- Track G: surprise ---------------------------------------------- #
    sur = SU.run(stamps=stamps, charge=BU.charge) if deep \
        else {"state": "SKIPPED"}

    # ---- Tracks H and I: state, then bounded models --------------------- #
    mlr = ML.run(stamps=stamps, charge=BU.charge) if deep \
        else {"state": "SKIPPED"}

    # ---- Tracks J and K: the kill battery on the strongest candidate ---- #
    best_sym = C.FROZEN_RULE["instrument_of_origin"]
    best_zone = "BC"
    ranked = [r for r in rep.get("ranked", [])
              if (r.get("n_events") or 0) >= C.MIN_EVENTS_TO_JUDGE_REPLICATION]
    if ranked:
        top = ranked[0]
        best_sym, best_zone = top["symbol"], (top.get("zone") or "ALL")
        if best_zone == "ALL":
            best_zone = None
    kill = KI.run(best_sym, stamps, zone=best_zone, charge=BU.charge) \
        if deep else {"state": "SKIPPED"}
    # The release's actual subject gets the battery too, whatever ranked
    # first - otherwise a lane could quietly avoid stress-testing the very
    # claim it was built to examine.
    kill_gold = (kill if (best_sym == C.FROZEN_RULE["instrument_of_origin"]
                          and best_zone == "BC")
                 else (KI.run(C.FROZEN_RULE["instrument_of_origin"], stamps,
                              zone="BC", charge=BU.charge) if deep
                       else {"state": "SKIPPED"}))

    # ---- Parallel information lanes ------------------------------------- #
    opts = OP.run(acquire=options_acquire)
    anl = AN.run()

    # ---- Frontier, freeze, purchase ------------------------------------- #
    front = FR.build(rep, rv=rv, killer=kill, causal=causal)
    shadows = FR.freeze_gate(front)
    purchase = FR.purchase_gate(rep, manifest)
    burden = BU.summary()

    lanes = {
        "L1_L4_REPLICATION": rep, "L5_L6_CAUSAL": causal,
        "L7_DISCOVERY": disc, "L8_RV": rv, "L9_SURPRISE": sur,
        "L10_L11_STATE_AND_ML": mlr, "L12_KILL": kill,
        "L12_KILL_GOLD_HOLDOUT": kill_gold,
        "L13_OPTIONS": opts, "L14_ANALYST": anl,
    }
    _write("R45_LANE_RESULTS.json",
           {"schema": "r45_lane_results/1", "campaign_id": C.CAMPAIGN_ID,
            "calculation_owner": CALCULATION_OWNER,
            "started_utc": started, "lanes": lanes,
            "safety_block": {
                "orders": 0, "paper_orders": 0, "portfolio_mutations": 0,
                "operational_writes": 0, "model_promotions": 0,
                "sleeve_activations": 0, "scheduler_changes": 0,
                "money_spent_usd": 0.0, "accounts_created": 0,
                "licences_accepted": 0}})
    _write("research_frontier.json", front)
    _write("R45_SHADOW_REGISTRY.json", shadows)
    _write("data_frontier.json", purchase)
    _write("search_burden.json", burden)
    _write("R45_ACQUISITION_MANIFEST.json", manifest)

    return {
        "campaign_id": C.CAMPAIGN_ID, "started_utc": started,
        "finished_utc": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "contract_hash": contract_body["contract_hash"],
        "lanes": lanes, "frontier": front, "shadows": shadows,
        "purchase": purchase, "burden": burden,
        "shell_policy": SP.record(), "acquisition": manifest,
        "best_candidate_for_kill_battery": {"symbol": best_sym,
                                            "zone": best_zone},
    }
