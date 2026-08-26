"""alpha_agent.r46.burden - two ledgers that may never be netted.

``HISTORICAL_DISCOVERY_BURDEN``
    the estate's cumulative count of effective search trials. Inherited at
    353 headline (355 conservative) from Release 45 and never reset. A trial
    is charged when a release CHOOSES something by looking at data.

``PROSPECTIVE_FORWARD_EVIDENCE``
    predictions put on the record before the outcome existed. These are not
    trials. A forward observation is the opposite of a search: it cannot be
    re-drawn, re-parameterised or re-selected, which is the whole reason this
    release exists.

Release 46 charges **zero** new historical trials for its seed cohort, and the
reason is structural rather than generous: it selected nothing. Every seed
parameter is a canonical constant fixed in
:data:`alpha_agent.r46.contract.SEED_PARAMETERS_WERE_NOT_SEARCHED` before any
market data was read. No sweep ran, no cell was ranked, no winner was picked.

What IS charged, and what this module exists to make impossible to forget:
**forward p-hacking**. The moment anyone chooses a threshold, promotes a
version, or picks a challenger AFTER seeing forward results, that choice is a
selection over the forward evidence and it is recorded here as a
``FORWARD_SELECTION`` decision. Release 45's whole finding was that a maximum
chosen by screening looks locally peaked from the inside; a forward screen has
exactly the same property and none of the excuses.
"""
from __future__ import annotations

from . import CAMPAIGN_ID, artifact_body, campaign_dir, read_json, write_json
from . import clock as CK
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r46.burden"

ARTIFACT = "r46_search_burden_ledger.json"
FORWARD_SELECTION_LEDGER = "r46_forward_selection_ledger.json"

#: New HISTORICAL search trials charged by Release 46, by family.
#: Every entry here would need a release that CHOSE a parameter from data.
R46_NEW_TRIALS_BY_FAMILY = {
    "PROSPECTIVE_TOURNAMENT_INFRASTRUCTURE": 0,
    "SEED_CHALLENGER_SELECTION": 0,
    "OPTIONS_VOL": 0,
    "ANALYST_REVISIONS": 0,
}

WHY_ZERO = (
    "a trial is charged when a release chooses a parameter, a cell, a "
    "threshold or a winner by looking at data. Release 46 chose none: the ten "
    "seed challengers use canonical constants written into the frozen "
    "contract before alpha_agent.r46.marketdata was first called, and no "
    "screen, sweep or ranking was run over this estate's returns to select "
    "any of them. Building infrastructure is not searching."
)


def new_trials() -> int:
    return int(sum(R46_NEW_TRIALS_BY_FAMILY.values()))


def historical(campaign_id: str = CAMPAIGN_ID) -> dict:
    inherited = C.INHERITED_GLOBAL_BURDEN
    inherited_cons = C.INHERITED_GLOBAL_BURDEN_CONSERVATIVE
    new = new_trials()
    body = artifact_body(
        "r46_search_burden/1", CALCULATION_OWNER,
        inherited_global_cumulative=inherited,
        inherited_global_conservative=inherited_cons,
        inherited_source=C.INHERITED_BURDEN_SOURCE,
        new_r46_effective_trials=new,
        GLOBAL_SEARCH_BURDEN=inherited + new,
        GLOBAL_SEARCH_BURDEN_CONSERVATIVE=inherited_cons + new,
        by_family=dict(R46_NEW_TRIALS_BY_FAMILY),
        why_zero=WHY_ZERO,
        burden_may_never_be_reset=True,
        prospective_evidence_is_not_search_burden=True,
    )
    write_json(campaign_dir(campaign_id) / ARTIFACT, body)
    return body


# --------------------------------------------------------------------------- #
def record_forward_selection(decision: dict,
                             campaign_id: str = CAMPAIGN_ID) -> dict:
    """Record a choice made AFTER seeing forward results. Append-only.

    Every entry is a debit against the credibility of whatever it selected.
    Nothing prevents a legitimate decision from being made; the ledger simply
    makes it impossible to make one invisibly.
    """
    p = campaign_dir(campaign_id) / FORWARD_SELECTION_LEDGER
    prior = read_json(p, default=None) or {}
    rows = list(prior.get("rows") or [])
    row = dict(decision)
    row.setdefault("recorded_at_utc", CK.iso(CK.now_utc()))
    row.setdefault("kind", "FORWARD_SELECTION")
    rows.append(row)
    body = artifact_body(
        "r46_forward_selection_ledger/1", CALCULATION_OWNER,
        n_forward_selections=len(rows),
        why_this_exists=(
            "choosing a threshold, a version or a challenger after seeing "
            "forward results is a selection over the forward evidence and "
            "inflates it exactly the way a historical screen does"),
        rows=rows)
    write_json(p, body)
    return body


def forward_selections(campaign_id: str = CAMPAIGN_ID) -> dict:
    body = read_json(campaign_dir(campaign_id) / FORWARD_SELECTION_LEDGER,
                     default=None)
    if body is None:
        return {"n_forward_selections": 0, "rows": [],
                "note": "no choice has yet been made after seeing forward "
                        "results - correct, because no forward result has "
                        "matured"}
    return body


def prospective(campaign_id: str = CAMPAIGN_ID) -> dict:
    from . import ledger as LG
    preds = LG.predictions(campaign_id)
    outs = LG.outcomes(campaign_id)
    return {
        "schema": "r46_prospective_evidence_ledger/1",
        "calculation_owner": CALCULATION_OWNER,
        "forward_predictions_emitted": len(preds),
        "forward_predictions_matured": len(outs),
        "forward_selections": forward_selections(campaign_id).get(
            "n_forward_selections", 0),
        "these_are_not_search_trials": True,
        "why": ("a forward observation cannot be re-drawn, re-parameterised "
                "or re-selected; that is the entire reason this release "
                "exists"),
    }
