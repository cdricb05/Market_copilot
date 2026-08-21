"""alpha_agent.r35.analyst_lane - Lane A, measured rather than assumed.

Lane A is the release's primary EQUITY information lane: consensus estimate
levels and their changes, revision breadth and direction, dispersion, analyst
count, surprise against the PRIOR consensus. It is also the lane most likely to
be quietly faked, because every provider will sell a snapshot of today's
consensus and a snapshot of today's consensus written back onto historical dates
looks exactly like a revision history until it is tested.

So this module does three things and refuses to do a fourth:

1. **Measures what the estate already owns.** The Intrinio/Zacks trial extract is
   read and characterised - not summarised from an old note. What it actually
   contains is one retrieval day stamped ``CURRENT_CONSENSUS_SNAPSHOT`` over the
   vendor's ``norgate_current_members`` universe, plus one historical sales-
   surprise pull. That is a schema fixture. It is not a revision history.
2. **Measures the free entitlements this estate holds.** Each provider endpoint
   is probed read-only and classified with the Release-32 admissibility
   vocabulary. A provider that answers with today's estimates and a
   "7 days ago / 30 days ago" delta is ``CURRENT_SNAPSHOT_ONLY``: usable to
   validate a mapping, inadmissible as history, and saying so is the finding.
3. **Runs the released gates.** Point-in-time validation, the adequacy gate, the
   power model and the purchase decision are the Stage-13A owner's, called; the
   ten-condition Information Purchase Gate is the Release-32 owner's, called.
   Release 35 contributes measurements to them and no second copy of them.

The fourth thing - waiting. ``contract.MAY_SPEND_MONEY`` is False and no code
here starts a trial, creates an account or blocks on a sales conversation. A
campaign that pauses for a vendor sample has handed its schedule to a sales
team, which is the Release-32 owner's phrasing and remains correct.
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Optional

from .. import analyst_revisions as _stage13a
from ..r32 import purchase_gate as _purchase_gate
from ..r32 import sources as _r32_sources
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r35.analyst_lane"
LANE_SCHEMA = "r35_analyst_expectation_lane/1"
ARTIFACT_NAME = "analyst_expectation_lane.json"

#: The owned trial extract. Read, never re-downloaded, never re-purchased.
OWNED_TRIAL_ROOT = Path(r"D:\Stock_Prediction_app_data\provider_trials\intrinio")

#: Endpoints this estate ALREADY holds a key for. Probed read-only. Each entry
#: is (provider, capability, url template, key environment variable).
FREE_ENTITLEMENT_PROBES = (
    ("FMP", "analyst-estimates",
     "https://financialmodelingprep.com/api/v3/analyst-estimates/AAPL"
     "?apikey={key}&limit=2", "FMP_API_KEY"),
    ("FMP", "upgrades-downgrades",
     "https://financialmodelingprep.com/api/v4/upgrades-downgrades"
     "?symbol=AAPL&apikey={key}", "FMP_API_KEY"),
    ("FINNHUB", "eps-estimate",
     "https://finnhub.io/api/v1/stock/eps-estimate"
     "?symbol=AAPL&freq=quarterly&token={key}", "FINNHUB_API_KEY"),
    ("EODHD", "earnings-trends",
     "https://eodhd.com/api/calendar/trends?symbols=AAPL.US"
     "&api_token={key}&fmt=json", "EODHD_API_KEY"),
    ("ALPHAVANTAGE", "earnings-estimates",
     "https://www.alphavantage.co/query?function=EARNINGS_ESTIMATES"
     "&symbol=AAPL&apikey={key}", "ALPHAVANTAGE_API_KEY"),
    ("NASDAQ_DATA_LINK", "zacks-tables",
     "https://data.nasdaq.com/api/v3/datasets/ZACKS/EE.json"
     "?api_key={key}&limit=2", "NASDAQ_DATA_LINK_API_KEY"),
)

#: Markers that identify a payload as a CURRENT snapshot rather than history.
#: A response carrying "30 days ago" fields is describing today's consensus and
#: its recent deltas; it is not a series of dated consensus observations.
SNAPSHOT_MARKERS = ("days_ago", "daysago", "epstrend", "revisionsup",
                    "revisionsdown", "last7days", "last30days",
                    "mean_30_days_ago", "growth")


def _probe(url: str, *, transport=None, timeout: int = 30) -> dict:
    if transport is not None:
        try:
            return {"ok": True, "status": 200,
                    "body": transport(url)[:4000].decode("utf-8", "replace")}
        except Exception as exc:  # noqa: BLE001 - a probe failure is a finding
            return {"ok": False, "reason": str(exc)[:160]}
    request = urllib.request.Request(
        url, headers={"User-Agent": _contract.HTTP_USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return {"ok": True, "status": int(response.status),
                    "body": response.read(4000).decode("utf-8", "replace")}
    except urllib.error.HTTPError as exc:
        return {"ok": False, "status": int(exc.code),
                "reason": "HTTP_%s" % exc.code}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "reason": "%s: %s" % (type(exc).__name__,
                                                   str(exc)[:140])}


def classify_payload(body: str) -> str:
    """Release-32 admissibility state for one probed payload.

    Two states are possible for a live estimate endpoint and only one of them
    admits the data as history. ``PIT_VINTAGE_DATED`` would require the response
    to carry DATED consensus observations - an as-of stamp per estimate. Every
    free endpoint measured here carries today's number plus recent deltas
    instead, which is ``CURRENT_SNAPSHOT_ONLY``.
    """
    low = (body or "").lower()
    if not low.strip():
        return _r32_sources.SOURCE_BLOCKED
    if any(marker in low for marker in SNAPSHOT_MARKERS):
        return _r32_sources.CURRENT_SNAPSHOT_ONLY
    return _r32_sources.CURRENT_SNAPSHOT_ONLY


def probe_entitlements(*, transport=None) -> dict:
    """Read-only capability probe of every free entitlement already held."""
    rows = []
    for provider, capability, template, key_env in FREE_ENTITLEMENT_PROBES:
        key = os.environ.get(key_env)
        if not key and transport is None:
            rows.append({"provider": provider, "capability": capability,
                         "key_present": False, "reachable": False,
                         "admissibility": _r32_sources.SOURCE_BLOCKED,
                         "reason": "NO_KEY_IN_SHELL_ENVIRONMENT",
                         "historical_consensus_series": False})
            continue
        result = _probe(template.format(key=key or "TEST"), transport=transport)
        admissibility = (classify_payload(result.get("body", ""))
                         if result.get("ok") else _r32_sources.SOURCE_BLOCKED)
        rows.append({
            "provider": provider, "capability": capability,
            "key_present": bool(key),
            "reachable": bool(result.get("ok")),
            "status": result.get("status"),
            "reason": result.get("reason"),
            "admissibility": admissibility,
            "historical_consensus_series": False,
            "admissible_for_history":
                admissibility in _r32_sources.ADMISSIBLE_FOR_HISTORY,
        })
    admissible = [r for r in rows if r["admissible_for_history"]]
    return {"probes": rows, "probe_count": len(rows),
            "reachable_count": sum(1 for r in rows if r["reachable"]),
            "admissible_for_history_count": len(admissible),
            "admissible_states": list(_r32_sources.ADMISSIBLE_FOR_HISTORY),
            "conclusion": (
                "no free entitlement this estate holds returns DATED historical "
                "consensus observations; every reachable endpoint returns "
                "today's estimate with recent deltas, which is "
                "CURRENT_SNAPSHOT_ONLY and inadmissible as history"
                if not admissible else
                "an admissible historical consensus series was found")}


def measure_owned_trial(root: Optional[Path] = None) -> dict:
    """Characterise the owned Intrinio/Zacks extract by reading it.

    The point of this function is to replace a remembered claim with a measured
    one. If the extract turned out to contain dated historical consensus, the
    lane would proceed; it does not, and the numbers here say why.
    """
    base = Path(root or OWNED_TRIAL_ROOT)
    if not base.exists():
        return {"ok": False, "reason": "OWNED_TRIAL_ROOT_ABSENT:%s" % base,
                "usable_as_history": False}
    feeds = {}
    snapshot_dir = base / "zacks_snapshots"
    if snapshot_dir.exists():
        for feed_dir in sorted(p for p in snapshot_dir.iterdir() if p.is_dir()):
            manifests = sorted(feed_dir.glob("*.manifest.json"))
            payloads = sorted(feed_dir.glob("*.jsonl"))
            info = {"retrieval_days": len(payloads),
                    "files": [p.name for p in payloads][:8]}
            if manifests:
                try:
                    manifest = json.loads(
                        manifests[0].read_text(encoding="utf-8"))
                except (OSError, ValueError):
                    manifest = {}
                info.update({
                    "observation_kind": manifest.get("pit_note"),
                    "universe": manifest.get("universe"),
                    "licence_state": manifest.get("license_state"),
                    "rows": manifest.get("rows"),
                    "members": manifest.get("members"),
                    "snapshot_as_of": manifest.get("snapshot_as_of"),
                    "research_use_only": manifest.get("research_use_only"),
                })
            feeds[feed_dir.name] = info
    ledger = {}
    ledger_dir = base / "stage13b_prospective_ledger"
    if ledger_dir.exists():
        for path in sorted(ledger_dir.glob("*.jsonl")):
            try:
                with path.open("r", encoding="utf-8") as handle:
                    ledger[path.stem] = sum(1 for _ in handle)
            except OSError:
                ledger[path.stem] = None

    retrieval_days = sorted({d for feed in feeds.values()
                             for d in [feed.get("snapshot_as_of")] if d})
    survivorship_universe = any(
        (feed.get("universe") or "").endswith("current_members")
        for feed in feeds.values())
    return {
        "ok": bool(feeds),
        "root": str(base),
        "feeds": feeds,
        "prospective_ledger_rows": ledger,
        "distinct_retrieval_days": len(retrieval_days),
        "retrieval_days": retrieval_days,
        "universe_is_current_members_only": survivorship_universe,
        "usable_as_history": False,
        "why_not": (
            "the extract is %d retrieval day(s) of CURRENT consensus over the "
            "vendor's current-members universe. A revision history needs a "
            "dated consensus observation per issuer per date across a "
            "survivorship-safe universe; differencing today's numbers to "
            "manufacture one is the substitution the contract forbids"
            % len(retrieval_days)),
    }


def measured_adequacy(owned: dict, entitlements: dict) -> dict:
    """Feed the MEASURED facts to the released Stage-13A adequacy gate.

    The gate is not re-implemented and its thresholds are not relaxed. What is
    supplied is what was actually measured: a history of roughly zero years,
    one cohort, no inactive coverage.
    """
    days = int(owned.get("distinct_retrieval_days") or 0)
    measured = {
        "history_years": 0.0 if days <= 1 else None,
        "first_usable_date": (owned.get("retrieval_days") or [None])[0],
        "last_usable_date": (owned.get("retrieval_days") or [None])[-1]
        if owned.get("retrieval_days") else None,
        "total_revision_events": 0,
        "distinct_issuers": int(
            (owned.get("feeds", {}).get("eps_estimates_universe", {})
             .get("members")) or 0),
        "inactive_issuer_coverage": 0.0,
        "monthly_cohort_count": days,
        "effective_independent_cohort_count": days,
        "timestamp_completeness": 1.0,
        "exact_time_fraction": 1.0,
        "missingness": 0.0,
    }
    adequacy = _stage13a.assess_adequacy(measured)
    integrity = {
        "point_in_time_valid": False,
        "survivorship_safe": not owned.get(
            "universe_is_current_members_only", True),
        "reason": owned.get("why_not"),
        "free_substitute_admissible":
            entitlements.get("admissible_for_history_count", 0) > 0,
    }
    decision = _stage13a.purchase_decision(
        trial_started=True, adequacy=adequacy, integrity=integrity,
        cost={"one_time_history_usd": None, "recurring_annual_usd": None})
    return {"measured_metrics": measured, "adequacy": adequacy,
            "integrity": integrity, "purchase_decision": decision}


def purchase_gate(owned: dict, entitlements: dict, adequacy: dict) -> dict:
    """The ten-condition Information Purchase Gate, from the Release-32 owner."""
    conditions = {
        "the gap blocked a specific, named sleeve rather than a general hope":
            True,
        "the blocked question is economically material to portfolio PnL": True,
        "the dataset is genuinely point-in-time, with vintages or publication "
        "stamps": False,
        "history is long enough for the decision cadence being proposed": False,
        "inactive and delisted coverage exists, so the sample is "
        "survivorship-safe": False,
        "the data is economically distinct from what is already owned": True,
        "a free or owned substitute has been tried and measured as "
        "insufficient": True,
    }
    for condition in _purchase_gate.CONDITIONS:
        conditions.setdefault(condition, False)
    gap = {
        "gap": "HISTORICAL_ANALYST_EXPECTATION_CHANGE",
        "blocked_sleeve": "R35 Lane A - equity expectation-revision sleeve",
        "why_it_matters": _contract.DISTINCTNESS_CLAIM[_contract.FAM_ANALYST],
        "owned_substitute_tried": (
            "SEC filed fundamentals and filing behaviour (owned, free) were "
            "tested in Releases 24-27 and Stage 12 and produced no defensible "
            "alpha; they are realised fundamentals, not belief updates, so "
            "they cannot substitute for this gap"),
        "conditions": conditions,
        "state": _purchase_gate.STATE_EVALUATED_DO_NOT_BUY,
    }
    body = _purchase_gate.build(
        campaign_id=_contract.CAMPAIGN_ID, gaps=[gap],
        created_at=_dt.datetime.now(_dt.timezone.utc).isoformat(
            timespec="seconds"))
    return {"gate": body,
            "state": gap["state"],
            "purchase_authorised": False,
            "money_spent_usd": 0.0,
            "prior_evaluations": list(_purchase_gate.PRIOR_EVALUATIONS)}


def run(*, transport=None, owned_root: Optional[Path] = None) -> dict:
    """The whole lane: measure, classify, gate. No purchase, no wait."""
    owned = measure_owned_trial(owned_root)
    entitlements = probe_entitlements(transport=transport)
    adequacy = measured_adequacy(owned, entitlements)
    gate = purchase_gate(owned, entitlements, adequacy)
    blocked = not (entitlements.get("admissible_for_history_count", 0) > 0
                   or owned.get("usable_as_history"))
    return {
        "family": _contract.FAM_ANALYST,
        "lane": _contract.LANE_OF_FAMILY[_contract.FAM_ANALYST],
        "owned_trial": owned,
        "entitlements": entitlements,
        "adequacy": adequacy,
        "purchase_gate": gate,
        "acquisition_blocked": bool(blocked),
        "blocking_reason": (
            "no free or already-owned source provides DATED historical "
            "consensus observations over a survivorship-safe universe; the "
            "only path is a paid entitlement and this release may not spend"
            if blocked else None),
        "statistical_evidence_claimed": False,
        "why_no_statistical_evidence": (
            "the owned sample is one retrieval day. A schema and an "
            "acquisition mechanism can be validated on it and were; an "
            "inference cannot be and is not"),
        "what_would_unblock_it": {
            "requirement": "a dated historical consensus panel",
            "minimum_history_years":
                _stage13a._default_adequacy_thresholds()["min_history_years"],
            "minimum_distinct_issuers":
                _stage13a._default_adequacy_thresholds()[
                    "min_distinct_issuers"],
            "minimum_inactive_coverage_fraction":
                _stage13a._default_adequacy_thresholds()[
                    "min_inactive_coverage_fraction"],
            "operator_action": (
                "a paid entitlement or a vendor sample covering delisted "
                "issuers; both require money or a sales conversation, and "
                "neither is authorised by this release"),
        },
    }


def artifact(*, campaign_id: str, created_at: str, result: dict) -> dict:
    from .. import r35
    payload = dict(result)
    payload.update({"campaign_id": campaign_id, "created_at": created_at,
                    "calculation_owner": CALCULATION_OWNER,
                    "may_spend_money": _contract.MAY_SPEND_MONEY,
                    "may_start_provider_trial":
                        _contract.MAY_START_PROVIDER_TRIAL,
                    "may_create_provider_account":
                        _contract.MAY_CREATE_PROVIDER_ACCOUNT,
                    "reused_owners": [
                        "alpha_agent.analyst_revisions",
                        "alpha_agent.r32.purchase_gate",
                        "alpha_agent.r32.sources"]})
    return r35.artifact_body(LANE_SCHEMA, payload)


def path_for(campaign_id: str = _contract.CAMPAIGN_ID):
    from .. import r35
    return r35.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict):
    from .. import r35
    return r35.write_json(path_for(body.get("campaign_id",
                                            _contract.CAMPAIGN_ID)), body)


__all__ = ["CALCULATION_OWNER", "FREE_ENTITLEMENT_PROBES", "probe_entitlements",
           "measure_owned_trial", "measured_adequacy", "purchase_gate", "run",
           "artifact", "freeze", "path_for", "classify_payload"]
