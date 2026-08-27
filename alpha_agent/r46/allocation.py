"""alpha_agent.r46.allocation - THE shadow target owner. Zero-base, frozen rules.

Every eligible decision session asks the canonical question at research
scale: if all shadow research capital were cash right now, which frozen
strategies should own it? An existing allocation earns no privilege beyond
what the rules below give it; there is no "keep what we had".

Four PREDECLARED policies, each a frozen rule with a version. If a rule
changes, the policy gets a new version and its own comparison history; the
old one is never edited.

``EQUAL_WEIGHT_ELIGIBLE_v1``   1/N over eligible strategies, fully deployed.
``EQUAL_RISK_v1``              weight proportional to 1/vol prior, fully
                               deployed. No evidence discount, no cluster
                               penalty - the risk-balanced control.
``EVIDENCE_DISCOUNTED_DIVERSIFIED_v1``   the CANONICAL policy:
    score_i = (1/vol_i) x evidence_score_i x edge_discount_i
    evidence_score_i = 0.10 + 0.90 x min(1, effective_i / required_i)
    edge_discount_i  = 1.0 (no matured evidence, or positive net alpha)
                       0.5 (early evidence negative), 0.25 (ECONOMIC_WATCH),
                       0.0 (ECONOMIC_KILL_CANDIDATE or FORWARD_REJECTED)
    redundancy       = each score divided by the size of its dependence
                       cluster (a cluster counts as one bet)
    deployment       = 25% + 75% x mean evidence_score (early capital is
                       SMALL and grows only as evidence accrues)
    concentration    = max 15% per strategy, 25% per cluster, 40% per asset
                       class; excess goes to cash, never redistributed
``CASH_CONTROL_v1``            100% cash at the risk-free rate.

NO HINDSIGHT. ``decide`` receives the evidence view assembled from outcomes
whose maturity is on or before the decision session and nothing later; the
weights it freezes apply to sessions strictly after the decision, and they
are appended to a ledger keyed by (policy, decision_session) that is never
rewritten. Weights are never optimised on accumulating forward results.

Research only. A shadow weight is not a target, a proposal or an order.
"""
from __future__ import annotations

import datetime as _dt
import math
from pathlib import Path

from . import CAMPAIGN_ID, artifact_body, campaign_dir, sha, write_json
from . import challengers as CH
from . import clock as CK
from . import contract as C
from . import strategy_pnl as SP

CALCULATION_OWNER = "alpha_agent.r46.allocation"

ARTIFACT = "R46_4_SHADOW_ALLOCATION.json"
LEDGER = "r46_4_shadow_allocations.json"

POLICY_EQUAL = "EQUAL_WEIGHT_ELIGIBLE_v1"
POLICY_RISK = "EQUAL_RISK_v1"
POLICY_EVIDENCE = "EVIDENCE_DISCOUNTED_DIVERSIFIED_v1"
POLICY_CASH = "CASH_CONTROL_v1"
POLICIES = (POLICY_EQUAL, POLICY_RISK, POLICY_EVIDENCE, POLICY_CASH)
CANONICAL_POLICY = POLICY_EVIDENCE

POLICY_RULES = {
    POLICY_EQUAL: {"rule": "1/N over eligible strategies; 100% deployed",
                   "evidence_discount": False, "cluster_penalty": False,
                   "caps": None, "deployment": 1.0},
    POLICY_RISK: {"rule": "weight proportional to 1/annual_vol prior; "
                          "100% deployed",
                  "evidence_discount": False, "cluster_penalty": False,
                  "caps": None, "deployment": 1.0},
    POLICY_EVIDENCE: {"rule": "risk-balanced x evidence score x edge "
                              "discount / cluster size; deployment 25%%+75%% x "
                              "mean evidence; caps 15/25/40",
                      "evidence_discount": True, "cluster_penalty": True,
                      "caps": {"strategy": 0.15, "cluster": 0.25,
                               "asset_class": 0.40},
                      "deployment": "0.25 + 0.75 x mean evidence score"},
    POLICY_CASH: {"rule": "100% cash at the risk-free rate",
                  "evidence_discount": None, "cluster_penalty": None,
                  "caps": None, "deployment": 0.0},
}

EVIDENCE_FLOOR = SP.SCALING_RULES["evidence_floor_share"]
DEPLOYMENT_FLOOR = 0.25


def _desk():
    from paper_trader.api import paper_trading_desk as desk
    return desk


def ledger_dir(campaign_id: str = CAMPAIGN_ID) -> Path:
    from . import trades as TR
    return TR.shadow_dir(campaign_id)


def rows(campaign_id: str = CAMPAIGN_ID) -> list:
    return _desk()._read_ledger(ledger_dir(campaign_id), LEDGER)


def latest(policy_id: str, before: _dt.date = None,
           campaign_id: str = CAMPAIGN_ID) -> dict:
    """The latest decision for ``policy_id`` STRICTLY BEFORE ``before``."""
    best = None
    for r in rows(campaign_id):
        if r.get("policy_id") != policy_id:
            continue
        if before is not None and str(r.get("decision_session")) >= str(before):
            continue
        if best is None or r["decision_session"] > best["decision_session"]:
            best = r
    return best or {}


def funding_for(challenger_id: str, entry_session: _dt.date, horizon: int,
                campaign_id: str = CAMPAIGN_ID) -> dict:
    """What each policy had decided for this strategy BEFORE the entry."""
    out = {}
    for pid in POLICIES:
        dec = latest(pid, before=entry_session, campaign_id=campaign_id)
        if not dec:
            continue
        w = float((dec.get("weights") or {}).get(challenger_id) or 0.0)
        out[pid] = {"weight": w, "decision_session": dec["decision_session"],
                    "nav_at_decision": dec.get("nav_at_decision")}
    return out


# --------------------------------------------------------------------------- #
# Eligibility and the frozen scores
# --------------------------------------------------------------------------- #
def eligible(entry: dict, evidence: dict, econ_state: str) -> dict:
    reasons = []
    if entry.get("state") == C.DATA_BLOCKED:
        reasons.append("DATA_BLOCKED")
    if any(v.get("state") == C.FORWARD_REJECTED
           for v in (evidence.get("cells") or {}).values()):
        reasons.append("FORWARD_REJECTED")
    if econ_state == SP.ECON_KILL_CANDIDATE:
        reasons.append("ECONOMIC_KILL_CANDIDATE")
    return {"eligible": not reasons, "reasons": reasons}


def evidence_score(evidence: dict) -> float:
    best = 0.0
    for v in (evidence.get("cells") or {}).values():
        need = max(1, int(v.get("required_effective_independent") or 1))
        eff = int(v.get("effective_independent") or 0)
        best = max(best, min(1.0, float(eff) / float(need)))
    return EVIDENCE_FLOOR + (1.0 - EVIDENCE_FLOOR) * best


def edge_discount(evidence: dict, econ_state: str) -> float:
    if econ_state == SP.ECON_KILL_CANDIDATE:
        return SP.SCALING_RULES["weight_when_kill_candidate"]
    if econ_state == SP.ECON_WATCH:
        return SP.SCALING_RULES["edge_discount_when_watch"]
    matured = sum(int(v.get("raw_matured") or 0)
                  for v in (evidence.get("cells") or {}).values())
    mean_alpha = evidence.get("mean_net_alpha_bps")
    if matured >= C.EARLY_EVIDENCE_MIN_MATURED and mean_alpha is not None \
            and mean_alpha <= 0:
        return SP.SCALING_RULES["edge_discount_when_early_evidence_negative"]
    return 1.0


def _cap(weights: dict, entries: dict, caps: dict) -> dict:
    """Apply concentration caps; excess becomes cash (never redistributed)."""
    w = dict(weights)
    for cid in list(w):
        w[cid] = min(w[cid], caps["strategy"])
    for key_fn, cap in ((lambda e: CH.cluster_for(e), caps["cluster"]),
                        (lambda e: e.get("asset_class"), caps["asset_class"])):
        groups: dict = {}
        for cid in w:
            groups.setdefault(key_fn(entries[cid]), []).append(cid)
        for members in groups.values():
            tot = sum(w[c] for c in members)
            if tot > cap and tot > 0:
                f = cap / tot
                for c in members:
                    w[c] *= f
    return w


def target(policy_id: str, entries: dict, evidence: dict, vols: dict,
           econ: dict) -> dict:
    """Weights for ONE policy from the frozen rule. Pure function."""
    elig = {cid: eligible(e, evidence.get(cid, {}), econ.get(cid))
            for cid, e in entries.items()}
    ok = [cid for cid, v in elig.items() if v["eligible"]]
    detail = {}
    if policy_id == POLICY_CASH or not ok:
        return {"weights": {}, "cash_weight": 1.0, "deployment": 0.0,
                "eligible": ok, "ineligible": {cid: v["reasons"]
                                               for cid, v in elig.items()
                                               if not v["eligible"]},
                "detail": {}}
    if policy_id == POLICY_EQUAL:
        w = {cid: 1.0 / len(ok) for cid in ok}
        deployment = 1.0
    elif policy_id == POLICY_RISK:
        inv = {cid: 1.0 / max(1e-6, float(vols[cid])) for cid in ok}
        tot = sum(inv.values())
        w = {cid: v / tot for cid, v in inv.items()}
        deployment = 1.0
    elif policy_id == POLICY_EVIDENCE:
        scores, ev_scores = {}, {}
        cluster_n: dict = {}
        for cid in ok:
            cluster_n[CH.cluster_for(entries[cid])] = \
                cluster_n.get(CH.cluster_for(entries[cid]), 0) + 1
        for cid in ok:
            es = evidence_score(evidence.get(cid, {}))
            ed = edge_discount(evidence.get(cid, {}), econ.get(cid))
            n_cl = cluster_n[CH.cluster_for(entries[cid])]
            s = (1.0 / max(1e-6, float(vols[cid]))) * es * ed / float(n_cl)
            scores[cid] = s
            ev_scores[cid] = es
            detail[cid] = {"inverse_vol": 1.0 / max(1e-6, float(vols[cid])),
                           "evidence_score": es, "edge_discount": ed,
                           "cluster_size": n_cl, "score": s}
        tot = sum(scores.values())
        deployment = (DEPLOYMENT_FLOOR + (1.0 - DEPLOYMENT_FLOOR)
                      * (sum(ev_scores.values()) / len(ev_scores)))
        w = ({cid: s / tot * deployment for cid, s in scores.items()}
             if tot > 0 else {})
        w = _cap(w, entries, POLICY_RULES[POLICY_EVIDENCE]["caps"])
    else:
        raise ValueError("unknown policy %r" % policy_id)
    w = {cid: round(v, 10) for cid, v in w.items() if v > 0}
    invested = float(sum(w.values()))
    return {"weights": w, "cash_weight": max(0.0, 1.0 - invested),
            "deployment": deployment, "eligible": ok,
            "ineligible": {cid: v["reasons"] for cid, v in elig.items()
                           if not v["eligible"]},
            "detail": detail}


# --------------------------------------------------------------------------- #
def decide(as_of: _dt.date, entries: dict, evidence: dict, vols: dict,
           econ: dict, nav_by_policy: dict, campaign_id: str = CAMPAIGN_ID,
           decided_at: _dt.datetime = None) -> dict:
    """Freeze every policy's target for the sessions AFTER ``as_of``.

    Idempotent on (policy_id, decision_session): a second call for the same
    session appends nothing and returns the frozen rows.
    """
    now = decided_at or CK.now_utc()
    have = {(r.get("policy_id"), str(r.get("decision_session")))
            for r in rows(campaign_id)}
    inputs_hash = sha({"evidence": evidence, "vols": vols, "econ": econ})
    new = []
    for pid in POLICIES:
        if (pid, str(as_of)) in have:
            continue
        t = target(pid, entries, evidence, vols, econ)
        new.append({
            "policy_id": pid,
            "policy_rule": POLICY_RULES[pid]["rule"],
            "decision_session": str(as_of),
            "applies_from_session": "strictly after decision_session",
            "decided_at_utc": CK.iso(now),
            "decided_at_utc_precise": CK.iso_precise(now),
            "nav_at_decision": nav_by_policy.get(pid),
            "weights": t["weights"],
            "cash_weight": t["cash_weight"],
            "deployment": t["deployment"],
            "n_allocated": len(t["weights"]),
            "eligible": t["eligible"],
            "ineligible": t["ineligible"],
            "detail": t["detail"],
            "inputs_hash": inputs_hash,
            "evidence_cutoff": "outcomes matured on or before decision_session",
            "zero_base": True,
            "hindsight": False,
            "weights_optimised_on_forward_results": False,
            "is_a_target": False, "is_a_proposal": False, "is_an_order": False,
            "calculation_owner": CALCULATION_OWNER,
        })
    appended = (_desk()._append_ledger(ledger_dir(campaign_id), LEDGER, new)
                if new else [])
    current = {pid: latest(pid, before=None, campaign_id=campaign_id)
               for pid in POLICIES}
    body = artifact_body(
        "r46_4_shadow_allocation/1", CALCULATION_OWNER,
        as_of=str(as_of),
        decided_at_utc=CK.iso(now),
        canonical_policy=CANONICAL_POLICY,
        policies=list(POLICIES),
        policy_rules={k: dict(v) for k, v in POLICY_RULES.items()},
        n_appended=len(appended),
        idempotent=True,
        current={pid: {k: v for k, v in (row or {}).items()
                       if k not in ("detail",)}
                 for pid, row in current.items()},
        canonical_weights=(current.get(CANONICAL_POLICY) or {}).get("weights"),
        canonical_cash_weight=(current.get(CANONICAL_POLICY) or {}).get(
            "cash_weight"),
        canonical_detail=(current.get(CANONICAL_POLICY) or {}).get("detail"),
        top_allocations=sorted(
            ((current.get(CANONICAL_POLICY) or {}).get("weights") or {})
            .items(), key=lambda kv: -kv[1])[:10],
        chain=_desk().verify_ledger(ledger_dir(campaign_id), LEDGER),
        no_hindsight=True,
        research_only=True,
    )
    write_json(campaign_dir(campaign_id) / ARTIFACT, body)
    return body


__all__ = ["CALCULATION_OWNER", "ARTIFACT", "LEDGER", "POLICIES",
           "POLICY_EQUAL", "POLICY_RISK", "POLICY_EVIDENCE", "POLICY_CASH",
           "CANONICAL_POLICY", "POLICY_RULES", "rows", "latest",
           "funding_for", "eligible", "evidence_score", "edge_discount",
           "target", "decide"]
