"""
alpha_agent/event_clustering.py — Stage 3.5 deterministic cross-source event
clustering.

Groups the SAME real-world event across heterogeneous normalized records — RSS/
Atom (NEWS_EVENT / REGULATORY_EVENT / PRESS_RELEASE), EODHD NEWS_EVENT, SEC
FILING_EVENT / INSIDER_FILING, Nasdaq TRADING_HALT and CORPORATE_ACTION — using
ONLY deterministic similarity. No LLM, no fuzzy ML, no wall-clock in the cluster
identity.

Rules:
  * a shared canonical URL or a shared source-native id is an EXACT match;
  * otherwise records cluster only when they share a resolved ticker AND their
    normalized-title token Jaccard clears a threshold AND they fall inside a
    bounded time window — a shared ticker ALONE never clusters two events;
  * conflicting facts across members (e.g. differing halt reason codes) are
    surfaced, never merged away;
  * the representative is chosen deterministically (most authoritative source,
    then earliest availability, then record id).

Immutable Stage 2 records are only ever REFERENCED by id — never copied or
mutated.
"""
from __future__ import annotations

from typing import Any, Optional

from .feed_contracts import (
    CLUSTER_ALGO_VERSION, canonical_link, normalize_title, title_tokens,
)
from .research_importers import sha256_text
from .source_contracts import (
    RT_CORPORATE_ACTION, RT_INSIDER_FILING, RT_PRESS_RELEASE,
    RT_REGULATORY_EVENT, RT_TRADING_HALT,
)

CC_EXACT = "EXACT"
CC_HIGH = "HIGH"
CC_MEDIUM = "MEDIUM"
CC_LOW = "LOW"
CC_NOT_CLUSTERED = "NOT_CLUSTERED"
_CONF_RANK = {CC_NOT_CLUSTERED: 0, CC_LOW: 1, CC_MEDIUM: 2, CC_HIGH: 3, CC_EXACT: 4}

# Source authority for representative selection (higher = more authoritative).
_SOURCE_RANK = {"sec_edgar": 5, "nasdaq_trader": 4, "rss_atom": 3,
                "eodhd": 2, "fred_alfred": 1}
_PRIMARY_SOURCES = ("sec_edgar", "nasdaq_trader")
_PRIMARY_TRUST = ("PRIMARY_OFFICIAL", "OFFICIAL_AGENCY")
_COMPANY_CATEGORIES = ("COMPANY_IR", "COMPANY_NEWSROOM")
# Payload fields whose disagreement across members is a genuine factual conflict.
_CONFLICT_FIELDS = ("reason_code", "resumption_trade_time", "resumption_date",
                    "action_type", "form")


def _date_of(rec: dict) -> str:
    return str(rec.get("available_at") or rec.get("effective_at") or "")[:10]


def _title_of(rec: dict) -> str:
    payload = rec.get("normalized_payload") or {}
    for key in ("title", "issue_name", "headline", "description"):
        val = payload.get(key)
        if val:
            return str(val)
    return str(rec.get("event_type") or rec.get("record_type") or "")


def _features(rec: dict) -> dict:
    payload = rec.get("normalized_payload") or {}
    link = payload.get("canonical_link") or payload.get("url") or ""
    title = _title_of(rec)
    return {
        "record_id": rec.get("record_id"),
        "record_type": rec.get("record_type"),
        "source_id": rec.get("source_id"),
        "ticker": (str(rec.get("ticker")).upper() if rec.get("ticker") else None),
        "date": _date_of(rec),
        "available_at": str(rec.get("available_at") or rec.get("effective_at") or ""),
        "title": title,
        "norm_title": normalize_title(title),
        "tokens": title_tokens(title),
        "canonical_link": canonical_link(link) if link else "",
        "native_id": rec.get("source_native_id") or "",
        "trust_level": payload.get("trust_level"),
        "source_category": payload.get("source_category"),
        "payload": payload,
    }


def _jaccard(a: frozenset, b: frozenset) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def _days_apart(d1: str, d2: str) -> Optional[int]:
    import datetime as _dt
    try:
        return abs((_dt.date.fromisoformat(d1) - _dt.date.fromisoformat(d2)).days)
    except ValueError:
        return None


def _pair_confidence(a: dict, b: dict, *, window_days: int,
                     high: float, medium: float) -> str:
    if a["canonical_link"] and a["canonical_link"] == b["canonical_link"]:
        return CC_EXACT
    if a["native_id"] and a["native_id"] == b["native_id"]:
        return CC_EXACT
    # Beyond exact link/id, a shared ticker is REQUIRED but never sufficient.
    if not a["ticker"] or a["ticker"] != b["ticker"]:
        return CC_NOT_CLUSTERED
    delta = _days_apart(a["date"], b["date"])
    if delta is None or delta > window_days:
        return CC_NOT_CLUSTERED
    sim = _jaccard(a["tokens"], b["tokens"])
    if sim >= high:
        return CC_HIGH
    if sim >= medium:
        return CC_MEDIUM
    return CC_NOT_CLUSTERED


class _UnionFind:
    def __init__(self, n: int) -> None:
        self.parent = list(range(n))

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[max(ra, rb)] = min(ra, rb)


def cluster_events(records: list[dict], *, window_days: int = 2,
                   high_threshold: float = 0.6, medium_threshold: float = 0.4,
                   now_iso: str = "", algo_version: str = CLUSTER_ALGO_VERSION
                   ) -> list[dict]:
    """Deterministically cluster records. Returns a sorted list of cluster dicts
    (see module docstring for fields). Input records are never mutated."""
    feats = [_features(r) for r in records if r.get("record_id")]
    feats.sort(key=lambda f: f["record_id"])
    n = len(feats)
    uf = _UnionFind(n)
    best_edge: dict[tuple[int, int], str] = {}
    for i in range(n):
        for j in range(i + 1, n):
            conf = _pair_confidence(feats[i], feats[j], window_days=window_days,
                                    high=high_threshold, medium=medium_threshold)
            if conf != CC_NOT_CLUSTERED:
                uf.union(i, j)
                best_edge[(i, j)] = conf

    groups: dict[int, list[int]] = {}
    for idx in range(n):
        groups.setdefault(uf.find(idx), []).append(idx)

    clusters: list[dict] = []
    for root in sorted(groups):
        members_idx = sorted(groups[root], key=lambda k: feats[k]["record_id"])
        members = [feats[k] for k in members_idx]
        member_ids = [m["record_id"] for m in members]
        # Strongest edge confidence inside the cluster (singletons NOT_CLUSTERED).
        conf = CC_NOT_CLUSTERED
        if len(members_idx) > 1:
            for a in range(len(members_idx)):
                for b in range(a + 1, len(members_idx)):
                    key = tuple(sorted((members_idx[a], members_idx[b])))
                    e = best_edge.get(key)
                    if e and _CONF_RANK[e] > _CONF_RANK[conf]:
                        conf = e
        sources = sorted({m["source_id"] for m in members if m["source_id"]})
        tickers = sorted({m["ticker"] for m in members if m["ticker"]})
        entities = sorted({e for m in members
                           for e in (m["payload"].get("mapped_entities") or []) if e})
        avails = sorted(m["available_at"] for m in members if m["available_at"])
        primary_present = any(
            m["source_id"] in _PRIMARY_SOURCES
            or (m["source_id"] == "rss_atom" and m["trust_level"] in _PRIMARY_TRUST)
            for m in members)
        company_present = any(
            m["record_type"] == RT_PRESS_RELEASE
            or m["source_category"] in _COMPANY_CATEGORIES for m in members)
        regulator_present = any(m["record_type"] == RT_REGULATORY_EVENT
                                for m in members)
        representative = min(
            members, key=lambda m: (-_SOURCE_RANK.get(m["source_id"], 0),
                                    m["available_at"] or "9999", m["record_id"]))
        normalized_title = max((m["norm_title"] for m in members),
                               key=lambda s: (len(s), s), default="")
        cluster_id = "clu_" + sha256_text(
            "%s|%s" % (algo_version, "|".join(member_ids)))[:24]
        clusters.append({
            "cluster_id": cluster_id,
            "algo_version": algo_version,
            "representative_record_id": representative["record_id"],
            "member_record_ids": member_ids,
            "member_sources": sources,
            "member_record_types": sorted({m["record_type"] for m in members
                                           if m["record_type"]}),
            "mapped_tickers": tickers,
            "mapped_entities": entities,
            "earliest_available_at": avails[0] if avails else None,
            "latest_available_at": avails[-1] if avails else None,
            "normalized_title": normalized_title,
            "event_category": representative["record_type"],
            "corroborating_source_count": len(sources),
            "primary_source_present": primary_present,
            "company_direct_source_present": company_present,
            "regulator_source_present": regulator_present,
            "clustering_confidence": conf,
            "clustering_reasons": _reasons(members, conf),
            "conflicting_facts": _conflicts(members),
            "created_at": now_iso,
        })
    clusters.sort(key=lambda c: c["cluster_id"])
    return clusters


def _reasons(members: list[dict], conf: str) -> list[str]:
    reasons: list[str] = []
    if conf == CC_EXACT:
        reasons.append("shared canonical URL or native id")
    elif conf in (CC_HIGH, CC_MEDIUM):
        reasons.append("shared ticker + title token similarity within time window")
    if len({m["source_id"] for m in members}) > 1:
        reasons.append("multiple distinct sources")
    if len(members) == 1:
        reasons.append("singleton — no corroborating event found")
    return reasons


def _conflicts(members: list[dict]) -> list[dict]:
    conflicts: list[dict] = []
    if len(members) < 2:
        return conflicts
    for field in _CONFLICT_FIELDS:
        values = sorted({str(m["payload"].get(field)) for m in members
                         if m["payload"].get(field) not in (None, "")})
        if len(values) > 1:
            conflicts.append({"field": field, "values": values})
    return conflicts


def index_clusters(clusters: list[dict]) -> dict[str, dict]:
    """record_id -> compact cluster membership summary, for downstream selection.
    Multi-member clusters mark exactly the representative as ``is_representative``."""
    out: dict[str, dict] = {}
    for c in clusters:
        multi = len(c["member_record_ids"]) > 1
        for rid in c["member_record_ids"]:
            out[rid] = {
                "cluster_id": c["cluster_id"],
                "representative_record_id": c["representative_record_id"],
                "member_record_ids": c["member_record_ids"],
                "corroborating_source_count": c["corroborating_source_count"],
                "clustering_confidence": c["clustering_confidence"],
                "is_representative": (rid == c["representative_record_id"]),
                "is_clustered_duplicate": multi and rid != c["representative_record_id"],
            }
    return out


def clusters_identity_projection(clusters: list[dict]) -> list[dict]:
    """Volatile-field-free projection of clusters for the deterministic run id
    (excludes created_at so identical data yields an identical run id)."""
    return [{k: v for k, v in sorted(c.items()) if k != "created_at"}
            for c in sorted(clusters, key=lambda c: c["cluster_id"])]


__all__ = ["CC_EXACT", "CC_HIGH", "CC_MEDIUM", "CC_LOW", "CC_NOT_CLUSTERED",
           "cluster_events", "index_clusters", "clusters_identity_projection"]
