"""
alpha_agent.identity_jobs — Stage 10 CANONICAL IDENTITY JOBS + DEPENDENCY PLANNER.

The minimum generic canonical job types + handlers the AlphaAgent needs to
OPERATE the historical-identity backlog through its EXISTING durable research
queue and one-job-per-cycle fair scheduler — Claude builds the reusable
capability; the AlphaAgent prioritizes, creates, claims and executes the jobs.

Every identity job is BOUNDED, IDEMPOTENT, RESTART-SAFE, FAILURE-ISOLATED,
DEDUPLICATED, CONFIG-CONTROLLED and persisted in the SAME ``ResearchQueue`` (no
second worker / queue). The jobs live on the exact new identity lanes under the
single new origin ``stage10-identity`` (the queue allowlist is broadened by
EXACTLY those, nothing else) and use ONLY categories already admitted by PASS A
(DATA_ACQUISITION / DATA_VALIDATION), so the companyfacts fairness contract is
untouched: the companyfacts promotion still skips PASS A when due, so identity
work can never starve the companyfacts continuation, and the identity backlog is
finite so it can never permanently starve the tournament.

Lanes (all origin ``stage10-identity``):
  * identity.discover        (DATA_ACQUISITION) — extract survivorship-safe
    Norgate security identity (id + ticker/name history + membership intervals)
    for the next bounded batch of not-yet-known securities.
  * identity.delisted        (DATA_ACQUISITION) — focus discovery on delisted
    (survivorship-only) securities so past periods are represented.
  * identity.membership      (DATA_ACQUISITION) — refresh membership intervals
    for a batch of known securities (append-only).
  * identity.ticker_history  (DATA_ACQUISITION) — enrich name/ticker history
    from owned SEC submissions formerNames for resolved securities.
  * identity.cik_resolve     (DATA_ACQUISITION) — run the deterministic matching
    contract for a batch of unmapped securities; persist RESOLVED / UNRESOLVED /
    AMBIGUOUS / CONFLICT (never a fabricated CIK).
  * identity.conflict_scan   (DATA_VALIDATION) — detect ticker-reuse / multi-CIK
    ambiguity + conflicts across the store.
  * identity.repair          (DATA_VALIDATION) — apply explicit audited repair
    rules to the unresolved backlog (bounded).
  * identity.coverage        (DATA_VALIDATION) — measure + persist an append-only
    mapping-coverage snapshot by rebalance date.
  * identity.companyfacts_plan (DATA_ACQUISITION) — plan historical companyfacts
    acquisition for resolved-but-unfetched CIKs (a plan artifact; no fetch here).
  * identity.readiness_eval  (DATA_VALIDATION) — recompute per-rebalance
    historical readiness from the store + the safety gate; keep DATA_HOLD until
    the configured contract passes (never flips the safety switch).
  * identity.event_map       (DATA_ACQUISITION) — map a batch of Form 4 / 8-K
    issuer CIKs to the effective security identity at the event date.

Pure stdlib; deterministic; the only side effects are the canonical identity
store, immutable artifacts under the identity artifact root, and enqueues into
the shared research queue. No operational-ledger / order / fill / signal / trade-
decision / model-promotion / prediction touch anywhere.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from . import historical_identity as _hi

ORIGIN = "stage10-identity"
# Stage 10.1 — HISTORICAL CIK BRIDGE CLOSURE rides the SAME shared research queue
# and the SAME ``identity.`` lane-prefix allowlist; it uses a distinct origin so
# the one-time full-source-exhaustion bulk campaign is identifiable in the queue.
ORIGIN_101 = "stage10.1-identity"

LANE_DISCOVER = "identity.discover"
LANE_DELISTED = "identity.delisted"
LANE_MEMBERSHIP = "identity.membership"
LANE_TICKER_HISTORY = "identity.ticker_history"
LANE_CIK_RESOLVE = "identity.cik_resolve"
LANE_CONFLICT_SCAN = "identity.conflict_scan"
LANE_REPAIR = "identity.repair"
LANE_COVERAGE = "identity.coverage"
LANE_COMPANYFACTS_PLAN = "identity.companyfacts_plan"
LANE_READINESS_EVAL = "identity.readiness_eval"
LANE_EVENT_MAP = "identity.event_map"

# Stage 10.1 lanes (full SEC bulk ingestion + complete Norgate universe bridge).
LANE_SEC_BULK_INVENTORY = "identity.sec_bulk_inventory"
LANE_SEC_SUBMISSIONS_INDEX = "identity.sec_submissions_index"
LANE_SEC_FILING_EVIDENCE_INDEX = "identity.sec_filing_evidence_index"
LANE_NORGATE_FULL_DISCOVERY = "identity.norgate_full_discovery"
LANE_CIK_FULL_RESOLUTION = "identity.cik_full_resolution"
LANE_MAPPING_COVERAGE_MEASURE = "identity.mapping_coverage_measure"
LANE_READINESS_RECHECK = "identity.readiness_recheck"
LANE_SUCCESSOR_SCAN = "identity.successor_scan"

IDENTITY_LANE_PREFIX = "identity."

_ACQUISITION_LANES = frozenset({
    LANE_DISCOVER, LANE_DELISTED, LANE_MEMBERSHIP, LANE_TICKER_HISTORY,
    LANE_CIK_RESOLVE, LANE_COMPANYFACTS_PLAN, LANE_EVENT_MAP,
    LANE_SEC_BULK_INVENTORY, LANE_SEC_SUBMISSIONS_INDEX,
    LANE_SEC_FILING_EVIDENCE_INDEX, LANE_NORGATE_FULL_DISCOVERY,
    LANE_CIK_FULL_RESOLUTION, LANE_SUCCESSOR_SCAN})
_VALIDATION_LANES = frozenset({
    LANE_CONFLICT_SCAN, LANE_REPAIR, LANE_COVERAGE, LANE_READINESS_EVAL,
    LANE_MAPPING_COVERAGE_MEASURE, LANE_READINESS_RECHECK})

DEFAULT_DISCOVER_BATCH = 25
DEFAULT_RESOLVE_BATCH = 50
DEFAULT_PRIORITY = 6   # above the companyfacts bootstrap (5) so a queued
#                        identity backlog job is admitted first while it exists;
#                        the companyfacts fairness promotion still skips PASS A.


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# --------------------------------------------------------------------------- #
# Immutable artifact writing.
# --------------------------------------------------------------------------- #
def write_artifact(root: str | Path, lane: str, payload: dict, *,
                   clock: Optional[Callable[[], str]] = None) -> str:
    """Write ONE immutable, content-addressed JSON artifact under
    ``<root>/<lane>/`` and return its path. Atomic (tmp + os.replace); never
    overwrites a different content at the same address."""
    now = (clock or _utc_now_iso)()
    ch = _hi.content_hash(payload)[:16]
    d = Path(root) / lane.replace(".", "_")
    d.mkdir(parents=True, exist_ok=True)
    path = d / ("%s_%s.json" % (str(now).replace(":", "").replace("-", ""), ch))
    if not path.exists():
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(_hi.canonical_json({"lane": lane, "written_at": now,
                                           **payload}), encoding="utf-8")
        os.replace(tmp, path)
    return str(path)


# --------------------------------------------------------------------------- #
# Identity job context (all heavy dependencies injected for testability).
# --------------------------------------------------------------------------- #
@dataclass
class IdentityJobContext:
    store: _hi.IdentityStore
    accessor: _hi.NorgateIdentityAccessor
    artifact_root: str
    index_name: str = "S&P 500"
    survivorship_watchlist: str = "S&P 500 Current & Past"
    current_watchlist: str = "S&P 500"
    membership_start: str = "1990-01-01"
    discover_batch: int = DEFAULT_DISCOVER_BATCH
    resolve_batch: int = DEFAULT_RESOLVE_BATCH
    rebalance_dates: "list[str]" = field(default_factory=list)
    cfg9: dict = field(default_factory=dict)
    # Owned SEC evidence, injected (all optional; absent => cannot resolve).
    owned_authoritative: Optional[dict] = None       # {security_id -> cik}
    direct_norgate_sec: Optional[dict] = None         # {assetid -> cik}
    ticker_cik_index: Optional[dict] = None           # {ticker -> [cik,...]}
    submissions_by_cik: Optional[dict] = None         # {cik -> parsed subs}
    repair_rules: Optional[dict] = None               # {security_id -> {cik}}
    # Stage 10.1 — the SEC issuer-history index + bulk-source acquisition wiring.
    issuer_index: Any = None                          # sec_issuer_index.SecIssuerIndex
    read_normalized: Optional[Callable] = None        # (record_type,*,limit)->iter
    transport: Optional[Callable] = None              # ingestion.default_transport
    stage101: dict = field(default_factory=dict)      # the stage10_1 config block
    clock: Optional[Callable[[], str]] = None

    def now(self) -> str:
        return (self.clock or _utc_now_iso)()


# Handler outcome tokens mirror autonomous_research (imported lazily to avoid a
# hard import cycle at module load).
def _outcomes():
    from . import autonomous_research as _ar
    return (_ar.OUTCOME_COMPLETED, _ar.OUTCOME_BLOCKED_SPECIFIC,
            _ar.OUTCOME_RETRYABLE)


# --------------------------------------------------------------------------- #
# Per-lane handlers. Each is bounded, idempotent, restart-safe and returns
# (outcome, detail) with an immutable artifact path or an exact blocker.
# --------------------------------------------------------------------------- #
def _known_symbols(ctx: IdentityJobContext) -> set:
    return {s["norgate_symbol"] for s in ctx.store.list_securities()}


def _discover(ctx: IdentityJobContext, job, *, delisted_only: bool,
              batch_size: Optional[int] = None) -> tuple:
    OK, BLK, RETRY = _outcomes()
    ok, why = ctx.accessor.available()
    if not ok:
        return BLK, {"real_work": "identity.discover", "disposition": "DATA_HOLD",
                     "blocker": "NORGATE_UNAVAILABLE", "reason": why,
                     "no_automatic_promotion": True}
    surv = ctx.accessor.watchlist_symbols(ctx.survivorship_watchlist)
    current = set(ctx.accessor.watchlist_symbols(ctx.current_watchlist))
    if not surv:
        return BLK, {"real_work": "identity.discover", "disposition": "DATA_HOLD",
                     "blocker": "NORGATE_EMPTY_UNIVERSE",
                     "reason": "survivorship watchlist '%s' returned no symbols"
                     % ctx.survivorship_watchlist}
    known = _known_symbols(ctx)
    pool = sorted(set(surv) - known)
    if delisted_only:
        pool = [s for s in pool if s not in current
                and _hi.parse_norgate_symbol(s)["is_delisted"]]
    bsz = int(batch_size if batch_size is not None else ctx.discover_batch)
    batch = pool[:max(1, bsz)]
    created = changed = 0
    sample = []
    for sym in batch:
        ident = _hi.extract_security_identity(
            ctx.accessor, sym, index_name=ctx.index_name,
            membership_start=ctx.membership_start)
        if ident is None:
            continue
        r = ctx.store.upsert_security(ident)
        created += 1 if r["created"] else 0
        changed += 1 if r["changed"] else 0
        if len(sample) < 5:
            sample.append({"symbol": sym, "security_id": ident.security_id,
                           "is_current": ident.is_current,
                           "delisting_date": ident.delisting_date})
    counts = ctx.store.counts()
    detail = {"real_work": ("identity.delisted" if delisted_only
                            else "identity.discover"),
              "batch_requested": len(batch), "securities_created": created,
              "securities_changed": changed,
              "total_securities": counts["total_securities"],
              "delisted_securities": counts["delisted_securities"],
              "remaining_backlog": max(0, len(pool) - len(batch)),
              "sample": sample, "no_automatic_promotion": True,
              "disposition": "DATA_HOLD"}
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    if created == 0 and changed == 0 and not batch:
        detail["note"] = "no new securities to discover (universe fully indexed)"
    return OK, detail


def _membership(ctx: IdentityJobContext, job) -> tuple:
    OK, BLK, RETRY = _outcomes()
    ok, why = ctx.accessor.available()
    if not ok:
        return BLK, {"real_work": "identity.membership", "blocker":
                     "NORGATE_UNAVAILABLE", "reason": why,
                     "disposition": "DATA_HOLD"}
    secs = ctx.store.list_securities(limit=100000)
    # Refresh a bounded batch of securities whose membership span set is small
    # (or absent) — deterministic ordering by security_id.
    batch = secs[:max(1, int(ctx.discover_batch))]
    appended = 0
    for s in batch:
        full = ctx.store.get_security(s["security_id"])
        if full and full.get("membership_intervals"):
            continue
        intervals = ctx.accessor.membership_intervals(
            s["norgate_symbol"], ctx.index_name, ctx.membership_start)
        if not intervals:
            continue
        ident = _hi.SecurityIdentity(
            security_id=s["security_id"], norgate_assetid=s["norgate_assetid"],
            norgate_symbol=s["norgate_symbol"], ticker=s["ticker"],
            issuer_name=s["issuer_name"], share_class=s["share_class"],
            base_type=s["base_type"], exchange=s["exchange"],
            security_start_date=s["security_start_date"],
            security_end_date=s["security_end_date"],
            delisting_date=s["delisting_date"], is_current=bool(s["is_current"]),
            ticker_history=[], name_history=[], membership_intervals=intervals)
        ctx.store.upsert_security(ident)
        appended += 1
    detail = {"real_work": "identity.membership", "batch": len(batch),
              "securities_with_new_membership": appended,
              "disposition": "DATA_HOLD", "no_automatic_promotion": True}
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


def _ticker_history(ctx: IdentityJobContext, job) -> tuple:
    OK, BLK, RETRY = _outcomes()
    subs = ctx.submissions_by_cik or {}
    if not subs:
        return BLK, {"real_work": "identity.ticker_history",
                     "blocker": "NO_OWNED_SUBMISSIONS_FORMER_NAMES",
                     "reason": ("no owned SEC submissions formerNames history to "
                                "enrich ticker/name history; the sec submissions "
                                "acquisition lane must run first"),
                     "disposition": "DATA_HOLD"}
    enriched = 0
    for s in ctx.store.list_securities(limit=100000):
        mp = ctx.store.active_mapping(s["security_id"])
        if not (mp and mp.get("status") == _hi.STATUS_RESOLVED and mp.get("cik")):
            continue
        sub = subs.get(_hi.norm_cik(mp["cik"]))
        if not sub:
            continue
        nh = []
        for fn in (sub.get("former_names") or []):
            if fn.get("name"):
                nh.append({"name": fn["name"], "effective_start": fn.get("from"),
                           "effective_end": fn.get("to"), "source": "sec_edgar"})
        if not nh:
            continue
        ident = _rehydrate(s, name_history=nh)
        ctx.store.upsert_security(ident)
        enriched += 1
    detail = {"real_work": "identity.ticker_history",
              "securities_enriched_from_former_names": enriched,
              "disposition": "DATA_HOLD"}
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


def _cik_resolve(ctx: IdentityJobContext, job) -> tuple:
    OK, BLK, RETRY = _outcomes()
    unmapped = ctx.store.list_securities(unmapped_only=True,
                                         limit=max(1, int(ctx.resolve_batch)))
    resolved = unresolved = ambiguous = conflict = 0
    for s in unmapped:
        sec = ctx.store.get_security(s["security_id"])
        res = _hi.match_security_to_cik(
            sec, owned_authoritative=ctx.owned_authoritative,
            direct_norgate_sec=ctx.direct_norgate_sec,
            ticker_cik_index=ctx.ticker_cik_index,
            submissions_by_cik=ctx.submissions_by_cik,
            repair_rules=ctx.repair_rules)
        ctx.store.record_mapping(res)
        if res.status == _hi.STATUS_RESOLVED:
            resolved += 1
        elif res.status == _hi.STATUS_AMBIGUOUS:
            ambiguous += 1
        elif res.status == _hi.STATUS_CONFLICT:
            conflict += 1
        else:
            unresolved += 1
    counts = ctx.store.counts()
    detail = {"real_work": "identity.cik_resolve", "batch": len(unmapped),
              "resolved": resolved, "unresolved": unresolved,
              "ambiguous": ambiguous, "conflict": conflict,
              "total_resolved": counts["resolved"],
              "unresolved_backlog": counts["unresolved_backlog"],
              "disposition": "DATA_HOLD", "no_automatic_promotion": True}
    if resolved == 0 and (ctx.owned_authoritative or ctx.ticker_cik_index or
                          ctx.submissions_by_cik) is None:
        detail["reason"] = ("no owned survivorship-safe security->CIK evidence "
                            "available; batch recorded UNRESOLVED (honest)")
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


def _conflict_scan(ctx: IdentityJobContext, job) -> tuple:
    OK, BLK, RETRY = _outcomes()
    # Ticker reuse across DISTINCT securities is a structural ambiguity signal.
    by_ticker: dict[str, set] = {}
    for s in ctx.store.list_securities(limit=100000):
        by_ticker.setdefault(s["ticker"], set()).add(s["security_id"])
    reused = {t: sorted(v) for t, v in by_ticker.items() if len(v) > 1}
    counts = ctx.store.counts()
    detail = {"real_work": "identity.conflict_scan",
              "tickers_reused_across_securities": len(reused),
              "reused_sample": dict(list(reused.items())[:10]),
              "mapping_conflicts": counts["conflict_rows"],
              "ambiguous_mappings": counts["ambiguous"],
              "disposition": "DATA_HOLD"}
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


def _repair(ctx: IdentityJobContext, job) -> tuple:
    OK, BLK, RETRY = _outcomes()
    rules = ctx.repair_rules or {}
    if not rules:
        return BLK, {"real_work": "identity.repair",
                     "blocker": "NO_AUDITED_REPAIR_RULES",
                     "reason": ("no explicit audited repair rules provided; the "
                                "unresolved backlog stays unresolved (never "
                                "guessed)"), "disposition": "DATA_HOLD"}
    repaired = 0
    for row in ctx.store.unresolved(limit=max(1, int(ctx.resolve_batch))):
        sid = row["security_id"]
        if sid not in rules:
            continue
        sec = ctx.store.get_security(sid)
        res = _hi.match_security_to_cik(sec, repair_rules=rules)
        ctx.store.record_mapping(res)
        if res.status == _hi.STATUS_RESOLVED:
            repaired += 1
    detail = {"real_work": "identity.repair", "repaired": repaired,
              "unresolved_backlog": ctx.store.counts()["unresolved_backlog"],
              "disposition": "DATA_HOLD"}
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


def _coverage(ctx: IdentityJobContext, job) -> tuple:
    OK, BLK, RETRY = _outcomes()
    snap = ctx.store.record_coverage_snapshot(
        as_of=ctx.now()[:10], rebalance_dates=ctx.rebalance_dates,
        index_name=ctx.index_name)
    detail = {"real_work": "identity.coverage",
              "total_securities": snap["total_securities"],
              "resolved": snap["resolved"], "unresolved": snap["unresolved"],
              "mapped_ciks": snap["mapped_ciks"],
              "by_date": snap["by_date"], "disposition": "DATA_HOLD"}
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


def _companyfacts_plan(ctx: IdentityJobContext, job) -> tuple:
    OK, BLK, RETRY = _outcomes()
    # CIKs resolved by the identity layer but not yet backed by owned PIT facts
    # are the highest-value historical companyfacts acquisition targets. This
    # produces a PLAN artifact only; the acquisition itself runs through the
    # existing companyfacts continuation, never here.
    resolved_ciks = []
    for s in ctx.store.list_securities(limit=100000):
        mp = ctx.store.active_mapping(s["security_id"])
        if mp and mp.get("status") == _hi.STATUS_RESOLVED and mp.get("cik"):
            resolved_ciks.append(mp["cik"])
    resolved_ciks = sorted(set(resolved_ciks))
    detail = {"real_work": "identity.companyfacts_plan",
              "resolved_ciks": len(resolved_ciks),
              "plan": {"campaign": "sec_companyfacts",
                       "historical_cik_targets": resolved_ciks[:200]},
              "disposition": "DATA_HOLD",
              "note": ("historical companyfacts acquisition is executed by the "
                       "existing companyfacts continuation, not here")}
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


def _readiness_eval(ctx: IdentityJobContext, job) -> tuple:
    OK, BLK, RETRY = _outcomes()
    from . import fundamental_readiness as _fr
    # Recompute the historical readiness/mapping status FROM THE MEASURED STORE
    # (never the config flag), then consult the safety gate. Below threshold the
    # single diagnostic stands and candidates stay DATA_HOLD. This NEVER flips
    # the safety switch — it only measures and reports honestly.
    mapping = _fr.historical_mapping_status_from_store(ctx.store, ctx.cfg9)
    gate_report = {}
    for signal in ((ctx.cfg9.get("stage9_5") or {}).get("fundamental_mvp") or {}
                   ).get("signals", ["gross_profitability"]):
        allowed = _fr.historical_fundamental_experiment_allowed(
            ctx.cfg9, store=ctx.store, signal=signal)
        gate_report[signal] = {"allowed": allowed["allowed"],
                               "diagnostic": allowed["diagnostic"]}
    detail = {"real_work": "identity.readiness_eval",
              "historical_mapping_available": mapping["available"],
              "measured_mapping_coverage_pct": mapping.get(
                  "measured_coverage_pct"),
              "required_coverage_pct": mapping.get("required_coverage_pct"),
              "gate": gate_report, "disposition": "DATA_HOLD",
              "safety_switch_flipped": False, "no_automatic_promotion": True,
              "diagnostic": _hi.STATUS_UNRESOLVED if not mapping["available"]
              else None}
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


def _event_map(ctx: IdentityJobContext, job) -> tuple:
    OK, BLK, RETRY = _outcomes()
    issuers = (job.payload or {}).get("issuer_ciks") or []
    event_date = (job.payload or {}).get("event_date")
    mapped = []
    for cik in issuers[:max(1, int(ctx.resolve_batch))]:
        m = map_event_issuer(ctx.store, cik, event_date)
        mapped.append(m)
    resolved = sum(1 for m in mapped if m.get("security_id"))
    detail = {"real_work": "identity.event_map", "issuers": len(issuers),
              "mapped": resolved,
              "ambiguous_or_unresolved": len(mapped) - resolved,
              "sample": mapped[:10], "disposition": "DATA_HOLD",
              "note": ("event candidates stay DATA_HOLD; mapping capability only")}
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


def _rehydrate(row: dict, *, name_history=None, ticker_history=None,
               membership=None) -> _hi.SecurityIdentity:
    return _hi.SecurityIdentity(
        security_id=row["security_id"], norgate_assetid=row["norgate_assetid"],
        norgate_symbol=row["norgate_symbol"], ticker=row["ticker"],
        issuer_name=row["issuer_name"], share_class=row["share_class"],
        base_type=row["base_type"], exchange=row["exchange"],
        security_start_date=row["security_start_date"],
        security_end_date=row["security_end_date"],
        delisting_date=row["delisting_date"], is_current=bool(row["is_current"]),
        ticker_history=ticker_history or [], name_history=name_history or [],
        membership_intervals=membership or [])


# =========================================================================== #
# Stage 10.1 — HISTORICAL CIK BRIDGE CLOSURE handlers. Full SEC bulk ingestion +
# complete Norgate universe resolution. Each is bounded (member/security cap +
# wall-clock budget), idempotent, restart-safe (durable cursors) and honest
# (never fabricates a CIK; ambiguous stays ambiguous; DATA_HOLD preserved).
# =========================================================================== #
def _bulk_cfg(ctx: IdentityJobContext) -> dict:
    return ctx.stage101 or {}


def _ua_headers(cfg: dict) -> dict:
    ua = "%s %s" % (cfg.get("user_agent_product",
                            "paper-trader-alpha-agent/2.0"),
                    cfg.get("contact_email") or "")
    return {"User-Agent": ua.strip(), "Accept-Encoding": "identity"}


def _submissions_paths(cfg: dict):
    root = cfg.get("bulk_root")
    if not root:
        return None, None
    return Path(root) / "submissions.zip", Path(root) / "submissions.manifest.json"


def _submissions_hash(cfg: dict) -> Optional[str]:
    _, manifest = _submissions_paths(cfg)
    if manifest and manifest.exists():
        try:
            return json.loads(manifest.read_text(encoding="utf-8")).get("sha256")
        except (OSError, ValueError):
            return None
    return None


def _universe_target(ctx: IdentityJobContext) -> int:
    try:
        if ctx.accessor.available()[0]:
            return len(ctx.accessor.watchlist_symbols(ctx.survivorship_watchlist))
    except Exception:  # noqa: BLE001
        return 0
    return 0


def _universe_fingerprint(ctx: IdentityJobContext) -> Optional[str]:
    try:
        syms = sorted(ctx.accessor.watchlist_symbols(ctx.survivorship_watchlist))
        if not syms:
            return None
        return _hi.content_hash(syms)
    except Exception:  # noqa: BLE001
        return None


def _resolution_epoch(ctx: IdentityJobContext) -> str:
    """A CHEAP, stable epoch key = (Norgate universe size, SEC issuer count). It
    changes only when the universe or the SEC index genuinely grows, and avoids
    hashing ~1M issuer rows on every coverage/readiness step."""
    total = ctx.store.counts()["total_securities"]
    iss = ctx.issuer_index.counts()["issuers"] if ctx.issuer_index is not None \
        else 0
    return "%d:%d" % (total, iss)


def _sec_bulk_inventory(ctx: IdentityJobContext, job) -> tuple:
    """Part 1 acquisition: inventory the SEC bulk sources and DOWNLOAD the
    submissions archive with the existing restart-safe/atomic/SHA-256 downloader
    (looped to completion inside a wall-clock budget; a transport interruption is
    RETRYABLE and resumes from the durable byte checkpoint). No parsing here."""
    import time as _time
    from . import sec_bulk_download as _bulk
    OK, BLK, RETRY = _outcomes()
    cfg = _bulk_cfg(ctx)
    final, manifest = _submissions_paths(cfg)
    url = cfg.get("submissions_url")
    if final is None or not url:
        return BLK, {"real_work": "identity.sec_bulk_inventory",
                     "blocker": "STAGE101_NOT_CONFIGURED",
                     "reason": "bulk_root/submissions_url not configured",
                     "disposition": "DATA_HOLD"}
    if ctx.transport is None:
        return BLK, {"real_work": "identity.sec_bulk_inventory",
                     "blocker": "NO_HTTP_TRANSPORT",
                     "reason": "no SEC HTTP transport injected", "disposition":
                     "DATA_HOLD"}
    dl = _bulk.BulkArchiveDownloader(
        url=url, dest_dir=final.parent, name="submissions",
        headers=_ua_headers(cfg),
        disk_budget_bytes=int(cfg.get("disk_budget_bytes", 4 * 1024 ** 3)),
        transport=ctx.transport)
    budget = float(cfg.get("download_time_budget_seconds", 1800))
    started = _time.monotonic()
    last = {}
    while True:
        last = dl.download_segment()
        if last.get("complete") or not last.get("ok"):
            break
        if (_time.monotonic() - started) >= budget:
            break
    inventory = {
        "submissions": {"final_path": str(final), "exists": final.exists(),
                        "bytes": (final.stat().st_size if final.exists()
                                  else 0)},
        "companyfacts_url": cfg.get("companyfacts_url"),
        "company_tickers_url": cfg.get("company_tickers_url"),
        "company_tickers_exchange_url":
            cfg.get("company_tickers_exchange_url")}
    detail = {"real_work": "identity.sec_bulk_inventory",
              "inventory": inventory,
              "download_disposition": last.get("disposition"),
              "progress_pct": last.get("progress_pct"),
              "archive_sha256": last.get("sha256"),
              "archive_bytes": last.get("total_bytes"),
              "complete": bool(last.get("complete")),
              "disposition": "DATA_HOLD", "no_automatic_promotion": True}
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    if last.get("complete"):
        return OK, detail
    # In progress or transport-interrupted: resume the SAME job next claim (the
    # durable byte checkpoint is preserved) rather than mark it done.
    detail["reason"] = last.get("reason", "download in progress; resuming")
    return RETRY, detail


def _sec_submissions_index(ctx: IdentityJobContext, job) -> tuple:
    """Part 2: stream the downloaded submissions archive into the durable SEC
    issuer-history index, resuming from the member cursor. Bounded per step +
    wall-clock; idempotent; malformed / path-traversal members isolated."""
    OK, BLK, RETRY = _outcomes()
    cfg = _bulk_cfg(ctx)
    if ctx.issuer_index is None:
        return BLK, {"real_work": "identity.sec_submissions_index",
                     "blocker": "NO_ISSUER_INDEX",
                     "reason": "SEC issuer-history index not configured",
                     "disposition": "DATA_HOLD"}
    final, _ = _submissions_paths(cfg)
    if final is None or not final.exists():
        return BLK, {"real_work": "identity.sec_submissions_index",
                     "blocker": "SEC_SUBMISSIONS_ARCHIVE_ABSENT",
                     "reason": ("submissions.zip not present; run "
                                "identity.sec_bulk_inventory first"),
                     "disposition": "DATA_HOLD"}
    ahash = _submissions_hash(cfg)
    r = ctx.issuer_index.index_submissions_archive(
        final, archive_hash=ahash,
        member_step=int(cfg.get("submissions_member_step", 2000)),
        time_budget_seconds=float(cfg.get("submissions_time_budget_seconds",
                                          1500)))
    detail = {"real_work": "identity.sec_submissions_index",
              "archive_sha256": ahash,
              "members_done": r.get("members_done"),
              "total_members": r.get("total_members"),
              "issuers_indexed": r.get("issuers_indexed"),
              "created_this_call": r.get("created_this_call"),
              "changed_this_call": r.get("changed_this_call"),
              "skipped_members": r.get("skipped_members"),
              "malformed_members": r.get("malformed_members"),
              "complete": bool(r.get("complete")),
              "elapsed_seconds": r.get("elapsed_seconds"),
              "disposition": "DATA_HOLD", "no_automatic_promotion": True}
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


def _sec_filing_evidence_index(ctx: IdentityJobContext, job) -> tuple:
    """Part 2 (owned/free evidence): index the CURRENT company_tickers[_exchange]
    maps (source-tagged, never mislabeled historical) and the owned normalized
    FILING_EVENT / INSIDER_FILING / FUNDAMENTAL_FACT filing-derived DATED ticker
    observations into the issuer index. Idempotent per source."""
    OK, BLK, RETRY = _outcomes()
    cfg = _bulk_cfg(ctx)
    if ctx.issuer_index is None:
        return BLK, {"real_work": "identity.sec_filing_evidence_index",
                     "blocker": "NO_ISSUER_INDEX", "reason":
                     "SEC issuer-history index not configured",
                     "disposition": "DATA_HOLD"}
    idx = ctx.issuer_index
    out: dict = {"company_tickers": None, "company_tickers_exchange": None,
                 "filing_evidence": {}}
    if ctx.transport is not None:
        for key, url in (("company_tickers", cfg.get("company_tickers_url")),
                         ("company_tickers_exchange",
                          cfg.get("company_tickers_exchange_url"))):
            if not url:
                continue
            try:
                resp = ctx.transport({"method": "GET", "url": url,
                                      "headers": _ua_headers(cfg)}, 60.0)
                if resp.get("status") == 200 and resp.get("body"):
                    doc = json.loads(resp["body"].decode("utf-8", "replace"))
                    out[key] = idx.index_company_tickers(doc, kind=key)
                else:
                    out[key] = {"http_status": resp.get("status")}
            except Exception as exc:  # noqa: BLE001 - source failure isolated
                out[key] = {"error": type(exc).__name__}
    if callable(ctx.read_normalized):
        total_obs = 0
        for rt in ("FILING_EVENT", "INSIDER_FILING", "FUNDAMENTAL_FACT"):
            try:
                recs = ctx.read_normalized(
                    rt, limit=int(cfg.get("filing_evidence_limit", 20000)))
                r = idx.index_filing_evidence(recs, source="owned_%s" % rt)
                out["filing_evidence"][rt] = r
                total_obs += int(r.get("observations_added") or 0)
            except Exception as exc:  # noqa: BLE001
                out["filing_evidence"][rt] = {"error": type(exc).__name__}
        out["filing_evidence"]["total_observations_added"] = total_obs
    ctx.store.set_meta("stage101_filing_evidence_done",
                       _submissions_hash(cfg) or "1")
    detail = {"real_work": "identity.sec_filing_evidence_index", "result": out,
              "index_counts": idx.counts(), "disposition": "DATA_HOLD",
              "no_automatic_promotion": True}
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


def _norgate_full_discovery(ctx: IdentityJobContext, job) -> tuple:
    """Part 3: discover the next bounded batch of the COMPLETE Norgate 'S&P 500
    Current & Past' universe (not a sample). Reuses the survivorship-safe
    identity extraction; assetid is the identity; delisted names are preserved."""
    cfg = _bulk_cfg(ctx)
    batch = int(cfg.get("norgate_full_batch", 250))
    return _discover(ctx, job, delisted_only=False, batch_size=batch)


def _classify_unresolved(security: dict, result, meta: dict) -> str:
    """Exact, evidence-based reason code for a non-RESOLVED security (Part 4).
    Deterministic from the candidate pools + the contract result."""
    from . import sec_issuer_index as _si
    tc = set(meta.get("ticker_candidates") or [])
    nc = set(meta.get("name_candidates") or [])
    allc = tc | nc
    if not security.get("norgate_assetid"):
        return _si.REASON_MISSING_NORGATE_IDENTITY
    if not allc:
        return _si.REASON_NO_SEC_CANDIDATE
    share_class = security.get("share_class") or "COMMON"
    st = result.status
    if st == _hi.STATUS_CONFLICT:
        return _si.REASON_TICKER_REUSE_CONFLICT
    if st == _hi.STATUS_AMBIGUOUS:
        if share_class not in ("COMMON",):
            return _si.REASON_SHARE_CLASS_AMBIGUITY
        if len(nc) > 1 and (result.evidence or {}).get("tier") == 4:
            return _si.REASON_NAME_COLLISION
        if len(tc) > 1:
            return _si.REASON_TICKER_REUSE_CONFLICT
        return _si.REASON_MULTIPLE_CIK_CANDIDATES
    if meta.get("overflow"):
        return _si.REASON_NAME_COLLISION
    if nc and not tc:
        return _si.REASON_DATE_INTERVAL_MISMATCH
    return _si.REASON_INSUFFICIENT_CORROBORATION


def _cik_full_resolution(ctx: IdentityJobContext, job) -> tuple:
    """Part 4: run the deterministic matching contract for the next bounded batch
    of not-yet-RESOLVED securities, enriched by per-security candidate lookup from
    the complete SEC issuer-history index. Pages by security_id (an epoch cursor)
    so every security is attempted exactly once per (universe, index) epoch —
    previously-unresolved securities are re-attempted with the fuller evidence.
    Persists exact reason codes + candidate pools so repair never repeats the
    search. Never fabricates a CIK."""
    OK, BLK, RETRY = _outcomes()
    cfg = _bulk_cfg(ctx)
    if ctx.issuer_index is None:
        return BLK, {"real_work": "identity.cik_full_resolution",
                     "blocker": "NO_ISSUER_INDEX", "reason":
                     "SEC issuer-history index not configured",
                     "disposition": "DATA_HOLD"}
    batch = int(cfg.get("resolve_full_batch", 500))
    epoch = _resolution_epoch(ctx)
    if ctx.store.get_meta("stage101_res_epoch") != epoch:
        ctx.store.set_meta("stage101_res_epoch", epoch)
        ctx.store.set_meta("stage101_res_cursor", "")
    cursor = ctx.store.get_meta("stage101_res_cursor") or ""
    all_secs = ctx.store.list_securities(limit=1000000)
    resolved = unresolved = ambiguous = conflict = attempted = 0
    reason_hist: dict = {}
    last_id = cursor
    exhausted = True
    for s in all_secs:
        sid = s["security_id"]
        if sid <= cursor:
            continue
        if attempted >= batch:
            exhausted = False
            break
        last_id = sid
        mp = ctx.store.active_mapping(sid)
        if mp and mp.get("status") == _hi.STATUS_RESOLVED:
            continue   # keep prior RESOLVED; just advance the cursor past it
        sec = ctx.store.get_security(sid)
        tki, sub, meta = ctx.issuer_index.candidate_evidence_for(sec)
        res = _hi.match_security_to_cik(
            sec, owned_authoritative=ctx.owned_authoritative,
            direct_norgate_sec=ctx.direct_norgate_sec, ticker_cik_index=tki,
            submissions_by_cik=sub, repair_rules=ctx.repair_rules)
        attempted += 1
        if res.status != _hi.STATUS_RESOLVED:
            code = _classify_unresolved(sec, res, meta)
            reason_hist[code] = reason_hist.get(code, 0) + 1
            allc = sorted(set(meta.get("ticker_candidates") or []) |
                          set(meta.get("name_candidates") or []))
            res.evidence["reason_code"] = code
            res.evidence["required_evidence"] = {
                "reason_code": code, "candidate_ciks": allc[:25],
                "ticker_candidates": sorted(meta.get("ticker_candidates") or
                                            [])[:25]}
        ctx.store.record_mapping(res)
        if res.status == _hi.STATUS_RESOLVED:
            resolved += 1
        elif res.status == _hi.STATUS_AMBIGUOUS:
            ambiguous += 1
        elif res.status == _hi.STATUS_CONFLICT:
            conflict += 1
        else:
            unresolved += 1
    ctx.store.set_meta("stage101_res_cursor", last_id)
    if exhausted:
        ctx.store.set_meta("stage101_resolution_complete", epoch)
    counts = ctx.store.counts()
    detail = {"real_work": "identity.cik_full_resolution", "attempted": attempted,
              "resolved": resolved, "unresolved": unresolved,
              "ambiguous": ambiguous, "conflict": conflict,
              "reason_distribution": reason_hist, "epoch_complete": exhausted,
              "total_resolved": counts["resolved"],
              "unresolved_backlog": counts["unresolved_backlog"],
              "disposition": "DATA_HOLD", "no_automatic_promotion": True}
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


def _mapping_version_hash(ctx: IdentityJobContext) -> str:
    """The canonical mapping-version hash binding the Norgate universe fingerprint,
    the SEC archive hash, the issuer-index digest, the identity-store digest and
    the matching-algorithm version + revision."""
    parts = {
        "algo": _hi.MAPPING_ALGORITHM_VERSION,
        "identity_store_digest": ctx.store.digest(),
        "issuer_index_fingerprint": (ctx.issuer_index.index_fingerprint()
                                     if ctx.issuer_index is not None else None),
        "norgate_universe_fingerprint": _universe_fingerprint(ctx),
        "sec_archive_hash": _submissions_hash(_bulk_cfg(ctx)),
    }
    import hashlib as _hl
    return _hl.sha256(_hi.canonical_json(parts).encode()).hexdigest()


def _coverage_breakdown(ctx: IdentityJobContext) -> dict:
    """Coverage by current/delisted, exchange, matching tier and unresolved reason
    code — one bounded pass over the store."""
    by_tier: dict = {}
    by_exchange: dict = {}
    cur_total = cur_res = del_total = del_res = 0
    for s in ctx.store.list_securities(limit=1000000):
        is_cur = bool(s["is_current"])
        mp = ctx.store.active_mapping(s["security_id"])
        resolved = bool(mp and mp.get("status") == _hi.STATUS_RESOLVED
                        and mp.get("cik"))
        if is_cur:
            cur_total += 1
            cur_res += 1 if resolved else 0
        else:
            del_total += 1
            del_res += 1 if resolved else 0
        ex = s.get("exchange") or "UNKNOWN"
        slot = by_exchange.setdefault(ex, {"total": 0, "resolved": 0})
        slot["total"] += 1
        slot["resolved"] += 1 if resolved else 0
        if resolved:
            t = "tier_%s" % mp.get("tier")
            by_tier[t] = by_tier.get(t, 0) + 1
    by_reason: dict = {}
    for row in ctx.store.unresolved(limit=1000000):
        code = None
        try:
            code = (json.loads(row.get("required_evidence") or "{}") or {}
                    ).get("reason_code")
        except (ValueError, TypeError):
            code = None
        code = code or row.get("reason") or "UNKNOWN"
        by_reason[code] = by_reason.get(code, 0) + 1
    return {
        "current_mapping_pct": round(100.0 * cur_res / cur_total, 4)
        if cur_total else 0.0,
        "delisted_mapping_pct": round(100.0 * del_res / del_total, 4)
        if del_total else 0.0,
        "current_total": cur_total, "current_resolved": cur_res,
        "delisted_total": del_total, "delisted_resolved": del_res,
        "by_tier": by_tier, "by_exchange": by_exchange,
        "by_reason_code": by_reason}


def _mapping_coverage_measure(ctx: IdentityJobContext, job) -> tuple:
    """Part 7: measure + persist the full coverage snapshot across EVERY configured
    rebalance date, compute the canonical mapping-version hash and the coverage
    breakdowns (current/delisted, exchange, tier, reason code). Append-only."""
    OK, BLK, RETRY = _outcomes()
    snap = ctx.store.record_coverage_snapshot(
        as_of=ctx.now()[:10], rebalance_dates=ctx.rebalance_dates,
        index_name=ctx.index_name)
    mvh = _mapping_version_hash(ctx)
    ctx.store.set_meta("stage101_mapping_version_hash", mvh)
    ctx.store.set_meta("stage101_coverage_complete", _resolution_epoch(ctx))
    breakdown = _coverage_breakdown(ctx)
    by_date = snap["by_date"]
    covs = sorted(v["mapping_coverage_pct"] for v in by_date.values()) \
        if by_date else []
    detail = {"real_work": "identity.mapping_coverage_measure",
              "total_securities": snap["total_securities"],
              "current_securities": snap["current_securities"],
              "delisted_securities": snap["delisted_securities"],
              "resolved": snap["resolved"], "unresolved": snap["unresolved"],
              "ambiguous": snap["ambiguous"], "conflict": snap["conflict"],
              "mapped_ciks": snap["mapped_ciks"],
              "mapping_version_hash": mvh, "by_date": by_date,
              "coverage_min_pct": covs[0] if covs else 0.0,
              "coverage_max_pct": covs[-1] if covs else 0.0,
              "coverage_median_pct": (covs[len(covs) // 2] if covs else 0.0),
              "breakdown": breakdown,
              "disposition": "DATA_HOLD", "no_automatic_promotion": True}
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


def _successor_scan(ctx: IdentityJobContext, job) -> tuple:
    """Part 5: record predecessor/successor RELATIONSHIP evidence (a delisted name
    that appears as another issuer's SEC formerName is a rename/succession signal)
    as SUPPORTING EVIDENCE ONLY. Never maps a security and never rewrites a
    predecessor's historical identity (status stays UNRESOLVED). Bounded."""
    OK, BLK, RETRY = _outcomes()
    if ctx.issuer_index is None:
        return BLK, {"real_work": "identity.successor_scan",
                     "blocker": "NO_ISSUER_INDEX", "reason":
                     "SEC issuer-history index not configured",
                     "disposition": "DATA_HOLD"}
    cfg = _bulk_cfg(ctx)
    batch = int(cfg.get("successor_batch", 200))
    idx = ctx.issuer_index
    scanned = recorded = 0
    for row in ctx.store.unresolved(limit=batch):
        sid = row["security_id"]
        scanned += 1
        sec = ctx.store.get_security(sid)
        if not sec:
            continue
        nnm = _hi._norm_name(sec.get("issuer_name") or "")
        if not nnm:
            continue
        for c in sorted(idx.candidates_by_name(nnm)):
            iss = idx.issuer(c)
            if not iss:
                continue
            former_hit = any(_hi._norm_name(f.get("name") or "") == nnm
                             for f in iss.get("former_names") or [])
            rel = "RENAME_OR_SUCCESSION" if former_hit else \
                "NAME_MATCH_CANDIDATE"
            if idx.record_successor_evidence(
                    predecessor_security_id=sid, predecessor_cik=None,
                    successor_cik=c, successor_security_id=None, relationship=rel,
                    effective_date=sec.get("delisting_date"),
                    evidence={"norm_name": nnm, "former_hit": former_hit,
                              "successor_name": iss.get("name")},
                    confidence=0.5 if former_hit else 0.3):
                recorded += 1
    ctx.store.set_meta("stage101_successor_complete", _resolution_epoch(ctx))
    detail = {"real_work": "identity.successor_scan", "scanned": scanned,
              "successor_evidence_recorded": recorded,
              "successor_evidence_total":
                  idx.counts()["successor_evidence_rows"],
              "disposition": "DATA_HOLD", "no_automatic_promotion": True,
              "note": ("successor relationships preserved as EVIDENCE ONLY; "
                       "never auto-collapsed; predecessor identity unchanged")}
    detail["artifact"] = write_artifact(ctx.artifact_root, job.lane, detail,
                                        clock=ctx.clock)
    return OK, detail


def _readiness_recheck(ctx: IdentityJobContext, job) -> tuple:
    """Part 8: re-evaluate historical readiness FROM THE MEASURED store + the
    safety gate (reuses the Stage 10 readiness evaluator), then stamp the
    resolution-epoch flag. Never flips the safety switch."""
    outcome, detail = _readiness_eval(ctx, job)
    from . import autonomous_research as _ar
    if outcome == _ar.OUTCOME_COMPLETED:
        ctx.store.set_meta("stage101_readiness_complete", _resolution_epoch(ctx))
    detail["real_work"] = "identity.readiness_recheck"
    return outcome, detail


_LANE_HANDLERS = {
    LANE_DISCOVER: lambda ctx, job: _discover(ctx, job, delisted_only=False),
    LANE_DELISTED: lambda ctx, job: _discover(ctx, job, delisted_only=True),
    LANE_MEMBERSHIP: _membership,
    LANE_TICKER_HISTORY: _ticker_history,
    LANE_CIK_RESOLVE: _cik_resolve,
    LANE_CONFLICT_SCAN: _conflict_scan,
    LANE_REPAIR: _repair,
    LANE_COVERAGE: _coverage,
    LANE_COMPANYFACTS_PLAN: _companyfacts_plan,
    LANE_READINESS_EVAL: _readiness_eval,
    LANE_EVENT_MAP: _event_map,
    # Stage 10.1
    LANE_SEC_BULK_INVENTORY: _sec_bulk_inventory,
    LANE_SEC_SUBMISSIONS_INDEX: _sec_submissions_index,
    LANE_SEC_FILING_EVIDENCE_INDEX: _sec_filing_evidence_index,
    LANE_NORGATE_FULL_DISCOVERY: _norgate_full_discovery,
    LANE_CIK_FULL_RESOLUTION: _cik_full_resolution,
    LANE_MAPPING_COVERAGE_MEASURE: _mapping_coverage_measure,
    LANE_READINESS_RECHECK: _readiness_recheck,
    LANE_SUCCESSOR_SCAN: _successor_scan,
}


def dispatch_identity_job(job, ctx: IdentityJobContext) -> tuple:
    """Dispatch ONE claimed identity job to its lane handler. Bounded, idempotent
    (a job already recorded in the store's processed_jobs is a no-op COMPLETED),
    restart-safe and failure-isolated (a handler exception becomes a bounded
    RETRYABLE — never crashes the drain). Returns (outcome, detail)."""
    from . import autonomous_research as _ar
    lane = str(getattr(job, "lane", ""))
    handler = _LANE_HANDLERS.get(lane)
    if handler is None:
        return _ar.OUTCOME_BLOCKED_SPECIFIC, {
            "reason": "no identity handler for lane %s" % lane}
    job_key = "%s:%s" % (lane, _hi.content_hash(job.payload or {})[:16])
    if ctx.store.already_processed(job_key):
        return _ar.OUTCOME_COMPLETED, {"real_work": lane, "idempotent_skip": True,
                                       "disposition": "DATA_HOLD"}
    try:
        outcome, detail = handler(ctx, job)
    except Exception as exc:  # noqa: BLE001 - never crash the drain
        return _ar.OUTCOME_RETRYABLE, {
            "reason": "identity handler raised: %s" % type(exc).__name__,
            "lane": lane}
    if outcome == _ar.OUTCOME_COMPLETED:
        ctx.store.mark_processed(job_key, job_id=getattr(job, "job_id", None),
                                 kind=lane)
    detail.setdefault("lane", lane)
    return outcome, detail


# --------------------------------------------------------------------------- #
# Event/insider identity integration (Part 9).
# --------------------------------------------------------------------------- #
def map_event_issuer(store: _hi.IdentityStore, cik: Any,
                     event_date: Optional[str]) -> dict:
    """Map a Form 4 / 8-K issuer CIK to the effective security identity at the
    event date, preserving the filing-availability boundary and the effective
    ticker/name on that date. Ambiguous issuer/security relationships remain
    unresolved (never rewritten by a merger successor). Read-only."""
    c = _hi.norm_cik(cik)
    out = {"issuer_cik": c, "event_date": event_date, "security_id": None,
           "ticker_effective": None, "status": _hi.STATUS_UNRESOLVED}
    if c is None:
        out["reason"] = "unparseable CIK"
        return out
    # Find securities whose ACTIVE resolved mapping is this CIK.
    hits = []
    for s in store.list_securities(limit=100000):
        mp = store.active_mapping(s["security_id"])
        if mp and mp.get("status") == _hi.STATUS_RESOLVED and \
                _hi.norm_cik(mp.get("cik")) == c:
            hits.append(s)
    if not hits:
        out["reason"] = "no resolved security maps to this issuer CIK"
        return out
    if event_date:
        # Prefer the security whose life covers the event date (effective-dated).
        covering = [s for s in hits if _hi._overlaps(
            s["security_start_date"], s["security_end_date"], event_date,
            event_date)]
        hits = covering or hits
    if len(hits) != 1:
        out["status"] = _hi.STATUS_AMBIGUOUS
        out["candidate_security_ids"] = sorted(h["security_id"] for h in hits)
        out["reason"] = "issuer CIK maps to multiple securities at event date"
        return out
    s = hits[0]
    full = store.get_security(s["security_id"])
    ticker = s["ticker"]
    if event_date and full:
        ticker = _ticker_effective_on(full, event_date) or ticker
    out.update({"security_id": s["security_id"], "ticker_effective": ticker,
                "status": _hi.STATUS_RESOLVED})
    return out


def _ticker_effective_on(full: dict, as_of: str) -> Optional[str]:
    best = None
    for h in (full.get("ticker_history") or []):
        st = h.get("effective_start") or "0000-00-00"
        en = h.get("effective_end") or _hi._OPEN_DATE
        if st <= as_of <= en:
            best = h.get("ticker")
    return best


# --------------------------------------------------------------------------- #
# Autonomous dependency planner (Part 7). The AlphaAgent generates its OWN next
# identity job: it inspects the store + blocked candidates, picks the single
# highest-value next action, enqueues exactly one idempotent job (at most one
# live identity job at a time) and records the prioritization reason.
# --------------------------------------------------------------------------- #
def _has_live_identity_job(queue) -> bool:
    from . import autonomous_research as _ar
    for st in (_ar.STATE_QUEUED, _ar.STATE_RUNNING, _ar.STATE_RETRYABLE):
        for j in queue.list_jobs(state=st, limit=1000):
            if str(j.lane).startswith(IDENTITY_LANE_PREFIX) and \
                    j.origin in (ORIGIN, ORIGIN_101):
                return True
    return False


def _enqueue101(queue, ctx: IdentityJobContext, lane: str, category: str,
                reason: str, payload: dict, *,
                max_attempts: Optional[int] = None) -> dict:
    """Enqueue exactly ONE Stage 10.1 job (distinct origin, same allowlisted
    ``identity.`` prefix) and record the prioritization reason."""
    p = dict(payload or {})
    p["prioritization_reason"] = reason
    p["planned_at"] = ctx.now()
    p["stage"] = "10.1"
    pr = int((ctx.stage101 or {}).get("priority", DEFAULT_PRIORITY))
    job_id = queue.enqueue(category, lane=lane, payload=p, priority=pr,
                           origin=ORIGIN_101, max_attempts=max_attempts)
    ctx.store.set_meta("last_planned_reason", "%s | %s" % (lane, reason))
    return {"job_id": job_id, "lane": lane, "category": category,
            "reason": reason, "origin": ORIGIN_101, "payload": p}


def _plan_stage101(queue, ctx: IdentityJobContext) -> Optional[dict]:
    """Drive the one-time HISTORICAL CIK BRIDGE campaign in dependency order:
    download -> index submissions -> index owned/free ticker evidence -> full
    Norgate discovery -> full candidate-based resolution -> coverage -> readiness
    -> successor scan. Returns a plan for the SINGLE highest-value next step, or
    None when the whole bridge is measured for the current epoch (so the tournament
    reclaims the slot). The AlphaAgent — not Claude — generates each next action."""
    from . import autonomous_research as _ar
    cfg = ctx.stage101 or {}
    idx = ctx.issuer_index
    # 1) Acquire the SEC submissions bulk archive (restart-safe download).
    final, _ = _submissions_paths(cfg)
    if final is None or not final.exists():
        return _enqueue101(
            queue, ctx, LANE_SEC_BULK_INVENTORY, _ar.CAT_DATA_ACQUISITION,
            "SEC submissions bulk archive not yet acquired; download it "
            "(restart-safe, atomic, SHA-256)", {"stage": "download"},
            max_attempts=int(cfg.get("download_max_attempts", 12)))
    # 2) Stream the archive into the durable SEC issuer-history index.
    st = idx.archive_status("submissions") if idx is not None else {}
    if not st.get("complete"):
        return _enqueue101(
            queue, ctx, LANE_SEC_SUBMISSIONS_INDEX, _ar.CAT_DATA_ACQUISITION,
            "SEC issuer-history index incomplete: %s/%s members indexed"
            % (st.get("members_done", 0), st.get("total_members", 0)),
            {"members_done": st.get("members_done", 0),
             "total_members": st.get("total_members", 0)},
            max_attempts=int(cfg.get("index_max_attempts", 12)))
    # 3) Index owned/free CURRENT ticker maps + filing-derived observations once
    #    per submissions-archive version (the HANDLER sets the done flag on
    #    success, so a failed job re-runs; a new archive re-runs).
    icounts = idx.counts()
    ahash = _submissions_hash(cfg) or "1"
    if (ctx.store.get_meta("stage101_filing_evidence_done") or "") != ahash:
        return _enqueue101(
            queue, ctx, LANE_SEC_FILING_EVIDENCE_INDEX, _ar.CAT_DATA_ACQUISITION,
            "index current company_tickers[_exchange] + owned filing-derived "
            "ticker observations (%d issuers indexed)" % icounts["issuers"],
            {"issuers": icounts["issuers"]})
    # 4) Discover the COMPLETE Norgate survivorship universe.
    counts = ctx.store.counts()
    target = _universe_target(ctx)
    total = counts["total_securities"]
    if target and total < target:
        return _enqueue101(
            queue, ctx, LANE_NORGATE_FULL_DISCOVERY, _ar.CAT_DATA_ACQUISITION,
            "full Norgate discovery: %d/%d survivorship-safe securities indexed"
            % (total, target), {"cursor": total, "target": target})
    # 5) Resolve every not-yet-RESOLVED security via the full issuer index.
    epoch = _resolution_epoch(ctx)
    if ctx.store.get_meta("stage101_resolution_complete") != epoch:
        return _enqueue101(
            queue, ctx, LANE_CIK_FULL_RESOLUTION, _ar.CAT_DATA_ACQUISITION,
            "run the deterministic matching contract over every not-yet-resolved "
            "security enriched by the complete SEC issuer-history index",
            {"epoch": epoch, "total": total})
    # 6-8) Measure coverage, re-evaluate readiness, record successor evidence —
    #      each ONCE per resolution epoch. The HANDLER stamps the epoch flag on
    #      success, so a failed step re-runs and none is silently skipped.
    if ctx.store.get_meta("stage101_coverage_complete") != epoch:
        return _enqueue101(
            queue, ctx, LANE_MAPPING_COVERAGE_MEASURE, _ar.CAT_DATA_VALIDATION,
            "measure mapping coverage on every configured rebalance date + "
            "canonical mapping-version hash", {"epoch": epoch})
    if ctx.store.get_meta("stage101_readiness_complete") != epoch:
        return _enqueue101(
            queue, ctx, LANE_READINESS_RECHECK, _ar.CAT_DATA_VALIDATION,
            "re-evaluate per-rebalance historical readiness + safety gate from "
            "measured canonical state", {"epoch": epoch})
    if ctx.store.get_meta("stage101_successor_complete") != epoch:
        return _enqueue101(
            queue, ctx, LANE_SUCCESSOR_SCAN, _ar.CAT_DATA_ACQUISITION,
            "record predecessor/successor relationship evidence (never "
            "auto-collapsed)", {"epoch": epoch})
    return None


def plan_next_identity_job(queue, ctx: IdentityJobContext, *,
                           cfg: Optional[dict] = None,
                           blocked_candidate_signals: Optional[list] = None
                           ) -> Optional[dict]:
    """Determine and enqueue the SINGLE highest-value next identity action, or
    return None when the identity backlog is empty (so the queue is never flooded
    and the tournament is never permanently starved). At most ONE live identity
    job exists at a time (idempotent). The prioritization reason is persisted in
    the job payload + the store meta. Returns {'job_id','lane','reason',...}."""
    from . import autonomous_research as _ar
    cfg = cfg or {}
    if _has_live_identity_job(queue):
        return None
    # Stage 10.1 — the one-time HISTORICAL CIK BRIDGE campaign takes precedence
    # while it is incomplete; when fully measured for the current epoch it yields
    # (returns None) and the legacy Stage 10 maintenance / tournament reclaim the
    # slot. Driven only when Stage 10.1 is enabled AND the issuer index exists.
    s101 = ctx.stage101 or {}
    if s101.get("enabled") and s101.get("planner_enabled", True) and \
            ctx.issuer_index is not None:
        plan = _plan_stage101(queue, ctx)
        if plan is not None:
            return plan
    counts = ctx.store.counts()
    # Universe target size (bounded, read-only) for the discovery gate.
    universe_target = 0
    try:
        if ctx.accessor.available()[0]:
            universe_target = len(ctx.accessor.watchlist_symbols(
                ctx.survivorship_watchlist))
    except Exception:  # noqa: BLE001
        universe_target = 0

    lane = category = reason = None
    payload: dict = {}
    total = counts["total_securities"]
    decided = (counts["resolved"] + counts["unresolved"] +
               counts["ambiguous"] + counts["conflict"])
    # Backlog lanes are a FINITE burst at high priority; maintenance is a
    # THROTTLED low-priority tail so the tournament is never permanently starved.
    priority = int((cfg.get("priority") if isinstance(cfg, dict) else None)
                   or DEFAULT_PRIORITY)

    if total > decided:
        # Resolve already-discovered securities BEFORE discovering more, so the
        # matching contract + coverage/readiness make incremental progress on the
        # owned data already in hand rather than waiting for the whole universe.
        lane, category = LANE_CIK_RESOLVE, _ar.CAT_DATA_ACQUISITION
        reason = ("%d discovered securities have no mapping decision; run the "
                  "deterministic matching contract" % (total - decided))
        payload = {"cursor": decided, "batch": ctx.resolve_batch}
    elif universe_target and total < universe_target:
        lane, category = LANE_DISCOVER, _ar.CAT_DATA_ACQUISITION
        reason = ("discovery incomplete: %d/%d survivorship-safe securities "
                  "indexed; discover the next batch" % (total, universe_target))
        payload = {"cursor": total, "batch": ctx.discover_batch}
    elif total and counts["delisted_securities"] == 0 and universe_target:
        lane, category = LANE_DELISTED, _ar.CAT_DATA_ACQUISITION
        reason = "no delisted securities represented; extract delisted names"
        payload = {"cursor": total}
    elif counts["unresolved_backlog"] > 0 and ctx.repair_rules:
        lane, category = LANE_REPAIR, _ar.CAT_DATA_VALIDATION
        reason = ("%d unresolved securities and audited repair rules available"
                  % counts["unresolved_backlog"])
        payload = {"backlog": counts["unresolved_backlog"]}
    else:
        # Backlog drained. Measure coverage + re-evaluate readiness EXACTLY ONCE
        # per coverage-epoch (the epoch changes only when the identity state
        # genuinely changes), at LOW priority so the tournament reclaims the slot
        # and identity maintenance can never permanently starve it. When both are
        # done for the current epoch, return None (no identity work) — the
        # backlog being finite, the identity lane goes quiet.
        epoch = "%d:%d:%d:%d" % (total, counts["resolved"],
                                 counts["unresolved_backlog"],
                                 counts["conflict"])
        done = ctx.store.get_meta("maintenance_epoch_%s" % epoch) or ""
        if "coverage" not in done:
            lane, category = LANE_COVERAGE, _ar.CAT_DATA_VALIDATION
            reason = "backlog drained; refresh mapping-coverage snapshot (epoch)"
            ctx.store.set_meta("maintenance_epoch_%s" % epoch,
                               (done + ",coverage").strip(","))
        elif "readiness" not in done:
            lane, category = LANE_READINESS_EVAL, _ar.CAT_DATA_VALIDATION
            reason = "backlog drained; re-evaluate historical readiness (epoch)"
            ctx.store.set_meta("maintenance_epoch_%s" % epoch,
                               (done + ",readiness").strip(","))
        else:
            # Nothing to do: identity backlog empty and this epoch measured. The
            # planner yields no job so the tournament / companyfacts run.
            return None
        priority = 1
        payload = {"maintenance": True, "coverage_epoch": epoch}

    payload["prioritization_reason"] = reason
    payload["planned_at"] = ctx.now()
    job_id = queue.enqueue(category, lane=lane, payload=payload,
                           priority=priority, origin=ORIGIN)
    ctx.store.set_meta("last_planned_reason", "%s | %s" % (lane, reason))
    return {"job_id": job_id, "lane": lane, "category": category,
            "reason": reason, "payload": payload}


__all__ = [
    "ORIGIN", "ORIGIN_101", "IDENTITY_LANE_PREFIX", "LANE_DISCOVER",
    "LANE_DELISTED", "LANE_MEMBERSHIP", "LANE_TICKER_HISTORY", "LANE_CIK_RESOLVE",
    "LANE_CONFLICT_SCAN", "LANE_REPAIR", "LANE_COVERAGE",
    "LANE_COMPANYFACTS_PLAN", "LANE_READINESS_EVAL", "LANE_EVENT_MAP",
    "LANE_SEC_BULK_INVENTORY", "LANE_SEC_SUBMISSIONS_INDEX",
    "LANE_SEC_FILING_EVIDENCE_INDEX", "LANE_NORGATE_FULL_DISCOVERY",
    "LANE_CIK_FULL_RESOLUTION", "LANE_MAPPING_COVERAGE_MEASURE",
    "LANE_READINESS_RECHECK", "LANE_SUCCESSOR_SCAN",
    "IdentityJobContext", "dispatch_identity_job", "plan_next_identity_job",
    "map_event_issuer", "write_artifact", "DEFAULT_PRIORITY",
]
