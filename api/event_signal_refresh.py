r"""Release 28 — the ONE event-driven signal-refresh orchestration path.

WHAT THIS IS
------------
The canonical owner of the complete path from arriving information to a reviewable
portfolio decision::

    sources due -> fetch since watermark -> normalize -> deduplicate ->
    persist immutable evidence -> affected securities -> refresh only the affected
    calculations -> update freshness -> measure score / rank / risk deltas ->
    materiality gate -> portfolio reassessment when warranted -> record WHY ->
    complete target portfolio if justified -> MANUAL REVIEW

It is ONE path, not one path per source. Every source is an adapter behind
``api.event_fabric``; every calculation is an EXISTING canonical owner.

DAILY AND EVENT MODE ARE THE SAME SYSTEM
----------------------------------------
``api.daily_research_cycle`` is the FULL dependency refresh: it refreshes every input
and calls the same four owners in the same order. This module is the INCREMENTAL
dependency refresh: it calls the SAME owners, with the same arguments, for the subset
of calculations the arriving information actually invalidated. Neither forks a
business calculation — the shared owner list is asserted in
``CANONICAL_CALCULATION_DELEGATES`` and tested.

WHAT IT WILL NOT DO
-------------------
No order is created, no target is confirmed, no proposal is approved, no model is
promoted, no operational holding, cash or NAV is mutated, and no scheduler is armed.
The cycle is directly callable and idempotent so a future scheduler can call this same
owner without a redesign.
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from paper_trader.api import event_fabric as fabric
from paper_trader.api import source_capability as scap
from paper_trader.engine import event_fabric as ek
from paper_trader.engine import event_materiality as emat

PHASE = "RELEASE28"
COMPOSITION_OWNER = "api.event_signal_refresh"
SCHEMA_VERSION = "1.0.0"
CYCLE_ID = "PAPER_TRADER_EVENT_SIGNAL_REFRESH"

#: Token-gated like every other write path in this system.
EXECUTE_CONFIRM_TOKEN = "CONFIRM_EVENT_SIGNAL_REFRESH"

#: The canonical owners BOTH the daily cycle and the event cycle delegate to. This
#: tuple is the machine-checkable statement that the two modes share calculations.
CANONICAL_CALCULATION_DELEGATES = {
    ek.CALC_MARKET_RISK_STATE: "api.price_panel",
    ek.CALC_PORTFOLIO_VALUATION: "api.portfolio_state",
    ek.CALC_UNIVERSE_SCORING: "api.universe_scoring",
    ek.CALC_HOLDING_OPPORTUNITY_COST: "api.holding_opportunity_cost",
    ek.CALC_PORTFOLIO_REASSESSMENT: "api.portfolio_reassessment",
    ek.CALC_REALLOCATION_PROPOSAL: "api.reallocation_proposal",
}

# Terminal states of one event cycle.
ST_NO_NEW_INFORMATION = "NO_NEW_INFORMATION"
ST_INFORMATION_NOT_MATERIAL = "INFORMATION_NOT_MATERIAL"
ST_DUPLICATE_TRIGGER_SUPPRESSED = "DUPLICATE_TRIGGER_SUPPRESSED"
ST_REASSESSED_NO_CHANGE = "REASSESSED_NO_CHANGE"
ST_PROPOSAL_AVAILABLE = "PROPOSAL_AVAILABLE_FOR_MANUAL_REVIEW"
ST_BLOCKED = "BLOCKED"
ST_NOT_RUN = "NOT_RUN"
CYCLE_STATES = (ST_NO_NEW_INFORMATION, ST_INFORMATION_NOT_MATERIAL,
                ST_DUPLICATE_TRIGGER_SUPPRESSED, ST_REASSESSED_NO_CHANGE,
                ST_PROPOSAL_AVAILABLE, ST_BLOCKED, ST_NOT_RUN)

SAFETY_BADGES = ["PREVIEW ONLY", "NO LIVE ORDERS", "AUTOMATION OFF", "MANUAL REVIEW"]

NOW_ENV = fabric.NOW_ENV


def _now() -> datetime:
    raw = os.environ.get(NOW_ENV)
    if raw:
        try:
            parsed = datetime.fromisoformat(raw)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return datetime.now(tz=timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


def _parse_dt(value: Any) -> Optional[datetime]:
    if not value:
        return None
    s = str(value).replace("Z", "+00:00")
    for candidate in (s, s[:19], s[:10]):
        try:
            dt = datetime.fromisoformat(candidate)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _f(x: Any) -> Optional[float]:
    try:
        return None if x is None else float(x)
    except (TypeError, ValueError):
        return None


def _summarize_hoc(result: Optional[dict]) -> Optional[dict]:
    """A bounded READ of what the canonical opportunity-cost owner decided."""
    a = (result or {}).get("assessment") or {}
    if not a:
        return None
    return {
        "calculation_owner": a.get("calculation_owner"),
        "assessment_state": a.get("assessment_state"),
        "assessment_hash": a.get("assessment_hash"),
        "eligible_market_date": a.get("eligible_market_date"),
        "recommendation_counts": a.get("recommendation_counts"),
        "recommendation_vocabulary": a.get("recommendation_vocabulary"),
        "holdings_reviewed": len(a.get("holding_reviews") or []),
        "addition_candidates": len(a.get("addition_candidates") or []),
    }


def _summarize_reassessment(result: Optional[dict]) -> Optional[dict]:
    r = (result or {}).get("reassessment") or {}
    if not r:
        return None
    d = r.get("decision") or {}
    return {
        "calculation_owner": r.get("calculation_owner"),
        "reassessment_state": r.get("reassessment_state"),
        "reassessment_hash": r.get("reassessment_hash"),
        "explanation": r.get("explanation") or r.get("summary_text"),
        "blockers": d.get("blockers"),
        "actionable_holdings": d.get("actionable_holdings"),
        "expected_net_improvement": d.get("expected_net_improvement"),
        "net_improvement_hurdle": d.get("net_improvement_hurdle"),
        "expected_one_way_turnover": d.get("expected_one_way_turnover"),
        "expected_transaction_cost_usd": d.get("expected_transaction_cost_usd"),
        "expected_return_state": d.get("expected_return_state"),
        "proposal_required": d.get("proposal_required"),
    }


def _summarize_proposal(result: Optional[dict]) -> Optional[dict]:
    """The COMPLETE target portfolio, summarised: what changed, what was retained, at
    what turnover and cost, and how risk/concentration move. Review-only."""
    p = (result or {}).get("proposal") or {}
    if not p:
        return None
    return {
        "calculation_owner": p.get("calculation_owner"),
        "proposal_state": p.get("proposal_state"),
        "proposal_hash": p.get("proposal_hash"),
        "eligible_market_date": p.get("eligible_market_date"),
        "action_counts": p.get("action_counts"),
        "action_vocabulary": p.get("action_vocabulary"),
        "allocations": p.get("allocations"),
        "turnover": p.get("turnover"),
        "risk": p.get("risk"),
        "signal": p.get("signal"),
        "portfolio": p.get("portfolio"),
        "constraints": p.get("constraints"),
        "data_gaps": p.get("data_gaps"),
        "hoc_reference": p.get("hoc_reference"),
        "safety": p.get("safety"),
    }


def _safety(performed_write: bool) -> dict:
    return {
        "read_only": not performed_write,
        "performed_write": bool(performed_write),
        "creates_orders": False,
        "confirms_target": False,
        "approves_proposal": False,
        "promotes_model": False,
        "mutates_operational_holdings": False,
        "mutates_operational_cash_or_nav": False,
        "enables_automation": False,
        "scheduler_armed": False,
        "badges": list(SAFETY_BADGES),
    }


class _Step:
    """One measured orchestration step.

    Each step is also a PROGRESS CHECKPOINT. The continuous collection worker
    passes its progress callback in, and entering/leaving a step is proof that a
    bounded unit of work happened — which is how a multi-minute cycle stays
    provably healthy instead of ageing out of a start-of-iteration heartbeat.
    """

    def __init__(self, steps: list, step_id: str, owner: str,
                 progress: Optional[Callable] = None) -> None:
        self.steps = steps
        self.step_id = step_id
        self.owner = owner
        self.progress = progress
        self.started = 0.0
        self.record: dict = {}

    def __enter__(self):
        self.started = time.time()
        self.record = {"step": self.step_id, "owner": self.owner,
                       "started_at": _now_iso(), "status": "RUNNING", "detail": None}
        self.steps.append(self.record)
        fabric.emit_progress(self.progress, "EVENT_CYCLE",
                             "%s started (%s)" % (self.step_id, self.owner))
        return self.record

    def __exit__(self, exc_type, exc, tb):
        self.record["duration_seconds"] = round(time.time() - self.started, 4)
        self.record["finished_at"] = _now_iso()
        if exc is not None:
            self.record["status"] = "FAILED"
            self.record["detail"] = str(exc)[:300]
            fabric.emit_progress(self.progress, "EVENT_CYCLE",
                                 "%s FAILED" % self.step_id)
            return False
        if self.record["status"] == "RUNNING":
            self.record["status"] = "OK"
        fabric.emit_progress(self.progress, "EVENT_CYCLE",
                             "%s done in %.1fs"
                             % (self.step_id, self.record["duration_seconds"]))
        return False


# --------------------------------------------------------------------------- #
# Default seams (production wiring). Every one is injectable so replay/tests never
# touch a production store or a provider.
# --------------------------------------------------------------------------- #
def _default_portfolio_state_loader():
    from paper_trader.api import portfolio_state as ps
    return ps.load_portfolio_state()


def _default_scoring_loader():
    from paper_trader.api import universe_scoring as us
    return us.build_universe_scoring()


def _default_price_panel_loader():
    from paper_trader.api import price_panel as pp
    return pp.load_operational_price_panel()


def _default_hoc_fn(*, scoring=None, hoc_dir=None):
    """The canonical Holding Opportunity-Cost owner — the SAME entry point the Daily
    Research Cycle uses. This module computes no opportunity cost of its own."""
    from paper_trader.api import holding_opportunity_cost as hoc
    return hoc.run_and_persist(scoring=scoring, hoc_dir=hoc_dir)


def _default_reassessment_fn(*, scoring=None, hoc_assessment=None, freshness=None,
                             reassessment_dir=None, hoc_dir=None):
    """The canonical Portfolio Reassessment owner (the economic-change gate) — the SAME
    entry point the Daily Research Cycle uses."""
    from paper_trader.api import portfolio_reassessment as prs
    return prs.run_and_persist(scoring=scoring, hoc_assessment=hoc_assessment,
                               freshness=freshness, reassessment_dir=reassessment_dir,
                               hoc_dir=hoc_dir)


def _default_proposal_fn(*, scoring=None, hoc_assessment=None, reallocation_dir=None,
                         hoc_dir=None):
    """The canonical Reallocation Proposal owner — the SAME entry point the Daily
    Research Cycle uses. Review-only: it confirms no target and creates no order."""
    from paper_trader.api import reallocation_proposal as rp
    return rp.run_and_persist(scoring=scoring, hoc_assessment=hoc_assessment,
                              reallocation_dir=reallocation_dir, hoc_dir=hoc_dir)


def _default_prior_ranking(*, active_book_id, eligible_market_date, hoc_dir=None):
    from paper_trader.api import holding_opportunity_cost as hoc
    art = hoc.load_latest_artifact(active_book_id=active_book_id,
                                   eligible_market_date=eligible_market_date,
                                   hoc_dir=hoc_dir)
    if not art:
        return None
    return ((art.get("assessment") or {}).get("diagnostics") or {}).get("rank_snapshot")


def _default_proposal_gate(reassessment):
    from paper_trader.api import portfolio_reassessment as prs
    return prs.should_build_proposal(reassessment)


# --------------------------------------------------------------------------- #
# Market / risk state — computed by the CANONICAL price-panel owner
# --------------------------------------------------------------------------- #
def latest_quote_prices(events: Optional[list]) -> dict:
    """The most recent DELAYED_QUOTE price per ticker out of this cycle's events.

    The quote lane's only decision value is the move it measures against the owned
    close. Reading it here — from the events the fabric already admitted — keeps that
    measurement in the risk owner and adds no second market-data client. The price is
    read from ``materiality_inputs``, which is where the event contract carries the
    values the gate is allowed to judge; an event never carries its raw payload.
    """
    out: dict[str, float] = {}
    for e in (events or []):
        if str((e or {}).get("family")) != ek.F_MARKET_QUOTE:
            continue
        tk = str((e.get("primary_ticker") or "")).upper()
        price = _f(((e.get("materiality_inputs") or {}).get("price")))
        if tk and price is not None and price > 0:
            out[tk] = price
    return out


def build_market_risk_state(*, price_panel: Optional[dict], tickers, eligible: Any,
                            latest_quotes: Optional[dict] = None) -> dict:
    """Per-holding risk state from the canonical owned panel.

    Every primitive (trailing return, realized volatility, max drawdown, median dollar
    volume) is computed by ``api.price_panel`` — this module hosts no second risk
    engine. ``volatility_ratio`` is the ratio of the owner's own 63-day realized
    volatility to its 126-day realized volatility: short-window risk relative to the
    name's own longer-run level.

    ``latest_quotes`` overlays the fastest legitimately available price on top of the
    owned close as ``ret_intraday``. It is a RISK measurement only: it never becomes
    a mark, never enters the panel and never touches a score. Without it the 15-minute
    quote lane could not put a same-session collapse on the review list before the
    next end-of-day bar exists.
    """
    from paper_trader.api import price_panel as pp
    series = (price_panel or {}).get("series") or {}
    eligible_s = str(eligible or "")[:10]
    quotes = {str(k).upper(): _f(v) for k, v in (latest_quotes or {}).items()}
    out: dict[str, dict] = {}
    missing: list[str] = []
    quoted = 0
    for tk in sorted({str(t).upper() for t in (tickers or [])}):
        s = series.get(tk)
        if not s or not s.get("dates"):
            missing.append(tk)
            continue
        j = pp.asof_index(s.get("dates") or [], eligible_s) if eligible_s else (
            len(s["dates"]) - 1)
        if j < 0:
            missing.append(tk)
            continue
        feats = pp.compute_features(s, j)
        rv63, rv126 = _f(feats.get("rvol_63")), _f(feats.get("rvol_126"))
        close = _f((s.get("adj") or [None] * (j + 1))[j]) if (s.get("adj")) else None
        quote = quotes.get(tk)
        ret_intraday = ((quote / close - 1.0)
                        if (quote is not None and close) else None)
        if ret_intraday is not None:
            quoted += 1
        out[tk] = {
            "as_of": s["dates"][j],
            "intraday_quote": quote,
            "intraday_reference_close": close,
            "ret_intraday": ret_intraday,
            "ret_1": feats.get("ret_1"),
            "ret_5": feats.get("ret_5"),
            "ret_21": feats.get("ret_21"),
            "rs_63": feats.get("rs_63"),
            "rvol_63": rv63,
            "rvol_126": rv126,
            "volatility_ratio": ((rv63 / rv126) if (rv63 is not None and rv126)
                                 else None),
            "maxdd_252": feats.get("maxdd_252"),
            "beta_63": feats.get("beta_63"),
            "median_dollar_volume": pp.trailing_median_dollar_volume(s, j, 20),
            "calculation_owner": "api.price_panel",
        }
    return {"calculation_owner": "api.price_panel", "eligible_market_date": eligible_s,
            "rows": out, "covered": len(out), "missing": missing,
            "intraday_quoted": quoted,
            "intraday_note": ("ret_intraday compares the delayed quote to the owned "
                              "close for the eligible market date. It is a risk "
                              "measurement only: it is never written to the panel, "
                              "never becomes the portfolio mark and never moves a "
                              "score."),
            "coverage_ratio": (len(out) / (len(out) + len(missing))
                               if (out or missing) else None)}


def build_rank_deltas(*, scoring: Optional[dict], prior_ranking: Optional[dict],
                      held) -> dict:
    """Rank / score deltas for held names against a REAL prior snapshot.

    The prior ranking is the previously persisted opportunity-cost artifact's rank
    snapshot. When none exists the delta is honestly absent — today's snapshot is never
    substituted for a prior one.
    """
    rows = list((scoring or {}).get("rankings") or [])
    current = {str(r.get("ticker")).upper(): r for r in rows if r.get("ticker")}
    prior = {str(k).upper(): v for k, v in (prior_ranking or {}).items()}
    held_set = {str(t).upper() for t in (held or [])}
    best_rank = None
    held_ranks = {}
    for tk in held_set:
        r = current.get(tk)
        if r and r.get("rank") is not None:
            held_ranks[tk] = int(r["rank"])
    # The best eligible ALTERNATIVE: the top-ranked name that is not currently held.
    for r in rows:
        tk = str(r.get("ticker") or "").upper()
        if tk and tk not in held_set and r.get("rank") is not None:
            best_rank = int(r["rank"])
            break

    out: dict[str, dict] = {}
    for tk in sorted(held_set):
        cur = current.get(tk) or {}
        rank_after = cur.get("rank")
        raw_prior = prior.get(tk)
        rank_before = None
        if isinstance(raw_prior, dict):
            rank_before = raw_prior.get("rank")
        elif raw_prior is not None:
            rank_before = raw_prior
        row = {
            "rank_before": (int(rank_before) if rank_before is not None else None),
            "rank_after": (int(rank_after) if rank_after is not None else None),
            "score_after": _f(cur.get("combined_score")),
            "score_before": (_f(raw_prior.get("combined_score"))
                             if isinstance(raw_prior, dict) else None),
            "prior_available": raw_prior is not None,
        }
        if rank_after is not None and best_rank is not None:
            row["best_alternative_rank_advantage"] = int(rank_after) - int(best_rank)
        out[tk] = row
    return {"calculation_owner": "api.universe_scoring", "rows": out,
            "prior_available": bool(prior),
            "best_alternative_rank": best_rank,
            "held_ranked": len(held_ranks),
            "note": ("Deltas are measured against the previously PERSISTED rank "
                     "snapshot. With no prior snapshot the delta is absent, never "
                     "zero.")}


# --------------------------------------------------------------------------- #
# Latency observability
# --------------------------------------------------------------------------- #
def measure_latency(*, events: list, steps: list, reassessment_at: Any = None,
                    proposal_at: Any = None) -> dict:
    """Measured source -> ingest -> signal -> reassessment -> proposal timings.

    Only what can actually be measured is reported. An event whose source stated no
    publication time contributes no latency figure; it is counted as unmeasurable
    rather than given an invented one.
    """
    per_source: dict[str, list] = {}
    unmeasurable = 0
    for ev in (events or []):
        pub = _parse_dt(ev.get("published_at")) or _parse_dt(ev.get("accepted_at"))
        ing = _parse_dt(ev.get("ingested_at"))
        if pub is None or ing is None:
            unmeasurable += 1
            continue
        per_source.setdefault(str(ev.get("source_id")), []).append(
            (ing - pub).total_seconds())
    source_rows = []
    for sid, vals in sorted(per_source.items()):
        ordered = sorted(vals)
        n = len(ordered)
        source_rows.append({
            "source_id": sid, "measured_events": n,
            "median_source_to_ingest_seconds": round(ordered[n // 2], 1),
            "max_source_to_ingest_seconds": round(ordered[-1], 1),
            "min_source_to_ingest_seconds": round(ordered[0], 1),
        })
    step_rows = [{"step": s.get("step"), "owner": s.get("owner"),
                  "duration_seconds": s.get("duration_seconds"),
                  "status": s.get("status")} for s in (steps or [])]
    total = round(sum(_f(s.get("duration_seconds")) or 0.0 for s in (steps or [])), 3)

    end_to_end = None
    ra = _parse_dt(reassessment_at)
    if ra is not None:
        pubs = [p for p in (_parse_dt(e.get("published_at")) for e in (events or []))
                if p is not None]
        if pubs:
            end_to_end = round((ra - min(pubs)).total_seconds(), 1)
    return {
        "contract_id": "paper_trader.event_latency_observability/1",
        "per_source": source_rows,
        "unmeasurable_events": unmeasurable,
        "measured_events": sum(r["measured_events"] for r in source_rows),
        "steps": step_rows,
        "cycle_duration_seconds": total,
        "oldest_event_to_reassessment_seconds": end_to_end,
        "reassessment_at": (str(reassessment_at) if reassessment_at else None),
        "proposal_available_at": (str(proposal_at) if proposal_at else None),
        "note": ("Measured, not modelled. Events whose source stated no publication "
                 "time are counted as unmeasurable instead of being given a fabricated "
                 "timestamp."),
    }


# --------------------------------------------------------------------------- #
# THE cycle
# --------------------------------------------------------------------------- #
def run_event_signal_refresh(
        *, confirm: Optional[str] = None, requested_by: Optional[str] = None,
        fabric_dir=None, hoc_dir=None, reassessment_dir=None, reallocation_dir=None,
        ingestion_root=None, news_root=None,
        portfolio_state: Optional[dict] = None, scoring: Optional[dict] = None,
        price_panel: Optional[dict] = None,
        portfolio_state_loader: Optional[Callable] = None,
        scoring_loader: Optional[Callable] = None,
        price_panel_loader: Optional[Callable] = None,
        corpus_events: Optional[list] = None,
        include_market_quotes: bool = False, include_gdelt: bool = False,
        quote_fetcher: Optional[Callable] = None,
        gdelt_fetcher: Optional[Callable] = None,
        entity_index: Optional[dict] = None,
        prior_ranking: Optional[dict] = None,
        prior_ranking_fn: Optional[Callable] = None,
        hoc_fn: Optional[Callable] = None,
        reassessment_fn: Optional[Callable] = None,
        proposal_fn: Optional[Callable] = None,
        proposal_gate_fn: Optional[Callable] = None,
        policy_overrides: Optional[dict] = None,
        lookback_days: int = fabric.DEFAULT_LOOKBACK_DAYS,
        candidate_depth: int = 100,
        now_iso: Optional[str] = None,
        progress_fn: Optional[Callable] = None,
        regime_before: Any = None, regime_after: Any = None) -> dict:
    """Run ONE event cycle. Idempotent: unchanged inputs produce no new decision.

    Returns the authoritative status of the cycle: what arrived, what it invalidated,
    whether it was material, what was recomputed, why a reassessment did or did not
    run, and what the operator must review.

    ``now_iso`` is the CALLER'S clock and becomes the identity stamp of every event
    the live adapters build in this cycle. The continuous collection service passes
    its own iteration clock, so there is ONE clock per cycle: an adapter never reads
    an ambient time the cycle does not know about.
    """
    started_iso = _now_iso()
    # ONE clock for this cycle. Live-adapter event identity is derived from it, never
    # from a second ambient read taken inside an adapter.
    cycle_now_iso = str(now_iso) if now_iso else started_iso
    steps: list[dict] = []
    warnings: list[str] = []
    blockers: list[dict] = []

    def _step(step_id: str, owner: str) -> "_Step":
        """One measured step, bound to this run's progress observer."""
        return _Step(steps, step_id, owner, progress=progress_fn)

    if str(confirm or "") != EXECUTE_CONFIRM_TOKEN:
        return {
            "schema_version": SCHEMA_VERSION, "phase": PHASE,
            "composition_owner": COMPOSITION_OWNER, "cycle_id": CYCLE_ID,
            "state": ST_NOT_RUN, "generated_at": started_iso,
            "confirm_required": EXECUTE_CONFIRM_TOKEN,
            "message": ("The event cycle is token-gated. Pass confirm=%r to run it."
                        % EXECUTE_CONFIRM_TOKEN),
            "safety": _safety(False),
        }

    ps_load = portfolio_state_loader or _default_portfolio_state_loader
    sc_load = scoring_loader or _default_scoring_loader
    pp_load = price_panel_loader or _default_price_panel_loader
    hoc_call = hoc_fn or _default_hoc_fn
    reassess_call = reassessment_fn or _default_reassessment_fn
    proposal_call = proposal_fn or _default_proposal_fn
    gate_call = proposal_gate_fn or _default_proposal_gate
    prior_fn = prior_ranking_fn or _default_prior_ranking

    # ---- 1. portfolio context (what we are reacting FOR) ------------------- #
    with _step("LOAD_PORTFOLIO_CONTEXT", "api.portfolio_state") as rec:
        ps = portfolio_state if portfolio_state is not None else ps_load()
        held = sorted({str(p.get("ticker")).upper()
                       for p in ((ps or {}).get("positions") or [])
                       if p.get("ticker")})
        active_book = ((ps or {}).get("active_book") or {}).get("book_id")
        eligible = ((ps or {}).get("dates") or {}).get("eligible_market_date")
        state_hash = (ps or {}).get("economic_state_hash") or (ps or {}).get("state_hash")
        rec["detail"] = "%d holdings; book=%s; eligible=%s" % (
            len(held), active_book, eligible)

    # ---- 2. which sources are due --------------------------------------- #
    with _step("RESOLVE_SOURCES_DUE", COMPOSITION_OWNER) as rec:
        capability = scap.build_capability_matrix(ingestion_root=ingestion_root,
                                                  news_root=news_root)
        watermarks = fabric.load_watermarks(fabric_dir=fabric_dir)
        freshness_before = fabric.build_source_freshness(
            capability=capability, watermarks=watermarks, anchor=eligible,
            fabric_dir=fabric_dir)
        due = [r["source_id"] for r in freshness_before["sources"]
               if r["integrated"] and r["status"] != "FRESH"]
        rec["detail"] = "%d integrated source(s) not confirmed fresh" % len(due)

    # ---- 3-4. fetch since watermark + normalize --------------------------- #
    raw_events: list[dict] = []
    adapter_results: dict[str, dict] = {}
    with _step("INGEST_SINCE_WATERMARK", "api.event_fabric") as rec:
        if corpus_events is not None:
            raw_events.extend(corpus_events)
            adapter_results["corpus"] = {"injected": True,
                                         "event_count": len(corpus_events)}
        else:
            idx = entity_index
            if idx is None:
                idx = fabric.build_entity_index(held, ingestion_root=ingestion_root)
            entity_index = idx
            corpus = fabric.ingest_corpus_lane(
                tickers=held, lookback_days=lookback_days,
                ingestion_root=ingestion_root, news_root=news_root,
                entity_index=idx, progress_fn=progress_fn)
            raw_events.extend(corpus["events"])
            adapter_results["corpus"] = {
                "event_count": corpus["event_count"],
                "scanned_files": corpus["scanned_files"],
                "per_source": corpus["per_source"],
                "bounded_by": corpus["bounded_by"]}
        if include_market_quotes:
            quotes = fabric.capture_market_quotes(held, fetcher=quote_fetcher,
                                                  now_iso=cycle_now_iso,
                                                  progress_fn=progress_fn)
            raw_events.extend(quotes["events"])
            adapter_results["market_quotes"] = {
                k: v for k, v in quotes.items() if k != "events"}
            if not quotes.get("ok"):
                warnings.append("MARKET_QUOTE adapter degraded: %s"
                                % quotes.get("detail"))
        if include_gdelt:
            gd = fabric.capture_gdelt_news(held, entity_index=entity_index,
                                           fetcher=gdelt_fetcher,
                                           now_iso=cycle_now_iso,
                                           progress_fn=progress_fn)
            raw_events.extend(gd["events"])
            adapter_results["gdelt"] = {k: v for k, v in gd.items() if k != "events"}
            if not gd.get("ok"):
                warnings.append("GDELT adapter degraded: %s" % gd.get("detail"))
        rec["detail"] = "%d normalized event(s) built" % len(raw_events)

    # ---- 5. deduplicate + persist immutable evidence ---------------------- #
    with _step("DEDUPLICATE_AND_PERSIST", "api.event_fabric") as rec:
        appended = fabric.append_events(raw_events, fabric_dir=fabric_dir)
        admitted = appended["admitted"]
        rec["detail"] = ("%d admitted, %d duplicate(s) suppressed"
                         % (appended["admitted_count"],
                            appended["duplicates_suppressed"]))

    unclassified = ek.unclassified_authority_count(admitted)
    if unclassified:
        blockers.append({"code": "UNCLASSIFIED_SIGNAL_AUTHORITY",
                         "detail": ("%d admitted event(s) carry no explicit decision "
                                    "authority." % unclassified)})

    # ---- 6. affected securities + concepts + calculations ----------------- #
    with _step("RESOLVE_DEPENDENCIES", "engine.event_fabric") as rec:
        informative = [e for e in admitted if ek.carries_new_information(e)]
        concepts = ek.concepts_for_events(informative)
        calculations = ek.affected_calculations(concepts)
        affected_entities = sorted({t for e in informative
                                    for t in (e.get("entities") or [])})
        affected_holdings = sorted(set(affected_entities) & set(held))
        rec["detail"] = ("%d concept(s) -> %d calculation(s); %d affected holding(s)"
                         % (len(concepts), len(calculations), len(affected_holdings)))

    # ---- 7. refresh only the affected inputs ------------------------------ #
    with _step("REFRESH_AFFECTED_INPUTS", "api.universe_scoring") as rec:
        sc = scoring
        needs_scoring = ek.CALC_UNIVERSE_SCORING in calculations
        if sc is None and (needs_scoring or ek.CALC_HOLDING_OPPORTUNITY_COST
                           in calculations):
            sc = sc_load()
        panel = price_panel
        needs_risk = ek.CALC_MARKET_RISK_STATE in calculations
        if panel is None and needs_risk:
            panel = pp_load()
        rec["detail"] = ("scoring=%s risk_panel=%s"
                         % ("refreshed" if sc is not None else "not needed",
                            "refreshed" if panel is not None else "not needed"))
        rec["refreshed_calculations"] = list(calculations)

    # ---- 8. update freshness / watermarks --------------------------------- #
    with _step("ADVANCE_WATERMARKS", "api.event_fabric") as rec:
        watermarks = fabric.advance_watermarks(
            watermarks=watermarks,
            per_source=(adapter_results.get("corpus") or {}).get("per_source") or {},
            admitted=admitted, duplicates=appended["duplicates_suppressed"])
        fabric.save_watermarks(watermarks, fabric_dir=fabric_dir)
        freshness_after = fabric.build_source_freshness(
            capability=capability, watermarks=watermarks, anchor=eligible,
            fabric_dir=fabric_dir)
        rec["detail"] = "%d degraded source(s)" % freshness_after["degraded_count"]

    # ---- 9. measure deltas ------------------------------------------------ #
    with _step("MEASURE_DELTAS", "api.price_panel + api.universe_scoring") as rec:
        # The quote lane speaks through the RISK owner and its thresholds, never by
        # merely existing: a delayed quote is an observation, and an observation is
        # material only when the move it measures crosses a stated level.
        risk_state = build_market_risk_state(
            price_panel=panel, tickers=held, eligible=eligible,
            latest_quotes=latest_quote_prices(informative)) if panel else {
            "rows": {}, "covered": 0, "missing": list(held),
            "calculation_owner": "api.price_panel",
            "coverage_ratio": 0.0 if held else None}
        pr = prior_ranking
        if pr is None:
            try:
                pr = prior_fn(active_book_id=active_book, eligible_market_date=eligible,
                              hoc_dir=hoc_dir)
            except Exception as exc:  # noqa: BLE001 - a missing prior is not a crash
                warnings.append("prior ranking unavailable: %s" % str(exc)[:160])
                pr = None
        rank_deltas = build_rank_deltas(scoring=sc, prior_ranking=pr, held=held)
        candidates = sorted({str(r.get("ticker")).upper()
                             for r in ((sc or {}).get("rankings") or [])[:candidate_depth]
                             if r.get("ticker")})
        rec["detail"] = ("risk rows=%d; rank rows=%d; prior_ranking=%s"
                         % (risk_state.get("covered") or 0,
                            len(rank_deltas["rows"]),
                            "available" if rank_deltas["prior_available"] else "absent"))

    # ---- 10. materiality gate --------------------------------------------- #
    with _step("MATERIALITY_GATE", "engine.event_materiality") as rec:
        prior_fp = _read_last_fingerprint(fabric_dir=fabric_dir)
        materiality = emat.assess_materiality(
            events=admitted, risk_state=risk_state.get("rows") or {},
            rank_deltas=rank_deltas["rows"], holdings=held, candidates=candidates,
            regime_before=regime_before, regime_after=regime_after,
            portfolio_state_hash=state_hash, prior_trigger_fingerprint=prior_fp,
            policy_overrides=dict(policy_overrides or {},
                                  candidate_depth=candidate_depth))
        rec["detail"] = "%s (%d trigger(s))" % (materiality["change_level"],
                                                materiality["trigger_count"])

    # ---- 11-12. reassess when warranted, and record WHY ------------------- #
    hoc_result = None
    reassessment = None
    proposal = None
    reassessment_at = None
    proposal_at = None
    # State precedence. A blocker outranks everything; a repeated trigger fingerprint
    # outranks the triggers that produced it; and MATERIALITY — not the mere arrival of
    # bytes — decides whether the portfolio question is asked. A risk threshold breached
    # by already-owned marks is material even when no new event arrived this cycle.
    if blockers:
        state = ST_BLOCKED
    elif materiality["duplicate_of_prior_trigger"]:
        state = ST_DUPLICATE_TRIGGER_SUPPRESSED
    elif not materiality["reassessment_required"]:
        state = (ST_INFORMATION_NOT_MATERIAL if materiality["data_changed"]
                 else ST_NO_NEW_INFORMATION)
    else:
        with _step("HOLDING_OPPORTUNITY_COST",
                   CANONICAL_CALCULATION_DELEGATES[ek.CALC_HOLDING_OPPORTUNITY_COST]) as rec:
            hoc_result = hoc_call(scoring=sc, hoc_dir=hoc_dir)
            rec["detail"] = "assessment built by the canonical owner"
        with _step("PORTFOLIO_REASSESSMENT",
                   CANONICAL_CALCULATION_DELEGATES[ek.CALC_PORTFOLIO_REASSESSMENT]) as rec:
            reassessment = reassess_call(
                scoring=sc, hoc_assessment=(hoc_result or {}).get("assessment"),
                freshness=None, reassessment_dir=reassessment_dir, hoc_dir=hoc_dir)
            reassessment_at = _now_iso()
            rec["detail"] = str(
                ((reassessment or {}).get("reassessment") or {}).get(
                    "reassessment_state") or "built")
        gate = {}
        try:
            gate = gate_call((reassessment or {}).get("reassessment") or reassessment)
        except Exception as exc:  # noqa: BLE001 - the gate must not crash the cycle
            warnings.append("proposal gate unavailable: %s" % str(exc)[:160])
        if gate.get("build_proposal"):
            with _step("REALLOCATION_PROPOSAL",
                       CANONICAL_CALCULATION_DELEGATES[ek.CALC_REALLOCATION_PROPOSAL]) as rec:
                proposal = proposal_call(
                    scoring=sc, hoc_assessment=(hoc_result or {}).get("assessment"),
                    reallocation_dir=reallocation_dir, hoc_dir=hoc_dir)
                proposal_at = _now_iso()
                rec["detail"] = "complete target portfolio built for manual review"
            state = ST_PROPOSAL_AVAILABLE
        else:
            state = ST_REASSESSED_NO_CHANGE
        _write_last_fingerprint(materiality["trigger_fingerprint"], fabric_dir=fabric_dir)

    latency = measure_latency(events=admitted, steps=steps,
                              reassessment_at=reassessment_at, proposal_at=proposal_at)

    payload = {
        "schema_version": SCHEMA_VERSION,
        "phase": PHASE,
        "composition_owner": COMPOSITION_OWNER,
        "cycle_id": CYCLE_ID,
        "state": state,
        "state_vocabulary": list(CYCLE_STATES),
        "generated_at": started_iso,
        "completed_at": _now_iso(),
        "requested_by": (str(requested_by) if requested_by else None),
        "active_book_id": active_book,
        "eligible_market_date": eligible,
        "portfolio_state_hash": state_hash,
        "holdings": held,
        "steps": steps,
        "sources_due": due,
        "adapters": adapter_results,
        "events_built": len(raw_events),
        "events_admitted": appended["admitted_count"],
        "duplicates_suppressed": appended["duplicates_suppressed"],
        "events_carrying_new_information": len(informative),
        "unclassified_signal_authority": unclassified,
        "concepts_invalidated": concepts,
        "signals_invalidated": ek.affected_signals(concepts),
        "calculations_refreshed": calculations,
        # REFRESH SCOPE, not the attention list: which holdings had an input
        # invalidated (a quote invalidates risk for its name). The holdings a MATERIAL
        # event named are ``materiality.affected_entities``.
        "affected_entities": affected_entities,
        "affected_holdings": affected_holdings,
        "market_risk_state": risk_state,
        "rank_deltas": rank_deltas,
        "materiality": materiality,
        "reassessment_ran": reassessment is not None,
        "reassessment_reason": materiality["reassessment_reason"],
        "proposal_built": proposal is not None,
        "holding_opportunity_cost": _summarize_hoc(hoc_result),
        "portfolio_reassessment": _summarize_reassessment(reassessment),
        "target_portfolio": _summarize_proposal(proposal),
        "source_freshness": freshness_after,
        "latency": latency,
        "warnings": warnings,
        "blockers": blockers,
        "canonical_delegates": dict(CANONICAL_CALCULATION_DELEGATES),
        "same_owners_as_daily_cycle": True,
        "manual_review_required": bool(proposal is not None),
        "safety": _safety(True),
        "note": ("The event cycle is the INCREMENTAL dependency refresh. It calls the "
                 "same canonical owners as the Daily Research Cycle for the subset of "
                 "calculations the arriving information invalidated, and terminates in "
                 "manual review."),
    }
    _persist_run(payload, fabric_dir=fabric_dir)
    return payload


# --------------------------------------------------------------------------- #
# Anti-churn fingerprint persistence + run artifacts
# --------------------------------------------------------------------------- #
def _fingerprint_path(fabric_dir=None):
    return fabric.state_root(fabric_dir) / "last_trigger.json"


def _read_last_fingerprint(*, fabric_dir=None) -> Optional[str]:
    return (fabric.read_json_artifact(_fingerprint_path(fabric_dir)) or {}).get(
        "trigger_fingerprint")


def _write_last_fingerprint(fingerprint: str, *, fabric_dir=None) -> None:
    fabric.save_json_artifact(
        _fingerprint_path(fabric_dir),
        {"trigger_fingerprint": fingerprint, "recorded_at": _now_iso()})


def _persist_run(payload: dict, *, fabric_dir=None) -> None:
    run_id = "evt_%s" % ek.sha256_text(
        "%s|%s" % (payload.get("generated_at"), payload.get("portfolio_state_hash")))[:16]
    payload["run_id"] = run_id
    root = fabric.runs_root(fabric_dir) / run_id
    fabric.save_json_artifact(root / "event_signal_refresh_status.json", payload)
    fabric.save_json_artifact(
        fabric.fabric_root(fabric_dir) / "latest.json",
        {"run_id": run_id, "state": payload.get("state"),
         "generated_at": payload.get("generated_at"),
         "run_dir": str(root)})


# --------------------------------------------------------------------------- #
# Read contract
# --------------------------------------------------------------------------- #
def load_event_signal_refresh_status(*, fabric_dir=None, ingestion_root=None,
                                     news_root=None, limit: int = 60,
                                     portfolio_state: Optional[dict] = None,
                                     portfolio_state_loader: Optional[Callable] = None
                                     ) -> dict:
    """READ-ONLY status: what arrived, what it affects, why anything changed.

    Performs no provider call, runs no engine and writes nothing.
    """
    ps_load = portfolio_state_loader or _default_portfolio_state_loader
    warnings: list[str] = []
    try:
        ps = portfolio_state if portfolio_state is not None else ps_load()
    except Exception as exc:  # noqa: BLE001 - a read contract must never crash
        ps = {}
        warnings.append("portfolio state unavailable: %s" % str(exc)[:160])
    held = sorted({str(p.get("ticker")).upper()
                   for p in ((ps or {}).get("positions") or []) if p.get("ticker")})
    eligible = ((ps or {}).get("dates") or {}).get("eligible_market_date")

    view = fabric.load_event_fabric(fabric_dir=fabric_dir, limit=limit, anchor=eligible,
                                    ingestion_root=ingestion_root, news_root=news_root)
    latest = fabric.read_latest_run(fabric_dir=fabric_dir)
    events = view["events"]
    # The READ surface is bound to the GATE's rule, not to a second definition of
    # "material". A bar or a delayed quote may carry new information and may bear a
    # trigger-capable authority, yet the gate does not treat its arrival as material —
    # so counting it here would report "42 material events" on a day when nothing
    # happened except that the market was open.
    material = [e for e in events
                if ek.authority_may_trigger_reassessment(e.get("decision_authority"))
                and ek.carries_new_information(e)
                and e.get("family") not in emat.MARKET_OBSERVATION_FAMILIES]
    affecting_holdings = [e for e in material
                          if set(e.get("entities") or []) & set(held)]
    return {
        "schema_version": SCHEMA_VERSION,
        "phase": PHASE,
        "composition_owner": COMPOSITION_OWNER,
        "cycle_id": CYCLE_ID,
        "generated_at": _now_iso(),
        "state": (latest or {}).get("state") or ST_NOT_RUN,
        "state_vocabulary": list(CYCLE_STATES),
        "last_run": latest,
        "eligible_market_date": eligible,
        "holdings": held,
        "event_contract": view["event_contract"],
        "dependency_graph": view["dependency_graph"],
        "materiality_policy": emat.policy_contract(),
        "capability_matrix": view["capability_matrix"],
        "terminal_audit": view["terminal_audit"],
        "source_freshness": view["source_freshness"],
        "recent_events": events,
        "recent_event_count": len(events),
        "events_by_family": view["events_by_family"],
        "events_by_authority": view["events_by_authority"],
        "material_events": material[:limit],
        "material_event_count": len(material),
        "events_affecting_holdings": affecting_holdings[:limit],
        "affected_holdings": sorted({t for e in affecting_holdings
                                     for t in (e.get("entities") or [])
                                     if t in set(held)}),
        "unclassified_signal_authority": view["unclassified_signal_authority"],
        "canonical_delegates": dict(CANONICAL_CALCULATION_DELEGATES),
        "confirm_required": EXECUTE_CONFIRM_TOKEN,
        "scheduler": {"armed": False, "interval_seconds": None,
                      "note": ("No scheduler is installed by this release. The cycle is "
                               "directly callable and idempotent so a future scheduled "
                               "caller uses this same owner without redesign.")},
        "warnings": warnings,
        "safety": _safety(False),
    }


__all__ = [
    "PHASE", "COMPOSITION_OWNER", "SCHEMA_VERSION", "CYCLE_ID",
    "EXECUTE_CONFIRM_TOKEN", "CANONICAL_CALCULATION_DELEGATES", "CYCLE_STATES",
    "ST_NO_NEW_INFORMATION", "ST_INFORMATION_NOT_MATERIAL",
    "ST_DUPLICATE_TRIGGER_SUPPRESSED", "ST_REASSESSED_NO_CHANGE",
    "ST_PROPOSAL_AVAILABLE", "ST_BLOCKED", "ST_NOT_RUN",
    "build_market_risk_state", "build_rank_deltas", "measure_latency",
    "run_event_signal_refresh", "load_event_signal_refresh_status",
]
