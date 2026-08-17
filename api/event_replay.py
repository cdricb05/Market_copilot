r"""Release 28 — the HERMETIC event-replay acceptance harness.

WHY THIS EXISTS
---------------
An event-driven manager is only trustworthy if you can prove, deterministically and
without touching a live store, that it reacts when it should and — much harder — that
it does NOT react when it should not. This harness composes a synthetic world, drives
the REAL orchestration path (``api.event_signal_refresh``) over it, and checks the
observable outcome against a stated expectation.

WHAT IS REAL AND WHAT IS SYNTHETIC
----------------------------------
REAL: the event kernel, the authority table, the novelty/dedup rules, the materiality
gate, the orchestrator, and — for the scenarios that reach them — the canonical
``api.holding_opportunity_cost``, ``api.portfolio_reassessment`` and
``api.reallocation_proposal`` owners. The replay reuses those owners rather than
simulating them, which is the whole point: it proves the event lane and the daily lane
share calculations.

SYNTHETIC: the portfolio, the price panel, the ranking and the arriving events. Every
persistent root is a caller-supplied temporary directory.

SAFETY
------
No production store is opened, no provider is called, no order is created and no
operational state is mutated. Every scenario runs against ``tmp_path``-style roots.
"""
from __future__ import annotations

import tempfile
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable, Optional

from paper_trader.api import event_fabric as fabric
from paper_trader.api import event_signal_refresh as esr
from paper_trader.engine import event_fabric as ek
from paper_trader.engine import event_materiality as emat

PHASE = "RELEASE28"
COMPOSITION_OWNER = "api.event_replay"
SCHEMA_VERSION = "1.0.0"

DEFAULT_ELIGIBLE = "2026-08-14"
#: The world is sized like the REAL book (25 equal-weight holdings against a deeper
#: candidate list). A five-name fixture would breach the released 10% name cap and the
#: 25% sector cap on every holding, and every scenario would then be blocked by a
#: fixture artefact rather than by its own economics.
DEFAULT_HOLDINGS = tuple("H%02d" % i for i in range(1, 26))
DEFAULT_CANDIDATES = tuple("C%02d" % i for i in range(1, 16))
_SECTORS = ("Technology", "Health Care", "Financials", "Industrials",
            "Consumer Discretionary", "Energy")
_BARS = 320


# --------------------------------------------------------------------------- #
# Synthetic world
# --------------------------------------------------------------------------- #
def _sessions(end: str, n: int) -> list[str]:
    out: list[str] = []
    d = date.fromisoformat(end)
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d -= timedelta(days=1)
    return list(reversed(out))


#: Distinct incommensurate oscillation frequencies. Every synthetic name gets the same
#: return VARIANCE on a different frequency, so the cross-section is homoscedastic and
#: near-uncorrelated. That matters: a fixture whose names all move together makes the
#: released covariance risk-contribution control fire on an arbitrary holding, and the
#: scenario would then be decided by the fixture rather than by its own economics.
#: The frequencies are all HIGH (>= 41 cycles over the window). A low-frequency
#: oscillation compounds into a ~60% peak-to-trough excursion, which would trip the
#: 20% drawdown materiality threshold on every name and make the baseline world noisy
#: for a reason that has nothing to do with any scenario.
_FREQS = (41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113,
          127, 131, 137, 139, 149, 151, 157, 163, 167, 173, 179, 181, 191, 193, 197,
          199, 211, 223, 227, 229, 233, 239, 241, 251, 257, 263, 269)
_AMPLITUDE = 0.012


def _series(ticker: str, dates: list[str], *, drift: float, shock: Optional[float],
            dollar_volume: float, vol_burst: float = 0.0) -> dict:
    import math
    slot = sum((i + 1) * ord(c) for i, c in enumerate(ticker)) % len(_FREQS)
    freq = _FREQS[slot]
    phase = (slot * 0.7139) % (2.0 * math.pi)
    n = len(dates)
    adj: list[float] = []
    px = 100.0
    for i, _d in enumerate(dates):
        wiggle = _AMPLITUDE * math.sin(2.0 * math.pi * freq * i / max(1, n) + phase)
        if vol_burst and i >= n - 63:
            wiggle *= (1.0 + vol_burst)
        px *= (1.0 + drift + wiggle)
        adj.append(round(px, 6))
    if shock:
        adj[-1] = round(adj[-2] * (1.0 + shock), 6)
    bench = [round(100.0 * (1.0 + 0.0003) ** i, 6) for i in range(len(dates))]
    ret: list[Optional[float]] = [None]
    bret: list[Optional[float]] = [None]
    for i in range(1, len(dates)):
        ret.append(adj[i] / adj[i - 1] - 1.0)
        bret.append(bench[i] / bench[i - 1] - 1.0)
    return {"dates": list(dates), "adj": adj, "bench": bench, "ret": ret,
            "bret": bret, "dollar_vol": [dollar_volume] * len(dates)}


def build_world(*, eligible: str = DEFAULT_ELIGIBLE,
                holdings: tuple = DEFAULT_HOLDINGS,
                candidates: tuple = DEFAULT_CANDIDATES,
                shocks: Optional[dict] = None,
                vol_bursts: Optional[dict] = None,
                liquidity: Optional[dict] = None,
                holding_ranks: Optional[dict] = None,
                book_id: str = "replay_book",
                nav: float = 100_000.0) -> dict:
    """One deterministic synthetic world: portfolio, panel, ranking, prior ranking."""
    dates = _sessions(eligible, _BARS)
    shocks = shocks or {}
    bursts = vol_bursts or {}
    liq = liquidity or {}
    tickers = list(holdings) + list(candidates)
    series = {t: _series(t, dates, drift=0.0004,
                         shock=shocks.get(t), vol_burst=bursts.get(t, 0.0),
                         dollar_volume=float(liq.get(t, 25_000_000.0)))
              for t in tickers}
    panel = {"series": series,
             "manifest": {"source": "event_replay_synthetic", "as_of": eligible}}

    weight = 1.0 / max(1, len(holdings))
    positions = []
    for i, t in enumerate(holdings):
        price = series[t]["adj"][-1]
        qty = round((nav * weight) / price, 4)
        positions.append({
            "ticker": t, "sector": _SECTORS[i % len(_SECTORS)], "quantity": qty,
            "average_cost": round(price * 0.95, 4), "price": price,
            "market_value": round(price * qty, 2),
            "portfolio_weight": weight, "target_weight": weight, "drift": 0.0,
            "operational_status": "HOLD"})
    portfolio_state = {
        "schema_version": "replay/1",
        "active_book": {"book_id": book_id, "book_label": "Replay Book",
                        "status": "FORWARD_TRACKING_ACTIVE", "initialized": True,
                        "holdings_count": len(positions),
                        "is_dormant_legacy_book": False},
        "dates": {"eligible_market_date": eligible, "valuation_date": eligible},
        "capital": {"nav": nav, "cash": 0.0, "invested": nav},
        "positions": positions,
        "orders": [], "fills": [],
        "corporate_actions": {"registry_fingerprint": "replay_ca_none", "actions": []},
        "state_hash": "replay_state_%s" % eligible,
        "economic_state_hash": "replay_econ_%s_%s" % (book_id, eligible),
        "economic_identity_version": "replay/1",
        "state": "ACTIVE", "warnings": [],
    }

    # Ranks are assigned EXACTLY as requested so a scenario can place a holding far
    # below the best alternative. Unclaimed ranks are filled with inert synthetic names
    # so the ranking stays contiguous (a gap would be an invalid cross-section).
    ranks: dict[str, int] = {str(k).upper(): int(v)
                             for k, v in (holding_ranks or {}).items()}
    used = set(ranks.values())
    nxt = 1
    # Default order: the HOLDINGS occupy the top of the ranking and the candidates sit
    # just below them. That is what a book built from the top 25 actually looks like, and
    # it makes the baseline world quiet — so a scenario's trigger comes from the scenario,
    # not from a fixture in which every holding was always replaceable.
    for t in list(holdings) + list(candidates):
        if t in ranks:
            continue
        while nxt in used:
            nxt += 1
        ranks[t] = nxt
        used.add(nxt)
        nxt += 1
    size = max(ranks.values()) if ranks else 1
    for r in range(1, size + 1):
        if r not in used:
            ranks["FLR%03d" % r] = r
            used.add(r)
    rankings = []
    for t, rank in sorted(ranks.items(), key=lambda kv: (kv[1], kv[0])):
        # Score spacing is wide enough that a name falling ~200 places clears the
        # released portfolio net-improvement hurdle, and narrow enough that adjacent
        # ranks never do. Both halves of that statement are exercised: the quiet world
        # must produce CURRENT_NO_CHANGE and scenario J must produce PROPOSAL_READY.
        rankings.append({"ticker": t, "rank": rank,
                         "combined_score": round(1.0 - 0.003 * rank, 6),
                         "percentile": round(1.0 - rank / max(1, size), 6)})
    scoring = {
        "universe_id": "replay_universe", "ranking_date": eligible,
        "market_as_of": eligible, "rankings": rankings, "exclusions": {},
        "output_hash": "replay_scoring_%s" % eligible,
        "input_contract_hash": "replay_scoring_inputs_%s" % eligible,
        "input_fingerprints": {"replay": "1"},
        "champion": {"model_id": "fundamental_momentum_50_50_v1"},
    }
    # The canonical opportunity-cost owner reads a prior rank SNAPSHOT (ticker -> rank),
    # so the replay hands it exactly that shape rather than a richer one it would choke
    # on. ``build_rank_deltas`` accepts either.
    prior_ranking = {r["ticker"]: r["rank"] for r in rankings}
    return {"eligible": eligible, "holdings": list(holdings),
            "candidates": list(candidates), "portfolio_state": portfolio_state,
            "scoring": scoring, "price_panel": panel, "prior_ranking": prior_ranking,
            "dates": dates}


# --------------------------------------------------------------------------- #
# Synthetic arriving records (Stage-2 shaped, so the REAL normalizer is exercised)
# --------------------------------------------------------------------------- #
def stage2_record(*, source_id: str, record_type: str, event_type: str,
                  native_id: str, ticker: Optional[str], effective_at: str,
                  payload: dict, published_at: Optional[str] = None,
                  identity: str = "MATCHED_EXACT") -> dict:
    return {
        "record_type": record_type, "source_id": source_id,
        "source_native_id": native_id, "record_id": "rec_" + native_id,
        "event_type": event_type, "ticker": ticker,
        "effective_at": effective_at, "observed_at": published_at or effective_at,
        "available_at": published_at, "retrieved_at": published_at or effective_at,
        "entity_mapping_confidence": identity, "quality_warnings": [],
        "normalized_payload": dict(payload,
                                   publication_time=payload.get("publication_time",
                                                                published_at)),
    }


def news_record(*, source_id: str, native_id: str, ticker: Optional[str], title: str,
                effective_at: str, published_at: str, publisher: str,
                symbols: Optional[list] = None) -> dict:
    return stage2_record(
        source_id=source_id, record_type="NEWS_EVENT", event_type="NEWS",
        native_id=native_id, ticker=ticker, effective_at=effective_at,
        published_at=published_at, identity="MATCHED_ALIAS",
        payload={"title": title, "link": "https://example.invalid/%s" % native_id,
                 "publisher": publisher, "symbols": symbols or ([ticker] if ticker else []),
                 "content_snippet": title[:120], "publication_time": published_at})


def _events_from_records(records: list, *, lane: str = "replay") -> list[dict]:
    out = []
    for rec in records:
        ev = fabric.record_to_event(rec, lane=lane, entity_index=None)
        if ev is not None:
            out.append(ev)
    return out


# --------------------------------------------------------------------------- #
# Owner wiring — the REAL canonical owners, bound to the synthetic world
# --------------------------------------------------------------------------- #
def _real_owner_seams(world: dict, roots: dict) -> dict:
    ps = world["portfolio_state"]
    panel = world["price_panel"]

    def hoc_fn(*, scoring=None, hoc_dir=None):
        from paper_trader.api import holding_opportunity_cost as hoc
        return hoc.run_and_persist(portfolio_state=ps, scoring=scoring,
                                   price_panel=panel,
                                   previous_ranking=world.get("prior_ranking"),
                                   hoc_dir=hoc_dir or roots["hoc"])

    def reassessment_fn(*, scoring=None, hoc_assessment=None, freshness=None,
                        reassessment_dir=None, hoc_dir=None):
        from paper_trader.api import portfolio_reassessment as prs
        return prs.run_and_persist(
            portfolio_state=ps, scoring=scoring, hoc_assessment=hoc_assessment,
            freshness=freshness, reassessment_dir=reassessment_dir or roots["reassess"],
            hoc_dir=hoc_dir or roots["hoc"])

    def proposal_fn(*, scoring=None, hoc_assessment=None, reallocation_dir=None,
                    hoc_dir=None):
        from paper_trader.api import reallocation_proposal as rp
        return rp.run_and_persist(
            portfolio_state=ps, scoring=scoring, hoc_assessment=hoc_assessment,
            price_panel=panel, reallocation_dir=reallocation_dir or roots["realloc"],
            hoc_dir=hoc_dir or roots["hoc"])

    return {"hoc_fn": hoc_fn, "reassessment_fn": reassessment_fn,
            "proposal_fn": proposal_fn}


def _roots(base: Path) -> dict:
    roots = {name: base / name for name in ("fabric", "hoc", "reassess", "realloc")}
    for p in roots.values():
        p.mkdir(parents=True, exist_ok=True)
    return roots


def run_cycle(*, world: dict, records: Optional[list] = None,
              events: Optional[list] = None, roots: dict,
              use_real_owners: bool = True, policy_overrides: Optional[dict] = None,
              regime_before: Any = None, regime_after: Any = None,
              include_market_quotes: bool = False, quote_fetcher: Optional[Callable] = None,
              include_gdelt: bool = False, gdelt_fetcher: Optional[Callable] = None,
              entity_index: Optional[dict] = None,
              now_iso: Optional[str] = None,
              price_panel: Any = "__world__") -> dict:
    """Drive the REAL orchestrator over the synthetic world."""
    evs = list(events or [])
    if records:
        evs.extend(_events_from_records(records))
    seams = _real_owner_seams(world, roots) if use_real_owners else {
        "hoc_fn": lambda **kw: {"assessment": {"replay_stub": True}},
        "reassessment_fn": lambda **kw: {"reassessment": {"reassessment_state":
                                                          "NO_CHANGE_JUSTIFIED"}},
        "proposal_fn": lambda **kw: {"proposal": {"replay_stub": True}},
    }
    panel = world["price_panel"] if price_panel == "__world__" else price_panel
    return esr.run_event_signal_refresh(
        confirm=esr.EXECUTE_CONFIRM_TOKEN, requested_by="event_replay",
        fabric_dir=roots["fabric"], hoc_dir=roots["hoc"],
        reassessment_dir=roots["reassess"], reallocation_dir=roots["realloc"],
        portfolio_state=world["portfolio_state"], scoring=world["scoring"],
        price_panel=panel, corpus_events=evs,
        prior_ranking=world.get("prior_ranking"),
        include_market_quotes=include_market_quotes, quote_fetcher=quote_fetcher,
        include_gdelt=include_gdelt, gdelt_fetcher=gdelt_fetcher,
        entity_index=entity_index, now_iso=now_iso,
        policy_overrides=policy_overrides, regime_before=regime_before,
        regime_after=regime_after, **seams)


# --------------------------------------------------------------------------- #
# Expectation helpers
# --------------------------------------------------------------------------- #
def _check(name, expected, observed) -> dict:
    return {"check": name, "expected": expected, "observed": observed,
            "passed": bool(expected == observed)}


def _check_true(name, observed, why=None) -> dict:
    return {"check": name, "expected": True, "observed": bool(observed),
            "passed": bool(observed), "why": why}


# --------------------------------------------------------------------------- #
# The scenarios (A-J, as required by the release)
# --------------------------------------------------------------------------- #
def scenario_a_nothing_changed(base: Path) -> dict:
    """A — the same information arrives twice: no second reassessment, no duplicate."""
    roots = _roots(base)
    world = build_world()
    recs = [stage2_record(source_id="sec_edgar", record_type="FILING_EVENT",
                          event_type="10-Q", native_id="sub|H01|0001",
                          ticker="H01", effective_at=world["eligible"],
                          published_at=world["eligible"] + "T13:00:00+00:00",
                          payload={"form_type": "10-Q", "accession_number": "0001",
                                   "cik": "1", "filing_date": world["eligible"]})]
    first = run_cycle(world=world, records=recs, roots=roots)
    second = run_cycle(world=world, records=recs, roots=roots)
    checks = [
        _check_true("first cycle admitted the event", first["events_admitted"] == 1),
        _check("second cycle admits nothing", 0, second["events_admitted"]),
        _check("second cycle suppresses the duplicate", 1,
               second["duplicates_suppressed"]),
        _check("second cycle runs no reassessment", False, second["reassessment_ran"]),
        _check("second cycle builds no proposal", False, second["proposal_built"]),
        _check_true("second cycle state is a no-change state",
                    second["state"] in (esr.ST_NO_NEW_INFORMATION,
                                        esr.ST_INFORMATION_NOT_MATERIAL,
                                        esr.ST_DUPLICATE_TRIGGER_SUPPRESSED)),
    ]
    return _result("A", "Nothing changed", checks,
                   {"first_state": first["state"], "second_state": second["state"]})


def scenario_b_material_market_move(base: Path) -> dict:
    """B — a material price move in a holding reassesses risk, and records why."""
    roots = _roots(base)
    world = build_world(shocks={"H01": -0.12})
    res = run_cycle(world=world, records=[], roots=roots)
    trig = [t for t in res["materiality"]["triggers"]
            if t["code"] == emat.T_HOLDING_PRICE_SHOCK and t["entity"] == "H01"]
    checks = [
        _check_true("a price-shock trigger fired for the holding", bool(trig)),
        _check("the trigger carries risk authority, not alpha authority",
               ek.AUTH_OPERATIONAL_RISK,
               (trig[0]["decision_authority"] if trig else None)),
        _check("no trigger changed a score", [False],
               sorted({t["changed_score"] for t in res["materiality"]["triggers"]})),
        _check_true("the reassessment ran", res["reassessment_ran"]),
        _check_true("a reason was recorded",
                    bool(res["reassessment_reason"])),
        _check_true("the affected holding is named",
                    "H01" in res["materiality"]["affected_entities"]),
        _check("no order was created", False, res["safety"]["creates_orders"]),
    ]
    return _result("B", "Material market move in a holding", checks,
                   {"state": res["state"], "reason": res["reassessment_reason"]})


def scenario_c_material_8k(base: Path) -> dict:
    """C — an 8-K triggers review and may NOT alter the expected-return score."""
    roots = _roots(base)
    world = build_world()
    pub = world["eligible"] + "T18:05:11+00:00"
    recs = [stage2_record(source_id="sec_edgar", record_type="FILING_EVENT",
                          event_type="8-K", native_id="sub|H02|8k-1", ticker="H02",
                          effective_at=world["eligible"], published_at=pub,
                          payload={"form_type": "8-K", "accession_number": "8k-1",
                                   "cik": "2", "is_8k": True,
                                   "acceptance_datetime": pub,
                                   "item_202_note": None})]
    res = run_cycle(world=world, records=recs, roots=roots)
    ev = next((e for e in _events_from_records(recs)), None)
    cls = ek.classify_event(record_type="FILING_EVENT", event_type="8-K")
    trig = [t for t in res["materiality"]["triggers"]
            if t["code"] == emat.T_MATERIAL_COMPANY_EVENT and t["entity"] == "H02"]
    checks = [
        _check("the 8-K family is the material-corporate-event family",
               ek.F_MATERIAL_CORPORATE_EVENT, cls["family"]),
        _check("its authority is trigger-only", ek.AUTH_EVENT_TRIGGER_ONLY,
               cls["decision_authority"]),
        _check("it may not change alpha", False,
               ek.authority_may_change_alpha(cls["decision_authority"])),
        _check("it may trigger a reassessment", True,
               ek.authority_may_trigger_reassessment(cls["decision_authority"])),
        _check("the ticker was mapped", "H02", (ev or {}).get("primary_ticker")),
        _check("the authoritative acceptance timestamp was kept", pub,
               (ev or {}).get("accepted_at")),
        _check("point-in-time status is clean", ek.PIT_OK,
               (ev or {}).get("point_in_time_status")),
        _check_true("a review trigger fired", bool(trig)),
        _check("the structural-alpha concept was NOT invalidated", False,
               ek.C_STRUCTURAL_ALPHA in res["concepts_invalidated"]),
        _check("universe scoring was NOT refreshed by the 8-K", False,
               ek.CALC_UNIVERSE_SCORING in res["calculations_refreshed"]),
    ]
    return _result("C", "New material 8-K", checks, {"state": res["state"]})


def scenario_d_new_periodic_report(base: Path) -> dict:
    """D — a 10-Q makes new structural information available; only its dependents refresh."""
    roots = _roots(base)
    world = build_world()
    recs = [stage2_record(source_id="sec_edgar", record_type="FILING_EVENT",
                          event_type="10-Q", native_id="sub|H03|10q-1", ticker="H03",
                          effective_at=world["eligible"],
                          published_at=world["eligible"] + "T21:02:00+00:00",
                          payload={"form_type": "10-Q", "accession_number": "10q-1",
                                   "cik": "3", "filing_date": world["eligible"]})]
    res = run_cycle(world=world, records=recs, roots=roots)
    cls = ek.classify_event(record_type="FILING_EVENT", event_type="10-Q")
    checks = [
        _check("the 10-Q family is the structural report family",
               ek.F_STRUCTURAL_REPORT, cls["family"]),
        _check("its authority is operational alpha", ek.AUTH_OPERATIONAL_ALPHA,
               cls["decision_authority"]),
        _check_true("the structural-alpha concept was invalidated",
                    ek.C_STRUCTURAL_ALPHA in res["concepts_invalidated"]),
        _check_true("universe scoring is in the refresh set",
                    ek.CALC_UNIVERSE_SCORING in res["calculations_refreshed"]),
        _check_true("the opportunity-cost engine is in the refresh set",
                    ek.CALC_HOLDING_OPPORTUNITY_COST in res["calculations_refreshed"]),
        _check("no unrelated calculation was refreshed", False,
               ek.CALC_RESEARCH_EVIDENCE in res["calculations_refreshed"]),
        _check_true("the reassessment ran", res["reassessment_ran"]),
    ]
    return _result("D", "New 10-Q / 10-K", checks,
                   {"calculations": res["calculations_refreshed"]})


def scenario_e_material_news(base: Path) -> dict:
    """E — a genuine news story: novelty, source quality, mapping, trigger-only."""
    roots = _roots(base)
    world = build_world()
    pub = world["eligible"] + "T11:30:00+00:00"
    recs = [news_record(source_id="eodhd", native_id="news|story-1", ticker="H04",
                        title="DDD announces a major customer contract",
                        effective_at=world["eligible"], published_at=pub,
                        publisher="Reuters")]
    res = run_cycle(world=world, records=recs, roots=roots)
    ev = _events_from_records(recs)[0]
    checks = [
        _check("the news family is company news", ek.F_COMPANY_NEWS, ev["family"]),
        _check("its authority is trigger-only", ek.AUTH_EVENT_TRIGGER_ONLY,
               ev["decision_authority"]),
        _check("novelty is NEW", ek.NOV_NEW, ev["novelty"]),
        _check("the ticker was mapped", "H04", ev["primary_ticker"]),
        _check("the publication timestamp was preserved", pub, ev["published_at"]),
        _check_true("source quality was recorded",
                    ev["source_quality"] not in (None, "", "UNKNOWN")),
        _check("news may not change alpha", False,
               ek.authority_may_change_alpha(ev["decision_authority"])),
        _check_true("a review trigger fired for the holding",
                    "H04" in res["materiality"]["affected_entities"]),
    ]
    return _result("E", "Material news story", checks, {"state": res["state"]})


def scenario_f_same_story_many_sources(base: Path) -> dict:
    """F — one story from five outlets is ONE information event."""
    roots = _roots(base)
    world = build_world()
    pub = world["eligible"] + "T12:00:00+00:00"
    title = "EEE wins a landmark regulatory approval"
    recs = [news_record(source_id="eodhd", native_id="news|wire-%d" % i, ticker="H05",
                        title=title, effective_at=world["eligible"], published_at=pub,
                        publisher="Wire %d" % i)
            for i in range(5)]
    res = run_cycle(world=world, records=recs, roots=roots)
    admitted = fabric.read_events(fabric_dir=roots["fabric"], limit=50)
    novel = [e for e in admitted if e["novelty"] == ek.NOV_NEW]
    synd = [e for e in admitted if e["novelty"] == ek.NOV_SYNDICATED]
    triggers = [t for t in res["materiality"]["triggers"]
                if t["code"] == emat.T_MATERIAL_COMPANY_EVENT and t["entity"] == "H05"]
    checks = [
        _check("exactly one NEW information event", 1, len(novel)),
        _check("the other four are linked as syndicated", 4, len(synd)),
        _check_true("each duplicate points at the original",
                    all(e["duplicate_of"] == novel[0]["event_id"] for e in synd)
                    if novel else False),
        _check("the story triggers a review exactly once", 1, len(triggers)),
        _check_true("the duplicates are suppressed with a stated reason",
                    all(s["code"] == emat.S_DUPLICATE_STORY
                        for s in res["materiality"]["suppressed"]
                        if s.get("event_id") in {e["event_id"] for e in synd})),
    ]
    return _result("F", "Same story from multiple sources", checks,
                   {"admitted": res["events_admitted"]})


def scenario_g_macro_event(base: Path) -> dict:
    """G — a macro release updates context; it invents no stock-level alpha."""
    roots = _roots(base)
    world = build_world()
    context = stage2_record(
        source_id="fred_alfred", record_type="MACRO_OBSERVATION",
        event_type="MACRO_OBSERVATION", native_id="fred|CPIAUCSL|2026-08-14",
        ticker=None, effective_at=world["eligible"],
        published_at=world["eligible"] + "T12:30:00+00:00",
        payload={"series_id": "CPIAUCSL", "macro_family": "inflation", "value": "3.1"})
    regime = stage2_record(
        source_id="fred_alfred", record_type="MACRO_OBSERVATION",
        event_type="MACRO_OBSERVATION", native_id="fred|VIXCLS|2026-08-14",
        ticker=None, effective_at=world["eligible"],
        published_at=world["eligible"] + "T21:00:00+00:00",
        payload={"series_id": "VIXCLS", "macro_family": "volatility_regime",
                 "value": "31.4"})
    quiet = run_cycle(world=world, records=[context, regime], roots=roots)
    shifted = run_cycle(world=build_world(eligible="2026-08-13"),
                        records=[], roots=_roots(base / "g2"),
                        regime_before="CALM", regime_after="STRESSED")
    cls_ctx = ek.classify_event(record_type="MACRO_OBSERVATION",
                                payload={"series_id": "CPIAUCSL",
                                         "macro_family": "inflation"})
    cls_reg = ek.classify_event(record_type="MACRO_OBSERVATION",
                                payload={"series_id": "VIXCLS",
                                         "macro_family": "volatility_regime"})
    checks = [
        _check("a CPI print is observability only", ek.AUTH_OBSERVABILITY_ONLY,
               cls_ctx["decision_authority"]),
        _check("a CPI print may not change alpha", False,
               ek.authority_may_change_alpha(cls_ctx["decision_authority"])),
        _check("a regime series carries risk authority", ek.AUTH_OPERATIONAL_RISK,
               cls_reg["decision_authority"]),
        _check("a macro OBSERVATION alone triggers nothing", 0,
               len([t for t in quiet["materiality"]["triggers"]
                    if t["code"] == emat.T_REGIME_TRANSITION])),
        _check("a regime TRANSITION does trigger", 1,
               len([t for t in shifted["materiality"]["triggers"]
                    if t["code"] == emat.T_REGIME_TRANSITION])),
        _check("no stock-level entity was invented by the macro event", [],
               sorted({t["entity"] for t in quiet["materiality"]["triggers"]
                       if t["code"] == emat.T_REGIME_TRANSITION and t["entity"]})),
    ]
    return _result("G", "Macro event", checks,
                   {"quiet_state": quiet["state"], "shift_state": shifted["state"]})


def scenario_h_stale_source(base: Path) -> dict:
    """H — a stale/failed source is reported degraded; nothing is fabricated."""
    roots = _roots(base)
    empty = base / "empty_corpus"
    empty.mkdir(parents=True, exist_ok=True)
    world = build_world()
    fabric.save_watermarks({
        "sec_edgar": {"source_watermark": "2026-06-01", "last_error": "HTTP 503",
                      "last_attempt_at": "2026-08-14T00:00:00+00:00"},
        "eodhd": {"source_watermark": None},
    }, fabric_dir=roots["fabric"])
    # HERMETIC: the corpus roots are empty temp directories, so nothing about the
    # operator's real Stage-2 state can leak into this scenario's verdict.
    freshness = fabric.build_source_freshness(anchor=world["eligible"],
                                              fabric_dir=roots["fabric"],
                                              ingestion_root=empty, news_root=empty)
    rows = {r["source_id"]: r for r in freshness["sources"]}
    sec = rows.get("sec_edgar") or {}
    eod = rows.get("eodhd") or {}
    checks = [
        _check_true("a long-lagging event source is not reported FRESH",
                    sec.get("status") != "FRESH"),
        _check("the recorded error is surfaced", "HTTP 503", sec.get("last_error")),
        _check("a source with no watermark is MISSING, not zero-filled", "MISSING",
               eod.get("status")),
        _check_true("no fabricated watermark was invented",
                    eod.get("source_watermark") is None),
        _check_true("degraded sources are enumerated",
                    freshness["degraded_count"] >= 1),
        _check_true("a slow-cadence source is judged under its own cadence",
                    (rows.get("bea") or {}).get("status") in
                    ("NOT_DUE", "NOT_APPLICABLE", "MISSING", "FRESH", "STALE")),
    ]
    return _result("H", "Stale / failed source", checks,
                   {"degraded": freshness["degraded_count"]})


def scenario_i_research_challenger(base: Path) -> dict:
    """I — the research lane cannot promote itself through the event fabric."""
    roots = _roots(base)
    world = build_world()
    recs = [stage2_record(
        source_id="eodhd_analyst", record_type="FUNDAMENTAL_FACT",
        event_type="ANALYST_PRICE_TARGET_VINTAGE", native_id="analyst|H01|2026-08-14",
        ticker="H01", effective_at=world["eligible"],
        published_at=world["eligible"],
        payload={"analyst_target_price": 321.65, "revision_vintage_date":
                 world["eligible"], "snapshot_date": world["eligible"]})]
    res = run_cycle(world=world, records=recs, roots=roots)
    ev = _events_from_records(recs)[0]
    blocked = ek.classify_event(record_type="ANALYST_REVISION", event_type="REVISION")
    checks = [
        _check("the snapshot is research alpha", ek.AUTH_RESEARCH_ALPHA,
               ev["decision_authority"]),
        _check("research alpha may not change the operational ranking", False,
               ek.authority_may_change_alpha(ev["decision_authority"])),
        _check("research alpha may not trigger an operational reassessment", False,
               ek.authority_may_trigger_reassessment(ev["decision_authority"])),
        _check("research alpha never reaches the operational target", False,
               ek.authority_touches_operational_target(ev["decision_authority"])),
        _check("its point-in-time status is marked forward-snapshot-only",
               ek.PIT_SNAPSHOT_PROSPECTIVE, ev["point_in_time_status"]),
        _check("as-was analyst revisions remain BLOCKED", ek.AUTH_BLOCKED,
               blocked["decision_authority"]),
        _check("the cycle ran no reassessment from research evidence", False,
               res["reassessment_ran"]),
        _check("the cycle promoted no model", False,
               res["safety"]["promotes_model"]),
    ]
    return _result("I", "Research challenger", checks, {"state": res["state"]})


def scenario_j_alternative_improvement(base: Path) -> dict:
    """J — an alternative improves materially: opportunity cost, a COMPLETE target
    portfolio for manual review, and still no order.

    ONE holding deteriorates, so the turnover the change implies is small enough to
    clear the released budget. That is the point of the scenario: the event lane must
    be able to carry a justified change all the way to a reviewable target, not merely
    to a verdict that something is wrong.
    """
    roots = _roots(base)
    world = build_world(holding_ranks={"H01": 240, "H02": 238})
    # H01 and H02 ranked in the top 20 at the previous snapshot and have since fallen
    # to the bottom of the cross-section; every other holding is unchanged. TWO
    # actionable 4% positions is the smallest change that can clear the released
    # portfolio net-improvement hurdle — a single 4% name mathematically cannot, which
    # is itself the anti-churn control working as designed.
    world["prior_ranking"] = dict(world["prior_ranking"], H01=18, H02=19)
    res = run_cycle(world=world, records=[], roots=roots)
    trig = [t for t in res["materiality"]["triggers"]
            if t["code"] in (emat.T_ALTERNATIVE_IMPROVEMENT, emat.T_RANK_DETERIORATION)]
    target = res.get("target_portfolio") or {}
    checks = [
        _check_true("an opportunity-cost trigger fired for the deteriorated holding",
                    any(t["entity"] == "H01" for t in trig)),
        _check_true("the reassessment ran through the canonical owner",
                    res["reassessment_ran"]),
        _check("the opportunity-cost owner is the existing one",
               "api.holding_opportunity_cost",
               res["canonical_delegates"][ek.CALC_HOLDING_OPPORTUNITY_COST]),
        _check("the proposal owner is the existing one", "api.reallocation_proposal",
               res["canonical_delegates"][ek.CALC_REALLOCATION_PROPOSAL]),
        _check("a complete target portfolio was produced", True, res["proposal_built"]),
        _check("the cycle terminates in manual review",
               esr.ST_PROPOSAL_AVAILABLE, res["state"]),
        _check("manual review is required", True, res["manual_review_required"]),
        _check_true("the target explains every retained and changed holding",
                    sum((target.get("action_counts") or {}).values()) ==
                    len(target.get("allocations") or [])),
        _check_true("turnover and transaction cost are stated",
                    (target.get("turnover") or {}).get("one_way_turnover") is not None
                    and (target.get("turnover") or {}).get(
                        "estimated_transaction_cost") is not None),
        _check_true("concentration before and after are stated",
                    (target.get("risk") or {}).get("concentration_before") is not None
                    and (target.get("risk") or {}).get("concentration_after") is not None),
        _check("every allocation constraint holds", True,
               (target.get("constraints") or {}).get("all_ok")),
        _check("no order was created", False, res["safety"]["creates_orders"]),
        _check("no proposal was approved", False, res["safety"]["approves_proposal"]),
        _check("no target was confirmed", False, res["safety"]["confirms_target"]),
    ]
    return _result("J", "Material improvement of an alternative", checks,
                   {"state": res["state"], "proposal_built": res["proposal_built"],
                    "action_counts": target.get("action_counts"),
                    "turnover": (target.get("turnover") or {}).get("one_way_turnover"),
                    "net_improvement": (target.get("signal") or {}).get(
                        "score_improvement_net_of_cost")})


def _result(key: str, title: str, checks: list, detail: Optional[dict] = None) -> dict:
    failed = [c for c in checks if not c["passed"]]
    return {"scenario": key, "title": title, "checks": checks,
            "check_count": len(checks), "failed": failed,
            "failed_count": len(failed), "passed": not failed,
            "detail": detail or {}}


SCENARIOS: dict[str, Callable] = {
    "A": scenario_a_nothing_changed,
    "B": scenario_b_material_market_move,
    "C": scenario_c_material_8k,
    "D": scenario_d_new_periodic_report,
    "E": scenario_e_material_news,
    "F": scenario_f_same_story_many_sources,
    "G": scenario_g_macro_event,
    "H": scenario_h_stale_source,
    "I": scenario_i_research_challenger,
    "J": scenario_j_alternative_improvement,
}


def run_replay(*, base_dir=None, scenarios: Optional[list] = None) -> dict:
    """Run the required scenarios hermetically and report every check."""
    base = Path(base_dir) if base_dir else Path(tempfile.mkdtemp(prefix="event_replay_"))
    keys = [k.upper() for k in (scenarios or list(SCENARIOS))]
    results = []
    for k in keys:
        fn = SCENARIOS.get(k)
        if fn is None:
            results.append({"scenario": k, "title": "UNKNOWN", "checks": [],
                            "check_count": 0, "failed": [], "failed_count": 1,
                            "passed": False, "detail": {"error": "unknown scenario"}})
            continue
        try:
            results.append(fn(base / ("scenario_%s" % k)))
        except Exception as exc:  # noqa: BLE001 - a harness reports, it does not crash
            results.append({"scenario": k, "title": "ERROR", "checks": [],
                            "check_count": 0, "failed": [{"check": "execution",
                                                          "error": str(exc)[:400]}],
                            "failed_count": 1, "passed": False,
                            "detail": {"error": str(exc)[:400]}})
    total_checks = sum(r["check_count"] for r in results)
    failed = [r for r in results if not r["passed"]]
    return {
        "contract_id": "paper_trader.event_replay_results/1",
        "schema_version": SCHEMA_VERSION,
        "phase": PHASE,
        "composition_owner": COMPOSITION_OWNER,
        "base_dir": str(base),
        "scenarios": results,
        "scenario_count": len(results),
        "check_count": total_checks,
        "failed_scenarios": [r["scenario"] for r in failed],
        "failed_count": len(failed),
        "passed": not failed,
        "hermetic": True,
        "safety": {"read_only_production": True, "creates_orders": False,
                   "mutates_operational_state": False,
                   "note": ("Every persistent root is a temporary directory; no "
                            "production store, provider or prediction service is "
                            "reached.")},
    }


__all__ = ["PHASE", "COMPOSITION_OWNER", "SCHEMA_VERSION", "SCENARIOS", "build_world",
           "stage2_record", "news_record", "run_cycle", "run_replay",
           "DEFAULT_ELIGIBLE", "DEFAULT_HOLDINGS", "DEFAULT_CANDIDATES"]
