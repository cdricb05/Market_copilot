"""Release 28 — the event-driven active portfolio manager.

WHAT THESE TESTS PROTECT
------------------------
The value of an event-driven manager is not that it reacts; it is that it reacts to
the RIGHT things, with the right authority, exactly once. Every test below is a
statement about one of those three properties, and each one corresponds to a way the
system could quietly become wrong:

* an unvalidated headline acquiring the power to change a score;
* a research challenger reaching the operational target through a side door;
* one story from five wires becoming five reasons to trade;
* a re-collected filing manufacturing a "new" trigger every cycle;
* a slow quarterly source being reported broken for behaving normally;
* a fabricated timestamp turning an unknown into a fact;
* the event lane growing a second copy of a calculation the daily lane already owns.

HERMETIC
--------
Every test runs against ``tmp_path`` roots. Nothing here opens a production store,
calls a provider, reaches the prediction service, creates an order or mutates any
operational state.
"""
from __future__ import annotations

import json

import pytest

from paper_trader.api import event_fabric as fabric
from paper_trader.api import event_replay as replay
from paper_trader.api import event_signal_refresh as esr
from paper_trader.api import source_capability as scap
from paper_trader.engine import event_fabric as ek
from paper_trader.engine import event_materiality as emat

ELIGIBLE = replay.DEFAULT_ELIGIBLE


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
@pytest.fixture()
def roots(tmp_path):
    out = {name: tmp_path / name for name in ("fabric", "hoc", "reassess", "realloc")}
    for p in out.values():
        p.mkdir(parents=True, exist_ok=True)
    return out


@pytest.fixture()
def world():
    return replay.build_world()


def _filing(form, ticker, *, native, eligible=ELIGIBLE, published=None):
    return replay.stage2_record(
        source_id="sec_edgar", record_type="FILING_EVENT", event_type=form,
        native_id=native, ticker=ticker, effective_at=eligible,
        published_at=published or (eligible + "T13:00:00+00:00"),
        payload={"form_type": form, "accession_number": native, "cik": "1",
                 "filing_date": eligible})


def _events(records):
    return [fabric.record_to_event(r, lane="test") for r in records]


# --------------------------------------------------------------------------- #
# 1. Signal authority — the safety boundary
# --------------------------------------------------------------------------- #
class TestSignalAuthority:
    def test_every_declared_family_carries_an_explicit_authority(self):
        for fam in ek.EVENT_FAMILY_TABLE:
            assert fam["decision_authority"] in ek.SIGNAL_AUTHORITIES, fam["family"]
            assert fam["signal_speed"] in ek.SIGNAL_SPEEDS, fam["family"]
            assert fam["why_authority"], fam["family"]

    def test_only_operational_alpha_may_change_a_score(self):
        allowed = {a for a in ek.SIGNAL_AUTHORITIES if ek.authority_may_change_alpha(a)}
        assert allowed == {ek.AUTH_OPERATIONAL_ALPHA}

    @pytest.mark.parametrize("record_type,event_type", [
        ("NEWS_EVENT", "NEWS"),
        ("FILING_EVENT", "8-K"),
        ("EARNINGS_EVENT", "EARNINGS_REPORT"),
        ("EARNINGS_EVENT", "8-K_ITEM_2.02"),
        ("INSIDER_FILING", "4"),
        ("REGULATORY_EVENT", "REGULATORY_EVENT:HEALTH_SAFETY"),
        ("PRESS_RELEASE", "PRESS_RELEASE"),
    ])
    def test_unvalidated_events_are_trigger_only(self, record_type, event_type):
        cls = ek.classify_event(record_type=record_type, event_type=event_type)
        assert cls["decision_authority"] == ek.AUTH_EVENT_TRIGGER_ONLY
        assert ek.authority_may_trigger_reassessment(cls["decision_authority"]) is True
        assert ek.authority_may_change_alpha(cls["decision_authority"]) is False

    def test_research_alpha_never_reaches_the_operational_target(self):
        cls = ek.classify_event(record_type="FUNDAMENTAL_FACT",
                                event_type="ANALYST_PRICE_TARGET_VINTAGE")
        assert cls["decision_authority"] == ek.AUTH_RESEARCH_ALPHA
        assert ek.authority_touches_operational_target(cls["decision_authority"]) is False
        assert ek.authority_may_trigger_reassessment(cls["decision_authority"]) is False

    def test_as_was_analyst_revisions_are_blocked(self):
        cls = ek.classify_event(record_type="ANALYST_REVISION", event_type="REVISION")
        assert cls["decision_authority"] == ek.AUTH_BLOCKED
        assert ek.authority_touches_operational_target(cls["decision_authority"]) is False

    def test_periodic_reports_are_the_only_alpha_bearing_filings(self):
        for form in ek.STRUCTURAL_FORMS:
            cls = ek.classify_event(record_type="FILING_EVENT", event_type=form)
            assert cls["decision_authority"] == ek.AUTH_OPERATIONAL_ALPHA, form
        for form in ek.MATERIAL_EVENT_FORMS + ek.INSIDER_FORMS:
            cls = ek.classify_event(record_type="FILING_EVENT", event_type=form)
            assert cls["decision_authority"] != ek.AUTH_OPERATIONAL_ALPHA, form

    def test_intraday_quotes_carry_risk_authority_only(self):
        cls = ek.classify_event(record_type="MARKET_QUOTE", event_type="DELAYED_QUOTE")
        assert cls["decision_authority"] == ek.AUTH_OPERATIONAL_RISK
        assert ek.authority_may_change_alpha(cls["decision_authority"]) is False
        assert ek.authority_may_change_risk(cls["decision_authority"]) is True

    def test_macro_context_may_not_rank_a_stock_but_regime_carries_risk(self):
        ctx = ek.classify_event(record_type="MACRO_OBSERVATION",
                                payload={"series_id": "CPIAUCSL",
                                         "macro_family": "inflation"})
        reg = ek.classify_event(record_type="MACRO_OBSERVATION",
                                payload={"series_id": "VIXCLS",
                                         "macro_family": "volatility_regime"})
        assert ctx["decision_authority"] == ek.AUTH_OBSERVABILITY_ONLY
        assert reg["decision_authority"] == ek.AUTH_OPERATIONAL_RISK

    def test_an_unknown_record_type_fails_closed(self):
        cls = ek.classify_event(record_type="SOME_FUTURE_FEED", event_type="X")
        assert cls["classified"] is False
        assert cls["decision_authority"] == ek.AUTH_OBSERVABILITY_ONLY
        assert ek.unclassified_authority_count([dict(cls, classified=False)]) == 1

    def test_every_record_type_the_corpus_produces_is_classified(self):
        """The 12 Stage-2 record types plus the two Stage-3.5 ones must all map."""
        produced = ("MARKET_BAR", "CORPORATE_ACTION", "UNIVERSE_MEMBERSHIP",
                    "SECURITY_IDENTITY", "FILING_EVENT", "FUNDAMENTAL_FACT",
                    "INSIDER_FILING", "EARNINGS_EVENT", "NEWS_EVENT",
                    "MACRO_OBSERVATION", "SHORT_VOLUME", "TRADING_HALT",
                    "REGULATORY_EVENT", "PRESS_RELEASE")
        for rt in produced:
            cls = ek.classify_event(record_type=rt, event_type=None)
            assert cls["classified"] is True, rt
            assert cls["family"] != "unmapped_record_type", rt


# --------------------------------------------------------------------------- #
# 2. Point-in-time discipline
# --------------------------------------------------------------------------- #
class TestPointInTime:
    def test_a_missing_publication_time_stays_unknown(self):
        ev = ek.build_event(source_id="s", record_type="NEWS_EVENT",
                            source_event_id="n1", payload={"title": "x"},
                            event_type="NEWS")
        assert ev["published_at"] is None
        assert ev["point_in_time_status"] == ek.PIT_UNKNOWN_AVAILABILITY
        assert any("PUBLICATION_TIME_UNKNOWN" in w for w in ev["quality_warnings"])

    def test_an_authoritative_acceptance_time_is_preserved_exactly(self):
        stamp = "2026-08-14T18:05:11.000Z"
        rec = _filing("8-K", "H01", native="a1", published=stamp)
        rec["normalized_payload"]["acceptance_datetime"] = stamp
        ev = fabric.record_to_event(rec, lane="test")
        assert ev["accepted_at"] == stamp
        assert ev["point_in_time_status"] == ek.PIT_OK

    def test_a_prospective_snapshot_is_marked_forward_only(self):
        ev = ek.build_event(source_id="eodhd_analyst", record_type="FUNDAMENTAL_FACT",
                            source_event_id="a1", payload={"analyst_target_price": 1.0},
                            event_type="ANALYST_PRICE_TARGET_VINTAGE",
                            published_at="2026-08-14")
        assert ev["point_in_time_status"] == ek.PIT_SNAPSHOT_PROSPECTIVE

    def test_a_period_end_is_never_promoted_to_a_publication_time(self):
        rec = replay.stage2_record(
            source_id="eodhd", record_type="EARNINGS_EVENT",
            event_type="EARNINGS_REPORT", native_id="e1", ticker="H01",
            effective_at="2026-07-31", published_at=None,
            payload={"period_end": "2026-06-30", "actual": 1.0})
        ev = fabric.record_to_event(rec, lane="test")
        assert ev["published_at"] is None
        assert ev["point_in_time_status"] == ek.PIT_UNKNOWN_AVAILABILITY


# --------------------------------------------------------------------------- #
# 3. Idempotency, novelty and deduplication
# --------------------------------------------------------------------------- #
class TestIdempotencyAndNovelty:
    def test_the_idempotency_key_ignores_ingestion_time(self):
        a = ek.build_event(source_id="s", record_type="NEWS_EVENT",
                           source_event_id="n", payload={"t": 1},
                           ingested_at="2026-08-14T00:00:00+00:00")
        b = ek.build_event(source_id="s", record_type="NEWS_EVENT",
                           source_event_id="n", payload={"t": 1},
                           ingested_at="2026-08-15T09:30:00+00:00")
        assert a["idempotency_key"] == b["idempotency_key"]
        assert a["event_id"] == b["event_id"]

    def test_reingesting_the_same_event_writes_nothing_new(self, roots):
        evs = _events([_filing("10-Q", "H01", native="q1")])
        first = fabric.append_events(evs, fabric_dir=roots["fabric"])
        second = fabric.append_events(evs, fabric_dir=roots["fabric"])
        assert first["admitted_count"] == 1
        assert second["admitted_count"] == 0
        assert second["duplicates_suppressed"] == 1
        assert second["written"] == 0

    def test_one_story_from_five_wires_is_one_information_event(self, roots):
        title = "H05 wins a landmark approval"
        evs = _events([
            replay.news_record(source_id="eodhd", native_id="w%d" % i, ticker="H05",
                               title=title, effective_at=ELIGIBLE,
                               published_at=ELIGIBLE + "T12:00:00+00:00",
                               publisher="Wire %d" % i)
            for i in range(5)])
        res = fabric.append_events(evs, fabric_dir=roots["fabric"])
        informative = [e for e in res["admitted"] if ek.carries_new_information(e)]
        assert len(informative) == 1
        syndicated = [e for e in res["admitted"] if e["novelty"] == ek.NOV_SYNDICATED]
        assert len(syndicated) == 4
        assert all(e["duplicate_of"] == informative[0]["event_id"] for e in syndicated)

    def test_the_same_document_under_two_collection_scopes_is_one_event(self, roots):
        """A wire article fetched once per symbol must not look like a correction."""
        url = "https://example.invalid/story-1"
        evs = []
        for sym in ("H01", "H02", "H03"):
            rec = replay.news_record(source_id="eodhd", native_id="scope|%s" % sym,
                                     ticker=sym, title="Sector-wide story",
                                     effective_at=ELIGIBLE,
                                     published_at=ELIGIBLE + "T10:00:00+00:00",
                                     publisher="Wire")
            rec["normalized_payload"]["link"] = url
            evs.append(fabric.record_to_event(rec, lane="test"))
        res = fabric.append_events(evs, fabric_dir=roots["fabric"])
        informative = [e for e in res["admitted"] if ek.carries_new_information(e)]
        assert len(informative) == 1
        assert all(e["novelty"] == ek.NOV_SYNDICATED
                   for e in res["admitted"] if e is not informative[0])

    def test_a_native_id_reused_on_a_LATER_DATE_is_a_new_observation(self):
        """The symbol-directory row ``nasdaqlisted|ABNB`` repeats every session."""
        day1 = ek.build_event(source_id="nasdaq_trader", record_type="SECURITY_IDENTITY",
                              source_event_id="nasdaqlisted|ABNB", payload={"v": 1},
                              effective_at="2026-08-13")
        day2 = ek.build_event(source_id="nasdaq_trader", record_type="SECURITY_IDENTITY",
                              source_event_id="nasdaqlisted|ABNB", payload={"v": 2},
                              effective_at="2026-08-14")
        seen = {ek.supersession_key(day1): day1["event_id"]}
        verdict = ek.classify_novelty(event=day2, seen_source_event_ids=seen)
        assert verdict["novelty"] == ek.NOV_NEW

    def test_a_same_day_reissue_with_changed_content_supersedes(self):
        first = ek.build_event(source_id="eodhd", record_type="EARNINGS_EVENT",
                               source_event_id="earnings|MSFT|2026-07-29",
                               payload={"estimate": 4.21}, effective_at="2026-07-29")
        revised = ek.build_event(source_id="eodhd", record_type="EARNINGS_EVENT",
                                 source_event_id="earnings|MSFT|2026-07-29",
                                 payload={"estimate": 4.21, "actual": 4.74},
                                 effective_at="2026-07-29")
        seen = {ek.supersession_key(first): first["event_id"]}
        verdict = ek.classify_novelty(event=revised, seen_source_event_ids=seen)
        applied = ek.apply_novelty(revised, verdict)
        assert applied["novelty"] == ek.NOV_MATERIAL_UPDATE
        assert applied["supersedes"] == first["event_id"]
        # History is never rewritten: the earlier event object is untouched.
        assert first["novelty"] == ek.NOV_NEW
        assert first["superseded_by"] is None

    def test_persisted_events_are_append_only(self, roots):
        fabric.append_events(_events([_filing("10-Q", "H01", native="q1")]),
                             fabric_dir=roots["fabric"])
        fabric.append_events(_events([_filing("8-K", "H02", native="k1")]),
                             fabric_dir=roots["fabric"])
        stored = fabric.read_events(fabric_dir=roots["fabric"], limit=50)
        assert len(stored) == 2
        assert {e["event_type"] for e in stored} == {"10-Q", "8-K"}


# --------------------------------------------------------------------------- #
# 4. Materiality / anti-churn
# --------------------------------------------------------------------------- #
class TestMateriality:
    def test_no_change_produces_no_reassessment(self):
        res = emat.assess_materiality(events=[], risk_state={}, rank_deltas={},
                                      holdings=["H01"], candidates=["C01"])
        assert res["change_level"] == emat.LVL_NO_CHANGE
        assert res["reassessment_required"] is False
        assert res["trigger_count"] == 0

    def test_a_duplicate_story_does_not_trigger(self):
        ev = ek.build_event(source_id="eodhd", record_type="NEWS_EVENT",
                            source_event_id="n1", payload={"title": "t"},
                            event_type="NEWS", entities=["H01"],
                            primary_ticker="H01")
        ev = ek.apply_novelty(ev, {"novelty": ek.NOV_SYNDICATED,
                                   "duplicate_of": "evt_other",
                                   "reason": "already seen"})
        res = emat.assess_materiality(events=[ev], holdings=["H01"])
        assert res["reassessment_required"] is False
        assert any(s["code"] == emat.S_DUPLICATE_STORY for s in res["suppressed"])

    def test_a_research_event_may_not_trigger(self):
        ev = ek.build_event(source_id="eodhd_analyst", record_type="FUNDAMENTAL_FACT",
                            source_event_id="a1", payload={"x": 1},
                            event_type="ANALYST_PRICE_TARGET_VINTAGE",
                            entities=["H01"], primary_ticker="H01")
        res = emat.assess_materiality(events=[ev], holdings=["H01"])
        assert res["reassessment_required"] is False
        assert any(s["code"] == emat.S_NON_TRIGGER_AUTHORITY for s in res["suppressed"])

    def test_an_event_about_an_unrelated_company_does_not_trigger(self):
        ev = ek.build_event(source_id="sec_edgar", record_type="FILING_EVENT",
                            source_event_id="k1", payload={"form_type": "8-K"},
                            event_type="8-K", entities=["ZZZZ"],
                            primary_ticker="ZZZZ")
        res = emat.assess_materiality(events=[ev], holdings=["H01"], candidates=["C01"])
        assert res["reassessment_required"] is False
        assert any(s["code"] == emat.S_UNRELATED_ENTITY for s in res["suppressed"])

    def test_a_material_move_in_a_holding_triggers_with_a_stated_threshold(self):
        res = emat.assess_materiality(
            events=[], risk_state={"H01": {"ret_1": -0.12}}, holdings=["H01"])
        assert res["reassessment_required"] is True
        trig = [t for t in res["triggers"] if t["code"] == emat.T_HOLDING_PRICE_SHOCK]
        assert trig and trig[0]["entity"] == "H01"
        assert trig[0]["threshold"] == emat.DEFAULT_POLICY["abs_return_1d"]
        assert trig[0]["changed_score"] is False

    def test_an_ordinary_move_does_not_trigger(self):
        res = emat.assess_materiality(
            events=[], risk_state={"H01": {"ret_1": -0.01, "maxdd_252": -0.05}},
            holdings=["H01"])
        assert res["reassessment_required"] is False
        assert any(s["code"] == emat.S_BELOW_THRESHOLD for s in res["suppressed"])

    def test_an_identical_trigger_set_is_suppressed_by_fingerprint(self):
        kw = dict(events=[], risk_state={"H01": {"ret_1": -0.12}}, holdings=["H01"],
                  portfolio_state_hash="ph1")
        first = emat.assess_materiality(**kw)
        second = emat.assess_materiality(
            prior_trigger_fingerprint=first["trigger_fingerprint"], **kw)
        assert second["duplicate_of_prior_trigger"] is True
        assert second["reassessment_required"] is False
        assert any(s["code"] == emat.S_DUPLICATE_TRIGGER for s in second["suppressed"])

    def test_the_fingerprint_ignores_recollection_of_the_same_information(self):
        """Re-collecting one filing twenty times must not look like new facts."""
        def one(n):
            evs = []
            for i in range(n):
                e = ek.build_event(source_id="sec_edgar", record_type="FILING_EVENT",
                                   source_event_id="acc|%d" % i,
                                   payload={"form_type": "8-K", "copy": i},
                                   event_type="8-K", entities=["H01"],
                                   primary_ticker="H01", effective_at=ELIGIBLE)
                evs.append(e)
            return emat.assess_materiality(events=evs, holdings=["H01"],
                                           portfolio_state_hash="ph1")
        a, b = one(1), one(20)
        assert a["trigger_fingerprint"] == b["trigger_fingerprint"]
        assert a["trigger_count"] == b["trigger_count"] == 1
        assert b["triggers"][0]["occurrences"] == 20

    def test_a_new_day_of_information_is_not_suppressed(self):
        def one(day):
            e = ek.build_event(source_id="sec_edgar", record_type="FILING_EVENT",
                               source_event_id="acc|%s" % day,
                               payload={"form_type": "8-K"}, event_type="8-K",
                               entities=["H01"], primary_ticker="H01",
                               effective_at=day)
            return emat.assess_materiality(events=[e], holdings=["H01"],
                                           portfolio_state_hash="ph1")
        today = one("2026-08-14")
        tomorrow = one("2026-08-15")
        assert today["trigger_fingerprint"] != tomorrow["trigger_fingerprint"]

    def test_a_macro_observation_is_not_material_but_a_regime_transition_is(self):
        quiet = emat.assess_materiality(events=[], holdings=["H01"],
                                        regime_before="CALM", regime_after="CALM")
        shift = emat.assess_materiality(events=[], holdings=["H01"],
                                        regime_before="CALM", regime_after="STRESSED")
        assert quiet["reassessment_required"] is False
        assert shift["reassessment_required"] is True
        assert shift["triggers"][0]["code"] == emat.T_REGIME_TRANSITION

    def test_the_policy_is_versioned_and_not_fitted(self):
        contract = emat.policy_contract()
        assert contract["policy_version"] == emat.MATERIALITY_POLICY_VERSION
        assert contract["fitted_to_outcomes"] is False
        for key in ("abs_return_1d", "max_drawdown", "rank_deterioration_places"):
            assert contract["threshold_reasons"][key]


# --------------------------------------------------------------------------- #
# 5. Dependency graph — incremental vs full refresh
# --------------------------------------------------------------------------- #
class TestDependencyGraph:
    def test_a_price_event_refreshes_risk_not_scoring(self):
        concepts = ek.concepts_for_events([
            ek.build_event(source_id="norgate_local", record_type="MARKET_BAR",
                           source_event_id="b", payload={}, event_type="EOD_BAR")])
        calcs = ek.affected_calculations(concepts)
        assert ek.CALC_MARKET_RISK_STATE in calcs
        assert ek.CALC_UNIVERSE_SCORING not in calcs

    def test_a_periodic_report_refreshes_scoring_and_opportunity_cost(self):
        concepts = ek.concepts_for_events([
            ek.build_event(source_id="sec_edgar", record_type="FILING_EVENT",
                           source_event_id="q", payload={}, event_type="10-Q")])
        calcs = ek.affected_calculations(concepts)
        assert ek.CALC_UNIVERSE_SCORING in calcs
        assert ek.CALC_HOLDING_OPPORTUNITY_COST in calcs

    def test_an_8k_refreshes_review_but_not_scoring(self):
        concepts = ek.concepts_for_events([
            ek.build_event(source_id="sec_edgar", record_type="FILING_EVENT",
                           source_event_id="k", payload={}, event_type="8-K")])
        assert ek.C_THESIS_REVIEW in concepts
        assert ek.C_STRUCTURAL_ALPHA not in concepts
        assert ek.CALC_UNIVERSE_SCORING not in ek.affected_calculations(concepts)

    def test_duplicate_events_invalidate_nothing(self):
        ev = ek.apply_novelty(
            ek.build_event(source_id="sec_edgar", record_type="FILING_EVENT",
                           source_event_id="q", payload={}, event_type="10-Q"),
            {"novelty": ek.NOV_DUPLICATE, "duplicate_of": "x", "reason": "seen"})
        assert ek.concepts_for_events([ev]) == []

    def test_the_graph_is_complete_and_ordered(self):
        graph = ek.build_dependency_graph()
        assert graph["families"]
        for fam in graph["families"]:
            for calc in fam["calculations"]:
                assert calc in graph["calculation_owners"]
        assert graph["creates_orders"] is False


# --------------------------------------------------------------------------- #
# 6. Source capability and the terminal audit
# --------------------------------------------------------------------------- #
class TestSourceCapability:
    def test_every_source_carries_a_terminal_state(self):
        matrix = scap.build_capability_matrix()
        for row in matrix["sources"]:
            assert row["terminal_state"] in ek.TERMINAL_SOURCE_STATES, row["source_id"]
            assert row["why_terminal"], row["source_id"]

    def test_no_source_is_left_available_but_not_integrated(self):
        audit = scap.terminal_audit()
        assert audit["counts"]["READY_UNINTEGRATED_USEFUL_SOURCES"] == 0, \
            audit["ready_unintegrated_useful_sources"]

    def test_the_forbidden_non_terminal_states_are_unreachable(self):
        matrix = scap.build_capability_matrix()
        states = {r["terminal_state"] for r in matrix["sources"]}
        assert not (states & set(ek.FORBIDDEN_NON_TERMINAL_STATES))

    def test_a_source_never_claims_more_authority_than_its_families(self):
        matrix = scap.build_capability_matrix()
        for row in matrix["sources"]:
            expected = sorted({ek.EVENT_FAMILIES[f]["decision_authority"]
                               for f in row["event_families"]})
            assert row["decision_authorities"] == expected, row["source_id"]
            if row["can_score_now"]:
                assert ek.AUTH_OPERATIONAL_ALPHA in expected, row["source_id"]

    def test_a_blocked_source_can_neither_score_nor_trigger(self):
        matrix = scap.build_capability_matrix()
        for row in matrix["sources"]:
            if row["terminal_state"].startswith("BLOCKED"):
                assert row["can_score_now"] is False, row["source_id"]
                assert row["can_trigger_now"] is False, row["source_id"]
                assert row["blocker"], row["source_id"]

    def test_the_matrix_reads_only(self):
        matrix = scap.build_capability_matrix()
        assert matrix["safety"]["read_only"] is True
        assert matrix["safety"]["purchased_data"] is False
        assert matrix["safety"]["paid_historical_call"] is False

    def test_the_collector_alias_resolves_to_one_source_identity(self):
        assert scap.canonical_source_id("rss_atom") == "news_rss"
        rec = replay.stage2_record(
            source_id="rss_atom", record_type="REGULATORY_EVENT",
            event_type="REGULATORY_EVENT:REGULATOR", native_id="r1", ticker=None,
            effective_at=ELIGIBLE, published_at=ELIGIBLE,
            payload={"title": "An enforcement action"})
        ev = fabric.record_to_event(rec, lane="test")
        assert ev["source_id"] == "news_rss"
        assert ev["collector_id"] == "rss_atom"


# --------------------------------------------------------------------------- #
# 7. Source freshness — cadence aware, never fabricated
# --------------------------------------------------------------------------- #
class TestSourceFreshness:
    def test_freshness_delegates_to_the_canonical_classifier(self, roots, tmp_path):
        empty = tmp_path / "empty"
        empty.mkdir()
        state = fabric.build_source_freshness(anchor=ELIGIBLE,
                                              fabric_dir=roots["fabric"],
                                              ingestion_root=empty, news_root=empty)
        assert state["classifier_owner"] == "api.data_freshness.classify_source"

    def test_a_missing_watermark_is_reported_missing_not_zero_filled(self, roots,
                                                                     tmp_path):
        empty = tmp_path / "empty"
        empty.mkdir()
        state = fabric.build_source_freshness(anchor=ELIGIBLE,
                                              fabric_dir=roots["fabric"],
                                              ingestion_root=empty, news_root=empty)
        rows = {r["source_id"]: r for r in state["sources"]}
        assert rows["eodhd"]["status"] == "MISSING"
        assert rows["eodhd"]["source_watermark"] is None

    def test_a_recorded_source_error_is_surfaced(self, roots, tmp_path):
        empty = tmp_path / "empty"
        empty.mkdir()
        fabric.save_watermarks(
            {"sec_edgar": {"source_watermark": "2026-05-01", "last_error": "HTTP 503"}},
            fabric_dir=roots["fabric"])
        state = fabric.build_source_freshness(anchor=ELIGIBLE,
                                              fabric_dir=roots["fabric"],
                                              ingestion_root=empty, news_root=empty)
        rows = {r["source_id"]: r for r in state["sources"]}
        assert rows["sec_edgar"]["last_error"] == "HTTP 503"
        assert rows["sec_edgar"]["status"] != "FRESH"
        assert state["degraded_count"] >= 1

    def test_a_publisher_driven_feed_is_not_stale_for_one_quiet_day(self, roots,
                                                                    tmp_path):
        empty = tmp_path / "empty"
        empty.mkdir()
        fabric.save_watermarks({"sec_edgar": {"source_watermark": "2026-08-13"}},
                               fabric_dir=roots["fabric"])
        state = fabric.build_source_freshness(anchor=ELIGIBLE,
                                              fabric_dir=roots["fabric"],
                                              ingestion_root=empty, news_root=empty)
        rows = {r["source_id"]: r for r in state["sources"]}
        assert rows["sec_edgar"]["status"] in ("FRESH", "NOT_DUE")

    def test_a_future_dated_corporate_action_does_not_mark_a_source_future_dated(self):
        """An EODHD dividend cursor names a future EX-DATE, not the collection date."""
        # The watermark helper prefers the collection date over the newest record.
        matrix = scap.build_capability_matrix()
        rows = {r["source_id"]: r for r in matrix["sources"]}
        wm = rows["eodhd"]["source_watermark"]
        assert wm is None or wm <= "2026-12-31"


# --------------------------------------------------------------------------- #
# 8. The orchestrator — one path, shared owners, no writes without a token
# --------------------------------------------------------------------------- #
class TestOrchestration:
    def test_the_cycle_is_token_gated(self, roots):
        res = esr.run_event_signal_refresh(confirm="nope", fabric_dir=roots["fabric"])
        assert res["state"] == esr.ST_NOT_RUN
        assert res["safety"]["performed_write"] is False
        assert res["confirm_required"] == esr.EXECUTE_CONFIRM_TOKEN

    def test_the_event_lane_delegates_to_the_daily_lane_owners(self):
        """The event cycle must call the SAME owners, not a second implementation."""
        from paper_trader.api import daily_research_cycle as drc
        src = json.dumps(esr.CANONICAL_CALCULATION_DELEGATES, sort_keys=True)
        assert "api.holding_opportunity_cost" in src
        assert "api.portfolio_reassessment" in src
        assert "api.reallocation_proposal" in src
        # The daily cycle's own defaults name the same owners.
        for owner in ("holding_opportunity_cost", "portfolio_reassessment",
                      "reallocation_proposal"):
            assert owner in drc.__doc__ or hasattr(drc, "_default_%s_fn" % (
                "holding_opp_cost" if owner == "holding_opportunity_cost"
                else ("reassessment" if owner == "portfolio_reassessment"
                      else "reallocation")))

    def test_the_cycle_hosts_no_second_engine(self):
        """It must not define a scoring, opportunity-cost or allocation calculation."""
        import inspect
        src = inspect.getsource(esr)
        for forbidden in ("def compute_scores(", "def compute_combined(",
                          "def build_books(", "def _percentiles(",
                          "def build_target(", "def place_order(",
                          "def create_order("):
            assert forbidden not in src, forbidden

    def test_a_quiet_cycle_writes_no_decision(self, roots, world):
        res = replay.run_cycle(world=world, records=[], roots=roots)
        assert res["reassessment_ran"] is False
        assert res["proposal_built"] is False
        assert res["state"] == esr.ST_NO_NEW_INFORMATION

    def test_the_cycle_never_creates_an_order_or_promotes_a_model(self, roots, world):
        res = replay.run_cycle(
            world=replay.build_world(holding_ranks={"H01": 240, "H02": 238}),
            records=[], roots=roots)
        for flag in ("creates_orders", "confirms_target", "approves_proposal",
                     "promotes_model", "mutates_operational_holdings",
                     "enables_automation", "scheduler_armed"):
            assert res["safety"][flag] is False, flag

    def test_latency_is_measured_not_fabricated(self, roots):
        world = replay.build_world()
        recs = [_filing("10-Q", "H01", native="q1",
                        published=ELIGIBLE + "T13:00:00+00:00"),
                replay.stage2_record(source_id="eodhd", record_type="EARNINGS_EVENT",
                                     event_type="EARNINGS_REPORT", native_id="e1",
                                     ticker="H02", effective_at=ELIGIBLE,
                                     published_at=None, payload={"actual": 1.0})]
        res = replay.run_cycle(world=world, records=recs, roots=roots)
        lat = res["latency"]
        assert lat["measured_events"] >= 1
        assert lat["unmeasurable_events"] >= 1
        assert lat["cycle_duration_seconds"] >= 0

    def test_the_read_contract_performs_no_write(self, roots, tmp_path, world):
        empty = tmp_path / "empty"
        empty.mkdir()
        payload = esr.load_event_signal_refresh_status(
            fabric_dir=roots["fabric"], ingestion_root=empty, news_root=empty,
            portfolio_state=world["portfolio_state"])
        assert payload["safety"]["read_only"] is True
        assert payload["safety"]["performed_write"] is False
        assert payload["scheduler"]["armed"] is False
        assert payload["confirm_required"] == esr.EXECUTE_CONFIRM_TOKEN

    def test_no_scheduler_is_armed_by_this_release(self, roots, tmp_path, world):
        empty = tmp_path / "empty"
        empty.mkdir()
        payload = esr.load_event_signal_refresh_status(
            fabric_dir=roots["fabric"], ingestion_root=empty, news_root=empty,
            portfolio_state=world["portfolio_state"])
        assert payload["scheduler"]["armed"] is False
        assert payload["scheduler"]["interval_seconds"] is None


# --------------------------------------------------------------------------- #
# 9. Market/risk state uses the canonical price owner
# --------------------------------------------------------------------------- #
class TestMarketRiskState:
    def test_risk_state_is_computed_by_the_price_panel_owner(self, world):
        state = esr.build_market_risk_state(price_panel=world["price_panel"],
                                            tickers=world["holdings"],
                                            eligible=world["eligible"])
        assert state["calculation_owner"] == "api.price_panel"
        assert state["covered"] == len(world["holdings"])
        row = state["rows"][world["holdings"][0]]
        for key in ("ret_1", "rvol_63", "maxdd_252", "median_dollar_volume"):
            assert key in row

    def test_a_shock_is_visible_in_the_risk_state(self, world):
        shocked = replay.build_world(shocks={"H01": -0.12})
        state = esr.build_market_risk_state(price_panel=shocked["price_panel"],
                                            tickers=["H01"],
                                            eligible=shocked["eligible"])
        assert state["rows"]["H01"]["ret_1"] < -0.10

    def test_missing_coverage_is_reported_not_invented(self, world):
        state = esr.build_market_risk_state(price_panel=world["price_panel"],
                                            tickers=["NOT_IN_PANEL"],
                                            eligible=world["eligible"])
        assert state["rows"] == {}
        assert state["missing"] == ["NOT_IN_PANEL"]

    def test_rank_deltas_are_absent_without_a_real_prior(self, world):
        deltas = esr.build_rank_deltas(scoring=world["scoring"], prior_ranking=None,
                                       held=world["holdings"])
        assert deltas["prior_available"] is False
        for row in deltas["rows"].values():
            assert row["rank_before"] is None
            assert row["prior_available"] is False


# --------------------------------------------------------------------------- #
# 10. Deterministic replay — the ten required scenarios
# --------------------------------------------------------------------------- #
class TestReplay:
    def test_every_required_scenario_is_defined(self):
        assert sorted(replay.SCENARIOS) == list("ABCDEFGHIJ")

    def test_the_replay_passes_every_scenario(self, tmp_path):
        res = replay.run_replay(base_dir=tmp_path / "replay")
        failed = [(s["scenario"], s["failed"]) for s in res["scenarios"]
                  if not s["passed"]]
        assert res["passed"] is True, failed
        assert res["check_count"] >= 70
        assert res["safety"]["creates_orders"] is False

    def test_replay_is_deterministic(self, tmp_path):
        a = replay.run_replay(base_dir=tmp_path / "a", scenarios=["A", "C", "F", "I"])
        b = replay.run_replay(base_dir=tmp_path / "b", scenarios=["A", "C", "F", "I"])
        strip = lambda r: [(s["scenario"], s["passed"],  # noqa: E731
                            [(c["check"], c["expected"], c["observed"])
                             for c in s["checks"]])
                           for s in r["scenarios"]]
        assert strip(a) == strip(b)


# --------------------------------------------------------------------------- #
# 11. Challenger continuity — the event fabric cannot promote research
# --------------------------------------------------------------------------- #
class TestChallengerContinuity:
    def test_the_event_lane_declares_no_promotion_path(self):
        import inspect
        for module in (esr, fabric, scap):
            src = inspect.getsource(module)
            for forbidden in ("promote_model(", "replace_champion(",
                              "confirm_target(", "place_order(", "submit_order(",
                              "create_order(", "run_fill_cycle("):
                assert forbidden not in src, "%s in %s" % (forbidden, module.__name__)

    def test_research_evidence_is_not_an_operational_calculation(self):
        graph = ek.build_dependency_graph()
        research = [f for f in graph["families"]
                    if f["decision_authority"] == ek.AUTH_RESEARCH_ALPHA]
        assert research
        for fam in research:
            assert fam["reaches_operational_target"] is False
            assert fam["calculations"] == [ek.CALC_RESEARCH_EVIDENCE]

    def test_no_forward_evidence_is_created_by_the_event_lane(self):
        import inspect
        src = inspect.getsource(esr) + inspect.getsource(fabric)
        for forbidden in ("capture_snapshots(", "TRUE_FORWARD", "advance_shadow_books(",
                          "maybe_activate_shadow_books("):
            assert forbidden not in src, forbidden


# --------------------------------------------------------------------------- #
# 12. Contracts are machine-readable and stable
# --------------------------------------------------------------------------- #
class TestContracts:
    def test_the_event_contract_names_its_guarantees(self):
        contract = ek.event_contract()
        assert contract["contract_id"] == ek.EVENT_CONTRACT_ID
        assert "idempotency_key" in contract["identity"]
        assert contract["safety"]["creates_orders"] is False
        assert set(ek.EVENT_FIELDS) <= set(contract["fields"])

    def test_a_built_event_carries_every_contract_field(self):
        ev = ek.build_event(source_id="s", record_type="NEWS_EVENT",
                            source_event_id="n", payload={"title": "t"},
                            event_type="NEWS")
        missing = [f for f in ek.EVENT_FIELDS if f not in ev]
        assert missing == []

    def test_the_contracts_are_json_serialisable(self):
        for payload in (ek.event_contract(), ek.build_dependency_graph(),
                        emat.policy_contract(), scap.build_capability_matrix(),
                        scap.terminal_audit()):
            json.loads(json.dumps(payload, default=str))
