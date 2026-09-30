"""R85 - a future-dated source record must never move the event-corpus window.

THE DEFECT
----------
The corpus lane computed its lookback floor as ``newest partition name - 14 days``.
On 2026-09-11 the official SEC press-release RSS feed re-served two 1997 releases
(97-99, 97-114) stamped 2026-11-05 and 2026-12-16. The collector filed them under
those future partitions, the floor jumped to 2026-12-02, and every legitimate
REGULATORY_EVENT partition from 2026-09-11 onward stopped reaching the fabric -
while the source watermark (2026-12-16) reported the feed as fresh.

WHAT THESE TESTS PROTECT
------------------------
* the window is anchored to the requested session and information cutoff;
* a stated publication later than our own recorded retrieval is DISPUTED, never
  POINT_IN_TIME_OK, and its availability is bounded by first observation;
* no watermark and no fabric partition is ever named by a future date;
* replay is idempotent and no source or fabric history is rewritten;
* an unmapped regulatory event reaches no holding and gains no authority;
* the UI qualifies a disputed date instead of showing it as an ordinary time.

HERMETIC: every root is ``tmp_path``. Nothing opens a production store.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from paper_trader.api import event_fabric as fabric
from paper_trader.api import event_signal_refresh as esr
from paper_trader.api import material_information as mi
from paper_trader.api import operator_presentation as op
from paper_trader.engine import event_fabric as ek

REPO = Path(__file__).resolve().parents[1]
RETRIEVED_SEC = "2026-09-11T02:58:49.612687+00:00"


def _rss_record(*, native: str, effective: str, published: str, retrieved: str,
                title: str, link: str = "https://www.sec.gov/newsroom/press-releases/x",
                tickers=None) -> dict:
    """The exact shape the Stage-3.5 RSS collector writes (collector copies the
    stated publication into observed_at/available_at; retrieved_at is ours)."""
    return {
        "available_at": published, "company_id": None, "effective_at": effective,
        "entity_mapping_confidence": "MATCHED_EXACT" if tickers else "UNMATCHED",
        "event_type": "REGULATORY_EVENT:REGULATOR", "exchange": None,
        "normalized_payload": {
            "canonical_link": link, "feed_id": "sec_press_releases",
            "mapped_tickers": list(tickers or []), "official_source": True,
            "publication_time": published,
            "publisher": "U.S. Securities and Exchange Commission",
            "source_category": "REGULATOR", "title": title,
            "trust_level": "PRIMARY_OFFICIAL"},
        "observed_at": published, "quality_warnings": [],
        "record_id": "rec_" + native, "record_type": "REGULATORY_EVENT",
        "retrieved_at": retrieved, "source_id": "rss_atom",
        "source_native_id": "sec_press_releases|" + native, "ticker": None,
    }


def _write(root: Path, rec: dict, *, part: str, name: str = "stage3_5_x.jsonl") -> Path:
    d = root / "normalized" / "REGULATORY_EVENT" / part[:4] / part[5:7] / part[8:10]
    d.mkdir(parents=True, exist_ok=True)
    p = d / name
    with p.open("a", encoding="utf-8", newline="\n") as fh:
        fh.write(json.dumps(rec) + "\n")
    return p


def _tree_digest(root: Path) -> dict:
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob("*")) if p.is_file()}


@pytest.fixture()
def news(tmp_path):
    """Valid September partitions plus the two observed future-dated SEC items."""
    root = tmp_path / "news_rss"
    for i, day in enumerate(("2026-09-05", "2026-09-12", "2026-09-20", "2026-09-28")):
        _write(root, _rss_record(native="it_valid_%d" % i, effective=day,
                                 published=day + "T10:00:00-04:00",
                                 retrieved=day + "T14:30:00+00:00",
                                 title="SEC release number %d" % i,
                                 link="https://www.sec.gov/r/valid-%d" % i), part=day)
    _write(root, _rss_record(native="it_1dbb0bbf", effective="2026-11-05",
                             published="2026-11-05T07:00:00-05:00",
                             retrieved=RETRIEVED_SEC,
                             title="SEC Chairman Arthur Levitt Iowa town meeting",
                             link="https://www.sec.gov/r/97-99"),
           part="2026-11-05", name="stage3_5_011efaf3b7653ca2.jsonl")
    _write(root, _rss_record(native="it_2e7b11b3", effective="2026-12-16",
                             published="2026-12-16T07:00:00-05:00",
                             retrieved=RETRIEVED_SEC,
                             title="Municipal Securities Underwriters Pay Fines",
                             link="https://www.sec.gov/r/97-114"),
           part="2026-12-16", name="stage3_5_011efaf3b7653ca2.jsonl")
    return root


def _ingest(news_root: Path, tmp_path: Path, **kw) -> dict:
    empty = tmp_path / "empty_stage2"
    empty.mkdir(exist_ok=True)
    return fabric.ingest_corpus_lane(ingestion_root=empty, news_root=news_root,
                                     record_types=["REGULATORY_EVENT"],
                                     entity_index={}, **kw)


def _natives(res: dict) -> set:
    return {e["source_event_id"].split("|")[1] for e in res["events"]}


# --------------------------------------------------------------------------- #
# 1. The window is anchored to the session, never to the newest partition name
# --------------------------------------------------------------------------- #
class TestSessionAnchoredWindow:
    def test_future_partitions_do_not_move_the_floor(self, news, tmp_path):
        res = _ingest(news, tmp_path, as_of="2026-09-29T20:00:00+00:00",
                      session="2026-09-29")
        # floor = 2026-09-15: the 20th and 28th are reachable again.
        assert _natives(res) == {"it_valid_2", "it_valid_3"}
        assert res["window"]["floor"] == "2026-09-15"
        assert res["window"]["anchored_to"] == "REQUESTED_SESSION_NOT_NEWEST_PARTITION"
        assert res["window"]["partitions_after_anchor"] == {
            "news_rss/REGULATORY_EVENT": ["2026-11-05", "2026-12-16"]}
        # The future-dated items were retrieved 09-11, before this floor.
        assert res["window"]["excluded"]["after_anchor_partition_outside_window"] == 2

    def test_the_old_newest_partition_rule_would_have_hidden_september(self, news):
        dates = fabric._partition_dates(news / "normalized" / "REGULATORY_EVENT")
        legacy_floor = "2026-12-02"                     # 2026-12-16 - 14 days
        assert [d for d in dates if d >= legacy_floor] == ["2026-12-16"]
        win = fabric.corpus_window(dates, anchor_date="2026-09-29", lookback_days=14)
        assert win["in_window"] == ["2026-09-20", "2026-09-28"]

    def test_a_single_november_partition_alone_is_also_harmless(self, tmp_path):
        root = tmp_path / "n"
        _write(root, _rss_record(native="a", effective="2026-09-25",
                                 published="2026-09-25T10:00:00+00:00",
                                 retrieved="2026-09-25T10:05:00+00:00", title="a"),
               part="2026-09-25")
        _write(root, _rss_record(native="b", effective="2026-11-05",
                                 published="2026-11-05T07:00:00-05:00",
                                 retrieved=RETRIEVED_SEC, title="b"), part="2026-11-05")
        res = _ingest(root, tmp_path, as_of="2026-09-29T20:00:00+00:00",
                      session="2026-09-29")
        assert _natives(res) == {"a"}

    def test_session_anchors_the_floor_when_it_is_earlier_than_the_clock(
            self, news, tmp_path):
        res = _ingest(news, tmp_path, as_of="2026-09-29T20:00:00+00:00",
                      session="2026-09-21")
        assert res["window"]["anchor_session"] == "2026-09-21"
        assert res["window"]["floor"] == "2026-09-07"
        # 09-28 is after the session anchor, but was retrieved before the cutoff
        # and inside the window, so it is legitimately available. So were the two
        # future-dated SEC items: first observed 09-11, inside [09-07, cutoff].
        assert _natives(res) == {"it_valid_1", "it_valid_2", "it_valid_3",
                                 "it_1dbb0bbf", "it_2e7b11b3"}
        disputed = [e for e in res["events"]
                    if e["point_in_time_status"] == ek.PIT_PUBLICATION_DISPUTED]
        assert len(disputed) == 2

    def test_a_record_not_yet_retrieved_at_the_cutoff_is_not_admitted(
            self, news, tmp_path):
        res = _ingest(news, tmp_path, as_of="2026-09-20T12:00:00+00:00",
                      session="2026-09-20")
        # it_valid_2 was retrieved 2026-09-20T14:30Z - after this cutoff.
        assert "it_valid_2" not in _natives(res)
        assert res["window"]["excluded"]["not_yet_retrieved_at_cutoff"] >= 1

    def test_a_future_dated_item_is_admitted_at_its_first_observation(
            self, news, tmp_path):
        res = _ingest(news, tmp_path, as_of="2026-09-12T12:00:00+00:00",
                      session="2026-09-11")
        by = {e["source_event_id"].split("|")[1]: e for e in res["events"]}
        assert {"it_1dbb0bbf", "it_2e7b11b3", "it_valid_0"} <= set(by)
        ev = by["it_2e7b11b3"]
        assert ev["published_at"] == "2026-12-16T07:00:00-05:00"   # kept verbatim
        assert ev["point_in_time_status"] == ek.PIT_PUBLICATION_DISPUTED
        assert ev["availability_basis"] == ek.AVAIL_FIRST_OBSERVATION
        assert ek.parse_instant(ev["available_at"]) == ek.parse_instant(RETRIEVED_SEC)
        # The per-source watermark candidate is never a future date.
        assert res["per_source"]["news_rss"]["newest_effective_at"] <= "2026-09-12"
        assert res["per_source"]["news_rss"]["publication_disputed"] == 2


# --------------------------------------------------------------------------- #
# 2. PIT verdict: disputed is never POINT_IN_TIME_OK
# --------------------------------------------------------------------------- #
class TestPublicationDispute:
    def _ev(self, **kw):
        base = dict(source_id="news_rss", record_type="REGULATORY_EVENT",
                    source_event_id="x", payload={"title": "t"})
        base.update(kw)
        return ek.build_event(**base)

    def test_publication_after_retrieval_is_disputed(self):
        ev = self._ev(published_at="2026-12-16T07:00:00-05:00", retrieved_at=RETRIEVED_SEC)
        assert ev["point_in_time_status"] == ek.PIT_PUBLICATION_DISPUTED
        assert ev["point_in_time_status"] != ek.PIT_OK
        assert any(w.startswith("PUBLICATION_TIMESTAMP_DISPUTED")
                   for w in ev["quality_warnings"])

    def test_availability_is_never_earlier_than_first_observation(self):
        av = ek.resolve_availability(published_at="2026-12-16", retrieved_at=RETRIEVED_SEC)
        assert ek.parse_instant(av["available_at"]) >= ek.parse_instant(RETRIEVED_SEC)

    def test_valid_earlier_publication_stays_point_in_time_ok(self):
        ev = self._ev(published_at="2026-09-10T20:00:00+00:00", retrieved_at=RETRIEVED_SEC)
        assert ev["point_in_time_status"] == ek.PIT_OK
        assert ev["availability_basis"] == ek.AVAIL_STATED_PUBLICATION
        assert ev["available_at"] == "2026-09-10T20:00:00+00:00"

    def test_small_clock_skew_is_not_a_dispute(self):
        ev = self._ev(published_at="2026-09-11T03:05:00+00:00", retrieved_at=RETRIEVED_SEC)
        assert ev["point_in_time_status"] == ek.PIT_OK

    def test_unknown_stays_unknown_and_is_not_filled_from_retrieval(self):
        ev = self._ev(retrieved_at=RETRIEVED_SEC)
        assert ev["point_in_time_status"] == ek.PIT_UNKNOWN_AVAILABILITY
        assert ev["available_at"] is None
        assert ev["availability_basis"] == ek.AVAIL_UNKNOWN

    def test_no_retrieval_record_means_no_dispute_can_be_proven(self):
        ev = self._ev(published_at="2026-12-16T07:00:00-05:00")
        assert ev["point_in_time_status"] == ek.PIT_OK

    def test_the_contract_declares_the_new_state_and_fields(self):
        c = ek.event_contract()
        assert ek.PIT_PUBLICATION_DISPUTED in c["point_in_time_states"]
        for f in ("retrieved_at", "available_at", "availability_basis"):
            assert f in c["fields"]
        ev = self._ev(published_at="2026-09-10", retrieved_at=RETRIEVED_SEC)
        assert [f for f in ek.EVENT_FIELDS if f not in ev] == []

    def test_identity_is_unchanged_by_the_new_fields(self):
        a = self._ev(published_at="2026-12-16", retrieved_at=RETRIEVED_SEC)
        b = self._ev(published_at="2026-12-16", retrieved_at="2026-09-20T00:00:00+00:00")
        assert a["event_id"] == b["event_id"]


# --------------------------------------------------------------------------- #
# 3. Watermarks, partitions, idempotency, immutability
# --------------------------------------------------------------------------- #
class TestNoFutureStateNoMutation:
    def test_a_watermark_is_never_advanced_to_a_future_date(self, news, tmp_path,
                                                            monkeypatch):
        monkeypatch.setenv(fabric.NOW_ENV, "2026-09-12T12:00:00+00:00")
        res = _ingest(news, tmp_path, as_of="2026-09-12T12:00:00+00:00",
                      session="2026-09-11")
        wm = fabric.advance_watermarks(watermarks={}, per_source=res["per_source"],
                                       admitted=res["events"], duplicates=0)
        assert wm["news_rss"]["source_watermark"] <= "2026-09-12"

    def test_a_prior_future_watermark_is_discarded_not_honoured(self, monkeypatch):
        monkeypatch.setenv(fabric.NOW_ENV, "2026-09-30T12:00:00+00:00")
        ev = ek.build_event(source_id="news_rss", record_type="REGULATORY_EVENT",
                            source_event_id="v", payload={"title": "v"},
                            effective_at="2026-09-29",
                            published_at="2026-09-29T10:00:00+00:00",
                            retrieved_at="2026-09-29T10:01:00+00:00")
        wm = fabric.advance_watermarks(
            watermarks={"news_rss": {"source_watermark": "2026-12-16"}},
            per_source={}, admitted=[ev], duplicates=0)
        assert wm["news_rss"]["source_watermark"] == "2026-09-29"
        assert wm["news_rss"]["future_watermark_discarded"] == "2026-12-16"

    def test_freshness_does_not_read_a_future_watermark_as_fresh(self, monkeypatch,
                                                                 tmp_path):
        monkeypatch.setenv(fabric.NOW_ENV, "2026-09-30T12:00:00+00:00")
        empty = tmp_path / "e"
        empty.mkdir()
        fr = fabric.build_source_freshness(
            watermarks={"news_rss": {"source_watermark": "2026-12-16"}},
            anchor="2026-09-29", ingestion_root=empty, news_root=empty)
        row = next(r for r in fr["sources"] if r["source_id"] == "news_rss")
        assert row["future_watermark_rejected"] == "2026-12-16"
        assert row["source_watermark"] is None
        assert row["status"] != "FRESH"

    def test_disputed_event_is_filed_at_first_observation_and_replay_is_idempotent(
            self, news, tmp_path):
        fab = tmp_path / "fabric"
        res = _ingest(news, tmp_path, as_of="2026-09-12T12:00:00+00:00",
                      session="2026-09-11")
        first = fabric.append_events(res["events"], fabric_dir=fab, run_id="r1")
        assert "2026-12-16" not in first["partitions"]
        assert "2026-11-05" not in first["partitions"]
        assert "2026-09-11" in first["partitions"]
        before = _tree_digest(fab / "events")
        again = _ingest(news, tmp_path, as_of="2026-09-12T12:00:00+00:00",
                        session="2026-09-11")
        second = fabric.append_events(again["events"], fabric_dir=fab, run_id="r2")
        assert second["admitted_count"] == 0
        assert second["duplicates_suppressed"] == len(again["events"])
        assert _tree_digest(fab / "events") == before           # nothing rewritten

    def test_ingestion_never_mutates_the_source_corpus(self, news, tmp_path):
        before = _tree_digest(news)
        _ingest(news, tmp_path, as_of="2026-09-29T20:00:00+00:00", session="2026-09-29")
        _ingest(news, tmp_path, as_of="2026-09-12T12:00:00+00:00", session="2026-09-11")
        assert _tree_digest(news) == before


# --------------------------------------------------------------------------- #
# 4. Authority and the full cycle: unmapped events reach nothing
# --------------------------------------------------------------------------- #
class TestAuthorityAndCycle:
    def test_regulatory_events_cannot_change_alpha(self, news, tmp_path):
        res = _ingest(news, tmp_path, as_of="2026-09-12T12:00:00+00:00",
                      session="2026-09-11")
        for ev in res["events"]:
            assert not ek.authority_may_change_alpha(ev["decision_authority"])
            assert ev["entities"] == []                  # unmapped stays unmapped

    def test_full_cycle_admits_september_suppresses_unmapped_and_creates_nothing(
            self, news, tmp_path, monkeypatch):
        monkeypatch.setenv(fabric.NOW_ENV, "2026-09-29T20:00:00+00:00")
        empty = tmp_path / "stage2"
        empty.mkdir()
        roots = {k: tmp_path / k for k in ("fabric", "hoc", "reassess", "realloc")}
        for p in roots.values():
            p.mkdir()
        ps = {"positions": [{"ticker": "AAA"}, {"ticker": "BBB"}],
              "active_book": {"book_id": "b"},
              "dates": {"eligible_market_date": "2026-09-29"}}
        out = esr.run_event_signal_refresh(
            confirm=esr.EXECUTE_CONFIRM_TOKEN, requested_by="r85_test",
            fabric_dir=roots["fabric"], hoc_dir=roots["hoc"],
            reassessment_dir=roots["reassess"], reallocation_dir=roots["realloc"],
            ingestion_root=empty, news_root=news, portfolio_state=ps,
            scoring={"rankings": []}, price_panel=None, entity_index={},
            prior_ranking={}, now_iso="2026-09-29T20:00:00+00:00",
            hoc_fn=lambda **kw: {"assessment": {"stub": True}},
            reassessment_fn=lambda **kw: {"reassessment": {
                "reassessment_state": "NO_CHANGE_JUSTIFIED"}},
            proposal_fn=lambda **kw: {"proposal": {"stub": True}})
        text = json.dumps(out, default=str)
        assert "REQUESTED_SESSION_NOT_NEWEST_PARTITION" in text
        stored = fabric.read_events(fabric_dir=roots["fabric"])
        natives = {e["source_event_id"].split("|")[1] for e in stored}
        assert natives == {"it_valid_2", "it_valid_3"}
        assert out.get("affected_holdings", []) in ([], None)
        safety = out.get("safety") or {}
        for flag in ("creates_orders", "approves_proposal", "promotes_model",
                     "enables_automation"):
            assert safety.get(flag) in (False, None), flag


# --------------------------------------------------------------------------- #
# 5. Presentation: a disputed date is qualified, never shown as ordinary
# --------------------------------------------------------------------------- #
class TestPresentation:
    def _disputed(self):
        return ek.build_event(source_id="news_rss", record_type="REGULATORY_EVENT",
                              source_event_id="d", payload={"title": "Old SEC release"},
                              effective_at="2026-12-16",
                              published_at="2026-12-16T07:00:00-05:00",
                              retrieved_at=RETRIEVED_SEC,
                              materiality_inputs={"title": "Old SEC release"})

    def test_material_information_shows_first_observation_with_qualification(self):
        feed = mi.build(event_refresh={"material_events": [self._disputed()]})
        row = feed["rows"][0]
        assert row["timestamp_quality"] == mi.TS_DISPUTED
        assert not str(row["timestamp"]).startswith("2026-12-16")
        assert str(row["timestamp"]).startswith("2026-09-11")
        assert row["stated_publication_at"] == "2026-12-16T07:00:00-05:00"
        assert "later than when this system first retrieved" in row["timestamp_note"]
        assert set(mi.TRANSPARENCY_FIELDS) <= set(row)

    def test_attention_summary_carries_the_qualification(self):
        feed = mi.build(event_refresh={"material_events": [self._disputed()]})
        items = op._alerts_summary(feed)["top_items"]
        assert items[0]["timestamp_quality"] == mi.TS_DISPUTED

    def test_ui_renders_the_disputed_badge_in_both_places_without_dialogs(self):
        html = (REPO / "api" / "ui" / "index.html").read_text(encoding="utf-8")
        assert html.count("DISPUTED SOURCE DATE") >= 2
        assert "it.timestamp_quality === 'PUBLICATION_DATE_DISPUTED'" in html
        assert "r.timestamp_quality === 'PUBLICATION_DATE_DISPUTED'" in html


# --------------------------------------------------------------------------- #
# R85.1 - the 2026-09-14 FDA "Apple Sauce" recall was resolved to AAPL
# --------------------------------------------------------------------------- #
class TestSingleTokenNameIsNotAnIdentity:
    INDEX = {"by_name": {"apple": "AAPL", "exxon mobil": "XOM"},
             "by_ticker": {"AAPL": ["apple"], "XOM": ["exxon mobil"]}}
    RECALL = ("Wakefern Food Corp. Voluntarily Recalls Wholesome Pantry Organic "
              "Unsweetened Apple Sauce")

    def _fda_record(self):
        return {"available_at": "2026-09-14T00:00:00-04:00", "effective_at": "2026-09-14",
                "entity_mapping_confidence": "UNMATCHED",
                "event_type": "REGULATORY_EVENT:HEALTH_SAFETY",
                "normalized_payload": {"feed_id": "fda_recalls", "mapped_tickers": [],
                                       "publication_time": "2026-09-14T00:00:00-04:00",
                                       "title": self.RECALL},
                "observed_at": "2026-09-14T00:00:00-04:00", "quality_warnings": [],
                "record_id": "rec_fda", "record_type": "REGULATORY_EVENT",
                "retrieved_at": "2026-09-14T12:00:00+00:00", "source_id": "rss_atom"}

    def test_a_one_word_name_inside_a_product_name_maps_nothing(self):
        ents, conf = fabric.resolve_entities(text=self.RECALL, entity_index=self.INDEX)
        assert ents == []
        assert conf == fabric.AMBIGUOUS_SINGLE_TOKEN_NAME

    def test_the_fda_recall_event_reaches_no_security(self):
        ev = fabric.record_to_event(self._fda_record(), lane="news_rss",
                                    entity_index=self.INDEX)
        assert ev["entities"] == [] and ev["primary_ticker"] is None
        assert ev["identity_confidence"] == fabric.AMBIGUOUS_SINGLE_TOKEN_NAME

    def test_a_multi_word_name_and_a_declared_ticker_still_resolve(self):
        assert fabric.resolve_entities(
            text="Exxon Mobil Corp raises dividend", entity_index=self.INDEX) == (
            ["XOM"], "MATCHED_ALIAS")
        assert fabric.resolve_entities(
            text=self.RECALL, entity_index=self.INDEX, declared=["AAPL"]) == (
            ["AAPL"], "MATCHED_EXACT")