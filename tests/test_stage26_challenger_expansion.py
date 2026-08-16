"""Stage 26 — Alpha Challenger Launch & New-Information Expansion.

Hermetic regressions. No network, no provider, no live price service, no
database beyond temporary SQLite files, no operational store. Every external
surface is injected.

The tests are grouped by the property they defend, because most of what can go
wrong in this stage is silent: a look-ahead that leaks backward, a share count
carried across a split, a shadow book that backdates its own evidence, or a
threshold that drifts between stages so a dead factor comes back to life.
"""
from __future__ import annotations

import json
import struct
import zipfile
from io import BytesIO
from pathlib import Path

import pytest

from alpha_agent import pit_market_equity as pme
from alpha_agent import pit_sector as ps
from alpha_agent import sec_financial_statement_sets as fsds
from alpha_agent import stage26_challenger_expansion as s26
from alpha_agent import tournament as tt


# =========================================================================== #
# SEC Financial Statement Data Sets — acquisition
# =========================================================================== #
SUB_HEADER = ("adsh\tcik\tname\tsic\tcountryba\tform\tperiod\tfy\tfp\tfiled\t"
              "accepted\tprevrpt\tdetail\tinstance\tnciks\taciks")


def _sub_row(adsh, cik, name, sic, form, period, filed, accepted):
    return "\t".join([adsh, cik, name, sic, "US", form, period, "2015", "FY",
                      filed, accepted, "0", "1", "x.xml", "1", ""])


def _sub_txt(rows):
    return ("%s\n%s\n" % (SUB_HEADER, "\n".join(rows))).encode("utf-8")


def _zip_bytes(members: dict, *, stored: "tuple[str, ...]" = ()) -> bytes:
    """Members listed in ``stored`` are written UNCOMPRESSED, so a fixture can
    model the real archive shape: sub.txt is a small deflated member sitting
    beside a num.txt bulk that dominates the file."""
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, payload in members.items():
            zf.writestr(name, payload,
                        compress_type=(zipfile.ZIP_STORED if name in stored
                                       else zipfile.ZIP_DEFLATED))
    return buf.getvalue()


def _range_transport(archive: bytes, *, calls=None):
    """A fake HTTP transport serving byte ranges out of an in-memory zip."""
    def _t(request, timeout):
        if calls is not None:
            calls.append(request)
        if request.get("method") == "HEAD":
            return {"status": 200,
                    "headers": {"Content-Length": str(len(archive)),
                                "Accept-Ranges": "bytes",
                                "Last-Modified": "Mon, 13 Apr 2026 13:07:34 GMT",
                                "ETag": '"abc"'},
                    "body": b""}
        rng = (request.get("headers") or {}).get("Range", "")
        lo, hi = rng.replace("bytes=", "").split("-")
        return {"status": 206, "headers": {}, "body": archive[int(lo):int(hi) + 1]}
    return _t


def test_remote_zip_member_fetch_reads_only_the_requested_member():
    """The whole point of the range fetcher: pull sub.txt without transferring
    the num.txt bulk that dominates every quarterly archive."""
    sub = _sub_txt([_sub_row("0001-15-000001", "320193", "APPLE INC", "3571",
                             "10-K", "20150930", "20151028",
                             "2015-10-28 16:31:00.0")])
    bulk = bytes((i * 7 + 13) % 251 for i in range(400000))
    archive = _zip_bytes({"sub.txt": sub, "num.txt": bulk, "pre.txt": bulk},
                         stored=("num.txt", "pre.txt"))
    calls = []
    f = fsds.RemoteZipMemberFetcher("https://example/2015q4.zip",
                                    transport=_range_transport(archive,
                                                               calls=calls))
    f.head()
    payload, meta = f.fetch_member("sub.txt")
    assert payload == sub
    # A handful of small requests, and a tiny fraction of the archive on the wire.
    assert len(archive) > 700000
    assert f.bytes_fetched < len(archive) / 5
    assert len(calls) <= 5
    assert meta["name"] == "sub.txt"


def test_acquirer_caches_and_never_refetches(tmp_path):
    sub = _sub_txt([_sub_row("0001-15-000001", "320193", "APPLE INC", "3571",
                             "10-K", "20150930", "20151028",
                             "2015-10-28 16:31:00.0")])
    archive = _zip_bytes({"sub.txt": sub, "num.txt": b"y" * 5000},
                         stored=("num.txt",))
    calls = []
    acq = fsds.FinancialStatementDataSetsAcquirer(
        cache_root=tmp_path, user_agent="test/1.0 (someone@example.com)",
        transport=_range_transport(archive, calls=calls), sleep=lambda _s: None)
    first = acq.acquire_quarter(2015, 4)
    assert first["disposition"] == fsds.D_COMPLETE
    n = len(calls)
    second = acq.acquire_quarter(2015, 4)
    assert second["disposition"] == fsds.D_CACHED
    assert len(calls) == n, "a verified cached member must not hit the network"
    assert second["member_sha256"] == first["member_sha256"]


def test_acquirer_refuses_without_an_identifying_contact(tmp_path):
    """SEC fair access requires a contact address; refusing beats being blocked."""
    with pytest.raises(ValueError):
        fsds.FinancialStatementDataSetsAcquirer(
            cache_root=tmp_path, user_agent="paper-trader/2.0")


def test_unpublished_quarter_is_reported_not_invented(tmp_path):
    def _t(request, timeout):
        return {"status": 404, "headers": {}, "body": b""}
    acq = fsds.FinancialStatementDataSetsAcquirer(
        cache_root=tmp_path, user_agent="t/1 (a@b.com)", transport=_t,
        sleep=lambda _s: None)
    assert acq.acquire_quarter(2099, 4)["disposition"] == fsds.D_NOT_PUBLISHED


def test_sub_txt_schema_change_is_loud():
    with pytest.raises(fsds.SchemaChanged):
        fsds.parse_sub_txt(b"adsh\tcik\tname\n1\t2\t3\n")


def test_sic_observations_use_acceptance_not_filed_when_available():
    rows = fsds.parse_sub_txt(_sub_txt([
        _sub_row("a-1", "320193", "APPLE INC", "3571", "10-K", "20150930",
                 "20151028", "2015-10-28 16:31:00.0")]))
    obs = fsds.sic_observations(rows)
    assert obs[0]["availability_basis"] == "SEC_ACCEPTANCE"
    assert obs[0]["available_at"] == "2015-10-28T16:31:00"
    assert obs[0]["sic"] == 3571


def test_sic_observation_without_sic_is_dropped_never_guessed():
    rows = fsds.parse_sub_txt(_sub_txt([
        _sub_row("a-1", "320193", "X", "", "10-K", "20150930", "20151028",
                 "2015-10-28 16:31:00.0")]))
    assert fsds.sic_observations(rows) == []


# =========================================================================== #
# Point-in-time sector — the look-ahead property
# =========================================================================== #
def test_pit_sic_series_never_leaks_a_later_reclassification():
    """The single property the whole Tier-C exercise exists to guarantee."""
    rows = fsds.parse_sub_txt(_sub_txt([
        _sub_row("a-1", "111", "ACME", "3571", "10-K", "20120930", "20121028",
                 "2012-10-28 16:31:00.0"),
        _sub_row("a-2", "111", "ACME", "6021", "10-K", "20200930", "20201028",
                 "2020-10-28 16:31:00.0")]))
    series = fsds.build_pit_sic_series(fsds.sic_observations(rows))
    # Before the first filing: unknown, never back-filled from a later one.
    assert series.sector_as_of("111", "2011-01-01") == ps.UNKNOWN
    # Between the two: the CONTEMPORANEOUS classification only.
    assert series.sector_as_of("111", "2015-06-30") == "Technology"
    # After: the reclassification applies from its acceptance time onward.
    assert series.sector_as_of("111", "2021-06-30") == "Financials"


def test_tier_c_is_inadmissible_for_signal_construction():
    c = s26.TIER_C_CONTRACT
    assert c["leakage_safe"] is True
    assert "signal construction" in c["inadmissible_for"]
    assert "candidate registration" in c["inadmissible_for"]
    assert "sector neutralisation" in c["admissible_for"]


def test_no_registered_valuation_spec_references_a_sector_tier():
    """A classification must not enter a registered signal, or the sector
    falsification becomes circular."""
    blob = json.dumps(s26.valuation_hypothesis_manifest())
    for tier in (s26.TIER_A, s26.TIER_B, s26.TIER_C):
        assert tier not in blob


# =========================================================================== #
# Point-in-time market equity — the two gaps and the split carry
# =========================================================================== #
def test_share_count_is_never_read_from_a_filing_after_the_formation_date(
        tmp_path):
    db = tmp_path / "shares.sqlite"
    _write_share_index(db, [
        ("0000000111", "EntityCommonStockSharesOutstanding", 1000.0,
         "2014-12-31", "2015-02-15"),
        ("0000000111", "EntityCommonStockSharesOutstanding", 2000.0,
         "2015-12-31", "2016-02-15")])
    counts = pme.PitShareCounts(db)
    counts.load()
    assert counts.shares_as_of("111", "2015-06-30")["shares"] == 1000.0
    assert counts.shares_as_of("111", "2016-06-30")["shares"] == 2000.0
    assert counts.shares_as_of("111", "2014-01-01") is None


def _write_share_index(path: Path, rows):
    import sqlite3
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE cf_fact (cik TEXT, concept_tag TEXT, value REAL,"
                 " period_end TEXT, filed TEXT)")
    conn.executemany("INSERT INTO cf_fact VALUES (?,?,?,?,?)", rows)
    conn.commit()
    conn.close()


def test_market_equity_carries_a_share_count_across_a_split_exactly():
    """A 2-for-1 split between the report date and the formation date doubles the
    capital-event factor, which must double the carried share count."""
    shares = {"shares": 1_000_000.0, "filed": "2020-02-15",
              "period_end": "2019-12-31", "concept": "x", "age_days": 200}
    # Pre-split: adjusted/unadjusted = 0.5. Post-split: 1.0.
    report_px = {"close_none": 200.0, "close_capital": 100.0,
                 "capital_factor": 0.5, "date": "2020-02-14"}
    formation_px = {"close_none": 110.0, "close_capital": 110.0,
                    "capital_factor": 1.0, "date": "2020-09-30"}
    out = pme.market_equity(shares_rec=shares, formation_px=formation_px,
                            report_px=report_px)
    assert out["ok"]
    assert out["capital_event_carry"] == pytest.approx(2.0)
    assert out["shares_at_formation"] == pytest.approx(2_000_000.0)
    assert out["market_equity"] == pytest.approx(2_000_000.0 * 110.0)
    assert out["split_between_report_and_formation"] is True


def test_market_equity_without_a_report_price_refuses_rather_than_assuming():
    """Assuming 'no split happened' is exactly the plausible-but-wrong number
    Stage 25 warned about."""
    shares = {"shares": 1e6, "filed": "2020-02-15", "concept": "x",
              "period_end": "2019-12-31", "age_days": 1}
    out = pme.market_equity(
        shares_rec=shares,
        formation_px={"close_none": 10.0, "close_capital": 10.0,
                      "capital_factor": 1.0},
        report_px=None)
    assert out["ok"] is False
    assert out["disposition"] == pme.NO_FACTOR


def test_missing_market_equity_inputs_stay_missing():
    for kwargs, disposition in (
            ({"shares_rec": None, "formation_px": {"capital_factor": 1.0},
              "report_px": {"capital_factor": 1.0}}, pme.NO_SHARES),
            ({"shares_rec": {"shares": 1.0, "filed": "2020-01-01"},
              "formation_px": None, "report_px": {"capital_factor": 1.0}},
             pme.NO_PRICE)):
        assert pme.market_equity(**kwargs)["disposition"] == disposition


def test_implausible_market_equity_is_dropped_not_winsorized():
    shares = {"shares": 1.0, "filed": "2020-01-01", "period_end": "2019-12-31",
              "concept": "x", "age_days": 1}
    out = pme.market_equity(
        shares_rec=shares,
        formation_px={"close_none": 1.0, "close_capital": 1.0,
                      "capital_factor": 1.0},
        report_px={"close_none": 1.0, "close_capital": 1.0,
                   "capital_factor": 1.0})
    assert out["disposition"] == pme.IMPLAUSIBLE
    assert out["ok"] is False


def test_valuation_factor_is_none_when_market_equity_is_absent():
    """A missing denominator must not silently become a computable ratio."""
    rec = {"cur": {"net_income": 100.0, "revenue": 900.0,
                   "stockholders_equity": 500.0}}
    for spec in s26.VALUATION_FACTORS:
        assert spec.value(rec) is None, spec.name


def test_enterprise_value_is_not_computed_without_debt_and_cash():
    rec = {"cur": {"revenue": 900.0, "market_equity": 1000.0}}
    assert s26.VALUATION_BY_NAME["s26_sales_to_ev"].value(rec) is None
    rec["cur"].update({"long_term_debt": 200.0, "cash": 50.0})
    assert s26.VALUATION_BY_NAME["s26_sales_to_ev"].value(rec) == pytest.approx(
        900.0 / 1150.0)


def test_size_hypothesis_carries_the_pre_registered_negative_sign():
    assert s26.VALUATION_BY_NAME["s26_market_equity_size"].direction == -1


def test_every_valuation_sign_is_fixed_before_evaluation():
    m = s26.valuation_hypothesis_manifest()
    assert m["signs_fixed_before_evaluation"] is True
    assert m["sign_fitted_from_data"] is False
    assert m["brute_force_parameter_search_performed"] is False
    assert len(m["experiments"]) == len(s26.VALUATION_FACTORS)
    for e in m["experiments"]:
        assert e["expected_sign"] in (1, -1)
        assert e["economic_rationale"]


# =========================================================================== #
# The released companyfacts parser — opt-in widening must not change defaults
# =========================================================================== #
def test_share_unit_widening_is_opt_in_and_default_behaviour_is_unchanged():
    from alpha_agent.sec_companyfacts_index import parse_companyfacts_facts

    doc = {"cik": 111, "facts": {
        "us-gaap": {
            "Assets": {"units": {"USD": [
                {"val": 10.0, "end": "2015-12-31", "fy": 2015, "fp": "FY",
                 "filed": "2016-02-15", "accn": "a-1", "form": "10-K"}]}},
            "CommonStockSharesOutstanding": {"units": {"shares": [
                {"val": 99.0, "end": "2015-12-31", "fy": 2015, "fp": "FY",
                 "filed": "2016-02-15", "accn": "a-1", "form": "10-K"}]}}},
        "dei": {
            "EntityCommonStockSharesOutstanding": {"units": {"shares": [
                {"val": 98.0, "end": "2016-01-31", "fy": 2015, "fp": "FY",
                 "filed": "2016-02-15", "accn": "a-1", "form": "10-K"}]}}}}}
    tags = {"Assets", "CommonStockSharesOutstanding",
            "EntityCommonStockSharesOutstanding"}

    default = parse_companyfacts_facts(doc, cik="0000000111", target_tags=tags)
    assert [f["concept"] for f in default] == ["Assets"], (
        "the released monetary-USD / us-gaap-only behaviour must be unchanged")

    widened = parse_companyfacts_facts(
        doc, cik="0000000111", target_tags=tags,
        extra_units={"shares"}, extra_taxonomies=("dei",))
    got = {f["concept"]: f["value"] for f in widened}
    assert got == {"Assets": 10.0, "CommonStockSharesOutstanding": 99.0,
                   "EntityCommonStockSharesOutstanding": 98.0}


# =========================================================================== #
# Shadow books — governance, immutability and no fake maturation
# =========================================================================== #
def _seed_keeper(reg, name: str, *, score: float = 0.70) -> str:
    """Seed one scored KEEP_FOR_RESEARCH candidate and return its candidate id."""
    cid = reg.seed_candidate(
        name=name, family="quality_profitability", spec={"feature": name},
        data_dependencies=["owned"], universe="u", pit_status="OWNED_PIT")
    reg.record_evaluation(cid, metrics={"rank_ic": 0.05},
                          scores={"combined_score": score},
                          gate={"target_state": tt.KEEP_FOR_RESEARCH},
                          pit={"point_in_time_valid": True})
    reg.transition(cid, tt.TESTING)
    reg.transition(cid, tt.KEEP_FOR_RESEARCH)
    return cid


def _cfg(tmp_path, *, require_allowlist: bool):
    return {"shadow_books": {
        "shadow_book_root": str(tmp_path / "books"),
        "min_combined_score_for_shadow": 0.55,
        "max_active_shadow_books": 3, "cost_bps_round_trip": 50,
        "benchmark": "SPY",
        tt.SHADOW_ALLOWLIST_REQUIRED_KEY: require_allowlist}}


def test_shadow_activation_is_fail_closed_without_an_allowlist(tmp_path):
    """A candidate can clear the score floor while its own falsification battery
    condemns it, so a score threshold must not be able to open a forward book."""
    reg = tt.CandidateRegistry(tmp_path / "t.sqlite")
    try:
        _seed_keeper(reg, "keeper")
        assert tt.maybe_activate_shadow_books(
            reg, _cfg(tmp_path, require_allowlist=True)) == []
        assert reg.list(state=tt.SHADOW_BOOK_ACTIVE) == []
        assert reg.list_shadow_books() == []
    finally:
        reg.close()


def test_allowlist_enrols_only_the_named_candidate(tmp_path):
    """The Stage-26 governance property: a fragile candidate that clears the
    score floor must NOT be swept into a forward book alongside the eligible one."""
    reg = tt.CandidateRegistry(tmp_path / "t.sqlite")
    try:
        good = _seed_keeper(reg, "keeper")
        fragile = _seed_keeper(reg, "concentration_fragile")
        out = tt.maybe_activate_shadow_books(
            reg, _cfg(tmp_path, require_allowlist=True),
            eligible_candidate_ids=[good], evidence_date="2026-08-16")
        assert [o["candidate_id"] for o in out] == [good]
        assert {c["candidate_id"]
                for c in reg.list(state=tt.KEEP_FOR_RESEARCH)} == {fragile}
        assert {c["candidate_id"]
                for c in reg.list(state=tt.SHADOW_BOOK_ACTIVE)} == {good}
    finally:
        reg.close()


def test_activation_without_an_inception_provider_would_make_an_empty_book(
        tmp_path):
    """Documents WHY Stage 26 supplies one: the canonical activator's default
    writes a book that can never produce a mark."""
    reg = tt.CandidateRegistry(tmp_path / "t.sqlite")
    try:
        cid = _seed_keeper(reg, "n")
        tt.maybe_activate_shadow_books(
            reg, _cfg(tmp_path, require_allowlist=True),
            eligible_candidate_ids=[cid], evidence_date="2026-08-16")
        book = tt.ShadowBook(tmp_path / "books", "sb_%s" % cid)
        assert book._load()["inception"]["membership"] == []
        assert s26.shadow_book_nav(membership=[], prices={}) is None
    finally:
        reg.close()


def test_membership_is_dollar_neutral_and_uses_the_frozen_ranking():
    ranked = [("A", 9.0), ("B", 8.0), ("C", 1.0), ("D", 0.0)]
    prices = {s: 10.0 for s, _ in ranked}
    m = s26.build_shadow_membership(ranked=ranked, leg_size=2,
                                    entry_prices=prices)
    longs = [p for p in m if p["leg"] == "LONG"]
    shorts = [p for p in m if p["leg"] == "SHORT"]
    assert {p["symbol"] for p in longs} == {"A", "B"}
    assert {p["symbol"] for p in shorts} == {"C", "D"}
    assert sum(p["weight"] for p in m) == pytest.approx(0.0)
    assert all(p["entry_price"] == 10.0 for p in m)


def test_names_without_an_entry_price_never_enter_the_book():
    ranked = [("A", 9.0), ("B", 8.0), ("C", 1.0), ("D", 0.0)]
    m = s26.build_shadow_membership(
        ranked=ranked, leg_size=2, entry_prices={"A": 10.0, "C": 5.0})
    assert {p["symbol"] for p in m} == {"A", "C"}


def test_nav_kernel_refuses_a_partially_priced_book():
    membership = [{"symbol": "A", "weight": 0.5, "entry_price": 10.0},
                  {"symbol": "B", "weight": 0.5, "entry_price": 10.0},
                  {"symbol": "C", "weight": -1.0, "entry_price": 10.0}]
    assert s26.shadow_book_nav(membership=membership,
                               prices={"A": 11.0}) is None
    full = s26.shadow_book_nav(
        membership=membership, prices={"A": 11.0, "B": 11.0, "C": 10.0},
        notional=100000.0, cost_bps=0.0)
    assert full["nav"] == pytest.approx(110000.0)
    assert full["coverage"] == pytest.approx(1.0)


def test_mark_provider_returns_none_for_an_unknown_book(tmp_path):
    provider = s26.make_shadow_mark_provider(
        tmp_path / "books", close_provider=lambda syms, date: {})
    assert provider("nope", "2026-08-17") is None


def test_mark_provider_prices_a_real_book(tmp_path):
    book = tt.ShadowBook(tmp_path / "books", "sb_c1")
    book.inception(candidate_id="c1", inception_date="2026-08-16",
                   membership=[{"symbol": "A", "weight": 1.0,
                                "entry_price": 100.0},
                               {"symbol": "B", "weight": -1.0,
                                "entry_price": 100.0}],
                   benchmark="SPY", cost_bps=0.0, spec={}, notional=100000.0)
    provider = s26.make_shadow_mark_provider(
        tmp_path / "books", cost_bps=0.0,
        close_provider=lambda syms, date: {"A": 110.0, "B": 100.0,
                                           "SPY": 500.0})
    mark = provider("c1", "2026-08-17")
    assert mark["nav"] == pytest.approx(110000.0)
    assert mark["benchmark_close"] == 500.0
    assert mark["turnover"] == 0.0


def test_forward_history_is_never_backdated(tmp_path):
    book = tt.ShadowBook(tmp_path / "books", "sb_c1")
    book.inception(candidate_id="c1", inception_date="2026-08-16",
                   membership=[{"symbol": "A", "weight": 1.0,
                                "entry_price": 1.0}],
                   benchmark="SPY", cost_bps=0.0, spec={})
    with pytest.raises(tt.RetroactiveError):
        book.record_mark(date="2026-08-15", nav=1.0)
    with pytest.raises(tt.RetroactiveError):
        book.record_mark(date="2026-08-16", nav=1.0)
    book.record_mark(date="2026-08-17", nav=101.0)
    with pytest.raises(tt.RetroactiveError):
        book.record_mark(date="2026-08-17", nav=102.0)


def test_inception_is_first_write_wins(tmp_path):
    book = tt.ShadowBook(tmp_path / "books", "sb_c1")
    book.inception(candidate_id="c1", inception_date="2026-08-16",
                   membership=[{"symbol": "A"}], benchmark="SPY",
                   cost_bps=0.0, spec={"v": 1})
    book.inception(candidate_id="c1", inception_date="2027-01-01",
                   membership=[{"symbol": "Z"}], benchmark="QQQ",
                   cost_bps=99.0, spec={"v": 2})
    inc = book._load()["inception"]
    assert inc["date"] == "2026-08-16"
    assert inc["membership"] == [{"symbol": "A"}]
    assert inc["spec"] == {"v": 1}


def test_a_new_book_starts_with_zero_marks(tmp_path):
    book = tt.ShadowBook(tmp_path / "books", "sb_c1")
    book.inception(candidate_id="c1", inception_date="2026-08-16",
                   membership=[{"symbol": "A"}], benchmark="SPY",
                   cost_bps=0.0, spec={})
    assert book.replay()["forward_observations"] == 0


# =========================================================================== #
# The freeze — a frozen spec must not drift
# =========================================================================== #
def test_challenger_freeze_is_content_hashed_and_deterministic():
    a = s26.challenger_freeze_contract()
    b = s26.challenger_freeze_contract()
    assert a["spec_hash"] == b["spec_hash"]
    assert a["refit_forbidden"] is True
    assert a["weights_fitted_from_data"] is False


def test_freeze_hash_is_stable_across_candidate_lifecycle_changes():
    """The hash must cover the SPECIFICATION only. If it moved when the
    candidate transitioned KEEP_FOR_RESEARCH -> SHADOW_BOOK_ACTIVE, or when a
    score was re-measured, it would not be a freeze - and the hash recorded
    inside an already-open shadow book would stop matching."""
    identity = {"candidate_id": "c1", "name": s26.FROZEN_CHALLENGER,
                "family": "quality_profitability", "spec_hash": "abc",
                "spec_version": "1.0.0", "code_hash": "z",
                "pit_status": "OWNED_PIT", "universe": "u",
                "data_dependencies": ["owned"], "experiment_ids": ["e"]}
    before = s26.challenger_freeze_contract(
        registry_row={**identity, "lifecycle_state": "KEEP_FOR_RESEARCH",
                      "combined_score": 0.685172,
                      "latest_evidence_date": "2026-08-15T23:25:15",
                      "active_shadow_book_id": None},
        stage25_evidence={"run_id": "stage25_f811c142f7dbd7e0"})
    after = s26.challenger_freeze_contract(
        registry_row={**identity, "lifecycle_state": "SHADOW_BOOK_ACTIVE",
                      "combined_score": 0.71,
                      "latest_evidence_date": "2026-09-01T00:00:00",
                      "active_shadow_book_id": "sb_c1"},
        stage25_evidence={"run_id": "different"})
    assert before["spec_hash"] == after["spec_hash"]
    assert before["registry_lifecycle"] != after["registry_lifecycle"]


def test_freeze_hash_changes_when_the_specification_changes(monkeypatch):
    base = s26.challenger_freeze_contract()
    moved = s26.challenger_freeze_contract(
        data_fingerprints={"concept_mapping_version_hash": "changed"})
    assert moved["spec_hash"] != base["spec_hash"]


def test_frozen_spec_carries_no_classification_input():
    f = s26.challenger_freeze_contract()
    assert "NONE" in f["standalone"]["sector_tier_used_in_construction"]


# =========================================================================== #
# Sector revalidation — thresholds must not drift between stages
# =========================================================================== #
def test_survival_thresholds_are_the_stage25_ones_verbatim():
    from alpha_agent import stage25_alpha_discovery as s25
    assert s26.SURVIVE_MIN_T is s25.RND_SURVIVE_MIN_T
    assert s26.SURVIVE_MIN_RETENTION is s25.RND_SURVIVE_MIN_RETENTION
    assert s26.SURVIVE_MIN_T == 2.0
    assert s26.SURVIVE_MIN_RETENTION == 0.50


def test_revalidation_declares_what_it_did_not_change():
    from alpha_agent.stage25_alpha_discovery import Stage25Panel
    out = s26.sector_revalidation(Stage25Panel(), factors=())
    assert "the hypotheses" in out["what_did_not_change"]
    assert "the factor definitions" in out["what_did_not_change"]
    assert out["thresholds_reused_from_stage25"]["min_controlled_ic_t"] == 2.0


def test_rnd_stays_concentration_fragile_regardless_of_sector_evidence():
    assert s26.SECTOR_AFFECTED["s24_rnd_intensity"] == "CONCENTRATION_FRAGILE"


# =========================================================================== #
# Governance — no promotion, no operational reach
# =========================================================================== #
def test_no_automatic_promotion_anywhere_in_stage26():
    from api import universe_scoring
    assert universe_scoring.AUTOMATIC_PROMOTION_ALLOWED is False
    src = Path("alpha_agent/stage26_challenger_expansion.py").read_text(
        encoding="utf-8")
    assert "AUTOMATIC_PROMOTION_ALLOWED" not in src
    # READY_FOR_MANUAL_REVIEW may be NAMED in a governance explanation, but the
    # stage must never transition a candidate into it, nor into any promoted
    # state. Only the shadow lifecycle is reachable from here.
    assert "READY_FOR_MANUAL_REVIEW)" not in src
    assert ".transition(" not in src
    readiness = s26.shadow_forward_readiness(
        registry_counts={}, shadow_books=[], frozen={"spec_hash": "x"})
    assert readiness["governance"]["promotion_possible_from_this_stage"] is False
    assert readiness["governance"][
        "operator_approval_required_for_promotion"] is True


def test_stage26_module_contains_no_order_execution_terms():
    src = Path("alpha_agent/stage26_challenger_expansion.py").read_text(
        encoding="utf-8")
    for term in ("place_order", "submit_order", "execute_order", "send_order",
                 "broker_execute", "live_order", "route_order"):
        assert "%s(" % term not in src


def test_forward_evidence_contract_forbids_fake_maturation():
    c = s26.forward_evidence_contract(readiness={}, frozen={"spec_hash": "x"})
    assert c["fake_maturation_forbidden"] is True
    assert c["backdating_forbidden"] is True
    assert c["no_production_cycle_run_by_stage26"] is True


def test_hoc_contract_does_not_rerun_the_historical_counterfactual():
    h = s26.hoc_forward_contract()
    assert h["historical_counterfactual_rerun_by_stage26"] is False
    assert h["no_second_hoc_engine_created"] is True
    assert h["current_state"] == "INSUFFICIENT_FORWARD_EVIDENCE"


def test_purchase_gate_never_authorises_and_applies_the_released_rule():
    frontier = s26.new_information_frontier(
        sector_status={"state": "READY_FOR_PIT_RESEARCH"},
        market_cap_status={"stage26_state": "READY_FOR_PIT_RESEARCH"},
        valuation_outcome={"state": "COMPLETE", "summary": ""})
    exhaustion = s26.research_exhaustion_update(frontier=frontier)
    gate = s26.external_data_purchase_gate(
        frontier=frontier, exhaustion=exhaustion,
        valuation_outcome={"state": "COMPLETE"})
    assert gate["authorises_purchase"] is False
    assert gate["verdict"] in ("WAIT", "BUY_CANDIDATE", "REJECT")
    intrinio = next(d for d in gate["datasets"] if "Intrinio" in d["dataset"])
    assert "AS-WAS" in intrinio["hard_requirement"]


def test_exhaustion_update_creates_no_second_queue():
    e = s26.research_exhaustion_update()
    assert e["second_queue_created"] is False
    assert e["concepts"]["ANALYST_REVISIONS"] == "WAITING_FOR_DATA"
    assert "CONCENTRATION_FRAGILE" in e["concepts"]["R_AND_D"]


def test_intrinio_lane_spends_no_quota():
    s = s26.intrinio_status()
    assert s["paid_api_called_by_stage26"] is False
    assert s["quota_spent_by_stage26"] == 0
    assert s["current_snapshots_used_as_historical_vintages"] is False
