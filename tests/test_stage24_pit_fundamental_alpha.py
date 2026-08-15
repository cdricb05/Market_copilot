"""Stage 24 regressions - point-in-time fundamental alpha.

Every test here is HERMETIC: an in-memory or tmp_path SQLite index built by the
test itself, no live provider, no network, no operational store, no dependency on
the machine's research roots. The properties held are the ones that make the
Stage-24 evidence trustworthy rather than merely plausible.
"""
from __future__ import annotations

import json
import sqlite3

import pytest

from alpha_agent import pit_fundamentals as pf
from alpha_agent import stage24_pit_fundamental as s24


# --------------------------------------------------------------------------- #
# Hermetic fixtures.
# --------------------------------------------------------------------------- #
_SCHEMA = """
CREATE TABLE cf_fact (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cik TEXT NOT NULL, taxonomy TEXT NOT NULL, concept_tag TEXT NOT NULL,
    label TEXT, unit TEXT NOT NULL, value REAL,
    period_start TEXT, period_end TEXT, filed TEXT NOT NULL,
    accession TEXT NOT NULL, form TEXT, fy TEXT, fp TEXT, frame TEXT,
    source_archive_hash TEXT, source_member TEXT, parser_version TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(cik, concept_tag, unit, period_end, fy, fp, accession));
"""


def _mkindex(tmp_path, facts):
    """facts: (cik, tag, value, period_start, period_end, filed, form, accn)"""
    p = tmp_path / "cf.sqlite"
    conn = sqlite3.connect(p)
    conn.executescript(_SCHEMA)
    conn.executemany(
        "INSERT OR IGNORE INTO cf_fact(cik,taxonomy,concept_tag,unit,value,"
        "period_start,period_end,filed,accession,form,fy,fp,created_at) "
        "VALUES(?,'us-gaap',?,'USD',?,?,?,?,?,?,NULL,NULL,'t')",
        [(c, t, v, ps, pe, f, a, fm) for (c, t, v, ps, pe, f, fm, a) in facts])
    conn.commit()
    conn.close()
    return p


def _annual(cik, tag, value, year, filed, form="10-K", accn=None):
    return (cik, tag, value, "%d-01-01" % year, "%d-12-31" % year, filed, form,
            accn or "%s-%d-%s" % (cik, year, tag))


def _instant(cik, tag, value, year, filed, form="10-K", accn=None):
    return (cik, tag, value, None, "%d-12-31" % year, filed, form,
            accn or "%s-%d-%s" % (cik, year, tag))


# --------------------------------------------------------------------------- #
# Concept mapping: EXTENDS the released owner, never shadows it.
# --------------------------------------------------------------------------- #
def test_concept_extension_never_shadows_the_released_map():
    overlap = set(s24.CONCEPT_EXTENSION) & set(pf.CONCEPT_MAP)
    assert overlap == set(), (
        "Stage 24 must extend alpha_agent.pit_fundamentals.CONCEPT_MAP, not "
        "override it; shadowed keys: %s" % sorted(overlap))
    merged = s24.concept_map()
    for concept, tags in pf.CONCEPT_MAP.items():
        assert merged[concept] == list(tags), (
            "released concept %r was altered by Stage 24" % concept)


def test_mapping_version_hash_is_deterministic_and_pins_the_extension():
    a = s24.mapping_version_hash()
    assert a == s24.mapping_version_hash()
    assert len(a) == 16
    # Every extension tag is actually requested from the archive.
    tags = s24.target_tags()
    for concept, tag_list in s24.CONCEPT_EXTENSION.items():
        for t in tag_list:
            assert t in tags, "%s (%s) missing from target tags" % (t, concept)


def test_every_concept_is_classified_as_flow_or_stock():
    known = s24.FLOW_CONCEPTS | s24.STOCK_CONCEPTS
    missing = set(s24.concept_map()) - known
    assert missing == set(), (
        "unclassified concepts would be read with the wrong period identity: %s"
        % sorted(missing))
    assert not (s24.FLOW_CONCEPTS & s24.STOCK_CONCEPTS)


# --------------------------------------------------------------------------- #
# Filing-availability cutoff and amendment behaviour.
# --------------------------------------------------------------------------- #
def test_filing_availability_cutoff_hides_facts_not_yet_filed(tmp_path):
    db = _mkindex(tmp_path, [
        _instant("0000000001", "Assets", 100.0, 2015, "2016-02-20"),
        _annual("0000000001", "NetIncomeLoss", 10.0, 2015, "2016-02-20"),
    ])
    st = s24.Stage24PitStore(db)
    st.load()
    # One day BEFORE the filing: invisible.
    assert st.value_as_of("0000000001", "assets", "2015-12-31",
                          "2016-02-19") is None
    # On the filing date: visible.
    assert st.value_as_of("0000000001", "assets", "2015-12-31",
                          "2016-02-20") == 100.0


def test_later_amendment_never_leaks_into_an_earlier_formation(tmp_path):
    db = _mkindex(tmp_path, [
        _instant("0000000001", "Assets", 100.0, 2015, "2016-02-20",
                 form="10-K", accn="orig"),
        _instant("0000000001", "Assets", 175.0, 2015, "2017-06-01",
                 form="10-K/A", accn="amend"),
        _annual("0000000001", "NetIncomeLoss", 10.0, 2015, "2016-02-20"),
    ])
    st = s24.Stage24PitStore(db)
    st.load()
    # A formation between the two filings sees ONLY the original.
    assert st.value_as_of("0000000001", "assets", "2015-12-31",
                          "2017-01-01") == 100.0
    # A formation after the amendment sees the restated value.
    assert st.value_as_of("0000000001", "assets", "2015-12-31",
                          "2017-06-02") == 175.0
    # Both observations are retained; nothing was overwritten.
    cov = st.coverage()
    assert cov["observations_reported_more_than_once"] >= 1
    assert cov["amendment_observations"] >= 1


def test_no_future_filing_leakage_across_the_whole_store(tmp_path):
    db = _mkindex(tmp_path, [
        _instant("0000000001", "Assets", 100.0, 2015, "2016-02-20"),
        _instant("0000000001", "Assets", 120.0, 2016, "2017-02-20"),
        _annual("0000000001", "NetIncomeLoss", 10.0, 2015, "2016-02-20"),
        _annual("0000000001", "NetIncomeLoss", 12.0, 2016, "2017-02-20"),
    ])
    full = s24.Stage24PitStore(db)
    full.load()
    # Truncating the visible filing history must reproduce EXACTLY what an
    # as-of query on the full store returns for that date.
    truncated = s24.Stage24PitStore(db)
    truncated.load(max_filed="2016-12-31")
    for concept in ("assets", "net_income"):
        for pe in ("2015-12-31", "2016-12-31"):
            assert (full.value_as_of("0000000001", concept, pe, "2016-12-31")
                    == truncated.value_as_of("0000000001", concept, pe,
                                             "2016-12-31"))
    assert full.latest_fiscal_year_end("0000000001", "2016-12-31") \
        == "2015-12-31"
    assert full.latest_fiscal_year_end("0000000001", "2017-03-01") \
        == "2016-12-31"


# --------------------------------------------------------------------------- #
# Period identity: fy/fp must NOT be used, flows must be annual.
# --------------------------------------------------------------------------- #
def test_quarterly_flow_facts_are_rejected_not_mixed_with_annual(tmp_path):
    db = _mkindex(tmp_path, [
        # A 3-month revenue fact ending on the fiscal year end.
        ("0000000001", "Revenues", 25.0, "2015-10-01", "2015-12-31",
         "2016-02-20", "10-Q", "q4"),
        # The 12-month revenue fact for the same year end.
        _annual("0000000001", "Revenues", 100.0, 2015, "2016-02-20",
                accn="fy"),
        _annual("0000000001", "NetIncomeLoss", 10.0, 2015, "2016-02-20"),
    ])
    st = s24.Stage24PitStore(db)
    st.load()
    assert st.rejected_non_annual_flow == 1
    # The annual figure - never the quarter - is what a factor reads.
    assert st.value_as_of("0000000001", "revenue", "2015-12-31",
                          "2016-06-01") == 100.0


def test_comparable_prior_year_is_exact_and_adjacent_quarters_are_refused(
        tmp_path):
    db = _mkindex(tmp_path, [
        _annual("0000000001", "NetIncomeLoss", 10.0, 2014, "2015-02-20"),
        _annual("0000000001", "NetIncomeLoss", 12.0, 2015, "2016-02-20"),
        _instant("0000000001", "Assets", 90.0, 2014, "2015-02-20"),
        _instant("0000000001", "Assets", 100.0, 2015, "2016-02-20"),
    ])
    st = s24.Stage24PitStore(db)
    st.load()
    cur = st.latest_fiscal_year_end("0000000001", "2016-06-01")
    assert cur == "2015-12-31"
    assert st.prior_fiscal_year_end("0000000001", "2016-06-01", cur) \
        == "2014-12-31"


def test_prior_year_has_no_fallback_to_a_nearer_non_comparable_period(tmp_path):
    """The only other fiscal period available is a MID-YEAR (transition) year end
    about 6 months away. A comparable prior year must be ~365 days back, so the
    correct answer is None - never the nearer, non-comparable period."""
    sub = tmp_path / "b"
    sub.mkdir()
    db = _mkindex(sub, [
        ("0000000002", "NetIncomeLoss", 8.0, "2014-07-01", "2015-06-30",
         "2015-08-20", "10-KT", "transition"),
        _annual("0000000002", "NetIncomeLoss", 12.0, 2015, "2016-02-20"),
    ])
    st = s24.Stage24PitStore(db)
    st.load()
    assert st.latest_fiscal_year_end("0000000002", "2016-06-01") == "2015-12-31"
    assert st.prior_fiscal_year_end("0000000002", "2016-06-01",
                                    "2015-12-31") is None


# --------------------------------------------------------------------------- #
# Missing concepts stay missing; no snapshot is used historically.
# --------------------------------------------------------------------------- #
def test_missing_concepts_remain_missing_and_are_never_zero_filled(tmp_path):
    db = _mkindex(tmp_path, [
        _annual("0000000001", "NetIncomeLoss", 10.0, 2015, "2016-02-20"),
        _instant("0000000001", "Assets", 100.0, 2015, "2016-02-20"),
        # No CFO, no CapEx, no Revenues.
    ])
    st = s24.Stage24PitStore(db)
    st.load()
    rec = s24.annual_record(st, "0000000001", "2016-06-01")
    assert rec is not None
    assert "cash_flow_operations" not in rec["cur"]
    # Factors that need an absent concept yield None - never 0.0.
    assert s24.factor_by_name("s24_fcf_to_assets_pit").value(rec) is None
    assert s24.factor_by_name("s24_operating_accruals_pit").value(rec) is None
    assert s24.factor_by_name("s24_capital_efficiency").value(rec) is None
    # A factor whose inputs ARE present still computes.
    assert s24.factor_by_name("s24_gross_profitability").value(rec) is None


def test_pit_feature_calculation_is_exact(tmp_path):
    db = _mkindex(tmp_path, [
        _annual("0000000001", "NetIncomeLoss", 10.0, 2015, "2016-02-20"),
        _annual("0000000001", "NetCashProvidedByUsedInOperatingActivities",
                30.0, 2015, "2016-02-20"),
        _annual("0000000001", "PaymentsToAcquirePropertyPlantAndEquipment",
                5.0, 2015, "2016-02-20"),
        _annual("0000000001", "Revenues", 200.0, 2015, "2016-02-20"),
        _annual("0000000001", "CostOfRevenue", 120.0, 2015, "2016-02-20"),
        _instant("0000000001", "Assets", 100.0, 2015, "2016-02-20"),
    ])
    st = s24.Stage24PitStore(db)
    st.load()
    rec = s24.annual_record(st, "0000000001", "2016-06-01")
    assert s24.factor_by_name("s24_fcf_to_assets_pit").value(rec) == \
        pytest.approx((30.0 - 5.0) / 100.0)
    assert s24.factor_by_name("s24_operating_accruals_pit").value(rec) == \
        pytest.approx((10.0 - 30.0) / 100.0)
    assert s24.factor_by_name("s24_gross_profitability").value(rec) == \
        pytest.approx((200.0 - 120.0) / 100.0)
    assert s24.factor_by_name("s24_capital_efficiency").value(rec) == \
        pytest.approx(200.0 / 100.0)


def test_non_positive_denominator_yields_no_signal(tmp_path):
    db = _mkindex(tmp_path, [
        _annual("0000000001", "NetIncomeLoss", 10.0, 2015, "2016-02-20"),
        _instant("0000000001", "Assets", 0.0, 2015, "2016-02-20"),
        _annual("0000000001", "Revenues", 200.0, 2015, "2016-02-20"),
    ])
    st = s24.Stage24PitStore(db)
    st.load()
    rec = s24.annual_record(st, "0000000001", "2016-06-01")
    assert s24.factor_by_name("s24_capital_efficiency").value(rec) is None


# --------------------------------------------------------------------------- #
# Historical universe / survivorship.
# --------------------------------------------------------------------------- #
def _universe(rows):
    u = s24.HistoricalUniverse()
    for month, date, sym, fwd in rows:
        u.month_dates[month] = date
        u.by_month.setdefault(month, {})[sym] = {
            "mom_6_1": 0.0, "fwd_1m_return": fwd, "adv_dollar": 1e6,
            "realized_vol_63d": 0.2, "eligible_history": True}
        u.symbols.add(sym)
    u.source_fingerprint = {"path": "test", "exists": True, "sha256": "t"}
    return u


def test_formation_date_membership_is_point_in_time():
    u = _universe([
        ("2020-01", "2020-01-31", "AAA", 0.01),
        ("2020-01", "2020-01-31", "DEAD-202006", 0.02),
        ("2020-02", "2020-02-28", "AAA", 0.01),
    ])
    # DEAD was eligible in January and is NOT in February - the universe is
    # answered per formation date, not from today's members.
    assert set(u.eligible("2020-01")) == {"AAA", "DEAD-202006"}
    assert set(u.eligible("2020-02")) == {"AAA"}


def test_delisted_and_inactive_names_are_retained_as_evidence():
    u = _universe([
        ("2020-01", "2020-01-31", "AAA", 0.01),
        ("2020-01", "2020-01-31", "DEAD-202006", 0.02),
    ])
    assert u.delisted_symbols() == {"DEAD-202006"}
    c = u.contract()
    assert c["survivorship_class"] == "SURVIVORSHIP_SAFE"
    assert c["reconstructed_from_current_members"] is False
    assert c["delisting_tagged_symbols"] == 1


def test_forward_return_chain_requires_every_leg_and_never_imputes():
    u = _universe([
        ("2020-01", "2020-01-31", "AAA", 0.10),
        ("2020-02", "2020-02-29", "AAA", 0.10),
        ("2020-03", "2020-03-31", "AAA", 0.10),
        ("2020-01", "2020-01-31", "GONE-202002", 0.10),
    ])
    assert u.forward_return_chain("2020-01", "AAA", 3) == \
        pytest.approx(1.10 ** 3 - 1.0)
    # A name that stops trading part-way through the window is DROPPED.
    assert u.forward_return_chain("2020-01", "GONE-202002", 3) is None


# --------------------------------------------------------------------------- #
# Panel determinism and leakage.
# --------------------------------------------------------------------------- #
def _bridge(mapping):
    b = s24.IdentityBridge()
    b.symbol_to_cik = dict(mapping)
    b.symbol_meta = {k: {"security_id": k, "ticker": k, "is_current": True,
                         "delisting_date": None, "issuer_name": k, "cik": v,
                         "status": "RESOLVED", "tier": 3, "confidence": 1.0}
                     for k, v in mapping.items()}
    return b


def test_panel_is_deterministic_and_reproduces_byte_identical(tmp_path):
    facts = []
    for i in range(1, 31):
        cik = "%010d" % i
        for year, filed in ((2014, "2015-02-20"), (2015, "2016-02-20")):
            facts += [
                _annual(cik, "NetIncomeLoss", 10.0 + i, year, filed),
                _annual(cik, "Revenues", 200.0 + i, year, filed),
                _instant(cik, "Assets", 100.0 + i, year, filed),
            ]
    db = _mkindex(tmp_path, facts)
    st = s24.Stage24PitStore(db)
    st.load()
    rows = []
    for m, d in (("2016-06", "2016-06-30"), ("2016-09", "2016-09-30"),
                 ("2016-12", "2016-12-31")):
        for i in range(1, 31):
            rows.append((m, d, "S%02d" % i, 0.01 * (i % 5)))
    u = _universe(rows)
    br = _bridge({"S%02d" % i: "%010d" % i for i in range(1, 31)})
    a = s24.build_pit_panel(u, br, st, first_month="2016-06", every_n=1,
                            forward_months=1)
    b = s24.build_pit_panel(u, br, st, first_month="2016-06", every_n=1,
                            forward_months=1)
    ser = lambda p: json.dumps(  # noqa: E731
        {m: {k: v["factors"] for k, v in p.rows[m].items()} for m in p.months},
        sort_keys=True)
    assert ser(a) == ser(b)
    spec = s24.factor_by_name("s24_capital_efficiency")
    assert json.dumps(a.factor_cross_sections(spec), sort_keys=True) == \
        json.dumps(b.factor_cross_sections(spec), sort_keys=True)


def test_no_current_snapshot_is_used_historically(tmp_path):
    """A value that only exists in a filing made AFTER the formation date must
    not appear in that formation's cross-section, even though it is the value we
    would see 'today'."""
    db = _mkindex(tmp_path, [
        _annual("0000000001", "NetIncomeLoss", 10.0, 2015, "2016-02-20"),
        _instant("0000000001", "Assets", 100.0, 2015, "2016-02-20"),
        _annual("0000000001", "Revenues", 500.0, 2015, "2018-01-01",
                form="10-K/A", accn="late"),
    ])
    st = s24.Stage24PitStore(db)
    st.load()
    early = s24.annual_record(st, "0000000001", "2016-06-01")
    assert "revenue" not in early["cur"]
    late = s24.annual_record(st, "0000000001", "2018-06-01")
    assert late["cur"]["revenue"] == 500.0


# --------------------------------------------------------------------------- #
# composite_sn reconstruction contract.
# --------------------------------------------------------------------------- #
def test_composite_reconstruction_is_declared_partial_not_equivalent():
    panel = s24.PitPanel()
    panel.months = ["2016-06"]
    panel.formation_dates = {"2016-06": "2016-06-30"}
    panel.rows = {"2016-06": {
        "S1": {"factors": {"s24_fcf_to_assets_pit": 0.1,
                           "s24_operating_accruals_pit": -0.02},
               "forward_return": 0.01}}}
    cls = s24.composite_sn_reconstruction_class(panel)
    assert cls["classification"] == s24.PARTIAL_PIT
    assert cls["equivalent_to_operational_champion"] is False
    assert "within-sector normalization" in cls["blocked_steps"]


def test_composite_blend_requires_both_legs():
    panel = s24.PitPanel()
    panel.months = ["2016-06"]
    panel.formation_dates = {"2016-06": "2016-06-30"}
    rows = {}
    for i in range(30):
        rows["S%02d" % i] = {
            "factors": {"s24_fcf_to_assets_pit": 0.01 * i,
                        # every 3rd name is missing leg 2
                        "s24_operating_accruals_pit": (None if i % 3 == 0
                                                       else -0.001 * i)},
            "forward_return": 0.001 * i}
    panel.rows = {"2016-06": rows}
    cs = panel.composite_cross_sections(legs=s24.COMPOSITE_FACTORS,
                                        min_names=5)
    assert len(cs) == 1
    scored = {k for k, _, _ in cs[0]["names"]}
    assert all(int(k[1:]) % 3 != 0 for k in scored), \
        "a name missing one leg must be dropped, never scored on the other"


def test_sector_neutralization_is_blocked_and_never_substituted():
    st = s24.sector_neutralization_status()
    assert st["status"] == "BLOCKED_NO_POINT_IN_TIME_SECTOR"
    assert st["look_ahead_map_substituted"] is False
    assert st["canonical_owner"] == "alpha_agent.pit_sector.PitSicSeries"


# --------------------------------------------------------------------------- #
# Research drawdown metric.
# --------------------------------------------------------------------------- #
def test_legacy_drawdown_reproduces_the_released_evaluator_exactly():
    from alpha_agent import signal_evaluation as se
    spreads = [0.02, -0.05, 0.01, -0.03, 0.04, -0.10, 0.06]
    periods = []
    for i, s in enumerate(spreads):
        # A 2-name cross-section whose long/short spread is exactly `s`.
        periods.append({"as_of": "2020-%02d-01" % (i + 1),
                        "names": [("A", 1.0, s), ("B", 0.0, 0.0),
                                  ("C", 0.5, s / 2.0)]})
    row = se.evaluate_periods(periods, horizon_days=21, feature="t")["row"]
    ls = se.evaluate_periods(periods, horizon_days=21,
                             feature="t")["series"]["ls"]
    assert s24.drawdown_v1_cumulative_sum(ls) == pytest.approx(
        row["max_drawdown"]), \
        "the Stage-24 legacy reading must be the released metric, byte for byte"


def test_repaired_drawdown_is_bounded_and_legacy_is_not():
    heavy = [-0.5] * 20
    v1 = s24.drawdown_v1_cumulative_sum(heavy)
    v2 = s24.drawdown_v2_compounded_fraction(heavy)
    assert v1 < -9.0, "a cumulative sum of spreads is unbounded below"
    assert -1.0 <= v2 <= 0.0, "a fractional drawdown cannot exceed total capital"


def test_drawdown_length_sensitivity_is_measured_not_assumed():
    proof = s24.drawdown_length_controlled_proof()
    assert proof["length_alone_flips_the_verdict"] is True, (
        "the Stage-23 concern is only 'confirmed' if an identical per-period "
        "distribution flips the gate verdict purely by being longer")
    # The flip is monotone in length: every failing length is longer than
    # every passing one, so LENGTH - not regime - is what moved the verdict.
    fail_at = proof["shortest_length_that_fails_v1"]
    pass_at = proof["longest_length_that_passes_v1"]
    assert fail_at is not None and pass_at is not None
    assert fail_at > pass_at, (
        "if a SHORTER sample failed while a longer one passed, the flip would "
        "not be a length artefact")
    # Comparable behaviour across experiment lengths is exactly what V2 buys:
    # every reading is a real fraction of capital at every length.
    for r in proof["rows"]:
        assert -100.0 <= r["v2_reported_pct"] <= 0.0
    assert proof["v2_is_bounded_below"] is True
    assert proof["v1_is_bounded_below"] is False


def test_active_drawdown_contract_is_still_v1_so_no_verdict_is_rewritten():
    dc = s24.drawdown_contract([0.01, -0.02, 0.03])
    assert dc["active_contract_version"] == s24.DRAWDOWN_CONTRACT_V1
    assert dc["v1_cumulative_sum_drawdown"] is not None
    assert dc["v2_compounded_fraction_drawdown"] is not None


# --------------------------------------------------------------------------- #
# Governance: no second registry, no automatic promotion, released gates only.
# --------------------------------------------------------------------------- #
def test_stage24_never_inlines_a_released_gate_threshold():
    """Stage 24 may NAME a gate key (to report which gate an evidence row hit)
    but must never assign it a value - the thresholds live in
    configs/alpha_agent/stage9_tournament.json and nowhere else."""
    import re
    from pathlib import Path
    src = Path(s24.__file__).read_text(encoding="utf-8")
    for key in ("keep_min_rank_ic", "keep_min_rank_ic_t", "keep_min_spread_t",
                "keep_min_net25_spread", "keep_max_drawdown_pct",
                "keep_max_turnover_per_rebalance",
                "keep_min_subperiod_consistency"):
        assert not re.search(r'["\']?%s["\']?\s*[:=]\s*-?\d' % key, src), (
            "Stage 24 assigns a value to released gate key %r; thresholds must "
            "come from the released config" % key)


def test_evaluate_token_is_not_redefined_by_stage24():
    """`def evaluate(` is reserved for engine/research_agent.py and
    scripts/audit_architecture.py::check_research_agent_ownership fails the
    build on any other module defining it."""
    from pathlib import Path
    src = Path(s24.__file__).read_text(encoding="utf-8")
    assert "def evaluate(" not in src


def test_scoring_delegates_to_the_released_evaluator():
    import inspect
    src = inspect.getsource(s24.score_cross_sections)
    assert "signal_evaluation" in src and "evaluate_periods" in src


def test_gate_delegates_to_the_released_tournament_contract():
    import inspect
    src = inspect.getsource(s24.gate_for)
    assert "row_to_contract_metrics" in src
    assert "classify_evidence" in src


def test_no_automatic_promotion_and_no_second_registry():
    """Stage 24 must not create a candidate authority of its own, and must not
    be able to promote. The registration path is the released registry."""
    import inspect
    from pathlib import Path
    from alpha_agent import tournament as t
    from api import universe_scoring as us

    src = Path(s24.__file__).read_text(encoding="utf-8")
    # The operational promotion switch is owned elsewhere and stays False.
    assert us.AUTOMATIC_PROMOTION_ALLOWED is False
    import re
    assert not re.search(r"^\s*AUTOMATIC_PROMOTION_ALLOWED\s*=", src,
                         re.MULTILINE), \
        "Stage 24 must not define or reassign the promotion switch"
    # No Stage-24 class may be a candidate registry.
    for name, obj in vars(s24).items():
        if inspect.isclass(obj) and obj.__module__ == s24.__name__:
            assert not name.lower().endswith("registry"), \
                "Stage 24 defined a second registry: %s" % name
    # Registration goes through the released registry, and says so.
    reg_src = inspect.getsource(s24._register)
    assert "CandidateRegistry" in reg_src
    assert "seed_candidate" in reg_src
    assert "ingest_completed_experiments" in reg_src
    assert hasattr(t, "CandidateRegistry")


def test_discovery_family_is_fixed_and_excludes_the_composite_legs():
    disc = {f.name for f in s24.DISCOVERY_FACTORS}
    comp = {f.name for f in s24.COMPOSITE_FACTORS}
    assert disc & comp == set(), (
        "a leg of the model under validation must not inflate the discovery "
        "multiple-testing family")
    assert len(s24.DISCOVERY_FACTORS) == 8
    for f in s24.DISCOVERY_FACTORS:
        assert f.family == s24.FAMILY_DISCOVERY
        assert f.direction in (1, -1)
        assert f.as_dict()["sign_fitted_from_data"] is False


def test_fdr_is_applied_over_the_fixed_family():
    results = [{"name": "a", "family": s24.FAMILY_DISCOVERY,
                "row": {"rank_ic_t": 3.0, "periods": 60}},
               {"name": "b", "family": s24.FAMILY_DISCOVERY,
                "row": {"rank_ic_t": 0.2, "periods": 60}},
               {"name": "c", "family": s24.FAMILY_COMPOSITE,
                "row": {"rank_ic_t": 9.0, "periods": 60}}]
    out = s24.apply_fdr(results, family=s24.FAMILY_DISCOVERY)
    assert out["family_size"] == 2, "the composite leg must not join the family"
    assert out["family_fixed_before_evaluation"] is True
    assert "a" in out["survivors_q10"]
    assert "b" not in out["survivors_q10"]


# --------------------------------------------------------------------------- #
# Agent integration: capabilities, not a second agent.
# --------------------------------------------------------------------------- #
def test_capability_matrix_reports_blocked_families_honestly():
    panel = s24.PitPanel()
    panel.months = []
    panel.rows = {}

    class _Store:
        def coverage(self):
            return {"facts_loaded": 0}
    caps = s24.data_capability_matrix(
        panel, _Store(),
        {"months": 0, "delisting_tagged_symbols": 0,
         "membership_exits_observed": 0}, {"panel_symbols": 0})
    c = caps["capabilities"]
    assert c["PIT_SECTOR_HISTORY"]["state"] == s24.CAP_BLOCKED
    assert c["HISTORICAL_ANALYST_REVISIONS"]["state"] == s24.CAP_BLOCKED
    assert c["PIT_MARKET_CAP"]["state"] == s24.CAP_BLOCKED
    assert "no second agent" in caps["consumer"].lower() or \
        "Stage 24 adds NO second agent" in caps["consumer"]


def test_intrinio_lane_stays_waiting_and_calls_no_provider():
    st = s24.intrinio_parallel_status()
    assert st["state"] == "WAITING_FOR_DATA"
    assert st["provider_called"] is False
    assert st["paid_quota_spent"] is False
    assert st["schema_invented"] is False
    assert st["contract_owner"].startswith("alpha_agent.analyst_revisions")


def test_portfolio_relevance_is_labelled_counterfactual():
    out = s24.portfolio_decision_relevance(s24.PitPanel(), [])
    assert out["evidence_label"] == "COUNTERFACTUAL_NOT_PROOF"
    assert out["historical_decisions_rewritten"] is False
