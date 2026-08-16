"""Stage 25 - autonomous multi-source alpha discovery regressions.

Hermetic: every test builds its own tiny SEC index, identity bridge, issuer index
and momentum panel in a tmp directory. No network, no provider, no live store, no
backend, no PostgreSQL.

The tests are grouped by the property they defend:

  A. concept ownership          - Stage 25 extends, never shadows
  B. point-in-time integrity    - no future filing, no restatement leakage
  C. sector tiering             - Tier A is leakage-safe; Tier B is a control only
  D. disclosure semantics       - MISSING / NOT_REPORTED / ZERO / NOT_APPLICABLE
  E. controls                   - trailing beta is PIT and never defaulted
  F. horizon discipline         - forward windows do not overlap
  G. evaluation transforms      - neutralisation, winner drop, matched universe
  H. governance                 - no auto promotion, no second registry, no
                                  look-ahead in a registered candidate
  I. reproducibility            - identical inputs, identical artefacts
"""
from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

import pytest

from alpha_agent import pit_fundamentals as pf
from alpha_agent import pit_sector as ps
from alpha_agent import stage24_pit_fundamental as s24
from alpha_agent import stage25_alpha_discovery as s25


# --------------------------------------------------------------------------- #
# Fixtures - a miniature but structurally faithful owned-data stack.
# --------------------------------------------------------------------------- #
N_SYMBOLS = 60
N_MONTHS = 48
FIRST_YEAR = 2012


def _months():
    out = []
    y, m = FIRST_YEAR, 1
    for _ in range(N_MONTHS):
        out.append("%04d-%02d" % (y, m))
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return out


def _sym(i: int) -> str:
    # Every fourth name carries a Norgate delisting suffix, so the fixture
    # exercises the survivorship identity rather than a clean current universe.
    return "SYM%02d-201506" % i if i % 4 == 0 else "SYM%02d" % i


def _cik(i: int) -> str:
    return str(100000 + i).zfill(10)


@pytest.fixture()
def owned(tmp_path: Path) -> dict:
    months = _months()

    # -- momentum monthly panel (universe + controls + forward returns) ----- #
    panel_csv = tmp_path / "momentum_monthly_panel.csv"
    with open(panel_csv, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["month", "market_date", "ticker", "mom_6_1",
                    "fwd_1m_return", "is_member", "adv_dollar",
                    "realized_vol_63d", "eligible_history", "sector"])
        for mi, month in enumerate(months):
            market_date = "%s-28" % month
            for i in range(N_SYMBOLS):
                # Deterministic, signal-bearing: names with a higher index get a
                # slightly higher forward return, and the fundamentals below are
                # built to rank the same way, so factors are detectable.
                fwd = 0.004 * ((i % 10) - 4.5) + 0.001 * ((mi % 7) - 3)
                w.writerow([month, market_date, _sym(i),
                            round(0.01 * ((i % 5) - 2), 6), round(fwd, 6), 1,
                            1_000_000 * (i + 1), 0.2 + 0.001 * i, 1, "Unknown"])

    # -- historical identity layer ------------------------------------------ #
    ident = tmp_path / "historical_identity.sqlite"
    con = sqlite3.connect(ident)
    con.executescript(
        "create table securities(norgate_symbol text, security_id text,"
        " ticker text, is_current int, delisting_date text, issuer_name text);"
        "create table cik_map(security_id text, cik text, status text,"
        " tier text, confidence real, active int);")
    for i in range(N_SYMBOLS):
        sid = "SEC%02d" % i
        con.execute("insert into securities values(?,?,?,?,?,?)",
                    (_sym(i), sid, "SYM%02d" % i, 0 if i % 4 == 0 else 1,
                     "2015-06-30" if i % 4 == 0 else None, "Issuer %d" % i))
        con.execute("insert into cik_map values(?,?,?,?,?,?)",
                    (sid, _cik(i), "RESOLVED", "T1", 0.99, 1))
    con.commit()
    con.close()

    # -- SEC companyfacts index --------------------------------------------- #
    cf = tmp_path / "cf.sqlite"
    con = sqlite3.connect(cf)
    con.execute("create table cf_fact(cik text, concept_tag text, value real,"
                " period_start text, period_end text, filed text, form text,"
                " accession text)")

    def fact(cik, tag, value, pe, filed, ps_=None, form="10-K", acc="a1"):
        con.execute("insert into cf_fact values(?,?,?,?,?,?,?,?)",
                    (cik, tag, value, ps_, pe, filed, form, acc))

    for i in range(N_SYMBOLS):
        cik = _cik(i)
        for fy in range(FIRST_YEAR - 2, FIRST_YEAR + 6):
            pe = "%d-12-31" % fy
            start = "%d-01-01" % fy
            filed = "%d-02-15" % (fy + 1)
            assets = 1000.0 + 10 * i
            rev = 600.0 + 12 * i
            cogs = 300.0
            sga = 100.0 - 1.5 * (i % 10)          # ranks with the forward return
            fact(cik, "Assets", assets, pe, filed)
            fact(cik, "Liabilities", 400.0, pe, filed)
            fact(cik, "StockholdersEquity", assets - 400.0, pe, filed)
            fact(cik, "CashAndCashEquivalentsAtCarryingValue", 50.0 + i, pe, filed)
            fact(cik, "AssetsCurrent", 300.0 + i, pe, filed)
            fact(cik, "LiabilitiesCurrent", 150.0, pe, filed)
            fact(cik, "InventoryNet", 80.0, pe, filed)
            fact(cik, "AccountsReceivableNetCurrent", 90.0, pe, filed)
            fact(cik, "LongTermDebtNoncurrent", 200.0, pe, filed)
            fact(cik, "PropertyPlantAndEquipmentNet", 250.0, pe, filed)
            fact(cik, "Revenues", rev, pe, filed, start)
            fact(cik, "CostOfRevenue", cogs, pe, filed, start)
            fact(cik, "NetIncomeLoss", 80.0 + i, pe, filed, start)
            fact(cik, "OperatingIncomeLoss", 90.0 + i, pe, filed, start)
            fact(cik, "NetCashProvidedByUsedInOperatingActivities",
                 100.0 + i, pe, filed, start)
            fact(cik, "PaymentsToAcquirePropertyPlantAndEquipment",
                 30.0, pe, filed, start)
            fact(cik, "SellingGeneralAndAdministrativeExpense", sga, pe, filed,
                 start)
            fact(cik, "NetCashProvidedByUsedInFinancingActivities",
                 -20.0, pe, filed, start)
            # R&D is reported by only half the universe: the disclosure
            # taxonomy needs both reporters and non-reporters to be real.
            if i % 2 == 0:
                fact(cik, "ResearchAndDevelopmentExpense", 20.0 + i, pe, filed,
                     start)
            # A handful of issuers are banks by their OWN disclosure signature.
            if i < 6:
                fact(cik, "Deposits", 900.0, pe, filed)
    con.commit()
    con.close()

    # -- SEC issuer index (entity-level SIC) --------------------------------- #
    iss = tmp_path / "issuer.sqlite"
    con = sqlite3.connect(iss)
    con.execute("create table issuer(cik text, name text, sic text,"
                " sic_description text, first_filing text)")
    for i in range(N_SYMBOLS):
        sic = "6022" if i < 6 else ("3571" if i % 3 == 0 else "2834")
        con.execute("insert into issuer values(?,?,?,?,?)",
                    (_cik(i), "Issuer %d" % i, sic, "d", "2005-01-01"))
    con.commit()
    con.close()

    return {"panel": panel_csv, "identity": ident, "cf": cf, "issuer": iss,
            "root": tmp_path / "research", "months": months}


@pytest.fixture()
def built(owned) -> dict:
    universe = s24.HistoricalUniverse.from_momentum_panel(owned["panel"])
    bridge = s24.IdentityBridge(owned["identity"])
    bridge.load()
    store = s25.Stage25PitStore(owned["cf"])
    assert store.load()["ok"]
    sectors = s25.SectorHistory(owned["issuer"])
    sectors.load_entity_sic(set(bridge.symbol_to_cik.values()))
    beta = s25.TrailingBeta(universe)
    panel = s25.build_panel(universe, bridge, store, sectors, beta,
                            first_month="%d-01" % FIRST_YEAR)
    # ``owned`` first: the built objects must win the key collision on "panel".
    return {**owned, "universe": universe, "bridge": bridge, "store": store,
            "sectors": sectors, "beta": beta, "panel": panel}


# =========================================================================== #
# A. concept ownership
# =========================================================================== #
def test_stage25_never_shadows_a_released_or_stage24_concept():
    released = set(pf.CONCEPT_MAP)
    stage24 = set(s24.CONCEPT_EXTENSION)
    stage25 = set(s25.CONCEPT_EXTENSION_25)
    assert not (stage25 & released), "Stage 25 shadows a Phase-9.3 concept"
    assert not (stage25 & stage24), "Stage 25 shadows a Stage-24 concept"
    merged = s25.concept_map()
    for k, v in pf.CONCEPT_MAP.items():
        assert merged[k] == list(v), "released concept %r was altered" % k
    for k, v in s24.CONCEPT_EXTENSION.items():
        assert merged[k] == list(v), "Stage-24 concept %r was altered" % k


def test_concept_map_raises_rather_than_silently_overriding(monkeypatch):
    monkeypatch.setitem(s25.CONCEPT_EXTENSION_25, "assets", ["Assets"])
    with pytest.raises(ValueError, match="shadow"):
        s25.concept_map()


def test_mapping_version_hash_is_deterministic_and_stage_specific():
    assert s25.mapping_version_hash() == s25.mapping_version_hash()
    assert s25.mapping_version_hash() != s24.mapping_version_hash()


# =========================================================================== #
# B. point-in-time integrity
# =========================================================================== #
def test_period_kind_is_read_from_the_fact_and_agrees_with_stage24(built):
    """The Stage-25 store decides duration-vs-instant from the fact's own
    period_start. On the Stage-24 concepts that must reproduce Stage 24's
    hard-coded partition exactly, or the two stages disagree about what an
    annual figure is."""
    store = built["store"]
    con = sqlite3.connect(built["cf"])
    rows = con.execute("select concept_tag, period_start from cf_fact").fetchall()
    con.close()
    t2c = s25.tag_to_concept()
    checked = 0
    for tag, p_start in rows:
        concept = t2c[tag][0]
        if concept not in (s24.FLOW_CONCEPTS | s24.STOCK_CONCEPTS):
            continue
        intrinsic_is_flow = bool(p_start)
        stage24_is_flow = concept in s24.FLOW_CONCEPTS
        assert intrinsic_is_flow == stage24_is_flow, (
            "period-kind disagreement for %s/%s" % (tag, concept))
        checked += 1
    assert checked > 100
    assert store.loaded_facts > 0


def test_a_later_amendment_is_invisible_to_an_earlier_formation(built, owned):
    """The whole point-in-time contract in one assertion."""
    cik = _cik(7)
    con = sqlite3.connect(owned["cf"])
    con.execute("insert into cf_fact values(?,?,?,?,?,?,?,?)",
                (cik, "Assets", 999999.0, None, "2013-12-31", "2020-06-01",
                 "10-K/A", "amend1"))
    con.commit()
    con.close()
    store = s25.Stage25PitStore(owned["cf"])
    store.load()
    before = store.value_as_of(cik, "assets", "2013-12-31", "2015-01-01")
    after = store.value_as_of(cik, "assets", "2013-12-31", "2021-01-01")
    assert before is not None and before != 999999.0
    assert after == 999999.0


def test_no_fact_filed_after_the_formation_date_enters_the_panel(built):
    """Every factor value in the panel must be derivable from facts filed by the
    formation date minus the reporting lag."""
    panel = built["panel"]
    for m in panel.months:
        as_of = s25._shift_days(panel.formation_dates[m],
                                s25.REPORTING_LAG_DAYS)
        for sym, row in panel.rows[m].items():
            assert row["period_end"] <= as_of
            filed_floor = built["store"].value_as_of(
                row["cik"], "assets", row["period_end"], as_of)
            assert filed_floor is not None


def test_max_filed_truncation_removes_later_information(owned):
    full = s25.Stage25PitStore(owned["cf"])
    full.load()
    trunc = s25.Stage25PitStore(owned["cf"])
    trunc.load(max_filed="2014-01-01")
    assert trunc.loaded_facts < full.loaded_facts


# =========================================================================== #
# C. sector tiering
# =========================================================================== #
def test_tier_a_is_leakage_safe(built):
    """A marker concept first filed in the future cannot classify the past."""
    store, sectors = built["store"], built["sectors"]
    cik = _cik(1)                      # a bank in the fixture
    assert sectors.tier_a(store, cik, "2020-01-01") == "Banking"
    # Before the issuer had filed anything at all it is Unknown, never Banking.
    assert sectors.tier_a(store, cik, "2009-01-01") == s25.TIER_A_UNKNOWN


def test_tier_a_never_reads_the_rnd_concept():
    """Tier A must not be circular with the hypothesis it is used to test."""
    markers = {m for _, mk in s25.TIER_A_RULES for m in mk}
    assert "research_development" not in markers


def test_tier_a_rule_order_is_frozen():
    assert [lab for lab, _ in s25.TIER_A_RULES] == ["Banking", "Insurance",
                                                    "RealEstate"]


def test_tier_b_is_declared_inadmissible_for_signal_construction():
    rule = s25.SectorHistory.tier_b_usage_rule()
    assert rule["leakage_safe"] is False
    for forbidden in ("signal construction", "candidate registration",
                      "challenger evidence", "promotion claim", "shadow book"):
        assert forbidden in rule["inadmissible_for"]


def test_no_sector_tier_enters_a_registered_candidate_signal(built):
    """Every registered signal must be computable from the annual accounting
    record ALONE - a record that carries no classification of any kind.

    This is the property that lets Tier B be used as a falsification control
    without contaminating anything that could ever be promoted."""
    panel = built["panel"]
    for spec in s25.DISCOVERY_FACTORS:
        assert "sector" not in spec.definition.lower()
        assert not any("sector" in c.lower() for c in spec.required)

    # Rebuild every factor from a bare record. If any factor consulted a
    # classification it would have to raise or return None for all rows.
    m = panel.months[0]
    sym = sorted(panel.rows[m])[0]
    row = panel.rows[m][sym]
    as_of = s25._shift_days(panel.formation_dates[m], s25.REPORTING_LAG_DAYS)
    bare = s25.annual_record(built["store"], row["cik"], as_of)
    assert bare is not None
    assert "sectors" not in bare and "sector" not in bare
    recomputed = {f.name: f.value(bare) for f in s25.DISCOVERY_FACTORS}
    assert recomputed == {k: v for k, v in row["factors"].items()
                          if k in recomputed}
    assert any(v is not None for v in recomputed.values())


def test_tier_b_availability_floor_blocks_pre_existence_classification(owned):
    sectors = s25.SectorHistory(owned["issuer"])
    sectors.load_entity_sic([_cik(9)])
    assert sectors.tier_b(_cik(9), "2020-01-01") != ps.UNKNOWN
    assert sectors.tier_b(_cik(9), "2001-01-01") == ps.UNKNOWN


def test_sector_capability_names_the_exact_free_unblocking_artifact():
    cap = s25.sector_capability_statement()
    art = cap["exact_unblocking_artifact"]
    assert "sub.txt" in art["name"]
    assert art["cost"].startswith("free")
    assert art["network_required"] is True
    assert cap["look_ahead_map_substituted_into_a_signal"] is False


# =========================================================================== #
# D. disclosure semantics
# =========================================================================== #
def test_missing_is_never_read_as_zero():
    rec = {"cur": {"assets": 100.0}, "prior": {}}
    assert s25._f_rnd_to_sales(rec) is None
    assert s24._f_rnd_intensity(rec) is None
    assert s25.rnd_availability_state(rec, "OperatingNonFinancial") == \
        s25.RND_NOT_REPORTED


def test_an_explicitly_tagged_zero_is_zero_not_missing():
    rec = {"cur": {"assets": 100.0, "research_development": 0.0}, "prior": {}}
    assert s24._f_rnd_intensity(rec) == 0.0
    assert s25.rnd_availability_state(rec, "OperatingNonFinancial") == s25.RND_ZERO


def test_not_applicable_uses_the_leakage_safe_tier_only():
    rec = {"cur": {"assets": 100.0}, "prior": {}}
    assert s25.rnd_availability_state(rec, "Banking") == s25.RND_NOT_APPLICABLE
    assert s25.rnd_availability_state(rec, "Insurance") == s25.RND_NOT_APPLICABLE
    assert s25.rnd_availability_state(rec, "OperatingNonFinancial") == \
        s25.RND_NOT_REPORTED


def test_generic_disclosure_state_matches_the_rnd_taxonomy():
    assert s25.disclosure_state(frozenset({"sganda"}), "sganda", "X") == \
        s25.RND_REPORTED
    assert s25.disclosure_state(frozenset(), "sganda", "Banking") == \
        s25.RND_NOT_REPORTED       # SG&A IS meaningful for a bank
    assert s25.disclosure_state(frozenset(), "research_development",
                                "Banking") == s25.RND_NOT_APPLICABLE


def test_disclosure_analysis_separates_reporters_from_non_reporters(built):
    d = s25.disclosure_selection_analysis(built["panel"],
                                          concept="research_development")
    counts = d["state_counts"]
    assert counts.get(s25.RND_REPORTED, 0) > 0
    assert counts.get(s25.RND_NOT_REPORTED, 0) > 0
    assert 0.0 < d["reporting_rate_mean"] < 1.0
    assert d["membership_test_is_pre_registered"] is False
    assert d["membership_spread"]["evidence_class"] == \
        "POST_CAMPAIGN_DIAGNOSTIC_NOT_IN_FDR_FAMILY"


# =========================================================================== #
# E. controls
# =========================================================================== #
def test_trailing_beta_uses_only_returns_realised_before_formation(built):
    beta = built["beta"]
    months = beta.months
    # The window ending at month i must be unchanged when every LATER month's
    # realised return is destroyed.
    i = len(months) - 3
    sym = _sym(11)
    before = beta.beta_as_of(months[i], sym)
    for m in months[i + 1:]:
        beta.realized.pop(m, None)
        beta.market.pop(m, None)
    assert beta.beta_as_of(months[i], sym) == before


def test_trailing_beta_is_none_when_underpowered_never_defaulted(built):
    beta = built["beta"]
    assert beta.beta_as_of(beta.months[0], _sym(3)) is None
    assert beta.contract()["missing_policy"].startswith("None")


def test_beta_realised_return_timing_matches_the_panel_convention(built):
    """realised(month m) must equal the panel's fwd_1m_return recorded at m-1."""
    beta, universe = built["beta"], built["universe"]
    months = universe.months()
    m_prev, m_cur = months[5], months[6]
    sym = _sym(13)
    expected = universe.eligible(m_prev)[sym]["fwd_1m_return"]
    assert beta.realized[m_cur][sym] == pytest.approx(expected)


# =========================================================================== #
# F. horizon discipline
# =========================================================================== #
def test_every_horizon_is_formed_without_overlapping_forward_windows():
    for h in s25.HORIZONS:
        stride_months = h["formation_stride"] * s25.FORMATION_EVERY_N_MONTHS
        assert stride_months >= h["forward_months"], (
            "horizon %s overlaps: %d-month windows every %d months"
            % (h["key"], h["forward_months"], stride_months))


def test_six_month_horizon_uses_a_wider_formation_stride(built):
    panel = built["panel"]
    assert len(panel.months_for("h6m")) < len(panel.months_for("h3m"))
    assert panel.months_for("h3m") == panel.months


def test_horizon_exploration_is_declared_part_of_the_testing_family():
    man = s25.hypothesis_manifest()
    assert man["multiple_testing"].startswith("Benjamini-Hochberg")
    assert man["sign_fitted_from_data"] is False
    assert man["brute_force_parameter_search_performed"] is False


# =========================================================================== #
# G. evaluation transforms
# =========================================================================== #
def test_sector_neutralisation_pools_small_groups_instead_of_zeroing_them(built):
    panel = built["panel"]
    spec = s25.factor_by_name("s25_cash_to_assets")
    periods = panel.factor_cross_sections(spec)
    neutral = s25.sector_neutral_cross_sections(periods, panel, tier=s25.TIER_A)
    assert neutral, "neutralisation produced no cross-sections"
    for p in neutral:
        vals = [v for _, v, _ in p["names"]]
        assert any(abs(v) > 1e-12 for v in vals), (
            "a whole cross-section was demeaned to zero")
    # Same names in, same names out.
    assert {k for k, _, _ in neutral[0]["names"]} == \
        {k for k, _, _ in periods[0]["names"]}


def test_drop_top_winners_removes_the_largest_realised_returns():
    periods = [{"as_of": "2020-01-31", "month": "2020-01",
                "names": [("S%02d" % i, float(i), float(i)) for i in range(40)]}]
    out = s25.drop_top_winners(periods, 3)
    kept = {k for k, _, _ in out[0]["names"]}
    assert {"S39", "S38", "S37"}.isdisjoint(kept)
    assert len(kept) == 37


def test_restrict_to_common_scores_the_incumbent_on_the_challenger_names():
    base = [{"as_of": "d1", "names": [("A", 1.0, 0.1), ("B", 2.0, 0.2),
                                      ("C", 3.0, 0.3)] +
             [("X%02d" % i, float(i), 0.01 * i) for i in range(30)]}]
    challenger = [{"as_of": "d1", "names": [("A", 1.0, 0.1)] +
                   [("X%02d" % i, float(i), 0.01 * i) for i in range(30)]}]
    out = s25.restrict_to_common(base, challenger)
    names = {k for k, _, _ in out[0]["names"]}
    assert "B" not in names and "C" not in names and "A" in names


def test_evaluate_variant_reports_underpowered_rather_than_a_number():
    v = s25.evaluate_variant([], feature="x", label="empty")
    assert v["insufficient"] is True
    assert v["periods"] == 0


# =========================================================================== #
# H. governance
# =========================================================================== #
def test_falsification_thresholds_are_pre_registered_constants():
    assert s25.RND_SURVIVE_MIN_T == 2.0
    assert s25.RND_SURVIVE_MIN_RETENTION == 0.50
    assert s25.MIN_PERIODS_FOR_VERDICT == 12


def test_a_control_that_kills_the_signal_produces_a_failure_verdict():
    base = {"periods": 40, "rank_ic": 0.05, "rank_ic_t": 3.0}
    dead = {"variant": "sector_neutral[x]", "periods": 40, "rank_ic": 0.005,
            "rank_ic_t": 0.4, "insufficient": False}
    v = s25._rd_verdict(raw_ic=0.05, base=base, tests=[dead], style={},
                        winners=[], subperiods=[],
                        disclosure={"membership_spread": {"insufficient": True}},
                        single_sector=[], leave_one_out=[])
    assert v["label"] == "SECTOR_EXPLAINED"
    assert v["controls_failed"] == 1


def test_a_signal_surviving_every_control_is_labelled_as_such():
    base = {"periods": 40, "rank_ic": 0.05, "rank_ic_t": 3.0}
    alive = {"variant": "sector_neutral[x]", "periods": 40, "rank_ic": 0.048,
             "rank_ic_t": 2.9, "insufficient": False}
    v = s25._rd_verdict(raw_ic=0.05, base=base, tests=[alive], style={},
                        winners=[], subperiods=[],
                        disclosure={"membership_spread": {"insufficient": True}},
                        single_sector=[], leave_one_out=[])
    assert v["label"] == "SURVIVES_SECTOR_AND_STYLE_CONTROLS"


def test_the_verdict_states_the_look_ahead_asymmetry():
    v = s25._rd_verdict(raw_ic=0.05, base={"periods": 40, "rank_ic": 0.05},
                        tests=[], style={}, winners=[], subperiods=[],
                        disclosure={"membership_spread": {"insufficient": True}},
                        single_sector=[], leave_one_out=[])
    assert "conclusive" in v["asymmetry_disclaimer"]
    assert "provisional" in v["asymmetry_disclaimer"]


def test_underpowered_evidence_yields_no_verdict():
    v = s25._rd_verdict(raw_ic=0.05, base={"periods": 4, "rank_ic": 0.05},
                        tests=[], style={}, winners=[], subperiods=[],
                        disclosure={"membership_spread": {"insufficient": True}},
                        single_sector=[], leave_one_out=[])
    assert v["label"] == "UNDERPOWERED_NO_VERDICT"


def test_a_gate_failing_candidate_can_never_be_classified_as_alpha():
    out = s25.classify_candidate(
        result={"gate": {"target_state": "REJECTED"},
                "row": {"periods": 40}},
        incr={"vs": {s25.BASELINE_COMPOSITE: {"partial_rank_ic_t": 9.0},
                     s25.BASELINE_MOMENTUM: {"partial_rank_ic_t": 9.0}}},
        neutral={})
    assert out["classification"] == s25.CLS_FAILED_ROBUST


def test_a_baseline_restatement_is_classified_redundant():
    out = s25.classify_candidate(
        result={"gate": {"target_state": "KEEP_FOR_RESEARCH"},
                "row": {"periods": 40}},
        incr={"vs": {s25.BASELINE_COMPOSITE: {
            "mean_cross_sectional_rank_correlation": 0.95,
            "partial_rank_ic_t": 0.2}}},
        neutral={})
    assert out["classification"] == s25.CLS_REDUNDANT_COMPOSITE


def test_ensemble_weights_are_never_fitted_and_the_menu_is_capped():
    comp = [{"as_of": "d1", "names": [("A", 1.0, 0.1), ("B", 2.0, 0.2)]}]
    mom = [{"as_of": "d1", "names": [("A", 2.0, 0.1), ("B", 1.0, 0.2)]}]
    picks = [("c1", comp), ("c2", mom)]
    refs = [("r1", comp)]
    menu = s25.ensemble_menu(comp=comp, mom=mom, picks=picks, references=refs)
    assert len(menu) <= s25.MAX_ENSEMBLE_STRUCTURES
    for item in menu:
        assert abs(sum(item["weights"]) - 1.0) < 1e-9
        assert len(item["weights"]) == len(item["parts"])


def test_a_reference_ensemble_can_never_be_reported_as_a_challenger():
    ens = {"structures": [
        {"name": "operational_shape_5050", "insufficient": False,
         "gate": {"target_state": "REJECTED"}},
        {"name": "reference_x", "insufficient": False, "reference_only": True,
         "rank_ic_t": 99.0, "gate": {"target_state": "KEEP_FOR_RESEARCH"},
         "delta_vs_operational_shape_matched_universe": {"rank_ic_t": 9.0,
                                                         "net25": 9.0}}]}
    out = s25.challenger_assessment([], {"survivors_q10": []}, ens, {},
                                    {"verdict": {"label": "X"}}, None)
    assert out["best_ensemble_beating_operational_shape"] is None


def test_registration_never_promotes_and_never_creates_a_second_registry(
        built, tmp_path):
    from alpha_agent import tournament as t
    cfg = t.load_config(Path(__file__).resolve().parents[1]
                        / "configs" / "alpha_agent" / "stage9_tournament.json")
    db = tmp_path / "tournament.sqlite"
    row = {"feature": "s25_cash_to_assets", "periods": 30, "universe": 50,
           "rank_ic_mean": 0.01, "rank_ic_t": 0.5, "spread_t": 0.4,
           "turnover": 0.1, "net_annualized_return": 0.01,
           "gross_annualized_return": 0.02, "max_drawdown": -0.1,
           "subperiod_consistency": 0.5, "regime_consistency": 0.5,
           "rank_ic_positive_ratio": 0.5}
    res = s25.register_candidates(
        [{"name": "s25_cash_to_assets", "family": s25.FAM_BALANCE,
          "spec": {"expected_sign": 1}, "row": row}], cfg, db_path=str(db))
    assert res["ok"] is True
    assert res["automatic_promotion"] is False
    assert res["second_registry_created"] is False
    assert res["champion_changed"] is False
    assert res["shadow_book_created"] is False
    con = sqlite3.connect(db)
    assert con.execute("select count(*) from shadow_books").fetchone()[0] == 0
    spec = json.loads(con.execute(
        "select spec_json from candidates limit 1").fetchone()[0])
    con.close()
    assert spec["sector_tier_used_in_construction"].startswith("NONE")


def test_forward_tracking_starts_nothing():
    out = s25.forward_tracking_status({"research_challengers": ["x"]},
                                      {"verdict": {"label": "Y"}})
    assert out["shadow_book_created_by_stage25"] is False
    assert out["true_forward_evidence_written_by_stage25"] is False


def test_intrinio_stays_blocked_when_no_historical_extract_exists():
    st = s25.intrinio_status()
    assert st["paid_api_called"] is False
    assert st["quota_spent"] is False
    assert st["provider_schema_invented"] is False
    assert st["no_intrinio_only_framework_created"] is True
    blob = json.dumps(st).upper()
    assert "WAITING" in blob or "BLOCKED" in blob or "ABSENT" in blob


def test_purchase_gate_authorises_nothing_and_prefers_the_free_artifact():
    cap = {"data_families": {"PIT_SECTOR_HISTORY": {
        "evidence": s25.sector_capability_statement()}}}
    gate = s25.external_data_purchase_gate(
        capability=cap,
        exhaustion={"families": {"f": {"state": s25.EX_ACTIVE_HIGH}},
                    "do_not_reopen": []},
        fdr={"survivors_q10": []}, rd={"verdict": {"label": "X"}},
        challengers={"research_challengers": []})
    assert gate["purchase_authorized"] is False
    assert gate["datasets"][0]["cost"] == "free"
    assert gate["datasets"][1]["recommendation"] in ("WAIT", "REJECT")
    assert gate["datasets"][2]["recommendation"] == "REJECT"


def test_hoc_counterfactual_is_labelled_and_never_rewrites_history(built):
    panel = built["panel"]
    spec = s25.factor_by_name("s25_cash_to_assets")
    periods = panel.factor_cross_sections(spec)
    out = s25.hoc_counterfactual(panel, candidates={"c": periods},
                                 baseline_periods=periods)
    assert out["historical_decisions_rewritten"] is False
    assert out["true_forward_evidence_touched"] is False
    for e in out["panel_counterfactual"]:
        assert e["evidence_class"] == "COUNTERFACTUAL_NOT_PROOF"
    assert out["real_decision_status"] in ("MEASURABLE",
                                           "INSUFFICIENT_FORWARD_EVIDENCE")


def test_exhaustion_lists_the_prior_negative_results_as_do_not_reopen():
    ex = s25.alpha_family_exhaustion([], {"survivors_q10": []},
                                     {"label": "SECTOR_EXPLAINED"})
    assert "residual_momentum" in ex["do_not_reopen"]
    assert "fundamental_momentum_cfo_change" in ex["do_not_reopen"]
    assert ex["families"]["historical_analyst_revisions"]["state"] == \
        s25.EX_WAITING


def test_research_queue_creates_no_second_agent_or_queue():
    q = s25.research_queue(
        capability={"data_families": {}}, exhaustion={"do_not_reopen": []},
        fdr={"survivors_q10": []}, rd={}, challengers={},
        purchase={}, multi={})
    assert q["second_queue_created"] is False
    assert q["second_agent_created"] is False
    assert q["agent_may_promote_models"] is False
    assert q["agent_may_change_holdings"] is False


def test_gate_integrity_neither_weakens_a_threshold_nor_rewrites_evidence(built):
    rep = s25.research_gate_integrity([], {}, built["panel"], {})
    assert rep["thresholds_weakened_to_make_candidates_pass"] is False
    assert rep["historical_evidence_rewritten"] is False
    assert rep["drawdown"]["stage25_changed_the_active_version"] is False
    assert rep["drawdown"]["active_contract_version"] == s24.DRAWDOWN_CONTRACT_V1
    assert rep["duplicated_formation_observations"]["defect_found"] is False


# =========================================================================== #
# I. end-to-end determinism and survivorship
# =========================================================================== #
def test_the_universe_keeps_delisted_names(built):
    dead = built["universe"].delisted_symbols()
    assert dead, "the fixture universe has no delisted names"
    assert built["universe"].contract()["survivorship_class"] == \
        "SURVIVORSHIP_SAFE"
    scored = {s for m in built["panel"].months for s in built["panel"].rows[m]}
    assert scored & dead, "no delisted name reached a cross-section"


def test_run_is_reproducible_and_writes_every_required_artifact(owned):
    kwargs = dict(research_root=owned["root"], mom_panel=owned["panel"],
                  identity_db=owned["identity"], cf_index=owned["cf"],
                  issuer_db=owned["issuer"], register=False)
    a = s25.run(**kwargs)
    assert a["ok"] is True and a["token"] == s25.READY
    required = {
        "research_capability_map", "pit_sector_history_summary",
        "rd_falsification", "pit_fundamental_expansion", "hypothesis_manifest",
        "experiment_results", "alpha_family_exhaustion",
        "orthogonality_matrix", "incremental_alpha_matrix", "ensemble_results",
        "challenger_results", "forward_tracking_status",
        "hoc_counterfactual_results", "autonomous_research_queue",
        "intrinio_status", "external_data_purchase_gate",
        "research_gate_integrity", "stage25_summary"}
    written = {x["artifact"] for x in a["artifacts"]}
    assert required <= written, "missing artefacts: %s" % (required - written)

    b = s25.run(**kwargs)
    assert b["run_id"] == a["run_id"], "identical inputs changed the run id"
    assert {x["artifact"]: x["sha256"] for x in b["artifacts"]} == \
        {x["artifact"]: x["sha256"] for x in a["artifacts"]}


def test_run_reports_no_promotion_and_no_portfolio_mutation(owned):
    res = s25.run(research_root=owned["root"], mom_panel=owned["panel"],
                  identity_db=owned["identity"], cf_index=owned["cf"],
                  issuer_db=owned["issuer"], register=False)
    s = res["summary"]
    assert s["automatic_promotion"] is False
    assert s["portfolio_mutation"] is False
    assert s["orders_created"] == 0
    assert s["operational_model_unchanged"] == "fundamental_momentum_50_50_v1"
    ch = res["payload"]["challenger_results"]
    assert ch["second_tournament_created"] is False
    assert ch["second_champion_authority_created"] is False
    assert ch["automatic_promotion_possible"] is False


def test_fdr_is_applied_over_the_whole_family_and_q_is_monotone_in_p(owned):
    res = s25.run(research_root=owned["root"], mom_panel=owned["panel"],
                  identity_db=owned["identity"], cf_index=owned["cf"],
                  issuer_db=owned["issuer"], register=False)
    fdr = res["payload"]["experiment_results"]["multiple_testing"]
    assert fdr["family_size"] == len(s25.DISCOVERY_FACTORS)
    assert fdr["family_fixed_before_evaluation"] is True
    members = [m for m in fdr["members"] if m["pvalue"] is not None]
    ordered = sorted(members, key=lambda m: m["pvalue"])
    qs = [m["bh_q"] for m in ordered]
    assert all(qs[i] <= qs[i + 1] + 1e-12 for i in range(len(qs) - 1)), \
        "BH q-values are not monotone in p"


def test_every_hypothesis_declares_its_sign_before_evaluation():
    for spec in s25.DISCOVERY_FACTORS:
        d = spec.as_dict()
        assert d["expected_sign"] in (1, -1)
        assert d["sign_fitted_from_data"] is False
        assert d["economic_hypothesis"] and d["economic_rationale"]
        assert d["factor_definition"] and d["required_concepts"]


def test_discovery_family_size_is_inside_the_declared_research_budget():
    assert 20 <= len(s25.DISCOVERY_FACTORS) <= 40
    assert s25.MAX_ENSEMBLE_STRUCTURES <= 15
