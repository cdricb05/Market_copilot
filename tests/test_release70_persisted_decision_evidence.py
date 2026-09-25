"""Release 70 — does the decision evidence SURVIVE the real persistence path?

The R70 candidate-generation suite proves the kernels compute the right thing. It does
not prove any of it is ever recorded, because it hands ``build_observation`` a ``rec``
dict it built itself. Production does not: the only producer of that argument is

    engine.portfolio_reassessment.build_reassessment   -> holding_assessments
    api.portfolio_reassessment._history_row            -> row["recommendations"]
    api.reassessment_outcomes._default_history_loader  -> build_observations
    api.reassessment_outcomes.capture_matured_outcomes -> outcome_observations.json

and the projection in the middle was LOSSY. Measured before this suite existed, every
field R70 added was dropped there: ``replacement_shortlist`` arrived as ``[]``,
``proposed_exposure_reduction`` as ``None`` (so action-sized economics silently fell
back to the full position on every single row), and the candidate score, both sectors,
the decision mark and the switching costs as ``None``. The values were computed,
published on the immutable artifact, and thrown away one function before the store. A
test that inspects a hand-built ``rec`` can never see that; only a round trip can.

So these tests drive the REAL chain and then read the REAL file back off disk.

Bounded and hermetic: pure kernels plus the genuine file writer, pointed at ``tmp_path``.
No DB, provider, prediction, network, backend, operational ledger, live store or real
cycle is touched, and nothing here can write outside the temporary directory.
"""
from __future__ import annotations

import json

from paper_trader.engine import holding_opportunity_cost as HOC
from paper_trader.engine import portfolio_reassessment as PRK
from paper_trader.engine import reassessment_outcomes as ROK
from paper_trader.api import portfolio_reassessment as PRS
from paper_trader.api import reassessment_outcomes as RO

BOOK = "alpha_paper_book_1"
DATE = "2026-08-05"
HORIZONS = [5, 20]

#: Six Tech holdings at 4% each (Tech = 24% of NAV, just inside the 25% sector cap) and
#: one Energy holding at 4%. Each NAME stays well under both the 10% name cap and the
#: per-name risk-contribution limit, so every holding earns a clean REPLACE and the only
#: thing separating the two classes is the holding-relative sector rule under test.
TECH_HELD = ["AAA", "AA2", "AA3", "AA4", "AA5", "AA6"]
ENERGY_HELD = ["BBB"]
INCUMBENTS = TECH_HELD + ENERGY_HELD
#: The two first-choice candidates the sector geometry forces apart.
TECH_PICK = "TEC1"
ENERGY_PICK = "ENE1"


# --------------------------------------------------------------------------- #
# Deterministic builders
# --------------------------------------------------------------------------- #
def _rets(n, seed=1):
    """A deterministic, DECORRELATED return series per seed.

    An arithmetic generator gives several holdings the same series up to a shift, which
    concentrates measured risk contribution and pushes otherwise-clean holdings into a
    RISK_CONTRIBUTION_BREACH — a REDUCE, not the REPLACE these tests are about. A mixing
    step keeps every series distinct while staying fully reproducible.
    """
    out, x = [], (seed * 2654435761) & 0xFFFFFFFF
    for _ in range(n):
        x = (x * 1103515245 + 12345) & 0xFFFFFFFF
        out.append((((x >> 16) % 21) - 10) / 1000.0)
    return out


def _trailing(seed=1, n=130):
    rets = _rets(n, seed)
    adj = [100.0]
    for r in rets:
        adj.append(adj[-1] * (1.0 + r))
    return {"dates": ["d%03d" % i for i in range(len(adj))], "adj": adj,
            "ret": [None] + rets}


def _urow(ticker, rank, pct, sector="Tech", adv=2e7, eligible=True):
    return {"ticker": ticker, "rank": rank, "combined_score": pct, "percentile": pct,
            "fundamental_score": pct, "fundamental_percentile": pct,
            "momentum_score": pct, "momentum_percentile": pct,
            "sector": sector, "adv_dollar": adv, "eligible": eligible}


def _pos(ticker, sector, weight, mv):
    return {"ticker": ticker, "sector": sector, "quantity": 100,
            "current_weight": weight, "market_value": mv}


def _hoc_input():
    """Six Tech holdings + one Energy holding, and a four-sector alternative pool.

    The Tech sector sits at 0.24 of NAV against a 0.25 sector cap, so swapping the
    ENERGY name (weight 0.04) into the top-ranked TECH name would post 0.28 and is
    refused, while swapping a TECH name into it is weight-neutral and allowed. The two
    holding classes are therefore FORCED to different first choices — the one thing the
    pre-R70 kernel structurally could not express.
    """
    universe = [_urow(t, 20 + i, 0.50 - i * 0.01, "Tech")
                for i, t in enumerate(TECH_HELD)]
    universe += [
        _urow("BBB", 30, 0.45, "Energy"),
        _urow("TEC1", 1, 0.99, "Tech"), _urow("ENE1", 2, 0.98, "Energy"),
        _urow("HLT1", 3, 0.97, "Health"), _urow("UTL1", 4, 0.96, "Utilities"),
        _urow("TEC2", 5, 0.95, "Tech"), _urow("ENE2", 6, 0.94, "Energy"),
        _urow("HLT2", 7, 0.93, "Health"), _urow("UTL2", 8, 0.92, "Utilities"),
        _urow("TEC3", 9, 0.91, "Tech"), _urow("ENE3", 10, 0.90, "Energy"),
        _urow("HLT3", 11, 0.89, "Health"), _urow("UTL3", 12, 0.88, "Utilities"),
    ]
    return {
        "schema_version": HOC.INPUT_SCHEMA_VERSION,
        "eligible_market_date": DATE, "valuation_date": DATE,
        "active_book_id": BOOK, "active_book_label": "Alpha Paper Book #1",
        "portfolio_state_hash": "PSHASH", "universe_scoring_hash": "USHASH",
        "universe_input_contract_hash": "USIN",
        "nav": 100000.0, "cash": 8000.0,
        "positions": ([_pos(t, "Tech", 0.04, 4000.0) for t in TECH_HELD]
                      + [_pos("BBB", "Energy", 0.04, 4000.0)]),
        "universe_rows": universe,
        "previous_ranking": {**{t: 19 + i for i, t in enumerate(TECH_HELD)},
                             "BBB": 29},
        "previous_ranking_state": "AVAILABLE",
        "trailing_prices": {t: _trailing(i + 1) for i, t in enumerate(INCUMBENTS)},
        "median_dollar_volume": {t: 5e7 for t in INCUMBENTS},
        "aligned_returns": {"dates": ["d%03d" % i for i in range(60)],
                            "series": {t: _rets(60, i + 1)
                                       for i, t in enumerate(INCUMBENTS)}},
    }


def _reassessment(*, churn_on=("AAA",)):
    """The REAL chain: opportunity cost -> input contract -> reassessment.

    ``churn_on`` names holdings whose recent change history makes the churn control
    bind, so the withheld branch is produced by the genuine governance code rather than
    asserted into existence.
    """
    hoc = HOC.build_assessment(input_contract=_hoc_input())
    assert hoc["assessment_state"] == "READY", hoc.get("blockers")
    hoc = {**hoc, "provenance": {**(hoc.get("provenance") or {}),
                                 "economic_state_hash": "econ_1",
                                 "corporate_actions_hash": "ca_1"}}
    ps = {"dates": {"eligible_market_date": DATE, "valuation_date": DATE},
          "active_book": {"book_id": BOOK, "book_label": "Alpha Paper Book #1"},
          "capital": {"nav": 100000.0, "cash": 8000.0},
          "state_hash": "doc_1", "economic_state_hash": "econ_1",
          "corporate_actions": {"registry_fingerprint": "ca_1", "actions": []}}
    stale = PRS._default_corporate_action_staleness(
        hoc_assessment=hoc, portfolio_state=ps, active_book_id=BOOK)
    recent = [{"eligible_market_date": DATE, "ticker": t, "direction": "IN",
               "source": "rebalance"} for t in churn_on]
    ic = PRS.build_input_contract(
        portfolio_state=ps, scoring={"output_hash": "s1"}, hoc_assessment=hoc,
        corporate_action_stale=stale, recent_change_history=recent)
    return PRK.build_reassessment(input_contract=ic)


def _history_row(res, *, rid="prs_2026-08-05_alpha_paper_book_1_r70"):
    """The row the PRODUCTION projection builds. Never hand-written."""
    return PRS._history_row(artifact={"reassessment_id": rid,
                                      "generated_at": "2026-08-05T20:00:00+00:00",
                                      "reassessment": res})


def _calendar(n=40, start=DATE):
    from datetime import date, timedelta
    d, out = date.fromisoformat(start), []
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _series(cal, **moves):
    """100 on the decision date, 100*(1+move) at every later close: a step, so the
    realized return at ANY matured horizon is exactly ``move``."""
    return {tk: [[d, 100.0 if i == 0 else round(100.0 * (1.0 + mv), 6)]
                 for i, d in enumerate(cal)]
            for tk, mv in moves.items()}


def _default_moves():
    """Every incumbent falls, both candidates rise, by DIFFERENT amounts so no two rows
    share a realized spread by accident."""
    moves = {t: -0.10 + i * 0.01 for i, t in enumerate(INCUMBENTS)}
    moves[TECH_PICK] = 0.20
    moves[ENERGY_PICK] = 0.15
    return moves


def _evidence(cal=None, **moves):
    cal = cal or _calendar()
    return {"series": _series(cal, **(moves or _default_moves())), "calendar": cal,
            "horizons": HORIZONS, "evidence_fingerprint": "ev_fp_r70"}


def _capture(tmp_path, *, res=None, evidence=None, rid=None):
    res = res if res is not None else _reassessment()
    row = _history_row(res, **({"rid": rid} if rid else {}))
    return RO.capture_matured_outcomes(
        history=[row], evidence=evidence or _evidence(),
        lineage={"latest_completed_rebalance": {}},
        outcome_dir=tmp_path, active_book_id=BOOK)


def _persisted(tmp_path):
    """Read the observations back OFF DISK, not from the capture return value."""
    return json.loads((tmp_path / "outcome_observations.json")
                      .read_text(encoding="utf-8"))


def _one(rows, ticker, horizon=20):
    return next(r for r in rows if r["ticker"] == ticker
                and r["horizon_eligible_closes"] == horizon)


# =========================================================================== #
# 1. The seam itself. This is the regression for the defect above.
# =========================================================================== #
def test_01_the_history_projection_carries_every_r70_decision_field():
    """The production projection must not drop what the kernel computed.

    Before the R70 seam repair, ``_history_row`` listed twelve fields and none of the
    R70 additions, so the evidence died one function before the store.
    """
    res = _reassessment()
    row = _history_row(res)
    by_ticker = {a["ticker"]: a for a in res["holding_assessments"]}
    assert set(r["ticker"] for r in row["recommendations"]) == set(INCUMBENTS)
    for rec in row["recommendations"]:
        src = by_ticker[rec["ticker"]]
        for field in PRS._R70_DECISION_EVIDENCE_FIELDS:
            assert field in rec, "%s dropped for %s" % (field, rec["ticker"])
            assert rec[field] == src[field], \
                "%s altered in projection for %s" % (field, rec["ticker"])
    # And the shortlist specifically arrives whole, not shortened or emptied.
    for rec in row["recommendations"]:
        assert rec["replacement_shortlist"], rec["ticker"]
        assert len(rec["replacement_shortlist"]) == rec["replacement_shortlist_size"]


def test_02_the_governance_verbs_both_survive_the_projection():
    res = _reassessment(churn_on=("AAA",))
    recs = {r["ticker"]: r for r in _history_row(res)["recommendations"]}
    assert recs["AAA"]["source_recommendation"] == PRK.REC_REPLACE
    assert recs["AAA"]["recommendation"] == PRK.REC_HOLD
    assert recs["AAA"]["action_withheld"] is True
    assert recs["AA2"]["source_recommendation"] == PRK.REC_REPLACE
    assert recs["AA2"]["action_withheld"] is False


# =========================================================================== #
# 2. A blocked REPLACE, all the way to disk and back.
# =========================================================================== #
def test_03_a_blocked_replace_is_persisted_as_a_blocked_replace(tmp_path):
    """The operator-facing claim: what was PROPOSED and what was PERMITTED are both
    on the persisted row, and the blocked action is never laundered into a HOLD."""
    out = _capture(tmp_path)
    assert out["performed_write"] is True and out["observations_newly_matured"] > 0
    rows = _persisted(tmp_path)
    aaa = _one(rows, "AAA")
    assert aaa["source_recommendation"] == PRK.REC_REPLACE
    assert aaa["recommendation"] == PRK.REC_HOLD
    assert aaa["action_withheld"] is True
    assert "CHURN_COOLDOWN_ACTIVE" in aaa["withheld_reason_codes"]
    # A genuine, unblocked REPLACE is stored as one.
    aa2 = _one(rows, "AA2")
    assert aa2["source_recommendation"] == aa2["recommendation"] == PRK.REC_REPLACE
    assert aa2["action_withheld"] is False


def test_04_the_blocked_replace_reaches_the_replacement_bucket_from_disk(tmp_path):
    """The whole point of bucketing on the proposed action: a withheld REPLACE must be
    counted as a REPLACE that was blocked, read straight out of the store."""
    _capture(tmp_path)
    rows = RO.load_observations(outcome_dir=tmp_path, active_book_id=BOOK)
    card = ROK.build_scorecard(rows)
    assert card["by_proposed_action"].get(PRK.REC_REPLACE, 0) >= 3
    assert card["withheld_by_proposed_action"].get(PRK.REC_REPLACE, 0) >= 1
    # The permitted-action view still records the HOLD, so the governance effect is
    # the visible difference between the two counts.
    assert card["by_recommendation"].get(PRK.REC_HOLD, 0) >= 1
    assert card["withheld_replacement_outcomes"]["observations"] >= 1


# =========================================================================== #
# 3. Every frozen field survives save/read UNCHANGED.
# =========================================================================== #
def test_05_decision_evidence_survives_save_and_read_unchanged(tmp_path):
    res = _reassessment()
    _capture(tmp_path, res=res)
    rows = _persisted(tmp_path)
    src = {a["ticker"]: a for a in res["holding_assessments"]}
    for tk in INCUMBENTS:
        obs = _one(rows, tk)
        a = src[tk]
        # candidate identity + score + sector
        assert obs["replacement_ticker"] == a["strongest_replacement_ticker"]
        assert obs["replacement_score_at_decision"] == a["replacement_score"]
        assert obs["replacement_sector_at_decision"] == a["replacement_sector"]
        # the incumbent it was compared against
        assert obs["incumbent_score_at_decision"] == a["signal_score"]
        assert obs["incumbent_sector_at_decision"] == a["sector"]
        # the decision mark
        assert obs["incumbent_mark_at_decision"] == a["market_value"]
        assert obs["incumbent_mark_at_decision"] is not None
        # switching cost
        assert obs["switching_cost_bps_at_decision"] == a["switching_cost_bps"]
        assert obs["switching_cost_usd_at_decision"] == a["switching_cost_usd"]
        assert obs["switching_cost_usd_at_decision"] is not None
        # intended exposure reduction, at the ORIGINAL action's size
        assert obs["proposed_exposure_reduction_at_decision"] == \
            a["proposed_exposure_reduction"]
        assert obs["proposed_exposure_reduction_basis"] == \
            a["proposed_exposure_reduction_basis"]
        # decision date + maturity horizon
        assert obs["decision_date"] == DATE
        assert obs["eligible_market_date"] == DATE
        assert obs["horizon_eligible_closes"] == 20
        assert obs["maturity_market_date"] is not None
        assert "Frozen at the decision" in obs["evidence_immutability"]


def test_06_the_whole_shortlist_survives_verbatim(tmp_path):
    """Not the winner — the full set of alternatives actually considered. A later reader
    cannot re-judge the decision if it only has the name that won."""
    res = _reassessment()
    _capture(tmp_path, res=res)
    rows = _persisted(tmp_path)
    src = {a["ticker"]: a for a in res["holding_assessments"]}
    for tk in INCUMBENTS:
        obs = _one(rows, tk)
        expected = src[tk]["replacement_shortlist"]
        assert obs["replacement_shortlist_at_decision"] == expected
        assert obs["shortlist_size_at_decision"] == len(expected) >= 2
        # It survived a JSON round trip, so it is genuinely on disk and not an
        # in-memory alias of the assessment object.
        assert obs["replacement_shortlist_at_decision"][0]["ticker"] == \
            obs["replacement_ticker"]
        for cand in obs["replacement_shortlist_at_decision"]:
            assert cand["label"] == HOC.NON_ALLOCATED_LABEL


def test_07_the_action_is_sized_by_the_proposed_quantity_on_disk(tmp_path):
    """A REDUCE moves part of the position and a withheld REPLACE released nothing;
    both must be scored at the size the DECISION specified, never the whole holding."""
    _capture(tmp_path)
    rows = _persisted(tmp_path)
    for tk in INCUMBENTS:
        obs = _one(rows, tk)
        assert obs["portfolio_impact_sizing_basis"] == "PROPOSED_QUANTITY", tk
        assert obs["portfolio_impact_sizing_weight"] == \
            obs["proposed_exposure_reduction_at_decision"]
    # The withheld REPLACE proposed a FULL exit even though governance released 0.
    aaa = _one(rows, "AAA")
    assert aaa["proposed_exposure_reduction_basis"] == "FULL_POSITION"
    assert aaa["proposed_exposure_reduction_at_decision"] == 0.04


# =========================================================================== #
# 4. Different holdings, different candidates — persisted.
# =========================================================================== #
def test_08_different_holdings_persist_different_candidates(tmp_path):
    """The pre-R70 store held 8,825 rows carrying ONE distinct candidate. The store is
    only cured if DIFFERENT names actually reach disk."""
    _capture(tmp_path)
    rows = _persisted(tmp_path)
    assert _one(rows, "AAA")["replacement_ticker"] == TECH_PICK
    assert _one(rows, "BBB")["replacement_ticker"] == ENERGY_PICK
    distinct = {r["replacement_ticker"] for r in rows if r.get("replacement_ticker")}
    assert len(distinct) >= 2, distinct
    # and the shortlists differ, not just the winners
    assert ([c["ticker"] for c in _one(rows, "AAA")["replacement_shortlist_at_decision"]]
            != [c["ticker"] for c in
                _one(rows, "BBB")["replacement_shortlist_at_decision"]])


def test_09_the_holding_relative_sector_rule_is_what_separated_them(tmp_path):
    """Names the MECHANISM, so a future refactor that re-globalises the sector test
    fails here rather than silently collapsing the candidate set again."""
    res = _reassessment()
    src = {a["ticker"]: a for a in res["holding_assessments"]}
    # Tech incumbent: the top-ranked Tech name is a weight-neutral same-sector swap.
    assert src["AAA"]["strongest_replacement_ticker"] == TECH_PICK
    assert src["AAA"]["replacement_sector"] == "Tech"
    # Energy incumbent: that same swap would push Tech past its cap, so it is refused
    # and the best admissible alternative is the Energy name.
    assert src["BBB"]["strongest_replacement_ticker"] == ENERGY_PICK
    assert TECH_PICK not in [c["ticker"]
                            for c in src["BBB"]["replacement_shortlist"]]


# =========================================================================== #
# 5. Idempotency, immutability, and no invented evidence.
# =========================================================================== #
def test_10_repeated_capture_is_idempotent_byte_for_byte(tmp_path):
    first = _capture(tmp_path)
    blob = (tmp_path / "outcome_observations.json").read_text(encoding="utf-8")
    for _ in range(3):
        again = _capture(tmp_path)
        assert again["observations_newly_matured"] == 0
        assert again["performed_write"] is False
        assert again["observations_total"] == first["observations_total"]
    assert (tmp_path / "outcome_observations.json").read_text(encoding="utf-8") == blob
    ids = [r["observation_id"] for r in _persisted(tmp_path)]
    assert len(ids) == len(set(ids))


def test_11_a_recapture_on_different_prices_never_rewrites_recorded_evidence(tmp_path):
    _capture(tmp_path)
    before = (tmp_path / "outcome_observations.json").read_text(encoding="utf-8")
    moved = _evidence(**{**{t: 0.40 for t in INCUMBENTS},
                         TECH_PICK: -0.30, ENERGY_PICK: -0.30})
    out = _capture(tmp_path, evidence=moved)
    assert out["rewrote_existing_evidence"] is False
    assert out["performed_write"] is False
    assert (tmp_path / "outcome_observations.json").read_text(encoding="utf-8") == before


def test_12_historical_v1_evidence_is_immutable_and_stays_distinguishable(tmp_path):
    """A v1 row recorded under the OLD decision semantics is never rewritten by a v2
    capture of the same economics, and the two remain told apart by their own stamps."""
    res = _reassessment()
    row = _history_row(res)
    built = RO.build_observations(history=[row], evidence=_evidence(),
                                  lineage={"latest_completed_rebalance": {}},
                                  active_book_id=BOOK)
    target = next(o for o in built["observations"]
                  if o["ticker"] == "AAA" and o["horizon_eligible_closes"] == 20
                  and o["maturity"] == ROK.MAT_MATURE)
    legacy = {
        **target,
        "outcome_policy_version": "reassessment_outcome_policy.v1",
        "recommendation": PRK.REC_HOLD, "source_recommendation": PRK.REC_HOLD,
        "replacement_shortlist_at_decision": [], "shortlist_size_at_decision": 0,
        "proposed_exposure_reduction_at_decision": None,
        "realized_spread": -0.99,
        "observation_id": "rout_2026-08-05_%s_AAA_h20_LEGACYV1" % BOOK,
        "economic_identity_key": ROK.economic_observation_key(target),
        "recorded_at": "2026-08-05T00:00:00+00:00",
        "immutable": True, "backfilled": False,
    }
    path = tmp_path / "outcome_observations.json"
    path.write_text(json.dumps([legacy], indent=2, sort_keys=True), encoding="utf-8")
    legacy_before = json.dumps(legacy, sort_keys=True)

    out = _capture(tmp_path, res=res)
    rows = _persisted(tmp_path)
    kept = next(r for r in rows if r["observation_id"].endswith("LEGACYV1"))

    # 1. the historical row is untouched, field for field
    assert json.dumps(kept, sort_keys=True) == legacy_before
    assert kept["realized_spread"] == -0.99
    assert out["rewrote_existing_evidence"] is False
    # 2. its economics were recognised, so no second copy was minted
    assert out["economically_identical_skipped_this_run"] >= 1
    assert sum(1 for r in rows if r["ticker"] == "AAA"
               and r["horizon_eligible_closes"] == 20) == 1
    # 3. v1 and v2 rows coexist and are distinguishable by their own version stamp
    versions = {r["outcome_policy_version"] for r in rows}
    assert "reassessment_outcome_policy.v1" in versions
    assert ROK.OUTCOME_POLICY_VERSION in versions
    assert ROK.OUTCOME_POLICY_VERSION == "reassessment_outcome_policy.v2"
    # 4. and the v1 row is NOT mistaken for R70 evidence
    assert kept["replacement_shortlist_at_decision"] == []
    v2 = _one([r for r in rows if r["ticker"] == "BBB"], "BBB")
    assert v2["outcome_policy_version"] == ROK.OUTCOME_POLICY_VERSION
    assert v2["replacement_shortlist_at_decision"]


def test_13_nothing_unmatured_is_ever_persisted(tmp_path):
    """No retrospective rewriting and no invented forward evidence: a horizon the owned
    calendar cannot reach is reported as PENDING and is absent from the store."""
    n = len(INCUMBENTS)
    out = _capture(tmp_path, evidence=_evidence(cal=_calendar(8)))
    assert out["pending_observation_count"] == n      # horizon 20, every holding
    rows = _persisted(tmp_path)
    assert {r["horizon_eligible_closes"] for r in rows} == {5}
    for r in rows:
        assert r["maturity"] == ROK.MAT_MATURE
        assert r["realized_spread"] is not None
        assert r["backfilled"] is False and r["immutable"] is True

    # A longer owned calendar later matures horizon 20 — APPENDED, never backfilled.
    out2 = _capture(tmp_path, evidence=_evidence(_calendar(40)))
    assert out2["observations_newly_matured"] == n
    rows2 = _persisted(tmp_path)
    assert {r["horizon_eligible_closes"] for r in rows2} == {5, 20}
    # the horizon-5 rows recorded earlier are still the SAME rows
    by_id = {r["observation_id"]: r for r in rows2}
    for r in rows:
        assert json.dumps(by_id[r["observation_id"]], sort_keys=True) == \
            json.dumps(r, sort_keys=True)
    assert len(rows2) == len(rows) + n


def test_14_the_persisted_sample_reports_its_own_independence(tmp_path):
    """Four holdings against two candidates in ONE session is a small, correlated
    sample, and the store must say so rather than publish a verdict."""
    _capture(tmp_path)
    rows = RO.load_observations(outcome_dir=tmp_path, active_book_id=BOOK)
    card = ROK.build_scorecard(rows)
    bucket = card["replacement_outcomes"]
    assert bucket["distinct_candidates"] == 2
    assert bucket["session_clusters"] == 1
    assert bucket["verdict_permitted"] is False
    assert "TOO_FEW_EFFECTIVE_OBSERVATIONS" in bucket["verdict_blocked_reason_codes"]
    pol = ROK.build_policy_intelligence(rows)
    assert pol["policy_state"] != "POLICY_REVIEW_CANDIDATE"


def test_15_the_capture_writes_evidence_only(tmp_path):
    """Safety: the forward-evidence writer touches no holding, order, fill or NAV."""
    out = _capture(tmp_path)
    assert out["append_only"] is True
    assert out["conflicts"] == []
    safety = RO._safety()
    for flag in ("created_orders", "created_fills", "changed_holdings", "changed_cash",
                 "changed_nav", "automation_enabled", "approved_proposal",
                 "promoted_model", "broker_enabled", "live_orders_enabled"):
        assert safety[flag] is False, flag
    assert sorted(p.name for p in tmp_path.iterdir()) == ["outcome_observations.json"]
