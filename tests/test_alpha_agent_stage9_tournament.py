"""Stage 9 Autonomous Alpha Tournament — deterministic, hermetic tests.

Every store (candidate registry, shadow books, the shared research queue) lives
under pytest's ``tmp_path``; no real network, credential, scheduled task or
operational trading ledger is ever touched, and no operational state is mutated.
Covers the 24 WS13 behaviours.
"""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]
import sys
if str(_REPO.parent) not in sys.path:
    sys.path.insert(0, str(_REPO.parent))

from paper_trader.alpha_agent import tournament as T  # noqa: E402
from paper_trader.alpha_agent import telegram_control as TC  # noqa: E402
from paper_trader.alpha_agent import report_renderer as RR  # noqa: E402
from paper_trader.alpha_agent import autonomous_research as AR  # noqa: E402
from paper_trader.alpha_agent import runtime as RT  # noqa: E402

_CFG_PATH = _REPO / "configs" / "alpha_agent" / "stage9_tournament.json"
_REAL_CFG = json.loads(_CFG_PATH.read_text(encoding="utf-8"))


class Clock:
    """Deterministic monotonic ISO clock."""

    def __init__(self) -> None:
        self.n = 0

    def __call__(self) -> str:
        self.n += 1
        return "2026-07-31T%02d:%02d:%02d" % (
            self.n // 3600, (self.n // 60) % 60, self.n % 60)


def _cfg(tmp_path) -> dict:
    c = copy.deepcopy(_REAL_CFG)
    c["tournament_db"] = str(tmp_path / "tournament.sqlite")
    c["shadow_book_root"] = str(tmp_path / "shadows")
    c["shadow_books"]["shadow_book_root"] = str(tmp_path / "shadows")
    return c


def _reg(tmp_path, clock=None):
    return T.CandidateRegistry(tmp_path / "tournament.sqlite",
                               clock=clock or Clock())


def _grid(n25, n50):
    return {"grid": [
        {"cost_bps": 5, "net_annualized_return": (n25 or 0) + 0.01,
         "flips_sign": False},
        {"cost_bps": 25, "net_annualized_return": n25, "flips_sign": n25 <= 0},
        {"cost_bps": 50, "net_annualized_return": n50, "flips_sign": n50 <= 0}]}


def _row(feature, *, ic_t=3.5, ic=0.03, pos=0.6, sp_t=3.0, gross=0.12,
         net=0.09, n25=0.08, n50=0.06, turn=0.4, dd=-0.18, sub=1.0, reg=1.0,
         erosion=0.15, champ_comp=0.9, univ=120, per=48, leak=False):
    return {"feature": feature, "rank_ic_mean": ic, "rank_ic_t": ic_t,
            "rank_ic_positive_ratio": pos, "decile_spread_mean": 0.02,
            "spread_t": sp_t, "oos_ic_mean": ic * 0.8,
            "gross_annualized_return": gross, "net_annualized_return": net,
            "turnover": turn, "max_drawdown": dd, "subperiod_consistency": sub,
            "regime_consistency": reg, "cost_erosion_ratio": erosion,
            "cost_flips_sign": (n25 <= 0), "champion_complementarity": champ_comp,
            "universe": univ, "periods": per, "sharpe": 1.1,
            "annualized_vol": 0.1, "beats_null_control": True,
            "spy_excess_annualized": 0.05, "leakage_warning": leak,
            "cost_sensitivity": _grid(n25, n50)}


def _campaign(*rows, held=("sector_neutral_momentum",)):
    return {"results": list(rows),
            "held": [{"feature": f} for f in held]}


def _fingerprint(root: Path) -> str:
    h = {}
    for f in sorted(Path(root).rglob("*")):
        if f.is_file():
            h[str(f)] = hashlib.sha256(f.read_bytes()).hexdigest()
    return hashlib.sha256(json.dumps(h, sort_keys=True).encode()).hexdigest()


# --------------------------------------------------------------------------- #
# 1. Candidate registry persistence.
# --------------------------------------------------------------------------- #
def test_01_registry_persists_across_reopen(tmp_path):
    reg = _reg(tmp_path)
    cid = reg.seed_candidate(name="X", family=T.FAM_PRICE_MOMENTUM,
                             spec={"feature": "residual_momentum"},
                             data_dependencies=["owned"], universe="U",
                             pit_status=T.PIT_OWNED_PRICE)
    reg.close()
    reg2 = _reg(tmp_path)
    got = reg2.get(cid)
    assert got is not None and got["name"] == "X"
    assert reg2.counts_by_state()[T.PROPOSED] == 1
    reg2.close()


# --------------------------------------------------------------------------- #
# 2. Candidate specification deduplication.
# --------------------------------------------------------------------------- #
def test_02_spec_dedupe(tmp_path):
    reg = _reg(tmp_path)
    a = reg.seed_candidate(name="A", family=T.FAM_PRICE_MOMENTUM,
                           spec={"feature": "residual_momentum", "h": 63},
                           data_dependencies=[], universe="U",
                           pit_status=T.PIT_OWNED_PRICE)
    b = reg.seed_candidate(name="A-again", family=T.FAM_PRICE_MOMENTUM,
                           spec={"feature": "residual_momentum", "h": 63},
                           data_dependencies=[], universe="U",
                           pit_status=T.PIT_OWNED_PRICE)
    assert a == b
    assert len(reg.list()) == 1
    # Seeding the whole default catalogue twice does not duplicate.
    n1 = len(T.seed_families(reg))
    before = len(reg.list())
    T.seed_families(reg)
    assert len(reg.list()) == before
    assert n1 == len(T.default_candidate_specs())
    reg.close()


# --------------------------------------------------------------------------- #
# 3. Candidate lifecycle transitions (legal ok; illegal raises).
# --------------------------------------------------------------------------- #
def test_03_lifecycle_transitions(tmp_path):
    reg = _reg(tmp_path)
    cid = reg.seed_candidate(name="X", family=T.FAM_PRICE_MOMENTUM,
                             spec={"feature": "f"}, data_dependencies=[],
                             universe="U", pit_status=T.PIT_OWNED_PRICE)
    reg.transition(cid, T.TESTING)
    reg.transition(cid, T.KEEP_FOR_RESEARCH)
    assert reg.get(cid)["lifecycle_state"] == T.KEEP_FOR_RESEARCH
    # illegal: PROPOSED -> KEEP directly.
    c2 = reg.seed_candidate(name="Y", family=T.FAM_PRICE_MOMENTUM,
                            spec={"feature": "g"}, data_dependencies=[],
                            universe="U", pit_status=T.PIT_OWNED_PRICE)
    with pytest.raises(T.TransitionError):
        reg.transition(c2, T.KEEP_FOR_RESEARCH)
    reg.close()


# --------------------------------------------------------------------------- #
# 4. DATA_HOLD for insufficient / non-PIT data.
# --------------------------------------------------------------------------- #
def test_04_data_hold_insufficient(tmp_path):
    cfg = _cfg(tmp_path)
    # coverage below the floor -> DATA_HOLD (never REJECTED on missing data).
    m = {"coverage_pct": 20.0, "scored_periods": 4, "min_names_per_period": 120,
         "point_in_time_valid": True, "survivorship_safe": True,
         "lookahead_contamination": False}
    g = T.classify_evidence(m, cfg)
    assert g["target_state"] == T.DATA_HOLD
    assert g["complete"] is False
    # look-ahead contamination is DATA_HOLD, not a silent pass.
    m2 = {"coverage_pct": 100.0, "scored_periods": 48, "min_names_per_period": 120,
          "point_in_time_valid": True, "survivorship_safe": True,
          "lookahead_contamination": True}
    assert T.classify_evidence(m2, cfg)["blocker"] == T.BLOCK_LOOKAHEAD


# --------------------------------------------------------------------------- #
# 5. Weak COMPLETE evidence -> REJECTED.
# --------------------------------------------------------------------------- #
def test_05_weak_complete_rejected(tmp_path):
    cfg = _cfg(tmp_path)
    row = _row("short_term_reversal", ic_t=0.3, ic=0.002, pos=0.47, sp_t=0.2,
               net=-0.01, n25=-0.01, n50=-0.03, turn=2.6, dd=-0.5, sub=0.4,
               reg=0.4, erosion=0.9)
    m = T.row_to_contract_metrics(row)
    g = T.classify_evidence(m, cfg)
    assert g["target_state"] == T.REJECTED
    assert g["complete"] is True
    assert g["failed_gates"]  # at least one specific failed gate


# --------------------------------------------------------------------------- #
# 6. Strong but immature evidence -> KEEP_FOR_RESEARCH (not auto-promoted).
# --------------------------------------------------------------------------- #
def test_06_strong_complete_keep(tmp_path):
    cfg = _cfg(tmp_path)
    m = T.row_to_contract_metrics(_row("residual_momentum", ic_t=3.5))
    g = T.classify_evidence(m, cfg)
    assert g["target_state"] == T.KEEP_FOR_RESEARCH
    assert g["evidence_status"] == T.EVIDENCE_COMPLETE_STRONG


# --------------------------------------------------------------------------- #
# 7. No candidate automatically promotes; READY is manual-only.
# --------------------------------------------------------------------------- #
def test_07_no_automatic_promotion(tmp_path):
    assert "PROMOTED" not in T.LIFECYCLE_STATES
    # No transition target is a live/promoted state.
    for targets in T.ALLOWED_TRANSITIONS.values():
        assert "PROMOTED" not in targets
    cfg = _cfg(tmp_path)
    reg = _reg(tmp_path)
    out = T.run_tournament_cycle(reg, cfg, campaign_result=_campaign(
        _row("residual_momentum", ic_t=4.0)), evidence_date="2026-07-31",
        max_candidates=50)
    # the tick never reaches READY_FOR_MANUAL_REVIEW automatically.
    assert out["counts_by_state"][T.READY_FOR_MANUAL_REVIEW] == 0
    reg.close()


# --------------------------------------------------------------------------- #
# 8. Leaderboard scoring is reproducible.
# --------------------------------------------------------------------------- #
def test_08_scoring_reproducible(tmp_path):
    cfg = _cfg(tmp_path)
    m = T.row_to_contract_metrics(_row("residual_momentum", ic_t=3.2))
    s1 = T.score_candidate(m, cfg, corr_champion=0.1)
    s2 = T.score_candidate(m, cfg, corr_champion=0.1)
    assert s1 == s2
    assert 0.0 <= s1["combined_score"] <= 1.0
    # decomposable: the six sub-scores are all present.
    for k in ("historical_evidence_score", "robustness_score", "cost_score",
              "stability_score", "diversification_score", "forward_evidence_score"):
        assert k in s1


# --------------------------------------------------------------------------- #
# 9. Gross-return strength cannot hide excessive costs.
# --------------------------------------------------------------------------- #
def test_09_gross_cannot_hide_costs(tmp_path):
    cfg = _cfg(tmp_path)
    # Strong IC + strong gross, but costs erase the net at 25bps -> REJECTED and
    # its (still-computed) score is capped well below a clean strong candidate.
    costly = T.row_to_contract_metrics(
        _row("x", ic_t=3.5, gross=0.30, net=-0.02, n25=-0.02, n50=-0.05,
             erosion=0.95, turn=1.9))
    g = T.classify_evidence(costly, cfg)
    assert g["target_state"] == T.REJECTED
    assert T.REJECT_NOT_COST_ROBUST in g["failed_gates"]
    clean = T.row_to_contract_metrics(_row("y", ic_t=3.5))
    sc_costly = T.score_candidate(costly, cfg, corr_champion=0.1)
    sc_clean = T.score_candidate(clean, cfg, corr_champion=0.1)
    assert sc_costly["cost_score"] == 0.0
    assert sc_costly["combined_score"] < sc_clean["combined_score"]


# --------------------------------------------------------------------------- #
# 10. Unstable candidates are penalized (low robustness sub-score).
# --------------------------------------------------------------------------- #
def test_10_unstable_penalized(tmp_path):
    cfg = _cfg(tmp_path)
    stable = T.row_to_contract_metrics(_row("s", sub=1.0, reg=1.0))
    unstable = T.row_to_contract_metrics(_row("u", sub=0.2, reg=0.2))
    ss = T.score_candidate(stable, cfg, corr_champion=0.1)
    su = T.score_candidate(unstable, cfg, corr_champion=0.1)
    assert su["robustness_score"] < ss["robustness_score"]


# --------------------------------------------------------------------------- #
# 11. Regime weakness is visible (specific gate on complete evidence).
# --------------------------------------------------------------------------- #
def test_11_regime_weakness_visible(tmp_path):
    cfg = _cfg(tmp_path)
    m = T.row_to_contract_metrics(_row("r", reg=0.2))
    g = T.classify_evidence(m, cfg)
    assert g["target_state"] == T.REJECTED
    assert T.REJECT_UNSTABLE_REGIME in g["failed_gates"]


# --------------------------------------------------------------------------- #
# 12. Highly correlated candidates receive a diversification penalty.
# --------------------------------------------------------------------------- #
def test_12_correlation_penalty(tmp_path):
    cfg = _cfg(tmp_path)
    m = T.row_to_contract_metrics(_row("c"))
    diverse = T.score_candidate(m, cfg, corr_champion=0.05)
    redundant = T.score_candidate(m, cfg, corr_champion=0.95)
    assert redundant["diversification_score"] < diverse["diversification_score"]
    assert redundant["combined_score"] < diverse["combined_score"]


# --------------------------------------------------------------------------- #
# 13. Automatic next-experiment generation is bounded and deduplicated.
# --------------------------------------------------------------------------- #
def test_13_generation_bounded_deduped(tmp_path):
    cfg = _cfg(tmp_path)
    reg = _reg(tmp_path)
    T.run_tournament_cycle(reg, cfg, campaign_result=_campaign(
        _row("residual_momentum", ic_t=3.6), _row("low_volatility", ic_t=2.8)),
        evidence_date="2026-07-31", max_candidates=50)
    budget = cfg["auto_experiments"]["max_new_experiments_per_cycle"]
    g1 = T.generate_next_experiments(reg, cfg, queue=None)
    assert g1["count"] <= budget
    total_before = reg.generated_count()
    # Re-run until exhausted; never exceeds the distinct plan set (no endless dup).
    for _ in range(20):
        T.generate_next_experiments(reg, cfg, queue=None)
    total_after = reg.generated_count()
    # a second identical pass adds nothing new once exhausted.
    stable = reg.generated_count()
    T.generate_next_experiments(reg, cfg, queue=None)
    assert reg.generated_count() == stable
    assert total_after >= total_before
    reg.close()


# --------------------------------------------------------------------------- #
# 14. Queue continuation survives restart (durable shared queue).
# --------------------------------------------------------------------------- #
def test_14_queue_survives_restart(tmp_path):
    cfg = _cfg(tmp_path)
    reg = _reg(tmp_path)
    qpath = tmp_path / "autonomy.sqlite"
    q = AR.ResearchQueue(qpath, clock=Clock())
    T.run_tournament_cycle(reg, cfg, queue=q, campaign_result=_campaign(
        _row("residual_momentum", ic_t=3.6)), evidence_date="2026-07-31",
        max_candidates=50)
    depth = q.depth()
    assert depth > 0
    # reopen -> jobs persist, still claimable.
    q2 = AR.ResearchQueue(qpath, clock=Clock())
    assert q2.depth() == depth
    job = q2.claim_next()
    assert job is not None and job.lane.startswith("tournament.")
    reg.close()


# --------------------------------------------------------------------------- #
# 15. A failed experiment does not stop the tournament.
# --------------------------------------------------------------------------- #
def test_15_failure_does_not_stop(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    reg = _reg(tmp_path)
    T.seed_families(reg)
    real = T._evaluate_one
    calls = {"n": 0}

    def boom(registry, c, cand, fm, held, ss, ed):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("synthetic evaluator failure")
        return real(registry, c, cand, fm, held, ss, ed)

    monkeypatch.setattr(T, "_evaluate_one", boom)
    out = T.run_tournament_cycle(reg, cfg, seed=False, campaign_result=_campaign(
        _row("residual_momentum", ic_t=3.6)), evidence_date="2026-07-31",
        max_candidates=4)
    # first candidate errored, the rest still evaluated; the cycle completed.
    assert out["status"] == "OK"
    assert any(r.get("error") for r in out["evaluated"])
    assert len(out["evaluated"]) >= 2
    reg.close()


# --------------------------------------------------------------------------- #
# 16. Shadow books cannot mutate the operational portfolio.
# --------------------------------------------------------------------------- #
def test_16_shadow_no_operational_mutation(tmp_path):
    cfg = _cfg(tmp_path)
    opsroot = tmp_path / "operational_ledger"
    opsroot.mkdir()
    (opsroot / "paper_books.json").write_text('{"immutable": true}',
                                              encoding="utf-8")
    before = _fingerprint(opsroot)
    reg = _reg(tmp_path)
    out = T.run_tournament_cycle(reg, cfg, campaign_result=_campaign(
        _row("residual_momentum", ic_t=4.0)), evidence_date="2026-07-31",
        max_candidates=50)
    assert out["shadow_books_activated"]  # at least one shadow book opened
    assert _fingerprint(opsroot) == before  # operational ledger untouched
    # the shadow book wrote ONLY under the shadow root.
    sbroot = Path(cfg["shadow_book_root"])
    assert sbroot.exists() and any(sbroot.rglob("shadow_book.json"))
    reg.close()


# --------------------------------------------------------------------------- #
# 17. Forward history cannot be backfilled retroactively.
# --------------------------------------------------------------------------- #
def test_17_no_retroactive_forward(tmp_path):
    book = T.ShadowBook(tmp_path / "sh", "sb_x")
    book.inception(candidate_id="c", inception_date="2026-07-31", membership=[],
                   benchmark="SPY", cost_bps=50.0, spec={})
    with pytest.raises(T.RetroactiveError):
        book.record_mark(date="2026-07-30", nav=100000.0)  # before inception
    book.record_mark(date="2026-08-03", nav=100500.0)
    with pytest.raises(T.RetroactiveError):
        book.record_mark(date="2026-08-03", nav=100600.0)  # not after last
    rp = book.replay()
    assert rp["forward_observations"] == 1
    assert rp["label"] == T.SHADOW_LABEL


# --------------------------------------------------------------------------- #
# 18. Telegram commands return real tournament data.
# --------------------------------------------------------------------------- #
def test_18_telegram_real_data(tmp_path):
    cfg = _cfg(tmp_path)
    dbp = cfg["tournament_db"]
    reg = _reg(tmp_path)
    T.run_tournament_cycle(reg, cfg, campaign_result=_campaign(
        _row("residual_momentum", ic_t=3.6)), evidence_date="2026-07-31",
        max_candidates=50)
    reg.close()
    prov = TC.build_tournament_providers(
        tournament_loader=lambda: T.load_tournament(db_path=dbp),
        candidate_loader=lambda c: T.load_candidate(db_path=dbp, candidate_id=c))
    out = prov["tournament"]()
    assert "ALPHA TOURNAMENT" in out
    assert "SHADOW_BOOK_ACTIVE" in out or "KEEP_FOR_RESEARCH" in out
    lb = prov["leaderboard"]()
    assert "residual momentum" in lb.lower()
    fam = prov["families"]()
    assert "PRICE_MOMENTUM" in fam


# --------------------------------------------------------------------------- #
# 19. Unknown candidate ids return a precise diagnostic.
# --------------------------------------------------------------------------- #
def test_19_unknown_id_diagnostic(tmp_path):
    cfg = _cfg(tmp_path)
    dbp = cfg["tournament_db"]
    reg = _reg(tmp_path)
    T.seed_families(reg)
    reg.close()
    prov = TC.build_tournament_providers(
        tournament_loader=lambda: T.load_tournament(db_path=dbp),
        candidate_loader=lambda c: T.load_candidate(db_path=dbp, candidate_id=c))
    assert "No candidate matches" in prov["candidate"]("NOPE_XYZ")
    assert "No shadow book matches" in prov["shadowbook"]("NOPE_XYZ")
    assert T.load_candidate(db_path=dbp, candidate_id="does-not-exist") is None


# --------------------------------------------------------------------------- #
# 20. Reports only flag meaningful changes.
# --------------------------------------------------------------------------- #
def test_20_reports_only_meaningful(tmp_path):
    raw = ["No meaningful tournament change since the previous report.",
           "New retained candidate: 63-day residual momentum (PRICE_MOMENTUM)"]
    assert RR.material_tournament_changes(raw) == [
        "New retained candidate: 63-day residual momentum (PRICE_MOMENTUM)"]
    assert RR.tournament_change_lines({}) == []
    assert RR.tournament_change_lines(
        {"tournament_changes": ["No tournament activity to report."]}) == []
    got = RR.tournament_change_lines(
        {"tournament_changes": ["Candidate rejected: Short-term reversal (x)"]})
    assert got == ["Candidate rejected: Short-term reversal (x)"]


# --------------------------------------------------------------------------- #
# 21. API/read payloads are read-only (no rows added by reads).
# --------------------------------------------------------------------------- #
def test_21_read_only_payloads(tmp_path):
    cfg = _cfg(tmp_path)
    dbp = cfg["tournament_db"]
    reg = _reg(tmp_path)
    T.run_tournament_cycle(reg, cfg, campaign_result=_campaign(
        _row("residual_momentum", ic_t=3.6)), evidence_date="2026-07-31",
        max_candidates=50)
    before = reg.counts_by_state()
    reg.close()
    p = T.load_tournament(db_path=dbp)
    snap = T.tournament_snapshot(db_path=dbp)
    _ = T.meaningful_tournament_changes(db_path=dbp)
    assert p["status"] == "OK" and p["read_only"] is True
    assert p["no_automatic_promotion"] is True
    assert snap["no_automatic_promotion"] is True
    reg2 = _reg(tmp_path)
    assert reg2.counts_by_state() == before  # reads added no rows
    reg2.close()


# --------------------------------------------------------------------------- #
# 22. UI payload renders both empty and populated states safely.
# --------------------------------------------------------------------------- #
def test_22_payload_empty_and_populated(tmp_path):
    # empty: no DB yet -> controlled UNAVAILABLE (never raises).
    empty = T.tournament_snapshot(db_path=str(tmp_path / "missing.sqlite"))
    assert empty["status"] == "UNAVAILABLE"
    assert "safety_badges" in empty
    # populated.
    cfg = _cfg(tmp_path)
    reg = _reg(tmp_path)
    T.run_tournament_cycle(reg, cfg, campaign_result=_campaign(
        _row("residual_momentum", ic_t=3.6)), evidence_date="2026-07-31",
        max_candidates=50)
    reg.close()
    snap = T.tournament_snapshot(db_path=cfg["tournament_db"])
    assert snap["status"] == "OK"
    assert isinstance(snap["top_candidates"], list)
    assert snap["counts_by_state"][T.SHADOW_BOOK_ACTIVE] >= 0


# --------------------------------------------------------------------------- #
# 23. Running a full tick leaves an unrelated operational ledger byte-identical.
# --------------------------------------------------------------------------- #
def test_23_operational_ledger_untouched(tmp_path):
    cfg = _cfg(tmp_path)
    ops = tmp_path / "ops"
    ops.mkdir()
    for n in ("paper_books.json", "paper_fills.json", "forward_performance.json"):
        (ops / n).write_text('{"n":"%s"}' % n, encoding="utf-8")
    before = _fingerprint(ops)
    reg = _reg(tmp_path)
    q = AR.ResearchQueue(tmp_path / "q.sqlite", clock=Clock())
    T.run_tournament_cycle(reg, cfg, queue=q, campaign_result=_campaign(
        _row("residual_momentum", ic_t=3.7), _row("low_volatility", ic_t=2.9)),
        evidence_date="2026-07-31", max_candidates=50)
    reg.close()
    assert _fingerprint(ops) == before


# --------------------------------------------------------------------------- #
# 24. Existing Stage 8 / 8.1 functionality remains intact (opt-in integration).
# --------------------------------------------------------------------------- #
def test_24_stage8_intact():
    import inspect
    sig = inspect.signature(RT.run_autonomy_cycle)
    # tournament tick is strictly opt-in; existing callers are unchanged.
    assert sig.parameters["run_tournament"].default is False
    # command registry still advertises the Stage 8 commands + the new ones.
    for c in ("/status", "/queue", "/experiments", "/candidates", "/attribution"):
        assert c in TC.COMMANDS
    for c in ("/tournament", "/leaderboard", "/candidate", "/why", "/compare",
              "/shadowbooks", "/shadowbook", "/families"):
        assert c in TC.COMMANDS
    # the durable-queue category vocabulary is unchanged (no new categories).
    assert len(AR.JOB_CATEGORIES) == 12


# --------------------------------------------------------------------------- #
# Bonus: manual-review gate never fires without forward observations.
# --------------------------------------------------------------------------- #
def test_25_manual_review_requires_forward(tmp_path):
    cfg = _cfg(tmp_path)
    m = T.row_to_contract_metrics(_row("m", ic_t=4.0))
    sc = T.score_candidate(m, cfg, corr_champion=0.05)
    gate = {"complete": True, "target_state": T.KEEP_FOR_RESEARCH}
    r0 = T.qualifies_for_manual_review(
        metrics=m, scores=sc, cfg=cfg, gate=gate, corr_champion=0.05,
        corr_retained=0.1, incremental_net25=0.03, forward_observations=0,
        caveats_displayed=True)
    assert r0["eligible"] is False
    assert "INSUFFICIENT_FORWARD_OBSERVATIONS" in r0["missing"]
    r1 = T.qualifies_for_manual_review(
        metrics=m, scores=sc, cfg=cfg, gate=gate, corr_champion=0.05,
        corr_retained=0.1, incremental_net25=0.03, forward_observations=40,
        caveats_displayed=True)
    assert r1["eligible"] is True
