"""Controlled Autonomous Research Execution Bridge — targeted tests.

Deterministic and hermetic: the pure kernel (``engine.research_bridge``) is driven by
explicit dicts; the composition/dispatch/persistence owner (``api.research_bridge``) is
driven by a fake in-memory queue + temporary artifact roots; the Alpha Strength Gate is
exercised against the REAL ``alpha_agent.tournament.classify_evidence`` with the REAL
``configs/alpha_agent/stage9_tournament.json`` thresholds. No live queue / tournament DB /
operational ledger / provider / prediction is touched.

Matrix:
  A. architecture (single owner/kernel, delegation, no forbidden terms, safety)
  B. multi-horizon coherence            C. idempotency (opp/mandate/dispatch/result/reassess)
  D. PIT                                E. alpha strength (real gate)
  F. safety                             G. UI / read model            H. recovery
"""
from __future__ import annotations

import copy
import json
from pathlib import Path

from paper_trader.engine import research_bridge as K
from paper_trader.api import research_bridge as A

ROOT = Path(__file__).resolve().parent.parent
KERNEL_SRC = (ROOT / "engine" / "research_bridge.py").read_text(encoding="utf-8")
OWNER_SRC = (ROOT / "api" / "research_bridge.py").read_text(encoding="utf-8")
UI = ROOT / "api" / "ui" / "index.html"
INVENTORY = ROOT / "docs" / "architecture" / "system_inventory.json"


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
def _champion():
    return {"model_id": "composite_sn", "primary_model_id": "fundamental_momentum_50_50_v1",
            "strategy_version": "v1", "model_registry_version": "25",
            "automatic_promotion_allowed": False}


def _assessment(opps=None):
    return {
        "eligible_market_date": "2026-08-07",
        "champion": _champion(),
        "assessment_hash": "AH1",
        "provenance": {"forward_evidence_hash": "2026-08-07"},
        "degradation": {"categories": ["TURNOVER_INEFFICIENCY"]},
        "research_opportunities": opps if opps is not None else [
            {"opportunity_id": "investigate_rank_ic_degradation", "category": "SIGNAL",
             "hypothesis": "rank IC deteriorated", "reason": "Forward rank IC is WATCH",
             "priority": 60, "expected_research_value": "MEDIUM",
             "permitted_data": ["OWNED_FORWARD_EVIDENCE", "OWNED_UNIVERSE_SCORING"],
             "proposed_experiment": {"experiment_type": "RANK_IC_WALK_FORWARD"}},
            {"opportunity_id": "inspect_regime_evidence", "category": "DATA",
             "priority": 40, "permitted_data": ["OWNED_PRICE_PANEL"],
             "proposed_experiment": {"experiment_type": "REGIME_EVIDENCE_INSPECTION"}},
        ],
    }


class FakeQueue:
    """In-memory stand-in for alpha_agent.autonomous_research.ResearchQueue: idempotent
    enqueue by dedupe_key + requeue_stale."""

    def __init__(self):
        self.jobs = {}
        self.enqueue_calls = 0
        self._seq = 0
        self.stale = 0

    def enqueue(self, category, *, lane, payload=None, priority=0, max_attempts=None,
                dedupe_key=None, origin="seed", not_before=None):
        self.enqueue_calls += 1
        if dedupe_key in self.jobs:
            return self.jobs[dedupe_key]
        self._seq += 1
        jid = "job_%03d" % self._seq
        self.jobs[dedupe_key] = jid
        return jid

    def requeue_stale(self, *, now=None, stale_seconds=None):
        return self.stale


def _metrics(**over):
    """A COMPLETE-and-STRONG contract-metrics row (passes every stage9 gate)."""
    m = {"coverage_pct": 80.0, "scored_periods": 20, "min_names_per_period": 40,
         "point_in_time_valid": True, "survivorship_safe": True,
         "lookahead_contamination": False,
         "rank_ic": 0.05, "rank_ic_t": 3.0, "positive_ic_hit_rate": 0.60,
         "spread_t": 3.0, "net25_spread": 0.01, "subperiod_consistency": 0.80,
         "regime_consistency": 0.70, "max_drawdown_pct": -10.0,
         "turnover_per_rebalance": 0.5, "sector_concentration_pct": 20.0,
         "worst_period_return_pct": -10.0}
    m.update(over)
    return m


def _cand(cid="c1", metrics=None, **over):
    c = {"candidate_id": cid, "family": "profitability_quality", "horizon": 63,
         "metrics": metrics if metrics is not None else _metrics(),
         "champion_correlation": 0.4, "incremental_net25_spread": 0.01,
         "forward_observation_count": 30, "rank_ic_t": 3.0, "n_periods": 20}
    c.update(over)
    return c


def _normalized():
    return A.select_opportunity(assessment=_assessment())


# =========================================================================== #
# A. ARCHITECTURE
# =========================================================================== #
def test_a01_single_calculation_owner():
    assert K.CALCULATION_OWNER == "engine.research_bridge"
    assert A.COMPOSITION_OWNER == "api.research_bridge"


def test_a02_owner_delegates_to_kernel():
    assert "from paper_trader.engine import research_bridge as kernel" in OWNER_SRC
    for fn in ("kernel.build_mandate", "kernel.evaluate_alpha_strength",
               "kernel.build_result_envelope", "kernel.reassess",
               "kernel.dispatch_descriptor"):
        assert fn in OWNER_SRC


def test_a03_kernel_is_pure_no_io():
    # No filesystem/network/env in the pure kernel.
    for term in ("open(", "requests", "os.environ", "sqlite3", "urllib", "Path("):
        assert term not in KERNEL_SRC, term
    assert "import os" not in KERNEL_SRC


def test_a04_no_second_queue_or_tournament_or_registry():
    # The bridge REUSES the existing queue/tournament; it must not define its own.
    assert "class ResearchQueue" not in OWNER_SRC and "class ResearchQueue" not in KERNEL_SRC
    assert "class CandidateRegistry" not in OWNER_SRC
    assert "autonomous_research.ResearchQueue" in OWNER_SRC  # reuse reference
    assert "tournament.classify_evidence" in OWNER_SRC       # reuse gate


def test_a05_no_operational_ledger_literal():
    assert ".paper_trader" not in OWNER_SRC
    assert ".paper_trader" not in KERNEL_SRC


def test_a06_no_execution_call_terms():
    for term in ("place_order", "submit_order", "execute_order", "send_order",
                 "broker_execute", "route_order"):
        assert (term + "(") not in OWNER_SRC
        assert (term + "(") not in KERNEL_SRC


def test_a07_modules_registered_in_inventory():
    inv = json.loads(INVENTORY.read_text(encoding="utf-8"))
    paths = {m.get("path", "").replace("\\", "/") for m in inv.get("modules", [])}
    assert "engine/research_bridge.py" in paths
    assert "api/research_bridge.py" in paths


def test_a08_terminal_states_reuse_existing_constants():
    # No invented terminal strings — reuse the tournament vocabulary.
    assert K.OUTCOME_NO_DEFENSIBLE_ALPHA == "NO_DEFENSIBLE_ALPHA"
    assert K.OUTCOME_DATA_HOLD == "DATA_HOLD"
    assert K.OUTCOME_KEEP_FOR_RESEARCH == "KEEP_FOR_RESEARCH"
    assert K.OUTCOME_MANUAL_REVIEW == "READY_FOR_MANUAL_REVIEW"


# =========================================================================== #
# B. MULTI-HORIZON COHERENCE
# =========================================================================== #
def test_b01_quality_not_one_session():
    ok, reason = K.horizon_eligible("profitability_quality", 1)
    assert ok is False and "EVENT" in reason


def test_b02_quality_medium_long_ok():
    assert K.horizon_eligible("profitability_quality", 63)[0] is True
    assert K.horizon_eligible("profitability_quality", 20)[0] is True


def test_b03_event_family_one_session_ok():
    assert K.horizon_eligible("post_filing_price_reaction", 1)[0] is True
    assert K.horizon_eligible("post_filing_price_reaction", 63)[0] is False


def test_b04_forward_label_vocab_is_5_20_63():
    assert K.FORWARD_LABEL_HORIZONS == (5, 20, 63)
    assert K.FORWARD_MATURITY_HORIZONS == (1, 5, 20, 63)
    assert K.EXECUTION_LAG_SESSIONS == 1


def test_b05_coherent_horizons_for_signal_families():
    ch = K.coherent_horizons(category="SIGNAL",
                             families=["profitability_quality", "price_momentum"])
    assert ch["horizons"] == [20, 63]
    assert ch["forward_label_horizons"] == [20, 63]
    assert ch["short_event_horizons"] == []
    assert ch["execution_lag_sessions"] == 1


def test_b06_inappropriate_combo_rejected_explicitly():
    ch = K.coherent_horizons(category="SIGNAL", families=["profitability_quality"])
    reasons = {(r["horizon"], r["reason"]) for r in ch["rejected"]}
    assert (1, "ONE_SESSION_ONLY_FOR_EVENT_OR_FILING_REACTION_FAMILIES") in reasons


# =========================================================================== #
# C. IDEMPOTENCY
# =========================================================================== #
def test_c01_normalize_opportunity_deterministic():
    a = _normalized()
    b = _normalized()
    assert a == b
    assert a["dispatchable"] is True
    assert a["opportunity_id"] == "investigate_rank_ic_degradation"


def test_c02_mandate_id_and_hash_idempotent():
    no = _normalized()
    m1 = K.finalize_mandate_hash(K.build_mandate(normalized_opportunity=no))
    m2 = K.finalize_mandate_hash(K.build_mandate(normalized_opportunity=no))
    assert m1["mandate_id"] == m2["mandate_id"]
    assert m1["mandate_hash"] == m2["mandate_hash"]
    assert m1["dispatch_identity"] == m2["dispatch_identity"]


def test_c03_persist_mandate_idempotent(tmp_path):
    no = _normalized()
    r1 = A.build_and_persist_mandate(normalized_opportunity=no,
                                     active_book_id="alpha_paper_book_1", bridge_dir=tmp_path)
    r2 = A.build_and_persist_mandate(normalized_opportunity=no,
                                     active_book_id="alpha_paper_book_1", bridge_dir=tmp_path)
    assert r1["status"] == "CREATED"
    assert r2["status"] == "REUSED_EXISTING"
    files = list((tmp_path / "mandates").glob("*.json"))
    assert len(files) == 1


def test_c04_dispatch_idempotent_no_duplicate_job(tmp_path):
    no = _normalized()
    rec = A.build_and_persist_mandate(normalized_opportunity=no,
                                      active_book_id="b", bridge_dir=tmp_path)
    q = FakeQueue()
    d1 = A.dispatch_mandate(mandate=rec["mandate"], active_book_id="b", queue=q,
                            bridge_dir=tmp_path)
    d2 = A.dispatch_mandate(mandate=rec["mandate"], active_book_id="b", queue=q,
                            bridge_dir=tmp_path)
    assert d1["job_id"] == d2["job_id"]
    assert len(q.jobs) == 1          # one live job for repeated dispatch
    assert q.enqueue_calls == 2      # both attempts hit the queue; dedupe collapses them


def test_c05_ingest_result_idempotent(tmp_path):
    no = _normalized()
    rec = A.build_and_persist_mandate(normalized_opportunity=no, active_book_id="b",
                                      bridge_dir=tmp_path)
    weak = _cand(metrics=_metrics(rank_ic_t=0.4))
    out1 = A.ingest_result(mandate=rec["mandate"], candidate_rows=[weak],
                           campaign={"entrypoint": "unit", "terminal_token": "X"},
                           active_book_id="b", bridge_dir=tmp_path)
    out2 = A.ingest_result(mandate=rec["mandate"], candidate_rows=[weak],
                           campaign={"entrypoint": "unit", "terminal_token": "X"},
                           active_book_id="b", bridge_dir=tmp_path)
    assert out1["envelope"]["envelope_hash"] == out2["envelope"]["envelope_hash"]


def test_c06_reassess_deterministic(tmp_path):
    no = _normalized()
    env = K.build_result_envelope(
        mandate=K.build_mandate(normalized_opportunity=no),
        alpha_strength={"outcome": "NO_DEFENSIBLE_ALPHA", "counts": {}},
        campaign={}, candidate_evidence=[])
    r1 = K.reassess(normalized_opportunity=no, envelope=env)
    r2 = K.reassess(normalized_opportunity=no, envelope=env)
    assert r1 == r2


# =========================================================================== #
# D. PIT
# =========================================================================== #
def test_d01_pit_invalid_is_data_hold():
    cfg = A.load_gate_cfg()
    row = _cand(metrics=_metrics(point_in_time_valid=False))
    g = K.evaluate_alpha_strength(candidates=[row], classify_fn=_real_classify(), gate_cfg=cfg)
    assert g["outcome"] == "DATA_HOLD"


def test_d02_lookahead_is_data_hold():
    cfg = A.load_gate_cfg()
    row = _cand(metrics=_metrics(lookahead_contamination=True))
    g = K.evaluate_alpha_strength(candidates=[row], classify_fn=_real_classify(), gate_cfg=cfg)
    assert g["outcome"] == "DATA_HOLD"


def test_d03_eligible_date_propagates_no_hindsight():
    no = _normalized()
    assert no["eligible_market_date"] == "2026-08-07"
    m = K.build_mandate(normalized_opportunity=no)
    assert m["eligible_market_date"] == "2026-08-07"
    assert m["requirements"]["execution_lag_sessions"] == 1


# =========================================================================== #
# E. ALPHA STRENGTH (real gate)
# =========================================================================== #
def _real_classify():
    from paper_trader.alpha_agent import tournament as trn
    return trn.classify_evidence


def test_e01_strong_fdr_orthogonal_forward_is_manual_review():
    cfg = A.load_gate_cfg()
    row = _cand(champion_correlation=0.4, incremental_net25_spread=0.01,
                forward_observation_count=30, rank_ic_t=3.0, n_periods=20)
    g = K.evaluate_alpha_strength(candidates=[row], classify_fn=_real_classify(),
                                  gate_cfg=cfg, fdr_qvalues={"c1": 0.01})
    assert g["outcome"] == "READY_FOR_MANUAL_REVIEW"
    assert g["requires_manual_model_review"] is True


def test_e02_correlated_challenger_not_manual_review():
    cfg = A.load_gate_cfg()
    row = _cand(champion_correlation=0.97)      # redundant vs champion
    g = K.evaluate_alpha_strength(candidates=[row], classify_fn=_real_classify(),
                                  gate_cfg=cfg, fdr_qvalues={"c1": 0.01})
    assert g["outcome"] == "KEEP_FOR_RESEARCH"
    assert g["counts"]["manual_review_eligible"] == 0


def test_e03_fdr_failure_blocks_manual_review():
    cfg = A.load_gate_cfg()
    row = _cand()
    g = K.evaluate_alpha_strength(candidates=[row], classify_fn=_real_classify(),
                                  gate_cfg=cfg, fdr_qvalues={"c1": 0.9})
    assert g["outcome"] == "KEEP_FOR_RESEARCH"     # KEEP, not manual review


def test_e04_weak_ic_t_is_no_defensible_alpha():
    cfg = A.load_gate_cfg()
    row = _cand(metrics=_metrics(rank_ic_t=0.4))
    g = K.evaluate_alpha_strength(candidates=[row], classify_fn=_real_classify(), gate_cfg=cfg)
    assert g["outcome"] == "NO_DEFENSIBLE_ALPHA"
    assert g["no_defensible_alpha"] is True


def test_e05_high_cost_rejects():
    cfg = A.load_gate_cfg()
    row = _cand(metrics=_metrics(net25_spread=-0.01))
    g = K.evaluate_alpha_strength(candidates=[row], classify_fn=_real_classify(), gate_cfg=cfg)
    assert g["outcome"] == "NO_DEFENSIBLE_ALPHA"


def test_e06_regime_fragility_rejects():
    cfg = A.load_gate_cfg()
    row = _cand(metrics=_metrics(regime_consistency=0.2))
    g = K.evaluate_alpha_strength(candidates=[row], classify_fn=_real_classify(), gate_cfg=cfg)
    assert g["outcome"] == "NO_DEFENSIBLE_ALPHA"


def test_e07_insufficient_sample_holds():
    cfg = A.load_gate_cfg()
    row = _cand(metrics=_metrics(scored_periods=5))
    g = K.evaluate_alpha_strength(candidates=[row], classify_fn=_real_classify(), gate_cfg=cfg)
    assert g["outcome"] == "DATA_HOLD"


def test_e08_no_defensible_alpha_is_valid_terminal():
    assert "NO_DEFENSIBLE_ALPHA" in K.ALPHA_STRENGTH_OUTCOME_VOCAB
    # mixed: one KEEP (no fdr) + one reject -> KEEP dominates (still not manual)
    cfg = A.load_gate_cfg()
    rows = [_cand("c1"), _cand("c2", metrics=_metrics(rank_ic_t=0.3))]
    g = K.evaluate_alpha_strength(candidates=rows, classify_fn=_real_classify(),
                                  gate_cfg=cfg, fdr_qvalues={"c1": 0.9, "c2": 0.9})
    assert g["outcome"] == "KEEP_FOR_RESEARCH"


def test_e09_gate_never_forces_challenger():
    cfg = A.load_gate_cfg()
    row = _cand(metrics=_metrics(rank_ic_t=0.4))
    g = K.evaluate_alpha_strength(candidates=[row], classify_fn=_real_classify(), gate_cfg=cfg)
    assert g["forces_challenger"] is False
    assert g["automatic_promotion_allowed"] is False


def test_e10_canonical_lifecycle_is_authoritative():
    # A candidate whose isolated metrics recompute to KEEP but whose AUTHORITATIVE tournament
    # lifecycle is REJECTED must NOT be reported as a challenger — the canonical state wins and
    # the reproduction mismatch is surfaced transparently.
    cfg = A.load_gate_cfg()
    row = _cand(metrics=_metrics(rank_ic_t=3.0), canonical_target_state="REJECTED")
    g = K.evaluate_alpha_strength(candidates=[row], classify_fn=_real_classify(), gate_cfg=cfg,
                                  fdr_qvalues={"c1": 0.01})
    assert g["outcome"] == "NO_DEFENSIBLE_ALPHA"
    per = g["per_candidate"][0]
    assert per["target_state"] == "REJECTED"
    assert per["reproduced_target_state"] == "KEEP_FOR_RESEARCH"
    assert per["gate_reproduced"] is False


def test_e11_mixed_rejected_and_data_hold_is_no_defensible_alpha():
    cfg = A.load_gate_cfg()
    rows = [_cand("c1", metrics=_metrics(rank_ic_t=0.3), canonical_target_state="REJECTED"),
            _cand("c2", metrics=_metrics(scored_periods=5), canonical_target_state="DATA_HOLD")]
    g = K.evaluate_alpha_strength(candidates=rows, classify_fn=_real_classify(), gate_cfg=cfg)
    assert g["outcome"] == "NO_DEFENSIBLE_ALPHA"
    assert g["counts"]["rejected"] == 1
    assert g["counts"]["data_hold"] == 1
    assert g["gate_reproduced_all"] is True


def test_e12_only_data_hold_is_data_hold():
    cfg = A.load_gate_cfg()
    rows = [_cand("c1", metrics=_metrics(scored_periods=5), canonical_target_state="DATA_HOLD")]
    g = K.evaluate_alpha_strength(candidates=rows, classify_fn=_real_classify(), gate_cfg=cfg)
    assert g["outcome"] == "DATA_HOLD"


# =========================================================================== #
# F. SAFETY
# =========================================================================== #
def test_f01_kernel_safety_all_false():
    s = K._safety()
    for k in ("created_orders", "created_fills", "changed_holdings", "changed_nav",
              "promoted_model", "recalibrated_model", "replaced_champion",
              "performed_broker_execution", "enabled_cadence", "purchased_data",
              "activated_paid_provider", "wrote_to_operational_ledger"):
        assert s[k] is False
    assert s["automation_off"] is True


def test_f02_mandate_forbidden_actions():
    m = K.build_mandate(normalized_opportunity=_normalized())
    for a in ("CREATE_ORDERS", "AUTO_PROMOTE_MODEL", "ENABLE_CADENCE", "PURCHASE_DATA",
              "ACTIVATE_PAID_DATA_PROVIDER", "MUTATE_OPERATIONAL_PORTFOLIO"):
        assert a in m["forbidden_actions"]
    assert m["requirements"]["paid_data_permitted"] is False
    assert m["requirements"]["owned_data_only"] is True


def test_f03_reassess_never_promotes():
    no = _normalized()
    env = K.build_result_envelope(
        mandate=K.build_mandate(normalized_opportunity=no),
        alpha_strength={"outcome": "READY_FOR_MANUAL_REVIEW", "counts": {}},
        campaign={}, candidate_evidence=[])
    r = K.reassess(normalized_opportunity=no, envelope=env)
    assert r["champion_changed"] is False
    assert r["automatic_model_promotion_allowed"] is False
    assert r["manual_model_review_required"] is True


def test_f04_read_model_governance(tmp_path):
    p = A.load_research_bridge(bridge_dir=tmp_path)
    assert p["governance"]["champion_changed"] is False
    assert p["governance"]["automatic_model_promotion_allowed"] is False
    assert p["governance"]["manual_model_review_required"] is True


# =========================================================================== #
# G. UI / READ MODEL
# =========================================================================== #
def test_g01_not_run_before_any_mandate(tmp_path):
    p = A.load_research_bridge(bridge_dir=tmp_path)
    assert p["state"] == "NOT_RUN"


def test_g02_read_model_after_full_cycle(tmp_path):
    no = _normalized()
    rec = A.build_and_persist_mandate(normalized_opportunity=no, active_book_id="b",
                                      bridge_dir=tmp_path)
    A.dispatch_mandate(mandate=rec["mandate"], active_book_id="b", queue=FakeQueue(),
                       bridge_dir=tmp_path)
    A.ingest_result(mandate=rec["mandate"], candidate_rows=[_cand(metrics=_metrics(rank_ic_t=0.3))],
                    campaign={"entrypoint": "unit", "terminal_token": "NO_DEFENSIBLE_ALPHA",
                              "experiments_executed": 2},
                    active_book_id="b", bridge_dir=tmp_path)
    p = A.load_research_bridge(active_book_id="b", eligible_market_date="2026-08-07",
                              bridge_dir=tmp_path)
    assert p["state"] == "COMPLETED"
    assert p["alpha_discovery"]["outcome"] == "NO_DEFENSIBLE_ALPHA"
    assert p["reassessment"]["verdict"] == "NO_DEFENSIBLE_ALPHA"
    assert p["research_opportunity"]["opportunity_id"] == "investigate_rank_ic_degradation"
    assert p["mandate"]["horizons"]["forward_label_horizons"] == [20, 63]


def test_g03_read_model_supports_manual_review_state(tmp_path):
    no = _normalized()
    rec = A.build_and_persist_mandate(normalized_opportunity=no, active_book_id="b",
                                      bridge_dir=tmp_path)
    A.ingest_result(mandate=rec["mandate"],
                    candidate_rows=[_cand(champion_correlation=0.3, forward_observation_count=30)],
                    campaign={"entrypoint": "unit"}, active_book_id="b",
                    bridge_dir=tmp_path)
    p = A.load_research_bridge(active_book_id="b", eligible_market_date="2026-08-07",
                              bridge_dir=tmp_path)
    assert p["alpha_discovery"]["outcome"] == "READY_FOR_MANUAL_REVIEW"
    assert p["alpha_discovery"]["requires_manual_model_review"] is True


def test_g04_ui_wires_bridge_route_and_loader():
    html = UI.read_text(encoding="utf-8", errors="replace")
    assert "/v1/research/research-bridge" in html
    assert "loadResearchBridge" in html


def test_g05_compact_summary(tmp_path):
    no = _normalized()
    rec = A.build_and_persist_mandate(normalized_opportunity=no, active_book_id="b",
                                      bridge_dir=tmp_path)
    A.ingest_result(mandate=rec["mandate"], candidate_rows=[_cand(metrics=_metrics(rank_ic_t=0.3))],
                    campaign={"entrypoint": "unit"}, active_book_id="b", bridge_dir=tmp_path)
    s = A.compact_summary(active_book_id="b", eligible_market_date="2026-08-07",
                          bridge_dir=tmp_path)
    assert s["research_bridge_outcome"] == "NO_DEFENSIBLE_ALPHA"
    assert s["manual_model_review_required"] is True


# =========================================================================== #
# H. RECOVERY
# =========================================================================== #
def test_h01_stale_recovery_reuses_queue(tmp_path):
    q = FakeQueue()
    q.stale = 3
    out = A.recover_stale_dispatch(queue=q)
    assert out["reclaimed"] == 3
    assert "requeue_stale" in out["recovery_owner"]


def test_h02_dispatch_survives_process_restart(tmp_path):
    no = _normalized()
    rec = A.build_and_persist_mandate(normalized_opportunity=no, active_book_id="b",
                                      bridge_dir=tmp_path)
    A.dispatch_mandate(mandate=rec["mandate"], active_book_id="b", queue=FakeQueue(),
                       bridge_dir=tmp_path)
    # Fresh read (simulated restart) sees the durable dispatch record.
    reloaded = A.load_latest_mandate_record(active_book_id="b",
                                            eligible_market_date="2026-08-07",
                                            bridge_dir=tmp_path)
    assert reloaded["dispatch"]["idempotent"] is True
    assert reloaded["lifecycle_state"] == "DISPATCHED"


def test_h03_second_dispatch_no_duplicate_after_restart(tmp_path):
    no = _normalized()
    rec = A.build_and_persist_mandate(normalized_opportunity=no, active_book_id="b",
                                      bridge_dir=tmp_path)
    q = FakeQueue()
    A.dispatch_mandate(mandate=rec["mandate"], active_book_id="b", queue=q, bridge_dir=tmp_path)
    # Re-load mandate from disk and dispatch again -> same dedupe key, no new job.
    reloaded = A.load_latest_mandate_record(active_book_id="b",
                                            eligible_market_date="2026-08-07",
                                            bridge_dir=tmp_path)
    A.dispatch_mandate(mandate=reloaded["mandate"], active_book_id="b", queue=q,
                       bridge_dir=tmp_path)
    assert len(q.jobs) == 1


# =========================================================================== #
# I. OPERATIONAL-LEDGER OWNERSHIP (Part 3) — the ONE canonical fingerprint owner
# =========================================================================== #
SCRIPT_SRC = (ROOT / "scripts" / "run_research_bridge_campaign.py").read_text(encoding="utf-8")


def test_i01_campaign_script_has_no_operational_ledger_literal():
    # The campaign script must not hard-code any operational-ledger path or walk it.
    assert ".paper_trader" not in SCRIPT_SRC
    assert "Path.home()" not in SCRIPT_SRC
    assert "_ledger_fingerprints" not in SCRIPT_SRC   # the removed duplicate implementation
    assert "rglob" not in SCRIPT_SRC                  # no bridge-local root walk


def test_i02_campaign_script_reuses_canonical_fingerprint_owner():
    assert "fingerprint_operational_ledgers" in SCRIPT_SRC
    assert "run_mandate_campaign" in SCRIPT_SRC       # drives the real execution path


def test_i03_owner_fingerprint_delegates_to_runtime():
    from paper_trader.alpha_agent import runtime as rt
    # api owner delegates to the ONE canonical owner and hard-codes no ledger path.
    assert "rt.fingerprint_ledgers(cfg)" in OWNER_SRC
    assert ".paper_trader" not in OWNER_SRC
    cfg = json.loads((ROOT / "configs" / "alpha_agent" /
                      "stage5_experiment_factory.json").read_text(encoding="utf-8"))
    canonical = rt.fingerprint_ledgers(cfg)
    reused = A.fingerprint_operational_ledgers()
    assert reused == canonical                        # exact reuse (same map)


def test_i04_fingerprint_before_equals_after_when_unchanged():
    a = A.fingerprint_operational_ledgers()
    b = A.fingerprint_operational_ledgers()
    assert a == b                                     # deterministic; nothing mutated


# =========================================================================== #
# J. TRUE END-TO-END EXECUTION (Part 4) — reuse the shared AlphaAgent machinery.
#    A REAL ResearchQueue (tmp sqlite) proves genuine claim/attempts/settle; a fake
#    tournament registry + fake CAT_EXPERIMENT handler keep it hermetic (no Norgate
#    panel / real tournament DB). Nothing operational is touched.
# =========================================================================== #
_COMPUTABLE = "trend_slope_t"     # a real alpha_agent.price_factors.COMPUTABLE_FEATURES key


class _FakeRegistry:
    """Minimal CandidateRegistry stand-in: list() + spec-dedupe + latest_evaluation."""

    def __init__(self, candidates, evals=None):
        self._c = list(candidates)
        self._gen = set()
        self._evals = dict(evals or {})

    def list(self, *, state=None, limit=None):
        return list(self._c)

    def try_register_generated(self, *, strategy, spec, candidate_id):
        key = (strategy, json.dumps(spec, sort_keys=True), candidate_id)
        if key in self._gen:
            return None                                # deterministic dedupe (idempotent)
        self._gen.add(key)
        return "sh_%d" % len(self._gen)

    def latest_evaluation(self, cid):
        return self._evals.get(cid)


def _real_queue(tmp_path):
    from paper_trader.alpha_agent import autonomous_research as ar
    return ar.ResearchQueue(str(tmp_path / "autonomy_unit.sqlite"))


def _reg_candidates():
    return [{"candidate_id": "cand_price", "family": "price_momentum",
             "lifecycle_state": "REJECTED",
             "spec": {"feature": _COMPUTABLE, "horizon_days": 63, "rebalance": "monthly"}}]


def _price_mandate():
    return K.build_mandate(normalized_opportunity=_normalized())


def test_j01_mandate_permits_owned_price():
    m = _price_mandate()
    assert "price_momentum" in m["allowed_hypothesis_families"]
    assert A._mandate_permits_owned_price(m) is True
    # a purely-fundamental mandate does not admit the owned computable-price catalogue
    assert A._mandate_permits_owned_price(
        {"allowed_hypothesis_families": ["valuation"]}) is False


def test_j02_generate_enqueues_real_bridge_experiment(tmp_path):
    from paper_trader.alpha_agent import autonomous_research as ar
    q = _real_queue(tmp_path)
    reg = _FakeRegistry(_reg_candidates())
    gen = A.generate_mandate_experiments(mandate=_price_mandate(), registry=reg, queue=q,
                                         max_experiments=4)
    assert len(gen) == 1
    job = q.get(gen[0]["job_id"])
    assert job.category == ar.CAT_EXPERIMENT
    assert job.origin == A.BRIDGE_ORIGIN == "research-bridge"
    assert job.lane == A.BRIDGE_EXPERIMENT_LANE
    assert job.payload["tournament"] is True
    assert job.payload["feature"] == _COMPUTABLE
    assert job.payload["research_bridge_mandate_id"] == _price_mandate()["mandate_id"]


def test_j03_generate_is_idempotent_for_same_mandate(tmp_path):
    q = _real_queue(tmp_path)
    reg = _FakeRegistry(_reg_candidates())
    m = _price_mandate()
    g1 = A.generate_mandate_experiments(mandate=m, registry=reg, queue=q)
    g2 = A.generate_mandate_experiments(mandate=m, registry=reg, queue=q)
    assert len(g1) == 1 and len(g2) == 0              # dedupe: no duplicate experiment
    assert q.depth() == 1


def test_j03b_distinct_horizon_studies_get_distinct_jobs(tmp_path):
    # Two registered studies for the SAME feature at DIFFERENT horizons are distinct
    # experiments and MUST get distinct queue jobs (dedupe_key includes the horizon).
    q = _real_queue(tmp_path)
    reg = _FakeRegistry([
        {"candidate_id": "a", "family": "price_momentum", "lifecycle_state": "REJECTED",
         "spec": {"feature": _COMPUTABLE, "horizon_days": 21, "rebalance": "monthly"}},
        {"candidate_id": "b", "family": "price_momentum", "lifecycle_state": "REJECTED",
         "spec": {"feature": _COMPUTABLE, "horizon_days": 63, "rebalance": "monthly"}},
    ])
    gen = A.generate_mandate_experiments(mandate=_price_mandate(), registry=reg, queue=q,
                                         max_experiments=8)
    assert len(gen) == 2
    assert len({g["job_id"] for g in gen}) == 2       # no collision across horizons
    assert q.depth() == 2


def test_j04_generate_noop_for_non_price_mandate(tmp_path):
    q = _real_queue(tmp_path)
    reg = _FakeRegistry(_reg_candidates())
    m = dict(_price_mandate())
    m["allowed_hypothesis_families"] = ["valuation", "growth_investment"]
    assert A.generate_mandate_experiments(mandate=m, registry=reg, queue=q) == []
    assert q.depth() == 0


def _fake_handlers():
    from paper_trader.alpha_agent import autonomous_research as ar

    def _h(job):
        return ar.OUTCOME_COMPLETED, {
            "real_work": "unit_price_experiment", "feature": job.payload.get("feature"),
            "row": {"rank_ic_t": 0.4, "scored_periods": 20, "rank_ic": 0.01,
                    "net25_spread": -0.001}}
    return {ar.CAT_EXPERIMENT: _h}


def test_j05_drain_is_bridge_exclusive_and_claims_only_revalidations(tmp_path):
    from paper_trader.alpha_agent import autonomous_research as ar
    q = _real_queue(tmp_path)
    reg = _FakeRegistry(_reg_candidates())
    m = _price_mandate()
    gen = A.generate_mandate_experiments(mandate=m, registry=reg, queue=q)
    assert len(gen) == 1
    # Also enqueue the inert governance MARKER (shorter lane) and a FOREIGN tournament
    # job — neither must be claimed by the bridge-scoped operator drain.
    q.enqueue(ar.CAT_EXPERIMENT, lane="research_bridge.experiment",
              payload={"mandate_id": m["mandate_id"]}, origin=A.BRIDGE_ORIGIN,
              dedupe_key="gov_marker")
    q.enqueue(ar.CAT_EXPERIMENT, lane="tournament.stage9_4_revalidation",
              payload={"tournament": True, "feature": _COMPUTABLE},
              origin="stage9-tournament", dedupe_key="foreign_job")
    report = A.drain_mandate_experiments(queue=q, handlers=_fake_handlers(), mandate=m)
    assert report["jobs_claimed"] == 1                 # ONLY the bridge revalidation
    assert report["jobs_completed"] == 1
    # governance marker + foreign job untouched (still runnable).
    assert q.runnable_depth() == 2
    job = q.get(gen[0]["job_id"])
    assert job.state == ar.STATE_COMPLETED
    assert job.attempts == 1                            # genuine single claim/execution


def test_j06_completed_and_census_are_mandate_scoped(tmp_path):
    from paper_trader.alpha_agent import autonomous_research as ar
    q = _real_queue(tmp_path)
    reg = _FakeRegistry(_reg_candidates())
    m = _price_mandate()
    A.generate_mandate_experiments(mandate=m, registry=reg, queue=q)
    A.drain_mandate_experiments(queue=q, handlers=_fake_handlers(), mandate=m)
    completed = A._completed_mandate_jobs(queue=q, mandate_id=m["mandate_id"])
    assert len(completed) == 1
    assert completed[0]["feature"] == _COMPUTABLE
    assert completed[0]["result"]["rank_ic_t"] == 0.4
    census = A._mandate_job_census(queue=q, mandate_id=m["mandate_id"])
    assert census["generated"] == 1
    assert census["claimed"] == 1                       # attempts>=1 == executed
    assert census["completed"] == 1
    # a different mandate id sees NONE of this mandate's jobs
    assert A._mandate_job_census(queue=q, mandate_id="rbm_other")["generated"] == 0


def test_j07_fresh_candidate_rows_read_from_registry_eval():
    reg = _FakeRegistry(
        _reg_candidates(),
        evals={"cand_price": {"evaluated_at": "2026-08-07T00:00:00Z",
                              "metrics": {"rank_ic_t": 0.4, "scored_periods": 20,
                                          "corr_vs_champion": 0.3, "horizon_days": 63}}})
    rows = A._fresh_candidate_rows(registry=reg, candidate_ids=["cand_price"])
    assert len(rows) == 1
    assert rows[0]["candidate_id"] == "cand_price"
    assert rows[0]["rank_ic_t"] == 0.4
    assert rows[0]["champion_correlation"] == 0.3
    assert rows[0]["canonical_target_state"] == "REJECTED"


def test_j08_ingest_mandate_experiments_empty_is_zero():
    out = A.ingest_mandate_experiments(registry=None, cfg9={}, completed=[])
    assert out["imported"] == 0 and out["skipped"] == 0


def test_j09_execution_counts_are_not_len_of_all_rows():
    # Source-level guard: experiments_executed is the mandate-scoped CLAIMED count,
    # never len(all pre-existing registry rows) — the exact defect being repaired.
    assert '"experiments_executed": census["claimed"]' in OWNER_SRC
    assert '"prior_candidates_considered": prior_candidates' in OWNER_SRC
    assert '"experiments_completed": census["completed"]' in OWNER_SRC
    # the campaign script no longer equates executed to len(rows)
    assert "len(rows)" not in SCRIPT_SRC
    assert '"experiments_executed": len(' not in SCRIPT_SRC


def test_j10_scheduled_cadence_never_admits_bridge_origin():
    # The bridge origin/lane must NOT be in the scheduled collect-drain allowlist, so a
    # dispatched bridge job stays inert under cadence (only an explicit operator drain runs it).
    cd = json.loads((ROOT / "configs" / "alpha_agent" / "stage8_autonomy.json")
                    .read_text(encoding="utf-8"))["autonomy"]["collect_drain"]
    assert A.BRIDGE_ORIGIN not in cd["allowed_origins"]
    assert not any(str(p).startswith("research_bridge")
                   for p in cd["allowed_lane_prefixes"])
    assert "run_mandate_campaign" in OWNER_SRC          # the operator-run entrypoint exists
