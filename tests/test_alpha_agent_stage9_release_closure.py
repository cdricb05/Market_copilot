"""Stage 9 release closure - deterministic, hermetic tests for the PRODUCTION
integration: the scheduled AlphaAgent-Collect cycle running the Stage 9
tournament from configuration, canonical registry auto-init, idempotent
completed-experiment ingestion, shadow-book daily advancement, the real report
model wiring and canonical read-only surfaces.

Every store (candidate registry, shadow books, the shared research queue, the
runtime state DB) lives under pytest's ``tmp_path``; no real network, credential,
scheduled task or operational trading ledger is ever touched, and no operational
state is mutated. Covers the 16 WS8 behaviours.
"""
from __future__ import annotations

import copy
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO.parent) not in sys.path:
    sys.path.insert(0, str(_REPO.parent))

from paper_trader.alpha_agent import tournament as T            # noqa: E402
from paper_trader.alpha_agent import runtime as RT              # noqa: E402
from paper_trader.alpha_agent import runtime_contracts as rc    # noqa: E402
from paper_trader.alpha_agent import report_renderer as RR      # noqa: E402
from paper_trader.alpha_agent import autonomous_research as AR  # noqa: E402
from paper_trader.alpha_agent import telegram_control as TC     # noqa: E402

_S9_PATH = _REPO / "configs" / "alpha_agent" / "stage9_tournament.json"
_REAL_S9 = json.loads(_S9_PATH.read_text(encoding="utf-8"))
_REAL_S8_PATH = _REPO / "configs" / "alpha_agent" / "stage8_autonomy.json"


# --------------------------------------------------------------------------- #
# Deterministic Stage 5 result rows / campaign (mirrors the Stage 9 unit tests).
# --------------------------------------------------------------------------- #
def _grid(n25, n50):
    return {"grid": [
        {"cost_bps": 5, "net_annualized_return": (n25 or 0) + 0.01},
        {"cost_bps": 25, "net_annualized_return": n25},
        {"cost_bps": 50, "net_annualized_return": n50}]}


def _row(feature, *, ic_t=3.6, ic=0.03, pos=0.6, sp_t=3.0, net=0.09, n25=0.08,
         n50=0.06, turn=0.4, dd=-0.18, sub=1.0, reg=1.0, erosion=0.15,
         champ_comp=0.9, univ=120, per=48, decision="KEEP_FOR_RESEARCH"):
    return {"feature": feature, "rank_ic_mean": ic, "rank_ic_t": ic_t,
            "rank_ic_positive_ratio": pos, "decile_spread_mean": 0.02,
            "spread_t": sp_t, "oos_ic_mean": ic * 0.8,
            "gross_annualized_return": 0.12, "net_annualized_return": net,
            "turnover": turn, "max_drawdown": dd, "subperiod_consistency": sub,
            "regime_consistency": reg, "cost_erosion_ratio": erosion,
            "cost_flips_sign": (n25 <= 0), "champion_complementarity": champ_comp,
            "universe": univ, "periods": per, "sharpe": 1.1,
            "annualized_vol": 0.1, "beats_null_control": True,
            "spy_excess_annualized": 0.05, "leakage_warning": False,
            "decision": decision, "cost_sensitivity": _grid(n25, n50)}


def _campaign(*rows, held=("sector_neutral_momentum",)):
    return {"results": list(rows), "held": [{"feature": f} for f in held]}


def _fake_campaign(*rows):
    def _fn(_cfg, **_kw):
        return _campaign(*rows)
    return _fn


def _fingerprint(root: Path) -> str:
    h = {}
    for f in sorted(Path(root).rglob("*")):
        if f.is_file():
            h[str(f)] = hashlib.sha256(f.read_bytes()).hexdigest()
    return hashlib.sha256(json.dumps(h, sort_keys=True).encode()).hexdigest()


# --------------------------------------------------------------------------- #
# Hermetic config trio: stage4 -> stage8 -> stage9, all rooted in tmp_path.
# --------------------------------------------------------------------------- #
def _write(path: Path, obj: dict) -> Path:
    path.write_text(json.dumps(obj), encoding="utf-8")
    return path


def _s9_cfg(tmp_path):
    c = copy.deepcopy(_REAL_S9)
    c["tournament_db"] = str(tmp_path / "tournament.sqlite")
    c["shadow_book_root"] = str(tmp_path / "shadows")
    c["shadow_books"]["shadow_book_root"] = str(tmp_path / "shadows")
    return c, _write(tmp_path / "stage9.json", c)


def _s8_cfg(tmp_path, s9_path, *, enabled=True):
    c = {
        "stage8_root": str(tmp_path / "s8root"),
        "autonomy": {"queue_db": str(tmp_path / "autonomy.sqlite"),
                     "max_jobs_per_cycle": 1},
        "tournament": {"enabled": enabled, "config": str(s9_path),
                       "max_candidates_per_cycle": 4},
        "production_handlers": {"panel_date_start": "2016-01-01"},
    }
    return c, _write(tmp_path / "stage8.json", c)


def _s4_cfg(tmp_path, s8_path):
    runtime_root = tmp_path / "runtime"
    ledger = tmp_path / "ledger"
    ledger.mkdir()
    (ledger / "paper_books.json").write_text('{"immutable": true}',
                                             encoding="utf-8")
    return {
        "stage": "4", "runtime_root": str(runtime_root),
        "recipient_email": "x@example.com",
        "stage1_registry_root": str(tmp_path / "registry"),
        "stage2_ingestion_root": str(tmp_path / "ingestion"),
        "stage3_director_root": str(tmp_path / "director"),
        "stage3_5_news_rss_root": str(tmp_path / "news_rss"),
        "stage7_recovery_root": str(tmp_path / "recovery"),
        "operational_ledger_roots": [str(ledger)],
        "cadence": {"max_research_cycles_per_day": 2, "stale_lock_seconds": 900,
                    "heartbeat_stale_seconds": 7200},
        "provider_order": ["claude_code", "anthropic_http"],
        "allowed_task_names": list(rc.ALPHA_AGENT_TASK_NAMES),
        "email": {"credential_dir": str(tmp_path / "no_creds"),
                  "app_credential_file": "gmail_smtp_app_password.dpapi",
                  "smtp_host": "smtp.gmail.com", "smtp_port": 587,
                  "transport": "gmail_smtp", "delivery_provider": "gmail_smtp"},
        "stage_configs": {"stage8_autonomy": str(s8_path)},
    }


def _norm(component, *, status="OK", ok=True):
    return {"component": component, "status": status, "terminal": status,
            "ok": ok, "verified": True, "no_new": False, "run_id": None,
            "run_dir": None, "counts": {}, "metrics": {}, "raw": {}}


class _CollectDrivers(RT.StageDrivers):
    """Deterministic collect drivers - all five steps succeed, nothing else."""

    def verify_stage1(self):
        return _norm(rc.COMPONENT_STAGE1)

    def collect_stage2(self, mode):
        return _norm(rc.COMPONENT_STAGE2)

    def verify_stage2(self):
        return _norm(rc.COMPONENT_STAGE2)

    def collect_stage35(self, mode):
        return _norm(rc.COMPONENT_STAGE35)

    def verify_stage35(self):
        return _norm(rc.COMPONENT_STAGE35)

    def research_stage3(self, mode):
        return _norm(rc.COMPONENT_STAGE3)

    def verify_stage3(self):
        return _norm(rc.COMPONENT_STAGE3)


def _runtime(tmp_path, *, enabled=True):
    _s9, s9p = _s9_cfg(tmp_path)
    _s8, s8p = _s8_cfg(tmp_path, s9p, enabled=enabled)
    cfg = _s4_cfg(tmp_path, s8p)
    r = RT.Runtime(cfg, drivers=_CollectDrivers(),
                   clock=RT.FixedClock(datetime(2026, 7, 29, 18, 30, 0)))
    return r, s9p, s8p


# --------------------------------------------------------------------------- #
# 1. tournament.enabled=false skips the tick.
# --------------------------------------------------------------------------- #
def test_01_disabled_skips_tick(tmp_path, monkeypatch):
    monkeypatch.setattr(T, "build_owned_price_campaign",
                        _fake_campaign(_row("residual_momentum")))
    r, s9p, _ = _runtime(tmp_path, enabled=False)
    res = r.run_collect()
    tt = res.detail["tournament"]
    assert tt["tournament_attempted"] is False
    assert tt["tournament_status"] == "DISABLED"
    # no canonical registry file is created when disabled.
    assert not (tmp_path / "tournament.sqlite").exists()


# --------------------------------------------------------------------------- #
# 2. tournament.enabled=true runs the tick through the scheduled collect path.
# --------------------------------------------------------------------------- #
def test_02_enabled_runs_via_collect(tmp_path, monkeypatch):
    monkeypatch.setattr(T, "build_owned_price_campaign",
                        _fake_campaign(_row("residual_momentum", ic_t=4.0)))
    r, s9p, _ = _runtime(tmp_path, enabled=True)
    res = r.run_collect()
    tt = res.detail["tournament"]
    assert res.terminal == rc.READY           # collection itself succeeded
    assert tt["tournament_attempted"] is True
    assert tt["tournament_status"] == "OK"
    assert tt["candidates_evaluated"] > 0
    # the registry was created and seeded by the scheduled cycle.
    reg = T.CandidateRegistry(tmp_path / "tournament.sqlite")
    assert sum(reg.counts_by_state().values()) == len(
        T.default_candidate_specs())
    reg.close()


# --------------------------------------------------------------------------- #
# 3. Production does not depend on manually passing run_tournament=True.
# --------------------------------------------------------------------------- #
def test_03_no_manual_flag_needed(tmp_path, monkeypatch):
    # (a) run_autonomy_cycle keeps its backwards-compatible default...
    import inspect
    assert inspect.signature(
        RT.run_autonomy_cycle).parameters["run_tournament"].default is False
    # (b) ...yet the tournament runs purely from config (no run_tournament arg).
    monkeypatch.setattr(T, "build_owned_price_campaign",
                        _fake_campaign(_row("residual_momentum", ic_t=3.7)))
    _s9, s9p = _s9_cfg(tmp_path)
    cfg8, _ = _s8_cfg(tmp_path, s9p, enabled=True)
    out = RT.run_autonomy_cycle(cfg8, max_jobs=1)   # no run_tournament passed
    assert "tournament" in out
    assert out["tournament"]["status"] == "OK"


# --------------------------------------------------------------------------- #
# 4. One tournament exception does not stop collection.
# --------------------------------------------------------------------------- #
def test_04_exception_isolated(tmp_path, monkeypatch):
    def _boom(*a, **k):
        raise RuntimeError("synthetic tournament failure")

    monkeypatch.setattr(RT, "run_tournament_tick", _boom)
    r, _, _ = _runtime(tmp_path, enabled=True)
    res = r.run_collect()
    assert res.terminal == rc.READY            # collection unaffected
    tt = res.detail["tournament"]
    assert tt["tournament_status"] == "TOURNAMENT_ERROR"
    assert tt["tournament_error"] and "RuntimeError" in tt["tournament_error"]


# --------------------------------------------------------------------------- #
# 5. Canonical registry initializes automatically on the first cycle.
# --------------------------------------------------------------------------- #
def test_05_registry_autoinit(tmp_path, monkeypatch):
    monkeypatch.setattr(T, "build_owned_price_campaign",
                        _fake_campaign(_row("residual_momentum")))
    r, _, _ = _runtime(tmp_path, enabled=True)
    assert not (tmp_path / "tournament.sqlite").exists()
    r.run_collect()
    assert (tmp_path / "tournament.sqlite").exists()   # created, no manual init


# --------------------------------------------------------------------------- #
# 6. Candidate registry survives restart (durable, resumable).
# --------------------------------------------------------------------------- #
def test_06_registry_survives_restart(tmp_path, monkeypatch):
    monkeypatch.setattr(T, "build_owned_price_campaign",
                        _fake_campaign(_row("residual_momentum")))
    r, _, _ = _runtime(tmp_path, enabled=True)
    r.run_collect()
    reg1 = T.CandidateRegistry(tmp_path / "tournament.sqlite")
    counts = reg1.counts_by_state()
    reg1.close()
    # a second process reopens the SAME registry and sees the same state.
    reg2 = T.CandidateRegistry(tmp_path / "tournament.sqlite")
    assert reg2.counts_by_state() == counts
    assert sum(counts.values()) == len(T.default_candidate_specs())
    reg2.close()


# --------------------------------------------------------------------------- #
# 7. Completed-experiment import is idempotent.
# --------------------------------------------------------------------------- #
def test_07_ingest_idempotent(tmp_path):
    reg = T.CandidateRegistry(tmp_path / "t.sqlite")
    T.seed_families(reg)
    job = {"job_id": "j1", "spec": {"feature": "residual_momentum"},
           "result": _row("residual_momentum", ic_t=3.8)}
    a = T.ingest_completed_experiments(reg, _REAL_S9, completed=[job],
                                       evidence_date="2026-07-29")
    b = T.ingest_completed_experiments(reg, _REAL_S9, completed=[job],
                                       evidence_date="2026-07-29")
    assert a["imported"] == 1 and b["imported"] == 0 and b["skipped"] == 1
    assert reg.processed_count() == 1
    reg.close()


# --------------------------------------------------------------------------- #
# 8. A duplicate completed job is not reprocessed across cycles.
# --------------------------------------------------------------------------- #
def test_08_duplicate_job_not_reprocessed(tmp_path):
    reg = T.CandidateRegistry(tmp_path / "t.sqlite")
    T.seed_families(reg)
    job = {"job_id": "dup", "spec": {"feature": "low_volatility"},
           "result": _row("low_volatility", ic_t=3.1)}
    for _ in range(4):
        T.ingest_completed_experiments(reg, _REAL_S9, completed=[job],
                                       evidence_date="2026-07-29")
    assert reg.processed_count() == 1               # ingested exactly once
    reg.close()


# --------------------------------------------------------------------------- #
# 9. New evidence updates the proper candidate.
# --------------------------------------------------------------------------- #
def test_09_new_evidence_updates_candidate(tmp_path):
    reg = T.CandidateRegistry(tmp_path / "t.sqlite")
    T.seed_families(reg)
    cand = T._candidate_for_feature(reg, "residual_momentum")
    assert cand["lifecycle_state"] == T.PROPOSED
    # a strong completed result moves it OFF proposed and records the evidence.
    T.ingest_completed_experiments(
        reg, _REAL_S9, completed=[{
            "job_id": "e1", "spec": {"feature": "residual_momentum"},
            "result": _row("residual_momentum", ic_t=4.0)}],
        evidence_date="2026-07-29")
    got = reg.get(cand["candidate_id"])
    assert got["lifecycle_state"] == T.KEEP_FOR_RESEARCH
    assert "e1" in got["experiment_ids"]
    assert got["latest_evidence_date"] == "2026-07-29"
    # a genuinely new result on a later date is a NEW hash -> re-ingested.
    out = T.ingest_completed_experiments(
        reg, _REAL_S9, completed=[{
            "job_id": "e2", "spec": {"feature": "residual_momentum"},
            "result": _row("residual_momentum", ic_t=4.2)}],
        evidence_date="2026-07-30")
    assert out["imported"] == 1
    assert reg.get(cand["candidate_id"])["latest_evidence_date"] == "2026-07-30"
    reg.close()


# --------------------------------------------------------------------------- #
# 10. A malformed result is isolated (specific diagnostic, never stops).
# --------------------------------------------------------------------------- #
def test_10_malformed_isolated(tmp_path):
    reg = T.CandidateRegistry(tmp_path / "t.sqlite")
    T.seed_families(reg)
    completed = [
        {"job_id": "bad1", "spec": {}, "result": {}},            # no feature
        {"job_id": "bad2", "spec": {"feature": "residual_momentum"},
         "result": {"feature": "residual_momentum"}},            # no metrics
        {"job_id": "good", "spec": {"feature": "residual_momentum"},
         "result": _row("residual_momentum", ic_t=3.9)},
    ]
    out = T.ingest_completed_experiments(reg, _REAL_S9, completed=completed,
                                         evidence_date="2026-07-29")
    assert out["malformed"] == 2 and out["imported"] == 1
    kinds = {c["kind"] for c in reg.recent_changes(limit=30)}
    assert "EXPERIMENT_INGEST_MALFORMED" in kinds
    reg.close()


# --------------------------------------------------------------------------- #
# 11. Generated work enters the shared canonical queue via the collect cycle.
# --------------------------------------------------------------------------- #
def test_11_generated_work_enters_queue(tmp_path, monkeypatch):
    monkeypatch.setattr(T, "build_owned_price_campaign",
                        _fake_campaign(_row("residual_momentum", ic_t=3.8)))
    r, _, _ = _runtime(tmp_path, enabled=True)
    res = r.run_collect()
    assert res.detail["tournament"]["experiments_generated"] > 0
    # the SAME canonical queue db the config points at holds the new jobs.
    q = AR.ResearchQueue(tmp_path / "autonomy.sqlite")
    assert q.depth() > 0
    job = q.claim_next()
    assert job is not None and job.lane.startswith("tournament.")
    assert job.origin == "stage9-tournament"


# --------------------------------------------------------------------------- #
# 12. Shadow progression is non-retroactive and idempotent.
# --------------------------------------------------------------------------- #
def test_12_shadow_non_retroactive_idempotent(tmp_path):
    cfg = copy.deepcopy(_REAL_S9)
    root = tmp_path / "shadows"
    cfg["shadow_book_root"] = str(root)
    cfg["shadow_books"]["shadow_book_root"] = str(root)
    reg = T.CandidateRegistry(tmp_path / "t.sqlite")
    T.seed_families(reg)
    cand = T._candidate_for_feature(reg, "residual_momentum")
    cid = cand["candidate_id"]
    # stand up one ACTIVE shadow book with an inception snapshot.
    reg.transition(cid, T.TESTING)
    reg.transition(cid, T.KEEP_FOR_RESEARCH)
    book = T.ShadowBook(root, "sb_x")
    book.inception(candidate_id=cid, inception_date="2026-07-29", membership=[],
                   benchmark="SPY", cost_bps=50.0, spec={})
    reg.create_shadow_book(cid, "sb_x", inception_date="2026-07-29", meta={})

    marks = {"2026-07-30": {"nav": 100500.0}}

    def provider(_cid, date):
        return marks.get(date)   # None on a non-market day / missing price

    # advances one immutable mark on a market day.
    a = T.advance_shadow_books(reg, cfg, mark_provider=provider,
                               evidence_date="2026-07-30")
    assert a[0]["status"] == "ADVANCED"
    # same date again -> idempotent no-advance (no duplicate mark).
    b = T.advance_shadow_books(reg, cfg, mark_provider=provider,
                               evidence_date="2026-07-30")
    assert b[0]["status"] == "NO_ADVANCE"
    # a non-market day (no completed close) -> honest coverage diagnostic, NO mark.
    c = T.advance_shadow_books(reg, cfg, mark_provider=provider,
                               evidence_date="2026-08-01")
    assert c[0]["status"] == "DATA_HOLD"
    assert c[0]["blocker"] == T.SHADOW_MARK_MISSING
    assert book.replay()["forward_observations"] == 1   # exactly one true mark
    reg.close()


# --------------------------------------------------------------------------- #
# 13. The ACTUAL report model receives material tournament changes.
# --------------------------------------------------------------------------- #
def test_13_report_model_includes_material_changes(tmp_path, monkeypatch):
    # a retaining cycle writes a NEW_RETAINED_CANDIDATE change to the tmp db.
    s9, s9p = _s9_cfg(tmp_path)
    reg = T.CandidateRegistry(s9["tournament_db"])
    T.run_tournament_cycle(reg, s9, campaign_result=_campaign(
        _row("residual_momentum", ic_t=4.0)), evidence_date="2026-07-29",
        max_candidates=50)
    reg.close()
    # the runtime resolves EXACTLY this stage9 config and run_research attaches
    # its material changes to the model it renders.
    r, run_s9p, _ = _runtime(tmp_path, enabled=True)
    assert Path(r._stage9_config_path()) == run_s9p == s9p
    captured = {}
    real = RR.attach_tournament_changes

    def _spy(model, *, config_path=None, loader=None):
        captured["config_path"] = config_path
        return real(model, config_path=config_path, loader=loader)

    monkeypatch.setattr(RR, "attach_tournament_changes", _spy)
    res = r.run_research(label="morning", send_email=False)
    assert res is not None
    assert captured.get("config_path") == str(s9p)   # wired to canonical path
    # and that path yields a real material line.
    model = RR.attach_tournament_changes({}, config_path=str(s9p))
    lines = RR.tournament_change_lines(model)
    assert any("New retained candidate" in ln for ln in lines)


# --------------------------------------------------------------------------- #
# 14. An unchanged tournament standing suppresses report noise.
# --------------------------------------------------------------------------- #
def test_14_unchanged_suppresses_noise(tmp_path):
    # a registry with only non-material activity yields no report lines.
    s9, s9p = _s9_cfg(tmp_path)
    reg = T.CandidateRegistry(s9["tournament_db"])
    T.seed_families(reg)
    reg.record_change("TOURNAMENT_CYCLE", None, {"evaluated": 0})
    reg.close()
    model = RR.attach_tournament_changes({}, config_path=str(s9p))
    assert RR.tournament_change_lines(model) == []


# --------------------------------------------------------------------------- #
# 15. Telegram / API providers resolve the CANONICAL production registry.
# --------------------------------------------------------------------------- #
def test_15_providers_read_canonical(tmp_path):
    cfg8 = json.loads(_REAL_S8_PATH.read_text(encoding="utf-8-sig"))
    resolved = TC._resolve_stage9_config_path(cfg8)
    assert resolved and Path(resolved) == _S9_PATH
    canonical_db = json.loads(
        Path(resolved).read_text(encoding="utf-8"))["tournament_db"]
    assert canonical_db == _REAL_S9["tournament_db"]
    assert "stage8" in canonical_db.lower()      # the approved research root
    # the read-only providers consume that canonical config without raising.
    prov = TC.build_tournament_providers(config_path=resolved)
    assert isinstance(prov["tournament"](), str)
    assert isinstance(prov["families"](), str)
    # the API route helper resolves the identical canonical config file.
    from paper_trader.api import app as _api
    assert Path(_api._alpha_agent_stage9_config_path()) == _S9_PATH


# --------------------------------------------------------------------------- #
# 16. The collect+tournament cycle leaves the operational ledger byte-identical.
# --------------------------------------------------------------------------- #
def test_16_operational_ledger_unchanged(tmp_path, monkeypatch):
    monkeypatch.setattr(T, "build_owned_price_campaign",
                        _fake_campaign(_row("residual_momentum", ic_t=4.0),
                                       _row("low_volatility", ic_t=2.9)))
    r, _, _ = _runtime(tmp_path, enabled=True)
    ledger = Path(r.cfg["operational_ledger_roots"][0])
    before = _fingerprint(ledger)
    res = r.run_collect()
    assert res.status != "LEDGER_MUTATION_DETECTED"
    assert res.detail["tournament"]["tournament_attempted"] is True
    assert _fingerprint(ledger) == before        # byte-identical


# --------------------------------------------------------------------------- #
# 17. Two candidates that share a feature are BOTH classified in one cycle
# (the per-candidate idempotency key never strands a feature-sharing sibling).
# --------------------------------------------------------------------------- #
def test_17_feature_sharing_candidates_both_classified(tmp_path):
    cfg = copy.deepcopy(_REAL_S9)
    cfg["tournament_db"] = str(tmp_path / "t.sqlite")
    cfg["shadow_book_root"] = str(tmp_path / "sh")
    cfg["shadow_books"]["shadow_book_root"] = str(tmp_path / "sh")
    reg = T.CandidateRegistry(tmp_path / "t.sqlite")
    T.seed_families(reg)
    sharing = [c for c in reg.list()
               if (c.get("spec") or {}).get("feature") == "residual_momentum"]
    assert len(sharing) >= 2   # the catalogue really does share this feature
    T.run_tournament_cycle(reg, cfg, campaign_result=_campaign(
        _row("residual_momentum", ic_t=1.0, sp_t=1.0, sub=0.4, reg=0.4)),
        evidence_date="2026-07-29", max_candidates=200)
    # every candidate sharing the scored feature left PROPOSED (none stranded).
    for c in sharing:
        assert reg.get(c["candidate_id"])["lifecycle_state"] != T.PROPOSED
    reg.close()
