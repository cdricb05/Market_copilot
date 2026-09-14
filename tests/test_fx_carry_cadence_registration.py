"""The human-authorised TRUE_FORWARD registration of the frozen FX carry cadence record.

``ALPHA_RECOVERY_FX_CARRY_CADENCE_H1_F9B1ACA7`` is an immutable forward-package
file, not a ResearchMemory freeze. These tests prove the door that registers it:

* the record is re-verified by BOTH of its hashes and by its frozen cadence, and a
  tampered or re-parameterised record is refused, never repaired;
* the instrument universe is read from the R64 owner record of the same dataset;
* registration goes through the ONE governed adoption owner, exactly once, with
  zero backfill and zero forward observations;
* a repeat never duplicates it, and a different identity under the same id is refused;
* nothing is written without the confirmation token AND --execute;
* no portfolio effect, no promotion, and no application import in the research module.
"""
from __future__ import annotations

import ast
import importlib.util
import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from alpha_agent import r64 as R64  # noqa: E402
from alpha_agent.alpha_recovery import RESEARCH_ROOT_ENV, stable_hash  # noqa: E402
from alpha_agent.alpha_recovery import fx_carry_cadence_challenger as FXC  # noqa: E402
from paper_trader.api import canonical_forward_accrual as CFA  # noqa: E402
from paper_trader.api import forward_challenger_registry as FCR  # noqa: E402
from paper_trader.api import prospective_adoption as PA  # noqa: E402

SCRIPT = _ROOT / "scripts" / "register_fx_carry_cadence_challenger.py"
UNIVERSE = ["6A", "6B", "6C", "6E", "6J", "6M", "6N", "6S", "DX"]


def _load_script():
    spec = importlib.util.spec_from_file_location("_fx_carry_registration", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _spec(**over) -> dict:
    s = {"cell_id": FXC.FROZEN_CELL_ID, "construction": {},
         "costs": "R38 per-market cost per side on one-way turnover",
         "emission_rule": "decision at close t from data <= t, effective close t+1, no backfill",
         "horizon_sessions": 1, "mode": "XS", "no_trade_band": 0.25, "scope": "FX_FUTURES",
         "signal": {"added_dimension": "CARRY", "baseline": ["PRICE_RETURN_STATE"], "kind": "AUGMENTATION"},
         "trade_every_sessions": 5, "universe": {"instruments": None, "rule": "fixture"}}
    s.update(over)
    return s


def _record(spec: dict | None = None, **over) -> dict:
    spec = spec or _spec()
    body = {"asset_class": "FX_FUTURES", "campaign_id": "alpha_recovery_offensive_v1",
            "cell_id": FXC.FROZEN_CELL_ID, "challenger_id": FXC.CHALLENGER_ID,
            "classification": "ECONOMIC_UNDER_CONTROLS_NOT_FDR", "forward_specification": spec,
            "freeze_record_hash": stable_hash(spec), "holdings_changed": False, "horizon_sessions": 1,
            "information_identity": {"family": "FX_CARRY_CADENCE", "dimension": "CARRY", "baseline": []},
            "kind": "CROSS_DOMAIN_SLEEVE", "live_registration_performed": False,
            "promotion_allowed": False, "qualification_evidence": {}, "records_are_immutable": True,
            "schema": "alpha_recovery_forward_candidate/1",
            "why": "cadence verdict ECONOMIC_UNDER_CONTROLS_NOT_FDR"}
    body.update(over)
    body["record_hash"] = stable_hash(body)
    return body


def _write_estate(tmp_path: Path, monkeypatch, rec: dict, *, universe=None, cell_n=9) -> dict:
    ar, r64 = tmp_path / "alpha_recovery", tmp_path / "r64"
    monkeypatch.setenv(RESEARCH_ROOT_ENV, str(ar))
    monkeypatch.setenv(R64.RESEARCH_ROOT_ENV, str(r64))
    monkeypatch.setattr(FXC, "RECORD_HASH", rec["record_hash"])
    monkeypatch.setattr(FXC, "FREEZE_RECORD_HASH", rec["freeze_record_hash"])
    monkeypatch.setattr(FXC, "RECORD_FILE", "%s_%s.json" % (FXC.CHALLENGER_ID, rec["record_hash"][:12]))
    (ar / "challengers").mkdir(parents=True)
    (ar / "challengers" / FXC.RECORD_FILE).write_text(json.dumps(rec), encoding="utf-8")
    (ar / "cells").mkdir(parents=True)
    (ar / "cells" / FXC.CADENCE_CELL_FILE).write_text(json.dumps({"n_instruments": cell_n}),
                                                      encoding="utf-8")
    (r64 / "challengers").mkdir(parents=True)
    (r64 / "challengers" / "R64_FX_FUTURES_CARRY_H1_E82A5C66_fixture.json").write_text(
        json.dumps({"universe": {"instruments": list(universe or UNIVERSE), "rule": "fixture"}}),
        encoding="utf-8")
    return {"registry": tmp_path / "registry", "adoption": tmp_path / "adoption"}


@pytest.fixture
def estate(tmp_path, monkeypatch):
    return _write_estate(tmp_path, monkeypatch, _record())


def _reg_files(dirs: dict) -> list:
    d = Path(dirs["registry"]) / "registrations"
    return sorted(d.glob("*.json")) if d.exists() else []


# --------------------------------------------------------------------------- #
# The exact frozen identity
# --------------------------------------------------------------------------- #
def test_the_live_record_is_the_pinned_identity_with_the_r64_universe():
    if not (FXC.record_dir() / FXC.RECORD_FILE).exists():
        pytest.skip("the frozen forward-package record is not on this machine")
    res = FXC.resolve()
    assert res["ok"], res["problems"]
    assert res["record"]["record_hash"] == FXC.RECORD_HASH
    assert res["universe"]["instruments"] == UNIVERSE
    row = FXC.freeze_row(res)
    assert row["forward_challenger"] == {"challenger_id": FXC.CHALLENGER_ID,
                                         "record_hash": FXC.FREEZE_RECORD_HASH,
                                         "inception": FXC.INCEPTION}
    assert row["spec_json"]["trade_every_sessions"] == 5 and row["spec_json"]["no_trade_band"] == 0.25


def test_a_tampered_record_is_refused_never_repaired(estate):
    path = FXC.record_dir() / FXC.RECORD_FILE
    rec = json.loads(path.read_text(encoding="utf-8"))
    rec["forward_specification"]["no_trade_band"] = 0.5
    path.write_text(json.dumps(rec), encoding="utf-8")
    res = FXC.resolve()
    assert not res["ok"]
    assert "SPECIFICATION_DOES_NOT_HASH_TO_THE_FREEZE_RECORD_HASH" in res["problems"]
    assert "RECORD_BYTES_DO_NOT_HASH_TO_THE_RECORDED_HASH" in res["problems"]
    with pytest.raises(ValueError):
        FXC.freeze_row(res)


def test_the_frozen_cadence_is_read_back_and_checked_never_chosen(tmp_path, monkeypatch):
    rec = _record(_spec(trade_every_sessions=1))
    _write_estate(tmp_path, monkeypatch, rec)
    problems = FXC.resolve()["problems"]
    assert any(p.startswith("FROZEN_TRADE_EVERY_SESSIONS_MISMATCH") for p in problems)


def test_the_universe_comes_from_the_r64_owner_and_must_agree_with_the_cadence_cell(tmp_path, monkeypatch):
    _write_estate(tmp_path, monkeypatch, _record(), cell_n=8)
    problems = FXC.resolve()["problems"]
    assert any(p.startswith("UNIVERSE_SIZE_DISAGREES_WITH_THE_CADENCE_CELL") for p in problems)


def test_the_model_family_is_the_cadence_owner_spelling():
    src = (_ROOT / "alpha_agent" / "alpha_recovery" / "cadence.py").read_text(encoding="utf-8")
    assert '"XS_LONG_SHORT_RISK_CONTROLLED_CADENCE_%d" % trade_every' in src
    assert FXC.MODEL_FAMILY == "XS_LONG_SHORT_RISK_CONTROLLED_CADENCE_%d" % 5


# --------------------------------------------------------------------------- #
# Registration: once, prospective, no backfill
# --------------------------------------------------------------------------- #
def test_registers_once_through_the_governed_owner_with_zero_backfill(estate):
    mod = _load_script()
    out = mod.register(confirm=PA.ADOPT_CONFIRM_TOKEN, registry_dir_override=estate["registry"],
                       adoption_dir_override=estate["adoption"])
    assert out["outcome"] == PA.ADOPTED and out["adopted"] and out["owner_called"]
    files = _reg_files(estate)
    assert len(files) == 1
    rec = json.loads(files[0].read_text(encoding="utf-8"))
    assert rec["challenger_id"] == FXC.CHALLENGER_ID
    assert rec["backfilled"] is False
    assert rec["forward_observations_at_registration"] == 0
    assert rec["predictions_emitted"] == 0 and rec["matured_observations"] == 0
    assert rec["freeze_record_hash"] == FXC.FREEZE_RECORD_HASH
    assert rec["instrument_scope"] == UNIVERSE
    assert rec["identity"]["release"] == "ALPHA_RECOVERY_OFFENSIVE"
    assert rec["challenger_class"] == PA.CLASS_SIGNAL_CANONICAL
    proof = mod.proof(out, estate["registry"])
    assert proof["FX_TRUE_FORWARD_REGISTERED"] == "YES"
    assert proof["FX_BACKFILL"] == 0
    assert proof["FX_FORWARD_OBSERVATIONS_AT_INCEPTION"] == 0


def test_a_second_run_resolves_to_the_same_registration_and_calls_nothing(estate):
    mod = _load_script()
    kw = dict(confirm=PA.ADOPT_CONFIRM_TOKEN, registry_dir_override=estate["registry"],
              adoption_dir_override=estate["adoption"])
    first = mod.register(**kw)
    second = mod.register(**kw)
    assert first["identity_hash"] == second["identity_hash"]
    assert second["outcome"] == mod.D_ALREADY_REGISTERED and second["owner_called"] is False
    assert len(_reg_files(estate)) == 1
    assert mod.proof(second, estate["registry"])["FX_DUPLICATE_REGISTRATION"] == "NO"


def test_a_different_identity_under_the_same_challenger_id_is_refused(estate, monkeypatch):
    mod = _load_script()
    kw = dict(confirm=PA.ADOPT_CONFIRM_TOKEN, registry_dir_override=estate["registry"],
              adoption_dir_override=estate["adoption"])
    mod.register(**kw)
    monkeypatch.setattr(FXC, "VENUE", "SOMEWHERE_ELSE")
    again = mod.register(**kw)
    assert again["outcome"] == mod.D_REFUSED_DUPLICATE
    assert again["owner_called"] is False and again["adopted"] is False
    assert len(_reg_files(estate)) == 1


def test_nothing_is_written_without_the_token_and_execute(estate):
    mod = _load_script()
    dirs = ["--registry-dir", str(estate["registry"]), "--adoption-dir", str(estate["adoption"])]
    assert mod.main(dirs) == 0                     # dry run
    assert mod.main(["--execute"] + dirs) == 1     # no token
    assert mod.main(["--confirm", PA.ADOPT_CONFIRM_TOKEN] + dirs) == 0   # token without --execute = dry run
    assert _reg_files(estate) == []
    assert not Path(estate["adoption"]).exists()


def test_the_observation_clock_opens_strictly_after_registration(estate):
    mod = _load_script()
    mod.register(confirm=PA.ADOPT_CONFIRM_TOKEN, registry_dir_override=estate["registry"],
                 adoption_dir_override=estate["adoption"])
    rec = json.loads(_reg_files(estate)[0].read_text(encoding="utf-8"))
    clock = rec["observation_clock"]
    assert clock["state"] == FCR.CLOCK_INSTRUMENT_OWNED
    assert clock["sessions_between_inception_and_registration_are_never_synthesised"] is True
    reg_session = rec["registration_session"]
    sessions = ["2026-01-02", reg_session, "2099-01-02", "2099-01-05", "2099-01-06"]
    grid = CFA.decision_grid(rec, sessions=sessions, cadence_sessions=5)
    assert grid and all(s > reg_session for s in grid)


def test_no_portfolio_effect_and_no_promotion(estate):
    mod = _load_script()
    out = mod.register(confirm=PA.ADOPT_CONFIRM_TOKEN, registry_dir_override=estate["registry"],
                       adoption_dir_override=estate["adoption"])
    rec = json.loads(_reg_files(estate)[0].read_text(encoding="utf-8"))
    for k in ("allocated_capital", "changed_holdings", "changed_cash", "changed_nav",
              "created_orders", "created_fills", "promoted_model", "automatic_model_promotion_allowed"):
        assert rec["safety"][k] is False, k
    assert rec["promotion_allowed"] is False and rec["automatic_promotion_allowed"] is False
    proof = mod.proof(out, estate["registry"])
    assert proof["FX_PORTFOLIO_EFFECT"] == "NONE" and proof["FX_AUTO_PROMOTION"] == "NO"
    assert FXC.EVIDENCE_AT_INCEPTION["capital_eligible"] is False


def test_the_research_module_imports_no_application_owner():
    src = Path(FXC.__file__).read_text(encoding="utf-8")
    tree = ast.parse(src)
    names = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names += [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            names.append(node.module or "")
    for bad in ("paper_trader", "api", "prospective_adoption", "forward_challenger_registry",
                "canonical_forward_accrual", "engine"):
        assert not any(n == bad or n.startswith(bad + ".") or n.endswith("." + bad) for n in names), bad
    code = "\n".join(ln for ln in src.splitlines() if not ln.strip().startswith(("#", '"', "'")))
    for call in ("register_forward_challenger(", "adopt_prospective_freeze(", "create_order(",
                 "promote_model("):
        assert call not in code


def test_the_script_calls_the_governed_owner_exactly_once_and_offers_no_backdate():
    src = SCRIPT.read_text(encoding="utf-8")
    assert src.count("adopt_prospective_freeze(") == 1
    for arg in ('"--effective-from"', '"--date"', '"--backfill"', '"--as-of"', '"--since"', '"--all"'):
        assert arg not in src
    assert "--confirm" in src and "--execute" in src and "confirm=" in src
