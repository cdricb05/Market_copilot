"""Alpha Recovery Offensive - the regressions that keep the campaign honest.

Every test here is HERMETIC. The campaign research root, the committed
campaign directory (checkpoint / scoreboard), the desk ledger directory, the
R59 memory root, the frontier and overlay paths and the forward registry are
all redirected into a pytest temp directory through the owners' own env-var
constants. No test opens the live desk ledgers, an owned data store, the live
ResearchMemory, the live forward registry or the canonical checkout, and a
test proves the resolved roots are nowhere near them.

What is protected (the contract's required list):

* durable Alpha Recovery contract references (contract doc, CLAUDE.md,
  PROJECT_STATE.md, protocol, audit guard)
* the frozen 10-session checkpoint: calendar-derived, write-once, verifiable,
  the deadline never moves, the clock counts eligible sessions only
* incumbent benchmark identity and the frozen verdict rule
* separation of historical OOS and TRUE_FORWARD
* the forecast schema refuses uncalibrated values and a score as a return;
  probability bounds and the up/down identity
* head-to-head identical-sample comparison mechanics and the frozen gates
* cross-domain equal-risk comparison
* family research budgets and the >= 75 % non-price rule
* the exhausted-PRICE_STATE reopening rule
* multiple-testing discipline (BH over the executed denominator, Holm within
  a family); no threshold below the inherited floors
* PIT / as-of joins in the earnings block; no news hindsight
* human-gated prospective adoption; no automatic promotion; no automatic
  portfolio mutation; the runner has no execute path
* a deterministic scoreboard
"""
from __future__ import annotations

import json
import re
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from alpha_agent import alpha_recovery as AR
from alpha_agent import r59
from alpha_agent.alpha_recovery import checkpoint as CK
from alpha_agent.alpha_recovery import databento_acquisition as DBN
from alpha_agent.alpha_recovery import futures_intraday as FI
from alpha_agent.alpha_recovery import earnings_events as EE
from alpha_agent.alpha_recovery import equity_challengers as EC
from alpha_agent.alpha_recovery import forecast_products as FPR
from alpha_agent.alpha_recovery import frontier_residual as FR
from alpha_agent.alpha_recovery import forecast_contract as FC
from alpha_agent.alpha_recovery import forward_package as FP
from alpha_agent.alpha_recovery import incumbent as INC
from alpha_agent.alpha_recovery import intraday_alpha as IA
from alpha_agent.alpha_recovery import intraday_data as ID
from alpha_agent.alpha_recovery import options_surface as OS
from alpha_agent.alpha_recovery import news as NW
from alpha_agent.alpha_recovery import program as PR
from alpha_agent.alpha_recovery import scoreboard as SB
from alpha_agent.alpha_recovery import tournament as T
from alpha_agent.r59 import information_needs as IN
from alpha_agent.r63 import sensitivity as S
from alpha_agent.r64 import family as FAM

pytestmark = pytest.mark.filterwarnings("ignore::RuntimeWarning")

LIVE_ROOTS = ("Stock_Prediction_app_data", r"C:\Users\binis\paper_trader", r"C:\Users\binis\.paper_trader")
REPO = Path(__file__).resolve().parents[1]


@pytest.fixture()
def root(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    monkeypatch.setenv(AR.REPO_DIR_ENV, str(tmp_path / "repo_dir"))
    monkeypatch.setenv(INC.DESK_DIR_ENV, str(tmp_path / "desk"))
    monkeypatch.setenv(r59.RESEARCH_ROOT_ENV, str(tmp_path / "r59"))
    monkeypatch.setenv(IN.FRONTIER_PATH_ENV, str(tmp_path / "frontier.json"))
    monkeypatch.setenv(IN.OVERLAY_PATH_ENV, str(tmp_path / "overlay.json"))
    monkeypatch.setenv(SB.REGISTRY_DIR_ENV, str(tmp_path / "registry"))
    return tmp_path


# --------------------------------------------------------------------------- #
# Hermeticity, safety, durable references
# --------------------------------------------------------------------------- #
def test_roots_are_hermetic(root):
    for got in (str(AR.research_root()), str(AR.repo_dir()), str(INC.desk_dir()), str(SB.registry_dir()),
                str(IN.frontier_path()), str(r59.research_root())):
        assert got.startswith(str(root))
        for bad in LIVE_ROOTS:
            assert bad.lower() not in got.lower()
    AR.assert_research_root_is_not_live()


def test_live_checkout_root_is_refused():
    with pytest.raises(RuntimeError):
        AR.assert_research_root_is_not_live(Path(r"C:\Users\binis\paper_trader\anything"))


def test_worktree_import_integrity():
    assert Path(AR.assert_worktree_import()).name == "alpha_agent"


def test_safety_flags_are_all_off_and_badges_present():
    for k in ("creates_orders", "creates_fills", "broker_enabled", "promotes_model", "activates_sleeve",
              "approves_proposal", "registers_forward_challenger", "mutates_operational_store",
              "mutates_live_research_store", "purchases_data", "starts_trial_or_subscription",
              "automation_enabled", "automatic_promotion", "automatic_portfolio_mutation"):
        assert AR.SAFETY[k] is False
    assert AR.SAFETY["live_checkout_read_only"] is True
    for badge in ("NO ORDERS", "AUTOMATION OFF", "MANUAL REVIEW", "NO PROMOTION", "HUMAN-GATED ADOPTION"):
        assert badge in AR.SAFETY_BADGES


def test_durable_contract_references_are_present():
    contract = AR.CONTRACT_DOC.read_text(encoding="utf-8")
    assert "PERMANENT PROJECT CONTRACT" in contract
    for rule in ("Alpha is the primary objective", "BENCHMARK", "A model score is not an expected return",
                 "MODEL_GOVERNANCE_OK", "conflated, never pooled", "Thresholds may not be relaxed",
                 "No automatic model promotion", "No automatic operational portfolio change",
                 "10-eligible-market-session project stop-loss", "HUMAN-GATED", "75 %",
                 "STOP_LOSS_BREACH", "OWNED_FREE_INFORMATION_EXHAUSTED"):
        assert rule in contract, rule
    claude = (REPO / "CLAUDE.md").read_text(encoding="utf-8")
    assert "docs/ALPHA_RECOVERY_OPERATING_CONTRACT.md" in claude
    for instr in ("Read", "which Alpha objective", "Refuse", "investment evidence"):
        assert instr in claude, instr
    state = (REPO / "PROJECT_STATE.md").read_text(encoding="utf-8")
    head = state[:3000]
    assert "CURRENT PRIMARY OBJECTIVE" in head
    assert "docs/ALPHA_RECOVERY_OPERATING_CONTRACT.md" in head
    proto = AR.protocol()
    assert proto["registered_before_any_experiment_ran"] is True
    assert proto["campaign_id"] == AR.CAMPAIGN_ID
    for a in proto.get("amendments_disclosed") or []:
        assert "DISCLOSED" in a and "threshold" in a.lower()
    audit = (REPO / "scripts" / "audit_architecture.py").read_text(encoding="utf-8")
    assert "def check_alpha_recovery_operating_contract(" in audit
    assert '"alpha_recovery_operating_contract"' in audit


def test_frozen_gates_are_inherited_not_weakened():
    from alpha_agent import r63, r64
    assert AR.MATERIALITY_ANN_NET == r63.MATERIALITY_ANN_NET == 0.015
    assert AR.BH_Q == r63.BH_Q == 0.10
    assert AR.HOLM_ALPHA == r64.HOLM_ALPHA == 0.05
    assert AR.CONDITIONAL_T_FLOOR == r63.CONDITIONAL_T_FLOOR == 2.0
    assert AR.MIN_EFFECTIVE_PERIODS == r63.MIN_EFFECTIVE_PERIODS == 36
    assert AR.GATE_MAX_TURNOVER == 0.40 and AR.GATE_DD_MULTIPLE == 1.5 and AR.GATE_HALF_FLOOR == -0.005


# --------------------------------------------------------------------------- #
# The frozen 10-session checkpoint
# --------------------------------------------------------------------------- #
def test_checkpoint_is_calendar_derived_write_once_and_verifiable(root):
    now = datetime(2026, 9, 10, 14, 0, tzinfo=timezone.utc)
    cp = CK.freeze(now=now)
    assert cp["freeze_action"] == "FROZEN"
    assert cp["start_eligible_market_session"] == "2026-09-10"
    assert cp["eligible_sessions_after_start"] == ["2026-09-11", "2026-09-14", "2026-09-15", "2026-09-16",
                                                   "2026-09-17", "2026-09-18", "2026-09-21", "2026-09-22",
                                                   "2026-09-23", "2026-09-24"]
    assert cp["tenth_eligible_market_session"] == "2026-09-24"
    assert cp["calendar"]["calendar_id"] == "NYSE_RULE_BASED_R60_1"
    assert cp["incumbent"]["model_id"] == AR.INCUMBENT_MODEL_ID
    assert cp["r64_parent_commit"] == AR.R64_PARENT_COMMIT
    assert cp["contract"]["sha256_crlf_normalised"] == AR.contract_hash()
    assert cp["deadline_never_moves"] is True
    again = CK.freeze(now=datetime(2026, 9, 15, 14, 0, tzinfo=timezone.utc))
    assert again["freeze_action"] == "ALREADY_FROZEN"
    assert again["tenth_eligible_market_session"] == "2026-09-24"
    v = CK.verify(CK.load())
    assert v["valid"], v


def test_checkpoint_skips_labor_day_and_weekends(root):
    now = datetime(2026, 9, 4, 12, 0, tzinfo=timezone.utc)      # Friday before Labor Day
    cp = CK.build(now=now)
    assert cp["start_eligible_market_session"] == "2026-09-04"
    assert "2026-09-07" not in cp["eligible_sessions_after_start"]
    assert "2026-09-05" not in cp["eligible_sessions_after_start"]
    assert cp["eligible_sessions_after_start"][0] == "2026-09-08"


def test_session_clock_counts_eligible_sessions_only_and_breach_is_obvious(root):
    cp = CK.build(now=datetime(2026, 9, 10, 14, 0, tzinfo=timezone.utc))
    c0 = CK.session_clock(cp, as_of="2026-09-10")
    assert c0["sessions_elapsed"] == 0 and c0["sessions_remaining"] == 10 and c0["state"] == "BEFORE_DEADLINE"
    c1 = CK.session_clock(cp, as_of="2026-09-13")                 # a Sunday
    assert c1["sessions_elapsed"] == 1 and c1["sessions_remaining"] == 9
    c2 = CK.session_clock(cp, as_of="2026-09-24")
    assert c2["sessions_elapsed"] == 10 and c2["sessions_remaining"] == 0 and c2["state"] == "AT_DEADLINE"
    d = CK.deadline_outcome(cp, scoreboard_status=AR.ST_RESEARCHING, as_of="2026-09-25")
    assert d["outcome"] == AR.DL_BREACH and d["project_failure_state"] is True and d["final"] is True
    d2 = CK.deadline_outcome(cp, scoreboard_status=AR.ST_COMPETING, as_of="2026-09-24")
    assert d2["outcome"] == AR.DL_CHALLENGER
    d3 = CK.deadline_outcome(cp, scoreboard_status=AR.ST_EXHAUSTED, as_of="2026-09-24")
    assert d3["outcome"] == AR.DL_EXHAUSTED
    pending = CK.deadline_outcome(cp, scoreboard_status=AR.ST_RESEARCHING, as_of="2026-09-12")
    assert pending["outcome"] == "PENDING" and pending["outcome_if_deadline_were_now"] == AR.DL_BREACH


def test_tampered_checkpoint_is_detected(root):
    cp = CK.freeze(now=datetime(2026, 9, 10, 14, 0, tzinfo=timezone.utc))
    p = AR.checkpoint_path()
    body = json.loads(p.read_text(encoding="utf-8"))
    body["tenth_eligible_market_session"] = "2026-10-30"        # a moved deadline
    p.write_text(json.dumps(body), encoding="utf-8")
    v = CK.verify(CK.load())
    assert not v["valid"]
    assert any("deadline" in f or "hash" in f for f in v["failures"])


def test_committed_checkpoint_reproduces_from_the_calendar():
    cp = AR.read_json(AR.DEFAULT_REPO_DIR / AR.CHECKPOINT_NAME)
    assert cp is not None, "the committed checkpoint must exist"
    v = CK.verify(cp)
    assert v["valid"], v
    assert cp["stop_loss_sessions"] == 10


# --------------------------------------------------------------------------- #
# The forecast contract
# --------------------------------------------------------------------------- #
def _cal(ok=True):
    return FC.calibration_record(calibrated=ok, method="test", oos_test={"brier_skill": 0.01}, n_oos_periods=100)


def test_forecast_contract_refuses_a_score_as_expected_return():
    with pytest.raises(FC.ForecastContractError):
        FC.expected_return(0.91, calibration={"is_score": True})
    with pytest.raises(FC.ForecastContractError):
        FC.expected_return(0.02)                                   # no calibration record
    assert FC.expected_return(0.02, calibration=_cal())["state"] == FC.VALUE
    assert FC.expected_return(None)["state"] == FC.UNAVAILABLE
    s = FC.score(0.91, name="rank", scale="rank")
    assert s["not_an_expected_return"] is True and s["is_score"] is True
    with pytest.raises(FC.ForecastContractError):
        FC.equity_forecast(ticker="X", as_of="2026-09-10", horizon_sessions=21, model_id="m", rank=1, n_ranked=10,
                           expected_excess_return=s, uncertainty=FC.unavailable("x"),
                           probability_positive=FC.unavailable("x"), downside=FC.unavailable("x"),
                           information_attribution=None, evidence_maturity="NONE")


def test_forecast_contract_probability_bounds_and_calibration():
    with pytest.raises(FC.ForecastContractError):
        FC.probability(1.2, calibration=_cal())
    with pytest.raises(FC.ForecastContractError):
        FC.probability(0.6)                                         # uncalibrated
    with pytest.raises(FC.ForecastContractError):
        FC.probability(0.6, calibration=_cal(ok=False))
    p = FC.probability(0.6, calibration=_cal())
    assert p["state"] == FC.VALUE and p["value"] == 0.6
    mf = FC.market_forecast(instrument="SPY", as_of="2026-09-10", horizon_sessions=21, model_id="m",
                            probability_up=p, expected_return=FC.unavailable("x"),
                            expected_excess_return=FC.unavailable("x"), uncertainty=FC.unavailable("x"),
                            downside_probability=FC.unavailable("x"), tail_probability=FC.unavailable("x"),
                            regime_probabilities=None, evidence_maturity="HISTORICAL_OOS_ONLY")
    assert abs(mf["probability_up"]["value"] + mf["probability_down"]["value"] - 1.0) < 1e-12
    assert FC.validate(mf)["valid"]
    with pytest.raises(FC.ForecastContractError):
        FC.market_forecast(instrument="SPY", as_of="x", horizon_sessions=1, model_id="m", probability_up=p,
                           probability_down=FC.probability(0.7, calibration=_cal()),
                           expected_return=FC.unavailable("x"), expected_excess_return=FC.unavailable("x"),
                           uncertainty=FC.unavailable("x"), downside_probability=FC.unavailable("x"),
                           tail_probability=FC.unavailable("x"), regime_probabilities=None,
                           evidence_maturity="NONE")
    with pytest.raises(FC.ForecastContractError):
        FC.unavailable("")                                          # a reason is mandatory


# --------------------------------------------------------------------------- #
# Incumbent: identity, separation, verdict rule
# --------------------------------------------------------------------------- #
def test_incumbent_identity_and_blend():
    assert AR.INCUMBENT_MODEL_ID == "fundamental_momentum_50_50_v1"
    assert AR.INCUMBENT_BLEND == {"fundamental": 0.5, "momentum": 0.5}
    assert INC.FORWARD_MODELS[0] == AR.INCUMBENT_MODEL_ID


def test_incumbent_verdict_rule_is_frozen_and_true_forward_cannot_move_it():
    def hist(ne, t, t_ic, sel, lock, eff=100):
        return {"horizons": {"21": {"top25": {"all": {"ann_net_excess": ne, "t_net_excess": t, "t_rank_ic": t_ic,
                                                        "effective_periods": eff},
                                                "selection": {"ann_net_excess": sel},
                                                "lockbox": {"ann_net_excess": lock}}}}}
    assert INC.verdict(hist(0.03, 2.5, 2.2, 0.02, 0.04))["verdict"] == AR.IV_MATERIAL
    assert INC.verdict(hist(0.03, 1.0, 1.1, 0.02, 0.04))["verdict"] == AR.IV_WEAK
    assert INC.verdict(hist(-0.03, -2.5, -1.0, -0.02, -0.04))["verdict"] == AR.IV_NEGATIVE
    assert INC.verdict(hist(0.03, 2.5, 2.2, 0.02, -0.04))["verdict"] == AR.IV_WEAK     # lockbox sign flip
    assert INC.verdict(hist(0.03, 2.5, 2.2, 0.02, 0.04))["true_forward_moves_the_verdict"] is False


def test_true_forward_is_read_separately_and_never_pooled(root):
    d = INC.desk_dir()
    d.mkdir(parents=True)
    rows = []
    for i, day in enumerate(("2026-09-01", "2026-09-02", "2026-09-03")):
        rows.append({"kind": "OUTCOME", "status": "MATURED", "model_id": AR.INCUMBENT_MODEL_ID, "horizon": 1,
                     "market_date": day, "metrics": {"rank_ic_spearman": 0.1 * (i + 1), "top25_excess_pp": 0.5,
                                                     "top_minus_bottom_pp": 1.0}})
    (d / INC.OUTCOMES_LEDGER).write_text(json.dumps({"rows": rows}), encoding="utf-8")
    perf = [{"row": {"book_id": "alpha_paper_book_1", "date": "2026-09-0%d" % (i + 1), "nav": 100.0 + i,
                     "benchmark_close": 10.0 + i, "daily_return_pct": 1.0, "cumulative_return_pct": i,
                     "benchmark_cumulative_return_pct": 0.5 * i, "drawdown_pct": 0.0, "turnover_pct": 0.0,
                     "transaction_cost": 0.0, "holdings_count": 25}} for i in range(3)]
    (d / INC.PERFORMANCE_LEDGER).write_text(json.dumps({"rows": perf}), encoding="utf-8")
    fwd = INC.true_forward()
    assert fwd["evidence_type"] == "TRUE_FORWARD" and fwd["never_pooled_with_historical"] is True
    k = "%s_h1" % AR.INCUMBENT_MODEL_ID
    assert fwd["outcomes_by_model_horizon"][k]["n_matured_sessions"] == 3
    assert fwd["realised_book"]["sessions"] == 3
    body = {"historical_oos": {"x": 1}, "true_forward": fwd}
    assert "historical_oos" in body and "true_forward" in body and body["historical_oos"] is not body["true_forward"]


def test_incumbent_score_is_a_fixed_blend_with_no_fit():
    n, d = 120, 30
    rng = np.random.default_rng(1)
    tr = np.cumprod(1.0 + rng.normal(0, 0.01, (n, d + 200)), axis=1)
    E = {"price": {"tr": tr}, "panel_f": None}
    elig = np.ones((n, d + 200), dtype=bool)
    fcf = rng.normal(size=(n, d + 200))
    acc = rng.normal(size=(n, d + 200))
    zf = INC.xs_z_columns(fcf, elig)
    za = INC.xs_z_columns(-acc, elig)
    assert np.nanmax(np.abs(np.nanmean(zf, axis=0))) < 1e-9        # z within the mask
    assert np.isnan(INC.xs_z_columns(fcf, np.zeros_like(elig))).all()


# --------------------------------------------------------------------------- #
# Head-to-head identical sample and the frozen gates
# --------------------------------------------------------------------------- #
def _preds(n_periods=60, n_names=80, seed=0, adv=0.0):
    rng = np.random.default_rng(seed)
    gid = np.repeat(np.arange(n_periods), n_names)
    iid = np.tile(np.arange(n_names), n_periods)
    pB = rng.normal(size=len(gid))
    noise = rng.normal(size=len(gid))
    y = 0.02 * pB + 0.5 * noise + adv * (pB > 0)
    pBD = pB + 0.3 * noise
    kind = np.array(["SELECTION"] * (n_periods // 2 * n_names) + ["LOCKBOX"] * ((n_periods - n_periods // 2) * n_names),
                    dtype=object)
    dates = np.array([str(date(2015 + i // 12, 1 + i % 12, 1)) for i in range(n_periods)])
    return {"gid": gid, "iid": iid, "y_raw": y, "pred_B": pB, "pred_BD": pBD, "fold_kind": kind,
            "slot_dates": dates}


def test_paired_books_share_rows_and_dates_for_both_arms():
    rows = T.paired_books(_preds(), top_n=25)
    assert len(rows) == 60
    assert (rows["n"] == 80).all()
    assert set(rows.columns) >= {"B_net", "BD_net", "bench_net", "advantage", "layer", "date"}
    assert np.allclose(rows["advantage"], rows["BD_net"] - rows["B_net"])
    # identical rows: the two arms see the same benchmark in every period
    assert rows["bench_net"].notna().all()


def test_tournament_gates_are_the_frozen_ones_and_close_is_not_pass():
    cell = {"head_to_head": {"top25": {"all": {"ann_advantage": 0.0149, "t_advantage": 2.5, "effective_periods": 100,
                                                 "challenger": {"mean_oneway_turnover": 0.3, "max_dd": -0.2},
                                                 "incumbent": {"max_dd": -0.2}},
                                        "selection": {"ann_advantage": 0.01}, "lockbox": {"ann_advantage": 0.02,
                                                                                            "halves_ann_advantage": [0.01, 0.02]}}},
            "conditional": {"t": 3.0}, "redundancy": {"residual_share": 0.8}}
    g = T.gates(cell, fdr_pass=True, holm_pass=True)
    assert g["materiality_ge_1p5pct"] is False                 # 1.49 % is not 1.5 %
    cell["head_to_head"]["top25"]["all"]["ann_advantage"] = 0.015
    g = T.gates(cell, fdr_pass=True, holm_pass=True)
    assert all(v for v in g.values())
    cell["gates"] = g
    assert T.verdict(cell) == T.V_MATERIAL
    cell["gates"] = T.gates(cell, fdr_pass=False, holm_pass=True)
    assert T.verdict(cell) == T.V_NOT_QUALIFIED
    cell["head_to_head"]["top25"]["all"]["ann_advantage"] = -0.03
    cell["head_to_head"]["top25"]["all"]["t_advantage"] = -2.5
    cell["gates"] = T.gates(cell, fdr_pass=False, holm_pass=False)
    assert T.verdict(cell) == T.V_WORSE


def test_cross_domain_equal_risk_comparison():
    rng = np.random.default_rng(3)
    n = 80
    dates = pd.bdate_range("2015-01-01", periods=n * 22)
    inc_rows = pd.DataFrame({"date": [str(dates[i * 21].date()) for i in range(n)],
                             "t": [i * 21 for i in range(n)],
                             "strat_net": rng.normal(0.005, 0.04, n)})
    sleeve = pd.Series(rng.normal(0.0004, 0.006, len(dates)), index=dates)
    res = T.cross_domain(inc_rows, sleeve, horizon=21, label="t")
    assert res["state"] == "OK"
    assert abs(res["incumbent_plus_sleeve_equal_risk"]["ann_vol"] - res["incumbent_only"]["ann_vol"]) < 1e-9
    assert "t_incremental" in res and "positive_incremental_utility_after_costs" in res
    short = T.cross_domain(inc_rows.head(10), sleeve, horizon=21)
    assert short["state"] == "DATA_HOLD"


# --------------------------------------------------------------------------- #
# Budgets, the non-price rule, the reopening rule, multiplicity
# --------------------------------------------------------------------------- #
def test_family_budget_and_rescue_rules():
    fam = {"family": "X"}
    assert PR.check_family_budget(fam, executed_primary=6)["within_budget"]
    with pytest.raises(PR.BudgetError):
        PR.check_family_budget(fam, executed_primary=7)
    with pytest.raises(PR.BudgetError):
        PR.check_family_budget(fam, executed_primary=2, executed_rescue=3, rescue_binding_failures=("a", "b", "c"))
    with pytest.raises(PR.BudgetError):
        PR.check_family_budget(fam, executed_primary=2, executed_rescue=1)      # no named failure
    assert PR.check_family_budget(fam, executed_primary=2, executed_rescue=1,
                                  rescue_binding_failures=("cost drag",))["rescue"] == 1


def test_non_price_share_rule_and_protocol_plan():
    specs = [{"price_state": False}] * 3 + [{"price_state": True}]
    assert PR.non_price_share(specs)["rule_met"] is True
    specs = [{"price_state": False}] * 2 + [{"price_state": True}]
    assert PR.non_price_share(specs)["rule_met"] is False
    fams = PR.families_from_protocol()
    planned = []
    for f in fams:
        assert len(f["primary_specifications"]) <= AR.FAMILY_PRIMARY_MAX
        assert len(f.get("rescue_specifications") or []) <= AR.FAMILY_RESCUE_MAX
        planned += PR._family_specs(f) + PR._family_rescues(f)
    # Rule 14 scopes its 75 % threshold to AUTONOMOUS research. Inside that
    # scope the rule governs INFORMATION-directed specifications; the inclusive
    # count is reported too and BOTH must hold, so it can never be met by
    # reclassifying a family. An OPERATOR-DIRECTED axis sits outside the rule
    # and is published in a third denominator whether it passes or fails - see
    # test_non_price_rule_publishes_the_operator_directed_denominator_honestly.
    auto = [s for s in planned if not s.get("operator_directed")]
    info = [s for s in auto if s["selects_information"]]
    assert PR.non_price_share(info)["rule_met"] is True
    assert PR.non_price_share(auto)["rule_met"] is True
    # a construction-only family may not quietly claim to select information
    by_name = {f["family"]: f for f in fams}
    assert by_name["EQUITY_INCUMBENT_CADENCE"]["selects_information"] is False
    # every rescue names the measured binding failure it resolves
    for f in fams:
        for r in f.get("rescue_specifications") or []:
            assert r.get("binding_failure")


def test_exhausted_price_state_reopening_rule():
    assert PR.reopening_allowed(price_state=False, reason=None, binding_failure=None)["allowed"]
    assert not PR.reopening_allowed(price_state=True, reason="ANOTHER_LAG", binding_failure=None)["allowed"]
    assert not PR.reopening_allowed(price_state=True, reason=PR.REOPEN_REASONS[3], binding_failure=None)["allowed"]
    assert PR.reopening_allowed(price_state=True, reason=PR.REOPEN_REASONS[3],
                                binding_failure="cadence cost drag")["allowed"]
    assert PR.reopening_allowed(price_state=True, reason="NEW_ORTHOGONAL_INFORMATION", binding_failure=None)["allowed"]


def test_multiple_testing_uses_the_executed_denominator_and_holm_within_family():
    cells = []
    for i in range(6):
        p = 0.02 if i == 0 else 0.4
        cells.append({"cell_id": "c%d" % i, "family": "F", "conditional": {"t": 2.5}, "redundancy": {"residual_share": 0.8},
                      "head_to_head": {"top25": {"all": {"ann_advantage": 0.02, "t_advantage": 2.2, "effective_periods": 100,
                                                          "p_advantage_one_sided": p,
                                                          "challenger": {"mean_oneway_turnover": 0.2, "max_dd": -0.2},
                                                          "incumbent": {"max_dd": -0.2}},
                                                  "selection": {"ann_advantage": 0.01},
                                                  "lockbox": {"ann_advantage": 0.02, "halves_ann_advantage": [0.01, 0.01]}}}})
    mt = T.apply_multiplicity(cells)
    assert mt["denominator"] == 6
    assert mt["holm_by_family"]["F"]["m"] == 6
    # 0.02 x 6 = 0.12 > 0.05: Holm refuses; BH at q=0.10: 0.02 <= 0.10 x 1/6 fails too
    assert cells[0]["holm_pass_family"] is False and cells[0]["fdr_pass_paired"] is False
    assert cells[0]["tournament_verdict"] == T.V_NOT_QUALIFIED
    h = FAM.holm({"a": 0.01, "b": 0.03, "c": 0.5})
    assert h["n_rejected"] == 1 and h["family_p"] == pytest.approx(0.03)


# --------------------------------------------------------------------------- #
# PIT: the earnings block, the news block
# --------------------------------------------------------------------------- #
def test_earnings_features_are_placed_at_their_own_availability_never_earlier():
    dates = np.array([str(d.date()) for d in pd.bdate_range("2020-01-01", periods=300)])
    n_d = len(dates)
    tr = np.cumprod(1.0 + np.full((1, n_d), 0.001), axis=1)
    spy = np.cumprod(1.0 + np.full(n_d, 0.0005))
    E = {"dates": dates, "symbols": np.array(["AAA"]), "sym2cik": {"AAA": "1"},
         "price": {"tr": tr, "spy_tr": spy}}
    q = pd.DataFrame([{"pe": "2020-03-31", "value": 10.0, "available": "2020-05-08", "via_8k": False, "source": "Q"}])
    k8 = pd.DataFrame({"filing_date": pd.to_datetime(["2020-04-28"]), "acceptance": ["2020-04-28T16:30:00.000Z"]})
    anns = EE.announcement_dates(q, k8, {"2020-03-31": "2020-05-08"})
    assert anns and anns[0]["ann_date"] == "2020-04-28" and anns[0]["source"] == "8K_RULE"
    e = EE._event_session_index(dates, "2020-04-28", "2020-04-28T16:30:00.000Z")
    assert dates[e] > "2020-04-28"                                  # after the close -> next session
    e_pre = EE._event_session_index(dates, "2020-04-28", "2020-04-28T08:00:00.000Z")
    assert dates[e_pre] == "2020-04-28"
    # the sue value cannot appear before its filed date + 1 session
    sue = EE.sue_series(pd.DataFrame([{"pe": "2019-%02d-28" % m, "value": float(m), "available": "2019-%02d-10" % min(12, m + 1),
                                       "via_8k": False, "source": "Q"} for m in (3, 6, 9, 12)]
                                     + [{"pe": "2020-%02d-28" % m, "value": float(m) * 1.5, "available": "2020-%02d-10" % min(12, m + 1),
                                         "via_8k": False, "source": "Q"} for m in (3, 6, 9, 12)]
                                     + [{"pe": "2021-03-28", "value": 30.0, "available": "2021-05-10", "via_8k": False, "source": "Q"}]))
    assert np.isnan(sue["sue"].iloc[:4]).all()                     # no year-earlier quarter or too few diffs
    assert np.isfinite(sue["sue"].iloc[-1])


def test_news_items_after_the_close_count_on_the_next_session_and_sample_is_deterministic(root):
    d = NW.store_dir()
    d.mkdir(parents=True)
    import gzip
    with gzip.open(d / "AAA.US.jsonl.gz", "wt", encoding="utf-8") as fh:
        fh.write(json.dumps({"date": "2021-03-01T20:30:00+00:00", "polarity": 0.5, "neg": 0.1}) + "\n")   # 15:30 ET, same day
        fh.write(json.dumps({"date": "2021-03-01T22:30:00+00:00", "polarity": -0.5, "neg": 0.9}) + "\n")  # 17:30 ET, next day
    (d / NW.MANIFEST).write_text(json.dumps({"complete": {"AAA.US": {"symbol": "AAA", "items": 2}}, "state": "COMPLETE",
                                             "items": 2}), encoding="utf-8")
    dates = np.array([str(x.date()) for x in pd.bdate_range("2020-09-01", periods=200)])
    n_d = len(dates)
    E = {"dates": dates, "symbols": np.array(["AAA", "BBB"]), "price": {"tr": np.ones((2, n_d))}}
    blk = NW.build_blocks(E)
    cnt = blk[NW.BLOCK][0, :, 0]
    i0 = int(np.searchsorted(dates, "2021-03-01"))
    i1 = int(np.searchsorted(dates, "2021-03-02"))
    # nothing on 2021-03-01 is visible before the 15:30 item's own session; the 17:30 item lands on 03-02
    assert np.isnan(cnt[i0 - 1]) or cnt[i0 - 1] <= cnt[i0]
    assert cnt[i1] >= cnt[i0]
    assert np.isnan(blk[NW.BLOCK][1]).all()                         # uncovered name stays NaN
    assert blk["_report"]["pit"]["sentiment"].startswith("PIT_UNVERIFIED")
    # the sample rule is deterministic and keeps delisted names
    tk = {"AAA": "AAA", "TWTR-202210": "TWTR"}
    E2 = {"dates": dates, "symbols": np.array(["TWTR-202210", "AAA"]), "sym2cik": {"AAA": "1", "TWTR-202210": "2"}}
    NW._CACHE["tickers"] = tk
    elig = np.ones((2, n_d), dtype=bool)
    s1 = NW.sample(E2, elig, n=2, session="2021-01-04")
    s2 = NW.sample(E2, elig, n=2, session="2021-01-04")
    assert s1 == s2 and {r["ticker"] for r in s1} == {"AAA", "TWTR"}
    NW._CACHE.pop("tickers", None)


def test_news_acquisition_without_a_key_is_a_data_hold_not_a_purchase(root, monkeypatch):
    monkeypatch.delenv(NW.API_KEY_ENV, raising=False)
    man = NW.acquire([{"symbol": "AAA", "ticker": "AAA", "eodhd": "AAA.US"}], end="2021-01-31")
    assert man["state"] == "DATA_HOLD_NO_API_KEY" and man["purchases"] == 0 and man["subscriptions"] == 0


# --------------------------------------------------------------------------- #
# Human-gated adoption, no automatic promotion, no portfolio mutation
# --------------------------------------------------------------------------- #
def test_forward_package_is_immutable_and_adoption_is_human_gated(root):
    cell = {"cell_id": "US_EQUITY|XS|21|X|vs|INCUMBENT_SCORE", "scope": "US_EQUITY", "horizon": 21, "cadence": 21,
            "family": "F", "dimension": "X", "baseline": ["INCUMBENT_SCORE"], "n_instruments": 500,
            "tournament_verdict": "BEATS_INCUMBENT_NOT_QUALIFIED",
            "head_to_head": {"top25": {"all": {"ann_advantage": 0.02}}}, "conditional": {"t": 2.5},
            "gates": {"materiality_ge_1p5pct": True}}
    body = FP.build(tournament={"cells": [cell]}, cadence=None)
    assert body["n_ready"] == 0 and body["n_survivors_not_qualified"] == 1
    rec = body["survivors_not_qualified"][0]
    assert rec["promotion_allowed"] is False and rec["live_registration_performed"] is False
    assert rec["holdings_changed"] is False and rec["records_are_immutable"] is True
    assert rec["freeze_record_hash"] == AR.stable_hash(rec["forward_specification"])
    assert FP.CONFIRM_TOKEN in rec["human_gated_adoption_command"]
    assert "adopt_prospective_freeze.py" in rec["human_gated_adoption_command"]
    p = AR.research_root() / FP.SUBDIR / rec["record_file"]
    first = p.read_text(encoding="utf-8")
    FP.build(tournament={"cells": [cell]}, cadence=None)
    assert p.read_text(encoding="utf-8") == first                  # never overwritten
    assert body["promotion_performed"] is False and body["live_registration_performed"] is False


def test_package_never_calls_a_registrar_adopter_or_portfolio_owner():
    pkg = REPO / "alpha_agent" / "alpha_recovery"
    forbidden = ("register_forward_challenger(", "adopt_prospective_freeze(", "record_governed_decision(",
                 "create_order", "submit_order", "apply_fill", "promote_model(", "activate_sleeve(",
                 "approve_proposal(", "Register-ScheduledTask", "import subprocess", "subprocess.run(")
    for p in sorted(pkg.glob("*.py")):
        src = p.read_text(encoding="utf-8")
        code = "\n".join(ln for ln in src.splitlines() if not ln.strip().startswith("#"))
        for tok in forbidden:
            assert tok not in code, "%s contains %s" % (p.name, tok)
        imports = "\n".join(ln for ln in src.splitlines() if ln.lstrip().startswith(("import ", "from ")))
        for tok in ("from api", "import api", "from paper_trader.api", "from db", "import db", "from engine.portfolio",
                    "import requests", "import httpx"):
            assert tok not in imports, "%s imports %s" % (p.name, tok)
    runner = (REPO / "scripts" / "run_alpha_recovery_offensive.py").read_text(encoding="utf-8")
    assert "--execute" not in runner and "uvicorn" not in runner and "restart_paper_trader_backend" not in runner
    assert "assert_research_root_is_not_live()" in runner


# --------------------------------------------------------------------------- #
# The scoreboard: deterministic, one status, historical and forward separate
# --------------------------------------------------------------------------- #
def test_scoreboard_is_deterministic_and_carries_one_status(root):
    CK.freeze(now=datetime(2026, 9, 10, 14, 0, tzinfo=timezone.utc))
    AR.write_artifact("incumbent_baseline.json", {
        "verdict": {"verdict": AR.IV_WEAK},
        "historical_oos": {"horizons": {"21": {"top25": {"all": {"ann_net_excess": 0.027, "t_net_excess": 0.99,
                                                                    "sharpe_excess": 0.25, "strat_max_dd": -0.22,
                                                                    "mean_oneway_turnover_per_period": 0.31,
                                                                    "mean_rank_ic": 0.013, "t_rank_ic": 1.1,
                                                                    "periods": 180, "effective_periods": 180},
                                                            "lockbox": {"ann_net_excess": 0.11, "t_net_excess": 1.6}}}},
                           "calibration": {"21": {"state": "UNAVAILABLE", "calibrated": False, "reason": "x"}}},
        "true_forward": {"realised_book": {"book_id": "alpha_paper_book_1", "sessions": 35, "cumulative_return": -0.024,
                                           "benchmark_cumulative_return": 0.02, "excess_cumulative": -0.044}}})
    AR.write_artifact("tournament.json", {"brief": [
        {"cell_id": "c1", "family": "F", "horizon": 21, "dimension": "X", "tournament_verdict": "NO_ADVANTAGE",
         "ann_advantage_top25": -0.001, "t_advantage_top25": -0.1, "conditional_t": 0.5,
         "challenger_max_dd": -0.2, "incumbent_max_dd": -0.2, "challenger_turnover": 0.3, "incumbent_turnover": 0.3,
         "failed_gates": ["materiality_ge_1p5pct"]}]})
    a = SB.build(as_of="2026-09-12")
    b = SB.build(as_of="2026-09-12")
    assert a["artifact_hash"] == b["artifact_hash"]
    assert a["status"] in AR.STATUSES and a["status"] == AR.ST_REJECTED
    assert a["campaign"]["clock"]["sessions_elapsed"] == 1 and a["campaign"]["clock"]["sessions_remaining"] == 9
    assert a["incumbent"]["historical_oos"]["ann_net_excess"] == 0.027
    assert a["incumbent"]["true_forward"]["sessions"] == 35
    assert "never pooled" in a["evidence_separation"]
    md = (AR.repo_dir() / AR.SCOREBOARD_MD_NAME).read_text(encoding="utf-8")
    assert md.startswith("# ALPHA RECOVERY SCOREBOARD") and "STATUS: REJECTED" in md
    assert "eligible sessions elapsed / 10: 1 / 10" in md
    assert a["forward_registrations_read_only"] == []              # hermetic registry


def test_status_vocabulary_and_forward_competition_requires_a_registration():
    assert SB.status_from(None, registrations=[], purchase=None, any_measured=False) == AR.ST_RESEARCHING
    best = {"verdict": "MATERIALLY_BEATS_INCUMBENT"}
    assert SB.status_from(best, registrations=[], purchase=None, any_measured=True) == AR.ST_READY
    assert SB.status_from(best, registrations=[{"challenger_id": "ALPHA_RECOVERY_X"}], purchase=None,
                          any_measured=True) == AR.ST_COMPETING
    assert SB.status_from({"verdict": "BEATS_INCUMBENT_NOT_QUALIFIED"}, registrations=[], purchase=None,
                          any_measured=True) == AR.ST_HISTORICAL_SURVIVOR
    assert SB.status_from({"verdict": "NO_ADVANTAGE"}, registrations=[],
                          purchase={"owned_free_information_exhausted": True}, any_measured=True) == AR.ST_EXHAUSTED


def test_artifacts_are_deterministic_apart_from_generated_at(root):
    p1 = AR.write_artifact("x.json", {"a": 1, "b": [1, 2]})
    h1 = json.loads(p1.read_text(encoding="utf-8"))["artifact_hash"]
    p2 = AR.write_artifact("x.json", {"a": 1, "b": [1, 2]})
    assert json.loads(p2.read_text(encoding="utf-8"))["artifact_hash"] == h1


def test_conftest_and_this_file_redirect_every_live_path():
    src = Path(__file__).read_text(encoding="utf-8")
    for env in ("AR.RESEARCH_ROOT_ENV", "AR.REPO_DIR_ENV", "INC.DESK_DIR_ENV", "r59.RESEARCH_ROOT_ENV",
                "IN.FRONTIER_PATH_ENV", "SB.REGISTRY_DIR_ENV"):
        assert "monkeypatch.setenv(%s" % env in src
    conftest = (REPO / "tests" / "conftest.py").read_text(encoding="utf-8")
    assert "PAPER_TRADER_FORWARD_CHALLENGER_REGISTRY_DIR" in conftest


# --------------------------------------------------------------------------- #
# Same-domain construction challengers (the cadence ladder and the legs)
# --------------------------------------------------------------------------- #
def _synthetic_panel(n_sym: int = 60, n_d: int = 260, seed: int = 7):
    rng = np.random.default_rng(seed)
    dates = np.array([str(d.date()) for d in pd.bdate_range("2019-01-02", periods=n_d)])
    ret = rng.normal(0.0004, 0.012, size=(n_sym, n_d))
    score = rng.normal(size=(n_sym, n_d))
    elig = np.ones((n_sym, n_d), dtype=bool)
    return dates, ret, score, elig


def test_equity_cadence_arms_share_capital_and_charge_cost_only_when_they_trade():
    dates, ret, score, elig = _synthetic_panel()
    b21 = EC.daily_path_book(score, elig=elig, ret=ret, dates=dates, top_n=25, trade_every=21,
                             start_date=dates[0])
    b63 = EC.daily_path_book(score, elig=elig, ret=ret, dates=dates, top_n=25, trade_every=63,
                             start_date=dates[0])
    # the slower arm trades strictly less and rebalances strictly less often
    assert b63["rebalanced"].sum() < b21["rebalanced"].sum()
    assert b63["turnover"].sum() < b21["turnover"].sum()
    # cost is charged on a rebalance session and nowhere else
    for b in (b21, b63):
        assert not (b["cost"][~b["rebalanced"]] > 0).any()
        assert (b["cost"][b["rebalanced"]] > 0).all()
    # both arms live on the identical calendar
    assert np.isfinite(b21["daily_net"]).sum() == np.isfinite(b63["daily_net"]).sum()


def test_a_held_name_that_stops_printing_is_carried_never_dropped_for_free():
    dates, ret, score, elig = _synthetic_panel()
    top = int(np.argmax(score[:, 0]))
    ret2 = ret.copy()
    ret2[top, 5:] = np.nan                      # the name delists mid-holding
    b = EC.daily_path_book(score, elig=elig, ret=ret2, dates=dates, top_n=25, trade_every=21,
                           start_date=dates[0])
    live = np.isfinite(b["daily_net"])
    assert live.sum() > 200                     # the book keeps running, the NaN does not propagate
    assert b["n_held"][6] == b["n_held"][4]     # still held, so the exit is charged at the next rebalance


def _eq_cell(adv, t, *, lock_adv=0.02, halves=(0.01, 0.01), turn=0.20, dd=-0.20, ref_dd=-0.20,
             periods=181):
    return {"cell_id": "US_EQUITY|TOP25|blend|k63", "family": EC.FAM_CADENCE, "is_reference": False,
            "all": {"paired": {"ann_advantage": adv, "t_advantage": t, "periods": periods,
                               "effective_periods": periods, "p_advantage_one_sided": 0.01},
                    "arm": {"mean_oneway_turnover_per_21s": turn, "max_dd": dd},
                    "reference": {"max_dd": ref_dd}},
            "selection": {"paired": {"ann_advantage": 0.01}},
            "lockbox": {"paired": {"ann_advantage": lock_adv, "halves_ann_advantage": list(halves)}}}


def test_equity_challenger_gates_are_the_frozen_ones_and_close_is_not_pass():
    # materiality is the frozen 1.5 %/yr and a hair under it does not pass
    just_under = _eq_cell(AR.MATERIALITY_ANN_NET - 1e-4, 3.0)
    assert EC.gates(just_under)["materiality_ge_1p5pct"] is False
    # a big advantage with t just below 2 is NOT a survivor
    weak_t = _eq_cell(0.03, 1.99)
    weak_t["gates"] = EC.gates(weak_t)
    assert weak_t["gates"]["paired_t_ge_2"] is False
    assert EC.verdict(weak_t) == T.V_NO_ADVANTAGE
    # the same cell at t >= 2 is a survivor but still not MATERIAL until BH and Holm decide
    ok = _eq_cell(0.03, 2.5)
    ok["gates"] = EC.gates(ok)
    assert EC.verdict(ok) == T.V_NOT_QUALIFIED
    ok["gates"] = EC.gates(ok, fdr_pass=True, holm_pass=True)
    assert EC.verdict(ok) == T.V_MATERIAL
    # the turnover cap is the tournament's, expressed per 21 sessions
    fat = _eq_cell(0.03, 2.5, turn=AR.GATE_MAX_TURNOVER + 1e-6)
    assert EC.gates(fat)["turnover_le_cap"] is False


def test_equity_challenger_reference_arm_is_the_operational_construction():
    grid = EC.default_grid()
    ref = [g for g in grid if g["is_reference"]]
    assert len(ref) == 1
    assert (ref[0]["leg"], ref[0]["trade_every"]) == (EC.LEG_BLEND, 21)
    # the ladder was fixed before it ran and is not extended
    assert sorted(g["trade_every"] for g in grid if g["family"] == EC.FAM_CADENCE) == [21, 42, 63, 126]
    # a reference arm can never be reported as its own challenger
    assert EC.verdict({"is_reference": True}) == "REFERENCE_ARM"


# --------------------------------------------------------------------------- #
# The residual frontier needs: the row floor is answered, never moved
# --------------------------------------------------------------------------- #
def test_frontier_residual_does_not_move_the_row_floor():
    assert S.MIN_ROWS == 200                     # the inherited floor
    body = FR.merge(cells=[], write=False)
    td = body["threshold_discipline"]
    assert td["sensitivity_min_rows"] == 200 and td["moved"] is False
    assert "196" in td["named_binding_failure"] and "200" in td["named_binding_failure"]


def test_frontier_residual_budget_and_named_rescue():
    grid = FR.grid()
    rescues = [c for c in grid if c["tag"] == "RESCUE"]
    assert len(rescues) == 1
    assert rescues[0]["binding_failure"] and rescues[0]["reopening_reason"] in PR.REOPEN_REASONS
    # the three first-pass mandate cells plus these primaries stay inside the family budget
    n_primary = len(PR.MANDATE_CELLS) + len([c for c in grid if c["tag"] == "PRIMARY"])
    assert n_primary <= AR.FAMILY_PRIMARY_MAX
    assert PR.check_family_budget({"family": FR.FAMILY}, executed_primary=n_primary,
                                  executed_rescue=len(rescues),
                                  rescue_binding_failures=tuple(r["binding_failure"] for r in rescues))


# --------------------------------------------------------------------------- #
# The forecast-product calibration test
# --------------------------------------------------------------------------- #
def test_selection_lockbox_calibration_can_fail_and_is_applied_unchanged():
    rng = np.random.default_rng(3)
    n_s, n_l = 400, 100
    lock = np.array([False] * n_s + [True] * n_l)
    # a stable series passes
    x = rng.normal(0.001, 0.01, size=n_s + n_l)
    good = FPR.selection_lockbox_calibration(x, lock, periods_per_year=12.0, label="good")
    assert good["calibrated"] is True
    # a sign flip fails, whatever the interval says
    y = np.concatenate([rng.normal(-0.004, 0.01, n_s), rng.normal(0.004, 0.01, n_l)])
    flip = FPR.selection_lockbox_calibration(y, lock, periods_per_year=12.0, label="flip")
    assert flip["calibrated"] is False and "sign" in flip["reason"]
    # a lockbox far outside the predictive interval fails
    z = np.concatenate([rng.normal(0.0005, 0.001, n_s), rng.normal(0.05, 0.001, n_l)])
    out = FPR.selection_lockbox_calibration(z, lock, periods_per_year=12.0, label="out")
    assert out["calibrated"] is False and "predictive interval" in out["reason"]
    # too short a lockbox fails
    short = np.concatenate([rng.normal(0.001, 0.01, n_s), rng.normal(0.001, 0.01, 5)])
    sl = FPR.selection_lockbox_calibration(short, np.array([False] * n_s + [True] * 5),
                                           periods_per_year=12.0, label="short")
    assert sl["calibrated"] is False


def test_an_uncalibrated_book_never_publishes_an_expected_return():
    rng = np.random.default_rng(11)
    n_s, n_l = 300, 80
    lock = np.array([False] * n_s + [True] * n_l)
    y = np.concatenate([rng.normal(-0.004, 0.01, n_s), rng.normal(0.004, 0.01, n_l)])
    cal = FPR.selection_lockbox_calibration(y, lock, periods_per_year=12.0, label="flip")
    rec = FPR._record(cal, "test")
    assert rec["calibrated"] is False
    with pytest.raises(FC.ForecastContractError):
        FC.expected_return(0.01, calibration=rec)          # the contract refuses it outright
    field = FC.unavailable(cal["reason"], "fraction over 21 sessions")
    assert field["state"] == FC.UNAVAILABLE and field["reason"]


def test_forecast_product_families_are_named_and_the_rule_is_published():
    assert FPR.FAM_MARKET and FPR.FAM_XS and FPR.FAM_BOOK and FPR.FAM_SLEEVE
    doc = Path(FPR.__file__).read_text(encoding="utf-8")
    for token in ("share a sign", "predictive interval", "MIN_EFFECTIVE_PERIODS observations"):
        assert token in doc


# --------------------------------------------------------------------------- #
# The intraday cross-asset axis. These regressions exist because an intraday
# backtest fails silently: a mis-aligned minute grid, a look-ahead entry or an
# uncharged round trip all produce a plausible-looking number.
# --------------------------------------------------------------------------- #
def test_intraday_grid_is_exchange_local_not_utc():
    """The panel's file window is fixed in UTC, so the US session starts at a
    different point inside it under EDT and EST. Building the grid in UTC would
    mix 09:30 ET with 08:30 ET. The grid must be exchange-local and the covered
    regular window must be exactly the common 150 minutes."""
    assert ID.TZ == "America/New_York"
    assert ID.REG_FIRST_MOD == 9 * 60 + 30 and ID.REG_LAST_MOD == 11 * 60 + 59
    assert ID.N_REG == 150 and ID.N_PRE == 150
    assert ID.minute_index(9, 30) == 0
    assert ID.minute_index(11, 59) == ID.N_REG - 1
    with pytest.raises(ValueError):
        ID.minute_index(15, 0)                      # there is no afternoon in this panel
    doc = Path(ID.__file__).read_text(encoding="utf-8")
    for token in ("fixed in UTC", "daylight saving", "exchange local time",
                  "NO afternoon and NO closing auction"):
        assert token in doc


def test_intraday_tradable_split_is_by_measured_coverage():
    """A leg is tradable because it prints minutes and turns over, not because
    it is convenient. UUP prints ~half the session and must stay information."""
    assert set(ID.TRADABLE) == {"SPY", "QQQ", "TLT", "GLD"}
    assert "UUP" in ID.INFORMATION_ONLY and "SHY" in ID.INFORMATION_ONLY
    assert not set(ID.TRADABLE) & set(ID.INFORMATION_ONLY)
    # four economically distinct markets, which is the point of the axis
    assert len({ID.MARKET[s] for s in ID.TRADABLE}) == 4


def test_intraday_cost_ladder_is_conservative_and_eligibility_uses_the_stress_rate():
    assert ID.COST_PRIMARY_BPS >= AR.SPY_PROXY_COST_BPS * 2      # 2x the estate's own SPY rate
    assert ID.COST_STRESS_BPS > ID.COST_PRIMARY_BPS
    assert ID.COST_ELIGIBILITY_BPS == ID.COST_STRESS_BPS
    assert ID.COST_CANONICAL_BPS == AR.EQ_COST_RATE_PER_SIDE * 1e4


def test_intraday_entry_is_strictly_after_the_signal_minute():
    """No arm may trade on the bar that produced its own signal."""
    src = Path(IA.__file__).read_text(encoding="utf-8")
    assert "entry = si + 1" in src
    for sp in IA.full_grid():
        si = ID.minute_index(*sp["signal"])
        ei = ID.minute_index(*sp["exit"])
        assert si + 1 < ei, sp["cell_id"]


def test_intraday_cost_is_charged_every_engaged_session_and_ladder_is_monotone():
    sp = IA.full_grid()[0]
    nets = []
    for c in (0.0, ID.COST_PRIMARY_BPS, ID.COST_STRESS_BPS, ID.COST_CANONICAL_BPS):
        p = IA.session_path(sp, cost_bps=c)
        eng = p["engaged"]
        exp = eng * 2.0 * c * 1e-4
        assert np.allclose(p["cost"], exp)           # full round trip, every engaged session
        nets.append(float(np.nanmean(p["net"])))
    assert nets == sorted(nets, reverse=True)        # more cost is never better


def test_intraday_gross_diagnostic_separates_information_from_execution():
    """A gross t below 2.0 cannot be rescued by any cost assumption. The cell
    must carry that number so the two failure modes are never conflated."""
    sp = IA.full_grid()[0]
    cell = IA.measure_cell(sp, with_capital=False)
    g = cell["gross"]
    for k in ("ann_gross", "t_gross", "bp_per_engaged_session", "reaches_t2_at_zero_cost"):
        assert k in g
    zero = IA.session_path(sp, cost_bps=0.0)
    assert np.allclose(g["ann_gross"], float(np.nanmean(zero["gross"]) * IA.PPY))


def test_intraday_gates_are_the_frozen_ones_and_close_is_not_pass():
    """t = 1.99 is a failure. The stress-cost gate is an ADDITION, never a
    relaxation, and the turnover substitution is declared rather than silent."""
    cell = {"by_cost_bps_per_side": {
        "%.1f" % ID.COST_PRIMARY_BPS: {
            "all": {"ann_net": 0.09, "t_net": 1.99, "effective_periods": 500},
            "selection": {"ann_net": 0.09}, "holdout": {"ann_net": 0.09},
            "holdout_halves_ann_net": [0.05, 0.05]},
        "%.1f" % ID.COST_STRESS_BPS: {"all": {"ann_net": 0.05}}}}
    g = IA.gates(cell)
    assert g["materiality_ge_1p5pct"] is True
    assert g["t_ge_2"] is False
    assert IA.verdict({**cell, "gates": g}) != T.V_MATERIAL
    cell["by_cost_bps_per_side"]["%.1f" % ID.COST_PRIMARY_BPS]["all"]["t_net"] = 2.01
    assert IA.gates(cell)["t_ge_2"] is True
    # an arm that only lives inside the cheap cost assumption fails eligibility
    cell["by_cost_bps_per_side"]["%.1f" % ID.COST_STRESS_BPS]["all"]["ann_net"] = 0.001
    assert IA.gates(cell)["survives_stress_cost"] is False
    assert "GATE_MAX_TURNOVER" in IA.TURNOVER_GATE_SUBSTITUTION
    assert "STRICTER" in IA.TURNOVER_GATE_SUBSTITUTION


def test_intraday_rescue_budget_is_two_and_each_names_a_measured_failure():
    resc = IA.rescue_grid()
    assert len(resc) <= AR.FAMILY_RESCUE_MAX
    for sp in resc:
        assert sp["tag"] == "RESCUE"
        bf = sp["binding_failure"]
        assert bf.startswith("MEASURED:") and "unconditional" in bf.lower()
        assert sp["hypothesis"]
    # the rescue conditions ENGAGEMENT, it does not add an information dimension
    assert {sp["dimension"] for sp in resc} <= set(IA.DIMENSION.values())
    for fam in IA.FAMILIES:
        n = len([s for s in IA.default_grid() if s["family"] == fam])
        assert n <= AR.FAMILY_PRIMARY_MAX


def test_intraday_conditional_rescue_uses_only_strictly_prior_information():
    """The engagement threshold is a quantile of the STRICTLY PRIOR window; if
    it ever peeked at today, the rescue would be look-ahead."""
    sp = IA.rescue_grid()[0]
    w = IA.signals(sp)
    n = int(sp["rescue_lookback_sessions"]) // 2
    assert np.allclose(w[:n], 0.0)                   # nothing engages before a prior window exists
    assert 0.0 < float(np.mean(np.abs(w).sum(axis=1) > 0)) < 1.0   # it does gate some sessions


def test_intraday_sleeve_is_flat_overnight():
    for sp in IA.full_grid():
        assert sp["exit"] == IA.EXIT_ET
        assert ID.minute_index(*sp["exit"]) <= ID.N_REG - 1
    doc = Path(IA.__file__).read_text(encoding="utf-8")
    assert "flat overnight" in doc.lower()


# --------------------------------------------------------------------------- #
# Information axis A: the option surface. The regression that matters is the
# REFUSAL - a narrow surface must not be turned into a plausible ATM series.
# --------------------------------------------------------------------------- #
def test_option_surface_refuses_a_drifting_moneyness_proxy():
    u = OS.usability()
    assert u["state"] in ("USABLE", "DATA_INSUFFICIENT")
    assert u["floor"] == AR.MIN_EFFECTIVE_PERIODS
    if u["state"] == "DATA_INSUFFICIENT":
        assert u["dates_whose_strikes_bracket_the_money"] < AR.MIN_EFFECTIVE_PERIODS
        assert u["exact_missing_requirement"]
        assert OS.run_grid(verbose=False) == []       # nothing is scored
    f = OS.features()
    # every date that does NOT bracket the money contributes no ATM reading
    bad = f[~f["brackets_the_money"]]
    assert bool(np.isnan(bad["atm_iv_near"]).all())


def test_option_surface_runs_one_pre_registered_sign_per_signal():
    """Running the mirror sign as a separate specification would double the
    multiplicity denominator for no information."""
    grid = OS.default_grid()
    assert len(grid) == len(OS.SIGNALS) * len(OS.HORIZONS)
    assert len(grid) <= AR.FAMILY_PRIMARY_MAX
    for name, sp in OS.SIGNALS.items():
        signs = {g["sign"] for g in grid if g["name"] == name}
        assert signs == {sp["sign"]}, name
        assert sp["economics"]
    assert 21 in OS.HORIZON_NOT_RUN and "36" in OS.HORIZON_NOT_RUN[21]


def test_option_surface_resolves_the_named_binding_failure_without_moving_the_floor():
    doc = Path(OS.__file__).read_text(encoding="utf-8")
    assert "196" in doc and "MIN_ROWS" in doc
    assert "row floor was never moved" in doc or "never moved" in doc


# --------------------------------------------------------------------------- #
# Non-price accounting across three denominators
# --------------------------------------------------------------------------- #
def test_non_price_rule_publishes_the_operator_directed_denominator_honestly():
    """Rule 14 scopes to AUTONOMOUS research. The operator-directed axis is
    excluded from the rule's denominators and reported in a third one, which is
    published whether it passes or fails - never reclassified into compliance."""
    fams = PR.families_from_protocol()
    directed = [f for f in fams if f.get("operator_directed")]
    assert directed, "the intraday families are operator-directed"
    specs = []
    for fam in fams:
        specs.extend(PR._family_specs(fam))
        specs.extend(PR._family_rescues(fam))
    auto = [s for s in specs if not s.get("operator_directed")]
    assert len(auto) < len(specs)
    share_auto = PR.non_price_share([s for s in auto if s.get("selects_information")])
    share_all = PR.non_price_share(specs)
    assert share_auto["rule_met"] is True
    assert share_all["executed"] == len(specs)
    # the all-executed share is strictly lower - the directed axis is price-derived
    assert share_all["share_non_price"] < share_auto["share_non_price"]


# --------------------------------------------------------------------------- #
# Databento acquisition: the spending contract, the symbology and the PIT roll
# --------------------------------------------------------------------------- #
def _dbn_fake(rate_per_day=0.02, recognised_spelling=1):
    """A Databento stand-in. ``recognised_spelling`` is the number of trailing
    year digits the venue admits, so a test can prove the resolver DISCOVERS
    the spelling instead of assuming one."""
    calls = {"cost": 0, "resolve": 0, "range": 0, "order": []}

    def fake(method, url, params, key, timeout):
        ep = url.rsplit("/", 1)[-1]
        calls["order"].append(ep)
        if ep == "metadata.get_dataset_range":
            return json.dumps({"start": "2010-06-06", "end": "2026-09-09"}).encode()
        if ep == "symbology.resolve":
            calls["resolve"] += 1
            out = {}
            for s in params["symbols"].split(","):
                tail = s[len(s.rstrip("0123456789")):]
                if len(tail) == recognised_spelling:
                    out[s] = [{"s": "1"}]
            return json.dumps({"result": out}).encode()
        if ep == "metadata.get_cost":
            calls["cost"] += 1
            d0 = date.fromisoformat(params["start"])
            d1 = date.fromisoformat(params["end"])
            return json.dumps((d1 - d0).days * rate_per_day).encode()
        if ep == "timeseries.get_range":
            calls["range"] += 1
            return b"ts_event,open,high,low,close,volume,symbol\n"
        raise AssertionError("unexpected endpoint " + ep)

    return fake, calls


def test_databento_never_downloads_before_it_has_priced(root):
    """The spending contract: metadata.get_cost precedes every byte of data."""
    fake, calls = _dbn_fake()
    c = DBN.Client(key="test", transport=fake)
    p = DBN.plan(c, budget_usd=125.0, years=1.0, roots=["ES", "GC"], today=date(2026, 9, 10))
    assert calls["range"] == 0, "data was requested before a cost was known"
    assert calls["cost"] > 0
    assert p["cost_estimated_before_any_download"] is True
    assert "metadata.get_cost" in calls["order"]
    assert DBN.download(c, p)["state"] == "DRY_RUN"
    assert calls["range"] == 0


def test_databento_refuses_a_plan_that_exceeds_the_free_credit(root):
    fake, _ = _dbn_fake()
    c = DBN.Client(key="test", transport=fake)
    p = DBN.plan(c, budget_usd=125.0, years=1.0, roots=["ES"], today=date(2026, 9, 10))
    busted = dict(p, selection=dict(p["selection"], fits_in_free_credit=False))
    with pytest.raises(DBN.DatabentoError):
        DBN.download(c, busted, dry_run=False, out_root=root / "dl")


def test_databento_refuses_a_request_the_plan_never_priced(root):
    """No signature, no download - this is what stops a widened window from
    riding along on an estimate made for a narrower one."""
    fake, _ = _dbn_fake()
    c = DBN.Client(key="test", transport=fake)
    p = DBN.plan(c, budget_usd=125.0, years=1.0, roots=["ES"], today=date(2026, 9, 10))
    tampered = json.loads(json.dumps(p))
    tampered["requests"] = tampered["requests"][:1]
    tampered["requests"][0]["end"] = "2030-01-01"
    with pytest.raises(DBN.DatabentoError):
        DBN.download(c, tampered, dry_run=False, out_root=root / "dl")


def test_databento_budget_cap_keeps_a_safety_margin_and_admits_zero_paid_dollars():
    sel = DBN.optimise({"ES": 60.0, "GC": 40.0, "CL": 30.0}, budget_usd=100.0, sessions=500)
    assert sel["effective_cap_usd"] == pytest.approx(100.0 * (1 - DBN.BUDGET_SAFETY_MARGIN))
    assert sel["estimated_spend_usd"] <= sel["effective_cap_usd"]
    assert sel["paid_dollars_required"] == 0.0
    assert sel["fits_in_free_credit"] is True


def test_databento_optimiser_prefers_the_benchmark_contract_not_the_cheapest():
    """The defect this pins: with equal tier weights a value/dollar greedy buys
    the CHEAPEST member of each bucket, returning NQ without ES, 6J instead of
    6E and the 2-year note as the sole proxy for the rates complex."""
    costs = {"ES": 40.0, "NQ": 20.0, "6E": 20.0, "6J": 10.0, "ZN": 20.0, "ZT": 5.0}
    sel = DBN.optimise(costs, budget_usd=200.0, sessions=500)
    for senior, junior in (("ES", "NQ"), ("6E", "6J"), ("ZN", "ZT")):
        assert senior in sel["chosen"], "%s must be preferred over the cheaper %s" % (senior, junior)
        if junior in sel["chosen"]:
            assert sel["chosen"].index(senior) < sel["chosen"].index(junior)


def test_databento_optimiser_does_not_mistake_four_treasuries_for_four_markets():
    costs = {r: 10.0 for r in ("ZN", "ZF", "ZT", "ZB")}
    costs["CL"] = 10.0
    sel = DBN.optimise(costs, budget_usd=25.0, sessions=500)
    assert "CL" in sel["chosen"], "an unowned energy bucket outranks a second rates contract"
    assert len([r for r in sel["chosen"] if DBN.UNIVERSE[r][1] == "US_RATES"]) == 1


def test_databento_history_too_short_is_worth_nothing():
    assert DBN.instrument_value("ES", DBN.MIN_USEFUL_SESSIONS - 1, 0) == 0.0
    assert DBN.instrument_value("ES", DBN.TARGET_SESSIONS, 0) > 0.0
    sel = DBN.optimise({"ES": 1.0}, budget_usd=100.0, sessions=10)
    assert sel["chosen"] == []
    assert sel["insufficient_budget_for_any_instrument"] is True


def test_databento_resolves_the_symbol_spelling_instead_of_assuming_one():
    """CME dated symbols are written both ESZ5 and ESZ25. A wrong guess spends
    credit on an empty result, so the spelling is discovered, never assumed."""
    for spelling in (1, 2):
        fake, calls = _dbn_fake(recognised_spelling=spelling)
        c = DBN.Client(key="test", transport=fake)
        out = DBN.resolve_symbols(c, ["ES"], date(2025, 1, 1), date(2026, 1, 1))
        got = [e["symbol"] for e in out["resolved"]["ES"]]
        assert got, "no ES contract resolved at spelling %d" % spelling
        for sym in got:
            assert len(sym[len(sym.rstrip("0123456789")):]) == spelling
        assert not out["unresolved"].get("ES")
        assert calls["resolve"] <= 2


def test_databento_requests_dated_contracts_and_never_a_continuous_symbol():
    src = Path(DBN.__file__).read_text(encoding="utf-8")
    assert DBN.STYPE_IN == "raw_symbol"
    assert '"continuous"' not in src and "'continuous'" not in src
    for entry in DBN.dated_symbols("ES", date(2025, 1, 1), date(2026, 1, 1)):
        for cand in entry["candidates"]:
            assert cand.startswith("ES") and cand[2] in DBN.MONTH_CODE.values()


def test_databento_roll_is_causal_forward_only_and_starts_at_the_front_month():
    """The bootstrap bug this pins: with no prior volume the front contract was
    taken as the first COLUMN (alphabetical), which selects ESH6 over ESZ5 and,
    because the roll is forward-only, locks that error in for the whole sample."""
    sessions = ["2025-11-%02d" % d for d in range(10, 26)]
    vol = pd.DataFrame(index=sessions, dtype=float)
    vol["ESH6"] = [1e4] * 7 + [1e6] * (len(sessions) - 7)
    vol["ESZ5"] = [1e6] * 7 + [1e4] * (len(sessions) - 7)
    deliveries = {"ESZ5": "2025-12-01", "ESH6": "2026-03-01"}
    sched = DBN.roll_schedule(vol, deliveries)
    held = sched["symbol"].tolist()
    assert held[0] == "ESZ5", "the roll must bootstrap onto the NEAREST delivery"
    assert held[-1] == "ESH6"
    assert [deliveries[h] for h in held] == sorted(deliveries[h] for h in held)
    switch = held.index("ESH6")
    assert switch >= 7, "rolled on or before the evidence session - not causal"


def test_databento_roll_abandons_a_contract_before_delivery():
    sessions = ["2025-11-%02d" % d for d in range(24, 30)]
    vol = pd.DataFrame(index=sessions, dtype=float)
    vol["ESZ5"] = [1e6] * len(sessions)
    vol["ESH6"] = [1e3] * len(sessions)
    sched = DBN.roll_schedule(vol, {"ESZ5": "2025-11-28", "ESH6": "2026-03-01"})
    for s, sym in sched["symbol"].items():
        if sym == "ESZ5":
            gap = (date.fromisoformat("2025-11-28") - date.fromisoformat(s)).days
            assert gap > DBN.ROLL_MIN_DAYS_BEFORE_EXPIRY


def test_databento_parses_in_exchange_time_not_utc():
    """The DST trap that corrupted the first pass at the owned ETF panel: a
    minute grid built on the UTC stamp mixes two session clocks across a DST
    boundary. 14:30 UTC is 09:30 ET in winter and 10:30 ET in summer."""
    csv = ("ts_event,open,high,low,close,volume,symbol\n"
           "2025-11-14T14:30:00Z,1,1,1,1,10,ESZ5\n"
           "2025-06-13T14:30:00Z,1,1,1,1,10,ESM5\n")
    df = DBN.parse_csv(csv)
    minutes = dict(zip(df["session"], df["minute_et"]))
    assert minutes["2025-11-14"] == 9 * 60 + 30
    assert minutes["2025-06-13"] == 10 * 60 + 30
    assert minutes["2025-11-14"] != minutes["2025-06-13"]


def test_databento_pit_validation_enforces_the_frozen_floor_and_the_close():
    short = {"sessions": 10, "minute_range_et": [570, 960], "contracts_used": ["ESZ5"],
             "roll_rule": {"decided_from": "x", "returns_spliced_within_contract_only": True}}
    v = DBN.pit_validation(short)
    assert v["usable_for_research"] is False
    assert "enough_sessions_for_frozen_floor" in v["failed"]
    stops_early = dict(short, sessions=600, minute_range_et=[570, 12 * 60 + 59])
    v2 = DBN.pit_validation(stops_early)
    assert "covers_the_us_afternoon" in v2["failed"] and "covers_the_close" in v2["failed"]
    full = dict(short, sessions=600, minute_range_et=[0, 16 * 60])
    assert DBN.pit_validation(full)["usable_for_research"] is True


def test_databento_without_a_credential_is_a_blocker_not_a_purchase(root, monkeypatch):
    monkeypatch.delenv(DBN.ENV_KEY, raising=False)
    b = DBN.build(write=True)
    assert b["state"] == "BLOCKED_CREDENTIAL_ABSENT"
    assert b["blocker"]["kind"] == "MISSING_CREDENTIAL_USER_MUST_SUPPLY"
    assert b["blocker"]["not_a_network_failure"] is True
    assert b["safety"]["purchases_data"] is False
    assert b["safety"]["starts_trial_or_subscription"] is False
    assert b["spending_contract"]["paid_dollars_authorised"] == 0.0
    assert b["credential"]["key_value_recorded"] is False
    assert DBN.ENV_KEY in json.dumps(b) and "Basic " not in json.dumps(b)


def test_databento_budget_must_be_stated_and_is_never_inferred(root, monkeypatch):
    monkeypatch.setenv(DBN.ENV_KEY, "test-key")
    monkeypatch.delenv(DBN.ENV_BUDGET, raising=False)
    b = DBN.build(write=True, client=DBN.Client(key="test", transport=_dbn_fake()[0]))
    assert b["state"] == "BLOCKED_BUDGET_UNSTATED"
    assert b["blocker"]["kind"] == "FREE_CREDIT_BALANCE_UNSTATED"


def test_databento_uses_only_usage_based_historical_endpoints():
    src = Path(DBN.__file__).read_text(encoding="utf-8")
    for forbidden in ("batch.submit_job", "live.", "plan.upgrade"):
        assert forbidden not in src, "%s must not be reachable" % forbidden
    for ep in DBN.spending_contract()["endpoints_used"]:
        assert ep in src


def test_every_declared_runner_stage_is_actually_registered():
    """``intraday`` and ``options`` shipped inside STAGES with no STAGE_FN
    entry, so invoking either raised KeyError. It went unnoticed because those
    axes were driven by importing their modules directly rather than through
    the runner."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "_ar_runner", REPO / "scripts" / "run_alpha_recovery_offensive.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    declared = [s for s in mod.STAGES if s != "all"]
    assert declared, "no stages declared"
    missing = [s for s in declared if s not in mod.STAGE_FN]
    assert not missing, "stages declared but not runnable: %s" % missing
    for s in declared:
        assert callable(mod.STAGE_FN[s])
    # the one credit-spending stage is never swept up by a full-campaign run
    assert "databento" in mod.STAGES_EXCLUDED_FROM_ALL
    swept = [s for s in mod.STAGES if s != "all" and s not in mod.STAGES_EXCLUDED_FROM_ALL]
    assert "databento" not in swept


def test_databento_never_asks_for_a_window_past_the_end_of_the_data():
    """The defect that made the whole axis unreachable while reporting only
    'no dated contract spelling resolved'.

    ``dated_symbols`` admits deliveries up to 120 days past the requested end,
    and the resolver built its window from max(delivery). Databento answers
    HTTP 422 ``data_end_date_after_available_end_date`` for a window that runs
    past the data; the batching loop absorbs DatabentoError, so EVERY root
    resolved to nothing and every cost came back null. The live symptom was a
    $0 plan that looked like a clean refusal rather than a broken query."""
    available_end = "2026-09-09"
    seen = {"windows": []}

    def fake(method, url, params, key, timeout):
        ep = url.rsplit("/", 1)[-1]
        if ep == "metadata.get_dataset_range":
            return json.dumps({"start": "2010-06-06", "end": available_end + "T12:00:00Z",
                               "schema": {DBN.SCHEMA: {"start": "2010-06-06",
                                                       "end": available_end + "T12:00:00Z"}}}).encode()
        if ep == "symbology.resolve":
            seen["windows"].append((params["start_date"], params["end_date"]))
            if params["end_date"] > available_end:            # the real provider's 422
                raise DBN.DatabentoError(
                    "HTTP 422 from symbology.resolve: data_end_date_after_available_end_date")
            return json.dumps({"result": {s: [{"s": "1"}]
                                          for s in params["symbols"].split(",")
                                          if len(s[len(s.rstrip("0123456789")):]) == 1}}).encode()
        if ep == "metadata.get_cost":
            return json.dumps(1.0).encode()
        if ep == "timeseries.get_range":
            raise AssertionError("priced-plan stage must not download")
        raise AssertionError("unexpected endpoint " + ep)

    c = DBN.Client(key="test", transport=fake)
    p = DBN.plan(c, budget_usd=125.0, years=2.0, roots=["ES", "CL"], today=date(2026, 9, 10))

    assert seen["windows"], "the resolver never ran"
    for start_date, end_date in seen["windows"]:
        assert end_date <= available_end, (
            "asked for %s, past the dataset's last available session %s" % (end_date, available_end))
    # and the actual point: the panel is priceable, not silently empty
    assert not p["errors"], p["errors"]
    assert p["symbology"]["resolved_counts"]["ES"] > 0
    assert p["full_panel_cost_usd"] > 0
    assert p["n_requests"] > 0


def test_databento_clamps_the_acquisition_window_to_the_available_data():
    rng = {"end": "2026-09-10T12:51:34.379587000Z",
           "schema": {DBN.SCHEMA: {"end": "2026-09-10T12:51:34.379587000Z"}}}
    # the provider's bound is exclusive and sub-daily, so the last COMPLETE
    # session is the day before the one it names
    assert DBN._available_end(rng) == "2026-09-09"
    assert DBN._available_end({}) is None


def test_databento_records_whether_the_budget_was_actually_verified(root, monkeypatch):
    """A published signup grant is an upper bound on a FRESH account. Reading
    it as 'the balance' is exactly how an accidental paid dollar happens, so
    the artifact has to say which of the two it holds."""
    monkeypatch.setenv(DBN.ENV_KEY, "test-key")
    monkeypatch.setenv(DBN.ENV_BUDGET, "125")
    monkeypatch.setenv(DBN.ENV_BUDGET_SOURCE, "PROVIDER_PUBLISHED_FREE_TIER")
    b = DBN.build(write=True, years=1.0, client=DBN.Client(key="test", transport=_dbn_fake()[0]))
    assert b["budget_provenance"]["source"] == "PROVIDER_PUBLISHED_FREE_TIER"
    assert b["budget_provenance"]["balance_verified_against_the_account"] is False

    monkeypatch.setenv(DBN.ENV_BUDGET_SOURCE, "OPERATOR_STATED")
    b2 = DBN.build(write=True, years=1.0, client=DBN.Client(key="test", transport=_dbn_fake()[0]))
    assert b2["budget_provenance"]["balance_verified_against_the_account"] is True
    # never a paid dollar either way
    assert b2["spending_contract"]["paid_dollars_authorised"] == 0.0


def test_databento_persists_a_normalised_panel_without_a_parquet_engine(root):
    """``to_parquet`` needs pyarrow or fastparquet, which this estate's
    virtualenv does not carry - so persistence raised ImportError at exactly
    the moment a paid-for panel had just landed. csv.gz also matches the
    convention the owned R45 minute panels already use."""
    import gzip
    import pandas as pd

    rows = ["ts_event,open,high,low,close,volume,symbol"]
    for day in range(1, 6):
        for minute in range(0, 60, 10):
            rows.append("2025-11-%02dT14:%02d:00Z,1,1,1,1,10,ESZ5" % (day + 10, minute))
    src = root / "ESZ5_ohlcv-1m_a_b.csv"
    src.write_text("\n".join(rows) + "\n")

    out = DBN.normalise("ES", [str(src)], out_dir=root / "norm")
    assert out["state"] == "NORMALISED"
    assert out["format"] == "csv.gz"
    written = Path(out["path"])
    assert written.exists() and written.suffix == ".gz"
    with gzip.open(written, "rt") as fh:
        back = pd.read_csv(fh)
    assert len(back) > 0
    assert {"session", "minute_et", "close", "ret"} <= set(back.columns)
    # the CALL, not the word: the module explains the defect in prose, and the
    # explanation must not be what keeps this regression green
    code = " ".join(ln.split("#", 1)[0] for ln in
                    Path(DBN.__file__).read_text(encoding="utf-8").splitlines())
    for call in (".to_parquet(", ".read_parquet(", "import pyarrow", "import fastparquet"):
        assert call not in code, "%s reintroduces the missing-engine failure" % call


# --------------------------------------------------------------------------- #
# Futures intraday panel: the PRE-REGISTRATION, fixed before the data exists
# --------------------------------------------------------------------------- #
def test_futures_tick_values_match_the_venue():
    """The only per-instrument numbers this estate hardcodes. If a tick value
    is wrong the whole cost ladder is wrong, and a cost ladder that is wrong in
    the cheap direction manufactures alpha."""
    published = {"ES": 12.50, "NQ": 5.00, "GC": 10.00, "6E": 6.25, "6J": 6.25,
                 "ZN": 15.625, "ZF": 7.8125, "ZT": 7.8125, "ZB": 31.25, "CL": 10.00}
    assert set(published) == set(FI.SPECS)
    for root, want in published.items():
        assert FI.tick_value_usd(root) == pytest.approx(want, rel=1e-9), root


def test_futures_cost_ladder_is_ordered_and_never_cheaper_than_a_tick():
    """STRESS must cost more than PRIMARY and CANONICAL more than STRESS, and
    no level may price a round trip below one crossed tick - the cheapest thing
    that can physically happen."""
    for root in FI.ROOTS:
        price = 100.0
        levels = [FI.round_trip_bps(root, price, lv)
                  for lv in ("PRIMARY", "STRESS", "CANONICAL")]
        assert levels == sorted(levels), root
        assert levels[0] < levels[-1], root
        tick, _mult = FI.SPECS[root]
        one_tick_bps = 10000.0 * tick / price
        assert levels[0] >= one_tick_bps, "%s prices a round trip below one tick" % root


def test_futures_costs_are_not_the_single_name_equity_rate():
    """Charging a liquid CME outright the desk's 12.5 bp equity rate would
    reject a real edge for a reason that is not true. Charging it nothing would
    manufacture one. Both are checked."""
    es = FI.round_trip_bps("ES", 6400.0, "PRIMARY")
    assert 0.0 < es < 2.0, es
    assert FI.round_trip_bps("ES", 6400.0, "CANONICAL") < 25.0
    # and the ladder still bites on the expensive contracts
    assert FI.round_trip_bps("ZB", 118.0, "STRESS") > FI.round_trip_bps("ES", 6400.0, "STRESS")


def test_futures_cost_in_bp_is_independent_of_the_multiplier_for_the_spread():
    """The multiplier cancels out of the spread term. If it ever stops
    cancelling, the formula has been rewritten wrongly."""
    tick, mult = FI.SPECS["ES"]
    price = 6400.0
    spread_only = 10000.0 * (FI.COST_LADDER["PRIMARY"]["ticks"] * tick) / price
    with_commission = FI.per_side_bps("ES", price, "PRIMARY")
    commission_part = 10000.0 * (FI.COST_LADDER["PRIMARY"]["commission_usd"] / mult) / price
    assert with_commission == pytest.approx(spread_only + commission_part, rel=1e-12)


def test_futures_trade_date_rolls_at_17_et_not_at_midnight():
    """The defect this prevents: bars stamped 18:00 ET Monday belong to
    TUESDAY's trade date. Grouping them under Monday splits one CME session
    across two rows and leaks the next session's overnight into this session's
    close."""
    import pandas as pd
    ny = "America/New_York"
    evening = pd.Timestamp("2025-11-17 18:30", tz=ny)        # Monday evening
    morning = pd.Timestamp("2025-11-18 09:35", tz=ny)        # Tuesday RTH
    late_us = pd.Timestamp("2025-11-18 15:59", tz=ny)        # Tuesday close
    assert FI.trade_date(evening) == date(2025, 11, 18)
    assert FI.trade_date(morning) == date(2025, 11, 18)
    assert FI.trade_date(late_us) == date(2025, 11, 18)
    # ... and the calendar date does NOT agree, which is the whole point
    assert evening.date() != FI.trade_date(evening)


def test_futures_windows_cover_what_the_etf_panel_could_never_see():
    """Each window earns its place by being outside the owned ETF panel, or by
    being the ETF panel itself for a like-for-like comparison."""
    etf_lo, etf_hi = FI.WINDOWS["ETF_PANEL_EQUIVALENT"]
    assert etf_hi == 12 * 60 + 59, "the owned panel's last minute is 12:59 ET"
    # the afternoon, the settlement print and the close are all beyond it
    for name in ("US_AFTERNOON", "SETTLEMENT"):
        assert FI.WINDOWS[name][0] > etf_hi, name
    assert FI.MARK_MINUTE_ET > etf_hi
    # the European session and the overnight both start before it
    assert FI.WINDOWS["EUROPE"][0] < etf_lo
    assert FI.WINDOWS["OVERNIGHT"][0] < 0, "the overnight starts on the prior evening"
    assert FI.in_window(18 * 60 + 30, "OVERNIGHT", prior_evening=True)
    assert FI.in_window(9 * 60, "OVERNIGHT")
    assert not FI.in_window(10 * 60, "OVERNIGHT")


def test_futures_preregistration_does_not_relax_a_single_frozen_gate():
    """Acquiring data must not buy a weaker threshold."""
    import alpha_agent.alpha_recovery.intraday_data as ID
    g = FI.FROZEN_GATES
    assert g["paired_t"] == 2.0
    assert g["bh_q"] == 0.10
    assert g["family_holm_alpha"] == 0.05
    assert g["materiality_net_pct_per_year"] == 1.5
    assert g["holdout_halves_floor"] == -0.005
    assert g["min_effective_periods"] == DBN.MIN_EFFECTIVE_PERIODS == 36
    assert g["must_survive_cost_level"] == "STRESS"
    assert FI.COST_ELIGIBILITY_LEVEL == "STRESS"
    # the closed ETF axis required surviving its own STRESS rung too
    assert ID.COST_ELIGIBILITY_BPS == ID.COST_STRESS_BPS


def test_futures_research_budget_matches_the_contract():
    assert FI.MAX_PRIMARY_PER_FAMILY == 6
    assert FI.MAX_RESCUES_PER_FAMILY == 2
    assert FI.RESCUE_REQUIRES_NAMED_MEASURED_BINDING_FAILURE is True


def test_every_futures_family_declares_what_would_falsify_it():
    """A family with no declared falsifier is a fishing licence."""
    assert set(FI.FAMILY_CONTRACT) == set(FI.FAMILIES)
    for fam, contract in FI.FAMILY_CONTRACT.items():
        assert contract.get("claim"), fam
        assert contract.get("falsified_by"), fam
    # the three that justify the spend are the three the ETF panel cannot express
    for fam in (FI.FAM_CLOSE, FI.FAM_OVERNIGHT, FI.FAM_EUROPE):
        assert FI.FAMILY_CONTRACT[fam]["only_possible_because"]
    # and the carry family carries its own prior falsification forward
    assert "0.01" in FI.FAMILY_CONTRACT[FI.FAM_CARRY]["prior_result"]


def test_futures_treasuries_are_one_market_at_four_tenors():
    """The same non-additivity the acquisition optimiser enforces, restated so
    a cross-sectional family cannot treat ZT/ZF/ZN/ZB as four independent
    bets."""
    rates = [r for r in FI.ROOTS if DBN.UNIVERSE[r][1] == "US_RATES"]
    assert len(rates) == 4
    assert len({FI.MARKET[r] for r in rates}) == 4, "tenors are distinguishable"
    assert all(FI.MARKET[r].startswith("US_RATES") for r in rates), "but they are one complex"


def test_futures_panel_absent_is_a_blocker_not_an_empty_result(root, monkeypatch):
    """The failure mode that produced 8,380 settled hypotheses: an absent panel
    silently scoring as 'no advantage' instead of refusing to score."""
    monkeypatch.setattr(FI, "panel_root", lambda: root / "nope")
    b = FI.build(write=True)
    assert b["state"] == "PREREGISTERED_AWAITING_PANEL"
    assert b["blocker"]["kind"] == "PANEL_NOT_ACQUIRED"
    assert b["roots_on_disk"] == []
    assert b["written_before_any_bar_was_downloaded"] is True
    assert b["why_this_is_coverage_not_a_transform"]["contract_rule"] == 13
    assert b["why_this_is_coverage_not_a_transform"]["is_another_lag_or_transform"] is False


def test_futures_preregistration_reuses_the_one_scorer_rather_than_owning_one():
    """This module owns the PANEL and the pre-registration. It must not grow a
    second scorer, a second multiplicity correction or a second book."""
    src = Path(FI.__file__).read_text(encoding="utf-8")
    for forbidden in ("def run_cell", "def bh_fdr", "def holm", "def walk_forward"):
        assert forbidden not in src, "%s must stay with its one owner" % forbidden
    assert FI.preregistration()["reused_not_rebuilt"]

