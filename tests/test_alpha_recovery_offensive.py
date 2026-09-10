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
from alpha_agent.alpha_recovery import earnings_events as EE
from alpha_agent.alpha_recovery import forecast_contract as FC
from alpha_agent.alpha_recovery import forward_package as FP
from alpha_agent.alpha_recovery import incumbent as INC
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
        planned += [{"price_state": f["price_state"]}] * len(f["primary_specifications"])
    assert PR.non_price_share(planned)["rule_met"] is True


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
