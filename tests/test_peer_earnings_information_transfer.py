"""Executor for PEER_EARNINGS_INFORMATION_TRANSFER_V1 - synthetic data only.

Protects: every gate path in the preregistered order (QUALIFIED only as a human gate, never
capital eligible); the point-in-time SIC rule (a later reclassification never reaches an earlier
decision); a calendar that reads nothing after the decision close; no early exit on an actual
announcement; the priced-before-entry kill; the price-state control kill; refusal of a changed
kill rule; the ``validate_result`` contract; and an untouched confirmation window after a
qualification failure.
"""
from __future__ import annotations

import functools
import json
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from alpha_agent import alpha_recovery as AR  # noqa: E402
from alpha_agent.alpha_recovery import event_8k_data as D  # noqa: E402
from alpha_agent.alpha_recovery import peer_earnings_information_transfer as PE  # noqa: E402
from alpha_agent.r59 import mechanisms as MX  # noqa: E402

DATES = pd.bdate_range("2008-01-02", "2026-09-03")
N_IND, PER_IND, N_EARLY = 8, 12, 4
N = N_IND * PER_IND
INCUMBENT = pd.Series(np.random.default_rng(99).normal(0.0004, 0.01, len(DATES)), index=DATES)
OK_FSDS = {"passes": True, "digest": "synthetic", "missing": [], "mismatched": []}


def _cik(i: int) -> str:
    return str(100000 + i)


def _filings(move: dict | None = None) -> list:
    """Four seasons a year. In each industry four EARLY reporters announce around the 18th of
    Jan/Apr/Jul/Oct (industries 4-7 one week later) and eight LATE reporters 20-27 days after."""
    out = []
    for year in range(2008, 2027):
        for m in (1, 4, 7, 10):
            base = date(year, m, 18)
            for k in range(N_IND):
                shift = 0 if k < N_IND // 2 else 7
                for j in range(PER_IND):
                    i = k * PER_IND + j
                    off = shift + (j % 2) if j < N_EARLY else shift + 20 + (j - N_EARLY)
                    if move and (i, year, m) in move:
                        off = move[(i, year, m)]
                    day = base + timedelta(days=off)
                    out.append({"issuer_cik": _cik(i), "acceptance_utc": "%sT12:00:00.000Z" % day.isoformat(),
                                "filing_date": day.isoformat(), "accession": "%d-%s" % (i, day.isoformat())})
    return out


@functools.lru_cache(maxsize=None)
def _base(seed: int = 7, industry_wide: bool = False):
    """Market + idiosyncratic returns; each (industry, season) draws a news shock carried by its
    early reporters' event-session returns (or, ``industry_wide``, by every member that session)."""
    rng = np.random.default_rng(seed)
    market = rng.normal(0.0003, 0.008, len(DATES))
    ret = market[None, :] + rng.normal(0.0, 0.012, (N, len(DATES)))
    dates64 = DATES.values.astype("datetime64[ns]")
    by_key: dict = {}
    for f in _filings():
        i = int(f["accession"].split("-")[0])
        if i % PER_IND >= N_EARLY:
            continue
        key = (f["filing_date"][:4], int(f["filing_date"][5:7]), i // PER_IND)
        by_key.setdefault(key, []).append((i, D.decision_session(dates64, f["acceptance_utc"])))
    for key in sorted(by_key):
        s = rng.normal(0.0, 0.04)
        evs = [(i, t) for i, t in by_key[key] if t is not None]
        if not evs:                                   # a season after the panel's last session
            continue
        if industry_wide:
            k = key[2]
            ret[k * PER_IND:(k + 1) * PER_IND, min(t for _, t in evs)] += s
        else:
            for i, t in evs:
                ret[i, t] += s
    return market, ret


def _sic(*, drop=(), extra=()) -> dict:
    obs = [(_cik(i), "2008-01-10T12:00:00", (20 + i // PER_IND) * 100 + 1) for i in range(N) if i not in drop]
    return PE.sic_index(obs + list(extra))


def _inputs(ret, market, *, sic=None, filings=None) -> dict:
    panel = {"dates": DATES, "symbols": ["S%d" % i for i in range(N)], "tr": np.cumprod(1.0 + ret, axis=1),
             "mem": np.ones((N, len(DATES)), dtype=bool), "spy": np.cumprod(1.0 + market), "identity": "SYNTHETIC"}
    return {"panel": panel, "row_cik": [_cik(i) for i in range(N)], "sic_index": sic if sic is not None else _sic(),
            "fsds_integrity": OK_FSDS, "filings": filings if filings is not None else _filings(),
            "identity": {"synthetic": True}}


def _cohorts(inputs: dict) -> list:
    p = inputs["panel"]
    ann, _ = PE.announcement_sessions(inputs["filings"], set(inputs["row_cik"]), p["dates"])
    dec = PE.weekly_decisions(p["dates"], PE.QUALIFICATION[0], PE.CONFIRMATION[1])
    return PE.build_cohorts(p, inputs["row_cik"], ann, inputs["sic_index"], dec)["cohorts"]


def _world(*, effect=0.0, pre_effect=0.0, momentum=0.0, conf_sign=1.0, h1_only=False,
           industry_wide=False, seed=7) -> dict:
    """Plant a response in the traded names AFTER the calendar exists. Planted returns sit on
    sessions the calendar never reads, so the cohorts are unchanged by construction."""
    market, ret0 = _base(seed, industry_wide)
    base = _inputs(ret0, market)
    tr0 = base["panel"]["tr"]
    ret = ret0.copy()
    for c in _cohorts(base):
        if not c["traded"]:
            continue
        sign = conf_sign if c["date"] >= PE.CONFIRMATION[0] else 1.0
        if h1_only and PE.HALVES["H2_2015_2019"][0] <= c["date"] <= PE.HALVES["H2_2015_2019"][1]:
            continue
        t = c["t"]
        for e in c["entrants"]:
            r, en = e["row"], e["entry"]
            if pre_effect:
                ret[r, en] += sign * pre_effect * e["signal"]
            if effect:
                ret[r, en + 1:en + 4] += sign * effect * e["signal"] / 3.0
            if momentum:
                own = tr0[r, t] / tr0[r, t - PE.PEER_LOOKBACK] - 1.0
                ret[r, en + 1:en + 4] += momentum * own / 3.0
    return _inputs(ret, market)


# --------------------------------------------------------------------------- #
# Contract
# --------------------------------------------------------------------------- #
def test_the_frozen_kill_rule_is_the_catalog_rule_verbatim_and_a_changed_rule_is_refused():
    d = json.loads((_ROOT / "research" / "alpha_agent" / "MECHANISM_FRONTIER.json").read_text(encoding="utf-8"))
    ms = d["mechanisms"] if isinstance(d["mechanisms"], list) else list(d["mechanisms"].values())
    entry = next(m for m in ms if m.get("mechanism_id") == PE.MECHANISM_ID)
    assert entry["pnl_gate"]["KILL_RULE"] == PE.KILL_RULE_FROZEN
    changed = {"mechanism_id": PE.MECHANISM_ID,
               "pnl_gate": {"KILL_RULE": PE.KILL_RULE_FROZEN.replace("t < 2.0", "t < 1.5")}}
    with pytest.raises(ValueError, match="KILL_RULE"):
        PE.run_mechanism(mechanism=changed)
    with pytest.raises(ValueError):
        PE.run_mechanism(mechanism={"mechanism_id": "OTHER", "pnl_gate": {"KILL_RULE": PE.KILL_RULE_FROZEN}})


def test_multiplicity_inherits_ten_cells_and_tests_the_p_value_for_none_explicitly():
    assert len(PE.INHERITED_NULLS) == 10 and PE.multiplicity(None)["m"] == 11
    assert PE.multiplicity(None)["passes"] is False
    assert PE.multiplicity(0.0)["passes"] is True                 # an underflowed p is the strongest evidence
    assert PE.multiplicity(0.10 / 11)["passes"] is True
    assert PE.multiplicity(0.02)["passes"] is False               # passes at m = 1, fails at m = 11
    assert PE.IC_NW_LAG == 3 and PE.COST_BPS == 12.5


# --------------------------------------------------------------------------- #
# Point in time
# --------------------------------------------------------------------------- #
def test_pit_sic_uses_only_submissions_accepted_strictly_before_the_decision_date():
    idx = PE.sic_index([("42", "2012-03-01T09:00:00", 2834), ("42", "2018-06-01T08:00:00", 7372),
                        ("0043", "2012-03-01", 4911)])
    assert PE.sic_as_of(idx, "42", "2012-03-01") is None           # accepted ON the decision date: not used
    assert PE.sic_as_of(idx, "42", "2012-03-02") == 2834
    assert PE.sic_as_of(idx, "42", "2018-06-01") == 2834           # the reclassification is not yet usable
    assert PE.sic_as_of(idx, "42", "2018-06-02") == 7372
    assert PE.sic_as_of(idx, "43", "2012-03-01") is None and PE.sic_as_of(idx, "43", "2012-03-02") == 4911
    assert PE.sic_as_of(idx, None, "2020-01-01") is None and PE.sic_as_of(idx, "99", "2020-01-01") is None


def test_a_later_sic_change_never_reaches_an_earlier_decision():
    market, ret = _base()
    change = (_cik(0), "2018-06-01T08:00:00", 2701)            # an industry-20 early reporter becomes industry 27
    before = _cohorts(_inputs(ret, market))
    after = _cohorts(_inputs(ret, market, sic=_sic(extra=[change])))

    def view(cs):
        return [(c["t"], [(e["row"], round(e["signal"], 12), e["npeers"], e["side"], e["exit"]) for e in c["entrants"]])
                for c in cs]

    cut = "2018-06-01"
    assert view([c for c in before if c["date"] <= cut]) == view([c for c in after if c["date"] <= cut])
    assert view([c for c in before if c["date"] > cut]) != view([c for c in after if c["date"] > cut])


def test_the_calendar_reads_nothing_after_the_decision_close():
    market, ret = _base()
    inputs = _inputs(ret, market)
    cs = _cohorts(inputs)
    traded = [c for c in cs if c["traded"]]
    assert len([c for c in traded if PE.QUALIFICATION[0] <= c["date"] <= PE.QUALIFICATION[1]]) == 80
    t0 = traded[40]["t"]
    poisoned = dict(inputs)
    panel = dict(inputs["panel"])
    rng = np.random.default_rng(1)
    panel["tr"] = inputs["panel"]["tr"].copy()
    panel["tr"][:, t0 + 1:] *= rng.uniform(0.5, 1.5, panel["tr"][:, t0 + 1:].shape)
    panel["spy"] = inputs["panel"]["spy"].copy()
    panel["spy"][t0 + 1:] *= 1.7
    poisoned["panel"] = panel
    cs2 = _cohorts(poisoned)
    early = [c for c in cs if c["t"] <= t0]
    early2 = [c for c in cs2 if c["t"] <= t0]
    assert [c["entrants"] for c in early] == [c["entrants"] for c in early2]
    e = traded[40]["entrants"][0]
    assert e["entry"] == t0 + PE.ENTRY_DELAY
    assert e["exit"] == min(e["entry"] + PE.HOLD_CAP, e["expected"] - PE.PRE_EXPECTED_BUFFER)
    assert sum(1 for x in traded[40]["entrants"] if x["side"] == 1) == len(traded[40]["entrants"]) // 3


def test_an_actual_announcement_inside_the_window_never_exits_early():
    market, ret = _base()
    row, year, month = 4, 2012, 7                              # a late reporter in industry 0
    moved = _filings(move={(row, year, month): 9})             # announces nine days after the base date
    cs = _cohorts(_inputs(ret, market, filings=moved))
    hit = [(c, e) for c in cs for e in c["entrants"] if e["row"] == row and c["date"].startswith("2012-07")]
    assert len(hit) == 1
    c, e = hit[0]
    assert e["actual_next_announcement"] is not None and e["entry"] < e["actual_next_announcement"] <= e["exit"]
    assert e["actual_at_or_before_planned_exit"] is True
    assert e["exit"] == min(e["entry"] + PE.HOLD_CAP, e["expected"] - PE.PRE_EXPECTED_BUFFER)
    reader = PE.PriceReader(np.cumprod(1.0 + ret, axis=1), np.cumprod(1.0 + market))
    PE.measure([c], reader, DATES, c["date"], c["date"], label="ONE", audit=[])
    assert reader.max_session_read == max(x["exit"] for x in c["entrants"])


# --------------------------------------------------------------------------- #
# Gates, end to end on synthetic data
# --------------------------------------------------------------------------- #
def test_a_reproduced_planted_effect_qualifies_only_as_a_human_gate(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    body = PE.run(verbose=False, write=True, inputs=_world(effect=1.0), incumbent_daily=INCUMBENT)
    res = body["executor_result"]
    assert res["verdict"] == "QUALIFIED", res["why"]
    assert res["capital_eligible"] is False and body["capital_eligible"] is False
    assert "HUMAN" in res["why"] and MX.validate_result(res) == []
    assert body["windows_measured"] == ["QUALIFICATION", "H1_2010_2014", "H2_2015_2019", "CONFIRMATION"]
    assert res["multiplicity"]["m"] == 11 and res["multiplicity"]["passes"] is True
    assert res["statistic"]["lockbox_t"] >= 2.0 and res["statistic"]["pre_entry_share"] < PE.PRICED_SHARE_MAX
    assert Path(res["artifact"]).exists() and Path(res["artifact"]).name == PE.ARTIFACT_NAME


def test_the_confirmation_window_is_not_read_after_a_qualification_failure():
    inputs = _world(effect=-1.0)
    res = PE.evaluate(inputs, incumbent_daily=INCUMBENT)
    assert res["executor_result"]["verdict"] == "KILLED_WRONG_SIGN", res["executor_result"]["why"]
    assert res["confirmation"] == "UNTOUCHED" and res["executor_result"]["statistic"]["lockbox_t"] is None
    assert "CONFIRMATION" not in res["windows_measured"]
    last_q_exit = max(e["exit"] for c in _cohorts(inputs) if c["traded"]
                      and PE.QUALIFICATION[0] <= c["date"] <= PE.QUALIFICATION[1] for e in c["entrants"])
    assert res["max_session_read"] == last_q_exit
    poisoned = dict(inputs)
    panel = dict(inputs["panel"])
    panel["tr"] = inputs["panel"]["tr"].copy()
    panel["tr"][:, last_q_exit + 1:] *= np.random.default_rng(3).uniform(0.2, 5.0, panel["tr"][:, last_q_exit + 1:].shape)
    poisoned["panel"] = panel
    res2 = PE.evaluate(poisoned, incumbent_daily=INCUMBENT)
    assert res2["executor_result"]["verdict"] == res["executor_result"]["verdict"]
    assert res2["qualification"] == res["qualification"] and res2["confirmation"] == "UNTOUCHED"


def test_a_move_completed_before_entry_is_killed_as_priced_before_entry():
    res = PE.evaluate(_world(pre_effect=1.0), incumbent_daily=INCUMBENT)["executor_result"]
    assert res["verdict"] == "KILLED_PRICED_BEFORE_ENTRY", res["why"]
    assert res["kill_rule_fired"] == "PRICED_BEFORE_ENTRY" and res["statistic"]["lockbox_t"] is None
    out = PE.evaluate(_world(pre_effect=2.0, effect=0.5), incumbent_daily=INCUMBENT)
    res = out["executor_result"]
    assert out["gate_inputs"]["ic_t"] >= 2.0                    # the holding window still earns ...
    assert res["verdict"] == "KILLED_PRICED_BEFORE_ENTRY" and res["gate"] == "PRICED_BEFORE_ENTRY", res["why"]
    assert out["gate_inputs"]["pre_share"] >= PE.PRICED_SHARE_MAX   # ... but most of the move came first
    assert out["confirmation"] == "UNTOUCHED"


def test_a_signal_that_only_restates_price_state_is_killed_by_the_control():
    out = PE.evaluate(_world(industry_wide=True, momentum=1.0), incumbent_daily=INCUMBENT)
    res = out["executor_result"]
    assert out["gate_inputs"]["ic_t"] >= 2.0 and out["gate_inputs"]["pre_share"] < PE.PRICED_SHARE_MAX
    assert res["verdict"] == "KILLED_NONINCREMENTAL" and res["gate"] == "PRICE_STATE_CONTROL", res["why"]
    assert out["confirmation"] == "UNTOUCHED"


def test_an_effect_confined_to_one_half_is_killed_unstable():
    out = PE.evaluate(_world(effect=1.0, h1_only=True), incumbent_daily=INCUMBENT)
    res = out["executor_result"]
    assert res["verdict"] == "KILLED_UNSTABLE" and res["gate"] == "HALVES", res["why"]
    assert out["gate_inputs"]["halves"]["H2_2015_2019"]["ann_net"] < 0.0


def test_thin_pit_sic_coverage_is_a_data_hold_before_any_traded_return_is_read():
    market, ret = _base()
    out = PE.evaluate(_inputs(ret, market, sic=_sic(drop=range(12))), incumbent_daily=INCUMBENT)
    res = out["executor_result"]
    assert res["verdict"] == "DATA_HOLD" and res["kill_rule_fired"] == "DATA", res["why"]
    assert "PIT SIC covers 87.5%" in res["why"]
    assert out["windows_measured"] == [] and out["max_session_read"] == -1
    assert MX.validate_result(res) == [] and res["capital_eligible"] is False


def test_the_agent_contract_holds_on_a_missing_input_and_on_a_full_run(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    entry = {"mechanism_id": PE.MECHANISM_ID, "pnl_gate": {"KILL_RULE": PE.KILL_RULE_FROZEN}}

    def missing():
        raise FileNotFoundError("no FSDS")

    monkeypatch.setattr(PE, "load_inputs", missing)
    out = PE.run_mechanism(mechanism=entry)
    assert out["verdict"] == "DATA_HOLD" and MX.validate_result(out) == [] and out["capital_eligible"] is False
    market, ret = _base()
    monkeypatch.setattr(PE, "load_inputs", lambda: _inputs(ret, market, sic=_sic(drop=range(12))))
    import alpha_agent.alpha_recovery.intraday_alpha as IA
    monkeypatch.setattr(IA, "incumbent_daily_path", lambda: INCUMBENT)
    out = PE.run_mechanism(mechanism=entry)
    assert out["verdict"] == "DATA_HOLD" and MX.validate_result(out) == []
    assert Path(out["artifact"]).exists()


# --------------------------------------------------------------------------- #
# Every gate path, in order (pure gate functions)
# --------------------------------------------------------------------------- #
PASS = {"data_hold": None, "ic_mean": 0.05, "ic_t": 3.0, "p_ic": 0.001, "pre_ic_mean": 0.0, "pre_ic_t": 0.1,
        "pre_share": 0.1, "resid_ic_t": 2.5, "ann_net": 0.03,
        "halves": {"H1_2010_2014": {"ann_net": 0.02, "ic_mean": 0.04},
                   "H2_2015_2019": {"ann_net": 0.01, "ic_mean": 0.03}},
        "bh_pass": True,
        "incumbent": {"state": "OK", "incremental_ann_net_return": 0.02, "t_incremental": 2.5,
                      "positive_incremental_utility_after_costs": True}}
CARRIED = {"pre_ic_mean": 0.06, "pre_ic_t": 3.5}


@pytest.mark.parametrize("over, verdict, gate", [
    ({"data_hold": "thin PIT SIC"}, "DATA_HOLD", "DATA"),
    ({"data_hold": "thin PIT SIC", "ic_mean": -1.0}, "DATA_HOLD", "DATA"),
    ({"ic_mean": -0.01}, "KILLED_WRONG_SIGN", "FROZEN_SIGN"),
    ({"ic_mean": -0.01, **CARRIED}, "KILLED_PRICED_BEFORE_ENTRY", "FROZEN_SIGN"),
    ({"ic_t": 1.99}, "NO_EDGE", "SIGNED_RANK_IC_T"),
    ({"ic_t": None}, "NO_EDGE", "SIGNED_RANK_IC_T"),
    ({"ic_t": 1.99, **CARRIED}, "KILLED_PRICED_BEFORE_ENTRY", "SIGNED_RANK_IC_T"),
    ({"ic_t": 1.0, "ann_net": 0.0}, "NO_EDGE", "SIGNED_RANK_IC_T"),
    ({"pre_share": 0.50}, "KILLED_PRICED_BEFORE_ENTRY", "PRICED_BEFORE_ENTRY"),
    ({"pre_share": None}, "KILLED_PRICED_BEFORE_ENTRY", "PRICED_BEFORE_ENTRY"),
    ({"resid_ic_t": 1.9}, "KILLED_NONINCREMENTAL", "PRICE_STATE_CONTROL"),
    ({"resid_ic_t": 1.9, "ann_net": 0.0}, "KILLED_NONINCREMENTAL", "PRICE_STATE_CONTROL"),
    ({"ann_net": 0.0149}, "KILLED_BELOW_MATERIALITY", "MATERIALITY"),
    ({"halves": {"H1_2010_2014": {"ann_net": 0.02, "ic_mean": 0.04},
                 "H2_2015_2019": {"ann_net": -0.001, "ic_mean": 0.03}}}, "KILLED_UNSTABLE", "HALVES"),
    ({"halves": {"H1_2010_2014": {"ann_net": 0.02, "ic_mean": 0.0},
                 "H2_2015_2019": {"ann_net": 0.01, "ic_mean": 0.03}}}, "KILLED_UNSTABLE", "HALVES"),
    ({"bh_pass": False}, "KILLED_MULTIPLICITY", "BH_INHERITED"),
    ({"p_ic": None}, "KILLED_MULTIPLICITY", "BH_INHERITED"),
    ({"bh_pass": False, "incumbent": {"state": "DATA_HOLD"}}, "KILLED_MULTIPLICITY", "BH_INHERITED"),
    ({"incumbent": {"state": "DATA_HOLD", "why": "no path"}}, "DATA_HOLD", "INCUMBENT_DATA"),
    ({"incumbent": dict(PASS["incumbent"], incremental_ann_net_return=0.0149)},
     "NO_INCREMENTAL_INFORMATION_EDGE", "INCUMBENT"),
    ({"incumbent": dict(PASS["incumbent"], positive_incremental_utility_after_costs=False)},
     "NO_INCREMENTAL_INFORMATION_EDGE", "INCUMBENT"),
])
def test_each_qualification_gate_fires_in_the_preregistered_order(over, verdict, gate):
    v = PE.qualification_gate(dict(PASS, **over))
    assert v is not None and v["verdict"] == verdict and v["gate"] == gate, v
    assert v["verdict"] in MX.EXECUTOR_VERDICTS


def test_a_qualification_that_passes_every_gate_returns_none():
    assert PE.qualification_gate(dict(PASS)) is None
    assert PE.qualification_gate(dict(PASS, p_ic=0.0)) is None


C_PASS = {"cohorts": 50, "ic_mean": 0.04, "ic_t": 2.5, "p_ic": 0.006, "pre_share": 0.1, "ann_net": 0.02}


@pytest.mark.parametrize("over, verdict, gate", [
    ({"data_hold": "thin"}, "DATA_HOLD", "CONFIRMATION_DATA"),
    ({"cohorts": PE.MIN_EFFECTIVE_PERIODS - 1}, "NEED_MORE_EVIDENCE", "CONFIRMATION_SAMPLE"),
    ({"ic_mean": -0.02}, "KILLED_UNSTABLE", "CONFIRMATION_SIGN"),
    ({"ic_t": 1.9}, "NO_EDGE", "CONFIRMATION_IC_T"),
    ({"p_ic": None}, "NO_EDGE", "CONFIRMATION_P"),
    ({"pre_share": 0.6}, "KILLED_PRICED_BEFORE_ENTRY", "CONFIRMATION_PRICED_BEFORE_ENTRY"),
    ({"ann_net": 0.01}, "KILLED_BELOW_MATERIALITY", "CONFIRMATION_MATERIALITY"),
    ({}, "QUALIFIED", None),
    ({"p_ic": 0.0}, "QUALIFIED", None),
])
def test_each_confirmation_gate_path(over, verdict, gate):
    v = PE.confirmation_gate(dict(C_PASS, **over))
    assert v["verdict"] == verdict and v["gate"] == gate, v
    if verdict == "QUALIFIED":
        r = PE._result(v, {}, {}, PE.multiplicity(0.001), {})      # noqa: SLF001
        assert r["capital_eligible"] is False and MX.validate_result(r) == []
