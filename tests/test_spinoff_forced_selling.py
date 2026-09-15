"""Executor and data layer for SPINOFF_PARENT_HOLDER_FORCED_SELLING_V1 - synthetic data only.

Pinned: the contract (kill rule, mechanism id), the distribution / exclusion / listing extraction (the calibration
traps: Section 368 "plan of reorganization" and "blank check preferred stock" are NOT exclusions; OTC listings are
not listings), registrant grouping, the link states (a sibling's symbol does not link without a name, a renamed
spinco links by its later EDGAR name), the point-in-time rule, the 21st-quote entry and 146th-quote exit, the slot
accounting with the hedge, costs and the slot cap, the gate order, the unread confirmation after a failure, the data
holds and the Alpha Agent executor contract.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from alpha_agent import alpha_recovery as AR  # noqa: E402
from alpha_agent.alpha_recovery import spinoff_data as SD  # noqa: E402
from alpha_agent.alpha_recovery import spinoff_forced_selling as SF  # noqa: E402
from alpha_agent.r59 import mechanisms as MX  # noqa: E402

CATALOG = _ROOT / "research" / "alpha_agent" / "MECHANISM_FRONTIER.json"
SESSIONS = pd.bdate_range("1995-01-02", "2026-09-10")


def _entry():
    body = json.loads(CATALOG.read_text(encoding="utf-8"))
    return next(m for m in body["mechanisms"] if m["mechanism_id"] == SF.MECHANISM_ID)


def test_01_the_kill_rule_is_the_catalogs_byte_for_byte():
    assert _entry()["pnl_gate"]["KILL_RULE"] == SF.KILL_RULE_FROZEN


def test_02_a_changed_contract_is_refused():
    entry = _entry()
    with pytest.raises(ValueError):
        SF.run_mechanism(mechanism={**entry, "mechanism_id": "OTHER"})
    with pytest.raises(ValueError):
        SF.run_mechanism(mechanism=dict(entry, pnl_gate=dict(entry["pnl_gate"], KILL_RULE="Kill if nothing.")))


def test_03_multiplicity_is_one_cell_plus_three_inherited_nulls():
    assert SF.multiplicity(0.02)["m"] == 4 and SF.multiplicity(0.02)["passes"] is True
    assert SF.multiplicity(0.03)["passes"] is False and SF.multiplicity(None)["passes"] is False
    assert SF.multiplicity(0.5)["single_survivor_threshold"] == pytest.approx(0.025)


def test_04_extraction_calibration_traps():
    text = ("This information statement is furnished in connection with the distribution by Parent of all of the "
            "outstanding shares of our common stock on a pro rata basis. The distribution is intended to qualify as a "
            "plan of reorganization within the meaning of Section 368. Our charter authorizes blank check preferred "
            "stock. The Company has filed an application to list the Common Stock on the New York Stock Exchange "
            "(the \"NYSE\") under the symbol \"NEWC.\" Parent's common stock is listed on the NYSE under the symbol "
            "\"PRNT\". Its warrants trade on the OTC Bulletin Board under the symbol \"WRNT\".")
    s = SD.extract_spinoff(text)
    assert s["distribution_language"] and not s["not_spin_language"]
    assert [t["ticker"] for t in s["symbols"]] == ["NEWC", "PRNT"]
    assert SD.extract_spinoff("See “The Spin-Off” and “Dividend Policy”.")["distribution_language"] is True
    for bad in ("the company emerged from chapter 11 and its plan of reorganization",
                "we have elected to be regulated as a business development company", "we are a blank check company"):
        assert SD.extract_spinoff("a pro rata distribution; " + bad)["not_spin_language"] is True
    assert SD.name_tokens("Consolidated Freightways Corp /DE/") == ["CONSOLIDATED", "FREIGHTWAYS"]


def _row(cik, date, acc, company=None, form="10-12B"):
    return {"cik": cik, "company": company or "Zeta%s Holdings Inc" % cik, "form": form, "date": date, "path": "x",
            "accession": acc}


def _parsed(acc, *, spin=True, symbols=()):
    return {acc: {"accession": acc, "spinoff": {"separation_language": spin, "distribution_language": spin,
                                                "not_spin_language": False,
                                                "symbols": [{"ticker": t, "kind": "SYMBOL_STATEMENT", "at": 0}
                                                            for t in symbols]}}}


def test_05_registrants_and_every_link_state():
    rows = [_row("101", "2015-01-05", "a1"), _row("101", "2015-02-10", "a2", form="10-12B/A"),
            _row("102", "2015-01-05", "b1"), _row("103", "2015-01-05", "c1", company="Wyndco Corp"),
            _row("104", "2015-01-05", "d1", company="Latey Inc"), _row("105", "2015-01-05", "e1", company="Imatco Corp"),
            _row("106", "2015-01-05", "f1", company="Doubleco Inc"), _row("107", "2015-01-05", "g1", company="Nameless Inc")]
    parsed = {**_parsed("a1"), **_parsed("a2", symbols=("NEWC", "PRNT")), **_parsed("b1", spin=False, symbols=("NOPE",)),
              **_parsed("c1", symbols=("RLGY",)), **_parsed("d1", symbols=("LATE",)), **_parsed("e1", symbols=("IMAT",)),
              **_parsed("f1", symbols=("DUBL",)), **_parsed("g1")}
    regs = {g["cik"]: g for g in SD.registrants(rows, parsed)}
    assert regs["101"]["is_spinoff"] and regs["101"]["symbols"] == ["NEWC", "PRNT"] and regs["101"]["filings"] == 2
    assert not regs["102"]["is_spinoff"]
    universe = {"NEWC": [("NEWC", "2015-03-02")], "PRNT": [("PRNT", "1970-01-02")], "RLGY": [("RLGY", "2015-02-01")],
                "WYND": [("WYND", "2015-02-01")], "LATE": [("LATE", "2017-01-03")], "GLAS": [("GLAS-201901", "2015-03-01")],
                "DUBL": [("DUBL", "2015-03-01"), ("DUBL-201901", "2015-03-02")],
                "NML": [("NML", "2015-04-01"), ("NMLB-201801", "2015-04-02")]}
    names = {"NEWC": "Zeta101 Holdings Inc Common", "PRNT": "Parent Corp", "RLGY": "Realco Corp Common",
             "WYND": "Wyndco Corp Common", "LATE": "Latey Inc", "GLAS-201901": "Glassco Enterprises Inc Common",
             "DUBL": "Doubleco Inc Common", "DUBL-201901": "Doubleco Inc Class B",
             "NML": "Nameless Inc Common", "NMLB-201801": "Nameless Inc Class B"}
    edgar = {"105": {"names": ["Imatco Corp", "Glassco Enterprises, Inc."], "latest": "Glassco Enterprises, Inc.",
                     "record": True}}
    links = {g["cik"]: g for g in SD.link_norgate([g for g in regs.values() if g["is_spinoff"]], universe, names, edgar)}
    assert links["101"]["link_state"] == "SYMBOL" and links["101"]["link"] == ("NEWC", "2015-03-02", "NEWC")
    assert links["103"]["link_state"] == "NAME" and links["103"]["link"][0] == "WYND"      # sibling symbol refused
    assert links["104"]["link_state"] == "UNCONFIRMED_SYMBOL" or links["104"]["link_state"] == "NO_QUOTE_IN_WINDOW"
    assert links["105"]["link_state"] == "NAME" and links["105"]["link"][0] == "GLAS-201901"  # renamed spinco
    assert links["106"]["link_state"] == "SYMBOL_AMBIGUOUS"
    assert links["107"]["link_state"] == "NAME_AMBIGUOUS"


def test_06_classification_stated_only_after_entry_is_not_public_and_the_quote_rule_is_exact():
    q = [str(d.date()) for d in SESSIONS[(SESSIONS >= "2015-03-02")][:300]]
    rows = [_row("201", "2015-01-05", "a1"), _row("201", q[40], "a2", form="10-12B/A"), _row("202", "2015-01-05", "b1")]
    parsed = {**_parsed("a1", spin=False, symbols=("LATE",)), **_parsed("a2", symbols=("LATE",)),
              **_parsed("b1", symbols=("GOOD",))}
    universe = {"LATE": [("LATE", q[0])], "GOOD": [("GOOD", q[0])]}
    names = {"LATE": "Zeta201 Holdings Inc Common", "GOOD": "Zeta202 Holdings Inc Common"}
    edgar = {c: {"names": ["Zeta%s Holdings Inc" % c], "latest": "Zeta%s Holdings Inc" % c, "record": True}
             for c in ("201", "202")}
    ev = SF.pit_events(rows, parsed, universe, {"LATE": q, "GOOD": q}, names, edgar)
    assert ev["link_states"]["NOT_PUBLIC_BY_ENTRY"] == 1 and len(ev["events"]) == 1
    e = ev["events"][0]
    assert (e["symbol"], e["entry"], e["exit"], e["full_hold"]) == ("GOOD", q[20], q[145], True)
    assert ev["link_coverage"] == 1.0 and ev["submissions_share"] == 1.0


def _px(dates, closes, opens=None):
    closes = np.asarray(closes, dtype=float)
    return pd.DataFrame({"Open": closes if opens is None else opens, "Close": closes}, index=pd.DatetimeIndex(dates))


def test_07_growth_and_slot_accounting_with_hedge_costs_and_the_cap():
    hold = SESSIONS[5000:5010]
    px = _px(hold, np.linspace(11, 20, 10), opens=np.r_[10.0, np.linspace(11, 19, 9)]).drop(index=hold[3])
    g = SF.event_growth(px, str(hold[0].date()), str(hold[-1].date()), SESSIONS)
    assert g.iloc[0] == pytest.approx(1.1) and g.iloc[3] == pytest.approx(g.iloc[2])
    hedge = pd.Series(100.0, index=SESSIONS)
    ev = {"entry": str(hold[0].date()), "exit": str(hold[-1].date()), "symbol": "S"}
    b = SF.book([ev], {"S": px}, hedge, SESSIONS, cost_bps=25.0)
    c, ch = 25e-4, 1e-4
    total = SF.SLOT * ((g.iloc[-1] - 1.0) - (c + ch) - (c * g.iloc[-1] + ch))
    assert b["pnl"].sum() == pytest.approx(total) and b["events_taken"] == 1
    many = [dict(ev) for _ in range(SF.MAX_SLOTS + 1)]
    assert SF.book(many, {"S": px}, hedge, SESSIONS, cost_bps=0.0)["events_skipped"] == 1


def _inputs(excess_per_event: float, *, conf_excess=None, per_year: int = 10, coverage_break: bool = False, seed=4):
    rng = np.random.default_rng(seed)
    rows, parsed, universe, quotes, names, prices, edgar = [], {}, {}, {}, {}, {}, {}
    k = 0
    for y in range(1996, 2027):
        starts = SESSIONS[(SESSIONS >= "%d-01-01" % y) & (SESSIONS <= "%d-12-31" % y)]
        for j in range(per_year):
            first = starts[int(j * len(starts) / per_year)]
            i0 = SESSIONS.get_loc(first)
            qd = SESSIONS[i0:i0 + 160]
            if len(qd) < 150:
                continue
            k += 1
            cik, t, acc, company = str(10000 + k), "T%05d" % k, "acc%06d" % k, "Kco%05d Holdings Inc" % k
            rows.append(_row(cik, str((first - pd.Timedelta(days=60)).date()), acc, company=company))
            parsed.update(_parsed(acc, symbols=(t,)))
            edgar[cik] = {"names": [company], "latest": company, "record": True}
            universe[t] = [(t, str(first.date()))]
            names[t] = "Kco%05d Holdings Inc Common" % k
            if coverage_break and k % 3 == 0:
                names[t] = "Unrelated Other Common"
            quotes[t] = [str(d.date()) for d in qd]
            eff = excess_per_event if (conf_excess is None or y < 2013) else conf_excess
            r = rng.normal(0.0, 0.02, len(qd))
            r[20:146] += eff / 126.0
            close = 20.0 * np.cumprod(1.0 + r)
            prices[t] = _px(qd, close, opens=np.r_[close[0], close[:-1]])
    lv = pd.Series(100.0 * np.cumprod(1.0 + rng.normal(0.0002, 0.01, len(SESSIONS))), index=SESSIONS)
    ctrl = pd.DataFrame({s: 100.0 * np.cumprod(1.0 + rng.normal(0.0002, 0.01, len(SESSIONS))) for s in SF.CONTROL_SYMBOLS},
                        index=SESSIONS)
    return {"rows": rows, "parsed": parsed, "universe": universe, "names": names, "edgar": edgar, "prices": prices,
            "quote_dates": quotes, "sessions": SESSIONS, "hedge": lv, "controls": ctrl, "failed_index_quarters": [],
            "price_load_failed": []}


def test_08_a_planted_recovery_qualifies_and_is_never_capital_eligible(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    body = SF.run(verbose=False, write=True, inputs=_inputs(0.25))
    res = body["result"]
    assert res["verdict"] == "QUALIFIED", res["why"]
    assert body["capital_eligible"] is False and res["untouched_confirmation"] == "CONFIRMED"
    assert res["data"]["qualification_events"] == 170 and res["multiplicity"]["m"] == 4
    assert MX.validate_result(SF.executor_result(body)) == []


def test_09_a_wrong_sign_is_no_edge_and_the_confirmation_is_unread():
    res = SF.run(verbose=False, write=False, inputs=_inputs(-0.25))["result"]
    assert res["verdict"] == "NO_EDGE" and res["gate"] == "WRONG_SIGN"
    assert res["confirmation"]["state"] == "UNREAD" and res["untouched_confirmation"] == "NOT_READ"
    assert all(r[0] <= SF.QUALIFICATION[1] for r in res["qualification_event_rows"])


def test_10_a_planted_effect_that_does_not_reproduce_fails_at_confirmation():
    res = SF.run(verbose=False, write=False, inputs=_inputs(0.25, conf_excess=-0.25))["result"]
    assert res["verdict"] == "NO_EDGE" and res["gate"] == "CONFIRMATION" and res["untouched_confirmation"] == "FAILED"


def test_11_weak_links_or_a_missing_index_quarter_are_a_data_hold():
    res = SF.run(verbose=False, write=False, inputs=_inputs(0.25, coverage_break=True))["result"]
    assert res["verdict"] == "DATA_HOLD" and "IDENTITY_RESOLUTION" in res["why"]
    inp = _inputs(0.25)
    inp["failed_index_quarters"] = ["2004Q2"]
    res = SF.run(verbose=False, write=False, inputs=inp)["result"]
    assert res["verdict"] == "DATA_HOLD" and "INDEX_QUARTERS_MISSING" in res["why"]


def test_12_the_executor_contract_is_what_the_agent_validates(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    inp = _inputs(0.0)
    monkeypatch.setattr(SF, "load_inputs", lambda: inp)
    out = SF.run_mechanism(mechanism=_entry())
    assert MX.validate_result(out) == [] and out["capital_eligible"] is False

    def boom():
        raise OSError("norgate is not running")
    monkeypatch.setattr(SF, "load_inputs", boom)
    held = SF.run_mechanism(mechanism=_entry())
    assert held["verdict"] == "DATA_HOLD" and MX.validate_result(held) == []
