"""Executor for SCHEDULED_EVENT_EQUITY_PREMIUM_V1 - synthetic data only.

No vendor, no network, no real price. Pinned: the contract (kill rule, mechanism id), the frozen
calendar parsers (scheduled meetings only, cross-month and slash-month headings, the close-meeting
merge, headline release days), the session mapping (a holiday release is held over the next
session), the book arithmetic (bill rate strictly before the session, one round trip per window),
the gate order, the unread confirmation after a qualification failure, the multiplicity burden and
the Alpha Agent executor contract.
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
from alpha_agent.alpha_recovery import scheduled_event_data as SD  # noqa: E402
from alpha_agent.alpha_recovery import scheduled_event_equity_premium as SE  # noqa: E402
from alpha_agent.r59 import mechanisms as MX  # noqa: E402

CATALOG = _ROOT / "research" / "alpha_agent" / "MECHANISM_FRONTIER.json"


def _entry():
    body = json.loads(CATALOG.read_text(encoding="utf-8"))
    return next(m for m in body["mechanisms"] if m["mechanism_id"] == SE.MECHANISM_ID)


def test_01_the_kill_rule_is_the_catalogs_byte_for_byte():
    assert _entry()["pnl_gate"]["KILL_RULE"] == SE.KILL_RULE_FROZEN


def test_02_a_changed_contract_is_refused():
    entry = _entry()
    with pytest.raises(ValueError):
        SE.run_mechanism(mechanism={**entry, "mechanism_id": "OTHER"})
    with pytest.raises(ValueError):
        SE.run_mechanism(mechanism=dict(entry, pnl_gate=dict(entry["pnl_gate"], KILL_RULE="Kill if nothing.")))


def test_03_multiplicity_is_two_cells_plus_four_inherited_nulls():
    m = SE.multiplicity({"PRE_FOMC": 0.01, "ANNOUNCEMENT_DAY": 0.9})
    assert m["m"] == 6 and m["passes"]["PRE_FOMC"] is True and m["passes"]["ANNOUNCEMENT_DAY"] is False
    assert abs(m["single_survivor_threshold"] - 0.10 / 6) < 1e-12
    assert SE.multiplicity({"PRE_FOMC": 0.02, "ANNOUNCEMENT_DAY": 0.9})["passes"]["PRE_FOMC"] is False
    assert SE.multiplicity({"PRE_FOMC": None, "ANNOUNCEMENT_DAY": None})["m"] == 6


def test_04_history_headings_keep_only_scheduled_meetings():
    html = "".join("<h5 class='x'>%s</h5>" % h for h in (
        "January 29-30 Meeting - 2013", "April/May 30-1 Meeting - 2013", "October 16 (unscheduled) - 2013",
        "January 3 Conference Call - 2013", "March 17-18 (cancelled) Meeting - 2013",
        "March 19 (notation vote) - 2013", "Jan/Feb 31-1 Meeting - 2013", "December 17&ndash;18 Meeting - 2013",
        "January 31-February 1 Meeting - 2013", "FOMC Search", "Meeting calendars - 2012"))
    rows = SD.parse_history_page(html, 2013)
    kinds = {r["decision_day"]: r["kind"] for r in rows}
    assert kinds["2013-01-30"] == SD.K_SCHEDULED and kinds["2013-05-01"] == SD.K_SCHEDULED
    assert kinds["2013-02-01"] == SD.K_SCHEDULED and kinds["2013-12-18"] == SD.K_SCHEDULED
    assert kinds["2013-10-16"] == SD.K_UNSCHEDULED and kinds["2013-01-03"] == SD.K_CALL
    assert kinds["2013-03-18"] == SD.K_CANCELLED and kinds["2013-03-19"] == SD.K_NOTATION
    days, merged = SD.merge_close_meetings(["2003-08-12", "2003-09-15", "2003-09-16", "2003-10-28"])
    assert days == ["2003-08-12", "2003-09-16", "2003-10-28"] and merged == [{"dropped": "2003-09-15",
                                                                              "kept": "2003-09-16"}]


def test_05_the_calendar_page_parses_cross_month_blocks_and_votes():
    block = ('<div class="fomc-meeting__month col"><strong>%s</strong></div>'
             '<div class="fomc-meeting__date col">%s</div>')
    html = ('<h4><a id="1">2023 FOMC Meetings</a></h4>' + block % ("Jan/Feb", "31-1") + block % ("March", "21-22*")
            + '<h4><a id="2">2025 FOMC Meetings</a></h4>' + block % ("August", "22 (notation vote)")
            + block % ("Oct/Nov", "31-1"))
    rows = SD.parse_calendar_page(html)
    got = {r["decision_day"]: r["kind"] for r in rows["rows"]}
    assert rows["problems"] == []
    assert got == {"2023-02-01": SD.K_SCHEDULED, "2023-03-22": SD.K_SCHEDULED,
                   "2025-08-22": SD.K_NOTATION, "2025-11-01": SD.K_SCHEDULED}


def test_06_headline_release_days_are_first_publication_per_reference_month(tmp_path):
    macro = tmp_path / "_data_macro"
    macro.mkdir()
    obs = [{"date": "2020-01-01", "realtime_start": "2020-02-13"}, {"date": "2020-01-01", "realtime_start": "2020-03-11"},
           {"date": "2020-02-01", "realtime_start": "2020-03-11"}]
    (macro / Path(SD.FROZEN_CAPTURES["CPI_INITIAL_RELEASES"]).name).write_text(json.dumps({"observations": obs}))
    first = SD.initial_release_days("CPI_INITIAL_RELEASES", root=tmp_path)
    assert first == {"2020-01-01": "2020-02-13", "2020-02-01": "2020-03-11"}


def test_07_a_holiday_release_is_held_over_the_next_session():
    sessions = pd.DatetimeIndex(["2015-04-01", "2015-04-02", "2015-04-06", "2015-04-07"])
    held = SE.held_sessions(["2015-04-03", "2015-04-02"], sessions)       # Good Friday, Thursday
    assert held == {pd.Timestamp("2015-04-06"): ["2015-04-03"], pd.Timestamp("2015-04-02"): ["2015-04-02"]}


def test_08_book_uses_the_prior_bill_rate_and_one_round_trip_per_window():
    idx = pd.bdate_range("2020-01-06", periods=7)
    px = pd.Series([100.0, 101.0, 102.0, 101.0, 103.0, 104.0, 104.0], index=idx)
    rates = pd.Series([2.52, 99.0, 2.52, 2.52, 50.0, 2.52, 2.52], index=idx)   # a spike dated ON a held session
    frame = SE.daily_frame(px, rates)
    assert frame.index[0] == idx[1]
    held = {idx[3], idx[4]}                                                      # two consecutive sessions
    bk = SE.book(frame, held, 1.0)
    c = 2.52 / 100.0 / 252.0
    r3, r4 = 101.0 / 102.0 - 1.0, 103.0 / 101.0 - 1.0
    assert bk.loc[idx[3], "x"] == pytest.approx(r3 - c - 1e-4)                   # prior obs 2.52, entry cost
    assert bk.loc[idx[4], "x"] == pytest.approx(r4 - c - 1e-4)                   # 50.0 is dated on idx[4]: unused
    assert int(bk["entry"].sum()) == 1 and int(bk["exit"].sum()) == 1
    assert (bk.loc[[idx[1], idx[2], idx[5], idx[6]], "x"] == 0.0).all()


# --------------------------------------------------------------------------- #
# Synthetic estate for the gate order
# --------------------------------------------------------------------------- #
def _calendars(sessions: pd.DatetimeIndex) -> dict:
    fomc, cpi, emp = [], [], []
    frame = pd.Series(sessions, index=sessions)
    for (y, m), g in frame.groupby([sessions.year, sessions.month]):
        if not 1994 <= y <= 2026 or len(g) < 16:
            continue
        emp.append(str(g.iloc[3].date()))
        cpi.append(str(g.iloc[9].date()))
        if m in (1, 3, 5, 6, 7, 9, 10, 12):
            fomc.append(str(g.iloc[14].date()))
    lo, hi = SE.EVENT_SPAN
    per_year = {str(y): sum(1 for d in fomc if d[:4] == str(y)) for y in SE.FOMC_YEARS}
    return {"events": {"FOMC": [d for d in fomc if lo <= d <= hi], "CPI": [d for d in cpi if lo <= d <= hi],
                       "EMPLOYMENT": [d for d in emp if lo <= d <= hi]},
            "fomc_per_year": per_year, "fomc_problems": [], "merged_headings": [],
            "headline_days_off_release_calendar": {"CPI": [], "EMPLOYMENT": []}}


def _estate(effect: float, *, conf_effect=None, seed: int = 7):
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range("1993-06-01", "2026-12-31")
    cal = _calendars(idx)
    r = pd.Series(rng.normal(0.0002, 0.008, len(idx)), index=idx)
    for d in cal["events"]["FOMC"]:
        t = pd.Timestamp(d)
        eff = effect if (conf_effect is None or t < pd.Timestamp(SE.CONFIRMATION[0])) else conf_effect
        r.loc[t] += eff
    spy = (1.0 + r).cumprod() * 100.0
    dtb3 = pd.Series(3.0, index=idx)
    return spy, dtb3, cal


def test_09_a_wrong_sign_is_no_edge_and_leaves_the_confirmation_unread():
    spy, dtb3, cal = _estate(-0.012)
    body = SE.run(verbose=False, write=False, spy_tr=spy, dtb3=dtb3, calendars=cal, integ={"problems": []})
    res = body["result"]
    assert res["verdict"] == "NO_EDGE" and res["cells"]["PRE_FOMC"]["gate"] == SE.G_WRONG_SIGN
    assert all(res["cells"][c]["confirmation"] == {"state": "UNREAD",
                                                   "why": "read only after every qualification gate passes"}
               for c in SE.CELLS)
    assert res["untouched_confirmation"] == "NOT_READ" and body["capital_eligible"] is False


def test_10_a_broken_calendar_or_hash_is_a_data_hold():
    spy, dtb3, cal = _estate(0.012)
    cal["fomc_per_year"]["2003"] = 3
    res = SE.run(verbose=False, write=False, spy_tr=spy, dtb3=dtb3, calendars=cal, integ={"problems": []})["result"]
    assert res["verdict"] == "DATA_HOLD" and "FOMC_2003_HAS_3_SCHEDULED_DECISION_DAYS" in res["why"]
    spy, dtb3, cal = _estate(0.012)
    res = SE.run(verbose=False, write=False, spy_tr=spy, dtb3=dtb3, calendars=cal,
                 integ={"problems": ["CAPTURE_HASH_CPI_RELEASE_DATES: observed x, frozen y"]})["result"]
    assert res["verdict"] == "DATA_HOLD" and res["cells"] == {}


def test_11_the_integrity_check_names_an_altered_capture(tmp_path):
    out = SE.integrity(capture_root=tmp_path, history_root=tmp_path, dtb3_path=tmp_path / "absent.json")
    assert any(p.startswith("CAPTURE_HASH_FOMC_CALENDAR") for p in out["problems"])
    assert any(p.startswith("DTB3_HASH") for p in out["problems"])
    assert "FOMC_HISTORY_PAGE_1994_MISSING_OR_ALTERED" in out["problems"]


def test_12_a_planted_effect_that_does_not_reproduce_fails_at_confirmation():
    spy, dtb3, cal = _estate(0.012, conf_effect=-0.012)
    res = SE.run(verbose=False, write=False, spy_tr=spy, dtb3=dtb3, calendars=cal, integ={"problems": []})["result"]
    pre = res["cells"]["PRE_FOMC"]
    assert pre["confirmation"]["state"] == "READ" and pre["gate"] == SE.G_CONFIRMATION
    assert res["verdict"] == "NO_EDGE" and res["untouched_confirmation"] == "FAILED"


def test_13_a_reproduced_planted_effect_qualifies_and_is_never_capital_eligible(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    spy, dtb3, cal = _estate(0.012)
    body = SE.run(verbose=False, write=True, spy_tr=spy, dtb3=dtb3, calendars=cal, integ={"problems": []})
    res = body["result"]
    assert res["cells"]["PRE_FOMC"]["gate"] is None and res["verdict"] == "QUALIFIED"
    assert res["untouched_confirmation"] == "CONFIRMED" and body["capital_eligible"] is False
    assert res["multiplicity"]["m"] == 6
    assert Path(body["artifact_path"]).resolve().is_relative_to((tmp_path / "research").resolve())
    out = SE.executor_result(body)
    assert MX.validate_result(out) == [] and out["verdict"] == "QUALIFIED"


def test_14_the_executor_contract_is_what_the_agent_validates(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    spy, dtb3, cal = _estate(-0.004)
    monkeypatch.setattr(SE, "load_spy_total_return", lambda: spy)
    monkeypatch.setattr(SE, "load_dtb3", lambda path=None: dtb3)
    monkeypatch.setattr(SE, "event_calendars", lambda **k: cal)
    monkeypatch.setattr(SE, "integrity", lambda **k: {"problems": [], "observed_sha256": {}})
    out = SE.run_mechanism(mechanism=_entry())
    assert MX.validate_result(out) == [] and out["capital_eligible"] is False
    assert out["verdict"] in ("NO_EDGE", "DATA_HOLD", "QUALIFIED")

    def boom():
        raise OSError("norgate is not running")
    monkeypatch.setattr(SE, "load_spy_total_return", boom)
    held = SE.run_mechanism(mechanism=_entry())
    assert held["verdict"] == "DATA_HOLD" and MX.validate_result(held) == []
