"""Stage 14 targeted tests — orthogonal alpha discovery campaign.

Covers (per the Stage 14 contract): PIT boundaries of the new open-aware
reader and calculators, survivorship/source filtering, the frozen hypothesis
constants, membership-event extraction (boundary/capture/gap rules), event-
study entry lag + abnormal math + adequacy DATA_HOLD, queue idempotency,
value_fn default-path preservation, FDR wiring, and no-operational-mutation
guards. Pure synthetic fixtures — no network, no owned-data dependency."""
from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pytest

from paper_trader.alpha_agent import autonomous_research as ar
from paper_trader.alpha_agent import historical_price_panel as hpp
from paper_trader.alpha_agent import price_factors as pf
from paper_trader.alpha_agent import stage14_orthogonal as s14


# --------------------------------------------------------------------------- #
# Fixture helpers.
# --------------------------------------------------------------------------- #
def _sessions(start: str, n: int) -> list:
    """n weekday dates from start (deterministic synthetic session calendar)."""
    d = date.fromisoformat(start)
    out = []
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _write_market_bar(root: Path, day: str, records: list) -> None:
    y, m, dd = day[:4], day[5:7], day[8:10]
    p = root / "normalized" / "MARKET_BAR" / y / m / dd
    p.mkdir(parents=True, exist_ok=True)
    f = p / "stage2_test.jsonl"
    with open(f, "a", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


def _bar(aid, day, opn, close, source="norgate_local"):
    return {"security_id": aid, "source_id": source, "effective_at": day,
            "normalized_payload": {"Open": opn, "Close": close}}


# --------------------------------------------------------------------------- #
# Open-aware reader: source filter, Open requirement, ordering, dedup.
# --------------------------------------------------------------------------- #
def test_open_close_reader_filters_and_sorts(tmp_path):
    root = tmp_path / "ingestion"
    _write_market_bar(root, "2020-01-03", [
        _bar("101", "2020-01-03", 10.0, 11.0),
        _bar(None, "2020-01-03", 9.0, 9.5, source="eodhd"),      # no assetid
        {"security_id": "102", "source_id": "norgate_local",
         "effective_at": "2020-01-03",
         "normalized_payload": {"Close": 5.0}},                   # no Open
        _bar("103", "2020-01-03", -1.0, 4.0),                     # bad Open
    ])
    _write_market_bar(root, "2020-01-02", [
        _bar("101", "2020-01-02", 9.5, 10.0),
        _bar("101", "2020-01-02", 999.0, 999.0),                  # dup date
    ])
    panel = hpp.build_assetid_open_close_panel(root)
    assert set(panel) == {"101"}
    assert panel["101"][0][0] == "2020-01-02" and panel["101"][1][0] == "2020-01-03"
    assert panel["101"][0] == ("2020-01-02", 9.5, 10.0)  # first record wins
    # close-only canonical reader is untouched by the additive reader
    closes = hpp.build_assetid_price_panel(root)
    assert "102" in closes  # close-only reader still accepts Open-less bars


# --------------------------------------------------------------------------- #
# Frozen hypothesis constants (preregistration integrity).
# --------------------------------------------------------------------------- #
def test_frozen_hypothesis_registry_shape():
    assert len(s14.HYPOTHESES) == 6
    ids = [h["id"] for h in s14.HYPOTHESES]
    assert ids == ["S14-A1", "S14-A2", "S14-A3", "S14-B1", "S14-B2", "S14-C1"]
    fam_sizes = {}
    for h in s14.HYPOTHESES:
        fam_sizes[h["family"]] = fam_sizes.get(h["family"], 0) + 1
    assert fam_sizes == {s14.FAMILY_OVERNIGHT: 3, s14.FAMILY_MEMBERSHIP: 2,
                         s14.FAMILY_SEASONALITY: 1}
    # Lane A binds the EXISTING frozen Stage 9.4 candidates verbatim.
    a = {h["feature"]: h for h in s14.HYPOTHESES
         if h["family"] == s14.FAMILY_OVERNIGHT}
    assert a["overnight_persistence"]["candidate_id"] == "c9_pricemomentu_e28ffecf4f"
    assert a["overnight_intraday_divergence"]["candidate_id"] == "c9_pricemomentu_be1eb51ec8"
    assert a["gap_concentration"]["candidate_id"] == "c9_pricemomentu_8a40c702bf"
    for h in a.values():
        assert h["expected_sign"] == 1 and h["horizon_days"] == 21


# --------------------------------------------------------------------------- #
# Overnight calculators: exact values + PIT (no look-ahead) + floors.
# --------------------------------------------------------------------------- #
def _synth_oc_series(days, on_ret=0.01, in_ret=-0.005, c0=100.0):
    """Series where every session has overnight return on_ret and intraday
    in_ret exactly."""
    out = []
    close = c0
    for d in days:
        opn = close * (1.0 + on_ret)
        close = opn * (1.0 + in_ret)
        out.append((d, opn, close))
    return out


def test_overnight_calculators_exact_and_pit():
    days = _sessions("2018-01-02", 400)
    oc = {"A1": _synth_oc_series(days, 0.01, -0.005)}
    arrays = s14.build_overnight_arrays(oc)
    fn = s14.make_overnight_value_fn(arrays)
    closes = [c for _d, _o, c in oc["A1"]]
    idx = 300
    v_pers = fn("overnight_persistence", name_key="A1", name_dates=days,
                name_closes=closes, idx=idx, mret_by_date={})
    v_div = fn("overnight_intraday_divergence", name_key="A1",
               name_dates=days, name_closes=closes, idx=idx, mret_by_date={})
    v_gap = fn("gap_concentration", name_key="A1", name_dates=days,
               name_closes=closes, idx=idx, mret_by_date={})
    assert v_pers == pytest.approx(0.01, abs=1e-12)
    assert v_div == pytest.approx(0.015, abs=1e-12)
    # constant synthetic returns leave only float-rounding variance; the share
    # must still be a valid [0,1] value or an honest None — never an error.
    assert v_gap is None or 0.0 <= v_gap <= 1.0
    # PIT: mutating FUTURE bars never changes the value at idx.
    oc2 = {"A1": list(oc["A1"])}
    oc2["A1"][idx + 1] = (days[idx + 1], 1.0, 2.0)
    arrays2 = s14.build_overnight_arrays(oc2)
    fn2 = s14.make_overnight_value_fn(arrays2)
    v_pers2 = fn2("overnight_persistence", name_key="A1", name_dates=days,
                  name_closes=closes, idx=idx, mret_by_date={})
    assert v_pers2 == pytest.approx(v_pers, abs=1e-15)
    # Window floor: far-too-short history -> None, never a guess.
    assert fn("overnight_persistence", name_key="A1", name_dates=days,
              name_closes=closes, idx=50, mret_by_date={}) is None


def test_gap_concentration_distinguishes_variance_location():
    days = _sessions("2018-01-02", 300)
    # alternating overnight moves, flat intraday -> gap share ~= 1
    out = []
    close = 100.0
    for j, d in enumerate(days):
        opn = close * (1.0 + (0.02 if j % 2 == 0 else -0.02))
        close = opn  # intraday flat
        out.append((d, opn, close))
    arrays = s14.build_overnight_arrays({"G": out})
    fn = s14.make_overnight_value_fn(arrays)
    closes = [c for _d, _o, c in out]
    v = fn("gap_concentration", name_key="G", name_dates=days,
           name_closes=closes, idx=290, mret_by_date={})
    assert v == pytest.approx(1.0, abs=1e-9)


# --------------------------------------------------------------------------- #
# value_fn hook preserves the released default path byte-for-byte.
# --------------------------------------------------------------------------- #
def test_value_fn_default_path_identical():
    days = _sessions("2015-01-02", 320)
    panel = {}
    for k in range(8):
        aid = "P%d" % k
        closes = [100.0 * (1.0 + 0.0005 * k) ** i * (1 + 0.001 * ((i + k) % 5))
                  for i in range(len(days))]
        panel[aid] = list(zip(days, closes))
    base = pf.build_factor_cross_sections(
        panel, feature="trend_slope_t", horizon_days=21, rebalance="monthly")
    delegated = pf.build_factor_cross_sections(
        panel, feature="trend_slope_t", horizon_days=21, rebalance="monthly",
        value_fn=lambda feature, *, name_key, name_dates, name_closes, idx,
        mret_by_date: pf.factor_value(feature, name_dates=name_dates,
                                      name_closes=name_closes, idx=idx,
                                      mret_by_date=mret_by_date))
    assert base["cross_sections"] == delegated["cross_sections"]
    assert base["portfolio_returns"] == delegated["portfolio_returns"]
    assert base["turnovers"] == delegated["turnovers"]
    # additive keys exist and align
    assert len(base["cross_section_names"]) == base["periods"]
    assert len(base["formation_dates"]) == base["periods"]


# --------------------------------------------------------------------------- #
# Seasonality: target month, momentum-guard exclusion, min-obs floor.
# --------------------------------------------------------------------------- #
def test_seasonality_target_month_and_exclusion():
    days = _sessions("2008-01-02", 3300)  # ~13 years of sessions
    # +8% drift concentrated in February, flat otherwise.
    closes = [100.0]
    for j in range(1, len(days)):
        month = int(days[j][5:7])
        prev_month = int(days[j - 1][5:7])
        step = 1.0
        if month == 2:
            step = 1.004  # ~+8% over ~20 Feb sessions
        closes.append(closes[-1] * step)
        _ = prev_month
    fn = s14.make_seasonality_value_fn(lookback_years=10,
                                       exclusion_sessions=252, min_obs=5)
    # formation in January -> target month February -> strongly positive
    jan_idx = max(i for i, d in enumerate(days) if d.startswith("2019-01"))
    v_jan = fn("same_month_seasonality", name_key="S", name_dates=days,
               name_closes=closes, idx=jan_idx, mret_by_date={})
    assert v_jan is not None and v_jan > 0.05
    # formation in May -> target June (flat months) -> ~0
    may_idx = max(i for i, d in enumerate(days) if d.startswith("2019-05"))
    v_may = fn("same_month_seasonality", name_key="S", name_dates=days,
               name_closes=closes, idx=may_idx, mret_by_date={})
    assert v_may is not None and abs(v_may) < 0.005
    # too-short history -> min-obs floor -> None
    v_early = fn("same_month_seasonality", name_key="S", name_dates=days,
                 name_closes=closes, idx=400, mret_by_date={})
    assert v_early is None


# --------------------------------------------------------------------------- #
# Membership spans: latest-capture rule, boundary exclusion, gap merge.
# --------------------------------------------------------------------------- #
def _write_membership(root: Path, day: str, records: list) -> None:
    y, m, dd = day[:4], day[5:7], day[8:10]
    p = root / "normalized" / "UNIVERSE_MEMBERSHIP" / y / m / dd
    p.mkdir(parents=True, exist_ok=True)
    with open(p / "stage6_test.jsonl", "a", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


def _span(aid, mf, mt, retrieved, index_name="S&P 500"):
    return {"security_id": aid, "source_id": "norgate_local",
            "effective_at": mf, "retrieved_at": retrieved,
            "normalized_payload": {"index_name": index_name,
                                   "member_from": mf, "member_to": mt}}


def test_membership_capture_boundary_and_gap_rules(tmp_path):
    root = tmp_path / "ing"
    old_cap, new_cap = "2020-06-01T00:00:00Z", "2021-06-01T00:00:00Z"
    # OLD capture: truncated member_to (would fabricate a deletion) — ignored.
    _write_membership(root, "2015-01-02", [
        _span("1", "2015-01-02", "2020-05-29", old_cap)])
    # NEW capture (authoritative): window 2015-01-02 .. 2021-05-28.
    _write_membership(root, "2015-01-03", [
        _span("1", "2015-01-02", "2021-05-28", new_cap),        # boundary both
        _span("2", "2016-03-01", "2021-05-28", new_cap),        # genuine add
        _span("3", "2015-01-02", "2018-07-20", new_cap),        # genuine drop
        _span("4", "2016-01-04", "2016-06-01", new_cap),        # drop then...
        _span("4", "2016-06-03", "2021-05-28", new_cap),        # ...re-add: gap<=5 -> merged
        _span("5", "2016-01-04", "2016-06-01", new_cap),
        _span("5", "2017-01-05", "2021-05-28", new_cap),        # big gap -> real events
        _span("6", "2016-05-02", "2021-05-28", new_cap, index_name="Russell 1000"),
    ])
    stores = s14.read_membership_spans([root])
    assert len(stores) == 1 and stores[0]["capture"] == new_cap
    sessions = _sessions("2015-01-02", 1700)
    session_index = {d: i for i, d in enumerate(sessions)}
    merged, boundaries = s14.merge_spans_across_stores(stores, session_index)
    assert len(merged["4"]) == 1          # micro-gap merged
    assert len(merged["5"]) == 2          # genuine exit + re-entry kept
    adds, drops = s14.extract_membership_events(merged, boundaries)
    add_ids = {(e["assetid"], e["date"]) for e in adds}
    drop_ids = {(e["assetid"], e["date"]) for e in drops}
    assert ("2", "2016-03-01") in add_ids
    assert ("1", "2015-01-02") not in add_ids          # window-start boundary
    assert ("3", "2018-07-20") in drop_ids
    assert ("1", "2021-05-28") not in drop_ids          # capture-end boundary
    assert ("4", "2016-06-01") not in drop_ids          # merged micro-gap
    assert ("5", "2016-06-01") in drop_ids and ("5", "2017-01-05") in add_ids
    assert not any(e["assetid"] == "6" for e in adds)   # other index excluded


# --------------------------------------------------------------------------- #
# Event study: strict entry lag, abnormal-vs-members math, adequacy hold.
# --------------------------------------------------------------------------- #
def test_event_study_entry_abnormal_and_adequacy():
    days = _sessions("2019-01-02", 130)
    # 25 flat members (benchmark 0%), one event name rising 1%/session.
    panel = {}
    spans = {}
    for k in range(25):
        aid = "M%d" % k
        panel[aid] = [(d, 100.0) for d in days]
        spans[aid] = [(days[0], days[-1])]
    ev_aid = "EVT"
    panel[ev_aid] = [(d, 100.0 * (1.01 ** i)) for i, d in enumerate(days)]
    spans[ev_aid] = [(days[10], days[-1])]
    events = [{"assetid": ev_aid, "date": days[10]}]
    study = s14.run_membership_event_study(
        events, "ADDITION", -1, spans, panel, horizons=(5,))
    h5 = study["horizons"][5]
    assert h5["n_events_measured"] == 1
    # entry is the FIRST session STRICTLY AFTER the event date, so the 5-session
    # abnormal window covers exactly sessions 11..16 of the 1%/session riser.
    assert h5["n_cohorts"] == 1
    # Frozen benchmark: EW of ALL members on the entry date — the event name is
    # a member itself, so it is self-included (26 members: 25 flat + itself).
    r = (1.01 ** 5) - 1.0
    expected_abnormal = r - (r / 26.0)
    assert h5["gross_mean_abnormal"] == pytest.approx(expected_abnormal,
                                                      rel=1e-9)
    # signed by expected_sign(-1): positive drift AGAINST the hypothesis
    assert h5["signed_gross_mean_abnormal"] == pytest.approx(
        -expected_abnormal, rel=1e-9)
    assert h5["distinct_issuers"] == 1
    # 1 cohort < 12 -> honest DATA_HOLD, never forced
    assert h5["adequacy_blocker"].startswith("DATA_HOLD_INSUFFICIENT_COHORTS")


def test_event_study_unmeasurable_without_forward_history():
    days = _sessions("2019-01-02", 30)
    panel = {"M%d" % k: [(d, 100.0) for d in days] for k in range(25)}
    spans = {aid: [(days[0], days[-1])] for aid in panel}
    panel["E"] = [(d, 100.0) for d in days]
    spans["E"] = [(days[0], days[-1])]
    events = [{"assetid": "E", "date": days[27]}]   # only 2 forward sessions
    study = s14.run_membership_event_study(
        events, "DELETION", 1, spans, panel, horizons=(5,))
    assert study["horizons"][5]["n_events_measured"] == 0
    assert study["horizons"][5]["n_unmeasurable"] == 1


# --------------------------------------------------------------------------- #
# ResearchQueue: idempotent enqueue, allowlisted claim, cadence isolation.
# --------------------------------------------------------------------------- #
def test_queue_idempotent_enqueue_and_allowlist_isolation(tmp_path):
    q = ar.ResearchQueue(tmp_path / "autonomy.sqlite")
    payload = {"stage": "stage14", "hypothesis_id": "S14-A1"}
    j1 = q.enqueue(ar.CAT_EXPERIMENT, lane=s14.LANE_PREFIX + "x",
                   payload=payload, origin=s14.ORIGIN)
    j2 = q.enqueue(ar.CAT_EXPERIMENT, lane=s14.LANE_PREFIX + "x",
                   payload=payload, origin=s14.ORIGIN)
    assert j1 == j2 and q.depth() == 1
    # The production cadence allowlist (stage8_autonomy.json PASS A) can NEVER
    # claim a stage14 job: neither origin nor lane prefix matches.
    cadence_origins = ["stage9-tournament", "stage9-weakest-gate",
                       "stage10-identity", "stage10.1-identity",
                       "stage10.2-fundamentals", "stage10.3-marketbar",
                       "stage11-multifactor", "stage12-autopsy"]
    cadence_lanes = ["tournament.", "acq.sec_form4_8k", "acq.sec_companyfacts",
                     "identity.", "fundamentals.", "market_bar.", "stage11.",
                     "stage12."]
    assert q.claim_next(origins=cadence_origins,
                        lane_prefixes=cadence_lanes) is None
    got = q.claim_next(origins=[s14.ORIGIN], lane_prefixes=[s14.LANE_PREFIX])
    assert got is not None and got.job_id == j1
    q.complete(got.job_id, result={"ok": True})
    assert q.claim_next(origins=[s14.ORIGIN],
                        lane_prefixes=[s14.LANE_PREFIX]) is None


# --------------------------------------------------------------------------- #
# FDR wiring over the frozen families.
# --------------------------------------------------------------------------- #
def test_stage14_fdr_families_and_sensitivity():
    stats = [
        {"id": "S14-A1", "family": s14.FAMILY_OVERNIGHT, "t": 4.0, "n": 150},
        {"id": "S14-A2", "family": s14.FAMILY_OVERNIGHT, "t": 0.3, "n": 150},
        {"id": "S14-A3", "family": s14.FAMILY_OVERNIGHT, "t": None, "n": 150},
        {"id": "S14-B1", "family": s14.FAMILY_MEMBERSHIP, "t": 1.0, "n": 100},
        {"id": "S14-B2", "family": s14.FAMILY_MEMBERSHIP, "t": -0.5, "n": 100},
        {"id": "S14-C1", "family": s14.FAMILY_SEASONALITY, "t": 0.1, "n": 150},
    ]
    out = s14.stage14_fdr(stats)
    ov = out["families"][s14.FAMILY_OVERNIGHT]
    assert ov["family_size_declared"] == 3 and ov["tested"] == 2
    assert "S14-A1" in ov["survivors"] and "S14-A2" not in ov["survivors"]
    allf = out["families"][s14.FAMILY_ALL]
    assert allf["family_size_declared"] == 6 and allf["tested"] == 5
    # q is monotone-safe: the strong t survives the full-family pass too
    assert "S14-A1" in allf["survivors"]
    assert out["alpha"] == 0.05


# --------------------------------------------------------------------------- #
# No-operational-mutation guards.
# --------------------------------------------------------------------------- #
def test_no_operational_paths_referenced():
    src = Path(s14.__file__).read_text(encoding="utf-8")
    for forbidden in ("paper_trading_desk", "current_alpha_paper_book",
                      "multi_horizon_alpha_ledger", "operational_book",
                      "daily_close", "create_order", "place_order"):
        assert forbidden not in src
    script = (Path(s14.__file__).resolve().parents[1] / "scripts"
              / "run_stage14_orthogonal_campaign.py").read_text(
        encoding="utf-8")
    assert "fingerprint_ledgers" in script
    assert "OPERATIONAL LEDGER FINGERPRINT CHANGED" in script
    for forbidden in ("create_order", "place_order", "confirm_daily_close",
                      "run_daily_close", "apply_target"):
        assert forbidden not in script
