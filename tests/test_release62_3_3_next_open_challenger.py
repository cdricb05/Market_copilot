r"""Release 62.3.3 - the zero-new-subscription NEXT-OPEN challenger, and the
guards that keep it honest.

WHAT THESE TESTS DEFEND AGAINST
    This challenger is dangerous in one specific way: it looks exactly like a
    challenger that already has evidence. It shares its feature, its sign, its
    z-score, its horizon and its whole option construction with a sibling that
    is HISTORICALLY_CONFIRMED, and the diagnostic that chose its entry boundary
    returned +26.65 %/yr at t 3.00. Every one of those numbers was measured on
    the sample the sign was discovered on, and none of them tests THIS
    specification.

    So the tests split into two halves. Tests 01-04 prove the identity is
    separate and that NOTHING evidential crosses from the sibling. Tests 05-13
    prove the prospective contract: the three sessions never collapse into one,
    a decision exists before the entry open or the session is missed for ever,
    and vendor publication is measured rather than assumed. Tests 14-19 prove
    the safety boundary and the cost.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from paper_trader.alpha_agent import alpha_recovery as AR
from paper_trader.alpha_agent.alpha_recovery import databento_acquisition as DA
from paper_trader.alpha_agent.alpha_recovery import next_open_challenger as NOC
from paper_trader.alpha_agent.alpha_recovery import opra_publication_probe as PP
from paper_trader.alpha_agent.alpha_recovery import options_surface as OS
from paper_trader.alpha_agent.alpha_recovery import prospective_decision as PD
from paper_trader.alpha_agent.alpha_recovery import reversed_skew as RS

SRC = Path(NOC.__file__).read_text(encoding="utf-8")
CODE = "\n".join(ln for ln in SRC.splitlines() if not ln.strip().startswith("#"))
PROBE_SRC = Path(PP.__file__).read_text(encoding="utf-8")

#: One ordinary week, and one that crosses a holiday. Both are asserted because
#: "the next session" is a CALENDAR fact and a weekday-arithmetic bug survives
#: every test that only looks at a Tuesday.
ENTRY = "2026-09-15"
INFO = "2026-09-14"
#: 2026-11-26 is Thanksgiving: the session before 2026-11-27 is 2026-11-25.
HOLIDAY_ENTRY = "2026-11-27"
HOLIDAY_INFO = "2026-11-25"

WINDOW_OPEN_NOW = "2026-09-15T05:00:00+00:00"      # 01:00 ET on the entry session
BEFORE_WINDOW = "2026-09-14T22:00:00+00:00"        # 18:00 ET on the information session
AFTER_OPEN = "2026-09-15T14:00:00+00:00"           # 10:00 ET, the open has passed


@pytest.fixture()
def root(tmp_path, monkeypatch):
    """A private research root, so no test can touch the campaign's own store."""
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    return tmp_path


def _declare(**over):
    decl = NOC.policy_declaration({"identity_hash": "test-identity",
                                   "freeze_record_hash": "test-freeze",
                                   "model_spec_hash": "test-spec"})
    decl.update(over)
    return PD.declare_policy(**decl)


def _freeze(session=ENTRY, *, position=1.0, now=WINDOW_OPEN_NOW, observed_at=None):
    ctx = NOC.decision_context(session)
    return PD.freeze_decision(
        challenger_id=NOC.CHALLENGER_ID, eligible_session=session,
        weights={"SPY": position}, source_data_hash="src", feature_state_hash="feat",
        feature_observed_at=observed_at or ctx["information_cutoff_at"],
        now=now, context=ctx)


# --------------------------------------------------------------------------- #
# 01-04  A SEPARATE IDENTITY, AND NOTHING EVIDENTIAL CROSSES THE LINE
# --------------------------------------------------------------------------- #
def test_01_the_new_challenger_identity_is_distinct_from_the_same_day_one():
    """Two challengers, one signal. If the ids, the freeze hashes or the
    decision stores could collide, forward evidence would be pooled across two
    different entry boundaries."""
    assert NOC.CHALLENGER_ID != RS.CHALLENGER_ID
    assert NOC.CHALLENGER_ID == "REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1"
    assert NOC.SIBLING_CHALLENGER_ID == RS.CHALLENGER_ID
    assert NOC.specification_hash() != AR.stable_hash(RS.frozen_specification())
    assert NOC.freeze_row()["hypothesis_id"] != RS.freeze_row()["hypothesis_id"]
    assert NOC.freeze_record()["record_hash"] != RS.freeze_record()["record_hash"]
    # separate stores, so a decision can never land in the sibling's directory
    assert PD.decisions_dir(NOC.CHALLENGER_ID) != PD.decisions_dir(RS.CHALLENGER_ID)
    # ... and the SIGNAL is genuinely the same one, imported not retyped
    sib = RS.frozen_specification()
    mine = NOC.frozen_specification()
    for k in ("feature", "sign", "horizon_sessions", "zscore_lookback",
              "snapshot_et", "moneyness_construction", "strike_band",
              "quote_schema", "implied_volatility"):
        assert mine[k] == sib[k], "the signal must be the SAME: %s" % k


def test_02_the_same_day_challenger_is_unchanged(root):
    """The sibling is frozen. Nothing in this release may edit its
    specification, its sign, its horizon or its already-declared policy."""
    assert RS.CHALLENGER_ID == "REVERSED_SPY_PUT_CALL_SKEW_H5"
    assert RS.FROZEN_SIGN == -OS.SIGNALS[RS.SOURCE_SIGNAL]["sign"]
    assert RS.FROZEN_HORIZON == 5
    assert RS.frozen_specification()["emission_rule"].startswith(
        "the decision is formed from the 15:45 ET option snapshot on session t "
        "and held for 5 sessions")
    # the sibling's entry mark is its own session's close and stays that way
    assert "16:00" not in NOC.frozen_specification()["entry_mark_et"]
    assert NOC.frozen_specification()["entry_mark_et"] == "09:30"
    # the live campaign policy file, if present, is internally consistent -
    # i.e. nobody rewrote it in place
    live = Path(AR.DEFAULT_RESEARCH_ROOT) / PD.DECISIONS_SUBDIR / RS.CHALLENGER_ID / PD.POLICY_FILE
    if live.exists():
        body = json.loads(live.read_text(encoding="utf-8"))
        assert body["challenger_id"] == RS.CHALLENGER_ID
        assert body["entry_mark_et"] == [16, 0]
        assert body["declared_information_cutoff_et"] == [15, 45]
        assert body["record_hash"] == AR.stable_hash(
            {k: v for k, v in body.items()
             if k not in ("declared_at", "record_hash")}), (
            "the sibling's declared policy has been altered in place")
        assert "execution_contract" not in body, (
            "the sibling never declared one and must not have gained one")


def test_03_no_historical_evidence_is_inherited():
    """The sibling's discovery sample, its independent confirmation and its
    holdout are EVIDENCE, and none of them belongs to this entry boundary."""
    rec = NOC.freeze_record()
    q = rec["qualification_evidence"]
    assert q["state"] == "NONE"
    assert q["inherited_from_sibling"] is False
    assert q["discovery_sample_excluded"] is True
    assert q["independent_historical_confirmation"] == "NOT_RUN_AND_NOT_INHERITED"
    assert "DIAGNOSTIC ONLY" in q["disclosure"]
    spec = NOC.frozen_specification()
    for banned in ("discovery_sample", "discovery_disclosure",
                   "discovery_sample_is_qualification_evidence"):
        assert banned not in spec, "the new spec carries the sibling's %s" % banned
    # every sibling field is deliberately classified as inherited or refused
    assert NOC.unclassified_sibling_fields() == []


def test_04_no_historical_qualification_is_inherited():
    e = NOC.EVIDENCE_AT_INCEPTION
    assert e["historical_qualification"] == "NONE"
    assert e["historical_qualification_inherited_from_sibling"] is False
    assert e["discovery_evidence_inherited"] is False
    assert e["true_forward_observations"] == 0
    assert e["capital_eligible"] is False
    assert e["promotion_allowed"] is False
    body = NOC.build(write=False)
    assert body["historical_qualification_inherited"] is False
    assert body["true_forward_observations"] == 0
    assert body["capital_eligible"] is False
    # the diagnostic that chose the boundary is PROVENANCE, never evidence
    assert NOC.BOUNDARY_SELECTED_BY["no_entry_time_was_searched"] is True
    assert "DIAGNOSTIC ONLY" in NOC.EVIDENCE_DISCLOSURE


# --------------------------------------------------------------------------- #
# 05-11  THE PROSPECTIVE CONTRACT
# --------------------------------------------------------------------------- #
def test_05_no_observation_exists_before_prospective_registration(root):
    st = NOC.state()
    assert st["state"] == NOC.AWAITING_REGISTRATION
    assert st["registered"] is False
    assert st["true_forward_observations"] == 0
    assert st["capital_eligible"] is False
    assert st["decisions_frozen"] == 0
    tf = NOC.true_forward_state()
    assert tf["state"] == "NOT_REGISTERED"
    assert tf["true_forward_observations"] == 0
    assert tf["capital_eligible_now"] is False
    assert NOC.freeze_record()["live_registration_performed"] is False


def test_06_session_data_is_never_available_before_the_vendor_publishes(root, monkeypatch):
    """The probe reports what the VENDOR says. It may never infer availability
    from the clock - the mistake a Saturday dataset-range reading once made."""
    monkeypatch.setenv(DA.ENV_KEY, "test-key")

    def transport_not_yet(method, url, params, key, timeout):
        assert "timeseries" not in url, "the probe must never reach a data call"
        return json.dumps({"schema": {PP.SCHEMA: {"start": "2024-01-01T00:00:00Z",
                                                  "end": "2026-09-12T00:00:00Z"}}}
                          ).encode("utf-8")

    res = PP.poll(INFO, client=DA.Client(key="k", transport=transport_not_yet),
                  now="2026-09-14T21:00:00+00:00")
    assert res["outcome"] == PP.NOT_PUBLISHED
    assert res["available"] is False
    assert res["range_covers_session"] is False
    assert res["downloaded_bytes"] == 0 and res["paid_usd"] == 0.0

    calls = {"n": 0}

    def transport_covers_but_empty(method, url, params, key, timeout):
        calls["n"] += 1
        if "get_dataset_range" in url:
            return json.dumps({"schema": {PP.SCHEMA: {"end": "2026-09-16T00:00:00Z"}}}
                              ).encode("utf-8")
        assert "get_billable_size" in url
        return b"0"

    res2 = PP.poll(INFO, client=DA.Client(key="k", transport=transport_covers_but_empty),
                   now="2026-09-14T21:00:00+00:00")
    # A dataset-level range is a CLAIM; bytes for the session's own day are the
    # fact. A covering range with no bytes is not availability.
    assert res2["outcome"] == PP.RANGE_COVERS_BUT_NO_BYTES
    assert res2["available"] is False
    assert calls["n"] == 2
    # an unprobed session is reported as unknown, never as available
    assert PP.session_state("2026-09-21")["state"] == "NOT_PROBED"
    assert PP.session_state("2026-09-21")["available"] is None


def test_07_a_decision_must_exist_before_the_entry_session_opens(root):
    _declare()
    early = _freeze(now=BEFORE_WINDOW)
    assert early["outcome"] == PD.REFUSED_TOO_EARLY
    assert early["frozen"] is False
    ok = _freeze(now=WINDOW_OPEN_NOW)
    assert ok["outcome"] == PD.FROZEN and ok["frozen"] is True
    rec = ok["decision"]
    # the window shuts at the 09:30 ET open, which is the mark it will be scored at
    assert rec["emission_window_closes_at"] == NOC.decision_context(ENTRY)["entry_mark_at"]
    assert rec["decision_timestamp"] < rec["emission_window_closes_at"]


def test_08_a_decision_offered_after_the_open_is_rejected(root):
    _declare()
    late = _freeze(now=AFTER_OPEN)
    assert late["outcome"] == PD.REFUSED_TOO_LATE
    assert late["frozen"] is False
    assert late["backfill_refused"] is True
    assert not PD.decision_path(NOC.CHALLENGER_ID, ENTRY).exists()


def test_09_a_missed_decision_can_never_be_backfilled(root):
    _declare()
    assert _freeze(now=AFTER_OPEN)["frozen"] is False
    # a LATER attempt, with any timestamp, still cannot create the record
    for attempt in ("2026-09-15T20:00:00+00:00", "2026-09-30T12:00:00+00:00"):
        out = _freeze(now=attempt)
        assert out["outcome"] == PD.REFUSED_TOO_LATE
        assert out["frozen"] is False
    assert PD.list_decisions(NOC.CHALLENGER_ID) == []
    st = NOC.state(ENTRY, now="2026-09-16T12:00:00+00:00")
    # the session's own lifecycle is reported on the facts, whether or not the
    # registration paperwork is finished
    assert st["entry_state"] == NOC.MISSED
    assert st["backfill_refused"] is True
    assert PD.SAFETY["backfill_allowed"] is False
    assert NOC.SAFETY["backfill_allowed"] is False


def test_10_an_identical_repeated_freeze_is_idempotent(root):
    _declare()
    first = _freeze()
    assert first["outcome"] == PD.FROZEN
    path = PD.decision_path(NOC.CHALLENGER_ID, ENTRY)
    raw = path.read_bytes()
    again = _freeze(now="2026-09-15T06:30:00+00:00")       # same decision, later clock
    assert again["outcome"] == PD.ALREADY_FROZEN
    assert again["frozen"] is True and again["idempotent"] is True
    assert path.read_bytes() == raw, "an idempotent retry rewrote the record"
    assert len(PD.list_decisions(NOC.CHALLENGER_ID)) == 1


def test_11_a_conflicting_freeze_is_rejected_and_the_held_record_stands(root):
    _declare()
    held = _freeze(position=1.0)["decision"]
    raw = PD.decision_path(NOC.CHALLENGER_ID, ENTRY).read_bytes()
    clash = _freeze(position=-1.0)
    assert clash["outcome"] == PD.REFUSED_CONFLICT
    assert clash["frozen"] is False
    assert clash["held_weights"] == held["weights"]
    assert clash["offered_weights"] != held["weights"]
    assert PD.decision_path(NOC.CHALLENGER_ID, ENTRY).read_bytes() == raw


# --------------------------------------------------------------------------- #
# 12-13  THE THREE SESSIONS NEVER COLLAPSE INTO ONE
# --------------------------------------------------------------------------- #
def test_12_entry_is_the_actual_next_session_open(root):
    assert NOC.ENTRY_MARK_ET == (9, 30)
    assert NOC.EXECUTION_BOUNDARY == "NEXT_ELIGIBLE_SESSION_OPEN"
    ctx = NOC.decision_context(ENTRY)
    assert ctx["information_session"] == INFO
    assert ctx["entry_session"] == ENTRY
    assert ctx["information_session"] != ctx["entry_session"]
    assert ctx["signal_session_is_not_the_entry_session"] is True
    # 09:30 America/New_York, resolved through the ONE Eastern clock owner
    assert ctx["entry_mark_at"] == "2026-09-15T13:30:00+00:00"
    assert ctx["information_cutoff_at"] == "2026-09-14T19:45:00+00:00"
    assert ctx["information_cutoff_at"] < ctx["entry_mark_at"]
    # "next" is a CALENDAR fact, not a weekday one: Thanksgiving is skipped
    hol = NOC.decision_context(HOLIDAY_ENTRY)
    assert hol["information_session"] == HOLIDAY_INFO
    assert NOC.is_eligible_session("2026-11-26") is False
    assert NOC.entry_session_for(HOLIDAY_INFO) == HOLIDAY_ENTRY
    # ... and the frozen decision CARRIES all of it, so no reader must infer it
    _declare()
    rec = _freeze()["decision"]
    dc = rec["decision_context"]
    assert dc["information_session"] == INFO
    assert dc["entry_session"] == ENTRY == rec["eligible_session"]
    assert dc["entry_boundary"] == NOC.EXECUTION_BOUNDARY
    assert dc["maturity_session"] == ctx["maturity_session"]


def test_13_maturity_counts_five_eligible_sessions_from_the_ENTRY(root):
    assert NOC.HORIZON_COUNTS_FROM == "ENTRY_SESSION"
    ctx = NOC.decision_context(ENTRY)
    assert ctx["evaluation_horizon_sessions"] == RS.FROZEN_HORIZON == 5
    assert ctx["maturity_session"] == "2026-09-22"
    # counted from the ENTRY session, not from the information session - the two
    # answers differ by exactly one eligible session, which is the whole point
    from_info = NOC.next_eligible_session(INFO, RS.FROZEN_HORIZON)
    assert from_info == "2026-09-21"
    assert ctx["maturity_session"] != from_info
    # and it steps over holidays like every other session count here
    assert NOC.maturity_session_for(HOLIDAY_ENTRY) == "2026-12-04"


# --------------------------------------------------------------------------- #
# 14-19  COSTS, SAFETY AND THE PRICE OF THE DATA
# --------------------------------------------------------------------------- #
def test_14_costs_are_charged_on_both_the_long_and_the_short_leg(root):
    """A short measured as flat is the R62.3 kernel defect. Gross exposure - the
    quantity costs are charged on - must be 1.0 for BOTH signs."""
    _declare()
    long_rec = _freeze(session=ENTRY, position=1.0)["decision"]
    # a different entry session, judged inside ITS own window (01:00 ET)
    short_rec = _freeze(session=HOLIDAY_ENTRY, position=-1.0,
                        now="2026-11-27T06:00:00+00:00")["decision"]
    assert long_rec["gross_exposure"] == 1.0
    assert short_rec["gross_exposure"] == 1.0, "the short leg reads as flat"
    assert long_rec["net_exposure"] == 1.0 and short_rec["net_exposure"] == -1.0
    for rec in (long_rec, short_rec):
        cp = rec["cost_policy"]
        assert cp["applied"] == "both legs of every decision"
        assert cp["bps_per_side"] == OS.COST_PRIMARY_BPS > 0
        assert list(cp["ladder_bps_per_side"]) == list(OS.COST_LADDER_BPS)
        assert rec["gross_exposure"] * 2.0 * cp["bps_per_side"] * 1e-4 > 0.0


def test_15_nothing_here_promotes_a_model():
    for tok in ("promote_model(", "activate_sleeve(", "approve_proposal(",
                "promotion_ready", "adopt_"):
        assert tok not in CODE, "the challenger module reaches %r" % tok
    assert NOC.freeze_record()["promotion_allowed"] is False
    assert NOC.SAFETY["promotes_model"] is False
    assert "promote the model" in NOC.REGISTRATION_SCOPE["not_approved"]
    assert ("inherit the sibling's historical confirmation"
            in NOC.REGISTRATION_SCOPE["not_approved"])


def test_16_nothing_here_allocates_capital():
    for tok in ("allocate_capital(", "engine.portfolio", "operational_book",
                "NAV", "cash_balance"):
        assert tok not in CODE, "the challenger module reaches %r" % tok
    assert NOC.SAFETY["allocates_capital"] is False
    assert NOC.build(write=False)["capital_eligible"] is False
    assert NOC.state()["capital_eligible"] is False
    assert "allocate capital" in NOC.REGISTRATION_SCOPE["not_approved"]


def test_17_there_is_no_order_or_fill_path():
    for src, name in ((CODE, "challenger"), (PROBE_SRC, "publication probe")):
        body = "\n".join(l for l in src.splitlines()
                         if not l.strip().startswith("#"))
        for tok in ("create_order", "submit_order", "apply_fill", "broker",
                    "place_order", "execution_venue"):
            assert tok not in body, "the %s module reaches %r" % (name, tok)
    assert NOC.SAFETY["creates_orders"] is False
    assert NOC.SAFETY["creates_fills"] is False
    assert PP.SAFETY["creates_orders"] is False


def test_18_a_restart_preserves_the_declared_policy_and_the_frozen_decision(root):
    """The seam is a FILE, so a process that dies between the freeze and the
    entry must find exactly what it left."""
    _declare()
    rec = _freeze()["decision"]
    pol_raw = PD.policy_path(NOC.CHALLENGER_ID).read_bytes()
    dec_raw = PD.decision_path(NOC.CHALLENGER_ID, ENTRY).read_bytes()

    # "restart": every in-memory handle dropped, everything re-read from disk
    reloaded_pol = PD.load_policy(NOC.CHALLENGER_ID)
    reloaded_dec = PD.load_decision(NOC.CHALLENGER_ID, ENTRY)
    assert reloaded_pol["record_hash"] == json.loads(pol_raw)["record_hash"]
    assert reloaded_dec == json.loads(dec_raw)
    assert reloaded_dec["record_hash"] == rec["record_hash"]
    assert reloaded_dec["decision_context"]["entry_session"] == ENTRY
    # a re-declaration after a restart is idempotent and never amends
    again = _declare()
    assert again["outcome"] == PD.ALREADY_FROZEN and again["idempotent"] is True
    assert PD.policy_path(NOC.CHALLENGER_ID).read_bytes() == pol_raw
    st = NOC.state(ENTRY, now=WINDOW_OPEN_NOW)
    assert st["decisions_frozen"] == 1 and st["frozen_sessions"] == [ENTRY]


def test_19_no_new_paid_subscription_is_required(root):
    assert NOC.NEW_SUBSCRIPTION_COST_USD == 0.0
    spec = NOC.frozen_specification()
    assert spec["requires_live_market_data"] is False
    assert spec["information_source_entitlement"] == "HISTORICAL_ONLY"
    assert spec["new_subscription_cost_usd"] == 0.0
    assert NOC.SAFETY["purchases_data"] is False
    assert NOC.SAFETY["starts_trial_or_subscription"] is False
    assert NOC.forward_arithmetic()["marginal_data_cost_usd_per_session"] == 0.0
    # no live gateway is reachable from either new module
    for src, name in ((CODE, "challenger"), (PROBE_SRC, "publication probe")):
        body = "\n".join(l for l in src.splitlines()
                         if not l.strip().startswith("#"))
        for tok in ("lsg.databento.com", "13000", "cram=", "live.databento"):
            assert tok not in body, "the %s module reaches a LIVE gateway: %r" % (name, tok)
    # the probe may reach METADATA only; a data call would cost money
    assert PP.ENDPOINTS_USED == ("metadata.get_dataset_range",
                                 "metadata.get_billable_size")
    # the billable call itself is ``Client.get_range_csv``; the probe must not
    # reach it (the docstring NAMES the endpoint to say it is unreachable, so
    # the proof is the absence of the CALL, not of the word)
    assert "get_range_csv" not in PROBE_SRC
    assert ".download(" not in PROBE_SRC and "acquire(" not in PROBE_SRC
    assert PP.SAFETY["paid_dollars_authorised"] == 0.0
    assert PP.SAFETY["downloads_market_data"] is False


# --------------------------------------------------------------------------- #
# The shared owner was extended, and the extension must be invisible to the
# challenger that never asked for it.
# --------------------------------------------------------------------------- #
def test_20_the_shared_decision_owner_is_byte_identical_without_the_new_fields(root):
    """``context`` and ``execution_contract`` are additive. A challenger that
    passes neither must get exactly the record it got before they existed -
    identity hash, record hash and key set."""
    assert "decision_context" not in PD._IDENTITY_FIELDS
    plain = PD.declare_policy(
        challenger_id="PLAIN_TEST", information_cutoff_et=(15, 45),
        entry_mark_et=(16, 0), rebalance_cadence_sessions=5,
        evaluation_horizon_sessions=5, cost_policy={"bps_per_side": 1.0},
        instrument_scope=["SPY"], identity={"identity_hash": "h"},
        emission_rule="unchanged")
    assert "execution_contract" not in plain["policy"]
    assert plain["policy"]["record_hash"] == AR.stable_hash(
        {k: v for k, v in plain["policy"].items()
         if k not in ("declared_at", "record_hash")})
    out = PD.freeze_decision(
        challenger_id="PLAIN_TEST", eligible_session="2026-09-15",
        weights={"SPY": 1.0}, source_data_hash="s", feature_state_hash="f",
        feature_observed_at="2026-09-15T19:45:00+00:00",
        now="2026-09-15T19:50:00+00:00")
    assert out["outcome"] == PD.FROZEN
    assert "decision_context" not in out["decision"]
    assert out["decision"]["record_hash"] == AR.stable_hash(
        {k: v for k, v in out["decision"].items()
         if k not in ("record_hash", "decision_timestamp", "attempted_at")})


def test_21_the_publication_probe_reports_an_upper_bound_not_a_latency(root):
    """A first poll that was already positive never saw the session absent, so
    the elapsed time measures when someone asked - not when the vendor
    published. It must refuse to call that a delay."""
    PP.record({"session": "2026-09-11", "queried_at": "2026-09-12T21:00:00+00:00",
               "available": True, "outcome": PP.PUBLISHED})
    rep = PP.report("2026-09-11")
    assert rep["FIRST_AVAILABLE_TIME"] == "2026-09-12T21:00:00+00:00"
    assert rep["PUBLICATION_DELAY"] == "UNBOUNDED - the first poll was already positive"
    assert rep["AVAILABLE_BEFORE_NEXT_OPEN"] == "YES"   # 2026-09-14 09:30 ET
    assert rep["measured_delay_is_an_upper_bound"] is True

    # A negative poll BEFORE the close is what bounds the answer.
    PP.record({"session": "2026-09-18", "queried_at": "2026-09-18T19:00:00+00:00",
               "available": False, "outcome": PP.NOT_PUBLISHED})
    PP.record({"session": "2026-09-18", "queried_at": "2026-09-18T23:00:00+00:00",
               "available": True, "outcome": PP.PUBLISHED})
    bounded = PP.session_state("2026-09-18")
    assert bounded["delay_is_bounded"] is True
    assert bounded["publication_delay_hours"] == 3.0     # 20:00 UTC close -> 23:00
    assert bounded["resolution_seconds"] == 4 * 3600
    assert PP.report("2026-09-18")["AVAILABLE_BEFORE_NEXT_OPEN"] == "YES"
