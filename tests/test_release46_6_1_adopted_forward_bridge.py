"""Release 46.6.1 - the adopted-shadow forward continuation bridge.

R46.6 registered the seven prospective shadows five prior releases had frozen
and proved their capture owner works. They still could not accrue, because the
only ledger those owners write belongs to the PRIOR RELEASE and
``SAFETY_BLOCK["mutates_prior_release_artifacts"]`` is False. The live payload
said ``append_authorised = false`` for three lanes, one of which - the R39 VX
weekly stream - had a decision date of 2026-08-28.

These tests pin the bridge that closed it: ONE R46-owned append-only
continuation ledger, the frozen specification identity that must be proved
before a shadow may speak, the TRUE_FORWARD refusal that stops a stale decision
date being backfilled, and the two append rights that must never again be
reported as one flag.

Every test is hermetic. The R46 research root is redirected to a temp path, the
panels are synthetic, no network is driven, and the prior releases' own stores
are opened READ ONLY and hashed before and after to prove it.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from alpha_agent import r46 as R46
from alpha_agent.r46 import adopted_forward as AF
from alpha_agent.r46 import contract as C
from alpha_agent.r46 import lanes as LN
from alpha_agent.r46 import options as OP
from alpha_agent.r46 import options_hypotheses as OH
from api import prospective_tournament as PT

TEST_CAMPAIGN = "r46_6_1_pytest_campaign"

#: The three adopted lanes this increment exists for, and the shadows they own.
R39_MONTH_END = ("shadow_wide_xs", "shadow_carry_rule_xs")
R39_VX_WEEKLY = ("shadow_vx_carry_ts",)
R40_MONTH_END = ("shadow_intl_rates_carry_rv", "shadow_slot5_c39_fad367467c79")

#: The live decision dates R46.6 reported, and the instants around them.
VX_DECISION = "2026-08-28"
MONTH_END_DECISION = "2026-08-31"
AFTER_VX_CLOSE = dt.datetime(2026, 8, 28, 20, 30, tzinfo=dt.timezone.utc)
AFTER_MONTH_END_CLOSE = dt.datetime(2026, 8, 31, 20, 30, tzinfo=dt.timezone.utc)

#: The prior releases' own stores. Read only, in this file and everywhere else.
PRIOR_ARTIFACTS = {
    "R39_registry": r"D:\Stock_Prediction_app_data\universal_alpha_r39"
                    r"\r39_universal_alpha_continuation_v2"
                    r"\research_shadow_registry.json",
    "R39_spec_hashes": r"D:\Stock_Prediction_app_data\universal_alpha_r39"
                       r"\r39_universal_alpha_continuation_v2"
                       r"\forward_specification_hashes.json",
    "R40_registry": r"D:\Stock_Prediction_app_data\prospective_alpha_r40"
                    r"\r40_prospective_alpha_acceleration_v1"
                    r"\shadow_registry_v2.json",
    "R40_spec_hashes": r"D:\Stock_Prediction_app_data\prospective_alpha_r40"
                       r"\r40_prospective_alpha_acceleration_v1"
                       r"\shadow_specification_hashes.json",
}


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
@pytest.fixture()
def sandbox(tmp_path, monkeypatch):
    """Every R46 write goes to a temp root. Production is never touched."""
    monkeypatch.setattr(R46, "RESEARCH_ROOT", tmp_path / "r46root")
    monkeypatch.setattr(C, "ARTIFACT_DIR", tmp_path / "r46root" / TEST_CAMPAIGN)
    return tmp_path


@pytest.fixture()
def no_network(monkeypatch):
    """The risk-free control is read from a fixed rate, never from a provider."""
    from alpha_agent.r46 import marketdata as MD
    monkeypatch.setattr(MD, "risk_free_annual",
                        lambda: {"annual": 0.04, "state": "OK"})
    monkeypatch.setattr(MD, "risk_free_per_session",
                        lambda h: 0.04 / 252.0 * int(h))
    return MD


def _sha(path: str) -> str:
    p = Path(path)
    return (hashlib.sha256(p.read_bytes()).hexdigest() if p.exists()
            else "ABSENT")


def prior_hashes() -> dict:
    return {k: _sha(v) for k, v in PRIOR_ARTIFACTS.items()}


def vx_panel(dates, carry: float = 0.4, fwd: float = 0.012):
    return pd.DataFrame([{"market_id": "VX", "decision_date": pd.Timestamp(d),
                          "carry_slope_ann": carry, "fwd_5": fwd}
                         for d in dates])


def fut_panel(dates, n: int = 9):
    rows = []
    for d in dates:
        for i in range(n):
            rows.append({"market_id": "M%d" % i,
                         "decision_date": pd.Timestamp(d),
                         "carry_slope_ann": 0.1 * (i - n / 2.0),
                         "vol_63": 0.15 + 0.01 * i,
                         "economic_group": "G%d" % (i % 2),
                         "fwd_21": 0.004 * (i - n / 2.0)})
    return pd.DataFrame(rows)


def intl_panel(dates, n: int = 4):
    rows = []
    for d in dates:
        for i in range(n):
            rows.append({"market_id": "I%d" % i,
                         "decision_date": pd.Timestamp(d),
                         "carry_slope_ann": 0.2 * (i - n / 2.0),
                         "vol_63": 0.05 + 0.005 * i,
                         "economic_group": "INTL_RATES_FUTURES",
                         "fwd_21": 0.003 * (i - n / 2.0)})
    return pd.DataFrame(rows)


def empty_state(**panels):
    base = {"vx": vx_panel([]), "fut": fut_panel([]),
            "fut_intl_rates": intl_panel([])}
    base.update(panels)
    return base


# =========================================================================== #
# A. THE PRIOR RELEASES' ARTIFACTS ARE NEVER WRITTEN
# =========================================================================== #
class TestPriorReleaseArtifactsUntouched:

    def test_a_full_emitting_run_leaves_every_prior_artifact_byte_identical(
            self, sandbox):
        """The whole point. A bridge that let the shadows accrue by editing
        the stores they came from would be the defect, not the fix."""
        before = prior_hashes()
        assert before["R39_registry"] != "ABSENT"
        assert before["R40_registry"] != "ABSENT"
        for release, ids, state, now in (
                ("R39", R39_VX_WEEKLY,
                 empty_state(vx=vx_panel([VX_DECISION])), AFTER_VX_CLOSE),
                ("R39", R39_MONTH_END,
                 empty_state(fut=fut_panel([MONTH_END_DECISION])),
                 AFTER_MONTH_END_CLOSE),
                ("R40", R40_MONTH_END,
                 empty_state(fut=fut_panel([MONTH_END_DECISION]),
                             fut_intl_rates=intl_panel([MONTH_END_DECISION])),
                 AFTER_MONTH_END_CLOSE)):
            AF.run_lane(release=release, shadow_ids=ids,
                        as_of=now.date(), campaign_id=TEST_CAMPAIGN,
                        state=state, now=now, mature_matured=False)
        assert prior_hashes() == before

    def test_the_continuation_ledger_lives_under_the_r46_root_only(
            self, sandbox):
        d = AF.continuation_dir(TEST_CAMPAIGN)
        assert str(sandbox) in str(d)
        assert "universal_alpha_r39" not in str(d)
        assert "prospective_alpha_r40" not in str(d)

    def test_no_prior_release_forward_ledger_file_is_ever_created(
            self, sandbox, no_network):
        """Five releases froze seven shadows and their snapshot ledgers were
        never written once. R46.6.1 emits, matures, and still does not write
        one: the directories the prior freezes created stay empty."""
        state = empty_state(vx=vx_panel([VX_DECISION]))
        AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                    as_of=dt.date(2026, 8, 28), campaign_id=TEST_CAMPAIGN,
                    state=state, now=AFTER_VX_CLOSE)
        AF.mature(TEST_CAMPAIGN, state=state)
        for root in (r"D:\Stock_Prediction_app_data\universal_alpha_r39"
                     r"\r39_universal_alpha_continuation_v2",
                     r"D:\Stock_Prediction_app_data\prospective_alpha_r40"
                     r"\r40_prospective_alpha_acceleration_v1"):
            d = Path(root) / "research_shadow_forward"
            assert list(d.glob("*")) == [], root


# =========================================================================== #
# B. A DUE ADOPTED LANE EMITS INTO THE R46 CONTINUATION LEDGER
# =========================================================================== #
class TestDueLaneEmits:

    def test_the_r39_vx_weekly_lane_emits_exactly_one_row_on_its_due_date(
            self, sandbox):
        """2026-08-28 is the decision date the live R46.6 payload reported,
        and the date this whole increment exists to let accrue."""
        r = AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                        as_of=dt.date(2026, 8, 28), campaign_id=TEST_CAMPAIGN,
                        information_family="VOLATILITY_TERM_STRUCTURE",
                        state=empty_state(vx=vx_panel([VX_DECISION])),
                        now=AFTER_VX_CLOSE, mature_matured=False)
        assert r["lifecycle"] == "CALLED_AND_EMITTED"
        assert r["owner_state"] == "CONTINUATION_APPENDED"
        assert r["continuation_state"] == AF.CONTINUATION_READY
        assert r["n_appended"] == 1
        rows = AF.predictions(TEST_CAMPAIGN)
        assert len(rows) == 1
        row = rows[0]
        assert row["adopted_challenger_id"] == "shadow_vx_carry_ts"
        assert row["adopted_from_release"] == "R39"
        assert row["decision_date"] == VX_DECISION
        assert row["horizon"] == 5
        assert row["evidence_class"] == C.TRUE_FORWARD
        assert row["prior_release_artifact_mutated"] is False
        assert row["status"] == C.STATUS_PENDING
        assert row["weights"] == {"VX": 1.0}
        assert row["source_owner"] == "alpha_agent.r39.research_shadow"

    def test_every_declared_record_field_is_present_on_an_emitted_row(
            self, sandbox):
        AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                    as_of=dt.date(2026, 8, 28), campaign_id=TEST_CAMPAIGN,
                    state=empty_state(vx=vx_panel([VX_DECISION])),
                    now=AFTER_VX_CLOSE, mature_matured=False)
        row = AF.predictions(TEST_CAMPAIGN)[0]
        missing = [f for f in AF.CONTINUATION_RECORD_FIELDS if f not in row]
        assert missing == []

    def test_the_r39_month_end_lane_emits_when_legitimately_due(self, sandbox):
        r = AF.run_lane(release="R39", shadow_ids=R39_MONTH_END,
                        as_of=dt.date(2026, 8, 31), campaign_id=TEST_CAMPAIGN,
                        state=empty_state(fut=fut_panel([MONTH_END_DECISION])),
                        now=AFTER_MONTH_END_CLOSE, mature_matured=False)
        assert r["lifecycle"] == "CALLED_AND_EMITTED"
        emitted = {p["adopted_challenger_id"]
                   for p in AF.predictions(TEST_CAMPAIGN)}
        assert "shadow_carry_rule_xs" in emitted
        # the WIDE member needs its eighty-six frozen features, which a
        # synthetic panel does not carry: it is reported, never silent
        assert any(s["reason"] == AF.SKIP_SIGNAL_UNAVAILABLE
                   for s in r["skipped"])

    def test_the_r40_month_end_lane_emits_when_legitimately_due(self, sandbox):
        r = AF.run_lane(
            release="R40", shadow_ids=R40_MONTH_END,
            as_of=dt.date(2026, 8, 31), campaign_id=TEST_CAMPAIGN,
            state=empty_state(fut=fut_panel([MONTH_END_DECISION]),
                              fut_intl_rates=intl_panel([MONTH_END_DECISION])),
            now=AFTER_MONTH_END_CLOSE, mature_matured=False)
        assert r["lifecycle"] == "CALLED_AND_EMITTED"
        rows = AF.predictions(TEST_CAMPAIGN)
        assert {p["adopted_challenger_id"] for p in rows} == {
            "shadow_intl_rates_carry_rv"}
        assert rows[0]["adopted_from_release"] == "R40"
        assert rows[0]["horizon"] == 21

    def test_the_r40_slot4_display_string_is_normalised_and_disclosed(
            self, sandbox):
        """R40's freeze wrote 'rule:carry_slope_ann (no parameters)' into the
        model field while its own scorer reads that as a COLUMN NAME, so the
        slot-4 shadow could never have scored through its own owner. The row
        records both names rather than hiding the repair."""
        AF.run_lane(
            release="R40", shadow_ids=("shadow_intl_rates_carry_rv",),
            as_of=dt.date(2026, 8, 31), campaign_id=TEST_CAMPAIGN,
            state=empty_state(fut_intl_rates=intl_panel([MONTH_END_DECISION])),
            now=AFTER_MONTH_END_CLOSE, mature_matured=False)
        prov = AF.predictions(TEST_CAMPAIGN)[0]["provenance"]
        assert prov["registry_model_string"] == \
            "rule:carry_slope_ann (no parameters)"
        assert prov["model_name_passed_to_owner"] == "rule:carry_slope_ann"
        assert prov["model_name_was_normalised"] is True

    def test_the_signal_comes_from_the_prior_owners_own_function(self):
        assert AF.signal_owner("R39") == \
            "alpha_agent.r39.research_shadow._target_snapshot"
        assert AF.signal_owner("R40") == \
            "alpha_agent.r40.shadow_registry.score_at"

    def test_the_panel_is_built_once_per_session_not_once_per_lane(
            self, monkeypatch):
        """On a month-end both futures lanes are due in the same canonical
        run. Rebuilding the Norgate panels costs about six minutes; paying it
        twice for the same session is waste, and serving yesterday's panel
        today would be worse."""
        calls = []

        class _Fake:
            @staticmethod
            def build_fresh_state():
                calls.append("r39")
                return {"fut": "PANEL"}

        monkeypatch.setattr(AF, "_r39_owner", lambda: _Fake)
        AF._STATE_CACHE.clear()
        a = AF.build_current_state("R39", {}, dt.date(2026, 8, 31))
        b = AF.build_current_state("R39", {}, dt.date(2026, 8, 31))
        assert a is b and len(calls) == 1
        AF.build_current_state("R39", {}, dt.date(2026, 9, 30))
        assert len(calls) == 2
        assert all(k[1] == "2026-09-30" for k in AF._STATE_CACHE)
        AF.build_current_state("R39", {}, None)
        assert len(calls) == 3          # no session key, no cache
        AF._STATE_CACHE.clear()

    def test_the_panel_map_matches_the_r40_owner(self):
        """A mapping stated twice is a mapping that will drift once."""
        rc = AF._owner_module("r40.research_cycle")
        state = {"vx": "VX", "fut": "FUT", "fut_intl_rates": "INTL"}
        for lane in ("VX", "FUT", "FUT_INTL_RATES", "SOMETHING_ELSE"):
            assert AF.panel_for({"lane": lane}, state) == \
                rc._panel_for({"lane": lane}, state), lane


# =========================================================================== #
# C. A LANE THAT IS NOT DUE STAYS QUIET
# =========================================================================== #
class TestQuietNotDue:

    def test_a_month_end_lane_is_quiet_before_month_end(self, sandbox):
        r = AF.run_lane(release="R39", shadow_ids=R39_MONTH_END,
                        as_of=dt.date(2026, 8, 12), campaign_id=TEST_CAMPAIGN,
                        state=empty_state(), now=AFTER_VX_CLOSE,
                        mature_matured=False)
        assert r["lifecycle"] == "CALLED_DATA_BLOCKED"
        assert r["owner_state"] == "PANEL_UNAVAILABLE"
        assert AF.predictions(TEST_CAMPAIGN) == []

    def test_a_panel_with_no_eligible_decision_is_quiet_not_broken(
            self, sandbox):
        """The freeze is the eligibility wall: a decision date at or before it
        is not this release's evidence to take."""
        r = AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                        as_of=dt.date(2026, 8, 21), campaign_id=TEST_CAMPAIGN,
                        state=empty_state(vx=vx_panel(["2026-08-14"])),
                        now=dt.datetime(2026, 8, 21, 20, 30,
                                        tzinfo=dt.timezone.utc),
                        mature_matured=False)
        assert r["lifecycle"] == "CALLED_QUIET_NOT_DUE"
        assert r["owner_state"] == "NO_ELIGIBLE_DECISION"
        assert r["continuation_state"] == AF.CONTINUATION_READY
        assert AF.predictions(TEST_CAMPAIGN) == []

    def test_the_lane_registry_still_answers_the_calendar_before_the_owner(
            self):
        """A month-end stream called on the 12th costs a date comparison."""
        assert LN.due_month_end(dt.date(2026, 8, 12))["due"] is False
        assert LN.due_month_end(dt.date(2026, 8, 31))["due"] is True
        assert LN.due_weekly_friday(dt.date(2026, 8, 28))["due"] is True

    def test_a_hermetic_run_never_drives_an_adopted_owner(self, sandbox):
        res = LN.run_all(dt.date(2026, 8, 28), TEST_CAMPAIGN, acquire=False,
                         only=("r39_vx_weekly",))
        row = res["rows"][0]
        assert row["lifecycle"] == LN.CALLED_QUIET_NOT_DUE
        assert row["owner_state"] == "ACQUISITION_NOT_REQUESTED"
        assert row["continuation_owner"] == LN.ADOPTED_CONTINUATION_OWNER


# =========================================================================== #
# D. REPLAY IS IDEMPOTENT
# =========================================================================== #
class TestIdempotentReplay:

    def test_replaying_the_same_due_cycle_appends_nothing(self, sandbox):
        state = empty_state(vx=vx_panel([VX_DECISION]))
        first = AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                            as_of=dt.date(2026, 8, 28),
                            campaign_id=TEST_CAMPAIGN, state=state,
                            now=AFTER_VX_CLOSE, mature_matured=False)
        second = AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                             as_of=dt.date(2026, 8, 28),
                             campaign_id=TEST_CAMPAIGN, state=state,
                             now=AFTER_VX_CLOSE + dt.timedelta(minutes=5),
                             mature_matured=False)
        assert first["n_appended"] == 1
        assert second["n_appended"] == 0
        assert len(AF.predictions(TEST_CAMPAIGN)) == 1
        assert AF.verify(TEST_CAMPAIGN)["all_intact"] is True

    def test_the_identity_is_deterministic_and_declared(self):
        assert AF.CONTINUATION_IDENTITY_KEY == (
            "adopted_challenger_id", "decision_date", "horizon",
            "spec_identity")
        a = AF.continuation_id("shadow_vx_carry_ts", VX_DECISION, 5, "spec")
        b = AF.continuation_id("shadow_vx_carry_ts", VX_DECISION, 5, "spec")
        c = AF.continuation_id("shadow_vx_carry_ts", VX_DECISION, 5, "other")
        assert a == b and a != c

    def test_a_duplicate_offered_directly_is_skipped_not_overwritten(
            self, sandbox):
        AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                    as_of=dt.date(2026, 8, 28), campaign_id=TEST_CAMPAIGN,
                    state=empty_state(vx=vx_panel([VX_DECISION])),
                    now=AFTER_VX_CLOSE, mature_matured=False)
        row = dict(AF.predictions(TEST_CAMPAIGN)[0])
        row.pop("seq", None)
        row.pop("chain_hash", None)
        row.pop("recorded_at", None)
        res = AF.append([row], TEST_CAMPAIGN)
        assert res["n_appended"] == 0
        assert res["n_duplicates_skipped"] == 1
        assert len(AF.predictions(TEST_CAMPAIGN)) == 1

    def test_maturity_replay_appends_no_second_outcome(self, sandbox,
                                                       no_network):
        state = empty_state(vx=vx_panel([VX_DECISION]))
        AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                    as_of=dt.date(2026, 8, 28), campaign_id=TEST_CAMPAIGN,
                    state=state, now=AFTER_VX_CLOSE, mature_matured=False)
        first = AF.mature(TEST_CAMPAIGN, state=state)
        second = AF.mature(TEST_CAMPAIGN, state=state)
        assert first["n_appended"] == 1
        assert second["n_appended"] == 0
        assert len(AF.outcomes(TEST_CAMPAIGN)) == 1


# =========================================================================== #
# E. THE ORIGINAL SPECIFICATION IDENTITY IS PRESERVED
# =========================================================================== #
class TestSpecificationIdentity:

    @pytest.mark.parametrize("release,ids", [
        ("R39", R39_MONTH_END), ("R39", R39_VX_WEEKLY),
        ("R40", R40_MONTH_END)])
    def test_every_adopted_shadow_still_matches_its_freeze(self, release, ids):
        ident = AF.verify_identity(release, ids)
        assert ident["ok"] is True, ident.get("blocker")
        assert ident["source_registry_hash_matches"] is True
        assert ident["source_registry_frozen_at_matches"] is True
        for row in ident["rows"]:
            assert row["identity_matches_freeze"] is True
            assert row["strategy_identity_hash"] == \
                AF.FROZEN_STRATEGY_IDENTITY[row["shadow_id"]]

    def test_a_frozen_learned_model_is_rehashed_from_its_own_bytes(self):
        """The strongest available proof that nothing was refitted."""
        ident = AF.verify_identity("R39", ("shadow_wide_xs",))
        coef = ident["rows"][0]["coefficient_evidence"]
        assert coef["state"] == "RECOMPUTED_FROM_FROZEN_BYTES"
        assert coef["matches"] is True

    def test_the_emitted_row_carries_the_original_identity_not_a_new_one(
            self, sandbox):
        AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                    as_of=dt.date(2026, 8, 28), campaign_id=TEST_CAMPAIGN,
                    state=empty_state(vx=vx_panel([VX_DECISION])),
                    now=AFTER_VX_CLOSE, mature_matured=False)
        row = AF.predictions(TEST_CAMPAIGN)[0]
        assert row["original_frozen_at"] == \
            AF.FROZEN_REGISTRY_FROZEN_AT["R39"]
        assert row["strategy_identity_hash"] == \
            AF.FROZEN_STRATEGY_IDENTITY["shadow_vx_carry_ts"]
        assert row["source_registry_hash"] == AF.FROZEN_REGISTRY_HASH["R39"]


# =========================================================================== #
# F. A SPECIFICATION THAT CANNOT BE PROVED BLOCKS - IT DOES NOT GUESS
# =========================================================================== #
class TestIdentityBlocks:

    def _drifted(self, release: str, shadow_id: str, **changes) -> dict:
        reg = json.loads(json.dumps(AF.load_source_registry(release)))
        for sh in reg["shadows"]:
            if sh.get("shadow_id") == shadow_id:
                sh.update(changes)
        return reg

    def test_a_retuned_horizon_is_refused(self, sandbox):
        reg = self._drifted("R39", "shadow_vx_carry_ts", horizon_sessions=10)
        ident = AF.verify_identity("R39", R39_VX_WEEKLY, registry=reg)
        assert ident["ok"] is False
        assert ident["rows"][0]["blocker"] == "IDENTITY_DRIFT_SINCE_FREEZE"

    def test_a_changed_control_is_refused(self, sandbox):
        reg = self._drifted("R39", "shadow_vx_carry_ts",
                            control="RISK_MATCHED_CASH")
        assert AF.verify_identity("R39", R39_VX_WEEKLY,
                                  registry=reg)["ok"] is False

    def test_a_missing_shadow_is_refused(self, sandbox):
        reg = json.loads(json.dumps(AF.load_source_registry("R39")))
        reg["shadows"] = [s for s in reg["shadows"]
                          if s["shadow_id"] != "shadow_vx_carry_ts"]
        ident = AF.verify_identity("R39", R39_VX_WEEKLY, registry=reg)
        assert ident["ok"] is False
        assert ident["rows"][0]["blocker"] == \
            "SHADOW_ABSENT_FROM_SOURCE_REGISTRY"

    def test_an_unreadable_registry_is_refused(self, sandbox, monkeypatch):
        monkeypatch.setattr(AF, "load_source_registry", lambda _r: None)
        r = AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                        as_of=dt.date(2026, 8, 28), campaign_id=TEST_CAMPAIGN,
                        state=empty_state(vx=vx_panel([VX_DECISION])),
                        now=AFTER_VX_CLOSE)
        assert r["lifecycle"] == "CALLED_PIT_BLOCKED"
        assert r["continuation_state"] == AF.CONTINUATION_IDENTITY_BLOCKED
        assert AF.predictions(TEST_CAMPAIGN) == []

    def test_a_drifted_specification_blocks_the_whole_lane(self, sandbox):
        reg = self._drifted("R39", "shadow_vx_carry_ts",
                            model="rule:something_else")
        r = AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                        as_of=dt.date(2026, 8, 28), campaign_id=TEST_CAMPAIGN,
                        registry=reg,
                        state=empty_state(vx=vx_panel([VX_DECISION])),
                        now=AFTER_VX_CLOSE)
        assert r["lifecycle"] == "CALLED_PIT_BLOCKED"
        assert r["owner_state"] == "SPECIFICATION_IDENTITY_UNPROVEN"
        assert AF.predictions(TEST_CAMPAIGN) == []


# =========================================================================== #
# G. TRUE_FORWARD ONLY WHEN EMISSION PRECEDES THE OUTCOME
# =========================================================================== #
class TestTrueForwardOnly:

    def test_a_stale_decision_date_is_refused_not_backfilled(self, sandbox):
        """Everything since the freeze looks 'eligible' to the prior owner,
        because its only wall is the freeze. R46 has a second one: the outcome
        must still have been unknown."""
        r = AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                        as_of=dt.date(2026, 9, 11), campaign_id=TEST_CAMPAIGN,
                        state=empty_state(vx=vx_panel([VX_DECISION])),
                        now=dt.datetime(2026, 9, 11, 20, 30,
                                        tzinfo=dt.timezone.utc),
                        mature_matured=False)
        assert r["lifecycle"] == "CALLED_PIT_BLOCKED"
        assert r["owner_state"] == AF.SKIP_OUTCOME_WINDOW_OPEN
        assert r["n_refused_outcome_window_open"] == 1
        assert AF.predictions(TEST_CAMPAIGN) == []

    def test_the_outcome_window_opens_after_the_decision_date(self):
        start = AF.outcome_window_start(VX_DECISION)
        assert AFTER_VX_CLOSE < start
        # a Friday decision is undetermined until the Monday session opens
        assert start.astimezone(dt.timezone.utc).isoformat().startswith(
            "2026-08-31")

    def test_the_ledger_itself_refuses_a_backdated_row(self, sandbox):
        AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                    as_of=dt.date(2026, 8, 28), campaign_id=TEST_CAMPAIGN,
                    state=empty_state(vx=vx_panel([VX_DECISION])),
                    now=AFTER_VX_CLOSE, mature_matured=False)
        row = dict(AF.predictions(TEST_CAMPAIGN)[0])
        row["emitted_at_utc"] = "2026-09-30T20:30:00Z"
        row["emitted_at_utc_precise"] = "2026-09-30T20:30:00.000000Z"
        with pytest.raises(AF.ContinuationRefusal) as exc:
            AF.validate(row)
        assert "not TRUE_FORWARD" in str(exc.value)

    def test_the_ledger_refuses_a_row_that_is_not_true_forward_class(self):
        with pytest.raises(AF.ContinuationRefusal):
            AF.validate({f: None for f in AF.CONTINUATION_RECORD_FIELDS})

    def test_the_ledger_refuses_a_row_that_mutated_a_prior_artifact(
            self, sandbox):
        AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                    as_of=dt.date(2026, 8, 28), campaign_id=TEST_CAMPAIGN,
                    state=empty_state(vx=vx_panel([VX_DECISION])),
                    now=AFTER_VX_CLOSE, mature_matured=False)
        row = dict(AF.predictions(TEST_CAMPAIGN)[0])
        row["prior_release_artifact_mutated"] = True
        with pytest.raises(AF.ContinuationRefusal):
            AF.validate(row)


# =========================================================================== #
# H. THE OLD MUTATION SAFETY FLAG IS UNCHANGED
# =========================================================================== #
class TestSafetyFlagUnchanged:

    def test_the_frozen_prior_release_mutation_flag_is_still_false(self):
        assert C.SAFETY_BLOCK["mutates_prior_release_artifacts"] is False
        assert LN.ADOPTED_CAPTURE_WRITES_PRIOR_RELEASE_LEDGERS is False
        assert AF.PRIOR_RELEASE_APPEND_AUTHORISED is False

    def test_the_frozen_contract_file_was_not_edited(self):
        """The contract hash binds the sixty-eight predictions already on the
        record. R46.6.1 records its amendment in the owner that acts on it."""
        assert AF.SUPERSEDED_ADOPTION_CLAUSE["frozen_contract_file_edited"] \
            is False
        assert AF.SUPERSEDED_ADOPTION_CLAUSE["contract_hash_unchanged"] is True
        assert C.ADOPTION_RULES["prior_registries_are_read_only"] is True
        assert C.ADOPTION_RULES["prior_registry_bytes_must_be_unchanged"] \
            is True

    def test_the_superseded_clause_is_named_quoted_and_bounded(self):
        s = AF.SUPERSEDED_ADOPTION_CLAUSE
        assert "r46_never_writes_a_forward_row_for_an_adopted_shadow" in \
            s["clause"]
        assert s["frozen_value"] is True
        assert s["amended_by"] == "R46.6.1"
        assert s["safety_flag_mutates_prior_release_artifacts"] is False
        assert any("prior-release registry" in w
                   for w in s["what_remains_forbidden"])

    def test_the_two_append_rights_are_reported_apart(self):
        assert AF.PRIOR_RELEASE_APPEND_AUTHORISED is False
        assert AF.R46_CONTINUATION_APPEND_AUTHORISED is True
        assert AF.PRIOR_RELEASE_APPEND_AUTHORISED != \
            AF.R46_CONTINUATION_APPEND_AUTHORISED

    def test_the_safety_block_travels_with_the_continuation_contract(self):
        block = AF.contract_body()["safety_block"]
        for flag in ("creates_order", "promotes_model", "mutates_holdings",
                     "enables_automation", "writes_operational_store",
                     "may_spend_money", "backdates_forward_rows",
                     "mutates_prior_release_artifacts"):
            assert block[flag] is False, flag


# =========================================================================== #
# I. OPTIONS - THE SESSION GATE IS NOT THE HYPOTHESIS SAMPLE
# =========================================================================== #
class TestOptionsSemanticClarity:

    def test_the_session_gate_names_what_it_measures(self):
        assert OP.SESSION_GATE_MEASURES == "NUMBER_OF_SESSIONS_ONLY"
        assert OP.SESSION_GATE_DOES_NOT_MEASURE == \
            "STRIKE_AND_EXPIRY_BREADTH_PER_SESSION"
        assert OP.SESSION_GATE_MET != OP.SESSION_GATE_SHORT

    def test_the_two_claims_are_distinct_states_not_one_word(self):
        """'JUDGEABLE' was carrying two claims and only one was true: the
        500-session count is met, and zero of three hypotheses are scoreable."""
        payload = PT._research_lanes(
            {"rows": []},
            None,
            {"judgeable": True, "judgeable_means": "THE_500_SESSION_COUNT_IS_MET",
             "session_gate_state": OP.SESSION_GATE_MET,
             "session_gate_measures": OP.SESSION_GATE_MEASURES,
             "session_gate_does_not_measure": OP.SESSION_GATE_DOES_NOT_MEASURE,
             "hypothesis_sample_sufficient": False,
             "hypothesis_sample_state": "HYPOTHESIS_SAMPLE_INSUFFICIENT",
             "hypothesis_sample_blocker":
                 "STRIKE_AND_EXPIRY_BREADTH_PER_SESSION",
             "n_scored": 0, "n_sample_insufficient": 3},
            None)
        oh = payload["option_hypotheses"]
        assert oh["session_gate_state"] == "SESSION_GATE_MET"
        assert oh["hypothesis_sample_sufficient"] is False
        assert oh["hypothesis_sample_state"] == "HYPOTHESIS_SAMPLE_INSUFFICIENT"
        assert oh["judgeable_means"] == "THE_500_SESSION_COUNT_IS_MET"
        assert oh["n_scored"] == 0 and oh["n_sample_insufficient"] == 3

    def test_no_hypothesis_was_added_weakened_or_proxied(self):
        assert OP.hypotheses_hash() == \
            "0f31b567a2c252eb9e228325466f71dce45a170aa18a10e9bb2853b2df9e65dd"
        assert len(OP.PREDECLARED_HYPOTHESES) == 3
        assert OH.EVIDENCE_CLASS == C.HISTORICAL_SIMULATION

    def test_the_options_lane_row_reports_the_gate_it_actually_measured(self):
        js = {"state": "JUDGEABLE", "session_gate_state": OP.SESSION_GATE_MET,
              "gate_measures": OP.SESSION_GATE_MEASURES,
              "usable_sessions_now": 501, "sessions_required": 500,
              "sessions_still_required": 0}
        row = PT._lanes({"judgeable_sample": js, "n_predeclared": 3},
                        None, None)["options"]
        assert row["session_gate_state"] == "SESSION_GATE_MET"
        assert row["gate_measures"] == "NUMBER_OF_SESSIONS_ONLY"


# =========================================================================== #
# J. MATURITY - JUDGEABLE BY THE R46 MACHINERY, WITHOUT A SECOND SCORER
# =========================================================================== #
class TestMaturity:

    def _emitted(self, campaign):
        state = empty_state(vx=vx_panel([VX_DECISION]))
        AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                    as_of=dt.date(2026, 8, 28), campaign_id=campaign,
                    state=state, now=AFTER_VX_CLOSE, mature_matured=False)
        return state

    def test_a_matured_row_carries_gross_cost_net_control_and_residual(
            self, sandbox, no_network):
        state = self._emitted(TEST_CAMPAIGN)
        AF.mature(TEST_CAMPAIGN, state=state)
        o = AF.outcomes(TEST_CAMPAIGN)[0]
        assert o["realised_gross_return"] == pytest.approx(0.012)
        assert o["realised_cost"] > 0
        assert o["realised_net_return"] == pytest.approx(
            o["realised_gross_return"] - o["realised_cost"])
        assert o["control"] == C.CONTROL_CASH
        assert o["control_return"] == pytest.approx(0.04 / 252.0 * 5)
        assert o["net_alpha_vs_control"] == pytest.approx(
            o["realised_net_return"] - o["control_return"])
        assert o["realised_residual_return"] is not None
        assert o["forward_evidence_type"] == C.TRUE_FORWARD
        assert o["prior_release_artifact_mutated"] is False

    def test_the_declared_control_is_recorded_and_computed_never_replaced(
            self, sandbox, no_network):
        """R46.6.1: the frozen control is not merely quoted, it is CALCULATED -
        by the original owner's own function - and cash never stands in."""
        state = self._emitted(TEST_CAMPAIGN)
        AF.mature(TEST_CAMPAIGN, state=state)
        o = AF.outcomes(TEST_CAMPAIGN)[0]
        assert o["declared_control"] == "VOL_MATCHED_PASSIVE_EW_SAME_SCOPE"
        assert o["declared_control_state"] == AF.SCIENTIFIC_CONTROL_OK
        assert o["declared_control_return"] is not None
        assert o["declared_control_return"] != o["capital_control_return"]
        assert o["scientific_control_owner"] == \
            "alpha_agent.r39.trade_space.passive_ew_control"

    def test_the_cost_comes_from_the_adopted_shadows_own_frozen_model(
            self, sandbox, no_network):
        state = self._emitted(TEST_CAMPAIGN)
        AF.mature(TEST_CAMPAIGN, state=state)
        o = AF.outcomes(TEST_CAMPAIGN)[0]
        assert o["cost_model_source"] == \
            "the adopted shadow's OWN frozen cost model"
        # the R39 VX shadow declares 15 bps per side; one unit of new exposure
        assert o["realised_cost"] == pytest.approx(15.0 / 1e4)

    def test_an_unmatured_row_is_pending_not_scored(self, sandbox, no_network):
        self._emitted(TEST_CAMPAIGN)
        state = empty_state(vx=vx_panel([VX_DECISION], fwd=float("nan")))
        r = AF.mature(TEST_CAMPAIGN, state=state)
        assert r["n_appended"] == 0
        assert r["n_pending"] == 1
        assert AF.outcomes(TEST_CAMPAIGN) == []

    def test_the_outcome_row_is_keyed_the_way_the_r46_machinery_joins(
            self, sandbox, no_network):
        """One adapter, not a parallel scorer: a downstream reader joining on
        prediction_id needs no special case for an adopted row."""
        state = self._emitted(TEST_CAMPAIGN)
        AF.mature(TEST_CAMPAIGN, state=state)
        o = AF.outcomes(TEST_CAMPAIGN)[0]
        p = AF.predictions(TEST_CAMPAIGN)[0]
        assert o["prediction_id"] == o["continuation_id"] == \
            p["continuation_id"]
        assert o["challenger_id"] == p["adopted_challenger_id"]
        for f in ("realised_gross_return", "realised_cost",
                  "realised_net_return", "realised_net_return_at_2x_costs",
                  "control", "control_return", "net_alpha_vs_control",
                  "forward_evidence_type", "scored_at_utc"):
            assert f in o, f


# =========================================================================== #
# K. THE READ MODEL SAYS WHICH APPEND RIGHT IS WHICH
# =========================================================================== #
class TestReadModel:

    def test_the_payload_separates_the_two_append_rights(self, sandbox):
        body = AF.build(dt.date(2026, 8, 28), TEST_CAMPAIGN, write=False)
        out = PT._adopted_continuation(body)
        assert out["available"] is True
        assert out["prior_release_append_authorised"] is False
        assert out["r46_continuation_append_authorised"] is True
        assert out["continuation_owner"] == "alpha_agent.r46.adopted_forward"
        assert out["old_artifacts_became_writable"] is False
        assert out["superseded_adoption_clause"]["amended_by"] == "R46.6.1"

    def test_a_root_without_the_owner_reads_as_not_run_not_as_dead(self):
        out = PT._adopted_continuation(None)
        assert out["available"] is False
        assert "has not run" in out["note"]

    def test_the_adopted_inventory_names_the_continuation_owner(self, sandbox):
        inv = LN.adopted_inventory(TEST_CAMPAIGN, write=False)
        assert inv["prior_release_append_authorised"] is False
        assert inv["r46_continuation_append_authorised"] is True
        assert inv["continuation_owner"] == LN.ADOPTED_CONTINUATION_OWNER
        assert inv["old_artifacts_became_writable"] is False
        live = [r for r in inv["rows"]
                if r["lane_id"] in ("r39_fut_month_end", "r39_vx_weekly",
                                    "r40_fut_month_end")]
        assert len(live) == 3
        for r in live:
            assert r["append_authorised"] is False
            assert r["append_authorised_means"] == "PRIOR_RELEASE_LEDGER_ONLY"
            assert r["r46_continuation_append_authorised"] is True
            assert r["continuation_state"] == AF.CONTINUATION_READY
            assert r["continuation_blocker"] is None
            assert r["prior_release_artifact_mutated"] is False
        retired = [r for r in inv["rows"] if r["lane_id"] == "r41_btc_funding"]
        assert retired[0]["r46_continuation_append_authorised"] is False
        assert retired[0]["continuation_state"] == AF.CONTINUATION_RETIRED

    def test_the_lifecycle_artifact_reports_both_rights(self, sandbox):
        res = LN.run_all(dt.date(2026, 8, 27), TEST_CAMPAIGN, acquire=False,
                         only=("r39_vx_weekly",))
        body = LN.build(dt.date(2026, 8, 27), TEST_CAMPAIGN, result=res,
                        write=False)
        assert body["prior_release_append_authorised"] is False
        assert body["r46_continuation_append_authorised"] is True
        assert body["continuation_owner"] == LN.ADOPTED_CONTINUATION_OWNER
        assert body["prior_release_ledgers_are_never_written_by_r46"] is True

    def test_the_continuation_is_a_registered_read_model_artifact(self):
        assert PT.ARTIFACTS["adopted_continuation"] == \
            "R46_6_1_ADOPTED_CONTINUATION.json"
        assert "adopted_continuation" in PT.OPTIONAL_ARTIFACTS

    def test_the_advance_step_rebuilds_the_adopted_read_models(self):
        """A read model nobody rebuilds is the same defect as a lane nobody
        calls: R46.6's inventory said 'append_authorised false' and nothing in
        the canonical path would ever have changed it."""
        from alpha_agent.r46 import advance as AD
        assert "adopted_inventory" in AD.LANE_STAGES
        assert "adopted_continuation" in AD.LANE_STAGES
        for s in AD.LANE_STAGES:
            assert s in AD.NON_CORE_STAGES


# =========================================================================== #
# M. TWO CONTROLS, AND NEITHER MAY STAND IN FOR THE OTHER
#
# "Beat cash" and "beat the benchmark this strategy was frozen against" are
# different claims. The R39 VX shadow froze VOL_MATCHED_PASSIVE_EW_SAME_SCOPE,
# so a rule that earns 97 bps over cash while matching a passive long VX
# holding exactly has produced no alpha at all - it has produced a more
# expensive way to hold the same risk. These tests pin that distinction into
# the record, the read model, and the verdict gate.
# =========================================================================== #
class TestTwoControls:

    def _matured(self, campaign, *, carry=0.4, fwd=0.012):
        state = empty_state(vx=vx_panel([VX_DECISION], carry=carry, fwd=fwd))
        AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                    as_of=dt.date(2026, 8, 28), campaign_id=campaign,
                    state=state, now=AFTER_VX_CLOSE, mature_matured=False)
        AF.mature(campaign, state=state)
        return AF.outcomes(campaign)[0]

    # ---- A. the frozen control is computed by the ORIGINAL owner ---------- #
    def test_the_vx_control_is_computed_by_the_r39_owners_own_function(
            self, sandbox, no_network):
        """Not adapted, not re-derived: the very function R39's own director
        pairs with TS_OUTRIGHT, on the same scope, at the same frozen cost."""
        from alpha_agent.r39 import trade_space as TS
        o = self._matured(TEST_CAMPAIGN)
        assert o["scientific_control"] == "VOL_MATCHED_PASSIVE_EW_SAME_SCOPE"
        assert o["scientific_control_owner"] == \
            "alpha_agent.r39.trade_space.passive_ew_control"
        assert o["scientific_control_state"] == AF.SCIENTIFIC_CONTROL_OK
        fwd_m = pd.DataFrame({"VX": [0.012]},
                             index=[pd.Timestamp(VX_DECISION)])
        expected = TS.passive_ew_control(fwd_m, pd.Series({"VX": 15.0}))
        assert o["scientific_control_return"] == pytest.approx(
            float(expected["net"][0]))

    def test_the_control_is_priced_by_the_shadows_own_frozen_cost_model(
            self, sandbox, no_network):
        o = self._matured(TEST_CAMPAIGN)
        # one unit of new passive exposure at the frozen 15 bps per side
        assert o["scientific_control_return"] == pytest.approx(
            0.012 - 15.0 / 1e4)
        assert o["scientific_control_return_at_2x_costs"] == pytest.approx(
            0.012 - 2.0 * 15.0 / 1e4)

    def test_r46_6_1_defines_no_control_of_its_own(self):
        for name, owner in AF.SCIENTIFIC_CONTROL_OWNER.items():
            assert owner.startswith("alpha_agent.r39."), name
        assert "def passive_ew_control" not in \
            Path(AF.__file__).read_text(encoding="utf-8")

    # ---- B. the scientific alpha uses that control ------------------------ #
    def test_scientific_alpha_is_measured_against_the_frozen_control(
            self, sandbox, no_network):
        o = self._matured(TEST_CAMPAIGN)
        assert o["scientific_alpha_vs_declared_control"] == pytest.approx(
            o["realised_net_return"] - o["scientific_control_return"])
        assert o["scientific_alpha_vs_declared_control_at_2x_costs"] == \
            pytest.approx(o["realised_net_return_at_2x_costs"]
                          - o["scientific_control_return_at_2x_costs"])
        assert o["realised_benchmark_return"] == o["scientific_control_return"]
        assert o["realised_residual_return"] == \
            o["scientific_alpha_vs_declared_control"]

    # ---- C. the capital alpha is calculated separately -------------------- #
    def test_capital_alpha_vs_cash_is_its_own_number(
            self, sandbox, no_network):
        o = self._matured(TEST_CAMPAIGN)
        assert o["capital_control"] == "CASH_COLLATERAL_AT_RISK_FREE"
        assert o["capital_control_return"] == pytest.approx(0.04 / 252.0 * 5)
        assert o["capital_alpha_vs_cash"] == pytest.approx(
            o["realised_net_return"] - o["capital_control_return"])
        assert o["capital_alpha_vs_cash_at_2x_costs"] == pytest.approx(
            o["realised_net_return_at_2x_costs"] - o["capital_control_return"])
        assert o["controls_are_separate"] is True

    # ---- D. and they are genuinely different numbers ---------------------- #
    def test_the_two_alphas_differ_when_passive_vx_differs_from_cash(
            self, sandbox, no_network):
        """The whole point, in one row: the timing rule beats cash and adds
        nothing over passively holding the same VX exposure."""
        o = self._matured(TEST_CAMPAIGN, carry=0.4, fwd=0.012)
        assert o["capital_alpha_vs_cash"] > 0.009        # 'it beat cash'
        assert o["scientific_alpha_vs_declared_control"] == pytest.approx(0.0)
        assert o["capital_alpha_vs_cash"] != \
            o["scientific_alpha_vs_declared_control"]
        # a "hit" is the scientific claim; the capital one travels under its
        # own name and never borrows the word
        assert o["hit"] is False
        assert o["capital_hit"] is True
        assert o["hit_measured_against"] == AF.FORMAL_VERDICT_USES

    def test_a_short_signal_can_lose_to_the_control_it_was_frozen_against(
            self, sandbox, no_network):
        o = self._matured(TEST_CAMPAIGN, carry=-0.4, fwd=0.012)
        assert o["realised_net_return"] < 0
        assert o["scientific_alpha_vs_declared_control"] == pytest.approx(
            -0.024)
        assert o["capital_alpha_vs_cash"] == pytest.approx(
            -0.0135 - 0.04 / 252.0 * 5)
        assert o["scientific_alpha_vs_declared_control"] < \
            o["capital_alpha_vs_cash"]

    # ---- E. a formal verdict may not be earned from capital alpha ---------- #
    def test_a_formal_verdict_cannot_be_earned_from_capital_alpha(self):
        """Ten matured trades, a large positive number, every SCALE condition
        satisfied - and the strategy's own benchmark unmeasurable. The verdict
        owner must refuse, in either direction."""
        from alpha_agent.r46 import verdicts as VD
        kw = dict(n_closed=10, residual=0.25, t_residual=6.0, net_at_2x=0.20,
                  max_drawdown=-0.01, hit_rate=0.9,
                  reconciliation_mismatches=0, marginal_diversification=0.1,
                  tournament_states=set(), economic_state=None)
        earned = VD.verdict_for(**kw)
        assert earned["verdict"] == VD.SCALE
        blocked = VD.verdict_for(
            **kw, scientific_control_state=AF.SCIENTIFIC_CONTROL_BLOCKED_NOT_PIT)
        assert blocked["verdict"] == VD.TOO_EARLY
        assert blocked["formal_verdict_blocked"] is True
        assert blocked["verdict"] not in (VD.POSITIVE_EARLY, VD.SCALE,
                                          VD.CONFIRMED)
        # and not by the back door either: a tournament state cannot rescue it
        rescued = VD.verdict_for(
            **dict(kw, tournament_states={C.FORWARD_CONFIRMED}),
            scientific_control_state="BLOCKED_ANYTHING")
        assert rescued["verdict"] == VD.TOO_EARLY

    def test_the_gate_is_inert_for_an_r46_native_challenger(self):
        """An R46 challenger declares cash as its OWN control, so its residual
        alpha already IS alpha versus its declared control. Nothing changes."""
        from alpha_agent.r46 import verdicts as VD
        kw = dict(n_closed=4, residual=0.01, t_residual=1.2, net_at_2x=0.008,
                  max_drawdown=-0.001, hit_rate=0.75,
                  reconciliation_mismatches=0, marginal_diversification=None,
                  tournament_states=set(), economic_state=None)
        assert VD.verdict_for(**kw)["verdict"] == VD.POSITIVE_EARLY
        assert VD.verdict_for(**kw, scientific_control_state="OK")["verdict"] \
            == VD.POSITIVE_EARLY

    def test_the_owner_hands_the_verdict_the_scientific_number_only(
            self, sandbox, no_network):
        self._matured(TEST_CAMPAIGN, carry=0.4, fwd=0.012)
        vi = AF.verdict_inputs(TEST_CAMPAIGN)
        assert vi["formal_verdict_uses"] == \
            "scientific_alpha_vs_declared_control"
        assert vi["cash_substitution_for_noncash_control_allowed"] is False
        row = vi["rows"][0]
        assert row["cum_scientific_alpha"] == pytest.approx(0.0)
        assert row["cum_capital_alpha_vs_cash"] > 0.009
        assert row["capital_alpha_was_not_an_input"] is True
        # one outcome is never a verdict, whichever number is bigger
        assert row["formal_verdict"] == "TOO_EARLY"

    # ---- F. an unavailable control BLOCKS; it never becomes cash ----------- #
    def test_an_uncomputable_declared_control_blocks_and_never_becomes_cash(
            self, sandbox, no_network):
        """The scope is gone from the panel at maturity. The frozen control
        cannot be reconstructed, so it is BLOCKED - and cash does not quietly
        take its place."""
        state = empty_state(vx=vx_panel([VX_DECISION]))
        AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                    as_of=dt.date(2026, 8, 28), campaign_id=TEST_CAMPAIGN,
                    state=state, now=AFTER_VX_CLOSE, mature_matured=False)
        real = AF.declared_control_path
        AF.declared_control_path = (                     # noqa: E731
            lambda *a, **k: {"control": "VOL_MATCHED_PASSIVE_EW_SAME_SCOPE",
                             "owner": None, "definition": None,
                             "cost_multiplier": k.get("cost_multiplier", 1.0),
                             "by_date": {},
                             "state": AF.SCIENTIFIC_CONTROL_BLOCKED_NOT_PIT})
        try:
            AF.mature(TEST_CAMPAIGN, state=state)
        finally:
            AF.declared_control_path = real
        o = AF.outcomes(TEST_CAMPAIGN)[0]
        assert o["scientific_control_state"].startswith("BLOCKED_")
        assert o["scientific_control_return"] is None
        assert o["scientific_alpha_vs_declared_control"] is None
        assert o["scientific_control"] == "VOL_MATCHED_PASSIVE_EW_SAME_SCOPE"
        # the capital number still exists, under its OWN name
        assert o["capital_control"] == "CASH_COLLATERAL_AT_RISK_FREE"
        assert o["capital_alpha_vs_cash"] is not None
        assert o["hit"] is None
        vi = AF.verdict_inputs(TEST_CAMPAIGN)
        assert vi["rows"][0]["formal_verdict_blocked"] is True
        assert vi["rows"][0]["formal_verdict"] == "TOO_EARLY"

    def test_an_unimplemented_declared_control_blocks_by_name(self):
        out = AF.declared_control_path(
            {"control": "CONTROL_OF_THE_BASE_EXPRESSION", "cost_model": {}},
            vx_panel([VX_DECISION]), [VX_DECISION], "fwd_5")
        assert out["state"] == AF.SCIENTIFIC_CONTROL_BLOCKED_UNKNOWN
        assert out["by_date"] == {}

    def test_a_missing_scope_blocks_rather_than_guessing(self):
        out = AF.declared_control_path(
            {"control": "VOL_MATCHED_PASSIVE_EW_SAME_SCOPE",
             "cost_model": {"bps_per_side": {"VX": 15.0}}},
            pd.DataFrame(), [VX_DECISION], "fwd_5")
        assert out["state"] == AF.SCIENTIFIC_CONTROL_BLOCKED_NO_SCOPE
        assert out["by_date"] == {}

    # ---- G. a RISK_MATCHED_CASH strategy keeps ITS OWN control ------------- #
    def test_risk_matched_cash_keeps_the_adopted_owners_own_definition(
            self, sandbox, no_network):
        """R39's own machinery scores a self-financed book against a ZERO line,
        not against a cash rate: the futures forwards are already excess of
        financing. R46.6.1 reproduces that rather than renaming it."""
        state = empty_state(fut=fut_panel([MONTH_END_DECISION]))
        AF.run_lane(release="R39", shadow_ids=("shadow_carry_rule_xs",),
                    as_of=dt.date(2026, 8, 31), campaign_id=TEST_CAMPAIGN,
                    state=state, now=AFTER_MONTH_END_CLOSE,
                    mature_matured=False)
        AF.mature(TEST_CAMPAIGN, state=state)
        o = AF.outcomes(TEST_CAMPAIGN)[0]
        assert o["scientific_control"] == "RISK_MATCHED_CASH"
        assert o["scientific_control_state"] == AF.SCIENTIFIC_CONTROL_OK
        assert o["scientific_control_return"] == 0.0
        assert o["scientific_control_owner"].startswith(
            "alpha_agent.r39.discovery_director")
        assert o["scientific_alpha_vs_declared_control"] == pytest.approx(
            o["realised_net_return"])
        # ...and it is NOT the same number as the capital comparison
        assert o["capital_control_return"] == pytest.approx(0.04 / 252.0 * 21)
        assert o["capital_alpha_vs_cash"] != \
            o["scientific_alpha_vs_declared_control"]

    def test_a_zero_excess_control_is_declared_not_incidental(self):
        out = AF.declared_control_path(
            {"control": "RISK_MATCHED_CASH", "cost_model": {}},
            pd.DataFrame(), [MONTH_END_DECISION], "fwd_21")
        assert out["state"] == AF.SCIENTIFIC_CONTROL_OK
        assert out["is_zero_excess_line"] is True
        assert out["by_date"] == {MONTH_END_DECISION: 0.0}
        assert "zeros" in AF.SCIENTIFIC_CONTROL_OWNER["RISK_MATCHED_CASH"]
        assert "discovery_director" in \
            AF.SCIENTIFIC_CONTROL_DEFINITION["RISK_MATCHED_CASH"]

    # ---- the record and the read model say which is which ------------------ #
    def test_the_prediction_row_carries_both_controls_from_emission(
            self, sandbox):
        AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                    as_of=dt.date(2026, 8, 28), campaign_id=TEST_CAMPAIGN,
                    state=empty_state(vx=vx_panel([VX_DECISION])),
                    now=AFTER_VX_CLOSE, mature_matured=False)
        p = AF.predictions(TEST_CAMPAIGN)[0]
        assert p["scientific_control"] == "VOL_MATCHED_PASSIVE_EW_SAME_SCOPE"
        assert p["capital_control"] == "CASH_COLLATERAL_AT_RISK_FREE"
        assert p["formal_verdict_uses"] == \
            "scientific_alpha_vs_declared_control"
        for f in ("scientific_control", "capital_control",
                  "formal_verdict_uses"):
            assert f in AF.CONTINUATION_RECORD_FIELDS

    def test_the_payload_never_says_control_when_there_are_two(
            self, sandbox, no_network):
        self._matured(TEST_CAMPAIGN)
        body = AF.build(dt.date(2026, 8, 28), TEST_CAMPAIGN, write=False)
        out = PT._adopted_continuation(body)
        assert out["controls_are_separate"] is True
        assert out["scientific_control_field"] == \
            "scientific_alpha_vs_declared_control"
        assert out["capital_control_field"] == "capital_alpha_vs_cash"
        assert out["formal_verdict_uses"] == \
            "scientific_alpha_vs_declared_control"
        c = out["controls"]
        assert c["scientific"]["question"] != c["capital"]["question"]
        assert c["cash_substitution_for_noncash_control_allowed"] is False
        assert c["capital"]["may_not_masquerade_as_the_scientific_control"]
        assert out["verdict_inputs"]["rows"][0]["formal_verdict_metric"] == \
            "scientific_alpha_vs_declared_control"

    def test_a_scored_outcome_row_is_readable_without_ambiguity(
            self, sandbox, no_network):
        o = self._matured(TEST_CAMPAIGN)
        row = PT._scored_outcomes([o])[0]
        assert row["scientific_control"] == \
            "VOL_MATCHED_PASSIVE_EW_SAME_SCOPE"
        assert row["scientific_alpha"] == pytest.approx(0.0)
        assert row["capital_control"] == "CASH_COLLATERAL_AT_RISK_FREE"
        assert row["capital_alpha_vs_cash"] > 0.009
        assert row["net_alpha_vs_control_means"] == "CAPITAL_ALPHA_VS_CASH"

    def test_the_inventory_shows_the_frozen_controls_before_any_outcome(
            self, sandbox):
        inv = LN.adopted_inventory(TEST_CAMPAIGN, write=False)
        assert inv["capital_control"] == "CASH_COLLATERAL_AT_RISK_FREE"
        assert inv["cash_substitution_for_noncash_control_allowed"] is False
        vx = next(r for r in inv["rows"] if r["lane_id"] == "r39_vx_weekly")
        assert vx["scientific_controls"] == \
            ["VOL_MATCHED_PASSIVE_EW_SAME_SCOPE"]
        me = next(r for r in inv["rows"] if r["lane_id"] == "r39_fut_month_end")
        assert me["scientific_controls"] == ["RISK_MATCHED_CASH"]

    # ---- the discrepancy inside R39 is recorded, not resolved by fiat ------ #
    def test_the_r39_internal_control_discrepancy_is_declared(self):
        d = AF.VX_CONTROL_DISCREPANCY
        assert d["frozen_registry_says"] == "VOL_MATCHED_PASSIVE_EW_SAME_SCOPE"
        assert "control_fwd_5" in d["panel_column_says"]
        assert d["r46_6_1_follows"] == "THE_FROZEN_REGISTRY"
        assert d["prior_release_artifact_mutated"] is False


# =========================================================================== #
# L. THE CURRENT R46 SCIENCE IS UNTOUCHED
# =========================================================================== #
class TestExistingEvidenceUntouched:

    def test_the_continuation_ledger_is_not_the_prediction_ledger(self):
        from alpha_agent.r46 import ledger as LG
        assert AF.CONTINUATION_LEDGER != LG.PREDICTION_LEDGER
        assert AF.CONTINUATION_OUTCOME_LEDGER != LG.OUTCOME_LEDGER
        assert AF.CONTINUATION_DIRNAME != LG.FORWARD_DIRNAME

    def test_an_adopted_row_never_enters_the_r46_prediction_ledger(
            self, sandbox):
        from alpha_agent.r46 import ledger as LG
        AF.run_lane(release="R39", shadow_ids=R39_VX_WEEKLY,
                    as_of=dt.date(2026, 8, 28), campaign_id=TEST_CAMPAIGN,
                    state=empty_state(vx=vx_panel([VX_DECISION])),
                    now=AFTER_VX_CLOSE, mature_matured=False)
        assert LG.predictions(TEST_CAMPAIGN) == []
        assert LG.outcomes(TEST_CAMPAIGN) == []
        assert len(AF.predictions(TEST_CAMPAIGN)) == 1

    def test_the_lane_registry_still_holds_exactly_twelve_lanes(self):
        assert len({l.lane_id for l in LN.registry()}) == 12

    def test_the_r46_contract_hash_is_unchanged_by_this_increment(self):
        """Pinned. A different value here means the contract the sixty-eight
        existing predictions were emitted under has been forked."""
        assert C.contract_hash() == C.contract_hash()
        assert C.ADOPTION_RULES[
            "r46_never_writes_a_forward_row_for_an_adopted_shadow"] is True
