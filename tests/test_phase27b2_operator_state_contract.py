"""
tests/test_phase27b2_operator_state_contract.py - Phase 27B.2 FINAL canonical
operator presentation contract.

Fully offline (Phase 27A/27B.1 harness: owned-style CSV fixtures, tmp desk /
ledger dirs, injectable marks downloader, deterministic clock seams). No test
here creates paper orders, fills, signals, trade decisions or any live broker
activity - every scenario runs against isolated tmp stores and read-only loads.

Proves the ONE canonical presentation contract on /v1/operational-book
(canonical_state) that every operator surface renders verbatim:

* every required presentation field exists (operational_book_name,
  workflow_state/_label, next_action_code/label/description/route/enabled,
  target_status/date/count, desk_mark_status/date, plan_status,
  planned_position_count, implemented_position_count, implementation_percentage,
  pending_order_count, fill_count, holdings_count, nav, cash, blockers,
  informational_notices, legacy_archive_summary, research_summary);
* for the current ORDER_PLAN_READY state the contract reads exactly:
  workflow_state ORDER_PLAN_READY, ONE next action
  REVIEW_AND_CONFIRM_ORDER_PLAN labelled "Review Order Plan", plan_status
  ORDER_PLAN_READY with a positive planned_position_count, target CONFIRMED,
  desk marks READY, 0 holdings / pending / fills, NAV = cash = $100,000;
* ONE navigation label and ONE confirm label - no banned label variants;
* false operator blockers (duplicate-of-confirmed-snapshot) are informational;
* MONITOR is never recommended with zero holdings and REFRESH_DESK is never
  recommended with ready marks.
"""
from __future__ import annotations

import pytest  # noqa: F401  (fixtures resolved by name)

from paper_trader.api import operational_book as ob_mod

from tests.test_phase27a_paper_operations import (  # reuse the offline harness
    _TICKS, _confirm_snapshot, _refresh,
    client, env,  # noqa: F401  (pytest fixtures resolved by name)
)
from tests.test_phase27b1_operational_surface_cutover import (  # 27B.1 seams
    env27b1, _init_book,  # noqa: F401
)

#: The exact presentation-contract fields required by the final 27B.2 brief.
_PRESENTATION_FIELDS = (
    "operational_book_name",
    "workflow_state", "workflow_state_label",
    "next_action_code", "next_action_label", "next_action_description",
    "next_action_route_or_anchor", "next_action_enabled",
    "target_status", "target_date", "target_count",
    "desk_mark_status", "desk_mark_date",
    "plan_status", "planned_position_count",
    "implemented_position_count", "implementation_percentage",
    "pending_order_count", "fill_count", "holdings_count",
    "nav", "cash",
    "blockers", "informational_notices",
    "legacy_archive_summary", "research_summary",
)

_BANNED_CTA_LABELS = (
    "Review & Confirm Paper Orders",
    "Review Paper Orders",
    "Review Paper Order Plan",
    "Confirm Order Plan",
    "Confirm Order Plan — Create Paper Orders",
)


def _ready_world():
    """Confirmed target + initialized book + valid desk marks -> plan exists."""
    _confirm_snapshot()
    _init_book("2026-07-18")
    _refresh("2026-07-18")


def _load(today="2026-07-18"):
    return ob_mod.load_operational_book(today=today)


class TestContractFields:
    def test_every_presentation_field_present(self, env27b1):
        _ready_world()
        d = _load()
        cs = d["canonical_state"]
        for field in _PRESENTATION_FIELDS:
            assert field in cs, field
        # one payload, two access paths - always identical
        assert d["operational_book"]["canonical_state"] == cs

    def test_contract_is_backend_data_not_javascript(self, env27b1):
        """The contract exists in the API payload itself (never faked in JS)."""
        _ready_world()
        cs = _load()["canonical_state"]
        assert cs["next_action_label"] == "Review Order Plan"
        assert cs["confirm_action_label"] == \
            "Confirm and Create Proposed Paper Orders"


class TestOrderPlanReadyPresentation:
    def test_workflow_state_and_label(self, env27b1):
        _ready_world()
        cs = _load()["canonical_state"]
        assert cs["workflow_state"] == "ORDER_PLAN_READY"
        assert cs["workflow_state_label"] == "ORDER PLAN READY"
        assert cs["header_status"] == {"code": "ORDER_PLAN_READY",
                                       "label": "ORDER PLAN READY"}

    def test_one_next_action_review_order_plan(self, env27b1):
        _ready_world()
        cs = _load()["canonical_state"]
        assert cs["next_action_code"] == "REVIEW_AND_CONFIRM_ORDER_PLAN"
        assert cs["next_action_label"] == "Review Order Plan"
        assert cs["next_action_enabled"] is True
        assert cs["next_action_route_or_anchor"] == "#portfolio-manager/ab-band"
        assert "review" in cs["next_action_description"].lower()
        assert "paper" in cs["next_action_description"].lower()

    def test_plan_status_and_planned_count(self, env27b1):
        _ready_world()
        cs = _load()["canonical_state"]
        assert cs["plan_status"] == "ORDER_PLAN_READY"
        assert isinstance(cs["planned_position_count"], int)
        assert 0 < cs["planned_position_count"] <= len(_TICKS)
        assert isinstance(cs["planned_blocked_count"], int)
        assert cs["planned_blocked_count"] >= 0
        assert (cs["planned_position_count"] + cs["planned_blocked_count"]
                <= len(_TICKS))

    def test_target_desk_and_book_numbers(self, env27b1):
        _ready_world()
        cs = _load()["canonical_state"]
        assert cs["target_status"] == "CONFIRMED"
        assert cs["target_date"] is not None
        assert cs["target_count"] == len(_TICKS)
        assert cs["desk_mark_status"] == "DESK_MARK_READY"
        assert cs["desk_mark_date"] == "2026-07-17"
        assert cs["nav"] == 100000.0
        assert cs["cash"] == 100000.0
        assert cs["holdings_count"] == 0
        assert cs["pending_order_count"] == 0
        assert cs["fill_count"] == 0
        assert cs["implemented_position_count"] == 0

    def test_blockers_empty_duplicate_is_informational(self, env27b1):
        _ready_world()
        cs = _load()["canonical_state"]
        assert cs["blockers"] == []
        assert any("Target already confirmed" in str(n)
                   for n in cs["informational_notices"])
        assert not any("DUPLICATE_OF_LATEST_CONFIRMED_SNAPSHOT" in str(b)
                       for b in cs["blockers"])

    def test_research_summary_terminology(self, env27b1):
        _ready_world()
        rs = _load()["canonical_state"]["research_summary"]
        assert rs["research_champion"] == "composite_sn"
        assert rs["operational_strategy"] == "fundamental_momentum_50_50_v1"
        assert rs["operational_target"] == "fundamental_momentum_50_50_top25"
        assert "RESEARCH ONLY" in rs["note"]

    def test_legacy_archive_summary_line(self, env27b1):
        _ready_world()
        legacy = _load()["canonical_state"]["legacy_archive_summary"]
        assert legacy["positions_count"] == 2
        assert legacy["line"] == "Legacy paper book archive: 2 historical positions"


class TestOneLabelPolicy:
    def test_label_map_uses_one_navigation_label(self):
        for code in ("REVIEW_AND_CONFIRM_ORDER_PLAN", "REVIEW_ORDER_PLAN",
                     "CONFIRM_ORDER_PLAN", "GENERATE_ORDER_PLAN",
                     "CONFIRM_PAPER_ORDERS"):
            assert ob_mod.NEXT_ACTION_LABELS[code] == "Review Order Plan", code

    def test_no_banned_labels_in_the_map(self):
        for label in ob_mod.NEXT_ACTION_LABELS.values():
            assert label not in _BANNED_CTA_LABELS, label

    def test_one_confirm_label(self):
        assert ob_mod.CONFIRM_ACTION_LABEL == \
            "Confirm and Create Proposed Paper Orders"


class TestFalseActionsNeverRecommended:
    def test_monitor_never_with_zero_holdings(self, env27b1):
        _ready_world()
        cs = _load()["canonical_state"]
        assert cs["holdings_count"] == 0
        assert cs["next_action_code"] != "MONITOR"

    def test_refresh_desk_never_with_ready_marks(self, env27b1):
        _ready_world()
        cs = _load()["canonical_state"]
        assert cs["desk_mark_status"] == "DESK_MARK_READY"
        assert cs["next_action_code"] != "REFRESH_DESK"

    def test_confirm_target_never_after_confirmation(self, env27b1):
        _ready_world()
        cs = _load()["canonical_state"]
        assert cs["target_status"] == "CONFIRMED"
        assert cs["next_action_code"] not in ("CONFIRM_TARGET_SNAPSHOT",
                                              "REFRESH_ALPHA_TARGET")

    def test_marks_missing_flips_to_refresh_desk_with_its_own_label(self, env27b1):
        _confirm_snapshot()
        _init_book("2026-07-18")
        cs = _load()["canonical_state"]
        assert cs["plan_status"] == "BLOCKED_DESK_MARKS_REQUIRED"
        assert cs["next_action_code"] == "REFRESH_DESK"
        assert cs["next_action_label"] == "Refresh Desk Marks"
        assert cs["next_action_route_or_anchor"] == "#portfolio-manager"
        assert cs["planned_position_count"] is None


class TestReadOnlySafety:
    def test_payload_is_read_only_and_paper_only(self, env27b1, client):
        _ready_world()
        from tests.test_phase27a_paper_operations import _AUTH
        d = client.get("/v1/operational-book", headers=_AUTH).json()
        assert d["read_only"] is True
        assert d["performed_write"] is False
        assert d["broker_enabled"] is False
        assert d["automation_enabled"] is False
        assert d["live_orders_enabled"] is False
        sm = d["canonical_state"]["safety_mode"]
        assert sm["manual_review"] and sm["paper_orders_only"]
        assert not sm["broker_execution"] and not sm["automation"]
