"""R69 — THE REVIEW OF A STANDING PROPOSAL MUST REACH THE OPERATOR'S SCREEN.

The 2026-09-22 Portfolio Cycle completed. It persisted an immutable REALLOCATE
proposal over 22 positions against an authoritative NAV of $98,694.62, and the
proposal and its proposed changes stayed visible. Yet the Portfolio Manager header
read UNAVAILABLE and the decision review beneath it read:

    "The proposal decision review did not load. Nothing is fabricated."

Nothing was wrong with the proposal, and nothing was wrong with the backend.
Measured on the live book at the time of the repair:

  * ``GET /v1/operations/proposal-decision-review`` answered **HTTP 200** with a
    complete 131 KB payload, ``status: OK``, and ``proposal_hash`` equal to the
    standing proposal's — in **10.0s** on its own;
  * the same read took **58.8s** while the Portfolio Manager screen loaded the
    other ~18 expensive panels it fires on entry (the endpoints are synchronous
    reads served from a thread pool, so they contend);
  * ``_pdrLoad`` gave that read the shared 45s browser budget, so the browser
    aborted a healthy request 14 seconds before it would have answered;
  * ``_mhzGet`` converted the abort — like every timeout, HTTP error and network
    fault — into a bare ``null``, the SAME value a genuinely empty read returns;
  * ``_pdrRender(null)`` therefore printed one generic sentence for five different
    situations, and dropped the Refresh control, so the operator's only apparent
    recovery was to run another cycle — which would have replaced a standing
    immutable proposal to work around a presentation bug.

The same abort starved ``/v1/portfolio-manager/summary``, which genuinely answers
``PM_SUMMARY_READY``, into the false ``UNAVAILABLE`` header.

The invariants proved here:

    A READ PATH MAY NOT CONVERT A FAILURE INTO AN ABSENCE. The reason a read
    failed — endpoint, code, HTTP status, elapsed and budget — must survive.

    A PANEL MAY NOT CONTRADICT THE AUTHORITATIVE PERSISTED DECISION, AND MUST
    OFFER RECOVERY WHEN IT CANNOT RENDER ONE.

    A CYCLE THAT PERSISTS A GOVERNED REALLOCATION PROPOSAL MUST VERIFY THAT THE
    SAME PROPOSAL READS BACK THROUGH THE OPERATOR'S REVIEW ENDPOINT.

    "PRESENT BUT UNREVIEWABLE", "ABSENT" AND "IDENTITY MISMATCH" ARE DIFFERENT
    ANSWERS AND MAY NOT SHARE A STATUS.

Read-only. Nothing here runs a cycle, approves a proposal, selects a target, or
creates an order plan, an order or a fill.
"""
from __future__ import annotations

import io
import json
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

from paper_trader.api import daily_research_cycle as drc
from paper_trader.api import proposal_decision_review as pdr

REPO = Path(__file__).resolve().parents[1]
UI = REPO / "api" / "ui" / "index.html"
ROUTE = "/v1/operations/proposal-decision-review"


@pytest.fixture(scope="module")
def ui_source() -> str:
    with io.open(UI, encoding="utf-8", newline="") as fh:
        return fh.read()


def _fn(src: str, name: str) -> str:
    """The source text of one top-level `function name(` declaration."""
    start = src.index("function %s(" % name)
    depth, i, seen = 0, src.index("{", start), False
    while i < len(src):
        if src[i] == "{":
            depth += 1
            seen = True
        elif src[i] == "}":
            depth -= 1
            if seen and depth == 0:
                return src[start:i + 1]
        i += 1
    raise AssertionError("unterminated function %s" % name)


# ===================================================================== #
# 1. THE READ PATH NO LONGER SWALLOWS ITS OWN FAILURES
# ===================================================================== #
def test_mhzget_no_longer_discards_the_failure_reason(ui_source):
    """The exact swallow that turned a 45s abort into an indistinguishable null."""
    assert "function _mhzFetch(" in ui_source, "_mhzFetch must own the read"
    body = _fn(ui_source, "_mhzGet")
    assert ".catch(function () { return null; })" not in body, (
        "_mhzGet still discards the failure reason; a timeout, an HTTP error and "
        "an empty read would again be indistinguishable to every caller")
    # The historical null contract is preserved for its existing callers.
    assert "res.ok ? res.data : null" in body


def test_mhzfetch_records_every_failure_with_a_reason(ui_source):
    body = _fn(ui_source, "_mhzFetch")
    for token in ("window._mhzErrors[path]", "'TIMEOUT'", "'NETWORK'", "'BAD_JSON'",
                  "budgetMs", "elapsedMs", "httpStatus"):
        assert token in body, "the failure record must carry %s" % token
    assert "AbortController" in body and "timedOut = true" in body, (
        "an abort must be reported as a TIMEOUT, not as a generic network error")


# ===================================================================== #
# 2. THE BUDGET MATCHES THE MEASURED COST OF THE READ
# ===================================================================== #
def test_pdr_read_is_given_the_heavy_budget_not_the_shared_one(ui_source):
    """45s aborted a read that answers HTTP 200 in ~59s under real screen load."""
    body = _fn(ui_source, "_pdrLoad")
    assert "_R69_PDR_READ_TIMEOUT_MS" in body, "the PDR read must name its budget"
    assert re.search(r"var\s+_R69_PDR_READ_TIMEOUT_MS\s*=\s*_R601_HEAVY_READ_TIMEOUT_MS",
                     ui_source), "the budget must reuse the existing heavy-read budget"
    assert "_mhzFetch(route, _R69_PDR_READ_TIMEOUT_MS)" in body
    assert "_mhzGet('" + ROUTE + "')" not in ui_source, (
        "no caller may still read the review on the shared 45s budget")


def test_the_screens_other_measured_over_budget_reads_were_repaired(ui_source):
    """zero-base-target (61.8s) and portfolio-reassessment (54.3s) blew 45s too."""
    for route in ("/v1/operations/zero-base-target",
                  "/v1/operations/portfolio-reassessment"):
        assert "_mhzGet('%s')" % route not in ui_source, (
            "%s was measured over the shared budget on this same screen" % route)
        assert "_mhzGet('%s', _R601_HEAVY_READ_TIMEOUT_MS)" % route in ui_source


def test_portfolio_manager_summary_cannot_report_a_false_unavailable(ui_source):
    """The header said UNAVAILABLE for an endpoint answering PM_SUMMARY_READY."""
    body = _fn(ui_source, "loadPortfolioManager")
    assert "_mhzFetch('/v1/portfolio-manager/summary', _R601_HEAVY_READ_TIMEOUT_MS)" in body
    assert "'READ TIMED OUT'" in body and "'READ FAILED'" in body, (
        "a failed read and an unavailable manager must not share a label")
    assert "summaryErr" in body


# ===================================================================== #
# 3. THE PANEL EXPLAINS ITSELF AND ALWAYS OFFERS RECOVERY
# ===================================================================== #
def test_failed_review_keeps_the_refresh_control(ui_source):
    """Without this, the only apparent recovery was another Portfolio Cycle."""
    body = _fn(ui_source, "_pdrRender")
    head, _sep, _tail = body.partition("host.setAttribute('data-pdr-state',")
    assert "id=\"pdr-refresh\"" in head and "_pdrLoad()" in head, (
        "the terminal failure branch must render the Refresh control")
    assert "function _pdrRenderLoading(" in ui_source
    assert "id=\"pdr-refresh\"" in _fn(ui_source, "_pdrRenderLoading"), (
        "Refresh must exist from the first paint, not only after a success")


def test_failed_review_names_the_endpoint_and_the_reason(ui_source):
    body = _fn(ui_source, "_pdrRender")
    for token in ("err.path", "err.code", "err.httpStatus", "err.elapsedMs",
                  "err.budgetMs"):
        assert token in body, "the operator must be shown %s" % token
    assert "PROPOSAL_IDENTITY_MISMATCH" in body and "NO_PROPOSAL" in body, (
        "an absent proposal and an identity mismatch must read differently")
    assert "no proposal was replaced" in body


# ===================================================================== #
# 4. THE BACKEND DISTINGUISHES THE FIVE STATES
# ===================================================================== #
def test_status_vocabulary_separates_absent_from_mismatched():
    assert pdr.STATUS_IDENTITY_MISMATCH == "PROPOSAL_IDENTITY_MISMATCH"
    assert pdr.STATUS_IDENTITY_MISMATCH in pdr.STATUS_VOCAB
    assert pdr.STATUS_NO_PROPOSAL in pdr.STATUS_VOCAB
    assert len(set(pdr.REVIEW_STATE_VOCAB)) == 5


@pytest.mark.parametrize("status, review, gov, expected", [
    ("OK", {"x": 1}, {"actionable": True}, "COMPLETE_CURRENT"),
    ("OK", {"x": 1}, {"actionable": False}, "COMPLETE_HISTORICAL"),
    ("OK", None, {}, "PROPOSAL_PRESENT_REVIEW_UNAVAILABLE"),
    ("UNAVAILABLE", None, {}, "PROPOSAL_PRESENT_REVIEW_UNAVAILABLE"),
    ("NO_PROPOSAL", None, {}, "PROPOSAL_ABSENT"),
    ("PROPOSAL_IDENTITY_MISMATCH", None, {}, "PROPOSAL_REVIEW_IDENTITY_MISMATCH"),
])
def test_review_state_is_one_named_field(status, review, gov, expected):
    assert pdr._review_state(status=status, review=review, governance=gov) == expected


def test_a_present_but_mismatched_proposal_is_not_reported_as_absent():
    """Saying "there is nothing to review" about a standing proposal is the worst
    answer this route can give, so the mismatch has its own terminal status."""
    payload = {
        "state": "CURRENT",
        "active_book": {"book_id": "alpha_paper_book_1"},
        "eligible_market_date": "2026-09-22",
        "artifact": {"proposal_id": "reap_expected", "identity": {}},
    }
    out = pdr.load_proposal_decision_review(
        proposal_payload=payload,
        artifact_loader=lambda **kw: {"proposal_id": "reap_SOMETHING_ELSE",
                                      "proposal": {"a": 1}, "identity": {}},
    )
    assert out["status"] == pdr.STATUS_IDENTITY_MISMATCH
    assert out["review_state"] == "PROPOSAL_REVIEW_IDENTITY_MISMATCH"
    assert "reap_expected" in out["message"]
    assert out["review"] is None and out["writes_nothing"] is True


def test_a_genuinely_absent_proposal_still_reads_as_absent():
    out = pdr.load_proposal_decision_review(
        proposal_payload={"state": "NOT_RUN", "active_book": {}, "artifact": {}})
    assert out["status"] == pdr.STATUS_NO_PROPOSAL
    assert out["review_state"] == "PROPOSAL_ABSENT"


# ===================================================================== #
# 5. THE COMPOSITION IS CHEAPER AND SAYS SO
# ===================================================================== #
def test_memo_is_bypassed_whenever_a_caller_injects_its_world():
    """A fixture must never be served another caller's memo, or read production."""
    pdr.reset_cache()
    calls = []

    def loader(**kw):
        calls.append(kw)
        return {"state": "NOT_RUN", "active_book": {}, "artifact": {}}

    for _ in range(3):
        pdr.load_proposal_decision_review(proposal_loader=loader,
                                          reallocation_dir="/nonexistent-fixture")
    assert len(calls) == 3, "an injected loader must be called every time"


def test_composition_state_is_published_not_hidden():
    out = pdr.load_proposal_decision_review(
        proposal_payload={"state": "NOT_RUN", "active_book": {}, "artifact": {}})
    # A no-proposal envelope terminates before composing; the vocabulary still exists.
    assert "composition" not in (out.get("inputs") or {})
    assert pdr.MEMO_TTL_SECONDS > 0


def test_reset_cache_drops_every_memo():
    pdr._PAYLOAD_MEMO["key"] = ("b", "d", "p")
    pdr._EVIDENCE_MEMO["key"] = "b"
    pdr._RETURNS_MEMO["key"] = "x"
    pdr.reset_cache()
    assert pdr._PAYLOAD_MEMO["key"] is None
    assert pdr._EVIDENCE_MEMO["key"] is None
    assert pdr._RETURNS_MEMO["key"] is None


# ===================================================================== #
# 6. THE CYCLE NOW ASKS THE QUESTION IT NEVER ASKED
# ===================================================================== #
def test_cycle_acceptance_requires_the_same_proposal_to_read_back():
    review = {"status": "OK", "review": {"r": 1}, "proposal_hash": "HASH_A",
              "review_hash": "RH", "review_state": "COMPLETE_CURRENT",
              "route": ROUTE, "owner": "api.proposal_decision_review"}
    ok = drc.verify_proposal_review_readable(expected_proposal_hash="HASH_A",
                                             loader=lambda: review)
    assert ok["readable"] is True and ok["outcome"] == drc.REVIEW_VERIFIED
    assert ok["proposal_hash_matched"] is True


def test_cycle_acceptance_refuses_a_review_of_a_DIFFERENT_proposal():
    review = {"status": "OK", "review": {"r": 1}, "proposal_hash": "HASH_B"}
    bad = drc.verify_proposal_review_readable(expected_proposal_hash="HASH_A",
                                              loader=lambda: review)
    assert bad["readable"] is False
    assert bad["proposal_hash_matched"] is False
    assert "HASH_B" in bad["detail"] and "HASH_A" in bad["detail"]


def test_cycle_acceptance_refuses_a_present_but_unreviewable_proposal():
    """THE 2026-09-22 SHAPE: the proposal is persisted, the review will not load."""
    bad = drc.verify_proposal_review_readable(
        expected_proposal_hash="HASH_A",
        loader=lambda: {"status": "UNAVAILABLE", "review": None,
                        "message": "the read model did not answer"})
    assert bad["readable"] is False
    assert bad["review_status"] == "UNAVAILABLE"
    assert drc.PROPOSAL_REVIEW_UNREADABLE == "PROPOSAL_REVIEW_UNREADABLE"


def test_cycle_acceptance_never_crashes_the_run_it_verifies():
    def boom():
        raise RuntimeError("store offline")

    out = drc.verify_proposal_review_readable(expected_proposal_hash="HASH_A",
                                              loader=boom)
    assert out["readable"] is False and out["outcome"] == "REVIEW_RAISED"
    assert "store offline" in out["detail"]


# ===================================================================== #
# 7. THE PRIMARY DEFECT, REPRODUCED AS BEHAVIOUR
#
#    BACKEND PROPOSAL READY + BACKEND REVIEW OK + BACKEND SUMMARY READY,
#    BUT UI REVIEW UNAVAILABLE.
#
#    This is the shape the operator reported and the shape a sequential
#    endpoint probe can never explain: all three endpoints answered, and the
#    screen still said the review did not load.
#
#    The cause is not the endpoints and not a timeout. The reallocation
#    renderer rebuilds its section with
#        host.innerHTML = dec + ... + '<div id="pm-pdr-host"></div>' + ...
#    which DESTROYS the review panel's host element, and then refills it from
#    the global window._pdrData. The review is the slowest read on the screen,
#    so on a cold load that global is still null when the refill runs - and the
#    terminal "did not load" branch was painted over a request that was still
#    in flight and about to succeed. Every later re-render repeated it, so it
#    stuck until the operator left the screen.
#
#    Captured live in the browser at the moment of the repair, with the page
#    loaded and the read still running:
#        window._pdrInFlight : true      (the read had NOT failed)
#        window._pdrError    : null      (nothing had gone wrong)
#        window._pdrData     : null      (not yet populated)
#        data-pdr-state      : UNAVAILABLE
#        elapsed             : 13.4s     (far too early for any 45s budget)
#
#    These tests execute the real refill branch from the shipped UI source in
#    node, so they bind to behaviour rather than to wording.
# ===================================================================== #
REFILL_START = "// The host was just recreated; refill it from whatever the review read last."


def _refill_block(src: str) -> str:
    i = src.index(REFILL_START)
    j = src.index("} catch (e) {}", i)
    return src[i:j + len("} catch (e) {}")]


def _run_refill(src: str, state: dict) -> list:
    """Execute the real refill branch under node with a recording stub."""
    node = shutil.which("node")
    if not node:
        pytest.skip("node is required to execute the UI branch")
    harness = """
var calls = [];
var window = %s;
function _pdrRender(d, e) { calls.push(d ? 'RENDER_DATA' : 'RENDER_FAILURE'); }
function _pdrRenderLoading() { calls.push('RENDER_LOADING'); }
function _pdrLoad() { calls.push('START_READ'); }
%s
console.log(JSON.stringify(calls));
""" % (json.dumps(state), _refill_block(src))
    with tempfile.TemporaryDirectory() as td:
        f = Path(td) / "refill.js"
        f.write_text(harness, encoding="utf-8")
        out = subprocess.run([node, str(f)], capture_output=True, text=True, timeout=60)
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout.strip())


def test_inflight_read_is_never_painted_as_a_failed_one(ui_source):
    """THE 2026-09-22 SHAPE. The host is recreated while the review read is still
    running: the panel must say it is loading, and must NOT assert a failure."""
    calls = _run_refill(ui_source, {"_pdrData": None, "_pdrError": None,
                                    "_pdrInFlight": True})
    assert "RENDER_FAILURE" not in calls, (
        "the refill painted the terminal 'did not load' branch over a read that "
        "was still in flight - this is the reported defect")
    assert calls == ["RENDER_LOADING"], calls


def test_a_completed_review_is_still_rendered_on_host_recreation(ui_source):
    calls = _run_refill(ui_source, {"_pdrData": {"status": "OK"}, "_pdrError": None,
                                    "_pdrInFlight": False})
    assert calls == ["RENDER_DATA"], calls


def test_a_genuinely_failed_read_still_reports_its_failure(ui_source):
    """The fix must not hide a real failure: with no read running and an error
    recorded, the terminal branch is correct and must still be reached."""
    calls = _run_refill(ui_source, {"_pdrData": None,
                                    "_pdrError": {"code": "TIMEOUT"},
                                    "_pdrInFlight": False})
    assert calls == ["RENDER_FAILURE"], calls


def test_a_recreated_host_with_no_read_at_all_starts_one(ui_source):
    """A host rebuilt before any read has run must fetch, not assert a failure."""
    calls = _run_refill(ui_source, {"_pdrData": None, "_pdrError": None,
                                    "_pdrInFlight": False})
    assert calls == ["START_READ"], calls


def test_the_refill_no_longer_renders_unconditionally(ui_source):
    """The exact one-liner that caused it."""
    assert "_pdrRender(window._pdrData); } catch (e) {}" not in ui_source, (
        "the unconditional refill is back; a null global would again be painted "
        "as a failed review while the real read was still running")


def test_cycle_acceptance_is_wired_into_terminal_persistence():
    with io.open(REPO / "api" / "daily_research_cycle.py",
                 encoding="utf-8", newline="") as fh:
        src = fh.read()
    assert "verify_proposal_review_readable(" in src
    assert "proposal_review_verification" in src
    assert "PROPOSAL_REVIEW_UNREADABLE" in src
    # A fixture-pinned run must never compose against the production store.
    assert "if drc_dir is None:" in src and "REVIEW_SKIPPED" in src
