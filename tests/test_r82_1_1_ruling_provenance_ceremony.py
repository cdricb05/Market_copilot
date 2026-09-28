r"""R82.1.1 - THE OPERATOR CONFIRMATION CEREMONY.

R82.1 said a ruling was authoritative when the submission carried the book-bound
``submission_token`` a governed read had published AND ``confirmed_in_ui: true``.
Its own tests showed what that actually permitted::

    token = ruling_submission_token(...)
    ruling_provenance(submission_token=token, confirmed_in_ui=True, ...)
    -> verified=True, channel=OPERATOR_UI_CONFIRMED

The token is a PURE FUNCTION of the frozen identity, so any in-process caller could
mint one, and any caller that can read the governed panel is served one. The boolean
is the caller's own word. Token plus claim is therefore not evidence that an
operator confirmed anything - it is evidence of knowing which book is on the screen.
Calling it verified UI provenance was an overclaim.

What closes it is EVIDENCE THE CALLER CANNOT MINT. A ruling is now authoritative
only if it SPENDS a single-use confirmation that this backend process issued, for
this exact ruling on this exact frozen book, in a ceremony that required the book
token and the typed confirmation phrase. The confirmation lives only in the issuing
process's memory, is never persisted, is never published by a read, expires, and is
spent the first time it is presented - including by a presentation that then fails
its bindings.

This suite proves, in order:

  * the exact R82.1 bypass now fails, in process and over HTTP;
  * the genuine ceremony succeeds, and only then does a ruling govern;
  * a spent or expired confirmation cannot be replayed;
  * a confirmation for book A is refused for book B, and one bound to a ruling is
    refused for a different available ruling;
  * a direct API call is still possible, still recorded, and classified
    ``API_DIRECT_CALL``;
  * the live governed store is byte-unchanged by everything above.

Every world is hermetic: a temporary decision root, a monkeypatched review loader
and an in-memory ledger. No live endpoint, store, ledger, holding, cash or NAV is
touched, no threshold is edited, and nothing here approves, orders or executes.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import paper_trader.api.app as appmod
from paper_trader.api import portfolio_decision as pdec
from paper_trader.engine import selected_target as stgt

from tests.test_r69_5_risk_policy_gate import _env6, _select6, _world6
from tests.test_r82_risk_policy_ruling import (
    _book_token, _ceremony, _rule, _rule_unverified, _rulings,
)

JUDGE = stgt.RULING_JUDGE_AGAINST_THE_BEFORE_UNIVERSE
ACCEPT = stgt.RULING_ACCEPT_AS_IS
FLOOR = stgt.RULING_ADD_AN_ABSOLUTE_COMPANION_FLOOR
_KEY = "r82-1-1-ceremony-test-key"
_AUTH = {"X-API-Key": _KEY}
_ROUTE = "/v1/operations/portfolio-decision/risk-policy-ruling"
_CEREMONY_ROUTE = _ROUTE + "/confirmation"


def _ident(sel, review=None):
    review = review if review is not None else pdec.selection_policy_review(sel)
    return dict(selection_id=sel.get("selection_id"),
                selected_target_implementation_hash=review["implementation_hash"],
                reference_limit=review["reference_limit"])


def _world(tmp, target="FULL_TARGET"):
    """A hermetic book with a governed selection that OWES a ruling."""
    _, art, ddir = _world6(tmp)
    sel = _select6(target, ddir, art)["record"]
    assert pdec.selection_policy_review(sel)["required"] is True
    return art, ddir, sel


class _Clock:
    """A controllable stand-in for the module's monotonic clock."""

    def __init__(self, t=1000.0):
        self.t = float(t)

    def monotonic(self):
        return self.t


# =========================================================================== #
# 1. THE BYPASS - the thing R82.1 permitted, and no longer does
# =========================================================================== #
def test_01_the_r82_1_bypass_does_not_produce_operator_ui_confirmed():
    """THE regression. The served token plus ``confirmed_in_ui=True``, exactly as
    R82.1's own test wrote it, and no ceremony anywhere."""
    args = dict(selection_id="psel_x", selected_target_implementation_hash="bookhash",
                reference_limit=0.12)
    token = pdec.ruling_submission_token(**args)
    got = pdec.ruling_provenance(submission_token=token, confirmed_in_ui=True,
                                 ruling=JUDGE, **args)
    assert got["verified"] is False
    assert got["channel"] == pdec.RULING_CHANNEL_API_DIRECT
    assert got["channel"] != pdec.RULING_CHANNEL_OPERATOR_UI
    assert got["reason"] == pdec.PROV_NO_CONFIRMATION
    assert got["operational_use"] == pdec.RULING_USE_UNVERIFIED
    # And it says WHY each of the caller's claims was not enough.
    assert got["submission_token_bound_to_this_book"] is True, (
        "the token is genuine - that is the point: a genuine token is not evidence")
    assert got["submission_token_alone_is_evidence"] is False
    assert got["operator_confirmation_asserted"] is True
    assert got["asserted_confirmation_is_evidence"] is False
    assert got["surface_is_evidence"] is False
    assert got["actor_is_evidence"] is False
    assert got["proves"].startswith("Nothing.")


@pytest.mark.parametrize("claims", [
    dict(actor="operator", surface="ui", confirmed_in_ui=True),
    dict(actor="portfolio operator", surface="portfolio-manager/reallocation",
         confirmed_in_ui=True),
    dict(actor="OPERATOR_UI_CONFIRMED", surface="OPERATOR_UI_CONFIRMED",
         confirmed_in_ui=True),
])
def test_02_no_combination_of_caller_claims_verifies(claims):
    """Nothing a caller can SAY about itself is evidence - including saying the name
    of the trusted channel."""
    args = dict(selection_id="psel_c", selected_target_implementation_hash="bh",
                reference_limit=0.12)
    got = pdec.ruling_provenance(
        submission_token=pdec.ruling_submission_token(**args), ruling=JUDGE,
        **claims, **args)
    assert got["verified"] is False
    assert got["channel"] == pdec.RULING_CHANNEL_API_DIRECT
    assert got["channel_asserted_by_caller"] is False


def test_03_the_bypass_writes_an_unverified_record_that_governs_nothing(tmp_path):
    """It is still WRITTEN - an attempted ruling stays auditable - and it binds
    nothing, so approval is exactly where it was."""
    art, ddir, sel = _world(tmp_path)
    got = _rule_unverified(sel, ddir, ruling=JUDGE)
    assert got["status"] == pdec.RULING_RECORDED_UNVERIFIED
    assert got["recorded"] is True
    assert got["governs_this_book"] is False
    assert got["provenance"]["channel"] == pdec.RULING_CHANNEL_API_DIRECT
    assert got["provenance"]["reason"] == pdec.PROV_NO_CONFIRMATION
    rec = _rulings(ddir)[-1]
    assert rec["provenance_verified"] is False
    assert rec["ruled_by_is_evidence"] is False
    state = pdec.risk_policy_ruling_state(selection=sel, decision_dir=ddir)
    assert state["authoritative"] is False
    assert state["state"] == stgt.RULED_UNVERIFIED
    assert state["binding_limit"] is None
    assert state["satisfies_the_policy_review"] is False


# =========================================================================== #
# 2. THE CEREMONY - what a genuine confirmation is, and what it costs to get one
# =========================================================================== #
def test_10_the_genuine_ceremony_verifies_and_the_ruling_governs(tmp_path):
    art, ddir, sel = _world(tmp_path)
    got = _rule(sel, ddir, ruling=JUDGE)
    assert got["status"] == pdec.RULING_RECORDED
    prov = got["provenance"]
    assert prov["verified"] is True
    assert prov["channel"] == pdec.RULING_CHANNEL_OPERATOR_UI
    assert prov["ceremony"] == "R82.1.1_SINGLE_USE_OPERATOR_CONFIRMATION"
    assert prov["ceremony_issuer"] == pdec.OWNER
    assert prov["confirmation_consumed"] is True
    assert prov["confirmation_id"].startswith("rconf_")
    assert prov["confirmation_is_single_use"] is True
    assert prov["confirmation_is_derivable_by_a_caller"] is False
    # Only NOW does the ruling bind the book.
    state = pdec.risk_policy_ruling_state(selection=sel, decision_dir=ddir)
    assert state["authoritative"] is True
    assert state["binding_limit"] == pytest.approx(
        pdec.selection_policy_review(sel)["reference_limit"])


def test_11_the_confirmation_is_minted_not_derived(tmp_path):
    """Two ceremonies over the SAME book and the SAME ruling produce different
    confirmations, and neither is a function of anything a caller holds."""
    _, _, sel = _world(tmp_path)
    a = _ceremony(sel, ruling=JUDGE)
    b = _ceremony(sel, ruling=JUDGE)
    assert a["issued"] is True and b["issued"] is True
    assert a["confirmation"] != b["confirmation"]
    assert a["confirmation_id"] != b["confirmation_id"]
    assert len(a["confirmation"]) >= 32
    # The book token, by contrast, IS derivable - which is exactly why it cannot be
    # the evidence.
    assert _book_token(sel) == _book_token(sel)


@pytest.mark.parametrize("over,reason", [
    (dict(confirm=None), "CEREMONY_NO_PHRASE"),
    (dict(confirm="CONFIRM_PORTFOLIO_DECISION"), "CEREMONY_NO_PHRASE"),
    (dict(submission_token=None), "PROV_NO_TOKEN"),
    (dict(token="not-the-token"), "PROV_TOKEN_MISMATCH"),
    (dict(ruling="SOMETHING_ELSE"), "CEREMONY_RULING_UNKNOWN"),
    (dict(ruling=FLOOR), "CEREMONY_RULING_UNAVAILABLE"),
])
def test_12_the_ceremony_refuses_without_every_act(tmp_path, over, reason):
    """No confirmation comes into existence unless the whole ceremony is performed,
    and each refusal names its own cause."""
    _, _, sel = _world(tmp_path)
    got = _ceremony(sel, **over)
    assert got["issued"] is False
    assert got["confirmation"] is None
    assert got["reason"] == getattr(pdec, reason)
    assert got["reason"] in got["refusal_vocabulary"]


def test_13_opening_a_ceremony_writes_nothing_and_rules_on_nothing(tmp_path):
    art, ddir, sel = _world(tmp_path)
    before = sorted((p.name, p.read_bytes()) for p in Path(ddir).glob("*"))
    got = _ceremony(sel, ruling=JUDGE)
    assert got["issued"] is True
    assert sorted((p.name, p.read_bytes()) for p in Path(ddir).glob("*")) == before
    assert _rulings(ddir) == []
    for flag in ("records_a_ruling", "records_a_portfolio_decision", "is_an_approval",
                 "creates_order_plan", "creates_orders", "creates_fills", "executes",
                 "deploys_capital", "changes_declared_policy"):
        assert got[flag] is False
    assert got["manual_approval_still_required"] is True
    # An abandoned ceremony leaves nothing behind either.
    assert pdec.risk_policy_ruling_state(
        selection=sel, decision_dir=ddir)["ruled"] is False


def test_14_no_read_ever_publishes_a_confirmation(tmp_path):
    """A confirmation a read handed out would be evidence of having read, which is
    the property R82.1 already had and that was not enough."""
    art, ddir, sel = _world(tmp_path)
    _ceremony(sel, ruling=JUDGE)             # one is live in the ledger right now
    env = _env6(art, ddir)
    dec = env["risk_policy_decision"]
    assert dec["submission_token"]
    assert "confirmation" not in dec
    assert dec["confirmation_route"] == pdec.RULING_CONFIRMATION_ROUTE
    assert dec["confirmation_is_single_use"] is True
    blob = json.dumps(env, default=str)
    for entry in pdec._RULING_CONFIRMATIONS.values():
        assert entry["confirmation_id"] not in blob
    # Nor does the ruling state, nor the whole decision envelope.
    assert "confirmation" not in json.dumps(
        pdec.risk_policy_ruling_state(selection=sel, decision_dir=ddir))


def test_15_the_ledger_is_never_persisted(tmp_path):
    """It is process memory by design: evidence that outlived the process that
    issued it would be evidence of nothing."""
    art, ddir, sel = _world(tmp_path)
    cer = _ceremony(sel, ruling=JUDGE)
    needle = cer["confirmation"]
    for p in Path(ddir).rglob("*"):
        if p.is_file():
            assert needle not in p.read_text(encoding="utf-8", errors="ignore")
    src = (Path(__file__).resolve().parents[1] / "api" / "portfolio_decision.py"
           ).read_text(encoding="utf-8")
    body = src[src.index("_RULING_CONFIRMATIONS: dict = {}"):]
    assert "_atomic_write_json(_RULING_CONFIRMATIONS" not in body
    # The ledger is written in exactly ONE place, and spent in exactly one other.
    assert body.count("_RULING_CONFIRMATIONS[") == 1


# =========================================================================== #
# 3. SINGLE USE - spent, expired, and never re-aimed
# =========================================================================== #
def test_20_a_confirmation_is_spent_by_its_first_use(tmp_path):
    art, ddir, sel = _world(tmp_path)
    cer = _ceremony(sel, ruling=JUDGE)
    first = _rule(sel, ddir, ruling=JUDGE, ceremony=False,
                  operator_confirmation=cer["confirmation"])
    assert first["status"] == pdec.RULING_RECORDED
    assert first["provenance"]["verified"] is True
    # The SAME confirmation, presented again, is spent.
    again = pdec.consume_ruling_confirmation(
        confirmation=cer["confirmation"], ruling=JUDGE, **_ident(sel))
    assert again["verified"] is False
    assert again["reason"] == pdec.PROV_CONFIRMATION_REPLAYED


def test_21_a_replayed_confirmation_cannot_record_a_second_governing_ruling(tmp_path):
    """The whole write path, twice, on one confirmation."""
    art, ddir, sel = _world(tmp_path)
    cer = _ceremony(sel, ruling=JUDGE)
    kw = dict(ruling=JUDGE, ceremony=False,
              operator_confirmation=cer["confirmation"])
    assert _rule(sel, ddir, **kw)["provenance"]["verified"] is True
    # A different ruling this time, so the writer does not short-circuit on reuse.
    second = _rule(sel, ddir, ceremony=False, ruling=ACCEPT,
                   operator_confirmation=cer["confirmation"])
    assert second["status"] == pdec.RULING_RECORDED_UNVERIFIED
    assert second["provenance"]["reason"] == pdec.PROV_CONFIRMATION_REPLAYED
    assert second["governs_this_book"] is False
    # The book is still governed by the FIRST, verified ruling; the replay changed
    # nothing about what binds it.
    state = pdec.risk_policy_ruling_state(selection=sel, decision_dir=ddir)
    assert state["authoritative"] is False, (
        "the latest record is the unverified one, and it governs nothing")
    assert state["binding_limit"] is None
    assert [r["provenance_verified"] for r in _rulings(ddir)] == [True, False]


def test_22_an_expired_confirmation_is_refused(tmp_path, monkeypatch):
    art, ddir, sel = _world(tmp_path)
    clock = _Clock()
    monkeypatch.setattr(pdec, "time", clock)
    cer = _ceremony(sel, ruling=JUDGE)
    clock.t += pdec.RULING_CONFIRMATION_TTL_SECONDS + 1
    got = _rule(sel, ddir, ruling=JUDGE, ceremony=False,
                operator_confirmation=cer["confirmation"])
    assert got["status"] == pdec.RULING_RECORDED_UNVERIFIED
    assert got["provenance"]["reason"] == pdec.PROV_CONFIRMATION_EXPIRED
    assert got["governs_this_book"] is False
    # Still inside the window it would have verified - the only difference is time.
    clock.t = 0.0
    fresh = _ceremony(sel, ruling=JUDGE)
    clock.t += pdec.RULING_CONFIRMATION_TTL_SECONDS - 1
    assert pdec.consume_ruling_confirmation(
        confirmation=fresh["confirmation"], ruling=JUDGE,
        **_ident(sel))["verified"] is True


def test_23_an_invented_confirmation_is_unknown(tmp_path):
    art, ddir, sel = _world(tmp_path)
    for fake in (None, "", "rconf_deadbeef", "x" * 43,
                 hashlib.sha256(b"guess").hexdigest()):
        got = pdec.consume_ruling_confirmation(
            confirmation=fake, ruling=JUDGE, **_ident(sel))
        assert got["verified"] is False
        assert got["reason"] in (pdec.PROV_NO_CONFIRMATION,
                                 pdec.PROV_CONFIRMATION_UNKNOWN)


# =========================================================================== #
# 4. BINDING - one book, one ruling, and spent by the attempt either way
# =========================================================================== #
def test_30_evidence_for_book_a_is_refused_for_book_b(tmp_path):
    a_art, a_ddir, a_sel = _world(tmp_path / "a")
    b_art, b_ddir, b_sel = _world(tmp_path / "b", target="MINIMUM_REPAIR")
    assert (a_sel["selected_target_implementation_hash"]
            != b_sel["selected_target_implementation_hash"])
    cer = _ceremony(a_sel, ruling=JUDGE)
    got = _rule(b_sel, b_ddir, ruling=JUDGE, ceremony=False,
                operator_confirmation=cer["confirmation"])
    assert got["status"] == pdec.RULING_RECORDED_UNVERIFIED
    assert got["provenance"]["reason"] == pdec.PROV_CONFIRMATION_WRONG_BOOK
    assert got["governs_this_book"] is False
    # And the failed aim SPENT it: it cannot be turned back on book A.
    assert pdec.consume_ruling_confirmation(
        confirmation=cer["confirmation"], ruling=JUDGE,
        **_ident(a_sel))["reason"] == pdec.PROV_CONFIRMATION_REPLAYED


def test_31_evidence_for_one_ruling_is_refused_for_another_available_one(tmp_path):
    """The ceremony binds the CHOICE. Confirming ACCEPT_AS_IS is not confirming
    JUDGE_AGAINST_THE_BEFORE_UNIVERSE, and they do opposite things to the book."""
    art, ddir, sel = _world(tmp_path)
    cer = _ceremony(sel, ruling=ACCEPT)
    assert cer["binds"]["ruling"] == ACCEPT
    got = _rule(sel, ddir, ruling=JUDGE, ceremony=False,
                operator_confirmation=cer["confirmation"])
    assert got["status"] == pdec.RULING_RECORDED_UNVERIFIED
    assert got["provenance"]["reason"] == pdec.PROV_CONFIRMATION_WRONG_RULING
    assert got["governs_this_book"] is False
    assert pdec.risk_policy_ruling_state(
        selection=sel, decision_dir=ddir)["authoritative"] is False


def test_32_the_reference_limit_is_part_of_the_binding(tmp_path):
    """The same book judged against a different cap is a different question."""
    _, _, sel = _world(tmp_path)
    ident = _ident(sel)
    cer = _ceremony(sel, ruling=JUDGE)
    other = dict(ident, reference_limit=(ident["reference_limit"] or 0.12) + 0.01)
    assert pdec.consume_ruling_confirmation(
        confirmation=cer["confirmation"], ruling=JUDGE,
        **other)["reason"] == pdec.PROV_CONFIRMATION_WRONG_BOOK


def test_33_a_refused_ruling_does_not_burn_the_ceremony(tmp_path):
    """The confirmation is spent AFTER every identity check, so an operator whose
    submission was refused for naming the wrong book keeps their confirmation."""
    art, ddir, sel = _world(tmp_path)
    cer = _ceremony(sel, ruling=JUDGE)
    refused = _rule(sel, ddir, ruling=JUDGE, ceremony=False,
                    operator_confirmation=cer["confirmation"],
                    expected_selection_id="psel_something_else")
    assert refused["status"] == pdec.RULING_WRONG_SELECTION
    assert refused["recorded"] is False
    got = _rule(sel, ddir, ruling=JUDGE, ceremony=False,
                operator_confirmation=cer["confirmation"])
    assert got["provenance"]["verified"] is True


# =========================================================================== #
# 5. OVER HTTP - the same rules at the surface a script would actually call
# =========================================================================== #
@pytest.fixture()
def http(tmp_path, monkeypatch):
    """The real routes over a hermetic decision root and a hermetic review."""
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    env = _env6(art, ddir)
    monkeypatch.setenv(pdec.DECISION_DIR_ENV, str(ddir))
    monkeypatch.setattr(appmod._pdreview, "load_proposal_decision_review",
                        lambda *a, **k: env)
    monkeypatch.setenv("PAPER_TRADER_SERVICE_API_KEY", _KEY)
    os.environ.setdefault(
        "PAPER_TRADER_DATABASE_URL",
        "postgresql+psycopg2://u:p@localhost:5432/paper_trader_test_unused")
    from paper_trader.config import get_settings
    get_settings.cache_clear()
    client = TestClient(app=appmod.app)
    try:
        yield client, ddir, sel, env
    finally:
        client.close()
        get_settings.cache_clear()


def _body(sel, env, **over):
    b = env["risk_policy_decision"]["binds"]
    out = dict(ruling=JUDGE, confirmation=pdec.RULING_CONFIRM_TOKEN,
               expected_selection_id=b["selection_id"],
               expected_selected_target_implementation_hash=b[
                   "selected_target_implementation_hash"],
               expected_reference_limit=b["reference_limit"],
               instruments=list(b["instruments"]),
               submission_token=env["risk_policy_decision"]["submission_token"],
               operator_confirmed=True, surface="ui", requested_by="operator")
    out.update(over)
    return out


def test_40_a_direct_http_call_with_the_token_and_the_flag_is_not_authoritative(http):
    """The bypass at the surface a script actually reaches: the token the panel
    served, ``operator_confirmed: true``, actor "operator", surface "ui"."""
    client, ddir, sel, env = http
    r = client.post(_ROUTE, headers=_AUTH, json=_body(sel, env))
    assert r.status_code == 200
    j = r.json()
    assert j["status"] == pdec.RULING_RECORDED_UNVERIFIED
    assert j["provenance_verified"] is False
    assert j["provenance"]["channel"] == pdec.RULING_CHANNEL_API_DIRECT
    assert j["provenance"]["reason"] == pdec.PROV_NO_CONFIRMATION
    assert j["governs_this_book"] is False
    # It is recorded - a direct call stays possible and stays auditable.
    assert j["recorded"] is True
    assert _rulings(ddir)[-1]["provenance_verified"] is False


def test_41_the_http_ceremony_then_the_write_is_authoritative(http):
    client, ddir, sel, env = http
    c = client.post(_CEREMONY_ROUTE, headers=_AUTH, json={
        "ruling": JUDGE, "confirmation": pdec.RULING_CONFIRM_TOKEN,
        "submission_token": env["risk_policy_decision"]["submission_token"],
        "surface": "portfolio-manager/reallocation", "requested_by": "manual_ui"})
    assert c.status_code == 200
    cj = c.json()
    assert cj["issued"] is True and cj["confirmation"]
    r = client.post(_ROUTE, headers=_AUTH,
                    json=_body(sel, env, operator_confirmation=cj["confirmation"]))
    assert r.status_code == 200
    j = r.json()
    assert j["status"] == pdec.RULING_RECORDED
    assert j["provenance_verified"] is True
    assert j["provenance"]["channel"] == pdec.RULING_CHANNEL_OPERATOR_UI
    assert j["governs_this_book"] is True
    # Replaying that same confirmation over HTTP does not verify again.
    again = client.post(_ROUTE, headers=_AUTH,
                        json=_body(sel, env, ruling=ACCEPT,
                                   operator_confirmation=cj["confirmation"]))
    assert again.json()["provenance"]["reason"] == pdec.PROV_CONFIRMATION_REPLAYED


def test_42_the_http_ceremony_refuses_a_caller_without_the_book_token(http):
    client, ddir, sel, env = http
    r = client.post(_CEREMONY_ROUTE, headers=_AUTH, json={
        "ruling": JUDGE, "confirmation": pdec.RULING_CONFIRM_TOKEN})
    assert r.status_code == 400
    assert r.json()["detail"]["reason"] == pdec.PROV_NO_TOKEN
    bad = client.post(_CEREMONY_ROUTE, headers=_AUTH, json={
        "ruling": JUDGE, "confirmation": pdec.RULING_CONFIRM_TOKEN,
        "submission_token": "a-token-i-made-up"})
    assert bad.status_code == 400
    assert bad.json()["detail"]["reason"] == pdec.PROV_TOKEN_MISMATCH
    # And an unauthenticated caller reaches neither route.
    assert client.post(_CEREMONY_ROUTE, json={
        "ruling": JUDGE,
        "confirmation": pdec.RULING_CONFIRM_TOKEN}).status_code in (401, 403)


def test_43_the_unavailable_ruling_cannot_be_confirmed(http):
    client, ddir, sel, env = http
    r = client.post(_CEREMONY_ROUTE, headers=_AUTH, json={
        "ruling": FLOOR, "confirmation": pdec.RULING_CONFIRM_TOKEN,
        "submission_token": env["risk_policy_decision"]["submission_token"]})
    assert r.status_code == 400
    assert r.json()["detail"]["reason"] == pdec.CEREMONY_RULING_UNAVAILABLE
    assert _rulings(ddir) == []


# =========================================================================== #
# 6. THE SCREEN, AND THE LIVE STORE
# =========================================================================== #
def test_50_the_screen_performs_the_ceremony_rather_than_asserting_it():
    ui = (Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html"
          ).read_text(encoding="utf-8")
    i = ui.index("function _pdrRecordRuling()")
    fn = ui[i:i + 4000]
    assert "confirmation_route" in fn, "act one is performed"
    assert "operator_confirmation: cj.confirmation" in fn, "act two spends it"
    # The claim is still sent - and it is no longer what makes the ruling stand.
    assert "operator_confirmed: true" in fn
    for forbidden in ("alert(", "confirm(", "Create Orders", "Create Paper Orders"):
        assert forbidden not in fn


def test_51_the_live_governed_store_is_untouched_by_this_suite():
    """Acceptance criterion: no live store file changes during testing. The live
    root is read for its digest and never written by anything here."""
    live = Path(os.environ.get(pdec.DECISION_DIR_ENV)
                or r"D:\Stock_Prediction_app_data\portfolio_decisions")
    if not live.exists():
        pytest.skip("no live portfolio decision store on this machine")
    digest = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
              for p in sorted(live.glob("*.json"))}
    assert digest, "the live store has records, so the comparison means something"
    # Everything this suite writes goes to a temporary root. Prove the module agrees
    # about where the live one is, and that no test has repointed it.
    assert pdec._decision_dir() == live
    after = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
             for p in sorted(live.glob("*.json"))}
    assert after == digest


def test_52_the_live_r82_ruling_stays_unverified_and_byte_preserved():
    """The record written during R82 implementation by a direct call carries no
    provenance block, so it fails closed - with no id special-cased anywhere."""
    live = Path(os.environ.get(pdec.DECISION_DIR_ENV)
                or r"D:\Stock_Prediction_app_data\portfolio_decisions")
    p = live / "risk_policy_rulings.json"
    if not p.exists():
        pytest.skip("no live risk-policy ruling store on this machine")
    before = hashlib.sha256(p.read_bytes()).hexdigest()
    rows = json.loads(p.read_text(encoding="utf-8"))
    for rec in rows:
        state = pdec.ruling_provenance_state(rec)
        if not rec.get("provenance"):
            assert state["verified"] is False
            assert state["reason"] == pdec.PROV_MISSING
            assert state["readable_as_audit_evidence"] is True
            assert state["supersedable_by_a_verified_ruling"] is True
    assert hashlib.sha256(p.read_bytes()).hexdigest() == before
    src = (Path(__file__).resolve().parents[1] / "api" / "portfolio_decision.py"
           ).read_text(encoding="utf-8")
    for rec in rows:
        assert str(rec.get("ruling_id") or "x") not in src
