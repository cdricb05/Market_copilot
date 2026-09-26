r"""R74.1 - A HEALTHY PROCESS RUNNING OBSOLETE CODE IS STILL A STALE PRODUCER.

On 2026-09-25 the leased PaperTrader-InformationCollection worker had loaded
application release 736f60a15764 on 2026-09-24 and was still running it. Its
heartbeat was seconds old, its singleton lock was held, its iterations were
advancing: every liveness signal the system had said HEALTHY. The scheduled task
could not replace it, because the single-instance lease it holds is exactly what
stops a second worker starting.

At 20:35:57Z the governed Daily Research Cycle wrote the opportunity-cost
artifact for eligible date 2026-09-24 under ``hoc_decision_policy.v2``. At
21:27:06Z - fifty-two minutes LATER - the stale worker wrote its own artifact for
the same book and the same eligible date under ``hoc_decision_policy.v1``, and
because it arrived later it took the session's index pointer. Every downstream
HOC and reassessment identity read then disagreed with the governed run.

Three independent defects had to line up, and this module pins all three:

  1. The HOC persistence owner decided versioning on the ECONOMIC state, the
     assessment EVIDENCE and the CONCLUSION. All three are properties of the
     assessment. None of them is the POLICY that produced it, so no outcome
     could refuse an older policy superseding a newer one.
  2. The collection owner's released-authority flag was the literal ``True``.
     Its loaded-release identity was captured once, at worker start, and a
     capture taken once went on authorising prospective writes for the whole
     life of the process.
  3. The service state write used a FIXED temp filename and no tolerance for a
     Windows sharing race, so a legitimate concurrent reader of
     ``collection_service_state.json`` could fail the ``os.replace`` outright
     and kill the iteration (observed 2026-09-14T21:32:57Z, WinError 5).

What is NOT asserted here: that history was repaired. The 2026-09-24 artifacts
and their supersession records are immutable and stay exactly as written. The
guard stops the NEXT stale write; it does not rewrite the one that happened.
"""
from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path

import pytest

from paper_trader.api import holding_opportunity_cost as hoc
from paper_trader.api import information_collection as ic
from paper_trader.api import runtime_identity as rid


# --------------------------------------------------------------------------- #
# 1. The HOC monotonic decision-policy guard
# --------------------------------------------------------------------------- #
def _entry(*, artifact_id: str, policy: str, assessment_hash: str,
           evidence_hash: str = "ev", econ: str = "econ") -> dict:
    return {"artifact_id": artifact_id, "decision_policy_version": policy,
            "assessment_hash": assessment_hash,
            "assessment_evidence_hash": evidence_hash,
            "economic_state_hash": econ,
            "decision_fingerprint": "fp_" + artifact_id,
            "eligible_market_date": "2026-09-24",
            "active_book_id": "alpha_paper_book_1"}


def test_01_policy_version_parts_orders_only_what_it_can_order():
    assert hoc._policy_version_parts("hoc_decision_policy.v2") == (
        "hoc_decision_policy", 2)
    assert hoc._policy_version_parts("hoc_decision_policy.v10") == (
        "hoc_decision_policy", 10)
    # Unorderable spellings are None, never a guessed ordinal: a guard that
    # invented an order would refuse writes it cannot actually judge.
    for bad in (None, "", "v2", "hoc_decision_policy", "hoc_decision_policy.vNEXT",
                "hoc_decision_policy.2"):
        assert hoc._policy_version_parts(bad) is None, bad


def test_02_the_newest_policy_is_read_from_the_CHAIN_not_the_pointer():
    """The pointer is what a stale producer moves, so the guard reads the chain.

    This is the 2026-09-24 state exactly: the top-level pointer names the v1
    artifact, and v2 survives only inside ``versions``. A guard that consulted
    the pointer would let the SECOND stale write through.
    """
    existing = {
        **_entry(artifact_id="hoc_v1", policy="hoc_decision_policy.v1",
                 assessment_hash="h1"),
        "versions": [
            _entry(artifact_id="hoc_v2", policy="hoc_decision_policy.v2",
                   assessment_hash="h2"),
            _entry(artifact_id="hoc_v1", policy="hoc_decision_policy.v1",
                   assessment_hash="h1"),
        ],
    }
    assert existing["decision_policy_version"] == "hoc_decision_policy.v1"
    assert hoc.highest_indexed_policy_version(existing) == "hoc_decision_policy.v2"


def test_03_an_older_policy_is_refused_and_a_newer_one_is_not():
    existing = {**_entry(artifact_id="hoc_v2", policy="hoc_decision_policy.v2",
                         assessment_hash="h2"), "versions": []}

    refusal = hoc.stale_policy_refusal(
        incoming_policy_version="hoc_decision_policy.v1", existing=existing)
    assert refusal is not None
    assert refusal["incoming_ordinal"] == 1
    assert refusal["newest_indexed_ordinal"] == 2
    assert "may not supersede" in refusal["reason"]

    # Equal and forward are both admissible: the guard is monotonicity, not a
    # freeze. A v2 -> v3 migration must still be able to land.
    assert hoc.stale_policy_refusal(
        incoming_policy_version="hoc_decision_policy.v2", existing=existing) is None
    assert hoc.stale_policy_refusal(
        incoming_policy_version="hoc_decision_policy.v3", existing=existing) is None


def test_04_the_guard_refuses_only_what_it_can_prove():
    """No existing record, an unparseable version, or a different family: all pass.

    A gate that failed closed on every case it did not understand would break
    every caller rather than the one that is wrong.
    """
    existing = {**_entry(artifact_id="hoc_v2", policy="hoc_decision_policy.v2",
                         assessment_hash="h2"), "versions": []}
    assert hoc.stale_policy_refusal(
        incoming_policy_version="hoc_decision_policy.v1", existing=None) is None
    assert hoc.stale_policy_refusal(
        incoming_policy_version=None, existing=existing) is None
    assert hoc.stale_policy_refusal(
        incoming_policy_version="some_other_policy.v1", existing=existing) is None
    legacy = {**_entry(artifact_id="hoc_old", policy=None,
                       assessment_hash="h0"), "versions": []}
    assert hoc.stale_policy_refusal(
        incoming_policy_version="hoc_decision_policy.v1", existing=legacy) is None


def test_05_the_refusal_is_a_named_persistence_outcome():
    assert hoc.PERSIST_STALE_POLICY == "REJECTED_STALE_DECISION_POLICY"
    assert hoc.PERSIST_STALE_POLICY in hoc.PERSIST_STATUS_VOCAB
    # It is NOT a success: nothing retrievable was written.
    assert hoc.PERSIST_STALE_POLICY not in hoc.PERSIST_SUCCESS_STATUSES
    # The five Release-54.3 outcomes are still spelled exactly as they were.
    for name in ("CREATED", "REUSED_EXISTING", "CREATED_NEW_VERSION",
                 "CREATED_ASSESSMENT_VERSION", "CONFLICT_REJECTED",
                 "REJECTED_INCONSISTENT_IDENTITY"):
        assert name in hoc.PERSIST_STATUS_VOCAB


def test_06_persist_assessment_refuses_the_stale_write_end_to_end(tmp_path,
                                                                 monkeypatch):
    """The whole September-24 sequence, replayed against a temp store.

    v2 lands, then a producer on older code offers v1 for the same book and
    eligible date. The v1 write must be refused, no artifact file may appear for
    it, and the index pointer must still name the v2 artifact.
    """
    store = tmp_path / "hoc"
    ic_date = "2026-09-24"
    book = "alpha_paper_book_1"

    def _ic(policy_version: str, *, evidence: str) -> dict:
        return {"eligible_market_date": ic_date, "active_book_id": book,
                "portfolio_state_hash": "ps_" + evidence,
                "economic_state_hash": "econ_same",
                "corporate_actions_hash": "ca",
                "universe_scoring_hash": "us_" + evidence}

    def _persist(policy_version: str, *, evidence: str, ahash: str):
        monkeypatch.setattr(hoc, "DECISION_POLICY_VERSION", policy_version)
        result = {"assessment_state": hoc.STATE_READY,
                  "assessment_hash": ahash,
                  "rows": [{"ticker": "AAA", "recommendation": "HOLD"}],
                  "evidence_marker": evidence}
        return hoc.persist_assessment(result=result,
                                      input_contract=_ic(policy_version,
                                                         evidence=evidence),
                                      hoc_dir=store)

    governed = _persist("hoc_decision_policy.v2", evidence="governed", ahash="h_v2")
    assert governed["persisted"] is True, governed
    assert governed["status"] in hoc.PERSIST_SUCCESS_STATUSES
    v2_artifact_id = governed["artifact_id"]

    stale = _persist("hoc_decision_policy.v1", evidence="stale", ahash="h_v1")
    assert stale["status"] == hoc.PERSIST_STALE_POLICY, stale
    assert stale["persisted"] is False
    assert stale["artifact_id"] is None
    assert stale["stale_decision_policy"]["newest_indexed_ordinal"] == 2
    assert stale["stale_decision_policy"]["incoming_ordinal"] == 1
    assert "Restart" in stale["stale_decision_policy"]["remediation"]

    # The pointer did not move and the v2 artifact is untouched.
    index = json.loads((store / "index.json").read_text(encoding="utf-8"))
    entry = index["%s|%s" % (book, ic_date)]
    assert entry["artifact_id"] == v2_artifact_id
    assert entry["decision_policy_version"] == "hoc_decision_policy.v2"
    # No artifact file was written for the refused v1 assessment.
    written = sorted(p.name for p in (store / "artifacts").glob("*.json"))
    assert written == ["%s.json" % v2_artifact_id], written


def test_07_a_forward_migration_still_creates_a_version(tmp_path, monkeypatch):
    """The guard must not freeze the store: v2 -> v3 has to land normally."""
    store = tmp_path / "hoc"
    book, date = "alpha_paper_book_1", "2026-09-24"

    def _persist(policy_version: str, *, evidence: str, ahash: str):
        monkeypatch.setattr(hoc, "DECISION_POLICY_VERSION", policy_version)
        return hoc.persist_assessment(
            result={"assessment_state": hoc.STATE_READY, "assessment_hash": ahash,
                    "rows": [{"ticker": "AAA"}], "evidence_marker": evidence},
            input_contract={"eligible_market_date": date, "active_book_id": book,
                            "portfolio_state_hash": "ps_" + evidence,
                            "economic_state_hash": "econ_same",
                            "corporate_actions_hash": "ca",
                            "universe_scoring_hash": "us_" + evidence},
            hoc_dir=store)

    assert _persist("hoc_decision_policy.v2", evidence="a",
                    ahash="h_a")["persisted"] is True
    forward = _persist("hoc_decision_policy.v3", evidence="b", ahash="h_b")
    assert forward["status"] != hoc.PERSIST_STALE_POLICY, forward
    assert forward["persisted"] is True
    index = json.loads((store / "index.json").read_text(encoding="utf-8"))
    entry = index["%s|%s" % (book, date)]
    assert entry["decision_policy_version"] == "hoc_decision_policy.v3"
    # The superseded v2 stays discoverable in the append-only chain.
    assert len(entry["versions"]) == 2
    assert entry["versions"][0]["decision_policy_version"] == "hoc_decision_policy.v2"


# --------------------------------------------------------------------------- #
# 2. The bounded released-authority attestation
# --------------------------------------------------------------------------- #
def _loaded(commit: str) -> dict:
    return {"identity_kind": "LOADED_APPLICATION_IDENTITY", "commit": commit,
            "captured_at": "2026-09-24T20:35:02+00:00"}


def _source(commit: str) -> dict:
    return {"identity_kind": "SOURCE_REPOSITORY_IDENTITY", "commit": commit,
            "read_at": "2026-09-26T15:48:00+00:00", "dirty": False}


@pytest.fixture(autouse=True)
def _clear_attestation_cache():
    ic.reset_source_attestation_cache_for_tests()
    yield
    ic.reset_source_attestation_cache_for_tests()


def test_08_a_stale_loaded_release_loses_its_released_authority():
    att = ic.source_attestation(
        loaded_release=_loaded("736f60a15764aaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        source_identity=_source("38dc0141519961ecb1e15cb5cea7c9ef4c5b1556"))
    assert att["outcome"] == ic.ATTEST_SOURCE_OBSOLETE
    assert att["within_authority"] is False
    assert att["verdict"] == rid.ALIGNMENT_STALE
    # The reason has to tell an operator what to DO, not merely that it refused.
    assert "Restart" in att["reason"]
    assert "manage_information_collection.ps1" in att["reason"]


def test_09_an_aligned_worker_keeps_its_authority():
    same = "38dc0141519961ecb1e15cb5cea7c9ef4c5b1556"
    att = ic.source_attestation(loaded_release=_loaded(same),
                               source_identity=_source(same))
    assert att["outcome"] == ic.ATTEST_WITHIN_AUTHORITY
    assert att["within_authority"] is True
    assert att["verdict"] == rid.ALIGNMENT_ALIGNED


def test_10_an_unprovable_alignment_does_NOT_refuse():
    """UNKNOWN is not evidence of obsolescence.

    Refusing here would stop collection whenever a git directory momentarily
    could not be read - a permanent, uncollectable information miss traded for
    a defect that was never proven.
    """
    att = ic.source_attestation(loaded_release=None,
                               source_identity=_source("38dc014"))
    assert att["verdict"] == rid.ALIGNMENT_UNKNOWN
    assert att["outcome"] == ic.ATTEST_NOT_PROVABLE
    assert att["within_authority"] is True

    ic.reset_source_attestation_cache_for_tests()
    att = ic.source_attestation(loaded_release=_loaded("736f60a"),
                               source_identity={"commit": None})
    assert att["verdict"] == rid.ALIGNMENT_UNKNOWN
    assert att["within_authority"] is True


def test_11_the_attestation_is_BOUNDED_in_age():
    """It may be reused briefly, then it must be re-earned.

    The startup-only capture is the defect being fixed, so the replacement is
    explicitly not allowed to become a second capture that lives forever.
    """
    assert ic.ATTESTATION_MAX_AGE_SECONDS <= 60.0
    # Shorter than the shortest collection cadence, or it could span an iteration.
    assert ic.ATTESTATION_MAX_AGE_SECONDS < 300.0

    calls = {"n": 0}
    real = rid.read_source_identity

    def counting(**kw):
        calls["n"] += 1
        return real(**kw)

    same = "38dc0141519961ecb1e15cb5cea7c9ef4c5b1556"
    # Within the window the cached attestation is reused (no second source read).
    first = ic.source_attestation(loaded_release=_loaded(same), now_monotonic=1000.0,
                                  source_identity=_source(same))
    assert "reused_within_seconds" not in first
    second = ic.source_attestation(loaded_release=_loaded(same), now_monotonic=1000.5)
    assert second["reused_within_seconds"] == pytest.approx(0.5)
    # Past the window it is re-taken rather than reused.
    third = ic.source_attestation(
        loaded_release=_loaded(same),
        now_monotonic=1000.0 + ic.ATTESTATION_MAX_AGE_SECONDS + 1.0,
        source_identity=_source(same))
    assert "reused_within_seconds" not in third


def test_12_the_attestation_never_raises_through_the_iteration(monkeypatch):
    def boom(**kw):
        raise RuntimeError("git directory unreadable")

    monkeypatch.setattr(rid, "read_source_identity", boom)
    att = ic.source_attestation(loaded_release=_loaded("736f60a"))
    assert att["outcome"] == ic.ATTEST_NOT_PROVABLE
    assert att["within_authority"] is True
    assert "UNPROVEN" in att["reason"]


def test_13_the_collection_owner_never_reads_HEAD_itself():
    """R55.2's rule survives R74.1.

    ``api/information_collection.py`` may not read the revision on disk. A module
    that reads HEAD is one step from reporting it as what some process loaded,
    and that would make every stale worker look current - the exact inversion
    this release is fixing. So the attestation is DELEGATED: the comparison, the
    refusal rule and the bounded cache all live with the identity owner.
    """
    ic_text = Path(ic.__file__).read_text(encoding="utf-8")
    assert "read_source_identity(" not in ic_text
    assert "rev-parse" not in ic_text
    # And no second implementation of the commit comparison here.
    assert "loaded_commit ==" not in ic_text
    assert 'verdict = "STALE' not in ic_text

    # The vocabulary is BOUND from the owner, never restated.
    assert ic.ATTEST_SOURCE_OBSOLETE is rid.ATTEST_SOURCE_OBSOLETE
    assert ic.ATTEST_OUTCOMES is rid.ATTEST_OUTCOMES
    assert ic.ATTESTATION_MAX_AGE_SECONDS == rid.ATTESTATION_MAX_AGE_SECONDS

    # The decision itself is the identity owner's classifier, unchanged.
    rid_text = Path(rid.__file__).read_text(encoding="utf-8")
    start = rid_text.index("def released_authority(")
    end = rid_text.index("\ndef ", start + 1)
    body = rid_text[start:end]
    assert "classify_alignment(" in body
    # The identity owner stays inert: it writes nothing and restarts nothing.
    for forbidden in ("write_text(", "mkdir(", "unlink(", "Popen", "os.kill",
                      "threading."):
        assert forbidden not in body, forbidden


# --------------------------------------------------------------------------- #
# 3. The atomic state write
# --------------------------------------------------------------------------- #
def test_14_the_temp_filename_is_unique_per_write(tmp_path):
    """A FIXED temp name let concurrent writers share one path and orphan it.

    The observed failure named that exact orphan:
    ``collection_service_state.json.tmp -> collection_service_state.json``.
    """
    target = tmp_path / "collection_service_state.json"
    seen = set()
    real_replace = os.replace

    def capture(src, dst):
        seen.add(os.path.basename(str(src)))
        return real_replace(src, dst)

    import paper_trader.api.information_collection as mod
    orig = mod.os.replace
    mod.os.replace = capture
    try:
        for i in range(5):
            mod._atomic_write_json(target, {"i": i})
    finally:
        mod.os.replace = orig

    assert len(seen) == 5, seen
    # And never the fixed name that collided.
    assert "collection_service_state.json.tmp" not in seen
    assert json.loads(target.read_text(encoding="utf-8")) == {"i": 4}


def test_15_a_transient_sharing_race_is_retried_not_raised(tmp_path):
    """WinError 5 on the RENAME is a reader holding the destination, not corruption."""
    target = tmp_path / "collection_service_state.json"
    target.write_text('{"old": true}', encoding="utf-8")

    import paper_trader.api.information_collection as mod
    real_replace = os.replace
    attempts = {"n": 0}

    def flaky(src, dst):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise PermissionError(5, "Access is denied")
        return real_replace(src, dst)

    orig = mod.os.replace
    mod.os.replace = flaky
    try:
        mod._atomic_write_json(target, {"new": True})
    finally:
        mod.os.replace = orig

    assert attempts["n"] == 3
    assert json.loads(target.read_text(encoding="utf-8")) == {"new": True}
    # No orphan temp file survived the retries.
    assert sorted(p.name for p in tmp_path.glob("*.tmp")) == []


def test_16_a_permanent_failure_still_raises_and_leaves_no_debris(tmp_path):
    """Silently swallowing the write would turn a visible failure into a stale lie."""
    target = tmp_path / "collection_service_state.json"
    target.write_text('{"old": true}', encoding="utf-8")

    import paper_trader.api.information_collection as mod

    def always(src, dst):
        raise PermissionError(5, "Access is denied")

    orig = mod.os.replace
    mod.os.replace = always
    try:
        with pytest.raises(PermissionError):
            mod._atomic_write_json(target, {"new": True})
    finally:
        mod.os.replace = orig

    # The prior file is intact - the failed replace never damaged it ...
    assert json.loads(target.read_text(encoding="utf-8")) == {"old": True}
    # ... and the orphan temp file was cleaned up.
    assert sorted(p.name for p in tmp_path.glob("*.tmp")) == []


def test_17_a_concurrent_reader_never_observes_a_partial_file(tmp_path):
    """The replace stays atomic: a reader sees the old bytes or the new ones.

    It reads through this owner's OWN reader, ``_read_json``, because that is what
    every production reader of this file uses and it already absorbs ``OSError``.
    That matters: on Windows the destination is briefly unopenable *while* the
    rename is in flight, so a raw ``read_text`` can itself raise WinError 5 - the
    mirror image of the write-side failure this release fixes. A transient miss is
    therefore expected and returns ``None``; what must NEVER happen is a payload
    that parses but is partial, truncated or from no write at all.
    """
    import paper_trader.api.information_collection as mod
    target = tmp_path / "collection_service_state.json"
    mod._atomic_write_json(target, {"n": 0, "pad": "x" * 20000})

    stop = threading.Event()
    bad: list = []
    seen = {"ok": 0, "miss": 0}

    def reader():
        while not stop.is_set():
            payload = mod._read_json(target)
            if payload is None:
                seen["miss"] += 1
            elif "n" not in payload or len(payload.get("pad") or "") != 20000:
                bad.append(payload)
            else:
                seen["ok"] += 1
            # A real reader reads the state and goes away; it does not hold the
            # destination open at 100% duty. Without this the thread is a pathological
            # spin that can starve ANY finite replace budget, which measures the test
            # rather than the code.
            time.sleep(0.0005)

    t = threading.Thread(target=reader, daemon=True)
    t.start()
    try:
        for i in range(1, 60):
            mod._atomic_write_json(target, {"n": i, "pad": "x" * 20000})
            time.sleep(0.001)
    finally:
        stop.set()
        t.join(timeout=5)

    assert bad == [], bad[:2]
    # The test would be vacuous if the reader had never actually read anything.
    assert seen["ok"] > 0, seen
    assert json.loads(target.read_text(encoding="utf-8"))["n"] == 59


# --------------------------------------------------------------------------- #
# 4. History is NOT rewritten
# --------------------------------------------------------------------------- #
def test_18_the_guard_repairs_nothing_and_rewrites_nothing(tmp_path, monkeypatch):
    """Containment is forward-only.

    The inverted 2026-09-24 pointer is a historical fact with its own
    supersession record. Nothing added in R74.1 may move it, delete it or
    re-point it: reconstructing evidence after the fact is the thing this
    project refuses on principle.
    """
    store = tmp_path / "hoc"
    book, date = "alpha_paper_book_1", "2026-09-24"

    def _persist(policy_version: str, *, evidence: str, ahash: str):
        monkeypatch.setattr(hoc, "DECISION_POLICY_VERSION", policy_version)
        return hoc.persist_assessment(
            result={"assessment_state": hoc.STATE_READY, "assessment_hash": ahash,
                    "rows": [{"ticker": "AAA"}], "evidence_marker": evidence},
            input_contract={"eligible_market_date": date, "active_book_id": book,
                            "portfolio_state_hash": "ps_" + evidence,
                            "economic_state_hash": "econ_same",
                            "corporate_actions_hash": "ca",
                            "universe_scoring_hash": "us_" + evidence},
            hoc_dir=store)

    # Reproduce an ALREADY-inverted session: v1 holds the pointer, v2 is in the
    # chain behind it. This is the live 2026-09-24 shape.
    _persist("hoc_decision_policy.v2", evidence="governed", ahash="h_v2")
    index_path = store / "index.json"
    index = json.loads(index_path.read_text(encoding="utf-8"))
    key = "%s|%s" % (book, date)
    v2_entry = index[key]["versions"][0]
    inverted = {**v2_entry, "artifact_id": "hoc_stale_v1",
                "decision_policy_version": "hoc_decision_policy.v1",
                "assessment_hash": "h_stale_v1",
                "supersedes_artifact_id": v2_entry["artifact_id"]}
    index[key] = {**inverted, "versions": [v2_entry, inverted]}
    index_path.write_text(json.dumps(index, indent=2), encoding="utf-8")
    before = index_path.read_text(encoding="utf-8")

    # A further v1 write is refused - and refused on the CHAIN's v2, not on the
    # v1 the pointer names, which is why the first stale write cannot legitimise
    # the second.
    again = _persist("hoc_decision_policy.v1", evidence="stale2", ahash="h_stale_2")
    assert again["status"] == hoc.PERSIST_STALE_POLICY, again
    assert again["stale_decision_policy"]["newest_indexed_ordinal"] == 2

    # The historical record is byte-identical: no repair, no re-point, no delete.
    assert index_path.read_text(encoding="utf-8") == before
    still = json.loads(before)[key]
    assert still["artifact_id"] == "hoc_stale_v1"
    assert still["supersedes_artifact_id"] == v2_entry["artifact_id"]
    assert [v["decision_policy_version"] for v in still["versions"]] == [
        "hoc_decision_policy.v2", "hoc_decision_policy.v1"]
