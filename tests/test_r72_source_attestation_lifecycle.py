r"""R72 - A CLEAN-SOURCE ATTESTATION TAKEN ONCE IS NOT AN ATTESTATION.

What this suite is for
----------------------
``alpha_agent.r59.runtime.maturation_policy`` gates the one thing in this estate
that can never be recomputed later: a PROSPECTIVE forward-evidence row, which is
a record of what was known at a moment. Its rule is right - only a source known
to be committed and clean may write one - and its reasoning is written down:
"writing it from an uncommitted worktree would permanently attribute rows to
code that never existed in the history."

The defect was never the rule. It was WHEN the rule ran. ``run_forever``
evaluated it ONCE, before entering its loop, and the R52 worker is persistent by
design - it holds its lease for days across an unbounded number of cycles. So a
single verdict taken at startup authorised every later prospective write, from
whatever the checkout had become since. The estate measured this on itself: a
worker that started clean at 93cf809 went on reporting ``COMMITTED_CLEAN_SOURCE``
and maturing forward evidence while the working tree was edited underneath it.

R72 re-takes the attestation AT THE MOMENT OF USE, and makes it stricter by one
condition. A checkout that has moved to a different commit is refused even when
it is perfectly clean, because the modules already in memory came from the old
revision while anything read from disk now comes from the new one - and that
mixture is not a revision at all, so a row written under it could be attributed
to neither.

Hermetic. Every test injects its own source reader; none runs a worker, takes a
lease, opens the live research store, reads git, emits a prediction, freezes a
decision, writes an evidence store, promotes a model or allocates capital.
"""
from __future__ import annotations

import pytest

from paper_trader.alpha_agent.r59 import runtime as RT

REPO = "C:\\deployed\\paper_trader"
OLD = "93cf809d5d00c0b7f0f3b0e9a0d2e6f1a0000000"
NEW = "ffffffffffffffffffffffffffffffffffffffff"


def _src(*, commit=OLD, dirty=False, resolved="GIT",
         deployed=None, pin=None) -> dict:
    return {"commit": commit, "commit_short": (commit or "")[:12],
            "branch": "stage19-controlled-rebalance", "dirty": dirty,
            "resolved_from": resolved, "repo_root": REPO,
            "is_deployed_source": deployed, "deployment_pin": pin}


def _identity(**kw) -> dict:
    return {"instance_id": "i1", "pid": 1, "host": "h",
            "started_at": "2026-09-25T18:10:26+00:00",
            "source": _src(**kw)}


def _reader(**kw):
    def _r(*, repo_root=None):
        return _src(**kw)
    return _r


# --------------------------------------------------------------------------- #
# 1. THE DEFECT, REPRODUCED
# --------------------------------------------------------------------------- #
def test_01_the_startup_policy_authorises_a_clean_start():
    """Unchanged behaviour: the startup verdict itself was never wrong."""
    v = RT.maturation_policy(_identity())
    assert v["allowed"] is True
    assert v["reason"] == "COMMITTED_CLEAN_SOURCE"


def test_02_the_startup_policy_cannot_see_a_later_change():
    """The whole defect in one assertion.

    The identity is a SNAPSHOT. Asking it again returns the same answer however
    much the checkout has moved, because it is not looking at the checkout.
    """
    ident = _identity(dirty=False)
    assert RT.maturation_policy(ident)["allowed"] is True
    # the tree is now filthy, and the snapshot is serenely unaware
    assert RT.maturation_policy(ident)["allowed"] is True


# --------------------------------------------------------------------------- #
# 2. THE REPAIR: RE-TAKEN AT THE MOMENT OF USE
# --------------------------------------------------------------------------- #
def test_03_a_clean_unchanged_checkout_is_still_allowed():
    v = RT.maturation_policy_now(_identity(), reader=_reader())
    assert v["allowed"] is True
    assert v["reason"] == "COMMITTED_CLEAN_SOURCE"
    assert v["rechecked"] is True


def test_04_a_checkout_that_became_dirty_is_refused():
    """The live case: the worker started clean and the tree was then edited."""
    v = RT.maturation_policy_now(_identity(dirty=False),
                                 reader=_reader(dirty=True))
    assert v["allowed"] is False
    assert v["reason"] == "SOURCE_HAS_UNCOMMITTED_CHANGES"
    assert v["current_dirty"] is True
    assert v["startup_commit"] == OLD


def test_05_a_clean_checkout_that_MOVED_is_refused():
    """Stricter than startup by exactly one condition, and this is it.

    Nothing is dirty. The worker is simply not running the code on disk any
    more, so a prospective row belongs to neither revision.
    """
    v = RT.maturation_policy_now(_identity(commit=OLD),
                                 reader=_reader(commit=NEW))
    assert v["allowed"] is False
    assert v["reason"] == RT.SOURCE_MOVED
    assert v["startup_commit"] == OLD
    assert v["current_commit"] == NEW
    assert "different programs" in v["detail"]
    assert "Restart the worker" in v["detail"]


def test_06_an_unresolvable_revision_is_refused():
    v = RT.maturation_policy_now(_identity(),
                                 reader=_reader(commit=None,
                                                resolved="UNRESOLVED"))
    assert v["allowed"] is False
    assert v["reason"] == "SOURCE_REVISION_UNRESOLVED"


def test_07_a_reader_that_raises_refuses_rather_than_propagates():
    """Fail CLOSED, and never take the worker down for asking."""
    def boom(*, repo_root=None):
        raise RuntimeError("git is not available")

    v = RT.maturation_policy_now(_identity(), reader=boom)
    assert v["allowed"] is False
    assert v["reason"] == "SOURCE_REVISION_UNRESOLVED"
    assert v["rechecked"] is True


def test_08_a_non_deployed_checkout_is_still_refused(monkeypatch):
    """The startup rule is inherited whole, not re-implemented.

    The deployment pin is read from the ENVIRONMENT by ``source_identity``,
    not supplied by the injected reader - the reader answers "which revision",
    the environment answers "which checkout is the deployment". So the pin is
    set here rather than faked, which is also what makes this a real test of
    the inherited rule.
    """
    monkeypatch.setenv(RT.DEPLOYED_ROOT_ENV, "C:\\somewhere\\else\\entirely")
    v = RT.maturation_policy_now(_identity(), reader=_reader())
    assert v["allowed"] is False
    assert v["reason"] == "SOURCE_IS_NOT_THE_DEPLOYED_CHECKOUT"


def test_08b_the_recheck_reads_the_pin_from_the_environment_each_time():
    """A deployment pin set AFTER startup must be honoured, not cached."""
    import os
    before = RT.maturation_policy_now(_identity(), reader=_reader())
    assert before["allowed"] is True
    os.environ[RT.DEPLOYED_ROOT_ENV] = "C:\\not\\this\\repo"
    try:
        after = RT.maturation_policy_now(_identity(), reader=_reader())
        assert after["allowed"] is False
        assert after["reason"] == "SOURCE_IS_NOT_THE_DEPLOYED_CHECKOUT"
    finally:
        os.environ.pop(RT.DEPLOYED_ROOT_ENV, None)


@pytest.mark.parametrize("missing", ["startup", "both"])
def test_09_a_missing_startup_commit_never_manufactures_a_match(missing):
    """An unknown startup revision may not be treated as 'unchanged'."""
    ident = _identity(commit=None) if missing == "startup" else {"source": {}}
    v = RT.maturation_policy_now(ident, reader=_reader(commit=NEW))
    # It is allowed or refused on the CURRENT source's own merits, and the
    # moved-commit rule simply cannot fire without two commits to compare.
    assert v["reason"] in ("COMMITTED_CLEAN_SOURCE", RT.SOURCE_MOVED,
                           "SOURCE_REVISION_UNRESOLVED")
    if v["reason"] == "COMMITTED_CLEAN_SOURCE":
        assert v["startup_commit"] in (None, "")


# --------------------------------------------------------------------------- #
# 3. THE WIRING - the loop must USE the re-taken verdict
# --------------------------------------------------------------------------- #
def _runtime_src() -> str:
    import pathlib
    return pathlib.Path(RT.__file__).read_text(encoding="utf-8")


def test_10_the_loop_gates_maturation_on_the_rechecked_verdict():
    """Source-level, because this is the wiring the defect was made of.

    A future edit that goes back to reading the startup verdict at the
    maturation point fails HERE, and not six weeks later in a row attributed
    to code that never existed.
    """
    src = _runtime_src()
    i = src.index("# ---- 2. FORWARD EVIDENCE")
    block = src[i:i + 1800]
    assert "maturation_policy_now(identity" in block, (
        "the maturation gate no longer re-takes the source attestation")
    assert "if gate.get(\"allowed\")" in block, (
        "the maturation is gated on something other than the re-taken verdict")
    assert "if maturation.get(\"allowed\")" not in block, (
        "the loop is back to inheriting the STARTUP verdict at the point of "
        "use, which is the defect itself")


def test_11_an_operator_disable_still_outranks_a_clean_tree():
    """A human refusal is not overturned by re-reading git."""
    src = _runtime_src()
    i = src.index("# ---- 2. FORWARD EVIDENCE")
    block = src[i:i + 1800]
    assert "if allow_maturation is False:" in block
    assert block.index("if allow_maturation is False:") < \
        block.index("maturation_policy_now(identity")


def test_12_a_refusal_after_recheck_is_logged_not_silent():
    """A skipped maturation nobody can see is the silence R72 removed."""
    src = _runtime_src()
    assert "maturation_refused_after_recheck" in src
    i = src.index("maturation_refused_after_recheck")
    block = src[i:i + 500]
    for field in ("startup_commit", "current_commit", "current_dirty",
                  "reason"):
        assert field in block, field


def test_13_the_status_publishes_both_attestations():
    """Beside, never instead of: they differ exactly when it matters."""
    src = _runtime_src()
    assert '"maturation_now": maturation_now' in src
    assert '"maturation": maturation,' in src


def test_14_the_recheck_is_exported():
    assert "maturation_policy_now" in RT.__all__
    assert "SOURCE_MOVED" in RT.__all__
    assert callable(RT.maturation_policy_now)


def test_15_nothing_here_writes_prospective_evidence():
    """This module gates a write; it must never perform one."""
    src = _runtime_src()
    i = src.index("def maturation_policy_now")
    body = src[i:src.index("\n\n\n", i)]
    for forbidden in ("freeze_decision", "record_result", "write_artifact",
                      "INSERT ", "UPDATE ", "commit()", "adopt_forward",
                      "_mature_forward_evidence"):
        assert forbidden not in body, forbidden
