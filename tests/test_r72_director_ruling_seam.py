r"""R72 - THE DIRECTOR'S RULING, AND THE GOVERNOR THAT COULD NOT READ IT.

What this suite is for
----------------------
The R59 queue held seventeen blocked jobs and no runnable work. Fourteen were
FAMILY_EXHAUSTED and three were DEPENDENCY_BLOCKED, and every one of them
carried ``clears_on = INFORMATION`` - "something has to arrive". The runtime
therefore slept, correctly, on its own reading of its own queue.

Meanwhile the R71 research director had already ruled on those three cross-asset
families, in as many words: the only information that could reopen them is
non-price terms-of-trade data the estate does not own, and they must not be
re-proposed. That is a TERMINAL state. It was recorded in a campaign JSON file,
and the governor reads a sqlite database, so the ruling reached no reader and
the loop kept waiting for information that only a purchase gate could deliver.

``alpha_agent.r59.blockers`` had already named this exact failure in its own
docstring before it could happen: "A runtime may legitimately SLEEP on a TIME
blocker; sleeping on a TERMINAL one and calling it research is the failure mode
this taxonomy exists to make visible."

So these tests assert the SEAM, in both directions:

  * a ruling must be DURABLE where the governor already reads;
  * a ruling must OVERRIDE the engine's free-text symptom, because one is a
    governance fact about a family and the other is one attempt's message;
  * the override must be VISIBLE - the recorded code is kept beside the
    authoritative one, so the ruling can be audited rather than trusted;
  * a family nobody has ruled on must NOT be implicitly cleared;
  * a ruling may not invent a twelfth blocker reason, because a second
    vocabulary is a second owner;
  * and reconciliation must MUTATE NO JOB, because the queue row belongs to the
    worker that holds the lease.

Hermetic. Every test builds its own scratch memory database in ``tmp_path`` and
none of them opens, reads or writes the live research store, the live queue, the
operational book or any evidence store. Nothing here emits a prediction, freezes
a decision, registers a challenger, promotes a model or allocates capital.
"""
from __future__ import annotations

import pytest

from paper_trader.alpha_agent.r59 import blockers as B
from paper_trader.alpha_agent.r59 import memory as M


#: The three cross-asset jobs, as the live queue actually recorded them.
NO_MEMBERS = "engine returned NO_MEMBERS"
CROSS_ASSET_FAMILIES = ("CROSS_ASSET_RELATIVE_VALUE", "CROSS_ASSET_LEAD_LAG",
                        "CROSS_ASSET_REGIME_CONDITIONING")

#: The R71 ruling, verbatim.
R71_RATIONALE = ("NOTHING from futures prices. Only genuinely non-price "
                 "terms-of-trade information (national export/import price "
                 "indices with vintages), which is not owned. Do not "
                 "re-propose.")


@pytest.fixture()
def mem(tmp_path):
    return M.open_memory(tmp_path / "research_memory.sqlite")


def _job(family: str, *, reason: str = NO_MEMBERS,
         asset_class: str = "CROSS_ASSET") -> dict:
    return {"job_id": "j_%s" % family, "lane": "r59.cross_asset.cross_asset",
            "state": "BLOCKED", "attempts": 3, "blocked_reason": reason,
            "payload": {"asset_class": asset_class, "family": family,
                        "kind": "CROSS_ASSET"}}


def _rule(mem, family: str, *, reason: str = B.WAITING_FOR_EXTERNAL_ENTITLEMENT,
          verdict: str = "REFUSED") -> dict:
    return mem.record_director_ruling(
        asset_class="CROSS_ASSET", economic_family=family, verdict=verdict,
        blocker_reason=reason, rationale=R71_RATIONALE,
        reopen_condition="OWNED_NON_PRICE_TERMS_OF_TRADE_VINTAGES",
        campaign_id="R71_FORWARD_AND_NEW_INFORMATION",
        decided_by="quant-research-director", decision_date="2026-09-25",
        source_artifact=("research/agents/"
                         "campaign_r71_forward_and_new_information/"
                         "research_director_decision.json"))


# --------------------------------------------------------------------------- #
# 1. THE STATE BEFORE THE REPAIR - reproduced, so the repair is falsifiable
# --------------------------------------------------------------------------- #
def test_01_an_unruled_job_is_classified_by_its_engine_sentence(mem):
    """The pre-R72 behaviour, which is still correct when nobody has ruled."""
    c = B.classify_job(_job(CROSS_ASSET_FAMILIES[0]), mem=mem)
    assert c["reason_code"] == B.DEPENDENCY_BLOCKED
    assert c["clears_on"] == B.CLEARS_ON_INFORMATION
    assert c["is_authoritative"] is False
    assert c["director_ruling"] is None


def test_02_the_three_cross_asset_jobs_all_read_as_merely_waiting(mem):
    """All three, so the repair is measured against the whole defect."""
    rows = [B.classify_job(_job(f), mem=mem) for f in CROSS_ASSET_FAMILIES]
    assert {r["clears_on"] for r in rows} == {B.CLEARS_ON_INFORMATION}
    s = B.summarise(rows)
    assert s["terminal_without_a_decision"] == 0
    assert s["needs_new_information"] == 3


# --------------------------------------------------------------------------- #
# 2. THE RULING IS DURABLE WHERE THE GOVERNOR ALREADY READS
# --------------------------------------------------------------------------- #
def test_03_a_ruling_round_trips_through_the_research_memory(mem):
    _rule(mem, CROSS_ASSET_FAMILIES[0])
    got = mem.director_ruling(asset_class="CROSS_ASSET",
                              economic_family=CROSS_ASSET_FAMILIES[0])
    assert got is not None
    assert got["verdict"] == "REFUSED"
    assert got["blocker_reason"] == B.WAITING_FOR_EXTERNAL_ENTITLEMENT
    assert got["decided_by"] == "quant-research-director"
    assert got["reopen_condition"] == "OWNED_NON_PRICE_TERMS_OF_TRADE_VINTAGES"
    assert "not owned" in got["rationale"]
    assert got["source_artifact"].endswith("research_director_decision.json")


def test_04_a_ruling_survives_reopening_the_store(tmp_path):
    """Durable means durable: a new handle on the same file sees it."""
    p = tmp_path / "research_memory.sqlite"
    _rule(M.open_memory(p), CROSS_ASSET_FAMILIES[0])
    again = M.open_memory(p).director_ruling(
        asset_class="CROSS_ASSET", economic_family=CROSS_ASSET_FAMILIES[0])
    assert again is not None and again["verdict"] == "REFUSED"


def test_05_a_ruling_is_readable_through_a_read_only_handle(tmp_path):
    """The classifier must never need a WRITE handle to ask a question.

    It runs while the worker holds the lease; opening the store for writing to
    classify a blocker would contend with the one process allowed to write.
    """
    p = tmp_path / "research_memory.sqlite"
    _rule(M.open_memory(p), CROSS_ASSET_FAMILIES[0])
    ro = M.open_memory_readonly(p)
    assert ro.director_ruling(asset_class="CROSS_ASSET",
                              economic_family=CROSS_ASSET_FAMILIES[0])
    c = B.classify_job(_job(CROSS_ASSET_FAMILIES[0]), mem=ro)
    assert c["reason_code"] == B.WAITING_FOR_EXTERNAL_ENTITLEMENT


def test_06_the_newest_ruling_replaces_the_previous_one(mem):
    _rule(mem, CROSS_ASSET_FAMILIES[0])
    out = _rule(mem, CROSS_ASSET_FAMILIES[0], reason=B.SUPERSEDED,
                verdict="SUPERSEDED_BY_A_LATER_ARTIFACT")
    assert out["replaced"] is not None
    assert out["replaced"]["blocker_reason"] == \
        B.WAITING_FOR_EXTERNAL_ENTITLEMENT
    now = mem.director_ruling(asset_class="CROSS_ASSET",
                              economic_family=CROSS_ASSET_FAMILIES[0])
    assert now["blocker_reason"] == B.SUPERSEDED
    assert len(mem.director_rulings(asset_class="CROSS_ASSET")) == 1


# --------------------------------------------------------------------------- #
# 3. THE RULING OVERRIDES THE SYMPTOM, AND THE OVERRIDE IS VISIBLE
# --------------------------------------------------------------------------- #
def test_07_a_ruling_moves_an_informational_blocker_to_terminal(mem):
    """The sentence the repair exists to delete."""
    _rule(mem, CROSS_ASSET_FAMILIES[0])
    c = B.classify_job(_job(CROSS_ASSET_FAMILIES[0]), mem=mem)
    assert c["reason_code"] == B.WAITING_FOR_EXTERNAL_ENTITLEMENT
    assert c["clears_on"] == B.CLEARS_TERMINAL
    assert c["is_authoritative"] is True


def test_08_the_recorded_code_is_kept_beside_the_authoritative_one(mem):
    """A ruling that cannot be audited is a ruling that must be trusted."""
    _rule(mem, CROSS_ASSET_FAMILIES[0])
    c = B.classify_job(_job(CROSS_ASSET_FAMILIES[0]), mem=mem)
    assert c["recorded_reason_code"] == B.DEPENDENCY_BLOCKED
    assert c["reason_code"] != c["recorded_reason_code"]
    dr = c["director_ruling"]
    assert dr["overrode_recorded_code"] == B.DEPENDENCY_BLOCKED
    assert dr["ruled_by"] == "quant-research-director"
    assert dr["campaign_id"] == "R71_FORWARD_AND_NEW_INFORMATION"
    assert dr["rationale"] and dr["reopen_condition"]


def test_09_a_ruling_that_agrees_with_the_text_is_still_marked_authoritative(mem):
    """Agreement is a RULING, not an absence of one."""
    _rule(mem, CROSS_ASSET_FAMILIES[0], reason=B.DEPENDENCY_BLOCKED)
    c = B.classify_job(_job(CROSS_ASSET_FAMILIES[0]), mem=mem)
    assert c["reason_code"] == B.DEPENDENCY_BLOCKED
    assert c["is_authoritative"] is True
    assert c["director_ruling"]["overrode_recorded_code"] is None


def test_10_an_unruled_family_is_never_implicitly_cleared(mem):
    """Fail-closed. Ruling one family may not speak for its neighbours."""
    _rule(mem, CROSS_ASSET_FAMILIES[0])
    other = B.classify_job(_job(CROSS_ASSET_FAMILIES[1]), mem=mem)
    assert other["is_authoritative"] is False
    assert other["reason_code"] == B.DEPENDENCY_BLOCKED
    assert other["clears_on"] == B.CLEARS_ON_INFORMATION


def test_11_the_text_only_classification_is_still_available(mem):
    """The taxonomy's own rules stay testable without a store."""
    _rule(mem, CROSS_ASSET_FAMILIES[0])
    c = B.classify_job(_job(CROSS_ASSET_FAMILIES[0]), mem=mem,
                       consult_rulings=False)
    assert c["reason_code"] == B.DEPENDENCY_BLOCKED
    assert c["is_authoritative"] is False


# --------------------------------------------------------------------------- #
# 4. A RULING MAY NOT INVENT A VOCABULARY, OR SKIP ITS OWN REASONS
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("bad", ["DIRECTOR_SAYS_NO", "", "dependency_blocked"])
def test_12_a_non_canonical_blocker_reason_is_refused(mem, bad):
    with pytest.raises(ValueError):
        mem.record_director_ruling(
            asset_class="CROSS_ASSET", economic_family="X", verdict="REFUSED",
            blocker_reason=bad, rationale="because")


def test_13_a_ruling_without_a_rationale_is_refused(mem):
    with pytest.raises(ValueError):
        mem.record_director_ruling(
            asset_class="CROSS_ASSET", economic_family="X", verdict="REFUSED",
            blocker_reason=B.SUPERSEDED, rationale="   ")


def test_14_a_ruling_must_name_an_asset_class_and_a_family(mem):
    for ac, fam in (("", "F"), ("CROSS_ASSET", "")):
        with pytest.raises(ValueError):
            mem.record_director_ruling(
                asset_class=ac, economic_family=fam, verdict="REFUSED",
                blocker_reason=B.SUPERSEDED, rationale="because")


def test_15_every_declared_ruling_code_is_in_the_taxonomy(mem):
    """Whatever is stored, a reader can always look it up."""
    for code in B.BLOCKER_REASONS:
        mem.record_director_ruling(
            asset_class="US_EQUITY", economic_family="F_%s" % code,
            verdict="RULED", blocker_reason=code, rationale="r")
    for row in mem.director_rulings(asset_class="US_EQUITY"):
        assert row["blocker_reason"] in B.BLOCKER_REASONS
        assert B.CLEARANCE[row["blocker_reason"]] in B.CLEARANCE_VOCAB
        assert B.DESCRIPTION[row["blocker_reason"]]


# --------------------------------------------------------------------------- #
# 5. RECONCILIATION REPORTS, AND MUTATES NOTHING
# --------------------------------------------------------------------------- #
def test_16_reconcile_names_every_stale_job(mem):
    for f in CROSS_ASSET_FAMILIES:
        _rule(mem, f)
    rows = [B.classify_job(_job(f), mem=mem) for f in CROSS_ASSET_FAMILIES]
    rec = B.reconcile(rows)
    assert rec["blocked_total"] == 3
    assert rec["with_a_director_ruling"] == 3
    assert rec["n_reclassified"] == 3
    assert rec["n_now_terminal"] == 3
    for s in rec["reclassified"]:
        assert s["recorded_clears_on"] == B.CLEARS_ON_INFORMATION
        assert s["authoritative_clears_on"] == B.CLEARS_TERMINAL
        assert s["ruled_by"] == "quant-research-director"
        assert s["reopen_condition"]


def test_17_reconcile_mutates_no_job(mem):
    """The queue row belongs to the worker holding the lease."""
    _rule(mem, CROSS_ASSET_FAMILIES[0])
    job = _job(CROSS_ASSET_FAMILIES[0])
    before = dict(job)
    rec = B.reconcile([B.classify_job(job, mem=mem)])
    assert rec["mutates_no_job"] is True
    assert job == before
    src = (__import__("pathlib").Path(B.__file__)).read_text(encoding="utf-8")
    body = src[src.index("def reconcile("):src.index("__all__")]
    for mutator in ("UPDATE ", "INSERT ", "DELETE ", "commit(", "_guard_write",
                    "enqueue", "claim("):
        assert mutator not in body, mutator


def test_18_an_unruled_estate_reconciles_to_nothing(mem):
    rows = [B.classify_job(_job(f), mem=mem) for f in CROSS_ASSET_FAMILIES]
    rec = B.reconcile(rows)
    assert rec["with_a_director_ruling"] == 0
    assert rec["without_a_director_ruling"] == 3
    assert rec["n_reclassified"] == 0
    assert rec["n_now_terminal"] == 0


def test_19_summarise_counts_the_authority(mem):
    for f in CROSS_ASSET_FAMILIES:
        _rule(mem, f)
    rows = [B.classify_job(_job(f), mem=mem) for f in CROSS_ASSET_FAMILIES]
    s = B.summarise(rows)
    assert s["terminal_without_a_decision"] == 3
    assert s["needs_new_information"] == 0
    assert s["authoritative_from_a_director_ruling"] == 3
    assert s["reclassified_by_a_ruling"] == 3
    assert s["every_blocker_is_classified"] is True


# --------------------------------------------------------------------------- #
# 6. THE SEAM ITSELF - a reader that cannot reach the ruling is the defect
# --------------------------------------------------------------------------- #
def test_20_the_ruling_is_recorded_as_an_event_too(mem):
    """So a ruling is visible to anyone reading the estate's history."""
    _rule(mem, CROSS_ASSET_FAMILIES[0])
    kinds = [e.get("kind") for e in mem.events(limit=50)]
    assert "DIRECTOR_RULING_RECORDED" in kinds


def test_21_a_store_that_cannot_be_read_never_fails_a_classification():
    """A blocker classification must not fail because it asked a question."""
    class Exploding:
        def director_ruling(self, **_):
            raise RuntimeError("store unavailable")

    c = B.classify_job(_job(CROSS_ASSET_FAMILIES[0]), mem=Exploding())
    assert c["reason_code"] == B.DEPENDENCY_BLOCKED
    assert c["is_authoritative"] is False


def test_22_a_job_with_no_declared_family_is_not_ruled_on(mem):
    """A ruling is keyed on a family; a job without one cannot inherit it."""
    _rule(mem, CROSS_ASSET_FAMILIES[0])
    job = _job(CROSS_ASSET_FAMILIES[0])
    job["payload"] = {"asset_class": "CROSS_ASSET"}
    c = B.classify_job(job, mem=mem)
    assert c["is_authoritative"] is False
    assert B.ruling_for({"asset_class": "CROSS_ASSET"}, mem=mem) is None


def test_23_this_suite_touches_no_live_store():
    """Measured, not promised.

    Its own body is excluded from the scan, because the names it forbids have
    to be written down somewhere in order to be forbidden.
    """
    import pathlib
    src = pathlib.Path(__file__).read_text(encoding="utf-8")
    src = src[:src.index("def test_23_this_suite_touches_no_live_store")]
    for forbidden in ("open_memory()", "open_memory_readonly()",
                      "r59_autonomous_alpha", "open_queue", "Stock_Prediction",
                      "freeze_decision", "prospective_decision",
                      "operational_book"):
        assert forbidden not in src, forbidden
    # Every store this suite opens is under pytest's tmp_path.
    assert src.count("M.open_memory") == src.count("M.open_memory(tmp_path") \
        + src.count("M.open_memory(p)") + src.count("M.open_memory_readonly(p)")
