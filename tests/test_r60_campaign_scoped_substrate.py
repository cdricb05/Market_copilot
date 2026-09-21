r"""INFORMATION_FRONTIER_ALPHA_OFFENSIVE_V1 - the spawn gate must stay able to
ask for data the estate has never certified.

Hermetic. Every test runs against a research memory inside ``tmp_path``. No
test measures an experiment, reveals a lockbox, spends statistical budget or
consumes a research campaign.

THE DEFECT
----------
``briefs.campaign_state`` scopes every fact it reports to the campaign's own
experiment ids - except three. ``data_certified`` / ``universe_defined`` /
``features_published`` asked the memory whether ANY dataset, ANY universe and
ANY feature set had ever been recorded. Those events are estate-wide and
permanent, so from the first certified dataset onwards the spawn plan skipped
the data-foundation, universe-construction and feature-library agents for every
campaign that followed, with the reason code FOUNDATION_DONE.

That is fatal precisely for the campaign this run is: one whose premise is
information the estate has NEVER certified. ``pipeline.preregister`` still
refuses a hypothesis whose feature set is not published, whose universe is not
defined, or whose dataset is not PIT_SAFE - so the campaign would have been
refused at WRITE time by the very agents the gate declined to SPAWN.

THE RULE
--------
A campaign spec DECLARES its substrate:

    "requires": {"datasets": [...], "universes": [...], "feature_sets": [...]}

and a foundation role is skipped only when every declared id is already in the
memory. A spec that declares nothing keeps the estate-wide answer, so every
campaign spec written before this change reads exactly as it did before.
"""
from __future__ import annotations

import pytest

from paper_trader.alpha_agent import agents_v2 as A
from paper_trader.alpha_agent import r59
from paper_trader.alpha_agent.agents_v2 import briefs as BR
from paper_trader.alpha_agent.agents_v2 import pipeline as P
from paper_trader.alpha_agent.agents_v2 import routing as RT
from paper_trader.alpha_agent.r59 import memory as M

CAMPAIGN = "HERMETIC_R60_SUBSTRATE_ACCEPTANCE"

#: The three roles the estate-wide flags could permanently silence.
FOUNDATION_ROLES = (A.DATA_FOUNDATION, A.UNIVERSE, A.FEATURES)


@pytest.fixture()
def pipe(tmp_path, monkeypatch):
    monkeypatch.setenv(r59.RESEARCH_ROOT_ENV, str(tmp_path / "r59_root"))
    mem = M.ResearchMemory(tmp_path / "research_memory.sqlite")
    return P.AgentPipeline(mem, artifact_root=tmp_path / "agents_v2")


def _certify_old_estate(pipe) -> None:
    """What a PRIOR campaign left behind: one certified chain, unrelated to
    anything the next campaign wants to ask."""
    pipe.perform(A.DATA_FOUNDATION, "certify_data", dict(
        dataset_id="old_estate_panel", asset_classes=[r59.AC_FX],
        pit_status=P.PIT_SAFE,
        availability_rule="value stamped at the instant it became observable",
        survivorship="expired contracts retained"))
    pipe.perform(A.UNIVERSE, "define_universe", dict(
        universe_id="old_estate_universe", dataset_id="old_estate_panel",
        asset_class=r59.AC_FX, execution_representation="LONG_SHORT",
        short_leg_expressible=True,
        rules="liquidity floor from trailing data only"))
    pipe.perform(A.FEATURES, "publish_features", dict(
        feature_set_id="old_estate_features",
        universe_id="old_estate_universe",
        features=[{"name": "f1", "lag": 1, "source": "old_estate_panel"}],
        leakage_check="PASS"))


def _spec(requires=None) -> dict:
    spec = {"campaign_id": CAMPAIGN, "experiments": []}
    if requires is not None:
        spec["requires"] = requires
    return spec


NEW_SUBSTRATE = {"datasets": ["brand_new_information_v1"],
                 "universes": ["brand_new_universe"],
                 "feature_sets": ["brand_new_features"]}


def _roles(plan: dict) -> dict:
    return {d["role"]: d for d in plan["roles"]}


# --------------------------------------------------------------------------- #
# The defect itself
# --------------------------------------------------------------------------- #
def test_new_substrate_still_spawns_the_foundation_roles(pipe):
    """A prior campaign's certification must not answer a NEW campaign's
    question. This is the regression: before the fix all three were SKIP."""
    _certify_old_estate(pipe)
    state = BR.campaign_state(pipe, CAMPAIGN, _spec(NEW_SUBSTRATE))

    assert state["data_certified"] is False
    assert state["universe_defined"] is False
    assert state["features_published"] is False

    plan = RT.spawn_plan(state)
    for role in FOUNDATION_ROLES:
        assert role in plan["spawn"], "%s was not spawnable" % role


def test_declared_substrate_that_exists_is_skipped(pipe):
    """The efficiency discipline is unchanged: a role with nothing to do is
    still not spawned, and the skip still carries a reason code."""
    _certify_old_estate(pipe)
    state = BR.campaign_state(pipe, CAMPAIGN, _spec(
        {"datasets": ["old_estate_panel"],
         "universes": ["old_estate_universe"],
         "feature_sets": ["old_estate_features"]}))

    assert state["data_certified"] is True
    assert state["universe_defined"] is True
    assert state["features_published"] is True

    plan = RT.spawn_plan(state)
    roles = _roles(plan)
    for role in FOUNDATION_ROLES:
        assert role in plan["skip"]
        assert roles[role]["reason"], "a skip must carry a reason code"


def test_partially_certified_substrate_still_spawns(pipe):
    """One of two declared datasets certified is NOT done. A campaign that
    got half its substrate must still be able to ask for the other half."""
    _certify_old_estate(pipe)
    state = BR.campaign_state(pipe, CAMPAIGN, _spec(
        {"datasets": ["old_estate_panel", "brand_new_information_v1"]}))

    assert state["data_certified"] is False
    assert state["substrate_missing"]["datasets"] == [
        "brand_new_information_v1"]
    assert A.DATA_FOUNDATION in RT.spawn_plan(state)["spawn"]


# --------------------------------------------------------------------------- #
# Backward compatibility - the settled campaigns must read as they did
# --------------------------------------------------------------------------- #
def test_spec_declaring_nothing_keeps_the_estate_wide_answer(pipe):
    """R56 and R57 specs carry no ``requires`` block. Their projection must be
    byte-for-byte the behaviour they were measured under."""
    _certify_old_estate(pipe)
    for spec in (None, _spec(), _spec({})):
        state = BR.campaign_state(pipe, CAMPAIGN, spec)
        assert state["data_certified"] is True
        assert state["universe_defined"] is True
        assert state["features_published"] is True
        assert RT.spawn_plan(state)["spawn"] == [A.DIRECTOR]


def test_empty_estate_still_spawns_everything(pipe):
    """With nothing certified at all, a spec that declares nothing is still
    told to build its foundation."""
    state = BR.campaign_state(pipe, CAMPAIGN, None)
    assert state["data_certified"] is False
    for role in FOUNDATION_ROLES:
        assert role in RT.spawn_plan(state)["spawn"]


# --------------------------------------------------------------------------- #
# The projection stays a projection
# --------------------------------------------------------------------------- #
def test_state_reports_what_is_missing_not_just_that_something_is(pipe):
    """A spawn decision the orchestrator cannot explain is a spawn decision it
    will argue with. The state names the exact unmet ids."""
    _certify_old_estate(pipe)
    state = BR.campaign_state(pipe, CAMPAIGN, _spec(NEW_SUBSTRATE))
    assert state["substrate_required"] == {
        "datasets": ["brand_new_information_v1"],
        "universes": ["brand_new_universe"],
        "feature_sets": ["brand_new_features"]}
    assert state["substrate_missing"] == state["substrate_required"]


def test_no_new_verb_and_the_gate_is_still_the_only_writer(pipe):
    """``campaign_state`` is READ-ONLY. Asking it about substrate the memory
    does not hold must not create that substrate."""
    state = BR.campaign_state(pipe, CAMPAIGN, _spec(NEW_SUBSTRATE))
    assert state["data_certified"] is False
    assert pipe._latest(P.EV_DATA, "brand_new_information_v1") is None
    assert pipe._latest(P.EV_UNIVERSE, "brand_new_universe") is None
    assert pipe._latest(P.EV_FEATURES, "brand_new_features") is None
    # and a second read is identical - no accumulating side effect
    assert BR.campaign_state(pipe, CAMPAIGN, _spec(NEW_SUBSTRATE)) == state


# --------------------------------------------------------------------------- #
# The skeptic must be shown the burden its verdict is charged against
# --------------------------------------------------------------------------- #
def test_skeptic_brief_burden_matches_the_charged_denominator(pipe):
    """R60's skeptic caught this: the brief reported campaign_cells 0 while
    ``pipeline.skeptic_review`` charged 20.

    ``briefs.skeptic_brief`` called the right owner but omitted
    ``campaign_method``, so the campaign's own prosecuted cells fell out of
    the denominator the skeptic was shown - always understating it, always in
    the direction that flatters the candidate. A reviewer told the search was
    narrower than it was is a reviewer being argued with, not briefed.
    """
    from paper_trader.alpha_agent.r59 import handlers as H

    charged = H.search_denominator(
        pipe.mem, family_key="", asset_class=r59.AC_US_EQUITY,
        machine_generated=False, campaign_method=A.GENERATION_METHOD,
        within_family_tests=1)
    shown = H.search_denominator(
        pipe.mem, family_key="", asset_class=r59.AC_US_EQUITY,
        machine_generated=False, within_family_tests=1)
    # The two differ ONLY by the campaign term; that is the bug's signature.
    assert set(charged) == set(shown)
    assert charged["campaign_cells"] >= shown["campaign_cells"]

    brief = BR.skeptic_brief(pipe, run_id="R60", campaign_id=CAMPAIGN,
                             experiment_id="does-not-exist", result=None)
    burden = brief["METRICS"]["BURDEN"]
    assert burden.get("campaign_cells") == charged["campaign_cells"], (
        "the brief must carry the campaign term the review is charged")
    assert burden.get("total") == charged["total"]


# --------------------------------------------------------------------------- #
# The director's brief must not grow with the census
# --------------------------------------------------------------------------- #
def test_brief_dataset_block_is_bounded_however_large_the_census(pipe):
    """The census's dataset list grows every time a campaign certifies data.

    R60 added four rows and pushed the director's brief from 499 to 501 words
    against a 500-word contract - a breach with no plausible connection, for a
    future reader, between cause and symptom. The brief now carries a bounded
    digest plus a pointer, so the contract holds at any census size.
    """
    census = {"available_pit_datasets": [
        {"dataset": "ds_%03d" % i, "asset_class": "US_EQUITY",
         "state": "OWNED, heavily mined with a great deal of prose attached"}
        for i in range(200)]}
    digest = BR._dataset_digest(census)
    assert len(digest) == BR.MAX_BRIEF_DATASETS
    assert BR.prose_words(digest) < 200


def test_brief_prefers_actionable_datasets_over_spent_ones(pipe):
    """A brief with room for twelve rows must not spend them on the families
    the estate has already mined out."""
    census = {"available_pit_datasets": (
        [{"dataset": "spent_%d" % i, "asset_class": "US_EQUITY",
          "state": "OWNED, heavily mined"} for i in range(20)]
        + [{"dataset": "fresh", "asset_class": "US_EQUITY",
            "state": "ALREADY_OWNED_UNUSED"}])}
    names = [d["dataset"] for d in BR._dataset_digest(census)]
    assert names[0] == "fresh", names[:3]


def test_dataset_digest_is_stable(pipe):
    """Two runs against one census produce the same brief, so a re-brief after
    a context reset is not a different brief."""
    census = {"available_pit_datasets": [
        {"dataset": "ds_%02d" % i, "asset_class": "US_EQUITY",
         "state": "FREE_AVAILABLE" if i % 3 else "OWNED, heavily mined"}
        for i in range(40)]}
    assert BR._dataset_digest(census) == BR._dataset_digest(census)


def test_real_census_keeps_the_director_brief_inside_its_contract(pipe):
    """The live census, as it stands on disk right now, must brief cleanly."""
    brief = BR.director_brief(pipe, run_id="R60", campaign_id=CAMPAIGN,
                              spec=None)
    assert BR.brief_problems(brief) == []
    assert brief["FACTS"]["available_datasets_pointer"]


def test_preregister_still_refuses_uncertified_substrate(pipe):
    """The reason the gate had to change: the WRITE-time rule is unchanged and
    unchallenged. Spawning the foundation agents is the only legal way through
    it - never a softer pre-registration."""
    _certify_old_estate(pipe)
    with pytest.raises(P.PipelineRefusal) as exc:
        pipe.perform(A.DIRECTOR, "preregister", dict(
            owning_agent=A.MOMENTUM,
            hypothesis="a hypothesis on substrate nobody certified",
            asset_class=r59.AC_US_EQUITY, family="FUNDAMENTAL_MOMENTUM",
            feature_set_id="brand_new_features", horizon_sessions=21,
            parameters={"lookback": 63}, long_short=False,
            discovery_sample={"start": r59.DISCOVERY_START,
                              "end": r59.VALIDATION_START},
            evaluation_sample={"validation": r59.VALIDATION_START,
                               "lockbox": r59.LOCKBOX_START},
            cost_model={"rate_per_side": 0.0025,
                        "basis": "traded notional"},
            expected_sign=1, information_family="NEW_INFORMATION",
            instrument_scope=["AAA"], venue="RESEARCH"))
    assert exc.value.code == "FEATURES_NOT_PUBLISHED"
