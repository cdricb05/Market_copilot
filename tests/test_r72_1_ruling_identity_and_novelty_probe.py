r"""R72.1 - TWO WAYS AN ANSWER CAN BE MANUFACTURED RATHER THAN REACHED.

What this suite is for
----------------------
R72 made the research director's ruling durable where the governor already
reads. It left two ways for this estate's most final answers to appear without
anybody having produced them.

**A. The ruling store could not tell two mechanisms apart.**

``director_rulings`` was keyed ``(asset_class, economic_family)``. An economic
family is not a mechanism: ``US_EQUITY | EVENT_OVERREACTION`` holds the SEC
comment-letter cell AND a NASDAQ trading-halt cell, which read different
information through different models and are settled by different evidence.
Under that key, a ruling written about the first TERMINALLY closed the second -
a refusal nobody wrote, delivered by a primary key. The key is now the same
four-part mechanism identity every other reader in the estate uses, with ``*``
for a component the director did not bind.

The migration is the delicate half. The ten rulings on record bound no
mechanism, because the old key could not express one. They therefore migrate to
``('*', '*')`` - family-wide - which is *exactly* the reach they already had.
Reading a mechanism out of a ruling's rationale text would be a NARROWING, and
narrowing a recorded refusal is a governance act no migration may perform on the
director's behalf. That asymmetry is asserted here in both directions.

**B. A malformed novelty probe returned a governed refusal.**

``mechanism_state`` treated every component as optional, so a payload that bound
none of them made the match predicate vacuously true. The probe matched all
8,472 rows in research memory, found settled ones among them, and returned
``SETTLED_DO_NOT_REPEAT`` - the estate's most authoritative verdict, computed
from a question that named nothing. A malformed question must fail as malformed.
Neither of the answers it could otherwise produce is believable: the refusal is
not a governed refusal, and an absence of hits would not be novelty.

Hermetic. Every test builds its own scratch memory database in ``tmp_path``.
None opens, reads or writes the live research store, the live queue, the
operational book or any evidence store. Nothing here emits a prediction, freezes
a decision, registers a challenger, promotes a model or allocates capital.
"""
from __future__ import annotations

import sqlite3

import pytest

from paper_trader.alpha_agent.r59 import blockers as B
from paper_trader.alpha_agent.r59 import memory as M


#: The two cells that share ``US_EQUITY | EVENT_OVERREACTION`` and that the old
#: primary key could not tell apart. This is the defect, named.
COMMENT_LETTERS = "SEC_COMMENT_LETTERS"
TRADING_HALTS = "NASDAQ_TRADING_HALTS"
EVENT_FAMILY = "EVENT_OVERREACTION"

#: The legacy table, verbatim as R72 shipped it.
LEGACY_DDL = """
CREATE TABLE memory_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE director_rulings (
    asset_class       TEXT NOT NULL,
    economic_family   TEXT NOT NULL,
    verdict           TEXT NOT NULL,
    blocker_reason    TEXT NOT NULL,
    rationale         TEXT NOT NULL,
    reopen_condition  TEXT,
    campaign_id       TEXT,
    decided_by        TEXT,
    decision_date     TEXT,
    source_artifact   TEXT,
    detail_json       TEXT,
    updated_at        TEXT NOT NULL,
    PRIMARY KEY (asset_class, economic_family)
);
"""

#: The ten live rulings, reduced to the identity and provenance fields this
#: migration must carry. Shape matches the live store on 2026-09-25.
LIVE_TEN = [
    ("COMMODITY_FUTURES", "CARRY", "REFUSED_AS_ALREADY_MEASURED",
     "FAMILY_EXHAUSTED", "R73_POST_R72_AGENDA"),
    ("CROSS_ASSET", "CARRY", "REFUSED_AS_ALREADY_MEASURED",
     "FAMILY_EXHAUSTED", "R73_POST_R72_AGENDA"),
    ("COMMODITY_FUTURES", "POSITIONING", "REFUSED_AS_ALREADY_MEASURED",
     "FAMILY_EXHAUSTED", "R73_POST_R72_AGENDA"),
    ("US_EQUITY", "LIQUIDITY_PREMIUM",
     "REFUSED_A_UNIVERSE_IS_NOT_NEW_INFORMATION", "FAMILY_EXHAUSTED",
     "R73_POST_R72_AGENDA"),
    ("US_EQUITY", "RESIDUAL_MOMENTUM", "REFUSED_AS_ALREADY_MEASURED",
     "FAMILY_EXHAUSTED", "R73_POST_R72_AGENDA"),
    ("US_EQUITY", "SHORT_TERM_REVERSAL", "REFUSED_AS_ALREADY_MEASURED",
     "FAMILY_EXHAUSTED", "R73_POST_R72_AGENDA"),
    ("US_EQUITY", "PROFITABILITY", "REFUSED_AS_ALREADY_MEASURED",
     "FAMILY_EXHAUSTED", "R73_POST_R72_AGENDA"),
    ("CROSS_ASSET", "CROSS_ASSET_RELATIVE_VALUE", "REFUSED",
     "WAITING_FOR_EXTERNAL_ENTITLEMENT", "R71_FORWARD_AND_NEW_INFORMATION"),
    ("CROSS_ASSET", "CROSS_ASSET_REGIME_CONDITIONING",
     "REFUSED_AS_A_REPLICATION", "SUPERSEDED",
     "R71_FORWARD_AND_NEW_INFORMATION"),
    ("CROSS_ASSET", "CROSS_ASSET_LEAD_LAG", "CONSIDERED_AND_NOT_PROPOSED",
     "WAITING_FOR_EXTERNAL_ENTITLEMENT", "R71_FORWARD_AND_NEW_INFORMATION"),
]


def _legacy_store(path, rows) -> None:
    """A pre-R72.1 memory holding ``rows``, built without the new owner."""
    conn = sqlite3.connect(str(path))
    conn.executescript(LEGACY_DDL)
    conn.executemany(
        "INSERT INTO director_rulings VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    conn.close()


def _legacy_row(asset_class, family, verdict, reason, campaign):
    return (asset_class, family, verdict, reason,
            "rationale for %s/%s" % (asset_class, family),
            "REOPEN_%s" % family, campaign, "quant-research-director",
            "2026-09-25", "research/agents/%s/RULING.json" % campaign, "{}",
            "2026-09-25T19:07:14+00:00")


@pytest.fixture()
def mem(tmp_path):
    return M.open_memory(tmp_path / "research_memory.sqlite")


@pytest.fixture()
def migrated_ten(tmp_path):
    """The ten live rulings, recorded legacy-shaped and then migrated."""
    db = tmp_path / "research_memory.sqlite"
    _legacy_store(db, [_legacy_row(*r) for r in LIVE_TEN])
    return M.open_memory(db)


# --------------------------------------------------------------------------- #
# A. THE MIGRATION: ten rulings in, ten rulings out, none re-scoped.
# --------------------------------------------------------------------------- #
def test_the_ten_rulings_survive_the_rekey(migrated_ten):
    rows = migrated_ten.director_rulings()
    assert len(rows) == len(LIVE_TEN)
    got = {(r["asset_class"], r["economic_family"]) for r in rows}
    assert got == {(r[0], r[1]) for r in LIVE_TEN}


def test_every_migrated_ruling_keeps_its_provenance_verbatim(migrated_ten):
    """Rationale, reopen condition, director, campaign and artifact, unchanged.

    A migration that preserves the KEY but rewrites the REASON has destroyed the
    only thing that makes a refusal auditable later.
    """
    for asset_class, family, verdict, reason, campaign in LIVE_TEN:
        r = migrated_ten.director_ruling(asset_class=asset_class,
                                         economic_family=family)
        assert r is not None, (asset_class, family)
        assert r["verdict"] == verdict
        assert r["blocker_reason"] == reason
        assert r["campaign_id"] == campaign
        assert r["rationale"] == "rationale for %s/%s" % (asset_class, family)
        assert r["reopen_condition"] == "REOPEN_%s" % family
        assert r["decided_by"] == "quant-research-director"
        assert r["source_artifact"] == \
            "research/agents/%s/RULING.json" % campaign


def test_a_family_wide_ruling_does_not_become_narrower(migrated_ten):
    """It migrates to the wildcard, which is the reach it already had.

    The temptation is to read ``SEC comment letters`` out of a rationale and
    bind the ruling to that mechanism. That would narrow a recorded refusal -
    a governance act, on the director's behalf, performed by a schema change.
    """
    for asset_class, family, _v, _r, _c in LIVE_TEN:
        row = migrated_ten.director_ruling(asset_class=asset_class,
                                           economic_family=family)
        assert row["information_family"] == M.RULING_ANY
        assert row["model_family"] == M.RULING_ANY
        assert row["ruling_scope"] == M.RULING_SCOPE_FAMILY


def test_a_family_wide_ruling_does_not_become_broader(migrated_ten):
    """It still reaches only ITS family, in ITS asset class."""
    assert migrated_ten.director_ruling(
        asset_class="US_EQUITY", economic_family="CARRY") is None
    assert migrated_ten.director_ruling(
        asset_class="RATES_FUTURES", economic_family="PROFITABILITY") is None


def test_the_migration_report_is_explicit_about_the_mapping(migrated_ten):
    rep = migrated_ten.director_ruling_migration_report()
    assert rep["migration"] == M.DIRECTOR_RULING_IDENTITY_MIGRATION
    assert rep["key_before"] == ["asset_class", "economic_family"]
    assert rep["key_after"] == ["asset_class", "economic_family",
                                "information_family", "model_family"]
    assert rep["rulings_before"] == rep["rulings_after"] == len(LIVE_TEN)
    assert rep["every_ruling_preserved"] is True
    assert rep["n_narrowed"] == 0 and rep["n_broadened"] == 0
    assert len(rep["mapping"]) == len(LIVE_TEN)
    for entry in rep["mapping"]:
        assert entry["scope_before"] == M.RULING_SCOPE_FAMILY
        assert entry["scope_after"] == M.RULING_SCOPE_FAMILY
        assert entry["narrowed"] is False and entry["broadened"] is False


def test_the_collision_report_exists_and_is_empty(migrated_ten):
    """The old PK already made the target key unique, so nothing may collide.

    Reported anyway: a migration that cannot state it lost nothing has not
    shown that it lost nothing.
    """
    rep = migrated_ten.director_ruling_migration_report()
    assert rep["n_collisions"] == 0
    assert rep["collisions"] == []


def test_migration_is_idempotent(tmp_path):
    db = tmp_path / "research_memory.sqlite"
    _legacy_store(db, [_legacy_row(*r) for r in LIVE_TEN])
    first = M.open_memory(db).director_ruling_migration_report()
    for _ in range(4):
        again = M.open_memory(db)
    assert len(again.director_rulings()) == len(LIVE_TEN)
    # Re-running would stamp a new time; the report must be the FIRST one.
    assert again.director_ruling_migration_report()["migrated_at"] == \
        first["migrated_at"]


def test_a_store_born_after_r72_1_reports_no_migration(mem):
    """``None`` means "never held a family-keyed ruling", not "lost one"."""
    assert mem.director_ruling_migration_report() is None


def test_migration_refuses_to_drop_the_original_if_a_row_would_be_lost(
        tmp_path):
    """The guard, exercised. A rebuild that loses a ruling must raise.

    The INSERT is sabotaged through a connection proxy rather than a patched
    sqlite3 type, so the rest of the migration runs exactly as it ships and the
    DROP is reached by the real code path or not at all.
    """
    legacy = tmp_path / "legacy.sqlite"
    _legacy_store(legacy, [_legacy_row(*r) for r in LIVE_TEN])
    owner = M.open_memory(tmp_path / "elsewhere.sqlite")   # a fresh, migrated

    class LossyConn:
        def __init__(self, inner):
            self._inner = inner

        def execute(self, sql, *a, **k):
            if sql.lstrip().upper().startswith(
                    "INSERT INTO DIRECTOR_RULINGS__R72_1"):
                sql = sql + " WHERE rowid > 3"
            return self._inner.execute(sql, *a, **k)

    conn = sqlite3.connect(str(legacy))
    conn.row_factory = sqlite3.Row
    try:
        with pytest.raises(RuntimeError, match="would lose rulings"):
            M.ResearchMemory._migrate_director_ruling_identity(
                owner, LossyConn(conn))
        conn.rollback()
    finally:
        conn.close()
    # The original table is intact, because the DROP never ran...
    assert len(M.open_memory(legacy).director_rulings()) == len(LIVE_TEN)
    # ...and the interrupted rebuild left nothing that blocks the retry.
    assert M.open_memory(legacy).director_ruling(
        asset_class="US_EQUITY", economic_family="PROFITABILITY") is not None


def test_an_interrupted_rebuild_can_be_retried(tmp_path):
    """A leftover scratch table must not make the store unopenable."""
    db = tmp_path / "research_memory.sqlite"
    _legacy_store(db, [_legacy_row(*r) for r in LIVE_TEN])
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE director_rulings__r72_1 (x TEXT)")
    conn.commit()
    conn.close()
    assert len(M.open_memory(db).director_rulings()) == len(LIVE_TEN)


# --------------------------------------------------------------------------- #
# A. THE POINT OF THE REKEY: two mechanisms, one family, one ruling each.
# --------------------------------------------------------------------------- #
def _rule(mem, information_family=None, *, rationale, model_family=None,
          verdict="REFUSED", reason="FAMILY_EXHAUSTED"):
    return mem.record_director_ruling(
        asset_class="US_EQUITY", economic_family=EVENT_FAMILY,
        information_family=information_family, model_family=model_family,
        verdict=verdict, blocker_reason=reason, rationale=rationale,
        reopen_condition="NEW_ORTHOGONAL_INFORMATION",
        decided_by="quant-research-director", campaign_id="R72_1")


def test_a_ruling_on_one_mechanism_does_not_terminate_its_sibling(mem):
    """THE DEFECT, asserted. Comment letters closed; trading halts untouched."""
    _rule(mem, COMMENT_LETTERS, model_family="LONG_ONLY",
          rationale="comment letters are exhausted")

    closed = mem.director_ruling(
        asset_class="US_EQUITY", economic_family=EVENT_FAMILY,
        information_family=COMMENT_LETTERS, model_family="LONG_ONLY")
    assert closed is not None
    assert closed["ruling_scope"] == M.RULING_SCOPE_MECHANISM
    assert closed["matched_exactly"] is True

    untouched = mem.director_ruling(
        asset_class="US_EQUITY", economic_family=EVENT_FAMILY,
        information_family=TRADING_HALTS, model_family="LONG_ONLY")
    assert untouched is None, \
        "a ruling about SEC comment letters terminated a trading-halt cell"


def test_two_independent_mechanisms_in_one_family_keep_separate_rulings(mem):
    _rule(mem, COMMENT_LETTERS, model_family="LONG_ONLY",
          rationale="letters: exhausted")
    _rule(mem, TRADING_HALTS, model_family="LONG_ONLY",
          rationale="halts: waiting on entitlement",
          verdict="REFUSED", reason="WAITING_FOR_EXTERNAL_ENTITLEMENT")
    assert len(mem.director_rulings(economic_family=EVENT_FAMILY)) == 2
    letters = mem.director_ruling(
        asset_class="US_EQUITY", economic_family=EVENT_FAMILY,
        information_family=COMMENT_LETTERS, model_family="LONG_ONLY")
    halts = mem.director_ruling(
        asset_class="US_EQUITY", economic_family=EVENT_FAMILY,
        information_family=TRADING_HALTS, model_family="LONG_ONLY")
    assert letters["blocker_reason"] == "FAMILY_EXHAUSTED"
    assert halts["blocker_reason"] == "WAITING_FOR_EXTERNAL_ENTITLEMENT"


def test_resolution_prefers_the_mechanism_over_the_family(mem):
    _rule(mem, rationale="the whole family is closed")
    _rule(mem, COMMENT_LETTERS, model_family="LONG_ONLY",
          rationale="letters specifically",
          verdict="REFUSED_AS_A_REPLICATION", reason="SUPERSEDED")
    r = mem.director_ruling(
        asset_class="US_EQUITY", economic_family=EVENT_FAMILY,
        information_family=COMMENT_LETTERS, model_family="LONG_ONLY")
    assert r["rationale"] == "letters specifically"
    assert r["blocker_reason"] == "SUPERSEDED"
    assert r["matched_on"] == {"information_family": COMMENT_LETTERS,
                               "model_family": "LONG_ONLY"}


def test_a_family_wide_ruling_still_catches_an_unruled_mechanism(mem):
    """A genuine family-wide refusal keeps its reach. That is not the defect."""
    _rule(mem, rationale="the whole family is closed")
    r = mem.director_ruling(
        asset_class="US_EQUITY", economic_family=EVENT_FAMILY,
        information_family=TRADING_HALTS, model_family="LONG_ONLY")
    assert r is not None
    assert r["ruling_scope"] == M.RULING_SCOPE_FAMILY
    assert r["matched_exactly"] is False


def test_a_mechanism_scoped_ruling_never_reaches_a_caller_that_names_none(mem):
    """The R59 queue's jobs carry no mechanism. They must match only step 4.

    If an unnamed mechanism could match a mechanism-scoped ruling, the very
    defect this release closes would reappear through the blocker
    reconciliation instead of through the primary key.
    """
    _rule(mem, COMMENT_LETTERS, model_family="LONG_ONLY",
          rationale="letters specifically")
    assert mem.director_ruling(asset_class="US_EQUITY",
                               economic_family=EVENT_FAMILY) is None


def test_recording_a_mechanism_ruling_does_not_replace_the_family_ruling(mem):
    _rule(mem, rationale="family-wide")
    _rule(mem, COMMENT_LETTERS, model_family="LONG_ONLY",
          rationale="mechanism-only")
    rows = mem.director_rulings(economic_family=EVENT_FAMILY)
    assert len(rows) == 2
    family = mem.director_ruling(asset_class="US_EQUITY",
                                 economic_family=EVENT_FAMILY)
    assert family["rationale"] == "family-wide"


def test_re_recording_the_same_mechanism_replaces_only_that_row(mem):
    _rule(mem, rationale="family-wide")
    _rule(mem, COMMENT_LETTERS, model_family="LONG_ONLY", rationale="first")
    out = _rule(mem, COMMENT_LETTERS, model_family="LONG_ONLY",
                rationale="second")
    assert out["replaced"]["rationale"] == "first"
    assert len(mem.director_rulings(economic_family=EVENT_FAMILY)) == 2
    assert mem.director_ruling(
        asset_class="US_EQUITY", economic_family=EVENT_FAMILY,
        information_family=COMMENT_LETTERS,
        model_family="LONG_ONLY")["rationale"] == "second"


def test_an_unbound_component_is_the_wildcard_however_it_is_spelled(mem):
    """``None``, ``""`` and whitespace are one statement, not three scopes."""
    _rule(mem, "", model_family="   ", rationale="family-wide, spelled blank")
    row = mem.director_ruling(asset_class="US_EQUITY",
                              economic_family=EVENT_FAMILY)
    assert row["information_family"] == M.RULING_ANY
    assert row["model_family"] == M.RULING_ANY
    assert row["ruling_scope"] == M.RULING_SCOPE_FAMILY


def test_the_wildcard_may_not_name_an_asset_class_or_family(mem):
    with pytest.raises(ValueError, match="mechanism wildcard"):
        mem.record_director_ruling(
            asset_class=M.RULING_ANY, economic_family=EVENT_FAMILY,
            verdict="REFUSED", blocker_reason="FAMILY_EXHAUSTED",
            rationale="every asset class at once")


def test_a_ruling_still_may_not_invent_a_blocker_reason(mem):
    """R72's rule, unchanged by the rekey: no twelfth vocabulary entry."""
    with pytest.raises(ValueError, match="canonical taxonomy"):
        _rule(mem, COMMENT_LETTERS, rationale="x", reason="NOT_A_REAL_REASON")


# --------------------------------------------------------------------------- #
# A. THE LEGACY READER: a read-only handle meets an unmigrated store.
# --------------------------------------------------------------------------- #
def test_a_read_only_handle_reads_an_unmigrated_store(tmp_path):
    """The live case on 2026-09-25: reconciliation runs read-only, and a
    read-only handle can never migrate. Losing the ruling to a missing column
    would silently return three TERMINAL jobs to "waiting for information"."""
    db = tmp_path / "research_memory.sqlite"
    _legacy_store(db, [_legacy_row(*r) for r in LIVE_TEN])
    ro = M.ResearchMemory(db, read_only=True)
    rows = ro.director_rulings()
    assert len(rows) == len(LIVE_TEN)
    r = ro.director_ruling(asset_class="CROSS_ASSET",
                           economic_family="CROSS_ASSET_LEAD_LAG")
    assert r is not None
    assert r["ruling_scope"] == M.RULING_SCOPE_FAMILY
    # Still legacy on disk: reading it did not rewrite it.
    conn = sqlite3.connect(str(db))
    cols = {c[1] for c in conn.execute("PRAGMA table_info(director_rulings)")}
    conn.close()
    assert "information_family" not in cols


def test_an_unmigrated_store_reports_a_family_reach_not_a_mechanism_one(
        tmp_path):
    db = tmp_path / "research_memory.sqlite"
    _legacy_store(db, [_legacy_row(*r) for r in LIVE_TEN])
    ro = M.ResearchMemory(db, read_only=True)
    r = ro.director_ruling(asset_class="US_EQUITY",
                           economic_family="PROFITABILITY",
                           information_family="ANY_MECHANISM_AT_ALL",
                           model_family="LONG_ONLY")
    assert r["ruling_scope"] == M.RULING_SCOPE_FAMILY
    assert r["matched_exactly"] is False
    assert r["matched_on"] == {"information_family": M.RULING_ANY,
                               "model_family": M.RULING_ANY}


# --------------------------------------------------------------------------- #
# A. THE SEAM: the blocker reconciliation, end to end.
# --------------------------------------------------------------------------- #
def _job(family, *, information_family=None, model_family=None):
    payload = {"asset_class": "US_EQUITY", "family": family,
               "kind": "EVENT"}
    if information_family:
        payload["information_family"] = information_family
    if model_family:
        payload["model_family"] = model_family
    return {"job_id": "j_%s" % (information_family or family),
            "lane": "r59.us_equity.event", "state": "BLOCKED", "attempts": 3,
            "blocked_reason": "engine returned NO_MEMBERS",
            "payload": payload}


def test_reconciliation_moves_only_the_ruled_mechanism(mem):
    _rule(mem, COMMENT_LETTERS, model_family="LONG_ONLY",
          rationale="letters exhausted")
    ruled = B.classify_job(
        _job(EVENT_FAMILY, information_family=COMMENT_LETTERS,
             model_family="LONG_ONLY"), mem=mem)
    sibling = B.classify_job(
        _job(EVENT_FAMILY, information_family=TRADING_HALTS,
             model_family="LONG_ONLY"), mem=mem)
    assert ruled["is_authoritative"] is True
    assert ruled["reason_code"] == "FAMILY_EXHAUSTED"
    assert ruled["director_ruling"]["ruling_scope"] == M.RULING_SCOPE_MECHANISM
    assert sibling["is_authoritative"] is False
    assert sibling["reason_code"] == sibling["recorded_reason_code"]


def test_the_reach_of_the_ruling_that_moved_a_job_is_visible(mem):
    """An auditor must be able to see that a FAMILY-WIDE ruling moved a job."""
    _rule(mem, rationale="family-wide")
    row = B.classify_job(
        _job(EVENT_FAMILY, information_family=TRADING_HALTS,
             model_family="LONG_ONLY"), mem=mem)
    authority = row["director_ruling"]
    assert authority["ruling_scope"] == M.RULING_SCOPE_FAMILY
    assert authority["matched_exactly"] is False
    assert authority["matched_on"]["information_family"] == M.RULING_ANY


def test_a_job_naming_no_mechanism_is_unmoved_by_a_mechanism_ruling(mem):
    """The seventeen live jobs' shape: asset_class and family, nothing else."""
    _rule(mem, COMMENT_LETTERS, model_family="LONG_ONLY", rationale="letters")
    row = B.classify_job(_job(EVENT_FAMILY), mem=mem)
    assert row["is_authoritative"] is False
    assert row["director_ruling"] is None


# --------------------------------------------------------------------------- #
# B. THE MALFORMED NOVELTY PROBE.
# --------------------------------------------------------------------------- #
def _settled(mem, n=6):
    """A memory holding settled rows across two mechanisms in one family."""
    for i in range(n):
        hid = mem.register(
            title="h%d" % i, release="R72_1", origin="TEST",
            generation_method="TEST", asset_class="US_EQUITY",
            economic_family=EVENT_FAMILY,
            information_family=COMMENT_LETTERS if i % 2 else TRADING_HALTS,
            model_family="LONG_ONLY", spec={"i": i})
        mem.record_result(hid, outcome="NO_ALPHA_EVIDENCE",
                          statistic={"lockbox_t": 0.4 + i})
    return mem


@pytest.mark.parametrize("payload", [
    {},
    {"asset_class": None, "economic_family": None,
     "information_family": None, "model_family": None},
    {"asset_class": "", "economic_family": "", "information_family": "",
     "model_family": ""},
    {"asset_class": "   ", "economic_family": None},
])
def test_an_unbound_mechanism_query_is_refused_not_answered(mem, payload):
    _settled(mem)
    st = mem.mechanism_state(**payload)
    assert st["query_valid"] is False
    assert st["invalid_query_reason"] == M.MECHANISM_QUERY_UNBOUND
    assert st["mechanism_is_settled"] is False
    assert st["n_matching"] == 0 and st["n_settled"] == 0


def test_the_unbound_query_used_to_match_every_row(mem):
    """The defect, measured on this fixture rather than asserted in prose."""
    _settled(mem, n=6)
    assert len(mem.list_hypotheses(limit=10)) == 6
    st = mem.mechanism_state()
    assert st["n_matching"] == 0, \
        "an unbound query still matched the whole store"


@pytest.mark.parametrize("payload,expect_bound", [
    ({"asset_class": "US_EQUITY"}, ["asset_class"]),
    ({"economic_family": EVENT_FAMILY}, ["economic_family"]),
    ({"information_family": COMMENT_LETTERS}, ["information_family"]),
    ({"model_family": "LONG_ONLY"}, ["model_family"]),
    ({"asset_class": "US_EQUITY", "economic_family": EVENT_FAMILY},
     ["asset_class", "economic_family"]),
])
def test_a_partial_identity_query_is_still_valid(mem, payload, expect_bound):
    """Preserved behaviour: asking about a whole family is a real question."""
    _settled(mem)
    st = mem.mechanism_state(**payload)
    assert st["query_valid"] is True
    assert st["invalid_query_reason"] is None
    assert st["bound_components"] == sorted(expect_bound)
    assert st["n_matching"] > 0
    assert st["mechanism_is_settled"] is True


def test_a_blank_component_narrows_nothing_rather_than_filtering_to_blank(mem):
    """The mirror defect: a blank must not yield a false NO_ROW_IN_MEMORY."""
    _settled(mem)
    st = mem.mechanism_state(asset_class="US_EQUITY", economic_family="")
    assert st["query_valid"] is True
    assert st["bound_components"] == ["asset_class"]
    assert st["n_matching"] > 0


def test_a_full_identity_query_is_unchanged(mem):
    _settled(mem)
    st = mem.mechanism_state(asset_class="US_EQUITY",
                             economic_family=EVENT_FAMILY,
                             information_family=COMMENT_LETTERS,
                             model_family="LONG_ONLY")
    assert st["query_valid"] is True
    assert st["n_bound_components"] == 4
    assert st["mechanism_is_settled"] is True
    assert st["best_lockbox_t"] is not None


def test_a_full_identity_query_for_an_untested_mechanism_is_novel(mem):
    _settled(mem)
    st = mem.mechanism_state(asset_class="US_EQUITY",
                             economic_family=EVENT_FAMILY,
                             information_family="SOMETHING_NOBODY_TESTED",
                             model_family="LONG_ONLY")
    assert st["query_valid"] is True
    assert st["n_matching"] == 0
    assert st["mechanism_is_settled"] is False


def test_is_novel_still_attaches_a_mechanism_and_never_an_unbound_one(mem):
    """``is_novel`` guards its own call; it must not acquire INVALID_QUERY."""
    _settled(mem)
    plain = mem.is_novel(family=EVENT_FAMILY, spec={"i": 0})
    assert plain.get("mechanism") is None
    with_mech = mem.is_novel(family=EVENT_FAMILY, spec={"i": 0},
                             asset_class="US_EQUITY")
    assert with_mech["mechanism"]["query_valid"] is True
    assert with_mech["mechanism"]["invalid_query_reason"] is None


# --------------------------------------------------------------------------- #
# B. THE PROBE SURFACE: what the director's command actually prints.
# --------------------------------------------------------------------------- #
@pytest.fixture()
def probe():
    import importlib.util
    from pathlib import Path
    path = Path(__file__).resolve().parents[1] / "scripts" / \
        "alpha_agents_v2.py"
    spec = importlib.util.spec_from_file_location("_a2_probe", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod._check_mechanism


def test_the_probe_reports_invalid_query_not_a_governed_refusal(mem, probe):
    _settled(mem)
    out = probe(mem, {})
    assert out["verdict"] == "INVALID_QUERY"
    assert out["is_governed_refusal"] is False
    assert out["novelty_claim_is_believable"] is False
    assert "INVALID_QUERY" in out["verdict_vocabulary"]


def test_the_probe_never_calls_a_malformed_query_settled(mem, probe):
    """The exact symptom: SETTLED_DO_NOT_REPEAT from a payload naming nothing."""
    _settled(mem)
    for payload in ({}, {"asset_class": None}, {"economic_family": "  "}):
        assert probe(mem, payload)["verdict"] != "SETTLED_DO_NOT_REPEAT"


def test_the_probe_still_refuses_a_settled_mechanism(mem, probe):
    _settled(mem)
    out = probe(mem, {"asset_class": "US_EQUITY",
                      "economic_family": EVENT_FAMILY,
                      "information_family": COMMENT_LETTERS,
                      "model_family": "LONG_ONLY"})
    assert out["verdict"] == "SETTLED_DO_NOT_REPEAT"
    assert out["is_governed_refusal"] is True
    assert out["novelty_claim_is_believable"] is False


def test_the_probe_still_believes_a_genuinely_novel_mechanism(mem, probe):
    _settled(mem)
    out = probe(mem, {"asset_class": "US_EQUITY",
                      "economic_family": EVENT_FAMILY,
                      "information_family": "NEVER_TESTED",
                      "model_family": "LONG_ONLY"})
    assert out["verdict"] == "NO_ROW_IN_MEMORY"
    assert out["novelty_claim_is_believable"] is True


def test_the_probe_reads_nothing_and_runs_nothing(mem, probe):
    _settled(mem)
    out = probe(mem, {})
    assert out["read_only"] is True
    assert out["experiments_run"] == 0
    assert out["evaluation_samples_read"] == 0
