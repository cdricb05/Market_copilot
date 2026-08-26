"""alpha_agent.r46.campaign - run the tournament once, end to end.

The order matters and is fixed:

1. freeze the contract;
2. record the shell-policy state, violation or not;
3. probe every challenger's data path and register the cohort, adopting the
   five prior registries read-only;
4. score anything that has already matured - BEFORE emitting, so a run can
   never be accused of having seen its own new outcome;
5. emit the next TRUE_FORWARD batch, idempotently;
6. re-run emission to PROVE idempotency, in-run, rather than asserting it;
7. build the leaderboard;
8. publish the maturity schedule and both burden ledgers.

Step 6 is not a test. It runs in production every time, because "idempotent"
is a claim about the system as configured right now, and the cheapest place to
discover that a deduplication key drifted is the run that would otherwise have
written the duplicate.

This function writes ONLY into the R46 research root. It touches no
operational store, no portfolio, no order, no scheduler and no prior
release's artifacts.
"""
from __future__ import annotations

import datetime as _dt

from . import CAMPAIGN_ID, artifact_body, campaign_dir, write_json
from . import analyst as AN
from . import burden as BD
from . import clock as CK
from . import contract as C
from . import emit as EM
from . import judge as JD
from . import leaderboard as LB
from . import ledger as LG
from . import marketdata as MD
from . import options as OP
from . import registry as RG
from . import shell_policy as SP

CALCULATION_OWNER = "alpha_agent.r46.campaign"

FINAL_ARTIFACT = "R46_FINAL_VERDICT.json"


def run(campaign_id: str = CAMPAIGN_ID, emitted_at: _dt.datetime = None,
        emit_batch: bool = True, acquire_options: bool = True) -> dict:
    started = CK.now_utc()
    now = emitted_at or started

    contract = C.write()
    shell = SP.write()

    registry = RG.register(campaign_id, frozen_at=CK.iso(now))

    judged_before = JD.score_pending(campaign_id, now)

    emission = None
    idempotency = None
    if emit_batch:
        emission = EM.emit(campaign_id, registry, now)
        replay = EM.emit(campaign_id, registry, now)
        idempotency = {
            "second_run_appended": replay.get("n_appended", 0),
            "second_run_duplicates_skipped":
                replay.get("n_duplicates_skipped", 0),
            "no_duplicate_created": replay.get("n_appended", 0) == 0,
            "verdict": ("IDEMPOTENT" if replay.get("n_appended", 0) == 0
                        else "NOT_IDEMPOTENT"),
            "proved_in_run": True,
        }

    options_lane = OP.run(acquire=acquire_options, campaign_id=campaign_id)
    analyst_lane = AN.run(campaign_id)

    board = LB.build(campaign_id, registry)
    schedule = EM.maturity_schedule(campaign_id)
    hist_burden = BD.historical(campaign_id)
    prosp_burden = BD.prospective(campaign_id)
    chain = LG.verify(campaign_id)

    terminal = _terminal_state(emission, board, chain, idempotency)

    verdict = artifact_body(
        "r46_final_verdict/1", CALCULATION_OWNER,
        started_utc=CK.iso(started),
        finished_utc=CK.iso(CK.now_utc()),
        contract_hash=contract["contract_hash"],
        objective=C.OBJECTIVE,
        the_question=C.THE_QUESTION,

        TERMINAL_STATE=terminal,

        # Whether the first batch EXISTS, not whether this particular run
        # happened to append one. A correctly idempotent second run appends
        # nothing and must not report that the tournament never started.
        FIRST_TRUE_FORWARD_BATCH_EMITTED=bool(
            board["total_forward_predictions_emitted"] > 0),
        first_batch=_first_batch(campaign_id, emission),

        CHALLENGERS_REGISTERED=registry["n_total"],
        CHALLENGERS_ACTIVE=registry["n_active"],
        CHALLENGERS_BLOCKED=registry["n_blocked"],
        ASSET_CLASSES_ACTIVE=registry["asset_classes_active"],
        HORIZONS_ACTIVE=registry["horizons_active"],

        TRUE_FORWARD_LEDGER_RESULT=("INTACT" if chain["all_intact"]
                                    else "CHAIN_BROKEN"),
        OUTCOME_JUDGE_RESULT=_judge_result(judged_before),
        LEADERBOARD_RESULT=("BUILT n_rows=%d" % board["n_rows"]),
        IDEMPOTENCY_RESULT=(idempotency or {}).get("verdict", "NOT_RUN"),
        PIT_RESULT=_pit_result(campaign_id),

        EARLIEST_PREDICTION_MATURITY=schedule.get("earliest_maturity"),
        NEXT_MATERIAL_EVIDENCE_TIME=schedule.get(
            "next_material_evidence_time"),

        GLOBAL_HISTORICAL_SEARCH_BURDEN=hist_burden["GLOBAL_SEARCH_BURDEN"],
        NEW_R46_HISTORICAL_TRIALS=hist_burden["new_r46_effective_trials"],
        PROSPECTIVE_FORWARD_EVIDENCE=prosp_burden,

        OPTIONS_PROGRESS=_lane_line(options_lane),
        ANALYST_REVISION_PROGRESS=_analyst_line(analyst_lane),

        MONEY_SPENT_USD=0.0,
        NEW_ACCOUNTS=0,
        LICENCES_ACCEPTED=0,
        OPERATIONAL_WRITES=0,
        PORTFOLIO_MUTATIONS=0,
        ORDERS=0,
        MODEL_PROMOTIONS=0,
        SCHEDULER_CHANGES=0,
        SHELL_POLICY_VIOLATION=shell["SHELL_POLICY_VIOLATION"],

        options_lane=options_lane,
        analyst_lane=analyst_lane,
        registry=_registry_digest(registry),
        emission=emission,
        idempotency=idempotency,
        judge_run=judged_before,
        maturity_schedule=schedule,
        leaderboard_digest=_board_digest(board),
        ledger=LG.summary(campaign_id),
        historical_burden=hist_burden,
        provider_state=MD.provider_state(),
        shell_policy=shell,
        the_finding=_the_finding(registry),
    )
    write_json(campaign_dir(campaign_id) / FINAL_ARTIFACT, verdict)
    return verdict


# --------------------------------------------------------------------------- #
def _first_batch(campaign_id: str, emission) -> dict:
    """The FIRST batch ever emitted, read from the batch artifact.

    Not the batch this run produced: once the tournament is running, most runs
    correctly produce none, and reporting the first batch as absent because
    today's call was a no-op would misstate the record.
    """
    from . import read_json
    body = read_json(campaign_dir(campaign_id) / EM.BATCH_ARTIFACT,
                     default=None) or {}
    batches = list(body.get("batches") or [])
    if not batches:
        return {"emitted": False,
                "reason": (emission or {}).get(
                    "reason", "no batch has been emitted yet")}
    first = batches[0]
    return {
        "emitted": True,
        "batch_id": first.get("batch_id"),
        "emitted_at_utc": first.get("emitted_at_utc"),
        "emitted_market_timestamp": first.get("emitted_market_timestamp"),
        "entry_session_date": first.get("entry_session_date"),
        "outcome_window_start_utc": first.get("outcome_window_start_utc"),
        "prediction_count": first.get("n_appended"),
        "challengers": first.get("challengers"),
        "asset_classes": first.get("asset_classes"),
        "horizons": first.get("horizons"),
        "earliest_expected_maturity": first.get("earliest_expected_maturity"),
        "latest_expected_maturity": first.get("latest_expected_maturity"),
        "skipped_challengers": first.get("skipped_challengers"),
        "state": first.get("state"),
        "n_batches_total": len(batches),
        "latest_batch_id": batches[-1].get("batch_id"),
    }


def _lane_line(lane: dict) -> str:
    js = lane.get("judgeable_sample") or {}
    return ("%s - %s of %s sessions, %s still required; %d hypotheses frozen "
            "before the confirming sessions exist; $%.2f spent"
            % (js.get("state"), js.get("usable_sessions_now"),
               js.get("sessions_required"), js.get("sessions_still_required"),
               lane.get("n_predeclared", 0),
               lane.get("money_spent_usd", 0.0)))


def _analyst_line(lane: dict) -> str:
    js = lane.get("judgeable_sample") or {}
    led = lane.get("ledger") or {}
    return ("%s - %s of %s observed revisions across %s snapshot dates "
            "(%s days); ~%s months remaining; challenger frozen in advance; "
            "never backfilled"
            % (js.get("state"), js.get("revisions_observed"),
               js.get("revisions_required"), led.get("n_snapshot_dates"),
               led.get("span_days"), js.get("approx_months_remaining")))


def _judge_result(run: dict) -> str:
    if run["n_newly_scored"]:
        return "SCORED %d" % run["n_newly_scored"]
    if run["n_still_pending"]:
        return "NOTHING_MATURED_YET (%d pending)" % run["n_still_pending"]
    return "NO_PREDICTIONS_TO_SCORE"


def _pit_result(campaign_id: str) -> str:
    rows = LG.predictions(campaign_id)
    if not rows:
        return "NO_ROWS"
    bad = [r for r in rows
           if r.get("point_in_time_status") != C.PIT_OK
           or not (str(r.get("emitted_at_utc"))
                   < str(r.get("outcome_window_start_utc")))]
    if bad:
        return "PIT_VIOLATION n=%d" % len(bad)
    return ("PIT_OK - every one of %d rows was emitted strictly before its "
            "outcome window opened" % len(rows))


def _registry_digest(reg: dict) -> dict:
    return {
        "frozen_at": reg.get("frozen_at"),
        "registry_hash": reg.get("registry_hash"),
        "n_r46_challengers": reg.get("n_r46_challengers"),
        "n_adopted": reg.get("n_adopted"),
        "n_active": reg.get("n_active"),
        "n_blocked": reg.get("n_blocked"),
        "retune_free": reg.get("retune_free"),
        "adoption_sources_unchanged": (reg.get("adoption") or {}).get(
            "all_sources_unchanged"),
        "adoption_finding": (reg.get("adoption") or {}).get("finding"),
        "challengers": [
            {"challenger_id": c["challenger_id"],
             "version": c["challenger_version"],
             "asset_class": c["asset_class"], "family": c["family"],
             "horizons": c["horizons"], "state": c["state"],
             "spec_hash": c["spec_hash"][:16],
             "feasibility": (c.get("feasibility") or {}).get("state"),
             "blocked_reason": c.get("blocked_reason")}
            for c in (reg.get("challengers") or ())],
    }


def _board_digest(board: dict) -> dict:
    return {
        "n_rows": board["n_rows"],
        "n_competing": board["n_competing"],
        "n_forward_pending": board["n_forward_pending"],
        "n_early": board["n_early"],
        "n_candidate": board["n_candidate"],
        "n_confirmed": board["n_confirmed"],
        "n_rejected": board["n_rejected"],
        "n_data_blocked": board["n_data_blocked"],
        "total_forward_predictions_emitted":
            board["total_forward_predictions_emitted"],
        "total_forward_predictions_matured":
            board["total_forward_predictions_matured"],
        "best_net_alpha_bps": board["best_net_alpha_bps"],
        "top": [{"rank": r["rank"], "challenger_id": r["challenger_id"],
                 "horizon": r.get("horizon"), "state": r["state"],
                 "raw_matured": r.get("raw_matured"),
                 "effective_independent": r.get("effective_independent"),
                 "net_alpha_bps": r.get("net_alpha_bps"),
                 "origin": r.get("origin")}
                for r in board["rows"][:20]],
    }


def _the_finding(registry: dict) -> str:
    adopted = (registry.get("adoption") or {}).get("n_adopted", 0)
    return (
        "Five releases each froze a prospective shadow registry. Together "
        "they hold %d frozen shadows and ZERO forward observations - the "
        "clock was started five times and never ticked, because two of the "
        "streams cannot accrue from this location and nothing ever called "
        "the other three's capture owner again. R46 does not add a sixth "
        "registry: it adopts all %d by reference, keeps their bytes and "
        "their owners untouched, and puts them on the same board as its own "
        "cohort so that the zero is visible in one place instead of spread "
        "across five campaign roots." % (adopted, adopted))


def _terminal_state(emission, board, chain, idempotency) -> str:
    """The state of the TOURNAMENT, not of this particular run.

    Deliberately stable across re-runs: once predictions are on the record the
    tournament is LIVE, whether or not today's call happened to append a new
    batch. A run that correctly appends nothing because the batch already
    exists has not made the tournament less live, and a terminal state that
    flickered between LIVE and EMITTED depending on run order would be a
    reporting artefact rather than a fact.
    """
    if not chain["all_intact"]:
        return "R46_FORWARD_INFRASTRUCTURE_INCOMPLETE"
    if idempotency and idempotency.get("verdict") != "IDEMPOTENT":
        return "R46_FORWARD_INFRASTRUCTURE_INCOMPLETE"
    if board["total_forward_predictions_matured"] > 0:
        return "R46_FORWARD_EVIDENCE_ALREADY_MATURING"
    if board["total_forward_predictions_emitted"] > 0:
        return "R46_PROSPECTIVE_ALPHA_TOURNAMENT_LIVE"
    if emission and emission.get("state") == "REFUSED_NOT_TRUE_FORWARD":
        return "R46_NO_VALID_FORWARD_WINDOW_TODAY"
    return "R46_TOURNAMENT_READY_NEXT_FORWARD_WINDOW"
