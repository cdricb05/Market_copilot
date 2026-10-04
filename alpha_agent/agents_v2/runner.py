r"""alpha_agent.agents_v2.runner - THE local campaign execution path.

RESEARCH ONLY. PAPER ONLY. NO ORDERS, NO FILLS, NO PROMOTION, NO ADOPTION.

What this is for
----------------
R56 ran its campaign by having a Claude agent narrate every deterministic step:
load the panel, build the mask, run the book, read three layers, compute an
attack, report a number. None of that needs a language model. It needs a
process that does the same arithmetic the same way every time.

So the division of labour is now explicit:

    the twelve agents DECIDE          agenda, hypotheses, parameters, gates,
                                      what to run, what a nontrivial result
                                      means, whether a survivor survives, and
                                      whether anything is published
    this module EXECUTES              universes, features, books, D/V/L
                                      slicing, cost arithmetic, placebos,
                                      doubled cost, subperiods, duplicate
                                      identity, gate evaluation, artifacts

The director freezes a CAMPAIGN SPEC. This module executes it and returns a
compact machine-readable summary per experiment - the thing an agent actually
has to think about - instead of a transcript of the thinking it did not have
to do.

What it is NOT
--------------
It is not a second registry, a second gate, a second queue or a second forward
clock. Every durable fact it writes goes through ``AgentPipeline``, which
writes the ONE research memory. Every verdict it reports comes from
``alpha_agent.r59.engines``. It cannot submit a candidate the pipeline would
refuse, and it has no verb that could create an order, a fill, a proposal or an
adoption.

Sequencing
----------
Layers are measured ONE AT A TIME and in order, through
``AgentPipeline.reveal_stage``. When a layer does not earn the next one the
experiment HALTS: the run stops there, the experiment is settled, and the
lockbox is never computed at all. That is the R56 defect this path exists to
make structurally impossible.

Idempotence
-----------
An experiment that has already been measured is not measured again. The memory
is the authority on what happened; the artifact is a rendering of it.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Callable, Optional

import numpy as np

from .. import r59
from ..r57 import engine as K
from ..r59 import engines as E
from ..r59 import native
from . import AGENT_SYSTEM_VERSION, SAFETY
from . import books as B
from . import campaign_clock as CK
from . import provenance as PV
from .pipeline import (EV_CANDIDATE, EV_FEATURES, EV_STAGE, AgentPipeline,
                       PipelineRefusal)

CALCULATION_OWNER = "alpha_agent.agents_v2.runner"
RUNNER_VERSION = "R57_LOCAL_CAMPAIGN_RUNNER_V1"

#: What a run can end as. Every one of these is a RECORDED outcome, never a
#: silent skip.
RUN_MEASURED = "MEASURED"          # all three layers revealed, candidate filed
RUN_HALTED = "HALTED"              # a layer did not earn the next one
RUN_ALREADY = "ALREADY_MEASURED"   # idempotent replay
RUN_REFUSED = "REFUSED"            # the pipeline refused the handoff
RUN_FAILED = "FAILED"              # the executor raised

#: Turnover is reported under different names by different books; ONE reader.
_TURNOVER_KEYS = ("mean_oneway_turnover_per_period",
                  "mean_oneway_turnover_per_day", "turnover")


class CampaignRefusal(RuntimeError):
    """The campaign spec itself is not executable."""


# --------------------------------------------------------------------------- #
# The frozen campaign spec
# --------------------------------------------------------------------------- #
REQUIRED_SPEC_KEYS = ("campaign_id", "executor_module", "experiments")
REQUIRED_EXPERIMENT_KEYS = ("experiment_id", "executor", "book")


def load_campaign_spec(path) -> dict:
    spec = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    missing = [k for k in REQUIRED_SPEC_KEYS if not spec.get(k)]
    if missing:
        raise CampaignRefusal("campaign spec is missing: %s"
                              % ", ".join(missing))
    for row in spec["experiments"]:
        bad = [k for k in REQUIRED_EXPERIMENT_KEYS if not row.get(k)]
        if bad:
            raise CampaignRefusal("experiment %s is missing: %s"
                                  % (row.get("experiment_id"), ", ".join(bad)))
        if row["book"] not in B.BOOKS:
            raise CampaignRefusal("experiment %s names unknown book %s"
                                  % (row["experiment_id"], row["book"]))
    return spec


def load_executors(path) -> dict:
    """Import the campaign's executor module BY PATH and take its registry.

    A campaign's hypotheses are its own research content and live with the
    campaign, exactly as R56's did. The runner is generic; what a hypothesis
    computes is not.
    """
    p = Path(path)
    if not p.is_absolute():
        p = Path(__file__).resolve().parents[2] / p
    if not p.exists():
        raise CampaignRefusal("executor module not found: %s" % p)
    if (p.parent / "__init__.py").exists():
        # A campaign laid out as a package is imported AS a package, so its
        # modules may import each other normally instead of each re-loading
        # the others by path and multiplying the substrate in memory.
        root = str(p.parent.parent)
        if root not in sys.path:
            sys.path.insert(0, root)
        mod = importlib.import_module("%s.%s" % (p.parent.name, p.stem))
        reg = getattr(mod, "EXECUTORS", None)
        if not isinstance(reg, dict) or not reg:
            raise CampaignRefusal("%s declares no EXECUTORS registry" % p)
        return reg
    name = "agents_v2_executors_%s" % r59.short_hash(str(p), 8)
    mod = sys.modules.get(name)
    if mod is None:
        spec = importlib.util.spec_from_file_location(name, p)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[name] = mod
        spec.loader.exec_module(mod)
    reg = getattr(mod, "EXECUTORS", None)
    if not isinstance(reg, dict) or not reg:
        raise CampaignRefusal(
            "%s declares no EXECUTORS registry; an executor is a callable "
            "returning a plan dict" % p)
    return reg


# --------------------------------------------------------------------------- #
# Reading a book result
# --------------------------------------------------------------------------- #
def turnover_of(stats: dict) -> Optional[float]:
    for k in _TURNOVER_KEYS:
        v = stats.get(k)
        if v is not None:
            return float(v)
    return None


def effective_cost_inputs_of(stats: dict, *,
                             horizon_sessions: Optional[int] = None) -> tuple:
    """``(effective_cost_per_side, additional_ann_cost_drag)`` a book's layer
    statistics imply, or ``(None, None)`` for a book that does not report
    them (R92).

    The event book annualises ``ann_rebalance_cost_drag`` as
    mean(2 x |w| @ cost) x observations/yr and ``ann_oneway_turnover`` as
    mean(|w|) x observations/yr, so their ratio over 2 is EXACTLY the
    notional-weighted per-side rate the book paid; ``ann_roll_cost_drag`` is
    its interior roll charge. Without these the skeptic's R61 cost budget
    fails closed (COST_BUDGET_NOT_EVALUABLE) on a per-market cost vector.

    Post-R99 PF1: the dated-contract book (``run_futures_book``) reports
    ``mean_oneway_turnover_per_period`` but no ``ann_oneway_turnover``. It
    annualises ``ann_rebalance_cost_drag`` by ``252 / horizon`` (NOT by the
    cadence), so the per-period turnover is annualised on that SAME basis and
    the ratio is again exactly the per-side rate. The horizon is not in the
    layer stats, so it must be supplied by the caller from the frozen,
    executed plan; without it the rate stays None and the budget fails closed.
    """
    rc = stats.get("ann_rebalance_cost_drag")
    to = stats.get("ann_oneway_turnover")
    if (to is None and stats.get("mean_oneway_turnover_per_period") is not None
            and horizon_sessions is not None and int(horizon_sessions) > 0):
        to = (float(stats["mean_oneway_turnover_per_period"])
              * PV.SESSIONS_PER_YEAR / float(int(horizon_sessions)))
    roll = stats.get("ann_roll_cost_drag")
    rate = None
    if rc is not None and to is not None and float(to) > 0.0:
        rate = float(rc) / (2.0 * float(to))
    return rate, (None if roll is None else float(roll))


def _clean(obj):
    """JSON-safe, and float32/int64 free."""
    if isinstance(obj, dict):
        return {str(k): _clean(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_clean(v) for v in obj]
    if isinstance(obj, np.generic):
        return obj.item()
    if isinstance(obj, np.ndarray):
        return [_clean(v) for v in obj.tolist()]
    return obj


# --------------------------------------------------------------------------- #
# The deterministic attacks
# --------------------------------------------------------------------------- #
def plan_fn(plan: dict) -> Callable:
    """A FRESH scoring callable for one run.

    A construction that carries state between decisions - a partial rebalance
    that needs last decision's weights - must not carry it between RUNS, or
    the validation layer would start from the discovery run's book and a
    placebo would start from the candidate's. ``make_fn`` is therefore the
    contract for any stateful construction; ``score_fn`` remains fine for a
    pure function of ``(panel, t)``.
    """
    make = plan.get("make_fn")
    if callable(make):
        return make()
    return plan["score_fn"]


def _stage_runner(plan: dict, *, stage: str, cost_mult: float = 1.0,
                  score_fn: Optional[Callable] = None) -> dict:
    """One measured layer under ``plan``, optionally at a different cost or
    with a different score function (that is all a placebo is)."""
    kw = dict(plan.get("book_kwargs") or {})
    kw.pop("cost_mult", None)
    return B.run_stage(book=plan["book"], stage=stage, panel=plan["panel"],
                       score_fn=score_fn if score_fn is not None
                       else plan_fn(plan),
                       cost_mult=float(cost_mult), **kw)


def adversarial_pack(plan: dict, lockbox: dict, *, margins: dict,
                     placebo_seeds=(101, 202, 303)) -> dict:
    """Every attack the skeptic needs that a MACHINE can settle.

    Reported in the skeptic's own report shape (``passed`` / ``measured`` /
    ``evidence``) so the skeptic grades measured numbers rather than assertions
    - but the VERDICT is still the skeptic's, and a failed attack here is not
    silently turned into a kill.
    """
    out: dict = {}

    # placebo_clean - COST-NEUTRAL (R57 A4).
    def _placebo(seed, mult):
        fn = B.permuted_score_fn(plan_fn(plan), seed=seed)
        return _stage_runner(plan, stage="L", cost_mult=mult,
                             score_fn=fn)["stats"]

    pl = B.placebo_cost_neutral(candidate_stats=lockbox, run_placebo=_placebo,
                                seeds=placebo_seeds)
    verdict = E.placebo_verdict(lockbox, pl["strongest"], margins=margins)
    out["placebo_clean"] = {
        "passed": bool(verdict["placebo_clean"]),
        "measured": verdict["measured"],
        "evidence": ("cost-neutral placebo, %d permutations, strongest seed "
                     "%s; candidate drag %.6g vs placebo drag %.6g"
                     % (len(pl["placebo_runs"]), pl["strongest_seed"],
                        verdict["candidate_cost_drag"],
                        verdict["placebo_cost_drag"])),
        "detail": _clean({"verdict": verdict,
                          "runs": [{k: v for k, v in r.items() if k != "stats"}
                                   for r in pl["placebo_runs"]]})}

    # cost_robust - the lockbox at TWICE the pre-registered cost.
    dbl = _stage_runner(plan, stage="L", cost_mult=2.0)["stats"]
    metrics = [k for k in r59.GATE_MATERIALITY_FLOORS if k in lockbox]
    key = metrics[0] if metrics else None
    dbl_v = E._as_float(dbl.get(key)) if key else None
    out["cost_robust"] = {
        "passed": bool(dbl_v is not None and dbl_v > 0),
        "measured": dbl_v,
        "evidence": "lockbox re-run at 2x the pre-registered cost model",
        "detail": _clean({"doubled_cost_lockbox": dbl})}

    # subperiod_stable - both halves of the lockbox.
    halves = lockbox.get("halves_ann_net_excess") or lockbox.get("halves")
    out["subperiod_stable"] = {
        "passed": bool(halves and all(h is not None and h > 0
                                      for h in halves)),
        "measured": (min(h for h in halves if h is not None)
                     if halves else None),
        "evidence": "both halves of the lockbox layer, measured by the book",
        "detail": _clean({"halves": halves})}
    return out


# --------------------------------------------------------------------------- #
# One experiment
# --------------------------------------------------------------------------- #
#: Books whose holding period is ``horizon`` and whose decision grid is
#: ``cadence`` (the event book's window is checked by R91; the tranche book
#: holds ``hold`` sessions and reads neither).
_HORIZON_BOOKS = (B.BOOK_FUTURES, B.BOOK_EQUITY_TOPN)
_REPO_ROOT = Path(__file__).resolve().parents[2]


def _campaign_path(p) -> Path:
    q = Path(p)
    return q if q.is_absolute() else _REPO_ROOT / q


def campaign_provenance(spec: dict) -> dict:
    """The post-R99 provenance declarations a campaign spec may carry.

    ``admission_rulings``     ordered director-ruling files; the LAST ruling
                              naming a cell is authoritative (declaration order,
                              never file mtimes). Absent -> no admission door.
    ``require_frozen_source`` every pre-registration must carry
                              ``parameters.frozen_source``.
    ``frozen_source_store``   the content-addressed blob store it is verified
                              against.
    """
    rulings = spec.get("admission_rulings")
    store = spec.get("frozen_source_store")
    contract = spec.get("provenance_contract")
    if contract is not None and contract != PV.PROVENANCE_CONTRACT_FULL:
        raise CampaignRefusal("unknown provenance_contract %r" % (contract,))
    full = contract == PV.PROVENANCE_CONTRACT_FULL
    if full and (rulings is None or not store):
        # R100 repair: under the full contract neither door may be absent.
        raise CampaignRefusal(
            "%s requires admission_rulings and frozen_source_store"
            % PV.PROVENANCE_CONTRACT_FULL)
    return {"admission_rulings": (None if rulings is None
                                  else [_campaign_path(f) for f in rulings]),
            "require_frozen_source": full or bool(
                spec.get("require_frozen_source", False)),
            "frozen_source_store": (None if not store
                                    else _campaign_path(store)),
            "provenance_contract": contract}


def preregister_frozen(pipe: AgentPipeline, *, director: str, row: dict,
                       payload: dict, executors: dict, store_dir,
                       source_paths=()) -> dict:
    """R100 repair: the ONE pre-registration path of a FULL_RECIPE_V1 campaign.

    In a single call, before any return is read, it:

    1. snapshots the canonical measurement code plus the campaign's own source
       into the write-once content store (automatic, never optional);
    2. builds the executor's plan WITHOUT running a book or revealing a layer;
    3. freezes the eight-part recipe that plan will execute; and
    4. pre-registers it through the pipeline, which re-checks the recipe and
       the snapshot.

    The runner later rebuilds the plan and refuses unless the executed recipe
    equals this frozen one.
    """
    fn = executors.get(row.get("executor"))
    if fn is None:
        raise CampaignRefusal("UNKNOWN_EXECUTOR: %s" % row.get("executor"))
    paths = ([_REPO_ROOT / p for p in PV.CANONICAL_SOURCE]
             + [_campaign_path(p) for p in source_paths])
    manifest = PV.snapshot(paths, repo_root=_REPO_ROOT, store_dir=store_dir)
    plan = fn(dict(row))
    plan.setdefault("book", row["book"])
    if plan["book"] != row["book"]:
        raise CampaignRefusal("executor %s built a %s book but the row says %s"
                              % (row["executor"], plan["book"], row["book"]))
    body = json.loads(json.dumps(payload))
    params = dict(body.get("parameters") or {})
    params[PV.FROZEN_SOURCE_KEY] = manifest
    if plan["book"] in _HORIZON_BOOKS:
        eh, ec = PV.executed_horizon_cadence(
            plan, default_horizon=r59.HORIZON, default_cadence=r59.CADENCE)
        params.setdefault("cadence_sessions", ec)
        PV.check_horizon_cadence({**body, "parameters": params}, plan,
                                 default_horizon=r59.HORIZON,
                                 default_cadence=r59.CADENCE)
    feats = pipe._latest(EV_FEATURES, str(body.get("feature_set_id"))) or {}
    view = {**body, "parameters": params, "dataset_id": feats.get("dataset_id")}
    recipe = PV.executed_recipe(plan, row, view, manifest,
                                default_horizon=r59.HORIZON,
                                default_cadence=r59.CADENCE,
                                repo_root=_REPO_ROOT)
    recipe["recipe_sha256"] = PV.recipe_sha256(recipe)
    params[PV.RECIPE_KEY] = recipe
    body["parameters"] = params
    body["provenance_contract"] = PV.PROVENANCE_CONTRACT_FULL
    body["frozen_source_store"] = str(store_dir)
    res = pipe.perform(director, "preregister", body)
    return {**res, "recipe_sha256": recipe["recipe_sha256"],
            "frozen_source_manifest_sha256": manifest["manifest_sha256"],
            "frozen_source_files": len(manifest["files"])}


def run_experiment(pipe: AgentPipeline, row: dict, executors: dict, *,
                   spec_hash: str, campaign_id: str,
                   artifact_dir: Optional[Path] = None,
                   provenance: Optional[dict] = None) -> dict:
    """Measure ONE pre-registered experiment, layer by layer, and file it.

    Returns a COMPACT summary. Nothing here decides anything: the advance rule
    is ``engines.stage_advance``, the verdict is ``engines.gate`` reached
    through ``skeptic_review``, and every write goes through the pipeline.

    Post-R99, three doors close BEFORE the first layer is read
    (``agents_v2.provenance``): the frozen source must still hash to its
    pre-registration, the executed horizon/cadence must equal the frozen ones,
    and the latest director ruling must admit the cell (G7 PASS).
    """
    prov = provenance or {}
    eid = row["experiment_id"]
    exp = pipe.mem.get(eid)
    if exp is None:
        return {"experiment_id": eid, "state": RUN_REFUSED,
                "reason": "UNKNOWN_EXPERIMENT"}
    frozen = exp.get("spec") or {}

    # Idempotence: the memory is the authority on what already happened.
    if pipe._latest(EV_CANDIDATE, eid) is not None or exp.get("outcome"):
        stages = [e["detail"] for e in pipe._events(EV_STAGE, eid)]
        return {"experiment_id": eid, "state": RUN_ALREADY,
                "outcome": exp.get("outcome"),
                "stages_revealed": [s["stage"] for s in stages],
                "layers": {s["stage"]: s["stats"] for s in stages}}

    fn = executors.get(row["executor"])
    if fn is None:
        return {"experiment_id": eid, "state": RUN_REFUSED,
                "reason": "UNKNOWN_EXECUTOR: %s" % row["executor"]}

    agent = frozen.get("owning_agent")
    layers: dict = {}
    try:
        plan = fn(row)
        plan.setdefault("book", row["book"])
        if plan["book"] != row["book"]:
            raise CampaignRefusal(
                "executor %s built a %s book but the frozen spec says %s"
                % (row["executor"], plan["book"], row["book"]))
        # R91 - an event window is a PRE-REGISTERED parameter. A plan whose
        # holding window or entry rule differs from the frozen spec is
        # refused before any layer is read.
        if plan["book"] == B.BOOK_EVENT:
            problems = B.EVB.check_frozen_parameters(frozen, plan)
            if problems:
                raise CampaignRefusal("EVENT_WINDOW_NOT_PREREGISTERED: "
                                      + "; ".join(problems))
            authoritative = None
        elif plan["book"] in _HORIZON_BOOKS:
            # ONE horizon and ONE cadence: what the book executes must be what
            # the pre-registration froze (R99 PF2: h=21 shown, h=1 executed).
            authoritative = PV.check_horizon_cadence(
                frozen, plan, default_horizon=r59.HORIZON,
                default_cadence=r59.CADENCE)
        else:
            authoritative = None
        manifest = PV.require_frozen_source(
            frozen, required=bool(prov.get("require_frozen_source")),
            repo_root=_REPO_ROOT, store_dir=prov.get("frozen_source_store"))
        # R100 repair: under FULL_RECIPE_V1 the eight-part recipe this plan
        # WILL execute must equal the frozen one, before any layer is read.
        recipe = None
        if prov.get("provenance_contract") == PV.PROVENANCE_CONTRACT_FULL:
            recipe = PV.require_recipe(frozen, PV.executed_recipe(
                plan, row, frozen, manifest, default_horizon=r59.HORIZON,
                default_cadence=r59.CADENCE, repo_root=_REPO_ROOT))
        # Read the admission LAST, immediately before the first layer: a
        # ruling written while the plan was being built still binds.
        admission = PV.require_admission(row.get("cell_id"),
                                         prov.get("admission_rulings"))
        bound = {"cell_id": row.get("cell_id"),
                 "provenance_contract": prov.get("provenance_contract"),
                 "recipe_sha256": (recipe or {}).get("recipe_sha256"),
                 "frozen_source_manifest_sha256": (manifest or {}).get(
                     "manifest_sha256"),
                 "admission": admission}
        for stage in r59.STAGES:
            measured = _stage_runner(plan, stage=stage)
            stats = _clean(measured["stats"])
            layers[stage] = stats
            rv = pipe.perform(agent, "reveal_stage", dict(
                experiment_id=eid, spec_hash=spec_hash, stage=stage,
                stats=stats, evaluator=CALCULATION_OWNER))
            if not rv["advance"] and stage != "L":
                return {"experiment_id": eid, "state": RUN_HALTED, **bound,
                        "halted_at": stage,
                        "halt_reasons": rv["halt_reasons"],
                        "layers": layers,
                        "lockbox_computed": False,
                        "outcome": r59.HO_NO_ALPHA_EVIDENCE}
    except (PipelineRefusal, PV.ProvenanceRefusal) as exc:
        return {"experiment_id": eid, "state": RUN_REFUSED,
                "reason": exc.code, "detail": str(exc)[:300],
                "layers": layers}
    except Exception as exc:                                   # noqa: BLE001
        return {"experiment_id": eid, "state": RUN_FAILED,
                "reason": type(exc).__name__, "detail": str(exc)[:300],
                "layers": layers}

    lockbox = layers["L"]
    margins = (pipe.contracts.gate_schema()["placebo"]["margin"])
    try:
        attacks = adversarial_pack(plan, lockbox, margins=margins)
    except (E.StageRefusal, B.BookRefusal) as exc:
        attacks = {"placebo_clean": {"passed": False, "measured": None,
                                     "evidence": "REFUSED: %s" % exc}}

    turn = turnover_of(lockbox)
    # R92: the measured per-side rate and roll drag travel with the candidate
    # so the skeptic's cost budget is evaluable on a per-market cost vector.
    eff_rate, roll_drag = effective_cost_inputs_of(
        lockbox, horizon_sessions=(authoritative or {}).get("horizon_sessions"))
    try:
        pipe.perform(agent, "submit_candidate", dict(
            experiment_id=eid, spec_hash=spec_hash,
            signal_sign=int(plan.get("signal_sign",
                                     frozen.get("expected_sign", 1))),
            evidence_kind="HISTORICAL",
            cost_model=frozen.get("cost_model"),
            turnover=float(turn if turn is not None else 0.0),
            layers=layers, evaluator=CALCULATION_OWNER,
            effective_cost_per_side=eff_rate,
            additional_ann_cost_drag=roll_drag))
    except PipelineRefusal as exc:
        return {"experiment_id": eid, "state": RUN_REFUSED,
                "reason": exc.code, "detail": str(exc)[:300],
                "layers": layers}

    summary = {
        "experiment_id": eid, "state": RUN_MEASURED,
        "campaign_id": campaign_id, "runner": RUNNER_VERSION,
        "owning_agent": agent, "book": row["book"],
        "executor": row["executor"],
        "asset_class": exp.get("asset_class"),
        "economic_family": exp.get("economic_family"),
        "horizon_sessions": exp.get("horizon_sessions"),
        # the single executed pair, equal to the frozen one by construction
        "executed_horizon_cadence": authoritative,
        **bound,
        "effective_cost_per_side": eff_rate,
        "layers": layers, "turnover": turn,
        "machine_checks": {"signal_sign": int(plan.get(
            "signal_sign", frozen.get("expected_sign", 1)))},
        "adversarial": attacks,
        "lockbox_computed": True,
    }
    if artifact_dir is not None:
        d = Path(artifact_dir)
        d.mkdir(parents=True, exist_ok=True)
        body = dict(summary)
        body["agent_system_version"] = AGENT_SYSTEM_VERSION
        body["safety"] = dict(SAFETY)
        (d / ("%s.json" % eid)).write_text(
            json.dumps(_clean(body), indent=1, default=str), encoding="utf-8")
    return summary


# --------------------------------------------------------------------------- #
# The batch
# --------------------------------------------------------------------------- #
def run_campaign(pipe: AgentPipeline, spec: dict, *,
                 only: Optional[list] = None,
                 artifact_dir: Optional[Path] = None,
                 work_dir: Optional[Path] = None) -> dict:
    """Execute every experiment in a frozen campaign spec.

    One failing experiment never stops the batch: research that cannot be
    measured is a RECORDED state, not an exception that loses the other
    eleven results.

    ``work_dir`` (the campaign folder) receives a runner SPAN per experiment
    in its work log (``agents_v2.campaign_clock``): measuring is real working
    time, and the only time a campaign clock may count besides evidenced work.
    """
    executors = load_executors(spec["executor_module"])
    prov = campaign_provenance(spec)
    rows = [r for r in spec["experiments"]
            if only is None or r["experiment_id"] in set(only)]
    out = []
    for row in rows:
        exp = pipe.mem.get(row["experiment_id"])
        sh = (r59.stable_hash(exp.get("spec") or {}) if exp else "")
        with CK.span(work_dir, "measure %s" % row["experiment_id"],
                     actor=CALCULATION_OWNER):
            out.append(run_experiment(pipe, row, executors, spec_hash=sh,
                                      campaign_id=spec["campaign_id"],
                                      artifact_dir=artifact_dir,
                                      provenance=prov))
    by_state: dict = {}
    for r in out:
        by_state[r["state"]] = by_state.get(r["state"], 0) + 1
    # A cell measured under an admission that was LATER withdrawn stays
    # visible as a governance exception (R99 C04); it is never a clean count.
    exceptions = []
    if prov["admission_rulings"] is not None:
        cell_of = {r["experiment_id"]: r.get("cell_id") for r in rows}
        measured = [cell_of[r["experiment_id"]] for r in out
                    if r.get("layers") and cell_of.get(r["experiment_id"])]
        exceptions = PV.admission_exceptions(measured,
                                             prov["admission_rulings"])
    validity = CK.valid_measurements(
        out, exceptions, cell_of={r["experiment_id"]: r.get("cell_id")
                                  for r in rows})
    return {"campaign_id": spec["campaign_id"], "runner": RUNNER_VERSION,
            "provenance_contract": prov.get("provenance_contract"),
            "governance_exceptions": exceptions,
            "valid_measurements": validity,
            "agent_system_version": AGENT_SYSTEM_VERSION,
            "experiments": len(out), "by_state": by_state,
            "lockboxes_computed": sum(1 for r in out
                                      if r.get("lockbox_computed")),
            "halted_before_lockbox": sum(1 for r in out
                                         if r["state"] == RUN_HALTED),
            "results": out, "safety": dict(SAFETY)}
