"""alpha_agent.r59.handlers - R59 lanes on the CANONICAL research queue.

There is no R59 queue. Mandates are seeded into
:class:`alpha_agent.autonomous_research.ResearchQueue` - the Stage-8 durable,
crash-safe, never-idle queue that already owns atomic claim/settle, per-lane
backoff, dead-worker recovery and idempotent enqueue - and executed by the
handlers below.

Routing follows the pattern Stage 10 established for the identity lanes: an
R59 handler claims ONLY jobs whose lane starts with ``r59.``; every other job
in the database keeps whatever handler it already had. That is what lets the
R59 engine share one queue with the live collection lanes without touching a
single existing job.

Outcome mapping is deliberate:

* a hypothesis that runs and FAILS its gate is COMPLETED, not failed. It
  produced evidence; the evidence was negative. Recording it as a failure
  would make the queue retry a settled scientific result.
* a missing substrate is BLOCKED_SPECIFIC - that lane waits, every other lane
  continues.
* a handler that raises becomes a bounded RETRYABLE inside the queue runner
  and never stops the cycle.

Handlers write research memory and R59 artifacts. They cannot write an
operational store, create an order or a fill, promote a model, activate a
sleeve or approve a proposal.
"""
from __future__ import annotations

from typing import Optional

from .. import autonomous_research as AR
from .. import r59
from . import engines as E
from . import governor as GOV
from . import memory as M

CALCULATION_OWNER = "alpha_agent.r59.handlers"


# --------------------------------------------------------------------------- #
# Seeding
# --------------------------------------------------------------------------- #
_KIND_LANE = {
    GOV.MANDATE_ECONOMIC: r59.LANE_ECONOMIC,
    GOV.MANDATE_MATHEMATICAL: r59.LANE_MATHEMATICAL,
    GOV.MANDATE_CROSS_ASSET: r59.LANE_CROSS_ASSET,
    GOV.MANDATE_DATA: r59.LANE_DATA_OPPORTUNITY,
    GOV.MANDATE_NATIVE: r59.LANE_NATIVE,
}

_NATIVE_NAMES = frozenset(
    ("CALENDAR_TERM_STRUCTURE", "RATES_CURVE_RV", "INTER_COMMODITY_RV",
     "AGRICULTURAL_SEASONALITY", "ROLL_STATE"))


def lane_for(mandate: dict) -> str:
    base = _KIND_LANE.get(mandate["kind"], r59.LANE_ECONOMIC)
    return "%s.%s" % (base, mandate["asset_class"].lower())


def seed_mandates(queue: AR.ResearchQueue, mandates: list) -> dict:
    """Enqueue mandates onto the canonical queue. Idempotent by mandate id.

    Priority is the mandate's BATCH RANK, not its raw expected information
    value. The queue drains strictly by priority, so seeding raw EIV let the
    highest-scoring kind - machine discovery scores highest in every scope -
    take every claim slot and the economic and cross-asset lanes never ran,
    even though the governor had already produced a diverse batch. Ranking here
    is what makes the governor's fairness decision survive into execution.
    """
    added, existing = [], []
    n = len(mandates)
    for rank, m in enumerate(mandates):
        cat = (AR.CAT_HYPOTHESIS_GENERATION
               if m["kind"] == GOV.MANDATE_MATHEMATICAL
               else AR.CAT_DATA_VALIDATION if m["kind"] == GOV.MANDATE_DATA
               else AR.CAT_SIGNAL_COMBINATION
               if m["kind"] in (GOV.MANDATE_CROSS_ASSET, GOV.MANDATE_NATIVE)
               else AR.CAT_EXPERIMENT)
        before = queue.depth()
        job_id = queue.enqueue(
            cat, lane=lane_for(m), payload=m,
            priority=int(n - int(m.get("batch_rank", rank))),
            dedupe_key="r59:%s" % m["mandate_id"], origin="r59_governor")
        (added if queue.depth() > before else existing).append(job_id)
    return {"enqueued": len(added), "already_live": len(existing),
            "depth": queue.depth()}


# --------------------------------------------------------------------------- #
# Handlers
# --------------------------------------------------------------------------- #
def campaign_cells(mem: M.ResearchMemory, generation_method: str) -> int:
    """How many cells this campaign has already prosecuted under one method.

    A brand-new economic family has almost no FAMILY burden, so charging only
    that would let the first pre-registered family on a new substrate be graded
    as if it were the only test ever run. R57 and R58 both corrected across
    their campaign's families; this is the same denominator.
    """
    return sum(1 for h in mem.list_hypotheses(limit=99999)
               if h.get("generation_method") == generation_method
               and h.get("outcome") is not None)


def search_denominator(mem: M.ResearchMemory, *, family_key: str,
                       asset_class: str, machine_generated: bool,
                       campaign_method: Optional[str] = None,
                       within_family_tests: int = 1) -> dict:
    """How many tests this claim must be charged for.

    Two components, both real:

    * the ATTRIBUTED family's own prior burden - if a candidate re-expresses
      the liquidity factor, it is charged for every liquidity test already run;
    * for a machine candidate, the GENERATIVE search that produced it - the
      machine grammar in this scope emits candidates by the dozen, and a
      t-statistic picked out of that pile is not worth what an isolated
      pre-registered test would be.

    Charging only the family burden would let a generator run hundreds of
    candidates and present the best one as if it had been the only one tried.
    """
    b = mem.burden()
    by_family = b.get("by_family") or {}
    family = int(by_family.get(family_key, 0))
    generative = 0
    if machine_generated:
        for key, n in by_family.items():
            parts = key.split("|")
            if len(parts) >= 3 and parts[2] == asset_class and \
                    parts[0].startswith("MACHINE_REPRESENTATION"):
                generative += int(n)
    campaign = (campaign_cells(mem, campaign_method) if campaign_method
                else 0)
    # ``within_family_tests`` is the multiplicity the cell itself created - a
    # horizon sweep that examines four holding periods and reports the best has
    # taken four draws, and must be charged for four.
    total = family + generative + campaign + max(0, within_family_tests - 1)
    return {"family": family, "generative_search": generative,
            "campaign_cells": campaign,
            "within_family_tests": int(within_family_tests),
            "total": int(total)}


def _settle(mem: M.ResearchMemory, *, mandate: dict, spec: dict, title: str,
            result: dict, generation_method: str, origin: str,
            information_family: str, model_family: str,
            horizon: Optional[int] = None,
            within_family_tests: int = 1,
            campaign_method: Optional[str] = None) -> dict:
    """Register, gate and record ONE measured hypothesis."""
    ac = mandate["asset_class"]
    econ = mandate["family"]
    fam = M.family_key(economic_family=econ,
                       information_family=information_family,
                       asset_class=ac, model_family=model_family)
    novelty = mem.is_novel(family=fam, spec=spec)
    den = search_denominator(mem, family_key=fam, asset_class=ac,
                             machine_generated=(origin == "MACHINE_GENERATED"),
                             campaign_method=campaign_method,
                             within_family_tests=within_family_tests)
    burden = den["total"]

    hid = mem.register(
        title=title, release=r59.RELEASE, origin=origin,
        generation_method=generation_method,
        information_family=information_family, economic_family=econ,
        asset_class=ac, model_family=model_family,
        horizon_sessions=horizon or r59.HORIZON,
        input_data_identity=result.get("evaluator", ""), spec=spec)

    g = E.gate(result, prior_burden=burden, family_tests=1)
    mem.record_result(
        hid, outcome=g["outcome"], evidence_maturity="HISTORICAL",
        statistic={"lockbox_t": g["lockbox_t"],
                   "lockbox_p_one_sided": g["lockbox_p_one_sided"],
                   "burden_corrected_p": g["burden_corrected_p"],
                   "burden_denominator": g["burden_denominator"],
                   "lockbox_observations": g["lockbox_observations"]},
        economics={"lockbox_materiality": g["lockbox_materiality"],
                   "validation_materiality": g["validation_materiality"],
                   "layers": result.get("layers")},
        robustness={"checks": g["checks"]},
        reason_rejected=None if g["qualified"] else ", ".join(g["failed_gates"]),
        reopen_condition="NEW_ORTHOGONAL_INFORMATION")
    return {"hypothesis_id": hid, "gate": g, "was_novel": novelty["novel"],
            "prior_family_burden": burden, "search_denominator": den}


def freeze_qualified(mem: M.ResearchMemory, *, hypothesis_id: str,
                     adopt_forward=None) -> dict:
    """Freeze a qualifying HISTORICAL candidate as a PROSPECTIVE challenger.

    Two facts must both survive, so two rows exist:

    * the historical hypothesis keeps its QUALIFIED outcome and its lockbox
      statistics - that is what the backtest found;
    * a NEW challenger row records the inception instant and the specification
      hash, carries zero forward observations, and counts nothing toward search
      burden - that is what the estate will be judged on.

    Collapsing them into one row would either erase the historical result or
    let it be mistaken for forward evidence. Whatever the challenger goes on to
    earn belongs to the R46/R52 prospective runtime; R59 writes only the
    inception.
    """
    h = mem.get(hypothesis_id)
    if h is None:
        return {"state": "UNKNOWN_HYPOTHESIS", "hypothesis_id": hypothesis_id}
    if h.get("outcome") != r59.HO_QUALIFIED:
        return {"state": "NOT_QUALIFIED", "hypothesis_id": hypothesis_id}

    challenger_id = "R59_%s_%s" % (
        str(h.get("economic_family") or "FAMILY").upper(),
        r59.short_hash(h.get("spec"), 8).upper())
    spec = dict(h.get("spec") or {})
    spec["frozen_from"] = hypothesis_id
    frozen_id = mem.register(
        title="R59 prospective challenger %s" % challenger_id,
        release=r59.RELEASE, origin="R59_GOVERNOR",
        generation_method="PROSPECTIVE_FREEZE",
        information_family=h.get("information_family") or "PRICE_STATE",
        economic_family=challenger_id,
        asset_class=h.get("asset_class"),
        model_family=h.get("model_family") or "XS_LONG_SHORT",
        horizon_sessions=h.get("horizon_sessions"),
        input_data_identity=str((h.get("spec") or {}).get("substrate") or ""),
        counts_to_burden=False, spec=spec)
    existing = mem.get(frozen_id)
    if existing and existing.get("outcome") == r59.HO_FORWARD_FROZEN:
        # R62.1.1 - AN EXISTING FREEZE IS STILL OWED ITS FORWARD CLOCK.
        #
        # Before this release the function returned here, BEFORE the adoption
        # owner was called. So a freeze whose forward half never happened -
        # every freeze written before R61, and any freeze whose adoption failed
        # once - could never self-heal, however many times the persistent
        # research runtime ran again. Four ACTIVE R58 challengers sat exactly
        # there: adoptable, and unreachable by the only code that adopts.
        #
        # Re-offering it to the SAME governed owner creates no second
        # registration path and cannot resurrect anything: the owner reclassifies
        # the lifecycle from persisted history (a WITHDRAWN, INVALIDATED,
        # SUPERSEDED, MATURED or FAILED freeze is refused before any store is
        # touched), keys idempotency on the identity hash, and the canonical
        # registrar's first-write-wins rule returns the EXISTING registration
        # untouched. The clock it would open is today's, never the old
        # inception, so nothing can be backdated by re-running. It promotes no
        # model and allocates no capital.
        adoption = _register_forward_evidence(mem, frozen_id, adopt_forward)
        return {"state": "ALREADY_FROZEN", "challenger_id": challenger_id,
                "hypothesis_id": frozen_id,
                "forward_adoption": adoption,
                "forward_evidence_started": bool(adoption.get("adopted")),
                "forward_adoption_owner": "api.prospective_adoption",
                "forward_adoption_retried_for_existing_freeze": True}

    inception = r59.now_iso()
    mem.freeze_forward(frozen_id, challenger_id=challenger_id,
                       inception=inception,
                       record_hash=r59.stable_hash(spec))
    body = {
        "challenger_id": challenger_id,
        "frozen_from_hypothesis": hypothesis_id,
        "inception": inception,
        "forward_observations_at_freeze": 0,
        "specification": spec,
        "historical_evidence": {
            "note": "HISTORICAL ONLY - never forward evidence",
            "statistic": h.get("statistic"),
            "economics": h.get("economics"),
        },
        "scoring_owner": "alpha_agent.r46 / alpha_agent.r52.runtime",
        "auto_promotion": False,
        "operational_effect": "NONE",
    }
    path = r59.write_artifact("%s.json" % challenger_id, body,
                              subdir="challengers")
    mem.event("PROSPECTIVE_FREEZE", subject=challenger_id,
              detail={"from": hypothesis_id, "inception": inception})
    # R61 - a freeze and the start of its forward evidence are ONE governed
    # operation. Before R61 this function ended here: the freeze was durable and
    # NOTHING registered the challenger with any forward-evidence owner, so a
    # qualified candidate accrued zero observations and looked exactly like one
    # that was merely young. Five freezes reached the operator that way.
    #
    # The adoption owner is idempotent and fail-closed. When no canonical
    # forward owner exists for this challenger's class it records a durable,
    # RESUMABLE intent and says so - which is a named gap the estate can see and
    # act on, rather than an orphan nobody knows about. It promotes nothing,
    # writes no holding and creates no order.
    adoption = _register_forward_evidence(mem, frozen_id, adopt_forward)
    return {"state": "FROZEN", "challenger_id": challenger_id,
            "hypothesis_id": frozen_id, "inception": inception,
            "artifact": str(path),
            "forward_adoption": adoption,
            "forward_evidence_started": bool(adoption.get("adopted")),
            "forward_adoption_owner": "api.prospective_adoption"}


#: R61 - the state a freeze is in when no adopter was INJECTED. The research
#: package may not import the application layer (an R59 invariant), so the
#: governed prospective-adoption owner arrives the same way the revision reader
#: does: from the entrypoint that composes them. An un-wired worker still
#: freezes and still says, in one word, that the forward half did not happen -
#: which is the visibility whose absence let five orphans survive two releases.
FORWARD_ADOPTION_NOT_WIRED = "FORWARD_ADOPTION_OWNER_NOT_INJECTED"


def _register_forward_evidence(mem, frozen_id: str, adopt_forward) -> dict:
    """Hand the freeze to THE governed prospective-adoption owner (R61).

    ``adopt_forward`` is INJECTED - this module never imports the application
    layer. Delegation only: it decides no lifecycle, computes no identity and
    writes to no forward store. A failure here never destroys the freeze; the
    freeze is already durable, and the adoption owner's own intent record is
    what makes the missing half recoverable.
    """
    if adopt_forward is None:
        return {"adopted": False, "outcome": FORWARD_ADOPTION_NOT_WIRED,
                "detail": "no prospective-adoption owner was injected by the "
                          "entrypoint; the freeze is durable and its forward "
                          "registration has NOT started"}
    try:
        return adopt_forward(
            freeze_row=mem.get(frozen_id) or {},
            # The clock starts from what is legitimately observable NOW, never
            # from an inception that has already passed. No session between the
            # two is synthesised.
            observation_clock_starts=r59.now_iso()[:10])
    except Exception as exc:                             # noqa: BLE001
        return {"adopted": False, "outcome": "ADOPTION_OWNER_UNAVAILABLE",
                "detail": str(exc)[:200]}


def make_handlers(mem: Optional[M.ResearchMemory] = None,
                  queue: Optional[AR.ResearchQueue] = None,
                  adopt_forward=None) -> dict:
    """Build the R59 handler map for the canonical queue categories.

    R61 - ``adopt_forward`` is the INJECTED governed prospective-adoption owner
    (see :func:`_register_forward_evidence`). ``None`` is a legitimate
    configuration and is reported as such; it is never silently ignored.
    """
    mem = mem or M.open_memory()

    def _economic(job) -> tuple:
        m = job.payload or {}
        ac, family = m.get("asset_class"), (m.get("payload") or {}).get("family")
        mode = (m.get("payload") or {}).get("mode") or "TIME_SERIES"
        if ac == r59.AC_US_EQUITY:
            feature = E.EQUITY_FEATURE_MAP.get(family)
            if not feature:
                return AR.OUTCOME_BLOCKED_SPECIFIC, {
                    "reason": "no equity feature declared for family %s" % family,
                    "real_work": "r59_economic"}
            sign = -1.0 if family in ("LOW_RISK_ANOMALY",
                                      "IDIOSYNCRATIC_VOLATILITY",
                                      "LIQUIDITY_PREMIUM") else 1.0
            res = E.run_equity_hypothesis(feature=feature, sign=sign,
                                          label="%s|%s" % (ac, family))
            info, model = "PRICE_STATE", "RANK_TOPN"
        else:
            fm = E.ECONOMIC_FEATURE_MAP.get(family)
            if not fm:
                return AR.OUTCOME_BLOCKED_SPECIFIC, {
                    "reason": "no futures feature declared for family %s" % family,
                    "real_work": "r59_economic"}
            # The engine's own map declares how a family must be expressed
            # (a trend family is a time-series book, a carry family is a
            # cross-sectional one); the mandate's mode is the frontier's view
            # of the same fact and is recorded for provenance, not obeyed.
            feature, fmode = fm
            res = E.run_futures_hypothesis(
                asset_class=ac, feature=feature, mode=fmode,
                label="%s|%s" % (ac, family))
            if mode and mode != fmode:
                res["mandate_mode_note"] = (
                    "mandate proposed %s; the family is executed as %s"
                    % (mode, fmode))
            info, model = "PRICE_STATE", "VOL_TARGET_BOOK"
        if res.get("state") != "MEASURED":
            return AR.OUTCOME_BLOCKED_SPECIFIC, {
                "reason": "engine returned %s" % res.get("state"),
                "real_work": "r59_economic", "detail": res}

        spec = {"family": family, "asset_class": ac,
                "feature": res.get("feature"), "mode": res.get("mode"),
                "evaluator": res.get("evaluator")}
        s = _settle(mem, mandate=m, spec=spec,
                    title="R59 %s / %s" % (ac, family), result=res,
                    generation_method="GOVERNOR_ECONOMIC_MANDATE",
                    origin="R59_GOVERNOR", information_family=info,
                    model_family=model)
        return AR.OUTCOME_COMPLETED, {
            "real_work": "r59_economic", "asset_class": ac, "family": family,
            "candidate_id": s["hypothesis_id"],
            "disposition": s["gate"]["outcome"],
            "lockbox_t": s["gate"]["lockbox_t"],
            "failed_gates": s["gate"]["failed_gates"],
            "prior_family_burden": s["prior_family_burden"],
            "hypotheses_measured": 1}

    def _mathematical(job) -> tuple:
        """Generate machine hypotheses and measure every one of them.

        This is one job that produces MANY hypotheses; the generated batch is
        measured inline so a machine candidate can never be counted as
        'generated' without also being counted against the search burden.
        """
        m = job.payload or {}
        ac = m.get("asset_class")
        p = m.get("payload") or {}
        kind = p.get("machine_kind") or "AUTO"
        seed = int(p.get("seed") or 5900)
        k = int(p.get("batch") or GOV.MACHINE_BATCH)

        equity = (ac == r59.AC_US_EQUITY)
        cands = (E.generate_equity_machine_hypotheses(kind=kind, k=k, seed=seed)
                 if equity else
                 E.generate_machine_hypotheses(asset_class=ac, kind=kind,
                                               k=k, seed=seed))
        if not cands:
            return AR.OUTCOME_BLOCKED_SPECIFIC, {
                "reason": "generator produced no non-degenerate candidate for "
                          "%s/%s" % (ac, kind),
                "real_work": "r59_mathematical"}

        measured, qualified, duplicates = [], [], []
        for c in cands:
            label = "%s|%s|%s" % (ac, kind, c["feature_name"])
            res = (E.run_equity_machine_hypothesis(
                kind=kind, seed=seed, feature_name=c["feature_name"],
                label=label) if equity else
                E.run_machine_hypothesis(
                    asset_class=ac, kind=kind, seed=seed,
                    feature_name=c["feature_name"], label=label))
            if res.get("state") != "MEASURED":
                continue

            # IDENTITY IS THE REALISED SIGNAL. abs(x), tanh(x), x+x and
            # rankxs(x) are four formulas and ONE book; keyed on the formula
            # they were counted as four independent tests and one of them
            # cleared the gate four times.
            eq = c.get("base_equivalent") or {}
            econ = (eq.get("economic_family")
                    if eq.get("is_monotone_equivalent") else None)
            spec = {"signal_fingerprint": res.get("signal_fingerprint"),
                    "asset_class": ac,
                    "model_family": "RANK_TOPN" if equity else "XS_RANK_BOOK"}

            # A machine feature that merely re-expresses a base column IS that
            # column's economic family, and must inherit the burden that family
            # has already absorbed - otherwise a rediscovery of the liquidity
            # factor is charged nothing for the price families R57 retired.
            mandate = dict(m)
            if econ:
                mandate["family"] = econ

            fam = M.family_key(
                economic_family=mandate["family"],
                information_family="PRICE_STATE", asset_class=ac,
                model_family=spec["model_family"])
            novelty = mem.is_novel(family=fam, spec=spec)
            if not novelty["novel"]:
                duplicates.append({"feature": c["feature_name"],
                                   "expression": c["spec"],
                                   "same_book_as": novelty["hypothesis_id"],
                                   "signal_fingerprint":
                                       res.get("signal_fingerprint")})
                continue

            s = _settle(mem, mandate=mandate, spec=spec,
                        title="R59 machine %s %s%s"
                              % (kind, c["feature_name"],
                                 (" [re-expresses %s]" % eq.get("base_column"))
                                 if econ else ""),
                        result=res,
                        generation_method=c["generation_method"],
                        origin="MACHINE_GENERATED",
                        information_family="PRICE_STATE",
                        model_family=spec["model_family"])
            measured.append({"hypothesis_id": s["hypothesis_id"],
                             "feature": c["feature_name"],
                             "expression": c["spec"],
                             "attributed_family": mandate["family"],
                             "base_equivalent": eq.get("base_column")
                             if econ else None,
                             "outcome": s["gate"]["outcome"],
                             "lockbox_t": s["gate"]["lockbox_t"]})
            if s["gate"]["qualified"]:
                qualified.append(s["hypothesis_id"])

        # Record how productive this generative space still is. Once the
        # generator stops producing books the estate has not already measured,
        # the space is genuinely spent and the frontier retires it - which is
        # what lets the loop reach a real terminal state.
        mem.record_generator_yield(asset_class=ac, kind=kind,
                                   generated=len(cands), novel=len(measured),
                                   duplicates=len(duplicates))

        return AR.OUTCOME_COMPLETED, {
            "real_work": "r59_mathematical", "asset_class": ac,
            "machine_kind": kind, "seed": seed,
            "generator": "alpha_agent.r39.representation_factory",
            "hypotheses_generated": len(cands),
            "hypotheses_measured": len(measured),
            "duplicate_books_rejected": len(duplicates),
            "duplicates": duplicates[:6],
            "qualified": qualified,
            "disposition": "QUALIFIED" if qualified else "NO_ALPHA_EVIDENCE",
            "measured": measured[:12]}

    def _native(job) -> tuple:
        """Run a dated-contract family across its groups, sweeping horizons.

        The horizon sweep is ONE search family: the cell that wins is recorded
        with the whole sweep attached, and the family is charged once for
        having looked at every horizon rather than once per horizon.
        """
        from . import native as NV
        m = job.payload or {}
        ac = m.get("asset_class")
        p = m.get("payload") or {}
        family = p.get("family")
        groups = p.get("groups") or [None]
        horizons = tuple(p.get("horizons") or NV.HORIZONS)

        if not NV.available():
            return AR.OUTCOME_BLOCKED_SPECIFIC, {
                "reason": "R38 native contract layer absent at %s"
                          % NV.NATIVE_LAYER_DIR,
                "real_work": "r59_native"}

        measured, skipped = [], []
        for grp in groups:
            res = NV.horizon_sweep(family=family, group=grp,
                                   horizons=horizons)
            if res.get("state") != "MEASURED":
                skipped.append({"group": grp, "state": res.get("state"),
                                "reason": res.get("reason")})
                continue
            sweep = res.get("horizon_sweep") or {}
            spec = {"family": family, "group": grp,
                    "substrate": "R38_NATIVE_CONTRACT_LAYER",
                    "horizons": list(sweep.get("horizons_run") or horizons)}
            mandate = dict(m)
            mandate["family"] = family
            s = _settle(mem, mandate=mandate, spec=spec,
                        title="R59 native %s / %s" % (family, grp or "ALL"),
                        result=res,
                        generation_method="GOVERNOR_NATIVE_MANDATE",
                        origin="R59_GOVERNOR",
                        information_family="FUTURES_TERM_STRUCTURE"
                        if family in ("CALENDAR_TERM_STRUCTURE",
                                      "RATES_CURVE_RV") else "PRICE_STATE",
                        model_family="XS_LONG_SHORT",
                        horizon=sweep.get("selected_horizon"),
                        # The sweep took one draw per horizon and reported the
                        # best; charge it for every draw. And charge the native
                        # campaign's own multiplicity, exactly as R57 and R58
                        # corrected across their campaign families.
                        within_family_tests=int(
                            sweep.get("horizons_examined") or 1),
                        campaign_method="GOVERNOR_NATIVE_MANDATE")
            row = {"hypothesis_id": s["hypothesis_id"],
                   "family": family, "group": grp,
                   "selected_horizon": sweep.get("selected_horizon"),
                   "per_horizon_lockbox_t":
                       sweep.get("per_horizon_lockbox_t"),
                   "outcome": s["gate"]["outcome"],
                   "lockbox_t": s["gate"]["lockbox_t"],
                   "burden_denominator": s["gate"]["burden_denominator"],
                   "burden_corrected_p": s["gate"]["burden_corrected_p"],
                   "failed_gates": s["gate"]["failed_gates"]}
            # A survivor is FROZEN and the loop carries on. A freeze is not a
            # stop condition and never touches the operational portfolio.
            if s["gate"]["qualified"]:
                row["freeze"] = freeze_qualified(
                    mem, hypothesis_id=s["hypothesis_id"],
                    adopt_forward=adopt_forward)
            measured.append(row)

        if not measured:
            return AR.OUTCOME_BLOCKED_SPECIFIC, {
                "reason": "no group produced a measurable cell",
                "real_work": "r59_native", "skipped": skipped}
        return AR.OUTCOME_COMPLETED, {
            "real_work": "r59_native", "asset_class": ac, "family": family,
            "substrate": "R38_NATIVE_CONTRACT_LAYER",
            "horizons_swept": list(horizons),
            "hypotheses_measured": len(measured),
            "groups_skipped": skipped,
            "disposition": ("QUALIFIED"
                            if any(x["outcome"] == r59.HO_QUALIFIED
                                   for x in measured)
                            else "NO_ALPHA_EVIDENCE"),
            "measured": measured}

    def _cross_asset(job) -> tuple:
        m = job.payload or {}
        if (m.get("payload") or {}).get("family") in _NATIVE_NAMES:
            return _native(job)
        return _economic(job)

    def _data_opportunity(job) -> tuple:
        """Re-evaluate one owned-but-unused information family.

        This handler does NOT purchase, subscribe or call a paid provider. It
        re-measures what is actually on disk and records whether the blocker is
        still real, so a data opportunity is a live claim rather than a note in
        a release document.
        """
        from . import opportunities as OPP
        m = job.payload or {}
        oid = (m.get("payload") or {}).get("opportunity_id") or ""
        rep = OPP.reassess(mem, opportunity_id=oid)
        return AR.OUTCOME_COMPLETED, {
            "real_work": "r59_data_opportunity", "opportunity_id": oid,
            "disposition": rep.get("state"),
            "resolved": rep.get("resolved"),
            "detail": rep.get("note")}

    return {
        AR.CAT_EXPERIMENT: _economic,
        AR.CAT_HYPOTHESIS_GENERATION: _mathematical,
        AR.CAT_SIGNAL_COMBINATION: _cross_asset,
        AR.CAT_DATA_VALIDATION: _data_opportunity,
    }


def route_r59(existing: dict, r59_handlers: dict) -> dict:
    """Compose R59 handlers with an existing production handler map.

    An R59 handler runs ONLY for a job whose lane starts with ``r59.``;
    everything else falls through to the handler the queue already had. This is
    the Stage-10 identity-lane pattern, and it is what makes it safe to run the
    R59 engine against a queue that also carries live collection work.
    """
    out = dict(existing or {})
    for cat, fn in r59_handlers.items():
        base = out.get(cat)

        def _routed(job, _fn=fn, _base=base, _cat=cat):
            if str(getattr(job, "lane", "")).startswith(r59.LANE_PREFIX):
                return _fn(job)
            if _base is None:
                return AR.OUTCOME_BLOCKED_SPECIFIC, {
                    "reason": "no non-R59 handler registered for %s" % _cat}
            return _base(job)

        out[cat] = _routed
    return out
