"""alpha_agent.r59.governor - the persistent research GOVERNOR (Level 1).

The governor answers ONE question - *what should we learn next?* - and answers
it from evidence rather than from a human picking a topic. It reads the
frontier, the graveyard, the search burden and the data-opportunity list, and
emits bounded RESEARCH MANDATES. It executes nothing, owns no capital and can
take no portfolio action.

Two properties are what actually end the "campaign ends, human chooses next"
loop:

* IT ALWAYS HAS AN ANSWER WHILE INDEPENDENT WORK EXISTS. ``generate_mandates``
  is a function of state, not of a human prompt. A failed hypothesis changes
  the state (burden up, family prosecuted) and therefore changes the next
  answer - which is why a rejection generates work instead of silence.
* IT REFUSES TO LET ONE SCOPE MONOPOLISE. A fixed share of every batch is
  reserved for the strongest NON-EQUITY ready mandates. Without that, expected
  information value alone would keep choosing the deepest, best-instrumented
  panel, and the frontier would quietly collapse back to US equities - the
  exact regression R59 exists to prevent.

Expected information value (EIV) is deliberately simple and inspectable: it
rewards an untouched scope, an unprosecuted family and a generative family with
budget left; it penalises a scope the estate has already searched heavily,
because a marginal test there is worth less and costs more in multiple-testing
burden.
"""
from __future__ import annotations

from typing import Optional

from .. import r59
from . import blockers as BLK
from . import frontier as FR
from . import information_needs as IN
from . import memory as M
from . import opportunities as OPP

CALCULATION_OWNER = "alpha_agent.r59.governor"

#: R64 - how many information needs one batch may OFFER (before fairness).
INFORMATION_NEED_BATCH = 6

MANDATE_ECONOMIC = "ECONOMIC_FAMILY"
MANDATE_MATHEMATICAL = "MACHINE_DISCOVERY"
MANDATE_CROSS_ASSET = "CROSS_ASSET"
MANDATE_DATA = "DATA_OPPORTUNITY"
MANDATE_NATIVE = "NATIVE_TERM_STRUCTURE"

#: Holding periods a native family sweeps. One sweep is ONE search family.
NATIVE_HORIZONS = (1, 5, 21, 63)


def _native_groups(family: str) -> list:
    """Which economic groups a native family addresses."""
    if family == "RATES_CURVE_RV":
        return ["TREASURY_FUTURES"]
    if family == "INTER_COMMODITY_RV":
        return list(FR.COMMODITY_RV_GROUPS)
    if family == "AGRICULTURAL_SEASONALITY":
        return list(FR.AG_RV_GROUPS)
    return [None]

# How many machine-generated candidates one mathematical mandate is worth. It
# is bounded so a generative family cannot flood the queue and starve the
# economic and cross-asset lanes in a single batch.
MACHINE_BATCH = 6

# --------------------------------------------------------------------------- #
# RESOURCE GOVERNANCE.
#
# A generative grammar is effectively unbounded: R59 measured its symbolic
# spaces still returning 75-89% books the estate had never seen after 3,561
# tests. Novelty therefore cannot retire it, and an allocator that reads
# novelty as value will spend the machine on mathematics until someone
# switches the machine off. The batch ceiling below caps how much of ONE
# batch machine discovery may take; it does not cap how much of the estate's
# WORK it takes, because one machine mandate expands into MACHINE_BATCH
# candidates while an economic mandate is one test. That is how 3,534 of
# R59's 3,561 measurements came from two grammars.
#
# Capacity is therefore allocated on MARGINAL USEFUL yield, measured from
# this estate's own record rather than assumed:
#
#     AUTO_TRANSFORM_GRAMMAR      2,003 scored, 7.0% reached |t| >= 2
#     SYMBOLIC_TREE_SEARCH        1,531 scored, 8.4% reached |t| >= 2
#     GOVERNOR_ECONOMIC_MANDATE      15 scored, 20.0%
#     GOVERNOR_NATIVE_MANDATE        11 scored, 18.2%
#
# Neither rate has yet produced a survivor, so "notable" is a WEAK proxy for
# value - but it is the honest one available, and the ordering is the point:
# a machine test is roughly a third as likely to produce anything worth
# looking at as an economic one, at the same cost. Allocation follows that
# ratio, is floored so mathematical discovery is throttled and never killed,
# and rises again on its own if the machine's yield recovers.
# --------------------------------------------------------------------------- #
GENERATIVE_METHODS = ("AUTO_TRANSFORM_GRAMMAR", "SYMBOLIC_TREE_SEARCH")

#: |t| at which a measured result is worth a second look. Not a qualification
#: threshold - the burden-corrected gate owns that - just the marker for "this
#: search produced something, rather than noise".
NOTABLE_T = 2.0

#: Share of recently SETTLED work above which generative discovery is judged
#: to be crowding out every other lane.
MACHINE_SHARE_CEILING = 0.60

#: How much recent work the allocator reads. Big enough to be stable, small
#: enough that a change in productivity shows up within a session.
CAPACITY_WINDOW = 500
MIN_FAMILY_OBSERVATIONS = 40

#: How far back the ALTERNATIVE baseline reaches. Deliberately the whole
#: record rather than the recent window: most non-generative rows are
#: imported prior-release hypotheses that carry no lockbox statistic, so a
#: 500-row window contained only eleven scored alternatives and the
#: marginal-yield term silently switched itself off. A baseline is allowed to
#: be old; what must be recent is the thing being judged against it.
ALTERNATIVE_BASELINE_LIMIT = 5000

#: Mathematical discovery is throttled, never killed (requirement G.1).
MIN_CAPACITY_MULTIPLIER = 0.25

#: The lanes whose exploration capacity is preserved whatever the machine is
#: doing. Named here so "we kept exploring elsewhere" is a contract rather
#: than an accident of which mandate happened to score highest.
PROTECTED_LANES = (
    r59.AC_US_EQUITY, r59.AC_RATES, r59.AC_COMMODITY, r59.AC_FX,
    r59.AC_EQUITY_INDEX, r59.AC_VOLATILITY, r59.AC_CROSS_ASSET,
    "EVENTS", "FUNDAMENTALS", "POSITIONING", "MACRO", "DATA_OPPORTUNITY")


#: How many generative tests a restored allocation is worth after new
#: information appears. Long enough for the grammar to actually search the
#: new inputs, short enough that one arrival cannot buy permanent capacity.
REOPEN_GRACE_TESTS = 100

#: memory_meta key holding the last-observed information-set fingerprint and
#: the generative test count when it changed.
CAPACITY_META_KEY = "governor_information_set_fingerprint"


def _reopen_state(mem, current_multiplier: float) -> dict:
    """Has the INFORMATION SET changed, and is the grace period still open?"""
    fingerprint = r59.short_hash(
        sorted((str(o.get("opportunity_id")), str(o.get("state")))
               for o in mem.opportunities()), 16)
    tests = mem.count_settled(methods=GENERATIVE_METHODS)
    prior = mem.get_meta(CAPACITY_META_KEY) or None

    if prior is None:
        # First observation is a BASELINE, not an arrival. Recording an
        # unknown world as "new information" would hand full allocation to
        # every fresh estate for no measured reason.
        mem.set_meta(CAPACITY_META_KEY,
                     {"fingerprint": fingerprint, "tests_at_change": tests,
                      "observed_at": r59.now_iso(), "change": "BASELINE"})
        return {"reopened": False, "fingerprint": fingerprint,
                "change": None, "tests_since_change": 0}

    if prior.get("fingerprint") != fingerprint:
        mem.set_meta(CAPACITY_META_KEY,
                     {"fingerprint": fingerprint, "tests_at_change": tests,
                      "observed_at": r59.now_iso(),
                      "change": "INFORMATION_SET_CHANGED"})
        return {"reopened": True, "fingerprint": fingerprint,
                "change": "INFORMATION_SET_CHANGED", "tests_since_change": 0}

    since = tests - int(prior.get("tests_at_change") or 0)
    open_grace = (prior.get("change") == "INFORMATION_SET_CHANGED"
                  and since < REOPEN_GRACE_TESTS)
    return {"reopened": bool(open_grace), "fingerprint": fingerprint,
            "change": prior.get("change") if open_grace else None,
            "tests_since_change": max(0, since)}


def _notable(row: dict) -> bool:
    t = (row.get("statistic") or {}).get("lockbox_t")
    try:
        return abs(float(t)) >= NOTABLE_T
    except (TypeError, ValueError):
        return False


def capacity_allocation(mem: Optional[M.ResearchMemory] = None, *,
                        window: int = CAPACITY_WINDOW) -> dict:
    """How much research capacity generative discovery has earned.

    Reads three measured quantities and nothing else:

    1. CROWDING - generative methods' share of recently settled work, against
       :data:`MACHINE_SHARE_CEILING`;
    2. MARGINAL YIELD - the notable-result rate of generative methods against
       the best non-generative alternative in the same record. This is the
       "deteriorating marginal useful yield" term: it falls when the grammar
       stops paying and rises when it starts again;
    3. NEW INFORMATION - if an information source changed state after the last
       generative result was settled, allocation is restored to full, because
       a grammar over new inputs is a different search from the one that was
       throttled.

    Returns the multiplier plus every input, so the decision can be read back
    rather than trusted.
    """
    mem = mem or M.open_memory()
    rows = mem.recent_settled(limit=int(window))
    n = len(rows)

    gen_rows = [r for r in rows if r.get("generation_method")
                in GENERATIVE_METHODS]
    share = (len(gen_rows) / n) if n else 0.0

    # The alternative's productivity is read from the estate's WHOLE record,
    # not from the same recent window. Generative work crowds the window out
    # by construction - that is the thing being measured - so scoring the
    # alternative there would leave it permanently below the minimum sample
    # and silently disable the marginal-yield term.
    other_rows = mem.recent_settled(limit=ALTERNATIVE_BASELINE_LIMIT,
                                    exclude_methods=GENERATIVE_METHODS)

    def _rate(rs: list):
        scored = [r for r in rs
                  if (r.get("statistic") or {}).get("lockbox_t") is not None]
        if len(scored) < MIN_FAMILY_OBSERVATIONS:
            return None, len(scored)
        return sum(1 for r in scored if _notable(r)) / len(scored), len(scored)

    gen_rate, gen_scored = _rate(gen_rows)
    alt_rate, alt_scored = _rate(other_rows)

    reasons: list = []
    crowding = 1.0
    if n and share > MACHINE_SHARE_CEILING:
        crowding = MACHINE_SHARE_CEILING / share
        reasons.append(
            "generative methods hold %.0f%% of the last %d settled results, "
            "above the %.0f%% ceiling" % (100 * share, n,
                                          100 * MACHINE_SHARE_CEILING))

    marginal = 1.0
    if gen_rate is not None and alt_rate:
        marginal = min(1.0, gen_rate / alt_rate)
        if marginal < 1.0:
            reasons.append(
                "generative notable-result rate %.3f is below the best "
                "non-generative rate %.3f over %d and %d scored results"
                % (gen_rate, alt_rate, gen_scored, alt_scored))

    multiplier = max(MIN_CAPACITY_MULTIPLIER, crowding * marginal)

    # 3. New information restores allocation: a grammar over inputs it has
    #    never seen is not the search that was throttled.
    #
    #    Detected by a FINGERPRINT of the opportunity set, never by a
    #    timestamp. Every session re-seeds the opportunity table, which
    #    rewrites ``updated_at`` on rows that did not change, so a
    #    timestamp rule would have reopened allocation on every single call
    #    and the throttle would have measured as a no-op forever.
    reopened = _reopen_state(mem, multiplier)
    if reopened.get("reopened"):
        multiplier = 1.0
        reasons.append(
            "the information set changed (%s) and generative search has had "
            "only %d of its %d restored tests; allocation is at full"
            % (reopened["change"], reopened["tests_since_change"],
               REOPEN_GRACE_TESTS))

    return {
        "calculation_owner": CALCULATION_OWNER,
        "window_settled": n,
        "generative_share_recent": round(share, 4),
        "generative_share_ceiling": MACHINE_SHARE_CEILING,
        "generative_notable_rate": (None if gen_rate is None
                                    else round(gen_rate, 4)),
        "alternative_notable_rate": (None if alt_rate is None
                                     else round(alt_rate, 4)),
        "crowding_multiplier": round(crowding, 4),
        "marginal_yield_multiplier": round(marginal, 4),
        "multiplier": round(multiplier, 4),
        "multiplier_floor": MIN_CAPACITY_MULTIPLIER,
        "machine_batch": max(1, int(round(MACHINE_BATCH * multiplier))),
        "reopened_by": (reopened.get("change")
                        if reopened.get("reopened") else None),
        "information_set_fingerprint": reopened.get("fingerprint"),
        "reasons": reasons,
        "protected_lanes": list(PROTECTED_LANES),
        "mathematical_discovery_disabled": False,
    }


def _eiv(*, asset_class: str, family: str, generative: bool,
         burden_here: int, hypotheses_here: int, instruments: int,
         frontier_state: str) -> float:
    """Expected information value in [0, 1]-ish. Higher runs sooner."""
    score = 0.50

    # An untouched scope is worth more than a heavily-searched one.
    if frontier_state == r59.FS_DATA_READY:
        score += 0.25
    elif frontier_state == r59.FS_RESEARCH_READY:
        score += 0.10

    # Diminishing returns: every prior test in this scope makes the next one
    # both less likely to be new and more expensive in burden.
    score -= min(0.30, burden_here / 800.0)

    # A generative family still has unexplored space; an enumerable family that
    # has never been entered is the single most informative thing available.
    if generative:
        score += 0.08
    if hypotheses_here == 0:
        score += 0.12

    # A wider cross-section supports a more credible cross-sectional claim.
    score += min(0.10, instruments / 400.0)
    return round(max(0.01, min(0.99, score)), 4)


def _mandate(kind: str, *, asset_class: str, family: str, eiv: float,
             reason: str, payload: dict) -> dict:
    body = {"kind": kind, "asset_class": asset_class, "family": family,
            "expected_information_value": eiv, "reason": reason,
            "payload": payload, "issued_by": CALCULATION_OWNER,
            "issued_at": r59.now_iso()}
    body["mandate_id"] = "M59_%s" % r59.short_hash(
        {k: body[k] for k in ("kind", "asset_class", "family", "payload")}, 12)
    return body


def _generative_draws(mem, asset_class: str, machine_kind: str) -> int:
    """How many candidates this generator has ALREADY drawn in this scope.

    Read from the estate's own ``generator_yield`` ledger - the count includes
    duplicates, which is the point: a duplicate is a draw that was made, and
    the next draw must therefore start somewhere the search has not been.
    Deterministic and replayable from persisted state; zero when the ledger has
    nothing to say, which reproduces the pre-R61 seed exactly.
    """
    want = str(machine_kind or "").upper()
    try:
        for row in (mem.generator_yield() or []):
            if str(row.get("asset_class") or "") != str(asset_class):
                continue
            if want and want not in str(row.get("kind") or "").upper():
                continue
            return int(row.get("generated") or 0)
    except Exception:                                    # noqa: BLE001
        return 0
    return 0


def generate_mandates(mem: Optional[M.ResearchMemory] = None, *,
                      limit: int = 24,
                      frontier_view: Optional[dict] = None) -> dict:
    """Produce the next bounded batch of research mandates.

    Never returns an empty batch while any READY scope has an open family; the
    empty case is a genuine terminal state and is reported as such with the
    reason, so a caller can distinguish "nothing left" from "nothing tried".
    """
    mem = mem or M.open_memory()
    view = frontier_view or FR.measure(mem)
    rows = view["asset_classes"]
    burden = mem.burden().get("by_asset_class") or {}
    capacity = capacity_allocation(mem)

    candidates: list = []
    for ac, row in rows.items():
        if row["state"] not in (r59.FS_DATA_READY, r59.FS_RESEARCH_READY,
                                r59.FS_ACTIVE_SEARCH):
            continue
        detail = row["detail"]
        specs = {s["family"]: s for s in detail.get("family_specs") or []}
        n_here = int(detail.get("hypotheses_recorded") or 0)
        b_here = int(burden.get(ac, 0))
        inst = int(detail.get("instruments") or 0)

        for family in detail.get("remaining_families") or []:
            spec = specs.get(family) or {}
            generative = bool(spec.get("generative"))
            eiv = _eiv(asset_class=ac, family=family, generative=generative,
                       burden_here=b_here, hypotheses_here=n_here,
                       instruments=inst, frontier_state=row["state"])
            if family in FR.NATIVE_FAMILIES:
                # A dated-contract family. It carries its economic GROUP and a
                # horizon sweep, because the substrate supports a real term
                # structure and a real holding-period question - neither of
                # which the back-adjusted panel could express.
                kind = MANDATE_NATIVE
                groups = _native_groups(family)
                payload = {"family": family, "groups": groups,
                           "horizons": list(NATIVE_HORIZONS)}
                eiv = min(0.95, eiv + 0.18)   # unexplored substrate
                reason = ("%s needs the R38 dated-contract layer; it has never "
                          "been prosecuted on real front/deferred prices"
                          % family)
            elif generative:
                kind = MANDATE_MATHEMATICAL
                machine_kind = ("SYMBOLIC" if family.endswith("SYMBOLIC")
                                else "AUTO")
                # DETERMINISTIC seed. Python's ``hash()`` of a string is
                # randomised per process (PYTHONHASHSEED), so seeding from it
                # made the machine search unreproducible between runs - two
                # invocations of the same state would explore different spaces
                # and neither could be replayed.
                # R61 - the seed must ADVANCE with the search itself. It was
                # derived from the scope's BURDEN alone, and a draw rejected as
                # a duplicate does not count to burden - so a scope whose every
                # draw was a duplicate re-derived the SAME seed, and therefore
                # the SAME expression, on every batch, forever. That is the
                # symbolic-enumeration fixpoint R61 forbids: 18,621 generation
                # jobs in twenty-four hours re-proposing one expression the
                # estate had already booked. Adding the generator's OWN draw
                # count (its existing yield ledger) makes each batch explore a
                # new region while keeping the seed fully deterministic and
                # replayable from persisted state - never a clock, never the
                # per-process randomised builtin hash.
                drawn = _generative_draws(mem, ac, machine_kind)
                payload = {"machine_kind": machine_kind,
                           "batch": capacity["machine_batch"],
                           "search_position": drawn,
                           "seed": 5900 + int(
                               r59.short_hash([ac, machine_kind, b_here,
                                               drawn], 8),
                               16) % 90000}
                # The capacity multiplier is applied to the SIZE of the search
                # as well as to its priority. Priority alone would not have
                # helped: the fairness round-robin takes one mandate of each
                # kind regardless of score, so an unthrottled machine mandate
                # would still expand into six tests against an economic
                # mandate's one - which is precisely how two grammars came to
                # own 99% of the estate's measurements.
                eiv = round(eiv * capacity["multiplier"], 4)
                reason = ("machine representation space for %s is generative "
                          "and has %d prior tests in this scope; capacity "
                          "multiplier %.2f -> batch %d"
                          % (ac, b_here, capacity["multiplier"],
                             capacity["machine_batch"]))
            elif ac == r59.AC_CROSS_ASSET:
                kind = MANDATE_CROSS_ASSET
                payload = {"family": family, "mode": (spec.get("mode")
                                                      or "CROSS_SECTIONAL")}
                reason = "cross-asset family %s never prosecuted" % family
            else:
                kind = MANDATE_ECONOMIC
                payload = {"family": family,
                           "mode": spec.get("mode") or "TIME_SERIES"}
                reason = ("economic family %s has no verdict in scope %s"
                          % (family, ac))
            candidates.append(_mandate(kind, asset_class=ac, family=family,
                                       eiv=eiv, reason=reason,
                                       payload=payload))

    # Data opportunities that are actionable WITHOUT a purchase.
    #
    # R61 - a re-probe is issued only when its own substrate has MOVED. The
    # probe for an owned-but-unreadable family restates a stored measurement:
    # re-running it against an unchanged substrate returns, with certainty,
    # what the last run returned. Four such mandates were re-issued roughly
    # 17,700 times each in a day for exactly that non-answer, which is a busy
    # loop wearing a research job's name. Nothing is retired here - the moment
    # the watermark moves the mandate is issuable again on the next batch.
    deferred_probes: list = []
    for opp in mem.opportunities():
        if opp["state"] != r59.DO_ALREADY_OWNED_UNUSED:
            continue
        informative = OPP.probe_is_informative(
            mem, opportunity_id=opp["opportunity_id"])
        if not informative.get("informative"):
            deferred_probes.append({
                "opportunity_id": opp["opportunity_id"],
                "reason": informative.get("reason"),
                "last_probe_at": informative.get("last_probe_at"),
                "watermark": informative.get("watermark"),
                "blocker_reason": BLK.WAITING_FOR_EXTERNAL_ENTITLEMENT,
                "reissued_when": "its own substrate watermark changes"})
            continue
        candidates.append(_mandate(
            MANDATE_DATA, asset_class=opp.get("asset_class")
            or r59.AC_US_EQUITY, family="DATA:%s" % opp["opportunity_id"],
            eiv=0.62,
            reason="owned information is present but unusable as delivered",
            payload={"opportunity_id": opp["opportunity_id"],
                     "title": opp["title"]}))

    # R64 - the governor's second question: which asset class x horizon x
    # information dimension has the highest REMAINING research value? Answered
    # from the information gap frontier artifact (READ by path through the
    # adapter; no research package is imported here), carried on the existing
    # DATA_OPPORTUNITY kind, lane and fairness cap, and deduped by a per-need
    # watermark so an unchanged frontier is never re-mandated.
    information_needs = IN.candidates(mem, limit=INFORMATION_NEED_BATCH)
    for row in information_needs:
        candidates.append(_mandate(
            MANDATE_DATA, asset_class=row["asset_class"], family=row["family"],
            eiv=row["eiv"], reason=row["reason"], payload=row["payload"]))

    candidates.sort(key=lambda m: -m["expected_information_value"])

    selected = _apply_fairness(candidates, limit=limit,
                               machine_multiplier=capacity["multiplier"])
    terminal = None
    if not selected:
        terminal = ("NO_INDEPENDENT_EXECUTABLE_RESEARCH_REMAINS"
                    if not candidates else "LIMIT_ZERO")

    mem.event("MANDATES_GENERATED", subject="governor",
              detail={"n_candidates": len(candidates),
                      "n_selected": len(selected),
                      "n_deferred_probes": len(deferred_probes),
                      "n_information_needs": len(information_needs),
                      "non_equity_selected": sum(
                          1 for m in selected
                          if m["asset_class"] != r59.AC_US_EQUITY)})
    return {"calculation_owner": CALCULATION_OWNER,
            "generated_at": r59.now_iso(),
            "n_candidates": len(candidates),
            "mandates": selected,
            "terminal": terminal,
            "capacity": capacity,
            # R64 - information needs offered this batch (before fairness).
            "n_information_needs": len(information_needs),
            "information_need_source": IN.SOURCE,
            # R61 - probes withheld because their substrate has not moved. A
            # suppressed mandate is stated, with the condition that revives it.
            "deferred_probes": deferred_probes,
            "n_deferred_probes": len(deferred_probes),
            "deferred_probe_policy": (
                "a data-opportunity probe is issued only when its own "
                "substrate watermark has changed; it is deferred, never "
                "retired, and never rate-limited by a clock"),
            "non_equity_reservation": r59.NON_EQUITY_RESERVATION}


#: No single mandate kind may take more than this share of a batch. Generative
#: families score highest in every scope, so pure EIV ordering filled whole
#: batches with machine discovery and the economic and cross-asset lanes never
#: ran. That is the same monopoly failure as the equity one, in a second
#: dimension, and it is capped the same way.
MAX_KIND_SHARE = 0.50


def _apply_fairness(candidates: list, *, limit: int,
                    machine_multiplier: float = 1.0) -> list:
    """Fill the batch by EIV under two diversity constraints.

    * a FLOOR reserving part of the batch for non-equity scopes;
    * a CEILING on how much of the batch any one mandate kind may take,
      tightened for mathematical discovery by its earned capacity.

    Both are soft in the same direction: if a constraint cannot be satisfied
    because the candidates do not exist, the slot returns to the open pool
    rather than shrinking the batch. A quota that silently reduced throughput
    would look exactly like the loop running dry, which is the one signal this
    release cannot afford to corrupt.
    """
    if limit <= 0:
        return []

    def _take(pool: list, n: int, taken: set) -> list:
        out = []
        for m in pool:
            if len(out) >= n:
                break
            if id(m) in taken:
                continue
            out.append(m)
            taken.add(id(m))
        return out

    taken: set = set()
    picked: list = []

    # 1. non-equity floor
    reserve = int(round(limit * r59.NON_EQUITY_RESERVATION))
    non_eq = [m for m in candidates if m["asset_class"] != r59.AC_US_EQUITY]
    picked += _take(non_eq, reserve, taken)

    # 2. per-kind ceiling, filling round-robin from the best of each kind
    cap = max(1, int(limit * MAX_KIND_SHARE))
    # Mathematical discovery keeps at least one slot in every batch - it is
    # throttled, never switched off - but its ceiling shrinks with its earned
    # capacity so the slots it gives up go to the other lanes.
    machine_cap = max(1, int(round(cap * float(machine_multiplier))))
    kinds: dict = {}
    for m in candidates:
        kinds.setdefault(m["kind"], []).append(m)
    counts = {k: sum(1 for m in picked if m["kind"] == k) for k in kinds}
    progress = True
    while len(picked) < limit and progress:
        progress = False
        for kind in sorted(kinds, key=lambda k: -max(
                (m["expected_information_value"] for m in kinds[k]),
                default=0.0)):
            if len(picked) >= limit:
                break
            kind_cap = machine_cap if kind == MANDATE_MATHEMATICAL else cap
            if counts.get(kind, 0) >= kind_cap:
                continue
            got = _take(kinds[kind], 1, taken)
            if got:
                picked += got
                counts[kind] = counts.get(kind, 0) + 1
                progress = True

    # 3. any remaining slots go to the best of what is left, ignoring the cap -
    #    an unfillable ceiling must not cost throughput. But a THROTTLED kind
    #    fills last: otherwise mathematical discovery hands back its slots at
    #    step 2 and immediately takes them again here, because it always
    #    scores highest, and the throttle measures as a no-op.
    if len(picked) < limit:
        not_throttled = [m for m in candidates
                         if m["kind"] != MANDATE_MATHEMATICAL]
        picked += _take(not_throttled, limit - len(picked), taken)
    if len(picked) < limit:
        picked += _take(candidates, limit - len(picked), taken)

    # The ORDER is the fairness decision and is returned as-is. Re-sorting by
    # expected information value here would undo the round-robin and hand the
    # front of the batch back to whichever kind scores highest - which is
    # exactly how the executor ended up running machine discovery only, even
    # though the batch itself was diverse.
    picked = picked[:limit]
    for rank, m in enumerate(picked):
        m["batch_rank"] = rank
    return picked


def stop_reason(mem: Optional[M.ResearchMemory] = None, *,
                frontier_view: Optional[dict] = None) -> dict:
    """Decide whether the autonomous loop may legitimately stop.

    Only the four conditions in section O count. "The last batch drained" is
    NOT one of them, which is why this asks the governor for more work before
    it will ever report a terminal state.
    """
    mem = mem or M.open_memory()
    batch = generate_mandates(mem, limit=8, frontier_view=frontier_view)
    if batch["mandates"]:
        return {"stop": False,
                "reason": "the research director can still generate %d "
                          "independently executable mandates"
                          % len(batch["mandates"]),
                "next_mandates": len(batch["mandates"])}
    view = frontier_view or FR.measure(mem)
    blocked = [ac for ac, r in view["asset_classes"].items()
               if r["state"] == r59.FS_BLOCKED]
    return {"stop": True,
            "condition": "A",
            "reason": "no independently executable READY research remains and "
                      "the governor cannot generate a valid new mandate "
                      "without new information",
            "blocked_scopes": blocked}
