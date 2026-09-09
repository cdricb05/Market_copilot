# Release 62.1.1 — Forward activation + live-state integrity

- **Branch:** `r62-1-1-forward-activation-integrity`
- **Built over:** `da77be7155ab8bd4acb887cba69ca9e95396b60b` (R62.1)
- **Worktree:** `D:\paper_trader_r62_1_1_forward_activation_integrity`
- **Deployed checkout `C:\Users\binis\paper_trader`:** READ ONLY throughout.
- **Status:** not committed, not pushed, not merged, not deployed, and **no live
  adoption was performed**.

R62.1.1 adds no economics, no gate, no threshold and no new business owner. It
closes the ACTIVATION gap R62.1 left behind — a canonical registrar with nothing
registered — and three live-state defects that made correct behaviour read as
failure.

---

## 1. The activation gap, and why the runtime could not close it

R61 made "qualify a challenger" and "start its forward evidence" ONE governed
operation. R62.1 gave that operation a canonical registrar. Both act at the
moment a freeze is **created**:

```
alpha_agent.r59.handlers.freeze_qualified()
    -> the challenger row is already in research memory
    -> return {"state": "ALREADY_FROZEN"}          # <- returned HERE
    -> _register_forward_evidence(...)             # <- never reached
```

So a freeze written before either release existed could never be adopted,
however many times the persistent research runtime ran. A live read-only
preflight proved the consequence:

| fact | value |
| --- | --- |
| `canonical_forward_registration_count` | 0 |
| `orphan_freezes_adoptable_count` | 4 |
| `orphan_freezes_closed_by_lifecycle_count` | 1 |
| `open_adoption_intents` | `[]` |

The four adoptable freezes — `R58_SHORT_VOLUME_PRESSURE_V1`,
`R58_DISCLOSURE_INTENSITY_V1`, `R58_FUND_MOMENTUM_VETO_V1`, `R58_FCF_PURE_V1` —
are all `ACTIVE`, `adoptable`, `FORWARD_SIGNAL_CANONICAL`, `US_EQUITY`, horizon
21, inception `2026-09-03`. The fifth,
`R59_CALENDAR_TERM_STRUCTURE_F9BE2426`, is `WITHDRAWN` and
`never_resurrectable`, and must never register.

---

## 2. The operator adoption contract

`scripts/adopt_prospective_freeze.py` is **THE** one operator entrypoint for
adopting an already-existing frozen challenger. The strict audit blocks on a
second one.

### What it owns

Nothing. It owns no lifecycle rule, no identity calculation, no observation
calendar, no registration rule, no evidence rule, no promotion rule and no
portfolio rule. It resolves operator-named ids against the canonical
ResearchMemory through `open_memory_readonly()` and calls
`api.prospective_adoption.adopt_prospective_freeze` **exactly once per requested
id**. It also refuses **before parsing** if `paper_trader` resolved to a
different checkout — the venv's editable-install finder sits ahead of every
`sys.path` entry, and an entrypoint that WRITES must never run another tree's
adoption code. The three rules it would otherwise have had to invent live with
the owner:

| rule | owner |
| --- | --- |
| which freeze does this id name? | `PA.resolve_freeze_by_challenger_id` |
| what boundary may observations start from? | `PA.current_prospective_boundary` |
| which token authorises the write? | `PA.OPERATOR_ADOPT_CONFIRM_TOKEN` |

### The delegation chain

```
scripts/adopt_prospective_freeze.py     resolves ids, decides nothing
  -> api.prospective_adoption           lifecycle, identity, durable intent
    -> api.forward_challenger_registry  re-checks lifecycle, resolves the clock
      -> (named) accrual owner          matured by alpha_agent.r52.runtime
```

### CLI contract

```
--challenger-id <ID>      repeatable, REQUIRED. There is no --all.
--confirm <TOKEN>         must be ADOPT_PROSPECTIVE_FORWARD_CLOCKS
--execute                 the explicit write switch
--json                    also emit the full report as one JSON object
```

Default behaviour is **DRY RUN**. A live write requires **both** `--confirm
ADOPT_PROSPECTIVE_FORWARD_CLOCKS` **and** `--execute`; either alone is refused
before anything is written.

### Terminal tokens

Exactly one is printed, on the last line:

| token | meaning |
| --- | --- |
| `FORWARD_ADOPTION_DRY_RUN_OK` | a dry run completed; nothing was written |
| `FORWARD_ADOPTION_OK` | every requested adoption is registered |
| `FORWARD_ADOPTION_REFUSED` | a governed refusal (lifecycle / unknown id / missing confirmation / missing write flag) |
| `FORWARD_ADOPTION_FAILED` | an owner failed; intents remain OPEN and resumable |

### Per-challenger output

challenger id · freeze/hypothesis id · resolution · lifecycle · canonical
registrar · outcome · refusal reason · registration timestamp · first eligible
observation session · next legitimate maturity session · backfilled flag ·
predictions emitted · matured observations · effective independent observations.

---

## 3. The no-backfill guarantee

There is no `--effective-from`, `--date`, `--backfill`, `--inception-override`,
`--as-of` or `--since`, in any spelling, and there never may be. The strict
audit fails the build if one appears. Three independent facts hold it up:

1. **The boundary is derived.** `current_prospective_boundary()` returns today's
   UTC date and takes no parameter an operator can reach.
2. **The registrar decides the first observation.** It is the first eligible
   session **strictly after** the session in which registration happened,
   resolved on the asset's own calendar. A session that already completed can
   never become a prediction.
3. **The owner refuses a boundary before its own inception.**
   `REFUSED_BACKDATED_OBSERVATION_CLOCK`, before any store is touched.

The five sessions between the 2026-09-03 R58 inception and an adoption today are
never synthesised, and no module on the path holds a code path that could write
one. `predictions_emitted`, `matured_observations` and
`effective_independent_observations` are 0 at registration and are only ever
advanced by the accrual owner.

## 3b. Recovery, without a second path

`freeze_qualified` now re-offers an **existing** freeze to the same governed
owner instead of returning early. That cannot resurrect anything:

- the lifecycle is reclassified from persisted history, so a `WITHDRAWN`,
  `INVALIDATED`, `SUPERSEDED`, `MATURED` or `FAILED` freeze is refused before any
  store is touched;
- the adoption identity hash keys idempotency and the registrar's
  first-write-wins returns the existing registration untouched;
- the clock it offers is today's, so re-running can never backdate;
- it creates no second registration path — it is the same owner — and it
  promotes nothing and allocates nothing.

---

## 4. Current vs historical service identity

**The defect.** "Is collection running now?" was answered by two payload paths,
and the CURRENT verdict rode on a composition it does not depend on: the full
`/v1/operations/information-collection` route composes source-runtime health,
the event-signal-refresh status over a 14 MB event index and the whole attention
universe. On the routes whose own read legitimately takes minutes, that 45-second
browser budget expired and the browser published `COLLECTION: UNAVAILABLE` — a
**service verdict manufactured from a transport failure** — on the global sticky
header, while another surface, reading a payload that had answered, showed
RUNNING. The same class of defect put `UNAVAILABLE` in the operational book's
global right-rail status.

**The fix, at the source.**

- `api.information_collection.resolve_current_collection_state()` is the ONE
  cheap read: the service-state document, the single-flight lock, and one pure
  lifecycle verdict. Nothing else.
- `api.information_collection.build_current_collection_state()` is the ONE block
  shape and the ONE provenance vocabulary
  (`CANONICAL_CURRENT_RUNTIME_READ` / `DECISION_SNAPSHOT_SECTION` /
  `CURRENT_COLLECTION_STATE_UNAVAILABLE`).
- The collection route and `api.active_manager_state` publish the **same
  object** from that one builder, so two surfaces cannot disagree.
- In the browser, ONE renderer places that block, called by both loaders. A
  fetch that did not answer says `COLLECTION READ DID NOT ANSWER` and asserts no
  service state; the operational book says `BOOK READ DID NOT ANSWER`.
- Historical event-cycle provenance still decides nothing:
  `decides_current_collection_health: False`.

---

## 5. The live reassessment: identity exact, withholding correct

**What the persisted artifacts say.** The natural post-close cycle
`evt_4c53be5424d574c8` (generated 2026-09-09T00:57:32Z, completed 01:04:44Z, 23
events admitted, runtime `da77be7155ab`) recorded:

```
state                    REASSESSED_NO_CHANGE
reassessment_ran         True
proposal_built           False
reassessment_state       CURRENT_NO_CHANGE
candidate_identity_hash  c329a4e57fa9cfea2f1412677ea19176
verdict                  INTRADAY_DECISION_WITHHELD
failing_checks           CONCLUSIVE_PRICED_OUTCOME, TARGET_HASH_BOUND,
                         FEASIBLE_TARGET_WAS_COMPUTED,
                         SWITCHING_ECONOMICS_COMPLETE,
                         RISK_BEFORE_AND_AFTER_PRICED,
                         CANDIDATE_ADDS_NEW_EVIDENCE
withheld_reason_codes    CANDIDATE_EVIDENCE_INCOMPLETE,
                         TARGET_IDENTITY_MISMATCH,
                         SWITCHING_ECONOMICS_INCOMPLETE,
                         DUPLICATE_CANDIDATE
```

**Target identity: exact.** `c329a4e57fa9cfea2f1412677ea19176` is byte-identical
to the standing governed decision
`gdec_2026-09-08_alpha_paper_book_1_c329a4e57fa9`. The chain signal refresh → HOC
→ reassessment → candidate → governance used **one exact persisted identity**.
There is no stale artifact, no composition mismatch and no transient
substitution. Nothing to fix.

**Candidate evidence: not incomplete.** The intraday producer contract
deliberately promotes only on a PRICED R47 outcome — concluding
`CURRENT_NO_CHANGE` for a SESSION is the session-terminal daily producer's
prerogative — so `build_intraday_candidate` set `decision = None` and no target
was built. Seven conditions that inspect a target then failed, and emitted three
codes describing broken evidence.

**Switching economics: nothing to price.** With no switch there are no switching
economics; the refusal was a category error, not a measurement.

**What changed.** A check now has THREE dispositions. When the reassessment
owner concluded `CURRENT_NO_CHANGE`, the cycle owner recorded `proposal_built:
False`, and the cycle owner recorded that a reassessment DID run, the gate
classifies the cycle into the **NO-PRICED-TARGET lane** and those seven
conditions become `NOT_APPLICABLE_TO_THIS_LANE`. The designed no-op is named
once, as `INTRADAY_CYCLE_REACHED_NO_PRICED_TARGET`. The daily gate's
`PROPOSAL_BINDING_CONSISTENT` rule is applied in the same lane, so a no-target
cycle that binds a proposal is still caught as a real
`TARGET_IDENTITY_MISMATCH` — a bound hash is deliberately NOT a lane fact,
because routing such a cycle back into the priced lane would let the stale
artifact satisfy `TARGET_HASH_BOUND` and pass unremarked.

**The verdict is unchanged.** The lane changes which conditions APPLY and never
whether an applicable one passed. The Sep-8 cycle is still `WITHHELD`, and
`DUPLICATE_CANDIDATE` — evidence identical to the standing decision — is still
the governing refusal. Nothing was replayed, rewritten or rebound.

---

## 6. HOC data gap, membership drift and latency

### 6a. HOC data gap — root cause: a weekday calendar

The 2026-09-08 assessment recorded:

```
data_gaps                 ["PRIOR_RANK_UNAVAILABLE"]
previous_ranking_state    UNAVAILABLE
previous_ranking_source_date  2026-09-07
assessment_state          DEGRADED
```

**2026-09-07 is Labor Day.** `engine.market_session.previous_trading_day` is
weekday-only unless it is handed the authoritative closure set, and
`api.holding_opportunity_cost` was not handing it one. So the "previous eligible
session" was a day the exchange did not trade, for which no assessment can ever
have been persisted — an unfillable gap on a session whose real predecessor,
2026-09-04, had a perfectly good artifact. It is the same defect class Release
60.1 fixed for session eligibility, in a module R60.1 did not reach.

The R60.1 supplier (`engine.exchange_calendar`) now answers, here and in
`api.portfolio_state._previous_trading_day_iso`. When it cannot answer for the
window the documented weekday-only behaviour is unchanged and the reason states
which calendar decided, so a degraded answer is never mistaken for an
authoritative one.

### 6b. Membership drift — classified, with names

`DAILY_CLOSE_COMPLETE_MEMBERSHIP_DRIFT` on 2026-09-08 carried
`proposed_change_count: 20` and gate outcome `MEMBERSHIP_DRIFT_DETECTED`. The
token alone could not separate two very different things, so
`api.daily_close.classify_membership_drift` now does:

| class | meaning | benign |
| --- | --- | --- |
| `NO_MEMBERSHIP_DRIFT` | held names match the target membership | yes |
| `LEGACY_RANK_MEMBERSHIP_COMPARISON` | held names differ from the top-ranked names — what the comparison measures | yes |
| `HELD_NAME_ABSENT_FROM_SCORING_UNIVERSE` | the model cannot evaluate a name the book owns | **no — integrity** |
| `MEMBERSHIP_INTEGRITY_NOT_VERIFIABLE_THIS_SESSION` | no evaluated decision scope; the question was not answered | no |

It names the **exact affected tickers** (target-not-held, held-not-in-target,
resized, and held-but-unscored) rather than a count, publishes the scoring
universe / decision scope / holding / open-order counts, and is **fail-closed on
the benign claim**: it never says "compatibility only" without having checked.
Close validity is unaffected either way — `membership_drift` remains in
`CLOSE_VALIDITY_EXCLUDED_INPUTS`, and a portfolio finding can never reopen an
operational close that already happened.

### 6c. Latency — the two halves of the R61 fix cancelled out

The Sep-8 governed record's own latency block says
`missing_measurements: []`, `latency_measurement_complete: true` and
`not_required_measurements: [event_cycle_started_at, observation_received_at]` —
and simultaneously `interval_dispositions` says `MISSING` for three of four
intervals. Two defects sat behind that contradiction:

1. **The read model could not see the declaration.** R61 taught the producer to
   excuse the endpoints a session-terminal daily decision never had, which
   removed them from `missing_measurements` — and `_latency_lane_scope` derived
   the structurally-absent set from `missing_measurements` alone. It came out
   empty, so the LATENCY acceptance row fell through to MISSING and the contract
   read 9/10 against a decision complete on its own terms. It now reads the
   producer's own `not_required_measurements` as well.
2. **The declaration was incomplete, and the census was partial.** Every
   endpoint the daily lane excuses is an EVENT CYCLE concept — the producer's own
   `stage_step_map` resolves each to a step of an event cycle — and R61 named two
   of five. `DAILY_LANE_ABSENT_LATENCY_STAGES` completes it. And
   `measure_decision_latency` iterated only the timestamp keys a caller happened
   to pass, so a producer that passed none claimed completeness while three
   intervals were MISSING; every interval endpoint is now in its own census.

**Not gaming the count.** The LATENCY row is decided on **its own key-fact
interval** (`observation_to_signal_seconds`), not on the whole record: it is
NOT_APPLICABLE only when an endpoint of that interval is one the lane never had
AND neither of its endpoints is a real, expected gap. An INTRADAY decision owns
every endpoint, so a gap there is still MISSING. The acceptance contract now
publishes `row_count`, `applicable_row_count`, `present_count`, `missing_count`
and `not_applicable_count`, and proves the two identities it closes under:

```
applicable = present + missing
total      = applicable + not_applicable
```

The same three-disposition arithmetic is published by both governance gates
(`checks_applicable` / `checks_passed` / `checks_failed` /
`checks_not_applicable`, with `counts_are_closed`).

---

## 7. Sep-8 governed decision — immutability

`CURRENT_NO_CHANGE`, session `2026-09-08`, record
`gdec_2026-09-08_alpha_paper_book_1_c329a4e57fa9`, turnover 0, cost 0, NAV
~97,496.72, cash ~4,482.71, 25 holdings.

No code path in this release regenerates, changes, supersedes or replays it, and
none creates a target, proposal, order or fill or touches holdings, cash or NAV.
The live candidate that ran after it remains a duplicate and remains withheld.

---

## 8. Architecture invariants added

`check_release62_1_1_forward_activation_integrity`, 18 strict-blocking fields:

- exactly one operator adoption entrypoint; it delegates to
  `api.prospective_adoption` and never imports or writes the registry;
- explicit confirmation required; explicit execute flag required; the boundary
  is derived and not supplied;
- no backdate argument in any spelling; no adopt-all path;
- a withdrawn lifecycle fails closed in **both** owners;
- one current-collection owner, no second resolver, no historical runtime
  deciding current health, no browser-published read-failure verdict;
- the exact candidate target identity chain, and a lane that decides no verdict;
- latency NOT_APPLICABLE semantics with no backfill;
- no portfolio, execution or promotion path in the entrypoint or the owner.

---

## 9. Remaining gaps

1. **The four R58 adoptions have not been performed.** This release ships the
   door, not the act. Until the live command below is run, the estate still holds
   zero canonical forward registrations.
2. **The daily lane does not stamp its own stage instants.** It declares the
   event-cycle endpoints structurally absent — true, because it runs no event
   cycle — rather than measuring an equivalent daily-lane chain. A daily-lane
   latency vocabulary of its own would be better and is not in this release.
3. **The heavy collection composition is unchanged.** Only the CURRENT verdict
   was moved off it; `/v1/operations/information-collection` still reads the
   event index and the attention universe on every call.
4. **Accrual is still not driven.** The registrar starts measurements and names
   the accrual owner; wiring the scheduled emission for canonical weight-book
   challengers remains a separate bounded slice.

---

## 10. Operator commands

### Deploy (canonical owner; never re-enter PowerShell to run it)

```powershell
$SmokePaths = @(
    '/v1/operations/workflow-state',
    '/v1/operations/information-collection',
    '/v1/operations/daily-close',
    '/v1/operational-book',
    '/v1/operations/portfolio-reassessment'
)

& C:\Users\binis\paper_trader\scripts\restart_paper_trader_backend.ps1 `
    -Force `
    -Port 8001 `
    -SmokePath $SmokePaths
```

### R58 dry run (writes nothing)

```powershell
& C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe `
    C:\Users\binis\paper_trader\scripts\adopt_prospective_freeze.py `
    --challenger-id R58_SHORT_VOLUME_PRESSURE_V1 `
    --challenger-id R58_DISCLOSURE_INTENSITY_V1 `
    --challenger-id R58_FUND_MOMENTUM_VETO_V1 `
    --challenger-id R58_FCF_PURE_V1
```

Expect `FORWARD_ADOPTION_DRY_RUN_OK`.

### R58 live adoption

```powershell
& C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe `
    C:\Users\binis\paper_trader\scripts\adopt_prospective_freeze.py `
    --challenger-id R58_SHORT_VOLUME_PRESSURE_V1 `
    --challenger-id R58_DISCLOSURE_INTENSITY_V1 `
    --challenger-id R58_FUND_MOMENTUM_VETO_V1 `
    --challenger-id R58_FCF_PURE_V1 `
    --confirm ADOPT_PROSPECTIVE_FORWARD_CLOCKS `
    --execute
```

Expect `FORWARD_ADOPTION_OK`.

### Post-adoption verification (read-only)

```powershell
& C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe `
    C:\Users\binis\paper_trader\scripts\adopt_prospective_freeze.py `
    --challenger-id R58_SHORT_VOLUME_PRESSURE_V1 `
    --challenger-id R58_DISCLOSURE_INTENSITY_V1 `
    --challenger-id R58_FUND_MOMENTUM_VETO_V1 `
    --challenger-id R58_FCF_PURE_V1 `
    --json
```

Expect `FORWARD_ADOPTION_DRY_RUN_OK` with all four `ALREADY_REGISTERED`, and
`canonical_forward_registration_count = 4` on
`GET /v1/research/alphaagent-outcomes`.

The withdrawn candidate must never be passed to any of these commands; if it is,
the expected result is `FORWARD_ADOPTION_REFUSED` with lifecycle `WITHDRAWN`.
