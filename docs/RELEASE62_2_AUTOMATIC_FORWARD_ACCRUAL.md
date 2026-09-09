# Release 62.2 — The Automatic Forward Accrual Loop

**Branch:** `r62-1-1-forward-activation-integrity`
**Built over:** `b623161` (R62.1.1, deployed 2026-09-09)
**Worktree:** `D:\paper_trader_r62_1_1_forward_activation_integrity`
**Canonical checkout:** `C:\Users\binis\paper_trader`

---

## 1. The gap, stated plainly

Release 62.1 built the canonical registrar (`api.forward_challenger_registry`).
Release 62.1.1 built the one governed operator entrypoint that fills it
(`scripts/adopt_prospective_freeze.py`). On **2026-09-09** the four ACTIVE R58
freezes were adopted against the live estate, and the backend reported exactly
what it should:

```
canonical_forward_registration_count = 4
orphan_freezes_adoptable_count       = 0
```

**And then nothing would ever have happened.**

A registration NAMES its accrual owner and its maturation owner:

```json
"evidence_accrual_owner": "engine.shadow_portfolio_evidence (the one pure
                           weight-book forward accrual kernel), matured by
                           alpha_agent.r52.runtime",
"maturation_owner":       "alpha_agent.r52.runtime"
```

No code path connected the two. `alpha_agent.r46.advance` advances the R46
**contract cohort** — a fixed list of specifications hashed into
`contract_hash`, which a registration is not a member of.
`api.shadow_portfolio_evidence` advances a **session cohort** of complete paper
portfolios, and says so in its own docstring. Neither had ever opened the
registry. The R52 runtime's seven stages contained no stage that could.

A registered challenger was a clock nothing wound. The four counters
(`predictions_emitted`, `matured_observations`, `pending_observations`,
`effective_independent_observations`) were structurally zero, forever, and the
registrar's own read model would have reported
`ACCRUING_NO_MATURED_OBSERVATION_YET` for the rest of the estate's life.

---

## 2. What landed

### 2.1 `api/canonical_forward_accrual.py` — the accrual owner

It owns **exactly one thing**: whether a canonical registration has a legal,
unemitted prospective decision *right now*, and the durable record of what
happened to that opportunity.

Deliberately the smallest connection that can exist:

| It is NOT | Because |
|---|---|
| a second registry | registrations are READ from `api.forward_challenger_registry` — it writes none and amends none, and the audit blocks `register_forward_challenger(` appearing in it |
| a second signal implementation | the weight book is READ from the originating release's own immutable frozen artifact and bound to the registration by `freeze_record_hash`; a mismatch is refused, never repaired |
| a second P&L kernel | every return, cost and curve is `engine.shadow_portfolio_evidence`'s — the same pure kernel R56 uses |
| a second scheduler | `alpha_agent.r52.runtime` already owns the research cadence and calls it as one more stage, inside the runtime's **existing** lock |
| a second calendar | exchange-session classes resolve through the registrar's authoritative NYSE supplier; every other class reads the instrument's own realised bars |

### 2.2 The runtime stage

`alpha_agent/r52/runtime.py` gains stage **5b**, between the forfeiture sweep
and the velocity rebuild:

```
1. runtime_lock                     6. velocity_operational
2. timing_contract                  7. promotion_frontier
3. chain_integrity
4. tournament_advance      (R46, unchanged)
5. forfeiture_sweep        (R52, unchanged)
5b. canonical_forward_accrual   <-- R62.2
```

It maps onto the runtime's **own** frozen stage vocabulary — `SUCCESS`,
`NOT_DUE`, `DATA_BLOCKED`, `FORFEITED`, `FAILED_RETRYABLE` — and invents none.
It also supplies the CURRENT lifecycle verdict for every frozen challenger
(`_lifecycle_by_challenger`), read from the ONE lifecycle owner, so a challenger
withdrawn *after* registration stops accruing at the next invocation rather than
at the next release.

### 2.3 The read-model seam

Registration records are immutable and first-write-wins, so their counters are
zero for life **by design**. The living counters belong to the accrual owner and
are **overlaid**, never recomputed:

- `api.forward_challenger_registry.registration_row(record, accrual=…)` overlays
  exactly the fields in `ACCRUAL_OVERLAY_FIELDS` and nothing else.
- The **run** publishes the read model (`accrual_projection.json`) and
  `api.alphaagent_outcomes` READS it. Recomputing on the GET would load the
  operational price panel a **second** time on a route that already loads it
  once through the R56 owner — putting a heavy composition back on a read path
  is the defect R62.1.1 spent a workstream removing. The payload carries
  `canonical_forward_accrual_generated_at`, so a stale number cannot be read as
  a current one.
- `load_canonical_forward_accrual()` still exists as the honest, expensive
  recomputation (`execute=False`, guaranteed to write nothing) for operators and
  tests; the read route may not call it, and the audit blocks that.
- The registrar must not import the accrual owner and must not accrue. Both are
  blocking audit invariants.

---

## 3. The chain, end to end

```
canonical registration        api.forward_challenger_registry      (read)
  -> the frozen decision      the ORIGINATING release's artifact   (read, hash-bound)
  -> prospective emission     engine.shadow_portfolio_evidence     (kernel)
  -> forward accrual          engine.shadow_portfolio_evidence     (kernel)
  -> maturation               the instrument's own realised bars
  -> effective independent    non-overlapping matured windows
  -> a human evidence gate    never this module
```

### 3.1 The decision grid

The first decision session is the registrar's own
`first_eligible_observation_session` — for an exchange-session class — or, for
an instrument-calendar class, the first realised session strictly after
registration. Later entries are spaced by the frozen construction's own
`rebalance_cadence_sessions` on the same realised calendar.

**The first decision session is governed by the freeze that was ADOPTED.** Its
book was frozen before registration and uses only information available then,
which is the whole point: the challenger enters late, on a stale book, and is
measured strictly afterwards. That is a handicap, never a look-ahead.

**Every later cadence boundary is a NEW decision**, and this module may not take
one on the originating owner's behalf.

---

## 4. No backfill, and what forfeiture actually means

An emission for decision session `S` is legal **only strictly before `S`** — the
same shape as the frozen R46 entry rule, and the same shape as the originating
freeze's own inception rule ("the position is effective at the NEXT close"). The
position is entered at the close of `S` and the kernel scores only bars dated
strictly after it, so the emitter has seen neither the entry mark nor any
outcome.

Emitting once `S` had closed would be weaker in a way that matters. The book is
frozen, so nothing about **what** is emitted could change — but **whether** to
emit could, and an emitter that has seen the session it is stamping is an
emitter that could skip a bad one. The rule removes that possibility rather than
trusting nobody to use it.

| Situation | State | Why |
|---|---|---|
| `S` is the next boundary and still in the future | `DUE` → emit exactly once | the decision is made **before** its session |
| a later boundary whose turn has not come | `NOT_DUE` / `OBSERVATION_SESSION_NOT_REACHED` | one decision at a time |
| `S` has arrived or passed, unemitted | `FORFEITED` / `EMISSION_WINDOW_CLOSED_WHEN_THE_SESSION_BEGAN` | the opportunity is gone and **may never be reconstructed** |
| no governed decision was frozen for `S` | `NOT_DUE` / `AWAITING_NEW_GOVERNED_FREEZE` | **not** a forfeiture — there was nothing to emit |
| the panel cannot price ≥95% of the book at its latest session | `DATA_BLOCKED` / `INSUFFICIENT_PRICED_WEIGHT…` | a flat mark would claim the unpriced names did not move |
| hash mismatch, or a closed lifecycle | `INTEGRITY_BLOCKED` | refused, never repaired |

The date authority is the **same** one the registrar used to derive the
prospective boundary at adoption (`current_prospective_boundary` — today's UTC
date). One clock opened these registrations; the same clock decides whether
their sessions have arrived.

Every forfeiture row carries `backfill_refused: true`,
`evidence_is_deliberately_absent: true` and `may_never_be_reconstructed: true`.
Recording a forfeiture is the **opposite** of backfilling: it writes down that
the evidence does not exist and never will.

The fourth row is the one that keeps the count honest. Treating a cadence
boundary with no frozen decision as a forfeiture would manufacture losses out of
governance — the estate would report evidence it had never been entitled to.

---

## 5. The multi-asset contract

There is no `ticker` anywhere in this module. `instrument_scope` is whatever the
frozen book names, the asset class travels as data, and the observation calendar
is resolved per class by the registrar:

| Class | Calendar |
|---|---|
| `US_EQUITY`, `US_ETF` | the authoritative NYSE session calendar (`engine.exchange_calendar` + `engine.market_session`) |
| `EQUITY_INDEX_FUTURES`, `RATES_FUTURES`, `COMMODITY_FUTURES`, `FX_FUTURES`, `VOLATILITY`, `CROSS_ASSET`, `CREDIT`, … | the instrument's OWN realised bar calendar, read from the panel |

`test_10b` proves a futures challenger maturing on **2026-09-12, a Saturday** —
a date that is a legitimate session for a market that prints on it and an
impossible one for an equity book. Imposing NYSE sessions on a rates future
would be a fabrication, not a fix.

---

## 6. Safety

`RESEARCH ONLY`. The accrual owner promotes no model, activates no sleeve,
allocates no capital, creates no order, fill, proposal or approval, changes no
holding, cash or NAV, runs no daily close and calls no portfolio cycle. It
writes to its own research root
(`PAPER_TRADER_CANONICAL_FORWARD_ACCRUAL_DIR`) and to nothing else.

The audit checks **calls and imports, never bare words**: the module
legitimately declares `"called_portfolio_cycle": False` in its safety block, and
an invariant that failed on that string would only teach the next author to stop
declaring what the module does not do.

---

## 7. Gates

| Gate | Result |
|---|---|
| `tests/test_release62_2_automatic_forward_accrual.py` | 65 passed |
| Impacted batch 1 (R46 ×7, R52, R58, R59 ×2) | 786 passed |
| Impacted batch 2 (R61, R62.1, R62.1.1, R62.2, R56, R51, R60, R60.1, architecture, calendar/session, forward phases) | see §7.1 |
| `scripts/audit_architecture.py --strict` | exit 0, inventory drift zero |
| `git diff --check` | clean |

### 7.1 Blocking invariants added (16)

`accrual_owners` (exactly one) · `discovers_from_the_registrar` ·
`accrual_owner_registers` (empty) · `identity_bound_by_freeze_hash` ·
`second_signal_implementation` (empty) · `runtime_owns_the_cadence` ·
`second_scheduler` (empty) · `unexpected_task_definition_owners` (empty) ·
`forfeiture_refuses_backfill` · `emits_strictly_before_its_session` ·
`missing_freeze_is_not_a_forfeiture` · `accrual_states_declared` ·
`registrar_overlays_only` · `read_model_is_read_only` · `one_pnl_kernel` ·
`portfolio_or_execution_paths` (empty)

---

## 8. Live state at the end of this release

The four canonical registrations, adopted 2026-09-09:

| challenger | freeze | first eligible observation | horizon |
|---|---|---|---|
| `R58_SHORT_VOLUME_PRESSURE_V1` | `H_53942dbe_9b1d3530461a` | 2026-09-10 | 21 |
| `R58_DISCLOSURE_INTENSITY_V1` | `H_bf7c02b3_37d591c4b3c4` | 2026-09-10 | 21 |
| `R58_FUND_MOMENTUM_VETO_V1` | `H_1fd6bc4a_be453695ef98` | 2026-09-10 | 21 |
| `R58_FCF_PURE_V1` | `H_2782ad19_b442974dc467` | 2026-09-10 | 21 |

All four: `backfilled: false`, `predictions_emitted: 0`,
`matured_observations: 0`, `effective_independent_observations: 0`,
registrar `api.forward_challenger_registry`.

`R59_CALENDAR_TERM_STRUCTURE_F9BE2426` remains **WITHDRAWN** and unregistered;
the entrypoint refuses it with `REFUSED_NOT_ADOPTABLE_LIFECYCLE_STATE` before
any store is touched, and the registrar refuses it again independently.

---

## 9. Deployment

### 9.1 Restart the research runtime (loads the new accrual stage)

```powershell
& C:\Users\binis\paper_trader\scripts\manage_research_runtime.ps1 `
    -RepoRoot C:\Users\binis\paper_trader `
    -Action Restart `
    -Execute
```

### 9.2 Restart the backend

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

### 9.3 Verify the estate is ARMED (read-only)

```powershell
& C:\Users\binis\paper_trader\scripts\manage_research_runtime.ps1 -Action Status
```

Then `GET /v1/research/alphaagent-outcomes` and read:

```
prospective.canonical_forward_registration_count            = 4
prospective.canonical_forward_accrual_owner                 = api.canonical_forward_accrual
prospective.canonical_forward_predictions_emitted           = 4
prospective.canonical_forward_forfeitures                   = 0
prospective.canonical_forward_accrual_states                = ["EMITTED"]
```

**Two outcomes are both success.** If the runtime's first invocation after
deployment happens on or before 2026-09-09 UTC, the four registrations are `DUE`
and it emits their first prospective predictions immediately, stamped for
2026-09-10 — that is a legitimate emission made strictly before its session, not
a manufactured one. If it happens later, 2026-09-10 is `FORFEITED` (recorded,
never repaired) and the estate is `ARMED_FOR_NEXT_LEGITIMATE_FORWARD_SESSION`
for the next governed freeze. No observation is ever forced to prove production
behaviour, and none is ever written late.

---

## 10. What R62.2 does not do

- It does not recompute an R58 signal. A rebalance is a decision, and the
  originating owner takes it — not this module.
- It does not award an evidence gate, promote a model or activate a sleeve.
  Reaching a gate remains a human's verdict through the existing governance.
- It does not merge canonical forward evidence with R46 or R56 evidence. Those
  are different evidence identities and summing them is exactly the mistake the
  next release must keep preventing.
