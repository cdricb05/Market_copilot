# Release 54.2.3.1 — Owned-Data Readiness Authority Reconciliation

Hotfix over R54.2.3. Removes the circular Sep-2 close blocker by reconciling the
two owned-data authorities the operator surfaces were reading as one.

## 1. The live contradiction (2026-09-02 ~17:36 ET)

For the owed Sep-2 close, one page carried, simultaneously:

| Surface | Claim |
| --- | --- |
| `api.daily_close` | `DAILY_CLOSE_DUE`, provider `OWNED_EODHD_LIVE` live-probed `READY` through `2026-09-02`, valuation 26/26 and decision 199/199 complete |
| `api.workflow_state` | `WAITING_FOR_OWNED_DATA`, blocker `OWNED_DATA_NOT_CONFIRMED` ("Owned data confirms only 2026-09-01") |
| the same workflow payload | `daily_close_gate.execution_allowed = true`, `operator_command.portfolio_cycle_actionable = true` |
| Today | "BLOCKED — OWNED_DATA_NOT_CONFIRMED" + "CATCH UP WAITING FOR OWNED DATA" + an "OWNED DATA READY" badge + a green "Run the portfolio cycle" CTA |

The UI derived none of this; every claim was published by the backend. Two
defects produced it, both in the workflow composition:

1. **A category error.** The workflow owner is probe-free by contract, so it
   read the *persisted* desk-mark confirmation date (which advances only when a
   close runs, and is therefore always one session behind before every close)
   as if it answered "does the provider have the owed session's data?" — a
   question only `api.daily_close`'s live probe answers, and had already
   answered `READY`. Requiring Sep-2 to be persisted before allowing the close
   that persists Sep-2 is circular.
2. **Two verdicts in one payload.** Stage 19.3 deliberately promoted an
   executable Daily Close in `WAITING_FOR_OWNED_DATA` (the close revalidates
   server-side and fails safe), while the same state emitted the
   `OWNED_DATA_NOT_CONFIRMED` blocker — a blocked banner beside a green CTA,
   by design then, a contradiction now.

`operator_presentation._session_recovery` had already been composing both
owners' answers side by side, with the documented philosophy "a disagreement is
shown rather than resolved". This release resolves it at the source.

## 2. The canonical readiness contract for an owed close

Two owned-data concepts, two names, never interchangeable:

| Concept | Field | Owner | Answers |
| --- | --- | --- | --- |
| Persisted owned-data confirmation | `owned_data_confirmation_date` (+ `owned_data_confirmation_is_persisted_state: true`) | desk marks via `engine.market_session` evaluation | which completed session has already been operationally **processed** |
| Live provider coverage | `owned_provider_coverage` / `provider_covers_recovery_session` / `provider_coverage_session` | **`api.daily_close`** | does the authoritative owned provider currently hold the EOD data the owed close needs |

The ONE coverage calculation is `api.daily_close.provider_covers_session(
provider_readiness, session, market_data_scope=…)` → `True` / `False` /
`None` (no answer observed). It also folds in the close owner's market-data
scopes when supplied: a session the provider has published but whose valuation
or decision scope is incomplete is **not coverable**.

For an owed completed session S:

* calendar says S is owed (`engine.market_session.completed_sessions_after`
  over the close journal — unchanged R54.2.1 arithmetic), **and**
* the close owner's probed answer covers S (`provider_latest_date >= S`,
  readiness affirmative, supplied scopes complete)

→ recovery is `CATCH_UP_REQUIRED` with the new data state
`PROVIDER_CONFIRMED_AWAITING_CLOSE`, the priority policy routes to
`READY_FOR_DAILY_CLOSE`, and the ONE portfolio cycle is actionable. The
persisted mark remaining on S-1 is the *expected* pre-close state and is
labelled as such — never as unavailability.

When the provider answer is **affirmatively negative** (behind, unavailable,
probe failing, incomplete scope) — or when **no answer was observed at all** —
the workflow fails closed: `WAITING_FOR_OWNED_DATA`, recovery
`CATCH_UP_WAITING_FOR_OWNED_DATA`, primary action a non-executable "Wait for
owned market data", `daily_close_gate.execution_allowed = false`,
`portfolio_cycle_actionable = false` with the blocking reason naming the
provider's own verdict. Both directions come from one decision, so no surface
can disagree.

## 3. How the answer reaches the probe-free workflow owner

`api.workflow_state` remains probe-free (its GET contract: "invokes no provider
network call"). It gains `provider_readiness=` / `market_data_scope=` inputs
and consumes the close owner's verdict verbatim; it never recomputes coverage.
Every composition supplies the answer:

* **`api.decision_snapshot._compose`** (serves every GET surface) now loads
  `api.daily_close` **before** `api.workflow_state` and passes
  `daily_close["provider_readiness"]` / `["market_data_scope"]` in — the live
  probe runs once per snapshot build (bounded by the existing 180-second
  snapshot age valve, so a provider publish surfaces within ≤3 minutes exactly
  as the close headline always has).
* **`api.portfolio_cycle._workflow_loader_default`** (the POST path) supplies
  the new bounded read-only `api.daily_close.assess_owned_provider_readiness()`
  — the same `_run_probe` + `_provider_readiness` assembly as the close GET,
  without loading the book/gate/engine/performance. A failing probe degrades to
  an affirmative `PROVIDER_UNAVAILABLE` block, which fails closed.
* **`api.operator_presentation.owner_loaders`** shares ONE daily-close read
  between the workflow loader and the presentation's own daily-close section.

`_session_recovery` in the presentation now prefers the workflow's *published*
verdict (`provider_covers_recovery_session`); its inline date comparison
survives only as a degrade fallback for payloads predating this release.

## 4. Supersessions (explicit)

* **Stage 19.3's unconditional close promotion in `WAITING_FOR_OWNED_DATA` is
  superseded.** A provider-covered owed close is `READY_FOR_DAILY_CLOSE` now,
  so `WAITING_FOR_OWNED_DATA` only remains when the provider answer is negative
  or unobserved — and it promotes nothing. One residue stays executable: the
  never-persisted bootstrap (no owned session confirmed at all) with the
  provider affirmatively covering the expected session, because the close is
  the only path that can bootstrap owned marks.
* **`api.portfolio_cycle.plan_next_step`** gains the explicit fail-closed stop
  `STOP_WAITING_FOR_OWNED_DATA` (previously this state fell through to the
  close-kind branch; a non-executable WAITING payload would otherwise have been
  reported as "unrecognised state").
* **`RECOVERY_DATA_STATES`** is extended (not rewritten) with
  `PROVIDER_CONFIRMED_AWAITING_CLOSE`. `CONFIRMED` keeps its persisted-marks
  meaning.

## 5. What did NOT change

* The Daily Close still revalidates provider readiness **server-side
  immediately before any write** (`run_daily_close` step 3: probe →
  `WAITING_FOR_MARKET_DATA`, no write; step 5: marks that cannot reach the
  session → `DATA_BLOCKED`, no write, no decision row).
* One orchestration path: `POST /v1/operations/portfolio-cycle/run` with the
  explicit `RUN_PORTFOLIO_CYCLE` token; the server still binds the **oldest**
  owed session; no recovery route, no force-close, no operator-supplied date,
  no synthetic marks, no holiday inference, no order/fill/broker reach, no
  automation.
* The R54.2.1 morning semantics with no provider answer composed:
  `CATCH_UP_REQUIRED` with `UNVERIFIED_UNTIL_CLOSE_REVALIDATES` (the close
  revalidates). An *affirmative* negative answer now waits even in the morning.
* `engine.market_session` is untouched; `_decide_overall`'s ladder is untouched
  (the composition passes the reconciled lag verdict through the existing
  `owned_data_lag` input).

## 6. Live verification (read-only, production stores, no restart)

Through the repaired composition at investigation time:

* `api.daily_close`: `DAILY_CLOSE_DUE`, provider `READY` through `2026-09-02`
  (queried live), scopes 26/26 and 199/199 complete.
* `api.workflow_state` with the readiness composed in:
  `READY_FOR_DAILY_CLOSE`, blockers `[]`, recovery `CATCH_UP_REQUIRED` for
  `2026-09-02` with `PROVIDER_CONFIRMED_AWAITING_CLOSE`,
  `portfolio_cycle_actionable true`, `daily_close_gate.execution_allowed true`,
  consistency `CONSISTENT`, `plan_next_step → DAILY_CLOSE`.
* Today panel: "CATCH UP REQUIRED" | owned data "READY" | "Run the Portfolio
  Cycle" — one message.
* Control (bare probe-free read, no readiness supplied): fails closed —
  `WAITING_FOR_OWNED_DATA`, not actionable, honest blocking reason.

## 7. Guard and test perimeter

* `scripts/audit_architecture.py` gains the blocking strict section
  `check_release54_2_3_1_owned_data_readiness_authority`: one coverage
  calculation (in the prober), probe-free workflow consuming the verdict,
  snapshot/orchestrator/presentation all supplying it, distinct concept names,
  the gate echoing the verdict it obeyed, and zero readiness derivation in the
  UI.
* New suite `tests/test_release54_2_3_1_owned_data_readiness_authority.py`
  (36 tests): the positive direction (owed + persisted-behind + provider READY
  → executable), every negative direction (behind / unavailable / failing probe
  / unprobed / incomplete valuation scope / incomplete decision scope → fail
  closed, not actionable), cross-surface no-contradiction invariants (state ↔
  gate ↔ CTA ↔ badge ↔ blocker), delegation (Active Manager State,
  presentation), safety (no recovery date/route, one close route, no
  order/fill/broker reach, automation off, the bounded assessment writes
  nothing), and the composition wiring.
* Updated to the superseding contract: `test_stage19_3…::test_32`,
  `test_operator_action_integrity` (WAITING promotion tests + harness world),
  `test_release48_operator_workflow` (plan map + underlying step),
  `test_release54_2_1…::test_36` (vocabulary extension), and the Stage-22 /
  Release-48 audit token homes.
