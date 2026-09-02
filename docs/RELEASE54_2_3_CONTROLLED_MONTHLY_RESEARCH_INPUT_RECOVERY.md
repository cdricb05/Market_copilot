# Release 54.2.3 — Controlled monthly research-input recovery

**Status:** implemented, tested, not committed.
**Base:** `dfe8eedb` (R54.2.2), branch `stage19-controlled-rebalance`.

R54.2.2 separated operational validity from governed research and then named, precisely,
the thing it could not do:

> "a CONTROLLED owned-panel refresh bounded to that session has run" —
> `operator_action: REFRESH_OWNED_SOURCE_PANEL_THEN_RUN_RESEARCH_MONTHLY_INPUT_EMITTER`

This release builds exactly that, and nothing else.

---

## 1. What was actually stale

One artifact, with one owner:

| | |
|---|---|
| Artifact | `D:\Stock_Prediction_app_data\phase24_cache\daily_panel\russell1000_cp_daily.npz` + `manifest.json` |
| Owner | `research.phase24_daily_panel` |
| `last_date` | **2026-08-05** (written 2026-08-05 21:13) |
| Content | Norgate "Russell 1000 Current & Past", TOTALRETURN, 2000-01-03 →, 3,076 securities, 6,687 trading days |

Everything downstream inherited that date: `current_momentum_scores.csv` carried
`month_label 2026-08`, `market_as_of_date 2026-08-05`; the September monthly input was
due and could not be produced; `alpha_target.run_refresh` refused to cross the month
boundary (`R_MONTH_BOUNDARY`); the Daily Research Cycle for 2026-09-01 could not run.

## 2. Why it stopped — the systemic cause

Not a scheduler failure, not a provider outage, not an entitlement problem, not a
date-selection bug, not a persistence failure. **No owner was responsible for advancing
it.**

* `build_daily_panel_from_norgate` is a **one-time acquisition**: it returns early when
  the NPZ already exists (`if os.path.exists(NPZ_PATH) and not force and limit is None`),
  and with `force=True` it does an unbounded full rebuild to the provider's *latest*
  observation.
* The only operational consumer, `api.monthly_momentum_emitter`, stated in its own
  docstring that it "NEVER triggers a panel refresh", because an uncontrolled full
  rebuild on the daily cycle would have been worse than blocking.
* So the panel advanced exactly once — on the day a human ran it — and every subsequent
  month became a permanent blocker clearable only by a hidden manual step.

Classification: **missing orchestration integration.** The repair therefore has to create
an owner for the maintenance, not just perform the maintenance.

## 3. Provider coverage (measured, read-only)

Owned local Norgate Data Director, NDU v1.0.74, databases include `US Equities` and
`US Equities Delisted`; last database update `2026-09-02 14:00 ET`.

| Probe | Result |
|---|---|
| AAPL / MSFT / JNJ / XOM daily bars | through **2026-09-01** (incl. 2026-08-31) |
| `index_constituent_timeseries` (PIT membership) | through **2026-09-01** |
| Watchlist "Russell 1000 Current & Past" | 3,597 symbols |
| Watchlist "Russell 1000" | 1,015 symbols |
| Measured pull rate | ~64 symbols/second (local database) |

The provider has everything the September session needs. The wall was never data
availability.

## 4. The point-in-time cutoff September actually requires

`mom_6_1` for month bucket *m* is `close[m-1] / close[m-7] - 1` on **month-end**
total-return closes. For the 2026-09 bucket that is:

```
close[2026-08-31] / close[2026-02-28] - 1
```

So the **momentum number sees information only through the 2026-08-31 close** — the
skip-one-month construction means the partial September has no influence on it.

The surrounding fields are as-of the session: PIT index membership, ADV, realized vol
and history counts are taken at the panel's last date, and the emitter's validation
requires `market_as_of_date == the eligible session`. Therefore:

> **The required panel cutoff is exactly the eligible session — 2026-09-01 — and never
> later.** Momentum from data through Aug-31; membership and diagnostics as of Sep-1;
> nothing after Sep-1.

This is why an unbounded rebuild is not merely wasteful but *wrong*: run on 2026-09-03 it
would produce a panel dated 2026-09-02, which the emitter correctly rejects as
`MONTHLY_PANEL_FUTURE_DATED` for a Sep-1 research session.

## 5. The bounded panel-refresh contract

Added to the **existing panel owner** (there is no second panel writer):

```python
research.phase24_daily_panel.refresh_daily_panel_as_of(as_of, ...)
```

| Property | How |
|---|---|
| Internal as-of cutoff | `end_date=as_of` passed to **both** `price_timeseries` and `index_constituent_timeseries` |
| No future rows | plus a post-assembly truncation `close_df.index <= as_of` (defence in depth) |
| Delisted / inactive retained | universe is the *Current & Past* watchlist, unchanged |
| PIT membership | per-symbol per-day constituent series, unchanged; the current watchlist is only a name *superset* and later joiners carry `member = 0` on earlier dates |
| Deterministic / idempotent | same cutoff → same `last_date`, same symbol set |
| Fails closed | `SOURCE_PANEL_INCOMPLETE`, `SOURCE_PANEL_FUTURE_DATED`, `HISTORICAL_UNIVERSE_COVERAGE_FAILED` — raised *before* promotion |
| No silent row loss | refuses when `securities_pulled` or `n_trading_days` would fall below the panel being replaced |
| Atomic | temp NPZ + temp manifest, `os.replace` only after every check passes — a failed refresh leaves the previous panel intact |

**Measured, hermetically** (scratch paths; the production panel untouched):

| | production panel (Aug-5) | bounded refresh to 2026-09-01 |
|---|---|---|
| `last_date` | 2026-08-05 | **2026-09-01** |
| trading days | 6,687 | **6,706** (+19 sessions) |
| securities | 3,076 | **3,076** |
| symbols missing | 521 | 521 |
| coverage fraction | 0.5407 | 0.5406 |
| rows later than cutoff | — | **0** |
| build time | — | **93.6 s** |
| retained inactive names | — | 1,459 |
| members on the last day | — | 1,015 |
| members now, absent in 2000 | — | 718 |

## 6. September's frozen monthly input — produced, hermetically

Running the unchanged Phase-25 mathematics over the bounded panel:

```
market_as_of_date    2026-09-01
current_month_label  2026-09
rows                 1008   (0 null mom_6_1, 0 duplicate tickers, 1008 eligible)
mom_definition       mom_6_1 = close[m-1]/close[m-7]-1 on month-end TOTALRETURN closes
```

Recomputed independently from the panel for the first ticker:
`close[2026-08-31] / close[2026-02-28] - 1 = 0.27044346`, identical to the emitted value.
The formula, the artifact schema and the identity columns are unchanged; the adapter's
first-write-wins promotion, conflict refusal and reuse-on-rerun are untouched.

## 7. Where the refresh is driven from

`api.monthly_momentum_emitter` — the bridge that already owned the source-panel policy —
gains the **policy**, not the mathematics:

```
inspect_source_panel(eligible)
  panel covers the session            -> USE_EXISTING      (no provider call)
  panel behind, refresh available     -> REFRESH_BOUNDED   (as_of = the eligible session)
  panel behind, refresh unavailable   -> BLOCK             (unchanged, honest)
  panel ahead of the session          -> BLOCK             (never rebuilt backwards)
  panel unverifiable                  -> BLOCK             (unchanged)
```

`production_emitter` performs **at most one** refresh per emission attempt, then
**re-inspects the panel on disk** — the decision to emit rests on the manifest the
refresh actually produced, never on the fact that a refresh ran.

A future-dated panel is deliberately *not* repaired by rebuilding: doing so would discard
observations a later session legitimately holds.

## 8. One orchestration path

No new route, button, workflow or date picker. The operator runs the same single command:

```
Run the portfolio cycle
  -> Daily Close (if due)
  -> Daily Research Cycle
       -> refresh required inputs
            momentum_monthly -> adapter -> bridge -> [bounded panel refresh] -> Phase 25
            price_score_refresh -> alpha_target.run_refresh (retried after the monthly)
       -> scoring -> Holding Opportunity Cost -> reassessment -> proposal
  -> governed portfolio decision (manual review; nothing approved or executed)
```

The prerequisite maintenance is surfaced on the step that performs it
(`prerequisite_maintenance: BOUNDED_SOURCE_PANEL_REFRESH`), so it is visible without being
a separate operator action. The DRC already retried the price refresh after a monthly
emission, so nothing in the orchestration needed to change.

## 9. Portfolio-cycle actionability

**The live UI defect was a backend truth defect.** The UI already read
`primary_action_available` and inferred nothing; the backend published `true`.

Root cause: two derivations of one fact disagreed.

* `build_execution_plan` asked only *"is a monthly emitter wired?"* → yes → planned an
  automatic refresh → plan not blocked → DRC `NOT_STARTED`, executable →
  `RESEARCH_CYCLE_REQUIRED` → green **Run the portfolio cycle**.
* `classify_input_recovery` also knew `source_panel_covered = False` → `TRUE_BLOCKER` →
  `RESEARCH_OBLIGATION_BLOCKED` → the banner said "cannot run yet: momentum_monthly".

Both statements came from the same payload, in the same response.

The repair is a single source of truth: the monthly owner publishes **its own verdict**,
`can_cover_eligible_session`, and both the plan and the classification read it. The
canonical rule is therefore:

> **The portfolio cycle is actionable exactly when the decided primary action is an
> executable normal-path mutation. A research input that the owner knows cannot be
> produced for the session makes the plan blocked, which makes the cycle blocked, which
> withholds the CTA.**

`build_operator_command` now also publishes that verdict under explicit names —
`portfolio_cycle_actionable`, `portfolio_cycle_safe_to_execute`,
`portfolio_cycle_action_code`, `portfolio_cycle_action_label`,
`portfolio_cycle_blocking_reason` — as **aliases of `primary_action_available`**, not a
second calculation. Note that the primary action's older `safe_to_execute` flag answers a
different question ("may this run without a confirmation token?") and is `False` for every
normal-path mutation by design; it was never the actionability gate and is not repurposed.

Because the bounded refresh now exists, the *correct* answer for 2026-09-01 is that the
cycle **is** actionable — the green CTA is honest, and the blocker banner disappears
because the blocker is gone, not because it was hidden.

## 10. Data-quality vocabulary

One backend-decided state per stage, published on the cycle status as
`research_input_quality`. The UI reads it; it performs no date arithmetic.

| Stage | States |
|---|---|
| Source panel | `SOURCE_PANEL_CURRENT`, `SOURCE_PANEL_STALE`, `SOURCE_PANEL_REFRESHING`, `SOURCE_PANEL_INCOMPLETE`, `MONTHLY_PANEL_FUTURE_DATED`, `MONTHLY_PANEL_COVERAGE_UNVERIFIABLE`, `HISTORICAL_UNIVERSE_COVERAGE_FAILED` |
| Monthly input | `MONTHLY_INPUT_MISSING`, `MONTHLY_INPUT_CURRENT`, `MONTHLY_INPUT_DUE`, `MONTHLY_INPUT_FUTURE_DATED`, `MONTHLY_INPUT_UNRECOVERABLE` |
| Price / score | `PRICE_REFRESH_READY`, `PRICE_REFRESH_WAITING_ON_MONTHLY` |

The established `status` spellings (`MONTHLY_PANEL_BEHIND_ELIGIBLE`,
`MONTHLY_PANEL_CURRENT`) are unchanged; `panel_state` carries the canonical name beside
them, set in one place so the two cannot drift.

## 11. Historical-gap policy

Sep-1 is **Outcome A: safely recoverable** — the panel advances to 2026-09-01, the
September frozen input is reproducible, and the normal Portfolio Cycle can run the Sep-1
research cycle.

What remains permanently gapped is unchanged and untouched: the **Sep-1 TRUE_FORWARD
snapshot** was never captured and is not reconstructable. It is documented, it does not
invalidate the operational close, and nothing in this release writes, back-fills or infers
a forward observation.

A historical gap never freezes later sessions: due-ness is `eligible_month >
current_input_month`, a comparison against the session in hand, not a queue of unfinished
months. An unrecoverable September would leave October to proceed on its own evidence.

## 12. Future new-month operation

Entirely generic — no production module contains a literal date (asserted by AST over
`api/monthly_momentum_emitter.py`, `api/monthly_momentum_input.py`,
`api/daily_research_cycle.py`).

On the first eligible session of any month *M*: the panel is behind → one bounded refresh
to that session → the month boundary is detected (`eligible_month > current_month`) → the
frozen feature is emitted for *M* → the price/score refresh proceeds → scoring runs. The
tests exercise the September transition, a synthetic October transition and a year
boundary through the same fixture shape.

## 13. Test perimeter

Risk-based, per the release brief: the dedicated R54.2.3 suite, the exact modified owners'
suites, and the direct downstream consumers of the changed owners. The changed surface is
a research-input maintenance path plus one presentation projection — no canonical
persistence schema, no order/fill/execution path, no NAV or portfolio mathematics, no
route registration, no shared model identity, no package infrastructure.
