# Release 30 — Zero-Base Adaptive Alpha Capital Allocation

> Canonical release document. Read with
> [PROJECT_CHARTER.md](PROJECT_CHARTER.md) ·
> [CURRENT_ARCHITECTURE.md](CURRENT_ARCHITECTURE.md) ·
> [TARGET_ARCHITECTURE.md](TARGET_ARCHITECTURE.md) ·
> [ARCHITECTURE_DECISIONS.md](ARCHITECTURE_DECISIONS.md)

## The rule this release enforces

*Ownership is not an investment thesis.* Every portfolio construction path in the
system before this release started from the current holdings and asked what to
change. That question quietly grants an incumbent name a status no evidence gave
it. Release 30 replaces the question with the one a portfolio manager actually
asks — **"if all of this were cash right now, what would we buy?"** — and then
prices the transition separately.

## The two objects, which are never conflated

| Object | Question it answers | Sees current holdings? |
|---|---|---|
| **ZERO-BASE TARGET** | The intrinsic desired allocation. If every investable dollar were cash at this decision timestamp, which eligible names and how much cash would we choose? | **No.** Not an input. |
| **IMPLEMENTABLE TARGET** | The same objective solved FROM the current book with transaction costs inside the economics. | **Yes** — and only here. |

Holdings influence the **economics of transition**, never which assets are
intrinsically attractive. `engine/zero_base_allocator.py` owns both and computes
them with the same objective, the same constraints and the same optimiser; the
only difference is the presence of the cost term and the starting point.

## Three cycles, unchanged in cadence, extended in content

| Cycle | Cadence | What Release 30 adds |
|---|---|---|
| **Signal refresh** | FREQUENT | a forward-return FORECAST refresh alongside the existing score refresh |
| **Portfolio reassessment** | FREQUENT | zero-base target recalculation + transition economics feeding the existing HOC → reassessment → proposal → decision chain |
| **Model recalibration** | CONTROLLED | the forecasting ensemble's weights and risk prices change only at an evidence checkpoint, never per event |

## Ownership — what was reused, extended and added

### REUSED unchanged (called as owners, never forked)

`engine/market_session` · `api/data_freshness` · `api/universe_scoring` ·
`api/multi_horizon_engine` (construction constants) · `api/paper_trading_desk`
(cost model) · `api/portfolio_state` · `api/holding_opportunity_cost` ·
`engine/portfolio_reassessment` · `engine/reallocation_proposal` ·
`api/portfolio_decision` · `api/rebalance_execution` (Stage 19, untouched) ·
`engine/event_fabric` + `api/event_signal_refresh` · `api/information_collection` ·
`api/forward_evidence` + `api/forward_prediction_skill` ·
`alpha_agent/stage24_pit_fundamental` (the point-in-time reader, identity bridge
and reporting-lag policy) · `alpha_agent/stage25_alpha_discovery`
(`s25_operating_profitability`) · the owned Phase-24 survivorship-free daily panel.

### EXTENDED (consolidation, not addition)

| Module | Change | Why it is a consolidation |
|---|---|---|
| `engine/holding_opportunity_cost.py` | extracted `build_covariance()`; `compute_risk_contributions()` now calls it | the allocator optimises against the SAME matrix the risk contributions are read from. A second covariance builder would be a second risk owner. |
| `api/price_panel.py` | added `aligned_returns()`; `api/reallocation_proposal._aligned_returns` delegates to it | one definition of "the trailing return series", used by both the Slice-7 proposal and the allocator. |
| `alpha_agent/stage24_pit_fundamental.py` | added public `gross_profit` and `pit_as_of()` | Release 30 reuses the released gross-profit fallback and the reporting-lag POLICY instead of restating either. Reaching for the private names would have been the silent fork these aliases prevent. |
| `api/app.py` | four new GET routes | read-only surfaces; no new POST, no new mutation. |
| `api/ui/index.html` | three new read-only regions | see the wireframe below. |

### NEW

| Module | Kind | Owns |
|---|---|---|
| `engine/return_forecast.py` | stdlib kernel | what a forward-return forecast IS: target quantity, horizons, model application, uncertainty, identity hashes |
| `api/return_forecast.py` | composition / read | artifact + input validation, activation state, forward-evidence capture |
| `engine/zero_base_allocator.py` | stdlib kernel | the objective, the constraints, the optimiser, transition economics |
| `api/zero_base_target.py` | composition / read | binds live canonical constants, assembles the input contract |
| `api/material_information.py` | read model | the capital-impact feed (owns no calculation) |
| `api/alpha_leaderboard.py` | read model | the model leaderboard (owns no calculation) |
| `alpha_agent/release30_panel.py` | research | the point-in-time feature/label panel |
| `alpha_agent/release30_models.py` | research | the learners (numpy) |
| `alpha_agent/release30_forecast_research.py` | research | walk-forward tournament, ensemble weights, risk-price calibration |
| `alpha_agent/release30_forecast_emitter.py` | research | the feature cross-section bridge |
| `scripts/run_release30_zero_base_research.py` | research runner | isolated Release 30 root |

### DEPRECATED

Nothing is deleted. The legacy **25-name target position count** is identified as
a *construction default of the equal-weight book*, not an investment constraint:
no evidence says 25 is the right number of names. It is retained in
`engine/reallocation_proposal` (whose behaviour is unchanged), declared in the
allocator as `LEGACY_TARGET_POSITION_COUNT`, reported in every payload, and does
not bind the zero-base objective — where the position count is emergent from the
weight caps.

## The forecasting layer

**The modelled quantity is a forward RETURN, never a price level**, and
specifically the forward TOTAL return minus the equal-weight mean of the same
eligible cross-section. A cross-sectional model cannot forecast the market's own
level, so that level is removed from the target rather than credited to the
model. `MARKET_BASELINE_POLICY = "MARKET_LEVEL_NOT_FORECAST"` states this in the
payload, and it is why cash competes against forecast *excess* return net of risk.

Horizons: **5 / 20 / 60 trading sessions** — sessions, not calendar days, because
every owned input is session-indexed.

Per name and horizon the layer exposes: `expected_return`,
`expected_excess_return`, `forecast_uncertainty`, `downside_return_q05`, `rank`,
`model_spec_hash`, `feature_snapshot_hash`, `training_cutoff`,
`point_in_time_status`.

### Why training lives in the research lane

`api/` and `engine/` are stdlib-only by long-standing convention, so the backend
imports cheaply. Fitting needs numpy. Release 30 therefore reuses the Phase-29D.2
monthly-momentum pattern exactly: research owns the mathematics and EMITS a frozen
artifact; the stdlib kernel owns validation, application and semantics.

## The objective, stated once

Over long-only weights `w` with cash `1 - sum(w)`:

```
U(w) = mu'w
       - (gamma/2) * w' Sigma_h w                 covariance risk
       - (phi/2)   * sum_i w_i^2 sigma_f_i^2      forecast uncertainty
       - delta     * max(0, -q05(w))              downside shortfall
       - cost(w, w_current)                       transition (zero-base: absent)

q05(w) = mu'w - z * tail * sqrt(w'Sigma_h w + sum_i w_i^2 sigma_f_i^2)
```

Subject to: long only · gross exposure ≤ 100 % · cash ≥ 0 · per-name weight cap ·
per-sector weight cap · liquidity participation cap on average dollar volume ·
eligible universe only.

**Every risk term is a property of the PORTFOLIO, not a sum of per-name
penalties.** Per-name forecast error is overwhelmingly idiosyncratic; charging it
name-by-name would price risk that diversification removes and would reject an
entire cross-section a diversified book handles comfortably. This was found
empirically during implementation — the first formulation, which penalised each
name's own downside quantile linearly, allocated 100 % to cash under every
realistic input.

`Sigma_h` and the uncertainty diagonal are PSD and `q05` is concave in `w`, so
`U` is concave over a convex set. The solver is Frank-Wolfe with a laminar greedy
linear oracle (name ⊂ sector ⊂ budget is a laminar family, so greedy is exactly
optimal), quadratic ray coefficients for an O(1) line search, and pairwise polish
steps for the interior-optimum tail. Convergence is declared against an
**economic** tolerance: a duality gap of 1e-7 in return units is about a cent on
a $100k paper book.

### The risk prices are derived, not chosen

| Parameter | Source |
|---|---|
| `gamma` | `mean / variance` of the candidate's realised per-period book excess return on WALK-FORWARD VALIDATION blocks — the price at which a fully-invested diversified book is exactly marginal. Clamped to [0.5, 50] with the clamp reported. |
| `phi` | equal to `gamma`: no evidence supports pricing forecast error differently from realised covariance risk. Kept a SEPARATE parameter so that can change. |
| `tail` | how much fatter the MEASURED residual 5 % left tail is than a normal one. |
| `delta` | `max(0, tail - 1)` — a symmetric residual distribution charges nothing extra, so the variance term is never double counted. |

### Cash

Cash is a real asset choice at a **declared** zero return
(`CASH_RETURN_POLICY = "ZERO_RETURN_PAPER_ASSUMPTION"`). No owned canonical
risk-free series is admissible as a portfolio-construction input — the
market-context owner is declared CONTEXT and never a signal (D-17) — and this
layer does not forecast the market's level. A name must therefore earn its place
against a genuinely riskless zero, and when the risk-adjusted opportunity set is
poor the optimiser holds cash.

## Point-in-time integrity

| Control | How |
|---|---|
| no future membership | the owned panel's own per-session PIT mask; delisted names retained |
| no future price | every feature slice ends at the decision session |
| no future fundamentals | visibility is the SEC `filed` date, through the released Stage-24 reader and its reporting lag |
| no future labels | an embargo of `ceil(horizon / step)` decision dates separates train, validation and test |
| no random split | blocks are contiguous and ordered |
| no future normalisation | every transform is per-decision-date and reads only that cross-section |
| no delisting survivorship in the LABEL | a name that stops trading inside a forward window is measured to its LAST OWNED CLOSE and retained, never dropped |
| no current sector as a historical feature | the canonical PIT sector owner classifies its snapshot as `ENTITY_SIC_SNAPSHOT_CONTROL`, inadmissible for signal construction. Sector enters only the CURRENT-date sector cap, where it is genuinely known. |

### The measured coverage limitation

Issuer resolution for the fundamental family succeeds for **56.7 %** of symbols
still in the universe but only **20.7 %** of symbols that have left it — a
**2.74x survivorship skew** in the rows on which any fundamental factor is
*defined*. This is measured, reported in `point_in_time_integrity.json`, and it
is why every fundamental comparison runs on a coverage-**MATCHED** sub-sample
where both sides see identical rows. That control isolates the forecast from the
sample; it does not remove the skew, so a fundamental result alone can never
justify activation.

The price/liquidity/risk family carries no such caveat: it is survivorship-free
across 304 decision dates and 277,466 rows.

## Governance

* **Historical walk-forward out-of-sample evidence may qualify a candidate for
  manual paper approval.** Release 30 does not require 12 future live
  observations first. This is a deliberate governance change, recorded in
  ARCHITECTURE_DECISIONS.
* **Existing TRUE_FORWARD evidence remains immutable.** None is fabricated,
  rewritten or reconstructed, and walk-forward evidence is never merged with it.
* **No automatic promotion.** `activation_state` is `NOT_ACTIVATED` unless a
  human writes an activation record carrying the explicit token. No code path in
  `api/return_forecast.py` can write one.
* **After activation**, every operational prediction is frozen BEFORE its outcome
  is known (`capture_forecast_snapshot`, append-only, first-write-wins), and
  outcomes are appended by the existing forward-evidence owners when a horizon
  matures.

## UI — the wireframe (produced before implementation, 1920×1080)

Three read-only regions are added inside the existing three-question navigation
(OPERATE · RESEARCH · SYSTEM). No new navigation item, no new operator surface,
no duplicate action button. **Today remains the sole normal-path mutation
surface**, and none of these regions carries an execute control.

### TODAY — one compact region added, above the existing decision cards

```
+--------------------------------------------------------------------------------------------------+
| TODAY                                            [Paper mode · Manual review · Automation off]    |
+--------------------------------------------------------------------------------------------------+
| TODAY HERO (existing, unchanged)                          ONE state, at most ONE primary action    |
+--------------------------------------------------------------------------------------------------+
| ACTIVE MANAGER (existing)                                                                          |
+--------------------------------------------------------------------------------------------------+
| >>> NEW: MATERIAL INFORMATION / CAPITAL IMPACT                    [READ ONLY] [Safety v]           |
|  What the system heard, how it read it, and what it changed.        7 material · 2 touch holdings  |
|  +----------+------+-----------------+-------------+-----------------------------------------+     |
|  | TIME     | NAME | WHAT CHANGED    | AUTHORITY   | FORECAST  RISK  HOC        -> RESULT    |     |
|  +----------+------+-----------------+-------------+-----------------------------------------+     |
|  | 08-20    | MSFT | Dividend 0.91   | OPER. RISK  |   --      yes   HOLD       -> NO CHANGE |     |
|  | 08-18    | DDOG | Delayed quote   | OPER. RISK  |   --      yes   HOLD->RED. -> WITHHELD  |     |
|  | 08-17    | ...  | 8-K filed       | TRIGGER ONLY|   --      --    review     -> NO CHANGE |     |
|  +----------+------+-----------------+-------------+-----------------------------------------+     |
|  Authority is decided once, by source family, in engine.event_fabric. An EVENT_TRIGGER_ONLY        |
|  event can trigger a reassessment and can NEVER contribute expected return.  [Full ledger ->]      |
+--------------------------------------------------------------------------------------------------+
| DAILY CLOSE (existing) | OPPORTUNITY COST (existing) | PORTFOLIO VERDICT (existing) | ...          |
+--------------------------------------------------------------------------------------------------+
```

Placement rationale: the operator's first question is *what happened*, and it must
sit above the conclusions that were drawn from it, not below them. It is a
12-row compact table, not a log — the deep ledger stays in Research.

### PORTFOLIO — one new region below the existing holdings table

```
+--------------------------------------------------------------------------------------------------+
| ZERO-BASE TARGET vs CURRENT PORTFOLIO      [PREVIEW ONLY][NOT A PROPOSAL][NO ORDERS][MANUAL REVIEW]|
|  "If all $99,913 were cash today, what would the model buy?"        Horizon 20 sessions            |
+---------------------------+---------------------------+------------------------------------------+
|  CURRENT                  |  ZERO-BASE IDEAL          |  IMPLEMENTABLE (after cost)              |
|  25 positions             |  N positions              |  M positions                             |
|  cash   4.5 %             |  cash   c %               |  cash   c' %                             |
|  E[excess]  x.xx %        |  E[excess]  y.yy %        |  E[excess]  z.zz %                       |
|  vol / q05  .. / ..       |  vol / q05  .. / ..       |  vol / q05  .. / ..                      |
|  net utility  ..          |  net utility  ..          |  net utility  ..                         |
+---------------------------+---------------------------+------------------------------------------+
|  TRANSITION   one-way turnover  tt %   cost $ccc   |  retained R  ·  removed X  ·  new N          |
|  Names the ideal wants but the implementable does not = positions whose improvement does not      |
|  cover the cost of getting into them.                                                             |
+--------------------------------------------------------------------------------------------------+
|  TARGET TABLE   ticker | sector | weight | $ | E[r] 5/20/60 | uncertainty | q05 | risk contrib |   |
|                 ADV participation | cap binding | current weight | change                          |
+--------------------------------------------------------------------------------------------------+
```

### RESEARCH — one new region: the alpha leaderboard

```
+--------------------------------------------------------------------------------------------------+
| ALPHA MODEL LEADERBOARD                                      [READ ONLY][NO LIVE PROMOTION]        |
|  Universe: price-only (survivorship-free)  |  fundamental-matched (COVERAGE_SURVIVORSHIP_SKEWED)   |
+--------------------------------------------------------------------------------------------------+
| MODEL                     LIFECYCLE   OOS rankIC (t)  net ret  IR   maxDD  turn  weight  VERDICT   |
| fundamental_momentum_50_50 OPERATIONAL      --           --     --    --     --     --   champion  |
| adaptive_ensemble          CANDIDATE      0.0xx (t)     x.x %  x.xx  -x.x%  x.xx   1.00   GO/NO-GO |
| ridge                      COMPONENT      0.0xx (t)     ...                        0.xx            |
| gbrt                       COMPONENT      ...                                      0.xx            |
| extra_trees                COMPONENT      ...                                      0.xx            |
| s25_operating_profitability COMPONENT     0.0xx (t)     ...                        0.xx            |
| baseline_momentum_leg      BENCHMARK      ...                                        --            |
+--------------------------------------------------------------------------------------------------+
|  Verdict criteria (all must pass): rank IC > benchmark · rank IC t >= 2 · net return > benchmark   |
|  · IR > benchmark · drawdown no worse · paired net-return t >= 2                                   |
|  Walk-forward dates, spec hash, ensemble method and forward-evidence count shown per universe.     |
+--------------------------------------------------------------------------------------------------+
```

### Acceptance criteria for the UI

1. No heavy scrolling on Today at 1920×1080 — the new region is one compact
   table with a fixed row cap.
2. No `alert()`, no `confirm()`, no blank button, no "Connect to Load" in a
   connected section.
3. Every new region carries visible safety badges and is explicitly labelled
   read-only / not-a-proposal.
4. No new execute control anywhere; no duplicate operator surface; Today remains
   the sole normal-path mutation surface.
5. Diagnostics stay in System · Audit.
6. Every value is rendered from the backend payload — no client-side arithmetic,
   no client-side verdict.

## Safety, unchanged

Paper-only · preview-first · manual review mandatory · automation off · no broker
execution · no Create Orders · no order plan · Stage 19 controlled execution
untouched. The zero-base target is not a proposal and cannot become one without
the existing proposal owner, the existing gate and a human.
