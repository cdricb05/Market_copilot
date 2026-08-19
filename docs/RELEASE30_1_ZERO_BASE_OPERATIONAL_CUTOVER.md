# Release 30.1 — Zero-Base Operational Cutover

> Canonical release document. Read with
> [PROJECT_CHARTER.md](PROJECT_CHARTER.md) ·
> [RELEASE30_ZERO_BASE_ADAPTIVE_ALLOCATOR.md](RELEASE30_ZERO_BASE_ADAPTIVE_ALLOCATOR.md) ·
> [CURRENT_ARCHITECTURE.md](CURRENT_ARCHITECTURE.md) ·
> [ARCHITECTURE_DECISIONS.md](ARCHITECTURE_DECISIONS.md)

## Verdict

**`R30_1_CALIBRATION_BLOCKED`** — the zero-base allocation architecture is not
cut over to the operational lane, and **must not be**, because the current
approved model's score cannot be turned into an expected return without
fabricating one.

The architecture Release 30 built is sound. What it lacks is a defensible `mu`.

## What was found

Release 30 shipped a frozen artifact, `model_artifact_operational.json`, whose
`activation` field reads `CURRENT_OPERATIONAL_MODEL` and whose 20-session
calibration slope is **−0.000848**.

The forecast kernel computes

```
expected_excess_return = slope * standardised(rank_normalise(score))
```

and the standardisation of a positive-weight rank blend is **strictly
monotone**. A negative slope therefore does not adjust the approved model — it
**reverses** it. Read back against the approved model's own ranking, the Aug-18
target that Release 30 labelled *"MODEL A — CURRENT OPERATIONAL MODEL / ZERO
BASE"* was:

| | |
|---|---|
| positions | 40 |
| weighted-average approved-model rank | **168 of 199** |
| approved model's TOP-25 names held | **0** |
| approved model's BOTTOM-25 names held | **19** |
| largest position | AES, 7.07 % of NAV, approved-model **rank 160** |

The portfolio carried the approved model's name and held the approved model's
worst names. Nothing in the codebase could have said so, because no rule
required an approved-model adapter to preserve the ranking it is an adapter for.

## The rule this release adds

> **An artifact that carries the approved model's NAME must carry its RANKING.**

`engine/return_forecast.py` is the ONE owner of that verdict
(`rank_identity()`), and it is a hard contract:

* a horizon whose calibration slope is **not positive** is **SUPPRESSED** — it
  emits no expected return, no uncertainty, no downside, for any consumer;
* a horizon that explicitly declares itself **NOT_CALIBRATED** is suppressed the
  same way, and is a valid *declaration* rather than a malformed artifact, so
  the research lane can say **why** a horizon failed instead of silently
  omitting it;
* a genuine research candidate is **not** bound by the contract — it is entitled
  to disagree with the incumbent in either direction, because it is not claiming
  to be the incumbent;
* the contract binds **retroactively**. The released Release-30 `operational`
  artifact declares `FROZEN_OPERATIONAL_CHAMPION_NO_FITTING` and is caught by it
  at 5 and 20 sessions. A contract that bound only future artifacts would have
  been a comment.

## The operational calibration, and why it is blocked

`alpha_agent/release30_1_operational_calibration.py` reconstructs
`fundamental_momentum_50_50_v1` — the model the operator actually runs — at every
owned decision date, using the **same owned inputs and the same construction**
as the live owner (`api.multi_horizon_engine.compute_scores` →
`compute_combined`): the fixed 50/50 blend of the within-common-universe
percentile ranks of `composite_sn` and `mom_6_1`. The percentile transform is
asserted equal to the live owner's, tie handling included.

No weight is fitted. No feature is added. No component of the Release-30
adaptive candidate is admitted, and `s25_operating_profitability` appears
nowhere. **Exactly one number per horizon is estimated**: the slope.

Reconstruction fidelity: **198 of the 199 live eligible names** are covered by
the latest reconstructed month.

### Measurement discipline

| Control | How |
|---|---|
| common decision timestamp | every name in a cross-section is measured over the SAME forward window, starting at the decision session |
| owned survivorship-free prices | the Phase-24 daily panel; a name that stops trading inside the window is measured to its LAST OWNED CLOSE and retained |
| strict walk-forward | contiguous ordered blocks, embargo of `ceil(horizon / 21)` decision dates, slope estimated on VALIDATION rows only |
| no random split | there is no RNG in the module; the audit checks for the behaviour, not the word |
| sign stability | the slope is re-estimated under three declared fold geometries |

### The measured result

81 owned decision dates, 2016-06-30 → 2026-05-29, ~199 names per cross-section.

| Horizon | measured slope | Newey-West *t* | sign stable | rank identity | state |
|---|---|---|---|---|---|
| 5 sessions | −0.000992 | −0.52 | no | **VIOLATED** | NOT_CALIBRATED |
| 20 sessions | +0.000965 | +0.95 | yes | preserved | NOT_CALIBRATED |
| 60 sessions | +0.001485 | +0.57 | **no** | preserved | NOT_CALIBRATED |

No horizon is distinguishable from zero. At 5 sessions the sign is negative — a
mapping that would invert the approved model. At 60 sessions the sign **flips**
between two equally defensible fold geometries (+0.0015 / −0.0003), and a slope
whose sign depends on the analyst's arbitrary choice of geometry is not a
calibration.

### Why the panel's own label is not admissible

The frozen fundamental panel carries a `forward_63d_return` column on which the
same blend reaches *t* = +2.29. It is not usable for an operational allocator:

* its forward window **starts at each name's own filing date**, staggered across
  the whole month (up to 20 distinct start dates per month, in every month). An
  allocator commits capital at ONE timestamp; a "cross-sectional excess" taken
  over non-overlapping windows is not a cross-sectional excess, and it silently
  absorbs market timing. Measured with a common start, the same evidence gives
  *t* = +1.03.
* it disagrees with the owned survivorship-free daily panel by a **median 4.4 %**
  over the same 63 sessions, matching neither the raw return nor the
  cross-sectional excess, and **no row records a window end date** — so its
  measurement cannot be verified.

## Current-session freshness — fixed

Release 30 reported `feature_as_of_date = 2026-08-05` against a requested
eligible date of `2026-08-18`. That gap was **not** a gap in the operational
model's inputs; it came from the periodic research price panel that the
operational lane was routed through.

`api/return_forecast.build_operational()` now reads the approved model's own
live score from `api.universe_scoring` at the current eligible market date. No
research file is in the live path. Freshness is judged by the canonical owner
`api.data_freshness`, on the sources it already declares required for signal
refresh; a source under a slower declared cadence (the quarterly fundamental
panel) is judged by its own owner, not by today's date.

Live proof, 2026-08-18:

```
forecast.eligible_market_date  = 2026-08-18   (= workflow eligible market date)
required input freshness       = FRESH
  owned_daily_prices   FRESH  2026-08-18   (DAILY,   required)
  price_score_refresh  FRESH  2026-08-18   (DAILY,   required)
  momentum_monthly     FRESH  2026-08-01   (MONTHLY, required)
  fundamental_quarterly       2026-05-22   (QUARTERLY, NOT required — own cadence)
state                          = BLOCKED
blocker                        = NO_CALIBRATED_HORIZON [5, 20, 60]
```

The lane is blocked by **economics**, not by data. That distinction is the point.

## Evidence you can open — source links and external references

Two UX additions, both governed by one rule: **a link is a convenience for a
human reading evidence; it is never an input.**

### Source links in Material Information

Every normalized event already records the URL it came from in
`payload_reference`. The capital-impact feed now exposes it as `source_url`,
alongside `source_title` (the headline the source published), `source_host` and a
named `source_url_state`. In the UI the Source column — and the *what changed*
text when a URL exists — is an anchor with `target="_blank"` and
`rel="noopener noreferrer"`.

| Rule | How |
|---|---|
| the backend owns the URL | it is the event's own `payload_reference`, exposed unchanged |
| the frontend infers nothing | `_r30srcLink` refuses to render an anchor without a backend URL; no renderer contains a literal URL or a hand-rolled `<a href=` — the audit checks both |
| no link where there is no URL | a non-URL reference (`eodhd\|AAPL\|2026-08-18`) is returned as `source_reference` and rendered as plain text |
| an unsafe reference never reaches an `href` | `api.external_references.safe_external_url` is the ONE owner of that decision: absolute `http`/`https` with a host, no control characters, length-bounded. `javascript:`, `data:`, `vbscript:`, `ftp:`, scheme-less and relative references are refused with a named reason |
| authority is untouched | the same event with and without a URL classifies identically — asserted field by field |
| an article is not alpha | an `EVENT_TRIGGER_ONLY` news event carrying a URL still reports `forecast_affected: false`, and the payload declares `external_article_is_not_alpha: true` |

A feed-supplied string is untrusted input. That is why the safety decision has
one owner rather than a check at each call site: two sanitisers drift, and the
weaker one wins wherever it happens to be called.

### External Market Sources — Markets only

A compact reading list on the **Markets** page, deliberately absent from Today,
which is the operating surface and carries only what the system itself concluded.

* FinancialJuice — <https://www.financialjuice.com/home>
* Trading Economics · Indicators — <https://tradingeconomics.com/indicators>
* Investing.com · Economic Calendar — <https://www.investing.com/economic-calendar>

All open in a new tab. `api/external_references.py` owns the list and answers the
only question that matters about it — *is any of this actually influencing the
portfolio?* — from the **canonical registries on every read**:
`api.source_capability` (is it a registered, ingested source?) and
`engine.event_fabric` (what authority does its event family carry?).

Today all three are `REFERENCE_ONLY`, `ingested: false`, `signal_authorities: []`,
`influences_portfolio_decisions: false`. That is derived, not captioned — a
hard-coded "reference only" label in HTML would be the frontend asserting a
backend fact, and it would keep asserting it on the day one of these sites is
genuinely wired into the collection lane. The region's badge renders the
backend's answer, so it changes by itself if that day comes.

The module makes no network call, creates no event, assigns no authority, and
declares `owns_no_calculation`.

### Signal transparency

A Material Information row carries the full evidence chain, declared as a
contract in `TRANSPARENCY_FIELDS` so it is checkable from the API rather than
from markup: source · source family · title · URL · host · timestamp · ingested
at · ticker · held · event type / sub-type / family · signal authority ·
authority reach · event quality · point-in-time status · what changed · forecast
affected · risk affected · HOC affected · HOC recommendation and deterioration ·
portfolio reassessed · canonical result.

The browser computes none of them. It renders what the read model published.

## Two lanes, never equally authoritative

`api/zero_base_target.py` now stamps every payload with its authority:

| Lane | Source | Can become a proposal |
|---|---|---|
| `RESEARCH_PREVIEW` | the Release-30 adaptive candidate, `NOT_ACTIVATED` | **never** |
| `GOVERNED_OPERATIONAL_TARGET` | the CURRENT APPROVED model, current session, rank-preserving calibration | only through the existing reassessment gate, the existing proposal owner and a human |

`run_operational_allocation()` **never falls back** to the research forecast — a
target an operator would read as governed must not be able to come from a model
the operator does not run. The audit enforces this on the AST.

## What was deliberately NOT done

Parts 4–12 of the release brief — making the implementable zero-base target the
ONE authoritative allocation, cutting `engine.reallocation_proposal` over to it,
routing `api.portfolio_decision` downstream of it, and wiring it into the DRC and
the event fabric — **are not implemented**.

Every one of them consumes `mu`. With no defensible `mu`, that cutover would
have replaced the legacy incremental target with a portfolio shaped by
constraints and noise, and given it the authority the legacy path has today. On
Aug-18 it would have replaced a 25-name book built from the approved model's top
ranks with a 40-name book built from its bottom sixth.

The legacy incremental construction in `engine.reallocation_proposal` therefore
remains the operational path, unchanged. It is not endorsed as ideal — Release 30
is right that ownership is not an investment thesis — but it is at least derived
from the approved model's ranking in the approved direction.

**The consolidation is a data problem, not an architecture problem.** It unblocks
the day an approved model has a rank-preserving, reliable forward-return
calibration.

## The three cycles

| Cycle | Cadence | Release 30.1 |
|---|---|---|
| **Signal refresh** | FREQUENT | unchanged; the operational forecast now reads its output at the current session instead of a periodic snapshot |
| **Portfolio reassessment** | FREQUENT | unchanged; the governed zero-base target does not yet participate, because it is DATA_BLOCKED |
| **Model recalibration** | CONTROLLED | the operational calibration is a frozen, hashed research artifact; it changes only when the research lane is re-run and reviewed |

## The canonical capital path

```
CURRENT APPROVED MODEL                       api.universe_scoring
        ↓                                    fundamental_momentum_50_50_v1
calibrated expected-return representation    engine.return_forecast
        ↓                                      + rank-identity contract
        ↓                                    ⛔ BLOCKED — no calibrated horizon
ZERO-BASE TARGET                             engine.zero_base_allocator
IMPLEMENTABLE TARGET                         engine.zero_base_allocator
        ↓
HOC                                          engine.holding_opportunity_cost
REASSESSMENT                                 engine.portfolio_reassessment
PROPOSAL                                     engine.reallocation_proposal
DECISION                                     api.portfolio_decision
MANUAL EXECUTION                             api.rebalance_execution (Stage 19)
```

The chain stops at the forecast gate, **upstream of every economic gate**.
Nothing is withheld by a limit, because nothing was proposed.

## Governance

* The Release-30 adaptive candidate remains **`R30_ADAPTIVE_MODEL_NO_GO` /
  `NOT_ACTIVATED`**. Nothing here activates it and no code path can.
* The approved model needs no activation record — it IS the approved model. What
  it needs is a calibration that preserves its ranking.
* `AUTOMATIC_PROMOTION_ALLOWED = False` throughout. No automatic promotion, no
  automatic recalibration, no order, no automation.
* Stage 19 controlled execution is untouched.

## Owners

| Concern | Owner |
|---|---|
| what a forecast IS, and the rank-identity verdict | `engine.return_forecast` |
| operational forecast composition, live cross-section, freshness delegation | `api.return_forecast` |
| the objective, constraints, optimiser, transition economics | `engine.zero_base_allocator` |
| target composition and lane authority | `api.zero_base_target` |
| the historical calibration of the approved model | `alpha_agent.release30_1_operational_calibration` |
| what may become an external `href`, and the reference reading list | `api.external_references` |
| the capital-impact feed and its row contract | `api.material_information` |
| whether a site is an ingested source | `api.source_capability` (read, never restated) |
| freshness | `api.data_freshness` (delegated, never restated) |
| covariance | `engine.holding_opportunity_cost.build_covariance` |
| trailing return series | `api.price_panel.aligned_returns` |
| proposal · decision · execution | `engine.reallocation_proposal` · `api.portfolio_decision` · `api.rebalance_execution` |

## Reproducing the evidence

```powershell
$py = 'C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe'
& $py scripts\run_release30_1_operational_calibration.py --stage verify
& $py scripts\run_release30_1_operational_calibration.py --stage calibrate
& $py scripts\run_release30_1_aug18_replay.py
& $py -m pytest tests\test_release30_1_operational_cutover.py -q
& $py scripts\audit_architecture.py --strict
```

Artifacts land under
`D:\Stock_Prediction_app_data\release30_1_zero_base_operational_cutover`
(research root; no operational store is written).

## UI acceptance

`api/ui/index.html` gains one region (External Market Sources, inside
`#tab-markets`), one Source column in the existing Material Information table,
and two helpers (`_r30attr`, `_r30srcLink`). `api/app.py` gains one GET route.
No navigation item, no operator control, no mutation route; Today remains the
sole normal-path mutation surface. No `alert()`, no `confirm()`, no blank
control, no execute control in any new or changed region.
