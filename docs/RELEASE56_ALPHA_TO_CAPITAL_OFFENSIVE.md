# Release 56 — Alpha-to-Capital Offensive

**One question, asked properly for the first time: not "which portfolio", but
"how many dollars of it, and does the next dollar pay for itself?"**

- **Branch:** `r56-alpha-capital-offensive`
- **Development worktree:** `D:\paper_trader_r56_alpha_capital` (isolated; the
  live `C:\Users\binis\paper_trader` system was READ ONLY throughout)
- **Built over:** `54cecf7` (R55.2.2)
- **Eligible market session used throughout:** `2026-09-03`
- **Safety envelope:** paper only, preview first, manual review. No order, no
  fill, no broker, no automation, no model promotion, no sleeve activation, and
  no write to any operational store.

---

## 1. Why this release exists

The project can safely reassess the book it holds. It could not answer the
question that decides P&L:

> If every investable dollar were cash right now, where should capital go — and
> is the next dollar of deployment worth its own transaction cost?

Three specific gaps blocked that answer, and each one is closed by exactly one
new owner:

| Gap | Why it mattered | New owner |
|---|---|---|
| The allocator answers *which portfolio*, never *how many dollars* | "Deploy the cash" and "rotate the book" were the same number on every surface | `engine.alpha_capital_frontier` + `api.cash_deployment_frontier` |
| Twenty-six campaign verdicts lived in twenty-six documents | A release could re-run a closed frontier and call it research | `api.alpha_opportunity_registry` |
| Release 46 races SIGNALS; nothing ever raced PORTFOLIOS | The portfolio is what actually holds the capital | `engine.shadow_portfolio_evidence` + `api.shadow_portfolio_evidence` |

A fourth owner, `api.alpha_capital`, composes them into the ONE operator read
model behind the **ALPHA & CAPITAL** surface. It owns exactly one calculation of
its own — the limiter ranking — and it is a ranking of facts other owners
measured.

---

## 2. The economic result, in one table

Measured on the live read-only state at eligible session `2026-09-03`
(NAV $98,361.40, cash $4,482.71 = 4.56%, 25 US-equity positions, one sleeve).

| Question | Answer |
|---|---|
| Should the cash be deployed? | **RESEARCH lane: yes** — all $4,482.71 clears the hurdle, net **+$3.29 per 20-session horizon** after $5.60 of cost, payback 12.6 sessions. **GOVERNED lane: MANUAL REVIEW — economic proof ABSENT.** |
| Is that worth doing? | It is +0.003% of NAV per horizon. It is real, it is positive, and it is not where the money is. |
| Should the book be rotated to the zero-base target? | **No.** The full rotation costs $190.19 and buys $130.57 of utility per horizon: **payback 29.1 sessions against a 20-session policy horizon.** |
| Is there a rotation that pays? | **Yes** — the transition-aware (implementable) target: switch cost $56.12, payback **13.7 sessions**, and rungs up to 25% of NAV clear the hurdle (+$25.07 net per horizon at the best rung). |
| What has actually destroyed P&L? | Realised excess vs SPY since inception is **−5.0852pp**, and it decomposes: **name selection −4.7210pp**, transaction cost **−0.2072pp**, cash drag **−0.1571pp**. |
| What is the primary limiter? | **SIGNAL_WEAKNESS** — measured, at −4.72pp. Cost and cash together are −0.36pp. |
| Is there an evidenced replacement? | **No.** Zero challengers are `FORWARD_CONFIRMED`; 17 effective independent observations exist across the whole tournament. |

**The one-sentence result:** *the book is not losing to cost, cash or risk
limits — it is losing to the ranking that chooses the names, and there is not yet
a single forward-confirmed alternative to give the capital to.*

---

## 3. Mandatory UI workflow — SCAN / REVIEW / PLAN / EXECUTE PREVIEW

### 3.1 SCAN — what already existed

Read before writing a line of code:

| Concern | Existing owner | Reused? |
|---|---|---|
| Objective, targets, caps, transition cost | `engine.zero_base_allocator` | **Imported.** No second optimiser. |
| Covariance | `engine.holding_opportunity_cost.build_covariance` | **Imported**, through one new shared helper (below). |
| NAV, cash, exposure | `api.capital_pool` → `api.paper_trading_desk.book_nav` | Composed. |
| Eligible universe, ranks, sector, ADV | `api.universe_scoring` | Composed. |
| Expected return / uncertainty | `api.return_forecast` | Composed (and it is `NOT_ACTIVATED`). |
| Capital eligibility | `api.investability_registry` → `api.opportunity_frontier` | Composed. |
| Holding review, addition candidates, cost policy | `api.holding_opportunity_cost` | Composed. |
| Forward signal tournament | `api.prospective_tournament` (R46) | Composed. |
| Sleeve frontier verdicts | `api.pnl_opportunity_frontier` (R32) | Composed. |
| Realised P&L, attribution, benchmark, cash drag | `api.paper_trading_desk`, `api.forward_evidence` | Composed; the cash-drag FORMULA is quoted, not re-derived. |
| Champion health, degradation, research opportunities | `api.research_agent` | Composed. |

**One refactor, and only one:** `engine.zero_base_allocator.horizon_covariance`
was extracted so the allocator and the capital ladder scale the daily covariance
to the policy horizon in the same place. Verified byte-identical: the live
allocation hash `b630572699862b1c780350e99b21fc24` reproduces exactly.

### 3.2 REVIEW — what the existing surfaces get wrong

- The zero-base surface publishes a transition PATH whose net-of-cost utility
  **declines monotonically from fraction 0** — and never says the word that
  explains it. A reader sees "the target is better" and does not see "and it
  takes 29 sessions to pay for that".
- "Deploy the cash" and "rotate the book" were one number. Deploying $1,000 of
  *gross buying* along the allocator's path actually consumes **$103.85** of
  cash and sells $896 of holdings. Reporting that as a cash decision tells an
  operator new money is at work when old money moved.
- The frontier reports `expected_return_state = NOT_CALIBRATED` in a field, and
  every downstream surface then quotes score improvements as though they were
  economics.
- Prior-release verdicts were unreachable from any running surface.

### 3.3 PLAN — the wireframe (1920×1080, first screen, no heavy scroll)

```
+----------+----------------------------------------------------------------------------------------+------------------+
| SIDEBAR  | TOP STATUS BAR                                                                          | RIGHT PANEL      |
| (global) | ALPHA & CAPITAL  ·  session 2026-09-03  ·  READY                                        | (340px, sticky)  |
|          | [PREVIEW ONLY][READ ONLY][NO ORDERS][ORDERS DISABLED][AUTOMATION OFF][MANUAL REVIEW]    |                  |
| Operate  +----------------------------------------------------------------------------------------+ SAFETY           |
|  Today   | KPI ROW — 6 compact cards, one line each                                                | preview-first    |
|  Portfo. | NAV $98,361 | CASH $4,483 (4.6%) | SINCE INCEPTION -1.64% vs SPY +3.45%                   | no orders        |
|  Markets | OPP. COST $1,645/yr | ZERO-BASE PAYBACK 29 sess | FORWARD CONFIRMED 0                    | manual review    |
|          +---------------------------------------------+------------------------------------------+------------------+
| Research | A · CASH DECISION            (dominant)     | B · WHAT IS LIMITING P&L                 | THE ONE ANSWER   |
|  Alpha & |   RESEARCH LANE  DEPLOY $4,482.71           |  1 SIGNAL_WEAKNESS        BINDING -4.72  | Cash: MANUAL     |
|  Capital |     net +$3.29 / horizon · payback 12.6     |  2 EVIDENCE_IMMATURITY    BINDING     -  | REVIEW REQUIRED  |
|  <active>|   GOVERNED LANE  MANUAL REVIEW              |  3 NO_CALIBRATED_EXP_RET  BINDING     -  | proof: ABSENT    |
|          |     economic proof ABSENT                   |  4 TXN COST + TURNOVER    MATERIAL -0.21 |                  |
| System   |   [ ladder · 7 rungs · compact table ]      |  5 CASH DRAG              SECONDARY-0.16 | NEXT EXPERIMENT  |
|          +---------------------------------------------+------------------------------------------+ r56_x4 forward   |
|          | C · ZERO-BASE vs TRANSITION-AWARE           | D · ALPHA & P&L SCOREBOARD               | portfolio race   |
|          |   current  25 names · cash 4.6% · U 0.00021 |  window | entity | ret | excess | dd     | EIV 0.87         |
|          |   zero-base 30 · cash 1.0% · U 0.00154      |  since inception: book / cash / SPY      |                  |
|          |   implement. 30 · cash 2.1% · U 0.00105     |  since 2026-08-26: R46 research book     | EVIDENCE         |
|          |   switch $190 · payback 29.1 sessions       |  since 2026-09-03: 6 portfolio challeng. | 0 FORWARD_CONF.  |
|          +---------------------------------------------+------------------------------------------+ 17 eff. indep.   |
|          | E · ALPHA OPPORTUNITY REGISTRY — compact, 8 rows + status counts                        |                  |
|          +----------------------------------------------------------------------------------------+                  |
|          | <details> AUDIT / ADVANCED — full ladders, all 28 families, rejected experiments,       |                  |
|          |            provenance hashes, degraded sources                                          |                  |
+----------+----------------------------------------------------------------------------------------+------------------+
```

**Acceptance criteria** (checked in
`tests/test_release56_alpha_capital.py::TestUiAcceptance`):

1. A dedicated `#tab-alpha-capital` view exists and is reachable from the left
   sidebar under Research, with a hash route.
2. Layout is a grid, never a vertical stack of full-width cards.
3. Six KPI cards, each carrying a real number — no decorative or empty card.
4. Every safety badge in the mandatory list is present in the view.
5. Diagnostics live inside a `<details>` Audit / Advanced block.
6. No `alert(`, no `confirm(`, no Create Orders control, no automation control.
7. No blank button: every button in the view has a text label.
8. The view issues GETs only.

### 3.4 EXECUTE PREVIEW — what shipped

Implemented exactly to the wireframe. The view is read-only: it issues four
GETs, renders them, and offers one Refresh button. It contains no form, no POST
and no confirmation token.

**Placement matters.** Every adjacent tab pair in `index.html` is already used as
a text slice boundary by some acceptance suite, so a new tab inserted between two
existing ones lands inside another view's region and breaks its assertions —
which is exactly what happened on the first attempt (`PREVIEW ONLY` contains the
substring `REVIEW ONLY`, and the Proposed Portfolio suite counts it). The view
therefore sits after `<!-- end tab-audit-advanced -->` and carries its own
`<!-- end tab-alpha-capital -->` marker, so its own suite never depends on
whatever tab happens to follow it.

**Two corrections the repository's own rules forced:**

1. `ORDERS DISABLED` is NOT shown. Phase 27B.6 and its regression test forbid it
   anywhere in the UI, and they are right: paper orders are REAL in this system
   and exist in the operational book under a governed manual workflow. Only live
   brokerage orders are structurally disabled, so the canonical wording is
   `NO LIVE BROKER ORDERS`. This deliberately departs from the badge list in
   `CLAUDE.md`, because on this surface that badge would be a false statement.
2. A real browser bug was caught before it shipped. A top-level
   `function _akRegistry(){}` IS `window._akRegistry` in a browser, so the
   loader's `window._akRegistry = reg` would have replaced the renderer with a
   plain object and thrown on the next render. Every stashed payload now carries
   a name no function uses (`_akRawFrontier`, `_akRegistryData`,
   `_akShadowData`), and `test_no_stashed_payload_shadows_a_renderer` catches the
   whole class statically.

**Verification.** All nine `<script>` blocks parse, and the ten Release-56
renderers were executed HEADLESS against the real payloads: six KPI cards, every
one carrying a real value, and no `undefined`, `NaN` or `[object Object]` in any
rendered section.

**Not done, and why:** Playwright acceptance against a running backend at
`http://127.0.0.1:8001/ui/`. The live backend serves the R55.2.2 tree and does
not contain these routes or this view, and starting a backend from this worktree
is forbidden by the release's own isolation rules. Browser acceptance is
therefore owed at merge time, not skipped.

---

## 4. The new owners

### 4.1 `engine/alpha_capital_frontier.py` — the capital axis

Pure kernel. Owns three calculations that did not exist:

**(a) The deployment ladder, in two modes that are never blurred.**

- `CASH_ONLY_BUYS_NO_SALES` — buys along the allocator's own buy direction and
  never sells; cash is a hard ceiling, and the shortfall on a rung larger than
  the cash on hand is reported as `unfunded_usd` rather than quietly funded by
  an imaginary sale.
- `REDEPLOYMENT_BUYS_AND_SALES` — walks the two-leg path
  current → implementable → zero-base, funded by cash and sales together.

Every point on either path is a blend of weight vectors the allocator already
produced, so the ladder cannot propose a portfolio the allocator would refuse.
Each rung reports destination, expected utility gain, transaction cost,
net-of-cost gain, incremental risk, incremental concentration (HHI and max name
weight), ADV participation, turnover, funding source, hurdle state and — when
the hurdle fails — the named reason cash won.

**(b) The payback horizon.**

```
payback_horizons = switch_cost_weight / utility_gain_per_horizon
```

The switch is paid once; the edge is earned per horizon. This is the number the
transition path implied and never stated.

**(c) The realised-excess decomposition.**

```
excess = cash_drag + transaction_cost_drag + unexplained
```

`cash_drag` quotes the desk's own formula. The residual is published as
`UNEXPLAINED_BY_CASH_OR_COST` and is **not** relabelled "selection alpha": the
beta assumption is declared, and the caveat travels with the number.

### 4.2 `api/alpha_opportunity_registry.py` — the Alpha Opportunity Factory

**28 economically distinct families.** Each carries asset classes, horizons, PIT
integrity, effective history, turnover, cost sensitivity, status, capital state,
the release and document that judged it, that release's own published figures,
and the named `reopen_condition`.

Live composition annotates each family with the Release-46 tournament's current
counts. Where a live challenger exists inside a family the frozen catalogue calls
closed, both are shown with a note — usually it means a later release re-opened
the question on **better data** (R32 measured multi-asset trend on index-level
proxies; R38 delivered native futures).

The **experiment queue** ranks by expected information value across four
declared components (orthogonality 0.35, evidence gain 0.30, prior plausibility
0.20, implementability 0.15) with a floor of 0.35, and it says **no** by name:
re-running the R32 sleeves scores 0.2125 and is rejected as
`REJECTED_RETESTS_AN_EXHAUSTED_FAMILY_WITH_NO_NEW_INFORMATION`.

### 4.3 `engine/` + `api/shadow_portfolio_evidence.py` — forward paper PORTFOLIOS

Six complete books frozen on `2026-09-03`, each with the point-in-time identity
of every input and a record hash over that body:

| challenger | what it claims | valued by |
|---|---|---|
| `r56_zero_base_research_v1` | 30 names, 1.0% cash — the R30 objective with no incumbency | `api.price_panel` |
| `r56_implementable_research_v1` | 30 names, 2.1% cash — the same objective solved FROM the book, costed | `api.price_panel` |
| `r56_governed_score_top25_v1` | equal-weight top 25 on the approved model's own ranking | `api.price_panel` |
| `r56_incumbent_book_v1` | the live book's 25 names, 4.6% cash (control) | `api.price_panel` |
| `r56_all_cash_v1` | 100% cash (control) | declared zero-return policy |
| `r56_benchmark_spy_v1` | SPY (control) | `api.paper_trading_desk` |

`r56_governed_score_top25_v1` is the interesting one: it is the approved model's
ranking with **nothing built on top of it**, so within weeks it separates "the
ranking is weak" from "everything we build on the ranking is weak".

**Frozen on 2026-09-03 at inception timestamp `2026-09-04T16:34:57Z`**, under the
new research root `D:\Stock_Prediction_app_data\r56_shadow_portfolios` (a NEW
directory; no operational store was written):

| challenger | session | names | cash | entry cost | record hash |
|---|---|---|---|---|---|
| `r56_all_cash_v1` | 2026-09-03 | 0 | 1.0000 | $0.00 | `3c022b0a402d543c` |
| `r56_benchmark_spy_v1` | 2026-09-03 | 1 | 0.0000 | $125.00 | `5c6469617c114515` |
| `r56_governed_score_top25_v1` | 2026-09-03 | 25 | 0.0000 | $125.00 | `23d0d73149673dee` |
| `r56_implementable_research_v1` | 2026-09-03 | 30 | 0.0206 | $122.43 | `c0041e5d50274ec4` |
| `r56_incumbent_book_v1` | 2026-09-03 | 25 | 0.0456 | $119.30 | `cdb760b8ecc9b44b` |
| `r56_zero_base_research_v1` | 2026-09-03 | 30 | 0.0097 | $123.79 | `75f85256cef4a600` |

Each record carries the same point-in-time identity:
`portfolio_state_hash d3323245…`, `economic_state_hash 932c7388…`,
`universe_scoring_hash fe6246b1…` (model `fundamental_momentum_50_50_v1`),
`forecast_model_spec_hash c283523e…`, `feature_snapshot_hash 6531ecd7…`,
`allocation_hash b6305726…`.

**The question these answer, and the one they do not.** They answer *which of
these portfolios, constructed from cash at the inception session, earns more
forward* — every book including the incumbent control pays a one-off entry cost
at the canonical per-side rate, which shifts each fully-invested book by about
the same 12.5bps and leaves the ranking between them intact while cash correctly
pays nothing. They do **not** answer *should the book switch*: that is a
switching-cost question and it is answered by the payback horizon in
`api.cash_deployment_frontier`. The read model states both, in
`comparison_framing`.

**No leader is named until a session is scored.** A leaderboard whose every row
has zero observations has no leader, and reporting its first row as one would be
an ordering artefact presented as a result.

**No hindsight, structurally.** The kernel refuses any bar dated on or before
inception; a session where under 95% of invested weight has a real bar is
reported uncovered and is not scored; nothing is rebalanced after inception; and
comparisons run through a function that intersects two books' calendars. On the
day they were frozen these challengers hold **zero** forward observations, and
that is reported as the correct state rather than hidden.

### 4.4 `api/alpha_capital.py` — the ONE read model

Eight answers, one calculation (the limiter ranking), one scoreboard in which
**every row carries its own window**.

---

### 4.5 The two ladders, measured

Eligible session `2026-09-03`, NAV $98,361.40, cash $4,482.71, cost 12.5bps per
side, policy horizon 20 sessions, research-lane forecast.

**CASH-ONLY** (buys only; capacity = the cash on hand, $4,482.71):

| rung | deployed | cost | net / horizon | payback | hurdle |
|---|---|---|---|---|---|
| $1,000 | $1,000.00 | $1.25 | +$0.78 | 12.3 sess | CLEARS |
| $2,500 | $2,500.00 | $3.12 | +$1.91 | 12.4 sess | CLEARS |
| $5,000 | $4,482.71 | $5.60 | +$3.29 | 12.6 sess | CLEARS (capped by cash) |
| 5% / 10% / 25% / 100% NAV | $4,482.71 | $5.60 | +$3.29 | 12.6 sess | CLEARS (capped by cash) |

Marginal net per unit deployed **+0.0008**; the cost consumes **61.9%** of the
marginal gain.

**REDEPLOYMENT** (buys and sales; capacity $77,886.10):

| rung | deployed | turnover | cost | net / horizon | hurdle |
|---|---|---|---|---|---|
| $1,000 | $1,000.00 | 0.96% | $2.37 | +$1.90 | CLEARS |
| $5,000 | $5,000.00 | 4.82% | $11.85 | +$8.79 | CLEARS |
| 10% NAV | $9,836.14 | 9.64% | $23.31 | +$15.61 | CLEARS |
| 25% NAV | $24,590.35 | 23.70% | $58.29 | **+$25.07** | CLEARS (best) |
| 100% NAV | $77,886.10 | 77.35% | $190.19 | **−$59.62** | **CASH WINS** |

The last row is the release's sharpest number: **the full rotation to the
zero-base target destroys $59.62 of expected utility per horizon**, because the
$130.57 it buys costs $190.19 to reach. The R30 surface has always published a
transition path that says this implicitly; nothing until now said it in words.

### 4.6 What actually made and lost money

Realised, since the book's inception, from `api.paper_trading_desk` and
`api.forward_evidence`:

| fact | value |
|---|---|
| Book cumulative return | −1.6386% (NAV $98,361.40 from $100,000) |
| SPY cumulative return | +3.4466% |
| Excess | **−5.0852pp** |
| Realised annualised volatility | 14.91% |
| Max drawdown | −4.6576% |
| Hit rate | 41.94% |
| Average daily turnover | 5.1667% |
| Cumulative transaction cost | $207.15 |

Decomposed by `engine.alpha_capital_frontier.excess_decomposition`:

| term | pct points | actionable by |
|---|---|---|
| **UNEXPLAINED_BY_CASH_OR_COST** | **−4.7210** | the selection model itself |
| TRANSACTION_COST_DRAG | −0.2072 | the turnover policy |
| CASH_DRAG | −0.1571 | the cash deployment decision |

Beta assumption: the invested sleeve is treated as beta one to the benchmark,
and the residual absorbs any deviation from it. It is reported as a residual and
never as a measured selection effect.

### 4.7 The limiters, ranked by measured impact

| # | limiter | severity | measured impact | measured by |
|---|---|---|---|---|
| 1 | SIGNAL_WEAKNESS | BINDING | −4.72pp | forward prediction skill (rank IC 0.0172, decile spread −0.4391pp on 25 obs) |
| 2 | EVIDENCE_IMMATURITY | BINDING | — | 0 `FORWARD_CONFIRMED`, 17 effective independent observations |
| 3 | NO_CALIBRATED_EXPECTED_RETURN | BINDING | — | `expected_return_state = NOT_CALIBRATED`, forecast `NOT_ACTIVATED` |
| 4 | TRANSACTION_COST_AND_TURNOVER | MATERIAL | −0.21pp | 5.17% average daily turnover; 61.9% of the marginal gain |
| 5 | MISSING_INFORMATION | MATERIAL | — | four campaigns concluded `NEW_ORTHOGONAL_INFORMATION_REQUIRED` |
| 6 | MODEL_WEAKNESS | MATERIAL | — | research agent: benchmark-relative WEAK, turnover-efficiency WEAK |
| 7 | SINGLE_SLEEVE_CAPITAL_ELIGIBILITY | MATERIAL | — | one non-cash eligible class against 10 with forward challengers |
| 8 | CASH_DRAG | SECONDARY | −0.16pp | 4.56% idle cash |

### 4.8 Experiment `r56_x2` — the payback-aware replace hurdle (RUN)

Bounded, read-only, owned data only. Today's 17 live Holding Opportunity-Cost
`EXIT`/`REPLACE` reviews were run through the Release-56 payback calculation.
The operational hurdle is expressed in SCORE units and is risk-blind; this asks
whether each swap repays its own switching cost in UTILITY.

**Result: 0 of 17 clear. All 17 are `NEVER_PAYS_BACK_AT_THIS_COST_RATE` —
because the utility gain is negative BEFORE a cent of cost is paid.**

| variant | utility / horizon | cost | net first horizon |
|---|---|---|---|
| best single swap (ITW → SNDK) | −$11.40 | $8.83 | **−$20.23** |
| worst single swap (DVN → SNDK) | −$30.49 | $10.86 | **−$41.35** |
| diversified basket: exit all 17, spread equally over the top 5 addition candidates | −$263.24 | $144.63 | **−$407.86** |

The diversified variant matters: concentrating every exit into the single
strongest replacement is not what a constrained proposal engine would do, and the
result survives the fairer construction. (Note: `strongest_replacement_ticker`
is a per-holding DIAGNOSTIC field. `engine.reallocation_proposal` is the real
construction owner and its state for this session is `NOT_RUN`, so the
operational stack has proposed none of these swaps.)

### 4.9 Why — the two models agree on selling and agree on nothing to buy

| comparison | overlap | Jaccard |
|---|---|---|
| HOC exits/replaces (17) vs zero-base removals (18) | 13 names | **0.591** |
| HOC top-10 addition candidates vs zero-base's 23 new names | **0 names** | **0.000** |

And the reason is measurable:

| set | median approved rank | median research-forecast mu (20 sessions) |
|---|---|---|
| the approved model's top-10 addition candidates | **11.5** | **−0.00056** |
| the zero-base target's 23 new names | 108 | **+0.00169** |
| the whole eligible universe (197 names) | — | +0.00021 (median) |

**The approved model's highest-ranked BUY candidates are, on the research
forecast, below-average expected return.** Rank agreement across the whole
universe is weak but positive (Spearman of approved rank against research mu =
−0.1444, i.e. rank-IC ≈ +0.14 in the conventional orientation): the two models
broadly agree which names are deteriorating and disagree completely about where
to put the money.

**Honest limits of this finding.** The Release-30 forecast is NOT the approved
model, is NOT activated, and its feature panel is stamped `2026-08-05` while the
eligible session is `2026-09-03`. Two unproven models disagreeing proves neither
right. What raises it above a curiosity is that the operational model's OWN
forward evidence points the same way: mean forward rank IC **+0.0172** and a
forward top-minus-bottom decile spread of **−0.4391pp** over 25 observations —
i.e. the top decile has not outperformed the bottom one. Two independent lines of
evidence say the failure is on the BUY side, which is exactly where the realised
−4.7210pp of unexplained excess is being generated.

---

## 5. Point-in-time safety

| Control | How |
|---|---|
| Decision timestamp | every payload stamps `eligible_market_date` and `generated_at` |
| Available-at | the ladder reads only the allocator's input contract, which is built from the eligible session |
| Input versions | `portfolio_state_hash`, `universe_scoring_hash`, `forecast_model_spec_hash`, `feature_snapshot_hash`, `allocation_hash`, `economic_state_hash` travel on every payload and inside every frozen record |
| No current-into-past substitution | the shadow kernel refuses bars dated `<= inception_session`; there is no code path that scores a record on a return that already existed |
| Survivorship | the frozen catalogue records which vendors failed the survivorship-safe test (EODHD analyst grades, FMP grades) and why they were not bought |
| Known limitation, stated | the owned feature panel behind the research forecast is at `2026-08-05` while the eligible session is `2026-09-03`; the forecast owner reports the gap and this release does not paper over it |

---

## 6. Model governance

R56 identifies a leader, identifies challengers, recommends a promotion review
and accumulates evidence. It does **not** replace a champion, change a holding,
activate a sleeve or promote anything. `automatic_promotion_allowed` is False in
every payload and `promotion_allowed` is False on every leaderboard row.

---

## 7. Files added / changed

**Added**

- `engine/alpha_capital_frontier.py`
- `engine/shadow_portfolio_evidence.py`
- `api/cash_deployment_frontier.py`
- `api/alpha_opportunity_registry.py`
- `api/shadow_portfolio_evidence.py`
- `api/alpha_capital.py`
- `tests/test_release56_alpha_capital.py`
- `docs/RELEASE56_ALPHA_TO_CAPITAL_OFFENSIVE.md`

**Changed**

- `engine/zero_base_allocator.py` — extracted `horizon_covariance` (verified
  byte-identical against the live allocation hash)
- `api/app.py` — four read-only GET routes
- `api/ui/index.html` — the ALPHA & CAPITAL view
- `docs/architecture/system_inventory.json` — six new modules
- `PROJECT_STATE.md`

---

## 7.1 Store writes

Exactly one directory was created, and it is a NEW Release-56 research root that
no operational owner reads:

```
D:\Stock_Prediction_app_data\r56_shadow_portfolios\records\*.json   (6 files)
```

No operational store was written. No production ledger, decision root,
reassessment root, desk root or provider cache was touched. The live backend was
not restarted, the collection worker was not restarted, no scheduled task was
changed and no mutation endpoint was called. Every live read in this release was
a `GET`.

---

## 8. What to do next, in order

1. **Let the frozen portfolio challengers accrue.** They start on the next
   session. Within weeks `r56_governed_score_top25_v1` versus
   `r56_incumbent_book_v1` separates "the ranking is weak" from "what we build on
   the ranking is weak", and `r56_zero_base_research_v1` tests §4.9's claim that
   the buy side is where the failure is. Nothing else this project can do
   produces those answers faster, and none of it requires a decision today.
2. **Prosecute the buy side, not the whole model.** §4.9 localises the failure:
   the sell signals of the two models agree, the buy signals share nothing, and
   the approved model's top-ranked additions carry below-median expected return
   on the research forecast while its own forward decile spread is negative. A
   recalibration study aimed at the ADDITION rule is a far smaller and better
   posed question than "recalibrate the champion".
3. **Decide the return-forecast activation question.** Every governed capital
   hurdle is unevidenced because no operational owner publishes an expected
   return. That is one manual governance decision, not a research programme —
   and until it is taken, `HURDLE_NOT_EVIDENCED_NO_CALIBRATED_EXPECTED_RETURN`
   is the honest answer to every capital question this system is asked.
4. **Adopt a payback-aware replace hurdle.** The operational hurdle is expressed
   in score units and is risk-blind: it cannot see that 0 of today's 17 flagged
   swaps raise risk-adjusted utility, nor that the full rotation needs 29
   sessions to repay a 20-session horizon's cost.
5. **Fix the turnover before fixing anything else that costs money.** 5.17%
   average daily turnover is roughly 1,300% of NAV traded per year, about 1.6%
   per year in cost, paid by a model whose forward decile spread is negative.
   Cost is not the biggest term in the shortfall, but it is the one that is
   certain.
6. **Do not re-run a closed family.** The registry names the condition for each
   one. The binding constraint on NEW alpha is information, not modelling
   effort, and four independent campaigns say so.
