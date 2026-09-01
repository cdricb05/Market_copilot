# Release 53 — Active Risk, Intraday Alpha & Cross-Market Capital Offensive

**Date:** 2026-09-01 · **Built on:** R52 commit `949ca9d` · **Mode:** single
agent, no subagents, Windows PowerShell only · **Safety:** paper-only,
shadow-only, no production policy change, no promotion, no orders, no
scheduler mutation.

Release 53 attacked the three bottlenecks named after the first governed
2026-08-31 portfolio cycle — risk appetite, true intraday alpha, and
cross-market alpha that deserves capital — as one release feeding the ONE
canonical allocator.

---

## Track A — Active Risk Appetite

**Owner:** `alpha_agent/r53/risk_appetite.py`.
**Artifacts:** `R53_POLICY_INVENTORY.json`, `R53_LIVE_BINDING_ANALYSIS.json`,
`R53_RISK_APPETITE_WALKFORWARD.json`, `R53_SHADOW_POLICIES.json` under
`D:\Stock_Prediction_app_data\active_risk_intraday_alpha_r53\r53_active_risk_intraday_alpha_v1\`.

Three instruments, one canonical kernel:

1. **Policy inventory** — the production policy read VERBATIM from its two
   canonical owners; the shared values agree exactly.
2. **Live binding census** — 17 DRC manifests, 1 proposal, the recorded
   decisions. Finding: on every governed observation to date the **switching
   hurdle bound** (closest approach 0.0242 against 0.05); no other constraint
   has yet reshaped a real proposal.
3. **Walk-forward policy-region study** — the owned survivorship-free
   Russell-1000 panel (2000→2026-08, PIT membership), 12-1 momentum PROXY
   signal (declared: the fundamental leg cannot be PIT-rebuilt and was not
   fabricated), decisions every 10 sessions, every portfolio construction
   through `engine.constrained_reallocation.solve_feasible_target` →
   `switching_economics` → `decide_outcome` with variant policy dicts.
   Development (<2018) and validation (≥2018) reported separately;
   one-at-a-time axis sweeps; **no champion selected**.

**What the study says about each control (proxy signal, both zones):**

| Control | Verdict |
|---|---|
| Name cap (10%) | **Dormant safety net** — 4%→15% barely moves any metric; ≥12% never binds at N=25. Keep. |
| Re-entry cooldown | **Dormant** — 0/5/21/63 sessions indistinguishable. Hysteresis already does this job. |
| Turnover budget (0.35) | **In the robust region** (0.25–0.35). 0.10 genuinely hurts (worse drawdown in validation); ≥0.50 adds cost without robust gain. |
| Switching hurdle (0.05) | **Mildly conservative.** 0.035 dominates in development (Sharpe 0.52 vs 0.29) and matches in validation (0.78 vs 0.75). Region 0.02–0.075 all defensible; 0.0 works only by paying ~4× the trade rate. |
| Position count (25) | **Concentration does not robustly pay.** Dev prefers MORE names (30 best); validation is mixed (10/20/30 similar). Region 20–30 is stable; 8–12 is zone-dependent. |
| Exit-buffer hysteresis | Mild improvement when widened (1.2→1.6×N); flat in validation. |
| Sector cap | **Not swept historically** (no PIT sector data; censused live instead — it has not yet bound in any real proposal). |
| Gross exposure | **Not swept** — long-only ≤100% is architecture; a >1 shadow would need a second allocator, forbidden by principle 1. |

**Three SHADOW policies frozen a priori** (round values, declared before any
zone was scored, hashes in `R53_SHADOW_POLICIES.json`):
`CURRENT_CONSERVATIVE_POLICY` (production verbatim),
`MODERATE_ACTIVE_POLICY` (N=15, hurdle 0.035, turnover 0.50, exit 24),
`HIGH_ACTIVE_POLICY` (N=10, cap 12%, hurdle 0.02, turnover 0.75, exit 20).
On the historical proxy the aggressive policies are ZONE-UNSTABLE (HIGH: dev
Sharpe 0.19 / val 0.82) — exactly why they stay shadow challengers to be
compared **prospectively** on future governed cycles through the same kernel
seam. **Production policy unchanged.**

**Risk-philosophy note.** The competition run (Track C) demonstrated the
architectural gap directly: a diversifying sleeve cannot clear a hurdle
expressed purely in score percentile — diversification value is invisible to
the switching arithmetic. The canonical primitive to evolve toward is a
**risk budget owned by the existing constraint owner** (volatility /
drawdown / concentration / turnover budgets as first-class policy terms the
kernel already partially owns), not a second allocator.

---

## Track B — Intraday runtime, honesty about the feed, and the factory

### Runtime status (`alpha_agent/r53/runtime_status.py` → `R53_INTRADAY_RUNTIME_STATUS.json`)

**Continuous collection is NOT running, and has not been since 2026-08-28
17:53Z.** Read-only inspection established the exact mechanism:

* The worker ran continuously 2026-08-21 → 2026-08-28 (loop 2512, 7/7
  sources healthy, ~60s cadence).
* At the 2026-08-28 logon the task relaunched it; the dead worker's lock
  heartbeat was ~100s old; the strict reclaim rule (silent > 900s AND pid
  gone) refused the slot → `SINGLE_FLIGHT_LOCK_HELD`, exit 3.
* The task's ONLY trigger is a LogonTrigger (Interactive principal, no
  retry, no repetition) — so a refused start is terminal until the next
  logon. Task `LastTaskResult = 3` still shows it.
* `PaperTrader-ResearchRuntime` (R52) is healthy: last run 09-01 08:15
  result 0, next 17:45, principal **S4U** (the R52 migration is done).

**Remediation:**
* **Code (done in R53):** `api.information_collection.acquire_service_lock_with_wait`
  — a live holder is refused instantly (single-flight preserved); a provably
  dead holder is waited out within the takeover window. The worker script
  starts through it, with signal handling installed before the wait. The
  audit's delegation invariant was extended to accept the wait variant.
* **Scheduler (operator, elevated; R53 changed no task):** re-register
  `PaperTrader-InformationCollection` with periodic triggers /
  restart-on-failure and the S4U principal — the same lifecycle treatment
  R52 gave the research runtime task.

### The intraday feed truth

The canonical lane owner (`alpha_agent.r46.intraday`) was re-probed LIVE
during regular hours with the venue key in the shell: **DATA_BLOCKED** —
Norgate is daily-only by construction, Polygon answered HTTP 403 for
current-session bars (owned plan is end-of-day), the R38/R45 minute panels
are frozen history. A 30-minute prediction stamped against data that arrives
after the horizon closes is not prospective; none was fabricated. Session
close remains served by the daily h1 cell.

### The intraday alpha factory (`alpha_agent/r53/intraday_factory.py` → `R53_INTRADAY_FACTORY.json`)

Built NOW so the evidence clock starts the day a feed exists:

* **8 frozen specifications across 7 economically distinct families**
  (opening-gap continuation + reversal partitioning the gap domain, intraday
  momentum per Gao-Han-Li-Zhou, intraday reversal, realized-vol breakout,
  sector-vs-index relative strength, abnormal-volume confirmation,
  volatility→equity lead-lag), all literature-standard parameters, all
  hash-frozen, `NOT_CALIBRATED`, shadow-only.
* **Prospective machinery on the canonical desk chain-hash primitives** (no
  second forward-evidence system): slot clock (10:00/12:00/14:00 ET, 15-min
  grace), TRUE_FORWARD-only ledger that refuses non-strict emission ordering,
  missing fields, stale inputs (>20 min) and duplicates (first emission
  wins); forfeiture ledger where every row must carry
  `backfill_refused: true` (the R52 convention); outcome scoring that
  accepts **MATURED windows only** and never blends mark-to-market (the
  R46.5 rule). Structural blocks (no feed) are distinguished from
  operational misses (window existed, nothing emitted) exactly as R52 split
  them.
* Authority boundary: intraday movement stays RISK authority, events stay
  TRIGGER authority; no R53 row carries expected-return authority; the R46
  promotion gates remain the only exit from shadow.

### Multi-horizon shadow view (`alpha_agent/r53/multi_horizon_view.py` → `R53_MULTI_HORIZON_VIEW.json`)

One page per capital pool: the production daily percentile per holding
(the ONLY production-authoritative signal), 51 tactical shadow rows from the
frozen challengers' current books per horizon, the intraday factory state,
and a per-instrument example view. **Five aggregation architectures
evaluated, none adopted** (zero matured intraday rows cannot calibrate any
combination rule; R44/R45 measured exactly that failure mode). Standing
rule: horizons do not average — they compete for capital through the one
allocator after promotion.

### Latency profile (`alpha_agent/r53/latency.py` → `R53_LATENCY_PROFILE.json`)

Measured from the 17 owned DRC manifests and the collection iteration
history:

* Median full cycle ≈ **303s**; worst 2026-08-31 cycle ≈ 21.4 min.
* Dominant bottlenecks: `ADVANCE_PROSPECTIVE_TOURNAMENT` (median ≈ 736s on
  runs where it fires) and `CAPTURE_FORWARD_EVIDENCE` (≈ 279s) — research
  accrual, not decision-making.
* **The governed decision chain (HOC → reassessment → proposal) runs in ≈
  7.3s median.** Event-driven reassessment does NOT need the research
  bolt-ons, so a ~5-minute event-to-decision budget is already realistic;
  true-intraday (60s) requires the incremental refresh path only.
* Collection, when running, detected new information within ≈ 30–90s.

---

## Track C — Shadow cross-market capital competition

**Owner:** `alpha_agent/r53/capital_competition.py` →
`R53_SHADOW_CAPITAL_COMPETITION.json`. Hermetic, through the REAL owners:
registry `approvals=` seam → opportunity frontier → the production-policy
constraint kernel → switching economics. Twelve scenarios (five flagship
sleeves alone + jointly, at the actual NAV and a $1M counterfactual).

**Findings:**

* **At the actual ~$99k NAV, no non-equity sleeve can receive ANY capital
  even with full hypothetical approval.** Every candidate contract's unit
  notional exceeds the 10% name cap
  (`UNIT_NOTIONAL_EXCEEDS_NAME_CAP_AT_NAV`). The binding constraint on
  cross-market capital today is **unit granularity, ahead of the evidence
  gate** — the same wall R51 measured, now quantified as a competition.
* **At $1M NAV**, the FX-carry sleeve's long leg (&6M) becomes executable
  and takes the name-cap weight; most commodity/equity-index contracts
  (&SI ≈ $335k, &HO ≈ $185k …) remain unit-blocked even there.
* **Short-only signals cannot receive long capital**: the rates
  (copper/gold → short &ZN) and volatility (carry → short/flat &VX) sleeves
  currently express bets the long-only production mandate cannot hold, and
  the artifact says so per sleeve.
* **Even when eligible, the switch does not clear the 0.05 hurdle**
  (net ≈ 0.027 at $1M with FX injected) — and the hurdle arithmetic cannot
  see diversification value, which is the concrete case for the Track-A
  risk-budget primitive.
* Diversification advisory (advisory-only, `engine.cross_asset_risk`, &ES
  book proxy): several currency futures reduce portfolio volatility at a
  10% delta; most commodity legs do not at current correlations.

### Two new frozen daily challengers (the offensive continues)

Through the canonical R46 door, cohort `R53_CROSS_MARKET_OFFENSIVE`, frozen
2026-09-01T14:29:22Z, `retune_free`, ZERO new historical trials, both
`CAN_ACCRUE` / `FORWARD_PENDING`:

* `r53_fut_xs_value_5y` — all-futures cross-sectional VALUE: the negative of
  the past 5-year return (Asness-Moskowitz-Pedersen 2013), thirds, h20 — the
  one own-price premium the field was missing.
* `r53_comdty_xs_skew_12m` — commodity 12-month realized-SKEWNESS sort
  (Fernandez-Perez–Frijns–Fuertes–Miffre 2018), long low-skew / short
  high-skew, thirds, h20 — a higher-moment sort key no live cell reads.

Six adjacent hypotheses declared and DECLINED with written reasons
(`R53_DECLINED`), including the intraday cells (feed-blocked) and the
crypto revival (unchanged). Their first TRUE_FORWARD emissions belong to
tonight's scheduled R52 runtime (17:45 ET) — no manual advance was run.

---

## Registry / evidence state after R53

45 R46-door challengers (43 + 2), 52 with adopted shadows; burden unchanged
(no new historical trials); chains intact; `PROMOTION_READY = 0` (frontier
refreshed by the runtime this morning); production still
`us_equity_fundamental_momentum_50_50_v1` + `cash_usd`.

## Files changed

**New:** `alpha_agent/r53/` (7 modules), `scripts/r53_store_hash.py`,
`tests/test_release53_active_risk_intraday_alpha.py`, this document.
**Modified:** `alpha_agent/r46/challengers.py` (R53 cohort + 2 signal owners;
no earlier tuple touched), `alpha_agent/r46/emit.py` +
`alpha_agent/r46/feasibility.py` (probe-map entries),
`api/information_collection.py` (bounded-wait acquire),
`scripts/run_information_collection_service.py` (starts through it),
`scripts/audit_architecture.py` (delegation invariant accepts the wait
variant), three R46 baseline tests (sanctioned cohort-growth pattern),
`PROJECT_STATE.md`.

## Safety attestation

Production stores hashed before and after: **byte-identical**. No order, no
fill, no broker, no approval, no promotion, no sleeve activation, no
production policy change, no scheduler mutation, no Daily Close / DRC /
Portfolio Cycle call, no backdated or backfilled forward row, $0 spent.
