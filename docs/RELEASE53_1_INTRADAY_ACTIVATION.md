# Release 53.1 — Intraday Activation & Alpha-to-Capital Conversion

**Date:** 2026-09-01 · **Branch:** `stage19-controlled-rebalance` (working
tree over `949ca9d`, together with Release 53) · **Status:** research +
operating-resiliency release. Paper-only, shadow-only, preview-first. No
production policy change, no promotion, no sleeve activation, no orders, no
scheduler mutation from research code, $0 spent.

Release 53 built the machinery and named the walls. Release 53.1 converted
four of them into capability.

---

## 1. The intraday feed wall is DOWN (Track B)

R53 ended with the intraday factory frozen at `SPECS_FROZEN_AWAITING_FEED`
because the only sources probed (Norgate, Polygon free tier, historical
panels) could not serve a current-session bar. R53.1 exhausted the owned
estate the prompt demanded, and the wall turned out to be an unprobed door:

| Source | Measured today | Class |
|---|---|---|
| Yahoo chart via `engine.market_data` (canonical owner, extended) | 5-min OHLCV bars for the CURRENT session, exchange-stamped, ~30-166s behind | **NEAR_REAL_TIME** (bars) |
| Tiingo IEX (owned key) | last trade, ~0s delay | REAL_TIME (quote-only) |
| Finnhub (owned key) | quote, ~30s delay | NEAR_REAL_TIME (quote-only) |
| EODHD (subscription) | delayed OHLC quote, ~15.8 min | DELAYED_INTRADAY (quote-only) |
| AlphaVantage intraday / FMP / Polygon aggs | premium-gated / legacy-dead / 403 | NOT_ENTITLED |
| Norgate | daily by construction (R45 measurement stands) | DAILY_ONLY |

New canonical pieces, no second owner anywhere:

* `engine.market_data.fetch_recent_intraday_bars` / `fetch_current_session_bars`
  — the declared canonical Yahoo owner extended from "latest quote" to bars;
* `alpha_agent.r46.intraday` — the ONE lane owner now probes all owned
  sources and carries the honest latency taxonomy
  (REAL_TIME / NEAR_REAL_TIME / DELAYED_INTRADAY / DAILY_ONLY /
  NOT_ENTITLED); lane state today: **AVAILABLE_NOW**;
* `alpha_agent.r53_1.intraday_feed` — the normalization adapter (completed
  bars only, measured freshness, same-feed marks);
* `alpha_agent.r53_1.intraday_signals` — the eight FROZEN R53 specs
  implemented verbatim (hashes verified against the registration-time
  record; conventions declared, nothing retuned);
* `scripts/run_intraday_emission.py` — the ONE slot runner (probe → snapshot
  → score matured → emit → attempts sidecar → artifact).

**First evidence:** at the legal 12:00 ET slot today the runner emitted
**36 TRUE_FORWARD predictions** (gap-continuation 9, intraday-momentum 9,
sector-relative-strength 18; the other five specs refused honestly —
thresholds unmet), data freshness 6 seconds, emitted strictly before every
outcome window, chain-hashed, deduplicated. 11.4 seconds end-to-end.
`intraday_feed_gate.json` records the purchase gate as
**NOT_REQUIRED_OWNED_SOURCE_USABLE**.

## 2. Continuous collection is repaired in code and contract (Track A)

The 2026-08-28 outage was an OPERATING failure: Interactive principal,
logon-only trigger, no periodic recovery (R53 fixed the lock-side race).
R53.1 finishes the envelope with the R52 installer pattern:

* `scripts/install_information_collection_task.ps1` — the ONE definition
  owner: S4U, boot trigger (2-min delay), a DAILY trigger repeating every 30
  minutes for a one-day duration as the recovery clock (consecutive one-day
  windows abut, so coverage is continuous; IgnoreNew makes each firing a
  no-op while the worker lives), no execution time limit, full-definition
  comparison, hermetic `-DecisionProbe`/`-TriggerProbe`/`-ClassifyProbe`,
  explicit `-Force` migration. *Hotfix 2026-09-01:* the first operator
  migration proved Task Scheduler REJECTS a serialized `TimeSpan.MaxValue`
  repetition duration (`P99999999DT23H59M59S`) as "incorrectly formatted or
  out of range" — the daily/P1D shape is the scheduler's own UI preset with
  the same recovery semantics, and registration failures are now classified
  honestly (a definition/XML rejection is never blamed on elevation);
* `scripts/validate_information_collection_task.ps1` — read-only validator
  (Interactive NEVER validates) plus live worker/heartbeat reporting;
* `manage_information_collection.ps1::Install-CollectionTask` now DELEGATES
  registration to the installer (inline registration removed);
* `scripts/audit_architecture.py` — the collection-ownership invariant
  evolved: legal script set is exactly {manager, installer, validator}, the
  manager must delegate and may not register inline, the validator must be
  read-only. Strict audit exits 0.

Installation needs an ELEVATED shell (S4U) and is the OPERATOR's action;
research code changed no scheduler state.

* `scripts/install_intraday_emission_task.ps1` — the same pattern for
  `PaperTrader-IntradayEmission` (daily 10:00/12:00/14:00 + 16:20 scoring
  pass; StartWhenAvailable deliberately OFF — a missed slot is a recorded
  forfeiture, never a late run). For today only, a detached watcher covers
  the 14:00 slot and the 16:21 scoring pass.

## 3. Risk is a budget, not a veto (Track C)

`alpha_agent.r53_1.risk_budget` extends the canonical machinery (cross-asset
risk maths, R53 competition seams, frontier percentile scores — all reused)
with the quantity the 0.05 score-only hurdle cannot see: **expected-return
strength per unit of incremental portfolio risk**, under explicit budgets
(volatility / unit-weight / asset-class / turnover / collateral) for the
three a-priori shadow policies. Measured today at the actual $99,113 NAV:

* **&VX correlates −0.71 to the book** — the single best measured
  diversifier, inexpressible today (short-only signal, long-only mandate)
  and one whole unit is 16.7% of NAV;
* **&MBT at 8% weight adds only +2.8bp/day portfolio volatility while
  diversification absorbs 8.8bp** — three quarters of its standalone risk;
* under MODERATE budgets &M2K and &MBT are ALLOCATABLE; &MET is correctly
  sized out by the volatility budget (the CASE-2 behaviour working);
* under CONSERVATIVE budgets nothing passes — granularity, exactly as the
  R53 competition found.

Alpha-strength figures are the frontier's ORDINAL percentiles and are
labelled within-sleeve; expected returns remain NOT_CALIBRATED everywhere.
The production hurdle is untouched.

## 4. Executable at ~$99k NAV (Track D)

Probed from the OWNED Norgate databases (nothing assumed):

* **OWNED micros:** MES / MNQ / M2K / MYM (equity index, 1,842 sessions
  each), **MBT (Micro Bitcoin, $7.9k/unit)** and **MET (Micro Ether,
  $249/unit)** — the last two **executable under the PRODUCTION 10% cap at
  today's NAV**. &M2K ($14.8k) fits a 15% shadow cap today (min NAV $148k
  at 10%); &VX ($16.6k) fits at 20%.
* **NOT owned:** micro FX (M6E/M6A/…), micro metals/energy (MGC/SIL/MCL/…),
  micro yield futures, VXM — those sleeves stay unit-locked at this NAV.
* **MET misclassification fixed:** `api.market_reference_data` had CME Micro
  Ether classed as an international equity index future; the owned database
  names it "Micro Ether" (r38's contract table agreed). Corrected to
  AC_CRYPTO_FUTURES, regression-tested.
* **ETF proxies classified** (SAME_THESIS_SAME_MARKET /
  PROXY_WITH_BASIS_RISK / NOT_EQUIVALENT): GLD/SLV/FXE/FXY/SPY/QQQ/IWM carry
  their theses; USO/UNG/IEF/TLT carry modellable basis; a long-vol ETF can
  NEVER express the VX curve-carry signal (NOT_EQUIVALENT).
* **Futures risk-model study** (shadow): dollar-vol per unit spans orders of
  magnitude at equal notional — the case for volatility-contribution sizing
  with notional caps retained as safety limits, for a future release.
* **Short capability** (`short_capability.py`): the allocator's C_LONG_ONLY
  clip, the frontier's long-leg admission and the desk's unsigned quantities
  assume long-only; the risk maths and the forward-evidence rows are already
  signed. Futures shorts ≠ equity shorts (no borrow vs borrow ledger).
  Mandatory controls declared. NOTHING activated.

## 5. Latency

Decision chain (HOC → reassessment → proposal): **7.3s median** (R53, 17
governed runs). Intraday emission runner: **11.4s** end-to-end measured
live. Near-real-time event-to-decision estimate **~45s** — inside the 60s
true-intraday budget. The binding constraint is DETECTION: the 15-minute
quote poll (and, until the task is re-registered, a collector that is not
running at all). The engine is not the bottleneck; the clock feeding it is.

## 6. Verification

* `tests/test_release53_1_intraday_activation.py` — 38 tests: installer
  decisions with the real PowerShell binder, validator principal rules,
  forming-bar/mark honesty, latency-class honesty, gap-cell partition,
  frozen-hash regression, TRUE_FORWARD-through-adapter discipline
  (dedupe/staleness/no-slot), risk-budget NAV conservation + granularity +
  vol-budget + collateral, MET classification, long-only wall location,
  ledger integrity, safety flags.
* Touched-owner suites green: R29 collection, R50 multi-asset, R53, R47,
  market_data — 378 tests in the final batches, 0 failures.
* Strict architecture audit: exit 0 (collection-ownership invariant evolved
  to the three-script contract). `git diff --check`: clean.
* Production stores (61 files) hashed before and after: identical.
