# Release 51 — Non-Equity Alpha Qualification & Operational Promotion Offensive

**One promotion frontier · one honest distance per sleeve · one new frozen
challenger (the missing family) · zero invented evidence · zero promotions**

Research only. Paper only. Preview-first. Manual review mandatory. No broker,
no order, no automation, no model promotion, no purchase. Built on R50
(`b1d588e9`), which left every non-equity sleeve behind exactly ONE blocker:
`NO_APPROVED_OPERATIONAL_SIGNAL` — an evidence gate, not a code gap.

---

## 1. The question and the honest answer

> Which non-equity sleeve is closest to earning operational capital, what
> exact evidence is still missing, and can we close that gap now?

**PROMOTION_READY_COUNT = 0.** The R46 prospective tournament is three
sessions old: 104 forward predictions emitted, 3 matured, and the best cell
holds 2 effective independent observations against a frozen floor of
{1d: 60, 5d: 40, 20d: 24}. No shortcut to future evidence exists, and this
release took none. What R51 did instead was (a) measure every sleeve's
remaining distance honestly, (b) close every gap that could be closed
without waiting, and (c) raise forward-evidence throughput so the waiting
is as short as the calendar allows.

## 2. The promotion frontier (one owner)

`alpha_agent/r51/promotion_frontier.py` — a PURE calculation: every input is
injected (R46 leaderboard / velocity / verdicts / continuation state, the
R50 sleeve records, owned unit economics, NAV), nothing is read from disk,
nothing is written, and the distance score **never replaces the real
gates** (`alpha_agent.r46.contract.FORWARD_EVIDENCE_GATES` decide, alone).
Primary ranking key: weeks of forward evidence still missing at the
projected velocity. A sleeve with no working data path ranks behind every
sleeve with one. Excitement is not a key.

Standings at 2026-08-30 (details in the handoff's
`promotion_distance_ranking.json`):

| # | Sleeve | State | Weeks to evidence floor | Beyond the sample deficit |
|---|--------|-------|------------------------|---------------------------|
| 1 | equity-index futures | ACCRUING | ~4.8 | unit granularity at $99k NAV |
| 2 | volatility (VX) | ACCRUING | ~7.8 | first matured VX row −192 bps net (one row decides nothing); highest cost class |
| 3 | commodity futures | ACCRUING | ~24 | granularity |
| 4 | FX futures | ACCRUING | ~24 | granularity |
| 5 | multi-asset trend | ACCRUING | ~24 | no single-sleeve registry record |
| 6 | event / macro | ACCRUING | ~40 | granularity |
| 7 | rates futures | ACCRUING | ~40 | granularity is BINDING (see §4) |
| — | adopted R39/R40 shadows | CONTINUATION_ARMED | own calendars | first month-end decision 2026-08-31 |
| — | crypto | BLOCKED | — | R42 verdict stands; shadows retired |

## 3. The one new challenger — FX carry, at last

Every serious family in the estate had a prospective clock except **FX
carry** — the strongest non-equity historical prior (R36: rank IC 0.155,
t 7.97; R43: premium real, timing zero) and never frozen forward.

`r51_fx_xs_carry_cip` (cohort `R51_NON_EQUITY_PROMOTION`,
`alpha_agent/r46/challengers.py`): covered interest parity makes the FX
futures curve the market's own print of the short-rate differential, so the
signal is the annualised front/next slope of the OWNED dated FX futures —
the exact Koijen-Moskowitz-Pedersen-Vrugt carry definition, through the
same frozen `futures_curve_carry` arithmetic the commodity curve cell has
used since R46.3, with no external rate feed. Thirds long/short across the
eight CME currency futures (&DX excluded — a basket is not a currency
leg); control cash; FX_FUTURES costs on traded notional; horizons 5/20;
every parameter a canonical constant; **no sweep ran and zero new
historical trials were charged** (global burden unchanged at 353/355).

Frozen through the canonical registry door (41 challengers, retune-free,
zero prior freezes moved) and its first two TRUE_FORWARD predictions were
emitted through ONE canonical `advance()` on 2026-08-30 21:49Z — entering
the 2026-08-31 close, emitted before the outcome window opened; the chain
ledger refused the other 34 cells as duplicates of Friday's batch, exactly
as designed. First live book (read-only smoke): long 6M/6A/6B, short
6S/6J/6C — the textbook carry book.

Six adjacent avenues are **declared and DECLINED** with written reasons
(`R51_DECLINED`): FX PPP value (unaudited CPI vintages), international
short-rate xs (a third expression of the same premium), ML on the futures
cross-section (30 markets is an overfitting engine), crypto revival (no
new hypothesis; R42 stands), VX variants (one cluster, never multiplied),
micro-yield-futures challenger (no owned history to freeze against).

## 4. Micro-contract truth (the R50 granularity finding, closed out)

* **Rates:** NO owned contract fits the 10 % name cap at the $99,383
  production NAV — smallest is ZF at ~$106k unit notional (minimum NAV
  ~$1.06M for one unit). CME **micro Treasury yield futures are not in the
  owned entitlement** (verified against all 124 dated and 112 continuous
  Norgate roots). That is a purchase-gate question for the operator;
  R51 bought nothing. The rates sleeve's science continues to accrue —
  the constraint is implementation at this book size, and **no risk limit
  was relaxed to fit a large contract**.
* Owned micros that exist and are already in the R50 registry roots:
  MES $38.6k / MNQ $59k / M2K $14.9k / MYM $26.8k (equity index),
  MBT $7.8k / MET $245 (crypto). VX at ~$16.9k is the smallest non-crypto
  owned futures unit. Cboe Mini-VIX (VXM) is not in the entitlement.
* Full arithmetic per sleeve at both NAVs: handoff
  `micro_contract_feasibility.json`.

## 5. R50 integration readiness — proven hermetically

For each of the top five sleeves (equity-index, volatility, commodity, FX,
rates): the registry represents it; an **injected** approval through the
`approvals=` seam derives CAPITAL_ELIGIBLE (and without the injection the
sleeve stays ineligible — the production control); rank normalisation is
defined; the frontier consumes the sleeve at
`OPERATIONAL_SLEEVE_NORMALISED_RANK` with `expected_return NOT_CALIBRATED`;
the constraint owner reshapes a mixed book under the declared quarter caps;
one unit values from owned reference data (notional, margin, collateral);
execution + NAV are certified by the R50 suite re-run green on this tree.
All five PASS (`r50_integration_proofs.json`). Five manual-review
pre-packets exist (`promotion_packets/`), every decision
`CONTINUE_OBSERVATION`. **No approval was performed.**

## 6. Cadence — the real bottleneck, named

Forward evidence accrues only when the daily cycle runs; a skipped session
forfeits ~34 emissions permanently (the entry rule forbids backfill by
construction). Monday 2026-08-31 is the **first month-end decision** for
the adopted R39/R40 continuation lanes since R46.6.1 authorised their
ledger, and the next VX-Friday is 2026-09-04. The
`PaperTrader-InformationCollection` task still has only a LogonTrigger
(the R46.6.2 finding); adding a post-close trigger is an operator
decision — **R51 changed no scheduler.**

## 7. Safety and evidence

Money spent $0.00. Trials 0. Promotions 0. Approvals 0. Orders 0. Fills 0.
Holdings/NAV mutations 0. Scheduler changes 0. Operational stores
byte-identical before/after (60 files, `c5537bbc…`). The research delta is
exactly the canonical owners' writes (registry +1 challenger, append-only
ledgers +2 predictions +1 outcome, rebuilt read models, lane captures);
chains intact; verdict CLEAN. 20 new tests; 634 passed / 0 failed across
R51 + R46.x + R50 suites; `audit_architecture.py --strict` exit 0.
Handoff: `D:\Temp\paper_trader_release51_non_equity_promotion_offensive_handoff`.
