# Release 66 — hedged relative value, and the SPY options evidence defect

**RUN_ID** `R66_HEDGED_RELATIVE_VALUE_AND_PORTFOLIO_PNL_OFFENSIVE`
**Date** 2026-09-22
**Terminal** `R66_NO_CELL_MEASURED_ZERO_BURDEN_CHARGED` (research) +
`R66_SPY_COLLECTION_DEFECT_REPAIRED` (runtime)

Research only. Paper only. No order, no fill, no promotion, no adoption, no
automation, no backfill. Research `NEW_PAID_DATA_COST = $0`; the runtime repair
spent **$0.0226** of the producer's own pre-authorised $0.25/session budget.

---

## 1. The SPY options defect: the vendor published, we failed to collect

Two registered challengers — `REVERSED_SPY_PUT_CALL_SKEW_H5` and
`…_NEXT_OPEN_V1` — had reported `AWAITING_SOURCE_PUBLICATION` for eight days.
That word blamed the vendor. **The vendor was not at fault.**

### Root cause, at one line

`alpha_agent/alpha_recovery/options_acquisition.py` — `underlying_levels()` read
a **frozen one-off ES futures research panel that ends 2026-09-10** and which
nothing refreshes (correct for a frozen panel, fatal as a band anchor).
`plan()` centres each expiry's strike band on a median of those levels. For the
near leg the frozen rule needed (`2026-10-16`), neither of `plan`'s two windows
found an owned level, so the expiry was dropped by a **bare `continue` that
recorded nothing** — while the sibling failure path three lines later correctly
recorded into `errors`.

Consequence chain, measured live:

```
near leg needed        2026-10-16
plan() returns         1 request  (2026-09-18 only)
near-leg filter        discards it
n_requests             0
budget gate            $0.00 <= $0.225   -> PASSES
download               writes nothing
build_surface          NO_ROWS
entry_state            AWAITING_SOURCE_PUBLICATION   <- blames the vendor
r52 runtime            DATA_BLOCKED
runtime journal        {"stage": …, "state": "DATA_BLOCKED", "duration_ms": …}
```

The publication probe had recorded **PUBLISHED** for 2026-09-15 … 2026-09-21.
The data was there. We never asked for it.

### It stayed invisible because the journal threw the reason away

`alpha_agent/r52/runtime.py` kept exactly three fields per stage — `stage`,
`state`, `duration_ms`. `blocked_on` was present in the advance dict all along
and was never read. Six-plus cycles a day for eight days recorded the word and
never once the reason.

### A second, independent silence, found while proving the first repair

The surface's **last date** and the **last date its scored field actually
works** are different questions, and only the first was ever asked.

| | pre-repair archive | after repair |
|---|---|---|
| last date in surface | 2026-09-11 | 2026-09-21 |
| last **usable** `skew` | **2026-08-28** | 2026-09-21 |

The surface had gone on advancing its last date for **nine sessions while the
feature was dead**. So the true blind period was **2026-08-31 → 2026-09-21, 15
sessions**, not the 6 the gap calculation believed.

### The repair

| # | Change | File |
|---|---|---|
| A1 | The band anchor is **extended**, never replaced: dates the frozen panel covers keep exactly their ES/10 level, so every band centre ever used is reproduced byte-identically; only dates beyond it — which had no anchor at all — come from the owned SPY close series. | `options_acquisition.py` |
| A2 | An expiry that cannot be centred is **recorded in `errors`**, not dropped silently. | `options_acquisition.py` |
| A3 | New refusal `NEAR_LEG_NOT_PRICEABLE`: an append whose *needed* near leg was not priced refuses by name instead of buying nothing and blaming the vendor. **This is the safeguard.** | `next_open_runtime.py` |
| A4 | `blocked_on` and a new `blocked_owner` are attached to the stage row. | `r52/runtime.py` |
| A5 | The journal carries a small allow-list instead of three fields. | `r52/runtime.py` |
| A6 | `blocked_on` **resolves its disjunction** — the probe already knows whether the vendor published, so the message names `VENDOR` or `LOCAL_COLLECTION`. | `next_open_challenger.py` |
| A7 | `_surface_last_usable_session()` reports the divergence above. Diagnostic only — the gap logic is unchanged. | `next_open_runtime.py` |

### Proof it worked

```
near leg 2026-10-16   priced $0.022566   (was: silently dropped, n_requests 0)
append                APPENDED, 1 band downloaded, $0.0226 spent
surface               503 -> 509 dates,  now ends 2026-09-21
last usable skew      2026-08-28 -> 2026-09-21   (15 sessions recovered)
```

**Integrity of the frozen evidence — verified, not asserted.** Comparing the new
surface against the archived original on contract identity
(`date, expiration, type, strike`):

- **0** old rows missing; **77,114** common keys; **0** columns changed.
- Derived `skew`: **0 values changed**, max change `0`.
- 9 dates flipped `NaN → value`; **0 values lost**. Those 9 were previously
  unusable and are now computable.

The append added information and rewrote nothing.

### The remaining blocker is now genuine, and correctly attributed

```
information_session  2026-09-22
blocked_owner        VENDOR
blocked_on           the historical vendor has not published session 2026-09-22
                     yet (publication_state=NOT_PUBLISHED); no local action can
                     change this
```

### Forfeiture semantics: unchanged, deliberately

`forfeitures_total = 0` is **correct under the contract as written** and was not
touched. The contract reserves forfeiture for a frozen decision whose emission
window shut; a cadence boundary at which the owner never froze anything is
`AWAITING_NEW_GOVERNED_FREEZE` — a fact about governance, not a loss. The
architecture audit's `a missing freeze is not a forfeiture` check passes, no
historical prediction was manufactured, and the `forfeitures/` directory has
still never been created.

### One known limit, requiring an operator action

The persistent worker is long-lived and **holds pre-fix imports**. It will keep
running the old anchor until restarted. Command in §4.

---

## 2. Hedged relative value: what the data and the estate actually permit

The mandate named five families. **Two are data-blocked, three are settled, and
exactly one unrowed axis survived — which then failed its own gate.**

| Family | Verdict |
|---|---|
| **A** Treasury vs interest-rate swap | `REQUIRED_DATA_MISSING` — the owned FRED panel has 39 columns and **zero** swap columns. FRED's `DSWP*` were discontinued in 2016 and nothing replaced them. |
| **B** Treasury cash–futures basis | `REQUIRED_DATA_MISSING` on **two independent fields** — no CUSIP cash-bond prices, and no conversion factors / deliverable basket. CTD economics are *not computable*. |
| **C** Yield-curve RV | **SETTLED.** `RATES_CARRY_CURVE_RV` closed; R43 prosecuted all six US curve spreads. |
| **D** Related-futures RV | **SETTLED.** R43 prosecuted commodity calendars (1v2, 1v3) and twelve production-chain spreads. |
| **E** FX / commodity RV | **SETTLED / REGISTERED.** FX carry is already registered forward — accrue, do not re-test. |

These are **data and duplicate-identity blockers, not economic findings**, and
none may be reported as `NO_ALPHA_EVIDENCE`.

### The one axis with no prior row

Verified at source: `alpha_agent/r43/rv.py:81-98` declares 24 rates structures.
Every one is a same-country **curve** spread or a cross-country spread of the
same tenor. The three STIR structures are all **cross-country** (`ZQ/LEU`,
`SR3/SO3`, `SR3/YIR`). **There is no `(ZN,SR3)` and no `(ZN,ZQ)`** — the
swap-spread axis, which trades government against collateralised funding rather
than curve shape, and which *has* gone negative post-2008 in a way a curve spread
cannot express.

The director registered it conditionally, froze every parameter in advance
(long ZN / short beta-scaled SR3, lagged rolling beta `BETA_WIN=63`, expected
sign `+1`, H21 on a sample-anchored grid, primary statistic = **increment over a
volatility-matched passive book**, zero conditioning variables), and put two
gates in front of it.

### It failed Gate A, and could not have passed at any parameter

| | |
|---|---|
| ZN daily vol | 0.00361 |
| SR3 daily vol | 0.00013 (**27× smaller**) |
| correlation | 0.3253 → **R² = 0.1058** |
| frozen hedge variance reduction | **−0.0504** |
| floor | 0.25 |

For a single linear hedge the maximum achievable variance reduction **is** the
R², 0.1058. **No beta, no window, no estimator reaches 0.25 with SR3.** The
frozen rolling beta (median 15.4) makes the position *riskier* than unhedged ZN.

The director had named this exact risk in advance: a 3-month rate future cannot
hedge 6.5 years of duration, so what remains is *"the HUMAN_GATED unconditional
long-ZN book wearing a hedge as a disguise."* At a 10.6 % ceiling, that is what
it is.

**`WITHDRAWN_BEFORE_MEASUREMENT` · `DATA_HOLD` · burden charged 0 ·
`NO_ALPHA_EVIDENCE` not filed** — the cell computed no return, so it said nothing
about alpha in either direction.

The deeper finding: **a real fixed-income hedge is a DV01 hedge.** DV01 needs
duration, which needs the swap curve or the bond/contract analytics. The estate
owns neither, so it can only hedge like-for-like points on one curve. That is a
capability limit, measured.

`r66_02` (terms-of-trade cross-asset RV) was **refused before pre-registration**
as the fourth sighting of *timing a premium with a conditioning variable*.
CROSS_ASSET stays at 2,448.

**Campaign burden charged: 0.** RATES_FUTURES stays 864.

---

## 3. The primary deliverable: can this book hold any of it?

Commissioned by the director as ruling 6, and the more valuable half of R66.

**Book:** NAV **$99,127.48**, free cash **$4,482.71 (4.5 %)**, 25 equity
holdings, eligible session 2026-09-21.

**Verdicts across eleven structures: 0 `IMPLEMENTABLE`, 1
`SINGLE_POSITION_ONLY`, 10 `NOT_IMPLEMENTABLE_AT_THIS_NAV`.**

The constraint that stops almost all of them is **not margin**:

- **Margin is a POLICY constraint** — relievable by holding more cash.
- **Gross notional is a STRUCTURAL constraint** — set by exchange contract size
  against NAV, and **no cash policy changes it**. At a 30 % cash policy, ten of
  eleven can post margin and ten still carry more market exposure than the whole
  portfolio.

One `ZT` contract is **$205,906 — 2.08× the entire NAV**. The mandate's headline
structure, ZN/SR3, needs **1:17 — eighteen contracts, $4,203,247, 42.4× NAV** —
the worst of the eleven, for the same reason Gate A failed.

The single survivor is **corn/wheat `ZC/ZW` 2:1**, gross notional 0.83× NAV,
which misses today by **$82 of margin** and becomes holdable at a 10 % cash
policy.

**The leverage trap, made concrete.** That structure carries $82,450 of gross
notional on $4,565 of margin — ~18:1. A 1 % return on gross notional is 0.83 %
of NAV *and simultaneously* 18 % "return on committed capital". The second number
is the first times the margin ratio. Portfolio returns are reported on **NAV**.

**What would fix it, and whether we own it.** Micro futures cut notional ~10×.
The estate owns **no micro Treasury and no micro FX** — the only micros in the
105-market registry are equity index and crypto, and none reached the certified
layer.

**The blocker named precisely: not economics, not evidence, not approval —
instrument granularity, plus the absence of owned micro-contract data.**

### The forward-evidence book as a portfolio

8 registered · 5 emitted · **0 matured** · **0 effective observations** · 0
forfeitures. Six of eight are US equity risk (four `US_EQUITY` plus two
`US_ETF` on the *same* underlying). Capacity is not binding and cannot be:
nothing has matured, so no correlation or diversification statistic on this book
would be measuring anything yet.

---

## 4. Validation and operator actions

Architecture audit: **exit 0**, including `a missing freeze is not a forfeiture:
True` and inventory drift `OK`.

Targeted regressions: **139 passed** across the next-open, same-session,
FX-carry and new R66 suites; the R66 suite itself is **19 tests**.

Known **environmental** failure, pre-existing and unrelated:
`test_release59_persistent_research_runtime.py::test_a_development_worktree_may_never_be_promoted_into_a_service`
fails **by construction** from the deployed checkout (documented environmental
failure #3). The `PaperTrader-ResearchRuntime` task was verified still
`Running` afterwards.

```powershell
# 1. Regressions
& C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe -m pytest -q `
    tests\test_release66_next_open_collection_repair.py `
    tests\test_release62_3_3_next_open_challenger.py `
    tests\test_release62_3_4_next_open_live_runtime.py `
    tests\test_release62_3_5_forward_runtime_integration.py `
    tests\test_release62_3_same_session_true_forward.py `
    tests\test_fx_carry_cadence_forward_runtime.py

# 2. Strict architecture audit
& C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe scripts\audit_architecture.py

# 3. REQUIRED - the persistent worker holds pre-fix imports and will keep
#    running the old band anchor until it is restarted.
& C:\Users\binis\paper_trader\scripts\manage_research_runtime.ps1 -Action Restart -Execute

# 4. Rebuild the feasibility artifact (read-only, zero burden)
& C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe `
    research\agents\campaign_r66_hedged_rv\build_feasibility.py
```

A backend restart is **not** required — no API surface changed.

---

## 5. Artifacts

| Artifact | What it holds |
|---|---|
| `research/agents/campaign_r66_hedged_rv/DATA_CERTIFICATION.json` | the nine data families, measured |
| `…/PRIOR_ART.json` | R43's 24 rates structures and the duplicate-identity argument |
| `…/DIRECTOR_R66_RULING.json` | the six rulings and every frozen parameter |
| `…/GATE_A_HEDGE_MATERIALITY.json` | the withdrawal, and why no re-parameterisation is permitted |
| `…/PORTFOLIO_FEASIBILITY.json` / `.md` | the primary deliverable |
| `…/CAMPAIGN_RESULT.json` | terminal state and escalations |
| `D:\…\r66_hedged_rv\STRUCTURE_FEASIBILITY.json` | per-structure record |

## 6. Escalations for the human owner

1. **Diversity gate NOT SATISFIED** — 1 asset class against 3, 0 H1-H5 against
   1, 0 cross-asset against 1. Unsatisfiable here by construction: the only
   fixes are padding to a quota (forbidden by the protocol *and* by the
   operator's own instruction) or widening the contract, which is a human
   decision. Declared, not waived.
2. **Prospective registration of a static hedged premium** needs human
   authorisation **and** a live producer. The research substrate is frozen at
   2026-08-21, one session-month behind the book.
3. **The month-end ZN long** stays `HUMAN_GATED`; refused outright for R66.
