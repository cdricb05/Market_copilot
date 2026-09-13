# Alpha Recovery Operating Contract

**Status:** PERMANENT PROJECT CONTRACT (not a release narrative).
**Contract version:** 1
**Applies to:** every agent, session, release, worktree and human proposing or
implementing Paper Trader work from 2026-09-10 onward.
**Read with:** `docs/PROJECT_CHARTER.md` (eight principles, safety boundaries),
`docs/RESEARCH_CAMPAIGN_CONTRACT.md` (frozen research budgets and lockbox
rules), `docs/INFORMATION_PURCHASE_GATE.md` (ten conditions before a purchase).
**Machine-readable companions:** `research/alpha_recovery/alpha_recovery_checkpoint.json`
(the frozen 10-session stop-loss) and
`research/alpha_recovery/alpha_recovery_scoreboard.json` (the primary progress
measure), both owned by `alpha_agent/alpha_recovery/`.
**Guard:** `scripts/audit_architecture.py::check_alpha_recovery_operating_contract`
and `tests/test_alpha_recovery_offensive.py`.

---

## 0. Why this contract exists

Sixty-four releases produced a governed, safe, point-in-time-honest research
and paper-trading estate - and **8,380 settled hypotheses across 311 families
with zero QUALIFIED**. The incumbent operational model has never been shown to
carry alpha; it has been treated as the reference portfolio because it exists.
Architecture, dashboards, naming and cleanup absorbed the calendar while the
investment question stayed unanswered.

There is now ONE top-level objective, and this document makes it impossible to
forget.

## 1. The primary outcome

Produce one or more forecasting / model specifications that **materially
outperform the incumbent after realistic costs** and enter governed
TRUE_FORWARD competition,

**or**

prove that the owned / free information estate cannot presently produce
adequate alpha (`OWNED_FREE_INFORMATION_EXHAUSTED`) and identify the **exact
missing information** with a quantitatively defensible acquisition / purchase
case.

Architecture, dashboards, UI polish, naming, refactoring and cleanup are NOT
primary objectives. They are done only when they are demonstrated blockers to
the alpha objective, documented as `BLOCKER / ALPHA IMPACT / MINIMUM FIX`, and
implemented as the minimum fix.

## 2. What every future agent must do before proposing work

1. Read this contract first.
2. State, in the proposal, **which alpha objective the proposed work advances**
   (a challenger, a forecast product, evidence capture, prospective
   competition, the exhaustion / purchase case) - or state that it is a
   documented blocker to one of them.
3. Refuse to make non-blocking infrastructure, UI or cleanup the primary next
   milestone.
4. Measure project progress by **investment evidence** (the scoreboard in
   section 4), never by release numbers, lines changed, tests passed or
   architecture work.

## 3. The seventeen permanent rules

1. **Alpha is the primary objective.**
2. The incumbent `fundamental_momentum_50_50_v1` is the **BENCHMARK**, not the
   presumed correct portfolio model.
3. Large alpha-driven portfolio rotations may not be justified solely by
   changes in an uncalibrated incumbent score. Risk, liquidity, integrity and
   other true hard constraints remain separate and remain in force.
4. Every serious new experiment is measured **against the incumbent** on an
   identical sample (universe, PIT dates, costs, risk budget, evidence
   partitions) or, where the asset domain differs, against the
   **incumbent-only portfolio as an incremental capital allocation** under the
   same total risk / capital budget.
5. **A model score is not an expected return.** A field that cannot be
   calibrated honestly is null / `UNAVAILABLE`, never a score wearing a
   return's name.
6. `MODEL_GOVERNANCE_OK` does not mean `MODEL_HAS_ALPHA`.
7. Historical out-of-sample evidence and TRUE_FORWARD evidence are never
   conflated, never pooled, never averaged.
8. Research significance, economic significance and forward evidence are
   three separate gates. Passing one says nothing about the others.
9. Thresholds may not be relaxed after seeing results. Close is not pass
   (R64's Holm p 0.0509 against 0.05 is the standing example).
10. No automatic model promotion.
11. No automatic operational portfolio change.
12. Research continues while forward evidence accrues. Usable research time is
    never left idle waiting for evidence.
13. An exhausted PRICE_STATE family reopens only when genuinely new orthogonal
    information arrives, PIT history materially improves, coverage materially
    improves, or a truly distinct economic implementation resolves a NAMED
    binding failure. Another lag, transform, normalisation, tree or parameter
    is not sufficient.
14. At least **75 %** of newly executed autonomous research specifications /
    compute targets non-exhausted information needs rather than PRICE_STATE
    transformations, unless the measured information frontier itself proves
    that a different allocation has higher expected research value.
15. There is a **10-eligible-market-session project stop-loss**. The campaign
    start session and the deadline session are computed by the canonical
    exchange-session owner (`engine/exchange_calendar.py`, through
    `api/forward_challenger_registry.py`) and frozen in
    `research/alpha_recovery/alpha_recovery_checkpoint.json`. By the deadline
    the system must hold ONE of: (A) at least one materially superior
    challenger in governed TRUE_FORWARD competition; or (B)
    `OWNED_FREE_INFORMATION_EXHAUSTED` with the exact missing information,
    acquisition path, cost, break-even alpha, confidence haircut and expected
    portfolio value. The deadline never moves because results are weak.
16. A challenger enters prospective evidence as soon as it passes the frozen
    qualification gates; nobody waits for day 10. Actual live registration /
    adoption remains **HUMAN-GATED** (`scripts/adopt_prospective_freeze.py`
    with its explicit confirmation token).
17. Release numbers, lines changed, tests passed and architecture work are NOT
    measures of investment success.

## 4. The permanent project scoreboard

`research/alpha_recovery/alpha_recovery_scoreboard.json` (machine-readable,
deterministic: its `artifact_hash` excludes `generated_at`) and
`research/alpha_recovery/ALPHA_RECOVERY_SCOREBOARD.md` (the concise human
rendering) are the **primary progress measure** of the project. They are
rebuilt by `scripts/run_alpha_recovery_offensive.py scoreboard` and never
edited by hand.

Minimum content:

| block | fields |
|---|---|
| INCUMBENT | historical OOS metrics; actual TRUE_FORWARD metrics (kept separate); expected-return calibration if available; directional calibration where applicable; net return; Sharpe; drawdown; turnover; benchmark-relative performance; evidence sample; evidence maturity |
| BEST CHALLENGER | the same fields |
| ADVANTAGE | incremental net return; Sharpe delta; drawdown delta; turnover delta; forecast-calibration improvement; statistical / multiple-testing status; economic materiality; forward evidence |
| STATUS | exactly one of `RESEARCHING`, `HISTORICAL_SURVIVOR`, `READY_FOR_FORWARD_QUALIFICATION`, `TRUE_FORWARD_COMPETING`, `REJECTED`, `OWNED_FREE_INFORMATION_EXHAUSTED` |
| CAMPAIGN | eligible sessions elapsed / 10; sessions remaining; share of new research effort spent on non-price information; number of economically distinct information families tested; candidate specifications alive; number in TRUE_FORWARD competition; highest-value unresolved information gap |

## 5. The stop-loss outcome

At the deadline session the outcome is exactly one of:

- `MATERIAL_CHALLENGER_IN_TRUE_FORWARD_COMPETITION`
- `OWNED_FREE_INFORMATION_EXHAUSTED`
- `STOP_LOSS_BREACH` - a **project failure state**, rendered as such on the
  scoreboard and in `PROJECT_STATE.md`, never softened.

Before the deadline the scoreboard shows `sessions_elapsed` and
`sessions_remaining` at every eligible session.

## 6. Evidence discipline (inherited, not invented)

- Same-domain challengers must satisfy the repository's existing frozen
  materiality contracts: the R57/R58/R59/R63 floors (net advantage over the
  benchmark >= 1.5 %/yr, lockbox sign agreement, halves floor, turnover cap,
  drawdown multiple, Benjamini-Hochberg at q = 0.10 over the whole executed
  denominator) and R64's family-aware Holm control. No weaker threshold may be
  introduced by any later release.
- Cross-domain sleeves require positive incremental portfolio utility after
  costs under equal capital / risk constraints and the same evidence discipline.
- Historical results influenced by R63/R64 are POST-SELECTION evidence; a
  lockbox that has been viewed is not pristine and is reported as such.
- The research / search burden is accounted for explicitly: every executed
  specification counts in the denominator, including failures.
- Point-in-time joins, walk-forward out-of-sample evaluation, nested model
  selection, purging / embargo, serially robust (Newey-West) inference,
  family-aware multiplicity, calibration tests and regime partitions are the
  tools; a result that skipped one says so.
- Textual / news information is point-in-time or it is not evidence: no
  current article is ever inserted into a historical period.

## 7. Research budgets per information family

- Maximum **6 primary specifications** per economic information family.
- Maximum **2 rescue specifications**, only when a specific MEASURED binding
  failure is named and the rescue addresses it. No cosmetic parameter search.
- Then the family is closed and the next frontier item is taken.

## 8. Safety (unchanged, permanent)

Paper only. Preview first. Manual review. No orders, no fills, no broker, no
automatic promotion, no automatic portfolio change, no purchase, no
subscription, no trial, no live-store write from research. The live checkout
`C:\Users\binis\paper_trader` is read-only for research work. Nothing in this
contract loosens any safety boundary in `docs/PROJECT_CHARTER.md`.

## 9. Completion tokens

A campaign session under this contract ends with exactly one of:

    ALPHA_RECOVERY_MATERIAL_CHALLENGER_READY
    ALPHA_RECOVERY_OWNED_FREE_INFORMATION_EXHAUSTED
    ALPHA_RECOVERY_IN_PROGRESS_WITHIN_STOP_LOSS
    DO_NOT_COMMIT - <true blocker>

A success token is never issued merely because implementation completed.

## 10. The question that decides whether work is worth doing

    WHAT WILL PRODUCE THE BEST RISK-ADJUSTED RETURN FROM CAPITAL,
    BASED ON INFORMATION AVAILABLE AT THE DECISION TIME?

If a piece of work does not move that answer forward, it is not done under
this contract.
