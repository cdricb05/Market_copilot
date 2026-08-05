# Paper Trader — Architecture Decisions

> Confirmed, provisional, and unresolved decisions with evidence and
> consequences. Governs [TARGET_ARCHITECTURE.md](TARGET_ARCHITECTURE.md) and
> [CONSOLIDATION_ROADMAP.md](CONSOLIDATION_ROADMAP.md); intent in
> [PROJECT_CHARTER.md](PROJECT_CHARTER.md). Each decision has a **status** of
> CONFIRMED, PROVISIONAL, or UNRESOLVED.

## Operating-cycle decisions (explicit)

### D-1 — Signal refresh runs frequently — CONFIRMED
- **Decision:** prices, features, scores, rankings, forecasts and data-quality
  status refresh as often as the available data supports; the daily research
  cycle is the current cadence, intraday is deferred (Milestone 6).
- **Evidence:** `alpha_target.run_refresh`, `multi_horizon_engine.build_current`,
  `daily_operating_run` already refresh on demand; no scheduler is enabled
  (4 AlphaAgent tasks Disabled).
- **Consequence:** the research-cycle orchestration (Slice 3) must be idempotent
  and cheap enough to run frequently.

### D-2 — Portfolio reassessment runs frequently — CONFIRMED
- **Decision:** after every signal refresh, every holding is compared with the
  strongest eligible alternatives (opportunity cost) to decide whether capital
  is better deployed elsewhere.
- **Evidence:** `daily_action_gate` already recomputes target-vs-holdings each
  event; `portfolio_manager` composes the decision surface.
- **Consequence:** motivates the Milestone-2 opportunity-cost engine (Slice 6);
  reassessment must never mutate holdings (manual review).

### D-3 — Model recalibration is controlled and evidence-gated — CONFIRMED
- **Decision:** models recalibrate/replace only on sufficient new evidence,
  forward deterioration, drift, a formal checkpoint, or a proven challenger —
  never automatically.
- **Evidence:** `decide_tournament` yields eligibility only; factories emit
  `RESEARCH_ONLY`/`CHALLENGER_ELIGIBLE`; audit `research_execution_terms = 0`.
- **Consequence:** the unified `model_registry` (Slice 8) keeps promotion manual
  (Principle 7).

## Structural decisions

### D-4 — One authoritative owner per business concept — CONFIRMED
- **Evidence of the problem:** NAV has two authorities (`portfolio_valuation` vs
  `paper_trading_desk.book_nav`); eligible date has ≥8 resolvers; workflow state
  has 3 representations (CURRENT_ARCHITECTURE §10).
- **Consequence:** Slices 1–5 each end when the audit multi-writer set for the
  concept collapses to one.

### D-5 — Converge the two "daily operating" worlds — PROVISIONAL
- **Decision (proposed):** converge World A (`daily_operating_run`, Yahoo→Postgres)
  and World B (`daily_close`, owned-EODHD→JSON) onto one provider policy and one
  market-session service; deprecate the Yahoo ingest once parity is proven.
- **Evidence:** the two share no provider/store/market-date semantics
  (`daily_operating_run.py:4-14`); only `compute_market_date_alignment` reconciles.
- **Open question:** which store is authoritative for prices long-term
  (owned-EODHD is the operational truth; Postgres holds the legacy archive).
- **Consequence:** until resolved, both worlds coexist and can disagree about
  "today"; Slice 1 makes the date single-source first.

### D-6 — `db/session.py` is the model for a store service — CONFIRMED
- **Evidence:** zero ad-hoc DB sessions (audit `direct_db_sessions = 0`) vs ~13
  private ledger stores + ~11 hand-copied atomic writers.
- **Consequence:** the cross-cutting ledger/store service mirrors this boundary.

### D-7 — Keep Phase 28C separation of operational vs research/forward evidence — CONFIRMED
- **Evidence:** a valid operational close can be green while forward evidence is
  amber at a month boundary (`daily_close.py:720-735`); ACTIVE vs SHADOW never
  mixed (`forward_evidence.py:18-31`).
- **Consequence:** research failure never invalidates a valid close (Principle 3).

### D-8 — Point-in-time evidence is never fabricated — CONFIRMED
- **Evidence:** `ALREADY_PROCESSED` closes never backfill TRUE_FORWARD; recovery
  replays frozen artifacts verbatim or records a documented gap.
- **Consequence:** gaps remain visible; no backdating (Principle 4).

### D-9 — Legacy DB order-execution stack is quarantined, not extended — CONFIRMED
- **Decision:** `engine/reconciler`/`risk`/`portfolio`/`strategy` and the
  `/v1/review/create-orders` surface are legacy; no new execution is built on
  them; they move behind an explicit disabled boundary (Slice 11).
- **Evidence:** `engine/reconciler.run_fill_cycle` commits Order/Trade/Position —
  the only order/fill code, conflicting with the preview-only mandate.
- **Consequence:** execution stays deferred to Milestone 7 with explicit
  authorization.

### D-10 — No big-bang rewrite of the monoliths — CONFIRMED
- **Decision:** `api/app.py` (20.5k) and `api/ui/index.html` (26.7k) are reduced
  incrementally (domain routers, extracted view logic) as owning contexts
  stabilize — never rewritten wholesale.
- **Evidence:** 178 routes + the legacy pipeline are entangled in one file; 648
  implementation-coupled test assertions raise rewrite risk.
- **Consequence:** every slice preserves behavior and migrates contracts
  (Principle 8).

## Rejected alternatives

- **R-1 — Rewrite the backend from scratch.** Rejected: violates Principle 8;
  the entangled monolith + implementation-coupled tests make a big-bang rewrite
  high-risk with no incremental value.
- **R-2 — Bless the two NAV authorities as permanent.** Rejected: Principle 1;
  divergent NAV across UI surfaces is a correctness risk.
- **R-3 — Auto-promote challengers that pass gates.** Rejected: Principle 7;
  promotion requires manual approval and adequate forward evidence.
- **R-4 — Delete "unused" modules on static evidence alone.** Rejected: static
  analysis does not prove runtime behavior; REMOVE_CANDIDATE requires confirming
  no live consumer first.

## Unresolved

- **U-1 — Authoritative price store long-term** (owned-EODHD vs Postgres): see D-5.
- **U-2 — Fate of the frozen `current_alpha_*` (Phase 13/16/17/18) viewers:** most
  answer settled research questions; confirm no UI consumer before deprecating.
- **U-3 — Champion daily-refresh subprocess** (`current_alpha_daily_refresh`
  shelling into a separate research repo): whether to internalize or keep as an
  external boundary.
- **U-4 — `db/models` execution tables** (Signal/TradeDecision/Order/Trade):
  archive vs remove, pending Slice 11 quarantine outcome.
