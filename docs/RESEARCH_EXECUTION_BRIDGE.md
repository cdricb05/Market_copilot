# Controlled Autonomous Research Execution Bridge

Functional, canonical name of this capability. (Historical numbering "29J.2" is
deliberately NOT depended on; `docs/architecture/system_inventory.json` records this as the
planned next step after 29J.1.)

## Purpose

Close the one missing architectural connection between the Persistent Alpha Research Agent
(the research **governor**) and the existing AlphaAgent research lab (the quantitative
**research lab**), without collapsing their roles or duplicating any machinery:

```
PERSISTENT RESEARCH AGENT            engine/api research_agent — already emits ranked
        │                            bounded research opportunities
        ▼  Research Opportunity
CONTROLLED RESEARCH MANDATE          engine/api research_bridge (NEW) — bounds what may be
        │                            investigated (families / owned data / horizons /
        ▼  idempotent dispatch       budgets / requirements), never the answer
EXISTING ALPHAAGENT RESEARCH LAB     alpha_agent ResearchQueue + tournament + gates (REUSED)
        │
        ▼  immutable evidence
IMMUTABLE RESEARCH EVIDENCE          research_bridge envelope — references canonical evidence
        │                            (candidate/experiment ids, metrics), never duplicates
        ▼  Alpha Strength Gate       alpha_agent.tournament.classify_evidence + BH-FDR +
        │                            orthogonality (REUSED, thresholds never re-invented)
PERSISTENT RESEARCH AGENT            research_bridge.reassess — verdict; no promotion
   RE-ASSESSMENT
        │
        ▼
MANUAL MODEL REVIEW                  mandatory; the bridge changes no model / portfolio
```

Roles preserved: **Research Agent = governor**, **AlphaAgent = research lab**, **Portfolio
Manager = capital allocator**. The bridge can autonomously commission BOUNDED research; it
can NOT promote models or trade.

## Canonical owners (two, plus a driver)

| Component | Owner | Kind |
|---|---|---|
| Bridge calculation (mandate / horizons / envelope / alpha-strength decision / re-assessment) | `engine/research_bridge.py` | pure deterministic kernel (no I/O / RNG / clock) |
| Composition / idempotent dispatch / persistence / read | `api/research_bridge.py` | owner |
| Bounded campaign driver (Phase 9) | `scripts/run_research_bridge_campaign.py` | operator-run, owned-data, ledger-fingerprinted |
| Read endpoint | `GET /v1/research/research-bridge` | read-only (`NOT_RUN` before a mandate) |
| UI | `#research` → Research Bridge section (`rbridge-*`, `loadResearchBridge`) | reads backend verbatim, no math in JS |

## What is REUSED, never duplicated

- **Research Opportunity** concept — `engine.research_agent._research_opportunities`
  (normalized, not forked).
- **Queue / claim / lease / stale-recovery** — `alpha_agent.autonomous_research.ResearchQueue`
  (`enqueue` deduped by the deterministic dispatch identity; `requeue_stale`).
- **Tournament / candidate registry / gates / shadow books / leaderboard** —
  `alpha_agent.tournament` (`classify_evidence`, `CandidateRegistry`, lifecycle states, **no
  `PROMOTED` state**).
- **Alpha Strength Gate thresholds** — `configs/alpha_agent/stage9_tournament.json`
  (`evidence_completeness` + `gates` + `manual_review_gate`), read, never re-invented.
- **FDR / orthogonality / regimes / PIT** — `alpha_agent.selection_controls`
  (`benjamini_hochberg`), `alpha_agent.orthogonality`, `alpha_agent.regimes`,
  `alpha_agent.pit_*`, `alpha_agent.stage12_execution`.

No second research agent / queue / campaign / tournament / candidate registry / evidence
store / model-promotion path is created.

## Multi-horizon vocabulary (three distinct concepts, kept explicit)

- `{5, 20, 63}` sessions — AlphaAgent **cross-sectional forward-label** research horizons.
- `1` session — the **execution / label-maturity lag** (`stage12_execution.entry_index`); a
  research horizon ONLY for event / post-filing-reaction families.
- `1/5/20/63` — the **forward-prediction maturation** vocabulary
  (`api.forward_prediction_skill`).

Per-family eligibility is deterministic (`engine.research_bridge.coherent_horizons`):
quality / momentum / valuation / growth / cross-family → `{20, 63}`; reversal / liquidity /
residual → `{5, 20}`; event / post-filing-reaction → `{1, 5(, 20)}`. A medium-term
fundamental factor is never mechanically tested at 1 session.

## Terminal outcomes (existing constants only)

`READY_FOR_MANUAL_REVIEW`, `KEEP_FOR_RESEARCH`, `DATA_HOLD`, `REJECTED`,
`NO_DEFENSIBLE_ALPHA`. `NO_DEFENSIBLE_ALPHA` is a **successful scientific** outcome;
`DATA_HOLD` is a **successful safety** outcome. A challenger is **never forced**. When the
research-lab supplies its authoritative per-candidate lifecycle (the tournament
`CandidateRegistry` state), that lifecycle is authoritative for aggregation while
`classify_evidence` is re-run for reproduction/verification (`gate_reproduced` surfaces any
mismatch honestly).

## Idempotency & recovery

Deterministic identities (`sha256`, sorted-json; no `Date.now`/RNG): `mandate_id`,
`dispatch_identity`, `envelope_hash`. Repeat opportunity/mandate construction, dispatch,
result ingestion and re-assessment do not duplicate. Dispatch reuses the queue's live-dedupe
index; recovery reuses `ResearchQueue.requeue_stale`. Process restart resumes from the
durable mandate/envelope artifacts and the durable queue.

## Persistence

Immutable mandate + result-envelope artifacts under `PAPER_TRADER_RESEARCH_BRIDGE_DIR`
(default `D:\Stock_Prediction_app_data\research_bridge`) — a research root, atomic writes,
idempotent by hash, supersede-on-different-evidence. Dispatch enqueues into the existing
`alpha_agent` autonomy queue on `D:`. **Never** an operational ledger (`~/.paper_trader`),
PostgreSQL, champion pointer, target, order, fill, holding, cash or NAV.

## Safety boundaries (structural)

Research governance only. No order/fill; no holding/target/cash/NAV mutation; no model
promotion/recalibration/retraining; no champion pointer write; no broker; no cadence /
scheduled task; no prediction run; no paid provider / data purchase (owned data only). The
web surface is GET-only; commissioning is operator-run and bounded. Manual model review is
mandatory. The bridge-scoped dispatch origin/lane is NOT admitted by the scheduled collect
drain, so a dispatched job stays inert until an explicit operator-run bounded drain.

## First campaign (Phase 9) result

Eligible session **2026-08-07**, champion **composite_sn**, opportunity
**investigate_rank_ic_degradation** (SIGNAL; degradation `TURNOVER_INEFFICIENCY`). Mandate
families `[price_momentum, profitability_quality, cross_family]`, horizons `{20, 63}`. The
bridge dispatched idempotently into the real queue and ingested the real tournament evidence
(52 candidates: 30 REJECTED, 22 DATA_HOLD, 0 FDR survivors) through the canonical gate.
**Conclusion: `NO_DEFENSIBLE_ALPHA`.** Champion unchanged; operational ledgers unchanged; no
order/fill/target/promotion/cadence. See `CAMPAIGN_REPORT.md`.
