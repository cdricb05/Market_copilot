# Target architecture — one owner, one orchestration path per concept (post-R99 review)

This is a review proposal. The canonical `docs/TARGET_ARCHITECTURE.md` remains authoritative; each roadmap slice that lands should update it. Nothing here is a rewrite. Every target owner below **already exists** and is already the dominant owner today. The target is reached by deleting or redirecting rivals, not by building new systems.

## Principles applied

1. **One calculation owner per concept.** Every other site *calls* it, or is a declared read-only projection that carries the owner's hash.
2. **One persistence owner per store.** Only that owner names the store's path; everyone else reads through the owner's reader.
3. **One orchestration path per cycle.**
   - The operational cycle: `portfolio_cycle`.
   - The frequent event cycle: the collection worker → ESR.
   - The research cycle: the R59 runtime and `scripts/alpha_agents_v2.py`.
4. **Reads never write.** A GET never opens a writable store handle.
5. **The UI projects; it never derives.** No safety, stage or business value is computed client-side, and missing data fails **closed** (hidden and disabled).
6. **Research is quarantined from operations.** Research never writes an operational ledger, and a research failure never invalidates a close.

## Target owner table

| Concept | Calculation owner (target) | Persistence owner (target) | Orchestration path (target) | Read model | Rivals to remove / redirect (register id) |
|---|---|---|---|---|---|
| Eligible market session | `engine/market_session.evaluate_session` + `engine/exchange_calendar` | none (pure) | called by `data_freshness`, which is the only session source for the operational path | `workflow-state.action_session_market_date` | D13 `daily_operating_run.latest_completed_market_date` and `alpha_target.latest_completed` redirect to the owner; D14 and D15 collapse to one `exchange_calendar.non_sessions_between` helper |
| Data freshness | `api/data_freshness.classify_source` / `load_data_freshness` | the DRC run manifest freezes the verdict used at decision time | DRC step 1; ESR | `/v1/operations/data-freshness` | D16 `portfolio_valuation._freshness` and `current_alpha_book._mark_freshness` delegate; D17 `_is_fundamental_stale` becomes a classifier input |
| Signal refresh | events: `information_collection` → `event_signal_refresh`; model inputs: `alpha_target.run_refresh` | owner stores, unchanged | events: collection worker (operator-enabled, attested); model inputs: `daily_close` only | `/information-collection`, `/event-signal-refresh` | D18 retire the legacy `current_alpha_daily_refresh` mark pipeline and the `/daily-run/execute` write path |
| Universe scoring | `api/universe_scoring.build_universe_scoring` (hashed input contract) over the `multi_horizon_engine` kernel | inside the HOC artifact, unchanged | DRC `SCORE_UNIVERSE`; ESR | `/v1/research/universe-scoring` | D19 operational callers obtain scores via `universe_scoring`, so they carry the input hash |
| Champion / challenger state | `multi_horizon_registry.model_registry` (champion id) + `forward_challenger_registry` | registry store | registration only through `prospective_adoption` | `/v1/research/research-agent` | D20 read the champion id from the registry |
| Model promotion | **none: manual, out of band** (`AUTOMATIC_PROMOTION_ALLOWED=False` stays) | — | a future controlled checkpoint only (P7) | — | none |
| Portfolio state / NAV | `api/paper_trading_desk.book_nav` | desk ledgers via a public `desk` ledger API with a cross-process lock | `daily_close` refresh → `alpha_book` → `operational_book` → `portfolio_state` | `/v1/operations/portfolio-state` | D01 `corporate_actions._holdings_cost_nav` calls `book_nav` (break the cycle with a small `desk_fold` module); D02 carries `book_nav`'s value and hash, never re-sums; D03 archive-only |
| Holding opportunity cost | `engine/holding_opportunity_cost.build_assessment` | `api/holding_opportunity_cost.persist_assessment` (+ cross-process lock on the index) | DRC; ESR (shared lock with DRC) | `/v1/operations/holding-opportunity-cost` | D21 `research_agent` reads through `load_*` owner readers |
| Portfolio reassessment | `engine/portfolio_reassessment.build_reassessment` | `api/portfolio_reassessment.persist_reassessment` (+ lock on `history.json`) | DRC after HOC; ESR on material events (docstring corrected) | `/v1/operations/portfolio-reassessment` | — |
| Target portfolio / proposal | `engine/reallocation_proposal` + `constrained_reallocation` + `proposal_decision_review` (CURRENT / MINIMUM_REPAIR / FULL_TARGET) | `api/reallocation_proposal.persist_proposal`; decisions in `api/portfolio_decision` | DRC → proposal → **manual** select → **manual** decide | `/reallocation-proposal`, `/proposal-decision-review` | D04 the bootstrap Top-25 target never renders as a live book's drift reference; D05 and D06 are labelled research or legacy in the inventory |
| Workflow state | `api/workflow_state.load_workflow_state` | none (read model) | `POST /v1/operations/portfolio-cycle/run` is the **single** operator action | `/v1/operations/workflow-state` | D07–D09 retire `command_center._derive_stage`, `daily_workflow_dashboard._derive_active_stage` and the `app` stage machines; the UI stops calling them |
| Forward evidence | one writer per store (unchanged) | owner stores; emissions written create-exclusive (`O_EXCL`) | daily close / DRC capture; r52 runtime emission | prediction-skill / accrual projection | D12 R46 tournament advance moves out of the DRC into the research runtime |
| Research memory | `alpha_agent/r59/memory` | `research_memory.sqlite`; writes only via `scripts/alpha_agents_v2.py` and the runtime | R59 runtime; agents_v2 | **read-only handle** on every API route | D28 fixed to `open_memory_readonly`; D29 audit which entrypoints write; D30 per-experiment admissions become a canonical memory event |
| Research measurement provenance | `alpha_agent/agents_v2/runner` + `agents_v2/provenance` (post-R99) | frozen-source content store + R59 memory | `run_agents_v2_campaign.py` | campaign results | preregistration tooling embeds `frozen_source`; campaign specs declare `admission_rulings` |
| Paper orders / fills / reconciliation | `paper_trading_desk` lifecycle and `settle_due_orders`; governed creation `rebalance_execution.confirm_rebalance_order_plan` | desk ledgers (locked) | separately authorised confirm only; **no UI button** until P7 | `/v1/operations/rebalance/...` | D31 the UI bootstrap button fails closed; staleness hashes become **required**; D24 and D25 research and operational code use public, owned APIs |

## Orchestration (target)

```
operator ── POST /v1/operations/portfolio-cycle/run  (ONE action, idempotent per session)
   └─ daily_close.run_daily_close         session → freshness → marks/NAV → TRUE_FORWARD capture → journal
   └─ daily_research_cycle.run_...        score → target → evidence → HOC → reassess → proposal   (stops at decision boundary)
        └─ manual: select target → record decision            (no orders)
        └─ separately authorised (P7 only): confirm order plan → desk settle → reconcile

collection worker (operator-enabled, attested) → information_collection → ESR → [material] HOC → reassess → proposal
research runtime (scheduled, leased)          → R59 memory / agents_v2 / r52 emissions   (never an operational ledger)
```

## Guard rails the audit must enforce (move from report-only to blocking, one at a time)

1. No writable `open_memory()` reachable from a GET route.
2. No UI control that creates orders or confirms plans unless it is hidden and disabled by default **and** on read failure.
3. No `D:\Stock_Prediction_app_data\<store>` path literal outside the store's owner (extends `check_direct_ledger_refs`).
4. No new `from api.paper_trading_desk import _…` or `desk._…` outside an allow-list that only shrinks.
5. Fix `_static_prefix` trailing-slash matching, and replace the `def`-name concept-writer regex with a declared writer list.
