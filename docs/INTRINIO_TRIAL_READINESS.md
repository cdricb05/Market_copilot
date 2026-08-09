# Data Expansion Readiness — Intrinio US Fundamentals trial (NOT activated)

Status: **NOT INTEGRATED, NOT ACTIVATED, NO CALL MADE.** This document only records the
exact evaluation contract to be used during a future, operator-approved 14-day Intrinio US
Fundamentals trial, and how an approved dataset would enter the Research Execution Bridge
through canonical dataset ownership. The Research Bridge is already able to accept an
approved Data Expansion dataset later; nothing here calls, subscribes to, or pays a provider.

## How an approved dataset enters the bridge (later, manual)

An external dataset is evaluated by the existing **Data Expansion / Purchase-Gate**
(`engine/data_expansion_gate.py` + `api/data_expansion.py`,
`GET /v1/research/data-expansion`). Only after that gate returns `PURCHASE_RECOMMENDED` /
`INTEGRATION_RECOMMENDED` AND an operator manually approves does the dataset become an owned
provider/provenance source (`alpha_agent.source_contracts`). At that point the bridge's
mandate may add the new **owned** data family to `allowed_data_families`; until then the
mandate permits `OWNED data only` (`paid_data_permitted = False`). The bridge itself never
purchases, activates a provider, or calls a paid API.

## Intrinio US Fundamentals — trial evaluation contract (14 days)

Evaluate across the Purchase-Gate dimensions; capture each as measured evidence, not opinion:

1. **PIT integrity** — every fundamental datum carries a real availability (filing/publish)
   date; no future-restatement leakage; restatements preserved as separate observations.
2. **Active / inactive / acquired / delisted coverage** — coverage over the FULL
   survivorship-safe universe (current + delisted), not current-only.
3. **Ticker / identifier continuity** — stable cross-vendor identifier; ticker changes and
   ticker reuse handled; mapping to the owned Norgate assetid / SEC CIK.
4. **Missingness** — per-concept, per-period missing rates; how gaps are represented.
5. **History depth** — earliest reliable PIT date per concept; enough periods for the
   `min_scored_periods = 12` gate on the survivorship-safe universe per rebalance.
6. **Incremental universe / sample value** — added eligible names/periods vs owned data.
7. **Incremental candidate features** — which new hypothesis families/features become
   computable that owned data cannot support today (e.g. clean shares-outstanding →
   `valuation` family, currently honestly `DATA_HOLD`).
8. **Incremental OOS alpha** — measured net-of-cost lift through the UNCHANGED
   `alpha_agent.tournament.classify_evidence` gates + BH-FDR (no gate lowered).
9. **Orthogonality** — incremental value vs the current champion (`|corr| ≤ 0.90`).
10. **Cost / turnover** — net-of-cost at the standard `cost_bps_round_trip = 50`.
11. **Licensing / cost purchase gate** — license terms, redistribution, per-seat/per-call
    cost → the Purchase-Gate `licensing` + `cost` dimensions.

A dataset is only worth purchasing if it clears hard blockers (defensible PIT history,
survivorship-safe coverage, sufficient usable history, clear licensing, no leakage, reliable
identifiers, adequate sample, redundancy-with-lift) AND shows measured incremental OOS alpha.
An honest `REJECT` / `INSUFFICIENT_EVIDENCE` / `RESEARCH_ONLY` outcome is acceptable.

## Zacks (current-data trial) — what it CAN and CANNOT validate

The Zacks trial provides CURRENT analyst-consensus data. During the trial it can validate:

- **schema** — field set and types of the consensus/estimate payloads;
- **identifiers** — ticker/identifier mapping and continuity;
- **coverage** — breadth of current-universe names covered;
- **cadence** — update frequency / freshness of consensus fields;
- **available consensus fields** — which estimate/revision fields exist.

It **CANNOT** validate **historical revision alpha**: without historical point-in-time
revision snapshots there is no way to measure whether analyst-revision signals produced
defensible out-of-sample alpha. That requires historical PIT revision data (the Stage 13A
`alpha_agent.analyst_revisions` framework already encodes this pre-data gate; a current-only
trial stays `DATA_HOLD` for the historical-alpha question).

## Explicit non-actions

No Intrinio/Zacks call made; no trial activated; no credential added; no provider
subscribed; no data purchased; no owned data family added to any mandate in this build.
