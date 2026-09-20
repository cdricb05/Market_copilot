# PAPER_TRADER_ALPHA_AGENTS_V2

Run: `PORT_AND_ACTIVATE_PAPER_TRADER_ALPHA_AGENTS_R56_V1` (2026-09-19).

The twelve quant research agents were born in a sibling repository
(`C:\Users\binis\Stock_Prediction_app_push`, commit `e641107`) under a "Phase 8-A"
contract. This release ports them into Paper Trader as a NEW, versioned,
Paper-Trader-native system. It is not a copy and it is not an override.

## 1. Governance

`research/agents/governance_contract.json` is the authority.

**Retired (14, each named beside its replacement):** S&P 500 only; long-only;
monthly only; five price families; no fundamentals; no regime work; no research
leverage; no meta-model fitting; "never touch Paper Trader"; "no commit, no push";
the Bash tool; preview-only publishing; the 20-experiment `EXPnn` cycle; Norgate
as the only provider.

**Preserved (20):** no broker execution, no orders, no fills, no automatic
promotion, no automatic portfolio approval, no hindsight reconstruction, no
fabricated TRUE_FORWARD evidence, no backfill, hypothesis before evaluation,
thresholds frozen before final evaluation, failure recording, experiment ids,
skeptic rejects by default, realistic cost on traded notional, PIT integrity, no
silent sign flipping, no rounding into a pass, no hidden deletion, manual review,
no hallucination.

An agent may never widen the contract itself. A scope change is a new version.

## 2. What exists

| Artifact | Path |
| --- | --- |
| 12 agent definitions (PowerShell only, no Bash) | `.claude/agents/*.md` |
| 7 contracts | `research/agents/*.json` |
| Identity, roster, owner names | `alpha_agent/agents_v2/__init__.py` |
| Specification reader + validator | `alpha_agent/agents_v2/contracts.py` |
| Governed handoff pipeline | `alpha_agent/agents_v2/pipeline.py` |
| Live book owner (delegates to frozen evidence) | `alpha_agent/agents_v2/books.py` |
| Local campaign execution path | `alpha_agent/agents_v2/runner.py` |
| PowerShell entrypoints | `scripts/alpha_agents_v2.py`, `scripts/run_agents_v2_campaign.py` |
| Acceptance tests | `tests/test_paper_trader_alpha_agents_v2.py`, `tests/test_release57_research_engine_repair.py` |
| Preparatory census | `research/agents/NEXT_CAMPAIGN_CENSUS.json` |
| Settled campaigns | `research/agents/campaign_r56_v2/` (frozen evidence), `research/agents/campaign_r57_wave2/` |

## 3. Who owns what

The Claude agents ORCHESTRATE. They own no durable state.

| Capability | Canonical owner |
| --- | --- |
| Experiment registry, graveyard, burden | `alpha_agent.r59.memory.ResearchMemory` |
| Statistical qualification gate | `alpha_agent.r59.engines.gate` |
| Search denominator | `alpha_agent.r59.handlers.search_denominator` |
| Statistical kernel | `alpha_agent.r57.engine` |
| Prospective freeze | `alpha_agent.r59.handlers.freeze_qualified` |
| Forward adoption / registrar / evidence | `api.prospective_adoption` / `api.forward_challenger_registry` / `api.canonical_forward_accrual` |
| Maturation | `alpha_agent.r52.runtime` |
| Capital eligibility | `api.capital_eligibility_gate` |

An agent experiment IS a `hypotheses` row (`release=AGENTS_V2`) and its history IS
`AGENTS_V2_*` rows in the memory's `events` journal. There is no second registry,
gate, queue, forward clock, P&L owner or tournament.

## 4. Orchestration

```
quant-research-director        preregister (mints the experiment id; freezes spec,
        |                      sign, cost model and the gate-schema hash)
data-foundation-agent          certify_data
universe-construction-agent    define_universe      (refused without certification)
feature-library-agent          publish_features     (refused without a universe)
        |
momentum | reversal | trend-breadth | volatility-liquidity      reveal_stage
        |                                                       D, then V, then L
        |                                                       submit_candidate
        |                                                       (parallel batch)
validation-skeptic-agent       skeptic_review   KILLED by default; the ONLY door
risk-portfolio-agent           risk_review      skeptic survivors only
meta-model-ensemble-agent      meta_review      validated survivors only
quant-research-director        director_clear   the only writer of QUALIFIED
signal-publishing-agent        publish_candidate, request_forward_registration
```

A Claude Code subagent cannot launch a subagent. The director therefore emits an
assignment manifest and the session orchestrator dispatches the four signal
agents in one parallel batch.

### Agents DECIDE, the local engine EXECUTES (R57)

The twelve agents own the agenda, the hypotheses, the parameters, the gates,
the interpretation of a nontrivial result and every publish decision. They do
NOT narrate deterministic work. Universes, features, books, D/V/L slicing,
cost arithmetic, cost-neutral placebos, doubled cost, subperiods and gate
evaluation run locally through `alpha_agent/agents_v2/runner.py`, which returns
ONE compact machine-readable summary per experiment.

A campaign is therefore three artifacts and one command:

```
research/agents/campaign_<id>/campaign_agenda.json   the director's freeze, as DATA
research/agents/campaign_<id>/executors.py           the hypotheses, as code
research/agents/campaign_<id>/campaign_spec.json     experiment id -> executor
```

```powershell
& $py scripts\preregister_wave2.py                  # foundation chain + freeze
& $py scripts\run_agents_v2_campaign.py --spec <campaign_spec.json> `
      --out results.json --artifacts artifacts\
```

### Sequential D/V/L reveal (R57)

`reveal_stage` measures ONE evaluation layer. Validation is measured only if
discovery earned it; the lockbox only if validation earned it. The advance test
is deliberately weak - 25% of that metric's own materiality floor - because it
decides only whether the estate has earned the right to LOOK at the next layer;
the verdict remains `alpha_agent.r59.engines.gate` on the lockbox alone.

A layer that does not advance HALTS the experiment: it is settled
NO_ALPHA_EVIDENCE, it still counts to the search burden, and **the lockbox is
never computed at all**, so it cannot be inspected, quoted or reused. The
ledger reports such a cell as `HALTED_AT_D` / `HALTED_AT_V` with
`LOCKBOX_COMPUTED: false` - never as `PREREGISTERED`, which would tell a reader
it was still pending.

In Wave 2 this was not a formality: 9 of 10 cells halted before the lockbox
(6 at discovery, 3 at validation) and only one lockbox was ever computed.

## 5. The publishing boundary

May: publish a research candidate artifact; raise a governed prospective
registration request; thereby start research shadow P&L (measured by
`api.canonical_forward_accrual`); attach a qualified candidate to canonical
forward accrual through its owners.

May not: create orders; create fills; approve a portfolio proposal; promote a
champion; make a sleeve capital eligible. None of these has a verb in the
pipeline, so none can be reached by a wrong argument.

The request has NO date argument; `effective_from`, `inception`, `backfill` and the
like are refused by name. The agents' CLI NEVER adopts: it stops at
`FROZEN_AWAITING_ADOPTION_OWNER` and names the challenger id. Paper Trader pins
exactly ONE operator door for starting a forward clock,
`scripts/adopt_prospective_freeze.py --challenger-id <id>`, run by a human. A first
draft of this release gave the agents' CLI an `--execute --confirm` mode; the
existing guard `test_15c_there_is_exactly_one_operator_adoption_entrypoint` caught
it as a second door and it was removed rather than allow-listed.

Known limit: a registration whose frozen-decision producer does not exist accrues
nothing (`api.canonical_forward_accrual` reports it DATA_BLOCKED). The request
carries `FORWARD_PRODUCER_REQUIRED` until a producer is named.

## 6. Using it (Windows PowerShell)

```powershell
$py  = 'C:\Users\binis\paper_trader\.venv-win\Scripts\python.exe'
$cli = 'C:\Users\binis\paper_trader\scripts\alpha_agents_v2.py'
& $py $cli validate-contracts
& $py $cli status
& $py $cli census --out D:\Temp\census.json   # NEVER over NEXT_CAMPAIGN_CENSUS.json
& $py $cli preregister --agent quant-research-director --input prereg.json
& $py $cli ledger
```

The definitions register as agent types when a NEW Claude Code session starts in
this repository.

## 7. The preparatory census

Read-only; no experiment run, no evaluation sample read, zero budget spent.

The estate is deep but NARROW: 8,445 settled hypotheses, 0 qualified, 97.4%
price-state information, 96% at exactly H21, 90% machine-generated rank books.
Every one of 1,858 equity tests is on the S&P 500 large-cap panel. Only 12
hypotheses have used the R38 dated-contract futures layer. H1-H5 is 38 tests.

Queued for the first agent-native campaign (13 proposals, 10 recommended): commodity
basis-momentum on the dated-contract layer; a mid/small-cap universe extension
(liquidity premium, residual momentum, H5 liquidity-provision reversal, gross
profitability); terms-of-trade cross-asset relative value; exchange open-interest
growth; SEC comment-letter drift; dividend-change announcement drift; a single
pre-declared cross-asset lead-lag channel. The honest prior is low; a campaign that
ends NO_ALPHA_EVIDENCE is a valid result and cash remains a real asset choice.
