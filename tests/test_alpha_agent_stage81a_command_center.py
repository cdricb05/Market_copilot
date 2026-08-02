"""tests/test_alpha_agent_stage81a_command_center.py — Stage 8.1A.

Deterministic coverage of the Telegram READ-ONLY command center: the fixed
command menu, the canonical Paper Trader operational providers (performance /
nav / book / positions / attribution) and the Stage 8 research providers
(queue / jobs / job / experiments / candidates / sources / coverage / blocked /
report), plus the deterministic natural-language aliases.

Every provider is fed a FAKE loader or a tmp sqlite queue — no DB, no desk
ledger, no network, no Telegram, no scheduled task and NO operational trading
state are ever touched. Proves the 8.1A contract:

  * /commands advertises ONLY commands that have a real handler (and /help is an
    alias of /commands);
  * /pnl and /performance return the same canonical operational values;
  * /nav returns the current NAV + market date;
  * "What is my PnL?" routes to the same provider as /pnl;
  * /book / /positions / /attribution return real canonical data and
    /attribution reconciles contributors to the daily P&L;
  * /queue / /job / /experiments return real research-queue state;
  * /candidates never claims a live promotion;
  * /sources + /coverage use the Stage 8 registry snapshot;
  * missing data returns a SPECIFIC actionable diagnostic (never the vague
    "not available right now");
  * unauthorized users stay denied; duplicate updates stay idempotent; no
    command mutates trading state; no credential is ever echoed; dangerous
    shell/SQL/trading language routes to help.
"""
from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO.parent) not in sys.path:
    sys.path.insert(0, str(_REPO.parent))

from paper_trader.alpha_agent import autonomous_research as ar  # noqa: E402
from paper_trader.alpha_agent import telegram_control as tc  # noqa: E402


# --------------------------------------------------------------------------- #
# Deterministic helpers + fixtures.
# --------------------------------------------------------------------------- #
class Clock:
    def __init__(self, start_s: int = 0):
        self.t = start_s

    def __call__(self) -> str:
        base = datetime(2026, 7, 31, tzinfo=timezone.utc) + timedelta(seconds=self.t)
        return base.replace(microsecond=0).isoformat()


def _q(tmp_path, name="autonomy.sqlite"):
    return ar.ResearchQueue(tmp_path / name, clock=Clock())


# Canonical operational-book payload shape (mirrors the LIVE
# api.operational_book.load_operational_book output for Alpha Paper Book #1).
FAKE_OPS = {
    "operational_book": {
        "book_id": "alpha_paper_book_1",
        "book_label": "Alpha Paper Book #1",
        "initialized": True,
        "strategy_name": "fundamental_momentum_50_50_v1",
        "target_name": "fundamental_momentum_50_50_top25",
        "nav": 99322.05, "nav_as_of_date": "2026-07-30",
        "cash": 4630.31, "invested": 94691.74,
        "holdings_count": 25, "fills_count": 25,
        "desk_mark_date": "2026-07-30", "desk_mark_status": "DESK_MARK_BEHIND",
        "target_count": 25, "implementation_count": 25,
        "portfolio_summary": {"cash_weight": 0.046619, "invested_weight": 0.953381,
                              "daily_pnl": 1196.82, "daily_pnl_available": True},
        "holdings_detail": [
            {"ticker": "RCL", "quantity": 13, "latest_price": 321.94,
             "market_value": 4185.22, "current_weight": 0.042138,
             "unrealized_pnl": 464.52},
            {"ticker": "MNST", "quantity": 42, "latest_price": 97.65,
             "market_value": 4101.30, "current_weight": 0.041293,
             "unrealized_pnl": 78.14},
            {"ticker": "MO", "quantity": 54, "latest_price": 67.94,
             "market_value": 3668.76, "current_weight": 0.036940,
             "unrealized_pnl": -233.29},
        ],
    },
}

# Canonical daily-attribution payload (mirrors forward_evidence.build_daily_attribution).
FAKE_ATTR = {
    "available": True, "status": "ATTRIBUTION_READY",
    "market_date": "2026-07-30", "prior_market_date": "2026-07-29",
    "portfolio": {"daily_pnl": 1196.82, "daily_return_pct": 1.2197,
                  "cumulative_pnl": -677.95, "cumulative_return_pct": -0.6779,
                  "spy_daily_return_pct": 1.6766, "spy_cumulative_return_pct": -0.7653,
                  "daily_excess_return_pct": -0.4569,
                  "cumulative_excess_return_pct": 0.0874, "drawdown_pct": -0.6779},
    "winners": [{"ticker": "ALAB", "pnl_contribution": 599.4, "daily_return_pct": 20.0},
                {"ticker": "AMD", "pnl_contribution": 390.81, "daily_return_pct": 13.0},
                {"ticker": "ANET", "pnl_contribution": 287.1, "daily_return_pct": 8.26}],
    "losers": [{"ticker": "MO", "pnl_contribution": -376.92, "daily_return_pct": -9.32},
               {"ticker": "HST", "pnl_contribution": -73.35, "daily_return_pct": -1.76}],
    "reconciliation": {"position_contribution_sum": 1196.82, "market_movement": 1196.82,
                       "residual": 0.0, "tolerance": 1.0, "reconciles": True},
}

FAKE_ATTR_NONE = {"available": False, "status": "NO_PRIOR_OPERATIONAL_MARK",
                  "market_date": "2026-07-30",
                  "reason": "The baseline mark has no prior day."}

FAKE_OPS_UNINIT = {"operational_book": {"initialized": False}}


def _op_providers(ops=FAKE_OPS, attr=FAKE_ATTR):
    return tc.build_operational_providers(
        ops_loader=lambda: ops, attribution_loader=lambda: attr)


def _cc_providers(tmp_path, ops=FAKE_OPS, attr=FAKE_ATTR, cfg=None, queue=None):
    q = queue if queue is not None else _q(tmp_path)
    return q, tc.build_command_center_providers(
        stage8_config=cfg or {}, queue=q,
        ops_loader=lambda: ops, attribution_loader=lambda: attr)


def _update(text, uid=111, cid=111, update_id=1):
    return {"update_id": update_id,
            "message": {"from": {"id": uid},
                        "chat": {"id": cid, "type": "private"}, "text": text}}


def _reply(providers, queue, text):
    r = tc.ControlRouter(providers=providers, queue=queue, secrets=[])
    return "\n".join(r.handle(_update(text)))


# --------------------------------------------------------------------------- #
# WS2 — the official command menu.
# --------------------------------------------------------------------------- #
class TestCommandMenu:
    def test_commands_lists_only_implemented_commands(self, tmp_path):
        # Every "/token" advertised in the menu must resolve to a real handler:
        # a read-only provider, /run, or the help/commands menu itself.
        _q0, providers = _cc_providers(tmp_path)
        # Command tokens may contain hyphens (e.g. /historical-universe).
        advertised = set(re.findall(r"/[a-z][a-z-]*", tc.HELP_TEXT))
        handled = set(tc._COMMAND_PROVIDER) | {"/help", "/commands", "/run"}
        assert advertised <= handled, advertised - handled
        # …and every read-only command has a wired provider in the live build.
        for command, key in tc._COMMAND_PROVIDER.items():
            assert key in providers, "%s -> %s not wired" % (command, key)
        # The registry-derived COMMANDS tuple contains no duplicates and matches
        # the menu's advertised set exactly.
        assert len(tc.COMMANDS) == len(set(tc.COMMANDS))
        assert advertised == set(tc.COMMANDS)

    def test_help_is_alias_of_commands(self):
        i_help = tc.resolve_intent("/help")
        i_cmds = tc.resolve_intent("/commands")
        assert i_help["kind"] == i_cmds["kind"] == tc.KIND_HELP
        assert i_help["help_reason"] == i_cmds["help_reason"] == "menu"
        r = tc.ControlRouter(providers={}, queue=None)
        assert r.handle(_update("/help")) == r.handle(_update("/commands"))


# --------------------------------------------------------------------------- #
# WS3 — performance / pnl / nav.
# --------------------------------------------------------------------------- #
class TestPerformanceCommands:
    def test_pnl_returns_canonical_performance_data(self):
        out = _op_providers()["performance"]()
        assert "PAPER BOOK PERFORMANCE" in out
        assert "Alpha Paper Book #1" in out
        assert "NAV: $99,322.05" in out
        assert "Today: +$1,196.82 (+1.22%)" in out
        assert "Since baseline: -$677.95 (-0.68%)" in out
        assert "SPY today: +1.68%" in out
        assert "Daily excess: -0.46 pp" in out
        assert "Cumulative excess: +0.09 pp" in out
        assert "Drawdown: -0.68%" in out

    def test_performance_returns_the_same_canonical_values(self):
        # /pnl and /performance are aliases of the ONE performance provider.
        assert tc._COMMAND_PROVIDER["/pnl"] == tc._COMMAND_PROVIDER["/performance"]
        assert tc._COMMAND_PROVIDER["/today"] == tc._COMMAND_PROVIDER["/pnl"]
        p = _op_providers()["performance"]
        assert p() == p()  # deterministic, same canonical values

    def test_nav_returns_current_nav_and_market_date(self):
        out = _op_providers()["nav"]()
        assert "NAV: $99,322.05" in out
        assert "2026-07-30" in out

    def test_nl_pnl_routes_to_same_provider_as_pnl(self):
        # The substantive contract (WS10 step 5): "What is my PnL?" reaches the
        # SAME provider as /pnl, so the answer is identical.
        nl = tc.resolve_intent("What is my PnL?")
        assert nl["kind"] == tc.KIND_READ_ONLY
        assert tc._COMMAND_PROVIDER[nl["command"]] == tc._COMMAND_PROVIDER["/pnl"]

    def test_how_did_portfolio_do_today_routes_to_performance(self):
        nl = tc.resolve_intent("How did the portfolio do today?")
        assert tc._COMMAND_PROVIDER[nl["command"]] == tc._COMMAND_PROVIDER["/pnl"]


# --------------------------------------------------------------------------- #
# WS4 — book / positions.
# --------------------------------------------------------------------------- #
class TestBookAndPositions:
    def test_book_returns_canonical_active_book(self):
        out = _op_providers()["book"]()
        assert "Alpha Paper Book #1" in out
        assert "fundamental_momentum_50_50_v1" in out
        assert "Holdings: 25 (target 25)" in out
        assert "Cash: 4.7%" in out and "Invested: 95.3%" in out
        assert "Valuation date: 2026-07-30" in out
        assert "RCL" in out  # top position by market value

    def test_positions_returns_real_position_rows(self):
        out = _op_providers()["positions"]()
        assert "POSITIONS — Alpha Paper Book #1 (3)" in out
        assert "RCL" in out and "MV $4,185.22" in out and "4.2%" in out
        assert "P/L +$464.52" in out
        assert "P/L -$233.29" in out  # MO, a losing position


# --------------------------------------------------------------------------- #
# WS5 — attribution.
# --------------------------------------------------------------------------- #
class TestAttribution:
    def test_attribution_reconciles_contributors_to_daily_pnl(self):
        out = _op_providers()["attribution"]()
        assert "DAILY ATTRIBUTION — 2026-07-30 (prior 2026-07-29)" in out
        assert "Total daily P&L: +$1,196.82 (+1.22%)" in out
        # Top contributor shown by DOLLAR contribution (not merely its return).
        assert "ALAB  +$599.40" in out
        assert "MO    -$376.92" in out
        # Reconciles Σ contributions to the NAV move.
        assert "residual +$0.00" in out and "OK" in out

    def test_attribution_ranks_by_contribution_not_holding_size(self):
        # RCL is the LARGEST holding but not a top daily contributor — the
        # command must show ALAB's actual contribution, not RCL by size.
        out = _op_providers()["attribution"]()
        assert out.index("ALAB") < out.index("Reconciliation")
        assert "ALAB" in out  # a real contributor, not the biggest position


# --------------------------------------------------------------------------- #
# WS6 — research commands.
# --------------------------------------------------------------------------- #
class TestResearchCommands:
    def test_queue_returns_real_queue_state(self, tmp_path):
        q = _q(tmp_path)
        q.enqueue(ar.CAT_EXPERIMENT, lane="experiment.value", payload={})
        out = tc.build_default_providers(queue=q)["queue"]()
        assert "Research queue:" in out
        assert "1 queued" in out
        assert "Next queued:" in out and "experiment.value" in out

    def test_job_returns_the_requested_job(self, tmp_path):
        q = _q(tmp_path)
        jid = q.enqueue(ar.CAT_EXPERIMENT, lane="experiment.momentum",
                        payload={})
        out = tc.build_default_providers(queue=q)["job"](jid)
        assert jid in out
        assert "Category: EXPERIMENT" in out
        assert "Lane: experiment.momentum" in out
        assert "State: QUEUED" in out

    def test_job_unknown_id_is_actionable_not_error(self, tmp_path):
        q = _q(tmp_path)
        out = tc.build_default_providers(queue=q)["job"]("job_does_not_exist")
        assert "No job found" in out and "/jobs" in out

    def test_experiments_returns_real_evidence_results(self, tmp_path):
        q = _q(tmp_path)
        jid = q.enqueue(ar.CAT_EXPERIMENT, lane="experiment.price.residual",
                        payload={})
        q.complete(jid, result={"experiments_completed": 6, "keep_for_research": 1,
                                 "results": [{"label": "residual_momentum_21d",
                                              "feature": "residual_momentum",
                                              "rank_ic_t": 1.4858, "spread_t": 1.2905,
                                              "decision": "REJECT_WEAK_EVIDENCE"}]})
        out = tc.build_default_providers(queue=q)["experiments"]()
        assert "residual_momentum_21d" in out
        assert "rank-IC t=1.486" in out
        assert "REJECT_WEAK_EVIDENCE" in out

    def test_candidates_does_not_claim_model_promotion(self, tmp_path):
        q = _q(tmp_path)
        keep = q.enqueue(ar.CAT_EXPERIMENT, lane="experiment.keep", payload={})
        q.complete(keep, result={"results": [
            {"label": "quality_composite", "decision": "KEEP_FOR_RESEARCH"},
            {"label": "junk_factor", "decision": "REJECT_WEAK_EVIDENCE"}]})
        out = tc.build_default_providers(queue=q)["candidates"]()
        assert "KEEP_FOR_RESEARCH: quality_composite" in out
        assert "REJECTED:" in out
        assert "No model is promoted to live" in out
        # Never asserts a live promotion.
        assert "promoted to live trading — research only" in out.lower() or \
            "no model is promoted to live" in out.lower()

    def test_sources_and_coverage_use_stage8_state(self, tmp_path):
        snap = {"source_count": 31,
                "classification_tally": {"ACCESSIBLE_NOW": 27,
                                         "ACCESSIBLE_AFTER_REPAIR": 1,
                                         "PROSPECTIVE_ONLY": 3},
                "sources": [{"source_id": "eodhd_eod", "information_family": "eod",
                             "classification": "ACCESSIBLE_NOW"}]}
        snap_path = tmp_path / "registry.json"
        snap_path.write_text(json.dumps(snap), encoding="utf-8")
        cfg = {"sources": {"registry_snapshot_path": str(snap_path)}}
        providers = tc.build_default_providers(stage8_config=cfg, queue=_q(tmp_path))
        srcs = providers["sources"]()
        cov = providers["coverage"]()
        assert "27 accessible now" in srcs
        assert "PIT limitation" in cov
        assert "1 free sources awaiting" in cov

    def test_blocked_returns_specific_reasons(self, tmp_path):
        q = _q(tmp_path)
        jid = q.enqueue(ar.CAT_DATA_ACQUISITION, lane="acquire.fred", payload={})
        q.block_specific(jid, "provider blocked: ALPHA_AGENT_STAGE2_BLOCKED")
        out = tc.build_default_providers(queue=q)["blocked"]()
        assert "acquire.fred" in out
        assert "ALPHA_AGENT_STAGE2_BLOCKED" in out


# --------------------------------------------------------------------------- #
# WS7 — natural-language aliases + actionable diagnostics.
# --------------------------------------------------------------------------- #
class TestNaturalLanguageAndDiagnostics:
    @pytest.mark.parametrize("text,provider_key", [
        ("What is my PnL?", "performance"),
        ("How did the portfolio do today?", "performance"),
        ("What is my NAV?", "nav"),
        ("Show my positions.", "positions"),
        ("What helped the portfolio today?", "attribution"),
        ("What hurt the portfolio today?", "attribution"),
        ("What is the research agent working on?", "queue"),
        ("What are the best alpha candidates?", "candidates"),
        ("What data sources are available?", "sources"),
    ])
    def test_nl_aliases_route_to_approved_commands(self, text, provider_key):
        it = tc.resolve_intent(text)
        assert it["kind"] == tc.KIND_READ_ONLY
        assert tc._COMMAND_PROVIDER[it["command"]] == provider_key

    def test_unknown_question_explains_no_llm_and_suggests_commands(self):
        r = tc.ControlRouter(providers={}, queue=None)
        out = "\n".join(r.handle(_update("tell me a story about penguins")))
        assert "conversational" in out.lower() or "llm" in out.lower()
        assert "/commands" in out
        assert "not available right now" not in out.lower()

    def test_missing_performance_data_is_specific_diagnostic(self):
        out = _op_providers(ops=FAKE_OPS_UNINIT)["performance"]()
        assert "PERFORMANCE DATA UNAVAILABLE" in out
        assert "Expected source:" in out and "Reason:" in out
        assert "not available right now" not in out.lower()

    def test_missing_attribution_data_is_specific_diagnostic(self):
        out = _op_providers(attr=FAKE_ATTR_NONE)["attribution"]()
        assert "ATTRIBUTION DATA UNAVAILABLE" in out
        assert "prior" in out.lower()


# --------------------------------------------------------------------------- #
# WS8 — security, idempotency, non-mutation, redaction.
# --------------------------------------------------------------------------- #
class TestSecurityAndSafety:
    def _cfg(self, user=111, chat=111):
        return {"telegram": {"enabled": True, "credential_dir": None,
                             "allowed_user_ids": [user], "allowed_chat_ids": [chat]}}

    def test_unauthorized_users_remain_denied(self):
        cfg = tc.TelegramConfig(self._cfg())
        assert tc.authorize(_update("/pnl", uid=111, cid=111), cfg)[0] is True
        assert tc.authorize(_update("/pnl", uid=222, cid=111), cfg)[0] is False
        assert tc.authorize(_update("/pnl", uid=111, cid=222), cfg)[0] is False

    def test_duplicate_updates_remain_idempotent(self, tmp_path):
        store = tc.TelegramStore(tmp_path / "tg.sqlite", clock=Clock())
        assert store.mark_seen(500) is True
        assert store.mark_seen(500) is False
        assert store.seen(500) is True

    def test_no_command_mutates_operational_or_queue_state(self, tmp_path):
        q, providers = _cc_providers(tmp_path)
        q.enqueue(ar.CAT_EXPERIMENT, lane="experiment.x", payload={})
        before = q.counts_by_state()
        for text in ("/pnl", "/performance", "/nav", "/book", "/positions",
                     "/attribution", "/queue", "/jobs", "/experiments",
                     "/candidates", "/sources", "/coverage", "/blocked",
                     "/report", "/status", "/health", "/data", "/commands"):
            _reply(providers, q, text)
        assert q.counts_by_state() == before
        # The router exposes exactly two effect classes (read providers + enqueue).
        r = tc.ControlRouter(providers={}, queue=None)
        assert set(vars(r)) <= {"providers", "queue", "secrets"}

    def test_no_credential_ever_echoed(self, tmp_path):
        secret = "123456789:AAterribleTokenValueThatMustNeverLeak000"
        q = _q(tmp_path)
        r = tc.ControlRouter(providers={"performance": lambda: "NAV " + secret},
                             queue=q, secrets=[secret])
        chunks = r.handle(_update("/pnl"))
        assert all(secret not in c for c in chunks)

    def test_dangerous_shell_sql_trading_language_routes_to_help(self):
        for text in ("rm -rf C:\\ ; DROP TABLE holdings", "os.system('calc')",
                     "sell all AAPL now", "create order buy 100 TSLA",
                     "DELETE FROM fills", "exec(open('x').read())"):
            it = tc.resolve_intent(text)
            assert it["kind"] == tc.KIND_HELP
        # …and the reply is the safe menu, never an executed action.
        r = tc.ControlRouter(providers={}, queue=None)
        out = "\n".join(r.handle(_update("DROP TABLE holdings; rm -rf /")))
        assert "command center" in out.lower() or "/commands" in out.lower()

    def test_read_only_command_never_enqueues(self, tmp_path):
        q, providers = _cc_providers(tmp_path)
        before = q.counts_by_state()[ar.STATE_QUEUED]
        _reply(providers, q, "/pnl")
        _reply(providers, q, "/book")
        assert q.counts_by_state()[ar.STATE_QUEUED] == before
