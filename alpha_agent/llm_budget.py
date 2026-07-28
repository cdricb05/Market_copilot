"""
alpha_agent/llm_budget.py — Stage 3 token / call-count / cost control.

Deterministic budget ledger enforcing HARD limits before any provider call:

* per-cycle call count, input tokens, output tokens;
* per-day input/output tokens (accumulated across runs via the state DB);
* daily + monthly estimated-cost warning and HARD-STOP thresholds.

Pricing is EXPLICIT configuration (model id, effective date, per-Mtok rates,
source note). Missing pricing or missing provider usage NEVER guesses a cost:
the cost is recorded as UNAVAILABLE and production cost control is reported as
not ready. When a hard limit trips, no further LLM calls are permitted; the
deterministic pipeline continues and the cycle ends BUDGET_EXHAUSTED.
"""
from __future__ import annotations

import math
from typing import Any, Optional

from .llm_contracts import COST_UNAVAILABLE

BUDGET_OK = "OK"
BUDGET_WARNING = "WARNING"
BUDGET_HARD_STOP = "HARD_STOP"

_REQUIRED_BUDGET_KEYS = (
    "max_calls_per_cycle", "max_input_tokens_per_cycle",
    "max_output_tokens_per_cycle", "max_input_tokens_per_day",
    "max_output_tokens_per_day", "daily_cost_warning_usd",
    "daily_cost_hard_stop_usd", "monthly_cost_warning_usd",
    "monthly_cost_hard_stop_usd", "max_event_records_per_cycle",
    "max_records_per_ticker", "max_prompt_characters",
    "max_response_characters")

_REQUIRED_PRICING_KEYS = ("model", "effective_date", "input_usd_per_mtok",
                          "output_usd_per_mtok", "source_note")


def validate_budget_config(budgets: dict) -> list[str]:
    """Return the list of missing required budget keys (empty = valid)."""
    return [k for k in _REQUIRED_BUDGET_KEYS if k not in budgets]


def estimate_tokens(text: str) -> int:
    """Deterministic conservative estimate: ~4 characters per token."""
    return int(math.ceil(len(text or "") / 4.0))


class PricingTable:
    """Explicit model pricing from configuration. Never assumes a price."""

    def __init__(self, rows: list[dict]):
        self.rows: dict[str, dict] = {}
        self.invalid: list[str] = []
        for row in rows or []:
            missing = [k for k in _REQUIRED_PRICING_KEYS if k not in row]
            if missing:
                self.invalid.append("%s missing %s"
                                    % (row.get("model", "?"), ",".join(missing)))
                continue
            self.rows[str(row["model"])] = row

    def has_model(self, model: Optional[str]) -> bool:
        return bool(model) and model in self.rows

    def cost_usd(self, model: Optional[str], usage: Optional[dict]) -> Any:
        """Exact estimated USD cost, or COST_UNAVAILABLE when pricing or usage
        is missing. Cache tokens priced only when cache rates are configured."""
        if not usage or not self.has_model(model):
            return COST_UNAVAILABLE
        row = self.rows[str(model)]
        try:
            cost = (float(usage.get("input_tokens") or 0) / 1e6
                    * float(row["input_usd_per_mtok"])
                    + float(usage.get("output_tokens") or 0) / 1e6
                    * float(row["output_usd_per_mtok"]))
            cache_w = usage.get("cache_creation_input_tokens")
            if cache_w and "cache_write_usd_per_mtok" in row:
                cost += float(cache_w) / 1e6 * float(row["cache_write_usd_per_mtok"])
            cache_r = usage.get("cache_read_input_tokens")
            if cache_r and "cache_read_usd_per_mtok" in row:
                cost += float(cache_r) / 1e6 * float(row["cache_read_usd_per_mtok"])
        except (TypeError, ValueError):
            return COST_UNAVAILABLE
        return round(cost, 6)


class BudgetLedger:
    """Hard budget enforcement for one director cycle.

    `prior_day` / `prior_month` carry the accumulated totals from the state DB
    for the cycle's calendar day and month: {"input_tokens", "output_tokens",
    "cost_usd" (float, only reliable costs)}."""

    def __init__(self, budgets: dict, pricing: PricingTable, *,
                 prior_day: Optional[dict] = None,
                 prior_month: Optional[dict] = None):
        missing = validate_budget_config(budgets)
        if missing:
            raise ValueError("budget config missing keys: %s" % ",".join(missing))
        self.b = budgets
        self.pricing = pricing
        self.calls = 0
        self.cycle_input = 0
        self.cycle_output = 0
        self.cycle_cost = 0.0
        self.cost_unavailable_calls = 0
        self.day_input = int((prior_day or {}).get("input_tokens") or 0)
        self.day_output = int((prior_day or {}).get("output_tokens") or 0)
        self.day_cost = float((prior_day or {}).get("cost_usd") or 0.0)
        self.month_cost = float((prior_month or {}).get("cost_usd") or 0.0)
        self.events: list[dict] = []
        self.hard_stopped = False
        self.hard_stop_reason: Optional[str] = None

    # ------------------------------------------------------------------ #
    def can_call(self, estimated_input_tokens: int,
                 max_output_tokens: int) -> dict:
        """MUST be consulted before every provider call. A HARD_STOP verdict
        means the call is forbidden."""
        if self.hard_stopped:
            return {"allowed": False, "state": BUDGET_HARD_STOP,
                    "reason": self.hard_stop_reason or "budget already exhausted"}
        checks = (
            (self.calls + 1 > int(self.b["max_calls_per_cycle"]),
             "max_calls_per_cycle %s reached" % self.b["max_calls_per_cycle"]),
            (self.cycle_input + estimated_input_tokens
             > int(self.b["max_input_tokens_per_cycle"]),
             "max_input_tokens_per_cycle %s would be exceeded"
             % self.b["max_input_tokens_per_cycle"]),
            (self.cycle_output + max_output_tokens
             > int(self.b["max_output_tokens_per_cycle"]),
             "max_output_tokens_per_cycle %s would be exceeded"
             % self.b["max_output_tokens_per_cycle"]),
            (self.day_input + estimated_input_tokens
             > int(self.b["max_input_tokens_per_day"]),
             "max_input_tokens_per_day %s would be exceeded"
             % self.b["max_input_tokens_per_day"]),
            (self.day_output + max_output_tokens
             > int(self.b["max_output_tokens_per_day"]),
             "max_output_tokens_per_day %s would be exceeded"
             % self.b["max_output_tokens_per_day"]),
            (self.day_cost >= float(self.b["daily_cost_hard_stop_usd"]),
             "daily_cost_hard_stop_usd %s reached"
             % self.b["daily_cost_hard_stop_usd"]),
            (self.month_cost >= float(self.b["monthly_cost_hard_stop_usd"]),
             "monthly_cost_hard_stop_usd %s reached"
             % self.b["monthly_cost_hard_stop_usd"]),
        )
        for tripped, reason in checks:
            if tripped:
                self.hard_stopped = True
                self.hard_stop_reason = reason
                self.events.append({"state": BUDGET_HARD_STOP, "reason": reason})
                return {"allowed": False, "state": BUDGET_HARD_STOP,
                        "reason": reason}
        state = BUDGET_OK
        if self.day_cost >= float(self.b["daily_cost_warning_usd"]):
            state = BUDGET_WARNING
            self.events.append({"state": BUDGET_WARNING,
                                "reason": "daily cost warning threshold reached"})
        elif self.month_cost >= float(self.b["monthly_cost_warning_usd"]):
            state = BUDGET_WARNING
            self.events.append({"state": BUDGET_WARNING,
                                "reason": "monthly cost warning threshold reached"})
        return {"allowed": True, "state": state, "reason": None}

    # ------------------------------------------------------------------ #
    def record_usage(self, *, provider: str, model: Optional[str],
                     usage: Optional[dict]) -> dict:
        """Record ACTUAL usage after a provider call. Returns the usage row
        (tokens + cost or UNAVAILABLE) for persistence."""
        self.calls += 1
        u = usage or {}
        inp = int(u.get("input_tokens") or 0)
        out = int(u.get("output_tokens") or 0)
        self.cycle_input += inp
        self.cycle_output += out
        self.day_input += inp
        self.day_output += out
        cost = self.pricing.cost_usd(model, usage) if usage else COST_UNAVAILABLE
        if cost == COST_UNAVAILABLE:
            self.cost_unavailable_calls += 1
        else:
            self.cycle_cost += float(cost)
            self.day_cost += float(cost)
            self.month_cost += float(cost)
        return {"provider": provider, "model": model,
                "input_tokens": inp, "output_tokens": out,
                "cache_creation_tokens": int(u.get("cache_creation_input_tokens") or 0),
                "cache_read_tokens": int(u.get("cache_read_input_tokens") or 0),
                "estimated_cost_usd": cost,
                "cost_available": cost != COST_UNAVAILABLE,
                "usage_reported": bool(usage)}

    # ------------------------------------------------------------------ #
    def snapshot(self) -> dict:
        """Full accounting snapshot for token_cost_report.json."""
        cost_reliable = self.cost_unavailable_calls == 0
        return {
            "calls": self.calls,
            "cycle_input_tokens": self.cycle_input,
            "cycle_output_tokens": self.cycle_output,
            "cycle_estimated_cost_usd":
                round(self.cycle_cost, 6) if cost_reliable else COST_UNAVAILABLE,
            "day_input_tokens": self.day_input,
            "day_output_tokens": self.day_output,
            "day_estimated_cost_usd":
                round(self.day_cost, 6) if cost_reliable else COST_UNAVAILABLE,
            "month_estimated_cost_usd":
                round(self.month_cost, 6) if cost_reliable else COST_UNAVAILABLE,
            "cost_unavailable_calls": self.cost_unavailable_calls,
            "budget_limits": dict(self.b),
            "budget_remaining": {
                "calls": max(0, int(self.b["max_calls_per_cycle"]) - self.calls),
                "cycle_input_tokens":
                    max(0, int(self.b["max_input_tokens_per_cycle"]) - self.cycle_input),
                "cycle_output_tokens":
                    max(0, int(self.b["max_output_tokens_per_cycle"]) - self.cycle_output),
                "day_input_tokens":
                    max(0, int(self.b["max_input_tokens_per_day"]) - self.day_input),
                "day_output_tokens":
                    max(0, int(self.b["max_output_tokens_per_day"]) - self.day_output),
                "daily_cost_usd":
                    (max(0.0, round(float(self.b["daily_cost_hard_stop_usd"])
                                    - self.day_cost, 6))
                     if cost_reliable else COST_UNAVAILABLE),
                "monthly_cost_usd":
                    (max(0.0, round(float(self.b["monthly_cost_hard_stop_usd"])
                                    - self.month_cost, 6))
                     if cost_reliable else COST_UNAVAILABLE),
            },
            "hard_stopped": self.hard_stopped,
            "hard_stop_reason": self.hard_stop_reason,
            "budget_events": self.events,
            "production_cost_control_ready": cost_reliable and self.calls > 0,
        }


__all__ = ["BUDGET_OK", "BUDGET_WARNING", "BUDGET_HARD_STOP",
           "validate_budget_config", "estimate_tokens",
           "PricingTable", "BudgetLedger"]
