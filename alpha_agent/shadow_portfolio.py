"""
alpha_agent.shadow_portfolio - Stage 11 Workstream F: the dedicated SHADOW-ONLY
target-portfolio subsystem.

Technically and visibly SEPARATE from the operational paper-trading portfolio: it
lives under its own Stage 11 research root, never imports or writes an operational
order / fill / position / cash / holding / signal / trade-decision record, and
never creates a broker order or enables execution or automation. It only computes
HYPOTHETICAL target holdings and prospective returns for a strategy that has
already passed every UNCHANGED evidence gate.

Contract:
  * a shadow strategy is ACTIVATED only for a single signal or ensemble that
    ``stage11_research`` marked ``qualifies`` (KEEP_FOR_RESEARCH under the
    unchanged ``classify_evidence`` gates, FDR-survived, parameter-stable, holdout-
    confirmed) AND whose combined score clears the tournament's shadow floor;
  * when nothing qualifies the store records an explicit, visible
    ``NO DEFENSIBLE ALPHA - SHADOW PORTFOLIO NOT ACTIVATED`` status (a shadow book
    is NEVER manufactured);
  * every snapshot is immutable and stamped with the safety labels
    SHADOW ONLY / NO ORDERS / NO BROKER / NO EXECUTION / RESEARCH SIGNALS ONLY.

Pure stdlib; deterministic; append-only research writes only.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Optional, Sequence

from . import fundamental_evidence as _fev
from . import signal_library as _sl

SAFETY_LABELS = ["SHADOW ONLY", "NO ORDERS", "NO BROKER", "NO EXECUTION",
                 "RESEARCH SIGNALS ONLY"]

STATUS_ACTIVE = "SHADOW_ACTIVE"
STATUS_NO_ALPHA = "NO_DEFENSIBLE_ALPHA"
NO_ALPHA_MESSAGE = "NO DEFENSIBLE ALPHA - SHADOW PORTFOLIO NOT ACTIVATED"

DEFAULT_NOTIONAL = 100000.0
DEFAULT_BENCHMARK = "SPY"


# --------------------------------------------------------------------------- #
# Target-book construction (long-only decile book, equal weight).
# --------------------------------------------------------------------------- #
def _rank01(vals: dict) -> dict:
    if not vals:
        return {}
    order = sorted(vals, key=lambda k: vals[k])
    n = len(order)
    return {k: (i / (n - 1) if n > 1 else 0.5) for i, k in enumerate(order)}


def _composite_scores(components: Sequence[tuple], ctx: dict, as_of: str) -> dict:
    """Weighted rank composite score per assetid for the latest formation date
    (names present in EVERY component). ``components`` = ``[(spec, weight)]``."""
    rank_maps = []
    for spec, w in components:
        raw = _sl.signal_cross_section(
            spec, ohlcv=ctx["ohlcv"], market=ctx.get("market"),
            store=ctx.get("store"), cik_to_assetid=ctx.get("cik_to_assetid"),
            sector_series=ctx.get("sector_series"), as_of=as_of)
        raw = _sl.winsorize(raw, spec.winsor)
        adj = {k: v * (spec.direction or 1) for k, v in raw.items()}
        rank_maps.append((_rank01(adj), float(w)))
    if not rank_maps:
        return {}
    common = set(rank_maps[0][0])
    for rm, _w in rank_maps[1:]:
        common &= set(rm)
    return {k: sum(w * rm[k] for rm, w in rank_maps) for k in common}


def build_target_book(*, strategy_name: str, components: Sequence[tuple],
                      ctx: dict, as_of: str, evidence_row: dict,
                      gate_verdict: dict, combined_score: Optional[float],
                      ticker_map: Optional[dict] = None,
                      sector_series=None, benchmark: str = DEFAULT_BENCHMARK,
                      notional: float = DEFAULT_NOTIONAL,
                      cost_bps: int = 50, holding_period_days: int = 63,
                      rebalance: str = "quarterly",
                      provenance: str = "") -> dict:
    """A full HYPOTHETICAL long-only decile target book on ``as_of`` with weights,
    ranks, factor scores, sector + concentration exposure, expected turnover, cost
    and the evidence snapshot. Never an order; never mutates operational state."""
    scores = _composite_scores(components, ctx, as_of)
    ticker_map = ticker_map or {}
    a2c = {a: c for c, a in (ctx.get("cik_to_assetid") or {}).items()}
    holdings = []
    if scores:
        ordered = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
        k = max(1, int(round(len(ordered) * _fev._DECILE_QUANTILE)))
        longs = ordered[:k]
        w = 1.0 / len(longs)
        sector_exposure: dict = {}
        for rank, (aid, sc) in enumerate(longs, start=1):
            cik = a2c.get(aid)
            sec = (sector_series.sector_as_of(cik, as_of)
                   if (sector_series is not None and cik) else "Unknown")
            sector_exposure[sec] = sector_exposure.get(sec, 0.0) + w
            holdings.append({
                "assetid": aid, "ticker": ticker_map.get(aid),
                "rank": rank, "target_weight": round(w, 6),
                "factor_score": round(float(sc), 6), "sector": sec,
                "notional": round(notional * w, 2)})
    else:
        sector_exposure = {}
    max_name = max((h["target_weight"] for h in holdings), default=None)
    max_sector = max(sector_exposure.values(), default=None)
    eff_n = (1.0 / sum(h["target_weight"] ** 2 for h in holdings)) \
        if holdings else None
    book = {
        "strategy_name": strategy_name,
        "as_of": as_of,
        "status": STATUS_ACTIVE,
        "holdings": holdings,
        "holdings_count": len(holdings),
        "long_only": True,
        "target_weights_sum": (round((1.0 / len(holdings)) * len(holdings), 6)
                               if holdings else 0.0),
        "cash_target": 0.0,
        "notional": notional,
        "benchmark": benchmark,
        "sector_exposure": {s: round(v, 6)
                            for s, v in sorted(sector_exposure.items())},
        "concentration": {"max_name_weight": max_name,
                          "max_sector_weight": max_sector,
                          "effective_n": (round(eff_n, 2) if eff_n else None)},
        "expected_turnover_per_rebalance": evidence_row.get("turnover"),
        "estimated_cost_bps_round_trip": int(cost_bps),
        "expected_holding_period_days": int(holding_period_days),
        "rebalance_schedule": rebalance,
        "evidence_snapshot": {
            "periods": evidence_row.get("periods"),
            "rank_ic_mean": evidence_row.get("rank_ic_mean"),
            "rank_ic_t": evidence_row.get("rank_ic_t"),
            "spread_t": evidence_row.get("spread_t"),
            "net_annualized_return": evidence_row.get("net_annualized_return"),
            "max_drawdown": evidence_row.get("max_drawdown"),
            "subperiod_consistency": evidence_row.get("subperiod_consistency"),
            "regime_consistency": evidence_row.get("regime_consistency"),
            "gate_target_state": (gate_verdict or {}).get("target_state"),
            "gate_complete": (gate_verdict or {}).get("complete")},
        "combined_score": combined_score,
        "decision_provenance": provenance,
        "components": [{"spec_id": s.spec_id, "name": s.name,
                        "weight": round(float(wt), 6)} for s, wt in components],
        "safety_labels": list(SAFETY_LABELS),
        "read_only": True,
        "operating_portfolio": False,
        "creates_orders": False,
    }
    book["strategy_version"] = _version_hash(book)
    return book


def _version_hash(book: dict) -> str:
    ident = {"strategy_name": book["strategy_name"], "as_of": book["as_of"],
             "components": book["components"],
             "holdings": [(h["assetid"], h["target_weight"])
                          for h in book["holdings"]]}
    payload = json.dumps(ident, sort_keys=True, separators=(",", ":"))
    return "sv_" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


# --------------------------------------------------------------------------- #
# The dedicated shadow store (separate root; append-only).
# --------------------------------------------------------------------------- #
class ShadowPortfolioStore:
    """Append-only shadow store rooted at its OWN Stage 11 directory - never the
    operational paper-trading tree. ``shadow_state.json`` holds the current
    decision; activated strategies are immutable per-(name, as_of) snapshots."""

    def __init__(self, root):
        self.root = Path(root)

    def _state_path(self) -> Path:
        return self.root / "shadow_state.json"

    def _write(self, path: Path, doc: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(doc, sort_keys=True, indent=2),
                       encoding="utf-8")
        tmp.replace(path)

    def state(self) -> dict:
        p = self._state_path()
        if p.exists():
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                return {}
        return {"status": "UNINITIALIZED", "safety_labels": list(SAFETY_LABELS)}

    def record_no_alpha(self, *, as_of: str, evaluated: int, reason: str = "",
                        detail: Optional[dict] = None) -> dict:
        doc = {"status": STATUS_NO_ALPHA, "message": NO_ALPHA_MESSAGE,
               "as_of": as_of, "candidates_evaluated": int(evaluated),
               "reason": reason or ("no single signal or ensemble passed every "
                                    "unchanged evidence gate"),
               "active_strategy": None, "safety_labels": list(SAFETY_LABELS),
               "detail": detail or {}}
        self._write(self._state_path(), doc)
        return doc

    def activate(self, book: dict) -> dict:
        """Persist an IMMUTABLE strategy snapshot and point the state at it.
        First-write-wins per (strategy_name, as_of): re-activating the same
        strategy/date is idempotent and never rewrites the snapshot."""
        sid = "%s_%s" % (_slug(book["strategy_name"]), book["as_of"])
        snap = self.root / "strategies" / ("%s.json" % sid)
        if not snap.exists():
            self._write(snap, book)
        doc = {"status": STATUS_ACTIVE,
               "message": "SHADOW STRATEGY ACTIVE - RESEARCH SIGNALS ONLY",
               "as_of": book["as_of"], "active_strategy": sid,
               "strategy_name": book["strategy_name"],
               "strategy_version": book["strategy_version"],
               "combined_score": book.get("combined_score"),
               "holdings_count": book.get("holdings_count"),
               "benchmark": book.get("benchmark"),
               "safety_labels": list(SAFETY_LABELS),
               "snapshot": str(snap)}
        self._write(self._state_path(), doc)
        return doc

    def load_strategy(self, sid: str) -> Optional[dict]:
        p = self.root / "strategies" / ("%s.json" % sid)
        if p.exists():
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                return None
        return None

    def list_strategies(self) -> list:
        d = self.root / "strategies"
        if not d.exists():
            return []
        return sorted(p.stem for p in d.glob("*.json"))


def _slug(s: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in str(s))[:48]


def summarize(store: ShadowPortfolioStore) -> dict:
    """Read-only shadow summary for the research API / command center."""
    state = store.state()
    out = {"status": state.get("status"), "message": state.get("message"),
           "as_of": state.get("as_of"),
           "active_strategy": state.get("active_strategy"),
           "safety_labels": list(SAFETY_LABELS), "strategies": []}
    sid = state.get("active_strategy")
    if sid:
        book = store.load_strategy(sid)
        if book:
            out["strategies"].append(book)
    return out


__all__ = [
    "SAFETY_LABELS", "STATUS_ACTIVE", "STATUS_NO_ALPHA", "NO_ALPHA_MESSAGE",
    "build_target_book", "ShadowPortfolioStore", "summarize",
]
