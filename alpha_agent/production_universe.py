"""
alpha_agent.production_universe — Stage 8 acceptance-vs-production run modes and
the DYNAMIC production research universe (WS2).

Two explicit, non-overlapping run modes replace the old ambiguous "bounded
universe" that leaked a six-symbol research subset into permanent production:

  * ACCEPTANCE MODE — a small, deterministic fixture universe (default
    AAPL/MSFT/NVDA/AMZN/JPM/XOM). Used ONLY by tests, smoke validation and manual
    acceptance. It is never selected by a scheduled production task.

  * PRODUCTION MODE — dynamically resolves the COMPLETE eligible research
    universe from Norgate's licensed, survivorship-safe library: the "S&P 500
    Current & Past" watchlist (current + historical constituents INCLUDING
    delisted securities). There is NO permanent hard-coded 6 / 300 / N cap: a
    per-job BATCH size is permitted, a permanent total-universe cap is not. The
    full universe is sharded by the durable cursor in ``acquisition_campaign``.

Resolution is deterministic (sorted) so the campaign cursor is stable across
runs. When the Norgate Data Updater is momentarily unavailable the resolver
DEGRADES honestly (``degraded=True`` + exact reason) and may fall back to the
owned survivorship-free price panel; it NEVER silently substitutes the six-symbol
acceptance fixture for the production universe.

Safety: pure resolution + hashing. No network beyond the local Norgate vendor
library, no operational-ledger write, no order/fill/signal/decision.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional

STAGE = "8"

ACCEPTANCE = "acceptance"
PRODUCTION = "production"
RUN_MODES = (ACCEPTANCE, PRODUCTION)

# The default acceptance fixture (tests / smoke / manual acceptance ONLY).
DEFAULT_ACCEPTANCE_UNIVERSE = ["AAPL", "MSFT", "NVDA", "AMZN", "JPM", "XOM"]

# Universe-source dispositions.
SRC_NORGATE_SURVIVORSHIP = "norgate_survivorship_current_and_past"
SRC_NORGATE_CURRENT = "norgate_current_constituents"
SRC_OWNED_PANEL_FALLBACK = "owned_price_panel_fallback"
SRC_ACCEPTANCE_FIXTURE = "acceptance_fixture"
SRC_DEGRADED_EMPTY = "degraded_no_universe_source"

# Universe scope: the full survivorship-safe set (current + past incl. delisted)
# for PIT price/filing research, or CURRENT constituents only for inherently
# forward-looking lanes (e.g. forward analyst vintages, where a delisted name has
# no future data). 'current' is a correct eligibility criterion, NOT a cap.
SCOPE_SURVIVORSHIP = "survivorship"
SCOPE_CURRENT = "current"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def universe_fingerprint(symbols: "list[str]") -> str:
    """Stable content hash of a symbol set (order-independent). Used to detect a
    universe change so the campaign cursor can absorb NEW members without ever
    dropping or re-doing completed ones."""
    canon = "\n".join(sorted({str(s).strip().upper() for s in symbols if s}))
    return "u_" + hashlib.sha256(canon.encode("utf-8")).hexdigest()[:24]


@dataclass
class UniverseResult:
    mode: str
    symbols: "list[str]"
    source: str
    survivorship_safe: bool
    degraded: bool = False
    reason: Optional[str] = None
    generated_at: str = field(default_factory=_utc_now_iso)

    @property
    def target_count(self) -> int:
        return len(self.symbols)

    @property
    def fingerprint(self) -> str:
        return universe_fingerprint(self.symbols)

    def as_dict(self) -> dict:
        return {
            "mode": self.mode, "symbols": self.symbols,
            "target_count": self.target_count, "source": self.source,
            "survivorship_safe": self.survivorship_safe,
            "degraded": self.degraded, "reason": self.reason,
            "fingerprint": self.fingerprint, "generated_at": self.generated_at,
        }


# --------------------------------------------------------------------------- #
# Run-mode resolution.
# --------------------------------------------------------------------------- #
def _production_cfg(cfg: dict) -> dict:
    p = cfg.get("production") if isinstance(cfg, dict) else None
    return p if isinstance(p, dict) else {}


def resolve_run_mode(cfg: dict, *, override: Optional[str] = None) -> str:
    """Resolve the run mode. An explicit ``override`` (the scheduled-task
    entrypoint passes 'production') wins; else ``production.run_mode`` from the
    config; else the safe default ACCEPTANCE (so a test/manual invocation never
    accidentally sweeps the full universe)."""
    for candidate in (override, _production_cfg(cfg).get("run_mode"),
                      cfg.get("run_mode") if isinstance(cfg, dict) else None):
        if candidate:
            value = str(candidate).strip().lower()
            if value in RUN_MODES:
                return value
    return ACCEPTANCE


def acceptance_universe(cfg: dict) -> "list[str]":
    """The deterministic acceptance fixture universe. Sourced from
    ``production.acceptance_universe`` else ``production_handlers.bounded_universe``
    else the built-in six-symbol default. Test/smoke/manual ONLY."""
    ph = cfg.get("production_handlers") if isinstance(cfg, dict) else None
    ph = ph if isinstance(ph, dict) else {}
    syms = (_production_cfg(cfg).get("acceptance_universe")
            or ph.get("bounded_universe")
            or DEFAULT_ACCEPTANCE_UNIVERSE)
    return sorted({str(s).strip().upper() for s in syms if s})


# --------------------------------------------------------------------------- #
# Norgate accessor (injectable for deterministic tests).
# --------------------------------------------------------------------------- #
class NorgateUniverseAccessor:
    """Thin, lazy wrapper over the licensed ``norgatedata`` vendor library. Never
    installed here; if the package/NDU is unavailable it degrades to ``None``
    results rather than raising."""

    def __init__(self, module: Any = None):
        self._nd = module

    def _import(self):
        if self._nd is not None:
            return self._nd
        try:
            import norgatedata  # noqa: PLC0415 — lazy local vendor import
            self._nd = norgatedata
        except Exception:  # noqa: BLE001
            self._nd = None
        return self._nd

    def available(self) -> "tuple[bool, str]":
        nd = self._import()
        if nd is None:
            return False, "norgatedata not importable (Norgate library absent)"
        try:
            if not bool(nd.status()):
                return False, "Norgate Data Updater not running"
        except Exception as exc:  # noqa: BLE001
            return False, "norgatedata.status() failed: %s" % type(exc).__name__
        return True, "NDU running"

    def watchlist_symbols(self, name: str) -> "list[str]":
        nd = self._import()
        if nd is None:
            return []
        try:
            return [str(s) for s in nd.watchlist_symbols(name)]
        except Exception:  # noqa: BLE001
            return []


# --------------------------------------------------------------------------- #
# Production universe resolution.
# --------------------------------------------------------------------------- #
def resolve_production_universe(
        cfg: dict, *, scope: str = SCOPE_SURVIVORSHIP,
        norgate: Optional[Any] = None,
        fallback_symbols: Optional[Callable[[], "list[str]"]] = None,
        now: Optional[Callable[[], str]] = None) -> UniverseResult:
    """Resolve the COMPLETE eligible production research universe.

    ``scope='survivorship'`` (default): Norgate "S&P 500 Current & Past"
    (survivorship-safe; includes delisted + past constituents) UNION the current
    "S&P 500" so no live member is ever missed — for PIT price/filing research.
    ``scope='current'``: the current constituents only — the correct eligibility
    for inherently forward-looking lanes (forward analyst vintages). Fallback
    (Norgate momentarily unavailable): the owned survivorship-free price panel
    (``fallback_symbols``) — labelled degraded, and NEVER the acceptance fixture.
    Deterministic (sorted) so the cursor is stable."""
    now = now or _utc_now_iso
    src = _production_cfg(cfg).get("production_universe_source") or {}
    survivorship_wl = src.get("survivorship_watchlist", "S&P 500 Current & Past")
    current_wl = src.get("current_watchlist", "S&P 500")

    acc = norgate if isinstance(norgate, NorgateUniverseAccessor) \
        else NorgateUniverseAccessor(norgate)
    ok, why = acc.available()
    if ok:
        if scope == SCOPE_CURRENT:
            merged = sorted({str(s).strip().upper()
                             for s in acc.watchlist_symbols(current_wl) if s})
            if merged:
                return UniverseResult(
                    mode=PRODUCTION, symbols=merged,
                    source=SRC_NORGATE_CURRENT, survivorship_safe=False,
                    degraded=False,
                    reason="Norgate current '%s' constituents (forward-looking "
                           "eligibility; delisted names have no future data)"
                           % current_wl, generated_at=now())
            why = ("Norgate reachable but current watchlist '%s' returned no "
                   "symbols" % current_wl)
        else:
            survivorship = acc.watchlist_symbols(survivorship_wl)
            current = acc.watchlist_symbols(current_wl) if current_wl else []
            merged = sorted({str(s).strip().upper()
                             for s in list(survivorship) + list(current) if s})
            if merged:
                return UniverseResult(
                    mode=PRODUCTION, symbols=merged,
                    source=SRC_NORGATE_SURVIVORSHIP, survivorship_safe=True,
                    degraded=False,
                    reason="Norgate '%s' (survivorship-safe; current + past "
                           "incl. delisted) unioned with current '%s'"
                           % (survivorship_wl, current_wl), generated_at=now())
            why = ("Norgate reachable but watchlist '%s' returned no symbols"
                   % survivorship_wl)

    # Degraded: Norgate unavailable/empty. Fall back to the owned price panel
    # (still survivorship-free, Norgate-sourced), NEVER the acceptance fixture.
    fb = sorted({str(s).strip().upper() for s in (fallback_symbols() or [])}) \
        if fallback_symbols else []
    if fb:
        return UniverseResult(
            mode=PRODUCTION, symbols=fb, source=SRC_OWNED_PANEL_FALLBACK,
            survivorship_safe=True, degraded=True,
            reason="Norgate universe unavailable (%s); fell back to the owned "
                   "survivorship-free price panel. This is a DEGRADED fallback, "
                   "not the permanent production universe — it re-resolves to the "
                   "full Norgate universe when NDU is reachable." % why,
            generated_at=now())
    return UniverseResult(
        mode=PRODUCTION, symbols=[], source=SRC_DEGRADED_EMPTY,
        survivorship_safe=True, degraded=True,
        reason="No production universe source available (%s) and no owned-panel "
               "fallback; the campaign holds until Norgate is reachable. The "
               "acceptance fixture is NEVER substituted for production." % why,
        generated_at=now())


def resolve_universe(cfg: dict, *, mode: Optional[str] = None,
                     scope: str = SCOPE_SURVIVORSHIP,
                     norgate: Optional[Any] = None,
                     fallback_symbols: Optional[Callable[[], "list[str]"]] = None,
                     now: Optional[Callable[[], str]] = None) -> UniverseResult:
    """Resolve the run universe for the effective mode. Acceptance returns the
    fixture; production dynamically resolves the full Norgate universe at the
    requested ``scope`` (survivorship-safe full set, or current constituents)."""
    now = now or _utc_now_iso
    resolved_mode = mode or resolve_run_mode(cfg)
    if resolved_mode == ACCEPTANCE:
        return UniverseResult(
            mode=ACCEPTANCE, symbols=acceptance_universe(cfg),
            source=SRC_ACCEPTANCE_FIXTURE, survivorship_safe=False,
            degraded=False,
            reason="deterministic acceptance fixture (tests / smoke / manual "
                   "acceptance only; never a scheduled production task)",
            generated_at=now())
    return resolve_production_universe(
        cfg, scope=scope, norgate=norgate, fallback_symbols=fallback_symbols,
        now=now)
