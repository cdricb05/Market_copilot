r"""Stage 19 — Corporate-Action adjustment owner (the ONE canonical split/CA authority).

Why this module exists
-----------------------
The paper desk records immutable fills at the OWNED-EODHD *adjusted* close as-of the
fill date, and marks positions each day at the OWNED-EODHD adjusted close as-of the
valuation date (``api.paper_trading_desk.book_nav``). When a held name executes a
forward stock split, the provider back-adjusts its ENTIRE adjusted-close series (every
historical bar is divided by the split ratio), but the immutable fill keeps its raw
pre-split share ``quantity`` and per-share price. The desk then multiplies a NON-doubled
share count by a HALVED mark and reports a phantom ~50% loss on that single name — the
Aug-11 2026 MNST forensic anomaly (fill 42 sh @ 95.67; same-day adjusted mark 47.835 =
exactly 2.0000x; current mark 45.53 -> 42*45.53 instead of 84*45.53, understating NAV by
one whole MNST position).

What this module OWNS (and NOTHING else does)
---------------------------------------------
  1. The pure, economically-invariant split arithmetic
     (:func:`split_position` / :func:`adjust_fills`): for split ratio ``R``,
       shares_after      = shares_before * R
       per_share_cost    = per_share_cost_before / R
       total_cost_basis  = UNCHANGED
       cash / net_cash   = UNCHANGED
       NAV / P&L         = UNCHANGED *solely because of the split*.
  2. A confirm-gated, append-only, auditable CORPORATE-ACTION REGISTRY
     (``PAPER_TRADER_CORPORATE_ACTIONS_DIR`` — its OWN evidence root, NEVER the desk
     ledger root). It is an *explicit correction artifact*: registering a split does not
     rewrite a single immutable desk fill; the correction is applied as a deterministic
     READ-TIME projection over the fills.
  3. A detection scan (:func:`scan_fill_mark_artifacts`) that flags any held name whose
     recorded fill price is an (approximately) integer multiple of the SAME-DATE adjusted
     mark — the split signature — so a future split cannot silently contaminate NAV.
  4. A BEFORE/AFTER economic reconciliation (:func:`reconcile_book`) proving NAV, total
     cost basis and cash are invariant to the correction except for the removal of the
     phantom mark/quantity mismatch.

What this module NEVER does
---------------------------
  * It NEVER rewrites an immutable desk fill / order / performance row (no silent
    destructive rewrite of append-only operational evidence — the registry is a separate,
    explicit correction artifact and the adjustment is a read-time projection).
  * It creates NO order, NO NAV owner (NAV stays :func:`api.paper_trading_desk.book_nav`,
    which simply consumes the adjusted-fills projection) and mutates NO cash.
  * It NEVER registers a corporate action automatically; every registration requires the
    explicit :data:`CONFIRM_TOKEN`.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

PHASE = "STAGE19"
OWNER = "api.corporate_actions"

# --- Vocabulary -------------------------------------------------------------- #
ACTION_FORWARD_SPLIT = "FORWARD_SPLIT"          # shares * R (R > 1), price / R
ACTION_REVERSE_SPLIT = "REVERSE_SPLIT"          # shares * R (R < 1), price / R
ACTION_VOCAB = (ACTION_FORWARD_SPLIT, ACTION_REVERSE_SPLIT)

# Explicit manual confirmation token required to register ANY corporate action.
CONFIRM_TOKEN = "CONFIRM_CORPORATE_ACTION_ADJUSTMENT"

# Detection tolerance: |fill_price / same_date_mark - round(ratio)| <= tol flags a split.
_SPLIT_DETECT_TOL = 0.02

# --- Registry root (its OWN evidence root, NEVER the desk ledger root) -------- #
CA_DIR_ENV = "PAPER_TRADER_CORPORATE_ACTIONS_DIR"
_DEFAULT_CA_DIR = Path(r"D:\Stock_Prediction_app_data\corporate_actions")
_REGISTRY_FILE = "corporate_actions.json"


# --------------------------------------------------------------------------- #
# io helpers (atomic, degrade-safe)
# --------------------------------------------------------------------------- #
def _now(now: Optional[datetime]) -> datetime:
    return now or datetime.now(timezone.utc)


def _now_iso(now: Optional[datetime]) -> str:
    return _now(now).astimezone(timezone.utc).isoformat()


def _ca_dir(actions_dir=None) -> Path:
    if actions_dir is not None:
        return Path(actions_dir)
    env = os.environ.get(CA_DIR_ENV)
    return Path(env) if env else _DEFAULT_CA_DIR


def _registry_path(actions_dir=None) -> Path:
    return _ca_dir(actions_dir) / _REGISTRY_FILE


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    blob = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(blob)
        os.replace(tmp, str(path))
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _stable_hash(obj: Any) -> str:
    blob = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- #
# 1. Pure, economically-invariant split arithmetic
# --------------------------------------------------------------------------- #
def split_position(shares: float, per_share_cost: Optional[float], ratio: float) -> dict:
    """Apply split ratio ``R`` to one position, preserving economic value EXACTLY.

    ``shares_after = shares * R``; ``per_share_cost_after = per_share_cost / R``; the
    total cost basis (``shares * per_share_cost``) is invariant. For a forward 2:1 split
    ``R = 2``; for a 1:4 reverse split ``R = 0.25``.
    """
    if ratio is None or ratio <= 0:
        raise ValueError("split ratio must be a positive number")
    shares_after = shares * ratio
    psc_after = (per_share_cost / ratio) if per_share_cost is not None else None
    total_before = (shares * per_share_cost) if per_share_cost is not None else None
    total_after = (shares_after * psc_after) if psc_after is not None else None
    return {
        "shares_before": shares, "shares_after": shares_after,
        "per_share_cost_before": per_share_cost, "per_share_cost_after": psc_after,
        "total_cost_basis_before": total_before, "total_cost_basis_after": total_after,
        "cost_basis_invariant": (total_before is None or total_after is None
                                 or abs(total_before - total_after) <= 1e-6),
        "ratio": ratio,
    }


def _int_or_float(n: float):
    """Return an int when the value is (numerically) whole, else a rounded float.

    Fill quantities are whole shares; a forward integer split keeps them whole, so we
    preserve the ``int`` type the desk expects for ``quantity``."""
    r = round(n, 6)
    return int(round(r)) if abs(r - round(r)) < 1e-9 else r


def adjust_fills(fills: list[dict], actions: list[dict]) -> list[dict]:
    """Return a READ-TIME adjusted VIEW of ``fills`` with every registered corporate
    action applied. The input list is never mutated; the immutable ledger is never
    rewritten. A fill is adjusted by an action iff:
      * the fill ticker matches the action ticker;
      * the fill ``book_id`` matches the action ``book_id`` (or the action's book_id is
        ``None`` -> applies to every book);
      * the fill's ``fill_date`` is strictly BEFORE the action ``ex_date`` (a fill on or
        after the ex-date is already stated on the post-split basis).
    Each matched fill has its ``quantity`` scaled by ``R`` and its per-share ``fill_price``
    divided by ``R``; ``gross_value``, ``transaction_cost`` and ``net_cash_delta`` are
    UNCHANGED (a split changes neither cash nor total cost basis). Adjustments compose
    (multiple splits multiply). Fills are matched to actions in ascending ex-date order so
    stacked splits apply deterministically.
    """
    if not actions:
        return [dict(f) for f in fills]
    ordered = sorted(actions, key=lambda a: (str(a.get("ex_date") or ""), a.get("ratio") or 0))
    out = []
    for f in fills:
        g = dict(f)
        fd = str(g.get("fill_date") or "")
        for a in ordered:
            if a.get("ticker") != g.get("ticker"):
                continue
            abk = a.get("book_id")
            if abk is not None and abk != g.get("book_id"):
                continue
            ex = str(a.get("ex_date") or "")
            if not fd or not ex or fd >= ex:
                continue
            r = float(a.get("ratio"))
            if r <= 0:
                continue
            q = g.get("quantity")
            if q is not None:
                g["quantity"] = _int_or_float(float(q) * r)
            fp = g.get("fill_price")
            if fp is not None:
                g["fill_price"] = round(float(fp) / r, 6)
            g.setdefault("corporate_action_adjustments", [])
            g["corporate_action_adjustments"].append(
                {"action_id": a.get("action_id"), "ticker": a.get("ticker"),
                 "ex_date": ex, "ratio": r, "action_type": a.get("action_type")})
        out.append(g)
    return out


def series_split_projection(*, dates: list, values: list, actions: list[dict]) -> dict:
    """Track B — bring ONE daily price series onto the CURRENT (post-action) basis
    for every registered corporate action, WITHOUT double-adjusting a series the
    provider already back-adjusted.

    Why this exists: the owned provider cache is a verbatim snapshot of what the
    provider returned, and on 2026-08-28 the provider returned the MNST series with
    the pre-2026-08-11 bars still on the PRE-split basis (last pre-ex close 90.36,
    first post-ex close 45.53 — a cross-boundary factor of 0.5039 against a
    registered 2.0 forward split). Read as returns, that splices a mechanical ~-50%
    "loss" into return_60d, drawdown_60d, volatility and every covariance the risk
    owners build. The generic invariant this function enforces is:

        A KNOWN corporate action must never be interpreted as an economic return.

    Mechanics (pure, deterministic, read-time — the stored cache is never rewritten):
      * Actions are applied newest ex-date first, so stacked actions compose.
      * Bars on/after the ex-date are by definition already on the post-action
        basis (they traded there); they are never rescaled.
      * Bars strictly before the ex-date are classified PER BAR against the bar
        that follows them (walking backwards from the ex-date boundary): the bar
        keeps its value or is divided by the ratio, whichever lands nearer (in log
        space) to the already-classified neighbour. This brings an unadjusted
        pre-ex segment onto the current basis, is an EXACT no-op on a series the
        provider already back-adjusted (no double adjustment), and also repairs a
        single interior bar the provider stated on the other basis (the observed
        2026-08-06 MNST bar).
      * A labelled approximation: a genuine one-day move larger than sqrt(ratio)
        adjacent to a registered ex-date could be misclassified. The projection
        only ever runs for explicitly REGISTERED actions, and the full per-action
        trace (bars rescaled, boundary factors) is returned for audit.

    Returns ``{"values", "changed", "n_bars_rescaled", "trace"}``. The input lists
    are never mutated. A series with no bars on one side of an ex-date is
    internally consistent on a single basis and is left untouched.
    """
    out = [(_f_or_none(v)) for v in (values or [])]
    n = len(out)
    trace: list[dict] = []
    total_rescaled = 0
    ordered = sorted((a for a in (actions or []) if a.get("ex_date")),
                     key=lambda a: str(a.get("ex_date") or ""), reverse=True)
    for a in ordered:
        try:
            ratio = float(a.get("ratio"))
        except (TypeError, ValueError):
            continue
        if ratio <= 0 or abs(ratio - 1.0) < 1e-12:
            continue
        ex = str(a.get("ex_date") or "")[:10]
        k = next((i for i, d in enumerate(dates or []) if str(d)[:10] >= ex), None)
        base = {"action_id": a.get("action_id"), "ticker": a.get("ticker"),
                "ex_date": ex, "ratio": ratio, "action_type": a.get("action_type")}
        if k is None:
            trace.append({**base, "classification": "NO_POST_EX_BARS",
                          "n_bars_rescaled": 0,
                          "detail": "every bar predates the ex-date; the series is "
                                    "internally consistent on one basis"})
            continue
        if k == 0:
            trace.append({**base, "classification": "NO_PRE_EX_BARS",
                          "n_bars_rescaled": 0,
                          "detail": "every bar is on/after the ex-date; already on "
                                    "the post-action basis"})
            continue
        anchor = next((out[i] for i in range(k, n)
                       if out[i] is not None and out[i] > 0), None)
        if anchor is None:
            trace.append({**base, "classification": "NO_PRICEABLE_POST_EX_BAR",
                          "n_bars_rescaled": 0})
            continue
        factor_before = (out[k] / out[k - 1]
                         if (out[k] and out[k - 1]) else None)
        n_scaled = 0
        for i in range(k - 1, -1, -1):
            v = out[i]
            if v is None or v <= 0:
                continue
            keep_dist = abs(math.log(v / anchor))
            rescaled = v / ratio
            rescale_dist = abs(math.log(rescaled / anchor))
            if rescale_dist < keep_dist:
                out[i] = round(rescaled, 6)
                n_scaled += 1
            anchor = out[i]
        factor_after = (out[k] / out[k - 1]
                        if (out[k] and out[k - 1]) else None)
        total_rescaled += n_scaled
        trace.append({
            **base,
            "classification": ("ALREADY_ADJUSTED" if n_scaled == 0
                               else "PRE_EX_SEGMENT_RESCALED"),
            "n_pre_ex_bars": k,
            "n_bars_rescaled": n_scaled,
            "cross_boundary_factor_before": (round(factor_before, 6)
                                             if factor_before else None),
            "cross_boundary_factor_after": (round(factor_after, 6)
                                            if factor_after else None),
        })
    return {"values": out, "changed": total_rescaled > 0,
            "n_bars_rescaled": total_rescaled, "trace": trace, "owner": OWNER}


def _f_or_none(x):
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


# --------------------------------------------------------------------------- #
# 2. Confirm-gated, append-only corporate-action registry
# --------------------------------------------------------------------------- #
def _action_id(ticker: str, ex_date: str, ratio: float, book_id: Optional[str]) -> str:
    key = "%s|%s|%s|%s" % (ticker, ex_date, ratio, book_id or "*")
    return "ca_%s_%s_%s" % (ticker, str(ex_date).replace("-", ""),
                            _stable_hash(key)[:8])


def load_actions(*, actions_dir=None, ticker: Optional[str] = None,
                 book_id: Optional[str] = None) -> list[dict]:
    """PURE reader of the registered corporate actions (optionally filtered). Degrade-safe:
    a missing/empty registry returns ``[]`` — so every NAV/holdings read that consults the
    registry is a no-op until a corporate action is explicitly registered."""
    rows = _load_json(_registry_path(actions_dir))
    if not isinstance(rows, list):
        return []
    out = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        if ticker is not None and r.get("ticker") != ticker:
            continue
        if book_id is not None and r.get("book_id") not in (None, book_id):
            continue
        out.append(r)
    return out


def current_actions(*, book_id: Optional[str] = None, as_of: Optional[str] = None,
                    actions_dir=None) -> list[dict]:
    """Stage 19.1 — THE ONE resolution point for a CURRENT economic read.

    Returns the registered corporate actions for ``book_id`` (plus the all-books actions).
    This is the function every current-state consumer calls; no consumer resolves the
    registry, filters ex-dates, or recreates split arithmetic itself. Degrade-safe: an
    empty registry returns ``[]``, so every consumer is an EXACT no-op until an operator
    explicitly registers a corporate action.

    ``as_of`` restricts the result to actions whose ``ex_date`` is on or before that date.
    It is for callers working against a NON-back-adjusted price series only. The desk
    deliberately does NOT use it: it marks against the owned provider's fully
    back-adjusted close series, where an earlier valuation date must also use the
    post-split share count (see ``paper_trading_desk._resolve_corporate_actions``). The
    ex-date governs the FILL side inside :func:`adjust_fills` in every case.
    """
    rows = load_actions(actions_dir=actions_dir, book_id=book_id)
    if as_of is None:
        return rows
    cutoff = str(as_of)[:10]
    return [r for r in rows if str(r.get("ex_date") or "")[:10] <= cutoff]


def registry_fingerprint(*, actions: Optional[list[dict]] = None,
                         book_id: Optional[str] = None, as_of: Optional[str] = None,
                         actions_dir=None) -> dict:
    """The deterministic IDENTITY of the corporate-action registry state a current read
    was computed against — the value every state hash / proposal identity binds.

    An EMPTY registry has a stable, well-known fingerprint, so an artifact written before
    this contract existed (no fingerprint recorded) compares equal to "no corporate action
    registered" and is therefore NOT stale. Registering an action changes the fingerprint,
    which invalidates every proposal / order plan bound to the previous one.
    """
    rows = actions if actions is not None else current_actions(
        book_id=book_id, as_of=as_of, actions_dir=actions_dir)
    compact = sorted(
        ({"action_id": r.get("action_id"), "action_type": r.get("action_type"),
          "ticker": r.get("ticker"), "ex_date": str(r.get("ex_date") or "")[:10],
          "ratio": float(r.get("ratio") or 0.0), "book_id": r.get("book_id")}
         for r in rows),
        key=lambda r: (r["ticker"] or "", r["ex_date"] or "", r["action_id"] or ""))
    return {"owner": OWNER, "n_registered": len(compact),
            "fingerprint": _stable_hash(compact), "actions": compact}


#: The fingerprint of an EMPTY registry — the backward-compatible default for any
#: artifact persisted before the Stage 19.1 identity contract existed.
EMPTY_REGISTRY_FINGERPRINT = _stable_hash([])

#: The staleness reason code every consumer reports verbatim (one vocabulary).
STALE_REASON_CORPORATE_ACTION = "CORPORATE_ACTION_REGISTERED_SINCE_PROPOSAL"


def staleness_vs_registry(bound_fingerprint: Optional[str], *,
                          book_id: Optional[str] = None, actions_dir=None,
                          current: Optional[dict] = None) -> dict:
    """Is an artifact that was BOUND to ``bound_fingerprint`` still valid against the
    CURRENT corporate-action registry?

    A registered corporate action changes the economic holdings a proposal / decision /
    order plan was computed against, so any artifact bound to an older registry state is
    STALE and must be re-reviewed — it can never be approved or turned into orders.

    Backward compatible by construction: an artifact with no recorded fingerprint
    (``None``) is treated as bound to the EMPTY registry, so with nothing registered it is
    NOT stale (exact no-op), while a newly registered action makes it stale immediately.
    """
    cur = current or registry_fingerprint(book_id=book_id, actions_dir=actions_dir)
    bound = bound_fingerprint or EMPTY_REGISTRY_FINGERPRINT
    stale = bound != cur["fingerprint"]
    return {
        "stale": stale,
        "reason": STALE_REASON_CORPORATE_ACTION if stale else None,
        "bound_corporate_actions_hash": bound,
        "current_corporate_actions_hash": cur["fingerprint"],
        "bound_to_empty_registry": bound == EMPTY_REGISTRY_FINGERPRINT,
        "n_registered_now": cur["n_registered"],
        "registered_actions_now": cur["actions"],
        "owner": OWNER,
        "message": (("A corporate action has been registered since this was produced; the "
                     "economic holdings it was computed against no longer describe the "
                     "current portfolio. A fresh review is required before any approval or "
                     "order plan.") if stale else None),
    }


def register_action(*, confirm: Optional[str], ticker: str, ex_date: str, ratio: float,
                    action_type: str = ACTION_FORWARD_SPLIT, book_id: Optional[str] = None,
                    source: Optional[str] = None, evidence: Optional[dict] = None,
                    actor: Optional[str] = None, actions_dir=None,
                    now: Optional[datetime] = None) -> dict:
    """Register ONE corporate action (append-only, idempotent). Writes NOTHING unless
    ``confirm == CONFIRM_TOKEN``. An identical (ticker, ex_date, ratio, book_id) action is
    idempotent (no duplicate). The registry is an explicit correction artifact; it never
    rewrites an immutable desk fill."""
    base = {"owner": OWNER, "phase": PHASE, "registered": False, "wrote_to_desk_ledger": False,
            "changed_holdings": False, "changed_cash": False, "changed_nav": False,
            "rewrote_immutable_evidence": False, "paper_only": True, "manual_review": True}
    if action_type not in ACTION_VOCAB:
        return {**base, "status": "INVALID_ACTION_TYPE",
                "message": "action_type must be one of %s" % (ACTION_VOCAB,)}
    if confirm != CONFIRM_TOKEN:
        return {**base, "status": "CONFIRMATION_REQUIRED",
                "message": "Registering a corporate action requires explicit confirmation.",
                "confirm_required_token": CONFIRM_TOKEN}
    try:
        ratio = float(ratio)
    except (TypeError, ValueError):
        return {**base, "status": "INVALID_RATIO", "message": "ratio must be numeric"}
    if ratio <= 0:
        return {**base, "status": "INVALID_RATIO", "message": "ratio must be > 0"}
    if not ticker or not ex_date:
        return {**base, "status": "INVALID_ACTION", "message": "ticker and ex_date required"}

    aid = _action_id(ticker, ex_date, ratio, book_id)
    existing = load_actions(actions_dir=actions_dir)
    for r in existing:
        if r.get("action_id") == aid:
            return {**base, "status": "REUSED_EXISTING", "registered": True, "reused": True,
                    "action": r}
    ts = _now_iso(now)
    record = {
        "action_id": aid, "owner": OWNER, "action_type": action_type,
        "ticker": ticker, "ex_date": str(ex_date)[:10], "ratio": ratio,
        "book_id": book_id, "source": source or "MANUAL_REGISTRATION",
        "evidence": evidence or {}, "registered_at": ts, "actor": actor or "operator",
        "confirm_token": CONFIRM_TOKEN, "immutable": True,
    }
    record["content_hash"] = _stable_hash({k: record[k] for k in
                                           ("action_type", "ticker", "ex_date", "ratio", "book_id")})
    rows = existing + [record]
    _atomic_write_json(_registry_path(actions_dir), rows)
    return {**base, "status": "REGISTERED", "registered": True, "reused": False,
            "action": record, "wrote_to_correction_artifact": True}


# --------------------------------------------------------------------------- #
# 3. Detection scan — the split signature (fill price = k * same-date mark, k != 1)
# --------------------------------------------------------------------------- #
def _series_at_or_before(series: list, as_of: str) -> Optional[float]:
    best = None
    for row in series or []:
        try:
            d, v = row[0], row[1]
        except (TypeError, IndexError):
            continue
        if v is None:
            continue
        if d <= as_of and (best is None or d > best[0]):
            best = (d, v)
    return best[1] if best else None


def scan_fill_mark_artifacts(fills: list[dict], marks: dict, *,
                             registered: Optional[list[dict]] = None) -> dict:
    """Detect held names whose recorded fill price is an (approximately) integer multiple
    (or unit fraction) of the SAME-DATE adjusted mark — the back-adjusted-series split
    signature. Read-only; suggests corrections but registers nothing."""
    series = (marks or {}).get("series") or {}
    reg_keys = {(r.get("ticker"), r.get("ex_date"), round(float(r.get("ratio") or 0), 6))
                for r in (registered or [])}
    suspects = []
    for f in fills:
        tk = f.get("ticker")
        fd = f.get("fill_date")
        fp = f.get("fill_price")
        if not tk or not fd or fp in (None, 0):
            continue
        m = _series_at_or_before(series.get(tk) or [], fd)
        if not m or m == 0:
            continue
        ratio = fp / m
        # forward-split signature (fill price a whole multiple of the halved mark)
        near_int = abs(ratio - round(ratio)) <= _SPLIT_DETECT_TOL and round(ratio) >= 2
        # reverse-split signature (fill price a unit fraction of the mark)
        inv = m / fp
        near_inv = abs(inv - round(inv)) <= _SPLIT_DETECT_TOL and round(inv) >= 2
        if near_int:
            suspects.append({"ticker": tk, "fill_date": fd, "fill_price": round(fp, 6),
                             "same_date_mark": round(m, 6), "implied_ratio": round(ratio, 6),
                             "suggested_ratio": float(round(ratio)),
                             "action_type": ACTION_FORWARD_SPLIT,
                             "already_registered": (tk, fd, float(round(ratio))) in {
                                 (t, e, r) for (t, e, r) in
                                 [(k[0], k[1], k[2]) for k in reg_keys]} or bool(
                                 [r for r in (registered or []) if r.get("ticker") == tk])})
        elif near_inv:
            suspects.append({"ticker": tk, "fill_date": fd, "fill_price": round(fp, 6),
                             "same_date_mark": round(m, 6), "implied_ratio": round(ratio, 6),
                             "suggested_ratio": round(1.0 / round(inv), 6),
                             "action_type": ACTION_REVERSE_SPLIT,
                             "already_registered": bool(
                                 [r for r in (registered or []) if r.get("ticker") == tk])})
    return {"owner": OWNER, "n_fills_scanned": len(fills), "n_suspects": len(suspects),
            "suspects": suspects, "detection_tolerance": _SPLIT_DETECT_TOL,
            "note": ("A suspect is a held name whose immutable fill price is ~an integer "
                     "multiple (forward split) or unit fraction (reverse split) of the "
                     "same-date owned adjusted mark — the back-adjusted-series signature.")}


# --------------------------------------------------------------------------- #
# 4. BEFORE/AFTER economic reconciliation over a desk book
# --------------------------------------------------------------------------- #
def _holdings_cost_nav(book: dict, fills: list[dict], marks: dict, as_of: Optional[str]):
    """Small local re-implementation of the desk fold (cash/holdings/cost/NAV) so the
    reconciliation is self-contained and does not import the desk (avoids a cycle)."""
    initial = float(book.get("initial_capital") or 0.0)
    cash = initial
    qty: dict[str, float] = {}
    cost: dict[str, float] = {}
    for f in sorted(fills, key=lambda x: (x.get("fill_date") or "", x.get("fill_id") or "")):
        if book.get("book_id") is not None and f.get("book_id") != book.get("book_id"):
            continue
        if as_of is not None and (f.get("fill_date") or "") > as_of:
            continue
        ncd = f.get("net_cash_delta")
        if ncd is not None:
            cash += float(ncd)
        tk = f.get("ticker")
        q = float(f.get("quantity") or 0)
        buy = f.get("side", "").endswith("BUY")
        qty[tk] = qty.get(tk, 0.0) + (q if buy else -q)
        if buy and ncd is not None:
            cost[tk] = cost.get(tk, 0.0) + (-float(ncd))
        if abs(qty.get(tk, 0.0)) < 1e-9:
            qty.pop(tk, None)
    series = (marks or {}).get("series") or {}
    invested = 0.0
    per_name = {}
    for tk, q in qty.items():
        px = _series_at_or_before(series.get(tk) or [], as_of) if as_of else None
        mv = (q * px) if px is not None else None
        if mv is not None:
            invested += mv
        per_name[tk] = {"quantity": q, "price": px, "market_value": mv,
                        "cost_basis": round(cost.get(tk, 0.0), 4) if tk in cost else None}
    return {"cash": round(cash, 4), "invested": round(invested, 4),
            "nav": round(cash + invested, 4), "holdings": qty,
            "cost_basis_total": round(sum(cost.values()), 4), "per_name": per_name}


def reconcile_book(*, book: dict, fills: list[dict], marks: dict, actions: list[dict],
                   as_of: Optional[str] = None) -> dict:
    """BEFORE/AFTER economic reconciliation of applying ``actions`` to ``book``. Proves the
    correction removes the phantom quantity/mark mismatch while leaving cash and total cost
    basis invariant; reports the per-name NAV delta (the phantom that was removed)."""
    as_of = as_of or (marks or {}).get("latest_completed_date")
    before = _holdings_cost_nav(book, [dict(f) for f in fills], marks, as_of)
    after = _holdings_cost_nav(book, adjust_fills(fills, actions), marks, as_of)
    per_name_delta = []
    names = sorted(set(before["per_name"]) | set(after["per_name"]))
    for tk in names:
        b = before["per_name"].get(tk, {})
        a = after["per_name"].get(tk, {})
        d_mv = ((a.get("market_value") or 0) - (b.get("market_value") or 0))
        if abs(d_mv) > 1e-6 or (a.get("quantity") != b.get("quantity")):
            per_name_delta.append({
                "ticker": tk,
                "quantity_before": b.get("quantity"), "quantity_after": a.get("quantity"),
                "market_value_before": b.get("market_value"),
                "market_value_after": a.get("market_value"),
                "market_value_delta": round(d_mv, 4),
                "cost_basis_before": b.get("cost_basis"), "cost_basis_after": a.get("cost_basis")})
    return {
        "owner": OWNER, "as_of_date": as_of, "book_id": book.get("book_id"),
        "actions_applied": [{"ticker": x.get("ticker"), "ex_date": x.get("ex_date"),
                             "ratio": x.get("ratio"), "action_type": x.get("action_type")}
                            for x in (actions or [])],
        "before": {"nav": before["nav"], "cash": before["cash"],
                   "invested": before["invested"], "cost_basis_total": before["cost_basis_total"]},
        "after": {"nav": after["nav"], "cash": after["cash"],
                  "invested": after["invested"], "cost_basis_total": after["cost_basis_total"]},
        "nav_delta": round(after["nav"] - before["nav"], 4),
        "cash_invariant": abs(after["cash"] - before["cash"]) <= 1e-4,
        "cost_basis_invariant": abs(after["cost_basis_total"] - before["cost_basis_total"]) <= 1e-4,
        "phantom_nav_removed": round(after["nav"] - before["nav"], 4),
        "per_name_delta": per_name_delta,
        "note": ("BEFORE marks a split name's raw (pre-split) share count against the "
                 "back-adjusted mark (the phantom loss). AFTER scales the pre-ex-date "
                 "shares by the split ratio; cash and total cost basis are invariant."),
    }


# --------------------------------------------------------------------------- #
# 4b. Stage 19.1 — CURRENT-economic-state performance projection (correction overlay)
# --------------------------------------------------------------------------- #
def _projected_nav(book: dict, adj_fills: list[dict], marks: dict, as_of: str):
    """(nav, cash, invested, holdings, missing) for ONE date on the corrected basis."""
    blk = _holdings_cost_nav(book, adj_fills, marks, as_of)
    missing = sorted(tk for tk, n in blk["per_name"].items() if n.get("price") is None)
    holdings = {tk: _int_or_float(q) for tk, q in sorted(blk["holdings"].items())}
    return blk["nav"], blk["cash"], blk["invested"], holdings, missing


def project_current_performance(*, book: dict, rows: list[dict], fills: list[dict],
                                marks: dict, actions: list[dict]) -> dict:
    """Stage 19.1 — the CURRENT-ECONOMIC-STATE view of the forward-performance curve.

    Why this exists
    ---------------
    A forward-performance row is IMMUTABLE historical evidence: it records the NAV that
    was true, on the provider basis that existed, on the day it was appended. When a held
    name later splits, the provider back-adjusts its whole close series, so the row written
    the day BEFORE the ex-date prices the raw share count on the PRE-split basis while the
    row written ON the ex-date prices the SAME raw share count on the POST-split basis.
    Read literally as CURRENT performance, that pair reports a ~50% one-day loss on the
    name that a split cannot economically cause.

    This function does NOT rewrite those rows. It returns a parallel CURRENT view in which
    every date's NAV is recomputed by the ONE canonical projection (:func:`adjust_fills`)
    against the CURRENT mark series, so the split contributes EXACTLY ZERO economic P&L.
    Each projected row carries the untouched historical value under ``raw`` so the
    immutable evidence is always recoverable and never silently overwritten.

    Degrade-safe and backward compatible: with no registered action (or no book / no rows)
    the input rows are returned unchanged and ``applied`` is ``False`` — an EXACT no-op.
    A date whose corrected valuation cannot be priced (a missing owned mark) keeps its raw
    values and is flagged ``projection_state='UNPRICED'`` rather than being invented.
    """
    base = {"owner": OWNER, "phase": PHASE, "applied": False,
            "rewrote_immutable_evidence": False,
            "actions_applied": [], "n_rows_projected": 0,
            "economic_pnl_from_corporate_action": 0.0,
            "note": ("A corporate action changes the share count and the per-share basis "
                     "together; it creates no economic profit or loss. The CURRENT view "
                     "recomputes each date's NAV through the ONE canonical projection; the "
                     "immutable historical row is preserved verbatim under 'raw'.")}
    if not actions or not rows or not book:
        return {**base, "rows": [dict(r) for r in rows or []]}

    adj = adjust_fills(fills, actions)
    initial = float(book.get("initial_capital") or 0.0)
    ordered = sorted(rows, key=lambda r: str(r.get("date") or ""))
    prev_nav = initial
    peak_nav = initial
    out: list[dict] = []
    n_projected = 0
    for r in ordered:
        d = str(r.get("date") or "")
        row = dict(r)
        nav, cash, invested, holdings, missing = _projected_nav(book, adj, marks, d) \
            if d else (None, None, None, None, None)
        if nav is None or missing:
            row["projection_state"] = "UNPRICED" if d else "NO_DATE"
            row["corporate_action_adjusted"] = False
            row["missing_marks"] = list(missing or r.get("missing_marks") or [])
            prev_nav = float(row.get("nav") or prev_nav)
            peak_nav = max(peak_nav, prev_nav)
            out.append(row)
            continue
        raw_keep = {k: r.get(k) for k in
                    ("nav", "cash", "invested", "holdings", "daily_return_pct",
                     "cumulative_return_pct", "drawdown_pct")}
        peak_nav = max(peak_nav, nav)
        row.update({
            "nav": round(nav, 2), "cash": round(cash, 2), "invested": round(invested, 2),
            "holdings": holdings, "holdings_count": len(holdings), "missing_marks": [],
            "daily_return_pct": (round(100.0 * (nav / prev_nav - 1.0), 4)
                                 if prev_nav else None),
            "cumulative_return_pct": (round(100.0 * (nav / initial - 1.0), 4)
                                      if initial else None),
            "drawdown_pct": (round(100.0 * (nav / peak_nav - 1.0), 4) if peak_nav else None),
            "projection_state": "CORPORATE_ACTION_ADJUSTED",
            "corporate_action_adjusted": True,
            "raw": raw_keep,
        })
        prev_nav = nav
        n_projected += 1
        out.append(row)
    return {**base, "applied": True, "rows": out, "n_rows_projected": n_projected,
            "actions_applied": [{"action_id": a.get("action_id"), "ticker": a.get("ticker"),
                                 "ex_date": a.get("ex_date"), "ratio": a.get("ratio"),
                                 "action_type": a.get("action_type")}
                                for a in actions]}


# --------------------------------------------------------------------------- #
# 5. READ contract — live forensic scan + BEFORE/AFTER reconciliation (writes nothing)
# --------------------------------------------------------------------------- #
def _ca_safety() -> dict:
    return {"read_only": True, "paper_only": True, "manual_review": True,
            "rewrote_immutable_evidence": False, "changed_holdings": False,
            "changed_cash": False, "changed_nav": False, "wrote_to_desk_ledger": False,
            "safety_badges": ["READ ONLY", "PAPER ONLY", "MANUAL REVIEW",
                              "NO SILENT REWRITE"]}


def load_corporate_action_report(*, desk_dir=None, actions_dir=None) -> dict:
    """READ-ONLY corporate-action integrity report over the live desk book. Composes the
    detection scan, the registered corrective actions, and (for the registered actions plus
    any high-confidence suspect) the BEFORE/AFTER economic reconciliation. Writes nothing;
    never registers automatically. Degrade-safe."""
    from paper_trader.api import paper_trading_desk as desk
    generated_at = _now_iso(None)
    try:
        sdir = desk._desk_dir(desk_dir)
        book = desk.open_book(sdir)
        fills = desk._fills(sdir)
        marks = desk.read_marks(desk_dir)
    except Exception as exc:  # noqa: BLE001
        return {"owner": OWNER, "phase": PHASE, "status": "UNAVAILABLE",
                "generated_at": generated_at, "message": str(exc)[:160], **_ca_safety()}
    if book is None:
        return {"owner": OWNER, "phase": PHASE, "status": "NO_ACTIVE_BOOK",
                "generated_at": generated_at, **_ca_safety()}
    registered = load_actions(actions_dir=actions_dir, book_id=book.get("book_id"))
    scan = scan_fill_mark_artifacts(fills, marks, registered=registered)
    # Reconcile against the union of registered actions and any confident (>=2x) suspects,
    # so the operator sees the exact NAV correction a registration WOULD apply.
    proposed = list(registered)
    reg_keys = {(r.get("ticker"), r.get("ex_date")) for r in registered}
    for s in scan["suspects"]:
        # a suspect's implied ex-date is unknown from the back-adjusted cache; the current
        # latest completed date is the first close on the post-split basis.
        ex = marks.get("latest_completed_date")
        if (s["ticker"], ex) in reg_keys:
            continue
        proposed.append({"ticker": s["ticker"], "ex_date": ex, "ratio": s["suggested_ratio"],
                         "action_type": s["action_type"], "book_id": None,
                         "provisional_suspect": True})
    reconciliation = reconcile_book(book=book, fills=fills, marks=marks,
                                    actions=proposed) if proposed else None
    return {
        "owner": OWNER, "phase": PHASE, "status": "OK", "generated_at": generated_at,
        "book_id": book.get("book_id"), "marks_date": marks.get("latest_completed_date"),
        "registered_actions": registered, "n_registered": len(registered),
        "scan": scan, "reconciliation": reconciliation,
        "confirm_required_token": CONFIRM_TOKEN,
        "register_path": "POST /v1/operations/corporate-actions/register",
        "note": ("Reconciliation previews the exact NAV correction a registration would "
                 "apply; a provisional suspect is shown until an operator explicitly "
                 "registers it with the confirmation token."),
        **_ca_safety(),
    }


__all__ = [
    "PHASE", "OWNER", "ACTION_FORWARD_SPLIT", "ACTION_REVERSE_SPLIT", "ACTION_VOCAB",
    "CONFIRM_TOKEN", "CA_DIR_ENV", "split_position", "adjust_fills", "load_actions",
    "register_action", "scan_fill_mark_artifacts", "reconcile_book",
    "load_corporate_action_report",
    # Track B — the READ-TIME price-series counterpart of adjust_fills.
    "series_split_projection",
    # Stage 19.1 — the CURRENT-state propagation contract.
    "current_actions", "registry_fingerprint", "EMPTY_REGISTRY_FINGERPRINT",
    "project_current_performance", "staleness_vs_registry",
    "STALE_REASON_CORPORATE_ACTION",
]
