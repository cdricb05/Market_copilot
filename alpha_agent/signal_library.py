"""
alpha_agent.signal_library - Stage 11 Workstream B: the multi-family cross-
sectional SIGNAL FACTORY.

A reusable, configuration-driven catalogue of interpretable cross-sectional rank
signals computed ONLY from owned, survivorship-safe data:

  * the owned normalized MARKET_BAR OHLCV panel keyed on the stable Norgate
    ``assetid`` (``historical_price_panel`` conventions - full owned history, no
    reader truncation, ticker text is NEVER the sole key);
  * the owned leakage-safe point-in-time SEC fundamentals
    (``pit_fundamentals.PitFundamentalsStore``); and
  * the owned leakage-safe point-in-time SIC research-sector series
    (``pit_sector.PitSicSeries``) for sector-relative / sector-neutral variants.

Every signal is one immutable ``SignalSpec`` carrying its family, required owned
fields, pre-registered direction, lookback, lag, rebalance, winsorization,
neutralization, minimum coverage, cost scenario and the panel / fundamental /
identity epochs. The spec id is a content hash over ONLY the semantically-
identifying fields, so two specifications that are actually the same collapse to
one candidate (no silent default makes two identical tests look distinct) and the
multiple-testing family size counts DISTINCT tests honestly.

A signal is IMPLEMENTED only when the owned fields it needs genuinely exist;
otherwise it is an explicit ``DATA_HOLD`` catalogue entry (never an approximation
of a field the project does not own - e.g. a market-cap valuation ratio when no
shares-outstanding concept is owned).

Pure Python stdlib; deterministic; read-only. Never writes an operational ledger,
order, fill, signal, trade decision, model or price row.
"""
from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Optional

from . import fundamental_signals as _fsig
from . import pit_fundamentals as _pfd

RT_MARKET_BAR = "MARKET_BAR"
DEFAULT_SOURCES = ("norgate_local",)

# The families the catalogue spans (>=6 required; 8 present, valuation honestly
# DATA_HOLD because no owned shares-outstanding / market-cap concept exists).
FAMILIES = (
    "price_momentum", "reversal_path", "risk_volatility", "liquidity_volume",
    "profitability_quality", "growth_investment", "valuation", "cross_family",
)

# Neutralization modes. ``sector`` demeans the raw factor within the leakage-safe
# PIT sector as-of the formation date (never the current-only GICS surface).
NEUTRAL_NONE = "none"
NEUTRAL_SECTOR = "sector"

# Owned PIT concept availability is checked against the store's present concepts.
_OWNED_ALWAYS = ("assets", "revenue", "cost_of_revenue", "gross_profit",
                 "liabilities", "stockholders_equity")


# --------------------------------------------------------------------------- #
# Owned OHLCV panel (assetid-keyed, full owned history).
# --------------------------------------------------------------------------- #
def iter_owned_ohlcv(ingestion_root, *, sources: Iterable[str] = DEFAULT_SOURCES,
                     date_start: Optional[str] = None,
                     date_end: Optional[str] = None,
                     max_files: Optional[int] = None):
    """Yield ``(assetid, date, open, high, low, close, volume, dollar_volume)``
    for every owned normalized MARKET_BAR record from ``sources`` in the window.
    ``assetid`` is the record's stable ``security_id``; records without one are
    skipped. Uses the adjusted total-return ``Close`` (consistent with the
    evaluator); dollar volume prefers the owned Norgate ``Turnover`` field, else
    ``Close * Volume``. Streaming, deterministic file order, no truncation."""
    base = Path(ingestion_root) / "normalized" / RT_MARKET_BAR
    if not base.exists():
        return
    allow = set(sources)
    files = sorted(base.rglob("*.jsonl"))
    if max_files is not None:
        files = files[:int(max_files)]
    for f in files:
        try:
            text = f.read_text(encoding="utf-8-sig")
        except OSError:
            continue
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            if allow and (rec.get("source_id") not in allow):
                continue
            assetid = rec.get("security_id")
            if not assetid:
                continue
            pay = rec.get("normalized_payload") or {}
            date = rec.get("effective_at") or pay.get("Date")
            close = pay.get("Close", pay.get("close"))
            if not date or close is None:
                continue
            date = str(date)[:10]
            if date_start and date < date_start:
                continue
            if date_end and date > date_end:
                continue
            try:
                c = float(close)
            except (TypeError, ValueError):
                continue
            o = _f(pay.get("Open"))
            h = _f(pay.get("High"))
            lo = _f(pay.get("Low"))
            v = _f(pay.get("Volume"))
            dv = _f(pay.get("Turnover"))
            if dv is None and v is not None:
                dv = c * v
            yield str(assetid), date, o, h, lo, c, v, dv


def _f(x):
    try:
        return None if x is None else float(x)
    except (TypeError, ValueError):
        return None


def build_ohlcv_panel(ingestion_root, *, sources: Iterable[str] = DEFAULT_SOURCES,
                      date_start: Optional[str] = None,
                      date_end: Optional[str] = None,
                      max_files: Optional[int] = None) -> dict:
    """The owned survivorship-safe OHLCV panel keyed by Norgate assetid:
    ``{assetid: {"d":[dates], "o":[], "h":[], "l":[], "c":[closes], "v":[vol],
    "dv":[dollar_vol]}}`` sorted ascending and de-duplicated by date (last write
    wins per date). The canonical multi-field reader for Stage 11 price signals."""
    raw: dict = {}
    for aid, d, o, h, lo, c, v, dv in iter_owned_ohlcv(
            ingestion_root, sources=sources, date_start=date_start,
            date_end=date_end, max_files=max_files):
        raw.setdefault(aid, {})[d] = (o, h, lo, c, v, dv)
    panel: dict = {}
    for aid, bydate in raw.items():
        dates = sorted(bydate)
        panel[aid] = {
            "d": dates,
            "o": [bydate[d][0] for d in dates],
            "h": [bydate[d][1] for d in dates],
            "l": [bydate[d][2] for d in dates],
            "c": [bydate[d][3] for d in dates],
            "v": [bydate[d][4] for d in dates],
            "dv": [bydate[d][5] for d in dates],
        }
    return panel


def close_panel(ohlcv: dict) -> dict:
    """Project an OHLCV panel down to the ``{assetid: [(date, close)]}`` form the
    evaluator's forward-return machinery consumes."""
    return {a: list(zip(s["d"], s["c"])) for a, s in ohlcv.items()}


# The signal math consumes only close (returns), volume and dollar volume; the
# on-disk cache stores just those three plus dates to keep it compact and fast to
# reload (a full owned-history OHLCV rebuild scans ~17.5k partition files).
_CACHE_FIELDS = ("c", "v", "dv")


def cache_ohlcv_panel(ohlcv: dict, cache_dir, epoch: str) -> str:
    """Persist a compact deterministic JSONL cache of the panel keyed by the owned
    price ``epoch`` (one line per assetid). Returns the cache path. Append-safe:
    written atomically to a temp file then replaced; never mutates the source
    tree."""
    d = Path(cache_dir)
    d.mkdir(parents=True, exist_ok=True)
    path = d / ("ohlcv_%s.jsonl" % epoch)
    tmp = path.with_suffix(".jsonl.tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        for aid in sorted(ohlcv):
            s = ohlcv[aid]
            rec = {"a": aid, "d": s["d"]}
            for k in _CACHE_FIELDS:
                rec[k] = s.get(k)
            fh.write(json.dumps(rec, separators=(",", ":")) + "\n")
    tmp.replace(path)
    return str(path)


def load_cached_ohlcv_panel(cache_dir, epoch: str) -> Optional[dict]:
    """Load the compact panel cache for ``epoch`` (``None`` if absent). The
    reconstructed panel carries empty ``o``/``h``/``l`` lists (unused by the
    signal math) so its shape matches :func:`build_ohlcv_panel`."""
    path = Path(cache_dir) / ("ohlcv_%s.jsonl" % epoch)
    if not path.exists():
        return None
    panel: dict = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        rec = json.loads(line)
        n = len(rec.get("d") or [])
        panel[rec["a"]] = {"d": rec["d"], "c": rec.get("c") or [],
                           "v": rec.get("v") or [], "dv": rec.get("dv") or [],
                           "o": [None] * n, "h": [None] * n, "l": [None] * n}
    return panel


def load_or_build_ohlcv_panel(ingestion_root, cache_dir, epoch: str, *,
                              sources: Iterable[str] = DEFAULT_SOURCES) -> dict:
    """Load the epoch-keyed panel cache if present, else build from the owned
    MARKET_BAR tree and cache it. The cache is invalidated automatically when the
    owned-price epoch changes (a repaired/extended panel yields a new epoch)."""
    cached = load_cached_ohlcv_panel(cache_dir, epoch)
    if cached is not None:
        return cached
    panel = build_ohlcv_panel(ingestion_root, sources=sources)
    try:
        cache_ohlcv_panel(panel, cache_dir, epoch)
    except OSError:
        pass
    return panel


def market_daily_returns(ohlcv: dict) -> dict:
    """Equal-weight market daily simple-return series ``{date: mean_ret}`` over
    every name present on consecutive days. Used to residualize name returns
    (idiosyncratic volatility / downside beta / residual momentum)."""
    acc: dict = {}
    cnt: dict = {}
    for s in ohlcv.values():
        d, c = s["d"], s["c"]
        for i in range(1, len(c)):
            if c[i - 1] and c[i - 1] > 0 and c[i] is not None:
                r = c[i] / c[i - 1] - 1.0
                acc[d[i]] = acc.get(d[i], 0.0) + r
                cnt[d[i]] = cnt.get(d[i], 0) + 1
    return {d: acc[d] / cnt[d] for d in acc if cnt[d]}


# --------------------------------------------------------------------------- #
# Small deterministic series helpers (index-safe, no look-ahead).
# --------------------------------------------------------------------------- #
def _idx_asof(dates: list, as_of: str) -> Optional[int]:
    for k in range(len(dates) - 1, -1, -1):
        if dates[k] <= as_of:
            return k
        # dates sorted ascending; the first <= from the end is the formation bar.
    return None


def _ret(c: list, i: int, lb: int, skip: int = 0) -> Optional[float]:
    a = i - skip
    b = i - lb
    if b < 0 or a < 0 or a >= len(c):
        return None
    if c[b] is None or c[a] is None or c[b] <= 0:
        return None
    return c[a] / c[b] - 1.0


def _daily_rets(c: list, i: int, lb: int) -> list:
    out = []
    lo = max(1, i - lb + 1)
    for k in range(lo, i + 1):
        if c[k - 1] and c[k - 1] > 0 and c[k] is not None:
            out.append(c[k] / c[k - 1] - 1.0)
    return out


def _mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def _std(xs):
    xs = [x for x in xs if x is not None]
    n = len(xs)
    if n < 2:
        return None
    m = sum(xs) / n
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1))


# --------------------------------------------------------------------------- #
# Primitive price/volume factor math (each returns a raw value or None).
# --------------------------------------------------------------------------- #
def _price_value(prim: str, params: dict, s: dict, i: int,
                 mkt: Optional[dict]) -> Optional[float]:
    c, v, dv, h, l = s["c"], s["v"], s["dv"], s["h"], s["l"]
    lb = int(params.get("lookback", 126))
    if prim == "momentum":
        return _ret(c, i, lb, skip=int(params.get("skip", 0)))
    if prim == "acceleration":
        r1 = _ret(c, i, lb)
        r2 = _ret(c, i - lb, lb)
        return None if r1 is None or r2 is None else r1 - r2
    if prim == "reversal":
        r = _ret(c, i, lb, skip=int(params.get("skip", 0)))
        return None if r is None else -r
    if prim == "dist_from_high":
        window = c[max(0, i - lb + 1):i + 1]
        window = [x for x in window if x is not None]
        if not window or c[i] is None:
            return None
        hi = max(window)
        return None if hi <= 0 else c[i] / hi - 1.0
    if prim == "drawdown_recovery":
        window = [x for x in c[max(0, i - lb + 1):i + 1] if x is not None]
        if len(window) < 3 or c[i] is None:
            return None
        lo_, hi_ = min(window), max(window)
        return None if hi_ <= lo_ else (c[i] - lo_) / (hi_ - lo_)
    if prim == "path_consistency":
        rs = _daily_rets(c, i, lb)
        if len(rs) < 10:
            return None
        return sum(1 for r in rs if r > 0) / len(rs)
    if prim == "realized_vol":
        rs = _daily_rets(c, i, lb)
        return None if len(rs) < 10 else -(_std(rs) or 0.0)
    if prim == "downside_vol":
        rs = _daily_rets(c, i, lb)
        neg = [r for r in rs if r < 0]
        if len(rs) < 10 or len(neg) < 3:
            return None
        return -math.sqrt(sum(r * r for r in neg) / len(neg))
    if prim == "max_drawdown":
        window = [x for x in c[max(0, i - lb + 1):i + 1] if x is not None]
        if len(window) < 10:
            return None
        peak = window[0]
        worst = 0.0
        for x in window:
            peak = max(peak, x)
            if peak > 0:
                worst = min(worst, x / peak - 1.0)
        return worst  # <= 0; less negative is better (sign +1)
    if prim in ("idio_vol", "downside_beta"):
        return _residual_stat(prim, s, i, lb, mkt)
    if prim == "amihud_illiquidity":
        rs = _daily_rets(c, i, lb)
        dvs = dv[max(1, i - lb + 1):i + 1]
        pairs = [(abs(r), d) for r, d in zip(rs, dvs) if d and d > 0]
        if len(pairs) < 10:
            return None
        return _mean([a / d for a, d in pairs])
    if prim == "dollar_volume":
        window = [x for x in dv[max(0, i - lb + 1):i + 1] if x and x > 0]
        if len(window) < 10:
            return None
        return -math.log(_mean(window))       # small = illiquidity premium
    if prim == "volume_accel":
        recent = [x for x in v[max(0, i - 20):i + 1] if x is not None]
        prior = [x for x in v[max(0, i - lb):max(0, i - 20)] if x is not None]
        if len(recent) < 5 or len(prior) < 10 or not _mean(prior):
            return None
        pm = _mean(prior)
        return None if not pm else _mean(recent) / pm - 1.0
    if prim == "abnormal_volume":
        base = [x for x in v[max(0, i - lb + 1):i + 1] if x is not None]
        if len(base) < 10 or v[i] is None or not _mean(base):
            return None
        return v[i] / _mean(base) - 1.0
    if prim == "liquidity_stability":
        window = [x for x in dv[max(0, i - lb + 1):i + 1] if x and x > 0]
        if len(window) < 10:
            return None
        m, sd = _mean(window), _std(window)
        if not m or sd is None:
            return None
        return -(sd / m)                       # stable dollar volume = higher
    return None


def _residual_stat(prim: str, s: dict, i: int, lb: int,
                   mkt: Optional[dict]) -> Optional[float]:
    """Idiosyncratic volatility (residual std) / downside beta of a name vs the
    equal-weight market, over ``lb`` days ending at ``i``. Deterministic OLS."""
    if not mkt:
        return None
    d, c = s["d"], s["c"]
    xs, ys = [], []
    lo = max(1, i - lb + 1)
    for k in range(lo, i + 1):
        if c[k - 1] and c[k - 1] > 0 and c[k] is not None and d[k] in mkt:
            ys.append(c[k] / c[k - 1] - 1.0)
            xs.append(mkt[d[k]])
    n = len(xs)
    if n < 20:
        return None
    mx, my = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    if sxx <= 0:
        return None
    beta = sum((xs[j] - mx) * (ys[j] - my) for j in range(n)) / sxx
    if prim == "downside_beta":
        dn = [(xs[j], ys[j]) for j in range(n) if xs[j] < 0]
        if len(dn) < 8:
            return None
        dmx = sum(x for x, _ in dn) / len(dn)
        dmy = sum(y for _, y in dn) / len(dn)
        dsxx = sum((x - dmx) ** 2 for x, _ in dn)
        if dsxx <= 0:
            return None
        dbeta = sum((x - dmx) * (y - dmy) for x, y in dn) / dsxx
        return -dbeta                          # low downside beta = defensive
    alpha = my - beta * mx
    resid = [ys[j] - (alpha + beta * xs[j]) for j in range(n)]
    return -(_std(resid) or 0.0)               # low idio vol = higher expected


# --------------------------------------------------------------------------- #
# Fundamental factor math (owned PIT concepts only).
# --------------------------------------------------------------------------- #
# Extra fundamental signals beyond the 3 owned MVP ones, each expressed over the
# owned concept vocabulary; their required concepts gate real support.
_FUND_EXTRA = {
    "operating_profitability": (["operating_income", "assets"], 1),
    "return_on_assets": (["net_income", "assets"], 1),
    "gross_margin": (["gross_profit", "revenue"], 1),
    "net_margin": (["net_income", "revenue"], 1),
    "leverage": (["liabilities", "assets"], -1),
    "cash_ratio": (["cash", "assets"], 1),
    "revenue_growth": (["revenue"], 1),
    "earnings_growth": (["net_income"], 1),
    "gross_profit_growth": (["gross_profit"], 1),
    "equity_issuance": (["stockholders_equity"], -1),
}


def _fval(store, cik, concept, fk, as_of):
    obs = store.as_of(cik, concept, fk, as_of)
    if obs is None:
        return None
    try:
        return float(obs.value)
    except (TypeError, ValueError):
        return None


def _fund_value(prim: str, store, cik: str, as_of: str) -> Optional[float]:
    """One fundamental signal value for ``cik`` as-of ``as_of`` (leakage-safe).
    Owned MVP signals delegate to ``fundamental_signals``; extra ratios/growth
    read owned concepts directly for the latest available fiscal period."""
    if prim in _fsig.SIGNALS:
        fk = store.latest_fiscal_key(cik, as_of, concept="assets")
        if fk is None:
            return None
        prior = (store.prior_fiscal_key(cik, as_of, fk, concept="assets")
                 if prim == "asset_growth" else None)
        if prim == "asset_growth" and prior is None:
            return None
        return _fsig.compute_signal(store, prim, cik=cik, fiscal_key=fk,
                                    as_of=as_of, prior_fiscal_key=prior).get("value")
    spec = _FUND_EXTRA.get(prim)
    if spec is None:
        return None
    concepts, _sign = spec
    anchor = concepts[-1] if concepts[-1] == "assets" else concepts[0]
    fk = store.latest_fiscal_key(cik, as_of, concept=anchor)
    if fk is None:
        return None
    if prim == "operating_profitability":
        a, oi = _fval(store, cik, "assets", fk, as_of), \
            _fval(store, cik, "operating_income", fk, as_of)
        return None if not a or a <= 0 or oi is None else oi / a
    if prim == "return_on_assets":
        a, ni = _fval(store, cik, "assets", fk, as_of), \
            _fval(store, cik, "net_income", fk, as_of)
        return None if not a or a <= 0 or ni is None else ni / a
    if prim == "gross_margin":
        rev, gp = _fval(store, cik, "revenue", fk, as_of), \
            _fval(store, cik, "gross_profit", fk, as_of)
        if rev and rev > 0 and gp is not None:
            return gp / rev
        cost = _fval(store, cik, "cost_of_revenue", fk, as_of)
        return None if not rev or rev <= 0 or cost is None else (rev - cost) / rev
    if prim == "net_margin":
        rev, ni = _fval(store, cik, "revenue", fk, as_of), \
            _fval(store, cik, "net_income", fk, as_of)
        return None if not rev or rev <= 0 or ni is None else ni / rev
    if prim == "leverage":
        a, li = _fval(store, cik, "assets", fk, as_of), \
            _fval(store, cik, "liabilities", fk, as_of)
        return None if not a or a <= 0 or li is None else li / a
    if prim == "cash_ratio":
        a, ca = _fval(store, cik, "assets", fk, as_of), \
            _fval(store, cik, "cash", fk, as_of)
        return None if not a or a <= 0 or ca is None else ca / a
    if prim in ("revenue_growth", "earnings_growth", "gross_profit_growth",
                "equity_issuance"):
        concept = {"revenue_growth": "revenue", "earnings_growth": "net_income",
                   "gross_profit_growth": "gross_profit",
                   "equity_issuance": "stockholders_equity"}[prim]
        cfk = store.latest_fiscal_key(cik, as_of, concept=concept)
        if cfk is None:
            return None
        pfk = store.prior_fiscal_key(cik, as_of, cfk, concept=concept)
        if pfk is None:
            return None
        cur, prev = _fval(store, cik, concept, cfk, as_of), \
            _fval(store, cik, concept, pfk, as_of)
        if cur is None or prev is None or prev == 0:
            return None
        return (cur - prev) / abs(prev)
    return None


# --------------------------------------------------------------------------- #
# SignalSpec.
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SignalSpec:
    name: str                       # human label
    family: str
    kind: str                       # price | fundamental | combo
    primitive: str                  # the compute primitive (or combo tag)
    direction: int                  # pre-registered expected sign (+1 / -1)
    lookback: int = 126
    lag: int = 1                    # formation-to-first-forward gap (days)
    skip: int = 0                   # momentum skip (e.g. 21 = ex last month)
    horizon_days: int = 63
    rebalance: str = "quarterly"
    winsor: float = 0.02
    neutralization: str = NEUTRAL_NONE
    min_coverage: int = 20
    cost_bps: int = 25
    requires: tuple = ()            # required owned concepts / fields
    components: tuple = ()          # for combos: contributing primitive names
    panel_epoch: str = ""
    fundamental_epoch: str = ""
    identity_epoch: str = ""
    params: tuple = ()              # extra (k, v) identity pairs (sorted)

    def identity(self) -> dict:
        """The semantically-identifying fields (NOT the display metadata) hashed
        into the immutable spec id; two specs with the same identity ARE the same
        test and collapse to one candidate."""
        return {
            "kind": self.kind, "primitive": self.primitive,
            "family": self.family, "direction": int(self.direction),
            "lookback": int(self.lookback), "lag": int(self.lag),
            "skip": int(self.skip), "horizon_days": int(self.horizon_days),
            "rebalance": self.rebalance, "winsor": round(float(self.winsor), 6),
            "neutralization": self.neutralization,
            "min_coverage": int(self.min_coverage),
            "components": sorted(self.components),
            "panel_epoch": self.panel_epoch,
            "fundamental_epoch": self.fundamental_epoch,
            "identity_epoch": self.identity_epoch,
            "params": sorted([list(p) for p in self.params]),
        }

    @property
    def spec_id(self) -> str:
        payload = json.dumps(self.identity(), sort_keys=True,
                             separators=(",", ":"))
        return "s11_" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

    def to_dict(self) -> dict:
        d = dict(self.identity())
        d.update({"spec_id": self.spec_id, "name": self.name,
                  "cost_bps": int(self.cost_bps),
                  "requires": list(self.requires)})
        return d


# --------------------------------------------------------------------------- #
# Cross-section builders (all keyed by assetid, consistent with the price panel).
# --------------------------------------------------------------------------- #
def price_cross_section(spec: SignalSpec, ohlcv: dict, market: Optional[dict],
                        as_of: str) -> dict:
    out: dict = {}
    params = {"lookback": spec.lookback, "skip": spec.skip}
    for aid, s in ohlcv.items():
        i = _idx_asof(s["d"], as_of)
        if i is None:
            continue
        val = _price_value(spec.primitive, params, s, i, market)
        if val is not None:
            out[aid] = val
    return out


def fundamental_cross_section(spec: SignalSpec, store, cik_to_assetid: dict,
                              as_of: str) -> dict:
    out: dict = {}
    if store is None:
        return out
    for cik, aid in cik_to_assetid.items():
        val = _fund_value(spec.primitive, store, cik, as_of)
        if val is not None:
            out[aid] = val
    return out


def _zscore(values: dict) -> dict:
    xs = list(values.values())
    m = _mean(xs)
    sd = _std(xs)
    if m is None or not sd:
        return {}
    return {k: (v - m) / sd for k, v in values.items()}


# Combo definitions: (leg primitive, leg kind, leg sign, leg lookback).
_COMBOS = {
    "quality_momentum": [("gross_profitability", "fundamental", 1, 0),
                         ("momentum", "price", 1, 126)],
    "profitability_low_investment": [("gross_profitability", "fundamental", 1, 0),
                                     ("asset_growth", "fundamental", -1, 0)],
    "defensive_quality": [("gross_profitability", "fundamental", 1, 0),
                          ("realized_vol", "price", 1, 126)],
    "balance_sheet_momentum": [("balance_sheet_quality", "fundamental", 1, 0),
                               ("momentum", "price", 1, 126)],
    "fundamental_price_momentum": [("revenue_growth", "fundamental", 1, 0),
                                   ("momentum", "price", 1, 126)],
}


def combo_cross_section(spec: SignalSpec, ohlcv: dict, market: Optional[dict],
                        store, cik_to_assetid: dict, as_of: str) -> dict:
    legs = _COMBOS.get(spec.primitive)
    if not legs:
        return {}
    zsum: dict = {}
    counts: dict = {}
    for prim, kind, sign, lb in legs:
        leg_spec = SignalSpec(name=prim, family="cross_family", kind=kind,
                              primitive=prim, direction=sign, lookback=lb or 126)
        if kind == "price":
            raw = price_cross_section(leg_spec, ohlcv, market, as_of)
        else:
            raw = fundamental_cross_section(leg_spec, store, cik_to_assetid,
                                            as_of)
        raw = {k: v * sign for k, v in raw.items()}
        z = _zscore(raw)
        for k, val in z.items():
            zsum[k] = zsum.get(k, 0.0) + val
            counts[k] = counts.get(k, 0) + 1
    n_legs = len(legs)
    # A combo value exists only for names present in EVERY leg (no partial name).
    return {k: zsum[k] for k in zsum if counts.get(k) == n_legs}


def signal_cross_section(spec: SignalSpec, *, ohlcv: dict,
                         market: Optional[dict] = None, store=None,
                         cik_to_assetid: Optional[dict] = None,
                         sector_series=None, as_of: str) -> dict:
    """The raw ``{assetid: value}`` cross-section for ``spec`` on ``as_of``
    (before the expected-sign multiply). Optionally sector-neutralized: the raw
    factor is demeaned within the leakage-safe PIT sector as-of the formation
    date; names with no contemporaneous sector classification are dropped
    (missing sector is never guessed)."""
    c2a = cik_to_assetid or {}
    if spec.kind == "price":
        vals = price_cross_section(spec, ohlcv, market, as_of)
    elif spec.kind == "fundamental":
        vals = fundamental_cross_section(spec, store, c2a, as_of)
    elif spec.kind == "combo":
        vals = combo_cross_section(spec, ohlcv, market, store, c2a, as_of)
    else:
        vals = {}
    if spec.neutralization == NEUTRAL_SECTOR and sector_series is not None:
        vals = _sector_demean(vals, c2a, sector_series, as_of)
    return vals


def _sector_demean(vals: dict, cik_to_assetid: dict, sector_series,
                   as_of: str) -> dict:
    """Demean the factor within the leakage-safe PIT sector as-of ``as_of``. The
    series is keyed by CIK; invert the CIK->assetid map to look up each name's
    sector. Names with an Unknown/absent sector are DROPPED (never bucketed into a
    guessed sector)."""
    a2c = {a: c for c, a in cik_to_assetid.items()}
    buckets: dict = {}
    sect_of: dict = {}
    for aid, v in vals.items():
        cik = a2c.get(aid)
        if cik is None:
            continue
        sec = sector_series.sector_as_of(cik, as_of)
        if not sec or sec == "Unknown":
            continue
        sect_of[aid] = sec
        buckets.setdefault(sec, []).append(v)
    means = {s: (sum(xs) / len(xs)) for s, xs in buckets.items() if xs}
    return {aid: vals[aid] - means[sect_of[aid]]
            for aid in sect_of if sect_of[aid] in means}


# --------------------------------------------------------------------------- #
# The primitive catalogue.
# --------------------------------------------------------------------------- #
def _p(name, family, kind, primitive, direction, **kw):
    return {"name": name, "family": family, "kind": kind,
            "primitive": primitive, "direction": direction, **kw}


# Every implemented primitive; ``requires`` lists the owned concepts/fields that
# must genuinely exist for the signal to be supported (else it is DATA_HOLD).
PRIMITIVE_DEFS = [
    # 1. price momentum -------------------------------------------------------
    _p("momentum_1m", "price_momentum", "price", "momentum", 1, lookback=21),
    _p("momentum_3m", "price_momentum", "price", "momentum", 1, lookback=63),
    _p("momentum_6m", "price_momentum", "price", "momentum", 1, lookback=126),
    _p("momentum_12m", "price_momentum", "price", "momentum", 1, lookback=252),
    _p("momentum_12_1", "price_momentum", "price", "momentum", 1,
       lookback=252, skip=21),
    _p("momentum_accel", "price_momentum", "price", "acceleration", 1,
       lookback=63),
    _p("momentum_6m_sector", "price_momentum", "price", "momentum", 1,
       lookback=126, neutralization=NEUTRAL_SECTOR, requires=("sector",)),
    _p("momentum_12_1_sector", "price_momentum", "price", "momentum", 1,
       lookback=252, skip=21, neutralization=NEUTRAL_SECTOR,
       requires=("sector",)),
    # 2. reversal / path ------------------------------------------------------
    _p("reversal_1w", "reversal_path", "price", "reversal", 1, lookback=5),
    _p("reversal_1m", "reversal_path", "price", "reversal", 1, lookback=21),
    _p("dist_from_52w_high", "reversal_path", "price", "dist_from_high", 1,
       lookback=252),
    _p("drawdown_recovery", "reversal_path", "price", "drawdown_recovery", 1,
       lookback=126),
    _p("path_consistency", "reversal_path", "price", "path_consistency", 1,
       lookback=126),
    # 3. risk / volatility ----------------------------------------------------
    _p("realized_vol", "risk_volatility", "price", "realized_vol", 1,
       lookback=126),
    _p("downside_vol", "risk_volatility", "price", "downside_vol", 1,
       lookback=126),
    _p("max_drawdown_12m", "risk_volatility", "price", "max_drawdown", 1,
       lookback=252),
    _p("idiosyncratic_vol", "risk_volatility", "price", "idio_vol", 1,
       lookback=126),
    _p("downside_beta", "risk_volatility", "price", "downside_beta", 1,
       lookback=252),
    # 4. liquidity / volume ---------------------------------------------------
    _p("amihud_illiquidity", "liquidity_volume", "price", "amihud_illiquidity",
       1, lookback=63, requires=("volume",)),
    _p("dollar_volume", "liquidity_volume", "price", "dollar_volume", 1,
       lookback=63, requires=("volume",)),
    _p("volume_accel", "liquidity_volume", "price", "volume_accel", 1,
       lookback=63, requires=("volume",)),
    _p("abnormal_volume", "liquidity_volume", "price", "abnormal_volume", -1,
       lookback=63, requires=("volume",)),
    _p("liquidity_stability", "liquidity_volume", "price", "liquidity_stability",
       1, lookback=126, requires=("volume",)),
    # 5. profitability / quality ---------------------------------------------
    _p("gross_profitability", "profitability_quality", "fundamental",
       "gross_profitability", 1, requires=("revenue", "cost_of_revenue",
                                           "assets")),
    _p("operating_profitability", "profitability_quality", "fundamental",
       "operating_profitability", 1, requires=("operating_income", "assets")),
    _p("return_on_assets", "profitability_quality", "fundamental",
       "return_on_assets", 1, requires=("net_income", "assets")),
    _p("gross_margin", "profitability_quality", "fundamental", "gross_margin", 1,
       requires=("gross_profit", "revenue")),
    _p("net_margin", "profitability_quality", "fundamental", "net_margin", 1,
       requires=("net_income", "revenue")),
    _p("balance_sheet_quality", "profitability_quality", "fundamental",
       "balance_sheet_quality", 1, requires=("assets", "liabilities",
                                             "stockholders_equity")),
    _p("leverage", "profitability_quality", "fundamental", "leverage", -1,
       requires=("liabilities", "assets")),
    _p("cash_ratio", "profitability_quality", "fundamental", "cash_ratio", 1,
       requires=("cash", "assets")),
    # 6. growth / investment --------------------------------------------------
    _p("asset_growth", "growth_investment", "fundamental", "asset_growth", -1,
       requires=("assets",)),
    _p("revenue_growth", "growth_investment", "fundamental", "revenue_growth", 1,
       requires=("revenue",)),
    _p("earnings_growth", "growth_investment", "fundamental", "earnings_growth",
       1, requires=("net_income",)),
    _p("gross_profit_growth", "growth_investment", "fundamental",
       "gross_profit_growth", 1, requires=("gross_profit",)),
    _p("equity_issuance", "growth_investment", "fundamental", "equity_issuance",
       -1, requires=("stockholders_equity",)),
    # 7. valuation (owned data does NOT include shares/market-cap -> DATA_HOLD)
    _p("earnings_yield", "valuation", "fundamental", "earnings_yield", 1,
       requires=("net_income", "market_cap")),
    _p("book_to_market", "valuation", "fundamental", "book_to_market", 1,
       requires=("stockholders_equity", "market_cap")),
    _p("sales_to_price", "valuation", "fundamental", "sales_to_price", 1,
       requires=("revenue", "market_cap")),
    # 8. cross-family combinations -------------------------------------------
    _p("quality_momentum", "cross_family", "combo", "quality_momentum", 1,
       requires=("revenue", "cost_of_revenue", "assets"),
       components=("gross_profitability", "momentum_6m")),
    _p("profitability_low_investment", "cross_family", "combo",
       "profitability_low_investment", 1,
       requires=("revenue", "cost_of_revenue", "assets"),
       components=("gross_profitability", "asset_growth")),
    _p("defensive_quality", "cross_family", "combo", "defensive_quality", 1,
       requires=("revenue", "cost_of_revenue", "assets"),
       components=("gross_profitability", "realized_vol")),
    _p("balance_sheet_momentum", "cross_family", "combo", "balance_sheet_momentum",
       1, requires=("assets", "liabilities", "stockholders_equity"),
       components=("balance_sheet_quality", "momentum_6m")),
    _p("fundamental_price_momentum", "cross_family", "combo",
       "fundamental_price_momentum", 1, requires=("revenue",),
       components=("revenue_growth", "momentum_6m")),
]

PRIMITIVE_COUNT = len(PRIMITIVE_DEFS)


def primitive_families() -> dict:
    fam: dict = {}
    for d in PRIMITIVE_DEFS:
        fam.setdefault(d["family"], []).append(d["name"])
    return fam


# --------------------------------------------------------------------------- #
# Support classification (implemented vs DATA_HOLD).
# --------------------------------------------------------------------------- #
def concept_availability(store) -> set:
    return set(store.concepts_present()) if store is not None else set()


def spec_supported(defn: dict, *, owned_concepts: set,
                   sector_available: bool, volume_available: bool) -> tuple:
    """Whether a primitive is genuinely supported by owned fields. Returns
    ``(supported: bool, reason)``. Unsupported -> an explicit DATA_HOLD catalogue
    entry (never an approximation)."""
    for req in defn.get("requires", ()):
        if req == "sector":
            if not sector_available:
                return False, "DATA_HOLD_NO_PIT_SECTOR"
        elif req == "volume":
            if not volume_available:
                return False, "DATA_HOLD_NO_VOLUME"
        elif req == "market_cap":
            return False, "DATA_HOLD_NO_MARKET_CAP_NO_SHARES_OUTSTANDING"
        elif req not in owned_concepts:
            return False, "DATA_HOLD_MISSING_CONCEPT:%s" % req
    return True, "SUPPORTED"


def build_catalogue(*, panel_epoch: str = "", fundamental_epoch: str = "",
                    identity_epoch: str = "",
                    horizons: Iterable[int] = (63,),
                    winsors: Iterable[float] = (0.02,),
                    include_sector_variants: bool = True) -> list:
    """Deterministically expand the primitive catalogue into a deduplicated list
    of ``SignalSpec`` across the parameter grid (horizons x winsorization x
    optional sector-neutral variants). Specs that collapse to the same immutable
    identity are emitted once. Returns the full spec list (implemented AND the
    valuation/sector/volume DATA_HOLD entries) - support is decided separately so
    the catalogue is a complete, honest inventory."""
    specs: dict = {}
    for defn in PRIMITIVE_DEFS:
        base_neu = defn.get("neutralization", NEUTRAL_NONE)
        neus = [base_neu]
        if (include_sector_variants and defn["kind"] == "price"
                and base_neu == NEUTRAL_NONE
                and defn["family"] in ("price_momentum", "reversal_path")):
            neus.append(NEUTRAL_SECTOR)
        for hz in horizons:
            for wz in winsors:
                for neu in neus:
                    req = tuple(defn.get("requires", ()))
                    if neu == NEUTRAL_SECTOR and "sector" not in req:
                        req = req + ("sector",)
                    spec = SignalSpec(
                        name=defn["name"] + ("_sn" if neu == NEUTRAL_SECTOR
                                             and base_neu == NEUTRAL_NONE else ""),
                        family=defn["family"], kind=defn["kind"],
                        primitive=defn["primitive"],
                        direction=int(defn["direction"]),
                        lookback=int(defn.get("lookback", 126)),
                        lag=int(defn.get("lag", 1)),
                        skip=int(defn.get("skip", 0)),
                        horizon_days=int(hz), winsor=float(wz),
                        neutralization=neu,
                        min_coverage=int(defn.get("min_coverage", 20)),
                        requires=req,
                        components=tuple(defn.get("components", ())),
                        panel_epoch=panel_epoch,
                        fundamental_epoch=fundamental_epoch,
                        identity_epoch=identity_epoch)
                    specs[spec.spec_id] = spec
    return sorted(specs.values(), key=lambda s: s.spec_id)


def winsorize(values: dict, winsor: float) -> dict:
    """Symmetric quantile clipping of the cross-section (deterministic)."""
    if not values or winsor <= 0:
        return dict(values)
    xs = sorted(values.values())
    n = len(xs)
    k = int(n * float(winsor))
    if k <= 0 or 2 * k >= n:
        return dict(values)
    lo, hi = xs[k], xs[n - 1 - k]
    return {key: (lo if v < lo else hi if v > hi else v)
            for key, v in values.items()}


__all__ = [
    "RT_MARKET_BAR", "DEFAULT_SOURCES", "FAMILIES", "NEUTRAL_NONE",
    "NEUTRAL_SECTOR", "iter_owned_ohlcv", "build_ohlcv_panel", "close_panel",
    "market_daily_returns", "SignalSpec", "signal_cross_section",
    "price_cross_section", "fundamental_cross_section", "combo_cross_section",
    "PRIMITIVE_DEFS", "PRIMITIVE_COUNT", "primitive_families",
    "concept_availability", "spec_supported", "build_catalogue", "winsorize",
    "cache_ohlcv_panel", "load_cached_ohlcv_panel", "load_or_build_ohlcv_panel",
]
