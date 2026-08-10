#!/usr/bin/env python
r"""Stage 13C — FROZEN out-of-sample Sales-Surprise confirmation test.

Evaluates the Stage 13B PROMISING_EXPLORATORY sales-surprise 63-session drift
on STRICTLY PRE-DISCOVERY history acquired from the same provider entitlement,
under the pre-registered contract frozen (and SHA256-stamped) BEFORE the first
older-history request was sent:

  D:\Temp\paper_trader_stage13c_sales_analyst_confirmation\
      SALES_SURPRISE_CONFIRMATION_CONTRACT.md

Everything scientific here is FROZEN — the module constants below restate the
contract and are covered by tests so they cannot drift silently:

  * signal          = provider ``sales_percent_diff`` exactly as delivered;
  * universe        = survivorship-safe historical S&P 500 membership on the
                      event date (``IdentityStore.historical_universe_on``;
                      effective-dated tickers; delisted members retained);
  * formation       = provider ``actual_reported_date`` (uniform BMO/AMC);
  * execution       = close of the FIRST owned eligible session STRICTLY AFTER
                      the report date (owned Norgate MARKET_BAR total-return);
  * horizons        = 63 eligible sessions PRIMARY; 5/20 secondary (reported,
                      never substituted; an unmatured horizon is EXCLUDED);
  * statistic       = quintiles WITHIN monthly report cohort (>=25 events),
                      Q5 - Q1 equal weight, plain t across cohorts PLUS an
                      overlap-honest view (63d ~ 3x overlapping months);
  * cost            = 25 bps per side (50 bps round trip), never reduced;
  * non-overlap     = confirmation events must precede the 2024-12-01 discovery
                      request window (measured discovery interval 2024-12-02 ..
                      2026-08-07); provider event ids are cross-checked too.

Selection-bias accounting: sales surprise was chosen AFTER an exploratory
screen of three event studies, so the primary gate uses a Bonferroni-of-3
threshold (t >= 2.39) and the overlap-honest t must independently reach 2.0.

Read-only against every operational store. Writes ONLY the machine result JSON
given by ``--out`` (research evidence). No orders, no ledger writes, no
promotion — the champion remains composite_sn regardless of outcome.
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import json
import math
import sys
from collections import defaultdict
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO.parent))

from paper_trader.alpha_agent.historical_identity import IdentityStore  # noqa: E402
from paper_trader.alpha_agent.historical_price_panel import (           # noqa: E402
    build_assetid_price_panel)

# --------------------------------------------------------------------------- #
# FROZEN CONTRACT CONSTANTS (mirror SALES_SURPRISE_CONFIRMATION_CONTRACT.md —
# sha256 5159D661CC50D9D2569FA3E5BAE460764369B0569350692F62BCED3A556F7C9A).
# Tests assert these values; changing any of them is a contract violation.
# --------------------------------------------------------------------------- #
DISCOVERY_REQUEST_START = "2024-12-01"   # discovery capture window start
CONFIRMATION_END = "2024-11-30"          # last admissible confirmation date
SIGNAL_FIELD = "sales_percent_diff"      # provider-computed; never recomputed
HORIZONS = (5, 20, 63)
PRIMARY_HORIZON = 63
N_QUANTILES = 5
COHORT_MIN_EVENTS = 25
COST_PER_SIDE = 0.0025                   # 25 bps per side -> 50 bps on spread
ROUND_TRIP_COST = 2 * COST_PER_SIDE
T_PRIMARY_BONFERRONI = 2.39              # two-sided 5% / family of 3
T_OVERLAP_HONEST = 2.0
OVERLAP_FACTOR = 3                       # 63d horizon / ~21-session months
MIN_COHORTS = 24
MIN_EVENTS_TOTAL = 1000
HIT_RATE_MIN = 0.55

DISP_CONFIRMED = "CONFIRMED_PROMISING_ALPHA_CANDIDATE"
DISP_PARTIAL = "PARTIAL_REPLICATION"
DISP_NULL = "NULL_REPLICATION"
DISP_ADVERSE = "ADVERSE_REPLICATION"
DISP_INSUFFICIENT = "INSUFFICIENT_HISTORY"

IDENTITY_DB = Path(r"D:\Stock_Prediction_app_data\alpha_agent\identity\historical_identity.sqlite")
INGESTION_ROOT = Path(r"D:\Stock_Prediction_app_data\alpha_agent\ingestion")
DEFAULT_HISTORICAL = Path(
    r"D:\Stock_Prediction_app_data\provider_trials\intrinio\zacks_snapshots"
    r"\sales_surprises_historical\2023-01-01_2024-11-30.jsonl")
DEFAULT_DISCOVERY = Path(
    r"D:\Stock_Prediction_app_data\provider_trials\intrinio\zacks_snapshots"
    r"\sales_surprises\2026-08-10.jsonl")
CHAMPION_PANEL = Path(
    r"C:\Users\binis\Stock_Prediction_app_push\research\output"
    r"\phase10l_historical_sector_neutral_scored_panel_reconstruction"
    r"\historical_sector_neutral_scored_panel.csv")
MOMENTUM_PANEL = Path(
    r"D:\Stock_Prediction_app_data\phase25_multi_horizon_alpha\_inputs"
    r"\momentum_monthly_panel.csv")


# --------------------------------------------------------------------------- #
# Loading (thin IO; the science below is pure functions on plain structures).
# --------------------------------------------------------------------------- #
def load_events_jsonl(path: Path) -> list:
    out = []
    with Path(path).open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            out.append(rec.get("payload") or {})
    return out


def admissible_confirmation_events(events: list) -> dict:
    """Apply the frozen admission rule. An event is admissible only when its
    ``actual_reported_date`` <= CONFIRMATION_END (strictly pre-discovery) and
    the frozen signal field + identity ticker are present. Nothing is ever
    remapped to another date; refusals are counted, never patched."""
    kept, refused = [], defaultdict(int)
    for p in events:
        d = str(p.get("actual_reported_date") or "")[:10]
        if not d:
            refused["no_reported_date"] += 1
            continue
        if d >= DISCOVERY_REQUEST_START:
            refused["inside_or_after_discovery_window"] += 1
            continue
        if d > CONFIRMATION_END:
            refused["after_confirmation_end"] += 1
            continue
        if p.get(SIGNAL_FIELD) is None:
            refused["signal_field_null"] += 1
            continue
        tkr = ((p.get("security") or {}).get("ticker") or "").strip().upper()
        if not tkr:
            refused["no_ticker"] += 1
            continue
        kept.append({"id": p.get("id"), "reported": d, "ticker": tkr,
                     "signal": float(p[SIGNAL_FIELD]),
                     "fiscal_year": p.get("fiscal_year"),
                     "fiscal_quarter": p.get("fiscal_quarter")})
    return {"events": kept, "refused": dict(refused)}


def assert_non_overlap(confirmation: list, discovery_payloads: list) -> dict:
    """HARD non-overlap guard between confirmation and discovery: disjoint
    provider event ids AND a strict date gap. Raises on violation."""
    disc_ids = {p.get("id") for p in discovery_payloads if p.get("id")}
    disc_dates = sorted(str(p.get("actual_reported_date") or "")[:10]
                        for p in discovery_payloads
                        if p.get("actual_reported_date"))
    conf_ids = {e["id"] for e in confirmation if e.get("id")}
    conf_dates = sorted(e["reported"] for e in confirmation)
    shared = conf_ids & disc_ids
    if shared:
        raise ValueError("confirmation/discovery share %d provider event ids"
                         % len(shared))
    if conf_dates and disc_dates and conf_dates[-1] >= disc_dates[0]:
        raise ValueError("confirmation events (%s) overlap discovery start (%s)"
                         % (conf_dates[-1], disc_dates[0]))
    return {"discovery_interval": [disc_dates[0] if disc_dates else None,
                                   disc_dates[-1] if disc_dates else None],
            "confirmation_interval": [conf_dates[0] if conf_dates else None,
                                      conf_dates[-1] if conf_dates else None],
            "gap_days": ((_dt.date.fromisoformat(disc_dates[0])
                          - _dt.date.fromisoformat(conf_dates[-1])).days
                         if conf_dates and disc_dates else None),
            "shared_event_ids": 0}


# --------------------------------------------------------------------------- #
# Survivorship-safe identity resolution at the event date.
# --------------------------------------------------------------------------- #
def resolve_events(events: list, universe_on) -> dict:
    """Resolve each event ticker against the historical S&P membership ON ITS
    REPORT DATE (``universe_on(date) -> list[dict]``). Delisted members are
    retained wherever they were members. Unresolved events are dropped and
    counted (the membership filter IS the study universe)."""
    by_date = defaultdict(list)
    for e in events:
        by_date[e["reported"]].append(e)
    resolved, unresolved = [], 0
    n_delisted_events = 0
    cur_secs, del_secs = set(), set()
    for d in sorted(by_date):
        idx = {}
        for u in universe_on(d):
            aid = u.get("norgate_assetid")
            if not aid:
                continue
            for key in {str(u.get("ticker_effective_on") or "").upper(),
                        str(u.get("ticker") or "").upper()}:
                if key:
                    idx.setdefault(key, u)
        for e in by_date[d]:
            u = idx.get(e["ticker"])
            if u is None:
                unresolved += 1
                continue
            # historical_universe_on rows carry delisting_date (no is_current
            # key): a security is delisted-at-some-point iff it has one.
            is_current = not u.get("delisting_date")
            e2 = dict(e, assetid=str(u["norgate_assetid"]),
                      security_id=u.get("security_id"),
                      is_current_security=is_current)
            if is_current:
                cur_secs.add(e2["assetid"])
            else:
                del_secs.add(e2["assetid"])
                n_delisted_events += 1
            resolved.append(e2)
    return {"events": resolved, "unresolved": unresolved,
            "resolved": len(resolved),
            "events_delisted_securities": n_delisted_events,
            "distinct_current_securities": len(cur_secs),
            "distinct_delisted_securities": len(del_secs)}


# --------------------------------------------------------------------------- #
# Forward returns on the owned panel (PIT: entry strictly after report date;
# an unmatured/missing horizon is EXCLUDED for that horizon, never substituted).
# --------------------------------------------------------------------------- #
def attach_forward_returns(events: list, panel: dict, sessions: list) -> list:
    pos = {d: i for i, d in enumerate(sessions)}
    out = []
    for e in events:
        ei = None
        for i, d in enumerate(sessions):        # first session STRICTLY after
            if d > e["reported"]:
                ei = i
                break
        if ei is None:
            continue
        px = dict(panel.get(e["assetid"], ()))
        entry_d = sessions[ei]
        p0 = px.get(entry_d)
        if not p0 or p0 <= 0:
            continue
        row = dict(e, entry_session=entry_d)
        got_any = False
        for h in HORIZONS:
            if ei + h >= len(sessions):
                continue                         # calendar not elapsed: exclude
            p1 = px.get(sessions[ei + h])
            if p1 is None:
                continue                         # security path missing: exclude
            row["fwd_%d" % h] = float(p1) / float(p0) - 1.0
            got_any = True
        if got_any:
            out.append(row)
    del pos
    return out


# --------------------------------------------------------------------------- #
# Frozen statistic: monthly-cohort quintile spread.
# --------------------------------------------------------------------------- #
def _mean(xs):
    return sum(xs) / len(xs) if xs else None


def _tstat(xs):
    n = len(xs)
    if n < 2:
        return None
    m = _mean(xs)
    var = sum((x - m) ** 2 for x in xs) / (n - 1)
    if var <= 0:
        return None
    return m / math.sqrt(var / n)


def _tstat_ndof(xs, n_eff):
    """t with an explicit (reduced) effective sample size."""
    n = len(xs)
    if n < 2 or n_eff < 2:
        return None
    m = _mean(xs)
    var = sum((x - m) ** 2 for x in xs) / (n - 1)
    if var <= 0:
        return None
    return m / math.sqrt(var / n_eff)


def cohort_spreads(events: list, horizon: int) -> list:
    """[(cohort_month, Q5-Q1 equal-weight spread, n_events)] for cohorts with
    >= COHORT_MIN_EVENTS events carrying this horizon. Deterministic ordering
    (signal, ticker) breaks ties; quintile k = idx * 5 // n."""
    key = "fwd_%d" % horizon
    coh = defaultdict(list)
    for e in events:
        if key in e:
            coh[e["reported"][:7]].append(e)
    out = []
    for month in sorted(coh):
        rows = sorted(coh[month], key=lambda e: (e["signal"], e["ticker"]))
        n = len(rows)
        if n < COHORT_MIN_EVENTS:
            continue
        buckets = defaultdict(list)
        for i, e in enumerate(rows):
            buckets[min(N_QUANTILES - 1, i * N_QUANTILES // n)].append(e[key])
        lo, hi = buckets.get(0), buckets.get(N_QUANTILES - 1)
        if not lo or not hi:
            continue
        out.append((month, _mean(hi) - _mean(lo), n))
    return out


def horizon_stats(events: list, horizon: int) -> dict:
    sp = cohort_spreads(events, horizon)
    xs = [s for _, s, _ in sp]
    n = len(xs)
    res = {"horizon": horizon, "cohorts": n,
           "events_used": sum(c for _, _, c in sp),
           "gross_spread_mean": round(_mean(xs), 6) if xs else None,
           "net_spread_mean": (round(_mean(xs) - ROUND_TRIP_COST, 6)
                               if xs else None),
           "t_stat": round(_tstat(xs), 3) if _tstat(xs) is not None else None,
           "hit_rate": round(sum(1 for x in xs if x > 0) / n, 3) if n else None,
           "cohort_months": [m for m, _, _ in sp]}
    if horizon == PRIMARY_HORIZON and n:
        n_eff = max(1, n // OVERLAP_FACTOR)
        t_adj = _tstat_ndof(xs, n_eff)
        halves = (xs[: n // 2], xs[n // 2:])
        # 3-phase non-overlapping monthly subsequences (every 3rd cohort).
        phases = {}
        for ph in range(OVERLAP_FACTOR):
            sub = xs[ph::OVERLAP_FACTOR]
            phases["phase_%d" % ph] = {
                "n": len(sub),
                "net_mean": (round(_mean(sub) - ROUND_TRIP_COST, 6)
                             if sub else None),
                "t": round(_tstat(sub), 3) if _tstat(sub) is not None else None}
        cum, peak, mdd = 0.0, 0.0, 0.0
        for x in xs:
            cum += x - ROUND_TRIP_COST
            peak = max(peak, cum)
            mdd = min(mdd, cum - peak)
        per_year = defaultdict(list)
        for (m, s, _c) in sp:
            per_year[m[:4]].append(s)
        res.update({
            "effective_independent_cohorts": n_eff,
            "t_stat_overlap_adjusted": (round(t_adj, 3)
                                        if t_adj is not None else None),
            "half1_net_mean": (round(_mean(halves[0]) - ROUND_TRIP_COST, 6)
                               if halves[0] else None),
            "half2_net_mean": (round(_mean(halves[1]) - ROUND_TRIP_COST, 6)
                               if halves[1] else None),
            "non_overlapping_phases": phases,
            "net_cumulative": round(cum, 6),
            "net_max_drawdown": round(mdd, 6),
            "per_year_gross_mean": {y: round(_mean(v), 6)
                                    for y, v in sorted(per_year.items())},
            "turnover_note": ("event portfolio reforms fully each monthly "
                              "cohort (~100% turnover; the 50 bps round-trip "
                              "cost is charged on every cohort spread)")})
    return res


# --------------------------------------------------------------------------- #
# Orthogonality vs champion / momentum (where legitimately computable).
# --------------------------------------------------------------------------- #
def _ranks(vals: list) -> list:
    order = sorted(range(len(vals)), key=lambda i: vals[i])
    r = [0.0] * len(vals)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and vals[order[j + 1]] == vals[order[i]]:
            j += 1
        avg = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            r[order[k]] = avg
        i = j + 1
    return r


def _pearson(a: list, b: list):
    n = len(a)
    if n < 3:
        return None
    ma, mb = _mean(a), _mean(b)
    sa = math.sqrt(sum((x - ma) ** 2 for x in a))
    sb = math.sqrt(sum((x - mb) ** 2 for x in b))
    if sa == 0 or sb == 0:
        return None
    return sum((a[i] - ma) * (b[i] - mb) for i in range(n)) / (sa * sb)


def spearman(a: list, b: list):
    return _pearson(_ranks(a), _ranks(b))


def partial_spearman(y: list, x: list, controls: list):
    """Rank partial correlation of y with x controlling the given covariate
    lists (each same length). OLS residuals on ranks via normal equations."""
    ry, rx = _ranks(y), _ranks(x)
    rcs = [_ranks(c) for c in controls]

    def _resid(t: list) -> list:
        cols = [[1.0] * len(t)] + rcs
        k = len(cols)
        ata = [[sum(cols[i][n] * cols[j][n] for n in range(len(t)))
                for j in range(k)] for i in range(k)]
        atb = [sum(cols[i][n] * t[n] for n in range(len(t))) for i in range(k)]
        # Gaussian elimination (k <= 3)
        m = [row[:] + [atb[i]] for i, row in enumerate(ata)]
        for col in range(k):
            piv = max(range(col, k), key=lambda r: abs(m[r][col]))
            if abs(m[piv][col]) < 1e-12:
                return None
            m[col], m[piv] = m[piv], m[col]
            for r in range(k):
                if r != col:
                    f = m[r][col] / m[col][col]
                    m[r] = [m[r][c] - f * m[col][c] for c in range(k + 1)]
        beta = [m[i][k] / m[i][i] for i in range(k)]
        return [t[n] - sum(beta[i] * cols[i][n] for i in range(k))
                for n in range(len(t))]

    ey, ex = _resid(ry), _resid(rx)
    if ey is None or ex is None:
        return None
    return _pearson(ey, ex)


def load_champion_scores(path: Path, min_date: str) -> dict:
    """{ticker: [(rebalance_date, composite_sn)] sorted} for rebalances on or
    after ``min_date`` (read-only; the frozen Phase 10-L scored panel)."""
    out = defaultdict(list)
    if not Path(path).is_file():
        return {}
    with Path(path).open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            rd = (row.get("rebalance_date") or "")[:10]
            if rd < min_date:
                continue
            try:
                out[str(row.get("ticker") or "").upper()].append(
                    (rd, float(row["composite_sn"])))
            except (TypeError, ValueError, KeyError):
                continue
    return {t: sorted(v) for t, v in out.items()}


def load_momentum_scores(path: Path, min_month: str) -> dict:
    """{(month, ticker): mom_6_1} for months >= min_month (read-only)."""
    out = {}
    if not Path(path).is_file():
        return {}
    with Path(path).open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            m = (row.get("month") or "")[:7]
            if m < min_month:
                continue
            try:
                out[(m, str(row.get("ticker") or "").upper())] = \
                    float(row["mom_6_1"])
            except (TypeError, ValueError, KeyError):
                continue
    return out


def _prev_month(month: str) -> str:
    y, m = int(month[:4]), int(month[5:7])
    return "%04d-%02d" % ((y - 1, 12) if m == 1 else (y, m - 1))


def orthogonality(events: list, champ: dict, mom: dict) -> dict:
    """Spearman of the frozen surprise signal vs champion / momentum scores at
    the event date (PIT: latest champion rebalance ON/BEFORE the report date
    within 400 days; momentum from the PRIOR calendar month), plus the rank
    partial of the 63d forward return on the signal controlling both."""
    sig_c, ch = [], []
    sig_m, mo = [], []
    quad = []          # (fwd63, signal, champ, mom) complete cases
    for e in events:
        s = e["signal"]
        c_val = None
        rows = champ.get(e["ticker"], ())
        for rd, v in reversed(rows):
            if rd <= e["reported"]:
                if (_dt.date.fromisoformat(e["reported"])
                        - _dt.date.fromisoformat(rd)).days <= 400:
                    c_val = v
                break
        m_val = mom.get((_prev_month(e["reported"][:7]), e["ticker"]))
        if c_val is not None:
            sig_c.append(s)
            ch.append(c_val)
        if m_val is not None:
            sig_m.append(s)
            mo.append(m_val)
        if c_val is not None and m_val is not None and "fwd_63" in e:
            quad.append((e["fwd_63"], s, c_val, m_val))
    part = None
    if len(quad) >= 30:
        part = partial_spearman([q[0] for q in quad], [q[1] for q in quad],
                                [[q[2] for q in quad], [q[3] for q in quad]])
    return {
        "spearman_vs_composite_sn": (round(spearman(sig_c, ch), 4)
                                     if len(sig_c) >= 30 else None),
        "n_vs_composite_sn": len(sig_c),
        "spearman_vs_mom_6_1": (round(spearman(sig_m, mo), 4)
                                if len(sig_m) >= 30 else None),
        "n_vs_mom_6_1": len(sig_m),
        "partial_rank_fwd63_on_signal_controlling_champ_mom":
            (round(part, 4) if part is not None else None),
        "n_partial": len(quad),
        "raw_rank_fwd63_on_signal_same_subset":
            (round(spearman([q[0] for q in quad], [q[1] for q in quad]), 4)
             if len(quad) >= 30 else None),
    }


# --------------------------------------------------------------------------- #
# Gates -> disposition (conservative; measured, never waived).
# --------------------------------------------------------------------------- #
def decide(primary: dict, resolved_meta: dict) -> dict:
    g = {}
    n_coh = primary.get("cohorts") or 0
    g["breadth_events"] = (primary.get("events_used") or 0) >= MIN_EVENTS_TOTAL
    g["min_cohorts"] = n_coh >= MIN_COHORTS
    g["positive_direction"] = (primary.get("gross_spread_mean") or 0) > 0
    g["net_positive"] = (primary.get("net_spread_mean") or 0) > 0
    g["t_primary_bonferroni"] = ((primary.get("t_stat") or 0)
                                 >= T_PRIMARY_BONFERRONI)
    g["t_overlap_honest"] = ((primary.get("t_stat_overlap_adjusted") or 0)
                             >= T_OVERLAP_HONEST)
    g["hit_rate"] = (primary.get("hit_rate") or 0) > HIT_RATE_MIN
    g["subperiod_halves_net_positive"] = (
        (primary.get("half1_net_mean") or 0) > 0
        and (primary.get("half2_net_mean") or 0) > 0)
    g["survivorship_delisted_retained"] = \
        (resolved_meta.get("distinct_delisted_securities") or 0) > 0
    insufficient = not (g["breadth_events"] and g["min_cohorts"])
    all_pass = all(g.values())
    net = primary.get("net_spread_mean")
    t = primary.get("t_stat")
    if insufficient:
        disp = DISP_INSUFFICIENT
    elif all_pass:
        disp = DISP_CONFIRMED
    elif net is not None and t is not None and net < 0 and t <= -2.0:
        disp = DISP_ADVERSE
    elif g["positive_direction"] and g["net_positive"]:
        disp = DISP_PARTIAL
    else:
        disp = DISP_NULL
    return {"gates": g, "disposition": disp,
            "no_champion_promotion": True, "champion": "composite_sn",
            "note": ("even CONFIRMED_PROMISING_ALPHA_CANDIDATE does not "
                     "promote; the Stage-13A adequacy + governance path is "
                     "still required for any operational use")}


# --------------------------------------------------------------------------- #
def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--historical", default=str(DEFAULT_HISTORICAL))
    ap.add_argument("--discovery", default=str(DEFAULT_DISCOVERY))
    ap.add_argument("--out", required=True,
                    help="machine result JSON path (research evidence)")
    args = ap.parse_args()

    hist_payloads = load_events_jsonl(Path(args.historical))
    disc_payloads = load_events_jsonl(Path(args.discovery))
    adm = admissible_confirmation_events(hist_payloads)
    sep = assert_non_overlap(adm["events"], disc_payloads)
    print("separation:", json.dumps(sep, sort_keys=True))

    store = IdentityStore(IDENTITY_DB)
    _cache: dict = {}

    def universe_on(d: str):
        if d not in _cache:
            _cache[d] = store.historical_universe_on(d)
        return _cache[d]

    res = resolve_events(adm["events"], universe_on)
    print("resolved=%d unresolved=%d delisted_secs=%d"
          % (res["resolved"], res["unresolved"],
             res["distinct_delisted_securities"]))

    panel = build_assetid_price_panel(INGESTION_ROOT, date_start="2022-06-01",
                                      date_end="2025-12-31")
    sessions = sorted({d for series in panel.values() for d, _ in series})
    events = attach_forward_returns(res["events"], panel, sessions)
    print("events_with_prices=%d sessions=%d [%s..%s]"
          % (len(events), len(sessions),
             sessions[0] if sessions else None,
             sessions[-1] if sessions else None))

    horizons = {str(h): horizon_stats(events, h) for h in HORIZONS}
    primary = horizons[str(PRIMARY_HORIZON)]
    champ = load_champion_scores(CHAMPION_PANEL, "2021-06-01")
    mom = load_momentum_scores(MOMENTUM_PANEL, "2022-06")
    orth = orthogonality(events, champ, mom)
    verdict = decide(primary, res)

    doc = {
        "study": "stage13c_sales_surprise_oos_confirmation",
        "generated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "contract_sha256": ("5159D661CC50D9D2569FA3E5BAE460764369B056"
                            "9350692F62BCED3A556F7C9A"),
        "frozen": {"signal_field": SIGNAL_FIELD, "horizons": list(HORIZONS),
                   "primary_horizon": PRIMARY_HORIZON,
                   "n_quantiles": N_QUANTILES,
                   "cohort_min_events": COHORT_MIN_EVENTS,
                   "cost_per_side": COST_PER_SIDE,
                   "t_primary_bonferroni": T_PRIMARY_BONFERRONI,
                   "t_overlap_honest": T_OVERLAP_HONEST,
                   "min_cohorts": MIN_COHORTS,
                   "min_events_total": MIN_EVENTS_TOTAL,
                   "hit_rate_min": HIT_RATE_MIN,
                   "confirmation_end": CONFIRMATION_END,
                   "discovery_request_start": DISCOVERY_REQUEST_START},
        "separation": sep,
        "admission": {"raw_rows": len(hist_payloads),
                      "admissible": len(adm["events"]),
                      "refused": adm["refused"]},
        "identity": {k: res[k] for k in (
            "resolved", "unresolved", "events_delisted_securities",
            "distinct_current_securities", "distinct_delisted_securities")},
        "events_with_prices": len(events),
        "horizons": horizons,
        "orthogonality": orth,
        "verdict": verdict,
        "safety": {"research_only": True, "no_orders": True,
                   "no_operational_writes": True, "no_promotion": True},
    }
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(doc, indent=1, sort_keys=True), encoding="utf-8")
    print(json.dumps({"disposition": verdict["disposition"],
                      "primary": {k: primary.get(k) for k in (
                          "cohorts", "events_used", "gross_spread_mean",
                          "net_spread_mean", "t_stat",
                          "t_stat_overlap_adjusted", "hit_rate",
                          "half1_net_mean", "half2_net_mean")}},
                     indent=1, sort_keys=True))
    print("STAGE13C_CONFIRMATION_%s" % verdict["disposition"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
