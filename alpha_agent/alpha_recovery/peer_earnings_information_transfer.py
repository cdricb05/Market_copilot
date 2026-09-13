r"""alpha_agent.alpha_recovery.peer_earnings_information_transfer - executor for the Alpha Agent
mechanism ``PEER_EARNINGS_INFORMATION_TRANSFER_V1``.

Contract: ``research/preregistration/PEER_EARNINGS_INFORMATION_TRANSFER_PREREGISTRATION.md``.
Executes it and changes nothing in it.

    mechanism    early reporters' announcement-window reactions reveal industry news that
                 same-industry firms which have not yet reported absorb only partially until
                 their own announcement (Thomas and Zhang 2008)
    signal       the EQUAL-WEIGHT mean market-adjusted announcement reaction of >= 3 same-industry
                 members (point-in-time 2-digit SIC) whose Item 2.02 reaction is complete by the
                 decision close, within the prior 20 sessions
    position     weekly cohorts of not-yet-reporting members: long the top tercile, short the
                 bottom tercile, entered at the close AFTER the decision close and held to the
                 earlier of entry + 20 sessions and two sessions before the EXPECTED announcement.
                 An actual announcement inside the window never shortens it (that would be
                 look-ahead); it is held and disclosed
    PIT          announcement instants are 8-K Item 2.02 acceptance timestamps; the industry is
                 the SIC of the latest SEC Financial Statement Data Sets submission accepted on a
                 calendar date STRICTLY before the decision date; the expected announcement is a
                 prior announcement session's date + 364 days

Existing owners only: ``event_8k_data`` (the Item-coded 8-K stream and the acceptance -> session
rule), ``control_block_events.identity_map`` (CIK -> panel rows, owned R58 bridge only, no
network), ``sec_financial_statement_sets`` (``sub.txt`` parsing, SIC observations, the manifest
hash), ``r63.sensitivity`` (``spearman``, ``nw_tstat``, ``bh_fdr``, ``_max_dd``),
``r63.pit.nw_lag`` and ``intraday_alpha.equal_risk_daily`` / ``incumbent_daily_path``.

THE CALENDAR (announcements, industries, peers, eligibility, exits, sides) reads no return of a
traded name. Its only price reads are each PEER's own announcement-window reaction, complete by
the decision close, and a candidate's price AT the decision close. Returns of traded names are
read only in :func:`measure`, only after the data gate, through :class:`PriceReader`, and the
untouched confirmation window only after every qualification gate passes.

RESEARCH ONLY. No purchase, no subscription, no promotion, no registration, no capital, no
proposal, no order, no fill, no live write. ``capital_eligible`` is always False; QUALIFIED only
opens a human gate.
"""
from __future__ import annotations

import hashlib
import json
from bisect import bisect_left
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from alpha_agent import sec_financial_statement_sets as FS
from alpha_agent.r63 import STANDALONE_T_FLOOR
from alpha_agent.r63 import pit as R63PIT
from alpha_agent.r63 import sensitivity as S

from . import (BH_Q, EQ_COST_RATE_PER_SIDE, MATERIALITY_ANN_NET, MIN_EFFECTIVE_PERIODS,
               research_root, stable_hash, write_artifact)
from . import control_block_events as CBE
from . import event_8k_data as D

CALCULATION_OWNER = "alpha_agent.alpha_recovery.peer_earnings_information_transfer"
MECHANISM_ID = "PEER_EARNINGS_INFORMATION_TRANSFER_V1"
ARTIFACT_NAME = "peer_earnings_information_transfer.json"
PREREGISTRATION = "research/preregistration/PEER_EARNINGS_INFORMATION_TRANSFER_PREREGISTRATION.md"
SCORER = ("alpha_agent.r63.sensitivity.spearman/nw_tstat/bh_fdr/_max_dd + alpha_agent.r63.pit.nw_lag + "
          "alpha_agent.alpha_recovery.intraday_alpha.equal_risk_daily (a weekly cross-sectional event "
          "book; run_cell's walk-forward ridge scorer has no event cohort or entry/exit window)")

#: The catalog's frozen kill rule, verbatim. An executor handed a different rule refuses to run
#: rather than silently judging a changed contract.
KILL_RULE_FROZEN = (
    "Kill if the signed rank IC t < 2.0 or the incremental net return over the incumbent is below "
    "1.5 %/yr at 12.5 bp; or the late reporters' move is already complete by the session after the "
    "peer signal (priced before entry); or it fails BH q=0.10 inheriting the own-issuer earnings and "
    "8-K event cells at p=1; or the industry assignment would require classification data from after "
    "the decision date.")

# --------------------------------------------------------------------------- #
# Owned inputs
# --------------------------------------------------------------------------- #
PIT_PANEL_NPZ = Path(r"D:\Stock_Prediction_app_data\r57_alpha_discovery\panels\sp500_pit_panel_v1.npz")
PIT_PANEL_META = Path(r"D:\Stock_Prediction_app_data\r57_alpha_discovery\panels\sp500_pit_panel_v1.meta.json")
FSDS_DIR = Path(r"D:\Stock_Prediction_app_data\alpha_agent\identity\sec_bulk\financial_statement_data_sets")
FSDS_FIRST_QUARTER = (2009, 2)
FSDS_LAST_QUARTER = (2026, 1)

# --------------------------------------------------------------------------- #
# Frozen design (section numbers refer to the preregistration)
# --------------------------------------------------------------------------- #
ANNOUNCEMENT_ITEM = "2.02"            # s1: Results of Operations and Financial Condition
FOLLOWUP_DAYS = 45                    # s1: a 2.02 within 45 days of the last kept one is a follow-up
SIC_GROUP_DIVISOR = 100               # s2: 2-digit SIC industry
PEER_LOOKBACK = 20                    # s3: peer event session in [t-20, t]
MIN_PEERS = 3                         # s3
NO_REPORT_SESSIONS = 42               # s4: no own announcement in [t-42, t]
EXPECTED_SHIFT_DAYS = 364             # s4: prior announcement session date + 52 weeks
MAX_LEAD_SESSIONS = 30                # s4: the expected session is at most t+30
ENTRY_DELAY = 1                       # s4: entry at the close AFTER the decision close
HOLD_CAP = 20                         # s4
PRE_EXPECTED_BUFFER = 2               # s4: exit two sessions before the expected session
MIN_HOLD = 3                          # s4
MIN_COHORT = 9                        # s5: three names per traded tercile
SLOT_WEIGHT = 0.20                    # s5: NAV per weekly cohort per leg (census: <= 5 concurrent)
FROZEN_SIGN = 1.0                     # s5: a HIGH peer reaction is LONG
DECISION_CADENCE = 5                  # s5: one decision per ISO week
IC_NW_LAG = R63PIT.nw_lag(HOLD_CAP, DECISION_CADENCE)   # = 3
BOOK_NW_LAG = 5                       # descriptive only
PPY = 252.0
COST_BPS = EQ_COST_RATE_PER_SIDE * 1e4                  # 12.5 bp per side
COST_LADDER_BPS = (0.0, COST_BPS, 25.0)
T_FLOOR = STANDALONE_T_FLOOR          # 2.0
PRICED_SHARE_MAX = 0.50               # s6
MIN_SIC_COVERAGE = 0.90               # s2 / gate 1
MIN_ANNOUNCEMENT_COVERAGE = 0.80      # s1 / gate 1
QUALIFICATION = ("2010-01-01", "2019-12-31")
HALVES = {"H1_2010_2014": ("2010-01-01", "2014-12-31"), "H2_2015_2019": ("2015-01-01", "2019-12-31")}
CONFIRMATION = ("2020-01-01", "2026-08-31")
#: s9 - the own-issuer earnings cells and the 8-K event cells, each inherited at p = 1.
INHERITED_NULLS = (
    "EARNINGS_EVENT_REACTION_SUE|US_EQUITY|XS|1|EARNINGS_EVENT_REACTION|vs|INCUMBENT_SCORE",
    "EARNINGS_EVENT_REACTION_SUE|US_EQUITY|XS|5|EARNINGS_EVENT_REACTION|vs|INCUMBENT_SCORE",
    "EARNINGS_EVENT_REACTION_SUE|US_EQUITY|XS|21|EARNINGS_EVENT_REACTION|vs|INCUMBENT_SCORE",
    "EARNINGS_EVENT_REACTION_SUE|US_EQUITY|XS|21|EARNINGS_EVENT_REACTION|vs|PRICE_RETURN_STATE+TREND+"
    "MOMENTUM+REVERSAL+REALISED_VOLATILITY+TAIL_CRASH_STATE+LIQUIDITY+VOLUME_PARTICIPATION+"
    "FUNDAMENTAL_LEVELS+FREE_CASH_FLOW",
    "EARNINGS_EVENT_REACTION_SUE|US_EQUITY|XS|21|EARNINGS_REACTION_ONLY|vs|INCUMBENT_SCORE",
    "EARNINGS_EVENT_REACTION_SUE|US_EQUITY|XS|21|EARNINGS_SURPRISE_ONLY|vs|INCUMBENT_SCORE",
    "EARNINGS_EVENT_REACTION_SUE|US_EQUITY|XS|63|EARNINGS_EVENT_REACTION|vs|INCUMBENT_SCORE",
    "EVENT_8K_ITEM_CODES|RESTRUCTURING_OR_IMPAIRMENT|h5",
    "EVENT_8K_ITEM_CODES|RESTRUCTURING_OR_IMPAIRMENT|h21",
    "EVENT_8K_ITEM_CODES|RESTRUCTURING_OR_IMPAIRMENT|h63",
)

# Executor verdict vocabulary (alpha_agent.r59.mechanisms.EXECUTOR_VERDICTS).
V_QUALIFIED = "QUALIFIED"
V_NO_EDGE = "NO_EDGE"
V_NO_INCREMENT = "NO_INCREMENTAL_INFORMATION_EDGE"
V_PRICED = "KILLED_PRICED_BEFORE_ENTRY"
V_MATERIALITY = "KILLED_BELOW_MATERIALITY"
V_NONINCREMENTAL = "KILLED_NONINCREMENTAL"
V_UNSTABLE = "KILLED_UNSTABLE"
V_WRONG_SIGN = "KILLED_WRONG_SIGN"
V_MULTIPLICITY = "KILLED_MULTIPLICITY"
V_DATA_HOLD = "DATA_HOLD"
V_NEED_MORE = "NEED_MORE_EVIDENCE"
VERDICTS_USED = (V_QUALIFIED, V_NO_EDGE, V_NO_INCREMENT, V_PRICED, V_MATERIALITY, V_NONINCREMENTAL,
                 V_UNSTABLE, V_WRONG_SIGN, V_MULTIPLICITY, V_DATA_HOLD, V_NEED_MORE)

_EMPTY = np.array([], dtype=int)


# --------------------------------------------------------------------------- #
# Loaders (read only)
# --------------------------------------------------------------------------- #
def load_panel(npz: Path = PIT_PANEL_NPZ, meta_path: Path = PIT_PANEL_META) -> dict:
    """The owned R57 point-in-time S&P 500 panel: total-return closes, membership, SPY."""
    meta = json.loads(Path(meta_path).read_text(encoding="utf-8"))
    z = np.load(npz, allow_pickle=False)
    return {"dates": pd.DatetimeIndex(pd.to_datetime(meta["dates"])),
            "symbols": [str(s) for s in meta["symbols"]],
            "tr": z["tr"].astype(np.float64), "mem": z["mem"].astype(bool),
            "spy": z["spy_tr"].astype(np.float64),
            "identity": "R57_SP500_PIT_PANEL:%s:%s..%s" % (meta.get("manifest_hash"), meta.get("date_start"),
                                                         meta.get("date_end"))}


def sic_index(observations) -> dict:
    """``{cik: (sorted available_at strings, sics)}`` from ``(cik, available_at, sic)`` tuples."""
    by = defaultdict(list)
    for cik, available_at, sic in observations:
        if cik is None or sic is None or not available_at:
            continue
        by[str(cik).lstrip("0")].append((str(available_at), int(sic)))
    out = {}
    for c, v in by.items():
        v.sort()
        out[c] = ([a for a, _ in v], [s for _, s in v])
    return out


def sic_as_of(index: dict, cik, day) -> Optional[int]:
    """The SIC of the latest FSDS submission accepted on a calendar date STRICTLY before ``day``.

    A submission accepted on the decision date itself is not used, whatever its time of day, so
    the industry never depends on classification data from on or after the decision date."""
    ix = index.get(str(cik)) if cik else None
    if not ix:
        return None
    k = bisect_left(ix[0], str(day)[:10])
    return ix[1][k - 1] if k > 0 else None


def load_fsds(root: Optional[Path] = None) -> dict:
    """Every owned FSDS ``sub.txt`` from 2009Q2 to 2026Q1, each verified against its manifest
    sha256. A missing or mismatched quarter fails the integrity check (gate 1)."""
    root = Path(root or FSDS_DIR)
    quarters = FS.quarters_through(*FSDS_LAST_QUARTER, start_year=FSDS_FIRST_QUARTER[0],
                                   start_quarter=FSDS_FIRST_QUARTER[1])
    missing, mismatched, digests, obs = [], [], [], []
    for y, q in quarters:
        d = root / ("%dq%d" % (y, q))
        fp, mp = d / FS.MEMBER_NAME, d / "manifest.json"
        if not (fp.exists() and mp.exists()):
            missing.append(d.name)
            continue
        blob = fp.read_bytes()
        try:
            want = json.loads(mp.read_text(encoding="utf-8")).get("member_sha256")
        except (OSError, ValueError):
            want = None
        got = FS.sha256_hex(blob)
        if not want or got != want:
            mismatched.append(d.name)
            continue
        digests.append(got)
        for o in FS.sic_observations(FS.parse_sub_txt(blob), quarter=d.name):
            if o.get("sic") is not None:
                obs.append((o["cik"], o["available_at"], o["sic"]))
    return {"index": sic_index(obs), "observations": len(obs),
            "integrity": {"root": str(root), "quarters_expected": len(quarters),
                          "quarters_verified": len(digests), "missing": missing, "mismatched": mismatched,
                          "passes": not missing and not mismatched,
                          "digest": hashlib.sha256("".join(digests).encode("utf-8")).hexdigest()}}


def load_announcement_filings(path: Optional[Path] = None) -> list:
    """Original 8-Ks declaring Item 2.02, from the stream owned by ``event_8k_data``."""
    p = Path(path) if path is not None else D.stream_path()
    if not p.exists():
        raise FileNotFoundError("the Item-coded 8-K stream %s is missing" % p)
    out = []
    with open(p, encoding="utf-8") as fh:
        for ln in fh:
            if ANNOUNCEMENT_ITEM not in ln:
                continue
            r = json.loads(ln)
            if r.get("form") == D.ORIGINAL_FORM and ANNOUNCEMENT_ITEM in (r.get("items") or []):
                out.append({"issuer_cik": str(r.get("issuer_cik") or ""),
                            "acceptance_utc": str(r.get("acceptance_utc") or ""),
                            "filing_date": str(r.get("filing_date") or "")[:10],
                            "accession": r.get("accession")})
    return out


def identity_from_map(cik2rows: dict, n_rows: int, meta: Optional[dict] = None) -> dict:
    row_cik: list = [None] * int(n_rows)
    for c, rows in cik2rows.items():
        for r in rows:
            row_cik[int(r)] = str(c)
    return {"row_cik": row_cik, "meta": dict(meta or {})}


def load_identity(symbols) -> dict:
    """CIK -> panel rows through the owned R58 RESOLVED bridge only (no SEC ticker map: no network)."""
    from alpha_agent.r58 import fundamentals as FU
    cik2rows, meta = CBE.identity_map({"symbols": np.asarray(symbols), "sym2cik": FU.cik_bridge()},
                                      use_sec_ticker_map=False)
    return identity_from_map(cik2rows, len(symbols), meta)


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load_inputs() -> dict:
    panel = load_panel()
    fsds = load_fsds()
    stream = D.stream_path()
    filings = load_announcement_filings(stream)
    ident = load_identity(panel["symbols"])
    return {"panel": panel, "row_cik": ident["row_cik"], "sic_index": fsds["index"],
            "fsds_integrity": fsds["integrity"], "filings": filings,
            "identity": {"panel": panel["identity"], "fsds_sub_txt_digest": fsds["integrity"]["digest"],
                         "fsds_quarters_verified": fsds["integrity"]["quarters_verified"],
                         "item_8k_stream_sha256": _sha256_file(stream),
                         "cik_bridge": "R58 RESOLVED cik_map via control_block_events.identity_map("
                                       "use_sec_ticker_map=False)",
                         "identity_meta": ident["meta"]}}


# --------------------------------------------------------------------------- #
# The calendar: announcements, industries, peers, eligibility (no traded-name return)
# --------------------------------------------------------------------------- #
def _day_numbers(dates) -> np.ndarray:
    return pd.DatetimeIndex(dates).values.astype("datetime64[D]").astype(np.int64)


def announcement_sessions(filings, ciks, dates) -> tuple:
    """``({cik: sorted unique event sessions}, stats)``.

    Per issuer, filings are taken in acceptance order; a 2.02 filed within FOLLOWUP_DAYS of the
    last KEPT one is a follow-up and is dropped. The event session is ``event_8k_data.
    decision_session``: the first session whose 16:00 ET close is strictly after acceptance."""
    dates64 = pd.DatetimeIndex(dates).values.astype("datetime64[ns]")
    wanted = {str(c) for c in ciks}
    by = defaultdict(list)
    for f in filings:
        c = str(f.get("issuer_cik") or "").lstrip("0")
        if c in wanted:
            by[c].append((str(f.get("acceptance_utc") or ""), str(f.get("filing_date") or "")[:10]))
    out, st = {}, Counter()
    by_year = defaultdict(Counter)
    for c, lst in by.items():
        lst.sort()
        last, es = None, []
        for acc, fd in lst:
            try:
                day = date.fromisoformat(fd)
            except ValueError:
                st["unreadable_filing_date"] += 1
                continue
            if last is not None and (day - last).days <= FOLLOWUP_DAYS:
                st["followups_collapsed"] += 1
                continue
            last = day
            t = D.decision_session(dates64, acc)
            if t is None:
                st["outside_calendar"] += 1
                continue
            st["announcements"] += 1
            by_year[fd[:4]][D.session_class(dates64, acc)] += 1
            es.append(int(t))
        out[c] = np.array(sorted(set(es)), dtype=int)
    stats = dict(st)
    stats["acceptance_class_by_year"] = {y: dict(v) for y, v in sorted(by_year.items())}
    return out, stats


def weekly_decisions(dates, lo: str, hi: str) -> np.ndarray:
    """The last panel session of each ISO week, within [lo, hi]."""
    dates = pd.DatetimeIndex(dates)
    iso = dates.isocalendar()
    last = pd.Series(np.arange(len(dates)), index=dates).groupby(
        [iso["year"].to_numpy(), iso["week"].to_numpy()]).max().to_numpy()
    last = np.sort(last)
    keep = np.asarray((dates[last] >= pd.Timestamp(lo)) & (dates[last] <= pd.Timestamp(hi)))
    return last[keep].astype(int)


def expected_session(day_num: np.ndarray, past: np.ndarray, t: int) -> Optional[int]:
    """The first session on or after (earliest past announcement session date + 364 days) that
    lies strictly after the decision date. Only announcements at or before ``t`` are used."""
    if not len(past):
        return None
    exp = day_num[past] + EXPECTED_SHIFT_DAYS
    fut = exp[exp > day_num[t]]
    if not len(fut):
        return None
    return int(np.searchsorted(day_num, fut.min(), side="left"))


def peer_reaction(tr: np.ndarray, spy: np.ndarray, row: int, e: int) -> float:
    """A PEER's own announcement-window reaction: total return from the close before its event
    session to the event-session close, minus SPY's total return over the same span."""
    if e < 1:
        return np.nan
    p0, p1, s0, s1 = float(tr[row, e - 1]), float(tr[row, e]), float(spy[e - 1]), float(spy[e])
    if not (np.isfinite(p0) and np.isfinite(p1) and np.isfinite(s0) and np.isfinite(s1) and p0 > 0 and s0 > 0):
        return np.nan
    return p1 / p0 - s1 / s0


def _assign_sides(entrants: list) -> None:
    order = sorted(range(len(entrants)), key=lambda i: (FROZEN_SIGN * entrants[i]["signal"], entrants[i]["row"]))
    k = len(entrants) // 3
    for e in entrants:
        e["side"] = 0
    for i in order[:k]:
        entrants[i]["side"] = -1
    for i in order[len(order) - k:]:
        entrants[i]["side"] = 1


def build_cohorts(panel: dict, row_cik: list, ann: dict, sic_idx: dict, decisions) -> dict:
    """Weekly cohorts, in decision order. Each (issuer, expected session) is scored ONCE, at the
    first decision where its industry has MIN_PEERS reported peers."""
    dates = panel["dates"]
    day_num = _day_numbers(dates)
    n_t = len(dates)
    mem, tr, spy = panel["mem"], panel["tr"], panel["spy"]
    consumed: set = set()
    cohorts, st = [], Counter()
    for t in decisions:
        t = int(t)
        dstr = str(dates[t].date())
        seen: set = set()
        info = []
        for r in np.flatnonzero(mem[:, t]):
            c = row_cik[r]
            if not c or c in seen:
                continue
            seen.add(c)
            sic = sic_as_of(sic_idx, c, dstr)
            if sic is None:
                st["member_decisions_without_pit_sic"] += 1
                continue
            ev = ann.get(c, _EMPTY)
            info.append((int(r), c, int(sic) // SIC_GROUP_DIVISOR, ev[:int(np.searchsorted(ev, t, side="right"))]))
        peers, members = defaultdict(list), defaultdict(list)
        for r, c, g, past in info:
            members[g].append(r)
            if len(past) and past[-1] >= t - PEER_LOOKBACK:
                x = peer_reaction(tr, spy, r, int(past[-1]))
                if np.isfinite(x):
                    peers[g].append(x)
        entrants = []
        for r, c, g, past in info:
            if not len(past) or past[-1] >= t - NO_REPORT_SESSIONS:
                continue
            exp = expected_session(day_num, past, t)
            if exp is None or exp > t + MAX_LEAD_SESSIONS:
                continue
            if len(peers[g]) < MIN_PEERS:
                continue
            key = (c, exp)
            if key in consumed:
                continue
            consumed.add(key)
            entry = t + ENTRY_DELAY
            exit_ = min(entry + HOLD_CAP, exp - PRE_EXPECTED_BUFFER)
            if exit_ - entry < MIN_HOLD:
                st["hold_below_minimum"] += 1
                continue
            if exit_ >= n_t:
                st["exit_beyond_calendar"] += 1
                continue
            p = float(tr[r, t])
            if not (np.isfinite(p) and p > 0):
                st["no_price_at_signal_close"] += 1
                continue
            ev = ann.get(c, _EMPTY)
            nxt = ev[ev > t]
            actual = int(nxt[0]) if len(nxt) else None
            entrants.append({"row": r, "cik": c, "group": g, "npeers": len(peers[g]),
                             "signal": float(np.mean(peers[g])), "entry": entry, "exit": int(exit_),
                             "hold": int(exit_ - entry), "expected": exp, "actual_next_announcement": actual,
                             "actual_at_or_before_planned_exit": actual is not None and actual <= exit_})
        if not entrants:
            continue
        traded = len(entrants) >= MIN_COHORT
        if traded:
            _assign_sides(entrants)
        else:
            for e in entrants:
                e["side"] = 0
        groups = {e["group"] for e in entrants}
        cohorts.append({"t": t, "date": dstr, "traded": traded, "entrants": entrants,
                        "group_members": {g: list(members[g]) for g in groups}})
    return {"cohorts": cohorts, "stats": dict(st)}


def coverage(panel: dict, row_cik: list, ann: dict, sic_idx: dict, lo: str, hi: str) -> dict:
    """Member-quarters: (panel row, calendar quarter) with PIT membership on the quarter's FIRST
    panel session in the window. Covered by PIT SIC iff the row has a CIK and a FSDS submission
    accepted strictly before that session's date; by an announcement iff an Item 2.02 event
    session falls inside the quarter. No price is read."""
    dates = pd.DatetimeIndex(panel["dates"])
    mem = panel["mem"]
    per = pd.PeriodIndex(dates, freq="Q")
    sel = np.asarray((dates >= pd.Timestamp(lo)) & (dates <= pd.Timestamp(hi)))
    keys = ("member_quarters", "bridged", "pit_sic", "announcement_in_quarter")
    by_year: dict = {}
    for q in sorted(set(per[sel])):
        cols = np.flatnonzero(np.asarray(per == q) & sel)
        t0, t1 = int(cols[0]), int(cols[-1])
        d0 = str(dates[t0].date())
        y = by_year.setdefault(str(q.year), dict.fromkeys(keys, 0))
        for r in np.flatnonzero(mem[:, t0]):
            y["member_quarters"] += 1
            c = row_cik[r]
            if not c:
                continue
            y["bridged"] += 1
            y["pit_sic"] += int(sic_as_of(sic_idx, c, d0) is not None)
            ev = ann.get(c, _EMPTY)
            y["announcement_in_quarter"] += int(bool(((ev >= t0) & (ev <= t1)).any()))
    tot = {k: int(sum(v[k] for v in by_year.values())) for k in keys}
    m = max(1, tot["member_quarters"])
    return {"window": [lo, hi], "totals": tot, "pit_sic_share": tot["pit_sic"] / m,
            "bridged_share": tot["bridged"] / m, "announcement_share": tot["announcement_in_quarter"] / m,
            "by_year": by_year}


def _window_years(dates, lo: str, hi: str) -> float:
    dates = pd.DatetimeIndex(dates)
    d = dates[(dates >= pd.Timestamp(lo)) & (dates <= pd.Timestamp(hi))]
    return ((d[-1] - d[0]).days + 1) / 365.25 if len(d) > 1 else 0.0


def calendar_census(cohorts: list, dates, lo: str, hi: str) -> dict:
    """Counts on the calendar only (no return)."""
    cw = [c for c in cohorts if lo <= c["date"] <= hi]
    traded = [c for c in cw if c["traded"]]
    ents = [e for c in traded for e in c["entrants"]]
    years = _window_years(dates, lo, hi)
    hurdle = (len(traded) / years * SLOT_WEIGHT * 2 * 2 * COST_BPS * 1e-4) if years > 0 else None
    return {"window": [lo, hi], "years": years, "decisions_with_entrants": len(cw),
            "entrants_all": int(sum(len(c["entrants"]) for c in cw)), "traded_cohorts": len(traded),
            "traded_cohorts_per_year": (len(traded) / years) if years > 0 else None,
            "traded_entrants": len(ents),
            "entrants_per_traded_cohort_median": float(np.median([len(c["entrants"]) for c in traded])) if traded else None,
            "hold_sessions_mean": float(np.mean([e["hold"] for e in ents])) if ents else None,
            "actual_at_or_before_planned_exit_share":
                float(np.mean([e["actual_at_or_before_planned_exit"] for e in ents])) if ents else None,
            "cost_hurdle_ann_nav": hurdle,
            "gross_needed_ann_nav_for_materiality": None if hurdle is None else hurdle + MATERIALITY_ANN_NET}


# --------------------------------------------------------------------------- #
# Measurement - the ONLY place a traded name's return is read
# --------------------------------------------------------------------------- #
class PriceReader:
    """Every price :func:`measure` reads passes through here; ``max_session_read`` lets an audit
    show the untouched confirmation window was never read."""

    def __init__(self, tr: np.ndarray, spy: np.ndarray):
        self._tr, self._spy = tr, spy
        self.max_session_read = -1

    def _note(self, s: int) -> None:
        self.max_session_read = max(self.max_session_read, int(s))

    def name(self, row: int, a: int, b: int) -> np.ndarray:
        self._note(b)
        return np.asarray(self._tr[row, a:b + 1], dtype=float)

    def name_at(self, row: int, s: int) -> float:
        self._note(s)
        return float(self._tr[row, s]) if s >= 0 else np.nan

    def spy(self, a: int, b: int) -> np.ndarray:
        self._note(b)
        return np.asarray(self._spy[a:b + 1], dtype=float)


def _nanmean(x) -> float:
    x = np.asarray(x, dtype=float)
    x = x[np.isfinite(x)]
    return float(x.mean()) if len(x) else np.nan


def _ic_stats(series, lag: int) -> dict:
    st = S.nw_tstat(np.asarray(series, dtype=float), lag=lag)
    return {"cohorts": st["n"], "mean": st["mean"], "t": st["t"], "p_one_sided": st["p_one_sided"], "nw_lag": lag}


def pre_entry_share(pre_mean, hold_mean) -> Optional[float]:
    """Share of the combined gross long-short response earned between the signal close and the
    entry close. 0 when the pre-entry response is not positive; 1 when the holding one is not."""
    if pre_mean is None or hold_mean is None or not np.isfinite(pre_mean) or not np.isfinite(hold_mean):
        return None
    if pre_mean <= 0:
        return 0.0
    if hold_mean <= 0:
        return 1.0
    return float(pre_mean / (pre_mean + hold_mean))


def _zscore(x: pd.Series) -> pd.Series:
    sd = x.std(ddof=0)
    return (x - x.mean()) / sd if np.isfinite(sd) and sd > 0 else x * 0.0


def price_state_control(rows: list) -> dict:
    """s7: within each cohort z-score the signal, the entrant's own return over (t-20, t] and its
    PIT industry's equal-weight return over the same span; residualise the signal z on the two
    control z's by one pooled OLS over the window; the rank IC of the residual against the
    holding return, per cohort, Newey-West lag IC_NW_LAG."""
    df = pd.DataFrame(rows, columns=["cohort", "signal", "own", "industry", "y"])
    df = df[np.isfinite(df["signal"]) & np.isfinite(df["own"]) & np.isfinite(df["industry"])]
    if len(df) < 3:
        return {"cohorts": 0, "mean": None, "t": None, "p_one_sided": None, "nw_lag": IC_NW_LAG}
    g = df.groupby("cohort", sort=True)
    zs, zo, zi = (g[c].transform(_zscore).to_numpy(dtype=float) for c in ("signal", "own", "industry"))
    X = np.column_stack([zo, zi])
    beta = np.linalg.lstsq(X, zs, rcond=None)[0]
    df = df.assign(resid=zs - X @ beta)
    ics = [S.spearman(s["resid"].to_numpy(), s["y"].to_numpy()) for _, s in df.groupby("cohort", sort=True)]
    out = _ic_stats(ics, IC_NW_LAG)
    out["coefficients"] = {"own_return_z": float(beta[0]), "industry_return_z": float(beta[1])}
    return out


def book_stats(x) -> dict:
    x = np.asarray(x, dtype=float)
    x = x[np.isfinite(x)]
    if len(x) < 3:
        return {"days": int(len(x)), "ann_net": None}
    sd = float(np.std(x, ddof=1))
    st = S.nw_tstat(x, BOOK_NW_LAG)
    return {"days": int(len(x)), "ann_net": float(x.mean() * PPY),
            "ann_vol": float(sd * np.sqrt(PPY)) if sd > 0 else None,
            "sharpe": float(x.mean() / sd * np.sqrt(PPY)) if sd > 0 else None,
            "t_nw_lag5_descriptive": st["t"], "max_dd": S._max_dd(x)}                   # noqa: SLF001


def measure(cohorts: list, prices: PriceReader, dates, lo: str, hi: str, *, label: str,
            audit: list) -> tuple:
    """(summary, daily net book at 12.5 bp) for the traded cohorts decided in [lo, hi]."""
    audit.append(label)
    dates = pd.DatetimeIndex(dates)
    n_t = len(dates)
    sel = [c for c in cohorts if c["traded"] and lo <= c["date"] <= hi]
    pnl, cost_w = np.zeros(n_t), np.zeros(n_t)
    ic_hold, ic_pre, ls_hold, ls_pre, ctrl = [], [], [], [], []
    n_ent = n_actual = 0
    for ci, c in enumerate(sel):
        t, ents = c["t"], c["entrants"]
        k_side = sum(1 for e in ents if e["side"] > 0)
        w = SLOT_WEIGHT / k_side if k_side else 0.0
        ind = {}
        for g, rows in c["group_members"].items():
            vals = [prices.name_at(r, t) / prices.name_at(r, t - PEER_LOOKBACK) - 1.0 for r in rows]
            ind[g] = _nanmean([v for v in vals if np.isfinite(v)])
        sig, y, ypre, R, Rpre, side = [], [], [], [], [], []
        for e in ents:
            n_ent += 1
            n_actual += int(bool(e["actual_at_or_before_planned_exit"]))
            r, en, ex = e["row"], e["entry"], e["exit"]
            path, sp = prices.name(r, t, ex), prices.spy(t, ex)
            p_t, seg = path[0], path[ENTRY_DELAY:]
            own_prev = prices.name_at(r, t - PEER_LOOKBACK)
            own = p_t / own_prev - 1.0 if np.isfinite(own_prev) and own_prev > 0 else np.nan
            if not (np.isfinite(seg[0]) and seg[0] > 0):            # no entry price: held in cash
                yy = yp = rp = np.nan
                rr = 0.0
            else:
                ok = np.flatnonzero(np.isfinite(seg) & (seg > 0))
                L = int(ok[-1])                                       # exit at the last finite close
                v = pd.Series(np.where(np.isfinite(seg[:L + 1]) & (seg[:L + 1] > 0), seg[:L + 1] / seg[0],
                                       np.nan)).ffill().to_numpy()
                rr = float(v[L] - 1.0)
                yy = rr - (sp[ENTRY_DELAY + L] / sp[ENTRY_DELAY] - 1.0)
                rp = float(seg[0] / p_t - 1.0)
                yp = rp - (sp[ENTRY_DELAY] / sp[0] - 1.0)
                if e["side"] != 0 and w > 0:
                    if L > 0:
                        pnl[en + 1:en + L + 1] += w * e["side"] * np.diff(v)
                    cost_w[en] += w
                    cost_w[en + L] += w * v[L]
            sig.append(e["signal"])
            y.append(yy)
            ypre.append(yp)
            R.append(rr)
            Rpre.append(rp)
            side.append(e["side"])
            ctrl.append((ci, e["signal"], own, ind.get(e["group"], np.nan), yy))
        sig, y, ypre, R, Rpre, side = (np.asarray(a, dtype=float) for a in (sig, y, ypre, R, Rpre, side))
        ic_hold.append(S.spearman(FROZEN_SIGN * sig, y))
        ic_pre.append(S.spearman(FROZEN_SIGN * sig, ypre))
        ls_hold.append(_nanmean(R[side > 0]) - _nanmean(R[side < 0]))
        ls_pre.append(_nanmean(Rpre[side > 0]) - _nanmean(Rpre[side < 0]))
    i0 = int(np.searchsorted(dates.values, pd.Timestamp(lo).to_datetime64(), side="left"))
    i_hi = int(np.searchsorted(dates.values, pd.Timestamp(hi).to_datetime64(), side="right")) - 1
    i1 = max([i_hi] + [max(e["exit"] for e in c["entrants"]) for c in sel])
    i1 = min(i1, n_t - 1)
    gross = pnl[i0:i1 + 1]
    ladder = {"%gbp" % cb: book_stats(gross - cost_w[i0:i1 + 1] * cb * 1e-4) for cb in COST_LADDER_BPS}
    daily_net = pd.Series(gross - cost_w[i0:i1 + 1] * COST_BPS * 1e-4, index=dates[i0:i1 + 1])
    hold_m, pre_m = _nanmean(ls_hold), _nanmean(ls_pre)
    summary = {
        "label": label, "window": [lo, hi], "cohorts": len(sel), "entrants": n_ent,
        "entrants_actual_at_or_before_planned_exit": n_actual,
        "signed_rank_ic": _ic_stats(ic_hold, IC_NW_LAG),
        "pre_entry_rank_ic": _ic_stats(ic_pre, 0),
        "ls_gross_per_cohort": {"holding_mean": None if not np.isfinite(hold_m) else hold_m,
                                "pre_entry_mean": None if not np.isfinite(pre_m) else pre_m},
        "pre_entry_share": pre_entry_share(pre_m, hold_m),
        "price_state_control": price_state_control(ctrl),
        "book_by_cost_bps_per_side": ladder, "book": ladder["%gbp" % COST_BPS],
        "ann_cost_drag": float(cost_w[i0:i1 + 1].mean() * COST_BPS * 1e-4 * PPY) if i1 >= i0 else None,
        "first_session": str(dates[i0].date()), "last_session": str(dates[i1].date())}
    return summary, daily_net


# --------------------------------------------------------------------------- #
# Gates, in the preregistered order (section 10)
# --------------------------------------------------------------------------- #
def _v(verdict: str, gate, why: str, kill=None) -> dict:
    return {"verdict": verdict, "gate": gate, "why": why, "kill_rule_fired": kill}


def _below(x, floor: float) -> bool:
    return x is None or not np.isfinite(float(x)) or float(x) < floor


def _r(x, n=3):
    return None if x is None else round(float(x), n)


def data_hold_reason(integrity: dict, cov: dict, traded_cohorts: int, window: str, *,
                     min_cohorts: int = MIN_EFFECTIVE_PERIODS) -> Optional[str]:
    why = []
    if not (integrity or {}).get("passes"):
        why.append("FSDS sub.txt integrity failed (missing %s, mismatched %s)"
                   % ((integrity or {}).get("missing"), (integrity or {}).get("mismatched")))
    if cov["pit_sic_share"] < MIN_SIC_COVERAGE:
        why.append("PIT SIC covers %.1f%% of %s member-quarters (floor %.0f%%; bridged %.1f%%)"
                   % (100 * cov["pit_sic_share"], window, 100 * MIN_SIC_COVERAGE, 100 * cov["bridged_share"]))
    if cov["announcement_share"] < MIN_ANNOUNCEMENT_COVERAGE:
        why.append("Item 2.02 announcements cover %.1f%% of %s member-quarters (floor %.0f%%)"
                   % (100 * cov["announcement_share"], window, 100 * MIN_ANNOUNCEMENT_COVERAGE))
    if traded_cohorts < min_cohorts:
        why.append("%d traded %s cohorts (floor %d)" % (traded_cohorts, window, min_cohorts))
    return "; ".join(why) or None


def pre_incumbent_gate(g: dict) -> Optional[dict]:
    """Gates 1-8. None means every one passed."""
    if g.get("data_hold"):
        return _v(V_DATA_HOLD, "DATA", g["data_hold"], "DATA")
    pre_carries = (g.get("pre_ic_mean") is not None and float(g["pre_ic_mean"]) > 0
                   and not _below(g.get("pre_ic_t"), T_FLOOR))
    if g.get("ic_mean") is None or float(g["ic_mean"]) <= 0:
        if pre_carries:
            return _v(V_PRICED, "FROZEN_SIGN", "the holding-window signed rank IC is not positive (%s) while the "
                      "signal-close-to-entry IC has t %s: the move is complete before a legitimate entry"
                      % (_r(g.get("ic_mean"), 4), _r(g.get("pre_ic_t"))), "PRICED_BEFORE_ENTRY")
        return _v(V_WRONG_SIGN, "FROZEN_SIGN", "mean signed rank IC %s is not positive; the sign is never "
                  "reversed" % _r(g.get("ic_mean"), 4), "SIGNED_RANK_IC_NOT_POSITIVE")
    if _below(g.get("ic_t"), T_FLOOR):
        if pre_carries:
            return _v(V_PRICED, "SIGNED_RANK_IC_T", "holding-window IC t %s below %.1f while the signal-close-to-"
                      "entry IC has t %s" % (_r(g.get("ic_t")), T_FLOOR, _r(g.get("pre_ic_t"))),
                      "PRICED_BEFORE_ENTRY")
        return _v(V_NO_EDGE, "SIGNED_RANK_IC_T", "signed rank IC NW t %s below %.1f (lag %d)"
                  % (_r(g.get("ic_t")), T_FLOOR, IC_NW_LAG), "SIGNED_RANK_IC_T_BELOW_2")
    share = g.get("pre_share")
    if share is None or float(share) >= PRICED_SHARE_MAX:
        return _v(V_PRICED, "PRICED_BEFORE_ENTRY", "the signal-close-to-entry session carries %s of the combined "
                  "gross long-short response (limit %.2f)" % (_r(share), PRICED_SHARE_MAX), "PRICED_BEFORE_ENTRY")
    if _below(g.get("resid_ic_t"), T_FLOOR):
        return _v(V_NONINCREMENTAL, "PRICE_STATE_CONTROL", "rank IC t after residualising on own and industry "
                  "look-back returns is %s, below %.1f" % (_r(g.get("resid_ic_t")), T_FLOOR),
                  "NOT_INCREMENTAL_TO_PRICE_STATE")
    if g.get("ann_net") is None or float(g["ann_net"]) < MATERIALITY_ANN_NET:
        return _v(V_MATERIALITY, "MATERIALITY", "net book %s/yr below %.3f at %.1f bp per side"
                  % (_r(g.get("ann_net"), 4), MATERIALITY_ANN_NET, COST_BPS), "NET_BOOK_BELOW_1.5PCT_PER_YEAR")
    halves = g.get("halves") or {}
    bad = sorted(k for k, h in halves.items()
                 if h.get("ann_net") is None or float(h["ann_net"]) <= 0
                 or h.get("ic_mean") is None or float(h["ic_mean"]) <= 0)
    if bad or len(halves) != len(HALVES):
        return _v(V_UNSTABLE, "HALVES", "non-positive net book or mean IC in %s" % (", ".join(bad) or "a missing half"),
                  "HALVES_DISAGREE")
    p = g.get("p_ic")
    if p is None or not g.get("bh_pass"):
        return _v(V_MULTIPLICITY, "BH_INHERITED", "one-sided p %s fails BH q=%.2f at m=%d with %d inherited cells at p=1"
                  % (p, BH_Q, 1 + len(INHERITED_NULLS), len(INHERITED_NULLS)),
                  "BH_Q010_FAILS_WITH_INHERITED_EARNINGS_AND_8K_CELLS")
    return None


def incumbent_gate(inc: Optional[dict]) -> Optional[dict]:
    """Gate 9."""
    if (inc or {}).get("state") != "OK":
        return _v(V_DATA_HOLD, "INCUMBENT_DATA", "the incumbent increment cannot be measured: %s"
                  % ((inc or {}).get("why") or "no incumbent path"), "DATA")
    x = inc.get("incremental_ann_net_return")
    if x is None or float(x) < MATERIALITY_ANN_NET or not inc.get("positive_incremental_utility_after_costs"):
        return _v(V_NO_INCREMENT, "INCUMBENT", "equal-risk increment over the incumbent %s/yr (floor %.3f), t %s"
                  % (_r(x, 4), MATERIALITY_ANN_NET, _r(inc.get("t_incremental"))),
                  "INCREMENT_OVER_INCUMBENT_BELOW_1.5PCT")
    return None


def qualification_gate(g: dict) -> Optional[dict]:
    """Gates 1-9 on one set of inputs (``g['incumbent']`` is the equal-risk result)."""
    return pre_incumbent_gate(g) or incumbent_gate(g.get("incumbent"))


def confirmation_gate(c: dict) -> dict:
    """Gate 10 on the untouched 2020-2026 window, reached only after gates 1-9."""
    if c.get("data_hold"):
        return _v(V_DATA_HOLD, "CONFIRMATION_DATA", c["data_hold"], "DATA")
    if int(c.get("cohorts") or 0) < MIN_EFFECTIVE_PERIODS:
        return _v(V_NEED_MORE, "CONFIRMATION_SAMPLE", "%s confirmation cohorts, floor %d"
                  % (c.get("cohorts"), MIN_EFFECTIVE_PERIODS))
    if c.get("ic_mean") is None or float(c["ic_mean"]) <= 0:
        return _v(V_UNSTABLE, "CONFIRMATION_SIGN", "confirmation mean signed rank IC %s is not positive"
                  % _r(c.get("ic_mean"), 4), "CONFIRMATION_SIGN_REVERSED")
    if _below(c.get("ic_t"), T_FLOOR):
        return _v(V_NO_EDGE, "CONFIRMATION_IC_T", "confirmation signed rank IC t %s below %.1f"
                  % (_r(c.get("ic_t")), T_FLOOR), "CONFIRMATION_IC_T_BELOW_2")
    p = c.get("p_ic")
    if p is None or float(p) > BH_Q:
        return _v(V_NO_EDGE, "CONFIRMATION_P", "confirmation one-sided p %s above %.2f" % (p, BH_Q),
                  "CONFIRMATION_P_ABOVE_Q")
    share = c.get("pre_share")
    if share is None or float(share) >= PRICED_SHARE_MAX:
        return _v(V_PRICED, "CONFIRMATION_PRICED_BEFORE_ENTRY", "confirmation pre-entry share %s (limit %.2f)"
                  % (_r(share), PRICED_SHARE_MAX), "PRICED_BEFORE_ENTRY")
    if c.get("ann_net") is None or float(c["ann_net"]) < MATERIALITY_ANN_NET:
        return _v(V_MATERIALITY, "CONFIRMATION_MATERIALITY", "confirmation net book %s/yr below %.3f"
                  % (_r(c.get("ann_net"), 4), MATERIALITY_ANN_NET), "CONFIRMATION_NET_BELOW_1.5PCT_PER_YEAR")
    return _v(V_QUALIFIED, None, "every preregistered qualification gate and the untouched confirmation passed; "
                                 "a HUMAN gate governs prospective registration and capital eligibility stays False")


def multiplicity(p) -> dict:
    """BH q = 0.10 at m = 1 + the inherited cells at p = 1. ``p`` must be present to pass."""
    m = 1 + len(INHERITED_NULLS)
    out = {"m": m, "q": BH_Q, "p_value": p, "inherited_nulls": list(INHERITED_NULLS),
           "inherited_p_value_policy": "p = 1 each", "single_survivor_threshold": BH_Q / m,
           "p_value_definition": "one-sided Newey-West p of the qualification signed rank IC series (lag %d)"
                                 % IC_NW_LAG, "denominator_reset": False}
    if p is None:
        return dict(out, passes=False, benjamini_hochberg=None)
    pv = {MECHANISM_ID: float(p)}
    pv.update({k: 1.0 for k in INHERITED_NULLS})
    bh = S.bh_fdr(pv, BH_Q)
    return dict(out, passes=MECHANISM_ID in bh["survivors"], benjamini_hochberg=bh)


def incumbent_increment(daily_net: pd.Series, incumbent_daily) -> dict:
    inc = incumbent_daily() if callable(incumbent_daily) else incumbent_daily
    if inc is None or not len(inc):
        return {"state": "DATA_HOLD", "why": "the incumbent daily path is unavailable"}
    from .intraday_alpha import equal_risk_daily
    return equal_risk_daily(daily_net, inc, label=MECHANISM_ID)


def gate_inputs(q: dict, halves: dict, mult: dict, data_hold: Optional[str] = None) -> dict:
    return {"data_hold": data_hold,
            "ic_mean": q["signed_rank_ic"]["mean"], "ic_t": q["signed_rank_ic"]["t"],
            "p_ic": q["signed_rank_ic"]["p_one_sided"],
            "pre_ic_mean": q["pre_entry_rank_ic"]["mean"], "pre_ic_t": q["pre_entry_rank_ic"]["t"],
            "pre_share": q["pre_entry_share"], "resid_ic_t": q["price_state_control"]["t"],
            "ann_net": q["book"].get("ann_net"),
            "halves": {k: {"ann_net": h["book"].get("ann_net"), "ic_mean": h["signed_rank_ic"]["mean"]}
                       for k, h in halves.items()},
            "bh_pass": bool(mult.get("passes"))}


# --------------------------------------------------------------------------- #
# Evaluation and the executor contract
# --------------------------------------------------------------------------- #
def artifact_path() -> str:
    return str(research_root() / "results" / ARTIFACT_NAME)


def _result(v: dict, statistic: dict, economics: dict, mult: dict, identity) -> dict:
    return {"verdict": v["verdict"], "why": v["why"], "gate": v.get("gate"),
            "kill_rule_fired": v.get("kill_rule_fired"),
            "statistic": statistic, "economics": economics, "multiplicity": mult,
            "artifact": artifact_path(), "capital_eligible": False, "scorer": SCORER,
            "input_data_identity": stable_hash(identity or {})}


def hold_result(why: str, identity=None) -> dict:
    return _result(_v(V_DATA_HOLD, "DATA", why, "DATA"), {"lockbox_t": None}, {}, multiplicity(None), identity)


def evaluate(inputs: dict, *, incumbent_daily=None) -> dict:
    panel = inputs["panel"]
    dates = pd.DatetimeIndex(panel["dates"])
    row_cik = inputs["row_cik"]
    sic_idx = inputs["sic_index"]
    integrity = inputs.get("fsds_integrity") or {"passes": False}
    identity = inputs.get("identity") or {}
    ann, ann_stats = announcement_sessions(inputs["filings"], {c for c in row_cik if c}, dates)
    decisions = weekly_decisions(dates, QUALIFICATION[0], CONFIRMATION[1])
    built = build_cohorts(panel, row_cik, ann, sic_idx, decisions)
    cohorts = built["cohorts"]
    cov_q = coverage(panel, row_cik, ann, sic_idx, *QUALIFICATION)
    traded_q = sum(1 for c in cohorts if c["traded"] and QUALIFICATION[0] <= c["date"] <= QUALIFICATION[1])
    audit: list = []
    prices = PriceReader(panel["tr"], panel["spy"])
    out = {"announcements": ann_stats, "calendar_stats": built["stats"],
           "coverage": {"QUALIFICATION": cov_q},
           "calendar": {"QUALIFICATION": calendar_census(cohorts, dates, *QUALIFICATION)},
           "fsds_integrity": integrity, "windows_measured": audit, "confirmation": "UNTOUCHED"}
    hold = data_hold_reason(integrity, cov_q, traded_q, "QUALIFICATION")
    if hold:
        out["gate_inputs"] = {"data_hold": hold}
        out["max_session_read"] = prices.max_session_read
        out["executor_result"] = _result(_v(V_DATA_HOLD, "DATA", hold, "DATA"),
                                         {"lockbox_t": None, "qualification_cohorts": traded_q,
                                          "pit_sic_member_quarter_share": cov_q["pit_sic_share"],
                                          "announcement_member_quarter_share": cov_q["announcement_share"]},
                                         {"cost_bps_per_side": COST_BPS}, multiplicity(None), identity)
        return out
    q, q_daily = measure(cohorts, prices, dates, *QUALIFICATION, label="QUALIFICATION", audit=audit)
    halves = {k: measure(cohorts, prices, dates, *w, label=k, audit=audit)[0] for k, w in HALVES.items()}
    mult = multiplicity(q["signed_rank_ic"]["p_one_sided"])
    g = gate_inputs(q, halves, mult)
    v = pre_incumbent_gate(g)
    inc = None
    if v is None:
        inc = incumbent_increment(q_daily, incumbent_daily)
        g["incumbent"] = {k: inc.get(k) for k in ("state", "why", "incremental_ann_net_return", "t_incremental",
                                                  "positive_incremental_utility_after_costs")}
        v = incumbent_gate(inc)
    conf, cg = None, None
    if v is None:
        cov_c = coverage(panel, row_cik, ann, sic_idx, *CONFIRMATION)
        out["coverage"]["CONFIRMATION"] = cov_c
        out["calendar"]["CONFIRMATION"] = calendar_census(cohorts, dates, *CONFIRMATION)
        traded_c = sum(1 for c in cohorts if c["traded"] and CONFIRMATION[0] <= c["date"] <= CONFIRMATION[1])
        hold_c = data_hold_reason(integrity, cov_c, traded_c, "CONFIRMATION", min_cohorts=0)
        if hold_c:
            cg = {"data_hold": hold_c}
        else:
            conf, _ = measure(cohorts, prices, dates, *CONFIRMATION, label="CONFIRMATION", audit=audit)
            cg = {"cohorts": conf["cohorts"], "ic_mean": conf["signed_rank_ic"]["mean"],
                  "ic_t": conf["signed_rank_ic"]["t"], "p_ic": conf["signed_rank_ic"]["p_one_sided"],
                  "pre_share": conf["pre_entry_share"], "ann_net": conf["book"].get("ann_net")}
        v = confirmation_gate(cg)
    out.update({"gate_inputs": g, "confirmation_gate_inputs": cg, "qualification": q, "halves": halves,
                "incumbent_equal_risk": inc, "confirmation": conf if conf else "UNTOUCHED",
                "max_session_read": prices.max_session_read})
    statistic = {
        "lockbox_t": conf["signed_rank_ic"]["t"] if conf else None,
        "lockbox_window": "CONFIRMATION %s..%s" % CONFIRMATION,
        "qualification_signed_rank_ic_mean": q["signed_rank_ic"]["mean"],
        "qualification_signed_rank_ic_t": q["signed_rank_ic"]["t"],
        "qualification_p_one_sided": q["signed_rank_ic"]["p_one_sided"], "ic_nw_lag": IC_NW_LAG,
        "pre_entry_rank_ic_t": q["pre_entry_rank_ic"]["t"], "pre_entry_share": q["pre_entry_share"],
        "price_state_residual_ic_t": q["price_state_control"]["t"],
        "qualification_cohorts": q["cohorts"], "qualification_entrants": q["entrants"],
        "confirmation_cohorts": conf["cohorts"] if conf else None}
    economics = {
        "units": "fraction of NAV; each weekly cohort carries %.2f NAV long and %.2f NAV short" % (SLOT_WEIGHT, SLOT_WEIGHT),
        "cost_bps_per_side": COST_BPS, "qualification_book": q["book"],
        "qualification_book_by_cost": q["book_by_cost_bps_per_side"],
        "qualification_ann_cost_drag": q["ann_cost_drag"],
        "halves_ann_net": {k: h["book"].get("ann_net") for k, h in halves.items()},
        "incumbent_increment_ann": (inc or {}).get("incremental_ann_net_return"),
        "confirmation_book": conf["book"] if conf else None}
    out["executor_result"] = _result(v, statistic, economics, mult, identity)
    return out


def design() -> dict:
    return {"announcement": "original 8-K declaring Item %s; follow-ups within %d days of the last kept one "
                            "dropped; event session = first session whose 16:00 ET close is strictly after "
                            "acceptance" % (ANNOUNCEMENT_ITEM, FOLLOWUP_DAYS),
            "reaction": "tr(e)/tr(e-1) - spy(e)/spy(e-1)",
            "industry": "2-digit SIC of the latest FSDS sub.txt submission accepted on a date strictly before "
                        "the decision date; members without one are excluded",
            "identity": "R58 RESOLVED cik_map (control_block_events.identity_map, no SEC ticker map); one row "
                        "per CIK per decision",
            "decisions": "last panel session of each ISO week",
            "peers": "same-industry members whose latest announcement session lies in [t-%d, t], >= %d, equal "
                     "weight (no owned PIT market capitalisation)" % (PEER_LOOKBACK, MIN_PEERS),
            "non_reporter": "member, no announcement in [t-%d, t], expected session <= t+%d, scored once per "
                            "(CIK, expected session) at the first decision with >= %d peers"
                            % (NO_REPORT_SESSIONS, MAX_LEAD_SESSIONS, MIN_PEERS),
            "expected_session": "first session on/after min(past announcement session date + %d days) that is "
                                "after the decision date" % EXPECTED_SHIFT_DAYS,
            "entry_exit": "entry close t+%d; exit close min(entry+%d, expected-%d); >= %d held sessions; an "
                          "actual announcement never exits early" % (ENTRY_DELAY, HOLD_CAP, PRE_EXPECTED_BUFFER,
                                                                     MIN_HOLD),
            "book": ">= %d entrants; long top / short bottom tercile (floor(n/3) each), equal weight, %.2f NAV per "
                    "leg per cohort, buy-and-hold to exit, %.1f bp per side on entry and exit notional"
                    % (MIN_COHORT, SLOT_WEIGHT, COST_BPS),
            "ic": "Spearman(signal, entry-to-exit total return minus SPY) per cohort; NW lag %d" % IC_NW_LAG,
            "pre_entry": "Spearman(signal, signal-close-to-entry-close return minus SPY); share = pre / (pre + "
                         "hold) of the mean gross long-short spreads; limit %.2f" % PRICED_SHARE_MAX,
            "price_state_control": "pooled OLS of within-cohort z(signal) on z(own (t-%d, t] return) and "
                                   "z(industry equal-weight (t-%d, t] return); residual rank IC NW t >= %.1f"
                                   % (PEER_LOOKBACK, PEER_LOOKBACK, T_FLOOR),
            "windows": {"QUALIFICATION": list(QUALIFICATION), "HALVES": {k: list(v) for k, v in HALVES.items()},
                        "CONFIRMATION": list(CONFIRMATION)},
            "coverage_floors": {"pit_sic_member_quarters": MIN_SIC_COVERAGE,
                                "announcement_member_quarters": MIN_ANNOUNCEMENT_COVERAGE},
            "cost_ladder_bps_per_side": list(COST_LADDER_BPS)}


def run(*, verbose: bool = True, write: bool = True, inputs: Optional[dict] = None,
        incumbent_daily="LOAD") -> dict:
    inputs = load_inputs() if inputs is None else inputs
    if isinstance(incumbent_daily, str):
        def incumbent_source():
            from .intraday_alpha import incumbent_daily_path
            return incumbent_daily_path()
    else:
        incumbent_source = incumbent_daily
    detail = evaluate(inputs, incumbent_daily=incumbent_source)
    body = {"schema": "alpha_recovery_peer_earnings_information_transfer/1",
            "calculation_owner": CALCULATION_OWNER, "mechanism_id": MECHANISM_ID,
            "preregistration": PREREGISTRATION, "preregistration_altered_after_results": False,
            "kill_rule_frozen": KILL_RULE_FROZEN, "design": design(), "scorer": SCORER,
            "capital_eligible": False}
    body.update(detail)
    res = body["executor_result"]
    body["verdict"], body["why"] = res["verdict"], res["why"]
    if write:
        write_artifact(ARTIFACT_NAME, body)
    if verbose:
        print("%s %s: %s" % (MECHANISM_ID, res["verdict"], res["why"]), flush=True)
    return body


def run_mechanism(*, mechanism: dict) -> dict:
    """The R59 mechanism-executor contract. Refuses a different mechanism or a kill rule that
    differs, byte for byte, from the preregistered one."""
    mid = (mechanism or {}).get("mechanism_id")
    if mid != MECHANISM_ID:
        raise ValueError("executor for %s was handed %r" % (MECHANISM_ID, mid))
    rule = ((mechanism or {}).get("pnl_gate") or {}).get("KILL_RULE")
    if rule != KILL_RULE_FROZEN:
        raise ValueError("the catalog KILL_RULE differs from the preregistered rule; an executor does not "
                         "judge a changed contract")
    try:
        body = run(verbose=False, write=True)
    except (OSError, KeyError, ValueError, ImportError) as exc:
        return hold_result("an owned input is unavailable or unreadable: %r" % (exc,))
    return dict(body["executor_result"])
